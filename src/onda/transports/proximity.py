"""Proximity store-and-forward transport.

Inspired by Briar, Scuttlebutt, and the NASA Delay-Tolerant Networking
work. The premise: two AIs that NEVER directly meet should still be able
to exchange a task if some friend of A also passes near C at some point.

How it actually works in v0.2:

  1. The application calls `manager.send_envelope(env, recipient_did=C)`
     where C is unreachable via any direct transport. The manager picks
     the proximity transport (priority 100, last resort).
  2. ProximityTransport seals the inner envelope for C (NaCl box with C's
     X25519 key, derived from C's DID), wraps it in a `ProximityCarrier`
     with a fresh UUID, and stores the carrier in our local mailbox.
  3. Whenever any underlying transport (BLE, LAN, …) discovers a peer P,
     ProximityTransport opportunistically pushes any forwardable carriers
     to P. P stores them in its own mailbox and repeats the process.
  4. Eventually a peer running an Onda node sees a carrier whose
     `final_recipient_did` matches its own DID. It decrypts the inner
     envelope, verifies the original sender's signature, and dispatches
     to the local handler exactly as if it had arrived directly.

End-to-end privacy:
  * The inner envelope is sealed with NaCl box for the final recipient.
    Relay nodes see only metadata (sender DID, recipient DID, timestamps).
  * The OUTER carrier envelope is signed by whoever is forwarding RIGHT
    NOW, so a relay can refuse to accept carriers from peers it doesn't
    trust without losing original-sender provenance.

Anti-loop / abuse limits:
  * UUID dedup via mailbox.has().
  * `hop_count` increments on every forward; `max_hops` (default 4)
    bounds spread.
  * `expires_at` (default 7 days) bounds longevity.
  * Mailbox size capped (default 1000 rows) to prevent flooding.
"""

from __future__ import annotations

import base64
import time
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

import trio

from ..crypto import decrypt_from, encrypt_for
from ..identity import Identity
from ..log import get_logger
from ..network.transport_base import (
    FrameHandler,
    IncomingFrame,
    PeerEndpoint,
    Transport,
    TransportError,
    TransportName,
    TransportPriority,
)
from ..protocol import Envelope, MessageType, ProximityCarrierBody
from ._mailbox import CarrierRow, ProximityMailbox, expires_at_from_ttl

log = get_logger(__name__)


class ProximityTransport(Transport):
    name: TransportName = "proximity"
    priority: int = TransportPriority.PROXIMITY

    def __init__(
        self,
        *,
        identity: Identity,
        mailbox_path: Path,
        carriers: list[Transport] | None = None,
        ttl_seconds: int = 7 * 24 * 3600,
        max_hops: int = 4,
        max_mailbox_rows: int = 1000,
    ) -> None:
        """
        `carriers` is a list of OTHER Transport instances we piggyback on
        for actual radio access. We do NOT own those transports — the
        manager does. The list lets us ask each "do you currently see peer
        X?" and "please send these bytes to peer X."
        """

        self.identity = identity
        self.mailbox = ProximityMailbox(mailbox_path, max_rows=max_mailbox_rows)
        self.ttl = ttl_seconds
        self.max_hops = max_hops
        self.carriers: list[Transport] = list(carriers or [])
        self._handler: FrameHandler | None = None
        self._discover_send: trio.MemorySendChannel[PeerEndpoint] | None = None
        self._discover_recv: trio.MemoryReceiveChannel[PeerEndpoint] | None = None

    # ---- Lifecycle --------------------------------------------------

    async def is_available(self) -> bool:
        # Proximity is always "available" in the sense that it can store
        # carriers; whether they ever leave the mailbox depends on the
        # underlying transports we piggyback on.
        return True

    async def start(self, on_frame: FrameHandler) -> None:
        if self._handler is not None:
            raise RuntimeError("ProximityTransport already started")
        self._handler = on_frame
        send, recv = trio.open_memory_channel[PeerEndpoint](max_buffer_size=64)
        self._discover_send = send
        self._discover_recv = recv

    async def stop(self) -> None:
        if self._discover_send is not None:
            await self._discover_send.aclose()
        self.mailbox.close()
        self._handler = None

    def add_carrier(self, t: Transport) -> None:
        """Register an additional underlying transport for opportunistic forwarding."""

        self.carriers.append(t)

    # ---- Discovery / send (Transport API) ---------------------------

    def peers(self) -> list[PeerEndpoint]:
        # Proximity has no "peers" of its own — meeting events are the
        # peers of the underlying transports. We expose nothing here so
        # the manager doesn't double-count.
        return []

    async def discover(self) -> AsyncIterator[PeerEndpoint]:
        if self._discover_recv is None:
            raise RuntimeError("ProximityTransport not started")
        async for ep in self._discover_recv:
            yield ep

    async def send(self, peer: PeerEndpoint, payload: bytes) -> bytes | None:
        """Enqueue a payload for eventual delivery to `peer.did`.

        We immediately try to forward via any visible underlying transport;
        if no carrier currently sees the recipient OR a relay we trust,
        the carrier sits in the mailbox until `attempt_drain()` is called
        on a successful encounter.
        """

        if not peer.did:
            raise TransportError("proximity send requires recipient DID")

        carrier = self._build_carrier(payload, recipient_did=peer.did)
        is_new = self.mailbox.store(carrier)
        if is_new:
            log.info(
                "proximity.queued",
                carrier_id=carrier.carrier_id,
                final_recipient=carrier.final_recipient_did,
            )
        # Try immediate opportunistic delivery.
        await self.attempt_drain()
        return None

    # ---- Forwarding mechanics ---------------------------------------

    async def attempt_drain(self) -> int:
        """Try to push every undelivered, unexpired carrier to a visible peer.

        Returns the number of carriers handed off. Called automatically
        on `send()` and intended to be called again whenever an underlying
        transport discovers a new peer (the Node spawns a background task
        that observes carrier transports' `discover()` streams and triggers
        this method).
        """

        rows = self.mailbox.all_forwardable()
        if not rows:
            return 0

        # Build a set of "I currently see" PeerEndpoints from every carrier.
        visible: dict[str, tuple[Transport, PeerEndpoint]] = {}
        for t in self.carriers:
            for ep in t.peers():
                if ep.did:
                    visible[ep.did] = (t, ep)

        delivered = 0
        for row in rows:
            # Direct delivery has highest priority: if we currently see the
            # final recipient, just hand it over.
            if row.final_recipient_did in visible:
                t, ep = visible[row.final_recipient_did]
                if await self._forward_one(row, t, ep, direct=True):
                    self.mailbox.mark_delivered(row.carrier_id)
                    delivered += 1
                    continue
            # Indirect: gossip to anyone we see (other than the original
            # author — they already have it).
            for did, (t, ep) in visible.items():
                if did == row.original_sender_did:
                    continue
                if await self._forward_one(row, t, ep, direct=False):
                    delivered += 1
                    break  # one hop per cycle is plenty
        return delivered

    async def _forward_one(
        self,
        row: CarrierRow,
        carrier_transport: Transport,
        peer: PeerEndpoint,
        *,
        direct: bool,
    ) -> bool:
        body = ProximityCarrierBody(
            carrier_id=row.carrier_id,
            final_recipient_did=row.final_recipient_did,
            sealed_inner_b64=row.sealed_inner_b64,
            created_at=row.created_at,
            expires_at=row.expires_at,
            hop_count=row.hop_count + 1,
            max_hops=row.max_hops,
            original_sender_did=row.original_sender_did,
        )
        env = Envelope.build(
            identity=self.identity,
            msg_type=MessageType.PROXIMITY_CARRIER,
            body=body,
            recipient=peer.did,
        )
        try:
            await carrier_transport.send(peer, env.to_json().encode("utf-8"))
        except Exception as exc:
            log.warning(
                "proximity.forward_failed",
                via=carrier_transport.name,
                peer=peer.did,
                err=str(exc),
            )
            return False
        log.info(
            "proximity.forwarded",
            carrier_id=row.carrier_id,
            via=carrier_transport.name,
            to=peer.did,
            direct=direct,
            hop_count=row.hop_count + 1,
        )
        return True

    # ---- Carrier construction ---------------------------------------

    def _build_carrier(self, inner_payload: bytes, *, recipient_did: str) -> CarrierRow:
        # Seal: NaCl box for recipient (using protocol.crypto helpers).
        sealed = encrypt_for(self.identity, recipient_did, inner_payload)
        sealed_b64 = base64.b64encode(sealed).decode("ascii")
        carrier_id = str(uuid.uuid4())
        created = datetime.now(timezone.utc).isoformat(timespec="seconds")
        expires = expires_at_from_ttl(self.ttl)
        return CarrierRow(
            carrier_id=carrier_id,
            final_recipient_did=recipient_did,
            original_sender_did=self.identity.did,
            sealed_inner_b64=sealed_b64,
            created_at=created,
            expires_at=expires,
            hop_count=0,
            max_hops=self.max_hops,
            delivered=False,
            seen_count=1,
        )

    # ---- Inbound carrier handling -----------------------------------

    async def handle_inbound_carrier(
        self,
        body: ProximityCarrierBody,
        from_peer: PeerEndpoint,
    ) -> Envelope | None:
        """Process a carrier delivered to us by some other transport.

        Called by the Node-level v2 handler when a verified envelope of
        type PROXIMITY_CARRIER arrives. Returns the Envelope (already
        verified) for the inner message IF we are the final recipient,
        otherwise stores the carrier for later forwarding and returns None.
        """

        # Drop expired or out-of-hops carriers immediately.
        try:
            expires = datetime.fromisoformat(body.expires_at)
        except ValueError:
            return None
        if expires <= datetime.now(timezone.utc):
            log.info("proximity.dropped_expired", carrier_id=body.carrier_id)
            return None
        if body.hop_count >= body.max_hops:
            log.info("proximity.dropped_hop_limit", carrier_id=body.carrier_id)
            return None

        # Anti-loop: already seen this carrier? Nothing to do.
        if self.mailbox.has(body.carrier_id):
            self.mailbox.store(  # bump seen_count
                CarrierRow(
                    carrier_id=body.carrier_id,
                    final_recipient_did=body.final_recipient_did,
                    original_sender_did=body.original_sender_did,
                    sealed_inner_b64=body.sealed_inner_b64,
                    created_at=body.created_at,
                    expires_at=body.expires_at,
                    hop_count=body.hop_count,
                    max_hops=body.max_hops,
                    delivered=False,
                    seen_count=1,
                )
            )
            return None

        # Are we the final recipient?
        if body.final_recipient_did == self.identity.did:
            try:
                sealed = base64.b64decode(body.sealed_inner_b64)
                inner = decrypt_from(self.identity, body.original_sender_did, sealed)
                inner_env = Envelope.from_json(inner)
            except Exception as exc:
                log.warning("proximity.decrypt_failed", err=str(exc))
                return None
            if not inner_env.verify():
                log.warning(
                    "proximity.inner_bad_signature",
                    carrier_id=body.carrier_id,
                )
                return None
            # Persist the carrier as "delivered" so re-arrivals dedupe.
            self.mailbox.store(
                CarrierRow(
                    carrier_id=body.carrier_id,
                    final_recipient_did=body.final_recipient_did,
                    original_sender_did=body.original_sender_did,
                    sealed_inner_b64=body.sealed_inner_b64,
                    created_at=body.created_at,
                    expires_at=body.expires_at,
                    hop_count=body.hop_count,
                    max_hops=body.max_hops,
                    delivered=True,
                    seen_count=1,
                )
            )
            return inner_env

        # Otherwise: store for later forwarding.
        self.mailbox.store(
            CarrierRow(
                carrier_id=body.carrier_id,
                final_recipient_did=body.final_recipient_did,
                original_sender_did=body.original_sender_did,
                sealed_inner_b64=body.sealed_inner_b64,
                created_at=body.created_at,
                expires_at=body.expires_at,
                hop_count=body.hop_count,
                max_hops=body.max_hops,
                delivered=False,
                seen_count=1,
            )
        )
        return None


__all__ = ["ProximityTransport"]
