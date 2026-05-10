"""TransportManager: orchestrate multiple transports under a single API.

Responsibilities:

  * Probe each registered transport's `is_available()` at startup.
  * Start the available ones with a unified `on_frame` handler that lifts
    raw bytes into Onda envelopes (signature verified before dispatch).
  * Maintain a single peer registry keyed by DID, with multiple endpoints
    per peer (one per transport). Endpoints are merged opportunistically
    as new info arrives (e.g. BLE learns the DID after the first signed
    frame).
  * Expose `send_envelope(recipient_did, envelope)` and `broadcast_envelope`
    that try transports in priority order and fall back on failure.

Why a single manager (vs. each transport being independent):

  * Onda's identity is a *DID*, not an IP. Two paths to the same DID must
    not look like two peers to the application.
  * Failure modes matter. If Internet is up but the peer's home router is
    down, we want to silently retry over LAN/BLE without bothering the
    Node-level code.
  * Routing decisions for store-and-forward (whether to seal-and-relay vs.
    deliver directly) need to see ALL transports at once.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable

import trio

from ..log import get_logger
from ..protocol import Envelope
from .transport_base import (
    FrameHandler,
    IncomingFrame,
    PeerEndpoint,
    Transport,
    TransportError,
    TransportName,
)

log = get_logger(__name__)


# Application-level callback the manager invokes once an envelope is parsed
# and verified. Returns the optional reply envelope.
EnvelopeHandler = Callable[[Envelope, PeerEndpoint], Awaitable[Envelope | None]]


@dataclass
class _PeerRecord:
    """Aggregated view of one peer across every transport that has seen them."""

    did: str
    endpoints: dict[TransportName, PeerEndpoint] = field(default_factory=dict)
    name: str | None = None

    def add(self, ep: PeerEndpoint) -> None:
        existing = self.endpoints.get(ep.transport)
        if existing is None:
            self.endpoints[ep.transport] = ep
        else:
            existing.merge(ep)
        if ep.name and not self.name:
            self.name = ep.name


class TransportManager:
    """Orchestrator for v0.2 multi-transport operation.

    Construction is decoupled from start: tests can build a manager with a
    custom mix of transports (e.g. two MockTransport instances) and only
    call start() on the ones they care about.
    """

    def __init__(
        self,
        transports: list[Transport],
        *,
        on_envelope: EnvelopeHandler,
        priority_override: dict[TransportName, int] | None = None,
    ) -> None:
        self._all = list(transports)
        self._active: list[Transport] = []
        self._on_envelope = on_envelope
        self._priority_override = priority_override or {}
        # DID -> aggregated record. Endpoints with no DID yet sit in
        # `_orphans` until a signed envelope arrives that names their DID.
        self._peers: dict[str, _PeerRecord] = {}
        self._orphans: list[PeerEndpoint] = []
        self._started = False

    # ---- Lifecycle ------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("TransportManager already started")
        for t in self._all:
            try:
                ok = await t.is_available()
            except Exception as exc:
                log.warning("transport.availability_check_failed", name=t.name, err=str(exc))
                ok = False
            if not ok:
                log.info("transport.skipped_unavailable", name=t.name)
                continue
            try:
                await t.start(self._handle_frame)
            except Exception as exc:
                log.exception("transport.start_failed", name=t.name)
                # Don't propagate — one broken transport must not kill the
                # whole node. Continue with the rest.
                continue
            self._active.append(t)
            log.info("transport.started", name=t.name, priority=self._priority_of(t))
        self._started = True

        # Spawn one discovery consumer per active transport. Each transport's
        # `discover()` is an async iterator of PeerEndpoints; we forward
        # every endpoint into the manager's peer registry. Without this
        # task the registry would only ever populate via inbound signed
        # frames, which means a peer discovered via mDNS-TXT (DID known
        # from advertisement) would stay invisible until it sent us
        # something — defeating the point of an `ask`-from-rest scenario.
        async def _consume(transport: Transport) -> None:
            try:
                async for ep in transport.discover():
                    self.observe_endpoint(ep)
            except Exception as exc:
                log.warning("manager.discover_consumer_failed", name=transport.name, err=str(exc))

        # We need a long-lived nursery to host these tasks. Since `start()`
        # is called from inside the Node's nursery, we spawn each consumer
        # via trio.lowlevel.current_task().context — we use a background
        # task spawned with `trio.lowlevel.spawn_system_task` so it survives
        # for the manager's lifetime regardless of who started us.
        for t in self._active:
            trio.lowlevel.spawn_system_task(_consume, t)

    async def stop(self) -> None:
        # Stop in reverse priority so that the most-fundamental transports
        # (Internet) are torn down last, allowing pending sends on faster
        # paths to drain first.
        for t in sorted(self._active, key=self._priority_of, reverse=True):
            try:
                await t.stop()
            except Exception as exc:
                log.warning("transport.stop_failed", name=t.name, err=str(exc))
        self._active.clear()
        self._started = False

    # ---- Helpers --------------------------------------------------------

    def _priority_of(self, t: Transport) -> int:
        return self._priority_override.get(t.name, t.priority)

    def _ordered_active(self) -> list[Transport]:
        return sorted(self._active, key=self._priority_of)

    def active_transports(self) -> list[TransportName]:
        return [t.name for t in self._ordered_active()]

    # ---- Inbound handling ----------------------------------------------

    async def _handle_frame(self, frame: IncomingFrame) -> bytes | None:
        # Parse envelope, verify signature, learn peer DID, dispatch upstream.
        try:
            env = Envelope.from_json(frame.payload)
        except Exception as exc:
            log.warning("manager.bad_payload", transport=frame.transport, err=str(exc))
            return None

        if not env.verify():
            log.warning(
                "manager.bad_signature",
                transport=frame.transport,
                sender=env.sender,
            )
            return None

        # Learn / reinforce peer mapping.
        if env.sender:
            ep = frame.peer
            ep.did = env.sender
            self._record(ep)

        try:
            reply = await self._on_envelope(env, frame.peer)
        except Exception:
            log.exception("manager.handler_failed", transport=frame.transport)
            return None

        if reply is None:
            return None
        return reply.to_json().encode("utf-8")

    # ---- Peer registry --------------------------------------------------

    def _record(self, ep: PeerEndpoint) -> None:
        if ep.did is None:
            self._orphans.append(ep)
            return
        rec = self._peers.get(ep.did)
        if rec is None:
            rec = _PeerRecord(did=ep.did)
            self._peers[ep.did] = rec
        rec.add(ep)

    def observe_endpoint(self, ep: PeerEndpoint) -> None:
        """External hook for transports that want to publish peers without
        going through `_handle_frame` (e.g. mDNS / BLE advertisement, where
        the DID may already be in the announcement payload).
        """

        self._record(ep)

    def known_peers(self) -> list[_PeerRecord]:
        return list(self._peers.values())

    def endpoints_for(self, did: str) -> list[PeerEndpoint]:
        rec = self._peers.get(did)
        if rec is None:
            return []
        return sorted(
            rec.endpoints.values(),
            key=lambda e: self._priority_for_name(e.transport),
        )

    def _priority_for_name(self, name: TransportName) -> int:
        for t in self._all:
            if t.name == name:
                return self._priority_of(t)
        return 9999

    # ---- Send -----------------------------------------------------------

    async def send_envelope(
        self,
        envelope: Envelope,
        *,
        recipient_did: str | None = None,
        timeout: float = 60.0,
    ) -> Envelope | None:
        """Send to a specific peer (or any peer if `recipient_did` is None),
        trying transports in priority order. Returns the parsed reply or None.
        """

        payload = envelope.to_json().encode("utf-8")
        targets = self._select_targets(recipient_did)
        if not targets:
            log.info("manager.send.no_targets", recipient_did=recipient_did)
            return None

        for ep, transport in targets:
            try:
                with trio.fail_after(timeout):
                    raw = await transport.send(ep, payload)
            except (TransportError, trio.TooSlowError) as exc:
                log.info(
                    "manager.send.failed_fallback",
                    transport=transport.name,
                    peer=ep.did,
                    err=str(exc),
                )
                continue
            except Exception as exc:
                log.warning(
                    "manager.send.unexpected",
                    transport=transport.name,
                    peer=ep.did,
                    err=str(exc),
                )
                continue
            if raw is None:
                # Fire-and-forget transport (proximity, BLE notify-only) —
                # success means "queued/sent", reply will arrive via on_frame.
                return None
            try:
                reply = Envelope.from_json(raw)
                if reply.verify():
                    return reply
                log.warning("manager.send.reply_bad_signature", transport=transport.name)
            except Exception as exc:
                log.warning("manager.send.reply_parse_failed", err=str(exc))
        return None

    async def broadcast_envelope(self, envelope: Envelope) -> list[Envelope]:
        """Send to every known peer (one path per peer), gathering verified replies."""

        replies: list[Envelope] = []
        for did in list(self._peers):
            reply = await self.send_envelope(envelope, recipient_did=did)
            if reply is not None:
                replies.append(reply)
        return replies

    def _select_targets(
        self, recipient_did: str | None
    ) -> list[tuple[PeerEndpoint, Transport]]:
        """Return [(endpoint, transport_instance), …] in priority order."""

        out: list[tuple[PeerEndpoint, Transport]] = []
        recs: list[_PeerRecord]
        if recipient_did is not None:
            rec = self._peers.get(recipient_did)
            recs = [rec] if rec else []
        else:
            recs = list(self._peers.values())

        active_by_name = {t.name: t for t in self._ordered_active()}
        for rec in recs:
            for ep in sorted(
                rec.endpoints.values(),
                key=lambda e: self._priority_for_name(e.transport),
            ):
                t = active_by_name.get(ep.transport)
                if t is not None:
                    out.append((ep, t))
        return out
