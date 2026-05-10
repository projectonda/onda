"""TransportManager tests.

Goals:
  * Manager picks transports in priority order and falls back on errors.
  * Inbound envelopes with bad signatures are silently dropped.
  * Inbound envelopes with good signatures invoke the application handler.
  * Peer registry merges endpoints across transports.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest
import trio

from onda.identity import Identity
from onda.network.manager import TransportManager
from onda.network.transport_base import (
    FrameHandler,
    IncomingFrame,
    PeerEndpoint,
    Transport,
    TransportError,
    TransportName,
    TransportPriority,
)
from onda.protocol import (
    DiscoveryBody,
    Envelope,
    MessageType,
    TaskRequestBody,
    TaskResponseBody,
)


# ---- Tiny fake transports for unit tests --------------------------------


@dataclass
class _Recorded:
    peer: PeerEndpoint
    payload: bytes


class _RecordingTransport(Transport):
    """Always available, records everything sent through it, returns a
    canned reply (or raises if `fail` is True).
    """

    def __init__(
        self,
        name: TransportName,
        priority: int,
        *,
        fail: bool = False,
        reply: bytes | None = None,
    ) -> None:
        self.name = name
        self.priority = priority
        self._fail = fail
        self._reply = reply
        self._handler: FrameHandler | None = None
        self.sent: list[_Recorded] = []
        self._peers: list[PeerEndpoint] = []

    async def is_available(self) -> bool:
        return True

    async def start(self, on_frame: FrameHandler) -> None:
        self._handler = on_frame

    async def stop(self) -> None:
        self._handler = None

    def peers(self) -> list[PeerEndpoint]:
        return list(self._peers)

    async def discover(self) -> AsyncIterator[PeerEndpoint]:
        if False:
            yield  # pragma: no cover

    async def send(self, peer: PeerEndpoint, payload: bytes) -> bytes | None:
        self.sent.append(_Recorded(peer=peer, payload=payload))
        if self._fail:
            raise TransportError(f"{self.name} simulated failure")
        return self._reply


# ---- Helpers ------------------------------------------------------------


def _build_envelope(
    sender: Identity, msg_type: MessageType, *, recipient: str | None = None
) -> Envelope:
    if msg_type == MessageType.TASK_REQUEST:
        body = TaskRequestBody(prompt="ciao")
    elif msg_type == MessageType.TASK_RESPONSE:
        body = TaskResponseBody(in_reply_to="x", answer="y", responder_name="bob")
    else:
        body = DiscoveryBody(name=sender.name)
    return Envelope.build(
        identity=sender, msg_type=msg_type, body=body, recipient=recipient
    )


async def _noop_handler(env: Envelope, peer: PeerEndpoint) -> Envelope | None:
    return None


# ---- Tests --------------------------------------------------------------


@pytest.mark.trio
async def test_manager_starts_only_available_transports() -> None:
    class Unavailable(_RecordingTransport):
        async def is_available(self) -> bool:
            return False

    avail = _RecordingTransport("internet", TransportPriority.INTERNET)
    unav = Unavailable("bluetooth", TransportPriority.BLUETOOTH)
    mgr = TransportManager([avail, unav], on_envelope=_noop_handler)
    await mgr.start()
    try:
        assert mgr.active_transports() == ["internet"]
    finally:
        await mgr.stop()


@pytest.mark.trio
async def test_manager_send_uses_priority_order() -> None:
    alice = Identity.generate("alice")
    bob = Identity.generate("bob")

    fast = _RecordingTransport("internet", TransportPriority.INTERNET)
    slow = _RecordingTransport("bluetooth", TransportPriority.BLUETOOTH)
    mgr = TransportManager([fast, slow], on_envelope=_noop_handler)
    await mgr.start()
    try:
        # Bob known via both transports.
        mgr.observe_endpoint(
            PeerEndpoint(transport="internet", address="ip://b", did=bob.did, name="bob")
        )
        mgr.observe_endpoint(
            PeerEndpoint(transport="bluetooth", address="MAC:b", did=bob.did, name="bob")
        )
        env = _build_envelope(alice, MessageType.TASK_REQUEST, recipient=bob.did)
        await mgr.send_envelope(env, recipient_did=bob.did)
        assert len(fast.sent) == 1
        assert len(slow.sent) == 0  # internet won the priority race
    finally:
        await mgr.stop()


@pytest.mark.trio
async def test_manager_falls_back_when_preferred_transport_fails() -> None:
    alice = Identity.generate("alice")
    bob = Identity.generate("bob")

    fast = _RecordingTransport("internet", TransportPriority.INTERNET, fail=True)
    slow = _RecordingTransport("bluetooth", TransportPriority.BLUETOOTH)
    mgr = TransportManager([fast, slow], on_envelope=_noop_handler)
    await mgr.start()
    try:
        mgr.observe_endpoint(
            PeerEndpoint(transport="internet", address="ip://b", did=bob.did)
        )
        mgr.observe_endpoint(
            PeerEndpoint(transport="bluetooth", address="MAC:b", did=bob.did)
        )
        env = _build_envelope(alice, MessageType.TASK_REQUEST, recipient=bob.did)
        await mgr.send_envelope(env, recipient_did=bob.did)
        assert len(fast.sent) == 1
        assert len(slow.sent) == 1  # fell back when fast raised
    finally:
        await mgr.stop()


@pytest.mark.trio
async def test_manager_drops_inbound_with_bad_signature() -> None:
    alice = Identity.generate("alice")
    bob = Identity.generate("bob")

    delivered: list[Envelope] = []

    async def handler(env: Envelope, peer: PeerEndpoint) -> Envelope | None:
        delivered.append(env)
        return None

    t = _RecordingTransport("internet", TransportPriority.INTERNET)
    mgr = TransportManager([t], on_envelope=handler)
    await mgr.start()
    try:
        env = _build_envelope(alice, MessageType.TASK_REQUEST)
        # Tamper with body after signing
        import json
        raw = json.loads(env.to_json())
        raw["body"]["prompt"] = "TAMPER"
        bad = json.dumps(raw).encode("utf-8")

        peer_ep = PeerEndpoint(
            transport="internet", address="ip://a", did=alice.did
        )
        await mgr._handle_frame(
            IncomingFrame(transport="internet", peer=peer_ep, payload=bad)
        )
        assert delivered == []
    finally:
        await mgr.stop()


@pytest.mark.trio
async def test_manager_dispatches_good_inbound() -> None:
    alice = Identity.generate("alice")

    delivered: list[Envelope] = []

    async def handler(env: Envelope, peer: PeerEndpoint) -> Envelope | None:
        delivered.append(env)
        return None

    t = _RecordingTransport("internet", TransportPriority.INTERNET)
    mgr = TransportManager([t], on_envelope=handler)
    await mgr.start()
    try:
        env = _build_envelope(alice, MessageType.TASK_REQUEST)
        peer_ep = PeerEndpoint(transport="internet", address="ip://a", did=None)
        await mgr._handle_frame(
            IncomingFrame(
                transport="internet",
                peer=peer_ep,
                payload=env.to_json().encode("utf-8"),
            )
        )
        assert len(delivered) == 1
        assert delivered[0].sender == alice.did
        # And the peer registry now knows alice.
        assert any(p.did == alice.did for p in mgr.known_peers())
    finally:
        await mgr.stop()


@pytest.mark.trio
async def test_manager_returns_no_peers_for_unknown_recipient() -> None:
    alice = Identity.generate("alice")

    t = _RecordingTransport("internet", TransportPriority.INTERNET)
    mgr = TransportManager([t], on_envelope=_noop_handler)
    await mgr.start()
    try:
        env = _build_envelope(alice, MessageType.TASK_REQUEST)
        reply = await mgr.send_envelope(env, recipient_did="did:key:zUnknown")
        assert reply is None
        assert len(t.sent) == 0
    finally:
        await mgr.stop()
