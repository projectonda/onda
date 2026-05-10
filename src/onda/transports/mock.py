"""In-process Transport for tests and demos.

Two MockTransport instances connected to the same `MockBus` exchange frames
synchronously in memory. No sockets, no Bluetooth, no Wi-Fi — works on any
CI runner. Used for:

  * Unit tests of `TransportManager` priority/fallback behavior.
  * The store-and-forward `proximity` integration test (3-node A→B→C flow).
  * The "cable-cut" demo when running on hardware that doesn't have BLE.

`MockBus` doubles as a discovery oracle: calling `bus.connect(t1, t2)`
makes `t1` and `t2` mutually visible. Disconnecting them simulates losing
range / a router going down.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import trio

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

log = get_logger(__name__)


# ---- Bus ----------------------------------------------------------------


@dataclass
class _BusEntry:
    """A MockTransport's slot inside the shared MockBus."""

    transport: MockTransport
    handler: FrameHandler | None = None
    visible_to: set[str] = field(default_factory=set)


class MockBus:
    """Shared registry that lets MockTransport instances find each other.

    Topology is explicit: by default no two transports see each other.
    Tests use `connect(a, b)` to bring them into mutual range and
    `disconnect(a, b)` to take them out. Asymmetric ranges are allowed
    (you can connect a→b without connecting b→a) for store-and-forward
    test scenarios where one side is offline.
    """

    def __init__(self) -> None:
        self._entries: dict[str, _BusEntry] = {}

    # ---- Membership --------------------------------------------------

    def attach(self, t: MockTransport) -> None:
        if t.id in self._entries:
            raise ValueError(f"MockTransport {t.id} already attached")
        self._entries[t.id] = _BusEntry(transport=t)

    def detach(self, t: MockTransport) -> None:
        self._entries.pop(t.id, None)
        for entry in self._entries.values():
            entry.visible_to.discard(t.id)

    # ---- Topology -----------------------------------------------------

    def connect(self, a: MockTransport, b: MockTransport, *, bidirectional: bool = True) -> None:
        self._entries[a.id].visible_to.add(b.id)
        if bidirectional:
            self._entries[b.id].visible_to.add(a.id)

    def disconnect(self, a: MockTransport, b: MockTransport) -> None:
        self._entries[a.id].visible_to.discard(b.id)
        self._entries[b.id].visible_to.discard(a.id)

    # ---- Internals (used by MockTransport) --------------------------

    def visible(self, src: MockTransport) -> list[MockTransport]:
        entry = self._entries.get(src.id)
        if entry is None:
            return []
        return [self._entries[oid].transport for oid in entry.visible_to if oid in self._entries]

    async def deliver(self, src: MockTransport, dst_id: str, payload: bytes) -> bytes | None:
        entry = self._entries.get(dst_id)
        if entry is None:
            raise TransportError(f"unknown mock peer {dst_id}")
        if src.id not in entry.visible_to:
            raise TransportError(
                f"{src.id} cannot reach {dst_id}: not connected on the bus"
            )
        if entry.handler is None:
            raise TransportError(f"{dst_id} has no frame handler registered")
        # Build the inbound view from the destination's perspective.
        peer = PeerEndpoint(
            transport="mock",
            address=src.id,
            did=src.advertised_did,
            name=src.advertised_name,
            last_seen=time.time(),
        )
        frame = IncomingFrame(transport="mock", peer=peer, payload=payload)
        return await entry.handler(frame)

    def _set_handler(self, t: MockTransport, h: FrameHandler) -> None:
        self._entries[t.id].handler = h


# ---- The transport itself ----------------------------------------------


class MockTransport(Transport):
    name: TransportName = "mock"
    priority: int = TransportPriority.MOCK

    def __init__(
        self,
        bus: MockBus,
        *,
        node_id: str | None = None,
        advertised_did: str | None = None,
        advertised_name: str | None = None,
    ) -> None:
        self.id = node_id or f"mock-{uuid.uuid4().hex[:8]}"
        self.bus = bus
        self.advertised_did = advertised_did
        self.advertised_name = advertised_name
        self._started = False
        self._handler: FrameHandler | None = None
        self._discover_send: trio.MemorySendChannel[PeerEndpoint] | None = None
        self._discover_recv: trio.MemoryReceiveChannel[PeerEndpoint] | None = None
        self.bus.attach(self)

    # ---- Capability check --------------------------------------------

    async def is_available(self) -> bool:
        return True  # always available; that's the point

    # ---- Lifecycle ---------------------------------------------------

    async def start(self, on_frame: FrameHandler) -> None:
        if self._started:
            raise RuntimeError("MockTransport already started")
        self._handler = on_frame
        self.bus._set_handler(self, on_frame)
        send, recv = trio.open_memory_channel[PeerEndpoint](max_buffer_size=64)
        self._discover_send = send
        self._discover_recv = recv
        # Seed discovery with currently-visible peers.
        for peer in self.bus.visible(self):
            ep = PeerEndpoint(
                transport="mock",
                address=peer.id,
                did=peer.advertised_did,
                name=peer.advertised_name,
            )
            try:
                send.send_nowait(ep)
            except trio.WouldBlock:
                pass
        self._started = True

    async def stop(self) -> None:
        self._started = False
        self.bus.detach(self)
        if self._discover_send is not None:
            await self._discover_send.aclose()

    # ---- Discovery ---------------------------------------------------

    def peers(self) -> list[PeerEndpoint]:
        return [
            PeerEndpoint(
                transport="mock",
                address=p.id,
                did=p.advertised_did,
                name=p.advertised_name,
            )
            for p in self.bus.visible(self)
        ]

    async def discover(self) -> AsyncIterator[PeerEndpoint]:
        if self._discover_recv is None:
            raise RuntimeError("MockTransport not started")
        async for ep in self._discover_recv:
            yield ep

    # ---- Send --------------------------------------------------------

    async def send(self, peer: PeerEndpoint, payload: bytes) -> bytes | None:
        if not self._started:
            raise TransportError("MockTransport not started")
        return await self.bus.deliver(self, peer.address, payload)


__all__ = ["MockBus", "MockTransport"]
