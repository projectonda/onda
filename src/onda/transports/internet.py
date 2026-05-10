"""Internet transport: libp2p TCP + manual bootstrap, conforming to Transport.

This transport is the v0.2 specialization of v0.1's libp2p path for the
"public internet, dial via known multiaddr" use case. It does NOT do mDNS;
that's `lan.py`. Both transports use the SAME `Libp2pHost` (see
`_libp2p_shared.py`) so a node listens on a single TCP port.

Why bootstrap-only at this layer:

  * mDNS only works on the same broadcast domain. Once you cross a router
    (e.g. mobile data ↔ home LAN), peers must be reached by explicit
    multiaddr or by a discovery service. v0.2 keeps both manual.
  * v0.3 will add DHT-based discovery as another `Transport`; the priority
    will fall between Internet and LAN. The interface here is ready for it.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator

import trio

from ..config import OndaSettings
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
from ._libp2p_shared import Libp2pHost, parse_p2p_multiaddr

log = get_logger(__name__)


class InternetTransport(Transport):
    name: TransportName = "internet"
    priority: int = TransportPriority.INTERNET

    def __init__(self, *, host: Libp2pHost, settings: OndaSettings) -> None:
        self._lhost = host
        self._settings = settings
        self._handler: FrameHandler | None = None
        self._peers: dict[str, PeerEndpoint] = {}  # keyed by multiaddr
        self._discover_send: trio.MemorySendChannel[PeerEndpoint] | None = None
        self._discover_recv: trio.MemoryReceiveChannel[PeerEndpoint] | None = None
        self._nursery: trio.Nursery | None = None

    async def is_available(self) -> bool:
        # libp2p is a hard dep of the package; the only way it's not
        # available is if the OS networking stack is broken, which we'd
        # find out trying to bind anyway.
        return True

    async def start(self, on_frame: FrameHandler) -> None:
        if self._handler is not None:
            raise RuntimeError("InternetTransport already started")
        self._handler = on_frame
        send, recv = trio.open_memory_channel[PeerEndpoint](max_buffer_size=64)
        self._discover_send = send
        self._discover_recv = recv
        self._lhost.set_inbound_handler(self._on_libp2p_inbound)

        # We don't own the libp2p host's lifecycle (the Node does), but we
        # do need to dial bootstrap addrs once the host is ready. We spawn
        # that work into a separate task so start() returns immediately.
        # The Node's nursery picks up the task via `dial_bootstrap()`.
        # Bootstrap dialing is in `dial_bootstrap()` — call it explicitly
        # from the Node after the host's `host_ready` event has fired.

    async def dial_bootstrap(self) -> None:
        """Dial every multiaddr in `OndaSettings.bootstrap`. Best-effort."""

        await self._lhost.host_ready.wait()
        for ma_str in self._settings.bootstrap:
            try:
                info = parse_p2p_multiaddr(ma_str)
            except Exception as exc:
                log.warning("internet.bootstrap_parse_failed", addr=ma_str, err=str(exc))
                continue
            try:
                await self._lhost.dial(info)
            except Exception as exc:
                log.warning(
                    "internet.bootstrap_dial_failed",
                    addr=ma_str,
                    err=str(exc),
                )
                continue
            ep = PeerEndpoint(
                transport="internet",
                address=ma_str,
                last_seen=time.time(),
            )
            self._peers[ma_str] = ep
            assert self._discover_send is not None
            try:
                self._discover_send.send_nowait(ep)
            except trio.WouldBlock:
                pass
            log.info("internet.bootstrap_connected", addr=ma_str)

    async def stop(self) -> None:
        if self._discover_send is not None:
            await self._discover_send.aclose()
        self._handler = None

    def peers(self) -> list[PeerEndpoint]:
        return list(self._peers.values())

    async def discover(self) -> AsyncIterator[PeerEndpoint]:
        if self._discover_recv is None:
            raise RuntimeError("InternetTransport not started")
        async for ep in self._discover_recv:
            yield ep

    async def send(self, peer: PeerEndpoint, payload: bytes) -> bytes | None:
        # The `address` field for Internet endpoints is a /p2p/ multiaddr.
        # Parse to PeerID then delegate to the shared host.
        try:
            info = parse_p2p_multiaddr(peer.address)
        except Exception as exc:
            raise TransportError(f"bad multiaddr: {peer.address!r}") from exc
        try:
            return await self._lhost.send(info.peer_id, payload)
        except Exception as exc:
            raise TransportError(str(exc)) from exc

    # ---- Inbound bridging --------------------------------------------

    async def _on_libp2p_inbound(self, peer_id, payload: bytes) -> bytes | None:
        # Both Internet and LAN share the same libp2p host, so they share
        # the inbound handler. We always tag the frame as "internet" here
        # because by the time it lands, libp2p has lost the discovery
        # context (was the peer first seen via mDNS or via bootstrap?).
        # The TransportManager dedupes by DID, so the tag is informational.
        if self._handler is None:
            return None
        ep = PeerEndpoint(
            transport="internet",
            address=f"/p2p/{peer_id.to_base58()}",
            last_seen=time.time(),
        )
        frame = IncomingFrame(transport="internet", peer=ep, payload=payload)
        return await self._handler(frame)
