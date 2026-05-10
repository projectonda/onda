"""LAN transport: libp2p over the same shared host, discovered via zeroconf mDNS.

Driving rationale:

  * Local discovery should "just work" without any out-of-band copy-paste
    of multiaddrs. mDNS / Zeroconf / Bonjour is the standard answer on
    every desktop OS.
  * py-libp2p's built-in mDNS has been historically fragile, so we drive
    Zeroconf ourselves. The TXT record advertises `peer_id` and `did` so
    a peer can be addressed before the first signed envelope arrives.
  * Once mDNS finds a peer, sending happens over the SAME libp2p host as
    the InternetTransport. From the byte-path point of view there's no
    distinction; the split here is purely about discovery semantics.
"""

from __future__ import annotations

import socket
import time
from collections.abc import AsyncIterator

import multiaddr
import trio
from libp2p.peer.id import ID as PeerID
from libp2p.peer.peerinfo import info_from_p2p_addr
from zeroconf import (
    InterfaceChoice,
    IPVersion,
    ServiceBrowser,
    ServiceInfo,
    ServiceListener,
    Zeroconf,
)

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
    TransportUnavailable,
)
from ._libp2p_shared import Libp2pHost

log = get_logger(__name__)


class _MDNSListener(ServiceListener):
    """Zeroconf listener that pushes parsed PeerInfos onto a trio channel.

    Zeroconf invokes these callbacks from its OWN (non-trio) thread. To
    cross into trio safely we use `trio.from_thread.run_sync` with an
    explicit `trio_token` captured by the caller at start-up time. Without
    that token the bridge silently raises `RuntimeError("this thread isn't
    associated with any trio run")`, which historically was caught and
    swallowed — leading to "mDNS sees nothing" in same-host smoke tests.
    """

    def __init__(self, send: trio.MemorySendChannel, trio_token: trio.lowlevel.TrioToken) -> None:
        self._send = send
        self._token = trio_token

    def _emit(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zc.get_service_info(type_, name)
        if info is None:
            return
        addrs = info.parsed_addresses(IPVersion.V4Only)
        port = info.port or 0
        peer_id_b = info.properties.get(b"peer_id")
        did_b = info.properties.get(b"did")
        peer_name_b = info.properties.get(b"name")
        if not peer_id_b or not addrs:
            return
        peer_id_str = peer_id_b.decode("ascii")
        did = did_b.decode("ascii") if did_b else None
        peer_name = peer_name_b.decode("utf-8") if peer_name_b else None

        for addr in addrs:
            ma = multiaddr.Multiaddr(f"/ip4/{addr}/tcp/{port}/p2p/{peer_id_str}")
            ep = PeerEndpoint(
                transport="lan",
                address=str(ma),
                did=did,
                name=peer_name,
                last_seen=time.time(),
                metadata={"mdns_service_name": name},
            )
            try:
                trio.from_thread.run_sync(
                    self._send.send_nowait, ep, trio_token=self._token
                )
            except Exception as exc:  # pragma: no cover - best-effort
                log.warning("lan.mdns_dispatch_failed", err=str(exc))

    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        self._emit(zc, type_, name)

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        self._emit(zc, type_, name)

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        return None


class LanTransport(Transport):
    name: TransportName = "lan"
    priority: int = TransportPriority.LAN

    def __init__(self, *, host: Libp2pHost, settings: OndaSettings) -> None:
        self._lhost = host
        self._settings = settings
        self._handler: FrameHandler | None = None
        self._peers: dict[str, PeerEndpoint] = {}  # keyed by multiaddr
        self._zc: Zeroconf | None = None
        self._svc: ServiceInfo | None = None
        self._browser: ServiceBrowser | None = None
        self._discover_send: trio.MemorySendChannel[PeerEndpoint] | None = None
        self._discover_recv: trio.MemoryReceiveChannel[PeerEndpoint] | None = None
        self._mdns_internal_send: trio.MemorySendChannel | None = None
        self._mdns_internal_recv: trio.MemoryReceiveChannel | None = None

    async def is_available(self) -> bool:
        if not self._settings.enable_mdns:
            return False
        # On a totally networkless host (e.g. CI in a sandbox), gethostbyname
        # for the local hostname can fail. Treat that as "no LAN" rather
        # than blowing up at start time.
        try:
            socket.gethostbyname(socket.gethostname())
            return True
        except Exception:
            return False

    async def start(self, on_frame: FrameHandler) -> None:
        if self._handler is not None:
            raise RuntimeError("LanTransport already started")
        self._handler = on_frame
        d_send, d_recv = trio.open_memory_channel[PeerEndpoint](max_buffer_size=64)
        m_send, m_recv = trio.open_memory_channel(max_buffer_size=64)
        self._discover_send = d_send
        self._discover_recv = d_recv
        self._mdns_internal_send = m_send
        self._mdns_internal_recv = m_recv
        self._lhost.set_inbound_handler(self._on_libp2p_inbound)
        # The Node spawns `serve()` separately so registration waits for the
        # underlying libp2p host to be listening.

    async def serve(self) -> None:
        await self._lhost.host_ready.wait()
        try:
            host_ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            host_ip = "127.0.0.1"

        peer_id_str = self._lhost.peer_id.to_base58()
        svc_name = f"onda-{peer_id_str[:12]}.{self._settings.mdns_service}"
        info = ServiceInfo(
            type_=self._settings.mdns_service,
            name=svc_name,
            addresses=[socket.inet_aton(host_ip)],
            port=self._settings.port,
            properties={
                b"peer_id": peer_id_str.encode("ascii"),
                b"did": self._lhost.identity.did.encode("ascii"),
                b"name": self._lhost.identity.name.encode("utf-8"),
                b"v": b"0.2",
            },
            server=f"{peer_id_str[:16]}.local.",
        )

        # InterfaceChoice.All includes the loopback interface, which makes
        # two Onda daemons on the SAME host see each other via mDNS. The
        # default (primary interface only) skips loopback and breaks
        # same-host smoke tests; on real two-device LANs both choices work.
        zc = Zeroconf(ip_version=IPVersion.V4Only, interfaces=InterfaceChoice.All)
        try:
            zc.register_service(info, allow_name_change=True)
        except Exception as exc:
            log.warning("lan.mdns_register_failed", err=str(exc))
            zc.close()
            raise TransportUnavailable(str(exc)) from exc

        self._zc = zc
        self._svc = info
        # Capture the trio token from the current task so the zeroconf
        # callback thread (which runs on its own pthread, not in our
        # nursery) can hand parsed PeerInfos back into trio safely.
        trio_token = trio.lowlevel.current_trio_token()
        listener = _MDNSListener(self._mdns_internal_send, trio_token)
        self._browser = ServiceBrowser(zc, self._settings.mdns_service, listener)
        log.info("lan.mdns_registered", service=svc_name)

        try:
            assert self._mdns_internal_recv is not None
            assert self._discover_send is not None
            async for ep in self._mdns_internal_recv:
                # Don't dial ourselves.
                if peer_id_str in ep.address:
                    continue
                if ep.address in self._peers:
                    self._peers[ep.address].merge(ep)
                    continue
                # New peer — dial via shared host, then announce on the
                # transport's discover() stream.
                try:
                    info = info_from_p2p_addr(multiaddr.Multiaddr(ep.address))
                    await self._lhost.dial(info)
                except Exception as exc:
                    log.warning(
                        "lan.dial_failed",
                        addr=ep.address,
                        err=str(exc),
                    )
                    continue
                self._peers[ep.address] = ep
                try:
                    self._discover_send.send_nowait(ep)
                except trio.WouldBlock:
                    pass
                log.info("lan.peer_found", addr=ep.address, did=ep.did)
        finally:
            try:
                zc.unregister_service(info)
            finally:
                zc.close()

    async def stop(self) -> None:
        if self._discover_send is not None:
            await self._discover_send.aclose()
        if self._mdns_internal_send is not None:
            await self._mdns_internal_send.aclose()
        # zc.close() already happens at the end of serve(); double-close is OK.
        if self._zc is not None:
            try:
                self._zc.close()
            except Exception:
                pass
        self._handler = None

    def peers(self) -> list[PeerEndpoint]:
        return list(self._peers.values())

    async def discover(self) -> AsyncIterator[PeerEndpoint]:
        if self._discover_recv is None:
            raise RuntimeError("LanTransport not started")
        async for ep in self._discover_recv:
            yield ep

    async def send(self, peer: PeerEndpoint, payload: bytes) -> bytes | None:
        try:
            info = info_from_p2p_addr(multiaddr.Multiaddr(peer.address))
        except Exception as exc:
            raise TransportError(f"bad multiaddr: {peer.address!r}") from exc
        try:
            return await self._lhost.send(info.peer_id, payload)
        except Exception as exc:
            raise TransportError(str(exc)) from exc

    # ---- Inbound bridging --------------------------------------------

    async def _on_libp2p_inbound(self, peer_id: PeerID, payload: bytes) -> bytes | None:
        # See InternetTransport: we route via the same libp2p host so the
        # tag here is informational only. We label the frame "lan" because
        # if the peer was discovered via mDNS, that's the recorded source.
        if self._handler is None:
            return None
        ep = PeerEndpoint(
            transport="lan",
            address=f"/p2p/{peer_id.to_base58()}",
            last_seen=time.time(),
        )
        frame = IncomingFrame(transport="lan", peer=ep, payload=payload)
        return await self._handler(frame)
