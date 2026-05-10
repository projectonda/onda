"""Wi-Fi Direct transport: SSID-based peer discovery + libp2p over the link.

Cross-platform reality check (this is why the module is generously
commented):

  * `pywifi` works decently on Linux and Windows for the **scan / connect**
    operations we need. On Linux it talks to `wpa_supplicant`; on Windows
    it uses the WLAN API.
  * `pywifi` does NOT do real peer-to-peer Wi-Fi Direct on macOS. Apple's
    user-space lacks programmatic Wi-Fi Direct. We honestly return
    `is_available() = False` on Darwin so the manager skips us.
  * Even on Linux, *creating* a hotspot programmatically requires either
    `nmcli` or a custom `wpa_supplicant.conf` and is very distro-specific.
    v0.2 therefore ships **scan-only** by default: the transport finds
    Onda-prefixed SSIDs (presumed to be peer hotspots created out of band)
    and treats them as available networks to join. Hosting a hotspot is
    documented as a manual step in `docs/transports.md`.

Once joined to an Onda Wi-Fi Direct network, this transport delegates the
byte path to the same `Libp2pHost` used by the Internet and LAN
transports. So in effect: Wi-Fi Direct = "discovery via SSID convention +
LAN-style libp2p over an ad-hoc link."

ADD-ONLY note: a future v0.3 may add a `WifiDirectHostTransport` that
brings up the AP. We deliberately DON'T put hotspot creation here so this
file stays small and platform-portable.
"""

from __future__ import annotations

import platform
import re
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
    TransportUnavailable,
)
from ._libp2p_shared import Libp2pHost

log = get_logger(__name__)


# By convention, every Onda node hosting a Wi-Fi Direct AP names it
# `Onda-<peer_id_prefix>`. We scan for this exact prefix.
ONDA_SSID_PREFIX = "Onda-"
# Inversely, when this transport hosts (future), it will use the same
# convention so other nodes can find it.

_SSID_REGEX = re.compile(r"^Onda-[0-9A-Za-z]{4,}$")


def _import_pywifi() -> object:
    try:
        import pywifi  # noqa: F401
    except ImportError as exc:
        raise TransportUnavailable(f"pywifi not installed: {exc}") from exc
    return pywifi


class WifiDirectTransport(Transport):
    name: TransportName = "wifi_direct"
    priority: int = TransportPriority.WIFI_DIRECT

    def __init__(
        self,
        *,
        host: Libp2pHost,
        settings: OndaSettings,
        scan_interval_s: float = 30.0,
    ) -> None:
        self._lhost = host
        self._settings = settings
        self.scan_interval_s = scan_interval_s

        self._handler: FrameHandler | None = None
        self._peers: dict[str, PeerEndpoint] = {}  # keyed by SSID
        self._discover_send: trio.MemorySendChannel[PeerEndpoint] | None = None
        self._discover_recv: trio.MemoryReceiveChannel[PeerEndpoint] | None = None
        self._iface: object | None = None  # pywifi Interface

    # ---- Capability check -------------------------------------------

    async def is_available(self) -> bool:
        # Honest about platform support. is_available() is also a "safe
        # probe" — it must never raise, so we treat any exception while
        # importing or probing pywifi as "this transport isn't usable here."
        if platform.system() == "Darwin":
            log.info("wifi_direct.unavailable_on_darwin")
            return False
        try:
            pywifi = _import_pywifi()
            wifi = pywifi.PyWiFi()  # type: ignore[attr-defined]
            ifaces = wifi.interfaces()
            if not ifaces:
                return False
            self._iface = ifaces[0]
            return True
        except Exception as exc:
            log.info("wifi_direct.probe_failed", err=str(exc))
            return False

    # ---- Lifecycle --------------------------------------------------

    async def start(self, on_frame: FrameHandler) -> None:
        if self._handler is not None:
            raise RuntimeError("WifiDirectTransport already started")
        self._handler = on_frame
        send, recv = trio.open_memory_channel[PeerEndpoint](max_buffer_size=64)
        self._discover_send = send
        self._discover_recv = recv
        self._lhost.set_inbound_handler(self._on_libp2p_inbound)

    async def serve(self) -> None:
        """Periodic SSID scan loop. Runs until cancelled."""

        if self._iface is None:
            return
        while True:
            try:
                ssids = await self._scan_once()
            except Exception as exc:
                log.warning("wifi_direct.scan_failed", err=str(exc))
                ssids = []
            for ssid in ssids:
                if ssid in self._peers:
                    self._peers[ssid].last_seen = time.time()
                    continue
                ep = PeerEndpoint(
                    transport="wifi_direct",
                    address=ssid,
                    last_seen=time.time(),
                    metadata={"ssid": ssid},
                )
                self._peers[ssid] = ep
                if self._discover_send is not None:
                    try:
                        self._discover_send.send_nowait(ep)
                    except trio.WouldBlock:
                        pass
                log.info("wifi_direct.peer_ssid_seen", ssid=ssid)
            await trio.sleep(self.scan_interval_s)

    async def _scan_once(self) -> list[str]:
        """Trigger an interface scan and return matching Onda SSIDs.

        pywifi's scan API is synchronous and blocks, so we run it in a
        thread to keep trio's loop responsive.
        """

        iface = self._iface

        def do_scan() -> list[str]:
            iface.scan()  # type: ignore[union-attr]
            time.sleep(2)  # pywifi's scan_results need a moment to populate
            results = iface.scan_results()  # type: ignore[union-attr]
            return [r.ssid for r in results if _SSID_REGEX.match(getattr(r, "ssid", ""))]

        return await trio.to_thread.run_sync(do_scan)

    async def stop(self) -> None:
        if self._discover_send is not None:
            await self._discover_send.aclose()
        self._handler = None

    # ---- Discovery / send (Transport API) ---------------------------

    def peers(self) -> list[PeerEndpoint]:
        return list(self._peers.values())

    async def discover(self) -> AsyncIterator[PeerEndpoint]:
        if self._discover_recv is None:
            raise RuntimeError("WifiDirectTransport not started")
        async for ep in self._discover_recv:
            yield ep

    async def send(self, peer: PeerEndpoint, payload: bytes) -> bytes | None:
        # This transport assumes that the OS is already joined to the
        # peer's Wi-Fi Direct network OR that the manager is calling us
        # only after the peer's libp2p multiaddr has been learned via
        # another channel. Without that join, sending falls through.
        ma = peer.metadata.get("multiaddr") if peer.metadata else None
        if not ma:
            raise TransportError(
                f"wifi_direct: no multiaddr for SSID {peer.address}; "
                "associate first then re-discover"
            )
        from ._libp2p_shared import parse_p2p_multiaddr
        try:
            info = parse_p2p_multiaddr(ma)
        except Exception as exc:
            raise TransportError(f"bad multiaddr: {ma!r}") from exc
        try:
            return await self._lhost.send(info.peer_id, payload)
        except Exception as exc:
            raise TransportError(str(exc)) from exc

    # ---- Inbound bridging --------------------------------------------

    async def _on_libp2p_inbound(self, peer_id, payload: bytes) -> bytes | None:
        if self._handler is None:
            return None
        ep = PeerEndpoint(
            transport="wifi_direct",
            address=f"/p2p/{peer_id.to_base58()}",
            last_seen=time.time(),
        )
        frame = IncomingFrame(transport="wifi_direct", peer=ep, payload=payload)
        return await self._handler(frame)


__all__ = ["WifiDirectTransport", "ONDA_SSID_PREFIX"]
