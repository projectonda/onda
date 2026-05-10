"""Bluetooth Low Energy transport (custom Onda GATT service).

Two BLE roles are needed for an Onda node:

  * **Peripheral** — advertise the Onda service so peers can find us, and
    accept writes / serve notifications. We use `bless` for this.
  * **Central** — scan for advertising peers, connect, write to their
    inbound characteristic, subscribe to their outbound characteristic.
    We use `bleak` for this.

Both roles run concurrently. On macOS, Linux (BlueZ), and Windows this is
permitted by the OS BLE stack; iOS/Android would need a different approach
(out of scope for v0.2).

GATT service layout (uses our own custom 128-bit UUIDs derived from
"onda" so they don't collide with any SIG-assigned services):

    Service UUID:            6f6e6461-0000-1000-8000-00805f9b34fb
    Inbound  characteristic: 6f6e6461-0001-1000-8000-00805f9b34fb  (Write)
    Outbound characteristic: 6f6e6461-0002-1000-8000-00805f9b34fb  (Notify)
    DID      characteristic: 6f6e6461-0003-1000-8000-00805f9b34fb  (Read)

The DID characteristic lets a central learn a peer's DID *before* the
first signed exchange, which makes peer-DID dedupe stable across
discovery cycles.

Limitations on macOS:
  * BLE permissions prompt fires the first time the daemon advertises.
  * Background advertising restrictions apply if the app is not in focus.
  * Real testing requires two physical devices.

The transport falls back to `is_available() = False` if `bleak` and
`bless` cannot both be imported, so a server without a Bluetooth radio
won't ever start it.
"""

from __future__ import annotations

import platform
import struct
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import trio

from ..config import OndaSettings
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
    TransportUnavailable,
)
from ._ble_framing import DEFAULT_FRAGMENT_MAX, Fragmenter, Reassembler

log = get_logger(__name__)


# ---- Onda GATT UUIDs ----------------------------------------------------
#
# The base "6f6e6461-…-00805f9b34fb" hex starts with "onda" in ASCII (6f 6e
# 64 61); the suffix is the standard SIG base UUID so off-the-shelf BLE
# scanners render the four bytes nicely as the service identifier.

ONDA_SERVICE_UUID = "6f6e6461-0000-1000-8000-00805f9b34fb"
ONDA_INBOUND_CHAR_UUID = "6f6e6461-0001-1000-8000-00805f9b34fb"
ONDA_OUTBOUND_CHAR_UUID = "6f6e6461-0002-1000-8000-00805f9b34fb"
ONDA_DID_CHAR_UUID = "6f6e6461-0003-1000-8000-00805f9b34fb"

# Manufacturer ID we use in advertising data so a peer's brief scan can
# tell "this is an Onda node" before resolving the full GATT service.
# 0xFFFF is reserved-for-testing per the Bluetooth SIG manufacturer list,
# which is exactly the right semantics for v0.2 ("not a real product").
ONDA_MFG_ID = 0xFFFF


# ---- Soft imports (so a no-Bluetooth box still imports the module) -----


def _import_ble() -> tuple[object, object]:
    """Import bleak + bless lazily, raising TransportUnavailable on failure.

    We don't import these at module top so a server without Bluetooth (or
    without these wheels installed) can still `import onda.transports`.
    """

    try:
        import bleak  # noqa: F401
        import bless  # noqa: F401
    except ImportError as exc:
        raise TransportUnavailable(f"bleak/bless not installed: {exc}") from exc
    return bleak, bless


# ---- Transport ---------------------------------------------------------


@dataclass
class _RemotePeer:
    """A central-side connection to a peer's GATT server."""

    address: str
    client: object  # bleak.BleakClient
    did: str | None = None
    name: str | None = None
    last_seen: float = field(default_factory=time.time)
    fragmenter: Fragmenter = field(default_factory=Fragmenter)


class BluetoothTransport(Transport):
    name: TransportName = "bluetooth"
    priority: int = TransportPriority.BLUETOOTH

    def __init__(self, *, identity: Identity, settings: OndaSettings) -> None:
        self.identity = identity
        self.settings = settings

        self._handler: FrameHandler | None = None
        self._peers: dict[str, _RemotePeer] = {}  # keyed by BLE address
        self._reassembler = Reassembler()
        self._discover_send: trio.MemorySendChannel[PeerEndpoint] | None = None
        self._discover_recv: trio.MemoryReceiveChannel[PeerEndpoint] | None = None

        # Filled in `start()` once we successfully import bleak/bless and
        # bring up the peripheral role.
        self._server: object | None = None  # bless.BlessServer
        self._scanner: object | None = None  # bleak.BleakScanner
        self._nursery: trio.Nursery | None = None
        self._mtu_payload: int = DEFAULT_FRAGMENT_MAX

    # ---- Capability check -------------------------------------------

    async def is_available(self) -> bool:
        try:
            _import_ble()
        except TransportUnavailable:
            return False
        # BLE on macOS without a Bluetooth radio still registers PyObjC
        # frameworks, so we can't be 100% sure here; the truthful check
        # happens when we try to start the peripheral. We return True for
        # any system where the libs imported OK and let start() fail
        # gracefully (which the manager catches).
        if platform.system() not in ("Darwin", "Linux", "Windows"):
            return False
        return True

    # ---- Lifecycle --------------------------------------------------

    async def start(self, on_frame: FrameHandler) -> None:
        if self._handler is not None:
            raise RuntimeError("BluetoothTransport already started")
        self._handler = on_frame
        d_send, d_recv = trio.open_memory_channel[PeerEndpoint](max_buffer_size=64)
        self._discover_send = d_send
        self._discover_recv = d_recv
        # Bringing up the BLE peripheral is platform-specific and async.
        # We defer it to `serve()` which the Node spawns into its nursery.

    async def serve(self) -> None:
        """Bring up advertising + scanning. Runs until cancelled."""

        try:
            bleak_mod, bless_mod = _import_ble()  # type: ignore[misc]
        except TransportUnavailable as exc:
            log.warning("bluetooth.unavailable", err=str(exc))
            return

        # ---- Peripheral side -----------------------------------------
        BlessServer = bless_mod.BlessServer  # type: ignore[attr-defined]
        BlessGATTCharacteristicProperties = bless_mod.GATTCharacteristicProperties  # type: ignore[attr-defined]
        GATTAttributePermissions = bless_mod.GATTAttributePermissions  # type: ignore[attr-defined]

        srv_name = f"onda-{self.identity.did[-12:]}"
        server = BlessServer(name=srv_name)
        server.read_request_func = self._on_read_request
        server.write_request_func = self._on_write_request

        await server.add_new_service(ONDA_SERVICE_UUID)
        # Inbound: peers WRITE here; we receive a fragmented Onda payload.
        await server.add_new_characteristic(
            service_uuid=ONDA_SERVICE_UUID,
            char_uuid=ONDA_INBOUND_CHAR_UUID,
            properties=BlessGATTCharacteristicProperties.write,
            value=None,
            permissions=GATTAttributePermissions.writeable,
        )
        # Outbound: we NOTIFY here for replies and unsolicited frames.
        await server.add_new_characteristic(
            service_uuid=ONDA_SERVICE_UUID,
            char_uuid=ONDA_OUTBOUND_CHAR_UUID,
            properties=(
                BlessGATTCharacteristicProperties.notify
                | BlessGATTCharacteristicProperties.read
            ),
            value=b"",
            permissions=GATTAttributePermissions.readable,
        )
        # DID: read-only advertisement of our DID so a central can dedupe.
        await server.add_new_characteristic(
            service_uuid=ONDA_SERVICE_UUID,
            char_uuid=ONDA_DID_CHAR_UUID,
            properties=BlessGATTCharacteristicProperties.read,
            value=self.identity.did.encode("utf-8"),
            permissions=GATTAttributePermissions.readable,
        )
        try:
            await server.start()
        except Exception as exc:
            log.warning("bluetooth.peripheral_start_failed", err=str(exc))
            return
        self._server = server
        log.info("bluetooth.advertising", name=srv_name, service=ONDA_SERVICE_UUID)

        # ---- Central side: scan and dial advertising peers -----------
        BleakScanner = bleak_mod.BleakScanner  # type: ignore[attr-defined]
        BleakClient = bleak_mod.BleakClient  # type: ignore[attr-defined]

        async def detection_callback(device, advertisement_data) -> None:
            uuids = (advertisement_data.service_uuids or [])
            if ONDA_SERVICE_UUID.lower() not in [u.lower() for u in uuids]:
                return
            addr = device.address
            if addr in self._peers:
                self._peers[addr].last_seen = time.time()
                return
            await self._connect_central(addr, BleakClient)

        scanner = BleakScanner(detection_callback=detection_callback)
        try:
            await scanner.start()
        except Exception as exc:
            log.warning("bluetooth.scanner_start_failed", err=str(exc))
            await server.stop()
            return
        self._scanner = scanner
        log.info("bluetooth.scanning")

        try:
            await trio.sleep_forever()
        finally:
            try:
                await scanner.stop()
            except Exception:
                pass
            try:
                await server.stop()
            except Exception:
                pass

    async def stop(self) -> None:
        if self._discover_send is not None:
            await self._discover_send.aclose()
        # Disconnect any open central connections.
        for peer in list(self._peers.values()):
            try:
                await peer.client.disconnect()  # type: ignore[attr-defined]
            except Exception:
                pass
        self._peers.clear()
        self._handler = None

    # ---- Discovery / send (Transport API) ---------------------------

    def peers(self) -> list[PeerEndpoint]:
        return [
            PeerEndpoint(
                transport="bluetooth",
                address=p.address,
                did=p.did,
                name=p.name,
                last_seen=p.last_seen,
            )
            for p in self._peers.values()
        ]

    async def discover(self) -> AsyncIterator[PeerEndpoint]:
        if self._discover_recv is None:
            raise RuntimeError("BluetoothTransport not started")
        async for ep in self._discover_recv:
            yield ep

    async def send(self, peer: PeerEndpoint, payload: bytes) -> bytes | None:
        rp = self._peers.get(peer.address)
        if rp is None:
            raise TransportError(f"no central connection to {peer.address}")
        # Fragment the payload and write each fragment. The reply comes
        # back via the OUTBOUND notify subscription registered at connect.
        frags = rp.fragmenter.fragment(payload)
        try:
            for frag in frags:
                await rp.client.write_gatt_char(  # type: ignore[attr-defined]
                    ONDA_INBOUND_CHAR_UUID,
                    frag.encode(),
                    response=True,
                )
        except Exception as exc:
            raise TransportError(f"BLE write failed: {exc}") from exc
        # Replies arrive asynchronously via _on_outbound_notify and feed
        # the manager handler directly. send() returns None to signal
        # "fire-and-forget at this layer."
        return None

    # ---- Central role internals -------------------------------------

    async def _connect_central(self, addr: str, BleakClient) -> None:
        client = BleakClient(addr)
        try:
            await client.connect()
        except Exception as exc:
            log.warning("bluetooth.connect_failed", addr=addr, err=str(exc))
            return
        try:
            did_bytes = await client.read_gatt_char(ONDA_DID_CHAR_UUID)
            did = did_bytes.decode("utf-8") if did_bytes else None
        except Exception:
            did = None
        try:
            await client.start_notify(
                ONDA_OUTBOUND_CHAR_UUID,
                lambda _ch, data: trio.from_thread.run_sync(
                    self._on_outbound_notify, addr, bytes(data)
                ),
            )
        except Exception as exc:
            log.warning("bluetooth.notify_subscribe_failed", addr=addr, err=str(exc))
            await client.disconnect()
            return

        rp = _RemotePeer(address=addr, client=client, did=did)
        self._peers[addr] = rp
        ep = PeerEndpoint(
            transport="bluetooth",
            address=addr,
            did=did,
            last_seen=rp.last_seen,
        )
        if self._discover_send is not None:
            try:
                self._discover_send.send_nowait(ep)
            except trio.WouldBlock:
                pass
        log.info("bluetooth.connected", addr=addr, did=did)

    def _on_outbound_notify(self, addr: str, data: bytes) -> None:
        # This is a notification on the OUTBOUND char; the peer (acting
        # as peripheral) is sending us a reply or unsolicited frame.
        assembled = self._reassembler.feed(addr, data)
        if assembled is None:
            return
        # Build an IncomingFrame and dispatch to the manager handler.
        if self._handler is None:
            return
        rp = self._peers.get(addr)
        ep = PeerEndpoint(
            transport="bluetooth",
            address=addr,
            did=rp.did if rp else None,
            name=rp.name if rp else None,
            last_seen=time.time(),
        )
        frame = IncomingFrame(transport="bluetooth", peer=ep, payload=assembled)
        # We're called from a non-trio thread (bleak callback). Dispatch
        # back into the trio loop in a fire-and-forget fashion.
        try:
            trio.from_thread.run(self._handler, frame)
        except Exception as exc:
            log.warning("bluetooth.dispatch_failed", err=str(exc))

    # ---- Peripheral role internals ----------------------------------

    def _on_read_request(self, characteristic, **_) -> bytes:
        # Bless calls this synchronously to satisfy a GATT read.
        if str(characteristic.uuid).lower() == ONDA_DID_CHAR_UUID.lower():
            return self.identity.did.encode("utf-8")
        return b""

    def _on_write_request(self, characteristic, value: bytes, **_) -> None:
        # Bless calls this synchronously when a central writes a fragment
        # to our INBOUND characteristic. We feed the reassembler; once a
        # complete payload arrives, we forward it to the manager handler
        # via trio.from_thread.run.
        if str(characteristic.uuid).lower() != ONDA_INBOUND_CHAR_UUID.lower():
            return
        # We don't know the remote address here because bless's cross-
        # platform API doesn't surface it on every backend. As a workaround
        # we use the characteristic UUID + a single shared reassembly key;
        # this is fine for v0.2 because GATT writes are serialized per
        # connection at the BLE link layer. A future v0.3 BLE upgrade can
        # carry the address explicitly via a header byte.
        peer_key = "<peripheral-write>"
        assembled = self._reassembler.feed(peer_key, value)
        if assembled is None:
            return
        if self._handler is None:
            return
        ep = PeerEndpoint(
            transport="bluetooth",
            address=peer_key,
            last_seen=time.time(),
        )
        frame = IncomingFrame(transport="bluetooth", peer=ep, payload=assembled)
        try:
            reply = trio.from_thread.run(self._handler, frame)
        except Exception as exc:
            log.warning("bluetooth.peripheral_dispatch_failed", err=str(exc))
            return
        if reply is None or not isinstance(reply, (bytes, bytearray)):
            return
        # Send the reply back via OUTBOUND notify, fragmenting if needed.
        # For the peripheral side we use a per-peer fragmenter on a single
        # counter (good enough until v0.3 multi-central support).
        # Note: bless lets us update the characteristic value, which then
        # gets delivered as a notification to subscribed centrals.
        try:
            for frag in Fragmenter(self._mtu_payload).fragment(bytes(reply)):
                trio.from_thread.run_sync(
                    self._notify_outbound, frag.encode()
                )
        except Exception as exc:
            log.warning("bluetooth.peripheral_reply_failed", err=str(exc))

    def _notify_outbound(self, payload: bytes) -> None:
        if self._server is None:
            return
        try:
            char = self._server.get_characteristic(ONDA_OUTBOUND_CHAR_UUID)  # type: ignore[attr-defined]
            char.value = payload  # type: ignore[attr-defined]
            self._server.update_value(ONDA_SERVICE_UUID, ONDA_OUTBOUND_CHAR_UUID)  # type: ignore[attr-defined]
        except Exception as exc:
            log.warning("bluetooth.notify_failed", err=str(exc))


__all__ = [
    "BluetoothTransport",
    "ONDA_SERVICE_UUID",
    "ONDA_INBOUND_CHAR_UUID",
    "ONDA_OUTBOUND_CHAR_UUID",
    "ONDA_DID_CHAR_UUID",
]
