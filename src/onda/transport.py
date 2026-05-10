"""py-libp2p transport + optional zeroconf mDNS discovery.

Design notes:

* The libp2p host is built from the SAME Ed25519 seed as the node's DID
  identity (see `identity.py`). That means `did:key:z6Mk…` and the libp2p
  PeerID `12D3KooW…` are two encodings of the same public key — a peer's
  reachable libp2p address is, by construction, proof of ownership of its DID.

* Wire framing on a libp2p stream is newline-delimited JSON. JSON because
  the spec asks for JSON-LD-shaped messages; newline-delimited because that
  is the simplest framing that survives chunked stream reads. Hard cap at
  1 MiB per message keeps a hostile peer from exhausting RAM.

* The mDNS layer lives next to libp2p rather than inside it because
  py-libp2p's built-in mDNS has historically been unreliable. Driving
  Zeroconf ourselves keeps discovery debuggable.

* This module is async. The daemon runs the host inside `trio.run(...)`
  (libp2p requires trio). Anything that needs to interoperate with libp2p
  goes through `anyio` to stay backend-agnostic.

If a future v0.2 wants to swap in a different transport, the public
surface here (`LibP2PTransport.start / dial / send / register_handler`) is
the contract to preserve — per the ADD-ONLY discipline, do not delete this
class; add a sibling and switch via `OndaSettings.transport_backend`.
"""

from __future__ import annotations

import socket
from dataclasses import dataclass, field
from typing import Awaitable, Callable

import multiaddr
import trio
from libp2p import new_host
from libp2p.abc import IHost, INetStream
from libp2p.crypto.ed25519 import Ed25519PrivateKey
from libp2p.crypto.keys import KeyPair
from libp2p.peer.id import ID as PeerID
from libp2p.peer.peerinfo import PeerInfo, info_from_p2p_addr
from zeroconf import IPVersion, ServiceBrowser, ServiceInfo, ServiceListener, Zeroconf

from . import __protocol_id__
from .config import OndaSettings
from .identity import Identity
from .log import get_logger
from .protocol import Envelope

log = get_logger(__name__)

_NEWLINE = b"\n"
_MAX_FRAME = 1_048_576  # 1 MiB


# ---- Stream framing helpers ----------------------------------------------


async def _read_line(stream: INetStream) -> bytes:
    buf = bytearray()
    while _NEWLINE not in buf:
        chunk = await stream.read(4096)
        if not chunk:
            break
        buf.extend(chunk)
        if len(buf) > _MAX_FRAME:
            raise ValueError(f"frame exceeds {_MAX_FRAME} bytes")
    line, _, _ = bytes(buf).partition(_NEWLINE)
    return line


async def _write_envelope(stream: INetStream, env: Envelope) -> None:
    await stream.write(env.to_json().encode("utf-8") + _NEWLINE)


# ---- Peer bookkeeping ----------------------------------------------------


@dataclass
class PeerHandle:
    peer_id: PeerID
    multiaddrs: list[str] = field(default_factory=list)
    did: str | None = None  # Filled in after Discovery exchange.
    name: str | None = None


# Handler signature: given an envelope and the peer it came from, optionally
# return a reply envelope written back on the same stream.
EnvelopeHandler = Callable[[Envelope, PeerHandle], Awaitable[Envelope | None]]


# ---- libp2p key adapter --------------------------------------------------


def _libp2p_keypair_from_identity(identity: Identity) -> KeyPair:
    """Build a libp2p Ed25519 KeyPair from our DID seed.

    Both PyNaCl and py-libp2p take the 32-byte Ed25519 seed; we expose this
    as a function so the equivalence between the two views is documented in
    one place.
    """

    priv = Ed25519PrivateKey.from_bytes(identity.seed)
    return KeyPair(priv, priv.get_public_key())


# ---- mDNS via zeroconf ---------------------------------------------------


class _OndaMDNSListener(ServiceListener):
    """Bridge zeroconf service events into a trio memory channel."""

    def __init__(self, send: trio.MemorySendChannel[PeerInfo]) -> None:
        self._send = send

    def _emit(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zc.get_service_info(type_, name)
        if info is None:
            return
        addrs = info.parsed_addresses(IPVersion.V4Only)
        port = info.port or 0
        peer_id_b = info.properties.get(b"peer_id")
        if not peer_id_b:
            return
        try:
            peer_id = PeerID.from_base58(peer_id_b.decode("ascii"))
        except Exception:
            return
        for addr in addrs:
            ma = multiaddr.Multiaddr(f"/ip4/{addr}/tcp/{port}/p2p/{peer_id.to_base58()}")
            try:
                # Schedule into trio from the zeroconf thread.
                trio.from_thread.run_sync(
                    self._send.send_nowait,
                    info_from_p2p_addr(ma),
                )
            except Exception as exc:  # pragma: no cover - best-effort
                log.debug("mdns.dispatch_failed", err=str(exc))

    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:  # noqa: D401
        self._emit(zc, type_, name)

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        self._emit(zc, type_, name)

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        return None


# ---- Main transport ------------------------------------------------------


class LibP2PTransport:
    def __init__(
        self,
        *,
        identity: Identity,
        settings: OndaSettings,
        on_envelope: EnvelopeHandler,
    ) -> None:
        self.identity = identity
        self.settings = settings
        self.on_envelope = on_envelope

        self._host: IHost | None = None
        self._peers: dict[str, PeerHandle] = {}  # keyed by peer_id base58
        self._did_to_peer: dict[str, str] = {}

        # mDNS bits, only set if enable_mdns.
        self._zc: Zeroconf | None = None
        self._svc_info: ServiceInfo | None = None
        self._browser: ServiceBrowser | None = None
        self._mdns_send: trio.MemorySendChannel[PeerInfo] | None = None
        self._mdns_recv: trio.MemoryReceiveChannel[PeerInfo] | None = None

    # ---- Properties --------------------------------------------------

    @property
    def host(self) -> IHost:
        if self._host is None:
            raise RuntimeError("transport not started")
        return self._host

    @property
    def peer_id(self) -> PeerID:
        return self.host.get_id()

    def listen_addrs(self) -> list[str]:
        return [str(a) for a in self.host.get_addrs()]

    def known_peers(self) -> list[PeerHandle]:
        return list(self._peers.values())

    # ---- Lifecycle ---------------------------------------------------

    async def serve(self, *, ready: trio.Event | None = None) -> None:
        """Run the host until cancelled.

        Splits cleanly so the daemon can `await transport.serve()` inside its
        own nursery while running the IPC server in parallel.
        """

        listen = multiaddr.Multiaddr(f"/ip4/{self.settings.host}/tcp/{self.settings.port}")
        kp = _libp2p_keypair_from_identity(self.identity)
        self._host = new_host(key_pair=kp)

        async with self.host.run(listen_addrs=[listen]):
            self.host.set_stream_handler(__protocol_id__, self._handle_stream)

            log.info(
                "libp2p.listening",
                peer_id=self.peer_id.to_base58(),
                addrs=self.listen_addrs(),
            )

            async with trio.open_nursery() as nursery:
                if self.settings.enable_mdns:
                    nursery.start_soon(self._run_mdns)

                # Dial every bootstrap node. Failures are logged, not fatal.
                for ma_str in self.settings.bootstrap:
                    nursery.start_soon(self._dial_bootstrap, ma_str)

                if ready is not None:
                    ready.set()

                await trio.sleep_forever()

    # ---- mDNS --------------------------------------------------------

    async def _run_mdns(self) -> None:
        # Build the service info AFTER the host is up so we can include the
        # real listen port. We register on the local hostname; if hostname
        # resolution fails (containers) we fall back to a generated label.
        send_ch, recv_ch = trio.open_memory_channel[PeerInfo](max_buffer_size=64)
        self._mdns_send = send_ch
        self._mdns_recv = recv_ch

        try:
            host_ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            host_ip = "127.0.0.1"

        svc_name = f"onda-{self.peer_id.to_base58()[:12]}.{self.settings.mdns_service}"
        info = ServiceInfo(
            type_=self.settings.mdns_service,
            name=svc_name,
            addresses=[socket.inet_aton(host_ip)],
            port=self.settings.port,
            properties={
                b"peer_id": self.peer_id.to_base58().encode("ascii"),
                b"did": self.identity.did.encode("ascii"),
                b"name": self.identity.name.encode("utf-8"),
                b"v": b"0.1",
            },
            server=f"{self.peer_id.to_base58()[:16]}.local.",
        )

        zc = Zeroconf(ip_version=IPVersion.V4Only)
        try:
            zc.register_service(info, allow_name_change=True)
        except Exception as exc:  # pragma: no cover
            log.warning("mdns.register_failed", err=str(exc))
            zc.close()
            return

        self._zc = zc
        self._svc_info = info
        listener = _OndaMDNSListener(send_ch)
        self._browser = ServiceBrowser(zc, self.settings.mdns_service, listener)
        log.info("mdns.registered", service=svc_name)

        try:
            async for peer_info in recv_ch:
                if peer_info.peer_id == self.peer_id:
                    continue  # don't dial ourselves
                await self._connect_peer(peer_info, source="mdns")
        finally:
            try:
                zc.unregister_service(info)
            finally:
                zc.close()

    # ---- Dialing -----------------------------------------------------

    async def _dial_bootstrap(self, ma_str: str) -> None:
        try:
            ma = multiaddr.Multiaddr(ma_str)
            info = info_from_p2p_addr(ma)
        except Exception as exc:
            log.warning("bootstrap.parse_failed", addr=ma_str, err=str(exc))
            return
        await self._connect_peer(info, source="bootstrap")

    async def _connect_peer(self, info: PeerInfo, *, source: str) -> None:
        try:
            await self.host.connect(info)
        except Exception as exc:
            log.warning("peer.connect_failed", peer=info.peer_id.to_base58(), err=str(exc))
            return

        handle = self._peers.setdefault(
            info.peer_id.to_base58(),
            PeerHandle(peer_id=info.peer_id, multiaddrs=[str(a) for a in info.addrs]),
        )
        log.info("peer.connected", peer=info.peer_id.to_base58(), source=source)

        # Exchange Discovery so we learn each other's DID and human name.
        try:
            await self._send_discovery(handle)
        except Exception as exc:
            log.warning("peer.discovery_failed", peer=info.peer_id.to_base58(), err=str(exc))

    async def _send_discovery(self, handle: PeerHandle) -> None:
        from .protocol import DiscoveryBody, MessageType  # local import to avoid cycles

        env = Envelope.build(
            identity=self.identity,
            msg_type=MessageType.DISCOVERY,
            body=DiscoveryBody(
                name=self.identity.name,
                libp2p_addrs=self.listen_addrs(),
            ),
        )
        reply = await self._open_and_exchange(handle.peer_id, env)
        if reply is None or not reply.verify():
            return
        if reply.type.value == "Discovery":
            disc = reply.parsed_body()
            assert hasattr(disc, "name")
            handle.did = reply.sender
            handle.name = disc.name  # type: ignore[union-attr]
            self._did_to_peer[reply.sender] = handle.peer_id.to_base58()
            log.info(
                "peer.discovered",
                peer=handle.peer_id.to_base58(),
                did=reply.sender,
                name=handle.name,
            )

    # ---- Public send -------------------------------------------------

    async def send_envelope(
        self, *, recipient_did: str | None, envelope: Envelope
    ) -> Envelope | None:
        """Send `envelope` to a specific peer (by DID) or to one peer if any.

        Returns the response envelope if the peer wrote one back on the same
        stream, else None. Caller is responsible for `verify()`.
        """

        peer_id_b58: str | None = None
        if recipient_did is not None:
            peer_id_b58 = self._did_to_peer.get(recipient_did)
        if peer_id_b58 is None and self._peers:
            # Broadcast-ish: pick the first known peer. The spec marks
            # multi-hop and routing out of scope for v0.1.
            peer_id_b58 = next(iter(self._peers))
        if peer_id_b58 is None:
            log.warning("send.no_peers")
            return None

        handle = self._peers[peer_id_b58]
        return await self._open_and_exchange(handle.peer_id, envelope)

    async def broadcast_envelope(self, envelope: Envelope) -> list[Envelope]:
        """Send to every known peer, gather replies. Returns verified replies only."""

        replies: list[Envelope] = []
        for handle in list(self._peers.values()):
            reply = await self._open_and_exchange(handle.peer_id, envelope)
            if reply is not None and reply.verify():
                replies.append(reply)
        return replies

    async def _open_and_exchange(
        self, peer_id: PeerID, envelope: Envelope
    ) -> Envelope | None:
        try:
            stream = await self.host.new_stream(peer_id, [__protocol_id__])
        except Exception as exc:
            log.warning("stream.open_failed", peer=peer_id.to_base58(), err=str(exc))
            return None

        try:
            await _write_envelope(stream, envelope)
            line = await _read_line(stream)
            if not line:
                return None
            return Envelope.from_json(line)
        except Exception as exc:
            log.warning("stream.exchange_failed", peer=peer_id.to_base58(), err=str(exc))
            return None
        finally:
            try:
                await stream.close()
            except Exception:
                pass

    # ---- Inbound stream handling ------------------------------------

    async def _handle_stream(self, stream: INetStream) -> None:
        peer_id = stream.muxed_conn.peer_id
        handle = self._peers.setdefault(
            peer_id.to_base58(), PeerHandle(peer_id=peer_id)
        )
        try:
            line = await _read_line(stream)
            if not line:
                return
            env = Envelope.from_json(line)

            # Hard invariant: every inter-node message is signed. Reject
            # unsigned or tampered envelopes BEFORE any further processing.
            if not env.verify():
                log.warning("inbound.bad_signature", peer=peer_id.to_base58())
                return

            # First time we see this peer's DID, remember the mapping.
            if handle.did is None and env.sender:
                handle.did = env.sender
                self._did_to_peer[env.sender] = peer_id.to_base58()

            reply = await self.on_envelope(env, handle)
            if reply is not None:
                await _write_envelope(stream, reply)
        except Exception as exc:
            log.warning("inbound.exchange_failed", err=str(exc))
        finally:
            try:
                await stream.close()
            except Exception:
                pass
