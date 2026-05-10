"""Shared libp2p host used by both InternetTransport and LanTransport.

Why a shared object:

  * Two libp2p hosts on the same machine would each need a separate TCP
    port. A v0.2 node would therefore listen on TWO ports just to express
    the "internet" and "lan" distinction — wasteful and confusing.
  * Once a peer is connected over libp2p, the transport-of-record (internet
    vs LAN) is irrelevant to the byte path. The distinction only matters
    for *discovery*, which is what the two Transport classes specialize.
  * It also keeps the v0.1 `LibP2PTransport` (in src/onda/transport.py)
    untouched per the ADD-ONLY discipline.

The split of concerns:

    Libp2pHost (this file)        — owns the host, opens streams, handles inbound
    InternetTransport             — bootstrap discovery + send via host
    LanTransport                  — zeroconf mDNS discovery + send via host
"""

from __future__ import annotations

import time
from typing import Any, Awaitable, Callable

import multiaddr
import trio
from libp2p import new_host
from libp2p.abc import IHost, INetStream
from libp2p.crypto.ed25519 import Ed25519PrivateKey
from libp2p.crypto.keys import KeyPair
from libp2p.peer.id import ID as PeerID
from libp2p.peer.peerinfo import PeerInfo, info_from_p2p_addr

from .. import __protocol_id__
from ..identity import Identity
from ..log import get_logger

log = get_logger(__name__)

_NEWLINE = b"\n"
_MAX_FRAME = 1_048_576


# Inbound handler: takes the sender's PeerID and the raw bytes; returns an
# optional reply that the host writes back on the same stream.
InboundHandler = Callable[[PeerID, bytes], Awaitable[bytes | None]]


def _libp2p_keypair_from_identity(identity: Identity) -> KeyPair:
    priv = Ed25519PrivateKey.from_bytes(identity.seed)
    return KeyPair(priv, priv.get_public_key())


class Libp2pHost:
    """One libp2p host shared between Internet and LAN transports.

    Lifecycle is driven by `serve()`: a single trio task that takes the
    host through `host.run(listen_addrs=…)` for the duration of the node.
    Other transports register themselves via `set_inbound_handler` and
    observe `host_ready` to know when they can start dialing.
    """

    def __init__(self, *, identity: Identity, host_addr: str, port: int) -> None:
        self.identity = identity
        self.host_addr = host_addr
        self.port = port
        self._host: IHost | None = None
        self._inbound_handler: InboundHandler | None = None
        self.host_ready = trio.Event()

    # ---- Properties --------------------------------------------------

    @property
    def host(self) -> IHost:
        if self._host is None:
            raise RuntimeError("Libp2pHost not started")
        return self._host

    @property
    def peer_id(self) -> PeerID:
        return self.host.get_id()

    def listen_addrs(self) -> list[str]:
        return [str(a) for a in self.host.get_addrs()]

    # ---- Wiring ------------------------------------------------------

    def set_inbound_handler(self, handler: InboundHandler) -> None:
        # Idempotent within a node: both internet+lan call this with the
        # SAME manager-supplied handler. Last writer wins, but that's fine
        # because the handler comes from one place (the TransportManager).
        self._inbound_handler = handler

    # ---- Lifecycle ---------------------------------------------------

    async def serve(self) -> None:
        listen = multiaddr.Multiaddr(f"/ip4/{self.host_addr}/tcp/{self.port}")
        kp = _libp2p_keypair_from_identity(self.identity)
        self._host = new_host(key_pair=kp)

        async with self.host.run(listen_addrs=[listen]):
            self.host.set_stream_handler(__protocol_id__, self._handle_stream)
            log.info(
                "libp2p_host.listening",
                peer_id=self.peer_id.to_base58(),
                addrs=self.listen_addrs(),
            )
            self.host_ready.set()
            await trio.sleep_forever()

    # ---- Send --------------------------------------------------------

    async def dial(self, info: PeerInfo) -> None:
        await self.host.connect(info)

    async def send(self, peer_id: PeerID, payload: bytes) -> bytes | None:
        try:
            stream = await self.host.new_stream(peer_id, [__protocol_id__])
        except Exception as exc:
            log.warning("libp2p_host.stream_open_failed", peer=peer_id.to_base58(), err=str(exc))
            raise

        try:
            await stream.write(payload + _NEWLINE)
            line = await self._read_line(stream)
            if not line:
                return None
            return line
        finally:
            try:
                await stream.close()
            except Exception:
                pass

    @staticmethod
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

    # ---- Inbound -----------------------------------------------------

    async def _handle_stream(self, stream: INetStream) -> None:
        peer_id = stream.muxed_conn.peer_id
        try:
            line = await self._read_line(stream)
            if not line:
                return
            handler = self._inbound_handler
            if handler is None:
                log.debug("libp2p_host.no_handler_yet", peer=peer_id.to_base58())
                return
            reply = await handler(peer_id, bytes(line))
            if reply is not None:
                await stream.write(reply + _NEWLINE)
        except Exception as exc:
            log.warning("libp2p_host.inbound_failed", peer=peer_id.to_base58(), err=str(exc))
        finally:
            try:
                await stream.close()
            except Exception:
                pass


# ---- Helpers for transports ---------------------------------------------


def parse_p2p_multiaddr(ma_str: str) -> PeerInfo:
    """Parse a `/ip4/…/tcp/…/p2p/…` multiaddr to a PeerInfo, raising on bad input."""

    return info_from_p2p_addr(multiaddr.Multiaddr(ma_str))


__all__ = ["InboundHandler", "Libp2pHost", "parse_p2p_multiaddr"]
