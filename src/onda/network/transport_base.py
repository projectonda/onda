"""Transport abstraction shared by every concrete v0.2 transport.

Why this layer exists:

  * Onda's wire-level guarantees (DID-signed envelopes, optional NaCl-box
    encryption, JSON-LD shape) are transport-agnostic. The v0.2 protocol
    therefore needs an interface that doesn't leak libp2p, BLE, or Wi-Fi
    specifics into the rest of the codebase.
  * Store-and-forward (proximity) is fundamentally bytes-in/bytes-out plus a
    "did we already see this?" check. Anything richer than bytes would force
    the relay node to parse a payload it shouldn't even be able to decrypt.
  * Tests must run on CI machines that have no Bluetooth and no Wi-Fi adapter.
    A clean ABC lets us swap a `MockTransport` in for the same contract.

Two-layer split:

    `Transport`     — discovery + connection + send/receive of opaque bytes
    `TransportManager` (manager.py) — routes Onda envelopes to the right
                       transport, dedupes peers across transports, falls
                       back when a preferred transport is unhealthy

`Transport` instances do NOT know about envelopes. They deliver `bytes`
upward via an `on_frame` callback the manager registers at start time.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Literal,
    Protocol,
)

# Stable string set so callers can pattern-match on transport name without
# importing the concrete classes (which may not be importable on this OS).
TransportName = Literal[
    "internet",
    "lan",
    "bluetooth",
    "wifi_direct",
    "proximity",
    "mock",
]


class TransportPriority:
    """Default priority of each transport when multiple paths reach the same peer.

    Lower number = preferred. Tunable per-deployment via
    `OndaSettings.transport_priority`. The defaults express the intuition:
    "use the fast pipe if you have it; degrade gracefully through wireless;
    fall back to sneakernet last."
    """

    INTERNET = 0
    LAN = 10
    WIFI_DIRECT = 30
    BLUETOOTH = 50
    MOCK = 90  # tests
    PROXIMITY = 100  # store-and-forward, last resort


class TransportError(Exception):
    """Base class for all transport-layer errors.

    Concrete transports raise subclasses; the manager catches `TransportError`
    to trigger fallback to the next-priority transport without leaking
    transport-specific exception types into application code.
    """


class TransportUnavailable(TransportError):
    """Raised when a transport's hardware/permissions are missing at runtime.

    Distinct from `TransportError` because the manager uses this signal to
    skip a transport during start-up rather than retry it.
    """


# ---- Peer endpoints ------------------------------------------------------


@dataclass
class PeerEndpoint:
    """A single way to reach a single peer via a single transport.

    A peer may have multiple endpoints (e.g. one BLE, one LAN, one Internet);
    the manager keeps them all and chooses by priority + availability.

    `did` may be None when a transport learns about a peer before completing
    a Discovery handshake — for example, a BLE advertisement carries the
    peer's libp2p PeerID prefix but not yet its DID. The manager fills it
    in once the first signed envelope arrives.
    """

    transport: TransportName
    address: str
    did: str | None = None
    name: str | None = None
    last_seen: float = field(default_factory=time.time)
    rtt_estimate_ms: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def merge(self, other: PeerEndpoint) -> PeerEndpoint:
        """Update an endpoint in place with newer info, preserving the old DID
        if the new one is unknown.
        """

        if other.did and not self.did:
            self.did = other.did
        if other.name and not self.name:
            self.name = other.name
        if other.last_seen > self.last_seen:
            self.last_seen = other.last_seen
        if other.rtt_estimate_ms is not None:
            self.rtt_estimate_ms = other.rtt_estimate_ms
        # Metadata: shallow-merge, newer wins.
        self.metadata.update(other.metadata)
        return self


# ---- Frames + handler ----------------------------------------------------


@dataclass
class IncomingFrame:
    """A single message received by a transport, before envelope parsing.

    The manager reassembles fragments (BLE) and decrypts proximity wrappers
    before producing a frame here. Application-level Envelope.from_json /
    verify happens one layer up.
    """

    transport: TransportName
    peer: PeerEndpoint
    payload: bytes


class FrameHandler(Protocol):
    """Async callback the manager registers with each transport.

    Transports invoke it for every received frame. The handler returns a
    reply payload (or None if no reply is needed); transports that support
    in-stream replies (libp2p, GATT notify) should write it back on the same
    connection.
    """

    async def __call__(self, frame: IncomingFrame) -> bytes | None: ...


# ---- Transport ABC -------------------------------------------------------


class Transport(ABC):
    """Common contract for every v0.2 transport.

    Lifecycle:

        t = ConcreteTransport(...)
        if not await t.is_available():
            return  # nothing useful we can do
        await t.start(on_frame)        # spawn discovery + listening tasks
        async for ep in t.peers():     # observe live peers
            ...
        await t.send(ep, payload)      # blocking until ack or transport timeout
        await t.stop()                 # graceful shutdown

    Concrete transports are responsible for their own retries, framing, and
    fragmentation. The manager just chooses among them.
    """

    name: TransportName
    priority: int

    # ---- Capability check ------------------------------------------------

    @abstractmethod
    async def is_available(self) -> bool:
        """True if this transport can run on the current host right now.

        This is checked once at startup; transports must not lie. Examples
        of hard-no answers: Bluetooth adapter missing, kernel module missing
        (Wi-Fi Direct on a server), `bleak` not importable, OS unsupported.
        """

    # ---- Lifecycle -------------------------------------------------------

    @abstractmethod
    async def start(self, on_frame: FrameHandler) -> None:
        """Begin discovery and listening. May spawn background tasks.

        Implementations must be idempotent: calling start() twice is a bug
        that should raise. The handler reference is stored for the lifetime
        of the transport.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop discovery + listening; tear down OS resources.

        Must be safe to call before start() (no-op) and after start() failed
        (best-effort cleanup). Re-starting after stop() is allowed.
        """

    # ---- Discovery -------------------------------------------------------

    @abstractmethod
    def peers(self) -> list[PeerEndpoint]:
        """Snapshot of currently-known peer endpoints reachable via this transport."""

    @abstractmethod
    async def discover(self) -> AsyncIterator[PeerEndpoint]:
        """Async stream of newly-discovered or re-seen peer endpoints.

        Hot — may emit forever. The manager consumes this and merges into a
        unified registry. Yield each endpoint at most once per discovery
        burst; re-emit when the peer is rediscovered.
        """

    # ---- Send -----------------------------------------------------------

    @abstractmethod
    async def send(self, peer: PeerEndpoint, payload: bytes) -> bytes | None:
        """Send `payload` to `peer`, optionally returning a synchronous reply.

        Returning a reply is allowed but optional; transports without
        request/response semantics (BLE notify-only, proximity) return None
        and the reply is delivered later via `on_frame`.

        Raises `TransportError` (or subclass) if the send cannot complete.
        """


__all__ = [
    "FrameHandler",
    "IncomingFrame",
    "PeerEndpoint",
    "Transport",
    "TransportError",
    "TransportName",
    "TransportPriority",
    "TransportUnavailable",
]
