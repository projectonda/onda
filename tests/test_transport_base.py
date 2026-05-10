"""Unit tests for the bare Transport abstraction (no I/O)."""

from __future__ import annotations

from onda.network.transport_base import (
    PeerEndpoint,
    TransportPriority,
)


def test_priority_ordering_matches_intuition() -> None:
    # The order is documented in the manifesto as "fastest first, sneakernet
    # last." Codify it here so a future re-tune can't silently break it.
    assert TransportPriority.INTERNET < TransportPriority.LAN
    assert TransportPriority.LAN < TransportPriority.WIFI_DIRECT
    assert TransportPriority.WIFI_DIRECT < TransportPriority.BLUETOOTH
    assert TransportPriority.BLUETOOTH < TransportPriority.PROXIMITY


def test_peer_endpoint_merge_keeps_existing_did_when_other_unknown() -> None:
    a = PeerEndpoint(transport="lan", address="x", did="did:key:zABC", name="Alice")
    b = PeerEndpoint(transport="lan", address="x", did=None, name=None, last_seen=a.last_seen + 5)
    a.merge(b)
    assert a.did == "did:key:zABC"
    assert a.name == "Alice"
    assert a.last_seen == b.last_seen


def test_peer_endpoint_merge_fills_missing_did() -> None:
    a = PeerEndpoint(transport="bluetooth", address="MAC:01", did=None)
    b = PeerEndpoint(transport="bluetooth", address="MAC:01", did="did:key:zABC")
    a.merge(b)
    assert a.did == "did:key:zABC"


def test_peer_endpoint_merge_updates_metadata() -> None:
    a = PeerEndpoint(transport="lan", address="x", metadata={"hops": 1})
    b = PeerEndpoint(transport="lan", address="x", metadata={"rssi": -67})
    a.merge(b)
    assert a.metadata == {"hops": 1, "rssi": -67}
