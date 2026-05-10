"""MockTransport / MockBus tests.

Cover the cases that v0.2 integration tests rely on:
  * Two attached transports default to NOT seeing each other (unconnected).
  * connect()/disconnect() flip visibility.
  * deliver() correctly raises when out of range or no handler.
"""

from __future__ import annotations

import pytest
import trio

from onda.network.transport_base import IncomingFrame, PeerEndpoint
from onda.transports.mock import MockBus, MockTransport


async def _drain(t: MockTransport) -> list[IncomingFrame]:
    received: list[IncomingFrame] = []

    async def handler(frame: IncomingFrame) -> bytes | None:
        received.append(frame)
        return b"ack"

    await t.start(handler)
    return received


@pytest.mark.trio
async def test_unconnected_peers_cannot_send() -> None:
    bus = MockBus()
    a = MockTransport(bus, advertised_did="did:key:zA", advertised_name="alice")
    b = MockTransport(bus, advertised_did="did:key:zB", advertised_name="bob")
    await _drain(a)
    await _drain(b)

    with pytest.raises(Exception):
        await a.send(
            PeerEndpoint(transport="mock", address=b.id, did=b.advertised_did),
            b"hello",
        )


@pytest.mark.trio
async def test_connected_peers_exchange_payload_with_reply() -> None:
    bus = MockBus()
    a = MockTransport(bus, advertised_did="did:key:zA", advertised_name="alice")
    b = MockTransport(bus, advertised_did="did:key:zB", advertised_name="bob")
    await _drain(a)
    await _drain(b)
    bus.connect(a, b)

    reply = await a.send(
        PeerEndpoint(transport="mock", address=b.id, did=b.advertised_did),
        b"hello",
    )
    assert reply == b"ack"


@pytest.mark.trio
async def test_disconnect_drops_visibility() -> None:
    bus = MockBus()
    a = MockTransport(bus)
    b = MockTransport(bus)
    await _drain(a)
    await _drain(b)
    bus.connect(a, b)
    bus.disconnect(a, b)

    assert a.peers() == []
    assert b.peers() == []


@pytest.mark.trio
async def test_asymmetric_visibility() -> None:
    # A→B but not B→A: useful for store-and-forward scenarios where one
    # side is offline while the other is mobile.
    bus = MockBus()
    a = MockTransport(bus)
    b = MockTransport(bus)
    await _drain(a)
    await _drain(b)
    bus.connect(a, b, bidirectional=False)

    assert [p.address for p in a.peers()] == [b.id]
    assert b.peers() == []
