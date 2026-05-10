"""Pure-Python tests for BLE fragment / reassemble logic.

No bleak/bless involved; these run on any CI box and exercise the parts
of the BLE transport that are most likely to break silently in production.
"""

from __future__ import annotations

import os

import pytest

from onda.transports._ble_framing import (
    DEFAULT_FRAGMENT_MAX,
    Fragment,
    Fragmenter,
    Reassembler,
)


def test_fragment_codec_roundtrip() -> None:
    f = Fragment(msg_id=0x1234, seq=7, last=True, payload=b"abc")
    raw = f.encode()
    decoded = Fragment.decode(raw)
    assert decoded == f


def test_decode_rejects_too_short() -> None:
    with pytest.raises(ValueError):
        Fragment.decode(b"\x00\x00")


def test_fragmenter_single_fragment_for_small_payload() -> None:
    frags = Fragmenter(max_payload=100).fragment(b"hello")
    assert len(frags) == 1
    assert frags[0].last is True
    assert frags[0].seq == 0


def test_fragmenter_msg_ids_advance() -> None:
    f = Fragmenter(max_payload=100, start_msg_id=42)
    assert f.fragment(b"a")[0].msg_id == 42
    assert f.fragment(b"b")[0].msg_id == 43


def test_fragmenter_msg_id_wraps_at_16_bit() -> None:
    f = Fragmenter(max_payload=100, start_msg_id=0xFFFF)
    assert f.fragment(b"a")[0].msg_id == 0xFFFF
    assert f.fragment(b"b")[0].msg_id == 0


def test_fragmenter_multi_fragment_marks_last_correctly() -> None:
    f = Fragmenter(max_payload=10)
    frags = f.fragment(b"x" * 25)
    # 10 + 10 + 5
    assert len(frags) == 3
    assert [fr.seq for fr in frags] == [0, 1, 2]
    assert [fr.last for fr in frags] == [False, False, True]


def test_empty_payload_still_produces_one_terminating_fragment() -> None:
    frags = Fragmenter(max_payload=100).fragment(b"")
    assert len(frags) == 1
    assert frags[0].last is True
    assert frags[0].payload == b""


def test_reassembler_assembles_in_order_arrival() -> None:
    r = Reassembler()
    f = Fragmenter(max_payload=4)
    payload = b"hello world"
    frags = f.fragment(payload)
    out = None
    for frag in frags:
        out = r.feed("peer-1", frag.encode())
    assert out == payload


def test_reassembler_assembles_out_of_order_arrival() -> None:
    r = Reassembler()
    f = Fragmenter(max_payload=4)
    frags = f.fragment(b"hello world")
    # Reverse arrival order — Reassembler must still reconstruct correctly.
    out = None
    for frag in reversed(frags):
        out = r.feed("peer-1", frag.encode())
    assert out == b"hello world"


def test_reassembler_separates_two_concurrent_messages() -> None:
    r = Reassembler()
    a = Fragmenter(max_payload=4, start_msg_id=10).fragment(b"AAAAAAAAAAAA")
    b = Fragmenter(max_payload=4, start_msg_id=20).fragment(b"BBBBBBBB")
    # Interleave fragments from two senders on different msg_ids.
    queue = []
    for x, y in zip(a, b):
        queue.extend([x, y])
    queue.extend(a[len(b):])

    out_a, out_b = None, None
    for frag in queue:
        result = r.feed("peer-X", frag.encode())
        if result is not None:
            if out_a is None:
                out_a = result
            else:
                out_b = result
    # Both messages assembled (order depends on which finishes last).
    assert {out_a, out_b} == {b"AAAAAAAAAAAA", b"BBBBBBBB"}


def test_reassembler_keys_by_peer() -> None:
    r = Reassembler()
    f = Fragmenter(max_payload=4)
    frags = f.fragment(b"hello world")
    # Peer A sends fragment 0, peer B sends THEIR fragment 0 — they must
    # not collide.
    assert r.feed("A", frags[0].encode()) is None
    assert r.feed("B", frags[0].encode()) is None
    # Now finish B's message; A is still incomplete.
    for frag in frags[1:]:
        r.feed("B", frag.encode())
    # B is gone, A should still be partial.
    assert ("B", frags[0].msg_id) not in r._partials
    assert ("A", frags[0].msg_id) in r._partials


def test_reassembler_drop_clears_in_flight() -> None:
    r = Reassembler()
    f = Fragmenter(max_payload=4)
    frags = f.fragment(b"hello world")
    r.feed("A", frags[0].encode())
    r.drop("A")
    assert not r._partials


def test_random_size_payloads_roundtrip() -> None:
    # Stress test: random payloads through the pipeline survive intact.
    r = Reassembler()
    f = Fragmenter(max_payload=DEFAULT_FRAGMENT_MAX)
    for _ in range(20):
        size = int.from_bytes(os.urandom(2), "little") % (DEFAULT_FRAGMENT_MAX * 5 + 1)
        payload = os.urandom(size)
        frags = f.fragment(payload)
        out = None
        for frag in frags:
            out = r.feed("peer-rand", frag.encode())
        assert out == payload, f"roundtrip failed for size {size}"
