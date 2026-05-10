"""BLE fragment / reassemble helpers.

BLE GATT characteristics have a per-write MTU ceiling — typically 23 bytes
on first connection, negotiable up to ~512 bytes. Onda envelopes are easily
1–2 KiB after signatures and (optional) NaCl-box ciphertext, so every
outbound message has to be split and reassembled at the BLE boundary.

This module is intentionally pure-Python and BLE-stack-free: it lets us
unit-test fragmentation logic on any CI runner without a Bluetooth radio.

Wire format per fragment (binary, little-endian):

    +0  +1   +2     +3      +4 .. +N
    [msg_id (u16) ][seq u8][flags u8][payload bytes]

`flags` bits (currently only one defined; reserved bits MUST be zero):
    bit 0 = LAST  (1 = this is the final fragment of the message)

`msg_id` is chosen by the sender and must be stable for the duration of a
single fragmented message. The reassembler keys partial messages by
(sender_address, msg_id) so two senders fragmenting concurrently don't
clobber each other.

Why no fragment count up-front: with BLE, you don't reliably know the
final size in advance for a streaming reply, and including a count would
waste a precious header byte. Marking the LAST flag on the final fragment
keeps the header minimal (4 bytes) and is robust to growth.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator

_HEADER_LEN = 4
_FLAG_LAST = 0x01

# Default conservative MTU. Real BLE stacks negotiate higher numbers; the
# transport queries the live MTU and re-creates the fragmenter with that
# value, but we keep a safe default for tests.
DEFAULT_FRAGMENT_MAX = 180  # 184 byte ATT payload, minus 4-byte header


@dataclass
class Fragment:
    msg_id: int
    seq: int
    last: bool
    payload: bytes

    def encode(self) -> bytes:
        if not (0 <= self.msg_id <= 0xFFFF):
            raise ValueError(f"msg_id out of range: {self.msg_id}")
        if not (0 <= self.seq <= 0xFF):
            raise ValueError(f"seq out of range: {self.seq}")
        flags = _FLAG_LAST if self.last else 0
        return (
            self.msg_id.to_bytes(2, "little")
            + self.seq.to_bytes(1, "little")
            + flags.to_bytes(1, "little")
            + self.payload
        )

    @classmethod
    def decode(cls, raw: bytes) -> Fragment:
        if len(raw) < _HEADER_LEN:
            raise ValueError(f"fragment too short: {len(raw)} bytes")
        msg_id = int.from_bytes(raw[0:2], "little")
        seq = raw[2]
        flags = raw[3]
        payload = raw[_HEADER_LEN:]
        return cls(msg_id=msg_id, seq=seq, last=bool(flags & _FLAG_LAST), payload=payload)


class Fragmenter:
    """Split a bytes payload into BLE-sized fragments.

    Stateless apart from the `next_msg_id` counter. Tests rely on the
    counter being deterministic given a starting value.
    """

    def __init__(self, max_payload: int = DEFAULT_FRAGMENT_MAX, *, start_msg_id: int = 0) -> None:
        if max_payload <= 0:
            raise ValueError("max_payload must be positive")
        self.max_payload = max_payload
        self._next_msg_id = start_msg_id

    def fragment(self, data: bytes) -> list[Fragment]:
        msg_id = self._next_msg_id
        self._next_msg_id = (self._next_msg_id + 1) & 0xFFFF

        if not data:
            # An empty payload is still a complete (single-fragment) message.
            return [Fragment(msg_id=msg_id, seq=0, last=True, payload=b"")]

        out: list[Fragment] = []
        n = len(data)
        i = 0
        seq = 0
        while i < n:
            chunk = data[i : i + self.max_payload]
            i += len(chunk)
            if seq > 255:
                # Overflow protection: a 256-fragment message at MTU=180 is
                # ~46 KiB. We don't expect Onda envelopes anywhere near that.
                raise ValueError("message exceeds 256 fragments; raise MTU or split at app layer")
            out.append(Fragment(msg_id=msg_id, seq=seq, last=(i >= n), payload=chunk))
            seq += 1
        return out


@dataclass
class _PartialMessage:
    fragments: dict[int, bytes] = field(default_factory=dict)
    last_seq: int | None = None

    def add(self, frag: Fragment) -> None:
        self.fragments[frag.seq] = frag.payload
        if frag.last:
            # The last fragment's seq IS the index of the last fragment.
            self.last_seq = frag.seq

    @property
    def complete(self) -> bool:
        if self.last_seq is None:
            return False
        # We need every seq from 0..last_seq inclusive.
        return all(s in self.fragments for s in range(self.last_seq + 1))

    def assemble(self) -> bytes:
        assert self.last_seq is not None
        return b"".join(self.fragments[s] for s in range(self.last_seq + 1))


class Reassembler:
    """Buffer fragments per (peer, msg_id) until the message is complete.

    Caller feeds raw fragment bytes via `feed()`; the method returns the
    assembled payload when the LAST flag has arrived AND every preceding
    seq has been seen, else None.

    There is NO retransmission logic here. BLE GATT writes-with-response
    are reliable at the L2CAP layer, so missing fragments mean either the
    peer disappeared (we'll time out at a higher layer) or the link is
    fundamentally broken (no retransmit will help). Keeping this module
    transport-of-truth-free makes it trivially testable.
    """

    def __init__(self) -> None:
        self._partials: dict[tuple[str, int], _PartialMessage] = {}

    def feed(self, peer_addr: str, raw: bytes) -> bytes | None:
        try:
            frag = Fragment.decode(raw)
        except ValueError:
            return None
        key = (peer_addr, frag.msg_id)
        partial = self._partials.get(key)
        if partial is None:
            partial = _PartialMessage()
            self._partials[key] = partial
        partial.add(frag)
        if partial.complete:
            del self._partials[key]
            return partial.assemble()
        return None

    def drop(self, peer_addr: str) -> None:
        """Forget any in-flight messages from this peer (e.g. on disconnect)."""

        for key in list(self._partials):
            if key[0] == peer_addr:
                del self._partials[key]


__all__ = ["Fragment", "Fragmenter", "Reassembler", "DEFAULT_FRAGMENT_MAX"]
