"""Three-node store-and-forward walkthrough using MockTransport.

Runs on any machine without networking, BLE, or Wi-Fi. Demonstrates:

  * A and C never see each other (network topology forbids it).
  * A submits a TaskRequest addressed to C; the proximity transport seals
    the inner envelope for C and stores a carrier in A's local mailbox.
  * B encounters A first, drains A's mailbox into B's own.
  * B then encounters C, forwarding the carrier directly.
  * C decrypts the inner envelope, verifies A's original signature, and
    "answers" (here: prints what it would have run through Ollama).

Run with: `python examples/demo_proximity_three_nodes.py`
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

# Allow running before `pip install -e .`
_SRC = Path(__file__).resolve().parents[1] / "src"
if _SRC.exists() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import trio

from onda.identity import Identity
from onda.network.transport_base import IncomingFrame, PeerEndpoint
from onda.protocol import (
    Envelope,
    MessageType,
    ProximityCarrierBody,
    TaskRequestBody,
)
from onda.transports.mock import MockBus, MockTransport
from onda.transports.proximity import ProximityTransport


async def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="onda-prox-demo-"))
    print(f"=== Onda v0.2 proximity demo (mailboxes in {tmp}) ===")

    alice = Identity.generate("alice")
    bob = Identity.generate("bob")
    cleo = Identity.generate("cleo")

    bus = MockBus()
    a_mock = MockTransport(bus, advertised_did=alice.did, advertised_name="alice")
    b_mock = MockTransport(bus, advertised_did=bob.did, advertised_name="bob")
    c_mock = MockTransport(bus, advertised_did=cleo.did, advertised_name="cleo")

    a_prox = ProximityTransport(identity=alice, mailbox_path=tmp / "a.sqlite", carriers=[a_mock])
    b_prox = ProximityTransport(identity=bob, mailbox_path=tmp / "b.sqlite", carriers=[b_mock])
    c_prox = ProximityTransport(identity=cleo, mailbox_path=tmp / "c.sqlite", carriers=[c_mock])

    delivered_to_c: list[Envelope] = []

    def make_handler(prox):
        async def handler(frame: IncomingFrame) -> bytes | None:
            env = Envelope.from_json(frame.payload)
            if not env.verify():
                return None
            if env.type == MessageType.PROXIMITY_CARRIER:
                body = ProximityCarrierBody.model_validate(env.body)
                inner = await prox.handle_inbound_carrier(body, frame.peer)
                if inner is not None:
                    delivered_to_c.append(inner)
                    print(f"  [{prox.identity.name}] decrypted inner envelope from {inner.sender}")
            return None
        return handler

    await a_mock.start(make_handler(a_prox))
    await b_mock.start(make_handler(b_prox))
    await c_mock.start(make_handler(c_prox))

    async def noop(_): return None
    await a_prox.start(noop)
    await b_prox.start(noop)
    await c_prox.start(noop)

    print()
    print("Step 1: topology — A meets B, B meets C, but A and C never meet.")
    bus.connect(a_mock, b_mock)
    bus.connect(b_mock, c_mock)

    print("Step 2: A composes a signed TaskRequest addressed to C, then asks the")
    print("        proximity transport to carry it. The inner envelope is sealed")
    print("        with C's X25519 key — neither A's mailbox nor B's mailbox can")
    print("        read the prompt.")
    inner = Envelope.build(
        identity=alice,
        msg_type=MessageType.TASK_REQUEST,
        body=TaskRequestBody(
            prompt="Cleo, mio nonno raccontava di un pesce raro a Mazara — ne sai qualcosa?"
        ),
        recipient=cleo.did,
    )
    target = PeerEndpoint(transport="proximity", address="prox", did=cleo.did)
    await a_prox.send(target, inner.to_json().encode("utf-8"))
    print("        → A.send() returned. The carrier was forwarded to B (only peer A sees).")

    print()
    print("Step 3: B's mailbox now holds an opaque carrier addressed to C.")
    rows = b_prox.mailbox.all_rows()
    for r in rows:
        print(f"        carrier_id={r.carrier_id[:8]}…  recipient={r.final_recipient_did[:24]}…  hop={r.hop_count}")

    print()
    print("Step 4: time passes; B is now also visible to C. We trigger B's drain.")
    n = await b_prox.attempt_drain()
    print(f"        → B forwarded {n} carrier(s).")

    print()
    print("Step 5: C decrypts, verifies A's signature, and gets the original prompt.")
    if delivered_to_c:
        inner_env = delivered_to_c[0]
        body = inner_env.parsed_body()
        assert isinstance(body, TaskRequestBody)
        print(f"        From: {inner_env.sender}")
        print(f"        Prompt: {body.prompt}")
    else:
        print("        (nothing delivered — bug?)")

    print()
    print("=== End of demo ===")


if __name__ == "__main__":
    trio.run(main)
