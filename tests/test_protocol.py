"""Protocol-level tests: serialization, sign/verify, tamper detection,
and the optional NaCl-box encryption round-trip.

These tests do NOT exercise libp2p — they validate the message layer in
isolation, which is the right granularity for guaranteeing that two nodes
written against this code can interoperate.
"""

from __future__ import annotations

import base64
import json

import pytest

from onda.crypto import canonical_json, sign_payload, verify_payload
from onda.identity import Identity
from onda.protocol import (
    DiscoveryBody,
    Envelope,
    MessageType,
    TaskRequestBody,
    TaskResponseBody,
)


# ---- Canonical JSON & low-level signing ---------------------------------


def test_canonical_json_is_stable_under_key_order() -> None:
    a = canonical_json({"b": 1, "a": 2})
    b = canonical_json({"a": 2, "b": 1})
    assert a == b


def test_sign_and_verify_payload_roundtrip() -> None:
    ident = Identity.generate("alice")
    payload = {"hello": "world", "n": 42}
    sig = sign_payload(ident, payload)
    assert verify_payload(signer_did=ident.did, payload=payload, signature=sig)


def test_verify_fails_when_payload_tampered() -> None:
    ident = Identity.generate("alice")
    payload = {"x": 1}
    sig = sign_payload(ident, payload)
    tampered = {"x": 2}
    assert not verify_payload(signer_did=ident.did, payload=tampered, signature=sig)


def test_verify_fails_for_wrong_signer() -> None:
    alice = Identity.generate("alice")
    bob = Identity.generate("bob")
    payload = {"x": 1}
    sig = sign_payload(alice, payload)
    assert not verify_payload(signer_did=bob.did, payload=payload, signature=sig)


# ---- Envelope round-trip ------------------------------------------------


def test_envelope_task_request_roundtrip() -> None:
    ident = Identity.generate("alice")
    env = Envelope.build(
        identity=ident,
        msg_type=MessageType.TASK_REQUEST,
        body=TaskRequestBody(prompt="ciao"),
    )
    raw = env.to_json()
    parsed = Envelope.from_json(raw)
    assert parsed.verify()
    body = parsed.parsed_body()
    assert isinstance(body, TaskRequestBody)
    assert body.prompt == "ciao"


def test_envelope_emits_jsonld_aliases() -> None:
    ident = Identity.generate("alice")
    env = Envelope.build(
        identity=ident,
        msg_type=MessageType.DISCOVERY,
        body=DiscoveryBody(name="alice"),
    )
    raw = json.loads(env.to_json())
    assert "@context" in raw
    assert raw["@type"] == "Discovery"
    assert raw["from"] == ident.did
    assert raw["to"] is None


def test_envelope_signature_detects_body_tamper() -> None:
    ident = Identity.generate("alice")
    env = Envelope.build(
        identity=ident,
        msg_type=MessageType.TASK_REQUEST,
        body=TaskRequestBody(prompt="ciao"),
    )
    raw = json.loads(env.to_json())
    raw["body"]["prompt"] = "ciao mondo"  # tamper after signing
    tampered = Envelope.from_json(json.dumps(raw))
    assert not tampered.verify()


def test_envelope_signature_detects_signer_swap() -> None:
    alice = Identity.generate("alice")
    bob = Identity.generate("bob")
    env = Envelope.build(
        identity=alice,
        msg_type=MessageType.TASK_RESPONSE,
        body=TaskResponseBody(in_reply_to="x", answer="y", responder_name="alice"),
    )
    raw = json.loads(env.to_json())
    raw["from"] = bob.did  # claim to be Bob
    forged = Envelope.from_json(json.dumps(raw))
    assert not forged.verify()


def test_envelope_signature_detects_truncated() -> None:
    ident = Identity.generate("alice")
    env = Envelope.build(
        identity=ident,
        msg_type=MessageType.TASK_REQUEST,
        body=TaskRequestBody(prompt="x"),
    )
    raw = json.loads(env.to_json())
    raw["signature"] = base64.b64encode(b"\x00" * 64).decode("ascii")
    forged = Envelope.from_json(json.dumps(raw))
    assert not forged.verify()


# ---- Encryption (opt-in) ------------------------------------------------


def test_encrypted_envelope_roundtrip_between_two_nodes() -> None:
    alice = Identity.generate("alice")
    bob = Identity.generate("bob")

    env = Envelope.build(
        identity=alice,
        msg_type=MessageType.TASK_REQUEST,
        body=TaskRequestBody(prompt="segreto"),
        recipient=bob.did,
        encrypt_for_did=bob.did,
    )
    assert env.encrypted is True
    assert env.verify()

    # Bob decrypts and parses.
    parsed_body = env.parsed_body(bob)
    assert isinstance(parsed_body, TaskRequestBody)
    assert parsed_body.prompt == "segreto"


def test_encrypted_envelope_unreadable_by_third_party() -> None:
    alice = Identity.generate("alice")
    bob = Identity.generate("bob")
    eve = Identity.generate("eve")

    env = Envelope.build(
        identity=alice,
        msg_type=MessageType.TASK_REQUEST,
        body=TaskRequestBody(prompt="segreto"),
        recipient=bob.did,
        encrypt_for_did=bob.did,
    )

    with pytest.raises(Exception):
        env.parsed_body(eve)
