"""Tests for DID generation, persistence, and codec round-trips."""

from __future__ import annotations

from pathlib import Path

import pytest

from onda.identity import (
    DID_KEY_PREFIX,
    Identity,
    decode_did_key,
    encode_did_key,
)


def test_generate_yields_distinct_keys() -> None:
    a = Identity.generate("alice")
    b = Identity.generate("bob")
    assert a.did != b.did
    assert a.public_key != b.public_key
    assert a.did.startswith(DID_KEY_PREFIX)


def test_did_key_roundtrip() -> None:
    ident = Identity.generate("alice")
    pk = decode_did_key(ident.did)
    assert pk == ident.public_key
    assert encode_did_key(pk) == ident.did


def test_decode_rejects_unknown_method() -> None:
    with pytest.raises(ValueError):
        decode_did_key("did:web:example.com")


def test_decode_rejects_wrong_multicodec() -> None:
    # did:key with a non-Ed25519 prefix should be rejected.
    with pytest.raises(ValueError):
        decode_did_key("did:key:zABCDE")  # not a real ed25519 payload


def test_load_or_create_persists(tmp_path: Path) -> None:
    p = tmp_path / "id.json"
    a = Identity.load_or_create(name="alice", path=p)
    b = Identity.load_or_create(name="alice", path=p)
    assert a.did == b.did
    assert a.seed == b.seed


def test_save_uses_restrictive_permissions(tmp_path: Path) -> None:
    p = tmp_path / "id.json"
    Identity.generate("alice").save(p)
    mode = p.stat().st_mode & 0o777
    assert mode == 0o600
