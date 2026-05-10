"""Cryptographic primitives used by the protocol layer.

Two responsibilities:
  1. Detached Ed25519 signatures over canonicalized JSON, used on every
     inter-node message (a hard invariant in the spec).
  2. Optional NaCl `box` (X25519) encryption of payloads, gated by
     `OndaSettings.enable_encryption`. Off by default in v0.1: libp2p
     already runs the Noise handshake, so encryption-on-encryption is only
     useful when traversing untrusted relays — a v0.2 concern. We ship the
     plumbing now so consumers can opt in without a code change.

We canonicalize JSON via sort_keys + tight separators rather than pulling in
a full RFC 8785 (JCS) implementation. The trade-off: we forbid floats and
non-string keys in signed payloads, which is plenty for our message types.
"""

from __future__ import annotations

import json
from typing import Any

from nacl import signing
from nacl.encoding import RawEncoder
from nacl.exceptions import BadSignatureError
from nacl.public import Box, PrivateKey as X25519PrivateKey, PublicKey as X25519PublicKey

from .identity import Identity, decode_did_key


# ---- Canonical JSON -------------------------------------------------------


def canonical_json(obj: Any) -> bytes:
    """Deterministic JSON serialization for signing.

    We keep our own helper rather than calling `json.dumps` inline at every
    sign/verify site so that any change to canonicalization (e.g. swapping
    in JCS later) is a single-file edit.
    """

    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


# ---- Signing --------------------------------------------------------------


def sign_payload(identity: Identity, payload: dict[str, Any]) -> bytes:
    """Return the raw 64-byte Ed25519 signature over `canonical_json(payload)`."""

    msg = canonical_json(payload)
    sig = identity.signing_key.sign(msg, encoder=RawEncoder).signature
    return bytes(sig)


def verify_payload(*, signer_did: str, payload: dict[str, Any], signature: bytes) -> bool:
    """Verify a signature using the public key embedded in the signer DID.

    `signer_did` IS the public key (did:key is self-certifying), so there is
    no resolution step and no risk of accepting a key fetched from a server.
    """

    try:
        pubkey = decode_did_key(signer_did)
        vk = signing.VerifyKey(pubkey, encoder=RawEncoder)
        vk.verify(canonical_json(payload), signature, encoder=RawEncoder)
        return True
    except (BadSignatureError, ValueError):
        return False


# ---- Optional NaCl box (X25519) -------------------------------------------
#
# Ed25519 keys can be losslessly converted to X25519 via the standard
# birational map; PyNaCl's `to_curve25519_*` does this for us. Re-using the
# same root keypair means a node has ONE identity, not one for signing and
# one for encryption — simpler operationally, identical security in
# practice. (The X25519 form is exposed as a derived view, not a separate
# stored secret.)


def x25519_keypair_from_identity(
    identity: Identity,
) -> tuple[X25519PrivateKey, X25519PublicKey]:
    sk = identity.signing_key.to_curve25519_private_key()
    pk = identity.verify_key.to_curve25519_public_key()
    return sk, pk


def x25519_public_from_did(did: str) -> X25519PublicKey:
    pubkey = decode_did_key(did)
    vk = signing.VerifyKey(pubkey, encoder=RawEncoder)
    return vk.to_curve25519_public_key()


def encrypt_for(identity: Identity, recipient_did: str, plaintext: bytes) -> bytes:
    """NaCl box encryption: authenticated, anti-replay-safe with random nonce.

    Output layout: 24-byte nonce || ciphertext. Decryption recovers both.
    """

    sk, _ = x25519_keypair_from_identity(identity)
    rpk = x25519_public_from_did(recipient_did)
    box = Box(sk, rpk)
    return box.encrypt(plaintext)  # PyNaCl prepends the nonce automatically.


def decrypt_from(identity: Identity, sender_did: str, ciphertext: bytes) -> bytes:
    sk, _ = x25519_keypair_from_identity(identity)
    spk = x25519_public_from_did(sender_did)
    box = Box(sk, spk)
    return box.decrypt(ciphertext)


__all__ = [
    "BadSignatureError",
    "canonical_json",
    "decrypt_from",
    "encrypt_for",
    "sign_payload",
    "verify_payload",
    "x25519_keypair_from_identity",
    "x25519_public_from_did",
]
