"""DID identity layer.

Onda uses `did:key` with Ed25519. The DID method is purely self-certifying:
the public key IS embedded in the DID string, so resolution requires no
network call. This is exactly what we want for a server-less protocol.

Format (per W3C did:key spec, multicodec ed25519-pub = 0xed):
    did:key:z<base58btc(0xed01 || 32-byte-pubkey)>

We deliberately use the SAME Ed25519 keypair for the libp2p host identity
and the DID. That way `did:key:z6Mk…` and the libp2p PeerID `12D3KooW…` are
two encodings of the same public key, and a peer can prove ownership of its
DID simply by being reachable at its libp2p address. The libp2p adapter
lives in `transport.py` to keep this module dependency-light.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

import base58
from nacl import signing
from nacl.encoding import RawEncoder

# Multicodec varint prefix for "ed25519-pub" (0xed) followed by length tag 0x01.
# See https://github.com/multiformats/multicodec/blob/master/table.csv
_ED25519_MULTICODEC_PREFIX = b"\xed\x01"

DID_KEY_PREFIX = "did:key:z"


@dataclass(frozen=True)
class Identity:
    """A node's persistent cryptographic identity.

    Frozen because mutating an identity after load would break every signature
    we've already issued. Generate a new one if you want a different DID.
    """

    name: str
    did: str
    seed: bytes  # 32-byte Ed25519 seed; private — never log, never transmit.
    public_key: bytes  # 32-byte raw Ed25519 public key.

    # ---- Constructors -------------------------------------------------------

    @classmethod
    def generate(cls, name: str) -> Identity:
        """Generate a brand-new identity from OS randomness."""

        seed = os.urandom(32)
        return cls._from_seed(name=name, seed=seed)

    @classmethod
    def _from_seed(cls, *, name: str, seed: bytes) -> Identity:
        if len(seed) != 32:
            raise ValueError(f"seed must be 32 bytes, got {len(seed)}")
        sk = signing.SigningKey(seed, encoder=RawEncoder)
        pk = bytes(sk.verify_key.encode(encoder=RawEncoder))
        did = encode_did_key(pk)
        return cls(name=name, did=did, seed=seed, public_key=pk)

    # ---- Persistence --------------------------------------------------------

    @classmethod
    def load_or_create(cls, *, name: str, path: Path) -> Identity:
        """Load identity from `path`, or generate-and-save if missing.

        The file format is intentionally trivial JSON so an operator can
        inspect it and so we don't need a key-management daemon for v0.1.
        """

        if path.exists():
            return cls.load(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        ident = cls.generate(name)
        ident.save(path)
        return ident

    @classmethod
    def load(cls, path: Path) -> Identity:
        data = json.loads(path.read_text(encoding="utf-8"))
        seed = bytes.fromhex(data["seed_hex"])
        return cls._from_seed(name=data["name"], seed=seed)

    def save(self, path: Path) -> None:
        # 0o600 because this file holds a signing seed; if anyone else on the
        # box can read it, they can impersonate this node.
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "name": self.name,
            "did": self.did,
            "seed_hex": self.seed.hex(),
            "public_key_hex": self.public_key.hex(),
        }
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)

    # ---- Helpers ------------------------------------------------------------

    @property
    def signing_key(self) -> signing.SigningKey:
        return signing.SigningKey(self.seed, encoder=RawEncoder)

    @property
    def verify_key(self) -> signing.VerifyKey:
        return signing.VerifyKey(self.public_key, encoder=RawEncoder)


# ---- did:key codec --------------------------------------------------------


def encode_did_key(public_key: bytes) -> str:
    """Encode a 32-byte Ed25519 public key as a `did:key:z…` string."""

    if len(public_key) != 32:
        raise ValueError(f"Ed25519 pubkey must be 32 bytes, got {len(public_key)}")
    payload = _ED25519_MULTICODEC_PREFIX + public_key
    return DID_KEY_PREFIX + base58.b58encode(payload).decode("ascii")


def decode_did_key(did: str) -> bytes:
    """Decode a `did:key:z…` Ed25519 DID back to its 32-byte raw public key.

    Raises ValueError on anything that isn't a `did:key` with the Ed25519
    multicodec prefix; we don't want to accept arbitrary DID methods silently.
    """

    if not did.startswith(DID_KEY_PREFIX):
        raise ValueError(f"unsupported DID method or encoding: {did!r}")
    raw = base58.b58decode(did.removeprefix(DID_KEY_PREFIX))
    if not raw.startswith(_ED25519_MULTICODEC_PREFIX):
        raise ValueError("only Ed25519 did:key is supported in v0.1")
    pk = raw[len(_ED25519_MULTICODEC_PREFIX):]
    if len(pk) != 32:
        raise ValueError(f"decoded pubkey wrong length: {len(pk)}")
    return pk
