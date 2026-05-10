"""Onda wire protocol: message types, JSON-LD envelope, sign/verify.

Three message types are needed for v0.1:

  * `TaskRequest`  — "please answer this prompt with help from your memory"
  * `TaskResponse` — "here is the answer, with attribution and signature"
  * `Discovery`    — exchanged on first contact: my DID, name, capabilities

Wire format is a JSON-LD-shaped envelope:

    {
      "@context": "https://projectonda.com/ns/onda/v1",
      "@type": "TaskRequest",
      "id": "<uuid>",
      "issued_at": "<iso8601>",
      "from": "did:key:z6Mk…",
      "to":   "did:key:z6Mk…" | null,
      "body": { ...type-specific payload... },
      "signature": "<base64-ed25519>",
      "encrypted": false
    }

We sign the entire envelope minus the `signature` field, canonicalised. The
`@context` is intentionally a static URL: v0.1 does not run a JSON-LD
processor, but the field is present so a future v0.2 can layer in real
ANP-style expansion without changing the message shape.
"""

from __future__ import annotations

import base64
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from . import __jsonld_context__
from .crypto import (
    canonical_json,
    decrypt_from,
    encrypt_for,
    sign_payload,
    verify_payload,
)
from .identity import Identity


class MessageType(str, Enum):
    TASK_REQUEST = "TaskRequest"
    TASK_RESPONSE = "TaskResponse"
    DISCOVERY = "Discovery"
    # v0.2: store-and-forward "carrier" wrapping another envelope sealed for
    # a final recipient. Relay nodes only see the carrier metadata; the
    # inner envelope stays encrypted end-to-end.
    PROXIMITY_CARRIER = "ProximityCarrier"


# ---- Body schemas (the type-specific payload) -----------------------------


class TaskRequestBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    prompt: str = Field(..., description="Natural-language question for the peer.")
    max_tokens: int = Field(default=512, ge=1, le=8192)
    # Reserved for future message-specific hints (preferred language, persona,
    # etc.). Kept open as a dict so receivers can ignore unknown keys.
    hints: dict[str, Any] = Field(default_factory=dict)


class TaskResponseBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    in_reply_to: str = Field(..., description="`id` of the TaskRequest envelope.")
    answer: str
    # Self-attribution: the responder's human-friendly node name. The DID is
    # already in the envelope `from` field; this is just convenience.
    responder_name: str = ""
    # An optional confidence/error code for failed inferences.
    error: str | None = None


class DiscoveryBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    libp2p_addrs: list[str] = Field(default_factory=list)
    capabilities: list[str] = Field(default_factory=lambda: ["task"])


class ProximityCarrierBody(BaseModel):
    """v0.2: an opaque carrier for store-and-forward delivery.

    A relay node sees only:
      * `carrier_id` — UUID for dedup / anti-loop bookkeeping
      * `final_recipient_did` — so the relay knows whether THIS hop is the
        final destination or whether to keep relaying
      * `sealed_inner_b64` — the original Onda envelope, base64-encoded after
        being NaCl-box-encrypted with the final recipient's X25519 public key
        (derived from their DID). The relay cannot decrypt it.
      * `max_hops` / `hop_count` — drop on overflow to bound forwarding effort
      * `created_at` / `expires_at` — drop expired carriers without forwarding

    The OUTER carrier envelope is signed by whoever is forwarding it right
    now (the immediate sender), NOT by the original author. The original
    author's signature is preserved inside `sealed_inner_b64`.
    """

    model_config = ConfigDict(extra="forbid")
    carrier_id: str
    final_recipient_did: str
    sealed_inner_b64: str
    created_at: str
    expires_at: str
    max_hops: int = Field(default=4, ge=1, le=32)
    hop_count: int = Field(default=0, ge=0)
    # The original author's DID, repeated here so a relay can reason about
    # provenance (e.g. apply per-author rate limits) without decrypting.
    original_sender_did: str = ""


BodyT = Annotated[
    TaskRequestBody | TaskResponseBody | DiscoveryBody,
    Field(discriminator=None),
]


# ---- Envelope -------------------------------------------------------------


class Envelope(BaseModel):
    """Universal carrier for every Onda message on the wire."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    context: Literal["https://projectonda.com/ns/onda/v1"] = Field(
        default=__jsonld_context__,  # type: ignore[arg-type]
        alias="@context",
    )
    type: MessageType = Field(..., alias="@type")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    issued_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")
    )
    sender: str = Field(..., alias="from", description="DID of the sender.")
    recipient: str | None = Field(
        default=None,
        alias="to",
        description="DID of the intended recipient. None = broadcast.",
    )
    body: dict[str, Any] = Field(
        ..., description="Type-specific payload, validated separately."
    )
    encrypted: bool = Field(
        default=False,
        description="If True, `body` is {'ciphertext': '<b64>'} and must be "
        "decrypted before further parsing.",
    )
    signature: str = Field(default="", description="Base64-encoded Ed25519 over canonical(body+headers).")

    # ---- Construction ----------------------------------------------------

    @classmethod
    def build(
        cls,
        *,
        identity: Identity,
        msg_type: MessageType,
        body: BaseModel,
        recipient: str | None = None,
        encrypt_for_did: str | None = None,
    ) -> Envelope:
        """Create + sign (and optionally encrypt) an envelope.

        We sign the unencrypted form so a recipient can verify before
        decrypting. That choice means a malicious relay can't cause us to
        spend CPU on decryption of an unsigned blob.
        """

        body_dict = body.model_dump(mode="json")
        encrypted = False
        if encrypt_for_did is not None:
            ciphertext = encrypt_for(identity, encrypt_for_did, canonical_json(body_dict))
            body_dict = {"ciphertext": base64.b64encode(ciphertext).decode("ascii")}
            encrypted = True

        env = cls(
            **{
                "@context": __jsonld_context__,
                "@type": msg_type,
                "from": identity.did,
                "to": recipient,
                "body": body_dict,
                "encrypted": encrypted,
            }
        )
        env.signature = base64.b64encode(
            sign_payload(identity, env._signable())
        ).decode("ascii")
        return env

    # ---- Verification ----------------------------------------------------

    def _signable(self) -> dict[str, Any]:
        """Return the dict we sign / verify (everything but `signature`)."""

        return {
            "@context": self.context,
            "@type": self.type.value,
            "id": self.id,
            "issued_at": self.issued_at,
            "from": self.sender,
            "to": self.recipient,
            "body": self.body,
            "encrypted": self.encrypted,
        }

    def verify(self) -> bool:
        if not self.signature:
            return False
        try:
            sig = base64.b64decode(self.signature, validate=True)
        except ValueError:
            return False
        return verify_payload(
            signer_did=self.sender, payload=self._signable(), signature=sig
        )

    # ---- Optional decrypt + body parse -----------------------------------

    def decrypted_body(self, identity: Identity) -> dict[str, Any]:
        """Return the plaintext body dict, decrypting in place if needed.

        Caller must have already called `verify()`. We don't fold verify into
        this method because some flows (e.g. logging) want to see the
        envelope metadata even when the signature is bad.
        """

        if not self.encrypted:
            return self.body
        ct_b64 = self.body.get("ciphertext", "")
        ct = base64.b64decode(ct_b64)
        plaintext = decrypt_from(identity, self.sender, ct)
        import json

        return json.loads(plaintext.decode("utf-8"))

    def parsed_body(
        self, identity: Identity | None = None
    ) -> TaskRequestBody | TaskResponseBody | DiscoveryBody:
        """Validate the body against the schema for this envelope's type."""

        body_dict = self.body
        if self.encrypted:
            if identity is None:
                raise ValueError("identity required to decrypt envelope body")
            body_dict = self.decrypted_body(identity)
        match self.type:
            case MessageType.TASK_REQUEST:
                return TaskRequestBody.model_validate(body_dict)
            case MessageType.TASK_RESPONSE:
                return TaskResponseBody.model_validate(body_dict)
            case MessageType.DISCOVERY:
                return DiscoveryBody.model_validate(body_dict)
            case MessageType.PROXIMITY_CARRIER:
                return ProximityCarrierBody.model_validate(body_dict)

    # ---- Wire codec ------------------------------------------------------

    def to_json(self) -> str:
        # by_alias=True so we emit "@context" / "@type" / "from" / "to".
        return self.model_dump_json(by_alias=True)

    @classmethod
    def from_json(cls, raw: str | bytes) -> Envelope:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return cls.model_validate_json(raw)


__all__ = [
    "DiscoveryBody",
    "Envelope",
    "MessageType",
    "ProximityCarrierBody",
    "TaskRequestBody",
    "TaskResponseBody",
]
