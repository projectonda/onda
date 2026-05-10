"""Configuration via pydantic-settings.

Every behavioral switch in Onda is a flag here, never an in-place edit
elsewhere — this is the ADD-ONLY discipline laid out in the v0.1 spec.

Environment variables use the prefix `ONDA_` (e.g. `ONDA_LLM_BACKEND=echo`),
or a per-call override via the `OndaSettings(...)` constructor for tests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class OndaSettings(BaseSettings):
    """All knobs for an Onda node.

    Defaults reflect v0.1: encryption off, mDNS on, Ollama as the LLM backend.
    Tests override via `OndaSettings(llm_backend='echo', ...)`.
    """

    model_config = SettingsConfigDict(
        env_prefix="ONDA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Identity / storage ---------------------------------------------------

    name: str = Field(
        default="default",
        description="Human-friendly node name. Drives the on-disk directory at "
        "~/.onda/<name>/ and is broadcast as a DID label.",
    )
    home_dir: Path = Field(
        default_factory=lambda: Path.home() / ".onda",
        description="Root directory for all node state. One subdir per --name.",
    )

    # --- libp2p transport -----------------------------------------------------

    host: str = Field(default="0.0.0.0", description="Bind address for the libp2p host.")
    port: int = Field(default=9001, ge=1, le=65535, description="TCP port for libp2p.")
    bootstrap: list[str] = Field(
        default_factory=list,
        description="Multiaddrs to dial on startup, e.g. /ip4/127.0.0.1/tcp/9001/p2p/12D3...",
    )

    # --- Discovery ------------------------------------------------------------

    enable_mdns: bool = Field(
        default=True,
        description="Announce + browse on local mDNS. Disable for headless tests.",
    )
    mdns_service: str = Field(
        default="_onda._tcp.local.",
        description="Service type registered with Zeroconf. Don't change without "
        "coordinating across the network — peers using a different name won't see you.",
    )

    # --- Privacy --------------------------------------------------------------

    enable_encryption: bool = Field(
        default=False,
        description="Wrap payloads in NaCl box (X25519). Off by default in v0.1: "
        "libp2p already runs Noise underneath, and signatures provide integrity. "
        "Turn on when sending across an untrusted relay.",
    )

    # --- LLM ------------------------------------------------------------------

    llm_backend: Literal["ollama", "echo"] = Field(
        default="ollama",
        description="`ollama` for real inference; `echo` is a deterministic stub "
        "used in tests so we don't require a running Ollama for CI.",
    )
    ollama_url: str = Field(
        default="http://127.0.0.1:11434",
        description="URL of the local Ollama daemon.",
    )
    ollama_model: str = Field(
        default="llama3.2:3b",
        description="Model tag pulled into Ollama. The spec lists llama3.2:3b "
        "or mistral:7b — anything Ollama can serve will work.",
    )
    llm_timeout_s: float = Field(default=120.0, ge=1.0)

    # --- Memory ---------------------------------------------------------------

    memory_max_chars: int = Field(
        default=8000,
        description="Hard cap on the total memory text we splice into the LLM "
        "prompt. v0.1 strategy is full-dump retrieval, so we cap to protect the "
        "context window. Increase for larger local models.",
    )

    # --- IPC (CLI <-> daemon) -------------------------------------------------

    ipc_socket_name: str = Field(
        default="ipc.sock",
        description="Filename of the unix socket inside the node's home dir.",
    )

    # --- v0.2 multi-transport (ADD-ONLY: defaults preserve v0.1 behavior) ----

    transport_mode: Literal["v1", "v2"] = Field(
        default="v1",
        description="Choose between the v0.1 single-libp2p path (v1, default) "
        "and the v0.2 multi-transport stack (v2). v0.1 callers see no change "
        "until they explicitly opt in.",
    )
    transport_priority: list[str] = Field(
        default_factory=lambda: ["internet", "lan", "wifi_direct", "bluetooth", "proximity"],
        description="Order in which transports are tried per recipient when "
        "multiple paths exist. v0.2 only.",
    )
    enable_internet: bool = Field(default=True)
    enable_lan: bool = Field(default=True)
    enable_bluetooth: bool = Field(default=False)
    enable_wifi_direct: bool = Field(default=False)
    enable_proximity: bool = Field(default=False)

    # Proximity tuning (ignored unless enable_proximity is True).
    proximity_ttl_seconds: int = Field(default=7 * 24 * 3600, ge=60)
    proximity_max_hops: int = Field(default=4, ge=1, le=32)
    proximity_max_mailbox_rows: int = Field(default=1000, ge=10)

    # --- Misc -----------------------------------------------------------------

    log_level: str = Field(default="INFO")

    # --- Computed paths -------------------------------------------------------

    @property
    def node_dir(self) -> Path:
        return self.home_dir / self.name

    @property
    def identity_path(self) -> Path:
        return self.node_dir / "identity.json"

    @property
    def memory_path(self) -> Path:
        return self.node_dir / "memory.sqlite"

    @property
    def proximity_mailbox_path(self) -> Path:
        return self.node_dir / "proximity_mailbox.sqlite"

    @property
    def ipc_socket_path(self) -> Path:
        return self.node_dir / self.ipc_socket_name

    def ensure_dirs(self) -> None:
        """Create ~/.onda/<name>/. Safe to call repeatedly."""

        self.node_dir.mkdir(parents=True, exist_ok=True)
