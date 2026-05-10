"""Typer CLI for Onda v0.1.

Two roles:
  * `onda start` runs the long-lived daemon (libp2p host + IPC server).
  * `onda ask|remember|peers|recv|info` are short-lived clients that talk
    to a running daemon over its Unix socket.

The `--name` option selects which daemon to talk to (each named node has
its own subdirectory under `~/.onda/<name>/`). This is what makes it
possible to have "AI di Leonardo" and "AI di Pietro" running side by side
on the same laptop, as the spec demo requires.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Annotated, Optional

import trio
import typer

from .config import OndaSettings
from .identity import Identity
from .ipc import IPCClient, IPCError
from .log import configure as configure_logging
from .node import Node

app = typer.Typer(
    name="onda",
    help="Onda v0.1 — peer-to-peer protocol for personal AIs (https://projectonda.com)",
    no_args_is_help=True,
    add_completion=False,
)


# ---- Helpers ------------------------------------------------------------


def _settings_from_args(
    *,
    name: str,
    port: Optional[int] = None,
    host: Optional[str] = None,
    bootstrap: Optional[list[str]] = None,
    enable_mdns: Optional[bool] = None,
    enable_encryption: Optional[bool] = None,
    llm_backend: Optional[str] = None,
    ollama_model: Optional[str] = None,
    transport_mode: Optional[str] = None,
    enable_internet: Optional[bool] = None,
    enable_lan: Optional[bool] = None,
    enable_bluetooth: Optional[bool] = None,
    enable_wifi_direct: Optional[bool] = None,
    enable_proximity: Optional[bool] = None,
) -> OndaSettings:
    """Construct settings, with CLI flags overriding env vars + defaults."""

    overrides: dict[str, object] = {"name": name}
    if port is not None:
        overrides["port"] = port
    if host is not None:
        overrides["host"] = host
    if bootstrap:
        overrides["bootstrap"] = list(bootstrap)
    if enable_mdns is not None:
        overrides["enable_mdns"] = enable_mdns
    if enable_encryption is not None:
        overrides["enable_encryption"] = enable_encryption
    if llm_backend is not None:
        overrides["llm_backend"] = llm_backend
    if ollama_model is not None:
        overrides["ollama_model"] = ollama_model
    if transport_mode is not None:
        overrides["transport_mode"] = transport_mode
    if enable_internet is not None:
        overrides["enable_internet"] = enable_internet
    if enable_lan is not None:
        overrides["enable_lan"] = enable_lan
    if enable_bluetooth is not None:
        overrides["enable_bluetooth"] = enable_bluetooth
    if enable_wifi_direct is not None:
        overrides["enable_wifi_direct"] = enable_wifi_direct
    if enable_proximity is not None:
        overrides["enable_proximity"] = enable_proximity
    return OndaSettings(**overrides)  # type: ignore[arg-type]


def _client(name: str) -> IPCClient:
    settings = _settings_from_args(name=name)
    return IPCClient(settings.ipc_socket_path)


def _print_json(data: object) -> None:
    typer.echo(json.dumps(data, indent=2, ensure_ascii=False))


# ---- Daemon command -----------------------------------------------------


@app.command(help="Start the Onda node daemon (libp2p host + IPC).")
def start(
    name: Annotated[str, typer.Option("--name", help="Human-friendly node name.")] = "default",
    port: Annotated[int, typer.Option("--port", help="TCP port for libp2p.")] = 9001,
    host: Annotated[str, typer.Option("--host", help="Bind address.")] = "0.0.0.0",
    bootstrap: Annotated[
        Optional[list[str]],
        typer.Option(
            "--bootstrap",
            help="Multiaddr to dial on startup. Repeat for multiple bootstraps.",
        ),
    ] = None,
    no_mdns: Annotated[bool, typer.Option("--no-mdns", help="Disable mDNS announce/browse.")] = False,
    encrypt: Annotated[
        bool, typer.Option("--encrypt", help="Wrap payloads in NaCl box (X25519).")
    ] = False,
    llm_backend: Annotated[
        Optional[str], typer.Option("--llm-backend", help="ollama | echo")
    ] = None,
    ollama_model: Annotated[
        Optional[str], typer.Option("--ollama-model", help="Ollama model tag.")
    ] = None,
    log_level: Annotated[str, typer.Option("--log-level")] = "INFO",
    transport_mode: Annotated[
        str,
        typer.Option(
            "--transport-mode",
            help="`v1` (default; single libp2p host) or `v2` (multi-transport stack).",
        ),
    ] = "v1",
    enable_bluetooth: Annotated[
        bool, typer.Option("--enable-bluetooth", help="(v2) advertise + scan via BLE.")
    ] = False,
    enable_wifi_direct: Annotated[
        bool,
        typer.Option(
            "--enable-wifi-direct",
            help="(v2) scan for Onda-prefixed Wi-Fi SSIDs (Linux/Windows only).",
        ),
    ] = False,
    enable_proximity: Annotated[
        bool,
        typer.Option(
            "--enable-proximity",
            help="(v2) accept/forward store-and-forward carriers via other transports.",
        ),
    ] = False,
) -> None:
    configure_logging(level=log_level)
    settings = _settings_from_args(
        name=name,
        port=port,
        host=host,
        bootstrap=bootstrap,
        enable_mdns=not no_mdns,
        enable_encryption=encrypt,
        llm_backend=llm_backend,
        ollama_model=ollama_model,
        transport_mode=transport_mode,
        enable_bluetooth=enable_bluetooth,
        enable_wifi_direct=enable_wifi_direct,
        enable_proximity=enable_proximity,
    )
    node = Node(settings)

    typer.echo(
        f"Onda node '{settings.name}' starting on tcp/{settings.port}\n"
        f"  DID:     {node.identity.did}\n"
        f"  IPC:     {settings.ipc_socket_path}\n"
        f"  LLM:     {settings.llm_backend} ({settings.ollama_model})\n"
        f"  mDNS:    {'on' if settings.enable_mdns else 'off'}\n"
        f"  encrypt: {'on' if settings.enable_encryption else 'off'}",
        err=True,
    )

    try:
        trio.run(node.run)
    except KeyboardInterrupt:
        typer.echo("shutdown.", err=True)


# ---- Client commands ----------------------------------------------------


@app.command(help="Send a task (prompt) to peers via the running daemon.")
def ask(
    prompt: Annotated[str, typer.Argument(help="Natural-language task.")],
    name: Annotated[str, typer.Option("--name", help="Which daemon to use.")] = "default",
    to: Annotated[
        Optional[str],
        typer.Option("--to", help="Target a specific recipient by DID."),
    ] = None,
    max_tokens: Annotated[int, typer.Option("--max-tokens")] = 512,
    timeout: Annotated[
        float,
        typer.Option(
            "--timeout",
            help="Seconds to wait for the daemon's reply. Includes peer LLM time.",
        ),
    ] = 600.0,
) -> None:
    try:
        result = _client(name).call(
            "ask",
            {"prompt": prompt, "max_tokens": max_tokens, "to": to},
            timeout=timeout,
        )
    except IPCError as exc:
        typer.echo(f"error: {exc.code}: {exc.message}", err=True)
        raise typer.Exit(code=2)

    answers = result.get("answers", [])
    if not answers:
        typer.echo("(no answers received)", err=True)
        raise typer.Exit(code=1)

    for a in answers:
        attribution = a.get("from_did") or "unknown"
        peer_name = a.get("from_name") or ""
        if a.get("error"):
            typer.echo(f"--- error from {attribution} ({peer_name}): {a['error']} ---")
            continue
        typer.echo(f"--- Risposta da {attribution} ('{peer_name}') ---")
        typer.echo(a.get("answer", ""))


@app.command(help="Add a fragment of knowledge to the local node's memory.")
def remember(
    content: Annotated[str, typer.Argument(help="Text fragment to remember.")],
    name: Annotated[str, typer.Option("--name", help="Which daemon to use.")] = "default",
    tags: Annotated[str, typer.Option("--tags")] = "",
) -> None:
    try:
        result = _client(name).call("remember", {"content": content, "tags": tags})
    except IPCError as exc:
        typer.echo(f"error: {exc.code}: {exc.message}", err=True)
        raise typer.Exit(code=2)
    typer.echo(f"remembered #{result.get('id')} at {result.get('created_at')}")


@app.command(help="List peers known to the running daemon.")
def peers(
    name: Annotated[str, typer.Option("--name", help="Which daemon to use.")] = "default",
) -> None:
    try:
        result = _client(name).call("peers")
    except IPCError as exc:
        typer.echo(f"error: {exc.code}: {exc.message}", err=True)
        raise typer.Exit(code=2)
    _print_json(result)


@app.command(help="List recent inbound tasks the daemon has answered.")
def recv(
    name: Annotated[str, typer.Option("--name", help="Which daemon to use.")] = "default",
) -> None:
    try:
        result = _client(name).call("recv")
    except IPCError as exc:
        typer.echo(f"error: {exc.code}: {exc.message}", err=True)
        raise typer.Exit(code=2)
    _print_json(result)


@app.command(help="Show the daemon's identity and listen addresses.")
def info(
    name: Annotated[str, typer.Option("--name", help="Which daemon to use.")] = "default",
) -> None:
    try:
        result = _client(name).call("info")
    except IPCError as exc:
        typer.echo(f"error: {exc.code}: {exc.message}", err=True)
        raise typer.Exit(code=2)
    _print_json(result)


@app.command(help="Generate a fresh DID identity in ~/.onda/<name>/ without starting a daemon.")
def init(
    name: Annotated[str, typer.Option("--name", help="Node name.")] = "default",
) -> None:
    settings = _settings_from_args(name=name)
    settings.ensure_dirs()
    if settings.identity_path.exists():
        typer.echo(f"identity already exists at {settings.identity_path}", err=True)
        raise typer.Exit(code=1)
    ident = Identity.load_or_create(name=name, path=settings.identity_path)
    typer.echo(f"created {ident.did} at {settings.identity_path}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
