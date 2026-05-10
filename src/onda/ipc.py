"""Local IPC between the running daemon and the CLI.

The transport is a Unix domain socket at `~/.onda/<name>/ipc.sock`. Each
request is a single line of JSON; each response is a single line of JSON.
This is just enough JSON-RPC to support the v0.1 CLI verbs:

  * ask        — daemon broadcasts a TaskRequest, returns gathered answers
  * remember   — write a fragment into local memory
  * peers      — list connected peers (PeerID, DID, name)
  * recv       — list recent inbound TaskRequests handled by this daemon
  * shutdown   — orderly stop

We use Unix sockets rather than HTTP because (a) no extra TCP port to
collide with libp2p on, (b) filesystem permissions naturally scope access
to the user, and (c) the spec already namespaces everything under
`~/.onda/<name>/` — the socket fits the same pattern.
"""

from __future__ import annotations

import json
import os
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

import trio

from .log import get_logger

log = get_logger(__name__)


# Method handler: takes params dict, returns result dict (or raises).
MethodHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


@dataclass
class IPCError(Exception):
    code: str
    message: str


# ---- Server -------------------------------------------------------------


class IPCServer:
    """Tiny line-delimited JSON-RPC server bound to a unix socket."""

    def __init__(self, socket_path: Path) -> None:
        self.socket_path = socket_path
        self._handlers: dict[str, MethodHandler] = {}

    def register(self, method: str, handler: MethodHandler) -> None:
        self._handlers[method] = handler

    async def serve(self) -> None:
        # Always remove a stale socket file from a previous crashed daemon —
        # leaving it there would make `socket.bind` fail with "address in use".
        try:
            self.socket_path.unlink()
        except FileNotFoundError:
            pass
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)

        sock = trio.socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        await sock.bind(str(self.socket_path))
        sock.listen(16)
        # 0o600: only the owning user can talk to the daemon.
        os.chmod(self.socket_path, 0o600)
        log.info("ipc.listening", path=str(self.socket_path))

        try:
            # Single long-lived nursery: each accepted client gets a child
            # task and concurrent clients are served in parallel. We catch
            # exceptions per-client in `_handle_client_safe` so that a rude
            # client (closes mid-write, sends garbage, disconnects on us)
            # cannot tear down the whole IPC server.
            async with trio.open_nursery() as nursery:
                while True:
                    client, _ = await sock.accept()
                    nursery.start_soon(self._handle_client_safe, client)
        finally:
            sock.close()
            try:
                self.socket_path.unlink()
            except FileNotFoundError:
                pass

    async def _handle_client_safe(self, client: trio.socket.SocketType) -> None:
        """Wrapper that swallows expected client-side disconnect errors.

        BrokenResourceError / ClosedResourceError fire whenever the CLI
        process exits before reading our response (e.g. it timed out, the
        user hit Ctrl-C, or it just doesn't care). None of these should
        crash the daemon.
        """

        try:
            await self._handle_client(client)
        except (trio.BrokenResourceError, trio.ClosedResourceError, BrokenPipeError) as exc:
            log.debug("ipc.client_disconnect", err=str(exc))
        except Exception as exc:
            log.exception("ipc.client_unexpected", err=str(exc))

    async def _handle_client(self, client: trio.socket.SocketType) -> None:
        stream = trio.SocketStream(client)
        try:
            buf = b""
            while True:
                chunk = await stream.receive_some(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, _, buf = buf.partition(b"\n")
                    await self._dispatch(stream, line)
        finally:
            await stream.aclose()

    async def _dispatch(self, stream: trio.SocketStream, line: bytes) -> None:
        try:
            req = json.loads(line.decode("utf-8"))
            req_id = req.get("id")
            method = req["method"]
            params = req.get("params") or {}
        except Exception as exc:
            await self._send(stream, None, error={"code": "bad_request", "message": str(exc)})
            return

        handler = self._handlers.get(method)
        if handler is None:
            await self._send(stream, req_id, error={"code": "unknown_method", "message": method})
            return

        try:
            result = await handler(params)
            await self._send(stream, req_id, result=result)
        except IPCError as exc:
            await self._send(stream, req_id, error={"code": exc.code, "message": exc.message})
        except Exception as exc:
            log.exception("ipc.handler_error", method=method)
            await self._send(stream, req_id, error={"code": "internal_error", "message": str(exc)})

    async def _send(
        self,
        stream: trio.SocketStream,
        req_id: Any,
        *,
        result: Any = None,
        error: dict[str, str] | None = None,
    ) -> None:
        payload: dict[str, Any] = {"id": req_id}
        if error is not None:
            payload["error"] = error
        else:
            payload["result"] = result
        await stream.send_all(json.dumps(payload).encode("utf-8") + b"\n")


# ---- Sync client (used by the CLI) ---------------------------------------


class IPCClient:
    """Blocking client. The CLI is short-lived, so trio is overkill here."""

    def __init__(self, socket_path: Path, *, timeout: float = 30.0) -> None:
        self.socket_path = socket_path
        self.timeout = timeout

    def call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        # Per-call override exists because `ask` waits on a peer's LLM, which
        # can easily exceed the default 30s on cold-load of a 3B model. Other
        # methods (remember, peers, info) finish in milliseconds and keep the
        # short default.
        if not self.socket_path.exists():
            raise IPCError(
                code="no_daemon",
                message=f"No Onda daemon running at {self.socket_path}. "
                "Start one with `onda start --name <name>`.",
            )
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout if timeout is not None else self.timeout)
            sock.connect(str(self.socket_path))
            req = json.dumps({"id": "1", "method": method, "params": params or {}}) + "\n"
            sock.sendall(req.encode("utf-8"))

            buf = b""
            while b"\n" not in buf:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buf += chunk
            line, _, _ = buf.partition(b"\n")
            resp = json.loads(line.decode("utf-8"))

        if "error" in resp and resp["error"] is not None:
            err = resp["error"]
            raise IPCError(code=err.get("code", "error"), message=err.get("message", ""))
        return resp.get("result") or {}
