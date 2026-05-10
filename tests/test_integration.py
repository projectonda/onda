"""Two-node integration test.

Boots two real Onda Nodes in subprocesses, talks to them via their IPC
sockets the same way the CLI does, asks one node a question, and asserts
that the other answered it with a verifiable signature.

We use the `echo` LLM backend so this test does NOT require Ollama.

The test is opt-in via the `requires_libp2p` marker because py-libp2p is
sensitive to OS networking quirks; CI failures here usually point to the
environment, not the protocol.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest


pytestmark = pytest.mark.timeout(120)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_socket(
    path: Path, *, proc: subprocess.Popen | None = None, timeout: float = 30.0
) -> None:
    start = time.time()
    while time.time() - start < timeout:
        if path.exists():
            return
        # If the daemon process died, surface its stderr so we don't time out
        # in silence — historically this was where bad py-libp2p imports hid.
        if proc is not None and proc.poll() is not None:
            err = b""
            if proc.stderr is not None:
                err = proc.stderr.read() or b""
            raise RuntimeError(
                f"daemon exited with code {proc.returncode} before opening "
                f"{path}\nstderr:\n{err.decode('utf-8', errors='replace')}"
            )
        time.sleep(0.1)
    raise TimeoutError(f"socket never appeared at {path}")


def _ipc_call(socket_path: Path, method: str, params: dict | None = None) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(30)
        s.connect(str(socket_path))
        req = json.dumps({"id": "1", "method": method, "params": params or {}}) + "\n"
        s.sendall(req.encode("utf-8"))
        buf = b""
        while b"\n" not in buf:
            chunk = s.recv(4096)
            if not chunk:
                break
            buf += chunk
        line, _, _ = buf.partition(b"\n")
    resp = json.loads(line.decode("utf-8"))
    if resp.get("error"):
        raise RuntimeError(resp["error"])
    return resp["result"]


def _spawn_node(
    *,
    home: Path,
    name: str,
    port: int,
    bootstrap: list[str],
) -> subprocess.Popen:
    env = os.environ.copy()
    env["ONDA_HOME_DIR"] = str(home)
    env["ONDA_LLM_BACKEND"] = "echo"
    env["PYTHONUNBUFFERED"] = "1"

    args = [
        sys.executable,
        "-m",
        "onda",
        "start",
        "--name",
        name,
        "--port",
        str(port),
        "--host",
        "127.0.0.1",
        "--no-mdns",
        "--llm-backend",
        "echo",
    ]
    for ma in bootstrap:
        args.extend(["--bootstrap", ma])
    return subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


@pytest.fixture
def short_home(tmp_path: Path) -> Path:
    """Workaround for the macOS AF_UNIX 104-char limit.

    pytest's `tmp_path` lives under /private/var/folders/… which already
    eats >70 chars before we add /home/<name>/ipc.sock. We mkdtemp under
    /tmp instead and clean up on teardown.
    """

    import shutil
    import tempfile

    d = Path(tempfile.mkdtemp(prefix="onda-it-", dir="/tmp"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.mark.requires_libp2p
def test_two_nodes_exchange_signed_task(short_home: Path) -> None:
    home = short_home / "h"
    home.mkdir()

    port_a = _free_port()
    port_b = _free_port()

    # Node A — no bootstrap; will be dialed by B.
    proc_a = _spawn_node(home=home, name="alice", port=port_a, bootstrap=[])
    try:
        sock_a = home / "alice" / "ipc.sock"
        _wait_for_socket(sock_a, proc=proc_a)
        info_a = _ipc_call(sock_a, "info")
        assert info_a["peer_id"]
        peer_a = info_a["peer_id"]

        # Node B — bootstraps to A so they're connected before we ask.
        boot = f"/ip4/127.0.0.1/tcp/{port_a}/p2p/{peer_a}"
        proc_b = _spawn_node(home=home, name="bob", port=port_b, bootstrap=[boot])
        try:
            sock_b = home / "bob" / "ipc.sock"
            _wait_for_socket(sock_b, proc=proc_b)
            # Pre-load Bob's memory the way the spec demo does.
            _ipc_call(
                sock_b,
                "remember",
                {"content": "Il pomodoro nella Sicilia occidentale è dolce."},
            )

            # Give the libp2p connection a moment to settle and finish the
            # Discovery exchange. We poll up to ~10s rather than sleeping.
            deadline = time.time() + 15
            while time.time() < deadline:
                peers = _ipc_call(sock_a, "peers")["peers"]
                if peers and peers[0].get("did"):
                    break
                time.sleep(0.2)
            peers = _ipc_call(sock_a, "peers")["peers"]
            assert peers, "Alice should have discovered Bob"

            result = _ipc_call(
                sock_a, "ask", {"prompt": "Come si coltiva il pomodoro?"}
            )
            answers = result["answers"]
            assert answers, "expected at least one answer"

            answer = answers[0]
            # echo backend embeds the user prompt so we can assert flow.
            assert "pomodoro" in answer["answer"]
            assert answer["from_did"].startswith("did:key:z")
            assert answer["from_name"] == "bob"

            # Bob should also have recorded receipt.
            recv = _ipc_call(sock_b, "recv")["tasks"]
            assert recv and recv[0]["prompt"].startswith("Come si coltiva")
        finally:
            proc_b.terminate()
            try:
                proc_b.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc_b.kill()
    finally:
        proc_a.terminate()
        try:
            proc_a.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc_a.kill()
