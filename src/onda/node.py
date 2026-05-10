"""Onda Node — the running daemon.

A Node owns:
  * one persistent Identity (DID + Ed25519 keypair, on disk under ~/.onda/<name>)
  * one local SQLite memory store
  * one LLM backend (Ollama or echo)
  * one libp2p host (via LibP2PTransport)
  * one IPC server (Unix socket) that the CLI talks to

The Node is the *only* component that combines all of these. Everything else
in the package is a single-responsibility module so it can be tested in
isolation. This file is therefore mostly wiring + the inbound-task handler
that turns a TaskRequest into an LLM call.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

import trio

from .config import OndaSettings
from .identity import Identity
from .ipc import IPCError, IPCServer
from .llm import LLMBackend, build_prompt, make_backend
from .log import get_logger
from .memory import MemoryStore
from .protocol import (
    DiscoveryBody,
    Envelope,
    MessageType,
    ProximityCarrierBody,
    TaskRequestBody,
    TaskResponseBody,
)
from .transport import LibP2PTransport, PeerHandle

# v0.2 imports are deliberately deferred so a v0.1 user with ADD-ONLY
# defaults doesn't have to install BLE/WiFi-Direct extras to run a node.
# `_build_transport_v2()` does the import lazily.

log = get_logger(__name__)


# ---- Recently-handled task log (in-memory, cleared on restart) ----------


@dataclass
class HandledTask:
    received_at: float
    sender_did: str
    sender_name: str | None
    prompt: str
    answer: str


class Node:
    def __init__(self, settings: OndaSettings) -> None:
        self.settings = settings
        settings.ensure_dirs()

        self.identity = Identity.load_or_create(
            name=settings.name, path=settings.identity_path
        )
        self.memory = MemoryStore(settings.memory_path)
        self.llm: LLMBackend = make_backend(settings)

        # ADD-ONLY: in v1 we keep the single LibP2PTransport. In v2 we
        # additionally spin up the multi-transport stack. `self.transport`
        # is always set so legacy callers still find it; `self.manager` is
        # only set when transport_mode == "v2".
        self.transport = LibP2PTransport(
            identity=self.identity,
            settings=settings,
            on_envelope=self._on_envelope,
        )
        self.manager = None  # set in _build_transport_v2 if needed
        self._libp2p_host = None  # shared host for v2 internet+lan
        self._v2_transports: list = []  # concrete transports owned by manager
        self._proximity = None  # ProximityTransport, if enabled

        self.ipc = IPCServer(settings.ipc_socket_path)
        self._register_ipc_methods()

        # Bounded ring of recently-handled inbound TaskRequests for `recv`.
        self._recv_log: deque[HandledTask] = deque(maxlen=128)

    # ---- Lifecycle ---------------------------------------------------

    async def run(self) -> None:
        """Run libp2p + IPC concurrently until cancelled.

        v1 mode (default): spawns the v0.1 LibP2PTransport and the IPC server.
        v2 mode: spawns the shared libp2p host + each enabled v0.2 transport
                 + a TransportManager + the IPC server.
        """

        log.info(
            "node.start",
            name=self.identity.name,
            did=self.identity.did,
            port=self.settings.port,
            transport_mode=self.settings.transport_mode,
        )
        if self.settings.transport_mode == "v2":
            await self._run_v2()
        else:
            await self._run_v1()

    async def _run_v1(self) -> None:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.transport.serve)
            nursery.start_soon(self.ipc.serve)
            await trio.sleep_forever()

    async def _run_v2(self) -> None:
        # Build the v2 stack lazily so import errors only surface when the
        # user explicitly opts in.
        await self._build_transport_v2()
        assert self.manager is not None
        async with trio.open_nursery() as nursery:
            # Shared libp2p host: brought up first because Internet/LAN
            # both need it before they can dial.
            if self._libp2p_host is not None:
                nursery.start_soon(self._libp2p_host.serve)
            nursery.start_soon(self.manager.start)

            # Some transports own auxiliary tasks (zeroconf browser, BLE
            # scanner, scan loop) — they expose a `serve()` we spawn here.
            for t in self._v2_transports:
                serve = getattr(t, "serve", None)
                if callable(serve):
                    nursery.start_soon(serve)
                # Internet's bootstrap dial needs the host listening first.
                dial_bs = getattr(t, "dial_bootstrap", None)
                if callable(dial_bs):
                    nursery.start_soon(dial_bs)

            nursery.start_soon(self.ipc.serve)
            await trio.sleep_forever()

    async def _build_transport_v2(self) -> None:
        """Construct (but don't start) the v2 transport stack.

        This is where every v0.2 feature flag gets honored. Each enabled
        transport is appended to the list passed to `TransportManager`.
        """

        from .network import TransportManager
        from .transports._libp2p_shared import Libp2pHost

        host = Libp2pHost(
            identity=self.identity,
            host_addr=self.settings.host,
            port=self.settings.port,
        )
        self._libp2p_host = host

        transports: list = []
        if self.settings.enable_internet:
            from .transports.internet import InternetTransport

            transports.append(InternetTransport(host=host, settings=self.settings))
        if self.settings.enable_lan:
            from .transports.lan import LanTransport

            transports.append(LanTransport(host=host, settings=self.settings))
        if self.settings.enable_bluetooth:
            from .transports.bluetooth import BluetoothTransport

            transports.append(
                BluetoothTransport(identity=self.identity, settings=self.settings)
            )
        if self.settings.enable_wifi_direct:
            from .transports.wifi_direct import WifiDirectTransport

            transports.append(WifiDirectTransport(host=host, settings=self.settings))
        if self.settings.enable_proximity:
            from .transports.proximity import ProximityTransport

            # Proximity piggybacks on every other transport for actual
            # bytes. We pass them in here so it can opportunistically drain.
            self._proximity = ProximityTransport(
                identity=self.identity,
                mailbox_path=self.settings.proximity_mailbox_path,
                carriers=list(transports),
                ttl_seconds=self.settings.proximity_ttl_seconds,
                max_hops=self.settings.proximity_max_hops,
                max_mailbox_rows=self.settings.proximity_max_mailbox_rows,
            )
            transports.append(self._proximity)

        self._v2_transports = transports

        # Priority overrides from settings.transport_priority.
        priority_override: dict = {}
        for idx, name in enumerate(self.settings.transport_priority):
            priority_override[name] = idx * 10  # 0, 10, 20, …

        self.manager = TransportManager(
            transports,
            on_envelope=self._on_envelope_v2,
            priority_override=priority_override,
        )

    # ---- Inbound envelope dispatcher --------------------------------

    async def _on_envelope(
        self, env: Envelope, peer: PeerHandle
    ) -> Envelope | None:
        """Called by the transport for every verified inbound envelope.

        Signature has already been verified by the transport. Our job is to
        match on type and produce a response envelope (or None for fire-
        and-forget messages).
        """

        match env.type:
            case MessageType.DISCOVERY:
                return self._handle_discovery(env, peer)
            case MessageType.TASK_REQUEST:
                return await self._handle_task_request(env, peer)
            case MessageType.TASK_RESPONSE:
                # Responses are correlated by the caller in send_envelope;
                # arriving here means a peer pushed an unsolicited response.
                # We log and ignore — multi-hop routing is out of scope for v0.1.
                log.debug("inbound.unsolicited_response", id=env.id)
                return None
            case MessageType.PROXIMITY_CARRIER:
                # v0.1 path doesn't run a proximity transport, but a peer
                # might still send us a carrier. Drop it cleanly.
                log.debug("inbound.proximity_carrier_in_v1_mode", id=env.id)
                return None

    async def _on_envelope_v2(self, env: Envelope, peer) -> Envelope | None:
        """v0.2 inbound dispatch.

        Same as _on_envelope but uses PeerEndpoint instead of PeerHandle and
        knows how to unwrap PROXIMITY_CARRIER messages so the inner task
        looks identical to a directly-delivered one.
        """

        # Adapt PeerEndpoint to a duck-typed PeerHandle-like object so we
        # can re-use _handle_task_request without rewriting it.
        class _EndpointAsHandle:
            def __init__(self, ep) -> None:
                self.peer_id = type("_ID", (), {"to_base58": lambda _self=None: ep.address})()
                self.did = ep.did
                self.name = ep.name

        proxied = _EndpointAsHandle(peer)

        match env.type:
            case MessageType.DISCOVERY:
                return self._handle_discovery(env, proxied)
            case MessageType.TASK_REQUEST:
                return await self._handle_task_request(env, proxied)
            case MessageType.TASK_RESPONSE:
                log.debug("inbound.unsolicited_response", id=env.id)
                return None
            case MessageType.PROXIMITY_CARRIER:
                if self._proximity is None:
                    log.debug("inbound.carrier_no_proximity", id=env.id)
                    return None
                body = ProximityCarrierBody.model_validate(env.body)
                inner = await self._proximity.handle_inbound_carrier(body, peer)
                if inner is None:
                    return None
                # The inner envelope is the original task request from the
                # ORIGINAL author. Re-dispatch it to ourselves so the rest
                # of the pipeline (LLM, recv log) treats it identically to
                # a direct delivery.
                return await self._on_envelope_v2(inner, peer)

    def _handle_discovery(self, env: Envelope, peer: PeerHandle) -> Envelope:
        body = DiscoveryBody(
            name=self.identity.name,
            libp2p_addrs=self.transport.listen_addrs(),
        )
        return Envelope.build(
            identity=self.identity,
            msg_type=MessageType.DISCOVERY,
            body=body,
            recipient=env.sender,
        )

    async def _handle_task_request(
        self, env: Envelope, peer: PeerHandle
    ) -> Envelope:
        try:
            req = env.parsed_body(self.identity)
            assert isinstance(req, TaskRequestBody)
        except Exception as exc:
            log.warning("task.parse_failed", err=str(exc))
            return self._build_error_response(env, error=f"bad_request: {exc}")

        log.info(
            "task.received",
            from_did=env.sender,
            from_name=peer.name,
            prompt_chars=len(req.prompt),
        )

        try:
            system = build_prompt(
                owner_name=self.identity.name,
                memory=self.memory,
                settings=self.settings,
            )
            answer = await self.llm.complete(
                system=system, user=req.prompt, max_tokens=req.max_tokens
            )
        except Exception as exc:
            log.exception("task.llm_failed")
            return self._build_error_response(env, error=f"llm_error: {exc}")

        self._recv_log.append(
            HandledTask(
                received_at=time.time(),
                sender_did=env.sender,
                sender_name=peer.name,
                prompt=req.prompt,
                answer=answer,
            )
        )

        body = TaskResponseBody(
            in_reply_to=env.id,
            answer=answer,
            responder_name=self.identity.name,
        )
        return Envelope.build(
            identity=self.identity,
            msg_type=MessageType.TASK_RESPONSE,
            body=body,
            recipient=env.sender,
        )

    def _build_error_response(self, req_env: Envelope, *, error: str) -> Envelope:
        body = TaskResponseBody(
            in_reply_to=req_env.id,
            answer="",
            responder_name=self.identity.name,
            error=error,
        )
        return Envelope.build(
            identity=self.identity,
            msg_type=MessageType.TASK_RESPONSE,
            body=body,
            recipient=req_env.sender,
        )

    # ---- IPC method registration ------------------------------------

    def _register_ipc_methods(self) -> None:
        self.ipc.register("ask", self._ipc_ask)
        self.ipc.register("remember", self._ipc_remember)
        self.ipc.register("peers", self._ipc_peers)
        self.ipc.register("recv", self._ipc_recv)
        self.ipc.register("info", self._ipc_info)

    async def _ipc_ask(self, params: dict[str, Any]) -> dict[str, Any]:
        prompt = params.get("prompt", "").strip()
        max_tokens = int(params.get("max_tokens", 512))
        recipient_did = params.get("to")  # optional
        if not prompt:
            raise IPCError(code="bad_params", message="prompt is required")

        # v1 vs v2 differ in WHICH transport we ask "do we have anyone?".
        if self.settings.transport_mode == "v2":
            if self.manager is None or not self.manager.known_peers():
                raise IPCError(code="no_peers", message="no peers connected")
        else:
            if not self.transport.known_peers():
                raise IPCError(code="no_peers", message="no peers connected")

        body = TaskRequestBody(prompt=prompt, max_tokens=max_tokens)
        envelope = Envelope.build(
            identity=self.identity,
            msg_type=MessageType.TASK_REQUEST,
            body=body,
            recipient=recipient_did,
        )

        if self.settings.transport_mode == "v2":
            assert self.manager is not None
            if recipient_did:
                reply = await self.manager.send_envelope(envelope, recipient_did=recipient_did)
                replies = [reply] if reply is not None else []
            else:
                replies = await self.manager.broadcast_envelope(envelope)
        elif recipient_did:
            reply = await self.transport.send_envelope(
                recipient_did=recipient_did, envelope=envelope
            )
            replies = [reply] if reply is not None else []
        else:
            replies = await self.transport.broadcast_envelope(envelope)

        out: list[dict[str, Any]] = []
        for r in replies:
            if r is None or not r.verify():
                continue
            try:
                rb = r.parsed_body(self.identity)
                assert isinstance(rb, TaskResponseBody)
            except Exception:
                continue
            out.append(
                {
                    "from_did": r.sender,
                    "from_name": rb.responder_name,
                    "answer": rb.answer,
                    "error": rb.error,
                    "in_reply_to": rb.in_reply_to,
                }
            )
        return {"answers": out}

    async def _ipc_remember(self, params: dict[str, Any]) -> dict[str, Any]:
        content = params.get("content", "").strip()
        tags = str(params.get("tags") or "")
        if not content:
            raise IPCError(code="bad_params", message="content is required")
        frag = self.memory.remember(content=content, tags=tags)
        return {"id": frag.id, "created_at": frag.created_at}

    async def _ipc_peers(self, _: dict[str, Any]) -> dict[str, Any]:
        peers: list[dict[str, Any]] = []
        if self.settings.transport_mode == "v2" and self.manager is not None:
            for rec in self.manager.known_peers():
                peers.append(
                    {
                        "did": rec.did,
                        "name": rec.name,
                        "transports": sorted(rec.endpoints.keys()),
                        "endpoints": [
                            {
                                "transport": ep.transport,
                                "address": ep.address,
                                "last_seen": ep.last_seen,
                            }
                            for ep in rec.endpoints.values()
                        ],
                    }
                )
        else:
            for p in self.transport.known_peers():
                peers.append(
                    {
                        "peer_id": p.peer_id.to_base58(),
                        "did": p.did,
                        "name": p.name,
                        "addrs": p.multiaddrs,
                    }
                )
        return {"peers": peers}

    async def _ipc_recv(self, _: dict[str, Any]) -> dict[str, Any]:
        return {
            "tasks": [
                {
                    "received_at": t.received_at,
                    "sender_did": t.sender_did,
                    "sender_name": t.sender_name,
                    "prompt": t.prompt,
                    "answer": t.answer,
                }
                for t in list(self._recv_log)
            ]
        }

    async def _ipc_info(self, _: dict[str, Any]) -> dict[str, Any]:
        addrs: list[str] = []
        peer_id = ""
        if self.settings.transport_mode == "v2":
            if self._libp2p_host is not None:
                try:
                    addrs = self._libp2p_host.listen_addrs()
                    peer_id = self._libp2p_host.peer_id.to_base58()
                except RuntimeError:
                    pass
            active = self.manager.active_transports() if self.manager else []
        else:
            try:
                addrs = self.transport.listen_addrs()
                peer_id = self.transport.peer_id.to_base58()
            except RuntimeError:
                pass
            active = []
        return {
            "name": self.identity.name,
            "did": self.identity.did,
            "peer_id": peer_id,
            "addrs": addrs,
            "transport_mode": self.settings.transport_mode,
            "active_transports": active,
            "settings": {
                "host": self.settings.host,
                "port": self.settings.port,
                "llm_backend": self.settings.llm_backend,
                "ollama_model": self.settings.ollama_model,
                "enable_mdns": self.settings.enable_mdns,
                "enable_encryption": self.settings.enable_encryption,
                "enable_internet": self.settings.enable_internet,
                "enable_lan": self.settings.enable_lan,
                "enable_bluetooth": self.settings.enable_bluetooth,
                "enable_wifi_direct": self.settings.enable_wifi_direct,
                "enable_proximity": self.settings.enable_proximity,
            },
        }
