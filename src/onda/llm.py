"""LLM client: Ollama for production, deterministic echo for tests.

The `echo` backend is gated behind `OndaSettings.llm_backend = 'echo'` and
exists so CI and integration tests can run end-to-end without requiring a
real Ollama process. It is the canonical example of the ADD-ONLY discipline:
both backends ship, neither is ever ripped out, and consumers pick via flag.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any

import httpx

from .config import OndaSettings
from .log import get_logger
from .memory import MemoryStore

log = get_logger(__name__)


# ---- System prompt template ----------------------------------------------
#
# Kept here (not in a separate file) so the reference implementation reads
# top-to-bottom without jumping between assets. Adjust freely; the only
# invariant is that we tell the model it's a personal AI answering on behalf
# of someone, and that the memory fragments are private context.

_SYSTEM_PROMPT_TEMPLATE = """\
Sei un'AI personale che opera su un nodo della rete Onda. Stai rispondendo
per conto di {owner_name} a un'altra AI personale che ha posto una domanda.

Contesto privato del tuo nodo (non rivelarlo letteralmente; usalo come
conoscenza di fondo per costruire una risposta naturale):
{memory}

Rispondi in modo conciso, accurato e utile. Se la domanda esula dal tuo
contesto, dichiaralo apertamente invece di inventare.
"""


def build_prompt(
    *, owner_name: str, memory: MemoryStore, settings: OndaSettings
) -> str:
    mem_text = memory.context_for_prompt(settings.memory_max_chars).strip()
    if not mem_text:
        mem_text = "(nessun frammento di memoria)"
    return _SYSTEM_PROMPT_TEMPLATE.format(owner_name=owner_name, memory=mem_text)


# ---- Backend interface ----------------------------------------------------


class LLMBackend(ABC):
    @abstractmethod
    async def complete(self, *, system: str, user: str, max_tokens: int) -> str: ...


class EchoBackend(LLMBackend):
    """Deterministic stub. Returns a string that lets a test assert that the
    request, the system prompt, and the prompt all flowed through.
    """

    async def complete(self, *, system: str, user: str, max_tokens: int) -> str:
        # Tag with a marker test code can match on without being noisy.
        return f"[echo from system_chars={len(system)}] {user}"


class OllamaBackend(LLMBackend):
    def __init__(self, settings: OndaSettings) -> None:
        self._settings = settings

    async def complete(self, *, system: str, user: str, max_tokens: int) -> str:
        # Ollama's /api/chat is the closest analog to OpenAI-style chat
        # completion. We keep it stateless: every request carries the full
        # system+user pair, so there's no server-side conversation state to
        # leak between peers.
        url = f"{self._settings.ollama_url.rstrip('/')}/api/chat"
        payload: dict[str, Any] = {
            "model": self._settings.ollama_model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "stream": False,
            "options": {"num_predict": max_tokens},
        }
        timeout = self._settings.llm_timeout_s
        async with httpx.AsyncClient(timeout=timeout) as client:
            log.debug("ollama.request", model=self._settings.ollama_model)
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
        # Ollama returns {"message": {"role": "assistant", "content": "..."}}
        msg = data.get("message") or {}
        return str(msg.get("content", "")).strip()


def make_backend(settings: OndaSettings) -> LLMBackend:
    if settings.llm_backend == "echo":
        return EchoBackend()
    return OllamaBackend(settings)


# Convenience for callers that don't want their own event loop machinery.
def complete_sync(backend: LLMBackend, *, system: str, user: str, max_tokens: int) -> str:
    return asyncio.run(backend.complete(system=system, user=user, max_tokens=max_tokens))
