"""Local knowledge store.

Each node owns a single SQLite file under `~/.onda/<name>/memory.sqlite`.
Memory is private to the node by spec — it is consulted to construct LLM
context but is never sent over the wire as such. Only the LLM's *answer*
leaves the node.

v0.1 retrieval strategy is deliberately the simplest possible: dump every
fragment into the prompt, capped at `OndaSettings.memory_max_chars` so we
don't blow past the model's context window. Smarter retrieval (FTS5,
embeddings) is gated for a future flag and is *not* removed here when added —
this is the ADD-ONLY discipline.
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


_SCHEMA = """
CREATE TABLE IF NOT EXISTS fragments (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    content      TEXT NOT NULL,
    tags         TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fragments_created_at ON fragments(created_at);
"""


@dataclass(frozen=True)
class Fragment:
    id: int
    content: str
    tags: str
    created_at: str


class MemoryStore:
    """Thread-safe wrapper over a single SQLite file.

    SQLite's default thread-check is per-connection; we serialize via a lock
    rather than connection-per-thread because nodes are low-QPS and the lock
    is far easier to reason about during a demo.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(self.path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)

    # ---- Lifecycle -------------------------------------------------------

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def _cursor(self) -> Iterator[sqlite3.Cursor]:
        with self._lock:
            cur = self._conn.cursor()
            try:
                yield cur
            finally:
                cur.close()

    # ---- CRUD ------------------------------------------------------------

    def remember(self, content: str, tags: str = "") -> Fragment:
        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO fragments(content, tags, created_at) VALUES (?, ?, ?)",
                (content, tags, ts),
            )
            row_id = cur.lastrowid
        return Fragment(id=row_id or 0, content=content, tags=tags, created_at=ts)

    def all_fragments(self) -> list[Fragment]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT id, content, tags, created_at FROM fragments ORDER BY id ASC"
            )
            rows = cur.fetchall()
        return [
            Fragment(id=r["id"], content=r["content"], tags=r["tags"], created_at=r["created_at"])
            for r in rows
        ]

    def forget(self, fragment_id: int) -> bool:
        with self._cursor() as cur:
            cur.execute("DELETE FROM fragments WHERE id = ?", (fragment_id,))
            return cur.rowcount > 0

    # ---- Retrieval for prompt construction -------------------------------

    def context_for_prompt(self, max_chars: int) -> str:
        """Return all fragments concatenated, hard-capped to `max_chars`.

        We append fragments oldest-first until we'd cross the cap, then stop.
        Truncating the *last* fragment in the middle of a sentence is worse
        than dropping it entirely, so we drop.
        """

        if max_chars <= 0:
            return ""
        out: list[str] = []
        used = 0
        for frag in self.all_fragments():
            piece = f"- {frag.content.strip()}\n"
            if used + len(piece) > max_chars:
                break
            out.append(piece)
            used += len(piece)
        return "".join(out)
