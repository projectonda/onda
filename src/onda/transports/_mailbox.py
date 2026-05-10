"""SQLite-backed mailbox for proximity store-and-forward carriers.

Each ProximityCarrier in flight is persisted here so that:

  * Restarting the daemon doesn't lose pending carriers.
  * The relay can apply size + age limits without holding everything in RAM.
  * Anti-loop (UUID dedup) is durable across restarts.

Schema:

    carriers(
      carrier_id     TEXT PRIMARY KEY,    -- UUID; dedupe key
      final_recipient_did TEXT NOT NULL,  -- who eventually delivers
      original_sender_did TEXT,           -- provenance, may be ''
      sealed_inner_b64 TEXT NOT NULL,     -- the encrypted inner envelope
      created_at     TEXT NOT NULL,
      expires_at     TEXT NOT NULL,
      hop_count      INTEGER NOT NULL,
      max_hops       INTEGER NOT NULL,
      delivered      INTEGER NOT NULL DEFAULT 0,
      seen_count     INTEGER NOT NULL DEFAULT 1
    );

`delivered = 1` means we've already delivered this to its final recipient
(or recognised that we are the final recipient). We keep the row around
for another expiration window so re-arrivals via different transports are
deduped silently rather than re-delivered.
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

_SCHEMA = """
CREATE TABLE IF NOT EXISTS carriers (
    carrier_id           TEXT PRIMARY KEY,
    final_recipient_did  TEXT NOT NULL,
    original_sender_did  TEXT NOT NULL DEFAULT '',
    sealed_inner_b64     TEXT NOT NULL,
    created_at           TEXT NOT NULL,
    expires_at           TEXT NOT NULL,
    hop_count            INTEGER NOT NULL,
    max_hops             INTEGER NOT NULL,
    delivered            INTEGER NOT NULL DEFAULT 0,
    seen_count           INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_carriers_recipient ON carriers(final_recipient_did, delivered);
CREATE INDEX IF NOT EXISTS idx_carriers_expires_at ON carriers(expires_at);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _from_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


@dataclass
class CarrierRow:
    carrier_id: str
    final_recipient_did: str
    original_sender_did: str
    sealed_inner_b64: str
    created_at: str
    expires_at: str
    hop_count: int
    max_hops: int
    delivered: bool
    seen_count: int


class ProximityMailbox:
    """Thread-safe wrapper for a single SQLite mailbox file."""

    def __init__(self, path: Path, *, max_rows: int = 1000) -> None:
        self.path = path
        self.max_rows = max_rows
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(self.path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)

    # ---- Lifecycle -----------------------------------------------------

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

    # ---- Insert / update ----------------------------------------------

    def store(self, row: CarrierRow) -> bool:
        """Insert a new carrier; return True if it's new, False if duplicate.

        Duplicates are silently noted (`seen_count` ticked) so a relay can
        observe how often the same carrier comes back and short-circuit
        forwarding loops.
        """

        with self._cursor() as cur:
            cur.execute(
                "SELECT carrier_id FROM carriers WHERE carrier_id = ?",
                (row.carrier_id,),
            )
            if cur.fetchone() is not None:
                cur.execute(
                    "UPDATE carriers SET seen_count = seen_count + 1 WHERE carrier_id = ?",
                    (row.carrier_id,),
                )
                return False
            cur.execute(
                """
                INSERT INTO carriers
                  (carrier_id, final_recipient_did, original_sender_did, sealed_inner_b64,
                   created_at, expires_at, hop_count, max_hops, delivered, seen_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    row.carrier_id,
                    row.final_recipient_did,
                    row.original_sender_did,
                    row.sealed_inner_b64,
                    row.created_at,
                    row.expires_at,
                    row.hop_count,
                    row.max_hops,
                    1 if row.delivered else 0,
                ),
            )
        self._enforce_size_limit()
        return True

    def mark_delivered(self, carrier_id: str) -> None:
        with self._cursor() as cur:
            cur.execute(
                "UPDATE carriers SET delivered = 1 WHERE carrier_id = ?",
                (carrier_id,),
            )

    # ---- Queries ------------------------------------------------------

    def has(self, carrier_id: str) -> bool:
        with self._cursor() as cur:
            cur.execute(
                "SELECT 1 FROM carriers WHERE carrier_id = ?",
                (carrier_id,),
            )
            return cur.fetchone() is not None

    def pending_for(self, recipient_did: str) -> list[CarrierRow]:
        """Return undelivered, unexpired carriers addressed to a peer."""

        now = _now_iso()
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT * FROM carriers
                WHERE final_recipient_did = ?
                  AND delivered = 0
                  AND expires_at > ?
                ORDER BY created_at ASC
                """,
                (recipient_did, now),
            )
            return [self._row(r) for r in cur.fetchall()]

    def all_forwardable(self, *, limit: int = 50) -> list[CarrierRow]:
        """Return any undelivered, unexpired carriers — these are eligible
        for opportunistic forwarding to whoever we currently see."""

        now = _now_iso()
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT * FROM carriers
                WHERE delivered = 0
                  AND expires_at > ?
                  AND hop_count < max_hops
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (now, limit),
            )
            return [self._row(r) for r in cur.fetchall()]

    def all_rows(self) -> list[CarrierRow]:
        with self._cursor() as cur:
            cur.execute("SELECT * FROM carriers ORDER BY created_at ASC")
            return [self._row(r) for r in cur.fetchall()]

    # ---- Maintenance --------------------------------------------------

    def vacuum_expired(self) -> int:
        """Delete expired carriers; return count removed."""

        now = _now_iso()
        with self._cursor() as cur:
            cur.execute("DELETE FROM carriers WHERE expires_at <= ?", (now,))
            return cur.rowcount

    def _enforce_size_limit(self) -> None:
        with self._cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM carriers")
            (count,) = cur.fetchone()
            if count <= self.max_rows:
                return
            # Drop oldest delivered first, then oldest undelivered.
            excess = count - self.max_rows
            cur.execute(
                """
                DELETE FROM carriers
                WHERE carrier_id IN (
                    SELECT carrier_id FROM carriers
                    ORDER BY delivered DESC, created_at ASC
                    LIMIT ?
                )
                """,
                (excess,),
            )

    # ---- Helpers ------------------------------------------------------

    @staticmethod
    def _row(r: sqlite3.Row) -> CarrierRow:
        return CarrierRow(
            carrier_id=r["carrier_id"],
            final_recipient_did=r["final_recipient_did"],
            original_sender_did=r["original_sender_did"],
            sealed_inner_b64=r["sealed_inner_b64"],
            created_at=r["created_at"],
            expires_at=r["expires_at"],
            hop_count=r["hop_count"],
            max_hops=r["max_hops"],
            delivered=bool(r["delivered"]),
            seen_count=r["seen_count"],
        )


def expires_at_from_ttl(ttl_seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat(
        timespec="seconds"
    )


__all__ = ["CarrierRow", "ProximityMailbox", "expires_at_from_ttl"]
