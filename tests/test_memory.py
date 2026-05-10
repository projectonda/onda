"""Memory store tests.

Covers basic CRUD plus the v0.1 retrieval invariant (full dump capped at
`memory_max_chars`). Embedding / FTS5 retrieval is gated behind future
flags; when added, write a sibling test file rather than rewriting this one.
"""

from __future__ import annotations

from pathlib import Path

from onda.memory import MemoryStore


def test_remember_returns_fragment(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "m.sqlite")
    f = store.remember("Il pomodoro siciliano è dolce.")
    assert f.id > 0
    assert f.content.startswith("Il pomodoro")


def test_isolation_per_store(tmp_path: Path) -> None:
    a = MemoryStore(tmp_path / "a.sqlite")
    b = MemoryStore(tmp_path / "b.sqlite")
    a.remember("only-in-a")
    b.remember("only-in-b")
    assert [f.content for f in a.all_fragments()] == ["only-in-a"]
    assert [f.content for f in b.all_fragments()] == ["only-in-b"]


def test_context_for_prompt_caps_at_max_chars(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "m.sqlite")
    for i in range(20):
        store.remember(f"frammento {i:02d} " + "x" * 50)
    out = store.context_for_prompt(max_chars=200)
    assert len(out) <= 200
    assert "frammento 00" in out


def test_forget_removes_row(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "m.sqlite")
    f = store.remember("ephemeral")
    assert store.forget(f.id) is True
    assert store.all_fragments() == []
    assert store.forget(f.id) is False


def test_persists_across_instances(tmp_path: Path) -> None:
    p = tmp_path / "m.sqlite"
    s1 = MemoryStore(p)
    s1.remember("survive")
    s1.close()
    s2 = MemoryStore(p)
    assert [f.content for f in s2.all_fragments()] == ["survive"]
