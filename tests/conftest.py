"""Shared pytest fixtures.

We always run with `llm_backend=echo` in tests so nothing depends on a real
Ollama process. Any test that wants real inference must be marked
`@pytest.mark.requires_ollama` and is skipped by default in CI.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from onda.config import OndaSettings
from onda.identity import Identity


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "requires_ollama: needs a running Ollama daemon at ONDA_OLLAMA_URL",
    )
    config.addinivalue_line(
        "markers",
        "requires_libp2p: needs a working py-libp2p stack (boots subprocess nodes)",
    )


@pytest.fixture
def home_dir(tmp_path: Path) -> Path:
    return tmp_path / "onda-home"


@pytest.fixture
def settings_factory(home_dir: Path):
    def _make(**overrides: object) -> OndaSettings:
        base: dict[str, object] = {
            "name": "test-node",
            "home_dir": home_dir,
            "host": "127.0.0.1",
            "port": 0,  # let OS pick when libp2p actually binds in integration tests
            "enable_mdns": False,
            "llm_backend": "echo",
        }
        base.update(overrides)
        return OndaSettings(**base)  # type: ignore[arg-type]

    return _make


@pytest.fixture
def identity(tmp_path: Path) -> Identity:
    return Identity.load_or_create(name="alice", path=tmp_path / "id.json")
