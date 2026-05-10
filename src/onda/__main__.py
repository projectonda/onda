"""Allow `python -m onda …` as an alias for the installed `onda` script."""

from __future__ import annotations

from onda.cli import app

if __name__ == "__main__":
    app()
