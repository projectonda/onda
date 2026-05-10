"""Spec-literal entry point: lets users run `python cli.py …` from the repo root.

The real implementation lives in `src/onda/cli.py`; this module is a shim so the
demo commands documented in the README and in projectonda.com work verbatim.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running this file before the package is `pip install -e .`-d.
_SRC = Path(__file__).resolve().parent / "src"
if _SRC.exists() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from onda.cli import app  # noqa: E402

if __name__ == "__main__":
    app()
