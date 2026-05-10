"""Concrete v0.2 Transport implementations.

Each module is independently importable; absence of OS-level libraries
(BlueZ, Wi-Fi adapter, etc.) is handled at runtime via `is_available()`,
not at import time, so missing hardware never breaks `from onda.transports
import …` on a CI box.
"""

from __future__ import annotations
