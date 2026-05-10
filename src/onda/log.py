"""Structured logging setup.

We use structlog because every event in a P2P node is naturally a record
("peer connected", "task received", "signature verified") and grepping JSON
logs is far more useful than parsing free-form strings when debugging across
two terminals during the demo.

Console renderer in dev (TTY-attached), JSON renderer otherwise. The choice is
auto-detected, but `ONDA_LOG_FORMAT=json|console` overrides.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

import structlog


def _want_json() -> bool:
    fmt = os.environ.get("ONDA_LOG_FORMAT", "").lower()
    if fmt == "json":
        return True
    if fmt == "console":
        return False
    # Default: console when stderr is a TTY (interactive), JSON otherwise.
    return not sys.stderr.isatty()


_configured = False


def configure(level: str = "INFO") -> None:
    """Idempotently configure structlog + stdlib logging.

    Idempotent because tests, the daemon, and the CLI may all call it; we want
    the first caller's settings to stick rather than reset on every import.
    """

    global _configured
    if _configured:
        return

    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=log_level,
    )

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    if _want_json():
        renderer: Any = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=[*shared_processors, structlog.processors.format_exc_info, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )
    _configured = True


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a configured logger. Call `configure()` first if you need a level."""

    if not _configured:
        configure()
    return structlog.get_logger(name) if name else structlog.get_logger()
