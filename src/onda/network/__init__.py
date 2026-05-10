"""Onda v0.2 network layer.

This package introduces an abstract `Transport` ABC and a `TransportManager`
that orchestrates multiple concrete transports (libp2p Internet, libp2p LAN,
Bluetooth, Wi-Fi Direct, Proximity store-and-forward) under a single API.

v0.1's `src/onda/transport.py` is intentionally untouched (ADD-ONLY): it
remains the default behind `OndaSettings.transport_mode = 'v1'`. The v0.2
machinery activates with `transport_mode = 'v2'`.
"""

from __future__ import annotations

from .transport_base import (
    IncomingFrame,
    PeerEndpoint,
    Transport,
    TransportError,
    TransportName,
    TransportPriority,
)
from .manager import TransportManager

__all__ = [
    "IncomingFrame",
    "PeerEndpoint",
    "Transport",
    "TransportError",
    "TransportManager",
    "TransportName",
    "TransportPriority",
]
