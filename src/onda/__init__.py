"""Onda — peer-to-peer protocol for personal AIs.

Reference implementation, v0.1. See https://projectonda.com for the manifesto.

Public surface intentionally small: most consumers use the CLI; library users
import `Node`, `OndaSettings`, and the protocol message classes.
"""

from __future__ import annotations

__version__ = "0.1.0"
__protocol_id__ = "/onda/1.0.0"  # libp2p stream protocol identifier
__jsonld_context__ = "https://projectonda.com/ns/onda/v1"
