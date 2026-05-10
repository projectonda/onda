# Changelog

All notable changes to Onda. The project follows the principle that
**each feature is added behind a flag, never deleted** — a CHANGELOG
entry that talks about removing existing behavior should be very rare.

## [0.2.0] — 2026-05-09

### Added — multi-transport stack

* **Transport abstraction** (`src/onda/network/`):
  - `Transport` ABC with the per-protocol contract (discover / connect /
    send / receive / `is_available`).
  - `PeerEndpoint` aggregating one peer's reachable address per transport.
  - `TransportManager` with priority-based selection, fallback on
    transport errors, and DID-keyed deduplication of peers.

* **Concrete transports** (`src/onda/transports/`):
  - `internet.py` — libp2p TCP + manual bootstrap, conforming to the new
    ABC. Uses the same shared libp2p host as `lan.py`.
  - `lan.py` — libp2p over zeroconf-discovered peers (mDNS).
  - `bluetooth.py` — custom GATT service over BLE, both peripheral
    (advertising via `bless`) and central (scanning via `bleak`) roles.
    Pure-Python fragment / reassemble logic in `_ble_framing.py` so the
    bytes-level invariants are CI-testable without hardware.
  - `wifi_direct.py` — `pywifi`-based scan for Onda-prefixed SSIDs;
    transport remains scan-only by default. Reports
    `is_available() = False` on macOS (Apple's user-space lacks
    programmatic Wi-Fi Direct).
  - `proximity.py` + `_mailbox.py` — store-and-forward layer with
    end-to-end NaCl-box encryption (X25519 derived from `did:key`),
    UUID-based anti-loop, hop-limit and TTL-based abuse limits, and
    SQLite-backed persistence at `~/.onda/<name>/proximity_mailbox.sqlite`.
  - `mock.py` — `MockBus` + `MockTransport` for CI tests; topology is
    explicit so tests can simulate "A sees B, B sees C, A doesn't see C".

* **Protocol** (`src/onda/protocol.py`):
  - New `MessageType.PROXIMITY_CARRIER` and `ProximityCarrierBody`. The
    inner envelope stays encrypted end-to-end inside; relays only see
    metadata.

* **Settings** (`src/onda/config.py`):
  - `transport_mode: "v1" | "v2"` (default `"v1"` — ADD-ONLY).
  - `enable_internet`, `enable_lan`, `enable_bluetooth`,
    `enable_wifi_direct`, `enable_proximity`.
  - `transport_priority` (list of transport names; default
    "fastest first, sneakernet last").
  - `proximity_ttl_seconds`, `proximity_max_hops`,
    `proximity_max_mailbox_rows` for the S&F layer.

* **Node** (`src/onda/node.py`):
  - `_run_v2()` and `_build_transport_v2()` parallel paths. The v0.1
    `_run_v1()` is unchanged.
  - `_on_envelope_v2()` knows how to unwrap PROXIMITY_CARRIER envelopes
    and re-dispatch the decrypted inner task as if directly received.

* **CLI** (`src/onda/cli.py`):
  - `--transport-mode v1|v2`, `--enable-bluetooth`, `--enable-wifi-direct`,
    `--enable-proximity`. Existing v0.1 flags work identically.
  - `peers` output extended in v2: shows per-transport endpoints per peer.
  - `info` output extended in v2: shows `active_transports` and the
    per-transport enable flags.

* **Tests**:
  - `test_transport_base.py`, `test_mock_transport.py`, `test_manager.py`
  - `test_ble_framing.py` (pure-Python fragment / reassemble)
  - `test_wifi_direct.py` (mocked pywifi interface)
  - `test_proximity.py` (canonical A→B→C three-node walkthrough)

* **Docs**:
  - `docs/transports.md` — per-transport behavior, limits, platforms.
  - `docs/network-architecture.md` — block diagram + multi-hop byte flow.
  - `docs/pi_setup.md` — step-by-step real-hardware demo on Raspberry Pi.

* **Examples**:
  - `examples/demo_cable_cut_lan.sh` — two daemons discover each other
    via mDNS without any bootstrap multiaddr ("cable-cut" demo).
  - `examples/demo_proximity_three_nodes.py` — pure-Python S&F walkthrough.

* **Optional install extras**: `onda[ble]`, `onda[wifi-direct]`,
  `onda[all-transports]`. The `dev` extra includes them all.

### Changed

* `pytest` config switched from `asyncio_mode = "auto"` to
  `asyncio_mode = "strict"` + `trio_mode = "true"` so `pytest-trio` and
  `pytest-asyncio` cooperate. Existing tests are sync and unaffected.

### Unchanged (ADD-ONLY)

* `src/onda/transport.py` (the v0.1 `LibP2PTransport`) is byte-for-byte
  identical. v0.1 daemons still run with `transport_mode = "v1"` (default).

## [0.1.0] — 2026-05-07

Initial reference implementation. See README for the v0.1 feature set.
