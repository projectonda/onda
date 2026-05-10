# Onda v0.2 Transports

This document describes each concrete transport, its operating principle,
the platforms it works on, the limitations to expect, and how to test it
locally without specialized hardware.

The key invariant common to every transport: **Onda envelopes are signed
end-to-end with the original author's DID key, and (optionally) sealed
end-to-end with the recipient's X25519 key**. The transport layer never
sees plaintext payloads it isn't supposed to. A relay node carrying a
proximity carrier knows *who* and *to whom* but not *what*.

## Quick reference

| Transport     | Module                     | Priority | Default | Discovery                  | Bytes path                   |
| ---           | ---                        | ---      | ---     | ---                        | ---                          |
| `internet`    | `transports/internet.py`   | 0        | on      | manual `--bootstrap`       | shared libp2p host (TCP)     |
| `lan`         | `transports/lan.py`        | 10       | on      | zeroconf mDNS              | shared libp2p host (TCP)     |
| `wifi_direct` | `transports/wifi_direct.py`| 30       | off     | SSID prefix `Onda-…`       | shared libp2p host (over Wi-Fi Direct link) |
| `bluetooth`   | `transports/bluetooth.py`  | 50       | off     | BLE advertisement          | custom GATT service          |
| `mock`        | `transports/mock.py`       | 90       | tests   | `MockBus.connect(...)`     | in-memory                    |
| `proximity`   | `transports/proximity.py`  | 100      | off     | piggyback on others        | encrypted carrier over any   |

## Internet (libp2p TCP + manual bootstrap)

* **Module**: `src/onda/transports/internet.py`
* **Discovery**: explicit multiaddrs passed via `--bootstrap` or
  `OndaSettings.bootstrap`. Persisted across restarts via `~/.onda/<name>/`.
* **Limits**: cannot discover peers not already known. Add a DHT here in
  v0.3 if needed.
* **Test locally**: spin up two daemons, copy peer A's full
  `/ip4/.../tcp/.../p2p/12D3...` multiaddr, give it to peer B as
  `--bootstrap`. See `examples/demo_two_nodes.sh` from v0.1.

## LAN (libp2p over the same host, mDNS-discovered)

* **Module**: `src/onda/transports/lan.py`
* **Discovery**: zeroconf advertises `_onda._tcp.local.` with TXT record
  containing the peer's `peer_id` and `did`. Browser dials any peer found.
* **Limits**: only works on the same broadcast domain. Most modern LANs
  (consumer Wi-Fi, wired ethernet) work; some "guest" networks block mDNS.
* **Test locally**: enable mDNS on two daemons (default), no bootstrap;
  they should find each other within ~5 seconds.
* **Cable-cut demo**: see `examples/demo_cable_cut_lan.sh`. Internet
  cut at the OS firewall, LAN keeps working.

## Wi-Fi Direct (`pywifi`)

* **Module**: `src/onda/transports/wifi_direct.py`
* **Discovery**: SSID convention `Onda-<peer_id_prefix>`. The transport
  scans for these and announces them as peer endpoints.
* **Hosting**: v0.2 ships scan-only; creating a hotspot programmatically
  is platform-specific and out of the auto-start path. On Linux, do it
  with `nmcli`:

  ```bash
  nmcli device wifi hotspot ifname wlan0 \
      con-name onda-host \
      ssid Onda-$(hostname -s) \
      band bg \
      password "lassoffinepoidi17"
  ```

  On Windows: `netsh wlan set hostednetwork mode=allow ssid=Onda-foo
  key=...`. macOS does not provide a programmatic equivalent.
* **Platform reality**: usable on Linux + Windows. **macOS returns
  `is_available() = False`** because Apple's user-space lacks
  programmatic Wi-Fi Direct.
* **Once joined**: the link looks like a regular LAN, so the
  shared libp2p host handles bytes; mDNS may also work over the link.
* **Test locally**: skip on Mac; run on Linux with two adapters in
  monitor mode, or on a Pi with `wlan0` configured for AP mode (see
  `docs/pi_setup.md`).

## Bluetooth Low Energy (custom GATT service)

* **Module**: `src/onda/transports/bluetooth.py`
* **GATT layout**:

  * Service `6f6e6461-0000-1000-8000-00805f9b34fb` (the leading bytes are
    "onda" in ASCII).
  * Characteristic `…0001…`: WRITE — peer writes inbound fragments here.
  * Characteristic `…0002…`: NOTIFY/READ — we push outbound fragments.
  * Characteristic `…0003…`: READ — exposes our DID for early dedupe.
* **Roles**: each Onda node runs **both** central (scanner / writer) and
  peripheral (advertiser / GATT server) roles concurrently. macOS,
  Linux/BlueZ, Windows all permit this.
* **Fragmentation**: GATT MTU is 23 B by default and can be negotiated up
  to ~512 B. Onda envelopes are routinely 1–2 KiB after signature, so we
  fragment on send and reassemble per-(peer, msg_id) on receive. Logic is
  pure-Python and unit-tested in `tests/test_ble_framing.py`.
* **Permissions on macOS**: the first time an Onda daemon advertises, the
  OS will prompt the user for Bluetooth access. Headless servers on
  macOS therefore need a one-time interactive launch.
* **Limits**: GATT is per-peripheral; BLE bandwidth is ~5–50 KiB/s
  realistic. Suitable for short tasks, not for streaming.
* **Test locally on a single Mac**: not possible (one BLE adapter). Use
  two devices, e.g. two Macs or one Mac and one Pi (see `docs/pi_setup.md`).
* **Mock-driven tests**: `tests/test_ble_framing.py` covers the
  fragment/reassemble logic which is the most fragile real-world bit.

## Proximity (store-and-forward)

* **Module**: `src/onda/transports/proximity.py`
* **Mailbox**: SQLite at `~/.onda/<name>/proximity_mailbox.sqlite`.
  Persistent across restarts. Default size cap 1000 rows; default carrier
  TTL 7 days; default max_hops 4.
* **Carrier**: a `ProximityCarrier`-typed Onda envelope wrapping a
  NaCl-box-encrypted inner envelope addressed to the FINAL recipient.
  Relays cannot decrypt; only the named recipient can.
* **How a relay forwards**: when any underlying transport (BLE, LAN,
  Wi-Fi Direct, Internet, …) discovers a peer, the proximity transport
  iterates its mailbox and pushes carriers either:
  1. directly to the recipient if currently visible, or
  2. opportunistically to anyone else (gossip), incrementing `hop_count`.
* **Anti-loop**: every carrier has a UUID. The mailbox dedupes by UUID;
  re-arrivals only tick a `seen_count`.
* **Privacy**: Onda uses `did:key` so the recipient's X25519 public key is
  derivable from the DID alone — no key-server lookup, no centralised
  identity. A relay sees only the metadata fields (sender, recipient,
  timestamps, hop count); the inner payload is opaque.
* **Test locally**: pure-Python with `MockTransport`. See
  `tests/test_proximity.py::test_a_sends_to_c_via_b` for the canonical
  three-node flow.

## Picking transports per deployment

Set `transport_priority` in settings (or `--transport-priority` env var) to
re-order. Default ordering is "fastest first, sneakernet last":

```
internet → lan → wifi_direct → bluetooth → proximity
```

A field-deployed sensor might invert this, dropping internet and
preferring proximity for opportunistic mule-style delivery.

## Adding a new transport (ADD-ONLY)

1. Create `src/onda/transports/<your_transport>.py` subclassing
   `onda.network.Transport`.
2. Implement `is_available`, `start`, `stop`, `peers`, `discover`, `send`.
3. Add a corresponding `enable_<name>` flag to `OndaSettings` (default
   `False`).
4. Wire it in `Node._build_transport_v2()` behind that flag.
5. Add unit tests using `MockTransport` for any logic that doesn't need
   real hardware.

The v0.1 `transport.py` is **not** to be modified — keep adding modules.
