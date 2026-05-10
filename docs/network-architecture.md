# Onda v0.2 Network Architecture

```
                  Application layer (Node, IPC, CLI, LLM)
                                │
                                ▼
                  ┌──────────────────────────┐
                  │     TransportManager     │  picks priority, falls back
                  │  (network/manager.py)    │  on errors, dedupes by DID
                  └──┬───┬───┬───┬───┬──────┘
        prio 0  ─────┘   │   │   │   └──── prio 100
        ┌─────────────┐  │   │   │        ┌──────────────────────────┐
        │  Internet   │  │   │   │        │   Proximity (S&F)        │
        │  libp2p TCP │  │   │   │        │   sealed carriers        │
        │ + bootstrap │  │   │   │        │   SQLite mailbox         │
        └─────────────┘  │   │   │        │   piggybacks on others   │
                         │   │   │        └──────────────────────────┘
                  ┌──────▼─┐ │   │
                  │  LAN   │ │   │
                  │ libp2p │ │   │
                  │  +mDNS │ │   │
                  └────────┘ │   │
                             │   │
                  ┌──────────▼─┐ │
                  │ WiFi Direct│ │
                  │ pywifi scan│ │
                  └────────────┘ │
                                 │
                       ┌─────────▼──┐
                       │  Bluetooth │
                       │  GATT cust │
                       │  + bleak   │
                       │  + bless   │
                       └────────────┘

  Internet & LAN share ONE libp2p host (`Libp2pHost`, see
  transports/_libp2p_shared.py) so a node listens on a single TCP port.
  WiFi Direct delegates to the same host once associated.
```

## End-to-end byte flow (multi-hop A → B → C via proximity)

```
   Alice (A)                              Bob (B)                              Cleo (C)
   --------                               --------                             --------
   build inner Envelope (TaskRequest)
   sign with A.ed25519                                                         
                                                                               
   ┌─ ProximityTransport.send(C, payload) ┐
   │  1. encrypt payload with C.x25519 (NaCl box)                              
   │  2. wrap as ProximityCarrier{
   │       carrier_id, recipient=C,
   │       sealed_inner=…, hop_count=0,
   │       max_hops=4, expires_at=…}
   │  3. store in A.mailbox
   │  4. attempt_drain():
   │     visible_peers = {B} (no C in range)
   │     pick B as relay
   │     OUTER envelope: ProximityCarrier
   │     signed by A (current sender)
   └────────────────┬─────────────────────┘
                    │
                    ▼  via underlying transport (BLE / LAN / mock)
                                                                               
                                            inbound: verifies A's outer sig
                                            handle_inbound_carrier:
                                              - dedup by carrier_id (new)
                                              - hop_count=1 < max_hops
                                              - recipient ≠ self (B)
                                              - store in B.mailbox
                                                                               
                                            later: B encounters C
                                            attempt_drain():
                                              visible_peers = {C}
                                              C matches recipient → DIRECT
                                              OUTER envelope: signed by B
                                              hop_count=2
                                                                               
                                                                                ▼ via underlying transport
                                                                               
                                                                               inbound: verifies B's outer sig
                                                                               handle_inbound_carrier:
                                                                                 - dedup (new)
                                                                                 - recipient = self (C)
                                                                                 - decrypt sealed_inner with C.x25519
                                                                                 - parse inner Envelope
                                                                                 - VERIFY A.ed25519 sig on inner
                                                                                 - dispatch as if directly received
```

Two distinct signatures per hop, only one ever needs to verify against
the original author:

* **Outer signature**: whoever is forwarding the carrier RIGHT NOW. Each
  hop re-signs. A relay can therefore drop carriers from peers it
  doesn't trust without losing the original-author proof.
* **Inner signature**: the original author's. Carried unchanged inside
  the sealed envelope. C verifies it after decrypting; if it fails, the
  message is dropped — even though the outer hop was trusted, the
  *content* wasn't authored by who the carrier metadata claimed.

## ADD-ONLY discipline

The v0.1 `src/onda/transport.py` is untouched. v0.2 adds:

* `src/onda/network/` — abstract layer
* `src/onda/transports/` — concrete transport implementations
* New flags in `src/onda/config.py` (defaults preserve v0.1 behavior)
* New methods in `src/onda/node.py` (existing ones unchanged)

`OndaSettings.transport_mode` defaults to `"v1"`. A user upgrading from
v0.1 sees no behavioral change unless they explicitly opt in with
`--transport-mode v2`.
