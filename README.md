# Onda

> A peer-to-peer protocol for personal AIs. No central servers. Each person runs their own AI node; nodes find each other and trade tasks directly. — [Manifesto: projectonda.com](https://projectonda.com)

Onda is the **reference implementation**, in Python. It is intentionally readable and deliberately small. Every behavioral switch is a flag (`OndaSettings`); per project policy, code is *added*, not deleted.

* **v0.1** (default, `transport_mode=v1`): single libp2p host with TCP + manual bootstrap + mDNS. The minimum viable thing.
* **v0.2** (`transport_mode=v2`): multi-transport stack — libp2p (Internet & LAN) + BLE GATT + Wi-Fi Direct + store-and-forward via proximity, all behind a `TransportManager` with priority + fallback. See [`CHANGELOG.md`](CHANGELOG.md) and [`docs/network-architecture.md`](docs/network-architecture.md).

## What you get in v0.1

- A long-lived **Onda node** (`onda start`) that
  - owns a `did:key` Ed25519 identity persisted at `~/.onda/<name>/identity.json`,
  - speaks the Onda wire protocol (`/onda/1.0.0`) over [py-libp2p](https://github.com/libp2p/py-libp2p),
  - announces / discovers peers on the local network via mDNS (Zeroconf),
  - accepts manual bootstrap multiaddrs for remote peers,
  - serves a local IPC socket so you can drive it from the CLI.
- A **CLI** (`onda ask`, `onda remember`, `onda peers`, `onda recv`, `onda info`) that talks to the running node over the IPC socket.
- A **JSON-LD-shaped envelope** (`@context`, `@type`, `from`, `to`, `body`, `signature`) for every inter-node message, signed with the sender's DID key. Receivers verify the signature *before* doing anything else.
- An **opt-in NaCl-box (X25519)** layer for end-to-end payload encryption between specific peers, on top of libp2p's Noise channel.
- A **local SQLite memory** so a node can answer questions using its owner's private knowledge — without that knowledge ever leaving the node.

Out of scope for v0.1 (explicitly): web UI, multi-hop routing, persistent peer discovery beyond mDNS+bootstrap, federated learning, cross-node sharded memory, auth beyond DID signatures.

## Architecture

```
                    Person A                                                                Person B
            ┌───────────────────────┐                                            ┌───────────────────────┐
            │  ~/.onda/leonardo/    │                                            │   ~/.onda/pietro/     │
            │   identity.json       │                                            │    identity.json      │
            │   memory.sqlite       │                                            │    memory.sqlite      │
            │   ipc.sock            │                                            │    ipc.sock           │
            └───────────┬───────────┘                                            └───────────┬───────────┘
                        │                                                                    │
                  unix socket                                                           unix socket
                        │                                                                    │
       ┌────────────────┴───────────────────┐                          ┌────────────────────┴───────────────┐
       │  onda CLI       (ask / remember /  │                          │  onda CLI       (ask / remember /  │
       │                 peers / recv)      │                          │                 peers / recv)      │
       └────────────────────────────────────┘                          └────────────────────────────────────┘

       ┌────────────────────────────────────┐                          ┌────────────────────────────────────┐
       │              Node A                │                          │              Node B                │
       │  ┌──────────────────────────────┐  │                          │  ┌──────────────────────────────┐  │
       │  │  Identity (did:key:z6Mk…)    │  │                          │  │  Identity (did:key:z6Mk…)    │  │
       │  ├──────────────────────────────┤  │                          │  ├──────────────────────────────┤  │
       │  │  Memory (SQLite)             │  │                          │  │  Memory (SQLite)             │  │
       │  ├──────────────────────────────┤  │                          │  ├──────────────────────────────┤  │
       │  │  LLM ── Ollama (localhost)   │  │                          │  │  LLM ── Ollama (localhost)   │  │
       │  └──────────────────────────────┘  │                          │  └──────────────────────────────┘  │
       │                                    │                          │                                    │
       │  libp2p host ─── /onda/1.0.0 ──────┼──────── TCP + Noise ─────┼──── /onda/1.0.0 ─── libp2p host    │
       │  zeroconf mDNS  ───────────────────┼────── _onda._tcp.local. ─┼─────────────────── zeroconf mDNS   │
       └────────────────────────────────────┘                          └────────────────────────────────────┘

   wire envelope (JSON-LD-shaped, Ed25519-signed):
   {"@context":"https://projectonda.com/ns/onda/v1",
    "@type":"TaskRequest","id":"…","issued_at":"…",
    "from":"did:key:z6Mk…","to":"did:key:z6Mk…",
    "body":{"prompt":"Come si coltiva il pomodoro a Mazara del Vallo?"},
    "signature":"…","encrypted":false}
```

## Installation

```bash
git clone https://github.com/projectonda/onda.git
cd onda
make install
source .venv/bin/activate
```

Two install-time gotchas worth flagging up front:

* **Python 3.12 or 3.13.** py-libp2p pulls in `coincurve==21.0.0` and `fastecdsa`, neither of which ships pre-built wheels for Python 3.14 yet. The Makefile honours `PYTHON=python3.12` if your system default is newer.
* **GMP headers on macOS.** `fastecdsa` builds a C extension against `libgmp`. Install with `brew install gmp`, and if the build still can't find the headers re-run `make install` with the brew prefix exported:

  ```bash
  export CPPFLAGS="-I$(brew --prefix gmp)/include"
  export LDFLAGS="-L$(brew --prefix gmp)/lib"
  PYTHON=python3.12 make install
  ```

You'll also want a local [Ollama](https://ollama.com) and a model pulled:

```bash
ollama pull llama3.2:3b   # or mistral:7b
```

If you only want to play with the protocol layer (no real LLM), set `ONDA_LLM_BACKEND=echo` and Ollama is no longer required.

## Demo: cable-cut LAN (v0.2)

Two daemons on the same Mac discover each other purely via Zeroconf mDNS — no bootstrap multiaddr — and exchange a signed task. The same configuration survives an internet outage on real hardware (see [docs/pi_setup.md](docs/pi_setup.md)).

```bash
ONDA_LLM_BACKEND=echo bash examples/demo_cable_cut_lan.sh
```

## Demo: store-and-forward across three nodes (v0.2)

A and C never see each other. B carries a sealed carrier between them. C decrypts and verifies A's original signature.

```bash
.venv/bin/python examples/demo_proximity_three_nodes.py
```

## Demo: two nodes, two terminals (v0.1)

The script `examples/demo_two_nodes.sh` automates this. You can also do it by hand:

**Terminal 1 — Leonardo's AI**

```bash
onda start --port 9001 --name "leonardo"
# stderr prints the node's DID and PeerID — copy the multiaddr like
#   /ip4/127.0.0.1/tcp/9001/p2p/12D3KooW…
```

**Terminal 2 — Pietro's AI, bootstrapped to Leonardo's**

```bash
onda start --port 9002 --name "pietro" \
    --bootstrap /ip4/127.0.0.1/tcp/9001/p2p/<LEONARDO_PEER_ID>
```

**Terminal 3 — pre-load Pietro's memory**

```bash
onda remember --name pietro \
    "Il pomodoro nella Sicilia occidentale, soprattutto a Mazara del Vallo, \
     beneficia del clima mite e dell'acqua salmastra dei pozzi costieri…"
```

**Terminal 3 — ask from Leonardo**

```bash
onda ask --name leonardo "Come si coltiva il pomodoro a Mazara del Vallo?"
```

You should see something like:

```
--- Risposta da did:key:z6MkpQK7… ('pietro') ---
La coltivazione del pomodoro a Mazara del Vallo …
```

The CLI prints the responder's full DID. The signature has already been verified by Leonardo's node — if it had been tampered with in transit, the response would have been silently dropped.

## Configuration

All knobs live in `OndaSettings` (`src/onda/config.py`). Override via flag, env var (`ONDA_*`), or `.env`:

| flag                          | env                       | default        |
| ----------------------------- | ------------------------- | -------------- |
| `--name`                      | `ONDA_NAME`               | `default`      |
| `--port`                      | `ONDA_PORT`               | `9001`         |
| `--bootstrap` (repeatable)    | `ONDA_BOOTSTRAP`          | `[]`           |
| `--no-mdns`                   | `ONDA_ENABLE_MDNS`        | `true`         |
| `--encrypt`                   | `ONDA_ENABLE_ENCRYPTION`  | `false`        |
| `--llm-backend`               | `ONDA_LLM_BACKEND`        | `ollama`       |
| `--ollama-model`              | `ONDA_OLLAMA_MODEL`       | `llama3.2:3b`  |

## Security model

- **Every** inter-node message is Ed25519-signed; receivers verify before *any* other processing. A peer that cannot produce a valid signature is silent to the rest of the network.
- The DID method is `did:key`, which is **self-certifying** — the public key is encoded in the DID string, so there is no resolution step and no chance of accepting a key vended by a server.
- libp2p's Noise handshake gives transport-level confidentiality. The optional `--encrypt` flag adds NaCl-box on top, useful when traversing untrusted relays (a v0.2 use case shipped now per the ADD-ONLY discipline).
- `~/.onda/<name>/identity.json` holds the signing seed. It is written `0o600`. Never commit it.

## Layout

```
onda/
├── src/onda/
│   ├── identity.py     did:key Ed25519, persistence
│   ├── crypto.py       canonical JSON, sign/verify, NaCl box
│   ├── protocol.py     Envelope + body schemas, sign/verify in build/verify
│   ├── memory.py       SQLite knowledge store
│   ├── llm.py          Ollama HTTP + echo backend
│   ├── transport.py    py-libp2p host + zeroconf mDNS
│   ├── ipc.py          Unix-socket JSON-RPC (CLI ↔ daemon)
│   ├── node.py         glues identity + memory + llm + transport + ipc
│   ├── cli.py          Typer commands
│   └── config.py       pydantic-settings
├── tests/              protocol, identity, memory, 2-node integration
├── examples/demo_two_nodes.sh
├── cli.py              spec-literal `python cli.py …` shim
├── pyproject.toml
├── Makefile
└── LICENSE             (Apache 2.0)
```

## Development

```bash
make test         # unit + integration (echo LLM, no Ollama needed)
make lint         # ruff
make typecheck    # mypy strict
```

## License

Apache License 2.0. See [LICENSE](LICENSE).

The Apache 2.0 license gives explicit patent grants from contributors and is more friendly to downstream commercial integration than MIT — useful for a protocol that aims to be implemented by third parties (browsers, mobile apps, embedded devices) without legal friction.
