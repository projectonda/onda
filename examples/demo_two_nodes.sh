#!/usr/bin/env bash
# demo_two_nodes.sh — boot two Onda nodes locally and run the spec scenario.
#
# This script reproduces the scenario from the v0.1 spec:
#   1. Start node A ("leonardo") on port 9001
#   2. Start node B ("pietro") on port 9002, bootstrapped to A
#   3. Pre-load B's memory with a fragment about Sicilian tomatoes
#   4. Ask A a question; A relays it to B; B answers via local Ollama;
#      A receives the signed response and prints attribution.
#
# Requirements: Ollama running locally with `llama3.2:3b` pulled.
# Override the model with ONDA_OLLAMA_MODEL=mistral:7b ./demo_two_nodes.sh
#
# Use ONDA_LLM_BACKEND=echo to skip Ollama (the daemon then returns a
# deterministic stub answer; useful when you only want to see the protocol
# in motion).

set -euo pipefail

PORT_A=${PORT_A:-9001}
PORT_B=${PORT_B:-9002}
HOME_DIR=${ONDA_HOME_DIR:-"$HOME/.onda"}
BACKEND=${ONDA_LLM_BACKEND:-ollama}
MODEL=${ONDA_OLLAMA_MODEL:-llama3.2:3b}

ONDA_BIN=${ONDA_BIN:-onda}
if ! command -v "$ONDA_BIN" >/dev/null 2>&1; then
    # Fall back to module form when the package isn't installed on PATH yet.
    ONDA_BIN="python -m onda"
fi

echo ">>> Starting Leonardo's AI on tcp/$PORT_A …"
$ONDA_BIN start \
    --name leonardo \
    --port "$PORT_A" \
    --host 127.0.0.1 \
    --no-mdns \
    --llm-backend "$BACKEND" \
    --ollama-model "$MODEL" \
    > /tmp/onda-leonardo.log 2>&1 &
PID_A=$!
trap 'kill $PID_A 2>/dev/null || true; kill ${PID_B:-} 2>/dev/null || true' EXIT

# Wait for Leonardo's IPC socket to appear, then read his peer_id.
SOCK_A="$HOME_DIR/leonardo/ipc.sock"
for _ in $(seq 1 50); do
    [[ -S "$SOCK_A" ]] && break
    sleep 0.2
done
if [[ ! -S "$SOCK_A" ]]; then
    echo "Leonardo's daemon did not come up — see /tmp/onda-leonardo.log" >&2
    exit 1
fi

PY=${PYTHON:-python3}
PEER_A=$($ONDA_BIN info --name leonardo | "$PY" -c 'import json,sys;print(json.load(sys.stdin)["peer_id"])')
echo "    Leonardo PeerID = $PEER_A"

BOOT="/ip4/127.0.0.1/tcp/$PORT_A/p2p/$PEER_A"

echo ">>> Starting Pietro's AI on tcp/$PORT_B (bootstrap → $BOOT) …"
$ONDA_BIN start \
    --name pietro \
    --port "$PORT_B" \
    --host 127.0.0.1 \
    --no-mdns \
    --bootstrap "$BOOT" \
    --llm-backend "$BACKEND" \
    --ollama-model "$MODEL" \
    > /tmp/onda-pietro.log 2>&1 &
PID_B=$!

SOCK_B="$HOME_DIR/pietro/ipc.sock"
for _ in $(seq 1 50); do
    [[ -S "$SOCK_B" ]] && break
    sleep 0.2
done

echo ">>> Pre-loading Pietro's memory …"
$ONDA_BIN remember --name pietro \
    "Il pomodoro nella Sicilia occidentale, soprattutto a Mazara del Vallo, \
beneficia del clima mite, del suolo argilloso-sabbioso e dell'acqua salmastra \
dei pozzi costieri. La semina avviene tra febbraio e marzo sotto tunnel; il \
trapianto in campo aperto a fine aprile."

# Give discovery a moment.
sleep 2

echo ">>> Leonardo asks Pietro a question …"
$ONDA_BIN ask --name leonardo "Come si coltiva il pomodoro a Mazara del Vallo?"

echo
echo ">>> Pietro's recv log:"
$ONDA_BIN recv --name pietro
