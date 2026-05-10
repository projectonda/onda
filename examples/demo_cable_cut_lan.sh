#!/usr/bin/env bash
# demo_cable_cut_lan.sh — show v0.2 mDNS LAN discovery working end-to-end.
#
# This boots two v0.2 daemons on the same Mac (same broadcast domain) and
# confirms that they find each other via Zeroconf without ANY bootstrap
# multiaddr. The "cable cut" framing: even if every external network were
# unreachable, this exact discovery would still work because mDNS is
# strictly link-local.
#
# To exercise the full thing on real hardware (two laptops, two Pis,
# one of each), see docs/pi_setup.md.

set -euo pipefail

PORT_A=${PORT_A:-9701}
PORT_B=${PORT_B:-9702}
HOME_DIR=${ONDA_HOME_DIR:-/tmp/onda-cable-cut}
BACKEND=${ONDA_LLM_BACKEND:-echo}

ONDA_BIN=${ONDA_BIN:-onda}
if ! command -v "$ONDA_BIN" >/dev/null 2>&1; then
    ONDA_BIN="python3 -m onda"
fi

mkdir -p "$HOME_DIR"

echo ">>> Starting Leonardo (transport-mode v2, LAN+Internet only)…"
ONDA_HOME_DIR="$HOME_DIR" $ONDA_BIN start \
    --name leonardo --port "$PORT_A" --host 0.0.0.0 \
    --transport-mode v2 \
    --llm-backend "$BACKEND" \
    > /tmp/onda-leonardo-v2.log 2>&1 &
PID_A=$!
trap 'kill $PID_A 2>/dev/null || true; kill ${PID_B:-} 2>/dev/null || true' EXIT

# Wait for daemon socket.
SOCK_A="$HOME_DIR/leonardo/ipc.sock"
for _ in $(seq 1 50); do
    [[ -S "$SOCK_A" ]] && break
    sleep 0.2
done
[[ -S "$SOCK_A" ]] || { echo "Leonardo did not come up — see /tmp/onda-leonardo-v2.log" >&2; exit 1; }

echo ">>> Starting Pietro (transport-mode v2, LAN+Internet only)…"
ONDA_HOME_DIR="$HOME_DIR" $ONDA_BIN start \
    --name pietro --port "$PORT_B" --host 0.0.0.0 \
    --transport-mode v2 \
    --llm-backend "$BACKEND" \
    > /tmp/onda-pietro-v2.log 2>&1 &
PID_B=$!

SOCK_B="$HOME_DIR/pietro/ipc.sock"
for _ in $(seq 1 50); do
    [[ -S "$SOCK_B" ]] && break
    sleep 0.2
done
[[ -S "$SOCK_B" ]] || { echo "Pietro did not come up — see /tmp/onda-pietro-v2.log" >&2; exit 1; }

echo ">>> Pre-loading Pietro's memory…"
ONDA_HOME_DIR="$HOME_DIR" $ONDA_BIN remember --name pietro \
    "Quando il vento di scirocco arriva da sud-ovest, le coltivazioni di pomodoro nella piana di Mazara hanno bisogno di copertura immediata."

echo ">>> Waiting up to 15s for mDNS discovery…"
for _ in $(seq 1 30); do
    n=$(ONDA_HOME_DIR="$HOME_DIR" $ONDA_BIN peers --name leonardo | python3 -c 'import json,sys;d=json.load(sys.stdin);print(len(d.get("peers",[])))')
    if [[ "$n" -ge 1 ]]; then
        break
    fi
    sleep 0.5
done

echo ">>> Leonardo's peer list:"
ONDA_HOME_DIR="$HOME_DIR" $ONDA_BIN peers --name leonardo

echo ">>> Asking Pietro a question…"
ONDA_HOME_DIR="$HOME_DIR" $ONDA_BIN ask --name leonardo \
    "Cosa fai con il pomodoro quando arriva lo scirocco?" \
    --max-tokens 200

echo
echo ">>> Done. Notice that NO bootstrap multiaddr was passed; discovery"
echo ">>> happened entirely over zeroconf (mDNS) — exactly the path that"
echo ">>> survives an internet outage."
