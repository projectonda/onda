# Onda v0.2 — Raspberry Pi setup for the "cable-cut" demo

The Pi-based demo exercises every transport: real Wi-Fi Direct, real BLE,
real proximity (sneakernet) — none of which are realistic to test on a
single Mac.

## Hardware

* 2× Raspberry Pi 4 (4 GB RAM each is enough for `llama3.2:3b`).
* SD card with Raspberry Pi OS 64-bit (Bookworm). Headless OK.
* Power, ethernet (initial setup), nothing else.

You can substitute Pi 5 (faster Ollama) or Pi Zero 2 W (BLE only, no
Ollama). Wi-Fi Direct tests need at least one Pi running BlueZ + a Wi-Fi
adapter that supports AP mode (the built-in Pi 4 chip does).

## Bootstrap, both Pis

```bash
sudo apt update
sudo apt install -y python3-pip python3-venv libgmp-dev \
    bluez bluez-tools network-manager wpasupplicant

# Optional: Ollama for real LLM inference
curl -fsSL https://ollama.com/install.sh | sh
ollama pull llama3.2:3b   # or smollm2:135m for the Pi Zero 2 W

git clone https://github.com/projectonda/onda.git
cd onda
make install
source .venv/bin/activate
```

## Scenario A — LAN cable-cut (mDNS keeps working)

1. Both Pis on the same Wi-Fi (router-controlled).
2. Disable internet egress on each: `sudo iptables -A OUTPUT -d 0/0 -p tcp --dport 80 -j REJECT` (or pull the WAN cable).
3. Start daemons:

   ```bash
   # Pi 1
   onda start --name leonardo --port 9001 --transport-mode v2

   # Pi 2
   onda start --name pietro --port 9001 --transport-mode v2
   ```

4. Verify they discovered each other via mDNS:

   ```bash
   onda peers --name leonardo   # should list pietro with transport=lan
   ```

5. Ask:

   ```bash
   onda ask --name leonardo "Cosa coltivate qui?"
   ```

The cable is "cut" but `lan` keeps working because mDNS is link-local.

## Scenario B — BLE-only (no Wi-Fi at all)

1. Disable Wi-Fi on both Pis: `sudo nmcli radio wifi off`.
2. Verify Bluetooth radio is up: `bluetoothctl power on`.
3. Start daemons with BLE enabled:

   ```bash
   onda start --name leonardo --port 9001 \
              --transport-mode v2 --enable-bluetooth

   onda start --name pietro --port 9001 \
              --transport-mode v2 --enable-bluetooth
   ```

4. Wait ~5–15 s for advertisement + scan to find each other.
5. Verify with `onda peers --name leonardo` — should list pietro with
   `transport=bluetooth`.
6. Ask a SHORT question (BLE bandwidth is ~5–50 KiB/s on Pi 4):

   ```bash
   onda ask --name leonardo "Una parola: ti senti bene?" --max-tokens 32
   ```

## Scenario C — Wi-Fi Direct hotspot + sneakernet

The most ambitious scenario. Three Pis: A, B, C. A and C never share a
network; B carries messages between them.

1. **B as a mobile relay**: switches between A's and C's hotspots.
2. **A's hotspot**:

   ```bash
   sudo nmcli device wifi hotspot ifname wlan0 \
       con-name onda-A-hotspot \
       ssid Onda-leonardo \
       password "lassoffinepoidi17"
   ```

3. **C's hotspot** (different SSID, same prefix):

   ```bash
   sudo nmcli device wifi hotspot ifname wlan0 \
       con-name onda-C-hotspot \
       ssid Onda-cleo \
       password "lassoffinepoidi17"
   ```

4. **All three start daemons with proximity + wifi_direct**:

   ```bash
   onda start --name leonardo --port 9001 \
       --transport-mode v2 --enable-wifi-direct --enable-proximity
   # … same for pietro (B) and cleo (C)
   ```

5. **A sends to C**:

   ```bash
   onda ask --name leonardo --to did:key:z6Mk…cleo \
       "Come stai oggi?" --max-tokens 64
   ```

   Since C is not visible from A, the carrier ends up in A's mailbox.

6. **B physically walks to A's vicinity → joins Onda-leonardo SSID →
   discovers A's daemon → drains A's mailbox into B's mailbox.**

7. **B walks to C's vicinity → joins Onda-cleo SSID → encounters C → B
   forwards the carrier; C decrypts and answers (the answer takes the
   reverse path).**

This is the canonical proximity scenario from the manifesto.

## Troubleshooting

* **mDNS doesn't find peers**: some networks block IPv4 multicast. Try
  `avahi-browse -a` to verify Zeroconf is reaching across hosts.
* **BLE permission errors on macOS**: System Settings → Privacy &
  Security → Bluetooth → enable for the launching terminal.
* **Wi-Fi Direct on macOS**: NOT SUPPORTED. The transport returns
  `is_available() = False`. Use Linux/Windows/Pi.
* **GMP build failure on `make install`**: `sudo apt install libgmp-dev`
  (Linux), `brew install gmp` (macOS).
