#!/usr/bin/env bash
# Phase 0–1 on LINEAR bridge topology (WH_TOPO=linear).
set -euo pipefail
REPO=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
OUT=${RESULT_DIR:-"$REPO/build/linear_phase01_$(date +%Y%m%d-%H%M%S)"}
mkdir -p "$OUT"
REPORT="$REPO/build/report-data/linear_phase01_baseline.md"

echo "OUT=$OUT"
{
  echo "# Linear topology Phase0–1"
  echo
  echo "- path: VM1 10.20.20.1 — VM2 — VM3 — VM4 10.40.40.2"
  echo "- SSH: 10.10.10.161–164"
  echo
  echo "## Phase 0 — addressing"
  echo
} >"$REPORT"

for ip in 161 162 163 164; do
  echo "===== node .$ip =====" | tee -a "$OUT/phase0.txt"
  ssh -o BatchMode=yes fyp1@10.10.10.$ip 'hostname; ip -br a | grep -E "enp6s1[89]|enp6s2[01]"; ip r | grep -E "10\.(20|30|40)\."' | tee -a "$OUT/phase0.txt"
done

{
  echo '```'
  cat "$OUT/phase0.txt"
  echo '```'
  echo
  echo "## Phase 0 — ping / traceroute"
  echo
} >>"$REPORT"

ssh -o BatchMode=yes fyp1@10.10.10.161 'ping -c 3 -W 1 10.20.20.2; ping -c 3 -W 2 10.40.40.2; traceroute -n -q1 -w1 10.40.40.2' | tee "$OUT/ping_tr.txt"
{
  echo '```'
  cat "$OUT/ping_tr.txt"
  echo '```'
  echo
  echo "## Phase 1 — iperf3"
  echo
  echo "| link | TCP Gbps | UDP@500M recv/loss | UDP@1G recv/loss | UDP@2G recv/loss |"
  echo "|---|---:|---|---|---|"
} >>"$REPORT"

run_link() {
  local src_mgmt=$1 dst=$2 label=$3 sink_mgmt=$4
  echo "=== $label -> $dst ===" | tee -a "$OUT/iperf.txt"
  ssh -o BatchMode=yes fyp1@$sink_mgmt "pkill -u fyp1 iperf3 2>/dev/null || true; sleep 0.2
    nohup iperf3 -s -B $dst -p 5201 >/tmp/iperf_lin_tcp.log 2>&1 &
    nohup iperf3 -s -B $dst -p 5202 >/tmp/iperf_lin_udp.log 2>&1 &
    sleep 0.4"
  local tcp udp500 udp1g udp2g
  parse="$REPO/scripts/local/iperf_parse_bits.py"
  tcp=$(ssh -o BatchMode=yes fyp1@$src_mgmt "iperf3 -c $dst -p 5201 -t 8 -J" | python3 "$parse" tcp)
  udp500=$(ssh -o BatchMode=yes fyp1@$src_mgmt "iperf3 -c $dst -p 5202 -u -b 500M -t 6 -J" | python3 "$parse" udp)
  udp1g=$(ssh -o BatchMode=yes fyp1@$src_mgmt "iperf3 -c $dst -p 5202 -u -b 1G -t 6 -J" | python3 "$parse" udp)
  udp2g=$(ssh -o BatchMode=yes fyp1@$src_mgmt "iperf3 -c $dst -p 5202 -u -b 2G -t 6 -J" | python3 "$parse" udp)
  echo "$label TCP=${tcp}G UDP500=$udp500 UDP1G=$udp1g UDP2G=$udp2g" | tee -a "$OUT/iperf.txt"
  echo "| $label | $tcp | $udp500 | $udp1g | $udp2g |" >>"$REPORT"
  ssh -o BatchMode=yes fyp1@$sink_mgmt "pkill -u fyp1 iperf3 2>/dev/null || true"
}

run_link 10.10.10.161 10.20.20.2 hop1_VM1_VM2 10.10.10.162
run_link 10.10.10.162 10.30.30.2 hop2_VM2_VM3 10.10.10.163
run_link 10.10.10.163 10.40.40.2 hop3_VM3_VM4 10.10.10.164
run_link 10.10.10.161 10.40.40.2 e2e_VM1_VM4 10.10.10.164

echo "Wrote $REPORT"
