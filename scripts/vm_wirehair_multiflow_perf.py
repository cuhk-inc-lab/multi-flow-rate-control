#!/usr/bin/env python3
"""Wirehair fountain multi-flow performance on VM (Node1 -> Node2 direct).

Each flow is an independent Wirehair sender thread (--udp-send-multi).
Receiver demuxes by wire flow_id (--max-flows N).
Per-flow rate = total_rate / N (paced). ACK uses ack_port_base + flow_index.
"""

from __future__ import annotations

import re
import subprocess
import sys
import time

SSH1 = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.161"]
SSH2 = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.162"]

WG = "/home/fyp1/work/multi-flow-rate-control/build/wg_multi_pipeline"
BASE = "/tmp/wirehair_mf"
PORT_BASE = 24300
ACK_BASE = 24400
N2 = "10.10.12.2"
MIB_PER_FLOW = 20
REPAIR = 20
WINDOW = 8
# (flows, total_rate_mbps); 0 total = unlimited per-flow (no --rate)
CASES = [
    (1, 200),
    (2, 200),
    (4, 200),
    (8, 200),
    (1, 400),
    (2, 400),
    (4, 400),
    (8, 400),
    (2, 0),
    (4, 0),
    (8, 0),
]


def run(cmd, check=True, timeout=900):
    r = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if check and r.returncode != 0:
        print("FAIL:", cmd, file=sys.stderr)
        print(r.stdout[-3000:], file=sys.stderr)
        print(r.stderr[-3000:], file=sys.stderr)
        raise SystemExit(r.returncode or 1)
    return r


def ssh(host, script, check=True, timeout=900):
    return run(host + [script], check=check, timeout=timeout)


def wait_grep(host, log, pat, n=400):
    for _ in range(n):
        q = pat.replace("'", "'\\''")
        if ssh(host, f"grep -q '{q}' {log}", check=False).returncode == 0:
            return True
        time.sleep(0.1)
    return False


def cleanup():
    ssh(SSH1, "pkill -f 'wg_multi_pipeline.*(243|244)' >/dev/null 2>&1 || true",
        check=False)
    ssh(SSH2, "pkill -f 'wg_multi_pipeline.*(243|244)' >/dev/null 2>&1 || true",
        check=False)
    time.sleep(0.4)


def prepare():
    ssh(
        SSH1,
        f"""
set -e
mkdir -p {BASE}
for i in $(seq 0 7); do
  f={BASE}/in_$i.bin
  if [ ! -f "$f" ] || [ "$(stat -c%s "$f")" -ne $(({MIB_PER_FLOW}*1024*1024)) ]; then
    python3 - <<PY
from pathlib import Path
size = {MIB_PER_FLOW} * 1024 * 1024
idx = $i
path = Path("{BASE}/in_" + str(idx) + ".bin")
block = bytes(((j + idx * 17) * 37 + j // 19) & 0xff for j in range(65536))
with path.open("wb") as f:
    left = size
    while left:
        n = min(left, len(block))
        f.write(block[:n])
        left -= n
print(path, path.stat().st_size)
PY
  fi
  sha256sum "$f" > "$f.sha"
done
""",
    )
    ssh(SSH2, f"mkdir -p {BASE}")
    run(
        [
            "scp",
            "-o",
            "BatchMode=yes",
            "-r",
            *[f"fyp1@10.10.10.161:{BASE}/in_{i}.bin.sha" for i in range(8)],
            "fyp1@10.10.10.162:/tmp/",
        ]
    )
    # scp multiple files to /tmp then move — cleaner:
    ssh(
        SSH2,
        f"""
mkdir -p {BASE}
for i in $(seq 0 7); do
  mv -f /tmp/in_$i.bin.sha {BASE}/in_$i.bin.sha 2>/dev/null || true
done
ls {BASE}/*.sha | wc -l
""",
    )


def one_case(flows: int, total_rate: float, port: int, ack_base: int) -> dict:
    cleanup()
    tag = f"f{flows}_r{int(total_rate) if total_rate > 0 else 0}"
    prefix = f"{BASE}/{tag}_out_"
    rlog = f"{BASE}/{tag}_recv.log"
    slog = f"{BASE}/{tag}_send.log"
    per_rate = (total_rate / flows) if total_rate > 0 else 0.0
    rate_label = "unlimited" if total_rate <= 0 else str(int(total_rate))

    ssh(SSH2, f"rm -f {prefix}* {rlog}; : > {rlog}")
    ssh(SSH1, f"rm -f {slog}; : > {slog}")

    ssh(
        SSH2,
        f"""
nohup {WG} --codec wirehair --wh-segment-mib=10 --wh-repair-pct={REPAIR} \
  --wh-window={WINDOW} --wh-ack \
  --local-node-id 4 --udp-recv {port} {prefix} --max-flows {flows} \
  --idle-sec 90 --strict \
  >{rlog} 2>&1 &
echo $! > {BASE}/{tag}_recv.pid
""",
    )
    if not wait_grep(SSH2, rlog, "wirehair-recv: listening", 400):
        # also accept udp-recv listening line
        if not wait_grep(SSH2, rlog, "listening", 50):
            print(ssh(SSH2, f"cat {rlog}", check=False).stdout[-2000:])
            return {"flows": flows, "total_rate": rate_label, "verify": "RECV_FAIL"}

    # Build --flow args
    flow_args = []
    for i in range(flows):
        rate_part = "" if per_rate <= 0 else f":{per_rate:g}"
        flow_args.append(f'--flow "{i}:{N2}:{port}:{BASE}/in_{i}.bin{rate_part}"')
    flow_cli = " ".join(flow_args)

    send = ssh(
        SSH1,
        f"""
set -e
/usr/bin/time -f 'wall_sec=%e' -o {BASE}/{tag}_time.txt \
  {WG} --codec wirehair --wh-segment-mib=10 --wh-repair-pct={REPAIR} \
  --wh-window={WINDOW} --wh-ack --ack-port={ack_base} \
  --local-node-id 1 --final-dst 4 --ttl 8 \
  --udp-send-multi {flow_cli} \
  >{slog} 2>&1
cat {BASE}/{tag}_time.txt
echo ---SEND---
cat {slog}
""",
        timeout=1200,
    )

    for _ in range(600):
        if ssh(SSH2, f"kill -0 $(cat {BASE}/{tag}_recv.pid) 2>/dev/null",
               check=False).returncode != 0:
            break
        time.sleep(0.2)

    ver = ssh(
        SSH2,
        f"""
ok=0
fail=0
for i in $(seq 0 $(({flows}-1))); do
  exp=$(cut -d' ' -f1 {BASE}/in_$i.bin.sha)
  cand=
  if [ {flows} -eq 1 ]; then
    # max_flows=1: output_path is used as the file itself
    [ -f "{prefix}" ] && cand="{prefix}"
  fi
  if [ -z "$cand" ]; then
    cand=$(ls -1 {prefix}*flow_$i 2>/dev/null | head -1)
  fi
  if [ -z "$cand" ]; then
    cand=$(ls -1 {prefix}*flow_$i* 2>/dev/null | head -1)
  fi
  if [ -z "$cand" ] || [ ! -f "$cand" ]; then
    echo FLOW_$i=MISSING
    fail=$((fail+1))
    continue
  fi
  got=$(sha256sum "$cand" | awk '{{print $1}}')
  if [ "$exp" = "$got" ]; then
    echo FLOW_$i=OK size=$(stat -c%s "$cand") path=$cand
    ok=$((ok+1))
  else
    echo FLOW_$i=FAIL size=$(stat -c%s "$cand") path=$cand
    fail=$((fail+1))
  fi
done
echo OK_COUNT=$ok FAIL_COUNT=$fail
ls -l {prefix}* {prefix} 2>/dev/null | head -20
""",
        check=False,
    )

    row = {
        "flows": flows,
        "total_rate": rate_label,
        "per_rate": per_rate if per_rate > 0 else None,
        "ok_flows": 0,
        "fail_flows": flows,
        "verify": "FAIL",
    }
    m = re.search(r"OK_COUNT=(\d+).*FAIL_COUNT=(\d+)", ver.stdout, re.S)
    if m:
        row["ok_flows"] = int(m.group(1))
        row["fail_flows"] = int(m.group(2))
        row["verify"] = "OK" if int(m.group(2)) == 0 and int(m.group(1)) == flows else "FAIL"

    t = re.search(r"wall_sec=([0-9.]+)", send.stdout)
    if t:
        row["wall_s"] = float(t.group(1))
        src = flows * MIB_PER_FLOW * 1024 * 1024
        row["agg_source_bytes"] = src
        if row["wall_s"] > 0:
            row["agg_goodput_mbps"] = src * 8.0 / row["wall_s"] / 1e6

    # Sum repair_sent / wire_bytes across per-flow log lines
    repairs = [int(x) for x in re.findall(r"repair_sent=(\d+)", send.stdout)]
    wires = [int(x) for x in re.findall(r"wire_bytes=(\d+)", send.stdout)]
    if repairs:
        row["repair_sent_sum"] = sum(repairs)
    if wires:
        row["wire_bytes_sum"] = sum(wires)
        if row.get("wall_s"):
            row["agg_wire_mbps"] = sum(wires) * 8.0 / row["wall_s"] / 1e6

    if row["verify"] != "OK":
        print("--- outputs ---")
        print(ver.stdout[-2500:])
        print("--- recv ---")
        print(ssh(SSH2, f"tail -40 {rlog}", check=False).stdout)
        print("--- send ---")
        print(send.stdout[-2500:])
    return row


def main():
    print(
        f"Wirehair multi-flow: {MIB_PER_FLOW} MiB/flow, repair={REPAIR}%, "
        f"window={WINDOW}, ACK, direct N1→N2"
    )
    prepare()
    # Fix sha copy if scp -r failed oddly
    for i in range(8):
        run(
            [
                "scp",
                "-o",
                "BatchMode=yes",
                f"fyp1@10.10.10.161:{BASE}/in_{i}.bin.sha",
                f"fyp1@10.10.10.162:{BASE}/in_{i}.bin.sha",
            ],
            check=False,
        )

    results = []
    port = PORT_BASE
    ack = ACK_BASE
    for flows, total in CASES:
        print(f"\n=== flows={flows} total_rate="
              f"{'unlimited' if total<=0 else total} port={port} ===")
        row = one_case(flows, total, port, ack)
        results.append(row)
        print(row)
        port += 1
        ack += 16  # leave room for 8 flows
        sys.stdout.flush()

    cleanup()
    print("\n==== SUMMARY ====")
    print(
        f"{'flows':>5} {'tot_rate':>10} {'verify':>6} {'ok':>3} "
        f"{'wall':>7} {'agg_gp':>8} {'agg_wire':>8} {'repairΣ':>8}"
    )
    for r in results:
        print(
            f"{r.get('flows',0):5d} {str(r.get('total_rate')):>10} "
            f"{r.get('verify','?'):>6} {r.get('ok_flows',0):3d} "
            f"{r.get('wall_s', float('nan')):7.2f} "
            f"{r.get('agg_goodput_mbps', float('nan')):8.1f} "
            f"{r.get('agg_wire_mbps', float('nan')):8.1f} "
            f"{r.get('repair_sent_sum', -1):8d}"
        )


if __name__ == "__main__":
    main()
