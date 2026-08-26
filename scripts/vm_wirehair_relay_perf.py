#!/usr/bin/env python3
"""Wirehair ACK performance: Node1 → Node2 (opaque relay) → Node3.

Topology (data plane):
  Node1 station0 10.10.12.1  --udp-send-->  Node2 ap0 10.10.12.2 (wire_relay)
  Node2 station1 10.10.23.1  --forward--->  Node3 ap1 10.10.23.2 (--udp-recv)
  ACK: Node3 → peer(Node2) → --return-hop→ Node1 10.10.12.1:ack-port
"""

from __future__ import annotations

import re
import subprocess
import sys
import time

SSH1 = [
    "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=15",
    "fyp1@10.10.10.161",
]
SSH2 = [
    "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=15",
    "fyp1@10.10.10.162",
]
SSH3 = [
    "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=15",
    "fyp1@10.10.10.163",
]

WG = "/home/fyp1/work/multi-flow-rate-control/build/wg_multi_pipeline"
RELAY = "/home/fyp1/work/multi-flow-rate-control/build/wire_relay"
BASE = "/tmp/wirehair_relay_perf"
PORT_BASE = 23910
INPUT = f"{BASE}/input.bin"
N1_DATA = "10.10.12.1"
N2_LISTEN = "10.10.12.2"
N3_DATA = "10.10.23.2"
MIB = 50
REPAIR = 20
WINDOW = 8
# 0 = unlimited
RATES = [40, 80, 160, 200, 0]


def run(cmd, check=True, timeout=900):
    r = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if check and r.returncode != 0:
        print("FAIL:", cmd, file=sys.stderr)
        print(r.stdout, file=sys.stderr)
        print(r.stderr, file=sys.stderr)
        raise SystemExit(r.returncode or 1)
    return r


def ssh(host, script, check=True, timeout=900):
    return run(host + [script], check=check, timeout=timeout)


def wait_grep(host, log, pat, n=300):
    for _ in range(n):
        q = pat.replace("'", "'\\''")
        if ssh(host, f"grep -q '{q}' {log}", check=False).returncode == 0:
            return True
        time.sleep(0.1)
    return False


def cleanup():
    ssh(
        SSH1,
        "pkill -f 'wg_multi_pipeline.*2391' >/dev/null 2>&1 || true",
        check=False,
    )
    ssh(
        SSH2,
        "pkill -f 'wire_relay.*2391' >/dev/null 2>&1 || true; "
        "pkill -f 'wg_multi_pipeline.*2391' >/dev/null 2>&1 || true",
        check=False,
    )
    ssh(
        SSH3,
        "pkill -f 'wg_multi_pipeline.*2391' >/dev/null 2>&1 || true",
        check=False,
    )
    time.sleep(0.5)


def prepare_input():
    ssh(
        SSH1,
        f"""
set -e
mkdir -p {BASE}
if [ ! -f {INPUT} ] || [ "$(stat -c%s {INPUT})" -ne $(({MIB}*1024*1024)) ]; then
  python3 - <<'PY'
from pathlib import Path
size = {MIB} * 1024 * 1024
path = Path("{INPUT}")
block = bytes((i * 37 + i // 17 + 23) & 0xff for i in range(65536))
with path.open("wb") as f:
    left = size
    while left:
        n = min(left, len(block))
        f.write(block[:n])
        left -= n
print(path.stat().st_size)
PY
fi
sha256sum {INPUT} | tee {INPUT}.sha
""",
    )
    for host, dest in (
        (SSH2, "fyp1@10.10.10.162"),
        (SSH3, "fyp1@10.10.10.163"),
    ):
        ssh(host, f"mkdir -p {BASE}")
        run(
            [
                "scp",
                "-o",
                "BatchMode=yes",
                f"fyp1@10.10.10.161:{INPUT}.sha",
                f"{dest}:{INPUT}.sha",
            ]
        )


def one_run(tag: str, rate: float, port: int) -> dict:
    cleanup()
    out = f"{BASE}/{tag}.out"
    rlog = f"{BASE}/{tag}_recv.log"
    xlog = f"{BASE}/{tag}_relay.log"
    slog = f"{BASE}/{tag}_send.log"
    ack_port = port + 1
    rate_arg = "" if rate <= 0 else f"--rate-mbps {rate}"
    rate_label = "unlimited" if rate <= 0 else str(int(rate))

    ssh(SSH3, f"rm -f {out} {rlog}; : > {rlog}")
    ssh(SSH2, f"rm -f {xlog}; : > {xlog}")
    ssh(SSH1, f"rm -f {slog}; : > {slog}")

    # Sink on Node3
    ssh(
        SSH3,
        f"""
nohup {WG} --codec wirehair --wh-segment-mib=10 --wh-repair-pct={REPAIR} \
  --wh-window={WINDOW} --wh-ack \
  --local-node-id 4 --udp-recv {port} {out} --idle-sec 60 --strict \
  >{rlog} 2>&1 &
echo $! > {BASE}/{tag}_recv.pid
""",
    )
    if not wait_grep(SSH3, rlog, "wirehair-recv: listening", 300):
        print(ssh(SSH3, f"cat {rlog}", check=False).stdout)
        return {"rate": rate_label, "verify": "RECV_FAIL"}

    # Opaque relay on Node2
    ssh(
        SSH2,
        f"""
nohup {RELAY} --local-node-id 2 --listen {port} \
  --next-hop {N3_DATA}:{port} --return-hop {N1_DATA}:{ack_port} \
  --idle-exit-sec 90 \
  >{xlog} 2>&1 &
echo $! > {BASE}/{tag}_relay.pid
""",
    )
    if not wait_grep(SSH2, xlog, "wire-relay: local_node_id", 300):
        print(ssh(SSH2, f"cat {xlog}", check=False).stdout)
        return {"rate": rate_label, "verify": "RELAY_FAIL"}

    send = ssh(
        SSH1,
        f"""
set -e
/usr/bin/time -f 'wall_sec=%e' -o {BASE}/{tag}_time.txt \
  {WG} --codec wirehair --wh-segment-mib=10 --wh-repair-pct={REPAIR} \
  --wh-window={WINDOW} --wh-ack --ack-port={ack_port} \
  --local-node-id 1 --final-dst 4 --ttl 8 {rate_arg} \
  --udp-send {N2_LISTEN} {port} {INPUT} >{slog} 2>&1
cat {BASE}/{tag}_time.txt
echo ---SEND---
cat {slog}
""",
        timeout=900,
    )

    for _ in range(500):
        if (
            ssh(
                SSH3,
                f"kill -0 $(cat {BASE}/{tag}_recv.pid) 2>/dev/null",
                check=False,
            ).returncode
            != 0
        ):
            break
        time.sleep(0.2)

    ver = ssh(
        SSH3,
        f"""
exp=$(cut -d' ' -f1 {INPUT}.sha)
if [ ! -f {out} ]; then echo VERIFY=FAIL; echo missing_out; exit 0; fi
got=$(sha256sum {out} | awk '{{print $1}}')
echo exp=$exp
echo got=$got
ls -l {out}
if [ "$exp" = "$got" ]; then echo VERIFY=OK; else echo VERIFY=FAIL; fi
""",
        check=False,
    )

    relay_stats = ssh(
        SSH2,
        f"tail -n 40 {xlog}; "
        f"grep -E 'forward|drop|ack|delivered' {xlog} | tail -20 || true",
        check=False,
    )

    row = {
        "rate": rate_label,
        "verify": "OK" if "VERIFY=OK" in ver.stdout else "FAIL",
    }
    m = re.search(
        r"wirehair-send: source_bytes=(\d+) segments=(\d+) repair_sent=(\d+) "
        r"wire_bytes=(\d+)",
        send.stdout,
    )
    if m:
        row["source_bytes"] = int(m.group(1))
        row["segments"] = int(m.group(2))
        row["repair_sent"] = int(m.group(3))
        row["wire_bytes"] = int(m.group(4))
    t = re.search(r"wall_sec=([0-9.]+)", send.stdout)
    if t:
        row["wall_s"] = float(t.group(1))
        if row.get("source_bytes") and row["wall_s"] > 0:
            row["goodput_mbps"] = row["source_bytes"] * 8.0 / row["wall_s"] / 1e6
        if row.get("wire_bytes") and row["wall_s"] > 0:
            row["wire_mbps"] = row["wire_bytes"] * 8.0 / row["wall_s"] / 1e6
    u = re.search(
        r"udp-send:.*elapsed=([0-9.]+)s goodput_mbps=([0-9.]+) wire_mbps=([0-9.]+)",
        send.stdout,
    )
    if u:
        row["udp_elapsed"] = float(u.group(1))
        row["udp_goodput"] = float(u.group(2))
        row["udp_wire"] = float(u.group(3))
    fa = re.search(r"forward_ack[=:](\d+)", relay_stats.stdout)
    if fa:
        row["relay_forward_ack"] = int(fa.group(1))
    dn = re.search(r"drop_no_return_hop[=:](\d+)", relay_stats.stdout)
    if dn:
        row["relay_drop_no_return"] = int(dn.group(1))

    if row["verify"] != "OK":
        print("--- recv log ---")
        print(ssh(SSH3, f"cat {rlog}", check=False).stdout[-4000:])
        print("--- relay log ---")
        print(relay_stats.stdout[-4000:])
        print("--- send log ---")
        print(send.stdout[-4000:])
    return row


def main():
    print(
        f"Wirehair relay perf: {MIB} MiB, repair={REPAIR}%, window={WINDOW}, "
        f"path N1→N2(relay)→N3"
    )
    prepare_input()
    results = []
    port = PORT_BASE
    for rate in RATES:
        tag = f"r{int(rate) if rate > 0 else 0}"
        print(f"\n=== rate={rate if rate > 0 else 'unlimited'} port={port} ===")
        row = one_run(tag, rate, port)
        results.append(row)
        print(row)
        port += 2
        sys.stdout.flush()

    cleanup()
    print("\n==== SUMMARY ====")
    hdr = (
        f"{'rate':>10} {'verify':>8} {'wall_s':>8} {'goodput':>10} "
        f"{'wire_mbps':>10} {'repair':>8} {'fwd_ack':>8}"
    )
    print(hdr)
    for r in results:
        print(
            f"{r.get('rate','?'):>10} {r.get('verify','?'):>8} "
            f"{r.get('wall_s', float('nan')):8.2f} "
            f"{r.get('goodput_mbps', float('nan')):10.1f} "
            f"{r.get('wire_mbps', float('nan')):10.1f} "
            f"{r.get('repair_sent', -1):8d} "
            f"{r.get('relay_forward_ack', -1):8d}"
        )
    fails = [r for r in results if r.get("verify") != "OK"]
    if fails:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
