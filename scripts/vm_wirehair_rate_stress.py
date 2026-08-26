#!/usr/bin/env python3
"""Wirehair ACK rate stress on node1 -> node2."""

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
BIN = "/home/fyp1/work/multi-flow-rate-control/build/wg_multi_pipeline"
BASE = "/tmp/wirehair_rate_stress"
PORT_BASE = 23800
INPUT = f"{BASE}/input_100m.bin"
MIB = 100
REPAIR = 20
WINDOW = 8
# 0 means no --rate-mbps (unlimited / as-fast-as-possible)
RATES = [40, 80, 120, 160, 200, 300, 400, 0]
ROUNDS_AT_PEAK = 3


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


def wait_grep(host, log, pat, n=250):
    for _ in range(n):
        q = pat.replace("'", "'\\''")
        if ssh(host, f"grep -q '{q}' {log}", check=False).returncode == 0:
            return True
        time.sleep(0.1)
    return False


def cleanup():
    for host in (SSH1, SSH2):
        ssh(
            host,
            "pkill -f 'wg_multi_pipeline.*238' >/dev/null 2>&1 || true",
            check=False,
        )
    ssh(SSH1, "sudo -n tc qdisc del dev station0 root 2>/dev/null || true", check=False)
    time.sleep(0.4)


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
    ssh(SSH2, f"mkdir -p {BASE}")
    run(
        [
            "scp",
            "-o",
            "BatchMode=yes",
            f"fyp1@10.10.10.161:{INPUT}.sha",
            f"fyp1@10.10.10.162:{INPUT}.sha",
        ]
    )


def one_run(tag: str, rate: float, port: int) -> dict:
    cleanup()
    out = f"{BASE}/{tag}.out"
    rlog = f"{BASE}/{tag}_recv.log"
    slog = f"{BASE}/{tag}_send.log"
    ack_port = port + 1
    rate_arg = "" if rate <= 0 else f"--rate-mbps {rate}"
    rate_label = "unlimited" if rate <= 0 else str(int(rate))

    ssh(SSH2, f"rm -f {out} {rlog}; : > {rlog}")
    ssh(SSH1, f"rm -f {slog}; : > {slog}")
    ssh(
        SSH2,
        f"""
nohup {BIN} --codec wirehair --wh-segment-mib=10 --wh-repair-pct={REPAIR} \
  --wh-window={WINDOW} --wh-ack \
  --local-node-id 4 --udp-recv {port} {out} --idle-sec 40 --strict \
  >{rlog} 2>&1 &
echo $! > {BASE}/{tag}_recv.pid
""",
    )
    if not wait_grep(SSH2, rlog, "wirehair-recv: listening", 250):
        print(ssh(SSH2, f"cat {rlog}", check=False).stdout)
        return {"rate": rate_label, "verify": "RECV_FAIL"}

    send = ssh(
        SSH1,
        f"""
set -e
/usr/bin/time -f 'wall_sec=%e' -o {BASE}/{tag}_time.txt \
  {BIN} --codec wirehair --wh-segment-mib=10 --wh-repair-pct={REPAIR} \
  --wh-window={WINDOW} --wh-ack --ack-port={ack_port} \
  --local-node-id 1 --final-dst 4 --ttl 8 {rate_arg} \
  --udp-send 10.10.12.2 {port} {INPUT} >{slog} 2>&1
cat {BASE}/{tag}_time.txt
echo ---SEND---
cat {slog}
""",
        timeout=900,
    )

    for _ in range(400):
        if (
            ssh(
                SSH2,
                f"kill -0 $(cat {BASE}/{tag}_recv.pid) 2>/dev/null",
                check=False,
            ).returncode
            != 0
        ):
            break
        time.sleep(0.2)

    ver = ssh(
        SSH2,
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
    # optional udp-send line
    u = re.search(
        r"udp-send:.*elapsed=([0-9.]+)s goodput_mbps=([0-9.]+) wire_mbps=([0-9.]+)",
        send.stdout,
    )
    if u:
        row["udp_elapsed"] = float(u.group(1))
        row["udp_goodput"] = float(u.group(2))
        row["udp_wire"] = float(u.group(3))

    print(
        f"rate={row['rate']:>9} verify={row['verify']:<4} "
        f"wall={row.get('wall_s', float('nan')):7.2f}s "
        f"goodput={row.get('goodput_mbps', float('nan')):7.1f} "
        f"wire={row.get('wire_mbps', float('nan')):7.1f} "
        f"repair={row.get('repair_sent', -1)}",
        flush=True,
    )
    if row["verify"] != "OK":
        print("--- send ---")
        print(send.stdout[-2000:])
        print("--- recv ---")
        print(ssh(SSH2, f"cat {rlog}", check=False).stdout[-1500:])
    return row


def main():
    cleanup()
    prepare_input()
    print(
        f"stress: {MIB} MiB wirehair+ACK repair={REPAIR}% window={WINDOW} "
        f"rates={RATES}",
        flush=True,
    )

    results = []
    port = PORT_BASE
    for rate in RATES:
        tag = f"r{int(rate) if rate else 0}"
        row = one_run(tag, float(rate), port)
        results.append(row)
        port += 10
        if row["verify"] != "OK" and rate != 0:
            # keep going a bit to see if unlimited also fails
            pass

    ok_rates = [r for r in results if r["verify"] == "OK"]
    print("\n===== SWEEP SUMMARY =====")
    for r in results:
        print(r)

    if not ok_rates:
        print("NO_SUCCESS")
        raise SystemExit(2)

    # peak among successful: prefer highest target rate that passed; for
    # unlimited use measured goodput
    def sort_key(r):
        if r["rate"] == "unlimited":
            return r.get("goodput_mbps", 0.0)
        return float(r["rate"])

    best = max(ok_rates, key=sort_key)
    print(
        f"\nBEST_OK target={best['rate']} "
        f"goodput={best.get('goodput_mbps', float('nan')):.1f} Mbps "
        f"wire={best.get('wire_mbps', float('nan')):.1f} Mbps "
        f"repair={best.get('repair_sent')}"
    )

    # stability: re-run best measured unlimited or highest numeric 3 times
    peak_rate = 0.0 if best["rate"] == "unlimited" else float(best["rate"])
    # if unlimited passed, stress that; else stress the highest numeric OK
    numeric_ok = [r for r in ok_rates if r["rate"] != "unlimited"]
    if any(r["rate"] == "unlimited" and r["verify"] == "OK" for r in results):
        stress_rate = 0.0
        stress_label = "unlimited"
    elif numeric_ok:
        stress_rate = max(float(r["rate"]) for r in numeric_ok)
        stress_label = str(int(stress_rate))
    else:
        stress_rate = 0.0
        stress_label = "unlimited"

    print(f"\n===== STABILITY x{ROUNDS_AT_PEAK} @ {stress_label} =====", flush=True)
    stab = []
    for i in range(ROUNDS_AT_PEAK):
        row = one_run(f"stab{i}", stress_rate, PORT_BASE + 200 + i * 10)
        stab.append(row)

    stab_ok = [r for r in stab if r["verify"] == "OK"]
    print("\n===== STABILITY SUMMARY =====")
    for r in stab:
        print(r)
    if stab_ok:
        gps = [r["goodput_mbps"] for r in stab_ok if "goodput_mbps" in r]
        print(
            f"stability_ok={len(stab_ok)}/{len(stab)} "
            f"goodput_avg={sum(gps)/len(gps):.1f} "
            f"min={min(gps):.1f} max={max(gps):.1f}"
        )
    else:
        print("stability_ok=0")
        raise SystemExit(3)

    cleanup()
    print("\nDONE")


if __name__ == "__main__":
    main()
