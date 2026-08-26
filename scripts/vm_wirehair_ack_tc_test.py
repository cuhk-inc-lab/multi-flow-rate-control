#!/usr/bin/env python3
"""Cross-VM Wirehair+ACK: clean path vs tc netem 3% loss on station0."""

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
REPO = "/home/fyp1/work/multi-flow-rate-control"
BIN = f"{REPO}/build/wg_multi_pipeline"
BASE = "/tmp/wirehair_ack_tc"
PORT = 23100
ACK_PORT = 23101
# Exact multiple of default 10 MiB segment so the last segment keeps a real
# repair budget (tiny tails with 10% repair can round to 0 packets).
FILE_BYTES = 50 * 1024 * 1024
RATE_MBPS = 40
REPAIR_PCT = 20


def run(cmd, check=True, timeout=600):
    r = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if check and r.returncode != 0:
        print("FAIL:", cmd, file=sys.stderr)
        print(r.stdout, file=sys.stderr)
        print(r.stderr, file=sys.stderr)
        raise SystemExit(r.returncode or 1)
    return r


def ssh(host, script, check=True, timeout=600):
    return run(host + [script], check=check, timeout=timeout)


def wait_grep(host, log, pat, n=200):
    for _ in range(n):
        q = pat.replace("'", "'\\''")
        if ssh(host, f"grep -q '{q}' {log}", check=False).returncode == 0:
            return True
        time.sleep(0.1)
    return False


def cleanup_procs():
    for host in (SSH1, SSH2):
        ssh(
            host,
            "pkill -f 'wg_multi_pipeline.*2310' >/dev/null 2>&1 || true",
            check=False,
        )
    time.sleep(0.5)


def set_loss(pct: int):
    if pct <= 0:
        out = ssh(
            SSH1,
            "sudo -n tc qdisc del dev station0 root 2>/dev/null || true; "
            "tc qdisc show dev station0",
        )
    else:
        out = ssh(
            SSH1,
            f"sudo -n tc qdisc replace dev station0 root netem loss {pct}%; "
            "tc qdisc show dev station0",
        )
    print("tc:", out.stdout.strip())


def parse_send_log(text: str) -> dict:
    out: dict = {}
    m = re.search(
        r"wirehair-send: source_bytes=(\d+) segments=(\d+) repair_sent=(\d+) "
        r"wire_bytes=(\d+) ack=(\w+)",
        text,
    )
    if m:
        out["source_bytes"] = int(m.group(1))
        out["segments"] = int(m.group(2))
        out["repair_sent"] = int(m.group(3))
        out["wire_bytes"] = int(m.group(4))
        out["ack"] = m.group(5)
        out["wirehair_line"] = m.group(0)
    m = re.search(
        r"udp-send: source_bytes=(\d+) wire_bytes=(\d+) elapsed=([0-9.]+)s "
        r"goodput_mbps=([0-9.]+) wire_mbps=([0-9.]+)",
        text,
    )
    if m:
        out["wire_bytes"] = int(m.group(2))
        out["elapsed_s"] = float(m.group(3))
        out["goodput_mbps"] = float(m.group(4))
        out["wire_mbps"] = float(m.group(5))
        out["udp_line"] = m.group(0)
    m = re.search(r"wall_sec=([0-9.]+)", text)
    if m:
        out["time_wall_s"] = float(m.group(1))
    return out


def prepare_input():
    gen = f"""
set -e
mkdir -p {BASE}
python3 - <<'PY'
from pathlib import Path
size = {FILE_BYTES}
path = Path("{BASE}/input.bin")
block = bytes((i * 37 + i // 17 + 23) & 0xff for i in range(65536))
with path.open("wb") as f:
    left = size
    while left:
        chunk = block[:min(left, len(block))]
        f.write(chunk)
        left -= len(chunk)
print(path.stat().st_size)
PY
sha256sum {BASE}/input.bin | tee {BASE}/input.sha
"""
    print(ssh(SSH1, gen).stdout)
    ssh(SSH2, f"mkdir -p {BASE}")
    run(
        [
            "scp",
            "-o",
            "BatchMode=yes",
            f"fyp1@10.10.10.161:{BASE}/input.sha",
            f"fyp1@10.10.10.162:{BASE}/input.sha",
        ]
    )


def run_case(name: str, loss: int) -> dict:
    print(f"\n===== CASE {name} loss={loss}% =====", flush=True)
    cleanup_procs()
    set_loss(loss)
    out = f"{BASE}/{name}.out"
    rlog = f"{BASE}/{name}_recv.log"
    slog = f"{BASE}/{name}_send.log"
    ssh(SSH2, f"rm -f {out} {rlog}; : > {rlog}")
    ssh(SSH1, f"rm -f {slog}; : > {slog}")

    ssh(
        SSH2,
        f"""
nohup {BIN} --codec wirehair --wh-segment-mib=10 --wh-repair-pct={REPAIR_PCT} --wh-ack \
  --local-node-id 4 --udp-recv {PORT} {out} --idle-sec 30 --strict \
  >{rlog} 2>&1 &
echo $! > {BASE}/{name}_recv.pid
""",
    )
    if not wait_grep(SSH2, rlog, "wirehair-recv: listening", 200):
        print(ssh(SSH2, f"cat {rlog}", check=False).stdout)
        raise SystemExit("receiver not ready")

    send = ssh(
        SSH1,
        f"""
set -e
/usr/bin/time -f 'wall_sec=%e' -o {BASE}/{name}_time.txt \
  {BIN} --codec wirehair --wh-segment-mib=10 --wh-repair-pct={REPAIR_PCT} --wh-ack \
  --ack-port={ACK_PORT} --local-node-id 1 --final-dst 4 --ttl 8 --rate-mbps {RATE_MBPS} \
  --udp-send 10.10.12.2 {PORT} {BASE}/input.bin >{slog} 2>&1
cat {BASE}/{name}_time.txt
echo ---SENDLOG---
cat {slog}
""",
        timeout=600,
    )
    print(send.stdout)

    for _ in range(250):
        alive = ssh(
            SSH2,
            f"kill -0 $(cat {BASE}/{name}_recv.pid) 2>/dev/null",
            check=False,
        )
        if alive.returncode != 0:
            break
        time.sleep(0.2)

    recv = ssh(
        SSH2,
        f"echo ---RECV---; cat {rlog}; echo ---OUT---; ls -l {out}; "
        f"sha256sum {out}; cat {BASE}/input.sha",
        check=False,
    )
    print(recv.stdout)

    verify = ssh(
        SSH2,
        f"""
exp=$(cut -d' ' -f1 {BASE}/input.sha)
got=$(sha256sum {out} | awk '{{print $1}}')
echo exp=$exp
echo got=$got
if [ "$exp" = "$got" ]; then echo VERIFY=OK; else echo VERIFY=FAIL; fi
""",
    )
    print(verify.stdout)

    parsed = parse_send_log(send.stdout)
    parsed["loss_pct"] = loss
    parsed["verify"] = "OK" if "VERIFY=OK" in verify.stdout else "FAIL"
    print("PARSED", parsed, flush=True)
    return parsed


def main():
    cleanup_procs()
    set_loss(0)
    prepare_input()
    results = {
        "clean": run_case("clean", 0),
        "loss3": run_case("loss3", 3),
    }
    set_loss(0)
    cleanup_procs()

    print("\n===== SUMMARY =====")
    for k, v in results.items():
        print(k, v)

    if results["clean"]["verify"] != "OK" or results["loss3"]["verify"] != "OK":
        raise SystemExit(2)
    print("BOTH_FILES_OK")

    rc = results["clean"].get("repair_sent")
    rl = results["loss3"].get("repair_sent")
    print(f"repair_sent clean={rc} loss3={rl}")
    if rc is not None and rl is not None and rl > rc:
        print("LOSS_USED_MORE_REPAIR: yes")
    else:
        print("LOSS_USED_MORE_REPAIR: no/unclear")

    tc = results["clean"].get("time_wall_s") or results["clean"].get("elapsed_s")
    tl = results["loss3"].get("time_wall_s") or results["loss3"].get("elapsed_s")
    print(f"TIME clean={tc} loss3={tl}")
    if tl is not None and tc is not None and tl > tc:
        print("LOSS_TOOK_LONGER: yes")
    else:
        print("LOSS_TOOK_LONGER: no/unclear")


if __name__ == "__main__":
    main()
