#!/usr/bin/env python3
"""VM Wirehair+ACK regression after window/MTU/repair fixes."""

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
BASE = "/tmp/wirehair_ack_retest"
PORT = 23700
ACK_PORT = 23701
RATE_MBPS = 40
REPAIR_PCT = 20
WINDOW = 8


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
            "pkill -f 'wg_multi_pipeline.*2370' >/dev/null 2>&1 || true",
            check=False,
        )
    time.sleep(0.4)


def set_loss(pct: float):
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


def prepare(mib: int) -> str:
    path = f"{BASE}/input_{mib}m.bin"
    ssh(
        SSH1,
        f"""
set -e
mkdir -p {BASE}
python3 - <<'PY'
from pathlib import Path
size = {mib} * 1024 * 1024
path = Path("{path}")
block = bytes((i * 37 + i // 17 + 23) & 0xff for i in range(65536))
with path.open("wb") as f:
    left = size
    while left:
        chunk = block[:min(left, len(block))]
        f.write(chunk)
        left -= len(chunk)
print(path.stat().st_size)
PY
sha256sum {path} | tee {path}.sha
""",
    )
    ssh(SSH2, f"mkdir -p {BASE}")
    run(
        [
            "scp",
            "-o",
            "BatchMode=yes",
            f"fyp1@10.10.10.161:{path}.sha",
            f"fyp1@10.10.10.162:{path}.sha",
        ]
    )
    return path


def run_case(name: str, input_path: str, loss: float) -> dict:
    print(f"\n===== {name} loss={loss}% =====", flush=True)
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
nohup {BIN} --codec wirehair --wh-segment-mib=10 --wh-repair-pct={REPAIR_PCT} \
  --wh-window={WINDOW} --wh-ack \
  --local-node-id 4 --udp-recv {PORT} {out} --idle-sec 35 --strict \
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
  {BIN} --codec wirehair --wh-segment-mib=10 --wh-repair-pct={REPAIR_PCT} \
  --wh-window={WINDOW} --wh-ack --ack-port={ACK_PORT} \
  --local-node-id 1 --final-dst 4 --ttl 8 --rate-mbps {RATE_MBPS} \
  --udp-send 10.10.12.2 {PORT} {input_path} >{slog} 2>&1
cat {BASE}/{name}_time.txt
echo ---SENDLOG---
cat {slog}
""",
        timeout=600,
    )
    print(send.stdout.strip())

    for _ in range(300):
        if (
            ssh(
                SSH2,
                f"kill -0 $(cat {BASE}/{name}_recv.pid) 2>/dev/null",
                check=False,
            ).returncode
            != 0
        ):
            break
        time.sleep(0.2)

    verify = ssh(
        SSH2,
        f"""
exp=$(cut -d' ' -f1 {input_path}.sha)
got=$(sha256sum {out} | awk '{{print $1}}')
echo exp=$exp
echo got=$got
ls -l {out}
if [ "$exp" = "$got" ]; then echo VERIFY=OK; else echo VERIFY=FAIL; fi
echo ---RECV---
cat {rlog}
""",
    )
    print(verify.stdout.strip())

    parsed: dict = {"loss": loss, "verify": "FAIL"}
    m = re.search(
        r"wirehair-send: source_bytes=(\d+) segments=(\d+) repair_sent=(\d+) "
        r"wire_bytes=(\d+) ack=(\w+)",
        send.stdout,
    )
    if m:
        parsed.update(
            {
                "source_bytes": int(m.group(1)),
                "segments": int(m.group(2)),
                "repair_sent": int(m.group(3)),
                "wire_bytes": int(m.group(4)),
                "ack": m.group(5),
            }
        )
    t = re.search(r"wall_sec=([0-9.]+)", send.stdout)
    if t:
        parsed["wall_s"] = float(t.group(1))
    parsed["verify"] = "OK" if "VERIFY=OK" in verify.stdout else "FAIL"
    print("PARSED", parsed, flush=True)
    return parsed


def main():
    cleanup_procs()
    set_loss(0)
    ssh(SSH1, f"mkdir -p {BASE}")
    ssh(SSH2, f"mkdir -p {BASE}")

    path50 = prepare(50)
    results = {
        "50m_clean": run_case("50m_clean", path50, 0),
        "50m_loss3": run_case("50m_loss3", path50, 3),
    }

    set_loss(0)
    cleanup_procs()

    print("\n===== SUMMARY =====")
    for k, v in results.items():
        print(k, v)

    ok = all(v["verify"] == "OK" for v in results.values())
    print("BOTH_OK", ok)
    if (
        results["50m_clean"].get("repair_sent") is not None
        and results["50m_loss3"].get("repair_sent") is not None
    ):
        print(
            "MORE_REPAIR",
            results["50m_loss3"]["repair_sent"]
            > results["50m_clean"]["repair_sent"],
        )
    if (
        results["50m_clean"].get("wall_s") is not None
        and results["50m_loss3"].get("wall_s") is not None
    ):
        print(
            "LONGER",
            results["50m_loss3"]["wall_s"] > results["50m_clean"]["wall_s"],
        )
    if not ok:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
