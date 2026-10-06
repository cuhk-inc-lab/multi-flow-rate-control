#!/usr/bin/env python3
"""Run real BATS on the linear VM path.

VM1 source --10.20.20--> VM2 relay --10.30.30--> VM3 relay --10.40.40--> VM4 dest

Each hop calls bats_encode / bats_recode / bats_decode from ../bats.
This is not wire_relay --bats-recoder identity.

Env:
  BATS_K=128
  BATS_T=256
  BATS_BYTES=32768
  BATS_LOSS=0          # artificial DATA loss percent on source and both relays
  BATS_RATE=200
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
BIN = REPO / "build" / "bats_line"
REMOTE_BIN = "/home/fyp1/work/multi-flow-rate-control/build/bats_line"
SSH_OPTS = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
HOST = {
    1: "fyp1@10.10.10.161",
    2: "fyp1@10.10.10.162",
    3: "fyp1@10.10.10.163",
    4: "fyp1@10.10.10.164",
}

K = int(os.environ.get("BATS_K", "128"))
T = int(os.environ.get("BATS_T", "256"))
NBYTES = int(os.environ.get("BATS_BYTES", "32768"))
LOSS = int(os.environ.get("BATS_LOSS", "0"))
RATE = int(os.environ.get("BATS_RATE", "200"))
FLOWS = int(os.environ.get("BATS_FLOWS", "1"))
IDLE = int(os.environ.get("BATS_IDLE", "30"))
WAIT_SEC = int(os.environ.get("BATS_WAIT_SEC", "40"))
SRC_TIMEOUT = int(os.environ.get("BATS_SRC_TIMEOUT", "180"))
PORT = {2: 19020, 3: 19030, 4: 19040}
BIND = {2: "10.20.20.2", 3: "10.30.30.2", 4: "10.40.40.2"}
NEXT = {1: "10.20.20.2:19020", 2: "10.30.30.2:19030", 3: "10.40.40.2:19040"}


def run(cmd: list[str], check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if check and proc.returncode != 0:
        sys.stderr.write("FAIL: " + " ".join(cmd) + "\n")
        sys.stderr.write(proc.stdout[-2000:] + proc.stderr[-2000:])
        raise SystemExit(proc.returncode or 1)
    return proc


def ssh(node: int, script: str, check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return run(["ssh", "-n", *SSH_OPTS, HOST[node], script], check=check, timeout=timeout)


def wait_log(node: int, path: str, pat: str, n: int = 80) -> bool:
    q = pat.replace("'", "'\\''")
    for _ in range(n):
        if ssh(node, f"grep -q '{q}' {path}", check=False).returncode == 0:
            return True
        time.sleep(0.1)
    return False


def stop() -> None:
    for node in (1, 2, 3, 4):
        ssh(node, "pkill -f '[b]ats_line' >/dev/null 2>&1 || true", check=False)
    time.sleep(0.3)


def main() -> None:
    if not BIN.is_file():
        raise SystemExit(f"missing {BIN}; make bats-line first")
    out_dir = REPO / "build" / "bats_linear"
    tag = os.environ.get("BATS_TAG", "").strip()
    if tag:
        out_dir = out_dir / tag
    out_dir.mkdir(parents=True, exist_ok=True)

    if os.environ.get("BATS_SKIP_SCP", "0") != "1":
        print("copy binary")
        for node in (1, 2, 3, 4):
            run(["scp", *SSH_OPTS, str(BIN), f"{HOST[node]}:{REMOTE_BIN}"])

    print("preflight linear hops")
    ssh(1, "ping -c 1 -W 1 10.20.20.2 >/dev/null")
    ssh(2, "ping -c 1 -W 1 10.30.30.2 >/dev/null")
    ssh(3, "ping -c 1 -W 1 10.40.40.2 >/dev/null")

    stop()
    ssh(
        1,
        f"""python3 - <<'PY'
flows = {FLOWS}
nbytes = {NBYTES}
for flow in range(flows):
    block = bytes(((flow + 1) * i * 17 + 3 + flow) & 255 for i in range(1024 * 1024))
    path = '/tmp/bats_in.bin' if flow == 0 else f'/tmp/bats_in.bin.{{flow}}'
    left = nbytes
    with open(path, 'wb') as f:
        while left:
            n = min(left, len(block))
            f.write(block[:n])
            left -= n
print(flows, nbytes)
PY""",
        timeout=180,
    )

    remote_in = "/tmp/bats_in.bin"
    remote_out = "/tmp/bats_out.bin"
    logs = {
        4: "/tmp/bats_line_dest.log",
        3: "/tmp/bats_line_relay2.log",
        2: "/tmp/bats_line_relay1.log",
        1: "/tmp/bats_line_source.log",
    }
    for node in (1, 2, 3, 4):
        ssh(node, f"rm -f {logs[node]}", check=False)
    ssh(4, "rm -f /tmp/bats_out.bin /tmp/bats_out.bin.[0-9]*", check=False)
    ssh(1, "rm -f /tmp/bats_line_source.log /tmp/bats_line_source.*.log", check=False)

    common = (
        f"--k {K} --t {T} --seed 0xB175 --reorder-ms 20 --idle-sec {IDLE} "
        f"--loss-percent {LOSS} --rate-mbps {RATE}"
    )
    hop = f"{common} --flows {FLOWS}"
    ssh(
        4,
        f"nohup {REMOTE_BIN} --role dest --bind {BIND[4]} --listen {PORT[4]} "
        f"--output {remote_out} {hop} >{logs[4]} 2>&1 &",
    )
    if not wait_log(4, logs[4], "bats-line: listening"):
        ssh(4, f"cat {logs[4]}", check=False)
        raise SystemExit("dest did not listen")
    ssh(
        3,
        f"nohup {REMOTE_BIN} --role relay --bind {BIND[3]} --listen {PORT[3]} "
        f"--next {NEXT[3]} --recode-seed 22 --loss-seed 3 {hop} >{logs[3]} 2>&1 &",
    )
    if not wait_log(3, logs[3], "bats-line: listening"):
        ssh(3, f"cat {logs[3]}", check=False)
        raise SystemExit("relay2 did not listen")
    ssh(
        2,
        f"nohup {REMOTE_BIN} --role relay --bind {BIND[2]} --listen {PORT[2]} "
        f"--next {NEXT[2]} --recode-seed 11 --loss-seed 2 {hop} >{logs[2]} 2>&1 &",
    )
    if not wait_log(2, logs[2], "bats-line: listening"):
        ssh(2, f"cat {logs[2]}", check=False)
        raise SystemExit("relay1 did not listen")

    t0 = time.monotonic()
    src_script = "fail=0\npids=\n"
    for flow in range(FLOWS):
        remote_flow_in = remote_in if flow == 0 else f"{remote_in}.{flow}"
        src_script += (
            f"{REMOTE_BIN} --role source --bind 10.20.20.1 --flow-id {flow} "
            f"--input {remote_flow_in} --next {NEXT[1]} --loss-seed {flow + 1} "
            f"{common} >/tmp/bats_line_source.{flow}.log 2>&1 &\n"
            "pids=\"$pids $!\"\n"
        )
    src_script += (
        "for p in $pids; do wait \"$p\" || fail=1; done\n"
        "cat /tmp/bats_line_source.*.log > /tmp/bats_line_source.log\n"
        "exit $fail\n"
    )
    ssh(1, src_script, timeout=SRC_TIMEOUT)

    deadline = time.monotonic() + WAIT_SEC
    done = False
    while time.monotonic() < deadline:
        text = ssh(4, f"cat {logs[4]}", check=False).stdout
        if "gens_ok=" in text or " ok=0 " in text or "idle timeout" in text:
            done = "gens_ok=" in text
            break
        time.sleep(0.25)
    if not done:
        for node, path in logs.items():
            text = ssh(node, f"cat {path}", check=False).stdout
            (out_dir / f"vm{node}.log").write_text(text)
            print(f"===== vm{node} =====")
            print(text, end="" if text.endswith("\n") else "\n")
        raise SystemExit("dest did not finish decode")

    for node, path in logs.items():
        text = ssh(node, f"cat {path}").stdout
        (out_dir / f"vm{node}.log").write_text(text)
        print(f"===== vm{node} =====")
        print(text, end="" if text.endswith("\n") else "\n")

    for flow in range(FLOWS):
        remote_flow_in = remote_in if flow == 0 else f"{remote_in}.{flow}"
        remote_flow_out = remote_out if flow == 0 else f"{remote_out}.{flow}"
        in_hash = ssh(1, f"sha256sum {remote_flow_in}").stdout.split()[0]
        out_hash = ssh(4, f"sha256sum {remote_flow_out}").stdout.split()[0]
        print(f"sha256 flow={flow} in  {in_hash}")
        print(f"sha256 flow={flow} out {out_hash}")
        if in_hash != out_hash:
            raise SystemExit(f"file mismatch flow={flow}")
    for name, needle in (
        ("vm1.log", "api=bats_encode"),
        ("vm1.log", "unit_coeff_ok=1"),
        ("vm1.log", "g_on_wire=0"),
        ("vm2.log", "api=bats_recode"),
        ("vm3.log", "api=bats_recode"),
        ("vm4.log", "api=bats_decode"),
        ("vm4.log", "dense_coeff_packets="),
    ):
        if needle not in (out_dir / name).read_text():
            raise SystemExit(f"missing {needle} in {name}")
    dense = 0
    for line in (out_dir / "vm4.log").read_text().splitlines():
        if "dense_coeff_packets=" in line:
            dense = int(line.split("dense_coeff_packets=")[1].split()[0])
    if dense <= 0:
        raise SystemExit("destination coefficients were not recoded")
    total_bytes = NBYTES * FLOWS
    e2e_s = time.monotonic() - t0
    goodput = (total_bytes * 8 / 1e6) / e2e_s if e2e_s > 0 else 0
    print(
        f"PASS linear VMs flows={FLOWS} bytes={NBYTES} K={K} T={T} loss={LOSS}% "
        f"rate={RATE} dense={dense} e2e_s={e2e_s:.3f} goodput_mbps={goodput:.1f}"
    )
    stop()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        stop()
        raise
