#!/usr/bin/env python3
"""PFC stage-2 matrix: loss × ACK/fixed-repair × direct/relay (× repeats).

Does not modify upstream scripts; lives under scripts/local/.

Fixed: --codec wirehair --wh-segment-mib=10 --wh-window=4
Loss injection: sudo -n tc netem on VM1 {NETEM_IFACE} (restored after each case).

Env:
  WH_LOSS_SMOKE=1   small subset (validate path)
  WH_LOSS_SKIP=N    skip first N completed cases
  WH_LOSS_FRESH=1   ignore prior JSON
  WH_LOSS_NAME=...  output basename under build/
  WH_LOSS_MIB=40    payload size in MiB (multiple of segment recommended)
  WH_LOSS_RATE=100  sender --rate-mbps
"""

from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parent))
from topo_lab import N1, N2, N3, NETEM_IFACE  # noqa: E402
N2_DIRECT = N2
N1_DATA = N1
N3_DATA = N3

SSH1 = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.161"]
SSH2 = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.162"]
SSH3 = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.163"]

WG = "/home/fyp1/work/multi-flow-rate-control/build/wg_multi_pipeline"
RELAY = "/home/fyp1/work/multi-flow-rate-control/build/wire_relay"
BASE = "/tmp/wirehair_loss_repair_matrix"
PORT_BASE = 25100
ACK_BASE = 25200


SEGMENT_MIB = 10
WINDOW = 4
IDLE_SEC = 20
RELAY_IDLE = 35
POST_SEND_WAIT_S = 40
FILE_MIB = int(os.environ.get("WH_LOSS_MIB", "40"))
RATE_MBPS = float(os.environ.get("WH_LOSS_RATE", "100"))
REPEATS = 3
LOSSES = [0, 1, 3, 5, 10, 20]
# mode: (name, ack:bool, repair_pct:int)
MODES = [
    ("ack", True, 10),
    ("noack_r5", False, 5),
    ("noack_r10", False, 10),
    ("noack_r20", False, 20),
    ("noack_r30", False, 30),
]
TOPOS = ["direct", "relay"]

SMOKE = os.environ.get("WH_LOSS_SMOKE", "0") == "1"
SKIP_CASES = int(os.environ.get("WH_LOSS_SKIP", "0"))
FRESH = os.environ.get("WH_LOSS_FRESH", "0") == "1"
NAME = os.environ.get("WH_LOSS_NAME", "wirehair_loss_repair_matrix")
if re.fullmatch(r"[A-Za-z0-9_.-]+", NAME) is None:
    raise ValueError("WH_LOSS_NAME contains unsupported characters")

if SMOKE:
    LOSSES = [0, 3]
    MODES = [("ack", True, 10), ("noack_r10", False, 10)]
    TOPOS = ["direct", "relay"]
    REPEATS = 1
    NAME = NAME + "_smoke"

REPO = Path(__file__).resolve().parents[2]
LOCAL_JSON = REPO / "build" / f"{NAME}.json"
LOCAL_CSV = REPO / "build" / f"{NAME}.csv"
LOCAL_LOG = REPO / "build" / f"{NAME}.log"


def run(cmd, check=True, timeout=1200):
    r = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if check and r.returncode != 0:
        print("FAIL:", cmd, file=sys.stderr)
        print(r.stdout[-4000:], file=sys.stderr)
        print(r.stderr[-4000:], file=sys.stderr)
        raise SystemExit(r.returncode or 1)
    return r


def ssh(host, script, check=True, timeout=1200):
    return run(host + [script], check=check, timeout=timeout)


def wait_grep(host, log, pat, n=400):
    for _ in range(n):
        q = pat.replace("'", "'\\''")
        if ssh(host, f"grep -q '{q}' {log}", check=False).returncode == 0:
            return True
        time.sleep(0.1)
    return False


def cleanup():
    for host, pat in (
        (SSH1, "wg_multi_pipeline.*(251|252)"),
        (SSH2, "wire_relay.*251|wg_multi_pipeline.*(251|252)"),
        (SSH3, "wg_multi_pipeline.*(251|252)"),
    ):
        ssh(host, f"pkill -f '{pat}' >/dev/null 2>&1 || true", check=False)
    time.sleep(0.4)


def set_loss(pct: int):
    # Prefer passwordless sudo -n; else WH_SUDO_PASS via sudo -S (do not log password).
    sudo_pass = os.environ.get("WH_SUDO_PASS", "")
    if sudo_pass:
        esc = sudo_pass.replace("'", "'\\''")
        pre = f"printf '%s\\n' '{esc}' | sudo -S -p '' "
    else:
        pre = "sudo -n "
    if pct <= 0:
        out = ssh(
            SSH1,
            f"{pre}tc qdisc del dev {NETEM_IFACE} root 2>/dev/null || true; "
            f"tc qdisc show dev {NETEM_IFACE}",
        )
    else:
        out = ssh(
            SSH1,
            f"{pre}tc qdisc replace dev {NETEM_IFACE} root netem loss {pct}%; "
            f"tc qdisc show dev {NETEM_IFACE}",
        )
    return out.stdout.strip()


def prepare():
    ssh(
        SSH1,
        f"""
set -e
mkdir -p {BASE}
python3 - <<'PY'
from pathlib import Path
import hashlib
path = Path("{BASE}/input.bin")
size = {FILE_MIB} * 1024 * 1024
if not (path.is_file() and path.stat().st_size == size):
    block = bytes((i * 37 + i // 19) & 0xff for i in range(65536))
    with path.open("wb") as f:
        left = size
        while left:
            n = min(left, len(block))
            f.write(block[:n])
            left -= n
h = hashlib.sha256()
with path.open("rb") as f:
    for chunk in iter(lambda: f.read(1 << 20), b""):
        h.update(chunk)
Path("{BASE}/input.sha").write_text(h.hexdigest() + "  input.bin\\n")
print(path, path.stat().st_size, h.hexdigest())
PY
""",
        timeout=600,
    )
    for host, dest in ((SSH2, "fyp1@10.10.10.162"), (SSH3, "fyp1@10.10.10.163")):
        ssh(host, f"mkdir -p {BASE}")
        run(
            [
                "scp",
                "-o",
                "BatchMode=yes",
                f"fyp1@10.10.10.161:{BASE}/input.sha",
                f"{dest}:{BASE}/",
            ]
        )


def parse_send(text: str) -> dict:
    row: dict = {}
    m = re.search(r"wall_sec=([0-9.]+)", text)
    if m:
        row["wall_sec"] = float(m.group(1))
    m = re.search(
        r"wirehair-send: source_bytes=(\d+) segments=(\d+) repair_sent=(\d+) "
        r"wire_bytes=(\d+) ack=(\w+)",
        text,
    )
    if m:
        row["source_bytes"] = int(m.group(1))
        row["segments"] = int(m.group(2))
        row["repair_sent"] = int(m.group(3))
        row["wire_bytes"] = int(m.group(4))
        row["ack"] = m.group(5)
    m = re.search(r"repair_rounds=(\d+)", text)
    if m:
        row["repair_rounds"] = int(m.group(1))
    m = re.search(r"ack_timeout=yes", text)
    row["ack_timeout"] = bool(m)
    m = re.search(r"status=(\w+)", text)
    if m:
        row["send_status"] = m.group(1)
    if row.get("wall_sec") and row.get("source_bytes"):
        row["goodput_mbps"] = row["source_bytes"] * 8.0 / row["wall_sec"] / 1e6
    if row.get("source_bytes") and row.get("wire_bytes"):
        row["repair_overhead_pct"] = (
            (row["wire_bytes"] / row["source_bytes"]) - 1.0
        ) * 100.0
    return row


def parse_recv(text: str) -> dict:
    row: dict = {}
    m = re.search(r"latency end-to-end:.*?p95_us=([0-9.]+)", text)
    if m:
        row["e2e_p95_us"] = float(m.group(1))
    drops = re.findall(r"ahead_window_drops=(\d+)", text)
    if drops:
        row["ahead_window_drops"] = sum(int(x) for x in drops)
    if "decode_complete=yes" in text:
        row["decode_complete"] = True
    return row


def verify(sink_host, out_path: str) -> dict:
    ver = ssh(
        sink_host,
        f"""
set +e
exp=$(cut -d' ' -f1 {BASE}/input.sha)
if [ ! -f '{out_path}' ]; then
  echo VERIFY=MISSING
  echo OUT_BYTES=0
  exit 0
fi
got=$(sha256sum '{out_path}' | awk '{{print $1}}')
bytes=$(stat -c%s '{out_path}')
echo OUT_BYTES=$bytes
echo exp=$exp
echo got=$got
if [ "$exp" = "$got" ]; then echo VERIFY=OK; else echo VERIFY=FAIL; fi
""",
        check=False,
    )
    row = {"checksum_ok": False, "out_bytes": 0, "verify": "FAIL"}
    m = re.search(r"OUT_BYTES=(\d+)", ver.stdout)
    if m:
        row["out_bytes"] = int(m.group(1))
    if "VERIFY=OK" in ver.stdout:
        row["checksum_ok"] = True
        row["verify"] = "OK"
    elif "VERIFY=MISSING" in ver.stdout:
        row["verify"] = "MISSING"
    row["verify_raw"] = ver.stdout[-500:]
    return row


def case_list():
    cases = []
    idx = 0
    for loss in LOSSES:
        for mode_name, ack, repair in MODES:
            for topo in TOPOS:
                for rep in range(1, REPEATS + 1):
                    cases.append(
                        {
                            "idx": idx,
                            "loss_pct": loss,
                            "mode": mode_name,
                            "ack": ack,
                            "repair_pct": repair,
                            "topo": topo,
                            "rep": rep,
                        }
                    )
                    idx += 1
    return cases


def one_case(c: dict, port: int) -> dict:
    cleanup()
    tc_show = set_loss(c["loss_pct"])
    ack_flag = "--wh-ack" if c["ack"] else "--no-wh-ack"
    repair = c["repair_pct"]
    topo = c["topo"]
    tag = (
        f"{topo}_loss{c['loss_pct']}_{c['mode']}_rep{c['rep']}"
    )
    prefix = f"{BASE}/{tag}.out"
    rlog = f"{BASE}/{tag}_recv.log"
    slog = f"{BASE}/{tag}_send.log"
    xlog = f"{BASE}/{tag}_relay.log"
    ack_port = ACK_BASE + (port - PORT_BASE)
    sink = SSH2 if topo == "direct" else SSH3

    for host in (SSH1, SSH2, SSH3):
        ssh(host, f"mkdir -p {BASE}")
    ssh(sink, f"rm -f {prefix} {rlog}; : > {rlog}")
    ssh(SSH1, f"rm -f {slog}; : > {slog}")
    if topo == "relay":
        ssh(SSH2, f"rm -f {xlog}; : > {xlog}")

    recv_cmd = f"""
nohup {WG} --codec wirehair --wh-segment-mib={SEGMENT_MIB} --wh-repair-pct={repair} \
  --wh-window={WINDOW} {ack_flag} \
  --local-node-id 4 --udp-recv {port} {prefix} --idle-sec {IDLE_SEC} --strict \
  >{rlog} 2>&1 &
echo $! > {BASE}/{tag}_recv.pid
"""
    ssh(sink, recv_cmd)
    if not wait_grep(sink, rlog, "wirehair-recv: listening", 400):
        set_loss(0)
        return {
            **c,
            "verify": "RECV_FAIL",
            "tc": tc_show,
            "recv_log": ssh(sink, f"tail -40 {rlog}", check=False).stdout[-1500:],
        }

    if topo == "relay":
        ssh(
            SSH2,
            f"""
nohup {RELAY} --local-node-id 2 --listen {port} \
  --next-hop {N3_DATA}:{port} --return-hop {N1_DATA}:{ack_port} \
  --idle-exit-sec {RELAY_IDLE} \
  >{xlog} 2>&1 &
echo $! > {BASE}/{tag}_relay.pid
""",
        )
        if not wait_grep(SSH2, xlog, "wire-relay: local_node_id", 300):
            set_loss(0)
            return {
                **c,
                "verify": "RELAY_FAIL",
                "tc": tc_show,
                "relay_log": ssh(SSH2, f"tail -40 {xlog}", check=False).stdout[-1500:],
            }

    ack_port_arg = f"--ack-port={ack_port}" if c["ack"] else ""
    send_script = f"""
set -e
/usr/bin/time -f 'wall_sec=%e' -o {BASE}/{tag}_time.txt \
  {WG} --codec wirehair --wh-segment-mib={SEGMENT_MIB} --wh-repair-pct={repair} \
  --wh-window={WINDOW} {ack_flag} {ack_port_arg} \
  --local-node-id 1 --final-dst 4 --ttl 8 --rate-mbps {RATE_MBPS:g} \
  --udp-send {N2_DIRECT} {port} {BASE}/input.bin >{slog} 2>&1
cat {BASE}/{tag}_time.txt
echo ---SEND---
cat {slog}
"""
    try:
        send = ssh(SSH1, send_script, timeout=900)
        send_out = send.stdout
    except SystemExit:
        send_out = ssh(
            SSH1, f"cat {BASE}/{tag}_time.txt {slog} 2>/dev/null || true", check=False
        ).stdout
        row = {**c, "verify": "SEND_FAIL", "tc": tc_show}
        row.update(parse_send(send_out))
        set_loss(0)
        cleanup()
        return row

    deadline = time.time() + POST_SEND_WAIT_S
    while time.time() < deadline:
        if (
            ssh(
                sink,
                f"kill -0 $(cat {BASE}/{tag}_recv.pid) 2>/dev/null",
                check=False,
            ).returncode
            != 0
        ):
            break
        time.sleep(0.2)
    else:
        ssh(
            sink,
            f"kill $(cat {BASE}/{tag}_recv.pid) 2>/dev/null || true",
            check=False,
        )
        time.sleep(0.3)

    if topo == "relay":
        ssh(
            SSH2,
            f"kill $(cat {BASE}/{tag}_relay.pid) 2>/dev/null || true",
            check=False,
        )

    recv_text = ssh(sink, f"cat {rlog}", check=False).stdout
    row = {**c, "tc": tc_show, "port": port}
    row.update(parse_send(send_out))
    row.update(parse_recv(recv_text))
    row.update(verify(sink, prefix))
    src = row.get("source_bytes") or (FILE_MIB * 1024 * 1024)
    if src:
        row["byte_completion_pct"] = 100.0 * row.get("out_bytes", 0) / src
    if "e2e_p95_us" not in row:
        row["e2e_p95_us"] = None  # wirehair recv path may not emit block latency

    set_loss(0)
    cleanup()
    # drop bulky logs from JSON unless failed
    if row.get("verify") == "OK":
        row.pop("verify_raw", None)
    return row


def save(results):
    LOCAL_JSON.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_JSON.write_text(json.dumps(results, indent=2))
    fields = [
        "idx",
        "topo",
        "loss_pct",
        "mode",
        "ack",
        "repair_pct",
        "rep",
        "wall_sec",
        "goodput_mbps",
        "repair_overhead_pct",
        "repair_sent",
        "repair_rounds",
        "byte_completion_pct",
        "e2e_p95_us",
        "checksum_ok",
        "verify",
        "ack_timeout",
        "send_status",
    ]
    with LOCAL_CSV.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in results:
            w.writerow(r)


def main():
    cases = case_list()
    print(
        f"cases={len(cases)} file_mib={FILE_MIB} rate={RATE_MBPS} "
        f"segment={SEGMENT_MIB} window={WINDOW} smoke={SMOKE}",
        flush=True,
    )
    print(f"json={LOCAL_JSON}", flush=True)

    results = []
    if LOCAL_JSON.is_file() and not FRESH:
        try:
            results = json.loads(LOCAL_JSON.read_text())
            print(f"resuming with {len(results)} prior rows", flush=True)
        except json.JSONDecodeError:
            results = []

    cleanup()
    set_loss(0)
    prepare()

    done = len(results)
    if SKIP_CASES > done:
        done = SKIP_CASES

    for c in cases:
        if c["idx"] < done:
            continue
        port = PORT_BASE + (c["idx"] % 80)
        print(
            f"\n===== [{c['idx']+1}/{len(cases)}] topo={c['topo']} "
            f"loss={c['loss_pct']}% mode={c['mode']} rep={c['rep']} =====",
            flush=True,
        )
        try:
            row = one_case(c, port)
        except Exception as exc:  # noqa: BLE001
            set_loss(0)
            cleanup()
            row = {**c, "verify": "EXCEPTION", "error": str(exc)}
        results.append(row)
        save(results)
        print(
            "RESULT",
            {
                k: row.get(k)
                for k in (
                    "verify",
                    "wall_sec",
                    "goodput_mbps",
                    "repair_overhead_pct",
                    "byte_completion_pct",
                    "e2e_p95_us",
                    "checksum_ok",
                    "repair_sent",
                )
            },
            flush=True,
        )

    set_loss(0)
    cleanup()
    save(results)
    ok = sum(1 for r in results if r.get("verify") == "OK")
    print(f"\nDONE ok={ok}/{len(results)} -> {LOCAL_JSON}", flush=True)
    if ok < len(results):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
