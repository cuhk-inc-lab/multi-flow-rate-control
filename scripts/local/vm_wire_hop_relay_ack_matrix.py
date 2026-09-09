#!/usr/bin/env python3
"""Stage-3 redo: hops × kernel-vs-app-relay × codecs × wirehair ACK.

Paths:
  hop1_direct   VM1 -> VM2 (10.10.12.2)            [no app relay]
  hop2_kernel   VM1 -> VM3 (10.10.23.2) via ip_forward
  hop2_relay    VM1 -> VM2 wire_relay -> VM3
  hop3_kernel   VM1 -> VM4 (10.10.34.2) via ip_forward
  hop3_relay    VM1 -> VM2 relay -> VM3 relay -> VM4

Codecs: copy block xor-fec rs-fec rs
Wirehair: --wh-ack and --no-wh-ack (segment-mib=10 window=4 repair=10)

Env:
  WH_HOP_SMOKE=1   tiny subset
  WH_HOP_FRESH=1   ignore prior JSON
  WH_HOP_SKIP=N
  WH_HOP_NAME=...
  WH_HOP_MIB=64
  WH_HOP_RATES="200 400 500 600 750"
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

SSH = {
    1: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.161"],
    2: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.162"],
    3: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.163"],
    4: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.164"],
}

WG = "/home/fyp1/work/multi-flow-rate-control/build/wg_multi_pipeline"
RELAY = "/home/fyp1/work/multi-flow-rate-control/build/wire_relay"
BASE = "/tmp/wire_hop_matrix"
PORT_BASE = 26100
ACK_BASE = 26200

N1 = "10.10.12.1"
N2 = "10.10.12.2"
N3 = "10.10.23.2"
N4 = "10.10.34.2"

FILE_MIB = int(os.environ.get("WH_HOP_MIB", "64"))
RATES = [
    int(x)
    for x in os.environ.get("WH_HOP_RATES", "200 400 500 600 750").split()
]
IDLE_SEC = int(os.environ.get("WH_HOP_IDLE", "20"))
RELAY_IDLE = IDLE_SEC + 20
POST_SEND_WAIT_S = 45
WH_SEGMENT = 10
WH_WINDOW = 4
WH_REPAIR = 10

PATHS = [
    # name, hops, app_relay, sink_node, next_hop_ip, relay_chain [(node, next_ip)]
    ("hop1_direct", 1, False, 2, N2, []),
    ("hop2_kernel", 2, False, 3, N3, []),
    ("hop2_relay", 2, True, 3, N2, [(2, N3)]),
    ("hop3_kernel", 3, False, 4, N4, []),
    ("hop3_relay", 3, True, 4, N2, [(2, "10.10.23.2"), (3, N4)]),
]

# codec_mode: (label, codec, wh_ack or None)
CODEC_MODES = [
    ("copy", "copy", None),
    ("block", "block", None),
    ("xor-fec", "xor-fec", None),
    ("rs-fec", "rs-fec", None),
    ("rs", "rs", None),
    ("wirehair_noack", "wirehair", False),
    ("wirehair_ack", "wirehair", True),
]

SMOKE = os.environ.get("WH_HOP_SMOKE", "0") == "1"
FRESH = os.environ.get("WH_HOP_FRESH", "0") == "1"
SKIP = int(os.environ.get("WH_HOP_SKIP", "0"))
NAME = os.environ.get("WH_HOP_NAME", "wire_hop_relay_ack_matrix")
if re.fullmatch(r"[A-Za-z0-9_.-]+", NAME) is None:
    raise ValueError("bad WH_HOP_NAME")

if SMOKE:
    PATHS = [p for p in PATHS if p[0] in ("hop1_direct", "hop2_relay", "hop3_relay")]
    CODEC_MODES = [
        ("copy", "copy", None),
        ("wirehair_noack", "wirehair", False),
        ("wirehair_ack", "wirehair", True),
    ]
    RATES = [200, 400]
    NAME = NAME + "_smoke"

REPO = Path(__file__).resolve().parents[2]
LOCAL_JSON = REPO / "build" / f"{NAME}.json"
LOCAL_CSV = REPO / "build" / f"{NAME}.csv"


def run(cmd, check=True, timeout=1200):
    r = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if check and r.returncode != 0:
        print("FAIL:", cmd, file=sys.stderr)
        print((r.stdout or "")[-3000:], file=sys.stderr)
        print((r.stderr or "")[-3000:], file=sys.stderr)
        raise SystemExit(r.returncode or 1)
    return r


def ssh(node, script, check=True, timeout=1200):
    return run(SSH[node] + [script], check=check, timeout=timeout)


def wait_grep(node, log, pat, n=400):
    for _ in range(n):
        q = pat.replace("'", "'\\''")
        if ssh(node, f"grep -q '{q}' {log}", check=False).returncode == 0:
            return True
        time.sleep(0.1)
    return False


def cleanup():
    pats = {
        1: "wg_multi_pipeline.*(261|262)",
        2: "wire_relay.*261|wg_multi_pipeline.*(261|262)",
        3: "wire_relay.*261|wg_multi_pipeline.*(261|262)",
        4: "wg_multi_pipeline.*(261|262)",
    }
    for node, pat in pats.items():
        ssh(node, f"pkill -f '{pat}' >/dev/null 2>&1 || true", check=False)
    time.sleep(0.4)


def prepare():
    ssh(
        1,
        f"""
set -e
mkdir -p {BASE}
python3 - <<'PY'
from pathlib import Path
import hashlib
path = Path("{BASE}/input.bin")
size = {FILE_MIB} * 1024 * 1024
if not (path.is_file() and path.stat().st_size == size):
    block = bytes((i * 41 + i // 23) & 0xff for i in range(65536))
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
print(path.stat().st_size, h.hexdigest())
PY
""",
        timeout=600,
    )
    for node, dest in (
        (2, "fyp1@10.10.10.162"),
        (3, "fyp1@10.10.10.163"),
        (4, "fyp1@10.10.10.164"),
    ):
        ssh(node, f"mkdir -p {BASE}")
        run(
            [
                "scp",
                "-o",
                "BatchMode=yes",
                f"fyp1@10.10.10.161:{BASE}/input.sha",
                f"{dest}:{BASE}/",
            ]
        )


def codec_flags(codec: str, wh_ack):
    if codec != "wirehair":
        return f"--codec {codec}", f"--codec {codec}"
    ack = "--wh-ack" if wh_ack else "--no-wh-ack"
    common = (
        f"--codec wirehair --wh-segment-mib={WH_SEGMENT} "
        f"--wh-repair-pct={WH_REPAIR} --wh-window={WH_WINDOW} {ack}"
    )
    return common, common


def parse_send(text: str, codec: str) -> dict:
    row: dict = {}
    m = re.search(r"wall_sec=([0-9.]+)", text)
    if m:
        row["wall_sec"] = float(m.group(1))
    m = re.search(
        r"udp-send:.*?source_mbps=([0-9.]+).*?wire_mbps=([0-9.]+)", text
    )
    if m:
        row["actual_source_mbps"] = float(m.group(1))
        row["wire_mbps"] = float(m.group(2))
    m = re.search(
        r"wirehair-send: source_bytes=(\d+) segments=\d+ repair_sent=(\d+) "
        r"wire_bytes=(\d+) ack=(\w+)",
        text,
    )
    if m:
        row["source_bytes"] = int(m.group(1))
        row["repair_sent"] = int(m.group(2))
        row["wire_bytes"] = int(m.group(3))
        row["ack_mode_log"] = m.group(4)
        if row.get("wall_sec"):
            row["goodput_mbps"] = row["source_bytes"] * 8.0 / row["wall_sec"] / 1e6
            row["wire_mbps"] = row["wire_bytes"] * 8.0 / row["wall_sec"] / 1e6
    if row.get("wall_sec") and row.get("actual_source_mbps") and "goodput_mbps" not in row:
        # paced source ≈ goodput when PASS
        row["goodput_mbps"] = row["actual_source_mbps"]
    return row


def verify(sink: int, out_path: str) -> dict:
    ver = ssh(
        sink,
        f"""
set +e
exp=$(cut -d' ' -f1 {BASE}/input.sha)
if [ ! -f '{out_path}' ]; then echo VERIFY=MISSING; echo OUT_BYTES=0; exit 0; fi
got=$(sha256sum '{out_path}' | awk '{{print $1}}')
bytes=$(stat -c%s '{out_path}')
echo OUT_BYTES=$bytes
if [ "$exp" = "$got" ]; then echo VERIFY=OK; else echo VERIFY=FAIL; fi
""",
        check=False,
    )
    row = {"verify": "FAIL", "checksum_ok": False, "out_bytes": 0}
    m = re.search(r"OUT_BYTES=(\d+)", ver.stdout)
    if m:
        row["out_bytes"] = int(m.group(1))
    if "VERIFY=OK" in ver.stdout:
        row["verify"] = "OK"
        row["checksum_ok"] = True
    elif "VERIFY=MISSING" in ver.stdout:
        row["verify"] = "MISSING"
    return row


def one_case(path, codec_mode, rate: int, port: int) -> dict:
    path_name, hops, app_relay, sink, next_hop, relays = path
    label, codec, wh_ack = codec_mode
    cleanup()

    tag = f"{path_name}_{label}_r{rate}"
    out = f"{BASE}/{tag}.out"
    rlog = f"{BASE}/{tag}_recv.log"
    slog = f"{BASE}/{tag}_send.log"
    ack_port = ACK_BASE + (port - PORT_BASE)
    recv_flags, send_flags = codec_flags(codec, wh_ack)

    for n in (1, 2, 3, 4):
        ssh(n, f"mkdir -p {BASE}")
    ssh(sink, f"rm -f {out} {rlog}; : > {rlog}")
    ssh(1, f"rm -f {slog}; : > {slog}")

    # receiver
    ssh(
        sink,
        f"""
nohup {WG} {recv_flags} --local-node-id 4 \
  --udp-recv {port} {out} --idle-sec {IDLE_SEC} --strict \
  >{rlog} 2>&1 &
echo $! > {BASE}/{tag}_recv.pid
""",
    )
    ready_pat = "wirehair-recv: listening" if codec == "wirehair" else "listening"
    # non-wirehair may say different; wait for process + brief sleep
    if codec == "wirehair":
        if not wait_grep(sink, rlog, ready_pat, 400):
            return {
                "path": path_name,
                "hops": hops,
                "app_relay": app_relay,
                "codec_mode": label,
                "codec": codec,
                "wh_ack": wh_ack,
                "rate": rate,
                "verify": "RECV_FAIL",
            }
    else:
        time.sleep(0.8)
        if (
            ssh(sink, f"kill -0 $(cat {BASE}/{tag}_recv.pid)", check=False).returncode
            != 0
        ):
            return {
                "path": path_name,
                "hops": hops,
                "app_relay": app_relay,
                "codec_mode": label,
                "codec": codec,
                "wh_ack": wh_ack,
                "rate": rate,
                "verify": "RECV_FAIL",
                "recv_log": ssh(sink, f"tail -30 {rlog}", check=False).stdout[-1000:],
            }

    # app relays
    if app_relay:
        for i, (rnode, rnext) in enumerate(relays):
            xlog = f"{BASE}/{tag}_relay{rnode}.log"
            ssh(rnode, f"rm -f {xlog}; : > {xlog}")
            ret = f"--return-hop {N1}:{ack_port}" if wh_ack else ""
            # for multi-hop ACK return, each relay needs return toward previous;
            # wire_relay learns from DATA; return-hop is fallback to sender.
            ssh(
                rnode,
                f"""
nohup {RELAY} --local-node-id {rnode} --listen {port} \
  --next-hop {rnext}:{port} {ret} --idle-exit-sec {RELAY_IDLE} \
  >{xlog} 2>&1 &
echo $! > {BASE}/{tag}_relay{rnode}.pid
""",
            )
            if not wait_grep(rnode, xlog, "wire-relay: local_node_id", 300):
                return {
                    "path": path_name,
                    "hops": hops,
                    "app_relay": app_relay,
                    "codec_mode": label,
                    "codec": codec,
                    "wh_ack": wh_ack,
                    "rate": rate,
                    "verify": "RELAY_FAIL",
                    "relay_node": rnode,
                }

    ack_arg = f"--ack-port={ack_port}" if (codec == "wirehair" and wh_ack) else ""
    ttl = max(8, hops + 2)
    send_script = f"""
set -e
/usr/bin/time -f 'wall_sec=%e' -o {BASE}/{tag}_time.txt \
  {WG} {send_flags} {ack_arg} \
  --local-node-id 1 --final-dst 4 --ttl {ttl} --rate-mbps {rate} \
  --udp-send {next_hop} {port} {BASE}/input.bin >{slog} 2>&1
cat {BASE}/{tag}_time.txt
echo ---SEND---
cat {slog}
"""
    try:
        send = ssh(1, send_script, timeout=900)
        send_out = send.stdout
    except SystemExit:
        send_out = ssh(
            1, f"cat {BASE}/{tag}_time.txt {slog} 2>/dev/null || true", check=False
        ).stdout
        row = {
            "path": path_name,
            "hops": hops,
            "app_relay": app_relay,
            "codec_mode": label,
            "codec": codec,
            "wh_ack": wh_ack,
            "rate": rate,
            "verify": "SEND_FAIL",
        }
        row.update(parse_send(send_out, codec))
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
        ssh(sink, f"kill $(cat {BASE}/{tag}_recv.pid) 2>/dev/null || true", check=False)

    if app_relay:
        for rnode, _ in relays:
            ssh(
                rnode,
                f"kill $(cat {BASE}/{tag}_relay{rnode}.pid) 2>/dev/null || true",
                check=False,
            )

    row = {
        "path": path_name,
        "hops": hops,
        "app_relay": app_relay,
        "codec_mode": label,
        "codec": codec,
        "wh_ack": wh_ack,
        "rate": rate,
        "port": port,
        "next_hop": next_hop,
    }
    row.update(parse_send(send_out, codec))
    row.update(verify(sink, out))
    src = FILE_MIB * 1024 * 1024
    row["byte_completion_pct"] = 100.0 * row.get("out_bytes", 0) / src
    if row.get("verify") == "OK" and "goodput_mbps" not in row:
        row["goodput_mbps"] = float(rate)
    cleanup()
    return row


def case_list():
    cases = []
    idx = 0
    for path in PATHS:
        for cm in CODEC_MODES:
            for rate in RATES:
                cases.append({"idx": idx, "path": path, "codec_mode": cm, "rate": rate})
                idx += 1
    return cases


def save(results):
    LOCAL_JSON.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_JSON.write_text(json.dumps(results, indent=2))
    fields = [
        "idx",
        "path",
        "hops",
        "app_relay",
        "codec_mode",
        "codec",
        "wh_ack",
        "rate",
        "verify",
        "checksum_ok",
        "wall_sec",
        "goodput_mbps",
        "actual_source_mbps",
        "wire_mbps",
        "repair_sent",
        "byte_completion_pct",
    ]
    with LOCAL_CSV.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in results:
            w.writerow(r)


def main():
    cases = case_list()
    print(
        f"cases={len(cases)} file_mib={FILE_MIB} rates={RATES} smoke={SMOKE}",
        flush=True,
    )
    print(f"json={LOCAL_JSON}", flush=True)

    results = []
    if LOCAL_JSON.is_file() and not FRESH:
        try:
            results = json.loads(LOCAL_JSON.read_text())
            print(f"resume {len(results)} rows", flush=True)
        except json.JSONDecodeError:
            results = []

    cleanup()
    prepare()
    done = max(len(results), SKIP)

    for c in cases:
        if c["idx"] < done:
            continue
        path = c["path"]
        cm = c["codec_mode"]
        rate = c["rate"]
        port = PORT_BASE + (c["idx"] % 90)
        print(
            f"\n===== [{c['idx']+1}/{len(cases)}] {path[0]} {cm[0]} {rate}m =====",
            flush=True,
        )
        try:
            row = one_case(path, cm, rate, port)
        except Exception as exc:  # noqa: BLE001
            cleanup()
            row = {
                "path": path[0],
                "hops": path[1],
                "app_relay": path[2],
                "codec_mode": cm[0],
                "codec": cm[1],
                "wh_ack": cm[2],
                "rate": rate,
                "verify": "EXCEPTION",
                "error": str(exc),
            }
        row["idx"] = c["idx"]
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
                    "wire_mbps",
                    "byte_completion_pct",
                    "repair_sent",
                )
            },
            flush=True,
        )

    cleanup()
    save(results)
    ok = sum(1 for r in results if r.get("verify") == "OK")
    print(f"\nDONE ok={ok}/{len(results)} -> {LOCAL_JSON}", flush=True)


if __name__ == "__main__":
    main()
