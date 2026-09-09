#!/usr/bin/env python3
"""Phase 4A: multi-flow saturation on hop3 kernel vs app-relay.

Per-flow rates are derived from Phase-3 hop3 ceilings (VM4 buffers tuned).
Finds max per-flow --rate-mbps where ALL flows checksum-PASS (precision 50).

Env:
  WH_P4_SMOKE=1
  WH_P4_FRESH=1
  WH_P4_NAME=...
  WH_P4_MIB=64          # payload MiB per flow (shared file)
  WH_P4_PATHS=kernel,relay
  WH_P4_CODECS=copy,wirehair_ack,...
  WH_P4_FLOWS=2,4,8
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
from topo_lab import N1, N2, N3, N4, RELAY2_NEXT, RELAY3_NEXT  # noqa: E402

SSH = {
    1: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.161"],
    2: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.162"],
    3: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.163"],
    4: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.164"],
}

WG = "/home/fyp1/work/multi-flow-rate-control/build/wg_multi_pipeline"
RELAY = "/home/fyp1/work/multi-flow-rate-control/build/wire_relay"
BASE = "/tmp/wire_p4_multiflow"
PORT_BASE = 28100
ACK_BASE = 28200

# Phase-3 hop3 ceilings after VM4 buffer retune (goodput Mbps)
CEIL = {
    "kernel": {
        "copy": 1150,
        "block": 1289,
        "xor-fec": 992,
        "rs-fec": 881,
        "rs": 882,
        "wirehair_noack": 1712,
        "wirehair_ack": 1069,
    },
    "relay": {
        "copy": 800,
        "block": 650,
        "xor-fec": 450,
        "rs-fec": 350,
        "rs": 400,
        "wirehair_noack": 746,
        "wirehair_ack": 831,
    },
}

WH_SEGMENT, WH_WINDOW, WH_REPAIR = 2, 8, 10
PRECISION = 50
FILE_MIB = int(os.environ.get("WH_P4_MIB", "64"))
IDLE_SEC = 25
RELAY_IDLE = 50
POST_SEND_WAIT_S = 90

PATHS = [x.strip() for x in os.environ.get("WH_P4_PATHS", "kernel,relay").split(",") if x.strip()]
FLOWS_LIST = [int(x) for x in os.environ.get("WH_P4_FLOWS", "2,4,8").split(",") if x.strip()]
CODEC_MODES = [
    ("copy", "copy", None),
    ("block", "block", None),
    ("xor-fec", "xor-fec", None),
    ("rs-fec", "rs-fec", None),
    ("rs", "rs", None),
    ("wirehair_noack", "wirehair", False),
    ("wirehair_ack", "wirehair", True),
]
if os.environ.get("WH_P4_CODECS"):
    want = {x.strip() for x in os.environ["WH_P4_CODECS"].split(",")}
    CODEC_MODES = [c for c in CODEC_MODES if c[0] in want]

SMOKE = os.environ.get("WH_P4_SMOKE", "0") == "1"
FRESH = os.environ.get("WH_P4_FRESH", "0") == "1"
NAME = os.environ.get("WH_P4_NAME", "phase4_multiflow_sat")
if re.fullmatch(r"[A-Za-z0-9_.-]+", NAME) is None:
    raise ValueError("bad name")

if SMOKE:
    PATHS = ["kernel", "relay"]
    FLOWS_LIST = [2, 4]
    CODEC_MODES = [("copy", "copy", None), ("wirehair_ack", "wirehair", True)]
    FILE_MIB = 32
    NAME = NAME + "_smoke"

REPO = Path(__file__).resolve().parents[2]
LOCAL_JSON = REPO / "build" / f"{NAME}.json"
LOCAL_CSV = REPO / "build" / f"{NAME}.csv"
PROBES_JSON = REPO / "build" / f"{NAME}_probes.json"


def run(cmd, check=True, timeout=1200):
    r = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if check and r.returncode != 0:
        print("FAIL:", cmd, file=sys.stderr)
        print((r.stdout or "")[-2000:], file=sys.stderr)
        print((r.stderr or "")[-2000:], file=sys.stderr)
        raise SystemExit(r.returncode or 1)
    return r


def ssh(node, script, check=True, timeout=1200):
    return run(SSH[node] + [script], check=check, timeout=timeout)


def wait_grep(node, log, pat, n=500):
    for _ in range(n):
        q = pat.replace("'", "'\\''")
        if ssh(node, f"grep -q '{q}' {log}", check=False).returncode == 0:
            return True
        time.sleep(0.1)
    return False


def cleanup():
    for node, pat in (
        (1, "wg_multi_pipeline.*(281|282)"),
        (2, "wire_relay.*281|wg_multi_pipeline.*(281|282)"),
        (3, "wire_relay.*281|wg_multi_pipeline.*(281|282)"),
        (4, "wg_multi_pipeline.*(281|282)"),
    ):
        ssh(node, f"pkill -f '{pat}' >/dev/null 2>&1 || true", check=False)
    time.sleep(0.4)


def round50(x: float) -> int:
    return max(PRECISION, int(PRECISION * round(x / PRECISION)))


def rate_ladder(ceiling: float, flows: int) -> list[int]:
    base = ceiling / flows
    raw = [0.5 * base, 0.75 * base, 1.0 * base, 1.25 * base]
    out = []
    for v in raw:
        r = round50(v)
        if r not in out:
            out.append(r)
    return out


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
    block = bytes((i * 47 + i // 31) & 0xff for i in range(65536))
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


def parse_send(text: str) -> dict:
    row: dict = {}
    m = re.search(r"wall_sec=([0-9.]+)", text)
    if m:
        row["wall_sec"] = float(m.group(1))
    # aggregate wirehair lines if multi
    sources = [int(x) for x in re.findall(r"wirehair-send: source_bytes=(\d+)", text)]
    wires = [int(x) for x in re.findall(r"wirehair-send:.*?wire_bytes=(\d+)", text)]
    repairs = [int(x) for x in re.findall(r"repair_sent=(\d+)", text)]
    if sources and row.get("wall_sec"):
        row["source_bytes"] = sum(sources)
        row["wire_bytes"] = sum(wires) if wires else None
        row["repair_sent"] = sum(repairs) if repairs else 0
        row["agg_goodput_mbps"] = row["source_bytes"] * 8.0 / row["wall_sec"] / 1e6
        if row.get("wire_bytes"):
            row["agg_wire_mbps"] = row["wire_bytes"] * 8.0 / row["wall_sec"] / 1e6
    gps = [float(x) for x in re.findall(r"source_mbps=([0-9.]+)", text)]
    wms = [float(x) for x in re.findall(r"wire_mbps=([0-9.]+)", text)]
    if gps:
        row["udp_source_mbps_sum"] = sum(gps)
        row["agg_goodput_mbps"] = row.get("agg_goodput_mbps") or sum(gps)
    if wms:
        row["agg_wire_mbps"] = row.get("agg_wire_mbps") or sum(wms)
    return row


def verify_flows(sink: int, prefix: str, flows: int) -> dict:
    ver = ssh(
        sink,
        f"""
set +e
exp=$(cut -d' ' -f1 {BASE}/input.sha)
ok=0; fail=0
for i in $(seq 0 $(({flows}-1))); do
  cand=$(ls -1 {prefix}*flow_$i* 2>/dev/null | head -1)
  if [ -z "$cand" ] || [ ! -f "$cand" ]; then
    echo FLOW_$i=MISSING; fail=$((fail+1)); continue
  fi
  got=$(sha256sum "$cand" | awk '{{print $1}}')
  bytes=$(stat -c%s "$cand")
  if [ "$exp" = "$got" ]; then echo FLOW_$i=OK bytes=$bytes; ok=$((ok+1))
  else echo FLOW_$i=FAIL bytes=$bytes; fail=$((fail+1)); fi
done
echo OK_COUNT=$ok FAIL_COUNT=$fail
""",
        check=False,
    )
    m = re.search(r"OK_COUNT=(\d+).*FAIL_COUNT=(\d+)", ver.stdout, re.S)
    ok = int(m.group(1)) if m else 0
    fail = int(m.group(2)) if m else flows
    return {
        "flows_ok": ok,
        "flows_fail": fail,
        "verify": "OK" if ok == flows and fail == 0 else "FAIL",
        "verify_raw": ver.stdout[-1500:],
    }


def one_probe(path: str, codec_mode, flows: int, rate: int, port: int) -> dict:
    label, codec, wh_ack = codec_mode
    app_relay = path == "relay"
    next_hop = N2 if app_relay else N4
    sink = 4
    cleanup()
    tag = f"{path}_{label}_f{flows}_r{rate}_p{port}"
    prefix = f"{BASE}/{tag}_out_"
    rlog = f"{BASE}/{tag}_recv.log"
    slog = f"{BASE}/{tag}_send.log"
    ack_port = ACK_BASE + (port - PORT_BASE)
    recv_flags, send_flags = codec_flags(codec, wh_ack)

    for n in (1, 2, 3, 4):
        ssh(n, f"mkdir -p {BASE}")
    ssh(sink, f"rm -f {prefix}* {rlog}; : > {rlog}")
    ssh(1, f"rm -f {slog}; : > {slog}")

    ssh(
        sink,
        f"""
nohup {WG} {recv_flags} --local-node-id 4 \
  --udp-recv {port} {prefix} --max-flows {flows} --idle-sec {IDLE_SEC} --strict \
  >{rlog} 2>&1 &
echo $! > {BASE}/{tag}_recv.pid
""",
    )
    if codec == "wirehair":
        if not wait_grep(sink, rlog, "wirehair-recv: listening", 500):
            return {"verify": "RECV_FAIL", "rate": rate, "flows": flows}
    else:
        time.sleep(0.8)

    if app_relay:
        for rnode, rnext in ((2, RELAY2_NEXT), (3, RELAY3_NEXT)):
            xlog = f"{BASE}/{tag}_relay{rnode}.log"
            ssh(rnode, f"rm -f {xlog}; : > {xlog}")
            ret = f"--return-hop {N1}:{ack_port}" if wh_ack else ""
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
                return {"verify": "RELAY_FAIL", "rate": rate, "relay_node": rnode}

    ack_arg = f"--ack-port={ack_port}" if (codec == "wirehair" and wh_ack) else ""
    flow_args = " ".join(
        f'--flow "{i}:{next_hop}:{port}:{BASE}/input.bin:{rate}"' for i in range(flows)
    )
    send_script = f"""
set -e
/usr/bin/time -f 'wall_sec=%e' -o {BASE}/{tag}_time.txt \
  {WG} {send_flags} {ack_arg} \
  --local-node-id 1 --final-dst 4 --ttl 8 \
  --udp-send-multi {flow_args} >{slog} 2>&1
cat {BASE}/{tag}_time.txt
echo ---SEND---
cat {slog}
"""
    try:
        send_out = ssh(1, send_script, timeout=900).stdout
    except SystemExit:
        send_out = ssh(
            1, f"cat {BASE}/{tag}_time.txt {slog} 2>/dev/null || true", check=False
        ).stdout
        row = {"verify": "SEND_FAIL", "rate": rate, "flows": flows}
        row.update(parse_send(send_out))
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
        time.sleep(0.25)
    else:
        ssh(sink, f"kill $(cat {BASE}/{tag}_recv.pid) 2>/dev/null || true", check=False)

    if app_relay:
        for rnode in (2, 3):
            ssh(
                rnode,
                f"kill $(cat {BASE}/{tag}_relay{rnode}.pid) 2>/dev/null || true",
                check=False,
            )

    row = {"rate": rate, "flows": flows, "port": port}
    row.update(parse_send(send_out))
    row.update(verify_flows(sink, prefix, flows))
    # offered aggregate
    row["offered_agg_mbps"] = rate * flows
    if row.get("agg_goodput_mbps") is None and row.get("verify") == "OK" and row.get("wall_sec"):
        row["agg_goodput_mbps"] = FILE_MIB * flows * 1024 * 1024 * 8 / row["wall_sec"] / 1e6
    cleanup()
    ssh(sink, f"rm -f {prefix}*", check=False)
    return row


def search_config(path: str, codec_mode, flows: int, port_base: int, probes: list) -> dict:
    label = codec_mode[0]
    ceiling = CEIL[path][label]
    ladder = rate_ladder(ceiling, flows)
    print(
        f"\n##### {path} {label} flows={flows} ceil={ceiling} ladder={ladder} #####",
        flush=True,
    )
    last_pass = None
    first_fail = None
    port = port_base
    for rate in ladder:
        port += 1
        print(f"  probe rate={rate} ...", flush=True)
        r = one_probe(path, codec_mode, flows, rate, port)
        r.update(
            {
                "path": path,
                "codec_mode": label,
                "codec": codec_mode[1],
                "wh_ack": codec_mode[2],
                "phase3_ceil": ceiling,
                "phase": "ladder",
            }
        )
        probes.append(r)
        print(
            f"  -> {r.get('verify')} flows_ok={r.get('flows_ok')} "
            f"agg_goodput={r.get('agg_goodput_mbps')} offered={r.get('offered_agg_mbps')}",
            flush=True,
        )
        if r.get("verify") == "OK":
            last_pass = r
        else:
            first_fail = r
            break

    # refine between last_pass and first_fail if both exist
    if last_pass and first_fail and first_fail["rate"] - last_pass["rate"] > PRECISION:
        lo, hi = last_pass["rate"], first_fail["rate"]
        while hi - lo > PRECISION:
            mid = round50((lo + hi) / 2)
            if mid <= lo:
                mid = lo + PRECISION
            if mid >= hi:
                mid = hi - PRECISION
            if mid <= lo or mid >= hi:
                break
            port += 1
            print(f"  refine {mid} (lo={lo} hi={hi}) ...", flush=True)
            r = one_probe(path, codec_mode, flows, mid, port)
            r.update(
                {
                    "path": path,
                    "codec_mode": label,
                    "codec": codec_mode[1],
                    "wh_ack": codec_mode[2],
                    "phase3_ceil": ceiling,
                    "phase": "refine",
                }
            )
            probes.append(r)
            print(
                f"  -> {r.get('verify')} agg_goodput={r.get('agg_goodput_mbps')}",
                flush=True,
            )
            if r.get("verify") == "OK":
                last_pass = r
                lo = mid
            else:
                first_fail = r
                hi = mid

    # if first ladder point failed, try lower
    if last_pass is None and first_fail is not None:
        lo, hi = 0, first_fail["rate"]
        while hi - lo > PRECISION:
            mid = round50((lo + hi) / 2)
            if mid <= lo:
                mid = lo + PRECISION
            if mid >= hi:
                mid = hi - PRECISION
            if mid <= 0 or mid >= hi:
                break
            port += 1
            print(f"  low-refine {mid} ...", flush=True)
            r = one_probe(path, codec_mode, flows, mid, port)
            r.update(
                {
                    "path": path,
                    "codec_mode": label,
                    "codec": codec_mode[1],
                    "wh_ack": codec_mode[2],
                    "phase3_ceil": ceiling,
                    "phase": "low_refine",
                }
            )
            probes.append(r)
            print(f"  -> {r.get('verify')} agg_goodput={r.get('agg_goodput_mbps')}", flush=True)
            if r.get("verify") == "OK":
                last_pass = r
                lo = mid
            else:
                first_fail = r
                hi = mid

    return {
        "path": path,
        "codec_mode": label,
        "codec": codec_mode[1],
        "wh_ack": codec_mode[2],
        "flows": flows,
        "phase3_ceil_mbps": ceiling,
        "max_pass_per_flow_mbps": None if not last_pass else last_pass["rate"],
        "max_pass_offered_agg_mbps": None
        if not last_pass
        else last_pass["rate"] * flows,
        "max_pass_agg_goodput_mbps": None
        if not last_pass
        else last_pass.get("agg_goodput_mbps"),
        "first_fail_per_flow_mbps": None if not first_fail else first_fail["rate"],
        "efficiency_vs_ceil": None
        if not last_pass
        else (last_pass["rate"] * flows) / ceiling,
        "precision_mbps": PRECISION,
    }


def save(results, probes):
    LOCAL_JSON.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_JSON.write_text(json.dumps(results, indent=2))
    PROBES_JSON.write_text(json.dumps(probes, indent=2))
    fields = [
        "path",
        "codec_mode",
        "flows",
        "phase3_ceil_mbps",
        "max_pass_per_flow_mbps",
        "max_pass_offered_agg_mbps",
        "max_pass_agg_goodput_mbps",
        "first_fail_per_flow_mbps",
        "efficiency_vs_ceil",
        "precision_mbps",
    ]
    with LOCAL_CSV.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in results:
            w.writerow(r)


def main():
    configs = [(p, c, f) for p in PATHS for c in CODEC_MODES for f in FLOWS_LIST]
    print(
        f"configs={len(configs)} paths={PATHS} flows={FLOWS_LIST} "
        f"codecs={[c[0] for c in CODEC_MODES]} mib={FILE_MIB} smoke={SMOKE}",
        flush=True,
    )
    results, probes = [], []
    if LOCAL_JSON.is_file() and not FRESH:
        try:
            results = json.loads(LOCAL_JSON.read_text())
            if PROBES_JSON.is_file():
                probes = json.loads(PROBES_JSON.read_text())
            print(f"resume {len(results)}", flush=True)
        except json.JSONDecodeError:
            results, probes = [], []

    cleanup()
    prepare()
    done = len(results)
    for i, (path, cm, flows) in enumerate(configs):
        if i < done:
            continue
        row = search_config(path, cm, flows, PORT_BASE + i * 30, probes)
        results.append(row)
        save(results, probes)
        print("SAT", row, flush=True)
    cleanup()
    save(results, probes)
    print(f"\nDONE -> {LOCAL_JSON}", flush=True)


if __name__ == "__main__":
    main()
