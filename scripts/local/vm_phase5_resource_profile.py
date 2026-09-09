#!/usr/bin/env python3
"""Phase 5: resource profile (host CPU/NIC + process CPU/RSS).

Parts:
  A — single-flow hop3 kernel/relay × codecs × ~50%/100% of phase3 ceiling
  B — multi-flow scaling 2/4/8 at phase4A PASS per-flow rates
  C — re-run 4B batch1 stress with host monitors (proc summary from stress logs)

Env:
  WH_P5_PARTS=A,B,C
  WH_P5_SMOKE=1
  WH_P5_FRESH=1
  WH_P5_NAME=phase5_resource
  WH_P5_MIB=64
"""

from __future__ import annotations

import csv
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parent))
from topo_lab import N1, N2, N3, N4, RELAY2_NEXT, RELAY3_NEXT, MONITOR_IFACES, TOPO  # noqa: E402

SSH = {
    1: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.161"],
    2: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.162"],
    3: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.163"],
    4: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.164"],
}
HOST = {
    1: "fyp1@10.10.10.161",
    2: "fyp1@10.10.10.162",
    3: "fyp1@10.10.10.163",
    4: "fyp1@10.10.10.164",
}

WG = "/home/fyp1/work/multi-flow-rate-control/build/wg_multi_pipeline"
RELAY = "/home/fyp1/work/multi-flow-rate-control/build/wire_relay"
BASE = "/tmp/wire_p5_resource"
PORT_BASE = 29100
ACK_BASE = 29200

IFACES = MONITOR_IFACES

# Phase-3 hop3 ceilings after VM4 buffer retune (goodput Mbps)
CEIL = {
    "kernel": {
        "copy": 1150,
        "rs": 882,
        "wirehair_noack": 1712,
        "wirehair_ack": 1069,
    },
    "relay": {
        "copy": 800,
        "rs": 400,
        "wirehair_noack": 746,
        "wirehair_ack": 831,
    },
}

# Phase-4A max PASS per-flow rates
P4_PASS = {
    "kernel": {
        "copy": {2: 700, 4: 150, 8: 50},
        "rs": {2: 550, 4: 150, 8: 50},
        "wirehair_noack": {2: 1050, 4: 550, 8: 250},
        "wirehair_ack": {2: 650, 4: 350, 8: 150},
    },
    "relay": {
        "copy": {2: 500, 4: 150, 8: 50},
        "rs": {2: 200, 4: 50, 8: None},
        "wirehair_noack": {2: 400, 4: 150, 8: 50},
        "wirehair_ack": {2: 500, 4: 250, 8: 100},
    },
}

WH_SEGMENT, WH_WINDOW, WH_REPAIR = 2, 8, 10
FILE_MIB = int(os.environ.get("WH_P5_MIB", "64"))
IDLE_SEC = 25
RELAY_IDLE = 55
SAMPLE_HZ = float(os.environ.get("WH_P5_HZ", "0.25"))
POST_SEND_WAIT_S = 100

PARTS = [x.strip().upper() for x in os.environ.get("WH_P5_PARTS", "A,B,C").split(",") if x.strip()]
SMOKE = os.environ.get("WH_P5_SMOKE", "0") == "1"
FRESH = os.environ.get("WH_P5_FRESH", "0") == "1"
NAME = os.environ.get("WH_P5_NAME", "phase5_resource")
if re.fullmatch(r"[A-Za-z0-9_.-]+", NAME) is None:
    raise ValueError("bad name")

CODEC_MODES = [
    ("copy", "copy", None),
    ("rs", "rs", None),
    ("wirehair_noack", "wirehair", False),
    ("wirehair_ack", "wirehair", True),
]
PATHS = ["kernel", "relay"]
FLOWS_LIST = [2, 4, 8]
LOAD_FRACS = [0.5, 1.0]

if SMOKE:
    NAME = NAME + "_smoke"
    CODEC_MODES = [("copy", "copy", None), ("wirehair_ack", "wirehair", True)]
    PATHS = ["kernel", "relay"]
    FLOWS_LIST = [2, 4]
    LOAD_FRACS = [0.5]
    FILE_MIB = min(FILE_MIB, 32)

REPO = Path(__file__).resolve().parents[2]
OUT_DIR = REPO / "build" / NAME
LOCAL_JSON = OUT_DIR / f"{NAME}.json"
LOCAL_CSV = OUT_DIR / f"{NAME}.csv"
MON_PY = REPO / "scripts" / "proc_resource_monitor.py"
HOST_MON_PY = REPO / "scripts" / "iperf_like_monitor.py"


def run(cmd, check=True, timeout=1800):
    r = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if check and r.returncode != 0:
        print("FAIL:", cmd, file=sys.stderr)
        print((r.stdout or "")[-2000:], file=sys.stderr)
        print((r.stderr or "")[-2000:], file=sys.stderr)
        raise SystemExit(r.returncode or 1)
    return r


def ssh(node, script, check=True, timeout=1800):
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
        (1, "wg_multi_pipeline.*(291|292)|iperf_like_monitor|proc_resource_monitor"),
        (2, "wire_relay.*291|wg_multi_pipeline.*(291|292)|iperf_like_monitor|proc_resource_monitor"),
        (3, "wire_relay.*291|wg_multi_pipeline.*(291|292)|iperf_like_monitor|proc_resource_monitor"),
        (4, "wg_multi_pipeline.*(291|292)|iperf_like_monitor|proc_resource_monitor"),
    ):
        ssh(node, f"pkill -f '{pat}' >/dev/null 2>&1 || true", check=False)
    time.sleep(0.5)


def round50(x: float) -> int:
    return max(50, int(50 * round(x / 50)))


def codec_flags(codec: str, wh_ack):
    if codec != "wirehair":
        return f"--codec {codec}", f"--codec {codec}"
    ack = "--wh-ack" if wh_ack else "--no-wh-ack"
    common = (
        f"--codec wirehair --wh-segment-mib={WH_SEGMENT} "
        f"--wh-repair-pct={WH_REPAIR} --wh-window={WH_WINDOW} {ack}"
    )
    return common, common


def prepare_payload():
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
    for node, dest in ((2, HOST[2]), (3, HOST[3]), (4, HOST[4])):
        ssh(node, f"mkdir -p {BASE}")
        run(["scp", "-o", "BatchMode=yes", f"{HOST[1]}:{BASE}/input.sha", f"{dest}:{BASE}/"])


def stage_monitors():
    for node in (1, 2, 3, 4):
        run(["scp", "-o", "BatchMode=yes", str(MON_PY), f"{HOST[node]}:/tmp/proc_resource_monitor.py"])
        run(["scp", "-o", "BatchMode=yes", str(HOST_MON_PY), f"{HOST[node]}:/tmp/iperf_like_monitor.py"])


def summarize_proc_csv(text: str) -> dict:
    cpus, rss = [], []
    for line in text.strip().splitlines()[1:]:
        parts = line.split(",")
        if len(parts) < 3:
            continue
        try:
            cpus.append(float(parts[1]))
        except ValueError:
            pass
        try:
            if parts[2]:
                rss.append(float(parts[2]))
        except ValueError:
            pass
    if not cpus:
        return {"avg_cpu": None, "peak_cpu": None, "peak_rss_kb": None, "samples": 0}
    return {
        "avg_cpu": round(sum(cpus) / len(cpus), 2),
        "peak_cpu": round(max(cpus), 2),
        "peak_rss_kb": int(max(rss)) if rss else None,
        "samples": len(cpus),
    }


def summarize_host_csv(text: str) -> dict:
    """Aggregate host CPU from __cpu__ / any rows' cpu_pct; NIC peak max(rx,tx)."""
    cpus = []
    nic_mbps = []
    for line in text.strip().splitlines()[1:]:
        parts = line.split(",")
        if len(parts) < 9:
            continue
        iface, _ts, rx_bps, tx_bps = parts[0], parts[1], parts[2], parts[3]
        cpu = parts[8]
        try:
            cpus.append(float(cpu))
        except ValueError:
            pass
        if iface != "__cpu__":
            try:
                mbps = max(float(rx_bps), float(tx_bps)) * 8.0 / 1e6
                nic_mbps.append(mbps)
            except ValueError:
                pass
    out = {
        "host_avg_cpu": round(sum(cpus) / len(cpus), 2) if cpus else None,
        "host_peak_cpu": round(max(cpus), 2) if cpus else None,
        "nic_peak_mbps": round(max(nic_mbps), 1) if nic_mbps else None,
        "nic_avg_mbps": round(sum(nic_mbps) / len(nic_mbps), 1) if nic_mbps else None,
    }
    return out


def fetch_text(node: int, path: str) -> str:
    r = ssh(node, f"test -f {path} && cat {path} || true", check=False)
    return r.stdout or ""


def verify_flows(sink: int, prefix: str, flows: int) -> dict:
    ver = ssh(
        sink,
        f"""
set +e
exp=$(cut -d' ' -f1 {BASE}/input.sha)
ok=0; fail=0
for i in $(seq 0 $(({flows}-1))); do
  cand=$(ls -1 {prefix}*flow_$i* 2>/dev/null | head -1)
  if [ -z "$cand" ] && [ "{flows}" = "1" ]; then
    if [ -f "{prefix}" ]; then cand="{prefix}"
    else cand=$(ls -1 {prefix}* 2>/dev/null | head -1); fi
  fi
  if [ -z "$cand" ] || [ ! -f "$cand" ]; then
    echo FLOW_$i=MISSING; fail=$((fail+1)); continue
  fi
  got=$(sha256sum "$cand" | awk '{{print $1}}')
  bytes=$(stat -c%s "$cand")
  if [ "$exp" = "$got" ]; then echo FLOW_$i=OK bytes=$bytes file=$cand; ok=$((ok+1))
  else echo FLOW_$i=FAIL bytes=$bytes file=$cand; fail=$((fail+1)); fi
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
        "verify_raw": (ver.stdout or "")[-800:],
    }


def parse_send(text: str) -> dict:
    row: dict = {}
    m = re.search(r"wall_sec=([0-9.]+)", text)
    if m:
        row["wall_sec"] = float(m.group(1))
    sources = [int(x) for x in re.findall(r"wirehair-send: source_bytes=(\d+)", text)]
    sources += [int(x) for x in re.findall(r"wire-multi-send: flow_id=\d+.*?source_bytes=(\d+)", text)]
    sources += [int(x) for x in re.findall(r"source_bytes=(\d+)", text)]
    # dedupe while preserving order
    seen = set()
    uniq = []
    for s in sources:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    if uniq and row.get("wall_sec"):
        row["source_bytes"] = sum(uniq) if len(uniq) > 1 else uniq[0]
        # if multiple identical captures from overlapping regexes, prefer sum of flow lines
        flow_srcs = [int(x) for x in re.findall(r"wire-multi-send: flow_id=\d+[^\n]*source_bytes=(\d+)", text)]
        if flow_srcs:
            row["source_bytes"] = sum(flow_srcs)
        row["agg_goodput_mbps"] = row["source_bytes"] * 8.0 / row["wall_sec"] / 1e6
    wh_sources = [int(x) for x in re.findall(r"wirehair-send: source_bytes=(\d+)", text)]
    if wh_sources and row.get("wall_sec"):
        row["source_bytes"] = sum(wh_sources)
        row["agg_goodput_mbps"] = row["source_bytes"] * 8.0 / row["wall_sec"] / 1e6
    gps = [float(x) for x in re.findall(r"source_mbps=([0-9.]+)", text)]
    if gps:
        row["agg_goodput_mbps"] = row.get("agg_goodput_mbps") or sum(gps)
    return row


def one_case(part: str, path: str, codec_mode, flows: int, rate: int, port: int) -> dict:
    label, codec, wh_ack = codec_mode
    app_relay = path == "relay"
    next_hop = N2 if app_relay else N4
    sink = 4
    cleanup()
    tag = f"{part}_{path}_{label}_f{flows}_r{rate}_p{port}"
    case_dir = f"{BASE}/{tag}"
    prefix = f"{case_dir}/out_"
    rlog = f"{case_dir}/recv.log"
    slog = f"{case_dir}/send.log"
    ack_port = ACK_BASE + (port - PORT_BASE)
    recv_flags, send_flags = codec_flags(codec, wh_ack)

    print(f"\n===== {tag} =====", flush=True)
    for n in (1, 2, 3, 4):
        ssh(n, f"mkdir -p {case_dir}/mon; rm -f {case_dir}/mon/* {prefix}* {rlog} {slog} 2>/dev/null; : > {rlog}")

    # start host monitors on involved nodes
    involved = [1, 4] + ([2, 3] if app_relay else [])
    for n in involved:
        ifaces = IFACES[n]
        ssh(
            n,
            f"""
set +e
mkdir -p {case_dir}/mon
pkill -f 'iperf_like_monitor.py.*{tag}' >/dev/null 2>&1
setsid python3 /tmp/iperf_like_monitor.py {ifaces} {SAMPLE_HZ} {case_dir}/mon/host_n{n}.csv \
  >{case_dir}/mon/host_n{n}.log 2>&1 < /dev/null &
echo $! > {case_dir}/mon/host_n{n}.pid
exit 0
""",
            check=False,
        )

    # receiver
    ssh(
        sink,
        f"""
set +e
setsid {WG} {recv_flags} --local-node-id 4 \
  --udp-recv {port} {prefix} --max-flows {flows} --idle-sec {IDLE_SEC} --strict \
  >{rlog} 2>&1 < /dev/null &
echo $! > {case_dir}/recv.pid
exit 0
""",
        check=False,
    )
    if codec == "wirehair":
        if not wait_grep(sink, rlog, "wirehair-recv: listening", 500):
            return {"part": part, "tag": tag, "verify": "RECV_FAIL"}
    else:
        time.sleep(0.8)
    recv_pid = ssh(sink, f"cat {case_dir}/recv.pid").stdout.strip()
    ssh(
        sink,
        f"""
nohup python3 /tmp/proc_resource_monitor.py {recv_pid} {SAMPLE_HZ} {case_dir}/mon/proc_recv.csv \
  {case_dir}/mon/STOP >{case_dir}/mon/proc_recv.log 2>&1 &
echo $! > {case_dir}/mon/proc_recv.pid
""",
    )

    relay_pids = {}
    if app_relay:
        for rnode, rnext in ((2, RELAY2_NEXT), (3, RELAY3_NEXT)):
            xlog = f"{case_dir}/relay{rnode}.log"
            ret = f"--return-hop {N1}:{ack_port}" if wh_ack else ""
            ssh(
                rnode,
                f"""
set +e
setsid {RELAY} --local-node-id {rnode} --listen {port} \
  --next-hop {rnext}:{port} {ret} --idle-exit-sec {RELAY_IDLE} \
  >{xlog} 2>&1 < /dev/null &
echo $! > {case_dir}/relay{rnode}.pid
exit 0
""",
                check=False,
            )
            if not wait_grep(rnode, xlog, "wire-relay: local_node_id", 300):
                return {"part": part, "tag": tag, "verify": "RELAY_FAIL", "relay_node": rnode}
            pid = ssh(rnode, f"cat {case_dir}/relay{rnode}.pid").stdout.strip()
            relay_pids[rnode] = pid
            ssh(
                rnode,
                f"""
nohup python3 /tmp/proc_resource_monitor.py {pid} {SAMPLE_HZ} {case_dir}/mon/proc_relay{rnode}.csv \
  {case_dir}/mon/STOP >{case_dir}/mon/proc_relay{rnode}.log 2>&1 &
""",
            )

    ack_arg = f"--ack-port={ack_port}" if (codec == "wirehair" and wh_ack) else ""
    flow_args = " ".join(
        f'--flow "{i}:{next_hop}:{port}:{BASE}/input.bin:{rate}"' for i in range(flows)
    )
    # start sender; attach proc monitor via pgrep
    ssh(
        1,
        f"""
set +e
rm -f {case_dir}/send.done {case_dir}/send.pid
(
  /usr/bin/time -f 'wall_sec=%e' -o {case_dir}/time.txt \
    {WG} {send_flags} {ack_arg} \
    --local-node-id 1 --final-dst 4 --ttl 8 \
    --udp-send-multi {flow_args} >{slog} 2>&1
  echo DONE > {case_dir}/send.done
) &
echo $! > {case_dir}/send_wrapper.pid
for i in $(seq 1 80); do
  pid=$(pgrep -f 'wg_multi_pipeline.*--udp-send-multi' | head -1)
  if [ -n "$pid" ]; then
    echo $pid > {case_dir}/send.pid
    setsid python3 /tmp/proc_resource_monitor.py $pid {SAMPLE_HZ} \
      {case_dir}/mon/proc_send.csv {case_dir}/mon/STOP \
      >{case_dir}/mon/proc_send.log 2>&1 < /dev/null &
    break
  fi
  sleep 0.05
done
exit 0
""",
        check=False,
    )

    # wait for send done
    deadline = time.time() + POST_SEND_WAIT_S + max(30, int(FILE_MIB * 8 * flows / max(rate, 1) * 1.5))
    while time.time() < deadline:
        if ssh(1, f"test -f {case_dir}/send.done", check=False).returncode == 0:
            break
        time.sleep(1)
    else:
        print("  warn: send timeout", flush=True)

    # allow recv idle flush
    time.sleep(min(IDLE_SEC + 5, 35))

    # stop monitors
    for n in involved:
        ssh(n, f"touch {case_dir}/mon/STOP; sleep 0.3; pkill -f 'iperf_like_monitor.py.*{case_dir}' >/dev/null 2>&1 || true", check=False)
        # STOP is local to each node for proc monitors on that node
        ssh(n, f"touch {case_dir}/mon/STOP", check=False)
    time.sleep(0.8)

    send_txt = fetch_text(1, slog) + "\n" + fetch_text(1, f"{case_dir}/time.txt")
    send_stats = parse_send(send_txt)
    ver = verify_flows(sink, prefix, flows)

    resources = {
        "send": summarize_proc_csv(fetch_text(1, f"{case_dir}/mon/proc_send.csv")),
        "recv": summarize_proc_csv(fetch_text(sink, f"{case_dir}/mon/proc_recv.csv")),
        "host": {},
    }
    for n in involved:
        resources["host"][f"n{n}"] = summarize_host_csv(fetch_text(n, f"{case_dir}/mon/host_n{n}.csv"))
    if app_relay:
        for rnode in (2, 3):
            resources[f"relay{rnode}"] = summarize_proc_csv(
                fetch_text(rnode, f"{case_dir}/mon/proc_relay{rnode}.csv")
            )

    # pull monitor artifacts to host
    local_case = OUT_DIR / "cases" / tag
    local_case.mkdir(parents=True, exist_ok=True)
    for n in involved:
        run(
            ["scp", "-o", "BatchMode=yes", "-r", f"{HOST[n]}:{case_dir}/mon", f"{local_case}/mon_n{n}"],
            check=False,
        )

    row = {
        "part": part,
        "tag": tag,
        "path": path,
        "codec_mode": label,
        "codec": codec,
        "wh_ack": wh_ack,
        "flows": flows,
        "rate_mbps": rate,
        "file_mib": FILE_MIB,
        "verify": ver["verify"],
        "flows_ok": ver["flows_ok"],
        "flows_fail": ver["flows_fail"],
        "agg_goodput_mbps": send_stats.get("agg_goodput_mbps"),
        "wall_sec": send_stats.get("wall_sec"),
        "send_avg_cpu": resources["send"]["avg_cpu"],
        "send_peak_cpu": resources["send"]["peak_cpu"],
        "send_peak_rss_kb": resources["send"]["peak_rss_kb"],
        "recv_avg_cpu": resources["recv"]["avg_cpu"],
        "recv_peak_cpu": resources["recv"]["peak_cpu"],
        "recv_peak_rss_kb": resources["recv"]["peak_rss_kb"],
        "relay2_avg_cpu": (resources.get("relay2") or {}).get("avg_cpu"),
        "relay2_peak_cpu": (resources.get("relay2") or {}).get("peak_cpu"),
        "relay2_peak_rss_kb": (resources.get("relay2") or {}).get("peak_rss_kb"),
        "relay3_avg_cpu": (resources.get("relay3") or {}).get("avg_cpu"),
        "relay3_peak_cpu": (resources.get("relay3") or {}).get("peak_cpu"),
        "relay3_peak_rss_kb": (resources.get("relay3") or {}).get("peak_rss_kb"),
        "host_n1_peak_cpu": (resources["host"].get("n1") or {}).get("host_peak_cpu"),
        "host_n4_peak_cpu": (resources["host"].get("n4") or {}).get("host_peak_cpu"),
        "host_n2_peak_cpu": (resources["host"].get("n2") or {}).get("host_peak_cpu"),
        "host_n3_peak_cpu": (resources["host"].get("n3") or {}).get("host_peak_cpu"),
        "n4_nic_peak_mbps": (resources["host"].get("n4") or {}).get("nic_peak_mbps"),
        "resources": resources,
    }
    print(
        f"  -> {row['verify']} goodput={row.get('agg_goodput_mbps')} "
        f"sendCPU={row['send_avg_cpu']}/{row['send_peak_cpu']} rss={row['send_peak_rss_kb']} "
        f"recvCPU={row['recv_avg_cpu']}/{row['recv_peak_cpu']} rss={row['recv_peak_rss_kb']}",
        flush=True,
    )
    # free sink/relay disk: drop bulky outputs after metrics are collected
    for n in (1, 2, 3, 4):
        ssh(
            n,
            f"rm -f {prefix}* {case_dir}/out_* {rlog} {slog} 2>/dev/null; "
            f"rm -rf {case_dir}/mon 2>/dev/null || true",
            check=False,
        )
    cleanup()
    return row


def run_part_a(rows: list, port_counter: list) -> None:
    for path in PATHS:
        for mode in CODEC_MODES:
            label = mode[0]
            ceil = CEIL[path][label]
            for frac in LOAD_FRACS:
                rate = round50(ceil * frac)
                port_counter[0] += 1
                port = PORT_BASE + port_counter[0]
                rows.append(one_case("A", path, mode, 1, rate, port))
                save(rows)


def run_part_b(rows: list, port_counter: list) -> None:
    for path in PATHS:
        for mode in CODEC_MODES:
            label = mode[0]
            for flows in FLOWS_LIST:
                rate = P4_PASS[path][label].get(flows)
                if rate is None:
                    continue
                port_counter[0] += 1
                port = PORT_BASE + port_counter[0]
                rows.append(one_case("B", path, mode, flows, rate, port))
                save(rows)


def run_part_c(rows: list) -> None:
    """Re-run 4B batch1 stress with fixed monitors; record host CPU peaks."""
    print("\n===== PART C: 4B batch1 stress with monitors =====", flush=True)
    cfg = "scripts/local/stress_linear_4b_batch1_copy_mixed.json"
    result = f"build/wire-stress-linear-4b-batch1-p5"
    # ensure monitor helpers visible to local runner (already patched)
    r = ssh(
        1,
        f"""
set -e
cd /home/fyp1/work/multi-flow-rate-control
rm -rf {result}
RESULT_DIR={result} ./scripts/local/run_wire_stress.sh {cfg}
""",
        check=False,
        timeout=600,
    )
    # pull summary
    local = OUT_DIR / "part_c_stress"
    local.mkdir(parents=True, exist_ok=True)
    run(
        [
            "rsync",
            "-az",
            "--exclude=out",
            "--exclude=payloads",
            f"{HOST[1]}:/home/fyp1/work/multi-flow-rate-control/{result}/",
            str(local) + "/",
        ],
        check=False,
    )
    # summarize host monitors if present
    mon_dir = local / "monitor"
    host_summary = {}
    if mon_dir.is_dir():
        for csvf in mon_dir.glob("*.csv"):
            host_summary[csvf.stem] = summarize_host_csv(csvf.read_text(encoding="utf-8", errors="ignore"))
    status = "PASS" if "Summary: 6 PASS" in (r.stdout or "") or (local / "streams.csv").is_file() else "FAIL"
    # parse streams.csv pass count
    sc = local / "streams.csv"
    if sc.is_file():
        lines = sc.read_text().strip().splitlines()
        npass = sum(1 for ln in lines[1:] if ",PASS," in ln)
        nfail = sum(1 for ln in lines[1:] if ",FAIL," in ln)
        status = "PASS" if nfail == 0 and npass > 0 else "FAIL"
    else:
        npass = nfail = 0
    row = {
        "part": "C",
        "tag": "4b_batch1_copy_mixed",
        "path": "mixed",
        "codec_mode": "copy",
        "codec": "copy",
        "wh_ack": None,
        "flows": 6,
        "rate_mbps": None,
        "file_mib": None,
        "verify": status,
        "flows_ok": npass,
        "flows_fail": nfail,
        "agg_goodput_mbps": None,
        "wall_sec": None,
        "host_monitors": host_summary,
        "result_dir": str(local),
        "send_avg_cpu": None,
        "send_peak_cpu": None,
        "send_peak_rss_kb": None,
        "recv_avg_cpu": None,
        "recv_peak_cpu": None,
        "recv_peak_rss_kb": None,
        "relay2_avg_cpu": None,
        "relay2_peak_cpu": None,
        "relay2_peak_rss_kb": None,
        "relay3_avg_cpu": None,
        "relay3_peak_cpu": None,
        "relay3_peak_rss_kb": None,
        "host_n1_peak_cpu": (host_summary.get("node1") or {}).get("host_peak_cpu"),
        "host_n4_peak_cpu": (host_summary.get("node4") or {}).get("host_peak_cpu"),
        "host_n2_peak_cpu": (host_summary.get("node2") or {}).get("host_peak_cpu"),
        "host_n3_peak_cpu": (host_summary.get("node3") or {}).get("host_peak_cpu"),
        "n4_nic_peak_mbps": (host_summary.get("node4") or {}).get("nic_peak_mbps"),
    }
    print(f"  -> part C {status} host={host_summary}", flush=True)
    rows.append(row)
    save(rows)


def save(rows: list) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_JSON.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    flat_keys = [
        "part",
        "tag",
        "path",
        "codec_mode",
        "flows",
        "rate_mbps",
        "verify",
        "agg_goodput_mbps",
        "send_avg_cpu",
        "send_peak_cpu",
        "send_peak_rss_kb",
        "recv_avg_cpu",
        "recv_peak_cpu",
        "recv_peak_rss_kb",
        "relay2_avg_cpu",
        "relay2_peak_cpu",
        "relay2_peak_rss_kb",
        "relay3_avg_cpu",
        "relay3_peak_cpu",
        "relay3_peak_rss_kb",
        "host_n1_peak_cpu",
        "host_n2_peak_cpu",
        "host_n3_peak_cpu",
        "host_n4_peak_cpu",
        "n4_nic_peak_mbps",
    ]
    with LOCAL_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=flat_keys, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_report(rows: list) -> None:
    sub = "linear" if TOPO == "linear" else "vxlan"
    report = REPO / "build" / "report-data" / sub / "phase5_resource_profile.md"
    report.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Phase 5 — 运行时资源剖面（CPU / 内存 / NIC）",
        "",
        f"- 原始: `build/{NAME}/`",
        f"- 采样: 进程 `proc_resource_monitor.py` + 主机 `iperf_like_monitor.py` @ {SAMPLE_HZ} Hz",
        f"- wirehair: seg={WH_SEGMENT} win={WH_WINDOW}",
        f"- payload: {FILE_MIB} MiB/flow",
        "",
        "## Part A — 单流（相对 phase3 上限 50%/100%）",
        "",
        "| path | codec | rate | verify | goodput | send avg/peak CPU | send peak RSS (MB) | recv avg/peak CPU | recv peak RSS (MB) | relay2/3 peak CPU |",
        "|---|---|---:|---|---:|---|---:|---|---:|---|",
    ]

    def rss_mb(kb):
        return "" if kb is None else f"{kb/1024:.1f}"

    def cpu_pair(a, p):
        if a is None and p is None:
            return ""
        return f"{a}/{p}"

    for r in rows:
        if r.get("part") != "A":
            continue
        lines.append(
            f"| {r['path']} | {r['codec_mode']} | {r['rate_mbps']} | {r['verify']} | "
            f"{'' if r.get('agg_goodput_mbps') is None else round(r['agg_goodput_mbps'],1)} | "
            f"{cpu_pair(r.get('send_avg_cpu'), r.get('send_peak_cpu'))} | {rss_mb(r.get('send_peak_rss_kb'))} | "
            f"{cpu_pair(r.get('recv_avg_cpu'), r.get('recv_peak_cpu'))} | {rss_mb(r.get('recv_peak_rss_kb'))} | "
            f"{cpu_pair(r.get('relay2_peak_cpu'), r.get('relay3_peak_cpu'))} |"
        )

    lines += [
        "",
        "## Part B — 多流扩展（2/4/8，码率=4A PASS 点）",
        "",
        "| path | codec | flows | rate/flow | verify | send peak CPU/RSS(MB) | recv peak CPU/RSS(MB) | relay2 peak CPU/RSS |",
        "|---|---|---:|---:|---|---|---|---|",
    ]
    for r in rows:
        if r.get("part") != "B":
            continue
        lines.append(
            f"| {r['path']} | {r['codec_mode']} | {r['flows']} | {r['rate_mbps']} | {r['verify']} | "
            f"{r.get('send_peak_cpu')}/{rss_mb(r.get('send_peak_rss_kb'))} | "
            f"{r.get('recv_peak_cpu')}/{rss_mb(r.get('recv_peak_rss_kb'))} | "
            f"{r.get('relay2_peak_cpu')}/{rss_mb(r.get('relay2_peak_rss_kb'))} |"
        )

    lines += ["", "## Part C — 4B 应用向抽检（batch1 copy mixed）", ""]
    for r in rows:
        if r.get("part") != "C":
            continue
        lines.append(f"- status: **{r['verify']}** ({r.get('flows_ok')}/{r.get('flows_ok',0)+r.get('flows_fail',0)} PASS)")
        lines.append(f"- host peak CPU: n1={r.get('host_n1_peak_cpu')} n2={r.get('host_n2_peak_cpu')} n3={r.get('host_n3_peak_cpu')} n4={r.get('host_n4_peak_cpu')}")
        lines.append(f"- n4 NIC peak Mbps: {r.get('n4_nic_peak_mbps')}")
        lines.append(f"- artifacts: `{r.get('result_dir')}`")

    lines += [
        "",
        "## 读数说明",
        "",
        "- **进程 CPU%**：相对单核（可 >100% 若多核）；avg/peak 来自传输窗口采样。",
        "- **RSS**：进程常驻内存峰值；wirehair 随 window×segment×flows 上升。",
        "- **主机 CPU**：整机利用率；含内核转发开销。",
        "",
    ]
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {report}", flush=True)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows: list = []
    if LOCAL_JSON.exists() and not FRESH:
        rows = json.loads(LOCAL_JSON.read_text(encoding="utf-8"))
        print(f"resume {len(rows)} rows from {LOCAL_JSON}", flush=True)

    done_tags = {r.get("tag") for r in rows if r.get("verify") not in (None, "RECV_FAIL", "RELAY_FAIL")}
    # For simplicity on FRESH wipe
    if FRESH:
        rows = []
        done_tags = set()
        if OUT_DIR.exists():
            shutil.rmtree(OUT_DIR)
        OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Phase5 parts={PARTS} smoke={SMOKE} name={NAME}", flush=True)
    prepare_payload()
    stage_monitors()
    port_counter = [0]

    # skip already-done by rebuilding schedule and checking tags
    if "A" in PARTS:
        planned = []
        for path in PATHS:
            for mode in CODEC_MODES:
                for frac in LOAD_FRACS:
                    rate = round50(CEIL[path][mode[0]] * frac)
                    planned.append(("A", path, mode, 1, rate))
        for part, path, mode, flows, rate in planned:
            port_counter[0] += 1
            port = PORT_BASE + port_counter[0]
            tag = f"{part}_{path}_{mode[0]}_f{flows}_r{rate}_p{port}"
            # port embedded in tag makes resume fragile; match by softer key
            soft = f"{part}_{path}_{mode[0]}_f{flows}_r{rate}_"
            if any(t.startswith(soft) for t in done_tags):
                print(f"skip {soft}*", flush=True)
                continue
            rows.append(one_case(part, path, mode, flows, rate, port))
            done_tags.add(rows[-1]["tag"])
            save(rows)

    if "B" in PARTS:
        for path in PATHS:
            for mode in CODEC_MODES:
                for flows in FLOWS_LIST:
                    rate = P4_PASS[path][mode[0]].get(flows)
                    if rate is None:
                        continue
                    soft = f"B_{path}_{mode[0]}_f{flows}_r{rate}_"
                    if any(t.startswith(soft) for t in done_tags):
                        print(f"skip {soft}*", flush=True)
                        continue
                    port_counter[0] += 1
                    port = PORT_BASE + port_counter[0]
                    rows.append(one_case("B", path, mode, flows, rate, port))
                    done_tags.add(rows[-1]["tag"])
                    save(rows)

    if "C" in PARTS:
        if any(r.get("part") == "C" for r in rows) and not FRESH:
            print("skip part C (already present)", flush=True)
        else:
            # sync local stress runner fixes first from host happens outside; assume synced
            run_part_c(rows)

    save(rows)
    write_report(rows)
    print(f"DONE -> {LOCAL_JSON}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
