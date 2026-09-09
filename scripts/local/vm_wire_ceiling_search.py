#!/usr/bin/env python3
"""Find per-path/codec throughput ceiling with <=50 Mbps precision.

Wirehair uses throughput-oriented params: --wh-segment-mib=2 --wh-window=8.
For each (path, codec_mode), climb then binary-search the max --rate-mbps
that still PASSes. PASS = checksum OK, or byte completion >= WH_PASS_PCT
(default 98). Report that rate and measured goodput.

Env:
  WH_CEIL_SMOKE=1
  WH_CEIL_FRESH=1
  WH_CEIL_SKIP=N
  WH_CEIL_NAME=...
  WH_CEIL_MIB=200
  WH_CEIL_MAX=5000
  WH_CEIL_PATHS=hop1_direct,hop2_kernel,...
  WH_CEIL_CODECS=copy,rs,wirehair_noack,wirehair_ack
  WH_PASS_PCT=98
"""

from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import sys
time = __import__("time")
from pathlib import Path
import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parent))
from topo_lab import N1, N2, N3, N4, CEILING_PATHS, MONITOR_IFACES, TOPO  # noqa: E402

SSH = {
    1: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.161"],
    2: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.162"],
    3: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.163"],
    4: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.164"],
}

WG = "/home/fyp1/work/multi-flow-rate-control/build/wg_multi_pipeline"
RELAY = "/home/fyp1/work/multi-flow-rate-control/build/wire_relay"
BASE = "/tmp/wire_ceil_matrix"
PORT_BASE = 27100
ACK_BASE = 27200


FILE_MIB = int(os.environ.get("WH_CEIL_MIB", "200"))
MAX_RATE = int(os.environ.get("WH_CEIL_MAX", "5000"))
PRECISION = 50
PASS_PCT = float(os.environ.get("WH_PASS_PCT", "98"))
EXPECTED_BYTES = FILE_MIB * 1024 * 1024
# Throughput-oriented Wirehair (matches prior full-matrix peaks)
WH_SEGMENT = 2
WH_WINDOW = 8
WH_REPAIR = 10
IDLE_SEC = int(os.environ.get("WH_IDLE_SEC", "25"))
RELAY_IDLE = int(os.environ.get("WH_RELAY_IDLE", "45"))
POST_SEND_WAIT_S = int(os.environ.get("WH_POST_SEND_WAIT_S", "60"))

PATHS = list(CEILING_PATHS)

CODEC_MODES = [
    ("copy", "copy", None),
    ("block", "block", None),
    ("xor-fec", "xor-fec", None),
    ("rs-fec", "rs-fec", None),
    ("rs", "rs", None),
    ("wirehair_noack", "wirehair", False),
    ("wirehair_ack", "wirehair", True),
]

# Coarse ladder then binary refine to PRECISION (start low so 3hop ceilings are found)
COARSE = [200, 400, 600, 800, 1200, 1600, 2000, 2500, 3000, 4000, 5000]
for _extra in (6000, 7000, 8000, 9000, 10000):
    if _extra <= MAX_RATE and _extra not in COARSE:
        COARSE.append(_extra)

SMOKE = os.environ.get("WH_CEIL_SMOKE", "0") == "1"
FRESH = os.environ.get("WH_CEIL_FRESH", "0") == "1"
SKIP = int(os.environ.get("WH_CEIL_SKIP", "0"))
NAME = os.environ.get("WH_CEIL_NAME", "wire_ceiling_search")
PATH_FILTER = {
    x.strip()
    for x in os.environ.get("WH_CEIL_PATHS", "").split(",")
    if x.strip()
}
CODEC_FILTER = {
    x.strip()
    for x in os.environ.get("WH_CEIL_CODECS", "").split(",")
    if x.strip()
}
if re.fullmatch(r"[A-Za-z0-9_.-]+", NAME) is None:
    raise ValueError("bad name")

if PATH_FILTER:
    PATHS = [p for p in PATHS if p[0] in PATH_FILTER]
    if not PATHS:
        raise ValueError(f"WH_CEIL_PATHS matched nothing: {PATH_FILTER}")

if CODEC_FILTER:
    CODEC_MODES = [c for c in CODEC_MODES if c[0] in CODEC_FILTER]
    if not CODEC_MODES:
        raise ValueError(f"WH_CEIL_CODECS matched nothing: {CODEC_FILTER}")

if SMOKE:
    PATHS = [p for p in PATHS if p[0] in ("hop1_direct", "hop3_relay")]
    CODEC_MODES = [
        ("copy", "copy", None),
        ("wirehair_ack", "wirehair", True),
    ]
    COARSE = [800, 1500, 2500]
    MAX_RATE = 2500
    FILE_MIB = 64
    NAME = NAME + "_smoke"

REPO = Path(__file__).resolve().parents[2]
LOCAL_JSON = REPO / "build" / f"{NAME}.json"
LOCAL_CSV = REPO / "build" / f"{NAME}.csv"
PROBES_JSON = REPO / "build" / f"{NAME}_probes.json"


def run(cmd, check=True, timeout=1200):
    r = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if check and r.returncode != 0:
        print("FAIL:", cmd, file=sys.stderr)
        print((r.stdout or "")[-2500:], file=sys.stderr)
        print((r.stderr or "")[-2500:], file=sys.stderr)
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
        (1, "wg_multi_pipeline.*(271|272)"),
        (2, "wire_relay.*271|wg_multi_pipeline.*(271|272)"),
        (3, "wire_relay.*271|wg_multi_pipeline.*(271|272)"),
        (4, "wg_multi_pipeline.*(271|272)"),
    ):
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
    block = bytes((i * 43 + i // 29) & 0xff for i in range(65536))
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
        timeout=900,
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
    m = re.search(
        r"udp-send:.*?source_mbps=([0-9.]+).*?wire_mbps=([0-9.]+)", text
    )
    if m:
        row["actual_source_mbps"] = float(m.group(1))
        row["wire_mbps"] = float(m.group(2))
        row["goodput_mbps"] = float(m.group(1))
    m = re.search(
        r"wirehair-send: source_bytes=(\d+) segments=\d+ repair_sent=(\d+) "
        r"wire_bytes=(\d+) ack=(\w+)",
        text,
    )
    if m:
        row["source_bytes"] = int(m.group(1))
        row["repair_sent"] = int(m.group(2))
        row["wire_bytes"] = int(m.group(3))
        if row.get("wall_sec"):
            row["goodput_mbps"] = row["source_bytes"] * 8.0 / row["wall_sec"] / 1e6
            row["wire_mbps"] = row["wire_bytes"] * 8.0 / row["wall_sec"] / 1e6
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


def round_rate(x: float) -> int:
    return int(PRECISION * round(x / PRECISION))


def probe_pass(row: dict) -> bool:
    """PASS = checksum OK, or completion >= PASS_PCT."""
    if row.get("verify") == "OK" or row.get("checksum_ok"):
        return True
    out_b = int(row.get("out_bytes") or 0)
    if EXPECTED_BYTES <= 0 or out_b <= 0:
        return False
    return (100.0 * out_b / EXPECTED_BYTES) >= PASS_PCT


def annotate_probe(row: dict) -> dict:
    out_b = int(row.get("out_bytes") or 0)
    completion = 100.0 * out_b / EXPECTED_BYTES if EXPECTED_BYTES else 0.0
    row["expected_bytes"] = EXPECTED_BYTES
    row["completion_pct"] = round(completion, 3)
    row["pass"] = probe_pass(row)
    return row



def _iface_list(node: int) -> list[str]:
    raw = MONITOR_IFACES.get(node, "")
    return [x for x in str(raw).split() if x]


def snapshot_iface_bytes(node: int) -> dict:
    """Read /sys NIC rx_bytes/tx_bytes for MONITOR_IFACES[node]."""
    ifaces = _iface_list(node)
    if not ifaces:
        return {}
    quoted = " ".join(ifaces)
    script = f"""
python3 - <<'PY'
from pathlib import Path
import json
out = {{}}
for name in "{quoted}".split():
    base = Path("/sys/class/net") / name / "statistics"
    if not base.is_dir():
        continue
    out[name] = {{
        "rx": int((base / "rx_bytes").read_text()),
        "tx": int((base / "tx_bytes").read_text()),
    }}
print(json.dumps(out))
PY
"""
    r = ssh(node, script, check=False)
    try:
        return json.loads((r.stdout or "").strip().splitlines()[-1])
    except Exception:
        return {}


def iface_delta_mbps(before: dict, after: dict, wall_sec) -> dict:
    """Aggregate NIC byte deltas into Mbps over wall_sec."""
    row = {
        "link_tx_bytes": 0,
        "link_rx_bytes": 0,
        "link_tx_mbps": None,
        "link_rx_mbps": None,
        "link_ifaces": sorted(set(before) | set(after)),
    }
    if not wall_sec or wall_sec <= 0:
        return row
    tx = rx = 0
    for name in set(before) | set(after):
        b = before.get(name) or {"rx": 0, "tx": 0}
        a = after.get(name) or {"rx": 0, "tx": 0}
        tx += max(0, int(a.get("tx", 0)) - int(b.get("tx", 0)))
        rx += max(0, int(a.get("rx", 0)) - int(b.get("rx", 0)))
    row["link_tx_bytes"] = tx
    row["link_rx_bytes"] = rx
    row["link_tx_mbps"] = tx * 8.0 / wall_sec / 1e6
    row["link_rx_mbps"] = rx * 8.0 / wall_sec / 1e6
    return row


def one_probe(path, codec_mode, rate: int, port: int) -> dict:
    path_name, hops, app_relay, sink, next_hop, relays = path
    label, codec, wh_ack = codec_mode
    cleanup()
    tag = f"{path_name}_{label}_r{rate}_p{port}"
    out = f"{BASE}/{tag}.out"
    rlog = f"{BASE}/{tag}_recv.log"
    slog = f"{BASE}/{tag}_send.log"
    ack_port = ACK_BASE + (port - PORT_BASE)
    recv_flags, send_flags = codec_flags(codec, wh_ack)

    for n in (1, 2, 3, 4):
        ssh(n, f"mkdir -p {BASE}")
    ssh(sink, f"rm -f {out} {rlog}; : > {rlog}")
    ssh(1, f"rm -f {slog}; : > {slog}")

    ssh(
        sink,
        f"""
nohup {WG} {recv_flags} --local-node-id 4 \
  --udp-recv {port} {out} --idle-sec {IDLE_SEC} --strict \
  >{rlog} 2>&1 &
echo $! > {BASE}/{tag}_recv.pid
""",
    )
    if codec == "wirehair":
        if not wait_grep(sink, rlog, "wirehair-recv: listening", 500):
            return {"rate": rate, "verify": "RECV_FAIL"}
    else:
        time.sleep(0.8)
        if ssh(sink, f"kill -0 $(cat {BASE}/{tag}_recv.pid)", check=False).returncode != 0:
            return {
                "rate": rate,
                "verify": "RECV_FAIL",
                "recv_log": ssh(sink, f"tail -20 {rlog}", check=False).stdout[-800:],
            }

    if app_relay:
        relay_extra = os.environ.get("WH_RELAY_EXTRA", "").strip()
        for rnode, rnext in relays:
            xlog = f"{BASE}/{tag}_relay{rnode}.log"
            ssh(rnode, f"rm -f {xlog}; : > {xlog}")
            ret = f"--return-hop {N1}:{ack_port}" if wh_ack else ""
            ssh(
                rnode,
                f"""
nohup {RELAY} --local-node-id {rnode} --listen {port} \
  --next-hop {rnext}:{port} {ret} --idle-exit-sec {RELAY_IDLE} \
  {relay_extra} \
  >{xlog} 2>&1 &
echo $! > {BASE}/{tag}_relay{rnode}.pid
""",
            )
            if not wait_grep(rnode, xlog, "wire-relay: local_node_id", 300):
                return {"rate": rate, "verify": "RELAY_FAIL", "relay_node": rnode}

    ack_arg = f"--ack-port={ack_port}" if (codec == "wirehair" and wh_ack) else ""
    ttl = max(8, hops + 2)
    # NIC counters: sender TX + sink RX (link throughput vs app goodput/wire)
    snap_tx0 = snapshot_iface_bytes(1)
    snap_rx0 = snapshot_iface_bytes(sink)
    t_link0 = time.time()
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
        send_out = ssh(1, send_script, timeout=900).stdout
    except SystemExit:
        send_out = ssh(
            1, f"cat {BASE}/{tag}_time.txt {slog} 2>/dev/null || true", check=False
        ).stdout
        row = {"rate": rate, "verify": "SEND_FAIL"}
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

    snap_tx1 = snapshot_iface_bytes(1)
    snap_rx1 = snapshot_iface_bytes(sink)
    t_link1 = time.time()

    row = {"rate": rate, "port": port}
    row.update(parse_send(send_out))
    row.update(verify(sink, out))
    # Prefer app wall_sec for rates; fall back to wall-clock around send+drain.
    link_wall = row.get("wall_sec") or max(0.001, t_link1 - t_link0)
    tx_stats = iface_delta_mbps(snap_tx0, snap_tx1, link_wall)
    rx_stats = iface_delta_mbps(snap_rx0, snap_rx1, link_wall)
    row["link_tx_mbps"] = tx_stats.get("link_tx_mbps")
    row["link_rx_mbps"] = rx_stats.get("link_rx_mbps")
    row["link_tx_bytes"] = tx_stats.get("link_tx_bytes")
    row["link_rx_bytes"] = rx_stats.get("link_rx_bytes")
    row["link_tx_ifaces"] = tx_stats.get("link_ifaces")
    row["link_rx_ifaces"] = rx_stats.get("link_ifaces")
    # Explicit aliases for reports: goodput vs app-wire vs NIC throughput
    row["throughput_wire_mbps"] = row.get("wire_mbps")
    row["throughput_link_tx_mbps"] = row.get("link_tx_mbps")
    row["throughput_link_rx_mbps"] = row.get("link_rx_mbps")
    cleanup()
    # drop large remote outs
    ssh(sink, f"rm -f {out}", check=False)
    return row


def search_ceiling(path, codec_mode, port_base: int, probes: list) -> dict:
    path_name = path[0]
    label = codec_mode[0]
    print(f"\n##### SEARCH {path_name} {label} #####", flush=True)

    last_pass = None
    first_fail = None
    port = port_base

    def take(rate: int, phase: str) -> bool:
        nonlocal port, last_pass, first_fail
        port += 1
        print(f"  {phase} {rate} ...", flush=True)
        r = one_probe(path, codec_mode, rate, port)
        r.update(
            {
                "path": path_name,
                "hops": path[1],
                "app_relay": path[2],
                "codec_mode": label,
                "codec": codec_mode[1],
                "wh_ack": codec_mode[2],
                "phase": phase,
            }
        )
        annotate_probe(r)
        probes.append(r)
        print(
            f"  -> verify={r.get('verify')} completion={r.get('completion_pct')}% "
            f"pass={r.get('pass')} goodput={r.get('goodput_mbps')} "
            f"wire={r.get('wire_mbps')} link_tx={r.get('link_tx_mbps')} "
            f"link_rx={r.get('link_rx_mbps')}",
            flush=True,
        )
        if r.get("pass"):
            last_pass = r
            return True
        first_fail = r
        return False

    for rate in COARSE:
        if rate > MAX_RATE:
            break
        if not take(rate, "coarse"):
            break

    if last_pass is None:
        # First coarse already failed: search downward from fail toward PRECISION
        if first_fail is None:
            return {
                "path": path_name,
                "hops": path[1],
                "app_relay": path[2],
                "codec_mode": label,
                "codec": codec_mode[1],
                "wh_ack": codec_mode[2],
                "max_pass_rate_mbps": None,
                "ceiling_mbps": None,
                "precision_mbps": PRECISION,
                "note": "no_probe",
            }
        lo = 0
        hi = first_fail["rate"]
        while hi - lo > PRECISION:
            mid = round_rate((lo + hi) / 2)
            if mid <= lo:
                mid = lo + PRECISION
            if mid >= hi:
                mid = hi - PRECISION
            if mid <= lo or mid >= hi:
                break
            if take(mid, "low_refine"):
                lo = mid
            else:
                hi = mid
        if last_pass is None:
            return {
                "path": path_name,
                "hops": path[1],
                "app_relay": path[2],
                "codec_mode": label,
                "codec": codec_mode[1],
                "wh_ack": codec_mode[2],
                "max_pass_rate_mbps": None,
                "first_fail_rate_mbps": first_fail["rate"],
                "ceiling_mbps": None,
                "precision_mbps": PRECISION,
                "note": "no_pass_below_first_fail",
            }

    lo = last_pass["rate"]
    hi = first_fail["rate"] if first_fail else min(MAX_RATE + PRECISION, lo + PRECISION)

    # If never failed, try push to MAX_RATE once
    if first_fail is None and lo < MAX_RATE:
        if take(MAX_RATE, "push_max"):
            lo = MAX_RATE
        else:
            hi = MAX_RATE

    # Binary search between lo(pass) and hi(fail)
    if first_fail is not None:
        while hi - lo > PRECISION:
            mid = round_rate((lo + hi) / 2)
            if mid <= lo:
                mid = lo + PRECISION
            if mid >= hi:
                mid = hi - PRECISION
            if mid <= lo or mid >= hi:
                break
            if take(mid, "refine"):
                lo = mid
            else:
                hi = mid

    # Among all PASS probes for this config, also track best goodput
    passes = [
        p
        for p in probes
        if p.get("path") == path_name
        and p.get("codec_mode") == label
        and p.get("pass")
        and p.get("goodput_mbps")
    ]
    best_gp = max(passes, key=lambda x: x["goodput_mbps"]) if passes else last_pass

    return {
        "path": path_name,
        "hops": path[1],
        "app_relay": path[2],
        "codec_mode": label,
        "codec": codec_mode[1],
        "wh_ack": codec_mode[2],
        "wh_segment_mib": WH_SEGMENT if codec_mode[1] == "wirehair" else None,
        "wh_window": WH_WINDOW if codec_mode[1] == "wirehair" else None,
        "max_pass_rate_mbps": last_pass["rate"],
        "max_pass_goodput_mbps": last_pass.get("goodput_mbps"),
        "max_pass_wire_mbps": last_pass.get("wire_mbps"),
        "max_pass_link_tx_mbps": last_pass.get("link_tx_mbps"),
        "max_pass_link_rx_mbps": last_pass.get("link_rx_mbps"),
        "best_goodput_mbps": best_gp.get("goodput_mbps"),
        "best_goodput_at_rate": best_gp.get("rate"),
        "best_wire_mbps": best_gp.get("wire_mbps"),
        "best_link_tx_mbps": best_gp.get("link_tx_mbps"),
        "best_link_rx_mbps": best_gp.get("link_rx_mbps"),
        "first_fail_rate_mbps": first_fail["rate"] if first_fail else None,
        "precision_mbps": PRECISION,
        "ceiling_mbps": best_gp.get("goodput_mbps"),
        "ceiling_link_tx_mbps": best_gp.get("link_tx_mbps"),
        "ceiling_link_rx_mbps": best_gp.get("link_rx_mbps"),
        "repair_sent": last_pass.get("repair_sent"),
    }



def save(results, probes):
    LOCAL_JSON.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_JSON.write_text(json.dumps(results, indent=2))
    PROBES_JSON.write_text(json.dumps(probes, indent=2))
    fields = [
        "path",
        "hops",
        "app_relay",
        "codec_mode",
        "wh_ack",
        "wh_segment_mib",
        "wh_window",
        "max_pass_rate_mbps",
        "first_fail_rate_mbps",
        "best_goodput_mbps",
        "best_goodput_at_rate",
        "best_wire_mbps",
        "best_link_tx_mbps",
        "best_link_rx_mbps",
        "ceiling_mbps",
        "ceiling_link_tx_mbps",
        "ceiling_link_rx_mbps",
        "precision_mbps",
        "repair_sent",
    ]
    with LOCAL_CSV.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in results:
            w.writerow(r)

    # Markdown summary table (goodput + link side-by-side)
    report_subdir = "linear" if TOPO == "linear" else "vxlan"
    md_path = REPO / "build" / "report-data" / report_subdir / f"{NAME}.md"
    md_path.parent.mkdir(parents=True, exist_ok=True)
    codecs = []
    for r in results:
        c = r.get("codec_mode")
        if c and c not in codecs:
            codecs.append(c)
    paths = []
    for r in results:
        p = r.get("path")
        if p and p not in paths:
            paths.append(p)
    by = {(r.get("path"), r.get("codec_mode")): r for r in results}

    def fmt_num(x):
        if x is None:
            return "—"
        if isinstance(x, float):
            return f"{x:.0f}" if abs(x - round(x)) < 0.05 else f"{x:.1f}"
        return str(x)

    def cell_gp_link(r):
        if not r or r.get("ceiling_mbps") is None:
            return "—"
        gp = r.get("ceiling_mbps")
        ltx = r.get("best_link_tx_mbps")
        lrx = r.get("best_link_rx_mbps")
        # Prefer tx; show rx if materially different
        if ltx is None and lrx is None:
            return f"{gp:.0f}"
        link = ltx if ltx is not None else lrx
        if ltx is not None and lrx is not None and abs(ltx - lrx) > 50:
            return f"{gp:.0f} / tx{ltx:.0f} rx{lrx:.0f}"
        return f"{gp:.0f} / {link:.0f}"

    lines = [
        f"# Phase3 单流上限 — `{NAME}`",
        "",
        f"- PASS: checksum OK 或完成率 ≥ {PASS_PCT:.0f}%",
        f"- payload: {FILE_MIB} MiB；精度 ≤{PRECISION} Mbps；max_rate={MAX_RATE}",
        f"- Wirehair: seg={WH_SEGMENT} win={WH_WINDOW}",
        f"- **goodput** = PASS 探针中最大实测有效吞吐（源数据）",
        f"- **link_tx / link_rx** = 同一次 best-goodput 探针的 NIC 计数吞吐（Mbps）",
        f"- 主表单元格格式：`goodput / link`（单位 Mbps；link 默认取 link_tx）",
        "",
        "## 上限 goodput / link (Mbps)",
        "",
        "| path \\ codec | " + " | ".join(codecs) + " |",
        "|---|" + "|".join(["---:"] * len(codecs)) + "|",
    ]
    for path in paths:
        cells = [cell_gp_link(by.get((path, c))) for c in codecs]
        lines.append(f"| {path} | " + " | ".join(cells) + " |")

    lines.extend(
        [
            "",
            "## 上限 goodput only (Mbps)",
            "",
            "| path \\ codec | " + " | ".join(codecs) + " |",
            "|---|" + "|".join(["---:"] * len(codecs)) + "|",
        ]
    )
    for path in paths:
        cells = []
        for c in codecs:
            r = by.get((path, c))
            if not r or r.get("ceiling_mbps") is None:
                cells.append("—")
            else:
                cells.append(f"{r['ceiling_mbps']:.0f} (pass≤{r.get('max_pass_rate_mbps')})")
        lines.append(f"| {path} | " + " | ".join(cells) + " |")

    lines.extend(
        [
            "",
            "## 同探针链路吞吐 link_tx (Mbps)",
            "",
            "| path \\ codec | " + " | ".join(codecs) + " |",
            "|---|" + "|".join(["---:"] * len(codecs)) + "|",
        ]
    )
    for path in paths:
        cells = []
        for c in codecs:
            r = by.get((path, c))
            if not r or r.get("best_link_tx_mbps") is None:
                cells.append("—")
            else:
                cells.append(fmt_num(r.get("best_link_tx_mbps")))
        lines.append(f"| {path} | " + " | ".join(cells) + " |")

    lines.extend(
        [
            "",
            "## 明细",
            "",
            "| path | codec | max_pass_rate | goodput | wire | link_tx | link_rx | first_fail |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for r in results:
        def f(x):
            return "—" if x is None else (f"{x:.1f}" if isinstance(x, float) else str(x))

        # wire at best-goodput probe is not stored separately; use max_pass_wire as proxy
        # Prefer matching best probe wire if present on row later
        wire = r.get("best_wire_mbps")
        if wire is None:
            wire = r.get("max_pass_wire_mbps")
        lines.append(
            f"| {r.get('path')} | {r.get('codec_mode')} | {f(r.get('max_pass_rate_mbps'))} | "
            f"{f(r.get('ceiling_mbps'))} | {f(wire)} | "
            f"{f(r.get('best_link_tx_mbps'))} | {f(r.get('best_link_rx_mbps'))} | "
            f"{f(r.get('first_fail_rate_mbps'))} |"
        )
    lines.append("")
    md_path.write_text("\n".join(lines) + "\n")


def main():
    configs = [(p, c) for p in PATHS for c in CODEC_MODES]
    print(
        f"configs={len(configs)} paths={[p[0] for p in PATHS]} "
        f"codecs={[c[0] for c in CODEC_MODES]} "
        f"file_mib={FILE_MIB} max_rate={MAX_RATE} pass>={PASS_PCT}% "
        f"precision={PRECISION} wh_seg={WH_SEGMENT} wh_win={WH_WINDOW} smoke={SMOKE}",
        flush=True,
    )
    results = []
    probes = []
    if LOCAL_JSON.is_file() and not FRESH:
        try:
            results = json.loads(LOCAL_JSON.read_text())
            if PROBES_JSON.is_file():
                probes = json.loads(PROBES_JSON.read_text())
            print(f"resume results={len(results)}", flush=True)
        except json.JSONDecodeError:
            results = []
            probes = []

    cleanup()
    prepare()
    done = max(len(results), SKIP)
    port_base = PORT_BASE

    for i, (path, cm) in enumerate(configs):
        if i < done:
            continue
        row = search_ceiling(path, cm, port_base + i * 40, probes)
        results.append(row)
        save(results, probes)
        print("CEILING", row, flush=True)

    cleanup()
    save(results, probes)
    print(f"\nDONE -> {LOCAL_JSON}", flush=True)
    report_subdir = "linear" if TOPO == "linear" else "vxlan"
    print(f"MD   -> {REPO / 'build' / 'report-data' / report_subdir / (NAME + '.md')}", flush=True)


if __name__ == "__main__":
    main()
