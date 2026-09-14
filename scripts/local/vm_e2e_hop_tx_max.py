#!/usr/bin/env python3
"""VM1→VM4 max per-hop NIC TX (ignore loss / checksum).

Measures outbound TX on each hop while blasting:
  hop1: VM1 enp6s19  (10.20.20.1 → VM2)
  hop2: VM2 enp6s20  (10.30.30.1 → VM3)
  hop3: VM3 enp6s21  (10.40.40.1 → VM4)

Cases:
  - iperf3 UDP: each hop alone, plus e2e kernel VM1→VM4
  - copy / rs / PFC no-ACK / PFC ACK  ×  hop3_kernel / hop3_relay

iperf3 has no app-relay counterpart; raw UDP baseline is kernel only.
PASS/checksum is recorded but never used to pick the number — we keep the
probe with the highest hop TX.

Env:
  WH_TOPO=linear          (required for this lab)
  WH_HOPTX_NAME=...
  WH_HOPTX_MIB=200
  WH_HOPTX_RATES=2000,5000,10000
  WH_HOPTX_IPERF_RATES=2G,5G,10G
  WH_HOPTX_IPERF_SEC=6
  WH_HOPTX_SMOKE=1
"""

from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from topo_lab import N1, N2, N3, N4, RELAY2_NEXT, RELAY3_NEXT  # noqa: E402

SSH = {
    1: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.161"],
    2: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.162"],
    3: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.163"],
    4: ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.164"],
}

WG = "/home/fyp1/work/multi-flow-rate-control/build/wg_multi_pipeline"
RELAY = "/home/fyp1/work/multi-flow-rate-control/build/wire_relay"
MON_PY = "/tmp/iperf_like_monitor.py"
BASE = "/tmp/wire_hoptx"
PORT_BASE = 28100
ACK_BASE = 28200

# hop_id -> (node, outbound iface)
HOP_TX = {
    1: (1, "enp6s19"),
    2: (2, "enp6s20"),
    3: (3, "enp6s21"),
}

FILE_MIB = int(os.environ.get("WH_HOPTX_MIB", "200"))
NAME = os.environ.get("WH_HOPTX_NAME", "e2e_hop_tx_max")
IPERF_SEC = int(os.environ.get("WH_HOPTX_IPERF_SEC", "6"))
IDLE_SEC = 20
RELAY_IDLE = 40
WH_SEGMENT = 2
WH_WINDOW = int(os.environ.get("WH_HOPTX_WINDOW", "8"))
WH_REPAIR = 10
SAMPLE_PERIOD = 0.2

APP_RATES = [
    int(x)
    for x in os.environ.get("WH_HOPTX_RATES", "2000,5000,10000").split(",")
    if x.strip()
]
IPERF_RATES = [
    x.strip()
    for x in os.environ.get("WH_HOPTX_IPERF_RATES", "2G,5G,10G").split(",")
    if x.strip()
]

CODECS = [
    ("copy", "copy", None),
    ("rs", "rs", None),
    ("wirehair_noack", "wirehair", False),
    ("wirehair_ack", "wirehair", True),
]
PATHS = [
    ("hop3_kernel", False, 4, N4, []),
    ("hop3_relay", True, 4, N2, [(2, RELAY2_NEXT), (3, RELAY3_NEXT)]),
]

if os.environ.get("WH_HOPTX_SMOKE", "0") == "1":
    FILE_MIB = 64
    APP_RATES = [2000, 5000]
    IPERF_RATES = ["2G", "5G"]
    IPERF_SEC = 4
    CODECS = [("copy", "copy", None), ("wirehair_ack", "wirehair", True)]
    NAME = NAME + "_smoke"

PATH_FILTER = {
    x.strip()
    for x in os.environ.get("WH_HOPTX_PATHS", "").split(",")
    if x.strip()
}
CODEC_FILTER = {
    x.strip()
    for x in os.environ.get("WH_HOPTX_CODECS", "").split(",")
    if x.strip()
}
SKIP_IPERF = os.environ.get("WH_HOPTX_SKIP_IPERF", "0") == "1"
if PATH_FILTER:
    PATHS = [p for p in PATHS if p[0] in PATH_FILTER]
if CODEC_FILTER:
    CODECS = [c for c in CODECS if c[0] in CODEC_FILTER]

if re.fullmatch(r"[A-Za-z0-9_.-]+", NAME) is None:
    raise ValueError("bad name")

REPO = Path(__file__).resolve().parents[2]
OUT_DIR = REPO / "build" / NAME
LOCAL_JSON = OUT_DIR / f"{NAME}.json"
LOCAL_CSV = OUT_DIR / f"{NAME}.csv"
LOCAL_MD = OUT_DIR / f"{NAME}.md"
PROBES_JSON = OUT_DIR / f"{NAME}_probes.json"


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
    q = pat.replace("'", "'\\''")
    for _ in range(n):
        if ssh(node, f"grep -q '{q}' {log}", check=False).returncode == 0:
            return True
        time.sleep(0.1)
    return False


def codec_flags(codec: str, wh_ack):
    if codec != "wirehair":
        return f"--codec {codec}", f"--codec {codec}"
    ack = "--wh-ack" if wh_ack else "--no-wh-ack"
    common = (
        f"--codec wirehair --wh-segment-mib={WH_SEGMENT} "
        f"--wh-repair-pct={WH_REPAIR} --wh-window={WH_WINDOW} {ack}"
    )
    return common, common


def cleanup():
    for node, pat in (
        (1, "wg_multi_pipeline.*(281|282)|iperf3|iperf_like_monitor.py"),
        (2, "wire_relay.*281|wg_multi_pipeline.*(281|282)|iperf3|iperf_like_monitor.py"),
        (3, "wire_relay.*281|wg_multi_pipeline.*(281|282)|iperf3|iperf_like_monitor.py"),
        (4, "wg_multi_pipeline.*(281|282)|iperf3|iperf_like_monitor.py"),
    ):
        ssh(node, f"pkill -f '{pat}' >/dev/null 2>&1 || true", check=False)
    time.sleep(0.4)


def install_monitor():
    mon = REPO / "scripts" / "iperf_like_monitor.py"
    for n, host in (
        (1, "fyp1@10.10.10.161"),
        (2, "fyp1@10.10.10.162"),
        (3, "fyp1@10.10.10.163"),
        (4, "fyp1@10.10.10.164"),
    ):
        ssh(n, f"mkdir -p {BASE}")
        run(["scp", "-o", "BatchMode=yes", str(mon), f"{host}:{MON_PY}"])


def prepare_input():
    ssh(
        1,
        f"""
set -e
mkdir -p {BASE}
python3 - <<'PY'
from pathlib import Path
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
print(path.stat().st_size)
PY
""",
        timeout=900,
    )


def snapshot_hop_tx() -> dict:
    """Return {hop: tx_bytes} for the three outbound ifaces."""
    out = {}
    for hop, (node, iface) in HOP_TX.items():
        r = ssh(
            node,
            f"cat /sys/class/net/{iface}/statistics/tx_bytes",
            check=False,
        )
        try:
            out[hop] = int((r.stdout or "0").strip())
        except ValueError:
            out[hop] = 0
    return out


def start_hop_monitors(tag: str) -> None:
    for hop, (node, iface) in HOP_TX.items():
        csvp = f"{BASE}/{tag}_hop{hop}.csv"
        ssh(
            node,
            f"""
set +e
pkill -f 'iperf_like_monitor.py.*{tag}_hop{hop}' >/dev/null 2>&1
setsid python3 {MON_PY} {iface} {SAMPLE_PERIOD} {csvp} \
  >{BASE}/{tag}_hop{hop}.mon.log 2>&1 < /dev/null &
echo $! > {BASE}/{tag}_hop{hop}.mon.pid
exit 0
""",
            check=False,
        )


def stop_hop_monitors(tag: str) -> dict:
    peaks = {}
    for hop, (node, iface) in HOP_TX.items():
        ssh(
            node,
            f"kill $(cat {BASE}/{tag}_hop{hop}.mon.pid 2>/dev/null) >/dev/null 2>&1 || true",
            check=False,
        )
        r = ssh(node, f"cat {BASE}/{tag}_hop{hop}.csv 2>/dev/null || true", check=False)
        peak_mbps = 0.0
        for line in (r.stdout or "").splitlines()[1:]:
            parts = line.split(",")
            if len(parts) < 4:
                continue
            try:
                tx_bps = float(parts[3])
            except ValueError:
                continue
            peak_mbps = max(peak_mbps, tx_bps / 1e6)
        peaks[hop] = peak_mbps
    return peaks


def hop_delta_mbps(before: dict, after: dict, wall_sec: float) -> dict:
    row = {}
    if not wall_sec or wall_sec <= 0:
        wall_sec = 0.001
    for hop in (1, 2, 3):
        dtx = max(0, int(after.get(hop, 0)) - int(before.get(hop, 0)))
        row[f"hop{hop}_tx_bytes"] = dtx
        row[f"hop{hop}_tx_avg_mbps"] = dtx * 8.0 / wall_sec / 1e6
    return row


def parse_send(text: str) -> dict:
    row: dict = {}
    m = re.search(r"wall_sec=([0-9.]+)", text)
    if m:
        row["wall_sec"] = float(m.group(1))
    m = re.search(r"udp-send:.*?source_mbps=([0-9.]+).*?wire_mbps=([0-9.]+)", text)
    if m:
        row["goodput_mbps"] = float(m.group(1))
        row["wire_mbps"] = float(m.group(2))
    m = re.search(
        r"wirehair-send: source_bytes=(\d+) segments=\d+ repair_sent=(\d+) "
        r"wire_bytes=(\d+)",
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


def parse_iperf_json(text: str) -> dict:
    row = {
        "iperf_sender_mbps": None,
        "iperf_recv_mbps": None,
        "iperf_lost_pct": None,
    }
    try:
        d = json.loads(text)
    except json.JSONDecodeError:
        return row
    end = d.get("end") or {}
    s = end.get("sum") or {}
    r = end.get("sum_received") or {}
    if s.get("bits_per_second") is not None:
        row["iperf_sender_mbps"] = float(s["bits_per_second"]) / 1e6
    if r.get("bits_per_second") is not None:
        row["iperf_recv_mbps"] = float(r["bits_per_second"]) / 1e6
    if r.get("lost_percent") is not None:
        row["iperf_lost_pct"] = float(r["lost_percent"])
    return row


def attach_peaks(row: dict, peaks: dict) -> None:
    for hop in (1, 2, 3):
        row[f"hop{hop}_tx_peak_mbps"] = peaks.get(hop, 0.0)


def one_iperf(label: str, src: int, dst_ip: str, sink: int, offered: str, port: int) -> dict:
    tag = f"iperf_{label}_{offered}_p{port}"
    print(f"  iperf3 {label} -b {offered} ...", flush=True)
    cleanup()
    ssh(sink, f"pkill -u fyp1 iperf3 >/dev/null 2>&1 || true; mkdir -p {BASE}")
    ssh(
        sink,
        f"nohup iperf3 -s -B {dst_ip} -p {port} >{BASE}/{tag}_srv.log 2>&1 & echo $! > {BASE}/{tag}_srv.pid",
    )
    time.sleep(0.4)
    start_hop_monitors(tag)
    time.sleep(0.3)
    snap0 = snapshot_hop_tx()
    t0 = time.time()
    client = ssh(
        src,
        f"iperf3 -c {dst_ip} -p {port} -u -b {offered} -t {IPERF_SEC} -l 1370 -J",
        check=False,
        timeout=60,
    )
    t1 = time.time()
    snap1 = snapshot_hop_tx()
    peaks = stop_hop_monitors(tag)
    ssh(sink, f"pkill -u fyp1 iperf3 >/dev/null 2>&1 || true", check=False)
    row = {
        "kind": "iperf3_udp",
        "path": label,
        "codec": "iperf3_udp",
        "forwarding": "kernel" if label.startswith("e2e") else "direct_hop",
        "offered": offered,
        "port": port,
        "wall_sec": max(0.001, t1 - t0),
        "ok": client.returncode == 0,
    }
    row.update(parse_iperf_json(client.stdout or ""))
    row.update(hop_delta_mbps(snap0, snap1, row["wall_sec"]))
    attach_peaks(row, peaks)
    return row


def one_app(path, codec_mode, rate: int, port: int) -> dict:
    path_name, app_relay, sink, next_hop, relays = path
    label, codec, wh_ack = codec_mode
    tag = f"{path_name}_{label}_r{rate}_p{port}"
    print(f"  {path_name} {label} {rate} ...", flush=True)
    cleanup()
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
            return {
                "kind": "app",
                "path": path_name,
                "codec": label,
                "rate": rate,
                "ok": False,
                "verify": "RECV_FAIL",
            }
    else:
        time.sleep(0.6)

    if app_relay:
        hops = 3
        for rnode, rnext in relays:
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
                return {
                    "kind": "app",
                    "path": path_name,
                    "codec": label,
                    "rate": rate,
                    "ok": False,
                    "verify": "RELAY_FAIL",
                    "relay_node": rnode,
                }
    else:
        hops = 3

    ack_arg = f"--ack-port={ack_port}" if (codec == "wirehair" and wh_ack) else ""
    ttl = max(8, hops + 2)
    start_hop_monitors(tag)
    time.sleep(0.3)
    snap0 = snapshot_hop_tx()
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
        send_ok = True
    except SystemExit:
        send_out = ssh(
            1, f"cat {BASE}/{tag}_time.txt {slog} 2>/dev/null || true", check=False
        ).stdout
        send_ok = False

    # TX window = send only (do not wait for recv drain)
    snap1 = snapshot_hop_tx()
    peaks = stop_hop_monitors(tag)

    row = {
        "kind": "app",
        "path": path_name,
        "codec": label,
        "forwarding": "relay" if app_relay else "kernel",
        "rate": rate,
        "offered": f"{rate}M",
        "port": port,
        "ok": send_ok,
    }
    row.update(parse_send(send_out))
    wall = row.get("wall_sec") or 0.001
    row["wall_sec"] = wall
    row.update(hop_delta_mbps(snap0, snap1, wall))
    attach_peaks(row, peaks)
    cleanup()
    ssh(sink, f"rm -f {out}", check=False)
    return row


def best_by_hop(probes: list[dict], pred) -> dict:
    """For each hop, pick the probe with the highest avg TX (peak as tie-break)."""
    best = {}
    for hop in (1, 2, 3):
        avg_k = f"hop{hop}_tx_avg_mbps"
        peak_k = f"hop{hop}_tx_peak_mbps"
        chosen = None
        for p in probes:
            if not pred(p):
                continue
            score = (float(p.get(avg_k) or 0), float(p.get(peak_k) or 0))
            if chosen is None or score > chosen[0]:
                chosen = (score, p)
        if chosen:
            p = chosen[1]
            best[f"hop{hop}"] = {
                "tx_avg_mbps": p.get(avg_k),
                "tx_peak_mbps": p.get(peak_k),
                "offered": p.get("offered") or p.get("rate"),
                "path": p.get("path"),
                "iperf_sender_mbps": p.get("iperf_sender_mbps"),
                "iperf_recv_mbps": p.get("iperf_recv_mbps"),
                "iperf_lost_pct": p.get("iperf_lost_pct"),
                "wire_mbps": p.get("wire_mbps"),
            }
    return best


def write_outputs(probes: list[dict]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    PROBES_JSON.write_text(json.dumps(probes, indent=2) + "\n")

    summary = {
        "iperf3_per_hop": best_by_hop(
            probes, lambda p: p.get("kind") == "iperf3_udp" and p.get("path", "").startswith("hop")
        ),
        "iperf3_e2e_kernel": best_by_hop(
            probes, lambda p: p.get("kind") == "iperf3_udp" and p.get("path") == "e2e_kernel"
        ),
        "app": {},
    }
    for path_name, *_ in PATHS:
        for label, *_ in CODECS:
            summary["app"][f"{path_name}:{label}"] = best_by_hop(
                probes,
                lambda p, pn=path_name, lb=label: p.get("kind") == "app"
                and p.get("path") == pn
                and p.get("codec") == lb,
            )
    LOCAL_JSON.write_text(json.dumps(summary, indent=2) + "\n")

    fields = [
        "kind",
        "path",
        "codec",
        "forwarding",
        "offered",
        "rate",
        "ok",
        "wall_sec",
        "hop1_tx_avg_mbps",
        "hop2_tx_avg_mbps",
        "hop3_tx_avg_mbps",
        "hop1_tx_peak_mbps",
        "hop2_tx_peak_mbps",
        "hop3_tx_peak_mbps",
        "iperf_sender_mbps",
        "iperf_recv_mbps",
        "iperf_lost_pct",
        "wire_mbps",
        "goodput_mbps",
    ]
    with LOCAL_CSV.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for p in probes:
            w.writerow(p)

    def fmt(x):
        if x is None:
            return "—"
        try:
            return f"{float(x):.0f}"
        except (TypeError, ValueError):
            return str(x)

    def row3(block: dict) -> str:
        cells = []
        for hop in (1, 2, 3):
            h = block.get(f"hop{hop}") or {}
            cells.append(f"{fmt(h.get('tx_avg_mbps'))} (peak {fmt(h.get('tx_peak_mbps'))})")
        return " | ".join(cells)

    lines = [
        f"# VM1→VM4 per-hop max TX ({NAME})",
        "",
        "Only NIC outbound TX is ranked. Loss / checksum do not select the winner.",
        "",
        f"- file: {FILE_MIB} MiB; app offered {APP_RATES} Mbps; iperf3 -b {IPERF_RATES} for {IPERF_SEC}s",
        "- hop1 TX = VM1 enp6s19; hop2 TX = VM2 enp6s20; hop3 TX = VM3 enp6s21",
        "- avg = byte delta / send wall; peak = 0.2s sample",
        "",
        "## iperf3 UDP — each hop alone (raw hop capacity)",
        "",
        "| hop | max TX avg (peak) Mbps | offered | iperf sender / recv / loss |",
        "|---|---:|---:|---|",
    ]
    per = summary["iperf3_per_hop"]
    for hop, lab in ((1, "hop1"), (2, "hop2"), (3, "hop3")):
        h = per.get(f"hop{hop}") or {}
        lines.append(
            f"| {lab} | {fmt(h.get('tx_avg_mbps'))} (peak {fmt(h.get('tx_peak_mbps'))}) | "
            f"{h.get('offered','—')} | "
            f"{fmt(h.get('iperf_sender_mbps'))} / {fmt(h.get('iperf_recv_mbps'))} / "
            f"{h.get('iperf_lost_pct')} |"
        )
    e2e = summary["iperf3_e2e_kernel"]
    lines += [
        "",
        "## iperf3 UDP — e2e VM1→VM4 kernel (same flow, three hop TX)",
        "",
        "| hop1 TX | hop2 TX | hop3 TX |",
        "|---|---|---|",
        f"| {row3(e2e)} |",
        "",
        "## App codecs VM1→VM4 — max hop TX (Mbps avg / peak)",
        "",
        "| forwarding | codec | hop1 TX | hop2 TX | hop3 TX |",
        "|---|---|---|---|---|",
    ]
    for path_name, app_relay, *_ in PATHS:
        fwd = "relay" if app_relay else "kernel"
        for label, *_ in CODECS:
            block = summary["app"].get(f"{path_name}:{label}") or {}
            lines.append(f"| {fwd} | {label} | {row3(block)} |")
    lines += [
        "",
        f"Probes: `{PROBES_JSON}`",
        f"CSV: `{LOCAL_CSV}`",
        "",
    ]
    LOCAL_MD.write_text("\n".join(lines) + "\n")
    print(LOCAL_MD.read_text())


def main() -> int:
    if os.environ.get("WH_TOPO", "").strip().lower() != "linear":
        print("Set WH_TOPO=linear", file=sys.stderr)
        return 2
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"OUT={OUT_DIR}", flush=True)
    cleanup()
    install_monitor()
    prepare_input()

    probes: list[dict] = []
    port = PORT_BASE

    if not SKIP_IPERF:
        print("\n##### iperf3 per-hop #####", flush=True)
        hop_iperf = (
            ("hop1", 1, N2, 2),
            ("hop2", 2, N3, 3),
            ("hop3", 3, N4, 4),
        )
        for label, src, dst_ip, sink in hop_iperf:
            for offered in IPERF_RATES:
                port += 1
                probes.append(one_iperf(label, src, dst_ip, sink, offered, port))

        print("\n##### iperf3 e2e kernel #####", flush=True)
        for offered in IPERF_RATES:
            port += 1
            probes.append(one_iperf("e2e_kernel", 1, N4, 4, offered, port))

    print("\n##### app codecs e2e #####", flush=True)
    for path in PATHS:
        for codec_mode in CODECS:
            for rate in APP_RATES:
                port += 1
                probes.append(one_app(path, codec_mode, rate, port))

    cleanup()
    write_outputs(probes)
    print(f"Wrote {LOCAL_MD}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
