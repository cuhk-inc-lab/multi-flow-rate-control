#!/usr/bin/env python3
"""Wirehair full matrix on VMs: direct/relay × ACK on/off × 1/2/4 flows × rates.

Rates (Mbps): 500, 1000, 2000, 5000  (sparse above 1G — WiFi saturates earlier)
Payload: 500 MiB single-flow; 200 MiB per flow for multi-flow.

Direct:  Node1 station0 -> Node2 ap0
Relay:   Node1 -> Node2 wire_relay opaque -> Node3

Note: relay learns per-flow previous-hop UDP endpoints for RETURN_PATH ACK.
``--return-hop`` remains a fallback when no route has been learned yet.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

SSH1 = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.161"]
SSH2 = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.162"]
SSH3 = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.163"]

WG = "/home/fyp1/work/multi-flow-rate-control/build/wg_multi_pipeline"
RELAY = "/home/fyp1/work/multi-flow-rate-control/build/wire_relay"
BASE = "/tmp/wirehair_full_matrix"
PORT_BASE = 24500
ACK_BASE = 24600

N2_DIRECT = "10.10.12.2"
N1_DATA = "10.10.12.1"
N3_DATA = "10.10.23.2"

MIB_SINGLE = 500
MIB_PER_FLOW = 200
SEGMENT_MIB = 2
REPAIR = 20
WINDOW = 8
IDLE_SEC = 25
RELAY_IDLE = 40
# After send finishes, wait at most this many seconds for recv to exit, then SIGTERM.
POST_SEND_WAIT_S = 35
# Resume: skip first N completed cases (0-based count). Env WH_MATRIX_SKIP overrides.
SKIP_CASES = int(os.environ.get("WH_MATRIX_SKIP", "0"))
FRESH_RUN = os.environ.get("WH_MATRIX_FRESH", "0") == "1"
ACK_ONLY = os.environ.get("WH_MATRIX_ACK_ONLY", "0") == "1"
MATRIX_NAME = os.environ.get("WH_MATRIX_NAME", "wirehair_full_matrix")
if re.fullmatch(r"[A-Za-z0-9_.-]+", MATRIX_NAME) is None:
    raise ValueError("WH_MATRIX_NAME contains unsupported characters")

# Density: denser near WiFi knee, sparse into synthetic overdrive.
RATES = [500, 1000, 2000, 5000]
FLOWS = [1, 2, 4]
TOPOS = ["direct", "relay"]
ACKS = [True] if ACK_ONLY else [False, True]

LOCAL_LOG = Path(__file__).resolve().parents[1] / "build" / f"{MATRIX_NAME}.log"
LOCAL_JSON = Path(__file__).resolve().parents[1] / "build" / f"{MATRIX_NAME}.json"


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


def wait_grep(host, log, pat, n=500):
    for _ in range(n):
        q = pat.replace("'", "'\\''")
        if ssh(host, f"grep -q '{q}' {log}", check=False).returncode == 0:
            return True
        time.sleep(0.1)
    return False


def cleanup():
    for host, pat in (
        (SSH1, "wg_multi_pipeline.*(245|246)"),
        (SSH2, "wire_relay.*245|wg_multi_pipeline.*(245|246)"),
        (SSH3, "wg_multi_pipeline.*(245|246)"),
    ):
        ssh(host, f"pkill -f '{pat}' >/dev/null 2>&1 || true", check=False)
    time.sleep(0.5)


def cleanup_outputs():
    """Delete generated sink files while preserving inputs and checksums."""
    for host in (SSH2, SSH3):
        ssh(
            host,
            f"rm -f {BASE}/*_out {BASE}/*_out_* 2>/dev/null || true",
            check=False,
        )


def prepare():
    ssh(
        SSH1,
        f"""
set -e
mkdir -p {BASE}
python3 - <<'PY'
from pathlib import Path
base = Path("{BASE}")
jobs = [("in_single.bin", {MIB_SINGLE}, 0)] + [
    (f"in_{{i}}.bin", {MIB_PER_FLOW}, i + 1) for i in range(4)
]
for name, mib, salt in jobs:
    path = base / name
    size = mib * 1024 * 1024
    if path.is_file() and path.stat().st_size == size:
        continue
    block = bytes(((j + salt * 17) * 37 + j // 19) & 0xff for j in range(65536))
    with path.open("wb") as f:
        left = size
        while left:
            n = min(left, len(block))
            f.write(block[:n])
            left -= n
    print("wrote", path, path.stat().st_size)
for name, _, _ in jobs:
    path = base / name
    import hashlib
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    (base / (name + ".sha")).write_text(h.hexdigest() + "  " + name + "\\n")
print("done")
PY
ls -lh {BASE}/*.bin
""",
        timeout=1800,
    )
    for host, dest in (
        (SSH2, "fyp1@10.10.10.162"),
        (SSH3, "fyp1@10.10.10.163"),
    ):
        ssh(host, f"mkdir -p {BASE}")
        run(
            [
                "scp",
                "-o",
                "BatchMode=yes",
                f"fyp1@10.10.10.161:{BASE}/in_single.bin.sha",
                *[f"fyp1@10.10.10.161:{BASE}/in_{i}.bin.sha" for i in range(4)],
                f"{dest}:{BASE}/",
            ]
        )


def parse_send(stdout: str, flows: int, completed: bool = True) -> dict:
    row: dict = {}
    t = re.search(r"wall_sec=([0-9.]+)", stdout)
    if t:
        row["wall_s"] = float(t.group(1))
    row["ack_timeouts"] = len(
        re.findall(r"ack_timeout=yes", stdout)
    )
    summaries = re.findall(
        r"wirehair-send: source_bytes=(\d+) segments=\d+ "
        r"repair_sent=(\d+) wire_bytes=(\d+) ack=\w+"
        r"(?: status=\w+)?",
        stdout,
    )
    sources = [int(source) for source, _, _ in summaries]
    repairs = [int(repair) for _, repair, _ in summaries]
    wires = [int(wire) for _, _, wire in summaries]
    round_counts = [
        int(value) for value in re.findall(r"repair_rounds=(\d+)", stdout)
    ]
    window_hwm = [
        int(value) for value in re.findall(r"send_window_hwm=(\d+)", stdout)
    ]
    if repairs:
        row["repair_sent"] = sum(repairs)
        row["repair_per_flow"] = repairs
    if wires:
        row["wire_bytes"] = sum(wires)
    if sources:
        row["source_bytes"] = sum(sources)
    elif completed and flows == 1:
        row["source_bytes"] = MIB_SINGLE * 1024 * 1024
    elif completed:
        row["source_bytes"] = flows * MIB_PER_FLOW * 1024 * 1024
    if completed and row.get("wall_s") and row.get("source_bytes"):
        row["goodput_mbps"] = row["source_bytes"] * 8.0 / row["wall_s"] / 1e6
    if row.get("wall_s") and row.get("wire_bytes"):
        row["wire_mbps"] = row["wire_bytes"] * 8.0 / row["wall_s"] / 1e6
    if round_counts:
        row["repair_rounds"] = sum(round_counts)
    if window_hwm:
        row["send_window_hwm"] = max(window_hwm)
    return row


def verify_outputs(sink_host, flows: int, prefix: str, single: bool) -> dict:
    if single:
        script = f"""
exp=$(cut -d' ' -f1 {BASE}/in_single.bin.sha)
out={prefix}
if [ ! -f "$out" ]; then echo OK_COUNT=0 FAIL_COUNT=1; echo MISSING; exit 0; fi
got=$(sha256sum "$out" | awk '{{print $1}}')
if [ "$exp" = "$got" ]; then echo OK_COUNT=1 FAIL_COUNT=0; else echo OK_COUNT=0 FAIL_COUNT=1; fi
ls -l "$out"
"""
    else:
        script = f"""
ok=0; fail=0
for i in $(seq 0 $(({flows}-1))); do
  exp=$(cut -d' ' -f1 {BASE}/in_$i.bin.sha)
  cand=
  if [ {flows} -eq 1 ]; then
    [ -f "{prefix}" ] && cand="{prefix}"
  fi
  if [ -z "$cand" ]; then
    cand=$(ls -1 {prefix}*flow_$i 2>/dev/null | head -1)
  fi
  if [ -z "$cand" ]; then
    cand=$(ls -1 {prefix}*flow_$i* 2>/dev/null | head -1)
  fi
  if [ -z "$cand" ] || [ ! -f "$cand" ]; then
    echo FLOW_$i=MISSING; fail=$((fail+1)); continue
  fi
  got=$(sha256sum "$cand" | awk '{{print $1}}')
  if [ "$exp" = "$got" ]; then
    echo FLOW_$i=OK; ok=$((ok+1))
  else
    echo FLOW_$i=FAIL; fail=$((fail+1))
  fi
done
echo OK_COUNT=$ok FAIL_COUNT=$fail
"""
    ver = ssh(sink_host, script, check=False)
    row = {"ok_flows": 0, "fail_flows": flows, "verify": "FAIL"}
    m = re.search(r"OK_COUNT=(\d+).*FAIL_COUNT=(\d+)", ver.stdout, re.S)
    if m:
        row["ok_flows"] = int(m.group(1))
        row["fail_flows"] = int(m.group(2))
        row["verify"] = (
            "OK"
            if int(m.group(2)) == 0 and int(m.group(1)) == flows
            else "FAIL"
        )
    row["verify_out"] = ver.stdout[-1500:]
    return row


def one_case(topo: str, ack: bool, flows: int, total_rate: float, port: int) -> dict:
    cleanup()
    ack_flag = "--wh-ack" if ack else "--no-wh-ack"
    rate_label = str(int(total_rate))
    tag = f"{topo}_f{flows}_{'ack' if ack else 'noack'}_r{rate_label}"
    single = flows == 1
    # Use dedicated 500MiB file for single; multi uses per-flow 200MiB.
    use_single_file = single
    prefix = f"{BASE}/{tag}_out"
    rlog = f"{BASE}/{tag}_recv.log"
    slog = f"{BASE}/{tag}_send.log"
    xlog = f"{BASE}/{tag}_relay.log"
    ack_base = ACK_BASE + (port - PORT_BASE)
    per_rate = total_rate / flows
    sink = SSH2 if topo == "direct" else SSH3
    send_host = N2_DIRECT  # both direct and relay: first hop is N2

    for host in (SSH1, SSH2, SSH3):
        ssh(host, f"mkdir -p {BASE}")
    ssh(sink, f"rm -f {prefix} {prefix}_* {rlog}; : > {rlog}")
    ssh(SSH1, f"rm -f {slog}; : > {slog}")
    if topo == "relay":
        ssh(SSH2, f"rm -f {xlog}; : > {xlog}")

    # Receiver
    if use_single_file:
        recv_cmd = f"""
nohup {WG} --codec wirehair --wh-segment-mib={SEGMENT_MIB} --wh-repair-pct={REPAIR} \\
  --wh-window={WINDOW} {ack_flag} \\
  --local-node-id 4 --udp-recv {port} {prefix} --idle-sec {IDLE_SEC} --strict \\
  >{rlog} 2>&1 &
echo $! > {BASE}/{tag}_recv.pid
"""
    else:
        recv_cmd = f"""
nohup {WG} --codec wirehair --wh-segment-mib={SEGMENT_MIB} --wh-repair-pct={REPAIR} \\
  --wh-window={WINDOW} {ack_flag} \\
  --local-node-id 4 --udp-recv {port} {prefix}_ --max-flows {flows} \\
  --idle-sec {IDLE_SEC} --strict \\
  >{rlog} 2>&1 &
echo $! > {BASE}/{tag}_recv.pid
"""
    ssh(sink, recv_cmd)
    if not wait_grep(sink, rlog, "listening", 500):
        print(ssh(sink, f"tail -50 {rlog}", check=False).stdout)
        return {
            "topo": topo,
            "ack": ack,
            "flows": flows,
            "total_rate": rate_label,
            "verify": "RECV_FAIL",
        }

    if topo == "relay":
        # Learned per-flow routes handle ACK return; return-hop is a fallback.
        ssh(
            SSH2,
            f"""
nohup {RELAY} --local-node-id 2 --listen {port} \\
  --next-hop {N3_DATA}:{port} --return-hop {N1_DATA}:{ack_base} \\
  --deferred-per-flow 4096 --deferred-total 32768 --egress-capacity 16384 \\
  --idle-exit-sec {RELAY_IDLE} \\
  >{xlog} 2>&1 &
echo $! > {BASE}/{tag}_relay.pid
""",
        )
        if not wait_grep(SSH2, xlog, "wire-relay: local_node_id", 400):
            print(ssh(SSH2, f"tail -50 {xlog}", check=False).stdout)
            return {
                "topo": topo,
                "ack": ack,
                "flows": flows,
                "total_rate": rate_label,
                "verify": "RELAY_FAIL",
            }

    # Sender
    if use_single_file:
        ack_port_arg = f"--ack-port={ack_base}" if ack else ""
        rate_arg = f"--rate-mbps {per_rate:g}"
        send_script = f"""
set -e
/usr/bin/time -f 'wall_sec=%e' -o {BASE}/{tag}_time.txt \\
  {WG} --codec wirehair --wh-segment-mib={SEGMENT_MIB} --wh-repair-pct={REPAIR} \\
  --wh-window={WINDOW} {ack_flag} {ack_port_arg} \\
  --local-node-id 1 --final-dst 4 --ttl 8 {rate_arg} \\
  --udp-send {send_host} {port} {BASE}/in_single.bin >{slog} 2>&1
cat {BASE}/{tag}_time.txt
echo ---SEND---
cat {slog}
"""
    else:
        flow_args = []
        for i in range(flows):
            flow_args.append(
                f'--flow "{i}:{send_host}:{port}:{BASE}/in_{i}.bin:{per_rate:g}"'
            )
        flow_cli = " ".join(flow_args)
        ack_port_arg = f"--ack-port={ack_base}" if ack else ""
        send_script = f"""
set -e
/usr/bin/time -f 'wall_sec=%e' -o {BASE}/{tag}_time.txt \\
  {WG} --codec wirehair --wh-segment-mib={SEGMENT_MIB} --wh-repair-pct={REPAIR} \\
  --wh-window={WINDOW} {ack_flag} {ack_port_arg} \\
  --local-node-id 1 --final-dst 4 --ttl 8 \\
  --udp-send-multi {flow_cli} >{slog} 2>&1
cat {BASE}/{tag}_time.txt
echo ---SEND---
cat {slog}
"""

    try:
        send = ssh(SSH1, send_script, timeout=900)
    except SystemExit:
        send = ssh(SSH1, f"cat {slog} {BASE}/{tag}_time.txt 2>/dev/null; true", check=False)
        row = {
            "topo": topo,
            "ack": ack,
            "flows": flows,
            "total_rate": rate_label,
            "per_rate": per_rate,
            "verify": "SEND_FAIL",
        }
        row.update(parse_send(send.stdout, flows, completed=False))
        return row

    # Wait for receiver exit (bounded — do not sit on idle-sec for minutes)
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
            f"kill $(cat {BASE}/{tag}_recv.pid) 2>/dev/null || true; "
            f"pkill -f 'wg_multi_pipeline.*{port}' >/dev/null 2>&1 || true",
            check=False,
        )
        time.sleep(0.3)
    if topo == "relay":
        ssh(
            SSH2,
            f"kill $(cat {BASE}/{tag}_relay.pid) 2>/dev/null || true; "
            f"pkill -f 'wire_relay.*{port}' >/dev/null 2>&1 || true",
            check=False,
        )

    row = {
        "topo": topo,
        "ack": ack,
        "flows": flows,
        "total_rate": rate_label,
        "per_rate": round(per_rate, 3),
        "mib_per_flow": MIB_SINGLE if use_single_file else MIB_PER_FLOW,
    }
    row.update(parse_send(send.stdout, flows))
    out_prefix = prefix if use_single_file else f"{prefix}_"
    row.update(verify_outputs(sink, flows, out_prefix, use_single_file))
    recv_info = ssh(
        sink,
        f"grep -E 'socket_rcvbuf=|ahead_window_drops=' {rlog} || true",
        check=False,
    )
    recv_buf = re.search(
        r"socket_rcvbuf=(\d+) requested=(\d+)", recv_info.stdout
    )
    if recv_buf:
        row["socket_rcvbuf"] = int(recv_buf.group(1))
        row["socket_rcvbuf_requested"] = int(recv_buf.group(2))
    ahead_drops = [
        int(value)
        for value in re.findall(r"ahead_window_drops=(\d+)", recv_info.stdout)
    ]
    if ahead_drops:
        row["receiver_ahead_window_drops"] = sum(ahead_drops)

    if topo == "relay":
        rx = ssh(
            SSH2,
            f"grep -E 'drop_|forward|high_watermark|deferred' {xlog} | tail -20 || true",
            check=False,
        )
        row["relay_tail"] = rx.stdout[-800:]
        data_hwm = re.search(
            r"data_egress_high_watermark=(\d+)", rx.stdout
        )
        ack_hwm = re.search(
            r"ack_egress_high_watermark=(\d+)", rx.stdout
        )
        if data_hwm:
            row["relay_data_egress_hwm"] = int(data_hwm.group(1))
        if ack_hwm:
            row["relay_ack_egress_hwm"] = int(ack_hwm.group(1))

    return row


def load_prior_results(console_log: Path) -> list[dict]:
    """Parse already-finished rows from a previous console run."""
    if not console_log.is_file():
        return []
    text = console_log.read_text(errors="replace")
    prior = []
    header_re = re.compile(
        r"=== \[(\d+)/\d+\] topo=(\w+) ack=(True|False) "
        r"flows=(\d+) rate=(\d+)[^\n]*"
    )
    result_re = re.compile(
        r"  -> verify=(\S+) wall=(\S+) gp=(\S+) wire=(\S+) "
        r"repair=(\S+) ok=(\S+)"
    )
    headers = list(header_re.finditer(text))
    for pos, header in enumerate(headers):
        section_end = (
            headers[pos + 1].start() if pos + 1 < len(headers) else len(text)
        )
        result = result_re.search(text, header.end(), section_end)
        if result is None:
            continue
        idx, topo, ack, flows, rate = header.groups()
        ver, wall, gp, wire, repair, ok = result.groups()
        def fnum(x):
            try:
                return float(x) if x not in ("None", "nan") else None
            except ValueError:
                return None
        def inum(x):
            try:
                return int(float(x)) if x not in ("None",) else None
            except ValueError:
                return None
        prior.append(
            {
                "topo": topo,
                "ack": ack == "True",
                "flows": int(flows),
                "total_rate": rate,
                "verify": ver,
                "wall_s": fnum(wall),
                "goodput_mbps": fnum(gp),
                "wire_mbps": fnum(wire),
                "repair_sent": inum(repair),
                "ok_flows": inum(ok),
                "resumed": True,
                "_case_index": int(idx),
            }
        )
    return prior


def main():
    LOCAL_LOG.parent.mkdir(parents=True, exist_ok=True)
    cleanup()
    console = LOCAL_LOG.parent / f"{MATRIX_NAME}.console.log"
    prior = [] if FRESH_RUN else load_prior_results(console)
    skip = SKIP_CASES
    if skip <= 0 and prior:
        skip = max(r["_case_index"] for r in prior)
        print(f"Auto-resume: skipping first {skip} completed cases", flush=True)

    print("Preparing inputs (500 MiB single / 200 MiB ×4)…", flush=True)
    prepare()

    cases = []
    for topo in TOPOS:
        for ack in ACKS:
            for flows in FLOWS:
                for rate in RATES:
                    cases.append((topo, ack, flows, float(rate)))

    print(
        f"Matrix: {len(cases)} cases | skip={skip} | rates={RATES} flows={FLOWS} "
        f"topo={TOPOS} ack={ACKS} | segment={SEGMENT_MIB}MiB "
        f"idle={IDLE_SEC}s post_wait={POST_SEND_WAIT_S}s",
        flush=True,
    )

    results = [r for r in prior if r.get("_case_index", 0) <= skip]
    # keep only up to skip
    results = [r for r in results if r["_case_index"] <= skip]
    # dedupe by index
    by_idx = {r["_case_index"]: r for r in results}
    results = [by_idx[i] for i in sorted(by_idx) if i <= skip]

    port = PORT_BASE + (skip % max(1, (ACK_BASE - PORT_BASE - 20) // 10)) * 10
    if port >= ACK_BASE - 20:
        port = PORT_BASE

    mode = "a" if skip > 0 and LOCAL_LOG.is_file() else "w"
    with LOCAL_LOG.open(mode) as logf:
        if skip > 0:
            logf.write(f"\n==== RESUME skip={skip} ====\n")
        for i, (topo, ack, flows, rate) in enumerate(cases, 1):
            if i <= skip:
                continue
            cleanup_outputs()
            hdr = (
                f"\n=== [{i}/{len(cases)}] topo={topo} ack={ack} "
                f"flows={flows} rate={int(rate)} port={port} ==="
            )
            print(hdr, flush=True)
            logf.write(hdr + "\n")
            logf.flush()
            try:
                row = one_case(topo, ack, flows, rate, port)
            except Exception as e:
                row = {
                    "topo": topo,
                    "ack": ack,
                    "flows": flows,
                    "total_rate": str(int(rate)),
                    "verify": f"EXC:{type(e).__name__}",
                    "error": str(e)[:200],
                }
            row["_case_index"] = i
            results.append(row)
            line = (
                f"  -> verify={row.get('verify')} wall={row.get('wall_s')} "
                f"gp={row.get('goodput_mbps')} wire={row.get('wire_mbps')} "
                f"repair={row.get('repair_sent')} ok={row.get('ok_flows')}"
            )
            print(line, flush=True)
            logf.write(json.dumps(row, ensure_ascii=False) + "\n")
            logf.write(line + "\n")
            logf.flush()
            cleanup_outputs()
            port += 10
            # avoid port wrap into ACK range collision
            if port >= ACK_BASE - 20:
                port = PORT_BASE

        # Summary table
        logf.write("\n==== SUMMARY ====\n")
        hdr = (
            f"{'topo':7} {'ack':3} {'fl':>2} {'rate':>5} {'ver':>8} "
            f"{'wall':>7} {'gp':>8} {'wire':>8} {'repair':>8}"
        )
        print("\n" + hdr, flush=True)
        logf.write(hdr + "\n")
        for r in results:
            wall = r.get("wall_s")
            gp = r.get("goodput_mbps")
            wire = r.get("wire_mbps")
            repair = r.get("repair_sent")
            line = (
                f"{r.get('topo','?'):7} {'on' if r.get('ack') else 'off':3} "
                f"{int(r.get('flows') or 0):2d} {str(r.get('total_rate')):>5} "
                f"{str(r.get('verify')):>8} "
                f"{(wall if wall is not None else float('nan')):7.2f} "
                f"{(gp if gp is not None else float('nan')):8.1f} "
                f"{(wire if wire is not None else float('nan')):8.1f} "
                f"{(repair if repair is not None else -1):8d}"
            )
            print(line, flush=True)
            logf.write(line + "\n")

    LOCAL_JSON.write_text(json.dumps(results, indent=2))
    print(f"\nWrote {LOCAL_JSON} and {LOCAL_LOG}", flush=True)
    cleanup()
    cleanup_outputs()
    fails = sum(1 for r in results if r.get("verify") != "OK")
    print(f"Done: {len(results) - fails}/{len(results)} OK", flush=True)
    return 0 if fails == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
