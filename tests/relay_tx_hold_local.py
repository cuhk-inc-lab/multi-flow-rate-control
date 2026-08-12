#!/usr/bin/env python3
"""
Local HOL pressure test using --test-tx-hold-us (TEST ONLY).

Compares:
  Phase0 = build/wire_relay_hol_baseline  (inline RX, may block on egress wait)
  Phase1 = build/wire_relay               (deferred RX)

Does not drop packets deliberately; only slows TX after dequeue.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import signal
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / "build"
OUTDIR = BUILD / "bench_logs" / f"local_txhold_{time.strftime('%Y%m%d_%H%M%S')}"
WIRE_MAGIC = 0x57475031
WIRE_VERSION = 3
WIRE_TYPE_DATA = 1
WIRE_HEADER_SIZE = 44


def be64(v: int) -> bytes:
    return struct.pack("!Q", v)


def make_datagram(flow_id: int, block_id: int, final_dst: int = 4, ttl: int = 8,
                  payload: bytes = b"xxxx") -> bytes:
    hdr = bytearray(WIRE_HEADER_SIZE)
    struct.pack_into("!I", hdr, 0, WIRE_MAGIC)
    hdr[4] = WIRE_VERSION
    hdr[5] = WIRE_TYPE_DATA
    hdr[6] = final_dst & 0xFF
    hdr[7] = ttl & 0xFF
    struct.pack_into("!I", hdr, 8, flow_id)
    hdr[12:20] = be64(block_id)
    struct.pack_into("!H", hdr, 20, 0)  # shard_index
    struct.pack_into("!H", hdr, 22, 1)  # shard_count
    struct.pack_into("!H", hdr, 24, len(payload))
    struct.pack_into("!H", hdr, 26, len(payload))
    hdr[28:36] = be64(0)
    hdr[36:44] = be64(0)
    return bytes(hdr) + payload


def pf(line: str, key: str) -> int:
    m = re.search(rf"\b{re.escape(key)}=(\d+)\b", line or "")
    return int(m.group(1)) if m else 0


def summary_line(text: str) -> str:
    last = ""
    for ln in text.splitlines():
        if " summary " in ln and "wire-relay: local_node_id=" in ln:
            last = ln
    return last


def udp_snmp_local() -> dict:
    out = {"InDatagrams": 0, "NoPorts": 0, "InErrors": 0, "RcvbufErrors": 0}
    lines = Path("/proc/net/snmp").read_text().splitlines()
    for i, l in enumerate(lines):
        if l.startswith("Udp:") and i + 1 < len(lines) and lines[i + 1].startswith("Udp:"):
            h = l.split()[1:]
            v = list(map(int, lines[i + 1].split()[1:]))
            d = dict(zip(h, v))
            for k in out:
                out[k] = int(d.get(k, 0))
            break
    return out


def avg_pidstat_cpu(path: Path) -> float:
    if not path.exists():
        return 0.0
    vals = []
    for ln in path.read_text(errors="replace").splitlines():
        if ln.startswith("#") or not ln.strip() or "Linux" in ln:
            continue
        parts = ln.split()
        if len(parts) < 5:
            continue
        try:
            vals.append(float(parts[-3]) if parts[-2].isdigit() else float(parts[-2]))
        except ValueError:
            continue
    return round(sum(vals) / len(vals), 2) if vals else 0.0


def run_one(phase: str, bin_path: Path, cap: int, wait_ms: int, hold_us: int,
            n_pkts: int, listen_port: int, sink_port: int) -> dict:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    run_id = f"{phase}_c{cap}_w{wait_ms}_h{hold_us}_{time.strftime('%H%M%S')}"
    relay_log = OUTDIR / f"{run_id}.relay.log"
    cpu_log = OUTDIR / f"{run_id}.cpu.log"
    print(f"START {run_id} bin={bin_path.name}", flush=True)

    # Absorb next-hop UDP (ignore failures if nothing listens briefly).
    sink = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sink.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sink.bind(("127.0.0.1", sink_port))
    sink.setblocking(False)

    pre = udp_snmp_local()
    cmd = [
        str(bin_path),
        "--local-node-id", "2",
        "--listen", str(listen_port),
        "--next-hop", f"127.0.0.1:{sink_port}",
        "--egress-capacity", str(cap),
        "--egress-wait-ms", str(wait_ms),
        "--test-tx-hold-us", str(hold_us),
        "--idle-exit-sec", "2",
        "--deferred-per-flow", "256",
        "--deferred-total", "1024",
    ]
    with relay_log.open("w") as lf:
        proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT)
    time.sleep(0.4)
    if proc.poll() is not None:
        raise RuntimeError(f"relay exited early:\n{relay_log.read_text()}")

    pidstat = None
    if Path("/usr/bin/pidstat").exists():
        with cpu_log.open("w") as cf:
            pidstat = subprocess.Popen(
                ["pidstat", "-rud", "-p", str(proc.pid), "1"],
                stdout=cf,
                stderr=subprocess.STDOUT,
            )

    # Blast datagrams as fast as possible (non-local final_dst so they forward).
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    t0 = time.time()
    for i in range(n_pkts):
        dgram = make_datagram(flow_id=7, block_id=i, final_dst=4, ttl=8)
        sock.sendto(dgram, ("127.0.0.1", listen_port))
    blast_s = time.time() - t0
    sock.close()

    # Drain sink briefly so sendto path stays healthy.
    end_drain = time.time() + 1.0
    while time.time() < end_drain:
        try:
            sink.recvfrom(65535)
        except BlockingIOError:
            time.sleep(0.01)

    # Stop relay (summary on TERM path via stop + cleanup).
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=15)
        exit_note = "exited_ok"
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
        exit_note = "killed_hang"
    if pidstat is not None:
        pidstat.send_signal(signal.SIGTERM)
        try:
            pidstat.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pidstat.kill()

    # Allow idle-exit / cleanup to flush; if TERM path already printed summary OK.
    time.sleep(0.2)
    post = udp_snmp_local()
    text = relay_log.read_text(errors="replace")
    summ = summary_line(text)
    sink.close()

    row = {
        "run_id": run_id,
        "phase": phase,
        "capacity": cap,
        "wait_ms": wait_ms,
        "hold_us": hold_us,
        "n_pkts": n_pkts,
        "blast_s": round(blast_s, 4),
        "exit": exit_note,
        "egress_waited": pf(summ, "egress_waited"),
        "egress_wait_ns_total": pf(summ, "egress_wait_ns_total"),
        "egress_wait_ns_max": pf(summ, "egress_wait_ns_max"),
        "egress_hwm": pf(summ, "egress_high_watermark"),
        "deferred_hwm": pf(summ, "deferred_hwm"),
        "drop_deferred_flow": pf(summ, "drop_deferred_flow"),
        "drop_deferred_total": pf(summ, "drop_deferred_total"),
        "drop_deferred_table": pf(summ, "drop_deferred_table"),
        "drop_egress_full": pf(summ, "drop_egress_full"),
        "drop_egress_timeout": pf(summ, "drop_egress_timeout"),
        "forward": pf(summ, "forward"),
        "rx": pf(summ, "rx"),
        "summary_present": 1 if summ else 0,
        "udp_InErrors_delta": post["InErrors"] - pre["InErrors"],
        "udp_RcvbufErrors_delta": post["RcvbufErrors"] - pre["RcvbufErrors"],
        "udp_InDatagrams_delta": post["InDatagrams"] - pre["InDatagrams"],
        "udp_NoPorts_delta": post["NoPorts"] - pre["NoPorts"],
        "cpu_avg": avg_pidstat_cpu(cpu_log),
        "relay_log": str(relay_log),
    }
    print(
        f"DONE {run_id} waited={row['egress_waited']} hwm={row['egress_hwm']} "
        f"def_hwm={row['deferred_hwm']} timeout_drop={row['drop_egress_timeout']} "
        f"def_drop={row['drop_deferred_flow']+row['drop_deferred_total']} "
        f"rcvbuf={row['udp_RcvbufErrors_delta']} forward={row['forward']} "
        f"blast_s={row['blast_s']} exit={exit_note}",
        flush=True,
    )
    return row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cap", type=int, default=8)
    ap.add_argument("--wait-ms", type=int, default=2)
    ap.add_argument("--holds", default="100,500,1000")
    ap.add_argument("--pkts", type=int, default=400)
    ap.add_argument("--listen-port", type=int, default=19051)
    ap.add_argument("--sink-port", type=int, default=19052)
    args = ap.parse_args()

    phase1 = BUILD / "wire_relay"
    phase0 = BUILD / "wire_relay_hol_baseline"
    if not phase1.exists() or not phase0.exists():
        print("missing binaries; build with: make wire-relay wire-relay-hol-baseline",
              file=sys.stderr)
        return 1

    holds = [int(x) for x in args.holds.split(",") if x.strip()]
    OUTDIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUTDIR / "summary_local_txhold.csv"
    fields = [
        "run_id", "phase", "capacity", "wait_ms", "hold_us", "n_pkts", "blast_s",
        "exit", "egress_waited", "egress_wait_ns_total", "egress_wait_ns_max",
        "egress_hwm", "deferred_hwm", "drop_deferred_flow", "drop_deferred_total",
        "drop_deferred_table", "drop_egress_full", "drop_egress_timeout",
        "forward", "rx", "summary_present",
        "udp_InErrors_delta", "udp_RcvbufErrors_delta", "udp_InDatagrams_delta",
        "udp_NoPorts_delta", "cpu_avg", "relay_log",
    ]
    rows = []
    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        port_i = 0
        for hold in holds:
            for phase, binary in (("phase1", phase1), ("phase0", phase0)):
                listen = args.listen_port + port_i * 2
                sink = args.sink_port + port_i * 2
                port_i += 1
                row = run_one(phase, binary, args.cap, args.wait_ms, hold,
                              args.pkts, listen, sink)
                rows.append(row)
                w.writerow(row)
                f.flush()
                time.sleep(0.3)

    # Conclusion
    lines = [f"CSV={csv_path}", f"DIR={OUTDIR}", ""]
    for hold in holds:
        p0 = [r for r in rows if r["phase"] == "phase0" and r["hold_us"] == hold]
        p1 = [r for r in rows if r["phase"] == "phase1" and r["hold_us"] == hold]
        if not p0 or not p1:
            continue
        a, b = p0[0], p1[0]
        lines.append(
            f"hold={hold}us cap={args.cap} wait={args.wait_ms}: "
            f"P0 waited={a['egress_waited']} hwm={a['egress_hwm']} "
            f"timeout_drop={a['drop_egress_timeout']} rcvbuf={a['udp_RcvbufErrors_delta']} "
            f"blast_s={a['blast_s']} | "
            f"P1 waited={b['egress_waited']} hwm={b['egress_hwm']} "
            f"def_hwm={b['deferred_hwm']} def_drop="
            f"{b['drop_deferred_flow']+b['drop_deferred_total']} "
            f"timeout_drop={b['drop_egress_timeout']} rcvbuf={b['udp_RcvbufErrors_delta']} "
            f"blast_s={b['blast_s']}"
        )
    p1_any_wait = any(r["egress_waited"] > 0 for r in rows if r["phase"] == "phase1")
    p0_any_wait = any(r["egress_waited"] > 0 for r in rows if r["phase"] == "phase0")
    p1_def = max((r["deferred_hwm"] for r in rows if r["phase"] == "phase1"), default=0)
    lines += [
        "",
        f"egress_waited>0 seen: phase0={p0_any_wait} phase1={p1_any_wait}",
        f"phase1 max deferred_hwm={p1_def}",
        "NOTE: This is a local TX-hold pressure test only; not a claim that "
        "VM HOL is fixed.",
    ]
    concl = OUTDIR / "CONCLUSION.txt"
    concl.write_text("\n".join(lines) + "\n")
    print(concl.read_text())
    return 0


if __name__ == "__main__":
    sys.exit(main())
