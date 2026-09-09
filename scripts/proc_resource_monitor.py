#!/usr/bin/env python3
"""Sample one process CPU% and RSS until the PID exits or a stop file appears.

Usage:
  python3 proc_resource_monitor.py PID HZ OUT_CSV [STOP_FILE]

CSV columns:
  ts,cpu_pct,rss_kb,vsize_kb
"""

from __future__ import annotations

import os
import sys
import time


def read_stat(pid: int) -> tuple[int, int] | None:
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as f:
            raw = f.read()
    except FileNotFoundError:
        return None
    # comm may contain spaces/parentheses; split after last ')'
    rparen = raw.rfind(")")
    if rparen < 0:
        return None
    fields = raw[rparen + 2 :].split()
    # fields[11]=utime, fields[12]=stime (0-based after comm)
    utime = int(fields[11])
    stime = int(fields[12])
    return utime + stime, int(fields[20]) if len(fields) > 20 else 0


def read_status_rss_kb(pid: int) -> int | None:
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
                if line.startswith("VmSize:"):
                    # keep scanning for VmRSS
                    pass
    except FileNotFoundError:
        return None
    return None


def read_status_vsize_kb(pid: int) -> int | None:
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmSize:"):
                    return int(line.split()[1])
    except FileNotFoundError:
        return None
    return None


def clk_tck() -> int:
    return os.sysconf(os.sysconf_names["SC_CLK_TCK"])


def main() -> int:
    if len(sys.argv) not in (4, 5):
        print(
            "usage: proc_resource_monitor.py PID HZ OUT_CSV [STOP_FILE]",
            file=sys.stderr,
        )
        return 2
    pid = int(sys.argv[1])
    hz = float(sys.argv[2])
    out = sys.argv[3]
    stop = sys.argv[4] if len(sys.argv) == 5 else ""

    prev = read_stat(pid)
    if prev is None:
        print(f"pid {pid} not found", file=sys.stderr)
        return 1
    prev_jiffies, _ = prev
    prev_t = time.time()
    ticks = clk_tck()

    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", encoding="utf-8") as g:
        g.write("ts,cpu_pct,rss_kb,vsize_kb\n")

    while True:
        if stop and os.path.exists(stop):
            break
        time.sleep(max(hz, 0.1))
        now = time.time()
        cur = read_stat(pid)
        if cur is None:
            break
        jiffies, _ = cur
        dt = max(now - prev_t, 1e-6)
        dj = max(0, jiffies - prev_jiffies)
        cpu_pct = 100.0 * (dj / ticks) / dt
        rss = read_status_rss_kb(pid)
        vsz = read_status_vsize_kb(pid)
        with open(out, "a", encoding="utf-8") as g:
            g.write(
                f"{now:.3f},{cpu_pct:.2f},{rss if rss is not None else ''},"
                f"{vsz if vsz is not None else ''}\n"
            )
        prev_jiffies = jiffies
        prev_t = now

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
