#!/usr/bin/env python3
"""Sample NIC counters and host CPU; write a CSV timeseries.

Usage:
  python3 iperf_like_monitor.py IFACES HZ OUT_CSV
  IFACES may be empty or "-" for CPU-only sampling.
"""

from __future__ import annotations

import os
import sys
import time


def read_iface(iface: str) -> dict[str, int]:
    base = f"/sys/class/net/{iface}/statistics"
    out: dict[str, int] = {}
    for name in (
        "rx_bytes",
        "tx_bytes",
        "rx_dropped",
        "tx_dropped",
        "rx_errors",
        "tx_errors",
    ):
        with open(f"{base}/{name}", encoding="utf-8") as f:
            out[name] = int(f.read().strip())
    return out


def read_cpu() -> tuple[int, int]:
    """Return (idle+iowait, total) jiffies from /proc/stat aggregate."""
    with open("/proc/stat", encoding="utf-8") as f:
        parts = f.readline().split()
    # user nice system idle iowait irq softirq steal ...
    nums = [int(x) for x in parts[1:]]
    idle = nums[3] + (nums[4] if len(nums) > 4 else 0)
    total = sum(nums)
    return idle, total


def main() -> int:
    if len(sys.argv) != 4:
        print("usage: iperf_like_monitor.py IFACES HZ OUT_CSV", file=sys.stderr)
        return 2
    raw = sys.argv[1].strip()
    ifaces = [] if raw in ("", "-") else raw.split()
    hz = float(sys.argv[2])
    log = sys.argv[3]

    prev_iface: dict[str, dict[str, int]] = {}
    for iface in ifaces:
        path = f"/sys/class/net/{iface}/statistics"
        if os.path.isdir(path):
            prev_iface[iface] = read_iface(iface)

    prev_idle, prev_total = read_cpu()
    prev_t = time.time()

    os.makedirs(os.path.dirname(log) or ".", exist_ok=True)
    with open(log, "w", encoding="utf-8") as g:
        g.write("iface,ts,rx_bps,tx_bps,rx_drop,tx_drop,rx_err,tx_err,cpu_pct\n")

    while True:
        time.sleep(max(hz, 0.2))
        now = time.time()
        dt = max(now - prev_t, 1e-6)

        idle, total = read_cpu()
        didle = idle - prev_idle
        dtotal = total - prev_total
        cpu_pct = 0.0 if dtotal <= 0 else max(0.0, min(100.0, 100.0 * (1.0 - didle / dtotal)))
        prev_idle, prev_total = idle, total

        with open(log, "a", encoding="utf-8") as g:
            if prev_iface:
                for iface, old in list(prev_iface.items()):
                    try:
                        cur = read_iface(iface)
                    except OSError:
                        continue
                    rx_bps = (cur["rx_bytes"] - old["rx_bytes"]) * 8.0 / dt
                    tx_bps = (cur["tx_bytes"] - old["tx_bytes"]) * 8.0 / dt
                    g.write(
                        "%s,%d,%.0f,%.0f,%d,%d,%d,%d,%.2f\n"
                        % (
                            iface,
                            int(now),
                            rx_bps,
                            tx_bps,
                            cur["rx_dropped"],
                            cur["tx_dropped"],
                            cur["rx_errors"],
                            cur["tx_errors"],
                            cpu_pct,
                        )
                    )
                    prev_iface[iface] = cur
            # Always emit a CPU-only row for charting.
            g.write("__cpu__,%d,0,0,0,0,0,0,%.2f\n" % (int(now), cpu_pct))
        prev_t = now


if __name__ == "__main__":
    raise SystemExit(main())
