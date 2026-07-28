#!/usr/bin/env python3
"""Build SVG CPU / RX / TX charts from wire-stress monitor CSVs.

Usage:
  python3 wire_stress_charts.py RESULT_DIR

Writes charts/*.svg and returns markdown image links for results.md.
"""
from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


Series = Dict[str, List[Tuple[float, float]]]


def svg_line_chart(
    series: Series,
    title: str,
    ylabel: str,
    width: int = 720,
    height: int = 280,
    y_is_pct: bool = False,
) -> str:
    colors = [
        "#2563eb",
        "#dc2626",
        "#16a34a",
        "#ca8a04",
        "#9333ea",
        "#0891b2",
        "#ea580c",
        "#4b5563",
    ]
    pad_l, pad_r, pad_t, pad_b = 56, 16, 28, 40
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b

    all_pts: List[Tuple[float, float]] = []
    for pts in series.values():
        all_pts.extend(pts)
    if not all_pts:
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">'
            f'<text x="20" y="40" fill="#666">no data: {title}</text></svg>'
        )

    xs = [p[0] for p in all_pts]
    ys = [p[1] for p in all_pts]
    xmin, xmax = min(xs), max(xs)
    ymin, ymax = min(ys), max(ys)
    if y_is_pct:
        ymin, ymax = 0.0, 100.0
    if xmax <= xmin:
        xmax = xmin + 1.0
    if ymax <= ymin:
        ymax = ymin + 1.0
    if not y_is_pct:
        span = ymax - ymin
        ymin = max(0.0, ymin - span * 0.05)
        ymax = ymax + span * 0.08

    def sx(x: float) -> float:
        return pad_l + (x - xmin) / (xmax - xmin) * plot_w

    def sy(y: float) -> float:
        return pad_t + (1.0 - (y - ymin) / (ymax - ymin)) * plot_h

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#fafafa"/>',
        f'<text x="{pad_l}" y="18" font-family="sans-serif" font-size="14" '
        f'font-weight="600" fill="#111">{title}</text>',
        f'<text x="14" y="{pad_t + plot_h / 2}" font-family="sans-serif" font-size="11" '
        f'fill="#555" transform="rotate(-90 14,{pad_t + plot_h / 2})">{ylabel}</text>',
        f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t + plot_h}" '
        f'stroke="#ccc"/>',
        f'<line x1="{pad_l}" y1="{pad_t + plot_h}" x2="{pad_l + plot_w}" '
        f'y2="{pad_t + plot_h}" stroke="#ccc"/>',
    ]

    for i in range(5):
        yv = ymin + (ymax - ymin) * i / 4.0
        y = sy(yv)
        label = f"{yv:.0f}" if (y_is_pct or ymax >= 10) else f"{yv:.1f}"
        parts.append(
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{pad_l + plot_w}" y2="{y:.1f}" '
            f'stroke="#eee"/>'
        )
        parts.append(
            f'<text x="{pad_l - 6}" y="{y + 4:.1f}" text-anchor="end" '
            f'font-family="sans-serif" font-size="10" fill="#666">{label}</text>'
        )

    for i in range(5):
        xv = xmin + (xmax - xmin) * i / 4.0
        x = sx(xv)
        parts.append(
            f'<text x="{x:.1f}" y="{pad_t + plot_h + 18}" text-anchor="middle" '
            f'font-family="sans-serif" font-size="10" fill="#666">{xv:.0f}s</text>'
        )

    for idx, (name, pts) in enumerate(series.items()):
        if len(pts) < 2:
            continue
        color = colors[idx % len(colors)]
        d = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in pts)
        parts.append(
            f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{d}"/>'
        )
        parts.append(
            f'<text x="{pad_l + 8 + (idx % 6) * 110}" y="{height - 8 - 12 * (idx // 6)}" '
            f'font-family="sans-serif" font-size="11" fill="{color}">{name}</text>'
        )

    parts.append("</svg>")
    return "\n".join(parts)


def load_monitor(path: Path):
    rx: dict[str, list[tuple[float, float]]] = defaultdict(list)
    tx: dict[str, list[tuple[float, float]]] = defaultdict(list)
    cpu: list[tuple[float, float]] = []
    if not path.is_file():
        return dict(rx), dict(tx), cpu
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    if not rows:
        return dict(rx), dict(tx), cpu
    t0 = min(int(r["ts"]) for r in rows if r.get("ts"))
    for r in rows:
        try:
            t_rel = float(int(r["ts"]) - t0)
            iface = r["iface"]
            cpu_pct = float(r["cpu_pct"]) if r.get("cpu_pct") not in (None, "") else 0.0
            if iface == "__cpu__":
                cpu.append((t_rel, cpu_pct))
                continue
            rx[iface].append((t_rel, float(r["rx_bps"]) / 1e6))
            tx[iface].append((t_rel, float(r["tx_bps"]) / 1e6))
        except (KeyError, ValueError):
            continue
    return dict(rx), dict(tx), cpu


def peak(series: Iterable[tuple[float, float]]) -> float:
    vals = [y for _, y in series]
    return max(vals) if vals else 0.0


def build_charts(result_dir: Path) -> List[str]:
    """Write SVG charts under result_dir/charts/. Return markdown lines."""
    mon_dir = result_dir / "monitor"
    charts = result_dir / "charts"
    charts.mkdir(parents=True, exist_ok=True)

    csvs = sorted(mon_dir.glob("*.csv")) if mon_dir.is_dir() else []
    if not csvs:
        return ["- (no monitor CSV; charts skipped)"]

    md: List[str] = []
    cpu_all: Series = {}
    summary_rows: List[str] = []

    for csv_path in csvs:
        key = csv_path.stem
        rx, tx, cpu = load_monitor(csv_path)
        label = key

        if cpu:
            cpu_all[key] = cpu
            (charts / f"{key}-cpu.svg").write_text(
                svg_line_chart({"cpu": cpu}, f"{label} CPU", "%", y_is_pct=True),
                encoding="utf-8",
            )
        if rx:
            (charts / f"{key}-rx.svg").write_text(
                svg_line_chart(rx, f"{label} RX", "Mbps"),
                encoding="utf-8",
            )
        if tx:
            (charts / f"{key}-tx.svg").write_text(
                svg_line_chart(tx, f"{label} TX", "Mbps"),
                encoding="utf-8",
            )

        rx_peak = max((peak(v) for v in rx.values()), default=0.0)
        tx_peak = max((peak(v) for v in tx.values()), default=0.0)
        cpu_peak = peak(cpu)
        summary_rows.append(
            f"| {key} | {cpu_peak:.1f} | {rx_peak:.1f} | {tx_peak:.1f} |"
        )

        md.append(f"### {label}")
        md.append("")
        if (charts / f"{key}-cpu.svg").is_file():
            md.append(f"![CPU {label}](charts/{key}-cpu.svg)")
            md.append("")
        if (charts / f"{key}-rx.svg").is_file():
            md.append(f"![RX {label}](charts/{key}-rx.svg)")
            md.append("")
        if (charts / f"{key}-tx.svg").is_file():
            md.append(f"![TX {label}](charts/{key}-tx.svg)")
            md.append("")

    if len(cpu_all) > 1:
        (charts / "cpu-all.svg").write_text(
            svg_line_chart(cpu_all, "CPU all nodes", "%", y_is_pct=True),
            encoding="utf-8",
        )
        md.insert(0, "")
        md.insert(0, "![CPU all nodes](charts/cpu-all.svg)")
        md.insert(0, "")
        md.insert(0, "### CPU overlay")

    head = [
        "### Peak summary",
        "",
        "| node | CPU peak % | RX peak Mbps | TX peak Mbps |",
        "| --- | ---: | ---: | ---: |",
        *summary_rows,
        "",
    ]
    return head + md


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: wire_stress_charts.py RESULT_DIR", file=sys.stderr)
        return 2
    result_dir = Path(sys.argv[1])
    lines = build_charts(result_dir)
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
