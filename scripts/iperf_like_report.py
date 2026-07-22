#!/usr/bin/env python3
"""Build report.md + SVG charts from an iperf-like wire result directory.

Usage:
  python3 iperf_like_report.py RESULT_DIR
"""

from __future__ import annotations

import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

# Must match apps/wg_multi_pipeline/stream_config.h + wire_udp.c
PKG_SIZE = 188
WIRE_HEADER_SIZE = 44
DATAGRAM_APP = WIRE_HEADER_SIZE + PKG_SIZE  # 232 B UDP payload
IP_UDP_HDR = 20 + 8  # typical IPv4 + UDP
DATAGRAM_IP = DATAGRAM_APP + IP_UDP_HDR  # 260 B IP packet (no Ethernet)
DATA_SHARDS = 4

CODEC_SHARDS = {
    "copy": 8,
    "block": 8,
    "xor-fec": 5,  # 4 data + 1 XOR
    "rs-fec": 6,  # 4 data + 2 RS
    "none": 4,
}


def codec_info(codec: str) -> dict[str, Any]:
    codec = (codec or "copy").strip().lower()
    shards = CODEC_SHARDS.get(codec, CODEC_SHARDS["copy"])
    shard_exp = shards / float(DATA_SHARDS)
    app_exp = shard_exp * (DATAGRAM_APP / float(PKG_SIZE))
    ip_exp = shard_exp * (DATAGRAM_IP / float(PKG_SIZE))
    return {
        "codec": codec,
        "data_shards": DATA_SHARDS,
        "wire_shards": shards,
        "shard_expansion": shard_exp,
        "app_expansion": app_exp,
        "ip_expansion": ip_exp,
        "pkg_size": PKG_SIZE,
        "wire_header": WIRE_HEADER_SIZE,
        "datagram_app": DATAGRAM_APP,
        "datagram_ip": DATAGRAM_IP,
    }


def load_meta(result_dir: Path) -> dict[str, Any]:
    meta_path = result_dir / "meta.json"
    if meta_path.is_file():
        try:
            return json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    meta: dict[str, Any] = {"codec": "copy", "streams": []}
    summary = result_dir / "summary.md"
    if summary.is_file():
        text = summary.read_text(encoding="utf-8")
        m = re.search(r"- Codec:\s*(\S+)", text)
        if m:
            meta["codec"] = m.group(1)
        for row in re.finditer(
            r"\|\s*(\d+)\s*\|\s*([^|]+)\|\s*([^|]+)\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|",
            text,
        ):
            meta["streams"].append(
                {
                    "stream": int(row.group(1)),
                    "sender": row.group(2).strip(),
                    "dst": row.group(3).strip(),
                    "rate_mbps": float(row.group(4)),
                    "duration_s": float(row.group(5)),
                }
            )
    streams_csv = result_dir / "streams.csv"
    if streams_csv.is_file():
        rows = list(csv.DictReader(streams_csv.open(encoding="utf-8")))
        if rows:
            meta["streams"] = [
                {
                    "stream": int(r["stream"]),
                    "sender": r.get("sender", ""),
                    "dst": r.get("dst", ""),
                    "rate_mbps": float(r["rate_mbps"]),
                    "duration_s": float(r["duration_s"]),
                }
                for r in rows
                if r.get("stream")
            ]
    return meta


def svg_line_chart(
    series: dict[str, list[tuple[float, float]]],
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

    all_pts: list[tuple[float, float]] = []
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
            f'<text x="{pad_l + 8 + idx * 110}" y="{height - 8}" '
            f'font-family="sans-serif" font-size="11" fill="{color}">{name}</text>'
        )

    parts.append("</svg>")
    return "\n".join(parts)


def load_monitor(path: Path):
    rx: dict[str, list[tuple[float, float]]] = defaultdict(list)
    tx: dict[str, list[tuple[float, float]]] = defaultdict(list)
    cpu: list[tuple[float, float]] = []
    if not path.is_file():
        return rx, tx, cpu
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    if not rows:
        return rx, tx, cpu
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


FLOW_LINE_RE = re.compile(
    r"udp-recv:\s+flow\s+\d+\s+output=(?P<path>\S+)\s+"
    r"output_bytes=(?P<bytes>\d+)\s+datagrams=(?P<dgrams>\d+)\s+"
    r"duplicates=(?P<dup>\d+)\s+late=(?P<late>\d+)\s+malformed=(?P<mal>\d+)\s+"
    r"recovered_groups=(?P<rec>\d+)\s+dropped_groups=(?P<drop>\d+)\s+"
    r"missing_data_shards=(?P<miss>\d+)"
)

LAT_RE = re.compile(
    r"latency\s+(?P<kind>\S+):\s+samples=(?P<samples>\d+)\s+.*?"
    r"p95_us=(?P<p95>[0-9.]+)"
)


def parse_recv_logs(logs_dir: Path) -> dict[str, dict]:
    by_base: dict[str, dict] = {}
    for log in sorted(logs_dir.glob("*-recv.log")):
        text = log.read_text(encoding="utf-8", errors="replace")
        chunks = re.split(r"(?=udp-recv: flow )", text)
        for chunk in chunks:
            m = FLOW_LINE_RE.search(chunk)
            if not m:
                continue
            base = Path(m.group("path")).name
            info = {
                "log": log.name,
                "output_bytes": int(m.group("bytes")),
                "datagrams": int(m.group("dgrams")),
                "duplicates": int(m.group("dup")),
                "late": int(m.group("late")),
                "malformed": int(m.group("mal")),
                "recovered_groups": int(m.group("rec")),
                "dropped_groups": int(m.group("drop")),
                "missing_data_shards": int(m.group("miss")),
                "e2e_p95_us": None,
                "jitter_p95_us": None,
            }
            for lm in LAT_RE.finditer(chunk):
                if lm.group("kind") == "end_to_end":
                    info["e2e_p95_us"] = float(lm.group("p95"))
                elif lm.group("kind") == "end_to_end_jitter":
                    info["jitter_p95_us"] = float(lm.group("p95"))
            by_base[base] = info
    return by_base


def md_escape(s: str) -> str:
    return s.replace("|", "\\|")


def wire_accounting_section(meta: dict[str, Any], ci: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    lines.append("## Wire packet size & expansion")
    lines.append("")
    lines.append(
        "Compared with plain iperf3 (`-b X M` ≈ X Mbps on the wire), this app "
        "sends **small TS shards** plus a **wire header**, and codecs may emit "
        "**extra parity/padding shards**."
    )
    lines.append("")
    lines.append("### Constants")
    lines.append("")
    lines.append("| Item | Value |")
    lines.append("| --- | --- |")
    lines.append(f"| TS package (`PKG_SIZE`) | {ci['pkg_size']} B |")
    lines.append(f"| Wire header (`WIRE_HEADER_SIZE`) | {ci['wire_header']} B |")
    lines.append(
        f"| UDP payload per datagram | {ci['datagram_app']} B "
        f"(= {ci['wire_header']}+{ci['pkg_size']}) |"
    )
    lines.append(
        f"| Typical IPv4+UDP packet | {ci['datagram_ip']} B "
        f"(+{IP_UDP_HDR} B IP/UDP hdr; Ethernet headers not included) |"
    )
    lines.append(f"| Source data shards / group | {ci['data_shards']} |")
    lines.append(
        f"| Wire shards / group (`{ci['codec']}`) | {ci['wire_shards']} |"
    )
    lines.append(
        f"| Shard expansion | ×{ci['shard_expansion']:.3f} "
        f"({ci['wire_shards']}/{ci['data_shards']}) |"
    )
    lines.append(
        f"| App-layer expansion (incl. wire hdr) | ×{ci['app_expansion']:.3f} |"
    )
    lines.append(
        f"| IP-layer expansion (incl. IP/UDP hdr) | ×{ci['ip_expansion']:.3f} |"
    )
    lines.append("")
    lines.append(
        f"**Rule of thumb:** estimated wire Mbps ≈ source Mbps × "
        f"{ci['app_expansion']:.3f} (UDP payload) or × {ci['ip_expansion']:.3f} (IP)."
    )
    lines.append("")

    streams = meta.get("streams") or []
    if streams:
        lines.append("### Per-stream source vs estimated wire rate")
        lines.append("")
        lines.append(
            "| Stream | Source Mbps | Est. wire Mbps (UDP payload) | "
            "Est. wire Mbps (IP) | Duration s |"
        )
        lines.append("| --- | ---: | ---: | ---: | ---: |")
        sum_src = 0.0
        sum_app = 0.0
        sum_ip = 0.0
        for s in streams:
            src = float(s["rate_mbps"])
            app = src * ci["app_expansion"]
            ip = src * ci["ip_expansion"]
            sum_src += src
            sum_app += app
            sum_ip += ip
            lines.append(
                f"| {s['stream']} | {src:g} | {app:.3f} | {ip:.3f} | "
                f"{s['duration_s']:g} |"
            )
        lines.append(
            f"| **Σ concurrent** | **{sum_src:g}** | **{sum_app:.3f}** | "
            f"**{sum_ip:.3f}** | |"
        )
        lines.append("")
        lines.append(
            "Σ assumes all streams overlap (barrier start). Stream 6 may end "
            "earlier if `DURATION_SHORT_S` < `DURATION_S`."
        )
        lines.append("")
        lines.append(
            "When comparing to `run_iperf_like_baseline.sh`, matching source "
            f"`Σ={sum_src:g} Mbps` is **not** equivalent — match the **est. wire** "
            f"column (≈ **{sum_app:.1f} Mbps** UDP payload for this codec/run)."
        )
        lines.append("")
    return lines


def generate(result_dir: Path) -> Path:
    charts = result_dir / "charts"
    charts.mkdir(parents=True, exist_ok=True)
    monitor = result_dir / "monitor"
    logs = result_dir / "logs"
    streams_csv = result_dir / "streams.csv"
    report_path = result_dir / "report.md"

    meta = load_meta(result_dir)
    ci = codec_info(str(meta.get("codec") or "copy"))

    nodes = [
        ("node1", "Node1 (sender)"),
        ("node2", "Node2 (relay/sender)"),
        ("node3", "Node3 (relay)"),
        ("node4", "Node4 (receiver)"),
    ]

    cpu_all: dict[str, list[tuple[float, float]]] = {}
    node_peaks: list[str] = []

    for key, label in nodes:
        csv_path = monitor / f"{key}-ifaces.csv"
        rx, tx, cpu = load_monitor(csv_path)
        if cpu:
            cpu_all[key] = cpu
        if rx:
            (charts / f"{key}-rx.svg").write_text(
                svg_line_chart(rx, f"{label} RX", "Mbps"), encoding="utf-8"
            )
        if tx:
            (charts / f"{key}-tx.svg").write_text(
                svg_line_chart(tx, f"{label} TX", "Mbps"), encoding="utf-8"
            )
        if cpu:
            (charts / f"{key}-cpu.svg").write_text(
                svg_line_chart({"cpu": cpu}, f"{label} CPU", "%", y_is_pct=True),
                encoding="utf-8",
            )
        rx_peak = max((peak(v) for v in rx.values()), default=0.0)
        tx_peak = max((peak(v) for v in tx.values()), default=0.0)
        cpu_peak = peak(cpu)
        node_peaks.append(
            f"| {label} | {rx_peak:.2f} | {tx_peak:.2f} | {cpu_peak:.1f} | "
            f"`monitor/{key}-ifaces.csv` |"
        )

    if cpu_all:
        (charts / "cpu-all.svg").write_text(
            svg_line_chart(cpu_all, "CPU all nodes", "%", y_is_pct=True),
            encoding="utf-8",
        )

    flow_metrics = parse_recv_logs(logs)
    stream_rows: list[dict[str, str]] = []
    if streams_csv.is_file():
        stream_rows = list(csv.DictReader(streams_csv.open(encoding="utf-8")))

    summary_bits = []
    summary_md = result_dir / "summary.md"
    if summary_md.is_file():
        for line in summary_md.read_text(encoding="utf-8").splitlines():
            if line.startswith("- "):
                summary_bits.append(line)

    lines: list[str] = []
    lines.append("# Iperf-like wire test report")
    lines.append("")
    lines.append(f"Result dir: `{result_dir}`")
    lines.append("")
    if summary_bits:
        lines.append("## Run summary")
        lines.append("")
        lines.extend(summary_bits)
        lines.append("")

    lines.extend(wire_accounting_section(meta, ci))

    lines.append("## Streams (integrity / loss / recovery)")
    lines.append("")
    lines.append(
        "| Stream | Status | Source Mbps | Est. wire Mbps | Bytes | Datagrams | Dup | Late | "
        "Malformed | Recovered | Dropped | Missing shards | E2E p95 (us) | Jitter p95 (us) | Output |"
    )
    lines.append(
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
    )

    for row in stream_rows:
        matched = row.get("matched_output") or "NA"
        base = Path(matched).name if matched != "NA" else ""
        fm = flow_metrics.get(base, {})

        def pick(key: str, csv_key: str | None = None) -> str:
            if key in fm and fm[key] is not None:
                return str(fm[key])
            if csv_key and row.get(csv_key) not in (None, ""):
                return str(row[csv_key])
            return "NA"

        try:
            src_rate = float(row.get("rate_mbps") or 0)
        except ValueError:
            src_rate = 0.0
        est_wire = src_rate * ci["app_expansion"]

        lines.append(
            "| {sid} | {st} | {rate} | {wire:.3f} | {nbytes} | {dgrams} | {dup} | {late} | {mal} | "
            "{rec} | {drop} | {miss} | {e2e} | {jit} | `{out}` |".format(
                sid=row.get("stream", ""),
                st=row.get("status", ""),
                rate=row.get("rate_mbps", ""),
                wire=est_wire,
                nbytes=pick("output_bytes", "payload_bytes"),
                dgrams=pick("datagrams"),
                dup=pick("duplicates"),
                late=pick("late"),
                mal=pick("malformed"),
                rec=pick("recovered_groups", "recovered_groups"),
                drop=pick("dropped_groups", "dropped_groups"),
                miss=pick("missing_data_shards", "missing_data_shards"),
                e2e=pick("e2e_p95_us", "e2e_p95_us"),
                jit=pick("jitter_p95_us", "jitter_p95_us"),
                out=md_escape(base or matched),
            )
        )
    lines.append("")
    lines.append(
        "Notes: `Recovered` / `Dropped` / `Missing shards` come from the receiver "
        "FEC/group accounting. For `copy`/`block`, recovered is usually 0; "
        "`xor-fec`/`rs-fec` may show recoveries under loss. "
        "`Est. wire Mbps` uses app-layer expansion (UDP payload including wire header)."
    )
    lines.append("")

    lines.append("## Node peaks")
    lines.append("")
    lines.append("| Node | RX peak (Mbps) | TX peak (Mbps) | CPU peak (%) | Source |")
    lines.append("| --- | ---: | ---: | ---: | --- |")
    lines.extend(node_peaks)
    lines.append("")
    lines.append(
        "Compare TX/RX peaks to the **Σ est. wire Mbps** above. Peaks near that "
        "estimate are consistent; large gaps may indicate pacing, idle gaps, or "
        "measurement on a subset of ifaces."
    )
    lines.append("")

    lines.append("## Charts")
    lines.append("")
    if (charts / "cpu-all.svg").is_file():
        lines.append("### CPU (all nodes)")
        lines.append("")
        lines.append("![CPU all](charts/cpu-all.svg)")
        lines.append("")

    for key, label in nodes:
        lines.append(f"### {label}")
        lines.append("")
        for kind, title in (("cpu", "CPU"), ("rx", "RX"), ("tx", "TX")):
            rel = f"charts/{key}-{kind}.svg"
            if (result_dir / rel).is_file():
                lines.append(f"**{title}**")
                lines.append("")
                lines.append(f"![{label} {title}]({rel})")
                lines.append("")
        lines.append("")

    lines.append("## Raw artifacts")
    lines.append("")
    lines.append("- `meta.json` — codec, rates, durations, expansion knobs")
    lines.append("- `streams.csv` — PASS/FAIL + latency columns")
    lines.append("- `summary.md` — short summary")
    lines.append("- `monitor/*-ifaces.csv` — per-second iface + CPU")
    lines.append("- `logs/*` — sender/receiver logs")
    lines.append("- `out/` — decoded payloads")
    lines.append("- `charts/*.svg` — figures embedded above")
    lines.append("")

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: iperf_like_report.py RESULT_DIR", file=sys.stderr)
        return 2
    path = Path(sys.argv[1])
    if not path.is_dir():
        print(f"error: not a directory: {path}", file=sys.stderr)
        return 1
    out = generate(path)
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
