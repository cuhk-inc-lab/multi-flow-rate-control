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
DECODE_BLOCK = PKG_SIZE * DATA_SHARDS  # 752

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
    r"udp-recv:\s+flow\s+(?P<fid>\d+)\s+output=(?P<path>\S+)\s+"
    r"output_bytes=(?P<bytes>\d+)\s+datagrams=(?P<dgrams>\d+)\s+"
    r"duplicates=(?P<dup>\d+)\s+late=(?P<late>\d+)\s+malformed=(?P<mal>\d+)\s+"
    r"recovered_groups=(?P<rec>\d+)\s+dropped_groups=(?P<drop>\d+)\s+"
    r"missing_data_shards=(?P<miss>\d+)"
)

LAT_RE = re.compile(
    r"latency\s+(?P<kind>\S+):\s+samples=(?P<samples>\d+)\s+.*?"
    r"p95_us=(?P<p95>[0-9.]+)"
)

OUT_DIR_BY_STREAM = {1: "n4", 2: "n4", 3: "n2", 4: "n2", 5: "n3", 6: "n4"}


def parse_recv_logs(logs_dir: Path) -> tuple[dict[str, dict], dict[int, dict]]:
    """Return (by_output_basename, by_wire_flow_id)."""
    by_base: dict[str, dict] = {}
    by_flow: dict[int, dict] = {}
    for log in sorted(logs_dir.glob("*-recv.log")):
        text = log.read_text(encoding="utf-8", errors="replace")
        chunks = re.split(r"(?=udp-recv: flow )", text)
        for chunk in chunks:
            m = FLOW_LINE_RE.search(chunk)
            if not m:
                continue
            base = Path(m.group("path")).name
            fid = int(m.group("fid"))
            info = {
                "log": log.name,
                "flow_id": fid,
                "output_path": m.group("path"),
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
                "incomplete": False,
                "missing_groups_end": 0,
            }
            for lm in LAT_RE.finditer(chunk):
                if lm.group("kind") == "end_to_end":
                    info["e2e_p95_us"] = float(lm.group("p95"))
                elif lm.group("kind") == "end_to_end_jitter":
                    info["jitter_p95_us"] = float(lm.group("p95"))
            by_base[base] = info
            by_flow[fid] = info

        for im in re.finditer(
            r"udp-recv: flow (?P<fid>\d+) incomplete: received_blocks=(?P<rx>\d+) "
            r"expected_blocks=(?P<exp>\d+) missing_groups=(?P<miss>\d+)",
            text,
        ):
            fid = int(im.group("fid"))
            extra = {
                "incomplete": True,
                "received_blocks": int(im.group("rx")),
                "expected_blocks": int(im.group("exp")),
                "missing_groups_end": int(im.group("miss")),
                "log": log.name,
            }
            if fid in by_flow:
                by_flow[fid].update(extra)
            else:
                by_flow[fid] = extra
    return by_base, by_flow


def expected_blocks(payload_bytes: int) -> int:
    if payload_bytes <= 0:
        return 0
    return (payload_bytes + DECODE_BLOCK - 1) // DECODE_BLOCK


def find_stream_output(
    result_dir: Path,
    stream_id: int,
    payload_path: Path,
    matched: str,
    flow_metrics_by_id: dict[int, dict],
) -> Path | None:
    if matched and matched != "NA":
        p = Path(matched)
        if not p.is_absolute():
            p = result_dir / p
        if p.is_file():
            return p

    # Prefer path recorded in recv log for this wire flow_id
    fm = flow_metrics_by_id.get(stream_id) or {}
    log_path = fm.get("output_path")
    if log_path:
        remote = Path(str(log_path))
        sub = OUT_DIR_BY_STREAM.get(stream_id)
        if sub is not None:
            local = result_dir / "out" / sub / remote.name
            if local.is_file():
                return local
            # also try basename under out/
            for p in (result_dir / "out").rglob(remote.name):
                if p.is_file():
                    return p

    sub = OUT_DIR_BY_STREAM.get(stream_id)
    if sub is None:
        return None
    out_dir = result_dir / "out" / sub
    if not out_dir.is_dir():
        return None

    # Filename tag includes flow_<id>
    tagged = sorted(out_dir.glob(f"*flow_{stream_id}.ts"))
    if len(tagged) == 1:
        return tagged[0]
    if tagged:
        return tagged[0]

    if not payload_path.is_file():
        return None
    want = payload_path.stat().st_size
    cands = [p for p in out_dir.glob("*.ts") if p.is_file() and p.stat().st_size == want]
    return cands[0] if len(cands) == 1 else (cands[0] if cands else None)


def content_mismatch_pct(
    payload: Path, output: Path | None, status: str | None = None
) -> float | None:
    if status and status.upper() == "PASS":
        return 0.0
    if output is None or not payload.is_file() or not output.is_file():
        return None
    try:
        pa = payload.stat().st_size
        pb = output.stat().st_size
    except OSError:
        return None
    if pa == 0 and pb == 0:
        return 0.0
    mism = 0
    n = max(pa, pb)
    with payload.open("rb") as fa, output.open("rb") as fb:
        while True:
            ca = fa.read(1024 * 1024)
            cb = fb.read(1024 * 1024)
            if not ca and not cb:
                break
            if len(ca) != len(cb):
                mism += abs(len(ca) - len(cb))
            for x, y in zip(ca, cb):
                if x != y:
                    mism += 1
    return 100.0 * mism / float(n) if n else 0.0


def loss_stats_for_stream(
    ci: dict[str, Any],
    payload_bytes: int,
    fm: dict,
    mismatch_pct: float | None,
) -> dict[str, Any]:
    blocks = expected_blocks(payload_bytes)
    # Prefer end-of-flow expected_blocks from incomplete line when present
    if fm.get("expected_blocks"):
        try:
            blocks = max(blocks, int(fm["expected_blocks"]))
        except (TypeError, ValueError):
            pass
    shards = int(ci["wire_shards"])
    exp_dgrams = blocks * shards  # DATA shards; END not included
    dgrams = int(fm["datagrams"]) if fm.get("datagrams") is not None else None
    dups = int(fm["duplicates"]) if fm.get("duplicates") is not None else 0
    late = int(fm["late"]) if fm.get("late") is not None else 0
    dropped = int(fm["dropped_groups"]) if fm.get("dropped_groups") is not None else None
    recovered = int(fm["recovered_groups"]) if fm.get("recovered_groups") is not None else None
    missing_shards = (
        int(fm["missing_data_shards"]) if fm.get("missing_data_shards") is not None else None
    )
    # Trailing incomplete groups (never flushed) also count as group loss
    miss_end = int(fm.get("missing_groups_end") or 0)
    if dropped is not None and miss_end:
        dropped = dropped + miss_end
    elif dropped is None and miss_end:
        dropped = miss_end

    unique_rx = None if dgrams is None else max(0, dgrams - dups)
    # Datagrams that arrived but were too late still count as received for loss;
    # loss ≈ missing on the wire relative to expected DATA datagrams.
    dgram_loss_pct = None
    if exp_dgrams > 0 and unique_rx is not None:
        dgram_loss_pct = max(0.0, 100.0 * (1.0 - unique_rx / float(exp_dgrams)))
        if unique_rx >= exp_dgrams:
            dgram_loss_pct = 0.0

    group_drop_pct = None
    if blocks > 0 and dropped is not None:
        group_drop_pct = 100.0 * dropped / float(blocks)

    recovered_pct = None
    if blocks > 0 and recovered is not None:
        recovered_pct = 100.0 * recovered / float(blocks)

    late_pct = None
    if exp_dgrams > 0 and late:
        late_pct = 100.0 * late / float(exp_dgrams)

    return {
        "expected_blocks": blocks,
        "expected_datagrams": exp_dgrams,
        "rx_datagrams": dgrams,
        "unique_rx_datagrams": unique_rx,
        "est_datagram_loss_pct": dgram_loss_pct,
        "dropped_groups": dropped,
        "group_drop_pct": group_drop_pct,
        "recovered_groups": recovered,
        "recovered_pct": recovered_pct,
        "late_datagrams": late,
        "late_pct": late_pct,
        "missing_data_shards": missing_shards,
        "content_mismatch_pct": mismatch_pct,
    }


def fmt_pct(v: float | None, digits: int = 4) -> str:
    if v is None:
        return "NA"
    return f"{v:.{digits}f}"


def fmt_num(v) -> str:
    if v is None:
        return "NA"
    return str(v)


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

    flow_metrics_by_base, flow_metrics_by_id = parse_recv_logs(logs)
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

    # Build per-stream loss rows
    loss_rows: list[dict[str, Any]] = []
    label_of = {1: "s1", 2: "s2", 3: "s3", 4: "s4", 5: "s5", 6: "s6"}

    lines.append("## Streams (integrity / loss / recovery)")
    lines.append("")
    lines.append(
        "| Stream | Status | Src Mbps | Wire Mbps | "
        "Est. dgram loss % | Group drop % | Recovered % | Late % | "
        "Content mismatch % | Rx/Exp dgrams | Dropped groups | Output |"
    )
    lines.append(
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- |"
    )

    for row in stream_rows:
        try:
            sid = int(row.get("stream") or 0)
        except ValueError:
            continue
        label = label_of.get(sid, f"s{sid}")
        payload_path = result_dir / "payloads" / f"{label}.ts"
        matched = row.get("matched_output") or "NA"
        out_path = find_stream_output(
            result_dir, sid, payload_path, matched, flow_metrics_by_id
        )
        base = out_path.name if out_path is not None else (
            Path(matched).name if matched != "NA" else ""
        )
        fm = dict(flow_metrics_by_id.get(sid) or {})
        if not fm and base:
            fm = dict(flow_metrics_by_base.get(base) or {})

        try:
            payload_bytes = int(row.get("payload_bytes") or 0)
        except ValueError:
            payload_bytes = payload_path.stat().st_size if payload_path.is_file() else 0
        if payload_bytes <= 0 and payload_path.is_file():
            payload_bytes = payload_path.stat().st_size

        mismatch = content_mismatch_pct(
            payload_path, out_path, status=str(row.get("status") or "")
        )
        stats = loss_stats_for_stream(ci, payload_bytes, fm, mismatch)
        stats.update(
            {
                "stream": sid,
                "status": row.get("status", ""),
                "rate_mbps": row.get("rate_mbps", ""),
                "output": base or matched,
            }
        )
        loss_rows.append(stats)

        try:
            src_rate = float(row.get("rate_mbps") or 0)
        except ValueError:
            src_rate = 0.0
        est_wire = src_rate * ci["app_expansion"]
        rx = stats["unique_rx_datagrams"]
        exp = stats["expected_datagrams"]
        rx_exp = f"{fmt_num(rx)}/{exp}" if exp else "NA"

        lines.append(
            "| {sid} | {st} | {rate} | {wire:.3f} | {dloss} | {gloss} | {rec} | {late} | "
            "{cm} | {rxexp} | {drop} | `{out}` |".format(
                sid=sid,
                st=row.get("status", ""),
                rate=row.get("rate_mbps", ""),
                wire=est_wire,
                dloss=fmt_pct(stats["est_datagram_loss_pct"]),
                gloss=fmt_pct(stats["group_drop_pct"]),
                rec=fmt_pct(stats["recovered_pct"]),
                late=fmt_pct(stats["late_pct"]),
                cm=fmt_pct(stats["content_mismatch_pct"]),
                rxexp=rx_exp,
                drop=fmt_num(stats["dropped_groups"]),
                out=md_escape(base or matched or "NA"),
            )
        )

    lines.append("")
    lines.append(
        "**How to read loss columns**"
    )
    lines.append("")
    lines.append(
        "- `Est. dgram loss %` ≈ `1 - (rx_datagrams - duplicates) / expected_data_datagrams`, "
        f"where expected = ceil(payload/{DECODE_BLOCK}) × {ci['wire_shards']} "
        f"shards for codec `{ci['codec']}`."
    )
    lines.append(
        "- `Group drop %` = `dropped_groups / expected_blocks` (receiver gave up on a group)."
    )
    lines.append(
        "- `Recovered %` = groups repaired by FEC / expected blocks "
        "(`copy`/`block` usually 0; may still be high if the recover path always runs)."
    )
    lines.append(
        "- `Late %` = late datagrams / expected datagrams (arrived after window advance)."
    )
    lines.append(
        "- `Content mismatch %` = byte-wise diff vs source payload "
        "(0 means hash PASS; >0 with full length means silent corruption)."
    )
    lines.append(
        "- PASS/FAIL is still **sha256 of full file**. Loss % explains *why* a FAIL happened."
    )
    lines.append("")

    # Persist machine-readable loss table
    loss_csv = result_dir / "loss.csv"
    with loss_csv.open("w", encoding="utf-8", newline="") as g:
        w = csv.DictWriter(
            g,
            fieldnames=[
                "stream",
                "status",
                "rate_mbps",
                "expected_blocks",
                "expected_datagrams",
                "rx_datagrams",
                "unique_rx_datagrams",
                "est_datagram_loss_pct",
                "dropped_groups",
                "group_drop_pct",
                "recovered_groups",
                "recovered_pct",
                "late_datagrams",
                "late_pct",
                "missing_data_shards",
                "content_mismatch_pct",
                "output",
            ],
        )
        w.writeheader()
        for r in loss_rows:
            w.writerow({k: r.get(k) for k in w.fieldnames})

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
    lines.append("- `loss.csv` — per-stream estimated datagram/group loss + content mismatch")
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
