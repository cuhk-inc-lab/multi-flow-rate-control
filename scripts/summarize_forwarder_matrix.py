#!/usr/bin/env python3
"""Append per-run rows and build aggregate summary for forwarder loss matrix."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import statistics
import sys
from pathlib import Path
from typing import Any

RUN_FIELDS = [
    "run_id",
    "repeat",
    "configured_loss_pct",
    "actual_forwarder_loss_pct",
    "sent_datagrams",
    "received_datagrams",
    "forwarded_datagrams",
    "dropped_datagrams",
    "codec",
    "rs_profile",
    "seed",
    "expected_blocks",
    "completed_blocks",
    "block_completion_pct",
    "wire_shard_loss_pct",
    "missing_groups",
    "late_datagrams",
    "dropped_groups",
    "recovered_groups",
    "groups_received",
    "groups_failed",
    "window_overflow",
    "pending_recovered_groups",
    "skipped_groups",
    "output_bytes",
    "input_sha256",
    "output_sha256",
    "status",
    "source_mbps",
    "wire_mbps",
    "goodput_mbps",
    "elapsed_sec",
    "fail_reason",
    "run_dir",
]

SUMMARY_FIELDS = [
    "configured_loss_pct",
    "codec",
    "rs_profile",
    "total_runs",
    "successful_runs",
    "success_rate_pct",
    "avg_actual_forwarder_loss_pct",
    "min_actual_forwarder_loss_pct",
    "max_actual_forwarder_loss_pct",
    "avg_wire_shard_loss_pct",
    "min_wire_shard_loss_pct",
    "max_wire_shard_loss_pct",
    "avg_block_completion_pct",
    "min_block_completion_pct",
    "max_block_completion_pct",
    "avg_missing_groups",
    "max_missing_groups",
    "avg_late_datagrams",
    "max_late_datagrams",
    "avg_dropped_groups",
    "max_dropped_groups",
    "avg_recovered_groups",
    "max_recovered_groups",
    "avg_groups_failed",
    "max_groups_failed",
    "avg_window_overflow",
    "max_window_overflow",
    "avg_pending_recovered_groups",
    "max_pending_recovered_groups",
    "avg_skipped_groups",
    "max_skipped_groups",
    "avg_goodput_mbps",
    "min_goodput_mbps",
    "max_goodput_mbps",
    "avg_wire_mbps",
    "min_wire_mbps",
    "max_wire_mbps",
]


def na_float(v: Any) -> float | None:
    if v in (None, "", "NA"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def na_int(v: Any) -> int | None:
    if v in (None, "", "NA"):
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def read_flow_row(matrix_dir: Path) -> dict[str, str]:
    flows = matrix_dir / "flows.csv"
    if not flows.is_file():
        return {}
    with flows.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    return rows[0] if rows else {}


def read_case_row(matrix_dir: Path) -> dict[str, str]:
    results = matrix_dir / "results.csv"
    if not results.is_file():
        return {}
    with results.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    return rows[0] if rows else {}


def read_incomplete_stats(matrix_dir: Path) -> dict[str, str]:
    log_dir = matrix_dir / "logs"
    out: dict[str, str] = {}
    if not log_dir.is_dir():
        return out
    for log in sorted(log_dir.glob("*-receiver.log")):
        for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
            if "udp-recv:" not in line or " flow " not in line:
                continue
            for tok in line.split():
                if "=" not in tok:
                    continue
                key, value = tok.split("=", 1)
                if key in (
                    "received_blocks",
                    "expected_blocks",
                    "missing_groups",
                    "recovered_groups",
                    "decoded_blocks",
                    "groups_received",
                    "groups_failed",
                    "window_overflow",
                    "pending_recovered_groups",
                    "dropped_groups",
                    "late",
                    "skipped_groups",
                ):
                    out[key] = value
    return out


def read_input_hash(matrix_dir: Path) -> str:
    for p in sorted(matrix_dir.glob("payloads/*/flow0.sha256")):
        return p.read_text(encoding="utf-8").strip()
    return "NA"


def output_path_from_log(matrix_dir: Path) -> Path | None:
    log_dir = matrix_dir / "logs"
    if not log_dir.is_dir():
        return None
    for log in sorted(log_dir.glob("*-receiver.log")):
        for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
            if "udp-recv:" in line and "output=" in line:
                for tok in line.split():
                    if tok.startswith("output="):
                        return Path(tok.split("=", 1)[1])
    return None


def file_sha256(path: Path | None) -> str:
    if path is None or not path.is_file():
        return "NA"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def wire_mbps_from_shards(sent_shards: int | None, elapsed: float | None, header: int = 40, payload: int = 1400) -> str:
    if sent_shards is None or elapsed is None or elapsed <= 0:
        return "NA"
    bits = sent_shards * (header + payload) * 8
    return f"{bits / elapsed / 1e6:.4f}"


def goodput_mbps(output_bytes: int | None, elapsed: float | None) -> str:
    if output_bytes is None or elapsed is None or elapsed <= 0:
        return "NA"
    return f"{output_bytes * 8 / elapsed / 1e6:.4f}"


def collect_run_row(
    run_id: str,
    repeat: int,
    configured_loss: float,
    codec: str,
    rs_profile: str,
    seed: int,
    source_mbps: float,
    run_dir: Path,
) -> dict[str, str]:
    matrix_dir = run_dir / "matrix"
    fwd_summary_path = run_dir / "forwarder-summary.json"
    flow = read_flow_row(matrix_dir)
    case = read_case_row(matrix_dir)

    fwd: dict[str, Any] = {}
    if fwd_summary_path.is_file():
        fwd = json.loads(fwd_summary_path.read_text(encoding="utf-8"))

    configured_loss_pct = f"{configured_loss * 100.0:.4f}"
    actual_fwd = fwd.get("actual_loss_pct")
    actual_forwarder_loss_pct = (
        f"{float(actual_fwd):.4f}" if actual_fwd is not None else "NA"
    )

    sent_datagrams = str(fwd.get("received", "NA"))
    forwarded_datagrams = str(fwd.get("forwarded", "NA"))
    dropped_datagrams = str(fwd.get("dropped", "NA"))
    received_datagrams = flow.get("received_shards", "NA")

    status = case.get("status") or flow.get("status") or "ERR"
    elapsed = na_float(case.get("elapsed_s"))
    elapsed_sec = f"{elapsed:.3f}" if elapsed is not None else "NA"

    output_bytes = flow.get("output_bytes", "NA")
    out_i = na_int(output_bytes)
    input_sha256 = read_input_hash(matrix_dir)
    out_path = output_path_from_log(matrix_dir)
    output_sha256_val = file_sha256(out_path)

    sent_i = na_int(sent_datagrams)
    wire_mbps = wire_mbps_from_shards(sent_i, elapsed)
    goodput = goodput_mbps(out_i, elapsed)

    incomplete = read_incomplete_stats(matrix_dir)
    if flow.get("missing_groups", "NA") in ("", "NA") and incomplete.get("missing_groups"):
        flow["missing_groups"] = incomplete["missing_groups"]
    if flow.get("completed_blocks", "NA") in ("", "NA") and incomplete.get("received_blocks"):
        flow["completed_blocks"] = incomplete["received_blocks"]
    if flow.get("expected_blocks", "NA") in ("", "NA") and incomplete.get("expected_blocks"):
        flow["expected_blocks"] = incomplete["expected_blocks"]
    for src, dst in (
        ("recovered_groups", "recovered_groups"),
        ("groups_received", "groups_received"),
        ("groups_failed", "groups_failed"),
        ("window_overflow", "window_overflow"),
        ("pending_recovered_groups", "pending_recovered_groups"),
        ("skipped_groups", "skipped_groups"),
        ("dropped_groups", "dropped_groups"),
        ("late", "late_datagrams"),
    ):
        if flow.get(dst, "NA") in ("", "NA") and incomplete.get(src):
            flow[dst] = incomplete[src]

    missing = flow.get("missing_groups", "NA")
    if missing in ("", "NA") and status not in ("PASS", "MARKED"):
        missing = "0"

    return {
        "run_id": run_id,
        "repeat": str(repeat),
        "configured_loss_pct": configured_loss_pct,
        "actual_forwarder_loss_pct": actual_forwarder_loss_pct,
        "sent_datagrams": sent_datagrams,
        "received_datagrams": received_datagrams,
        "forwarded_datagrams": forwarded_datagrams,
        "dropped_datagrams": dropped_datagrams,
        "codec": codec,
        "rs_profile": rs_profile if rs_profile else "—",
        "seed": str(seed),
        "expected_blocks": flow.get("expected_blocks", "NA"),
        "completed_blocks": flow.get("completed_blocks", "NA"),
        "block_completion_pct": flow.get("block_completion_pct", "NA"),
        "wire_shard_loss_pct": flow.get("wire_shard_loss_pct", "NA"),
        "missing_groups": missing,
        "late_datagrams": flow.get("late_datagrams", "NA"),
        "dropped_groups": flow.get("dropped_groups", "NA"),
        "recovered_groups": flow.get("recovered_groups", "NA"),
        "groups_received": flow.get("groups_received", "NA"),
        "groups_failed": flow.get("groups_failed", "NA"),
        "window_overflow": flow.get("window_overflow", "NA"),
        "pending_recovered_groups": flow.get("pending_recovered_groups", "NA"),
        "skipped_groups": flow.get("skipped_groups", "NA"),
        "output_bytes": output_bytes,
        "input_sha256": input_sha256,
        "output_sha256": output_sha256_val,
        "status": status,
        "source_mbps": f"{source_mbps:.4f}",
        "wire_mbps": wire_mbps,
        "goodput_mbps": goodput,
        "elapsed_sec": elapsed_sec,
        "fail_reason": flow.get("fail_reason", case.get("notes", "")),
        "run_dir": str(run_dir),
    }


def append_run_row(result_root: Path, row: dict[str, str]) -> None:
    runs_csv = result_root / "results_runs.csv"
    write_header = not runs_csv.exists() or runs_csv.stat().st_size == 0
    with runs_csv.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=RUN_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow({k: row.get(k, "NA") for k in RUN_FIELDS})


def aggregate(result_root: Path) -> list[dict[str, str]]:
    runs_csv = result_root / "results_runs.csv"
    if not runs_csv.is_file():
        return []
    with runs_csv.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    groups: dict[tuple[str, str, str], list[dict[str, str]]] = {}
    for row in rows:
        key = (row["configured_loss_pct"], row["codec"], row["rs_profile"])
        groups.setdefault(key, []).append(row)

    summaries: list[dict[str, str]] = []

    def avg_field(items: list[dict[str, str]], field: str) -> str:
        vals = [na_float(r.get(field)) for r in items]
        vals = [v for v in vals if v is not None]
        if not vals:
            return "NA"
        return f"{statistics.mean(vals):.4f}"

    def min_field(items: list[dict[str, str]], field: str) -> str:
        vals = [na_float(r.get(field)) for r in items]
        vals = [v for v in vals if v is not None]
        if not vals:
            return "NA"
        return f"{min(vals):.4f}"

    def max_field(items: list[dict[str, str]], field: str) -> str:
        vals = [na_float(r.get(field)) for r in items]
        vals = [v for v in vals if v is not None]
        if not vals:
            return "NA"
        return f"{max(vals):.4f}"

    for (loss_pct, codec, profile), items in sorted(groups.items()):
        total = len(items)
        success = sum(1 for r in items if r.get("status") in ("PASS", "MARKED"))
        summaries.append(
            {
                "configured_loss_pct": loss_pct,
                "codec": codec,
                "rs_profile": profile,
                "total_runs": str(total),
                "successful_runs": str(success),
                "success_rate_pct": f"{100.0 * success / total:.2f}" if total else "NA",
                "avg_actual_forwarder_loss_pct": avg_field(items, "actual_forwarder_loss_pct"),
                "min_actual_forwarder_loss_pct": min_field(items, "actual_forwarder_loss_pct"),
                "max_actual_forwarder_loss_pct": max_field(items, "actual_forwarder_loss_pct"),
                "avg_wire_shard_loss_pct": avg_field(items, "wire_shard_loss_pct"),
                "min_wire_shard_loss_pct": min_field(items, "wire_shard_loss_pct"),
                "max_wire_shard_loss_pct": max_field(items, "wire_shard_loss_pct"),
                "avg_block_completion_pct": avg_field(items, "block_completion_pct"),
                "min_block_completion_pct": min_field(items, "block_completion_pct"),
                "max_block_completion_pct": max_field(items, "block_completion_pct"),
                "avg_missing_groups": avg_field(items, "missing_groups"),
                "max_missing_groups": max_field(items, "missing_groups"),
                "avg_late_datagrams": avg_field(items, "late_datagrams"),
                "max_late_datagrams": max_field(items, "late_datagrams"),
                "avg_dropped_groups": avg_field(items, "dropped_groups"),
                "max_dropped_groups": max_field(items, "dropped_groups"),
                "avg_recovered_groups": avg_field(items, "recovered_groups"),
                "max_recovered_groups": max_field(items, "recovered_groups"),
                "avg_groups_failed": avg_field(items, "groups_failed"),
                "max_groups_failed": max_field(items, "groups_failed"),
                "avg_window_overflow": avg_field(items, "window_overflow"),
                "max_window_overflow": max_field(items, "window_overflow"),
                "avg_pending_recovered_groups": avg_field(
                    items, "pending_recovered_groups"
                ),
                "max_pending_recovered_groups": max_field(
                    items, "pending_recovered_groups"
                ),
                "avg_skipped_groups": avg_field(items, "skipped_groups"),
                "max_skipped_groups": max_field(items, "skipped_groups"),
                "avg_goodput_mbps": avg_field(items, "goodput_mbps"),
                "min_goodput_mbps": min_field(items, "goodput_mbps"),
                "max_goodput_mbps": max_field(items, "goodput_mbps"),
                "avg_wire_mbps": avg_field(items, "wire_mbps"),
                "min_wire_mbps": min_field(items, "wire_mbps"),
                "max_wire_mbps": max_field(items, "wire_mbps"),
            }
        )
    return summaries


def write_summary_csv(result_root: Path, summaries: list[dict[str, str]]) -> Path:
    out = result_root / "results_summary.csv"
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        for row in summaries:
            writer.writerow(row)
    return out


def write_markdown(result_root: Path, summaries: list[dict[str, str]]) -> Path:
    out = result_root / "results.md"
    lines = [
        "# UDP forwarder RS loss tolerance matrix",
        "",
        f"Result root: `{result_root}`",
        "",
        "## Aggregate by configuration",
        "",
        "| Loss % | Codec | RS | Runs | Success | Success % | Avg fwd loss | Avg wire loss | Avg block % | Avg goodput Mbps |",
        "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summaries:
        lines.append(
            f"| {row['configured_loss_pct']} | {row['codec']} | {row['rs_profile']} | "
            f"{row['total_runs']} | {row['successful_runs']} | {row['success_rate_pct']} | "
            f"{row['avg_actual_forwarder_loss_pct']} | {row['avg_wire_shard_loss_pct']} | "
            f"{row['avg_block_completion_pct']} | {row['avg_goodput_mbps']} |"
        )
    lines.extend(["", "See `results_runs.csv` for per-run detail.", ""])
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_add = sub.add_parser("append-run")
    p_add.add_argument("--result-root", type=Path, required=True)
    p_add.add_argument("--run-id", required=True)
    p_add.add_argument("--repeat", type=int, required=True)
    p_add.add_argument("--loss", type=float, required=True)
    p_add.add_argument("--codec", required=True)
    p_add.add_argument("--rs-profile", default="")
    p_add.add_argument("--seed", type=int, required=True)
    p_add.add_argument("--source-mbps", type=float, default=40.0)
    p_add.add_argument("--run-dir", type=Path, required=True)

    p_agg = sub.add_parser("aggregate")
    p_agg.add_argument("--result-root", type=Path, required=True)

    args = parser.parse_args()
    if args.cmd == "append-run":
        row = collect_run_row(
            args.run_id,
            args.repeat,
            args.loss,
            args.codec,
            args.rs_profile,
            args.seed,
            args.source_mbps,
            args.run_dir,
        )
        append_run_row(args.result_root, row)
        print(json.dumps(row, indent=2))
        return 0

    summaries = aggregate(args.result_root)
    write_summary_csv(args.result_root, summaries)
    write_markdown(args.result_root, summaries)
    print(f"Wrote {len(summaries)} summary rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
