#!/usr/bin/env python3
"""Parse P1 RS compute-penalty runs and write CSV / Markdown summaries.

Does not talk to the binary except via saved logs. Wire bytes are estimated:
  wire_bytes_est = blocks * n * 1444 + 44
because --udp-send-multi does not log actual wire_bytes (P2).
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
import sys
from pathlib import Path
from typing import Any

PKG_SIZE = 1400
HEADER_SIZE = 44
DATAGRAM_SIZE = PKG_SIZE + HEADER_SIZE  # 1444
INTERLEAVE_DEPTH = 1

RUNS_FIELDS = [
    "run_id",
    "experiment",
    "path_mode",
    "profile",
    "k",
    "r",
    "n",
    "interleave_depth",
    "payload_bytes",
    "target_wire_rate_mbps",
    "source_rate_mbps",
    "loss_target",
    "seed",
    "warmup",
    "rep",
    "sender_node",
    "receiver_node",
    "sender_host",
    "receiver_host",
    "sender_data_ip",
    "receiver_data_ip",
    "final_dst",
    "ttl",
    "local_node_id",
    "forwarder_node",
    "fwd_listen",
    "fwd_dest",
    "hash",
    "overflow",
    "completion_s",
    "payload_goodput_mbps",
    "attempted_payload_goodput_mbps",
    "successful_file_goodput_mbps",
    "wire_bytes_est",
    "wire_bitrate_est_mbps",
    "wire_overhead_pct",
    "theoretical_payload_goodput_mbps",
    "processing_penalty_pct",
    "sender_elapsed_s",
    "sender_user_s",
    "sender_sys_s",
    "sender_cpu_pct",
    "sender_maxrss_kb",
    "receiver_elapsed_s",
    "receiver_user_s",
    "receiver_sys_s",
    "receiver_cpu_pct",
    "receiver_maxrss_kb",
    "forwarder_elapsed_s",
    "forwarder_user_s",
    "forwarder_sys_s",
    "forwarder_cpu_pct",
    "forwarder_maxrss_kb",
    "forwarder_received",
    "forwarder_forwarded",
    "forwarder_dropped",
    "forwarder_actual_loss_pct",
    "encode_p50_us",
    "encode_p95_us",
    "encode_p99_us",
    "decode_p50_us",
    "decode_p95_us",
    "decode_p99_us",
    "e2e_p50_us",
    "e2e_p95_us",
    "e2e_p99_us",
    "sent_shards",
    "recv_shards",
    "wire_shard_loss_pct",
    "recovered_groups",
    "decoded_blocks",
    "groups_failed",
    "window_overflow",
    "git_rev",
    "binary_sha256",
    "nproc",
    "cpu_governor",
    "loadavg",
    "notes",
]


def na() -> str:
    return "NA"


def parse_profile(profile: str) -> tuple[int, int, int]:
    name = profile.strip()
    if name == "none":
        return 1, 0, 1
    # Appendix only: copy preserves four data shards without redundancy.
    if name == "copy":
        return 4, 0, 4
    if "+" not in name:
        raise ValueError(f"unsupported profile {profile!r}")
    k_s, r_s = name.split("+", 1)
    k = int(k_s)
    r = int(r_s)
    if k < 1 or r < 1:
        raise ValueError(f"invalid RS profile {profile!r}")
    return k, r, k + r


def source_rate_mbps(wire_rate: float, k: int, n: int) -> float:
    """Payload pacing that targets wire_rate including 44 B headers."""
    if wire_rate <= 0 or k < 1 or n < 1:
        raise ValueError("wire_rate/k/n must be positive")
    return wire_rate * (k * PKG_SIZE) / (n * DATAGRAM_SIZE)


def theoretical_payload_goodput_mbps(wire_rate: float, k: int, n: int) -> float:
    return source_rate_mbps(wire_rate, k, n)


def fmt_num(value: Any, digits: int = 6) -> str:
    if value is None or value == na():
        return na()
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return na()
        return f"{value:.{digits}f}".rstrip("0").rstrip(".")
    return str(value)


def _read_text(path: Path | None) -> str:
    if path is None or not path.is_file():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def parse_time_file(path: Path | None) -> dict[str, str]:
    out = {
        "elapsed_s": na(),
        "user_s": na(),
        "sys_s": na(),
        "cpu_pct": na(),
        "maxrss_kb": na(),
    }
    text = _read_text(path)
    if not text.strip():
        return out
    kv = {}
    for token in text.replace("\n", " ").split():
        if "=" in token:
            key, val = token.split("=", 1)
            kv[key] = val
    elapsed = kv.get("elapsed_sec")
    user = kv.get("user_sec")
    sysv = kv.get("sys_sec")
    cpu = kv.get("cpu_pct", "").rstrip("%")
    rss = kv.get("maxrss_kb")
    if elapsed and elapsed not in ("?", ""):
        out["elapsed_s"] = elapsed
    if user and user not in ("?", ""):
        out["user_s"] = user
    if sysv and sysv not in ("?", ""):
        out["sys_s"] = sysv
    if cpu and cpu not in ("?", ""):
        out["cpu_pct"] = cpu
    if rss and rss not in ("?", ""):
        out["maxrss_kb"] = rss
    return out


def _search_kv(text: str, key: str) -> str:
    m = re.search(rf"\b{re.escape(key)}=([0-9.]+)", text)
    return m.group(1) if m else na()


def parse_latency(text: str, metric: str) -> dict[str, str]:
    out = {"p50_us": na(), "p95_us": na(), "p99_us": na()}
    m = re.search(
        rf"^latency {re.escape(metric)}:.*\bp50_us=([0-9.]+).*?\bp95_us=([0-9.]+).*?\bp99_us=([0-9.]+)",
        text,
        re.MULTILINE,
    )
    if not m:
        return out
    out["p50_us"], out["p95_us"], out["p99_us"] = m.group(1), m.group(2), m.group(3)
    return out


def parse_sender_log(text: str) -> dict[str, str]:
    out = {"blocks": na(), "source_bytes": na()}
    m = re.search(
        r"wire-multi-send: flow_id=0 blocks=(\d+) source_bytes=(\d+)",
        text,
    )
    if m:
        out["blocks"] = m.group(1)
        out["source_bytes"] = m.group(2)
    return out


def parse_receiver_log(text: str) -> dict[str, str]:
    out = {
        "output_bytes": na(),
        "recv_shards": na(),
        "seen_datagrams": na(),
        "recovered_groups": na(),
        "decoded_blocks": na(),
        "groups_failed": na(),
        "window_overflow": na(),
        "incomplete": "0",
    }
    if re.search(r"udp-recv: flow \d+ incomplete:", text):
        out["incomplete"] = "1"
    m = re.search(
        r"udp-recv: flow \d+ output=\S+ output_bytes=(\d+) datagrams=(\d+) "
        r"seen_datagrams=(\d+) .* recovered_groups=(\d+) .* decoded_blocks=(\d+) "
        r".* groups_failed=(\d+) window_overflow=(\d+)",
        text,
    )
    if m:
        out["output_bytes"] = m.group(1)
        # seen_datagrams includes late arrivals; that is the wire-visible count.
        out["recv_shards"] = m.group(3)
        out["seen_datagrams"] = m.group(3)
        out["recovered_groups"] = m.group(4)
        out["decoded_blocks"] = m.group(5)
        out["groups_failed"] = m.group(6)
        out["window_overflow"] = m.group(7)
        return out
    # Incomplete line still has window_overflow / recovered_groups.
    out["window_overflow"] = _search_kv(text, "window_overflow")
    out["recovered_groups"] = _search_kv(text, "recovered_groups")
    out["decoded_blocks"] = _search_kv(text, "decoded_blocks")
    out["groups_failed"] = _search_kv(text, "groups_failed")
    out["recv_shards"] = _search_kv(text, "seen_datagrams")
    return out


def _f(value: str) -> float | None:
    if value in (None, "", na()):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _i(value: str) -> int | None:
    if value in (None, "", na()):
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def build_row(
    meta: dict[str, Any],
    sender_log: str,
    receiver_log: str,
    sender_time: dict[str, str],
    receiver_time: dict[str, str],
    forwarder_time: dict[str, str] | None = None,
    forwarder_summary: dict[str, Any] | None = None,
) -> dict[str, str]:
    k = int(meta["k"])
    r = int(meta["r"])
    n = int(meta["n"])
    experiment = str(meta["experiment"])
    payload_bytes = int(meta["payload_bytes"])
    notes: list[str] = []
    if meta.get("notes"):
        notes.append(str(meta["notes"]))
    if forwarder_time is None:
        forwarder_time = {
            "elapsed_s": na(),
            "user_s": na(),
            "sys_s": na(),
            "cpu_pct": na(),
            "maxrss_kb": na(),
        }
    fwd_recv = na()
    fwd_fwd = na()
    fwd_drop = na()
    fwd_loss = na()
    if forwarder_summary:
        fwd_recv = forwarder_summary.get("received", na())
        fwd_fwd = forwarder_summary.get("forwarded", na())
        fwd_drop = forwarder_summary.get("dropped", na())
        fwd_loss = forwarder_summary.get("actual_loss_pct", na())
        notes.append(
            f"forwarder_received={fwd_recv} dropped={fwd_drop} actual_loss_pct={fwd_loss}"
        )

    snd = parse_sender_log(sender_log)
    rcv = parse_receiver_log(receiver_log)
    combined_log = receiver_log + "\n" + sender_log
    enc = parse_latency(combined_log, "encode")
    dec = parse_latency(combined_log, "decode")
    e2e = parse_latency(combined_log, "end_to_end")

    hash_status = str(meta.get("hash", na()))
    overflow = rcv["window_overflow"]
    if overflow == na() and rcv["incomplete"] == "1":
        notes.append("receiver_incomplete_overflow_unparsed")

    completion_s = sender_time["elapsed_s"]
    payload_gp = na()
    attempted = na()
    successful = na()
    el = _f(completion_s)
    if el is not None and el > 0 and payload_bytes > 0:
        gp = payload_bytes * 8.0 / el / 1e6
        payload_gp = gp
        attempted = gp
        if hash_status == "PASS":
            successful = gp
        elif hash_status == "FAIL":
            successful = 0.0
            notes.append("hash_FAIL_successful_file_goodput=0")

    blocks = _i(snd["blocks"])
    wire_bytes = na()
    wire_bitrate = na()
    overhead = na()
    if blocks is not None and n >= 1:
        wb = blocks * n * DATAGRAM_SIZE + HEADER_SIZE
        wire_bytes = wb
        notes.append("wire_bytes_est=blocks*n*1444+44")
        if el is not None and el > 0:
            wire_bitrate = wb * 8.0 / el / 1e6
        if payload_bytes > 0:
            overhead = 100.0 * (wb / payload_bytes - 1.0)

    target_w = meta.get("target_wire_rate_mbps", na())
    theoretical = na()
    penalty = na()
    tw = _f(str(target_w)) if str(target_w) not in ("", na(), "None") else None
    if experiment in ("B", "C") and tw is not None and tw > 0:
        theoretical = theoretical_payload_goodput_mbps(tw, k, n)
        ovf_i = _i(overflow)
        gp_m = payload_gp if isinstance(payload_gp, float) else None
        if hash_status != "PASS" or ovf_i != 0:
            penalty = na()
            if hash_status != "PASS":
                notes.append("processing_penalty_pct=NA (hash not PASS)")
        elif gp_m is not None and theoretical > 0:
            penalty = 100.0 * (1.0 - gp_m / theoretical)
            if penalty < 0:
                notes.append("pacing/timer/measurement jitter (penalty_pct<0)")

    if experiment == "A":
        theoretical = na()
        penalty = na()
        notes.append("A: no theoretical_goodput/penalty_pct (not a wire-rate test)")

    ovf_i = _i(overflow)
    if ovf_i is not None and ovf_i > 0:
        if experiment == "A":
            notes.append("queue/processing saturation (window_overflow>0)")
        else:
            notes.append("window_overflow>0")
    if rcv["incomplete"] == "1":
        notes.append("receiver_incomplete")

    sent_shards = na()
    if blocks is not None:
        sent_shards = blocks * n
    recv_shards = rcv["recv_shards"]
    shard_loss = na()
    ss = _i(str(sent_shards)) if sent_shards != na() else None
    rs = _i(recv_shards)
    if ss is not None and ss > 0 and rs is not None:
        shard_loss = 100.0 * (1.0 - rs / ss)

    row = {
        "run_id": meta["run_id"],
        "experiment": experiment,
        "path_mode": meta.get("path_mode", na()),
        "profile": meta["profile"],
        "k": k,
        "r": r,
        "n": n,
        "interleave_depth": INTERLEAVE_DEPTH,
        "payload_bytes": payload_bytes,
        "target_wire_rate_mbps": target_w if target_w is not None else na(),
        "source_rate_mbps": meta.get("source_rate_mbps", na()),
        "loss_target": meta.get("loss_target", na()),
        "seed": meta.get("seed", na()),
        "warmup": meta.get("warmup", 0),
        "rep": meta.get("rep", na()),
        "sender_node": meta.get("sender_node", na()),
        "receiver_node": meta.get("receiver_node", na()),
        "sender_host": meta.get("sender_host", na()),
        "receiver_host": meta.get("receiver_host", na()),
        "sender_data_ip": meta.get("sender_data_ip", na()),
        "receiver_data_ip": meta.get("receiver_data_ip", na()),
        "final_dst": meta.get("final_dst", na()),
        "ttl": meta.get("ttl", na()),
        "local_node_id": meta.get("local_node_id", na()),
        "forwarder_node": meta.get("forwarder_node", na()),
        "fwd_listen": meta.get("fwd_listen", na()),
        "fwd_dest": meta.get("fwd_dest", na()),
        "hash": hash_status,
        "overflow": overflow,
        "completion_s": completion_s,
        "payload_goodput_mbps": fmt_num(payload_gp, 4),
        "attempted_payload_goodput_mbps": fmt_num(attempted, 4),
        "successful_file_goodput_mbps": fmt_num(successful, 4),
        "wire_bytes_est": fmt_num(wire_bytes, 0),
        "wire_bitrate_est_mbps": fmt_num(wire_bitrate, 4),
        "wire_overhead_pct": fmt_num(overhead, 4),
        "theoretical_payload_goodput_mbps": fmt_num(theoretical, 4),
        "processing_penalty_pct": fmt_num(penalty, 4),
        "sender_elapsed_s": sender_time["elapsed_s"],
        "sender_user_s": sender_time["user_s"],
        "sender_sys_s": sender_time["sys_s"],
        "sender_cpu_pct": sender_time["cpu_pct"],
        "sender_maxrss_kb": sender_time["maxrss_kb"],
        "receiver_elapsed_s": receiver_time["elapsed_s"],
        "receiver_user_s": receiver_time["user_s"],
        "receiver_sys_s": receiver_time["sys_s"],
        "receiver_cpu_pct": receiver_time["cpu_pct"],
        "receiver_maxrss_kb": receiver_time["maxrss_kb"],
        "forwarder_elapsed_s": forwarder_time["elapsed_s"],
        "forwarder_user_s": forwarder_time["user_s"],
        "forwarder_sys_s": forwarder_time["sys_s"],
        "forwarder_cpu_pct": forwarder_time["cpu_pct"],
        "forwarder_maxrss_kb": forwarder_time["maxrss_kb"],
        "forwarder_received": fwd_recv,
        "forwarder_forwarded": fwd_fwd,
        "forwarder_dropped": fwd_drop,
        "forwarder_actual_loss_pct": fwd_loss,
        "encode_p50_us": enc["p50_us"],
        "encode_p95_us": enc["p95_us"],
        "encode_p99_us": enc["p99_us"],
        "decode_p50_us": dec["p50_us"],
        "decode_p95_us": dec["p95_us"],
        "decode_p99_us": dec["p99_us"],
        "e2e_p50_us": e2e["p50_us"],
        "e2e_p95_us": e2e["p95_us"],
        "e2e_p99_us": e2e["p99_us"],
        "sent_shards": fmt_num(sent_shards, 0),
        "recv_shards": recv_shards,
        "wire_shard_loss_pct": fmt_num(shard_loss, 4),
        "recovered_groups": rcv["recovered_groups"],
        "decoded_blocks": rcv["decoded_blocks"],
        "groups_failed": rcv["groups_failed"],
        "window_overflow": overflow,
        "git_rev": meta.get("git_rev", na()),
        "binary_sha256": meta.get("binary_sha256", na()),
        "nproc": meta.get("nproc", na()),
        "cpu_governor": meta.get("cpu_governor", na()),
        "loadavg": meta.get("loadavg", na()),
        "notes": "; ".join(notes) if notes else na(),
    }
    for key in RUNS_FIELDS:
        if key not in row or row[key] is None or row[key] == "":
            row[key] = na()
        else:
            row[key] = str(row[key])
    return row


def _load_forwarder_summary(run_dir: Path) -> dict[str, Any] | None:
    path = run_dir / "forwarder-summary.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def append_run(meta_path: Path, runs_csv: Path) -> dict[str, str]:
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    run_dir = meta_path.parent
    sender_log = _read_text(run_dir / "sender.log")
    receiver_log = _read_text(run_dir / "receiver.log")
    sender_time = parse_time_file(run_dir / "sender.time")
    receiver_time = parse_time_file(run_dir / "receiver.time")
    forwarder_time = parse_time_file(run_dir / "forwarder.time")
    forwarder_summary = _load_forwarder_summary(run_dir)
    row = build_row(
        meta,
        sender_log,
        receiver_log,
        sender_time,
        receiver_time,
        forwarder_time,
        forwarder_summary,
    )
    new_file = not runs_csv.is_file() or runs_csv.stat().st_size == 0
    with runs_csv.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=RUNS_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerow(row)
    return row


def _median(values: list[float]) -> str:
    if not values:
        return na()
    return fmt_num(statistics.median(values), 4)


def _floats(rows: list[dict[str, str]], key: str) -> list[float]:
    out: list[float] = []
    for row in rows:
        v = _f(row.get(key, na()))
        if v is not None:
            out.append(v)
    return out


def summarize(runs_csv: Path, out_dir: Path) -> None:
    if not runs_csv.is_file():
        raise SystemExit(f"missing {runs_csv}")
    with runs_csv.open(encoding="utf-8", newline="") as fh:
        rows = [r for r in csv.DictReader(fh)]
    official = [r for r in rows if str(r.get("warmup", "0")) in ("0", "false", "False")]
    summary_csv = out_dir / "summary.csv"
    summary_md = out_dir / "summary.md"

    groups: dict[tuple, list[dict[str, str]]] = {}
    for row in official:
        key = (
            row["experiment"],
            row["profile"],
            row.get("target_wire_rate_mbps", na()),
            row.get("source_rate_mbps", na()),
            row.get("loss_target", na()),
        )
        groups.setdefault(key, []).append(row)

    sum_fields = [
        "experiment",
        "profile",
        "k",
        "r",
        "n",
        "target_wire_rate_mbps",
        "source_rate_mbps",
        "loss_target",
        "n_runs",
        "n_pass",
        "n_fail",
        "hash_pass_rate",
        "median_completion_s",
        "median_payload_goodput_mbps",
        "median_attempted_payload_goodput_mbps",
        "mean_successful_file_goodput_mbps",
        "median_wire_overhead_pct",
        "median_theoretical_payload_goodput_mbps",
        "median_processing_penalty_pct",
        "median_sender_user_s",
        "median_sender_sys_s",
        "median_sender_cpu_pct",
        "median_receiver_user_s",
        "median_receiver_sys_s",
        "median_receiver_cpu_pct",
        "cpu_seconds_per_gib_payload",
        "median_encode_p95_us",
        "median_decode_p95_us",
        "median_e2e_p95_us",
        "median_wire_shard_loss_pct",
        "median_recovered_groups",
        "n_overflow",
    ]
    summaries: list[dict[str, str]] = []
    for key, rs in sorted(groups.items()):
        n_runs = len(rs)
        n_pass = sum(1 for r in rs if r.get("hash") == "PASS")
        n_fail = n_runs - n_pass
        n_ovf = sum(1 for r in rs if (_i(r.get("overflow", na())) or 0) > 0)
        pass_clean = [
            r
            for r in rs
            if r.get("hash") == "PASS" and _i(r.get("overflow", na())) == 0
        ]
        payload = _i(rs[0].get("payload_bytes", na())) or 0
        cpu_per_gib = na()
        if payload > 0:
            pair = []
            for r in rs:
                su = _f(r.get("sender_user_s", na()))
                ss = _f(r.get("sender_sys_s", na()))
                ru = _f(r.get("receiver_user_s", na()))
                rs_ = _f(r.get("receiver_sys_s", na()))
                if None not in (su, ss, ru, rs_):
                    pair.append(su + ss + ru + rs_)
            if pair:
                cpu_per_gib = fmt_num(
                    statistics.median(pair) / (payload / (1024 ** 3)), 4
                )

        succ_vals = []
        for r in rs:
            v = _f(r.get("successful_file_goodput_mbps", na()))
            if v is not None:
                succ_vals.append(v)
        mean_succ = fmt_num(sum(succ_vals) / len(succ_vals), 4) if succ_vals else na()

        penalty_src = pass_clean if rs[0]["experiment"] in ("B", "C") else []
        rec = {
            "experiment": rs[0]["experiment"],
            "profile": rs[0]["profile"],
            "k": rs[0]["k"],
            "r": rs[0]["r"],
            "n": rs[0]["n"],
            "target_wire_rate_mbps": rs[0].get("target_wire_rate_mbps", na()),
            "source_rate_mbps": rs[0].get("source_rate_mbps", na()),
            "loss_target": rs[0].get("loss_target", na()),
            "n_runs": str(n_runs),
            "n_pass": str(n_pass),
            "n_fail": str(n_fail),
            "hash_pass_rate": fmt_num(n_pass / n_runs if n_runs else None, 4),
            "median_completion_s": _median(_floats(rs, "completion_s")),
            "median_payload_goodput_mbps": _median(_floats(rs, "payload_goodput_mbps")),
            "median_attempted_payload_goodput_mbps": _median(
                _floats(rs, "attempted_payload_goodput_mbps")
            ),
            "mean_successful_file_goodput_mbps": mean_succ,
            "median_wire_overhead_pct": _median(_floats(rs, "wire_overhead_pct")),
            "median_theoretical_payload_goodput_mbps": _median(
                _floats(rs, "theoretical_payload_goodput_mbps")
            ),
            "median_processing_penalty_pct": _median(
                _floats(penalty_src, "processing_penalty_pct")
            ),
            "median_sender_user_s": _median(_floats(rs, "sender_user_s")),
            "median_sender_sys_s": _median(_floats(rs, "sender_sys_s")),
            "median_sender_cpu_pct": _median(_floats(rs, "sender_cpu_pct")),
            "median_receiver_user_s": _median(_floats(rs, "receiver_user_s")),
            "median_receiver_sys_s": _median(_floats(rs, "receiver_sys_s")),
            "median_receiver_cpu_pct": _median(_floats(rs, "receiver_cpu_pct")),
            "cpu_seconds_per_gib_payload": cpu_per_gib,
            "median_encode_p95_us": _median(_floats(rs, "encode_p95_us")),
            "median_decode_p95_us": _median(_floats(rs, "decode_p95_us")),
            "median_e2e_p95_us": _median(_floats(rs, "e2e_p95_us")),
            "median_wire_shard_loss_pct": _median(_floats(rs, "wire_shard_loss_pct")),
            "median_recovered_groups": _median(_floats(rs, "recovered_groups")),
            "n_overflow": str(n_ovf),
        }
        summaries.append(rec)

    with summary_csv.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=sum_fields)
        writer.writeheader()
        for rec in summaries:
            writer.writerow(rec)

    def rows_for(exp: str) -> list[dict[str, str]]:
        return [s for s in summaries if s["experiment"] == exp]

    lines = [
        "# RS compute-penalty P1 summary",
        "",
        "P1 uses **estimated** wire bytes: `blocks * n * 1444 + 44`.",
        "`--udp-send-multi` does not log actual `wire_bytes`; that is P2.",
        "",
        "- **Wire overhead** = extra bytes from parity (and 44 B headers), not CPU.",
        "- **Processing penalty** (B/C only, hash PASS and overflow=0) = shortfall vs",
        "  `W * (k*1400)/(n*1444)`. Negative values are jitter, not truncated.",
        "- Experiment **A** has no `penalty_pct` / theoretical goodput.",
        "- `copy` is not in the primary tables.",
        "- `interleave_depth=1` (no interleaving in this tree).",
        "- Strict hash PASS is required for complete-file goodput.",
        "",
        f"Official runs in `runs.csv`: **{len(official)}** (warmup excluded from medians).",
        "",
    ]

    def table(exp: str, cols: list[tuple[str, str]], title: str, extra: str) -> None:
        block = rows_for(exp)
        lines.append(f"## {title}")
        lines.append("")
        lines.append(extra)
        lines.append("")
        if not block:
            lines.append("_No official runs._")
            lines.append("")
            return
        lines.append("| " + " | ".join(c[0] for c in cols) + " |")
        lines.append("| " + " | ".join("---" for _ in cols) + " |")
        for rec in block:
            lines.append("| " + " | ".join(str(rec.get(c[1], na())) for c in cols) + " |")
        lines.append("")

    table(
        "A",
        [
            ("profile", "profile"),
            ("source Mbps", "source_rate_mbps"),
            ("PASS/FAIL", "n_pass"),
            ("n_fail", "n_fail"),
            ("overflow runs", "n_overflow"),
            ("median completion s", "median_completion_s"),
            ("median payload goodput", "median_payload_goodput_mbps"),
            ("CPU s / GiB", "cpu_seconds_per_gib_payload"),
            ("snd user s", "median_sender_user_s"),
            ("rcv user s", "median_receiver_user_s"),
            ("encode p95 us", "median_encode_p95_us"),
            ("decode p95 us", "median_decode_p95_us"),
        ],
        "Experiment A — compute ceiling (loopback, same payload-rate)",
        "No `processing_penalty_pct`. Overflow/hash FAIL is queue/processing saturation, not parity efficiency.",
    )
    table(
        "B",
        [
            ("profile", "profile"),
            ("target W", "target_wire_rate_mbps"),
            ("source rate", "source_rate_mbps"),
            ("PASS", "n_pass"),
            ("FAIL", "n_fail"),
            ("median goodput", "median_payload_goodput_mbps"),
            ("theoretical goodput", "median_theoretical_payload_goodput_mbps"),
            ("wire overhead %", "median_wire_overhead_pct"),
            ("processing penalty %", "median_processing_penalty_pct"),
        ],
        "Experiment B — fixed wire-rate, no loss",
        "Wire overhead and processing penalty are separate columns. Penalty uses PASS && overflow=0 only.",
    )
    table(
        "C",
        [
            ("profile", "profile"),
            ("W", "target_wire_rate_mbps"),
            ("loss", "loss_target"),
            ("pass rate", "hash_pass_rate"),
            ("mean successful-file goodput", "mean_successful_file_goodput_mbps"),
            ("median attempted goodput", "median_attempted_payload_goodput_mbps"),
            ("median shard loss %", "median_wire_shard_loss_pct"),
            ("median recovered groups", "median_recovered_groups"),
            ("snd CPU %", "median_sender_cpu_pct"),
        ],
        "Experiment C — i.i.d. loss, fixed wire-rate, strict hash",
        "Hash FAIL sets successful-file goodput to 0 for that run. Partial output is not success throughput.",
    )

    total_pass = sum(1 for r in official if r.get("hash") == "PASS")
    total_fail = sum(1 for r in official if r.get("hash") != "PASS")
    total_ovf = sum(1 for r in official if (_i(r.get("overflow", na())) or 0) > 0)
    lines.append("## Official run counts")
    lines.append("")
    lines.append(
        f"- PASS={total_pass}  FAIL={total_fail}  overflow_runs={total_ovf}  "
        f"(warmup excluded; processing_penalty median uses PASS && overflow=0 only)"
    )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- `none` is k=1,r=0,n=1; source_rate = W*1400/1444, not W.")
    lines.append("- Latency `decode` is `Codec_decode` at emit, not `Codec_recover`.")
    lines.append(
        "- FAIL successful_file_goodput=0 is not mixed into processing_penalty medians."
    )
    summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def verify_runs(runs_csv: Path, expected_ids: Path) -> int:
    """Return 0 iff CSV matches expected run_ids exactly (order-independent)."""
    if not expected_ids.is_file():
        print(f"error: missing expected ids {expected_ids}", file=sys.stderr)
        return 1
    expected = [
        line.strip()
        for line in expected_ids.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    if not expected:
        print("error: expected_run_ids is empty", file=sys.stderr)
        return 1
    if len(expected) != len(set(expected)):
        print("error: duplicate run_id in expected list", file=sys.stderr)
        return 1
    if not runs_csv.is_file():
        print(f"error: missing {runs_csv}", file=sys.stderr)
        return 1
    with runs_csv.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    actual_ids = [r.get("run_id", "") for r in rows]
    if len(actual_ids) != len(set(actual_ids)):
        dup = sorted({x for x in actual_ids if actual_ids.count(x) > 1})
        print(f"error: duplicate run_id in runs.csv: {dup}", file=sys.stderr)
        return 1
    exp_set = set(expected)
    act_set = set(actual_ids)
    missing = sorted(exp_set - act_set)
    extra = sorted(act_set - exp_set)
    if len(actual_ids) != len(expected) or missing or extra:
        print(
            f"error: run count mismatch expected={len(expected)} actual={len(actual_ids)}",
            file=sys.stderr,
        )
        if missing:
            print(f"  missing: {missing}", file=sys.stderr)
        if extra:
            print(f"  extra: {extra}", file=sys.stderr)
        return 1
    official = [r for r in rows if str(r.get("warmup", "0")) in ("0", "false", "False")]
    print(
        f"verify_ok expected={len(expected)} actual={len(actual_ids)} "
        f"official={len(official)} unique_run_ids=1"
    )
    return 0


def cmd_dry_run(args: argparse.Namespace) -> int:
    profiles = [p.strip() for p in args.profiles.split(",") if p.strip()]
    print("interleave_depth=1")
    print(f"experiment={args.experiment}")
    if args.experiment == "A":
        print(f"payload_rate_mbps={args.payload_rate}")
        print(f"{'profile':<10} {'k':>4} {'r':>4} {'n':>4} {'source_rate':>14}")
        for p in profiles:
            k, r, n = parse_profile(p)
            print(f"{p:<10} {k:>4} {r:>4} {n:>4} {args.payload_rate:>14}")
        return 0
    rates = [float(x) for x in args.wire_rates.split(",") if x.strip()]
    losses = [x.strip() for x in args.losses.split(",") if x.strip()] or ["0"]
    print(
        f"{'profile':<10} {'k':>4} {'r':>4} {'n':>4} {'W':>8} {'source_rate':>14} {'loss':>8}"
    )
    for p in profiles:
        k, r, n = parse_profile(p)
        for w in rates:
            src = source_rate_mbps(w, k, n)
            for loss in losses if args.experiment == "C" else ["0"]:
                print(f"{p:<10} {k:>4} {r:>4} {n:>4} {w:>8g} {src:>14.6f} {loss:>8}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_src = sub.add_parser("source-rate", help="print payload pacing for target W")
    p_src.add_argument("--k", type=int, required=True)
    p_src.add_argument("--n", type=int, required=True)
    p_src.add_argument("--wire-rate", type=float, required=True)

    p_dry = sub.add_parser("dry-run")
    p_dry.add_argument("--experiment", required=True, choices=["A", "B", "C"])
    p_dry.add_argument("--profiles", required=True)
    p_dry.add_argument("--wire-rates", default="10,20,50")
    p_dry.add_argument("--payload-rate", default="NA")
    p_dry.add_argument("--losses", default="0,0.01,0.02,0.05")

    p_app = sub.add_parser("append")
    p_app.add_argument("--meta", type=Path, required=True)
    p_app.add_argument("--runs-csv", type=Path, required=True)

    p_sum = sub.add_parser("summarize")
    p_sum.add_argument("--runs-csv", type=Path, required=True)
    p_sum.add_argument("--out-dir", type=Path, required=True)

    p_ver = sub.add_parser("verify")
    p_ver.add_argument("--runs-csv", type=Path, required=True)
    p_ver.add_argument("--expected-ids", type=Path, required=True)

    args = parser.parse_args(argv or sys.argv[1:])
    if args.cmd == "source-rate":
        print(f"{source_rate_mbps(args.wire_rate, args.k, args.n):.6f}")
        return 0
    if args.cmd == "dry-run":
        return cmd_dry_run(args)
    if args.cmd == "append":
        row = append_run(args.meta, args.runs_csv)
        print(json.dumps(row, indent=2))
        return 0
    if args.cmd == "summarize":
        args.out_dir.mkdir(parents=True, exist_ok=True)
        summarize(args.runs_csv, args.out_dir)
        print(f"wrote {args.out_dir / 'summary.csv'} and {args.out_dir / 'summary.md'}")
        return 0
    if args.cmd == "verify":
        return verify_runs(args.runs_csv, args.expected_ids)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
