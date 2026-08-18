#!/usr/bin/env python3
"""Compute wire vs block completion metrics for wire multi-flow matrix."""

from __future__ import annotations

import math
import sys


def as_int(v: str | None) -> int | None:
    if v in (None, "", "NA"):
        return None
    try:
        return int(float(v))
    except ValueError:
        return None


def as_pct(num: int | None, den: int | None) -> str:
    if num is None or den is None or den <= 0:
        return "NA"
    return f"{max(0.0, 100.0 * num / den):.4f}"


def as_loss_pct(received: int | None, sent: int | None) -> str:
    if received is None or sent is None or sent <= 0:
        return "NA"
    return f"{max(0.0, 100.0 * (1.0 - received / sent)):.4f}"


def codec_geometry(codec: str, data_k: int, parity_r: int) -> tuple[int, int]:
    pkg = 1400
    if codec == "none":
        return 1, pkg
    if codec == "xor-fec":
        return 5, 4 * pkg
    if codec == "rs-fec":
        return 6, 4 * pkg
    if codec == "rs":
        return data_k + parity_r, data_k * pkg
    if codec in ("copy", "block"):
        return 4, 4 * pkg
    return 4, 4 * pkg


def compute(
    codec: str,
    payload_bytes: int,
    seen: int | None,
    late: int | None,
    dropped_groups: int | None,
    decoded_blocks: int | None,
    recv_blocks: int | None,
    expect_blocks: int | None,
    data_k: int,
    parity_r: int,
    recovered_groups: int | None = None,
    groups_failed: int | None = None,
    window_overflow: int | None = None,
    pending_recovered_groups: int | None = None,
    skipped_groups: int | None = None,
) -> dict[str, str]:
    shards, input_block = codec_geometry(codec, data_k, parity_r)

    expected_blocks = expect_blocks
    if expected_blocks is None and payload_bytes > 0:
        expected_blocks = math.ceil(payload_bytes / input_block)

    sent_shards = (
        expected_blocks * shards
        if expected_blocks is not None and expected_blocks > 0
        else None
    )
    received_shards = seen
    completed_blocks = decoded_blocks if decoded_blocks is not None else recv_blocks

    return {
        "sent_shards": "NA" if sent_shards is None else str(sent_shards),
        "received_shards": "NA" if received_shards is None else str(received_shards),
        "wire_shard_loss_pct": as_loss_pct(received_shards, sent_shards),
        "expected_blocks": "NA" if expected_blocks is None else str(expected_blocks),
        "completed_blocks": "NA" if completed_blocks is None else str(completed_blocks),
        "block_completion_pct": as_pct(completed_blocks, expected_blocks),
        "late_datagrams": "NA" if late is None else str(late),
        "dropped_groups": "NA" if dropped_groups is None else str(dropped_groups),
        "recovered_groups": "NA" if recovered_groups is None else str(recovered_groups),
        "groups_failed": "NA" if groups_failed is None else str(groups_failed),
        "window_overflow": "NA" if window_overflow is None else str(window_overflow),
        "pending_recovered_groups": "NA"
        if pending_recovered_groups is None
        else str(pending_recovered_groups),
        "skipped_groups": "NA" if skipped_groups is None else str(skipped_groups),
    }


def main() -> int:
    if len(sys.argv) not in (11, 16):
        print(
            "usage: flow_loss_metrics.py CODEC PAYLOAD SEEN LATE DROPPED "
            "DECODED_BLOCKS RECV_BLOCKS EXPECT_BLOCKS RS_K RS_PARITY "
            "[RECOVERED GROUPS_FAILED WINDOW_OVERFLOW PENDING SKIPPED]",
            file=sys.stderr,
        )
        return 2

    codec = sys.argv[1].strip().lower()
    payload = as_int(sys.argv[2]) or 0
    seen = as_int(sys.argv[3])
    late = as_int(sys.argv[4])
    dropped = as_int(sys.argv[5])
    decoded = as_int(sys.argv[6])
    recv_blocks = as_int(sys.argv[7])
    expect_blocks = as_int(sys.argv[8])
    data_k = as_int(sys.argv[9]) or 4
    parity_r = as_int(sys.argv[10]) or 2
    recovered = as_int(sys.argv[11]) if len(sys.argv) > 11 else None
    groups_failed = as_int(sys.argv[12]) if len(sys.argv) > 12 else None
    window_overflow = as_int(sys.argv[13]) if len(sys.argv) > 13 else None
    pending = as_int(sys.argv[14]) if len(sys.argv) > 14 else None
    skipped = as_int(sys.argv[15]) if len(sys.argv) > 15 else None

    metrics = compute(
        codec,
        payload,
        seen,
        late,
        dropped,
        decoded,
        recv_blocks,
        expect_blocks,
        data_k,
        parity_r,
        recovered,
        groups_failed,
        window_overflow,
        pending,
        skipped,
    )
    print(
        metrics["sent_shards"],
        metrics["received_shards"],
        metrics["wire_shard_loss_pct"],
        metrics["expected_blocks"],
        metrics["completed_blocks"],
        metrics["block_completion_pct"],
        metrics["late_datagrams"],
        metrics["dropped_groups"],
        metrics["recovered_groups"],
        metrics["groups_failed"],
        metrics["window_overflow"],
        metrics["pending_recovered_groups"],
        metrics["skipped_groups"],
        sep="\t",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
