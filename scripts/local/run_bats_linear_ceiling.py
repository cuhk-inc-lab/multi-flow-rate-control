#!/usr/bin/env python3
"""Find the highest BATS send rate that still recovers a file on the linear VMs.

Each trial is source → relay → relay → dest. Pass means the output SHA-256
matches. The reported capacity is the achieved UDP payload rate on that trial,
not only the --rate-mbps target.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "scripts" / "local" / "run_bats_linear.py"
OUT = REPO / "build" / "bats_linear" / "ceiling"
BYTES = int(os.environ.get("BATS_CEIL_BYTES", str(1024 * 1024)))
RATES = [
    int(x)
    for x in os.environ.get("BATS_CEIL_RATES", "500,1000,2000,4000,8000,12000").split(",")
    if x.strip()
]


def field(text: str, key: str) -> str:
    m = re.search(rf"{key}=(\S+)", text)
    return m.group(1) if m else ""


def one(rate: int, skip_scp: bool) -> dict:
    env = os.environ.copy()
    tag = f"r{rate}"
    env.update(
        {
            "BATS_BYTES": str(BYTES),
            "BATS_RATE": str(rate),
            "BATS_LOSS": "0",
            "BATS_K": "128",
            "BATS_T": "256",
            "BATS_IDLE": "120",
            "BATS_WAIT_SEC": "120",
            "BATS_SRC_TIMEOUT": "240",
            "BATS_TAG": f"ceiling/{tag}",
            "BATS_SKIP_SCP": "1" if skip_scp else "0",
        }
    )
    print(f"\n===== rate target {rate} Mbps  bytes={BYTES} =====", flush=True)
    proc = subprocess.run(
        [sys.executable, str(RUNNER)],
        env=env,
        text=True,
        capture_output=True,
        timeout=400,
    )
    log_dir = REPO / "build" / "bats_linear" / "ceiling" / tag
    (log_dir / "runner.out").write_text(proc.stdout + "\n" + proc.stderr)
    src = (log_dir / "vm1.log").read_text() if (log_dir / "vm1.log").is_file() else ""
    r1 = (log_dir / "vm2.log").read_text() if (log_dir / "vm2.log").is_file() else ""
    r2 = (log_dir / "vm3.log").read_text() if (log_dir / "vm3.log").is_file() else ""
    dst = (log_dir / "vm4.log").read_text() if (log_dir / "vm4.log").is_file() else ""
    row = {
        "rate": rate,
        "pass": proc.returncode == 0 and "PASS linear" in proc.stdout,
        "src_mbps": field(src, "achieved_mbps"),
        "r1_mbps": field(r1, "achieved_mbps"),
        "r2_mbps": field(r2, "achieved_mbps"),
        "src_ms": field(src, "elapsed_ms"),
        "r1_ms": field(r1, "forward_ms"),
        "r2_ms": field(r2, "forward_ms"),
        "partial": field(r1, "partial_batches") + "/" + field(r2, "partial_batches"),
        "goodput": "",
    }
    m = re.search(r"goodput_mbps=([0-9.]+)", proc.stdout)
    if m:
        row["goodput"] = m.group(1)
    print(
        f"result target={rate} pass={row['pass']} src={row['src_mbps']} "
        f"r1={row['r1_mbps']} r2={row['r2_mbps']} goodput={row['goodput']} "
        f"partial={row['partial']}",
        flush=True,
    )
    if proc.returncode != 0:
        tail = (proc.stdout + proc.stderr)[-1500:]
        print(tail, flush=True)
    return row


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    rows = []
    skip = False
    best = None
    for rate in RATES:
        row = one(rate, skip)
        skip = True
        rows.append(row)
        if row["pass"]:
            best = row
        else:
            break
    lines = [
        "target_mbps pass src_mbps relay1_mbps relay2_mbps goodput_mbps src_ms r1_ms r2_ms partial_r1/r2",
    ]
    for row in rows:
        lines.append(
            "{rate} {ok} {src_mbps} {r1_mbps} {r2_mbps} {goodput} {src_ms} {r1_ms} {r2_ms} {partial}".format(
                ok="PASS" if row["pass"] else "FAIL", **row
            )
        )
    text = "\n".join(lines) + "\n"
    (OUT / "summary.txt").write_text(text)
    print("\n" + text)
    if best is None:
        raise SystemExit("no rate recovered the file")
    print(
        f"MAX pass target={best['rate']} Mbps  "
        f"source_achieved={best['src_mbps']} Mbps  "
        f"relay1={best['r1_mbps']} Mbps  relay2={best['r2_mbps']} Mbps  "
        f"e2e_goodput={best['goodput']} Mbps"
    )


if __name__ == "__main__":
    main()
