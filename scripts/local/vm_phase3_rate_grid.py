#!/usr/bin/env python3
"""Phase3 retest: fixed rate grid (no binary search).

Rates: 2000..10000 step 500 (includes 4500/5000).
Pass: checksum OK, or byte completion >= 98%.
Default paths: hop3_kernel + hop3_relay; all codecs from ceiling search.

Env:
  WH_TOPO=linear|vxlan
  WH_CEIL_MIB=200
  WH_P3G_NAME=linear_phase3_rate_grid
  WH_P3G_FRESH=1
  WH_P3G_PATHS=hop3_kernel,hop3_relay
  WH_P3G_CODECS=copy,block,...   (optional subset)
"""

from __future__ import annotations

import csv
import json
import os
import sys
from pathlib import Path

os.environ.setdefault("WH_TOPO", "linear")
os.environ.setdefault("WH_CEIL_MIB", "200")

sys.path.insert(0, str(Path(__file__).resolve().parent))
import vm_wire_ceiling_search as m  # noqa: E402

RATES = [
    int(x)
    for x in os.environ.get(
        "WH_P3G_RATES",
        "2000,2500,3000,3500,4000,4500,5000,5500,6000,6500,7000,7500,8000,8500,9000,9500,10000",
    ).split(",")
    if x.strip()
]
PASS_COMPLETION = float(os.environ.get("WH_P3G_PASS_COMPLETION", "98"))
EXPECTED = m.FILE_MIB * 1024 * 1024

NAME = os.environ.get("WH_P3G_NAME", "linear_phase3_rate_grid")
FRESH = os.environ.get("WH_P3G_FRESH", "0") == "1"
PATH_FILTER = {
    x.strip()
    for x in os.environ.get("WH_P3G_PATHS", "hop3_kernel,hop3_relay").split(",")
    if x.strip()
}
CODEC_FILTER = {
    x.strip()
    for x in os.environ.get("WH_P3G_CODECS", "").split(",")
    if x.strip()
}

REPO = Path(__file__).resolve().parents[2]
OUT_JSON = REPO / "build" / f"{NAME}.json"
OUT_PROBES = REPO / "build" / f"{NAME}_probes.json"
OUT_CSV = REPO / "build" / f"{NAME}.csv"
OUT_MD = REPO / "build" / "report-data" / f"{NAME}.md"


def is_pass(row: dict) -> bool:
    if row.get("verify") == "OK":
        return True
    return float(row.get("completion_pct") or 0) >= PASS_COMPLETION


def summarize(probes: list[dict]) -> list[dict]:
    keys = sorted({(p["path"], p["codec_mode"]) for p in probes})
    results = []
    for path, codec in keys:
        subset = [p for p in probes if p["path"] == path and p["codec_mode"] == codec]
        passes = [p for p in subset if p.get("pass_ge98")]
        best = max(passes, key=lambda x: float(x.get("goodput_mbps") or 0)) if passes else None
        max_rate = max((p["rate"] for p in passes), default=None)
        first_fail = next((p for p in subset if not p.get("pass_ge98")), None)
        results.append(
            {
                "path": path,
                "codec_mode": codec,
                "codec": subset[0].get("codec"),
                "wh_ack": subset[0].get("wh_ack"),
                "n_probes": len(subset),
                "n_pass_ge98": len(passes),
                "max_pass_rate_mbps": max_rate,
                "first_fail_rate_mbps": first_fail["rate"] if first_fail else None,
                "best_goodput_mbps": None if best is None else best.get("goodput_mbps"),
                "best_goodput_at_rate": None if best is None else best.get("rate"),
                "ceiling_mbps": None if best is None else best.get("goodput_mbps"),
                "best_completion_pct": None if best is None else best.get("completion_pct"),
                "best_verify": None if best is None else best.get("verify"),
            }
        )
    return results


def write_md(results: list[dict], probes: list[dict]) -> None:
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Phase3 rate grid ({NAME})",
        "",
        f"- topo: `{os.environ.get('WH_TOPO')}`",
        f"- file: `{m.FILE_MIB}` MiB",
        f"- rates: `{RATES}`",
        f"- pass: checksum OK **or** completion ≥ {PASS_COMPLETION:.0f}%",
        "",
        "## Ceiling summary",
        "",
        "| path | codec | max_pass_rate | ceiling goodput | best@rate | first_fail | pass/total |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for r in results:
        lines.append(
            f"| {r['path']} | {r['codec_mode']} | {r.get('max_pass_rate_mbps')} | "
            f"{None if r.get('ceiling_mbps') is None else round(r['ceiling_mbps'],1)} | "
            f"{r.get('best_goodput_at_rate')} | {r.get('first_fail_rate_mbps')} | "
            f"{r.get('n_pass_ge98')}/{r.get('n_probes')} |"
        )
    lines += [
        "",
        "## All probes",
        "",
        "| path | codec | rate | verify | completion% | goodput | wire_thru | link_tx | link_rx | pass≥98 |",
        "|---|---|---:|---|---:|---:|---:|---:|---:|---|",
    ]
    for p in probes:
        def _r(k):
            v = p.get(k)
            return None if v is None else round(float(v), 1)

        lines.append(
            f"| {p['path']} | {p['codec_mode']} | {p['rate']} | {p.get('verify')} | "
            f"{p.get('completion_pct')} | {_r('goodput_mbps')} | {_r('wire_mbps')} | "
            f"{_r('link_tx_mbps')} | {_r('link_rx_mbps')} | {p.get('pass_ge98')} |"
        )
    OUT_MD.write_text("\n".join(lines) + "\n")


def save(results: list[dict], probes: list[dict]) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(results, indent=2) + "\n")
    OUT_PROBES.write_text(json.dumps(probes, indent=2) + "\n")
    fields = [
        "path",
        "codec_mode",
        "max_pass_rate_mbps",
        "first_fail_rate_mbps",
        "best_goodput_mbps",
        "best_goodput_at_rate",
        "ceiling_mbps",
        "n_pass_ge98",
        "n_probes",
    ]
    with OUT_CSV.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in results:
            w.writerow(r)
    write_md(results, probes)


def main() -> None:
    paths = [p for p in m.PATHS if p[0] in PATH_FILTER]
    codecs = [c for c in m.CODEC_MODES if (not CODEC_FILTER or c[0] in CODEC_FILTER)]
    if not paths:
        raise SystemExit(f"no paths matched {PATH_FILTER}")
    configs = [(p, c) for p in paths for c in codecs]
    total = len(configs) * len(RATES)
    print(
        f"configs={len(configs)} rates={len(RATES)} probes={total} "
        f"file_mib={m.FILE_MIB} pass_completion>={PASS_COMPLETION}",
        flush=True,
    )

    probes: list[dict] = []
    done_keys: set[tuple] = set()
    if OUT_PROBES.is_file() and not FRESH:
        try:
            probes = json.loads(OUT_PROBES.read_text())
            done_keys = {(p["path"], p["codec_mode"], int(p["rate"])) for p in probes}
            print(f"resume probes={len(probes)}", flush=True)
        except json.JSONDecodeError:
            probes = []

    m.cleanup()
    m.prepare()
    port = m.PORT_BASE + 800
    n = 0
    for path, cm in configs:
        pname, label = path[0], cm[0]
        for rate in RATES:
            n += 1
            key = (pname, label, rate)
            if key in done_keys:
                print(f"[{n}/{total}] skip {pname} {label} {rate}", flush=True)
                continue
            port += 1
            print(f"\n[{n}/{total}] === {pname} {label} rate={rate} ===", flush=True)
            row = m.one_probe(path, cm, rate, port)
            out_b = int(row.get("out_bytes") or 0)
            completion = 100.0 * out_b / EXPECTED if EXPECTED else 0.0
            row.update(
                {
                    "path": pname,
                    "hops": path[1],
                    "app_relay": path[2],
                    "codec_mode": label,
                    "codec": cm[1],
                    "wh_ack": cm[2],
                    "rate": rate,
                    "expected_bytes": EXPECTED,
                    "completion_pct": round(completion, 3),
                    "pass_strict": row.get("verify") == "OK",
                    "pass_ge98": completion >= PASS_COMPLETION or row.get("verify") == "OK",
                }
            )
            # align pass_ge98 with helper (checksum alone also counts)
            row["pass_ge98"] = is_pass(row)
            probes.append(row)
            done_keys.add(key)
            print(
                f"  verify={row.get('verify')} completion={completion:.2f}% "
                f"goodput={row.get('goodput_mbps')} wire={row.get('wire_mbps')} "
                f"link_tx={row.get('link_tx_mbps')} link_rx={row.get('link_rx_mbps')} "
                f"pass98={row['pass_ge98']}",
                flush=True,
            )
            results = summarize(probes)
            save(results, probes)

    results = summarize(probes)
    save(results, probes)
    m.cleanup()
    print(f"\nDONE -> {OUT_JSON}", flush=True)
    print(f"MD   -> {OUT_MD}", flush=True)


if __name__ == "__main__":
    main()
