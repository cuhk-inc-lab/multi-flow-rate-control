#!/usr/bin/env python3
"""hop3_relay low-rate grid + egress tuning A/B.

Rates: 400..2000 step 200
Codecs: copy, block
Variants:
  A) default relay args
  B) --egress-wait-ms 5 --egress-capacity 65536

Env:
  WH_TOPO=linear
  WH_CEIL_MIB=200
  WH_P3R_FRESH=1
  WH_P3R_NAME=linear_phase3_relay_lowrate
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

RATES = [400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000]
CODECS = ["copy", "block"]
PATH_NAME = "hop3_relay"
PASS_COMPLETION = 98.0
EXPECTED = m.FILE_MIB * 1024 * 1024

VARIANTS = [
    ("default", ""),
    ("egress_tuned", "--egress-wait-ms 5 --egress-capacity 65536"),
]

NAME = os.environ.get("WH_P3R_NAME", "linear_phase3_relay_lowrate")
FRESH = os.environ.get("WH_P3R_FRESH", "0") == "1"

REPO = Path(__file__).resolve().parents[2]
OUT_JSON = REPO / "build" / f"{NAME}.json"
OUT_PROBES = REPO / "build" / f"{NAME}_probes.json"
OUT_MD = REPO / "build" / "report-data" / f"{NAME}.md"


def is_pass(row: dict) -> bool:
    if row.get("verify") == "OK":
        return True
    return float(row.get("completion_pct") or 0) >= PASS_COMPLETION


def summarize(probes: list[dict]) -> list[dict]:
    keys = sorted({(p["variant"], p["codec_mode"]) for p in probes})
    out = []
    for variant, codec in keys:
        subset = [
            p
            for p in probes
            if p["variant"] == variant and p["codec_mode"] == codec
        ]
        passes = [p for p in subset if p.get("pass_ge98")]
        best = (
            max(passes, key=lambda x: float(x.get("goodput_mbps") or 0))
            if passes
            else None
        )
        max_rate = max((p["rate"] for p in passes), default=None)
        # first fail among ascending rates
        first_fail = None
        for p in sorted(subset, key=lambda x: x["rate"]):
            if not p.get("pass_ge98"):
                first_fail = p
                break
        out.append(
            {
                "path": PATH_NAME,
                "variant": variant,
                "codec_mode": codec,
                "n_probes": len(subset),
                "n_pass_ge98": len(passes),
                "max_pass_rate_mbps": max_rate,
                "first_fail_rate_mbps": None if first_fail is None else first_fail["rate"],
                "ceiling_mbps": None if best is None else best.get("goodput_mbps"),
                "best_goodput_at_rate": None if best is None else best.get("rate"),
                "relay_extra": subset[0].get("relay_extra", ""),
            }
        )
    return out


def write_md(results: list[dict], probes: list[dict]) -> None:
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Phase3 hop3_relay low-rate A/B ({NAME})",
        "",
        f"- file: `{m.FILE_MIB}` MiB",
        f"- rates: `{RATES}`",
        f"- codecs: `{CODECS}`",
        f"- pass: checksum OK or completion ≥ {PASS_COMPLETION:.0f}%",
        "",
        "## Summary",
        "",
        "| variant | codec | max_pass_rate | ceiling goodput | first_fail | pass/total | relay_extra |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for r in results:
        c = r.get("ceiling_mbps")
        lines.append(
            f"| {r['variant']} | {r['codec_mode']} | {r.get('max_pass_rate_mbps')} | "
            f"{None if c is None else round(c,1)} | {r.get('first_fail_rate_mbps')} | "
            f"{r.get('n_pass_ge98')}/{r.get('n_probes')} | `{r.get('relay_extra')}` |"
        )
    lines += [
        "",
        "## Probes",
        "",
        "| variant | codec | rate | verify | completion% | goodput | wire_thru | link_tx | link_rx | pass≥98 |",
        "|---|---|---:|---|---:|---:|---:|---:|---:|---|",
    ]
    for p in probes:
        def _r(k):
            v = p.get(k)
            return None if v is None else round(float(v), 1)

        lines.append(
            f"| {p['variant']} | {p['codec_mode']} | {p['rate']} | {p.get('verify')} | "
            f"{p.get('completion_pct')} | {_r('goodput_mbps')} | {_r('wire_mbps')} | "
            f"{_r('link_tx_mbps')} | {_r('link_rx_mbps')} | {p.get('pass_ge98')} |"
        )
    OUT_MD.write_text("\n".join(lines) + "\n")


def save(results: list[dict], probes: list[dict]) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(results, indent=2) + "\n")
    OUT_PROBES.write_text(json.dumps(probes, indent=2) + "\n")
    write_md(results, probes)


def main() -> None:
    path = next(p for p in m.PATHS if p[0] == PATH_NAME)
    codecs = {c[0]: c for c in m.CODEC_MODES}
    total = len(VARIANTS) * len(CODECS) * len(RATES)
    print(
        f"path={PATH_NAME} variants={len(VARIANTS)} codecs={CODECS} "
        f"rates={RATES} probes={total} file_mib={m.FILE_MIB}",
        flush=True,
    )

    probes: list[dict] = []
    done: set[tuple] = set()
    if OUT_PROBES.is_file() and not FRESH:
        probes = json.loads(OUT_PROBES.read_text())
        done = {
            (p["variant"], p["codec_mode"], int(p["rate"])) for p in probes
        }
        print(f"resume probes={len(probes)}", flush=True)

    m.cleanup()
    m.prepare()
    port = m.PORT_BASE + 1200
    n = 0
    for vname, extra in VARIANTS:
        os.environ["WH_RELAY_EXTRA"] = extra
        print(f"\n##### VARIANT {vname} extra=[{extra}] #####", flush=True)
        for clabel in CODECS:
            cm = codecs[clabel]
            for rate in RATES:
                n += 1
                key = (vname, clabel, rate)
                if key in done:
                    print(f"[{n}/{total}] skip {vname} {clabel} {rate}", flush=True)
                    continue
                port += 1
                print(
                    f"\n[{n}/{total}] === {vname} {PATH_NAME} {clabel} rate={rate} ===",
                    flush=True,
                )
                row = m.one_probe(path, cm, rate, port)
                out_b = int(row.get("out_bytes") or 0)
                completion = 100.0 * out_b / EXPECTED if EXPECTED else 0.0
                row.update(
                    {
                        "path": PATH_NAME,
                        "variant": vname,
                        "relay_extra": extra,
                        "codec_mode": clabel,
                        "codec": cm[1],
                        "rate": rate,
                        "expected_bytes": EXPECTED,
                        "completion_pct": round(completion, 3),
                        "pass_strict": row.get("verify") == "OK",
                    }
                )
                row["pass_ge98"] = is_pass(row)
                probes.append(row)
                done.add(key)
                print(
                    f"  verify={row.get('verify')} completion={completion:.2f}% "
                    f"goodput={row.get('goodput_mbps')} wire={row.get('wire_mbps')} "
                    f"link_tx={row.get('link_tx_mbps')} link_rx={row.get('link_rx_mbps')} "
                    f"pass98={row['pass_ge98']}",
                    flush=True,
                )
                save(summarize(probes), probes)

    save(summarize(probes), probes)
    m.cleanup()
    os.environ.pop("WH_RELAY_EXTRA", None)
    print(f"\nDONE -> {OUT_JSON}", flush=True)
    print(f"MD   -> {OUT_MD}", flush=True)


if __name__ == "__main__":
    main()
