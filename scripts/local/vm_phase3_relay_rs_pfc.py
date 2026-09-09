#!/usr/bin/env python3
from __future__ import annotations
import json, os, sys
from pathlib import Path

os.environ.setdefault("WH_TOPO", "linear")
os.environ.setdefault("WH_CEIL_MIB", "200")

sys.path.insert(0, "/home/scy/work/multi-flow-rate-control/scripts/local")
import vm_wire_ceiling_search as m

RATES = [400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000]
CODECS = ["rs", "wirehair_ack"]  # PFC ~= wirehair_ack
PATH_NAME = "hop3_relay"
PASS_COMPLETION = 98.0
EXPECTED = m.FILE_MIB * 1024 * 1024
VARIANTS = [
    ("default", ""),
    ("egress_tuned", "--egress-wait-ms 5 --egress-capacity 65536"),
]
NAME = "linear_phase3_relay_rs_pfc"
REPO = Path("/home/scy/work/multi-flow-rate-control")
OUT_JSON = REPO / "build" / f"{NAME}.json"
OUT_PROBES = REPO / "build" / f"{NAME}_probes.json"
OUT_MD = REPO / "build" / "report-data" / f"{NAME}.md"

def is_pass(row):
    return row.get("verify") == "OK" or float(row.get("completion_pct") or 0) >= PASS_COMPLETION

def summarize(probes):
    out = []
    keys = sorted({(p["variant"], p["codec_mode"]) for p in probes})
    for variant, codec in keys:
        subset = [p for p in probes if p["variant"] == variant and p["codec_mode"] == codec]
        passes = [p for p in subset if p.get("pass_ge98")]
        best = max(passes, key=lambda x: float(x.get("goodput_mbps") or 0)) if passes else None
        max_rate = max((p["rate"] for p in passes), default=None)
        first_fail = next((p for p in sorted(subset, key=lambda x: x["rate"]) if not p.get("pass_ge98")), None)
        out.append({
            "path": PATH_NAME, "variant": variant, "codec_mode": codec,
            "n_probes": len(subset), "n_pass_ge98": len(passes),
            "max_pass_rate_mbps": max_rate,
            "first_fail_rate_mbps": None if first_fail is None else first_fail["rate"],
            "ceiling_mbps": None if best is None else best.get("goodput_mbps"),
            "best_goodput_at_rate": None if best is None else best.get("rate"),
            "relay_extra": subset[0].get("relay_extra", ""),
        })
    return out

def write_md(results, probes):
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Phase3 hop3_relay rs + PFC(wirehair_ack) ({NAME})",
        "", f"- rates: `{RATES}`", f"- codecs: `{CODECS}` (PFC = wirehair_ack)",
        f"- pass: checksum OK or completion ≥ {PASS_COMPLETION:.0f}%", "",
        "## Summary", "",
        "| variant | codec | max_pass_rate | ceiling goodput | first_fail | pass/total |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for r in results:
        c = r.get("ceiling_mbps")
        lines.append(
            f"| {r['variant']} | {r['codec_mode']} | {r.get('max_pass_rate_mbps')} | "
            f"{None if c is None else round(c,1)} | {r.get('first_fail_rate_mbps')} | "
            f"{r.get('n_pass_ge98')}/{r.get('n_probes')} |"
        )
    lines += ["", "## Probes", "",
              "| variant | codec | rate | verify | completion% | goodput | wire_thru | link_tx | link_rx | pass≥98 |",
              "|---|---|---:|---|---:|---:|---:|---:|---:|---|"]
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

def save(results, probes):
    OUT_JSON.write_text(json.dumps(results, indent=2) + "\n")
    OUT_PROBES.write_text(json.dumps(probes, indent=2) + "\n")
    write_md(results, probes)

def main():
    path = next(p for p in m.PATHS if p[0] == PATH_NAME)
    codecs = {c[0]: c for c in m.CODEC_MODES}
    total = len(VARIANTS) * len(CODECS) * len(RATES)
    print(f"path={PATH_NAME} codecs={CODECS} rates={RATES} probes={total}", flush=True)
    probes = []
    m.cleanup(); m.prepare()
    port = m.PORT_BASE + 1600
    n = 0
    for vname, extra in VARIANTS:
        os.environ["WH_RELAY_EXTRA"] = extra
        print(f"\n##### VARIANT {vname} [{extra}] #####", flush=True)
        for clabel in CODECS:
            cm = codecs[clabel]
            for rate in RATES:
                n += 1
                port += 1
                print(f"\n[{n}/{total}] === {vname} {clabel} rate={rate} ===", flush=True)
                row = m.one_probe(path, cm, rate, port)
                out_b = int(row.get("out_bytes") or 0)
                completion = 100.0 * out_b / EXPECTED if EXPECTED else 0.0
                row.update({
                    "path": PATH_NAME, "variant": vname, "relay_extra": extra,
                    "codec_mode": clabel, "codec": cm[1], "wh_ack": cm[2],
                    "rate": rate, "expected_bytes": EXPECTED,
                    "completion_pct": round(completion, 3),
                    "pass_strict": row.get("verify") == "OK",
                })
                row["pass_ge98"] = is_pass(row)
                probes.append(row)
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

if __name__ == "__main__":
    main()
