#!/usr/bin/env bash
# Master: re-run Phase0–5 on LINEAR bridge topology.
# Required: WH_SUDO_PASS for netem on VM1 (phase2). Export before run; do not commit.
set -uo pipefail
REPO=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$REPO"
export WH_TOPO=linear
export WH_SUDO_PASS="${WH_SUDO_PASS:-}"
LOG=$REPO/build/linear_full_run.log
mkdir -p "$REPO/build/report-data" "$REPO/build"
exec > >(tee -a "$LOG") 2>&1

echo "======== LINEAR FULL RUN start $(date) WH_TOPO=$WH_TOPO ========"

echo "===== PHASE 0-1 ====="
chmod +x scripts/local/run_linear_phase01.sh
bash scripts/local/run_linear_phase01.sh || echo "WARN phase01 rc=$?"

echo "===== PHASE 2 loss matrix ====="
if [[ -n "${WH_SUDO_PASS}" ]]; then
  WH_LOSS_FRESH=1 WH_LOSS_NAME=linear_wirehair_loss_repair_matrix \
    python3 -u scripts/local/vm_wirehair_loss_repair_matrix.py || echo "WARN phase2 rc=$?"
  python3 - <<'PY' || true
import json
from pathlib import Path
from collections import Counter
p=Path('build/linear_wirehair_loss_repair_matrix.json')
if p.exists():
    rows=json.loads(p.read_text())
    ok=sum(1 for r in rows if r.get('checksum_ok'))
    Path('build/report-data/linear_phase2_loss_repair.md').write_text(
        f"# Linear Phase2 loss×ACK\n\ncases={len(rows)} checksum_ok={ok}\n\nraw: `{p}`\n")
    print('phase2 cases', len(rows), 'ok', ok)
PY
else
  echo "SKIP phase2: set WH_SUDO_PASS"
fi

echo "===== PHASE 3 ceiling ====="
WH_CEIL_FRESH=1 WH_CEIL_NAME=linear_wire_ceiling_search \
  python3 -u scripts/local/vm_wire_ceiling_search.py || echo "WARN phase3 rc=$?"
python3 - <<'PY' || true
import json
from pathlib import Path
p=Path('build/linear_wire_ceiling_search.json')
if not p.exists():
    raise SystemExit
rows=json.loads(p.read_text())
lines=["# Linear Phase3 ceiling (goodput Mbps)\n","| path | mode | ceiling | max_pass_rate |","|---|---|---:|---:|"]
ceil={"kernel":{},"relay":{}}
for r in rows:
    lines.append(f"| {r.get('path')} | {r.get('codec_mode')} | {None if r.get('ceiling_mbps') is None else round(r['ceiling_mbps'])} | {r.get('max_pass_rate_mbps')} |")
    path=r.get('path'); mode=r.get('codec_mode'); c=r.get('ceiling_mbps')
    if c is None: continue
    if path=='hop3_kernel': ceil['kernel'][mode]=float(c)
    if path=='hop3_relay': ceil['relay'][mode]=float(c)
Path('build/report-data/linear_phase3_ceiling.md').write_text("\n".join(lines)+"\n")
Path('build/linear_phase3_ceil_for_p4.json').write_text(json.dumps(ceil, indent=2)+"\n")
print('phase3 rows', len(rows), 'ceil', ceil)
PY

# Stop after Phase3 when requested (default for this linear campaign).
if [[ "${WH_STOP_AFTER_PHASE3:-1}" == "1" ]]; then
  echo "===== STOP after Phase3 (WH_STOP_AFTER_PHASE3=1) $(date) ====="
  echo "======== LINEAR RUN end (phase0-3 only) $(date) ========"
  exit 0
fi

echo "===== PHASE 4A multiflow ====="
# Update CEIL in phase4 module from linear phase3 if available
python3 - <<'PY'
import json, re
from pathlib import Path
ceil_path=Path('build/linear_phase3_ceil_for_p4.json')
mod=Path('scripts/local/vm_phase4_multiflow_sat.py')
if not ceil_path.exists():
    print('no ceil file, keep defaults')
    raise SystemExit(0)
ceil=json.loads(ceil_path.read_text())
t=mod.read_text()
# replace CEIL = { ... } block by writing overlay file executed first
Path('scripts/local/_linear_p4_ceil_overlay.py').write_text(
    "CEIL = " + json.dumps(ceil) + "\n"
)
print('wrote ceil overlay', ceil)
PY
WH_P4_FRESH=1 WH_P4_NAME=linear_phase4_multiflow_sat \
  python3 -u -c '
import json, os, sys
from pathlib import Path
sys.path.insert(0, "scripts/local")
os.environ["WH_TOPO"]="linear"
import vm_phase4_multiflow_sat as m
ov=Path("scripts/local/_linear_p4_ceil_overlay.py")
if ov.exists():
    ns={}
    exec(ov.read_text(), ns)
    for path, modes in ns.get("CEIL", {}).items():
        for mode, v in modes.items():
            if path in m.CEIL and mode in m.CEIL[path]:
                m.CEIL[path][mode]=float(v)
                print(f"override CEIL[{path}][{mode}]={v}")
sys.exit(m.main())
' || echo "WARN phase4 rc=$?"

echo "===== PHASE 4B stress ====="
rsync -az scripts/local/stress_linear_4b_*.json scripts/local/run_wire_stress.sh scripts/local/wire_stress_run.py \
  scripts/iperf_like_monitor.py \
  fyp1@10.10.10.161:/home/fyp1/work/multi-flow-rate-control/scripts/local/ 2>/dev/null || \
rsync -az scripts/local/stress_linear_4b_*.json scripts/local/run_wire_stress.sh scripts/local/wire_stress_run.py \
  fyp1@10.10.10.161:/home/fyp1/work/multi-flow-rate-control/scripts/local/
# monitor helper lives in scripts/
rsync -az scripts/iperf_like_monitor.py scripts/wire_stress_charts.py \
  fyp1@10.10.10.161:/home/fyp1/work/multi-flow-rate-control/scripts/ || true
for b in 1 2 3; do
  case $b in
    1) cfg=stress_linear_4b_batch1_copy_mixed.json ;;
    2) cfg=stress_linear_4b_batch2_copy_sink4.json ;;
    3) cfg=stress_linear_4b_batch3_wirehair_ack_mixed.json ;;
  esac
  echo "--- 4B batch $b $cfg ---"
  ssh -o BatchMode=yes fyp1@10.10.10.161 \
    "cd ~/work/multi-flow-rate-control && chmod +x scripts/local/run_wire_stress.sh && \
     RESULT_DIR=build/wire-stress-linear-4b-batch$b ./scripts/local/run_wire_stress.sh scripts/local/$cfg" \
    || echo "WARN 4B batch$b failed"
  mkdir -p "$REPO/build/wire-stress-linear-4b-batch$b"
  rsync -az --exclude=out --exclude=payloads \
    fyp1@10.10.10.161:~/work/multi-flow-rate-control/build/wire-stress-linear-4b-batch$b/ \
    "$REPO/build/wire-stress-linear-4b-batch$b/" || true
done
python3 - <<'PY' || true
from pathlib import Path
lines=["# Linear Phase4B stress\n"]
for b in (1,2,3):
    p=Path(f"build/wire-stress-linear-4b-batch{b}/results.md")
    lines.append(f"## batch{b}\n")
    lines.append(p.read_text() if p.exists() else "(missing)\n")
Path("build/report-data/linear_phase4b_stress.md").write_text("\n".join(lines))
PY

echo "===== PHASE 5 resource ====="
# point part C to linear batch1
python3 - <<'PY'
from pathlib import Path
p=Path('scripts/local/vm_phase5_resource_profile.py')
t=p.read_text()
t2=t.replace('stress_4b_batch1_copy_mixed.json','stress_linear_4b_batch1_copy_mixed.json')
t2=t2.replace('build/wire-stress-4b-batch1-p5','build/wire-stress-linear-4b-batch1-p5')
if t2!=t:
    p.write_text(t2)
    print('phase5 partC cfg -> linear')
PY
WH_P5_FRESH=1 WH_P5_NAME=linear_phase5_resource WH_P5_PARTS=A,B,C \
  python3 -u scripts/local/vm_phase5_resource_profile.py || echo "WARN phase5 rc=$?"
if [[ -f build/report-data/phase5_resource_profile.md ]]; then
  cp -a build/report-data/phase5_resource_profile.md build/report-data/linear_phase5_resource_profile.md
fi

python3 - <<'PY'
from pathlib import Path
Path("build/report-data/linear_full_summary.md").write_text(
"""# Linear topology full re-run summary

- Topology: bridge hops `10.20.20` / `10.30.30` / `10.40.40`
- Reports under `build/report-data/linear_*.md`
- Raw: `build/linear_*` and `build/wire-stress-linear-*`
""")
print("done summary")
PY

echo "======== LINEAR FULL RUN end $(date) ========"
