#!/usr/bin/env bash
# Wait for after-pool lowrate job, write before/after comparison, then launch commercial P3-5.
set -uo pipefail
REPO=/home/scy/work/multi-flow-rate-control
cd "$REPO"
PIDF=build/linear_phase3_relay_lowrate_after_pool.pid
LOG=build/wait_then_commercial.log
exec > >(tee -a "$LOG") 2>&1

echo "[waiter] start $(date)"
if [[ -f "$PIDF" ]]; then
  pid=$(cat "$PIDF")
  while kill -0 "$pid" 2>/dev/null; do
    echo "[waiter] after_pool still running pid=$pid $(date)"
    sleep 30
  done
fi

# Wait until probes complete or DONE marker
for i in $(seq 1 180); do
  if grep -q 'DONE ->' build/linear_phase3_relay_lowrate_after_pool.log 2>/dev/null; then
    echo "[waiter] saw DONE marker"
    break
  fi
  n=$(python3 - <<'PY'
import json
from pathlib import Path
p=Path('build/linear_phase3_relay_lowrate_after_pool_probes.json')
print(len(json.loads(p.read_text())) if p.exists() else 0)
PY
)
  echo "[waiter] probes=$n/36 ($(date))"
  if [[ "$n" -ge 36 ]]; then
    break
  fi
  sleep 20
done

echo "[waiter] writing before/after comparison $(date)"
python3 - <<'PY'
import json
from pathlib import Path
before=Path('build/linear_phase3_relay_lowrate.json')
after=Path('build/linear_phase3_relay_lowrate_after_pool.json')
lines=['# hop3_relay lowrate: before vs after datagram-pool\n',
       '| variant | codec | before max_pass | before ceiling | after max_pass | after ceiling | Δ ceiling |\n',
       '|---|---|---:|---:|---:|---:|---:|\n']
b={ (r['variant'],r['codec_mode']):r for r in (json.loads(before.read_text()) if before.exists() else []) }
a={ (r['variant'],r['codec_mode']):r for r in (json.loads(after.read_text()) if after.exists() else []) }
keys=sorted(set(b)|set(a))
for k in keys:
    rb,ra=b.get(k),a.get(k)
    cb=None if not rb else rb.get('ceiling_mbps')
    ca=None if not ra else ra.get('ceiling_mbps')
    d=None if cb is None or ca is None else round(ca-cb,1)
    lines.append(
        f"| {k[0]} | {k[1]} | {None if not rb else rb.get('max_pass_rate_mbps')} | "
        f"{None if cb is None else round(cb,1)} | {None if not ra else ra.get('max_pass_rate_mbps')} | "
        f"{None if ca is None else round(ca,1)} | {d} |\n")
Path('build/report-data/linear/linear_relay_pool_before_after.md').write_text(''.join(lines))
print('wrote build/report-data/linear/linear_relay_pool_before_after.md')
PY

echo "[waiter] launching commercial Phase3-5 $(date)"
chmod +x scripts/local/run_commercial_linear_p3_p5.sh scripts/local/lab_disk_cleanup.sh
export WH_SUDO_PASS="${WH_SUDO_PASS:-fyp1user}"
export WH_TOPO=linear
export WH_STOP_AFTER_PHASE3=0
TAG="commercial_pool_$(date +%Y%m%d-%H%M%S)"
echo "$TAG" > build/commercial_active_tag.txt
nohup env WH_SUDO_PASS="$WH_SUDO_PASS" WH_COMMERCIAL_TAG="$TAG" \
  bash scripts/local/run_commercial_linear_p3_p5.sh \
  >> "build/${TAG}_run.log" 2>&1 &
echo $! > build/commercial_p3p5.pid
echo "[waiter] commercial pid=$(cat build/commercial_p3p5.pid) tag=$TAG"
echo "[waiter] done spawn $(date)"
