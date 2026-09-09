#!/usr/bin/env bash
# Commercial-grade linear Phase3–5 campaign (post datagram-pool relay).
# Separates goodput vs wire/link throughput; auto disk cleanup between phases.
#
# Env:
#   WH_SUDO_PASS     sudo password for netem / cleanup (do not commit)
#   WH_TOPO=linear
#   WH_STOP_AFTER_PHASE3=0  (this campaign runs 3–5)
set -uo pipefail
REPO=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$REPO"
export WH_TOPO=linear
export WH_SUDO_PASS="${WH_SUDO_PASS:-}"
export WH_STOP_AFTER_PHASE3=0
# Faster but still complete drains for commercial sweep
export WH_POST_SEND_WAIT_S="${WH_POST_SEND_WAIT_S:-25}"
export WH_IDLE_SEC="${WH_IDLE_SEC:-15}"
export WH_RELAY_IDLE="${WH_RELAY_IDLE:-25}"
export WH_CEIL_MIB="${WH_CEIL_MIB:-200}"
export WH_DISK_THRESHOLD_PCT="${WH_DISK_THRESHOLD_PCT:-78}"

TAG="${WH_COMMERCIAL_TAG:-commercial_$(date +%Y%m%d-%H%M%S)}"
LOG=$REPO/build/${TAG}_run.log
mkdir -p "$REPO/build/report-data/linear" "$REPO/build/report-data/vxlan" "$REPO/build"
exec > >(tee -a "$LOG") 2>&1

cleanup_disk() {
  echo "===== DISK CLEANUP $(date) ====="
  chmod +x scripts/local/lab_disk_cleanup.sh
  WH_SUDO_PASS="${WH_SUDO_PASS}" bash scripts/local/lab_disk_cleanup.sh || true
}

echo "======== COMMERCIAL Phase3-5 start $(date) tag=$TAG ========"
echo "metrics: goodput_mbps | wire_mbps(app) | link_tx_mbps/link_rx_mbps(NIC)"
echo "waits: POST=$WH_POST_SEND_WAIT_S IDLE=$WH_IDLE_SEC RELAY_IDLE=$WH_RELAY_IDLE MIB=$WH_CEIL_MIB"

cleanup_disk

# Ensure latest wire_relay on VMs
echo "===== SYNC wire_relay ====="
for h in 161 162 163 164; do
  rsync -az build/wire_relay "fyp1@10.10.10.$h:/home/fyp1/work/multi-flow-rate-control/build/wire_relay" || echo "WARN sync .$h"
done

echo "===== PHASE 3A hop3_kernel rate grid ====="
cleanup_disk
WH_P3G_FRESH=1 \
WH_P3G_NAME=${TAG}_p3_kernel \
WH_P3G_PATHS=hop3_kernel \
WH_P3G_RATES=2000,2500,3000,3500,4000,4500,5000,5500,6000,6500,7000,7500,8000,8500,9000,9500,10000 \
  python3 -u scripts/local/vm_phase3_rate_grid.py || echo "WARN p3 kernel rc=$?"

echo "===== PHASE 3B hop3_relay rate grid (egress tuned) ====="
cleanup_disk
export WH_RELAY_EXTRA="--egress-wait-ms 5 --egress-capacity 65536"
WH_P3G_FRESH=1 \
WH_P3G_NAME=${TAG}_p3_relay \
WH_P3G_PATHS=hop3_relay \
WH_P3G_RATES=400,600,800,1000,1200,1400,1600,1800,2000,2500,3000 \
  python3 -u scripts/local/vm_phase3_rate_grid.py || echo "WARN p3 relay rc=$?"
unset WH_RELAY_EXTRA

echo "===== PHASE 3 merge ceil for P4 ====="
python3 - <<PY
import json
from pathlib import Path
tag = "${TAG}"
ceil = {"kernel": {}, "relay": {}}
for name, key in ((f"{tag}_p3_kernel", "kernel"), (f"{tag}_p3_relay", "relay")):
    p = Path(f"build/{name}.json")
    if not p.exists():
        continue
    for r in json.loads(p.read_text()):
        mode = r.get("codec_mode")
        c = r.get("ceiling_mbps")
        if mode and c is not None:
            ceil[key][mode] = float(c)
Path("build/linear_phase3_ceil_for_p4.json").write_text(json.dumps(ceil, indent=2) + "\n")
Path(f"build/{tag}_ceil_for_p4.json").write_text(json.dumps(ceil, indent=2) + "\n")
print("ceil_for_p4", ceil)
PY

echo "===== PHASE 4A multiflow saturation ====="
cleanup_disk
python3 - <<'PY'
import json
from pathlib import Path
ceil_path = Path("build/linear_phase3_ceil_for_p4.json")
if ceil_path.exists():
    ceil = json.loads(ceil_path.read_text())
    Path("scripts/local/_linear_p4_ceil_overlay.py").write_text("CEIL = " + json.dumps(ceil) + "\n")
    print("wrote ceil overlay", ceil)
PY
WH_P4_FRESH=1 WH_P4_NAME=${TAG}_phase4_multiflow_sat \
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
' || echo "WARN phase4a rc=$?"

echo "===== PHASE 4B stress batches ====="
cleanup_disk
rsync -az scripts/local/stress_linear_4b_*.json scripts/local/run_wire_stress.sh scripts/local/wire_stress_run.py \
  fyp1@10.10.10.161:/home/fyp1/work/multi-flow-rate-control/scripts/local/ || true
rsync -az scripts/iperf_like_monitor.py scripts/wire_stress_charts.py \
  fyp1@10.10.10.161:/home/fyp1/work/multi-flow-rate-control/scripts/ || true
for b in 1 2 3; do
  case $b in
    1) cfg=stress_linear_4b_batch1_copy_mixed.json ;;
    2) cfg=stress_linear_4b_batch2_copy_sink4.json ;;
    3) cfg=stress_linear_4b_batch3_wirehair_ack_mixed.json ;;
  esac
  echo "--- 4B batch $b $cfg ---"
  cleanup_disk
  ssh -o BatchMode=yes fyp1@10.10.10.161 \
    "cd ~/work/multi-flow-rate-control && chmod +x scripts/local/run_wire_stress.sh && \
     RESULT_DIR=build/wire-stress-${TAG}-4b-batch$b ./scripts/local/run_wire_stress.sh scripts/local/$cfg" \
    || echo "WARN 4B batch$b failed"
  mkdir -p "$REPO/build/wire-stress-${TAG}-4b-batch$b"
  rsync -az --exclude=out --exclude=payloads \
    fyp1@10.10.10.161:~/work/multi-flow-rate-control/build/wire-stress-${TAG}-4b-batch$b/ \
    "$REPO/build/wire-stress-${TAG}-4b-batch$b/" || true
done
python3 - <<PY
from pathlib import Path
tag = "${TAG}"
lines = [f"# {tag} Phase4B stress\n"]
for b in (1, 2, 3):
    p = Path(f"build/wire-stress-{tag}-4b-batch{b}/results.md")
    lines.append(f"## batch{b}\n")
    lines.append(p.read_text() if p.exists() else "(missing)\n")
Path(f"build/report-data/linear/{tag}_phase4b_stress.md").write_text("\n".join(lines))
PY

echo "===== PHASE 5 resource profile ====="
cleanup_disk
python3 - <<'PY'
from pathlib import Path
p = Path("scripts/local/vm_phase5_resource_profile.py")
t = p.read_text()
t2 = t.replace("stress_4b_batch1_copy_mixed.json", "stress_linear_4b_batch1_copy_mixed.json")
t2 = t2.replace("build/wire-stress-4b-batch1-p5", "build/wire-stress-linear-4b-batch1-p5")
if t2 != t:
    p.write_text(t2)
    print("phase5 partC cfg -> linear")
PY
WH_P5_FRESH=1 WH_P5_NAME=${TAG}_phase5_resource WH_P5_PARTS=A,B,C \
  python3 -u scripts/local/vm_phase5_resource_profile.py || echo "WARN phase5 rc=$?"
if [[ -f build/report-data/vxlan/phase5_resource_profile.md ]]; then
  cp -a build/report-data/vxlan/phase5_resource_profile.md "build/report-data/linear/${TAG}_phase5_resource_profile.md"
fi

echo "===== COMMERCIAL BRIEF ====="
python3 scripts/local/generate_commercial_brief.py --tag "$TAG" || echo "WARN brief rc=$?"

cleanup_disk
echo "======== COMMERCIAL Phase3-5 end $(date) tag=$TAG ========"
echo "LOG=$LOG"
echo "BRIEF=build/report-data/linear/${TAG}_commercial_brief.md"
