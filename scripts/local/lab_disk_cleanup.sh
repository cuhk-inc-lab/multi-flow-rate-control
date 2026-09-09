#!/usr/bin/env bash
# Auto-clean lab VM disk when usage is high.
# Safe targets: /tmp/wire_* experiment artifacts, old stress outs, core dumps.
set -uo pipefail
THRESHOLD_PCT="${WH_DISK_THRESHOLD_PCT:-80}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=12)
HOSTS=(fyp1@10.10.10.161 fyp1@10.10.10.162 fyp1@10.10.10.163 fyp1@10.10.10.164)

clean_one() {
  local host=$1
  local used
  used=$(ssh "${SSH_OPTS[@]}" "$host" "df -P / | awk 'NR==2{gsub(/%/,\"\",\$5); print \$5}'" 2>/dev/null || echo 0)
  echo "[disk] $host used=${used}%"
  if [[ "${used:-0}" -lt "$THRESHOLD_PCT" ]]; then
    return 0
  fi
  echo "[disk] CLEAN $host (>=${THRESHOLD_PCT}%)"
  ssh "${SSH_OPTS[@]}" "$host" 'bash -s' <<'EOS'
set -uo pipefail
# experiment scratch
rm -rf /tmp/wire_ceil_matrix /tmp/wire_* 2>/dev/null || true
rm -rf /tmp/wg_* /tmp/iperf* 2>/dev/null || true
# old large stress payloads/outs under build (keep recent json/md)
REPO=$HOME/work/multi-flow-rate-control
if [[ -d $REPO/build ]]; then
  find "$REPO/build" -type d \( -name out -o -name payloads -o -name 'wire-stress*' \) \
    -prune -exec rm -rf {} + 2>/dev/null || true
  find "$REPO/build" -type f \( -name '*.out' -o -name 'input.bin' -o -name 'core' -o -name 'core.*' \) \
    -mtime +0 -delete 2>/dev/null || true
  # trim very large logs older than today
  find "$REPO/build" -type f -name '*.log' -size +200M -mtime +0 -delete 2>/dev/null || true
fi
# apt/journal light trim if sudo works
if sudo -n true 2>/dev/null; then
  sudo -n journalctl --vacuum-size=50M >/dev/null 2>&1 || true
elif [[ -n "${WH_SUDO_PASS:-}" ]]; then
  printf '%s\n' "$WH_SUDO_PASS" | sudo -S -p '' journalctl --vacuum-size=50M >/dev/null 2>&1 || true
fi
df -h / | tail -1
EOS
}

for h in "${HOSTS[@]}"; do
  clean_one "$h" || echo "[disk] WARN clean failed $h"
done
