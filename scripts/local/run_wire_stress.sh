#!/usr/bin/env sh
# Local Phase-4B wrapper: uses scripts/local/wire_stress_run.py (supports defaults.wg_extra).
set -u
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
cfg=${1:-}
if [ -z "$cfg" ]; then
  echo "Usage: $0 CONFIG.json|.yaml [--result-dir DIR]" >&2
  exit 2
fi
shift
has_result_dir=0
for arg in "$@"; do
  if [ "$arg" = "--result-dir" ]; then
    has_result_dir=1
    break
  fi
done
if [ "$has_result_dir" -eq 0 ] && [ -n "${RESULT_DIR:-}" ]; then
  set -- --result-dir "$RESULT_DIR" "$@"
fi
exec python3 "$script_dir/wire_stress_run.py" "$cfg" --repo-root "$repo_root" "$@"
