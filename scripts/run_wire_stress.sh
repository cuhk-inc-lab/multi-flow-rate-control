#!/usr/bin/env sh

# Configurable multi-stream wire stress test.
# Run from Node1 (or any host listed as ssh: local in the config):
#
#   ./scripts/run_wire_stress.sh scripts/examples/stress_lab.yaml
#   ./scripts/run_wire_stress.sh path/to/stress.json
#
# Artifacts under build/wire-stress-<ts>/:
#   results.md, streams.csv, logs/, monitor/, config copy
#
# See docs/SCRIPTS.md and scripts/examples/stress_lab.yaml.

set -u

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)

cfg=${1:-}
if [ -z "$cfg" ]; then
    cat >&2 <<'EOF'
Usage:
  ./scripts/run_wire_stress.sh CONFIG.yaml|.json [--result-dir DIR]

Example:
  ./scripts/run_wire_stress.sh scripts/examples/stress_lab.yaml

Config describes nodes + streams (from/to/file/rate_mbps/codec).
The runner groups processes by codec because wg_multi_pipeline is
one-codec-per-process. Max 8 flows per receiver process.

Requires: build/wg_multi_pipeline (make wg-demo).
YAML configs need PyYAML; JSON works with stdlib only.
EOF
    exit 2
fi
shift

# Allow RESULT_DIR env override when --result-dir not passed.
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
