#!/usr/bin/env sh
# RS loss tolerance sweep using userspace udp_loss_forwarder (loopback, no tc).
#
#   LOSS_LIST="0 0.01 0.02 0.05" CODECS="none rs" RS_PROFILES="16+2 16+4" \
#     REPEATS=5 ./scripts/run_link_loss_forwarder_matrix.sh build/ffrs_seed.bin
#
# Optional: --repeats N (overrides REPEATS env)
#
# Topology per run:
#   sender -> 127.0.0.1:FWD_LISTEN_PORT (forwarder) -> 127.0.0.1:RECV_PORT (wg recv)

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
bin="$repo_root/build/wg_multi_pipeline"
forwarder_py="$script_dir/udp_loss_forwarder.py"
summarize_py="$script_dir/summarize_forwarder_matrix.py"

repeats=${REPEATS:-1}
while [ "$#" -gt 0 ]; do
    case "$1" in
        --repeats)
            repeats=$2
            shift 2
            ;;
        --repeats=*)
            repeats=${1#*=}
            shift 1
            ;;
        *)
            break
            ;;
    esac
done

seed_file=${1:-}
if [ "$#" -ge 1 ]; then
    shift 1
fi

loss_list=${LOSS_LIST:-"0 0.01 0.02 0.03 0.05 0.08 0.10"}
codecs=${CODECS:-"none rs"}
rs_profiles=${RS_PROFILES:-"16+2 16+4"}
rate=${RATE:-40}
flows=${FLOWS:-1}
duration_s=${DURATION_S:-15}
idle_sec=${IDLE_SEC:-60}
recv_port_base=${RECV_PORT:-19210}
fwd_listen_port_base=${FWD_LISTEN_PORT:-19211}
fwd_seed_base=${FWD_SEED:-1000}
matrix_seed_base=${MATRIX_SEED:-5000}
result_root=${RESULT_DIR:-"$repo_root/build/link-loss-forwarder-loopback"}
batch_ts=$(date +%Y%m%d-%H%M%S)
result_root="$result_root/$batch_ts"

die() { echo "error: $*" >&2; exit 1; }

[ -n "$seed_file" ] || die "usage: run_link_loss_forwarder_matrix.sh [--repeats N] SEED.bin"
[ -f "$seed_file" ] || die "missing seed: $seed_file"
[ -x "$bin" ] || die "missing binary: $bin (run make wg-demo)"
[ -f "$forwarder_py" ] || die "missing $forwarder_py"
[ -f "$summarize_py" ] || die "missing $summarize_py"

mkdir -p "$result_root/runs"
: > "$result_root/results_runs.csv"

echo "Link loss forwarder matrix (loopback)"
echo "  result_root=$result_root"
echo "  loss_list=$loss_list codecs=$codecs rs_profiles=$rs_profiles rate=$rate repeats=$repeats"
echo "  duration=${duration_s}s idle=${idle_sec}s"

run_counter=0

for loss in $loss_list; do
    for codec in $codecs; do
        profiles=$rs_profiles
        if [ "$codec" != "rs" ]; then
            profiles="none"
        fi
        for profile in $profiles; do
            [ "$codec" != "rs" ] && [ "$profile" = "none" ] && profile=""
            profile_label=${profile:-none}
            repeat=1
            while [ "$repeat" -le "$repeats" ]; do
                run_counter=$((run_counter + 1))
                loss_tag=$(echo "$loss" | tr '.' 'p')
                run_id="${batch_ts}-loss${loss_tag}-${codec}-${profile_label}-r${repeat}"
                run_dir="$result_root/runs/$run_id"
                matrix_dir="$run_dir/matrix"
                fwd_summary="$run_dir/forwarder-summary.json"

                port_offset=$((run_counter * 2))
                recv_port=$((recv_port_base + port_offset))
                fwd_listen_port=$((fwd_listen_port_base + port_offset))

                fwd_seed=$((fwd_seed_base + run_counter * 997 + repeat * 13))
                matrix_seed=$((matrix_seed_base + run_counter))

                rs_env=
                if [ "$codec" = "rs" ]; then
                    rs_env="RS_PROFILE=$profile"
                fi

                mkdir -p "$run_dir"
                echo "=== run $run_id loss=$loss codec=$codec profile=${profile:-—} repeat=$repeat seed=$fwd_seed ports=$fwd_listen_port->$recv_port ==="

                python3 "$forwarder_py" \
                    --listen-host 127.0.0.1 --listen-port "$fwd_listen_port" \
                    --forward-host 127.0.0.1 --forward-port "$recv_port" \
                    --loss "$loss" --seed "$fwd_seed" \
                    --summary-json "$fwd_summary" \
                    > "$run_dir/forwarder.log" 2>&1 &
                fwd_pid=$!
                sleep 0.3

                # shellcheck disable=SC2086
                env LOCAL=1 SEND_PORT="$fwd_listen_port" PORT_BASE="$recv_port" \
                    CODECS="$codec" RATES="$rate" FLOWS="$flows" \
                    DURATION_S="$duration_s" IDLE_SEC="$idle_sec" \
                    MONITOR_RELAYS=0 RESULT_DIR="$matrix_dir" \
                    FINAL_DST=2 TTL=2 LOCAL_NODE_ID=2 KEEP_REMOTE_OUTPUT=1 \
                    $rs_env \
                    "$script_dir/run_wire_multiflow_matrix.sh" \
                    "127.0.0.1" "127.0.0.1" "$seed_file" \
                    > "$run_dir/matrix-driver.log" 2>&1 || true

                kill "$fwd_pid" 2>/dev/null || true
                wait "$fwd_pid" 2>/dev/null || true

                python3 "$summarize_py" append-run \
                    --result-root "$result_root" \
                    --run-id "$run_id" \
                    --repeat "$repeat" \
                    --loss "$loss" \
                    --codec "$codec" \
                    --rs-profile "$profile" \
                    --seed "$fwd_seed" \
                    --source-mbps "$rate" \
                    --run-dir "$run_dir"

                repeat=$((repeat + 1))
            done
        done
    done
done

python3 "$summarize_py" aggregate --result-root "$result_root"

echo
echo "Done: $result_root"
echo "  results_runs.csv"
echo "  results_summary.csv"
echo "  results.md"
column -t -s, "$result_root/results_summary.csv" 2>/dev/null || cat "$result_root/results_summary.csv"
