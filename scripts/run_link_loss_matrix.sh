#!/usr/bin/env sh
# Sweep link loss (tc netem) vs codec / RS geometry.
#
# Default: Node1 → Node2 only (receiver on Node2 ap0).
# Loss is applied on Node1 **station0** egress (same N1→N2 hop).  Root netem on
# Node2 ap0 only affects egress and does not drop inbound traffic from Node1.
#
#   LOSS_LIST="0 1 2 5 8" CODECS="none rs" RS_PROFILES="16+2 16+4" \
#     ./scripts/run_link_loss_matrix.sh build/ffrs_seed.bin
#
# Four-hop (legacy): TWO_NODE=0 RECEIVER_SSH=fyp1@10.10.10.164 RECEIVER_IP=10.10.34.2 \
#   TC_DEV=station1 ./scripts/run_link_loss_matrix.sh seed.ts
#
# Env:
#   TWO_NODE=1             Node1→Node2 direct (default)
#   LOSS_LIST              tc netem loss % on NODE2 TC_DEV
#   CODECS / RS_PROFILES / RATE
#   TC_LOCAL=1             apply tc on this host (default 1 for TWO_NODE=1)
#   TC_SSH / TC_DEV        two-node default: station0 on sender (local tc)
#   FINAL_DST / TTL / LOCAL_NODE_ID   two-node defaults: 2 / 2 / 2
#   IDLE_SEC               receiver idle (default 60 for loss runs)
#   BEST_EFFORT=1          receiver --best-effort (skip gaps; hash expected FAIL)
#   APPLY_TC=1

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

two_node=${TWO_NODE:-1}
seed=${1:-}
if [ "$#" -ge 1 ]; then
    shift 1
fi

loss_list=${LOSS_LIST:-"0 1 2 3 5 8 10"}
codecs=${CODECS:-"none rs"}
rs_profiles=${RS_PROFILES:-"16+2 16+4"}
rate=${RATE:-40}
apply_tc=${APPLY_TC:-1}
idle_sec=${IDLE_SEC:-60}
best_effort=${BEST_EFFORT:-0}
ssh_opts="-o BatchMode=yes -o ConnectTimeout=10"
timestamp=$(date +%Y%m%d-%H%M%S)
result_root=${RESULT_DIR:-"build/link-loss-$timestamp"}

if [ "$two_node" = "1" ]; then
    receiver_ssh=${RECEIVER_SSH:-fyp1@10.10.10.162}
    receiver_ip=${RECEIVER_IP:-10.10.12.2}
    final_dst=${FINAL_DST:-2}
    ttl=${TTL:-2}
    local_node_id=${LOCAL_NODE_ID:-2}
    monitor_relays=${MONITOR_RELAYS:-0}
    tc_local=${TC_LOCAL:-1}
    tc_ssh=${TC_SSH:-}
    tc_dev=${TC_DEV:-station0}
    path_label="Node1→Node2 (tc egress dev $tc_dev → recv $receiver_ip ap0)"
else
    receiver_ssh=${RECEIVER_SSH:-fyp1@10.10.10.164}
    receiver_ip=${RECEIVER_IP:-10.10.34.2}
    final_dst=${FINAL_DST:-4}
    ttl=${TTL:-8}
    local_node_id=${LOCAL_NODE_ID:-4}
    tc_ssh=${TC_SSH:-${NODE2_SSH:-fyp1@10.10.10.162}}
    tc_dev=${TC_DEV:-station1}
    tc_local=${TC_LOCAL:-0}
    monitor_relays=${MONITOR_RELAYS:-1}
    path_label="Node1→Node4 (tc on $tc_ssh dev $tc_dev)"
fi

tc_target=${tc_dev}
if [ "$tc_local" != "1" ]; then
    tc_target="$tc_ssh:$tc_dev"
fi

run_tc() {
    if [ "$tc_local" = "1" ]; then
        # shellcheck disable=SC2068
        sudo -n tc "$@"
    else
        ssh $ssh_opts "$tc_ssh" "sudo -n tc $(printf '%q ' "$@")"
    fi
}

die() { echo "error: $*" >&2; exit 1; }

[ -n "$seed" ] || die "usage: run_link_loss_matrix.sh SEED.ts"
[ -f "$seed" ] || die "missing seed: $seed"

apply_netem() {
    loss=$1
    if [ "$apply_tc" != "1" ]; then
        echo "  (APPLY_TC=0, assuming tc already set to ${loss}% on $tc_target)"
        return 0
    fi
    if [ "$loss" = "0" ] || [ "$loss" = "0.0" ]; then
        run_tc qdisc del dev "$tc_dev" root 2>/dev/null || true
        return 0
    fi
    run_tc qdisc replace dev "$tc_dev" root netem loss "${loss}%" \
        || die "tc netem failed on $tc_target"
}

summary_csv="$result_root/summary.csv"
mkdir -p "$result_root"
echo "loss_pct,codec,rs_profile,rate_mbps,status,flows_pass,wire_shard_loss_pct,block_completion_pct,missing_groups,recovered_groups,groups_failed,window_overflow,pending_recovered_groups,skipped_groups,output_bytes,payload_bytes,byte_completion_pct,e2e_p95_us,n2_avg_mbps,notes" \
    > "$summary_csv"

echo "Link loss matrix: $path_label"
echo "  receiver=$receiver_ssh ($receiver_ip) final_dst=$final_dst ttl=$ttl local_node_id=$local_node_id"
echo "  loss_list=$loss_list codecs=$codecs rs_profiles=$rs_profiles rate=$rate idle=${idle_sec}s best_effort=$best_effort"

for loss in $loss_list; do
    echo "=== link loss ${loss}% (tc on $tc_target) ==="
    apply_netem "$loss"

    for codec in $codecs; do
        profiles=$rs_profiles
        if [ "$codec" != "rs" ]; then
            profiles="none"
        fi
        for profile in $profiles; do
            [ "$codec" != "rs" ] && [ "$profile" = "none" ] && profile=""
            label="loss${loss}-${codec}"
            if [ "$codec" = "rs" ]; then
                label="${label}-${profile}"
            fi
            subdir="$result_root/$label"
            rs_env=
            if [ "$codec" = "rs" ]; then
                rs_env="RS_PROFILE=$profile"
            fi
            echo "--- $label @ ${rate}Mbps ---"
            # shellcheck disable=SC2086
            env CODECS="$codec" RATES="$rate" FLOWS=1 DURATION_S=15 \
                MONITOR_RELAYS="$monitor_relays" IDLE_SEC="$idle_sec" PORT_BASE=9200 \
                FINAL_DST="$final_dst" TTL="$ttl" LOCAL_NODE_ID="$local_node_id" \
                BEST_EFFORT="$best_effort" \
                RESULT_DIR="$subdir" $rs_env \
                "$script_dir/run_wire_multiflow_matrix.sh" \
                "$receiver_ssh" "$receiver_ip" "$seed" \
                || true

            if [ ! -f "$subdir/results.csv" ]; then
                echo "$loss,$codec,${profile:-—},$rate,ERR,0,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,no_results" >> "$summary_csv"
                continue
            fi
            row=$(tail -n 1 "$subdir/results.csv")
            IFS=',' read -r c r n2p n2a n3p n3a fl st fp wire_loss block_pct e2e el srrc rrrc notes <<EOF
$row
EOF
            flow_csv="$subdir/flows.csv"
            missing_groups=NA
            recovered_groups=NA
            groups_failed=NA
            window_overflow=NA
            pending_recovered=NA
            skipped_groups=NA
            output_bytes=NA
            payload_bytes=NA
            byte_completion_pct=NA
            if [ -f "$flow_csv" ]; then
                eval "$(python3 - "$flow_csv" <<'PY'
import csv, sys
path = sys.argv[1]
keys = (
    "missing_groups",
    "recovered_groups",
    "groups_failed",
    "window_overflow",
    "pending_recovered_groups",
    "skipped_groups",
    "output_bytes",
    "payload_bytes",
)
try:
    with open(path, encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
except OSError:
    rows = []
row = rows[0] if rows else {}
for key in keys:
    val = row.get(key) or "NA"
    if key == "pending_recovered_groups":
        print(f"pending_recovered={val}")
    else:
        print(f"{key}={val}")
out = row.get("output_bytes") or ""
pay = row.get("payload_bytes") or ""
try:
    o = float(out)
    p = float(pay)
    pct = f"{100.0 * o / p:.4f}" if p > 0 else "NA"
except (TypeError, ValueError):
    pct = "NA"
print(f"byte_completion_pct={pct}")
PY
)"
            fi
            echo "$loss,$codec,${profile:-—},$rate,$st,$fp,$wire_loss,$block_pct,$missing_groups,$recovered_groups,$groups_failed,$window_overflow,$pending_recovered,$skipped_groups,$output_bytes,$payload_bytes,$byte_completion_pct,$e2e,$n2a,${notes:-—}" >> "$summary_csv"
            echo "  -> $st flows=$fp wire_loss=$wire_loss% block=$block_pct% bytes=$byte_completion_pct% recovered=$recovered_groups failed=$groups_failed overflow=$window_overflow pending=$pending_recovered skipped=$skipped_groups out=$output_bytes/$payload_bytes N2avg=$n2a"
        done
    done
done

apply_netem 0
echo
echo "Done: $summary_csv"
column -t -s, "$summary_csv" 2>/dev/null || cat "$summary_csv"
