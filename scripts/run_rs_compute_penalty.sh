#!/usr/bin/env sh
# P1 RS compute-penalty runner (POSIX sh).
# Does not modify C sources, decoder, header, pacing, tests, or the forwarder.
#
# Main path: cross-VM Node1 → Node2 with --udp-send-multi and one --flow.
#   Node1 sender: station0 10.10.12.1  SSH fyp1@10.10.10.161
#   Node2 receiver: ap0 10.10.12.2     SSH fyp1@10.10.10.162
# Loopback only when --path loopback.
#
# Fixed wire-rate (B/C):
#   source_rate = W * (k * 1400) / ((k + r) * 1444)
# none: k=1, r=0, n=1, source_rate = W * 1400 / 1444  (never source=W).
#
# Naming (avoid clobbering loop indices with RS params):
#   rs_k, rs_parity, rs_n, rep_index, warmup_index, rate_mbps, loss_value, profile_name

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
parse_py="$script_dir/parse_rs_compute_penalty.py"
bin_rel="./build/wg_multi_pipeline"
fwd_py="$script_dir/udp_loss_forwarder.py"
time_fmt='elapsed_sec=%e user_sec=%U sys_sec=%S cpu_pct=%P maxrss_kb=%M'
ssh_opts="-o BatchMode=yes -o ConnectTimeout=10"

experiment=
profiles="none,16+2,16+4,16+6"
input_path=
repetitions=5
warmup=1
wire_rates="10,20,50"
payload_rate=
calibrate_rates="80,160,240,320"
skip_calibrate=0
losses="0,0.01,0.02,0.05"
seed=42
output_dir=
dry_run=0
include_copy=0

# Path defaults favor cross_vm (Node1→Node2).
path=cross_vm
send_host=10.10.12.2
recv_host=10.10.12.2
recv_port=9200
fwd_listen_port=9210
final_dst=2
ttl=2
local_node_id=2
receiver_ssh=fyp1@10.10.10.162
sender_ssh=fyp1@10.10.10.161
remote_repo='$HOME/work/multi-flow-rate-control'
sender_data_ip=10.10.12.1
idle_sec=
sender_input_path=

# Track which path-sensitive flags the user set.
send_host_set=0
recv_host_set=0
recv_port_set=0
fwd_listen_port_set=0
final_dst_set=0
ttl_set=0
local_node_id_set=0
receiver_ssh_set=0
sender_ssh_set=0
sender_data_ip_set=0
idle_sec_set=0

# Infrastructure failure count (hash FAIL is NOT an infra failure).
runner_errors=0
expected_ids_file=
runs_csv=

die() { echo "error: $*" >&2; exit 1; }

usage() {
    cat <<'EOF'
Usage: run_rs_compute_penalty.sh --experiment A|B|C [options]

  --experiment A|B|C
  --path cross_vm|loopback     default cross_vm (Node1→Node2 WiFi)
  --profiles none,16+2,16+4,16+6
  --input FILE                 required except --dry-run
  --repetitions N              default 5
  --warmup N                   default 1
  --wire-rates 10,20,50        B/C target wire Mbps (100 gated by precheck)
  --payload-rate Mbps          A (required unless calibrating)
  --calibrate-rates 80,160,... A none calibration ladder
  --skip-calibrate
  --losses 0,0.01,0.02,0.05    C i.i.d. per-datagram loss
  --seed N                     default 42
  --output-dir DIR

  Cross-VM defaults (--path cross_vm):
  --send-host 10.10.12.2       UDP destination (Node2 ap0)
  --recv-host                  unused for dest; kept for docs
  --recv-port 9200
  --fwd-listen-port 9210       C: Node2 listen (must differ from recv-port)
  --final-dst 2 --ttl 2 --local-node-id 2
  --receiver-ssh fyp1@10.10.10.162   empty = local receiver
  --sender-ssh fyp1@10.10.10.161     empty = local sender
  --remote-repo $HOME/work/multi-flow-rate-control   (expanded on remote)
  --sender-data-ip 10.10.12.1  CSV meta only (Node1 station0)
  --idle-sec N                 default 30 cross_vm / 8 loopback

  Loopback (--path loopback): send/recv 127.0.0.1; clears SSH unless set;
  final_dst/ttl/local_node_id default 4/8/4 after path apply.

  --include-copy               appendix only; not a primary baseline
  --dry-run                    print k,r,n and source-rate; do not start processes
EOF
}

require_arg() {
    # $1=flag $2=value
    if [ -z "${2:-}" ]; then
        die "$1 requires a value"
    fi
}

is_uint() {
    case "$1" in
        ''|*[!0-9]*) return 1 ;;
        *) return 0 ;;
    esac
}

is_number() {
    python3 -c "import sys
try:
    float(sys.argv[1])
except Exception:
    raise SystemExit(1)" "$1"
}

comma_to_words() {
    echo "$1" | tr ',' ' ' | sed 's/  */ /g;s/^ //;s/ $//'
}

profile_krn() {
    python3 -c "import sys
sys.path.insert(0, r'''$script_dir''')
from parse_rs_compute_penalty import parse_profile
try:
    k,r,n=parse_profile(sys.argv[1])
except Exception as e:
    print('error:', e, file=sys.stderr)
    raise SystemExit(1)
print(k,r,n)" "$1"
}

src_rate_for() {
    python3 "$parse_py" source-rate --k "$1" --n "$2" --wire-rate "$3"
}

codec_args() {
    profile_name=$1
    if [ "$profile_name" = "none" ]; then
        echo "--codec none"
    elif [ "$profile_name" = "copy" ]; then
        echo "--codec copy"
    else
        echo "--codec rs --rs-profile=$profile_name"
    fi
}

validate_profiles() {
    plist=$1
    [ -n "$plist" ] || die "empty --profiles"
    for profile_name in $plist; do
        profile_krn "$profile_name" >/dev/null || die "invalid profile: $profile_name"
    done
}

validate_number_list() {
    label=$1
    values=$2
    [ -n "$values" ] || die "empty $label"
    for v in $values; do
        is_number "$v" || die "non-numeric $label entry: $v"
    done
}

path_mode_for() {
    if [ "$path" = "loopback" ]; then
        echo "loopback"
    elif [ "$experiment" = "C" ]; then
        echo "cross_vm_fwd_n2"
    else
        echo "cross_vm_direct"
    fi
}

run_timeout_s_for() {
    # Args: source_rate_mbps. Wall-clock budget for one run.
    python3 -c "import math,sys
rate=max(float(sys.argv[1]), 0.1)
payload=int(sys.argv[2])
idle=int(sys.argv[3])
xfer=payload*8.0/(rate*1e6)
# transfer + idle drain + startup/cleanup margin; floor 90s
print(max(90, int(math.ceil(xfer + idle + 45))))" "$1" "$payload_bytes" "$idle_sec"
}

wait_log() {
    file=$1
    pattern=$2
    tries=80
    wait_i=0
    while [ "$wait_i" -lt "$tries" ]; do
        if [ -f "$file" ] && grep -q "$pattern" "$file" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
        wait_i=$((wait_i + 1))
    done
    return 1
}

ssh_cmd() {
    # shellcheck disable=SC2086
    ssh $ssh_opts "$@"
}

scp_cmd() {
    # shellcheck disable=SC2086
    scp $ssh_opts "$@"
}

stop_udp_port() {
    # $1=port  $2=optional ssh target (empty = local)
    port=$1
    host=${2:-}
    if [ -n "$host" ]; then
        ssh_cmd "$host" "fuser -k ${port}/udp >/dev/null 2>&1 || true" || true
    else
        fuser -k "${port}/udp" >/dev/null 2>&1 || true
    fi
}

# SIGTERM first so udp_loss_forwarder can write summary-json (fuser -k is SIGKILL).
stop_forwarder_graceful() {
    # $1=port  $2=ssh_target_or_empty  $3=optional remote summary path to wait for
    port=$1
    host=${2:-}
    summary_path=${3:-}
    if [ -n "$host" ]; then
        ssh_cmd "$host" "
            pid=\$(ss -ulnp 2>/dev/null | sed -n 's/.*:${port} .*pid=\([0-9][0-9]*\).*/\1/p' | head -1)
            if [ -n \"\$pid\" ]; then
                kill -TERM \"\$pid\" 2>/dev/null || true
            fi
            # Also term any time-wrapped children holding the port.
            fuser -TERM ${port}/udp >/dev/null 2>&1 || true
            i=0
            while [ \$i -lt 30 ]; do
                if [ -n \"$summary_path\" ] && [ -f \"$summary_path\" ]; then
                    break
                fi
                if ! ss -ulnp 2>/dev/null | grep -q ':${port} '; then
                    break
                fi
                sleep 0.1
                i=\$((i + 1))
            done
            fuser -k ${port}/udp >/dev/null 2>&1 || true
        " || true
    else
        pid=$(ss -ulnp 2>/dev/null | sed -n "s/.*:${port} .*pid=\([0-9][0-9]*\).*/\1/p" | head -1)
        if [ -n "$pid" ]; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
        fuser -TERM "${port}/udp" >/dev/null 2>&1 || true
        wait_i=0
        while [ "$wait_i" -lt 30 ]; do
            if [ -n "$summary_path" ] && [ -f "$summary_path" ]; then
                break
            fi
            if ! ss -ulnp 2>/dev/null | grep -q ":${port} "; then
                break
            fi
            sleep 0.1
            wait_i=$((wait_i + 1))
        done
        fuser -k "${port}/udp" >/dev/null 2>&1 || true
    fi
}

wait_remote_udp_listen() {
    # $1=ssh_target  $2=port  $3=tries
    host=$1
    port=$2
    tries=$3
    wait_i=0
    while [ "$wait_i" -lt "$tries" ]; do
        if ssh_cmd "$host" "ss -ulnp | grep -q ':${port} '" 2>/dev/null; then
            return 0
        fi
        sleep 0.2
        wait_i=$((wait_i + 1))
    done
    return 1
}

is_hundred() {
    [ "$1" = "100" ] || [ "$1" = "100.0" ]
}

make_run_id() {
    # Args: profile wire loss tag(warmN|rN)
    echo "${experiment}_$1_W$2_L$3_$4" | tr '+.' '__'
}

expect_run_id() {
    run_id=$1
    echo "$run_id" >> "$expected_ids_file"
}

note_infra_error() {
    runner_errors=$((runner_errors + 1))
    echo "infra_error: $*" >&2
}

meta_common_fields_py() {
    # Emit shared meta JSON fields for append_fail_row / run_once.
    # Uses shell vars already in scope; printed as Python dict fragment.
    path_mode=$(path_mode_for)
    if [ "$path" = "loopback" ]; then
        sn="local"
        rn="local"
        fwd_node="NA"
    else
        sn="node1"
        rn="node2"
        if [ "$experiment" = "C" ]; then
            fwd_node="node2"
        else
            fwd_node="NA"
        fi
    fi
    if [ "$experiment" = "C" ]; then
        fwd_l="$fwd_listen_port"
        fwd_d="127.0.0.1:${recv_port}"
    else
        fwd_l="NA"
        fwd_d="NA"
    fi
    sh_host=${sender_ssh:-local}
    rh_host=${receiver_ssh:-local}
    rdata_ip=$send_host
    cat <<PY
  "path_mode": "$path_mode",
  "sender_node": "$sn",
  "receiver_node": "$rn",
  "sender_host": "$sh_host",
  "receiver_host": "$rh_host",
  "sender_data_ip": "$sender_data_ip",
  "receiver_data_ip": "$rdata_ip",
  "final_dst": "$final_dst",
  "ttl": "$ttl",
  "local_node_id": "$local_node_id",
  "forwarder_node": "$fwd_node",
  "fwd_listen": "$fwd_l",
  "fwd_dest": "$fwd_d",
PY
}

append_fail_row() {
    # Best-effort CSV row when infrastructure fails mid-run.
    # Args: profile src_rate target_w loss is_warmup rep run_id notes_extra
    fail_profile=$1
    fail_src=$2
    fail_w=$3
    fail_loss=$4
    fail_warm=$5
    fail_rep=$6
    fail_id=$7
    fail_notes=$8
    fail_dir="$output_dir/runs/$fail_id"
    mkdir -p "$fail_dir"
    krn=$(profile_krn "$fail_profile" 2>/dev/null || echo "0 0 0")
    fail_k=$(echo "$krn" | awk '{print $1}')
    fail_parity=$(echo "$krn" | awk '{print $2}')
    fail_n=$(echo "$krn" | awk '{print $3}')
    echo "FAIL" > "$fail_dir/hash.txt"
    extra_meta=$(meta_common_fields_py)
    python3 - "$fail_dir/meta.json" <<PY
import json, sys
meta = {
  "run_id": "$fail_id",
  "experiment": "$experiment",
  "profile": "$fail_profile",
  "k": int("$fail_k"),
  "r": int("$fail_parity"),
  "n": int("$fail_n"),
  "payload_bytes": int("$payload_bytes"),
  "target_wire_rate_mbps": "$fail_w",
  "source_rate_mbps": "$fail_src",
  "loss_target": "$fail_loss",
  "seed": "$seed",
  "warmup": int("$fail_warm"),
  "rep": int("$fail_rep"),
  "hash": "FAIL",
  "git_rev": "$git_rev",
  "binary_sha256": "$binary_sha",
  "nproc": "$nproc_v",
  "cpu_governor": "$gov_v",
  "loadavg": "$load_v",
  "notes": "$fail_notes",
$extra_meta
}
if "$experiment" == "A":
    meta["target_wire_rate_mbps"] = "NA"
    meta["loss_target"] = "0"
json.dump(meta, open(sys.argv[1], "w"), indent=2)
PY
    if ! python3 "$parse_py" append --meta "$fail_dir/meta.json" --runs-csv "$runs_csv" \
        >"$fail_dir/row.json" 2>"$fail_dir/append.err"; then
        note_infra_error "append failed for $fail_id"
    fi
}

kill_tree() {
    pid=$1
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        sleep 0.2
        kill -KILL "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
}

remote_workdir_for() {
    echo "/tmp/rs-pen-$1"
}

cleanup_run_ports() {
    # Clear recv (and fwd for C) on receiver; clear send-side if SSH sender.
    if [ -n "$receiver_ssh" ]; then
        stop_udp_port "$recv_port" "$receiver_ssh"
        if [ "$experiment" = "C" ]; then
            stop_udp_port "$fwd_listen_port" "$receiver_ssh"
        fi
    else
        stop_udp_port "$recv_port" ""
        if [ "$experiment" = "C" ]; then
            stop_udp_port "$fwd_listen_port" ""
        fi
    fi
    if [ -n "$sender_ssh" ]; then
        # Best-effort: sender should not listen, but clear if stale.
        stop_udp_port "$recv_port" "$sender_ssh" || true
    fi
}

run_once() {
    # Args: profile_name src_rate target_w loss is_warmup rep_index run_id
    profile_name=$1
    src_rate=$2
    target_w=$3
    loss_value=$4
    is_warmup=$5
    rep_index=$6
    run_id=$7

    k_r_n=$(profile_krn "$profile_name") || {
        note_infra_error "profile parse failed: $profile_name"
        append_fail_row "$profile_name" "$src_rate" "$target_w" "$loss_value" \
            "$is_warmup" "$rep_index" "$run_id" "profile_parse_failed"
        return 1
    }
    rs_k=$(echo "$k_r_n" | awk '{print $1}')
    rs_parity=$(echo "$k_r_n" | awk '{print $2}')
    rs_n=$(echo "$k_r_n" | awk '{print $3}')
    path_mode=$(path_mode_for)

    echo "RUN experiment=$experiment profile=$profile_name k=$rs_k r=$rs_parity n=$rs_n wire_rate=$target_w source_rate=$src_rate loss=$loss_value rep=$rep_index warmup=$is_warmup path_mode=$path_mode run_id=$run_id"

    ca=$(codec_args "$profile_name")
    # shellcheck disable=SC2086
    set -- $ca
    rundir="$output_dir/runs/$run_id"
    mkdir -p "$rundir"
    out_file="$rundir/out.bin"
    remote_workdir=$(remote_workdir_for "$run_id")
    send_port=$recv_port
    if [ "$experiment" = "C" ]; then
        send_port=$fwd_listen_port
    fi
    flow_input=${sender_input_path:-$input_path}

    run_timeout_s=$(run_timeout_s_for "$src_rate")
    echo "timeout_s=$run_timeout_s idle_sec=$idle_sec" > "$rundir/timeout.txt"
    run_deadline=$(( $(date +%s) + run_timeout_s ))

    cleanup_run_ports
    sleep 0.2

    {
        echo "bin=$bin_rel"
        echo "profile=$profile_name"
        echo "codec_args=$ca"
        echo "src_rate=$src_rate"
        echo "target_w=$target_w"
        echo "loss=$loss_value"
        echo "send=$send_host:$send_port"
        echo "recv_port=$recv_port"
        echo "flow=0:${send_host}:${send_port}:${flow_input}:${src_rate}"
        echo "udp-send-multi=yes"
        echo "path_mode=$path_mode"
        echo "path=$path"
        echo "sender_ssh=${sender_ssh:-}"
        echo "receiver_ssh=${receiver_ssh:-}"
        echo "remote_workdir=$remote_workdir"
        echo "final_dst=$final_dst ttl=$ttl local_node_id=$local_node_id"
        echo "timeout_s=$run_timeout_s"
    } > "$rundir/cmdline.txt"

    fwd_pid=
    recv_pid=
    send_pid=
    timed_out=0
    killed=0
    send_rc=0
    notes="ok"

    # --- Start receiver (Node2 or local) ---
    if [ -n "$receiver_ssh" ]; then
        ssh_cmd "$receiver_ssh" \
            "cd \"$remote_repo\" && mkdir -p \"$remote_workdir\" && rm -f \"$remote_workdir/out.bin\" \"$remote_workdir/receiver.time\" && \
             /usr/bin/time -f '$time_fmt' -o \"$remote_workdir/receiver.time\" \
             $bin_rel $ca --local-node-id $local_node_id --udp-recv $recv_port \"$remote_workdir/out.bin\" --max-flows 1 --idle-sec $idle_sec --strict" \
            >"$rundir/receiver.log" 2>&1 &
        recv_pid=$!
        printf '%s\n' "$bin_rel $ca --local-node-id $local_node_id --udp-recv $recv_port $remote_workdir/out.bin --max-flows 1 --idle-sec $idle_sec --strict" \
            > "$rundir/receiver.cmd"
    else
        rm -f "$out_file"
        /usr/bin/time -f "$time_fmt" -o "$rundir/receiver.time" \
            "$bin_rel" "$@" --local-node-id "$local_node_id" \
            --udp-recv "$recv_port" "$out_file" --max-flows 1 --idle-sec "$idle_sec" --strict \
            >"$rundir/receiver.log" 2>&1 &
        recv_pid=$!
        printf '%s\n' "$bin_rel $ca --local-node-id $local_node_id --udp-recv $recv_port $out_file --max-flows 1 --idle-sec $idle_sec --strict" \
            > "$rundir/receiver.cmd"
    fi

    if ! wait_log "$rundir/receiver.log" "udp-recv: listening"; then
        note_infra_error "receiver did not listen ($run_id)"
        cat "$rundir/receiver.log" >&2 || true
        kill_tree "$recv_pid"
        append_fail_row "$profile_name" "$src_rate" "$target_w" "$loss_value" \
            "$is_warmup" "$rep_index" "$run_id" "receiver_listen_failed"
        return 1
    fi

    # --- Experiment C: start forwarder on Node2 (listen FWD → 127.0.0.1:RECV) ---
    if [ "$experiment" = "C" ]; then
        fwd_log="$rundir/forwarder.log"
        fwd_json="$rundir/forwarder-summary.json"
        if [ -n "$receiver_ssh" ]; then
            # ssh -f returns locally; remote python would otherwise hold the session.
            # shellcheck disable=SC2086
            ssh -f -n $ssh_opts "$receiver_ssh" \
                "cd \"$remote_repo\" && mkdir -p \"$remote_workdir\" && \
                 /usr/bin/time -f '$time_fmt' -o \"$remote_workdir/forwarder.time\" \
                 python3 scripts/udp_loss_forwarder.py \
                   --listen-host 0.0.0.0 --listen-port $fwd_listen_port \
                   --forward-host 127.0.0.1 --forward-port $recv_port \
                   --loss $loss_value --seed $seed \
                   --summary-json \"$remote_workdir/forwarder-summary.json\" \
                   > \"$remote_workdir/forwarder.log\" 2>&1" || {
                note_infra_error "forwarder ssh start failed ($run_id)"
                kill_tree "$recv_pid"
                append_fail_row "$profile_name" "$src_rate" "$target_w" "$loss_value" \
                    "$is_warmup" "$rep_index" "$run_id" "forwarder_start_failed"
                return 1
            }
            if ! wait_remote_udp_listen "$receiver_ssh" "$fwd_listen_port" 40; then
                note_infra_error "forwarder did not listen ($run_id)"
                stop_udp_port "$fwd_listen_port" "$receiver_ssh"
                kill_tree "$recv_pid"
                append_fail_row "$profile_name" "$src_rate" "$target_w" "$loss_value" \
                    "$is_warmup" "$rep_index" "$run_id" "forwarder_listen_failed"
                return 1
            fi
            printf '%s\n' "udp_loss_forwarder 0.0.0.0:$fwd_listen_port -> 127.0.0.1:$recv_port loss=$loss_value" \
                > "$rundir/forwarder.cmd"
        else
            /usr/bin/time -f "$time_fmt" -o "$rundir/forwarder.time" \
                python3 "$fwd_py" \
                --listen-host 0.0.0.0 --listen-port "$fwd_listen_port" \
                --forward-host 127.0.0.1 --forward-port "$recv_port" \
                --loss "$loss_value" --seed "$seed" --summary-json "$fwd_json" \
                >"$fwd_log" 2>&1 &
            fwd_pid=$!
            if ! wait_log "$fwd_log" "udp_loss_forwarder: listen"; then
                # Also accept ss readiness if log line differs.
                wait_i=0
                fwd_ok=0
                while [ "$wait_i" -lt 20 ]; do
                    if ss -ulnp 2>/dev/null | grep -q ":${fwd_listen_port} "; then
                        fwd_ok=1
                        break
                    fi
                    sleep 0.2
                    wait_i=$((wait_i + 1))
                done
                if [ "$fwd_ok" != "1" ]; then
                    note_infra_error "forwarder did not start ($run_id)"
                    kill_tree "$fwd_pid"
                    kill_tree "$recv_pid"
                    append_fail_row "$profile_name" "$src_rate" "$target_w" "$loss_value" \
                        "$is_warmup" "$rep_index" "$run_id" "forwarder_start_failed"
                    return 1
                fi
            fi
            printf '%s\n' "udp_loss_forwarder 0.0.0.0:$fwd_listen_port -> 127.0.0.1:$recv_port loss=$loss_value" \
                > "$rundir/forwarder.cmd"
        fi
    fi

    # --- Start sender (Node1 SSH or local) ---
    printf '%s\n' "$bin_rel $ca --final-dst $final_dst --ttl $ttl --udp-send-multi --flow 0:${send_host}:${send_port}:${flow_input}:${src_rate}" \
        > "$rundir/sender.cmd"

    set +e
    if [ -n "$sender_ssh" ]; then
        ssh_cmd "$sender_ssh" \
            "cd \"$remote_repo\" && mkdir -p \"$remote_workdir\" && \
             /usr/bin/time -f '$time_fmt' -o \"$remote_workdir/sender.time\" \
             $bin_rel $ca --final-dst $final_dst --ttl $ttl \
             --udp-send-multi \
             --flow 0:${send_host}:${send_port}:${flow_input}:${src_rate}" \
            >"$rundir/sender.log" 2>&1 &
        send_pid=$!
    else
        /usr/bin/time -f "$time_fmt" -o "$rundir/sender.time" \
            "$bin_rel" "$@" --final-dst "$final_dst" --ttl "$ttl" \
            --udp-send-multi \
            --flow "0:${send_host}:${send_port}:${flow_input}:${src_rate}" \
            >"$rundir/sender.log" 2>&1 &
        send_pid=$!
    fi

    while kill -0 "$send_pid" 2>/dev/null; do
        if [ "$(date +%s)" -ge "$run_deadline" ]; then
            timed_out=1
            echo "deadline: sender timeout" >> "$rundir/timeout.txt"
            kill_tree "$send_pid"
            break
        fi
        sleep 0.2
    done
    wait "$send_pid" 2>/dev/null
    send_rc=$?
    set -e

    # Sender done (or timed out); wait for receiver idle exit under deadline.
    wait_i=0
    wait_limit=$((idle_sec + 25))
    while [ "$wait_i" -lt "$wait_limit" ]; do
        if ! kill -0 "$recv_pid" 2>/dev/null; then
            break
        fi
        if [ "$(date +%s)" -ge "$run_deadline" ]; then
            timed_out=1
            echo "deadline: receiver timeout" >> "$rundir/timeout.txt"
            break
        fi
        sleep 1
        wait_i=$((wait_i + 1))
    done
    if kill -0 "$recv_pid" 2>/dev/null; then
        killed=1
        kill_tree "$recv_pid"
    else
        wait "$recv_pid" 2>/dev/null || true
    fi
    recv_pid=

    # --- Stop forwarder; collect remote artifacts ---
    if [ -n "$receiver_ssh" ]; then
        if [ "$experiment" = "C" ]; then
            stop_forwarder_graceful "$fwd_listen_port" "$receiver_ssh" \
                "$remote_workdir/forwarder-summary.json"
            sleep 0.2
            scp_cmd "$receiver_ssh:$remote_workdir/forwarder.time" "$rundir/forwarder.time" 2>/dev/null || true
            scp_cmd "$receiver_ssh:$remote_workdir/forwarder-summary.json" "$rundir/forwarder-summary.json" 2>/dev/null || true
            scp_cmd "$receiver_ssh:$remote_workdir/forwarder.log" "$rundir/forwarder.log" 2>/dev/null || true
        fi
        scp_cmd "$receiver_ssh:$remote_workdir/receiver.time" "$rundir/receiver.time" 2>/dev/null || true
        # Prefer remote hash to avoid large scp over WiFi.
        out_sha=$(ssh_cmd "$receiver_ssh" "sha256sum \"$remote_workdir/out.bin\" 2>/dev/null" | awk '{print $1}') || out_sha=
        if [ -n "$out_sha" ]; then
            echo "$out_sha" > "$rundir/output.sha256"
        else
            echo "missing" > "$rundir/output.sha256"
            scp_cmd "$receiver_ssh:$remote_workdir/out.bin" "$out_file" 2>/dev/null || true
            if [ -f "$out_file" ]; then
                out_sha=$(sha256sum "$out_file" | awk '{print $1}')
                echo "$out_sha" > "$rundir/output.sha256"
            fi
        fi
        stop_udp_port "$recv_port" "$receiver_ssh"
        ssh_cmd "$receiver_ssh" "rm -rf \"$remote_workdir\"" 2>/dev/null || true
    else
        if [ "$experiment" = "C" ]; then
            stop_forwarder_graceful "$fwd_listen_port" "" "$rundir/forwarder-summary.json"
            fwd_pid=
        elif [ -n "$fwd_pid" ]; then
            kill -TERM "$fwd_pid" 2>/dev/null || true
            wait "$fwd_pid" 2>/dev/null || true
            fwd_pid=
        fi
        stop_udp_port "$recv_port" ""
        if [ -f "$out_file" ]; then
            out_sha=$(sha256sum "$out_file" | awk '{print $1}')
            echo "$out_sha" > "$rundir/output.sha256"
        else
            echo "missing" > "$rundir/output.sha256"
            out_sha=
        fi
    fi

    if [ -n "$sender_ssh" ]; then
        scp_cmd "$sender_ssh:$remote_workdir/sender.time" "$rundir/sender.time" 2>/dev/null || true
        ssh_cmd "$sender_ssh" "rm -rf \"$remote_workdir\"" 2>/dev/null || true
    fi

    hash_st=FAIL
    if [ -z "${out_sha:-}" ] && [ -f "$rundir/output.sha256" ]; then
        out_sha=$(cat "$rundir/output.sha256")
    fi
    if [ -n "${out_sha:-}" ] && [ "$out_sha" != "missing" ] && [ "$out_sha" = "$input_sha" ]; then
        hash_st=PASS
    fi
    echo "$hash_st" > "$rundir/hash.txt"
    echo "$send_rc" > "$rundir/sender.rc"
    echo "$input_sha" > "$rundir/input.sha256"

    notes="send_rc=$send_rc"
    if [ "$timed_out" = "1" ]; then
        notes="$notes; run_timeout"
        note_infra_error "run timeout ($run_id)"
    fi
    if [ "$killed" = "1" ]; then
        notes="$notes; receiver_killed_after_wait"
    fi
    if [ "$send_rc" -ne 0 ]; then
        notes="$notes; sender_nonzero_exit"
        note_infra_error "sender exit $send_rc ($run_id)"
    fi
    if [ ! -s "$rundir/sender.time" ] || [ ! -s "$rundir/receiver.time" ]; then
        notes="$notes; missing_time_file"
        note_infra_error "missing /usr/bin/time output ($run_id)"
    fi
    if [ "$experiment" = "C" ] && [ ! -s "$rundir/forwarder.time" ]; then
        notes="$notes; missing_forwarder_time"
        note_infra_error "missing forwarder.time ($run_id)"
    fi
    if [ ! -s "$rundir/sender.log" ]; then
        notes="$notes; empty_sender_log"
        note_infra_error "empty sender log ($run_id)"
    fi

    if [ "$path" = "loopback" ]; then
        sn="local"
        rn="local"
        fwd_node="NA"
    else
        sn="node1"
        rn="node2"
        if [ "$experiment" = "C" ]; then
            fwd_node="node2"
        else
            fwd_node="NA"
        fi
    fi
    if [ "$experiment" = "C" ]; then
        fwd_l="$fwd_listen_port"
        fwd_d="127.0.0.1:${recv_port}"
    else
        fwd_l="NA"
        fwd_d="NA"
    fi
    sh_host=${sender_ssh:-local}
    rh_host=${receiver_ssh:-local}

    python3 - "$rundir/meta.json" <<PY
import json, sys
meta = {
  "run_id": "$run_id",
  "experiment": "$experiment",
  "profile": "$profile_name",
  "k": int("$rs_k"),
  "r": int("$rs_parity"),
  "n": int("$rs_n"),
  "payload_bytes": int("$payload_bytes"),
  "target_wire_rate_mbps": "$target_w",
  "source_rate_mbps": "$src_rate",
  "loss_target": "$loss_value",
  "seed": "$seed",
  "warmup": int("$is_warmup"),
  "rep": int("$rep_index"),
  "hash": "$hash_st",
  "git_rev": "$git_rev",
  "binary_sha256": "$binary_sha",
  "nproc": "$nproc_v",
  "cpu_governor": "$gov_v",
  "loadavg": "$load_v",
  "notes": """$notes""",
  "path_mode": "$path_mode",
  "sender_node": "$sn",
  "receiver_node": "$rn",
  "sender_host": "$sh_host",
  "receiver_host": "$rh_host",
  "sender_data_ip": "$sender_data_ip",
  "receiver_data_ip": "$send_host",
  "final_dst": "$final_dst",
  "ttl": "$ttl",
  "local_node_id": "$local_node_id",
  "forwarder_node": "$fwd_node",
  "fwd_listen": "$fwd_l",
  "fwd_dest": "$fwd_d",
}
if "$experiment" == "A":
    meta["target_wire_rate_mbps"] = "NA"
    meta["loss_target"] = "0"
elif str(meta["target_wire_rate_mbps"]) in ("", "NA", "none"):
    meta["target_wire_rate_mbps"] = "NA"
json.dump(meta, open(sys.argv[1], "w"), indent=2)
PY
    if ! python3 "$parse_py" append --meta "$rundir/meta.json" --runs-csv "$runs_csv" \
        >"$rundir/row.json" 2>"$rundir/append.err"; then
        note_infra_error "log parse/append failed ($run_id)"
        cat "$rundir/append.err" >&2 || true
        rm -f "$out_file"
        return 1
    fi
    if ! grep -q 'wire-multi-send: flow_id=0 blocks=' "$rundir/sender.log" 2>/dev/null; then
        note_infra_error "sender log missing blocks line ($run_id)"
    fi
    rm -f "$out_file"
    return 0
}

run_profile_reps() {
    rp_profile=$1
    rp_src=$2
    rp_w=$3
    rp_loss=$4
    warmup_index=0
    while [ "$warmup_index" -lt "$warmup" ]; do
        rid=$(make_run_id "$rp_profile" "$rp_w" "$rp_loss" "warm${warmup_index}")
        expect_run_id "$rid"
        run_once "$rp_profile" "$rp_src" "$rp_w" "$rp_loss" 1 "$warmup_index" "$rid" || true
        warmup_index=$((warmup_index + 1))
    done
    rep_index=1
    while [ "$rep_index" -le "$repetitions" ]; do
        rid=$(make_run_id "$rp_profile" "$rp_w" "$rp_loss" "r${rep_index}")
        expect_run_id "$rid"
        run_once "$rp_profile" "$rp_src" "$rp_w" "$rp_loss" 0 "$rep_index" "$rid" || true
        rep_index=$((rep_index + 1))
    done
}

last_row_ok() {
    row=$1
    python3 -c "
import json, sys
p=sys.argv[1]
d=json.load(open(p))
h=d.get('hash','')
ov=str(d.get('overflow', d.get('window_overflow','NA')))
sys.exit(0 if h=='PASS' and ov=='0' else 1)
" "$row"
}

# ---- argv ----
while [ "$#" -gt 0 ]; do
    case "$1" in
        --experiment) require_arg "$1" "${2:-}"; experiment=$2; shift 2 ;;
        --path) require_arg "$1" "${2:-}"; path=$2; shift 2 ;;
        --profiles) require_arg "$1" "${2:-}"; profiles=$2; shift 2 ;;
        --input) require_arg "$1" "${2:-}"; input_path=$2; shift 2 ;;
        --repetitions) require_arg "$1" "${2:-}"; repetitions=$2; shift 2 ;;
        --warmup) require_arg "$1" "${2:-}"; warmup=$2; shift 2 ;;
        --wire-rates) require_arg "$1" "${2:-}"; wire_rates=$2; shift 2 ;;
        --payload-rate) require_arg "$1" "${2:-}"; payload_rate=$2; shift 2 ;;
        --calibrate-rates) require_arg "$1" "${2:-}"; calibrate_rates=$2; shift 2 ;;
        --skip-calibrate) skip_calibrate=1; shift ;;
        --losses) require_arg "$1" "${2:-}"; losses=$2; shift 2 ;;
        --seed) require_arg "$1" "${2:-}"; seed=$2; shift 2 ;;
        --output-dir) require_arg "$1" "${2:-}"; output_dir=$2; shift 2 ;;
        --send-host) require_arg "$1" "${2:-}"; send_host=$2; send_host_set=1; shift 2 ;;
        --recv-host) require_arg "$1" "${2:-}"; recv_host=$2; recv_host_set=1; shift 2 ;;
        --recv-port) require_arg "$1" "${2:-}"; recv_port=$2; recv_port_set=1; shift 2 ;;
        --fwd-listen-port) require_arg "$1" "${2:-}"; fwd_listen_port=$2; fwd_listen_port_set=1; shift 2 ;;
        --receiver-ssh)
            # empty allowed = local receiver
            [ "$#" -ge 2 ] || die "$1 requires a value (empty string OK)"
            receiver_ssh=$2
            receiver_ssh_set=1
            shift 2
            ;;
        --sender-ssh)
            # empty allowed = local sender
            [ "$#" -ge 2 ] || die "$1 requires a value (empty string OK)"
            sender_ssh=$2
            sender_ssh_set=1
            shift 2
            ;;
        --remote-repo) require_arg "$1" "${2:-}"; remote_repo=$2; shift 2 ;;
        --sender-data-ip) require_arg "$1" "${2:-}"; sender_data_ip=$2; sender_data_ip_set=1; shift 2 ;;
        --final-dst) require_arg "$1" "${2:-}"; final_dst=$2; final_dst_set=1; shift 2 ;;
        --ttl) require_arg "$1" "${2:-}"; ttl=$2; ttl_set=1; shift 2 ;;
        --local-node-id) require_arg "$1" "${2:-}"; local_node_id=$2; local_node_id_set=1; shift 2 ;;
        --idle-sec) require_arg "$1" "${2:-}"; idle_sec=$2; idle_sec_set=1; shift 2 ;;
        --include-copy) include_copy=1; shift ;;
        --dry-run) dry_run=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) die "unknown arg: $1" ;;
    esac
done

[ -n "$experiment" ] || die "--experiment is required"
case "$experiment" in
    A|B|C) ;;
    *) die "--experiment must be A, B, or C" ;;
esac

case "$path" in
    cross_vm|loopback) ;;
    *) die "--path must be cross_vm or loopback" ;;
esac

# Apply path-dependent defaults (user-set flags win).
if [ "$path" = "loopback" ]; then
    if [ "$send_host_set" = "0" ]; then send_host=127.0.0.1; fi
    if [ "$recv_host_set" = "0" ]; then recv_host=127.0.0.1; fi
    if [ "$sender_ssh_set" = "0" ]; then sender_ssh=; fi
    if [ "$receiver_ssh_set" = "0" ]; then receiver_ssh=; fi
    if [ "$final_dst_set" = "0" ]; then final_dst=4; fi
    if [ "$ttl_set" = "0" ]; then ttl=8; fi
    if [ "$local_node_id_set" = "0" ]; then local_node_id=4; fi
    if [ "$sender_data_ip_set" = "0" ]; then sender_data_ip=127.0.0.1; fi
    if [ "$idle_sec_set" = "0" ]; then idle_sec=8; fi
    if [ "$recv_port_set" = "0" ]; then recv_port=19520; fi
    if [ "$fwd_listen_port_set" = "0" ]; then fwd_listen_port=19620; fi
else
    # cross_vm: keep compiled-in defaults unless user overrode.
    if [ "$idle_sec_set" = "0" ]; then idle_sec=30; fi
fi

is_uint "$repetitions" || die "--repetitions must be a non-negative integer"
is_uint "$warmup" || die "--warmup must be a non-negative integer"
is_uint "$recv_port" || die "--recv-port must be an integer"
is_uint "$fwd_listen_port" || die "--fwd-listen-port must be an integer"
is_uint "$idle_sec" || die "--idle-sec must be a positive integer"
[ "$idle_sec" -ge 1 ] || die "--idle-sec must be >= 1"
is_uint "$seed" || die "--seed must be an integer"
is_uint "$final_dst" || die "--final-dst must be an integer"
is_uint "$ttl" || die "--ttl must be an integer"
is_uint "$local_node_id" || die "--local-node-id must be an integer"

if [ "$experiment" = "C" ] && [ "$fwd_listen_port" = "$recv_port" ]; then
    die "experiment C requires --fwd-listen-port != --recv-port (got $fwd_listen_port)"
fi

cd "$repo_root"

profile_list=$(comma_to_words "$profiles")
if [ "$include_copy" = "1" ]; then
    profile_list="$profile_list copy"
fi
validate_profiles "$profile_list"

if [ "$dry_run" = "1" ]; then
    python3 "$parse_py" dry-run --experiment "$experiment" --profiles "$profiles" \
        --wire-rates "$wire_rates" --payload-rate "${payload_rate:-NA}" \
        --losses "$losses"
    exit 0
fi

[ -f "$parse_py" ] || die "missing $parse_py"
[ -n "$input_path" ] || die "--input is required"
[ -f "$input_path" ] || die "input not found: $input_path"
command -v /usr/bin/time >/dev/null 2>&1 || die "missing /usr/bin/time"
if [ "$experiment" = "C" ]; then
    [ -f "$fwd_py" ] || die "missing $fwd_py"
fi
if [ "$experiment" = "A" ] && [ "$skip_calibrate" = "1" ]; then
    [ -n "$payload_rate" ] || die "A --skip-calibrate requires --payload-rate"
    is_number "$payload_rate" || die "--payload-rate must be numeric"
fi
if [ "$experiment" = "A" ] && [ "$skip_calibrate" != "1" ]; then
    validate_number_list "--calibrate-rates" "$(comma_to_words "$calibrate_rates")"
fi
if [ "$experiment" = "B" ] || [ "$experiment" = "C" ]; then
    validate_number_list "--wire-rates" "$(comma_to_words "$wire_rates")"
fi
if [ "$experiment" = "C" ]; then
    validate_number_list "--losses" "$(comma_to_words "$losses")"
fi

# Binary checks: local only when loopback or local sender; else SSH both ends.
need_local_bin=0
if [ "$path" = "loopback" ] || [ -z "$sender_ssh" ]; then
    need_local_bin=1
fi
if [ -z "$receiver_ssh" ]; then
    need_local_bin=1
fi
if [ "$need_local_bin" = "1" ]; then
    [ -x "$bin_rel" ] || die "missing $bin_rel (build with make wg-demo; P1 does not build)"
fi
if [ -n "$sender_ssh" ]; then
    ssh_cmd "$sender_ssh" "test -x \"$remote_repo/build/wg_multi_pipeline\"" \
        || die "sender missing executable: $sender_ssh:$remote_repo/build/wg_multi_pipeline"
fi
if [ -n "$receiver_ssh" ]; then
    ssh_cmd "$receiver_ssh" "test -x \"$remote_repo/build/wg_multi_pipeline\"" \
        || die "receiver missing executable: $receiver_ssh:$remote_repo/build/wg_multi_pipeline"
    if [ "$experiment" = "C" ]; then
        ssh_cmd "$receiver_ssh" "test -f \"$remote_repo/scripts/udp_loss_forwarder.py\"" \
            || die "receiver missing udp_loss_forwarder.py on $receiver_ssh"
    fi
fi

payload_bytes=$(wc -c < "$input_path" | tr -d ' ')
input_sha=$(sha256sum "$input_path" | awk '{print $1}')
input_sha16=$(echo "$input_sha" | cut -c1-16)

# Stage input onto sender once (reuse remote path for all --flow lines).
if [ -n "$sender_ssh" ]; then
    sender_input_path="/tmp/rs-pen-input-$input_sha16"
    echo "staging input -> $sender_ssh:$sender_input_path"
    scp_cmd "$input_path" "$sender_ssh:$sender_input_path" \
        || die "failed to scp input to sender $sender_ssh"
else
    sender_input_path=$input_path
fi

timestamp=$(date +%Y%m%d-%H%M%S)
if [ -z "$output_dir" ]; then
    output_dir="build/rs-compute-penalty-${experiment}-${timestamp}"
fi
mkdir -p "$output_dir/env" "$output_dir/runs" || die "cannot create output-dir $output_dir"
runs_csv="$output_dir/runs.csv"
expected_ids_file="$output_dir/env/expected_run_ids.txt"
: > "$expected_ids_file"
: > "$runs_csv"

# Resolve binary sha (local preferred; else sender remote).
if [ -x "$bin_rel" ]; then
    binary_sha=$(sha256sum "$bin_rel" | awk '{print $1}')
elif [ -n "$sender_ssh" ]; then
    binary_sha=$(ssh_cmd "$sender_ssh" "sha256sum \"$remote_repo/build/wg_multi_pipeline\"" | awk '{print $1}')
else
    binary_sha=NA
fi

{
    echo "timestamp=$(date -Iseconds 2>/dev/null || date)"
    echo "hostname=$(hostname)"
    echo "nproc=$(nproc 2>/dev/null || echo NA)"
    echo "uname=$(uname -a)"
    echo "loadavg=$(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null || echo NA)"
    gov=NA
    if [ -r /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor ]; then
        gov=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
    fi
    echo "cpu_governor=$gov"
    echo "git_rev=$(git rev-parse HEAD 2>/dev/null || echo NA)"
    if git diff --quiet 2>/dev/null; then
        echo "git_dirty=0"
    else
        echo "git_dirty=1"
        git diff > "$output_dir/env/git.diff" 2>/dev/null || true
    fi
    git status --short > "$output_dir/env/git-status.txt" 2>/dev/null || true
    echo "binary_sha256=$binary_sha"
    echo "input_path=$input_path"
    echo "input_sha256=$input_sha"
    echo "payload_bytes=$payload_bytes"
    echo "experiment=$experiment"
    echo "path=$path"
    echo "path_mode=$(path_mode_for)"
    echo "send_host=$send_host"
    echo "recv_port=$recv_port"
    echo "fwd_listen_port=$fwd_listen_port"
    echo "final_dst=$final_dst"
    echo "ttl=$ttl"
    echo "local_node_id=$local_node_id"
    echo "sender_ssh=${sender_ssh:-}"
    echo "receiver_ssh=${receiver_ssh:-}"
    echo "sender_data_ip=$sender_data_ip"
    echo "remote_repo=$remote_repo"
    echo "sender_input_path=$sender_input_path"
    echo "seed=$seed"
    echo "interleave_depth=1"
    echo "repetitions=$repetitions"
    echo "warmup=$warmup"
    echo "idle_sec=$idle_sec"
} > "$output_dir/env/snapshot.txt"
cp "$output_dir/env/snapshot.txt" "$output_dir/env/snapshot.env"
git_rev=$(grep '^git_rev=' "$output_dir/env/snapshot.txt" | cut -d= -f2)
nproc_v=$(grep '^nproc=' "$output_dir/env/snapshot.txt" | cut -d= -f2)
gov_v=$(grep '^cpu_governor=' "$output_dir/env/snapshot.txt" | cut -d= -f2)
load_v=$(grep '^loadavg=' "$output_dir/env/snapshot.txt" | cut -d= -f2-)

echo "RS compute-penalty P1  experiment=$experiment  path=$path  out=$output_dir"
echo "  path_mode=$(path_mode_for)  send=$send_host  recv_port=$recv_port  idle_sec=$idle_sec"
echo "  sender_ssh=${sender_ssh:-local}  receiver_ssh=${receiver_ssh:-local}"
echo "  final_dst=$final_dst ttl=$ttl local_node_id=$local_node_id"
echo "  input=$input_path bytes=$payload_bytes sha=$input_sha"
echo "  profiles=$profile_list  udp-send-multi single-flow  interleave_depth=1"
echo "  repetitions=$repetitions warmup=$warmup (POSIX sh, set -eu)"
if [ "$experiment" = "C" ]; then
    echo "  forwarder 0.0.0.0:$fwd_listen_port -> 127.0.0.1:$recv_port"
fi

if [ "$experiment" = "A" ]; then
    if [ "$skip_calibrate" != "1" ]; then
        echo "=== A calibrate none payload-rate ladder ==="
        best=
        for rate_mbps in $(comma_to_words "$calibrate_rates"); do
            rid="A_cal_none_pr${rate_mbps}"
            expect_run_id "$rid"
            run_once none "$rate_mbps" NA 0 1 0 "$rid" || true
            hash_st=$(cat "$output_dir/runs/$rid/hash.txt")
            ovf=$(python3 -c "import json; print(json.load(open('$output_dir/runs/$rid/row.json')).get('window_overflow','NA'))" 2>/dev/null || echo NA)
            echo "    hash=$hash_st overflow=$ovf"
            if [ "$hash_st" = "PASS" ] && [ "$ovf" = "0" ]; then
                best=$rate_mbps
            fi
        done
        [ -n "$best" ] || die "A calibration: no payload-rate with hash PASS and overflow=0"
        payload_rate=$best
        echo "  selected payload-rate=$payload_rate"
        echo "$payload_rate" > "$output_dir/env/selected_payload_rate.txt"
    fi
    [ -n "$payload_rate" ] || die "A requires --payload-rate or calibration"
    for profile_name in $profile_list; do
        echo "=== A profile $profile_name source=$payload_rate ==="
        run_profile_reps "$profile_name" "$payload_rate" NA 0
    done
elif [ "$experiment" = "B" ]; then
    rates_main=
    has_100=0
    for rate_mbps in $(comma_to_words "$wire_rates"); do
        if is_hundred "$rate_mbps"; then
            has_100=1
        else
            rates_main="$rates_main $rate_mbps"
        fi
    done
    for rate_mbps in $rates_main; do
        for profile_name in $profile_list; do
            krn=$(profile_krn "$profile_name")
            rs_k=$(echo "$krn" | awk '{print $1}')
            rs_n=$(echo "$krn" | awk '{print $3}')
            src=$(src_rate_for "$rs_k" "$rs_n" "$rate_mbps")
            echo "=== B $profile_name  W=$rate_mbps  source=$src ==="
            run_profile_reps "$profile_name" "$src" "$rate_mbps" 0
        done
    done
    if [ "$has_100" = "1" ]; then
        echo "=== B W=100 precheck (all profiles must hash PASS and overflow=0) ==="
        pre_ok=1
        for profile_name in $profile_list; do
            krn=$(profile_krn "$profile_name")
            rs_k=$(echo "$krn" | awk '{print $1}')
            rs_n=$(echo "$krn" | awk '{print $3}')
            src=$(src_rate_for "$rs_k" "$rs_n" 100)
            rid=$(make_run_id "$profile_name" 100 0 precheck)
            expect_run_id "$rid"
            run_once "$profile_name" "$src" 100 0 1 0 "$rid" || true
            if ! last_row_ok "$output_dir/runs/$rid/row.json"; then
                echo "  precheck FAIL $profile_name"
                pre_ok=0
            fi
        done
        if [ "$pre_ok" = "1" ]; then
            echo "  W=100 precheck passed; running official reps"
            for profile_name in $profile_list; do
                krn=$(profile_krn "$profile_name")
                rs_k=$(echo "$krn" | awk '{print $1}')
                rs_n=$(echo "$krn" | awk '{print $3}')
                src=$(src_rate_for "$rs_k" "$rs_n" 100)
                echo "=== B $profile_name  W=100  source=$src ==="
                run_profile_reps "$profile_name" "$src" 100 0
            done
        else
            echo "  skipping official W=100 (precheck failed)"
            echo "skipped W=100: not all profiles hash PASS and overflow=0" \
                > "$output_dir/env/skip_w100.txt"
        fi
    fi
elif [ "$experiment" = "C" ]; then
    for rate_mbps in $(comma_to_words "$wire_rates"); do
        for loss_value in $(comma_to_words "$losses"); do
            for profile_name in $profile_list; do
                krn=$(profile_krn "$profile_name")
                rs_k=$(echo "$krn" | awk '{print $1}')
                rs_n=$(echo "$krn" | awk '{print $3}')
                src=$(src_rate_for "$rs_k" "$rs_n" "$rate_mbps")
                echo "=== C $profile_name  W=$rate_mbps  loss=$loss_value  source=$src ==="
                run_profile_reps "$profile_name" "$src" "$rate_mbps" "$loss_value"
            done
        done
    done
fi

python3 "$parse_py" summarize --runs-csv "$runs_csv" --out-dir "$output_dir" \
    || die "summarize failed"

echo "=== verify expected vs runs.csv ==="
if ! python3 "$parse_py" verify --runs-csv "$runs_csv" --expected-ids "$expected_ids_file"; then
    note_infra_error "expected/actual run_id mismatch"
fi

official_n=$(python3 -c "import csv,sys
rows=list(csv.DictReader(open(sys.argv[1])))
print(sum(1 for r in rows if str(r.get('warmup','0')) in ('0','false','False')))" "$runs_csv")
echo "official_rows=$official_n runner_errors=$runner_errors"
echo "Done: $runs_csv"
echo "      $output_dir/summary.md"

if [ "$runner_errors" -ne 0 ]; then
    echo "error: $runner_errors infrastructure failure(s); see notes above" >&2
    exit 1
fi
exit 0
