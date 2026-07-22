#!/usr/bin/env sh

# Concurrent multi-destination wire test (iperf-like), run from Node1.
#
# Default streams (supervisor diagram):
#   1) Node1 -> Node4  10 Mbps / 10 s
#   2) Node2 -> Node4  10 Mbps / 10 s
#   3) Node1 -> Node2  20 Mbps / 10 s
#   4) Node2 -> Node2  20 Mbps / 10 s  (loopback)
#   5) Node1 -> Node3  10 Mbps / 10 s
#   6) Node1 -> Node4  20 Mbps /  5 s
#
# Example:
#   NODE2_SSH=fyp1@10.10.10.162 NODE2_IP=10.10.12.2 \
#   NODE3_SSH=fyp1@10.10.10.163 NODE3_IP=10.10.23.2 \
#   NODE4_SSH=fyp1@10.10.10.164 NODE4_IP=10.10.34.2 \
#   NODE2_IFACES="ens5 ens6" NODE3_IFACES="ens5 ens6" \
#     ./scripts/run_iperf_like_wire.sh input-128m.ts

set -u

input_path=${1:-}
codec=${CODEC:-copy}
port=${PORT:-9000}
idle_sec=${IDLE_SEC:-8}
barrier_sec=${BARRIER_SEC:-5}
monitor_hz=${MONITOR_HZ:-1}
remote_repo=${REMOTE_REPO:-"$HOME/work/multi-flow-rate-control"}
ssh_opts="-o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=5 -o ServerAliveCountMax=2"
timestamp=$(date +%Y%m%d-%H%M%S)
result_dir=${RESULT_DIR:-"build/iperf-like-wire-$timestamp"}
local_work="$result_dir/payloads"
bin_rel="./build/wg_multi_pipeline"
remote_run_id="iperf-like-$timestamp"
ssh_cmd_timeout=${SSH_CMD_TIMEOUT:-20}

node2_ssh=${NODE2_SSH:-}
node3_ssh=${NODE3_SSH:-}
node4_ssh=${NODE4_SSH:-}
node2_ip=${NODE2_IP:-}
node3_ip=${NODE3_IP:-}
node4_ip=${NODE4_IP:-}
node2_ifaces=${NODE2_IFACES:-}
node3_ifaces=${NODE3_IFACES:-}

usage() {
    cat >&2 <<'EOF'
Usage: ./scripts/run_iperf_like_wire.sh INPUT_FILE

Required env:
  NODE2_SSH NODE2_IP
  NODE3_SSH NODE3_IP
  NODE4_SSH NODE4_IP

Optional env:
  CODEC=copy|block|xor-fec|rs-fec   (default: copy)
  PORT=9000
  IDLE_SEC=8
  BARRIER_SEC=5
  MONITOR_HZ=1
  REMOTE_REPO=$HOME/work/multi-flow-rate-control
  RESULT_DIR=build/iperf-like-wire-<timestamp>
  NODE2_IFACES="ens5 ens6"
  NODE3_IFACES="ens5 ens6"

Example:
  NODE2_SSH=fyp1@10.10.10.162 NODE2_IP=10.10.12.2 \
  NODE3_SSH=fyp1@10.10.10.163 NODE3_IP=10.10.23.2 \
  NODE4_SSH=fyp1@10.10.10.164 NODE4_IP=10.10.34.2 \
  NODE2_IFACES="ens5 ens6" NODE3_IFACES="ens5 ens6" \
    ./scripts/run_iperf_like_wire.sh input-128m.ts
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

need() {
    eval "val=\${$1:-}"
    [ -n "$val" ] || die "missing required env: $1"
}

bytes_for() {
    awk -v r="$1" -v d="$2" 'BEGIN { printf "%.0f", r * d * 1000000 / 8 }'
}

ssh_run() {
    host=$1
    shift
    # shellcheck disable=SC2086
    ssh $ssh_opts "$host" "$@"
}

# Like ssh_run, but fail fast if the remote command stalls (e.g. bad detach).
ssh_run_timeout() {
    host=$1
    shift
    if command -v timeout >/dev/null 2>&1; then
        # shellcheck disable=SC2086
        timeout "$ssh_cmd_timeout" ssh $ssh_opts "$host" "$@"
    else
        ssh_run "$host" "$@"
    fi
}

gen_payload() {
    src=$1
    out=$2
    nbytes=$3
    tag=$4
    python3 - "$src" "$out" "$nbytes" "$tag" <<'PY'
import hashlib
import sys

src, out, n, tag = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
seed = hashlib.sha256(tag.encode()).digest()
written = 0
bufsize = 64 * 1024
with open(src, "rb") as f, open(out, "wb") as g:
    prefix = (seed * ((min(n, 4096) + 31) // 32))[: min(n, 4096)]
    g.write(prefix)
    written = len(prefix)
    while written < n:
        chunk = f.read(min(bufsize, n - written))
        if not chunk:
            f.seek(0)
            continue
        g.write(chunk)
        written += len(chunk)
PY
}

latency_field() {
    metric=$1
    key=$2
    file=$3
    awk -v wanted_metric="$metric:" -v wanted_key="$key" '
        $1 == "latency" && $2 == wanted_metric {
            for (i = 3; i <= NF; i++) {
                split($i, field, "=")
                if (field[1] == wanted_key) value = field[2]
            }
        }
        END { print value == "" ? "NA" : value }
    ' "$file"
}

csv_field() {
    key=$1
    file=$2
    awk -v wanted="$key" '
        /^udp-recv:/ {
            for (i = 1; i <= NF; i++) {
                split($i, field, "=")
                if (field[1] == wanted) value = field[2]
            }
        }
        END { print value == "" ? "NA" : value }
    ' "$file"
}

match_hash_in_dir() {
    want_hash=$1
    dir=$2
    for f in "$dir"/*; do
        [ -f "$f" ] || continue
        h=$(sha256sum "$f" | awk '{print $1}')
        if [ "$h" = "$want_hash" ]; then
            printf '%s\n' "$f"
            return 0
        fi
    done
    return 1
}

relay_peak() {
    csv=$1
    [ -f "$csv" ] || { echo "NA"; return; }
    awk -F, 'NR > 1 {
        if ($3 + 0 > rx) rx = $3 + 0
        if ($4 + 0 > tx) tx = $4 + 0
      }
      END {
        if (NR <= 1) print "NA"
        else printf "rx_peak_bps=%.0f tx_peak_bps=%.0f", rx, tx
      }' "$csv"
}

capture_link_stats() {
    host=$1
    ifaces=$2
    outfile=$3
    [ -n "$ifaces" ] || { echo "no-ifaces" > "$outfile"; return; }
    ssh_run "$host" "python3 -c \"
import time
ifaces = '''$ifaces'''.split()
print('ts', int(time.time()))
for iface in ifaces:
    base = '/sys/class/net/%s/statistics' % iface
    try:
        vals = []
        for name in ('rx_bytes','tx_bytes','rx_packets','tx_packets','rx_dropped','tx_dropped','rx_errors','tx_errors'):
            with open(base + '/' + name) as f:
                vals.append('%s=%s' % (name, f.read().strip()))
        print(iface, ' '.join(vals))
    except Exception as e:
        print(iface, 'ERROR', e)
\"" > "$outfile"
}

install_remote_monitor() {
    host=$1
    ifaces=$2
    remote_csv=$3
    hz=$4
    [ -n "$ifaces" ] || { echo ""; return; }

    # Upload a tiny monitor script, then nohup it.
    mon_local="$result_dir/remote-monitor.py"
    cat > "$mon_local" <<'PY'
import sys, time, os
ifaces = sys.argv[1].split()
hz = float(sys.argv[2])
log = sys.argv[3]

def read_stats(iface):
    base = "/sys/class/net/%s/statistics" % iface
    out = {}
    for name in ("rx_bytes", "tx_bytes", "rx_dropped", "tx_dropped", "rx_errors", "tx_errors"):
        with open("%s/%s" % (base, name)) as f:
            out[name] = int(f.read().strip())
    return out

prev = {}
for iface in ifaces:
    path = "/sys/class/net/%s/statistics" % iface
    if os.path.isdir(path):
        prev[iface] = read_stats(iface)
prev_t = time.time()
with open(log, "w") as g:
    g.write("iface,ts,rx_bps,tx_bps,rx_drop,tx_drop,rx_err,tx_err\n")
while True:
    time.sleep(hz)
    now = time.time()
    dt = max(now - prev_t, 1e-6)
    with open(log, "a") as g:
        for iface, old in list(prev.items()):
            cur = read_stats(iface)
            rx_bps = (cur["rx_bytes"] - old["rx_bytes"]) * 8.0 / dt
            tx_bps = (cur["tx_bytes"] - old["tx_bytes"]) * 8.0 / dt
            g.write("%s,%d,%.0f,%.0f,%d,%d,%d,%d\n" % (
                iface, int(now), rx_bps, tx_bps,
                cur["rx_dropped"], cur["tx_dropped"],
                cur["rx_errors"], cur["tx_errors"]))
            prev[iface] = cur
    prev_t = now
PY
    # shellcheck disable=SC2086
    scp $ssh_opts "$mon_local" "$host:$remote_repo/build/$remote_run_id/monitor/monitor.py" >/dev/null
    ssh_run "$host" "nohup python3 '$remote_repo/build/$remote_run_id/monitor/monitor.py' '$ifaces' '$hz' '$remote_csv' >/dev/null 2>&1 & echo \$!"
}

if [ -z "$input_path" ]; then
    usage
    exit 2
fi
[ -f "$input_path" ] || die "input file does not exist: $input_path"
[ -x ./build/wg_multi_pipeline ] || die "local binary missing; run make wg-demo first"

need NODE2_SSH
need NODE3_SSH
need NODE4_SSH
need NODE2_IP
need NODE3_IP
need NODE4_IP

mkdir -p "$result_dir/payloads" "$result_dir/logs" "$result_dir/remote" \
    "$result_dir/monitor" "$result_dir/out/n2" "$result_dir/out/n3" "$result_dir/out/n4" \
    || die "cannot create $result_dir"

# sid sender rate dur label outdir_key
# sender: local|node2
# outdir_key: n2|n3|n4
stream_defs="
1 local 10 10 s1 n4 $node4_ip
2 node2 10 10 s2 n4 $node4_ip
3 local 20 10 s3 n2 $node2_ip
4 node2 20 10 s4 n2 127.0.0.1
5 local 10 10 s5 n3 $node3_ip
6 local 20 5 s6 n4 $node4_ip
"

echo "== preparing payloads =="
echo "$stream_defs" | while read -r sid sender rate dur label outkey dst; do
    [ -n "${sid:-}" ] || continue
    nbytes=$(bytes_for "$rate" "$dur")
    out="$local_work/${label}.ts"
    echo "  stream$sid -> $out (${nbytes} bytes, ${rate}Mbps x ${dur}s)"
    gen_payload "$input_path" "$out" "$nbytes" "stream$sid"
    sha256sum "$out" > "$local_work/${label}.sha256"
done

echo "== checking remote binaries =="
for host in "$node2_ssh" "$node3_ssh" "$node4_ssh"; do
    ssh_run "$host" "cd '$remote_repo' && test -x $bin_rel" \
        || die "binary missing on $host"
done

echo "== creating remote dirs =="
for host in "$node2_ssh" "$node3_ssh" "$node4_ssh"; do
    ssh_run "$host" "mkdir -p '$remote_repo/build/$remote_run_id'/{out,logs,monitor}"
done

echo "== syncing Node2 payloads =="
ssh_run "$node2_ssh" "mkdir -p '$remote_repo/build/iperf-like-payloads'"
# shellcheck disable=SC2086
scp $ssh_opts "$local_work/s2.ts" "$local_work/s4.ts" \
    "$node2_ssh:$remote_repo/build/iperf-like-payloads/" >/dev/null

echo "== capturing pre-test link counters =="
capture_link_stats "$node2_ssh" "$node2_ifaces" "$result_dir/monitor/node2-link-before.txt"
capture_link_stats "$node3_ssh" "$node3_ifaces" "$result_dir/monitor/node3-link-before.txt"

start_remote_receiver() {
    host=$1
    out_prefix=$2
    max_flows=$3
    log_rel=$4
    pid_rel=$5

    # Write PID on the remote host, then print it. Avoid relying on SSH staying
    # attached to the long-running udp-recv process.
    if ! out=$(ssh_run_timeout "$host" "cd '$remote_repo' || exit 1
rm -f '$pid_rel'
if command -v setsid >/dev/null 2>&1; then
  setsid $bin_rel --codec '$codec' --udp-recv '$port' '$out_prefix' --max-flows '$max_flows' --idle-sec '$idle_sec' >'$log_rel' 2>&1 </dev/null &
else
  nohup $bin_rel --codec '$codec' --udp-recv '$port' '$out_prefix' --max-flows '$max_flows' --idle-sec '$idle_sec' >'$log_rel' 2>&1 </dev/null &
fi
echo \$! > '$pid_rel'
sleep 0.3
if ! kill -0 \"\$(cat '$pid_rel')\" 2>/dev/null; then
  echo \"receiver exited immediately on $host\" >&2
  tail -n 40 '$log_rel' >&2 || true
  exit 1
fi
cat '$pid_rel'"); then
        die "failed to start receiver on $host (ssh timeout=${ssh_cmd_timeout}s). Try manually:
  ssh $host 'cd $remote_repo && $bin_rel --codec $codec --udp-recv $port /tmp/manual_ --max-flows 2 --idle-sec 3'"
    fi
    printf '%s\n' "$out" | tr -d ' \r' | tail -n 1
}

echo "== starting receivers =="
echo "  Node4 receiver..."
n4_recv_pid=$(start_remote_receiver "$node4_ssh" \
    "build/$remote_run_id/out/n4_" 8 \
    "build/$remote_run_id/logs/n4-recv.log" \
    "build/$remote_run_id/logs/n4-recv.pid")
echo "  Node2 receiver..."
n2_recv_pid=$(start_remote_receiver "$node2_ssh" \
    "build/$remote_run_id/out/n2_" 8 \
    "build/$remote_run_id/logs/n2-recv.log" \
    "build/$remote_run_id/logs/n2-recv.pid")
echo "  Node3 receiver..."
n3_recv_pid=$(start_remote_receiver "$node3_ssh" \
    "build/$remote_run_id/out/n3_" 4 \
    "build/$remote_run_id/logs/n3-recv.log" \
    "build/$remote_run_id/logs/n3-recv.pid")
echo "  pids: n4=$n4_recv_pid n2=$n2_recv_pid n3=$n3_recv_pid" > "$result_dir/remote/recv-pids.txt"
echo "  pids: n4=$n4_recv_pid n2=$n2_recv_pid n3=$n3_recv_pid"
printf '%s\n' "$n4_recv_pid" > "$result_dir/remote/n4-recv.pid"
printf '%s\n' "$n2_recv_pid" > "$result_dir/remote/n2-recv.pid"
printf '%s\n' "$n3_recv_pid" > "$result_dir/remote/n3-recv.pid"
sleep 1

echo "== starting relay monitors =="
n2_mon_pid=$(install_remote_monitor "$node2_ssh" "$node2_ifaces" \
    "$remote_repo/build/$remote_run_id/monitor/node2-ifaces.csv" "$monitor_hz")
n3_mon_pid=$(install_remote_monitor "$node3_ssh" "$node3_ifaces" \
    "$remote_repo/build/$remote_run_id/monitor/node3-ifaces.csv" "$monitor_hz")
echo "${n2_mon_pid:-}" > "$result_dir/remote/n2-mon.pid"
echo "${n3_mon_pid:-}" > "$result_dir/remote/n3-mon.pid"

start_at=$(( $(date +%s) + barrier_sec ))
echo "== barrier START_AT=$start_at (wait ${barrier_sec}s) =="

# Node1 sender: streams 1,3,5,6
(
    while [ "$(date +%s)" -lt "$start_at" ]; do sleep 0.05; done
    ./build/wg_multi_pipeline --codec "$codec" --udp-send-multi \
        --flow "${node4_ip}:${port}:${local_work}/s1.ts:10" \
        --flow "${node2_ip}:${port}:${local_work}/s3.ts:20" \
        --flow "${node3_ip}:${port}:${local_work}/s5.ts:10" \
        --flow "${node4_ip}:${port}:${local_work}/s6.ts:20" \
        > "$result_dir/logs/node1-send.log" 2>&1
) &
local_send_pid=$!

# Node2 sender: streams 2,4
ssh_run "$node2_ssh" "cd '$remote_repo' && nohup sh -c '
  start_at=$start_at
  while [ \"\$(date +%s)\" -lt \"\$start_at\" ]; do sleep 0.05; done
  $bin_rel --codec \"$codec\" --udp-send-multi \
    --flow \"${node4_ip}:${port}:build/iperf-like-payloads/s2.ts:10\" \
    --flow \"127.0.0.1:${port}:build/iperf-like-payloads/s4.ts:20\" \
    > \"build/$remote_run_id/logs/n2-send.log\" 2>&1
' >/dev/null 2>&1 </dev/null & echo \$!" > "$result_dir/remote/n2-send.pid"
n2_send_pid=$(tr -d ' \n' < "$result_dir/remote/n2-send.pid")
echo "  Node2 sender pid=$n2_send_pid"

echo "== waiting for senders =="
wait "$local_send_pid"
local_send_rc=$?
ssh_run "$node2_ssh" "while kill -0 $n2_send_pid 2>/dev/null; do sleep 0.5; done" || true

echo "== waiting for receivers =="
for pair in "$node4_ssh:$n4_recv_pid" "$node2_ssh:$n2_recv_pid" "$node3_ssh:$n3_recv_pid"; do
    host=${pair%%:*}
    pid=${pair##*:}
    ssh_run "$host" "while kill -0 $pid 2>/dev/null; do sleep 0.5; done" || true
done

echo "== stopping monitors =="
[ -n "${n2_mon_pid:-}" ] && ssh_run "$node2_ssh" "kill $n2_mon_pid 2>/dev/null || true"
[ -n "${n3_mon_pid:-}" ] && ssh_run "$node3_ssh" "kill $n3_mon_pid 2>/dev/null || true"

capture_link_stats "$node2_ssh" "$node2_ifaces" "$result_dir/monitor/node2-link-after.txt"
capture_link_stats "$node3_ssh" "$node3_ifaces" "$result_dir/monitor/node3-link-after.txt"

echo "== collecting artifacts =="
# shellcheck disable=SC2086
scp $ssh_opts "$node4_ssh:$remote_repo/build/$remote_run_id/logs/n4-recv.log" "$result_dir/logs/" >/dev/null
# shellcheck disable=SC2086
scp $ssh_opts "$node2_ssh:$remote_repo/build/$remote_run_id/logs/n2-recv.log" "$result_dir/logs/" >/dev/null
# shellcheck disable=SC2086
scp $ssh_opts "$node3_ssh:$remote_repo/build/$remote_run_id/logs/n3-recv.log" "$result_dir/logs/" >/dev/null
# shellcheck disable=SC2086
scp $ssh_opts "$node2_ssh:$remote_repo/build/$remote_run_id/logs/n2-send.log" "$result_dir/logs/" >/dev/null 2>/dev/null || true
# shellcheck disable=SC2086
scp $ssh_opts "$node2_ssh:$remote_repo/build/$remote_run_id/monitor/node2-ifaces.csv" "$result_dir/monitor/" >/dev/null 2>/dev/null || true
# shellcheck disable=SC2086
scp $ssh_opts "$node3_ssh:$remote_repo/build/$remote_run_id/monitor/node3-ifaces.csv" "$result_dir/monitor/" >/dev/null 2>/dev/null || true
# shellcheck disable=SC2086
scp $ssh_opts "$node2_ssh:$remote_repo/build/$remote_run_id/out/n2_*" "$result_dir/out/n2/" >/dev/null 2>/dev/null || true
# shellcheck disable=SC2086
scp $ssh_opts "$node3_ssh:$remote_repo/build/$remote_run_id/out/n3_*" "$result_dir/out/n3/" >/dev/null 2>/dev/null || true
# shellcheck disable=SC2086
scp $ssh_opts "$node4_ssh:$remote_repo/build/$remote_run_id/out/n4_*" "$result_dir/out/n4/" >/dev/null 2>/dev/null || true

streams_csv="$result_dir/streams.csv"
summary_md="$result_dir/summary.md"
echo "stream,sender,dst,rate_mbps,duration_s,payload_bytes,status,matched_output,e2e_p95_us,jitter_p95_us,recovered_groups,dropped_groups,missing_data_shards" \
    > "$streams_csv"

pass_count=0
total=0

echo "== validating streams by hash =="
echo "$stream_defs" | while read -r sid sender rate dur label outkey dst; do
    [ -n "${sid:-}" ] || continue
    # note: while-loop subshell cannot update outer pass_count; write temp lines then recount
    :
done

# Validate outside subshell
tmp_rows="$result_dir/remote/stream-rows.tmp"
: > "$tmp_rows"
echo "$stream_defs" | while read -r sid sender rate dur label outkey dst; do
    [ -n "${sid:-}" ] || continue
    payload="$local_work/${label}.ts"
    want_hash=$(awk '{print $1}' "$local_work/${label}.sha256")
    nbytes=$(wc -c < "$payload" | tr -d ' ')
    out_dir="$result_dir/out/$outkey"
    status=FAIL
    matched=NA
    if matched=$(match_hash_in_dir "$want_hash" "$out_dir"); then
        status=PASS
    else
        matched=NA
    fi

    case "$outkey" in
        n4) recv_log="$result_dir/logs/n4-recv.log" ;;
        n2) recv_log="$result_dir/logs/n2-recv.log" ;;
        n3) recv_log="$result_dir/logs/n3-recv.log" ;;
        *) recv_log="" ;;
    esac

    e2e_p95=NA
    jit_p95=NA
    recovered=NA
    dropped=NA
    missing=NA
    if [ -f "$recv_log" ]; then
        e2e_p95=$(latency_field end_to_end p95_us "$recv_log")
        jit_p95=$(latency_field end_to_end_jitter p95_us "$recv_log")
        recovered=$(csv_field recovered_groups "$recv_log")
        dropped=$(csv_field dropped_groups "$recv_log")
        missing=$(csv_field missing_data_shards "$recv_log")
    fi

    echo "$sid,$sender,$dst,$rate,$dur,$nbytes,$status,$matched,$e2e_p95,$jit_p95,$recovered,$dropped,$missing" >> "$tmp_rows"
    echo "  stream$sid [$label] $sender -> $dst : $status"
done

cat "$tmp_rows" >> "$streams_csv"
pass_count=$(awk -F, '$7=="PASS"{c++} END{print c+0}' "$tmp_rows")
total=$(awk -F, 'NF{c++} END{print c+0}' "$tmp_rows")

{
    echo "# Iperf-like concurrent wire test"
    echo
    echo "- Timestamp: $timestamp"
    echo "- Codec: $codec"
    echo "- UDP port: $port"
    echo "- Barrier START_AT: $start_at"
    echo "- Input seed: $input_path"
    echo "- Git: $(git rev-parse --short HEAD 2>/dev/null || echo NA)"
    echo "- Streams PASS: $pass_count / $total"
    echo "- Node1 sender exit code: $local_send_rc"
    echo
    echo "## Stream map"
    echo
    echo "| Stream | Sender | Destination | Rate Mbps | Duration s |"
    echo "| --- | --- | --- | ---: | ---: |"
    echo "| 1 | Node1 | Node4 ($node4_ip) | 10 | 10 |"
    echo "| 2 | Node2 | Node4 ($node4_ip) | 10 | 10 |"
    echo "| 3 | Node1 | Node2 ($node2_ip) | 20 | 10 |"
    echo "| 4 | Node2 | Node2 (127.0.0.1) | 20 | 10 |"
    echo "| 5 | Node1 | Node3 ($node3_ip) | 10 | 10 |"
    echo "| 6 | Node1 | Node4 ($node4_ip) | 20 | 5 |"
    echo
    echo "## Relay peaks"
    echo
    echo "- Node2 ($node2_ifaces): $(relay_peak "$result_dir/monitor/node2-ifaces.csv")"
    echo "- Node3 ($node3_ifaces): $(relay_peak "$result_dir/monitor/node3-ifaces.csv")"
    echo
    echo "## Artifacts"
    echo
    echo "- \`streams.csv\` : per-stream PASS/FAIL + latency fields"
    echo "- \`logs/\` : sender/receiver logs"
    echo "- \`monitor/\` : relay iface timeseries + before/after counters"
    echo "- \`out/\` : fetched decoded outputs"
} > "$summary_md"

echo
echo "Done."
echo "  Summary: $summary_md"
echo "  CSV:     $streams_csv"
echo "  PASS:    $pass_count / $total"
cat "$summary_md"
