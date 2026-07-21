#!/usr/bin/env sh

# Run from Node1. Node4 must accept key-based SSH from Node1.
#
# Example:
#   CODECS="copy block xor-fec rs-fec" RATES="20 24 28 32" \
#     ./scripts/run_wire_matrix.sh fyp1@10.10.10.164 10.10.34.2 input-128m.ts

set -u

receiver_ssh=${1:-}
receiver_ip=${2:-}
input_path=${3:-}
remote_repo=${RECEIVER_REPO:-"$HOME/work/multi-flow-rate-control"}
codecs=${CODECS:-"copy block xor-fec rs-fec"}
rates=${RATES:-"20 24 28 32"}
idle_sec=${IDLE_SEC:-5}
port_base=${PORT_BASE:-9000}
timestamp=$(date +%Y%m%d-%H%M%S)
result_dir=${RESULT_DIR:-"build/wire-matrix-$timestamp"}
ssh_opts="-o BatchMode=yes -o ConnectTimeout=10"

usage() {
    echo "Usage: $0 RECEIVER_SSH RECEIVER_DATA_IP INPUT_FILE" >&2
    echo "Example: $0 fyp1@10.10.10.164 10.10.34.2 input-128m.ts" >&2
    echo "Set CODECS, RATES, RECEIVER_REPO, IDLE_SEC, PORT_BASE, or RESULT_DIR as needed." >&2
}

die() {
    echo "error: $*" >&2
    exit 1
}

csv_field() {
    key=$1
    file=$2

    awk -v wanted="$key" '
        /^udp-recv:/ {
            for (i = 1; i <= NF; i++) {
                split($i, field, "=")
                if (field[1] == wanted) {
                    value = field[2]
                }
            }
        }
        END { print value == "" ? "NA" : value }
    ' "$file"
}

latency_field() {
    metric=$1
    key=$2
    file=$3

    awk -v wanted_metric="$metric:" -v wanted_key="$key" '
        $1 == "latency" && $2 == wanted_metric {
            for (i = 3; i <= NF; i++) {
                split($i, field, "=")
                if (field[1] == wanted_key) {
                    value = field[2]
                }
            }
        }
        END { print value == "" ? "NA" : value }
    ' "$file"
}

if [ -z "$receiver_ssh" ] || [ -z "$receiver_ip" ] || [ -z "$input_path" ]; then
    usage
    exit 2
fi
if [ ! -f "$input_path" ]; then
    die "input file does not exist: $input_path"
fi
if [ ! -x ./build/wg_multi_pipeline ]; then
    die "build/wg_multi_pipeline is missing; run make wg-demo first"
fi

mkdir -p "$result_dir" || die "cannot create result directory: $result_dir"
csv="$result_dir/results.csv"
markdown="$result_dir/results.md"
input_hash=$(sha256sum "$input_path" | awk '{print $1}')

ssh $ssh_opts "$receiver_ssh" \
    "cd '$remote_repo' && test -x ./build/wg_multi_pipeline" \
    || die "Node4 is not reachable with key-based SSH, or its binary is missing"

{
    echo "# Wire codec benchmark"
    echo
    echo "- Sender: $(hostname)"
    echo "- Receiver SSH: $receiver_ssh"
    echo "- Receiver data IP: $receiver_ip"
    echo "- Input: $input_path"
    echo "- Input SHA-256: $input_hash"
    echo "- Codecs: $codecs"
    echo "- Configured source rates: $rates Mbps"
    echo
    echo "| Codec | Source target Mbps | Source actual Mbps | Wire Mbps | Status | E2E avg µs | E2E p95 µs | E2E p99 µs | Jitter p95 µs | Recovered groups |"
    echo "| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |"
} > "$markdown"

echo "codec,target_source_mbps,actual_source_mbps,wire_mbps,status,hash_match,receiver_bytes,datagrams,recovered_groups,dropped_groups,missing_data_shards,encode_avg_us,transfer_avg_us,decode_avg_us,e2e_avg_us,e2e_p50_us,e2e_p95_us,e2e_p99_us,jitter_avg_us,jitter_p95_us,jitter_p99_us" \
    > "$csv"

case_number=0
for codec in $codecs; do
    for rate in $rates; do
        port=$((port_base + case_number))
        label="${codec}-${rate}m"
        remote_output="$remote_repo/build/wire-matrix-$timestamp-$label.ts"
        receiver_log="$result_dir/$label-receiver.log"
        sender_log="$result_dir/$label-sender.log"
        receiver_pid=
        sender_rc=1
        receiver_rc=1
        remote_hash=NA
        status=FAIL

        echo "=== $label: UDP port $port ==="
        ssh $ssh_opts "$receiver_ssh" \
            "cd '$remote_repo' && exec ./build/wg_multi_pipeline --codec '$codec' --udp-recv '$port' '$remote_output' --idle-sec '$idle_sec'" \
            > "$receiver_log" 2>&1 &
        receiver_pid=$!
        sleep 1

        if ! kill -0 "$receiver_pid" 2>/dev/null; then
            wait "$receiver_pid" || true
            echo "receiver exited before sender started; see $receiver_log" >&2
        else
            if ./build/wg_multi_pipeline --codec "$codec" --rate-mbps "$rate" \
                --udp-send "$receiver_ip" "$port" "$input_path" \
                > "$sender_log" 2>&1; then
                sender_rc=0
            fi
            if wait "$receiver_pid"; then
                receiver_rc=0
            fi
            remote_hash=$(ssh $ssh_opts "$receiver_ssh" \
                "sha256sum '$remote_output' 2>/dev/null | awk '{print \$1}'" \
                2>/dev/null || printf 'NA')
            if [ "$sender_rc" -eq 0 ] && [ "$receiver_rc" -eq 0 ] &&
                [ "$remote_hash" = "$input_hash" ]; then
                status=PASS
            fi
        fi

        actual_source=$(awk '/^udp-send:/ { for (i = 1; i <= NF; i++) if ($i ~ /^source_mbps=/) { sub(/^source_mbps=/, "", $i); value=$i } } END { print value == "" ? "NA" : value }' "$sender_log")
        wire_mbps=$(awk '/^udp-send:/ { for (i = 1; i <= NF; i++) if ($i ~ /^wire_mbps=/) { sub(/^wire_mbps=/, "", $i); value=$i } } END { print value == "" ? "NA" : value }' "$sender_log")
        receiver_bytes=$(csv_field output_bytes "$receiver_log")
        datagrams=$(csv_field datagrams "$receiver_log")
        recovered=$(csv_field recovered_groups "$receiver_log")
        dropped=$(csv_field dropped_groups "$receiver_log")
        missing=$(csv_field missing_data_shards "$receiver_log")
        encode_avg=$(latency_field encode avg_us "$receiver_log")
        transfer_avg=$(latency_field transfer avg_us "$receiver_log")
        decode_avg=$(latency_field decode avg_us "$receiver_log")
        e2e_avg=$(latency_field end_to_end avg_us "$receiver_log")
        e2e_p50=$(latency_field end_to_end p50_us "$receiver_log")
        e2e_p95=$(latency_field end_to_end p95_us "$receiver_log")
        e2e_p99=$(latency_field end_to_end p99_us "$receiver_log")
        jitter_avg=$(latency_field end_to_end_jitter avg_us "$receiver_log")
        jitter_p95=$(latency_field end_to_end_jitter p95_us "$receiver_log")
        jitter_p99=$(latency_field end_to_end_jitter p99_us "$receiver_log")

        echo "$codec,$rate,$actual_source,$wire_mbps,$status,$([ "$remote_hash" = "$input_hash" ] && echo yes || echo no),$receiver_bytes,$datagrams,$recovered,$dropped,$missing,$encode_avg,$transfer_avg,$decode_avg,$e2e_avg,$e2e_p50,$e2e_p95,$e2e_p99,$jitter_avg,$jitter_p95,$jitter_p99" \
            >> "$csv"
        echo "| $codec | $rate | $actual_source | $wire_mbps | $status | $e2e_avg | $e2e_p95 | $e2e_p99 | $jitter_p95 | $recovered |" \
            >> "$markdown"
        case_number=$((case_number + 1))
    done
done

echo "Results:"
echo "  CSV:      $csv"
echo "  Markdown: $markdown"
cat "$markdown"
