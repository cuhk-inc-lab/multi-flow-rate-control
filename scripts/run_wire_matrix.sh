#!/usr/bin/env sh

# Run from Node1. Node4 must accept key-based SSH from Node1.
#
# Example:
#   CODECS="copy block xor-fec rs-fec" RATES="20 24 28 32" \
#     ./scripts/run_wire_matrix.sh fyp1@10.10.10.164 10.10.34.2 input-128m.ts
#
# Artifacts (kept lean):
#   results.md   — short table: status + loss% + key latency
#   results.csv  — machine-readable (includes full latency columns)
#   logs/        — per-case sender/receiver logs

set -u

receiver_ssh=${1:-}
receiver_ip=${2:-}
input_path=${3:-}
remote_repo=${RECEIVER_REPO:-"$HOME/work/multi-flow-rate-control"}
codecs=${CODECS:-"copy block xor-fec rs-fec"}
rates=${RATES:-"20 24 28 32"}
idle_sec=${IDLE_SEC:-5}
port_base=${PORT_BASE:-9000}
keep_remote=${KEEP_REMOTE_OUTPUT:-1}
decode_mark=${DECODE_MARK:-0}
timestamp=$(date +%Y%m%d-%H%M%S)
result_dir=${RESULT_DIR:-"build/wire-matrix-$timestamp"}
ssh_opts="-o BatchMode=yes -o ConnectTimeout=10"

usage() {
    echo "Usage: $0 RECEIVER_SSH RECEIVER_DATA_IP INPUT_FILE" >&2
    echo "Example: $0 fyp1@10.10.10.164 10.10.34.2 input-128m.ts" >&2
    echo "Set CODECS, RATES, RECEIVER_REPO, IDLE_SEC, PORT_BASE, RESULT_DIR," >&2
    echo "KEEP_REMOTE_OUTPUT=0 to delete receiver .ts after hash check," >&2
    echo "DECODE_MARK=1 to append a decode proof footer into the received file." >&2
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

# Print: est_dgram_loss_pct late_pct group_drop_pct recovered_pct
loss_stats() {
    codec=$1
    payload_bytes=$2
    datagrams=$3
    seen=$4
    late=$5
    dropped=$6
    recovered=$7

    python3 - "$codec" "$payload_bytes" "$datagrams" "$seen" "$late" "$dropped" "$recovered" <<'PY'
import math, sys

codec = sys.argv[1].strip().lower()
try:
    payload = int(sys.argv[2])
except ValueError:
    payload = 0

def as_int(v):
    if v in (None, "", "NA"):
        return None
    try:
        return int(v)
    except ValueError:
        return None

dgrams = as_int(sys.argv[3])
seen = as_int(sys.argv[4])
late = as_int(sys.argv[5]) or 0
dropped = as_int(sys.argv[6])
recovered = as_int(sys.argv[7])

shards = {"copy": 8, "block": 8, "xor-fec": 5, "rs-fec": 6, "none": 4}.get(codec, 8)
decode_block = 188 * 4
blocks = 0 if payload <= 0 else (payload + decode_block - 1) // decode_block
exp = blocks * shards
rx = seen if seen is not None else dgrams
if rx is not None and seen is None and late:
    # Older logs: late not included in datagrams
    rx = rx + late

def pct(num, den):
    if den <= 0 or num is None:
        return "NA"
    return f"{100.0 * float(num) / float(den):.4f}"

loss = "NA"
if exp > 0 and rx is not None:
    loss_v = max(0.0, 100.0 * (1.0 - float(rx) / float(exp)))
    if rx >= exp:
        loss_v = 0.0
    loss = f"{loss_v:.4f}"

print(loss, pct(late, exp), pct(dropped, blocks), pct(recovered, blocks))
PY
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

mkdir -p "$result_dir/logs" || die "cannot create result directory: $result_dir"
csv="$result_dir/results.csv"
markdown="$result_dir/results.md"
input_hash=$(sha256sum "$input_path" | awk '{print $1}')
input_bytes=$(wc -c < "$input_path" | tr -d ' ')
decode_mark_opt=
if [ "$decode_mark" = "1" ]; then
    decode_mark_opt="--decode-mark"
fi

ssh $ssh_opts "$receiver_ssh" \
    "cd '$remote_repo' && test -x ./build/wg_multi_pipeline" \
    || die "Node4 is not reachable with key-based SSH, or its binary is missing"

{
    echo "# Wire codec benchmark"
    echo
    echo "- Sender: $(hostname)"
    echo "- Receiver: $receiver_ssh ($receiver_ip)"
    echo "- Input: \`$input_path\` ($input_bytes bytes)"
    echo "- Codecs: $codecs"
    echo "- Rates: $rates Mbps (source)"
    echo "- PASS = sha256 match (with DECODE_MARK=1, hash FAIL is expected; look for [WG_DECODE_MARK] footer)"
    if [ "$decode_mark" = "1" ]; then
        echo "- Decode-mark: **ON** (receiver uses --decode-mark; footer proves Codec_decode ran)"
    else
        echo "- Decode-mark: off (set DECODE_MARK=1 to enable)"
    fi
    echo "- Receiver outputs kept on Node4: \`build/wire-matrix-${timestamp}-<codec>-<rate>m.ts\` (set KEEP_REMOTE_OUTPUT=0 to delete)"
    echo "- Est. dgram loss % ≈ \`1 - seen_datagrams / (ceil(bytes/752) × shards)\` (late counted as arrived when \`seen_datagrams\` present)"
    echo
    echo "| Codec | Mbps | Status | Mark | Est. loss % | Late % | Drop % | Recovered % | Wire Mbps | E2E p95 µs | Jitter p95 µs |"
    echo "| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
} > "$markdown"

echo "codec,target_source_mbps,actual_source_mbps,wire_mbps,status,hash_match,decode_mark_found,payload_bytes,receiver_bytes,datagrams,seen_datagrams,late,est_dgram_loss_pct,late_pct,group_drop_pct,recovered_pct,recovered_groups,dropped_groups,missing_data_shards,encode_avg_us,transfer_avg_us,decode_avg_us,e2e_avg_us,e2e_p50_us,e2e_p95_us,e2e_p99_us,jitter_avg_us,jitter_p95_us,jitter_p99_us" \
    > "$csv"

case_number=0
pass_count=0
total_count=0
for codec in $codecs; do
    for rate in $rates; do
        port=$((port_base + case_number))
        label="${codec}-${rate}m"
        remote_output="$remote_repo/build/wire-matrix-$timestamp-$label.ts"
        receiver_log="$result_dir/logs/$label-receiver.log"
        sender_log="$result_dir/logs/$label-sender.log"
        receiver_pid=
        sender_rc=1
        receiver_rc=1
        remote_hash=NA
        status=FAIL

        echo "=== $label: UDP port $port ==="
        # shellcheck disable=SC2086
        ssh $ssh_opts "$receiver_ssh" \
            "cd '$remote_repo' && exec ./build/wg_multi_pipeline --codec '$codec' --udp-recv '$port' '$remote_output' --idle-sec '$idle_sec' $decode_mark_opt" \
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
            mark_found=no
            if [ "$decode_mark" = "1" ]; then
                if ssh $ssh_opts "$receiver_ssh" \
                    "grep -q '\\[WG_DECODE_MARK\\]' '$remote_output' 2>/dev/null"; then
                    mark_found=yes
                elif grep -q 'decode-mark appended' "$receiver_log" 2>/dev/null; then
                    mark_found=yes
                fi
                if [ "$mark_found" = "yes" ] && [ "$status" != "PASS" ]; then
                    status=MARKED
                fi
            fi
            if [ "$keep_remote" != "1" ]; then
                ssh $ssh_opts "$receiver_ssh" "rm -f '$remote_output'" >/dev/null 2>&1 || true
            fi
        fi

        if [ -z "${mark_found:-}" ]; then
            mark_found=no
        fi

        actual_source=$(awk '/^udp-send:/ { for (i = 1; i <= NF; i++) if ($i ~ /^source_mbps=/) { sub(/^source_mbps=/, "", $i); value=$i } } END { print value == "" ? "NA" : value }' "$sender_log")
        wire_mbps=$(awk '/^udp-send:/ { for (i = 1; i <= NF; i++) if ($i ~ /^wire_mbps=/) { sub(/^wire_mbps=/, "", $i); value=$i } } END { print value == "" ? "NA" : value }' "$sender_log")
        receiver_bytes=$(csv_field output_bytes "$receiver_log")
        datagrams=$(csv_field datagrams "$receiver_log")
        seen=$(csv_field seen_datagrams "$receiver_log")
        late=$(csv_field late "$receiver_log")
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

        # shellcheck disable=SC2086
        set -- $(loss_stats "$codec" "$input_bytes" "$datagrams" "$seen" "$late" "$dropped" "$recovered")
        est_loss=${1:-NA}
        late_pct=${2:-NA}
        drop_pct=${3:-NA}
        rec_pct=${4:-NA}

        hash_match=no
        if [ "$remote_hash" = "$input_hash" ]; then
            hash_match=yes
        fi

        echo "$codec,$rate,$actual_source,$wire_mbps,$status,$hash_match,$mark_found,$input_bytes,$receiver_bytes,$datagrams,$seen,$late,$est_loss,$late_pct,$drop_pct,$rec_pct,$recovered,$dropped,$missing,$encode_avg,$transfer_avg,$decode_avg,$e2e_avg,$e2e_p50,$e2e_p95,$e2e_p99,$jitter_avg,$jitter_p95,$jitter_p99" \
            >> "$csv"
        echo "| $codec | $rate | $status | $mark_found | $est_loss | $late_pct | $drop_pct | $rec_pct | $wire_mbps | $e2e_p95 | $jitter_p95 |" \
            >> "$markdown"

        total_count=$((total_count + 1))
        if [ "$status" = "PASS" ] || [ "$status" = "MARKED" ]; then
            pass_count=$((pass_count + 1))
        fi
        echo "  -> $status  mark=$mark_found  loss%=$est_loss  late%=$late_pct"
        case_number=$((case_number + 1))
    done
done

{
    echo
    echo "## Summary"
    echo
    echo "- Cases PASS: **$pass_count / $total_count**"
    echo "- Detailed latency / counters: \`results.csv\`"
    echo "- Raw logs: \`logs/*-sender.log\`, \`logs/*-receiver.log\`"
    if [ "$keep_remote" = "1" ]; then
        echo "- Receiver files: \`$remote_repo/build/wire-matrix-${timestamp}-*.ts\` on $receiver_ssh"
    fi
} >> "$markdown"

echo
echo "Done. PASS $pass_count / $total_count"
echo "  Report: $markdown"
echo "  CSV:    $csv"
echo "  Logs:   $result_dir/logs/"
if [ "$keep_remote" = "1" ]; then
    echo "  Remote: $receiver_ssh:$remote_repo/build/wire-matrix-${timestamp}-*.ts"
fi
echo
cat "$markdown"
