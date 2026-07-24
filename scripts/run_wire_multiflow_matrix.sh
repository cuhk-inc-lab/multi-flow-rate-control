#!/usr/bin/env sh

# Concurrent multi-flow wire matrix: Node1 -> Node4 only.
# Run from Node1. Node4 must accept key-based SSH from Node1.
#
# Provide one local file per flow (recommended):
#   CODECS="copy xor-fec" RATES="10 20" \
#     ./scripts/run_wire_multiflow_matrix.sh fyp1@10.10.10.164 10.10.34.2 \
#       a.bin b.bin c.bin d.bin
#
# Or one seed file (legacy): script synthesizes FLOWS distinct payloads:
#   FLOWS=4 DURATION_S=10 RATES="10 20" \
#     ./scripts/run_wire_multiflow_matrix.sh fyp1@10.10.10.164 10.10.34.2 seed.ts
#
# Artifacts under build/wire-multiflow-<ts>/:
#   results.md   — summary + per-case / per-flow tables
#   results.csv  — one row per codec×rate case (aggregate)
#   flows.csv    — one row per flow within each case
#   logs/        — sender/receiver logs
#   payloads/    — copies/hashes of per-flow inputs
#   out/         — fetched receiver outputs (optional)

set -u

receiver_ssh=${1:-}
receiver_ip=${2:-}
if [ "$#" -ge 2 ]; then
    shift 2
fi
# "$@" = input file list

remote_repo=${RECEIVER_REPO:-"$HOME/work/multi-flow-rate-control"}
codecs=${CODECS:-"copy"}
rates=${RATES:-"10 20"}
dur_s=${DURATION_S:-10}
idle_sec=${IDLE_SEC:-10}
port_base=${PORT_BASE:-9100}
keep_remote=${KEEP_REMOTE_OUTPUT:-1}
fetch_out=${FETCH_OUTPUT:-1}
use_no_pace=${USE_NO_PACE:-0}
timestamp=$(date +%Y%m%d-%H%M%S)
result_dir=${RESULT_DIR:-"build/wire-multiflow-$timestamp"}
ssh_opts="-o BatchMode=yes -o ConnectTimeout=10"
bin_rel="./build/wg_multi_pipeline"
input_count=$#
user_files=0
flows=

if [ "$input_count" -gt 1 ]; then
    user_files=1
    flows=$input_count
    if [ -n "${FLOWS:-}" ] && [ "$FLOWS" -ne "$input_count" ]; then
        echo "error: FLOWS=$FLOWS but $input_count input files given" >&2
        exit 2
    fi
elif [ "$input_count" -eq 1 ] && [ -z "${FLOWS:-}" ]; then
    user_files=1
    flows=1
elif [ "$input_count" -eq 1 ]; then
    user_files=0
    flows=$FLOWS
else
    flows=${FLOWS:-4}
fi

usage() {
    cat >&2 <<'EOF'
Usage:
  ./scripts/run_wire_multiflow_matrix.sh RECEIVER_SSH RECEIVER_DATA_IP FILE0 [FILE1 ...]
  ./scripts/run_wire_multiflow_matrix.sh RECEIVER_SSH RECEIVER_DATA_IP SEED_FILE   # + FLOWS=N

Examples:
  # Your own files = one flow each (flow_id 0..N-1)
  CODECS="copy" RATES="10 20" \
    ./scripts/run_wire_multiflow_matrix.sh fyp1@10.10.10.164 10.10.34.2 \
      flow0.bin flow1.bin flow2.bin flow3.bin

  # Synthesize N payloads from one seed (sized by rate × DURATION_S)
  FLOWS=4 DURATION_S=10 RATES="10 20" \
    ./scripts/run_wire_multiflow_matrix.sh fyp1@10.10.10.164 10.10.34.2 seed.ts

Env:
  CODECS="copy ..."
  RATES="10 20"           per-flow source Mbps (same for all flows in a case)
  FLOWS=N                 seed mode only; with multiple FILEs, count is automatic
  DURATION_S=10           seed mode only (payload ≈ rate × duration)
  IDLE_SEC=10
  PORT_BASE=9100
  KEEP_REMOTE_OUTPUT=1
  FETCH_OUTPUT=1
  USE_NO_PACE=0
  RECEIVER_REPO=$HOME/work/multi-flow-rate-control
  RESULT_DIR=build/wire-multiflow-<timestamp>
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

bytes_for() {
    awk -v r="$1" -v d="$2" 'BEGIN { printf "%.0f", r * d * 1000000 / 8 }'
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

# Per-flow field from "udp-recv: flow N ... key=value"
flow_csv_field() {
    flow_id=$1
    key=$2
    file=$3
    awk -v fid="$flow_id" -v wanted="$key" '
        $1 == "udp-recv:" && $2 == "flow" && $3 == fid {
            for (i = 1; i <= NF; i++) {
                split($i, field, "=")
                if (field[1] == wanted) value = field[2]
            }
        }
        END { print value == "" ? "NA" : value }
    ' "$file"
}

latency_field() {
    metric=$1
    key=$2
    file=$3
    # Prefer the last latency block in the log (multi-flow prints one per flow).
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

# After a "udp-recv: flow FID" line, take the next matching latency metric for that flow.
flow_latency_field() {
    flow_id=$1
    metric=$2
    key=$3
    file=$4
    awk -v fid="$flow_id" -v wanted_metric="$metric:" -v wanted_key="$key" '
        $1 == "udp-recv:" && $2 == "flow" && $3 == fid {
            capture = 1
            next
        }
        capture && $1 == "latency" && $2 == wanted_metric {
            for (i = 3; i <= NF; i++) {
                split($i, field, "=")
                if (field[1] == wanted_key) value = field[2]
            }
            capture = 0
        }
        END { print value == "" ? "NA" : value }
    ' "$file"
}

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
        return int(float(v))
    except ValueError:
        return None

shards = {"copy": 8, "block": 8, "xor-fec": 5, "rs-fec": 6}.get(codec, 8)
datagrams = as_int(sys.argv[3])
seen = as_int(sys.argv[4])
late = as_int(sys.argv[5])
dropped = as_int(sys.argv[6])
recovered = as_int(sys.argv[7])
blocks = math.ceil(payload / 752) if payload > 0 else 0
exp = blocks * shards if blocks > 0 else 0

def pct(num, den):
    if num is None or den is None or den <= 0:
        return "NA"
    return f"{100.0 * num / den:.4f}"

arrived = seen if seen is not None else datagrams
if arrived is None or exp <= 0:
    loss = "NA"
else:
    loss_v = max(0.0, 100.0 * (1.0 - arrived / exp))
    if late is not None and seen is not None:
        # seen already includes late as arrived when present
        pass
    loss = f"{loss_v:.4f}"

print(loss, pct(late, exp), pct(dropped, blocks), pct(recovered, blocks))
PY
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

# Find remote output whose sha256 matches; print path on success.
remote_match_hash() {
    want_hash=$1
    prefix=$2
    ssh $ssh_opts "$receiver_ssh" \
        "for f in ${prefix}*; do
           [ -f \"\$f\" ] || continue
           h=\$(sha256sum \"\$f\" | awk '{print \$1}')
           if [ \"\$h\" = '$want_hash' ]; then echo \"\$f\"; exit 0; fi
         done
         exit 1" 2>/dev/null
}

eta_seconds() {
    # rough paced send time for largest payload at given Mbps
    python3 - "$1" "$2" <<'PY'
import sys
try:
    nbytes = int(sys.argv[1])
    mbps = float(sys.argv[2])
except ValueError:
    print(60)
    raise SystemExit
if mbps <= 0:
    print(60)
else:
    print(max(15, int(nbytes * 8 / (mbps * 1e6) + 20)))
PY
}

if [ -z "$receiver_ssh" ] || [ -z "$receiver_ip" ] || [ "$input_count" -lt 1 ]; then
    usage
    exit 2
fi
if [ ! -x ./build/wg_multi_pipeline ]; then
    die "build/wg_multi_pipeline is missing; run make wg-demo first"
fi
case "$flows" in
    ''|*[!0-9]*) die "FLOWS must be a positive integer" ;;
esac
if [ "$flows" -lt 1 ]; then
    die "FLOWS must be >= 1"
fi

mkdir -p "$result_dir/logs" "$result_dir/payloads" "$result_dir/out" \
    || die "cannot create $result_dir"
input_list="$result_dir/input_files.txt"
: > "$input_list"
for f in "$@"; do
    if [ ! -f "$f" ]; then
        die "input file does not exist: $f"
    fi
    # store absolute-ish path for later cases
    printf '%s\n' "$f" >> "$input_list"
done
seed_path=
if [ "$user_files" -eq 0 ]; then
    seed_path=$(sed -n '1p' "$input_list")
fi

csv="$result_dir/results.csv"
flows_csv="$result_dir/flows.csv"
markdown="$result_dir/results.md"

ssh $ssh_opts "$receiver_ssh" \
    "cd '$remote_repo' && test -x $bin_rel" \
    || die "Node4 is not reachable with key-based SSH, or its binary is missing"

pace_opt=
if [ "$use_no_pace" = "1" ]; then
    pace_opt="--no-pace"
fi

{
    echo "# Wire multi-flow benchmark (Node1 → Node4)"
    echo
    echo "- Sender: $(hostname)"
    echo "- Receiver: $receiver_ssh ($receiver_ip)"
    if [ "$user_files" -eq 1 ]; then
        echo "- Mode: **user files** (one file per flow)"
        echo "- Inputs:"
        fid=0
        while [ "$fid" -lt "$flows" ]; do
            fpath=$(sed -n "$((fid + 1))p" "$input_list")
            fbytes=$(wc -c < "$fpath" | tr -d ' ')
            echo "  - flow $fid: \`$fpath\` ($fbytes bytes)"
            fid=$((fid + 1))
        done
    else
        echo "- Mode: **seed synthesize** (\`FLOWS=$flows\`, \`DURATION_S=${dur_s}s\`)"
        echo "- Seed: \`$seed_path\`"
    fi
    echo "- Flows: **$flows** (flow_id 0..$((flows - 1)), concurrent \`--udp-send-multi\`)"
    echo "- Codecs: $codecs"
    echo "- Per-flow rates: $rates Mbps"
    if [ "$user_files" -eq 0 ]; then
        echo "- Duration: ${dur_s}s (payload ≈ rate × duration)"
    else
        echo "- Payload: full user files (rate only paces send)"
    fi
    echo "- PASS = all flows sha256-match their payloads"
    echo "- Aggregate source ≈ FLOWS × per-flow rate"
    echo
    echo "## Case summary"
    echo
    echo "| Codec | Per-flow Mbps | Flows | Status | Flows PASS | Est. loss % (avg) | E2E p95 µs (avg) | Jitter p95 µs (avg) |"
    echo "| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |"
} > "$markdown"

echo "codec,per_flow_mbps,flows,status,flows_pass,flows_total,agg_target_mbps,payload_bytes_per_flow,est_loss_pct_avg,e2e_p95_us_avg,jitter_p95_us_avg" \
    > "$csv"
echo "codec,per_flow_mbps,flow_id,status,hash_match,payload_bytes,output_bytes,datagrams,seen_datagrams,late,est_dgram_loss_pct,late_pct,group_drop_pct,recovered_pct,recovered_groups,dropped_groups,missing_data_shards,e2e_p95_us,jitter_p95_us,matched_output" \
    > "$flows_csv"

case_number=0
case_pass=0
case_total=0

for codec in $codecs; do
    for rate in $rates; do
        port=$((port_base + case_number))
        label="${codec}-${rate}m-${flows}f"
        case_dir="$result_dir/out/$label"
        payload_dir="$result_dir/payloads/$label"
        remote_base="$remote_repo/build/wire-multiflow-$timestamp-$label"
        remote_prefix="$remote_base/out_"
        receiver_log="$result_dir/logs/$label-receiver.log"
        sender_log="$result_dir/logs/$label-sender.log"
        mkdir -p "$case_dir" "$payload_dir"

        echo "=== $label: UDP $port, $flows flows @ ${rate}Mbps ==="

        flow_args=
        fid=0
        nbytes_ref=NA
        max_payload=0
        while [ "$fid" -lt "$flows" ]; do
            pout="$payload_dir/flow${fid}.bin"
            if [ "$user_files" -eq 1 ]; then
                src=$(sed -n "$((fid + 1))p" "$input_list")
                cp -f -- "$src" "$pout"
            else
                nbytes=$(bytes_for "$rate" "$dur_s")
                nbytes_ref=$nbytes
                gen_payload "$seed_path" "$pout" "$nbytes" "${label}-flow${fid}"
            fi
            sha256sum "$pout" > "$payload_dir/flow${fid}.sha256"
            fbytes=$(wc -c < "$pout" | tr -d ' ')
            if [ "$nbytes_ref" = "NA" ]; then
                nbytes_ref=$fbytes
            fi
            if [ "$fbytes" -gt "$max_payload" ]; then
                max_payload=$fbytes
            fi
            if [ "$user_files" -eq 1 ]; then
                srcname=$(basename -- "$(sed -n "$((fid + 1))p" "$input_list")")
            else
                srcname=synthesized
            fi
            echo "  payload flow $fid: $fbytes bytes ($srcname)"
            flow_args="$flow_args --flow ${fid}:${receiver_ip}:${port}:${pout}:${rate}"
            fid=$((fid + 1))
        done
        nbytes=$nbytes_ref

        # Idle must outlast the paced send of the largest flow, else recv exits early.
        case_idle=$idle_sec
        need_idle=$(eta_seconds "$max_payload" "$rate")
        if [ "$need_idle" -gt "$case_idle" ]; then
            case_idle=$need_idle
        fi
        echo "  idle-sec=$case_idle (auto from largest payload @ ${rate}Mbps; override with IDLE_SEC)"
        echo "  sending (may take ~${need_idle}s for largest flow at ${rate}Mbps)..."

        ssh $ssh_opts "$receiver_ssh" \
            "cd '$remote_repo' && rm -rf '$remote_base' && mkdir -p '$remote_base'" \
            || die "cannot create remote dir"

        # shellcheck disable=SC2086
        ssh $ssh_opts "$receiver_ssh" \
            "cd '$remote_repo' && exec $bin_rel --codec '$codec' --lock-memory \
              --udp-recv '$port' '$remote_prefix' --max-flows '$flows' --idle-sec '$case_idle'" \
            > "$receiver_log" 2>&1 &
        receiver_pid=$!
        sleep 1

        sender_rc=1
        receiver_rc=1
        status=FAIL
        flows_pass=0

        if ! kill -0 "$receiver_pid" 2>/dev/null; then
            wait "$receiver_pid" || true
            echo "  receiver exited early; see $receiver_log" >&2
        else
            # shellcheck disable=SC2086
            if ./build/wg_multi_pipeline $pace_opt --codec "$codec" --udp-send-multi \
                $flow_args > "$sender_log" 2>&1; then
                sender_rc=0
                echo "  sender done (rc=0)"
            else
                sender_rc=$?
                echo "  sender failed (rc=$sender_rc); see $sender_log" >&2
            fi
            echo "  waiting for receiver idle/exit..."
            if wait "$receiver_pid"; then
                receiver_rc=0
            else
                receiver_rc=$?
            fi
            echo "  receiver done (rc=$receiver_rc)"
        fi

        # Validate on Node4 by sha256 first (scp of large files is unreliable for PASS/FAIL).
        loss_sum=0
        loss_n=0
        e2e_sum=0
        e2e_n=0
        jit_sum=0
        jit_n=0

        fid=0
        while [ "$fid" -lt "$flows" ]; do
            want_hash=$(awk '{print $1}' "$payload_dir/flow${fid}.sha256")
            payload_bytes=$(wc -c < "$payload_dir/flow${fid}.bin" | tr -d ' ')
            flow_status=FAIL
            hash_match=no
            matched=NA
            fail_reason=no_match
            log_fid=$fid

            remote_hit=$(remote_match_hash "$want_hash" "$remote_prefix" || true)
            if [ -n "$remote_hit" ]; then
                flow_status=PASS
                hash_match=yes
                matched=$remote_hit
                flows_pass=$((flows_pass + 1))
                fail_reason=
                log_fid=$(printf '%s\n' "$remote_hit" | sed -n 's/.*_flow_\([0-9][0-9]*\)\..*/\1/p')
                [ -n "$log_fid" ] || log_fid=$fid
            else
                # Help diagnose: compare remote output_bytes vs payload.
                out_bytes_guess=$(flow_csv_field "$fid" output_bytes "$receiver_log")
                if [ "$out_bytes_guess" != "NA" ] && [ "$out_bytes_guess" != "$payload_bytes" ]; then
                    fail_reason="size_mismatch_out=${out_bytes_guess}_want=${payload_bytes}"
                else
                    fail_reason="hash_mismatch_or_missing_remote_file"
                fi
            fi

            datagrams=$(flow_csv_field "$log_fid" datagrams "$receiver_log")
            seen=$(flow_csv_field "$log_fid" seen_datagrams "$receiver_log")
            late=$(flow_csv_field "$log_fid" late "$receiver_log")
            recovered=$(flow_csv_field "$log_fid" recovered_groups "$receiver_log")
            dropped=$(flow_csv_field "$log_fid" dropped_groups "$receiver_log")
            missing=$(flow_csv_field "$log_fid" missing_data_shards "$receiver_log")
            out_bytes=$(flow_csv_field "$log_fid" output_bytes "$receiver_log")
            e2e_p95=$(flow_latency_field "$log_fid" end_to_end p95_us "$receiver_log")
            jit_p95=$(flow_latency_field "$log_fid" end_to_end_jitter p95_us "$receiver_log")

            # shellcheck disable=SC2086
            set -- $(loss_stats "$codec" "$payload_bytes" "$datagrams" "$seen" "$late" "$dropped" "$recovered")
            est_loss=${1:-NA}
            late_pct=${2:-NA}
            drop_pct=${3:-NA}
            rec_pct=${4:-NA}

            echo "$codec,$rate,$fid,$flow_status,$hash_match,$payload_bytes,$out_bytes,$datagrams,$seen,$late,$est_loss,$late_pct,$drop_pct,$rec_pct,$recovered,$dropped,$missing,$e2e_p95,$jit_p95,$matched" \
                >> "$flows_csv"

            case "$est_loss" in
                NA|'') ;;
                *)
                    loss_sum=$(awk -v a="$loss_sum" -v b="$est_loss" 'BEGIN{print a+b}')
                    loss_n=$((loss_n + 1))
                    ;;
            esac
            case "$e2e_p95" in
                NA|'') ;;
                *)
                    e2e_sum=$(awk -v a="$e2e_sum" -v b="$e2e_p95" 'BEGIN{print a+b}')
                    e2e_n=$((e2e_n + 1))
                    ;;
            esac
            case "$jit_p95" in
                NA|'') ;;
                *)
                    jit_sum=$(awk -v a="$jit_sum" -v b="$jit_p95" 'BEGIN{print a+b}')
                    jit_n=$((jit_n + 1))
                    ;;
            esac

            if [ "$flow_status" = "PASS" ]; then
                echo "  flow $fid -> PASS  loss%=$est_loss  e2e_p95=$e2e_p95  out_bytes=$out_bytes"
            else
                echo "  flow $fid -> FAIL  loss%=$est_loss  e2e_p95=$e2e_p95  out_bytes=$out_bytes  reason=$fail_reason"
            fi
            fid=$((fid + 1))
        done

        if [ "$fetch_out" = "1" ]; then
            echo "  fetching remote outputs (optional; not used for PASS/FAIL)..."
            # shellcheck disable=SC2086
            scp $ssh_opts "$receiver_ssh:$remote_prefix*" "$case_dir/" >/dev/null 2>&1 || \
                echo "  warning: scp fetch failed or partial (large files); remote hash already checked" >&2
        fi

        if [ "$sender_rc" -eq 0 ] && [ "$receiver_rc" -eq 0 ] &&
            [ "$flows_pass" -eq "$flows" ]; then
            status=PASS
        fi

        loss_avg=NA
        e2e_avg=NA
        jit_avg=NA
        if [ "$loss_n" -gt 0 ]; then
            loss_avg=$(awk -v s="$loss_sum" -v n="$loss_n" 'BEGIN{printf "%.4f", s/n}')
        fi
        if [ "$e2e_n" -gt 0 ]; then
            e2e_avg=$(awk -v s="$e2e_sum" -v n="$e2e_n" 'BEGIN{printf "%.3f", s/n}')
        fi
        if [ "$jit_n" -gt 0 ]; then
            jit_avg=$(awk -v s="$jit_sum" -v n="$jit_n" 'BEGIN{printf "%.3f", s/n}')
        fi

        agg=$(awk -v f="$flows" -v r="$rate" 'BEGIN{print f*r}')
        echo "$codec,$rate,$flows,$status,$flows_pass,$flows,$agg,$nbytes,$loss_avg,$e2e_avg,$jit_avg" \
            >> "$csv"
        echo "| $codec | $rate | $flows | $status | $flows_pass / $flows | $loss_avg | $e2e_avg | $jit_avg |" \
            >> "$markdown"

        case_total=$((case_total + 1))
        if [ "$status" = "PASS" ]; then
            case_pass=$((case_pass + 1))
        fi
        echo "  -> case $status ($flows_pass/$flows flows)"

        if [ "$keep_remote" != "1" ]; then
            ssh $ssh_opts "$receiver_ssh" "rm -rf '$remote_base'" >/dev/null 2>&1 || true
        fi

        case_number=$((case_number + 1))
    done
done

{
    echo
    echo "## Per-flow details"
    echo
    echo "See \`flows.csv\` for every flow (status, loss %, latency)."
    echo
    echo "## Summary"
    echo
    echo "- Cases PASS: **$case_pass / $case_total**"
    echo "- \`results.csv\` — per codec×rate aggregate"
    echo "- \`flows.csv\` — per-flow rows"
    echo "- Logs: \`logs/*-sender.log\`, \`logs/*-receiver.log\`"
    if [ "$keep_remote" = "1" ]; then
        echo "- Remote outputs: \`$remote_repo/build/wire-multiflow-${timestamp}-*/\` on $receiver_ssh"
    fi
} >> "$markdown"

echo
echo "Done. Cases PASS $case_pass / $case_total"
echo "  Report: $markdown"
echo "  CSV:    $csv"
echo "  Flows:  $flows_csv"
echo "  Logs:   $result_dir/logs/"
echo
cat "$markdown"
