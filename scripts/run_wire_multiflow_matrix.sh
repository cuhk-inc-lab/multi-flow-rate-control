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
#   results.md   — run config, case summary, per-case/per-flow tables, metric notes
#   results.csv  — one row per codec×rate case (bitrates, timing, RCs)
#   flows.csv    — one row per flow (loss, latency, sender order check)
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

# Receiver demux assigns peer_map ids by first-seen 5-tuple order, which may
# NOT equal the sender --flow id. Open lines look like:
#   udp-recv: opened flow <mapped> (wire flow <sender_id>) -> <path>
# Resolve sender wire id → mapped log id used in later "udp-recv: flow N ..." lines.
wire_to_log_fid() {
    wire_fid=$1
    file=$2
    mapped=$(awk -v wid="$wire_fid" '
        $1 == "udp-recv:" && $2 == "opened" && $3 == "flow" &&
        $5 == "(wire" && $6 == "flow" {
            w = $7
            sub(/\)/, "", w)
            if (w == wid) {
                print $4
                exit
            }
        }
    ' "$file")
    # Fallback: old logs / missing open line — assume ids match.
    if [ -n "$mapped" ]; then
        printf '%s\n' "$mapped"
    else
        printf '%s\n' "$wire_fid"
    fi
}

# Per-flow field from "udp-recv: flow N ... key=value" (N = mapped log id)
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

# output= path may contain no spaces; take that field. flow_id = mapped log id.
flow_output_path() {
    flow_id=$1
    file=$2
    awk -v fid="$flow_id" '
        $1 == "udp-recv:" && $2 == "flow" && $3 == fid {
            for (i = 1; i <= NF; i++) {
                if ($i ~ /^output=/) {
                    sub(/^output=/, "", $i)
                    print $i
                    exit
                }
            }
        }
    ' "$file"
}

# Hash a specific remote file (cd into repo first — logs often use relative paths).
remote_sha256() {
    path=$1
    case "$path" in
        /*) ssh $ssh_opts "$receiver_ssh" "sha256sum -- '$path' 2>/dev/null | awk '{print \$1}'" ;;
        *)  ssh $ssh_opts "$receiver_ssh" "cd '$remote_repo' && sha256sum -- '$path' 2>/dev/null | awk '{print \$1}'" ;;
    esac
}

# Find remote output whose sha256 matches; print path on success.
remote_match_hash() {
    want_hash=$1
    prefix=$2
    ssh $ssh_opts "$receiver_ssh" \
        "for f in ${prefix}*; do
           [ -f \"\$f\" ] || continue
           h=\$(sha256sum -- \"\$f\" 2>/dev/null | awk '{print \$1}')
           [ -n \"\$h\" ] || continue
           if [ \"\$h\" = '$want_hash' ]; then echo \"\$f\"; exit 0; fi
         done
         exit 1"
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

human_bytes() {
    python3 - "$1" <<'PY'
import sys
n = int(sys.argv[1])
if n < 1024:
    print(f"{n} B")
elif n < 1024 * 1024:
    print(f"{n / 1024:.1f} KB")
elif n < 1024 * 1024 * 1024:
    print(f"{n / (1024 * 1024):.2f} MB")
else:
    print(f"{n / (1024 * 1024 * 1024):.2f} GB")
PY
}

throughput_mbps() {
    python3 - "$1" "$2" <<'PY'
import sys
try:
    nbytes = int(sys.argv[1])
    secs = float(sys.argv[2])
except ValueError:
    print("NA")
    raise SystemExit
if secs <= 0:
    print("NA")
else:
    print(f"{nbytes * 8 / secs / 1e6:.2f}")
PY
}

# blocks=, source_bytes=, order (ok|corrupt|NA) from sender log.
sender_flow_field() {
    wire_fid=$1
    field=$2
    file=$3
    awk -v wid="$wire_fid" -v want="$field" '
        /wire-multi-send:/ {
            fid = ""
            blocks = ""
            source_bytes = ""
            order = ""
            for (i = 1; i <= NF; i++) {
                split($i, f, "=")
                if (f[1] == "flow_id") fid = f[2]
                if (f[1] == "blocks") blocks = f[2]
                if (f[1] == "source_bytes") source_bytes = f[2]
            }
            if (fid != wid) next
            if (/ORDER_CORRUPT/) order = "corrupt"
            else if (/order_ok/) order = "ok"
            if (want == "blocks") { print blocks; exit }
            if (want == "source_bytes") { print source_bytes; exit }
            if (want == "order") { print order; exit }
        }
        END {
            if (want == "order") print "NA"
        }
    ' "$file"
}

# p50_us, p95_us, p99_us, avg_us, … from latency metric after flow line.
flow_latency_stat() {
    flow_id=$1
    metric=$2
    stat=$3
    file=$4
    awk -v fid="$flow_id" -v wanted_metric="$metric:" -v wanted_stat="$stat" '
        $1 == "udp-recv:" && $2 == "flow" && $3 == fid {
            capture = 1
            next
        }
        capture && $1 == "udp-recv:" {
            capture = 0
        }
        capture && $1 == "latency" && $2 == wanted_metric {
            for (i = 3; i <= NF; i++) {
                split($i, field, "=")
                if (field[1] == wanted_stat) value = field[2]
            }
            capture = 0
        }
        END { print value == "" ? "NA" : value }
    ' "$file"
}

if [ -z "$receiver_ssh" ] || [ -z "$receiver_ip" ] || [ "$input_count" -lt 1 ]; then
    usage
    exit 2
fi
if [ ! -x ./build/wg_multi_pipeline ]; then
    die "build/wg_multi_pipeline is missing; run make wg-demo first"
fi

local_rev=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
run_started_at=$(date -Iseconds 2>/dev/null || date)
sender_host=$(hostname 2>/dev/null || echo unknown)
# Deferred-reorder fix required on the SENDER binary (Node1).
if ! git merge-base --is-ancestor 5ee8fc3 HEAD 2>/dev/null; then
    die "git HEAD ($local_rev) lacks fix 5ee8fc3; git pull && make -j"
fi
if ! strings ./build/wg_multi_pipeline 2>/dev/null | grep -q 'ORDER_CORRUPT'; then
    die "build/wg_multi_pipeline looks stale (no ORDER_CORRUPT check); run: make -j"
fi

remote_rev=$(ssh $ssh_opts "$receiver_ssh" \
    "cd '$remote_repo' && git rev-parse --short HEAD 2>/dev/null" || echo unknown)
echo "Sender git=$local_rev  Receiver git=$remote_rev"
echo "Note: deferred-reorder fix is sender-side; Node1 must be rebuilt after pull."

case "$flows" in
    ''|*[!0-9]*) die "FLOWS must be a positive integer" ;;
esac
if [ "$flows" -lt 1 ]; then
    die "FLOWS must be >= 1"
fi
# Must match MF_MAX_FLOWS / WIRE_MAX_FLOWS in the binary (currently 8).
if [ "$flows" -gt 8 ]; then
    die "FLOWS=$flows exceeds binary max_flows limit 8"
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
case_details="$result_dir/case_details.partial"
: > "$case_details"

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
    echo "## Run configuration"
    echo
    echo "- **Started:** $run_started_at"
    echo "- **Result dir:** \`$result_dir\`"
    echo "- **Sender:** $sender_host (git \`$local_rev\`)"
    echo "- **Receiver:** $receiver_ssh → $receiver_ip (git \`$remote_rev\`)"
    echo "- **Binary:** \`$bin_rel\`"
    echo "- **Flows:** $flows concurrent (\`--udp-send-multi\`, flow_id 0..$((flows - 1)))"
    echo "- **Codecs:** $codecs"
    echo "- **Per-flow target rate:** $rates Mbps"
    echo "- **Aggregate target (all flows):** flows × per-flow rate"
    if [ "$use_no_pace" = "1" ]; then
        echo "- **Pacing:** \`--no-pace\` on CLI (wire multi-send still uses FlowManager pacing off + wire TX rate)"
    else
        echo "- **Pacing:** default (wire multi-send: FlowManager off, wire TX \`source_rate_mbps\` only)"
    fi
    echo "- **UDP port base:** $port_base (one port per case)"
    echo "- **Receiver idle:** ${idle_sec}s default (auto-extended per case from largest payload)"
    echo "- **PASS criteria:** every flow sha256-matches its payload on Node4"
    echo
    if [ "$user_files" -eq 1 ]; then
        echo "### Input files (one flow each)"
        echo
        echo "| Flow | File | Size | sha256 |"
        echo "| ---: | --- | ---: | --- |"
        fid=0
        while [ "$fid" -lt "$flows" ]; do
            fpath=$(sed -n "$((fid + 1))p" "$input_list")
            fbytes=$(wc -c < "$fpath" | tr -d ' ')
            fhash=$(sha256sum "$fpath" | awk '{print $1}')
            fhash_short=$(echo "$fhash" | cut -c1-16)
            echo "| $fid | \`$fpath\` | $(human_bytes "$fbytes") | \`${fhash_short}…\` |"
            fid=$((fid + 1))
        done
        echo
    else
        echo "- **Mode:** seed synthesize (\`FLOWS=$flows\`, \`DURATION_S=${dur_s}s\`, payload ≈ rate × duration)"
        echo "- **Seed:** \`$seed_path\`"
        echo
    fi
    echo "## Case summary"
    echo
    echo "| Case | Codec | Mbps/flow | Agg Mbps | Max payload | Est. send s | Elapsed s | Snd RC | Rcv RC | Status | PASS | Loss avg % | E2E p95 µs | Jit p95 µs |"
    echo "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |"
} > "$markdown"

echo "codec,per_flow_mbps,flows,case_label,status,flows_pass,flows_total,agg_target_mbps,payload_bytes_per_flow,max_payload_bytes,est_send_s,elapsed_s,sender_rc,receiver_rc,est_loss_pct_avg,e2e_p95_us_avg,jitter_p95_us_avg" \
    > "$csv"
echo "codec,per_flow_mbps,case_label,flow_id,log_fid,status,hash_match,payload_bytes,output_bytes,target_mbps,eff_mbps,datagrams,seen_datagrams,late,duplicates,malformed,decoded_blocks,est_dgram_loss_pct,late_pct,group_drop_pct,recovered_pct,recovered_groups,dropped_groups,missing_data_shards,e2e_p50_us,e2e_p95_us,e2e_p99_us,jitter_p95_us,encode_p95_us,decode_p95_us,sender_blocks,sender_source_bytes,sender_order_ok,fail_reason,matched_output" \
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
        out_suffix_args=
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
                case "$srcname" in
                    *.*) flow_ext=".${srcname##*.}" ;;
                    *)   flow_ext= ;;
                esac
            else
                srcname=synthesized
                flow_ext=.bin
            fi
            out_suffix_args="$out_suffix_args --out-suffix ${fid}:${flow_ext}"
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
        echo "  tip: mid-path speedometer should stay near ${rate}Mbps until send finishes; quiet path = stall"

        case_start=$(date +%s)

        ssh $ssh_opts "$receiver_ssh" \
            "cd '$remote_repo' && rm -rf '$remote_base' && mkdir -p '$remote_base'" \
            || die "cannot create remote dir"

        # shellcheck disable=SC2086
        ssh $ssh_opts "$receiver_ssh" \
            "cd '$remote_repo' && exec $bin_rel --codec '$codec' --lock-memory \
              --udp-recv '$port' '$remote_prefix' --max-flows '$flows' --idle-sec '$case_idle' \
              $out_suffix_args" \
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

        case_end=$(date +%s)
        case_elapsed=$((case_end - case_start))

        # Validate on Node4 by sha256 first (scp of large files is unreliable for PASS/FAIL).
        loss_sum=0
        loss_n=0
        e2e_sum=0
        e2e_n=0
        jit_sum=0
        jit_n=0

        {
            echo
            echo "### $label"
            echo
            echo "- UDP port **$port** | idle **${case_idle}s** | est. paced send **~${need_idle}s** | wall **${case_elapsed}s**"
            echo "- Target: **${rate} Mbps/flow** × **$flows flows** → **$(awk -v f="$flows" -v r="$rate" 'BEGIN{print f*r}') Mbps** aggregate"
            echo "- Sender rc=$sender_rc | Receiver rc=$receiver_rc"
            echo
            echo "| Wire | Log | Status | Payload | Out | Target Mbps | Eff Mbps | Loss % | Late % | Recov % | E2E p50 | E2E p95 | E2E p99 | Jit p95 | Enc p95 | Dec p95 | Order |"
            echo "| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
        } >> "$case_details"

        fid=0
        while [ "$fid" -lt "$flows" ]; do
            want_hash=$(awk '{print $1}' "$payload_dir/flow${fid}.sha256")
            payload_bytes=$(wc -c < "$payload_dir/flow${fid}.bin" | tr -d ' ')
            flow_status=FAIL
            hash_match=no
            matched=NA
            fail_reason=no_match
            # Sender --flow id → receiver peer_map / log id (may differ).
            log_fid=$(wire_to_log_fid "$fid" "$receiver_log")

            remote_hit=
            remote_hash=NA
            # Prefer the exact path from the receiver log (relative to remote_repo).
            log_out=$(flow_output_path "$log_fid" "$receiver_log")
            if [ -n "$log_out" ] && [ "$log_out" != "NA" ]; then
                remote_hash=$(remote_sha256 "$log_out" || true)
                if [ -n "$remote_hash" ] && [ "$remote_hash" = "$want_hash" ]; then
                    remote_hit=$log_out
                fi
            fi
            # Fallback: scan prefix glob (absolute paths under remote_base).
            if [ -z "$remote_hit" ]; then
                remote_hit=$(remote_match_hash "$want_hash" "$remote_prefix" || true)
                if [ -n "$remote_hit" ]; then
                    remote_hash=$want_hash
                elif [ -n "$log_out" ] && [ "$log_out" != "NA" ] &&
                    { [ -z "$remote_hash" ] || [ "$remote_hash" = "NA" ]; }; then
                    remote_hash=$(remote_sha256 "$log_out" || true)
                    [ -n "$remote_hash" ] || remote_hash=NA
                fi
            fi

            if [ -n "$remote_hit" ]; then
                flow_status=PASS
                hash_match=yes
                matched=$remote_hit
                flows_pass=$((flows_pass + 1))
                fail_reason=
            else
                out_bytes_guess=$(flow_csv_field "$log_fid" output_bytes "$receiver_log")
                if [ "$out_bytes_guess" != "NA" ] && [ "$out_bytes_guess" != "$payload_bytes" ]; then
                    fail_reason="size_mismatch_out=${out_bytes_guess}_want=${payload_bytes}"
                elif [ "$remote_hash" != "NA" ] && [ -n "$remote_hash" ] &&
                    [ "$remote_hash" != "$want_hash" ]; then
                    fail_reason="content_hash_mismatch"
                else
                    fail_reason="hash_mismatch_or_missing_remote_file"
                fi
            fi

            datagrams=$(flow_csv_field "$log_fid" datagrams "$receiver_log")
            seen=$(flow_csv_field "$log_fid" seen_datagrams "$receiver_log")
            late=$(flow_csv_field "$log_fid" late "$receiver_log")
            duplicates=$(flow_csv_field "$log_fid" duplicates "$receiver_log")
            malformed=$(flow_csv_field "$log_fid" malformed "$receiver_log")
            decoded_blocks=$(flow_csv_field "$log_fid" decoded_blocks "$receiver_log")
            recovered=$(flow_csv_field "$log_fid" recovered_groups "$receiver_log")
            dropped=$(flow_csv_field "$log_fid" dropped_groups "$receiver_log")
            missing=$(flow_csv_field "$log_fid" missing_data_shards "$receiver_log")
            out_bytes=$(flow_csv_field "$log_fid" output_bytes "$receiver_log")
            e2e_p50=$(flow_latency_stat "$log_fid" end_to_end p50_us "$receiver_log")
            e2e_p95=$(flow_latency_stat "$log_fid" end_to_end p95_us "$receiver_log")
            e2e_p99=$(flow_latency_stat "$log_fid" end_to_end p99_us "$receiver_log")
            jit_p95=$(flow_latency_stat "$log_fid" end_to_end_jitter p95_us "$receiver_log")
            enc_p95=$(flow_latency_stat "$log_fid" encode p95_us "$receiver_log")
            dec_p95=$(flow_latency_stat "$log_fid" decode p95_us "$receiver_log")
            sender_blocks=$(sender_flow_field "$fid" blocks "$sender_log")
            sender_source_bytes=$(sender_flow_field "$fid" source_bytes "$sender_log")
            sender_order=$(sender_flow_field "$fid" order "$sender_log")
            eff_mbps=$(throughput_mbps "$payload_bytes" "$case_elapsed")

            # shellcheck disable=SC2086
            set -- $(loss_stats "$codec" "$payload_bytes" "$datagrams" "$seen" "$late" "$dropped" "$recovered")
            est_loss=${1:-NA}
            late_pct=${2:-NA}
            drop_pct=${3:-NA}
            rec_pct=${4:-NA}

            echo "$codec,$rate,$label,$fid,$log_fid,$flow_status,$hash_match,$payload_bytes,$out_bytes,$rate,$eff_mbps,$datagrams,$seen,$late,$duplicates,$malformed,$decoded_blocks,$est_loss,$late_pct,$drop_pct,$rec_pct,$recovered,$dropped,$missing,$e2e_p50,$e2e_p95,$e2e_p99,$jit_p95,$enc_p95,$dec_p95,$sender_blocks,$sender_source_bytes,$sender_order,$fail_reason,$matched" \
                >> "$flows_csv"

            echo "| $fid | $log_fid | $flow_status | $(human_bytes "$payload_bytes") | ${out_bytes:-NA} | $rate | $eff_mbps | $est_loss | $late_pct | $rec_pct | $e2e_p50 | $e2e_p95 | $e2e_p99 | $jit_p95 | $enc_p95 | $dec_p95 | $sender_order |" \
                >> "$case_details"

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
                echo "  flow $fid -> PASS  loss%=$est_loss  e2e_p95=$e2e_p95  eff=${eff_mbps}Mbps  order=$sender_order"
            else
                echo "  flow $fid -> FAIL  loss%=$est_loss  e2e_p95=$e2e_p95  out_bytes=$out_bytes  reason=$fail_reason"
                echo "           wire_id=$fid log_id=$log_fid local_sha=$want_hash"
                echo "           remote_sha=$remote_hash  remote_path=${log_out:-NA}"
                echo "           recv: recovered=$recovered dropped=$dropped late=$late missing=$missing order=$sender_order"
            fi
            fid=$((fid + 1))
        done

        if [ "$flows_pass" -lt "$flows" ]; then
            {
                echo
                echo "**Failures:** see console / \`logs/$label-receiver.log\` / \`logs/$label-sender.log\`"
            } >> "$case_details"
        fi

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
        echo "$codec,$rate,$flows,$label,$status,$flows_pass,$flows,$agg,$nbytes,$max_payload,$need_idle,$case_elapsed,$sender_rc,$receiver_rc,$loss_avg,$e2e_avg,$jit_avg" \
            >> "$csv"
        echo "| $label | $codec | $rate | $agg | $(human_bytes "$max_payload") | $need_idle | $case_elapsed | $sender_rc | $receiver_rc | $status | $flows_pass/$flows | $loss_avg | $e2e_avg | $jit_avg |" \
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
    echo "## Case details"
    cat "$case_details"
    echo
    echo "## Artifacts"
    echo
    echo "- Cases PASS: **$case_pass / $case_total**"
    echo "- \`results.csv\` — per codec×rate aggregate (bitrates, timing, RCs)"
    echo "- \`flows.csv\` — per-flow rows (loss, latency, sender order check)"
    echo "- Logs: \`logs/<case>-sender.log\`, \`logs/<case>-receiver.log\`"
    echo "- Payloads: \`payloads/<case>/flowN.bin\` + sha256"
    if [ "$fetch_out" = "1" ]; then
        echo "- Local outputs (optional fetch): \`out/<case>/\`"
    fi
    if [ "$keep_remote" = "1" ]; then
        echo "- Remote outputs: \`$remote_repo/build/wire-multiflow-${timestamp}-*/\` on $receiver_ssh"
    fi
    echo
    echo "### Metric notes"
    echo
    echo "- **Target Mbps** — per-flow \`source_rate_mbps\` from sender (\`--flow id:host:port:file:rate\`)"
    echo "- **Agg Mbps** — flows × per-flow rate (concurrent multi-send)"
    echo "- **Eff Mbps** — payload bytes ÷ wall-clock case time (includes idle wait; not wire-only)"
    echo "- **Loss %** — estimated from expected FEC shards vs \`seen_datagrams\`"
    echo "- **Order** — sender \`order_ok\` / \`ORDER_CORRUPT\` (FlowManager ingress vs drain fingerprint)"
    echo "- Mid-path speedometer ≈ per-flow target until send completes; quiet path mid-transfer = stall"
} >> "$markdown"

echo
echo "Done. Cases PASS $case_pass / $case_total"
echo "  Report: $markdown"
echo "  CSV:    $csv"
echo "  Flows:  $flows_csv"
echo "  Logs:   $result_dir/logs/"
echo
cat "$markdown"
