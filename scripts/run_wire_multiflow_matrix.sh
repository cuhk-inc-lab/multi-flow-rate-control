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
# Artifacts under build/wire-multiflow-<ts>/ (kept lean):
#   results.md   — compact summary (est. + measured link Mbps, loss, latency)
#   results.csv  — one row per case
#   flows.csv    — one row per flow
#   logs/        — sender/receiver logs
#   monitor/     — Node2/Node3 NIC timeseries (real relay bitrate)
#   payloads/    — sha256 + tiny seed files only (no copies of user inputs)
#   out/         — only if FETCH_OUTPUT=1

set -u

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
monitor_py="$script_dir/iperf_like_monitor.py"

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
keep_remote=${KEEP_REMOTE_OUTPUT:-0}
fetch_out=${FETCH_OUTPUT:-0}
use_no_pace=${USE_NO_PACE:-0}
monitor_relays=${MONITOR_RELAYS:-1}
monitor_hz=${MONITOR_HZ:-1}
node2_ssh=${NODE2_SSH:-"fyp1@10.10.10.162"}
node3_ssh=${NODE3_SSH:-"fyp1@10.10.10.163"}
node2_ifaces=${NODE2_IFACES:-"ap0 station1"}
node3_ifaces=${NODE3_IFACES:-"ap1 station2"}
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
  KEEP_REMOTE_OUTPUT=0   keep remote out_* (default: delete after hash check)
  FETCH_OUTPUT=0         scp outputs into result dir (default: off; large)
  USE_NO_PACE=0
  MONITOR_RELAYS=1       sample Node2/Node3 NIC bitrate during each case
  MONITOR_HZ=1
  NODE2_SSH=fyp1@10.10.10.162   NODE2_IFACES="ap0 station1"
  NODE3_SSH=fyp1@10.10.10.163   NODE3_IFACES="ap1 station2"
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
    recv_blocks=$8
    expect_blocks=$9

    python3 - "$codec" "$payload_bytes" "$datagrams" "$seen" "$late" "$dropped" "$recovered" \
        "$recv_blocks" "$expect_blocks" <<'PY'
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
recv_blocks = as_int(sys.argv[8])
expect_blocks = as_int(sys.argv[9])
blocks = math.ceil(payload / 752) if payload > 0 else 0
if blocks <= 0 and expect_blocks is not None and expect_blocks > 0:
    blocks = expect_blocks
exp = blocks * shards if blocks > 0 else 0

def pct(num, den):
    if num is None or den is None or den <= 0:
        return "NA"
    return f"{100.0 * num / den:.4f}"

arrived = seen if seen is not None else datagrams
if arrived is None or exp <= 0:
    # Incomplete flow: estimate from received_blocks / expected_blocks.
    if recv_blocks is not None and expect_blocks is not None and expect_blocks > 0:
        loss = f"{max(0.0, 100.0 * (1.0 - recv_blocks / expect_blocks)):.4f}"
    elif recv_blocks is not None and blocks > 0:
        loss = f"{max(0.0, 100.0 * (1.0 - recv_blocks / blocks)):.4f}"
    else:
        loss = "NA"
else:
    loss = f"{max(0.0, 100.0 * (1.0 - arrived / exp)):.4f}"

print(loss, pct(late, exp), pct(dropped, blocks), pct(recovered, blocks))
PY
}

# Link (wire) Mbps ≈ source_mbps × (shards × UDP datagram) / source_block
# UDP datagram = WIRE_HEADER(44) + PKG(188) = 232; source_block = 752.
link_mbps() {
    python3 - "$1" "$2" "$3" <<'PY'
import sys
codec = sys.argv[1].strip().lower()
src = float(sys.argv[2])
flows = int(sys.argv[3])
shards = {"copy": 8, "block": 8, "xor-fec": 5, "rs-fec": 6}.get(codec, 8)
# wire expansion over paced source bytes
ratio = (shards * 232.0) / 752.0
print(f"{src * flows * ratio:.1f}")
PY
}

# From iface CSV: print "peak_mbps avg_mbps" (max(rx,tx) per sample; avg only when >=1Mbps).
monitor_mbps_summary() {
    csv=$1
    if [ ! -f "$csv" ]; then
        echo "NA NA"
        return
    fi
    python3 - "$csv" <<'PY'
import csv, sys
path = sys.argv[1]
peaks = []
active = []
try:
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("iface") == "__cpu__":
                continue
            rx = float(row.get("rx_bps") or 0)
            tx = float(row.get("tx_bps") or 0)
            m = max(rx, tx) / 1e6
            peaks.append(m)
            if m >= 1.0:
                active.append(m)
except OSError:
    print("NA NA")
    raise SystemExit
if not peaks:
    print("NA NA")
else:
    peak = max(peaks)
    avg = (sum(active) / len(active)) if active else 0.0
    print(f"{peak:.1f} {avg:.1f}")
PY
}

start_relay_monitor() {
    host=$1
    ifaces=$2
    remote_csv=$3
    local_csv=$4

    if [ ! -f "$monitor_py" ]; then
        echo ""
        return
    fi
    # shellcheck disable=SC2086
    if ! scp $ssh_opts "$monitor_py" "$host:/tmp/iperf_like_monitor.py" >/dev/null 2>&1; then
        echo ""
        return
    fi
    pid=$(ssh $ssh_opts "$host" \
        "rm -f '$remote_csv'; nohup python3 /tmp/iperf_like_monitor.py '$ifaces' '$monitor_hz' '$remote_csv' >/dev/null 2>&1 & echo \$!" \
        2>/dev/null || true)
    printf '%s\n' "$pid"
}

stop_relay_monitor() {
    host=$1
    pid=$2
    remote_csv=$3
    local_csv=$4

    if [ -n "${pid:-}" ]; then
        ssh $ssh_opts "$host" "kill $pid 2>/dev/null || true" >/dev/null 2>&1 || true
        sleep 0.3
        ssh $ssh_opts "$host" "kill -9 $pid 2>/dev/null || true" >/dev/null 2>&1 || true
    fi
    # shellcheck disable=SC2086
    scp $ssh_opts "$host:$remote_csv" "$local_csv" >/dev/null 2>&1 || true
    ssh $ssh_opts "$host" "rm -f '$remote_csv'" >/dev/null 2>&1 || true
}

source_agg_mbps() {
    awk -v f="$1" -v r="$2" 'BEGIN{printf "%.0f", f*r}'
}

# udp-recv: flow N incomplete: received_blocks=… expected_blocks=… missing_groups=…
flow_incomplete_field() {
    flow_id=$1
    key=$2
    file=$3
    awk -v fid="$flow_id" -v wanted="$key" '
        $1 == "udp-recv:" && $2 == "flow" && $3 == fid && $4 == "incomplete:" {
            for (i = 5; i <= NF; i++) {
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

mkdir -p "$result_dir/logs" "$result_dir/payloads" "$result_dir/monitor" \
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
fail_notes="$result_dir/fail_notes.partial"
: > "$fail_notes"

ssh $ssh_opts "$receiver_ssh" \
    "cd '$remote_repo' && test -x $bin_rel" \
    || die "Node4 is not reachable with key-based SSH, or its binary is missing"

if [ "$monitor_relays" = "1" ]; then
    if [ ! -f "$monitor_py" ]; then
        echo "warning: missing $monitor_py; relay measure disabled" >&2
        monitor_relays=0
    else
        echo "Relay monitors: Node2=$node2_ssh ($node2_ifaces)  Node3=$node3_ssh ($node3_ifaces)"
        # Soft check — do not abort matrix if a relay is briefly unreachable.
        if ! ssh $ssh_opts "$node2_ssh" "true" >/dev/null 2>&1; then
            echo "warning: cannot SSH Node2 ($node2_ssh); N2 measure will be NA" >&2
        fi
        if ! ssh $ssh_opts "$node3_ssh" "true" >/dev/null 2>&1; then
            echo "warning: cannot SSH Node3 ($node3_ssh); N3 measure will be NA" >&2
        fi
    fi
fi

pace_opt=
if [ "$use_no_pace" = "1" ]; then
    pace_opt="--no-pace"
fi

{
    echo "# Wire multi-flow (Node1 → Node4)"
    echo
    echo "- **When:** $run_started_at | sender \`$local_rev\` / recv \`$remote_rev\`"
    echo "- **Path:** $sender_host → $receiver_ssh ($receiver_ip)"
    echo "- **Flows:** $flows concurrent | codecs: $codecs | source rates: $rates Mbps/flow"
    if [ "$user_files" -eq 1 ]; then
        echo "- **Inputs:**"
        fid=0
        while [ "$fid" -lt "$flows" ]; do
            fpath=$(sed -n "$((fid + 1))p" "$input_list")
            fbytes=$(wc -c < "$fpath" | tr -d ' ')
            echo "  - flow $fid: \`$(basename -- "$fpath")\` ($(human_bytes "$fbytes"))"
            fid=$((fid + 1))
        done
    else
        echo "- **Mode:** seed synthesize FLOWS=$flows DURATION_S=${dur_s}s seed=\`$seed_path\`"
    fi
    echo "- **PASS:** sha256 match on receiver for every flow"
    echo "- **Link Mbps (est.):** flows × source × (shards×232)/752"
    if [ "$monitor_relays" = "1" ]; then
        echo "- **Measured:** Node2 \`$node2_ssh\` ($node2_ifaces) / Node3 \`$node3_ssh\` ($node3_ifaces) NIC peak/avg Mbps"
    fi
    echo
    echo "## Results"
    echo
    echo "| Codec | Src | Link est. | N2 peak | N2 avg | N3 peak | N3 avg | Status | Loss % | E2E p95 | Notes |"
    echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |"
} > "$markdown"

echo "codec,src_mbps_per_flow,link_mbps_est,n2_peak_mbps,n2_avg_mbps,n3_peak_mbps,n3_avg_mbps,flows,status,flows_pass,loss_pct,e2e_p95_us,elapsed_s,sender_rc,receiver_rc,notes" \
    > "$csv"
echo "codec,src_mbps,link_mbps_per_flow,flow_id,status,loss_pct,payload_bytes,output_bytes,e2e_p95_us,fail_reason" \
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
        mkdir -p "$payload_dir"
        if [ "$fetch_out" = "1" ]; then
            mkdir -p "$case_dir"
        fi

        case_link=$(link_mbps "$codec" "$rate" "$flows")
        case_link_per_flow=$(link_mbps "$codec" "$rate" 1)
        echo "=== $label: UDP $port, $flows flows @ ${rate}Mbps src / ${case_link}Mbps link ==="

        flow_args=
        out_suffix_args=
        fid=0
        nbytes_ref=NA
        max_payload=0
        while [ "$fid" -lt "$flows" ]; do
            if [ "$user_files" -eq 1 ]; then
                src=$(sed -n "$((fid + 1))p" "$input_list")
                # Hash in place — do not copy large user files into build/.
                send_path=$src
                sha256sum "$src" | awk '{print $1}' > "$payload_dir/flow${fid}.sha256"
                fbytes=$(wc -c < "$src" | tr -d ' ')
                printf '%s\n' "$fbytes" > "$payload_dir/flow${fid}.bytes"
                printf '%s\n' "$src" > "$payload_dir/flow${fid}.path"
                srcname=$(basename -- "$src")
                case "$srcname" in
                    *.*) flow_ext=".${srcname##*.}" ;;
                    *)   flow_ext= ;;
                esac
            else
                send_path="$payload_dir/flow${fid}.bin"
                nbytes=$(bytes_for "$rate" "$dur_s")
                nbytes_ref=$nbytes
                gen_payload "$seed_path" "$send_path" "$nbytes" "${label}-flow${fid}"
                sha256sum "$send_path" | awk '{print $1}' > "$payload_dir/flow${fid}.sha256"
                fbytes=$nbytes
                printf '%s\n' "$fbytes" > "$payload_dir/flow${fid}.bytes"
                srcname=synthesized
                flow_ext=.bin
            fi
            if [ "$nbytes_ref" = "NA" ]; then
                nbytes_ref=$fbytes
            fi
            if [ "$fbytes" -gt "$max_payload" ]; then
                max_payload=$fbytes
            fi
            out_suffix_args="$out_suffix_args --out-suffix ${fid}:${flow_ext}"
            echo "  payload flow $fid: $fbytes bytes ($srcname)"
            flow_args="$flow_args --flow ${fid}:${receiver_ip}:${port}:${send_path}:${rate}"
            fid=$((fid + 1))
        done
        nbytes=$nbytes_ref

        # Idle must outlast the paced send of the largest flow, else recv exits early.
        case_idle=$idle_sec
        need_idle=$(eta_seconds "$max_payload" "$rate")
        if [ "$need_idle" -gt "$case_idle" ]; then
            case_idle=$need_idle
        fi
        echo "  idle-sec=$case_idle (auto from largest payload @ ${rate}Mbps src)"
        echo "  sending (may take ~${need_idle}s)..."

        case_start=$(date +%s)
        n2_mon_pid=
        n3_mon_pid=
        n2_remote_csv="/tmp/wire-mf-${timestamp}-${label}-n2.csv"
        n3_remote_csv="/tmp/wire-mf-${timestamp}-${label}-n3.csv"
        n2_local_csv="$result_dir/monitor/${label}-node2.csv"
        n3_local_csv="$result_dir/monitor/${label}-node3.csv"

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

        if [ "$monitor_relays" = "1" ]; then
            n2_mon_pid=$(start_relay_monitor "$node2_ssh" "$node2_ifaces" \
                "$n2_remote_csv" "$n2_local_csv")
            n3_mon_pid=$(start_relay_monitor "$node3_ssh" "$node3_ifaces" \
                "$n3_remote_csv" "$n3_local_csv")
            echo "  monitors: n2_pid=${n2_mon_pid:-NA} n3_pid=${n3_mon_pid:-NA}"
            sleep 1
        fi

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

        if [ "$monitor_relays" = "1" ]; then
            stop_relay_monitor "$node2_ssh" "${n2_mon_pid:-}" "$n2_remote_csv" "$n2_local_csv"
            stop_relay_monitor "$node3_ssh" "${n3_mon_pid:-}" "$n3_remote_csv" "$n3_local_csv"
        fi

        case_end=$(date +%s)
        case_elapsed=$((case_end - case_start))

        n2_peak=NA
        n2_avg=NA
        n3_peak=NA
        n3_avg=NA
        if [ "$monitor_relays" = "1" ]; then
            # shellcheck disable=SC2086
            set -- $(monitor_mbps_summary "$n2_local_csv")
            n2_peak=${1:-NA}
            n2_avg=${2:-NA}
            # shellcheck disable=SC2086
            set -- $(monitor_mbps_summary "$n3_local_csv")
            n3_peak=${1:-NA}
            n3_avg=${2:-NA}
            echo "  measured: N2 peak/avg=${n2_peak}/${n2_avg} Mbps  N3 peak/avg=${n3_peak}/${n3_avg} Mbps  (est link ${case_link})"
        fi

        loss_sum=0
        loss_n=0
        e2e_sum=0
        e2e_n=0
        case_notes=
        loss_display=NA

        fid=0
        while [ "$fid" -lt "$flows" ]; do
            want_hash=$(tr -d ' \n' < "$payload_dir/flow${fid}.sha256")
            payload_bytes=$(tr -d ' \n' < "$payload_dir/flow${fid}.bytes")
            flow_status=FAIL
            fail_reason=no_match
            log_fid=$(wire_to_log_fid "$fid" "$receiver_log")

            remote_hit=
            remote_hash=NA
            log_out=$(flow_output_path "$log_fid" "$receiver_log")
            if [ -n "$log_out" ] && [ "$log_out" != "NA" ]; then
                remote_hash=$(remote_sha256 "$log_out" || true)
                if [ -n "$remote_hash" ] && [ "$remote_hash" = "$want_hash" ]; then
                    remote_hit=$log_out
                fi
            fi
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

            recv_blocks=$(flow_incomplete_field "$log_fid" received_blocks "$receiver_log")
            expect_blocks=$(flow_incomplete_field "$log_fid" expected_blocks "$receiver_log")
            missing_groups=$(flow_incomplete_field "$log_fid" missing_groups "$receiver_log")

            if [ -n "$remote_hit" ]; then
                flow_status=PASS
                flows_pass=$((flows_pass + 1))
                fail_reason=
            else
                out_bytes_guess=$(flow_csv_field "$log_fid" output_bytes "$receiver_log")
                if [ "$recv_blocks" != "NA" ]; then
                    fail_reason="incomplete_recv=${recv_blocks}/${expect_blocks}_missing_groups=${missing_groups}"
                elif [ "$out_bytes_guess" != "NA" ] && [ "$out_bytes_guess" != "$payload_bytes" ]; then
                    fail_reason="size_mismatch_out=${out_bytes_guess}_want=${payload_bytes}"
                elif [ "$remote_hash" != "NA" ] && [ -n "$remote_hash" ] &&
                    [ "$remote_hash" != "$want_hash" ]; then
                    fail_reason="content_hash_mismatch"
                else
                    fail_reason="missing_or_incomplete"
                fi
                if [ -n "$case_notes" ]; then
                    case_notes="$case_notes; "
                fi
                case_notes="${case_notes}f${fid}:${fail_reason}"
            fi

            datagrams=$(flow_csv_field "$log_fid" datagrams "$receiver_log")
            seen=$(flow_csv_field "$log_fid" seen_datagrams "$receiver_log")
            late=$(flow_csv_field "$log_fid" late "$receiver_log")
            recovered=$(flow_csv_field "$log_fid" recovered_groups "$receiver_log")
            dropped=$(flow_csv_field "$log_fid" dropped_groups "$receiver_log")
            out_bytes=$(flow_csv_field "$log_fid" output_bytes "$receiver_log")
            e2e_p95=$(flow_latency_field "$log_fid" end_to_end p95_us "$receiver_log")

            # shellcheck disable=SC2086
            set -- $(loss_stats "$codec" "$payload_bytes" "$datagrams" "$seen" "$late" \
                "$dropped" "$recovered" "$recv_blocks" "$expect_blocks")
            est_loss=${1:-NA}

            echo "$codec,$rate,$case_link_per_flow,$fid,$flow_status,$est_loss,$payload_bytes,$out_bytes,$e2e_p95,$fail_reason" \
                >> "$flows_csv"

            case "$est_loss" in
                NA|'') ;;
                *)
                    loss_sum=$(awk -v a="$loss_sum" -v b="$est_loss" 'BEGIN{print a+b}')
                    loss_n=$((loss_n + 1))
                    # Prefer the largest flow's loss for the case display.
                    if [ "$payload_bytes" -eq "$max_payload" ]; then
                        loss_display=$est_loss
                    fi
                    ;;
            esac
            case "$e2e_p95" in
                NA|'') ;;
                *)
                    e2e_sum=$(awk -v a="$e2e_sum" -v b="$e2e_p95" 'BEGIN{print a+b}')
                    e2e_n=$((e2e_n + 1))
                    ;;
            esac

            if [ "$flow_status" = "PASS" ]; then
                echo "  flow $fid -> PASS  loss%=$est_loss  e2e_p95=$e2e_p95"
            else
                echo "  flow $fid -> FAIL  loss%=$est_loss  e2e_p95=$e2e_p95  reason=$fail_reason"
                echo "           wire_id=$fid log_id=$log_fid out_bytes=$out_bytes"
                echo "           recv_blocks=$recv_blocks expect=$expect_blocks missing_groups=$missing_groups"
            fi
            fid=$((fid + 1))
        done

        if [ "$fetch_out" = "1" ]; then
            echo "  fetching remote outputs..."
            # shellcheck disable=SC2086
            scp $ssh_opts "$receiver_ssh:$remote_prefix*" "$case_dir/" >/dev/null 2>&1 || \
                echo "  warning: scp fetch failed or partial" >&2
        fi

        if [ "$sender_rc" -eq 0 ] && [ "$receiver_rc" -eq 0 ] &&
            [ "$flows_pass" -eq "$flows" ]; then
            status=PASS
        fi

        if [ "$loss_display" = "NA" ] && [ "$loss_n" -gt 0 ]; then
            loss_display=$(awk -v s="$loss_sum" -v n="$loss_n" 'BEGIN{printf "%.4f", s/n}')
        fi
        e2e_avg=NA
        if [ "$e2e_n" -gt 0 ]; then
            e2e_avg=$(awk -v s="$e2e_sum" -v n="$e2e_n" 'BEGIN{printf "%.3f", s/n}')
        fi
        if [ -z "$case_notes" ]; then
            case_notes="—"
        fi

        echo "$codec,$rate,$case_link,$n2_peak,$n2_avg,$n3_peak,$n3_avg,$flows,$status,$flows_pass/$flows,$loss_display,$e2e_avg,$case_elapsed,$sender_rc,$receiver_rc,$case_notes" \
            >> "$csv"
        echo "| $codec | $rate | $case_link | $n2_peak | $n2_avg | $n3_peak | $n3_avg | $status | $loss_display | $e2e_avg | $case_notes |" \
            >> "$markdown"

        if [ "$status" != "PASS" ]; then
            echo "- \`$label\`: $case_notes (snd=$sender_rc rcv=$receiver_rc)" >> "$fail_notes"
        fi

        case_total=$((case_total + 1))
        if [ "$status" = "PASS" ]; then
            case_pass=$((case_pass + 1))
        fi
        echo "  -> case $status ($flows_pass/$flows)  src=${rate}Mbps/flow  link_est=${case_link}  N2=${n2_peak}/${n2_avg}  N3=${n3_peak}/${n3_avg}  loss%=$loss_display"

        if [ "$keep_remote" != "1" ]; then
            ssh $ssh_opts "$receiver_ssh" "rm -rf '$remote_base'" >/dev/null 2>&1 || true
        fi

        case_number=$((case_number + 1))
    done
done

{
    echo
    echo "## Summary"
    echo
    echo "- Cases PASS: **$case_pass / $case_total**"
    if [ -s "$fail_notes" ]; then
        echo
        echo "### Failures"
        echo
        cat "$fail_notes"
        echo
    fi
    echo "Kept under \`$result_dir\`:"
    echo "- \`results.md\` / \`results.csv\` / \`flows.csv\`"
    echo "- \`logs/\` (sender + receiver)"
    if [ "$monitor_relays" = "1" ]; then
        echo "- \`monitor/<case>-node{2,3}.csv\` — real relay NIC Mbps"
    fi
    echo "- \`payloads/\` (sha256 only for user files; tiny seeds if synthesized)"
} >> "$markdown"
rm -f "$fail_notes"

echo
echo "Done. Cases PASS $case_pass / $case_total"
echo "  Report: $markdown"
echo
cat "$markdown"
