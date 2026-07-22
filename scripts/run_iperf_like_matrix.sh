#!/usr/bin/env sh

# Sweep CODEC × RATE_MBPS using run_iperf_like_wire.sh and write a summary table.
#
# Example (from Node1):
#   CODECS="copy xor-fec" RATES="1 2" \
#   NODE2_SSH=fyp1@10.10.10.162 NODE2_IP=10.10.12.2 \
#   NODE3_SSH=fyp1@10.10.10.163 NODE3_IP=10.10.23.2 \
#   NODE4_SSH=fyp1@10.10.10.164 NODE4_IP=10.10.34.2 \
#     ./scripts/run_iperf_like_matrix.sh input-128m.ts

set -u

input_path=${1:-}
codecs=${CODECS:-"copy xor-fec"}
rates=${RATES:-"1 2"}
extra_env=${EXTRA_ENV:-}
remote_repo=${REMOTE_REPO:-"$HOME/work/multi-flow-rate-control"}
timestamp=$(date +%Y%m%d-%H%M%S)
matrix_dir=${MATRIX_DIR:-"build/iperf-like-matrix-$timestamp"}
runs_dir="$matrix_dir/runs"
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
wire_script="$script_dir/run_iperf_like_wire.sh"

die() {
    echo "error: $*" >&2
    exit 1
}

[ -n "$input_path" ] || die "usage: $0 INPUT_FILE"
[ -f "$input_path" ] || die "input file does not exist: $input_path"
[ -x "$wire_script" ] || die "missing $wire_script"
[ -n "${NODE2_SSH:-}" ] && [ -n "${NODE2_IP:-}" ] || die "need NODE2_SSH NODE2_IP"
[ -n "${NODE3_SSH:-}" ] && [ -n "${NODE3_IP:-}" ] || die "need NODE3_SSH NODE3_IP"
[ -n "${NODE4_SSH:-}" ] && [ -n "${NODE4_IP:-}" ] || die "need NODE4_SSH NODE4_IP"

mkdir -p "$runs_dir" || die "cannot create $matrix_dir"

csv="$matrix_dir/matrix.csv"
md="$matrix_dir/matrix.md"
echo "codec,rate_mbps,pass,total,node2_rx_peak_bps,node2_tx_peak_bps,node3_rx_peak_bps,node3_tx_peak_bps,result_dir" \
    > "$csv"

relay_peak_field() {
    # args: csv_path which(rx|tx)
    file=$1
    which=$2
    [ -f "$file" ] || { echo NA; return; }
    awk -F, -v w="$which" 'NR > 1 {
        if (w == "rx" && $3 + 0 > m) m = $3 + 0
        if (w == "tx" && $4 + 0 > m) m = $4 + 0
      }
      END { if (NR <= 1) print "NA"; else printf "%.0f", m + 0 }' "$file"
}

echo "== iperf-like matrix =="
echo "  dir:    $matrix_dir"
echo "  codecs: $codecs"
echo "  rates:  $rates"
echo

idx=0
for codec in $codecs; do
    for rate in $rates; do
        idx=$((idx + 1))
        tag=$(printf '%02d_%s_%smbps' "$idx" "$codec" "$rate")
        run_dir="$runs_dir/$tag"
        echo "---- run $tag ----"

        # shellcheck disable=SC2086
        env \
            CODEC="$codec" \
            RATE_MBPS="$rate" \
            RESULT_DIR="$run_dir" \
            REMOTE_REPO="$remote_repo" \
            NODE2_SSH="$NODE2_SSH" NODE2_IP="$NODE2_IP" \
            NODE3_SSH="$NODE3_SSH" NODE3_IP="$NODE3_IP" \
            NODE4_SSH="$NODE4_SSH" NODE4_IP="$NODE4_IP" \
            ${NODE2_IFACES:+NODE2_IFACES="$NODE2_IFACES"} \
            ${NODE3_IFACES:+NODE3_IFACES="$NODE3_IFACES"} \
            $extra_env \
            "$wire_script" "$input_path" \
            | tee "$matrix_dir/logs-$tag.txt"

        streams_csv="$run_dir/streams.csv"
        pass=0
        total=0
        if [ -f "$streams_csv" ]; then
            pass=$(awk -F, 'NR>1 && $7=="PASS"{c++} END{print c+0}' "$streams_csv")
            total=$(awk -F, 'NR>1 && NF{c++} END{print c+0}' "$streams_csv")
        fi
        n2rx=$(relay_peak_field "$run_dir/monitor/node2-ifaces.csv" rx)
        n2tx=$(relay_peak_field "$run_dir/monitor/node2-ifaces.csv" tx)
        n3rx=$(relay_peak_field "$run_dir/monitor/node3-ifaces.csv" rx)
        n3tx=$(relay_peak_field "$run_dir/monitor/node3-ifaces.csv" tx)

        echo "$codec,$rate,$pass,$total,$n2rx,$n2tx,$n3rx,$n3tx,$run_dir" >> "$csv"
        echo "  => PASS $pass / $total  ($run_dir)"
        echo
    done
done

{
    echo "# Iperf-like matrix summary"
    echo
    echo "- Timestamp: $timestamp"
    echo "- Input: \`$input_path\`"
    echo "- CODECS: \`$codecs\`"
    echo "- RATES: \`$rates\`"
    echo "- Git: $(cd "$root_dir" && git rev-parse --short HEAD 2>/dev/null || echo NA)"
    echo
    echo "| Codec | Rate Mbps | PASS | Total | N2 rx peak | N2 tx peak | N3 rx peak | N3 tx peak | Result dir |"
    echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
    awk -F, 'NR>1 {
        printf "| %s | %s | %s | %s | %s | %s | %s | %s | `%s` |\n",
               $1,$2,$3,$4,$5,$6,$7,$8,$9
      }' "$csv"
    echo
    echo "Per-run details: \`runs/*/summary.md\`, \`runs/*/streams.csv\`, \`runs/*/monitor/\`."
} > "$md"

echo "== matrix done =="
echo "  Summary: $md"
echo "  CSV:     $csv"
cat "$md"
