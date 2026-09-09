#!/usr/bin/env sh
# Compare --codec none vs rs (default RS_PROFILE=16+2) for CPU/RSS while
# sweeping flow count and per-flow file size.
#
# Run on Node1 (sender host):
#   RECEIVER_SSH=fyp1@10.10.10.164 RECEIVER_DATA_IP=10.10.34.2 \
#     ./scripts/run_ffrs_resource_matrix.sh
#
# Env:
#   CODECS="none rs"
#   FLOWS_LIST="1 2 4"
#   SIZES_MB="5 20 50"     # per-flow payload size
#   RATE_MBPS=20           # source pacing per flow (ignored if RATES_LIST set)
#   RATES_LIST="50 100 150"  # optional rate sweep (Mbps)
#   RS_PROFILE=16+2
#   PORT_BASE=9600
#   SAMPLE_HZ=0.5
#   RESULT_DIR=build/ffrs-resource-<ts>
#   FINAL_DST TTL LOCAL_NODE_ID   1-hop N1→N2: 2 2 2  (omit = binary defaults 4/8/4)

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
cd "$repo_dir"

receiver_ssh=${RECEIVER_SSH:-fyp1@10.10.10.164}
receiver_ip=${RECEIVER_DATA_IP:-10.10.34.2}
remote_repo=${RECEIVER_REPO:-"$HOME/work/multi-flow-rate-control"}
codecs=${CODECS:-"none rs"}
flows_list=${FLOWS_LIST:-"1 2 4"}
sizes_mb=${SIZES_MB:-"5 20 50"}
rate_mbps=${RATE_MBPS:-20}
rates_list=${RATES_LIST:-"$rate_mbps"}
rs_profile=${RS_PROFILE:-16+2}
port_base=${PORT_BASE:-9600}
sample_hz=${SAMPLE_HZ:-0.5}
idle_sec=${IDLE_SEC:-60}
final_dst=${FINAL_DST:-}
ttl=${TTL:-}
local_node_id=${LOCAL_NODE_ID:-}
ssh_opts="-o BatchMode=yes -o ConnectTimeout=10"
timestamp=$(date +%Y%m%d-%H%M%S)
result_dir=${RESULT_DIR:-"build/ffrs-resource-$timestamp"}
bin=./build/wg_multi_pipeline
mon_py="$script_dir/proc_resource_monitor.py"

die() { echo "error: $*" >&2; exit 1; }

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

summarize_csv() {
    # Print: avg_cpu peak_cpu peak_rss_kb
    python3 - "$1" <<'PY'
import csv, sys
path = sys.argv[1]
cpus, rss = [], []
with open(path, newline="") as f:
    for row in csv.DictReader(f):
        try:
            cpus.append(float(row["cpu_pct"]))
        except (KeyError, ValueError):
            pass
        try:
            if row.get("rss_kb"):
                rss.append(float(row["rss_kb"]))
        except ValueError:
            pass
if not cpus:
    print("NA NA NA")
else:
    avg = sum(cpus) / len(cpus)
    peak_c = max(cpus)
    peak_r = max(rss) if rss else float("nan")
    print(f"{avg:.2f} {peak_c:.2f} {peak_r:.0f}")
PY
}

need_cmd ssh
need_cmd scp
need_cmd python3
test -x "$bin" || die "missing $bin (make wg-demo)"
test -f "$mon_py" || die "missing $mon_py"

mkdir -p "$result_dir/logs" "$result_dir/payloads" "$result_dir/monitor"
summary_csv="$result_dir/summary.csv"
echo "codec,flows,size_mb,rate_mbps,status,loss_ok,sender_avg_cpu,sender_peak_cpu,sender_peak_rss_kb,recv_avg_cpu,recv_peak_cpu,recv_peak_rss_kb,n2_avg_mbps,e2e_p95_us,elapsed_s,notes" \
    > "$summary_csv"

# Stage monitor on receiver.
scp $ssh_opts "$mon_py" "$receiver_ssh:/tmp/proc_resource_monitor.py" >/dev/null

rs_args=
case " $codecs " in
    *" rs "*) rs_args="--rs-profile=$rs_profile" ;;
esac

echo "FF-RS resource matrix"
wire_hdr_args=
recv_local_args=
if [ -n "$final_dst" ]; then
    wire_hdr_args="$wire_hdr_args --final-dst $final_dst"
fi
if [ -n "$ttl" ]; then
    wire_hdr_args="$wire_hdr_args --ttl $ttl"
fi
if [ -n "$local_node_id" ]; then
    recv_local_args="--local-node-id $local_node_id"
fi

echo "  codecs=$codecs  flows=$flows_list  sizes_mb=$sizes_mb  rates=$rates_list"
echo "  rs=$rs_profile  result=$result_dir"
echo "  path: $(hostname) -> $receiver_ssh ($receiver_ip)  final_dst=${final_dst:-default} ttl=${ttl:-default} local=${local_node_id:-default}"

case_i=0
for flows in $flows_list; do
    for size_mb in $sizes_mb; do
      for rate_mbps in $rates_list; do
        for codec in $codecs; do
            case_i=$((case_i + 1))
            port=$((port_base + case_i))
            label="${codec}-f${flows}-s${size_mb}m-r${rate_mbps}"
            echo "=== $label (UDP $port) ==="

            # Share payloads across codecs for the same (flows,size) to avoid
            # re-dd of large files and to save disk.
            payload_dir="$result_dir/payloads/shared-f${flows}-s${size_mb}m"
            mkdir -p "$payload_dir"
            flow_args=
            out_suffix_args=
            fid=0
            while [ "$fid" -lt "$flows" ]; do
                fpath="$payload_dir/flow${fid}.bin"
                if [ ! -f "$fpath" ]; then
                    echo "  generating ${size_mb}MiB payload flow $fid ..."
                    dd if=/dev/urandom of="$fpath" bs=1M count="$size_mb" status=none
                    sha256sum "$fpath" | awk '{print $1}' > "$payload_dir/flow${fid}.sha256"
                elif [ ! -f "$payload_dir/flow${fid}.sha256" ]; then
                    sha256sum "$fpath" | awk '{print $1}' > "$payload_dir/flow${fid}.sha256"
                fi
                if [ "$rate_mbps" = "0" ]; then
                    flow_args="$flow_args --flow ${fid}:${receiver_ip}:${port}:${fpath}"
                else
                    flow_args="$flow_args --flow ${fid}:${receiver_ip}:${port}:${fpath}:${rate_mbps}"
                fi
                out_suffix_args="$out_suffix_args --out-suffix ${fid}:.bin"
                fid=$((fid + 1))
            done

            case_rs=
            if [ "$codec" = "rs" ]; then
                case_rs=$rs_args
            fi

            remote_base="$remote_repo/build/ffrs-res-$timestamp-$label"
            remote_prefix="$remote_base/out_"
            recv_log="$result_dir/logs/${label}-receiver.log"
            send_log="$result_dir/logs/${label}-sender.log"
            send_mon="$result_dir/monitor/${label}-sender.csv"
            recv_mon="$result_dir/monitor/${label}-receiver.csv"
            recv_stop="/tmp/ffrs-res-stop-$timestamp-$label"
            send_stop="$result_dir/monitor/${label}-sender.stop"

            ssh $ssh_opts "$receiver_ssh" \
                "cd '$remote_repo' && rm -rf '$remote_base' && mkdir -p '$remote_base' && rm -f '$recv_stop'" \
                || die "remote mkdir failed"

            # shellcheck disable=SC2086
            ssh $ssh_opts "$receiver_ssh" \
                "cd '$remote_repo' && exec $bin --codec '$codec' --lock-memory $case_rs \
                  --udp-recv '$port' '$remote_prefix' --max-flows '$flows' \
                  --idle-sec '$idle_sec' --strict $recv_local_args $out_suffix_args" \
                >"$recv_log" 2>&1 &
            recv_ssh_pid=$!
            sleep 1

            # Discover remote binary PID (newest wg_multi_pipeline).
            recv_pid=$(ssh $ssh_opts "$receiver_ssh" \
                "pgrep -n -f '[w]g_multi_pipeline.*--udp-recv $port' || true")
            if [ -z "$recv_pid" ]; then
                echo "  receiver PID not found; see $recv_log" >&2
                kill "$recv_ssh_pid" 2>/dev/null || true
                wait "$recv_ssh_pid" 2>/dev/null || true
                echo "$codec,$flows,$size_mb,$rate_mbps,FAIL,0,NA,NA,NA,NA,NA,NA,NA,NA,0,no_recv_pid" \
                    >> "$summary_csv"
                continue
            fi
            ssh $ssh_opts "$receiver_ssh" \
                "nohup python3 /tmp/proc_resource_monitor.py '$recv_pid' '$sample_hz' \
                  '$remote_base/receiver_res.csv' '$recv_stop' >/dev/null 2>&1 & echo \$!" \
                >/tmp/ffrs_recv_mon_pid || true

            t0=$(date +%s)
            # shellcheck disable=SC2086
            rm -f "$send_stop"
            # Start sender in background so we can attach monitor to its PID.
            # shellcheck disable=SC2086
            $bin --codec "$codec" $case_rs $wire_hdr_args --udp-send-multi $flow_args \
                >"$send_log" 2>&1 &
            send_pid=$!
            sleep 0.3
            python3 "$mon_py" "$send_pid" "$sample_hz" "$send_mon" "$send_stop" \
                >/dev/null 2>&1 &
            send_mon_pid=$!

            send_rc=0
            if ! wait "$send_pid"; then
                send_rc=$?
            fi
            date +%s > "$send_stop" 2>/dev/null || touch "$send_stop"
            wait "$send_mon_pid" 2>/dev/null || true

            # Let receiver idle-flush, then stop its monitor and process.
            sleep 2
            ssh $ssh_opts "$receiver_ssh" "touch '$recv_stop'" >/dev/null 2>&1 || true
            if kill -0 "$recv_ssh_pid" 2>/dev/null; then
                # Wait for idle exit; kill if hung past idle+margin.
                wait_deadline=$((idle_sec + 40))
                waited=0
                while kill -0 "$recv_ssh_pid" 2>/dev/null && [ "$waited" -lt "$wait_deadline" ]; do
                    sleep 1
                    waited=$((waited + 1))
                done
                if kill -0 "$recv_ssh_pid" 2>/dev/null; then
                    ssh $ssh_opts "$receiver_ssh" \
                        "pkill -f '[w]g_multi_pipeline.*--udp-recv $port' || true" \
                        >/dev/null 2>&1 || true
                    wait "$recv_ssh_pid" 2>/dev/null || true
                else
                    wait "$recv_ssh_pid" 2>/dev/null || true
                fi
            fi
            t1=$(date +%s)
            elapsed=$((t1 - t0))

            scp $ssh_opts "$receiver_ssh:$remote_base/receiver_res.csv" "$recv_mon" \
                >/dev/null 2>&1 || true

            # Hash check:
            # max_flows==1 writes literally to output_path (the prefix).
            # max_flows>1 uses demux names: {prefix}src_*_flow_<id><suffix>
            flows_pass=0
            fid=0
            while [ "$fid" -lt "$flows" ]; do
                want=$(tr -d ' \n' < "$payload_dir/flow${fid}.sha256")
                if [ "$flows" -eq 1 ]; then
                    got=$(ssh $ssh_opts "$receiver_ssh" \
                        "sha256sum '$remote_prefix' 2>/dev/null | awk '{print \$1}'" \
                        || true)
                else
                    got=$(ssh $ssh_opts "$receiver_ssh" \
                        "sha256sum '$remote_base'/out_*_flow_${fid}.* \
                          '$remote_base'/out_*_flow_${fid} \
                          2>/dev/null | head -1 | awk '{print \$1}'" \
                        || true)
                fi
                if [ -n "$got" ] && [ "$got" = "$want" ]; then
                    flows_pass=$((flows_pass + 1))
                else
                    echo "  hash miss flow $fid want=$want got=${got:-NONE}" >&2
                fi
                fid=$((fid + 1))
            done
            status=FAIL
            loss_ok=0
            if [ "$flows_pass" -eq "$flows" ] && [ "$send_rc" -eq 0 ]; then
                status=PASS
                loss_ok=1
            fi

            # shellcheck disable=SC2086
            set -- $(summarize_csv "$send_mon")
            s_avg=${1:-NA}; s_peak=${2:-NA}; s_rss=${3:-NA}
            # shellcheck disable=SC2086
            set -- $(summarize_csv "$recv_mon")
            r_avg=${1:-NA}; r_peak=${2:-NA}; r_rss=${3:-NA}

            e2e=$(python3 - "$recv_log" <<'PY'
import re, sys
text = open(sys.argv[1], errors="ignore").read()
m = re.findall(r"end_to_end.*?p95_us=([0-9.]+)", text)
print(m[-1] if m else "NA")
PY
)
            notes=
            if [ "$send_rc" -ne 0 ]; then
                notes="sender_rc=$send_rc"
            fi
            if [ "$flows_pass" -ne "$flows" ]; then
                notes="${notes:+$notes;}hash $flows_pass/$flows"
            fi
            [ -n "$notes" ] || notes=—

            echo "$codec,$flows,$size_mb,$rate_mbps,$status,$loss_ok,$s_avg,$s_peak,$s_rss,$r_avg,$r_peak,$r_rss,NA,$e2e,$elapsed,$notes" \
                >> "$summary_csv"
            echo "  -> $status hash=$flows_pass/$flows  sendCPU avg/peak=$s_avg/$s_peak rss=${s_rss}kB  recvCPU avg/peak=$r_avg/$r_peak rss=${r_rss}kB  ${elapsed}s"

            # Cleanup remote outputs to save disk.
            ssh $ssh_opts "$receiver_ssh" "rm -rf '$remote_base'" >/dev/null 2>&1 || true
        done
      done
    done
done

echo
echo "Done. Summary: $summary_csv"
column -t -s, "$summary_csv" 2>/dev/null || cat "$summary_csv"
