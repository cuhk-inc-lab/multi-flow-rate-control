#!/usr/bin/env sh

# Concurrent 6-stream iperf3 UDP baseline — same topology as run_iperf_like_wire.sh.
# Use this to tell whether the path itself drops UDP, vs wg_multi_pipeline.
#
# Streams (same as wire script):
#   1) Node1 -> Node4
#   2) Node2 -> Node4
#   3) Node1 -> Node2
#   4) Node2 -> Node2  (loopback)
#   5) Node1 -> Node3
#   6) Node1 -> Node4  (shorter duration)
#
# Each stream uses its own iperf3 port (BASE_PORT .. BASE_PORT+5).
#
# Example (match a failing wire run's rates/durations):
#   RATE_S1=1 RATE_S2=1 RATE_S3=2 RATE_S4=2 RATE_S5=1 RATE_S6=2 \
#   DURATION_S=30 DURATION_SHORT_S=20 \
#   NODE2_SSH=fyp1@10.10.10.162 NODE2_IP=10.10.12.2 \
#   NODE3_SSH=fyp1@10.10.10.163 NODE3_IP=10.10.23.2 \
#   NODE4_SSH=fyp1@10.10.10.164 NODE4_IP=10.10.34.2 \
#     ./scripts/run_iperf_like_baseline.sh

set -u

rate_mbps=${RATE_MBPS:-1}
rate_s1=${RATE_S1:-$rate_mbps}
rate_s2=${RATE_S2:-$rate_mbps}
rate_s3=${RATE_S3:-$rate_mbps}
rate_s4=${RATE_S4:-$rate_mbps}
rate_s5=${RATE_S5:-$rate_mbps}
rate_s6=${RATE_S6:-$rate_mbps}
dur_s=${DURATION_S:-20}
dur_short_s=${DURATION_SHORT_S:-15}
barrier_sec=${BARRIER_SEC:-5}
base_port=${BASE_PORT:-15201}
loss_max_pct=${LOSS_MAX_PCT:-1}
ssh_opts="-o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=5 -o ServerAliveCountMax=2"
ssh_cmd_timeout=${SSH_CMD_TIMEOUT:-20}
timestamp=$(date +%Y%m%d-%H%M%S)
result_dir=${RESULT_DIR:-"build/iperf-like-baseline-$timestamp"}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
monitor_py="$script_dir/iperf_like_monitor.py"
monitor_hz=${MONITOR_HZ:-1}

node2_ssh=${NODE2_SSH:-}
node3_ssh=${NODE3_SSH:-}
node4_ssh=${NODE4_SSH:-}
node2_ip=${NODE2_IP:-}
node3_ip=${NODE3_IP:-}
node4_ip=${NODE4_IP:-}
node1_ifaces=${NODE1_IFACES:-"station0"}
node2_ifaces=${NODE2_IFACES:-"ap0 station1"}
node3_ifaces=${NODE3_IFACES:-"ap1 station2"}
node4_ifaces=${NODE4_IFACES:-"ap2"}

port_s1=$base_port
port_s2=$((base_port + 1))
port_s3=$((base_port + 2))
port_s4=$((base_port + 3))
port_s5=$((base_port + 4))
port_s6=$((base_port + 5))

usage() {
    cat >&2 <<'EOF'
Usage: ./scripts/run_iperf_like_baseline.sh

Required env:
  NODE2_SSH NODE2_IP
  NODE3_SSH NODE3_IP
  NODE4_SSH NODE4_IP

Optional env (same knobs as run_iperf_like_wire.sh):
  RATE_MBPS / RATE_S1..RATE_S6
  DURATION_S / DURATION_SHORT_S
  BASE_PORT=15201         (streams use BASE_PORT .. BASE_PORT+5)
  LOSS_MAX_PCT=1          (PASS if lost_percent <= this)
  BARRIER_SEC=5
  MONITOR_HZ=1
  NODE1_IFACES / NODE2_IFACES / NODE3_IFACES / NODE4_IFACES

Example:
  RATE_S1=1 RATE_S2=1 RATE_S3=2 RATE_S4=2 RATE_S5=1 RATE_S6=2 \
  DURATION_S=30 DURATION_SHORT_S=20 \
  NODE2_SSH=fyp1@10.10.10.162 NODE2_IP=10.10.12.2 \
  NODE3_SSH=fyp1@10.10.10.163 NODE3_IP=10.10.23.2 \
  NODE4_SSH=fyp1@10.10.10.164 NODE4_IP=10.10.34.2 \
    ./scripts/run_iperf_like_baseline.sh
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

ssh_run() {
    host=$1
    shift
    # shellcheck disable=SC2086
    ssh $ssh_opts "$host" "$@"
}

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

start_local_monitor() {
    ifaces=$1
    local_csv=$2
    hz=$3
    [ -f "$monitor_py" ] || { echo ""; return; }
    iface_arg=${ifaces:-"-"}
    python3 "$monitor_py" "$iface_arg" "$hz" "$local_csv" >/dev/null 2>&1 &
    echo $!
}

install_remote_monitor() {
    host=$1
    ifaces=$2
    remote_csv=$3
    hz=$4
    [ -f "$monitor_py" ] || { echo ""; return; }
    iface_arg=${ifaces:-"-"}
    # shellcheck disable=SC2086
    scp $ssh_opts "$monitor_py" "$host:/tmp/iperf_like_monitor.py" >/dev/null \
        || die "scp monitor failed to $host"
    ssh_run "$host" "nohup python3 /tmp/iperf_like_monitor.py '$iface_arg' '$hz' '$remote_csv' >/dev/null 2>&1 & echo \$!"
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit 0
fi

need NODE2_SSH
need NODE3_SSH
need NODE4_SSH
need NODE2_IP
need NODE3_IP
need NODE4_IP

command -v iperf3 >/dev/null 2>&1 || die "iperf3 missing on Node1"
for host in "$node2_ssh" "$node3_ssh" "$node4_ssh"; do
    ssh_run_timeout "$host" "command -v iperf3 >/dev/null" \
        || die "iperf3 missing on $host"
done

mkdir -p "$result_dir/logs" "$result_dir/json" "$result_dir/monitor" "$result_dir/remote" \
    || die "cannot create $result_dir"

echo "== iperf3 UDP baseline (same 6-stream map as wire test) =="
echo "  result: $result_dir"
echo "  rates:  $rate_s1/$rate_s2/$rate_s3/$rate_s4/$rate_s5/$rate_s6 Mbps"
echo "  dur:    ${dur_s}s (stream6 ${dur_short_s}s)"
echo "  ports:  $port_s1..$port_s6"
echo "  PASS if lost_percent <= ${loss_max_pct}%"
echo

# Kill any leftover iperf3 servers on these ports (best-effort).
cleanup_ports() {
    host=$1
    shift
    ports=$*
    for p in $ports; do
        ssh_run "$host" "
          fuser -k ${p}/tcp >/dev/null 2>&1 || true
          fuser -k ${p}/udp >/dev/null 2>&1 || true
          # fall back if fuser missing
          for pid in \$(ss -ltnp 2>/dev/null | awk -v p=':${p}' '\$4 ~ p {print \$NF}' | sed -n 's/.*pid=\\([0-9]*\\).*/\\1/p'); do
            kill \$pid 2>/dev/null || true
          done
        " || true
    done
}
cleanup_ports "$node2_ssh" "$port_s3" "$port_s4"
cleanup_ports "$node3_ssh" "$port_s5"
cleanup_ports "$node4_ssh" "$port_s1" "$port_s2" "$port_s6"
# local loopback server also on node2
sleep 0.3

start_iperf_server() {
    host=$1
    port=$2
    log_rel=$3
    # -1: one client then exit
    if ! out=$(ssh_run_timeout "$host" "
rm -f '$log_rel'
nohup iperf3 -s -p '$port' -1 >'$log_rel' 2>&1 </dev/null &
echo \$! > /tmp/iperf-srv-$port.pid
sleep 0.25
if ! kill -0 \"\$(cat /tmp/iperf-srv-$port.pid)\" 2>/dev/null; then
  tail -20 '$log_rel' >&2
  exit 1
fi
cat /tmp/iperf-srv-$port.pid
"); then
        die "failed to start iperf3 -s on $host:$port"
    fi
    printf '%s\n' "$out" | tr -d ' \r' | tail -n 1
}

echo "== starting iperf3 servers =="
# Node4: streams 1,2,6
n4_s1_pid=$(start_iperf_server "$node4_ssh" "$port_s1" "/tmp/iperf-s1-srv.log")
n4_s2_pid=$(start_iperf_server "$node4_ssh" "$port_s2" "/tmp/iperf-s2-srv.log")
n4_s6_pid=$(start_iperf_server "$node4_ssh" "$port_s6" "/tmp/iperf-s6-srv.log")
# Node2: streams 3,4
n2_s3_pid=$(start_iperf_server "$node2_ssh" "$port_s3" "/tmp/iperf-s3-srv.log")
n2_s4_pid=$(start_iperf_server "$node2_ssh" "$port_s4" "/tmp/iperf-s4-srv.log")
# Node3: stream 5
n3_s5_pid=$(start_iperf_server "$node3_ssh" "$port_s5" "/tmp/iperf-s5-srv.log")
echo "  n4: $n4_s1_pid $n4_s2_pid $n4_s6_pid | n2: $n2_s3_pid $n2_s4_pid | n3: $n3_s5_pid"
printf '%s\n' "$n4_s1_pid" "$n4_s2_pid" "$n4_s6_pid" > "$result_dir/remote/n4-srv.pids"
printf '%s\n' "$n2_s3_pid" "$n2_s4_pid" > "$result_dir/remote/n2-srv.pids"
printf '%s\n' "$n3_s5_pid" > "$result_dir/remote/n3-srv.pids"

echo "== starting node monitors =="
n1_mon_pid=$(start_local_monitor "$node1_ifaces" "$result_dir/monitor/node1-ifaces.csv" "$monitor_hz")
n2_mon_pid=$(install_remote_monitor "$node2_ssh" "$node2_ifaces" \
    "/tmp/node2-ifaces.csv" "$monitor_hz")
n3_mon_pid=$(install_remote_monitor "$node3_ssh" "$node3_ifaces" \
    "/tmp/node3-ifaces.csv" "$monitor_hz")
n4_mon_pid=$(install_remote_monitor "$node4_ssh" "$node4_ifaces" \
    "/tmp/node4-ifaces.csv" "$monitor_hz")
echo "  monitors: n1=$n1_mon_pid n2=$n2_mon_pid n3=$n3_mon_pid n4=$n4_mon_pid"

start_at=$(( $(date +%s) + barrier_sec ))
echo "== barrier START_AT=$start_at (wait ${barrier_sec}s) =="

# Node1 clients: streams 1,3,5,6
(
    while [ "$(date +%s)" -lt "$start_at" ]; do sleep 0.05; done
    iperf3 -c "$node4_ip" -p "$port_s1" -u -b "${rate_s1}M" -t "$dur_s" -J \
        > "$result_dir/json/s1.json" 2> "$result_dir/logs/s1-client.log"
    echo $? > "$result_dir/remote/s1.rc"
) &
pid_s1=$!
(
    while [ "$(date +%s)" -lt "$start_at" ]; do sleep 0.05; done
    iperf3 -c "$node2_ip" -p "$port_s3" -u -b "${rate_s3}M" -t "$dur_s" -J \
        > "$result_dir/json/s3.json" 2> "$result_dir/logs/s3-client.log"
    echo $? > "$result_dir/remote/s3.rc"
) &
pid_s3=$!
(
    while [ "$(date +%s)" -lt "$start_at" ]; do sleep 0.05; done
    iperf3 -c "$node3_ip" -p "$port_s5" -u -b "${rate_s5}M" -t "$dur_s" -J \
        > "$result_dir/json/s5.json" 2> "$result_dir/logs/s5-client.log"
    echo $? > "$result_dir/remote/s5.rc"
) &
pid_s5=$!
(
    while [ "$(date +%s)" -lt "$start_at" ]; do sleep 0.05; done
    iperf3 -c "$node4_ip" -p "$port_s6" -u -b "${rate_s6}M" -t "$dur_short_s" -J \
        > "$result_dir/json/s6.json" 2> "$result_dir/logs/s6-client.log"
    echo $? > "$result_dir/remote/s6.rc"
) &
pid_s6=$!

# Node2 clients: streams 2,4
ssh_run "$node2_ssh" "nohup sh -c '
  start_at=$start_at
  while [ \"\$(date +%s)\" -lt \"\$start_at\" ]; do sleep 0.05; done
  iperf3 -c $node4_ip -p $port_s2 -u -b ${rate_s2}M -t $dur_s -J \
    > /tmp/iperf-s2.json 2> /tmp/iperf-s2-client.log
  echo \$? > /tmp/iperf-s2.rc
' >/dev/null 2>&1 </dev/null & echo \$!" > "$result_dir/remote/n2-s2.pid"

ssh_run "$node2_ssh" "nohup sh -c '
  start_at=$start_at
  while [ \"\$(date +%s)\" -lt \"\$start_at\" ]; do sleep 0.05; done
  iperf3 -c 127.0.0.1 -p $port_s4 -u -b ${rate_s4}M -t $dur_s -J \
    > /tmp/iperf-s4.json 2> /tmp/iperf-s4-client.log
  echo \$? > /tmp/iperf-s4.rc
' >/dev/null 2>&1 </dev/null & echo \$!" > "$result_dir/remote/n2-s4.pid"

n2_s2_client=$(tr -d ' \n' < "$result_dir/remote/n2-s2.pid")
n2_s4_client=$(tr -d ' \n' < "$result_dir/remote/n2-s4.pid")
echo "  Node1 clients: s1=$pid_s1 s3=$pid_s3 s5=$pid_s5 s6=$pid_s6"
echo "  Node2 clients: s2=$n2_s2_client s4=$n2_s4_client"

echo "== waiting for clients =="
wait "$pid_s1" "$pid_s3" "$pid_s5" "$pid_s6" || true
ssh_run "$node2_ssh" "while kill -0 $n2_s2_client 2>/dev/null || kill -0 $n2_s4_client 2>/dev/null; do sleep 0.5; done" || true
echo "  clients finished"
sleep 1

echo "== stopping monitors =="
[ -n "${n1_mon_pid:-}" ] && kill "$n1_mon_pid" 2>/dev/null || true
[ -n "${n2_mon_pid:-}" ] && ssh_run "$node2_ssh" "kill $n2_mon_pid 2>/dev/null || true"
[ -n "${n3_mon_pid:-}" ] && ssh_run "$node3_ssh" "kill $n3_mon_pid 2>/dev/null || true"
[ -n "${n4_mon_pid:-}" ] && ssh_run "$node4_ssh" "kill $n4_mon_pid 2>/dev/null || true"

echo "== collecting =="
# shellcheck disable=SC2086
scp $ssh_opts "$node2_ssh:/tmp/iperf-s2.json" "$result_dir/json/s2.json" >/dev/null 2>/dev/null || true
# shellcheck disable=SC2086
scp $ssh_opts "$node2_ssh:/tmp/iperf-s4.json" "$result_dir/json/s4.json" >/dev/null 2>/dev/null || true
# shellcheck disable=SC2086
scp $ssh_opts "$node2_ssh:/tmp/iperf-s2-client.log" "$result_dir/logs/" >/dev/null 2>/dev/null || true
# shellcheck disable=SC2086
scp $ssh_opts "$node2_ssh:/tmp/iperf-s4-client.log" "$result_dir/logs/" >/dev/null 2>/dev/null || true
# shellcheck disable=SC2086
scp $ssh_opts "$node2_ssh:/tmp/node2-ifaces.csv" "$result_dir/monitor/" >/dev/null 2>/dev/null || true
# shellcheck disable=SC2086
scp $ssh_opts "$node3_ssh:/tmp/node3-ifaces.csv" "$result_dir/monitor/" >/dev/null 2>/dev/null || true
# shellcheck disable=SC2086
scp $ssh_opts "$node4_ssh:/tmp/node4-ifaces.csv" "$result_dir/monitor/" >/dev/null 2>/dev/null || true

# Best-effort cleanup leftover servers
cleanup_ports "$node2_ssh" "$port_s3" "$port_s4"
cleanup_ports "$node3_ssh" "$port_s5"
cleanup_ports "$node4_ssh" "$port_s1" "$port_s2" "$port_s6"

streams_csv="$result_dir/streams.csv"
summary_md="$result_dir/summary.md"

python3 - "$result_dir" "$loss_max_pct" \
    "$rate_s1" "$rate_s2" "$rate_s3" "$rate_s4" "$rate_s5" "$rate_s6" \
    "$dur_s" "$dur_short_s" \
    "$node2_ip" "$node3_ip" "$node4_ip" \
    "$port_s1" "$port_s2" "$port_s3" "$port_s4" "$port_s5" "$port_s6" \
    <<'PY'
import json, sys
from pathlib import Path

result_dir = Path(sys.argv[1])
loss_max = float(sys.argv[2])
rates = [sys.argv[i] for i in range(3, 9)]
dur_s, dur_short = sys.argv[9], sys.argv[10]
n2, n3, n4 = sys.argv[11], sys.argv[12], sys.argv[13]
ports = [sys.argv[i] for i in range(14, 20)]

meta = [
    (1, "local", n4, rates[0], dur_s, ports[0]),
    (2, "node2", n4, rates[1], dur_s, ports[1]),
    (3, "local", n2, rates[2], dur_s, ports[2]),
    (4, "node2", "127.0.0.1", rates[3], dur_s, ports[3]),
    (5, "local", n3, rates[4], dur_s, ports[4]),
    (6, "local", n4, rates[5], dur_short, ports[5]),
]

def parse(path: Path):
    if not path.is_file() or path.stat().st_size == 0:
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    end = data.get("end") or {}
    # Prefer receiver sum for UDP loss stats
    streams = (end.get("streams") or [])
    lost = packets = None
    bits_per_second = None
    jitter_ms = None
    lost_percent = None
    if streams:
        udp = (streams[0].get("udp") or streams[0])
        lost = udp.get("lost_packets")
        packets = udp.get("packets")
        bits_per_second = udp.get("bits_per_second")
        jitter_ms = udp.get("jitter_ms")
        lost_percent = udp.get("lost_percent")
    summ = end.get("sum") or end.get("sum_received") or {}
    if lost is None:
        lost = summ.get("lost_packets")
    if packets is None:
        packets = summ.get("packets")
    if bits_per_second is None:
        bits_per_second = summ.get("bits_per_second")
    if jitter_ms is None:
        jitter_ms = summ.get("jitter_ms")
    if lost_percent is None:
        lost_percent = summ.get("lost_percent")
    if lost_percent is None and lost is not None and packets:
        lost_percent = 100.0 * float(lost) / float(packets)
    return {
        "lost": lost,
        "packets": packets,
        "lost_percent": lost_percent,
        "bps": bits_per_second,
        "jitter_ms": jitter_ms,
        "error": (data.get("error") if isinstance(data.get("error"), str) else None),
    }

rows = []
pass_n = 0
print("stream,sender,dst,port,rate_mbps,duration_s,recv_mbps,lost_packets,packets,lost_percent,jitter_ms,status")
for sid, sender, dst, rate, dur, port in meta:
    info = parse(result_dir / "json" / f"s{sid}.json")
    if not info or info.get("lost_percent") is None:
        status = "FAIL"
        recv_mbps = lost = packets = lp = jit = "NA"
        if info and info.get("error"):
            print(f"  stream{sid}: iperf error: {info['error']}", file=sys.stderr)
    else:
        lp = float(info["lost_percent"])
        status = "PASS" if lp <= loss_max else "FAIL"
        if status == "PASS":
            pass_n += 1
        recv_mbps = f"{(info['bps'] or 0) / 1e6:.3f}" if info.get("bps") is not None else "NA"
        lost = info["lost"] if info["lost"] is not None else "NA"
        packets = info["packets"] if info["packets"] is not None else "NA"
        jit = f"{info['jitter_ms']:.3f}" if info.get("jitter_ms") is not None else "NA"
        lp = f"{lp:.4f}"
    line = f"{sid},{sender},{dst},{port},{rate},{dur},{recv_mbps},{lost},{packets},{lp},{jit},{status}"
    rows.append(line)
    print(f"  stream{sid} [{sender} -> {dst}:{port}] loss={lp}% : {status}")

csv_path = result_dir / "streams.csv"
csv_path.write_text(
    "stream,sender,dst,port,rate_mbps,duration_s,recv_mbps,lost_packets,packets,lost_percent,jitter_ms,status\n"
    + "\n".join(rows) + "\n",
    encoding="utf-8",
)

md = []
md.append("# Iperf3 UDP baseline (link check)")
md.append("")
md.append("Same 6-stream map as `run_iperf_like_wire.sh`, but plain **iperf3 -u**.")
md.append("If this PASS and wire FAIL → suspect the app/codec path.")
md.append("If this also FAIL → the shared UDP path is already lossy at these rates.")
md.append("")
md.append(f"- PASS threshold: lost_percent <= {loss_max}%")
md.append(f"- Streams PASS: {pass_n} / 6")
md.append("")
md.append("| Stream | Sender | Dest | Port | Rate Mbps | Dur s | Recv Mbps | Lost % | Jitter ms | Status |")
md.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
for line in rows:
    sid, sender, dst, port, rate, dur, recv, lost, packets, lp, jit, status = line.split(",")
    md.append(f"| {sid} | {sender} | {dst} | {port} | {rate} | {dur} | {recv} | {lp} | {jit} | {status} |")
md.append("")
md.append("## Artifacts")
md.append("")
md.append("- `streams.csv` / `json/s*.json` — iperf3 client JSON")
md.append("- `monitor/` — optional CPU/NIC timeseries (same monitor as wire test)")
md.append("")
(result_dir / "summary.md").write_text("\n".join(md) + "\n", encoding="utf-8")
print(f"PASS: {pass_n} / 6")
PY

echo
echo "Done."
echo "  Summary: $summary_md"
echo "  CSV:     $streams_csv"
cat "$summary_md"
