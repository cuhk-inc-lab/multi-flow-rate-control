#!/usr/bin/env sh

set -eu

bin=${1:?usage: wire_rs_test.sh path-to-wg_multi_pipeline}
base=${2:-build}
full_port=22005
one_loss_port=22006
two_loss_port=22007
partial_port=22008
input="$base/wire_rscode_input.ts"
ready_timeout_sec=${READY_TIMEOUT_SEC:-5}
receiver_pid=
proxy_pid=

cleanup() {
    if [ -n "${receiver_pid:-}" ]; then
        kill "$receiver_pid" 2>/dev/null || true
        wait "$receiver_pid" 2>/dev/null || true
        receiver_pid=
    fi
    if [ -n "${proxy_pid:-}" ]; then
        kill "$proxy_pid" 2>/dev/null || true
        wait "$proxy_pid" 2>/dev/null || true
        proxy_pid=
    fi
}
trap cleanup EXIT INT TERM

python3 - "$input" <<'PY'
from pathlib import Path
import sys
Path(sys.argv[1]).write_bytes(b"A" * 1400 + b"B" * 1400 +
                              b"C" * 1400 + b"D" * 1400)
PY

wait_receiver_ready() {
    log=$1
    deadline=$(($(date +%s) + ready_timeout_sec))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if [ -f "$log" ] && grep -q 'udp-recv: listening on UDP port' "$log" 2>/dev/null; then
            return 0
        fi
        if [ -n "${receiver_pid:-}" ] && ! kill -0 "$receiver_pid" 2>/dev/null; then
            echo "error: receiver exited before ready (log=$log)" >&2
            [ -f "$log" ] && cat "$log" >&2
            cleanup
            return 1
        fi
        sleep 0.05
    done
    echo "error: timed out after ${ready_timeout_sec}s waiting for receiver ready (log=$log)" >&2
    [ -f "$log" ] && cat "$log" >&2
    cleanup
    return 1
}

start_receiver() {
    port=$1
    output=$2
    shift 2
    rlog="$output.log"

    # Truncate this case's output/log so ready wait never matches a prior run.
    : >"$output"
    : >"$rlog"

    # idle-sec=3: ready-log wait already guarantees bind; 3s only avoids false
    # idle timeouts under test scheduling / sender startup. Does not change
    # RS recover, strict incomplete, cmp, or best-effort success criteria.
    "$bin" --codec rs --udp-recv "$port" "$output" \
        --idle-sec 3 "$@" >"$rlog" 2>&1 &
    receiver_pid=$!

    wait_receiver_ready "$rlog" || return 1
    # Brief post-ready buffer only; not the sync mechanism.
    sleep 0.05
}

start_drop_proxy() {
    listen_port=$1
    destination_port=$2
    dropped_indexes=$3

    python3 - "$listen_port" "$destination_port" "$dropped_indexes" <<'PY' &
import select
import socket
import struct
import sys
import time

listen_port, destination_port = map(int, sys.argv[1:3])
dropped = {int(index) for index in sys.argv[3].split(",") if index}
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("127.0.0.1", listen_port))
deadline = time.monotonic() + 5
end_deadline = None

while time.monotonic() < deadline:
    readable, _, _ = select.select([sock], [], [], 0.1)
    if not readable:
        if end_deadline is not None and time.monotonic() >= end_deadline:
            break
        continue
    packet, _ = sock.recvfrom(2048)
    if len(packet) < 44:
        continue
    # Wire header v3: magic, ver, type, final_dst, ttl, flow_id, block_id, shard_index, ...
    _, version, packet_type, _, _, _, _, shard_index, _, _, _ = \
        struct.unpack("!IBBBBIQHHHH", packet[:28])
    if version != 3:
        continue
    if packet_type == 1 and shard_index in dropped:
        continue
    sock.sendto(packet, ("127.0.0.1", destination_port))
    if packet_type == 2:
        end_deadline = time.monotonic() + 0.25
sock.close()
PY
    proxy_pid=$!
    sleep 0.1
}

run_sender() {
    port=$1
    "$bin" --codec rs --udp-send 127.0.0.1 "$port" "$input"
}

run_full() {
    output="$base/wire_rscode_full.ts"
    start_receiver "$full_port" "$output"
    run_sender "$full_port"
    wait "$receiver_pid"
    receiver_pid=
    cmp "$input" "$output"
}

run_recovery() {
    proxy_port=$1
    receiver_port=$2
    dropped=$3
    output=$4

    start_receiver "$receiver_port" "$output"
    start_drop_proxy "$proxy_port" "$receiver_port" "$dropped"
    run_sender "$proxy_port"
    wait "$proxy_pid"
    proxy_pid=
    wait "$receiver_pid"
    receiver_pid=
    cmp "$input" "$output"
}

run_strict_failure() {
    output="$base/wire_rscode_strict_incomplete.ts"

    start_receiver "$partial_port" "$output"
    start_drop_proxy 22018 "$partial_port" "1,3,4"
    run_sender 22018
    wait "$proxy_pid"
    proxy_pid=
    if wait "$receiver_pid"; then
        echo "strict RS receiver unexpectedly accepted an incomplete group" >&2
        exit 1
    fi
    receiver_pid=
    grep -q "incomplete" "$output.log"
}

run_best_effort() {
    output="$base/wire_rscode_partial.ts"
    expected="$base/wire_rscode_partial_expected.ts"

    python3 - "$expected" <<'PY'
from pathlib import Path
import sys
Path(sys.argv[1]).write_bytes(b"A" * 1400 + b"C" * 1400)
PY
    start_receiver "$partial_port" "$output" --best-effort
    start_drop_proxy 22019 "$partial_port" "1,3,4"
    run_sender 22019
    wait "$proxy_pid"
    proxy_pid=
    wait "$receiver_pid"
    receiver_pid=
    cmp "$expected" "$output"
    grep -q "missing_data_shards=2" "$output.log"
    grep -q "skipped_groups=1" "$output.log"
}

run_full
run_recovery 22016 "$one_loss_port" "2" "$base/wire_rscode_one_loss.ts"
run_recovery 22017 "$two_loss_port" "1,4" "$base/wire_rscode_two_loss.ts"
run_strict_failure
run_best_effort

echo "wire rscode RS full, recovery, strict, and best-effort tests passed"
