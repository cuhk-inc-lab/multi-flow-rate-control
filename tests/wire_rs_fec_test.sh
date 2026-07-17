#!/usr/bin/env sh

set -eu

bin=${1:?usage: wire_rs_fec_test.sh path-to-wg_multi_pipeline}
base=${2:-build}
full_port=21905
one_loss_port=21906
two_loss_port=21907
partial_port=21908
input="$base/wire_rs_input.ts"

cleanup() {
    [ -n "${receiver_pid:-}" ] && kill "$receiver_pid" 2>/dev/null || true
    [ -n "${proxy_pid:-}" ] && kill "$proxy_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

python3 - "$input" <<'PY'
from pathlib import Path
import sys
Path(sys.argv[1]).write_bytes(b"A" * 188 + b"B" * 188 +
                              b"C" * 188 + b"D" * 188)
PY

start_receiver() {
    port=$1
    output=$2
    shift 2
    "$bin" --codec rs-fec --udp-recv "$port" "$output" \
        --idle-sec 1 "$@" >"$output.log" 2>&1 &
    receiver_pid=$!
    sleep 1
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
    if len(packet) < 28:
        continue
    _, version, packet_type, _, _, _, shard_index, _, _, _ = \
        struct.unpack("!IBBHIQHHHH", packet[:28])
    if version != 1:
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
    "$bin" --codec rs-fec --udp-send 127.0.0.1 "$port" "$input"
}

run_full() {
    output="$base/wire_rs_full.ts"
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
    output="$base/wire_rs_strict_incomplete.ts"

    start_receiver "$partial_port" "$output"
    start_drop_proxy 21918 "$partial_port" "1,3,4"
    run_sender 21918
    wait "$proxy_pid"
    proxy_pid=
    if wait "$receiver_pid"; then
        echo "strict RS receiver unexpectedly accepted an incomplete group" >&2
        exit 1
    fi
    receiver_pid=
    rg -q "incomplete transfer" "$output.log"
}

run_best_effort() {
    output="$base/wire_rs_partial.ts"
    expected="$base/wire_rs_partial_expected.ts"

    python3 - "$expected" <<'PY'
from pathlib import Path
import sys
Path(sys.argv[1]).write_bytes(b"A" * 188 + b"C" * 188)
PY
    start_receiver "$partial_port" "$output" --best-effort
    start_drop_proxy 21919 "$partial_port" "1,3,4"
    run_sender 21919
    wait "$proxy_pid"
    proxy_pid=
    wait "$receiver_pid"
    receiver_pid=
    cmp "$expected" "$output"
    rg -q "missing_data_shards=2" "$output.log"
}

run_full
run_recovery 21916 "$one_loss_port" "2" "$base/wire_rs_one_loss.ts"
run_recovery 21917 "$two_loss_port" "1,4" "$base/wire_rs_two_loss.ts"
run_strict_failure
run_best_effort

echo "wire RS FEC full, recovery, strict, and best-effort tests passed"
