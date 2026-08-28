#!/usr/bin/env sh
set -eu

bin=${1:?usage: wire_wirehair_test.sh path-to-wg_multi_pipeline [build-dir]}
base=${2:-build}
relay="$(dirname "$bin")/wire_relay"
input="$base/wire_wirehair_25m.bin"
hop_input="$base/wire_wirehair_hop.bin"
receiver_pid=
proxy_pid=
relay1_pid=
relay2_pid=

cleanup() {
    for pid in "$receiver_pid" "$proxy_pid" "$relay1_pid" "$relay2_pid"; do
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
}
trap cleanup EXIT INT TERM

python3 - "$input" "$hop_input" <<'PY'
from pathlib import Path
import sys

def write_pattern(path, size):
    block = bytes((i * 37 + i // 17 + 23) & 0xff for i in range(65536))
    with Path(path).open("wb") as f:
        while size:
            part = block[:min(size, len(block))]
            f.write(part)
            size -= len(part)

write_pattern(sys.argv[1], 25 * 1024 * 1024 + 731)
write_pattern(sys.argv[2], 28_731)
PY

wait_ready() {
    log=$1
    pattern=$2
    pid=$3
    i=0
    while [ "$i" -lt 100 ]; do
        if grep -q "$pattern" "$log" 2>/dev/null; then
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            cat "$log" >&2
            return 1
        fi
        sleep 0.05
        i=$((i + 1))
    done
    cat "$log" >&2
    return 1
}

start_receiver() {
    codec=$1
    port=$2
    output=$3
    ack=$4
    log="$output.log"
    : >"$log"
    "$bin" --codec "$codec" --wh-segment-mib=10 --wh-repair-pct=10 "$ack" \
        --local-node-id 4 --udp-recv "$port" "$output" --idle-sec 8 --strict \
        >"$log" 2>&1 &
    receiver_pid=$!
    if [ "$codec" = wirehair ]; then
        wait_ready "$log" 'wirehair-recv: listening' "$receiver_pid"
    else
        wait_ready "$log" 'udp-recv: listening' "$receiver_pid"
    fi
}

run_sender() {
    port=$1
    source=$2
    ack=$3
    log=$4
    shift 4
    "$bin" --codec wirehair --wh-segment-mib=10 --wh-repair-pct=10 "$ack" \
        --local-node-id 1 --final-dst 4 --ttl 8 --rate-mbps 100 "$@" \
        --udp-send 127.0.0.1 "$port" "$source" >"$log" 2>&1
}

full_out="$base/wire_wirehair_full.bin"
start_receiver wirehair 22120 "$full_out" --no-wh-ack
run_sender 22120 "$input" --no-wh-ack "$base/wire_wirehair_full_send.log"
wait "$receiver_pid"
receiver_pid=
cmp "$input" "$full_out"
grep -Eq 'repair_sent=[1-9][0-9]*' "$base/wire_wirehair_full_send.log"
grep -q 'wirehair-recv: flow 0 worker started queue_cap=' "$full_out.log"

python3 - 22123 22121 <<'PY' &
import select
import socket
import struct
import sys
import time

listen_port, destination_port = map(int, sys.argv[1:3])
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 * 1024 * 1024)
sock.bind(("127.0.0.1", listen_port))
deadline = time.monotonic() + 15
while time.monotonic() < deadline:
    readable, _, _ = select.select([sock], [], [], 0.1)
    if not readable:
        continue
    packet, _ = sock.recvfrom(2048)
    if len(packet) < 52 or packet[4] != 4:
        continue
    packet_type = packet[5]
    packet_id = struct.unpack("!H", packet[20:22])[0]
    if packet_type == 1 and packet_id in {7, 101, 1003}:
        continue
    sock.sendto(packet, ("127.0.0.1", destination_port))
    if packet_type == 2:
        break
sock.close()
PY
proxy_pid=$!
loss_out="$base/wire_wirehair_loss.bin"
start_receiver wirehair 22121 "$loss_out" --no-wh-ack
run_sender 22123 "$input" --no-wh-ack "$base/wire_wirehair_loss_send.log"
wait "$proxy_pid"
proxy_pid=
wait "$receiver_pid"
receiver_pid=
cmp "$input" "$loss_out"

ack_out="$base/wire_wirehair_ack.bin"
start_receiver wirehair 22124 "$ack_out" --wh-ack
run_sender 22124 "$input" --wh-ack "$base/wire_wirehair_ack_send.log"
wait "$receiver_pid"
receiver_pid=
cmp "$input" "$ack_out"
ack_repairs=$(awk -F'repair_sent=' '{split($2, a, " "); print a[1]}' \
    "$base/wire_wirehair_ack_send.log")
test "$ack_repairs" -lt 1873
grep -q 'send_window_hwm=3' "$base/wire_wirehair_ack_send.log"
grep -q 'ahead_window_drops=0 ' "$ack_out.log"

mismatch_out="$base/wire_wirehair_mismatch.bin"
start_receiver rs 22122 "$mismatch_out" --no-wh-ack
run_sender 22122 "$hop_input" --no-wh-ack \
    "$base/wire_wirehair_mismatch_send.log"
if wait "$receiver_pid"; then
    echo "v3 RS receiver unexpectedly accepted wire v4 Wirehair" >&2
    exit 1
fi
receiver_pid=
test ! -s "$mismatch_out"

if [ -x "$relay" ]; then
    hop_out="$base/wire_wirehair_hop_out.bin"
    start_receiver wirehair 22133 "$hop_out" --wh-ack
    "$relay" --local-node-id 2 --listen 22131 \
        --next-hop 127.0.0.1:22132 --return-hop 127.0.0.1:22130 \
        --idle-exit-sec 8 >"$base/wire_wirehair_relay1.log" 2>&1 &
    relay1_pid=$!
    "$relay" --local-node-id 3 --listen 22132 \
        --next-hop 127.0.0.1:22133 --return-hop 127.0.0.1:22131 \
        --idle-exit-sec 8 >"$base/wire_wirehair_relay2.log" 2>&1 &
    relay2_pid=$!
    wait_ready "$base/wire_wirehair_relay1.log" 'wire-relay: local_node_id' \
        "$relay1_pid"
    wait_ready "$base/wire_wirehair_relay2.log" 'wire-relay: local_node_id' \
        "$relay2_pid"
    run_sender 22131 "$hop_input" --wh-ack \
        "$base/wire_wirehair_hop_send.log" --ack-port=22130
    wait "$receiver_pid"
    receiver_pid=
    cmp "$hop_input" "$hop_out"
    grep -q 'repair_sent=0 ' "$base/wire_wirehair_hop_send.log"
    kill "$relay1_pid" "$relay2_pid" 2>/dev/null || true
    wait "$relay1_pid" 2>/dev/null || true
    wait "$relay2_pid" 2>/dev/null || true
    relay1_pid=
    relay2_pid=

    # Multi-flow ACK return routing: the relay learns each sender socket from
    # forward DATA, so ACKs return to ack-port-base + flow index without a
    # single static --return-hop port collapsing all flows onto flow 0.
    multi_prefix="$base/wire_wirehair_multi_"
    multi_log="$base/wire_wirehair_multi_recv.log"
    rm -f "${multi_prefix}"* "$multi_log"
    "$bin" --codec wirehair --wh-segment-mib=10 --wh-repair-pct=10 --wh-ack \
        --local-node-id 4 --udp-recv 22141 "$multi_prefix" --max-flows 2 \
        --idle-sec 8 --strict >"$multi_log" 2>&1 &
    receiver_pid=$!
    wait_ready "$multi_log" 'wirehair-recv: listening' "$receiver_pid"
    "$relay" --local-node-id 2 --listen 22140 \
        --next-hop 127.0.0.1:22141 --idle-exit-sec 8 \
        >"$base/wire_wirehair_multi_relay.log" 2>&1 &
    relay1_pid=$!
    wait_ready "$base/wire_wirehair_multi_relay.log" \
        'wire-relay: local_node_id' "$relay1_pid"
    "$bin" --codec wirehair --wh-segment-mib=10 --wh-repair-pct=10 --wh-ack \
        --ack-port=22150 --local-node-id 1 --final-dst 4 --ttl 8 \
        --udp-send-multi \
        --flow "0:127.0.0.1:22140:$hop_input:100" \
        --flow "1:127.0.0.1:22140:$hop_input:100" \
        >"$base/wire_wirehair_multi_send.log" 2>&1
    wait "$receiver_pid"
    receiver_pid=
    flow0=
    flow1=
    for candidate in "${multi_prefix}"*flow_0*; do
        if [ -f "$candidate" ]; then flow0=$candidate; break; fi
    done
    for candidate in "${multi_prefix}"*flow_1*; do
        if [ -f "$candidate" ]; then flow1=$candidate; break; fi
    done
    test -n "$flow0"
    test -n "$flow1"
    cmp "$hop_input" "$flow0"
    cmp "$hop_input" "$flow1"
    test "$(grep -c 'wirehair-recv: flow .* worker started queue_cap=' \
        "$multi_log")" -eq 2
    kill "$relay1_pid" 2>/dev/null || true
    wait "$relay1_pid" 2>/dev/null || true
    relay1_pid=
fi

echo "wire v4 segmented Wirehair loss, ACK, mismatch, and relay tests passed"
