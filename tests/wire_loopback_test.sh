#!/usr/bin/env sh

set -eu

bin=${1:?usage: wire_loopback_test.sh path-to-wg_multi_pipeline}
base=${2:-build}
input="$base/wire_loopback_input.ts"
copy_output="$base/wire_loopback_copy.ts"
block_output="$base/wire_loopback_block.ts"
copy_port=21901
block_port=21902

cleanup() {
    [ -n "${receiver_pid:-}" ] && kill "$receiver_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

dd if=/dev/urandom of="$input" bs=188 count=40 status=none
dd if=/dev/urandom bs=1 count=96 status=none >> "$input"

run_case() {
    codec=$1
    port=$2
    output=$3

    "$bin" --codec "$codec" --udp-recv "$port" "$output" --idle-sec 1 \
        >"$base/wire_${codec}_receiver.log" 2>&1 &
    receiver_pid=$!
    sleep 1
    "$bin" --codec "$codec" --udp-send 127.0.0.1 "$port" "$input" \
        >"$base/wire_${codec}_sender.log" 2>&1
    wait "$receiver_pid"
    receiver_pid=
    cmp "$input" "$output"
}

run_case copy "$copy_port" "$copy_output"
run_case block "$block_port" "$block_output"

echo "wire loopback tests passed"
