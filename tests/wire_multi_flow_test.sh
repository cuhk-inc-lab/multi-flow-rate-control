#!/usr/bin/env sh

set -eu

bin=${1:?usage: wire_multi_flow_test.sh path-to-wg_multi_pipeline}
base=${2:-build}
input0="$base/wire_multi_input0.ts"
input1="$base/wire_multi_input1.ts"
prefix="$base/wire_multi_out_"
port=21910

rm -f "${prefix}"src_*_flow_*.ts

cleanup() {
    [ -n "${receiver_pid:-}" ] && kill "$receiver_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

dd if=/dev/urandom of="$input0" bs=188 count=20 status=none
dd if=/dev/urandom of="$input1" bs=752 count=5 status=none

"$bin" --codec copy --udp-recv "$port" "$prefix" --max-flows 2 --idle-sec 2 \
    >"$base/wire_multi_receiver.log" 2>&1 &
receiver_pid=$!
sleep 1

"$bin" --codec copy --udp-send-multi \
    --flow "0:127.0.0.1:$port:$input0" \
    --flow "1:127.0.0.1:$port:$input1" \
    >"$base/wire_multi_sender.log" 2>&1

wait "$receiver_pid"
receiver_pid=

out0=$(ls "${prefix}"src_*_flow_0.ts 2>/dev/null | head -n 1)
out1=$(ls "${prefix}"src_*_flow_1.ts 2>/dev/null | head -n 1)
test -n "$out0" && test -n "$out1"
cmp "$input0" "$out0"
cmp "$input1" "$out1"
grep -q 'flow 0' "$base/wire_multi_receiver.log"
grep -q 'flow 1' "$base/wire_multi_receiver.log"

echo "wire multi-flow tests passed"
