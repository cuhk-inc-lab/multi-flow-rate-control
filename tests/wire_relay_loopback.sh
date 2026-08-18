#!/usr/bin/env sh

set -eu

wg_bin=${1:?usage: wire_relay_loopback.sh path-to-wg_multi_pipeline path-to-wire_relay [build_dir]}
relay_bin=${2:?usage: wire_relay_loopback.sh path-to-wg_multi_pipeline path-to-wire_relay [build_dir]}
base=${3:-build}
input="$base/wire_relay_loopback_input.ts"
output="$base/wire_relay_loopback_output.ts"
block_output="$base/wire_relay_loopback_block_output.ts"
p2=22902
p3=22903
p4=22904

cleanup() {
    [ -n "${recv_pid:-}" ] && kill "$recv_pid" 2>/dev/null || true
    [ -n "${r3_pid:-}" ] && kill "$r3_pid" 2>/dev/null || true
    [ -n "${r2_pid:-}" ] && kill "$r2_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

dd if=/dev/urandom of="$input" bs=1400 count=20 status=none
dd if=/dev/urandom bs=1 count=96 status=none >> "$input"
rm -f "$output" "$block_output"

"$wg_bin" --codec copy --local-node-id 4 \
    --udp-recv "$p4" "$output" --idle-sec 2 \
    >"$base/wire_relay_loopback_recv.log" 2>&1 &
recv_pid=$!

"$relay_bin" --local-node-id 3 --listen "$p3" \
    --next-hop "127.0.0.1:$p4" --idle-exit-sec 8 \
    >"$base/wire_relay_loopback_r3.log" 2>&1 &
r3_pid=$!

"$relay_bin" --local-node-id 2 --listen "$p2" \
    --next-hop "127.0.0.1:$p3" --idle-exit-sec 8 \
    >"$base/wire_relay_loopback_r2.log" 2>&1 &
r2_pid=$!

sleep 1

"$wg_bin" --codec copy --final-dst 4 --ttl 8 \
    --udp-send 127.0.0.1 "$p2" "$input" \
    >"$base/wire_relay_loopback_send.log" 2>&1

wait "$recv_pid"
recv_pid=

# Stop relays after the receiver has completed so the test need not wait idle.
kill "$r3_pid" "$r2_pid" 2>/dev/null || true
wait "$r3_pid" 2>/dev/null || true
wait "$r2_pid" 2>/dev/null || true
r3_pid=
r2_pid=

cmp "$input" "$output"
grep -q 'forward=' "$base/wire_relay_loopback_r2.log"
grep -q 'forward=' "$base/wire_relay_loopback_r3.log"

# Case c: block codec at endpoints and +1/-1 at each relay.
p2=$((p2 + 10))
p3=$((p3 + 10))
p4=$((p4 + 10))

"$wg_bin" --codec block --local-node-id 4 \
    --udp-recv "$p4" "$block_output" --idle-sec 2 \
    >"$base/wire_relay_loopback_block_recv.log" 2>&1 &
recv_pid=$!

"$relay_bin" --local-node-id 3 --listen "$p3" \
    --next-hop "127.0.0.1:$p4" --idle-exit-sec 8 \
    --transit-hook plus-minus \
    >"$base/wire_relay_loopback_block_r3.log" 2>&1 &
r3_pid=$!

"$relay_bin" --local-node-id 2 --listen "$p2" \
    --next-hop "127.0.0.1:$p3" --idle-exit-sec 8 \
    --transit-hook plus-minus \
    >"$base/wire_relay_loopback_block_r2.log" 2>&1 &
r2_pid=$!

sleep 1

"$wg_bin" --codec block --final-dst 4 --ttl 8 \
    --udp-send 127.0.0.1 "$p2" "$input" \
    >"$base/wire_relay_loopback_block_send.log" 2>&1

wait "$recv_pid"
recv_pid=
kill "$r3_pid" "$r2_pid" 2>/dev/null || true
wait "$r3_pid" 2>/dev/null || true
wait "$r2_pid" 2>/dev/null || true
r3_pid=
r2_pid=

cmp "$input" "$block_output"
grep -q 'forward=' "$base/wire_relay_loopback_block_r2.log"
grep -q 'forward=' "$base/wire_relay_loopback_block_r3.log"

echo "wire relay copy and block plus-minus loopback tests passed"
