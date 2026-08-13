#!/usr/bin/env sh

set -eu

bin=${1:?usage: wire_loopback_test.sh path-to-wg_multi_pipeline}
base=${2:-build}
input="$base/wire_loopback_input.ts"
copy_output="$base/wire_loopback_copy.ts"
block_output="$base/wire_loopback_block.ts"
copy_port=21901
block_port=21902
ready_timeout_sec=${READY_TIMEOUT_SEC:-5}

cleanup() {
    [ -n "${receiver_pid:-}" ] && kill "$receiver_pid" 2>/dev/null || true
    receiver_pid=
}
trap cleanup EXIT INT TERM

wait_receiver_ready() {
    log=$1
    deadline=$(($(date +%s) + ready_timeout_sec))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if [ -f "$log" ] && grep -q 'udp-recv: listening on UDP port' "$log" 2>/dev/null; then
            return 0
        fi
        # Receiver already exited before ready — fail fast.
        if [ -n "${receiver_pid:-}" ] && ! kill -0 "$receiver_pid" 2>/dev/null; then
            echo "error: receiver exited before ready (log=$log)" >&2
            [ -f "$log" ] && cat "$log" >&2
            return 1
        fi
        sleep 0.05
    done
    echo "error: timed out after ${ready_timeout_sec}s waiting for receiver ready (log=$log)" >&2
    [ -f "$log" ] && cat "$log" >&2
    cleanup
    return 1
}

dd if=/dev/urandom of="$input" bs=1400 count=40 status=none
dd if=/dev/urandom bs=1 count=96 status=none >> "$input"

run_case() {
    codec=$1
    port=$2
    output=$3
    rlog="$base/wire_${codec}_receiver.log"
    slog="$base/wire_${codec}_sender.log"

    # Truncate per-codec logs so ready wait never matches a previous run.
    : >"$rlog"
    : >"$slog"

    # idle-sec=3: ready-log wait already guarantees bind; 3s only avoids false
    # idle timeouts under test scheduling / sender startup. Does not change
    # transport, decoder, cmp, or hash success criteria.
    "$bin" --codec "$codec" --udp-recv "$port" "$output" --idle-sec 3 \
        >"$rlog" 2>&1 &
    receiver_pid=$!

    wait_receiver_ready "$rlog" || return 1
    # Brief post-ready buffer only; not the sync mechanism.
    sleep 0.05

    "$bin" --codec "$codec" --udp-send 127.0.0.1 "$port" "$input" \
        >"$slog" 2>&1
    wait "$receiver_pid"
    receiver_pid=
    cmp "$input" "$output"
    grep -q '^latency end_to_end: samples=[1-9]' "$rlog"
    grep -q '^latency end_to_end_jitter: samples=[1-9]' "$rlog"
}

run_case copy "$copy_port" "$copy_output"
run_case block "$block_port" "$block_output"

echo "wire loopback tests passed"
