#!/usr/bin/env bash
# Dual-stream FIFO live demo: input1.ts + input2.ts through wg_multi_pipeline.
# Usage: ./scripts/run_dual_fifo.sh [dir_with_input1.ts_input2.ts]
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="${1:-$REPO}"
BIN="$REPO/build/wg_multi_pipeline"

IN0=/tmp/live_in0.ts
IN1=/tmp/live_in1.ts
OUT0=/tmp/live_out0.ts
OUT1=/tmp/live_out1.ts

if [[ ! -x "$BIN" ]]; then
    echo "Build first: cd $REPO && make wg-demo" >&2
    exit 1
fi

if [[ ! -f "$SRC_DIR/input1.ts" || ! -f "$SRC_DIR/input2.ts" ]]; then
    echo "Need $SRC_DIR/input1.ts and $SRC_DIR/input2.ts" >&2
    exit 1
fi

cleanup() {
    kill "$PIPE_PID" "$FF0_PID" "$FF1_PID" "$FFM0_PID" "$FFM1_PID" 2>/dev/null || true
    killall -9 ffmpeg ffplay 2>/dev/null || true
}
trap cleanup EXIT INT TERM

killall -9 ffmpeg ffplay wg_multi_pipeline 2>/dev/null || true
rm -f "$IN0" "$IN1" "$OUT0" "$OUT1"
mkfifo "$IN0" "$IN1" "$OUT0" "$OUT1"

echo "=== 1/4 ffplay output FIFOs (background) ==="
ffplay -loglevel error -f mpegts "$OUT0" &
FF0_PID=$!
ffplay -loglevel error -f mpegts "$OUT1" &
FF1_PID=$!
sleep 1

echo "=== 2/4 pipeline --multi (background) ==="
"$BIN" --multi "$IN0" "$OUT0" "$IN1" "$OUT1" &
PIPE_PID=$!
sleep 1

echo "=== 3/4 ffmpeg push (pipeline must already be waiting on inputs) ==="
ffmpeg -loglevel error -re -i "$SRC_DIR/input1.ts" -c copy -f mpegts -y "$IN0" &
FFM0_PID=$!
sleep 1
ffmpeg -loglevel error -re -i "$SRC_DIR/input2.ts" -c copy -f mpegts -y "$IN1" &
FFM1_PID=$!

echo "=== 4/4 streaming… (Ctrl+C to stop) ==="
echo "Video windows: two ffplay instances."
wait "$FFM0_PID" "$FFM1_PID" || true
wait "$PIPE_PID" || true

echo "Done."
