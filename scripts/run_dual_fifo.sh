#!/usr/bin/env bash
# Three-stream FIFO live demo:
#   input_1m.ts / input_10m.ts / input_20m.ts through wg_multi_pipeline.
# Opens three ffplay windows; pushes with ffmpeg -re (realtime).
# Usage: ./scripts/run_dual_fifo.sh [dir_with_input_*m.ts]
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="${1:-$REPO}"
BIN="$REPO/build/wg_multi_pipeline"

IN0=/tmp/live_in0_1m.ts
IN1=/tmp/live_in1_10m.ts
IN2=/tmp/live_in2_20m.ts
OUT0=/tmp/live_out0_1m.ts
OUT1=/tmp/live_out1_10m.ts
OUT2=/tmp/live_out2_20m.ts

SRC_1M="$SRC_DIR/input_1m.ts"
SRC_10M="$SRC_DIR/input_10m.ts"
SRC_20M="$SRC_DIR/input_20m.ts"

if [[ ! -x "$BIN" ]]; then
    echo "Build first: cd $REPO && make wg-demo" >&2
    exit 1
fi

if [[ ! -f "$SRC_1M" || ! -f "$SRC_10M" || ! -f "$SRC_20M" ]]; then
    echo "Need $SRC_1M, $SRC_10M, and $SRC_20M" >&2
    exit 1
fi

cleanup() {
    kill "$PIPE_PID" \
         "$FF0_PID" "$FF1_PID" "$FF2_PID" \
         "$FFM0_PID" "$FFM1_PID" "$FFM2_PID" 2>/dev/null || true
    killall -9 ffmpeg ffplay wg_multi_pipeline 2>/dev/null || true
}
trap cleanup EXIT INT TERM

killall -9 ffmpeg ffplay wg_multi_pipeline 2>/dev/null || true
sleep 0.5
rm -f "$IN0" "$IN1" "$IN2" "$OUT0" "$OUT1" "$OUT2"
mkfifo "$IN0" "$IN1" "$IN2" "$OUT0" "$OUT1" "$OUT2"

# -an: avoid audio-device contention so all three windows stay up.
# -left/-top: separate visible windows.
echo "=== 1/4 ffplay (3 windows, video-only) ==="
ffplay -loglevel warning -an -noborder \
    -window_title "1Mbps" -left 40 -top 40 -x 640 -y 360 \
    -f mpegts "$OUT0" &
FF0_PID=$!
sleep 0.3
ffplay -loglevel warning -an -noborder \
    -window_title "10Mbps" -left 720 -top 40 -x 640 -y 360 \
    -f mpegts "$OUT1" &
FF1_PID=$!
sleep 0.3
ffplay -loglevel warning -an -noborder \
    -window_title "20Mbps" -left 40 -top 440 -x 640 -y 360 \
    -f mpegts "$OUT2" &
FF2_PID=$!
sleep 1

echo "=== 2/4 pipeline --multi ==="
"$BIN" --multi \
    "$IN0" "$OUT0" \
    "$IN1" "$OUT1" \
    "$IN2" "$OUT2" &
PIPE_PID=$!
sleep 1

echo "=== 3/4 ffmpeg -re push (all three) ==="
ffmpeg -loglevel warning -re -i "$SRC_1M"  -c copy -f mpegts -y "$IN0" &
FFM0_PID=$!
ffmpeg -loglevel warning -re -i "$SRC_10M" -c copy -f mpegts -y "$IN1" &
FFM1_PID=$!
ffmpeg -loglevel warning -re -i "$SRC_20M" -c copy -f mpegts -y "$IN2" &
FFM2_PID=$!

echo "=== 4/4 streaming… (Ctrl+C to stop) ==="
echo "Windows: 1Mbps / 10Mbps / 20Mbps"
wait "$FFM0_PID" "$FFM1_PID" "$FFM2_PID" || true
wait "$PIPE_PID" || true

echo "Done."
