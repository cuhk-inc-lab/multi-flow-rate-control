#!/usr/bin/env bash
# Three-stream FIFO live demo — 1M / 10M / 20M through one --multi process.
# Requires 720p input_1m/10m/20m.ts (see scripts/encode_multibitrate.sh).
#
# Pipeline: --no-pace --codec <method> --multi. ffmpeg -re provides pacing.
#
# Usage: ./scripts/run_dual_fifo.sh [--codec block|copy|xor-fec|rs-fec|none] [dir_with_input_*m.ts]
# Close one window → others keep playing. Close all three → demo exits.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="$REPO"
SRC_DIR_SET=0
CODEC="block"
BIN="$REPO/build/wg_multi_pipeline"

usage() {
    cat <<EOF
Usage: $0 [--codec block|copy|xor-fec|rs-fec|none] [dir_with_input_*m.ts]

  block    Existing reversible +/- BlockCodec demo transform (default)
  copy     Systematic copy codec
  xor-fec  Four data TS packets plus one XOR parity packet
  rs-fec   Reed-Solomon: four data TS packets plus two parity packets
  none     Pointer-only relay; no codec
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
    --codec)
        [[ $# -ge 2 ]] || { usage >&2; exit 1; }
        CODEC="$2"
        shift 2
        ;;
    --help|-h)
        usage
        exit 0
        ;;
    -*)
        echo "Unknown option: $1" >&2
        usage >&2
        exit 1
        ;;
    *)
        if [[ "$SRC_DIR_SET" -ne 0 ]]; then
            echo "Only one input directory may be supplied." >&2
            usage >&2
            exit 1
        fi
        SRC_DIR="$1"
        SRC_DIR_SET=1
        shift
        ;;
    esac
done

case "$CODEC" in
block|copy|xor-fec|rs-fec|none) ;;
*)
    echo "Unsupported codec: $CODEC" >&2
    usage >&2
    exit 1
    ;;
esac

IN0=/tmp/live_in0.ts
IN1=/tmp/live_in1.ts
IN2=/tmp/live_in2.ts
OUT0=/tmp/live_out0.ts
OUT1=/tmp/live_out1.ts
OUT2=/tmp/live_out2.ts

if [[ ! -x "$BIN" ]]; then
    echo "Build first: cd $REPO && make wg-demo" >&2
    exit 1
fi

if [[ ! -f "$SRC_DIR/input_1m.ts" || ! -f "$SRC_DIR/input_10m.ts" || ! -f "$SRC_DIR/input_20m.ts" ]]; then
    echo "Need $SRC_DIR/input_{1,10,20}m.ts (encode with scripts/encode_multibitrate.sh)" >&2
    exit 1
fi

cleanup() {
    kill "$FFM0_PID" "$FFM1_PID" "$FFM2_PID" 2>/dev/null || true
    kill "$PIPE_PID" 2>/dev/null || true
    kill "$FF0_PID" "$FF1_PID" "$FF2_PID" 2>/dev/null || true
    killall -9 ffmpeg ffplay wg_multi_pipeline 2>/dev/null || true
}
trap cleanup EXIT INT TERM

killall -9 ffmpeg ffplay wg_multi_pipeline 2>/dev/null || true
rm -f "$IN0" "$IN1" "$IN2" "$OUT0" "$OUT1" "$OUT2"
mkfifo "$IN0" "$IN1" "$IN2" "$OUT0" "$OUT1" "$OUT2"

echo "=== 1/4 ffplay (close one = others continue; close all = exit) ==="
ffplay -loglevel quiet -an \
    -fflags nobuffer+discardcorrupt -flags low_delay -framedrop \
    -window_title "1Mbps"  -f mpegts "$OUT0" >/dev/null 2>&1 &
FF0_PID=$!
ffplay -loglevel quiet -an \
    -fflags nobuffer+discardcorrupt -flags low_delay -framedrop \
    -window_title "10Mbps" -f mpegts "$OUT1" >/dev/null 2>&1 &
FF1_PID=$!
ffplay -loglevel quiet -an \
    -fflags nobuffer+discardcorrupt -flags low_delay -framedrop \
    -window_title "20Mbps" -f mpegts "$OUT2" >/dev/null 2>&1 &
FF2_PID=$!
sleep 1

echo "=== 2/4 pipeline --multi (background, codec: $CODEC) ==="
"$BIN" --no-pace --codec "$CODEC" --multi \
    "$IN0" "$OUT0" \
    "$IN1" "$OUT1" \
    "$IN2" "$OUT2" &
PIPE_PID=$!
sleep 1

echo "=== 3/4 ffmpeg push (all three) ==="
ffmpeg -loglevel quiet -re -i "$SRC_DIR/input_1m.ts"  -c copy -f mpegts -y "$IN0" >/dev/null 2>&1 &
FFM0_PID=$!
ffmpeg -loglevel quiet -re -i "$SRC_DIR/input_10m.ts" -c copy -f mpegts -y "$IN1" >/dev/null 2>&1 &
FFM1_PID=$!
ffmpeg -loglevel quiet -re -i "$SRC_DIR/input_20m.ts" -c copy -f mpegts -y "$IN2" >/dev/null 2>&1 &
FFM2_PID=$!

echo "=== 4/4 streaming… (Ctrl+C or close all windows to stop) ==="
echo "Path: multi-flow split → $CODEC encode/decode → three ffplay windows."

# Exit when all players are gone, or all pushers finished (natural end).
while true; do
    viewers=0
    kill -0 "$FF0_PID" 2>/dev/null && viewers=$((viewers + 1))
    kill -0 "$FF1_PID" 2>/dev/null && viewers=$((viewers + 1))
    kill -0 "$FF2_PID" 2>/dev/null && viewers=$((viewers + 1))

    if [[ "$viewers" -eq 0 ]]; then
        echo "All player windows closed — shutting down."
        break
    fi

    pushers=0
    kill -0 "$FFM0_PID" 2>/dev/null && pushers=$((pushers + 1))
    kill -0 "$FFM1_PID" 2>/dev/null && pushers=$((pushers + 1))
    kill -0 "$FFM2_PID" 2>/dev/null && pushers=$((pushers + 1))

    if [[ "$pushers" -eq 0 ]]; then
        echo "All streams finished."
        break
    fi

    # Pipeline exited on its own (e.g. all outputs dead).
    if ! kill -0 "$PIPE_PID" 2>/dev/null; then
        echo "Pipeline exited."
        break
    fi

    sleep 0.3
done

echo "Done."
