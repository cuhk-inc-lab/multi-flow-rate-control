#!/usr/bin/env bash
# Encode one source into input_1m.ts / input_10m.ts / input_20m.ts (720p, same duration).
# Usage: ./scripts/encode_multibitrate.sh [source.ts_or_mp4] [out_dir]
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${1:-$REPO/input2.ts}"
OUT_DIR="${2:-$REPO}"
DURATION="${DURATION:-}"

if [[ ! -f "$SRC" ]]; then
    echo "Need source: $SRC" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
DUR_ARGS=()
if [[ -n "$DURATION" ]]; then
    DUR_ARGS=(-t "$DURATION")
fi

encode_one() {
    local br="$1" out="$2" buf="$3"
    echo "Encoding $out @ $br ..."
    ffmpeg -y -loglevel error -stats -i "$SRC" "${DUR_ARGS[@]}" \
        -vf "scale=-2:720" \
        -c:v libx264 -preset fast -b:v "$br" -maxrate "$br" -bufsize "$buf" \
        -c:a aac -b:a 128k \
        -f mpegts "$out"
}

encode_one 1M  "$OUT_DIR/input_1m.ts"  2M
encode_one 10M "$OUT_DIR/input_10m.ts" 20M
encode_one 20M "$OUT_DIR/input_20m.ts" 40M

echo "Done. Probe:"
for f in "$OUT_DIR/input_1m.ts" "$OUT_DIR/input_10m.ts" "$OUT_DIR/input_20m.ts"; do
    ffprobe -v error -select_streams v:0 \
        -show_entries stream=width,height -show_entries format=duration,bit_rate \
        -of default=nw=1:nk=1 "$f" | tr '\n' ' '
    echo "  $(basename "$f")"
done
