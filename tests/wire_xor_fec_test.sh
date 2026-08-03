#!/usr/bin/env sh

set -eu

bin=${1:?usage: wire_xor_fec_test.sh path-to-wg_multi_pipeline}
base=${2:-build}
recover_port=21903
partial_port=21904
recover_output="$base/wire_xor_recovered.ts"
partial_output="$base/wire_xor_partial.ts"
PKG=1400
VALID=$((PKG * 4))

cleanup() {
    [ -n "${receiver_pid:-}" ] && kill "$receiver_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

start_receiver() {
    port=$1
    output=$2

    "$bin" --codec xor-fec --udp-recv "$port" "$output" \
        --idle-sec 1 --best-effort >"$output.log" 2>&1 &
    receiver_pid=$!
    sleep 1
}

send_group() {
    port=$1
    mode=$2

    python3 - "$port" "$mode" "$PKG" "$VALID" <<'PY'
import socket
import struct
import sys

port = int(sys.argv[1])
mode = sys.argv[2]
pkg = int(sys.argv[3])
valid = int(sys.argv[4])
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
target = ("127.0.0.1", port)

def header(kind, block, index, valid_len, payload):
    # Wire header v3: magic, version, type, final_dst, ttl, flow_id, ...
    return struct.pack("!IBBBBIQHHHHQQ",
                       0x57475031, 3, kind, 4, 8, 0, block,
                       index, 5, valid_len, payload, 0, 0)

if mode == "recover":
    shards = ((0, b"A" * pkg), (1, b"B" * pkg),
              (3, b"D" * pkg), (4, b"\x04" * pkg))
elif mode == "partial":
    shards = ((0, b"A" * pkg), (2, b"C" * pkg))
else:
    raise SystemExit("unknown mode")

for index, payload in shards:
    sock.sendto(header(1, 0, index, valid, pkg) + payload, target)
sock.sendto(header(2, 1, 0, 0, 0), target)
sock.close()
PY
}

assert_output() {
    output=$1
    expected=$2

    python3 - "$output" "$expected" <<'PY'
from pathlib import Path
import sys

output, expected = map(Path, sys.argv[1:])
if expected.read_bytes() != output.read_bytes():
    raise SystemExit(f"unexpected output: {output}")
PY
}

start_receiver "$recover_port" "$recover_output"
send_group "$recover_port" recover
wait "$receiver_pid"
receiver_pid=
python3 - "$base/wire_xor_expected_full.ts" "$PKG" <<'PY'
from pathlib import Path
import sys
pkg = int(sys.argv[2])
Path(sys.argv[1]).write_bytes(b"A" * pkg + b"B" * pkg +
                              b"C" * pkg + b"D" * pkg)
PY
assert_output "$recover_output" "$base/wire_xor_expected_full.ts"

start_receiver "$partial_port" "$partial_output"
send_group "$partial_port" partial
wait "$receiver_pid"
receiver_pid=
python3 - "$base/wire_xor_expected_partial.ts" "$PKG" <<'PY'
from pathlib import Path
import sys
pkg = int(sys.argv[2])
Path(sys.argv[1]).write_bytes(b"A" * pkg + b"C" * pkg)
PY
assert_output "$partial_output" "$base/wire_xor_expected_partial.ts"

echo "wire XOR FEC recovery and best-effort tests passed"
