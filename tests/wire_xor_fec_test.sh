#!/usr/bin/env sh

set -eu

bin=${1:?usage: wire_xor_fec_test.sh path-to-wg_multi_pipeline}
base=${2:-build}
recover_port=21903
partial_port=21904
recover_output="$base/wire_xor_recovered.ts"
partial_output="$base/wire_xor_partial.ts"

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

    python3 - "$port" "$mode" <<'PY'
import socket
import struct
import sys

port = int(sys.argv[1])
mode = sys.argv[2]
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
target = ("127.0.0.1", port)

def header(kind, block, index, valid, payload):
    return struct.pack("!IBBHIQHHHHQQ",
                       0x57475031, 2, kind, 0, 0, block,
                       index, 5, valid, payload, 0, 0)

if mode == "recover":
    shards = ((0, b"A" * 188), (1, b"B" * 188),
              (3, b"D" * 188), (4, b"\x04" * 188))
elif mode == "partial":
    shards = ((0, b"A" * 188), (2, b"C" * 188))
else:
    raise SystemExit("unknown mode")

for index, payload in shards:
    sock.sendto(header(1, 0, index, 752, 188) + payload, target)
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
python3 - "$base/wire_xor_expected_full.ts" <<'PY'
from pathlib import Path
import sys
Path(sys.argv[1]).write_bytes(b"A" * 188 + b"B" * 188 +
                              b"C" * 188 + b"D" * 188)
PY
assert_output "$recover_output" "$base/wire_xor_expected_full.ts"

start_receiver "$partial_port" "$partial_output"
send_group "$partial_port" partial
wait "$receiver_pid"
receiver_pid=
python3 - "$base/wire_xor_expected_partial.ts" <<'PY'
from pathlib import Path
import sys
Path(sys.argv[1]).write_bytes(b"A" * 188 + b"C" * 188)
PY
assert_output "$partial_output" "$base/wire_xor_expected_partial.ts"

echo "wire XOR FEC recovery and best-effort tests passed"
