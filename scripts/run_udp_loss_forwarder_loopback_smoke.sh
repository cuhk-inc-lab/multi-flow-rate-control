#!/usr/bin/env sh
# Smoke test: udp_loss_forwarder on loopback with independent per-datagram loss.
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
forwarder_py="$script_dir/udp_loss_forwarder.py"
listen_port=${LISTEN_PORT:-19201}
forward_port=${FORWARD_PORT:-19200}
loss=${LOSS:-0.10}
seed=${SEED:-42}
count=${COUNT:-5000}
summary_json=${SUMMARY_JSON:-/tmp/udp-loss-forwarder-smoke.json}

if [ ! -f "$forwarder_py" ]; then
    echo "error: missing $forwarder_py" >&2
    exit 1
fi

python3 - "$forward_port" <<'PY' &
import socket
import sys

port = int(sys.argv[1])
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 * 1024 * 1024)
sock.bind(("127.0.0.1", port))
while True:
    sock.recvfrom(65535)
PY
recv_pid=$!

python3 "$forwarder_py" \
    --listen-host 127.0.0.1 --listen-port "$listen_port" \
    --forward-host 127.0.0.1 --forward-port "$forward_port" \
    --loss "$loss" --seed "$seed" \
    --summary-json "$summary_json" &
fwd_pid=$!
sleep 0.5

python3 - "$listen_port" "$count" <<'PY'
import socket
import sys
import time

port = int(sys.argv[1])
count = int(sys.argv[2])
payload = b"x" * 1400
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
for i in range(count):
    sock.sendto(payload, ("127.0.0.1", port))
    if i % 200 == 0:
        time.sleep(0.001)
PY

sleep 0.5
kill "$fwd_pid" 2>/dev/null || true
wait "$fwd_pid" 2>/dev/null || true
kill "$recv_pid" 2>/dev/null || true
wait "$recv_pid" 2>/dev/null || true

python3 - "$summary_json" "$loss" <<'PY'
import json
import math
import sys

path, target_loss = sys.argv[1], float(sys.argv[2])
with open(path, encoding="utf-8") as fh:
    summary = json.load(fh)
recv = summary["received"]
fwd = summary["forwarded"]
drop = summary["dropped"]
actual = summary["actual_loss_pct"] / 100.0
if recv <= 0:
    raise SystemExit("FAIL: forwarder received 0 datagrams")
if fwd + drop != recv:
    raise SystemExit(f"FAIL: fwd+drop={fwd+drop} != recv={recv}")
# Binomial tolerance on observed drop rate.
sigma = math.sqrt(max(recv * target_loss * (1 - target_loss), 1.0))
tol = max(3.0 * sigma / recv, 0.02)
if abs(actual - target_loss) > tol:
    raise SystemExit(
        f"FAIL: actual_loss={actual:.4f} target={target_loss:.4f} tol={tol:.4f} recv={recv}"
    )
print(
    f"PASS: recv={recv} fwd={fwd} drop={drop} "
    f"configured_loss={target_loss:.2%} actual_loss={actual:.2%}"
)
PY
