#!/usr/bin/env sh
# Standalone multi-flow check: one sender process, multiple input files.
#
# Local loopback (default):
#   ./scripts/check_wire_multi_flow.sh
#
# Cross-VM to Node4:
#   ./scripts/check_wire_multi_flow.sh --remote fyp1@10.10.10.164 10.10.34.2

set -eu

remote_ssh=
remote_ip=127.0.0.1
port=${PORT:-19091}
codec=${CODEC:-copy}
repo=${REMOTE_REPO:-$HOME/work/multi-flow-rate-control}
ssh_opts="-o BatchMode=yes -o ConnectTimeout=10"

if [ "${1:-}" = "--remote" ]; then
    remote_ssh=${2:?usage: $0 --remote SSH_HOST DATA_IP}
    remote_ip=${3:?usage: $0 --remote SSH_HOST DATA_IP}
fi

bin=./build/wg_multi_pipeline
test -x "$bin" || { echo "error: missing $bin (run make wg-demo)" >&2; exit 1; }

base=build/mf-check-$(date +%Y%m%d-%H%M%S)
mkdir -p "$base"
dd if=/dev/urandom of="$base/a.ts" bs=188 count=2000 status=none
dd if=/dev/urandom of="$base/b.ts" bs=188 count=3000 status=none
dd if=/dev/urandom of="$base/c.ts" bs=188 count=1500 status=none

sha_a=$(sha256sum "$base/a.ts" | awk '{print $1}')
sha_b=$(sha256sum "$base/b.ts" | awk '{print $1}')
sha_c=$(sha256sum "$base/c.ts" | awk '{print $1}')

start_local_recv() {
    "$bin" --codec "$codec" --udp-recv "$port" "$base/out_" \
        --max-flows 4 --idle-sec 8 >"$base/recv.log" 2>&1 &
    echo $!
}

start_remote_recv() {
    # Avoid `pkill -f wg_multi_pipeline` over SSH (kills the SSH session).
    # Start via a tiny remote script so $! is not eaten by nested ssh quoting.
    # shellcheck disable=SC2086
    ssh $ssh_opts "$remote_ssh" "fuser -k ${port}/udp >/dev/null 2>&1 || true"
    # shellcheck disable=SC2086
    ssh $ssh_opts "$remote_ssh" "cd '$repo' && rm -rf '$base' && mkdir -p '$base'"
    cat >"$base/start_recv.sh" <<EOF
#!/bin/sh
cd $repo
nohup $bin --codec $codec --udp-recv $port $base/out_ --max-flows 4 --idle-sec 8 \\
  >$base/recv.log 2>&1 </dev/null &
echo \$! >$base/recv.pid
EOF
    # shellcheck disable=SC2086
    scp $ssh_opts "$base/start_recv.sh" "$remote_ssh:$repo/$base/start_recv.sh" >/dev/null
    # shellcheck disable=SC2086
    ssh $ssh_opts "$remote_ssh" "sh '$repo/$base/start_recv.sh'"
    sleep 0.3
    # shellcheck disable=SC2086
    ssh $ssh_opts "$remote_ssh" "tr -d ' \n' < '$repo/$base/recv.pid'"
}

echo "== multi-flow check =="
echo "  codec=$codec port=$port dst=$remote_ip"
echo "  files: a.ts b.ts c.ts under $base"

if [ -n "$remote_ssh" ]; then
    recv_pid=$(start_remote_recv)
    echo "  remote receiver pid=$recv_pid on $remote_ssh"
else
    recv_pid=$(start_local_recv)
    echo "  local receiver pid=$recv_pid"
fi
sleep 0.5

set +e
"$bin" --codec "$codec" --udp-send-multi \
    --flow "${remote_ip}:${port}:${base}/a.ts:20" \
    --flow "${remote_ip}:${port}:${base}/b.ts:30" \
    --flow "${remote_ip}:${port}:${base}/c.ts:15" \
    >"$base/send.log" 2>&1
send_rc=$?
set -e
echo "  send_exit=$send_rc"
cat "$base/send.log"

if [ -n "$remote_ssh" ]; then
    i=0
    while [ "$i" -lt 50 ]; do
        # shellcheck disable=SC2086
        if ! ssh $ssh_opts "$remote_ssh" "kill -0 $recv_pid 2>/dev/null"; then
            break
        fi
        sleep 0.4
        i=$((i + 1))
    done
    # shellcheck disable=SC2086
    ssh $ssh_opts "$remote_ssh" "kill $recv_pid 2>/dev/null || true" || true
    mkdir -p "$base/fetched"
    # shellcheck disable=SC2086
    scp $ssh_opts "$remote_ssh:$repo/$base/recv.log" "$base/fetched/" >/dev/null
    # shellcheck disable=SC2086
    scp $ssh_opts "$remote_ssh:$repo/$base/out_"* "$base/fetched/" >/dev/null 2>&1 || true
    out_dir=$base/fetched
    recv_log=$base/fetched/recv.log
else
    wait "$recv_pid" || true
    out_dir=$base
    recv_log=$base/recv.log
fi

echo "== receiver =="
grep -E 'opened flow|output_bytes|incomplete|idle' "$recv_log" || cat "$recv_log"

ok=0
for pair in "a:$sha_a" "b:$sha_b" "c:$sha_c"; do
    tag=${pair%%:*}
    want=${pair#*:}
    matched=0
    for f in "$out_dir"/out_src_*_flow_*.ts "$out_dir"/out_*; do
        [ -f "$f" ] || continue
        h=$(sha256sum "$f" | awk '{print $1}')
        if [ "$h" = "$want" ]; then
            echo "  PASS $tag -> $(basename "$f")"
            matched=1
            ok=$((ok + 1))
            break
        fi
    done
    if [ "$matched" -eq 0 ]; then
        echo "  FAIL $tag"
    fi
done

echo "== result: $ok / 3 (send_exit=$send_rc) =="
test "$ok" -eq 3
test "$send_rc" -eq 0
echo "multi-flow check PASSED"
