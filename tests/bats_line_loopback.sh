#!/bin/sh
# 本机四进程：源 → 中继1 → 中继2 → 目的。
# 无丢包必须整文件还原，且目的端系数不再是单位阵（说明两跳真的 bats_recode 了）。
# 5% 丢包必须出现未收齐仍 recode 的 batch，并且仍然还原。

set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
bin=$root/build/bats_line
base=$root/build/bats_line_loopback
mkdir -p "$base"

if [ ! -x "$bin" ]; then
    echo "missing $bin; run: make -C \"$root\" bats-line" >&2
    exit 1
fi

cleanup() {
    [ -n "${src_pid:-}" ] && kill "$src_pid" 2>/dev/null || true
    [ -n "${src2_pid:-}" ] && kill "$src2_pid" 2>/dev/null || true
    [ -n "${r1_pid:-}" ] && kill "$r1_pid" 2>/dev/null || true
    [ -n "${r2_pid:-}" ] && kill "$r2_pid" 2>/dev/null || true
    [ -n "${dst_pid:-}" ] && kill "$dst_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

wait_listen() {
    log=$1
    i=0
    while [ "$i" -lt 50 ]; do
        if grep -q 'bats-line: listening' "$log" 2>/dev/null; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.05
    done
    echo "timeout waiting for $log" >&2
    return 1
}

field() {
    # field logfile key
    sed -n "s/.*$2=\\([^ ]*\\).*/\\1/p" "$1" | tail -n 1
}

run_case() {
    name=$1
    loss=$2
    bytes=$3
    k=$4
    t=$5
    in=$base/${name}.in
    out=$base/${name}.out
    p1=19102
    p2=19103
    p3=19104

    cleanup
    src_pid=
    r1_pid=
    r2_pid=
    dst_pid=
    rm -f "$out" \
        "$base/${name}.source.log" \
        "$base/${name}.relay1.log" \
        "$base/${name}.relay2.log" \
        "$base/${name}.dest.log"

    dd if=/dev/urandom of="$in" bs=1 count="$bytes" status=none

    "$bin" --role dest --listen "$p3" --output "$out" \
        --k "$k" --t "$t" --seed 0xB175 --reorder-ms 40 --idle-sec 20 \
        >"$base/${name}.dest.log" 2>&1 &
    dst_pid=$!
    wait_listen "$base/${name}.dest.log"

    "$bin" --role relay --listen "$p2" --next "127.0.0.1:$p3" \
        --recode-seed 22 --loss-percent "$loss" --loss-seed 3 \
        --reorder-ms 40 --idle-sec 20 \
        >"$base/${name}.relay2.log" 2>&1 &
    r2_pid=$!
    wait_listen "$base/${name}.relay2.log"

    "$bin" --role relay --listen "$p1" --next "127.0.0.1:$p2" \
        --recode-seed 11 --loss-percent "$loss" --loss-seed 2 \
        --reorder-ms 40 --idle-sec 20 \
        >"$base/${name}.relay1.log" 2>&1 &
    r1_pid=$!
    wait_listen "$base/${name}.relay1.log"

    "$bin" --role source --input "$in" --next "127.0.0.1:$p1" \
        --k "$k" --t "$t" --seed 0xB175 \
        --loss-percent "$loss" --loss-seed 1 --rate-mbps 300 \
        >"$base/${name}.source.log" 2>&1 &
    src_pid=$!

    src_rc=0
    r1_rc=0
    r2_rc=0
    dst_rc=0
    wait "$src_pid" || src_rc=$?
    src_pid=
    wait "$r1_pid" || r1_rc=$?
    r1_pid=
    wait "$r2_pid" || r2_rc=$?
    r2_pid=
    wait "$dst_pid" || dst_rc=$?
    dst_pid=

    echo "---- $name loss=${loss}% bytes=$bytes rc=$src_rc/$r1_rc/$r2_rc/$dst_rc ----"
    cat "$base/${name}.source.log"
    cat "$base/${name}.relay1.log"
    cat "$base/${name}.relay2.log"
    cat "$base/${name}.dest.log"

    if [ "$src_rc" -ne 0 ] || [ "$r1_rc" -ne 0 ] || [ "$r2_rc" -ne 0 ] || [ "$dst_rc" -ne 0 ]; then
        echo "FAIL $name: process exit" >&2
        return 1
    fi
    cmp "$in" "$out"
    grep -q 'api=bats_encode' "$base/${name}.source.log"
    grep -q 'unit_coeff_ok=1' "$base/${name}.source.log"
    grep -q 'g_on_wire=0' "$base/${name}.source.log"
    grep -q 'api=bats_recode' "$base/${name}.relay1.log"
    grep -q 'api=bats_recode' "$base/${name}.relay2.log"
    grep -q 'api=bats_decode' "$base/${name}.dest.log"

    dense=$(field "$base/${name}.dest.log" dense_coeff_packets)
    if [ -z "$dense" ] || [ "$dense" -le 0 ]; then
        echo "FAIL $name: destination coefficients stayed sparse; recode did not run" >&2
        return 1
    fi

    if [ "$loss" -eq 0 ]; then
        p1b=$(field "$base/${name}.relay1.log" partial_batches)
        p2b=$(field "$base/${name}.relay2.log" partial_batches)
        if [ "$p1b" != 0 ] || [ "$p2b" != 0 ]; then
            echo "FAIL $name: lossless path recoded a partial batch" >&2
            return 1
        fi
    else
        p1b=$(field "$base/${name}.relay1.log" partial_batches)
        e1b=$(field "$base/${name}.relay1.log" empty_batches)
        p2b=$(field "$base/${name}.relay2.log" partial_batches)
        e2b=$(field "$base/${name}.relay2.log" empty_batches)
        total=$((p1b + e1b + p2b + e2b))
        if [ "$total" -le 0 ]; then
            echo "FAIL $name: loss path never recoded a short batch" >&2
            return 1
        fi
    fi
    echo "PASS $name"
}

run_multi() {
    name=multi2
    k=32
    t=128
    bytes=4096
    in0=$base/${name}.in0
    in1=$base/${name}.in1
    out0=$base/${name}.out
    out1=$base/${name}.out.1
    p1=19102
    p2=19103
    p3=19104

    cleanup
    src_pid=
    src2_pid=
    r1_pid=
    r2_pid=
    dst_pid=
    rm -f "$out0" "$out1" \
        "$base/${name}.source0.log" \
        "$base/${name}.source1.log" \
        "$base/${name}.relay1.log" \
        "$base/${name}.relay2.log" \
        "$base/${name}.dest.log"

    dd if=/dev/urandom of="$in0" bs=1 count="$bytes" status=none
    dd if=/dev/urandom of="$in1" bs=1 count="$bytes" status=none

    "$bin" --role dest --listen "$p3" --output "$out0" --flows 2 \
        --k "$k" --t "$t" --seed 0xB175 --reorder-ms 40 --idle-sec 20 \
        >"$base/${name}.dest.log" 2>&1 &
    dst_pid=$!
    wait_listen "$base/${name}.dest.log"

    "$bin" --role relay --listen "$p2" --next "127.0.0.1:$p3" --flows 2 \
        --recode-seed 22 --loss-percent 0 --loss-seed 3 \
        --reorder-ms 40 --idle-sec 20 \
        >"$base/${name}.relay2.log" 2>&1 &
    r2_pid=$!
    wait_listen "$base/${name}.relay2.log"

    "$bin" --role relay --listen "$p1" --next "127.0.0.1:$p2" --flows 2 \
        --recode-seed 11 --loss-percent 0 --loss-seed 2 \
        --reorder-ms 40 --idle-sec 20 \
        >"$base/${name}.relay1.log" 2>&1 &
    r1_pid=$!
    wait_listen "$base/${name}.relay1.log"

    "$bin" --role source --flow-id 0 --input "$in0" --next "127.0.0.1:$p1" \
        --k "$k" --t "$t" --seed 0xB175 \
        --loss-percent 0 --loss-seed 1 --rate-mbps 300 \
        >"$base/${name}.source0.log" 2>&1 &
    src_pid=$!
    "$bin" --role source --flow-id 1 --input "$in1" --next "127.0.0.1:$p1" \
        --k "$k" --t "$t" --seed 0xB175 \
        --loss-percent 0 --loss-seed 4 --rate-mbps 300 \
        >"$base/${name}.source1.log" 2>&1 &
    src2_pid=$!

    src_rc=0
    src2_rc=0
    r1_rc=0
    r2_rc=0
    dst_rc=0
    wait "$src_pid" || src_rc=$?
    src_pid=
    wait "$src2_pid" || src2_rc=$?
    src2_pid=
    wait "$r1_pid" || r1_rc=$?
    r1_pid=
    wait "$r2_pid" || r2_rc=$?
    r2_pid=
    wait "$dst_pid" || dst_rc=$?
    dst_pid=

    echo "---- $name flows=2 bytes=$bytes rc=$src_rc/$src2_rc/$r1_rc/$r2_rc/$dst_rc ----"
    cat "$base/${name}.source0.log"
    cat "$base/${name}.source1.log"
    cat "$base/${name}.relay1.log"
    cat "$base/${name}.relay2.log"
    cat "$base/${name}.dest.log"

    if [ "$src_rc" -ne 0 ] || [ "$src2_rc" -ne 0 ] || [ "$r1_rc" -ne 0 ] ||
        [ "$r2_rc" -ne 0 ] || [ "$dst_rc" -ne 0 ]; then
        echo "FAIL $name: process exit" >&2
        return 1
    fi
    cmp "$in0" "$out0"
    cmp "$in1" "$out1"
    grep -q 'flow=0' "$base/${name}.source0.log"
    grep -q 'flow=1' "$base/${name}.source1.log"
    grep -q 'flows=2' "$base/${name}.relay1.log"
    grep -q 'flows=2' "$base/${name}.relay2.log"
    grep -q 'flows=2' "$base/${name}.dest.log"
    p1b=$(field "$base/${name}.relay1.log" partial_batches)
    p2b=$(field "$base/${name}.relay2.log" partial_batches)
    if [ "$p1b" != 0 ] || [ "$p2b" != 0 ]; then
        echo "FAIL $name: lossless path recoded a partial batch" >&2
        return 1
    fi
    echo "PASS $name"
}

run_case lossless 0 4096 32 128
run_case loss5 5 5000 32 128
run_multi
echo "bats_line_loopback: ok"
