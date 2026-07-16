#!/usr/bin/env sh

set -eu

receiver_ip=${1:?usage: run_vm_baseline.sh RECEIVER_IP [SECONDS]}
seconds=${2:-10}
udp_rates=${UDP_RATES:-"100M 250M 500M 750M 1G"}

if ! command -v iperf3 >/dev/null 2>&1; then
    echo "iperf3 is required" >&2
    exit 1
fi

if command -v traceroute >/dev/null 2>&1; then
    traceroute -n "$receiver_ip"
elif command -v tracepath >/dev/null 2>&1; then
    tracepath -n "$receiver_ip"
else
    echo "Neither traceroute nor tracepath is installed; hop count not recorded." >&2
fi

echo "=== TCP baseline ==="
iperf3 -c "$receiver_ip" -p 5201 -t "$seconds"

for rate in $udp_rates; do
    echo "=== UDP offered rate: $rate ==="
    iperf3 -c "$receiver_ip" -p 5202 -u -b "$rate" -t "$seconds"
done
