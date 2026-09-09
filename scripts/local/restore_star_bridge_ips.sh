#!/usr/bin/env bash
# Restore previous star-style bridge IPs (pre-linear attempt).
#   VM1: 10.20.20.11, 10.30.30.11, 10.40.40.11
#   VM2: 10.20.20.12
#   VM3: 10.30.30.12
#   VM4: 10.40.40.12
# SSH enp6s18 / 10.10.10.16x untouched.
#
# Usage: sudo bash scripts/local/restore_star_bridge_ips.sh
#   (auto-detects fyp01..04)

set -euo pipefail
[[ "$(id -u)" -eq 0 ]] || { echo "need root"; exit 1; }

hn=$(hostname -s 2>/dev/null || hostname)
for ifc in enp6s19 enp6s20 enp6s21; do
  ip addr flush dev "$ifc" 2>/dev/null || true
  ip link set "$ifc" up
done

# drop leftover linear routes if present
ip route del 10.20.20.0/24 2>/dev/null || true
ip route del 10.30.30.0/24 2>/dev/null || true
ip route del 10.40.40.0/24 2>/dev/null || true

case "$hn" in
  fyp01|vm1)
    ip addr add 10.20.20.11/24 dev enp6s19
    ip addr add 10.30.30.11/24 dev enp6s20
    ip addr add 10.40.40.11/24 dev enp6s21
    ;;
  fyp02|vm2)
    ip addr add 10.20.20.12/24 dev enp6s19
    ;;
  fyp03|vm3)
    ip addr add 10.30.30.12/24 dev enp6s20
    ;;
  fyp04|vm4)
    ip addr add 10.40.40.12/24 dev enp6s21
    ;;
  *)
    echo "unknown host $hn"; exit 1
    ;;
esac

echo "restored star IPs on $hn"
ip -br a | grep enp6s
