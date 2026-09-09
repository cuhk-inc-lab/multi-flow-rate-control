#!/usr/bin/env bash
# Configure LINEAR hop topology on virtio bridges (not VXLAN).
# Keep SSH on 10.10.10.16x / enp6s18 unchanged.
#
# Target:
#   VM1 enp6s19 10.20.20.1/24  ---  VM2 enp6s19 10.20.20.2/24
#   VM2 enp6s20 10.30.30.1/24  ---  VM3 enp6s20 10.30.30.2/24
#   VM3 enp6s21 10.40.40.1/24  ---  VM4 enp6s21 10.40.40.2/24
#
# Usage (on each VM, with sudo):
#   ROLE=vm1 sudo -E bash scripts/local/setup_linear_bridge_hops.sh
#   ROLE=vm2 sudo -E bash ...
#
# Prerequisite: host bridges must allow L2 between the pairs above.
# If enp6s20 on VM2 is NOT on the same Proxmox bridge as enp6s20 on VM3,
# rewire Proxmox first (see scripts/local/LINEAR_BRIDGE_HOPS.md).

set -euo pipefail

ROLE=${ROLE:-}
APPLY=${APPLY:-1} # 1=apply, 0=print only

die() { echo "error: $*" >&2; exit 1; }

# Auto-detect from hostname if ROLE unset (avoids applying vm4 on every node).
if [[ -z "$ROLE" ]]; then
  hn=$(hostname -s 2>/dev/null || hostname)
  case "$hn" in
    fyp01|vm1) ROLE=vm1 ;;
    fyp02|vm2) ROLE=vm2 ;;
    fyp03|vm3) ROLE=vm3 ;;
    fyp04|vm4) ROLE=vm4 ;;
    *) die "cannot detect ROLE from hostname='$hn'; set ROLE=vm1|vm2|vm3|vm4" ;;
  esac
  echo "auto ROLE=$ROLE (hostname=$hn)"
fi

[[ "$ROLE" =~ ^vm[1-4]$ ]] || die "bad ROLE=$ROLE (want vm1|vm2|vm3|vm4)"

need_root() {
  [[ "$(id -u)" -eq 0 ]] || die "run as root (sudo)"
}

flush_data_nics() {
  for ifc in enp6s19 enp6s20 enp6s21; do
    ip addr flush dev "$ifc" 2>/dev/null || true
    ip link set "$ifc" up
  done
}

add_addr() {
  local ifc=$1 cidr=$2
  ip addr add "$cidr" dev "$ifc"
  ip link set "$ifc" up
}

add_route() {
  ip route replace "$@"
}

enable_forward() {
  sysctl -w net.ipv4.ip_forward=1 >/dev/null
  # optional persist snippet
  mkdir -p /etc/sysctl.d
  echo 'net.ipv4.ip_forward=1' >/etc/sysctl.d/99-mfrc-linear-forward.conf
}

disable_forward() {
  # endpoints usually no need; leave sysctl file only on middle hops
  true
}

plan_vm1() {
  cat <<'EOF'
VM1 (endpoint):
  enp6s18 10.10.10.161/24  (SSH, untouched)
  enp6s19 10.20.20.1/24
  route 10.30.30.0/24 via 10.20.20.2
  route 10.40.40.0/24 via 10.20.20.2
EOF
}

apply_vm1() {
  flush_data_nics
  add_addr enp6s19 10.20.20.1/24
  add_route 10.30.30.0/24 via 10.20.20.2 dev enp6s19
  add_route 10.40.40.0/24 via 10.20.20.2 dev enp6s19
}

plan_vm2() {
  cat <<'EOF'
VM2 (hop):
  enp6s19 10.20.20.2/24
  enp6s20 10.30.30.1/24
  ip_forward=1
  route 10.40.40.0/24 via 10.30.30.2
EOF
}

apply_vm2() {
  flush_data_nics
  add_addr enp6s19 10.20.20.2/24
  add_addr enp6s20 10.30.30.1/24
  enable_forward
  add_route 10.40.40.0/24 via 10.30.30.2 dev enp6s20
}

plan_vm3() {
  cat <<'EOF'
VM3 (hop):
  enp6s20 10.30.30.2/24
  enp6s21 10.40.40.1/24
  ip_forward=1
  route 10.20.20.0/24 via 10.30.30.1
EOF
}

apply_vm3() {
  flush_data_nics
  add_addr enp6s20 10.30.30.2/24
  add_addr enp6s21 10.40.40.1/24
  enable_forward
  add_route 10.20.20.0/24 via 10.30.30.1 dev enp6s20
}

plan_vm4() {
  cat <<'EOF'
VM4 (endpoint):
  enp6s21 10.40.40.2/24
  route 10.20.20.0/24 via 10.40.40.1
  route 10.30.30.0/24 via 10.40.40.1
EOF
}

apply_vm4() {
  flush_data_nics
  add_addr enp6s21 10.40.40.2/24
  add_route 10.20.20.0/24 via 10.40.40.1 dev enp6s21
  add_route 10.30.30.0/24 via 10.40.40.1 dev enp6s21
}

echo "=== plan ROLE=$ROLE ==="
case "$ROLE" in
  vm1) plan_vm1 ;;
  vm2) plan_vm2 ;;
  vm3) plan_vm3 ;;
  vm4) plan_vm4 ;;
  *) die "bad ROLE" ;;
esac

if [[ "$APPLY" != "1" ]]; then
  echo "(APPLY=0, dry-run only)"
  exit 0
fi

need_root
case "$ROLE" in
  vm1) apply_vm1 ;;
  vm2) apply_vm2 ;;
  vm3) apply_vm3 ;;
  vm4) apply_vm4 ;;
esac

echo "=== result ==="
ip -br a | grep -E 'enp6s1[89]|enp6s2[01]'
ip r | grep -E '10\.(20|30|40)\.|forward' || ip r
echo "ip_forward=$(cat /proc/sys/net/ipv4/ip_forward)"
echo "OK ROLE=$ROLE"
