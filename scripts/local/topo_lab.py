"""Shared lab topology: VXLAN (legacy) vs linear virtio bridges.

Set WH_TOPO=linear|vxlan (default vxlan for old scripts; runners pass linear).
"""

from __future__ import annotations

import os

TOPO = os.environ.get("WH_TOPO", "vxlan").strip().lower()

# SSH always via mgmt
SSH_HOST = {
    1: "fyp1@10.10.10.161",
    2: "fyp1@10.10.10.162",
    3: "fyp1@10.10.10.163",
    4: "fyp1@10.10.10.164",
}

if TOPO == "linear":
    # Data-plane endpoints for hop sinks / next-hops
    N1 = "10.20.20.1"  # VM1 on hop1
    N2 = "10.20.20.2"  # VM2 on hop1
    N3 = "10.30.30.2"  # VM3 on hop2
    N4 = "10.40.40.2"  # VM4 on hop3
    # App-relay next-hop addresses (enter each hop)
    RELAY2_NEXT = "10.30.30.2"  # VM2 -> VM3
    RELAY3_NEXT = "10.40.40.2"  # VM3 -> VM4
    NETEM_IFACE = "enp6s19"
    MONITOR_IFACES = {
        1: "enp6s19",
        2: "enp6s19 enp6s20",
        3: "enp6s20 enp6s21",
        4: "enp6s21",
    }
    # iperf hop pairs (src_mgmt_node, dst_ip, label)
    IPERF_LINKS = [
        (1, "10.20.20.2", "hop1_VM1_VM2"),
        (2, "10.30.30.2", "hop2_VM2_VM3"),
        (3, "10.40.40.2", "hop3_VM3_VM4"),
        (1, "10.40.40.2", "e2e_VM1_VM4"),
    ]
else:
    N1 = "10.10.12.1"
    N2 = "10.10.12.2"
    N3 = "10.10.23.2"
    N4 = "10.10.34.2"
    RELAY2_NEXT = "10.10.23.2"
    RELAY3_NEXT = "10.10.34.2"
    NETEM_IFACE = "station0"
    MONITOR_IFACES = {
        1: "station0",
        2: "ap0 station1",
        3: "ap1 station2",
        4: "ap2",
    }
    IPERF_LINKS = [
        (1, "10.10.12.2", "hop1_VM1_VM2"),
        (2, "10.10.23.2", "hop2_VM2_VM3"),
        (3, "10.10.34.2", "hop3_VM3_VM4"),
        (1, "10.10.34.2", "e2e_VM1_VM4"),
    ]

# Ceiling / hop matrix path tuples:
# (name, hops, app_relay, sink_node, next_hop_ip, relays[(node, next_ip), ...])
CEILING_PATHS = [
    ("hop1_direct", 1, False, 2, N2, []),
    ("hop2_kernel", 2, False, 3, N3, []),
    ("hop2_relay", 2, True, 3, N2, [(2, RELAY2_NEXT)]),
    ("hop3_kernel", 3, False, 4, N4, []),
    ("hop3_relay", 3, True, 4, N2, [(2, RELAY2_NEXT), (3, RELAY3_NEXT)]),
]
