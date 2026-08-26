#!/usr/bin/env python3
"""Probe Wirehair+relay toward 1000 Mbps and isolate the bottleneck.

Path: Node1 -> Node2 (opaque wire_relay) -> Node3
Also baselines each WiFi hop with iperf3 UDP.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import time

SSH1 = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.161"]
SSH2 = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.162"]
SSH3 = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "fyp1@10.10.10.163"]

WG = "/home/fyp1/work/multi-flow-rate-control/build/wg_multi_pipeline"
RELAY = "/home/fyp1/work/multi-flow-rate-control/build/wire_relay"
BASE = "/tmp/wirehair_1g_probe"
PORT_BASE = 24100
IPERF_PORT = 25201
INPUT = f"{BASE}/input.bin"
N1_DATA = "10.10.12.1"
N2_AP0 = "10.10.12.2"
N2_ST1 = "10.10.23.1"
N3_AP1 = "10.10.23.2"
MIB = 100
REPAIR = 20
WINDOW = 8
# Target toward 1 Gbps
RATES = [200, 400, 600, 800, 1000, 0]  # 0 = unlimited


def run(cmd, check=True, timeout=900):
    r = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if check and r.returncode != 0:
        print("FAIL:", cmd, file=sys.stderr)
        print(r.stdout[-4000:], file=sys.stderr)
        print(r.stderr[-4000:], file=sys.stderr)
        raise SystemExit(r.returncode or 1)
    return r


def ssh(host, script, check=True, timeout=900):
    return run(host + [script], check=check, timeout=timeout)


def wait_grep(host, log, pat, n=400):
    for _ in range(n):
        q = pat.replace("'", "'\\''")
        if ssh(host, f"grep -q '{q}' {log}", check=False).returncode == 0:
            return True
        time.sleep(0.1)
    return False


def cleanup_apps():
    ssh(SSH1, "pkill -f 'wg_multi_pipeline.*241' >/dev/null 2>&1 || true; "
              "pkill -f 'iperf3.*25201' >/dev/null 2>&1 || true", check=False)
    ssh(SSH2, "pkill -f 'wire_relay.*241' >/dev/null 2>&1 || true; "
              "pkill -f 'wg_multi_pipeline.*241' >/dev/null 2>&1 || true; "
              "pkill -f 'iperf3.*25201' >/dev/null 2>&1 || true", check=False)
    ssh(SSH3, "pkill -f 'wg_multi_pipeline.*241' >/dev/null 2>&1 || true; "
              "pkill -f 'iperf3.*25201' >/dev/null 2>&1 || true", check=False)
    time.sleep(0.5)


def prepare_input():
    ssh(
        SSH1,
        f"""
set -e
mkdir -p {BASE}
if [ ! -f {INPUT} ] || [ "$(stat -c%s {INPUT})" -ne $(({MIB}*1024*1024)) ]; then
  python3 - <<'PY'
from pathlib import Path
size = {MIB} * 1024 * 1024
path = Path("{INPUT}")
block = bytes((i * 37 + i // 17 + 23) & 0xff for i in range(65536))
with path.open("wb") as f:
    left = size
    while left:
        n = min(left, len(block))
        f.write(block[:n])
        left -= n
print(path.stat().st_size)
PY
fi
sha256sum {INPUT} | tee {INPUT}.sha
""",
    )
    for host, dest in ((SSH2, "fyp1@10.10.10.162"), (SSH3, "fyp1@10.10.10.163")):
        ssh(host, f"mkdir -p {BASE}")
        run(["scp", "-o", "BatchMode=yes",
             f"fyp1@10.10.10.161:{INPUT}.sha", f"{dest}:{INPUT}.sha"])


def parse_relay_summary(text: str) -> dict:
    out = {}
    keys = [
        "rx", "forward", "local", "drop_ttl", "drop_malformed", "drop_send",
        "drop_egress_full", "drop_egress_timeout", "drop_deferred_flow",
        "drop_deferred_total", "drop_deferred_table", "egress_high_watermark",
        "deferred_hwm", "forward_ack", "drop_no_return_hop",
    ]
    for k in keys:
        m = re.search(rf"{k}=(\d+)", text)
        if m:
            out[k] = int(m.group(1))
    return out


def nic_snap(host, ifaces: str) -> dict:
    """Return {iface: (rx_bytes, tx_bytes, rx_drop, tx_drop)} via /sys."""
    script = f"""
python3 - <<'PY'
from pathlib import Path
ifaces = "{ifaces}".split()
for iface in ifaces:
    base = Path(f"/sys/class/net/{{iface}}/statistics")
    def g(n):
        p = base / n
        return int(p.read_text()) if p.exists() else -1
    print(iface, g("rx_bytes"), g("tx_bytes"), g("rx_dropped"), g("tx_dropped"),
          g("rx_errors"), g("tx_errors"))
PY
"""
    r = ssh(host, script, check=False)
    out = {}
    for line in r.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 7:
            out[parts[0]] = {
                "rx_b": int(parts[1]), "tx_b": int(parts[2]),
                "rx_drop": int(parts[3]), "tx_drop": int(parts[4]),
                "rx_err": int(parts[5]), "tx_err": int(parts[6]),
            }
    return out


def nic_delta(a: dict, b: dict) -> dict:
    d = {}
    for iface in b:
        if iface not in a:
            continue
        d[iface] = {k: b[iface][k] - a[iface][k] for k in b[iface]}
    return d


def iperf_udp_hop(tag, server_ssh, server_bind, client_ssh, client_dst,
                  bitrate_mbps, seconds=8) -> dict:
    cleanup_apps()
    slog = f"{BASE}/iperf_{tag}_srv.json"
    clog = f"{BASE}/iperf_{tag}_cli.json"
    ssh(server_ssh, f"rm -f {slog}; mkdir -p {BASE}")
    ssh(client_ssh, f"rm -f {clog}; mkdir -p {BASE}")
    ssh(
        server_ssh,
        f"nohup iperf3 -s -B {server_bind} -p {IPERF_PORT} -1 "
        f"--json >{slog} 2>/dev/null & echo $! > {BASE}/iperf_srv.pid",
    )
    time.sleep(0.8)
    ssh(
        client_ssh,
        f"iperf3 -c {client_dst} -p {IPERF_PORT} -u -b {bitrate_mbps}M "
        f"-t {seconds} -l 1370 --json >{clog} 2>/dev/null || true",
        timeout=120,
    )
    time.sleep(0.5)
    cli = ssh(client_ssh, f"cat {clog}", check=False).stdout
    srv = ssh(server_ssh, f"cat {slog}", check=False).stdout
    row = {"tag": tag, "target_mbps": bitrate_mbps}
    try:
        cj = json.loads(cli)
        end = cj.get("end", {})
        sum_ = end.get("sum", end.get("sum_sent", {}))
        row["cli_mbps"] = float(sum_.get("bits_per_second", 0)) / 1e6
        row["cli_lost_pct"] = float(sum_.get("lost_percent", -1))
        row["cli_lost"] = int(sum_.get("lost_packets", -1))
        row["cli_pkts"] = int(sum_.get("packets", -1))
    except Exception as e:
        row["cli_err"] = str(e)[:80]
    try:
        sj = json.loads(srv)
        end = sj.get("end", {})
        sum_ = end.get("sum", end.get("sum_received", {}))
        row["srv_mbps"] = float(sum_.get("bits_per_second", 0)) / 1e6
        row["srv_lost_pct"] = float(sum_.get("lost_percent", -1))
    except Exception as e:
        row["srv_err"] = str(e)[:80]
    cleanup_apps()
    return row


def one_wirehair(tag: str, rate: float, port: int, ack: bool) -> dict:
    cleanup_apps()
    out = f"{BASE}/{tag}.out"
    rlog = f"{BASE}/{tag}_recv.log"
    xlog = f"{BASE}/{tag}_relay.log"
    slog = f"{BASE}/{tag}_send.log"
    ack_port = port + 1
    rate_arg = "" if rate <= 0 else f"--rate-mbps {rate}"
    rate_label = "unlimited" if rate <= 0 else str(int(rate))
    ack_flag = "--wh-ack" if ack else "--no-wh-ack"
    ack_port_arg = f"--ack-port={ack_port}" if ack else ""

    for host in (SSH1, SSH2, SSH3):
        ssh(host, f"mkdir -p {BASE}")
    ssh(SSH3, f"rm -f {out} {rlog}; : > {rlog}")
    ssh(SSH2, f"rm -f {xlog}; : > {xlog}")
    ssh(SSH1, f"rm -f {slog}; : > {slog}")

    # NIC before
    n1_a = nic_snap(SSH1, "station0")
    n2_a = nic_snap(SSH2, "ap0 station1")
    n3_a = nic_snap(SSH3, "ap1")

    ssh(
        SSH3,
        f"""
nohup {WG} --codec wirehair --wh-segment-mib=10 --wh-repair-pct={REPAIR} \
  --wh-window={WINDOW} {ack_flag} \
  --local-node-id 4 --udp-recv {port} {out} --idle-sec 90 --strict \
  >{rlog} 2>&1 &
echo $! > {BASE}/{tag}_recv.pid
""",
    )
    if not wait_grep(SSH3, rlog, "wirehair-recv: listening", 400):
        print(ssh(SSH3, f"cat {rlog}", check=False).stdout[-2000:])
        return {"mode": "ack" if ack else "noack", "rate": rate_label,
                "verify": "RECV_FAIL"}

    ssh(
        SSH2,
        f"""
nohup {RELAY} --local-node-id 2 --listen {port} \
  --next-hop {N3_AP1}:{port} --return-hop {N1_DATA}:{ack_port} \
  --egress-capacity 16384 --idle-exit-sec 120 \
  >{xlog} 2>&1 &
echo $! > {BASE}/{tag}_relay.pid
""",
    )
    if not wait_grep(SSH2, xlog, "wire-relay: local_node_id", 400):
        print(ssh(SSH2, f"cat {xlog}", check=False).stdout[-2000:])
        return {"mode": "ack" if ack else "noack", "rate": rate_label,
                "verify": "RELAY_FAIL"}

    send = ssh(
        SSH1,
        f"""
set -e
/usr/bin/time -f 'wall_sec=%e' -o {BASE}/{tag}_time.txt \
  {WG} --codec wirehair --wh-segment-mib=10 --wh-repair-pct={REPAIR} \
  --wh-window={WINDOW} {ack_flag} {ack_port_arg} \
  --local-node-id 1 --final-dst 4 --ttl 8 {rate_arg} \
  --udp-send {N2_AP0} {port} {INPUT} >{slog} 2>&1
cat {BASE}/{tag}_time.txt
echo ---SEND---
cat {slog}
""",
        timeout=1200,
    )

    for _ in range(600):
        if ssh(SSH3, f"kill -0 $(cat {BASE}/{tag}_recv.pid) 2>/dev/null",
               check=False).returncode != 0:
            break
        time.sleep(0.2)

    # Give relay a moment to print summary on idle-exit (may still be up)
    time.sleep(0.5)
    # Force relay exit to get summary if still running
    ssh(SSH2, f"kill $(cat {BASE}/{tag}_relay.pid) 2>/dev/null || true",
        check=False)
    time.sleep(0.3)

    n1_b = nic_snap(SSH1, "station0")
    n2_b = nic_snap(SSH2, "ap0 station1")
    n3_b = nic_snap(SSH3, "ap1")
    nic = {
        "n1": nic_delta(n1_a, n1_b),
        "n2": nic_delta(n2_a, n2_b),
        "n3": nic_delta(n3_a, n3_b),
    }

    ver = ssh(
        SSH3,
        f"""
exp=$(cut -d' ' -f1 {INPUT}.sha)
if [ ! -f {out} ]; then echo VERIFY=FAIL; echo missing_out; exit 0; fi
got=$(sha256sum {out} | awk '{{print $1}}')
echo exp=$exp
echo got=$got
ls -l {out}
if [ "$exp" = "$got" ]; then echo VERIFY=OK; else echo VERIFY=FAIL; fi
""",
        check=False,
    )
    xtext = ssh(SSH2, f"cat {xlog}", check=False).stdout
    relay = parse_relay_summary(xtext)

    row = {
        "mode": "ack" if ack else "noack",
        "rate": rate_label,
        "verify": "OK" if "VERIFY=OK" in ver.stdout else "FAIL",
        "relay": relay,
        "nic": nic,
    }
    m = re.search(
        r"wirehair-send: source_bytes=(\d+) segments=(\d+) repair_sent=(\d+) "
        r"wire_bytes=(\d+)",
        send.stdout,
    )
    if m:
        row["source_bytes"] = int(m.group(1))
        row["segments"] = int(m.group(2))
        row["repair_sent"] = int(m.group(3))
        row["wire_bytes"] = int(m.group(4))
    t = re.search(r"wall_sec=([0-9.]+)", send.stdout)
    if t:
        row["wall_s"] = float(t.group(1))
        if row.get("source_bytes") and row["wall_s"] > 0:
            row["goodput_mbps"] = row["source_bytes"] * 8.0 / row["wall_s"] / 1e6
        if row.get("wire_bytes") and row["wall_s"] > 0:
            row["wire_mbps"] = row["wire_bytes"] * 8.0 / row["wall_s"] / 1e6

    if row["verify"] != "OK":
        print(f"--- FAIL {tag} recv ---")
        print(ssh(SSH3, f"tail -40 {rlog}", check=False).stdout)
        print(f"--- FAIL {tag} relay ---")
        print(xtext[-2500:])
        print(f"--- FAIL {tag} send ---")
        print(send.stdout[-2500:])
    return row


def fmt_row(r):
    rel = r.get("relay", {})
    drops = (rel.get("drop_deferred_flow", 0)
             + rel.get("drop_egress_full", 0)
             + rel.get("drop_send", 0))
    nic_drop = 0
    for node in r.get("nic", {}).values():
        for stats in node.values():
            for k in ("rx_drop", "tx_drop", "rx_err", "tx_err"):
                if stats.get(k, 0) > 0:
                    nic_drop += stats[k]
    return (
        f"{r.get('mode','?'):>5} {r.get('rate','?'):>10} "
        f"{r.get('verify','?'):>6} "
        f"{r.get('wall_s', float('nan')):7.2f} "
        f"{r.get('goodput_mbps', float('nan')):8.1f} "
        f"{r.get('wire_mbps', float('nan')):8.1f} "
        f"{r.get('repair_sent', -1):8d} "
        f"{rel.get('rx', -1):8d} "
        f"{drops:7d} "
        f"{rel.get('egress_high_watermark', -1):6d} "
        f"{nic_drop:7d}"
    )


def main():
    print(f"1Gbps probe: {MIB} MiB, repair={REPAIR}%, window={WINDOW}, "
          f"N1→N2(relay)→N3, egress_cap=16384")
    prepare_input()

    print("\n==== iperf3 UDP hop baselines (target 1000 Mbps, 8s, 1370B) ====")
    iperf_rows = []
    for tag, srv, bind, cli, dst in (
        ("hop12", SSH2, N2_AP0, SSH1, N2_AP0),
        ("hop23", SSH3, N3_AP1, SSH2, N3_AP1),
    ):
        row = iperf_udp_hop(tag, srv, bind, cli, dst, 1000, seconds=8)
        iperf_rows.append(row)
        print(row)
        sys.stdout.flush()
    # also try 500 to see if medium works
    for tag, srv, bind, cli, dst in (
        ("hop12_500", SSH2, N2_AP0, SSH1, N2_AP0),
        ("hop23_500", SSH3, N3_AP1, SSH2, N3_AP1),
    ):
        row = iperf_udp_hop(tag, srv, bind, cli, dst, 500, seconds=6)
        iperf_rows.append(row)
        print(row)
        sys.stdout.flush()

    results = []
    port = PORT_BASE
    for ack in (True, False):
        mode = "ACK" if ack else "noACK"
        print(f"\n==== Wirehair relay {mode} ====")
        for rate in RATES:
            tag = f"{'a' if ack else 'n'}{int(rate) if rate > 0 else 0}"
            print(f"\n--- {mode} rate={rate if rate>0 else 'unlimited'} "
                  f"port={port} ---")
            row = one_wirehair(tag, rate, port, ack=ack)
            results.append(row)
            print(fmt_row(row))
            print("  relay:", row.get("relay"))
            # compact nic drops
            for node, ifaces in row.get("nic", {}).items():
                interesting = {
                    i: s for i, s in ifaces.items()
                    if any(s.get(k, 0) for k in
                           ("rx_drop", "tx_drop", "rx_err", "tx_err"))
                    or s.get("rx_b", 0) > 1_000_000
                    or s.get("tx_b", 0) > 1_000_000
                }
                if interesting:
                    print(f"  nic[{node}]:", interesting)
            port += 2
            sys.stdout.flush()

    cleanup_apps()
    print("\n==== SUMMARY (mode rate verify wall goodput wire repair "
          "relay_rx drops egress_hwm nic_drop) ====")
    print(f"{'mode':>5} {'rate':>10} {'verify':>6} {'wall':>7} "
          f"{'goodput':>8} {'wire':>8} {'repair':>8} {'rx':>8} "
          f"{'drops':>7} {'ehwm':>6} {'nicdr':>7}")
    for r in results:
        print(fmt_row(r))

    print("\n==== iperf summary ====")
    for r in iperf_rows:
        print(
            f"{r.get('tag'):>12} target={r.get('target_mbps')} "
            f"cli={r.get('cli_mbps', float('nan')):.1f} "
            f"srv={r.get('srv_mbps', float('nan')):.1f} "
            f"lost%={r.get('cli_lost_pct', r.get('srv_lost_pct', -1))}"
        )

    # Persist for later
    payload = {"iperf": iperf_rows, "wirehair": results}
    # strip nic nested for size? keep it
    path = "/tmp/wirehair_1g_probe_summary.json"
    # write locally via stdout file from this machine
    with open("/home/scy/work/multi-flow-rate-control/build/wirehair_1g_probe.json",
              "w") as f:
        json.dump(payload, f, indent=2, default=str)
    print(f"\nWrote build/wirehair_1g_probe.json")


if __name__ == "__main__":
    main()
