#!/usr/bin/env python3
"""Configurable multi-stream wire stress orchestrator.

Called by scripts/run_wire_stress.sh. See scripts/examples/stress_lab.yaml.

One wg_multi_pipeline process = one codec. Streams are grouped by (to, codec)
for receivers and by (from, codec, dest_ip:port) for senders.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

WIRE_MAX_FLOWS = 8
SSH_OPTS = [
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=10",
    "-o",
    "ServerAliveInterval=5",
    "-o",
    "ServerAliveCountMax=3",
]


def die(msg: str, code: int = 1) -> None:
    print(f"error: {msg}", file=sys.stderr, flush=True)
    raise SystemExit(code)


def log(msg: str) -> None:
    print(msg, flush=True)


def load_config(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in (".yaml", ".yml"):
        try:
            import yaml  # type: ignore
        except ImportError:
            die(
                "YAML config requires PyYAML (pip install PyYAML). "
                "Or pass a .json config instead."
            )
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)
    if not isinstance(data, dict):
        die("config root must be an object")
    return data


@dataclass
class Node:
    name: str
    ssh: str
    ip: str
    monitor_ifaces: str = ""
    remote_repo: str = ""

    @property
    def is_local(self) -> bool:
        return self.ssh.strip().lower() == "local"


@dataclass
class Stream:
    id: int
    from_node: str
    to_node: str
    file: str
    rate_mbps: float
    codec: str
    src_hash: str = ""
    src_bytes: int = 0
    dest_ip: str = ""
    dest_port: int = 0
    status: str = "PENDING"
    note: str = ""
    recv_hash: str = ""
    log_fid: str = ""
    missing_groups: str = "NA"
    incomplete: bool = False
    file_ext: str = ""


@dataclass
class RecvGroup:
    key: str
    to_node: str
    codec: str
    port: int
    streams: List[Stream] = field(default_factory=list)
    remote_dir: str = ""
    prefix: str = ""
    log_path: str = ""
    pid: Optional[int] = None
    local_proc: Optional[subprocess.Popen] = None
    idle_sec: int = 20


@dataclass
class SendGroup:
    key: str
    from_node: str
    codec: str
    dest_ip: str
    dest_port: int
    streams: List[Stream] = field(default_factory=list)
    log_path: str = ""
    remote_log: str = ""
    pid: Optional[int] = None
    local_proc: Optional[subprocess.Popen] = None
    staged_files: Dict[int, str] = field(default_factory=dict)


def expand_home(path: str) -> str:
    return os.path.expanduser(path)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def file_size(path: Path) -> int:
    return path.stat().st_size


def eta_seconds(nbytes: int, mbps: float, idle: int) -> int:
    if mbps <= 0:
        return max(idle + 30, 60)
    return max(idle + 15, int(nbytes * 8 / (mbps * 1e6) + idle + 20))


def human_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KiB"
    if n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.2f} MiB"
    return f"{n / (1024 * 1024 * 1024):.2f} GiB"


def file_ext_of(path: str) -> str:
    name = Path(path).name
    if "." in name:
        return "." + name.rsplit(".", 1)[-1]
    return ""


def ssh_cmd(host: str, remote: str) -> List[str]:
    return ["ssh", *SSH_OPTS, host, remote]


def scp_to(local: str, host: str, remote: str) -> None:
    subprocess.run(
        ["scp", *SSH_OPTS, local, f"{host}:{remote}"],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


def scp_from(host: str, remote: str, local: str) -> bool:
    r = subprocess.run(
        ["scp", *SSH_OPTS, f"{host}:{remote}", local],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return r.returncode == 0


def remote_sha256(host: str, path: str) -> str:
    r = subprocess.run(
        ssh_cmd(host, f"sha256sum {shlex.quote(path)} 2>/dev/null | awk '{{print $1}}'"),
        capture_output=True,
        text=True,
    )
    return (r.stdout or "").strip()


def validate_and_build(
    cfg: dict, repo_root: Path
) -> Tuple[Dict[str, Node], List[Stream], dict]:
    nodes_raw = cfg.get("nodes") or {}
    streams_raw = cfg.get("streams") or []
    defaults = cfg.get("defaults") or {}
    if not nodes_raw:
        die("config.nodes is required")
    if not streams_raw:
        die("config.streams is required")

    remote_repo_default = expand_home(
        str(defaults.get("remote_repo") or "~/work/multi-flow-rate-control")
    )

    nodes: Dict[str, Node] = {}
    for name, n in nodes_raw.items():
        if not isinstance(n, dict):
            die(f"nodes.{name} must be an object")
        ssh = str(n.get("ssh", "")).strip()
        ip = str(n.get("ip", "")).strip()
        if not ssh or not ip:
            die(f"nodes.{name} needs ssh and ip")
        nodes[name] = Node(
            name=name,
            ssh=ssh,
            ip=ip,
            monitor_ifaces=str(n.get("monitor_ifaces", "") or ""),
            remote_repo=expand_home(str(n.get("remote_repo") or remote_repo_default)),
        )

    streams: List[Stream] = []
    for i, s in enumerate(streams_raw):
        if not isinstance(s, dict):
            die(f"streams[{i}] must be an object")
        try:
            sid = int(s["id"])
        except (KeyError, TypeError, ValueError):
            die(f"streams[{i}].id must be an integer")
        if sid < 0 or sid >= WIRE_MAX_FLOWS:
            die(
                f"stream id {sid} out of range; wire flow_id / --out-suffix "
                f"must be 0..{WIRE_MAX_FLOWS - 1}"
            )
        frm = str(s.get("from", "")).strip()
        to = str(s.get("to", "")).strip()
        if to.lower() == "loopback":
            to = frm
        if frm not in nodes:
            die(f"stream {sid}: unknown from node {frm!r}")
        if to not in nodes:
            die(f"stream {sid}: unknown to node {to!r}")
        codec = str(s.get("codec", "")).strip()
        if not codec:
            die(f"stream {sid}: codec required")
        fpath = str(s.get("file", "")).strip()
        if not fpath:
            die(f"stream {sid}: file required")
        try:
            rate = float(s["rate_mbps"])
        except (KeyError, TypeError, ValueError):
            die(f"stream {sid}: rate_mbps required number")
        if rate <= 0:
            die(f"stream {sid}: rate_mbps must be > 0")

        st = Stream(
            id=sid,
            from_node=frm,
            to_node=to,
            file=fpath,
            rate_mbps=rate,
            codec=codec,
            dest_ip="127.0.0.1" if frm == to else nodes[to].ip,
            file_ext=file_ext_of(fpath),
        )
        streams.append(st)

    for st in streams:
        if nodes[st.from_node].is_local:
            p = Path(st.file)
            if not p.is_file():
                alt = repo_root / st.file
                if alt.is_file():
                    st.file = str(alt.resolve())
                else:
                    die(f"stream {st.id}: local file not found: {st.file}")
            else:
                st.file = str(p.resolve())
            st.src_hash = sha256_file(Path(st.file))
            st.src_bytes = file_size(Path(st.file))
            st.file_ext = file_ext_of(st.file)

    by_recv: Dict[Tuple[str, str], List[Stream]] = defaultdict(list)
    for st in streams:
        by_recv[(st.to_node, st.codec)].append(st)
    for (to, codec), group in by_recv.items():
        if len(group) > WIRE_MAX_FLOWS:
            die(
                f"receiver group ({to}, {codec}) has {len(group)} streams; "
                f"max is {WIRE_MAX_FLOWS} per process"
            )
        ids = [s.id for s in group]
        if len(ids) != len(set(ids)):
            die(
                f"duplicate flow id in receiver group ({to}, {codec}); "
                f"id must be unique per (to, codec) listen socket"
            )

    return nodes, streams, defaults


def assign_ports_and_groups(
    streams: List[Stream], port_base: int
) -> Tuple[List[RecvGroup], List[SendGroup]]:
    recv_keys: List[Tuple[str, str]] = []
    seen = set()
    for st in streams:
        key = (st.to_node, st.codec)
        if key not in seen:
            seen.add(key)
            recv_keys.append(key)

    port_map = {key: port_base + i for i, key in enumerate(recv_keys)}

    recv_groups: Dict[str, RecvGroup] = {}
    for st in streams:
        rk = (st.to_node, st.codec)
        port = port_map[rk]
        st.dest_port = port
        gkey = f"{st.to_node}|{st.codec}|{port}"
        if gkey not in recv_groups:
            recv_groups[gkey] = RecvGroup(
                key=gkey, to_node=st.to_node, codec=st.codec, port=port
            )
        recv_groups[gkey].streams.append(st)

    send_groups: Dict[str, SendGroup] = {}
    for st in streams:
        sk = f"{st.from_node}|{st.codec}|{st.dest_ip}:{st.dest_port}"
        if sk not in send_groups:
            send_groups[sk] = SendGroup(
                key=sk,
                from_node=st.from_node,
                codec=st.codec,
                dest_ip=st.dest_ip,
                dest_port=st.dest_port,
            )
        send_groups[sk].streams.append(st)

    for sg in send_groups.values():
        if len(sg.streams) > WIRE_MAX_FLOWS:
            die(
                f"sender group {sg.key} has {len(sg.streams)} streams; "
                f"max is {WIRE_MAX_FLOWS}"
            )

    return list(recv_groups.values()), list(send_groups.values())


def parse_wire_to_log(recv_log: Path) -> Dict[int, int]:
    """Map sender wire flow_id -> mapped log id from open lines."""
    mapping: Dict[int, int] = {}
    if not recv_log.is_file():
        return mapping
    # udp-recv: opened flow <mapped> (wire flow <sender_id>) -> <path>
    pat = re.compile(
        r"udp-recv:\s+opened\s+flow\s+(\d+)\s+\(wire\s+flow\s+(\d+)\)"
    )
    for line in recv_log.read_text(errors="replace").splitlines():
        m = pat.search(line)
        if m:
            mapping[int(m.group(2))] = int(m.group(1))
    return mapping


def flow_output_path(log_path: Path, flow_id: int) -> str:
    if not log_path.is_file():
        return ""
    for line in log_path.read_text(errors="replace").splitlines():
        parts = line.split()
        if len(parts) >= 4 and parts[0] == "udp-recv:" and parts[1] == "flow" and parts[2] == str(flow_id):
            for tok in parts:
                if tok.startswith("output="):
                    return tok.split("=", 1)[1]
    return ""


def flow_incomplete_field(log_path: Path, flow_id: int, key: str) -> str:
    if not log_path.is_file():
        return "NA"
    value = "NA"
    for line in log_path.read_text(errors="replace").splitlines():
        parts = line.split()
        if (
            len(parts) >= 5
            and parts[0] == "udp-recv:"
            and parts[1] == "flow"
            and parts[2] == str(flow_id)
            and parts[3] == "incomplete:"
        ):
            for tok in parts[4:]:
                if "=" in tok:
                    k, v = tok.split("=", 1)
                    if k == key:
                        value = v
    return value


def nic_peak_avg(csv_path: Path) -> Tuple[str, str]:
    """Match multiflow monitor_mbps_summary: peak/avg of max(rx,tx) Mbps."""
    if not csv_path.is_file():
        return "NA", "NA"
    peaks: List[float] = []
    active: List[float] = []
    try:
        with csv_path.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("iface") == "__cpu__":
                    continue
                rx = float(row.get("rx_bps") or 0)
                tx = float(row.get("tx_bps") or 0)
                m = max(rx, tx) / 1e6
                peaks.append(m)
                if m >= 1.0:
                    active.append(m)
    except OSError:
        return "NA", "NA"
    if not peaks:
        return "NA", "NA"
    peak = max(peaks)
    avg = (sum(active) / len(active)) if active else 0.0
    return f"{peak:.1f}", f"{avg:.1f}"


class Orchestrator:
    def __init__(
        self,
        cfg_path: Path,
        result_dir: Path,
        repo_root: Path,
        bin_path: Path,
        monitor_py: Path,
    ):
        self.cfg_path = cfg_path
        self.result_dir = result_dir
        self.repo_root = repo_root
        self.bin_path = bin_path
        self.monitor_py = monitor_py
        self.cfg = load_config(cfg_path)
        self.nodes, self.streams, self.defaults = validate_and_build(self.cfg, repo_root)
        self.port_base = int(self.defaults.get("port_base", 9200))
        self.idle_sec = int(self.defaults.get("idle_sec", 20))
        self.monitor = bool(self.defaults.get("monitor", True))
        self.keep_remote = bool(self.defaults.get("keep_remote_output", True))
        self.monitor_hz = float(self.defaults.get("monitor_hz", 1))
        self.fetch_output = bool(self.defaults.get("fetch_output", False))
        self.recv_groups, self.send_groups = assign_ports_and_groups(
            self.streams, self.port_base
        )
        self.monitor_pids: List[Tuple[str, str, str, Path]] = []
        self.stamp = result_dir.name.replace("wire-stress-", "", 1)
        if self.stamp == result_dir.name:
            self.stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        for rg in self.recv_groups:
            max_eta = self.idle_sec
            for st in rg.streams:
                max_eta = max(max_eta, eta_seconds(st.src_bytes, st.rate_mbps, self.idle_sec))
            rg.idle_sec = max_eta

    def setup_dirs(self) -> None:
        for sub in ("logs", "monitor", "payloads", "out"):
            (self.result_dir / sub).mkdir(parents=True, exist_ok=True)
        shutil.copy2(self.cfg_path, self.result_dir / f"config{self.cfg_path.suffix}")

    def stage_remote_sender_files(self) -> None:
        for st in self.streams:
            node = self.nodes[st.from_node]
            if node.is_local:
                for sg in self.send_groups:
                    if st in sg.streams:
                        sg.staged_files[st.id] = st.file
                continue

            local = Path(st.file)
            if not local.is_file():
                alt = self.repo_root / st.file
                if alt.is_file():
                    local = alt

            remote_payload_dir = (
                f"{node.remote_repo}/build/wire-stress-{self.stamp}/payloads"
            )
            subprocess.run(
                ssh_cmd(node.ssh, f"mkdir -p {shlex.quote(remote_payload_dir)}"),
                check=True,
            )

            if local.is_file():
                st.src_hash = sha256_file(local)
                st.src_bytes = file_size(local)
                st.file_ext = file_ext_of(str(local))
                remote_path = (
                    f"{remote_payload_dir}/s{st.codec}_{st.id}_{local.name}"
                )
                log(f"  scp {local.name} -> {node.ssh}:{remote_path}")
                scp_to(str(local), node.ssh, remote_path)
                st.file = remote_path
            else:
                remote_path = st.file
                if not remote_path.startswith("/"):
                    remote_path = f"{node.remote_repo}/{st.file}"
                h = remote_sha256(node.ssh, remote_path)
                if not h:
                    die(f"stream {st.id}: cannot find file on {node.ssh}: {st.file}")
                st.src_hash = h
                st.file = remote_path
                st.file_ext = file_ext_of(remote_path)
                r = subprocess.run(
                    ssh_cmd(node.ssh, f"stat -c%s {shlex.quote(remote_path)}"),
                    capture_output=True,
                    text=True,
                )
                try:
                    st.src_bytes = int((r.stdout or "0").strip())
                except ValueError:
                    st.src_bytes = 0

            for sg in self.send_groups:
                if st in sg.streams:
                    sg.staged_files[st.id] = st.file

        # Recompute idle after sizes known for remote-only files
        for rg in self.recv_groups:
            max_eta = self.idle_sec
            for st in rg.streams:
                max_eta = max(max_eta, eta_seconds(st.src_bytes, st.rate_mbps, self.idle_sec))
            rg.idle_sec = max_eta

    def start_monitors(self) -> None:
        if not self.monitor or not self.monitor_py.is_file():
            return
        for name, node in self.nodes.items():
            if node.is_local or not node.monitor_ifaces:
                continue
            remote_csv = f"/tmp/wire-stress-mon-{self.stamp}-{name}.csv"
            local_csv = self.result_dir / "monitor" / f"{name}.csv"
            try:
                scp_to(str(self.monitor_py), node.ssh, "/tmp/iperf_like_monitor.py")
            except subprocess.CalledProcessError:
                print(f"  warn: could not scp monitor to {name}", file=sys.stderr)
                continue
            # Detach stdin so SSH does not block waiting on the background job.
            cmd = (
                f"rm -f {shlex.quote(remote_csv)}; "
                f"nohup python3 /tmp/iperf_like_monitor.py "
                f"{shlex.quote(node.monitor_ifaces)} {self.monitor_hz} "
                f"{shlex.quote(remote_csv)} </dev/null >/dev/null 2>&1 & echo $!"
            )
            r = subprocess.run(ssh_cmd(node.ssh, cmd), capture_output=True, text=True)
            pid = (r.stdout or "").strip().splitlines()[-1] if r.stdout else ""
            if pid.isdigit():
                self.monitor_pids.append((node.ssh, pid, remote_csv, local_csv))
                log(f"  monitor {name} pid={pid}")

    def stop_monitors(self) -> None:
        for host, pid, remote_csv, local_csv in self.monitor_pids:
            subprocess.run(
                ssh_cmd(
                    host,
                    f"kill {pid} 2>/dev/null || true; sleep 0.3; "
                    f"kill -9 {pid} 2>/dev/null || true",
                ),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            scp_from(host, remote_csv, str(local_csv))
            subprocess.run(
                ssh_cmd(host, f"rm -f {shlex.quote(remote_csv)}"),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

    def start_receivers(self) -> None:
        # Remote receivers: keep SSH in the foreground (backgrounded locally),
        # same pattern as run_wire_multiflow_matrix.sh. Avoids SSH hanging on
        # nohup/background jobs that still hold the session.
        for rg in self.recv_groups:
            node = self.nodes[rg.to_node]
            prefix_name = "out_"
            suffix_args: List[str] = []
            for st in sorted(rg.streams, key=lambda x: x.id):
                suffix_args.extend(["--out-suffix", f"{st.id}:{st.file_ext}"])

            local_log = (
                self.result_dir / "logs" / f"recv_{rg.to_node}_{rg.codec}_{rg.port}.log"
            )
            rg.log_path = str(local_log)
            logf = open(local_log, "w", encoding="utf-8")

            if node.is_local:
                out_dir = self.result_dir / "out" / f"{rg.to_node}_{rg.codec}_{rg.port}"
                out_dir.mkdir(parents=True, exist_ok=True)
                rg.remote_dir = str(out_dir)
                rg.prefix = str(out_dir / prefix_name)
                cmd = [
                    str(self.bin_path),
                    "--codec",
                    rg.codec,
                    "--lock-memory",
                    "--udp-recv",
                    str(rg.port),
                    rg.prefix,
                    "--max-flows",
                    str(len(rg.streams)),
                    "--idle-sec",
                    str(rg.idle_sec),
                    *suffix_args,
                ]
                log(
                    f"  recv local: port={rg.port} codec={rg.codec} "
                    f"flows={[s.id for s in rg.streams]} idle={rg.idle_sec}"
                )
                rg.local_proc = subprocess.Popen(
                    cmd, stdout=logf, stderr=subprocess.STDOUT, cwd=str(self.repo_root)
                )
            else:
                remote_dir = (
                    f"{node.remote_repo}/build/wire-stress-{self.stamp}/"
                    f"out_{rg.to_node}_{rg.codec}_{rg.port}"
                )
                rg.remote_dir = remote_dir
                rg.prefix = f"{remote_dir}/{prefix_name}"
                bin_rel = "./build/wg_multi_pipeline"
                suffix_sh = " ".join(
                    f"--out-suffix {shlex.quote(f'{st.id}:{st.file_ext}')}"
                    for st in sorted(rg.streams, key=lambda x: x.id)
                )
                remote_cmd = (
                    f"cd {shlex.quote(node.remote_repo)} && "
                    f"rm -rf {shlex.quote(remote_dir)} && "
                    f"mkdir -p {shlex.quote(remote_dir)} && "
                    f"exec {bin_rel} --codec {shlex.quote(rg.codec)} --lock-memory "
                    f"--udp-recv {rg.port} {shlex.quote(rg.prefix)} "
                    f"--max-flows {len(rg.streams)} --idle-sec {rg.idle_sec} "
                    f"{suffix_sh}"
                )
                log(
                    f"  recv {node.ssh}: port={rg.port} codec={rg.codec} "
                    f"flows={[s.id for s in rg.streams]} idle={rg.idle_sec}"
                )
                rg.local_proc = subprocess.Popen(
                    ssh_cmd(node.ssh, remote_cmd),
                    stdout=logf,
                    stderr=subprocess.STDOUT,
                )
            rg.pid = rg.local_proc.pid

        time.sleep(1.0)
        for rg in self.recv_groups:
            if rg.local_proc is not None and rg.local_proc.poll() is not None:
                die(
                    f"receiver exited early ({rg.key}, rc={rg.local_proc.returncode}); "
                    f"see {rg.log_path}"
                )
        log("  receivers are up")

    def start_senders(self) -> None:
        for sg in self.send_groups:
            node = self.nodes[sg.from_node]
            slog = self.result_dir / "logs" / (
                f"send_{sg.from_node}_{sg.codec}_{sg.dest_ip}_{sg.dest_port}.log"
            )
            sg.log_path = str(slog)
            logf = open(slog, "w", encoding="utf-8")

            flow_args: List[str] = []
            for st in sorted(sg.streams, key=lambda x: x.id):
                path = sg.staged_files.get(st.id, st.file)
                # --flow id:host:port:path:rate
                spec = f"{st.id}:{sg.dest_ip}:{sg.dest_port}:{path}:{st.rate_mbps:g}"
                flow_args.extend(["--flow", spec])

            if node.is_local:
                cmd = [
                    str(self.bin_path),
                    "--codec",
                    sg.codec,
                    "--udp-send-multi",
                    *flow_args,
                ]
                log(
                    f"  send local: codec={sg.codec} -> {sg.dest_ip}:{sg.dest_port} "
                    f"flows={[s.id for s in sg.streams]}"
                )
                sg.local_proc = subprocess.Popen(
                    cmd, stdout=logf, stderr=subprocess.STDOUT, cwd=str(self.repo_root)
                )
            else:
                bin_rel = "./build/wg_multi_pipeline"
                flow_parts = []
                for st in sorted(sg.streams, key=lambda x: x.id):
                    path = sg.staged_files.get(st.id, st.file)
                    spec = f"{st.id}:{sg.dest_ip}:{sg.dest_port}:{path}:{st.rate_mbps:g}"
                    flow_parts.extend(["--flow", shlex.quote(spec)])
                remote_cmd = (
                    f"cd {shlex.quote(node.remote_repo)} && "
                    f"exec {bin_rel} --codec {shlex.quote(sg.codec)} --udp-send-multi "
                    f"{' '.join(flow_parts)}"
                )
                log(
                    f"  send {node.ssh}: codec={sg.codec} -> {sg.dest_ip}:{sg.dest_port} "
                    f"flows={[s.id for s in sg.streams]}"
                )
                sg.local_proc = subprocess.Popen(
                    ssh_cmd(node.ssh, remote_cmd),
                    stdout=logf,
                    stderr=subprocess.STDOUT,
                )
            sg.pid = sg.local_proc.pid

    def wait_senders(self) -> None:
        max_wait = self.idle_sec + 60
        for st in self.streams:
            max_wait = max(max_wait, eta_seconds(st.src_bytes, st.rate_mbps, self.idle_sec))
        for rg in self.recv_groups:
            max_wait = max(max_wait, rg.idle_sec + 30)
        log(f"  waiting up to {max_wait}s for senders...")
        deadline = time.time() + max_wait
        pending = list(self.send_groups)
        while pending and time.time() < deadline:
            still = []
            for sg in pending:
                if sg.local_proc is not None and sg.local_proc.poll() is None:
                    still.append(sg)
            pending = still
            if pending:
                time.sleep(2)
        for sg in pending:
            log(f"  warn: sender still running, killing {sg.key}")
            if sg.local_proc is not None:
                sg.local_proc.send_signal(signal.SIGTERM)
                try:
                    sg.local_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    sg.local_proc.kill()

        # Wait for receivers to idle-exit
        log("  waiting for receivers to idle-exit...")
        recv_deadline = time.time() + max(rg.idle_sec for rg in self.recv_groups) + 30
        pending_r = list(self.recv_groups)
        while pending_r and time.time() < recv_deadline:
            still = []
            for rg in pending_r:
                if rg.local_proc is not None and rg.local_proc.poll() is None:
                    still.append(rg)
            pending_r = still
            if pending_r:
                time.sleep(2)

    def stop_receivers(self) -> None:
        for rg in self.recv_groups:
            if rg.local_proc is not None and rg.local_proc.poll() is None:
                rg.local_proc.send_signal(signal.SIGTERM)
                try:
                    rg.local_proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    rg.local_proc.kill()

        for sg in self.send_groups:
            if sg.local_proc is not None and sg.local_proc.poll() is None:
                sg.local_proc.send_signal(signal.SIGTERM)
                try:
                    sg.local_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    sg.local_proc.kill()

    def validate_streams(self) -> None:
        for rg in self.recv_groups:
            node = self.nodes[rg.to_node]
            log_path = Path(rg.log_path)
            wire_map = parse_wire_to_log(log_path)

            for st in rg.streams:
                wire_id = st.id
                log_fid = wire_map.get(wire_id, wire_id)
                st.log_fid = str(log_fid)

                found_hash = ""
                found_path = ""
                log_out = flow_output_path(log_path, log_fid)

                if node.is_local:
                    if log_out and Path(log_out).is_file():
                        found_hash = sha256_file(Path(log_out))
                        found_path = log_out
                    if not found_hash or found_hash != st.src_hash:
                        out_dir = Path(rg.remote_dir)
                        if out_dir.is_dir():
                            for p in out_dir.iterdir():
                                if p.is_file() and sha256_file(p) == st.src_hash:
                                    found_hash = st.src_hash
                                    found_path = str(p)
                                    break
                else:
                    if log_out:
                        h = remote_sha256(node.ssh, log_out)
                        if h:
                            found_hash = h
                            found_path = log_out
                    if found_hash != st.src_hash:
                        r = subprocess.run(
                            ssh_cmd(
                                node.ssh,
                                f"ls -1 {shlex.quote(rg.remote_dir)} 2>/dev/null",
                            ),
                            capture_output=True,
                            text=True,
                        )
                        for name in (r.stdout or "").splitlines():
                            name = name.strip()
                            if not name or name == "recv.log":
                                continue
                            path = f"{rg.remote_dir}/{name}"
                            h = remote_sha256(node.ssh, path)
                            if h == st.src_hash:
                                found_hash = h
                                found_path = path
                                break

                st.recv_hash = found_hash
                miss = flow_incomplete_field(log_path, log_fid, "missing_groups")
                st.missing_groups = miss
                st.incomplete = miss not in ("NA", "0", "")

                if found_hash and found_hash == st.src_hash:
                    st.status = "PASS"
                    st.note = Path(found_path).name if found_path else "hash_ok"
                elif found_hash:
                    st.status = "FAIL"
                    st.note = f"hash_mismatch got={found_hash[:12]}..."
                elif st.incomplete:
                    st.status = "FAIL"
                    st.note = f"incomplete missing_groups={miss}"
                else:
                    st.status = "FAIL"
                    st.note = "output_missing"

                if self.fetch_output and found_path and not node.is_local:
                    dest = self.result_dir / "out" / f"s{st.id}_{Path(found_path).name}"
                    scp_from(node.ssh, found_path, str(dest))

            if not self.keep_remote and not node.is_local:
                subprocess.run(
                    ssh_cmd(node.ssh, f"rm -rf {shlex.quote(rg.remote_dir)}"),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )

    def write_report(self) -> int:
        csv_path = self.result_dir / "streams.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "id",
                    "from",
                    "to",
                    "codec",
                    "rate_mbps",
                    "file",
                    "bytes",
                    "src_sha256",
                    "recv_sha256",
                    "dest_ip",
                    "dest_port",
                    "status",
                    "missing_groups",
                    "note",
                ]
            )
            for st in sorted(self.streams, key=lambda s: s.id):
                w.writerow(
                    [
                        st.id,
                        st.from_node,
                        st.to_node,
                        st.codec,
                        st.rate_mbps,
                        Path(st.file).name,
                        st.src_bytes,
                        st.src_hash,
                        st.recv_hash,
                        st.dest_ip,
                        st.dest_port,
                        st.status,
                        st.missing_groups,
                        st.note,
                    ]
                )

        mon_lines = []
        for name in sorted(self.nodes):
            csvf = self.result_dir / "monitor" / f"{name}.csv"
            if csvf.is_file():
                peak, avg = nic_peak_avg(csvf)
                mon_lines.append(f"- **{name}**: peak {peak} Mbps, avg {avg} Mbps")

        n_pass = sum(1 for s in self.streams if s.status == "PASS")
        n_fail = len(self.streams) - n_pass
        md = self.result_dir / "results.md"
        lines = [
            "# Wire stress results",
            "",
            f"- Config: `{self.cfg_path}`",
            f"- Result dir: `{self.result_dir}`",
            f"- Streams: {len(self.streams)} ({n_pass} PASS / {n_fail} FAIL)",
            f"- Receiver groups: {len(self.recv_groups)} (one process per to+codec)",
            f"- Sender groups: {len(self.send_groups)}",
            f"- idle_sec_base={self.idle_sec} port_base={self.port_base}",
            "",
            "## Relay NIC (optional)",
            "",
        ]
        if mon_lines:
            lines.extend(mon_lines)
        else:
            lines.append("- (no monitor data)")
        lines.extend(
            [
                "",
                "## Streams",
                "",
                "| id | from→to | codec | Mbps | bytes | status | note |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for st in sorted(self.streams, key=lambda s: s.id):
            lines.append(
                f"| {st.id} | {st.from_node}→{st.to_node} | {st.codec} | "
                f"{st.rate_mbps} | {human_bytes(st.src_bytes)} | {st.status} | {st.note} |"
            )
        lines.extend(["", "## Process groups", "", "### Receivers", ""])
        for rg in self.recv_groups:
            ids = ",".join(str(s.id) for s in rg.streams)
            lines.append(
                f"- `{rg.to_node}` codec=`{rg.codec}` port=`{rg.port}` "
                f"idle={rg.idle_sec}s flows=[{ids}]"
            )
        lines.extend(["", "### Senders", ""])
        for sg in self.send_groups:
            ids = ",".join(str(s.id) for s in sg.streams)
            lines.append(
                f"- `{sg.from_node}` codec=`{sg.codec}` → "
                f"`{sg.dest_ip}:{sg.dest_port}` flows=[{ids}]"
            )
        md.write_text("\n".join(lines) + "\n", encoding="utf-8")
        log(f"Wrote {md}")
        log(f"Wrote {csv_path}")
        log(f"Summary: {n_pass} PASS / {n_fail} FAIL")
        return n_fail

    def run(self) -> int:
        log(f"Config: {self.cfg_path}")
        log(f"Result: {self.result_dir}")
        log(
            f"Streams: {len(self.streams)}  recv_groups: {len(self.recv_groups)}  "
            f"send_groups: {len(self.send_groups)}"
        )
        self.setup_dirs()
        log("Staging sender files...")
        self.stage_remote_sender_files()
        log("Starting relay monitors...")
        self.start_monitors()
        try:
            log("Starting receivers...")
            self.start_receivers()
            log("Starting senders...")
            self.start_senders()
            self.wait_senders()
        finally:
            log("Stopping receivers / fetching logs...")
            self.stop_receivers()
            log("Stopping monitors...")
            self.stop_monitors()
        log("Validating SHA256...")
        self.validate_streams()
        n_fail = self.write_report()
        return 1 if n_fail else 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Configurable wire multi-stream stress test")
    ap.add_argument("config", type=Path, help="YAML or JSON config")
    ap.add_argument("--result-dir", type=Path, default=None)
    ap.add_argument("--repo-root", type=Path, default=None)
    args = ap.parse_args()

    script_dir = Path(__file__).resolve().parent
    repo_root = (args.repo_root or script_dir.parent).resolve()
    cfg = args.config.resolve()
    if not cfg.is_file():
        die(f"config not found: {cfg}")

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    result_dir = args.result_dir or (repo_root / "build" / f"wire-stress-{stamp}")
    result_dir = result_dir.resolve()
    result_dir.mkdir(parents=True, exist_ok=True)

    bin_path = repo_root / "build" / "wg_multi_pipeline"
    if not bin_path.is_file():
        die(f"binary not found: {bin_path} (run: make wg-demo)")

    monitor_py = script_dir / "iperf_like_monitor.py"
    orch = Orchestrator(cfg, result_dir, repo_root, bin_path, monitor_py)
    raise SystemExit(orch.run())


if __name__ == "__main__":
    main()
