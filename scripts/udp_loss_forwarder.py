#!/usr/bin/env python3
"""Userspace UDP forwarder with independent per-datagram loss (tc substitute).

Each received datagram is dropped or forwarded unchanged with probability --loss.
Use for controlled RS tolerance experiments without root/tc.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import signal
import socket
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO


BUF = 65535


@dataclass
class Stats:
    received: int = 0
    forwarded: int = 0
    dropped: int = 0
    started_at: float = field(default_factory=time.monotonic)

    @property
    def actual_loss_pct(self) -> float:
        if self.received <= 0:
            return 0.0
        return 100.0 * self.dropped / self.received


class Forwarder:
    def __init__(
        self,
        listen_host: str,
        listen_port: int,
        forward_host: str,
        forward_port: int,
        loss: float,
        seed: int,
        summary_json: Path | None,
        summary_csv: Path | None,
    ) -> None:
        if not 0.0 <= loss <= 1.0:
            raise ValueError("loss must be in [0.0, 1.0]")
        self.listen_addr = (listen_host, listen_port)
        self.forward_addr = (forward_host, forward_port)
        self.loss = loss
        self.seed = seed
        self.rng = random.Random(seed)
        self.stats = Stats()
        self.summary_json = summary_json
        self.summary_csv = summary_csv
        self._stop = False

        self.rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.rx.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 * 1024 * 1024)
        self.rx.bind(self.listen_addr)

        self.tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.tx.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8 * 1024 * 1024)

    def stop(self, _signum: int | None = None, _frame=None) -> None:
        self._stop = True

    def run(self) -> None:
        print(
            f"udp_loss_forwarder: listen {self.listen_addr[0]}:{self.listen_addr[1]} "
            f"-> {self.forward_addr[0]}:{self.forward_addr[1]} "
            f"loss={self.loss:.4f} seed={self.seed}",
            flush=True,
        )
        self.rx.settimeout(1.0)
        while not self._stop:
            try:
                data, _addr = self.rx.recvfrom(BUF)
            except (TimeoutError, socket.timeout):
                continue
            except InterruptedError:
                break
            self.stats.received += 1
            if self.rng.random() < self.loss:
                self.stats.dropped += 1
            else:
                self.tx.sendto(data, self.forward_addr)
                self.stats.forwarded += 1
            if self.stats.received % 10000 == 0:
                self._print_progress()

    def _print_progress(self) -> None:
        elapsed = max(time.monotonic() - self.stats.started_at, 1e-6)
        print(
            f"  recv={self.stats.received} fwd={self.stats.forwarded} "
            f"drop={self.stats.dropped} actual_loss={self.stats.actual_loss_pct:.2f}% "
            f"pps={self.stats.received / elapsed:.0f}",
            flush=True,
        )

    def write_summary(self) -> dict:
        elapsed = max(time.monotonic() - self.stats.started_at, 0.0)
        summary = {
            "listen_host": self.listen_addr[0],
            "listen_port": self.listen_addr[1],
            "forward_host": self.forward_addr[0],
            "forward_port": self.forward_addr[1],
            "configured_loss": self.loss,
            "configured_loss_pct": self.loss * 100.0,
            "received": self.stats.received,
            "forwarded": self.stats.forwarded,
            "dropped": self.stats.dropped,
            "actual_loss_pct": self.stats.actual_loss_pct,
            "elapsed_sec": elapsed,
        }
        if self.summary_json is not None:
            self.summary_json.parent.mkdir(parents=True, exist_ok=True)
            self.summary_json.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        if self.summary_csv is not None:
            self.summary_csv.parent.mkdir(parents=True, exist_ok=True)
            write_header = not self.summary_csv.exists() or self.summary_csv.stat().st_size == 0
            with self.summary_csv.open("a", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(summary.keys()))
                if write_header:
                    writer.writeheader()
                writer.writerow(summary)
        return summary


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--forward-host", default="127.0.0.1")
    parser.add_argument("--forward-port", type=int, required=True)
    parser.add_argument(
        "--loss",
        type=float,
        required=True,
        help="drop probability per datagram in [0.0, 1.0]",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--summary-json", type=Path, default=None)
    parser.add_argument("--summary-csv", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    forwarder = Forwarder(
        listen_host=args.listen_host,
        listen_port=args.listen_port,
        forward_host=args.forward_host,
        forward_port=args.forward_port,
        loss=args.loss,
        seed=args.seed,
        summary_json=args.summary_json,
        summary_csv=args.summary_csv,
    )
    signal.signal(signal.SIGINT, forwarder.stop)
    signal.signal(signal.SIGTERM, forwarder.stop)
    try:
        forwarder.run()
    finally:
        summary = forwarder.write_summary()
        print(
            "udp_loss_forwarder: summary "
            f"recv={summary['received']} fwd={summary['forwarded']} "
            f"drop={summary['dropped']} actual_loss={summary['actual_loss_pct']:.4f}%",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
