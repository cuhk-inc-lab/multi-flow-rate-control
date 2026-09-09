# RS compute-penalty benchmark (P1)

Quantify Reed-Solomon profile cost on the **Node1 → Node2** Wi‑Fi path
(and optionally loopback for regression), without changing C/C++, RS
arithmetic, the decoder, wire header, pacing, or existing tests.

## Topology (main path)

```text
Control host (orchestrator)
  ├─ SSH Node1 fyp1@10.10.10.161  sender  station0=10.10.12.1
  └─ SSH Node2 fyp1@10.10.10.162  receiver ap0=10.10.12.2
```

| Mode | Path | Ports |
| --- | --- | --- |
| A/B (`cross_vm_direct`) | Node1 → `10.10.12.2:9200` | recv=9200 |
| C (`cross_vm_fwd_n2`) | Node1 → `10.10.12.2:9210` → lo:`127.0.0.1:9200` | fwd≠recv |
| `--path loopback` | local 127.0.0.1 regression | 19520 / 19620 |

Wire header for cross_vm: `--final-dst 2 --ttl 2`, receiver `--local-node-id 2`.

## Semantics (unchanged)

- `--flow …:rate` = **source/payload Mbps**
- Always `--udp-send-multi` + one flow (not `--udp-send`)
- `none` = sole no-coding baseline; `copy` appendix only
- `interleave_depth=1`; strict hash; receiver `--strict`
- Fixed wire-rate: `source = W * (k*1400)/(n*1444)`; `none`: `W*1400/1444`
- `wire_bytes_est = blocks * n * 1444 + 44` (estimate until P2)
- Hash FAIL → `successful_file_goodput=0`, `penalty=NA`
- Forwarder CPU is timed **separately** (not mixed into sender/receiver)

## Ready handshake

1. Start Node2 receiver; poll log for `udp-recv: listening`
2. (C) Start Node2 forwarder; poll `ss` for listen port
3. Start Node1 sender
4. Do not use fixed sleep as the sync mechanism

## Runner

```sh
# Cross-VM (default)
sh scripts/run_rs_compute_penalty.sh --experiment A|B|C \
  --input FILE [options]

# Loopback regression
sh scripts/run_rs_compute_penalty.sh --path loopback --experiment A ...
```

Defaults: `--path cross_vm`, `--sender-ssh fyp1@10.10.10.161`,
`--receiver-ssh fyp1@10.10.10.162`, `--send-host 10.10.12.2`,
`--recv-port 9200`, `--fwd-listen-port 9210`, `idle_sec=30`.

Allowed new files only: this runner, `parse_rs_compute_penalty.py`, this doc,
and result directories. Do not modify apps/, Makefile, tests/, or the forwarder.
