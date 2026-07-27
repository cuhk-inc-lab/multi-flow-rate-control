# Script Guide

This document explains what each shell script in `scripts/` does, when to use
it, and the most common command forms.

## Prerequisites

- Build app binary first when running wire scripts:
  - `make wg-demo`
- Keep repositories side-by-side:
  - `multi-flow-rate-control`
  - `buffer-management-module`
- For cross-VM scripts, Node1 must have SSH key access to remote nodes.

## Quick Script Map

| Script | Purpose | Typical Use |
| --- | --- | --- |
| `scripts/run_vm_baseline.sh` | Single sender/receiver `iperf3` TCP+UDP baseline | Verify raw path capacity/loss before app tests |
| `scripts/run_wire_matrix.sh` | Single-flow codec×rate matrix (Node1 -> Node4) | Find passing bitrate per codec |
| `scripts/run_wire_multiflow_matrix.sh` | Multi-flow codec×rate matrix with per-flow checks | Validate concurrent flow behavior and relay load |
| `scripts/encode_multibitrate.sh` | Generate `input_1m.ts` / `input_10m.ts` / `input_20m.ts` | Prepare demo sources |
| `scripts/run_dual_fifo.sh` | Live 3-stream FIFO demo with ffplay windows | Visual local demo of multi-flow processing |

Helper (kept for multiflow relay NIC sampling):

| Script | Purpose |
| --- | --- |
| `scripts/iperf_like_monitor.py` | Sample NIC/CPU timeseries; used by multiflow matrix |

## Script Details

### `run_vm_baseline.sh`

**What it does**
- Runs one TCP `iperf3` test and a UDP rate sweep to one receiver.
- Prints traceroute/tracepath first when available.

**Usage**
- `sh scripts/run_vm_baseline.sh RECEIVER_IP [SECONDS]`

**Key env**
- `UDP_RATES` (default: `100M 250M 500M 750M 1G`)

---

### `run_wire_matrix.sh`

**What it does**
- For each codec and bitrate:
  - starts receiver on Node4
  - sends one file from Node1
  - validates remote SHA-256
  - records counters/latency in markdown + CSV

**Usage**
- `CODECS="copy block xor-fec rs-fec" RATES="20 24 28 32" ./scripts/run_wire_matrix.sh RECEIVER_SSH RECEIVER_DATA_IP INPUT_FILE`

**Key env**
- `CODECS`, `RATES`
- `RECEIVER_REPO`
- `IDLE_SEC`, `PORT_BASE`
- `KEEP_REMOTE_OUTPUT` (`1` keep, `0` delete)
- `DECODE_MARK` (teaching mode)

---

### `run_wire_multiflow_matrix.sh`

**What it does**
- Runs concurrent `--udp-send-multi` tests from Node1 to Node4.
- Supports:
  - one local file per flow, or
  - synthesized payloads from one seed file
- Validates per-flow hashes on receiver side.
- Reports measured Node2/Node3 relay NIC Mbps.

**Usage**
- User files mode (recommended):
  - `CODECS="copy xor-fec" RATES="10 20" ./scripts/run_wire_multiflow_matrix.sh RECEIVER_SSH RECEIVER_DATA_IP file0 file1 ...`
- Seed mode:
  - `FLOWS=4 DURATION_S=10 RATES="10 20" ./scripts/run_wire_multiflow_matrix.sh RECEIVER_SSH RECEIVER_DATA_IP seed.ts`

**Key env**
- `CODECS`, `RATES`, `FLOWS`, `DURATION_S`
- `RECEIVER_REPO`, `IDLE_SEC`, `PORT_BASE`
- `FETCH_OUTPUT`, `KEEP_REMOTE_OUTPUT`
- `MONITOR_RELAYS`, `MONITOR_HZ`
- `NODE2_SSH`, `NODE3_SSH`
- `NODE2_IFACES`, `NODE3_IFACES`

---

### `encode_multibitrate.sh`

**What it does**
- Uses ffmpeg to generate:
  - `input_1m.ts`
  - `input_10m.ts`
  - `input_20m.ts`
- Forces 720p and aligned duration.

**Usage**
- `./scripts/encode_multibitrate.sh [source.ts_or_mp4] [out_dir]`

**Key env**
- `DURATION` (optional clip length)

---

### `run_dual_fifo.sh`

**What it does**
- Local live demo:
  - starts 3 ffplay windows
  - runs one `--multi` pipeline process
  - pushes 1M/10M/20M TS streams through FIFO pipes

**Usage**
- `./scripts/run_dual_fifo.sh [--codec block|copy|xor-fec|rs-fec|none] [dir_with_input_*m.ts]`

**Notes**
- Requires `ffmpeg` and `ffplay`.
- Requires `input_1m.ts`, `input_10m.ts`, `input_20m.ts` (see `encode_multibitrate.sh`).

## Suggested Workflow

1. Validate path baseline:
   - `run_vm_baseline.sh`
2. Validate app behavior:
   - `run_wire_matrix.sh` (single flow codec sweep)
   - `run_wire_multiflow_matrix.sh` (multi-flow)
3. Optional local demo:
   - `encode_multibitrate.sh` then `run_dual_fifo.sh`
