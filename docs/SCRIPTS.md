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
| `scripts/check_wire_multi_flow.sh` | Fast smoke test for `--udp-send-multi` | Quick PASS/FAIL sanity check |
| `scripts/run_iperf_like_wire.sh` | 6 concurrent app streams across Node1/2/3/4 | Realistic multi-node stress + charts |
| `scripts/run_iperf_like_matrix.sh` | Sweep codec/rate over `run_iperf_like_wire.sh` | Batch comparison of many app runs |
| `scripts/run_iperf_like_baseline.sh` | 6 concurrent `iperf3` control run | Distinguish path loss from app/codec loss |
| `scripts/encode_multibitrate.sh` | Generate `input_1m.ts` / `input_10m.ts` / `input_20m.ts` | Prepare demo sources |
| `scripts/run_dual_fifo.sh` | Live 3-stream FIFO demo with ffplay windows | Visual local demo of multi-flow processing |

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
- Reports estimated link Mbps and measured Node2/Node3 relay NIC Mbps.

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

### `check_wire_multi_flow.sh`

**What it does**
- Creates 3 random payloads and runs one multi-flow sender process.
- Validates that all three hashes are recovered.
- Works local loopback or remote receiver mode.

**Usage**
- Local:
  - `./scripts/check_wire_multi_flow.sh`
- Remote Node4:
  - `./scripts/check_wire_multi_flow.sh --remote RECEIVER_SSH RECEIVER_DATA_IP`

**Key env**
- `CODEC` (default `copy`)
- `PORT`
- `REMOTE_REPO`

---

### `run_iperf_like_wire.sh`

**What it does**
- Runs 6 concurrent application streams across Node1/2/3/4.
- Collects stream stats, latency, recovery, and per-node NIC/CPU monitor data.
- Generates `report.md` and SVG charts via `scripts/iperf_like_report.py`.

**Usage**
- `NODE2_SSH=... NODE2_IP=... NODE3_SSH=... NODE3_IP=... NODE4_SSH=... NODE4_IP=... ./scripts/run_iperf_like_wire.sh INPUT_FILE`

**Key env**
- Required: `NODE2_SSH`, `NODE2_IP`, `NODE3_SSH`, `NODE3_IP`, `NODE4_SSH`, `NODE4_IP`
- Core knobs: `CODEC`, `RATE_MBPS` or `RATE_S1..RATE_S6`
- Timing: `DURATION_S`, `DURATION_SHORT_S`, `IDLE_SEC`, `BARRIER_SEC`
- Networking: `PORT`, `LOOP_PORT`
- Monitoring: `MONITOR_HZ`, `NODE*_IFACES`

---

### `run_iperf_like_matrix.sh`

**What it does**
- Repeats `run_iperf_like_wire.sh` across codec×rate combinations.
- Produces matrix-level markdown/CSV summary with relay peak counters.

**Usage**
- `CODECS="copy xor-fec" RATES="1 2" NODE2_SSH=... NODE2_IP=... NODE3_SSH=... NODE3_IP=... NODE4_SSH=... NODE4_IP=... ./scripts/run_iperf_like_matrix.sh INPUT_FILE`

**Key env**
- `CODECS`, `RATES`
- same required node vars as `run_iperf_like_wire.sh`
- `EXTRA_ENV` to pass extra knobs into each run

---

### `run_iperf_like_baseline.sh`

**What it does**
- Runs the same 6-stream topology as iperf-like wire script, but using `iperf3`.
- Serves as control experiment:
  - baseline PASS + wire FAIL -> app/codec path issue likely
  - baseline FAIL -> network path issue likely

**Usage**
- `NODE2_SSH=... NODE2_IP=... NODE3_SSH=... NODE3_IP=... NODE4_SSH=... NODE4_IP=... ./scripts/run_iperf_like_baseline.sh`

**Key env**
- `RATE_MBPS` or `RATE_S1..RATE_S6`
- `DURATION_S`, `DURATION_SHORT_S`
- `BASE_PORT`, `LOSS_MAX_PCT`
- `MONITOR_HZ`, `NODE*_IFACES`

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
   - `run_vm_baseline.sh` (single path)
   - `run_iperf_like_baseline.sh` (6-stream topology)
2. Validate app behavior:
   - `run_wire_matrix.sh` (single flow codec sweep)
   - `run_wire_multiflow_matrix.sh` (multi-flow)
3. Stress realistic topology:
   - `run_iperf_like_wire.sh`
   - `run_iperf_like_matrix.sh`

