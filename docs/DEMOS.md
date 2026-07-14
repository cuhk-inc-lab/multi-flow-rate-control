# wg_multi_pipeline — Demo Guide

This document describes the three main **file/FIFO** demos for `wg_multi_pipeline`:
how to run them, what they prove, and how the code implements each path.

For library integration handoff (API boundaries), see
[INTEGRATION_BOUNDARIES.md](INTEGRATION_BOUNDARIES.md).  
For unit/integration test commands, see [tests/TESTING.md](../tests/TESTING.md).

---

## Build

```bash
cd /path/to/multi-flow-rate-control
make wg-demo    # → build/wg_multi_pipeline
```

Requires sibling repo `../buffer-management-module` (see root [README.md](../README.md)).

---

## Shared pipeline logic

All file/FIFO demos use the same per-flow order (**multi before encode** by default):

```
ingress (188-byte TS packets)
  → ingress_push(mgr, flow_id, …)
  → FlowManager
       MixedQueue → dispatcher → per-flow queue → (optional) paced worker
  → either:
       A) --no-codec relay: DataPacket* queue → FileDrain_write_packet → output
       B) default codec: pipe → CircularBuffers → BlockCodec encode/decode → FileDrain
```

**Relay (`--no-codec`)** is preferred for live `ffplay` demos (clean TS bytes).  
**Codec path** is for block-coding / offline `cmp` tests.
| Concept | Value / location |
|---------|------------------|
| TS packet size | 188 B — `PKG_SIZE` in `apps/wg_multi_pipeline/stream_config.h` |
| Decode block | 4 × 188 = 752 B (`DECODE_BLOCK`) |
| Encode block | 8 × 188 = 1504 B (`ENCODE_BLOCK`) |
| `flow_id` | 0 for the first `in out` pair, 1 for the second, … |
| Main loop | `apps/wg_multi_pipeline/pipeline.c` — `wg_pipeline_run()` |
| CLI | `apps/wg_multi_pipeline/main.c` |

**Flags**

| Flag | Meaning |
|------|---------|
| `--no-pace` | Disable timeline pacing; byte-exact roundtrip for `cmp` |
| `--multi` | Multiple `input output` pairs (required for 2+ offline/FIFO flows) |
| (default) | Pacing enabled — closer to live rate matching |

File extensions (`.ts`, `.bin`, `.txt`, …) are ignored; all inputs are raw bytes.
For MPEG-TS demos, inputs should be multiples of 188 bytes per packet.

---

## Demo 1 — Offline multi-file transfer

**Purpose:** Run several files through the full pipeline at once and verify each
output matches its input (`cmp`). This mocks production multi-flow routing with a
fixed `flow_id` per input path.

### Commands

**Automated (3 flows, random data):**

```bash
make integration-test
```

This generates `build/wg_test_in{0,1,2}.ts`, runs the pipeline, and `cmp`s outputs.

**Your own files (example: three inputs):**

```bash
./build/wg_multi_pipeline --no-pace --multi \
  input0.bin  output0.bin \
  input1.bin output1.bin \
  input2.bin output2.bin

cmp input0.bin  output0.bin && echo "flow 0 OK"
cmp input1.bin output1.bin && echo "flow 1 OK"
cmp input2.bin output2.bin && echo "flow 2 OK"
```

**Single file:**

```bash
./build/wg_multi_pipeline --no-pace input.bin output.bin
cmp input.bin output.bin
```

**Relay mode (`--no-codec`):** skips BlockCodec, pipe, and byte CircularBuffers.
After pacing, workers enqueue `DataPacket*` into a pointer-only queue; the main
thread writes payload bytes at drain time via `FileDrain_write_packet()`.

```bash
./build/wg_multi_pipeline --no-codec --no-pace input.bin output.bin
cmp input.bin output.bin
```

### Implementation notes

- `main.c` parses `in out` pairs; pair *i* gets `flow_id = i`.
- `pump_file_ingress()` reads **188-byte packets** (buffers partial `fread` at EOF).
- No FIFOs; `FileDrain` writes directly to regular output files.
- `--no-pace` is recommended so `cmp` is deterministic.

**Key code:** `wg_pipeline_run()` → `pump_file_ingress()` → `process_flow_post_multi()`.

---

## Demo 2 — Single-stream FIFO live (TS)

**Purpose:** Simulate live streaming: `ffmpeg` pushes MPEG-TS into a FIFO,
`wg_multi_pipeline` processes it, `ffplay` reads the output FIFO in real time.

### Prerequisites

- `ffmpeg`, `ffplay` (or VLC on the output FIFO)
- Input file e.g. `input1.ts` (or any `.ts` source)

### Startup order (critical)

FIFOs block until both reader and writer are connected. **Always:**

```
1. Player  (reads output FIFO)
2. Pipeline
3. ffmpeg  (writes input FIFO)   ← last
```

### Commands

**Terminal 1 — player (first):**

```bash
killall ffmpeg ffplay wg_multi_pipeline 2>/dev/null

rm -f /tmp/live_in.ts /tmp/live_out.ts
mkfifo /tmp/live_in.ts /tmp/live_out.ts

ffplay -loglevel error -f mpegts /tmp/live_out.ts
```

**Terminal 2 — pipeline:**

```bash
cd /path/to/multi-flow-rate-control
./build/wg_multi_pipeline /tmp/live_in.ts /tmp/live_out.ts
```

Use `--no-pace` only for capture/`cmp` tests, not for realistic live simulation.

**Terminal 3 — push source (last):**

```bash
ffmpeg -re -i input1.ts -c copy -f mpegts -y /tmp/live_in.ts
```

Video appears in the **ffplay window**, not in the pipeline terminal.

### Offline capture check (optional)

Replace `ffplay` with a file capture to verify bytes:

```bash
cat /tmp/live_out.ts > /tmp/captured.ts &
# … start pipeline and ffmpeg as above …
cmp input1.ts /tmp/captured.ts
```

### Implementation notes

- Output FIFO is opened in `init_flow_stage()` before returning (needs a reader).
- Input FIFO uses **lazy open** in `ensure_input_open()` when the first read is
  attempted — avoids blocking on `live_in` before `ffmpeg` starts.
- `drain_pipe_to_post_multi()` reads the pipe in **188-byte-aligned** chunks.
- `FileDrain_pull_once()` writes only full 188-byte TS packets during streaming;
  tail flush uses `FileDrain_flush_remainder()` at EOF.

**Key code:** `init_flow_stage()`, `ensure_input_open()`, `drain_pipe_to_post_multi()`.

---

## Demo 3 — Multi-bitrate FIFO live (1M / 10M / 20M)

**Purpose:** Three simultaneous MPEG-TS flows at clearly different bitrates
through one `wg_multi_pipeline --multi` process. Each flow has its own
input/output FIFO and `ffplay` window.

### Prepare sources (720p, same duration)

Do **not** only change bitrate on 4K — three soft-decoded 4K windows will stutter.
Encode 720p multi-bitrate files once:

```bash
cd /path/to/multi-flow-rate-control
# default source: input2.ts (or pass another .ts/.mp4)
./scripts/encode_multibitrate.sh
# → input_1m.ts  (~1 Mbps)
# → input_10m.ts (~10 Mbps)
# → input_20m.ts (~20 Mbps)
```

Optional: `DURATION=60 ./scripts/encode_multibitrate.sh source.mp4 .`

### Scripted demo

```bash
killall ffmpeg ffplay wg_multi_pipeline 2>/dev/null
cd /path/to/multi-flow-rate-control
make wg-demo
./scripts/run_dual_fifo.sh
```

What the script does:

1. Opens **three** `ffplay` windows (`1Mbps` / `10Mbps` / `20Mbps`)
2. Starts `./build/wg_multi_pipeline --no-pace --no-codec --multi …`
3. Pushes all three files with `ffmpeg -re` (realtime)

Behaviour:

| Action | Result |
|--------|--------|
| Close **one** player window | That flow’s output is dropped; **other windows keep playing** |
| Close **all three** windows | Demo shuts down (players / pushers / pipeline) |
| Let files finish | Natural end when all `ffmpeg -re` exits |
| `Ctrl+C` | Cleanup trap stops everything |

Why `--no-pace --no-codec` for live playback:

- `ffmpeg -re` already paces realtime; pipeline pacing on top causes stutter under 3-flow load
- BlockCodec alters/transfers via pipes; live `ffplay` needs clean TS bytes (`--no-codec` relay)

### Manual (same idea)

```bash
# Players → pipeline → ffmpeg (order matters)
./build/wg_multi_pipeline --no-pace --no-codec --multi \
  /tmp/live_in0.ts /tmp/live_out0.ts \
  /tmp/live_in1.ts /tmp/live_out1.ts \
  /tmp/live_in2.ts /tmp/live_out2.ts

ffmpeg -re -i input_1m.ts  -c copy -f mpegts -y /tmp/live_in0.ts &
ffmpeg -re -i input_10m.ts -c copy -f mpegts -y /tmp/live_in1.ts &
ffmpeg -re -i input_20m.ts -c copy -f mpegts -y /tmp/live_in2.ts &
```

### Implementation notes

- Multi-flow **workers run in parallel** (one pthread per flow + dispatcher).
- FIFO ingress uses non-blocking open/`read` so one stuck pipe does not freeze others.
- Named-pipe inputs use `O_RDWR|O_NONBLOCK` so writers can connect without deadlocking the main loop.
- Closing a viewer marks that flow `output_dead`; remaining flows continue (`SIGPIPE` ignored).
- All outputs closed → pipeline exits with `all outputs closed; exiting`.

**Key code:** `ensure_input_open()` / `pump_file_ingress()`, `process_flow_relay()`,
`mark_output_dead()`; `scripts/run_dual_fifo.sh`, `scripts/encode_multibitrate.sh`.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|----------------|-----|
| Pipeline hangs on start | Output FIFO has no reader | Start `ffplay` (or `cat > file`) **before** pipeline |
| Pipeline hangs in `--multi` | Input FIFO open blocked other flows (old bug) | Rebuild; current code uses non-blocking FIFO ingest |
| Only one window / others never start | 4K soft-decode overload or blocking FIFO open | Use 720p `input_*m.ts`; rebuild latest pipeline |
| Stutter with 3 live windows | Double pacing (`-re` + pipeline pacing) or codec path | Script uses `--no-pace --no-codec` |
| ffplay H264 “Missing reference” at start | Joined mid-GOP / low-delay | Benign for live FIFO; script quiets player logs |
| Close one window → all stop | Old: one `FileDrain` error aborted whole run | Rebuild; per-flow `output_dead` keeps others alive |
| Close all windows → process hangs | Script waited only on `ffmpeg` | Current script polls players and exits when all closed |
| `ffmpeg` / `ffplay` Stopped (`jobs`) | Previous FIFO clients stuck | `killall ffmpeg ffplay wg_multi_pipeline` |
| `cmp` fails on large files | Old buffer/alignment bugs | Use current `stream_config.h` sizes; `--no-pace` |

### Clean reset

```bash
killall ffmpeg ffplay wg_multi_pipeline 2>/dev/null
rm -f /tmp/live_in*.ts /tmp/live_out*.ts
```

---

## Quick reference

| Demo | Ingress | Output | Verify |
|------|---------|--------|--------|
| 1 Offline multi-file | Regular files | Regular files | `cmp` |
| 2 Single FIFO live | FIFO ← `ffmpeg -re` | FIFO → `ffplay` | Playback |
| 3 Multi-bitrate FIFO | 3 FIFOs ← 1M/10M/20M | 3 × `ffplay` | Playback + independent close |

---

## Source map

| File | Role |
|------|------|
| `apps/wg_multi_pipeline/main.c` | CLI: `--multi`, `--no-pace`, `--no-codec`; ignores `SIGPIPE` |
| `apps/wg_multi_pipeline/pipeline.c` | Main loop, non-blocking FIFO ingress, relay/`output_dead` |
| `apps/wg_multi_pipeline/block_codec.c` | Reversible encode/decode transform |
| `apps/wg_multi_pipeline/buffer_transfer.c` | Full-block pump between encode buffers |
| `apps/wg_multi_pipeline/file_drain.c` | TS / packet write to file/FIFO |
| `apps/wg_multi_pipeline/stream_config.h` | Packet/block sizes, buffer capacities |
| `scripts/run_dual_fifo.sh` | Three-stream live demo (1M / 10M / 20M) |
| `scripts/encode_multibitrate.sh` | Encode 720p `input_{1,10,20}m.ts` |
| `src/flow_manager.c` | Multi-flow queueing and worker lifecycle |
| `src/dispatcher.c` | Mixed queue → per-flow routing |
