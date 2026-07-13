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

All file/FIFO demos use the same per-flow processing order (**multi before encode**):

```
ingress (188-byte TS packets)
  → ingress_push(mgr, flow_id, …)
  → FlowManager
       MixedQueue → dispatcher → per-flow queue → paced worker
  → pipe (raw bytes per flow)
  → drain_pipe_to_post_multi()   # packet-aligned reads
  → post_multi_in (CircularBuffer)
  → BlockCodec encode  (DECODE_BLOCK → ENCODE_BLOCK)
  → BufferTransfer_pump (full ENCODE_BLOCK chunks only)
  → receiver_in → BlockCodec decode → receiver_out
  → FileDrain (188-byte TS packets) → output file or FIFO
```

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
  input.ts  output0.ts \
  input1.ts output1.ts \
  input2.ts output2.ts

cmp input.ts  output0.ts && echo "flow 0 OK"
cmp input1.ts output1.ts && echo "flow 1 OK"
cmp input2.ts output2.ts && echo "flow 2 OK"
```

**Single file:**

```bash
./build/wg_multi_pipeline --no-pace input.ts output.ts
cmp input.ts output.ts
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

## Demo 3 — Multi-stream FIFO live (TS)

**Purpose:** Same as Demo 2, but **two or more** simultaneous TS streams (e.g.
`input1.ts` + `input2.ts`), each on its own `flow_id`.

### Scripted (dual stream)

```bash
killall ffmpeg ffplay wg_multi_pipeline 2>/dev/null
cd /path/to/multi-flow-rate-control
./scripts/run_dual_fifo.sh
```

Expects `input1.ts` and `input2.ts` in the repo root (or pass a directory as
argument). Opens two `ffplay` windows.

### Manual (dual stream)

**Step 1 — FIFOs + players:**

```bash
killall ffmpeg ffplay wg_multi_pipeline 2>/dev/null
rm -f /tmp/live_in0.ts /tmp/live_in1.ts /tmp/live_out0.ts /tmp/live_out1.ts
mkfifo /tmp/live_in0.ts /tmp/live_in1.ts /tmp/live_out0.ts /tmp/live_out1.ts

ffplay -loglevel error -f mpegts /tmp/live_out0.ts   # window 0
ffplay -loglevel error -f mpegts /tmp/live_out1.ts   # window 1 (second terminal)
```

**Step 2 — pipeline:**

```bash
./build/wg_multi_pipeline --multi \
  /tmp/live_in0.ts /tmp/live_out0.ts \
  /tmp/live_in1.ts /tmp/live_out1.ts
```

**Step 3 — push (one ffmpeg per flow, stagger starts):**

```bash
ffmpeg -re -i input1.ts -c copy -f mpegts -y /tmp/live_in0.ts
ffmpeg -re -i input2.ts -c copy -f mpegts -y /tmp/live_in1.ts
```

### Three or more streams

Add one `in out` FIFO pair per flow; one `ffplay` and one `ffmpeg` per stream:

```bash
./build/wg_multi_pipeline --multi \
  /tmp/live_in0.ts /tmp/live_out0.ts \
  /tmp/live_in1.ts /tmp/live_out1.ts \
  /tmp/live_in2.ts /tmp/live_out2.ts
```

### Implementation notes

- Same per-flow pipeline as Demo 2; `wg_pipeline_run()` loops all stages each tick.
- `FlowManager` multiplexes all flows through `MixedQueue` → dispatcher → per-flow
  workers; each worker writes to its own pipe.
- Lazy input open is required for `--multi` so opening `live_in1` does not block
  before `live_in0` has a writer.

**Key code:** `wg_pipeline_run()` main loop over `config->flow_count` stages;
`scripts/run_dual_fifo.sh` for orchestration.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|----------------|-----|
| Pipeline hangs on start | Output FIFO has no reader | Start `ffplay` (or `cat > file`) **before** pipeline |
| Pipeline hangs in `--multi` | Input FIFO has no writer | Start `ffmpeg` **after** pipeline; inputs open lazily |
| `ffmpeg` / `ffplay` Stopped (`jobs`) | Previous FIFO clients stuck | `killall ffmpeg ffplay wg_multi_pipeline` |
| ffplay `changing packet size` / `Packet corrupt` | Corrupted or misaligned TS | Rebuild latest code; ensure 188-byte packet alignment |
| Black screen, no error | Wrong order or no `ffmpeg` | Follow player → pipeline → ffmpeg |
| `cmp` fails on large files | Old buffer/alignment bugs | Use current `stream_config.h` buffer sizes; `--no-pace` |

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
| 3 Multi FIFO live | N FIFOs ← N `ffmpeg` | N FIFOs → N `ffplay` | Playback |

---

## Source map

| File | Role |
|------|------|
| `apps/wg_multi_pipeline/main.c` | CLI: `--multi`, `--no-pace`, file pairs |
| `apps/wg_multi_pipeline/pipeline.c` | Main loop, FIFO ingress, post-multi codec chain |
| `apps/wg_multi_pipeline/block_codec.c` | Reversible encode/decode transform |
| `apps/wg_multi_pipeline/buffer_transfer.c` | Full-block pump between encode buffers |
| `apps/wg_multi_pipeline/file_drain.c` | TS packet write to file/FIFO |
| `apps/wg_multi_pipeline/stream_config.h` | Packet/block sizes, buffer capacities |
| `scripts/run_dual_fifo.sh` | Dual-stream FIFO orchestration |
| `src/flow_manager.c` | Multi-flow queueing and worker lifecycle |
| `src/dispatcher.c` | Mixed queue → per-flow routing |
