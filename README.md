# multi-flow-rate-control

C11 / pthreads multi-flow rate-matched queueing module with CircularBuffer integration.

## Prerequisites — `buffer-management-module`

This repo **does not vendor** `CircularBuffer`. You must clone
[buffer-management-module](https://gitlab.hk/scyyy-group/buffer-management-module)
as a **sibling directory** (not a git submodule):

```
work/
├── multi-flow-rate-control/     ← this repo
└── buffer-management-module/    ← required at ../buffer-management-module
```

The Makefile resolves:

```
../buffer-management-module/include/circular_buffer.h
../buffer-management-module/src/circular_buffer.c   # compiled into this project
```

You **do not** need to `make` the buffer repo first — `multi-flow-rate-control`
compiles `circular_buffer.c` directly when you run `make` here.

### Clone (new machine)

```bash
mkdir -p ~/work && cd ~/work

git clone git@gitlab.hk:Scyyy/multi-flow-rate-control.git
git clone git@gitlab.hk:scyyy-group/buffer-management-module.git
```

If `buffer-management-module` is missing or not at `../buffer-management-module`,
build fails with errors such as `circular_buffer.h: No such file or directory`.

To use a different path, change `CB_DIR` in the `Makefile`.

## Module boundary

Core library path:

```
ingress_push / ingress_push_tuple
  → FlowManager (MixedQueue → per-flow queues)
  → paced worker → write(output_fd)
```

Encode / decode / file harness live in `apps/wg_multi_pipeline/` (demo app).

## Application — `wg_multi_pipeline`

Ingress → FlowManager → per-flow encode → buffer transfer → decode → output.

```
ingress ──► FlowManager (split + pacing) ──► raw bytes
       ──► encode ──► transfer ──► decode ──► output file
```

**Multi-file:** fixed `flow_id` per input path (`ingress_push`).  
**UDP:** full 5-tuple routing (`ingress_push_tuple`). See `apps/wg_multi_pipeline/README.md`.

### Quick test

```bash
make wg-demo
./build/wg_multi_pipeline --no-pace --multi \
  a.txt out_a.txt b.bin out_b.bin c.ts out_c.ts
cmp a.txt out_a.txt && cmp b.bin out_b.bin && cmp c.ts out_c.ts
```

```bash
./build/wg_multi_pipeline --no-pace --udp 5000 /tmp/out_ --idle-sec 3
```

In UDP mode, `--idle-sec` flushes an idle flow segment while the server keeps
listening; a later packet for that flow begins a new segment.

Automated 3-flow roundtrip: `make integration-test`.

wg-obfs handoff: **[docs/INTEGRATION_BOUNDARIES.md](docs/INTEGRATION_BOUNDARIES.md)**.  
Demo walkthrough (offline + FIFO live): **[docs/DEMOS.md](docs/DEMOS.md)**.  
Testing: **[tests/TESTING.md](tests/TESTING.md)**.

## Build & test

```bash
make                  # libmulti_flow.a
make test             # unit tests (run_tests.c)
make integration-test # multi-file wg_multi_pipeline (3 flows, cmp)
make wg-demo          # build wg_multi_pipeline
make sanitize         # ASan + test + integration-test
make tsan             # TSan + test + integration-test
make clean
```

## Library modules

| Module | Role |
|--------|------|
| `packet` | DataPacket alloc/free |
| `flow_buffer` | Per-flow blocking packet ring |
| `mixed_queue` | Upstream mixed input |
| `flow_manager` | Dispatcher + workers + lifecycle |
| `flow_worker` | Timeline pacing + write |
| `pipe_io` | Pipe I/O and drain-to-buffer glue |
| `fd_sink` | write() with partial retry |
| `flow_peer_map` | UDP 5-tuple → internal flow slot |
| `ingress_push` | Upstream bytes → `flow_manager_push` |

## Pacing

```
target_dequeue = stream_start_dequeue + (pkt_ts - stream_start_enqueue)
```

Reset stream anchor after idle dequeue wait.
