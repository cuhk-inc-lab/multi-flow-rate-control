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

## Spec module boundary

The Technical Specification defines this **module** (not files on disk):

```
[upstream producer]
  → CircularBuffer (byte ring)
  → packet_framer (bytes → DataPacket + enqueue_ts)
  → FlowManager (MixedQueue → per-flow queues)
  → paced worker → write(output_fd)    # stdout, socket, pipe, or file fd
  → [downstream consumer]
```

- **Ingress:** bytes already in (or being written to) `CircularBuffer` by another
  thread or component.
- **Egress:** `write()` to any open file descriptor; a path like `output.ts` is
  only one way to hold that fd in tests.
- **Not in the module:** reading `input.ts` from disk, encode/decode, or pipe
  roundtrips (those live in demo apps below).

## Two reference applications

### Spec-aligned demo (`spec_pipeline`)

Uses **files only as a test harness** to feed the ring buffer and capture output:

```
input.ts          ─┐  test harness (FileIngest)
                   ▼
              CircularBuffer ──► packet_framer ──► FlowManager ──► write(fd)
                   ▲                                              │
output.ts         ─┘  test harness (open output file as fd) ◄──────┘
```

Build with `make spec`. See `apps/spec_pipeline/README.md`.

### wg-obfs integration path (`wg_multi_pipeline`) — **multi before encode**

Ingress (`ingress_push` / `ingress_push_tuple`) → FlowManager → per-flow encode →
buffer transfer → decode → separate output files.

```
ingress ──► FlowManager (split + pacing) ──► raw bytes
       ──► encode ──► transfer ──► decode ──► output file
```

**File demo:** each input path is assigned a fixed internal `flow_id` (0, 1, 2, …).
**UDP (library):** route by full 5-tuple `(src, dst, protocol)` via `flow_peer_map`;
`flow_id` is only the compact slot index inside FlowManager — not a second grouping
rule. See `apps/wg_multi_pipeline/README.md`.

#### Quick test with your own files

Extension does not matter — inputs are raw bytes.

```bash
make wg-demo
./build/wg_multi_pipeline --no-pace --multi \
  a.txt out_a.txt b.bin out_b.bin c.ts out_c.ts
cmp a.txt out_a.txt && cmp b.bin out_b.bin && cmp c.ts out_c.ts
```

Automated 3-flow roundtrip: `make integration-test`.

UDP 5-tuple mapping (library only, no UDP recv in the demo yet): `make test`
(`test_flow_peer_map`). Full testing guide: **[tests/TESTING.md](tests/TESTING.md)**.
wg-obfs handoff points: **[docs/INTEGRATION_BOUNDARIES.md](docs/INTEGRATION_BOUNDARIES.md)**.

## Build & test

From `multi-flow-rate-control/` (with `../buffer-management-module` present):

```bash
make                  # libmulti_flow.a
make test             # unit tests (run_tests.c)
make integration-test # multi-file wg_multi_pipeline (3 flows, cmp)
make wg-demo          # build wg_multi_pipeline
make spec             # spec_pipeline (optional reference app)
make app              # multi_flow_relay (legacy, optional)
make demo             # in-memory demo (manual)
make sanitize         # ASan + test + integration-test
make tsan             # TSan + test + integration-test
make clean
```

See **[tests/TESTING.md](tests/TESTING.md)** for a full description of every test
target and what each `test_*` function checks.

## Library modules

| Module | Role |
|--------|------|
| `packet` | DataPacket alloc/free |
| `flow_buffer` | Per-flow blocking packet ring |
| `mixed_queue` | Upstream mixed input |
| `flow_manager` | Dispatcher + workers + lifecycle |
| `flow_worker` | Spec-style timeline pacing + write |
| `packet_framer` | CircularBuffer → DataPacket |
| `pipe_io` | Used by integration harness only |
| `fd_sink` | write() with partial retry |
| `flow_peer_map` | UDP 5-tuple → internal flow slot (wg-obfs ingress) |
| `ingress_push` | Upstream bytes → `flow_manager_push` |

## Pacing

Uses absolute timeline projection per spec:

```
target_dequeue = stream_start_dequeue + (pkt_ts - stream_start_enqueue)
```

Reset stream anchor after idle dequeue wait.

## Metrics

Per-flow `_Atomic` byte/packet counters and rolling-window bps (`flow_metrics_tick`).
`spec_pipeline`, `relay`, and `demo` sample metrics during execution.

For test coverage (including `test_rate_match_5s` and `test_pacing_timeline`), see
[tests/TESTING.md](tests/TESTING.md).
