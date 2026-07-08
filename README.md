# multi-flow-rate-control

C11 / pthreads multi-flow rate-matched queueing module with CircularBuffer integration.

Depends on `../buffer-management-module` for `CircularBuffer` only (linked at build time).

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

Ingress (`ingress_push` / future UDP peer map) → FlowManager → per-flow encode →
buffer transfer → decode → separate output files. Files mock wg-obfs with a fixed
`flow_id` per input path.

```
ingress (flow_id) ──► FlowManager (split + pacing) ──► raw bytes
                 ──► encode ──► transfer ──► decode ──► output.ts
```

```bash
make wg-demo
make integration-test
./build/wg_multi_pipeline --no-pace --multi in0.ts out0.ts in1.ts out1.ts in2.ts out2.ts
```

Future UDP hook: `ingress_push_peer(mgr, map, src_addr, ...)` — see
`include/ingress_push.h` and `include/flow_peer_map.h`. No wg-obfs code changes required.

## Build & test

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
| `flow_peer_map` | UDP src_addr → flow_id (wg-obfs ingress) |
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
