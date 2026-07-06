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

```bash
make spec
make spec-test
./build/spec_pipeline --no-pace input.ts output.ts
cmp input.ts output.ts
```

In production, replace `FileIngest` with your upstream writer to `CircularBuffer`,
and point `output_fd` at socket/pipe/`STDOUT_FILENO` instead of a file.

### Integration harness (`multi_flow_relay`)

Full stack test with encode/decode and a pipe roundtrip. **Outside** the spec
module boundary; validates `buffer-management-module` codecs.

```
input.ts ──► CircularBuffer ──► encode ──► packet_framer ──► FlowManager
         ──► paced pipe ──► decode ──► CircularBuffer ──► output.ts
         (files are test I/O only; same as above)
```

```bash
make app
make app-test
make app-test-multi
```

## Build

```bash
make              # libmulti_flow.a + circular_buffer.o
make test         # unit + integration tests
make spec         # spec_pipeline (spec-aligned app)
make spec-test    # byte-exact spec pipeline (pacing off)
make demo         # synthetic multi-flow demo (memory producers, no files)
make app          # multi_flow_relay integration harness
make app-test     # byte-exact relay roundtrip (pacing off)
make app-test-multi
make sanitize     # ASan + test + spec-test + app-test
make tsan         # ThreadSanitizer on unit tests + app-test-multi
make clean
```

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

## Pacing

Uses absolute timeline projection per spec:

```
target_dequeue = stream_start_dequeue + (pkt_ts - stream_start_enqueue)
```

Reset stream anchor after idle dequeue wait.

## Metrics

Per-flow `_Atomic` byte/packet counters and rolling-window bps (`flow_metrics_tick`).
`spec_pipeline`, `relay`, and `demo` sample metrics during execution. Unit tests
include `test_rate_match_5s` (±1% dequeue vs enqueue bps over 5s) and
`test_pacing_timeline`.
