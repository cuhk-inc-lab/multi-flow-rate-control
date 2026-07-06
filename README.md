# multi-flow-rate-control

C11 / pthreads multi-flow rate-matched queueing module with CircularBuffer integration.

Depends on `../buffer-management-module` for `CircularBuffer` only (linked at build time).

## Two pipelines

### Spec-compliant module flow (`spec_pipeline`)

This is the **canonical** path aligned with the Technical Specification:

```
file.ts
  → CircularBuffer (byte ring)
  → packet_framer (transport packets → DataPacket)
  → FlowManager (MixedQueue → per-flow queues)
  → paced worker → write(output.ts)     # direct fd, no pipe/decode
```

```bash
make spec
make spec-test
./build/spec_pipeline --no-pace input.ts output.ts
cmp input.ts output.ts
```

### Integration harness (`multi_flow_relay`)

End-to-end test with encode/decode and a pipe roundtrip. **Not** the spec module
boundary; used to validate integration with `buffer-management-module` codecs.

```
file.ts
  → CircularBuffer → encode → packet_framer → FlowManager
  → paced pipe → decode → CircularBuffer → file.ts
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
make demo         # synthetic multi-flow demo (memory producers)
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
