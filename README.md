# multi-flow-rate-control

C11 / pthreads multi-flow rate-matched queueing module with CircularBuffer integration.

## Architecture

```
file.ts
  → CircularBuffer (sending_in)
  → BlockCodec encode
  → CircularBuffer (sending_out)
  → packet_framer → FlowManager (MixedQueue → per-flow queues)
  → paced worker → pipe
  → CircularBuffer (receiver_in)
  → BlockCodec decode
  → CircularBuffer (receiver_out)
  → file.ts
```

Depends on `../buffer-management-module` for `CircularBuffer` only (linked at build time).

## Build

```bash
make              # libmulti_flow.a + circular_buffer.o
make test         # unit + integration tests
make demo         # synthetic multi-flow demo
make app          # multi_flow_relay (CircularBuffer + codec + FlowManager)
make app-test     # byte-exact single-flow roundtrip (pacing off)
make app-test-multi
make sanitize     # ASan + app-test
make tsan         # ThreadSanitizer on unit tests
make clean
```

## multi_flow_relay

```bash
# Single flow (like stream_relay)
./build/multi_flow_relay --no-pace input.ts output.ts
cmp input.ts output.ts

# Two flows
./build/multi_flow_relay --no-pace --multi in0.ts out0.ts in1.ts out1.ts

# With rate pacing enabled (output timing differs; use cmp only with --no-pace)
./build/multi_flow_relay input.ts output.ts
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
| `pipe_io` | Paced transfer between encode/decode stages |
| `fd_sink` | write() with partial retry |

## Pacing

Uses absolute timeline projection per spec:

```
target_dequeue = stream_start_dequeue + (pkt_ts - stream_start_enqueue)
```

Reset stream anchor after idle dequeue wait.

## Metrics

Per-flow `_Atomic` byte/packet counters and rolling-window bps (`flow_metrics_tick`).
