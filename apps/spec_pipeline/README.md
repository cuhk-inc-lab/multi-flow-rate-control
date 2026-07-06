# spec_pipeline

**Spec-compliant** reference application for the Rate-Matched Queueing Module.

## Pipeline (matches Technical Specification)

```
file.ts
  → FileIngest
  → CircularBuffer (byte ring from buffer-management-module)
  → packet_framer (188B transport packets → DataPacket)
  → FlowManager (MixedQueue → per-flow queues → paced workers)
  → write(output.ts)          # direct fd write, no pipe / decode
```

This is the canonical module boundary. It does **not** include encode/decode or
pipe roundtrip logic (see `../multi_flow_relay` for integration testing).

## Build & run

```bash
make spec
./build/spec_pipeline --no-pace input.ts output.ts
cmp input.ts output.ts
```

Multi-flow:

```bash
./build/spec_pipeline --no-pace --multi in0.ts out0.ts in1.ts out1.ts
```

Files with a trailing partial packet (&lt; 188 bytes) bypass FlowManager and are
written directly to the output fd after the paced path drains.
