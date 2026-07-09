# multi_flow_relay

**Legacy integration harness** (encode **before** FlowManager).

For the wg-obfs / multi-before-encode path, use `../wg_multi_pipeline` instead.

Exercises the full buffer-management-module stack: file ingest, encode,
FlowManager rate matching, pipe transfer, decode, and file drain.

## Pipeline

```
file.ts
  → CircularBuffer (sending_in)
  → BlockCodec encode (4×188 → 8×188)
  → CircularBuffer (sending_out)
  → packet_framer → FlowManager
  → paced worker → pipe
  → decode → CircularBuffer → file.ts
```

For the Technical Specification module flow, use `../spec_pipeline` instead.

## Build & run

```bash
make app
./build/multi_flow_relay --no-pace input.ts output.ts
cmp input.ts output.ts
```

See project root [README.md](../../README.md) and [tests/TESTING.md](../../tests/TESTING.md).
For multi-file testing of the canonical pipeline, use `../wg_multi_pipeline` and
`make integration-test`.
