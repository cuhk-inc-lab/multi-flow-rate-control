# spec_pipeline

Reference app for the **Technical Specification module boundary**.

## What is in the spec vs what is test-only

| Layer | In spec? | In this app |
|-------|----------|-------------|
| `CircularBuffer` ← upstream bytes | yes | fed by `FileIngest` reading `input.ts` |
| `packet_framer` → `FlowManager` → paced `write()` | yes | core path |
| `output_fd` (any fd) | yes | opened as `output.ts` for `cmp` tests |
| `FileIngest` / paths on disk | no | convenience for local verification |

## Module flow (spec)

```
[upstream] → CircularBuffer → packet_framer → FlowManager → write(output_fd) → [downstream]
```

## How this app wires it (test harness)

```
input.ts  ──FileIngest──►  CircularBuffer  ──►  framer  ──►  FlowManager  ──►  write(fd)
                                                                              │
output.ts  ◄──────────────────────── open as output fd ─────────────────────┘
```

`input.ts` / `output.ts` are **not** part of the module. They only simulate
“something fills the ring buffer” and “something receives `write()` output”.

## Build & run

```bash
make spec
./build/spec_pipeline --no-pace input.ts output.ts
cmp input.ts output.ts
```

Multi-flow (one input/output pair per `flow_id`):

```bash
./build/spec_pipeline --no-pace --multi in0.ts out0.ts in1.ts out1.ts
```

Trailing bytes shorter than 188B bypass FlowManager and are written directly to
the output fd after the paced path drains (keeps `cmp` byte-exact).

For encode/decode integration, see `../multi_flow_relay`.
