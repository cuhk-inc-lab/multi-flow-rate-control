# multi_flow_relay

Application test: integrates `CircularBuffer` (buffer-management-module) with
the multi-flow rate-matched queue in the middle of an encode/decode pipeline.

## Per-flow pipeline

```
input.ts
  → sending_in (CircularBuffer)
  → encode (752 → 1504)
  → sending_out
  → packet_framer → FlowManager → paced pipe
  → receiver_in
  → decode (1504 → 752)
  → receiver_out
  → output.ts
```

## Usage

```bash
make app
./build/multi_flow_relay --no-pace input.ts output.ts
cmp input.ts output.ts
```

Use `--no-pace` for byte-exact verification. Omit it to exercise rate matching.

## Multi-flow

```bash
./build/multi_flow_relay --no-pace --multi in0.ts out0.ts in1.ts out1.ts
```
