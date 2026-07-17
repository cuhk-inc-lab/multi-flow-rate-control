# wg_multi_pipeline

**Multi before encode:** ingress → FlowManager → per-flow encode → transfer → decode.

## Build

```bash
make wg-demo    # from repo root → build/wg_multi_pipeline
```

## Multi-file test (your own inputs)

Each `input output` pair is one flow. The *i*-th pair uses internal `flow_id = i`.
Extensions are ignored — all files are raw bytes.

```bash
./build/wg_multi_pipeline --no-pace --multi \
  a.txt out_a.txt \
  b.bin out_b.bin \
  c.ts  out_c.ts

cmp a.txt out_a.txt
cmp b.bin out_b.bin
cmp c.ts  out_c.ts
```

| Flag | Meaning |
|------|---------|
| `--no-pace` | Disable pacing; byte-exact for `cmp`; live scripts use this with `ffmpeg -re` |
| `--codec block` | Default: existing reversible `+/-` BlockCodec demo transform |
| `--codec xor-fec` | Systematic XOR FEC: 4 data TS packets + 1 parity packet |
| `--codec none` / `--no-codec` | Optional relay: skip coding; pointer-only post-worker queue |
| `--multi` | Multiple `in out` pairs; omit for a single pair |
| (default) | Pacing **on** + BlockCodec **on** (reversible `+/-`, not encryption) |

The local demo transfer does not drop packets. `xor-fec` therefore round-trips
normally; `XorFecCodec_recover_one()` is the transport-facing helper for
recovering one identified missing shard in a future network receiver.

```bash
./build/wg_multi_pipeline --no-pace --codec xor-fec input.ts output.ts
cmp input.ts output.ts
```

**Visual XOR FEC trace** (data `A` / `B` / `C` / `D`, generated parity, one
simulated loss, recovery, and decoded output):

```bash
make fec-trace
```

**Live multi-bitrate FIFO demo:** see [docs/DEMOS.md](../../docs/DEMOS.md) Demo 3  
(`scripts/run_dual_fifo.sh` uses `--no-pace --multi` with BlockCodec enabled).

**Single flow:**

```bash
./build/wg_multi_pipeline --no-pace input.ts output.ts
```

**Automated 3-flow test** (random data, `cmp` in Makefile):

```bash
make integration-test
```

**UDP ingress demo** (`ingress_push_tuple` + full pipeline):

```bash
make wg-demo
./build/wg_multi_pipeline --no-pace --udp 5000 /tmp/out_ --idle-sec 3
echo -n flow-a | nc -u -p 4001 127.0.0.1 5000
echo -n flow-b | nc -u -p 4002 127.0.0.1 5000
# → /tmp/out_flow0_segment0.bin, /tmp/out_flow1_segment0.bin, ...
```

`--idle-sec` is a **per-flow segment boundary**, not a server shutdown timer.
After a flow is idle for the configured interval, the app drains its queued
data, zero-pads a partial codec block if needed, and keeps listening for the
next segment. New data for that flow after the timeout starts a new segment.
Each completed segment is written to its own file, such as
`/tmp/out_flow0_segment1.bin`.

See [tests/TESTING.md](../../tests/TESTING.md) for full coverage.  
Step-by-step demos (offline + FIFO live): [docs/DEMOS.md](../../docs/DEMOS.md).

## Ingress: files vs UDP

### File demo (current)

Files mock wg-obfs upstream with a **fixed `flow_id` per path**:

```c
ingress_push(mgr, flow_id, data, len);
```

This skips 5-tuple lookup but exercises the same FlowManager → encode → decode path.

### UDP demo

Routing key is the **full UDP 5-tuple** `(src, dst, protocol)`, not `flow_id`.
`flow_id` is only the internal slot index for queues and workers.

```c
#include "ingress_push.h"

FlowTuple tuple;
flow_tuple_set(&tuple, src, src_len, dst, dst_len, IPPROTO_UDP);
ingress_push_tuple(mgr, peer_map, &tuple, payload, len);
```

- `src` — peer address from `recvfrom`
- `dst` — local bind address of the receiving socket
- Test 5-tuple mapping: `make test` (`test_flow_peer_map`)

`wg_multi_pipeline --udp` already provides the `recvfrom` loop for the demo.
Production integration can use the same calls in wg-obfs or another transport
adapter.

Integration handoff details: [docs/INTEGRATION_BOUNDARIES.md](../../docs/INTEGRATION_BOUNDARIES.md).

## Pipeline diagram

```
ingress (flow_id or 5-tuple)
  → FlowManager (MixedQueue → per-flow queues → optional pacing)
  → either:
       A) default: pipe → BlockCodec encode → transfer → decode → output
       B) --no-codec: DataPacket* queue → FileDrain_write_packet → output
```

BlockCodec is a demo encode/decode transform only — not cryptographic encryption.
