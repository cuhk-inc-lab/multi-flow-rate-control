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
| `--codec rs-fec` | Systematic Reed-Solomon FEC: 4 data TS packets + 2 parity packets |
| `--codec none` / `--no-codec` | Optional relay: skip coding; pointer-only post-worker queue |
| `--multi` | Multiple `in out` pairs; omit for a single pair |
| (default) | Pacing **on** + BlockCodec **on** (reversible `+/-`, not encryption) |

The local demo transfer does not drop packets. The wire UDP receiver recovers
one missing XOR FEC shard automatically when it receives 4 of 5 shards. With
`rs-fec`, it recovers up to two missing shards when any 4 of 6 shards arrive.

```bash
./build/wg_multi_pipeline --no-pace --codec xor-fec input.ts output.ts
cmp input.ts output.ts
```

**Visual XOR FEC trace** (data `A` / `B` / `C` / `D`, generated parity, one
simulated loss, recovery, and decoded output):

```bash
make fec-trace
```

**Systematic FEC best-effort wire receive:** use this only for live media. After the
end marker and `--idle-sec` timeout, unrecoverable groups output their received
data shards in order and skip missing data shards; parity is not output.

```bash
./build/wg_multi_pipeline --codec xor-fec \
  --udp-recv 9000 received.ts --idle-sec 1 --best-effort
```

`rs-fec` uses the BSD-licensed native Vandermonde backend provided by
`liberasurecode-dev`; install it before building on Debian/Ubuntu:

```bash
sudo apt-get install liberasurecode-dev
```

## Per-block latency and jitter

Wire sender/receiver transfers record per-block timestamps automatically. The
receiver prints encode, transfer, decode, end-to-end, and end-to-end jitter
statistics (`avg`, `p50`, `p95`, `p99`, and `max`) after a complete transfer.

For a cross-host run, synchronize the sender and receiver clocks first (for
example with Chrony). The measurements are:

```text
encode       = sender Codec_encode() end - start
transfer     = receiver decode-ready - sender encode end
decode       = receiver Codec_decode() end - start
end-to-end   = receiver decode end - sender encode start
jitter       = absolute difference between consecutive end-to-end delays
```

`transfer` and `end-to-end` include the network path, kernel forwarding,
receiver reassembly, and any FEC recovery. Receiver-side timestamps are
ignored when a block lacks a valid sender timestamp, which is useful for
hand-crafted protocol tests.

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
