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
| `--no-codec` | Optional relay: skip BlockCodec; pointer-only post-worker queue |
| `--multi` | Multiple `in out` pairs; omit for a single pair |
| (default) | Pacing **on** + BlockCodec **on** (reversible `+/-`, not encryption) |

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
# → /tmp/out_0.bin, /tmp/out_1.bin, ...
```

See [tests/TESTING.md](../../tests/TESTING.md) for full coverage.  
Step-by-step demos (offline + FIFO live): [docs/DEMOS.md](../../docs/DEMOS.md).

## Ingress: files vs UDP

### File demo (current)

Files mock wg-obfs upstream with a **fixed `flow_id` per path**:

```c
ingress_push(mgr, flow_id, data, len);
```

This skips 5-tuple lookup but exercises the same FlowManager → encode → decode path.

### UDP (library ready; demo binary not yet)

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

There is no UDP `recvfrom` loop in this app yet; wire the calls above in wg-obfs or
a small glue process when moving off file mocks.

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
