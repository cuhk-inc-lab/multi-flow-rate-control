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
| `--codec rs-fec` | Systematic Reed-Solomon FEC via liberasurecode: 4 data + 2 parity shards |
| `--codec rs` | Systematic Reed-Solomon FEC via vendored hqm/rscode (column-wise RS 4+2); GPL |
| `--codec none` / `--no-codec` | Local relay: skip coding; pointer-only post-worker queue. In wire UDP modes, `none` sends raw single-shard blocks |
| `--multi` | Multiple `in out` pairs; omit for a single pair |
| (default) | Pacing **on** + BlockCodec **on** (reversible `+/-`, not encryption) |

The local demo transfer does not drop packets. On the wire UDP receiver / shared
`WireFlowDecoder`:

- **Systematic codecs** (`copy`, `xor-fec`, `rs-fec`, `rs`): a block may decode
  once every original data shard in `[0, Codec_data_shards())` is present.
  Missing pad/parity shards do **not** block that block. Receiving `k` arbitrary
  shards (with a data shard still missing) is **not** enough.
- **Non-systematic** (`block`): still requires a full group (or unchanged
  recover path); no data-only fast path.
- **FEC recover** (`xor-fec` / `rs-fec` / `rs`): if any required data shard is
  missing, keep the existing recover threshold (e.g. XOR one erasure; RS any
  `k` of `n`). `rs` and `rs-fec` share geometry but are not wire-compatible;
  send and receive must use the same `--codec`.

```bash
./build/wg_multi_pipeline --no-pace --codec xor-fec input.ts output.ts
cmp input.ts output.ts
```

**Visual XOR FEC trace** (data `A` / `B` / `C` / `D`, generated parity, one
simulated loss, recovery, and decoded output):

```bash
make fec-trace
```

**Systematic FEC wire receive:** `--udp-recv` defaults to `--best-effort` (live
media). Missing or unrecoverable groups are skipped
(`skipped_groups`) so later recovered groups are still written in order:
mid-stream when a new block cannot enter the reorder window, after END
for remaining holes, and when the head has fewer than k shards while a
later group is already recovered. Unrecoverable systematic groups also output whatever
data shards arrived and omit missing data shards (`missing_data_shards`);
parity is not output. The file will not match the source sha256. Use `--strict`
for hash-verified complete-file transfers.

```bash
./build/wg_multi_pipeline --codec xor-fec \
  --udp-recv 9000 received.ts --idle-sec 3
# equivalent: add --best-effort (now the default)
# hash-complete: add --strict
```

`rs-fec` uses the BSD-licensed native Vandermonde backend provided by
`liberasurecode-dev`; install it before building on Debian/Ubuntu:

```bash
sudo apt-get install liberasurecode-dev
```

`rs` vendors [hqm/rscode](https://github.com/hqm/rscode) under
`third_party/rscode` (GPL). It applies classical RS with `NPAR=2` to each
byte-aligned column across the six shards (same 4+2 erasure capability as
`rs-fec`, independent implementation).

### `rs` recovery modes and runtime params

See **[docs/RS_CODEC.md](../../docs/RS_CODEC.md)** for architecture, API, and
bounds.

Default is **RS(6,4)** (`k=4`, `parity=2`). Set your own numbers:

```bash
./build/wg_multi_pipeline --codec rs --rs-k=16 --rs-parity=2 \
  --udp-send 10.10.34.2 9000 input.bin
# or: --rs-profile=16+2

./build/wg_multi_pipeline --codec rs --rs-k=16 --rs-parity=2 \
  --udp-recv 9000 out.bin
```

Sender and receiver must use the **same fixed** `(k, parity)`. Wire
`shard_count` must equal `k+parity`; it is validation only and does **not**
retune the process RS profile. Concurrent wire flows in one process share that
geometry. Working size follows your `(k,r)`; the GF(256) ceiling is
`k+r ≤ 255` (presence uses a bit array, not a 32-bit mask).

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

## Wire header v3 (`final_dst` / `ttl`)

Wire datagrams use a shared 44-byte header (`include/wire_header.h`, version 3).
Offsets 6–7 carry `final_dst` (ultimate delivery node id) and `ttl`. All other
fields keep the previous layout. Defaults: `--final-dst 4`, `--ttl 8`,
receiver `--local-node-id 4`.

For application-layer hops VM1→VM2→VM3→VM4, send to the **next hop** UDP
address while setting `final_dst` to the final node. Relays:
[`../wire_relay/README.md`](../wire_relay/README.md).

## Cross-VM wire multi-flow (single sender/receiver process)

Use wire multi-flow to transfer multiple concurrent streams with one app
process per VM.

Sender uses `--udp-send-multi` + repeated `--flow` specs, receiver uses
`--udp-recv <port> <out_prefix> --max-flows N`.

**Demux rules (do not confuse):**

- **Non-wire pipeline ingress** (`--udp`, fake `tuple:` file ingress): may map a
  UDP 5-tuple → internal slot via `flow_peer_map`.
- **Wire UDP receiver (`--udp-recv`)**: decoder / output selection uses **only**
  `WireHeader.flow_id` (full `uint32_t`). UDP source/dest address and port are
  **not** the demux key. This matters after `wire_relay`, which typically emits
  every flow from one `send_sock` (same peer tuple).

`--flow` spec formats:

- Explicit id (iperf-like): `<flow_id>:<receiver_ip>:<port>:<input_path>[:rate_mbps]`
- Fake upstream 5-tuple → internal `flow_id` via `flow_peer_map` on the **sender
  pipeline**, then the chosen id is written into the wire header:

```text
tuple:<src_ip>:<src_port>:<dst_ip>:<dst_port>:<wire_host>:<wire_port>:<input>[:rate]
```

Example: 2 flows (Node1 -> Node4) with `copy` codec.

Node4 (receiver):

```bash
./build/wg_multi_pipeline --codec copy --lock-memory --local-node-id 4 \
  --udp-recv 9000 /tmp/out_multi_ --idle-sec 5 --max-flows 2
```

Node1 (sender, explicit flow ids):

```bash
./build/wg_multi_pipeline --codec copy --final-dst 4 --ttl 8 --udp-send-multi \
  --flow "0:10.10.34.2:9000:input0.ts:32" \
  --flow "1:10.10.34.2:9000:input1.ts:32"
```

Node1 (sender, file + fake 5-tuple → flow_id, then same wire path):

```bash
./build/wg_multi_pipeline --codec copy --udp-send-multi \
  --flow "tuple:10.0.0.1:4001:10.10.12.1:5000:10.10.34.2:9000:input0.ts:32" \
  --flow "tuple:10.0.0.1:4002:10.10.12.1:5000:10.10.34.2:9000:input1.ts:32"
```

Stderr prints `tuple … => flow_id=N`. File chunks use `ingress_push_tuple` (same API as `--udp`).
The tuple is only the simulated ingress key on the sender; `wire_host:wire_port` is the real next hop.
Receiver output naming (one file per **wire** `flow_id`; peer tag is cosmetic):

`{out_prefix}src_<sender_ip>_p<sender_port>_flow_<wire_flow_id>.<suffix>`

Default suffix is `.ts`. Override per wire flow id with
`--out-suffix <flow_id>:<ext>` on `--udp-recv` (e.g. `--out-suffix 0:.txt --out-suffix 1:.ts`).
Note: `--out-suffix` table currently indexes ids `0..7` only.

Because the UDP sender source port may vary, use wildcard when validating:

```bash
cmp input0.ts /tmp/out_multi_src_*_flow_0.ts
cmp input1.ts /tmp/out_multi_src_*_flow_1.ts
```

See [tests/TESTING.md](../../tests/TESTING.md) for full coverage.  
Step-by-step demos (offline + FIFO live): [docs/DEMOS.md](../../docs/DEMOS.md).

## Ingress: files vs UDP

### File demo (current)

Files can mock upstream in two ways:

1. **Fixed `flow_id`** (explicit `--flow "id:host:port:file"`):

```c
ingress_push(mgr, flow_id, data, len);
```

2. **Fake 5-tuple → `flow_id`** (`--flow "tuple:src:sport:dst:dport:wire_host:wire_port:file"`):

```c
flow_tuple_set(&tuple, src, ..., dst, ..., IPPROTO_UDP);
ingress_push_tuple(mgr, peer_map, &tuple, data, len);
```

Both then share the same FlowManager → encode → wire path.
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
  → per-flow demux buffer → encode
  → either:
       A) local --multi: pipe → encode → transfer → decode → file
       B) --udp-send-multi: packet queue → encode → wire UDP shards
       C) --no-codec: DataPacket* queue → FileDrain_write_packet → output
```

`--udp-send-multi` mirrors local `--multi`: demux before encode, then the same
per-flow encode step. Only the demux buffer (packet queue vs pipe) and
post-encode egress (UDP vs local transfer/decode) differ.

For wire UDP only, `--codec none` is supported as raw passthrough: each block is
sent as a single `PKG_SIZE` shard with no parity and written back verbatim on the
receiver.

BlockCodec is a demo encode/decode transform only — not cryptographic encryption.
