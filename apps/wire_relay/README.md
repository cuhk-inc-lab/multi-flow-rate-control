# wire_relay

Explicit-hop opaque UDP relay for wire header **v3** (`final_dst` + `ttl`),
with optional local-destination decode (**L1** single-flow / **L2** multi-flow).

## Build

```bash
# from multi-flow-rate-control repo root
make wire-relay
```

Binary: `build/wire_relay`.

## Role (Phase 1 + Phase 2 + L1/L2)

```text
recvfrom(listen)  [UDP RX only]
  → copy into owned datagram
  → minimal wire_header_decode → flow_id
  → per-flow RelayDeferredHub (packet-level; never waits on egress)
processing worker (single thread):
  → round-robin pop (quota=8) → ingress_mu
  → if final_dst == local_node_id:
       [--local-decode] LocalDecodeHub / per-flow WireFlowDecoder
       else count-only local delivery
       (no TTL--, no GenerationCache, no EgressQueue, no sendto)
  → else ttl-- → [--process cache] → ForwardPending
  → release ingress_mu → timed/try EgressQueue enqueue
TX worker:
  → sendto(next-hop) / test capture; free

relay_inject_wire_datagram (harness):
  → synchronous path (does NOT enter deferred)
  → inject-only tests keep existing order; mixing inject+RX
    does not preserve per-flow relative order
```

- Default path does **not** encode/decode / recode.
- Local destination is decided only by `wire_header_is_local` / `final_dst`
  (never UDP/IP destination).
- `relay_inject_wire_datagram()` accepts **already encoded wire v3 datagrams** only.
- `--process forward` (default): Phase 1 path, no GenerationCache.
- `--process cache`: copy-based GenerationCache + still **opaque forward** every
  packet. Cache is observe/store only; not used for local decode state.
- See [`docs/WIRE_RELAY_PIPELINE.md`](../../docs/WIRE_RELAY_PIPELINE.md).

### `--local-decode`

Requires `--codec` and **exactly one** of `--output` / `--output-dir`.

| Mode | Flags | Behavior |
|------|-------|----------|
| **L1** | `--output FILE` | Single `flow_id`; second distinct id rejected |
| **L2** | `--output-dir DIR` | Up to `RELAY_MAX_FLOWS` flows; `DIR/flow_<id>.bin` |

- `DIR` must already exist (no recursive mkdir).
- Process-wide fixed codec / FEC geometry; shard metadata mismatch → drop + count.
- Enables `LOCAL_ENCODER` when `final_dst==local` (`reject_local_encoder_loopback=0`).
- Strict exit: incomplete active flows or errors → `EXIT_FAILURE`.

## CLI

```bash
./build/wire_relay \
  --local-node-id 2 \
  --listen 9000 \
  --next-hop 10.10.23.2:9000 \
  [--idle-exit-sec N] \
  [--egress-capacity N] \
  [--egress-wait-ms N] \
  [--deferred-per-flow N] \
  [--deferred-total N] \
  [--max-active-flows N] \
  [--process forward|cache] \
  [--gen-timeout-ms N] \
  [--max-gens N] \
  [--max-gens-per-flow N] \
  [--max-cache-bytes N] \
  [--local-decode --codec copy|xor-fec|rs-fec|rs \
      (--output FILE | --output-dir DIR)]
```

| Flag | Meaning |
|------|---------|
| `--local-node-id` | This node's id (Destination Check) |
| `--listen` | UDP bind port |
| `--next-hop` | `HOST:PORT` for non-local forward |
| `--idle-exit-sec` | Optional; exit after N seconds with no RX (tests) |
| `--egress-capacity` | Global EgressQueue slots (default 4096) |
| `--egress-wait-ms` | Max wait when egress full (default **0** = try-drop); `>0` blocks processing worker only, never UDP RX |
| `--deferred-per-flow` | Per-flow RX deferred datagram cap (default 128) |
| `--deferred-total` | Global RX deferred datagram cap (default 1024) |
| `--max-active-flows` | Max concurrent wire flow_id slots in deferred hub (1..64, default 64) |
| `--process` | `forward` (default) or `cache` |
| `--gen-timeout-ms` | Cache idle timeout (default 500) |
| `--max-gens` | Global generation limit (default 256) |
| `--max-gens-per-flow` | Per-flow generation limit (default 32) |
| `--max-cache-bytes` | Cache memory cap (default 32MiB) |
| `--local-decode` | Enable local-destination decode |
| `--codec` | Codec (requires `--local-decode`) |
| `--output` | L1 single-flow output file |
| `--output-dir` | L2 multi-flow output directory |

### L2 sink example (VM4)

```bash
mkdir -p /tmp/wire_out
./build/wire_relay \
  --local-node-id 4 \
  --listen 9000 \
  --next-hop 127.0.0.1:9 \
  --local-decode --codec copy --output-dir /tmp/wire_out \
  --idle-exit-sec 30
```

## Four-VM linear path

```text
VM1 encode+send  →  VM2 wire_relay  →  VM3 wire_relay  →  VM4 decode sink
final_dst=4         local=2             local=3             local_node_id=4
sendto(VM2)         next=VM3            next=VM4            --local-decode
```

## Loopback / unit tests

```bash
make wire-relay wg-demo
sh tests/wire_relay_loopback.sh ./build/wg_multi_pipeline ./build/wire_relay build
./build/relay_gen_cache_tests
./build/relay_egress_queue_tests
./build/relay_deferred_tests
./build/relay_local_decode_tests
```
