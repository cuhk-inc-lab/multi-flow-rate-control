# wire_relay

Explicit-hop opaque UDP relay for wire header **v3** (`final_dst` + `ttl`).

## Build

```bash
# from multi-flow-rate-control repo root
make wire-relay
```

Binary: `build/wire_relay`.

## Role (Phase 1 + Phase 2)

```text
recvfrom(listen)  [or relay_inject_wire_datagram — wire-level only]
  → copy into owned datagram
  → parse WireHeader
  → if final_dst == local_node_id → local delivery callback
  → else ttl-- (wire_header_encode back into datagram bytes)
  → [--process cache] DATA only: copy into GenerationCache (observe)
  → move current datagram into global EgressQueue (ownership move)
  → TX worker: sendto(next-hop) using datagram bytes only; free
```

- Does **not** encode/decode / recode.
- `relay_inject_wire_datagram()` accepts **already encoded wire v3 datagrams** only.
- `--process forward` (default): Phase 1 path, no GenerationCache.
- `--process cache`: copy-based GenerationCache + still **opaque forward** every
  packet (including duplicates / metadata mismatches / admission failures).
  Cache is observe/store/hook prep only; no wait-for-generation, no merge.
- END/control never enter GenerationCache; FIFO EgressQueue keeps DATA-before-END
  when DATA was enqueued first.
- See [`docs/WIRE_RELAY_PIPELINE.md`](../../docs/WIRE_RELAY_PIPELINE.md).

## CLI

```bash
./build/wire_relay \
  --local-node-id 2 \
  --listen 9000 \
  --next-hop 10.10.23.2:9000 \
  [--idle-exit-sec N] \
  [--egress-capacity N] \
  [--process forward|cache] \
  [--gen-timeout-ms N] \
  [--max-gens N] \
  [--max-gens-per-flow N] \
  [--max-cache-bytes N]
```

| Flag | Meaning |
|------|---------|
| `--local-node-id` | This node's id (Destination Check) |
| `--listen` | UDP bind port |
| `--next-hop` | `HOST:PORT` for non-local forward |
| `--idle-exit-sec` | Optional; exit after N seconds with no RX (tests) |
| `--egress-capacity` | Global EgressQueue slots (default 4096) |
| `--process` | `forward` (default) or `cache` |
| `--gen-timeout-ms` | Cache idle timeout (default 500) |
| `--max-gens` | Global generation limit (default 256) |
| `--max-gens-per-flow` | Per-flow generation limit (default 32) |
| `--max-cache-bytes` | Cache memory cap (default 32MiB) |

### Cache-mode example (VM2)

```bash
./build/wire_relay \
  --local-node-id 2 \
  --listen 9000 \
  --next-hop 10.10.23.2:9000 \
  --process cache \
  --gen-timeout-ms 500 \
  --max-gens 256 \
  --max-gens-per-flow 32 \
  --max-cache-bytes 33554432
```

## Four-VM linear path

```text
VM1 encode+send  →  VM2 wire_relay  →  VM3 wire_relay  →  VM4 udp-recv+decode
final_dst=4         local=2             local=3             local_node_id=4
sendto(VM2)         next=VM3            next=VM4
```

## Loopback / unit tests

```bash
make wire-relay wg-demo
sh tests/wire_relay_loopback.sh ./build/wg_multi_pipeline ./build/wire_relay build
./build/relay_gen_cache_tests
```
