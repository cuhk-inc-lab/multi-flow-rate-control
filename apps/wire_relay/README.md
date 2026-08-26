# wire_relay

Explicit-hop UDP relay for wire header **v3** (`final_dst` + `ttl`), with
local source encode, local-destination decode, and optional mid-hop transforms.

## Build

```bash
# from multi-flow-rate-control repo root
make wire-relay
```

Binary: `build/wire_relay`.

## Unified pipeline

```text
UDP in
  → ttl==0? drop
  → final_dst == local_node_id?
        yes → [--local-decode] LocalDecodeHub / WireFlowDecoder → file
        no  → TTL--
            → [--transit-hook identity|plus-minus] ingress transform
            → [--decode-reencode-stub] Phase 3A reserved (OPAQUE)
            → [--process cache] GenerationCache observe
            → EgressQueue → [plus-minus: payload -1] → sendto(next-hop)

Local file/FIFO
  → [--source] encode → inject → same Destination Check / transit / egress
```

- Default mid-hop path is **opaque forward**.
- `plus-minus` applies `+1` to each DATA payload byte before enqueue and `-1`
  after dequeue. Headers and END datagrams are unchanged.
- Locality is only `wire_header_is_local` / `final_dst` (never UDP/IP dst).
- `relay_inject_wire_datagram()` accepts already-encoded wire v3 datagrams;
  `--source` is the raw file/FIFO → encoder path.
- `--process cache`: observe/store only; still opaque-forward.
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

### `--source`

Requires `--codec`, `--final-dst`, and `--ttl`. Optional `--flow-id`, `--rate-mbps`.

- Background encode thread inside `relay_run`; injects DATA+END into the same
  Destination Check / transit / egress path as UDP RX.
- `FILE` may be a regular file or a named FIFO.
- If `final_dst == local_node_id`, also requires `--local-decode`.

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
  [--transit-hook identity|plus-minus] \
  [--decode-reencode-stub] \
  [--local-decode --codec copy|xor-fec|rs-fec|rs|wirehair|none \
      (--output FILE | --output-dir DIR)] \
  [--return-hop HOST:PORT] \
  [--wh-segment-mib=N --wh-repair-pct=P --wh-window=N --wh-ack|--no-wh-ack] \
  [--source FILE --final-dst N --ttl N --codec ... \
      [--flow-id N] [--rate-mbps N]]
```

| Flag | Meaning |
|------|---------|
| `--local-node-id` | This node's id (Destination Check) |
| `--listen` | UDP bind port |
| `--next-hop` | `HOST:PORT` for non-local forward |
| `--idle-exit-sec` | Optional; exit after N seconds with no UDP/source activity (tests) |
| `--egress-capacity` | Global EgressQueue slots (default 16384) |
| `--egress-wait-ms` | Max wait when egress full (default **0** = try-drop); `>0` blocks processing worker only, never UDP RX |
| `--deferred-per-flow` | Per-flow RX deferred datagram cap (default 4096) |
| `--deferred-total` | Global RX deferred datagram cap (default 32768) |
| `--max-active-flows` | Max concurrent wire flow_id slots in deferred hub (1..64, default 64) |
| `--process` | `forward` (default) or `cache` |
| `--gen-timeout-ms` | Cache idle timeout (default 500) |
| `--max-gens` | Global generation limit (default 256) |
| `--max-gens-per-flow` | Per-flow generation limit (default 32) |
| `--max-cache-bytes` | Cache memory cap (default 32MiB) |
| `--transit-hook identity` | Copy each ingress datagram through `recode_fn` |
| `--transit-hook plus-minus` | DATA payload ingress `+1`, egress `-1`; END/header unchanged |
| `--decode-reencode-stub` | Install Phase 3A stub (always OPAQUE) |
| `--local-decode` | Enable local-destination decode |
| `--source FILE` | Local file/FIFO encode → inject |
| `--codec` | Codec (requires `--local-decode` and/or `--source`) |
| `--final-dst` / `--ttl` | Required with `--source` |
| `--flow-id` / `--rate-mbps` | Optional source pacing |
| `--output` | L1 single-flow output file |
| `--output-dir` | L2 multi-flow output directory |
| `--return-hop` | Where to send Wirehair ACK (`WIRE_FLAG_RETURN_PATH`) |
| `--wh-segment-mib` / `--wh-repair-pct` | Segment size; no-ACK repair budget (ACK sprays until ACK, cap 100% source) |
| `--wh-window` | Receiver segment window (default 8, max 16) |
| `--wh-ack` / `--no-wh-ack` | Destination emits ACK; source stops leftover repair |

Wirehair (`--codec wirehair`) uses wire **v4**. ACK is optional (`--wh-ack`)
and must be routed back with `--return-hop` on each hop toward the source.
Library API (same v4, no sockets):
**[docs/FEC_TRANSPORT.md](../../docs/FEC_TRANSPORT.md)**.
Pipeline notes: **[docs/WIRE_RELAY_PIPELINE.md](../../docs/WIRE_RELAY_PIPELINE.md)**.

### Relay benchmark cases

- Case b (copy): sender/receiver use `--codec copy`; relay uses the default
  opaque transit path.
- Case c (+1/-1): sender/receiver use `--codec block`; relay adds
  `--transit-hook plus-minus`. Sender `+1`/receiver `-1` and relay
  ingress `+1`/egress `-1` each close independently.

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

Same binary on every node. Mid-hop stays opaque forward until Phase 3A/3B.

```text
VM1 --source  →  VM2 forward  →  VM3 forward  →  VM4 --local-decode
final_dst=4      local=2          local=3          local=4
```

```bash
# VM4
./build/wire_relay --local-node-id 4 --listen 9000 --next-hop 127.0.0.1:9 \
  --local-decode --codec copy --output /tmp/out.bin --idle-exit-sec 30

# VM3 / VM2
./build/wire_relay --local-node-id 3 --listen 9000 --next-hop VM4_IP:9000
./build/wire_relay --local-node-id 2 --listen 9000 --next-hop VM3_IP:9000

# VM1
./build/wire_relay --local-node-id 1 --listen 9000 --next-hop VM2_IP:9000 \
  --source input.bin --final-dst 4 --ttl 8 --codec copy
```

`wg_multi_pipeline --udp-send` / `--udp-recv` remain valid as source/sink.

## Loopback / unit tests

```bash
make wire-relay wg-demo
sh tests/wire_relay_loopback.sh ./build/wg_multi_pipeline ./build/wire_relay build
./build/relay_gen_cache_tests
./build/relay_egress_queue_tests
./build/relay_deferred_tests
./build/relay_local_decode_tests
./build/relay_local_source_tests
```
