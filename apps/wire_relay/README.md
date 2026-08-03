# wire_relay

Single-threaded opaque UDP relay for wire header **v3** (`final_dst` + `ttl`).

## Build

```bash
# from multi-flow-rate-control repo root
make wire-relay
```

Binary: `build/wire_relay`.

## Role

```text
recvfrom(listen)
  → parse WireHeader
  → if final_dst == local_node_id → local delivery callback (default: count only)
  → else ttl-- → sendto(next-hop)   # payload unchanged
```

Does **not** encode, decode, or recode. Optional `RelayRecodeFn` exists but is
disabled by default (`NULL`).

## CLI

```bash
./build/wire_relay \
  --local-node-id 2 \
  --listen 9000 \
  --next-hop 10.10.23.2:9000 \
  [--idle-exit-sec N]
```

| Flag | Meaning |
|------|---------|
| `--local-node-id` | This node's id (Destination Check) |
| `--listen` | UDP bind port |
| `--next-hop` | `HOST:PORT` for non-local forward |
| `--idle-exit-sec` | Optional; exit after N seconds with no RX (tests) |

## Four-VM linear path

```text
VM1 encode+send  →  VM2 wire_relay  →  VM3 wire_relay  →  VM4 udp-recv+decode
final_dst=4         local=2             local=3             local_node_id=4
sendto(VM2)         next=VM3            next=VM4
```

Example:

```bash
# VM4
./build/wg_multi_pipeline --codec copy --local-node-id 4 \
  --udp-recv 9000 /tmp/out.ts --idle-sec 5

# VM3
./build/wire_relay --local-node-id 3 --listen 9000 --next-hop VM4_IP:9000

# VM2
./build/wire_relay --local-node-id 2 --listen 9000 --next-hop VM3_IP:9000

# VM1 (UDP destination is VM2, not VM4)
./build/wg_multi_pipeline --codec copy --final-dst 4 --ttl 8 \
  --udp-send VM2_IP 9000 input.ts
```

Application-layer relay requires VM1 to target VM2. Do not rely on kernel
`ip_forward` to skip the relay processes.

## Loopback test

```bash
make wire-relay wg-demo
sh tests/wire_relay_loopback.sh ./build/wg_multi_pipeline ./build/wire_relay build
```
