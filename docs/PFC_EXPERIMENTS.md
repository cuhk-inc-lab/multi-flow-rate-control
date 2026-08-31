# PFC experiment handbook

Run Piecewise Fountain Code in this repo: binaries, flags, topologies,
tuning, metrics, and tests. Call graphs and library APIs are in
[PFC_ARCHITECTURE.md](PFC_ARCHITECTURE.md).

CLI name: `--codec wirehair`  
Wire format: header **v4**  
Binaries: `build/wg_multi_pipeline`, `build/wire_relay`

---

## 1. Overview

PFC splits a file into **segments** (default 10 MiB). Each segment is an
independent Wirehair fountain session and becomes many UDP DATA packets of at
most 1370 bytes. The receiver recovers segments (possibly out of order) and
writes them in segment-id order. The sender then emits END (`block_id` = total
segment count).

Two ACK modes:

| Mode | When to use | Repair behavior |
| --- | --- | --- |
| `--no-wh-ack` (default) | No return path, or fixed redundancy | After source packets, send `--wh-repair-pct` repair, then next segment |
| `--wh-ack` | ACKs can return (direct or via relay) | Send all source; then 5% repair micro-rounds until ACK or 100% source cap |

Both ends **must** use `--codec wirehair`. Mixing with RS/XOR/copy will fail.

---

## 2. Build

```bash
make wg-demo wire-relay
make integration-test          # includes wire loopback
./build/wirehair_segment_tests
make fec-transport             # library tests (no sockets)
```

Host buffers before high-rate runs:

```bash
sudo sysctl -w net.core.rmem_max=67108864
sudo sysctl -w net.core.wmem_max=67108864
sudo sysctl -w net.core.netdev_max_backlog=5000
```

Receiver log `socket_rcvbuf` should be near 64M.

---

## 3. Parameters

### 3.1 `wg_multi_pipeline` flags

| Flag | Default | Role | Notes |
| --- | --- | --- | --- |
| `--codec wirehair` | — | Selects v4 fountain path | Required on **every** PFC process |
| `--wh-segment-mib=N` | 10 | Segment size in MiB | Smaller (1–2) for short files / low memory; 10 for throughput |
| `--wh-repair-pct=P` | 10 | No-ACK: extra packets as % of source. ACK mode: `shard_count` / 100% cap, **not** micro-round size | No ACK: set to expected loss (e.g. 20). ACK: 10 is fine |
| `--wh-window=N` | 8 (max 16) | In-flight segments on **sender and receiver** | Sender and receiver must match. Memory ≈ `N × segment_bytes` per flow |
| `--wh-ack` / `--no-wh-ack` | off | Segment-level ACK | On if a return path exists |
| `--ack-port=N` | OS-assigned | Sender bind port for ACK UDP | Pin it when a relay must know the return address |
| `--final-dst=N` | 4 | Ultimate sink **node id** (not IP) | Must equal receiver `--local-node-id` |
| `--ttl=N` | 8 | Hop budget; relay decrements | ≥ number of forward hops |
| `--local-node-id=N` | 4 | This node’s id | Receiver / relay identity |
| `--flow-id=N` | 0 | Single-flow demux key | Unique per concurrent stream on the same receiver |
| `--rate-mbps=N` | unlimited | Cap on **wire** bytes (header + payload) | Per-flow in multi-send, summed for aggregate cap |
| `--no-pace` | off | Disable pacing | Local stress tests only |
| `--udp-send H P FILE` | — | Send one file to next hop | `H:P` is **next hop**, not necessarily the sink |
| `--udp-recv P OUT` | — | Receive on port `P` | `OUT` is file or prefix |
| `--udp-send-multi --flow …` | — | Several flows in one process | See §5 |
| `--max-flows=N` | — | Receiver flow slots | ≥ number of senders |
| `--strict` | — | Fail if output incomplete | Use in experiments |
| `--idle-sec=N` | — | Stop after idle | Receiver teardown |

`--flow` syntax for multi-send:

```text
--flow "flow_id:next_hop_ip:port:path:rate_mbps"
```

### 3.2 `wire_relay` flags

| Flag | Role |
| --- | --- |
| `--local-node-id` | Drop / local-deliver when `final_dst` matches |
| `--listen` / `--next-hop` | Ingress port and DATA next hop |
| `--return-hop HOST:PORT` | ACK fallback **before** the relay has learned this `flow_id` |
| `--local-decode --codec wirehair` | This hop is the sink |
| `--source FILE --codec wirehair` | This hop is the encoder (`--final-dst` and `--ttl` required) |
| `--wh-segment-mib` / `--wh-repair-pct` / `--wh-window` / `--wh-ack` | Same meaning as pipeline |

DATA / repair / END are **opaque**. The relay does not decode PFC.

### 3.3 Formulas

```text
source_packets = max(2, ceil(segment_bytes / 1370))
repair_packets = max(2, ceil(source_packets × repair_pct / 100))   # no-ACK send size

ACK micro-round = 5% of source_packets          # WH_ACK_REPAIR_ROUND_PCT
ACK repair cap  = source_packets                # 100% extra, then ack_timeout
```

Constants (`wirehair_segment.h`): window default 8 / max 16; initial ACK wait
50 ms; later repair wait 100 ms.

### 3.4 Pitfalls

- **UDP destination IP** = next hop. **`final_dst`** = sink node id.
- **`--wh-repair-pct`** is not the ACK spray size. ACK spray is always 5%.
- **`--wh-window`** is shared across all flows in one sender process
  (`max_inflight == window`).
- Library `fec_transport` `repair_percent == 0` means “use 10%”, not “no repair”.

---

## 4. Topologies

### 4.1 Direct (two nodes)

```text
Node1 --udp-send--> Node2 --udp-recv
Node2 --ACK--------> Node1 --ack-port
```

```bash
# Node2
./build/wg_multi_pipeline --codec wirehair --wh-ack \
  --udp-recv 9000 /tmp/out.bin --local-node-id 4 --idle-sec 5 --strict

# Node1
./build/wg_multi_pipeline --codec wirehair --wh-ack --ack-port=9100 \
  --wh-segment-mib=2 --wh-repair-pct=20 --wh-window=8 \
  --final-dst 4 --ttl 8 --rate-mbps 1000 \
  --udp-send 10.10.12.2 9000 input.bin
```

Without ACK, drop `--wh-ack` / `--ack-port` and raise `--wh-repair-pct` to cover loss.

### 4.2 Relay (three nodes)

```text
Node1 sender → Node2 relay (opaque, learns per-flow return) → Node3 receiver
ACK: Node3 → relay → Node1 --ack-port
```

```bash
# Node2
./build/wire_relay --local-node-id 2 --listen 9000 \
  --next-hop 10.10.23.2:9000

# Node3
./build/wg_multi_pipeline --codec wirehair --wh-ack \
  --udp-recv 9000 /tmp/out.bin --local-node-id 4 --strict

# Node1
./build/wg_multi_pipeline --codec wirehair --wh-ack --ack-port=9100 \
  --final-dst 4 --ttl 8 --udp-send 10.10.12.2 9000 input.bin
```

Use `--return-hop` on the relay only until it has learned the `flow_id` return
address (first DATA from that flow).

### 4.3 Lab VM matrix

| Node | SSH | Data network |
| --- | --- | --- |
| Node1 | `fyp1@10.10.10.161` | `10.10.12.1` |
| Node2 | `fyp1@10.10.10.162` | `10.10.12.2` |
| Node3 | `fyp1@10.10.10.163` | `10.10.23.2` |

```bash
python3 scripts/vm_wirehair_full_matrix.py
WH_MATRIX_ACK_ONLY=1 python3 scripts/vm_wirehair_full_matrix.py
WH_MATRIX_FRESH=1 python3 scripts/vm_wirehair_full_matrix.py
```

Dimensions: direct / relay × ACK on/off × 1/2/4 flows × 500/1000/2000/5000 Mbps.  
Output: `build/wirehair_full_matrix.json`.

---

## 5. Multi-flow

```bash
./build/wg_multi_pipeline --codec wirehair --wh-ack --udp-send-multi \
  --flow "0:10.10.12.2:9000:input0.bin:500" \
  --flow "1:10.10.12.2:9000:input1.bin:500"

./build/wg_multi_pipeline --codec wirehair --wh-ack \
  --udp-recv 9000 /tmp/out_ --max-flows 4 --local-node-id 4
```

- Demux is **`flow_id` only**, not the UDP 5-tuple.
- Output: `{prefix}src_{ip}_p{port}_flow_{id}.{suffix}`.
- Aggregate wire cap = sum of per-flow `rate_mbps`.
- In-flight segments for the whole process ≤ `--wh-window`.

---

## 6. Recommended setups

| Goal | Starting point |
| --- | --- |
| First bring-up | `--wh-segment-mib=1 --wh-window=4 --wh-ack --rate-mbps 100 --strict` |
| Lossy path, ACK available | `--wh-ack`, leave repair-pct at 10, watch `repair_rounds` |
| Lossy path, no ACK | `--no-wh-ack --wh-repair-pct=20` (or measured loss + margin) |
| ~1 Gbps opaque relay | Keep defaults; tune host rmem; watch relay `*_egress_*` and `drop_egress_full` |
| Low memory receiver | `--wh-segment-mib=1 --wh-window=4` (~4 MiB/flow vs ~80 MiB default) |
| Many flows | Raise `--max-flows`; window is **not** per-flow on the sender |

Mismatch `--wh-window` (sender larger than receiver) → `ahead_window_drops`.  
Repair hits 100% with no ACK → `ack_timeout=yes`, `status=failed`.

---

## 7. Logs and metrics

Sender summary:

```text
wirehair-send: source_bytes=… segments=… repair_sent=… wire_bytes=… repair_rounds=… send_window_hwm=… status=…
```

| Field | Meaning |
| --- | --- |
| `source_bytes` | File payload |
| `segments` | Segment count |
| `repair_sent` | Repair **packet ids** actually sent (often 0 if no loss) |
| `wire_bytes` | On-wire bytes including headers |
| `repair_rounds` | ACK 5% rounds |
| `send_window_hwm` | Peak in-flight segments |
| `status` | `ok` / `failed` |
| `ack_timeout` | Repair cap hit, no ACK |

Derived: `goodput = source_bytes × 8 / wall_s / 1e6` Mbps,  
`wire = wire_bytes × 8 / wall_s / 1e6` Mbps.

Receiver: `ahead_window_drops`, `socket_rcvbuf`.  
Relay: `ack_egress_*`, `data_egress_*` (HWM, wait, enqueue). TX prefers ACK;
after 8 consecutive ACKs it sends 1 DATA if DATA is waiting.

Integrity: `sha256sum` on input vs output, or `--strict`. There is no auth.

---

## 8. Limits

- Wire v4 only.
- `source_packets` ≤ 64000 per segment; `shard_count` fits in uint16.
- `window` ≤ 16.
- ACK repair cap = `source_packets` (100%).
- Relay drops when egress is full (`drop_egress_full`).

---

## Related

[PFC_ARCHITECTURE.md](PFC_ARCHITECTURE.md) ·
[apps/wg_multi_pipeline/README.md](../apps/wg_multi_pipeline/README.md) ·
[apps/wire_relay/README.md](../apps/wire_relay/README.md) ·
[SCRIPTS.md](SCRIPTS.md) ·
[WIRE_RELAY_PIPELINE.md](WIRE_RELAY_PIPELINE.md)
