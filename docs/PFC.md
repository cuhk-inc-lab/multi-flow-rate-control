# PFC (Piecewise Fountain Code) — Usage Guide

CLI name: `--codec wirehair`  
Wire format: header **v4**  
Codec library: `third_party/wirehair`

This document describes **system structure, runtime logic, configuration, and
usage**. After reading it you should be able to deploy direct, relay, and
multi-flow setups on your own.

---

## Table of contents

1. [System structure](#1-system-structure)
2. [Data units](#2-data-units)
3. [Wire format v4](#3-wire-format-v4)
4. [Sender logic](#4-sender-logic)
5. [Receiver logic](#5-receiver-logic)
6. [ACK and repair](#6-ack-and-repair)
7. [Sliding window](#7-sliding-window)
8. [Deployment topologies](#8-deployment-topologies)
9. [Multi-flow](#9-multi-flow)
10. [Relay behavior](#10-relay-behavior)
11. [Rate limiting](#11-rate-limiting)
12. [Logs and metrics](#12-logs-and-metrics)
13. [CLI reference](#13-cli-reference)
14. [Source files](#14-source-files)
15. [Testing](#15-testing)
16. [Limits](#16-limits)

---

## 1. System structure

### 1.1 Layers

```text
┌──────────────────────────────────────────────────────────┐
│  Binaries                                                 │
│  wg_multi_pipeline   sender / receiver                   │
│  wire_relay          transit / local source / local decode│
└────────────┬─────────────────────────┬───────────────────┘
             │                         │
┌────────────▼────────────┐ ┌──────────▼──────────┐
│  wire_udp.c             │ │  relay.c            │
│  UDP I/O, pacing,       │ │  forward, ACK return,│
│  sliding window,        │ │  dual egress queues, │
│  recvmmsg, decode workers│ │  per-flow route learn│
└────────────┬────────────┘ └──────────┬──────────┘
             │                         │
┌────────────▼─────────────────────────▼──────────┐
│  wirehair_segment.c                              │
│  Segmentation, encode/decode, window slots, ACK  │
└────────────┬────────────────────────────────────┘
             │
┌────────────▼────────────┐
│  third_party/wirehair   │
│  Per-segment fountain codec│
└─────────────────────────┘
```

### 1.2 Transfer flow (single file)

```text
file
  → split into segments of segment_bytes (segment 0, 1, 2, …)
  → each segment: independent Wirehair encode → many UDP DATA packets
  → receiver decodes → writes file in segment-id order
  → sender sends END (block_id = total segment count)
```

### 1.3 Program entry points

| Binary | Role |
| --- | --- |
| `wg_multi_pipeline` | End-to-end sender (`--udp-send`), receiver (`--udp-recv`), multi-flow (`--udp-send-multi`) |
| `wire_relay` | Opaque mid-hop forward; optional `--source` sender or `--local-decode` receiver |

Not compatible with v3 codecs (`copy`, `xor-fec`, `rs`, etc.). Both ends must
use `--codec wirehair`.

---

## 2. Data units

### 2.1 Segment

| Item | Description |
| --- | --- |
| Default size | 10 MiB (`--wh-segment-mib`) |
| Id | `block_id`, starts at 0 |
| Encoding | One independent Wirehair session per segment |
| Minimum pad | Segments shorter than `2 × 1370` bytes are padded before encode |

### 2.2 Packet

| Item | Value |
| --- | --- |
| Max payload | 1370 bytes (`WH_PACKET_SIZE`) |
| In-segment id | `shard_index` = Wirehair packet id |
| Type | source packet (id < source_packets) or repair packet (id ≥ source_packets) |

### 2.3 Packet count formulas

```text
source_packets = max(2, ceil(segment_bytes / 1370))
repair_packets = max(2, ceil(source_packets × repair_pct / 100))
```

| Mode | Total send per segment |
| --- | --- |
| No ACK | `source_packets + repair_packets`, then advance to next segment |
| ACK on | Send all source first; append repair in 5% micro-rounds until ACK or `source_packets` cap |

### 2.4 Default constants (`wirehair_segment.h`)

| Constant | Value |
| --- | --- |
| `WH_SEGMENT_WINDOW_DEFAULT` | 8 |
| `WH_SEGMENT_WINDOW_MAX` | 16 |
| `WH_ACK_REPAIR_ROUND_PCT` | 5 (ACK repair round size, % of source packets) |
| `WH_ACK_INITIAL_WAIT_MS` | 50 |
| `WH_ACK_REPAIR_WAIT_MS` | 100 |

---

## 3. Wire format v4

Definition: `include/wire_header.h`

### 3.1 Header fields (52 bytes)

| Field | PFC meaning |
| --- | --- |
| `version` | 4 |
| `type` | `DATA` / `END` / `ACK` |
| `final_dst` | Ultimate destination node id |
| `ttl` | Remaining hops (relay decrements per hop) |
| `flow_id` | Stream id (receiver demux key) |
| `block_id` | **Segment id** |
| `shard_index` | **In-segment packet id** |
| `shard_count` | Advertised packet-id ceiling for this segment |
| `payload_len` | Payload length |
| `origin_node` | Source node id (ACK `final_dst` target) |
| `flags` | `ACK_REQUEST` / `RETURN_PATH` |
| `segment_bytes` | Original segment byte count |

Routing uses **`final_dst` + `ttl`**, not UDP destination IP. Send UDP to the
**next hop** address; set `final_dst` to the final sink node.

### 3.2 Packet types

| type | Payload | Purpose |
| --- | --- | --- |
| `DATA` | ≤ 1370 B | source or repair |
| `END` | none | `block_id` = total segment count |
| `ACK` | none | `(flow_id, segment_id)` recovered |

### 3.3 Flags

| flag | Set by | Purpose |
| --- | --- | --- |
| `WIRE_FLAG_ACK_REQUEST` | Sender | Receiver emits ACK after recovery |
| `WIRE_FLAG_RETURN_PATH` | Receiver / relay | Marks ACK; forward on return path |

---

## 4. Sender logic

Implementation: `wire_udp.c` → `wirehair_udp_send_file()`  
Per-segment state: `wirehair_segment.c` → `WirehairSegmentTx`

### 4.1 Main loop

```text
while data remains or unacked segments in window:
  poll ACKs
  release acked base_segment slots
  if window not full → read next segment, create WirehairSegmentTx
  for each active slot:
    source incomplete → emit source batch (up to 32 packets)
    source complete and repair_due → emit one 5% repair round
send END
```

### 4.2 No-ACK mode

Serial per segment:

```text
emit source_packets packets
  → emit repair_packets packets
  → next segment
```

Total repair is fixed by `--wh-repair-pct`.

### 4.3 ACK mode (sliding window)

```text
in-flight segments: [base_segment, base_segment + window)

slot ring: slots[segment_id % window]

per-slot state machine:
  create tx → emit source → wait 50ms → [no ACK] emit 5% repair round → wait 100ms → repeat
  on ACK → mark acked
  when base_segment slot acked → release slot, base_segment++

repair total reaches source_packets with no ACK → ack_timeout, send fails
```

ACK registry: `wirehair_udp_ack_mark` / `wirehair_udp_ack_drain` (out-of-order
and duplicate ACKs).

Multi-flow: shared pacer `max_inflight == window`; total in-flight segments
across all flows ≤ window.

### 4.4 Sender log line

```text
wirehair-send: source_bytes=… segments=… repair_sent=… wire_bytes=…
               ack=on|off status=ok|failed repair_rounds=… send_window_hwm=…
```

---

## 5. Receiver logic

Implementation: `wirehair_udp_recv_file()` + `wirehair_segment_receiver_*`

### 5.1 Thread layout

```text
main thread (RX)
  poll + recvmmsg (up to 64 UDP datagrams per batch)
  → validate v4 header
  → enqueue by flow_id

one decode worker thread per flow
  → wirehair_segment_receiver_ingest()
  → on recovery → write file + send ACK (to datagram source address)
```

### 5.2 Per-flow state

| State | Meaning |
| --- | --- |
| `next_emit_segment` | Next segment id to write to disk |
| `slots[window]` | Up to `window` parallel decode slots |
| `ahead_window_drops` | Packets dropped for being beyond the window |

### 5.3 Ingest table

| Condition | Action |
| --- | --- |
| `segment_id < next_emit_segment` | If `ACK_REQUEST` set → re-send ACK |
| `segment_id ≥ next_emit_segment + window` | Drop, `ahead_window_drops++` |
| New segment in window | Allocate decoder, `wirehair_decode()` |
| Enough symbols | `wirehair_recover()` → ACK → try in-order write |
| `END` received | Record total segments; complete when `next_emit_segment` catches up |

Segments may recover out of order but are **written in segment-id order**.

---

## 6. ACK and repair

### 6.1 ACK packet contents

```text
type          = ACK
final_dst     = origin_node (from DATA header)
flags         = RETURN_PATH
flow_id       = stream id
block_id      = segment id
segment_bytes = recovered byte count
```

### 6.2 Sender ACK socket

- Bind `--ack-port` (OS-assigned if omitted)
- Poll ACK socket while sending
- Multi-flow: each flow may use a different source port; relay records return
  route per `flow_id`

### 6.3 Per-segment timeline (ACK mode)

```text
emit all source packets for segment
  → wait 50ms
  → no ACK: emit 5% repair round (repair_rounds++)
  → wait 100ms
  → repeat until ACK or repair reaches source_packets cap
```

| Parameter | Role in ACK mode |
| --- | --- |
| `--wh-repair-pct` | Repair budget in no-ACK mode; in ACK mode used for `shard_count` validation and 100% cap only |
| 5% micro-round | Always `WH_ACK_REPAIR_ROUND_PCT`, independent of `--wh-repair-pct` |

### 6.4 `repair_sent` meaning

Counts **repair packet ids** actually transmitted (excludes source packets).
Often 0 on loss-free paths.

---

## 7. Sliding window

Sender and receiver share `--wh-window` (default 8, max 16).

```text
sender admit:   segment_id ∈ [base_segment, base_segment + window)
receiver accept: segment_id ∈ [next_emit_segment, next_emit_segment + window)
```

| Event | Sender | Receiver |
| --- | --- | --- |
| Window full | Stop reading new segments | — |
| Segment too far ahead | — | Drop, `ahead_window_drops++` |
| Repair exhausted, no ACK | `ack_timeout`, exit failure | — |

Peak receiver memory per flow ≈ `window × segment_bytes` (default ~80 MiB).

---

## 8. Deployment topologies

### 8.1 Direct

```bash
# Node2 receiver
./build/wg_multi_pipeline --codec wirehair --wh-ack \
  --udp-recv 9000 /tmp/out.bin --local-node-id 4 --idle-sec 5 --strict

# Node1 sender
./build/wg_multi_pipeline --codec wirehair --wh-ack --ack-port=9100 \
  --wh-segment-mib=2 --wh-repair-pct=20 --wh-window=8 \
  --final-dst 4 --ttl 8 --rate-mbps 1000 \
  --udp-send 10.10.12.2 9000 input.bin
```

```text
Node1 --udp-send--> Node2 --udp-recv
Node2 --ACK--------> Node1 --ack-port
```

### 8.2 Relay (three nodes)

```text
Node1 sender
  → Node2 relay (opaque, learns per-flow return route)
  → Node3 receiver

ACK: Node3 → relay → Node1 --ack-port
```

```bash
# Node2 transit
./build/wire_relay --local-node-id 2 --listen 9000 \
  --next-hop 10.10.23.2:9000

# Node3 receiver
./build/wg_multi_pipeline --codec wirehair --wh-ack \
  --udp-recv 9000 /tmp/out.bin --local-node-id 4 --strict

# Node1 sender
./build/wg_multi_pipeline --codec wirehair --wh-ack --ack-port=9100 \
  --final-dst 4 --ttl 8 --udp-send 10.10.12.2 9000 input.bin
```

Use `--return-hop` only when the relay has **not yet learned** the return
address for that `flow_id`.

### 8.3 Lab VMs (matrix script)

| Node | SSH | Data network |
| --- | --- | --- |
| Node1 | `fyp1@10.10.10.161` | `10.10.12.1` |
| Node2 | `fyp1@10.10.10.162` | `10.10.12.2` |
| Node3 | `fyp1@10.10.10.163` | `10.10.23.2` |

---

## 9. Multi-flow

### 9.1 Send

```bash
./build/wg_multi_pipeline --codec wirehair --wh-ack --udp-send-multi \
  --flow "0:10.10.12.2:9000:input0.bin:500" \
  --flow "1:10.10.12.2:9000:input1.bin:500"
```

- `flow_id` is carried in the wire header
- Rate limit: per-flow `rate_mbps` values are **summed** for aggregate wire cap
- In-flight segments: shared `window` across all flows

### 9.2 Receive

```bash
./build/wg_multi_pipeline --codec wirehair --wh-ack \
  --udp-recv 9000 /tmp/out_ --max-flows 4 --local-node-id 4
```

- Demux by `flow_id` only, not UDP 5-tuple
- Output path: `{prefix}src_{ip}_p{port}_flow_{id}.{suffix}`

---

## 10. Relay behavior

### 10.1 Forward path

```text
UDP in → ttl==0 drop
      → final_dst==local → local-decode / local-source ACK delivery
      → else TTL-- → enqueue DATA egress → sendto(next-hop)
```

DATA / repair / END: **not decoded**, opaque forward.

### 10.2 ACK return path

```text
ACK + RETURN_PATH → ack egress (capacity 1024)
  → lookup per-flow learned previous-hop UDP address
  → sendto return path
  → if no route, use --return-hop
```

TX scheduling: prefer ACK; after 8 consecutive ACKs, send 1 DATA if waiting.

### 10.3 Relay as source

With `--source`, ACKs arriving locally are stored in
`source_ack_valid[segment_id % 16]` for the encoder thread to poll.

---

## 11. Rate limiting

`pace_to_source_rate()` limits **wire bytes** (header + payload).

| Scenario | Behavior |
| --- | --- |
| `--udp-send --rate-mbps N` | Single-flow wire rate ≤ N Mbps |
| `--udp-send-multi` | Aggregate wire rate ≤ sum of flow rates |
| `--no-pace` | No pacing |
| Repair packets | Count toward `wire_bytes` |

---

## 12. Logs and metrics

### 12.1 Sender summary

| Field | Meaning |
| --- | --- |
| `source_bytes` | Application payload bytes |
| `segments` | Segment count |
| `repair_sent` | Repair packets sent |
| `wire_bytes` | Total on-wire bytes (headers included) |
| `repair_rounds` | ACK repair round count |
| `send_window_hwm` | Peak in-flight segment count |
| `status` | `ok` / `failed` |

### 12.2 Script-derived metrics

| Metric | Formula |
| --- | --- |
| goodput | `source_bytes × 8 / wall_s / 1e6` Mbps |
| wire | `wire_bytes × 8 / wall_s / 1e6` Mbps |

### 12.3 Receiver

| Field | Meaning |
| --- | --- |
| `ahead_window_drops` | Packets dropped beyond receive window |
| `socket_rcvbuf` | Kernel-granted `SO_RCVBUF` |

### 12.4 Failure indicators

| Log | Meaning |
| --- | --- |
| `ack_timeout=yes` | Segment hit repair cap without ACK |
| `status=failed` | Send failed |

### 12.5 Relay summary

`ack_egress_*`, `data_egress_*`: queue high watermarks, wait times, enqueue counts.

---

## 13. CLI reference

### 13.1 `wg_multi_pipeline`

| Flag | Default | Description |
| --- | --- | --- |
| `--codec wirehair` | — | Enable PFC |
| `--wh-segment-mib=N` | 10 | Segment size (MiB) |
| `--wh-repair-pct=P` | 10 | No-ACK repair %; ACK mode cap helper |
| `--wh-window=N` | 8 | Sliding window (max 16) |
| `--wh-ack` / `--no-wh-ack` | off | Segment-level ACK |
| `--ack-port=N` | auto | Sender ACK listen port |
| `--final-dst=N` | 4 | Final destination node |
| `--ttl=N` | 8 | Hop budget |
| `--local-node-id=N` | 4 | This node's id |
| `--flow-id=N` | 0 | Single-flow wire id |
| `--rate-mbps=N` | unlimited | Wire rate cap |
| `--udp-send H P FILE` | — | Send |
| `--udp-recv P OUT` | — | Receive |
| `--udp-send-multi --flow …` | — | Multi-flow send |
| `--max-flows=N` | — | Max receive flows |
| `--strict` | — | Fail on incomplete output |
| `--idle-sec=N` | — | Receiver idle timeout |
| `--no-pace` | — | Disable pacing |

### 13.2 `wire_relay`

| Flag | Description |
| --- | --- |
| `--local-node-id` | This node's id |
| `--listen` / `--next-hop` | Listen port and next hop |
| `--local-decode --codec wirehair` | Local decode to disk |
| `--source FILE --codec wirehair` | Local encode and inject |
| `--wh-segment-mib` / `--wh-repair-pct` / `--wh-window` / `--wh-ack` | Same as wg_multi_pipeline |
| `--return-hop HOST:PORT` | ACK return fallback |
| `--final-dst` / `--ttl` | Required with `--source` |

---

## 14. Source files

| File | Role |
| --- | --- |
| `include/wire_header.h` | v4 header definition |
| `wirehair_segment.h/c` | Segment encode/decode, window, ACK |
| `wire_udp.h/c` | UDP I/O, sliding window, pacing, recvmmsg |
| `pipeline.c` | Multi-flow scheduling, shared pacer |
| `main.c` | CLI |
| `relay.c` | Forward, ACK return, route learning |
| `egress_queue.c` | ACK/DATA dual queues |
| `local_source.c` / `local_decode.c` | Relay-embedded send/receive |
| `fec_transport.c` | Socket-free library API (see `FEC_TRANSPORT.md`) |
| `scripts/vm_wirehair_full_matrix.py` | VM regression matrix |

Library embedding (without `wg_multi_pipeline` binary): see
[`FEC_TRANSPORT.md`](FEC_TRANSPORT.md).  
The library has no sliding window; windowing lives in `wire_udp.c`.

---

## 15. Testing

### 15.1 Local

```bash
make wg-demo wire-relay
make integration-test
./build/wirehair_segment_tests
```

### 15.2 VM matrix

```bash
python3 scripts/vm_wirehair_full_matrix.py
WH_MATRIX_ACK_ONLY=1 python3 scripts/vm_wirehair_full_matrix.py   # ACK cases only
WH_MATRIX_FRESH=1 python3 scripts/vm_wirehair_full_matrix.py      # ignore prior JSON
```

Dimensions: direct / relay × ACK on/off × 1/2/4 flows × 500/1000/2000/5000 Mbps  
Output: `build/wirehair_full_matrix.json`

### 15.3 Host tuning (before full-rate runs)

```bash
sudo sysctl -w net.core.rmem_max=67108864
sudo sysctl -w net.core.wmem_max=67108864
sudo sysctl -w net.core.netdev_max_backlog=5000
```

Confirm receiver log shows `socket_rcvbuf` near 64M.

---

## 16. Limits

- Wire v4 only; do not mix with v3 codecs
- `source_packets` ≤ 64000 per segment
- `shard_count` ≤ uint16 maximum
- `window` ≤ 16
- ACK mode repair cap per segment = `source_packets` (100%)
- No authentication; verify with `sha256sum` or `--strict`
- Relay drops on full queue (`drop_egress_full`); no delivery guarantee under overload

---

## Related docs

| Document | Contents |
| --- | --- |
| [`apps/wg_multi_pipeline/README.md`](../apps/wg_multi_pipeline/README.md) | Program CLI quick reference |
| [`apps/wire_relay/README.md`](../apps/wire_relay/README.md) | Relay CLI |
| [`WIRE_RELAY_PIPELINE.md`](WIRE_RELAY_PIPELINE.md) | Relay pipeline details |
| [`FEC_TRANSPORT.md`](FEC_TRANSPORT.md) | Library API |
| [`SCRIPTS.md`](SCRIPTS.md) | Script guide |
