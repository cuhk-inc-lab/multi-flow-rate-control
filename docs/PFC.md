# PFC — Piecewise Fountain Code

**PFC** is this project's application-layer reliable transport built on top of
[Wirehair](https://github.com/catid/wirehair) fountain coding. A file (or byte
stream) is split into **segments** (pieces); each segment is encoded as an
independent fountain session. Segments are numbered, windowed, and optionally
acknowledged so the sender can stop repair early and keep multiple segments in
flight.

In code and CLI the same mechanism is exposed as `--codec wirehair` and wire
header **v4**. This document uses **PFC** as the system name and **Wirehair**
when referring to the underlying codec library.

---

## Table of contents

1. [Why piecewise?](#1-why-piecewise)
2. [System placement](#2-system-placement)
3. [Topology](#3-topology)
4. [Wire format (v4)](#4-wire-format-v4)
5. [Segment geometry](#5-segment-geometry)
6. [Sender behavior](#6-sender-behavior)
7. [Receiver behavior](#7-receiver-behavior)
8. [ACK and repair](#8-ack-and-repair)
9. [Sliding window](#9-sliding-window)
10. [Multi-flow](#10-multi-flow)
11. [Relay path](#11-relay-path)
12. [Rate limiting and pacing](#12-rate-limiting-and-pacing)
13. [Receive pipeline](#13-receive-pipeline)
14. [Metrics and logging](#14-metrics-and-logging)
15. [CLI reference](#15-cli-reference)
16. [Code map](#16-code-map)
17. [Library embedding (`fec_transport`)](#17-library-embedding-fec_transport)
18. [Host tuning](#18-host-tuning)
19. [Testing](#19-testing)
20. [Troubleshooting](#20-troubleshooting)
21. [Design limits](#21-design-limits)
22. [Related docs](#22-related-docs)

---

## 1. Why piecewise?

A naïve end-to-end fountain over an entire file has two problems on a multi-hop
UDP path:

1. **Head-of-line blocking** — the receiver cannot emit bytes until the whole
   object recovers.
2. **Unbounded repair** — without per-piece feedback, the sender must spray
   repair for the entire file before knowing what arrived.

PFC solves this by **piecewise** sessions:

```text
file bytes
  → segment 0 (fountain encode → UDP packets → fountain decode → write)
  → segment 1
  → segment 2
  → …
  → END
```

Each segment is a self-contained Wirehair object (typically up to 10 MiB).
Optional **segment-level ACK** lets the sender stop repair for a recovered
piece while other pieces continue. A **bounded sliding window** keeps up to
`window` segments in flight so ACK waits do not serialize the whole transfer.

---

## 2. System placement

```text
┌─────────────────────────────────────────────────────────────────┐
│  Application                                                    │
│  wg_multi_pipeline  (--udp-send / --udp-recv / --udp-send-multi)│
│  wire_relay         (--source / --local-decode, opaque forward)│
└────────────┬───────────────────────────────┬────────────────────┘
             │                               │
┌────────────▼────────────┐     ┌────────────▼────────────┐
│  wire_udp.c             │     │  relay.c                │
│  UDP I/O, pacing,       │     │  TTL/forward, ACK lane, │
│  sliding window,        │     │  per-flow return route  │
│  recvmmsg batch RX      │     │                         │
└────────────┬────────────┘     └────────────┬────────────┘
             │                               │
┌────────────▼───────────────────────────────▼────────────┐
│  wirehair_segment.c                                     │
│  Per-segment encode/decode, ACK emit, window slots      │
└────────────┬────────────────────────────────────────────┘
             │
┌────────────▼────────────┐
│  third_party/wirehair   │
│  Fountain codec (C API) │
└─────────────────────────┘
```

| Layer | Responsibility |
| --- | --- |
| **Wirehair** | Generate / consume fountain packets for one segment |
| **wirehair_segment** | Segment boundaries, window, ACK, repair policy |
| **wire_udp** | Sockets, pacing, multi-flow demux, decode workers |
| **wire_relay** | Multi-hop forward, ACK return path, fair egress |

PFC is **not** compatible with wire v3 systematic codecs (`copy`, `xor-fec`,
`rs-fec`, `rs`). Receivers reject v3 traffic when configured for Wirehair.

---

## 3. Topology

### Direct (single hop)

```text
Node1  wg_multi_pipeline --udp-send
         │  DATA / repair / END  (v4)
         ▼
Node2  wg_multi_pipeline --udp-recv

ACK (if enabled):
Node2  ──ACK──►  Node1 --ack-port
```

### Relay (multi-hop)

```text
Node1 sender
  → Node2 relay (opaque forward, learn return route per flow_id)
  → Node3 receiver (--local-decode or wg_multi_pipeline --udp-recv)

ACK:
Node3 → relay3 → relay2 → Node1 --ack-port
```

Relay mid-hops are **opaque**: they do not decode PFC payload. They classify
v4 `RETURN_PATH` ACK packets into a dedicated egress lane and forward them to
the UDP endpoint learned from forward DATA for that `flow_id`.

`--return-hop HOST:PORT` on each relay is only a **fallback** when no route has
been learned yet (e.g. ACK arrives before the first DATA for that flow).

### Lab VM layout (matrix tests)

| Node | Role | Interface |
| --- | --- | --- |
| Node1 (`10.10.10.161`) | Sender | `station0` → `10.10.12.1` |
| Node2 (`10.10.10.162`) | Direct sink or relay hop | `ap0` → `10.10.12.2` |
| Node3 (`10.10.10.163`) | Relay sink | `10.10.23.2` |

Direct matrix: Node1 → Node2.  
Relay matrix: Node1 → Node2 `wire_relay` → Node3.

---

## 4. Wire format (v4)

Defined in `include/wire_header.h`. Magic `WGP1`, version `4`.

### Header layout (52 bytes)

| Offset | Field | PFC meaning |
| --- | --- | --- |
| 0–3 | magic | `0x57475031` (WGP1) |
| 4 | version | `4` |
| 5 | type | `DATA`, `END`, or `ACK` |
| 6 | final_dst | Ultimate delivery node id |
| 7 | ttl | Hop budget (decremented per relay) |
| 8–11 | flow_id | Stream id (demux key on receiver) |
| 12–19 | block_id | **Segment id** |
| 20–21 | shard_index | **Wirehair packet id** within segment |
| 22–23 | shard_count | Advertised packet id ceiling for segment |
| 24–25 | valid_len | Reserved / legacy |
| 26–27 | payload_len | Fountain payload bytes |
| 28–43 | encode timestamps | Sender `CLOCK_REALTIME` (optional telemetry) |
| 44 | origin_node | Source node id (ACK `final_dst` target) |
| 45 | flags | `ACK_REQUEST`, `RETURN_PATH` |
| 46–47 | reserved | Zero |
| 48–51 | segment_bytes | Original segment payload size |

### Packet types

| Type | Payload | Purpose |
| --- | --- | --- |
| `DATA` | ≤ 1370 B fountain symbol | Source or repair packet |
| `END` | empty | `block_id` = total segment count |
| `ACK` | empty | `(flow_id, segment_id)` recovery complete |

### Flags

| Flag | Set by | Meaning |
| --- | --- | --- |
| `WIRE_FLAG_ACK_REQUEST` | Sender | Receiver should emit ACK on recovery |
| `WIRE_FLAG_RETURN_PATH` | Receiver / relay | ACK datagram; forward toward `origin_node` |

Routing uses **`final_dst` + `ttl`**, not UDP destination IP. Send to the
**next hop** address while setting `final_dst` to the ultimate sink node.

---

## 5. Segment geometry

Constants in `apps/wg_multi_pipeline/wirehair_segment.h`:

| Constant | Value | Meaning |
| --- | --- | --- |
| `WH_SEGMENT_DEFAULT_BYTES` | 10 MiB | Default segment size |
| `WH_PACKET_SIZE` | 1370 | Per-packet payload (MTU 1450 − IP/UDP/header) |
| `WH_SEGMENT_WINDOW_DEFAULT` | 8 | Default in-flight segment window |
| `WH_SEGMENT_WINDOW_MAX` | 16 | Hard max window |
| `WH_SEGMENT_DEFAULT_REPAIR_PCT` | 10 | Default repair budget (no-ACK mode) |
| `WH_REPAIR_MIN_PACKETS` | 2 | Floor on repair packet count |
| `WH_ACK_REPAIR_ROUND_PCT` | 5 | ACK micro-round size (% of source pkts) |
| `WH_ACK_INITIAL_WAIT_MS` | 50 | Wait after source before first repair round |
| `WH_ACK_REPAIR_WAIT_MS` | 100 | Wait between repair micro-rounds |
| `WH_ACK_POLL_SLICE_MS` | 10 | ACK socket poll slice |

### Packet counts

For segment payload size `S` bytes:

```text
source_packets = ceil(S / 1370), minimum 2
repair_packets = max(2, ceil(source_packets × repair_pct / 100))
```

**No ACK:** sender transmits `source_packets + repair_packets` then advances.

**ACK on:** `repair_pct` does **not** set the target redundancy. It only
influences geometry validation. The sender emits source once, then 5%
micro-rounds until ACK or the **safety cap** of `source_packets` repair packets
(100% of source). Hitting the cap without ACK is a hard segment failure
(`ack_timeout`).

Small segments are padded to at least `2 × WH_PACKET_SIZE` for the Wirehair
codec (minimum block size).

---

## 6. Sender behavior

Implementation: `wirehair_udp_send_file()` in `wire_udp.c`, segment logic in
`wirehair_segment.c`.

### High-level loop

```text
read up to segment_bytes from file
  → create WirehairSegmentTx (encoder)
  → emit source packets (batched)
  → [ACK mode] wait / repair micro-rounds until ACK or cap
  → advance segment id
send END (block_id = total segments)
```

### No-ACK mode

Serial per segment:

1. Emit all source packet ids `0 .. source_packets-1`.
2. Emit repair ids `source_packets .. source_packets+repair_budget-1`.
3. Move to next segment immediately.

Repair is **finite** and predetermined by `--wh-repair-pct`.

### ACK mode (sliding window)

The sender keeps a ring of up to `window` active `WirehairUdpSendSlot` entries:

```text
admit segment id only if  id ∈ [base_segment, base_segment + window)

for each active unacked slot (round-robin):
  if source incomplete → emit up to 32 source packets
  else if repair_due    → emit one 5% repair micro-round
  else                  → wait

on ACK for segment id:
  mark slot acked

when slot at base_segment is acked:
  release slot, base_segment++, free window credit
```

Key properties:

- Multiple segments can be in flight; ACK wait is **not** serial.
- Out-of-order and duplicate ACKs are recorded in an ACK registry
  (`wirehair_udp_ack_mark` / `_drain`).
- Shared pacer also enforces `max_inflight == window` across multi-flow sends.

### Send summary line

```text
wirehair-send: source_bytes=… segments=… repair_sent=… wire_bytes=…
               ack=on|off status=ok|failed repair_rounds=… send_window_hwm=…
```

---

## 7. Receiver behavior

Implementation: `wirehair_segment_receiver_*` + `wirehair_udp_recv_file()`.

### Per-flow state

Each `flow_id` gets:

- `WirehairSegmentReceiver` with `window` decode slots
- `next_emit_segment` — lowest segment not yet written to disk
- Rolling window: accepts segments in
  `[next_emit_segment, next_emit_segment + window)`

### Ingest rules

| Condition | Action |
| --- | --- |
| `segment_id < next_emit_segment` (late DATA) | Re-ACK if ACK requested (covers dropped ACK) |
| `segment_id ≥ next_emit_segment + window` | Drop, increment `ahead_window_drops` |
| New segment in window | Allocate decoder slot, `wirehair_decode()` |
| Enough symbols received | `wirehair_recover()`, emit ACK, try in-order write |
| `END` | Set expected segment count; complete when all emitted |

Segments are written **in order** by `segment_id` even if they recover out of
order (buffered in slots until the head is ready).

### Receive architecture

```text
RX thread (poll + recvmmsg batch, up to 64 datagrams)
  → validate header, demux by flow_id
  → enqueue to per-flow bounded queue

Per-flow decode worker thread
  → wirehair_segment_receiver_ingest()
  → fwrite recovered segment
  → send ACK to learned peer address
```

Decoupling RX from decode prevents bursty WiFi/UDP delivery from stalling the
socket while Wirehair recovery runs.

---

## 8. ACK and repair

### ACK datagram

```text
type        = ACK
final_dst   = origin_node (from DATA header)
flags       = RETURN_PATH
flow_id     = stream id
block_id    = segment id
segment_bytes = recovered segment size
(payload empty)
```

### Sender ACK socket

Sender binds `--ack-port` (default: ephemeral) and polls for ACKs while
transmitting. In multi-flow mode each flow may use a distinct source port;
relay learns the correct return path per `flow_id`.

### Repair timeline (ACK mode, one segment)

```text
t=0     emit all source packets (possibly batched with other segments)
t+50ms  if no ACK → repair micro-round 1 (5% of source_packets fresh ids)
t+150ms if no ACK → repair micro-round 2
…       until ACK or repair_sent == source_packets (100% cap) → fail
```

### Why `repair_sent = 0` on clean paths

`repair_sent` counts **repair packet ids actually transmitted**. On a path
with negligible UDP loss, segments recover from source packets alone before
the first repair micro-round fires, so `repair_sent` stays 0. This does **not**
mean the network is lossless — only that loss did not require repair.

Under induced loss (`tc netem`), `repair_sent` becomes non-zero while files
still verify.

### Per-round repair size

ACK micro-rounds always use `WH_ACK_REPAIR_ROUND_PCT` (5%), **not**
`--wh-repair-pct`. The CLI repair percent mainly affects:

- No-ACK mode budget
- Header `shard_count` ceiling validation
- ACK mode safety cap calculation (100% of source)

---

## 9. Sliding window

Sender and receiver share the same `window` (default 8, max 16).

```text
Receiver accept range:  [next_emit, next_emit + window)
Sender admit range:     [base_segment, base_segment + window)
```

| Scenario | Sender | Receiver |
| --- | --- | --- |
| Window full, waiting for ACK | Stops admitting new segments | — |
| Sender too far ahead | — | Drops packets, `ahead_window_drops++` |
| ACK for segment beyond base | Recorded, applied when slot opens | — |
| Segment hits repair cap | `ack_timeout`, send fails | May stall if sender wrongly advanced (prevented) |

**Memory (receiver, per flow):** up to `window × segment_bytes` decode
buffers. Default 8 × 10 MiB ≈ 80 MiB peak per flow.

**Metric `send_window_hwm`:** high-water mark of concurrent in-flight segments
on the sender (should stay ≤ `window`).

---

## 10. Multi-flow

### Sending

```bash
./build/wg_multi_pipeline --codec wirehair --wh-ack --udp-send-multi \
  --flow "0:10.10.12.2:9000:input0.bin:500" \
  --flow "1:10.10.12.2:9000:input1.bin:500"
```

- Each flow has its own `flow_id` in the wire header.
- Rate-limited multi-flow shares one **aggregate wire-byte pacer**:
  limit = sum of per-flow `rate_mbps`. This spaces packets across flows
  instead of synchronized bursts.
- Shared window: total in-flight segments across all flows ≤ `window`.

### Receiving

```bash
./build/wg_multi_pipeline --codec wirehair --wh-ack \
  --udp-recv 9000 /tmp/out_ --max-flows 4 --local-node-id 4
```

Demux is **only** by `WireHeader.flow_id`, not UDP 5-tuple. After relay, all
flows may share the same UDP source port — `flow_id` is the authoritative key.

Output naming:

```text
{prefix}src_{sender_ip}_p{sender_port}_flow_{flow_id}.{suffix}
```

---

## 11. Relay path

See also [`WIRE_RELAY_PIPELINE.md`](WIRE_RELAY_PIPELINE.md) and
[`apps/wire_relay/README.md`](../apps/wire_relay/README.md).

### Forward path

DATA / repair / END: opaque copy, TTL decrement, enqueue to DATA egress lane.

### ACK return path

1. Receiver emits ACK to its immediate previous hop (UDP `sendto` peer).
2. Each relay classifies `ACK + RETURN_PATH`:
   - If local `--source` is waiting for this ACK → deliver to encoder thread.
   - Else lookup per-`flow_id` learned return route → ACK egress lane.
3. TX worker: prefer ACK lane, but after 8 consecutive ACKs send 1 DATA
   packet if waiting (fairness).

### Relay + sliding window

Relay `source_ack_valid[]` mirrors sender window capacity
(`RELAY_SOURCE_ACK_CAPACITY = WH_SEGMENT_WINDOW_MAX`) so multiple in-flight
ACKs toward a local `--source` encoder are not overwritten.

### Direct vs relay (behavior)

| Aspect | Same? |
| --- | --- |
| Segment / window / ACK semantics | Yes |
| `repair_sent`, `send_window_hwm` meaning | Yes |
| Peak throughput | No — relay adds hop latency and queueing |
| ACK routing | Relay learns per-flow; direct is one UDP hop |

---

## 12. Rate limiting and pacing

`pace_to_source_rate()` sleeps so **wire bytes** (headers + payload) track the
configured Mbps. Pacing is based on monotonic time from send start.

| Mode | Pacing |
| --- | --- |
| Single `--udp-send --rate-mbps N` | Per-flow wire-byte pace |
| `--udp-send-multi` | Shared pacer, aggregate = sum of flow rates |
| `--no-pace` | No sleep between packets (benchmark / test) |

Pacing applies to repair packets too (they count toward `wire_bytes`).

---

## 13. Receive pipeline

### Socket buffer

Receiver requests 64 MiB `SO_RCVBUF` and logs:

```text
udp-recv: socket_rcvbuf=<granted> requested=67108864
```

If granted ≪ requested, raise `net.core.rmem_max` (see [Host tuning](#18-host-tuning)).

### Batch receive

`recvmmsg()` reads up to `WIREHAIR_RECV_BATCH` (64) datagrams per syscall.

### Per-flow queue + worker

Each flow has a bounded queue between RX and decode threads. The worker
retains the source `sockaddr` of each datagram so ACKs return to the correct
previous hop (critical for relay).

---

## 14. Metrics and logging

### Sender (`wirehair-send:` summary)

| Field | Definition |
| --- | --- |
| `source_bytes` | Application payload bytes read from file |
| `segments` | Segment count sent |
| `repair_sent` | Fountain repair packet ids transmitted (not source ids) |
| `wire_bytes` | Total on-wire bytes including headers (all packets) |
| `repair_rounds` | ACK repair micro-rounds executed |
| `send_window_hwm` | Max concurrent in-flight segments |
| `ack=on/off` | Whether ACK mode was enabled |
| `status=ok/failed` | Overall send result |

### Derived (benchmark scripts)

| Metric | Formula |
| --- | --- |
| **goodput** | `source_bytes × 8 / wall_s / 1e6` Mbps |
| **wire** | `wire_bytes × 8 / wall_s / 1e6` Mbps |
| **overhead** | `(wire − goodput) / wire` |

`wall_s` comes from the pipeline wall-clock timer around the send path.

### Receiver

| Field | Meaning |
| --- | --- |
| `ahead_window_drops` | Packets dropped because `segment_id` was beyond window |
| `socket_rcvbuf` | Kernel-granted receive buffer |
| `flow N worker started` | Per-flow decode worker and queue capacity |

### Failure indicators

| Log | Meaning |
| --- | --- |
| `ack_timeout=yes` | Segment exhausted 100% repair cap without ACK |
| `status=failed` | Send aborted (timeout, I/O, or pacing error) |
| `SEND_FAIL` (matrix) | Sender exited non-zero |

### Relay egress (summary)

`ack_egress_*` and `data_egress_*` queue high-water marks, wait times, and
enqueue counts — useful when ACK traffic competes with bulk DATA at high rate.

---

## 15. CLI reference

### `wg_multi_pipeline`

```bash
# Receiver
./build/wg_multi_pipeline --codec wirehair --wh-ack \
  --udp-recv 9000 /tmp/out.bin --local-node-id 4 --idle-sec 5 --strict

# Sender
./build/wg_multi_pipeline --codec wirehair --wh-ack --ack-port=9100 \
  --wh-segment-mib=2 --wh-repair-pct=20 --wh-window=8 \
  --final-dst 4 --ttl 8 --rate-mbps 1000 \
  --udp-send 10.10.12.2 9000 input.bin
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--codec wirehair` | — | Enable PFC / wire v4 |
| `--wh-segment-mib=N` | 10 | Segment size |
| `--wh-repair-pct=P` | 10 | No-ACK repair budget; ACK cap helper |
| `--wh-window=N` | 8 | Shared sender/receiver window (max 16) |
| `--wh-ack` / `--no-wh-ack` | off | Segment ACK |
| `--ack-port=N` | ephemeral | Sender ACK listen port |
| `--final-dst=N` | 4 | Ultimate sink node id |
| `--ttl=N` | 8 | Hop budget |
| `--local-node-id=N` | 4 | This node's id (receiver filter) |
| `--flow-id=N` | 0 | Wire flow id |
| `--rate-mbps=N` | unlimited | Wire-byte pacing |
| `--strict` | — | Fail if output incomplete |
| `--idle-sec=N` | — | Receiver idle timeout |

### `wire_relay`

```bash
# Mid-hop (opaque)
./build/wire_relay --local-node-id 2 --listen 9000 \
  --next-hop 10.10.23.2:9000

# Sink
./build/wire_relay --local-node-id 4 --listen 9000 \
  --next-hop 127.0.0.1:9 \
  --local-decode --codec wirehair --wh-ack \
  --output-dir /tmp/out

# Source
./build/wire_relay --local-node-id 1 --listen 9000 \
  --next-hop 10.10.12.2:9000 \
  --source input.bin --codec wirehair --wh-ack \
  --final-dst 4 --ttl 8 --return-hop 10.10.12.1:9100
```

Relay accepts the same `--wh-*` flags when `--codec wirehair` is set.

---

## 16. Code map

| File | Role |
| --- | --- |
| `include/wire_header.h` | v4 header layout and flags |
| `apps/wg_multi_pipeline/wirehair_segment.h/c` | Segment fountain, window, ACK |
| `apps/wg_multi_pipeline/wire_udp.h/c` | UDP send/recv, sliding window, pacing |
| `apps/wg_multi_pipeline/pipeline.c` | Multi-flow orchestration, shared pacer |
| `apps/wg_multi_pipeline/main.c` | CLI parsing |
| `apps/wire_relay/relay.c` | Forward, ACK lane, return routes |
| `apps/wire_relay/egress_queue.c` | Dual-lane fair egress |
| `apps/wire_relay/local_source.c` | Relay-embedded sender |
| `apps/wire_relay/local_decode.c` | Relay-embedded receiver |
| `include/fec_transport.h` / `src/fec_transport.c` | Socket-free library API |
| `third_party/wirehair/` | Vendored fountain codec |
| `scripts/vm_wirehair_full_matrix.py` | VM regression matrix |
| `tests/wire_wirehair_test.sh` | Local integration tests |
| `tests/wirehair_segment_tests.c` | Segment unit tests |

---

## 17. Library embedding (`fec_transport`)

For custom UDP loops without `wg_multi_pipeline`, use `fec_transport` with
`FEC_CODEC_WIREHAIR`. Same v4 on-wire format; caller supplies `output` and
`ack_output` callbacks.

See [`FEC_TRANSPORT.md`](FEC_TRANSPORT.md) §3 and §6.

Differences from the full binary path:

- No built-in sliding window in the library (single-segment `fec_encoder_push`)
- `wg_multi_pipeline` adds windowing, `recvmmsg`, relay integration, and
  multi-flow pacing on top of `wirehair_segment_*`

---

## 18. Host tuning

Before high-rate PFC benchmarks on Linux:

```bash
sudo sysctl -w net.core.rmem_max=67108864
sudo sysctl -w net.core.wmem_max=67108864
sudo sysctl -w net.core.netdev_max_backlog=5000
```

Confirm receiver log shows `socket_rcvbuf` near the requested 64 MiB.

For cross-host latency/jitter stats, synchronize clocks (e.g. Chrony) — wire
headers carry `CLOCK_REALTIME` encode timestamps.

---

## 19. Testing

### Local

```bash
make wg-demo wire-relay
make integration-test          # includes wire_wirehair_test.sh
./build/wirehair_segment_tests
```

`tests/wire_wirehair_test.sh` covers direct ACK, sliding window
(`send_window_hwm`), and `ahead_window_drops=0`.

### VM matrix

```bash
python3 scripts/vm_wirehair_full_matrix.py
WH_MATRIX_ACK_ONLY=1 python3 scripts/vm_wirehair_full_matrix.py
```

Matrix dimensions:

- Topology: direct, relay
- ACK: on, off (`ACK_ONLY` env skips no-ACK)
- Flows: 1, 2, 4
- Rates: 500, 1000, 2000, 5000 Mbps

Output: `build/wirehair_full_matrix.json` and `.log`.

### Loss injection (manual)

```bash
# On sender egress interface (example)
sudo tc qdisc replace dev station0 root netem loss 1%
# … run PFC transfer …
sudo tc qdisc del dev station0 root
```

Expect `repair_sent > 0` under loss; `repair_sent = 0` on clean paths.

---

## 20. Troubleshooting

| Symptom | Likely cause | What to check |
| --- | --- | --- |
| `ack_timeout=yes` | Loss + ACK loss exceeds repair cap | `repair_sent`, path loss, relay ACK lane drops |
| `ahead_window_drops > 0` | Sender window too aggressive vs receiver | Lower rate, increase `--wh-window`, check relay backlog |
| `repair_sent = 0` but OK | Clean path, no repair needed | Normal; inject `tc netem` to verify repair path |
| Identical `repair_sent` at 1%/3%/5% loss | All segments need ≥1 repair round; fixed 5% round size | Expected per-round budget behavior |
| Low goodput with relay | Extra hop + fair egress + WiFi | Compare direct; check `ack_egress_*` / `data_egress_*` |
| `socket_rcvbuf` ≪ 64M | sysctl cap | Raise `rmem_max` |
| RS receiver rejects stream | Codec mismatch | Both ends `--codec wirehair` |
| Multi-flow ACK wrong port | Demux by 5-tuple instead of `flow_id` | Use distinct `flow_id`; relay learns per flow |

---

## 21. Design limits

- Wire v4 only; no mixing with v3 RS/copy on the same encoder.
- Max ~64000 source packets per segment (Wirehair block count limit).
- `shard_count` must fit in `uint16_t`.
- Receiver window max 16 segments.
- ACK repair cap: 100% of source packets per segment (not unbounded fountain).
- Not authenticated: verify file hashes (`sha256sum`, `--strict`).
- `fec_transport` and `wg_multi_pipeline` share wire format but not all features
  (windowing lives in `wire_udp.c`).
- Relay opaque forward does not recode or trim repair — end-to-end PFC semantics
  are preserved, but relay queues can drop under overload (`drop_egress_full`).

---

## 22. Related docs

| Document | Contents |
| --- | --- |
| [`apps/wg_multi_pipeline/README.md`](../apps/wg_multi_pipeline/README.md) | CLI quick start, multi-flow |
| [`apps/wire_relay/README.md`](../apps/wire_relay/README.md) | Relay CLI, egress lanes |
| [`WIRE_RELAY_PIPELINE.md`](WIRE_RELAY_PIPELINE.md) | Relay pipeline design |
| [`FEC_TRANSPORT.md`](FEC_TRANSPORT.md) | Library API |
| [`SCRIPTS.md`](SCRIPTS.md) | VM matrix and other scripts |
| [`VM_NETWORK_BENCHMARK.md`](VM_NETWORK_BENCHMARK.md) | Raw path baselines (iperf) |
| [`tests/TESTING.md`](../tests/TESTING.md) | General test guide |
| [`third_party/wirehair/README.md`](../third_party/wirehair/README.md) | Wirehair codec internals |
