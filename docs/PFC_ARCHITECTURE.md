# PFC architecture handbook

Piecewise Fountain Code inside this repository: layers, packet path, functions,
and library embedding. CLI recipes and VM runs are in
[PFC_EXPERIMENTS.md](PFC_EXPERIMENTS.md).

---

## 1. Place in the repository

Three PFC implementations share wire v4:

```text
┌───────────────────────────────────────────────────────────┐
│  Ready-made binaries                                         │
│  apps/wg_multi_pipeline   sender / receiver / multi-flow     │
│  apps/wire_relay          opaque forward / --source / decode │
│                                                              │
│  sliding window, recvmmsg, ACK socket, pacing live HERE      │
│  wire_udp.c  pipeline.c  relay.c  egress_queue.c             │
│           │                                                  │
│           ▼                                                  │
│  apps/wg_multi_pipeline/wirehair_segment.c                   │
│  per-segment encode/decode, window slots, ACK headers        │
└─────────────────────────────────────────────────────────┴────────────────────────────────┘
                           │ same v4 datagrams
┌─────────────────────────┴──────────────────────────────────┐
│  Socket-free library                                         │
│  src/fec_transport.c  +  include/fec_transport.h             │
│  codec = FEC_CODEC_WIREHAIR                                  │
│  NO sliding window: one segment session at a time            │
│  caller owns UDP; drain() / decoder_input()                  │
└─────────────────────────┴──────────────────────────────────┘
                           │
                           ▼
                 third_party/wirehair
                 wirehair_encoder_create / encode
                 wirehair_decoder_create / decode / recover
```

The core `src/flow_buffer.c` / `flow_manager.c` path is the **multi-flow rate
control library** (`libmulti_flow.a`). PFC UDP demos do **not** encode through
that buffer; they use `wire_udp` + `wirehair_segment`.
`fec_transport` is the embeddable cousin of the same v4 format.

v3 RS/XOR/`copy` stays on `FEC_CODEC_RS` / `block_codec`. Do not mix versions
on one flow.

---

## 2. Data units and wire v4

Defined in `include/wire_header.h` (52-byte v4 header).

| Unit | Meaning |
| --- | --- |
| Segment | Application bytes, default 10 MiB; id = `block_id` |
| Packet | ≤ 1370 B (`WH_PACKET_SIZE` / `PKG_SIZE`); id = `shard_index` |
| Source packet | `shard_index < source_packets` |
| Repair packet | `shard_index ≥ source_packets` |

Header fields PFC uses:

| Field | Role |
| --- | --- |
| `version` | 4 |
| `type` | `DATA` / `END` / `ACK` |
| `final_dst` + `ttl` | Routing (relay). UDP IP is only the next hop |
| `flow_id` | Demux key |
| `block_id` | Segment id (`END` carries total segment count) |
| `shard_index` / `shard_count` | Packet id / advertised ceiling |
| `origin_node` | ACK `final_dst` |
| `flags` | `WIRE_FLAG_ACK_REQUEST`, `WIRE_FLAG_RETURN_PATH` |
| `segment_bytes` | Original segment length (unpadded) |

Short segments are padded to `2 × 1370` before `wirehair_encoder_create`.

---

## 3. Packet path (binaries)

### 3.1 Sender (`wg_multi_pipeline --udp-send`)

```text
main.c  parse --codec wirehair
  → pipeline.c  (multi-flow: shared pacer, max_inflight = window)
    → wirehair_udp_send_file()           wire_udp.c
         poll ACK socket
         wirehair_udp_ack_mark / _drain  (OOO + duplicate ACK)
         if window has room: read file slice, WirehairSegmentTx
         per slot:
           emit source batches (≤ 32 packets)
           if ACK mode and repair_due: 5% repair round
         send END
         pace_to_source_rate()           wire bytes
```

No-ACK: serial `source_packets` then `repair_packets` then next segment.

ACK slot machine (`WirehairSegmentTx` in `wirehair_segment.c`):

```text
create tx → emit source → wait 50 ms
  → no ACK: emit 5% repair → wait 100 ms → repeat
  → ACK: mark acked
  → base_segment slot acked: release, base_segment++
  → repair total == source_packets: ack_timeout, fail
```

Window: `segment_id ∈ [base_segment, base_segment + window)`.
Slot ring: `slots[segment_id % window]`.

### 3.2 Receiver (`--udp-recv`)

```text
wirehair_udp_recv_file()
  main thread: poll + recvmmsg (≤ 64)
    validate v4 → enqueue by flow_id
  per-flow decode worker:
    wirehair_segment_receiver_ingest()
      recover → write in-order → ACK to datagram source address
```

Ingest rules:

| Condition | Action |
| --- | --- |
| `segment_id < next_emit_segment` | Re-ACK if `ACK_REQUEST` |
| `segment_id ≥ next_emit + window` | Drop, `ahead_window_drops++` |
| New in-window segment | Allocate decoder, `wirehair_decode()` |
| Enough symbols | `wirehair_recover()` → ACK → try emit |
| `END` | Record total; done when `next_emit` catches up |

### 3.3 Relay (`apps/wire_relay`)

```text
UDP in → ttl==0 drop
      → final_dst==local → local_decode / source ACK delivery
      → else TTL-- → DATA egress → sendto(next-hop)

ACK + RETURN_PATH → ack egress (cap 1024)
  → per-flow learned prev-hop UDP
  → else --return-hop
```

`egress_queue.c`: prefer ACK; after 8 ACKs, one DATA if waiting.  
`--source` stores ACKs in `source_ack_valid[segment_id % 16]` for the encoder
thread. Local encode/decode: `local_source.c` / `local_decode.c`.

---

## 4. Function map

### 4.1 Segment codec — `wirehair_segment.c` / `.h`

| Symbol | Role |
| --- | --- |
| `wirehair_segment_config_defaults` / `_valid` | Fill / check `segment_bytes`, `repair_percent`, `ack_enabled`, nodes |
| `wirehair_segment_source_packets` | `max(2, ceil(bytes/1370))` |
| `wirehair_segment_repair_packets` | From source count × percent |
| `WirehairSegmentTx` | One encode session: packet ids, repair rounds, acked flag |
| `wirehair_segment_receiver_create` / `_destroy` | Per-flow decode window |
| `wirehair_segment_receiver_ingest` | DATA/END into slots; emit app bytes + ACK header via callbacks |

This file does **not** open sockets. Binaries and `fec_transport` both call it
(library decoder uses the receiver; library encoder currently drives
`wirehair_*` directly for the send session).

### 4.2 UDP and window — `wire_udp.c` / `.h`

| Symbol | Role |
| --- | --- |
| `wirehair_udp_send_file` | File → segments → windowed send + END |
| `wirehair_udp_recv_file` | Socket loop, workers, files |
| `wirehair_udp_ack_mark` / `_drain` | ACK registry |
| `pace_to_source_rate` | Token / sleep on header+payload bytes |

Sliding window exists only here (and relay source that mirrors it), not in
`fec_transport`.

### 4.3 Multi-flow scheduling — `pipeline.c`

Shares one pacer across `--udp-send-multi` flows. `max_inflight` equals
`--wh-window` for the **process**, not per flow.

### 4.4 CLI — `apps/wg_multi_pipeline/main.c`

Parses `--codec`, `--wh-*`, `--udp-send*`, `--flow`, node ids. Relay CLI is
`apps/wire_relay` `main` / `relay.c`.

### 4.5 Library — `fec_transport`

| API | Role |
| --- | --- |
| `fec_transport_config_init` | Zero + safe defaults |
| `fec_encoder_create` / `_destroy` / `_reset` | Encoder; Wirehair allocates `wh_buf` of `segment_bytes` |
| `fec_encoder_push` | Append app bytes; start session when segment full |
| `fec_encoder_flush` | Encode partial segment; empty flush still queues v4 END |
| `fec_encoder_update` | Idle flush timeout + fill + drain |
| `fec_encoder_drain(budget)` | `callbacks.output`; skip queued PARITY if `wh_acked` |
| `fec_encoder_input_ack` | Set `wh_acked` if ACK `flow_id`+`block_id` match current session |
| `fec_encoder_has_pending` / `_get_stats` | Queue / session leftover |
| `fec_decoder_create` | Wraps `WirehairSegmentReceiver` |
| `fec_decoder_input` | Parse v4; ignore ACK; ingest DATA/END |
| `FecCallbacks.output` | Recovered app bytes (must not block; `FEC_OUTPUT_BLOCKED` retries) |
| `FecCallbacks.ack_output` | Full v4 ACK datagram toward the source |

Config fields (Wirehair): `segment_bytes`, `repair_percent`, `ack_enabled`,
`origin_node`, `final_dst`, `ttl`, `ack_ttl`, `flow_id`, `flush_timeout_ns`,
`output_queue_*`, `wire_rate_bps`.

Library ACK drops leftover queued repair for the **current** session. Binary ACK
mode uses a **window of segments** and 5% micro-rounds. The two control loops
are not the same.

---

## 5. Embedding

### 5.1 Binaries

See [PFC_EXPERIMENTS.md](PFC_EXPERIMENTS.md). Same v4 as the library.

### 5.2 `fec_transport` (caller-owned sockets)

```c
FecTransportConfig cfg;
fec_transport_config_init(&cfg);
cfg.codec = FEC_CODEC_WIREHAIR;
cfg.segment_bytes = 1u << 20;
cfg.repair_percent = 10;     /* 0 means default 10, not “no repair” */
cfg.ack_enabled = 1;
cfg.flow_id = 7;
cfg.origin_node = 1;
cfg.final_dst = 4;
cfg.ttl = 8;

FecCallbacks enc_cb = {.output = send_udp, .ctx = tx_sock};
FecCallbacks dec_cb = {
    .output = write_app,
    .ack_output = send_ack_udp,
    .ctx = ctx,
};
FecEncoder *enc = fec_encoder_create(&cfg, &enc_cb);
FecDecoder *dec = fec_decoder_create(&cfg, &dec_cb);

fec_encoder_push(enc, data, len, now_ns);
fec_encoder_flush(enc);
fec_encoder_drain(enc, SIZE_MAX);

/* RX */
if (is_ack)
    fec_encoder_input_ack(enc, pkt, len);
else
    fec_decoder_input(dec, pkt, len, now_ns);
```

Caller duties: decrement TTL when forwarding; route ACK; do not mix RS and
Wirehair on one encoder; match `flow_id` / `segment_bytes` / `ack_enabled` on
both ends.

Reference tests: `tests/fec_transport_tests.c`
(`test_wirehair_roundtrip_and_drop`, `test_wirehair_ack_stops_repair`).

### 5.3 Pipeline changes

Multi-segment window, recvmmsg, and the multi-flow pacer belong in
`wire_udp.c` / `wirehair_segment.c`, not `fec_transport`.

---

## 6. Source files

| Path | Duty |
| --- | --- |
| `include/wire_header.h` | v3/v4 header |
| `apps/wg_multi_pipeline/wirehair_segment.h/.c` | Segment sessions, receiver window |
| `apps/wg_multi_pipeline/wire_udp.h/.c` | UDP, sliding window, pacing |
| `apps/wg_multi_pipeline/pipeline.c` | Multi-flow + shared pacer |
| `apps/wg_multi_pipeline/main.c` | CLI |
| `apps/wire_relay/relay.c` | Forward, ACK return, route learn |
| `apps/wire_relay/egress_queue.c` | ACK/DATA lanes |
| `src/fec_transport.c` | Socket-free RS + Wirehair |
| `include/fec_transport.h` | Public library API |
| `third_party/wirehair` | Fountain math |
| `scripts/vm_wirehair_full_matrix.py` | VM matrix |
| `docs/FEC_TRANSPORT.md` | Library-only guide (RS + Wirehair) |

---

## 7. Tests

```bash
make fec-transport
./build/wirehair_segment_tests
make integration-test
```

Window or ACK timing changes need the VM matrix, not only loopback.

---

## Related

[PFC_EXPERIMENTS.md](PFC_EXPERIMENTS.md) ·
[FEC_TRANSPORT.md](FEC_TRANSPORT.md) ·
[WIRE_RELAY_PIPELINE.md](WIRE_RELAY_PIPELINE.md) ·
[INTEGRATION_BOUNDARIES.md](INTEGRATION_BOUNDARIES.md)
