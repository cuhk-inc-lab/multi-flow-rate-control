# FEC transport library (`fec_transport`)

Socket-free library in `include/fec_transport.h` / `src/fec_transport.c`.
Callers own UDP (or any datagram path). The library queues wire datagrams
and the caller `drain()`s them.

Two independent backends share the same encoder/decoder API:

| `config.codec` | Wire | Coding | ACK |
| --- | --- | --- | --- |
| `FEC_CODEC_RS` (default) | v3 groups | Reed-Solomon `k+r` | none |
| `FEC_CODEC_WIREHAIR` | v4 segments | Wirehair fountain | optional v4 ACK |

The RS path is unchanged: same group window, metadata, recover, and
`data_shards` / `parity_shards` / `shard_size`. Enabling Wirehair does **not**
alter RS encode or decode.

This library is separate from `wg_multi_pipeline` / `wire_relay`. Those
binaries still encode Wirehair via `wirehair_segment_*` (see §6). Use
`fec_transport` when you want a non-blocking encoder/decoder you can plug
into your own UDP loop.

## 1. Event loop

```text
push(app bytes)  →  encode into output queue
drain(budget)    →  callbacks.output (DATA / repair / RS metadata)
decoder_input(rx datagram)
  → recovered app bytes on callbacks.output
  → Wirehair ACK datagrams on callbacks.ack_output  (if ack_enabled)
encoder_input_ack(ACK datagram)  → stop remaining repair for that segment
```

Typical poll:

```c
fec_encoder_push(enc, payload, n, now_ns);
fec_encoder_update(enc, now_ns);          /* flush timeout / fill queue */
fec_encoder_drain(enc, budget);           /* send as many as the socket allows */

/* on UDP RX */
if (looks_like_ack)
    fec_encoder_input_ack(enc, pkt, len);
else
    fec_decoder_input(dec, pkt, len, now_ns);
```

`output` / `ack_output` must not block. Return `FEC_OUTPUT_BLOCKED` if the
socket would block; the library keeps the datagram until a later `drain`.

## 2. Reed-Solomon (`FEC_CODEC_RS`)

```c
FecTransportConfig cfg;
fec_transport_config_init(&cfg);
cfg.codec = FEC_CODEC_RS;          /* default */
cfg.data_shards = 4;
cfg.parity_shards = 2;
cfg.shard_size = 1316;             /* optional; 0 = PKG_SIZE */

FecCallbacks cb = {.output = send_udp, .ctx = sock};
FecEncoder *enc = fec_encoder_create(&cfg, &cb);
FecDecoder *dec = fec_decoder_create(&cfg, &cb);
```

Leave `segment_bytes` / `repair_percent` / `ack_enabled` at 0. They are
ignored for RS. See `tests/fec_transport_tests.c` for group drop/recover
coverage.

## 3. Wirehair (`FEC_CODEC_WIREHAIR`)

Encoder accumulates app bytes until `segment_bytes` (or `flush`), then
emits fountain packets on wire **v4**. Decoder recovers the original
segment and, if ACK is on, emits one ACK datagram.

### Config

Call `fec_transport_config_init()` first, then set:

| Field | Meaning | Default if 0 |
| --- | --- | --- |
| `codec` | Must be `FEC_CODEC_WIREHAIR` | — |
| `segment_bytes` | Bytes gathered before a fountain session | 10 MiB |
| `repair_percent` | No ACK: extra packets as % of source (floor 2). ACK: cap only (100% source); binaries use 5% micro-rounds with short waits | 10 |
| `window` | Shared sender/receiver in-flight segment window | 8 |
| `ack_enabled` | Request ACK; encoder stops leftover repair | **off** (must set `1`) |
| `origin_node` | v4 `origin_node` | 1 |
| `final_dst` | Ultimate delivery node | wire default (4) |
| `ttl` | DATA/repair TTL | wire default (8) |
| `ack_ttl` | ACK TTL | same as `ttl` |
| `flow_id` | Wire `flow_id` (encoder and decoder must match) | 0 |
| `flush_timeout_ns` | Flush a partial segment after idle | 0 = only explicit `flush` |
| `output_queue_*` / `wire_rate_bps` | Same pacing as RS | library defaults |

`repair_percent == 0` is treated as “use default 10%”, not “no repair”.
To send almost no repair, set a small positive percent (for example `1`).

Encoder and decoder should use the **same** `flow_id`, `segment_bytes`,
and `ack_enabled`.

### Minimal roundtrip (no ACK)

```c
FecTransportConfig cfg;
fec_transport_config_init(&cfg);
cfg.codec = FEC_CODEC_WIREHAIR;
cfg.segment_bytes = 1u << 20;   /* 1 MiB; tests often use 4 KiB */
cfg.repair_percent = 10;
cfg.flow_id = 7;
cfg.origin_node = 1;
cfg.final_dst = 4;
cfg.ttl = 8;

FecCallbacks enc_cb = {.output = send_udp, .ctx = tx_sock};
FecCallbacks dec_cb = {.output = write_app, .ctx = app};
FecEncoder *enc = fec_encoder_create(&cfg, &enc_cb);
FecDecoder *dec = fec_decoder_create(&cfg, &dec_cb);

fec_encoder_push(enc, data, len, now_ns);
fec_encoder_flush(enc);                 /* last segment + v4 END */
fec_encoder_drain(enc, SIZE_MAX);

/* each received DATA/repair/END */
fec_decoder_input(dec, datagram, length, now_ns);
```

An empty `flush` with nothing buffered still sends a v4 END.

### ACK: stop leftover repair

1. Decoder: set `cfg.ack_enabled = 1` and `cb.ack_output = send_ack_udp`.
2. When a segment recovers, the library calls `ack_output` with a full
   v4 ACK datagram (header only). You send it back toward the source
   (direct UDP, or hop-by-hop as in `wire_relay --return-hop`).
3. Sender: on ACK RX, `fec_encoder_input_ack(enc, datagram, length)`.
   Remaining queued **parity** for that segment is dropped; no more
   repair is generated.

`wg_multi_pipeline` / `wire_relay` extend this with a bounded sender sliding
window: at most `window` segments may be in flight, ACKs may arrive
out-of-order, and unacknowledged segments receive timed 5% repair micro-rounds
until ACK or the 100%-of-source cap. Multi-flow sends share one aggregate
wire-byte pacer so flows do not burst in lockstep.

```c
cfg.ack_enabled = 1;

FecCallbacks dec_cb = {
    .output = write_app,
    .ack_output = send_ack_udp,   /* NULL = still recover, never stop repair */
    .ctx = ctx,
};

/* sender RX path */
fec_encoder_input_ack(enc, ack_datagram, ack_len);
```

The library never opens sockets. ACK return routing is the caller’s job.

`drain(1)` in a loop is a useful pattern: send one packet, immediately
feed the decoder (or the real network), then feed ACK back so repair
stops early.

## 4. Tests

```bash
make fec-transport          # build + run
# or
make -j4 build/fec_transport_tests && ./build/fec_transport_tests
```

Wirehair cases in `tests/fec_transport_tests.c`:

- full segment recovered with no ACK
- first fountain packet dropped, still recovered from repair
- ACK arrives mid-drain and `parity_datagrams_tx` stays small

## 5. What this library does not do

- No `sendto` / `recvfrom`
- No multi-hop TTL rewrite (you decrement TTL if you forward)
- No RS ↔ Wirehair mixing on one encoder
- RS groups are not fountain packets; a Wirehair decoder rejects v3 RS
  traffic (`FEC_ERR_NOT_FEC` / invalid header)

## 6. Ready-made binaries (not `fec_transport`)

`wg_multi_pipeline` and `wire_relay` implement the same v4 format with
`apps/wg_multi_pipeline/wirehair_segment.c`.

### Direct UDP (`wg_multi_pipeline`)

```bash
# receiver
./build/wg_multi_pipeline --codec wirehair --wh-ack \
  --udp-recv 9000 /tmp/out.bin --local-node-id 4 --idle-sec 5 --strict

# sender (ACK socket on --ack-port)
./build/wg_multi_pipeline --codec wirehair --wh-ack --ack-port=9100 \
  --wh-segment-mib=1 --wh-repair-pct=10 \
  --final-dst 4 --ttl 8 --flow-id 0 \
  --udp-send 10.10.12.2 9000 input.bin
```

Defaults: 10 MiB segments, 10% repair, ACK **off** (`--no-wh-ack`).
`--wh-ack` requests ACK as soon as the segment recovers.

### Relay path (`wire_relay`)

Forward hops stay opaque. Destination uses `--local-decode --codec wirehair`.
ACK datagrams set `WIRE_FLAG_RETURN_PATH` and follow `--return-hop`:

```text
source --source --wh-ack
  → relay --return-hop <previous>
  → dest --local-decode --wh-ack --return-hop <previous>
ACK travels the reverse hops to the source --ack-port.
```

See [`apps/wg_multi_pipeline/README.md`](../apps/wg_multi_pipeline/README.md),
[`apps/wire_relay/README.md`](../apps/wire_relay/README.md),
[`WIRE_RELAY_PIPELINE.md`](WIRE_RELAY_PIPELINE.md), and
[`PFC.md`](PFC.md) (full system overview).
