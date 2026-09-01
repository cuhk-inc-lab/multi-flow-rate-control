# Building an encoder, relay, and decoder

How to assemble a three-node PFC (Wirehair / wire v4) path using this
repository. Three integration levels exist; pick one and keep **matching
config** on every hop that touches the same `flow_id`.

| Level | Encoder | Relay | Decoder | When to use |
| --- | --- | --- | --- | --- |
| **A. `fec_transport` library** | `fec_encoder_*` | Your UDP forward loop **or** `wire_relay` binary | `fec_decoder_*` | Embed in your own process; you own sockets |
| **B. Ready-made binaries** | `wg_multi_pipeline --udp-send` | `wire_relay` (opaque) | `wg_multi_pipeline --udp-recv` | Fastest bring-up, no C integration |
| **C. Segment modules** | `wirehair_segment_sender` + emit callback | `wire_relay` **or** custom TTL decrement + `sendto` | `wirehair_segment_receiver` | Same logic as binaries; full control over I/O |

All three produce the **same wire v4 datagrams**. Mixing v3 codecs (`copy`,
`rs`, …) with Wirehair on one flow will fail.

Related docs: [PFC.md](PFC.md) · [FEC_TRANSPORT.md](FEC_TRANSPORT.md) ·
[PFC_ARCHITECTURE.md](PFC_ARCHITECTURE.md) · [PFC_EXPERIMENTS.md](PFC_EXPERIMENTS.md)

---

## 1. Topology

Typical lab (one relay hop):

```text
Node1 (encoder)          Node2 (relay)              Node3 (decoder)
10.10.12.1               10.10.12.2 listen          10.10.23.2 listen
local-node-id = 1        local-node-id = 2          local-node-id = 4

DATA:  udp-send → 10.10.12.2:PORT  →  forward → 10.10.23.2:PORT
ACK:   ack-port ← 10.10.12.1       ←  return  ← (decoder emits ACK)
```

Routing uses **wire header** fields, not UDP destination alone:

| Field | Encoder sets | Relay does | Decoder |
| --- | --- | --- | --- |
| `final_dst` | Ultimate sink node id (e.g. `4`) | If `!= local_node_id`, decrement `ttl`, forward | Must equal `--local-node-id` |
| `ttl` | Hop budget (e.g. `8`) | Decrement each forward hop | — |
| `flow_id` | Demux key | Opaque; learns per-flow return route for ACK | Must match sender |
| `origin_node` | Source node id (ACK target) | ACK `final_dst` points here | Copied into ACK |

UDP `sendto` address is always the **next hop IP:port**, not the final sink.

---

## 2. Config that must match

On every encoder/decoder pair for the same flow:

| Parameter | Encoder | Decoder | Notes |
| --- | --- | --- | --- |
| Codec | `FEC_CODEC_WIREHAIR` / `--codec wirehair` | same | |
| `segment_bytes` / `--wh-segment-mib` | same | same | Default 10 MiB |
| `window` / `--wh-window` | same | same | Default 8; memory ≈ `window × segment_bytes` |
| `ack_enabled` / `--wh-ack` | same | same | Both on or both off |
| `flow_id` | same | same | Default `0` |
| `repair_percent` / `--wh-repair-pct` | same | same | ACK mode: cap only; repair rounds are 5% |

Relay does **not** decode; it only forwards opaque v4 datagrams and routes
ACKs back.

---

## 3. Level A — `fec_transport` library (recommended embed)

**Headers:** `include/fec_transport.h`  
**Sources:** `src/fec_transport.c`, `src/wire_header.c`, plus Wirehair objects
(see Makefile `FEC_TRANSPORT_TEST_BIN` link line).

### 3.1 Build / link

```bash
make fec-transport    # builds and runs unit tests
```

Link your binary with at least:

```text
fec_transport.o  wire_header.o  wg_wirehair_segment.o  wg_wirehair_segment_sender.o
+ third_party/wirehair (libwirehair.a)
```

Copy the pattern from `Makefile` target `fec_transport_tests`.

### 3.2 Encoder

```c
#include "fec_transport.h"

static FecOutputStatus send_udp(void *ctx, const uint8_t *data, size_t len)
{
    int sock = *(int *)ctx;
    ssize_t n = sendto(sock, data, len, 0, next_hop_addr, addrlen);
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return FEC_OUTPUT_BLOCKED;
    return n == (ssize_t)len ? FEC_OUTPUT_OK : FEC_OUTPUT_ERROR;
}

FecTransportConfig cfg;
fec_transport_config_init(&cfg);
cfg.codec = FEC_CODEC_WIREHAIR;
cfg.segment_bytes = 2u * 1024u * 1024u;  /* 2 MiB */
cfg.repair_percent = 10;
cfg.window = 4;
cfg.ack_enabled = 1;
cfg.origin_node = 1;
cfg.final_dst = 4;
cfg.ttl = 8;
cfg.flow_id = 0;
cfg.wire_rate_bps = 200u * 1000000u / 8u;  /* optional pacing */

FecCallbacks enc_cb = {.output = send_udp, .ctx = &tx_sock};
FecEncoder *enc = fec_encoder_create(&cfg, &enc_cb);
```

**Event loop (ACK mode):**

```c
uint64_t now_ns = monotonic_ns();

/* application data */
fec_encoder_push(enc, buf, len, now_ns);

/* end of stream */
fec_encoder_flush(enc);

/* drive repair timing + drain output queue */
fec_encoder_update(enc, now_ns);
fec_encoder_drain(enc, 32);   /* budget = max datagrams this call */

/* on ACK socket RX */
fec_encoder_input_ack(enc, ack_pkt, ack_len);
```

| API | Role |
| --- | --- |
| `fec_encoder_push` | Buffer app bytes; admit full segments |
| `fec_encoder_flush` | Flush partial segment; queue v4 END |
| `fec_encoder_update` | **Required in ACK mode** — repair micro-round timing |
| `fec_encoder_drain` | Invoke `output` callback for queued datagrams |
| `fec_encoder_input_ack` | Stop repair for acknowledged segment |

No-ACK mode: `ack_enabled = 0`; `update` still fills the queue but repair
timing is simpler (one segment at a time).

### 3.3 Decoder

```c
static FecOutputStatus write_app(void *ctx, const uint8_t *data, size_t len)
{
    fwrite(data, 1, len, (FILE *)ctx);
    return FEC_OUTPUT_OK;
}

static FecOutputStatus send_ack_udp(void *ctx, const uint8_t *data, size_t len)
{
    /* send ACK datagram toward previous hop (or direct to origin_node) */
    return send_udp(ctx, data, len);
}

FecCallbacks dec_cb = {
    .output = write_app,
    .ack_output = cfg.ack_enabled ? send_ack_udp : NULL,
    .ctx = out_fp,
};
FecDecoder *dec = fec_decoder_create(&cfg, &dec_cb);

/* on DATA / repair / END RX */
fec_decoder_input(dec, pkt, len, now_ns);
```

Recovered segment bytes arrive on `output`. When `ack_enabled`, one v4 ACK
datagram is emitted on `ack_output` per recovered segment — **you must send
it** (directly or via relay).

### 3.4 Relay with the library

The library does **not** include a relay. Two options:

**Option 1 — use `wire_relay` binary (usual):** encoder and decoder use
`fec_transport`; middle node runs `wire_relay` unchanged.

**Option 2 — forward in your process:**

```c
WireHeader hdr;
if (wire_header_decode(&hdr, pkt, len) != 0)
    return;

if (hdr.ttl == 0) { /* drop */ return; }

if (hdr.final_dst == my_node_id) {
    fec_decoder_input(dec, pkt, len, now_ns);
    return;
}

hdr.ttl--;
/* re-encode header into pkt[0..], sendto(next_hop) */
```

ACK datagrams with `WIRE_FLAG_RETURN_PATH` must be forwarded toward
`origin_node` (relay learns per-flow UDP return from the forward path).

---

## 4. Level B — ready-made binaries

No C code. Same v4 format as the library.

### 4.1 Build

```bash
make wg-demo wire-relay
```

### 4.2 Decoder (Node3)

```bash
./build/wg_multi_pipeline \
  --codec wirehair --wh-ack \
  --wh-segment-mib=2 --wh-window=4 \
  --local-node-id 4 \
  --udp-recv 9000 /tmp/out.bin \
  --idle-sec 60 --strict
```

### 4.3 Relay (Node2)

Opaque forward; does not decode PFC.

```bash
./build/wire_relay \
  --local-node-id 2 \
  --listen 9000 \
  --next-hop 10.10.23.2:9000 \
  --return-hop 10.10.12.1:9100 \
  --idle-exit-sec 120
```

`--return-hop` is a fallback until the relay learns the sender's UDP endpoint
for each `flow_id`. After the first forward DATA, ACKs route automatically.

### 4.4 Encoder (Node1)

```bash
./build/wg_multi_pipeline \
  --codec wirehair --wh-ack --ack-port=9100 \
  --wh-segment-mib=2 --wh-window=4 \
  --local-node-id 1 --final-dst 4 --ttl 8 \
  --rate-mbps 200 \
  --udp-send 10.10.12.2 9000 /tmp/input.bin
```

Verify: `sha256sum` of `input.bin` and `out.bin` must match.

Full VM commands and failure demos: [PFC_EXPERIMENTS.md](PFC_EXPERIMENTS.md) §4.2.

---

## 5. Level C — segment modules (binary-style control)

Used internally by `wg_multi_pipeline` (`wire_udp.c`) and, for ACK send,
`fec_transport` (`wirehair_segment_sender.c`).

**Headers:** `apps/wg_multi_pipeline/wirehair_segment.h`,
`apps/wg_multi_pipeline/wirehair_segment_sender.h`

### 5.1 Encoder with sliding window (ACK mode)

```c
#include "wirehair_segment_sender.h"

static int my_emit(const WireHeader *hdr, const uint8_t *payload,
                   size_t payload_len,
                   WirehairSegmentSenderPacketKind kind, void *ctx)
{
    /* build full datagram, sendto(next_hop); return 0 ok, 1 backpressure, -1 error */
    (void)kind;
    return send_v4_datagram(ctx, hdr, payload, payload_len);
}

WirehairSegmentConfig wh;
wirehair_segment_config_defaults(&wh);
wh.segment_bytes = 2u * 1024u * 1024u;
wh.repair_percent = 10;
wh.window = 4;
wh.ack_enabled = true;
wh.origin_node = 1;

WirehairSegmentSender *sender = wirehair_segment_sender_create(
    &wh, flow_id, final_dst, ttl, my_emit, tx_ctx);

/* per segment */
wirehair_segment_sender_admit(sender, segment_id, data, data_len);

/* main loop */
wirehair_segment_sender_tick(sender, now_ns);
wirehair_segment_sender_input_ack(sender, ack_datagram, ack_len);

/* after all input */
wirehair_segment_sender_mark_input_finished(sender);
wirehair_segment_sender_try_emit_end(sender);
```

### 5.2 Decoder

```c
static int on_segment(uint32_t flow_id, const uint8_t *data, size_t len, void *ctx)
{
    (void)flow_id;
    fwrite(data, 1, len, (FILE *)ctx);
    return 0;
}

static int on_ack(const WireHeader *ack, void *ctx)
{
    return send_ack_datagram(ctx, ack);
}

WirehairSegmentReceiver *rx = wirehair_segment_receiver_create(
    &wh, flow_id, on_segment, app_ctx,
    wh.ack_enabled ? on_ack : NULL, ack_ctx);

/* per datagram */
wirehair_segment_receiver_ingest(rx, &hdr, payload, payload_len);
```

### 5.3 Relay

Do not reimplement unless necessary. Run `wire_relay` or forward opaque
datagrams with TTL/`final_dst` checks as in §3.4.

---

## 6. ACK path end-to-end

```text
1. Encoder sets WIRE_FLAG_ACK_REQUEST on DATA (library/binary does this).
2. Decoder recovers segment → emits v4 ACK (type=ACK, block_id=segment_id).
3. If relay sits in the middle:
     - Forward DATA: relay records (flow_id → previous_hop UDP).
     - Return ACK: relay sends to learned hop (or --return-hop fallback).
4. Encoder receives ACK → fec_encoder_input_ack / wirehair_segment_sender_input_ack
     → stops repair for that segment.
```

Bind a fixed `--ack-port` / `ack_port` on the encoder when a relay needs
`--return-hop HOST:PORT` before route learning.

---

## 7. Minimal single-process loop (library, loopback test)

Pattern from `tests/fec_transport_tests.c`:

```c
fec_encoder_push(enc, payload, payload_len, now_ns);
fec_encoder_flush(enc);

while (fec_encoder_has_pending(enc)) {
    fec_encoder_update(enc, now_ns);
    fec_encoder_drain(enc, 8);
    for (each new wire datagram) {
        fec_decoder_input(dec, dg, dg_len, now_ns);
        for (each ACK captured from dec_cb.ack_output)
            fec_encoder_input_ack(enc, ack, ack_len);
    }
}
```

Run: `make fec-transport`

---

## 8. Checklist before shipping

- [ ] Encoder and decoder: same `segment_bytes`, `window`, `ack_enabled`, `flow_id`
- [ ] `final_dst` equals decoder `local-node-id`
- [ ] `ttl` ≥ number of relay hops
- [ ] Encoder `origin_node` matches your node id; ACK socket reachable
- [ ] Relay: `--next-hop` points at downstream; `--return-hop` matches encoder ACK bind
- [ ] Callbacks (`output`, `ack_output`) never block; return `FEC_OUTPUT_BLOCKED` on `EAGAIN`
- [ ] ACK mode: call `fec_encoder_update(enc, now_ns)` between drains
- [ ] Do not mix RS (`FEC_CODEC_RS`) and Wirehair on the same flow

---

## 9. File map

| Component | Primary files |
| --- | --- |
| Library API | `include/fec_transport.h`, `src/fec_transport.c` |
| Wire header | `include/wire_header.h`, `src/wire_header.c` |
| Segment codec | `apps/wg_multi_pipeline/wirehair_segment.c` |
| ACK window sender | `apps/wg_multi_pipeline/wirehair_segment_sender.c` |
| Binary sender/receiver | `apps/wg_multi_pipeline/wire_udp.c`, `main.c` |
| Relay | `apps/wire_relay/relay.c`, `apps/wire_relay/main.c` |
| Unit tests | `tests/fec_transport_tests.c` |
| Integration tests | `tests/wire_wirehair_test.sh`, `tests/wire_relay_loopback.sh` |
