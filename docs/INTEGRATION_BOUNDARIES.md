# Integration Boundaries (wg-obfs / multi-flow-rate-control)

This document defines the **four handoff points** between `multi-flow-rate-control`
(multi), `buffer-management-module` (CircularBuffer), and wg-obfs (encode / network /
decode / downstream).

Reference implementation: `apps/wg_multi_pipeline/` (`make wg-demo`,
`make integration-test`).

Pipeline order: **multi before encode**.

```
UDP recvfrom
    → [1] ingress_push_tuple()  → FlowManager (5-tuple split + pacing)
    → per-flow pipe (raw bytes)
    → post_multi_in (CircularBuffer)
    → encode
    → sending_out (CircularBuffer)
    → [2] transmit (WiFi / socket / wg tunnel)
    → receiver_in (CircularBuffer)
    → [3] receive from network
    → decode
    → receiver_out (CircularBuffer)
    → [4] downstream consumer
```

Each **flow** has its own buffers, pipes, and workers. Flows do not share encoded
or decoded byte streams.

---

## Module ownership

| Layer | Owner | Responsibility |
|-------|-------|----------------|
| UDP ingress + 5-tuple routing | **multi** (`ingress_push`, `flow_peer_map`) | `ingress_push_tuple()` |
| MixedQueue + per-flow pacing | **multi** (`FlowManager`) | Split streams, rate match |
| Raw bytes after multi | **handoff pipe / fd** | FM worker `write(output_fd)` |
| CircularBuffer encode path | **buffer module** + wg-obfs glue | `post_multi_in` … `receiver_out` |
| Block encode / decode | **wg-obfs** (or demo `BlockCodec`) | `Codec_encode` / `Codec_decode` |
| Network transfer | **wg-obfs / network** | Replace demo `BufferTransfer_pump` |

---

## Boundary 1 — Upstream ingress (UDP → multi)

**Owner:** multi-flow-rate-control

**Entry API:**

```c
#include "ingress_push.h"

IngressPushStatus ingress_push_tuple(FlowManager *mgr,
                                     FlowPeerMap *map,
                                     const FlowTuple *tuple,
                                     const void *data,
                                     size_t len);
```

**Prerequisites:**

```c
flow_peer_map_init(&map, max_flows);
flow_manager_init(&mgr, &cfg);
flow_manager_start(&mgr);
/* per-flow output_fd → pipe write end (or downstream ingest) */
```

**Inputs (from wg-obfs):**

| Field | Source |
|-------|--------|
| `tuple.src` | `recvfrom` peer address |
| `tuple.dst` | local socket bind address (`getsockname`) |
| `tuple.protocol` | `IPPROTO_UDP` |
| `data`, `len` | UDP payload |

**Routing:** full 5-tuple `(src, dst, protocol)` is the key. `flow_id` is only the
internal slot index assigned on first sight (`flow_peer_map_lookup`).

**Output:** bytes enter **MixedQueue** → dispatcher → per-flow packet queue → paced
worker → `write(flow.output_fd)`.

**Not this boundary:** do **not** write UDP payload directly into CircularBuffer.
Use `ingress_push_tuple` only.

**File demo equivalent:** `ingress_push(mgr, fixed_flow_id, data, len)` in
`apps/wg_multi_pipeline/pipeline.c` (`pump_file_ingress`).

---

## Boundary 2 — Post-encode transmit

**Owner:** wg-obfs / network stack

**Source buffer (per flow):** `sending_out` (`CircularBuffer`)

Encoded data is produced after reading `DECODE_BLOCK` (752 B) from `post_multi_in`,
calling `Codec_encode`, and writing `ENCODE_BLOCK` (1504 B) to `sending_out`.

**wg-obfs responsibility:**

- Read encoded bytes from `sending_out` (per flow).
- Send over WiFi / UDP / TCP / wg tunnel to the receiver side.

**Demo substitute:** `BufferTransfer_pump(sending_out, receiver_in, …)` in
`apps/wg_multi_pipeline/buffer_transfer.c` — copies in-process instead of real I/O.

**Handoff contract:**

| Direction | Buffer | Operation |
|-----------|--------|-----------|
| Sender reads | `sending_out` | `Buffer_Read` or drain helper |
| Network | — | wg-obfs transport |
| Receiver writes | `receiver_in` | see Boundary 3 |

---

## Boundary 3 — Pre-decode receive

**Owner:** wg-obfs / network stack

**Destination buffer (per flow):** `receiver_in` (`CircularBuffer`)

**wg-obfs responsibility:**

- Receive bytes from the network.
- Write into the matching flow's `receiver_in`.

**Downstream in pipeline:** when `receiver_in` holds at least `ENCODE_BLOCK` bytes,
`Codec_decode` runs and writes `DECODE_BLOCK` bytes to `receiver_out`.

**Demo substitute:** same `BufferTransfer_pump` as Boundary 2 (sender → receiver in
one process).

---

## Boundary 4 — Post-decode downstream

**Owner:** wg-obfs / downstream consumer

**Source buffer (per flow):** `receiver_out` (`CircularBuffer`)

**wg-obfs responsibility:**

- Read decoded raw bytes from `receiver_out`.
- Forward to player, file, next processing stage, etc.

**Demo substitute:** `FileDrain_pull_once(&drain, receiver_out, …)` writes to an
output file. Verify with `cmp input output` when using `--no-pace`.

---

## Per-flow CircularBuffer chain (Boundaries 1→2→3→4)

After multi, each `flow_id` owns:

| Buffer | Role |
|--------|------|
| `post_multi_in` | raw bytes from FM pipe → encode input |
| `sending_out` | encode output → **transmit reads here** (Boundary 2) |
| `receiver_in` | **receive writes here** (Boundary 3) → decode input |
| `receiver_out` | decode output → **downstream reads here** (Boundary 4) |

Block sizes (`apps/wg_multi_pipeline/stream_config.h`):

| Symbol | Size |
|--------|------|
| `PKG_SIZE` | 188 B |
| `DECODE_BLOCK` | 752 B (4 × 188) |
| `ENCODE_BLOCK` | 1504 B (8 × 188) |

Tail bytes smaller than one block may bypass encode/decode via passthrough
(`passthrough_tail` in `pipeline.c`).

---

## Wiring checklist for wg-obfs

1. **Init multi:** `flow_peer_map_init`, `flow_manager_init`, set per-flow
   `output_fd`, `flow_manager_start`.
2. **UDP loop:** `recvfrom` → `flow_tuple_set` → `ingress_push_tuple`.
3. **FM → encode:** read each flow's pipe → write `post_multi_in`.
4. **Encode → network:** read `sending_out` → send (Boundary 2).
5. **Network → decode:** receive → write `receiver_in` (Boundary 3).
6. **Decode → downstream:** read `receiver_out` → consumer (Boundary 4).
7. **Shutdown:** `flow_manager_stop`, `flow_manager_destroy`, `flow_peer_map_destroy`.

**Do not** use legacy `multi_flow_relay` order (encode before FlowManager). Use
`wg_multi_pipeline` order (multi before encode).

---

## API quick reference

| Header | Key symbols |
|--------|-------------|
| `ingress_push.h` | `ingress_push`, `ingress_push_tuple` |
| `flow_peer_map.h` | `FlowTuple`, `flow_tuple_set`, `flow_peer_map_lookup` |
| `flow_manager.h` | `FlowManager`, `flow_manager_init/start/push` |
| `circular_buffer.h` | `Buffer_Init`, `Buffer_Read`, `Buffer_Write` (buffer repo) |

**Return codes (`ingress_push_tuple`):**

| Value | Meaning |
|-------|---------|
| `INGRESS_PUSH_OK` | success |
| `INGRESS_PUSH_ERR_INVALID` | null mgr/map/tuple or bad args |
| `INGRESS_PUSH_ERR_ALLOC` | packet alloc failed |
| `INGRESS_PUSH_ERR_MGR` | `flow_manager_push` failed (e.g. queue full) |
| `INGRESS_PUSH_ERR_PEER` | 5-tuple lookup failed (e.g. `max_flows` exhausted) |

---

## Verification

**multi library:**

```bash
make test    # includes test_flow_peer_map (5-tuple → flow_id)
```

**End-to-end file harness (3 flows, encode + decode):**

```bash
make integration-test
```

**Manual multi-file run:**

```bash
make wg-demo
./build/wg_multi_pipeline --no-pace --multi in0 out0 in1 out1 in2 out2
cmp in0 out0 && cmp in1 out1 && cmp in2 out2
```

UDP end-to-end is not in the demo binary yet; Boundary 1 is validated via unit tests
and file/mock ingress in `wg_multi_pipeline`.

---

## One-line summary for collaborators

> Call `ingress_push_tuple` after UDP `recvfrom` (Boundary 1). After multi, each flow
> has its own CircularBuffer chain: read `sending_out` to transmit (2), write
> `receiver_in` on receive (3), read `receiver_out` for downstream (4). Multi handles
> 5-tuple split and pacing only; encode, network, and decode stay in wg-obfs.
