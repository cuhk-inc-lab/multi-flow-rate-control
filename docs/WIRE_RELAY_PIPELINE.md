# Wire Relay Pipeline Design

Application-layer explicit-hop relay for wire header **v3**
(`final_dst` + `ttl`). Binary: `apps/wire_relay/` → `build/wire_relay`.

Wire v3 has **no independent coding vector** — only
`type` / `shard_index` / `shard_count` / `valid_len` (plus routing fields).
Generation key is `(flow_id, block_id)`. Do not invent a separate
`generation_id` field on the wire.

---

## Phase roadmap

| Phase | Scope | Status |
|-------|--------|--------|
| **1** | Opaque multi-flow forward + global EgressQueue + wire-level inject | **Implemented** |
| **2** | GenerationCache for DATA shards; END/control ordering | Future |
| **3A** | Decode-and-reencode relay (recover source, re-run current FEC encoder) | Future |
| **3B** | True network recode | Future — requires new wire version |

### Phase 1 (current)

```text
UDP RX  ─┐
         ├─► ingress_mu ─► dest check / TTL rewrite / process ─► EgressQueue
inject  ─┘                                                      │
                                                                ▼
                                                         TX worker sendto
```

- No GenerationCache.
- No real recode (`recode_fn` default `NULL` = FORWARD_ORIGINAL).
- No changes to wire header, sender, receiver, or codec.

### Phase 3A vs 3B (explicit split)

- **3A — decode-and-reencode:** After collecting enough recoverable shards for a
  block, decode source, then call the **existing** FEC encoder to emit shards
  recognizable by the current protocol. Possible **without** changing wire v3.
- **3B — true network recode:** Arbitrary linear combinations at the relay.
  **Not** claimable under wire v3. Requires a future wire version with coding
  vector, coefficient seed, or equivalent metadata.

---

## Design boundaries (confirmed)

### 1. Wire-level local injection

`relay_inject_wire_datagram()` accepts an **already encoded** wire v3
datagram only. It is **not** raw local data → encoder. Wiring raw bytes into
the existing encoder remains separate integration work.

### 2. No arbitrary network recode on wire v3

See Phase 3A / 3B above. Phase 3 must not claim true network coding while the
header stays v3.

### 3. TTL consistency: bytes are source of truth

After TTL decrement, call `wire_header_encode()` so the on-wire datagram bytes
match. TX worker sends **only** `EgressPacket.datagram[0..len)`; it must not
rely on a separate in-memory header copy.

### 4. FORWARD_ORIGINAL: move, do not re-copy

Ingress already owns `datagram`. Move ownership into `EgressPacket` /
EgressQueue. TX frees after `sendto`. Extra copy-based paths are reserved for
future GenerationCache only.

### 5. Ingress thread safety

UDP RX and local inject may both call ingress submit. Phase 1 serializes
destination check, TTL rewrite, process, and enqueue under **`ingress_mu`**.
Phase 2 GenerationCache must not be lock-free under multiple producers.

### 6. END / control vs GenerationCache (Phase 2+)

- DATA shards only enter cache keyed by `(flow_id, block_id)`.
- END must wait until that flow’s prior cache is flushed, forwarded, or
  explicitly dropped before entering EgressQueue.
- Receiver must never see END before later recoded DATA of the same flow.

### 7. GenerationCache consistency (Phase 2+)

For one cache entry:

- All packets must agree on `final_dst`, `shard_count`, and FEC/type semantics.
- Drop duplicate `shard_index`.
- If TTLs differ, future output TTL uses the **minimum already-decremented** TTL.
- Never mix different `final_dst` into the same cache.

### 8. Monotonic timestamps

Use `uint64_t` nanoseconds (`CLOCK_MONOTONIC`), not `double`.

---

## Phase 1 data path notes

- RX: one copy from socket buffer into owned heap datagram; then move to egress.
- Inject: one copy from caller buffer into owned heap; then same ingress path.
- Optional `RelayRecodeFn` (if ever set) may allocate a new buffer and free the
  original; default path does not.
- CLI unchanged aside from optional `--egress-capacity`.
