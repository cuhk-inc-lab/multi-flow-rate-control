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
| **2** | Copy-based GenerationCache + process hook; still opaque forward | **Implemented** |
| **3A** | Decode-and-reencode relay (recover source, re-run current FEC encoder) | Future |
| **3B** | True network recode | Future — requires new wire version |

### Phase 1

```text
UDP RX / inject → ingress_mu → dest check / TTL rewrite → EgressQueue → TX
```

### Phase 2

```text
UDP RX / inject
  → ingress_mu
  → dest check / TTL-- (encode into datagram)
  → DATA + --process cache: copy into GenerationCache; process hook
  → END/control: expire stale gens for flow; never cache; enqueue
  → move current datagram → EgressQueue → TX
```

Defaults:

- `--process forward` — no cache (Phase 1 behavior).
- `--process cache` — observe/store only; **still opaque-forward** every packet.
- Limits: `gen_timeout_ms=500`, `max_gens=256`, `max_gens_per_flow=32`,
  `max_cache_bytes=32MiB`.

Cache copy and EgressPacket ownership are **independent**. On admission failure,
default policy is **still forward** the current packet without caching
(`gen_admission_failed++`).

### Phase 3A vs 3B

- **3A — decode-and-reencode:** After collecting enough recoverable shards,
  decode source, re-run the existing FEC encoder. Possible on wire v3.
- **3B — true network recode:** Needs a future wire version with coding vectors.

---

## Design boundaries (confirmed)

### 1. Wire-level local injection

`relay_inject_wire_datagram()` accepts an **already encoded** wire v3
datagram only. It is **not** raw local data → encoder.

### 2. No arbitrary network recode on wire v3

See Phase 3A / 3B.

### 3. TTL consistency: bytes are source of truth

After TTL decrement, call `wire_header_encode()`. TX sends datagram bytes only.
Cache stores copies whose TTL is already decremented. Future recode outputs
should use the **minimum** cached TTL; Phase 2 only records `min_ttl`.

### 4. FORWARD_ORIGINAL: move for egress; copy for cache

Ingress moves the live datagram into EgressQueue. GenerationCache holds a
separate `malloc` copy per shard.

### 5. Ingress thread safety

`ingress_mu` serializes dest check, TTL rewrite, GenerationCache, process hook,
and EgressQueue enqueue. TX worker never touches the cache.

### 6. END / control

- Never enter GenerationCache.
- On END: expire timed-out gens for that flow, then enqueue END.
- With a single global FIFO EgressQueue, DATA enqueued before END is sent first.

### 7. GenerationCache consistency

Per `(flow_id, block_id)` entry:

- Agree on `final_dst`, `shard_count`, `type`, `valid_len`, `payload_len`.
- Duplicate `shard_index`: do not overwrite; `gen_duplicate++`; still forward.
- Metadata mismatch: refuse insert; `gen_metadata_mismatch++`; still forward.
- Complete generation (`present_count == shard_count`) → `GEN_READY`; keep until
  timeout/eviction; do **not** block opaque forward.

### 8. Monotonic timestamps

`uint64_t` nanoseconds (`CLOCK_MONOTONIC`), not `double`.

### Eviction vs timeout

- Timeout: idle beyond `gen_timeout_ms` → `gen_timeout++`.
- Eviction: LRU / oldest `last_update_ns` to satisfy per-flow, global count, or
  byte limits → `gen_evicted++`. Prefer same-flow victims when the per-flow
  limit is the binding constraint.
