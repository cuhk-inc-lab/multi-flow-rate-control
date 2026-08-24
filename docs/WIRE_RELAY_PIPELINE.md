# Wire Relay Pipeline Design

Application-layer **per-node** program for wire headers **v3 and v4**
(`final_dst` + `ttl`). Binary: `apps/wire_relay/` → `build/wire_relay`.
Each node runs this one binary: local encode, destination check, optional
decode, and opaque forward. Mid-hop recode / decode-reencode are reserved.

Wire v3 has **no independent coding vector** — only
`type` / `shard_index` / `shard_count` / `valid_len` (plus routing fields).
Generation key is `(flow_id, block_id)`. Do not invent a separate
`generation_id` field on the wire.

Wirehair alone uses wire v4. It treats `block_id` as a segment id,
`shard_index` as a Wirehair packet id, and adds `origin_node`, flags, and
`segment_bytes`. Defaults are 10 MiB per segment and a repair ceiling of 10%.
`--wh-ack` requests an ACK as soon as recovery completes; otherwise the sender
always transmits the full repair budget.

ACK datagrams carry `(flow_id, segment_id)`, set `WIRE_FLAG_RETURN_PATH`, and
are forwarded through each relay's `--return-hop HOST:PORT`. A linear path
must configure each return hop toward the previous relay; the relay nearest
the sender points to the sender's `--ack-port`. Wirehair v4 bypasses the
v3-only `GenerationCache`. A receiver keeps at most four active segment
decoders, so the default peak decode payload storage is about 40 MiB per flow.

Embedding the same Wirehair+ACK path without sockets:
[`docs/FEC_TRANSPORT.md`](FEC_TRANSPORT.md).

---

## Unified per-node pipeline

Target structure (encode / decode / forward implemented; mid-hop transform reserved):

```text
UDP in
  → ttl==0? drop
  → final_dst == local_node_id?
        yes → delivery_fn (LocalDecodeHub → file); no TTL-- / egress
        no  → TTL--
            → [optional] recode_fn            # ingress; NULL = opaque
            → [optional] decode_reencode_fn   # Phase 3A reserved; stub=OPAQUE
            → [--process cache] observe
            → EgressQueue → [optional] egress_fn → sendto(next-hop)

Local file / FIFO (--source)
  → encode → fill final_dst/ttl → relay_inject_wire_datagram
  → same Destination Check / transit / egress as UDP in
```

Locality is **only** `final_dst == local_node_id` (never UDP/IP dst).

---

## Phase roadmap

| Phase | Scope | Status |
|-------|--------|--------|
| **1** | Opaque multi-flow forward + global EgressQueue + wire-level inject | **Implemented** |
| **2** | Copy-based GenerationCache + process hook; still opaque forward | **Implemented** |
| **L0** | Extract reusable `WireFlowDecoder` from udp-recv | **Implemented** |
| **L1** | Explicit-hop relay local-destination decode (`--output FILE`) | **Implemented** |
| **L2** | Multi-flow local decode (`--output-dir DIR`) | **Implemented** |
| **S0** | Local file/FIFO source encode → inject (`--source`) | **Implemented** |
| **3A** | Decode-and-reencode relay (recover source, re-run current FEC encoder) | **Interface reserved** (`decode_reencode_fn`; stub OPAQUE) |
| **3B** | True network recode | Future — requires new wire version |

### Local destination decode (L1 / L2)

When `--local-decode` is set on `wire_relay`:

```text
UDP RX / inject
  → ingress_mu
  → if wire_header_is_local(header, local_node_id):
       LocalDecodeHub
         L1 --output FILE:      one WireFlowDecoder → FILE
         L2 --output-dir DIR:   per-flow WireFlowDecoder → DIR/flow_<id>.bin
       (no TTL--, no GenerationCache, no EgressQueue, no sendto)
  → else: existing Phase 1/2 forward path unchanged
```

- Locality is **only** `final_dst == local_node_id` (never UDP/IP dst).
- Exactly one sink: `--output` **or** `--output-dir` (not both); `--codec` required.
- **P0A:** `delivery_fn` / LocalDecodeHub decode+I/O run **outside**
  `RelayCtx.ingress_mu`. Hub serializes ingest via its own `hub->mu`.
  This is not an async decode worker: the inject caller still waits for
  the callback to return. UDP RX local delivery runs on the processing worker.
- **L1** (`--output FILE`): single `flow_id`; a second id is rejected
  (`local_decode_flow_rejected`).
- **L2** (`--output-dir DIR`): up to `RELAY_MAX_FLOWS` concurrent local flows;
  each binds a slot until hub destroy (no slot reuse after END); overflow
  rejects with `local_decode_flow_rejected` (capacity). `DIR` must already exist.
- All flows share one process-wide codec / FEC geometry; mismatched
  `shard_count` (must equal fixed `expected_shards`) → metadata mismatch +
  drop. RS geometry is process startup config (`--rs-k` / `--rs-parity`);
  wire `shard_count` does not retune the global RS profile.
- Local decode uses `WireFlowDecoder`: systematic codecs may finish a block
  when all original data shards are present even if pad/parity shards are
  lost; missing any data shard still requires FEC recover or else strict
  incomplete.
- With `--local-decode`, `reject_local_encoder_loopback` is cleared so
  `LOCAL_ENCODER` packets with `final_dst==local` can sink-decode.
- Without `--local-decode`, delivery stays count-only (default).
- Strict exit via `local_decode_hub_strict_check`: hub errors/rejections, or any
  active incomplete / per-flow ingest_error → process `EXIT_FAILURE`.

Local decode is **not** Phase 3A mid-hop recode and does **not** use
GenerationCache for decoder state.

### Phase 1

```text
UDP RX
  → minimal wire_header_decode (flow_id)
  → per-flow RelayDeferredHub (packet-level; drop-new on overflow)
  → processing worker (single; RR + quota=8)
      → ingress_mu: dest / TTL / optional cache / ForwardPending
      → outside lock: egress try/timed enqueue
  → TX worker → sendto

inject (harness): synchronous; does not enter deferred
```

### Phase 2

```text
UDP RX → deferred → processing worker
  → ingress_mu
  → dest check / TTL-- (encode into datagram)
  → DATA + --process cache: copy into GenerationCache; process hook
  → END/control: expire stale gens for flow; never cache; enqueue
  → build ForwardPending under ingress_mu; enqueue outside ingress_mu
  → global FIFO EgressQueue → TX
```

Defaults:

- `--process forward` — no cache (Phase 1 behavior).
- `--process cache` — observe/store only; **still opaque-forward** every packet.
- `--egress-wait-ms 0` — try-drop baseline; `>0` timed wait on **processing worker only**.
- `--deferred-per-flow 128`, `--deferred-total 1024`, `--max-active-flows 64`.
- Limits: `gen_timeout_ms=500`, `max_gens=256`, `max_gens_per_flow=32`,
  `max_cache_bytes=32MiB`.

### Egress backpressure (timed wait)

Global **FIFO** EgressQueue between **processing worker** and TX worker.
UDP RX never waits on this queue.
| `--egress-wait-ms` | Behavior on full queue |
|--------------------|-------------------------|
| `0` | Non-blocking `try_enqueue`; drop new packet → `drop_egress_full` |
| `>0` | Wait up to N ms for space via `pthread_cond_timedwait`; timeout drops new packet → `drop_egress_timeout` |

- Never drop-oldest; never overwrite queued packets.
- Processing builds `ForwardPending` under `ingress_mu`, then enqueues **outside**
  `ingress_mu` so timed wait does not block cache work on the mutex.
  UDP RX only enqueues into `RelayDeferredHub` and never waits on egress.
- Queue-global metrics: `egress_enqueue_immediate`, `egress_enqueue_waited`,
  `egress_wait_ns_total/max`, `egress_high_watermark`.
- Per-flow drop counters: `drop_egress_full` (try path), `drop_egress_timeout` (timed path).
- Deferred hub drops (new packet only): `drop_deferred_overflow_flow`,
  `drop_deferred_overflow_total`, `drop_deferred_table_full`.
- If timed wait / deferred overflow drops DATA but END later enqueues, downstream
  may still report incomplete / missing groups — a fundamental UDP + bounded-queue
  limitation, not masked as success.

Cache copy and EgressPacket ownership are **independent**. On admission failure,
default policy is **still forward** the current packet without caching
(`gen_admission_failed++`).

### Local source (S0)

`--source FILE --final-dst N --ttl N --codec ... [--flow-id] [--rate-mbps]`:

- Encodes on a background thread inside `relay_run`.
- Emits wire v3 DATA + END via `relay_inject_wire_datagram`.
- Shares Destination Check / TTL / transit hooks / EgressQueue with UDP RX.
- `FILE` may be a regular file or named FIFO.
- If `final_dst == local_node_id`, `--local-decode` is required.

### Phase 3A vs 3B

- **3A — decode-and-reencode:** After collecting enough recoverable shards,
  decode source, re-run the existing FEC encoder. Possible on wire v3.
  Hook: `RelayDecodeReencodeFn` / `--decode-reencode-stub` (always OPAQUE today).
- **3B — true network recode:** Needs a future wire version with coding vectors.
  Do not overload `recode_fn` / `decode_reencode_fn` for 3B.
- Per-datagram optional transforms today: `--transit-hook identity` copies at
  ingress; `--transit-hook plus-minus` applies DATA payload `+1` at ingress and
  `-1` after egress dequeue. Headers and END datagrams are unchanged.

---

## Design boundaries (confirmed)

### 1. Wire-level local injection

`relay_inject_wire_datagram()` accepts an **already encoded** wire v3
datagram only. Raw local data → encoder is `local_source_run` / `--source`.

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

`ingress_mu` serializes dest check, TTL rewrite, GenerationCache, and process
hook. EgressQueue enqueue runs **outside** `ingress_mu` via `ForwardPending`
(timed or try). TX worker never touches the cache.

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
- Per-flow quota (`max_gens_per_flow`) uses the full `uint32_t` wire `flow_id`
  (including 0). High ids are not clamped into a shared stats bucket for
  admission/eviction. `per_flow_created[8]` remains clipped telemetry only.
- Local-destination packets (`final_dst == local_node_id`) never enter the
  GenerationCache.

### Test harness lifecycle

`relay_harness_open` / `relay_harness_close` are for in-process unit tests
(no listen socket; optional TX capture).

**Contract:** call `relay_harness_close(ctx)` only after the caller has stopped
and **joined** every thread that might call `relay_inject_wire_datagram(ctx, ...)`.

- Close waits for an already-admitted inject until its entire submit path,
  including a deferred `RelayDeliveryFn` invocation outside `ingress_mu`, has
  returned (`inject_in_flight` stays elevated across that callback).
- Close does **not** support arbitrary concurrent inject-vs-close for threads
  that have not yet acquired `ingress_mu` / incremented `inject_in_flight`;
  those threads must be stopped and joined first.
- Production `relay_run` owns its stack `RelayCtx`; RX exits before cleanup, so
  there is no external inject-after-shutdown pointer problem.
