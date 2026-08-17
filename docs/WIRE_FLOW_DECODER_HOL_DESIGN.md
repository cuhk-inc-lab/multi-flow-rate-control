# Wire Flow Decoder: Head-of-Line Notes

Status: **Phase A / B / C implemented.** Best-effort also slides the reorder
window mid-stream (`make_room`) when a new block cannot enter
`[next_emit, next_emit+W)`. RS codec, wire header, and relay paths are
unchanged. Phase D (per-group idle TIMEOUT before END) is **not** implemented.

## Implemented behaviour

Receive, recover, and emit are separate (`wire_flow_decoder.c`).

### Late vs window vs overflow

```
block_id < next_emit_block          → late_datagrams (already emitted / skipped)
next_emit_block ≤ block_id < +W     → accept into a group slot (W = 128)
block_id ≥ next_emit_block + W      → strict: window_overflow
                                      best-effort: skip stuck heads until the
                                      block fits; else window_overflow
no free slot in window              → same as beyond-W (strict overflow /
                                      best-effort make_room)
```

Lookup is by `block_id` over 128 slots. Occupancy is `state != EMPTY`.
Strict mode does not evict ACTIVE / RECOVERED groups. Best-effort `make_room`
never skips a RECOVERED head (it emits it); it last-chance recovers an ACTIVE
head, then skips if still not RECOVERED. At most W skips per packet.

### Group state

```
EMPTY → ACTIVE → RECOVERED → EMITTED
              ↘ FAILED (END finalize, or best-effort make_room last-chance)
```

Each group recovers independently on a new non-duplicate shard.
`CODEC_RECOVER_UNAVAILABLE` / ingest-time `CODEC_RECOVER_ERR` stay ACTIVE.
FAILED is set at END, or mid-stream in best-effort when `make_room` gives an
ACTIVE head a last-chance recover and it still cannot recover.

### Emit

- **Strict (`--strict`):** only emit when head is RECOVERED. Missing or FAILED
  head stalls. Hash PASS requires every group emitted in order
  (`next_emit_block == end_block_count`). Later recovered groups wait in RAM
  (`pending_recovered_groups`) and are not written.
- **Best-effort (CLI default, `--best-effort`):** skip missing or FAILED head after END
  (`skipped_groups++`; not `dropped_groups`). Mid-stream, skip stuck heads
  only when a new DATA block cannot enter the reorder window (`make_room`).
  In-window holes still wait until END. Systematic codecs may still write
  received data shards (`missing_data_shards`). Then emit later RECOVERED
  groups in order. Hash is expected to FAIL; use skip / byte / block gap, not
  wire loss.

`dropped_groups` remains a compat field. Best-effort holes use `skipped_groups`.

### Counters (FAIL diagnosis)

| Field | Meaning |
|-------|---------|
| `wire_shard_loss_pct` | `1 - seen_datagrams / sent_shards` (on-wire) |
| `block_completion_pct` | `decoded_blocks / expected_blocks` (fully emitted) |
| `groups_failed` | groups that could not recover at END or make_room last-chance |
| `pending_recovered_groups` | recovered, waiting on head (strict HOL) |
| `window_overflow` | shards still outside `[next_emit, next_emit+W)` or no slot after any make_room |
| `skipped_groups` | best-effort holes advanced past (END and mid-stream make_room) |
| `missing_groups` | `end_count - next_emit` while still incomplete |

Do not label block-completion failure as physical/wire loss.

---

## Why enlarging W alone is not enough

Raising `WIRE_FLOW_GROUP_WINDOW` only holds more in-flight groups. It does not
remove ordered-emit HOL, recover extra erasures, or turn WiFi burst loss into
i.i.d. loss. RS(k+r) still only guarantees ≤ r erasures **per block**.

---

## Not done (Phase D)

Idle-head TIMEOUT (`gen_timeout_ms`-style) is not implemented. Best-effort
file transfers that overflow the window now slide via `make_room` without
waiting for END. Live / low-rate streams whose window is not full still wait
until idle/END for in-window holes.

---

## Non-goals

- No change to RS encode/decode math (`rs_codec.c`, `Codec_recover`).
- No change to wire header layout or relay forwarding.
- Strict PASS remains sha256 match. Best-effort is not the default matrix mode.
