# Wire Flow Decoder: Head-of-Line Notes

Status: **Phase A / B / C implemented.** RS codec, wire header, and relay paths
are unchanged. Phase D (per-group TIMEOUT before END) is **not** implemented.

## Implemented behaviour

Receive, recover, and emit are separate (`wire_flow_decoder.c`).

### Late vs window vs overflow

```
block_id < next_emit_block          → late_datagrams (already emitted / skipped)
next_emit_block ≤ block_id < +W     → accept into a group slot (W = 128)
block_id ≥ next_emit_block + W      → window_overflow (not dropped_groups)
no free slot in window              → window_overflow
```

Lookup is by `block_id` over 128 slots. Occupancy is `state != EMPTY`.
ACTIVE / RECOVERED groups are not evicted to make room.

### Group state

```
EMPTY → ACTIVE → RECOVERED → EMITTED
              ↘ FAILED (only at END finalize, or equivalent)
```

Each group recovers independently on a new non-duplicate shard.
`CODEC_RECOVER_UNAVAILABLE` / ingest-time `CODEC_RECOVER_ERR` stay ACTIVE.
FAILED is set at END when the group still cannot recover.

### Emit

- **Strict (default):** only emit when head is RECOVERED. Missing or FAILED
  head stalls. Hash PASS requires every group emitted in order
  (`next_emit_block == end_block_count`). Later recovered groups wait in RAM
  (`pending_recovered_groups`) and are not written.
- **Best-effort (`--best-effort`, after END):** skip missing or FAILED head
  (`skipped_groups++`; not `dropped_groups`). Systematic codecs may still write
  received data shards (`missing_data_shards`). Then emit later RECOVERED
  groups in order. Hash is expected to FAIL; use skip / byte / block gap, not
  wire loss.

`dropped_groups` remains a compat field. Best-effort holes use `skipped_groups`.

### Counters (FAIL diagnosis)

| Field | Meaning |
|-------|---------|
| `wire_shard_loss_pct` | `1 - seen_datagrams / sent_shards` (on-wire) |
| `block_completion_pct` | `decoded_blocks / expected_blocks` (fully emitted) |
| `groups_failed` | groups that could not recover at END |
| `pending_recovered_groups` | recovered, waiting on head (strict HOL) |
| `window_overflow` | shards outside `[next_emit, next_emit+W)` or no slot |
| `skipped_groups` | best-effort holes advanced past |
| `missing_groups` | `end_count - next_emit` while still incomplete |

Do not label block-completion failure as physical/wire loss.

---

## Why enlarging W alone is not enough

Raising `WIRE_FLOW_GROUP_WINDOW` only holds more in-flight groups. It does not
remove ordered-emit HOL, recover extra erasures, or turn WiFi burst loss into
i.i.d. loss. RS(k+r) still only guarantees ≤ r erasures **per block**.

---

## Not done (Phase D)

TIMEOUT: skip or fail a stuck ACTIVE head before END. File transfers send END,
so Phase C is enough for that path. Live streams without END still wait until
idle/END.

---

## Non-goals

- No change to RS encode/decode math (`rs_codec.c`, `Codec_recover`).
- No change to wire header layout or relay forwarding.
- Strict PASS remains sha256 match. Best-effort is not the default matrix mode.
