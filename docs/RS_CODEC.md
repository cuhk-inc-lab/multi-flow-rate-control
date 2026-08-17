# Reed-Solomon codec (`--codec rs`)

This document describes the **hqm/rscode-backed** column-wise RS codec in
`apps/wg_multi_pipeline/`: software architecture, API syntax, and parameter
meanings. It is separate from `--codec rs-fec` (liberasurecode), which is
**not wire-compatible** with `--codec rs`.

## 1. What “RS(n, k)” means here

| Symbol | Meaning |
| --- | --- |
| \(k\) | Data shards (= message symbols per column codeword) |
| \(r\) | Parity shards |
| \(n = k + r\) | Total shards on the wire for one block |

Each **symbol is one byte**. Layout is **column-wise**: for every offset
\(i \in [0, PKG\_SIZE)\), one codeword is
\([d_0[i], \ldots, d_{k-1}[i], p_0[i], \ldots, p_{r-1}[i]]\).

Losing one UDP datagram = one erasure in every codeword.

**Default:** \(k=4\), \(r=2\) → **RS(6, 4)**.

You can set **any** supported pair yourself (see bounds below), e.g. RS(18, 16)
with `--rs-k=16 --rs-parity=2`.

## 2. Software architecture

```text
  source (k × PKG_SIZE)
       → Codec_encode (column-wise RS(n,k))
       → n UDP shards (WireHeader.shard_count = n)
       → receiver present_bits (bitset)
       → Codec_recover → Codec_decode → k × data bytes
```

| File | Role |
| --- | --- |
| `rs_codec.c` / `rs_codec.h` | Params, encode, recover |
| `codec.h` | `CodecVTable`, `present_bits` bit array helpers |
| `third_party/rscode/` | Startup: extract default 4+2 parity coeffs; optional `RS_ENCODE_RSCODE` reference encode |
| `wire_udp.c` | Wire UDP send/recv; receiver demux by `flow_id`; validates `shard_count` |

## 3. API syntax

### Set any (k, r)

```c
/* Primary API — set once before encode/decode for this process */
int RsCodec_set_params(size_t data_shards, size_t parity_shards);
void RsCodec_get_params(size_t *data_shards, size_t *parity_shards);

/* Optional shortcuts for 4+1 / 4+2 / 4+3 */
int RsCodec_set_profile(RsProfile profile);
```

### Receiver / wire decode geometry (fixed per process)

Sender and receiver must use the **same** fixed `(k, r)` (CLI `--rs-k` /
`--rs-parity` / `--rs-profile`, or defaults). At WireFlowDecoder create time:

```text
expected_shards = Codec_output_block_size(codec) / PKG_SIZE   /* == k + r */
```

Every DATA/END `WireHeader.shard_count` must equal `expected_shards`. Mismatch
packets are dropped as malformed / metadata mismatch. The wire decoder **must
not** call `RsCodec_set_profile_from_shard_count` — header `shard_count` is
validation only, not a remote profile command.

`RsCodec_set_params_from_shard_count` / `RsCodec_set_profile_from_shard_count`
remain available for explicit init/bench helpers; they are not used on the
wire ingest/recover path.

One process supports one RS geometry for all concurrent wire flows. Different
`(k, r)` requires separate receiver/relay processes (or a future wire-protocol
codec-profile id).

### Recover

```c
/* present_bits: bitset, length >= codec_present_bytes(shard_count) */
Codec_recover(codec, shards, present_bits, shard_count);
```

Recover uses the process-fixed matrix for the configured `(k, r)`.

### Encode plan / hot path

Startup (`RsCodec_set_params` / default init) publishes an immutable
`RsEncodePlan` (generator coeffs + optional `r×k×256` mul table, capped at
`RS_ENCODE_MUL_TABLE_MAX_BYTES`). For default `4+2`, parity coeffs are
extracted once from hqm/rscode `encode_data` on unit vectors so the table
path stays bit-exact with historical wire parity. Encode workers must start
only after configuration.

| Geometry | Default (`AUTO`) path |
| --- | --- |
| `4+2` | optimized general table (unlocked; bit-exact vs rscode) |
| `16+2` | specialized dual-parity table scan |
| other `(k,r)` | optimized general table |

Reference / verification only (not AUTO):

- `RS_ENCODE_LEGACY` — byte-wise `gmult` against plan coeffs
- `RS_ENCODE_RSCODE` — hqm `encode_data` for `4+2` only (holds `rs_lock`)

`RsCodec_set_encode_impl()` can force `GENERAL` / `FAST_16_2` / `LEGACY` /
`RSCODE` for tests and `rs_encode_bench`.

### CLI

```bash
# Default RS(6,4)
./build/wg_multi_pipeline --codec rs --udp-send HOST PORT in.bin

# Your own numbers (example: RS(18,16))
./build/wg_multi_pipeline --codec rs --rs-k=16 --rs-parity=2 \
  --udp-send HOST PORT in.bin

# Same thing via profile string
./build/wg_multi_pipeline --codec rs --rs-profile=16+2 \
  --udp-send HOST PORT in.bin

# Receiver: same fixed (k, parity) as sender
./build/wg_multi_pipeline --codec rs --rs-k=16 --rs-parity=2 \
  --udp-recv PORT out.bin
```

| Flag | Meaning |
| --- | --- |
| `--rs-k=N` | Data shards \(k\) |
| `--rs-parity=M` | Parity shards \(r\) |
| `--rs-profile=K+R` | Shorthand for k+r (also accepts `4+1` / `4+2` / `4+3`) |

## 4. Bounds (follow your settings)

**Working size = whatever you set.** If you choose `--rs-k=16 --rs-parity=2`,
then for that run:

- input block = 16 × 1400
- wire shards n = 18
- can repair up to 2 erasures

There is **no separate quota** like “max k=20, max r=12”.

| Hard ceiling | Why |
| --- | --- |
| \(n = k + r \le 255\) | GF(256) byte-symbol Reed–Solomon |
| Buffer | `CODEC_MAX_ENCODE_BLOCK = 255 × PKG_SIZE` |

Examples that work: `4+2`, `16+2`, `8+4`, `40+2`, `200+10` (as long as n≤255).
Rejected: `254+2` (n=256 > 255).

Presence is tracked with a **bit array**, not a fixed-width integer mask — there
is no 16/32-bit “shard count” ceiling from the mask type.

## 5. Parameter meaning

| Parameter | Meaning |
| --- | --- |
| `PKG_SIZE` (1400) | Bytes per shard / UDP payload |
| \(k\) | Data shards; input block = \(k \times\) `PKG_SIZE` |
| \(r\) | Parity shards; can repair up to \(r\) erasures |
| \(n=k+r\) | `WireHeader.shard_count` |
| Column | One RS codeword per byte offset |

### Complexity (matrix recover, per block)

- Encode: \(O(L \cdot k \cdot r)\), \(L=\)`PKG_SIZE`
- Recover (≤ \(r\) erasures): \(O(k^3 + e \cdot L \cdot k)\) on-demand invert

## 6. Runtime adaptation

```text
observe loss → choose new (k, r) or keep k and change r
  → RsCodec_set_params(k, r) on sender
  → new groups advertise n via shard_count
  → receiver keeps same --rs-k, updates r from shard_count
```

**Note:** changing **k** mid-flow requires both ends to switch together; the
wire header currently carries only `shard_count` (n), not k. Prefer fixing k
and adapting r for on-path control loops.

## 7. Related docs

- [`apps/wg_multi_pipeline/README.md`](../apps/wg_multi_pipeline/README.md)
- [`apps/wire_relay/README.md`](../apps/wire_relay/README.md) — same `--codec rs`
  on `--source` / `--local-decode`
- [`WIRE_RELAY_PIPELINE.md`](WIRE_RELAY_PIPELINE.md)
- [`VM_NETWORK_BENCHMARK.md`](VM_NETWORK_BENCHMARK.md)
- [`SCRIPTS.md`](SCRIPTS.md)
