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
| `third_party/rscode/` | Used for default 4+2 encode |
| `wire_udp.c` | Sharding; applies params from `shard_count` |

## 3. API syntax

### Set any (k, r)

```c
/* Primary API — next encode block uses these params */
int RsCodec_set_params(size_t data_shards, size_t parity_shards);
void RsCodec_get_params(size_t *data_shards, size_t *parity_shards);

/* Optional shortcuts for 4+1 / 4+2 / 4+3 */
int RsCodec_set_profile(RsProfile profile);
```

### Receiver (k fixed, r from wire)

Both ends must agree on **k** (`--rs-k`). Parity may change per group:

```c
/* Before Codec_recover for a group: */
RsCodec_set_params_from_shard_count(header.shard_count);
/* sets r = shard_count - k */
```

### Recover

```c
/* present_bits: bitset, length >= codec_present_bytes(shard_count) */
Codec_recover(codec, shards, present_bits, shard_count);
```

Recover always uses the **matrix** erasure path (supports all configured (k, r)).

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

# Receiver: same k (parity follows shard_count)
./build/wg_multi_pipeline --codec rs --rs-k=16 --udp-recv PORT out.bin
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
- [`VM_NETWORK_BENCHMARK.md`](VM_NETWORK_BENCHMARK.md)
- [`SCRIPTS.md`](SCRIPTS.md)
