#ifndef CODEC_H
#define CODEC_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

typedef struct Codec Codec;

typedef enum CodecKind {
    CODEC_KIND_NONE = 0,
    CODEC_KIND_BLOCK,
    CODEC_KIND_COPY,
    CODEC_KIND_XOR_FEC,
    CODEC_KIND_RS_FEC,
    CODEC_KIND_RS
} CodecKind;

typedef enum CodecRecoverStatus {
    CODEC_RECOVER_OK = 0,
    CODEC_RECOVER_UNAVAILABLE,
    CODEC_RECOVER_ERR
} CodecRecoverStatus;

/*
 * present_bits is a bitset: shard i is present when
 *   (present_bits[i / 8] & (1u << (i % 8))) != 0
 * Length must be at least codec_present_bytes(shard_count).
 * No fixed 16/32-bit integer width limit.
 */
typedef struct CodecVTable {
    void (*encode)(const Codec *self, unsigned char *data, size_t len);
    void (*decode)(const Codec *self, unsigned char *data, size_t len);
    size_t (*input_block_size)(const Codec *self);
    size_t (*output_block_size)(const Codec *self);
    size_t (*data_shards)(const Codec *self);
    size_t (*parity_shards)(const Codec *self);
    int (*is_systematic)(const Codec *self);
    CodecRecoverStatus (*recover)(const Codec *self,
                                  unsigned char *shards,
                                  const uint8_t *present_bits,
                                  size_t shard_count);
} CodecVTable;

struct Codec {
    const CodecVTable *vtable;
    const void        *impl;
};

static inline size_t codec_present_bytes(size_t shard_count)
{
    return (shard_count + 7u) / 8u;
}

static inline int codec_present_get(const uint8_t *bits, size_t index)
{
    return (bits[index / 8u] >> (index % 8u)) & 1;
}

static inline void codec_present_set(uint8_t *bits, size_t index)
{
    bits[index / 8u] |= (uint8_t)(1u << (index % 8u));
}

static inline void codec_present_clear_all(uint8_t *bits, size_t shard_count)
{
    if (bits != NULL && shard_count > 0) {
        memset(bits, 0, codec_present_bytes(shard_count));
    }
}

static inline void codec_present_set_all(uint8_t *bits, size_t shard_count)
{
    size_t nbytes;
    size_t rem;

    if (bits == NULL || shard_count == 0) {
        return;
    }
    nbytes = codec_present_bytes(shard_count);
    memset(bits, 0xff, nbytes);
    rem = shard_count % 8u;
    if (rem != 0u) {
        bits[nbytes - 1u] = (uint8_t)((1u << rem) - 1u);
    }
}

/* Convenience for tests: pack a small integer mask into a bitset. */
static inline void codec_present_from_u64(uint8_t *bits, size_t shard_count,
                                         uint64_t mask)
{
    size_t i;

    codec_present_clear_all(bits, shard_count);
    for (i = 0; i < shard_count && i < 64u; i++) {
        if ((mask & (1ull << i)) != 0) {
            codec_present_set(bits, i);
        }
    }
}

void Codec_encode(const Codec *codec, unsigned char *data, size_t len);
void Codec_decode(const Codec *codec, unsigned char *data, size_t len);
size_t Codec_input_block_size(const Codec *codec);
size_t Codec_output_block_size(const Codec *codec);
size_t Codec_data_shards(const Codec *codec);
size_t Codec_parity_shards(const Codec *codec);
int Codec_is_systematic(const Codec *codec);
CodecRecoverStatus Codec_recover(const Codec *codec,
                                 unsigned char *shards,
                                 const uint8_t *present_bits,
                                 size_t shard_count);

const Codec *BlockCodec_get(void);
const Codec *CopyCodec_get(void);
const Codec *XorFecCodec_get(void);
const Codec *RsFecCodec_get(void);
const Codec *RsCodec_get(void);
const Codec *Codec_get(CodecKind kind);

#endif /* CODEC_H */
