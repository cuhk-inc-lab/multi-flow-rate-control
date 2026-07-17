#ifndef CODEC_H
#define CODEC_H

#include <stddef.h>
#include <stdint.h>

typedef struct Codec Codec;

typedef enum CodecKind {
    CODEC_KIND_NONE = 0,
    CODEC_KIND_BLOCK,
    CODEC_KIND_COPY,
    CODEC_KIND_XOR_FEC,
    CODEC_KIND_RS_FEC
} CodecKind;

typedef enum CodecRecoverStatus {
    CODEC_RECOVER_OK = 0,
    CODEC_RECOVER_UNAVAILABLE,
    CODEC_RECOVER_ERR
} CodecRecoverStatus;

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
                                  uint16_t present_mask);
} CodecVTable;

struct Codec {
    const CodecVTable *vtable;
    const void        *impl;
};

void Codec_encode(const Codec *codec, unsigned char *data, size_t len);
void Codec_decode(const Codec *codec, unsigned char *data, size_t len);
size_t Codec_input_block_size(const Codec *codec);
size_t Codec_output_block_size(const Codec *codec);
size_t Codec_data_shards(const Codec *codec);
size_t Codec_parity_shards(const Codec *codec);
int Codec_is_systematic(const Codec *codec);
CodecRecoverStatus Codec_recover(const Codec *codec,
                                 unsigned char *shards,
                                 uint16_t present_mask);

const Codec *BlockCodec_get(void);
const Codec *CopyCodec_get(void);
const Codec *XorFecCodec_get(void);
const Codec *RsFecCodec_get(void);
const Codec *Codec_get(CodecKind kind);

#endif /* CODEC_H */
