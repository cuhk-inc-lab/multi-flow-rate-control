#ifndef CODEC_H
#define CODEC_H

#include <stddef.h>

typedef struct Codec Codec;

typedef enum CodecKind {
    CODEC_KIND_NONE = 0,
    CODEC_KIND_BLOCK,
    CODEC_KIND_COPY,
    CODEC_KIND_XOR_FEC
} CodecKind;

typedef struct CodecVTable {
    void (*encode)(const Codec *self, unsigned char *data, size_t len);
    void (*decode)(const Codec *self, unsigned char *data, size_t len);
    size_t (*input_block_size)(const Codec *self);
    size_t (*output_block_size)(const Codec *self);
} CodecVTable;

struct Codec {
    const CodecVTable *vtable;
    const void        *impl;
};

void Codec_encode(const Codec *codec, unsigned char *data, size_t len);
void Codec_decode(const Codec *codec, unsigned char *data, size_t len);
size_t Codec_input_block_size(const Codec *codec);
size_t Codec_output_block_size(const Codec *codec);

const Codec *BlockCodec_get(void);
const Codec *CopyCodec_get(void);
const Codec *XorFecCodec_get(void);
const Codec *Codec_get(CodecKind kind);

#endif /* CODEC_H */
