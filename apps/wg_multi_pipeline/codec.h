#ifndef CODEC_H
#define CODEC_H

#include <stddef.h>

typedef struct Codec Codec;

typedef struct CodecVTable {
    void (*encode)(const Codec *self, unsigned char *data, size_t len);
    void (*decode)(const Codec *self, unsigned char *data, size_t len);
} CodecVTable;

struct Codec {
    const CodecVTable *vtable;
    const void        *impl;
};

void Codec_encode(const Codec *codec, unsigned char *data, size_t len);
void Codec_decode(const Codec *codec, unsigned char *data, size_t len);

const Codec *BlockCodec_get(void);

#endif /* CODEC_H */
