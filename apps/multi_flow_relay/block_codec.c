#include "block_codec.h"
#include "stream_config.h"

#include <string.h>

static void block_encode(const Codec *self, unsigned char *data, size_t len)
{
    unsigned char in[DECODE_BLOCK];

    (void)self;

    if (len != ENCODE_BLOCK) {
        return;
    }

    memcpy(in, data, DECODE_BLOCK);

    for (size_t pkg = 0; pkg < PACKAGES_PER_DECODE_BLOCK; pkg++) {
        size_t        off_in   = pkg * PKG_SIZE;
        size_t        off_out1 = pkg * PKG_SIZE;
        size_t        off_out2 = (pkg + PACKAGES_PER_DECODE_BLOCK) * PKG_SIZE;
        unsigned char add1     = (unsigned char)(pkg + 1u);
        unsigned char add2     = (unsigned char)(pkg + 5u);
        size_t        j;

        for (j = 0; j < PKG_SIZE; j++) {
            data[off_out1 + j] = (unsigned char)(in[off_in + j] + add1);
            data[off_out2 + j] = (unsigned char)(in[off_in + j] + add2);
        }
    }
}

static void block_decode(const Codec *self, unsigned char *data, size_t len)
{
    unsigned char out[DECODE_BLOCK];

    (void)self;

    if (len != ENCODE_BLOCK) {
        return;
    }

    for (size_t pkg = 0; pkg < PACKAGES_PER_DECODE_BLOCK; pkg++) {
        size_t        off_enc1 = pkg * PKG_SIZE;
        size_t        off_out  = pkg * PKG_SIZE;
        unsigned char sub1     = (unsigned char)(pkg + 1u);
        size_t        j;

        for (j = 0; j < PKG_SIZE; j++) {
            out[off_out + j] = (unsigned char)(data[off_enc1 + j] - sub1);
        }
    }

    memcpy(data, out, DECODE_BLOCK);
}

static const CodecVTable block_codec_vtable = {
    .encode = block_encode,
    .decode = block_decode,
};

static const Codec block_codec = {
    .vtable = &block_codec_vtable,
    .impl   = NULL,
};

const Codec *BlockCodec_get(void)
{
    return &block_codec;
}
