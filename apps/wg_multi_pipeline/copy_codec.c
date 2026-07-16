#include "codec.h"
#include "stream_config.h"

#include <string.h>

/*
 * Benchmark codec: preserve the BlockCodec's 4-packet to 8-packet geometry
 * without doing per-byte coding work. The original payload stays in the
 * systematic first half; the second half represents unused coded capacity.
 */
static void copy_encode(const Codec *self, unsigned char *data, size_t len)
{
    (void)self;

    if (len != ENCODE_BLOCK) {
        return;
    }

    memset(data + DECODE_BLOCK, 0, ENCODE_BLOCK - DECODE_BLOCK);
}

static void copy_decode(const Codec *self, unsigned char *data, size_t len)
{
    (void)self;
    (void)data;

    if (len != ENCODE_BLOCK) {
        return;
    }
}

static size_t copy_input_block_size(const Codec *self)
{
    (void)self;
    return DECODE_BLOCK;
}

static size_t copy_output_block_size(const Codec *self)
{
    (void)self;
    return ENCODE_BLOCK;
}

static const CodecVTable copy_codec_vtable = {
    .encode = copy_encode,
    .decode = copy_decode,
    .input_block_size = copy_input_block_size,
    .output_block_size = copy_output_block_size,
};

static const Codec copy_codec = {
    .vtable = &copy_codec_vtable,
    .impl = NULL,
};

const Codec *CopyCodec_get(void)
{
    return &copy_codec;
}
