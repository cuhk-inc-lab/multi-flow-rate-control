#include "codec.h"
#include "stream_config.h"

/*
 * Benchmark codec: preserve one four-packet input block unchanged in a
 * four-packet encoded block. The pipeline already places input bytes in the
 * destination work buffer, so the in-place encode/decode operations are no-ops.
 */
static void copy_encode(const Codec *self, unsigned char *data, size_t len)
{
    (void)self;
    (void)data;
    (void)len;
}

static void copy_decode(const Codec *self, unsigned char *data, size_t len)
{
    (void)self;
    (void)data;

    (void)len;
}

static size_t copy_input_block_size(const Codec *self)
{
    (void)self;
    return DECODE_BLOCK;
}

static size_t copy_output_block_size(const Codec *self)
{
    (void)self;
    return DECODE_BLOCK;
}

static size_t copy_data_shards(const Codec *self)
{
    (void)self;
    return PACKAGES_PER_DECODE_BLOCK;
}

static size_t copy_parity_shards(const Codec *self)
{
    (void)self;
    return 0u;
}

static int copy_is_systematic(const Codec *self)
{
    (void)self;
    return 1;
}

static const CodecVTable copy_codec_vtable = {
    .encode = copy_encode,
    .decode = copy_decode,
    .input_block_size = copy_input_block_size,
    .output_block_size = copy_output_block_size,
    .data_shards = copy_data_shards,
    .parity_shards = copy_parity_shards,
    .is_systematic = copy_is_systematic,
};

static const Codec copy_codec = {
    .vtable = &copy_codec_vtable,
    .impl = NULL,
};

const Codec *CopyCodec_get(void)
{
    return &copy_codec;
}
