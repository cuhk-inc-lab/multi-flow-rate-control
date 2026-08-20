#include "block_codec.h"
#include "stream_config.h"

static void block_encode(const Codec *self, unsigned char *data, size_t len)
{
    size_t byte;

    (void)self;

    if (data == NULL || len != DECODE_BLOCK) {
        return;
    }

    for (byte = 0; byte < len; byte++) {
        data[byte] = (unsigned char)(data[byte] + 1u);
    }
}

static void block_decode(const Codec *self, unsigned char *data, size_t len)
{
    size_t byte;

    (void)self;

    if (data == NULL || len != DECODE_BLOCK) {
        return;
    }

    for (byte = 0; byte < len; byte++) {
        data[byte] = (unsigned char)(data[byte] - 1u);
    }
}

static size_t block_input_block_size(const Codec *self)
{
    (void)self;
    return DECODE_BLOCK;
}

static size_t block_output_block_size(const Codec *self)
{
    (void)self;
    return DECODE_BLOCK;
}

static size_t block_data_shards(const Codec *self)
{
    (void)self;
    return PACKAGES_PER_DECODE_BLOCK;
}

static size_t block_parity_shards(const Codec *self)
{
    (void)self;
    return 0u;
}

static int block_is_systematic(const Codec *self)
{
    (void)self;
    return 0;
}

static int block_allows_best_effort(const Codec *self)
{
    (void)self;
    return 1;
}

static const CodecVTable block_codec_vtable = {
    .encode = block_encode,
    .decode = block_decode,
    .input_block_size = block_input_block_size,
    .output_block_size = block_output_block_size,
    .data_shards = block_data_shards,
    .parity_shards = block_parity_shards,
    .is_systematic = block_is_systematic,
    .allows_best_effort = block_allows_best_effort,
};

static const Codec block_codec = {
    .vtable = &block_codec_vtable,
    .impl   = NULL,
};

const Codec *BlockCodec_get(void)
{
    return &block_codec;
}
