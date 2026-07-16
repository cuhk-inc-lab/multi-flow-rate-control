#include "xor_fec_codec.h"

#include "stream_config.h"

#include <stddef.h>
#include <string.h>

#define XOR_FEC_SHARD_COUNT \
    (PACKAGES_PER_DECODE_BLOCK + XOR_FEC_PARITY_SHARDS)

static void xor_fec_encode(const Codec *self, unsigned char *data, size_t len)
{
    unsigned char *parity;
    size_t         shard;
    size_t         byte;

    (void)self;

    if (len != XOR_FEC_ENCODE_BLOCK) {
        return;
    }

    parity = data + DECODE_BLOCK;
    memset(parity, 0, PKG_SIZE);

    for (shard = 0; shard < PACKAGES_PER_DECODE_BLOCK; shard++) {
        const unsigned char *input = data + shard * PKG_SIZE;

        for (byte = 0; byte < PKG_SIZE; byte++) {
            parity[byte] ^= input[byte];
        }
    }
}

static void xor_fec_decode(const Codec *self, unsigned char *data, size_t len)
{
    (void)self;

    /*
     * This codec is systematic: with no loss, the first four shards are
     * already the original payload. Packet-loss recovery is performed by
     * XorFecCodec_recover_one() after a transport has identified the absent
     * shard in a FEC group.
     */
    if (len != XOR_FEC_ENCODE_BLOCK) {
        return;
    }

    (void)data;
}

static size_t xor_fec_input_block_size(const Codec *self)
{
    (void)self;
    return DECODE_BLOCK;
}

static size_t xor_fec_output_block_size(const Codec *self)
{
    (void)self;
    return XOR_FEC_ENCODE_BLOCK;
}

static const CodecVTable xor_fec_codec_vtable = {
    .encode = xor_fec_encode,
    .decode = xor_fec_decode,
    .input_block_size = xor_fec_input_block_size,
    .output_block_size = xor_fec_output_block_size,
};

static const Codec xor_fec_codec = {
    .vtable = &xor_fec_codec_vtable,
    .impl = NULL,
};

const Codec *XorFecCodec_get(void)
{
    return &xor_fec_codec;
}

int XorFecCodec_recover_one(unsigned char *shards[XOR_FEC_SHARD_COUNT],
                            const bool present[XOR_FEC_SHARD_COUNT])
{
    size_t missing = XOR_FEC_SHARD_COUNT;
    size_t shard;
    size_t byte;

    if (shards == NULL || present == NULL) {
        return -1;
    }

    for (shard = 0; shard < XOR_FEC_SHARD_COUNT; shard++) {
        if (!present[shard]) {
            if (missing != XOR_FEC_SHARD_COUNT || shards[shard] == NULL) {
                return -1;
            }
            missing = shard;
        } else if (shards[shard] == NULL) {
            return -1;
        }
    }

    if (missing == XOR_FEC_SHARD_COUNT) {
        return 0;
    }

    memset(shards[missing], 0, PKG_SIZE);
    for (shard = 0; shard < XOR_FEC_SHARD_COUNT; shard++) {
        if (shard == missing) {
            continue;
        }
        for (byte = 0; byte < PKG_SIZE; byte++) {
            shards[missing][byte] ^= shards[shard][byte];
        }
    }

    return 1;
}
