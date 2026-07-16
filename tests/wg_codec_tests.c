#include "codec.h"
#include "stream_config.h"
#include "xor_fec_codec.h"

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#define XOR_FEC_SHARD_COUNT \
    (PACKAGES_PER_DECODE_BLOCK + XOR_FEC_PARITY_SHARDS)

static int test_copy_codec_preserves_systematic_payload(void)
{
    unsigned char block[ENCODE_BLOCK];
    unsigned char original[DECODE_BLOCK];
    size_t        byte;

    for (byte = 0; byte < DECODE_BLOCK; byte++) {
        block[byte] = (unsigned char)(byte * 17u + 3u);
    }
    memcpy(original, block, sizeof(original));
    memset(block + DECODE_BLOCK, 0xa5, ENCODE_BLOCK - DECODE_BLOCK);

    Codec_encode(CopyCodec_get(), block, sizeof(block));
    Codec_decode(CopyCodec_get(), block, sizeof(block));

    return memcmp(block, original, sizeof(original)) == 0 &&
           memcmp(block + DECODE_BLOCK,
                  (unsigned char[ENCODE_BLOCK - DECODE_BLOCK]){0},
                  ENCODE_BLOCK - DECODE_BLOCK) == 0 ? 0 : -1;
}

static int test_xor_fec_recovers_one_shard(void)
{
    unsigned char encoded[XOR_FEC_ENCODE_BLOCK];
    unsigned char original[DECODE_BLOCK];
    unsigned char shards[XOR_FEC_SHARD_COUNT][PKG_SIZE];
    unsigned char *shard_ptrs[XOR_FEC_SHARD_COUNT];
    bool present[XOR_FEC_SHARD_COUNT];
    size_t shard;
    size_t byte;

    for (byte = 0; byte < DECODE_BLOCK; byte++) {
        encoded[byte] = (unsigned char)(byte * 31u + 7u);
    }
    memcpy(original, encoded, sizeof(original));
    Codec_encode(XorFecCodec_get(), encoded, sizeof(encoded));

    for (shard = 0; shard < XOR_FEC_SHARD_COUNT; shard++) {
        memcpy(shards[shard], encoded + shard * PKG_SIZE, PKG_SIZE);
        shard_ptrs[shard] = shards[shard];
        present[shard] = true;
    }

    memset(shards[2], 0, PKG_SIZE);
    present[2] = false;
    if (XorFecCodec_recover_one(shard_ptrs, present) != 1 ||
        memcmp(shards[2], original + 2 * PKG_SIZE, PKG_SIZE) != 0) {
        return -1;
    }
    present[2] = true;

    memcpy(shards[4], encoded + 4 * PKG_SIZE, PKG_SIZE);
    present[4] = true;
    memset(shards[4], 0, PKG_SIZE);
    present[4] = false;
    if (XorFecCodec_recover_one(shard_ptrs, present) != 1 ||
        memcmp(shards[4], encoded + 4 * PKG_SIZE, PKG_SIZE) != 0) {
        return -1;
    }
    present[4] = true;

    present[0] = false;
    present[1] = false;
    if (XorFecCodec_recover_one(shard_ptrs, present) != -1) {
        return -1;
    }

    return 0;
}

int main(void)
{
    if (test_copy_codec_preserves_systematic_payload() != 0) {
        fprintf(stderr, "Copy codec test failed\n");
        return 1;
    }

    if (test_xor_fec_recovers_one_shard() != 0) {
        fprintf(stderr, "XOR FEC codec test failed\n");
        return 1;
    }

    puts("XOR FEC codec test passed");
    return 0;
}
