#include "codec.h"
#include "stream_config.h"
#include <stdio.h>
#include <string.h>

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
    size_t byte;

    for (byte = 0; byte < DECODE_BLOCK; byte++) {
        encoded[byte] = (unsigned char)(byte * 31u + 7u);
    }
    memcpy(original, encoded, sizeof(original));
    Codec_encode(XorFecCodec_get(), encoded, sizeof(encoded));

    memset(encoded + 2 * PKG_SIZE, 0, PKG_SIZE);
    if (Codec_recover(XorFecCodec_get(), encoded, 0x1bu) != CODEC_RECOVER_OK ||
        memcmp(encoded + 2 * PKG_SIZE, original + 2 * PKG_SIZE, PKG_SIZE) != 0) {
        return -1;
    }

    if (Codec_recover(XorFecCodec_get(), encoded, 0x1cu) !=
        CODEC_RECOVER_UNAVAILABLE) {
        return -1;
    }

    return 0;
}

static int test_rs_fec_recovers_two_shards(void)
{
    unsigned char encoded[RS_FEC_ENCODE_BLOCK];
    unsigned char original[DECODE_BLOCK];
    size_t byte;

    for (byte = 0; byte < DECODE_BLOCK; byte++) {
        encoded[byte] = (unsigned char)(byte * 13u + 11u);
    }
    memcpy(original, encoded, sizeof(original));
    Codec_encode(RsFecCodec_get(), encoded, sizeof(encoded));

    memset(encoded + PKG_SIZE, 0, PKG_SIZE);
    memset(encoded + 4u * PKG_SIZE, 0, PKG_SIZE);
    if (Codec_recover(RsFecCodec_get(), encoded, 0x2du) != CODEC_RECOVER_OK ||
        memcmp(encoded, original, sizeof(original)) != 0) {
        return -1;
    }

    if (Codec_recover(RsFecCodec_get(), encoded, 0x29u) !=
        CODEC_RECOVER_UNAVAILABLE) {
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

    if (test_rs_fec_recovers_two_shards() != 0) {
        fprintf(stderr, "RS FEC codec test failed\n");
        return 1;
    }

    puts("XOR and RS FEC codec tests passed");
    return 0;
}
