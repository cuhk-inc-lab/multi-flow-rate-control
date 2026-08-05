#include "codec.h"
#include "rs_codec.h"
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

static int test_rs_recovers_two_shards(void)
{
    unsigned char encoded[RS_FEC_ENCODE_BLOCK];
    unsigned char original[DECODE_BLOCK];
    size_t byte;

    for (byte = 0; byte < DECODE_BLOCK; byte++) {
        encoded[byte] = (unsigned char)(byte * 19u + 5u);
    }
    memcpy(original, encoded, sizeof(original));
    Codec_encode(RsCodec_get(), encoded, sizeof(encoded));

    memset(encoded + PKG_SIZE, 0, PKG_SIZE);
    memset(encoded + 4u * PKG_SIZE, 0, PKG_SIZE);
    if (Codec_recover(RsCodec_get(), encoded, 0x2du) != CODEC_RECOVER_OK ||
        memcmp(encoded, original, sizeof(original)) != 0) {
        return -1;
    }

    memset(encoded + 2u * PKG_SIZE, 0, PKG_SIZE);
    if (Codec_recover(RsCodec_get(), encoded, 0x3bu) != CODEC_RECOVER_OK ||
        memcmp(encoded, original, sizeof(original)) != 0) {
        return -1;
    }

    if (Codec_recover(RsCodec_get(), encoded, 0x29u) !=
        CODEC_RECOVER_UNAVAILABLE) {
        return -1;
    }

    return 0;
}

static unsigned popcount_mask(uint16_t mask)
{
    unsigned count = 0;

    while (mask != 0) {
        count += mask & 1u;
        mask >>= 1;
    }
    return count;
}

static void fill_rs_source(unsigned char block[RS_FEC_ENCODE_BLOCK],
                           unsigned seed)
{
    size_t byte;
    unsigned state = seed;

    for (byte = 0; byte < DECODE_BLOCK; byte++) {
        state = state * 1103515245u + 12345u;
        block[byte] = (unsigned char)(state >> 16);
    }
    memset(block + DECODE_BLOCK, 0, RS_FEC_ENCODE_BLOCK - DECODE_BLOCK);
}

static int test_rs_matrix_exhaustive_compatibility(void)
{
    const uint16_t valid_mask = 0x3fu;
    uint16_t mask;

    if (RsCodec_prepare_matrix() != 0 || !RsCodec_matrix_ready()) {
        return -1;
    }

    for (mask = 0; mask <= valid_mask; mask++) {
        unsigned char encoded[RS_FEC_ENCODE_BLOCK];
        unsigned char source[DECODE_BLOCK];
        unsigned char legacy[RS_FEC_ENCODE_BLOCK];
        unsigned char matrix[RS_FEC_ENCODE_BLOCK];
        unsigned present = popcount_mask(mask);
        size_t shard;
        CodecRecoverStatus legacy_status;
        CodecRecoverStatus matrix_status;

        fill_rs_source(encoded, 0x4d2u + mask);
        memcpy(source, encoded, sizeof(source));
        if (RsCodec_set_recover_mode(RS_RECOVER_LEGACY) != 0) {
            return -1;
        }
        Codec_encode(RsCodec_get(), encoded, sizeof(encoded));
        memcpy(legacy, encoded, sizeof(legacy));
        memcpy(matrix, encoded, sizeof(matrix));

        for (shard = 0; shard < RS_FEC_TOTAL_SHARDS; shard++) {
            if ((mask & (uint16_t)(1u << shard)) == 0) {
                memset(legacy + shard * PKG_SIZE, 0, PKG_SIZE);
                memset(matrix + shard * PKG_SIZE, 0, PKG_SIZE);
            }
        }

        legacy_status = Codec_recover(RsCodec_get(), legacy, mask);
        if (RsCodec_set_recover_mode(RS_RECOVER_MATRIX) != 0) {
            return -1;
        }
        matrix_status = Codec_recover(RsCodec_get(), matrix, mask);

        if (present < RS_FEC_DATA_SHARDS) {
            if (legacy_status != CODEC_RECOVER_UNAVAILABLE ||
                matrix_status != CODEC_RECOVER_UNAVAILABLE) {
                return -1;
            }
            continue;
        }

        if (legacy_status != CODEC_RECOVER_OK ||
            matrix_status != CODEC_RECOVER_OK ||
            memcmp(matrix, source, sizeof(source)) != 0) {
            return -1;
        }

        /*
         * Matrix mode deliberately skips parity-only reconstruction when all
         * systematic data shards are present. Otherwise it must reproduce the
         * complete legacy 6-shard result byte-for-byte.
         */
        if ((mask & 0x0fu) != 0x0fu) {
            if (memcmp(matrix, legacy, sizeof(matrix)) != 0) {
                return -1;
            }
        }
    }

    return RsCodec_set_recover_mode(RS_RECOVER_MATRIX);
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

    if (test_rs_recovers_two_shards() != 0) {
        fprintf(stderr, "rscode RS codec test failed\n");
        return 1;
    }

    if (test_rs_matrix_exhaustive_compatibility() != 0) {
        fprintf(stderr, "rscode RS matrix exhaustive compatibility test failed\n");
        return 1;
    }

    puts("XOR, RS FEC, and rscode RS codec tests passed (22 valid masks checked)");
    return 0;
}
