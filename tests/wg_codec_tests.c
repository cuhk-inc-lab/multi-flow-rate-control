#include "codec.h"
#include "rs_codec.h"
#include "stream_config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static CodecRecoverStatus recover_mask(const Codec *codec, unsigned char *shards,
                                       size_t shard_count, uint64_t mask)
{
    uint8_t bits[codec_present_bytes(64)];

    codec_present_from_u64(bits, shard_count, mask);
    return Codec_recover(codec, shards, bits, shard_count);
}

static int test_copy_codec_preserves_systematic_payload(void)
{
    const Codec  *codec = CopyCodec_get();
    unsigned char block[DECODE_BLOCK];
    unsigned char original[DECODE_BLOCK];
    size_t        byte;

    if (Codec_input_block_size(codec) != DECODE_BLOCK ||
        Codec_output_block_size(codec) != DECODE_BLOCK ||
        Codec_data_shards(codec) != 4u || Codec_parity_shards(codec) != 0u) {
        return -1;
    }
    for (byte = 0; byte < DECODE_BLOCK; byte++) {
        block[byte] = (unsigned char)(byte * 17u + 3u);
    }
    memcpy(original, block, sizeof(original));

    Codec_encode(codec, block, sizeof(block));
    Codec_decode(codec, block, sizeof(block));

    return memcmp(block, original, sizeof(original)) == 0 ? 0 : -1;
}

static int test_block_codec_uniform_add_subtract(void)
{
    const Codec  *codec = BlockCodec_get();
    unsigned char block[DECODE_BLOCK];
    unsigned char original[DECODE_BLOCK];
    size_t        byte;

    if (Codec_input_block_size(codec) != DECODE_BLOCK ||
        Codec_output_block_size(codec) != DECODE_BLOCK ||
        Codec_data_shards(codec) != 4u || Codec_parity_shards(codec) != 0u ||
        Codec_is_systematic(codec)) {
        return -1;
    }
    for (byte = 0; byte < DECODE_BLOCK; byte++) {
        block[byte] = (unsigned char)(byte * 29u + 251u);
    }
    memcpy(original, block, sizeof(original));

    Codec_encode(codec, block, sizeof(block));
    for (byte = 0; byte < sizeof(block); byte++) {
        if (block[byte] != (unsigned char)(original[byte] + 1u)) {
            return -1;
        }
    }
    Codec_decode(codec, block, sizeof(block));
    return memcmp(block, original, sizeof(original)) == 0 ? 0 : -1;
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
    if (recover_mask(XorFecCodec_get(), encoded, 5u, 0x1bu) != CODEC_RECOVER_OK ||
        memcmp(encoded + 2 * PKG_SIZE, original + 2 * PKG_SIZE, PKG_SIZE) != 0) {
        return -1;
    }

    if (recover_mask(XorFecCodec_get(), encoded, 5u, 0x1cu) !=
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
    if (recover_mask(RsFecCodec_get(), encoded, 6u, 0x2du) != CODEC_RECOVER_OK ||
        memcmp(encoded, original, sizeof(original)) != 0) {
        return -1;
    }

    if (recover_mask(RsFecCodec_get(), encoded, 6u, 0x29u) !=
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
    if (recover_mask(RsCodec_get(), encoded, 6u, 0x2du) != CODEC_RECOVER_OK ||
        memcmp(encoded, original, sizeof(original)) != 0) {
        return -1;
    }

    memset(encoded + 2u * PKG_SIZE, 0, PKG_SIZE);
    if (recover_mask(RsCodec_get(), encoded, 6u, 0x3bu) != CODEC_RECOVER_OK ||
        memcmp(encoded, original, sizeof(original)) != 0) {
        return -1;
    }

    if (recover_mask(RsCodec_get(), encoded, 6u, 0x29u) !=
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

static void fill_rs_source(unsigned char *block, size_t block_len, unsigned seed)
{
    size_t byte;
    unsigned state = seed;

    for (byte = 0; byte < DECODE_BLOCK && byte < block_len; byte++) {
        state = state * 1103515245u + 12345u;
        block[byte] = (unsigned char)(state >> 16);
    }
    if (block_len > DECODE_BLOCK) {
        memset(block + DECODE_BLOCK, 0, block_len - DECODE_BLOCK);
    }
}

static int test_rs_matrix_exhaustive_recovery(void)
{
    const uint16_t valid_mask = 0x3fu;
    uint16_t mask;

    if (RsCodec_set_params(4u, 2u) != 0 ||
        RsCodec_prepare_matrix() != 0 || !RsCodec_matrix_ready()) {
        return -1;
    }

    for (mask = 0; mask <= valid_mask; mask++) {
        unsigned char encoded[RS_FEC_ENCODE_BLOCK];
        unsigned char source[DECODE_BLOCK];
        unsigned present = popcount_mask(mask);
        size_t shard;
        CodecRecoverStatus status;

        fill_rs_source(encoded, sizeof(encoded), 0x4d2u + mask);
        memcpy(source, encoded, sizeof(source));
        Codec_encode(RsCodec_get(), encoded, sizeof(encoded));

        for (shard = 0; shard < RS_FEC_TOTAL_SHARDS; shard++) {
            if ((mask & (uint16_t)(1u << shard)) == 0) {
                memset(encoded + shard * PKG_SIZE, 0, PKG_SIZE);
            }
        }

        status = recover_mask(RsCodec_get(), encoded, 6u, mask);

        if (present < RS_FEC_DATA_SHARDS) {
            if (status != CODEC_RECOVER_UNAVAILABLE) {
                return -1;
            }
            continue;
        }

        if (status != CODEC_RECOVER_OK ||
            memcmp(encoded, source, sizeof(source)) != 0) {
            return -1;
        }
    }

    return 0;
}

static int test_rs_profile_one(size_t k, size_t r, uint64_t present_mask)
{
    unsigned char *encoded = NULL;
    unsigned char *source = NULL;
    size_t total;
    size_t shard;
    size_t n;
    size_t input_len;
    uint8_t *bits = NULL;
    int rc = -1;

    if (RsCodec_set_params(k, r) != 0) {
        return -1;
    }
    total = Codec_output_block_size(RsCodec_get());
    input_len = Codec_input_block_size(RsCodec_get());
    n = Codec_data_shards(RsCodec_get()) + Codec_parity_shards(RsCodec_get());
    if (total != n * PKG_SIZE || input_len != k * PKG_SIZE) {
        return -1;
    }

    encoded = malloc(total);
    source = malloc(input_len);
    bits = malloc(codec_present_bytes(n));
    if (encoded == NULL || source == NULL || bits == NULL) {
        goto out;
    }

    fill_rs_source(encoded, total, 0xABCDu + (unsigned)(k * 31u + r));
    memcpy(source, encoded, input_len);
    Codec_encode(RsCodec_get(), encoded, total);

    codec_present_from_u64(bits, n, present_mask);
    for (shard = 0; shard < n; shard++) {
        if (!codec_present_get(bits, shard)) {
            memset(encoded + shard * PKG_SIZE, 0, PKG_SIZE);
        }
    }
    if (Codec_recover(RsCodec_get(), encoded, bits, n) != CODEC_RECOVER_OK ||
        memcmp(encoded, source, input_len) != 0) {
        goto out;
    }
    rc = 0;
out:
    free(encoded);
    free(source);
    free(bits);
    return rc;
}

static int test_rs_profiles_recover(void)
{
    /* 4+1: drop shard 2 → mask 0x1b */
    if (test_rs_profile_one(4u, 1u, 0x1bu) != 0) {
        return -1;
    }
    /* 4+2: drop shards 1 and 4 → mask 0x2d */
    if (test_rs_profile_one(4u, 2u, 0x2du) != 0) {
        return -1;
    }
    /* 4+3: drop shards 0,3,5 → mask 0x56 */
    if (test_rs_profile_one(4u, 3u, 0x56u) != 0) {
        return -1;
    }
    /* custom 8+2: drop shards 1 and 7 → mask with bits 0,2,3,4,5,6,8,9 */
    if (test_rs_profile_one(8u, 2u, 0x3f5u) != 0) {
        return -1;
    }
    /* Beyond former uint32 ceiling: 40+2 drop shards 10 and 41 */
    if (test_rs_profile_one(40u, 2u, ((1ull << 42) - 1ull) ^ (1ull << 10) ^
                                      (1ull << 41)) != 0) {
        return -1;
    }
    /* GF(256) ceiling: n=256 must be rejected */
    if (RsCodec_set_params(254u, 2u) == 0) {
        return -1;
    }
    return RsCodec_set_params(4u, 2u);
}

int main(void)
{
    if (test_copy_codec_preserves_systematic_payload() != 0) {
        fprintf(stderr, "Copy codec test failed\n");
        return 1;
    }
    if (test_block_codec_uniform_add_subtract() != 0) {
        fprintf(stderr, "Block codec uniform +1/-1 test failed\n");
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

    if (RsCodec_set_params(4u, 2u) != 0) {
        fprintf(stderr, "rs default params init failed\n");
        return 1;
    }

    if (test_rs_recovers_two_shards() != 0) {
        fprintf(stderr, "rscode RS codec test failed\n");
        return 1;
    }

    if (test_rs_matrix_exhaustive_recovery() != 0) {
        fprintf(stderr, "rscode RS matrix exhaustive recovery test failed\n");
        return 1;
    }

    if (test_rs_profiles_recover() != 0) {
        fprintf(stderr, "rs runtime profile recovery test failed\n");
        return 1;
    }

    puts("Copy, block, XOR, RS FEC, and rscode RS codec tests passed");
    return 0;
}
