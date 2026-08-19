#include "codec.h"
#include "rs_codec.h"
#include "stream_config.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define EXPECT(cond)                                                           \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);   \
            return -1;                                                         \
        }                                                                      \
    } while (0)

static void fill_prefix(unsigned char *block, size_t input_bytes, unsigned seed)
{
    size_t i;
    unsigned state = seed;

    for (i = 0; i < input_bytes; i++) {
        state = state * 1103515245u + 12345u;
        block[i] = (unsigned char)(state >> 16);
    }
}

static int encode_with_impl(RsEncodeImpl impl, unsigned char *block, size_t len)
{
    RsCodec_set_encode_impl(impl);
    Codec_encode(RsCodec_get(), block, len);
    return 0;
}

static int test_rs_16_2_fast_path_bit_exact(void)
{
    const size_t k = 16;
    const size_t r = 2;
    const size_t n = k + r;
    const size_t block_bytes = n * PKG_SIZE;
    const size_t input_bytes = k * PKG_SIZE;
    unsigned trial;

    EXPECT(RsCodec_set_params(k, r) == 0);

    for (trial = 0; trial < 32u; trial++) {
        unsigned char *general = calloc(1, block_bytes);
        unsigned char *fast = calloc(1, block_bytes);

        EXPECT(general != NULL && fast != NULL);
        fill_prefix(general, input_bytes, 0xC0FFEEu + trial * 17u);
        memcpy(fast, general, block_bytes);

        encode_with_impl(RS_ENCODE_GENERAL, general, block_bytes);
        EXPECT(RsCodec_last_encode_impl() == RS_ENCODE_GENERAL);

        encode_with_impl(RS_ENCODE_FAST_16_2, fast, block_bytes);
        EXPECT(RsCodec_last_encode_impl() == RS_ENCODE_FAST_16_2);
        EXPECT(memcmp(general, fast, block_bytes) == 0);

        free(general);
        free(fast);
    }

    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    return 0;
}

static int test_rs_16_2_fast_path_edge_lengths(void)
{
    const size_t k = 16;
    const size_t r = 2;
    const size_t n = k + r;
    const size_t block_bytes = n * PKG_SIZE;
    const size_t input_bytes = k * PKG_SIZE;
    unsigned char *general;
    unsigned char *fast;
    unsigned char *ref;
    size_t odd_len;

    EXPECT(RsCodec_set_params(k, r) == 0);
    general = calloc(1, block_bytes);
    fast = calloc(1, block_bytes);
    ref = calloc(1, block_bytes);
    EXPECT(general != NULL && fast != NULL && ref != NULL);

    fill_prefix(ref, input_bytes, 0xABCDu);
    /* Poison parity region so no-op is visible. */
    memset(ref + input_bytes, 0x5a, block_bytes - input_bytes);

    /* API only encodes exact n*PKG_SIZE; short lengths must leave buffer intact. */
    {
        const size_t bad_lens[] = {0u, 1u, PKG_SIZE - 1u, PKG_SIZE,
                                   block_bytes - 1u};
        size_t i;

        for (i = 0; i < sizeof(bad_lens) / sizeof(bad_lens[0]); i++) {
            memcpy(general, ref, block_bytes);
            memcpy(fast, ref, block_bytes);
            encode_with_impl(RS_ENCODE_GENERAL, general, bad_lens[i]);
            encode_with_impl(RS_ENCODE_FAST_16_2, fast, bad_lens[i]);
            EXPECT(memcmp(general, ref, block_bytes) == 0);
            EXPECT(memcmp(fast, ref, block_bytes) == 0);
            EXPECT(memcmp(general, fast, block_bytes) == 0);
        }
    }

    /* Full block: bit-exact. */
    memcpy(general, ref, block_bytes);
    memcpy(fast, ref, block_bytes);
    encode_with_impl(RS_ENCODE_GENERAL, general, block_bytes);
    encode_with_impl(RS_ENCODE_FAST_16_2, fast, block_bytes);
    EXPECT(memcmp(general, fast, block_bytes) == 0);

    /* Non-word-aligned source content (odd pattern) still matches. */
    odd_len = input_bytes;
    memset(ref, 0, block_bytes);
    fill_prefix(ref, odd_len, 0x1111u);
    ref[1] = 0x7f;
    ref[input_bytes / 2u + 1u] = 0x01;
    memcpy(general, ref, block_bytes);
    memcpy(fast, ref, block_bytes);
    encode_with_impl(RS_ENCODE_GENERAL, general, block_bytes);
    encode_with_impl(RS_ENCODE_FAST_16_2, fast, block_bytes);
    EXPECT(memcmp(general, fast, block_bytes) == 0);

    free(general);
    free(fast);
    free(ref);
    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    return 0;
}

static int roundtrip_recover(size_t k, size_t r, uint64_t drop_mask)
{
    size_t n = k + r;
    size_t block_bytes = n * PKG_SIZE;
    size_t input_bytes = k * PKG_SIZE;
    unsigned char *block = calloc(1, block_bytes);
    unsigned char *source = calloc(1, input_bytes);
    uint8_t *bits = calloc(1, codec_present_bytes(n));
    size_t shard;
    int rc = -1;

    EXPECT(block != NULL && source != NULL && bits != NULL);
    EXPECT(RsCodec_set_params(k, r) == 0);
    fill_prefix(block, input_bytes, 0x4B1Du + (unsigned)(k * 13u + r));
    memcpy(source, block, input_bytes);
    Codec_encode(RsCodec_get(), block, block_bytes);

    codec_present_clear_all(bits, n);
    for (shard = 0; shard < n; shard++) {
        if ((drop_mask & (1ull << shard)) == 0) {
            codec_present_set(bits, shard);
        } else {
            memset(block + shard * PKG_SIZE, 0, PKG_SIZE);
        }
    }
    EXPECT(Codec_recover(RsCodec_get(), block, bits, n) == CODEC_RECOVER_OK);
    EXPECT(memcmp(block, source, input_bytes) == 0);
    rc = 0;
    free(block);
    free(source);
    free(bits);
    return rc;
}

static int test_rs_fast_path_fallback_geometries(void)
{
    EXPECT(RsCodec_set_params(4u, 2u) == 0);
    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    {
        unsigned char block[6u * PKG_SIZE];

        memset(block, 0, sizeof(block));
        fill_prefix(block, 4u * PKG_SIZE, 0x42u);
        Codec_encode(RsCodec_get(), block, sizeof(block));
        EXPECT(RsCodec_last_encode_impl() ==
               (RsCodec_simd_available() ? RS_ENCODE_SIMD
                                         : RS_ENCODE_GENERAL));
    }
    EXPECT(roundtrip_recover(4u, 2u, (1ull << 1) | (1ull << 4)) == 0);

    EXPECT(RsCodec_set_params(8u, 2u) == 0);
    {
        size_t n = 10u;
        unsigned char *block = calloc(1, n * PKG_SIZE);

        EXPECT(block != NULL);
        fill_prefix(block, 8u * PKG_SIZE, 0x88u);
        RsCodec_set_encode_impl(RS_ENCODE_AUTO);
        Codec_encode(RsCodec_get(), block, n * PKG_SIZE);
        EXPECT(RsCodec_last_encode_impl() ==
               (RsCodec_simd_available() ? RS_ENCODE_SIMD
                                         : RS_ENCODE_GENERAL));
        free(block);
    }
    EXPECT(roundtrip_recover(8u, 2u, (1ull << 3) | (1ull << 9)) == 0);

    EXPECT(RsCodec_set_params(16u, 3u) == 0);
    {
        size_t n = 19u;
        unsigned char *block = calloc(1, n * PKG_SIZE);

        EXPECT(block != NULL);
        fill_prefix(block, 16u * PKG_SIZE, 0x99u);
        RsCodec_set_encode_impl(RS_ENCODE_AUTO);
        Codec_encode(RsCodec_get(), block, n * PKG_SIZE);
        EXPECT(RsCodec_last_encode_impl() ==
               (RsCodec_simd_available() ? RS_ENCODE_SIMD
                                         : RS_ENCODE_GENERAL));
        free(block);
    }

    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    return 0;
}

static int test_rs_16_2_recovery_compatibility(void)
{
    const size_t k = 16;
    const size_t r = 2;
    const size_t n = 18;
    const size_t block_bytes = n * PKG_SIZE;
    const size_t input_bytes = k * PKG_SIZE;
    unsigned char *block;
    unsigned char *source;
    uint8_t bits[codec_present_bytes(18)];
    size_t shard;

    EXPECT(RsCodec_set_params(k, r) == 0);
    block = calloc(1, block_bytes);
    source = calloc(1, input_bytes);
    EXPECT(block != NULL && source != NULL);

    fill_prefix(block, input_bytes, 0x5150u);
    memcpy(source, block, input_bytes);
    encode_with_impl(RS_ENCODE_FAST_16_2, block, block_bytes);
    EXPECT(RsCodec_last_encode_impl() == RS_ENCODE_FAST_16_2);

    /* Drop 1 data shard. */
    codec_present_set_all(bits, n);
    codec_present_clear_all(bits, n);
    for (shard = 0; shard < n; shard++) {
        if (shard != 5u) {
            codec_present_set(bits, shard);
        } else {
            memset(block + shard * PKG_SIZE, 0, PKG_SIZE);
        }
    }
    EXPECT(Codec_recover(RsCodec_get(), block, bits, n) == CODEC_RECOVER_OK);
    EXPECT(memcmp(block, source, input_bytes) == 0);

    /* Re-encode and drop 2 shards (1 data + 1 parity). */
    memcpy(block, source, input_bytes);
    memset(block + input_bytes, 0, block_bytes - input_bytes);
    encode_with_impl(RS_ENCODE_FAST_16_2, block, block_bytes);
    codec_present_clear_all(bits, n);
    for (shard = 0; shard < n; shard++) {
        if (shard != 2u && shard != 17u) {
            codec_present_set(bits, shard);
        } else {
            memset(block + shard * PKG_SIZE, 0, PKG_SIZE);
        }
    }
    EXPECT(Codec_recover(RsCodec_get(), block, bits, n) == CODEC_RECOVER_OK);
    EXPECT(memcmp(block, source, input_bytes) == 0);

    free(block);
    free(source);
    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    return 0;
}

typedef struct ConcArg {
    unsigned char *blocks;
    size_t block_bytes;
    unsigned count;
    int failed;
} ConcArg;

static void *conc_worker(void *arg)
{
    ConcArg *c = arg;
    unsigned i;

    for (i = 0; i < c->count; i++) {
        unsigned char *block = c->blocks + (size_t)i * c->block_bytes;

        Codec_encode(RsCodec_get(), block, c->block_bytes);
    }
    return NULL;
}

static int test_rs_16_2_concurrent_encode(void)
{
    const unsigned flows = 8u;
    const unsigned blocks_each = 64u;
    const size_t block_bytes = 18u * PKG_SIZE;
    const size_t input_bytes = 16u * PKG_SIZE;
    unsigned char *arena;
    pthread_t threads[8];
    ConcArg args[8];
    unsigned f;
    unsigned char *golden;
    unsigned char *probe;

    EXPECT(RsCodec_set_params(16u, 2u) == 0);
    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    arena = calloc((size_t)flows * blocks_each, block_bytes);
    golden = calloc(1, block_bytes);
    probe = calloc(1, block_bytes);
    EXPECT(arena != NULL && golden != NULL && probe != NULL);

    for (f = 0; f < flows; f++) {
        unsigned b;

        args[f].blocks = arena + (size_t)f * blocks_each * block_bytes;
        args[f].block_bytes = block_bytes;
        args[f].count = blocks_each;
        args[f].failed = 0;
        for (b = 0; b < blocks_each; b++) {
            fill_prefix(args[f].blocks + (size_t)b * block_bytes, input_bytes,
                        0x1000u + f * 64u + b);
        }
    }

    for (f = 0; f < flows; f++) {
        EXPECT(pthread_create(&threads[f], NULL, conc_worker, &args[f]) == 0);
    }
    for (f = 0; f < flows; f++) {
        pthread_join(threads[f], NULL);
        EXPECT(args[f].failed == 0);
    }

    /* Spot-check one block against forced general. */
    fill_prefix(golden, input_bytes, 0xDEADu);
    memcpy(probe, golden, block_bytes);
    encode_with_impl(RS_ENCODE_GENERAL, golden, block_bytes);
    encode_with_impl(RS_ENCODE_FAST_16_2, probe, block_bytes);
    EXPECT(memcmp(golden, probe, block_bytes) == 0);

    free(arena);
    free(golden);
    free(probe);
    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    return 0;
}

int main(void)
{
    if (test_rs_16_2_fast_path_bit_exact() != 0) {
        fputs("test_rs_16_2_fast_path_bit_exact failed\n", stderr);
        return 1;
    }
    if (test_rs_16_2_fast_path_edge_lengths() != 0) {
        fputs("test_rs_16_2_fast_path_edge_lengths failed\n", stderr);
        return 1;
    }
    if (test_rs_fast_path_fallback_geometries() != 0) {
        fputs("test_rs_fast_path_fallback_geometries failed\n", stderr);
        return 1;
    }
    if (test_rs_16_2_recovery_compatibility() != 0) {
        fputs("test_rs_16_2_recovery_compatibility failed\n", stderr);
        return 1;
    }
    if (test_rs_16_2_concurrent_encode() != 0) {
        fputs("test_rs_16_2_concurrent_encode failed\n", stderr);
        return 1;
    }
    puts("rs encode fast-path tests passed");
    return 0;
}
