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

static uint64_t checksum_bytes(const unsigned char *buf, size_t len)
{
    uint64_t sum = 14695981039346656037ull;
    size_t i;

    for (i = 0; i < len; i++) {
        sum ^= buf[i];
        sum *= 1099511628211ull;
    }
    return sum;
}

static int encode_copy(RsEncodeImpl impl, unsigned char *dst,
                       const unsigned char *src, size_t len)
{
    memcpy(dst, src, len);
    RsCodec_set_encode_impl(impl);
    Codec_encode(RsCodec_get(), dst, len);
    return 0;
}

static int test_one_geometry_bit_exact(size_t k, size_t r, unsigned seed)
{
    size_t n = k + r;
    size_t block_bytes = n * PKG_SIZE;
    size_t input_bytes = k * PKG_SIZE;
    unsigned char *source;
    unsigned char *legacy;
    unsigned char *general;
    unsigned char *simd;
    unsigned char *auto_buf;
    size_t pk;
    size_t pr;
    size_t pn;
    size_t shard_bytes;

    EXPECT(RsCodec_set_params(k, r) == 0);
    EXPECT(RsCodec_encode_plan_ready());
    RsCodec_get_encode_plan_geometry(&pk, &pr, &pn, &shard_bytes);
    EXPECT(pk == k && pr == r && pn == n && shard_bytes == PKG_SIZE);

    source = calloc(1, block_bytes);
    legacy = calloc(1, block_bytes);
    general = calloc(1, block_bytes);
    simd = calloc(1, block_bytes);
    auto_buf = calloc(1, block_bytes);
    EXPECT(source && legacy && general && simd && auto_buf);

    fill_prefix(source, input_bytes, seed);
    encode_copy(RS_ENCODE_LEGACY, legacy, source, block_bytes);
    EXPECT(RsCodec_last_encode_impl() == RS_ENCODE_LEGACY);

    encode_copy(RS_ENCODE_GENERAL, general, source, block_bytes);
    EXPECT(RsCodec_last_encode_impl() == RS_ENCODE_GENERAL);
    EXPECT(memcmp(legacy, general, block_bytes) == 0);

    encode_copy(RS_ENCODE_SIMD, simd, source, block_bytes);
    EXPECT(RsCodec_last_encode_impl() ==
           (RsCodec_simd_available() ? RS_ENCODE_SIMD : RS_ENCODE_GENERAL));
    EXPECT(memcmp(legacy, simd, block_bytes) == 0);

    encode_copy(RS_ENCODE_AUTO, auto_buf, source, block_bytes);
    EXPECT(RsCodec_last_encode_impl() ==
           (RsCodec_simd_available() ? RS_ENCODE_SIMD : RS_ENCODE_GENERAL));
    EXPECT(memcmp(legacy, auto_buf, block_bytes) == 0);

    if (k == 4u && r == 2u) {
        unsigned char *rscode = calloc(1, block_bytes);

        EXPECT(rscode);
        encode_copy(RS_ENCODE_RSCODE, rscode, source, block_bytes);
        EXPECT(RsCodec_last_encode_impl() == RS_ENCODE_RSCODE);
        EXPECT(memcmp(general, rscode, block_bytes) == 0);
        free(rscode);
    }

    free(source);
    free(legacy);
    free(general);
    free(simd);
    free(auto_buf);
    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    return 0;
}

static int test_rs_general_table_path_bit_exact(void)
{
    EXPECT(test_one_geometry_bit_exact(4u, 2u, 0x1111u) == 0);
    EXPECT(test_one_geometry_bit_exact(4u, 1u, 0x1212u) == 0);
    EXPECT(test_one_geometry_bit_exact(8u, 2u, 0x2222u) == 0);
    EXPECT(test_one_geometry_bit_exact(16u, 2u, 0x3333u) == 0);
    EXPECT(test_one_geometry_bit_exact(8u, 4u, 0x4444u) == 0);
    EXPECT(test_one_geometry_bit_exact(16u, 6u, 0x5555u) == 0);
    EXPECT(test_one_geometry_bit_exact(32u, 2u, 0x6666u) == 0);
    return 0;
}

/*
 * Strict memcmp: rs_encode_general_table(4+2) vs explicit rscode encode_data
 * via Codec_encode (sender path). Hash/sink is not used as equivalence proof.
 */
static int cmp_general_vs_rscode_block(const unsigned char *source,
                                       size_t block_bytes)
{
    unsigned char *general = calloc(1, block_bytes);
    unsigned char *rscode = calloc(1, block_bytes);
    unsigned char *auto_buf = calloc(1, block_bytes);
    RsEncodeStats stats;

    EXPECT(general && rscode && auto_buf);

    encode_copy(RS_ENCODE_GENERAL, general, source, block_bytes);
    EXPECT(RsCodec_last_encode_impl() == RS_ENCODE_GENERAL);

    encode_copy(RS_ENCODE_RSCODE, rscode, source, block_bytes);
    EXPECT(RsCodec_last_encode_impl() == RS_ENCODE_RSCODE);
    EXPECT(memcmp(general, rscode, block_bytes) == 0);

    /* Sender-facing AUTO path must remain unlocked and match rscode. */
    RsCodec_reset_encode_stats();
    encode_copy(RS_ENCODE_AUTO, auto_buf, source, block_bytes);
    EXPECT(RsCodec_last_encode_impl() ==
           (RsCodec_simd_available() ? RS_ENCODE_SIMD : RS_ENCODE_GENERAL));
    EXPECT(memcmp(auto_buf, rscode, block_bytes) == 0);
    RsCodec_get_encode_stats(&stats);
    EXPECT(stats.lock_wait_ns == 0ull);
    EXPECT(stats.lock_hold_ns == 0ull);
    EXPECT(stats.rscode_path_calls == 0ull);
    EXPECT(stats.general_path_calls + stats.simd_path_calls == 1ull);

    free(general);
    free(rscode);
    free(auto_buf);
    return 0;
}

static int fill_pattern(unsigned char *block, size_t input_bytes, int kind,
                        unsigned seed)
{
    size_t i;

    if (kind == 0) {
        memset(block, 0, input_bytes);
    } else if (kind == 1) {
        memset(block, 0xff, input_bytes);
    } else if (kind == 2) {
        for (i = 0; i < input_bytes; i++) {
            block[i] = (unsigned char)(0xA5u ^ (unsigned char)(i & 0xffu));
        }
    } else if (kind == 3) {
        for (i = 0; i < input_bytes; i++) {
            block[i] = (unsigned char)((i * 17u + seed) & 0xffu);
        }
    } else {
        fill_prefix(block, input_bytes, seed);
    }
    return 0;
}

static int recover_drop_count(const unsigned char *encoded_full,
                              size_t k, size_t r, unsigned drop_count,
                              unsigned seed)
{
    size_t n = k + r;
    size_t block_bytes = n * PKG_SIZE;
    size_t input_bytes = k * PKG_SIZE;
    unsigned char *block = calloc(1, block_bytes);
    uint8_t *bits = calloc(1, codec_present_bytes(n));
    size_t shard;
    unsigned dropped = 0;
    unsigned state = seed;

    EXPECT(block && bits);
    memcpy(block, encoded_full, block_bytes);
    codec_present_clear_all(bits, n);
    for (shard = 0; shard < n; shard++) {
        codec_present_set(bits, shard);
    }
    while (dropped < drop_count) {
        size_t idx;

        state = state * 1103515245u + 12345u;
        idx = (size_t)((state >> 16) % n);
        if (codec_present_get(bits, idx)) {
            bits[idx / 8u] &= (uint8_t)~(1u << (idx % 8u));
            memset(block + idx * PKG_SIZE, 0, PKG_SIZE);
            dropped++;
        }
    }
    EXPECT(Codec_recover(RsCodec_get(), block, bits, n) == CODEC_RECOVER_OK);
    EXPECT(memcmp(block, encoded_full, input_bytes) == 0);
    free(block);
    free(bits);
    return 0;
}

static int test_rs_4_2_general_vs_rscode_bit_exact(void)
{
    const size_t k = 4u;
    const size_t r = 2u;
    const size_t n = 6u;
    const size_t block_bytes = n * PKG_SIZE;
    const size_t input_bytes = k * PKG_SIZE;
    static const unsigned seeds[] = {
        1u, 0x1111u, 0xDeadBeefu, 0xC0FFEEu, 42u, 0xA5A5A5A5u, 7u, 99991u};
    unsigned char *source = calloc(1, block_bytes);
    unsigned char *multi = calloc(8u, block_bytes);
    size_t si;
    int kind;
    unsigned b;

    EXPECT(source && multi);
    EXPECT(RsCodec_set_params(k, r) == 0);

    for (kind = 0; kind <= 4; kind++) {
        for (si = 0; si < sizeof(seeds) / sizeof(seeds[0]); si++) {
            memset(source, 0, block_bytes);
            fill_pattern(source, input_bytes, kind, seeds[si]);
            EXPECT(cmp_general_vs_rscode_block(source, block_bytes) == 0);
        }
    }

    /* Short/final block: zero-padded prefix (pipeline pad model). */
    for (si = 0; si < sizeof(seeds) / sizeof(seeds[0]); si++) {
        size_t valid_lens[] = {1u, 17u, PKG_SIZE - 1u, PKG_SIZE + 3u,
                               3u * PKG_SIZE + 11u, input_bytes - 1u};
        size_t vi;

        for (vi = 0; vi < sizeof(valid_lens) / sizeof(valid_lens[0]); vi++) {
            memset(source, 0, block_bytes);
            fill_prefix(source, valid_lens[vi], seeds[si]);
            EXPECT(cmp_general_vs_rscode_block(source, block_bytes) == 0);
        }
    }

    /* Multi-block sequence via Codec_encode, memcmp each block. */
    for (b = 0; b < 8u; b++) {
        unsigned char *blk = multi + (size_t)b * block_bytes;

        memset(blk, 0, block_bytes);
        fill_prefix(blk, input_bytes, 0x5000u + b * 97u);
        EXPECT(cmp_general_vs_rscode_block(blk, block_bytes) == 0);
    }

    /* Recovery after GENERAL encode: drop 1 and drop 2 shards. */
    {
        unsigned char *encoded = calloc(1, block_bytes);

        EXPECT(encoded);
        memset(source, 0, block_bytes);
        fill_prefix(source, input_bytes, 0x7EC0u);
        encode_copy(RS_ENCODE_GENERAL, encoded, source, block_bytes);
        EXPECT(recover_drop_count(encoded, k, r, 1u, 0xD101u) == 0);
        EXPECT(recover_drop_count(encoded, k, r, 2u, 0xD202u) == 0);

        /* Same recover after AUTO encode (SIMD where available). */
        encode_copy(RS_ENCODE_AUTO, encoded, source, block_bytes);
        EXPECT(RsCodec_last_encode_impl() ==
               (RsCodec_simd_available() ? RS_ENCODE_SIMD
                                         : RS_ENCODE_GENERAL));
        EXPECT(recover_drop_count(encoded, k, r, 1u, 0xD303u) == 0);
        EXPECT(recover_drop_count(encoded, k, r, 2u, 0xD404u) == 0);
        free(encoded);
    }

    free(source);
    free(multi);
    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    return 0;
}

static int recover_roundtrip(size_t k, size_t r, uint64_t drop_mask)
{
    size_t n = k + r;
    size_t block_bytes = n * PKG_SIZE;
    size_t input_bytes = k * PKG_SIZE;
    unsigned char *block = calloc(1, block_bytes);
    unsigned char *source = calloc(1, input_bytes);
    uint8_t *bits = calloc(1, codec_present_bytes(n));
    size_t shard;

    EXPECT(block && source && bits);
    EXPECT(RsCodec_set_params(k, r) == 0);
    fill_prefix(block, input_bytes, 0xA11Au + (unsigned)(k * 17u + r));
    memcpy(source, block, input_bytes);
    RsCodec_set_encode_impl(RS_ENCODE_SIMD);
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

    free(block);
    free(source);
    free(bits);
    return 0;
}

static int test_rs_general_table_path_recovery_compatible(void)
{
    EXPECT(recover_roundtrip(4u, 2u, (1ull << 1) | (1ull << 5)) == 0);
    EXPECT(recover_roundtrip(8u, 2u, (1ull << 2) | (1ull << 9)) == 0);
    EXPECT(recover_roundtrip(16u, 2u, (1ull << 4) | (1ull << 17)) == 0);
    EXPECT(recover_roundtrip(16u, 6u, (1ull << 2) | (1ull << 19) |
                                         (1ull << 21)) == 0);
    EXPECT(recover_roundtrip(8u, 4u, (1ull << 1) | (1ull << 3) | (1ull << 10) |
                                         (1ull << 11)) == 0);
    return 0;
}

static int test_rs_general_table_path_short_block(void)
{
    /* Codec_encode requires full n*PKG_SIZE; short valid payload is modeled as
     * zero-padded data prefix (same as pipeline pad), then encode. */
    size_t k = 8;
    size_t r = 2;
    size_t n = k + r;
    size_t block_bytes = n * PKG_SIZE;
    size_t valid_len = 3u * PKG_SIZE + 17u;
    unsigned char *legacy;
    unsigned char *general;
    unsigned char *source;

    EXPECT(RsCodec_set_params(k, r) == 0);
    source = calloc(1, block_bytes);
    legacy = calloc(1, block_bytes);
    general = calloc(1, block_bytes);
    EXPECT(source && legacy && general);

    fill_prefix(source, valid_len, 0x55AAu);
    encode_copy(RS_ENCODE_LEGACY, legacy, source, block_bytes);
    encode_copy(RS_ENCODE_GENERAL, general, source, block_bytes);
    EXPECT(memcmp(legacy, general, block_bytes) == 0);
    EXPECT(memcmp(general, source, k * PKG_SIZE) == 0);

    free(source);
    free(legacy);
    free(general);
    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    return 0;
}

static int test_rs_simd_shard_byte_boundaries(void)
{
    static const size_t sizes[] = {
        1u, 15u, 16u, 17u, 31u, 32u, 33u, 1300u, 1399u, 1400u, 1401u
    };
    size_t si;

    for (si = 0; si < sizeof(sizes) / sizeof(sizes[0]); si++) {
        const size_t k = 16u;
        const size_t r = 6u;
        size_t block_bytes = (k + r) * sizes[si];
        size_t input_bytes = k * sizes[si];
        unsigned pattern;

        EXPECT(RsCodec_set_params_ex(k, r, sizes[si]) == 0);
        EXPECT(RsCodec_encode_plan_has_nibble_table());
        for (pattern = 0; pattern < 3u; pattern++) {
            unsigned char *source = calloc(1, block_bytes);
            unsigned char *legacy = calloc(1, block_bytes);
            unsigned char *simd = calloc(1, block_bytes);

            EXPECT(source && legacy && simd);
            if (pattern == 1u) {
                memset(source, 0xff, input_bytes);
            } else if (pattern == 2u) {
                fill_prefix(source, input_bytes,
                            0x160600u + (unsigned)si * 31u);
            }
            encode_copy(RS_ENCODE_LEGACY, legacy, source, block_bytes);
            encode_copy(RS_ENCODE_SIMD, simd, source, block_bytes);
            EXPECT(memcmp(legacy, simd, block_bytes) == 0);
            free(source);
            free(legacy);
            free(simd);
        }
    }
    EXPECT(RsCodec_set_params(4u, 2u) == 0);
    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    return 0;
}

static int test_rs_simd_runtime_fallback(void)
{
    const size_t k = 16u;
    const size_t r = 6u;
    size_t block_bytes = (k + r) * PKG_SIZE;
    size_t input_bytes = k * PKG_SIZE;
    unsigned char *source = calloc(1, block_bytes);
    unsigned char *scalar = calloc(1, block_bytes);
    unsigned char *fallback = calloc(1, block_bytes);

    EXPECT(source && scalar && fallback);
    EXPECT(RsCodec_set_params(k, r) == 0);
    fill_prefix(source, input_bytes, 0xFA11BACCu);
    encode_copy(RS_ENCODE_GENERAL, scalar, source, block_bytes);

    RsCodec_set_simd_enabled_for_tests(0);
    encode_copy(RS_ENCODE_SIMD, fallback, source, block_bytes);
    EXPECT(RsCodec_last_encode_impl() == RS_ENCODE_GENERAL);
    EXPECT(memcmp(scalar, fallback, block_bytes) == 0);

    RsCodec_set_simd_enabled_for_tests(1);
    free(source);
    free(scalar);
    free(fallback);
    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    return 0;
}

typedef struct StressArg {
    unsigned char *blocks;
    unsigned char *refs;
    size_t block_bytes;
    unsigned count;
    int failed;
} StressArg;

static void *stress_worker(void *arg)
{
    StressArg *s = arg;
    unsigned i;

    for (i = 0; i < s->count; i++) {
        unsigned char *block = s->blocks + (size_t)i * s->block_bytes;
        const unsigned char *ref = s->refs + (size_t)i * s->block_bytes;

        Codec_encode(RsCodec_get(), block, s->block_bytes);
        if (memcmp(block, ref, s->block_bytes) != 0) {
            s->failed = 1;
            return NULL;
        }
    }
    return NULL;
}

static int test_rs_general_table_path_multiflow_stress(void)
{
    const size_t k = 16;
    const size_t r = 6;
    const size_t n = k + r;
    const size_t block_bytes = n * PKG_SIZE;
    const size_t input_bytes = k * PKG_SIZE;
    const unsigned flows = 8u;
    const unsigned blocks_each = 48u;
    unsigned char *arena;
    unsigned char *refs;
    pthread_t threads[8];
    StressArg args[8];
    unsigned f;

    EXPECT(RsCodec_set_params(k, r) == 0);
    RsCodec_set_encode_impl(RS_ENCODE_SIMD);
    arena = calloc((size_t)flows * blocks_each, block_bytes);
    refs = calloc((size_t)flows * blocks_each, block_bytes);
    EXPECT(arena && refs);

    for (f = 0; f < flows; f++) {
        unsigned b;

        args[f].blocks = arena + (size_t)f * blocks_each * block_bytes;
        args[f].refs = refs + (size_t)f * blocks_each * block_bytes;
        args[f].block_bytes = block_bytes;
        args[f].count = blocks_each;
        args[f].failed = 0;
        for (b = 0; b < blocks_each; b++) {
            unsigned char *block =
                args[f].blocks + (size_t)b * block_bytes;
            unsigned char *ref = args[f].refs + (size_t)b * block_bytes;

            fill_prefix(block, input_bytes, 0x7000u + f * 100u + b);
            encode_copy(RS_ENCODE_LEGACY, ref, block, block_bytes);
        }
    }

    RsCodec_set_encode_impl(RS_ENCODE_SIMD);
    for (f = 0; f < flows; f++) {
        EXPECT(pthread_create(&threads[f], NULL, stress_worker, &args[f]) == 0);
    }
    for (f = 0; f < flows; f++) {
        pthread_join(threads[f], NULL);
        EXPECT(args[f].failed == 0);
    }

    free(arena);
    free(refs);
    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    return 0;
}

static int test_rs_fast_path_fallback(void)
{
    unsigned char *block;
    size_t n;

    EXPECT(RsCodec_set_params(16u, 2u) == 0);
    n = 18u;
    block = calloc(1, n * PKG_SIZE);
    EXPECT(block);
    fill_prefix(block, 16u * PKG_SIZE, 0xABCDu);
    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    Codec_encode(RsCodec_get(), block, n * PKG_SIZE);
    EXPECT(RsCodec_last_encode_impl() ==
           (RsCodec_simd_available() ? RS_ENCODE_SIMD : RS_ENCODE_GENERAL));
    free(block);

    EXPECT(RsCodec_set_params(4u, 2u) == 0);
    n = 6u;
    block = calloc(1, n * PKG_SIZE);
    EXPECT(block);
    fill_prefix(block, 4u * PKG_SIZE, 0x4F2Au);
    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    Codec_encode(RsCodec_get(), block, n * PKG_SIZE);
    EXPECT(RsCodec_last_encode_impl() ==
           (RsCodec_simd_available() ? RS_ENCODE_SIMD : RS_ENCODE_GENERAL));
    free(block);

    EXPECT(RsCodec_set_params(8u, 4u) == 0);
    n = 12u;
    block = calloc(1, n * PKG_SIZE);
    EXPECT(block);
    fill_prefix(block, 8u * PKG_SIZE, 0xBEEFu);
    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    Codec_encode(RsCodec_get(), block, n * PKG_SIZE);
    EXPECT(RsCodec_last_encode_impl() ==
           (RsCodec_simd_available() ? RS_ENCODE_SIMD : RS_ENCODE_GENERAL));
    free(block);

    EXPECT(RsCodec_set_params(16u, 2u) == 0);
    n = 18u;
    block = calloc(1, n * PKG_SIZE);
    EXPECT(block);
    fill_prefix(block, 16u * PKG_SIZE, 0xC0DEu);
    RsCodec_set_encode_impl(RS_ENCODE_GENERAL);
    Codec_encode(RsCodec_get(), block, n * PKG_SIZE);
    EXPECT(RsCodec_last_encode_impl() == RS_ENCODE_GENERAL);
    free(block);

    RsCodec_set_encode_impl(RS_ENCODE_AUTO);
    return 0;
}

static int test_rs_encode_plan_geometry_guard(void)
{
    size_t k;
    size_t r;
    size_t n;
    size_t shard_bytes;

    EXPECT(RsCodec_set_params(16u, 2u) == 0);
    EXPECT(RsCodec_encode_plan_ready());
    EXPECT(RsCodec_encode_plan_has_mul_table());
    EXPECT(RsCodec_encode_plan_has_nibble_table());
    RsCodec_get_encode_plan_geometry(&k, &r, &n, &shard_bytes);
    EXPECT(k == 16u && r == 2u && n == 18u && shard_bytes == PKG_SIZE);

    EXPECT(RsCodec_set_params(8u, 4u) == 0);
    RsCodec_get_encode_plan_geometry(&k, &r, &n, &shard_bytes);
    EXPECT(k == 8u && r == 4u && n == 12u);
    EXPECT(RsCodec_encode_plan_has_mul_table());
    EXPECT(RsCodec_encode_plan_has_nibble_table());

    /* Reject invalid geometry without leaving a matching plan. */
    EXPECT(RsCodec_set_params(0u, 2u) != 0);
    RsCodec_get_encode_plan_geometry(&k, &r, &n, &shard_bytes);
    EXPECT(k == 8u && r == 4u && n == 12u);

    EXPECT(RsCodec_set_params(200u, 100u) != 0); /* n=300 > 255 */

    EXPECT(RsCodec_set_params(4u, 2u) == 0);
    RsCodec_get_encode_plan_geometry(&k, &r, &n, &shard_bytes);
    EXPECT(k == 4u && r == 2u && n == 6u);
    return 0;
}

int main(void)
{
    if (test_rs_general_table_path_bit_exact() != 0) {
        fputs("test_rs_general_table_path_bit_exact failed\n", stderr);
        return 1;
    }
    if (test_rs_4_2_general_vs_rscode_bit_exact() != 0) {
        fputs("test_rs_4_2_general_vs_rscode_bit_exact failed\n", stderr);
        return 1;
    }
    if (test_rs_general_table_path_recovery_compatible() != 0) {
        fputs("test_rs_general_table_path_recovery_compatible failed\n",
              stderr);
        return 1;
    }
    if (test_rs_general_table_path_short_block() != 0) {
        fputs("test_rs_general_table_path_short_block failed\n", stderr);
        return 1;
    }
    if (test_rs_simd_shard_byte_boundaries() != 0) {
        fputs("test_rs_simd_shard_byte_boundaries failed\n", stderr);
        return 1;
    }
    if (test_rs_simd_runtime_fallback() != 0) {
        fputs("test_rs_simd_runtime_fallback failed\n", stderr);
        return 1;
    }
    if (test_rs_general_table_path_multiflow_stress() != 0) {
        fputs("test_rs_general_table_path_multiflow_stress failed\n", stderr);
        return 1;
    }
    if (test_rs_fast_path_fallback() != 0) {
        fputs("test_rs_fast_path_fallback failed\n", stderr);
        return 1;
    }
    if (test_rs_encode_plan_geometry_guard() != 0) {
        fputs("test_rs_encode_plan_geometry_guard failed\n", stderr);
        return 1;
    }
    (void)checksum_bytes;
    puts("rs encode general table-path tests passed");
    return 0;
}
