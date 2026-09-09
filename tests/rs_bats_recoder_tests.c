#include "generation_cache.h"
#include "rs_bats_recoder.h"
#include "stream_config.h"
#include "wire_header.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_failures = 0;

#define EXPECT(cond)                                                           \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);   \
            g_failures++;                                                      \
        }                                                                      \
    } while (0)

static size_t make_rs_datagram(uint8_t *out, size_t cap, uint32_t flow_id,
                               uint64_t block_id, uint16_t shard_index,
                               uint16_t shard_count, uint8_t marker)
{
    WireHeader hdr;
    size_t len = WIRE_HEADER_SIZE + PKG_SIZE;

    if (cap < len) {
        return 0;
    }
    memset(&hdr, 0, sizeof(hdr));
    hdr.type = WIRE_TYPE_DATA;
    hdr.final_dst = 4;
    hdr.ttl = 7;
    hdr.flow_id = flow_id;
    hdr.block_id = block_id;
    hdr.shard_index = shard_index;
    hdr.shard_count = shard_count;
    hdr.valid_len = PKG_SIZE;
    hdr.payload_len = PKG_SIZE;
    wire_header_encode(out, &hdr);
    memset(out + WIRE_HEADER_SIZE, marker, PKG_SIZE);
    out[WIRE_HEADER_SIZE] = (uint8_t)(marker + shard_index);
    return len;
}

static void test_identity_roundtrip_rows(void)
{
    RsBatsRecoderConfig cfg;
    uint8_t in0[PKG_SIZE];
    uint8_t in1[PKG_SIZE];
    uint8_t in2[PKG_SIZE];
    uint8_t in3[PKG_SIZE];
    uint8_t in4[PKG_SIZE];
    uint8_t in5[PKG_SIZE];
    uint8_t out0[PKG_SIZE];
    uint8_t out1[PKG_SIZE];
    uint8_t out2[PKG_SIZE];
    uint8_t out3[PKG_SIZE];
    uint8_t out4[PKG_SIZE];
    uint8_t out5[PKG_SIZE];
    uint8_t *in_ptrs[] = {in0, in1, in2, in3, in4, in5};
    const uint8_t *in_rows[] = {
        in0, in1, in2, in3, in4, in5,
    };
    uint8_t *out_rows[] = {out0, out1, out2, out3, out4, out5};
    uint16_t shard;

    rs_bats_recoder_config_defaults(&cfg, 6);
    for (shard = 0; shard < 6; shard++) {
        memset(in_ptrs[shard], (uint8_t)(0xA0u + shard), PKG_SIZE);
        memset(out_rows[shard], 0, PKG_SIZE);
    }

    EXPECT(rs_bats_recode_identity(&cfg, in_rows, out_rows) == 0);
    for (shard = 0; shard < 6; shard++) {
        EXPECT(memcmp(in_rows[shard], out_rows[shard], PKG_SIZE) == 0);
    }
}

static void test_generation_cache_identity(void)
{
    GenerationCache cache;
    GenerationCacheConfig gcfg;
    GenerationEntry *entry = NULL;
    uint8_t buf[WIRE_HEADER_SIZE + PKG_SIZE];
    size_t len;
    RsBatsRecoderConfig cfg;
    RsBatsRecoderStats stats;
    uint8_t out0[PKG_SIZE];
    uint8_t out1[PKG_SIZE];
    uint8_t out2[PKG_SIZE];
    uint8_t out3[PKG_SIZE];
    uint8_t out4[PKG_SIZE];
    uint8_t out5[PKG_SIZE];
    uint8_t *out_rows[] = {out0, out1, out2, out3, out4, out5};
    uint16_t shard;
    uint64_t now = 1000000000ull;

    generation_cache_config_defaults(&gcfg);
    EXPECT(generation_cache_init(&cache, &gcfg) == 0);

    for (shard = 0; shard < 6; shard++) {
        len = make_rs_datagram(buf, sizeof(buf), 1, 42, shard, 6,
                               (uint8_t)(0x10u + shard));
        EXPECT(len > 0);
        EXPECT(generation_cache_insert(&cache, NULL, buf, len, now, NULL) ==
               GEN_INSERT_INVALID);
        WireHeader hdr;
        EXPECT(wire_header_decode(&hdr, buf, len) == 0);
        EXPECT(generation_cache_insert(&cache, &hdr, buf, len, now,
                                       shard == 5 ? &entry : NULL) ==
               GEN_INSERT_OK);
        now += 1000ull;
    }

    EXPECT(entry != NULL);
    EXPECT(entry->state == GEN_READY);
    rs_bats_recoder_config_defaults(&cfg, entry->shard_count);
    cfg.payload_len = entry->payload_len;
    memset(&stats, 0, sizeof(stats));
    EXPECT(rs_bats_recode_generation_identity(&cfg, entry, out_rows,
                                               &stats) == 0);
    EXPECT(stats.generations_recoded == 1);
    EXPECT(stats.recode_mismatch == 0);

    generation_cache_destroy(&cache);
}

/* n=4, payload=10, block=4 → last block 4x2 padded to 4x4 then I×B. */
static void test_block_matmul_with_padding(void)
{
    enum { N = 4, PAYLOAD = 10, BLOCK = 4 };
    RsBatsRecoderConfig cfg;
    uint8_t in[N][PAYLOAD];
    uint8_t out[N][PAYLOAD];
    const uint8_t *in_rows[N];
    uint8_t *out_rows[N];
    uint16_t r;
    uint16_t c;

    cfg.shard_count = N;
    cfg.payload_len = PAYLOAD;
    cfg.block_dim = BLOCK;
    EXPECT(rs_bats_recoder_block_dim(&cfg) == BLOCK);

    for (r = 0; r < N; r++) {
        for (c = 0; c < PAYLOAD; c++) {
            in[r][c] = (uint8_t)(0x10u * r + c);
        }
        in_rows[r] = in[r];
        out_rows[r] = out[r];
        memset(out[r], 0xFF, PAYLOAD);
    }

    EXPECT(rs_bats_recode_identity(&cfg, in_rows, out_rows) == 0);
    for (r = 0; r < N; r++) {
        EXPECT(memcmp(in[r], out[r], PAYLOAD) == 0);
    }
}

/* Advisor-style: 16 packets × 1024 bytes, square 16×16 blocks (1024%16==0). */
static void test_advisor_16x16_blocks(void)
{
    enum { N = 16, PAYLOAD = 1024 };
    RsBatsRecoderConfig cfg;
    uint8_t *in_blob;
    uint8_t *out_blob;
    uint8_t *in_w[N];
    const uint8_t *in_rows[N];
    uint8_t *out_rows[N];
    uint16_t r;
    uint16_t c;

    in_blob = malloc((size_t)N * PAYLOAD);
    out_blob = malloc((size_t)N * PAYLOAD);
    EXPECT(in_blob != NULL && out_blob != NULL);
    if (in_blob == NULL || out_blob == NULL) {
        free(in_blob);
        free(out_blob);
        return;
    }

    rs_bats_recoder_config_defaults(&cfg, N);
    cfg.payload_len = PAYLOAD;
    EXPECT(rs_bats_recoder_block_dim(&cfg) == N);

    for (r = 0; r < N; r++) {
        in_w[r] = in_blob + (size_t)r * PAYLOAD;
        out_rows[r] = out_blob + (size_t)r * PAYLOAD;
        in_rows[r] = in_w[r];
        for (c = 0; c < PAYLOAD; c++) {
            in_w[r][c] = (uint8_t)(r * 3u + c * 5u);
        }
    }

    EXPECT(rs_bats_recode_identity(&cfg, in_rows, out_rows) == 0);
    for (r = 0; r < N; r++) {
        EXPECT(memcmp(in_rows[r], out_rows[r], PAYLOAD) == 0);
    }

    free(in_blob);
    free(out_blob);
}

int main(void)
{
    test_identity_roundtrip_rows();
    test_generation_cache_identity();
    test_block_matmul_with_padding();
    test_advisor_16x16_blocks();

    if (g_failures != 0) {
        fprintf(stderr, "%d test failures\n", g_failures);
        return EXIT_FAILURE;
    }
    puts("rs_bats_recoder_tests: ok");
    return EXIT_SUCCESS;
}
