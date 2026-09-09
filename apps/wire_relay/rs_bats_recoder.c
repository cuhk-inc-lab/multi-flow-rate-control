#include "rs_bats_recoder.h"

#include "stream_config.h"
#include "wire_header.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>

#define RS_BATS_GF_POLY 0x11du

static uint8_t gf_mul_table[256][256];
static int gf_mul_ready;

static uint64_t monotonic_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint8_t gf_mul_slow(uint8_t left, uint8_t right)
{
    uint16_t product = 0;
    int bit;

    for (bit = 0; bit < 8; bit++) {
        if (right & 1u) {
            product ^= left;
        }
        right >>= 1u;
        if (left & 0x80u) {
            left = (uint8_t)((left << 1u) ^ RS_BATS_GF_POLY);
        } else {
            left <<= 1u;
        }
    }
    return (uint8_t)product;
}

static void gf_mul_table_init(void)
{
    unsigned left;
    unsigned right;

    if (gf_mul_ready) {
        return;
    }
    for (left = 0; left < 256u; left++) {
        for (right = 0; right < 256u; right++) {
            gf_mul_table[left][right] = gf_mul_slow((uint8_t)left, (uint8_t)right);
        }
    }
    gf_mul_ready = 1;
}

static uint8_t gf_mul(uint8_t left, uint8_t right)
{
    return gf_mul_table[left][right];
}

static int config_valid(const RsBatsRecoderConfig *cfg)
{
    if (cfg == NULL || cfg->shard_count < 2u || cfg->payload_len < 1u) {
        return 0;
    }
    if (cfg->shard_count > GEN_CACHE_MAX_SHARD_COUNT) {
        return 0;
    }
    if (cfg->block_dim != 0 && cfg->block_dim > GEN_CACHE_MAX_SHARD_COUNT) {
        return 0;
    }
    return 1;
}

uint16_t rs_bats_recoder_block_dim(const RsBatsRecoderConfig *cfg)
{
    if (cfg == NULL || cfg->shard_count < 2u) {
        return 0;
    }
    if (cfg->block_dim == 0) {
        return cfg->shard_count;
    }
    return cfg->block_dim;
}

void rs_bats_recoder_config_defaults(RsBatsRecoderConfig *cfg,
                                     uint16_t shard_count)
{
    if (cfg == NULL) {
        return;
    }
    cfg->shard_count = shard_count;
    cfg->payload_len = (uint16_t)PKG_SIZE;
    /* Square n x n blocks (16x16 when n=16). */
    cfg->block_dim = RS_BATS_DEFAULT_BLOCK_DIM;
}

/*
 * C = R × B over GF(256).
 * R: n x n row-major; B, C: n x w row-major (w = block_dim, may include pad).
 */
static void mat_mul_block(uint16_t n, uint16_t w, const uint8_t *R,
                          const uint8_t *B, uint8_t *C)
{
    uint16_t i;
    uint16_t j;
    uint16_t k;

    for (i = 0; i < n; i++) {
        for (j = 0; j < w; j++) {
            uint8_t acc = 0;

            for (k = 0; k < n; k++) {
                acc ^= gf_mul(R[(size_t)i * n + k], B[(size_t)k * w + j]);
            }
            C[(size_t)i * w + j] = acc;
        }
    }
}

int rs_bats_recode(const RsBatsRecoderConfig *cfg, const uint8_t *matrix,
                   const uint8_t *const *in_rows, uint8_t *const *out_rows)
{
    size_t col_start;
    uint16_t n;
    uint16_t block_dim;
    uint16_t payload_len;
    uint8_t *B_pad = NULL;
    uint8_t *C_pad = NULL;
    size_t pad_bytes;

    if (!config_valid(cfg) || matrix == NULL || in_rows == NULL ||
        out_rows == NULL) {
        return -1;
    }

    n = cfg->shard_count;
    block_dim = rs_bats_recoder_block_dim(cfg);
    payload_len = cfg->payload_len;
    if (block_dim < 1u) {
        return -1;
    }

    for (col_start = 0; col_start < n; col_start++) {
        if (in_rows[col_start] == NULL || out_rows[col_start] == NULL) {
            return -1;
        }
    }

    gf_mul_table_init();

    pad_bytes = (size_t)n * (size_t)block_dim;
    B_pad = malloc(pad_bytes);
    C_pad = malloc(pad_bytes);
    if (B_pad == NULL || C_pad == NULL) {
        free(B_pad);
        free(C_pad);
        return -1;
    }

    for (col_start = 0; col_start < payload_len; col_start += block_dim) {
        uint16_t valid_cols = block_dim;
        uint16_t row;

        if (col_start + valid_cols > payload_len) {
            valid_cols = (uint16_t)(payload_len - col_start);
        }

        /* Build n x block_dim block; zero-pad missing right columns. */
        memset(B_pad, 0, pad_bytes);
        for (row = 0; row < n; row++) {
            memcpy(B_pad + (size_t)row * block_dim,
                   in_rows[row] + col_start, valid_cols);
        }

        /* C_pad = R × B_pad  (matrix × matrix) */
        mat_mul_block(n, block_dim, matrix, B_pad, C_pad);

        /* Write back only real columns (drop padding). */
        for (row = 0; row < n; row++) {
            memcpy(out_rows[row] + col_start,
                   C_pad + (size_t)row * block_dim, valid_cols);
        }
    }

    free(B_pad);
    free(C_pad);
    return 0;
}

int rs_bats_recode_identity(const RsBatsRecoderConfig *cfg,
                            const uint8_t *const *in_rows,
                            uint8_t *const *out_rows)
{
    uint8_t identity[GEN_CACHE_MAX_SHARD_COUNT * GEN_CACHE_MAX_SHARD_COUNT];
    uint16_t row;

    if (!config_valid(cfg)) {
        return -1;
    }
    memset(identity, 0, sizeof(identity));
    for (row = 0; row < cfg->shard_count; row++) {
        identity[(size_t)row * cfg->shard_count + row] = 1u;
    }
    return rs_bats_recode(cfg, identity, in_rows, out_rows);
}

int rs_bats_generation_payload_rows(const GenerationEntry *gen,
                                    const uint8_t **out_rows)
{
    uint16_t shard;

    if (gen == NULL || out_rows == NULL || gen->slots == NULL ||
        gen->state != GEN_READY) {
        return -1;
    }
    if (gen->present_count < gen->shard_count) {
        return -1;
    }

    for (shard = 0; shard < gen->shard_count; shard++) {
        const GenerationSlot *slot = &gen->slots[shard];
        const uint8_t *datagram;

        if (!slot->present || slot->datagram_copy == NULL ||
            slot->len < WIRE_HEADER_SIZE + gen->payload_len) {
            return -1;
        }
        datagram = slot->datagram_copy;
        out_rows[shard] = datagram + WIRE_HEADER_SIZE;
    }
    return 0;
}

int rs_bats_recode_generation_identity(const RsBatsRecoderConfig *cfg,
                                       const GenerationEntry *gen,
                                       uint8_t *const *out_rows,
                                       RsBatsRecoderStats *stats)
{
    const uint8_t *in_rows[GEN_CACHE_MAX_SHARD_COUNT];
    uint64_t t0;
    uint64_t t1;
    uint16_t shard;
    int rc;

    if (!config_valid(cfg) || gen == NULL || out_rows == NULL) {
        return -1;
    }
    if (gen->shard_count != cfg->shard_count ||
        gen->payload_len != cfg->payload_len) {
        return -1;
    }
    if (rs_bats_generation_payload_rows(gen, in_rows) != 0) {
        return -1;
    }

    t0 = monotonic_ns();
    rc = rs_bats_recode_identity(cfg, in_rows, out_rows);
    t1 = monotonic_ns();

    if (rc != 0) {
        return rc;
    }

    for (shard = 0; shard < cfg->shard_count; shard++) {
        if (memcmp(in_rows[shard], out_rows[shard], cfg->payload_len) != 0) {
            if (stats != NULL) {
                stats->recode_mismatch++;
            }
            return -1;
        }
    }

    if (stats != NULL) {
        stats->generations_recoded++;
        stats->recode_body_ns += (t1 - t0);
    }
    return 0;
}
