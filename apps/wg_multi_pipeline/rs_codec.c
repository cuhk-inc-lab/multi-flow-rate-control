#include "rs_codec.h"
#include "rs_gf256_simd.h"

#include "stream_config.h"

#include "ecc.h"

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/*
 * Column-wise systematic RS(n,k). Default RS(6,4) extracts parity coefficients
 * from hqm/rscode encode_data (unit vectors) into an immutable RsEncodePlan so
 * scalar and SIMD plan encoders stay bit-exact with historical rscode wire
 * bytes. Other (k,r) use a Cauchy generator + the same immutable plan.
 *
 * Threading contract: finish RsCodec_set_params() before starting encode
 * workers. set_params must not run concurrently with encode. The published
 * encode plan is immutable for the lifetime of a geometry.
 * AUTO encode hot paths do not hold rs_lock; RS_ENCODE_RSCODE is the only
 * encode path that still locks (explicit reference).
 */

typedef struct RsGeometry {
    size_t k;
    size_t r;
    size_t n;
    size_t shard_bytes;
} RsGeometry;

/*
 * Immutable encode plan published under rs_lock by set_params/rs_init.
 * Encode hot paths read this without holding rs_lock.
 */
typedef struct RsEncodePlan {
    size_t k;
    size_t r;
    size_t n;
    size_t shard_bytes;
    /* Full generator copy (systematic identity + parity rows). */
    unsigned char coeff[RS_ABS_MAX_SHARDS][RS_ABS_MAX_SHARDS];
    /*
     * Optional expanded table: mul_table[((j * k) + i) * 256 + x]
     * == gmult(coeff[k+j][i], x). NULL when r*k*256 exceeds the cap.
     */
    uint8_t *mul_table;
    /*
     * SIMD table: 32 bytes per coefficient. First 16 entries multiply low
     * nibbles; the next 16 multiply high nibbles shifted by four bits.
     */
    uint8_t *nibble_table;
    int has_mul_table;
    int has_nibble_table;
    int initialized;
} RsEncodePlan;

static pthread_once_t  rs_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t rs_lock = PTHREAD_MUTEX_INITIALIZER;
static unsigned char   rs_generator[RS_ABS_MAX_SHARDS][RS_ABS_MAX_SHARDS];
static unsigned char   rs_gf_mul_table[256][256];
static int             rs_mul_table_ready;
static int             rs_matrix_ready;
static uint64_t        rs_matrix_init_ns;
static RsGeometry      rs_geo = {
    .k = 4u, .r = 2u, .n = 6u, .shard_bytes = PKG_SIZE
};
static RsEncodePlan    rs_encode_plan;
static RsEncodeImpl    rs_encode_impl = RS_ENCODE_AUTO;
static RsEncodeImpl    rs_last_encode_impl = RS_ENCODE_GENERAL;
static RsEncodeStats   rs_encode_stats;
static int             rs_simd_enabled = 1;

/* CLI RS(16,2): k=16 data, r=2 parity, n=18. */
#define RS_FAST_K 16u
#define RS_FAST_R 2u
#define RS_FAST_N (RS_FAST_K + RS_FAST_R)

static uint64_t monotonic_nanoseconds(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static unsigned char gf_mul(unsigned char left, unsigned char right)
{
    return (unsigned char)gmult((int)left, (int)right);
}

static void build_gf_mul_table(void)
{
    unsigned left;
    unsigned right;

    for (left = 0; left < 256u; left++) {
        for (right = 0; right < 256u; right++) {
            rs_gf_mul_table[left][right] =
                (unsigned char)gmult((int)left, (int)right);
        }
    }
    rs_mul_table_ready = 1;
}

static unsigned char gf_inv(unsigned char value)
{
    return (unsigned char)ginv((int)value);
}

static int params_valid(size_t k, size_t r, size_t shard_bytes)
{
    size_t n;

    if (k < 1u || r < 1u || shard_bytes < 1u || shard_bytes > UINT16_MAX) {
        return 0;
    }
    n = k + r;
    /* Working size is exactly (k,r). Ceiling: GF(256) RS codeword n <= 255. */
    if (n > RS_ABS_MAX_SHARDS) {
        return 0;
    }
    if (n * shard_bytes > CODEC_MAX_ENCODE_BLOCK) {
        return 0;
    }
    return 1;
}

static int matrix_inverse_dyn(const unsigned char *input, unsigned char *output,
                              size_t k)
{
    unsigned char augmented[RS_ABS_MAX_SHARDS][RS_ABS_MAX_SHARDS * 2u];
    size_t row;
    size_t column;

    if (k == 0 || k >= RS_ABS_MAX_SHARDS) {
        return -1;
    }

    for (row = 0; row < k; row++) {
        for (column = 0; column < k; column++) {
            augmented[row][column] = input[row * RS_ABS_MAX_SHARDS + column];
            augmented[row][k + column] = row == column ? 1u : 0u;
        }
    }

    for (column = 0; column < k; column++) {
        size_t pivot = column;
        size_t other;

        while (pivot < k && augmented[pivot][column] == 0) {
            pivot++;
        }
        if (pivot == k) {
            return -1;
        }
        if (pivot != column) {
            unsigned char tmp[RS_ABS_MAX_SHARDS * 2u];

            memcpy(tmp, augmented[pivot], k * 2u);
            memcpy(augmented[pivot], augmented[column], k * 2u);
            memcpy(augmented[column], tmp, k * 2u);
        }

        {
            unsigned char inverse = gf_inv(augmented[column][column]);

            for (other = 0; other < k * 2u; other++) {
                augmented[column][other] =
                    gf_mul(augmented[column][other], inverse);
            }
        }

        for (row = 0; row < k; row++) {
            unsigned char factor;

            if (row == column) {
                continue;
            }
            factor = augmented[row][column];
            if (factor == 0) {
                continue;
            }
            for (other = 0; other < k * 2u; other++) {
                augmented[row][other] ^=
                    gf_mul(factor, augmented[column][other]);
            }
        }
    }

    for (row = 0; row < k; row++) {
        for (column = 0; column < k; column++) {
            output[row * RS_ABS_MAX_SHARDS + column] =
                augmented[row][k + column];
        }
    }
    return 0;
}

static void fill_identity_data_rows(size_t k)
{
    size_t row;
    size_t column;

    memset(rs_generator, 0, sizeof(rs_generator));
    for (row = 0; row < k; row++) {
        for (column = 0; column < k; column++) {
            rs_generator[row][column] = row == column ? 1u : 0u;
        }
    }
}

static int build_generator_rscode_4_2(void)
{
    size_t column;
    size_t row;

    fill_identity_data_rows(4u);
    for (column = 0; column < 4u; column++) {
        unsigned char message[4] = {0};
        unsigned char codeword[6];

        message[column] = 1u;
        encode_data(message, 4, codeword);
        for (row = 4; row < 6u; row++) {
            rs_generator[row][column] = codeword[row];
        }
    }
    return 0;
}

static int build_generator_cauchy(size_t k, size_t r)
{
    size_t row;
    size_t column;

    fill_identity_data_rows(k);
    for (row = 0; row < r; row++) {
        unsigned char x = (unsigned char)(row + 1u);

        for (column = 0; column < k; column++) {
            unsigned char y = (unsigned char)(r + 1u + column);
            unsigned char den = (unsigned char)(x ^ y);

            if (den == 0) {
                return -1;
            }
            rs_generator[k + row][column] = gf_inv(den);
        }
    }
    return 0;
}

static int rebuild_generator(void)
{
    rs_encode_stats.rebuild_calls++;
    if (rs_geo.k == 4u && rs_geo.r == 2u) {
        return build_generator_rscode_4_2();
    }
    return build_generator_cauchy(rs_geo.k, rs_geo.r);
}

static void rs_encode_plan_clear(RsEncodePlan *plan)
{
    if (plan == NULL) {
        return;
    }
    free(plan->mul_table);
    free(plan->nibble_table);
    memset(plan, 0, sizeof(*plan));
}

static int rs_encode_mul_table_bytes(size_t k, size_t r, size_t *out_bytes)
{
    if (k == 0 || r == 0) {
        return -1;
    }
    if (k > SIZE_MAX / r) {
        return -1;
    }
    if (k * r > SIZE_MAX / 256u) {
        return -1;
    }
    *out_bytes = k * r * 256u;
    return 0;
}

/*
 * Build immutable encode plan from the current rs_generator.
 * On failure leaves *plan cleared (no partial publish).
 */
static int rs_encode_plan_build(RsEncodePlan *plan, size_t k, size_t r)
{
    size_t n = k + r;
    size_t table_bytes = 0;
    size_t nibble_bytes;
    size_t row;
    size_t column;
    size_t x;
    uint8_t *table = NULL;

    rs_encode_plan_clear(plan);
    if (!params_valid(k, r, rs_geo.shard_bytes) || !rs_mul_table_ready) {
        return -1;
    }

    plan->k = k;
    plan->r = r;
    plan->n = n;
    plan->shard_bytes = rs_geo.shard_bytes;
    memcpy(plan->coeff, rs_generator, sizeof(plan->coeff));

    if (rs_encode_mul_table_bytes(k, r, &table_bytes) != 0) {
        rs_encode_plan_clear(plan);
        return -1;
    }
    nibble_bytes = table_bytes / 8u;
    plan->nibble_table = malloc(nibble_bytes);
    if (plan->nibble_table == NULL) {
        rs_encode_plan_clear(plan);
        return -1;
    }

    if (table_bytes <= RS_ENCODE_MUL_TABLE_MAX_BYTES) {
        table = malloc(table_bytes);
        if (table == NULL) {
            rs_encode_plan_clear(plan);
            return -1;
        }
        for (row = 0; row < r; row++) {
            for (column = 0; column < k; column++) {
                unsigned char coeff = plan->coeff[k + row][column];
                uint8_t *slot = table + ((row * k + column) * 256u);

                for (x = 0; x < 256u; x++) {
                    slot[x] = rs_gf_mul_table[coeff][x];
                }
            }
        }
        plan->mul_table = table;
        plan->has_mul_table = 1;
    } else {
        /* Cap exceeded: still a valid plan; encode uses shared 256×256 table. */
        plan->mul_table = NULL;
        plan->has_mul_table = 0;
    }

    for (row = 0; row < r; row++) {
        for (column = 0; column < k; column++) {
            unsigned char coeff = plan->coeff[k + row][column];
            uint8_t *slot =
                plan->nibble_table + ((row * k + column) * 32u);

            for (x = 0; x < 16u; x++) {
                slot[x] = rs_gf_mul_table[coeff][x];
                slot[16u + x] = rs_gf_mul_table[coeff][x << 4u];
            }
        }
    }
    plan->has_nibble_table = 1;
    plan->initialized = 1;
    return 0;
}

static void rs_init(void)
{
    uint64_t started = monotonic_nanoseconds();

    initialize_ecc();
    build_gf_mul_table();
    rs_geo.k = 4u;
    rs_geo.r = 2u;
    rs_geo.n = 6u;
    rs_geo.shard_bytes = PKG_SIZE;
    rs_matrix_ready = 0;
    rs_encode_plan_clear(&rs_encode_plan);
    if (rebuild_generator() == 0 &&
        rs_encode_plan_build(&rs_encode_plan, rs_geo.k, rs_geo.r) == 0) {
        rs_matrix_ready = 1;
    } else {
        rs_encode_plan_clear(&rs_encode_plan);
        rs_matrix_ready = 0;
    }
    if (started != 0) {
        uint64_t finished = monotonic_nanoseconds();

        rs_matrix_init_ns = finished >= started ? finished - started : 0;
    }
}

static int rs_ready(void)
{
    return pthread_once(&rs_once, rs_init) == 0;
}

int RsCodec_prepare_matrix(void)
{
    return rs_ready() && rs_matrix_ready ? 0 : -1;
}

int RsCodec_matrix_ready(void)
{
    return RsCodec_prepare_matrix() == 0;
}

uint64_t RsCodec_matrix_init_ns(void)
{
    (void)RsCodec_prepare_matrix();
    return rs_matrix_init_ns;
}

void RsCodec_set_encode_impl(RsEncodeImpl impl)
{
    if (impl == RS_ENCODE_AUTO || impl == RS_ENCODE_GENERAL ||
        impl == RS_ENCODE_SIMD ||
        impl == RS_ENCODE_FAST_16_2 || impl == RS_ENCODE_LEGACY ||
        impl == RS_ENCODE_RSCODE) {
        rs_encode_impl = impl;
    }
}

RsEncodeImpl RsCodec_get_encode_impl(void)
{
    return rs_encode_impl;
}

RsEncodeImpl RsCodec_last_encode_impl(void)
{
    return rs_last_encode_impl;
}

void RsCodec_reset_encode_stats(void)
{
    memset(&rs_encode_stats, 0, sizeof(rs_encode_stats));
}

void RsCodec_get_encode_stats(RsEncodeStats *out)
{
    if (out != NULL) {
        *out = rs_encode_stats;
    }
}

uint64_t RsCodec_calibrate_gmul_ns(uint64_t iters)
{
    uint64_t started;
    uint64_t finished;
    uint64_t i;
    unsigned char acc = 1;
    unsigned char left = 3;
    unsigned char right = 7;

    if (!rs_ready()) {
        return 0;
    }
    started = monotonic_nanoseconds();
    for (i = 0; i < iters; i++) {
        acc ^= gf_mul(left, right);
        left = (unsigned char)(left + 1u);
        right = (unsigned char)(right + 3u);
    }
    finished = monotonic_nanoseconds();
    if (acc == 0xffu) {
        finished ^= 1u;
    }
    return finished >= started ? finished - started : 0;
}

int RsCodec_encode_plan_ready(void)
{
    return rs_ready() && rs_encode_plan.initialized;
}

int RsCodec_encode_plan_has_mul_table(void)
{
    return rs_encode_plan.initialized && rs_encode_plan.has_mul_table;
}

int RsCodec_encode_plan_has_nibble_table(void)
{
    return rs_encode_plan.initialized && rs_encode_plan.has_nibble_table;
}

int RsCodec_avx2_available(void)
{
    return rs_gf256_avx2_available();
}

int RsCodec_ssse3_available(void)
{
    return rs_gf256_ssse3_available();
}

int RsCodec_simd_available(void)
{
    return rs_gf256_simd_available();
}

void RsCodec_set_simd_enabled_for_tests(int enabled)
{
    rs_simd_enabled = enabled != 0;
}

void RsCodec_get_encode_plan_geometry(size_t *k, size_t *r, size_t *n,
                                      size_t *shard_bytes)
{
    (void)rs_ready();
    if (k != NULL) {
        *k = rs_encode_plan.k;
    }
    if (r != NULL) {
        *r = rs_encode_plan.r;
    }
    if (n != NULL) {
        *n = rs_encode_plan.n;
    }
    if (shard_bytes != NULL) {
        *shard_bytes = rs_encode_plan.shard_bytes;
    }
}

int RsCodec_set_params_ex(size_t data_shards, size_t parity_shards,
                          size_t shard_bytes)
{
    RsEncodePlan new_plan;
    int ok;

    if (shard_bytes == 0u) {
        shard_bytes = PKG_SIZE;
    }
    if (!params_valid(data_shards, parity_shards, shard_bytes) || !rs_ready()) {
        return -1;
    }

    memset(&new_plan, 0, sizeof(new_plan));

    pthread_mutex_lock(&rs_lock);
    if (rs_geo.k == data_shards && rs_geo.r == parity_shards &&
        rs_geo.shard_bytes == shard_bytes &&
        rs_matrix_ready && rs_encode_plan.initialized) {
        pthread_mutex_unlock(&rs_lock);
        return 0;
    }

    rs_geo.k = data_shards;
    rs_geo.r = parity_shards;
    rs_geo.n = data_shards + parity_shards;
    rs_geo.shard_bytes = shard_bytes;
    ok = rebuild_generator() == 0 &&
         rs_encode_plan_build(&new_plan, data_shards, parity_shards) == 0;
    if (!ok) {
        rs_encode_plan_clear(&new_plan);
        rs_encode_plan_clear(&rs_encode_plan);
        rs_matrix_ready = 0;
        pthread_mutex_unlock(&rs_lock);
        return -1;
    }

    rs_encode_plan_clear(&rs_encode_plan);
    rs_encode_plan = new_plan;
    /* Ownership of mul_table transferred; avoid double-free of new_plan. */
    memset(&new_plan, 0, sizeof(new_plan));
    rs_matrix_ready = 1;
    pthread_mutex_unlock(&rs_lock);
    return 0;
}

int RsCodec_set_params(size_t data_shards, size_t parity_shards)
{
    return RsCodec_set_params_ex(data_shards, parity_shards, PKG_SIZE);
}

void RsCodec_get_params(size_t *data_shards, size_t *parity_shards)
{
    (void)rs_ready();
    if (data_shards != NULL) {
        *data_shards = rs_geo.k;
    }
    if (parity_shards != NULL) {
        *parity_shards = rs_geo.r;
    }
}

int RsCodec_set_profile(RsProfile profile)
{
    switch (profile) {
    case RS_PROFILE_4_1:
        return RsCodec_set_params(4u, 1u);
    case RS_PROFILE_4_2:
        return RsCodec_set_params(4u, 2u);
    case RS_PROFILE_4_3:
        return RsCodec_set_params(4u, 3u);
    default:
        return -1;
    }
}

RsProfile RsCodec_get_profile(void)
{
    (void)rs_ready();
    if (rs_geo.k == 4u && rs_geo.r == 1u) {
        return RS_PROFILE_4_1;
    }
    if (rs_geo.k == 4u && rs_geo.r == 3u) {
        return RS_PROFILE_4_3;
    }
    return RS_PROFILE_4_2;
}

int RsCodec_params_is_wire_shard_count(uint16_t shard_count)
{
    (void)rs_ready();
    if (shard_count <= rs_geo.k) {
        return 0;
    }
    return params_valid(rs_geo.k, (size_t)shard_count - rs_geo.k,
                        rs_geo.shard_bytes);
}

int RsCodec_set_params_from_shard_count(uint16_t shard_count)
{
    if (!RsCodec_params_is_wire_shard_count(shard_count)) {
        return -1;
    }
    return RsCodec_set_params(rs_geo.k, (size_t)shard_count - rs_geo.k);
}

int RsCodec_set_profile_from_shard_count(uint16_t shard_count)
{
    return RsCodec_set_params_from_shard_count(shard_count);
}

int RsCodec_profile_is_wire_shard_count(uint16_t shard_count)
{
    return RsCodec_params_is_wire_shard_count(shard_count);
}

static void rs_encode_rscode_4_2(unsigned char *data)
{
    size_t byte;

    for (byte = 0; byte < PKG_SIZE; byte++) {
        unsigned char msg[4];
        unsigned char codeword[6];
        size_t shard;

        for (shard = 0; shard < 4u; shard++) {
            msg[shard] = data[shard * PKG_SIZE + byte];
        }
        encode_data(msg, 4, codeword);
        for (shard = 4; shard < 6u; shard++) {
            data[shard * PKG_SIZE + byte] = codeword[shard];
        }
    }
}

static void rs_stat_add_u64(uint64_t *counter, uint64_t delta)
{
    __atomic_fetch_add(counter, delta, __ATOMIC_RELAXED);
}

/* Reference oracle: byte-major gmult against plan->coeff (bit-exact baseline). */
static void rs_encode_legacy_gmul(unsigned char *data, const RsEncodePlan *plan)
{
    size_t byte;
    size_t shard;
    size_t column;
    size_t k = plan->k;
    size_t r = plan->r;
    size_t shard_bytes = plan->shard_bytes;

    for (byte = 0; byte < shard_bytes; byte++) {
        unsigned char msg[RS_ABS_MAX_SHARDS];

        for (column = 0; column < k; column++) {
            msg[column] = data[column * shard_bytes + byte];
        }
        for (shard = 0; shard < r; shard++) {
            unsigned char value = 0;

            for (column = 0; column < k; column++) {
                value ^= gf_mul(plan->coeff[k + shard][column], msg[column]);
            }
            data[(k + shard) * shard_bytes + byte] = value;
        }
    }
    rs_stat_add_u64(&rs_encode_stats.gmul_calls,
                    (uint64_t)shard_bytes * (uint64_t)k * (uint64_t)r);
}

/*
 * Optimized general path for arbitrary fixed (k,r):
 * scan each data shard once; XOR into all parity rows via precomputed tables.
 * No gmult() in the inner loop. Does not hold rs_lock.
 */
static void rs_encode_general_table(unsigned char *data, const RsEncodePlan *plan)
{
    size_t k = plan->k;
    size_t r = plan->r;
    size_t shard_bytes = plan->shard_bytes;
    size_t column;
    size_t row;
    size_t byte;

    memset(data + k * shard_bytes, 0, r * shard_bytes);

    for (column = 0; column < k; column++) {
        const unsigned char *src = data + column * shard_bytes;

        for (row = 0; row < r; row++) {
            unsigned char *parity = data + (k + row) * shard_bytes;
            const uint8_t *table;

            if (plan->has_mul_table) {
                table = plan->mul_table + ((row * k + column) * 256u);
            } else {
                table = rs_gf_mul_table[plan->coeff[k + row][column]];
            }

            for (byte = 0; byte < shard_bytes; byte++) {
                parity[byte] ^= table[src[byte]];
            }
        }
    }
    rs_stat_add_u64(&rs_encode_stats.gmul_calls,
                    (uint64_t)shard_bytes * (uint64_t)k * (uint64_t)r);
}

/*
 * SIMD general path for arbitrary fixed (k,r). Source bytes are split into
 * nibbles and multiplied through 16-byte shuffle tables. Dispatch prefers
 * AVX2, then SSSE3 (i7-3770 / Ivy Bridge), then scalar.
 */
static void rs_encode_general_simd(unsigned char *data,
                                   const RsEncodePlan *plan)
{
    size_t k = plan->k;
    size_t r = plan->r;
    size_t shard_bytes = plan->shard_bytes;
    size_t column;

    memset(data + k * shard_bytes, 0, r * shard_bytes);
    for (column = 0; column < k; column++) {
        rs_gf256_encode_column(
            data + k * shard_bytes, shard_bytes,
            data + column * shard_bytes,
            plan->nibble_table + column * 32u, k * 32u, r, shard_bytes);
    }
    rs_stat_add_u64(&rs_encode_stats.gmul_calls,
                    (uint64_t)shard_bytes * (uint64_t)k * (uint64_t)r);
}

static int rs_geometry_is_fast_16_2(size_t k, size_t r, size_t len)
{
    return k == RS_FAST_K && r == RS_FAST_R && len == RS_FAST_N * PKG_SIZE &&
           rs_mul_table_ready;
}

static void rs_encode_16_2_fast(unsigned char *data, const RsEncodePlan *plan)
{
    unsigned char *parity0 = data + RS_FAST_K * PKG_SIZE;
    unsigned char *parity1 = data + (RS_FAST_K + 1u) * PKG_SIZE;
    size_t column;
    size_t byte;

    memset(parity0, 0, PKG_SIZE);
    memset(parity1, 0, PKG_SIZE);

    for (column = 0; column < RS_FAST_K; column++) {
        const unsigned char *src = data + column * PKG_SIZE;
        const uint8_t *row0;
        const uint8_t *row1;

        if (plan->has_mul_table) {
            row0 = plan->mul_table + ((0u * RS_FAST_K + column) * 256u);
            row1 = plan->mul_table + ((1u * RS_FAST_K + column) * 256u);
        } else {
            row0 = rs_gf_mul_table[plan->coeff[RS_FAST_K][column]];
            row1 = rs_gf_mul_table[plan->coeff[RS_FAST_K + 1u][column]];
        }

        for (byte = 0; byte < PKG_SIZE; byte++) {
            unsigned char symbol = src[byte];

            parity0[byte] ^= row0[symbol];
            parity1[byte] ^= row1[symbol];
        }
    }
    rs_stat_add_u64(&rs_encode_stats.gmul_calls,
                    (uint64_t)PKG_SIZE * (uint64_t)RS_FAST_K *
                        (uint64_t)RS_FAST_R);
}

/*
 * Encode entry. AUTO/GENERAL/FAST/LEGACY plan paths do not hold rs_lock.
 * Only explicit RS_ENCODE_RSCODE (hqm encode_data, 4+2) locks for mutable ECC
 * globals. Plan is immutable while geometry is fixed (file header contract).
 */
static void rs_encode(const Codec *self, unsigned char *data, size_t len)
{
    uint64_t wait_start;
    uint64_t hold_start;
    uint64_t body_start;
    uint64_t body_end;
    const RsEncodePlan *plan;
    size_t k;
    size_t r;
    size_t n;
    int use_rscode_4_2;
    int use_fast;
    int use_simd;
    int use_legacy;

    (void)self;

    if (data == NULL || !rs_ready()) {
        return;
    }

    rs_stat_add_u64(&rs_encode_stats.encode_calls, 1ull);

    /*
     * Read immutable plan without lock under the process-fixed geometry
     * contract. set_params must not race encode.
     */
    plan = &rs_encode_plan;
    if (!rs_matrix_ready || !plan->initialized) {
        return;
    }
    k = plan->k;
    r = plan->r;
    n = plan->n;
    if (len != n * plan->shard_bytes) {
        return;
    }

    use_legacy = (rs_encode_impl == RS_ENCODE_LEGACY);
    /* Explicit reference only — AUTO paths remain unlocked. */
    use_rscode_4_2 = (rs_encode_impl == RS_ENCODE_RSCODE) &&
                     (k == 4u && r == 2u && plan->shard_bytes == PKG_SIZE);
    use_fast = !use_legacy && !use_rscode_4_2 &&
               rs_geometry_is_fast_16_2(k, r, len) &&
               rs_encode_impl == RS_ENCODE_FAST_16_2;
    use_simd = !use_legacy && !use_rscode_4_2 && !use_fast &&
               rs_simd_enabled && plan->has_nibble_table &&
               rs_gf256_simd_available() &&
               (rs_encode_impl == RS_ENCODE_AUTO ||
                rs_encode_impl == RS_ENCODE_SIMD);

    if (use_rscode_4_2) {
        wait_start = monotonic_nanoseconds();
        pthread_mutex_lock(&rs_lock);
        hold_start = monotonic_nanoseconds();
        if (hold_start >= wait_start) {
            rs_stat_add_u64(&rs_encode_stats.lock_wait_ns,
                            hold_start - wait_start);
        }
        body_start = monotonic_nanoseconds();
        rs_encode_rscode_4_2(data);
        body_end = monotonic_nanoseconds();
        pthread_mutex_unlock(&rs_lock);
        rs_last_encode_impl = RS_ENCODE_RSCODE;
        rs_stat_add_u64(&rs_encode_stats.rscode_path_calls, 1ull);
        if (body_end >= body_start) {
            rs_stat_add_u64(&rs_encode_stats.encode_body_ns,
                            body_end - body_start);
        }
        if (body_end >= hold_start) {
            rs_stat_add_u64(&rs_encode_stats.lock_hold_ns,
                            body_end - hold_start);
        }
        return;
    }

    body_start = monotonic_nanoseconds();
    if (use_legacy) {
        rs_encode_legacy_gmul(data, plan);
        rs_last_encode_impl = RS_ENCODE_LEGACY;
        rs_stat_add_u64(&rs_encode_stats.legacy_path_calls, 1ull);
    } else if (use_fast) {
        rs_encode_16_2_fast(data, plan);
        rs_last_encode_impl = RS_ENCODE_FAST_16_2;
        rs_stat_add_u64(&rs_encode_stats.fast_path_calls, 1ull);
    } else if (use_simd) {
        rs_encode_general_simd(data, plan);
        rs_last_encode_impl = RS_ENCODE_SIMD;
        rs_stat_add_u64(&rs_encode_stats.simd_path_calls, 1ull);
    } else {
        /* GENERAL, or AUTO/SIMD on a CPU without AVX2. */
        rs_encode_general_table(data, plan);
        rs_last_encode_impl = RS_ENCODE_GENERAL;
        rs_stat_add_u64(&rs_encode_stats.general_path_calls, 1ull);
    }
    body_end = monotonic_nanoseconds();
    if (body_end >= body_start) {
        rs_stat_add_u64(&rs_encode_stats.encode_body_ns, body_end - body_start);
    }
}

static void rs_decode(const Codec *self, unsigned char *data, size_t len)
{
    (void)self;
    if (data == NULL || len != rs_geo.n * rs_geo.shard_bytes) {
        return;
    }
}

static size_t rs_input_block_size(const Codec *self)
{
    (void)self;
    (void)rs_ready();
    return rs_geo.k * rs_geo.shard_bytes;
}

static size_t rs_output_block_size(const Codec *self)
{
    (void)self;
    (void)rs_ready();
    return rs_geo.n * rs_geo.shard_bytes;
}

static size_t rs_data_shards(const Codec *self)
{
    (void)self;
    (void)rs_ready();
    return rs_geo.k;
}

static size_t rs_parity_shards(const Codec *self)
{
    (void)self;
    (void)rs_ready();
    return rs_geo.r;
}

static int rs_is_systematic(const Codec *self)
{
    (void)self;
    return 1;
}

static int rs_data_shards_present(const uint8_t *present_bits, size_t k)
{
    size_t shard;

    for (shard = 0; shard < k; shard++) {
        if (!codec_present_get(present_bits, shard)) {
            return 0;
        }
    }
    return 1;
}

static CodecRecoverStatus rs_recover(const Codec *self,
                                     unsigned char *shards,
                                     const uint8_t *present_bits,
                                     size_t shard_count)
{
    unsigned char survivors[RS_ABS_MAX_SHARDS];
    unsigned char submatrix[RS_ABS_MAX_SHARDS * RS_ABS_MAX_SHARDS];
    unsigned char inverse[RS_ABS_MAX_SHARDS * RS_ABS_MAX_SHARDS];
    size_t shard;
    size_t received = 0;
    size_t survivor_count = 0;
    size_t column;

    (void)self;

    if (shards == NULL || present_bits == NULL ||
        shard_count != rs_geo.n || RsCodec_prepare_matrix() != 0) {
        return CODEC_RECOVER_ERR;
    }

    for (shard = 0; shard < rs_geo.n; shard++) {
        if (codec_present_get(present_bits, shard)) {
            received++;
            if (survivor_count < rs_geo.k) {
                survivors[survivor_count++] = (unsigned char)shard;
            }
        }
    }
    if (received < rs_geo.k) {
        return CODEC_RECOVER_UNAVAILABLE;
    }
    if (received == rs_geo.n) {
        return CODEC_RECOVER_OK;
    }

    if (rs_data_shards_present(present_bits, rs_geo.k)) {
        return CODEC_RECOVER_OK;
    }

    if (survivor_count != rs_geo.k) {
        return CODEC_RECOVER_ERR;
    }

    for (shard = 0; shard < rs_geo.k; shard++) {
        for (column = 0; column < rs_geo.k; column++) {
            submatrix[shard * RS_ABS_MAX_SHARDS + column] =
                rs_generator[survivors[shard]][column];
        }
    }
    if (matrix_inverse_dyn(submatrix, inverse, rs_geo.k) != 0) {
        return CODEC_RECOVER_ERR;
    }

    for (shard = 0; shard < rs_geo.n; shard++) {
        size_t byte;
        unsigned char repair_row[RS_ABS_MAX_SHARDS];
        size_t term;

        if (codec_present_get(present_bits, shard)) {
            continue;
        }

        for (column = 0; column < rs_geo.k; column++) {
            unsigned char value = 0;

            for (term = 0; term < rs_geo.k; term++) {
                value ^= gf_mul(rs_generator[shard][term],
                                inverse[term * RS_ABS_MAX_SHARDS + column]);
            }
            repair_row[column] = value;
        }

        for (byte = 0; byte < rs_geo.shard_bytes; byte++) {
            unsigned char value = 0;

            for (term = 0; term < rs_geo.k; term++) {
                value ^= gf_mul(repair_row[term],
                                shards[(size_t)survivors[term] *
                                           rs_geo.shard_bytes +
                                       byte]);
            }
            shards[shard * rs_geo.shard_bytes + byte] = value;
        }
    }
    return CODEC_RECOVER_OK;
}

static const CodecVTable rs_vtable = {
    .encode = rs_encode,
    .decode = rs_decode,
    .input_block_size = rs_input_block_size,
    .output_block_size = rs_output_block_size,
    .data_shards = rs_data_shards,
    .parity_shards = rs_parity_shards,
    .is_systematic = rs_is_systematic,
    .recover = rs_recover,
};

static const Codec rs_codec = {
    .vtable = &rs_vtable,
    .impl = NULL,
};

const Codec *RsCodec_get(void)
{
    (void)rs_ready();
    return &rs_codec;
}
