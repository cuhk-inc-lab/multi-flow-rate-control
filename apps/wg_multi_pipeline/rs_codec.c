#include "rs_codec.h"

#include "stream_config.h"

#include "ecc.h"

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

/*
 * Column-wise systematic RS(n,k). Default RS(6,4) keeps hqm/rscode encode
 * (NPAR=2) for wire compatibility. Any other (k,r) uses a Cauchy generator
 * and on-demand matrix repair (no 2^n plan table).
 */

typedef struct RsGeometry {
    size_t k;
    size_t r;
    size_t n;
} RsGeometry;

static pthread_once_t  rs_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t rs_lock = PTHREAD_MUTEX_INITIALIZER;
static unsigned char   rs_generator[RS_ABS_MAX_SHARDS][RS_ABS_MAX_SHARDS];
static int             rs_matrix_ready;
static uint64_t        rs_matrix_init_ns;
static RsRecoverMode   rs_recover_mode = RS_RECOVER_MATRIX;
static RsGeometry      rs_geo = {.k = 4u, .r = 2u, .n = 6u};

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

static unsigned char gf_inv(unsigned char value)
{
    return (unsigned char)ginv((int)value);
}

static int params_valid(size_t k, size_t r)
{
    size_t n;

    if (k < 1u || r < 1u) {
        return 0;
    }
    n = k + r;
    /* Working size is exactly (k,r). Ceiling: GF(256) RS codeword n <= 255. */
    if (n > RS_ABS_MAX_SHARDS) {
        return 0;
    }
    if (n * PKG_SIZE > CODEC_MAX_ENCODE_BLOCK) {
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
    if (rs_geo.k == 4u && rs_geo.r == 2u) {
        return build_generator_rscode_4_2();
    }
    return build_generator_cauchy(rs_geo.k, rs_geo.r);
}

static void rs_init(void)
{
    uint64_t started = monotonic_nanoseconds();

    initialize_ecc();
    rs_geo.k = 4u;
    rs_geo.r = 2u;
    rs_geo.n = 6u;
    rs_matrix_ready = rebuild_generator() == 0;
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

int RsCodec_set_params(size_t data_shards, size_t parity_shards)
{
    if (!params_valid(data_shards, parity_shards) || !rs_ready()) {
        return -1;
    }

    pthread_mutex_lock(&rs_lock);
    if (rs_geo.k == data_shards && rs_geo.r == parity_shards &&
        rs_matrix_ready) {
        pthread_mutex_unlock(&rs_lock);
        return 0;
    }
    rs_geo.k = data_shards;
    rs_geo.r = parity_shards;
    rs_geo.n = data_shards + parity_shards;
    rs_matrix_ready = rebuild_generator() == 0;
    pthread_mutex_unlock(&rs_lock);

    if (rs_recover_mode == RS_RECOVER_LEGACY &&
        !(rs_geo.k == 4u && rs_geo.r == 2u)) {
        rs_recover_mode = RS_RECOVER_MATRIX;
    }
    return rs_matrix_ready ? 0 : -1;
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
    return params_valid(rs_geo.k, (size_t)shard_count - rs_geo.k);
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

int RsCodec_set_recover_mode(RsRecoverMode mode)
{
    if (mode != RS_RECOVER_LEGACY && mode != RS_RECOVER_MATRIX) {
        return -1;
    }
    if (mode == RS_RECOVER_MATRIX && RsCodec_prepare_matrix() != 0) {
        return -1;
    }
    if (mode == RS_RECOVER_LEGACY &&
        !(rs_geo.k == 4u && rs_geo.r == 2u)) {
        return -1;
    }
    rs_recover_mode = mode;
    return 0;
}

RsRecoverMode RsCodec_get_recover_mode(void)
{
    return rs_recover_mode;
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

static void rs_encode_from_generator(unsigned char *data)
{
    size_t byte;
    size_t shard;
    size_t column;

    for (byte = 0; byte < PKG_SIZE; byte++) {
        unsigned char msg[RS_ABS_MAX_SHARDS];

        for (column = 0; column < rs_geo.k; column++) {
            msg[column] = data[column * PKG_SIZE + byte];
        }
        for (shard = rs_geo.k; shard < rs_geo.n; shard++) {
            unsigned char value = 0;

            for (column = 0; column < rs_geo.k; column++) {
                value ^= gf_mul(rs_generator[shard][column], msg[column]);
            }
            data[shard * PKG_SIZE + byte] = value;
        }
    }
}

static void rs_encode(const Codec *self, unsigned char *data, size_t len)
{
    (void)self;

    if (data == NULL || len != rs_geo.n * PKG_SIZE || !rs_ready() ||
        !rs_matrix_ready) {
        return;
    }

    pthread_mutex_lock(&rs_lock);
    if (rs_geo.k == 4u && rs_geo.r == 2u) {
        rs_encode_rscode_4_2(data);
    } else {
        rs_encode_from_generator(data);
    }
    pthread_mutex_unlock(&rs_lock);
}

static void rs_decode(const Codec *self, unsigned char *data, size_t len)
{
    (void)self;
    if (data == NULL || len != rs_geo.n * PKG_SIZE) {
        return;
    }
}

static size_t rs_input_block_size(const Codec *self)
{
    (void)self;
    (void)rs_ready();
    return rs_geo.k * PKG_SIZE;
}

static size_t rs_output_block_size(const Codec *self)
{
    (void)self;
    (void)rs_ready();
    return rs_geo.n * PKG_SIZE;
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

static CodecRecoverStatus rs_recover_legacy(const Codec *self,
                                            unsigned char *shards,
                                            const uint8_t *present_bits,
                                            size_t shard_count)
{
    int erasures[6];
    size_t shard;
    size_t received = 0;
    size_t missing_count = 0;
    size_t byte;
    int ok = 1;

    (void)self;

    if (!(rs_geo.k == 4u && rs_geo.r == 2u) || shards == NULL ||
        present_bits == NULL || shard_count != 6u || !rs_ready()) {
        return CODEC_RECOVER_ERR;
    }

    for (shard = 0; shard < 6u; shard++) {
        if (codec_present_get(present_bits, shard)) {
            received++;
        } else {
            erasures[missing_count++] = 5 - (int)shard;
            memset(shards + shard * PKG_SIZE, 0, PKG_SIZE);
        }
    }

    if (missing_count == 0) {
        return CODEC_RECOVER_OK;
    }
    if (received < 4u) {
        return CODEC_RECOVER_UNAVAILABLE;
    }

    pthread_mutex_lock(&rs_lock);
    for (byte = 0; byte < PKG_SIZE; byte++) {
        unsigned char codeword[6];

        for (shard = 0; shard < 6u; shard++) {
            codeword[shard] = shards[shard * PKG_SIZE + byte];
        }

        decode_data(codeword, 6);
        if (check_syndrome() == 0) {
            continue;
        }

        if (!correct_errors_erasures(codeword, 6, (int)missing_count,
                                     erasures)) {
            ok = 0;
            break;
        }

        for (shard = 0; shard < 6u; shard++) {
            if (!codec_present_get(present_bits, shard)) {
                shards[shard * PKG_SIZE + byte] = codeword[shard];
            }
        }
    }
    pthread_mutex_unlock(&rs_lock);

    return ok ? CODEC_RECOVER_OK : CODEC_RECOVER_ERR;
}

static CodecRecoverStatus rs_recover_matrix(const Codec *self,
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

        for (byte = 0; byte < PKG_SIZE; byte++) {
            unsigned char value = 0;

            for (term = 0; term < rs_geo.k; term++) {
                value ^= gf_mul(repair_row[term],
                                shards[(size_t)survivors[term] * PKG_SIZE +
                                       byte]);
            }
            shards[shard * PKG_SIZE + byte] = value;
        }
    }
    return CODEC_RECOVER_OK;
}

static CodecRecoverStatus rs_recover(const Codec *self,
                                     unsigned char *shards,
                                     const uint8_t *present_bits,
                                     size_t shard_count)
{
    if (rs_recover_mode == RS_RECOVER_MATRIX) {
        return rs_recover_matrix(self, shards, present_bits, shard_count);
    }
    return rs_recover_legacy(self, shards, present_bits, shard_count);
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
