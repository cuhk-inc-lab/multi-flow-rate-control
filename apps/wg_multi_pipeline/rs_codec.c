#include "rs_codec.h"

#include "stream_config.h"

#include "ecc.h"

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

/*
 * Column-wise systematic RS(4,2) using hqm/rscode (NPAR=2).
 * For each byte offset across the six shards, form a 6-byte codeword
 * [d0,d1,d2,d3,p0,p1]. Geometry matches rs-fec; parity bytes differ.
 *
 * rscode erasure locator L corrects codeword[csize - L - 1], so for
 * shard index s and csize=6, L = 5 - s.
 */

#define RS_CODEWORD_LEN \
    ((int)(RS_FEC_DATA_SHARDS + RS_FEC_PARITY_SHARDS))
#define RS_VALID_MASK ((uint16_t)((1u << RS_FEC_TOTAL_SHARDS) - 1u))

typedef struct RsRepairPlan {
    int           valid;
    unsigned char survivor[RS_FEC_DATA_SHARDS];
    unsigned char repair[RS_FEC_TOTAL_SHARDS][RS_FEC_DATA_SHARDS];
} RsRepairPlan;

static pthread_once_t  rs_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t rs_lock = PTHREAD_MUTEX_INITIALIZER;
static unsigned char   rs_generator[RS_FEC_TOTAL_SHARDS][RS_FEC_DATA_SHARDS];
static RsRepairPlan    rs_plans[1u << RS_FEC_TOTAL_SHARDS];
static int             rs_matrix_ready;
static uint64_t        rs_matrix_init_ns;
static RsRecoverMode   rs_recover_mode = RS_RECOVER_MATRIX;

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

static unsigned popcount_mask(uint16_t mask)
{
    unsigned count = 0;

    while (mask != 0) {
        count += mask & 1u;
        mask >>= 1;
    }
    return count;
}

static int matrix_inverse(unsigned char input[RS_FEC_DATA_SHARDS][RS_FEC_DATA_SHARDS],
                          unsigned char output[RS_FEC_DATA_SHARDS][RS_FEC_DATA_SHARDS])
{
    unsigned char augmented[RS_FEC_DATA_SHARDS][RS_FEC_DATA_SHARDS * 2u];
    size_t row;
    size_t column;

    for (row = 0; row < RS_FEC_DATA_SHARDS; row++) {
        for (column = 0; column < RS_FEC_DATA_SHARDS; column++) {
            augmented[row][column] = input[row][column];
            augmented[row][RS_FEC_DATA_SHARDS + column] =
                row == column ? 1u : 0u;
        }
    }

    for (column = 0; column < RS_FEC_DATA_SHARDS; column++) {
        size_t pivot = column;
        size_t other;

        while (pivot < RS_FEC_DATA_SHARDS &&
               augmented[pivot][column] == 0) {
            pivot++;
        }
        if (pivot == RS_FEC_DATA_SHARDS) {
            return -1;
        }
        if (pivot != column) {
            unsigned char tmp[RS_FEC_DATA_SHARDS * 2u];

            memcpy(tmp, augmented[pivot], sizeof(tmp));
            memcpy(augmented[pivot], augmented[column], sizeof(tmp));
            memcpy(augmented[column], tmp, sizeof(tmp));
        }

        {
            unsigned char inverse = gf_inv(augmented[column][column]);

            for (other = 0; other < RS_FEC_DATA_SHARDS * 2u; other++) {
                augmented[column][other] =
                    gf_mul(augmented[column][other], inverse);
            }
        }

        for (row = 0; row < RS_FEC_DATA_SHARDS; row++) {
            unsigned char factor;

            if (row == column) {
                continue;
            }
            factor = augmented[row][column];
            if (factor == 0) {
                continue;
            }
            for (other = 0; other < RS_FEC_DATA_SHARDS * 2u; other++) {
                augmented[row][other] ^= gf_mul(factor, augmented[column][other]);
            }
        }
    }

    for (row = 0; row < RS_FEC_DATA_SHARDS; row++) {
        for (column = 0; column < RS_FEC_DATA_SHARDS; column++) {
            output[row][column] = augmented[row][RS_FEC_DATA_SHARDS + column];
        }
    }
    return 0;
}

static int verify_inverse(unsigned char matrix[RS_FEC_DATA_SHARDS][RS_FEC_DATA_SHARDS],
                          unsigned char inverse[RS_FEC_DATA_SHARDS][RS_FEC_DATA_SHARDS])
{
    size_t row;
    size_t column;
    size_t term;

    for (row = 0; row < RS_FEC_DATA_SHARDS; row++) {
        for (column = 0; column < RS_FEC_DATA_SHARDS; column++) {
            unsigned char value = 0;

            for (term = 0; term < RS_FEC_DATA_SHARDS; term++) {
                value ^= gf_mul(matrix[row][term], inverse[term][column]);
            }
            if (value != (row == column ? 1u : 0u)) {
                return -1;
            }
        }
    }
    return 0;
}

static int build_repair_plans(void)
{
    uint16_t mask;
    size_t row;
    size_t column;

    for (column = 0; column < RS_FEC_DATA_SHARDS; column++) {
        unsigned char message[RS_FEC_DATA_SHARDS] = {0};
        unsigned char codeword[RS_FEC_TOTAL_SHARDS];

        message[column] = 1u;
        encode_data(message, (int)RS_FEC_DATA_SHARDS, codeword);
        for (row = 0; row < RS_FEC_TOTAL_SHARDS; row++) {
            rs_generator[row][column] = codeword[row];
        }
    }

    for (row = 0; row < RS_FEC_DATA_SHARDS; row++) {
        for (column = 0; column < RS_FEC_DATA_SHARDS; column++) {
            if (rs_generator[row][column] != (row == column ? 1u : 0u)) {
                return -1;
            }
        }
    }

    for (mask = 0; mask <= RS_VALID_MASK; mask++) {
        RsRepairPlan *plan = &rs_plans[mask];
        unsigned char submatrix[RS_FEC_DATA_SHARDS][RS_FEC_DATA_SHARDS];
        unsigned char inverse[RS_FEC_DATA_SHARDS][RS_FEC_DATA_SHARDS];
        size_t survivor_count = 0;

        memset(plan, 0, sizeof(*plan));
        if (popcount_mask(mask) < RS_FEC_DATA_SHARDS) {
            continue;
        }

        for (row = 0; row < RS_FEC_TOTAL_SHARDS &&
                      survivor_count < RS_FEC_DATA_SHARDS; row++) {
            if ((mask & (uint16_t)(1u << row)) != 0) {
                plan->survivor[survivor_count++] = (unsigned char)row;
            }
        }
        if (survivor_count != RS_FEC_DATA_SHARDS) {
            return -1;
        }

        for (row = 0; row < RS_FEC_DATA_SHARDS; row++) {
            for (column = 0; column < RS_FEC_DATA_SHARDS; column++) {
                submatrix[row][column] =
                    rs_generator[plan->survivor[row]][column];
            }
        }
        if (matrix_inverse(submatrix, inverse) != 0 ||
            verify_inverse(submatrix, inverse) != 0) {
            return -1;
        }

        for (row = 0; row < RS_FEC_TOTAL_SHARDS; row++) {
            for (column = 0; column < RS_FEC_DATA_SHARDS; column++) {
                size_t term;
                unsigned char value = 0;

                for (term = 0; term < RS_FEC_DATA_SHARDS; term++) {
                    value ^= gf_mul(rs_generator[row][term], inverse[term][column]);
                }
                plan->repair[row][column] = value;
            }
        }
        plan->valid = 1;
    }
    return 0;
}

static void rs_init(void)
{
    uint64_t started = monotonic_nanoseconds();

    initialize_ecc();
    rs_matrix_ready = build_repair_plans() == 0;
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

int RsCodec_set_recover_mode(RsRecoverMode mode)
{
    if (mode != RS_RECOVER_LEGACY && mode != RS_RECOVER_MATRIX) {
        return -1;
    }
    if (mode == RS_RECOVER_MATRIX && RsCodec_prepare_matrix() != 0) {
        return -1;
    }
    rs_recover_mode = mode;
    return 0;
}

RsRecoverMode RsCodec_get_recover_mode(void)
{
    return rs_recover_mode;
}

static void rs_encode(const Codec *self, unsigned char *data, size_t len)
{
    size_t byte;

    (void)self;

    if (data == NULL || len != RS_FEC_ENCODE_BLOCK || !rs_ready()) {
        return;
    }

    pthread_mutex_lock(&rs_lock);
    for (byte = 0; byte < PKG_SIZE; byte++) {
        unsigned char msg[RS_FEC_DATA_SHARDS];
        unsigned char codeword[RS_FEC_TOTAL_SHARDS];
        size_t shard;

        for (shard = 0; shard < RS_FEC_DATA_SHARDS; shard++) {
            msg[shard] = data[shard * PKG_SIZE + byte];
        }
        encode_data(msg, (int)RS_FEC_DATA_SHARDS, codeword);
        for (shard = RS_FEC_DATA_SHARDS; shard < RS_FEC_TOTAL_SHARDS; shard++) {
            data[shard * PKG_SIZE + byte] = codeword[shard];
        }
    }
    pthread_mutex_unlock(&rs_lock);
}

static void rs_decode(const Codec *self, unsigned char *data, size_t len)
{
    (void)self;

    /*
     * Systematic: recovery restores missing shards before decode,
     * leaving the original data in the first RS_FEC_DATA_SHARDS positions.
     */
    if (data == NULL || len != RS_FEC_ENCODE_BLOCK) {
        return;
    }
}

static size_t rs_input_block_size(const Codec *self)
{
    (void)self;
    return DECODE_BLOCK;
}

static size_t rs_output_block_size(const Codec *self)
{
    (void)self;
    return RS_FEC_ENCODE_BLOCK;
}

static size_t rs_data_shards(const Codec *self)
{
    (void)self;
    return RS_FEC_DATA_SHARDS;
}

static size_t rs_parity_shards(const Codec *self)
{
    (void)self;
    return RS_FEC_PARITY_SHARDS;
}

static int rs_is_systematic(const Codec *self)
{
    (void)self;
    return 1;
}

static CodecRecoverStatus rs_recover_legacy(const Codec *self,
                                            unsigned char *shards,
                                            uint16_t present_mask)
{
    int erasures[RS_FEC_TOTAL_SHARDS];
    size_t shard;
    size_t received = 0;
    size_t missing_count = 0;
    size_t byte;
    int ok = 1;

    (void)self;

    if (shards == NULL || (present_mask & (uint16_t)~0x3fu) != 0 || !rs_ready()) {
        return CODEC_RECOVER_ERR;
    }

    for (shard = 0; shard < RS_FEC_TOTAL_SHARDS; shard++) {
        if ((present_mask & (uint16_t)(1u << shard)) != 0) {
            received++;
        } else {
            erasures[missing_count++] = RS_CODEWORD_LEN - 1 - (int)shard;
            memset(shards + shard * PKG_SIZE, 0, PKG_SIZE);
        }
    }

    if (missing_count == 0) {
        return CODEC_RECOVER_OK;
    }
    if (received < RS_FEC_DATA_SHARDS) {
        return CODEC_RECOVER_UNAVAILABLE;
    }

    pthread_mutex_lock(&rs_lock);
    for (byte = 0; byte < PKG_SIZE; byte++) {
        unsigned char codeword[RS_FEC_TOTAL_SHARDS];

        for (shard = 0; shard < RS_FEC_TOTAL_SHARDS; shard++) {
            codeword[shard] = shards[shard * PKG_SIZE + byte];
        }

        decode_data(codeword, RS_CODEWORD_LEN);
        if (check_syndrome() == 0) {
            continue;
        }

        if (!correct_errors_erasures(codeword, RS_CODEWORD_LEN,
                                     (int)missing_count, erasures)) {
            ok = 0;
            break;
        }

        for (shard = 0; shard < RS_FEC_TOTAL_SHARDS; shard++) {
            if ((present_mask & (uint16_t)(1u << shard)) == 0) {
                shards[shard * PKG_SIZE + byte] = codeword[shard];
            }
        }
    }
    pthread_mutex_unlock(&rs_lock);

    return ok ? CODEC_RECOVER_OK : CODEC_RECOVER_ERR;
}

static CodecRecoverStatus rs_recover_matrix(const Codec *self,
                                            unsigned char *shards,
                                            uint16_t present_mask)
{
    const RsRepairPlan *plan;
    size_t shard;
    size_t received = 0;

    (void)self;

    if (shards == NULL || (present_mask & (uint16_t)~RS_VALID_MASK) != 0 ||
        RsCodec_prepare_matrix() != 0) {
        return CODEC_RECOVER_ERR;
    }

    for (shard = 0; shard < RS_FEC_TOTAL_SHARDS; shard++) {
        if ((present_mask & (uint16_t)(1u << shard)) != 0) {
            received++;
        }
    }
    if (received < RS_FEC_DATA_SHARDS) {
        return CODEC_RECOVER_UNAVAILABLE;
    }
    if (received == RS_FEC_TOTAL_SHARDS) {
        return CODEC_RECOVER_OK;
    }

    /*
     * The payload is systematic. When all data shards arrived, decoding can
     * proceed without recreating missing parity shards.
     */
    if ((present_mask & 0x0fu) == 0x0fu) {
        return CODEC_RECOVER_OK;
    }

    plan = &rs_plans[present_mask];
    if (!plan->valid) {
        return CODEC_RECOVER_ERR;
    }

    for (shard = 0; shard < RS_FEC_TOTAL_SHARDS; shard++) {
        size_t byte;

        if ((present_mask & (uint16_t)(1u << shard)) != 0) {
            continue;
        }
        for (byte = 0; byte < PKG_SIZE; byte++) {
            unsigned char value = 0;
            size_t survivor;

            for (survivor = 0; survivor < RS_FEC_DATA_SHARDS; survivor++) {
                value ^= gf_mul(plan->repair[shard][survivor],
                                shards[(size_t)plan->survivor[survivor] *
                                       PKG_SIZE + byte]);
            }
            shards[shard * PKG_SIZE + byte] = value;
        }
    }
    return CODEC_RECOVER_OK;
}

static CodecRecoverStatus rs_recover(const Codec *self,
                                     unsigned char *shards,
                                     uint16_t present_mask)
{
    if (rs_recover_mode == RS_RECOVER_MATRIX) {
        return rs_recover_matrix(self, shards, present_mask);
    }
    return rs_recover_legacy(self, shards, present_mask);
}

static const CodecVTable rs_codec_vtable = {
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
    .vtable = &rs_codec_vtable,
    .impl = NULL,
};

const Codec *RsCodec_get(void)
{
    return &rs_codec;
}
