#include "rs_codec.h"

#include "stream_config.h"

#include "ecc.h"

#include <pthread.h>
#include <stddef.h>
#include <string.h>

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

static pthread_once_t  rs_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t rs_lock = PTHREAD_MUTEX_INITIALIZER;

static void rs_init(void)
{
    initialize_ecc();
}

static int rs_ready(void)
{
    return pthread_once(&rs_once, rs_init) == 0;
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

static CodecRecoverStatus rs_recover(const Codec *self,
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
