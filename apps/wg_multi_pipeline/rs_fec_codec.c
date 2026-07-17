#include "rs_fec_codec.h"

#include "stream_config.h"

#include <liberasurecode/liberasurecode_rs_vand.h>

#include <pthread.h>
#include <stddef.h>

static pthread_once_t rs_once = PTHREAD_ONCE_INIT;
static int           *rs_generator_matrix;

static void rs_fec_init(void)
{
    init_liberasurecode_rs_vand((int)RS_FEC_DATA_SHARDS,
                                (int)RS_FEC_PARITY_SHARDS);
    rs_generator_matrix = make_systematic_matrix((int)RS_FEC_DATA_SHARDS,
                                                  (int)RS_FEC_PARITY_SHARDS);
}

static int rs_fec_ready(void)
{
    return pthread_once(&rs_once, rs_fec_init) == 0 &&
           rs_generator_matrix != NULL;
}

static void rs_fec_encode(const Codec *self, unsigned char *data, size_t len)
{
    char *data_shards[RS_FEC_DATA_SHARDS];
    char *parity_shards[RS_FEC_PARITY_SHARDS];
    size_t shard;

    (void)self;

    if (data == NULL || len != RS_FEC_ENCODE_BLOCK || !rs_fec_ready()) {
        return;
    }

    for (shard = 0; shard < RS_FEC_DATA_SHARDS; shard++) {
        data_shards[shard] = (char *)(data + shard * PKG_SIZE);
    }
    for (shard = 0; shard < RS_FEC_PARITY_SHARDS; shard++) {
        parity_shards[shard] = (char *)(data +
                                        (RS_FEC_DATA_SHARDS + shard) * PKG_SIZE);
    }

    (void)liberasurecode_rs_vand_encode(rs_generator_matrix,
                                        data_shards, parity_shards,
                                        (int)RS_FEC_DATA_SHARDS,
                                        (int)RS_FEC_PARITY_SHARDS,
                                        (int)PKG_SIZE);
}

static void rs_fec_decode(const Codec *self, unsigned char *data, size_t len)
{
    (void)self;

    /*
     * RS FEC is systematic: recovery restores missing shards before decode,
     * leaving the original data in the first RS_FEC_DATA_SHARDS positions.
     */
    if (data == NULL || len != RS_FEC_ENCODE_BLOCK) {
        return;
    }
}

static size_t rs_fec_input_block_size(const Codec *self)
{
    (void)self;
    return DECODE_BLOCK;
}

static size_t rs_fec_output_block_size(const Codec *self)
{
    (void)self;
    return RS_FEC_ENCODE_BLOCK;
}

static size_t rs_fec_data_shards(const Codec *self)
{
    (void)self;
    return RS_FEC_DATA_SHARDS;
}

static size_t rs_fec_parity_shards(const Codec *self)
{
    (void)self;
    return RS_FEC_PARITY_SHARDS;
}

static int rs_fec_is_systematic(const Codec *self)
{
    (void)self;
    return 1;
}

static CodecRecoverStatus rs_fec_recover(const Codec *self,
                                          unsigned char *shards,
                                          uint16_t present_mask)
{
    char *data_shards[RS_FEC_DATA_SHARDS];
    char *parity_shards[RS_FEC_PARITY_SHARDS];
    int missing[RS_FEC_TOTAL_SHARDS + 1u];
    size_t shard;
    size_t received = 0;
    size_t missing_count = 0;

    (void)self;

    if (shards == NULL || (present_mask & (uint16_t)~0x3fu) != 0 ||
        !rs_fec_ready()) {
        return CODEC_RECOVER_ERR;
    }

    for (shard = 0; shard < RS_FEC_TOTAL_SHARDS; shard++) {
        if ((present_mask & (uint16_t)(1u << shard)) != 0) {
            received++;
        } else {
            missing[missing_count++] = (int)shard;
        }
    }
    if (received < RS_FEC_DATA_SHARDS) {
        return CODEC_RECOVER_UNAVAILABLE;
    }
    /* The native backend takes a -1-terminated list of missing indices. */
    missing[missing_count] = -1;

    for (shard = 0; shard < RS_FEC_DATA_SHARDS; shard++) {
        data_shards[shard] = (char *)(shards + shard * PKG_SIZE);
    }
    for (shard = 0; shard < RS_FEC_PARITY_SHARDS; shard++) {
        parity_shards[shard] = (char *)(shards +
                                        (RS_FEC_DATA_SHARDS + shard) * PKG_SIZE);
    }

    if (liberasurecode_rs_vand_decode(rs_generator_matrix,
                                      data_shards, parity_shards,
                                      (int)RS_FEC_DATA_SHARDS,
                                      (int)RS_FEC_PARITY_SHARDS,
                                      missing, (int)PKG_SIZE, 1) != 0) {
        return CODEC_RECOVER_ERR;
    }

    return CODEC_RECOVER_OK;
}

static const CodecVTable rs_fec_codec_vtable = {
    .encode = rs_fec_encode,
    .decode = rs_fec_decode,
    .input_block_size = rs_fec_input_block_size,
    .output_block_size = rs_fec_output_block_size,
    .data_shards = rs_fec_data_shards,
    .parity_shards = rs_fec_parity_shards,
    .is_systematic = rs_fec_is_systematic,
    .recover = rs_fec_recover,
};

static const Codec rs_fec_codec = {
    .vtable = &rs_fec_codec_vtable,
    .impl = NULL,
};

const Codec *RsFecCodec_get(void)
{
    return &rs_fec_codec;
}
