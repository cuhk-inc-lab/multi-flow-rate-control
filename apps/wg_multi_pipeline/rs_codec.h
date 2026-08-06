#ifndef RS_CODEC_H
#define RS_CODEC_H

#include "codec.h"

#include <stddef.h>
#include <stdint.h>

/*
 * Runtime RS(n,k), n = data_shards + parity_shards.
 * Working size follows RsCodec_set_params / CLI.
 *
 * present_bits is a bit array (see codec.h) — no 16/32-bit integer width cap.
 * Practical ceiling: RS_ABS_MAX_SHARDS = 255 (GF(256) byte-symbol RS).
 * Recover always uses the matrix erasure path.
 */
#define RS_ABS_MAX_SHARDS 255u

#define RS_MAX_TOTAL_SHARDS  RS_ABS_MAX_SHARDS
#define RS_MAX_DATA_SHARDS   (RS_ABS_MAX_SHARDS - 1u)
#define RS_MAX_PARITY_SHARDS (RS_ABS_MAX_SHARDS - 1u)

typedef enum RsProfile {
    RS_PROFILE_4_1 = 0,
    RS_PROFILE_4_2,
    RS_PROFILE_4_3
} RsProfile;

const Codec *RsCodec_get(void);

int RsCodec_set_params(size_t data_shards, size_t parity_shards);
void RsCodec_get_params(size_t *data_shards, size_t *parity_shards);

int RsCodec_set_profile(RsProfile profile);
RsProfile RsCodec_get_profile(void);

int RsCodec_set_params_from_shard_count(uint16_t shard_count);
int RsCodec_params_is_wire_shard_count(uint16_t shard_count);

int RsCodec_set_profile_from_shard_count(uint16_t shard_count);
int RsCodec_profile_is_wire_shard_count(uint16_t shard_count);

int RsCodec_prepare_matrix(void);
int RsCodec_matrix_ready(void);
uint64_t RsCodec_matrix_init_ns(void);

#endif /* RS_CODEC_H */
