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
 *
 * Encode uses an immutable RsEncodePlan published by set_params().
 * Callers must finish RS configuration before starting encode workers;
 * set_params must not run concurrently with encode.
 */
#define RS_ABS_MAX_SHARDS 255u

#define RS_MAX_TOTAL_SHARDS  RS_ABS_MAX_SHARDS
#define RS_MAX_DATA_SHARDS   (RS_ABS_MAX_SHARDS - 1u)
#define RS_MAX_PARITY_SHARDS (RS_ABS_MAX_SHARDS - 1u)

/*
 * Cap for expanded encode mul_table (r * k * 256 bytes).
 * Worst-case n<=255 is ~4.2 MiB; 8 MiB leaves headroom and rejects overflow.
 * Over the cap: plan still publishes, but mul_table is NULL and encode uses
 * the shared 256×256 GF table (still no gmult in the hot loop).
 */
#define RS_ENCODE_MUL_TABLE_MAX_BYTES (8u * 1024u * 1024u)

typedef enum RsProfile {
    RS_PROFILE_4_1 = 0,
    RS_PROFILE_4_2,
    RS_PROFILE_4_3
} RsProfile;

const Codec *RsCodec_get(void);

int RsCodec_set_params(size_t data_shards, size_t parity_shards);
int RsCodec_set_params_ex(size_t data_shards, size_t parity_shards,
                          size_t shard_bytes);
void RsCodec_get_params(size_t *data_shards, size_t *parity_shards);

int RsCodec_set_profile(RsProfile profile);
RsProfile RsCodec_get_profile(void);

int RsCodec_set_params_from_shard_count(uint16_t shard_count);
int RsCodec_params_is_wire_shard_count(uint16_t shard_count);

/* Explicit init/bench helpers only — wire_flow_decoder must not call these. */
int RsCodec_set_profile_from_shard_count(uint16_t shard_count);
int RsCodec_profile_is_wire_shard_count(uint16_t shard_count);

int RsCodec_prepare_matrix(void);
int RsCodec_matrix_ready(void);
uint64_t RsCodec_matrix_init_ns(void);

/*
 * Encode implementation selection (tests / rs_encode_bench only).
 * AUTO: AVX2 nibble-shuffle for any geometry when available, otherwise
 *       optimized general scalar table plan.
 * GENERAL: force optimized scalar plan/table path for any geometry.
 * SIMD: use AVX2 when available, otherwise safely fall back to GENERAL.
 * FAST_16_2: require k=16,r=2 specialized path (else GENERAL).
 * LEGACY: reference byte-wise gmult against published plan coeffs.
 * RSCODE: explicit hqm/rscode encode_data for 4+2 only (locked; reference).
 */
typedef enum RsEncodeImpl {
    RS_ENCODE_AUTO = 0,
    RS_ENCODE_GENERAL,
    RS_ENCODE_SIMD,
    RS_ENCODE_FAST_16_2,
    RS_ENCODE_LEGACY,
    RS_ENCODE_RSCODE
} RsEncodeImpl;

typedef struct RsEncodeStats {
    uint64_t encode_calls;
    uint64_t rebuild_calls;
    uint64_t general_path_calls;
    uint64_t simd_path_calls;
    uint64_t fast_path_calls;
    uint64_t legacy_path_calls;
    uint64_t rscode_path_calls;
    uint64_t lock_wait_ns;
    uint64_t lock_hold_ns;
    uint64_t encode_body_ns;
    uint64_t gmul_calls;
} RsEncodeStats;

void RsCodec_set_encode_impl(RsEncodeImpl impl);
RsEncodeImpl RsCodec_get_encode_impl(void);
RsEncodeImpl RsCodec_last_encode_impl(void);
void RsCodec_reset_encode_stats(void);
void RsCodec_get_encode_stats(RsEncodeStats *out);

/* Times `iters` GF(256) multiplies with the same gmult() used by encode. */
uint64_t RsCodec_calibrate_gmul_ns(uint64_t iters);

/* Encode-plan introspection for tests. */
int RsCodec_encode_plan_ready(void);
int RsCodec_encode_plan_has_mul_table(void);
int RsCodec_encode_plan_has_nibble_table(void);
int RsCodec_avx2_available(void);
/* Test/benchmark hook; set before starting encode workers. */
void RsCodec_set_simd_enabled_for_tests(int enabled);
void RsCodec_get_encode_plan_geometry(size_t *k, size_t *r, size_t *n,
                                      size_t *shard_bytes);

#endif /* RS_CODEC_H */
