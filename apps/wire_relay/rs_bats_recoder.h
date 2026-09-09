#ifndef WIRE_RELAY_RS_BATS_RECODER_H
#define WIRE_RELAY_RS_BATS_RECODER_H

#include "generation_cache.h"

#include <stddef.h>
#include <stdint.h>

/*
 * BATS-style recoder over one RS wire generation (n = shard_count shards).
 *
 * Treat shard payloads as an n x payload_len matrix M in GF(256).  Along the
 * column axis, M is split into blocks of width block_dim.  Each block B
 * (n x w, w <= block_dim) is zero-padded on the right to n x block_dim, then
 * left-multiplied by an n x n coding matrix R:
 *
 *   B'_pad = R × B_pad     (matrix × matrix)
 *   write back the first w columns of B'_pad
 *
 * Default: block_dim = n so blocks are square (advisor-style 16x16 when n=16;
 * RS default n=6 yields 6x6 blocks).  Identity R leaves payload unchanged.
 */

/* 0 in config_defaults means "use shard_count" (square blocks). */
#define RS_BATS_DEFAULT_BLOCK_DIM 0u

typedef struct RsBatsRecoderConfig {
    uint16_t shard_count; /* n: rows / coding-matrix order */
    uint16_t payload_len; /* columns of M */
    uint16_t block_dim;   /* block width; 0 => n (square n x n blocks) */
} RsBatsRecoderConfig;

typedef struct RsBatsRecoderStats {
    uint64_t generations_recoded;
    uint64_t recode_body_ns;
    uint64_t recode_mismatch;
    uint64_t hold_packets;
    uint64_t emit_datagrams;
    uint64_t emit_fail;
} RsBatsRecoderStats;

void rs_bats_recoder_config_defaults(RsBatsRecoderConfig *cfg,
                                     uint16_t shard_count);

/* Resolved block width used by recode (never 0). */
uint16_t rs_bats_recoder_block_dim(const RsBatsRecoderConfig *cfg);

/*
 * out_rows[i] must point to payload_len writable bytes.
 * matrix is row-major n x n over GF(256): M' blocks = R × padded blocks.
 */
int rs_bats_recode(const RsBatsRecoderConfig *cfg,
                   const uint8_t *matrix,
                   const uint8_t *const *in_rows,
                   uint8_t *const *out_rows);

int rs_bats_recode_identity(const RsBatsRecoderConfig *cfg,
                            const uint8_t *const *in_rows,
                            uint8_t *const *out_rows);

/*
 * Gather shard payloads from a GEN_READY generation entry.  out_rows[i] receives
 * a pointer into the cached datagram copy (no extra allocation).  Returns 0 when
 * every shard_index in [0, shard_count) is present.
 */
int rs_bats_generation_payload_rows(const GenerationEntry *gen,
                                    const uint8_t **out_rows);

/*
 * Identity-recodes a complete generation into caller-provided row buffers.
 * stats may be NULL.
 */
int rs_bats_recode_generation_identity(const RsBatsRecoderConfig *cfg,
                                       const GenerationEntry *gen,
                                       uint8_t *const *out_rows,
                                       RsBatsRecoderStats *stats);

#endif /* WIRE_RELAY_RS_BATS_RECODER_H */
