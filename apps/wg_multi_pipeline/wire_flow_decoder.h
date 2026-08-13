#ifndef WIRE_FLOW_DECODER_H
#define WIRE_FLOW_DECODER_H

/*
 * Per-flow wire shard reassembly + Codec_recover/Codec_decode.
 *
 * Architecture (Phase A/C): Receive / Recover / Emit are separate.
 *
 * Receive:
 *   - block_id < next_emit_block  → truly late (already emitted / skipped)
 *   - block_id == next_emit_block → current head
 *   - next_emit_block < block_id < next_emit_block + WIRE_FLOW_GROUP_WINDOW
 *     → future group, accepted if a slot exists
 *   - block_id >= next_emit_block + WIRE_FLOW_GROUP_WINDOW, or no free slot:
 *       strict → window_overflow
 *       best-effort → skip stuck heads until the block fits (make_room);
 *         still window_overflow if it cannot
 *
 * Recover:
 *   - Each group independently; triggered only by a new non-duplicate shard
 *     (or once more at END finalize / make_room last-chance).
 *   - Independent of next_emit_block.
 *
 * Emit:
 *   - Ordered: only the current next_emit_block is written next.
 *   - Strict: emit only RECOVERED; missing/FAILED head stalls (hash PASS iff
 *     next_emit_block == end_block_count with no gaps).
 *   - Best-effort: after END, skip missing/FAILED head (skipped_groups).
 *     Mid-stream, skip stuck heads only when a new block cannot enter the
 *     reorder window. Optionally write systematic data shards that arrived,
 *     then emit later RECOVERED groups in order. Hash is expected to FAIL;
 *     use skip/byte gap metrics.
 */

#include "codec.h"
#include "stream_config.h"
#include "wire_header.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define WIRE_FLOW_GROUP_WINDOW 128u
#define WIRE_FLOW_MAX_SHARDS   (CODEC_MAX_ENCODE_BLOCK / PKG_SIZE)

typedef int (*WireDecodeOutputFn)(uint32_t flow_id, const uint8_t *data,
                                  size_t len, void *ctx);

typedef struct WireFlowDecoderStats {
    uint64_t output_bytes;
    uint64_t seen_datagrams;
    uint64_t received_datagrams;
    uint64_t duplicate_datagrams;
    uint64_t late_datagrams;
    uint64_t malformed_datagrams;
    uint64_t dropped_groups; /* compat; best-effort holes use skipped_groups */
    uint64_t recovered_groups; /* alias of groups_recovered (compat) */
    uint64_t missing_data_shards;
    uint64_t decoded_blocks;   /* alias of groups_emitted (compat) */
    uint64_t groups_received;
    uint64_t groups_recovered;
    uint64_t groups_emitted;
    uint64_t groups_failed;
    uint64_t window_overflow;
    uint64_t skipped_groups; /* best-effort: missing/FAILED/abandoned head */
    uint64_t pending_recovered_groups; /* snapshot; prefer helper below */
} WireFlowDecoderStats;

typedef struct WireFlowDecoder WireFlowDecoder;

typedef struct WireFlowDecoderConfig {
    uint32_t           flow_id;
    const Codec       *codec;
    /* Fixed at create: Codec_output_block_size(codec)/PKG_SIZE (== k+r for RS). */
    uint16_t           expected_shards;
    int                best_effort;
    size_t             input_size; /* Codec_input_block_size */
    WireDecodeOutputFn output_fn;
    void              *output_ctx;
} WireFlowDecoderConfig;

WireFlowDecoder *wire_flow_decoder_create(const WireFlowDecoderConfig *config);
void wire_flow_decoder_destroy(WireFlowDecoder *dec);

/*
 * Ingest one already-parsed wire datagram (DATA or END).
 * payload is header.payload_len bytes for DATA; ignored for END.
 * Returns 0 on handled (including malformed drop), -1 on hard I/O/codec error.
 */
int wire_flow_decoder_ingest(WireFlowDecoder *dec, const WireHeader *header,
                             const uint8_t *payload, size_t payload_len);

/* Best-effort: skip remaining holes after END and emit later recovered groups.
 * Mid-stream skip of stuck heads is done in ingest via make_room, not here.
 */
int wire_flow_decoder_flush_best_effort(WireFlowDecoder *dec);

bool wire_flow_decoder_is_complete(const WireFlowDecoder *dec);
bool wire_flow_decoder_end_seen(const WireFlowDecoder *dec);
uint64_t wire_flow_decoder_next_block(const WireFlowDecoder *dec);
uint64_t wire_flow_decoder_end_block_count(const WireFlowDecoder *dec);
const WireFlowDecoderStats *wire_flow_decoder_stats(const WireFlowDecoder *dec);
/* Recovered groups waiting for next_emit_block; not written to output yet. */
uint64_t wire_flow_decoder_pending_recovered_groups(const WireFlowDecoder *dec);

void wire_flow_decoder_print_latency(const WireFlowDecoder *dec);

/*
 * True iff wire_shard_count equals the decoder's fixed expected_shards.
 * Applies to every codec kind, including CODEC_KIND_RS: header.shard_count
 * is validation only and must not retune process RS geometry.
 */
int wire_flow_decoder_shard_count_ok(const Codec *codec, uint16_t shard_count,
                                     uint16_t expected_shards);

#endif /* WIRE_FLOW_DECODER_H */
