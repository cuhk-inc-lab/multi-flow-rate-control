#ifndef WIRE_FLOW_DECODER_H
#define WIRE_FLOW_DECODER_H

/*
 * Per-flow wire shard reassembly + Codec_recover/Codec_decode.
 * Extracted from the former static receiver path in wire_udp.c (L0).
 * Output is always via WireDecodeOutputFn (no FILE* inside the decoder).
 *
 * Emit conditions (ordered by next_block):
 * - all shards present, or
 * - systematic: every original data shard in [0, Codec_data_shards()) present
 *   (parity/pad may be missing), or
 * - FEC recover restores a full group.
 * received_count >= data_shards alone is never sufficient.
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
    uint64_t dropped_groups;
    uint64_t recovered_groups;
    uint64_t missing_data_shards;
    uint64_t decoded_blocks;
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

/* Best-effort flush remaining groups after END (systematic codecs only). */
int wire_flow_decoder_flush_best_effort(WireFlowDecoder *dec);

bool wire_flow_decoder_is_complete(const WireFlowDecoder *dec);
bool wire_flow_decoder_end_seen(const WireFlowDecoder *dec);
uint64_t wire_flow_decoder_next_block(const WireFlowDecoder *dec);
uint64_t wire_flow_decoder_end_block_count(const WireFlowDecoder *dec);
const WireFlowDecoderStats *wire_flow_decoder_stats(const WireFlowDecoder *dec);

void wire_flow_decoder_print_latency(const WireFlowDecoder *dec);

/*
 * True iff wire_shard_count equals the decoder's fixed expected_shards.
 * Applies to every codec kind, including CODEC_KIND_RS: header.shard_count
 * is validation only and must not retune process RS geometry.
 */
int wire_flow_decoder_shard_count_ok(const Codec *codec, uint16_t shard_count,
                                     uint16_t expected_shards);

#endif /* WIRE_FLOW_DECODER_H */
