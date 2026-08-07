#include "local_decode.h"

#include "stream_config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int local_decode_file_output(uint32_t flow_id, const uint8_t *data,
                                    size_t len, void *ctx)
{
    LocalDecodeHub *hub = ctx;

    (void)flow_id;
    if (hub == NULL || hub->output == NULL || data == NULL) {
        return -1;
    }
    if (fwrite(data, 1, len, hub->output) != len) {
        return -1;
    }
    if (fflush(hub->output) != 0) {
        return -1;
    }
    return 0;
}

int local_decode_hub_init(LocalDecodeHub *hub, const LocalDecodeHubConfig *cfg)
{
    size_t output_size;

    if (hub == NULL || cfg == NULL) {
        return -1;
    }
    memset(hub, 0, sizeof(*hub));

    hub->local_node_id = cfg->local_node_id;
    hub->codec = Codec_get(cfg->codec_kind);
    if (hub->codec == NULL) {
        return -1;
    }
    if (cfg->best_effort && !Codec_is_systematic(hub->codec)) {
        return -1;
    }
    hub->best_effort = cfg->best_effort;
    hub->input_size = Codec_input_block_size(hub->codec);
    output_size = Codec_output_block_size(hub->codec);
    if (hub->input_size == 0 || output_size == 0 ||
        output_size > CODEC_MAX_ENCODE_BLOCK || output_size % PKG_SIZE != 0) {
        return -1;
    }
    hub->expected_shards = (uint16_t)(output_size / PKG_SIZE);
    if (hub->expected_shards == 0 ||
        hub->expected_shards > WIRE_FLOW_MAX_SHARDS ||
        hub->expected_shards !=
            Codec_data_shards(hub->codec) + Codec_parity_shards(hub->codec)) {
        return -1;
    }

    if (cfg->output != NULL) {
        hub->output = cfg->output;
        hub->close_output = 0;
    } else if (cfg->output_path != NULL) {
        hub->output = fopen(cfg->output_path, "wb");
        if (hub->output == NULL) {
            return -1;
        }
        hub->close_output = 1;
    } else {
        return -1;
    }
    return 0;
}

void local_decode_hub_destroy(LocalDecodeHub *hub)
{
    if (hub == NULL) {
        return;
    }
    wire_flow_decoder_destroy(hub->dec);
    hub->dec = NULL;
    if (hub->close_output && hub->output != NULL) {
        fclose(hub->output);
    }
    hub->output = NULL;
    memset(hub, 0, sizeof(*hub));
}

static int ensure_decoder(LocalDecodeHub *hub, uint32_t flow_id)
{
    WireFlowDecoderConfig cfg;

    if (hub->dec != NULL) {
        return 0;
    }
    memset(&cfg, 0, sizeof(cfg));
    cfg.flow_id = flow_id;
    cfg.codec = hub->codec;
    cfg.expected_shards = hub->expected_shards;
    cfg.best_effort = hub->best_effort;
    cfg.input_size = hub->input_size;
    cfg.output_fn = local_decode_file_output;
    cfg.output_ctx = hub;
    hub->dec = wire_flow_decoder_create(&cfg);
    return hub->dec != NULL ? 0 : -1;
}

int local_decode_hub_delivery(const uint8_t *datagram, size_t len,
                              const WireHeader *hdr, void *ctx)
{
    LocalDecodeHub *hub = ctx;
    const uint8_t *payload;
    size_t payload_len;

    if (hub == NULL || datagram == NULL || hdr == NULL ||
        len < WIRE_HEADER_SIZE) {
        return -1;
    }

    if (!wire_header_is_local(hdr, hub->local_node_id)) {
        hub->stats.metadata_mismatch++;
        fprintf(stderr,
                "local_decode: non-local packet delivered to local decoder "
                "(final_dst=%u local_node_id=%u)\n",
                (unsigned)hdr->final_dst,
                (unsigned)hub->local_node_id);
        return -1;
    }

    if (hdr->type != WIRE_TYPE_DATA && hdr->type != WIRE_TYPE_END) {
        hub->stats.metadata_mismatch++;
        fprintf(stderr,
                "local_decode: decode metadata mismatch (type=%u)\n",
                (unsigned)hdr->type);
        return 0;
    }

    if (!wire_flow_decoder_shard_count_ok(hub->codec, hdr->shard_count,
                                          hub->expected_shards)) {
        hub->stats.metadata_mismatch++;
        fprintf(stderr,
                "local_decode: decode metadata mismatch "
                "(shard_count=%u expected=%u)\n",
                (unsigned)hdr->shard_count, (unsigned)hub->expected_shards);
        return 0;
    }

    if (hub->flow_bound) {
        if (hdr->flow_id != hub->bound_flow_id) {
            /* L1: single --output file; multi-flow needs L2 --output-dir. */
            hub->stats.flow_rejected++;
            fprintf(stderr,
                    "local_decode_flow_rejected: flow_id=%u bound=%u "
                    "(L1 single-flow only; multi-flow output-dir is L2)\n",
                    (unsigned)hdr->flow_id, (unsigned)hub->bound_flow_id);
            return 0;
        }
    } else {
        hub->bound_flow_id = hdr->flow_id;
        hub->flow_bound = 1;
        if (ensure_decoder(hub, hdr->flow_id) != 0) {
            hub->stats.ingest_error++;
            return -1;
        }
    }

    if (hub->dec == NULL && ensure_decoder(hub, hub->bound_flow_id) != 0) {
        hub->stats.ingest_error++;
        return -1;
    }

    if (hdr->type == WIRE_TYPE_DATA) {
        if (len != WIRE_HEADER_SIZE + hdr->payload_len ||
            hdr->payload_len != PKG_SIZE) {
            hub->stats.metadata_mismatch++;
            fprintf(stderr,
                    "local_decode: decode metadata mismatch "
                    "(DATA len/payload)\n");
            return 0;
        }
        payload = datagram + WIRE_HEADER_SIZE;
        payload_len = hdr->payload_len;
    } else {
        if (len != WIRE_HEADER_SIZE || hdr->payload_len != 0) {
            hub->stats.metadata_mismatch++;
            fprintf(stderr,
                    "local_decode: decode metadata mismatch "
                    "(END len/payload)\n");
            return 0;
        }
        payload = NULL;
        payload_len = 0;
    }

    if (wire_flow_decoder_ingest(hub->dec, hdr, payload, payload_len) != 0) {
        hub->stats.ingest_error++;
        return -1;
    }
    hub->stats.delivered++;
    return 0;
}

const LocalDecodeHubStats *local_decode_hub_stats(const LocalDecodeHub *hub)
{
    return hub != NULL ? &hub->stats : NULL;
}

int local_decode_hub_is_complete(const LocalDecodeHub *hub)
{
    return hub != NULL && hub->dec != NULL &&
           wire_flow_decoder_is_complete(hub->dec);
}

int local_decode_hub_strict_check(const LocalDecodeHub *hub)
{
    const LocalDecodeHubStats *st;

    if (hub == NULL) {
        return -1;
    }
    st = &hub->stats;
    if (st->ingest_error != 0 || st->metadata_mismatch != 0 ||
        st->flow_rejected != 0) {
        return -1;
    }
    if (hub->flow_bound && !local_decode_hub_is_complete(hub)) {
        return -1;
    }
    return 0;
}
