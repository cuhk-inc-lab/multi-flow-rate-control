#include "local_decode.h"

#include "stream_config.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static int local_decode_file_output(uint32_t flow_id, const uint8_t *data,
                                    size_t len, void *ctx)
{
    LocalDecodeFlow *flow = ctx;

    (void)flow_id;
    if (flow == NULL || flow->output == NULL || data == NULL) {
        return -1;
    }
    if (fwrite(data, 1, len, flow->output) != len) {
        return -1;
    }
    if (fflush(flow->output) != 0) {
        return -1;
    }
    return 0;
}

static int validate_output_dir(const char *dir)
{
    struct stat st;

    if (dir == NULL || dir[0] == '\0') {
        return -1;
    }
    if (stat(dir, &st) != 0) {
        return -1;
    }
    if (!S_ISDIR(st.st_mode)) {
        return -1;
    }
    return 0;
}

static LocalDecodeFlow *find_flow(LocalDecodeHub *hub, uint32_t flow_id)
{
    size_t i;

    for (i = 0; i < RELAY_MAX_FLOWS; i++) {
        if (hub->flows[i].active && hub->flows[i].flow_id == flow_id) {
            return &hub->flows[i];
        }
    }
    return NULL;
}

static LocalDecodeFlow *find_free_slot(LocalDecodeHub *hub)
{
    size_t i;

    for (i = 0; i < RELAY_MAX_FLOWS; i++) {
        if (!hub->flows[i].active) {
            return &hub->flows[i];
        }
    }
    return NULL;
}

static int open_flow_output(LocalDecodeHub *hub, LocalDecodeFlow *flow,
                            uint32_t flow_id)
{
    char path[LOCAL_DECODE_PATH_MAX];
    int n;

    if (hub->mode == LOCAL_DECODE_MODE_SINGLE_FILE) {
        if (hub->single_output == NULL) {
            return -1;
        }
        flow->output = hub->single_output;
        flow->close_output = 0;
        return 0;
    }

    n = snprintf(path, sizeof(path), "%s/flow_%u.bin", hub->output_dir,
                 (unsigned)flow_id);
    if (n < 0 || (size_t)n >= sizeof(path)) {
        fprintf(stderr,
                "local_decode: output path truncated for flow_id=%u\n",
                (unsigned)flow_id);
        return -1;
    }
    flow->output = fopen(path, "wb");
    if (flow->output == NULL) {
        fprintf(stderr, "local_decode: fopen(%s) failed: %s\n", path,
                strerror(errno));
        return -1;
    }
    flow->close_output = 1;
    return 0;
}

static int create_flow_decoder(LocalDecodeHub *hub, LocalDecodeFlow *flow,
                               uint32_t flow_id)
{
    WireFlowDecoderConfig cfg;

    memset(&cfg, 0, sizeof(cfg));
    cfg.flow_id = flow_id;
    cfg.codec = hub->codec;
    cfg.expected_shards = hub->expected_shards;
    cfg.best_effort = hub->best_effort;
    cfg.input_size = hub->input_size;
    cfg.output_fn = local_decode_file_output;
    cfg.output_ctx = flow;
    flow->dec = wire_flow_decoder_create(&cfg);
    return flow->dec != NULL ? 0 : -1;
}

/*
 * Bind a new flow_id to a free slot. On failure returns NULL and sets
 * hub->stats (caller may need flow_rejected vs ingest_error).
 */
static LocalDecodeFlow *bind_new_flow(LocalDecodeHub *hub, uint32_t flow_id,
                                      int *capacity_reject)
{
    LocalDecodeFlow *flow;

    *capacity_reject = 0;

    if (hub->mode == LOCAL_DECODE_MODE_SINGLE_FILE) {
        size_t i;

        for (i = 0; i < RELAY_MAX_FLOWS; i++) {
            if (hub->flows[i].active) {
                /* L1: second distinct flow_id is an explicit rejection. */
                return NULL;
            }
        }
    }

    flow = find_free_slot(hub);
    if (flow == NULL) {
        *capacity_reject = 1;
        return NULL;
    }

    memset(flow, 0, sizeof(*flow));
    flow->flow_id = flow_id;
    if (open_flow_output(hub, flow, flow_id) != 0) {
        memset(flow, 0, sizeof(*flow));
        return NULL;
    }
    if (create_flow_decoder(hub, flow, flow_id) != 0) {
        if (flow->close_output && flow->output != NULL) {
            fclose(flow->output);
        }
        memset(flow, 0, sizeof(*flow));
        return NULL;
    }
    flow->active = 1;
    return flow;
}

int local_decode_hub_init(LocalDecodeHub *hub, const LocalDecodeHubConfig *cfg)
{
    size_t output_size;
    size_t dir_len;

    if (hub == NULL || cfg == NULL) {
        return -1;
    }
    memset(hub, 0, sizeof(*hub));

    if (cfg->mode != LOCAL_DECODE_MODE_SINGLE_FILE &&
        cfg->mode != LOCAL_DECODE_MODE_OUTPUT_DIR) {
        return -1;
    }

    hub->mode = cfg->mode;
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

    if (cfg->mode == LOCAL_DECODE_MODE_SINGLE_FILE) {
        if (cfg->output != NULL) {
            hub->single_output = cfg->output;
            hub->single_close_output = 0;
        } else if (cfg->output_path != NULL) {
            hub->single_output = fopen(cfg->output_path, "wb");
            if (hub->single_output == NULL) {
                return -1;
            }
            hub->single_close_output = 1;
        } else {
            return -1;
        }
    } else {
        if (cfg->output_dir == NULL || validate_output_dir(cfg->output_dir) != 0) {
            return -1;
        }
        dir_len = strlen(cfg->output_dir);
        if (dir_len == 0 || dir_len >= sizeof(hub->output_dir)) {
            return -1;
        }
        memcpy(hub->output_dir, cfg->output_dir, dir_len + 1u);
    }
    return 0;
}

void local_decode_hub_destroy(LocalDecodeHub *hub)
{
    size_t i;

    if (hub == NULL) {
        return;
    }
    for (i = 0; i < RELAY_MAX_FLOWS; i++) {
        LocalDecodeFlow *flow = &hub->flows[i];

        if (!flow->active && flow->dec == NULL && flow->output == NULL) {
            continue;
        }
        wire_flow_decoder_destroy(flow->dec);
        flow->dec = NULL;
        if (flow->close_output && flow->output != NULL) {
            fclose(flow->output);
        }
        flow->output = NULL;
        memset(flow, 0, sizeof(*flow));
    }
    if (hub->single_close_output && hub->single_output != NULL) {
        fclose(hub->single_output);
    }
    hub->single_output = NULL;
    memset(hub, 0, sizeof(*hub));
}

int local_decode_hub_delivery(const uint8_t *datagram, size_t len,
                              const WireHeader *hdr, void *ctx)
{
    LocalDecodeHub *hub = ctx;
    LocalDecodeFlow *flow;
    const uint8_t *payload;
    size_t payload_len;
    int capacity_reject = 0;

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

    flow = find_flow(hub, hdr->flow_id);
    if (flow == NULL) {
        if (hub->mode == LOCAL_DECODE_MODE_SINGLE_FILE &&
            local_decode_hub_active_count(hub) > 0) {
            hub->stats.flow_rejected++;
            fprintf(stderr,
                    "local_decode_flow_rejected: flow_id=%u "
                    "(L1 --output FILE single-flow only; use --output-dir)\n",
                    (unsigned)hdr->flow_id);
            return 0;
        }

        flow = bind_new_flow(hub, hdr->flow_id, &capacity_reject);
        if (flow == NULL) {
            if (capacity_reject) {
                hub->stats.flow_rejected++;
                fprintf(stderr,
                        "local_decode_flow_rejected: flow_id=%u "
                        "capacity=%u (no free LocalDecodeFlow slot)\n",
                        (unsigned)hdr->flow_id, (unsigned)RELAY_MAX_FLOWS);
                return -1;
            }
            hub->stats.ingest_error++;
            return -1;
        }
    }

    if (hdr->type == WIRE_TYPE_DATA) {
        if (len != WIRE_HEADER_SIZE + hdr->payload_len ||
            hdr->payload_len != PKG_SIZE) {
            hub->stats.metadata_mismatch++;
            flow->metadata_mismatch++;
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
            flow->metadata_mismatch++;
            fprintf(stderr,
                    "local_decode: decode metadata mismatch "
                    "(END len/payload)\n");
            return 0;
        }
        payload = NULL;
        payload_len = 0;
    }

    if (wire_flow_decoder_ingest(flow->dec, hdr, payload, payload_len) != 0) {
        flow->ingest_error++;
        hub->stats.ingest_error++;
        return -1;
    }
    flow->delivered++;
    hub->stats.delivered++;
    return 0;
}

const LocalDecodeHubStats *local_decode_hub_stats(const LocalDecodeHub *hub)
{
    return hub != NULL ? &hub->stats : NULL;
}

size_t local_decode_hub_active_count(const LocalDecodeHub *hub)
{
    size_t i;
    size_t n = 0;

    if (hub == NULL) {
        return 0;
    }
    for (i = 0; i < RELAY_MAX_FLOWS; i++) {
        if (hub->flows[i].active) {
            n++;
        }
    }
    return n;
}

int local_decode_hub_is_complete(const LocalDecodeHub *hub)
{
    size_t i;
    size_t active = 0;

    if (hub == NULL) {
        return 0;
    }
    for (i = 0; i < RELAY_MAX_FLOWS; i++) {
        const LocalDecodeFlow *flow = &hub->flows[i];

        if (!flow->active) {
            continue;
        }
        active++;
        if (flow->dec == NULL || !wire_flow_decoder_is_complete(flow->dec)) {
            return 0;
        }
    }
    return active > 0;
}

int local_decode_hub_strict_check(const LocalDecodeHub *hub)
{
    const LocalDecodeHubStats *st;
    size_t i;

    if (hub == NULL) {
        return -1;
    }
    st = &hub->stats;
    if (st->ingest_error != 0 || st->metadata_mismatch != 0 ||
        st->flow_rejected != 0) {
        return -1;
    }
    for (i = 0; i < RELAY_MAX_FLOWS; i++) {
        const LocalDecodeFlow *flow = &hub->flows[i];

        if (!flow->active) {
            continue;
        }
        if (flow->ingest_error != 0) {
            return -1;
        }
        if (flow->dec == NULL || !wire_flow_decoder_is_complete(flow->dec)) {
            return -1;
        }
    }
    return 0;
}
