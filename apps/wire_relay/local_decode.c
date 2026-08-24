#include "local_decode.h"

#include "stream_config.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

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

static int local_decode_ack_output(const WireHeader *ack, void *ctx)
{
    LocalDecodeHub *hub = ctx;
    uint8_t datagram[WIRE_V4_HEADER_SIZE];
    ssize_t sent;

    if (hub == NULL || ack == NULL || hub->ack_sock < 0 ||
        hub->ack_target_len == 0) {
        return -1;
    }
    wire_header_encode_v4(datagram, ack);
    do {
        sent = sendto(hub->ack_sock, datagram, sizeof(datagram), 0,
                      (const struct sockaddr *)&hub->ack_target,
                      hub->ack_target_len);
    } while (sent < 0 && errno == EINTR);
    return sent == (ssize_t)sizeof(datagram) ? 0 : -1;
}

static int open_ack_target(LocalDecodeHub *hub, const char *host,
                           uint16_t port)
{
    struct addrinfo hints;
    struct addrinfo *results = NULL;
    struct addrinfo *entry;
    char port_text[16];

    if (hub == NULL || host == NULL || port == 0) {
        return -1;
    }
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    snprintf(port_text, sizeof(port_text), "%u", (unsigned)port);
    if (getaddrinfo(host, port_text, &hints, &results) != 0) {
        return -1;
    }
    for (entry = results; entry != NULL; entry = entry->ai_next) {
        if (entry->ai_addrlen > sizeof(hub->ack_target)) {
            continue;
        }
        hub->ack_sock = socket(entry->ai_family, SOCK_DGRAM, 0);
        if (hub->ack_sock >= 0) {
            memcpy(&hub->ack_target, entry->ai_addr, entry->ai_addrlen);
            hub->ack_target_len = (socklen_t)entry->ai_addrlen;
            freeaddrinfo(results);
            return 0;
        }
    }
    freeaddrinfo(results);
    return -1;
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

    if (hub->wirehair_mode) {
        flow->wirehair_dec = wirehair_segment_receiver_create(
            &hub->wirehair, flow_id, local_decode_file_output, flow,
            hub->wirehair.ack_enabled ? local_decode_ack_output : NULL,
            hub);
        return flow->wirehair_dec != NULL ? 0 : -1;
    }
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
    hub->ack_sock = -1;

    if (cfg->mode != LOCAL_DECODE_MODE_SINGLE_FILE &&
        cfg->mode != LOCAL_DECODE_MODE_OUTPUT_DIR) {
        return -1;
    }

    if (pthread_mutex_init(&hub->mu, NULL) != 0) {
        return -1;
    }
    hub->mu_inited = 1;

    hub->mode = cfg->mode;
    hub->local_node_id = cfg->local_node_id;
    hub->wirehair_mode = cfg->codec_kind == CODEC_KIND_WIREHAIR;
    hub->wirehair = cfg->wirehair;
    if (hub->wirehair_mode) {
        if (!wirehair_segment_config_valid(&hub->wirehair)) {
            goto fail;
        }
        if (hub->wirehair.ack_enabled &&
            open_ack_target(hub, cfg->return_hop_host,
                            cfg->return_hop_port) != 0) {
            goto fail;
        }
        hub->best_effort = 0;
        goto output_setup;
    }
    hub->codec = Codec_get(cfg->codec_kind);
    if (hub->codec == NULL) {
        goto fail;
    }
    if (cfg->best_effort && !Codec_allows_best_effort(hub->codec)) {
        goto fail;
    }
    hub->best_effort = cfg->best_effort;
    hub->input_size = Codec_input_block_size(hub->codec);
    output_size = Codec_output_block_size(hub->codec);
    if (hub->input_size == 0 || output_size == 0 ||
        output_size > CODEC_MAX_ENCODE_BLOCK || output_size % PKG_SIZE != 0) {
        goto fail;
    }
    hub->expected_shards = (uint16_t)(output_size / PKG_SIZE);
    if (hub->expected_shards == 0 ||
        hub->expected_shards > WIRE_FLOW_MAX_SHARDS ||
        hub->expected_shards !=
            Codec_data_shards(hub->codec) + Codec_parity_shards(hub->codec)) {
        goto fail;
    }

output_setup:
    if (cfg->mode == LOCAL_DECODE_MODE_SINGLE_FILE) {
        if (cfg->output != NULL) {
            hub->single_output = cfg->output;
            hub->single_close_output = 0;
        } else if (cfg->output_path != NULL) {
            hub->single_output = fopen(cfg->output_path, "wb");
            if (hub->single_output == NULL) {
                goto fail;
            }
            hub->single_close_output = 1;
        } else {
            goto fail;
        }
    } else {
        if (cfg->output_dir == NULL || validate_output_dir(cfg->output_dir) != 0) {
            goto fail;
        }
        dir_len = strlen(cfg->output_dir);
        if (dir_len == 0 || dir_len >= sizeof(hub->output_dir)) {
            goto fail;
        }
        memcpy(hub->output_dir, cfg->output_dir, dir_len + 1u);
    }
    return 0;

fail:
    if (hub->ack_sock >= 0) {
        close(hub->ack_sock);
        hub->ack_sock = -1;
    }
    if (hub->single_close_output && hub->single_output != NULL) {
        fclose(hub->single_output);
        hub->single_output = NULL;
    }
    if (hub->mu_inited) {
        pthread_mutex_destroy(&hub->mu);
        hub->mu_inited = 0;
    }
    memset(hub, 0, sizeof(*hub));
    return -1;
}

void local_decode_hub_destroy(LocalDecodeHub *hub)
{
    size_t i;

    if (hub == NULL) {
        return;
    }
    /*
     * Caller must guarantee no concurrent delivery (after relay_run /
     * harness close). Lock once to serialize vs a stray late call, then
     * tear down under the lock before destroying the mutex.
     */
    if (hub->mu_inited) {
        pthread_mutex_lock(&hub->mu);
    }
    for (i = 0; i < RELAY_MAX_FLOWS; i++) {
        LocalDecodeFlow *flow = &hub->flows[i];

        if (!flow->active && flow->dec == NULL && flow->output == NULL) {
            continue;
        }
        wire_flow_decoder_destroy(flow->dec);
        flow->dec = NULL;
        wirehair_segment_receiver_destroy(flow->wirehair_dec);
        flow->wirehair_dec = NULL;
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
    if (hub->ack_sock >= 0) {
        close(hub->ack_sock);
        hub->ack_sock = -1;
    }
    if (hub->mu_inited) {
        pthread_mutex_unlock(&hub->mu);
        pthread_mutex_destroy(&hub->mu);
        hub->mu_inited = 0;
    }
    memset(hub, 0, sizeof(*hub));
}

static size_t active_count_unlocked(const LocalDecodeHub *hub)
{
    size_t i;
    size_t n = 0;

    for (i = 0; i < RELAY_MAX_FLOWS; i++) {
        if (hub->flows[i].active) {
            n++;
        }
    }
    return n;
}

__attribute__((weak)) void local_decode_test_locked_enter(void) {}
__attribute__((weak)) void local_decode_test_locked_leave(void) {}

int local_decode_hub_delivery(const uint8_t *datagram, size_t len,
                              const WireHeader *hdr, void *ctx)
{
    LocalDecodeHub *hub = ctx;
    LocalDecodeFlow *flow;
    const uint8_t *payload;
    size_t payload_len;
    int capacity_reject = 0;
    int rc = 0;

    if (hub == NULL || !hub->mu_inited || datagram == NULL || hdr == NULL ||
        len < WIRE_HEADER_SIZE) {
        return -1;
    }

    pthread_mutex_lock(&hub->mu);
    local_decode_test_locked_enter();

    if (!wire_header_is_local(hdr, hub->local_node_id)) {
        hub->stats.metadata_mismatch++;
        fprintf(stderr,
                "local_decode: non-local packet delivered to local decoder "
                "(final_dst=%u local_node_id=%u)\n",
                (unsigned)hdr->final_dst,
                (unsigned)hub->local_node_id);
        rc = -1;
        goto out;
    }

    if (hdr->type != WIRE_TYPE_DATA && hdr->type != WIRE_TYPE_END) {
        hub->stats.metadata_mismatch++;
        fprintf(stderr,
                "local_decode: decode metadata mismatch (type=%u)\n",
                (unsigned)hdr->type);
        rc = 0;
        goto out;
    }

    if (!hub->wirehair_mode &&
        !wire_flow_decoder_shard_count_ok(hub->codec, hdr->shard_count,
                                          hub->expected_shards)) {
        hub->stats.metadata_mismatch++;
        fprintf(stderr,
                "local_decode: decode metadata mismatch "
                "(shard_count=%u expected=%u)\n",
                (unsigned)hdr->shard_count, (unsigned)hub->expected_shards);
        rc = 0;
        goto out;
    }

    flow = find_flow(hub, hdr->flow_id);
    if (flow == NULL) {
        if (hub->mode == LOCAL_DECODE_MODE_SINGLE_FILE &&
            active_count_unlocked(hub) > 0) {
            hub->stats.flow_rejected++;
            fprintf(stderr,
                    "local_decode_flow_rejected: flow_id=%u "
                    "(L1 --output FILE single-flow only; use --output-dir)\n",
                    (unsigned)hdr->flow_id);
            rc = 0;
            goto out;
        }

        flow = bind_new_flow(hub, hdr->flow_id, &capacity_reject);
        if (flow == NULL) {
            if (capacity_reject) {
                hub->stats.flow_rejected++;
                fprintf(stderr,
                        "local_decode_flow_rejected: flow_id=%u "
                        "capacity=%u (no free LocalDecodeFlow slot)\n",
                        (unsigned)hdr->flow_id, (unsigned)RELAY_MAX_FLOWS);
                rc = -1;
                goto out;
            }
            hub->stats.ingest_error++;
            rc = -1;
            goto out;
        }
    }

    if (hdr->type == WIRE_TYPE_DATA) {
        size_t header_size = wire_header_size(hdr);

        if (len != header_size + hdr->payload_len ||
            (!hub->wirehair_mode && hdr->payload_len != PKG_SIZE) ||
            (hub->wirehair_mode &&
             (hdr->version != WIRE_VERSION_V4 ||
              hdr->payload_len == 0 || hdr->payload_len > PKG_SIZE))) {
            hub->stats.metadata_mismatch++;
            flow->metadata_mismatch++;
            fprintf(stderr,
                    "local_decode: decode metadata mismatch "
                    "(DATA len/payload)\n");
            rc = 0;
            goto out;
        }
        payload = datagram + header_size;
        payload_len = hdr->payload_len;
    } else {
        if (len != wire_header_size(hdr) || hdr->payload_len != 0 ||
            (hub->wirehair_mode && hdr->version != WIRE_VERSION_V4)) {
            hub->stats.metadata_mismatch++;
            flow->metadata_mismatch++;
            fprintf(stderr,
                    "local_decode: decode metadata mismatch "
                    "(END len/payload)\n");
            rc = 0;
            goto out;
        }
        payload = NULL;
        payload_len = 0;
    }

    if ((hub->wirehair_mode &&
         wirehair_segment_receiver_ingest(flow->wirehair_dec, hdr, payload,
                                           payload_len) != 0) ||
        (!hub->wirehair_mode &&
         wire_flow_decoder_ingest(flow->dec, hdr, payload,
                                  payload_len) != 0)) {
        flow->ingest_error++;
        hub->stats.ingest_error++;
        rc = -1;
        goto out;
    }
    flow->delivered++;
    hub->stats.delivered++;
    rc = 0;

out:
    local_decode_test_locked_leave();
    pthread_mutex_unlock(&hub->mu);
    return rc;
}

int local_decode_hub_get_stats(LocalDecodeHub *hub, LocalDecodeHubStats *out)
{
    if (hub == NULL || out == NULL || !hub->mu_inited) {
        return -1;
    }
    pthread_mutex_lock(&hub->mu);
    *out = hub->stats;
    pthread_mutex_unlock(&hub->mu);
    return 0;
}

size_t local_decode_hub_active_count(LocalDecodeHub *hub)
{
    size_t n;

    if (hub == NULL || !hub->mu_inited) {
        return 0;
    }
    pthread_mutex_lock(&hub->mu);
    n = active_count_unlocked(hub);
    pthread_mutex_unlock(&hub->mu);
    return n;
}

static int local_flow_complete(const LocalDecodeHub *hub,
                               const LocalDecodeFlow *flow)
{
    if (hub->wirehair_mode) {
        return flow->wirehair_dec != NULL &&
               wirehair_segment_receiver_complete(flow->wirehair_dec);
    }
    return flow->dec != NULL && wire_flow_decoder_is_complete(flow->dec);
}

int local_decode_hub_is_complete(LocalDecodeHub *hub)
{
    size_t i;
    size_t active = 0;
    int complete = 0;

    if (hub == NULL || !hub->mu_inited) {
        return 0;
    }
    pthread_mutex_lock(&hub->mu);
    for (i = 0; i < RELAY_MAX_FLOWS; i++) {
        const LocalDecodeFlow *flow = &hub->flows[i];

        if (!flow->active) {
            continue;
        }
        active++;
        if (!local_flow_complete(hub, flow)) {
            complete = 0;
            goto out;
        }
    }
    complete = active > 0;
out:
    pthread_mutex_unlock(&hub->mu);
    return complete;
}

int local_decode_hub_strict_check(LocalDecodeHub *hub)
{
    const LocalDecodeHubStats *st;
    size_t i;
    int rc = 0;

    if (hub == NULL || !hub->mu_inited) {
        return -1;
    }
    pthread_mutex_lock(&hub->mu);
    st = &hub->stats;
    if (st->ingest_error != 0 || st->metadata_mismatch != 0 ||
        st->flow_rejected != 0) {
        rc = -1;
        goto out;
    }
    for (i = 0; i < RELAY_MAX_FLOWS; i++) {
        const LocalDecodeFlow *flow = &hub->flows[i];

        if (!flow->active) {
            continue;
        }
        if (flow->ingest_error != 0) {
            rc = -1;
            goto out;
        }
        if (!local_flow_complete(hub, flow)) {
            rc = -1;
            goto out;
        }
    }
    rc = 0;
out:
    pthread_mutex_unlock(&hub->mu);
    return rc;
}
