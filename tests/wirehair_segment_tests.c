#include "wirehair_segment.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct TestCtx {
    WirehairSegmentReceiver *receiver;
    uint8_t *output;
    size_t output_size;
    size_t output_capacity;
    unsigned ack_count;
} TestCtx;

static int collect_output(uint32_t flow_id, const uint8_t *data, size_t len,
                          void *opaque)
{
    TestCtx *ctx = opaque;

    (void)flow_id;
    if (ctx == NULL || ctx->output_size + len > ctx->output_capacity) {
        return -1;
    }
    memcpy(ctx->output + ctx->output_size, data, len);
    ctx->output_size += len;
    return 0;
}

static int collect_ack(const WireHeader *ack, void *opaque)
{
    TestCtx *ctx = opaque;

    if (ctx == NULL || ack == NULL || ack->type != WIRE_TYPE_ACK ||
        (ack->flags & WIRE_FLAG_RETURN_PATH) == 0) {
        return -1;
    }
    ctx->ack_count++;
    return 0;
}

static int loopback_emit(const WireHeader *header, const uint8_t *payload,
                         size_t payload_len, void *opaque)
{
    TestCtx *ctx = opaque;
    int result;

    result = wirehair_segment_receiver_ingest(ctx->receiver, header, payload,
                                              payload_len);
    if (result != 0) {
        fprintf(stderr, "ingest failed packet=%u/%u len=%zu\n",
                header->shard_index, header->shard_count, payload_len);
    }
    return result;
}

static int loopback_ack_poll(uint32_t flow_id, uint64_t segment_id,
                             unsigned wait_ms, void *opaque)
{
    TestCtx *ctx = opaque;

    (void)flow_id;
    (void)segment_id;
    (void)wait_ms;
    return ctx->ack_count > 0 ? 1 : 0;
}

static int run_roundtrip(size_t bytes, int ack_enabled)
{
    WirehairSegmentConfig config;
    WirehairSegmentSendStats stats;
    TestCtx ctx;
    uint8_t *input = NULL;
    WireHeader end;
    size_t i;
    int result = -1;

    wirehair_segment_config_defaults(&config);
    config.segment_bytes = 1024u * 1024u;
    config.ack_enabled = ack_enabled != 0;
    memset(&ctx, 0, sizeof(ctx));
    input = malloc(bytes);
    ctx.output = malloc(bytes);
    ctx.output_capacity = bytes;
    if (input == NULL || ctx.output == NULL) {
        goto out;
    }
    for (i = 0; i < bytes; i++) {
        input[i] = (uint8_t)(i * 37u + i / 19u);
    }
    ctx.receiver = wirehair_segment_receiver_create(
        &config, 7u, collect_output, &ctx, collect_ack, &ctx);
    if (ctx.receiver == NULL) {
        fprintf(stderr, "receiver create failed bytes=%zu\n", bytes);
        goto out;
    }
    if (wirehair_segment_send(
            &config, 7u, 0u, 4u, 8u, input, bytes, loopback_emit, &ctx,
            ack_enabled ? loopback_ack_poll : NULL, &ctx, &stats) != 0) {
        fprintf(stderr, "segment send failed bytes=%zu\n", bytes);
        goto out;
    }
    end = (WireHeader){
        .version = WIRE_VERSION_V4,
        .type = WIRE_TYPE_END,
        .final_dst = 4u,
        .ttl = 8u,
        .flow_id = 7u,
        .block_id = 1u,
        .origin_node = 1u,
    };
    if (wirehair_segment_receiver_ingest(ctx.receiver, &end, NULL, 0) != 0) {
        fprintf(stderr, "end ingest failed bytes=%zu\n", bytes);
        goto out;
    }
    if (!wirehair_segment_receiver_complete(ctx.receiver) ||
        ctx.output_size != bytes || memcmp(input, ctx.output, bytes) != 0 ||
        (ack_enabled &&
         (ctx.ack_count != 1u || !stats.stopped_by_ack ||
          stats.repair_sent != 0u))) {
        fprintf(stderr,
                "roundtrip mismatch bytes=%zu complete=%d output=%zu ack=%u\n",
                bytes, wirehair_segment_receiver_complete(ctx.receiver),
                ctx.output_size, ctx.ack_count);
        goto out;
    }
    result = 0;
out:
    wirehair_segment_receiver_destroy(ctx.receiver);
    free(input);
    free(ctx.output);
    return result;
}

int main(void)
{
    WirehairSegmentConfig config;

    wirehair_segment_config_defaults(&config);
    if (!wirehair_segment_config_valid(&config) ||
        wirehair_segment_source_packets(10u * 1024u * 1024u) != 7490u ||
        wirehair_segment_repair_packets(7490u, 10u) != 749u ||
        wirehair_segment_repair_packets(7490u, 0u) != 0u ||
        wirehair_segment_repair_packets(3u, 100u) != 3u ||
        run_roundtrip(28000u, 0) != 0 ||
        run_roundtrip(731u, 1) != 0) {
        fprintf(stderr, "wirehair segment tests failed\n");
        return 1;
    }
    puts("wirehair segment tests passed");
    return 0;
}
