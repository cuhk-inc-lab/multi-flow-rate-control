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

typedef struct PacketCopy {
    WireHeader header;
    uint8_t payload[WH_PACKET_SIZE];
    size_t payload_len;
} PacketCopy;

typedef struct PacketStore {
    PacketCopy packets[64];
    size_t count;
} PacketStore;

typedef struct DelayedAck {
    const PacketStore *store;
    size_t ack_after_packets;
    int never_ack;
} DelayedAck;

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

static int store_emit(const WireHeader *header, const uint8_t *payload,
                      size_t payload_len, void *opaque)
{
    PacketStore *store = opaque;

    if (store == NULL || store->count >=
                             sizeof(store->packets) / sizeof(store->packets[0]) ||
        payload_len > WH_PACKET_SIZE) {
        return -1;
    }
    store->packets[store->count].header = *header;
    if (payload != NULL && payload_len > 0) {
        memcpy(store->packets[store->count].payload, payload, payload_len);
    }
    store->packets[store->count].payload_len = payload_len;
    store->count++;
    return 0;
}

static int delayed_ack_poll(uint32_t flow_id, uint64_t segment_id,
                            unsigned wait_ms, void *opaque)
{
    DelayedAck *delay = opaque;

    (void)flow_id;
    (void)segment_id;
    (void)wait_ms;
    if (delay == NULL || delay->store == NULL || delay->never_ack) {
        return 0;
    }
    return delay->store->count >= delay->ack_after_packets ? 1 : 0;
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
         (ctx.ack_count < 1u || !stats.stopped_by_ack ||
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

static int test_outof_window_drop(void)
{
    WirehairSegmentConfig config;
    TestCtx ctx;
    PacketStore store;
    WirehairSegmentSendStats stats;
    uint8_t input[4096];
    size_t i;
    WireHeader ahead;
    int result = -1;

    wirehair_segment_config_defaults(&config);
    config.segment_bytes = 8192u;
    config.window = 2u;
    config.ack_enabled = false;
    memset(&ctx, 0, sizeof(ctx));
    memset(&store, 0, sizeof(store));
    for (i = 0; i < sizeof(input); i++) {
        input[i] = (uint8_t)(i + 3u);
    }
    ctx.output = malloc(sizeof(input));
    ctx.output_capacity = sizeof(input);
    if (ctx.output == NULL) {
        return -1;
    }
    ctx.receiver = wirehair_segment_receiver_create(
        &config, 3u, collect_output, &ctx, NULL, NULL);
    if (ctx.receiver == NULL) {
        goto out;
    }
    if (wirehair_segment_send(&config, 3u, 0u, 4u, 8u, input, sizeof(input),
                              store_emit, &store, NULL, NULL, &stats) != 0 ||
        store.count == 0) {
        goto out;
    }
    /* Fabricate a packet for segment_id far ahead of the window. */
    ahead = store.packets[0].header;
    ahead.block_id = 10u;
    if (wirehair_segment_receiver_ingest(ctx.receiver, &ahead,
                                         store.packets[0].payload,
                                         store.packets[0].payload_len) != 0) {
        fprintf(stderr, "out-of-window ingest should soft-drop\n");
        goto out;
    }
    /* In-window packets must still be accepted afterward. */
    for (i = 0; i < store.count; i++) {
        if (wirehair_segment_receiver_ingest(
                ctx.receiver, &store.packets[i].header,
                store.packets[i].payload, store.packets[i].payload_len) != 0) {
            fprintf(stderr, "in-window ingest failed after ahead drop\n");
            goto out;
        }
    }
    result = 0;
out:
    wirehair_segment_receiver_destroy(ctx.receiver);
    free(ctx.output);
    return result;
}

static int test_ack_repeat_after_recover(void)
{
    WirehairSegmentConfig config;
    TestCtx ctx;
    PacketStore store;
    WirehairSegmentSendStats stats;
    uint8_t input[4096];
    size_t i;
    int result = -1;

    wirehair_segment_config_defaults(&config);
    config.segment_bytes = 8192u;
    config.ack_enabled = true;
    memset(&ctx, 0, sizeof(ctx));
    memset(&store, 0, sizeof(store));
    for (i = 0; i < sizeof(input); i++) {
        input[i] = (uint8_t)(i + 11u);
    }
    ctx.output = malloc(sizeof(input));
    ctx.output_capacity = sizeof(input);
    if (ctx.output == NULL) {
        return -1;
    }
    ctx.receiver = wirehair_segment_receiver_create(
        &config, 5u, collect_output, &ctx, collect_ack, &ctx);
    if (ctx.receiver == NULL) {
        goto out;
    }
    /* No ack_poll: send up to the ACK safety cap so leftover repair exists. */
    if (wirehair_segment_send(&config, 5u, 0u, 4u, 8u, input, sizeof(input),
                              store_emit, &store, NULL, NULL, &stats) != 0 ||
        store.count < 3) {
        goto out;
    }
    for (i = 0; i < store.count; i++) {
        if (wirehair_segment_receiver_ingest(
                ctx.receiver, &store.packets[i].header,
                store.packets[i].payload, store.packets[i].payload_len) != 0) {
            goto out;
        }
        if (ctx.ack_count >= 1u && i + 1u < store.count) {
            if (wirehair_segment_receiver_ingest(
                    ctx.receiver, &store.packets[i + 1u].header,
                    store.packets[i + 1u].payload,
                    store.packets[i + 1u].payload_len) != 0) {
                goto out;
            }
            if (ctx.ack_count < 2u) {
                fprintf(stderr, "late packet did not re-ACK\n");
                goto out;
            }
            result = 0;
            goto out;
        }
    }
    fprintf(stderr, "did not recover/re-ACK ack=%u packets=%zu\n",
            ctx.ack_count, store.count);
out:
    wirehair_segment_receiver_destroy(ctx.receiver);
    free(ctx.output);
    return result;
}

static int test_ack_repair_rounds_and_timeout(void)
{
    WirehairSegmentConfig config;
    WirehairSegmentSendStats stats;
    PacketStore store;
    DelayedAck delay;
    uint8_t input[4096];
    uint32_t source_packets;
    size_t i;

    wirehair_segment_config_defaults(&config);
    config.segment_bytes = sizeof(input);
    config.repair_percent = 50u;
    config.ack_enabled = true;
    for (i = 0; i < sizeof(input); i++) {
        input[i] = (uint8_t)(i * 13u + 5u);
    }
    source_packets = wirehair_segment_source_packets(sizeof(input));

    /* An ACK available after the final source packet must avoid all repair. */
    memset(&store, 0, sizeof(store));
    delay = (DelayedAck){
        .store = &store,
        .ack_after_packets = source_packets,
    };
    if (wirehair_segment_send(&config, 9u, 0u, 4u, 8u, input,
                              sizeof(input), store_emit, &store,
                              delayed_ack_poll, &delay, &stats) != 0 ||
        !stats.stopped_by_ack || stats.ack_timed_out ||
        stats.repair_sent != 0u || stats.repair_rounds != 0u) {
        fprintf(stderr,
                "post-source ACK did not avoid repair sent=%u rounds=%u "
                "stopped=%d timeout=%d\n",
                stats.repair_sent, stats.repair_rounds,
                stats.stopped_by_ack, stats.ack_timed_out);
        return -1;
    }

    memset(&store, 0, sizeof(store));
    delay = (DelayedAck){
        .store = &store,
        .ack_after_packets = source_packets + 1u,
    };
    if (wirehair_segment_send(&config, 9u, 0u, 4u, 8u, input,
                              sizeof(input), store_emit, &store,
                              delayed_ack_poll, &delay, &stats) != 0 ||
        !stats.stopped_by_ack || stats.ack_timed_out ||
        stats.repair_sent == 0u || stats.repair_rounds == 0u) {
        fprintf(stderr,
                "delayed ACK did not stop repair rounds sent=%u rounds=%u "
                "stopped=%d timeout=%d\n",
                stats.repair_sent, stats.repair_rounds,
                stats.stopped_by_ack, stats.ack_timed_out);
        return -1;
    }

    memset(&store, 0, sizeof(store));
    delay = (DelayedAck){
        .store = &store,
        .never_ack = 1,
    };
    memset(&stats, 0, sizeof(stats));
    if (wirehair_segment_send(&config, 9u, 1u, 4u, 8u, input,
                              sizeof(input), store_emit, &store,
                              delayed_ack_poll, &delay, &stats) == 0 ||
        !stats.ack_timed_out || stats.stopped_by_ack ||
        stats.repair_sent != source_packets) {
        fprintf(stderr,
                "missing ACK did not hard-fail sent=%u source=%u "
                "stopped=%d timeout=%d\n",
                stats.repair_sent, source_packets,
                stats.stopped_by_ack, stats.ack_timed_out);
        return -1;
    }
    return 0;
}

static int test_incremental_tx_batches(void)
{
    WirehairSegmentConfig config;
    WirehairSegmentTx *tx;
    const WirehairSegmentSendStats *stats;
    PacketStore store;
    uint8_t input[4096];
    uint32_t source_packets;
    uint32_t repair_packets;
    int emitted;

    memset(input, 0x5a, sizeof(input));
    memset(&store, 0, sizeof(store));
    wirehair_segment_config_defaults(&config);
    config.segment_bytes = sizeof(input);
    config.repair_percent = 50u;
    config.ack_enabled = true;
    source_packets = wirehair_segment_source_packets(sizeof(input));
    repair_packets =
        wirehair_segment_ack_repair_round_packets(source_packets);
    tx = wirehair_segment_tx_create(&config, 17u, 3u, 4u, 8u,
                                    input, sizeof(input));
    if (tx == NULL) {
        return -1;
    }
    emitted = wirehair_segment_tx_emit_source(tx, 1u, store_emit, &store);
    if (emitted != 1 || wirehair_segment_tx_source_complete(tx)) {
        wirehair_segment_tx_destroy(tx);
        return -1;
    }
    emitted = wirehair_segment_tx_emit_source(
        tx, source_packets, store_emit, &store);
    if (emitted != (int)source_packets - 1 ||
        !wirehair_segment_tx_source_complete(tx) ||
        wirehair_segment_tx_repair_exhausted(tx)) {
        wirehair_segment_tx_destroy(tx);
        return -1;
    }
    emitted = wirehair_segment_tx_emit_repair(
        tx, repair_packets, store_emit, &store);
    stats = wirehair_segment_tx_stats(tx);
    if (emitted != (int)repair_packets || stats == NULL ||
        stats->repair_sent != repair_packets ||
        stats->repair_rounds != 1u ||
        stats->packets_sent != source_packets + repair_packets) {
        wirehair_segment_tx_destroy(tx);
        return -1;
    }
    wirehair_segment_tx_destroy(tx);
    return 0;
}

int main(void)
{
    WirehairSegmentConfig config;
    uint32_t ten_mib_packets;

    wirehair_segment_config_defaults(&config);
    ten_mib_packets =
        (10u * 1024u * 1024u + WH_PACKET_SIZE - 1u) / WH_PACKET_SIZE;
    if (!wirehair_segment_config_valid(&config) ||
        wirehair_segment_window(&config) != WH_SEGMENT_WINDOW_DEFAULT ||
        wirehair_segment_source_packets(10u * 1024u * 1024u) !=
            ten_mib_packets ||
        wirehair_segment_repair_packets(7490u, 10u) != 749u ||
        wirehair_segment_repair_packets(7490u, 0u) != 0u ||
        wirehair_segment_repair_packets(3u, 100u) != 3u ||
        wirehair_segment_repair_packets(3u, 10u) != WH_REPAIR_MIN_PACKETS ||
        wirehair_segment_repair_ceiling(100u, 10u, true) != 100u ||
        wirehair_segment_repair_ceiling(100u, 10u, false) != 10u ||
        wirehair_segment_repair_ceiling(3u, 10u, true) != 3u ||
        run_roundtrip(28000u, 0) != 0 ||
        run_roundtrip(731u, 1) != 0 ||
        test_outof_window_drop() != 0 ||
        test_ack_repeat_after_recover() != 0 ||
        test_ack_repair_rounds_and_timeout() != 0 ||
        test_incremental_tx_batches() != 0) {
        fprintf(stderr, "wirehair segment tests failed\n");
        return 1;
    }
    puts("wirehair segment tests passed");
    return 0;
}
