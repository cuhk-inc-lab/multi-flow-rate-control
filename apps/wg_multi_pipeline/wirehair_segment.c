#include "wirehair_segment.h"

#include "stream_config.h"

#include <wirehair/wirehair.h>

#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

typedef enum WirehairSegmentSlotState {
    WH_SEG_SLOT_EMPTY = 0,
    WH_SEG_SLOT_COLLECTING,
    WH_SEG_SLOT_RECOVERED
} WirehairSegmentSlotState;

typedef struct WirehairSegmentSlot {
    WirehairSegmentSlotState state;
    uint64_t segment_id;
    uint32_t segment_bytes;
    uint32_t codec_bytes;
    uint16_t packet_limit;
    uint8_t source_origin;
    bool ack_requested;
    bool ack_sent;
    WirehairCodec decoder;
    uint8_t *recovered;
} WirehairSegmentSlot;

struct WirehairSegmentReceiver {
    WirehairSegmentConfig config;
    uint32_t flow_id;
    uint8_t window;
    uint64_t next_emit_segment;
    uint64_t end_segment_count;
    bool end_seen;
    bool complete;
    uint64_t ahead_window_drops;
    WirehairSegmentOutputFn output_fn;
    void *output_ctx;
    WirehairSegmentAckEmitFn ack_fn;
    void *ack_ctx;
    WirehairSegmentSlot slots[WH_SEGMENT_WINDOW_MAX];
};

struct WirehairSegmentTx {
    WirehairSegmentConfig config;
    WirehairCodec encoder;
    uint8_t *padded;
    uint32_t flow_id;
    uint64_t segment_id;
    uint8_t final_dst;
    uint8_t ttl;
    uint32_t data_len;
    uint32_t packet_limit;
    uint32_t next_packet_id;
    WirehairSegmentSendStats stats;
};

static pthread_once_t wh_segment_once = PTHREAD_ONCE_INIT;
static WirehairResult wh_segment_init_result = Wirehair_Error;

static void initialize_wirehair(void)
{
    wh_segment_init_result = wirehair_init();
}

static int wirehair_ready(void)
{
    return pthread_once(&wh_segment_once, initialize_wirehair) == 0 &&
           wh_segment_init_result == Wirehair_Success;
}

void wirehair_segment_config_defaults(WirehairSegmentConfig *config)
{
    if (config == NULL) {
        return;
    }
    *config = (WirehairSegmentConfig){
        .segment_bytes = WH_SEGMENT_DEFAULT_BYTES,
        .repair_percent = WH_SEGMENT_DEFAULT_REPAIR_PCT,
        .window = WH_SEGMENT_WINDOW_DEFAULT,
        .ack_enabled = false,
        .origin_node = 1u,
        .ack_ttl = WIRE_DEFAULT_TTL,
    };
}

uint8_t wirehair_segment_window(const WirehairSegmentConfig *config)
{
    if (config == NULL || config->window == 0u) {
        return WH_SEGMENT_WINDOW_DEFAULT;
    }
    if (config->window > WH_SEGMENT_WINDOW_MAX) {
        return WH_SEGMENT_WINDOW_MAX;
    }
    return config->window;
}

uint32_t wirehair_segment_source_packets(uint32_t segment_bytes)
{
    uint32_t packets;

    if (segment_bytes == 0) {
        return 0;
    }
    packets = (segment_bytes + WH_PACKET_SIZE - 1u) / WH_PACKET_SIZE;
    return packets < 2u ? 2u : packets;
}

uint32_t wirehair_segment_repair_packets(uint32_t source_packets,
                                          uint8_t repair_percent)
{
    uint64_t scaled;
    uint32_t repair;

    if (source_packets == 0 || repair_percent == 0) {
        return 0;
    }
    scaled = (uint64_t)source_packets * repair_percent;
    repair = (uint32_t)((scaled + 99u) / 100u);
    if (repair < WH_REPAIR_MIN_PACKETS) {
        repair = WH_REPAIR_MIN_PACKETS;
    }
    return repair;
}

uint32_t wirehair_segment_ack_repair_round_packets(uint32_t source_packets)
{
    return wirehair_segment_repair_packets(source_packets,
                                           WH_ACK_REPAIR_ROUND_PCT);
}

uint32_t wirehair_segment_repair_ceiling(uint32_t source_packets,
                                          uint8_t repair_percent,
                                          bool ack_enabled)
{
    uint32_t repair = wirehair_segment_repair_packets(source_packets,
                                                       repair_percent);

    /* ACK mode: percent is not the target. Spray until ACK, cap at 100%
     * of source so a lost ACK cannot run forever. */
    if (ack_enabled && source_packets > repair) {
        return source_packets;
    }
    return repair;
}

int wirehair_segment_config_valid(const WirehairSegmentConfig *config)
{
    uint32_t source_packets;
    uint32_t repair_packets;

    if (config == NULL || config->segment_bytes == 0 ||
        config->repair_percent > 100u || config->origin_node == 0 ||
        config->ack_ttl == 0 ||
        (config->window != 0u && config->window > WH_SEGMENT_WINDOW_MAX)) {
        return 0;
    }
    source_packets = wirehair_segment_source_packets(config->segment_bytes);
    repair_packets = wirehair_segment_repair_ceiling(
        source_packets, config->repair_percent, config->ack_enabled);
    return source_packets <= 64000u &&
           source_packets + repair_packets <= UINT16_MAX;
}

static int emit_one_packet(WirehairCodec encoder,
                           const WirehairSegmentConfig *config,
                           uint32_t flow_id, uint64_t segment_id,
                           uint8_t final_dst, uint8_t ttl,
                           uint32_t packet_id, uint32_t packet_limit,
                           uint32_t data_len, uint32_t source_packets,
                           WirehairSegmentEmitFn emit_fn, void *emit_ctx,
                           WirehairSegmentSendStats *stats)
{
    uint8_t payload[WH_PACKET_SIZE];
    uint32_t written = 0;
    WirehairResult encode_result;
    WireHeader header;

    memset(payload, 0, sizeof(payload));
    encode_result = wirehair_encode(encoder, packet_id, payload,
                                    sizeof(payload), &written);
    if (encode_result != Wirehair_Success || written == 0 ||
        written > sizeof(payload)) {
        return -1;
    }
    header = (WireHeader){
        .version = WIRE_VERSION_V4,
        .type = WIRE_TYPE_DATA,
        .final_dst = final_dst,
        .ttl = ttl,
        .flow_id = flow_id,
        .block_id = segment_id,
        .shard_index = (uint16_t)packet_id,
        .shard_count = (uint16_t)packet_limit,
        .payload_len = (uint16_t)written,
        .origin_node = config->origin_node,
        .flags = config->ack_enabled ? WIRE_FLAG_ACK_REQUEST : 0u,
        .segment_bytes = data_len,
    };
    if (emit_fn(&header, payload, written, emit_ctx) != 0) {
        return -1;
    }
    stats->packets_sent++;
    if (packet_id >= source_packets) {
        stats->repair_sent++;
    }
    return 0;
}

WirehairSegmentTx *wirehair_segment_tx_create(
    const WirehairSegmentConfig *config, uint32_t flow_id,
    uint64_t segment_id, uint8_t final_dst, uint8_t ttl,
    const uint8_t *data, size_t data_len)
{
    WirehairSegmentTx *tx;
    const uint8_t *message = data;
    uint32_t codec_bytes;
    uint32_t repair_ceiling;

    if (!wirehair_segment_config_valid(config) || data == NULL ||
        data_len == 0 || data_len > config->segment_bytes ||
        data_len > UINT32_MAX || final_dst == 0 || ttl == 0 ||
        !wirehair_ready()) {
        return NULL;
    }
    tx = calloc(1, sizeof(*tx));
    if (tx == NULL) {
        return NULL;
    }
    tx->config = *config;
    tx->flow_id = flow_id;
    tx->segment_id = segment_id;
    tx->final_dst = final_dst;
    tx->ttl = ttl;
    tx->data_len = (uint32_t)data_len;
    tx->stats.source_packets =
        wirehair_segment_source_packets((uint32_t)data_len);
    tx->stats.repair_budget = wirehair_segment_repair_packets(
        tx->stats.source_packets, config->repair_percent);
    repair_ceiling = wirehair_segment_repair_ceiling(
        tx->stats.source_packets, config->repair_percent,
        config->ack_enabled);
    tx->packet_limit = tx->stats.source_packets + repair_ceiling;

    codec_bytes = (uint32_t)data_len;
    if (codec_bytes < 2u * WH_PACKET_SIZE) {
        codec_bytes = 2u * WH_PACKET_SIZE;
        tx->padded = calloc(1, codec_bytes);
        if (tx->padded == NULL) {
            wirehair_segment_tx_destroy(tx);
            return NULL;
        }
        memcpy(tx->padded, data, data_len);
        message = tx->padded;
    }
    tx->encoder = wirehair_encoder_create(NULL, message, codec_bytes,
                                           WH_PACKET_SIZE);
    if (tx->encoder == NULL) {
        wirehair_segment_tx_destroy(tx);
        return NULL;
    }
    return tx;
}

void wirehair_segment_tx_destroy(WirehairSegmentTx *tx)
{
    if (tx == NULL) {
        return;
    }
    wirehair_free(tx->encoder);
    free(tx->padded);
    free(tx);
}

static int wirehair_segment_tx_emit_range(
    WirehairSegmentTx *tx, uint32_t end_packet_id,
    WirehairSegmentEmitFn emit_fn, void *emit_ctx)
{
    uint32_t emitted = 0;

    if (tx == NULL || emit_fn == NULL ||
        end_packet_id < tx->next_packet_id) {
        return -1;
    }
    if (end_packet_id > tx->packet_limit) {
        end_packet_id = tx->packet_limit;
    }
    while (tx->next_packet_id < end_packet_id) {
        if (emit_one_packet(
                tx->encoder, &tx->config, tx->flow_id, tx->segment_id,
                tx->final_dst, tx->ttl, tx->next_packet_id,
                tx->packet_limit, tx->data_len, tx->stats.source_packets,
                emit_fn, emit_ctx, &tx->stats) != 0) {
            return -1;
        }
        tx->next_packet_id++;
        emitted++;
    }
    return (int)emitted;
}

int wirehair_segment_tx_emit_source(WirehairSegmentTx *tx,
                                    uint32_t max_packets,
                                    WirehairSegmentEmitFn emit_fn,
                                    void *emit_ctx)
{
    uint32_t end;

    if (tx == NULL || max_packets == 0) {
        return tx == NULL ? -1 : 0;
    }
    end = tx->next_packet_id + max_packets;
    if (end < tx->next_packet_id ||
        end > tx->stats.source_packets) {
        end = tx->stats.source_packets;
    }
    return wirehair_segment_tx_emit_range(tx, end, emit_fn, emit_ctx);
}

int wirehair_segment_tx_emit_repair(WirehairSegmentTx *tx,
                                    uint32_t max_packets,
                                    WirehairSegmentEmitFn emit_fn,
                                    void *emit_ctx)
{
    uint32_t end;
    int emitted;

    if (tx == NULL || tx->next_packet_id < tx->stats.source_packets ||
        max_packets == 0) {
        return tx == NULL ? -1 : 0;
    }
    end = tx->next_packet_id + max_packets;
    if (end < tx->next_packet_id || end > tx->packet_limit) {
        end = tx->packet_limit;
    }
    emitted = wirehair_segment_tx_emit_range(tx, end, emit_fn, emit_ctx);
    if (emitted > 0) {
        tx->stats.repair_rounds++;
    }
    return emitted;
}

bool wirehair_segment_tx_source_complete(const WirehairSegmentTx *tx)
{
    return tx != NULL &&
           tx->next_packet_id >= tx->stats.source_packets;
}

bool wirehair_segment_tx_repair_exhausted(const WirehairSegmentTx *tx)
{
    return tx != NULL && tx->next_packet_id >= tx->packet_limit;
}

const WirehairSegmentSendStats *wirehair_segment_tx_stats(
    const WirehairSegmentTx *tx)
{
    return tx != NULL ? &tx->stats : NULL;
}

static int wait_for_segment_ack(WirehairSegmentAckPollFn ack_poll,
                                void *ack_ctx, uint32_t flow_id,
                                uint64_t segment_id, unsigned wait_ms)
{
    unsigned waited = 0;

    if (ack_poll == NULL) {
        return 0;
    }
    while (waited < wait_ms) {
        unsigned slice = WH_ACK_POLL_SLICE_MS;
        int acked;

        if (slice > wait_ms - waited) {
            slice = wait_ms - waited;
        }
        acked = ack_poll(flow_id, segment_id, slice, ack_ctx);
        if (acked != 0) {
            return acked;
        }
        waited += slice;
    }
    return 0;
}

int wirehair_segment_send(const WirehairSegmentConfig *config,
                          uint32_t flow_id, uint64_t segment_id,
                          uint8_t final_dst, uint8_t ttl,
                          const uint8_t *data, size_t data_len,
                          WirehairSegmentEmitFn emit_fn, void *emit_ctx,
                          WirehairSegmentAckPollFn ack_poll, void *ack_ctx,
                          WirehairSegmentSendStats *stats)
{
    WirehairCodec encoder = NULL;
    uint8_t *padded = NULL;
    const uint8_t *message = data;
    uint32_t source_packets;
    uint32_t repair_budget;
    uint32_t repair_ceiling;
    uint32_t packet_limit;
    uint32_t codec_bytes;
    uint32_t packet_id;
    int result = -1;
    WirehairSegmentSendStats local_stats;

    memset(&local_stats, 0, sizeof(local_stats));
    if (!wirehair_segment_config_valid(config) || data == NULL ||
        data_len == 0 || data_len > config->segment_bytes ||
        data_len > UINT32_MAX || final_dst == 0 || ttl == 0 ||
        emit_fn == NULL || !wirehair_ready()) {
        return -1;
    }

    source_packets = wirehair_segment_source_packets((uint32_t)data_len);
    repair_budget = wirehair_segment_repair_packets(
        source_packets, config->repair_percent);
    repair_ceiling = wirehair_segment_repair_ceiling(
        source_packets, config->repair_percent, config->ack_enabled);
    packet_limit = source_packets + repair_ceiling;
    codec_bytes = (uint32_t)data_len;
    if (codec_bytes < 2u * WH_PACKET_SIZE) {
        codec_bytes = 2u * WH_PACKET_SIZE;
        padded = calloc(1, codec_bytes);
        if (padded == NULL) {
            return -1;
        }
        memcpy(padded, data, data_len);
        message = padded;
    }

    encoder = wirehair_encoder_create(NULL, message, codec_bytes,
                                      WH_PACKET_SIZE);
    if (encoder == NULL) {
        goto out;
    }

    local_stats.source_packets = source_packets;
    local_stats.repair_budget = repair_budget;

    /*
     * No ACK (or an embedding without an ACK poll callback): retain the
     * finite send behavior.  With ACK polling, do not advance to the next
     * segment unless this segment is acknowledged.  Source is sent once,
     * then fresh repair packet ids are emitted in small ACK micro-rounds
     * (WH_ACK_REPAIR_ROUND_PCT).  A short wait between rounds gives the
     * receiver time to decode/write and a late packet makes an already-
     * recovered receiver re-send a lost ACK.
     */
    if (!config->ack_enabled || ack_poll == NULL) {
        uint32_t send_limit = config->ack_enabled
                                  ? packet_limit
                                  : source_packets + repair_budget;

        for (packet_id = 0; packet_id < send_limit; packet_id++) {
            if (emit_one_packet(encoder, config, flow_id, segment_id,
                                final_dst, ttl, packet_id, packet_limit,
                                (uint32_t)data_len, source_packets, emit_fn,
                                emit_ctx, &local_stats) != 0) {
                goto out;
            }
        }
        result = 0;
        goto out;
    }

    for (packet_id = 0; packet_id < source_packets; packet_id++) {
        {
            int acked = ack_poll(flow_id, segment_id, 0u, ack_ctx);

            if (acked < 0) {
                goto out;
            }
            if (acked > 0) {
                local_stats.stopped_by_ack = true;
                result = 0;
                goto out;
            }
        }
        if (emit_one_packet(encoder, config, flow_id, segment_id, final_dst,
                            ttl, packet_id, packet_limit, (uint32_t)data_len,
                            source_packets, emit_fn, emit_ctx,
                            &local_stats) != 0) {
            goto out;
        }
    }

    /*
     * Decoding normally completes on the last few source packets.  Waiting
     * once here avoids immediately injecting a full repair round while the
     * receiver is decoding the segment or flushing its output.
     */
    {
        int acked = wait_for_segment_ack(ack_poll, ack_ctx, flow_id, segment_id,
                                         WH_ACK_INITIAL_WAIT_MS);

        if (acked < 0) {
            goto out;
        }
        if (acked > 0) {
            local_stats.stopped_by_ack = true;
            result = 0;
            goto out;
        }
    }

    while (packet_id < packet_limit) {
        uint32_t round_packets =
            wirehair_segment_ack_repair_round_packets(source_packets);
        uint32_t round_end = packet_id + round_packets;
        int acked;

        if (round_end < packet_id || round_end > packet_limit) {
            round_end = packet_limit;
        }
        local_stats.repair_rounds++;
        while (packet_id < round_end) {
            acked = ack_poll(flow_id, segment_id, 0u, ack_ctx);

            if (acked < 0) {
                goto out;
            }
            if (acked > 0) {
                local_stats.stopped_by_ack = true;
                result = 0;
                goto out;
            }
            if (emit_one_packet(encoder, config, flow_id, segment_id,
                                final_dst, ttl, packet_id, packet_limit,
                                (uint32_t)data_len, source_packets, emit_fn,
                                emit_ctx, &local_stats) != 0) {
                goto out;
            }
            packet_id++;
        }
        acked = wait_for_segment_ack(ack_poll, ack_ctx, flow_id, segment_id,
                                     WH_ACK_REPAIR_WAIT_MS);
        if (acked < 0) {
            goto out;
        }
        if (acked > 0) {
            local_stats.stopped_by_ack = true;
            result = 0;
            goto out;
        }
    }

    /*
     * Exhausting the advertised repair id space without an ACK is a hard
     * segment failure.  Advancing here would permanently strand the receiver
     * behind this segment and eventually overflow its decode window.
     */
    local_stats.ack_timed_out = true;
    result = -1;
out:
    wirehair_free(encoder);
    free(padded);
    if (stats != NULL) {
        *stats = local_stats;
    }
    return result;
}

static void release_slot(WirehairSegmentSlot *slot)
{
    if (slot == NULL) {
        return;
    }
    wirehair_free(slot->decoder);
    free(slot->recovered);
    memset(slot, 0, sizeof(*slot));
}

static WirehairSegmentSlot *find_slot(WirehairSegmentReceiver *receiver,
                                      uint64_t segment_id)
{
    size_t i;

    for (i = 0; i < receiver->window; i++) {
        if (receiver->slots[i].state != WH_SEG_SLOT_EMPTY &&
            receiver->slots[i].segment_id == segment_id) {
            return &receiver->slots[i];
        }
    }
    return NULL;
}

static int header_geometry_ok(const WirehairSegmentReceiver *receiver,
                              const WireHeader *header)
{
    uint32_t source_packets;
    uint32_t max_repair;

    if (header->segment_bytes == 0 ||
        header->segment_bytes > receiver->config.segment_bytes) {
        return 0;
    }
    source_packets = wirehair_segment_source_packets(header->segment_bytes);
    max_repair = wirehair_segment_repair_ceiling(
        source_packets, receiver->config.repair_percent,
        receiver->config.ack_enabled ||
            (header->flags & WIRE_FLAG_ACK_REQUEST) != 0);
    return header->shard_count >= source_packets &&
           header->shard_count <= source_packets + max_repair;
}

static WirehairSegmentSlot *allocate_slot(WirehairSegmentReceiver *receiver,
                                          const WireHeader *header)
{
    size_t i;
    uint32_t codec_bytes;

    if (!header_geometry_ok(receiver, header)) {
        return NULL;
    }
    codec_bytes = header->segment_bytes;
    if (codec_bytes < 2u * WH_PACKET_SIZE) {
        codec_bytes = 2u * WH_PACKET_SIZE;
    }

    for (i = 0; i < receiver->window; i++) {
        WirehairSegmentSlot *slot = &receiver->slots[i];

        if (slot->state != WH_SEG_SLOT_EMPTY) {
            continue;
        }
        slot->decoder =
            wirehair_decoder_create(NULL, codec_bytes, WH_PACKET_SIZE);
        slot->recovered = malloc(codec_bytes);
        if (slot->decoder == NULL || slot->recovered == NULL) {
            release_slot(slot);
            return NULL;
        }
        slot->state = WH_SEG_SLOT_COLLECTING;
        slot->segment_id = header->block_id;
        slot->segment_bytes = header->segment_bytes;
        slot->codec_bytes = codec_bytes;
        slot->packet_limit = header->shard_count;
        slot->source_origin = header->origin_node;
        slot->ack_requested = receiver->config.ack_enabled &&
                              (header->flags & WIRE_FLAG_ACK_REQUEST) != 0;
        return slot;
    }
    return NULL;
}

static int emit_return_ack(WirehairSegmentReceiver *receiver,
                           uint8_t origin_node, uint64_t segment_id,
                           uint32_t segment_bytes)
{
    WireHeader ack;

    if (receiver->ack_fn == NULL || origin_node == 0) {
        return 0;
    }
    ack = (WireHeader){
        .version = WIRE_VERSION_V4,
        .type = WIRE_TYPE_ACK,
        .final_dst = origin_node,
        .ttl = receiver->config.ack_ttl,
        .flow_id = receiver->flow_id,
        .block_id = segment_id,
        .origin_node = receiver->config.origin_node,
        .flags = WIRE_FLAG_RETURN_PATH,
        .segment_bytes = segment_bytes,
    };
    return receiver->ack_fn(&ack, receiver->ack_ctx);
}

static int emit_ack(WirehairSegmentReceiver *receiver,
                    WirehairSegmentSlot *slot)
{
    if (!slot->ack_requested) {
        return 0;
    }
    if (emit_return_ack(receiver, slot->source_origin, slot->segment_id,
                        slot->segment_bytes) != 0) {
        return -1;
    }
    slot->ack_sent = true;
    return 0;
}

static int emit_ready_segments(WirehairSegmentReceiver *receiver)
{
    for (;;) {
        WirehairSegmentSlot *slot = find_slot(
            receiver, receiver->next_emit_segment);

        if (slot == NULL || slot->state != WH_SEG_SLOT_RECOVERED) {
            break;
        }
        if (receiver->output_fn(receiver->flow_id, slot->recovered,
                                slot->segment_bytes,
                                receiver->output_ctx) != 0) {
            return -1;
        }
        release_slot(slot);
        receiver->next_emit_segment++;
    }
    if (receiver->end_seen &&
        receiver->next_emit_segment == receiver->end_segment_count) {
        receiver->complete = true;
    }
    return 0;
}

WirehairSegmentReceiver *wirehair_segment_receiver_create(
    const WirehairSegmentConfig *config, uint32_t flow_id,
    WirehairSegmentOutputFn output_fn, void *output_ctx,
    WirehairSegmentAckEmitFn ack_fn, void *ack_ctx)
{
    WirehairSegmentReceiver *receiver;

    if (!wirehair_segment_config_valid(config) || output_fn == NULL ||
        !wirehair_ready()) {
        return NULL;
    }
    receiver = calloc(1, sizeof(*receiver));
    if (receiver == NULL) {
        return NULL;
    }
    receiver->config = *config;
    receiver->window = wirehair_segment_window(config);
    receiver->flow_id = flow_id;
    receiver->output_fn = output_fn;
    receiver->output_ctx = output_ctx;
    receiver->ack_fn = ack_fn;
    receiver->ack_ctx = ack_ctx;
    return receiver;
}

void wirehair_segment_receiver_destroy(WirehairSegmentReceiver *receiver)
{
    size_t i;

    if (receiver == NULL) {
        return;
    }
    for (i = 0; i < WH_SEGMENT_WINDOW_MAX; i++) {
        release_slot(&receiver->slots[i]);
    }
    free(receiver);
}

int wirehair_segment_receiver_ingest(WirehairSegmentReceiver *receiver,
                                     const WireHeader *header,
                                     const uint8_t *payload,
                                     size_t payload_len)
{
    WirehairSegmentSlot *slot;
    WirehairResult decode_result;

    if (receiver == NULL || header == NULL ||
        header->version != WIRE_VERSION_V4 ||
        header->flow_id != receiver->flow_id) {
        return -1;
    }
    if (header->type == WIRE_TYPE_END) {
        receiver->end_seen = true;
        receiver->end_segment_count = header->block_id;
        return emit_ready_segments(receiver);
    }
    /* Late DATA after this segment was emitted: re-ACK so the sender can stop. */
    if (header->type == WIRE_TYPE_DATA &&
        header->block_id < receiver->next_emit_segment) {
        if (receiver->config.ack_enabled &&
            (header->flags & WIRE_FLAG_ACK_REQUEST) != 0) {
            return emit_return_ack(receiver, header->origin_node,
                                   header->block_id, header->segment_bytes);
        }
        return 0;
    }
    if (header->type != WIRE_TYPE_DATA || payload == NULL ||
        payload_len == 0 || payload_len != header->payload_len ||
        payload_len > WH_PACKET_SIZE || header->origin_node == 0 ||
        header->shard_count == 0 ||
        header->shard_index >= header->shard_count) {
        return -1;
    }
    /*
     * Ahead of the decode window: drop the datagram, do not kill the flow.
     * Sender backpressure (ACK wait) + a larger default window reduce how
     * often this happens under loss.
     */
    if (header->block_id - receiver->next_emit_segment >= receiver->window) {
        receiver->ahead_window_drops++;
        return 0;
    }

    slot = find_slot(receiver, header->block_id);
    if (slot == NULL) {
        if (!header_geometry_ok(receiver, header)) {
            return -1;
        }
        slot = allocate_slot(receiver, header);
        if (slot == NULL) {
            /* Window full or OOM: drop, keep the flow alive. */
            return 0;
        }
    }
    if (slot->packet_limit != header->shard_count ||
        slot->segment_bytes != header->segment_bytes ||
        slot->source_origin != header->origin_node) {
        return -1;
    }
    if (slot->state == WH_SEG_SLOT_RECOVERED) {
        return emit_ack(receiver, slot);
    }

    decode_result = wirehair_decode(slot->decoder, header->shard_index,
                                    payload, (uint32_t)payload_len);
    if (decode_result == Wirehair_NeedMore) {
        return 0;
    }
    if (decode_result != Wirehair_Success ||
        wirehair_recover(slot->decoder, slot->recovered,
                         slot->codec_bytes) != Wirehair_Success) {
        return -1;
    }
    slot->state = WH_SEG_SLOT_RECOVERED;
    if (emit_ack(receiver, slot) != 0) {
        return -1;
    }
    return emit_ready_segments(receiver);
}

bool wirehair_segment_receiver_complete(
    const WirehairSegmentReceiver *receiver)
{
    return receiver != NULL && receiver->complete;
}

uint64_t wirehair_segment_receiver_ahead_drops(
    const WirehairSegmentReceiver *receiver)
{
    return receiver != NULL ? receiver->ahead_window_drops : 0u;
}
