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
    uint64_t next_emit_segment;
    uint64_t end_segment_count;
    bool end_seen;
    bool complete;
    WirehairSegmentOutputFn output_fn;
    void *output_ctx;
    WirehairSegmentAckEmitFn ack_fn;
    void *ack_ctx;
    WirehairSegmentSlot slots[WH_SEGMENT_WINDOW];
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
        .ack_enabled = false,
        .origin_node = 1u,
        .ack_ttl = WIRE_DEFAULT_TTL,
    };
}

uint32_t wirehair_segment_source_packets(uint32_t segment_bytes)
{
    uint32_t packets;

    if (segment_bytes == 0) {
        return 0;
    }
    packets = (segment_bytes + PKG_SIZE - 1u) / PKG_SIZE;
    return packets < 2u ? 2u : packets;
}

uint32_t wirehair_segment_repair_packets(uint32_t source_packets,
                                          uint8_t repair_percent)
{
    uint64_t scaled;

    if (source_packets == 0 || repair_percent == 0) {
        return 0;
    }
    scaled = (uint64_t)source_packets * repair_percent;
    return (uint32_t)((scaled + 99u) / 100u);
}

int wirehair_segment_config_valid(const WirehairSegmentConfig *config)
{
    uint32_t source_packets;
    uint32_t repair_packets;

    if (config == NULL || config->segment_bytes == 0 ||
        config->repair_percent > 100u || config->origin_node == 0 ||
        config->ack_ttl == 0) {
        return 0;
    }
    source_packets = wirehair_segment_source_packets(config->segment_bytes);
    repair_packets = wirehair_segment_repair_packets(
        source_packets, config->repair_percent);
    return source_packets <= 64000u &&
           source_packets + repair_packets <= UINT16_MAX;
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
    packet_limit = source_packets + repair_budget;
    codec_bytes = (uint32_t)data_len;
    if (codec_bytes < 2u * PKG_SIZE) {
        codec_bytes = 2u * PKG_SIZE;
        padded = calloc(1, codec_bytes);
        if (padded == NULL) {
            return -1;
        }
        memcpy(padded, data, data_len);
        message = padded;
    }

    encoder = wirehair_encoder_create(NULL, message, codec_bytes, PKG_SIZE);
    if (encoder == NULL) {
        goto out;
    }

    local_stats.source_packets = source_packets;
    local_stats.repair_budget = repair_budget;
    for (packet_id = 0; packet_id < packet_limit; packet_id++) {
        uint8_t payload[PKG_SIZE];
        uint32_t written = 0;
        WirehairResult encode_result;
        WireHeader header;

        if (packet_id >= source_packets && config->ack_enabled &&
            ack_poll != NULL) {
            int acked = ack_poll(flow_id, segment_id,
                                 packet_id == source_packets ? 2u : 0u,
                                 ack_ctx);
            if (acked < 0) {
                goto out;
            }
            if (acked > 0) {
                local_stats.stopped_by_ack = true;
                break;
            }
        }

        memset(payload, 0, sizeof(payload));
        encode_result = wirehair_encode(encoder, packet_id, payload,
                                        sizeof(payload), &written);
        if (encode_result != Wirehair_Success || written == 0 ||
            written > sizeof(payload)) {
            goto out;
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
            .segment_bytes = (uint32_t)data_len,
        };
        if (emit_fn(&header, payload, written, emit_ctx) != 0) {
            goto out;
        }
        local_stats.packets_sent++;
        if (packet_id >= source_packets) {
            local_stats.repair_sent++;
        }
    }

    if (config->ack_enabled && !local_stats.stopped_by_ack &&
        ack_poll != NULL) {
        int acked = ack_poll(flow_id, segment_id, 2u, ack_ctx);
        if (acked < 0) {
            goto out;
        }
        local_stats.stopped_by_ack = acked > 0;
    }
    result = 0;
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

    for (i = 0; i < WH_SEGMENT_WINDOW; i++) {
        if (receiver->slots[i].state != WH_SEG_SLOT_EMPTY &&
            receiver->slots[i].segment_id == segment_id) {
            return &receiver->slots[i];
        }
    }
    return NULL;
}

static WirehairSegmentSlot *allocate_slot(WirehairSegmentReceiver *receiver,
                                          const WireHeader *header)
{
    size_t i;
    uint32_t source_packets;
    uint32_t max_repair;
    uint32_t codec_bytes;

    if (header->segment_bytes == 0 ||
        header->segment_bytes > receiver->config.segment_bytes) {
        return NULL;
    }
    source_packets = wirehair_segment_source_packets(header->segment_bytes);
    max_repair = wirehair_segment_repair_packets(
        source_packets, receiver->config.repair_percent);
    if (header->shard_count < source_packets ||
        header->shard_count > source_packets + max_repair) {
        return NULL;
    }
    codec_bytes = header->segment_bytes;
    if (codec_bytes < 2u * PKG_SIZE) {
        codec_bytes = 2u * PKG_SIZE;
    }

    for (i = 0; i < WH_SEGMENT_WINDOW; i++) {
        WirehairSegmentSlot *slot = &receiver->slots[i];

        if (slot->state != WH_SEG_SLOT_EMPTY) {
            continue;
        }
        slot->decoder = wirehair_decoder_create(NULL, codec_bytes, PKG_SIZE);
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

static int emit_ack(WirehairSegmentReceiver *receiver,
                    WirehairSegmentSlot *slot)
{
    WireHeader ack;

    if (!slot->ack_requested || slot->ack_sent || receiver->ack_fn == NULL) {
        return 0;
    }
    ack = (WireHeader){
        .version = WIRE_VERSION_V4,
        .type = WIRE_TYPE_ACK,
        .final_dst = slot->source_origin,
        .ttl = receiver->config.ack_ttl,
        .flow_id = receiver->flow_id,
        .block_id = slot->segment_id,
        .origin_node = receiver->config.origin_node,
        .flags = WIRE_FLAG_RETURN_PATH,
        .segment_bytes = slot->segment_bytes,
    };
    if (receiver->ack_fn(&ack, receiver->ack_ctx) != 0) {
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
    for (i = 0; i < WH_SEGMENT_WINDOW; i++) {
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
    /* Repair packets may still arrive after systematic recovery and release. */
    if (header->type == WIRE_TYPE_DATA &&
        header->block_id < receiver->next_emit_segment) {
        return 0;
    }
    if (header->type != WIRE_TYPE_DATA || payload == NULL ||
        payload_len == 0 || payload_len != header->payload_len ||
        payload_len > PKG_SIZE || header->origin_node == 0 ||
        header->shard_count == 0 ||
        header->shard_index >= header->shard_count ||
        header->block_id - receiver->next_emit_segment >= WH_SEGMENT_WINDOW) {
        return -1;
    }

    slot = find_slot(receiver, header->block_id);
    if (slot == NULL) {
        slot = allocate_slot(receiver, header);
        if (slot == NULL) {
            return -1;
        }
    }
    if (slot->packet_limit != header->shard_count ||
        slot->segment_bytes != header->segment_bytes ||
        slot->source_origin != header->origin_node) {
        return -1;
    }
    if (slot->state == WH_SEG_SLOT_RECOVERED) {
        return 0;
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
