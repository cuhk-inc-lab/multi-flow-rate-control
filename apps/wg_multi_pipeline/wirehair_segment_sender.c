#include "wirehair_segment_sender.h"

#include <stdlib.h>
#include <string.h>

typedef struct WirehairSegmentSenderSlot {
    int active;
    int acked;
    uint64_t segment_id;
    uint8_t *data;
    size_t data_len;
    WirehairSegmentTx *tx;
    uint64_t repair_due_ns;
    int repair_due_valid;
} WirehairSegmentSenderSlot;

struct WirehairSegmentSender {
    WirehairSegmentConfig config;
    uint32_t flow_id;
    uint8_t final_dst;
    uint8_t ttl;
    uint8_t window;
    WirehairSegmentSenderEmitFn emit_fn;
    void *emit_ctx;
    uint64_t base_segment;
    uint64_t next_segment_id;
    int input_finished;
    int end_emitted;
    int failed;
    int ack_timed_out;
    size_t active_count;
    size_t window_hwm;
    uint64_t repair_sent;
    uint64_t repair_rounds;
    uint64_t segments_completed;
    uint64_t ack_ids[WH_SEGMENT_WINDOW_MAX];
    uint8_t ack_valid[WH_SEGMENT_WINDOW_MAX];
    WirehairSegmentSenderSlot *slots;
};

typedef struct WirehairSegmentSenderEmitBridge {
    WirehairSegmentSender *sender;
    WirehairSegmentTx *tx;
} WirehairSegmentSenderEmitBridge;

static int sender_emit_bridge(const WireHeader *header, const uint8_t *payload,
                              size_t payload_len, void *opaque)
{
    WirehairSegmentSenderEmitBridge *bridge = opaque;
    const WirehairSegmentSendStats *stats;
    WirehairSegmentSenderPacketKind kind;
    int rc;

    if (bridge == NULL || bridge->sender == NULL ||
        bridge->sender->emit_fn == NULL || header == NULL) {
        return -1;
    }
    stats = wirehair_segment_tx_stats(bridge->tx);
    if (stats == NULL) {
        return -1;
    }
    kind = header->shard_index < stats->source_packets ? WH_SENDER_PKT_DATA
                                                       : WH_SENDER_PKT_REPAIR;
    if (kind == WH_SENDER_PKT_REPAIR) {
        bridge->sender->repair_sent++;
    }
    rc = bridge->sender->emit_fn(header, payload, payload_len, kind,
                                 bridge->sender->emit_ctx);
    if (rc > 0) {
        return -2;
    }
    return rc;
}

static int ack_contains(const WirehairSegmentSender *sender,
                        uint64_t segment_id)
{
    size_t index;

    if (sender == NULL) {
        return 0;
    }
    index = (size_t)(segment_id % WH_SEGMENT_WINDOW_MAX);
    return sender->ack_valid[index] &&
           sender->ack_ids[index] == segment_id;
}

static void ack_clear(WirehairSegmentSender *sender, uint64_t segment_id)
{
    size_t index;

    if (sender == NULL) {
        return;
    }
    index = (size_t)(segment_id % WH_SEGMENT_WINDOW_MAX);
    if (sender->ack_valid[index] && sender->ack_ids[index] == segment_id) {
        sender->ack_valid[index] = 0;
    }
}

static void ack_mark(WirehairSegmentSender *sender, const WireHeader *header)
{
    size_t index;

    if (sender == NULL || header == NULL ||
        header->flow_id != sender->flow_id ||
        header->block_id < sender->base_segment ||
        header->block_id - sender->base_segment >= sender->window) {
        return;
    }
    index = (size_t)(header->block_id % WH_SEGMENT_WINDOW_MAX);
    sender->ack_ids[index] = header->block_id;
    sender->ack_valid[index] = 1;
}

static void release_slot(WirehairSegmentSender *sender,
                         WirehairSegmentSenderSlot *slot)
{
    const WirehairSegmentSendStats *stats;

    if (sender == NULL || slot == NULL || !slot->active) {
        return;
    }
    stats = wirehair_segment_tx_stats(slot->tx);
    if (stats != NULL) {
        sender->repair_rounds += stats->repair_rounds;
    }
    wirehair_segment_tx_destroy(slot->tx);
    free(slot->data);
    memset(slot, 0, sizeof(*slot));
    sender->active_count--;
    sender->segments_completed++;
}

static WirehairSegmentSenderSlot *slot_at(WirehairSegmentSender *sender,
                                          uint64_t segment_id)
{
    if (sender == NULL || sender->window == 0u) {
        return NULL;
    }
    return &sender->slots[segment_id % sender->window];
}

static int release_acked_base(WirehairSegmentSender *sender)
{
    while (sender->active_count > 0) {
        WirehairSegmentSenderSlot *slot = slot_at(sender, sender->base_segment);

        if (slot == NULL || !slot->active ||
            slot->segment_id != sender->base_segment || !slot->acked) {
            break;
        }
        ack_clear(sender, slot->segment_id);
        release_slot(sender, slot);
        sender->base_segment++;
    }
    return 0;
}

static int apply_pending_acks(WirehairSegmentSender *sender)
{
    size_t i;
    int progress = 0;

    for (i = 0; i < sender->window; i++) {
        WirehairSegmentSenderSlot *slot = &sender->slots[i];

        if (slot->active && !slot->acked &&
            ack_contains(sender, slot->segment_id)) {
            slot->acked = 1;
            progress = 1;
        }
    }
    if (release_acked_base(sender) != 0) {
        return -1;
    }
    return progress;
}

WirehairSegmentSender *wirehair_segment_sender_create(
    const WirehairSegmentConfig *config, uint32_t flow_id, uint8_t final_dst,
    uint8_t ttl, WirehairSegmentSenderEmitFn emit_fn, void *emit_ctx)
{
    WirehairSegmentSender *sender;
    uint8_t window;

    if (!wirehair_segment_config_valid(config) || !config->ack_enabled ||
        final_dst == 0 || ttl == 0 || emit_fn == NULL) {
        return NULL;
    }
    window = wirehair_segment_window(config);
    sender = calloc(1, sizeof(*sender));
    if (sender == NULL) {
        return NULL;
    }
    sender->slots = calloc(window, sizeof(*sender->slots));
    if (sender->slots == NULL) {
        free(sender);
        return NULL;
    }
    sender->config = *config;
    sender->flow_id = flow_id;
    sender->final_dst = final_dst;
    sender->ttl = ttl;
    sender->window = window;
    sender->emit_fn = emit_fn;
    sender->emit_ctx = emit_ctx;
    return sender;
}

void wirehair_segment_sender_destroy(WirehairSegmentSender *sender)
{
    size_t i;

    if (sender == NULL) {
        return;
    }
    for (i = 0; i < sender->window; i++) {
        if (sender->slots[i].active) {
            wirehair_segment_tx_destroy(sender->slots[i].tx);
            free(sender->slots[i].data);
        }
    }
    free(sender->slots);
    free(sender);
}

int wirehair_segment_sender_can_admit(const WirehairSegmentSender *sender)
{
    WirehairSegmentSenderSlot *slot;

    if (sender == NULL || sender->failed || sender->input_finished) {
        return 0;
    }
    if (sender->next_segment_id - sender->base_segment >= sender->window) {
        return 0;
    }
    slot = slot_at((WirehairSegmentSender *)sender, sender->next_segment_id);
    return slot != NULL && !slot->active;
}

int wirehair_segment_sender_admit(WirehairSegmentSender *sender,
                                  uint64_t segment_id, const uint8_t *data,
                                  size_t data_len)
{
    WirehairSegmentSenderSlot *slot;

    if (sender == NULL || data == NULL || data_len == 0 ||
        data_len > sender->config.segment_bytes || sender->failed) {
        return -2;
    }
    if (!wirehair_segment_sender_can_admit(sender)) {
        return -1;
    }
    if (segment_id != sender->next_segment_id) {
        return -2;
    }
    slot = slot_at(sender, segment_id);
    if (slot == NULL || slot->active) {
        return -2;
    }
    slot->data = malloc(data_len);
    if (slot->data == NULL) {
        return -2;
    }
    memcpy(slot->data, data, data_len);
    slot->tx = wirehair_segment_tx_create(
        &sender->config, sender->flow_id, segment_id, sender->final_dst,
        sender->ttl, slot->data, data_len);
    if (slot->tx == NULL) {
        free(slot->data);
        memset(slot, 0, sizeof(*slot));
        return -2;
    }
    slot->active = 1;
    slot->segment_id = segment_id;
    slot->data_len = data_len;
    sender->next_segment_id++;
    sender->active_count++;
    if (sender->active_count > sender->window_hwm) {
        sender->window_hwm = sender->active_count;
    }
    return 0;
}

void wirehair_segment_sender_mark_input_finished(WirehairSegmentSender *sender)
{
    if (sender != NULL) {
        sender->input_finished = 1;
    }
}

int wirehair_segment_sender_input_ack(WirehairSegmentSender *sender,
                                      const void *datagram, size_t length)
{
    WireHeader header;

    if (sender == NULL || datagram == NULL || length == 0u) {
        return -1;
    }
    if (wire_header_decode(&header, datagram, length) != 0 ||
        header.version != WIRE_VERSION_V4 || header.type != WIRE_TYPE_ACK ||
        header.flow_id != sender->flow_id) {
        return -1;
    }
    ack_mark(sender, &header);
    (void)apply_pending_acks(sender);
    return 0;
}

int wirehair_segment_sender_tick(WirehairSegmentSender *sender,
                                 uint64_t now_ns)
{
    size_t i;
    int progress;

    if (sender == NULL || sender->failed) {
        return -1;
    }
    progress = apply_pending_acks(sender);
    for (i = 0; i < sender->window; i++) {
        WirehairSegmentSenderSlot *slot =
            &sender->slots[(sender->base_segment + i) % sender->window];
        WirehairSegmentSenderEmitBridge bridge;
        int emitted;
        const WirehairSegmentSendStats *stats;

        if (!slot->active || slot->acked) {
            continue;
        }
        bridge.sender = sender;
        bridge.tx = slot->tx;
        if (!wirehair_segment_tx_source_complete(slot->tx)) {
            emitted = wirehair_segment_tx_emit_source(
                slot->tx, WIREHAIR_SENDER_SOURCE_BATCH, sender_emit_bridge,
                &bridge);
            if (emitted < 0) {
                if (emitted == -2) {
                    break;
                }
                sender->failed = 1;
                return -1;
            }
            if (emitted > 0) {
                progress = 1;
            }
            if (wirehair_segment_tx_source_complete(slot->tx)) {
                slot->repair_due_ns =
                    now_ns + (uint64_t)WH_ACK_INITIAL_WAIT_MS * 1000000ull;
                slot->repair_due_valid = 1;
            }
            continue;
        }
        if (slot->repair_due_valid && now_ns < slot->repair_due_ns) {
            continue;
        }
        if (wirehair_segment_tx_repair_exhausted(slot->tx)) {
            stats = wirehair_segment_tx_stats(slot->tx);
            if (stats != NULL) {
                sender->repair_rounds += stats->repair_rounds;
            }
            sender->ack_timed_out = 1;
            sender->failed = 1;
            return -1;
        }
        emitted = wirehair_segment_tx_emit_repair(
            slot->tx,
            wirehair_segment_ack_repair_round_packets(
                wirehair_segment_tx_stats(slot->tx)->source_packets),
            sender_emit_bridge, &bridge);
        if (emitted < 0) {
            if (emitted == -2) {
                break;
            }
            sender->failed = 1;
            return -1;
        }
        slot->repair_due_ns =
            now_ns + (uint64_t)WH_ACK_REPAIR_WAIT_MS * 1000000ull;
        slot->repair_due_valid = 1;
        if (emitted > 0) {
            progress = 1;
        }
    }
    (void)progress;
    return 0;
}

int wirehair_segment_sender_idle(const WirehairSegmentSender *sender)
{
    return sender != NULL && sender->input_finished &&
           sender->active_count == 0 && !sender->failed;
}

int wirehair_segment_sender_failed(const WirehairSegmentSender *sender)
{
    return sender != NULL && sender->failed;
}

int wirehair_segment_sender_end_emitted(const WirehairSegmentSender *sender)
{
    return sender != NULL && sender->end_emitted;
}

int wirehair_segment_sender_try_emit_end(WirehairSegmentSender *sender)
{
    WireHeader end;

    if (sender == NULL || sender->failed || sender->end_emitted ||
        !sender->input_finished || sender->active_count != 0) {
        return 0;
    }
    end = (WireHeader){
        .version = WIRE_VERSION_V4,
        .type = WIRE_TYPE_END,
        .final_dst = sender->final_dst,
        .ttl = sender->ttl,
        .flow_id = sender->flow_id,
        .block_id = sender->next_segment_id,
        .origin_node = sender->config.origin_node,
        .flags = WIRE_FLAG_ACK_REQUEST,
        .segment_bytes = 0,
    };
    if (sender->emit_fn(&end, NULL, 0, WH_SENDER_PKT_END, sender->emit_ctx) !=
        0) {
        sender->failed = 1;
        return -1;
    }
    sender->end_emitted = 1;
    return 1;
}

void wirehair_segment_sender_get_stats(const WirehairSegmentSender *sender,
                                       WirehairSegmentSenderStats *stats)
{
    if (stats == NULL) {
        return;
    }
    memset(stats, 0, sizeof(*stats));
    if (sender == NULL) {
        return;
    }
    stats->repair_sent = sender->repair_sent;
    stats->repair_rounds = sender->repair_rounds;
    stats->send_window_hwm = sender->window_hwm;
    stats->segments_completed = sender->segments_completed;
    stats->end_segment_count = sender->next_segment_id;
    stats->ack_timed_out = sender->ack_timed_out;
}
