#ifndef WIREHAIR_SEGMENT_SENDER_H
#define WIREHAIR_SEGMENT_SENDER_H

#include "wirehair_segment.h"

#include <stddef.h>
#include <stdint.h>

#define WIREHAIR_SENDER_SOURCE_BATCH 32u

typedef enum WirehairSegmentSenderPacketKind {
    WH_SENDER_PKT_DATA = 0,
    WH_SENDER_PKT_REPAIR = 1,
    WH_SENDER_PKT_END = 2
} WirehairSegmentSenderPacketKind;

typedef int (*WirehairSegmentSenderEmitFn)(const WireHeader *header,
                                           const uint8_t *payload,
                                           size_t payload_len,
                                           WirehairSegmentSenderPacketKind kind,
                                           void *ctx);

typedef struct WirehairSegmentSenderStats {
    uint64_t repair_sent;
    uint64_t repair_rounds;
    size_t send_window_hwm;
    uint64_t segments_completed;
    uint64_t end_segment_count;
    int ack_timed_out;
} WirehairSegmentSenderStats;

typedef struct WirehairSegmentSender WirehairSegmentSender;

WirehairSegmentSender *wirehair_segment_sender_create(
    const WirehairSegmentConfig *config, uint32_t flow_id, uint8_t final_dst,
    uint8_t ttl, WirehairSegmentSenderEmitFn emit_fn, void *emit_ctx);
void wirehair_segment_sender_destroy(WirehairSegmentSender *sender);

/* Returns 0 on success, -1 if window full, -2 on other error. */
int wirehair_segment_sender_admit(WirehairSegmentSender *sender,
                                  uint64_t segment_id, const uint8_t *data,
                                  size_t data_len);

void wirehair_segment_sender_mark_input_finished(WirehairSegmentSender *sender);

int wirehair_segment_sender_input_ack(WirehairSegmentSender *sender,
                                      const void *datagram, size_t length);

/* Drive source/repair emission. Returns -1 on emit failure or ack timeout. */
int wirehair_segment_sender_tick(WirehairSegmentSender *sender,
                                 uint64_t now_ns);

int wirehair_segment_sender_can_admit(const WirehairSegmentSender *sender);

int wirehair_segment_sender_idle(const WirehairSegmentSender *sender);

int wirehair_segment_sender_failed(const WirehairSegmentSender *sender);

int wirehair_segment_sender_end_emitted(const WirehairSegmentSender *sender);

/* Emit END when input is finished and all segments are released. */
int wirehair_segment_sender_try_emit_end(WirehairSegmentSender *sender);

void wirehair_segment_sender_get_stats(const WirehairSegmentSender *sender,
                                       WirehairSegmentSenderStats *stats);

#endif /* WIREHAIR_SEGMENT_SENDER_H */
