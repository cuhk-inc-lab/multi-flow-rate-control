#ifndef WIREHAIR_SEGMENT_H
#define WIREHAIR_SEGMENT_H

#include "wire_header.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define WH_SEGMENT_DEFAULT_BYTES (10u * 1024u * 1024u)
#define WH_SEGMENT_DEFAULT_REPAIR_PCT 10u
#define WH_SEGMENT_WINDOW 4u

typedef struct WirehairSegmentConfig {
    uint32_t segment_bytes;
    uint8_t repair_percent;
    bool ack_enabled;
    uint8_t origin_node;
    uint8_t ack_ttl;
} WirehairSegmentConfig;

typedef struct WirehairSegmentSendStats {
    uint32_t source_packets;
    uint32_t repair_budget;
    uint32_t repair_sent;
    uint32_t packets_sent;
    bool stopped_by_ack;
} WirehairSegmentSendStats;

typedef int (*WirehairSegmentEmitFn)(const WireHeader *header,
                                     const uint8_t *payload,
                                     size_t payload_len, void *ctx);
typedef int (*WirehairSegmentAckPollFn)(uint32_t flow_id, uint64_t segment_id,
                                        unsigned wait_ms, void *ctx);
typedef int (*WirehairSegmentOutputFn)(uint32_t flow_id, const uint8_t *data,
                                       size_t len, void *ctx);
typedef int (*WirehairSegmentAckEmitFn)(const WireHeader *ack, void *ctx);

typedef struct WirehairSegmentReceiver WirehairSegmentReceiver;

void wirehair_segment_config_defaults(WirehairSegmentConfig *config);
int wirehair_segment_config_valid(const WirehairSegmentConfig *config);
uint32_t wirehair_segment_source_packets(uint32_t segment_bytes);
uint32_t wirehair_segment_repair_packets(uint32_t source_packets,
                                          uint8_t repair_percent);

int wirehair_segment_send(const WirehairSegmentConfig *config,
                          uint32_t flow_id, uint64_t segment_id,
                          uint8_t final_dst, uint8_t ttl,
                          const uint8_t *data, size_t data_len,
                          WirehairSegmentEmitFn emit_fn, void *emit_ctx,
                          WirehairSegmentAckPollFn ack_poll, void *ack_ctx,
                          WirehairSegmentSendStats *stats);

WirehairSegmentReceiver *wirehair_segment_receiver_create(
    const WirehairSegmentConfig *config, uint32_t flow_id,
    WirehairSegmentOutputFn output_fn, void *output_ctx,
    WirehairSegmentAckEmitFn ack_fn, void *ack_ctx);
void wirehair_segment_receiver_destroy(WirehairSegmentReceiver *receiver);
int wirehair_segment_receiver_ingest(WirehairSegmentReceiver *receiver,
                                     const WireHeader *header,
                                     const uint8_t *payload,
                                     size_t payload_len);
bool wirehair_segment_receiver_complete(
    const WirehairSegmentReceiver *receiver);

#endif /* WIREHAIR_SEGMENT_H */
