#ifndef WIREHAIR_SEGMENT_H
#define WIREHAIR_SEGMENT_H

#include "wire_header.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define WH_SEGMENT_DEFAULT_BYTES (10u * 1024u * 1024u)
#define WH_SEGMENT_DEFAULT_REPAIR_PCT 10u
/* Default / max in-flight unrecovered segments at the receiver. */
#define WH_SEGMENT_WINDOW_DEFAULT 8u
#define WH_SEGMENT_WINDOW_MAX 16u
/* Legacy alias used by older call sites; prefer config.window. */
#define WH_SEGMENT_WINDOW WH_SEGMENT_WINDOW_DEFAULT
/*
 * Packet payload sized for MTU 1450 without IP fragmentation:
 * 1450 - 20 (IPv4) - 8 (UDP) - 52 (wire v4) = 1370.
 */
#define WH_PACKET_SIZE 1370u
/* When repair_percent > 0, never advertise fewer than this many repair pkts. */
#define WH_REPAIR_MIN_PACKETS 2u
/*
 * ACK repair uses smaller per-round batches than --wh-repair-pct so a lossy
 * segment sprays incrementally instead of one large repair burst.
 */
#define WH_ACK_REPAIR_ROUND_PCT 5u
/*
 * Short decode/ACK windows: poll frequently between repair micro-rounds.
 */
#define WH_ACK_INITIAL_WAIT_MS 50u
#define WH_ACK_REPAIR_WAIT_MS 100u
#define WH_ACK_POLL_SLICE_MS 10u

typedef struct WirehairSegmentConfig {
    uint32_t segment_bytes;
    uint8_t repair_percent;
    uint8_t window; /* 1..WH_SEGMENT_WINDOW_MAX; 0 => default */
    bool ack_enabled;
    uint8_t origin_node;
    uint8_t ack_ttl;
} WirehairSegmentConfig;

typedef struct WirehairSegmentSendStats {
    uint32_t source_packets;
    uint32_t repair_budget;
    uint32_t repair_sent;
    uint32_t packets_sent;
    uint32_t repair_rounds;
    bool stopped_by_ack;
    bool ack_timed_out;
} WirehairSegmentSendStats;

typedef struct WirehairSegmentTx WirehairSegmentTx;

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
uint8_t wirehair_segment_window(const WirehairSegmentConfig *config);
uint32_t wirehair_segment_source_packets(uint32_t segment_bytes);
uint32_t wirehair_segment_repair_packets(uint32_t source_packets,
                                          uint8_t repair_percent);
/* Per-round repair batch size in ACK mode (independent of repair_percent). */
uint32_t wirehair_segment_ack_repair_round_packets(uint32_t source_packets);
/* On-wire repair ceiling. Without ACK this equals repair_packets (the
 * budget). With ACK it is a safety cap of 100% of source so the sender
 * can spray until ACK instead of a fixed redundancy. */
uint32_t wirehair_segment_repair_ceiling(uint32_t source_packets,
                                          uint8_t repair_percent,
                                          bool ack_enabled);

/*
 * Incremental encoder used by the UDP sliding-window sender.  The caller must
 * keep data alive until the tx is destroyed.
 */
WirehairSegmentTx *wirehair_segment_tx_create(
    const WirehairSegmentConfig *config, uint32_t flow_id,
    uint64_t segment_id, uint8_t final_dst, uint8_t ttl,
    const uint8_t *data, size_t data_len);
void wirehair_segment_tx_destroy(WirehairSegmentTx *tx);
int wirehair_segment_tx_emit_source(WirehairSegmentTx *tx,
                                    uint32_t max_packets,
                                    WirehairSegmentEmitFn emit_fn,
                                    void *emit_ctx);
int wirehair_segment_tx_emit_repair(WirehairSegmentTx *tx,
                                    uint32_t max_packets,
                                    WirehairSegmentEmitFn emit_fn,
                                    void *emit_ctx);
bool wirehair_segment_tx_source_complete(const WirehairSegmentTx *tx);
bool wirehair_segment_tx_repair_exhausted(const WirehairSegmentTx *tx);
const WirehairSegmentSendStats *wirehair_segment_tx_stats(
    const WirehairSegmentTx *tx);

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
uint64_t wirehair_segment_receiver_ahead_drops(
    const WirehairSegmentReceiver *receiver);

#endif /* WIREHAIR_SEGMENT_H */
