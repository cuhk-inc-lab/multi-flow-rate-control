#ifndef PACKET_H
#define PACKET_H

/*
 * DataPacket: flow-scoped unit moved through mixed and per-flow queues.
 *
 * Queues store DataPacket pointers only. Payload bytes are not copied on route.
 * Ownership transfers at queue boundaries via DataPacket **.
 */

#include <stddef.h>
#include <stdint.h>
#include <time.h>

typedef enum {
    PKT_OK = 0,
    PKT_ERR_INVALID = -1,
    PKT_ERR_ALLOC = -2,
    PKT_ERR_SYSTEM = -3
} PacketStatus;

typedef struct DataPacket {
    uint32_t        flow_id;
    struct timespec enqueue_ts;
    size_t          payload_len;
    unsigned char  *payload;
    void           *user_data;
} DataPacket;

typedef void (*PacketPayloadFreeFn)(void *payload, void *ctx);

/*
 * Create a packet with a heap-copied payload.
 * Sets enqueue_ts from CLOCK_MONOTONIC.
 * Returns NULL on allocation or clock failure.
 */
DataPacket *packet_create(uint32_t flow_id, const void *data, size_t len);

/*
 * Wrap caller-owned payload without copying.
 * If free_fn is non-NULL it is called by packet_free(); otherwise the caller
 * must free payload after packet_free() returns.
 */
DataPacket *packet_adopt(uint32_t flow_id,
                         unsigned char *payload,
                         size_t len,
                         PacketPayloadFreeFn free_fn,
                         void *free_ctx);

void packet_free(DataPacket *pkt);

/* Overwrite enqueue_ts with CLOCK_MONOTONIC now. */
PacketStatus packet_stamp_now(DataPacket *pkt);

#endif /* PACKET_H */
