#ifndef PACKET_H
#define PACKET_H

/*
 * DataPacket: flow-scoped unit moved through mixed and per-flow queues.
 * Payload is heap-owned unless created with packet_adopt().
 */

#include <stddef.h>
#include <stdint.h>
#include <time.h>

typedef struct DataPacket {
    uint32_t        flow_id;
    struct timespec enqueue_ts;
    size_t          payload_len;
    unsigned char  *payload;
    void           *user_data;
} DataPacket;

typedef void (*PacketPayloadFreeFn)(void *payload, void *ctx);

typedef struct PacketAdoptCtx {
    PacketPayloadFreeFn free_fn;
    void               *free_ctx;
} PacketAdoptCtx;

/* Allocate an empty packet with a copied payload. Caller owns until handed off. */
DataPacket *packet_create(uint32_t flow_id, const void *data, size_t len);

/*
 * Wrap caller-owned payload. free_fn is invoked by packet_free(); may be NULL
 * (payload must then be freed by the caller after packet_free()).
 */
DataPacket *packet_adopt(uint32_t flow_id,
                         unsigned char *payload,
                         size_t len,
                         PacketPayloadFreeFn free_fn,
                         void *free_ctx);

void packet_free(DataPacket *pkt);

/* Stamp enqueue_ts from CLOCK_MONOTONIC. */
int packet_stamp_now(DataPacket *pkt);

#endif /* PACKET_H */
