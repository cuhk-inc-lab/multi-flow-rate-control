#ifndef PACKET_POOL_H
#define PACKET_POOL_H

/*
 * Preallocated DataPacket + fixed-size payload slabs.
 * packet_free() returns pooled packets to the freelist.
 */

#include <stddef.h>
#include <stdint.h>

#include "packet.h"

typedef struct PacketPool PacketPool;

typedef enum {
    PP_OK = 0,
    PP_ERR_INVALID = -1,
    PP_ERR_ALLOC = -2,
    PP_ERR_EMPTY = -3
} PacketPoolStatus;

/*
 * capacity: number of packets
 * payload_cap: bytes per payload slab (e.g. PKG_SIZE)
 */
PacketPoolStatus packet_pool_init(PacketPool **out,
                                  size_t capacity,
                                  size_t payload_cap);
void             packet_pool_destroy(PacketPool *pool);

size_t packet_pool_capacity(const PacketPool *pool);
size_t packet_pool_payload_cap(const PacketPool *pool);
size_t packet_pool_available(const PacketPool *pool);

/*
 * Allocate a packet with an empty payload buffer of up to payload_cap.
 * Sets flow_id and stamps enqueue_ts. payload_len starts at 0; caller fills
 * payload[0..len) then sets payload_len before push.
 * Returns NULL if pool is empty or on clock failure (packet returned to pool).
 */
DataPacket *packet_pool_alloc(PacketPool *pool, uint32_t flow_id);

/* Return a pooled packet to the freelist. No-op if pkt is not from this pool. */
void packet_pool_release(PacketPool *pool, DataPacket *pkt);

int packet_is_pooled(const DataPacket *pkt);

#endif /* PACKET_POOL_H */
