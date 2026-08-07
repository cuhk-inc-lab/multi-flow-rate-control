#ifndef WIRE_RELAY_EGRESS_QUEUE_H
#define WIRE_RELAY_EGRESS_QUEUE_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

typedef struct EgressPacket {
    uint8_t  *datagram;      /* owned complete wire datagram */
    size_t    len;
    uint32_t  flow_id;
    uint64_t  generation_id; /* WireHeader.block_id */
    uint64_t  enqueue_ns;    /* CLOCK_MONOTONIC nanoseconds */
} EgressPacket;

typedef enum {
    EGRESS_OK = 0,
    EGRESS_ERR_INVALID = -1,
    EGRESS_ERR_ALLOC = -2,
    EGRESS_ERR_FULL = -3,
    EGRESS_ERR_EMPTY = -4,
    EGRESS_ERR_SHUTDOWN = -5
} EgressStatus;

typedef struct EgressQueue {
    EgressPacket   *slots;
    size_t          capacity;
    size_t          head;
    size_t          tail;
    size_t          count;
    pthread_mutex_t mutex;
    pthread_cond_t  not_empty;
    int             shutdown;
} EgressQueue;

EgressStatus egress_queue_init(EgressQueue *q, size_t capacity);
void         egress_queue_destroy(EgressQueue *q);
void         egress_queue_shutdown(EgressQueue *q);

/*
 * Non-blocking enqueue. On success, ownership of pkt->datagram moves into the
 * queue; caller must not free it. On failure, ownership remains with caller.
 */
EgressStatus egress_queue_try_enqueue(EgressQueue *q, EgressPacket *pkt);

/*
 * Blocking dequeue until a packet is available or shutdown with empty queue.
 * On success, ownership of out->datagram moves to caller.
 */
EgressStatus egress_queue_dequeue(EgressQueue *q, EgressPacket *out);

size_t egress_queue_count(const EgressQueue *q);

#endif /* WIRE_RELAY_EGRESS_QUEUE_H */
