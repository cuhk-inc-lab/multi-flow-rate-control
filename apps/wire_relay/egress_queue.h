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
    EGRESS_ERR_SHUTDOWN = -5,
    EGRESS_ERR_TIMEOUT = -6
} EgressStatus;

typedef struct EgressQueueStats {
    uint64_t enqueue_immediate;
    uint64_t enqueue_waited;
    uint64_t wait_ns_total;
    uint64_t wait_ns_max;
    uint64_t high_watermark;
} EgressQueueStats;

typedef struct EgressQueueWaiter {
    pthread_mutex_t mutex;
    pthread_cond_t  changed;
    uint64_t        generation;
} EgressQueueWaiter;

typedef struct EgressQueue {
    EgressPacket      *slots;
    size_t             capacity;
    size_t             head;
    size_t             tail;
    size_t             count;
    pthread_mutex_t    mutex;
    pthread_cond_t     not_empty;
    pthread_cond_t     not_full;
    int                shutdown;
    EgressQueueStats   stats;
    EgressQueueWaiter *waiter;
} EgressQueue;

typedef struct EgressFairDequeuer {
    EgressQueue       *ack;
    EgressQueue       *data;
    EgressQueueWaiter  waiter;
    unsigned           ack_quota;
    unsigned           consecutive_acks;
    int                initialized;
} EgressFairDequeuer;

EgressStatus egress_queue_init(EgressQueue *q, size_t capacity);
void         egress_queue_destroy(EgressQueue *q);
void         egress_queue_shutdown(EgressQueue *q);

/*
 * Non-blocking enqueue. On success, ownership of pkt->datagram moves into the
 * queue and pkt->datagram is set to NULL. On failure, ownership remains with
 * caller and pkt->datagram is unchanged.
 */
EgressStatus egress_queue_try_enqueue(EgressQueue *q, EgressPacket *pkt);

/*
 * Timed blocking enqueue when the queue is full.
 *
 * Ownership contract (same as try_enqueue):
 *   EGRESS_OK:        pkt->datagram moves into the queue; set to NULL.
 *   EGRESS_ERR_TIMEOUT / EGRESS_ERR_SHUTDOWN / EGRESS_ERR_INVALID:
 *                     ownership stays with caller; pkt->datagram unchanged.
 *
 * timeout_ms must be > 0. Waits at most timeout_ms (CLOCK_MONOTONIC) for
 * space; does not drop or replace existing queue entries.
 */
EgressStatus egress_queue_timed_enqueue(EgressQueue *q, EgressPacket *pkt,
                                        uint32_t timeout_ms);

/*
 * Blocking dequeue until a packet is available or shutdown with empty queue.
 * On success, ownership of out->datagram moves to caller.
 */
EgressStatus egress_queue_dequeue(EgressQueue *q, EgressPacket *out);

/*
 * Non-blocking dequeue. Returns EMPTY while an active queue has no packet,
 * and SHUTDOWN only when a shutdown queue has been drained.
 */
EgressStatus egress_queue_try_dequeue(EgressQueue *q, EgressPacket *out);

size_t egress_queue_count(const EgressQueue *q);

/* Thread-safe snapshot of queue-global enqueue/wait metrics. */
void egress_queue_stats_snapshot(const EgressQueue *q, EgressQueueStats *out);

/*
 * Blocking fair dequeue across two queues. ACK is preferred, but after
 * ack_quota consecutive ACK packets one DATA packet is selected when
 * available. A shared notification prevents polling when both queues are
 * empty. Both queues must outlive the dequeuer.
 */
EgressStatus egress_fair_dequeuer_init(EgressFairDequeuer *d,
                                       EgressQueue *ack, EgressQueue *data,
                                       unsigned ack_quota);
void egress_fair_dequeuer_destroy(EgressFairDequeuer *d);
EgressStatus egress_fair_dequeue(EgressFairDequeuer *d, EgressPacket *out);

#endif /* WIRE_RELAY_EGRESS_QUEUE_H */
