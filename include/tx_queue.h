#ifndef TX_QUEUE_H
#define TX_QUEUE_H

/*
 * tx_queue.h
 *
 * Bounded, backpressure-aware TX queue with a pluggable release gate for
 * congestion control / pacing.
 *
 * Difference from flow_buffer.h:
 *   - Stores arbitrary owned bytes (TxqPacket), not DataPacket*.
 *   - Byte-weighted capacity (for byte-based cwnd), optional.
 *   - Front insertion (push_front) for retransmit priority.
 *   - A release gate consulted before a packet is handed to the consumer.
 *     This is the hook the ARQ/CC core uses to throttle egress by cwnd and
 *     pacing rate. NULL gate => plain bounded FIFO.
 *
 * Ownership:
 *   - Producer owns payload until push*() returns TXQ_OK.
 *   - On any non-OK return the producer retains ownership (must free itself).
 *   - Queue owns the payload while enqueued.
 *   - Consumer receives ownership via pop*() and must release it through
 *     TxqPacket.free_fn (or free() when free_fn == NULL).
 *
 * Threading: pthreads. One or more producers, one or more consumers.
 */

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

typedef struct TxQueue TxQueue;

typedef enum {
    TXQ_OK = 0,
    TXQ_ERR_INVALID = -1,
    TXQ_ERR_ALLOC = -2,
    TXQ_ERR_FULL = -3,       /* try_push on a full queue */
    TXQ_ERR_EMPTY = -4,      /* try_pop on an empty queue */
    TXQ_ERR_SHUTDOWN = -5,
    TXQ_ERR_GATED = -6       /* try_pop: item present but gate currently blocks */
} TxqStatus;

typedef enum {
    TXQ_POLICY_BLOCK,        /* full: push blocks until space (backpressure) */
    TXQ_POLICY_TRY,          /* full: return TXQ_ERR_FULL immediately */
    TXQ_POLICY_DROP_HEAD     /* full: evict the oldest item, then accept */
} TxqOverflowPolicy;

typedef struct {
    void   *payload;                           /* owned bytes */
    size_t  length;
    uint32_t flow_id;
    uint64_t seq;                              /* ARQ sequence; 0 if unused */
    uint64_t enqueue_ns;                       /* CLOCK_MONOTONIC; 0 => stamped on push */
    void    *user;                             /* opaque, not owned, carried through */
    void  (*free_fn)(void *payload, void *ctx);/* NULL => free() */
    void   *free_ctx;
} TxqPacket;

/*
 * Release gate: consulted before the consumer receives the front packet.
 *   returns 0       => release now
 *   returns >0      => nanoseconds to wait before the packet is eligible
 *
 * Implemented and injected by the CC layer. NULL => always release.
 * When the CC layer refreshes its state (cwnd / pacing tokens), it calls
 * txq_release_notify() to wake a consumer blocked inside txq_pop().
 */
typedef uint64_t (*TxqReleaseGate)(TxQueue *q, const TxqPacket *pkt, void *ctx);

struct TxQueue {
    TxqPacket         *slots;
    size_t             capacity_items;
    size_t             capacity_bytes;          /* 0 => no byte limit */
    TxqOverflowPolicy  policy;
    size_t             head;
    size_t             tail;
    size_t             count;
    size_t             bytes;
    pthread_mutex_t    mutex;
    pthread_cond_t     cond_readable;           /* push, release_notify, shutdown */
    pthread_cond_t     cond_not_full;           /* pop freed space, shutdown */
    int                shutdown;
    TxqReleaseGate     gate_fn;
    void              *gate_ctx;
};

/*
 * In-place initialization. capacity_items must be > 0. capacity_bytes == 0
 * disables byte weighting. Uses CLOCK_MONOTONIC for timed waits.
 */
TxqStatus txq_init(TxQueue *q,
                   size_t capacity_items,
                   size_t capacity_bytes,
                   TxqOverflowPolicy policy);

/*
 * Releases all remaining payloads (via free_fn/free), destroys sync prims,
 * and frees the slot array. Does NOT free `q` itself (in-place init).
 * Safe on NULL.
 */
void txq_destroy(TxQueue *q);

/* Marks shutdown, wakes all blocked producers and consumers. Idempotent. */
void txq_shutdown(TxQueue *q);

/* Install / replace the release gate. NULL removes it. */
void txq_set_release_gate(TxQueue *q, TxqReleaseGate fn, void *ctx);

/* Wake consumers blocked in txq_pop() after the gate state has changed. */
void txq_release_notify(TxQueue *q);

/* ---- Producer ---- */

/*
 * Push respecting the queue policy.
 *   BLOCK    : blocks until space is available.
 *   TRY      : never blocks; returns TXQ_ERR_FULL if full.
 *   DROP_HEAD: evicts the oldest item(s) to make room, always succeeds
 *              (unless shut down).
 * Ownership of pkt->payload transfers to the queue on TXQ_OK only.
 */
TxqStatus txq_push(TxQueue *q, TxqPacket *pkt);

/* Non-blocking push regardless of policy. Returns TXQ_ERR_FULL if full. */
TxqStatus txq_try_push(TxQueue *q, TxqPacket *pkt);

/*
 * Push at the front (retransmit priority). Obeys the queue policy when full.
 * A front-insert into a byte-limited queue is charged against the byte budget
 * like any other push.
 */
TxqStatus txq_push_front(TxQueue *q, TxqPacket *pkt);

/* ---- Consumer ---- */

/*
 * Blocking pop. Waits until (a) a packet is available AND (b) the gate
 * releases it. Gate delay is honored via CLOCK_MONOTONIC timed wait, and may
 * be cut short by txq_release_notify(). Ownership transfers to the caller.
 */
TxqStatus txq_pop(TxQueue *q, TxqPacket *out);

/*
 * Non-blocking pop. Returns TXQ_ERR_EMPTY if no packet, TXQ_ERR_GATED if the
 * front packet is present but the gate currently blocks.
 */
TxqStatus txq_try_pop(TxQueue *q, TxqPacket *out);

/* ---- Introspection ---- */

size_t txq_count(const TxQueue *q);
size_t txq_bytes(const TxQueue *q);
size_t txq_capacity_items(const TxQueue *q);
size_t txq_capacity_bytes(const TxQueue *q);

#endif /* TX_QUEUE_H */
