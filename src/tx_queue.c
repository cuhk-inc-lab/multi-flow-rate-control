#include "tx_queue.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static uint64_t txq_now_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

static void txq_free_packet(const TxqPacket *pkt)
{
    if (pkt == NULL || pkt->payload == NULL) {
        return;
    }
    if (pkt->free_fn != NULL) {
        pkt->free_fn(pkt->payload, pkt->free_ctx);
    } else {
        free(pkt->payload);
    }
}

/*
 * Whether enqueuing a packet of `len` bytes would overflow.
 *
 * The byte limit only blocks when the queue is non-empty, so a single packet
 * larger than capacity_bytes can still make progress on an empty queue
 * (otherwise BLOCK would wait forever and DROP_HEAD could never accept it).
 */
static int txq_full_locked(const TxQueue *q, size_t len)
{
    if (q->count >= q->capacity_items) {
        return 1;
    }
    if (q->capacity_bytes != 0 && q->bytes != 0 &&
        q->bytes + len > q->capacity_bytes) {
        return 1;
    }
    return 0;
}

/* Remove and free the front packet. Caller holds mutex. */
static void txq_evict_head_locked(TxQueue *q)
{
    TxqPacket *pkt = &q->slots[q->head];

    q->bytes -= pkt->length;
    txq_free_packet(pkt);
    q->head = (q->head + 1u) % q->capacity_items;
    q->count--;
}

static void txq_insert_tail_locked(TxQueue *q, const TxqPacket *pkt)
{
    q->slots[q->tail] = *pkt;
    q->tail = (q->tail + 1u) % q->capacity_items;
    q->count++;
    q->bytes += pkt->length;
}

static void txq_insert_head_locked(TxQueue *q, const TxqPacket *pkt)
{
    size_t slot = (q->head + q->capacity_items - 1u) % q->capacity_items;

    q->slots[slot] = *pkt;
    q->head = slot;
    q->count++;
    q->bytes += pkt->length;
}

TxqStatus txq_init(TxQueue *q,
                   size_t capacity_items,
                   size_t capacity_bytes,
                   TxqOverflowPolicy policy)
{
    pthread_condattr_t cattr;

    if (q == NULL || capacity_items == 0) {
        return TXQ_ERR_INVALID;
    }

    memset(q, 0, sizeof(*q));
    q->slots = calloc(capacity_items, sizeof(*q->slots));
    if (q->slots == NULL) {
        return TXQ_ERR_ALLOC;
    }

    if (pthread_condattr_init(&cattr) != 0) {
        free(q->slots);
        q->slots = NULL;
        return TXQ_ERR_ALLOC;
    }
    if (pthread_condattr_setclock(&cattr, CLOCK_MONOTONIC) != 0) {
        pthread_condattr_destroy(&cattr);
        free(q->slots);
        q->slots = NULL;
        return TXQ_ERR_ALLOC;
    }

    if (pthread_mutex_init(&q->mutex, NULL) != 0) {
        pthread_condattr_destroy(&cattr);
        free(q->slots);
        q->slots = NULL;
        return TXQ_ERR_ALLOC;
    }
    if (pthread_cond_init(&q->cond_readable, &cattr) != 0) {
        pthread_mutex_destroy(&q->mutex);
        pthread_condattr_destroy(&cattr);
        free(q->slots);
        q->slots = NULL;
        return TXQ_ERR_ALLOC;
    }
    if (pthread_cond_init(&q->cond_not_full, &cattr) != 0) {
        pthread_cond_destroy(&q->cond_readable);
        pthread_mutex_destroy(&q->mutex);
        pthread_condattr_destroy(&cattr);
        free(q->slots);
        q->slots = NULL;
        return TXQ_ERR_ALLOC;
    }
    pthread_condattr_destroy(&cattr);

    q->capacity_items = capacity_items;
    q->capacity_bytes = capacity_bytes;
    q->policy = policy;
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    q->bytes = 0;
    q->shutdown = 0;
    q->gate_fn = NULL;
    q->gate_ctx = NULL;
    return TXQ_OK;
}

void txq_destroy(TxQueue *q)
{
    size_t i;

    if (q == NULL || q->slots == NULL) {
        return;
    }

    pthread_mutex_lock(&q->mutex);
    for (i = 0; i < q->count; i++) {
        size_t idx = (q->head + i) % q->capacity_items;
        txq_free_packet(&q->slots[idx]);
    }
    q->count = 0;
    q->bytes = 0;
    pthread_mutex_unlock(&q->mutex);

    pthread_cond_destroy(&q->cond_readable);
    pthread_cond_destroy(&q->cond_not_full);
    pthread_mutex_destroy(&q->mutex);

    free(q->slots);
    q->slots = NULL;
}

void txq_shutdown(TxQueue *q)
{
    if (q == NULL) {
        return;
    }
    pthread_mutex_lock(&q->mutex);
    q->shutdown = 1;
    pthread_cond_broadcast(&q->cond_readable);
    pthread_cond_broadcast(&q->cond_not_full);
    pthread_mutex_unlock(&q->mutex);
}

void txq_set_release_gate(TxQueue *q, TxqReleaseGate fn, void *ctx)
{
    if (q == NULL) {
        return;
    }
    pthread_mutex_lock(&q->mutex);
    q->gate_fn = fn;
    q->gate_ctx = ctx;
    /* A newly-installed gate may have opened the release; wake consumers. */
    pthread_cond_broadcast(&q->cond_readable);
    pthread_mutex_unlock(&q->mutex);
}

void txq_release_notify(TxQueue *q)
{
    if (q == NULL) {
        return;
    }
    pthread_mutex_lock(&q->mutex);
    pthread_cond_broadcast(&q->cond_readable);
    pthread_mutex_unlock(&q->mutex);
}

/*
 * Core enqueue. `front` selects head vs tail insertion. `blocking` selects
 * policy-respecting behavior (BLOCK waits) vs strict try (never blocks).
 *
 * On every non-OK return the caller retains ownership of pkt->payload.
 */
static TxqStatus txq_enqueue(TxQueue *q, TxqPacket *pkt, int front, int blocking)
{
    TxqStatus result = TXQ_OK;

    if (q == NULL || pkt == NULL || pkt->payload == NULL || pkt->length == 0) {
        return TXQ_ERR_INVALID;
    }

    pthread_mutex_lock(&q->mutex);

    if (q->shutdown) {
        pthread_mutex_unlock(&q->mutex);
        return TXQ_ERR_SHUTDOWN;
    }

    if (pkt->enqueue_ns == 0) {
        pkt->enqueue_ns = txq_now_ns();
    }

    for (;;) {
        if (!txq_full_locked(q, pkt->length)) {
            break;
        }
        if (q->policy == TXQ_POLICY_DROP_HEAD) {
            /* Evict oldest until it fits. If empty and still too large
             * (only possible under a byte cap with one oversized packet),
             * fall through and accept it anyway to avoid a permanent stall. */
            while (q->count != 0 && txq_full_locked(q, pkt->length)) {
                txq_evict_head_locked(q);
            }
            break;
        }
        if (!blocking || q->policy != TXQ_POLICY_BLOCK) {
            /* TRY (or non-blocking caller on any policy): decline. */
            result = TXQ_ERR_FULL;
            goto out;
        }
        /* BLOCK: wait for space or shutdown. */
        pthread_cond_wait(&q->cond_not_full, &q->mutex);
        if (q->shutdown) {
            result = TXQ_ERR_SHUTDOWN;
            goto out;
        }
    }

    if (front) {
        txq_insert_head_locked(q, pkt);
    } else {
        txq_insert_tail_locked(q, pkt);
    }
    pthread_cond_signal(&q->cond_readable);
    result = TXQ_OK;

out:
    pthread_mutex_unlock(&q->mutex);
    return result;
}

TxqStatus txq_push(TxQueue *q, TxqPacket *pkt)
{
    return txq_enqueue(q, pkt, 0, 1);
}

TxqStatus txq_try_push(TxQueue *q, TxqPacket *pkt)
{
    return txq_enqueue(q, pkt, 0, 0);
}

TxqStatus txq_push_front(TxQueue *q, TxqPacket *pkt)
{
    return txq_enqueue(q, pkt, 1, 1);
}

TxqStatus txq_pop(TxQueue *q, TxqPacket *out)
{
    if (q == NULL || out == NULL) {
        return TXQ_ERR_INVALID;
    }

    pthread_mutex_lock(&q->mutex);

    for (;;) {
        if (q->count == 0) {
            if (q->shutdown) {
                pthread_mutex_unlock(&q->mutex);
                return TXQ_ERR_SHUTDOWN;
            }
            pthread_cond_wait(&q->cond_readable, &q->mutex);
            continue;
        }

        if (q->gate_fn != NULL) {
            TxqPacket *front_pkt = &q->slots[q->head];
            uint64_t delay = q->gate_fn(q, front_pkt, q->gate_ctx);

            if (delay != 0) {
                if (q->shutdown) {
                    /* Allow drain on shutdown even if gated. */
                } else {
                    struct timespec deadline;
                    uint64_t now = txq_now_ns();
                    uint64_t abstime = now + delay;

                    deadline.tv_sec = (time_t)(abstime / UINT64_C(1000000000));
                    deadline.tv_nsec = (long)(abstime % UINT64_C(1000000000));
                    pthread_cond_timedwait(&q->cond_readable, &q->mutex,
                                           &deadline);
                    continue;
                }
            }
        }

        /* Release the front packet. */
        *out = q->slots[q->head];
        q->head = (q->head + 1u) % q->capacity_items;
        q->count--;
        q->bytes -= out->length;
        pthread_cond_signal(&q->cond_not_full);
        pthread_mutex_unlock(&q->mutex);
        return TXQ_OK;
    }
}

TxqStatus txq_try_pop(TxQueue *q, TxqPacket *out)
{
    TxqPacket *front_pkt;

    if (q == NULL || out == NULL) {
        return TXQ_ERR_INVALID;
    }

    pthread_mutex_lock(&q->mutex);

    if (q->count == 0) {
        pthread_mutex_unlock(&q->mutex);
        return TXQ_ERR_EMPTY;
    }

    if (q->gate_fn != NULL) {
        front_pkt = &q->slots[q->head];
        if (q->gate_fn(q, front_pkt, q->gate_ctx) != 0) {
            pthread_mutex_unlock(&q->mutex);
            return TXQ_ERR_GATED;
        }
    }

    *out = q->slots[q->head];
    q->head = (q->head + 1u) % q->capacity_items;
    q->count--;
    q->bytes -= out->length;
    pthread_cond_signal(&q->cond_not_full);
    pthread_mutex_unlock(&q->mutex);
    return TXQ_OK;
}

size_t txq_count(const TxQueue *q)
{
    size_t count;

    if (q == NULL) {
        return 0;
    }
    pthread_mutex_lock((pthread_mutex_t *)&q->mutex);
    count = q->count;
    pthread_mutex_unlock((pthread_mutex_t *)&q->mutex);
    return count;
}

size_t txq_bytes(const TxQueue *q)
{
    size_t bytes;

    if (q == NULL) {
        return 0;
    }
    pthread_mutex_lock((pthread_mutex_t *)&q->mutex);
    bytes = q->bytes;
    pthread_mutex_unlock((pthread_mutex_t *)&q->mutex);
    return bytes;
}

size_t txq_capacity_items(const TxQueue *q)
{
    if (q == NULL) {
        return 0;
    }
    return q->capacity_items;
}

size_t txq_capacity_bytes(const TxQueue *q)
{
    if (q == NULL) {
        return 0;
    }
    return q->capacity_bytes;
}
