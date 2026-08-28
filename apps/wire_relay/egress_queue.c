#include "egress_queue.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static uint64_t mono_ns_now(void)
{
    struct timespec now;

    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0) {
        return 0;
    }
    return (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
}

static void mono_deadline_from_ms(uint32_t timeout_ms, struct timespec *out)
{
    struct timespec now;
    uint64_t deadline_ns;

    clock_gettime(CLOCK_MONOTONIC, &now);
    deadline_ns = (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec +
                  (uint64_t)timeout_ms * 1000000ull;
    out->tv_sec = (time_t)(deadline_ns / 1000000000ull);
    out->tv_nsec = (long)(deadline_ns % 1000000000ull);
}

static void record_enqueue_stats(EgressQueue *q, int waited, uint64_t wait_ns)
{
    if (waited) {
        q->stats.enqueue_waited++;
        q->stats.wait_ns_total += wait_ns;
        if (wait_ns > q->stats.wait_ns_max) {
            q->stats.wait_ns_max = wait_ns;
        }
    } else {
        q->stats.enqueue_immediate++;
    }
    if (q->count > q->stats.high_watermark) {
        q->stats.high_watermark = q->count;
    }
}

static void waiter_notify(EgressQueueWaiter *waiter)
{
    if (waiter == NULL) {
        return;
    }
    pthread_mutex_lock(&waiter->mutex);
    waiter->generation++;
    pthread_cond_signal(&waiter->changed);
    pthread_mutex_unlock(&waiter->mutex);
}

static EgressStatus enqueue_locked(EgressQueue *q, EgressPacket *pkt, int waited,
                                   uint64_t wait_ns)
{
    q->slots[q->tail] = *pkt;
    pkt->datagram = NULL;
    pkt->len = 0;
    q->tail = (q->tail + 1u) % q->capacity;
    q->count++;
    record_enqueue_stats(q, waited, wait_ns);
    pthread_cond_signal(&q->not_empty);
    return EGRESS_OK;
}

EgressStatus egress_queue_init(EgressQueue *q, size_t capacity)
{
    pthread_condattr_t attr;

    if (q == NULL || capacity == 0) {
        return EGRESS_ERR_INVALID;
    }
    memset(q, 0, sizeof(*q));
    q->slots = calloc(capacity, sizeof(*q->slots));
    if (q->slots == NULL) {
        return EGRESS_ERR_ALLOC;
    }
    q->capacity = capacity;
    if (pthread_mutex_init(&q->mutex, NULL) != 0) {
        free(q->slots);
        q->slots = NULL;
        return EGRESS_ERR_ALLOC;
    }
    if (pthread_cond_init(&q->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&q->mutex);
        free(q->slots);
        q->slots = NULL;
        return EGRESS_ERR_ALLOC;
    }
    if (pthread_condattr_init(&attr) != 0) {
        pthread_cond_destroy(&q->not_empty);
        pthread_mutex_destroy(&q->mutex);
        free(q->slots);
        q->slots = NULL;
        return EGRESS_ERR_ALLOC;
    }
    if (pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) != 0) {
        pthread_condattr_destroy(&attr);
        pthread_cond_destroy(&q->not_empty);
        pthread_mutex_destroy(&q->mutex);
        free(q->slots);
        q->slots = NULL;
        return EGRESS_ERR_ALLOC;
    }
    if (pthread_cond_init(&q->not_full, &attr) != 0) {
        pthread_condattr_destroy(&attr);
        pthread_cond_destroy(&q->not_empty);
        pthread_mutex_destroy(&q->mutex);
        free(q->slots);
        q->slots = NULL;
        return EGRESS_ERR_ALLOC;
    }
    pthread_condattr_destroy(&attr);
    return EGRESS_OK;
}

void egress_queue_destroy(EgressQueue *q)
{
    size_t i;

    if (q == NULL) {
        return;
    }
    if (q->slots != NULL) {
        for (i = 0; i < q->capacity; i++) {
            free(q->slots[i].datagram);
            q->slots[i].datagram = NULL;
        }
        free(q->slots);
        q->slots = NULL;
    }
    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
    pthread_mutex_destroy(&q->mutex);
    memset(q, 0, sizeof(*q));
}

void egress_queue_shutdown(EgressQueue *q)
{
    EgressQueueWaiter *waiter;

    if (q == NULL) {
        return;
    }
    pthread_mutex_lock(&q->mutex);
    q->shutdown = 1;
    waiter = q->waiter;
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
    waiter_notify(waiter);
}

EgressStatus egress_queue_try_enqueue(EgressQueue *q, EgressPacket *pkt)
{
    EgressStatus st;
    EgressQueueWaiter *waiter;

    if (q == NULL || pkt == NULL || pkt->datagram == NULL || pkt->len == 0) {
        return EGRESS_ERR_INVALID;
    }

    pthread_mutex_lock(&q->mutex);
    if (q->shutdown) {
        pthread_mutex_unlock(&q->mutex);
        return EGRESS_ERR_SHUTDOWN;
    }
    if (q->count >= q->capacity) {
        pthread_mutex_unlock(&q->mutex);
        return EGRESS_ERR_FULL;
    }
    st = enqueue_locked(q, pkt, 0, 0);
    waiter = q->waiter;
    pthread_mutex_unlock(&q->mutex);
    waiter_notify(waiter);
    return st;
}

EgressStatus egress_queue_timed_enqueue(EgressQueue *q, EgressPacket *pkt,
                                        uint32_t timeout_ms)
{
    struct timespec deadline;
    uint64_t wait_start_ns = 0;
    int waited = 0;
    EgressStatus st;
    EgressQueueWaiter *waiter;

    if (q == NULL || pkt == NULL || pkt->datagram == NULL || pkt->len == 0 ||
        timeout_ms == 0) {
        return EGRESS_ERR_INVALID;
    }

    mono_deadline_from_ms(timeout_ms, &deadline);

    pthread_mutex_lock(&q->mutex);
    if (q->shutdown) {
        pthread_mutex_unlock(&q->mutex);
        return EGRESS_ERR_SHUTDOWN;
    }
    if (q->count < q->capacity) {
        st = enqueue_locked(q, pkt, 0, 0);
        waiter = q->waiter;
        pthread_mutex_unlock(&q->mutex);
        waiter_notify(waiter);
        return st;
    }

    wait_start_ns = mono_ns_now();
    while (q->count >= q->capacity && !q->shutdown) {
        int rc;

        waited = 1;
        rc = pthread_cond_timedwait(&q->not_full, &q->mutex, &deadline);
        if (rc == ETIMEDOUT) {
            pthread_mutex_unlock(&q->mutex);
            return EGRESS_ERR_TIMEOUT;
        }
        if (rc != 0 && rc != EINTR) {
            pthread_mutex_unlock(&q->mutex);
            return EGRESS_ERR_INVALID;
        }
    }
    if (q->shutdown) {
        pthread_mutex_unlock(&q->mutex);
        return EGRESS_ERR_SHUTDOWN;
    }
    st = enqueue_locked(q, pkt, waited, mono_ns_now() - wait_start_ns);
    waiter = q->waiter;
    pthread_mutex_unlock(&q->mutex);
    waiter_notify(waiter);
    return st;
}

static EgressStatus dequeue_locked(EgressQueue *q, EgressPacket *out)
{
    if (q->count == 0) {
        return q->shutdown ? EGRESS_ERR_SHUTDOWN : EGRESS_ERR_EMPTY;
    }
    *out = q->slots[q->head];
    memset(&q->slots[q->head], 0, sizeof(q->slots[q->head]));
    q->head = (q->head + 1u) % q->capacity;
    q->count--;
    pthread_cond_signal(&q->not_full);
    return EGRESS_OK;
}

EgressStatus egress_queue_dequeue(EgressQueue *q, EgressPacket *out)
{
    EgressStatus st;

    if (q == NULL || out == NULL) {
        return EGRESS_ERR_INVALID;
    }

    pthread_mutex_lock(&q->mutex);
    while (q->count == 0 && !q->shutdown) {
        pthread_cond_wait(&q->not_empty, &q->mutex);
    }
    st = dequeue_locked(q, out);
    pthread_mutex_unlock(&q->mutex);
    return st;
}

EgressStatus egress_queue_try_dequeue(EgressQueue *q, EgressPacket *out)
{
    EgressStatus st;

    if (q == NULL || out == NULL) {
        return EGRESS_ERR_INVALID;
    }
    pthread_mutex_lock(&q->mutex);
    st = dequeue_locked(q, out);
    pthread_mutex_unlock(&q->mutex);
    return st;
}

size_t egress_queue_count(const EgressQueue *q)
{
    size_t n;

    if (q == NULL) {
        return 0;
    }
    pthread_mutex_lock((pthread_mutex_t *)&q->mutex);
    n = q->count;
    pthread_mutex_unlock((pthread_mutex_t *)&q->mutex);
    return n;
}

void egress_queue_stats_snapshot(const EgressQueue *q, EgressQueueStats *out)
{
    if (q == NULL || out == NULL) {
        return;
    }
    pthread_mutex_lock((pthread_mutex_t *)&q->mutex);
    *out = q->stats;
    pthread_mutex_unlock((pthread_mutex_t *)&q->mutex);
}

static void queue_set_waiter(EgressQueue *q, EgressQueueWaiter *waiter)
{
    pthread_mutex_lock(&q->mutex);
    q->waiter = waiter;
    pthread_mutex_unlock(&q->mutex);
}

EgressStatus egress_fair_dequeuer_init(EgressFairDequeuer *d,
                                       EgressQueue *ack, EgressQueue *data,
                                       unsigned ack_quota)
{
    if (d == NULL || ack == NULL || data == NULL || ack == data ||
        ack_quota == 0) {
        return EGRESS_ERR_INVALID;
    }
    memset(d, 0, sizeof(*d));
    if (pthread_mutex_init(&d->waiter.mutex, NULL) != 0) {
        return EGRESS_ERR_ALLOC;
    }
    if (pthread_cond_init(&d->waiter.changed, NULL) != 0) {
        pthread_mutex_destroy(&d->waiter.mutex);
        return EGRESS_ERR_ALLOC;
    }
    d->ack = ack;
    d->data = data;
    d->ack_quota = ack_quota;
    d->initialized = 1;
    queue_set_waiter(ack, &d->waiter);
    queue_set_waiter(data, &d->waiter);
    return EGRESS_OK;
}

void egress_fair_dequeuer_destroy(EgressFairDequeuer *d)
{
    if (d == NULL || !d->initialized) {
        return;
    }
    queue_set_waiter(d->ack, NULL);
    queue_set_waiter(d->data, NULL);
    pthread_cond_destroy(&d->waiter.changed);
    pthread_mutex_destroy(&d->waiter.mutex);
    memset(d, 0, sizeof(*d));
}

EgressStatus egress_fair_dequeue(EgressFairDequeuer *d, EgressPacket *out)
{
    if (d == NULL || out == NULL || !d->initialized) {
        return EGRESS_ERR_INVALID;
    }

    while (1) {
        EgressStatus ack_st;
        EgressStatus data_st;
        uint64_t observed;

        pthread_mutex_lock(&d->waiter.mutex);
        observed = d->waiter.generation;
        pthread_mutex_unlock(&d->waiter.mutex);

        if (d->consecutive_acks >= d->ack_quota) {
            data_st = egress_queue_try_dequeue(d->data, out);
            if (data_st == EGRESS_OK) {
                d->consecutive_acks = 0;
                return EGRESS_OK;
            }
            ack_st = egress_queue_try_dequeue(d->ack, out);
            if (ack_st == EGRESS_OK) {
                return EGRESS_OK;
            }
        } else {
            ack_st = egress_queue_try_dequeue(d->ack, out);
            if (ack_st == EGRESS_OK) {
                d->consecutive_acks++;
                return EGRESS_OK;
            }
            data_st = egress_queue_try_dequeue(d->data, out);
            if (data_st == EGRESS_OK) {
                d->consecutive_acks = 0;
                return EGRESS_OK;
            }
        }

        if (ack_st == EGRESS_ERR_SHUTDOWN &&
            data_st == EGRESS_ERR_SHUTDOWN) {
            return EGRESS_ERR_SHUTDOWN;
        }
        if ((ack_st != EGRESS_ERR_EMPTY &&
             ack_st != EGRESS_ERR_SHUTDOWN) ||
            (data_st != EGRESS_ERR_EMPTY &&
             data_st != EGRESS_ERR_SHUTDOWN)) {
            return EGRESS_ERR_INVALID;
        }

        pthread_mutex_lock(&d->waiter.mutex);
        while (observed == d->waiter.generation) {
            pthread_cond_wait(&d->waiter.changed, &d->waiter.mutex);
        }
        pthread_mutex_unlock(&d->waiter.mutex);
    }
}
