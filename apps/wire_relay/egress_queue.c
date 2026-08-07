#include "egress_queue.h"

#include <stdlib.h>
#include <string.h>

EgressStatus egress_queue_init(EgressQueue *q, size_t capacity)
{
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
    pthread_cond_destroy(&q->not_empty);
    pthread_mutex_destroy(&q->mutex);
    memset(q, 0, sizeof(*q));
}

void egress_queue_shutdown(EgressQueue *q)
{
    if (q == NULL) {
        return;
    }
    pthread_mutex_lock(&q->mutex);
    q->shutdown = 1;
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
}

EgressStatus egress_queue_try_enqueue(EgressQueue *q, EgressPacket *pkt)
{
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
    q->slots[q->tail] = *pkt;
    pkt->datagram = NULL;
    pkt->len = 0;
    q->tail = (q->tail + 1u) % q->capacity;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
    return EGRESS_OK;
}

EgressStatus egress_queue_dequeue(EgressQueue *q, EgressPacket *out)
{
    if (q == NULL || out == NULL) {
        return EGRESS_ERR_INVALID;
    }

    pthread_mutex_lock(&q->mutex);
    while (q->count == 0 && !q->shutdown) {
        pthread_cond_wait(&q->not_empty, &q->mutex);
    }
    if (q->count == 0) {
        pthread_mutex_unlock(&q->mutex);
        return EGRESS_ERR_SHUTDOWN;
    }
    *out = q->slots[q->head];
    memset(&q->slots[q->head], 0, sizeof(q->slots[q->head]));
    q->head = (q->head + 1u) % q->capacity;
    q->count--;
    pthread_mutex_unlock(&q->mutex);
    return EGRESS_OK;
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
