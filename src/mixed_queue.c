#include "mixed_queue.h"

#include <stdlib.h>

static int valid_queue(const MixedQueue *mq)
{
    return mq != NULL && mq->slots != NULL && mq->capacity > 0;
}

MixedQueueStatus mixed_queue_init(MixedQueue *mq, size_t capacity)
{
    if (mq == NULL || capacity == 0) {
        return MQ_ERR_INVALID;
    }

    mq->slots = calloc(capacity, sizeof(*mq->slots));
    if (mq->slots == NULL) {
        return MQ_ERR_ALLOC;
    }

    mq->capacity = capacity;
    mq->count = 0;
    mq->head = 0;
    mq->tail = 0;
    mq->shutdown = 0;

    if (pthread_mutex_init(&mq->mutex, NULL) != 0) {
        free(mq->slots);
        mq->slots = NULL;
        return MQ_ERR_ALLOC;
    }

    if (pthread_cond_init(&mq->not_full, NULL) != 0) {
        pthread_mutex_destroy(&mq->mutex);
        free(mq->slots);
        mq->slots = NULL;
        return MQ_ERR_ALLOC;
    }

    if (pthread_cond_init(&mq->not_empty, NULL) != 0) {
        pthread_cond_destroy(&mq->not_full);
        pthread_mutex_destroy(&mq->mutex);
        free(mq->slots);
        mq->slots = NULL;
        return MQ_ERR_ALLOC;
    }

    return MQ_OK;
}

void mixed_queue_destroy(MixedQueue *mq)
{
    if (mq == NULL) {
        return;
    }

    if (mq->slots != NULL) {
        pthread_mutex_lock(&mq->mutex);
        while (mq->count > 0) {
            DataPacket *pkt = mq->slots[mq->head];

            mq->head = (mq->head + 1) % mq->capacity;
            mq->count--;
            packet_free(pkt);
        }
        pthread_mutex_unlock(&mq->mutex);
    }

    pthread_cond_destroy(&mq->not_empty);
    pthread_cond_destroy(&mq->not_full);
    pthread_mutex_destroy(&mq->mutex);

    free(mq->slots);
    mq->slots = NULL;
    mq->capacity = 0;
    mq->count = 0;
    mq->head = 0;
    mq->tail = 0;
    mq->shutdown = 0;
}

void mixed_queue_shutdown(MixedQueue *mq)
{
    if (mq == NULL || mq->slots == NULL) {
        return;
    }

    pthread_mutex_lock(&mq->mutex);
    mq->shutdown = 1;
    pthread_cond_broadcast(&mq->not_full);
    pthread_cond_broadcast(&mq->not_empty);
    pthread_mutex_unlock(&mq->mutex);
}

MixedQueueStatus mixed_queue_push(MixedQueue *mq, DataPacket **pkt)
{
    if (!valid_queue(mq) || pkt == NULL || *pkt == NULL) {
        return MQ_ERR_INVALID;
    }

    pthread_mutex_lock(&mq->mutex);

    while (mq->count == mq->capacity && !mq->shutdown) {
        pthread_cond_wait(&mq->not_full, &mq->mutex);
    }

    if (mq->shutdown) {
        pthread_mutex_unlock(&mq->mutex);
        return MQ_ERR_SHUTDOWN;
    }

    mq->slots[mq->tail] = *pkt;
    mq->tail = (mq->tail + 1) % mq->capacity;
    mq->count++;
    *pkt = NULL;

    pthread_cond_signal(&mq->not_empty);
    pthread_mutex_unlock(&mq->mutex);

    return MQ_OK;
}

MixedQueueStatus mixed_queue_pop(MixedQueue *mq, DataPacket **pkt)
{
    if (!valid_queue(mq) || pkt == NULL) {
        return MQ_ERR_INVALID;
    }

    pthread_mutex_lock(&mq->mutex);

    while (mq->count == 0 && !mq->shutdown) {
        pthread_cond_wait(&mq->not_empty, &mq->mutex);
    }

    if (mq->count == 0 && mq->shutdown) {
        pthread_mutex_unlock(&mq->mutex);
        return MQ_ERR_SHUTDOWN;
    }

    *pkt = mq->slots[mq->head];
    mq->slots[mq->head] = NULL;
    mq->head = (mq->head + 1) % mq->capacity;
    mq->count--;

    pthread_cond_signal(&mq->not_full);
    pthread_mutex_unlock(&mq->mutex);

    return MQ_OK;
}

MixedQueueStatus mixed_queue_try_push(MixedQueue *mq, DataPacket **pkt)
{
    if (!valid_queue(mq) || pkt == NULL || *pkt == NULL) {
        return MQ_ERR_INVALID;
    }

    pthread_mutex_lock(&mq->mutex);

    if (mq->shutdown) {
        pthread_mutex_unlock(&mq->mutex);
        return MQ_ERR_SHUTDOWN;
    }

    if (mq->count == mq->capacity) {
        pthread_mutex_unlock(&mq->mutex);
        return MQ_ERR_FULL;
    }

    mq->slots[mq->tail] = *pkt;
    mq->tail = (mq->tail + 1) % mq->capacity;
    mq->count++;
    *pkt = NULL;

    pthread_cond_signal(&mq->not_empty);
    pthread_mutex_unlock(&mq->mutex);

    return MQ_OK;
}

MixedQueueStatus mixed_queue_try_pop(MixedQueue *mq, DataPacket **pkt)
{
    if (!valid_queue(mq) || pkt == NULL) {
        return MQ_ERR_INVALID;
    }

    pthread_mutex_lock(&mq->mutex);

    if (mq->count == 0) {
        MixedQueueStatus st = mq->shutdown ? MQ_ERR_SHUTDOWN : MQ_ERR_EMPTY;
        pthread_mutex_unlock(&mq->mutex);
        return st;
    }

    *pkt = mq->slots[mq->head];
    mq->slots[mq->head] = NULL;
    mq->head = (mq->head + 1) % mq->capacity;
    mq->count--;

    pthread_cond_signal(&mq->not_full);
    pthread_mutex_unlock(&mq->mutex);

    return MQ_OK;
}

size_t mixed_queue_count(const MixedQueue *mq)
{
    size_t count;

    if (!valid_queue(mq)) {
        return 0;
    }

    pthread_mutex_lock((pthread_mutex_t *)&mq->mutex);
    count = mq->count;
    pthread_mutex_unlock((pthread_mutex_t *)&mq->mutex);

    return count;
}

int mixed_queue_is_empty(const MixedQueue *mq)
{
    return mixed_queue_count(mq) == 0;
}

int mixed_queue_is_full(const MixedQueue *mq)
{
    size_t count;
    size_t capacity;

    if (!valid_queue(mq)) {
        return 0;
    }

    pthread_mutex_lock((pthread_mutex_t *)&mq->mutex);
    count = mq->count;
    capacity = mq->capacity;
    pthread_mutex_unlock((pthread_mutex_t *)&mq->mutex);

    return count == capacity;
}
