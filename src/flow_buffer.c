#include "flow_buffer.h"

#include <errno.h>
#include <stdlib.h>

static int valid_buffer(const FlowCircularBuffer *fb)
{
    return fb != NULL && fb->slots != NULL && fb->capacity > 0;
}

FlowBufferStatus flow_buffer_init(FlowCircularBuffer *fb, size_t capacity)
{
    if (fb == NULL || capacity == 0) {
        return FB_ERR_INVALID;
    }

    fb->slots = calloc(capacity, sizeof(*fb->slots));
    if (fb->slots == NULL) {
        return FB_ERR_ALLOC;
    }

    fb->capacity = capacity;
    fb->count = 0;
    fb->head = 0;
    fb->tail = 0;
    fb->shutdown = 0;

    if (pthread_mutex_init(&fb->mutex, NULL) != 0) {
        free(fb->slots);
        fb->slots = NULL;
        return FB_ERR_ALLOC;
    }

    if (pthread_cond_init(&fb->not_full, NULL) != 0) {
        pthread_mutex_destroy(&fb->mutex);
        free(fb->slots);
        fb->slots = NULL;
        return FB_ERR_ALLOC;
    }

    if (pthread_cond_init(&fb->not_empty, NULL) != 0) {
        pthread_cond_destroy(&fb->not_full);
        pthread_mutex_destroy(&fb->mutex);
        free(fb->slots);
        fb->slots = NULL;
        return FB_ERR_ALLOC;
    }

    return FB_OK;
}

void flow_buffer_destroy(FlowCircularBuffer *fb)
{
    if (fb == NULL) {
        return;
    }

    if (fb->slots != NULL) {
        pthread_mutex_lock(&fb->mutex);
        while (fb->count > 0) {
            DataPacket *pkt = fb->slots[fb->head];

            fb->head = (fb->head + 1) % fb->capacity;
            fb->count--;
            packet_free(pkt);
        }
        pthread_mutex_unlock(&fb->mutex);
    }

    pthread_cond_destroy(&fb->not_empty);
    pthread_cond_destroy(&fb->not_full);
    pthread_mutex_destroy(&fb->mutex);

    free(fb->slots);
    fb->slots = NULL;
    fb->capacity = 0;
    fb->count = 0;
    fb->head = 0;
    fb->tail = 0;
    fb->shutdown = 0;
}

void flow_buffer_shutdown(FlowCircularBuffer *fb)
{
    if (fb == NULL || fb->slots == NULL) {
        return;
    }

    pthread_mutex_lock(&fb->mutex);
    fb->shutdown = 1;
    pthread_cond_broadcast(&fb->not_full);
    pthread_cond_broadcast(&fb->not_empty);
    pthread_mutex_unlock(&fb->mutex);
}

static FlowBufferStatus enqueue_locked(FlowCircularBuffer *fb,
                                       DataPacket **pkt,
                                       int block)
{
    if (!valid_buffer(fb) || pkt == NULL || *pkt == NULL) {
        return FB_ERR_INVALID;
    }

    pthread_mutex_lock(&fb->mutex);

    while (fb->count == fb->capacity && !fb->shutdown) {
        if (!block) {
            pthread_mutex_unlock(&fb->mutex);
            return FB_ERR_INVALID;
        }
        pthread_cond_wait(&fb->not_full, &fb->mutex);
    }

    if (fb->shutdown) {
        pthread_mutex_unlock(&fb->mutex);
        return FB_ERR_SHUTDOWN;
    }

    fb->slots[fb->tail] = *pkt;
    fb->tail = (fb->tail + 1) % fb->capacity;
    fb->count++;
    *pkt = NULL;

    pthread_cond_signal(&fb->not_empty);
    pthread_mutex_unlock(&fb->mutex);

    return FB_OK;
}

static FlowBufferStatus dequeue_locked(FlowCircularBuffer *fb,
                                       DataPacket **pkt,
                                       int block)
{
    if (!valid_buffer(fb) || pkt == NULL) {
        return FB_ERR_INVALID;
    }

    pthread_mutex_lock(&fb->mutex);

    while (fb->count == 0 && !fb->shutdown) {
        if (!block) {
            pthread_mutex_unlock(&fb->mutex);
            return FB_ERR_INVALID;
        }
        pthread_cond_wait(&fb->not_empty, &fb->mutex);
    }

    if (fb->count == 0 && fb->shutdown) {
        pthread_mutex_unlock(&fb->mutex);
        return FB_ERR_SHUTDOWN;
    }

    *pkt = fb->slots[fb->head];
    fb->slots[fb->head] = NULL;
    fb->head = (fb->head + 1) % fb->capacity;
    fb->count--;

    pthread_cond_signal(&fb->not_full);
    pthread_mutex_unlock(&fb->mutex);

    return FB_OK;
}

FlowBufferStatus flow_buffer_enqueue(FlowCircularBuffer *fb, DataPacket **pkt)
{
    return enqueue_locked(fb, pkt, 1);
}

FlowBufferStatus flow_buffer_dequeue(FlowCircularBuffer *fb, DataPacket **pkt)
{
    return dequeue_locked(fb, pkt, 1);
}

FlowBufferStatus flow_buffer_try_enqueue(FlowCircularBuffer *fb, DataPacket **pkt)
{
    if (!valid_buffer(fb) || pkt == NULL || *pkt == NULL) {
        return FB_ERR_INVALID;
    }

    pthread_mutex_lock(&fb->mutex);

    if (fb->shutdown) {
        pthread_mutex_unlock(&fb->mutex);
        return FB_ERR_SHUTDOWN;
    }

    if (fb->count == fb->capacity) {
        pthread_mutex_unlock(&fb->mutex);
        return FB_ERR_INVALID;
    }

    fb->slots[fb->tail] = *pkt;
    fb->tail = (fb->tail + 1) % fb->capacity;
    fb->count++;
    *pkt = NULL;

    pthread_cond_signal(&fb->not_empty);
    pthread_mutex_unlock(&fb->mutex);

    return FB_OK;
}

FlowBufferStatus flow_buffer_try_dequeue(FlowCircularBuffer *fb, DataPacket **pkt)
{
    if (!valid_buffer(fb) || pkt == NULL) {
        return FB_ERR_INVALID;
    }

    pthread_mutex_lock(&fb->mutex);

    if (fb->count == 0) {
        FlowBufferStatus st = fb->shutdown ? FB_ERR_SHUTDOWN : FB_ERR_INVALID;
        pthread_mutex_unlock(&fb->mutex);
        return st;
    }

    *pkt = fb->slots[fb->head];
    fb->slots[fb->head] = NULL;
    fb->head = (fb->head + 1) % fb->capacity;
    fb->count--;

    pthread_cond_signal(&fb->not_full);
    pthread_mutex_unlock(&fb->mutex);

    return FB_OK;
}

size_t flow_buffer_count(const FlowCircularBuffer *fb)
{
    size_t count;

    if (!valid_buffer(fb)) {
        return 0;
    }

    pthread_mutex_lock((pthread_mutex_t *)&fb->mutex);
    count = fb->count;
    pthread_mutex_unlock((pthread_mutex_t *)&fb->mutex);

    return count;
}

int flow_buffer_is_empty(const FlowCircularBuffer *fb)
{
    return flow_buffer_count(fb) == 0;
}

int flow_buffer_is_full(const FlowCircularBuffer *fb)
{
    size_t count;
    size_t capacity;

    if (!valid_buffer(fb)) {
        return 0;
    }

    pthread_mutex_lock((pthread_mutex_t *)&fb->mutex);
    count = fb->count;
    capacity = fb->capacity;
    pthread_mutex_unlock((pthread_mutex_t *)&fb->mutex);

    return count == capacity;
}
