#ifndef FLOW_BUFFER_H
#define FLOW_BUFFER_H

/*
 * Per-flow blocking packet ring.
 *
 * Stores DataPacket pointers in a fixed-capacity circular queue.
 * Enqueue blocks when full; dequeue blocks when empty.
 * pthread_cond_wait is used inside while loops in the implementation.
 */

#include <stddef.h>
#include <pthread.h>

#include "packet.h"

typedef enum {
    FB_OK = 0,
    FB_ERR_INVALID = -1,
    FB_ERR_ALLOC = -2,
    FB_ERR_FULL = -3,
    FB_ERR_EMPTY = -4,
    FB_ERR_SHUTDOWN = -5
} FlowBufferStatus;

typedef struct FlowCircularBuffer {
    DataPacket    **slots;
    size_t          capacity;
    size_t          count;
    size_t          head;
    size_t          tail;

    pthread_mutex_t mutex;
    pthread_cond_t  not_full;
    pthread_cond_t  not_empty;

    int             shutdown;
} FlowCircularBuffer;

FlowBufferStatus flow_buffer_init(FlowCircularBuffer *fb, size_t capacity);
void             flow_buffer_destroy(FlowCircularBuffer *fb);
void             flow_buffer_shutdown(FlowCircularBuffer *fb);

/*
 * Blocking enqueue. On success, ownership moves into the queue and *pkt is NULL.
 * Returns FB_ERR_SHUTDOWN if shutdown is set while waiting.
 */
FlowBufferStatus flow_buffer_enqueue(FlowCircularBuffer *fb, DataPacket **pkt);

/*
 * Blocking dequeue. On success, *pkt receives an owned packet.
 * If woke_from_idle is non-NULL, it is set to 1 when this dequeue followed
 * an empty-queue wait (used to reset rate pacing after idle).
 * Returns FB_ERR_SHUTDOWN if shutdown is set and the queue is empty.
 */
FlowBufferStatus flow_buffer_dequeue(FlowCircularBuffer *fb,
                                     DataPacket **pkt,
                                     int *woke_from_idle);

/*
 * Non-blocking enqueue.
 * Returns FB_ERR_FULL if the queue has no free slot.
 */
FlowBufferStatus flow_buffer_try_enqueue(FlowCircularBuffer *fb, DataPacket **pkt);

/*
 * Non-blocking dequeue.
 * Returns FB_ERR_EMPTY if the queue has no packet.
 */
FlowBufferStatus flow_buffer_try_dequeue(FlowCircularBuffer *fb, DataPacket **pkt);

size_t flow_buffer_count(const FlowCircularBuffer *fb);
int    flow_buffer_is_empty(const FlowCircularBuffer *fb);
int    flow_buffer_is_full(const FlowCircularBuffer *fb);

#endif /* FLOW_BUFFER_H */
