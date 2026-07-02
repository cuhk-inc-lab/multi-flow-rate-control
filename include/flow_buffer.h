#ifndef FLOW_BUFFER_H
#define FLOW_BUFFER_H

/*
 * Per-flow blocking packet ring. Stores DataPacket pointers only (no payload copy).
 * Enqueue/dequeue transfer ownership via DataPacket **.
 */

#include <stddef.h>
#include <pthread.h>

#include "packet.h"

typedef enum {
    FB_OK = 0,
    FB_ERR_INVALID = -1,
    FB_ERR_ALLOC = -2,
    FB_ERR_SHUTDOWN = -3
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
 * Blocking enqueue. On success, *pkt is set to NULL (ownership transferred).
 * Returns FB_ERR_SHUTDOWN if shutdown was requested while waiting.
 */
FlowBufferStatus flow_buffer_enqueue(FlowCircularBuffer *fb, DataPacket **pkt);

/*
 * Blocking dequeue. On success, *pkt receives an owned packet pointer.
 * Returns FB_ERR_SHUTDOWN if shutdown was requested and queue is empty.
 */
FlowBufferStatus flow_buffer_dequeue(FlowCircularBuffer *fb, DataPacket **pkt);

FlowBufferStatus flow_buffer_try_enqueue(FlowCircularBuffer *fb, DataPacket **pkt);
FlowBufferStatus flow_buffer_try_dequeue(FlowCircularBuffer *fb, DataPacket **pkt);

size_t flow_buffer_count(const FlowCircularBuffer *fb);
int    flow_buffer_is_empty(const FlowCircularBuffer *fb);
int    flow_buffer_is_full(const FlowCircularBuffer *fb);

#endif /* FLOW_BUFFER_H */
