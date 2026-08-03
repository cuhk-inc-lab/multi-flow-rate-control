#ifndef MIXED_QUEUE_H
#define MIXED_QUEUE_H

/*
 * Upstream mixed-flow packet queue.
 * Same semantics as FlowCircularBuffer but without per-flow routing.
 */

#include <stddef.h>
#include <stdatomic.h>
#include <pthread.h>

#include "packet.h"

typedef enum {
    MQ_OK = 0,
    MQ_ERR_INVALID = -1,
    MQ_ERR_ALLOC = -2,
    MQ_ERR_FULL = -3,
    MQ_ERR_EMPTY = -4,
    MQ_ERR_SHUTDOWN = -5
} MixedQueueStatus;

typedef struct MixedQueue {
    DataPacket    **slots;
    size_t          capacity;
    size_t          count;
    size_t          head;
    size_t          tail;

    pthread_mutex_t mutex;
    pthread_cond_t  not_full;
    pthread_cond_t  not_empty;

    int             shutdown;
    _Atomic size_t *occupancy; /* optional mirror */
} MixedQueue;

MixedQueueStatus mixed_queue_init(MixedQueue *mq, size_t capacity);
void             mixed_queue_destroy(MixedQueue *mq);
void             mixed_queue_shutdown(MixedQueue *mq);

MixedQueueStatus mixed_queue_push(MixedQueue *mq, DataPacket **pkt);
MixedQueueStatus mixed_queue_pop(MixedQueue *mq, DataPacket **pkt);
MixedQueueStatus mixed_queue_try_push(MixedQueue *mq, DataPacket **pkt);
MixedQueueStatus mixed_queue_try_pop(MixedQueue *mq, DataPacket **pkt);

/*
 * Pop up to max_n packets under one lock. Returns number popped (0..max_n).
 * On shutdown with empty queue returns 0 and *saw_shutdown=1 if provided.
 */
size_t mixed_queue_try_pop_batch(MixedQueue *mq,
                                 DataPacket **out,
                                 size_t max_n,
                                 int *saw_shutdown);

size_t mixed_queue_count(const MixedQueue *mq);
/* Lock-free snapshot when occupancy is maintained externally (optional). */
size_t mixed_queue_count_unlocked_hint(const MixedQueue *mq);

int    mixed_queue_is_empty(const MixedQueue *mq);
int    mixed_queue_is_full(const MixedQueue *mq);

/* Optional atomic occupancy mirror updated on push/pop (may be NULL). */
void mixed_queue_set_occupancy_counter(MixedQueue *mq, _Atomic size_t *counter);

#endif /* MIXED_QUEUE_H */
