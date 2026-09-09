#ifndef WIRE_RELAY_DATAGRAM_POOL_H
#define WIRE_RELAY_DATAGRAM_POOL_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Fixed-size datagram slab pool.
 *
 * acquire() returns a buffer of buf_size bytes (typically RELAY_MAX_DATAGRAM).
 * Callers pass the pointer through queues (ownership transfer) and release()
 * when done. Pointers not from this pool are freed with free() so mixed
 * legacy malloc paths remain safe during migration.
 */

#ifndef DATAGRAM_POOL_DEFAULT_CAPACITY
#define DATAGRAM_POOL_DEFAULT_CAPACITY 8192u
#endif

typedef struct DatagramPoolStats {
    uint64_t acquire_pooled;
    uint64_t acquire_fallback;
    uint64_t release_pooled;
    uint64_t release_foreign;
    uint64_t high_watermark_in_use;
} DatagramPoolStats;

typedef struct DatagramPool {
    uint8_t           *slab;       /* capacity * buf_size */
    uint8_t          **free_stack; /* pointers into slab */
    size_t             capacity;
    size_t             buf_size;
    size_t             free_count;
    size_t             in_use;
    pthread_mutex_t    mu;
    int                mu_inited;
    DatagramPoolStats  stats;
} DatagramPool;

int datagram_pool_init(DatagramPool *pool, size_t capacity, size_t buf_size);
void datagram_pool_destroy(DatagramPool *pool);

/* Returns NULL only if both slab and fallback malloc fail. */
uint8_t *datagram_pool_acquire(DatagramPool *pool);

/*
 * Return a buffer. Safe with NULL. Foreign (non-slab) pointers are free()'d.
 */
void datagram_pool_release(DatagramPool *pool, uint8_t *ptr);

int datagram_pool_contains(const DatagramPool *pool, const uint8_t *ptr);

void datagram_pool_stats_snapshot(const DatagramPool *pool,
                                  DatagramPoolStats *out);

#endif /* WIRE_RELAY_DATAGRAM_POOL_H */
