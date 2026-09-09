#include "datagram_pool.h"

#include <stdlib.h>
#include <string.h>

int datagram_pool_init(DatagramPool *pool, size_t capacity, size_t buf_size)
{
    size_t i;

    if (pool == NULL || capacity == 0 || buf_size == 0) {
        return -1;
    }
    memset(pool, 0, sizeof(*pool));
    pool->capacity = capacity;
    pool->buf_size = buf_size;
    pool->slab = (uint8_t *)calloc(capacity, buf_size);
    pool->free_stack = (uint8_t **)calloc(capacity, sizeof(uint8_t *));
    if (pool->slab == NULL || pool->free_stack == NULL) {
        free(pool->slab);
        free(pool->free_stack);
        pool->slab = NULL;
        pool->free_stack = NULL;
        return -1;
    }
    if (pthread_mutex_init(&pool->mu, NULL) != 0) {
        free(pool->slab);
        free(pool->free_stack);
        pool->slab = NULL;
        pool->free_stack = NULL;
        return -1;
    }
    pool->mu_inited = 1;
    for (i = 0; i < capacity; i++) {
        pool->free_stack[i] = pool->slab + i * buf_size;
    }
    pool->free_count = capacity;
    return 0;
}

void datagram_pool_destroy(DatagramPool *pool)
{
    if (pool == NULL) {
        return;
    }
    if (pool->mu_inited) {
        pthread_mutex_destroy(&pool->mu);
        pool->mu_inited = 0;
    }
    free(pool->slab);
    free(pool->free_stack);
    memset(pool, 0, sizeof(*pool));
}

int datagram_pool_contains(const DatagramPool *pool, const uint8_t *ptr)
{
    size_t off;

    if (pool == NULL || pool->slab == NULL || ptr == NULL || pool->buf_size == 0) {
        return 0;
    }
    if (ptr < pool->slab) {
        return 0;
    }
    off = (size_t)(ptr - pool->slab);
    if (off >= pool->capacity * pool->buf_size) {
        return 0;
    }
    return (off % pool->buf_size) == 0;
}

uint8_t *datagram_pool_acquire(DatagramPool *pool)
{
    uint8_t *ptr = NULL;

    if (pool == NULL || !pool->mu_inited) {
        return NULL;
    }

    pthread_mutex_lock(&pool->mu);
    if (pool->free_count > 0) {
        pool->free_count--;
        ptr = pool->free_stack[pool->free_count];
        pool->in_use++;
        pool->stats.acquire_pooled++;
        if (pool->in_use > pool->stats.high_watermark_in_use) {
            pool->stats.high_watermark_in_use = pool->in_use;
        }
        pthread_mutex_unlock(&pool->mu);
        return ptr;
    }
    pthread_mutex_unlock(&pool->mu);

    ptr = (uint8_t *)malloc(pool->buf_size);
    if (ptr != NULL) {
        pthread_mutex_lock(&pool->mu);
        pool->stats.acquire_fallback++;
        pthread_mutex_unlock(&pool->mu);
    }
    return ptr;
}

void datagram_pool_release(DatagramPool *pool, uint8_t *ptr)
{
    if (ptr == NULL) {
        return;
    }
    if (pool == NULL || !pool->mu_inited || !datagram_pool_contains(pool, ptr)) {
        if (pool != NULL && pool->mu_inited) {
            pthread_mutex_lock(&pool->mu);
            pool->stats.release_foreign++;
            pthread_mutex_unlock(&pool->mu);
        }
        free(ptr);
        return;
    }

    pthread_mutex_lock(&pool->mu);
    if (pool->free_count < pool->capacity) {
        pool->free_stack[pool->free_count++] = ptr;
        if (pool->in_use > 0) {
            pool->in_use--;
        }
        pool->stats.release_pooled++;
    } else {
        /* Should not happen; treat as foreign to avoid corruption. */
        pool->stats.release_foreign++;
        pthread_mutex_unlock(&pool->mu);
        free(ptr);
        return;
    }
    pthread_mutex_unlock(&pool->mu);
}

void datagram_pool_stats_snapshot(const DatagramPool *pool,
                                  DatagramPoolStats *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (pool == NULL || !pool->mu_inited) {
        return;
    }
    pthread_mutex_lock((pthread_mutex_t *)&pool->mu);
    *out = pool->stats;
    pthread_mutex_unlock((pthread_mutex_t *)&pool->mu);
}
