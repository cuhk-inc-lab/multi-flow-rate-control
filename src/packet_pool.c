#include "packet_pool.h"

#include "time_utils.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

struct PacketPool {
    size_t          capacity;
    size_t          payload_cap;
    DataPacket     *packets;
    unsigned char  *payload_slab;
    DataPacket    **free_list;
    size_t          free_count;
    pthread_mutex_t mutex;
};

PacketPoolStatus packet_pool_init(PacketPool **out,
                                  size_t capacity,
                                  size_t payload_cap)
{
    PacketPool *pool;
    size_t i;

    if (out == NULL || capacity == 0 || payload_cap == 0) {
        return PP_ERR_INVALID;
    }

    pool = calloc(1, sizeof(*pool));
    if (pool == NULL) {
        return PP_ERR_ALLOC;
    }

    pool->capacity = capacity;
    pool->payload_cap = payload_cap;
    pool->packets = calloc(capacity, sizeof(*pool->packets));
    pool->payload_slab = calloc(capacity, payload_cap);
    pool->free_list = calloc(capacity, sizeof(*pool->free_list));
    if (pool->packets == NULL || pool->payload_slab == NULL ||
        pool->free_list == NULL) {
        free(pool->packets);
        free(pool->payload_slab);
        free(pool->free_list);
        free(pool);
        return PP_ERR_ALLOC;
    }

    if (pthread_mutex_init(&pool->mutex, NULL) != 0) {
        free(pool->packets);
        free(pool->payload_slab);
        free(pool->free_list);
        free(pool);
        return PP_ERR_ALLOC;
    }

    for (i = 0; i < capacity; i++) {
        DataPacket *pkt = &pool->packets[i];

        pkt->payload = pool->payload_slab + i * payload_cap;
        pkt->pool = pool;
        pool->free_list[i] = pkt;
    }
    pool->free_count = capacity;
    *out = pool;
    return PP_OK;
}

void packet_pool_destroy(PacketPool *pool)
{
    if (pool == NULL) {
        return;
    }

    pthread_mutex_destroy(&pool->mutex);
    free(pool->free_list);
    free(pool->payload_slab);
    free(pool->packets);
    free(pool);
}

size_t packet_pool_capacity(const PacketPool *pool)
{
    return pool != NULL ? pool->capacity : 0;
}

size_t packet_pool_payload_cap(const PacketPool *pool)
{
    return pool != NULL ? pool->payload_cap : 0;
}

size_t packet_pool_available(const PacketPool *pool)
{
    size_t n;

    if (pool == NULL) {
        return 0;
    }
    pthread_mutex_lock((pthread_mutex_t *)&pool->mutex);
    n = pool->free_count;
    pthread_mutex_unlock((pthread_mutex_t *)&pool->mutex);
    return n;
}

DataPacket *packet_pool_alloc(PacketPool *pool, uint32_t flow_id)
{
    DataPacket *pkt;

    if (pool == NULL) {
        return NULL;
    }

    pthread_mutex_lock(&pool->mutex);
    if (pool->free_count == 0) {
        pthread_mutex_unlock(&pool->mutex);
        return NULL;
    }
    pool->free_count--;
    pkt = pool->free_list[pool->free_count];
    pool->free_list[pool->free_count] = NULL;
    pthread_mutex_unlock(&pool->mutex);

    pkt->flow_id = flow_id;
    pkt->payload_len = 0;
    pkt->user_data = NULL;
    pkt->pool = pool;
    if (time_utils_now_mono(&pkt->enqueue_ts) != TU_OK) {
        packet_pool_release(pool, pkt);
        return NULL;
    }
    return pkt;
}

void packet_pool_release(PacketPool *pool, DataPacket *pkt)
{
    if (pool == NULL || pkt == NULL || pkt->pool != pool) {
        return;
    }

    pkt->flow_id = 0;
    pkt->payload_len = 0;
    pkt->user_data = NULL;
    memset(&pkt->enqueue_ts, 0, sizeof(pkt->enqueue_ts));

    pthread_mutex_lock(&pool->mutex);
    if (pool->free_count < pool->capacity) {
        pool->free_list[pool->free_count++] = pkt;
    }
    pthread_mutex_unlock(&pool->mutex);
}

int packet_is_pooled(const DataPacket *pkt)
{
    return pkt != NULL && pkt->pool != NULL;
}
