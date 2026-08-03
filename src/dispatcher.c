#include "dispatcher.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#ifndef FM_DISPATCH_BATCH
#define FM_DISPATCH_BATCH 32u
#endif

#ifndef FM_DEFERRED_QUOTA
#define FM_DEFERRED_QUOTA 8u
#endif

static void deferred_mark_active(FlowManager *mgr, uint32_t flow_id)
{
    if (flow_id < 64u) {
        atomic_fetch_or_explicit(&mgr->deferred_active_bits,
                                 (uint64_t)1u << flow_id,
                                 memory_order_relaxed);
    }
}

static void deferred_clear_active(FlowManager *mgr, uint32_t flow_id)
{
    if (flow_id < 64u) {
        atomic_fetch_and_explicit(&mgr->deferred_active_bits,
                                  ~((uint64_t)1u << flow_id),
                                  memory_order_relaxed);
    }
}

static int deferred_init(DeferredQueue *dq, size_t capacity)
{
    if (dq == NULL || capacity == 0) {
        return 0;
    }

    dq->slots = calloc(capacity, sizeof(*dq->slots));
    if (dq->slots == NULL) {
        return 0;
    }

    dq->capacity = capacity;
    dq->head = 0;
    dq->tail = 0;
    atomic_store(&dq->count, 0);
    return 1;
}

static void deferred_destroy(DeferredQueue *dq)
{
    if (dq == NULL) {
        return;
    }

    while (atomic_load(&dq->count) > 0) {
        DataPacket *pkt = dq->slots[dq->head];

        dq->head = (dq->head + 1) % dq->capacity;
        atomic_fetch_sub(&dq->count, 1);
        packet_free(pkt);
    }

    free(dq->slots);
    dq->slots = NULL;
    dq->capacity = 0;
    dq->head = 0;
    dq->tail = 0;
    atomic_store(&dq->count, 0);
}

static int deferred_push(FlowManager *mgr, uint32_t flow_id, DataPacket *pkt)
{
    DeferredQueue *dq;

    if (mgr == NULL || pkt == NULL || flow_id >= mgr->config.max_flows) {
        return 0;
    }

    dq = &mgr->deferred[flow_id];
    if (atomic_load(&dq->count) >= dq->capacity) {
        return 0;
    }

    dq->slots[dq->tail] = pkt;
    dq->tail = (dq->tail + 1) % dq->capacity;
    if (atomic_fetch_add(&dq->count, 1) == 0) {
        deferred_mark_active(mgr, flow_id);
    }
    atomic_fetch_add_explicit(&mgr->deferred_total_count, 1, memory_order_relaxed);
    return 1;
}

static DataPacket *deferred_pop(FlowManager *mgr, uint32_t flow_id)
{
    DeferredQueue *dq;
    DataPacket *pkt;
    size_t prev;

    if (mgr == NULL || flow_id >= mgr->config.max_flows) {
        return NULL;
    }

    dq = &mgr->deferred[flow_id];
    if (atomic_load(&dq->count) == 0) {
        return NULL;
    }

    pkt = dq->slots[dq->head];
    dq->slots[dq->head] = NULL;
    dq->head = (dq->head + 1) % dq->capacity;
    prev = atomic_fetch_sub(&dq->count, 1);
    atomic_fetch_sub_explicit(&mgr->deferred_total_count, 1, memory_order_relaxed);
    if (prev == 1) {
        deferred_clear_active(mgr, flow_id);
    }
    return pkt;
}

static void record_deferred_overflow(FlowManager *mgr, uint32_t flow_id)
{
    mgr->route_errors++;
    atomic_fetch_add_explicit(&mgr->drop_deferred_overflow, 1, memory_order_relaxed);
    if (flow_id < mgr->config.max_flows) {
        atomic_fetch_add_explicit(&mgr->flows[flow_id].drop_deferred_overflow,
                                  1, memory_order_relaxed);
    }
}

static int route_packet(FlowManager *mgr, DataPacket *pkt)
{
    uint32_t flow_id = pkt->flow_id;
    FlowContext *flow;
    FlowBufferStatus fb_st;
    size_t bytes = pkt->payload_len;

    if (flow_id >= mgr->config.max_flows) {
        mgr->route_errors++;
        packet_free(pkt);
        return 1;
    }

    flow = &mgr->flows[flow_id];

    /*
     * If this flow already has deferred packets, new ones must append there.
     * Otherwise a slot freed by the worker can admit a later mixed-queue
     * packet ahead of an earlier deferred one (reorder → corrupt payload).
     */
    if (atomic_load(&mgr->deferred[flow_id].count) > 0) {
        if (!deferred_push(mgr, flow_id, pkt)) {
            record_deferred_overflow(mgr, flow_id);
            packet_free(pkt);
        }
        return 1;
    }

    fb_st = flow_buffer_try_enqueue(&flow->queue, &pkt);
    if (fb_st == FB_OK) {
        flow_metrics_record_enqueue(&flow->metrics, bytes);
        return 1;
    }

    if (fb_st == FB_ERR_SHUTDOWN) {
        packet_free(pkt);
        return 0;
    }

    if (!deferred_push(mgr, flow_id, pkt)) {
        record_deferred_overflow(mgr, flow_id);
        packet_free(pkt);
    }

    return 1;
}

static int drain_deferred(FlowManager *mgr)
{
    uint32_t i;
    uint32_t examined = 0;
    int progress = 0;
    uint32_t start;
    uint64_t active;

    if (mgr == NULL || mgr->deferred == NULL || mgr->config.max_flows == 0) {
        return 0;
    }

    active = atomic_load_explicit(&mgr->deferred_active_bits, memory_order_relaxed);
    if (active == 0 && flow_manager_deferred_total(mgr) == 0) {
        return 0;
    }

    start = mgr->deferred_rr_cursor % mgr->config.max_flows;

    for (examined = 0; examined < mgr->config.max_flows; examined++) {
        unsigned quota = FM_DEFERRED_QUOTA;

        i = (start + examined) % mgr->config.max_flows;
        if (i < 64u && (active & ((uint64_t)1u << i)) == 0 &&
            atomic_load(&mgr->deferred[i].count) == 0) {
            continue;
        }

        while (quota-- > 0 && atomic_load(&mgr->deferred[i].count) > 0) {
            DataPacket *pkt = mgr->deferred[i].slots[mgr->deferred[i].head];
            FlowBufferStatus fb_st;
            size_t bytes = pkt->payload_len;

            fb_st = flow_buffer_try_enqueue(&mgr->flows[i].queue, &pkt);
            if (fb_st != FB_OK) {
                break;
            }

            deferred_pop(mgr, i);
            flow_metrics_record_enqueue(&mgr->flows[i].metrics, bytes);
            progress = 1;
        }
    }

    mgr->deferred_rr_cursor = (start + 1u) % mgr->config.max_flows;
    return progress;
}

void *dispatcher_thread(void *arg)
{
    FlowManager *mgr = arg;
    DataPacket *batch[FM_DISPATCH_BATCH];

    if (mgr == NULL) {
        return NULL;
    }

    while (flow_manager_is_running(mgr)) {
        MixedQueueStatus mq_st;
        int progress = 0;
        uint64_t wake_seen;
        size_t n;
        size_t bi;
        int saw_shutdown = 0;

        progress |= drain_deferred(mgr);

        n = mixed_queue_try_pop_batch(&mgr->mixed, batch, FM_DISPATCH_BATCH,
                                      &saw_shutdown);
        if (saw_shutdown && n == 0) {
            break;
        }

        if (n > 0) {
            for (bi = 0; bi < n; bi++) {
                if (batch[bi] != NULL) {
                    route_packet(mgr, batch[bi]);
                }
            }
            progress = 1;
            continue;
        }

        if (progress) {
            continue;
        }

        if (!flow_manager_is_running(mgr)) {
            break;
        }

        /*
         * Deferred packets wait on per-flow queue space. Snapshot the wake
         * generation, retry drain (in case a worker already freed a slot and
         * signaled), then wait only if no newer wake arrived.
         */
        if (flow_manager_deferred_total(mgr) > 0) {
            pthread_mutex_lock(&mgr->dispatch_wake_mtx);
            wake_seen = mgr->dispatch_wake_gen;
            pthread_mutex_unlock(&mgr->dispatch_wake_mtx);

            if (drain_deferred(mgr)) {
                continue;
            }

            pthread_mutex_lock(&mgr->dispatch_wake_mtx);
            while (flow_manager_is_running(mgr) &&
                   flow_manager_deferred_total(mgr) > 0 &&
                   mgr->dispatch_wake_gen == wake_seen) {
                pthread_cond_wait(&mgr->dispatch_wake_cv,
                                  &mgr->dispatch_wake_mtx);
            }
            pthread_mutex_unlock(&mgr->dispatch_wake_mtx);
            continue;
        }

        {
            DataPacket *pkt = NULL;

            mq_st = mixed_queue_pop(&mgr->mixed, &pkt);
            if (mq_st == MQ_ERR_SHUTDOWN) {
                break;
            }
            if (mq_st == MQ_OK && pkt != NULL) {
                route_packet(mgr, pkt);
            }
        }
    }

    drain_deferred(mgr);
    return NULL;
}

DispatcherStatus dispatcher_start(FlowManager *mgr)
{
    if (mgr == NULL || mgr->dispatcher_started) {
        return DP_ERR_INVALID;
    }

    if (pthread_create(&mgr->dispatcher_thread, NULL, dispatcher_thread, mgr) != 0) {
        return DP_ERR_SYSTEM;
    }

    mgr->dispatcher_started = 1;
    return DP_OK;
}

DispatcherStatus dispatcher_join(FlowManager *mgr)
{
    if (mgr == NULL || !mgr->dispatcher_started) {
        return DP_ERR_INVALID;
    }

    if (pthread_join(mgr->dispatcher_thread, NULL) != 0) {
        return DP_ERR_SYSTEM;
    }

    mgr->dispatcher_started = 0;
    return DP_OK;
}

int dispatcher_init_deferred(FlowManager *mgr)
{
    uint32_t i;

    if (mgr == NULL || mgr->config.max_flows == 0) {
        return 0;
    }

    mgr->deferred = calloc(mgr->config.max_flows, sizeof(*mgr->deferred));
    if (mgr->deferred == NULL) {
        return 0;
    }

    for (i = 0; i < mgr->config.max_flows; i++) {
        if (!deferred_init(&mgr->deferred[i], mgr->config.mixed_queue_capacity)) {
            while (i > 0) {
                i--;
                deferred_destroy(&mgr->deferred[i]);
            }
            free(mgr->deferred);
            mgr->deferred = NULL;
            return 0;
        }
    }

    atomic_store(&mgr->deferred_active_bits, 0);
    atomic_store(&mgr->deferred_total_count, 0);
    mgr->deferred_rr_cursor = 0;
    return 1;
}

void dispatcher_destroy_deferred(FlowManager *mgr)
{
    uint32_t i;

    if (mgr == NULL || mgr->deferred == NULL) {
        return;
    }

    for (i = 0; i < mgr->config.max_flows; i++) {
        deferred_destroy(&mgr->deferred[i]);
    }

    free(mgr->deferred);
    mgr->deferred = NULL;
    atomic_store(&mgr->deferred_active_bits, 0);
    atomic_store(&mgr->deferred_total_count, 0);
}
