#include "dispatcher.h"

#include <stdlib.h>

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
    dq->count = 0;
    return 1;
}

static void deferred_destroy(DeferredQueue *dq)
{
    if (dq == NULL) {
        return;
    }

    while (dq->count > 0) {
        DataPacket *pkt = dq->slots[dq->head];

        dq->head = (dq->head + 1) % dq->capacity;
        dq->count--;
        packet_free(pkt);
    }

    free(dq->slots);
    dq->slots = NULL;
    dq->capacity = 0;
    dq->head = 0;
    dq->tail = 0;
    dq->count = 0;
}

static int deferred_push(DeferredQueue *dq, DataPacket *pkt)
{
    if (dq == NULL || pkt == NULL || dq->count >= dq->capacity) {
        return 0;
    }

    dq->slots[dq->tail] = pkt;
    dq->tail = (dq->tail + 1) % dq->capacity;
    dq->count++;
    return 1;
}

static DataPacket *deferred_pop(DeferredQueue *dq)
{
    DataPacket *pkt;

    if (dq == NULL || dq->count == 0) {
        return NULL;
    }

    pkt = dq->slots[dq->head];
    dq->slots[dq->head] = NULL;
    dq->head = (dq->head + 1) % dq->capacity;
    dq->count--;
    return pkt;
}

static int deferred_total(const FlowManager *mgr)
{
    size_t total = 0;
    uint32_t i;

    if (mgr == NULL || mgr->deferred == NULL) {
        return 0;
    }

    for (i = 0; i < mgr->config.max_flows; i++) {
        total += mgr->deferred[i].count;
    }

    return (int)total;
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
    fb_st = flow_buffer_try_enqueue(&flow->queue, &pkt);
    if (fb_st == FB_OK) {
        flow_metrics_record_enqueue(&flow->metrics, bytes);
        return 1;
    }

    if (fb_st == FB_ERR_SHUTDOWN) {
        packet_free(pkt);
        return 0;
    }

    if (!deferred_push(&mgr->deferred[flow_id], pkt)) {
        mgr->route_errors++;
        packet_free(pkt);
    }

    return 1;
}

static int drain_deferred(FlowManager *mgr)
{
    uint32_t i;
    int progress = 0;

    for (i = 0; i < mgr->config.max_flows; i++) {
        DeferredQueue *dq = &mgr->deferred[i];
        FlowContext *flow = &mgr->flows[i];

        while (dq->count > 0) {
            DataPacket *pkt = dq->slots[dq->head];
            FlowBufferStatus fb_st;
            size_t bytes = pkt->payload_len;

            fb_st = flow_buffer_try_enqueue(&flow->queue, &pkt);
            if (fb_st != FB_OK) {
                break;
            }

            deferred_pop(dq);
            flow_metrics_record_enqueue(&flow->metrics, bytes);
            progress = 1;
        }
    }

    return progress;
}

void *dispatcher_thread(void *arg)
{
    FlowManager *mgr = arg;

    if (mgr == NULL) {
        return NULL;
    }

    while (flow_manager_is_running(mgr)) {
        DataPacket *pkt = NULL;
        MixedQueueStatus mq_st;
        int progress = 0;

        progress |= drain_deferred(mgr);

        mq_st = mixed_queue_try_pop(&mgr->mixed, &pkt);
        if (mq_st == MQ_ERR_SHUTDOWN) {
            break;
        }

        if (mq_st == MQ_OK && pkt != NULL) {
            route_packet(mgr, pkt);
            progress = 1;
            continue;
        }

        if (progress) {
            continue;
        }

        if (!flow_manager_is_running(mgr)) {
            break;
        }

        if (deferred_total(mgr) > 0) {
            pthread_mutex_lock(&mgr->dispatch_wake_mtx);
            pthread_cond_wait(&mgr->dispatch_wake_cv, &mgr->dispatch_wake_mtx);
            pthread_mutex_unlock(&mgr->dispatch_wake_mtx);
            continue;
        }

        pkt = NULL;
        mq_st = mixed_queue_pop(&mgr->mixed, &pkt);
        if (mq_st == MQ_ERR_SHUTDOWN) {
            break;
        }
        if (mq_st == MQ_OK && pkt != NULL) {
            route_packet(mgr, pkt);
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
        if (!deferred_init(&mgr->deferred[i], mgr->config.per_flow_queue_capacity)) {
            while (i > 0) {
                i--;
                deferred_destroy(&mgr->deferred[i]);
            }
            free(mgr->deferred);
            mgr->deferred = NULL;
            return 0;
        }
    }

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
}
