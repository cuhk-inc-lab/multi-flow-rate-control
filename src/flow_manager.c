#include "flow_manager.h"

#include "dispatcher.h"
#include "flow_worker.h"

#include <stdlib.h>
#include <string.h>

static int valid_config(const FlowManagerConfig *cfg)
{
    return cfg != NULL &&
           cfg->max_flows > 0 &&
           cfg->per_flow_queue_capacity > 0 &&
           cfg->mixed_queue_capacity > 0;
}

int flow_manager_is_running(const FlowManager *mgr)
{
    return mgr != NULL && mgr->running && !mgr->shutdown_requested;
}

FlowManagerStatus flow_manager_init(FlowManager *mgr, const FlowManagerConfig *cfg)
{
    uint32_t i;

    if (mgr == NULL || !valid_config(cfg)) {
        return FM_ERR_INVALID;
    }

    memset(mgr, 0, sizeof(*mgr));
    mgr->config = *cfg;

    if (mixed_queue_init(&mgr->mixed, cfg->mixed_queue_capacity) != MQ_OK) {
        return FM_ERR_ALLOC;
    }

    if (pthread_mutex_init(&mgr->dispatch_wake_mtx, NULL) != 0) {
        mixed_queue_destroy(&mgr->mixed);
        return FM_ERR_ALLOC;
    }

    if (pthread_cond_init(&mgr->dispatch_wake_cv, NULL) != 0) {
        pthread_mutex_destroy(&mgr->dispatch_wake_mtx);
        mixed_queue_destroy(&mgr->mixed);
        return FM_ERR_ALLOC;
    }

    mgr->flows = calloc(cfg->max_flows, sizeof(*mgr->flows));
    if (mgr->flows == NULL) {
        pthread_cond_destroy(&mgr->dispatch_wake_cv);
        pthread_mutex_destroy(&mgr->dispatch_wake_mtx);
        mixed_queue_destroy(&mgr->mixed);
        return FM_ERR_ALLOC;
    }

    for (i = 0; i < cfg->max_flows; i++) {
        int output_fd = cfg->default_output_fd;

        if (cfg->output_fds != NULL) {
            output_fd = cfg->output_fds[i];
        }

        if (flow_context_init(&mgr->flows[i],
                              i,
                              cfg->per_flow_queue_capacity,
                              output_fd,
                              mgr) != FC_OK) {
            while (i > 0) {
                i--;
                flow_context_destroy(&mgr->flows[i]);
            }
            free(mgr->flows);
            mgr->flows = NULL;
            pthread_cond_destroy(&mgr->dispatch_wake_cv);
            pthread_mutex_destroy(&mgr->dispatch_wake_mtx);
            mixed_queue_destroy(&mgr->mixed);
            return FM_ERR_ALLOC;
        }

        if (cfg->encode_scratch_cap > 0) {
            flow_context_set_encoder(&mgr->flows[i], NULL, NULL, cfg->encode_scratch_cap);
        }
    }

    if (!dispatcher_init_deferred(mgr)) {
        for (i = 0; i < cfg->max_flows; i++) {
            flow_context_destroy(&mgr->flows[i]);
        }
        free(mgr->flows);
        mgr->flows = NULL;
        pthread_cond_destroy(&mgr->dispatch_wake_cv);
        pthread_mutex_destroy(&mgr->dispatch_wake_mtx);
        mixed_queue_destroy(&mgr->mixed);
        return FM_ERR_ALLOC;
    }

    return FM_OK;
}

FlowManagerStatus flow_manager_start(FlowManager *mgr)
{
    uint32_t i;

    if (mgr == NULL || mgr->flows == NULL) {
        return FM_ERR_INVALID;
    }

    if (mgr->running) {
        return FM_ERR_STATE;
    }

    mgr->shutdown_requested = 0;
    mgr->running = 1;

    for (i = 0; i < mgr->config.max_flows; i++) {
        if (flow_worker_start(&mgr->flows[i]) != FW_OK) {
            mgr->running = 0;
            while (i > 0) {
                i--;
                flow_buffer_shutdown(&mgr->flows[i].queue);
                flow_worker_join(&mgr->flows[i]);
            }
            return FM_ERR_ALLOC;
        }
    }

    if (dispatcher_start(mgr) != DP_OK) {
        mgr->running = 0;
        for (i = 0; i < mgr->config.max_flows; i++) {
            flow_buffer_shutdown(&mgr->flows[i].queue);
            flow_worker_join(&mgr->flows[i]);
        }
        return FM_ERR_ALLOC;
    }

    return FM_OK;
}

FlowManagerStatus flow_manager_stop(FlowManager *mgr)
{
    uint32_t i;

    if (mgr == NULL || mgr->flows == NULL) {
        return FM_ERR_INVALID;
    }

    if (!mgr->running && !mgr->dispatcher_started) {
        return FM_OK;
    }

    mgr->shutdown_requested = 1;
    mgr->running = 0;

    mixed_queue_shutdown(&mgr->mixed);

    for (i = 0; i < mgr->config.max_flows; i++) {
        flow_buffer_shutdown(&mgr->flows[i].queue);
    }

    flow_manager_dispatch_wake(mgr);

    if (mgr->dispatcher_started) {
        dispatcher_join(mgr);
    }

    for (i = 0; i < mgr->config.max_flows; i++) {
        if (mgr->flows[i].worker_started) {
            flow_worker_join(&mgr->flows[i]);
        }
    }

    return FM_OK;
}

void flow_manager_destroy(FlowManager *mgr)
{
    uint32_t i;

    if (mgr == NULL) {
        return;
    }

    flow_manager_stop(mgr);

    dispatcher_destroy_deferred(mgr);

    if (mgr->flows != NULL) {
        for (i = 0; i < mgr->config.max_flows; i++) {
            flow_context_destroy(&mgr->flows[i]);
        }
        free(mgr->flows);
        mgr->flows = NULL;
    }

    mixed_queue_destroy(&mgr->mixed);
    pthread_cond_destroy(&mgr->dispatch_wake_cv);
    pthread_mutex_destroy(&mgr->dispatch_wake_mtx);
    mgr->dispatcher_started = 0;
    mgr->running = 0;
    mgr->shutdown_requested = 0;
    mgr->route_errors = 0;
}

void flow_manager_dispatch_wake(FlowManager *mgr)
{
    if (mgr == NULL) {
        return;
    }

    pthread_mutex_lock(&mgr->dispatch_wake_mtx);
    pthread_cond_broadcast(&mgr->dispatch_wake_cv);
    pthread_mutex_unlock(&mgr->dispatch_wake_mtx);
}

FlowManagerStatus flow_manager_push(FlowManager *mgr, DataPacket **pkt)
{
    MixedQueueStatus st;

    if (mgr == NULL || pkt == NULL || *pkt == NULL) {
        return FM_ERR_INVALID;
    }

    if (!mgr->running || mgr->shutdown_requested) {
        return FM_ERR_SHUTDOWN;
    }

    st = mixed_queue_push(&mgr->mixed, pkt);
    if (st == MQ_ERR_SHUTDOWN) {
        return FM_ERR_SHUTDOWN;
    }
    if (st != MQ_OK) {
        return FM_ERR_STATE;
    }

    return FM_OK;
}
