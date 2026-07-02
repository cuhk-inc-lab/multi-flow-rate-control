#include "dispatcher.h"

void *dispatcher_thread(void *arg)
{
    FlowManager *mgr = arg;

    if (mgr == NULL) {
        return NULL;
    }

    while (flow_manager_is_running(mgr)) {
        DataPacket *pkt = NULL;
        MixedQueueStatus mq_st;
        FlowBufferStatus fb_st;
        uint32_t flow_id;
        FlowContext *flow;

        mq_st = mixed_queue_pop(&mgr->mixed, &pkt);
        if (mq_st == MQ_ERR_SHUTDOWN) {
            break;
        }
        if (mq_st != MQ_OK || pkt == NULL) {
            continue;
        }

        flow_id = pkt->flow_id;
        if (flow_id >= mgr->config.max_flows) {
            mgr->route_errors++;
            packet_free(pkt);
            continue;
        }

        flow = &mgr->flows[flow_id];
        fb_st = flow_buffer_enqueue(&flow->queue, &pkt);
        if (fb_st == FB_ERR_SHUTDOWN) {
            packet_free(pkt);
            break;
        }
        if (fb_st != FB_OK) {
            packet_free(pkt);
            continue;
        }

        flow->metrics.enqueued++;
    }

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
