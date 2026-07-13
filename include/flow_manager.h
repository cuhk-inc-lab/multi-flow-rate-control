#ifndef FLOW_MANAGER_H
#define FLOW_MANAGER_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

#include "flow_context.h"
#include "mixed_queue.h"

typedef enum {
    FM_OK = 0,
    FM_ERR_INVALID = -1,
    FM_ERR_ALLOC = -2,
    FM_ERR_STATE = -3,
    FM_ERR_SHUTDOWN = -4
} FlowManagerStatus;

typedef struct DeferredQueue {
    DataPacket    **slots;
    size_t          capacity;
    size_t          head;
    size_t          tail;
    _Atomic size_t  count;
} DeferredQueue;

typedef struct FlowManagerConfig {
    uint32_t max_flows;
    size_t   per_flow_queue_capacity;
    size_t   mixed_queue_capacity;
    int      default_output_fd;
    const int *output_fds;
    size_t   encode_scratch_cap;
} FlowManagerConfig;

typedef struct FlowManager {
    FlowManagerConfig config;
    FlowContext      *flows;
    DeferredQueue    *deferred;
    MixedQueue        mixed;
    pthread_t         dispatcher_thread;
    pthread_mutex_t   dispatch_wake_mtx;
    pthread_cond_t    dispatch_wake_cv;
    int               running;
    int               shutdown_requested;
    int               dispatcher_started;
    uint64_t          route_errors;
} FlowManager;

FlowManagerStatus flow_manager_init(FlowManager *mgr, const FlowManagerConfig *cfg);
FlowManagerStatus flow_manager_start(FlowManager *mgr);
FlowManagerStatus flow_manager_stop(FlowManager *mgr);
void              flow_manager_destroy(FlowManager *mgr);

FlowManagerStatus flow_manager_push(FlowManager *mgr, DataPacket **pkt);

int flow_manager_is_running(const FlowManager *mgr);

size_t flow_manager_deferred_count(const FlowManager *mgr, uint32_t flow_id);

void flow_manager_dispatch_wake(FlowManager *mgr);

#endif /* FLOW_MANAGER_H */
