#ifndef FLOW_MANAGER_H
#define FLOW_MANAGER_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

#include "flow_context.h"
#include "mixed_queue.h"
#include "packet_pool.h"

#ifndef FM_DISPATCH_BATCH
#define FM_DISPATCH_BATCH 32u
#endif

#ifndef FM_DEFERRED_QUOTA
#define FM_DEFERRED_QUOTA 8u
#endif

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
    /* 0 => derive: mixed + per_flow * max_flows (capped). */
    size_t   packet_pool_capacity;
    /* 0 => no pool. Typical: PKG_SIZE from the app. */
    size_t   packet_pool_payload_cap;
} FlowManagerConfig;

typedef struct FlowManager {
    FlowManagerConfig config;
    FlowContext      *flows;
    DeferredQueue    *deferred;
    MixedQueue        mixed;
    PacketPool       *packet_pool;
    pthread_t         dispatcher_thread;
    pthread_mutex_t   dispatch_wake_mtx;
    pthread_cond_t    dispatch_wake_cv;
    uint64_t          dispatch_wake_gen;
    int               running;
    int               shutdown_requested;
    int               dispatcher_started;
    uint64_t          route_errors;
    /* Deferred overflow drops (visible accounting). */
    _Atomic uint64_t  drop_deferred_overflow;
    /* Atomic occupancy for lock-light ingress gating. */
    _Atomic size_t    mixed_occupancy;
    /* Active deferred flows (bit i => deferred[i] non-empty). */
    _Atomic uint64_t  deferred_active_bits;
    _Atomic size_t    deferred_total_count;
    uint32_t          deferred_rr_cursor;
} FlowManager;

FlowManagerStatus flow_manager_init(FlowManager *mgr, const FlowManagerConfig *cfg);
FlowManagerStatus flow_manager_start(FlowManager *mgr);
FlowManagerStatus flow_manager_stop(FlowManager *mgr);
void              flow_manager_destroy(FlowManager *mgr);

FlowManagerStatus flow_manager_push(FlowManager *mgr, DataPacket **pkt);

int flow_manager_is_running(const FlowManager *mgr);

size_t flow_manager_deferred_count(const FlowManager *mgr, uint32_t flow_id);
size_t flow_manager_deferred_total(const FlowManager *mgr);

PacketPool *flow_manager_packet_pool(FlowManager *mgr);

uint64_t flow_manager_drop_deferred_overflow(const FlowManager *mgr);

void flow_manager_dispatch_wake(FlowManager *mgr);

/*
 * Wake dispatcher only when this flow may have deferred work waiting on a
 * freed per-flow queue slot. Safe no-op if deferred is empty for flow_id.
 */
void flow_manager_dispatch_wake_if_deferred(FlowManager *mgr, uint32_t flow_id);

#endif /* FLOW_MANAGER_H */
