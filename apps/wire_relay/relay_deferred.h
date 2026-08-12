#ifndef WIRE_RELAY_RELAY_DEFERRED_H
#define WIRE_RELAY_RELAY_DEFERRED_H

/*
 * Per-flow packet-level deferred hub for wire_relay RX decoupling.
 *
 * Ownership:
 *   try_push OK  → hub owns pkt->datagram (caller pointer cleared)
 *   try_push fail → caller retains datagram
 *   try_pop OK   → caller owns out->datagram
 *
 * Single producer (RX) + single consumer (processing worker) are supported;
 * Phase 1 uses a mutex + condition variable (not lock-free).
 */

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

#ifndef RELAY_DEFERRED_QUOTA
#define RELAY_DEFERRED_QUOTA 8u
#endif

#ifndef RELAY_DEFAULT_DEFERRED_PER_FLOW
#define RELAY_DEFAULT_DEFERRED_PER_FLOW 128u
#endif

#ifndef RELAY_DEFAULT_DEFERRED_TOTAL
#define RELAY_DEFAULT_DEFERRED_TOTAL 1024u
#endif

#ifndef RELAY_DEFAULT_MAX_ACTIVE_FLOWS
#define RELAY_DEFAULT_MAX_ACTIVE_FLOWS 64u
#endif

#ifndef RELAY_MAX_ACTIVE_FLOWS_LIMIT
#define RELAY_MAX_ACTIVE_FLOWS_LIMIT 64u
#endif

typedef struct RelayDeferredPacket {
    uint8_t *datagram; /* owned complete UDP datagram */
    size_t   len;
    uint32_t flow_id;
    uint64_t enqueue_ns;
} RelayDeferredPacket;

typedef enum {
    RELAY_DEFERRED_OK = 0,
    RELAY_DEFERRED_ERR_INVALID = -1,
    RELAY_DEFERRED_ERR_ALLOC = -2,
    RELAY_DEFERRED_ERR_FULL_FLOW = -3,
    RELAY_DEFERRED_ERR_FULL_TOTAL = -4,
    RELAY_DEFERRED_ERR_TABLE_FULL = -5,
    RELAY_DEFERRED_ERR_EMPTY = -6,
    RELAY_DEFERRED_ERR_SHUTDOWN = -7
} RelayDeferredStatus;

typedef struct RelayDeferredHubConfig {
    uint32_t max_active_flows; /* must be 1..64 */
    size_t   per_flow_capacity; /* datagrams per flow; >0 */
    size_t   total_capacity;    /* global datagram cap; >0 */
} RelayDeferredHubConfig;

typedef struct RelayDeferredHubStats {
    uint64_t drop_overflow_flow;
    uint64_t drop_overflow_total;
    uint64_t drop_table_full;
    uint64_t enqueue_ok;
    uint64_t dequeue_ok;
    uint64_t high_watermark;
} RelayDeferredHubStats;

typedef struct RelayDeferredSlot {
    int                   in_use;
    uint32_t              wire_flow_id;
    RelayDeferredPacket  *ring;
    size_t                capacity;
    size_t                head;
    size_t                tail;
    size_t                count;
    uint64_t              drop_overflow_flow;
} RelayDeferredSlot;

typedef struct RelayDeferredHub {
    RelayDeferredHubConfig config;
    RelayDeferredSlot     *slots;
    size_t                 total_count;
    uint64_t               active_bits; /* bit i => slots[i].count > 0 */
    uint32_t               rr_cursor;
    int                    rr_sticky_valid;
    uint32_t               rr_sticky_slot;
    uint32_t               rr_quota_left; /* remaining pops on sticky slot */
    uint64_t               wake_gen;
    pthread_mutex_t        mu;
    pthread_cond_t         wake_cv;
    int                    shutdown;
    int                    mu_inited;
    int                    cv_inited;
    RelayDeferredHubStats  stats;
} RelayDeferredHub;

RelayDeferredStatus relay_deferred_hub_init(RelayDeferredHub *hub,
                                            const RelayDeferredHubConfig *cfg);
void                relay_deferred_hub_shutdown(RelayDeferredHub *hub);
void                relay_deferred_hub_destroy(RelayDeferredHub *hub);

/*
 * Enqueue one owned datagram. On success ownership moves into the hub and
 * pkt->datagram is set to NULL. On failure ownership stays with the caller.
 * Drops are not performed here — caller frees on failure and accounts drops.
 * Hub still increments the matching drop_* counter for observability.
 */
RelayDeferredStatus relay_deferred_hub_try_push(RelayDeferredHub *hub,
                                                RelayDeferredPacket *pkt);

/*
 * Wait until at least one active flow has packets, or shutdown.
 * Uses pthread_cond_wait (no busy poll). Returns SHUTDOWN when stopped and
 * empty; OK when work may be available.
 */
RelayDeferredStatus relay_deferred_hub_wait(RelayDeferredHub *hub);

/*
 * Pop one datagram from the next non-empty active slot (round-robin).
 * Within a flow, FIFO is preserved. Returns EMPTY when no active work.
 * On OK, ownership of out->datagram moves to the caller.
 */
RelayDeferredStatus relay_deferred_hub_try_pop(RelayDeferredHub *hub,
                                               RelayDeferredPacket *out);

size_t relay_deferred_hub_total_count(const RelayDeferredHub *hub);
int    relay_deferred_hub_is_shutdown(const RelayDeferredHub *hub);
void   relay_deferred_hub_stats_snapshot(const RelayDeferredHub *hub,
                                         RelayDeferredHubStats *out);

#endif /* WIRE_RELAY_RELAY_DEFERRED_H */
