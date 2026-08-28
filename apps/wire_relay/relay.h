#ifndef WIRE_RELAY_RELAY_H
#define WIRE_RELAY_RELAY_H

#include "egress_queue.h"
#include "generation_cache.h"
#include "local_source.h"
#include "process.h"
#include "recode.h"
#include "relay_deferred.h"
#include "wire_header.h"

#include <stddef.h>
#include <stdint.h>

/*
 * Unified per-node pipeline (wire_relay):
 *
 *   UDP in
 *     → ttl==0? drop
 *     → final_dst == local_node_id?
 *           yes → delivery_fn (decode → file/app); no TTL-- / no egress
 *           no  → TTL--
 *               → [optional] recode_fn (per-datagram; NULL = opaque)
 *               → [optional] decode_reencode_fn (Phase 3A reserved; stub OPAQUE)
 *               → ACK/Data EgressQueues → fair TX
 *                 → [optional] egress_fn → sendto(next-hop/return route)
 *
 *   Local file/FIFO (optional LocalSourceConfig)
 *     → encode → fill final_dst/ttl → relay_inject_wire_datagram
 *       → same Destination Check / transit / egress as UDP in
 */

#ifndef RELAY_MAX_FLOWS
#define RELAY_MAX_FLOWS 8u
#endif

#ifndef RELAY_DEFAULT_EGRESS_CAPACITY
#define RELAY_DEFAULT_EGRESS_CAPACITY 16384u
#endif

#ifndef RELAY_ACK_EGRESS_CAPACITY
#define RELAY_ACK_EGRESS_CAPACITY 1024u
#endif

#ifndef RELAY_ACK_EGRESS_QUOTA
#define RELAY_ACK_EGRESS_QUOTA 8u
#endif

#ifndef RELAY_MAX_DATAGRAM
#define RELAY_MAX_DATAGRAM (WIRE_MAX_HEADER_SIZE + 2048u)
#endif

typedef enum {
    RELAY_SRC_PREVIOUS_NODE = 1,
    RELAY_SRC_LOCAL_ENCODER = 2
} RelayPacketSource;

typedef int (*RelayDeliveryFn)(const uint8_t *datagram, size_t len,
                               const WireHeader *hdr, void *ctx);
/*
 * Local-destination delivery callback (final_dst == local_node_id).
 *
 * Invoked AFTER RelayCtx.ingress_mu is released. Do not assume ingress_mu is
 * held. Thread-safety is the callback owner's responsibility (LocalDecodeHub
 * serializes via its own mutex). Codec recover/decode and file I/O must not
 * run under ingress_mu.
 *
 * Behavior change vs early Phase-1/2: delivery is no longer serialized by
 * ingress_mu alone.
 */

/* Optional TX capture for tests; when set, sendto is skipped. */
typedef void (*RelayTxCaptureFn)(const uint8_t *datagram, size_t len,
                                 void *ctx);

typedef struct RelayFlowStats {
    uint64_t rx;
    uint64_t forward;
    uint64_t local_deliver;
    uint64_t drop_ttl;
    uint64_t drop_malformed;
    uint64_t drop_send;
    uint64_t drop_egress_full;
    uint64_t drop_egress_timeout;
    uint64_t drop_deferred_overflow_flow;
    uint64_t drop_deferred_overflow_total;
    uint64_t drop_deferred_table_full;
    uint64_t inject_ok;
    uint64_t inject_reject_loopback;
    uint64_t gen_created;
    uint64_t gen_ready;
    uint64_t gen_timeout;
    uint64_t gen_evicted;
    uint64_t gen_admission_failed;
    uint64_t gen_metadata_mismatch;
    uint64_t gen_duplicate;
    uint64_t forward_data;
    uint64_t forward_ack;
    uint64_t drop_no_return_hop;
} RelayFlowStats;

typedef struct RelayConfig {
    uint8_t             local_node_id;
    uint16_t            listen_port;
    const char         *next_hop_host;
    uint16_t            next_hop_port;
    const char         *return_hop_host;
    uint16_t            return_hop_port;
    /*
     * Optional per-datagram mid-hop transform after TTL--
     * (default NULL = opaque forward). When set, must still produce a
     * complete wire datagram; TX only sends those bytes.
     */
    RelayRecodeFn       recode_fn;
    void               *recode_ctx;
    /*
     * Optional in-place transform after EgressQueue dequeue, immediately
     * before capture/sendto. Case c uses this to undo its ingress +1.
     */
    RelayEgressFn       egress_fn;
    void               *egress_ctx;
    /*
     * Reserved Phase 3A generation-level decode-and-reencode hook
     * (default NULL = not called). Stub: relay_decode_reencode_stub.
     * Non-OPAQUE actions are not implemented and fall back to opaque.
     */
    RelayDecodeReencodeFn decode_reencode_fn;
    void                 *decode_reencode_ctx;
    /* See RelayDeliveryFn: runs outside ingress_mu (P0A). */
    RelayDeliveryFn     delivery_fn;
    void               *delivery_ctx;
    /*
     * Optional local file/FIFO source. When non-NULL, relay_run starts a
     * thread that encodes and injects into the same ingress/egress path.
     * Pointer must remain valid for the duration of relay_run.
     */
    const LocalSourceConfig *local_source;
    RelayProcessMode    process_mode; /* default FORWARD */
    RelayProcessFn      process_fn;   /* optional; default continue forward */
    void               *process_ctx;
    /* 0 => run until SIGINT/SIGTERM; otherwise exit after idle seconds. */
    unsigned            idle_exit_sec;
    /* Packet slots in the DATA/other EgressQueue (default 16384). */
    size_t              egress_capacity;
    /*
     * Max wait when EgressQueue is full (milliseconds). 0 = try-drop only
     * (egress_queue_try_enqueue); >0 = timed backpressure on the processing
     * worker only (never on the UDP RX thread).
     */
    uint32_t            egress_wait_ms;
    /*
     * RX deferred hub (packet-level). 0 => library defaults:
     *   per_flow=4096, total=32768, max_active_flows=64 (must be 1..64).
     */
    size_t              deferred_per_flow;
    size_t              deferred_total;
    uint32_t            max_active_flows;
    /*
     * TEST ONLY: microseconds to sleep in the TX worker after dequeue and
     * before sendto/capture. Default 0 (no delay). Used to force egress
     * backlog in HOL experiments; must not be used in production.
     */
    uint32_t            test_tx_hold_us;
    /*
     * Wire-level local injection: if final_dst == local_node_id, reject by
     * default (1). Set 0 to deliver via delivery_fn instead.
     */
    int                 reject_local_encoder_loopback;
    /* GenerationCache limits (used when process_mode == CACHE). */
    uint32_t            gen_timeout_ms;
    size_t              max_gens_global;
    size_t              max_gens_per_flow;
    uint64_t            max_cache_bytes;
} RelayConfig;

typedef enum {
    RELAY_OK = 0,
    RELAY_ERR = -1
} RelayStatus;

typedef enum {
    RELAY_INGRESS_OK = 0,
    RELAY_INGRESS_ERR_INVALID = -1,
    RELAY_INGRESS_ERR_ALLOC = -2,
    RELAY_INGRESS_ERR_SHUTDOWN = -3,
    RELAY_INGRESS_ERR_LOOPBACK = -4
} RelayIngressStatus;

typedef struct RelayCtx RelayCtx;

RelayStatus relay_run(const RelayConfig *config);

/*
 * Wire-level local injection: datagram must already be a valid wire v3 UDP
 * payload (header + optional shard). This is NOT raw-bytes -> encoder.
 * Thread-safe vs concurrent injects via ingress mutex.
 *
 * Phase 1: inject remains synchronous (does not enter RelayDeferredHub).
 * Inject-only harness tests keep existing ordering semantics. Mixing inject
 * with the real UDP RX path does NOT preserve per-flow relative order
 * between the two sources.
 *
 * On success, copies datagram into an owned buffer then submits.
 */
RelayIngressStatus relay_inject_wire_datagram(RelayCtx *ctx,
                                              const uint8_t *datagram,
                                              size_t len);

/* Exposed for unit tests: mono time helper. */
uint64_t relay_mono_ns(void);

/*
 * In-process harness (no listen socket). TX calls capture_fn instead of
 * sendto when capture_fn != NULL. Used by Phase-2 unit tests.
 *
 * Lifecycle:
 * relay_harness_close() waits for an already-admitted inject until its
 * entire submit path, including a deferred RelayDeliveryFn invocation
 * outside ingress_mu, has returned.
 *
 * It still does NOT support arbitrary concurrent inject-vs-close for
 * threads that have not yet acquired ingress_mu / incremented
 * inject_in_flight. Callers must stop and join all potential injectors
 * before close.
 */
RelayStatus relay_harness_open(RelayCtx **out, const RelayConfig *config,
                               RelayTxCaptureFn capture_fn, void *capture_ctx);
void relay_harness_close(RelayCtx *ctx);
const RelayFlowStats *relay_total_stats(const RelayCtx *ctx);
const GenerationCacheStats *relay_cache_stats(const RelayCtx *ctx);
GenerationCache *relay_generation_cache(RelayCtx *ctx);
void relay_egress_stats_snapshot(const RelayCtx *ctx, EgressQueueStats *out);
void relay_ack_egress_stats_snapshot(const RelayCtx *ctx,
                                     EgressQueueStats *out);
void relay_data_egress_stats_snapshot(const RelayCtx *ctx,
                                      EgressQueueStats *out);
void relay_deferred_stats_snapshot(const RelayCtx *ctx,
                                   RelayDeferredHubStats *out);

#endif /* WIRE_RELAY_RELAY_H */
