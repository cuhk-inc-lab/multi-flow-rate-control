#ifndef WIRE_RELAY_RELAY_H
#define WIRE_RELAY_RELAY_H

#include "recode.h"
#include "wire_header.h"

#include <stddef.h>
#include <stdint.h>

#ifndef RELAY_MAX_FLOWS
#define RELAY_MAX_FLOWS 8u
#endif

#ifndef RELAY_DEFAULT_EGRESS_CAPACITY
#define RELAY_DEFAULT_EGRESS_CAPACITY 4096u
#endif

#ifndef RELAY_MAX_DATAGRAM
#define RELAY_MAX_DATAGRAM (WIRE_HEADER_SIZE + 2048u)
#endif

typedef enum {
    RELAY_SRC_PREVIOUS_NODE = 1,
    RELAY_SRC_LOCAL_ENCODER = 2
} RelayPacketSource;

typedef int (*RelayDeliveryFn)(const uint8_t *datagram, size_t len,
                               const WireHeader *hdr, void *ctx);

typedef struct RelayFlowStats {
    uint64_t rx;
    uint64_t forward;
    uint64_t local_deliver;
    uint64_t drop_ttl;
    uint64_t drop_malformed;
    uint64_t drop_send;
    uint64_t drop_egress_full;
    uint64_t inject_ok;
    uint64_t inject_reject_loopback;
} RelayFlowStats;

typedef struct RelayConfig {
    uint8_t             local_node_id;
    uint16_t            listen_port;
    const char         *next_hop_host;
    uint16_t            next_hop_port;
    /*
     * Optional Phase-1 hook (default NULL = FORWARD_ORIGINAL / opaque).
     * When set, must still produce a complete wire datagram; TX only sends
     * those bytes. Not used for GenerationCache (Phase 2+).
     */
    RelayRecodeFn       recode_fn;
    void               *recode_ctx;
    RelayDeliveryFn     delivery_fn;
    void               *delivery_ctx;
    /* 0 => run until SIGINT/SIGTERM; otherwise exit after idle seconds. */
    unsigned            idle_exit_sec;
    /* Packet slots in the global EgressQueue (default 4096). */
    size_t              egress_capacity;
    /*
     * Wire-level local injection: if final_dst == local_node_id, reject by
     * default (1). Set 0 to deliver via delivery_fn instead.
     */
    int                 reject_local_encoder_loopback;
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
 * Thread-safe vs RX via ingress mutex.
 *
 * On success, copies datagram into an owned buffer then submits.
 */
RelayIngressStatus relay_inject_wire_datagram(RelayCtx *ctx,
                                              const uint8_t *datagram,
                                              size_t len);

/* Exposed for unit tests: mono time helper. */
uint64_t relay_mono_ns(void);

#endif /* WIRE_RELAY_RELAY_H */
