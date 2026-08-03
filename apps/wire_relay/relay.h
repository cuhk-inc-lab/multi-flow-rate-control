#ifndef WIRE_RELAY_RELAY_H
#define WIRE_RELAY_RELAY_H

#include "recode.h"
#include "wire_header.h"

#include <stdint.h>

#ifndef RELAY_MAX_FLOWS
#define RELAY_MAX_FLOWS 8u
#endif

typedef int (*RelayDeliveryFn)(const uint8_t *datagram, size_t len,
                               const WireHeader *hdr, void *ctx);

typedef struct RelayFlowStats {
    uint64_t rx;
    uint64_t forward;
    uint64_t local_deliver;
    uint64_t drop_ttl;
    uint64_t drop_malformed;
    uint64_t drop_send;
} RelayFlowStats;

typedef struct RelayConfig {
    uint8_t             local_node_id;
    uint16_t            listen_port;
    const char         *next_hop_host;
    uint16_t            next_hop_port;
    RelayRecodeFn       recode_fn;      /* NULL = disabled (default) */
    void               *recode_ctx;
    RelayDeliveryFn     delivery_fn;    /* NULL = count-only local delivery */
    void               *delivery_ctx;
    /* 0 = run until SIGINT/SIGTERM; otherwise exit after this many idle seconds. */
    unsigned            idle_exit_sec;
} RelayConfig;

typedef enum {
    RELAY_OK = 0,
    RELAY_ERR = -1
} RelayStatus;

RelayStatus relay_run(const RelayConfig *config);

#endif /* WIRE_RELAY_RELAY_H */
