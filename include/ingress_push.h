#ifndef INGRESS_PUSH_H
#define INGRESS_PUSH_H

/*
 * Push upstream bytes into FlowManager as DataPackets.
 *
 * - File demo: ingress_push(mgr, fixed_flow_id, ...)
 * - UDP: build FlowTuple (5-tuple) -> ingress_push_tuple(...)
 */

#include <stddef.h>
#include <stdint.h>

#include "flow_manager.h"
#include "flow_peer_map.h"

typedef enum {
    INGRESS_PUSH_OK = 0,
    INGRESS_PUSH_ERR_INVALID = -1,
    INGRESS_PUSH_ERR_ALLOC = -2,
    INGRESS_PUSH_ERR_MGR = -3,
    INGRESS_PUSH_ERR_PEER = -4
} IngressPushStatus;

IngressPushStatus ingress_push(FlowManager *mgr,
                               uint32_t flow_id,
                               const void *data,
                               size_t len);

/*
 * Resolve internal flow slot from the full UDP 5-tuple, then push.
 * The tuple is the routing key; flow_id is only the assigned slot index.
 */
IngressPushStatus ingress_push_tuple(FlowManager *mgr,
                                     FlowPeerMap *map,
                                     const FlowTuple *tuple,
                                     const void *data,
                                     size_t len);

#endif /* INGRESS_PUSH_H */
