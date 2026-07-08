#ifndef INGRESS_PUSH_H
#define INGRESS_PUSH_H

/*
 * Push upstream bytes into FlowManager as DataPackets.
 * Demo uses fixed flow_id; production UDP uses flow_peer_map_lookup().
 */

#include <stddef.h>
#include <stdint.h>
#include <sys/socket.h>

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

IngressPushStatus ingress_push_peer(FlowManager *mgr,
                                    FlowPeerMap *map,
                                    const struct sockaddr *sa,
                                    socklen_t salen,
                                    const void *data,
                                    size_t len);

#endif /* INGRESS_PUSH_H */
