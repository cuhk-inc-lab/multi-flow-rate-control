#include "ingress_push.h"

#include "packet.h"
#include "packet_pool.h"

#include <string.h>

IngressPushStatus ingress_push(FlowManager *mgr,
                               uint32_t flow_id,
                               const void *data,
                               size_t len)
{
    DataPacket *pkt;
    FlowManagerStatus st;
    PacketPool *pool;

    if (mgr == NULL || (len > 0 && data == NULL)) {
        return INGRESS_PUSH_ERR_INVALID;
    }

    pool = flow_manager_packet_pool(mgr);
    if (pool != NULL && len <= packet_pool_payload_cap(pool)) {
        pkt = packet_pool_alloc(pool, flow_id);
        if (pkt != NULL) {
            if (len > 0) {
                memcpy(pkt->payload, data, len);
            }
            pkt->payload_len = len;
        }
    } else {
        pkt = NULL;
    }

    if (pkt == NULL) {
        pkt = packet_create(flow_id, data, len);
        if (pkt == NULL) {
            return INGRESS_PUSH_ERR_ALLOC;
        }
    }

    st = flow_manager_push(mgr, &pkt);
    if (st != FM_OK) {
        packet_free(pkt);
        return INGRESS_PUSH_ERR_MGR;
    }

    return INGRESS_PUSH_OK;
}

IngressPushStatus ingress_push_prepared(FlowManager *mgr, DataPacket *pkt)
{
    FlowManagerStatus st;

    if (mgr == NULL || pkt == NULL) {
        return INGRESS_PUSH_ERR_INVALID;
    }

    st = flow_manager_push(mgr, &pkt);
    if (st != FM_OK) {
        packet_free(pkt);
        return INGRESS_PUSH_ERR_MGR;
    }

    return INGRESS_PUSH_OK;
}

IngressPushStatus ingress_push_tuple(FlowManager *mgr,
                                     FlowPeerMap *map,
                                     const FlowTuple *tuple,
                                     const void *data,
                                     size_t len)
{
    uint32_t flow_id;

    if (map == NULL || tuple == NULL) {
        return INGRESS_PUSH_ERR_INVALID;
    }

    flow_id = flow_peer_map_lookup(map, tuple);
    if (flow_id == (uint32_t)-1) {
        return INGRESS_PUSH_ERR_PEER;
    }

    return ingress_push(mgr, flow_id, data, len);
}
