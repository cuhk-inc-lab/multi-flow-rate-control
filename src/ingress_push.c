#include "ingress_push.h"

#include "packet.h"

IngressPushStatus ingress_push(FlowManager *mgr,
                               uint32_t flow_id,
                               const void *data,
                               size_t len)
{
    DataPacket *pkt;
    FlowManagerStatus st;

    if (mgr == NULL || (len > 0 && data == NULL)) {
        return INGRESS_PUSH_ERR_INVALID;
    }

    pkt = packet_create(flow_id, data, len);
    if (pkt == NULL) {
        return INGRESS_PUSH_ERR_ALLOC;
    }

    st = flow_manager_push(mgr, &pkt);
    if (st != FM_OK) {
        packet_free(pkt);
        return INGRESS_PUSH_ERR_MGR;
    }

    return INGRESS_PUSH_OK;
}

IngressPushStatus ingress_push_peer(FlowManager *mgr,
                                    FlowPeerMap *map,
                                    const struct sockaddr *sa,
                                    socklen_t salen,
                                    const void *data,
                                    size_t len)
{
    uint32_t flow_id;

    if (map == NULL || sa == NULL) {
        return INGRESS_PUSH_ERR_INVALID;
    }

    flow_id = flow_peer_map_lookup(map, sa, salen);
    if (flow_id == (uint32_t)-1) {
        return INGRESS_PUSH_ERR_PEER;
    }

    return ingress_push(mgr, flow_id, data, len);
}
