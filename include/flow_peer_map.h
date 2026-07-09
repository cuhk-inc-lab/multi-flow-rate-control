#ifndef FLOW_PEER_MAP_H
#define FLOW_PEER_MAP_H

/*
 * Map a UDP 5-tuple to an internal flow slot (flow_id) for FlowManager.
 *
 * Tuple = (src IP:port, dst IP:port, protocol). The full tuple is the
 * routing key; flow_id is the compact index assigned on first sight.
 */

#include <netinet/in.h>
#include <stdint.h>
#include <sys/socket.h>

typedef enum {
    FPM_OK = 0,
    FPM_ERR_INVALID = -1,
    FPM_ERR_ALLOC = -2,
    FPM_ERR_LIMIT = -3
} FlowPeerMapStatus;

typedef struct FlowTuple {
    struct sockaddr_storage src;
    socklen_t               src_len;
    struct sockaddr_storage dst;
    socklen_t               dst_len;
    uint8_t                 protocol; /* e.g. IPPROTO_UDP */
} FlowTuple;

typedef struct FlowPeerMap FlowPeerMap;

/*
 * Fill out from recvfrom src plus local socket bind address (dst) and protocol.
 * Returns 0 on success, -1 on invalid arguments.
 */
int flow_tuple_set(FlowTuple *out,
                   const struct sockaddr *src,
                   socklen_t src_len,
                   const struct sockaddr *dst,
                   socklen_t dst_len,
                   uint8_t protocol);

FlowPeerMapStatus flow_peer_map_init(FlowPeerMap **map, uint32_t max_flows);
void              flow_peer_map_destroy(FlowPeerMap *map);

/*
 * Return flow_id for this 5-tuple, or assign the next slot on first use.
 * Returns (uint32_t)-1 on error or when the table is full.
 */
uint32_t flow_peer_map_lookup(FlowPeerMap *map, const FlowTuple *tuple);

#endif /* FLOW_PEER_MAP_H */
