#ifndef FLOW_PEER_MAP_H
#define FLOW_PEER_MAP_H

/*
 * Map UDP peer (src IP:port) to flow_id for multi-flow ingress.
 * Used when wg-obfs (or any UDP source) delivers datagrams without an
 * application-level flow header.
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

typedef struct FlowPeerMap FlowPeerMap;

FlowPeerMapStatus flow_peer_map_init(FlowPeerMap **map, uint32_t max_flows);
void              flow_peer_map_destroy(FlowPeerMap *map);

/*
 * Return existing flow_id for sa, or assign the next id in [0, max_flows).
 * Returns (uint32_t)-1 on error or when the table is full.
 */
uint32_t flow_peer_map_lookup(FlowPeerMap *map,
                              const struct sockaddr *sa,
                              socklen_t salen);

#endif /* FLOW_PEER_MAP_H */
