#include "flow_peer_map.h"

#include <stdlib.h>
#include <string.h>

typedef struct FlowPeerEntry {
    struct sockaddr_storage addr;
    socklen_t               addrlen;
    uint32_t                flow_id;
} FlowPeerEntry;

struct FlowPeerMap {
    FlowPeerEntry *entries;
    size_t         count;
    size_t         capacity;
    uint32_t       next_flow_id;
    uint32_t       max_flows;
};

static int peer_addr_equal(const struct sockaddr *a, socklen_t alen,
                           const struct sockaddr *b, socklen_t blen)
{
    if (a == NULL || b == NULL || alen == 0 || blen == 0) {
        return 0;
    }

    if (alen != blen) {
        return 0;
    }

    return memcmp(a, b, alen) == 0;
}

FlowPeerMapStatus flow_peer_map_init(FlowPeerMap **map, uint32_t max_flows)
{
    FlowPeerMap *m;

    if (map == NULL || max_flows == 0) {
        return FPM_ERR_INVALID;
    }

    m = calloc(1, sizeof(*m));
    if (m == NULL) {
        return FPM_ERR_ALLOC;
    }

    m->capacity = 16;
    m->entries = calloc(m->capacity, sizeof(*m->entries));
    if (m->entries == NULL) {
        free(m);
        return FPM_ERR_ALLOC;
    }

    m->max_flows = max_flows;
    m->next_flow_id = 0;
    *map = m;
    return FPM_OK;
}

void flow_peer_map_destroy(FlowPeerMap *map)
{
    if (map == NULL) {
        return;
    }

    free(map->entries);
    free(map);
}

uint32_t flow_peer_map_lookup(FlowPeerMap *map,
                              const struct sockaddr *sa,
                              socklen_t salen)
{
    size_t i;

    if (map == NULL || sa == NULL || salen == 0 ||
        salen > sizeof(struct sockaddr_storage)) {
        return (uint32_t)-1;
    }

    for (i = 0; i < map->count; i++) {
        if (peer_addr_equal(sa, salen,
                            (const struct sockaddr *)&map->entries[i].addr,
                            map->entries[i].addrlen)) {
            return map->entries[i].flow_id;
        }
    }

    if (map->next_flow_id >= map->max_flows) {
        return (uint32_t)-1;
    }

    if (map->count >= map->capacity) {
        size_t         new_cap = map->capacity * 2u;
        FlowPeerEntry *grown;

        grown = realloc(map->entries, new_cap * sizeof(*grown));
        if (grown == NULL) {
            return (uint32_t)-1;
        }

        memset(grown + map->capacity, 0,
               (new_cap - map->capacity) * sizeof(*grown));
        map->entries = grown;
        map->capacity = new_cap;
    }

    map->entries[map->count].flow_id = map->next_flow_id;
    memcpy(&map->entries[map->count].addr, sa, salen);
    map->entries[map->count].addrlen = salen;
    map->count++;
    return map->next_flow_id++;
}
