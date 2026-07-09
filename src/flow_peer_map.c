#include "flow_peer_map.h"

#include <stdlib.h>
#include <string.h>

typedef struct FlowPeerEntry {
    FlowTuple tuple;
    uint32_t  flow_id;
} FlowPeerEntry;

struct FlowPeerMap {
    FlowPeerEntry *entries;
    size_t         count;
    size_t         capacity;
    uint32_t       next_flow_id;
    uint32_t       max_flows;
};

static int sockaddr_equal(const struct sockaddr_storage *a,
                          socklen_t alen,
                          const struct sockaddr_storage *b,
                          socklen_t blen)
{
    if (a == NULL || b == NULL || alen == 0 || blen == 0) {
        return 0;
    }

    if (alen != blen) {
        return 0;
    }

    return memcmp(a, b, alen) == 0;
}

static int tuple_equal(const FlowTuple *a, const FlowTuple *b)
{
    if (a == NULL || b == NULL) {
        return 0;
    }

    if (a->protocol != b->protocol) {
        return 0;
    }

    return sockaddr_equal(&a->src, a->src_len, &b->src, b->src_len) &&
           sockaddr_equal(&a->dst, a->dst_len, &b->dst, b->dst_len);
}

int flow_tuple_set(FlowTuple *out,
                   const struct sockaddr *src,
                   socklen_t src_len,
                   const struct sockaddr *dst,
                   socklen_t dst_len,
                   uint8_t protocol)
{
    if (out == NULL || src == NULL || dst == NULL ||
        src_len == 0 || dst_len == 0 ||
        src_len > sizeof(out->src) || dst_len > sizeof(out->dst)) {
        return -1;
    }

    memset(out, 0, sizeof(*out));
    memcpy(&out->src, src, src_len);
    out->src_len = src_len;
    memcpy(&out->dst, dst, dst_len);
    out->dst_len = dst_len;
    out->protocol = protocol;
    return 0;
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

uint32_t flow_peer_map_lookup(FlowPeerMap *map, const FlowTuple *tuple)
{
    size_t i;

    if (map == NULL || tuple == NULL) {
        return (uint32_t)-1;
    }

    for (i = 0; i < map->count; i++) {
        if (tuple_equal(&map->entries[i].tuple, tuple)) {
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
    map->entries[map->count].tuple = *tuple;
    map->count++;
    return map->next_flow_id++;
}
