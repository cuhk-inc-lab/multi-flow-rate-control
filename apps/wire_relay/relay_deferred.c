#include "relay_deferred.h"

#include <stdlib.h>
#include <string.h>

static void packet_clear(RelayDeferredPacket *pkt)
{
    if (pkt == NULL) {
        return;
    }
    pkt->datagram = NULL;
    pkt->len = 0;
    pkt->flow_id = 0;
    pkt->enqueue_ns = 0;
}

static void free_slot_ring(RelayDeferredSlot *slot)
{
    size_t i;

    if (slot == NULL || slot->ring == NULL) {
        return;
    }
    for (i = 0; i < slot->capacity; i++) {
        free(slot->ring[i].datagram);
        packet_clear(&slot->ring[i]);
    }
    free(slot->ring);
    slot->ring = NULL;
    slot->capacity = 0;
    slot->head = 0;
    slot->tail = 0;
    slot->count = 0;
}

static uint32_t slot_index_for_flow(const RelayDeferredHub *hub, uint32_t flow_id)
{
    if (hub->config.max_active_flows == 0) {
        return 0;
    }
    return flow_id % hub->config.max_active_flows;
}

static int find_slot_index(const RelayDeferredHub *hub, uint32_t flow_id,
                           uint32_t *out_index)
{
    uint32_t start;
    uint32_t i;
    uint32_t n;

    if (hub == NULL || out_index == NULL || hub->slots == NULL) {
        return -1;
    }
    n = hub->config.max_active_flows;
    start = slot_index_for_flow(hub, flow_id);
    for (i = 0; i < n; i++) {
        uint32_t idx = (start + i) % n;
        const RelayDeferredSlot *slot = &hub->slots[idx];

        if (slot->in_use && slot->wire_flow_id == flow_id) {
            *out_index = idx;
            return 0;
        }
    }
    return -1;
}

static int find_free_slot_index(const RelayDeferredHub *hub, uint32_t flow_id,
                                uint32_t *out_index)
{
    uint32_t start;
    uint32_t i;
    uint32_t n;

    if (hub == NULL || out_index == NULL || hub->slots == NULL) {
        return -1;
    }
    n = hub->config.max_active_flows;
    start = slot_index_for_flow(hub, flow_id);
    for (i = 0; i < n; i++) {
        uint32_t idx = (start + i) % n;

        if (!hub->slots[idx].in_use) {
            *out_index = idx;
            return 0;
        }
    }
    return -1;
}

static void mark_active(RelayDeferredHub *hub, uint32_t slot_index)
{
    hub->active_bits |= (1ull << slot_index);
}

static void clear_active(RelayDeferredHub *hub, uint32_t slot_index)
{
    hub->active_bits &= ~(1ull << slot_index);
}

static void release_empty_slot(RelayDeferredHub *hub, uint32_t slot_index)
{
    RelayDeferredSlot *slot = &hub->slots[slot_index];

    if (slot->count != 0) {
        return;
    }
    clear_active(hub, slot_index);
    slot->in_use = 0;
    slot->wire_flow_id = 0;
    slot->head = 0;
    slot->tail = 0;
}

static int config_valid(const RelayDeferredHubConfig *cfg)
{
    if (cfg == NULL) {
        return 0;
    }
    if (cfg->max_active_flows < 1u ||
        cfg->max_active_flows > RELAY_MAX_ACTIVE_FLOWS_LIMIT) {
        return 0;
    }
    if (cfg->per_flow_capacity == 0 || cfg->total_capacity == 0) {
        return 0;
    }
    return 1;
}

RelayDeferredStatus relay_deferred_hub_init(RelayDeferredHub *hub,
                                            const RelayDeferredHubConfig *cfg)
{
    uint32_t i;

    if (hub == NULL || !config_valid(cfg)) {
        return RELAY_DEFERRED_ERR_INVALID;
    }

    memset(hub, 0, sizeof(*hub));
    hub->config = *cfg;

    hub->slots = calloc(cfg->max_active_flows, sizeof(*hub->slots));
    if (hub->slots == NULL) {
        return RELAY_DEFERRED_ERR_ALLOC;
    }

    for (i = 0; i < cfg->max_active_flows; i++) {
        RelayDeferredSlot *slot = &hub->slots[i];

        slot->ring = calloc(cfg->per_flow_capacity, sizeof(*slot->ring));
        if (slot->ring == NULL) {
            uint32_t j;

            for (j = 0; j < i; j++) {
                free_slot_ring(&hub->slots[j]);
            }
            free(hub->slots);
            hub->slots = NULL;
            return RELAY_DEFERRED_ERR_ALLOC;
        }
        slot->capacity = cfg->per_flow_capacity;
    }

    if (pthread_mutex_init(&hub->mu, NULL) != 0) {
        for (i = 0; i < cfg->max_active_flows; i++) {
            free_slot_ring(&hub->slots[i]);
        }
        free(hub->slots);
        hub->slots = NULL;
        return RELAY_DEFERRED_ERR_ALLOC;
    }
    hub->mu_inited = 1;

    if (pthread_cond_init(&hub->wake_cv, NULL) != 0) {
        pthread_mutex_destroy(&hub->mu);
        hub->mu_inited = 0;
        for (i = 0; i < cfg->max_active_flows; i++) {
            free_slot_ring(&hub->slots[i]);
        }
        free(hub->slots);
        hub->slots = NULL;
        return RELAY_DEFERRED_ERR_ALLOC;
    }
    hub->cv_inited = 1;
    return RELAY_DEFERRED_OK;
}

void relay_deferred_hub_shutdown(RelayDeferredHub *hub)
{
    if (hub == NULL || !hub->mu_inited) {
        return;
    }
    pthread_mutex_lock(&hub->mu);
    hub->shutdown = 1;
    hub->wake_gen++;
    pthread_cond_broadcast(&hub->wake_cv);
    pthread_mutex_unlock(&hub->mu);
}

void relay_deferred_hub_destroy(RelayDeferredHub *hub)
{
    uint32_t i;

    if (hub == NULL) {
        return;
    }

    if (hub->mu_inited) {
        pthread_mutex_lock(&hub->mu);
        hub->shutdown = 1;
        pthread_mutex_unlock(&hub->mu);
    }

    if (hub->slots != NULL) {
        for (i = 0; i < hub->config.max_active_flows; i++) {
            free_slot_ring(&hub->slots[i]);
            hub->slots[i].in_use = 0;
        }
        free(hub->slots);
        hub->slots = NULL;
    }
    hub->total_count = 0;
    hub->active_bits = 0;

    if (hub->cv_inited) {
        pthread_cond_destroy(&hub->wake_cv);
        hub->cv_inited = 0;
    }
    if (hub->mu_inited) {
        pthread_mutex_destroy(&hub->mu);
        hub->mu_inited = 0;
    }
}

RelayDeferredStatus relay_deferred_hub_try_push(RelayDeferredHub *hub,
                                                RelayDeferredPacket *pkt)
{
    uint32_t slot_index;
    RelayDeferredSlot *slot;
    int became_nonempty = 0;

    if (hub == NULL || pkt == NULL || pkt->datagram == NULL || pkt->len == 0 ||
        !hub->mu_inited) {
        return RELAY_DEFERRED_ERR_INVALID;
    }

    pthread_mutex_lock(&hub->mu);
    if (hub->shutdown) {
        pthread_mutex_unlock(&hub->mu);
        return RELAY_DEFERRED_ERR_SHUTDOWN;
    }

    if (find_slot_index(hub, pkt->flow_id, &slot_index) != 0) {
        if (find_free_slot_index(hub, pkt->flow_id, &slot_index) != 0) {
            hub->stats.drop_table_full++;
            pthread_mutex_unlock(&hub->mu);
            return RELAY_DEFERRED_ERR_TABLE_FULL;
        }
        slot = &hub->slots[slot_index];
        slot->in_use = 1;
        slot->wire_flow_id = pkt->flow_id;
        slot->head = 0;
        slot->tail = 0;
        slot->count = 0;
    } else {
        slot = &hub->slots[slot_index];
    }

    if (hub->total_count >= hub->config.total_capacity) {
        hub->stats.drop_overflow_total++;
        pthread_mutex_unlock(&hub->mu);
        return RELAY_DEFERRED_ERR_FULL_TOTAL;
    }
    if (slot->count >= slot->capacity) {
        slot->drop_overflow_flow++;
        hub->stats.drop_overflow_flow++;
        pthread_mutex_unlock(&hub->mu);
        return RELAY_DEFERRED_ERR_FULL_FLOW;
    }

    if (slot->count == 0) {
        became_nonempty = 1;
    }

    slot->ring[slot->tail] = *pkt;
    packet_clear(pkt);
    slot->tail = (slot->tail + 1u) % slot->capacity;
    slot->count++;
    hub->total_count++;
    hub->stats.enqueue_ok++;
    if (hub->total_count > hub->stats.high_watermark) {
        hub->stats.high_watermark = hub->total_count;
    }

    if (became_nonempty) {
        mark_active(hub, slot_index);
        hub->wake_gen++;
        pthread_cond_signal(&hub->wake_cv);
    }

    pthread_mutex_unlock(&hub->mu);
    return RELAY_DEFERRED_OK;
}

RelayDeferredStatus relay_deferred_hub_wait(RelayDeferredHub *hub)
{
    if (hub == NULL || !hub->mu_inited) {
        return RELAY_DEFERRED_ERR_INVALID;
    }

    pthread_mutex_lock(&hub->mu);
    while (hub->active_bits == 0 && !hub->shutdown) {
        pthread_cond_wait(&hub->wake_cv, &hub->mu);
    }
    if (hub->active_bits == 0 && hub->shutdown) {
        pthread_mutex_unlock(&hub->mu);
        return RELAY_DEFERRED_ERR_SHUTDOWN;
    }
    pthread_mutex_unlock(&hub->mu);
    return RELAY_DEFERRED_OK;
}

static int select_rr_slot(RelayDeferredHub *hub, uint32_t *out_index)
{
    uint32_t n;
    uint32_t i;

    if (hub->active_bits == 0 || hub->config.max_active_flows == 0) {
        return -1;
    }

    /* Continue sticky flow while quota remains and packets remain. */
    if (hub->rr_sticky_valid && hub->rr_quota_left > 0) {
        uint32_t idx = hub->rr_sticky_slot;

        if ((hub->active_bits & (1ull << idx)) != 0 &&
            hub->slots[idx].count > 0) {
            *out_index = idx;
            return 0;
        }
        hub->rr_sticky_valid = 0;
        hub->rr_quota_left = 0;
    }

    n = hub->config.max_active_flows;
    for (i = 0; i < n; i++) {
        uint32_t idx = (hub->rr_cursor + i) % n;

        if ((hub->active_bits & (1ull << idx)) != 0 &&
            hub->slots[idx].count > 0) {
            *out_index = idx;
            hub->rr_sticky_valid = 1;
            hub->rr_sticky_slot = idx;
            hub->rr_quota_left = RELAY_DEFERRED_QUOTA;
            hub->rr_cursor = (idx + 1u) % n;
            return 0;
        }
    }
    hub->rr_sticky_valid = 0;
    hub->rr_quota_left = 0;
    return -1;
}

RelayDeferredStatus relay_deferred_hub_try_pop(RelayDeferredHub *hub,
                                               RelayDeferredPacket *out)
{
    uint32_t slot_index;
    RelayDeferredSlot *slot;

    if (hub == NULL || out == NULL || !hub->mu_inited) {
        return RELAY_DEFERRED_ERR_INVALID;
    }
    packet_clear(out);

    pthread_mutex_lock(&hub->mu);
    if (select_rr_slot(hub, &slot_index) != 0) {
        RelayDeferredStatus st = hub->shutdown ? RELAY_DEFERRED_ERR_SHUTDOWN
                                               : RELAY_DEFERRED_ERR_EMPTY;
        pthread_mutex_unlock(&hub->mu);
        return st;
    }

    slot = &hub->slots[slot_index];
    *out = slot->ring[slot->head];
    packet_clear(&slot->ring[slot->head]);
    slot->head = (slot->head + 1u) % slot->capacity;
    slot->count--;
    hub->total_count--;
    hub->stats.dequeue_ok++;
    if (hub->rr_sticky_valid && hub->rr_sticky_slot == slot_index &&
        hub->rr_quota_left > 0) {
        hub->rr_quota_left--;
        if (hub->rr_quota_left == 0 || slot->count == 0) {
            hub->rr_sticky_valid = 0;
            hub->rr_quota_left = 0;
        }
    }

    if (slot->count == 0) {
        release_empty_slot(hub, slot_index);
    }

    pthread_mutex_unlock(&hub->mu);
    return RELAY_DEFERRED_OK;
}

size_t relay_deferred_hub_total_count(const RelayDeferredHub *hub)
{
    size_t count;

    if (hub == NULL || !hub->mu_inited) {
        return 0;
    }
    pthread_mutex_lock((pthread_mutex_t *)&hub->mu);
    count = hub->total_count;
    pthread_mutex_unlock((pthread_mutex_t *)&hub->mu);
    return count;
}

int relay_deferred_hub_is_shutdown(const RelayDeferredHub *hub)
{
    int shutdown;

    if (hub == NULL || !hub->mu_inited) {
        return 1;
    }
    pthread_mutex_lock((pthread_mutex_t *)&hub->mu);
    shutdown = hub->shutdown;
    pthread_mutex_unlock((pthread_mutex_t *)&hub->mu);
    return shutdown;
}

void relay_deferred_hub_stats_snapshot(const RelayDeferredHub *hub,
                                       RelayDeferredHubStats *out)
{
    if (hub == NULL || out == NULL || !hub->mu_inited) {
        if (out != NULL) {
            memset(out, 0, sizeof(*out));
        }
        return;
    }
    pthread_mutex_lock((pthread_mutex_t *)&hub->mu);
    *out = hub->stats;
    pthread_mutex_unlock((pthread_mutex_t *)&hub->mu);
}
