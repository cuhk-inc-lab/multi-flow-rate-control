#include "generation_cache.h"

#include <stdlib.h>
#include <string.h>

#ifndef GEN_CACHE_STAT_FLOWS
#define GEN_CACHE_STAT_FLOWS 8u
#endif

void generation_cache_config_defaults(GenerationCacheConfig *cfg)
{
    if (cfg == NULL) {
        return;
    }
    cfg->gen_timeout_ms = GEN_CACHE_DEFAULT_TIMEOUT_MS;
    cfg->max_gens_global = GEN_CACHE_DEFAULT_MAX_GENS_GLOBAL;
    cfg->max_gens_per_flow = GEN_CACHE_DEFAULT_MAX_GENS_PER_FLOW;
    cfg->max_cache_bytes = GEN_CACHE_DEFAULT_MAX_BYTES;
}

static size_t flow_stat_index(uint32_t flow_id)
{
    if (flow_id >= GEN_CACHE_STAT_FLOWS) {
        return GEN_CACHE_STAT_FLOWS - 1u;
    }
    return (size_t)flow_id;
}

static void entry_free(GenerationEntry *entry)
{
    uint16_t i;

    if (entry == NULL) {
        return;
    }
    if (entry->slots != NULL) {
        for (i = 0; i < entry->shard_count; i++) {
            free(entry->slots[i].datagram_copy);
            entry->slots[i].datagram_copy = NULL;
        }
        free(entry->slots);
        entry->slots = NULL;
    }
    free(entry);
}

static void lru_unlink(GenerationCache *cache, GenerationEntry *entry)
{
    if (entry->lru_prev != NULL) {
        entry->lru_prev->lru_next = entry->lru_next;
    } else {
        cache->lru_head = entry->lru_next;
    }
    if (entry->lru_next != NULL) {
        entry->lru_next->lru_prev = entry->lru_prev;
    } else {
        cache->lru_tail = entry->lru_prev;
    }
    entry->lru_prev = NULL;
    entry->lru_next = NULL;
}

static void lru_push_mru(GenerationCache *cache, GenerationEntry *entry)
{
    entry->lru_prev = NULL;
    entry->lru_next = cache->lru_head;
    if (cache->lru_head != NULL) {
        cache->lru_head->lru_prev = entry;
    } else {
        cache->lru_tail = entry;
    }
    cache->lru_head = entry;
}

static void lru_touch(GenerationCache *cache, GenerationEntry *entry)
{
    if (cache->lru_head == entry) {
        return;
    }
    lru_unlink(cache, entry);
    lru_push_mru(cache, entry);
}

enum {
    CACHE_REMOVE_EVICT = 0,
    CACHE_REMOVE_TIMEOUT = 1,
    CACHE_REMOVE_SILENT = 2
};

static void cache_remove_entry(GenerationCache *cache, GenerationEntry *entry,
                               int reason)
{
    size_t fi;

    if (cache == NULL || entry == NULL) {
        return;
    }
    lru_unlink(cache, entry);
    if (cache->stats.gen_cached_bytes_current >= entry->bytes) {
        cache->stats.gen_cached_bytes_current -= entry->bytes;
    } else {
        cache->stats.gen_cached_bytes_current = 0;
    }
    fi = flow_stat_index(entry->key.flow_id);
    if (cache->flow_counts[fi] > 0) {
        cache->flow_counts[fi]--;
    }
    if (cache->count > 0) {
        cache->count--;
    }
    if (reason == CACHE_REMOVE_TIMEOUT) {
        cache->stats.gen_timeout++;
    } else if (reason == CACHE_REMOVE_EVICT) {
        cache->stats.gen_evicted++;
    }
    entry_free(entry);
}

static int entry_timed_out(const GenerationCache *cache,
                           const GenerationEntry *entry, uint64_t now_ns)
{
    uint64_t timeout_ns;

    if (cache->cfg.gen_timeout_ms == 0) {
        return 0;
    }
    timeout_ns = (uint64_t)cache->cfg.gen_timeout_ms * 1000000ull;
    if (now_ns < entry->last_update_ns) {
        return 0;
    }
    return (now_ns - entry->last_update_ns) >= timeout_ns;
}

static size_t expire_one_pass(GenerationCache *cache, uint64_t now_ns,
                              int32_t flow_id_filter)
{
    GenerationEntry *cur;
    GenerationEntry *next;
    size_t removed = 0;

    cur = cache->lru_tail;
    while (cur != NULL) {
        next = cur->lru_prev;
        if ((flow_id_filter < 0 ||
             (uint32_t)flow_id_filter == cur->key.flow_id) &&
            entry_timed_out(cache, cur, now_ns)) {
            cache_remove_entry(cache, cur, CACHE_REMOVE_TIMEOUT);
            removed++;
        }
        cur = next;
    }
    return removed;
}

size_t generation_cache_expire(GenerationCache *cache, uint64_t now_ns,
                               int32_t flow_id_filter)
{
    if (cache == NULL) {
        return 0;
    }
    return expire_one_pass(cache, now_ns, flow_id_filter);
}

uint64_t generation_cache_earliest_deadline_ns(const GenerationCache *cache)
{
    GenerationEntry *cur;
    uint64_t timeout_ns;
    uint64_t earliest = 0;

    if (cache == NULL || cache->count == 0 || cache->cfg.gen_timeout_ms == 0) {
        return 0;
    }
    timeout_ns = (uint64_t)cache->cfg.gen_timeout_ms * 1000000ull;
    for (cur = cache->lru_head; cur != NULL; cur = cur->lru_next) {
        uint64_t deadline = cur->last_update_ns + timeout_ns;

        if (earliest == 0 || deadline < earliest) {
            earliest = deadline;
        }
    }
    return earliest;
}

int generation_cache_poll_timeout_ms(const GenerationCache *cache,
                                     uint64_t now_ns, int empty_poll_ms)
{
    uint64_t deadline;
    uint64_t remain_ns;
    unsigned long long ms;

    if (empty_poll_ms < 1) {
        empty_poll_ms = 1;
    }
    if (cache == NULL || cache->count == 0) {
        return empty_poll_ms;
    }
    deadline = generation_cache_earliest_deadline_ns(cache);
    if (deadline == 0) {
        return empty_poll_ms;
    }
    if (now_ns >= deadline) {
        return 0;
    }
    remain_ns = deadline - now_ns;
    /* ceil(remain_ns / 1e6) */
    ms = (remain_ns + 999999ull) / 1000000ull;
    if (ms == 0) {
        return 0;
    }
    if (ms > (unsigned long long)empty_poll_ms) {
        return empty_poll_ms;
    }
    return (int)ms;
}

static GenerationEntry *find_entry(GenerationCache *cache, uint32_t flow_id,
                                   uint64_t block_id)
{
    GenerationEntry *cur;

    for (cur = cache->lru_head; cur != NULL; cur = cur->lru_next) {
        if (cur->key.flow_id == flow_id && cur->key.block_id == block_id) {
            return cur;
        }
    }
    return NULL;
}

GenerationEntry *generation_cache_find(GenerationCache *cache,
                                       uint32_t flow_id, uint64_t block_id)
{
    if (cache == NULL) {
        return NULL;
    }
    return find_entry(cache, flow_id, block_id);
}

int generation_cache_slot_present(const GenerationEntry *entry,
                                  uint16_t shard_index)
{
    if (entry == NULL || entry->slots == NULL ||
        shard_index >= entry->shard_count) {
        return 0;
    }
    return entry->slots[shard_index].present != 0;
}

static int meta_matches(const GenerationEntry *entry, const WireHeader *hdr)
{
    return entry->final_dst == hdr->final_dst &&
           entry->type == hdr->type &&
           entry->shard_count == hdr->shard_count &&
           entry->valid_len == hdr->valid_len &&
           entry->payload_len == hdr->payload_len;
}

static int needs_eviction(const GenerationCache *cache, uint32_t flow_id,
                          size_t add_bytes)
{
    size_t fi = flow_stat_index(flow_id);

    if (cache->flow_counts[fi] >= cache->cfg.max_gens_per_flow) {
        return 1;
    }
    if (cache->count >= cache->cfg.max_gens_global) {
        return 1;
    }
    if (cache->stats.gen_cached_bytes_current + add_bytes >
        cache->cfg.max_cache_bytes) {
        return 1;
    }
    return 0;
}

/*
 * Evict LRU entries until creating a new gen for flow_id with add_bytes fits.
 * Prefer evicting within the same flow when per-flow limit is the constraint;
 * otherwise global LRU.
 */
static int make_room(GenerationCache *cache, uint32_t flow_id,
                     size_t add_bytes, uint64_t now_ns)
{
    size_t fi = flow_stat_index(flow_id);
    size_t guard = 0;

    (void)expire_one_pass(cache, now_ns, -1);

    while (needs_eviction(cache, flow_id, add_bytes) &&
           cache->count > 0 && guard < cache->cfg.max_gens_global + 8u) {
        GenerationEntry *victim = NULL;
        GenerationEntry *cur;

        guard++;
        if (cache->flow_counts[fi] >= cache->cfg.max_gens_per_flow) {
            for (cur = cache->lru_tail; cur != NULL; cur = cur->lru_prev) {
                if (cur->key.flow_id == flow_id) {
                    victim = cur;
                    break;
                }
            }
        }
        if (victim == NULL) {
            victim = cache->lru_tail;
        }
        if (victim == NULL) {
            break;
        }
        /* Never evict the entry we are about to grow into — none yet. */
        cache_remove_entry(cache, victim, CACHE_REMOVE_EVICT);
    }

    if (needs_eviction(cache, flow_id, add_bytes)) {
        return -1;
    }
    return 0;
}

static GenerationEntry *entry_create(const WireHeader *hdr, uint64_t now_ns)
{
    GenerationEntry *entry;

    if (hdr->shard_count == 0 ||
        hdr->shard_count > GEN_CACHE_MAX_SHARD_COUNT ||
        hdr->shard_index >= hdr->shard_count) {
        return NULL;
    }

    entry = calloc(1, sizeof(*entry));
    if (entry == NULL) {
        return NULL;
    }
    entry->key.flow_id = hdr->flow_id;
    entry->key.block_id = hdr->block_id;
    entry->final_dst = hdr->final_dst;
    entry->type = hdr->type;
    entry->shard_count = hdr->shard_count;
    entry->valid_len = hdr->valid_len;
    entry->payload_len = hdr->payload_len;
    entry->min_ttl = hdr->ttl;
    entry->meta_set = 1;
    entry->state = GEN_COLLECTING;
    entry->created_ns = now_ns;
    entry->last_update_ns = now_ns;
    entry->slots = calloc(hdr->shard_count, sizeof(*entry->slots));
    if (entry->slots == NULL) {
        free(entry);
        return NULL;
    }
    return entry;
}

static GenerationInsertStatus store_slot(GenerationCache *cache,
                                         GenerationEntry *entry,
                                         const WireHeader *hdr,
                                         const uint8_t *datagram,
                                         size_t len,
                                         uint64_t now_ns)
{
    GenerationSlot *slot;
    uint8_t *copy;

    if (hdr->shard_index >= entry->shard_count) {
        cache->stats.gen_metadata_mismatch++;
        return GEN_INSERT_MISMATCH;
    }

    slot = &entry->slots[hdr->shard_index];
    if (slot->present) {
        /* Do not refresh timeout, LRU, slots, present_count, bytes, or min_ttl. */
        cache->stats.gen_duplicate++;
        return GEN_INSERT_DUPLICATE;
    }

    /* Growing an existing entry may need byte-limit eviction. */
    while (cache->stats.gen_cached_bytes_current + len >
               cache->cfg.max_cache_bytes &&
           cache->count > 0) {
        GenerationEntry *victim = cache->lru_tail;

        if (victim == NULL || victim == entry) {
            /* Try any other entry. */
            for (victim = cache->lru_tail; victim != NULL;
                 victim = victim->lru_prev) {
                if (victim != entry) {
                    break;
                }
            }
        }
        if (victim == NULL || victim == entry) {
            cache->stats.gen_admission_failed++;
            return GEN_INSERT_ADMISSION_FAILED;
        }
        cache_remove_entry(cache, victim, CACHE_REMOVE_EVICT);
    }
    if (cache->stats.gen_cached_bytes_current + len >
        cache->cfg.max_cache_bytes) {
        cache->stats.gen_admission_failed++;
        return GEN_INSERT_ADMISSION_FAILED;
    }

    copy = malloc(len);
    if (copy == NULL) {
        cache->stats.gen_admission_failed++;
        return GEN_INSERT_ADMISSION_FAILED;
    }
    memcpy(copy, datagram, len);
    slot->datagram_copy = copy;
    slot->len = len;
    slot->present = 1;
    entry->present_count++;
    entry->bytes += len;
    entry->last_update_ns = now_ns;
    if (hdr->ttl < entry->min_ttl) {
        entry->min_ttl = hdr->ttl;
    }
    cache->stats.gen_cached_bytes_current += len;
    if (cache->stats.gen_cached_bytes_current >
        cache->stats.gen_cached_bytes_peak) {
        cache->stats.gen_cached_bytes_peak =
            cache->stats.gen_cached_bytes_current;
    }
    lru_touch(cache, entry);

    if (entry->present_count >= entry->shard_count &&
        entry->state != GEN_READY) {
        entry->state = GEN_READY;
        cache->stats.gen_ready++;
    }
    return GEN_INSERT_OK;
}

GenerationInsertStatus generation_cache_insert(
    GenerationCache *cache,
    const WireHeader *hdr,
    const uint8_t *datagram,
    size_t len,
    uint64_t now_ns,
    GenerationEntry **out_entry)
{
    GenerationEntry *entry;

    if (out_entry != NULL) {
        *out_entry = NULL;
    }
    if (cache == NULL || hdr == NULL || datagram == NULL ||
        len < WIRE_HEADER_SIZE) {
        return GEN_INSERT_INVALID;
    }
    if (hdr->type != WIRE_TYPE_DATA) {
        return GEN_INSERT_INVALID;
    }
    if (hdr->shard_count == 0 ||
        hdr->shard_count > GEN_CACHE_MAX_SHARD_COUNT ||
        hdr->shard_index >= hdr->shard_count) {
        cache->stats.gen_metadata_mismatch++;
        return GEN_INSERT_MISMATCH;
    }

    (void)expire_one_pass(cache, now_ns, -1);

    entry = find_entry(cache, hdr->flow_id, hdr->block_id);
    if (entry != NULL) {
        if (!meta_matches(entry, hdr)) {
            cache->stats.gen_metadata_mismatch++;
            if (out_entry != NULL) {
                *out_entry = entry;
            }
            return GEN_INSERT_MISMATCH;
        }
        {
            GenerationInsertStatus st =
                store_slot(cache, entry, hdr, datagram, len, now_ns);
            if (out_entry != NULL &&
                (st == GEN_INSERT_OK || st == GEN_INSERT_DUPLICATE)) {
                *out_entry = entry;
            }
            return st;
        }
    }

    if (make_room(cache, hdr->flow_id, len, now_ns) != 0) {
        cache->stats.gen_admission_failed++;
        return GEN_INSERT_ADMISSION_FAILED;
    }

    entry = entry_create(hdr, now_ns);
    if (entry == NULL) {
        cache->stats.gen_admission_failed++;
        return GEN_INSERT_ADMISSION_FAILED;
    }

    {
        GenerationInsertStatus st;
        size_t fi = flow_stat_index(hdr->flow_id);

        lru_push_mru(cache, entry);
        cache->count++;
        cache->flow_counts[fi]++;
        cache->stats.gen_created++;
        cache->stats.per_flow_created[fi]++;

        st = store_slot(cache, entry, hdr, datagram, len, now_ns);
        if (st != GEN_INSERT_OK) {
            /* Rollback empty/failed new entry without eviction accounting. */
            if (cache->stats.gen_created > 0) {
                cache->stats.gen_created--;
            }
            if (cache->stats.per_flow_created[fi] > 0) {
                cache->stats.per_flow_created[fi]--;
            }
            cache_remove_entry(cache, entry, CACHE_REMOVE_SILENT);
            return st;
        }
        if (out_entry != NULL) {
            *out_entry = entry;
        }
        return GEN_INSERT_OK;
    }
}

int generation_cache_init(GenerationCache *cache,
                          const GenerationCacheConfig *cfg)
{
    GenerationCacheConfig local;

    if (cache == NULL) {
        return -1;
    }
    memset(cache, 0, sizeof(*cache));
    if (cfg == NULL) {
        generation_cache_config_defaults(&local);
        cfg = &local;
    }
    cache->cfg = *cfg;
    if (cache->cfg.gen_timeout_ms == 0) {
        cache->cfg.gen_timeout_ms = GEN_CACHE_DEFAULT_TIMEOUT_MS;
    }
    if (cache->cfg.max_gens_global == 0) {
        cache->cfg.max_gens_global = GEN_CACHE_DEFAULT_MAX_GENS_GLOBAL;
    }
    if (cache->cfg.max_gens_per_flow == 0) {
        cache->cfg.max_gens_per_flow = GEN_CACHE_DEFAULT_MAX_GENS_PER_FLOW;
    }
    if (cache->cfg.max_cache_bytes == 0) {
        cache->cfg.max_cache_bytes = GEN_CACHE_DEFAULT_MAX_BYTES;
    }
    return 0;
}

void generation_cache_destroy(GenerationCache *cache)
{
    GenerationEntry *cur;
    GenerationEntry *next;

    if (cache == NULL) {
        return;
    }
    cur = cache->lru_head;
    while (cur != NULL) {
        next = cur->lru_next;
        entry_free(cur);
        cur = next;
    }
    memset(cache, 0, sizeof(*cache));
}

size_t generation_cache_count(const GenerationCache *cache)
{
    return cache != NULL ? cache->count : 0;
}

size_t generation_cache_count_flow(const GenerationCache *cache,
                                   uint32_t flow_id)
{
    if (cache == NULL) {
        return 0;
    }
    return cache->flow_counts[flow_stat_index(flow_id)];
}

const GenerationCacheStats *generation_cache_stats(
    const GenerationCache *cache)
{
    return cache != NULL ? &cache->stats : NULL;
}
