#ifndef WIRE_RELAY_GENERATION_CACHE_H
#define WIRE_RELAY_GENERATION_CACHE_H

#include "wire_header.h"

#include <stddef.h>
#include <stdint.h>

#ifndef GEN_CACHE_DEFAULT_TIMEOUT_MS
#define GEN_CACHE_DEFAULT_TIMEOUT_MS 500u
#endif

#ifndef GEN_CACHE_DEFAULT_MAX_GENS_GLOBAL
#define GEN_CACHE_DEFAULT_MAX_GENS_GLOBAL 256u
#endif

#ifndef GEN_CACHE_DEFAULT_MAX_GENS_PER_FLOW
#define GEN_CACHE_DEFAULT_MAX_GENS_PER_FLOW 32u
#endif

#ifndef GEN_CACHE_DEFAULT_MAX_BYTES
#define GEN_CACHE_DEFAULT_MAX_BYTES (32ull * 1024ull * 1024ull)
#endif

#ifndef GEN_CACHE_MAX_SHARD_COUNT
#define GEN_CACHE_MAX_SHARD_COUNT 256u
#endif

typedef struct GenerationKey {
    uint32_t flow_id;
    uint64_t block_id;
} GenerationKey;

typedef enum {
    GEN_COLLECTING = 0,
    GEN_READY = 1
} GenerationState;

typedef enum {
    GEN_INSERT_OK = 0,
    GEN_INSERT_DUPLICATE = 1,
    GEN_INSERT_MISMATCH = 2,
    GEN_INSERT_ADMISSION_FAILED = 3,
    GEN_INSERT_INVALID = 4
} GenerationInsertStatus;

typedef struct GenerationSlot {
    uint8_t *datagram_copy; /* owned copy; TTL already decremented */
    size_t   len;
    uint8_t  present;
} GenerationSlot;

typedef struct GenerationEntry {
    GenerationKey    key;
    uint8_t          final_dst;
    uint8_t          type;
    uint16_t         shard_count;
    uint16_t         valid_len;
    uint16_t         payload_len;
    uint8_t          min_ttl; /* min already-decremented TTL in cache */
    uint8_t          meta_set;
    GenerationState  state;
    uint16_t         present_count;
    uint64_t         created_ns;
    uint64_t         last_update_ns;
    size_t           bytes;
    GenerationSlot  *slots; /* length shard_count */
    struct GenerationEntry *lru_prev;
    struct GenerationEntry *lru_next;
} GenerationEntry;

typedef struct GenerationCacheStats {
    uint64_t gen_created;
    uint64_t gen_ready;
    uint64_t gen_timeout;
    uint64_t gen_evicted;
    uint64_t gen_admission_failed;
    uint64_t gen_metadata_mismatch;
    uint64_t gen_duplicate;
    uint64_t gen_cached_bytes_current;
    uint64_t gen_cached_bytes_peak;
    uint64_t per_flow_created[8]; /* RELAY_MAX_FLOWS-sized; clipped */
} GenerationCacheStats;

typedef struct GenerationCacheConfig {
    uint32_t gen_timeout_ms;
    size_t   max_gens_global;
    size_t   max_gens_per_flow;
    uint64_t max_cache_bytes;
} GenerationCacheConfig;

typedef struct GenerationCache GenerationCache;

void generation_cache_config_defaults(GenerationCacheConfig *cfg);

int generation_cache_init(GenerationCache *cache,
                          const GenerationCacheConfig *cfg);
void generation_cache_destroy(GenerationCache *cache);

/*
 * Insert a copy of datagram (must already have TTL decremented + encoded).
 * Only WIRE_TYPE_DATA. On DUPLICATE/MISMATCH/ADMISSION_FAILED the cache is
 * unchanged (or not created); caller still decides opaque forward.
 *
 * out_entry may be NULL; when non-NULL and status is OK/DUPLICATE, points at
 * the live entry (owned by cache).
 */
GenerationInsertStatus generation_cache_insert(
    GenerationCache *cache,
    const WireHeader *hdr,
    const uint8_t *datagram,
    size_t len,
    uint64_t now_ns,
    GenerationEntry **out_entry);

/* Drop timed-out entries. If flow_id_filter >= 0, only that flow. */
size_t generation_cache_expire(GenerationCache *cache, uint64_t now_ns,
                               int32_t flow_id_filter);

/*
 * Earliest last_update_ns + gen_timeout across live entries.
 * Returns 0 when cache is empty or timeout disabled.
 */
uint64_t generation_cache_earliest_deadline_ns(const GenerationCache *cache);

/*
 * Poll wait suggestion in milliseconds.
 * Empty cache => empty_poll_ms (e.g. 1000). Non-empty => ceil time until
 * earliest deadline, clamped to [1, empty_poll_ms] (0 if already due).
 */
int generation_cache_poll_timeout_ms(const GenerationCache *cache,
                                     uint64_t now_ns, int empty_poll_ms);

size_t generation_cache_count(const GenerationCache *cache);
size_t generation_cache_count_flow(const GenerationCache *cache,
                                   uint32_t flow_id);
const GenerationCacheStats *generation_cache_stats(
    const GenerationCache *cache);

/* Test helpers */
GenerationEntry *generation_cache_find(GenerationCache *cache,
                                       uint32_t flow_id, uint64_t block_id);
int generation_cache_slot_present(const GenerationEntry *entry,
                                  uint16_t shard_index);

/* Opaque storage size for stack/embedded allocation in RelayCtx. */
struct GenerationCache {
    GenerationCacheConfig cfg;
    GenerationEntry      *lru_head; /* MRU */
    GenerationEntry      *lru_tail; /* LRU */
    size_t                count;
    size_t                flow_counts[8];
    GenerationCacheStats  stats;
};

#endif /* WIRE_RELAY_GENERATION_CACHE_H */
