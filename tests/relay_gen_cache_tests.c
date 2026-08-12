#include "egress_queue.h"
#include "generation_cache.h"
#include "relay.h"
#include "wire_header.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_failures = 0;

#define EXPECT(cond)                                                           \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);    \
            g_failures++;                                                      \
        }                                                                      \
    } while (0)

static size_t make_data_datagram(uint8_t *out, size_t cap, uint32_t flow_id,
                                 uint64_t block_id, uint16_t shard_index,
                                 uint16_t shard_count, uint8_t final_dst,
                                 uint8_t ttl, uint16_t valid_len,
                                 uint16_t payload_len, uint8_t marker)
{
    WireHeader hdr;
    size_t len;

    if (cap < WIRE_HEADER_SIZE + payload_len) {
        return 0;
    }
    memset(&hdr, 0, sizeof(hdr));
    hdr.type = WIRE_TYPE_DATA;
    hdr.final_dst = final_dst;
    hdr.ttl = ttl;
    hdr.flow_id = flow_id;
    hdr.block_id = block_id;
    hdr.shard_index = shard_index;
    hdr.shard_count = shard_count;
    hdr.valid_len = valid_len;
    hdr.payload_len = payload_len;
    wire_header_encode(out, &hdr);
    memset(out + WIRE_HEADER_SIZE, marker, payload_len);
    len = WIRE_HEADER_SIZE + payload_len;
    return len;
}

static size_t make_end_datagram(uint8_t *out, size_t cap, uint32_t flow_id,
                                uint8_t final_dst, uint8_t ttl)
{
    WireHeader hdr;

    if (cap < WIRE_HEADER_SIZE) {
        return 0;
    }
    memset(&hdr, 0, sizeof(hdr));
    hdr.type = WIRE_TYPE_END;
    hdr.final_dst = final_dst;
    hdr.ttl = ttl;
    hdr.flow_id = flow_id;
    hdr.block_id = 0;
    hdr.shard_index = 0;
    hdr.shard_count = 1;
    hdr.valid_len = 0;
    hdr.payload_len = 0;
    wire_header_encode(out, &hdr);
    return WIRE_HEADER_SIZE;
}

static void test_key_isolation_flow(void)
{
    GenerationCache cache;
    GenerationCacheConfig cfg;
    uint8_t buf[WIRE_HEADER_SIZE + 16];
    size_t len;
    WireHeader hdr;
    uint64_t now = 1000000000ull;

    generation_cache_config_defaults(&cfg);
    EXPECT(generation_cache_init(&cache, &cfg) == 0);

    len = make_data_datagram(buf, sizeof(buf), 1, 42, 0, 2, 4, 7, 10, 16, 0xA1);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, now, NULL) ==
           GEN_INSERT_OK);

    len = make_data_datagram(buf, sizeof(buf), 2, 42, 0, 2, 4, 7, 10, 16, 0xA2);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, now, NULL) ==
           GEN_INSERT_OK);

    EXPECT(generation_cache_count(&cache) == 2);
    EXPECT(generation_cache_find(&cache, 1, 42) != NULL);
    EXPECT(generation_cache_find(&cache, 2, 42) != NULL);
    generation_cache_destroy(&cache);
}

static void test_key_isolation_block(void)
{
    GenerationCache cache;
    GenerationCacheConfig cfg;
    uint8_t buf[WIRE_HEADER_SIZE + 16];
    size_t len;
    WireHeader hdr;
    uint64_t now = 2000000000ull;

    generation_cache_config_defaults(&cfg);
    EXPECT(generation_cache_init(&cache, &cfg) == 0);

    len = make_data_datagram(buf, sizeof(buf), 3, 10, 0, 2, 4, 7, 10, 16, 1);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, now, NULL) ==
           GEN_INSERT_OK);

    len = make_data_datagram(buf, sizeof(buf), 3, 11, 0, 2, 4, 7, 10, 16, 2);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, now, NULL) ==
           GEN_INSERT_OK);

    EXPECT(generation_cache_count(&cache) == 2);
    EXPECT(generation_cache_find(&cache, 3, 10) != NULL);
    EXPECT(generation_cache_find(&cache, 3, 11) != NULL);
    generation_cache_destroy(&cache);
}

static void test_duplicate_and_mismatch(void)
{
    GenerationCache cache;
    GenerationCacheConfig cfg;
    uint8_t buf[WIRE_HEADER_SIZE + 16];
    size_t len;
    WireHeader hdr;
    GenerationEntry *entry;
    uint64_t now = 3000000000ull;
    const GenerationCacheStats *st;

    generation_cache_config_defaults(&cfg);
    EXPECT(generation_cache_init(&cache, &cfg) == 0);

    len = make_data_datagram(buf, sizeof(buf), 1, 5, 0, 2, 4, 7, 10, 16, 0x11);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, now, &entry) ==
           GEN_INSERT_OK);
    EXPECT(entry != NULL);
    EXPECT(entry->slots[0].datagram_copy[WIRE_HEADER_SIZE] == 0x11);

    /* Duplicate: different marker must not overwrite. */
    len = make_data_datagram(buf, sizeof(buf), 1, 5, 0, 2, 4, 7, 10, 16, 0x22);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    {
        uint64_t lu = entry->last_update_ns;
        uint16_t pc = entry->present_count;
        size_t bytes = entry->bytes;
        uint8_t min_ttl = entry->min_ttl;

        EXPECT(generation_cache_insert(&cache, &hdr, buf, len, now + 999999ull,
                                       &entry) == GEN_INSERT_DUPLICATE);
        EXPECT(entry->slots[0].datagram_copy[WIRE_HEADER_SIZE] == 0x11);
        EXPECT(entry->last_update_ns == lu);
        EXPECT(entry->present_count == pc);
        EXPECT(entry->bytes == bytes);
        EXPECT(entry->min_ttl == min_ttl);
    }
    st = generation_cache_stats(&cache);
    EXPECT(st->gen_duplicate == 1);

    /* Metadata mismatch: shard_count differs. */
    len = make_data_datagram(buf, sizeof(buf), 1, 5, 1, 3, 4, 7, 10, 16, 0x33);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, now, NULL) ==
           GEN_INSERT_MISMATCH);
    EXPECT(generation_cache_stats(&cache)->gen_metadata_mismatch >= 1);
    EXPECT(generation_cache_slot_present(entry, 1) == 0);

    generation_cache_destroy(&cache);
}

static void test_timeout(void)
{
    GenerationCache cache;
    GenerationCacheConfig cfg;
    uint8_t buf[WIRE_HEADER_SIZE + 8];
    size_t len;
    WireHeader hdr;
    uint64_t t0 = 1000000000ull;

    generation_cache_config_defaults(&cfg);
    cfg.gen_timeout_ms = 50;
    EXPECT(generation_cache_init(&cache, &cfg) == 0);

    len = make_data_datagram(buf, sizeof(buf), 1, 1, 0, 2, 4, 7, 8, 8, 9);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, t0, NULL) ==
           GEN_INSERT_OK);
    EXPECT(generation_cache_count(&cache) == 1);

    EXPECT(generation_cache_expire(&cache, t0 + 49ull * 1000000ull, -1) == 0);
    EXPECT(generation_cache_count(&cache) == 1);
    EXPECT(generation_cache_expire(&cache, t0 + 51ull * 1000000ull, -1) == 1);
    EXPECT(generation_cache_count(&cache) == 0);
    EXPECT(generation_cache_stats(&cache)->gen_timeout == 1);
    generation_cache_destroy(&cache);
}

static void test_eviction_lru(void)
{
    GenerationCache cache;
    GenerationCacheConfig cfg;
    uint8_t buf[WIRE_HEADER_SIZE + 8];
    size_t len;
    WireHeader hdr;
    uint64_t now = 5000000000ull;

    generation_cache_config_defaults(&cfg);
    cfg.max_gens_global = 1;
    cfg.max_gens_per_flow = 8;
    EXPECT(generation_cache_init(&cache, &cfg) == 0);

    len = make_data_datagram(buf, sizeof(buf), 1, 1, 0, 2, 4, 7, 8, 8, 1);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, now, NULL) ==
           GEN_INSERT_OK);

    now += 1000;
    len = make_data_datagram(buf, sizeof(buf), 1, 2, 0, 2, 4, 7, 8, 8, 2);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, now, NULL) ==
           GEN_INSERT_OK);

    EXPECT(generation_cache_count(&cache) == 1);
    EXPECT(generation_cache_find(&cache, 1, 1) == NULL);
    EXPECT(generation_cache_find(&cache, 1, 2) != NULL);
    EXPECT(generation_cache_stats(&cache)->gen_evicted >= 1);
    generation_cache_destroy(&cache);
}

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    int             hold;
    int             released;
    uint8_t         packets[8][RELAY_MAX_DATAGRAM];
    size_t          lens[8];
    uint8_t         types[8];
    int             count;
} TxCapture;

typedef struct {
    TxCapture *cap;
    int delay_ms;
} CaptureReleaseArgs;

static void *release_capture_after_delay(void *arg)
{
    CaptureReleaseArgs *ra = arg;

    usleep((useconds_t)ra->delay_ms * 1000u);
    pthread_mutex_lock(&ra->cap->mu);
    ra->cap->released = 1;
    pthread_cond_broadcast(&ra->cap->cv);
    pthread_mutex_unlock(&ra->cap->mu);
    return NULL;
}

static void tx_capture_cb(const uint8_t *datagram, size_t len, void *arg)
{
    TxCapture *cap = arg;
    WireHeader hdr;

    pthread_mutex_lock(&cap->mu);
    while (cap->hold && !cap->released) {
        pthread_cond_wait(&cap->cv, &cap->mu);
    }
    if (cap->count < 8 && len <= RELAY_MAX_DATAGRAM) {
        memcpy(cap->packets[cap->count], datagram, len);
        cap->lens[cap->count] = len;
        if (wire_header_decode(&hdr, datagram, len) == 0) {
            cap->types[cap->count] = hdr.type;
        }
        cap->count++;
    }
    pthread_mutex_unlock(&cap->mu);
}

static RelayConfig harness_cfg(RelayProcessMode mode, size_t egress_cap)
{
    RelayConfig cfg;

    memset(&cfg, 0, sizeof(cfg));
    cfg.local_node_id = 2;
    cfg.process_mode = mode;
    cfg.egress_capacity = egress_cap;
    cfg.egress_wait_ms = 2;
    cfg.reject_local_encoder_loopback = 1;
    cfg.gen_timeout_ms = 500;
    cfg.max_gens_global = 32;
    cfg.max_gens_per_flow = 8;
    cfg.max_cache_bytes = 1024 * 1024;
    return cfg;
}

static void wait_forwarded(const RelayCtx *ctx, uint64_t want, int ms)
{
    int i;
    for (i = 0; i < ms; i++) {
        const RelayFlowStats *st = relay_total_stats(ctx);
        if (st != NULL && st->forward >= want) {
            return;
        }
        usleep(1000);
    }
}

static void test_opaque_forward_duplicate_mismatch(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg = harness_cfg(RELAY_PROCESS_CACHE, 64);
    uint8_t buf[WIRE_HEADER_SIZE + 16];
    size_t len;
    const GenerationCacheStats *gst;
    GenerationEntry *entry;

    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);
    pthread_cond_init(&cap.cv, NULL);

    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);

    len = make_data_datagram(buf, sizeof(buf), 1, 9, 0, 2, 4, 8, 10, 16, 0xAA);
    EXPECT(relay_inject_wire_datagram(ctx, buf, len) == RELAY_INGRESS_OK);
    len = make_data_datagram(buf, sizeof(buf), 1, 9, 0, 2, 4, 8, 10, 16, 0xBB);
    EXPECT(relay_inject_wire_datagram(ctx, buf, len) == RELAY_INGRESS_OK);
    /* mismatch shard_count — still forward */
    len = make_data_datagram(buf, sizeof(buf), 1, 9, 1, 4, 4, 8, 10, 16, 0xCC);
    EXPECT(relay_inject_wire_datagram(ctx, buf, len) == RELAY_INGRESS_OK);

    wait_forwarded(ctx, 3, 500);
    EXPECT(relay_total_stats(ctx)->forward == 3);
    gst = relay_cache_stats(ctx);
    EXPECT(gst != NULL);
    EXPECT(gst->gen_duplicate >= 1);
    EXPECT(gst->gen_metadata_mismatch >= 1);

    entry = generation_cache_find(relay_generation_cache(ctx), 1, 9);
    EXPECT(entry != NULL);
    EXPECT(entry->slots[0].datagram_copy[WIRE_HEADER_SIZE] == 0xAA);
    /* TTL decremented in cache copy */
    {
        WireHeader ch;
        EXPECT(wire_header_decode(&ch, entry->slots[0].datagram_copy,
                                  entry->slots[0].len) == 0);
        EXPECT(ch.ttl == 7);
    }

    relay_harness_close(ctx);
    pthread_mutex_destroy(&cap.mu);
    pthread_cond_destroy(&cap.cv);
}

static void test_end_ordering(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg = harness_cfg(RELAY_PROCESS_CACHE, 64);
    uint8_t buf[WIRE_HEADER_SIZE + 16];
    size_t len;
    int i;

    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);
    pthread_cond_init(&cap.cv, NULL);

    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);

    for (i = 0; i < 3; i++) {
        len = make_data_datagram(buf, sizeof(buf), 2, 100, (uint16_t)i, 3, 4, 8,
                                 10, 16, (uint8_t)(0x30 + i));
        EXPECT(relay_inject_wire_datagram(ctx, buf, len) == RELAY_INGRESS_OK);
    }
    len = make_end_datagram(buf, sizeof(buf), 2, 4, 8);
    EXPECT(relay_inject_wire_datagram(ctx, buf, len) == RELAY_INGRESS_OK);

    wait_forwarded(ctx, 4, 500);
    EXPECT(cap.count == 4);
    EXPECT(cap.types[0] == WIRE_TYPE_DATA);
    EXPECT(cap.types[1] == WIRE_TYPE_DATA);
    EXPECT(cap.types[2] == WIRE_TYPE_DATA);
    EXPECT(cap.types[3] == WIRE_TYPE_END);
    /* END must not create a generation cache entry */
    EXPECT(generation_cache_find(relay_generation_cache(ctx), 2, 0) == NULL);

    relay_harness_close(ctx);
    pthread_mutex_destroy(&cap.mu);
    pthread_cond_destroy(&cap.cv);
}

static void test_queue_full_cache_survives(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg = harness_cfg(RELAY_PROCESS_CACHE, 2);
    uint8_t buf[WIRE_HEADER_SIZE + 8];
    size_t len;
    int i;
    const RelayFlowStats *st;
    GenerationEntry *entry;

    cfg.egress_wait_ms = 0; /* try-drop baseline */

    memset(&cap, 0, sizeof(cap));
    cap.hold = 1;
    pthread_mutex_init(&cap.mu, NULL);
    pthread_cond_init(&cap.cv, NULL);

    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);

    /* First packet dequeued into blocked TX; second fills queue; third drops. */
    for (i = 0; i < 4; i++) {
        len = make_data_datagram(buf, sizeof(buf), 1, (uint64_t)(50 + i), 0, 1,
                                 4, 8, 8, 8, (uint8_t)i);
        (void)relay_inject_wire_datagram(ctx, buf, len);
        usleep(2000);
    }

    st = relay_total_stats(ctx);
    EXPECT(st->drop_egress_full >= 1);

    /* Cache copies for successfully cached packets remain. */
    entry = generation_cache_find(relay_generation_cache(ctx), 1, 50);
    EXPECT(entry != NULL);
    EXPECT(generation_cache_slot_present(entry, 0) == 1);

    pthread_mutex_lock(&cap.mu);
    cap.released = 1;
    pthread_cond_broadcast(&cap.cv);
    pthread_mutex_unlock(&cap.mu);

    wait_forwarded(ctx, 1, 500);
    relay_harness_close(ctx);
    pthread_mutex_destroy(&cap.mu);
    pthread_cond_destroy(&cap.cv);
}

static void run_queue_pressure(uint32_t egress_wait_ms, int release_after_ms,
                               int packets, uint64_t *forward,
                               uint64_t *drop_full, uint64_t *drop_timeout)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg = harness_cfg(RELAY_PROCESS_CACHE, 2);
    uint8_t buf[WIRE_HEADER_SIZE + 8];
    size_t len;
    int i;
    const RelayFlowStats *st;
    pthread_t release_thread;
    CaptureReleaseArgs release_args;
    int release_thread_started = 0;

    cfg.egress_wait_ms = egress_wait_ms;
    memset(&cap, 0, sizeof(cap));
    cap.hold = 1;
    pthread_mutex_init(&cap.mu, NULL);
    pthread_cond_init(&cap.cv, NULL);

    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);

    if (release_after_ms > 0) {
        release_args.cap = &cap;
        release_args.delay_ms = release_after_ms;
        EXPECT(pthread_create(&release_thread, NULL, release_capture_after_delay,
                              &release_args) == 0);
        if (ctx != NULL) {
            release_thread_started = 1;
        }
    }

    for (i = 0; i < packets; i++) {
        len = make_data_datagram(buf, sizeof(buf), 1, (uint64_t)(200 + i), 0, 1,
                                 4, 8, 8, 8, (uint8_t)i);
        (void)relay_inject_wire_datagram(ctx, buf, len);
        usleep(2000);
    }

    st = relay_total_stats(ctx);
    *forward = st->forward;
    *drop_full = st->drop_egress_full;
    *drop_timeout = st->drop_egress_timeout;
    if (!cap.released) {
        pthread_mutex_lock(&cap.mu);
        cap.released = 1;
        pthread_cond_broadcast(&cap.cv);
        pthread_mutex_unlock(&cap.mu);
    }
    if (release_thread_started) {
        EXPECT(pthread_join(release_thread, NULL) == 0);
    }
    wait_forwarded(ctx, st->forward, 500);
    relay_harness_close(ctx);
    pthread_mutex_destroy(&cap.mu);
    pthread_cond_destroy(&cap.cv);
}

static void test_timed_backpressure_vs_try_drop(void)
{
    uint64_t try_forward = 0;
    uint64_t try_full = 0;
    uint64_t try_timeout = 0;
    uint64_t timed_forward = 0;
    uint64_t timed_full = 0;
    uint64_t timed_timeout = 0;
    uint64_t try_forward_recover = 0;
    uint64_t try_full_recover = 0;
    uint64_t try_timeout_recover = 0;
    uint64_t timed_forward_recover = 0;
    uint64_t timed_full_recover = 0;
    uint64_t timed_timeout_recover = 0;

    /*
     * Scenario A: hold TX longer than timed wait. Validates timeout drop path.
     */
    run_queue_pressure(0, 30, 4, &try_forward, &try_full, &try_timeout);
    run_queue_pressure(20, 30, 4, &timed_forward, &timed_full, &timed_timeout);

    EXPECT(try_full >= 1);
    EXPECT(try_timeout == 0);
    EXPECT(timed_full == 0);
    EXPECT(timed_timeout >= 1);
    EXPECT(try_forward >= timed_forward);

    /*
     * Scenario B: release TX before timeout. Timed path should forward more
     * packets than try-drop baseline, with fewer drops.
     */
    run_queue_pressure(0, 10, 6, &try_forward_recover, &try_full_recover,
                       &try_timeout_recover);
    run_queue_pressure(20, 10, 6, &timed_forward_recover, &timed_full_recover,
                       &timed_timeout_recover);

    EXPECT(try_timeout_recover == 0);
    EXPECT(timed_timeout_recover == 0);
    EXPECT(timed_full_recover == 0);
    EXPECT(timed_forward_recover > try_forward_recover);
    EXPECT(timed_full_recover < try_full_recover);
}

static void test_admission_still_forwards(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg = harness_cfg(RELAY_PROCESS_CACHE, 64);
    uint8_t buf[WIRE_HEADER_SIZE + 64];
    size_t len;
    WireHeader hdr;
    const GenerationCacheStats *gst;

    /* Tiny byte budget: first packet caches; second cannot grow/create. */
    cfg.max_cache_bytes = WIRE_HEADER_SIZE + 8;
    cfg.max_gens_global = 8;
    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);
    pthread_cond_init(&cap.cv, NULL);

    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);

    len = make_data_datagram(buf, sizeof(buf), 1, 1, 0, 2, 4, 8, 8, 8, 1);
    EXPECT(relay_inject_wire_datagram(ctx, buf, len) == RELAY_INGRESS_OK);

    len = make_data_datagram(buf, sizeof(buf), 1, 2, 0, 2, 4, 8, 8, 8, 2);
    EXPECT(relay_inject_wire_datagram(ctx, buf, len) == RELAY_INGRESS_OK);

    wait_forwarded(ctx, 2, 500);
    EXPECT(relay_total_stats(ctx)->forward == 2);
    gst = relay_cache_stats(ctx);
    EXPECT(gst != NULL);
    /* Either eviction kept one gen, or admission_failed on second. */
    EXPECT(gst->gen_admission_failed + gst->gen_evicted >= 1);

    (void)hdr;
    relay_harness_close(ctx);
    pthread_mutex_destroy(&cap.mu);
    pthread_cond_destroy(&cap.cv);
}

static void test_duplicate_does_not_refresh_timeout(void)
{
    GenerationCache cache;
    GenerationCacheConfig cfg;
    uint8_t buf[WIRE_HEADER_SIZE + 8];
    size_t len;
    WireHeader hdr;
    GenerationEntry *entry;
    uint64_t t0 = 10ull * 1000000000ull;
    uint64_t lu;

    generation_cache_config_defaults(&cfg);
    cfg.gen_timeout_ms = 100;
    EXPECT(generation_cache_init(&cache, &cfg) == 0);

    len = make_data_datagram(buf, sizeof(buf), 1, 7, 0, 2, 4, 7, 8, 8, 1);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, t0, &entry) ==
           GEN_INSERT_OK);
    lu = entry->last_update_ns;

    /* Near timeout: duplicate must not extend deadline. */
    len = make_data_datagram(buf, sizeof(buf), 1, 7, 0, 2, 4, 7, 8, 8, 2);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len,
                                   t0 + 90ull * 1000000ull,
                                   &entry) == GEN_INSERT_DUPLICATE);
    EXPECT(entry->last_update_ns == lu);
    EXPECT(generation_cache_expire(&cache, t0 + 100ull * 1000000ull, -1) == 1);
    EXPECT(generation_cache_count(&cache) == 0);
    EXPECT(generation_cache_stats(&cache)->gen_timeout == 1);
    generation_cache_destroy(&cache);
}

static void test_duplicate_does_not_change_lru(void)
{
    GenerationCache cache;
    GenerationCacheConfig cfg;
    uint8_t buf[WIRE_HEADER_SIZE + 8];
    size_t len;
    WireHeader hdr;
    uint64_t now = 20ull * 1000000000ull;

    generation_cache_config_defaults(&cfg);
    cfg.max_gens_global = 2;
    cfg.max_gens_per_flow = 8;
    EXPECT(generation_cache_init(&cache, &cfg) == 0);

    /* A then B => A is LRU. */
    len = make_data_datagram(buf, sizeof(buf), 1, 1, 0, 2, 4, 7, 8, 8, 1);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, now, NULL) ==
           GEN_INSERT_OK);
    now += 1000;
    len = make_data_datagram(buf, sizeof(buf), 1, 2, 0, 2, 4, 7, 8, 8, 2);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, now, NULL) ==
           GEN_INSERT_OK);

    /* Duplicate on A must not promote A; C should evict A. */
    now += 1000;
    len = make_data_datagram(buf, sizeof(buf), 1, 1, 0, 2, 4, 7, 8, 8, 9);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, now, NULL) ==
           GEN_INSERT_DUPLICATE);

    now += 1000;
    len = make_data_datagram(buf, sizeof(buf), 1, 3, 0, 2, 4, 7, 8, 8, 3);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, now, NULL) ==
           GEN_INSERT_OK);

    EXPECT(generation_cache_find(&cache, 1, 1) == NULL);
    EXPECT(generation_cache_find(&cache, 1, 2) != NULL);
    EXPECT(generation_cache_find(&cache, 1, 3) != NULL);
    EXPECT(generation_cache_stats(&cache)->gen_evicted >= 1);
    generation_cache_destroy(&cache);
}

static void test_poll_timeout_accuracy(void)
{
    GenerationCache cache;
    GenerationCacheConfig cfg;
    uint8_t buf[WIRE_HEADER_SIZE + 8];
    size_t len;
    WireHeader hdr;
    uint64_t t0 = 30ull * 1000000000ull;
    int poll_ms;

    generation_cache_config_defaults(&cfg);
    cfg.gen_timeout_ms = 50;
    EXPECT(generation_cache_init(&cache, &cfg) == 0);

    EXPECT(generation_cache_poll_timeout_ms(&cache, t0, 1000) == 1000);

    len = make_data_datagram(buf, sizeof(buf), 1, 1, 0, 2, 4, 7, 8, 8, 1);
    EXPECT(wire_header_decode(&hdr, buf, len) == 0);
    EXPECT(generation_cache_insert(&cache, &hdr, buf, len, t0, NULL) ==
           GEN_INSERT_OK);

    poll_ms = generation_cache_poll_timeout_ms(&cache, t0, 1000);
    EXPECT(poll_ms >= 1);
    EXPECT(poll_ms <= 50);
    EXPECT(poll_ms != 1000);

    poll_ms = generation_cache_poll_timeout_ms(&cache, t0 + 40ull * 1000000ull,
                                               1000);
    EXPECT(poll_ms >= 1);
    EXPECT(poll_ms <= 10);

    poll_ms = generation_cache_poll_timeout_ms(&cache, t0 + 50ull * 1000000ull,
                                               1000);
    EXPECT(poll_ms == 0);

    generation_cache_destroy(&cache);
}

typedef struct {
    RelayCtx       *ctx;
    const uint8_t  *datagram;
    size_t          len;
    int             loops;
} InjectWorkerArgs;

static void *inject_worker_main(void *arg)
{
    InjectWorkerArgs *wa = arg;
    int i;

    for (i = 0; i < wa->loops; i++) {
        RelayIngressStatus st =
            relay_inject_wire_datagram(wa->ctx, wa->datagram, wa->len);
        if (st == RELAY_INGRESS_ERR_SHUTDOWN) {
            break;
        }
    }
    return NULL;
}

static volatile int g_hold_process;
static volatile int g_process_entered;

static RelayProcessAction hold_process_fn(const WireHeader *hdr,
                                          const uint8_t *datagram, size_t len,
                                          GenerationEntry *gen,
                                          GenerationInsertStatus insert_status,
                                          void *ctx)
{
    (void)hdr;
    (void)datagram;
    (void)len;
    (void)gen;
    (void)insert_status;
    (void)ctx;
    g_process_entered = 1;
    while (!g_hold_process) {
        usleep(200);
    }
    return RELAY_PROCESS_CONTINUE_FORWARD;
}

static void test_harness_close_inject_safety(void)
{
    const int rounds = 40;
    const int nthreads = 4;
    int r;
    uint8_t buf[WIRE_HEADER_SIZE + 8];
    size_t len;
    pthread_t blocker;
    InjectWorkerArgs blocker_args;
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg;

    len = make_data_datagram(buf, sizeof(buf), 1, 1, 0, 1, 4, 8, 8, 8, 0x5A);

    /*
     * Contract: join all injectors, then close. Concurrent inject vs close is
     * not supported; this loop only verifies the join-before-close protocol.
     */
    for (r = 0; r < rounds; r++) {
        TxCapture cap_round;
        RelayConfig cfg_round = harness_cfg(RELAY_PROCESS_CACHE, 64);
        pthread_t th[4];
        InjectWorkerArgs args[4];
        int t;

        memset(&cap_round, 0, sizeof(cap_round));
        pthread_mutex_init(&cap_round.mu, NULL);
        pthread_cond_init(&cap_round.cv, NULL);
        EXPECT(relay_harness_open(&ctx, &cfg_round, tx_capture_cb, &cap_round) ==
               RELAY_OK);

        for (t = 0; t < nthreads; t++) {
            args[t].ctx = ctx;
            args[t].datagram = buf;
            args[t].len = len;
            args[t].loops = 40;
            EXPECT(pthread_create(&th[t], NULL, inject_worker_main, &args[t]) ==
                   0);
        }
        for (t = 0; t < nthreads; t++) {
            EXPECT(pthread_join(th[t], NULL) == 0);
        }
        relay_harness_close(ctx);
        ctx = NULL;
        pthread_mutex_destroy(&cap_round.mu);
        pthread_cond_destroy(&cap_round.cv);
    }

    /*
     * Already-entered ingress must finish before close: hold process_fn, then
     * release, join injector (in_flight -> 0), then close (assert idle).
     */
    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);
    pthread_cond_init(&cap.cv, NULL);
    cfg = harness_cfg(RELAY_PROCESS_CACHE, 64);
    cfg.process_fn = hold_process_fn;
    g_hold_process = 0;
    g_process_entered = 0;
    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);

    blocker_args.ctx = ctx;
    blocker_args.datagram = buf;
    blocker_args.len = len;
    blocker_args.loops = 1;
    EXPECT(pthread_create(&blocker, NULL, inject_worker_main, &blocker_args) ==
           0);
    while (!g_process_entered) {
        usleep(200);
    }
    g_hold_process = 1;
    EXPECT(pthread_join(blocker, NULL) == 0);
    relay_harness_close(ctx);
    ctx = NULL;
    pthread_mutex_destroy(&cap.mu);
    pthread_cond_destroy(&cap.cv);
}

static int insert_one(GenerationCache *cache, uint32_t flow_id, uint64_t block_id,
                      uint64_t now)
{
    uint8_t buf[WIRE_HEADER_SIZE + 8];
    size_t len;
    WireHeader hdr;

    len = make_data_datagram(buf, sizeof(buf), flow_id, block_id, 0, 1, 4, 7, 8,
                             8, 0x11);
    if (wire_header_decode(&hdr, buf, len) != 0) {
        return -1;
    }
    return (int)generation_cache_insert(cache, &hdr, buf, len, now, NULL);
}

static void test_cache_high_flow_ids_have_independent_per_flow_limits(void)
{
    GenerationCache cache;
    GenerationCacheConfig cfg;
    uint64_t now = 1000000000ull;

    generation_cache_config_defaults(&cfg);
    cfg.max_gens_per_flow = 2;
    cfg.max_gens_global = 64;
    cfg.max_cache_bytes = 1024 * 1024;
    EXPECT(generation_cache_init(&cache, &cfg) == 0);

    EXPECT(insert_one(&cache, 8, 0, now) == GEN_INSERT_OK);
    EXPECT(insert_one(&cache, 8, 1, now) == GEN_INSERT_OK);
    EXPECT(insert_one(&cache, 9, 0, now) == GEN_INSERT_OK);
    EXPECT(insert_one(&cache, 9, 1, now) == GEN_INSERT_OK);

    EXPECT(generation_cache_count_flow(&cache, 8) == 2);
    EXPECT(generation_cache_count_flow(&cache, 9) == 2);
    EXPECT(generation_cache_count(&cache) == 4);
    EXPECT(generation_cache_find(&cache, 8, 0) != NULL);
    EXPECT(generation_cache_find(&cache, 8, 1) != NULL);
    EXPECT(generation_cache_find(&cache, 9, 0) != NULL);
    EXPECT(generation_cache_find(&cache, 9, 1) != NULL);
    EXPECT(generation_cache_stats(&cache)->gen_evicted == 0);
    EXPECT(generation_cache_stats(&cache)->gen_admission_failed == 0);

    generation_cache_destroy(&cache);
}

static void test_cache_high_flow_id_evicts_only_own_generation(void)
{
    GenerationCache cache;
    GenerationCacheConfig cfg;
    uint64_t now = 2000000000ull;

    generation_cache_config_defaults(&cfg);
    cfg.max_gens_per_flow = 2;
    cfg.max_gens_global = 64;
    cfg.max_cache_bytes = 1024 * 1024;
    EXPECT(generation_cache_init(&cache, &cfg) == 0);

    EXPECT(insert_one(&cache, 8, 0, now) == GEN_INSERT_OK);
    EXPECT(insert_one(&cache, 8, 1, now + 1) == GEN_INSERT_OK);
    EXPECT(insert_one(&cache, 9, 0, now + 2) == GEN_INSERT_OK);
    EXPECT(insert_one(&cache, 9, 1, now + 3) == GEN_INSERT_OK);
    EXPECT(insert_one(&cache, 8, 2, now + 4) == GEN_INSERT_OK);

    EXPECT(generation_cache_count_flow(&cache, 8) == 2);
    EXPECT(generation_cache_count_flow(&cache, 9) == 2);
    EXPECT(generation_cache_find(&cache, 8, 0) == NULL); /* same-flow LRU */
    EXPECT(generation_cache_find(&cache, 8, 1) != NULL);
    EXPECT(generation_cache_find(&cache, 8, 2) != NULL);
    EXPECT(generation_cache_find(&cache, 9, 0) != NULL);
    EXPECT(generation_cache_find(&cache, 9, 1) != NULL);
    EXPECT(generation_cache_stats(&cache)->gen_evicted >= 1);

    generation_cache_destroy(&cache);
}

static void test_cache_global_eviction_updates_exact_flow_account(void)
{
    GenerationCache cache;
    GenerationCacheConfig cfg;
    uint64_t now = 3000000000ull;

    generation_cache_config_defaults(&cfg);
    cfg.max_gens_global = 2;
    cfg.max_gens_per_flow = 32;
    cfg.max_cache_bytes = 1024 * 1024;
    EXPECT(generation_cache_init(&cache, &cfg) == 0);

    EXPECT(insert_one(&cache, 8, 0, now) == GEN_INSERT_OK);
    EXPECT(insert_one(&cache, 9, 0, now + 1) == GEN_INSERT_OK);
    EXPECT(insert_one(&cache, 10, 0, now + 2) == GEN_INSERT_OK);

    EXPECT(generation_cache_count(&cache) == 2);
    EXPECT(generation_cache_count_flow(&cache, 8) == 0);
    EXPECT(generation_cache_find(&cache, 8, 0) == NULL);
    EXPECT(generation_cache_count_flow(&cache, 9) == 1);
    EXPECT(generation_cache_count_flow(&cache, 10) == 1);

    /* Freed account for flow 8 can be reused by a new flow. */
    EXPECT(insert_one(&cache, 11, 0, now + 3) == GEN_INSERT_OK);
    EXPECT(generation_cache_count_flow(&cache, 11) == 1);
    EXPECT(generation_cache_count(&cache) == 2);

    generation_cache_destroy(&cache);
}

static void test_cache_flow_id_zero_and_uint32_max_are_distinct(void)
{
    GenerationCache cache;
    GenerationCacheConfig cfg;
    uint64_t now = 4000000000ull;

    generation_cache_config_defaults(&cfg);
    EXPECT(generation_cache_init(&cache, &cfg) == 0);

    EXPECT(insert_one(&cache, 0, 0, now) == GEN_INSERT_OK);
    EXPECT(insert_one(&cache, UINT32_MAX, 0, now + 1) == GEN_INSERT_OK);

    EXPECT(generation_cache_count_flow(&cache, 0) == 1);
    EXPECT(generation_cache_count_flow(&cache, UINT32_MAX) == 1);
    EXPECT(generation_cache_count(&cache) == 2);
    EXPECT(generation_cache_find(&cache, 0, 0) != NULL);
    EXPECT(generation_cache_find(&cache, UINT32_MAX, 0) != NULL);

    generation_cache_destroy(&cache);
}

static int insert_payload(GenerationCache *cache, uint32_t flow_id,
                          uint64_t block_id, uint16_t payload_len, uint64_t now)
{
    uint8_t buf[WIRE_HEADER_SIZE + 512];
    size_t len;
    WireHeader hdr;

    if (payload_len > 512) {
        return -1;
    }
    len = make_data_datagram(buf, sizeof(buf), flow_id, block_id, 0, 1, 4, 7,
                             payload_len, payload_len, 0x5A);
    if (len == 0 || wire_header_decode(&hdr, buf, len) != 0) {
        return -1;
    }
    return (int)generation_cache_insert(cache, &hdr, buf, len, now, NULL);
}

static size_t live_flow_entries(GenerationCache *cache, uint32_t flow_id,
                                uint64_t max_block_inclusive)
{
    size_t n = 0;
    uint64_t b;

    for (b = 0; b <= max_block_inclusive; b++) {
        if (generation_cache_find(cache, flow_id, b) != NULL) {
            n++;
        }
    }
    return n;
}

/*
 * Multi-round make_room: consecutive admissions that each evict, plus one
 * admission that requires multiple byte-limit victims in a single make_room.
 * Asserts exact flow accounts stay aligned with live entries (flows 8/9/10).
 */
static void test_cache_repeated_evictions_keep_accounts_consistent(void)
{
    GenerationCache cache;
    GenerationCacheConfig cfg;
    uint64_t now = 5000000000ull;
    uint64_t evicted_before;
    uint64_t i;

    generation_cache_config_defaults(&cfg);
    cfg.max_gens_global = 4;
    cfg.max_gens_per_flow = 2;
    cfg.max_cache_bytes = 1024 * 1024;
    EXPECT(generation_cache_init(&cache, &cfg) == 0);

    EXPECT(insert_one(&cache, 8, 0, now++) == GEN_INSERT_OK);
    EXPECT(insert_one(&cache, 8, 1, now++) == GEN_INSERT_OK);
    EXPECT(insert_one(&cache, 9, 0, now++) == GEN_INSERT_OK);
    EXPECT(insert_one(&cache, 9, 1, now++) == GEN_INSERT_OK);
    EXPECT(generation_cache_count(&cache) == 4);
    EXPECT(generation_cache_count_flow(&cache, 8) == 2);
    EXPECT(generation_cache_count_flow(&cache, 9) == 2);
    EXPECT(generation_cache_count_flow(&cache, 10) == 0);

    evicted_before = generation_cache_stats(&cache)->gen_evicted;

    for (i = 0; i < 6; i++) {
        EXPECT(insert_one(&cache, 10, i, now++) == GEN_INSERT_OK);
        EXPECT(generation_cache_count(&cache) == 4);
        EXPECT(generation_cache_count_flow(&cache, 8) +
                   generation_cache_count_flow(&cache, 9) +
                   generation_cache_count_flow(&cache, 10) ==
               4);
        EXPECT(generation_cache_count_flow(&cache, 10) <= 2);
        EXPECT(generation_cache_count_flow(&cache, 10) ==
               live_flow_entries(&cache, 10, i));
        EXPECT(generation_cache_count_flow(&cache, 8) ==
               live_flow_entries(&cache, 8, 1));
        EXPECT(generation_cache_count_flow(&cache, 9) ==
               live_flow_entries(&cache, 9, 1));
    }
    EXPECT(generation_cache_stats(&cache)->gen_evicted >= evicted_before + 6);
    EXPECT(generation_cache_count_flow(&cache, 10) == 2);

    generation_cache_destroy(&cache);

    /* One insert → multiple make_room iterations (byte budget). */
    generation_cache_config_defaults(&cfg);
    cfg.max_gens_global = 16;
    cfg.max_gens_per_flow = 16;
    cfg.max_cache_bytes = 200;
    EXPECT(generation_cache_init(&cache, &cfg) == 0);

    /* len = 44+36 = 80 each → two gens use 160. */
    EXPECT(insert_payload(&cache, 8, 0, 36, now++) == GEN_INSERT_OK);
    EXPECT(insert_payload(&cache, 9, 0, 36, now++) == GEN_INSERT_OK);
    EXPECT(generation_cache_count(&cache) == 2);
    EXPECT(generation_cache_count_flow(&cache, 8) == 1);
    EXPECT(generation_cache_count_flow(&cache, 9) == 1);

    evicted_before = generation_cache_stats(&cache)->gen_evicted;
    /* len = 44+106 = 150; 160+150 > 200 → need two 80-byte victims. */
    EXPECT(insert_payload(&cache, 10, 0, 106, now++) == GEN_INSERT_OK);
    EXPECT(generation_cache_stats(&cache)->gen_evicted >= evicted_before + 2);
    EXPECT(generation_cache_count(&cache) == 1);
    EXPECT(generation_cache_count_flow(&cache, 8) == 0);
    EXPECT(generation_cache_count_flow(&cache, 9) == 0);
    EXPECT(generation_cache_count_flow(&cache, 10) == 1);
    EXPECT(generation_cache_find(&cache, 10, 0) != NULL);
    EXPECT(generation_cache_count_flow(&cache, 8) +
               generation_cache_count_flow(&cache, 9) +
               generation_cache_count_flow(&cache, 10) ==
           generation_cache_count(&cache));

    generation_cache_destroy(&cache);
}

static int noop_local_delivery(const uint8_t *datagram, size_t len,
                               const WireHeader *hdr, void *arg)
{
    (void)datagram;
    (void)len;
    (void)hdr;
    (void)arg;
    return 0;
}

static void test_local_packets_still_bypass_generation_cache(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg = harness_cfg(RELAY_PROCESS_CACHE, 64);
    uint8_t buf[WIRE_HEADER_SIZE + 8];
    size_t len;

    /* reject_local_encoder_loopback=0 requires delivery_fn, else init forces 1. */
    cfg.local_node_id = 4;
    cfg.delivery_fn = noop_local_delivery;
    cfg.delivery_ctx = NULL;
    cfg.reject_local_encoder_loopback = 0;
    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);
    pthread_cond_init(&cap.cv, NULL);
    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);

    len = make_data_datagram(buf, sizeof(buf), 8, 1, 0, 1, 4, 8, 8, 8, 0x42);
    EXPECT(relay_inject_wire_datagram(ctx, buf, len) == RELAY_INGRESS_OK);
    EXPECT(relay_total_stats(ctx)->local_deliver >= 1);
    EXPECT(relay_total_stats(ctx)->forward == 0);
    EXPECT(cap.count == 0);
    EXPECT(generation_cache_count(relay_generation_cache(ctx)) == 0);
    EXPECT(relay_total_stats(ctx)->gen_created == 0);

    relay_harness_close(ctx);
    pthread_mutex_destroy(&cap.mu);
    pthread_cond_destroy(&cap.cv);
}

int main(void)
{
    test_key_isolation_flow();
    test_key_isolation_block();
    test_duplicate_and_mismatch();
    test_duplicate_does_not_refresh_timeout();
    test_duplicate_does_not_change_lru();
    test_timeout();
    test_poll_timeout_accuracy();
    test_eviction_lru();
    test_opaque_forward_duplicate_mismatch();
    test_end_ordering();
    test_queue_full_cache_survives();
    test_timed_backpressure_vs_try_drop();
    test_admission_still_forwards();
    test_harness_close_inject_safety();

    test_cache_high_flow_ids_have_independent_per_flow_limits();
    test_cache_high_flow_id_evicts_only_own_generation();
    test_cache_global_eviction_updates_exact_flow_account();
    test_cache_flow_id_zero_and_uint32_max_are_distinct();
    test_cache_repeated_evictions_keep_accounts_consistent();
    test_local_packets_still_bypass_generation_cache();

    if (g_failures != 0) {
        fprintf(stderr, "relay_gen_cache_tests: %d failure(s)\n", g_failures);
        return 1;
    }
    fprintf(stderr, "relay_gen_cache_tests: all passed\n");
    return 0;
}
