#include "local_decode.h"
#include "relay.h"
#include "stream_config.h"
#include "wire_header.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int g_failures = 0;

#define EXPECT(cond)                                                           \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);    \
            g_failures++;                                                      \
        }                                                                      \
    } while (0)

typedef struct TxCapture {
    pthread_mutex_t mu;
    size_t          count;
    uint8_t         packets[16][RELAY_MAX_DATAGRAM];
    size_t          lens[16];
    uint8_t         ttls[16];
} TxCapture;

typedef struct TtlProbe {
    LocalDecodeHub *hub;
    uint8_t         last_ttl;
    int             seen;
} TtlProbe;

static void tx_capture_cb(const uint8_t *datagram, size_t len, void *arg)
{
    TxCapture *cap = arg;
    WireHeader hdr;

    pthread_mutex_lock(&cap->mu);
    if (cap->count < 16 && len <= RELAY_MAX_DATAGRAM) {
        memcpy(cap->packets[cap->count], datagram, len);
        cap->lens[cap->count] = len;
        if (wire_header_decode(&hdr, datagram, len) == 0) {
            cap->ttls[cap->count] = hdr.ttl;
        }
        cap->count++;
    }
    pthread_mutex_unlock(&cap->mu);
}

static size_t tx_capture_count(TxCapture *cap)
{
    size_t count;

    pthread_mutex_lock(&cap->mu);
    count = cap->count;
    pthread_mutex_unlock(&cap->mu);
    return count;
}

static int ttl_probe_delivery(const uint8_t *datagram, size_t len,
                              const WireHeader *hdr, void *ctx)
{
    TtlProbe *probe = ctx;

    if (probe == NULL || hdr == NULL) {
        return -1;
    }
    probe->last_ttl = hdr->ttl;
    probe->seen++;
    return local_decode_hub_delivery(datagram, len, hdr, probe->hub);
}

static RelayConfig harness_cfg(uint8_t local_id, RelayProcessMode mode)
{
    RelayConfig cfg;

    memset(&cfg, 0, sizeof(cfg));
    cfg.local_node_id = local_id;
    cfg.process_mode = mode;
    cfg.egress_capacity = 64;
    cfg.reject_local_encoder_loopback = 1;
    cfg.gen_timeout_ms = 500;
    cfg.max_gens_global = 32;
    cfg.max_gens_per_flow = 8;
    cfg.max_cache_bytes = 1024 * 1024;
    return cfg;
}

static void wait_local(const RelayCtx *ctx, uint64_t want, int ms)
{
    int i;

    for (i = 0; i < ms; i++) {
        const RelayFlowStats *st = relay_total_stats(ctx);

        if (st != NULL && st->local_deliver >= want) {
            return;
        }
        usleep(1000);
    }
}

static void wait_forward(const RelayCtx *ctx, uint64_t want, int ms)
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

static int build_copy_block_datagrams(uint8_t datagrams[][RELAY_MAX_DATAGRAM],
                                      size_t *lens, uint16_t *out_shards,
                                      const uint8_t *plaintext, size_t plen,
                                      uint32_t flow_id, uint64_t block_id,
                                      uint8_t final_dst, uint8_t ttl)
{
    const Codec *codec = CopyCodec_get();
    unsigned char *block;
    size_t input_size;
    size_t output_size;
    uint16_t shard_count;
    uint16_t shard;
    uint16_t valid_len;

    if (codec == NULL || plaintext == NULL || plen == 0) {
        return -1;
    }
    input_size = Codec_input_block_size(codec);
    output_size = Codec_output_block_size(codec);
    if (plen > input_size || output_size % PKG_SIZE != 0) {
        return -1;
    }
    valid_len = (uint16_t)plen;
    shard_count = (uint16_t)(output_size / PKG_SIZE);
    block = calloc(1, output_size);
    if (block == NULL) {
        return -1;
    }
    memcpy(block, plaintext, plen);
    Codec_encode(codec, block, output_size);

    for (shard = 0; shard < shard_count; shard++) {
        WireHeader hdr;
        size_t dlen = WIRE_HEADER_SIZE + PKG_SIZE;

        memset(&hdr, 0, sizeof(hdr));
        hdr.type = WIRE_TYPE_DATA;
        hdr.final_dst = final_dst;
        hdr.ttl = ttl;
        hdr.flow_id = flow_id;
        hdr.block_id = block_id;
        hdr.shard_index = shard;
        hdr.shard_count = shard_count;
        hdr.valid_len = valid_len;
        hdr.payload_len = (uint16_t)PKG_SIZE;
        wire_header_encode(datagrams[shard], &hdr);
        memcpy(datagrams[shard] + WIRE_HEADER_SIZE,
               block + (size_t)shard * PKG_SIZE, PKG_SIZE);
        lens[shard] = dlen;
    }
    free(block);
    *out_shards = shard_count;
    return 0;
}

static size_t make_end(uint8_t *out, size_t cap, uint32_t flow_id,
                       uint64_t block_count, uint16_t shard_count,
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
    hdr.block_id = block_count;
    hdr.shard_index = 0;
    hdr.shard_count = shard_count;
    hdr.valid_len = 0;
    hdr.payload_len = 0;
    wire_header_encode(out, &hdr);
    return WIRE_HEADER_SIZE;
}

static int read_file(const char *path, uint8_t *buf, size_t cap, size_t *out_len)
{
    FILE *fp;
    size_t n;

    fp = fopen(path, "rb");
    if (fp == NULL) {
        return -1;
    }
    n = fread(buf, 1, cap, fp);
    fclose(fp);
    *out_len = n;
    return 0;
}

static void test_local_copy_end_matches_source(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg;
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    const char *out_path = "build/relay_ld_copy_out.bin";
    uint8_t plaintext[512];
    uint8_t datagrams[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t lens[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t shards = 0;
    uint8_t endbuf[WIRE_HEADER_SIZE];
    size_t end_len;
    uint8_t got[512];
    size_t got_len = 0;
    uint16_t i;
    const LocalDecodeHubStats *hst;

    for (i = 0; i < sizeof(plaintext); i++) {
        plaintext[i] = (uint8_t)(i * 17u + 3u);
    }
    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);
    unlink(out_path);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_SINGLE_FILE;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_path = out_path;
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);

    cfg = harness_cfg(4, RELAY_PROCESS_FORWARD);
    cfg.delivery_fn = local_decode_hub_delivery;
    cfg.delivery_ctx = &hub;
    cfg.reject_local_encoder_loopback = 0;

    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);
    EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, plaintext,
                                      sizeof(plaintext), 7, 0, 4, 8) == 0);
    for (i = 0; i < shards; i++) {
        EXPECT(relay_inject_wire_datagram(ctx, datagrams[i], lens[i]) ==
               RELAY_INGRESS_OK);
    }
    end_len = make_end(endbuf, sizeof(endbuf), 7, 1, shards, 4, 8);
    EXPECT(relay_inject_wire_datagram(ctx, endbuf, end_len) ==
           RELAY_INGRESS_OK);

    wait_local(ctx, (uint64_t)shards + 1u, 2000);
    EXPECT(local_decode_hub_is_complete(&hub));
    EXPECT(local_decode_hub_strict_check(&hub) == 0);
    EXPECT(tx_capture_count(&cap) == 0);
    EXPECT(relay_total_stats(ctx)->forward == 0);
    EXPECT(relay_total_stats(ctx)->local_deliver == (uint64_t)shards + 1u);

    EXPECT(read_file(out_path, got, sizeof(got), &got_len) == 0);
    EXPECT(got_len == sizeof(plaintext));
    EXPECT(memcmp(got, plaintext, sizeof(plaintext)) == 0);

    hst = local_decode_hub_stats(&hub);
    EXPECT(hst != NULL && hst->flow_rejected == 0 &&
           hst->metadata_mismatch == 0);

    relay_harness_close(ctx);
    local_decode_hub_destroy(&hub);
    pthread_mutex_destroy(&cap.mu);
    unlink(out_path);
}

static void test_nonlocal_still_tx_empty_output(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg;
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    const char *out_path = "build/relay_ld_nonlocal_out.bin";
    uint8_t plaintext[128];
    uint8_t datagrams[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t lens[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t shards = 0;
    uint8_t endbuf[WIRE_HEADER_SIZE];
    size_t end_len;
    uint8_t got[128];
    size_t got_len = 0;
    uint16_t i;

    memset(plaintext, 0x5a, sizeof(plaintext));
    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);
    unlink(out_path);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_SINGLE_FILE;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_path = out_path;
    hcfg.local_node_id = 2;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);

    /* local_node_id=2, packets destined to 4 → forward path */
    cfg = harness_cfg(2, RELAY_PROCESS_FORWARD);
    cfg.delivery_fn = local_decode_hub_delivery;
    cfg.delivery_ctx = &hub;
    cfg.reject_local_encoder_loopback = 0;

    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);
    EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, plaintext,
                                      sizeof(plaintext), 1, 0, 4, 8) == 0);
    for (i = 0; i < shards; i++) {
        EXPECT(relay_inject_wire_datagram(ctx, datagrams[i], lens[i]) ==
               RELAY_INGRESS_OK);
    }
    end_len = make_end(endbuf, sizeof(endbuf), 1, 1, shards, 4, 8);
    EXPECT(relay_inject_wire_datagram(ctx, endbuf, end_len) ==
           RELAY_INGRESS_OK);

    wait_forward(ctx, (uint64_t)shards + 1u, 2000);
    EXPECT(relay_total_stats(ctx)->forward == (uint64_t)shards + 1u);
    EXPECT(relay_total_stats(ctx)->local_deliver == 0);
    EXPECT(tx_capture_count(&cap) == (size_t)shards + 1u);
    EXPECT(!local_decode_hub_is_complete(&hub));
    EXPECT(read_file(out_path, got, sizeof(got), &got_len) == 0);
    EXPECT(got_len == 0);

    relay_harness_close(ctx);
    local_decode_hub_destroy(&hub);
    pthread_mutex_destroy(&cap.mu);
    unlink(out_path);
}

static void test_local_ttl_unchanged(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg;
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    TtlProbe probe;
    const char *out_path = "build/relay_ld_ttl_out.bin";
    uint8_t plaintext[64];
    uint8_t datagrams[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t lens[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t shards = 0;
    uint16_t i;

    memset(plaintext, 0x11, sizeof(plaintext));
    memset(&cap, 0, sizeof(cap));
    memset(&probe, 0, sizeof(probe));
    pthread_mutex_init(&cap.mu, NULL);
    unlink(out_path);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_SINGLE_FILE;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_path = out_path;
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);
    probe.hub = &hub;

    cfg = harness_cfg(4, RELAY_PROCESS_FORWARD);
    cfg.delivery_fn = ttl_probe_delivery;
    cfg.delivery_ctx = &probe;
    cfg.reject_local_encoder_loopback = 0;

    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);
    EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, plaintext,
                                      sizeof(plaintext), 3, 0, 4, 5) == 0);
    EXPECT(relay_inject_wire_datagram(ctx, datagrams[0], lens[0]) ==
           RELAY_INGRESS_OK);
    wait_local(ctx, 1, 1000);
    EXPECT(probe.seen >= 1);
    EXPECT(probe.last_ttl == 5);
    EXPECT(tx_capture_count(&cap) == 0);

    for (i = 1; i < shards; i++) {
        EXPECT(relay_inject_wire_datagram(ctx, datagrams[i], lens[i]) ==
               RELAY_INGRESS_OK);
    }

    relay_harness_close(ctx);
    local_decode_hub_destroy(&hub);
    pthread_mutex_destroy(&cap.mu);
    unlink(out_path);
}

static void test_local_no_cache_no_egress(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg;
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    const char *out_path = "build/relay_ld_cache_out.bin";
    uint8_t plaintext[96];
    uint8_t datagrams[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t lens[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t shards = 0;
    uint16_t i;

    memset(plaintext, 0x22, sizeof(plaintext));
    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);
    unlink(out_path);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_SINGLE_FILE;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_path = out_path;
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);

    cfg = harness_cfg(4, RELAY_PROCESS_CACHE);
    cfg.delivery_fn = local_decode_hub_delivery;
    cfg.delivery_ctx = &hub;
    cfg.reject_local_encoder_loopback = 0;

    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);
    EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, plaintext,
                                      sizeof(plaintext), 9, 0, 4, 7) == 0);
    for (i = 0; i < shards; i++) {
        EXPECT(relay_inject_wire_datagram(ctx, datagrams[i], lens[i]) ==
               RELAY_INGRESS_OK);
    }
    wait_local(ctx, shards, 2000);

    EXPECT(generation_cache_count(relay_generation_cache(ctx)) == 0);
    EXPECT(tx_capture_count(&cap) == 0);
    EXPECT(relay_total_stats(ctx)->forward == 0);
    EXPECT(relay_total_stats(ctx)->gen_created == 0);

    relay_harness_close(ctx);
    local_decode_hub_destroy(&hub);
    pthread_mutex_destroy(&cap.mu);
    unlink(out_path);
}

static void test_missing_shard_strict_no_corrupt_output(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg;
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    const char *out_path = "build/relay_ld_missing_out.bin";
    uint8_t plaintext[256];
    uint8_t datagrams[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t lens[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t shards = 0;
    uint8_t endbuf[WIRE_HEADER_SIZE];
    size_t end_len;
    uint8_t got[256];
    size_t got_len = 0;
    uint16_t i;

    for (i = 0; i < sizeof(plaintext); i++) {
        plaintext[i] = (uint8_t)(0xA0 + (i & 0x1fu));
    }
    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);
    unlink(out_path);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_SINGLE_FILE;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_path = out_path;
    hcfg.best_effort = 0;
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);

    cfg = harness_cfg(4, RELAY_PROCESS_FORWARD);
    cfg.delivery_fn = local_decode_hub_delivery;
    cfg.delivery_ctx = &hub;
    cfg.reject_local_encoder_loopback = 0;

    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);
    EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, plaintext,
                                      sizeof(plaintext), 11, 0, 4, 8) == 0);
    for (i = 0; i < 3; i++) {
        EXPECT(relay_inject_wire_datagram(ctx, datagrams[i], lens[i]) ==
               RELAY_INGRESS_OK);
    }
    end_len = make_end(endbuf, sizeof(endbuf), 11, 1, shards, 4, 8);
    EXPECT(relay_inject_wire_datagram(ctx, endbuf, end_len) ==
           RELAY_INGRESS_OK);

    wait_local(ctx, 4, 2000);
    EXPECT(!local_decode_hub_is_complete(&hub));
    EXPECT(local_decode_hub_active_count(&hub) > 0);
    EXPECT(local_decode_hub_strict_check(&hub) != 0);
    EXPECT(read_file(out_path, got, sizeof(got), &got_len) == 0);
    EXPECT(!(got_len == sizeof(plaintext) &&
             memcmp(got, plaintext, sizeof(plaintext)) == 0));

    relay_harness_close(ctx);
    local_decode_hub_destroy(&hub);
    pthread_mutex_destroy(&cap.mu);
    unlink(out_path);
}

static void test_single_output_still_rejects_second_flow(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg;
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    const char *out_path = "build/relay_ld_flow2_out.bin";
    uint8_t plaintext[32];
    uint8_t datagrams[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t lens[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t shards = 0;
    const LocalDecodeHubStats *hst;

    memset(plaintext, 0x33, sizeof(plaintext));
    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);
    unlink(out_path);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_SINGLE_FILE;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_path = out_path;
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);

    cfg = harness_cfg(4, RELAY_PROCESS_FORWARD);
    cfg.delivery_fn = local_decode_hub_delivery;
    cfg.delivery_ctx = &hub;
    cfg.reject_local_encoder_loopback = 0;

    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);
    EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, plaintext,
                                      sizeof(plaintext), 1, 0, 4, 8) == 0);
    EXPECT(relay_inject_wire_datagram(ctx, datagrams[0], lens[0]) ==
           RELAY_INGRESS_OK);
    wait_local(ctx, 1, 1000);

    EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, plaintext,
                                      sizeof(plaintext), 2, 0, 4, 8) == 0);
    EXPECT(relay_inject_wire_datagram(ctx, datagrams[0], lens[0]) ==
           RELAY_INGRESS_OK);
    wait_local(ctx, 2, 1000);

    hst = local_decode_hub_stats(&hub);
    EXPECT(hst != NULL && hst->flow_rejected >= 1);
    EXPECT(local_decode_hub_strict_check(&hub) != 0);

    relay_harness_close(ctx);
    local_decode_hub_destroy(&hub);
    pthread_mutex_destroy(&cap.mu);
    unlink(out_path);
}

static void test_local_encoder_not_blocked_with_decode(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg;
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    const char *out_path = "build/relay_ld_loop_out.bin";
    uint8_t plaintext[48];
    uint8_t datagrams[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t lens[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t shards = 0;

    memset(plaintext, 0x44, sizeof(plaintext));
    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);
    unlink(out_path);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_SINGLE_FILE;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_path = out_path;
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);

    cfg = harness_cfg(4, RELAY_PROCESS_FORWARD);
    cfg.delivery_fn = local_decode_hub_delivery;
    cfg.delivery_ctx = &hub;
    cfg.reject_local_encoder_loopback = 0;

    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);
    EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, plaintext,
                                      sizeof(plaintext), 5, 0, 4, 8) == 0);
    EXPECT(relay_inject_wire_datagram(ctx, datagrams[0], lens[0]) ==
           RELAY_INGRESS_OK);
    wait_local(ctx, 1, 1000);
    EXPECT(relay_total_stats(ctx)->inject_reject_loopback == 0);
    EXPECT(relay_total_stats(ctx)->local_deliver >= 1);

    relay_harness_close(ctx);
    local_decode_hub_destroy(&hub);
    pthread_mutex_destroy(&cap.mu);
    unlink(out_path);
}

static void test_delivery_rejects_nonlocal_final_dst(void)
{
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    const char *out_path = "build/relay_ld_reject_nonlocal.bin";
    uint8_t plaintext[64];
    uint8_t datagrams[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t lens[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t shards = 0;
    WireHeader hdr;
    uint8_t got[64];
    size_t got_len = 0;
    const LocalDecodeHubStats *hst;

    memset(plaintext, 0x55, sizeof(plaintext));
    unlink(out_path);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_SINGLE_FILE;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_path = out_path;
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);

    EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, plaintext,
                                      sizeof(plaintext), 1, 0, 2, 8) == 0);
    EXPECT(wire_header_decode(&hdr, datagrams[0], lens[0]) == 0);
    EXPECT(hdr.final_dst == 2);
    EXPECT(local_decode_hub_delivery(datagrams[0], lens[0], &hdr, &hub) == -1);

    hst = local_decode_hub_stats(&hub);
    EXPECT(hst != NULL && hst->metadata_mismatch >= 1);
    EXPECT(hst->delivered == 0);
    EXPECT(local_decode_hub_active_count(&hub) == 0);
    EXPECT(read_file(out_path, got, sizeof(got), &got_len) == 0);
    EXPECT(got_len == 0);
    EXPECT(local_decode_hub_strict_check(&hub) != 0);

    local_decode_hub_destroy(&hub);
    unlink(out_path);
}

static void test_output_io_failure_sets_ingest_error(void)
{
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    FILE *fp;
    uint8_t plaintext[128];
    uint8_t datagrams[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t lens[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t shards = 0;
    uint8_t endbuf[WIRE_HEADER_SIZE];
    size_t end_len;
    WireHeader hdr;
    uint16_t i;
    const LocalDecodeHubStats *hst;
    int saw_err = 0;

    memset(plaintext, 0x66, sizeof(plaintext));

    fp = fopen("/dev/full", "wb");
    EXPECT(fp != NULL);
    if (fp == NULL) {
        return;
    }

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_SINGLE_FILE;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output = fp; /* hub does not own; test fclose after destroy */
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);
    EXPECT(hub.single_close_output == 0);

    EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, plaintext,
                                      sizeof(plaintext), 8, 0, 4, 8) == 0);
    for (i = 0; i < shards; i++) {
        EXPECT(wire_header_decode(&hdr, datagrams[i], lens[i]) == 0);
        if (local_decode_hub_delivery(datagrams[i], lens[i], &hdr, &hub) != 0) {
            saw_err = 1;
            break;
        }
    }
    if (!saw_err) {
        end_len = make_end(endbuf, sizeof(endbuf), 8, 1, shards, 4, 8);
        EXPECT(wire_header_decode(&hdr, endbuf, end_len) == 0);
        (void)local_decode_hub_delivery(endbuf, end_len, &hdr, &hub);
    }

    hst = local_decode_hub_stats(&hub);
    EXPECT(hst != NULL && hst->ingest_error >= 1);
    EXPECT(local_decode_hub_strict_check(&hub) != 0);

    local_decode_hub_destroy(&hub);
    fclose(fp);
}

static void test_strict_incomplete_helper_fails(void)
{
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    const char *out_path = "build/relay_ld_strict_helper.bin";
    uint8_t plaintext[96];
    uint8_t datagrams[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t lens[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t shards = 0;
    uint8_t endbuf[WIRE_HEADER_SIZE];
    size_t end_len;
    WireHeader hdr;
    uint16_t i;

    memset(plaintext, 0x77, sizeof(plaintext));
    unlink(out_path);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_SINGLE_FILE;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_path = out_path;
    hcfg.best_effort = 0;
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);
    EXPECT(local_decode_hub_strict_check(&hub) == 0); /* unbound idle is OK */

    EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, plaintext,
                                      sizeof(plaintext), 13, 0, 4, 8) == 0);
    for (i = 0; i < 3; i++) {
        EXPECT(wire_header_decode(&hdr, datagrams[i], lens[i]) == 0);
        EXPECT(local_decode_hub_delivery(datagrams[i], lens[i], &hdr, &hub) ==
               0);
    }
    end_len = make_end(endbuf, sizeof(endbuf), 13, 1, shards, 4, 8);
    EXPECT(wire_header_decode(&hdr, endbuf, end_len) == 0);
    EXPECT(local_decode_hub_delivery(endbuf, end_len, &hdr, &hub) == 0);

    EXPECT(local_decode_hub_active_count(&hub) > 0);
    EXPECT(!local_decode_hub_is_complete(&hub));
    /* Same predicate main uses to flip RELAY_OK → RELAY_ERR. */
    EXPECT(local_decode_hub_strict_check(&hub) != 0);

    local_decode_hub_destroy(&hub);
    unlink(out_path);
}

static const char *k_multi_dir = "build/relay_ld_multi_out";

static void multi_dir_cleanup(void)
{
    char path[256];
    size_t i;

    for (i = 0; i < RELAY_MAX_FLOWS + 4u; i++) {
        /* Remove a small fixed set of known test flow ids and sequential ids. */
        (void)i;
    }
    /* Known filenames used by L2 tests. */
    unlink("build/relay_ld_multi_out/flow_101.bin");
    unlink("build/relay_ld_multi_out/flow_202.bin");
    unlink("build/relay_ld_multi_out/flow_1.bin");
    for (i = 0; i < RELAY_MAX_FLOWS + 2u; i++) {
        snprintf(path, sizeof(path), "build/relay_ld_multi_out/flow_%u.bin",
                 (unsigned)(10u + i));
        unlink(path);
    }
    rmdir(k_multi_dir);
}

static int multi_dir_prepare(void)
{
    multi_dir_cleanup();
    if (mkdir(k_multi_dir, 0755) != 0) {
        return -1;
    }
    return 0;
}

static int inject_copy_flow_complete(RelayCtx *ctx, uint32_t flow_id,
                                     const uint8_t *plaintext, size_t plen,
                                     uint8_t final_dst, uint8_t ttl)
{
    uint8_t datagrams[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t lens[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t shards = 0;
    uint8_t endbuf[WIRE_HEADER_SIZE];
    size_t end_len;
    uint16_t i;

    if (build_copy_block_datagrams(datagrams, lens, &shards, plaintext, plen,
                                   flow_id, 0, final_dst, ttl) != 0) {
        return -1;
    }
    for (i = 0; i < shards; i++) {
        if (relay_inject_wire_datagram(ctx, datagrams[i], lens[i]) !=
            RELAY_INGRESS_OK) {
            return -1;
        }
    }
    end_len = make_end(endbuf, sizeof(endbuf), flow_id, 1, shards, final_dst,
                       ttl);
    if (relay_inject_wire_datagram(ctx, endbuf, end_len) != RELAY_INGRESS_OK) {
        return -1;
    }
    return (int)shards + 1;
}

static void test_multiflow_interleaved_outputs(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg;
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    uint8_t pt101[200];
    uint8_t pt202[300];
    uint8_t d101[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    uint8_t d202[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t l101[PACKAGES_PER_ENCODE_BLOCK];
    size_t l202[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t s101 = 0;
    uint16_t s202 = 0;
    uint8_t endbuf[WIRE_HEADER_SIZE];
    size_t end_len;
    uint8_t got[512];
    size_t got_len = 0;
    uint16_t i;
    uint64_t local_want;

    memset(pt101, 0x11, sizeof(pt101));
    memset(pt202, 0x22, sizeof(pt202));
    for (i = 0; i < sizeof(pt101); i++) {
        pt101[i] = (uint8_t)(i + 1u);
    }
    for (i = 0; i < sizeof(pt202); i++) {
        pt202[i] = (uint8_t)(0x80u + (i & 0x3fu));
    }

    EXPECT(multi_dir_prepare() == 0);
    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_OUTPUT_DIR;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_dir = k_multi_dir;
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);

    cfg = harness_cfg(4, RELAY_PROCESS_FORWARD);
    cfg.delivery_fn = local_decode_hub_delivery;
    cfg.delivery_ctx = &hub;
    cfg.reject_local_encoder_loopback = 0;
    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);

    EXPECT(build_copy_block_datagrams(d101, l101, &s101, pt101, sizeof(pt101),
                                      101, 0, 4, 8) == 0);
    EXPECT(build_copy_block_datagrams(d202, l202, &s202, pt202, sizeof(pt202),
                                      202, 0, 4, 8) == 0);

    /* Interleave shards: 101[0], 202[0], 101[1], 202[1], ... */
    for (i = 0; i < s101 || i < s202; i++) {
        if (i < s101) {
            EXPECT(relay_inject_wire_datagram(ctx, d101[i], l101[i]) ==
                   RELAY_INGRESS_OK);
        }
        if (i < s202) {
            EXPECT(relay_inject_wire_datagram(ctx, d202[i], l202[i]) ==
                   RELAY_INGRESS_OK);
        }
    }
    end_len = make_end(endbuf, sizeof(endbuf), 101, 1, s101, 4, 8);
    EXPECT(relay_inject_wire_datagram(ctx, endbuf, end_len) ==
           RELAY_INGRESS_OK);
    end_len = make_end(endbuf, sizeof(endbuf), 202, 1, s202, 4, 8);
    EXPECT(relay_inject_wire_datagram(ctx, endbuf, end_len) ==
           RELAY_INGRESS_OK);

    local_want = (uint64_t)s101 + (uint64_t)s202 + 2u;
    wait_local(ctx, local_want, 3000);
    EXPECT(local_decode_hub_is_complete(&hub));
    EXPECT(local_decode_hub_strict_check(&hub) == 0);
    EXPECT(tx_capture_count(&cap) == 0);
    EXPECT(local_decode_hub_active_count(&hub) == 2);

    EXPECT(read_file("build/relay_ld_multi_out/flow_101.bin", got, sizeof(got),
                     &got_len) == 0);
    EXPECT(got_len == sizeof(pt101));
    EXPECT(memcmp(got, pt101, sizeof(pt101)) == 0);
    EXPECT(read_file("build/relay_ld_multi_out/flow_202.bin", got, sizeof(got),
                     &got_len) == 0);
    EXPECT(got_len == sizeof(pt202));
    EXPECT(memcmp(got, pt202, sizeof(pt202)) == 0);

    relay_harness_close(ctx);
    local_decode_hub_destroy(&hub);
    pthread_mutex_destroy(&cap.mu);
    multi_dir_cleanup();
}

static void test_multiflow_same_block_id_isolated(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg;
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    uint8_t pt101[180];
    uint8_t pt202[180];
    uint8_t got[256];
    size_t got_len = 0;
    int n;

    memset(pt101, 0xA1, sizeof(pt101));
    memset(pt202, 0xB2, sizeof(pt202));
    EXPECT(multi_dir_prepare() == 0);
    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_OUTPUT_DIR;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_dir = k_multi_dir;
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);

    cfg = harness_cfg(4, RELAY_PROCESS_FORWARD);
    cfg.delivery_fn = local_decode_hub_delivery;
    cfg.delivery_ctx = &hub;
    cfg.reject_local_encoder_loopback = 0;
    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);

    /* Both flows use block_id=0 inside inject_copy_flow_complete. */
    n = inject_copy_flow_complete(ctx, 101, pt101, sizeof(pt101), 4, 8);
    EXPECT(n > 0);
    n = inject_copy_flow_complete(ctx, 202, pt202, sizeof(pt202), 4, 8);
    EXPECT(n > 0);
    wait_local(ctx, (uint64_t)(2 * n), 3000);

    EXPECT(local_decode_hub_strict_check(&hub) == 0);
    EXPECT(read_file("build/relay_ld_multi_out/flow_101.bin", got, sizeof(got),
                     &got_len) == 0);
    EXPECT(got_len == sizeof(pt101) && memcmp(got, pt101, sizeof(pt101)) == 0);
    EXPECT(read_file("build/relay_ld_multi_out/flow_202.bin", got, sizeof(got),
                     &got_len) == 0);
    EXPECT(got_len == sizeof(pt202) && memcmp(got, pt202, sizeof(pt202)) == 0);

    relay_harness_close(ctx);
    local_decode_hub_destroy(&hub);
    pthread_mutex_destroy(&cap.mu);
    multi_dir_cleanup();
}

static void test_one_flow_end_other_continues(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg;
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    uint8_t pt101[120];
    uint8_t pt202[220];
    uint8_t got[256];
    size_t got_len = 0;
    size_t size101_after_end = 0;
    int n101;
    int n202;

    memset(pt101, 0x31, sizeof(pt101));
    memset(pt202, 0x32, sizeof(pt202));
    EXPECT(multi_dir_prepare() == 0);
    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_OUTPUT_DIR;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_dir = k_multi_dir;
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);

    cfg = harness_cfg(4, RELAY_PROCESS_FORWARD);
    cfg.delivery_fn = local_decode_hub_delivery;
    cfg.delivery_ctx = &hub;
    cfg.reject_local_encoder_loopback = 0;
    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);

    n101 = inject_copy_flow_complete(ctx, 101, pt101, sizeof(pt101), 4, 8);
    EXPECT(n101 > 0);
    wait_local(ctx, (uint64_t)n101, 2000);
    EXPECT(read_file("build/relay_ld_multi_out/flow_101.bin", got, sizeof(got),
                     &got_len) == 0);
    EXPECT(got_len == sizeof(pt101));
    size101_after_end = got_len;

    n202 = inject_copy_flow_complete(ctx, 202, pt202, sizeof(pt202), 4, 8);
    EXPECT(n202 > 0);
    wait_local(ctx, (uint64_t)(n101 + n202), 3000);

    EXPECT(read_file("build/relay_ld_multi_out/flow_101.bin", got, sizeof(got),
                     &got_len) == 0);
    EXPECT(got_len == size101_after_end);
    EXPECT(memcmp(got, pt101, sizeof(pt101)) == 0);
    EXPECT(read_file("build/relay_ld_multi_out/flow_202.bin", got, sizeof(got),
                     &got_len) == 0);
    EXPECT(got_len == sizeof(pt202) && memcmp(got, pt202, sizeof(pt202)) == 0);
    EXPECT(local_decode_hub_is_complete(&hub));
    EXPECT(local_decode_hub_strict_check(&hub) == 0);

    relay_harness_close(ctx);
    local_decode_hub_destroy(&hub);
    pthread_mutex_destroy(&cap.mu);
    multi_dir_cleanup();
}

static void test_output_dir_capacity_reject(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg;
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    uint8_t plaintext[32];
    uint8_t datagrams[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t lens[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t shards = 0;
    uint32_t i;
    const LocalDecodeHubStats *hst;
    RelayIngressStatus st;

    memset(plaintext, 0x44, sizeof(plaintext));
    EXPECT(multi_dir_prepare() == 0);
    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_OUTPUT_DIR;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_dir = k_multi_dir;
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);

    cfg = harness_cfg(4, RELAY_PROCESS_CACHE);
    cfg.delivery_fn = local_decode_hub_delivery;
    cfg.delivery_ctx = &hub;
    cfg.reject_local_encoder_loopback = 0;
    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);

    for (i = 0; i < RELAY_MAX_FLOWS; i++) {
        EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, plaintext,
                                          sizeof(plaintext), 10u + i, 0, 4,
                                          8) == 0);
        EXPECT(relay_inject_wire_datagram(ctx, datagrams[0], lens[0]) ==
               RELAY_INGRESS_OK);
    }
    wait_local(ctx, RELAY_MAX_FLOWS, 2000);
    EXPECT(local_decode_hub_active_count(&hub) == RELAY_MAX_FLOWS);

    EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, plaintext,
                                      sizeof(plaintext), 10u + RELAY_MAX_FLOWS,
                                      0, 4, 8) == 0);
    st = relay_inject_wire_datagram(ctx, datagrams[0], lens[0]);
    /* Delivery returns -1 but ingress still OK (local path swallows). */
    EXPECT(st == RELAY_INGRESS_OK);
    wait_local(ctx, RELAY_MAX_FLOWS + 1u, 1000);

    hst = local_decode_hub_stats(&hub);
    EXPECT(hst != NULL && hst->flow_rejected >= 1);
    EXPECT(local_decode_hub_active_count(&hub) == RELAY_MAX_FLOWS);
    EXPECT(tx_capture_count(&cap) == 0);
    EXPECT(generation_cache_count(relay_generation_cache(ctx)) == 0);
    EXPECT(relay_total_stats(ctx)->forward == 0);
    EXPECT(local_decode_hub_strict_check(&hub) != 0);

    relay_harness_close(ctx);
    local_decode_hub_destroy(&hub);
    pthread_mutex_destroy(&cap.mu);
    multi_dir_cleanup();
}

static void test_output_dir_local_only(void)
{
    RelayCtx *ctx = NULL;
    TxCapture cap;
    RelayConfig cfg;
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    uint8_t local_pt[64];
    uint8_t remote_pt[64];
    uint8_t datagrams[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t lens[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t shards = 0;
    int n;

    memset(local_pt, 0x51, sizeof(local_pt));
    memset(remote_pt, 0x52, sizeof(remote_pt));
    EXPECT(multi_dir_prepare() == 0);
    memset(&cap, 0, sizeof(cap));
    pthread_mutex_init(&cap.mu, NULL);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_OUTPUT_DIR;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_dir = k_multi_dir;
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);

    cfg = harness_cfg(4, RELAY_PROCESS_CACHE);
    cfg.delivery_fn = local_decode_hub_delivery;
    cfg.delivery_ctx = &hub;
    cfg.reject_local_encoder_loopback = 0;
    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);

    n = inject_copy_flow_complete(ctx, 101, local_pt, sizeof(local_pt), 4, 8);
    EXPECT(n > 0);
    wait_local(ctx, (uint64_t)n, 2000);
    EXPECT(generation_cache_count(relay_generation_cache(ctx)) == 0);
    EXPECT(tx_capture_count(&cap) == 0);

    EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, remote_pt,
                                      sizeof(remote_pt), 1, 0, 9, 8) == 0);
    EXPECT(relay_inject_wire_datagram(ctx, datagrams[0], lens[0]) ==
           RELAY_INGRESS_OK);
    wait_forward(ctx, 1, 1000);
    EXPECT(relay_total_stats(ctx)->forward >= 1);
    EXPECT(tx_capture_count(&cap) >= 1);
    EXPECT(generation_cache_count(relay_generation_cache(ctx)) >= 1);

    relay_harness_close(ctx);
    local_decode_hub_destroy(&hub);
    pthread_mutex_destroy(&cap.mu);
    multi_dir_cleanup();
}

static void test_output_dir_rejects_nonlocal_delivery(void)
{
    LocalDecodeHub hub;
    LocalDecodeHubConfig hcfg;
    uint8_t plaintext[48];
    uint8_t datagrams[PACKAGES_PER_ENCODE_BLOCK][RELAY_MAX_DATAGRAM];
    size_t lens[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t shards = 0;
    WireHeader hdr;
    struct stat st;

    memset(plaintext, 0x61, sizeof(plaintext));
    EXPECT(multi_dir_prepare() == 0);

    memset(&hcfg, 0, sizeof(hcfg));
    hcfg.mode = LOCAL_DECODE_MODE_OUTPUT_DIR;
    hcfg.codec_kind = CODEC_KIND_COPY;
    hcfg.output_dir = k_multi_dir;
    hcfg.local_node_id = 4;
    EXPECT(local_decode_hub_init(&hub, &hcfg) == 0);

    EXPECT(build_copy_block_datagrams(datagrams, lens, &shards, plaintext,
                                      sizeof(plaintext), 101, 0, 2, 8) == 0);
    EXPECT(wire_header_decode(&hdr, datagrams[0], lens[0]) == 0);
    EXPECT(local_decode_hub_delivery(datagrams[0], lens[0], &hdr, &hub) == -1);
    EXPECT(local_decode_hub_active_count(&hub) == 0);
    EXPECT(stat("build/relay_ld_multi_out/flow_101.bin", &st) != 0);
    EXPECT(local_decode_hub_strict_check(&hub) != 0);

    local_decode_hub_destroy(&hub);
    multi_dir_cleanup();
}

int main(void)
{
    if (mkdir("build", 0755) != 0) {
        /* ok if exists */
    }

    test_local_copy_end_matches_source();
    test_nonlocal_still_tx_empty_output();
    test_local_ttl_unchanged();
    test_local_no_cache_no_egress();
    test_missing_shard_strict_no_corrupt_output();
    test_single_output_still_rejects_second_flow();
    test_local_encoder_not_blocked_with_decode();
    test_delivery_rejects_nonlocal_final_dst();
    test_output_io_failure_sets_ingest_error();
    test_strict_incomplete_helper_fails();

    test_multiflow_interleaved_outputs();
    test_multiflow_same_block_id_isolated();
    test_one_flow_end_other_continues();
    test_output_dir_capacity_reject();
    test_output_dir_local_only();
    test_output_dir_rejects_nonlocal_delivery();

    if (g_failures != 0) {
        fprintf(stderr, "relay_local_decode_tests: %d failure(s)\n", g_failures);
        return EXIT_FAILURE;
    }
    fprintf(stderr, "relay_local_decode_tests: ok\n");
    return EXIT_SUCCESS;
}
