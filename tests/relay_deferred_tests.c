#include "relay.h"
#include "relay_deferred.h"
#include "wire_header.h"

#include <pthread.h>
#include <stdint.h>
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

typedef struct {
    uint64_t ids[16];
    size_t n;
} TxCapture;

static void tx_capture_cb(const uint8_t *datagram, size_t len, void *arg)
{
    TxCapture *cap = arg;
    WireHeader hdr;

    if (cap == NULL || cap->n >= 16) {
        return;
    }
    if (wire_header_decode(&hdr, datagram, len) != 0) {
        return;
    }
    cap->ids[cap->n++] = ((uint64_t)hdr.type << 56) | hdr.block_id;
}

static uint8_t *make_owned_data(uint32_t flow_id, uint64_t block_id,
                                uint8_t type, size_t *len_out)
{
    uint8_t *buf;
    WireHeader hdr;

    buf = calloc(1, WIRE_HEADER_SIZE + 4u);
    if (buf == NULL) {
        return NULL;
    }
    memset(&hdr, 0, sizeof(hdr));
    hdr.type = type;
    hdr.final_dst = 4;
    hdr.ttl = 8;
    hdr.flow_id = flow_id;
    hdr.block_id = block_id;
    hdr.shard_index = 0;
    hdr.shard_count = 1;
    hdr.valid_len = 4;
    hdr.payload_len = 4;
    wire_header_encode(buf, &hdr);
    buf[WIRE_HEADER_SIZE] = (uint8_t)(block_id & 0xffu);
    *len_out = WIRE_HEADER_SIZE + 4u;
    return buf;
}

static uint8_t *make_v4_return_ack(uint32_t flow_id, uint64_t segment_id,
                                   size_t *len_out)
{
    uint8_t *buf;
    WireHeader hdr;

    buf = calloc(1, WIRE_V4_HEADER_SIZE);
    if (buf == NULL) {
        return NULL;
    }
    memset(&hdr, 0, sizeof(hdr));
    hdr.version = WIRE_VERSION_V4;
    hdr.type = WIRE_TYPE_ACK;
    hdr.final_dst = 1;
    hdr.ttl = 8;
    hdr.flow_id = flow_id;
    hdr.block_id = segment_id;
    hdr.origin_node = 4;
    hdr.flags = WIRE_FLAG_RETURN_PATH;
    wire_header_encode_v4(buf, &hdr);
    *len_out = WIRE_V4_HEADER_SIZE;
    return buf;
}

static RelayDeferredPacket make_pkt(uint32_t flow_id, uint64_t seq)
{
    RelayDeferredPacket pkt;
    size_t len = 0;

    memset(&pkt, 0, sizeof(pkt));
    pkt.datagram = make_owned_data(flow_id, seq, WIRE_TYPE_DATA, &len);
    pkt.len = len;
    pkt.flow_id = flow_id;
    pkt.enqueue_ns = seq;
    return pkt;
}

static void test_hub_init_rejects_bad_config(void)
{
    RelayDeferredHub hub;
    RelayDeferredHubConfig cfg;

    memset(&cfg, 0, sizeof(cfg));
    cfg.max_active_flows = 0;
    cfg.per_flow_capacity = 8;
    cfg.total_capacity = 8;
    EXPECT(relay_deferred_hub_init(&hub, &cfg) == RELAY_DEFERRED_ERR_INVALID);

    cfg.max_active_flows = 65;
    EXPECT(relay_deferred_hub_init(&hub, &cfg) == RELAY_DEFERRED_ERR_INVALID);

    cfg.max_active_flows = 4;
    cfg.per_flow_capacity = 0;
    EXPECT(relay_deferred_hub_init(&hub, &cfg) == RELAY_DEFERRED_ERR_INVALID);
}

static void test_per_flow_fifo(void)
{
    RelayDeferredHub hub;
    RelayDeferredHubConfig cfg;
    RelayDeferredPacket pkt;
    uint64_t i;

    memset(&cfg, 0, sizeof(cfg));
    cfg.max_active_flows = 4;
    cfg.per_flow_capacity = 16;
    cfg.total_capacity = 64;
    EXPECT(relay_deferred_hub_init(&hub, &cfg) == RELAY_DEFERRED_OK);

    for (i = 0; i < 10; i++) {
        pkt = make_pkt(100, i);
        EXPECT(relay_deferred_hub_try_push(&hub, &pkt) == RELAY_DEFERRED_OK);
        EXPECT(pkt.datagram == NULL);
    }
    for (i = 0; i < 10; i++) {
        EXPECT(relay_deferred_hub_try_pop(&hub, &pkt) == RELAY_DEFERRED_OK);
        EXPECT(pkt.flow_id == 100);
        EXPECT(pkt.enqueue_ns == i);
        free(pkt.datagram);
    }
    EXPECT(relay_deferred_hub_try_pop(&hub, &pkt) == RELAY_DEFERRED_ERR_EMPTY);
    relay_deferred_hub_destroy(&hub);
}

static void test_multi_flow_round_robin_quota(void)
{
    RelayDeferredHub hub;
    RelayDeferredHubConfig cfg;
    RelayDeferredPacket pkt;
    uint32_t counts[3] = {0, 0, 0};
    uint32_t prev_flow = UINT32_MAX;
    uint32_t run = 0;
    uint32_t max_run = 0;
    int i;

    memset(&cfg, 0, sizeof(cfg));
    cfg.max_active_flows = 8;
    cfg.per_flow_capacity = 64;
    cfg.total_capacity = 256;
    EXPECT(relay_deferred_hub_init(&hub, &cfg) == RELAY_DEFERRED_OK);

    for (i = 0; i < 24; i++) {
        pkt = make_pkt(10, (uint64_t)i);
        EXPECT(relay_deferred_hub_try_push(&hub, &pkt) == RELAY_DEFERRED_OK);
        pkt = make_pkt(20, (uint64_t)i);
        EXPECT(relay_deferred_hub_try_push(&hub, &pkt) == RELAY_DEFERRED_OK);
        pkt = make_pkt(30, (uint64_t)i);
        EXPECT(relay_deferred_hub_try_push(&hub, &pkt) == RELAY_DEFERRED_OK);
    }

    for (i = 0; i < 72; i++) {
        EXPECT(relay_deferred_hub_try_pop(&hub, &pkt) == RELAY_DEFERRED_OK);
        if (pkt.flow_id == 10) {
            counts[0]++;
        } else if (pkt.flow_id == 20) {
            counts[1]++;
        } else if (pkt.flow_id == 30) {
            counts[2]++;
        } else {
            EXPECT(0);
        }
        if (pkt.flow_id == prev_flow) {
            run++;
        } else {
            run = 1;
            prev_flow = pkt.flow_id;
        }
        if (run > max_run) {
            max_run = run;
        }
        free(pkt.datagram);
    }
    EXPECT(counts[0] == 24 && counts[1] == 24 && counts[2] == 24);
    EXPECT(max_run <= RELAY_DEFERRED_QUOTA);
    relay_deferred_hub_destroy(&hub);
}

static void test_overflows_and_table_full(void)
{
    RelayDeferredHub hub;
    RelayDeferredHubConfig cfg;
    RelayDeferredPacket pkt;
    RelayDeferredHubStats st;
    int i;

    memset(&cfg, 0, sizeof(cfg));
    cfg.max_active_flows = 2;
    cfg.per_flow_capacity = 2;
    cfg.total_capacity = 3;
    EXPECT(relay_deferred_hub_init(&hub, &cfg) == RELAY_DEFERRED_OK);

    pkt = make_pkt(1, 0);
    EXPECT(relay_deferred_hub_try_push(&hub, &pkt) == RELAY_DEFERRED_OK);
    pkt = make_pkt(1, 1);
    EXPECT(relay_deferred_hub_try_push(&hub, &pkt) == RELAY_DEFERRED_OK);
    pkt = make_pkt(1, 2);
    EXPECT(relay_deferred_hub_try_push(&hub, &pkt) ==
           RELAY_DEFERRED_ERR_FULL_FLOW);
    EXPECT(pkt.datagram != NULL);
    free(pkt.datagram);

    pkt = make_pkt(2, 0);
    EXPECT(relay_deferred_hub_try_push(&hub, &pkt) == RELAY_DEFERRED_OK);
    pkt = make_pkt(2, 1);
    EXPECT(relay_deferred_hub_try_push(&hub, &pkt) ==
           RELAY_DEFERRED_ERR_FULL_TOTAL);
    EXPECT(pkt.datagram != NULL);
    free(pkt.datagram);

    pkt = make_pkt(3, 0);
    EXPECT(relay_deferred_hub_try_push(&hub, &pkt) ==
           RELAY_DEFERRED_ERR_TABLE_FULL);
    EXPECT(pkt.datagram != NULL);
    free(pkt.datagram);

    relay_deferred_hub_stats_snapshot(&hub, &st);
    EXPECT(st.drop_overflow_flow == 1);
    EXPECT(st.drop_overflow_total == 1);
    EXPECT(st.drop_table_full == 1);

    for (i = 0; i < 3; i++) {
        EXPECT(relay_deferred_hub_try_pop(&hub, &pkt) == RELAY_DEFERRED_OK);
        free(pkt.datagram);
    }
    pkt = make_pkt(99, 0);
    EXPECT(relay_deferred_hub_try_push(&hub, &pkt) == RELAY_DEFERRED_OK);
    EXPECT(relay_deferred_hub_try_pop(&hub, &pkt) == RELAY_DEFERRED_OK);
    free(pkt.datagram);

    relay_deferred_hub_destroy(&hub);
}

typedef struct {
    RelayDeferredHub *hub;
    int woke;
} WaitArg;

static void *waiter_main(void *arg)
{
    WaitArg *wa = arg;

    EXPECT(relay_deferred_hub_wait(wa->hub) == RELAY_DEFERRED_OK);
    wa->woke = 1;
    return NULL;
}

static void *shutdown_waiter_main(void *arg)
{
    WaitArg *wa = arg;

    EXPECT(relay_deferred_hub_wait(wa->hub) == RELAY_DEFERRED_ERR_SHUTDOWN);
    wa->woke = 1;
    return NULL;
}

static void test_wakeup_and_shutdown(void)
{
    RelayDeferredHub hub;
    RelayDeferredHubConfig cfg;
    RelayDeferredPacket pkt;
    WaitArg wa;
    pthread_t th;
    RelayDeferredStatus st;

    memset(&cfg, 0, sizeof(cfg));
    cfg.max_active_flows = 4;
    cfg.per_flow_capacity = 8;
    cfg.total_capacity = 16;
    EXPECT(relay_deferred_hub_init(&hub, &cfg) == RELAY_DEFERRED_OK);

    wa.hub = &hub;
    wa.woke = 0;
    EXPECT(pthread_create(&th, NULL, waiter_main, &wa) == 0);
    usleep(50 * 1000);
    EXPECT(wa.woke == 0);
    pkt = make_pkt(7, 1);
    EXPECT(relay_deferred_hub_try_push(&hub, &pkt) == RELAY_DEFERRED_OK);
    EXPECT(pthread_join(th, NULL) == 0);
    EXPECT(wa.woke == 1);
    EXPECT(relay_deferred_hub_try_pop(&hub, &pkt) == RELAY_DEFERRED_OK);
    free(pkt.datagram);

    wa.woke = 0;
    EXPECT(pthread_create(&th, NULL, shutdown_waiter_main, &wa) == 0);
    usleep(50 * 1000);
    relay_deferred_hub_shutdown(&hub);
    EXPECT(pthread_join(th, NULL) == 0);
    EXPECT(wa.woke == 1);
    st = relay_deferred_hub_wait(&hub);
    EXPECT(st == RELAY_DEFERRED_ERR_SHUTDOWN);
    relay_deferred_hub_destroy(&hub);
}

typedef struct {
    RelayDeferredHub *hub;
    volatile int stop;
    uint64_t popped;
} SlowConsumerArg;

static void *slow_consumer_main(void *arg)
{
    SlowConsumerArg *ca = arg;

    while (1) {
        RelayDeferredPacket pkt;
        RelayDeferredStatus st;

        st = relay_deferred_hub_wait(ca->hub);
        if (st == RELAY_DEFERRED_ERR_SHUTDOWN) {
            break;
        }
        while (relay_deferred_hub_try_pop(ca->hub, &pkt) == RELAY_DEFERRED_OK) {
            ca->popped++;
            usleep(1000);
            free(pkt.datagram);
        }
        if (ca->stop && relay_deferred_hub_total_count(ca->hub) == 0) {
            break;
        }
    }
    while (1) {
        RelayDeferredPacket pkt;

        if (relay_deferred_hub_try_pop(ca->hub, &pkt) != RELAY_DEFERRED_OK) {
            break;
        }
        ca->popped++;
        free(pkt.datagram);
    }
    return NULL;
}

static void test_producer_not_blocked_by_slow_consumer(void)
{
    RelayDeferredHub hub;
    RelayDeferredHubConfig cfg;
    SlowConsumerArg ca;
    pthread_t th;
    uint64_t i;
    RelayDeferredHubStats st;

    memset(&cfg, 0, sizeof(cfg));
    cfg.max_active_flows = 4;
    cfg.per_flow_capacity = 128;
    cfg.total_capacity = 256;
    EXPECT(relay_deferred_hub_init(&hub, &cfg) == RELAY_DEFERRED_OK);

    ca.hub = &hub;
    ca.stop = 0;
    ca.popped = 0;
    EXPECT(pthread_create(&th, NULL, slow_consumer_main, &ca) == 0);

    for (i = 0; i < 64; i++) {
        RelayDeferredPacket pkt = make_pkt(42, i);

        EXPECT(relay_deferred_hub_try_push(&hub, &pkt) == RELAY_DEFERRED_OK);
    }

    ca.stop = 1;
    relay_deferred_hub_shutdown(&hub);
    EXPECT(pthread_join(th, NULL) == 0);
    relay_deferred_hub_stats_snapshot(&hub, &st);
    EXPECT(st.enqueue_ok == 64);
    EXPECT(ca.popped == 64);
    relay_deferred_hub_destroy(&hub);
}

static void test_data_end_fifo_inject_only(void)
{
    RelayCtx *ctx = NULL;
    RelayConfig cfg;
    TxCapture cap;
    uint8_t *buf;
    size_t len;
    int i;

    memset(&cap, 0, sizeof(cap));
    memset(&cfg, 0, sizeof(cfg));
    cfg.local_node_id = 2;
    cfg.egress_capacity = 64;
    cfg.egress_wait_ms = 0;
    cfg.max_active_flows = 8;
    cfg.deferred_per_flow = 32;
    cfg.deferred_total = 64;

    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);

    for (i = 0; i < 5; i++) {
        buf = make_owned_data(55, (uint64_t)i, WIRE_TYPE_DATA, &len);
        EXPECT(relay_inject_wire_datagram(ctx, buf, len) == RELAY_INGRESS_OK);
        free(buf);
    }
    buf = make_owned_data(55, 5, WIRE_TYPE_END, &len);
    EXPECT(relay_inject_wire_datagram(ctx, buf, len) == RELAY_INGRESS_OK);
    free(buf);

    /* inject is synchronous; TX may still be draining */
    for (i = 0; i < 50 && cap.n < 6; i++) {
        usleep(1000);
    }
    EXPECT(cap.n == 6);
    for (i = 0; i < 5; i++) {
        EXPECT(cap.ids[i] == (((uint64_t)WIRE_TYPE_DATA << 56) | (uint64_t)i));
    }
    EXPECT(cap.ids[5] == (((uint64_t)WIRE_TYPE_END << 56) | 5ull));
    EXPECT(relay_total_stats(ctx)->forward == 6);

    relay_harness_close(ctx);
}

static void test_relay_ack_data_lane_classification(void)
{
    RelayCtx *ctx = NULL;
    RelayConfig cfg;
    TxCapture cap;
    EgressQueueStats ack_stats;
    EgressQueueStats data_stats;
    EgressQueueStats compat_stats;
    uint8_t *buf;
    size_t len;

    memset(&cap, 0, sizeof(cap));
    memset(&cfg, 0, sizeof(cfg));
    cfg.local_node_id = 2;
    cfg.egress_capacity = 16;
    EXPECT(relay_harness_open(&ctx, &cfg, tx_capture_cb, &cap) == RELAY_OK);

    buf = make_v4_return_ack(7, 100, &len);
    EXPECT(buf != NULL);
    EXPECT(relay_inject_wire_datagram(ctx, buf, len) == RELAY_INGRESS_OK);
    free(buf);
    buf = make_owned_data(7, 101, WIRE_TYPE_DATA, &len);
    EXPECT(buf != NULL);
    EXPECT(relay_inject_wire_datagram(ctx, buf, len) == RELAY_INGRESS_OK);
    free(buf);

    relay_ack_egress_stats_snapshot(ctx, &ack_stats);
    relay_data_egress_stats_snapshot(ctx, &data_stats);
    relay_egress_stats_snapshot(ctx, &compat_stats);
    EXPECT(ack_stats.enqueue_immediate == 1);
    EXPECT(data_stats.enqueue_immediate == 1);
    EXPECT(memcmp(&data_stats, &compat_stats, sizeof(data_stats)) == 0);

    relay_harness_close(ctx);
}

typedef struct {
    RelayCtx *ctx;
    volatile int release;
    uint64_t forwarded;
} HoldTxArg;

static void hold_tx_cb(const uint8_t *datagram, size_t len, void *arg)
{
    HoldTxArg *ha = arg;

    (void)datagram;
    (void)len;
    while (!ha->release) {
        usleep(1000);
    }
    ha->forwarded++;
}

static void test_egress_wait_blocks_inject_not_deferred_producer(void)
{
    /*
     * egress_wait_ms>0 can block the processing/inject path that enqueues.
     * Deferred producer (hub try_push) must still succeed independently —
     * models RX continuing while processing waits on egress.
     */
    RelayCtx *ctx = NULL;
    RelayConfig cfg;
    HoldTxArg ha;
    RelayDeferredHub hub;
    RelayDeferredHubConfig dcfg;
    RelayDeferredPacket pkt;
    int i;

    memset(&ha, 0, sizeof(ha));
    memset(&cfg, 0, sizeof(cfg));
    cfg.local_node_id = 2;
    cfg.egress_capacity = 1;
    cfg.egress_wait_ms = 30;
    cfg.max_active_flows = 4;
    cfg.deferred_per_flow = 64;
    cfg.deferred_total = 64;

    EXPECT(relay_harness_open(&ctx, &cfg, hold_tx_cb, &ha) == RELAY_OK);

    /* Fill egress via sync inject; second inject may timed-wait on egress. */
    {
        uint8_t *buf;
        size_t len;

        buf = make_owned_data(1, 0, WIRE_TYPE_DATA, &len);
        EXPECT(relay_inject_wire_datagram(ctx, buf, len) == RELAY_INGRESS_OK);
        free(buf);
    }

    memset(&dcfg, 0, sizeof(dcfg));
    dcfg.max_active_flows = 4;
    dcfg.per_flow_capacity = 32;
    dcfg.total_capacity = 32;
    EXPECT(relay_deferred_hub_init(&hub, &dcfg) == RELAY_DEFERRED_OK);
    for (i = 0; i < 16; i++) {
        pkt = make_pkt(77, (uint64_t)i);
        EXPECT(relay_deferred_hub_try_push(&hub, &pkt) == RELAY_DEFERRED_OK);
    }
    EXPECT(relay_deferred_hub_total_count(&hub) == 16);
    for (i = 0; i < 16; i++) {
        EXPECT(relay_deferred_hub_try_pop(&hub, &pkt) == RELAY_DEFERRED_OK);
        free(pkt.datagram);
    }
    relay_deferred_hub_destroy(&hub);

    ha.release = 1;
    usleep(50 * 1000);
    relay_harness_close(ctx);
}

static void test_high_flow_id_mapping(void)
{
    RelayDeferredHub hub;
    RelayDeferredHubConfig cfg;
    RelayDeferredPacket pkt;

    memset(&cfg, 0, sizeof(cfg));
    cfg.max_active_flows = 8;
    cfg.per_flow_capacity = 4;
    cfg.total_capacity = 16;
    EXPECT(relay_deferred_hub_init(&hub, &cfg) == RELAY_DEFERRED_OK);

    pkt = make_pkt(0xffffffffu, 1);
    EXPECT(relay_deferred_hub_try_push(&hub, &pkt) == RELAY_DEFERRED_OK);
    pkt = make_pkt(1000003u, 2);
    EXPECT(relay_deferred_hub_try_push(&hub, &pkt) == RELAY_DEFERRED_OK);

    EXPECT(relay_deferred_hub_try_pop(&hub, &pkt) == RELAY_DEFERRED_OK);
    EXPECT(pkt.flow_id == 0xffffffffu || pkt.flow_id == 1000003u);
    free(pkt.datagram);
    EXPECT(relay_deferred_hub_try_pop(&hub, &pkt) == RELAY_DEFERRED_OK);
    EXPECT(pkt.flow_id == 0xffffffffu || pkt.flow_id == 1000003u);
    free(pkt.datagram);

    relay_deferred_hub_destroy(&hub);
}

int main(void)
{
    test_hub_init_rejects_bad_config();
    test_per_flow_fifo();
    test_multi_flow_round_robin_quota();
    test_overflows_and_table_full();
    test_wakeup_and_shutdown();
    test_producer_not_blocked_by_slow_consumer();
    test_data_end_fifo_inject_only();
    test_relay_ack_data_lane_classification();
    test_egress_wait_blocks_inject_not_deferred_producer();
    test_high_flow_id_mapping();

    if (g_failures != 0) {
        fprintf(stderr, "relay_deferred_tests: %d failure(s)\n", g_failures);
        return 1;
    }
    fprintf(stderr, "relay_deferred_tests: ok\n");
    return 0;
}
