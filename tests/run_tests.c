#include "flow_manager.h"
#include "packet.h"
#include "time_utils.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "flow_buffer.h"
#include "mixed_queue.h"

/* ---- packet / flow_buffer (from test_flow_buffer.c) ---- */

#define QUEUE_CAP 4
#define PRODUCED  12

typedef struct {
    FlowCircularBuffer *fb;
    int                 errors;
} ProducerCtx;

typedef struct {
    FlowCircularBuffer *fb;
    int                 received;
    int                 errors;
} ConsumerCtx;

static void *fb_producer_thread(void *arg)
{
    ProducerCtx *ctx = arg;
    char payload[32];

    for (int i = 0; i < PRODUCED; i++) {
        DataPacket *pkt;
        int n;

        n = snprintf(payload, sizeof(payload), "pkt-%d", i);
        pkt = packet_create(0, payload, (size_t)n + 1);
        if (pkt == NULL) {
            ctx->errors++;
            return NULL;
        }

        if (flow_buffer_enqueue(ctx->fb, &pkt) != FB_OK) {
            packet_free(pkt);
            ctx->errors++;
            return NULL;
        }
    }

    return NULL;
}

static void *fb_consumer_thread(void *arg)
{
    ConsumerCtx *ctx = arg;

    while (ctx->received < PRODUCED) {
        DataPacket *pkt = NULL;
        FlowBufferStatus st;

        st = flow_buffer_dequeue(ctx->fb, &pkt, NULL);
        if (st != FB_OK || pkt == NULL) {
            ctx->errors++;
            return NULL;
        }

        ctx->received++;
        packet_free(pkt);
    }

    return NULL;
}

static int test_flow_buffer_roundtrip(void)
{
    FlowCircularBuffer fb;
    pthread_t prod, cons;
    ProducerCtx pctx;
    ConsumerCtx cctx;

    if (flow_buffer_init(&fb, QUEUE_CAP) != FB_OK) {
        return 1;
    }

    pctx = (ProducerCtx){ .fb = &fb, .errors = 0 };
    cctx = (ConsumerCtx){ .fb = &fb, .received = 0, .errors = 0 };

    if (pthread_create(&cons, NULL, fb_consumer_thread, &cctx) != 0) {
        flow_buffer_destroy(&fb);
        return 1;
    }

    {
        struct timespec delay = { .tv_sec = 0, .tv_nsec = 10000000L };
        nanosleep(&delay, NULL);
    }

    if (pthread_create(&prod, NULL, fb_producer_thread, &pctx) != 0) {
        flow_buffer_shutdown(&fb);
        pthread_join(cons, NULL);
        flow_buffer_destroy(&fb);
        return 1;
    }

    pthread_join(prod, NULL);
    pthread_join(cons, NULL);
    flow_buffer_destroy(&fb);

    return (pctx.errors != 0 || cctx.errors != 0 || cctx.received != PRODUCED);
}

static int test_flow_buffer_shutdown(void)
{
    FlowCircularBuffer fb;
    FlowBufferStatus st;
    DataPacket *pkt;

    if (flow_buffer_init(&fb, 2) != FB_OK) {
        return 1;
    }

    flow_buffer_shutdown(&fb);

    pkt = packet_create(0, "x", 2);
    if (pkt == NULL) {
        flow_buffer_destroy(&fb);
        return 1;
    }

    st = flow_buffer_enqueue(&fb, &pkt);
    if (st != FB_ERR_SHUTDOWN) {
        packet_free(pkt);
        flow_buffer_destroy(&fb);
        return 1;
    }
    packet_free(pkt);

    pkt = NULL;
    st = flow_buffer_dequeue(&fb, &pkt, NULL);
    if (st != FB_ERR_SHUTDOWN || pkt != NULL) {
        packet_free(pkt);
        flow_buffer_destroy(&fb);
        return 1;
    }

    flow_buffer_destroy(&fb);
    return 0;
}

static int test_packet_create_free(void)
{
    DataPacket *pkt = packet_create(3, "abc", 4);
    if (pkt == NULL || pkt->flow_id != 3 || pkt->payload_len != 4) {
        packet_free(pkt);
        return 1;
    }
    if (memcmp(pkt->payload, "abc", 4) != 0) {
        packet_free(pkt);
        return 1;
    }
    packet_free(pkt);
    return 0;
}

/* ---- time_utils ---- */

static int test_time_utils_sub_add(void)
{
    struct timespec a = { .tv_sec = 10, .tv_nsec = 500000000L };
    struct timespec b = { .tv_sec = 10, .tv_nsec = 200000000L };
    struct timespec out;
    struct timespec back;

    if (time_utils_ts_sub(&a, &b, &out) != TU_OK) {
        return 1;
    }
    if (out.tv_sec != 0 || out.tv_nsec != 300000000L) {
        return 1;
    }

    if (time_utils_ts_add(&b, &out, &back) != TU_OK) {
        return 1;
    }
    if (back.tv_sec != a.tv_sec || back.tv_nsec != a.tv_nsec) {
        return 1;
    }

    return 0;
}

static int test_time_utils_sleep_for(void)
{
    struct timespec start;
    struct timespec end;
    struct timespec delta;
    struct timespec expect = { .tv_sec = 0, .tv_nsec = 50000000L };

    if (time_utils_now_mono(&start) != TU_OK) {
        return 1;
    }

    if (time_utils_sleep_for(&expect) != TU_OK) {
        return 1;
    }

    if (time_utils_now_mono(&end) != TU_OK) {
        return 1;
    }

    if (time_utils_ts_sub(&end, &start, &delta) != TU_OK) {
        return 1;
    }

    if (delta.tv_sec == 0 && delta.tv_nsec < 30000000L) {
        return 1;
    }

    return 0;
}

/* ---- mixed_queue ---- */

static int test_mixed_queue_push_pop(void)
{
    MixedQueue mq;
    DataPacket *a;
    DataPacket *b;
    DataPacket *out;

    if (mixed_queue_init(&mq, 2) != MQ_OK) {
        return 1;
    }

    a = packet_create(0, "a", 2);
    b = packet_create(1, "b", 2);
    if (a == NULL || b == NULL) {
        packet_free(a);
        packet_free(b);
        mixed_queue_destroy(&mq);
        return 1;
    }

    if (mixed_queue_push(&mq, &a) != MQ_OK || a != NULL) {
        packet_free(a);
        packet_free(b);
        mixed_queue_destroy(&mq);
        return 1;
    }

    if (mixed_queue_push(&mq, &b) != MQ_OK || b != NULL) {
        packet_free(b);
        mixed_queue_destroy(&mq);
        return 1;
    }

    out = NULL;
    if (mixed_queue_pop(&mq, &out) != MQ_OK || out == NULL || out->flow_id != 0) {
        packet_free(out);
        mixed_queue_destroy(&mq);
        return 1;
    }
    packet_free(out);

    out = NULL;
    if (mixed_queue_pop(&mq, &out) != MQ_OK || out == NULL || out->flow_id != 1) {
        packet_free(out);
        mixed_queue_destroy(&mq);
        return 1;
    }
    packet_free(out);

    mixed_queue_destroy(&mq);
    return 0;
}

/* ---- flow_manager integration ---- */

typedef struct {
    FlowManager *mgr;
    uint32_t     flow_id;
    int          count;
    int          interval_ms;
    int          errors;
} MgrProducerCtx;

static void *mgr_producer_thread(void *arg)
{
    MgrProducerCtx *ctx = arg;
    struct timespec delay = { .tv_sec = 0, .tv_nsec = 0 };

    delay.tv_nsec = (long)ctx->interval_ms * 1000000L;

    for (int i = 0; i < ctx->count; i++) {
        DataPacket *pkt;
        char payload[32];
        int n;

        n = snprintf(payload, sizeof(payload), "f%u-%d", ctx->flow_id, i);
        pkt = packet_create(ctx->flow_id, payload, (size_t)n + 1);
        if (pkt == NULL) {
            ctx->errors++;
            return NULL;
        }

        if (flow_manager_push(ctx->mgr, &pkt) != FM_OK) {
            packet_free(pkt);
            ctx->errors++;
            return NULL;
        }

        if (i + 1 < ctx->count && delay.tv_nsec > 0) {
            time_utils_sleep_for(&delay);
        }
    }

    return NULL;
}

static int test_flow_manager_e2e(void)
{
    FlowManager mgr;
    FlowManagerConfig cfg = {
        .max_flows = 2,
        .per_flow_queue_capacity = 8,
        .mixed_queue_capacity = 16,
        .default_output_fd = -1,
        .output_fds = NULL,
        .encode_scratch_cap = 0
    };
    int fds[2];
    char buf0[64];
    char buf1[64];
    ssize_t n;
    pthread_t t0;
    pthread_t t1;
    MgrProducerCtx p0;
    MgrProducerCtx p1;

    fds[0] = open("/tmp/mfrc_flow0.test", O_RDWR | O_CREAT | O_TRUNC, 0600);
    fds[1] = open("/tmp/mfrc_flow1.test", O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fds[0] < 0 || fds[1] < 0) {
        close(fds[0]);
        close(fds[1]);
        return 1;
    }

    cfg.output_fds = fds;

    if (flow_manager_init(&mgr, &cfg) != FM_OK) {
        close(fds[0]);
        close(fds[1]);
        return 1;
    }

    if (flow_manager_start(&mgr) != FM_OK) {
        flow_manager_destroy(&mgr);
        close(fds[0]);
        close(fds[1]);
        return 1;
    }

    flow_context_set_pacing(&mgr.flows[0], 0);
    flow_context_set_pacing(&mgr.flows[1], 0);

    p0 = (MgrProducerCtx){ .mgr = &mgr, .flow_id = 0, .count = 4, .interval_ms = 20, .errors = 0 };
    p1 = (MgrProducerCtx){ .mgr = &mgr, .flow_id = 1, .count = 4, .interval_ms = 20, .errors = 0 };

    if (pthread_create(&t0, NULL, mgr_producer_thread, &p0) != 0 ||
        pthread_create(&t1, NULL, mgr_producer_thread, &p1) != 0) {
        flow_manager_stop(&mgr);
        flow_manager_destroy(&mgr);
        close(fds[0]);
        close(fds[1]);
        return 1;
    }

    pthread_join(t0, NULL);
    pthread_join(t1, NULL);

    flow_manager_stop(&mgr);

    if (p0.errors != 0 || p1.errors != 0) {
        flow_manager_destroy(&mgr);
        close(fds[0]);
        close(fds[1]);
        unlink("/tmp/mfrc_flow0.test");
        unlink("/tmp/mfrc_flow1.test");
        return 1;
    }

    if (atomic_load(&mgr.flows[0].metrics.enqueued_packets) != 4 ||
        atomic_load(&mgr.flows[1].metrics.enqueued_packets) != 4 ||
        atomic_load(&mgr.flows[0].metrics.dequeued_packets) != 4 ||
        atomic_load(&mgr.flows[1].metrics.dequeued_packets) != 4) {
        flow_manager_destroy(&mgr);
        close(fds[0]);
        close(fds[1]);
        unlink("/tmp/mfrc_flow0.test");
        unlink("/tmp/mfrc_flow1.test");
        return 1;
    }

    lseek(fds[0], 0, SEEK_SET);
    n = read(fds[0], buf0, sizeof(buf0) - 1);
    if (n <= 0) {
        flow_manager_destroy(&mgr);
        close(fds[0]);
        close(fds[1]);
        unlink("/tmp/mfrc_flow0.test");
        unlink("/tmp/mfrc_flow1.test");
        return 1;
    }
    buf0[n] = '\0';

    lseek(fds[1], 0, SEEK_SET);
    n = read(fds[1], buf1, sizeof(buf1) - 1);
    if (n <= 0) {
        flow_manager_destroy(&mgr);
        close(fds[0]);
        close(fds[1]);
        unlink("/tmp/mfrc_flow0.test");
        unlink("/tmp/mfrc_flow1.test");
        return 1;
    }
    buf1[n] = '\0';

    flow_manager_destroy(&mgr);
    close(fds[0]);
    close(fds[1]);
    unlink("/tmp/mfrc_flow0.test");
    unlink("/tmp/mfrc_flow1.test");

    if (strstr(buf0, "f0-0") == NULL || strstr(buf1, "f1-0") == NULL) {
        return 1;
    }

    return 0;
}

static int test_flow_manager_invalid_route(void)
{
    FlowManager mgr;
    FlowManagerConfig cfg = {
        .max_flows = 1,
        .per_flow_queue_capacity = 4,
        .mixed_queue_capacity = 4,
        .default_output_fd = -1,
        .output_fds = NULL,
        .encode_scratch_cap = 0
    };
    DataPacket *pkt;
    int fd = open("/tmp/mfrc_invalid.test", O_RDWR | O_CREAT | O_TRUNC, 0600);

    if (fd < 0) {
        return 1;
    }
    cfg.default_output_fd = fd;

    if (flow_manager_init(&mgr, &cfg) != FM_OK) {
        close(fd);
        unlink("/tmp/mfrc_invalid.test");
        return 1;
    }

    if (flow_manager_start(&mgr) != FM_OK) {
        flow_manager_destroy(&mgr);
        close(fd);
        unlink("/tmp/mfrc_invalid.test");
        return 1;
    }

    pkt = packet_create(99, "bad", 4);
    if (pkt == NULL) {
        flow_manager_stop(&mgr);
        flow_manager_destroy(&mgr);
        close(fd);
        unlink("/tmp/mfrc_invalid.test");
        return 1;
    }

    if (flow_manager_push(&mgr, &pkt) != FM_OK) {
        packet_free(pkt);
        flow_manager_stop(&mgr);
        flow_manager_destroy(&mgr);
        close(fd);
        unlink("/tmp/mfrc_invalid.test");
        return 1;
    }

    {
        struct timespec delay = { .tv_sec = 0, .tv_nsec = 100000000L };
        nanosleep(&delay, NULL);
    }

    flow_manager_stop(&mgr);

    if (mgr.route_errors != 1) {
        flow_manager_destroy(&mgr);
        close(fd);
        unlink("/tmp/mfrc_invalid.test");
        return 1;
    }

    flow_manager_destroy(&mgr);
    close(fd);
    unlink("/tmp/mfrc_invalid.test");
    return 0;
}

int main(void)
{
    if (test_packet_create_free() != 0) {
        fprintf(stderr, "test_packet_create_free failed\n");
        return 1;
    }
    if (test_time_utils_sub_add() != 0) {
        fprintf(stderr, "test_time_utils_sub_add failed\n");
        return 1;
    }
    if (test_time_utils_sleep_for() != 0) {
        fprintf(stderr, "test_time_utils_sleep_for failed\n");
        return 1;
    }
    if (test_flow_buffer_shutdown() != 0) {
        fprintf(stderr, "test_flow_buffer_shutdown failed\n");
        return 1;
    }
    if (test_flow_buffer_roundtrip() != 0) {
        fprintf(stderr, "test_flow_buffer_roundtrip failed\n");
        return 1;
    }
    if (test_mixed_queue_push_pop() != 0) {
        fprintf(stderr, "test_mixed_queue_push_pop failed\n");
        return 1;
    }
    if (test_flow_manager_invalid_route() != 0) {
        fprintf(stderr, "test_flow_manager_invalid_route failed\n");
        return 1;
    }
    if (test_flow_manager_e2e() != 0) {
        fprintf(stderr, "test_flow_manager_e2e failed\n");
        return 1;
    }

    printf("all tests passed\n");
    return 0;
}
