#include "flow_manager.h"
#include "flow_peer_map.h"
#include "ingress_push.h"
#include "packet.h"
#include "time_utils.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <math.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
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

/* ---- pacing timeline ---- */

#define PACE_PKTS        5
#define PACE_PAYLOAD     16
#define PACE_INTERVAL_MS 100

typedef struct {
    int              fd;
    struct timespec  times[PACE_PKTS];
    int              count;
    int              errors;
} PaceReaderCtx;

static void *pace_reader_thread(void *arg)
{
    PaceReaderCtx *ctx = arg;
    unsigned char  buf[PACE_PAYLOAD];

    while (ctx->count < PACE_PKTS) {
        ssize_t got = 0;

        while (got < PACE_PAYLOAD) {
            ssize_t n = read(ctx->fd, buf + got, (size_t)(PACE_PAYLOAD - got));

            if (n <= 0) {
                ctx->errors++;
                return NULL;
            }
            got += n;
        }

        if (time_utils_now_mono(&ctx->times[ctx->count]) != TU_OK) {
            ctx->errors++;
            return NULL;
        }
        ctx->count++;
    }

    return NULL;
}

static int pacing_interval_ok(const struct timespec *a, const struct timespec *b)
{
    struct timespec delta;
    long ms;

    if (time_utils_ts_sub(b, a, &delta) != TU_OK) {
        return 0;
    }

    ms = delta.tv_sec * 1000L + delta.tv_nsec / 1000000L;
    return ms >= (PACE_INTERVAL_MS - 35) && ms <= (PACE_INTERVAL_MS + 50);
}

static int test_pacing_timeline(void)
{
    FlowManager mgr;
    FlowManagerConfig cfg = {
        .max_flows = 1,
        .per_flow_queue_capacity = 8,
        .mixed_queue_capacity = 16,
        .default_output_fd = -1,
        .output_fds = NULL,
        .encode_scratch_cap = 0
    };
    int pipefd[2];
    int null_fd = open("/dev/null", O_WRONLY);
    int fds[1];
    pthread_t reader;
    PaceReaderCtx rctx = { .fd = -1, .count = 0, .errors = 0 };
    struct timespec delay = { .tv_sec = 0,
                              .tv_nsec = (long)PACE_INTERVAL_MS * 1000000L };
    unsigned char payload[PACE_PAYLOAD];

    memset(payload, 'x', sizeof(payload));

    if (null_fd < 0 || pipe(pipefd) != 0) {
        close(null_fd);
        return 1;
    }

    fds[0] = pipefd[1];
    cfg.output_fds = fds;

    if (flow_manager_init(&mgr, &cfg) != FM_OK) {
        close(pipefd[0]);
        close(pipefd[1]);
        close(null_fd);
        return 1;
    }

    flow_context_set_pacing(&mgr.flows[0], 1);

    if (flow_manager_start(&mgr) != FM_OK) {
        flow_manager_destroy(&mgr);
        close(pipefd[0]);
        close(pipefd[1]);
        close(null_fd);
        return 1;
    }

    rctx.fd = pipefd[0];
    if (pthread_create(&reader, NULL, pace_reader_thread, &rctx) != 0) {
        flow_manager_stop(&mgr);
        flow_manager_destroy(&mgr);
        close(pipefd[0]);
        close(pipefd[1]);
        close(null_fd);
        return 1;
    }

    for (int i = 0; i < PACE_PKTS; i++) {
        DataPacket *pkt = packet_create(0, payload, PACE_PAYLOAD);

        if (pkt == NULL) {
            flow_manager_stop(&mgr);
            pthread_join(reader, NULL);
            flow_manager_destroy(&mgr);
            close(pipefd[0]);
            close(pipefd[1]);
            close(null_fd);
            return 1;
        }

        if (flow_manager_push(&mgr, &pkt) != FM_OK) {
            packet_free(pkt);
            flow_manager_stop(&mgr);
            pthread_join(reader, NULL);
            flow_manager_destroy(&mgr);
            close(pipefd[0]);
            close(pipefd[1]);
            close(null_fd);
            return 1;
        }

        if (i + 1 < PACE_PKTS) {
            time_utils_sleep_for(&delay);
        }
    }

    pthread_join(reader, NULL);
    flow_manager_stop(&mgr);
    flow_manager_destroy(&mgr);
    close(pipefd[0]);
    close(pipefd[1]);
    close(null_fd);

    if (rctx.errors != 0 || rctx.count != PACE_PKTS) {
        return 1;
    }

    for (int i = 1; i < PACE_PKTS; i++) {
        if (!pacing_interval_ok(&rctx.times[i - 1], &rctx.times[i])) {
            return 1;
        }
    }

    return 0;
}

/* ---- ±1% bps over 5s window ---- */

#define RATE_WINDOW_SEC   5.0
#define RATE_RUN_SEC      6
#define RATE_INTERVAL_MS  20
#define RATE_PAYLOAD      100
#define RATE_COUNT        ((RATE_RUN_SEC * 1000) / RATE_INTERVAL_MS)

static void *rate_producer_thread(void *arg)
{
    MgrProducerCtx *ctx = arg;
    struct timespec delay = { .tv_sec = 0,
                              .tv_nsec = (long)RATE_INTERVAL_MS * 1000000L };
    unsigned char payload[RATE_PAYLOAD];

    memset(payload, 'r', sizeof(payload));

    for (int i = 0; i < RATE_COUNT; i++) {
        DataPacket *pkt = packet_create(ctx->flow_id, payload, RATE_PAYLOAD);

        if (pkt == NULL) {
            ctx->errors++;
            return NULL;
        }

        if (flow_manager_push(ctx->mgr, &pkt) != FM_OK) {
            packet_free(pkt);
            ctx->errors++;
            return NULL;
        }

        if (i + 1 < RATE_COUNT) {
            time_utils_sleep_for(&delay);
        }
    }

    return NULL;
}

static int test_rate_match_5s(void)
{
    FlowManager mgr;
    FlowManagerConfig cfg = {
        .max_flows = 1,
        .per_flow_queue_capacity = 32,
        .mixed_queue_capacity = 64,
        .default_output_fd = -1,
        .output_fds = NULL,
        .encode_scratch_cap = 0
    };
    int null_fd = open("/dev/null", O_WRONLY);
    int fds[1];
    pthread_t prod;
    MgrProducerCtx pctx;
    int got_window = 0;
    double last_enq = 0.0;
    double last_deq = 0.0;

    if (null_fd < 0) {
        return 1;
    }

    fds[0] = null_fd;
    cfg.output_fds = fds;

    if (flow_manager_init(&mgr, &cfg) != FM_OK) {
        close(null_fd);
        return 1;
    }

    flow_context_set_pacing(&mgr.flows[0], 1);

    if (flow_manager_start(&mgr) != FM_OK) {
        flow_manager_destroy(&mgr);
        close(null_fd);
        return 1;
    }

    pctx = (MgrProducerCtx){
        .mgr = &mgr, .flow_id = 0, .count = RATE_COUNT,
        .interval_ms = RATE_INTERVAL_MS, .errors = 0
    };

    if (pthread_create(&prod, NULL, rate_producer_thread, &pctx) != 0) {
        flow_manager_stop(&mgr);
        flow_manager_destroy(&mgr);
        close(null_fd);
        return 1;
    }

    while (atomic_load(&mgr.flows[0].metrics.dequeued_packets) < (uint64_t)RATE_COUNT) {
        if (flow_metrics_tick(&mgr.flows[0].metrics, RATE_WINDOW_SEC)) {
            last_enq = flow_metrics_get_enqueue_bps(&mgr.flows[0].metrics);
            last_deq = flow_metrics_get_dequeue_bps(&mgr.flows[0].metrics);
            if (last_enq > 0.0) {
                got_window = 1;
            }
        }

        {
            struct timespec tick_delay = { .tv_sec = 0, .tv_nsec = 50000000L };
            nanosleep(&tick_delay, NULL);
        }
    }

    pthread_join(prod, NULL);

    while (!flow_metrics_tick(&mgr.flows[0].metrics, RATE_WINDOW_SEC)) {
        struct timespec tick_delay = { .tv_sec = 0, .tv_nsec = 10000000L };
        nanosleep(&tick_delay, NULL);
    }
    last_enq = flow_metrics_get_enqueue_bps(&mgr.flows[0].metrics);
    last_deq = flow_metrics_get_dequeue_bps(&mgr.flows[0].metrics);
    if (last_enq > 0.0) {
        got_window = 1;
    }

    {
        uint64_t enq_bytes =
            atomic_load(&mgr.flows[0].metrics.enqueued_bytes);
        uint64_t deq_bytes =
            atomic_load(&mgr.flows[0].metrics.dequeued_bytes);

        flow_manager_stop(&mgr);
        flow_manager_destroy(&mgr);
        close(null_fd);

        if (pctx.errors != 0 || !got_window) {
            return 1;
        }

        if (enq_bytes != deq_bytes) {
            return 1;
        }

        if (last_enq <= 0.0) {
            return 1;
        }

        if (fabs(last_deq - last_enq) / last_enq > 0.01) {
            return 1;
        }
    }

    return 0;
}

/* ---- dispatcher HOL avoidance ---- */

static int test_dispatcher_avoids_hol(void)
{
    FlowManager mgr;
    FlowManagerConfig cfg = {
        .max_flows = 2,
        .per_flow_queue_capacity = 1,
        .mixed_queue_capacity = 16,
        .default_output_fd = -1,
        .output_fds = NULL,
        .encode_scratch_cap = 0
    };
    int null_fd = open("/dev/null", O_WRONLY);
    int fds[2];
    unsigned char payload[8];

    memset(payload, 'a', sizeof(payload));

    if (null_fd < 0) {
        return 1;
    }

    fds[0] = null_fd;
    fds[1] = null_fd;
    cfg.output_fds = fds;

    if (flow_manager_init(&mgr, &cfg) != FM_OK) {
        close(null_fd);
        return 1;
    }

    flow_context_set_pacing(&mgr.flows[0], 0);
    flow_context_set_pacing(&mgr.flows[1], 0);

    if (flow_manager_start(&mgr) != FM_OK) {
        flow_manager_destroy(&mgr);
        close(null_fd);
        return 1;
    }

    for (int i = 0; i < 3; i++) {
        DataPacket *pkt = packet_create(0, payload, sizeof(payload));

        if (pkt == NULL || flow_manager_push(&mgr, &pkt) != FM_OK) {
            packet_free(pkt);
            flow_manager_stop(&mgr);
            flow_manager_destroy(&mgr);
            close(null_fd);
            return 1;
        }
    }

    {
        DataPacket *pkt = packet_create(1, payload, sizeof(payload));

        if (pkt == NULL || flow_manager_push(&mgr, &pkt) != FM_OK) {
            packet_free(pkt);
            flow_manager_stop(&mgr);
            flow_manager_destroy(&mgr);
            close(null_fd);
            return 1;
        }
    }

    {
        struct timespec wait = { .tv_sec = 0, .tv_nsec = 50000000L };
        nanosleep(&wait, NULL);
    }

    if (atomic_load(&mgr.flows[1].metrics.dequeued_packets) < 1) {
        flow_manager_stop(&mgr);
        flow_manager_destroy(&mgr);
        close(null_fd);
        return 1;
    }

    flow_manager_stop(&mgr);
    flow_manager_destroy(&mgr);
    close(null_fd);
    return 0;
}

/* ---- complex multi-flow: uneven rates, sizes, concurrent producers ---- */

typedef struct {
    FlowManager *mgr;
    uint32_t     flow_id;
    int          count;
    int          interval_ms;
    size_t       payload_len;
    int          errors;
} ComplexProducerCtx;

static void *complex_producer_thread(void *arg)
{
    ComplexProducerCtx *ctx = arg;
    struct timespec delay = { .tv_sec = 0, .tv_nsec = 0 };
    unsigned char *payload;

    delay.tv_nsec = (long)ctx->interval_ms * 1000000L;
    payload = malloc(ctx->payload_len);
    if (payload == NULL) {
        ctx->errors++;
        return NULL;
    }

    memset(payload, (int)('a' + (char)ctx->flow_id), ctx->payload_len);

    for (int i = 0; i < ctx->count; i++) {
        DataPacket *pkt = packet_create(ctx->flow_id, payload, ctx->payload_len);

        if (pkt == NULL) {
            ctx->errors++;
            free(payload);
            return NULL;
        }

        if (flow_manager_push(ctx->mgr, &pkt) != FM_OK) {
            packet_free(pkt);
            ctx->errors++;
            free(payload);
            return NULL;
        }

        if (i + 1 < ctx->count && delay.tv_nsec > 0) {
            time_utils_sleep_for(&delay);
        }
    }

    free(payload);
    return NULL;
}

static int test_complex_multi_flow(void)
{
    FlowManager mgr;
    FlowManagerConfig cfg = {
        .max_flows = 2,
        .per_flow_queue_capacity = 16,
        .mixed_queue_capacity = 64,
        .default_output_fd = -1,
        .output_fds = NULL,
        .encode_scratch_cap = 0
    };
    int null_fd = open("/dev/null", O_WRONLY);
    int fds[2];
    pthread_t t0;
    pthread_t t1;
    ComplexProducerCtx p0;
    ComplexProducerCtx p1;
    const int count0 = 40;
    const int count1 = 28;

    if (null_fd < 0) {
        return 1;
    }

    fds[0] = null_fd;
    fds[1] = null_fd;
    cfg.output_fds = fds;

    if (flow_manager_init(&mgr, &cfg) != FM_OK) {
        close(null_fd);
        return 1;
    }

    flow_context_set_pacing(&mgr.flows[0], 1);
    flow_context_set_pacing(&mgr.flows[1], 1);

    if (flow_manager_start(&mgr) != FM_OK) {
        flow_manager_destroy(&mgr);
        close(null_fd);
        return 1;
    }

    p0 = (ComplexProducerCtx){
        .mgr = &mgr, .flow_id = 0, .count = count0,
        .interval_ms = 25, .payload_len = 96, .errors = 0
    };
    p1 = (ComplexProducerCtx){
        .mgr = &mgr, .flow_id = 1, .count = count1,
        .interval_ms = 40, .payload_len = 160, .errors = 0
    };

    if (pthread_create(&t0, NULL, complex_producer_thread, &p0) != 0 ||
        pthread_create(&t1, NULL, complex_producer_thread, &p1) != 0) {
        flow_manager_stop(&mgr);
        flow_manager_destroy(&mgr);
        close(null_fd);
        return 1;
    }

    pthread_join(t0, NULL);
    pthread_join(t1, NULL);

    {
        uint64_t enq0 = atomic_load(&mgr.flows[0].metrics.enqueued_packets);
        uint64_t deq0 = atomic_load(&mgr.flows[0].metrics.dequeued_packets);
        uint64_t enq1 = atomic_load(&mgr.flows[1].metrics.enqueued_packets);
        uint64_t deq1 = atomic_load(&mgr.flows[1].metrics.dequeued_packets);
        uint64_t enq_bytes0 = atomic_load(&mgr.flows[0].metrics.enqueued_bytes);
        uint64_t deq_bytes0 = atomic_load(&mgr.flows[0].metrics.dequeued_bytes);
        uint64_t enq_bytes1 = atomic_load(&mgr.flows[1].metrics.enqueued_bytes);
        uint64_t deq_bytes1 = atomic_load(&mgr.flows[1].metrics.dequeued_bytes);

        flow_manager_stop(&mgr);
        flow_manager_destroy(&mgr);
        close(null_fd);

        if (p0.errors != 0 || p1.errors != 0) {
            return 1;
        }
        if (enq0 != (uint64_t)count0 || deq0 != (uint64_t)count0 ||
            enq1 != (uint64_t)count1 || deq1 != (uint64_t)count1) {
            return 1;
        }
        if (enq_bytes0 != deq_bytes0 || enq_bytes1 != deq_bytes1) {
            return 1;
        }
        if (enq_bytes0 != (uint64_t)count0 * 96u ||
            enq_bytes1 != (uint64_t)count1 * 160u) {
            return 1;
        }
    }

    return 0;
}

/* ---- ingress / peer map ---- */

static int test_flow_peer_map(void)
{
    FlowPeerMap       *map = NULL;
    struct sockaddr_in src_a;
    struct sockaddr_in src_b;
    struct sockaddr_in dst0;
    struct sockaddr_in dst1;
    FlowTuple          tuple;
    uint32_t           f0;
    uint32_t           f1;
    uint32_t           f2;

    if (flow_peer_map_init(&map, 4) != FPM_OK) {
        return 1;
    }

    memset(&dst0, 0, sizeof(dst0));
    dst0.sin_family = AF_INET;
    dst0.sin_port = htons(5000);
    if (inet_pton(AF_INET, "127.0.0.1", &dst0.sin_addr) != 1) {
        flow_peer_map_destroy(map);
        return 1;
    }

    memset(&dst1, 0, sizeof(dst1));
    dst1.sin_family = AF_INET;
    dst1.sin_port = htons(5001);
    if (inet_pton(AF_INET, "127.0.0.1", &dst1.sin_addr) != 1) {
        flow_peer_map_destroy(map);
        return 1;
    }

    memset(&src_a, 0, sizeof(src_a));
    src_a.sin_family = AF_INET;
    src_a.sin_port = htons(4000);
    if (inet_pton(AF_INET, "10.0.0.1", &src_a.sin_addr) != 1) {
        flow_peer_map_destroy(map);
        return 1;
    }

    memset(&src_b, 0, sizeof(src_b));
    src_b.sin_family = AF_INET;
    src_b.sin_port = htons(4000);
    if (inet_pton(AF_INET, "10.0.0.2", &src_b.sin_addr) != 1) {
        flow_peer_map_destroy(map);
        return 1;
    }

    if (flow_tuple_set(&tuple,
                       (struct sockaddr *)&src_a, sizeof(src_a),
                       (struct sockaddr *)&dst0, sizeof(dst0),
                       IPPROTO_UDP) != 0) {
        flow_peer_map_destroy(map);
        return 1;
    }

    f0 = flow_peer_map_lookup(map, &tuple);
    if (f0 != 0) {
        flow_peer_map_destroy(map);
        return 1;
    }

    if (flow_peer_map_lookup(map, &tuple) != 0) {
        flow_peer_map_destroy(map);
        return 1;
    }

    if (flow_tuple_set(&tuple,
                       (struct sockaddr *)&src_b, sizeof(src_b),
                       (struct sockaddr *)&dst0, sizeof(dst0),
                       IPPROTO_UDP) != 0) {
        flow_peer_map_destroy(map);
        return 1;
    }

    f1 = flow_peer_map_lookup(map, &tuple);
    if (f1 != 1) {
        flow_peer_map_destroy(map);
        return 1;
    }

    if (flow_tuple_set(&tuple,
                       (struct sockaddr *)&src_a, sizeof(src_a),
                       (struct sockaddr *)&dst1, sizeof(dst1),
                       IPPROTO_UDP) != 0) {
        flow_peer_map_destroy(map);
        return 1;
    }

    f2 = flow_peer_map_lookup(map, &tuple);
    if (f2 != 2) {
        flow_peer_map_destroy(map);
        return 1;
    }

    flow_peer_map_destroy(map);
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
    if (test_pacing_timeline() != 0) {
        fprintf(stderr, "test_pacing_timeline failed\n");
        return 1;
    }
    if (test_rate_match_5s() != 0) {
        fprintf(stderr, "test_rate_match_5s failed\n");
        return 1;
    }
    if (test_dispatcher_avoids_hol() != 0) {
        fprintf(stderr, "test_dispatcher_avoids_hol failed\n");
        return 1;
    }
    if (test_complex_multi_flow() != 0) {
        fprintf(stderr, "test_complex_multi_flow failed\n");
        return 1;
    }
    if (test_flow_peer_map() != 0) {
        fprintf(stderr, "test_flow_peer_map failed\n");
        return 1;
    }

    printf("all tests passed\n");
    return 0;
}
