/*
 * Demo: two flows, synthetic producer, per-flow output files.
 *
 * Usage: ./build/multi_flow_demo [output_dir]
 * Default output_dir: /tmp/mfrc_demo
 */

#include "flow_context.h"
#include "flow_manager.h"
#include "packet.h"
#include "time_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>

#define FLOWS       2
#define PKTS_EACH   8
#define INTERVAL_MS 50
#define METRICS_WINDOW_SEC 5.0

typedef struct {
    FlowManager *mgr;
    uint32_t     flow_id;
} DemoProducerCtx;

typedef struct {
    FlowManager  *mgr;
    _Atomic int   running;
} DemoMetricsCtx;

static void *demo_metrics_thread(void *arg)
{
    DemoMetricsCtx *ctx = arg;
    struct timespec delay = { .tv_sec = 0, .tv_nsec = 200000000L };

    while (atomic_load(&ctx->running)) {
        for (uint32_t i = 0; i < FLOWS; i++) {
            (void)flow_metrics_tick(&ctx->mgr->flows[i].metrics, METRICS_WINDOW_SEC);
        }
        time_utils_sleep_for(&delay);
    }

    return NULL;
}

static void *demo_producer(void *arg)
{
    DemoProducerCtx *ctx = arg;
    struct timespec delay = { .tv_sec = 0, .tv_nsec = (long)INTERVAL_MS * 1000000L };
    char payload[64];

    for (int i = 0; i < PKTS_EACH; i++) {
        DataPacket *pkt;
        int n;

        n = snprintf(payload, sizeof(payload),
                     "flow=%u seq=%d\n", ctx->flow_id, i);
        pkt = packet_create(ctx->flow_id, payload, (size_t)n);
        if (pkt == NULL) {
            return NULL;
        }

        if (flow_manager_push(ctx->mgr, &pkt) != FM_OK) {
            packet_free(pkt);
            return NULL;
        }

        if (i + 1 < PKTS_EACH) {
            time_utils_sleep_for(&delay);
        }
    }

    return NULL;
}

static int open_flow_output(const char *dir, uint32_t flow_id)
{
    char path[256];
    int fd;

    snprintf(path, sizeof(path), "%s/flow_%u.out", dir, flow_id);
    fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    return fd;
}

int main(int argc, char **argv)
{
    const char *out_dir = "/tmp/mfrc_demo";
    FlowManager mgr;
    FlowManagerConfig cfg;
    int fds[FLOWS];
    pthread_t threads[FLOWS];
    pthread_t metrics_thread;
    DemoProducerCtx ctx[FLOWS];
    DemoMetricsCtx mctx;
    uint32_t i;

    if (argc > 1) {
        out_dir = argv[1];
    }

    if (mkdir(out_dir, 0755) != 0 && errno != EEXIST) {
        perror("mkdir");
        return 1;
    }

    for (i = 0; i < FLOWS; i++) {
        fds[i] = open_flow_output(out_dir, i);
        if (fds[i] < 0) {
            perror("open flow output");
            while (i > 0) {
                i--;
                close(fds[i]);
            }
            return 1;
        }
    }

    cfg = (FlowManagerConfig){
        .max_flows = FLOWS,
        .per_flow_queue_capacity = 16,
        .mixed_queue_capacity = 32,
        .default_output_fd = STDOUT_FILENO,
        .output_fds = fds,
        .encode_scratch_cap = 0
    };

    if (flow_manager_init(&mgr, &cfg) != FM_OK) {
        fprintf(stderr, "flow_manager_init failed\n");
        for (i = 0; i < FLOWS; i++) {
            close(fds[i]);
        }
        return 1;
    }

    if (flow_manager_start(&mgr) != FM_OK) {
        fprintf(stderr, "flow_manager_start failed\n");
        flow_manager_destroy(&mgr);
        for (i = 0; i < FLOWS; i++) {
            close(fds[i]);
        }
        return 1;
    }

    mctx.mgr = &mgr;
    atomic_store(&mctx.running, 1);
    if (pthread_create(&metrics_thread, NULL, demo_metrics_thread, &mctx) != 0) {
        flow_manager_stop(&mgr);
        flow_manager_destroy(&mgr);
        for (i = 0; i < FLOWS; i++) {
            close(fds[i]);
        }
        return 1;
    }

    for (i = 0; i < FLOWS; i++) {
        ctx[i].mgr = &mgr;
        ctx[i].flow_id = i;
        if (pthread_create(&threads[i], NULL, demo_producer, &ctx[i]) != 0) {
            flow_manager_stop(&mgr);
            flow_manager_destroy(&mgr);
            for (uint32_t j = 0; j < FLOWS; j++) {
                close(fds[j]);
            }
            return 1;
        }
    }

    for (i = 0; i < FLOWS; i++) {
        pthread_join(threads[i], NULL);
    }

    atomic_store(&mctx.running, 0);
    pthread_join(metrics_thread, NULL);

    flow_manager_stop(&mgr);

    printf("Demo complete. Output directory: %s\n", out_dir);
    for (i = 0; i < FLOWS; i++) {
        printf("  flow %u: enqueued=%llu dequeued=%llu enq_bytes=%llu deq_bytes=%llu "
               "sleeps=%llu enq_bps=%.0f deq_bps=%.0f\n",
               i,
               (unsigned long long)atomic_load(&mgr.flows[i].metrics.enqueued_packets),
               (unsigned long long)atomic_load(&mgr.flows[i].metrics.dequeued_packets),
               (unsigned long long)atomic_load(&mgr.flows[i].metrics.enqueued_bytes),
               (unsigned long long)atomic_load(&mgr.flows[i].metrics.dequeued_bytes),
               (unsigned long long)atomic_load(
                   &mgr.flows[i].metrics.pacing_sleeps),
               flow_metrics_get_enqueue_bps(&mgr.flows[i].metrics),
               flow_metrics_get_dequeue_bps(&mgr.flows[i].metrics));
        close(fds[i]);
    }

    flow_manager_destroy(&mgr);
    return 0;
}
