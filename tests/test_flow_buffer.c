#include "flow_buffer.h"
#include "packet.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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

static void *producer_thread(void *arg)
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

static void *consumer_thread(void *arg)
{
    ConsumerCtx *ctx = arg;

    while (ctx->received < PRODUCED) {
        DataPacket *pkt = NULL;
        FlowBufferStatus st;

        st = flow_buffer_dequeue(ctx->fb, &pkt);
        if (st != FB_OK || pkt == NULL) {
            ctx->errors++;
            return NULL;
        }

        if (pkt->payload == NULL || pkt->payload_len == 0) {
            ctx->errors++;
            packet_free(pkt);
            return NULL;
        }

        ctx->received++;
        packet_free(pkt);
    }

    return NULL;
}

static int test_blocking_roundtrip(void)
{
    FlowCircularBuffer fb;
    pthread_t prod, cons;
    ProducerCtx pctx;
    ConsumerCtx cctx;

    if (flow_buffer_init(&fb, QUEUE_CAP) != FB_OK) {
        fprintf(stderr, "flow_buffer_init failed\n");
        return 1;
    }

    pctx = (ProducerCtx){ .fb = &fb, .errors = 0 };
    cctx = (ConsumerCtx){ .fb = &fb, .received = 0, .errors = 0 };

    if (pthread_create(&cons, NULL, consumer_thread, &cctx) != 0) {
        flow_buffer_destroy(&fb);
        return 1;
    }

    {
        struct timespec delay = { .tv_sec = 0, .tv_nsec = 10000000L };
        nanosleep(&delay, NULL);
    }

    if (pthread_create(&prod, NULL, producer_thread, &pctx) != 0) {
        flow_buffer_shutdown(&fb);
        pthread_join(cons, NULL);
        flow_buffer_destroy(&fb);
        return 1;
    }

    pthread_join(prod, NULL);
    pthread_join(cons, NULL);
    flow_buffer_destroy(&fb);

    if (pctx.errors != 0 || cctx.errors != 0 || cctx.received != PRODUCED) {
        fprintf(stderr, "roundtrip failed: prod_err=%d cons_err=%d received=%d\n",
                pctx.errors, cctx.errors, cctx.received);
        return 1;
    }

  return 0;
}

static int test_shutdown_wakeup(void)
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
        fprintf(stderr, "expected FB_ERR_SHUTDOWN on enqueue, got %d\n", st);
        return 1;
    }

    pkt = NULL;
    st = flow_buffer_dequeue(&fb, &pkt);
    if (st != FB_ERR_SHUTDOWN || pkt != NULL) {
        packet_free(pkt);
        flow_buffer_destroy(&fb);
        fprintf(stderr, "expected FB_ERR_SHUTDOWN on dequeue\n");
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

int main(void)
{
    if (test_packet_create_free() != 0) {
        fprintf(stderr, "test_packet_create_free failed\n");
        return 1;
    }
    if (test_shutdown_wakeup() != 0) {
        fprintf(stderr, "test_shutdown_wakeup failed\n");
        return 1;
    }
    if (test_blocking_roundtrip() != 0) {
        fprintf(stderr, "test_blocking_roundtrip failed\n");
        return 1;
    }

    printf("all tests passed\n");
    return 0;
}
