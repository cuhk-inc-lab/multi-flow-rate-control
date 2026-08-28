#include "egress_queue.h"

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

static EgressPacket make_pkt(uint8_t marker)
{
    EgressPacket pkt;

    memset(&pkt, 0, sizeof(pkt));
    pkt.datagram = malloc(16);
    EXPECT(pkt.datagram != NULL);
    memset(pkt.datagram, marker, 16);
    pkt.len = 16;
    pkt.flow_id = marker;
    return pkt;
}

static void test_timed_enqueue_immediate(void)
{
    EgressQueue q;
    EgressPacket pkt;
    EgressQueueStats st;
    EgressStatus rc;

    EXPECT(egress_queue_init(&q, 4) == EGRESS_OK);
    pkt = make_pkt(1);
    rc = egress_queue_timed_enqueue(&q, &pkt, 10);
    EXPECT(rc == EGRESS_OK);
    EXPECT(pkt.datagram == NULL);
    egress_queue_stats_snapshot(&q, &st);
    EXPECT(st.enqueue_immediate == 1);
    EXPECT(st.enqueue_waited == 0);
    EXPECT(st.high_watermark == 1);
    egress_queue_destroy(&q);
}

typedef struct {
    EgressQueue *q;
    EgressPacket pkt;
    EgressStatus result;
    int done;
    pthread_mutex_t mu;
    pthread_cond_t cv;
} TimedProducerArgs;

static void *timed_producer_main(void *arg)
{
    TimedProducerArgs *args = arg;

    args->result = egress_queue_timed_enqueue(args->q, &args->pkt, 200);
    pthread_mutex_lock(&args->mu);
    args->done = 1;
    pthread_cond_broadcast(&args->cv);
    pthread_mutex_unlock(&args->mu);
    return NULL;
}

static void test_timed_enqueue_waits_and_wakes(void)
{
    EgressQueue q;
    EgressPacket p1;
    EgressPacket p2;
    TimedProducerArgs args;
    pthread_t th;
    EgressPacket out;
    EgressQueueStats st;

    EXPECT(egress_queue_init(&q, 2) == EGRESS_OK);
    p1 = make_pkt(1);
    p2 = make_pkt(2);
    EXPECT(egress_queue_try_enqueue(&q, &p1) == EGRESS_OK);
    EXPECT(egress_queue_try_enqueue(&q, &p2) == EGRESS_OK);

    memset(&args, 0, sizeof(args));
    args.q = &q;
    args.pkt = make_pkt(4);
    pthread_mutex_init(&args.mu, NULL);
    pthread_cond_init(&args.cv, NULL);

    EXPECT(pthread_create(&th, NULL, timed_producer_main, &args) == 0);
    usleep(5000);
    EXPECT(args.done == 0);
    EXPECT(egress_queue_dequeue(&q, &out) == EGRESS_OK);
    free(out.datagram);

    pthread_mutex_lock(&args.mu);
    while (!args.done) {
        pthread_cond_wait(&args.cv, &args.mu);
    }
    pthread_mutex_unlock(&args.mu);
    EXPECT(args.result == EGRESS_OK);
    EXPECT(args.pkt.datagram == NULL);
    EXPECT(pthread_join(th, NULL) == 0);

    egress_queue_stats_snapshot(&q, &st);
    EXPECT(st.enqueue_waited >= 1);
    EXPECT(st.wait_ns_total > 0);

    pthread_mutex_destroy(&args.mu);
    pthread_cond_destroy(&args.cv);
    egress_queue_destroy(&q);
}

static void test_timed_enqueue_timeout_ownership(void)
{
    EgressQueue q;
    EgressPacket p1;
    EgressPacket p2;
    EgressPacket blocked;
    EgressStatus rc;

    EXPECT(egress_queue_init(&q, 2) == EGRESS_OK);
    p1 = make_pkt(1);
    p2 = make_pkt(2);
    blocked = make_pkt(99);
    EXPECT(egress_queue_try_enqueue(&q, &p1) == EGRESS_OK);
    EXPECT(egress_queue_try_enqueue(&q, &p2) == EGRESS_OK);

    rc = egress_queue_timed_enqueue(&q, &blocked, 5);
    EXPECT(rc == EGRESS_ERR_TIMEOUT);
    EXPECT(blocked.datagram != NULL);
    EXPECT(blocked.datagram[0] == 99);
    free(blocked.datagram);
    blocked.datagram = NULL;

    EXPECT(egress_queue_count(&q) == 2);
    egress_queue_destroy(&q);
}

typedef struct {
    EgressQueue *q;
    EgressStatus result;
    int done;
    pthread_mutex_t mu;
    pthread_cond_t cv;
} ShutdownWaiterArgs;

static void *shutdown_waiter_main(void *arg)
{
    ShutdownWaiterArgs *args = arg;
    EgressPacket pkt = make_pkt(7);

    args->result = egress_queue_timed_enqueue(args->q, &pkt, 5000);
    free(pkt.datagram);
    pkt.datagram = NULL;
    pthread_mutex_lock(&args->mu);
    args->done = 1;
    pthread_cond_broadcast(&args->cv);
    pthread_mutex_unlock(&args->mu);
    return NULL;
}

static void test_timed_enqueue_shutdown_wakeup(void)
{
    EgressQueue q;
    EgressPacket p1;
    ShutdownWaiterArgs args;
    pthread_t th;

    EXPECT(egress_queue_init(&q, 1) == EGRESS_OK);
    p1 = make_pkt(1);
    EXPECT(egress_queue_try_enqueue(&q, &p1) == EGRESS_OK);

    memset(&args, 0, sizeof(args));
    args.q = &q;
    pthread_mutex_init(&args.mu, NULL);
    pthread_cond_init(&args.cv, NULL);
    EXPECT(pthread_create(&th, NULL, shutdown_waiter_main, &args) == 0);
    usleep(2000);
    egress_queue_shutdown(&q);

    pthread_mutex_lock(&args.mu);
    while (!args.done) {
        pthread_cond_wait(&args.cv, &args.mu);
    }
    pthread_mutex_unlock(&args.mu);
    EXPECT(args.result == EGRESS_ERR_SHUTDOWN);
    EXPECT(pthread_join(th, NULL) == 0);

    pthread_mutex_destroy(&args.mu);
    pthread_cond_destroy(&args.cv);
    egress_queue_destroy(&q);
}

static void test_fifo_order(void)
{
    EgressQueue q;
    EgressPacket packets[3];
    EgressPacket out;
    int i;

    EXPECT(egress_queue_init(&q, 8) == EGRESS_OK);
    for (i = 0; i < 3; i++) {
        packets[i] = make_pkt((uint8_t)(10 + i));
        EXPECT(egress_queue_timed_enqueue(&q, &packets[i], 10) == EGRESS_OK);
    }
    for (i = 0; i < 3; i++) {
        EXPECT(egress_queue_dequeue(&q, &out) == EGRESS_OK);
        EXPECT(out.datagram[0] == (uint8_t)(10 + i));
        free(out.datagram);
    }
    egress_queue_destroy(&q);
}

static void test_high_watermark(void)
{
    EgressQueue q;
    EgressPacket p1;
    EgressPacket p2;
    EgressQueueStats st;

    EXPECT(egress_queue_init(&q, 2) == EGRESS_OK);
    p1 = make_pkt(1);
    p2 = make_pkt(2);
    EXPECT(egress_queue_try_enqueue(&q, &p1) == EGRESS_OK);
    EXPECT(egress_queue_try_enqueue(&q, &p2) == EGRESS_OK);
    egress_queue_stats_snapshot(&q, &st);
    EXPECT(st.high_watermark == 2);
    egress_queue_destroy(&q);
}

static void test_fair_ack_priority(void)
{
    EgressQueue ack;
    EgressQueue data;
    EgressFairDequeuer d;
    EgressPacket pkt;
    EgressPacket out;

    EXPECT(egress_queue_init(&ack, 4) == EGRESS_OK);
    EXPECT(egress_queue_init(&data, 4) == EGRESS_OK);
    EXPECT(egress_fair_dequeuer_init(&d, &ack, &data, 8) == EGRESS_OK);
    pkt = make_pkt(20);
    EXPECT(egress_queue_try_enqueue(&data, &pkt) == EGRESS_OK);
    pkt = make_pkt(10);
    EXPECT(egress_queue_try_enqueue(&ack, &pkt) == EGRESS_OK);

    EXPECT(egress_fair_dequeue(&d, &out) == EGRESS_OK);
    EXPECT(out.datagram[0] == 10);
    free(out.datagram);
    EXPECT(egress_fair_dequeue(&d, &out) == EGRESS_OK);
    EXPECT(out.datagram[0] == 20);
    free(out.datagram);

    egress_queue_shutdown(&ack);
    egress_queue_shutdown(&data);
    egress_fair_dequeuer_destroy(&d);
    egress_queue_destroy(&ack);
    egress_queue_destroy(&data);
}

static void test_fair_quota_prevents_data_starvation(void)
{
    EgressQueue ack;
    EgressQueue data;
    EgressFairDequeuer d;
    EgressPacket pkt;
    EgressPacket out;
    int i;

    EXPECT(egress_queue_init(&ack, 16) == EGRESS_OK);
    EXPECT(egress_queue_init(&data, 4) == EGRESS_OK);
    EXPECT(egress_fair_dequeuer_init(&d, &ack, &data, 8) == EGRESS_OK);
    for (i = 0; i < 9; i++) {
        pkt = make_pkt((uint8_t)(10 + i));
        EXPECT(egress_queue_try_enqueue(&ack, &pkt) == EGRESS_OK);
    }
    pkt = make_pkt(99);
    EXPECT(egress_queue_try_enqueue(&data, &pkt) == EGRESS_OK);

    for (i = 0; i < 8; i++) {
        EXPECT(egress_fair_dequeue(&d, &out) == EGRESS_OK);
        EXPECT(out.datagram[0] == (uint8_t)(10 + i));
        free(out.datagram);
    }
    EXPECT(egress_fair_dequeue(&d, &out) == EGRESS_OK);
    EXPECT(out.datagram[0] == 99);
    free(out.datagram);
    EXPECT(egress_fair_dequeue(&d, &out) == EGRESS_OK);
    EXPECT(out.datagram[0] == 18);
    free(out.datagram);

    egress_queue_shutdown(&ack);
    egress_queue_shutdown(&data);
    egress_fair_dequeuer_destroy(&d);
    egress_queue_destroy(&ack);
    egress_queue_destroy(&data);
}

static void test_fair_single_lane_and_ownership(void)
{
    EgressQueue ack;
    EgressQueue data;
    EgressFairDequeuer d;
    EgressPacket pkt;
    EgressPacket out;
    uint8_t *owned;

    EXPECT(egress_queue_init(&ack, 4) == EGRESS_OK);
    EXPECT(egress_queue_init(&data, 4) == EGRESS_OK);
    EXPECT(egress_fair_dequeuer_init(&d, &ack, &data, 8) == EGRESS_OK);

    pkt = make_pkt(31);
    owned = pkt.datagram;
    EXPECT(egress_queue_try_enqueue(&ack, &pkt) == EGRESS_OK);
    EXPECT(pkt.datagram == NULL);
    EXPECT(egress_fair_dequeue(&d, &out) == EGRESS_OK);
    EXPECT(out.datagram == owned);
    free(out.datagram);

    pkt = make_pkt(41);
    owned = pkt.datagram;
    EXPECT(egress_queue_try_enqueue(&data, &pkt) == EGRESS_OK);
    EXPECT(pkt.datagram == NULL);
    EXPECT(egress_fair_dequeue(&d, &out) == EGRESS_OK);
    EXPECT(out.datagram == owned);
    free(out.datagram);

    egress_queue_shutdown(&ack);
    egress_queue_shutdown(&data);
    EXPECT(egress_fair_dequeue(&d, &out) == EGRESS_ERR_SHUTDOWN);
    egress_fair_dequeuer_destroy(&d);
    egress_queue_destroy(&ack);
    egress_queue_destroy(&data);
}

typedef struct {
    EgressFairDequeuer *d;
    EgressStatus result;
} FairWaiterArgs;

static void *fair_waiter_main(void *arg)
{
    FairWaiterArgs *args = arg;
    EgressPacket out;

    args->result = egress_fair_dequeue(args->d, &out);
    if (args->result == EGRESS_OK) {
        free(out.datagram);
    }
    return NULL;
}

static void test_fair_shutdown_wakes_empty_waiter(void)
{
    EgressQueue ack;
    EgressQueue data;
    EgressFairDequeuer d;
    FairWaiterArgs args;
    pthread_t th;

    EXPECT(egress_queue_init(&ack, 2) == EGRESS_OK);
    EXPECT(egress_queue_init(&data, 2) == EGRESS_OK);
    EXPECT(egress_fair_dequeuer_init(&d, &ack, &data, 8) == EGRESS_OK);
    memset(&args, 0, sizeof(args));
    args.d = &d;
    EXPECT(pthread_create(&th, NULL, fair_waiter_main, &args) == 0);
    usleep(2000);
    egress_queue_shutdown(&ack);
    usleep(2000);
    egress_queue_shutdown(&data);
    EXPECT(pthread_join(th, NULL) == 0);
    EXPECT(args.result == EGRESS_ERR_SHUTDOWN);

    egress_fair_dequeuer_destroy(&d);
    egress_queue_destroy(&ack);
    egress_queue_destroy(&data);
}

static void test_fair_enqueue_wakes_empty_waiter(void)
{
    EgressQueue ack;
    EgressQueue data;
    EgressFairDequeuer d;
    FairWaiterArgs args;
    EgressPacket pkt;
    pthread_t th;

    EXPECT(egress_queue_init(&ack, 2) == EGRESS_OK);
    EXPECT(egress_queue_init(&data, 2) == EGRESS_OK);
    EXPECT(egress_fair_dequeuer_init(&d, &ack, &data, 8) == EGRESS_OK);
    memset(&args, 0, sizeof(args));
    args.d = &d;
    EXPECT(pthread_create(&th, NULL, fair_waiter_main, &args) == 0);
    usleep(2000);
    pkt = make_pkt(55);
    EXPECT(egress_queue_try_enqueue(&data, &pkt) == EGRESS_OK);
    EXPECT(pthread_join(th, NULL) == 0);
    EXPECT(args.result == EGRESS_OK);

    egress_queue_shutdown(&ack);
    egress_queue_shutdown(&data);
    egress_fair_dequeuer_destroy(&d);
    egress_queue_destroy(&ack);
    egress_queue_destroy(&data);
}

int main(void)
{
    test_timed_enqueue_immediate();
    test_timed_enqueue_waits_and_wakes();
    test_timed_enqueue_timeout_ownership();
    test_timed_enqueue_shutdown_wakeup();
    test_fifo_order();
    test_high_watermark();
    test_fair_ack_priority();
    test_fair_quota_prevents_data_starvation();
    test_fair_single_lane_and_ownership();
    test_fair_shutdown_wakes_empty_waiter();
    test_fair_enqueue_wakes_empty_waiter();

    if (g_failures != 0) {
        fprintf(stderr, "%d failure(s)\n", g_failures);
        return 1;
    }
    fprintf(stderr, "relay_egress_queue_tests: ok\n");
    return 0;
}
