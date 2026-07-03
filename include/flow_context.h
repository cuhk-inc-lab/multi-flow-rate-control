#ifndef FLOW_CONTEXT_H
#define FLOW_CONTEXT_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <sys/types.h>
#include <pthread.h>

#include "flow_buffer.h"
#include "packet.h"

typedef struct FlowManager FlowManager;

typedef ssize_t (*PacketEncodeFn)(const DataPacket *in,
                                  unsigned char *out,
                                  size_t out_cap,
                                  void *ctx);

typedef struct FlowMetrics {
    _Atomic uint64_t enqueued_packets;
    _Atomic uint64_t dequeued_packets;
    _Atomic uint64_t enqueued_bytes;
    _Atomic uint64_t dequeued_bytes;
    uint64_t         pacing_sleeps;

    pthread_mutex_t  bps_mutex;
    double           calculated_enqueue_bps;
    double           calculated_dequeue_bps;
    struct timespec  bps_window_start;
    uint64_t         bps_window_enq_bytes;
    uint64_t         bps_window_deq_bytes;
} FlowMetrics;

typedef struct FlowPacingState {
    struct timespec stream_start_enqueue;
    struct timespec stream_start_dequeue;
    int             has_stream_start;
} FlowPacingState;

typedef enum {
    FC_OK = 0,
    FC_ERR_INVALID = -1,
    FC_ERR_ALLOC = -2
} FlowContextStatus;

typedef struct FlowContext {
    uint32_t            flow_id;
    FlowCircularBuffer  queue;
    FlowMetrics         metrics;
    FlowPacingState     pacing;
    int                 output_fd;
    int                 pacing_enabled;

    PacketEncodeFn      encode_fn;
    void               *encode_ctx;
    unsigned char      *encode_scratch;
    size_t              encode_scratch_cap;

    FlowManager        *owner;
    pthread_t           worker_thread;
    int                 worker_started;
} FlowContext;

FlowContextStatus flow_context_init(FlowContext *ctx,
                                    uint32_t flow_id,
                                    size_t queue_capacity,
                                    int output_fd,
                                    FlowManager *owner);

void flow_context_set_encoder(FlowContext *ctx,
                              PacketEncodeFn encode_fn,
                              void *encode_ctx,
                              size_t scratch_cap);

void flow_context_set_pacing(FlowContext *ctx, int enabled);

void flow_context_destroy(FlowContext *ctx);

void flow_metrics_record_enqueue(FlowMetrics *metrics, size_t bytes);
void flow_metrics_record_dequeue(FlowMetrics *metrics, size_t bytes);
void flow_metrics_tick(FlowMetrics *metrics, double window_sec);

#endif /* FLOW_CONTEXT_H */
