#ifndef FLOW_CONTEXT_H
#define FLOW_CONTEXT_H

#include <stddef.h>
#include <stdint.h>
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
    uint64_t enqueued;
    uint64_t dequeued;
    uint64_t blocked_enqueue;
    uint64_t blocked_dequeue;
    uint64_t bytes_written;
    uint64_t pacing_sleeps;
} FlowMetrics;

typedef struct FlowPacingState {
    struct timespec last_pkt_ts;
    int             has_last_ts;
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

void flow_context_destroy(FlowContext *ctx);

#endif /* FLOW_CONTEXT_H */
