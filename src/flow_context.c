#include "flow_context.h"

#include "time_utils.h"

#include <stdlib.h>
#include <string.h>

void flow_metrics_record_enqueue(FlowMetrics *metrics, size_t bytes)
{
    if (metrics == NULL) {
        return;
    }

    atomic_fetch_add_explicit(&metrics->enqueued_packets, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&metrics->enqueued_bytes, bytes, memory_order_relaxed);
}

void flow_metrics_record_dequeue(FlowMetrics *metrics, size_t bytes)
{
    if (metrics == NULL) {
        return;
    }

    atomic_fetch_add_explicit(&metrics->dequeued_packets, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&metrics->dequeued_bytes, bytes, memory_order_relaxed);
}

int flow_metrics_tick(FlowMetrics *metrics, double window_sec)
{
    struct timespec now;
    struct timespec elapsed;
    double seconds;
    int updated = 0;

    if (metrics == NULL || window_sec <= 0.0) {
        return 0;
    }

    if (time_utils_now_mono(&now) != TU_OK) {
        return 0;
    }

    pthread_mutex_lock(&metrics->bps_mutex);

    if (metrics->bps_window_start.tv_sec == 0 &&
        metrics->bps_window_start.tv_nsec == 0) {
        metrics->bps_window_start = now;
        metrics->bps_window_enq_bytes =
            atomic_load_explicit(&metrics->enqueued_bytes, memory_order_relaxed);
        metrics->bps_window_deq_bytes =
            atomic_load_explicit(&metrics->dequeued_bytes, memory_order_relaxed);
        pthread_mutex_unlock(&metrics->bps_mutex);
        return 0;
    }

    if (time_utils_ts_sub(&now, &metrics->bps_window_start, &elapsed) != TU_OK) {
        pthread_mutex_unlock(&metrics->bps_mutex);
        return 0;
    }

    seconds = (double)elapsed.tv_sec + (double)elapsed.tv_nsec / 1e9;
    if (seconds < window_sec) {
        pthread_mutex_unlock(&metrics->bps_mutex);
        return 0;
    }

    {
        uint64_t enq_now =
            atomic_load_explicit(&metrics->enqueued_bytes, memory_order_relaxed);
        uint64_t deq_now =
            atomic_load_explicit(&metrics->dequeued_bytes, memory_order_relaxed);

        metrics->calculated_enqueue_bps =
            (double)(enq_now - metrics->bps_window_enq_bytes) * 8.0 / seconds;
        metrics->calculated_dequeue_bps =
            (double)(deq_now - metrics->bps_window_deq_bytes) * 8.0 / seconds;

        metrics->bps_window_start = now;
        metrics->bps_window_enq_bytes = enq_now;
        metrics->bps_window_deq_bytes = deq_now;
        updated = 1;
    }

    pthread_mutex_unlock(&metrics->bps_mutex);
    return updated;
}

double flow_metrics_get_enqueue_bps(const FlowMetrics *metrics)
{
    double bps = 0.0;

    if (metrics == NULL) {
        return 0.0;
    }

    pthread_mutex_lock((pthread_mutex_t *)&metrics->bps_mutex);
    bps = metrics->calculated_enqueue_bps;
    pthread_mutex_unlock((pthread_mutex_t *)&metrics->bps_mutex);

    return bps;
}

double flow_metrics_get_dequeue_bps(const FlowMetrics *metrics)
{
    double bps = 0.0;

    if (metrics == NULL) {
        return 0.0;
    }

    pthread_mutex_lock((pthread_mutex_t *)&metrics->bps_mutex);
    bps = metrics->calculated_dequeue_bps;
    pthread_mutex_unlock((pthread_mutex_t *)&metrics->bps_mutex);

    return bps;
}

FlowContextStatus flow_context_init(FlowContext *ctx,
                                    uint32_t flow_id,
                                    size_t queue_capacity,
                                    int output_fd,
                                    FlowManager *owner)
{
    if (ctx == NULL || owner == NULL || queue_capacity == 0) {
        return FC_ERR_INVALID;
    }

    memset(ctx, 0, sizeof(*ctx));
    ctx->flow_id = flow_id;
    ctx->output_fd = output_fd;
    ctx->owner = owner;
    ctx->pacing_enabled = 1;

    if (pthread_mutex_init(&ctx->metrics.bps_mutex, NULL) != 0) {
        return FC_ERR_ALLOC;
    }

    if (flow_buffer_init(&ctx->queue, queue_capacity) != FB_OK) {
        pthread_mutex_destroy(&ctx->metrics.bps_mutex);
        return FC_ERR_ALLOC;
    }

    return FC_OK;
}

void flow_context_set_encoder(FlowContext *ctx,
                              PacketEncodeFn encode_fn,
                              void *encode_ctx,
                              size_t scratch_cap)
{
    if (ctx == NULL) {
        return;
    }

    free(ctx->encode_scratch);
    ctx->encode_scratch = NULL;
    ctx->encode_scratch_cap = 0;
    ctx->encode_fn = encode_fn;
    ctx->encode_ctx = encode_ctx;

    if (encode_fn == NULL || scratch_cap == 0) {
        return;
    }

    ctx->encode_scratch = malloc(scratch_cap);
    if (ctx->encode_scratch == NULL) {
        ctx->encode_fn = NULL;
        ctx->encode_ctx = NULL;
        return;
    }

    ctx->encode_scratch_cap = scratch_cap;
}

void flow_context_set_pacing(FlowContext *ctx, int enabled)
{
    if (ctx != NULL) {
        ctx->pacing_enabled = enabled ? 1 : 0;
    }
}

void flow_context_destroy(FlowContext *ctx)
{
    if (ctx == NULL) {
        return;
    }

    flow_buffer_destroy(&ctx->queue);
    free(ctx->encode_scratch);
    ctx->encode_scratch = NULL;
    ctx->encode_scratch_cap = 0;
    ctx->encode_fn = NULL;
    ctx->encode_ctx = NULL;
    ctx->worker_started = 0;
    pthread_mutex_destroy(&ctx->metrics.bps_mutex);
}
