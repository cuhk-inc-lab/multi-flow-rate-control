#include "flow_worker.h"

#include "fd_sink.h"
#include "flow_manager.h"
#include "time_utils.h"

static FlowWorkerStatus emit_packet(FlowContext *ctx, DataPacket *pkt, ssize_t *written)
{
    ssize_t out_len = 0;
    FdSinkStatus sink_st;

    if (ctx->encode_fn != NULL && ctx->encode_scratch != NULL) {
        out_len = ctx->encode_fn(pkt,
                                 ctx->encode_scratch,
                                 ctx->encode_scratch_cap,
                                 ctx->encode_ctx);
        if (out_len > 0) {
            sink_st = fd_sink_write(ctx->output_fd,
                                    ctx->encode_scratch,
                                    (size_t)out_len,
                                    written);
            return sink_st == FD_SINK_OK ? FW_OK : FW_ERR_SYSTEM;
        }
    }

    sink_st = fd_sink_write_packet(ctx->output_fd, pkt, written);
    return sink_st == FD_SINK_OK ? FW_OK : FW_ERR_SYSTEM;
}

static void pace_packet(FlowContext *ctx, const DataPacket *pkt)
{
    struct timespec now;
    struct timespec producer_delta;
    struct timespec target_dequeue;
    struct timespec sleep_for;

    if (!ctx->pacing_enabled) {
        return;
    }

    if (!ctx->pacing.has_stream_start) {
        if (time_utils_now_mono(&now) != TU_OK) {
            return;
        }
        ctx->pacing.stream_start_enqueue = pkt->enqueue_ts;
        ctx->pacing.stream_start_dequeue = now;
        ctx->pacing.has_stream_start = 1;
        return;
    }

    if (time_utils_ts_sub(&pkt->enqueue_ts,
                          &ctx->pacing.stream_start_enqueue,
                          &producer_delta) != TU_OK) {
        return;
    }

    if (time_utils_ts_add(&ctx->pacing.stream_start_dequeue,
                          &producer_delta,
                          &target_dequeue) != TU_OK) {
        return;
    }

    if (time_utils_now_mono(&now) != TU_OK) {
        return;
    }

    if (!time_utils_ts_before(&now, &target_dequeue)) {
        return;
    }

    if (time_utils_ts_sub(&target_dequeue, &now, &sleep_for) == TU_OK) {
        time_utils_sleep_for(&sleep_for);
        atomic_fetch_add_explicit(&ctx->metrics.pacing_sleeps, 1, memory_order_relaxed);
    }
}

void *flow_worker_thread(void *arg)
{
    FlowContext *ctx = arg;

    if (ctx == NULL) {
        return NULL;
    }

    while (1) {
        DataPacket *pkt = NULL;
        FlowBufferStatus st;
        ssize_t written = 0;
        int woke_from_idle = 0;

        st = flow_buffer_dequeue(&ctx->queue, &pkt, &woke_from_idle);
        if (st == FB_ERR_SHUTDOWN) {
            break;
        }
        if (st != FB_OK || pkt == NULL) {
            continue;
        }

        if (woke_from_idle) {
            ctx->pacing.has_stream_start = 0;
        }

        if (ctx->owner != NULL) {
            flow_manager_dispatch_wake(ctx->owner);
        }

        pace_packet(ctx, pkt);

        if (emit_packet(ctx, pkt, &written) != FW_OK) {
            packet_free(pkt);
            continue;
        }

        flow_metrics_record_dequeue(&ctx->metrics, pkt->payload_len);
        if (written > 0) {
            (void)written;
        }

        packet_free(pkt);
    }

    return NULL;
}

FlowWorkerStatus flow_worker_start(FlowContext *ctx)
{
    if (ctx == NULL || ctx->worker_started) {
        return FW_ERR_INVALID;
    }

    if (pthread_create(&ctx->worker_thread, NULL, flow_worker_thread, ctx) != 0) {
        return FW_ERR_SYSTEM;
    }

    ctx->worker_started = 1;
    return FW_OK;
}

FlowWorkerStatus flow_worker_join(FlowContext *ctx)
{
    if (ctx == NULL || !ctx->worker_started) {
        return FW_ERR_INVALID;
    }

    if (pthread_join(ctx->worker_thread, NULL) != 0) {
        return FW_ERR_SYSTEM;
    }

    ctx->worker_started = 0;
    return FW_OK;
}
