#include "flow_worker.h"

#include "fd_sink.h"
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

void *flow_worker_thread(void *arg)
{
    FlowContext *ctx = arg;

    if (ctx == NULL) {
        return NULL;
    }

    while (1) {
        DataPacket *pkt = NULL;
        FlowBufferStatus st;
        struct timespec delta;
        ssize_t written = 0;

        st = flow_buffer_dequeue(&ctx->queue, &pkt, NULL);
        if (st == FB_ERR_SHUTDOWN) {
            break;
        }
        if (st != FB_OK || pkt == NULL) {
            continue;
        }

        if (ctx->pacing.has_last_ts) {
            if (time_utils_ts_sub(&pkt->enqueue_ts,
                                  &ctx->pacing.last_pkt_ts,
                                  &delta) == TU_OK) {
                time_utils_sleep_for(&delta);
                ctx->metrics.pacing_sleeps++;
            }
        }

        if (emit_packet(ctx, pkt, &written) != FW_OK) {
            packet_free(pkt);
            continue;
        }

        ctx->metrics.dequeued++;
        if (written > 0) {
            ctx->metrics.bytes_written += (uint64_t)written;
        }

        ctx->pacing.last_pkt_ts = pkt->enqueue_ts;
        ctx->pacing.has_last_ts = 1;

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
