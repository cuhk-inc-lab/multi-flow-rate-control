#include "pipeline.h"

#include "block_codec.h"
#include "buffer_transfer.h"
#include "codec.h"
#include "file_drain.h"
#include "stream_config.h"

#include "circular_buffer.h"
#include "flow_context.h"
#include "flow_manager.h"
#include "ingress_push.h"

#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct FlowStage {
    uint32_t        flow_id;
    FILE           *input_fp;
    char            input_path[512];
    FileDrain       drain;
    CircularBuffer *post_multi_in;
    CircularBuffer *sending_out;
    CircularBuffer *receiver_in;
    CircularBuffer *receiver_out;
    int             pipefd[2];
    bool            ingest_done;
    uint64_t        packets_pushed;
} FlowStage;

static WgPipelineStatus passthrough_tail(CircularBuffer *src, CircularBuffer *dst)
{
    unsigned char *tmp;
    size_t         n;
    CB_Status      st;

    if (src == NULL || dst == NULL || Buffer_IsEmpty(src)) {
        return WG_PIPE_OK;
    }

    n = src->size;
    tmp = malloc(n);
    if (tmp == NULL) {
        return WG_PIPE_ERR;
    }

    st = Buffer_Read(src, tmp, n);
    if (st != CB_OK) {
        free(tmp);
        return WG_PIPE_ERR;
    }

    st = Buffer_Write(dst, tmp, n);
    free(tmp);
    if (st != CB_OK) {
        return WG_PIPE_ERR;
    }

    return WG_PIPE_OK;
}

static int drain_pipe_to_buffer(int fd, CircularBuffer *dst)
{
    unsigned char  buf[PKG_SIZE];
    ssize_t        n;
    int            flags;
    int            saved_flags = -1;
    int            progress = 0;

    if (fd < 0 || dst == NULL) {
        return 0;
    }

    flags = fcntl(fd, F_GETFL);
    if (flags >= 0) {
        saved_flags = flags;
        (void)fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    for (;;) {
        do {
            n = read(fd, buf, sizeof(buf));
        } while (n < 0 && errno == EINTR);

        if (n > 0) {
            if (Buffer_Write(dst, buf, (size_t)n) != CB_OK) {
                if (saved_flags >= 0) {
                    (void)fcntl(fd, F_SETFL, saved_flags);
                }
                return -1;
            }
            progress = 1;
            continue;
        }

        if (n == 0 || (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))) {
            break;
        }

        if (saved_flags >= 0) {
            (void)fcntl(fd, F_SETFL, saved_flags);
        }
        return -1;
    }

    if (saved_flags >= 0) {
        (void)fcntl(fd, F_SETFL, saved_flags);
    }

    return progress;
}

static WgPipelineStatus init_flow_stage(FlowStage *stage, const WgFlowPath *path)
{
    if (stage == NULL || path == NULL || path->input_path == NULL ||
        path->output_path == NULL) {
        return WG_PIPE_ERR;
    }

    memset(stage, 0, sizeof(*stage));
    stage->flow_id = path->flow_id;
    stage->pipefd[0] = -1;
    stage->pipefd[1] = -1;

    strncpy(stage->input_path, path->input_path, sizeof(stage->input_path) - 1u);
    stage->input_path[sizeof(stage->input_path) - 1u] = '\0';

    if (pipe(stage->pipefd) != 0) {
        return WG_PIPE_ERR;
    }

    stage->input_fp = fopen(path->input_path, "rb");
    if (stage->input_fp == NULL) {
        return WG_PIPE_ERR;
    }

    if (Buffer_Init(&stage->post_multi_in, BUFFER_BLOCK_COUNT, BUFFER_BLOCK_SIZE,
                    BUFFER_OVERFLOW_POLICY) != CB_OK ||
        Buffer_Init(&stage->sending_out, BUFFER_BLOCK_COUNT, BUFFER_BLOCK_SIZE,
                    BUFFER_OVERFLOW_POLICY) != CB_OK ||
        Buffer_Init(&stage->receiver_in, BUFFER_BLOCK_COUNT, BUFFER_BLOCK_SIZE,
                    BUFFER_OVERFLOW_POLICY) != CB_OK ||
        Buffer_Init(&stage->receiver_out, BUFFER_BLOCK_COUNT, BUFFER_BLOCK_SIZE,
                    BUFFER_OVERFLOW_POLICY) != CB_OK) {
        return WG_PIPE_ERR;
    }

    if (FileDrain_open(&stage->drain, path->output_path) != DRAIN_OK) {
        return WG_PIPE_ERR;
    }

    return WG_PIPE_OK;
}

static void destroy_flow_stage(FlowStage *stage)
{
    if (stage == NULL) {
        return;
    }

    if (stage->input_fp != NULL) {
        fclose(stage->input_fp);
        stage->input_fp = NULL;
    }

    FileDrain_close(&stage->drain);
    Buffer_Destroy(&stage->post_multi_in);
    Buffer_Destroy(&stage->sending_out);
    Buffer_Destroy(&stage->receiver_in);
    Buffer_Destroy(&stage->receiver_out);

    if (stage->pipefd[0] >= 0) {
        close(stage->pipefd[0]);
        stage->pipefd[0] = -1;
    }
    if (stage->pipefd[1] >= 0) {
        close(stage->pipefd[1]);
        stage->pipefd[1] = -1;
    }
}

static int flow_queues_drained(const FlowManager *mgr, uint32_t flow_id)
{
    if (mgr == NULL || flow_id >= mgr->config.max_flows) {
        return 0;
    }

    return mixed_queue_is_empty(&mgr->mixed) &&
           flow_buffer_is_empty(&mgr->flows[flow_id].queue);
}

static int flow_stage_quiescent(const FlowStage *st, const FlowManager *mgr)
{
    uint64_t enq;
    uint64_t deq;

    if (st == NULL || !st->ingest_done) {
        return 0;
    }

    enq = atomic_load(&mgr->flows[st->flow_id].metrics.enqueued_packets);
    deq = atomic_load(&mgr->flows[st->flow_id].metrics.dequeued_packets);

    if (enq < st->packets_pushed || deq < st->packets_pushed) {
        return 0;
    }

    return Buffer_IsEmpty(st->post_multi_in) &&
           Buffer_IsEmpty(st->sending_out) &&
           Buffer_IsEmpty(st->receiver_in) &&
           Buffer_IsEmpty(st->receiver_out) &&
           flow_queues_drained(mgr, st->flow_id);
}

static WgPipelineStatus pump_file_ingress(FlowStage *st, FlowManager *mgr)
{
    unsigned char buf[PKG_SIZE];
    size_t        n;

    if (st == NULL || st->input_fp == NULL) {
        return WG_PIPE_ERR;
    }

    n = fread(buf, 1, PKG_SIZE, st->input_fp);
    if (n == 0) {
        if (feof(st->input_fp)) {
            st->ingest_done = true;
            return WG_PIPE_OK;
        }
        return WG_PIPE_ERR;
    }

    if (ingress_push(mgr, st->flow_id, buf, n) != INGRESS_PUSH_OK) {
        return WG_PIPE_ERR;
    }

    st->packets_pushed++;
    return WG_PIPE_OK;
}

static WgPipelineStatus process_flow_post_multi(FlowStage *st, const Codec *codec,
                                                unsigned char *work, int *progress)
{
    while (st->post_multi_in->size >= DECODE_BLOCK) {
        if (Buffer_Read(st->post_multi_in, work, DECODE_BLOCK) != CB_OK) {
            return WG_PIPE_ERR;
        }
        Codec_encode(codec, work, ENCODE_BLOCK);
        if (Buffer_Write(st->sending_out, work, ENCODE_BLOCK) != CB_OK) {
            return WG_PIPE_ERR;
        }
        if (progress != NULL) {
            *progress = 1;
        }
    }

    for (;;) {
        size_t         moved = 0;
        TransferStatus xfer;

        xfer = BufferTransfer_pump(st->sending_out, st->receiver_in,
                                   ENCODE_BLOCK, &moved);
        if (xfer == TRANSFER_OK) {
            if (progress != NULL) {
                *progress = 1;
            }
            continue;
        }
        if (xfer == TRANSFER_SRC_EMPTY || xfer == TRANSFER_DST_FULL) {
            break;
        }
        return WG_PIPE_ERR;
    }

    while (st->receiver_in->size >= ENCODE_BLOCK) {
        if (Buffer_Read(st->receiver_in, work, ENCODE_BLOCK) != CB_OK) {
            return WG_PIPE_ERR;
        }
        Codec_decode(codec, work, ENCODE_BLOCK);
        if (Buffer_Write(st->receiver_out, work, DECODE_BLOCK) != CB_OK) {
            return WG_PIPE_ERR;
        }
        if (progress != NULL) {
            *progress = 1;
        }
    }

    if (st->ingest_done && st->post_multi_in->size > 0 &&
        st->post_multi_in->size < DECODE_BLOCK) {
        if (passthrough_tail(st->post_multi_in, st->receiver_out) != WG_PIPE_OK) {
            return WG_PIPE_ERR;
        }
        if (progress != NULL) {
            *progress = 1;
        }
    }

    if (st->ingest_done && st->sending_out->size > 0 &&
        st->sending_out->size < ENCODE_BLOCK) {
        if (passthrough_tail(st->sending_out, st->receiver_out) != WG_PIPE_OK) {
            return WG_PIPE_ERR;
        }
        if (progress != NULL) {
            *progress = 1;
        }
    }

    if (st->ingest_done && st->receiver_in->size > 0 &&
        st->receiver_in->size < ENCODE_BLOCK) {
        if (passthrough_tail(st->receiver_in, st->receiver_out) != WG_PIPE_OK) {
            return WG_PIPE_ERR;
        }
        if (progress != NULL) {
            *progress = 1;
        }
    }

    for (;;) {
        DrainStatus drain_st;

        drain_st = FileDrain_pull_once(&st->drain, st->receiver_out, PKG_SIZE, NULL);
        if (drain_st == DRAIN_OK) {
            if (progress != NULL) {
                *progress = 1;
            }
            continue;
        }
        if (drain_st == DRAIN_EMPTY) {
            break;
        }
        return WG_PIPE_ERR;
    }

    return WG_PIPE_OK;
}

static WgPipelineStatus flush_flow_tails(FlowStage *st, const Codec *codec)
{
    unsigned char work[ENCODE_BLOCK];
    int           progress = 0;

    if (st == NULL) {
        return WG_PIPE_ERR;
    }

    for (;;) {
        if (drain_pipe_to_buffer(st->pipefd[0], st->post_multi_in) <= 0) {
            break;
        }
    }

    return process_flow_post_multi(st, codec, work, &progress);
}

WgPipelineStatus wg_pipeline_run(const WgPipelineConfig *config)
{
    FlowManager       mgr;
    FlowManagerConfig mgr_cfg;
    FlowStage        *stages = NULL;
    uint32_t          max_flow_id = 0;
    uint32_t          i;
    const Codec      *codec = BlockCodec_get();
    WgPipelineStatus  status = WG_PIPE_OK;
    unsigned char     work[ENCODE_BLOCK];

    if (config == NULL || config->flows == NULL || config->flow_count == 0) {
        return WG_PIPE_ERR;
    }

    stages = calloc(config->flow_count, sizeof(*stages));
    if (stages == NULL) {
        return WG_PIPE_ERR;
    }

    for (i = 0; i < config->flow_count; i++) {
        if (config->flows[i].flow_id > max_flow_id) {
            max_flow_id = config->flows[i].flow_id;
        }
    }

    for (i = 0; i < config->flow_count; i++) {
        if (init_flow_stage(&stages[i], &config->flows[i]) != WG_PIPE_OK) {
            status = WG_PIPE_ERR;
            goto cleanup;
        }
    }

    mgr_cfg = (FlowManagerConfig){
        .max_flows = max_flow_id + 1u,
        .per_flow_queue_capacity = MF_QUEUE_CAPACITY,
        .mixed_queue_capacity = MF_MIXED_CAPACITY,
        .default_output_fd = -1,
        .output_fds = NULL,
        .encode_scratch_cap = 0
    };

    if (flow_manager_init(&mgr, &mgr_cfg) != FM_OK) {
        status = WG_PIPE_ERR;
        goto cleanup;
    }

    for (i = 0; i < config->flow_count; i++) {
        uint32_t fid = config->flows[i].flow_id;

        mgr.flows[fid].output_fd = stages[i].pipefd[1];
        flow_context_set_pacing(&mgr.flows[fid], config->pacing_enabled);
    }

    if (flow_manager_start(&mgr) != FM_OK) {
        status = WG_PIPE_ERR;
        goto cleanup_mgr;
    }

    for (;;) {
        int progress = 0;

        for (i = 0; i < config->flow_count; i++) {
            FlowStage *st = &stages[i];
            int        dr;

            dr = drain_pipe_to_buffer(st->pipefd[0], st->post_multi_in);
            if (dr > 0) {
                progress = 1;
            } else if (dr < 0) {
                status = WG_PIPE_ERR;
                goto cleanup_running;
            }

            if (process_flow_post_multi(st, codec, work, &progress) != WG_PIPE_OK) {
                status = WG_PIPE_ERR;
                goto cleanup_running;
            }

            if (!st->ingest_done) {
                WgPipelineStatus ingest_st = pump_file_ingress(st, &mgr);

                if (ingest_st == WG_PIPE_OK) {
                    progress = 1;
                } else {
                    status = WG_PIPE_ERR;
                    goto cleanup_running;
                }
            }
        }

        {
            bool all_done = true;

            for (i = 0; i < config->flow_count; i++) {
                if (!flow_stage_quiescent(&stages[i], &mgr)) {
                    all_done = false;
                    break;
                }
            }

            if (all_done) {
                break;
            }
        }

        for (i = 0; i < config->flow_count; i++) {
            uint32_t fid = config->flows[i].flow_id;

            (void)flow_metrics_tick(&mgr.flows[fid].metrics, 5.0);
        }

        if (!progress) {
            struct timespec delay = { .tv_sec = 0, .tv_nsec = 1000000L };

            nanosleep(&delay, NULL);
        }
    }

    for (i = 0; i < config->flow_count; i++) {
        if (flush_flow_tails(&stages[i], codec) != WG_PIPE_OK) {
            status = WG_PIPE_ERR;
            goto cleanup_running;
        }
    }

cleanup_running:
    for (i = 0; i < config->flow_count; i++) {
        uint32_t fid = config->flows[i].flow_id;

        fprintf(stderr,
                "flow %u: enq_bps=%.0f deq_bps=%.0f packets=%llu pushed=%llu\n",
                fid,
                flow_metrics_get_enqueue_bps(&mgr.flows[fid].metrics),
                flow_metrics_get_dequeue_bps(&mgr.flows[fid].metrics),
                (unsigned long long)atomic_load(
                    &mgr.flows[fid].metrics.dequeued_packets),
                (unsigned long long)stages[i].packets_pushed);
    }
    flow_manager_stop(&mgr);

cleanup_mgr:
    flow_manager_destroy(&mgr);

cleanup:
    if (stages != NULL) {
        for (i = 0; i < config->flow_count; i++) {
            destroy_flow_stage(&stages[i]);
        }
        free(stages);
    }

    return status;
}
