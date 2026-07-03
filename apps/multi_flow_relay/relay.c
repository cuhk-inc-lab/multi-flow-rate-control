#include "relay.h"

#include "block_codec.h"
#include "file_drain.h"
#include "file_ingest.h"
#include "stream_config.h"

#include "circular_buffer.h"
#include "flow_context.h"
#include "flow_manager.h"
#include "packet_framer.h"
#include "pipe_io.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct FlowStage {
    uint32_t        flow_id;
    FileIngest      ingest;
    FileDrain       drain;
    CircularBuffer *sending_in;
    CircularBuffer *sending_out;
    CircularBuffer *receiver_in;
    CircularBuffer *receiver_out;
    int             pipefd[2];
    bool            ingest_done;
} FlowStage;

static RelayStatus passthrough_tail(CircularBuffer *src, CircularBuffer *dst)
{
    unsigned char *tmp;
    size_t         n;
    CB_Status      st;

    if (src == NULL || dst == NULL || Buffer_IsEmpty(src)) {
        return RELAY_OK;
    }

    n = src->size;
    tmp = malloc(n);
    if (tmp == NULL) {
        return RELAY_ERR;
    }

    st = Buffer_Read(src, tmp, n);
    if (st != CB_OK) {
        free(tmp);
        return RELAY_ERR;
    }

    st = Buffer_Write(dst, tmp, n);
    free(tmp);
    if (st != CB_OK) {
        return RELAY_ERR;
    }

    return RELAY_OK;
}

static RelayStatus init_flow_stage(FlowStage *stage, const FlowRelayPath *path)
{
    if (stage == NULL || path == NULL) {
        return RELAY_ERR;
    }

    memset(stage, 0, sizeof(*stage));
    stage->flow_id = path->flow_id;

    if (pipe(stage->pipefd) != 0) {
        return RELAY_ERR;
    }

    if (Buffer_Init(&stage->sending_in, BUFFER_BLOCK_COUNT, BUFFER_BLOCK_SIZE,
                    BUFFER_OVERFLOW_POLICY) != CB_OK ||
        Buffer_Init(&stage->sending_out, BUFFER_BLOCK_COUNT, BUFFER_BLOCK_SIZE,
                    BUFFER_OVERFLOW_POLICY) != CB_OK ||
        Buffer_Init(&stage->receiver_in, BUFFER_BLOCK_COUNT, BUFFER_BLOCK_SIZE,
                    BUFFER_OVERFLOW_POLICY) != CB_OK ||
        Buffer_Init(&stage->receiver_out, BUFFER_BLOCK_COUNT, BUFFER_BLOCK_SIZE,
                    BUFFER_OVERFLOW_POLICY) != CB_OK) {
        return RELAY_ERR;
    }

    if (FileIngest_open(&stage->ingest, path->input_path) != INGEST_OK) {
        return RELAY_ERR;
    }

    if (FileDrain_open(&stage->drain, path->output_path) != DRAIN_OK) {
        return RELAY_ERR;
    }

    return RELAY_OK;
}

static void destroy_flow_stage(FlowStage *stage)
{
    if (stage == NULL) {
        return;
    }

    FileIngest_close(&stage->ingest);
    FileDrain_close(&stage->drain);
    Buffer_Destroy(&stage->sending_in);
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

static int flow_stage_quiescent(const FlowStage *st, const FlowManager *mgr,
                                uint64_t expected_through_mgr)
{
    uint64_t enq;
    uint64_t deq;

    if (st == NULL || !st->ingest_done) {
        return 0;
    }

    enq = atomic_load(&mgr->flows[st->flow_id].metrics.enqueued_packets);
    deq = atomic_load(&mgr->flows[st->flow_id].metrics.dequeued_packets);

    if (enq < expected_through_mgr || deq < expected_through_mgr) {
        return 0;
    }

    return Buffer_IsEmpty(st->sending_in) &&
           Buffer_IsEmpty(st->sending_out) &&
           Buffer_IsEmpty(st->receiver_in) &&
           Buffer_IsEmpty(st->receiver_out) &&
           flow_queues_drained(mgr, st->flow_id);
}

static uint64_t expected_mgr_packets(const FlowStage *st)
{
    size_t total;
    uint64_t blocks;

    if (st == NULL || st->sending_in == NULL) {
        return 0;
    }

    total = 0;
    if (st->ingest.fp != NULL) {
        long pos = ftell(st->ingest.fp);
        fseek(st->ingest.fp, 0, SEEK_END);
        total = (size_t)ftell(st->ingest.fp);
        fseek(st->ingest.fp, pos, SEEK_SET);
    }

    blocks = (uint64_t)(total / DECODE_BLOCK);
    return blocks;
}

RelayStatus Relay_run(const RelayConfig *config)
{
    FlowManager       mgr;
    FlowManagerConfig mgr_cfg;
    FlowStage        *stages = NULL;
    uint32_t          max_flow_id = 0;
    uint32_t          i;
    const Codec      *codec = BlockCodec_get();
    RelayStatus       status = RELAY_OK;
    unsigned char     work_enc[ENCODE_BLOCK];
    unsigned char     work_dec[ENCODE_BLOCK];

    if (config == NULL || config->flows == NULL || config->flow_count == 0) {
        return RELAY_ERR;
    }

    stages = calloc(config->flow_count, sizeof(*stages));
    if (stages == NULL) {
        status = RELAY_ERR;
        goto cleanup;
    }

    for (i = 0; i < config->flow_count; i++) {
        if (config->flows[i].flow_id > max_flow_id) {
            max_flow_id = config->flows[i].flow_id;
        }
    }

    for (i = 0; i < config->flow_count; i++) {
        stages[i].pipefd[0] = -1;
        stages[i].pipefd[1] = -1;
        if (init_flow_stage(&stages[i], &config->flows[i]) != RELAY_OK) {
            status = RELAY_ERR;
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
        status = RELAY_ERR;
        goto cleanup;
    }

    for (i = 0; i < config->flow_count; i++) {
        uint32_t fid = config->flows[i].flow_id;
        mgr.flows[fid].output_fd = stages[i].pipefd[1];
        flow_context_set_pacing(&mgr.flows[fid], config->pacing_enabled);
    }

    if (flow_manager_start(&mgr) != FM_OK) {
        status = RELAY_ERR;
        goto cleanup_mgr;
    }

    for (;;) {
        bool progress = false;

        for (i = 0; i < config->flow_count; i++) {
            FlowStage    *st = &stages[i];
            IngestStatus  ingest_st;
            PacketFramerStatus pf_st;
            PipeIoStatus  pipe_st;
            DrainStatus   drain_st;

            if (!st->ingest_done) {
                ingest_st = FileIngest_pump_once(&st->ingest, st->sending_in, NULL);
                if (ingest_st == INGEST_OK) {
                    progress = true;
                } else if (ingest_st == INGEST_EOF) {
                    st->ingest_done = true;
                } else if (ingest_st != INGEST_BUF_FULL) {
                    status = RELAY_ERR;
                    goto cleanup_running;
                }
            }

            while (st->sending_in->size >= DECODE_BLOCK) {
                if (Buffer_Read(st->sending_in, work_enc, DECODE_BLOCK) != CB_OK) {
                    status = RELAY_ERR;
                    goto cleanup_running;
                }
                Codec_encode(codec, work_enc, ENCODE_BLOCK);
                if (Buffer_Write(st->sending_out, work_enc, ENCODE_BLOCK) != CB_OK) {
                    status = RELAY_ERR;
                    goto cleanup_running;
                }

                pf_st = packet_framer_pump(st->sending_out,
                                           &mgr,
                                           st->flow_id,
                                           ENCODE_BLOCK,
                                           1);
                if (pf_st != PF_OK) {
                    status = RELAY_ERR;
                    goto cleanup_running;
                }

                pipe_st = pipe_io_read_to_buffer(st->pipefd[0],
                                                 st->receiver_in,
                                                 ENCODE_BLOCK,
                                                 1);
                if (pipe_st != PIPE_IO_OK) {
                    status = RELAY_ERR;
                    goto cleanup_running;
                }

                if (Buffer_Read(st->receiver_in, work_dec, ENCODE_BLOCK) != CB_OK) {
                    status = RELAY_ERR;
                    goto cleanup_running;
                }
                Codec_decode(codec, work_dec, ENCODE_BLOCK);
                if (Buffer_Write(st->receiver_out, work_dec, DECODE_BLOCK) != CB_OK) {
                    status = RELAY_ERR;
                    goto cleanup_running;
                }

                progress = true;
            }

            if (st->ingest_done && st->sending_in->size > 0 &&
                st->sending_in->size < DECODE_BLOCK) {
                if (passthrough_tail(st->sending_in, st->sending_out) != RELAY_OK) {
                    status = RELAY_ERR;
                    goto cleanup_running;
                }
                progress = true;
            }

            if (st->ingest_done && st->sending_out->size > 0 &&
                st->sending_out->size < ENCODE_BLOCK) {
                if (passthrough_tail(st->sending_out, st->receiver_out) != RELAY_OK) {
                    status = RELAY_ERR;
                    goto cleanup_running;
                }
                progress = true;
            }

            for (;;) {
                drain_st = FileDrain_pull_once(&st->drain, st->receiver_out,
                                               PKG_SIZE, NULL);
                if (drain_st == DRAIN_OK) {
                    progress = true;
                    continue;
                }
                if (drain_st == DRAIN_EMPTY) {
                    break;
                }
                status = RELAY_ERR;
                goto cleanup_running;
            }
        }

        {
            bool all_done = true;
            for (i = 0; i < config->flow_count; i++) {
                if (!flow_stage_quiescent(&stages[i], &mgr,
                                          expected_mgr_packets(&stages[i]))) {
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

cleanup_running:
    for (i = 0; i < config->flow_count; i++) {
        uint32_t fid = config->flows[i].flow_id;
        fprintf(stderr,
                "flow %u: enq_bps=%.0f deq_bps=%.0f packets=%llu\n",
                fid,
                flow_metrics_get_enqueue_bps(&mgr.flows[fid].metrics),
                flow_metrics_get_dequeue_bps(&mgr.flows[fid].metrics),
                (unsigned long long)atomic_load(
                    &mgr.flows[fid].metrics.dequeued_packets));
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
