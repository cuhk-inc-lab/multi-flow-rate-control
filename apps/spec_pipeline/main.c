/*
 * Spec-compliant pipeline (Plan A):
 *
 *   file -> CircularBuffer -> packet_framer -> FlowManager -> write(output fd)
 *
 * No encode/decode, no pipe loop. Matches the Technical Specification module
 * boundary when integrated with buffer-management-module CircularBuffer.
 *
 * Usage:
 *   ./build/spec_pipeline [--no-pace] <input.ts> <output.ts>
 *   ./build/spec_pipeline [--no-pace] --multi <in0> <out0> [<in1> <out1> ...]
 */

#include "file_ingest.h"
#include "stream_config.h"

#include "circular_buffer.h"
#include "fd_sink.h"
#include "flow_context.h"
#include "flow_manager.h"
#include "packet_framer.h"

#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
    uint32_t    flow_id;
    const char *input_path;
    const char *output_path;
} SpecPath;

typedef struct {
    uint32_t        flow_id;
    FileIngest      ingest;
    CircularBuffer *ring;
    int             output_fd;
    bool            ingest_done;
} SpecStage;

typedef struct {
    SpecPath  *paths;
    uint32_t   flow_count;
    int        pacing_enabled;
} SpecConfig;

static void print_usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s [--no-pace] <input.ts> <output.ts>\n"
            "  %s [--no-pace] --multi <in0.ts> <out0.ts> [<in1.ts> <out1.ts> ...]\n"
            "\n"
            "Spec pipeline per flow:\n"
            "  file -> CircularBuffer -> packet_framer -> FlowManager -> write(fd)\n"
            "\n"
            "Verify byte-exact (--no-pace): cmp <input.ts> <output.ts>\n",
            prog, prog);
}

static int open_output(const char *path)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);

    if (fd < 0) {
        perror(path);
    }

    return fd;
}

static int init_stage(SpecStage *stage, const SpecPath *path)
{
    if (stage == NULL || path == NULL) {
        return -1;
    }

    memset(stage, 0, sizeof(*stage));
    stage->flow_id = path->flow_id;
    stage->output_fd = -1;

    if (Buffer_Init(&stage->ring, BUFFER_BLOCK_COUNT, BUFFER_BLOCK_SIZE,
                    BUFFER_OVERFLOW_POLICY) != CB_OK) {
        return -1;
    }

    stage->output_fd = open_output(path->output_path);
    if (stage->output_fd < 0) {
        Buffer_Destroy(&stage->ring);
        stage->ring = NULL;
        return -1;
    }

    if (FileIngest_open(&stage->ingest, path->input_path) != INGEST_OK) {
        close(stage->output_fd);
        Buffer_Destroy(&stage->ring);
        stage->output_fd = -1;
        stage->ring = NULL;
        return -1;
    }

    return 0;
}

static void destroy_stage(SpecStage *stage)
{
    if (stage == NULL) {
        return;
    }

    FileIngest_close(&stage->ingest);
    Buffer_Destroy(&stage->ring);
    stage->ring = NULL;

    if (stage->output_fd >= 0) {
        close(stage->output_fd);
        stage->output_fd = -1;
    }
}

static uint64_t count_framer_packets(const SpecStage *stage)
{
    FILE *fp;
    long  size;

    if (stage == NULL || stage->ingest.path[0] == '\0') {
        return 0;
    }

    fp = fopen(stage->ingest.path, "rb");
    if (fp == NULL) {
        return 0;
    }

    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return 0;
    }

    size = ftell(fp);
    fclose(fp);

    if (size < 0) {
        return 0;
    }

    return (uint64_t)size / (uint64_t)PKG_SIZE;
}

static int flow_queues_drained(const FlowManager *mgr, uint32_t flow_id)
{
    if (mgr == NULL || flow_id >= mgr->config.max_flows) {
        return 0;
    }

    return mixed_queue_is_empty(&mgr->mixed) &&
           flow_buffer_is_empty(&mgr->flows[flow_id].queue);
}

static int stage_quiescent(const SpecStage *stage, const FlowManager *mgr,
                           uint64_t expected_packets)
{
    uint64_t enq;
    uint64_t deq;

    if (stage == NULL || mgr == NULL || !stage->ingest_done) {
        return 0;
    }

    enq = atomic_load(&mgr->flows[stage->flow_id].metrics.enqueued_packets);
    deq = atomic_load(&mgr->flows[stage->flow_id].metrics.dequeued_packets);

    if (enq < expected_packets || deq < expected_packets) {
        return 0;
    }

    return Buffer_IsEmpty(stage->ring) && flow_queues_drained(mgr, stage->flow_id);
}

static int flush_ring_tail(SpecStage *stage)
{
    unsigned char *tail;
    size_t         n;
    CB_Status      st;
    ssize_t        written = 0;

    if (stage == NULL || stage->ring == NULL || Buffer_IsEmpty(stage->ring)) {
        return 0;
    }

    n = stage->ring->size;
    tail = malloc(n);
    if (tail == NULL) {
        return -1;
    }

    st = Buffer_Read(stage->ring, tail, n);
    if (st != CB_OK) {
        free(tail);
        return -1;
    }

    if (fd_sink_write(stage->output_fd, tail, n, &written) != FD_SINK_OK) {
        free(tail);
        return -1;
    }

    free(tail);
    return 0;
}

static int run_spec_pipeline(const SpecConfig *config)
{
    FlowManager       mgr;
    FlowManagerConfig mgr_cfg;
    SpecStage        *stages = NULL;
    uint32_t          max_flow_id = 0;
    uint32_t          i;
    int               status = 0;

    if (config == NULL || config->paths == NULL || config->flow_count == 0) {
        return 1;
    }

    stages = calloc(config->flow_count, sizeof(*stages));
    if (stages == NULL) {
        return 1;
    }

    for (i = 0; i < config->flow_count; i++) {
        if (config->paths[i].flow_id > max_flow_id) {
            max_flow_id = config->paths[i].flow_id;
        }
    }

    for (i = 0; i < config->flow_count; i++) {
        if (init_stage(&stages[i], &config->paths[i]) != 0) {
            status = 1;
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
        status = 1;
        goto cleanup;
    }

    for (i = 0; i < config->flow_count; i++) {
        uint32_t fid = config->paths[i].flow_id;

        mgr.flows[fid].output_fd = stages[i].output_fd;
        flow_context_set_pacing(&mgr.flows[fid], config->pacing_enabled);
    }

    if (flow_manager_start(&mgr) != FM_OK) {
        status = 1;
        goto cleanup_mgr;
    }

    for (;;) {
        bool progress = false;

        for (i = 0; i < config->flow_count; i++) {
            SpecStage        *st = &stages[i];
            IngestStatus      ingest_st;
            PacketFramerStatus pf_st;

            if (!st->ingest_done) {
                ingest_st = FileIngest_pump_once(&st->ingest, st->ring, NULL);
                if (ingest_st == INGEST_OK) {
                    progress = true;
                } else if (ingest_st == INGEST_EOF) {
                    st->ingest_done = true;
                } else if (ingest_st != INGEST_BUF_FULL) {
                    status = 1;
                    goto cleanup_running;
                }
            }

            while ((size_t)st->ring->size >= PKG_SIZE) {
                pf_st = packet_framer_pump(st->ring,
                                           &mgr,
                                           st->flow_id,
                                           PKG_SIZE,
                                           1);
                if (pf_st != PF_OK) {
                    status = 1;
                    goto cleanup_running;
                }
                progress = true;
            }
        }

        for (i = 0; i < config->flow_count; i++) {
            uint32_t fid = config->paths[i].flow_id;
            (void)flow_metrics_tick(&mgr.flows[fid].metrics, 5.0);
        }

        {
            bool all_done = true;

            for (i = 0; i < config->flow_count; i++) {
                if (!stage_quiescent(&stages[i],
                                     &mgr,
                                     count_framer_packets(&stages[i]))) {
                    all_done = false;
                    break;
                }
            }

            if (all_done) {
                break;
            }
        }

        if (!progress) {
            struct timespec delay = { .tv_sec = 0, .tv_nsec = 1000000L };
            nanosleep(&delay, NULL);
        }
    }

    for (i = 0; i < config->flow_count; i++) {
        if (flush_ring_tail(&stages[i]) != 0) {
            status = 1;
            goto cleanup_running;
        }
    }

cleanup_running:
    for (i = 0; i < config->flow_count; i++) {
        uint32_t fid = config->paths[i].flow_id;
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
            destroy_stage(&stages[i]);
        }
        free(stages);
    }

    return status;
}

int main(int argc, char **argv)
{
    SpecPath  *paths = NULL;
    SpecConfig cfg;
    int        pacing = 1;
    int        multi = 0;
    int        argi = 1;
    int        pairs;
    int        i;
    int        rc;

    if (argc < 3) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    if (strcmp(argv[argi], "--no-pace") == 0) {
        pacing = 0;
        argi++;
    }

    if (argi < argc && strcmp(argv[argi], "--multi") == 0) {
        multi = 1;
        argi++;
    }

    if (multi) {
        if ((argc - argi) < 2 || ((argc - argi) % 2) != 0) {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        pairs = (argc - argi) / 2;
    } else {
        if (argc - argi != 2) {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        pairs = 1;
    }

    paths = calloc((size_t)pairs, sizeof(*paths));
    if (paths == NULL) {
        return EXIT_FAILURE;
    }

    for (i = 0; i < pairs; i++) {
        paths[i].flow_id = (uint32_t)i;
        paths[i].input_path = argv[argi + i * 2];
        paths[i].output_path = argv[argi + i * 2 + 1];
    }

    cfg = (SpecConfig){
        .paths = paths,
        .flow_count = (uint32_t)pairs,
        .pacing_enabled = pacing
    };

    rc = run_spec_pipeline(&cfg);
    free(paths);

    if (rc != 0) {
        fprintf(stderr, "spec_pipeline failed\n");
        return EXIT_FAILURE;
    }

    fprintf(stderr, "done: %d flow(s)%s\n",
            pairs, pacing ? "" : " (pacing off)");
    return EXIT_SUCCESS;
}
