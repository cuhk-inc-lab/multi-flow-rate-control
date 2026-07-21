#include "pipeline.h"

#include "block_codec.h"
#include "buffer_transfer.h"
#include "codec.h"
#include "file_drain.h"
#include "stream_config.h"
#include "wire_udp.h"

#include "circular_buffer.h"
#include "flow_context.h"
#include "flow_buffer.h"
#include "flow_manager.h"
#include "ingress_push.h"
#include "pipe_io.h"
#include "time_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

typedef struct FlowStage {
    uint32_t            flow_id;
    FILE               *input_fp;
    char                input_path[512];
    FileDrain           drain;
    bool                relay_mode;
    bool                output_dead;
    FlowCircularBuffer  post_multi_pkts;
    CircularBuffer     *post_multi_in;
    CircularBuffer     *sending_out;
    CircularBuffer     *receiver_in;
    CircularBuffer     *receiver_out;
    int                 pipefd[2];
    bool                ingest_done;
    bool                segment_input_drained;
    bool                segment_closing;
    bool                udp_segment_files;
    uint64_t            segment_packet_limit;
    uint64_t            segment_id;
    _Atomic uint64_t    packets_pushed;
    _Atomic int64_t     last_recv_ns;
    char                output_prefix[512];
    unsigned char       ingress_partial[PKG_SIZE];
    size_t              ingress_partial_len;
    unsigned char       pipe_partial[PKG_SIZE];
    size_t              pipe_partial_len;
    /*
     * Valid-byte count for the final padded decode block. A nonzero value
     * means its encoded form is still somewhere in the stage pipeline.
     */
    size_t              tail_valid_len;
    bool                wire_send_mode;
    bool                wire_tail_sent;
    bool                wire_end_sent;
} FlowStage;

static int buffer_has_space(CircularBuffer *buf, size_t need);
static int flow_packets_delivered(const FlowStage *st, const FlowManager *mgr);
static int drain_pipe_to_post_multi(FlowStage *st);

static WgPipelineStatus enqueue_padded_tail(FlowStage *st, const Codec *codec,
                                            unsigned char *work, int *progress)
{
    size_t input_block_size;
    size_t output_block_size;
    size_t n;

    if (st == NULL || codec == NULL || work == NULL) {
        return WG_PIPE_ERR;
    }
    input_block_size = Codec_input_block_size(codec);
    output_block_size = Codec_output_block_size(codec);
    if (!st->ingest_done || !st->segment_input_drained ||
        st->tail_valid_len != 0 ||
        st->post_multi_in->size == 0 ||
        st->post_multi_in->size >= input_block_size) {
        return WG_PIPE_OK;
    }

    if (!buffer_has_space(st->sending_out, output_block_size)) {
        return WG_PIPE_OK;
    }

    n = st->post_multi_in->size;
    if (Buffer_Read(st->post_multi_in, work, n) != CB_OK) {
        return WG_PIPE_ERR;
    }

    memset(work + n, 0, input_block_size - n);
    Codec_encode(codec, work, output_block_size);
    if (Buffer_Write(st->sending_out, work, output_block_size) != CB_OK) {
        return WG_PIPE_ERR;
    }

    st->tail_valid_len = n;
    if (progress != NULL) {
        *progress = 1;
    }

    return WG_PIPE_OK;
}

static WgPipelineStatus init_flow_stage(FlowStage *stage, const WgFlowPath *path,
                                        bool relay_mode, bool defer_output_open)
{
    if (stage == NULL || path == NULL || path->output_path == NULL) {
        return WG_PIPE_ERR;
    }

    memset(stage, 0, sizeof(*stage));
    stage->flow_id = path->flow_id;
    stage->relay_mode = relay_mode;
    atomic_init(&stage->packets_pushed, 0);
    atomic_init(&stage->last_recv_ns, 0);
    stage->pipefd[0] = -1;
    stage->pipefd[1] = -1;

    if (path->input_path != NULL) {
        strncpy(stage->input_path, path->input_path, sizeof(stage->input_path) - 1u);
        stage->input_path[sizeof(stage->input_path) - 1u] = '\0';
    }

    if (relay_mode) {
        if (flow_buffer_init(&stage->post_multi_pkts, MF_QUEUE_CAPACITY) != FB_OK) {
            return WG_PIPE_ERR;
        }
    } else {
        if (pipe(stage->pipefd) != 0) {
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
    }

  /* Output FIFO needs a reader (ffplay) before we return; input opens lazily. */
    if (!defer_output_open &&
        FileDrain_open(&stage->drain, path->output_path) != DRAIN_OK) {
        return WG_PIPE_ERR;
    }

    return WG_PIPE_OK;
}

static WgPipelineStatus init_wire_flow_stage(FlowStage *stage, uint32_t flow_id,
                                             const char *input_path)
{
    if (stage == NULL || input_path == NULL) {
        return WG_PIPE_ERR;
    }

    memset(stage, 0, sizeof(*stage));
    stage->flow_id = flow_id;
    stage->wire_send_mode = true;
    atomic_init(&stage->packets_pushed, 0);
    atomic_init(&stage->last_recv_ns, 0);
    stage->pipefd[0] = -1;
    stage->pipefd[1] = -1;
    strncpy(stage->input_path, input_path, sizeof(stage->input_path) - 1u);
    stage->input_path[sizeof(stage->input_path) - 1u] = '\0';

    if (pipe(stage->pipefd) != 0) {
        return WG_PIPE_ERR;
    }

    if (Buffer_Init(&stage->post_multi_in, BUFFER_BLOCK_COUNT, BUFFER_BLOCK_SIZE,
                    BUFFER_OVERFLOW_POLICY) != CB_OK) {
        return WG_PIPE_ERR;
    }

    return WG_PIPE_OK;
}

static uint64_t wire_realtime_nanoseconds(void)
{
    struct timespec now;

    if (clock_gettime(CLOCK_REALTIME, &now) != 0 || now.tv_sec < 0) {
        return 0;
    }
    return (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
}

static WgPipelineStatus send_wire_tail_block(FlowStage *st, const Codec *codec,
                                             WireUdpTx *tx, unsigned char *work,
                                             int *progress)
{
    size_t input_block_size;
    size_t output_block_size;
    size_t n;
    uint64_t encode_begin_ns;
    uint64_t encode_end_ns;

    if (st == NULL || codec == NULL || tx == NULL || work == NULL) {
        return WG_PIPE_ERR;
    }
    input_block_size = Codec_input_block_size(codec);
    output_block_size = Codec_output_block_size(codec);
    if (!st->ingest_done || !st->segment_input_drained || st->wire_tail_sent ||
        st->post_multi_in->size == 0 ||
        st->post_multi_in->size >= input_block_size) {
        return WG_PIPE_OK;
    }
    if (!wire_udp_tx_ready(tx)) {
        return WG_PIPE_OK;
    }

    n = st->post_multi_in->size;
    if (Buffer_Read(st->post_multi_in, work, n) != CB_OK) {
        return WG_PIPE_ERR;
    }
    memset(work + n, 0, input_block_size - n);
    encode_begin_ns = wire_realtime_nanoseconds();
    Codec_encode(codec, work, output_block_size);
    encode_end_ns = wire_realtime_nanoseconds();
    if (wire_udp_tx_send_block(tx, codec, work, n, encode_begin_ns,
                               encode_end_ns) != 0) {
        return WG_PIPE_ERR;
    }
    st->wire_tail_sent = true;
    if (progress != NULL) {
        *progress = 1;
    }
    return WG_PIPE_OK;
}

static WgPipelineStatus process_flow_wire_send(FlowStage *st, const Codec *codec,
                                               WireUdpTx *tx, unsigned char *work,
                                               int *progress)
{
    size_t input_block_size;
    size_t output_block_size;

    if (st == NULL || codec == NULL || tx == NULL || work == NULL) {
        return WG_PIPE_ERR;
    }

    input_block_size = Codec_input_block_size(codec);
    output_block_size = Codec_output_block_size(codec);
    if (input_block_size == 0 || output_block_size == 0 ||
        output_block_size > CODEC_MAX_ENCODE_BLOCK) {
        return WG_PIPE_ERR;
    }

    while (st->post_multi_in->size >= input_block_size) {
        uint64_t encode_begin_ns;
        uint64_t encode_end_ns;

        if (!wire_udp_tx_ready(tx)) {
            break;
        }
        if (Buffer_Read(st->post_multi_in, work, input_block_size) != CB_OK) {
            return WG_PIPE_ERR;
        }
        encode_begin_ns = wire_realtime_nanoseconds();
        Codec_encode(codec, work, output_block_size);
        encode_end_ns = wire_realtime_nanoseconds();
        if (wire_udp_tx_send_block(tx, codec, work, input_block_size,
                                   encode_begin_ns, encode_end_ns) != 0) {
            return WG_PIPE_ERR;
        }
        if (progress != NULL) {
            *progress = 1;
        }
    }

    return send_wire_tail_block(st, codec, tx, work, progress);
}

static int flow_queues_drained(const FlowManager *mgr, uint32_t flow_id)
{
    if (mgr == NULL || flow_id >= mgr->config.max_flows) {
        return 0;
    }

    return mixed_queue_is_empty(&mgr->mixed) &&
           flow_buffer_is_empty(&mgr->flows[flow_id].queue) &&
           (mgr->deferred == NULL ||
            flow_manager_deferred_count(mgr, flow_id) == 0);
}

static int wire_flow_stage_quiescent(const FlowStage *st, const FlowManager *mgr)
{
    if (st == NULL || !st->wire_send_mode || !st->ingest_done) {
        return 0;
    }

    if (!flow_packets_delivered(st, mgr)) {
        return 0;
    }

    return st->segment_input_drained &&
           st->pipe_partial_len == 0 &&
           Buffer_IsEmpty(st->post_multi_in) &&
           (st->wire_tail_sent || st->post_multi_in->size == 0) &&
           flow_queues_drained(mgr, st->flow_id);
}

static WgPipelineStatus configure_udp_segment_output(FlowStage *stage,
                                                      const char *output_prefix)
{
    if (stage == NULL || output_prefix == NULL ||
        output_prefix[0] == '\0' ||
        strlen(output_prefix) >= sizeof(stage->output_prefix)) {
        return WG_PIPE_ERR;
    }

    strncpy(stage->output_prefix, output_prefix, sizeof(stage->output_prefix) - 1u);
    stage->output_prefix[sizeof(stage->output_prefix) - 1u] = '\0';
    stage->udp_segment_files = true;
    return WG_PIPE_OK;
}

static WgPipelineStatus open_udp_segment_output(FlowStage *stage)
{
    char path[sizeof(stage->drain.path)];
    int  n;

    if (stage == NULL || !stage->udp_segment_files) {
        return WG_PIPE_ERR;
    }
    if (stage->drain.fp != NULL) {
        return WG_PIPE_OK;
    }

    n = snprintf(path, sizeof(path), "%sflow%u_segment%llu.bin",
                 stage->output_prefix, stage->flow_id,
                 (unsigned long long)stage->segment_id);
    if (n < 0 || (size_t)n >= sizeof(path)) {
        return WG_PIPE_ERR;
    }

    return FileDrain_open(&stage->drain, path) == DRAIN_OK ?
           WG_PIPE_OK : WG_PIPE_ERR;
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

    if (stage->relay_mode) {
        flow_buffer_shutdown(&stage->post_multi_pkts);
        flow_buffer_destroy(&stage->post_multi_pkts);
    } else if (stage->wire_send_mode) {
        Buffer_Destroy(&stage->post_multi_in);
        if (stage->pipefd[0] >= 0) {
            close(stage->pipefd[0]);
            stage->pipefd[0] = -1;
        }
        if (stage->pipefd[1] >= 0) {
            close(stage->pipefd[1]);
            stage->pipefd[1] = -1;
        }
    } else {
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
}

static void mark_segment_ending(FlowStage *st)
{
    if (st == NULL) {
        return;
    }

    st->segment_packet_limit = atomic_load(&st->packets_pushed);
    st->segment_input_drained = false;
    st->ingest_done = true;
}

static int flow_packets_delivered(const FlowStage *st, const FlowManager *mgr)
{
    uint64_t enq;
    uint64_t deq;

    if (st == NULL || mgr == NULL || !st->ingest_done ||
        st->flow_id >= mgr->config.max_flows) {
        return 0;
    }

    enq = atomic_load(&mgr->flows[st->flow_id].metrics.enqueued_packets);
    deq = atomic_load(&mgr->flows[st->flow_id].metrics.dequeued_packets);
    return enq >= st->segment_packet_limit && deq >= st->segment_packet_limit;
}

/*
 * Once all packets accepted for this segment have reached the pipe, drain it
 * one final time before padding a partial codec block. The second delivery
 * check closes the race where a worker writes just after the first drain.
 */
static WgPipelineStatus finish_segment_input(FlowStage *st, const FlowManager *mgr,
                                             int *progress)
{
    int dr;

    if (st == NULL || mgr == NULL || st->relay_mode ||
        !st->ingest_done || st->segment_input_drained) {
        return WG_PIPE_OK;
    }
    if (!flow_packets_delivered(st, mgr)) {
        return WG_PIPE_OK;
    }

    dr = drain_pipe_to_post_multi(st);
    if (dr < 0) {
        return WG_PIPE_ERR;
    }
    if (dr > 0 && progress != NULL) {
        *progress = 1;
    }
    if (!flow_packets_delivered(st, mgr)) {
        return WG_PIPE_OK;
    }

    if (st->pipe_partial_len > 0) {
        if (Buffer_Write(st->post_multi_in, st->pipe_partial,
                         st->pipe_partial_len) != CB_OK) {
            return WG_PIPE_ERR;
        }
        st->pipe_partial_len = 0;
        if (progress != NULL) {
            *progress = 1;
        }
    }

    st->segment_input_drained = true;
    return WG_PIPE_OK;
}

static int flow_stage_quiescent(const FlowStage *st, const FlowManager *mgr)
{
    if (st == NULL || !st->ingest_done) {
        return 0;
    }

    if (!flow_packets_delivered(st, mgr)) {
        return 0;
    }

    if (st->relay_mode) {
        return flow_buffer_is_empty(&st->post_multi_pkts) &&
               flow_queues_drained(mgr, st->flow_id);
    }

    return st->segment_input_drained &&
           st->pipe_partial_len == 0 &&
           Buffer_IsEmpty(st->post_multi_in) &&
           Buffer_IsEmpty(st->sending_out) &&
           Buffer_IsEmpty(st->receiver_in) &&
           Buffer_IsEmpty(st->receiver_out) &&
           flow_queues_drained(mgr, st->flow_id);
}

static int input_is_fifo(const FlowStage *st)
{
    struct stat stbuf;

    if (st == NULL || st->input_path[0] == '\0') {
        return 0;
    }
    if (stat(st->input_path, &stbuf) != 0) {
        return 0;
    }
    return S_ISFIFO(stbuf.st_mode);
}
static int flow_can_accept_ingress(const FlowStage *st, const FlowManager *mgr);
static void mark_output_dead(FlowStage *st);

static WgPipelineStatus ensure_input_open(FlowStage *st)
{
    int         fd;
    int         flags;
    int         oflags;
    FILE       *fp;
    struct stat stbuf;

    if (st == NULL || st->input_path[0] == '\0') {
        return WG_PIPE_ERR;
    }

    if (st->input_fp != NULL) {
        return WG_PIPE_OK;
    }

    /*
     * Regular files: O_RDONLY is fine; EOF ends the flow.
     * FIFOs: use O_RDWR|O_NONBLOCK so (1) open never blocks the multi-flow
     * main loop, (2) a writer side exists so peer ffmpeg open() can complete,
     * (3) idle reads return EAGAIN instead of false EOF.
     */
    oflags = O_RDONLY | O_NONBLOCK;
    if (stat(st->input_path, &stbuf) == 0 && S_ISFIFO(stbuf.st_mode)) {
        oflags = O_RDWR | O_NONBLOCK;
    }

    do {
        fd = open(st->input_path, oflags);
    } while (fd < 0 && errno == EINTR);

    if (fd < 0) {
        if (errno == ENXIO || errno == EAGAIN || errno == EWOULDBLOCK ||
            errno == EINTR) {
            return WG_PIPE_OK;
        }
        return WG_PIPE_ERR;
    }

    flags = fcntl(fd, F_GETFL);
    if (flags >= 0) {
        (void)fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    fp = fdopen(fd, "rb");
    if (fp == NULL) {
        close(fd);
        return WG_PIPE_ERR;
    }

    setvbuf(fp, NULL, _IONBF, 0);
    st->input_fp = fp;
    return WG_PIPE_OK;
}

static ssize_t read_input_nb(FlowStage *st, unsigned char *buf, size_t len)
{
    ssize_t n;
    int     fd;

    if (st == NULL || st->input_fp == NULL || buf == NULL || len == 0) {
        errno = EINVAL;
        return -1;
    }

    fd = fileno(st->input_fp);
    do {
        n = read(fd, buf, len);
    } while (n < 0 && errno == EINTR);

    return n;
}

static int drain_pipe_to_post_multi(FlowStage *st)
{
    unsigned char  buf[PKG_SIZE];
    ssize_t        n;
    int            flags;
    int            saved_flags = -1;
    int            total = 0;

    if (st == NULL || st->pipefd[0] < 0 || st->post_multi_in == NULL) {
        return -1;
    }

    flags = fcntl(st->pipefd[0], F_GETFL);
    if (flags >= 0) {
        saved_flags = flags;
        (void)fcntl(st->pipefd[0], F_SETFL, flags | O_NONBLOCK);
    }

    for (;;) {
        if (st->pipe_partial_len > 0) {
            size_t need = PKG_SIZE - st->pipe_partial_len;

            do {
                n = read(st->pipefd[0], st->pipe_partial + st->pipe_partial_len,
                         need);
            } while (n < 0 && errno == EINTR);

            if (n > 0) {
                st->pipe_partial_len += (size_t)n;
                if (st->pipe_partial_len < PKG_SIZE) {
                    break;
                }

                if (Buffer_Write(st->post_multi_in, st->pipe_partial,
                                 PKG_SIZE) != CB_OK) {
                    if (saved_flags >= 0) {
                        (void)fcntl(st->pipefd[0], F_SETFL, saved_flags);
                    }
                    return -1;
                }

                st->pipe_partial_len = 0;
                total += (int)PKG_SIZE;
                continue;
            }

            if (n == 0 || (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))) {
                break;
            }

            if (saved_flags >= 0) {
                (void)fcntl(st->pipefd[0], F_SETFL, saved_flags);
            }
            return -1;
        }

        do {
            n = read(st->pipefd[0], buf, PKG_SIZE);
        } while (n < 0 && errno == EINTR);

        if (n > 0) {
            if ((size_t)n < PKG_SIZE) {
                memcpy(st->pipe_partial, buf, (size_t)n);
                st->pipe_partial_len = (size_t)n;
                break;
            }

            if (Buffer_Write(st->post_multi_in, buf, PKG_SIZE) != CB_OK) {
                if (saved_flags >= 0) {
                    (void)fcntl(st->pipefd[0], F_SETFL, saved_flags);
                }
                return -1;
            }

            total += (int)PKG_SIZE;
            continue;
        }

        if (n == 0 || (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))) {
            break;
        }

        if (saved_flags >= 0) {
            (void)fcntl(st->pipefd[0], F_SETFL, saved_flags);
        }
        return -1;
    }

    if (saved_flags >= 0) {
        (void)fcntl(st->pipefd[0], F_SETFL, saved_flags);
    }

    return total;
}

static WgPipelineStatus push_ingress_packet(FlowStage *st, FlowManager *mgr,
                                            const unsigned char *data, size_t len)
{
    if (ingress_push(mgr, st->flow_id, data, len) != INGRESS_PUSH_OK) {
        return WG_PIPE_ERR;
    }

    atomic_fetch_add(&st->packets_pushed, 1);
    return WG_PIPE_OK;
}

static WgPipelineStatus pump_file_ingress(FlowStage *st, FlowManager *mgr)
{
    unsigned char buf[PKG_SIZE];
    ssize_t       n;
    int           pumped = 0;
    const int     max_pkts = 64;

    if (st == NULL || mgr == NULL) {
        return WG_PIPE_ERR;
    }

    if (ensure_input_open(st) != WG_PIPE_OK) {
        return WG_PIPE_ERR;
    }

    /* Writer not connected yet (FIFO); keep other flows progressing. */
    if (st->input_fp == NULL) {
        return WG_PIPE_OK;
    }

    while (pumped < max_pkts && !st->ingest_done) {
        if (!flow_can_accept_ingress(st, mgr)) {
            break;
        }

        if (st->ingress_partial_len > 0) {
            size_t need = PKG_SIZE - st->ingress_partial_len;

            n = read_input_nb(st, st->ingress_partial + st->ingress_partial_len,
                              need);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return WG_PIPE_OK;
                }
                return WG_PIPE_ERR;
            }
            if (n == 0) {
                if (input_is_fifo(st)) {
                    return WG_PIPE_OK;
                }
                if (st->ingress_partial_len > 0) {
                    WgPipelineStatus push_st;

                    push_st = push_ingress_packet(st, mgr, st->ingress_partial,
                                                  st->ingress_partial_len);
                    st->ingress_partial_len = 0;
                    if (push_st != WG_PIPE_OK) {
                        return push_st;
                    }
                }
                mark_segment_ending(st);
                return WG_PIPE_OK;
            }

            st->ingress_partial_len += (size_t)n;
            if (st->ingress_partial_len < PKG_SIZE) {
                return WG_PIPE_OK;
            }

            {
                WgPipelineStatus push_st;

                push_st = push_ingress_packet(st, mgr, st->ingress_partial,
                                              PKG_SIZE);
                st->ingress_partial_len = 0;
                if (push_st != WG_PIPE_OK) {
                    return push_st;
                }
                pumped++;
                continue;
            }
        }

        n = read_input_nb(st, buf, PKG_SIZE);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return WG_PIPE_OK;
            }
            return WG_PIPE_ERR;
        }
        if (n == 0) {
            if (input_is_fifo(st)) {
                return WG_PIPE_OK;
            }
            mark_segment_ending(st);
            return WG_PIPE_OK;
        }

        if ((size_t)n < PKG_SIZE) {
            memcpy(st->ingress_partial, buf, (size_t)n);
            st->ingress_partial_len = (size_t)n;
            return WG_PIPE_OK;
        }

        if (push_ingress_packet(st, mgr, buf, PKG_SIZE) != WG_PIPE_OK) {
            return WG_PIPE_ERR;
        }
        pumped++;
    }

    return WG_PIPE_OK;
}

static int buffer_has_space(CircularBuffer *buf, size_t need)
{
    if (buf == NULL) {
        return 0;
    }

    return (buf->capacity - buf->size) >= need;
}

static int flow_can_accept_ingress(const FlowStage *st, const FlowManager *mgr)
{
    size_t per_flow_cap;
    size_t deferred_count;

    if (st == NULL || mgr == NULL) {
        return 0;
    }

    per_flow_cap = mgr->config.per_flow_queue_capacity;
    deferred_count = flow_manager_deferred_count(mgr, st->flow_id);

    if (mixed_queue_count(&mgr->mixed) + 1 >= mgr->config.mixed_queue_capacity) {
        return 0;
    }

    if (flow_buffer_count(&mgr->flows[st->flow_id].queue) + 1 >= per_flow_cap) {
        return 0;
    }

    if (deferred_count + 1 >= mgr->config.mixed_queue_capacity) {
        return 0;
    }

    if (st->relay_mode && flow_buffer_is_full(&st->post_multi_pkts)) {
        return 0;
    }

    return 1;
}

static WgPipelineStatus process_flow_post_multi(FlowStage *st, const Codec *codec,
                                                unsigned char *work, int *progress)
{
    size_t input_block_size;
    size_t output_block_size;

    if (st == NULL || codec == NULL || work == NULL) {
        return WG_PIPE_ERR;
    }

    input_block_size = Codec_input_block_size(codec);
    output_block_size = Codec_output_block_size(codec);
    if (input_block_size == 0 || output_block_size == 0 ||
        output_block_size > CODEC_MAX_ENCODE_BLOCK) {
        return WG_PIPE_ERR;
    }

    while (st->post_multi_in->size >= input_block_size) {
        if (!buffer_has_space(st->sending_out, output_block_size)) {
            break;
        }

        if (Buffer_Read(st->post_multi_in, work, input_block_size) != CB_OK) {
            return WG_PIPE_ERR;
        }
        Codec_encode(codec, work, output_block_size);
        if (Buffer_Write(st->sending_out, work, output_block_size) != CB_OK) {
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
                                   output_block_size, &moved);
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

    while (st->receiver_in->size >= output_block_size) {
        size_t decoded_len = input_block_size;

        if (!buffer_has_space(st->receiver_out, input_block_size)) {
            break;
        }

        if (Buffer_Read(st->receiver_in, work, output_block_size) != CB_OK) {
            return WG_PIPE_ERR;
        }
        Codec_decode(codec, work, output_block_size);
        if (st->tail_valid_len != 0) {
            decoded_len = st->tail_valid_len;
        }
        if (Buffer_Write(st->receiver_out, work, decoded_len) != CB_OK) {
            return WG_PIPE_ERR;
        }
        if (st->tail_valid_len != 0) {
            st->tail_valid_len = 0;
        }
        if (progress != NULL) {
            *progress = 1;
        }
    }

    if (enqueue_padded_tail(st, codec, work, progress) != WG_PIPE_OK) {
        return WG_PIPE_ERR;
    }

    {
        DrainStatus drain_st;

        if (st->output_dead) {
            /* Drop decoded bytes for closed viewers. */
            while (!Buffer_IsEmpty(st->receiver_out)) {
                unsigned char discard[PKG_SIZE];
                size_t        n = st->receiver_out->size;

                if (n > sizeof(discard)) {
                    n = sizeof(discard);
                }
                if (Buffer_Read(st->receiver_out, discard, n) != CB_OK) {
                    break;
                }
                if (progress != NULL) {
                    *progress = 1;
                }
            }
            return WG_PIPE_OK;
        }

        /*
         * UDP segment files are opened by the ingress thread only after the
         * first datagram of a segment arrives. An idle flow has no output yet.
         */
        if (st->udp_segment_files && st->drain.fp == NULL) {
            return WG_PIPE_OK;
        }

        for (;;) {
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
            mark_output_dead(st);
            break;
        }

        if (!st->output_dead && st->ingest_done &&
            !Buffer_IsEmpty(st->receiver_out)) {
            drain_st = FileDrain_flush_remainder(&st->drain, st->receiver_out, NULL);
            if (drain_st == DRAIN_ERR) {
                mark_output_dead(st);
            }
        }
    }

    return WG_PIPE_OK;
}

static void mark_output_dead(FlowStage *st)
{
    if (st == NULL || st->output_dead) {
        return;
    }

    st->output_dead = true;
    FileDrain_close(&st->drain);
    fprintf(stderr, "flow %u: output closed; continuing other flows\n",
            st->flow_id);
}

static WgPipelineStatus process_flow_relay(FlowStage *st, int *progress)
{
    if (st == NULL) {
        return WG_PIPE_ERR;
    }

    for (;;) {
        DataPacket      *pkt = NULL;
        FlowBufferStatus fb_st;
        DrainStatus      drain_st;

        fb_st = flow_buffer_try_dequeue(&st->post_multi_pkts, &pkt);
        if (fb_st == FB_ERR_EMPTY) {
            break;
        }
        if (fb_st != FB_OK || pkt == NULL) {
            return WG_PIPE_ERR;
        }

        if (st->output_dead) {
            packet_free(pkt);
            if (progress != NULL) {
                *progress = 1;
            }
            continue;
        }

        drain_st = FileDrain_write_packet(&st->drain, pkt, NULL);
        packet_free(pkt);
        if (drain_st != DRAIN_OK) {
            /* Viewer closed this window — drop this flow's egress, keep others. */
            mark_output_dead(st);
            continue;
        }

        if (progress != NULL) {
            *progress = 1;
        }
    }

    return WG_PIPE_OK;
}

static WgPipelineStatus flush_flow_tails(FlowStage *st, const FlowManager *mgr,
                                         const Codec *codec)
{
    unsigned char work[CODEC_MAX_ENCODE_BLOCK];
    int           progress;

    if (st == NULL) {
        return WG_PIPE_ERR;
    }

    if (st->relay_mode) {
        return process_flow_relay(st, NULL);
    }

    if (finish_segment_input(st, mgr, &progress) != WG_PIPE_OK) {
        return WG_PIPE_ERR;
    }

    for (;;) {
        progress = 0;
        if (process_flow_post_multi(st, codec, work, &progress) != WG_PIPE_OK) {
            return WG_PIPE_ERR;
        }

        if (st->segment_input_drained &&
            st->tail_valid_len == 0 &&
            Buffer_IsEmpty(st->post_multi_in) &&
            Buffer_IsEmpty(st->sending_out) &&
            Buffer_IsEmpty(st->receiver_in) &&
            Buffer_IsEmpty(st->receiver_out)) {
            return WG_PIPE_OK;
        }
        if (!progress) {
            return WG_PIPE_ERR;
        }
    }
}

WgPipelineStatus wg_pipeline_run(const WgPipelineConfig *config)
{
    FlowManager       mgr;
    FlowManagerConfig mgr_cfg;
    FlowStage        *stages = NULL;
    uint32_t          max_flow_id = 0;
    uint32_t          i;
    const Codec      *codec;
    WgPipelineStatus  status = WG_PIPE_OK;
    unsigned char     work[CODEC_MAX_ENCODE_BLOCK];
    bool              relay_mode = false;

    if (config == NULL || config->flows == NULL || config->flow_count == 0) {
        return WG_PIPE_ERR;
    }

    relay_mode = config->codec_kind == CODEC_KIND_NONE;
    codec = relay_mode ? NULL : Codec_get(config->codec_kind);
    if (!relay_mode && codec == NULL) {
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
        if (init_flow_stage(&stages[i], &config->flows[i], relay_mode, false) != WG_PIPE_OK) {
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

        if (relay_mode) {
            mgr.flows[fid].relay_queue = &stages[i].post_multi_pkts;
            mgr.flows[fid].output_fd = -1;
        } else {
            mgr.flows[fid].output_fd = stages[i].pipefd[1];
        }
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

            if (relay_mode) {
                if (process_flow_relay(st, &progress) != WG_PIPE_OK) {
                    status = WG_PIPE_ERR;
                    goto cleanup_running;
                }
            } else {
                int dr;

                dr = drain_pipe_to_post_multi(st);
                if (dr > 0) {
                    progress = 1;
                } else if (dr < 0) {
                    status = WG_PIPE_ERR;
                    goto cleanup_running;
                }

                if (finish_segment_input(st, &mgr, &progress) != WG_PIPE_OK) {
                    status = WG_PIPE_ERR;
                    goto cleanup_running;
                }

                if (process_flow_post_multi(st, codec, work, &progress) != WG_PIPE_OK) {
                    status = WG_PIPE_ERR;
                    goto cleanup_running;
                }
            }

            if (!st->ingest_done) {
                WgPipelineStatus ingest_st = WG_PIPE_OK;

                if (flow_can_accept_ingress(st, &mgr)) {
                    ingest_st = pump_file_ingress(st, &mgr);
                }

                if (ingest_st == WG_PIPE_OK) {
                    if (flow_can_accept_ingress(st, &mgr)) {
                        progress = 1;
                    }
                } else {
                    status = WG_PIPE_ERR;
                    goto cleanup_running;
                }
            }
        }

        {
            bool all_done = true;
            bool all_outputs_closed = true;

            for (i = 0; i < config->flow_count; i++) {
                if (!stages[i].output_dead) {
                    all_outputs_closed = false;
                }
                if (!flow_stage_quiescent(&stages[i], &mgr)) {
                    all_done = false;
                }
            }

            if (all_outputs_closed) {
                fprintf(stderr, "all outputs closed; exiting\n");
                break;
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
        if (flush_flow_tails(&stages[i], &mgr, relay_mode ? NULL : codec) != WG_PIPE_OK) {
            status = WG_PIPE_ERR;
            goto cleanup_running;
        }
    }

cleanup_running:
    for (i = 0; i < config->flow_count; i++) {
        if (relay_mode) {
            flow_buffer_shutdown(&stages[i].post_multi_pkts);
        }
    }
    for (i = 0; i < config->flow_count; i++) {
        uint32_t fid = config->flows[i].flow_id;

        fprintf(stderr,
                "flow %u: enq_bps=%.0f deq_bps=%.0f packets=%llu pushed=%llu\n",
                fid,
                flow_metrics_get_enqueue_bps(&mgr.flows[fid].metrics),
                flow_metrics_get_dequeue_bps(&mgr.flows[fid].metrics),
                (unsigned long long)atomic_load(
                    &mgr.flows[fid].metrics.dequeued_packets),
                (unsigned long long)atomic_load(&stages[i].packets_pushed));
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

typedef struct UdpIngressCtx {
    int                      sock;
    FlowManager             *mgr;
    FlowPeerMap             *map;
    FlowStage               *stages;
    uint32_t                 flow_count;
    struct sockaddr_storage  local;
    socklen_t                local_len;
    _Atomic int              running;
    _Atomic int              failed;
    pthread_mutex_t          segment_mtx;
    pthread_cond_t           segment_ready;
    int                      segment_sync_ready;
} UdpIngressCtx;

static void udp_touch_flow(FlowStage *st)
{
    struct timespec now;

    if (st == NULL) {
        return;
    }

    if (time_utils_now_mono(&now) == TU_OK) {
        atomic_store(&st->last_recv_ns,
                     (int64_t)now.tv_sec * 1000000000LL + (int64_t)now.tv_nsec);
    }
}

static void udp_close_idle_segments(UdpIngressCtx *ctx, int64_t now_ns,
                                    unsigned idle_sec)
{
    uint32_t i;

    if (ctx == NULL || idle_sec == 0 || now_ns <= 0) {
        return;
    }

    pthread_mutex_lock(&ctx->segment_mtx);
    for (i = 0; i < ctx->flow_count; i++) {
        FlowStage *st = &ctx->stages[i];
        int64_t    last_ns = atomic_load(&st->last_recv_ns);

        if (!st->segment_closing && last_ns > 0 &&
            (now_ns - last_ns) >= (int64_t)idle_sec * 1000000000LL) {
            st->segment_closing = true;
            mark_segment_ending(st);
        }
    }
    pthread_mutex_unlock(&ctx->segment_mtx);
}

static int udp_segment_flushed(const FlowStage *st, const FlowManager *mgr)
{
    return st != NULL && mgr != NULL && st->ingest_done &&
           st->segment_input_drained && st->tail_valid_len == 0 &&
           st->pipe_partial_len == 0 &&
           Buffer_IsEmpty(st->post_multi_in) &&
           Buffer_IsEmpty(st->sending_out) &&
           Buffer_IsEmpty(st->receiver_in) &&
           Buffer_IsEmpty(st->receiver_out) &&
           flow_packets_delivered(st, mgr);
}

static WgPipelineStatus udp_reopen_flushed_segment(UdpIngressCtx *ctx, FlowStage *st,
                                                    const FlowManager *mgr, int *progress)
{
    if (ctx == NULL || st == NULL || !udp_segment_flushed(st, mgr)) {
        return WG_PIPE_OK;
    }

    pthread_mutex_lock(&ctx->segment_mtx);
    if (st->segment_closing && udp_segment_flushed(st, mgr)) {
        if (FileDrain_flush(&st->drain) != DRAIN_OK) {
            pthread_mutex_unlock(&ctx->segment_mtx);
            return WG_PIPE_ERR;
        }
        FileDrain_close(&st->drain);
        st->segment_id++;
        st->ingest_done = false;
        st->segment_input_drained = false;
        st->segment_packet_limit = 0;
        atomic_store(&st->last_recv_ns, 0);
        st->segment_closing = false;
        pthread_cond_broadcast(&ctx->segment_ready);
        if (progress != NULL) {
            *progress = 1;
        }
    }
    pthread_mutex_unlock(&ctx->segment_mtx);
    return WG_PIPE_OK;
}

static void *udp_ingress_thread(void *arg)
{
    UdpIngressCtx           *ctx = arg;
    unsigned char            buf[2048];
    struct sockaddr_storage  peer;
    socklen_t                peer_len;
    ssize_t                  n;
    FlowTuple                tuple;

    while (atomic_load(&ctx->running)) {
        peer_len = sizeof(peer);
        do {
            n = recvfrom(ctx->sock, buf, sizeof(buf), 0,
                         (struct sockaddr *)&peer, &peer_len);
        } while (n < 0 && errno == EINTR);

        if (n <= 0) {
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                struct timespec delay = { .tv_sec = 0, .tv_nsec = 1000000L };

                nanosleep(&delay, NULL);
                continue;
            }
            break;
        }

        if (flow_tuple_set(&tuple,
                           (struct sockaddr *)&peer, peer_len,
                           (struct sockaddr *)&ctx->local, ctx->local_len,
                           IPPROTO_UDP) != 0) {
            continue;
        }

        {
            uint32_t flow_id = flow_peer_map_lookup(ctx->map, &tuple);

            if (flow_id == (uint32_t)-1) {
                continue;
            }

            pthread_mutex_lock(&ctx->segment_mtx);
            while (ctx->stages[flow_id].segment_closing &&
                   atomic_load(&ctx->running)) {
                pthread_cond_wait(&ctx->segment_ready, &ctx->segment_mtx);
            }
            if (!atomic_load(&ctx->running)) {
                pthread_mutex_unlock(&ctx->segment_mtx);
                break;
            }

            if (open_udp_segment_output(&ctx->stages[flow_id]) != WG_PIPE_OK) {
                atomic_store(&ctx->failed, 1);
                pthread_mutex_unlock(&ctx->segment_mtx);
                break;
            }

            if (ingress_push(ctx->mgr, flow_id, buf, (size_t)n) == INGRESS_PUSH_OK) {
                atomic_fetch_add(&ctx->stages[flow_id].packets_pushed, 1);
                udp_touch_flow(&ctx->stages[flow_id]);
            }
            pthread_mutex_unlock(&ctx->segment_mtx);
        }
    }

    return NULL;
}

static int udp_open_socket(uint16_t port)
{
    int                sock;
    int                on = 1;
    struct sockaddr_in addr;

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        return -1;
    }

    if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) != 0) {
        close(sock);
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(sock);
        return -1;
    }

    return sock;
}

WgPipelineStatus wg_pipeline_run_udp(const WgUdpConfig *config)
{
    FlowManager       mgr;
    FlowManagerConfig mgr_cfg;
    FlowStage        *stages = NULL;
    WgFlowPath       *paths = NULL;
    FlowPeerMap      *peer_map = NULL;
    UdpIngressCtx     udp_ctx;
    pthread_t         udp_thread;
    uint32_t          i;
    const Codec      *codec;
    WgPipelineStatus  status = WG_PIPE_OK;
    unsigned char     work[CODEC_MAX_ENCODE_BLOCK];
    int               sock = -1;
    int               udp_started = 0;

    if (config == NULL || config->output_prefix == NULL ||
        config->max_flows == 0 || config->port == 0) {
        return WG_PIPE_ERR;
    }
    codec = Codec_get(config->codec_kind);
    if (codec == NULL) {
        return WG_PIPE_ERR;
    }

    memset(&udp_ctx, 0, sizeof(udp_ctx));
    stages = calloc(config->max_flows, sizeof(*stages));
    paths = calloc(config->max_flows, sizeof(*paths));
    if (stages == NULL || paths == NULL) {
        status = WG_PIPE_ERR;
        goto cleanup;
    }

    for (i = 0; i < config->max_flows; i++) {
        paths[i].flow_id = i;
        paths[i].input_path = NULL;
        paths[i].output_path = config->output_prefix;

        if (init_flow_stage(&stages[i], &paths[i], false, true) != WG_PIPE_OK) {
            status = WG_PIPE_ERR;
            goto cleanup;
        }
        if (configure_udp_segment_output(&stages[i], config->output_prefix) != WG_PIPE_OK) {
            status = WG_PIPE_ERR;
            goto cleanup;
        }
    }

    if (flow_peer_map_init(&peer_map, config->max_flows) != FPM_OK) {
        status = WG_PIPE_ERR;
        goto cleanup;
    }

    mgr_cfg = (FlowManagerConfig){
        .max_flows = config->max_flows,
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

    for (i = 0; i < config->max_flows; i++) {
        mgr.flows[i].output_fd = stages[i].pipefd[1];
        flow_context_set_pacing(&mgr.flows[i], config->pacing_enabled);
    }

    sock = udp_open_socket(config->port);
    if (sock < 0) {
        status = WG_PIPE_ERR;
        goto cleanup_mgr;
    }

    udp_ctx.sock = sock;
    udp_ctx.mgr = &mgr;
    udp_ctx.map = peer_map;
    udp_ctx.stages = stages;
    udp_ctx.flow_count = config->max_flows;
    udp_ctx.local_len = sizeof(udp_ctx.local);
    atomic_store(&udp_ctx.running, 1);
    atomic_store(&udp_ctx.failed, 0);
    if (pthread_mutex_init(&udp_ctx.segment_mtx, NULL) != 0) {
        status = WG_PIPE_ERR;
        goto cleanup_mgr;
    }
    if (pthread_cond_init(&udp_ctx.segment_ready, NULL) != 0) {
        pthread_mutex_destroy(&udp_ctx.segment_mtx);
        status = WG_PIPE_ERR;
        goto cleanup_mgr;
    }
    udp_ctx.segment_sync_ready = 1;

    if (getsockname(sock, (struct sockaddr *)&udp_ctx.local, &udp_ctx.local_len) != 0) {
        status = WG_PIPE_ERR;
        goto cleanup_mgr;
    }

    if (flow_manager_start(&mgr) != FM_OK) {
        status = WG_PIPE_ERR;
        goto cleanup_mgr;
    }

    if (pthread_create(&udp_thread, NULL, udp_ingress_thread, &udp_ctx) != 0) {
        status = WG_PIPE_ERR;
        goto cleanup_running;
    }
    udp_started = 1;

    fprintf(stderr, "UDP ingress on port %u, outputs %sflow0_segment0.bin …\n",
            (unsigned)config->port, config->output_prefix);

    for (;;) {
        int           progress = 0;
        struct timespec now;
        int64_t       now_ns = 0;

        if (time_utils_now_mono(&now) == TU_OK) {
            now_ns = (int64_t)now.tv_sec * 1000000000LL + (int64_t)now.tv_nsec;
        }

        if (atomic_load(&udp_ctx.failed)) {
            status = WG_PIPE_ERR;
            goto cleanup_running;
        }

        udp_close_idle_segments(&udp_ctx, now_ns, config->idle_sec);

        for (i = 0; i < config->max_flows; i++) {
            FlowStage *st = &stages[i];
            int        dr;

            dr = drain_pipe_to_post_multi(st);
            if (dr > 0) {
                progress = 1;
            } else if (dr < 0) {
                status = WG_PIPE_ERR;
                goto cleanup_running;
            }

            if (finish_segment_input(st, &mgr, &progress) != WG_PIPE_OK) {
                status = WG_PIPE_ERR;
                goto cleanup_running;
            }

            if (process_flow_post_multi(st, codec, work, &progress) != WG_PIPE_OK) {
                status = WG_PIPE_ERR;
                goto cleanup_running;
            }

            if (udp_reopen_flushed_segment(&udp_ctx, st, &mgr, &progress) != WG_PIPE_OK) {
                status = WG_PIPE_ERR;
                goto cleanup_running;
            }
        }

        for (i = 0; i < config->max_flows; i++) {
            (void)flow_metrics_tick(&mgr.flows[i].metrics, 5.0);
        }

        if (!progress) {
            struct timespec delay = { .tv_sec = 0, .tv_nsec = 1000000L };

            nanosleep(&delay, NULL);
        }
    }

cleanup_running:
    atomic_store(&udp_ctx.running, 0);
    if (udp_ctx.segment_sync_ready) {
        pthread_mutex_lock(&udp_ctx.segment_mtx);
        pthread_cond_broadcast(&udp_ctx.segment_ready);
        pthread_mutex_unlock(&udp_ctx.segment_mtx);
    }
    if (sock >= 0) {
        shutdown(sock, SHUT_RDWR);
    }
    if (udp_started) {
        pthread_join(udp_thread, NULL);
    }

    for (i = 0; i < config->max_flows; i++) {
        fprintf(stderr,
                "flow %u: enq_bps=%.0f deq_bps=%.0f packets=%llu pushed=%llu\n",
                i,
                flow_metrics_get_enqueue_bps(&mgr.flows[i].metrics),
                flow_metrics_get_dequeue_bps(&mgr.flows[i].metrics),
                (unsigned long long)atomic_load(
                    &mgr.flows[i].metrics.dequeued_packets),
                (unsigned long long)atomic_load(&stages[i].packets_pushed));
    }
    flow_manager_stop(&mgr);

cleanup_mgr:
    flow_manager_destroy(&mgr);

cleanup:
    if (sock >= 0) {
        close(sock);
    }
    if (udp_ctx.segment_sync_ready) {
        pthread_cond_destroy(&udp_ctx.segment_ready);
        pthread_mutex_destroy(&udp_ctx.segment_mtx);
    }
    flow_peer_map_destroy(peer_map);
    if (stages != NULL) {
        for (i = 0; i < config->max_flows; i++) {
            destroy_flow_stage(&stages[i]);
        }
        free(stages);
    }
    free(paths);

    return status;
}

WgPipelineStatus wg_pipeline_run_wire_multi_send(const WgWireMultiSendConfig *config)
{
    FlowManager       mgr;
    FlowManagerConfig mgr_cfg;
    FlowStage        *stages = NULL;
    WireUdpTx        *txs = NULL;
    uint32_t          max_flow_id = 0;
    uint32_t          i;
    const Codec      *codec;
    WgPipelineStatus  status = WG_PIPE_OK;
    unsigned char     work[CODEC_MAX_ENCODE_BLOCK];

    if (config == NULL || config->flows == NULL || config->flow_count == 0 ||
        config->codec_kind == CODEC_KIND_NONE) {
        return WG_PIPE_ERR;
    }

    codec = Codec_get(config->codec_kind);
    if (codec == NULL) {
        return WG_PIPE_ERR;
    }

    stages = calloc(config->flow_count, sizeof(*stages));
    txs = calloc(config->flow_count, sizeof(*txs));
    if (stages == NULL || txs == NULL) {
        status = WG_PIPE_ERR;
        goto cleanup;
    }

    for (i = 0; i < config->flow_count; i++) {
        if (config->flows[i].flow_id > max_flow_id) {
            max_flow_id = config->flows[i].flow_id;
        }
    }

    for (i = 0; i < config->flow_count; i++) {
        const WgWireFlowPath *path = &config->flows[i];

        if (init_wire_flow_stage(&stages[i], path->flow_id,
                                 path->input_path) != WG_PIPE_OK) {
            status = WG_PIPE_ERR;
            goto cleanup;
        }
        if (wire_udp_tx_init(&txs[i], path->host, path->port, path->flow_id,
                             path->source_rate_mbps) != 0) {
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

    fprintf(stderr,
            "wire-multi-send: %u flows via FlowManager -> wire UDP\n",
            config->flow_count);

    for (;;) {
        int progress = 0;

        for (i = 0; i < config->flow_count; i++) {
            FlowStage *st = &stages[i];
            int        dr;

            dr = drain_pipe_to_post_multi(st);
            if (dr > 0) {
                progress = 1;
            } else if (dr < 0) {
                status = WG_PIPE_ERR;
                goto cleanup_running;
            }

            if (finish_segment_input(st, &mgr, &progress) != WG_PIPE_OK) {
                status = WG_PIPE_ERR;
                goto cleanup_running;
            }

            if (process_flow_wire_send(st, codec, &txs[i], work, &progress) !=
                WG_PIPE_OK) {
                status = WG_PIPE_ERR;
                goto cleanup_running;
            }

            if (!st->ingest_done) {
                WgPipelineStatus ingest_st = WG_PIPE_OK;

                if (flow_can_accept_ingress(st, &mgr)) {
                    ingest_st = pump_file_ingress(st, &mgr);
                }
                if (ingest_st == WG_PIPE_OK) {
                    if (flow_can_accept_ingress(st, &mgr)) {
                        progress = 1;
                    }
                } else {
                    status = WG_PIPE_ERR;
                    goto cleanup_running;
                }
            }
        }

        {
            bool all_done = true;

            for (i = 0; i < config->flow_count; i++) {
                if (!wire_flow_stage_quiescent(&stages[i], &mgr)) {
                    all_done = false;
                    break;
                }
            }
            if (all_done) {
                break;
            }
        }

        if (!progress) {
            struct timespec delay = {.tv_sec = 0, .tv_nsec = 1000000L};

            nanosleep(&delay, NULL);
        }
    }

    for (i = 0; i < config->flow_count; i++) {
        if (!stages[i].wire_end_sent) {
            if (wire_udp_tx_send_end(&txs[i], codec) != 0) {
                status = WG_PIPE_ERR;
                goto cleanup_running;
            }
            stages[i].wire_end_sent = true;
            fprintf(stderr,
                    "wire-multi-send: flow_id=%u blocks=%llu source_bytes=%llu\n",
                    config->flows[i].flow_id,
                    (unsigned long long)txs[i].block_id,
                    (unsigned long long)txs[i].source_bytes);
        }
    }

cleanup_running:
    flow_manager_stop(&mgr);
cleanup_mgr:
    flow_manager_destroy(&mgr);
cleanup:
    if (txs != NULL) {
        for (i = 0; i < config->flow_count; i++) {
            wire_udp_tx_destroy(&txs[i]);
        }
        free(txs);
    }
    if (stages != NULL) {
        for (i = 0; i < config->flow_count; i++) {
            destroy_flow_stage(&stages[i]);
        }
        free(stages);
    }

    return status;
}
