#include "wire_udp.h"

#include "codec.h"
#include "stream_config.h"
#include "wire_flow_decoder.h"
#include "wire_header.h"

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <netdb.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define WIRE_MAX_SHARDS     (CODEC_MAX_ENCODE_BLOCK / PKG_SIZE)
/* Larger UDP buffers reduce drops under multi-flow microbursts of 232 B datagrams. */
#define WIRE_UDP_SOCKBUF    (8 * 1024 * 1024)
/*
 * Hybrid shard pacing:
 * - spacing >= SLEEP: nanosleep (+ spin finish)
 * - MIN .. SLEEP: busy-wait only (nanosleep too coarse)
 * - below MIN: sendmmsg burst + block-level pacing
 *
 * Older SLEEP=100us forced bursts above ~75-90 Mbps source for rs/xor; with
 * fast matrix recovery we keep spreading shards at higher rates to cut
 * correlated per-group loss.
 */
#define WIRE_SHARD_PACE_SLEEP_SEC 0.0001
#define WIRE_SHARD_PACE_MIN_SEC   0.000005

static const char *wire_codec_kind_name(CodecKind kind)
{
    switch (kind) {
    case CODEC_KIND_BLOCK:
        return "block";
    case CODEC_KIND_COPY:
        return "copy";
    case CODEC_KIND_XOR_FEC:
        return "xor-fec";
    case CODEC_KIND_RS_FEC:
        return "rs-fec";
    case CODEC_KIND_RS:
        return "rs";
    case CODEC_KIND_NONE:
    default:
        return "none";
    }
}

static void wire_set_udp_buffers(int sock)
{
    int buf = WIRE_UDP_SOCKBUF;

    if (sock < 0) {
        return;
    }
    (void)setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf));
    (void)setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf));
}

static uint64_t realtime_nanoseconds(void)
{
    struct timespec now;

    if (clock_gettime(CLOCK_REALTIME, &now) != 0 || now.tv_sec < 0) {
        return 0;
    }
    return (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
}

static double monotonic_seconds(void)
{
    struct timespec now;

    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        return 0.0;
    }
    return (double)now.tv_sec + (double)now.tv_nsec / 1000000000.0;
}

static void sleep_until_monotonic(double target)
{
    for (;;) {
        double now = monotonic_seconds();
        double delay;
        struct timespec sleep_for;

        if (target <= 0.0 || now <= 0.0 || target <= now) {
            return;
        }
        delay = target - now;
        if (delay >= WIRE_SHARD_PACE_SLEEP_SEC) {
            sleep_for.tv_sec = (time_t)delay;
            sleep_for.tv_nsec =
                (long)((delay - (double)sleep_for.tv_sec) * 1000000000.0);
            (void)nanosleep(&sleep_for, NULL);
            continue;
        }
        /* Short gaps: spin so we still separate shards inside one block. */
    }
}

static void pace_to_source_rate(double started, uint64_t source_bytes,
                                double source_rate_mbps)
{
    double target;
    double delay;
    struct timespec sleep_for;

    if (source_rate_mbps <= 0.0) {
        return;
    }

    target = started + ((double)source_bytes * 8.0) /
                         (source_rate_mbps * 1000000.0);
    delay = target - monotonic_seconds();
    if (delay <= 0.0) {
        return;
    }

    sleep_for.tv_sec = (time_t)delay;
    sleep_for.tv_nsec = (long)((delay - (double)sleep_for.tv_sec) * 1000000000.0);
    (void)nanosleep(&sleep_for, NULL);
}

static int open_sender_socket(const char *host, uint16_t port,
                              struct sockaddr_storage *address,
                              socklen_t *address_len)
{
    struct addrinfo hints;
    struct addrinfo *results = NULL;
    struct addrinfo *entry;
    char port_text[6];
    int sock = -1;

    if (host == NULL || address == NULL || address_len == NULL) {
        return -1;
    }

    snprintf(port_text, sizeof(port_text), "%u", (unsigned)port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;

    if (getaddrinfo(host, port_text, &hints, &results) != 0) {
        return -1;
    }

    for (entry = results; entry != NULL; entry = entry->ai_next) {
        if (entry->ai_addrlen > sizeof(*address)) {
            continue;
        }
        sock = socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol);
        if (sock >= 0) {
            wire_set_udp_buffers(sock);
            memcpy(address, entry->ai_addr, entry->ai_addrlen);
            *address_len = (socklen_t)entry->ai_addrlen;
            break;
        }
    }

    freeaddrinfo(results);
    return sock;
}

static int send_wire_datagram(int sock, const struct sockaddr *address,
                              socklen_t address_len, const WireHeader *header,
                              const unsigned char *payload)
{
    unsigned char datagram[WIRE_HEADER_SIZE + PKG_SIZE];
    size_t length = WIRE_HEADER_SIZE + header->payload_len;
    ssize_t sent;
    int retries = 0;

    if (header->payload_len > PKG_SIZE) {
        return -1;
    }

    wire_header_encode(datagram, header);
    if (header->payload_len > 0 && payload != NULL) {
        memcpy(datagram + WIRE_HEADER_SIZE, payload, header->payload_len);
    }

    for (;;) {
        sent = sendto(sock, datagram, length, 0, address, address_len);
        if (sent == (ssize_t)length) {
            return 0;
        }
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK ||
                         errno == ENOBUFS) &&
            retries < 10000) {
            struct timespec delay = {.tv_sec = 0, .tv_nsec = 100000L};

            retries++;
            nanosleep(&delay, NULL);
            continue;
        }
        fprintf(stderr, "wire-udp: sendto failed: %s\n", strerror(errno));
        return -1;
    }
}

/*
 * Send multiple shards with one sendmmsg when pacing spacing is zero.
 * Falls back to per-shard sendto on partial/unsupported failure.
 */
static int send_wire_datagrams_batch(int sock, const struct sockaddr *address,
                                     socklen_t address_len,
                                     const WireHeader *headers,
                                     const unsigned char *const *payloads,
                                     size_t count)
{
    unsigned char datagrams[WIRE_MAX_SHARDS][WIRE_HEADER_SIZE + PKG_SIZE];
    struct iovec iov[WIRE_MAX_SHARDS];
    struct mmsghdr msgs[WIRE_MAX_SHARDS];
    size_t i;
    int sent;
    int retries = 0;
    size_t offset = 0;

    if (count == 0 || count > WIRE_MAX_SHARDS) {
        return -1;
    }

    memset(msgs, 0, sizeof(msgs[0]) * count);
    for (i = 0; i < count; i++) {
        size_t length = WIRE_HEADER_SIZE + headers[i].payload_len;

        if (headers[i].payload_len > PKG_SIZE) {
            return -1;
        }
        wire_header_encode(datagrams[i], &headers[i]);
        if (headers[i].payload_len > 0 && payloads[i] != NULL) {
            memcpy(datagrams[i] + WIRE_HEADER_SIZE, payloads[i],
                   headers[i].payload_len);
        }
        iov[i].iov_base = datagrams[i];
        iov[i].iov_len = length;
        msgs[i].msg_hdr.msg_name = (void *)address;
        msgs[i].msg_hdr.msg_namelen = address_len;
        msgs[i].msg_hdr.msg_iov = &iov[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }

    while (offset < count) {
        sent = sendmmsg(sock, msgs + offset, (unsigned int)(count - offset), 0);
        if (sent > 0) {
            offset += (size_t)sent;
            retries = 0;
            continue;
        }
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK ||
                         errno == ENOBUFS) &&
            retries < 10000) {
            struct timespec delay = {.tv_sec = 0, .tv_nsec = 100000L};

            retries++;
            nanosleep(&delay, NULL);
            continue;
        }
        /* Fallback: finish remaining shards with sendto. */
        for (i = offset; i < count; i++) {
            if (send_wire_datagram(sock, address, address_len, &headers[i],
                                   payloads[i]) != 0) {
                return -1;
            }
        }
        return 0;
    }
    return 0;
}

int wire_udp_tx_init(WireUdpTx *tx, const char *host, uint16_t port,
                     uint32_t flow_id, double source_rate_mbps,
                     uint8_t final_dst, uint8_t ttl)
{
    if (tx == NULL || host == NULL || port == 0 || source_rate_mbps < 0.0 ||
        final_dst == 0 || ttl == 0) {
        return -1;
    }

    memset(tx, 0, sizeof(*tx));
    tx->sock = -1;
    tx->sock = open_sender_socket(host, port, &tx->address, &tx->address_len);
    if (tx->sock < 0) {
        return -1;
    }
    tx->flow_id = flow_id;
    tx->source_rate_mbps = source_rate_mbps;
    tx->final_dst = final_dst;
    tx->ttl = ttl;
    tx->started = monotonic_seconds();
    return 0;
}

void wire_udp_tx_destroy(WireUdpTx *tx)
{
    if (tx != NULL && tx->sock >= 0) {
        close(tx->sock);
        tx->sock = -1;
    }
}

bool wire_udp_tx_ready(const WireUdpTx *tx)
{
    double target;

    if (tx == NULL || tx->sock < 0) {
        return false;
    }
    if (tx->source_rate_mbps <= 0.0) {
        return true;
    }
    target = tx->started + ((double)tx->source_bytes * 8.0) /
                           (tx->source_rate_mbps * 1000000.0);
    return monotonic_seconds() >= target;
}

int wire_udp_tx_send_block(WireUdpTx *tx, const Codec *codec,
                           const unsigned char *encoded_block,
                           size_t valid_len, uint64_t encode_begin_ns,
                           uint64_t encode_end_ns)
{
    size_t output_size;
    size_t input_size;
    uint16_t shard_count;
    uint16_t shard;

    if (tx == NULL || codec == NULL || encoded_block == NULL || tx->sock < 0) {
        return -1;
    }
    input_size = Codec_input_block_size(codec);
    output_size = Codec_output_block_size(codec);
    if (input_size == 0 || output_size == 0 || output_size > CODEC_MAX_ENCODE_BLOCK ||
        output_size % PKG_SIZE != 0 || valid_len == 0 || valid_len > input_size) {
        return -1;
    }
    shard_count = (uint16_t)(output_size / PKG_SIZE);
    /*
     * Hybrid pacing:
     * - spacing >= SLEEP: nanosleep between shards
     * - MIN .. SLEEP: busy-wait between shards (avoids Wi-Fi/queue microbursts
     *   without giving up high source rates)
     * - spacing < MIN: burst shards via sendmmsg; block-level pacing remains
     */
    {
        double block_budget_sec = 0.0;
        double shard_spacing_sec = 0.0;
        double shard_target = 0.0;
        WireHeader headers[WIRE_MAX_SHARDS];
        const unsigned char *payloads[WIRE_MAX_SHARDS];

        if (tx->source_rate_mbps > 0.0 && shard_count > 1 && valid_len > 0) {
            block_budget_sec =
                ((double)valid_len * 8.0) / (tx->source_rate_mbps * 1000000.0);
            shard_spacing_sec = block_budget_sec / (double)shard_count;
            if (shard_spacing_sec < WIRE_SHARD_PACE_MIN_SEC) {
                shard_spacing_sec = 0.0;
            }
            if (shard_spacing_sec > 0.0) {
                shard_target = monotonic_seconds();
            }
        }

        for (shard = 0; shard < shard_count; shard++) {
            headers[shard] = (WireHeader){
                .type = WIRE_TYPE_DATA,
                .final_dst = tx->final_dst,
                .ttl = tx->ttl,
                .flow_id = tx->flow_id,
                .block_id = tx->block_id,
                .shard_index = shard,
                .shard_count = shard_count,
                .valid_len = (uint16_t)valid_len,
                .payload_len = PKG_SIZE,
                .encode_begin_ns = encode_begin_ns,
                .encode_end_ns = encode_end_ns,
            };
            payloads[shard] = encoded_block + (size_t)shard * PKG_SIZE;
        }

        if (shard_spacing_sec <= 0.0) {
            if (send_wire_datagrams_batch(tx->sock,
                                          (const struct sockaddr *)&tx->address,
                                          tx->address_len, headers, payloads,
                                          shard_count) != 0) {
                return -1;
            }
        } else {
            for (shard = 0; shard < shard_count; shard++) {
                if (send_wire_datagram(tx->sock,
                                       (const struct sockaddr *)&tx->address,
                                       tx->address_len, &headers[shard],
                                       payloads[shard]) != 0) {
                    return -1;
                }
                if (shard + 1 < shard_count) {
                    shard_target += shard_spacing_sec;
                    sleep_until_monotonic(shard_target);
                }
            }
        }
    }
    tx->source_bytes += valid_len;
    tx->block_id++;
    return 0;
}

int wire_udp_tx_send_end(WireUdpTx *tx, const Codec *codec)
{
    size_t output_size;
    WireHeader end;

    if (tx == NULL || codec == NULL || tx->sock < 0) {
        return -1;
    }
    output_size = Codec_output_block_size(codec);
    if (output_size == 0 || output_size % PKG_SIZE != 0) {
        return -1;
    }
    end = (WireHeader){
        .type = WIRE_TYPE_END,
        .final_dst = tx->final_dst,
        .ttl = tx->ttl,
        .flow_id = tx->flow_id,
        .block_id = tx->block_id,
        .shard_count = (uint16_t)(output_size / PKG_SIZE),
    };
    return send_wire_datagram(tx->sock, (const struct sockaddr *)&tx->address,
                              tx->address_len, &end, NULL);
}

int wire_udp_send(const WireUdpSendConfig *config)
{
    const Codec *codec;
    struct sockaddr_storage address;
    socklen_t address_len = 0;
    unsigned char block[CODEC_MAX_ENCODE_BLOCK];
    size_t input_size;
    size_t output_size;
    uint16_t shard_count;
    FILE *input = NULL;
    uint64_t block_id = 0;
    uint64_t source_bytes = 0;
    uint64_t wire_bytes = 0;
    double started;
    double elapsed;
    int sock = -1;
    int result = -1;

    if (config == NULL || config->host == NULL || config->input_path == NULL ||
        config->port == 0 || config->final_dst == 0 || config->ttl == 0) {
        return -1;
    }

    codec = Codec_get(config->codec_kind);
    if (codec == NULL) {
        return -1;
    }
    input_size = Codec_input_block_size(codec);
    output_size = Codec_output_block_size(codec);
    if (input_size == 0 || output_size == 0 || output_size > sizeof(block) ||
        output_size % PKG_SIZE != 0) {
        return -1;
    }
    shard_count = (uint16_t)(output_size / PKG_SIZE);
    if (shard_count == 0 || shard_count > WIRE_MAX_SHARDS ||
        shard_count != Codec_data_shards(codec) + Codec_parity_shards(codec)) {
        return -1;
    }

    input = fopen(config->input_path, "rb");
    if (input == NULL) {
        goto cleanup;
    }
    sock = open_sender_socket(config->host, config->port, &address, &address_len);
    if (sock < 0) {
        goto cleanup;
    }

    started = monotonic_seconds();
    for (;;) {
        WireHeader header;
        uint64_t encode_begin_ns;
        uint64_t encode_end_ns;
        size_t n = fread(block, 1, input_size, input);
        uint16_t shard;

        if (n == 0) {
            if (ferror(input)) {
                goto cleanup;
            }
            break;
        }

        memset(block + n, 0, input_size - n);
        encode_begin_ns = realtime_nanoseconds();
        Codec_encode(codec, block, output_size);
        encode_end_ns = realtime_nanoseconds();
        for (shard = 0; shard < shard_count; shard++) {
            header = (WireHeader){
                .type = WIRE_TYPE_DATA,
                .final_dst = config->final_dst,
                .ttl = config->ttl,
                .flow_id = config->flow_id,
                .block_id = block_id,
                .shard_index = shard,
                .shard_count = shard_count,
                .valid_len = (uint16_t)n,
                .payload_len = PKG_SIZE,
                .encode_begin_ns = encode_begin_ns,
                .encode_end_ns = encode_end_ns,
            };
            if (send_wire_datagram(sock, (struct sockaddr *)&address, address_len,
                                   &header, block + (size_t)shard * PKG_SIZE) != 0) {
                goto cleanup;
            }
            wire_bytes += WIRE_HEADER_SIZE + PKG_SIZE;
        }
        source_bytes += n;
        block_id++;
        pace_to_source_rate(started, source_bytes, config->source_rate_mbps);
    }

    {
        WireHeader end = {
            .type = WIRE_TYPE_END,
            .final_dst = config->final_dst,
            .ttl = config->ttl,
            .flow_id = config->flow_id,
            .block_id = block_id,
            .shard_count = shard_count,
        };
        if (send_wire_datagram(sock, (struct sockaddr *)&address, address_len,
                               &end, NULL) != 0) {
            goto cleanup;
        }
        wire_bytes += WIRE_HEADER_SIZE;
    }

    elapsed = monotonic_seconds() - started;
    fprintf(stderr,
            "udp-send: source_bytes=%llu wire_bytes=%llu elapsed=%.3fs "
            "source_mbps=%.2f wire_mbps=%.2f blocks=%llu\n",
            (unsigned long long)source_bytes, (unsigned long long)wire_bytes, elapsed,
            elapsed > 0.0 ? (double)source_bytes * 8.0 / elapsed / 1000000.0 : 0.0,
            elapsed > 0.0 ? (double)wire_bytes * 8.0 / elapsed / 1000000.0 : 0.0,
            (unsigned long long)block_id);
    result = 0;

cleanup:
    if (input != NULL) {
        fclose(input);
    }
    if (sock >= 0) {
        close(sock);
    }
    return result;
}

static int open_receiver_socket(uint16_t port)
{
    struct sockaddr_in address;
    int enable = 1;
    int sock;

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        return -1;
    }
    (void)setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));
    wire_set_udp_buffers(sock);

    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port);
    if (bind(sock, (struct sockaddr *)&address, sizeof(address)) != 0) {
        close(sock);
        return -1;
    }
    return sock;
}


static int wire_udp_file_output(uint32_t flow_id, const uint8_t *data, size_t len,
                                void *ctx)
{
    FILE *output = ctx;

    (void)flow_id;
    if (output == NULL || data == NULL) {
        return -1;
    }
    if (fwrite(data, 1, len, output) != len) {
        return -1;
    }
    return 0;
}

#define WIRE_MAX_FLOWS MF_MAX_FLOWS
#define WIRE_DECODE_QUEUE_CAP 2048u
#define WIRE_RECV_POLL_MS     100

typedef struct WireDecodeJob {
    WireHeader header;
    uint16_t   payload_len;
    uint8_t    payload[PKG_SIZE];
} WireDecodeJob;

typedef struct WireDecodeQueue {
    WireDecodeJob   *slots;
    size_t           cap;
    size_t           head;
    size_t           tail;
    size_t           count;
    int              recv_done;
    pthread_mutex_t  mtx;
    pthread_cond_t   not_empty;
    pthread_cond_t   not_full;
} WireDecodeQueue;

/*
 * Wire UDP receive demux key is WireHeader.flow_id only (full uint32_t).
 * Peer sockaddr is stored for output naming / logs and never used for lookup.
 *
 * Recv thread: parse + enqueue. One decode worker per flow (recover/emit/write),
 * matching per-flow encode+send threads on the TX path.
 */
typedef struct WireFlowCtx {
    bool              active;
    uint32_t          flow_id;
    struct sockaddr_storage peer_addr;
    socklen_t         peer_addr_len;
    WireFlowDecoder  *dec;
    FILE             *output;
    char              output_path[512];
    int               decode_mark;
    int               decode_mark_written;
    const char       *codec_name;
    /* Cached decode config for late decoder init (single-flow bind). */
    const Codec      *codec;
    uint16_t          expected_shards;
    size_t            input_size;
    int               best_effort;
    int               queue_inited;
    int               worker_started;
    pthread_t         worker;
    WireDecodeQueue   q;
    atomic_int        decode_complete;
    atomic_int        worker_failed;
} WireFlowCtx;

static void wire_flow_queue_destroy(WireFlowCtx *flow)
{
    if (flow == NULL || !flow->queue_inited) {
        return;
    }
    pthread_cond_destroy(&flow->q.not_full);
    pthread_cond_destroy(&flow->q.not_empty);
    pthread_mutex_destroy(&flow->q.mtx);
    free(flow->q.slots);
    flow->q.slots = NULL;
    flow->queue_inited = 0;
}

static int wire_flow_queue_init(WireFlowCtx *flow)
{
    if (flow == NULL) {
        return -1;
    }
    memset(&flow->q, 0, sizeof(flow->q));
    flow->q.cap = WIRE_DECODE_QUEUE_CAP;
    flow->q.slots = calloc(flow->q.cap, sizeof(*flow->q.slots));
    if (flow->q.slots == NULL) {
        return -1;
    }
    if (pthread_mutex_init(&flow->q.mtx, NULL) != 0) {
        free(flow->q.slots);
        flow->q.slots = NULL;
        return -1;
    }
    if (pthread_cond_init(&flow->q.not_empty, NULL) != 0) {
        pthread_mutex_destroy(&flow->q.mtx);
        free(flow->q.slots);
        flow->q.slots = NULL;
        return -1;
    }
    if (pthread_cond_init(&flow->q.not_full, NULL) != 0) {
        pthread_cond_destroy(&flow->q.not_empty);
        pthread_mutex_destroy(&flow->q.mtx);
        free(flow->q.slots);
        flow->q.slots = NULL;
        return -1;
    }
    flow->queue_inited = 1;
    return 0;
}

static void *wire_flow_decode_thread(void *arg)
{
    WireFlowCtx *flow = arg;

    if (flow == NULL) {
        return NULL;
    }

    for (;;) {
        WireDecodeJob job;

        pthread_mutex_lock(&flow->q.mtx);
        while (flow->q.count == 0 && !flow->q.recv_done) {
            pthread_cond_wait(&flow->q.not_empty, &flow->q.mtx);
        }
        if (flow->q.count == 0 && flow->q.recv_done) {
            pthread_mutex_unlock(&flow->q.mtx);
            break;
        }
        job = flow->q.slots[flow->q.head];
        flow->q.head = (flow->q.head + 1u) % flow->q.cap;
        flow->q.count--;
        pthread_cond_signal(&flow->q.not_full);
        pthread_mutex_unlock(&flow->q.mtx);

        if (wire_flow_decoder_ingest(
                flow->dec, &job.header, job.payload,
                job.header.type == WIRE_TYPE_DATA ? (size_t)job.payload_len
                                                  : 0) != 0) {
            atomic_store(&flow->worker_failed, 1);
            pthread_mutex_lock(&flow->q.mtx);
            flow->q.recv_done = 1;
            pthread_cond_broadcast(&flow->q.not_full);
            pthread_mutex_unlock(&flow->q.mtx);
            return NULL;
        }
        if (wire_flow_decoder_is_complete(flow->dec)) {
            atomic_store(&flow->decode_complete, 1);
        }
    }

    if (flow->best_effort) {
        if (wire_flow_decoder_flush_best_effort(flow->dec) != 0) {
            atomic_store(&flow->worker_failed, 1);
            return NULL;
        }
        if (wire_flow_decoder_is_complete(flow->dec)) {
            atomic_store(&flow->decode_complete, 1);
        }
    }
    return NULL;
}

static int wire_flow_start_worker(WireFlowCtx *flow)
{
    if (flow == NULL || flow->dec == NULL || !flow->queue_inited) {
        return -1;
    }
    if (flow->worker_started) {
        return 0;
    }
    if (pthread_create(&flow->worker, NULL, wire_flow_decode_thread, flow) !=
        0) {
        return -1;
    }
    flow->worker_started = 1;
    return 0;
}

static void wire_flow_stop_worker(WireFlowCtx *flow)
{
    if (flow == NULL || !flow->worker_started) {
        return;
    }
    pthread_mutex_lock(&flow->q.mtx);
    flow->q.recv_done = 1;
    pthread_cond_broadcast(&flow->q.not_empty);
    pthread_cond_broadcast(&flow->q.not_full);
    pthread_mutex_unlock(&flow->q.mtx);
    (void)pthread_join(flow->worker, NULL);
    flow->worker_started = 0;
}

static int wire_flow_enqueue(WireFlowCtx *flow, const WireHeader *header,
                             const uint8_t *payload, size_t payload_len)
{
    WireDecodeJob *slot;

    if (flow == NULL || header == NULL || !flow->queue_inited) {
        return -1;
    }
    if (payload_len > PKG_SIZE) {
        return -1;
    }

    pthread_mutex_lock(&flow->q.mtx);
    while (flow->q.count == flow->q.cap && !flow->q.recv_done) {
        pthread_cond_wait(&flow->q.not_full, &flow->q.mtx);
    }
    if (flow->q.recv_done || atomic_load(&flow->worker_failed)) {
        pthread_mutex_unlock(&flow->q.mtx);
        return -1;
    }
    slot = &flow->q.slots[flow->q.tail];
    slot->header = *header;
    slot->payload_len = (uint16_t)payload_len;
    if (payload_len > 0 && payload != NULL) {
        memcpy(slot->payload, payload, payload_len);
    }
    flow->q.tail = (flow->q.tail + 1u) % flow->q.cap;
    flow->q.count++;
    pthread_cond_signal(&flow->q.not_empty);
    pthread_mutex_unlock(&flow->q.mtx);
    return 0;
}

static int wire_flow_ensure_decoder(WireFlowCtx *flow)
{
    WireFlowDecoderConfig cfg;

    if (flow == NULL || flow->output == NULL || flow->codec == NULL) {
        return -1;
    }
    if (flow->dec != NULL) {
        return wire_flow_start_worker(flow);
    }
    memset(&cfg, 0, sizeof(cfg));
    cfg.flow_id = flow->flow_id;
    cfg.codec = flow->codec;
    cfg.expected_shards = flow->expected_shards;
    cfg.best_effort = flow->best_effort;
    cfg.input_size = flow->input_size;
    cfg.output_fn = wire_udp_file_output;
    cfg.output_ctx = flow->output;
    flow->dec = wire_flow_decoder_create(&cfg);
    if (flow->dec == NULL) {
        return -1;
    }
    return wire_flow_start_worker(flow);
}

/*
 * Append a human-readable mark proving Codec_decode ran for this flow.
 * Called after all decoded plaintext has been written.
 */
static int wire_append_decode_mark(WireFlowCtx *flow)
{
    time_t now;
    struct tm tm_utc;
    char timebuf[64];
    char line[512];
    int n;
    const WireFlowDecoderStats *st;

    if (flow == NULL || flow->output == NULL || !flow->decode_mark ||
        flow->decode_mark_written || flow->dec == NULL) {
        return 0;
    }
    st = wire_flow_decoder_stats(flow->dec);
    if (st == NULL || st->decoded_blocks == 0) {
        return 0;
    }

    now = time(NULL);
    if (gmtime_r(&now, &tm_utc) == NULL) {
        return -1;
    }
    if (strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ", &tm_utc) == 0) {
        return -1;
    }

    n = snprintf(line, sizeof(line),
                 "\n[WG_DECODE_MARK] decoded=yes time_utc=%s codec=%s "
                 "decoded_blocks=%llu flow_id=%u "
                 "note=this_file_passed_Codec_decode_on_receiver\n",
                 timebuf,
                 flow->codec_name != NULL ? flow->codec_name : "unknown",
                 (unsigned long long)st->decoded_blocks,
                 flow->flow_id);
    if (n < 0 || (size_t)n >= sizeof(line)) {
        return -1;
    }
    if (fwrite(line, 1, (size_t)n, flow->output) != (size_t)n) {
        return -1;
    }
    flow->decode_mark_written = 1;
    fprintf(stderr,
            "udp-recv: decode-mark appended to %s (decoded_blocks=%llu time=%s)\n",
            flow->output_path, (unsigned long long)st->decoded_blocks, timebuf);
    return 0;
}

static int wire_flow_format_output_tag(const struct sockaddr_storage *addr,
                                       socklen_t addr_len, uint32_t flow_id,
                                       char *out, size_t out_len)
{
    char host[INET6_ADDRSTRLEN];
    unsigned port = 0;

    (void)addr_len;
    if (out == NULL || out_len == 0) {
        return -1;
    }

    if (addr != NULL && addr->ss_family == AF_INET) {
        const struct sockaddr_in *ipv4 = (const struct sockaddr_in *)addr;

        if (inet_ntop(AF_INET, &ipv4->sin_addr, host, sizeof(host)) == NULL) {
            return -1;
        }
        port = ntohs(ipv4->sin_port);
    } else if (addr != NULL && addr->ss_family == AF_INET6) {
        const struct sockaddr_in6 *ipv6 = (const struct sockaddr_in6 *)addr;

        if (inet_ntop(AF_INET6, &ipv6->sin6_addr, host, sizeof(host)) == NULL) {
            return -1;
        }
        port = ntohs(ipv6->sin6_port);
    } else {
        snprintf(host, sizeof(host), "unknown");
    }

    /* Peer tag is cosmetic; demux key is wire flow_id only. */
    if (snprintf(out, out_len, "src_%s_p%u_flow_%u", host, port, flow_id) < 0 ||
        strlen(out) >= out_len) {
        return -1;
    }
    return 0;
}

static int wire_flow_output_path(const char *prefix,
                                 const struct sockaddr_storage *peer,
                                 socklen_t peer_len, uint32_t flow_id,
                                 const char *suffix, char *out, size_t out_len)
{
    char tag[160];

    if (prefix == NULL || out == NULL) {
        return -1;
    }
    if (suffix == NULL) {
        suffix = ".ts";
    }
    if (wire_flow_format_output_tag(peer, peer_len, flow_id, tag,
                                    sizeof(tag)) != 0) {
        return -1;
    }
    if (snprintf(out, out_len, "%s%s%s", prefix, tag, suffix) < 0 ||
        strlen(out) >= out_len) {
        return -1;
    }
    return 0;
}

static const char *wire_recv_suffix_for_flow(const WireUdpRecvConfig *config,
                                            uint32_t wire_flow_id)
{
    if (config != NULL &&
        wire_flow_id < (uint32_t)(sizeof(config->out_suffix_set) /
                                 sizeof(config->out_suffix_set[0])) &&
        config->out_suffix_set[wire_flow_id]) {
        return config->out_suffix_by_flow[wire_flow_id];
    }
    return ".ts";
}

static WireFlowCtx *wire_flow_find(WireFlowCtx flows[], size_t max_flows,
                                   uint32_t flow_id)
{
    size_t index;

    for (index = 0; index < max_flows; index++) {
        if (flows[index].active && flows[index].flow_id == flow_id) {
            return &flows[index];
        }
    }
    return NULL;
}

static size_t wire_flow_active_count(const WireFlowCtx flows[], size_t max_flows)
{
    size_t index;
    size_t n = 0;

    for (index = 0; index < max_flows; index++) {
        if (flows[index].active) {
            n++;
        }
    }
    return n;
}

static WireFlowCtx *wire_flow_alloc(WireFlowCtx flows[], size_t max_flows,
                                    uint32_t flow_id,
                                    const struct sockaddr_storage *peer,
                                    socklen_t peer_len,
                                    const char *output_path,
                                    int decode_mark, const char *codec_name,
                                    const Codec *codec, uint16_t expected_shards,
                                    size_t input_size, int best_effort)
{
    size_t index;

    for (index = 0; index < max_flows; index++) {
        if (!flows[index].active) {
            memset(&flows[index], 0, sizeof(flows[index]));
            flows[index].active = true;
            flows[index].flow_id = flow_id;
            if (peer != NULL && peer_len > 0) {
                if (peer_len > sizeof(flows[index].peer_addr)) {
                    peer_len = (socklen_t)sizeof(flows[index].peer_addr);
                }
                memcpy(&flows[index].peer_addr, peer, peer_len);
                flows[index].peer_addr_len = peer_len;
            }
            flows[index].decode_mark = decode_mark;
            flows[index].codec_name = codec_name;
            flows[index].codec = codec;
            flows[index].expected_shards = expected_shards;
            flows[index].input_size = input_size;
            flows[index].best_effort = best_effort;
            atomic_init(&flows[index].decode_complete, 0);
            atomic_init(&flows[index].worker_failed, 0);
            if (output_path != NULL) {
                strncpy(flows[index].output_path, output_path,
                        sizeof(flows[index].output_path) - 1u);
                flows[index].output_path[sizeof(flows[index].output_path) - 1u] =
                    '\0';
                flows[index].output = fopen(output_path, "wb");
                if (flows[index].output == NULL) {
                    flows[index].active = false;
                    return NULL;
                }
            }
            if (wire_flow_queue_init(&flows[index]) != 0) {
                if (flows[index].output != NULL) {
                    fclose(flows[index].output);
                    flows[index].output = NULL;
                }
                flows[index].active = false;
                return NULL;
            }
            if (wire_flow_ensure_decoder(&flows[index]) != 0) {
                wire_flow_queue_destroy(&flows[index]);
                if (flows[index].output != NULL) {
                    fclose(flows[index].output);
                    flows[index].output = NULL;
                }
                flows[index].active = false;
                return NULL;
            }
            return &flows[index];
        }
    }
    return NULL;
}

static void wire_flow_close(WireFlowCtx *flow)
{
    if (flow == NULL || !flow->active) {
        return;
    }
    wire_flow_stop_worker(flow);
    wire_flow_queue_destroy(flow);
    wire_flow_decoder_destroy(flow->dec);
    flow->dec = NULL;
    if (flow->output != NULL) {
        fclose(flow->output);
        flow->output = NULL;
    }
    flow->active = false;
}

static bool wire_flows_all_complete(const WireFlowCtx flows[], size_t max_flows)
{
    size_t index;
    bool   saw_active = false;

    for (index = 0; index < max_flows; index++) {
        if (!flows[index].active) {
            continue;
        }
        saw_active = true;
        if (!atomic_load(&flows[index].decode_complete)) {
            return false;
        }
    }
    return saw_active;
}

static int wire_flows_any_failed(const WireFlowCtx flows[], size_t max_flows)
{
    size_t index;

    for (index = 0; index < max_flows; index++) {
        if (flows[index].active && atomic_load(&flows[index].worker_failed)) {
            return 1;
        }
    }
    return 0;
}

static void wire_flows_stop_workers(WireFlowCtx flows[], size_t max_flows)
{
    size_t index;

    for (index = 0; index < max_flows; index++) {
        if (flows[index].active) {
            wire_flow_stop_worker(&flows[index]);
        }
    }
}

int wire_udp_recv(const WireUdpRecvConfig *config)
{
    const Codec *codec;
    WireFlowCtx *flows = NULL;
    unsigned char datagram[WIRE_HEADER_SIZE + PKG_SIZE];
    size_t        input_size;
    size_t        output_size;
    uint16_t      expected_shards;
    size_t        max_flows;
    int           multi_mode;
    double        last_receive;
    int           sock = -1;
    int           result = -1;
    size_t        fi;
    uint64_t      drop_wrong_dst = 0;
    uint64_t      malformed_headers = 0;
    uint64_t      flow_capacity_rejects = 0;
    uint64_t      flows_created = 0;

    if (config == NULL || config->output_path == NULL || config->port == 0 ||
        config->idle_sec == 0) {
        return -1;
    }

    max_flows = config->max_flows;
    if (max_flows == 0) {
        max_flows = 1;
    } else if (max_flows > WIRE_MAX_FLOWS) {
        fprintf(stderr,
                "udp-recv: --max-flows %u exceeds compile limit %u\n",
                (unsigned)config->max_flows, (unsigned)WIRE_MAX_FLOWS);
        return -1;
    }
    multi_mode = max_flows > 1;

    /* Heap: each flow holds a reassembly window of large encode blocks. */
    flows = calloc(WIRE_MAX_FLOWS, sizeof(*flows));
    if (flows == NULL) {
        return -1;
    }

    codec = Codec_get(config->codec_kind);
    if (codec == NULL) {
        goto cleanup;
    }
    if (config->best_effort && !Codec_allows_best_effort(codec)) {
        goto cleanup;
    }
    input_size = Codec_input_block_size(codec);
    output_size = Codec_output_block_size(codec);
    if (input_size == 0 || output_size == 0 || output_size > CODEC_MAX_ENCODE_BLOCK ||
        output_size % PKG_SIZE != 0) {
        goto cleanup;
    }
    expected_shards = (uint16_t)(output_size / PKG_SIZE);
    if (expected_shards == 0 || expected_shards > WIRE_MAX_SHARDS ||
        expected_shards != Codec_data_shards(codec) + Codec_parity_shards(codec)) {
        goto cleanup;
    }

    sock = open_receiver_socket(config->port);
    if (sock < 0) {
        goto cleanup;
    }

    if (!multi_mode) {
        WireFlowCtx *flow;

        /* Single-file mode: bind path now; wire flow_id set on first packet. */
        flow = wire_flow_alloc(flows, 1, 0, NULL, 0, config->output_path,
                               config->decode_mark,
                               wire_codec_kind_name(config->codec_kind),
                               codec, expected_shards, input_size,
                               config->best_effort);
        if (flow == NULL) {
            goto cleanup;
        }
    }

    fprintf(stderr,
            "udp-recv: listening on UDP port %u (max_flows=%zu local_node_id=%u"
            ", demux=wire_flow_id%s%s%s)\n",
            (unsigned)config->port, max_flows,
            (unsigned)config->local_node_id,
            multi_mode ? ", prefix mode" : "",
            config->best_effort ? ", best-effort" : ", strict",
            config->decode_mark ? ", decode-mark" : "");
    fprintf(stderr, "udp-recv: per-flow decode threads (max_flows=%zu)\n",
            max_flows);

    last_receive = monotonic_seconds();
    for (;;) {
        struct pollfd poll_fd = {.fd = sock, .events = POLLIN};
        int           polled = poll(&poll_fd, 1, WIRE_RECV_POLL_MS);

        if (polled < 0 && errno == EINTR) {
            continue;
        }
        if (polled < 0) {
            goto cleanup;
        }
        if (wire_flows_any_failed(flows, max_flows)) {
            goto cleanup;
        }
        if (polled == 0) {
            bool saw_flow = false;

            for (fi = 0; fi < max_flows; fi++) {
                if (flows[fi].active) {
                    saw_flow = true;
                    break;
                }
            }
            if (saw_flow && wire_flows_all_complete(flows, max_flows)) {
                break;
            }
            if (saw_flow &&
                monotonic_seconds() - last_receive >= (double)config->idle_sec) {
                /*
                 * After idle_sec with no packets, stop even if END was never seen
                 * (path loss, sender crash). Workers drain queued shards, then
                 * best-effort flush. Report incomplete flows below.
                 */
                if (multi_mode) {
                    fprintf(stderr,
                            "udp-recv: idle for %u s; ending multi-flow receive\n",
                            config->idle_sec);
                } else {
                    fprintf(stderr,
                            "udp-recv: idle for %u s; ending receive\n",
                            config->idle_sec);
                }
                break;
            }
            if (!saw_flow &&
                monotonic_seconds() - last_receive >= (double)config->idle_sec) {
                fprintf(stderr,
                        "udp-recv: no datagrams for %u s; ending receive\n",
                        config->idle_sec);
                break;
            }
            continue;
        }
        if ((poll_fd.revents & POLLIN) == 0) {
            continue;
        }

        {
            ssize_t                 received;
            struct sockaddr_storage peer_addr;
            socklen_t               peer_len = (socklen_t)sizeof(peer_addr);
            WireHeader              header;
            WireFlowCtx            *flow;

            do {
                received = recvfrom(sock, datagram, sizeof(datagram), 0,
                                    (struct sockaddr *)&peer_addr, &peer_len);
            } while (received < 0 && errno == EINTR);
            if (received < 0) {
                goto cleanup;
            }
            last_receive = monotonic_seconds();

            /*
             * Parse wire header before allocating a receiver flow so junk UDP
             * cannot exhaust max_flows. Demux uses header.flow_id only.
             */
            if (wire_header_decode(&header, datagram, (size_t)received) != 0) {
                malformed_headers++;
                continue;
            }
            if (config->local_node_id != 0 &&
                !wire_header_is_local(&header, config->local_node_id)) {
                drop_wrong_dst++;
                continue;
            }
            if (header.type == WIRE_TYPE_DATA) {
                if ((size_t)received != WIRE_HEADER_SIZE + header.payload_len ||
                    header.payload_len != PKG_SIZE ||
                    !wire_flow_decoder_shard_count_ok(codec, header.shard_count,
                                                 expected_shards) ||
                    header.shard_index >= header.shard_count ||
                    header.valid_len == 0 || header.valid_len > input_size) {
                    malformed_headers++;
                    continue;
                }
            } else if (header.type == WIRE_TYPE_END) {
                if ((size_t)received != WIRE_HEADER_SIZE ||
                    header.payload_len != 0 ||
                    !wire_flow_decoder_shard_count_ok(codec, header.shard_count,
                                                 expected_shards)) {
                    malformed_headers++;
                    continue;
                }
            } else {
                malformed_headers++;
                continue;
            }

            flow = wire_flow_find(flows, max_flows, header.flow_id);
            if (flow == NULL) {
                if (multi_mode) {
                    char        path[512];
                    const char *suffix =
                        wire_recv_suffix_for_flow(config, header.flow_id);

                    if (wire_flow_output_path(config->output_path, &peer_addr,
                                              peer_len, header.flow_id, suffix,
                                              path, sizeof(path)) != 0) {
                        continue;
                    }
                    flow = wire_flow_alloc(flows, max_flows, header.flow_id,
                                           &peer_addr, peer_len, path,
                                           config->decode_mark,
                                           wire_codec_kind_name(config->codec_kind),
                                           codec, expected_shards, input_size,
                                           config->best_effort);
                    if (flow == NULL) {
                        flow_capacity_rejects++;
                        fprintf(stderr,
                                "udp-recv: flow_capacity_reject wire_flow_id=%u "
                                "active=%zu max_flows=%zu\n",
                                header.flow_id, wire_flow_active_count(flows,
                                                                       max_flows),
                                max_flows);
                        continue;
                    }
                    flows_created++;
                    fprintf(stderr,
                            "udp-recv: opened wire_flow_id=%u -> %s "
                            "(active=%zu)\n",
                            header.flow_id, path,
                            wire_flow_active_count(flows, max_flows));
                } else {
                    flow = &flows[0];
                    flow->flow_id = header.flow_id;
                    if (peer_len > sizeof(flow->peer_addr)) {
                        peer_len = (socklen_t)sizeof(flow->peer_addr);
                    }
                    memcpy(&flow->peer_addr, &peer_addr, peer_len);
                    flow->peer_addr_len = peer_len;
                    if (wire_flow_ensure_decoder(flow) != 0) {
                        goto cleanup;
                    }
                }
            }

            if (wire_flow_ensure_decoder(flow) != 0) {
                goto cleanup;
            }
            if (wire_flow_enqueue(
                    flow, &header, datagram + WIRE_HEADER_SIZE,
                    header.type == WIRE_TYPE_DATA ? (size_t)header.payload_len
                                                  : 0) != 0) {
                goto cleanup;
            }

            if (!multi_mode && atomic_load(&flow->decode_complete)) {
                break;
            }
            if (multi_mode && wire_flows_all_complete(flows, max_flows)) {
                break;
            }
        }
    }

    wire_flows_stop_workers(flows, max_flows);
    if (wire_flows_any_failed(flows, max_flows)) {
        result = -1;
    } else {
        result = 0;
    }
    for (fi = 0; fi < max_flows; fi++) {
        WireFlowCtx *flow = &flows[fi];
        uint64_t     missing_groups = 0;

        if (!flow->active) {
            continue;
        }

        {
            const WireFlowDecoderStats *st = wire_flow_decoder_stats(flow->dec);
            uint64_t next_block = wire_flow_decoder_next_block(flow->dec);
            uint64_t end_count = wire_flow_decoder_end_block_count(flow->dec);
            int end_seen = wire_flow_decoder_end_seen(flow->dec);

            if (!end_seen || next_block != end_count) {
                if (end_seen && end_count > next_block) {
                    missing_groups = end_count - next_block;
                }
                fprintf(stderr,
                        "udp-recv: flow %u incomplete: received_blocks=%llu "
                        "expected_blocks=%llu missing_groups=%llu "
                        "recovered_groups=%llu decoded_blocks=%llu "
                        "groups_received=%llu groups_recovered=%llu "
                        "groups_emitted=%llu groups_failed=%llu "
                        "window_overflow=%llu pending_recovered_groups=%llu "
                        "skipped_groups=%llu "
                        "dropped_groups=%llu late=%llu seen_datagrams=%llu\n",
                        flow->flow_id, (unsigned long long)next_block,
                        (unsigned long long)(end_seen ? end_count : 0),
                        (unsigned long long)missing_groups,
                        (unsigned long long)(st ? st->recovered_groups : 0),
                        (unsigned long long)(st ? st->decoded_blocks : 0),
                        (unsigned long long)(st ? st->groups_received : 0),
                        (unsigned long long)(st ? st->groups_recovered : 0),
                        (unsigned long long)(st ? st->groups_emitted : 0),
                        (unsigned long long)(st ? st->groups_failed : 0),
                        (unsigned long long)(st ? st->window_overflow : 0),
                        (unsigned long long)(st ? st->pending_recovered_groups : 0),
                        (unsigned long long)(st ? st->skipped_groups : 0),
                        (unsigned long long)(st ? st->dropped_groups : 0),
                        (unsigned long long)(st ? st->late_datagrams : 0),
                        (unsigned long long)(st ? st->seen_datagrams : 0));
                result = -1;
                continue;
            }
            if (flow->output != NULL && fflush(flow->output) != 0) {
                result = -1;
                continue;
            }
            if (wire_append_decode_mark(flow) != 0) {
                result = -1;
                continue;
            }
            if (flow->output != NULL && fflush(flow->output) != 0) {
                result = -1;
                continue;
            }
            if (st == NULL) {
                result = -1;
                continue;
            }
            fprintf(stderr,
                    "udp-recv: flow %u output=%s output_bytes=%llu datagrams=%llu "
                    "seen_datagrams=%llu "
                    "duplicates=%llu late=%llu malformed=%llu recovered_groups=%llu "
                    "dropped_groups=%llu missing_data_shards=%llu decoded_blocks=%llu "
                    "groups_received=%llu groups_recovered=%llu groups_emitted=%llu "
                    "groups_failed=%llu window_overflow=%llu pending_recovered_groups=%llu "
                    "skipped_groups=%llu decode_mark=%s\n",
                    flow->flow_id, flow->output_path,
                    (unsigned long long)st->output_bytes,
                    (unsigned long long)st->received_datagrams,
                    (unsigned long long)st->seen_datagrams,
                    (unsigned long long)st->duplicate_datagrams,
                    (unsigned long long)st->late_datagrams,
                    (unsigned long long)st->malformed_datagrams,
                    (unsigned long long)st->recovered_groups,
                    (unsigned long long)st->dropped_groups,
                    (unsigned long long)st->missing_data_shards,
                    (unsigned long long)st->decoded_blocks,
                    (unsigned long long)st->groups_received,
                    (unsigned long long)st->groups_recovered,
                    (unsigned long long)st->groups_emitted,
                    (unsigned long long)st->groups_failed,
                    (unsigned long long)st->window_overflow,
                    (unsigned long long)st->pending_recovered_groups,
                    (unsigned long long)st->skipped_groups,
                    flow->decode_mark_written ? "yes" : "no");
            wire_flow_decoder_print_latency(flow->dec);
        }
    }

    if (!multi_mode && !flows[0].active) {
        result = -1;
    }
    fprintf(stderr,
            "udp-recv: summary active_flows=%zu flows_created=%llu "
            "flow_capacity_rejects=%llu malformed_headers=%llu "
            "drop_wrong_dst=%llu\n",
            wire_flow_active_count(flows, max_flows),
            (unsigned long long)flows_created,
            (unsigned long long)flow_capacity_rejects,
            (unsigned long long)malformed_headers,
            (unsigned long long)drop_wrong_dst);
    if (drop_wrong_dst > 0) {
        fprintf(stderr,
                "udp-recv: drop_wrong_dst=%llu (final_dst != local_node_id)\n",
                (unsigned long long)drop_wrong_dst);
    }

cleanup:
    if (flows != NULL) {
        for (fi = 0; fi < WIRE_MAX_FLOWS; fi++) {
            wire_flow_close(&flows[fi]);
        }
        free(flows);
    }
    if (sock >= 0) {
        close(sock);
    }
    return result;
}
