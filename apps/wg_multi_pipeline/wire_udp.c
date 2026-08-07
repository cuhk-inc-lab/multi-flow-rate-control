#include "wire_udp.h"

#include "codec.h"
#include "flow_peer_map.h"
#include "stream_config.h"
#include "wire_flow_decoder.h"
#include "wire_header.h"

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <netdb.h>
#include <poll.h>
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

typedef struct WireFlowKey {
    struct sockaddr_storage addr;
    socklen_t               addr_len;
    uint32_t                flow_id;
} WireFlowKey;

typedef struct WireFlowCtx {
    bool              active;
    WireFlowKey       key;
    WireFlowDecoder  *dec;
    FILE             *output;
    char              output_path[512];
    int               decode_mark;
    int               decode_mark_written;
    const char       *codec_name;
    /* Cached decode config for late decoder init (single-flow key assign). */
    const Codec      *codec;
    uint16_t          expected_shards;
    size_t            input_size;
    int               best_effort;
} WireFlowCtx;

static int wire_flow_ensure_decoder(WireFlowCtx *flow)
{
    WireFlowDecoderConfig cfg;

    if (flow == NULL || flow->output == NULL || flow->codec == NULL) {
        return -1;
    }
    if (flow->dec != NULL) {
        return 0;
    }
    memset(&cfg, 0, sizeof(cfg));
    cfg.flow_id = flow->key.flow_id;
    cfg.codec = flow->codec;
    cfg.expected_shards = flow->expected_shards;
    cfg.best_effort = flow->best_effort;
    cfg.input_size = flow->input_size;
    cfg.output_fn = wire_udp_file_output;
    cfg.output_ctx = flow->output;
    flow->dec = wire_flow_decoder_create(&cfg);
    return flow->dec != NULL ? 0 : -1;
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
                 flow->key.flow_id);
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

static int wire_flow_key_equal(const WireFlowKey *left, const WireFlowKey *right)
{
    if (left == NULL || right == NULL) {
        return 0;
    }
    if (left->flow_id != right->flow_id || left->addr_len != right->addr_len) {
        return 0;
    }
    return memcmp(&left->addr, &right->addr, left->addr_len) == 0;
}

static void wire_flow_key_from_peer(WireFlowKey *key,
                                    const struct sockaddr_storage *addr,
                                    socklen_t addr_len, uint32_t flow_id)
{
    if (key == NULL || addr == NULL) {
        return;
    }
    memset(key, 0, sizeof(*key));
    if (addr_len > sizeof(key->addr)) {
        addr_len = (socklen_t)sizeof(key->addr);
    }
    memcpy(&key->addr, addr, addr_len);
    key->addr_len = addr_len;
    key->flow_id = flow_id;
}

static int wire_flow_format_peer_tag(const WireFlowKey *key, char *out, size_t out_len)
{
    char host[INET6_ADDRSTRLEN];
    unsigned port = 0;

    if (key == NULL || out == NULL || out_len == 0) {
        return -1;
    }

    if (key->addr.ss_family == AF_INET) {
        const struct sockaddr_in *ipv4 = (const struct sockaddr_in *)&key->addr;

        if (inet_ntop(AF_INET, &ipv4->sin_addr, host, sizeof(host)) == NULL) {
            return -1;
        }
        port = ntohs(ipv4->sin_port);
    } else if (key->addr.ss_family == AF_INET6) {
        const struct sockaddr_in6 *ipv6 = (const struct sockaddr_in6 *)&key->addr;

        if (inet_ntop(AF_INET6, &ipv6->sin6_addr, host, sizeof(host)) == NULL) {
            return -1;
        }
        port = ntohs(ipv6->sin6_port);
    } else {
        snprintf(host, sizeof(host), "unknown");
    }

    if (snprintf(out, out_len, "src_%s_p%u_flow_%u", host, port, key->flow_id) < 0 ||
        strlen(out) >= out_len) {
        return -1;
    }
    return 0;
}

static int wire_flow_output_path(const char *prefix, const WireFlowKey *key,
                                 const char *suffix, char *out, size_t out_len)
{
    char tag[128];

    if (prefix == NULL || key == NULL || out == NULL) {
        return -1;
    }
    if (suffix == NULL) {
        suffix = ".ts";
    }
    if (wire_flow_format_peer_tag(key, tag, sizeof(tag)) != 0) {
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
                                   const WireFlowKey *key)
{
    size_t index;

    for (index = 0; index < max_flows; index++) {
        if (flows[index].active &&
            wire_flow_key_equal(&flows[index].key, key)) {
            return &flows[index];
        }
    }
    return NULL;
}

static WireFlowCtx *wire_flow_alloc(WireFlowCtx flows[], size_t max_flows,
                                    const WireFlowKey *key,
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
            flows[index].key = *key;
            flows[index].decode_mark = decode_mark;
            flows[index].codec_name = codec_name;
            flows[index].codec = codec;
            flows[index].expected_shards = expected_shards;
            flows[index].input_size = input_size;
            flows[index].best_effort = best_effort;
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
            if (wire_flow_ensure_decoder(&flows[index]) != 0) {
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
        if (!wire_flow_decoder_is_complete(flows[index].dec)) {
            return false;
        }
    }
    return saw_active;
}

int wire_udp_recv(const WireUdpRecvConfig *config)
{
    const Codec *codec;
    WireFlowCtx *flows = NULL;
    FlowPeerMap *flow_map = NULL;
    unsigned char datagram[WIRE_HEADER_SIZE + PKG_SIZE];
    size_t        input_size;
    size_t        output_size;
    uint16_t      expected_shards;
    size_t        max_flows;
    int           multi_mode;
    struct sockaddr_storage local_addr;
    socklen_t     local_len = (socklen_t)sizeof(local_addr);
    double        last_receive;
    int           sock = -1;
    int           result = -1;
    size_t        fi;
    uint64_t      drop_wrong_dst = 0;

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
    if (config->best_effort && !Codec_is_systematic(codec)) {
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
    if (getsockname(sock, (struct sockaddr *)&local_addr, &local_len) != 0) {
        goto cleanup;
    }
    if (flow_peer_map_init(&flow_map, (uint32_t)max_flows) != FPM_OK) {
        goto cleanup;
    }

    if (!multi_mode) {
        WireFlowKey  key = {0};
        WireFlowCtx *flow;

        flow = wire_flow_alloc(flows, 1, &key, config->output_path,
                               config->decode_mark,
                               wire_codec_kind_name(config->codec_kind),
                               codec, expected_shards, input_size,
                               config->best_effort);
        if (flow == NULL) {
            goto cleanup;
        }
    }

    fprintf(stderr,
            "udp-recv: listening on UDP port %u (max_flows=%zu local_node_id=%u%s%s)\n",
            (unsigned)config->port, max_flows,
            (unsigned)config->local_node_id,
            multi_mode ? ", prefix mode" : "",
            config->decode_mark ? ", decode-mark" : "");

    last_receive = monotonic_seconds();
    for (;;) {
        struct pollfd poll_fd = {.fd = sock, .events = POLLIN};
        int           polled = poll(&poll_fd, 1, 1000);

        if (polled < 0 && errno == EINTR) {
            continue;
        }
        if (polled < 0) {
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
            if (saw_flow &&
                monotonic_seconds() - last_receive >= (double)config->idle_sec) {
                if (multi_mode) {
                    /*
                     * Multi-flow receives can stall forever if some peers never
                     * send END (path loss, sender crash). After idle_sec with no
                     * packets, stop and report incomplete flows below.
                     */
                    fprintf(stderr,
                            "udp-recv: idle for %u s; ending multi-flow receive\n",
                            config->idle_sec);
                    break;
                }
                if (wire_flow_decoder_end_seen(flows[0].dec)) {
                    if (config->best_effort &&
                        !wire_flow_decoder_is_complete(flows[0].dec)) {
                        if (wire_flow_decoder_flush_best_effort(flows[0].dec) !=
                            0) {
                            goto cleanup;
                        }
                    }
                    break;
                }
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
            FlowTuple               tuple;
            uint32_t                mapped_flow_id;
            WireHeader              header;
            WireFlowKey             key;
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
             * Validate the wire header before peer-map allocation / opening
             * output files so junk UDP cannot exhaust max_flows.
             */
            if (wire_header_decode(&header, datagram, (size_t)received) != 0) {
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
                    continue;
                }
            } else if (header.type == WIRE_TYPE_END) {
                if ((size_t)received != WIRE_HEADER_SIZE ||
                    header.payload_len != 0 ||
                    !wire_flow_decoder_shard_count_ok(codec, header.shard_count,
                                                 expected_shards)) {
                    continue;
                }
            } else {
                continue;
            }

            if (flow_tuple_set(&tuple,
                               (struct sockaddr *)&peer_addr, peer_len,
                               (struct sockaddr *)&local_addr, local_len,
                               IPPROTO_UDP) != 0) {
                continue;
            }
            mapped_flow_id = flow_peer_map_lookup(flow_map, &tuple);
            if (mapped_flow_id == (uint32_t)-1) {
                continue;
            }

            wire_flow_key_from_peer(&key, &peer_addr, peer_len, mapped_flow_id);
            flow = wire_flow_find(flows, max_flows, &key);
            if (flow == NULL) {
                if (multi_mode) {
                    char        path[512];
                    const char *suffix =
                        wire_recv_suffix_for_flow(config, header.flow_id);

                    if (wire_flow_output_path(config->output_path, &key, suffix,
                                              path, sizeof(path)) != 0) {
                        continue;
                    }
                    flow = wire_flow_alloc(flows, max_flows, &key, path,
                                           config->decode_mark,
                                           wire_codec_kind_name(config->codec_kind),
                                           codec, expected_shards, input_size,
                                           config->best_effort);
                    if (flow == NULL) {
                        fprintf(stderr,
                                "udp-recv: flow table full, dropping mapped_flow=%u wire_flow=%u\n",
                                mapped_flow_id, header.flow_id);
                        continue;
                    }
                    fprintf(stderr, "udp-recv: opened flow %u (wire flow %u) -> %s\n",
                            mapped_flow_id, header.flow_id, path);
                } else {
                    flow = &flows[0];
                    flow->key = key;
                    if (wire_flow_ensure_decoder(flow) != 0) {
                        goto cleanup;
                    }
                }
            }

            if (wire_flow_ensure_decoder(flow) != 0) {
                goto cleanup;
            }
            if (wire_flow_decoder_ingest(
                    flow->dec, &header, datagram + WIRE_HEADER_SIZE,
                    header.type == WIRE_TYPE_DATA ? (size_t)header.payload_len
                                                  : 0) != 0) {
                goto cleanup;
            }

            if (!multi_mode && wire_flow_decoder_is_complete(flow->dec)) {
                break;
            }
            if (multi_mode && wire_flows_all_complete(flows, max_flows)) {
                break;
            }
        }
    }

    result = 0;
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
                        "expected_blocks=%llu missing_groups=%llu\n",
                        flow->key.flow_id, (unsigned long long)next_block,
                        (unsigned long long)(end_seen ? end_count : 0),
                        (unsigned long long)missing_groups);
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
                    "decode_mark=%s\n",
                    flow->key.flow_id, flow->output_path,
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
                    flow->decode_mark_written ? "yes" : "no");
            wire_flow_decoder_print_latency(flow->dec);
        }
    }

    if (!multi_mode && !flows[0].active) {
        result = -1;
    }
    if (drop_wrong_dst > 0) {
        fprintf(stderr,
                "udp-recv: drop_wrong_dst=%llu (final_dst != local_node_id)\n",
                (unsigned long long)drop_wrong_dst);
    }

cleanup:
    if (flow_map != NULL) {
        flow_peer_map_destroy(flow_map);
    }
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
