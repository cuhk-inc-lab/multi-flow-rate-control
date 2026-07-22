#include "wire_udp.h"

#include "codec.h"
#include "flow_peer_map.h"
#include "stream_config.h"

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

#define WIRE_MAGIC          0x57475031u /* WGP1 */
#define WIRE_VERSION        2u
#define WIRE_TYPE_DATA      1u
#define WIRE_TYPE_END       2u
#define WIRE_HEADER_SIZE    44u
#define WIRE_GROUP_WINDOW   128u
#define WIRE_MAX_SHARDS     (CODEC_MAX_ENCODE_BLOCK / PKG_SIZE)

typedef struct WireHeader {
    uint8_t  type;
    uint32_t flow_id;
    uint64_t block_id;
    uint16_t shard_index;
    uint16_t shard_count;
    uint16_t valid_len;
    uint16_t payload_len;
    /* Sender CLOCK_REALTIME timestamps; synchronize peers before comparing. */
    uint64_t encode_begin_ns;
    uint64_t encode_end_ns;
} WireHeader;

typedef struct WireGroup {
    bool          in_use;
    uint64_t      block_id;
    uint16_t      shard_count;
    uint16_t      valid_len;
    uint16_t      received_mask;
    uint64_t      encode_begin_ns;
    uint64_t      encode_end_ns;
    bool          timing_valid;
    unsigned char data[CODEC_MAX_ENCODE_BLOCK];
} WireGroup;

typedef struct LatencySample {
    uint64_t encode_ns;
    uint64_t transfer_ns;
    uint64_t decode_ns;
    uint64_t end_to_end_ns;
    uint64_t jitter_ns;
} LatencySample;

typedef struct LatencyStats {
    LatencySample *samples;
    size_t         count;
    size_t         capacity;
    uint64_t       invalid_samples;
    bool           have_previous_delay;
    uint64_t       previous_delay_ns;
    bool           disabled;
} LatencyStats;

static uint64_t host_to_be64(uint64_t value)
{
    uint32_t high = htonl((uint32_t)(value >> 32));
    uint32_t low = htonl((uint32_t)value);

    return ((uint64_t)low << 32) | high;
}

static uint64_t be64_to_host(uint64_t value)
{
    uint32_t high = ntohl((uint32_t)value);
    uint32_t low = ntohl((uint32_t)(value >> 32));

    return ((uint64_t)high << 32) | low;
}

static void wire_header_encode(unsigned char out[WIRE_HEADER_SIZE],
                               const WireHeader *header)
{
    uint32_t value32;
    uint64_t value64;
    uint16_t value16;

    value32 = htonl(WIRE_MAGIC);
    memcpy(out, &value32, sizeof(value32));
    out[4] = WIRE_VERSION;
    out[5] = header->type;
    out[6] = 0;
    out[7] = 0;

    value32 = htonl(header->flow_id);
    memcpy(out + 8, &value32, sizeof(value32));
    value64 = host_to_be64(header->block_id);
    memcpy(out + 12, &value64, sizeof(value64));
    value16 = htons(header->shard_index);
    memcpy(out + 20, &value16, sizeof(value16));
    value16 = htons(header->shard_count);
    memcpy(out + 22, &value16, sizeof(value16));
    value16 = htons(header->valid_len);
    memcpy(out + 24, &value16, sizeof(value16));
    value16 = htons(header->payload_len);
    memcpy(out + 26, &value16, sizeof(value16));
    value64 = host_to_be64(header->encode_begin_ns);
    memcpy(out + 28, &value64, sizeof(value64));
    value64 = host_to_be64(header->encode_end_ns);
    memcpy(out + 36, &value64, sizeof(value64));
}

static int wire_header_decode(WireHeader *header,
                              const unsigned char *data, size_t len)
{
    uint32_t value32;
    uint64_t value64;
    uint16_t value16;

    if (header == NULL || data == NULL || len < WIRE_HEADER_SIZE) {
        return -1;
    }

    memcpy(&value32, data, sizeof(value32));
    if (ntohl(value32) != WIRE_MAGIC || data[4] != WIRE_VERSION) {
        return -1;
    }

    header->type = data[5];
    memcpy(&value32, data + 8, sizeof(value32));
    header->flow_id = ntohl(value32);
    memcpy(&value64, data + 12, sizeof(value64));
    header->block_id = be64_to_host(value64);
    memcpy(&value16, data + 20, sizeof(value16));
    header->shard_index = ntohs(value16);
    memcpy(&value16, data + 22, sizeof(value16));
    header->shard_count = ntohs(value16);
    memcpy(&value16, data + 24, sizeof(value16));
    header->valid_len = ntohs(value16);
    memcpy(&value16, data + 26, sizeof(value16));
    header->payload_len = ntohs(value16);
    memcpy(&value64, data + 28, sizeof(value64));
    header->encode_begin_ns = be64_to_host(value64);
    memcpy(&value64, data + 36, sizeof(value64));
    header->encode_end_ns = be64_to_host(value64);
    return 0;
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

    if (header->payload_len > PKG_SIZE) {
        return -1;
    }

    wire_header_encode(datagram, header);
    if (header->payload_len > 0 && payload != NULL) {
        memcpy(datagram + WIRE_HEADER_SIZE, payload, header->payload_len);
    }

    do {
        sent = sendto(sock, datagram, length, 0, address, address_len);
    } while (sent < 0 && errno == EINTR);

    return sent == (ssize_t)length ? 0 : -1;
}

int wire_udp_tx_init(WireUdpTx *tx, const char *host, uint16_t port,
                     uint32_t flow_id, double source_rate_mbps)
{
    if (tx == NULL || host == NULL || port == 0 || source_rate_mbps < 0.0) {
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
    for (shard = 0; shard < shard_count; shard++) {
        WireHeader header = {
            .type = WIRE_TYPE_DATA,
            .flow_id = tx->flow_id,
            .block_id = tx->block_id,
            .shard_index = shard,
            .shard_count = shard_count,
            .valid_len = (uint16_t)valid_len,
            .payload_len = PKG_SIZE,
            .encode_begin_ns = encode_begin_ns,
            .encode_end_ns = encode_end_ns,
        };

        if (send_wire_datagram(tx->sock, (const struct sockaddr *)&tx->address,
                               tx->address_len, &header,
                               encoded_block + (size_t)shard * PKG_SIZE) != 0) {
            return -1;
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
        config->port == 0 || config->codec_kind == CODEC_KIND_NONE) {
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

static WireGroup *find_group(WireGroup groups[WIRE_GROUP_WINDOW], uint64_t block_id)
{
    size_t index;

    for (index = 0; index < WIRE_GROUP_WINDOW; index++) {
        if (groups[index].in_use && groups[index].block_id == block_id) {
            return &groups[index];
        }
    }
    return NULL;
}

static WireGroup *allocate_group(WireGroup groups[WIRE_GROUP_WINDOW],
                                 uint64_t block_id, uint16_t shard_count,
                                 uint16_t valid_len,
                                 uint64_t encode_begin_ns,
                                 uint64_t encode_end_ns)
{
    size_t index;

    for (index = 0; index < WIRE_GROUP_WINDOW; index++) {
        if (!groups[index].in_use) {
            groups[index] = (WireGroup){
                .in_use = true,
                .block_id = block_id,
                .shard_count = shard_count,
                .valid_len = valid_len,
                .encode_begin_ns = encode_begin_ns,
                .encode_end_ns = encode_end_ns,
                .timing_valid = encode_begin_ns != 0 &&
                                encode_end_ns >= encode_begin_ns,
            };
            return &groups[index];
        }
    }
    return NULL;
}

static bool group_complete(const WireGroup *group)
{
    uint16_t required;

    if (group == NULL || group->shard_count == 0 || group->shard_count > 16) {
        return false;
    }
    required = (uint16_t)((1u << group->shard_count) - 1u);
    return group->received_mask == required;
}

static unsigned group_received_count(const WireGroup *group)
{
    uint16_t mask;
    unsigned count = 0;

    if (group == NULL) {
        return 0;
    }

    mask = group->received_mask;
    while (mask != 0) {
        count += mask & 1u;
        mask >>= 1;
    }
    return count;
}

static int recover_group(WireGroup *group, const Codec *codec,
                         uint64_t *recovered_groups)
{
    size_t data_shards;
    CodecRecoverStatus status;

    if (group == NULL || codec == NULL || group_complete(group)) {
        return 0;
    }

    data_shards = Codec_data_shards(codec);
    if (data_shards == 0 || group_received_count(group) < data_shards) {
        return 0;
    }

    status = Codec_recover(codec, group->data, group->received_mask);
    if (status == CODEC_RECOVER_UNAVAILABLE) {
        return 0;
    }
    if (status != CODEC_RECOVER_OK) {
        return -1;
    }

    group->received_mask = (uint16_t)((1u << group->shard_count) - 1u);
    if (recovered_groups != NULL) {
        (*recovered_groups)++;
    }

    return 0;
}

static int compare_u64(const void *left, const void *right)
{
    uint64_t a = *(const uint64_t *)left;
    uint64_t b = *(const uint64_t *)right;

    return (a > b) - (a < b);
}

static uint64_t latency_sample_value(const LatencySample *sample, unsigned field)
{
    switch (field) {
    case 0:
        return sample->encode_ns;
    case 1:
        return sample->transfer_ns;
    case 2:
        return sample->decode_ns;
    case 3:
        return sample->end_to_end_ns;
    default:
        return sample->jitter_ns;
    }
}

static void latency_stats_add(LatencyStats *stats, uint64_t encode_begin_ns,
                              uint64_t encode_end_ns, uint64_t ready_ns,
                              uint64_t decode_done_ns)
{
    LatencySample *resized;
    LatencySample *sample;

    if (stats == NULL || stats->disabled) {
        return;
    }
    if (encode_begin_ns == 0 || encode_end_ns < encode_begin_ns ||
        ready_ns < encode_end_ns || decode_done_ns < ready_ns) {
        stats->invalid_samples++;
        return;
    }
    if (stats->count == stats->capacity) {
        size_t new_capacity = stats->capacity == 0 ? 1024u : stats->capacity * 2u;

        if (new_capacity <= stats->capacity ||
            new_capacity > SIZE_MAX / sizeof(*stats->samples)) {
            stats->disabled = true;
            return;
        }
        resized = realloc(stats->samples, new_capacity * sizeof(*stats->samples));
        if (resized == NULL) {
            stats->disabled = true;
            return;
        }
        stats->samples = resized;
        stats->capacity = new_capacity;
    }

    sample = &stats->samples[stats->count++];
    sample->encode_ns = encode_end_ns - encode_begin_ns;
    sample->transfer_ns = ready_ns - encode_end_ns;
    sample->decode_ns = decode_done_ns - ready_ns;
    sample->end_to_end_ns = decode_done_ns - encode_begin_ns;
    sample->jitter_ns = UINT64_MAX;
    if (stats->have_previous_delay) {
        sample->jitter_ns = sample->end_to_end_ns >= stats->previous_delay_ns
                                ? sample->end_to_end_ns - stats->previous_delay_ns
                                : stats->previous_delay_ns - sample->end_to_end_ns;
    }
    stats->previous_delay_ns = sample->end_to_end_ns;
    stats->have_previous_delay = true;
}

static void latency_stats_print_metric(const LatencyStats *stats,
                                       const char *name, unsigned field)
{
    uint64_t *values;
    uint64_t minimum = UINT64_MAX;
    uint64_t maximum = 0;
    long double total = 0.0;
    size_t count = 0;
    size_t index;

    if (stats == NULL || stats->count == 0) {
        return;
    }
    values = malloc(stats->count * sizeof(*values));
    if (values == NULL) {
        fprintf(stderr, "latency %s: unable to allocate percentile samples\n", name);
        return;
    }
    for (index = 0; index < stats->count; index++) {
        uint64_t value = latency_sample_value(&stats->samples[index], field);

        if (value == UINT64_MAX) {
            continue;
        }
        values[count++] = value;
        total += (long double)value;
        if (value < minimum) {
            minimum = value;
        }
        if (value > maximum) {
            maximum = value;
        }
    }
    if (count == 0) {
        free(values);
        return;
    }
    qsort(values, count, sizeof(*values), compare_u64);
    fprintf(stderr,
            "latency %s: samples=%zu avg_us=%.3Lf min_us=%.3f p50_us=%.3f "
            "p95_us=%.3f p99_us=%.3f max_us=%.3f\n",
            name, count, total / (long double)count / 1000.0L,
            (double)minimum / 1000.0,
            (double)values[(count - 1u) * 50u / 100u] / 1000.0,
            (double)values[(count - 1u) * 95u / 100u] / 1000.0,
            (double)values[(count - 1u) * 99u / 100u] / 1000.0,
            (double)maximum / 1000.0);
    free(values);
}

static void latency_stats_print(const LatencyStats *stats)
{
    if (stats == NULL) {
        return;
    }
    fprintf(stderr, "latency: completed_blocks=%zu invalid_samples=%" PRIu64
                    " collection=%s\n",
            stats->count, stats->invalid_samples,
            stats->disabled ? "disabled" : "enabled");
    latency_stats_print_metric(stats, "encode", 0);
    latency_stats_print_metric(stats, "transfer", 1);
    latency_stats_print_metric(stats, "decode", 2);
    latency_stats_print_metric(stats, "end_to_end", 3);
    latency_stats_print_metric(stats, "end_to_end_jitter", 4);
}

static int write_decoded_group(WireGroup *group, const Codec *codec,
                               FILE *output, uint64_t *output_bytes,
                               LatencyStats *latency_stats)
{
    size_t input_size = Codec_input_block_size(codec);
    size_t output_size = Codec_output_block_size(codec);
    uint64_t decode_ready_ns;
    uint64_t decode_done_ns;

    if (group == NULL || group->valid_len == 0 || group->valid_len > input_size ||
        group->shard_count * PKG_SIZE != output_size) {
        return -1;
    }
    decode_ready_ns = realtime_nanoseconds();
    Codec_decode(codec, group->data, output_size);
    decode_done_ns = realtime_nanoseconds();
    if (group->timing_valid) {
        latency_stats_add(latency_stats, group->encode_begin_ns,
                          group->encode_end_ns, decode_ready_ns, decode_done_ns);
    }
    if (fwrite(group->data, 1, group->valid_len, output) != group->valid_len) {
        return -1;
    }
    *output_bytes += group->valid_len;
    group->in_use = false;
    return 0;
}

static int flush_recoverable_groups(WireGroup groups[WIRE_GROUP_WINDOW],
                                    uint64_t *next_block, const Codec *codec,
                                    FILE *output, uint64_t *output_bytes,
                                    uint64_t *recovered_groups,
                                    LatencyStats *latency_stats)
{
    for (;;) {
        WireGroup *group = find_group(groups, *next_block);

        if (group == NULL) {
            return 0;
        }
        if (recover_group(group, codec, recovered_groups) != 0) {
            return -1;
        }
        if (!group_complete(group)) {
            return 0;
        }
        if (write_decoded_group(group, codec, output, output_bytes,
                                latency_stats) != 0) {
            return -1;
        }
        (*next_block)++;
    }
}

static int write_best_effort_group(WireGroup *group, const Codec *codec,
                                   FILE *output, uint64_t *output_bytes,
                                   uint64_t *missing_data_shards)
{
    size_t data_shards;
    size_t remaining;
    size_t shard;

    if (group == NULL || codec == NULL || !Codec_is_systematic(codec) ||
        group->valid_len == 0 ||
        group->valid_len > Codec_input_block_size(codec)) {
        return -1;
    }

    data_shards = Codec_data_shards(codec);
    if (data_shards == 0 || group->shard_count !=
        Codec_data_shards(codec) + Codec_parity_shards(codec)) {
        return -1;
    }

    remaining = group->valid_len;
    for (shard = 0; shard < data_shards && remaining > 0; shard++) {
        size_t shard_len = remaining > PKG_SIZE ? PKG_SIZE : remaining;

        if ((group->received_mask & (uint16_t)(1u << shard)) != 0) {
            if (fwrite(group->data + shard * PKG_SIZE, 1, shard_len, output) != shard_len) {
                return -1;
            }
            *output_bytes += shard_len;
        } else if (missing_data_shards != NULL) {
            (*missing_data_shards)++;
        }
        remaining -= shard_len;
    }

    group->in_use = false;
    return 0;
}

static int flush_best_effort_groups(WireGroup groups[WIRE_GROUP_WINDOW],
                                    uint64_t *next_block,
                                    uint64_t end_block_count,
                                    const Codec *codec, FILE *output,
                                    uint64_t *output_bytes,
                                    uint64_t *recovered_groups,
                                    uint64_t *dropped_groups,
                                    uint64_t *missing_data_shards,
                                    LatencyStats *latency_stats)
{
    while (*next_block < end_block_count) {
        WireGroup *group = find_group(groups, *next_block);

        if (group == NULL) {
            (*dropped_groups)++;
            (*next_block)++;
            continue;
        }
        if (recover_group(group, codec, recovered_groups) != 0) {
            return -1;
        }
        if (group_complete(group)) {
            if (write_decoded_group(group, codec, output, output_bytes,
                                    latency_stats) != 0) {
                return -1;
            }
        } else if (write_best_effort_group(group, codec, output, output_bytes,
                                           missing_data_shards) != 0) {
            return -1;
        }
        (*next_block)++;
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
    bool         active;
    bool         complete;
    WireFlowKey  key;
    WireGroup    groups[WIRE_GROUP_WINDOW];
    uint64_t     next_block;
    uint64_t     end_block_count;
    bool         end_seen;
    FILE        *output;
    char         output_path[512];
    uint64_t     output_bytes;
    uint64_t     received_datagrams;
    uint64_t     duplicate_datagrams;
    uint64_t     late_datagrams;
    uint64_t     malformed_datagrams;
    uint64_t     dropped_groups;
    uint64_t     recovered_groups;
    uint64_t     missing_data_shards;
    LatencyStats latency_stats;
} WireFlowCtx;

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
                                 char *out, size_t out_len)
{
    char tag[128];

    if (prefix == NULL || key == NULL || out == NULL) {
        return -1;
    }
    if (wire_flow_format_peer_tag(key, tag, sizeof(tag)) != 0) {
        return -1;
    }
    if (snprintf(out, out_len, "%s%s.ts", prefix, tag) < 0 || strlen(out) >= out_len) {
        return -1;
    }
    return 0;
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
                                    const char *output_path)
{
    size_t index;

    for (index = 0; index < max_flows; index++) {
        if (!flows[index].active) {
            memset(&flows[index], 0, sizeof(flows[index]));
            flows[index].active = true;
            flows[index].key = *key;
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
    free(flow->latency_stats.samples);
    flow->latency_stats.samples = NULL;
    if (flow->output != NULL) {
        fclose(flow->output);
        flow->output = NULL;
    }
    flow->active = false;
}

static int wire_flow_process_datagram(WireFlowCtx *flow, const WireHeader *header,
                                      const unsigned char *payload,
                                      size_t input_size, uint16_t expected_shards,
                                      const Codec *codec, int best_effort)
{
    WireGroup *group;
    uint16_t   bit;

    if (flow == NULL || header == NULL || codec == NULL || flow->complete) {
        return 0;
    }

    if (header->type == WIRE_TYPE_END) {
        if (header->shard_count != expected_shards || header->payload_len != 0 ||
            (flow->end_seen && header->block_id != flow->end_block_count)) {
            flow->malformed_datagrams++;
            return 0;
        }
        flow->end_seen = true;
        flow->end_block_count = header->block_id;
        if (flow->next_block == flow->end_block_count) {
            flow->complete = true;
        } else if (best_effort) {
            if (flush_best_effort_groups(flow->groups, &flow->next_block,
                                         flow->end_block_count, codec,
                                         flow->output, &flow->output_bytes,
                                         &flow->recovered_groups,
                                         &flow->dropped_groups,
                                         &flow->missing_data_shards,
                                         &flow->latency_stats) != 0) {
                return -1;
            }
            flow->complete = true;
        }
        return 0;
    }

    if (header->type != WIRE_TYPE_DATA || header->shard_count != expected_shards ||
        header->shard_index >= expected_shards ||
        header->valid_len == 0 || header->valid_len > input_size ||
        header->payload_len != PKG_SIZE) {
        flow->malformed_datagrams++;
        return 0;
    }
    if (header->block_id < flow->next_block) {
        flow->late_datagrams++;
        return 0;
    }

    group = find_group(flow->groups, header->block_id);
    if (group == NULL) {
        group = allocate_group(flow->groups, header->block_id, header->shard_count,
                               header->valid_len, header->encode_begin_ns,
                               header->encode_end_ns);
        if (group == NULL) {
            flow->dropped_groups++;
            return 0;
        }
    }
    if (group->shard_count != header->shard_count ||
        group->valid_len != header->valid_len ||
        group->encode_begin_ns != header->encode_begin_ns ||
        group->encode_end_ns != header->encode_end_ns) {
        flow->malformed_datagrams++;
        return 0;
    }
    bit = (uint16_t)(1u << header->shard_index);
    if ((group->received_mask & bit) != 0) {
        flow->duplicate_datagrams++;
        return 0;
    }
    memcpy(group->data + (size_t)header->shard_index * PKG_SIZE, payload, PKG_SIZE);
    group->received_mask |= bit;
    flow->received_datagrams++;

    if (flush_recoverable_groups(flow->groups, &flow->next_block, codec,
                                 flow->output, &flow->output_bytes,
                                 &flow->recovered_groups,
                                 &flow->latency_stats) != 0) {
        return -1;
    }
    if (flow->end_seen && flow->next_block == flow->end_block_count) {
        flow->complete = true;
    }
    return 0;
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
        if (!flows[index].complete) {
            return false;
        }
    }
    return saw_active;
}

int wire_udp_recv(const WireUdpRecvConfig *config)
{
    const Codec *codec;
    WireFlowCtx  flows[WIRE_MAX_FLOWS] = {0};
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

    if (config == NULL || config->output_path == NULL || config->port == 0 ||
        config->codec_kind == CODEC_KIND_NONE || config->idle_sec == 0) {
        return -1;
    }

    max_flows = config->max_flows;
    if (max_flows == 0) {
        max_flows = 1;
    } else if (max_flows > WIRE_MAX_FLOWS) {
        max_flows = WIRE_MAX_FLOWS;
    }
    multi_mode = max_flows > 1;

    codec = Codec_get(config->codec_kind);
    if (codec == NULL) {
        return -1;
    }
    if (config->best_effort && !Codec_is_systematic(codec)) {
        return -1;
    }
    input_size = Codec_input_block_size(codec);
    output_size = Codec_output_block_size(codec);
    if (input_size == 0 || output_size == 0 || output_size > CODEC_MAX_ENCODE_BLOCK ||
        output_size % PKG_SIZE != 0) {
        return -1;
    }
    expected_shards = (uint16_t)(output_size / PKG_SIZE);
    if (expected_shards == 0 || expected_shards > WIRE_MAX_SHARDS ||
        expected_shards != Codec_data_shards(codec) + Codec_parity_shards(codec)) {
        return -1;
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

        flow = wire_flow_alloc(flows, 1, &key, config->output_path);
        if (flow == NULL) {
            goto cleanup;
        }
    }

    fprintf(stderr,
            "udp-recv: listening on UDP port %u (max_flows=%zu%s)\n",
            (unsigned)config->port, max_flows,
            multi_mode ? ", prefix mode" : "");

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
                if (flows[0].end_seen) {
                    if (config->best_effort && !flows[0].complete) {
                        if (flush_best_effort_groups(
                                flows[0].groups, &flows[0].next_block,
                                flows[0].end_block_count, codec, flows[0].output,
                                &flows[0].output_bytes, &flows[0].recovered_groups,
                                &flows[0].dropped_groups,
                                &flows[0].missing_data_shards,
                                &flows[0].latency_stats) != 0) {
                            goto cleanup;
                        }
                        flows[0].complete = true;
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
            if (wire_header_decode(&header, datagram, (size_t)received) != 0) {
                continue;
            }
            if (header.type == WIRE_TYPE_DATA &&
                (size_t)received != WIRE_HEADER_SIZE + header.payload_len) {
                continue;
            }

            wire_flow_key_from_peer(&key, &peer_addr, peer_len, mapped_flow_id);
            flow = wire_flow_find(flows, max_flows, &key);
            if (flow == NULL) {
                if (multi_mode) {
                    char path[512];

                    if (wire_flow_output_path(config->output_path, &key, path,
                                                sizeof(path)) != 0) {
                        continue;
                    }
                    flow = wire_flow_alloc(flows, max_flows, &key, path);
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
                }
            }

            if (wire_flow_process_datagram(flow, &header,
                                           datagram + WIRE_HEADER_SIZE, input_size,
                                           expected_shards, codec,
                                           config->best_effort) != 0) {
                goto cleanup;
            }

            if (!multi_mode && flow->complete) {
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

        if (!flow->end_seen || flow->next_block != flow->end_block_count) {
            if (flow->end_seen && flow->end_block_count > flow->next_block) {
                missing_groups = flow->end_block_count - flow->next_block;
            }
            fprintf(stderr,
                    "udp-recv: flow %u incomplete: received_blocks=%llu "
                    "expected_blocks=%llu missing_groups=%llu\n",
                    flow->key.flow_id, (unsigned long long)flow->next_block,
                    (unsigned long long)(flow->end_seen ? flow->end_block_count : 0),
                    (unsigned long long)missing_groups);
            result = -1;
            continue;
        }
        if (flow->output != NULL && fflush(flow->output) != 0) {
            result = -1;
            continue;
        }
        fprintf(stderr,
                "udp-recv: flow %u output=%s output_bytes=%llu datagrams=%llu "
                "duplicates=%llu late=%llu malformed=%llu recovered_groups=%llu "
                "dropped_groups=%llu missing_data_shards=%llu\n",
                flow->key.flow_id, flow->output_path,
                (unsigned long long)flow->output_bytes,
                (unsigned long long)flow->received_datagrams,
                (unsigned long long)flow->duplicate_datagrams,
                (unsigned long long)flow->late_datagrams,
                (unsigned long long)flow->malformed_datagrams,
                (unsigned long long)flow->recovered_groups,
                (unsigned long long)flow->dropped_groups,
                (unsigned long long)flow->missing_data_shards);
        latency_stats_print(&flow->latency_stats);
    }

    if (!multi_mode && !flows[0].active) {
        result = -1;
    }

cleanup:
    if (flow_map != NULL) {
        flow_peer_map_destroy(flow_map);
    }
    for (fi = 0; fi < max_flows; fi++) {
        wire_flow_close(&flows[fi]);
    }
    if (sock >= 0) {
        close(sock);
    }
    return result;
}
