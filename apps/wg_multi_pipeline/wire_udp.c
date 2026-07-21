#include "wire_udp.h"

#include "codec.h"
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
                .flow_id = 0,
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
            .flow_id = 0,
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

int wire_udp_recv(const WireUdpRecvConfig *config)
{
    const Codec *codec;
    WireGroup groups[WIRE_GROUP_WINDOW] = {0};
    unsigned char datagram[WIRE_HEADER_SIZE + PKG_SIZE];
    size_t input_size;
    size_t output_size;
    uint16_t expected_shards;
    uint64_t next_block = 0;
    uint64_t end_block_count = 0;
    uint64_t output_bytes = 0;
    uint64_t received_datagrams = 0;
    uint64_t duplicate_datagrams = 0;
    uint64_t late_datagrams = 0;
    uint64_t malformed_datagrams = 0;
    uint64_t dropped_groups = 0;
    uint64_t missing_groups = 0;
    uint64_t recovered_groups = 0;
    uint64_t missing_data_shards = 0;
    LatencyStats latency_stats = {0};
    bool end_seen = false;
    double last_receive;
    FILE *output = NULL;
    int sock = -1;
    int result = -1;

    if (config == NULL || config->output_path == NULL || config->port == 0 ||
        config->codec_kind == CODEC_KIND_NONE || config->idle_sec == 0) {
        return -1;
    }
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

    output = fopen(config->output_path, "wb");
    if (output == NULL) {
        goto cleanup;
    }
    sock = open_receiver_socket(config->port);
    if (sock < 0) {
        goto cleanup;
    }

    fprintf(stderr, "udp-recv: listening on UDP port %u\n", (unsigned)config->port);
    last_receive = monotonic_seconds();
    for (;;) {
        struct pollfd poll_fd = {.fd = sock, .events = POLLIN};
        int polled = poll(&poll_fd, 1, 1000);

        if (polled < 0 && errno == EINTR) {
            continue;
        }
        if (polled < 0) {
            goto cleanup;
        }
        if (polled == 0) {
            if (end_seen &&
                monotonic_seconds() - last_receive >= (double)config->idle_sec) {
                if (config->best_effort &&
                    flush_best_effort_groups(groups, &next_block,
                                             end_block_count, codec, output,
                                             &output_bytes, &recovered_groups,
                                             &dropped_groups,
                                             &missing_data_shards,
                                             &latency_stats) != 0) {
                    goto cleanup;
                }
                break;
            }
            continue;
        }
        if ((poll_fd.revents & POLLIN) == 0) {
            continue;
        }

        {
            ssize_t received;
            WireHeader header;
            WireGroup *group;
            uint16_t bit;

            do {
                received = recvfrom(sock, datagram, sizeof(datagram), 0, NULL, NULL);
            } while (received < 0 && errno == EINTR);
            if (received < 0) {
                goto cleanup;
            }
            last_receive = monotonic_seconds();
            if (wire_header_decode(&header, datagram, (size_t)received) != 0) {
                malformed_datagrams++;
                continue;
            }

            if (header.type == WIRE_TYPE_END) {
                if (header.flow_id != 0 || header.shard_count != expected_shards ||
                    header.payload_len != 0 || (end_seen && header.block_id != end_block_count)) {
                    malformed_datagrams++;
                    continue;
                }
                end_seen = true;
                end_block_count = header.block_id;
                if (next_block == end_block_count) {
                    break;
                }
                continue;
            }

            if (header.type != WIRE_TYPE_DATA || header.flow_id != 0 ||
                header.shard_count != expected_shards ||
                header.shard_index >= expected_shards ||
                header.valid_len == 0 || header.valid_len > input_size ||
                header.payload_len != PKG_SIZE ||
                (size_t)received != WIRE_HEADER_SIZE + header.payload_len) {
                malformed_datagrams++;
                continue;
            }
            if (header.block_id < next_block) {
                late_datagrams++;
                continue;
            }

            group = find_group(groups, header.block_id);
            if (group == NULL) {
                group = allocate_group(groups, header.block_id, header.shard_count,
                                       header.valid_len, header.encode_begin_ns,
                                       header.encode_end_ns);
                if (group == NULL) {
                    dropped_groups++;
                    continue;
                }
            }
            if (group->shard_count != header.shard_count ||
                group->valid_len != header.valid_len ||
                group->encode_begin_ns != header.encode_begin_ns ||
                group->encode_end_ns != header.encode_end_ns) {
                malformed_datagrams++;
                continue;
            }
            bit = (uint16_t)(1u << header.shard_index);
            if ((group->received_mask & bit) != 0) {
                duplicate_datagrams++;
                continue;
            }
            memcpy(group->data + (size_t)header.shard_index * PKG_SIZE,
                   datagram + WIRE_HEADER_SIZE, PKG_SIZE);
            group->received_mask |= bit;
            received_datagrams++;

            if (flush_recoverable_groups(groups, &next_block, codec, output,
                                         &output_bytes, &recovered_groups,
                                         &latency_stats) != 0) {
                goto cleanup;
            }
            if (end_seen && next_block == end_block_count) {
                break;
            }
        }
    }

    if (!end_seen || next_block != end_block_count) {
        if (end_seen && end_block_count > next_block) {
            missing_groups = end_block_count - next_block;
        }
        fprintf(stderr, "udp-recv: incomplete transfer: received_blocks=%llu expected_blocks=%llu\n",
                (unsigned long long)next_block,
                (unsigned long long)(end_seen ? end_block_count : 0));
        fprintf(stderr, "udp-recv: missing_groups=%llu\n",
                (unsigned long long)missing_groups);
        goto cleanup;
    }
    if (fflush(output) != 0) {
        goto cleanup;
    }
    fprintf(stderr,
            "udp-recv: output_bytes=%llu datagrams=%llu duplicates=%llu late=%llu "
            "malformed=%llu recovered_groups=%llu dropped_groups=%llu "
            "missing_data_shards=%llu\n",
            (unsigned long long)output_bytes,
            (unsigned long long)received_datagrams,
            (unsigned long long)duplicate_datagrams,
            (unsigned long long)late_datagrams,
            (unsigned long long)malformed_datagrams,
            (unsigned long long)recovered_groups,
            (unsigned long long)dropped_groups,
            (unsigned long long)missing_data_shards);
    latency_stats_print(&latency_stats);
    result = 0;

cleanup:
    free(latency_stats.samples);
    if (output != NULL) {
        fclose(output);
    }
    if (sock >= 0) {
        close(sock);
    }
    return result;
}
