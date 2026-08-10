#include "wire_flow_decoder.h"

#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct WireGroup {
    bool          in_use;
    uint64_t      block_id;
    uint16_t      shard_count;
    uint16_t      valid_len;
    uint8_t      *received_bits;
    uint64_t      encode_begin_ns;
    uint64_t      encode_end_ns;
    bool          timing_valid;
    unsigned char *data;
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

struct WireFlowDecoder {
    bool                 inited;
    bool                 complete;
    uint32_t             flow_id;
    const Codec         *codec;
    uint16_t             expected_shards;
    int                  best_effort;
    size_t               input_size;
    WireDecodeOutputFn   output_fn;
    void                *output_ctx;
    WireGroup            groups[WIRE_FLOW_GROUP_WINDOW];
    uint64_t             next_block;
    uint64_t             end_block_count;
    bool                 end_seen;
    WireFlowDecoderStats stats;
    LatencyStats         latency_stats;
};

static uint64_t realtime_nanoseconds(void)
{
    struct timespec now;

    if (clock_gettime(CLOCK_REALTIME, &now) != 0 || now.tv_sec < 0) {
        return 0;
    }
    return (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
}

static void release_group(WireGroup *group)
{
    if (group == NULL) {
        return;
    }
    free(group->data);
    free(group->received_bits);
    memset(group, 0, sizeof(*group));
}

static WireGroup *find_group(WireGroup groups[WIRE_FLOW_GROUP_WINDOW],
                             uint64_t block_id)
{
    size_t index;

    for (index = 0; index < WIRE_FLOW_GROUP_WINDOW; index++) {
        if (groups[index].in_use && groups[index].block_id == block_id) {
            return &groups[index];
        }
    }
    return NULL;
}

static WireGroup *allocate_group(WireGroup groups[WIRE_FLOW_GROUP_WINDOW],
                                 uint64_t block_id, uint16_t shard_count,
                                 uint16_t valid_len, uint64_t encode_begin_ns,
                                 uint64_t encode_end_ns)
{
    size_t index;
    size_t data_bytes;
    size_t bit_bytes;
    unsigned char *data;
    uint8_t *bits;

    if (shard_count == 0 || shard_count > WIRE_FLOW_MAX_SHARDS) {
        return NULL;
    }

    data_bytes = (size_t)shard_count * PKG_SIZE;
    bit_bytes = codec_present_bytes(shard_count);
    data = calloc(1, data_bytes);
    bits = calloc(1, bit_bytes);
    if (data == NULL || bits == NULL) {
        free(data);
        free(bits);
        return NULL;
    }

    for (index = 0; index < WIRE_FLOW_GROUP_WINDOW; index++) {
        if (!groups[index].in_use) {
            release_group(&groups[index]);
            groups[index] = (WireGroup){
                .in_use = true,
                .block_id = block_id,
                .shard_count = shard_count,
                .valid_len = valid_len,
                .encode_begin_ns = encode_begin_ns,
                .encode_end_ns = encode_end_ns,
                .timing_valid = encode_begin_ns != 0 &&
                                encode_end_ns >= encode_begin_ns,
                .received_bits = bits,
                .data = data,
            };
            return &groups[index];
        }
    }
    free(data);
    free(bits);
    return NULL;
}

static bool group_complete(const WireGroup *group)
{
    size_t shard;

    if (group == NULL || group->shard_count == 0 ||
        group->received_bits == NULL ||
        group->shard_count > WIRE_FLOW_MAX_SHARDS) {
        return false;
    }
    for (shard = 0; shard < group->shard_count; shard++) {
        if (!codec_present_get(group->received_bits, shard)) {
            return false;
        }
    }
    return true;
}

static unsigned group_received_count(const WireGroup *group)
{
    size_t shard;
    unsigned count = 0;

    if (group == NULL || group->received_bits == NULL) {
        return 0;
    }

    for (shard = 0; shard < group->shard_count; shard++) {
        if (codec_present_get(group->received_bits, shard)) {
            count++;
        }
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

    /*
     * Use the process RS geometry fixed at startup / decoder create.
     * Do not call RsCodec_set_profile_from_shard_count here: wire
     * shard_count is validation input, not a remote profile command.
     */
    data_shards = Codec_data_shards(codec);
    if (data_shards == 0 || group_received_count(group) < data_shards) {
        return 0;
    }

    status = Codec_recover(codec, group->data, group->received_bits,
                           group->shard_count);
    if (status == CODEC_RECOVER_UNAVAILABLE) {
        return 0;
    }
    if (status != CODEC_RECOVER_OK) {
        return -1;
    }

    codec_present_set_all(group->received_bits, group->shard_count);
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

void wire_flow_decoder_print_latency(const WireFlowDecoder *dec)
{
    const LatencyStats *stats;

    if (dec == NULL) {
        return;
    }
    stats = &dec->latency_stats;
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

static int write_decoded_group(WireFlowDecoder *dec, WireGroup *group)
{
    size_t input_size;
    size_t output_size;
    uint64_t decode_ready_ns;
    uint64_t decode_done_ns;

    if (dec == NULL || group == NULL || dec->codec == NULL ||
        dec->output_fn == NULL) {
        return -1;
    }

    input_size = Codec_input_block_size(dec->codec);
    output_size = (size_t)group->shard_count * PKG_SIZE;

    if (group->valid_len == 0 || group->valid_len > input_size ||
        output_size == 0 || output_size > CODEC_MAX_ENCODE_BLOCK) {
        return -1;
    }

    decode_ready_ns = realtime_nanoseconds();
    Codec_decode(dec->codec, group->data, output_size);
    decode_done_ns = realtime_nanoseconds();
    dec->stats.decoded_blocks++;
    if (group->timing_valid) {
        latency_stats_add(&dec->latency_stats, group->encode_begin_ns,
                          group->encode_end_ns, decode_ready_ns, decode_done_ns);
    }
    if (dec->output_fn(dec->flow_id, group->data, group->valid_len,
                       dec->output_ctx) != 0) {
        return -1;
    }
    dec->stats.output_bytes += group->valid_len;
    release_group(group);
    return 0;
}

static int flush_recoverable_groups(WireFlowDecoder *dec)
{
    for (;;) {
        WireGroup *group = find_group(dec->groups, dec->next_block);

        if (group == NULL) {
            return 0;
        }
        if (recover_group(group, dec->codec, &dec->stats.recovered_groups) != 0) {
            return -1;
        }
        if (!group_complete(group)) {
            return 0;
        }
        if (write_decoded_group(dec, group) != 0) {
            return -1;
        }
        dec->next_block++;
    }
}

static int write_best_effort_group(WireFlowDecoder *dec, WireGroup *group)
{
    size_t data_shards;
    size_t remaining;
    size_t shard;

    if (dec == NULL || group == NULL || dec->codec == NULL ||
        !Codec_is_systematic(dec->codec) || group->valid_len == 0 ||
        group->valid_len > Codec_input_block_size(dec->codec) ||
        dec->output_fn == NULL) {
        return -1;
    }

    data_shards = Codec_data_shards(dec->codec);
    if (data_shards == 0 || group->shard_count !=
        Codec_data_shards(dec->codec) + Codec_parity_shards(dec->codec)) {
        return -1;
    }

    remaining = group->valid_len;
    for (shard = 0; shard < data_shards && remaining > 0; shard++) {
        size_t shard_len = remaining > PKG_SIZE ? PKG_SIZE : remaining;

        if (codec_present_get(group->received_bits, shard)) {
            if (dec->output_fn(dec->flow_id,
                               group->data + shard * PKG_SIZE, shard_len,
                               dec->output_ctx) != 0) {
                return -1;
            }
            dec->stats.output_bytes += shard_len;
        } else {
            dec->stats.missing_data_shards++;
        }
        remaining -= shard_len;
    }

    release_group(group);
    return 0;
}

int wire_flow_decoder_flush_best_effort(WireFlowDecoder *dec)
{
    if (dec == NULL || !dec->inited) {
        return -1;
    }

    while (dec->next_block < dec->end_block_count) {
        WireGroup *group = find_group(dec->groups, dec->next_block);

        if (group == NULL) {
            dec->stats.dropped_groups++;
            dec->next_block++;
            continue;
        }
        if (recover_group(group, dec->codec, &dec->stats.recovered_groups) != 0) {
            return -1;
        }
        if (group_complete(group)) {
            if (write_decoded_group(dec, group) != 0) {
                return -1;
            }
        } else if (write_best_effort_group(dec, group) != 0) {
            return -1;
        }
        dec->next_block++;
    }

    if (dec->end_seen && dec->next_block >= dec->end_block_count) {
        dec->complete = true;
    }
    return 0;
}

int wire_flow_decoder_shard_count_ok(const Codec *codec, uint16_t shard_count,
                                     uint16_t expected_shards)
{
    (void)codec;
    /* All codecs, including RS: wire shard_count must match fixed geometry. */
    return shard_count == expected_shards;
}

WireFlowDecoder *wire_flow_decoder_create(const WireFlowDecoderConfig *config)
{
    WireFlowDecoder *dec;

    if (config == NULL || config->codec == NULL || config->output_fn == NULL ||
        config->expected_shards == 0) {
        return NULL;
    }
    dec = calloc(1, sizeof(*dec));
    if (dec == NULL) {
        return NULL;
    }
    dec->inited = true;
    dec->flow_id = config->flow_id;
    dec->codec = config->codec;
    dec->expected_shards = config->expected_shards;
    dec->best_effort = config->best_effort;
    dec->input_size = config->input_size;
    dec->output_fn = config->output_fn;
    dec->output_ctx = config->output_ctx;
    return dec;
}

void wire_flow_decoder_destroy(WireFlowDecoder *dec)
{
    size_t index;

    if (dec == NULL || !dec->inited) {
        free(dec);
        return;
    }
    for (index = 0; index < WIRE_FLOW_GROUP_WINDOW; index++) {
        release_group(&dec->groups[index]);
    }
    free(dec->latency_stats.samples);
    free(dec);
}

int wire_flow_decoder_ingest(WireFlowDecoder *dec, const WireHeader *header,
                             const uint8_t *payload, size_t payload_len)
{
    WireGroup *group;

    if (dec == NULL || !dec->inited || header == NULL || dec->codec == NULL ||
        dec->complete) {
        return 0;
    }

    if (header->type == WIRE_TYPE_END) {
        if (!wire_flow_decoder_shard_count_ok(dec->codec, header->shard_count,
                                              dec->expected_shards) ||
            header->payload_len != 0 ||
            (dec->end_seen && header->block_id != dec->end_block_count)) {
            dec->stats.malformed_datagrams++;
            return 0;
        }
        dec->end_seen = true;
        dec->end_block_count = header->block_id;
        if (dec->next_block == dec->end_block_count) {
            dec->complete = true;
        } else if (dec->best_effort) {
            if (wire_flow_decoder_flush_best_effort(dec) != 0) {
                return -1;
            }
            dec->complete = true;
        }
        return 0;
    }

    if (header->type != WIRE_TYPE_DATA ||
        !wire_flow_decoder_shard_count_ok(dec->codec, header->shard_count,
                                          dec->expected_shards) ||
        header->shard_index >= header->shard_count ||
        header->valid_len == 0 || header->valid_len > dec->input_size ||
        header->payload_len != PKG_SIZE || payload == NULL ||
        payload_len < PKG_SIZE) {
        dec->stats.malformed_datagrams++;
        return 0;
    }
    dec->stats.seen_datagrams++;
    if (header->block_id < dec->next_block) {
        dec->stats.late_datagrams++;
        return 0;
    }

    group = find_group(dec->groups, header->block_id);
    if (group == NULL) {
        group = allocate_group(dec->groups, header->block_id, header->shard_count,
                               header->valid_len, header->encode_begin_ns,
                               header->encode_end_ns);
        if (group == NULL) {
            dec->stats.dropped_groups++;
            return 0;
        }
    }
    if (group->shard_count != header->shard_count ||
        group->valid_len != header->valid_len ||
        group->encode_begin_ns != header->encode_begin_ns ||
        group->encode_end_ns != header->encode_end_ns) {
        dec->stats.malformed_datagrams++;
        return 0;
    }
    if (header->shard_index >= group->shard_count ||
        group->received_bits == NULL || group->data == NULL) {
        dec->stats.malformed_datagrams++;
        return 0;
    }
    if (codec_present_get(group->received_bits, header->shard_index)) {
        dec->stats.duplicate_datagrams++;
        return 0;
    }
    memcpy(group->data + (size_t)header->shard_index * PKG_SIZE, payload,
           PKG_SIZE);
    codec_present_set(group->received_bits, header->shard_index);
    dec->stats.received_datagrams++;

    if (flush_recoverable_groups(dec) != 0) {
        return -1;
    }
    if (dec->end_seen && dec->next_block == dec->end_block_count) {
        dec->complete = true;
    }
    return 0;
}

bool wire_flow_decoder_is_complete(const WireFlowDecoder *dec)
{
    return dec != NULL && dec->complete;
}

bool wire_flow_decoder_end_seen(const WireFlowDecoder *dec)
{
    return dec != NULL && dec->end_seen;
}

uint64_t wire_flow_decoder_next_block(const WireFlowDecoder *dec)
{
    return dec != NULL ? dec->next_block : 0;
}

uint64_t wire_flow_decoder_end_block_count(const WireFlowDecoder *dec)
{
    return dec != NULL ? dec->end_block_count : 0;
}

const WireFlowDecoderStats *wire_flow_decoder_stats(const WireFlowDecoder *dec)
{
    return dec != NULL ? &dec->stats : NULL;
}
