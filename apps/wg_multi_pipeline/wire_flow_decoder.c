#include "wire_flow_decoder.h"

#include <assert.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef enum WireGroupState {
    WIRE_GROUP_EMPTY = 0,
    WIRE_GROUP_ACTIVE,
    WIRE_GROUP_RECOVERED,
    WIRE_GROUP_EMITTED,
    WIRE_GROUP_FAILED,
} WireGroupState;

typedef struct WireGroup {
    WireGroupState state;
    uint64_t       block_id;
    uint16_t       shard_count;
    uint16_t       valid_len;
    uint8_t       *received_bits;
    uint64_t       encode_begin_ns;
    uint64_t       encode_end_ns;
    bool           timing_valid;
    unsigned char *data;
    unsigned       last_recovery_count;
} WireGroup;

typedef enum WireRecoverResult {
    WIRE_RECOVER_PENDING = 0,
    WIRE_RECOVER_OK,
    WIRE_RECOVER_FAILED,
} WireRecoverResult;

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
    uint64_t             next_emit_block;
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
        if (groups[index].state != WIRE_GROUP_EMPTY &&
            groups[index].block_id == block_id) {
            return &groups[index];
        }
    }
    return NULL;
}

static bool window_has_empty_slot(const WireGroup groups[WIRE_FLOW_GROUP_WINDOW])
{
    size_t index;

    for (index = 0; index < WIRE_FLOW_GROUP_WINDOW; index++) {
        if (groups[index].state == WIRE_GROUP_EMPTY) {
            return true;
        }
    }
    return false;
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
        if (groups[index].state == WIRE_GROUP_EMPTY) {
            release_group(&groups[index]);
            groups[index] = (WireGroup){
                .state = WIRE_GROUP_ACTIVE,
                .block_id = block_id,
                .shard_count = shard_count,
                .valid_len = valid_len,
                .encode_begin_ns = encode_begin_ns,
                .encode_end_ns = encode_end_ns,
                .timing_valid = encode_begin_ns != 0 &&
                                encode_end_ns >= encode_begin_ns,
                .received_bits = bits,
                .data = data,
                .last_recovery_count = 0,
            };
            return &groups[index];
        }
    }
    free(data);
    free(bits);
    return NULL;
}

static bool group_all_shards_present(const WireGroup *group)
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

/*
 * Systematic codecs lay out original data in shards [0, Codec_data_shards()).
 * All of those must be present — not merely received_count >= data_shards.
 */
static bool group_systematic_data_ready(const WireGroup *group,
                                        const Codec *codec)
{
    size_t data_shards;
    size_t parity_shards;
    size_t shard;

    if (group == NULL || codec == NULL || group->received_bits == NULL ||
        !Codec_is_systematic(codec)) {
        return false;
    }
    data_shards = Codec_data_shards(codec);
    parity_shards = Codec_parity_shards(codec);
    if (data_shards == 0 ||
        group->shard_count != data_shards + parity_shards) {
        return false;
    }
    for (shard = 0; shard < data_shards; shard++) {
        if (!codec_present_get(group->received_bits, shard)) {
            return false;
        }
    }
    return true;
}

/*
 * Mark group RECOVERED when emit-ready. For RS FEC recovery, present bits are
 * set to all shards after Codec_recover OK so Codec_decode sees a full group.
 * Systematic fast-path only requires data shards present; parity slots may stay
 * unset — same contract as before (Codec_decode reads encoded layout).
 */
static void mark_group_recovered(WireGroup *group, WireFlowDecoderStats *stats,
                                 bool via_fec_recover)
{
    assert(group->state == WIRE_GROUP_ACTIVE);
    if (via_fec_recover) {
        codec_present_set_all(group->received_bits, group->shard_count);
    }
    group->state = WIRE_GROUP_RECOVERED;
    if (stats != NULL) {
        stats->recovered_groups++;
        stats->groups_recovered++;
    }
}

static WireRecoverResult try_recover_group(WireFlowDecoder *dec,
                                           WireGroup *group,
                                           bool finalize_at_end)
{
    size_t data_shards;
    unsigned count;
    CodecRecoverStatus status;

    if (dec == NULL || group == NULL || dec->codec == NULL) {
        return WIRE_RECOVER_PENDING;
    }

    if (group->state == WIRE_GROUP_RECOVERED ||
        group->state == WIRE_GROUP_EMITTED) {
        return WIRE_RECOVER_OK;
    }
    if (group->state == WIRE_GROUP_FAILED) {
        return WIRE_RECOVER_FAILED;
    }
    if (group->state != WIRE_GROUP_ACTIVE) {
        return WIRE_RECOVER_PENDING;
    }

    if (group_all_shards_present(group)) {
        mark_group_recovered(group, &dec->stats, false);
        return WIRE_RECOVER_OK;
    }

    if (group_systematic_data_ready(group, dec->codec)) {
        mark_group_recovered(group, &dec->stats, false);
        return WIRE_RECOVER_OK;
    }

    data_shards = Codec_data_shards(dec->codec);
    count = group_received_count(group);
    if (data_shards == 0 || count < data_shards) {
        if (finalize_at_end) {
            group->state = WIRE_GROUP_FAILED;
            dec->stats.groups_failed++;
            return WIRE_RECOVER_FAILED;
        }
        return WIRE_RECOVER_PENDING;
    }

    if (!finalize_at_end && count == group->last_recovery_count) {
        return WIRE_RECOVER_PENDING;
    }
    group->last_recovery_count = count;

    status = Codec_recover(dec->codec, group->data, group->received_bits,
                           group->shard_count);
    if (status == CODEC_RECOVER_OK) {
        mark_group_recovered(group, &dec->stats, true);
        return WIRE_RECOVER_OK;
    }
    if (status == CODEC_RECOVER_UNAVAILABLE) {
        if (finalize_at_end) {
            group->state = WIRE_GROUP_FAILED;
            dec->stats.groups_failed++;
            return WIRE_RECOVER_FAILED;
        }
        return WIRE_RECOVER_PENDING;
    }

    /* CODEC_RECOVER_ERR: correlated shards — wait for more during ingest. */
    if (finalize_at_end) {
        group->state = WIRE_GROUP_FAILED;
        dec->stats.groups_failed++;
        return WIRE_RECOVER_FAILED;
    }
    return WIRE_RECOVER_PENDING;
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

static uint64_t count_pending_recovered(const WireFlowDecoder *dec)
{
    size_t index;
    uint64_t pending = 0;

    if (dec == NULL) {
        return 0;
    }
    for (index = 0; index < WIRE_FLOW_GROUP_WINDOW; index++) {
        const WireGroup *group = &dec->groups[index];

        if (group->state == WIRE_GROUP_RECOVERED &&
            group->block_id >= dec->next_emit_block) {
            pending++;
        }
    }
    return pending;
}

static void refresh_pending_snapshot(WireFlowDecoder *dec)
{
    if (dec != NULL) {
        dec->stats.pending_recovered_groups = count_pending_recovered(dec);
    }
}

static bool block_in_reorder_window(uint64_t block_id, uint64_t next_emit)
{
    return block_id >= next_emit &&
           (block_id - next_emit) < (uint64_t)WIRE_FLOW_GROUP_WINDOW;
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

    if (group->state != WIRE_GROUP_RECOVERED) {
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
    group->state = WIRE_GROUP_EMITTED;
    dec->stats.decoded_blocks++;
    dec->stats.groups_emitted++;
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

/*
 * Advance past a missing, FAILED, or abandoned ACTIVE head in best-effort.
 * Does not allocate a slot for a missing block_id.
 */
static int skip_unrecoverable_head(WireFlowDecoder *dec, WireGroup *group)
{
    if (dec == NULL) {
        return -1;
    }
    if (group != NULL) {
        if (Codec_is_systematic(dec->codec) && group->valid_len > 0 &&
            group->data != NULL && group->received_bits != NULL) {
            if (write_best_effort_group(dec, group) != 0) {
                return -1;
            }
        } else {
            release_group(group);
        }
    }
    dec->stats.skipped_groups++;
    dec->next_emit_block++;
    return 0;
}

static bool block_needs_room(WireFlowDecoder *dec, uint64_t block_id)
{
    if (!block_in_reorder_window(block_id, dec->next_emit_block)) {
        return true;
    }
    if (find_group(dec->groups, block_id) != NULL) {
        return false;
    }
    return !window_has_empty_slot(dec->groups);
}

/*
 * Best-effort only: slide next_emit until block_id fits in the reorder window
 * (or a slot exists). Never skip a RECOVERED head; last-chance recover ACTIVE
 * heads first. At most WIRE_FLOW_GROUP_WINDOW skips per packet; never advance
 * past block_id.
 */
static int make_room_for_block(WireFlowDecoder *dec, uint64_t block_id)
{
    size_t guard = 0;

    if (dec == NULL || !dec->best_effort) {
        return 0;
    }

    while (block_needs_room(dec, block_id) &&
           guard < (size_t)WIRE_FLOW_GROUP_WINDOW) {
        WireGroup *head;

        if (dec->next_emit_block >= block_id) {
            break;
        }

        head = find_group(dec->groups, dec->next_emit_block);
        if (head != NULL && head->state == WIRE_GROUP_RECOVERED) {
            if (write_decoded_group(dec, head) != 0) {
                return -1;
            }
            dec->next_emit_block++;
            continue;
        }
        if (head != NULL && head->state == WIRE_GROUP_ACTIVE) {
            if (try_recover_group(dec, head, true) == WIRE_RECOVER_OK) {
                if (write_decoded_group(dec, head) != 0) {
                    return -1;
                }
                dec->next_emit_block++;
                continue;
            }
        }
        if (skip_unrecoverable_head(dec, head) != 0) {
            return -1;
        }
        guard++;
    }
    return 0;
}

static bool later_recovered_waiting(const WireFlowDecoder *dec)
{
    size_t index;

    if (dec == NULL) {
        return false;
    }
    for (index = 0; index < WIRE_FLOW_GROUP_WINDOW; index++) {
        const WireGroup *group = &dec->groups[index];

        if (group->state == WIRE_GROUP_RECOVERED &&
            group->block_id > dec->next_emit_block) {
            return true;
        }
    }
    return false;
}

/*
 * Best-effort: a missing head, a FAILED head, or an ACTIVE head with fewer
 * than k shards cannot recover yet. Skip it only when a later group is
 * already RECOVERED (HOL stall), not while still waiting on this group.
 */
static bool head_below_recover_threshold(const WireFlowDecoder *dec,
                                         const WireGroup *head)
{
    size_t k;

    if (head == NULL || head->state == WIRE_GROUP_FAILED) {
        return true;
    }
    if (head->state != WIRE_GROUP_ACTIVE || dec == NULL || dec->codec == NULL) {
        return false;
    }
    k = Codec_data_shards(dec->codec);
    return k == 0 || (size_t)group_received_count(head) < k;
}

static int try_emit_from_head(WireFlowDecoder *dec)
{
    for (;;) {
        WireGroup *group;

        if (dec->end_seen && dec->next_emit_block >= dec->end_block_count) {
            return 0;
        }

        group = find_group(dec->groups, dec->next_emit_block);
        if (group != NULL && group->state == WIRE_GROUP_RECOVERED) {
            if (write_decoded_group(dec, group) != 0) {
                return -1;
            }
            dec->next_emit_block++;
            continue;
        }
        if (dec->best_effort && dec->end_seen &&
            (group == NULL || group->state == WIRE_GROUP_FAILED)) {
            if (skip_unrecoverable_head(dec, group) != 0) {
                return -1;
            }
            continue;
        }
        if (dec->best_effort &&
            head_below_recover_threshold(dec, group) &&
            later_recovered_waiting(dec)) {
            if (group != NULL && group->state == WIRE_GROUP_ACTIVE &&
                try_recover_group(dec, group, true) == WIRE_RECOVER_OK) {
                if (write_decoded_group(dec, group) != 0) {
                    return -1;
                }
                dec->next_emit_block++;
                continue;
            }
            if (skip_unrecoverable_head(dec, group) != 0) {
                return -1;
            }
            continue;
        }
        /* Strict, or ACTIVE head still waiting for shards. */
        return 0;
    }
}

static void finalize_active_groups(WireFlowDecoder *dec)
{
    size_t index;

    for (index = 0; index < WIRE_FLOW_GROUP_WINDOW; index++) {
        WireGroup *group = &dec->groups[index];

        if (group->state == WIRE_GROUP_ACTIVE) {
            try_recover_group(dec, group, true);
        }
    }
}

int wire_flow_decoder_flush_best_effort(WireFlowDecoder *dec)
{
    if (dec == NULL || !dec->inited || !dec->best_effort) {
        return -1;
    }
    if (!dec->end_seen) {
        return 0;
    }

    finalize_active_groups(dec);
    if (try_emit_from_head(dec) != 0) {
        return -1;
    }
    refresh_pending_snapshot(dec);
    if (dec->next_emit_block >= dec->end_block_count) {
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
        finalize_active_groups(dec);
        if (try_emit_from_head(dec) != 0) {
            return -1;
        }
        refresh_pending_snapshot(dec);
        if (dec->next_emit_block == dec->end_block_count) {
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

    /* Truly late: block already emitted (next_emit_block advanced past it). */
    if (header->block_id < dec->next_emit_block) {
        dec->stats.late_datagrams++;
        return 0;
    }
    if (dec->best_effort) {
        if (make_room_for_block(dec, header->block_id) != 0) {
            return -1;
        }
        if (header->block_id < dec->next_emit_block) {
            dec->stats.late_datagrams++;
            return 0;
        }
    }
    if (!block_in_reorder_window(header->block_id, dec->next_emit_block)) {
        dec->stats.window_overflow++;
        return 0;
    }

    group = find_group(dec->groups, header->block_id);
    if (group == NULL) {
        group = allocate_group(dec->groups, header->block_id, header->shard_count,
                               header->valid_len, header->encode_begin_ns,
                               header->encode_end_ns);
        if (group == NULL) {
            dec->stats.window_overflow++;
            return 0;
        }
        dec->stats.groups_received++;
    }

    if (group->state == WIRE_GROUP_RECOVERED ||
        group->state == WIRE_GROUP_FAILED ||
        group->state == WIRE_GROUP_EMITTED) {
        dec->stats.duplicate_datagrams++;
        return 0;
    }
    assert(group->state == WIRE_GROUP_ACTIVE);

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

    try_recover_group(dec, group, false);

    if (try_emit_from_head(dec) != 0) {
        return -1;
    }
    refresh_pending_snapshot(dec);
    if (dec->end_seen && dec->next_emit_block == dec->end_block_count) {
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
    return dec != NULL ? dec->next_emit_block : 0;
}

uint64_t wire_flow_decoder_end_block_count(const WireFlowDecoder *dec)
{
    return dec != NULL ? dec->end_block_count : 0;
}

uint64_t wire_flow_decoder_pending_recovered_groups(const WireFlowDecoder *dec)
{
    return count_pending_recovered(dec);
}

const WireFlowDecoderStats *wire_flow_decoder_stats(const WireFlowDecoder *dec)
{
    return dec != NULL ? &dec->stats : NULL;
}
