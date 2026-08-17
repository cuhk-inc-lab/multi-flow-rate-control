#include "fec_transport.h"

#include "codec.h"
#include "rs_codec.h"
#include "stream_config.h"
#include "wire_header.h"

#include <arpa/inet.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#define FEC_DEFAULT_FLUSH_NS     (20ull * 1000ull * 1000ull)
#define FEC_DEFAULT_WINDOW       64u
#define FEC_DEFAULT_QUEUE_PKTS   256u
#define FEC_KIND_DATA            0u
#define FEC_KIND_METADATA        1u
#define FEC_KIND_PARITY          2u
#define FEC_GROUP_FREE           0
#define FEC_GROUP_COLLECTING     1
#define FEC_GROUP_COMPLETED      2
typedef struct {
    uint16_t len;
    uint8_t kind;
    uint8_t datagram[1];
} FecQueueItem;

typedef struct FecDecodeGroup {
    int active;
    int released_unrecoverable;
    int data_count_hint_valid;
    int metadata_valid;
    int emit_blocked;
    uint8_t data_count;
    uint64_t group_id;
    uint64_t last_seen_ns;
    uint8_t present[32];
    uint8_t delivered[32];
    uint16_t *lengths;
    unsigned char *block;
} FecDecodeGroup;

struct FecEncoder {
    FecTransportConfig config;
    FecCallbacks cb;
    const Codec *codec;
    uint16_t k;
    uint16_t r;
    uint16_t n;
    uint16_t shard_size;
    uint16_t message_shards;
    uint32_t epoch;
    uint32_t next_group;
    int group_exhausted;
    uint8_t data_count;
    int pending_since_valid;
    uint64_t pending_since_ns;
    uint64_t tokens;
    uint64_t last_refill_ns;
    int last_refill_valid;
    uint16_t *lengths;
    unsigned char *block;
    size_t q_cap;
    size_t q_count;
    size_t q_head;
    size_t q_bytes;
    size_t q_byte_cap;
    size_t q_stride;
    size_t max_datagram;
    uint8_t *q_storage;
    FecStats stats;
};

struct FecDecoder {
    FecTransportConfig config;
    FecCallbacks cb;
    const Codec *codec;
    uint16_t k;
    uint16_t r;
    uint16_t n;
    uint16_t shard_size;
    uint16_t message_shards;
    uint32_t epoch;
    uint64_t highest_group_id;
    int highest_group_valid;
    size_t group_window;
    FecDecodeGroup *groups;
    FecStats stats;
};

void fec_transport_config_init(FecTransportConfig *config)
{
    if (config == NULL) {
        return;
    }
    memset(config, 0, sizeof(*config));
    config->codec = FEC_CODEC_RS;
    config->data_shards = 4u;
    config->parity_shards = 2u;
    config->flush_timeout_ns = FEC_DEFAULT_FLUSH_NS;
    config->group_window = FEC_DEFAULT_WINDOW;
    config->output_queue_packets = FEC_DEFAULT_QUEUE_PKTS;
}

static size_t align8(size_t value)
{
    return (value + 7u) & ~(size_t)7u;
}

static int config_complete(const FecTransportConfig *in,
                           FecTransportConfig *out)
{
    uint32_t valid_span;

    if (in == NULL || out == NULL) {
        return 0;
    }
    *out = *in;
    if (out->codec != FEC_CODEC_RS) {
        return 0;
    }
    if (out->data_shards < 2u || out->parity_shards < 1u) {
        return 0;
    }
    if ((uint32_t)out->data_shards + (uint32_t)out->parity_shards >
        FEC_TRANSPORT_MAX_SHARDS) {
        return 0;
    }
    if (out->shard_size == 0u) {
        out->shard_size = (uint16_t)PKG_SIZE;
    }
    if (out->shard_size != (uint16_t)PKG_SIZE) {
        return 0;
    }
    valid_span = (uint32_t)out->data_shards * (uint32_t)out->shard_size;
    if (valid_span == 0u || valid_span > UINT16_MAX) {
        return 0;
    }
    if (out->group_window == 0u) {
        out->group_window = FEC_DEFAULT_WINDOW;
    }
    if (out->output_queue_packets == 0u) {
        out->output_queue_packets = FEC_DEFAULT_QUEUE_PKTS;
    }
    if (out->output_queue_packets <
        (size_t)out->parity_shards + 1u) {
        return 0;
    }
    if (out->output_queue_bytes == 0u) {
        out->output_queue_bytes =
            out->output_queue_packets *
            (WIRE_HEADER_SIZE + (size_t)out->shard_size);
    }
    return 1;
}

static int bind_rs(const FecTransportConfig *config, const Codec **codec)
{
    size_t data_shards;
    size_t parity_shards;

    if (RsCodec_set_params(config->data_shards, config->parity_shards) != 0) {
        return 0;
    }
    *codec = RsCodec_get();
    if (*codec == NULL) {
        return 0;
    }
    RsCodec_get_params(&data_shards, &parity_shards);
    return data_shards == config->data_shards &&
           parity_shards == config->parity_shards &&
           Codec_output_block_size(*codec) ==
               (size_t)(config->data_shards + config->parity_shards) *
               (size_t)config->shard_size;
}

static FecQueueItem *queue_slot(FecEncoder *encoder, size_t index)
{
    return (FecQueueItem *)(encoder->q_storage + index * encoder->q_stride);
}

static int queue_can_add(const FecEncoder *encoder, size_t packets, size_t bytes)
{
    if (encoder->q_count + packets > encoder->q_cap) {
        return 0;
    }
    if (encoder->q_bytes + bytes > encoder->q_byte_cap) {
        return 0;
    }
    return 1;
}

static void queue_pop(FecEncoder *encoder)
{
    FecQueueItem *item = queue_slot(encoder, encoder->q_head);

    encoder->q_bytes -= item->len;
    encoder->q_head = (encoder->q_head + 1u) % encoder->q_cap;
    encoder->q_count--;
    encoder->stats.output_queue_packets = encoder->q_count;
    encoder->stats.output_queue_bytes = encoder->q_bytes;
}

static void refill_tokens(FecEncoder *encoder, uint64_t now_ns)
{
    uint64_t burst;
    uint64_t add;
    uint64_t dt;
    uint64_t rate;

    if (encoder->config.wire_rate_bps == 0u) {
        return;
    }
    burst = encoder->config.wire_burst_bytes;
    if (burst == 0u) {
        burst = encoder->max_datagram;
    }
    if (!encoder->last_refill_valid) {
        encoder->tokens = burst;
        encoder->last_refill_ns = now_ns;
        encoder->last_refill_valid = 1;
        return;
    }
    if (now_ns <= encoder->last_refill_ns) {
        return;
    }
    dt = now_ns - encoder->last_refill_ns;
    rate = encoder->config.wire_rate_bps;
    add = (rate / 8ull) * (dt / 1000000000ull);
    add += (rate / 8ull) * (dt % 1000000000ull) / 1000000000ull;
    encoder->last_refill_ns = now_ns;
    if (add > UINT64_MAX - encoder->tokens) {
        encoder->tokens = burst;
        return;
    }
    encoder->tokens += add;
    if (encoder->tokens > burst) {
        encoder->tokens = burst;
    }
}

static uint64_t group_id_of(uint32_t epoch, uint32_t group)
{
    return ((uint64_t)epoch << 32) | (uint64_t)group;
}

static void pack_shard(FecEncoder *encoder,
                       size_t shard,
                       uint16_t payload_len,
                       uint8_t *packet)
{
    WireHeader header;

    memset(&header, 0, sizeof(header));
    header.type = WIRE_TYPE_DATA;
    header.block_id = group_id_of(encoder->epoch, encoder->next_group);
    header.shard_index = (uint16_t)shard;
    header.shard_count = encoder->n;
    header.valid_len = shard < encoder->message_shards ?
                       (uint16_t)(encoder->k * encoder->shard_size) :
                       (uint16_t)((encoder->data_count + 1u) *
                                  encoder->shard_size);
    header.payload_len = payload_len;
    wire_header_encode(packet, &header);
    memcpy(packet + WIRE_HEADER_SIZE,
           encoder->block + shard * encoder->shard_size,
           encoder->shard_size);
}

static FecStatus enqueue_shard(FecEncoder *encoder,
                               size_t shard,
                               uint16_t payload_len)
{
    uint8_t kind;
    size_t len = WIRE_HEADER_SIZE + (size_t)encoder->shard_size;
    FecQueueItem *item;
    size_t tail;

    if (payload_len == 0u || payload_len > encoder->shard_size) {
        return FEC_ERR_INVAL;
    }
    if (!queue_can_add(encoder, 1u, len)) {
        encoder->stats.queue_overflow_count++;
        encoder->stats.output_queue_packets = encoder->q_count;
        encoder->stats.output_queue_bytes = encoder->q_bytes;
        return FEC_ERR_QUEUE_FULL;
    }
    if (shard < encoder->message_shards) {
        kind = FEC_KIND_DATA;
    } else if (shard == encoder->message_shards) {
        kind = FEC_KIND_METADATA;
    } else {
        kind = FEC_KIND_PARITY;
    }
    tail = (encoder->q_head + encoder->q_count) % encoder->q_cap;
    item = queue_slot(encoder, tail);
    pack_shard(encoder, shard, payload_len, item->datagram);
    item->len = (uint16_t)len;
    item->kind = kind;
    encoder->q_count++;
    encoder->q_bytes += len;
    encoder->stats.output_queue_packets = encoder->q_count;
    encoder->stats.output_queue_bytes = encoder->q_bytes;
    return FEC_OK;
}

static FecStatus encode_and_enqueue_tails(FecEncoder *encoder)
{
    unsigned char *length_table;
    size_t shard;
    size_t tails;
    size_t tail_bytes;
    FecStatus status;

    if (encoder->data_count == 0u || encoder->group_exhausted) {
        return encoder->group_exhausted ? FEC_ERR_EXHAUSTED : FEC_OK;
    }
    tails = (size_t)encoder->n - (size_t)encoder->message_shards;
    tail_bytes = tails * (WIRE_HEADER_SIZE + (size_t)encoder->shard_size);
    if (!queue_can_add(encoder, tails, tail_bytes)) {
        encoder->stats.queue_overflow_count++;
        encoder->stats.output_queue_packets = encoder->q_count;
        encoder->stats.output_queue_bytes = encoder->q_bytes;
        return FEC_ERR_QUEUE_FULL;
    }

    length_table = encoder->block +
                   (size_t)encoder->message_shards * encoder->shard_size;
    memset(length_table, 0, encoder->shard_size);
    length_table[0] = encoder->data_count;
    for (shard = 0; shard < encoder->data_count; shard++) {
        uint16_t network_length = htons(encoder->lengths[shard]);

        memcpy(length_table + 1u + shard * sizeof(network_length),
               &network_length, sizeof(network_length));
    }
    Codec_encode(encoder->codec, encoder->block,
                 (size_t)encoder->n * encoder->shard_size);
    for (shard = encoder->message_shards; shard < encoder->n; shard++) {
        status = enqueue_shard(encoder, shard, encoder->shard_size);
        if (status != FEC_OK) {
            return status;
        }
    }
    if (encoder->next_group == UINT32_MAX) {
        encoder->group_exhausted = 1;
    } else {
        encoder->next_group++;
    }
    encoder->data_count = 0;
    encoder->pending_since_ns = 0;
    encoder->pending_since_valid = 0;
    memset(encoder->lengths, 0,
           (size_t)encoder->message_shards * sizeof(*encoder->lengths));
    memset(encoder->block, 0, (size_t)encoder->n * encoder->shard_size);
    return FEC_OK;
}

FecEncoder *fec_encoder_create(const FecTransportConfig *config,
                               const FecCallbacks *callbacks)
{
    FecTransportConfig filled;
    FecTransportConfig resolved;
    FecEncoder *encoder;
    const Codec *codec = NULL;
    size_t max_datagram;
    size_t stride;

    if (config == NULL) {
        fec_transport_config_init(&filled);
        config = &filled;
    }
    if (callbacks == NULL || callbacks->output == NULL ||
        !config_complete(config, &resolved) || !bind_rs(&resolved, &codec)) {
        return NULL;
    }
    max_datagram = WIRE_HEADER_SIZE + (size_t)resolved.shard_size;
    stride = align8(offsetof(FecQueueItem, datagram) + max_datagram);
    encoder = calloc(1, sizeof(*encoder));
    if (encoder == NULL) {
        return NULL;
    }
    encoder->config = resolved;
    encoder->cb = *callbacks;
    encoder->codec = codec;
    encoder->k = resolved.data_shards;
    encoder->r = resolved.parity_shards;
    encoder->n = (uint16_t)(encoder->k + encoder->r);
    encoder->shard_size = resolved.shard_size;
    encoder->message_shards = (uint16_t)(encoder->k - 1u);
    encoder->max_datagram = max_datagram;
    encoder->q_cap = resolved.output_queue_packets;
    encoder->q_byte_cap = resolved.output_queue_bytes;
    encoder->q_stride = stride;
    encoder->lengths = calloc(encoder->message_shards, sizeof(*encoder->lengths));
    encoder->block = calloc((size_t)encoder->n, encoder->shard_size);
    encoder->q_storage = calloc(encoder->q_cap, stride);
    if (encoder->lengths == NULL || encoder->block == NULL ||
        encoder->q_storage == NULL) {
        fec_encoder_destroy(encoder);
        return NULL;
    }
    return encoder;
}

void fec_encoder_destroy(FecEncoder *encoder)
{
    if (encoder == NULL) {
        return;
    }
    free(encoder->lengths);
    free(encoder->block);
    free(encoder->q_storage);
    free(encoder);
}

FecStatus fec_encoder_reset(FecEncoder *encoder, uint32_t epoch)
{
    if (encoder == NULL) {
        return FEC_ERR_INVAL;
    }
    encoder->epoch = epoch;
    encoder->next_group = 0;
    encoder->group_exhausted = 0;
    encoder->data_count = 0;
    encoder->pending_since_ns = 0;
    encoder->pending_since_valid = 0;
    encoder->q_count = 0;
    encoder->q_head = 0;
    encoder->q_bytes = 0;
    encoder->tokens = 0;
    encoder->last_refill_valid = 0;
    memset(encoder->lengths, 0,
           (size_t)encoder->message_shards * sizeof(*encoder->lengths));
    memset(encoder->block, 0, (size_t)encoder->n * encoder->shard_size);
    memset(&encoder->stats, 0, sizeof(encoder->stats));
    return FEC_OK;
}

int fec_encoder_has_pending(const FecEncoder *encoder)
{
    return encoder != NULL && encoder->data_count != 0;
}

uint64_t fec_encoder_next_update_ns(const FecEncoder *encoder)
{
    if (encoder == NULL || !encoder->pending_since_valid ||
        encoder->data_count == 0 ||
        encoder->config.flush_timeout_ns == 0u) {
        return 0;
    }
    return encoder->pending_since_ns + encoder->config.flush_timeout_ns;
}

FecStatus fec_encoder_flush(FecEncoder *encoder)
{
    if (encoder == NULL) {
        return FEC_ERR_INVAL;
    }
    return encode_and_enqueue_tails(encoder);
}

FecStatus fec_encoder_push(FecEncoder *encoder,
                           const void *data,
                           size_t length,
                           uint64_t now_ns)
{
    size_t need_packets;
    size_t need_bytes;
    size_t dg;
    FecStatus status;

    if (encoder == NULL || data == NULL || length == 0u ||
        length > encoder->shard_size) {
        return FEC_ERR_INVAL;
    }
    if (encoder->group_exhausted) {
        return FEC_ERR_EXHAUSTED;
    }
    if (encoder->data_count >= encoder->message_shards) {
        status = encode_and_enqueue_tails(encoder);
        if (status != FEC_OK) {
            return status;
        }
    }
    dg = WIRE_HEADER_SIZE + (size_t)encoder->shard_size;
    need_packets = 1u;
    need_bytes = dg;
    if ((uint16_t)(encoder->data_count + 1u) == encoder->message_shards) {
        size_t tails = (size_t)encoder->r + 1u;

        need_packets += tails;
        need_bytes += tails * dg;
    }
    if (!queue_can_add(encoder, need_packets, need_bytes)) {
        encoder->stats.queue_overflow_count++;
        encoder->stats.output_queue_packets = encoder->q_count;
        encoder->stats.output_queue_bytes = encoder->q_bytes;
        return FEC_ERR_QUEUE_FULL;
    }
    if (encoder->data_count == 0u) {
        memset(encoder->block, 0, (size_t)encoder->n * encoder->shard_size);
        memset(encoder->lengths, 0,
               (size_t)encoder->message_shards * sizeof(*encoder->lengths));
        encoder->pending_since_ns = now_ns;
        encoder->pending_since_valid = 1;
    }
    memcpy(encoder->block + (size_t)encoder->data_count * encoder->shard_size,
           data, length);
    encoder->lengths[encoder->data_count] = (uint16_t)length;
    status = enqueue_shard(encoder, encoder->data_count, (uint16_t)length);
    if (status != FEC_OK) {
        return status;
    }
    encoder->data_count++;
    if (encoder->data_count == encoder->message_shards) {
        return encode_and_enqueue_tails(encoder);
    }
    return FEC_OK;
}

FecStatus fec_encoder_drain(FecEncoder *encoder, size_t budget)
{
    if (encoder == NULL) {
        return FEC_ERR_INVAL;
    }
    while (budget > 0u && encoder->q_count > 0u) {
        FecQueueItem *item = queue_slot(encoder, encoder->q_head);
        FecOutputStatus out;

        if (encoder->config.wire_rate_bps != 0u &&
            encoder->tokens < item->len) {
            encoder->stats.wire_pacing_deferred++;
            break;
        }
        out = encoder->cb.output(encoder->cb.ctx, item->datagram, item->len);
        if (out == FEC_OUTPUT_BLOCKED) {
            encoder->stats.blocked_count++;
            break;
        }
        if (out != FEC_OUTPUT_OK) {
            encoder->stats.downstream_callback_errors++;
            return FEC_ERR_DOWNSTREAM;
        }
        if (encoder->config.wire_rate_bps != 0u) {
            encoder->tokens -= item->len;
        }
        if (item->kind == FEC_KIND_DATA) {
            encoder->stats.data_datagrams_tx++;
        } else if (item->kind == FEC_KIND_METADATA) {
            encoder->stats.metadata_datagrams_tx++;
        } else {
            encoder->stats.parity_datagrams_tx++;
        }
        queue_pop(encoder);
        budget--;
    }
    return FEC_OK;
}

FecStatus fec_encoder_update(FecEncoder *encoder, uint64_t now_ns)
{
    FecStatus status;

    if (encoder == NULL) {
        return FEC_ERR_INVAL;
    }
    refill_tokens(encoder, now_ns);
    if (encoder->pending_since_valid && encoder->data_count != 0u &&
        encoder->config.flush_timeout_ns != 0u &&
        now_ns >= encoder->pending_since_ns &&
        now_ns - encoder->pending_since_ns >=
            encoder->config.flush_timeout_ns) {
        status = encode_and_enqueue_tails(encoder);
        if (status != FEC_OK && status != FEC_ERR_QUEUE_FULL) {
            return status;
        }
    }
    return fec_encoder_drain(encoder, SIZE_MAX);
}

void fec_encoder_get_stats(const FecEncoder *encoder, FecStats *stats)
{
    if (stats == NULL) {
        return;
    }
    memset(stats, 0, sizeof(*stats));
    if (encoder != NULL) {
        *stats = encoder->stats;
    }
}

static void group_clear(FecDecodeGroup *group, uint16_t n, uint16_t shard_size,
                        uint16_t message_shards)
{
    int keep_block = group->block != NULL;
    unsigned char *block = group->block;
    uint16_t *lengths = group->lengths;

    memset(group, 0, sizeof(*group));
    group->block = block;
    group->lengths = lengths;
    if (keep_block) {
        memset(group->block, 0, (size_t)n * shard_size);
        memset(group->lengths, 0,
               (size_t)message_shards * sizeof(*group->lengths));
    }
}

static int expected_shard(const FecDecodeGroup *group,
                          size_t shard,
                          uint16_t message_shards,
                          uint16_t n)
{
    uint8_t data_count;

    if (shard >= n) {
        return 0;
    }
    if (shard >= message_shards) {
        return 1;
    }
    if (!(group->metadata_valid || group->data_count_hint_valid)) {
        return 1;
    }
    data_count = group->data_count;
    return shard < data_count;
}

static int all_messages_delivered(const FecDecodeGroup *group, uint8_t data_count)
{
    size_t index;

    for (index = 0; index < data_count; index++) {
        if (!codec_present_get(group->delivered, index)) {
            return 0;
        }
    }
    return 1;
}

static int messages_complete(const FecDecodeGroup *group)
{
    return group->metadata_valid &&
           all_messages_delivered(group, group->data_count);
}

static void record_group_loss(FecDecoder *decoder, const FecDecodeGroup *group,
                              int mark_unrecoverable)
{
    uint64_t missing_count = 0;
    uint64_t run = 0;
    uint64_t max_run = 0;
    size_t shard;

    if (group == NULL || group->active == FEC_GROUP_FREE) {
        return;
    }
    for (shard = 0; shard < decoder->n; shard++) {
        int missing = expected_shard(group, shard, decoder->message_shards,
                                     decoder->n) &&
                      !codec_present_get(group->present, shard);

        if (missing) {
            missing_count++;
            run++;
            if (run > max_run) {
                max_run = run;
            }
        } else {
            run = 0;
        }
    }
    if (missing_count == 0u) {
        return;
    }
    decoder->stats.missing_shards += missing_count;
    if (missing_count > decoder->r) {
        decoder->stats.over_r_groups++;
    }
    if (max_run > decoder->stats.max_missing_run) {
        decoder->stats.max_missing_run = max_run;
    }
    if (mark_unrecoverable && !messages_complete(group)) {
        decoder->stats.unrecoverable_groups++;
    }
}

static void complete_group(FecDecoder *decoder, FecDecodeGroup *group)
{
    if (group->active == FEC_GROUP_COMPLETED) {
        return;
    }
    record_group_loss(decoder, group, 0);
    group->active = FEC_GROUP_COMPLETED;
    group->emit_blocked = 0;
    decoder->stats.completed_groups++;
}

static FecStatus emit_record(FecDecoder *decoder,
                             FecDecodeGroup *group,
                             size_t shard,
                             const uint8_t *data,
                             size_t length)
{
    FecOutputStatus out;

    if (length == 0u) {
        return FEC_ERR_METADATA;
    }
    if (codec_present_get(group->delivered, shard)) {
        return FEC_OK;
    }
    out = decoder->cb.output(decoder->cb.ctx, data, length);
    if (out == FEC_OUTPUT_BLOCKED) {
        decoder->stats.blocked_count++;
        group->emit_blocked = 1;
        return FEC_OK;
    }
    if (out != FEC_OUTPUT_OK) {
        decoder->stats.downstream_callback_errors++;
        return FEC_ERR_DOWNSTREAM;
    }
    codec_present_set(group->delivered, shard);
    group->emit_blocked = 0;
    return FEC_OK;
}

static FecStatus emit_contiguous(FecDecoder *decoder, FecDecodeGroup *group)
{
    size_t index;
    size_t limit = decoder->message_shards;

    if (group->metadata_valid || group->data_count_hint_valid) {
        limit = group->data_count;
    }
    for (index = 0; index < limit; index++) {
        size_t length;

        if (codec_present_get(group->delivered, index)) {
            continue;
        }
        if (!codec_present_get(group->present, index)) {
            break;
        }
        length = group->lengths[index];
        if (length == 0u || length > decoder->shard_size) {
            return FEC_ERR_METADATA;
        }
        {
            FecStatus status = emit_record(
                decoder, group, index,
                group->block + index * decoder->shard_size, length);

            if (status != FEC_OK) {
                return status;
            }
            if (!codec_present_get(group->delivered, index)) {
                return FEC_OK;
            }
        }
    }
    return FEC_OK;
}

static FecStatus emit_available(FecDecoder *decoder, FecDecodeGroup *group)
{
    size_t index;
    size_t limit = (group->metadata_valid || group->data_count_hint_valid) ?
                   group->data_count : decoder->message_shards;

    for (index = 0; index < limit; index++) {
        size_t length;
        FecStatus status;

        if (!codec_present_get(group->present, index) ||
            codec_present_get(group->delivered, index)) {
            continue;
        }
        length = group->lengths[index];
        if (length == 0u || length > decoder->shard_size) {
            continue;
        }
        status = emit_record(decoder, group, index,
                             group->block + index * decoder->shard_size,
                             length);
        if (status != FEC_OK) {
            return status;
        }
        if (!codec_present_get(group->delivered, index)) {
            return FEC_OK;
        }
    }
    return FEC_OK;
}

static FecStatus parse_metadata(FecDecoder *decoder,
                                FecDecodeGroup *group,
                                uint8_t *data_count)
{
    const unsigned char *length_table;
    size_t index;

    if (!codec_present_get(group->present, decoder->message_shards)) {
        return FEC_ERR_METADATA;
    }
    length_table = group->block +
                   (size_t)decoder->message_shards * decoder->shard_size;
    *data_count = length_table[0];
    if (*data_count == 0u || *data_count > decoder->message_shards) {
        return FEC_ERR_METADATA;
    }
    if (group->data_count_hint_valid && group->data_count != *data_count) {
        return FEC_ERR_METADATA;
    }
    for (index = 0; index < *data_count; index++) {
        uint16_t network_length;
        size_t length;

        memcpy(&network_length,
               length_table + 1u + index * sizeof(network_length),
               sizeof(network_length));
        length = ntohs(network_length);
        if (length == 0u || length > decoder->shard_size) {
            return FEC_ERR_METADATA;
        }
        if (group->lengths[index] != 0u && group->lengths[index] != length) {
            return FEC_ERR_METADATA;
        }
        if (group->lengths[index] == 0u) {
            group->lengths[index] = (uint16_t)length;
        }
    }
    return FEC_OK;
}

static void mark_unused_zeros(FecDecoder *decoder, FecDecodeGroup *group,
                              uint8_t data_count)
{
    size_t index;

    for (index = data_count; index < decoder->message_shards; index++) {
        codec_present_set(group->present, index);
    }
}

static FecStatus emit_from_metadata(FecDecoder *decoder, FecDecodeGroup *group)
{
    uint8_t data_count = 0;
    size_t index;
    FecStatus status;

    status = parse_metadata(decoder, group, &data_count);
    if (status != FEC_OK) {
        return status;
    }
    group->data_count = data_count;
    group->metadata_valid = 1;
    mark_unused_zeros(decoder, group, data_count);
    for (index = 0; index < data_count; index++) {
        if (codec_present_get(group->delivered, index)) {
            continue;
        }
        status = emit_record(decoder, group, index,
                             group->block + index * decoder->shard_size,
                             group->lengths[index]);
        if (status != FEC_OK) {
            return status;
        }
        if (!codec_present_get(group->delivered, index)) {
            return FEC_OK;
        }
    }
    return FEC_OK;
}

static size_t present_count(const uint8_t *bits, size_t n)
{
    size_t shard;
    size_t count = 0;

    for (shard = 0; shard < n; shard++) {
        count += (size_t)codec_present_get(bits, shard);
    }
    return count;
}

static FecStatus recover_group(FecDecoder *decoder, FecDecodeGroup *group)
{
    uint8_t present_bits[32];
    size_t missing_data = 0;
    size_t shard;
    CodecRecoverStatus recovered;
    FecStatus status;
    int had_all_data = 1;

    if (present_count(group->present, decoder->n) < decoder->k) {
        return FEC_OK;
    }
    memcpy(present_bits, group->present, sizeof(present_bits));
    for (shard = 0; shard < decoder->k; shard++) {
        if (!codec_present_get(group->present, shard)) {
            had_all_data = 0;
            missing_data++;
        }
    }
    if (had_all_data) {
        recovered = CODEC_RECOVER_OK;
    } else {
        recovered = Codec_recover(decoder->codec, group->block, present_bits,
                                  decoder->n);
    }
    if (recovered == CODEC_RECOVER_UNAVAILABLE) {
        return FEC_OK;
    }
    if (recovered != CODEC_RECOVER_OK) {
        return FEC_ERR_CODEC;
    }
    if (!had_all_data) {
        decoder->stats.recovered_groups++;
        decoder->stats.recovered_shards += missing_data;
        for (shard = 0; shard < decoder->k; shard++) {
            codec_present_set(group->present, shard);
        }
    }
    status = emit_from_metadata(decoder, group);
    if (status != FEC_OK) {
        return status;
    }
    if (group->metadata_valid &&
        all_messages_delivered(group, group->data_count)) {
        complete_group(decoder, group);
    }
    return FEC_OK;
}

static FecDecodeGroup *find_group(FecDecoder *decoder, uint64_t group_id,
                                  int *stale)
{
    size_t index;
    FecDecodeGroup *oldest = NULL;
    FecDecodeGroup *free_group = NULL;
    FecDecodeGroup *completed = NULL;

    *stale = 0;
    for (index = 0; index < decoder->group_window; index++) {
        FecDecodeGroup *group = &decoder->groups[index];

        if (group->active == FEC_GROUP_COMPLETED &&
            group->group_id == group_id) {
            return group;
        }
        if (group->active == FEC_GROUP_FREE) {
            if (free_group == NULL) {
                free_group = group;
            }
            continue;
        }
        if (group->active == FEC_GROUP_COMPLETED) {
            if (completed == NULL || group->group_id < completed->group_id) {
                completed = group;
            }
            continue;
        }
        if (group->group_id == group_id) {
            return group;
        }
        if (oldest == NULL || group->group_id < oldest->group_id) {
            oldest = group;
        }
    }
    if (free_group != NULL) {
        group_clear(free_group, decoder->n, decoder->shard_size,
                    decoder->message_shards);
        free_group->active = FEC_GROUP_COLLECTING;
        free_group->group_id = group_id;
        return free_group;
    }
    if (completed != NULL) {
        group_clear(completed, decoder->n, decoder->shard_size,
                    decoder->message_shards);
        completed->active = FEC_GROUP_COLLECTING;
        completed->group_id = group_id;
        return completed;
    }
    if (oldest != NULL && group_id < oldest->group_id) {
        *stale = 1;
        return NULL;
    }
    if (oldest != NULL) {
        decoder->stats.evicted_groups++;
        emit_available(decoder, oldest);
        record_group_loss(decoder, oldest, 1);
        group_clear(oldest, decoder->n, decoder->shard_size,
                    decoder->message_shards);
        oldest->active = FEC_GROUP_COLLECTING;
        oldest->group_id = group_id;
        return oldest;
    }
    return NULL;
}

static FecStatus release_older_groups(FecDecoder *decoder, uint64_t new_group_id)
{
    for (;;) {
        FecDecodeGroup *oldest = NULL;
        size_t index;
        FecStatus status;

        for (index = 0; index < decoder->group_window; index++) {
            FecDecodeGroup *candidate = &decoder->groups[index];

            if (candidate->active != FEC_GROUP_COLLECTING ||
                candidate->released_unrecoverable ||
                candidate->group_id >= new_group_id) {
                continue;
            }
            if (oldest == NULL || candidate->group_id < oldest->group_id) {
                oldest = candidate;
            }
        }
        if (oldest == NULL) {
            return FEC_OK;
        }
        status = emit_available(decoder, oldest);
        if (status != FEC_OK) {
            return status;
        }
        oldest->released_unrecoverable = 1;
    }
}

static FecStatus apply_tail_hint(FecDecoder *decoder,
                                 FecDecodeGroup *group,
                                 const WireHeader *header)
{
    uint8_t hinted;

    if (header->valid_len < 2u * decoder->shard_size ||
        header->valid_len > decoder->k * decoder->shard_size ||
        header->valid_len % decoder->shard_size != 0u) {
        decoder->stats.invalid_datagrams++;
        return FEC_ERR_WIRE_HEADER;
    }
    hinted = (uint8_t)(header->valid_len / decoder->shard_size - 1u);
    if ((group->data_count_hint_valid && group->data_count != hinted) ||
        hinted == 0u || hinted > decoder->message_shards) {
        decoder->stats.invalid_datagrams++;
        return FEC_ERR_WIRE_HEADER;
    }
    group->data_count = hinted;
    group->data_count_hint_valid = 1;
    mark_unused_zeros(decoder, group, hinted);
    return FEC_OK;
}

FecDecoder *fec_decoder_create(const FecTransportConfig *config,
                               const FecCallbacks *callbacks)
{
    FecTransportConfig filled;
    FecTransportConfig resolved;
    FecDecoder *decoder;
    const Codec *codec = NULL;
    size_t index;

    if (config == NULL) {
        fec_transport_config_init(&filled);
        config = &filled;
    }
    if (callbacks == NULL || callbacks->output == NULL ||
        !config_complete(config, &resolved) || !bind_rs(&resolved, &codec)) {
        return NULL;
    }
    decoder = calloc(1, sizeof(*decoder));
    if (decoder == NULL) {
        return NULL;
    }
    decoder->config = resolved;
    decoder->cb = *callbacks;
    decoder->codec = codec;
    decoder->k = resolved.data_shards;
    decoder->r = resolved.parity_shards;
    decoder->n = (uint16_t)(decoder->k + decoder->r);
    decoder->shard_size = resolved.shard_size;
    decoder->message_shards = (uint16_t)(decoder->k - 1u);
    decoder->group_window = resolved.group_window;
    decoder->groups = calloc(decoder->group_window, sizeof(*decoder->groups));
    if (decoder->groups == NULL) {
        fec_decoder_destroy(decoder);
        return NULL;
    }
    for (index = 0; index < decoder->group_window; index++) {
        decoder->groups[index].block =
            calloc((size_t)decoder->n, decoder->shard_size);
        decoder->groups[index].lengths =
            calloc(decoder->message_shards, sizeof(uint16_t));
        if (decoder->groups[index].block == NULL ||
            decoder->groups[index].lengths == NULL) {
            fec_decoder_destroy(decoder);
            return NULL;
        }
    }
    return decoder;
}

void fec_decoder_destroy(FecDecoder *decoder)
{
    size_t index;

    if (decoder == NULL) {
        return;
    }
    if (decoder->groups != NULL) {
        for (index = 0; index < decoder->group_window; index++) {
            free(decoder->groups[index].block);
            free(decoder->groups[index].lengths);
        }
    }
    free(decoder->groups);
    free(decoder);
}

FecStatus fec_decoder_reset(FecDecoder *decoder, uint32_t epoch)
{
    size_t index;

    if (decoder == NULL) {
        return FEC_ERR_INVAL;
    }
    decoder->epoch = epoch;
    decoder->highest_group_id = 0;
    decoder->highest_group_valid = 0;
    memset(&decoder->stats, 0, sizeof(decoder->stats));
    for (index = 0; index < decoder->group_window; index++) {
        group_clear(&decoder->groups[index], decoder->n, decoder->shard_size,
                    decoder->message_shards);
    }
    return FEC_OK;
}

static int wire_shape_ok(const FecDecoder *decoder, const WireHeader *header,
                         size_t length)
{
    size_t expected = WIRE_HEADER_SIZE + (size_t)decoder->shard_size;

    if (header->type != WIRE_TYPE_DATA ||
        header->shard_count != decoder->n ||
        header->shard_index >= decoder->n ||
        header->payload_len == 0u ||
        header->payload_len > decoder->shard_size ||
        length != expected) {
        return 0;
    }
    if (header->shard_index < decoder->message_shards) {
        return header->valid_len ==
               (uint16_t)(decoder->k * decoder->shard_size);
    }
    return header->payload_len == decoder->shard_size;
}

FecStatus fec_decoder_input(FecDecoder *decoder,
                            const void *datagram,
                            size_t length,
                            uint64_t now_ns)
{
    const unsigned char *bytes = datagram;
    WireHeader header;
    FecDecodeGroup *group;
    uint32_t bit_index;
    int stale = 0;
    FecStatus status;

    if (decoder == NULL || datagram == NULL || length == 0u) {
        return FEC_ERR_INVAL;
    }
    if (length < WIRE_HEADER_SIZE) {
        return FEC_ERR_NOT_FEC;
    }
    if (wire_header_decode(&header, bytes, length) != 0) {
        return FEC_ERR_NOT_FEC;
    }
    if (header.type != WIRE_TYPE_DATA) {
        return FEC_ERR_NOT_FEC;
    }
    if (!wire_shape_ok(decoder, &header, length)) {
        decoder->stats.invalid_datagrams++;
        return FEC_ERR_WIRE_HEADER;
    }
    if ((uint32_t)(header.block_id >> 32) != decoder->epoch) {
        decoder->stats.stale_datagrams++;
        return FEC_ERR_STALE;
    }
    if (decoder->highest_group_valid &&
        header.block_id < decoder->highest_group_id &&
        decoder->highest_group_id - header.block_id >= decoder->group_window) {
        decoder->stats.stale_datagrams++;
        return FEC_ERR_STALE;
    }
    if (!decoder->highest_group_valid ||
        header.block_id > decoder->highest_group_id) {
        status = release_older_groups(decoder, header.block_id);
        if (status != FEC_OK) {
            return status;
        }
        decoder->highest_group_id = header.block_id;
        decoder->highest_group_valid = 1;
    }
    group = find_group(decoder, header.block_id, &stale);
    if (group == NULL) {
        if (stale) {
            decoder->stats.stale_datagrams++;
            return FEC_ERR_STALE;
        }
        return FEC_ERR_CODEC;
    }
    if (header.shard_index >= decoder->message_shards) {
        status = apply_tail_hint(decoder, group, &header);
        if (status != FEC_OK) {
            return status;
        }
    } else if (group->data_count_hint_valid &&
               header.shard_index >= group->data_count) {
        decoder->stats.invalid_datagrams++;
        return FEC_ERR_WIRE_HEADER;
    }
    bit_index = header.shard_index;
    if (group->active == FEC_GROUP_COMPLETED ||
        codec_present_get(group->present, bit_index)) {
        decoder->stats.duplicate_datagrams++;
        return FEC_OK;
    }
    decoder->stats.received_datagrams++;
    group->last_seen_ns = now_ns;
    memcpy(group->block + (size_t)header.shard_index * decoder->shard_size,
           bytes + WIRE_HEADER_SIZE, decoder->shard_size);
    codec_present_set(group->present, bit_index);
    if (header.shard_index < decoder->message_shards) {
        group->lengths[header.shard_index] = header.payload_len;
        status = emit_contiguous(decoder, group);
        if (status != FEC_OK) {
            return status;
        }
    }
    if (codec_present_get(group->present, decoder->message_shards)) {
        uint8_t data_count = 0;

        status = parse_metadata(decoder, group, &data_count);
        if (status != FEC_OK) {
            return status;
        }
        group->data_count = data_count;
        group->metadata_valid = 1;
        mark_unused_zeros(decoder, group, data_count);
        if (all_messages_delivered(group, data_count)) {
            complete_group(decoder, group);
            return FEC_OK;
        }
    }
    return recover_group(decoder, group);
}

static FecStatus retry_blocked(FecDecoder *decoder)
{
    size_t index;

    for (index = 0; index < decoder->group_window; index++) {
        FecDecodeGroup *group = &decoder->groups[index];
        FecStatus status;

        if (group->active != FEC_GROUP_COLLECTING || !group->emit_blocked) {
            continue;
        }
        status = emit_contiguous(decoder, group);
        if (status != FEC_OK) {
            return status;
        }
        if (group->emit_blocked) {
            continue;
        }
        if (group->metadata_valid) {
            status = emit_from_metadata(decoder, group);
            if (status != FEC_OK) {
                return status;
            }
            if (!group->emit_blocked &&
                all_messages_delivered(group, group->data_count)) {
                complete_group(decoder, group);
            }
        } else {
            status = recover_group(decoder, group);
            if (status != FEC_OK) {
                return status;
            }
        }
    }
    return FEC_OK;
}

FecStatus fec_decoder_update(FecDecoder *decoder, uint64_t now_ns)
{
    size_t index;
    FecStatus status;

    if (decoder == NULL) {
        return FEC_ERR_INVAL;
    }
    status = retry_blocked(decoder);
    if (status != FEC_OK) {
        return status;
    }
    if (decoder->config.flush_timeout_ns == 0u) {
        return FEC_OK;
    }
    for (index = 0; index < decoder->group_window; index++) {
        FecDecodeGroup *group = &decoder->groups[index];

        if (group->active != FEC_GROUP_COLLECTING ||
            group->last_seen_ns == 0u || now_ns < group->last_seen_ns ||
            now_ns - group->last_seen_ns < decoder->config.flush_timeout_ns) {
            continue;
        }
        status = emit_available(decoder, group);
        if (status != FEC_OK) {
            return status;
        }
        record_group_loss(decoder, group, 1);
        group_clear(group, decoder->n, decoder->shard_size,
                    decoder->message_shards);
        decoder->stats.expired_groups++;
    }
    return FEC_OK;
}

void fec_decoder_get_stats(const FecDecoder *decoder, FecStats *stats)
{
    if (stats == NULL) {
        return;
    }
    memset(stats, 0, sizeof(*stats));
    if (decoder != NULL) {
        *stats = decoder->stats;
    }
}
