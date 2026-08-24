#include "fec_transport.h"

#include "codec.h"
#include "rs_codec.h"
#include "stream_config.h"
#include "wire_header.h"
#include "wirehair_segment.h"

#include <wirehair/wirehair.h>

#include <arpa/inet.h>
#include <pthread.h>
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
    /* Wirehair session (unused when codec is RS). */
    uint8_t *wh_buf;
    uint8_t *wh_padded;
    WirehairCodec wh_codec;
    size_t wh_len;
    uint32_t wh_codec_bytes;
    uint32_t wh_source_packets;
    uint32_t wh_repair_budget;
    uint32_t wh_next_id;
    uint32_t wh_segment_bytes;
    uint64_t wh_segment_id;
    int wh_acked;
    int wh_send_end;
    int wh_end_sent;
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
    WirehairSegmentReceiver *wh_receiver;
};

static pthread_mutex_t fec_backend_mu = PTHREAD_MUTEX_INITIALIZER;
static int fec_backend_users;
static uint16_t fec_backend_k;
static uint16_t fec_backend_r;
static uint16_t fec_backend_shard;

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
    if (out->codec == FEC_CODEC_WIREHAIR) {
        WirehairSegmentConfig wh;

        if (out->segment_bytes == 0u) {
            out->segment_bytes = WH_SEGMENT_DEFAULT_BYTES;
        }
        if (out->repair_percent == 0u) {
            out->repair_percent = WH_SEGMENT_DEFAULT_REPAIR_PCT;
        }
        if (out->origin_node == 0u) {
            out->origin_node = 1u;
        }
        if (out->final_dst == 0u) {
            out->final_dst = WIRE_DEFAULT_FINAL_DST;
        }
        if (out->ttl == 0u) {
            out->ttl = WIRE_DEFAULT_TTL;
        }
        if (out->ack_ttl == 0u) {
            out->ack_ttl = out->ttl;
        }
        if (out->output_queue_packets == 0u) {
            out->output_queue_packets = FEC_DEFAULT_QUEUE_PKTS;
        }
        if (out->output_queue_bytes == 0u) {
            out->output_queue_bytes =
                out->output_queue_packets *
                (WIRE_V4_HEADER_SIZE + (size_t)PKG_SIZE);
        }
        wirehair_segment_config_defaults(&wh);
        wh.segment_bytes = out->segment_bytes;
        wh.repair_percent = out->repair_percent;
        wh.ack_enabled = out->ack_enabled != 0u;
        wh.origin_node = out->origin_node;
        wh.ack_ttl = out->ack_ttl;
        return wirehair_segment_config_valid(&wh);
    }
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
    if (out->shard_size < 32u) {
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

static void unbind_rs(void)
{
    pthread_mutex_lock(&fec_backend_mu);
    if (fec_backend_users > 0) {
        fec_backend_users--;
    }
    pthread_mutex_unlock(&fec_backend_mu);
}

static int bind_rs(const FecTransportConfig *config, const Codec **codec)
{
    size_t data_shards;
    size_t parity_shards;
    int ok = 0;

    pthread_mutex_lock(&fec_backend_mu);
    if (fec_backend_users > 0 &&
        (fec_backend_k != config->data_shards ||
         fec_backend_r != config->parity_shards ||
         fec_backend_shard != config->shard_size)) {
        pthread_mutex_unlock(&fec_backend_mu);
        return 0;
    }
    if (RsCodec_set_params_ex(config->data_shards, config->parity_shards,
                              config->shard_size) != 0) {
        pthread_mutex_unlock(&fec_backend_mu);
        return 0;
    }
    *codec = RsCodec_get();
    if (*codec != NULL) {
        RsCodec_get_params(&data_shards, &parity_shards);
        ok = data_shards == config->data_shards &&
             parity_shards == config->parity_shards &&
             Codec_output_block_size(*codec) ==
                 (size_t)(config->data_shards + config->parity_shards) *
                 (size_t)config->shard_size;
    }
    if (ok) {
        fec_backend_k = config->data_shards;
        fec_backend_r = config->parity_shards;
        fec_backend_shard = config->shard_size;
        fec_backend_users++;
    }
    pthread_mutex_unlock(&fec_backend_mu);
    return ok;
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

static int encoder_is_wirehair(const FecEncoder *encoder)
{
    return encoder != NULL && encoder->config.codec == FEC_CODEC_WIREHAIR;
}

static int decoder_is_wirehair(const FecDecoder *decoder)
{
    return decoder != NULL && decoder->config.codec == FEC_CODEC_WIREHAIR;
}

static WirehairSegmentConfig encoder_wh_config(const FecEncoder *encoder)
{
    WirehairSegmentConfig config;

    wirehair_segment_config_defaults(&config);
    config.segment_bytes = encoder->config.segment_bytes;
    config.repair_percent = encoder->config.repair_percent;
    config.ack_enabled = encoder->config.ack_enabled != 0u;
    config.origin_node = encoder->config.origin_node;
    config.ack_ttl = encoder->config.ack_ttl;
    return config;
}

static void wirehair_close_session(FecEncoder *encoder)
{
    if (encoder->wh_codec != NULL) {
        wirehair_free(encoder->wh_codec);
        encoder->wh_codec = NULL;
    }
    free(encoder->wh_padded);
    encoder->wh_padded = NULL;
    encoder->wh_codec_bytes = 0;
    encoder->wh_source_packets = 0;
    encoder->wh_repair_budget = 0;
    encoder->wh_next_id = 0;
    encoder->wh_segment_bytes = 0;
    encoder->wh_acked = 0;
    encoder->wh_len = 0;
}

static FecStatus wirehair_enqueue_header(FecEncoder *encoder,
                                         const WireHeader *header,
                                         const uint8_t *payload,
                                         uint8_t kind)
{
    size_t header_size = wire_header_size(header);
    size_t len = header_size + (size_t)header->payload_len;
    FecQueueItem *item;
    size_t tail;

    if (header->payload_len > 0u && payload == NULL) {
        return FEC_ERR_INVAL;
    }
    if (!queue_can_add(encoder, 1u, len)) {
        encoder->stats.queue_overflow_count++;
        encoder->stats.output_queue_packets = encoder->q_count;
        encoder->stats.output_queue_bytes = encoder->q_bytes;
        return FEC_ERR_QUEUE_FULL;
    }
    tail = (encoder->q_head + encoder->q_count) % encoder->q_cap;
    item = queue_slot(encoder, tail);
    if (header->version == WIRE_VERSION_V4) {
        wire_header_encode_v4(item->datagram, header);
    } else {
        wire_header_encode(item->datagram, header);
    }
    if (header->payload_len > 0u) {
        memcpy(item->datagram + header_size, payload, header->payload_len);
    }
    item->len = (uint16_t)len;
    item->kind = kind;
    encoder->q_count++;
    encoder->q_bytes += len;
    encoder->stats.output_queue_packets = encoder->q_count;
    encoder->stats.output_queue_bytes = encoder->q_bytes;
    return FEC_OK;
}

static FecStatus wirehair_start_session(FecEncoder *encoder)
{
    WirehairSegmentConfig config = encoder_wh_config(encoder);
    const uint8_t *message;
    uint32_t codec_bytes;

    if (encoder->wh_len == 0u || encoder->wh_codec != NULL) {
        return FEC_OK;
    }
    if (wirehair_init() != Wirehair_Success) {
        return FEC_ERR_CODEC;
    }
    codec_bytes = (uint32_t)encoder->wh_len;
    message = encoder->wh_buf;
    if (codec_bytes < 2u * PKG_SIZE) {
        codec_bytes = 2u * PKG_SIZE;
        encoder->wh_padded = calloc(1, codec_bytes);
        if (encoder->wh_padded == NULL) {
            return FEC_ERR_NOMEM;
        }
        memcpy(encoder->wh_padded, encoder->wh_buf, encoder->wh_len);
        message = encoder->wh_padded;
    }
    encoder->wh_codec = wirehair_encoder_create(NULL, message, codec_bytes,
                                                PKG_SIZE);
    if (encoder->wh_codec == NULL) {
        free(encoder->wh_padded);
        encoder->wh_padded = NULL;
        return FEC_ERR_CODEC;
    }
    encoder->wh_codec_bytes = codec_bytes;
    encoder->wh_segment_bytes = (uint32_t)encoder->wh_len;
    encoder->wh_source_packets =
        wirehair_segment_source_packets(encoder->wh_segment_bytes);
    encoder->wh_repair_budget = wirehair_segment_repair_packets(
        encoder->wh_source_packets, config.repair_percent);
    encoder->wh_next_id = 0;
    encoder->wh_acked = 0;
    encoder->wh_segment_id = ((uint64_t)encoder->epoch << 32) |
                             (uint64_t)encoder->next_group;
    if (encoder->next_group == UINT32_MAX) {
        encoder->group_exhausted = 1;
    } else {
        encoder->next_group++;
    }
    return FEC_OK;
}

static FecStatus wirehair_enqueue_next_packet(FecEncoder *encoder)
{
    uint8_t payload[PKG_SIZE];
    uint32_t written = 0;
    WirehairResult encode_result;
    WireHeader header;
    uint8_t kind;
    uint32_t packet_limit;
    FecStatus status;

    if (encoder->wh_codec == NULL) {
        return FEC_OK;
    }
    packet_limit = encoder->wh_source_packets + encoder->wh_repair_budget;
    if (encoder->wh_acked &&
        encoder->wh_next_id >= encoder->wh_source_packets) {
        wirehair_close_session(encoder);
        return FEC_OK;
    }
    if (encoder->wh_next_id >= packet_limit) {
        wirehair_close_session(encoder);
        return FEC_OK;
    }
    memset(payload, 0, sizeof(payload));
    encode_result = wirehair_encode(encoder->wh_codec, encoder->wh_next_id,
                                    payload, sizeof(payload), &written);
    if (encode_result != Wirehair_Success || written == 0 ||
        written > sizeof(payload)) {
        return FEC_ERR_CODEC;
    }
    header = (WireHeader){
        .version = WIRE_VERSION_V4,
        .type = WIRE_TYPE_DATA,
        .final_dst = encoder->config.final_dst,
        .ttl = encoder->config.ttl,
        .flow_id = encoder->config.flow_id,
        .block_id = encoder->wh_segment_id,
        .shard_index = (uint16_t)encoder->wh_next_id,
        .shard_count = (uint16_t)packet_limit,
        .payload_len = (uint16_t)written,
        .origin_node = encoder->config.origin_node,
        .flags = encoder->config.ack_enabled ? WIRE_FLAG_ACK_REQUEST : 0u,
        .segment_bytes = encoder->wh_segment_bytes,
    };
    kind = encoder->wh_next_id < encoder->wh_source_packets ?
               FEC_KIND_DATA :
               FEC_KIND_PARITY;
    status = wirehair_enqueue_header(encoder, &header, payload, kind);
    if (status != FEC_OK) {
        return status;
    }
    encoder->wh_next_id++;
    if (encoder->wh_next_id >= packet_limit ||
        (encoder->wh_acked &&
         encoder->wh_next_id >= encoder->wh_source_packets)) {
        wirehair_close_session(encoder);
    }
    return FEC_OK;
}

static FecStatus wirehair_enqueue_end(FecEncoder *encoder)
{
    WireHeader end;

    if (encoder->wh_end_sent) {
        return FEC_OK;
    }
    end = (WireHeader){
        .version = WIRE_VERSION_V4,
        .type = WIRE_TYPE_END,
        .final_dst = encoder->config.final_dst,
        .ttl = encoder->config.ttl,
        .flow_id = encoder->config.flow_id,
        .block_id = ((uint64_t)encoder->epoch << 32) |
                    (uint64_t)encoder->next_group,
        .origin_node = encoder->config.origin_node,
        .flags = encoder->config.ack_enabled ? WIRE_FLAG_ACK_REQUEST : 0u,
    };
    return wirehair_enqueue_header(encoder, &end, NULL, FEC_KIND_DATA);
}

static FecStatus wirehair_fill_queue(FecEncoder *encoder)
{
    FecStatus status;

    if (encoder->wh_codec == NULL && encoder->wh_len > 0u) {
        status = wirehair_start_session(encoder);
        if (status != FEC_OK) {
            return status;
        }
    }
    while (encoder->wh_codec != NULL) {
        status = wirehair_enqueue_next_packet(encoder);
        if (status == FEC_ERR_QUEUE_FULL) {
            return FEC_OK;
        }
        if (status != FEC_OK) {
            return status;
        }
    }
    if (encoder->wh_send_end && encoder->wh_len == 0u &&
        encoder->wh_codec == NULL && !encoder->wh_end_sent) {
        status = wirehair_enqueue_end(encoder);
        if (status == FEC_OK) {
            encoder->wh_end_sent = 1;
            encoder->wh_send_end = 0;
        }
        return status == FEC_ERR_QUEUE_FULL ? FEC_OK : status;
    }
    return FEC_OK;
}

static int wirehair_output(uint32_t flow_id, const uint8_t *data, size_t len,
                           void *opaque)
{
    FecDecoder *decoder = opaque;
    FecOutputStatus out;

    (void)flow_id;
    if (decoder == NULL || decoder->cb.output == NULL) {
        return -1;
    }
    out = decoder->cb.output(decoder->cb.ctx, data, len);
    if (out != FEC_OUTPUT_OK) {
        return -1;
    }
    decoder->stats.completed_groups++;
    return 0;
}

static int wirehair_ack_emit(const WireHeader *ack, void *opaque)
{
    FecDecoder *decoder = opaque;
    uint8_t datagram[WIRE_V4_HEADER_SIZE];
    FecOutputStatus out;

    if (decoder == NULL || ack == NULL || decoder->cb.ack_output == NULL) {
        return 0;
    }
    wire_header_encode_v4(datagram, ack);
    out = decoder->cb.ack_output(decoder->cb.ctx, datagram,
                                 wire_header_size(ack));
    return out == FEC_OUTPUT_OK ? 0 : -1;
}

static WirehairSegmentReceiver *wirehair_make_receiver(FecDecoder *decoder)
{
    WirehairSegmentConfig config;

    wirehair_segment_config_defaults(&config);
    config.segment_bytes = decoder->config.segment_bytes;
    config.repair_percent = decoder->config.repair_percent;
    config.ack_enabled = decoder->config.ack_enabled != 0u;
    config.origin_node = decoder->config.origin_node;
    config.ack_ttl = decoder->config.ack_ttl;
    return wirehair_segment_receiver_create(
        &config, decoder->config.flow_id, wirehair_output, decoder,
        decoder->config.ack_enabled ? wirehair_ack_emit : NULL, decoder);
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
        !config_complete(config, &resolved)) {
        return NULL;
    }
    if (resolved.codec == FEC_CODEC_RS && !bind_rs(&resolved, &codec)) {
        return NULL;
    }
    max_datagram = resolved.codec == FEC_CODEC_WIREHAIR ?
                       (WIRE_V4_HEADER_SIZE + (size_t)PKG_SIZE) :
                       (WIRE_HEADER_SIZE + (size_t)resolved.shard_size);
    stride = align8(offsetof(FecQueueItem, datagram) + max_datagram);
    encoder = calloc(1, sizeof(*encoder));
    if (encoder == NULL) {
        if (resolved.codec == FEC_CODEC_RS) {
            unbind_rs();
        }
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
    encoder->q_storage = calloc(encoder->q_cap, stride);
    if (resolved.codec == FEC_CODEC_WIREHAIR) {
        encoder->wh_buf = malloc(resolved.segment_bytes);
        if (encoder->wh_buf == NULL || encoder->q_storage == NULL) {
            fec_encoder_destroy(encoder);
            return NULL;
        }
        return encoder;
    }
    encoder->lengths = calloc(encoder->message_shards, sizeof(*encoder->lengths));
    encoder->block = calloc((size_t)encoder->n, encoder->shard_size);
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
    wirehair_close_session(encoder);
    free(encoder->wh_buf);
    free(encoder->lengths);
    free(encoder->block);
    free(encoder->q_storage);
    if (encoder->config.codec == FEC_CODEC_RS) {
        unbind_rs();
    }
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
    if (encoder_is_wirehair(encoder)) {
        wirehair_close_session(encoder);
        encoder->wh_send_end = 0;
        encoder->wh_end_sent = 0;
        memset(&encoder->stats, 0, sizeof(encoder->stats));
        return FEC_OK;
    }
    memset(encoder->lengths, 0,
           (size_t)encoder->message_shards * sizeof(*encoder->lengths));
    memset(encoder->block, 0, (size_t)encoder->n * encoder->shard_size);
    memset(&encoder->stats, 0, sizeof(encoder->stats));
    return FEC_OK;
}

int fec_encoder_has_pending(const FecEncoder *encoder)
{
    return encoder != NULL &&
           (encoder->data_count != 0 || encoder->q_count != 0 ||
            (encoder_is_wirehair(encoder) &&
             (encoder->wh_len != 0u || encoder->wh_codec != NULL ||
              encoder->wh_send_end)));
}

static uint64_t bytes_to_ns(uint64_t bytes, uint64_t rate_bps)
{
    if (rate_bps == 0u || bytes == 0u) {
        return 0;
    }
    return (bytes * 8ull * 1000000000ull + rate_bps - 1ull) / rate_bps;
}

uint64_t fec_encoder_next_update_ns(const FecEncoder *encoder)
{
    uint64_t flush_at = 0;
    uint64_t pace_at = 0;

    if (encoder == NULL) {
        return 0;
    }
    if (encoder_is_wirehair(encoder)) {
        if (encoder->pending_since_valid && encoder->wh_len != 0u &&
            encoder->config.flush_timeout_ns != 0u) {
            flush_at = encoder->pending_since_ns +
                       encoder->config.flush_timeout_ns;
        }
    } else if (encoder->pending_since_valid && encoder->data_count != 0 &&
        encoder->config.flush_timeout_ns != 0u) {
        flush_at = encoder->pending_since_ns + encoder->config.flush_timeout_ns;
    }
    if (encoder->q_count > 0u && encoder->config.wire_rate_bps != 0u &&
        encoder->last_refill_valid) {
        const FecQueueItem *item =
            (const FecQueueItem *)(encoder->q_storage +
                                   encoder->q_head * encoder->q_stride);

        if (encoder->tokens < item->len) {
            pace_at = encoder->last_refill_ns +
                      bytes_to_ns(item->len - encoder->tokens,
                                  encoder->config.wire_rate_bps);
        }
    }
    if (flush_at == 0u) {
        return pace_at;
    }
    if (pace_at == 0u) {
        return flush_at;
    }
    return flush_at < pace_at ? flush_at : pace_at;
}

FecStatus fec_encoder_flush(FecEncoder *encoder)
{
    if (encoder == NULL) {
        return FEC_ERR_INVAL;
    }
    if (encoder_is_wirehair(encoder)) {
        if (encoder->wh_len == 0u && encoder->wh_codec == NULL) {
            encoder->wh_send_end = 1;
        }
        return wirehair_fill_queue(encoder);
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

    if (encoder == NULL || data == NULL || length == 0u) {
        return FEC_ERR_INVAL;
    }
    if (encoder_is_wirehair(encoder)) {
        const uint8_t *bytes = data;

        if (encoder->group_exhausted) {
            return FEC_ERR_EXHAUSTED;
        }
        if (encoder->wh_codec != NULL) {
            status = wirehair_fill_queue(encoder);
            if (status != FEC_OK) {
                return status;
            }
            if (encoder->wh_codec != NULL) {
                return FEC_ERR_BUSY;
            }
        }
        while (length > 0u) {
            size_t room = encoder->config.segment_bytes - encoder->wh_len;
            size_t take;

            if (room == 0u) {
                status = wirehair_fill_queue(encoder);
                if (status != FEC_OK) {
                    return status;
                }
                if (encoder->wh_codec != NULL || encoder->wh_len != 0u) {
                    return FEC_ERR_BUSY;
                }
                room = encoder->config.segment_bytes;
            }
            if (encoder->wh_len == 0u) {
                encoder->pending_since_ns = now_ns;
                encoder->pending_since_valid = 1;
            }
            take = length < room ? length : room;
            memcpy(encoder->wh_buf + encoder->wh_len, bytes, take);
            encoder->wh_len += take;
            bytes += take;
            length -= take;
            if (encoder->wh_len == encoder->config.segment_bytes) {
                status = wirehair_fill_queue(encoder);
                if (status != FEC_OK) {
                    return status;
                }
            }
        }
        return FEC_OK;
    }
    if (length > encoder->shard_size) {
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
    if (encoder_is_wirehair(encoder)) {
        FecStatus filled = wirehair_fill_queue(encoder);

        if (filled != FEC_OK) {
            return filled;
        }
    }
    while (budget > 0u && encoder->q_count > 0u) {
        FecQueueItem *item = queue_slot(encoder, encoder->q_head);
        FecOutputStatus out;

        if (encoder_is_wirehair(encoder) && encoder->wh_acked &&
            item->kind == FEC_KIND_PARITY) {
            queue_pop(encoder);
            continue;
        }

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
    if (encoder_is_wirehair(encoder)) {
        FecStatus status;

        if (encoder->pending_since_valid && encoder->wh_len != 0u &&
            encoder->wh_codec == NULL &&
            encoder->config.flush_timeout_ns != 0u &&
            now_ns >= encoder->pending_since_ns &&
            now_ns - encoder->pending_since_ns >=
                encoder->config.flush_timeout_ns) {
            status = wirehair_fill_queue(encoder);
            if (status != FEC_OK && status != FEC_ERR_QUEUE_FULL) {
                return status;
            }
        } else {
            status = wirehair_fill_queue(encoder);
            if (status != FEC_OK && status != FEC_ERR_QUEUE_FULL) {
                return status;
            }
        }
        return fec_encoder_drain(encoder, SIZE_MAX);
    }
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

FecStatus fec_encoder_input_ack(FecEncoder *encoder,
                                const void *datagram,
                                size_t length)
{
    WireHeader header;

    if (encoder == NULL || datagram == NULL || length == 0u) {
        return FEC_ERR_INVAL;
    }
    if (!encoder_is_wirehair(encoder)) {
        return FEC_ERR_INVAL;
    }
    if (wire_header_decode(&header, datagram, length) != 0 ||
        header.version != WIRE_VERSION_V4 || header.type != WIRE_TYPE_ACK) {
        return FEC_ERR_NOT_FEC;
    }
    if (header.flow_id != encoder->config.flow_id) {
        return FEC_OK;
    }
    if (encoder->wh_codec != NULL && header.block_id == encoder->wh_segment_id) {
        encoder->wh_acked = 1;
        encoder->stats.recovered_groups++;
    }
    return FEC_OK;
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
                                  int *stale, FecStatus *err)
{
    size_t index;
    FecDecodeGroup *oldest = NULL;
    FecDecodeGroup *free_group = NULL;
    FecDecodeGroup *completed = NULL;

    *stale = 0;
    *err = FEC_OK;
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
        FecStatus emit_status = emit_available(decoder, oldest);

        if (emit_status != FEC_OK) {
            *err = emit_status;
            return NULL;
        }
        if (oldest->emit_blocked) {
            *err = FEC_ERR_QUEUE_FULL;
            return NULL;
        }
        decoder->stats.evicted_groups++;
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
        if (oldest->emit_blocked) {
            return FEC_OK;
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
        !config_complete(config, &resolved)) {
        return NULL;
    }
    if (resolved.codec == FEC_CODEC_RS && !bind_rs(&resolved, &codec)) {
        return NULL;
    }
    decoder = calloc(1, sizeof(*decoder));
    if (decoder == NULL) {
        if (resolved.codec == FEC_CODEC_RS) {
            unbind_rs();
        }
        return NULL;
    }
    decoder->config = resolved;
    decoder->cb = *callbacks;
    decoder->codec = codec;
    if (resolved.codec == FEC_CODEC_WIREHAIR) {
        decoder->wh_receiver = wirehair_make_receiver(decoder);
        if (decoder->wh_receiver == NULL) {
            fec_decoder_destroy(decoder);
            return NULL;
        }
        return decoder;
    }
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
    wirehair_segment_receiver_destroy(decoder->wh_receiver);
    if (decoder->config.codec == FEC_CODEC_RS) {
        unbind_rs();
    }
    free(decoder);
}

FecStatus fec_decoder_reset(FecDecoder *decoder, uint32_t epoch)
{
    size_t index;

    if (decoder == NULL) {
        return FEC_ERR_INVAL;
    }
    if (decoder_is_wirehair(decoder)) {
        wirehair_segment_receiver_destroy(decoder->wh_receiver);
        decoder->wh_receiver = wirehair_make_receiver(decoder);
        decoder->epoch = epoch;
        memset(&decoder->stats, 0, sizeof(decoder->stats));
        return decoder->wh_receiver != NULL ? FEC_OK : FEC_ERR_NOMEM;
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
    FecStatus find_err = FEC_OK;

    if (decoder == NULL || datagram == NULL || length == 0u) {
        return FEC_ERR_INVAL;
    }
    if (decoder_is_wirehair(decoder)) {
        WireHeader header;
        const unsigned char *bytes = datagram;
        size_t header_size;

        if (wire_header_decode(&header, bytes, length) != 0) {
            return FEC_ERR_NOT_FEC;
        }
        if (header.version != WIRE_VERSION_V4) {
            return FEC_ERR_NOT_FEC;
        }
        if (header.type == WIRE_TYPE_ACK) {
            return FEC_OK;
        }
        header_size = wire_header_size(&header);
        if (length < header_size ||
            length != header_size + (size_t)header.payload_len) {
            decoder->stats.invalid_datagrams++;
            return FEC_ERR_WIRE_HEADER;
        }
        decoder->stats.received_datagrams++;
        if (wirehair_segment_receiver_ingest(
                decoder->wh_receiver, &header,
                header.payload_len > 0u ? bytes + header_size : NULL,
                header.payload_len) != 0) {
            decoder->stats.invalid_datagrams++;
            return FEC_ERR_CODEC;
        }
        return FEC_OK;
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
    group = find_group(decoder, header.block_id, &stale, &find_err);
    if (group == NULL) {
        if (stale) {
            decoder->stats.stale_datagrams++;
            return FEC_ERR_STALE;
        }
        return find_err != FEC_OK ? find_err : FEC_ERR_CODEC;
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
    if (decoder_is_wirehair(decoder)) {
        return FEC_OK;
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
        if (group->emit_blocked) {
            continue;
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
