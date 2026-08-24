#include "local_source.h"

#include "stream_config.h"
#include "wire_header.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifndef LOCAL_SOURCE_MAX_DATAGRAM
#define LOCAL_SOURCE_MAX_DATAGRAM (WIRE_MAX_HEADER_SIZE + PKG_SIZE)
#endif

static double mono_seconds(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0.0;
    }
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void pace_block(double started, uint64_t source_bytes, double rate_mbps)
{
    double target;
    double now;

    if (rate_mbps <= 0.0) {
        return;
    }
    target = started + ((double)source_bytes * 8.0) / (rate_mbps * 1e6);
    for (;;) {
        now = mono_seconds();
        if (now >= target) {
            return;
        }
        {
            double remain = target - now;
            struct timespec req;

            if (remain >= 0.001) {
                req.tv_sec = (time_t)remain;
                req.tv_nsec = (long)((remain - (double)req.tv_sec) * 1e9);
                (void)nanosleep(&req, NULL);
            }
        }
    }
}

static uint64_t mono_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static int emit_one(RelayWireEmitFn emit_fn, void *emit_ctx,
                    const WireHeader *hdr, const uint8_t *payload,
                    LocalSourceStats *stats)
{
    uint8_t datagram[LOCAL_SOURCE_MAX_DATAGRAM];
    size_t header_size;
    size_t len;

    if (hdr == NULL || emit_fn == NULL) {
        return -1;
    }
    if (hdr->payload_len > PKG_SIZE) {
        return -1;
    }
    header_size = wire_header_size(hdr);
    len = header_size;
    if (hdr->version == WIRE_VERSION_V4) {
        wire_header_encode_v4(datagram, hdr);
    } else {
        wire_header_encode(datagram, hdr);
    }
    if (hdr->payload_len > 0) {
        if (payload == NULL) {
            return -1;
        }
        memcpy(datagram + header_size, payload, hdr->payload_len);
        len += hdr->payload_len;
    }
    if (emit_fn(datagram, len, emit_ctx) != 0) {
        if (stats != NULL) {
            stats->emit_errors++;
        }
        return -1;
    }
    if (stats != NULL) {
        stats->wire_datagrams++;
    }
    return 0;
}

typedef struct LocalWirehairEmitCtx {
    RelayWireEmitFn emit_fn;
    void *emit_ctx;
    LocalSourceStats *stats;
} LocalWirehairEmitCtx;

static int local_wirehair_emit(const WireHeader *header,
                               const uint8_t *payload, size_t payload_len,
                               void *opaque)
{
    LocalWirehairEmitCtx *ctx = opaque;

    if (header == NULL || payload_len != header->payload_len) {
        return -1;
    }
    return emit_one(ctx->emit_fn, ctx->emit_ctx, header, payload, ctx->stats);
}

static int local_wirehair_source_run(const LocalSourceConfig *config,
                                     RelayWireEmitFn emit_fn, void *emit_ctx,
                                     LocalSourceStats *stats)
{
    FILE *input = NULL;
    uint8_t *segment = NULL;
    uint64_t segment_id = 0;
    double started;
    int result = -1;
    LocalWirehairEmitCtx wh_emit = {
        .emit_fn = emit_fn,
        .emit_ctx = emit_ctx,
        .stats = stats,
    };

    if (!wirehair_segment_config_valid(&config->wirehair)) {
        return -1;
    }
    input = fopen(config->input_path, "rb");
    segment = malloc(config->wirehair.segment_bytes);
    if (input == NULL || segment == NULL) {
        goto out;
    }
    started = mono_seconds();
    for (;;) {
        size_t got = fread(segment, 1, config->wirehair.segment_bytes, input);
        WirehairSegmentSendStats send_stats;

        if (got == 0) {
            if (ferror(input)) {
                goto out;
            }
            break;
        }
        if (wirehair_segment_send(
                &config->wirehair, config->flow_id, segment_id,
                config->final_dst, config->ttl, segment, got,
                local_wirehair_emit, &wh_emit, config->ack_poll,
                config->ack_ctx,
                &send_stats) != 0) {
            goto out;
        }
        stats->blocks++;
        stats->source_bytes += got;
        segment_id++;
        pace_block(started, stats->source_bytes,
                   config->source_rate_mbps);
    }
    {
        WireHeader end = {
            .version = WIRE_VERSION_V4,
            .type = WIRE_TYPE_END,
            .final_dst = config->final_dst,
            .ttl = config->ttl,
            .flow_id = config->flow_id,
            .block_id = segment_id,
            .origin_node = config->wirehair.origin_node,
            .flags = config->wirehair.ack_enabled
                         ? WIRE_FLAG_ACK_REQUEST
                         : 0u,
        };
        if (emit_one(emit_fn, emit_ctx, &end, NULL, stats) != 0) {
            goto out;
        }
    }
    result = 0;
out:
    if (input != NULL) {
        fclose(input);
    }
    free(segment);
    return result;
}

int local_source_run(const LocalSourceConfig *config,
                     RelayWireEmitFn emit_fn, void *emit_ctx,
                     LocalSourceStats *stats_out)
{
    const Codec *codec;
    FILE *input = NULL;
    unsigned char *block = NULL;
    size_t input_size;
    size_t output_size;
    uint16_t shard_count;
    uint64_t block_id = 0;
    uint64_t source_bytes = 0;
    double started;
    LocalSourceStats stats;
    int result = -1;

    memset(&stats, 0, sizeof(stats));
    if (config == NULL || config->input_path == NULL || emit_fn == NULL ||
        config->final_dst == 0 || config->ttl == 0 ||
        config->source_rate_mbps < 0.0) {
        return -1;
    }
    if (config->codec_kind == CODEC_KIND_WIREHAIR) {
        result = local_wirehair_source_run(config, emit_fn, emit_ctx, &stats);
        if (stats_out != NULL) {
            *stats_out = stats;
        }
        return result;
    }

    codec = Codec_get(config->codec_kind);
    if (codec == NULL) {
        return -1;
    }
    input_size = Codec_input_block_size(codec);
    output_size = Codec_output_block_size(codec);
    if (input_size == 0 || output_size == 0 ||
        output_size > CODEC_MAX_ENCODE_BLOCK ||
        output_size % PKG_SIZE != 0) {
        return -1;
    }
    shard_count = (uint16_t)(output_size / PKG_SIZE);

    block = malloc(output_size);
    if (block == NULL) {
        return -1;
    }
    input = fopen(config->input_path, "rb");
    if (input == NULL) {
        free(block);
        return -1;
    }

    started = mono_seconds();
    for (;;) {
        size_t got;
        uint64_t encode_begin_ns;
        uint64_t encode_end_ns;
        uint16_t shard;
        WireHeader hdr;

        memset(block, 0, output_size);
        got = fread(block, 1, input_size, input);
        if (got == 0) {
            if (ferror(input)) {
                goto out;
            }
            break;
        }

        pace_block(started, source_bytes, config->source_rate_mbps);
        encode_begin_ns = mono_ns();
        Codec_encode(codec, block, output_size);
        encode_end_ns = mono_ns();

        for (shard = 0; shard < shard_count; shard++) {
            hdr = (WireHeader){
                .type = WIRE_TYPE_DATA,
                .final_dst = config->final_dst,
                .ttl = config->ttl,
                .flow_id = config->flow_id,
                .block_id = block_id,
                .shard_index = shard,
                .shard_count = shard_count,
                .valid_len = (uint16_t)got,
                .payload_len = PKG_SIZE,
                .encode_begin_ns = encode_begin_ns,
                .encode_end_ns = encode_end_ns,
            };
            if (emit_one(emit_fn, emit_ctx, &hdr,
                         block + (size_t)shard * PKG_SIZE, &stats) != 0) {
                goto out;
            }
        }

        source_bytes += got;
        stats.blocks++;
        stats.source_bytes = source_bytes;
        block_id++;
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
        if (emit_one(emit_fn, emit_ctx, &end, NULL, &stats) != 0) {
            goto out;
        }
    }

    result = 0;

out:
    if (input != NULL) {
        fclose(input);
    }
    free(block);
    if (stats_out != NULL) {
        *stats_out = stats;
    }
    return result;
}
