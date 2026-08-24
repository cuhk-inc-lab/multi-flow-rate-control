#include "fec_transport.h"
#include "wire_header.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define EXPECT(cond)                                                           \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);   \
            return -1;                                                         \
        }                                                                      \
    } while (0)

#define MAX_CAP 128
#define MAX_DG  8192

typedef struct {
    uint8_t buf[MAX_CAP][MAX_DG];
    size_t len[MAX_CAP];
    size_t count;
    size_t block_next;
    int fail;
} Capture;

static FecOutputStatus capture_output(void *ctx, const uint8_t *data, size_t len)
{
    Capture *cap = ctx;

    if (cap->fail) {
        return FEC_OUTPUT_ERROR;
    }
    if (cap->block_next > 0u) {
        cap->block_next--;
        return FEC_OUTPUT_BLOCKED;
    }
    if (cap->count >= MAX_CAP || len > MAX_DG) {
        return FEC_OUTPUT_ERROR;
    }
    memcpy(cap->buf[cap->count], data, len);
    cap->len[cap->count] = len;
    cap->count++;
    return FEC_OUTPUT_OK;
}

static void config_profile(FecTransportConfig *config, uint16_t k, uint16_t r)
{
    fec_transport_config_init(config);
    config->data_shards = k;
    config->parity_shards = r;
    config->flush_timeout_ns = 0;
}

static int push_records(FecEncoder *encoder, size_t count, unsigned seed)
{
    size_t i;

    for (i = 0; i < count; i++) {
        uint8_t rec[64];
        size_t n = 8u + (i % 17u);
        size_t b;

        for (b = 0; b < n; b++) {
            rec[b] = (uint8_t)(seed + i * 17u + b);
        }
        if (fec_encoder_push(encoder, rec, n, 1000u + i) != FEC_OK) {
            return -1;
        }
    }
    return 0;
}

static int records_match(const Capture *got, size_t count, unsigned seed)
{
    size_t i;

    if (got->count != count) {
        return 0;
    }
    for (i = 0; i < count; i++) {
        uint8_t rec[64];
        size_t n = 8u + (i % 17u);
        size_t b;

        for (b = 0; b < n; b++) {
            rec[b] = (uint8_t)(seed + i * 17u + b);
        }
        if (got->len[i] != n || memcmp(got->buf[i], rec, n) != 0) {
            return 0;
        }
    }
    return 1;
}

static int feed_decoder(FecDecoder *decoder, const Capture *wire, int drop_shard)
{
    size_t i;

    for (i = 0; i < wire->count; i++) {
        WireHeader header;
        FecStatus st;

        if (drop_shard >= 0 &&
            wire_header_decode(&header, wire->buf[i], wire->len[i]) == 0 &&
            header.shard_index == (uint16_t)drop_shard) {
            continue;
        }
        st = fec_decoder_input(decoder, wire->buf[i], wire->len[i], 2000u + i);
        if (st != FEC_OK && st != FEC_ERR_STALE) {
            return -1;
        }
    }
    return 0;
}

static int test_clean_roundtrip(uint16_t k, uint16_t r)
{
    FecTransportConfig config;
    Capture wire = {0};
    Capture app = {0};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecCallbacks dec_cb = {.output = capture_output, .ctx = &app};
    FecEncoder *encoder;
    FecDecoder *decoder;
    size_t messages = (size_t)k - 1u;
    FecStats stats;

    config_profile(&config, k, r);
    encoder = fec_encoder_create(&config, &enc_cb);
    decoder = fec_decoder_create(&config, &dec_cb);
    EXPECT(encoder != NULL && decoder != NULL);
    EXPECT(fec_encoder_reset(encoder, 7) == FEC_OK);
    EXPECT(fec_decoder_reset(decoder, 7) == FEC_OK);
    EXPECT(push_records(encoder, messages, 11) == 0);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    EXPECT(wire.count == (size_t)k + (size_t)r);
    EXPECT(feed_decoder(decoder, &wire, -1) == 0);
    EXPECT(records_match(&app, messages, 11));
    fec_decoder_get_stats(decoder, &stats);
    EXPECT(stats.completed_groups == 1);
    EXPECT(stats.unrecoverable_groups == 0);
    fec_encoder_destroy(encoder);
    fec_decoder_destroy(decoder);
    return 0;
}

static int test_drop_one_data_shard(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    Capture app = {0};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecCallbacks dec_cb = {.output = capture_output, .ctx = &app};
    FecEncoder *encoder;
    FecDecoder *decoder;
    FecStats stats;

    config_profile(&config, 4, 2);
    encoder = fec_encoder_create(&config, &enc_cb);
    decoder = fec_decoder_create(&config, &dec_cb);
    EXPECT(encoder && decoder);
    EXPECT(fec_encoder_reset(encoder, 1) == FEC_OK);
    EXPECT(fec_decoder_reset(decoder, 1) == FEC_OK);
    EXPECT(push_records(encoder, 3, 3) == 0);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    EXPECT(feed_decoder(decoder, &wire, 1) == 0);
    EXPECT(records_match(&app, 3, 3));
    fec_decoder_get_stats(decoder, &stats);
    EXPECT(stats.recovered_groups == 1);
    EXPECT(stats.recovered_shards >= 1);
    fec_encoder_destroy(encoder);
    fec_decoder_destroy(decoder);
    return 0;
}

static int test_drop_metadata_and_one_data(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    Capture app = {0};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecCallbacks dec_cb = {.output = capture_output, .ctx = &app};
    FecEncoder *encoder;
    FecDecoder *decoder;
    size_t i;

    config_profile(&config, 4, 2);
    encoder = fec_encoder_create(&config, &enc_cb);
    decoder = fec_decoder_create(&config, &dec_cb);
    EXPECT(encoder && decoder);
    EXPECT(fec_encoder_reset(encoder, 2) == FEC_OK);
    EXPECT(fec_decoder_reset(decoder, 2) == FEC_OK);
    EXPECT(push_records(encoder, 3, 9) == 0);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    for (i = 0; i < wire.count; i++) {
        WireHeader header;

        EXPECT(wire_header_decode(&header, wire.buf[i], wire.len[i]) == 0);
        if (header.shard_index == 0 || header.shard_index == 3) {
            continue;
        }
        EXPECT(fec_decoder_input(decoder, wire.buf[i], wire.len[i], 1) == FEC_OK);
    }
    EXPECT(records_match(&app, 3, 9));
    fec_encoder_destroy(encoder);
    fec_decoder_destroy(decoder);
    return 0;
}

static int test_over_r_does_not_forge(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    Capture app = {0};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecCallbacks dec_cb = {.output = capture_output, .ctx = &app};
    FecEncoder *encoder;
    FecDecoder *decoder;
    size_t i;
    FecStats stats;

    config_profile(&config, 4, 2);
    encoder = fec_encoder_create(&config, &enc_cb);
    decoder = fec_decoder_create(&config, &dec_cb);
    EXPECT(encoder && decoder);
    EXPECT(fec_encoder_reset(encoder, 3) == FEC_OK);
    EXPECT(fec_decoder_reset(decoder, 3) == FEC_OK);
    EXPECT(push_records(encoder, 3, 21) == 0);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    for (i = 0; i < wire.count; i++) {
        WireHeader header;

        EXPECT(wire_header_decode(&header, wire.buf[i], wire.len[i]) == 0);
        if (header.shard_index <= 2) {
            continue;
        }
        EXPECT(fec_decoder_input(decoder, wire.buf[i], wire.len[i], 1) == FEC_OK);
    }
    EXPECT(app.count == 0);
    fec_decoder_get_stats(decoder, &stats);
    EXPECT(stats.recovered_groups == 0);
    fec_encoder_destroy(encoder);
    fec_decoder_destroy(decoder);
    return 0;
}

static int test_duplicate_and_reorder(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    Capture app = {0};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecCallbacks dec_cb = {.output = capture_output, .ctx = &app};
    FecEncoder *encoder;
    FecDecoder *decoder;
    size_t i;
    FecStats stats;

    config_profile(&config, 4, 2);
    encoder = fec_encoder_create(&config, &enc_cb);
    decoder = fec_decoder_create(&config, &dec_cb);
    EXPECT(encoder && decoder);
    EXPECT(fec_encoder_reset(encoder, 4) == FEC_OK);
    EXPECT(fec_decoder_reset(decoder, 4) == FEC_OK);
    EXPECT(push_records(encoder, 3, 5) == 0);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    for (i = wire.count; i > 0u; i--) {
        EXPECT(fec_decoder_input(decoder, wire.buf[i - 1u], wire.len[i - 1u],
                                 1) == FEC_OK);
    }
    EXPECT(fec_decoder_input(decoder, wire.buf[0], wire.len[0], 2) == FEC_OK);
    EXPECT(records_match(&app, 3, 5));
    fec_decoder_get_stats(decoder, &stats);
    EXPECT(stats.duplicate_datagrams >= 1);
    fec_encoder_destroy(encoder);
    fec_decoder_destroy(decoder);
    return 0;
}

static int test_stale_epoch(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    Capture app = {0};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecCallbacks dec_cb = {.output = capture_output, .ctx = &app};
    FecEncoder *encoder;
    FecDecoder *decoder;

    config_profile(&config, 4, 2);
    encoder = fec_encoder_create(&config, &enc_cb);
    decoder = fec_decoder_create(&config, &dec_cb);
    EXPECT(encoder && decoder);
    EXPECT(fec_encoder_reset(encoder, 9) == FEC_OK);
    EXPECT(fec_decoder_reset(decoder, 8) == FEC_OK);
    EXPECT(push_records(encoder, 3, 1) == 0);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    EXPECT(fec_decoder_input(decoder, wire.buf[0], wire.len[0], 1) ==
           FEC_ERR_STALE);
    EXPECT(app.count == 0);
    fec_encoder_destroy(encoder);
    fec_decoder_destroy(decoder);
    return 0;
}

static int test_blocked_then_drain(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecEncoder *encoder;
    FecStats stats;

    config_profile(&config, 4, 2);
    encoder = fec_encoder_create(&config, &enc_cb);
    EXPECT(encoder);
    EXPECT(fec_encoder_reset(encoder, 1) == FEC_OK);
    EXPECT(push_records(encoder, 3, 8) == 0);
    wire.block_next = 1;
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    fec_encoder_get_stats(encoder, &stats);
    EXPECT(stats.blocked_count >= 1);
    EXPECT(wire.count == 0);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    EXPECT(wire.count == 6);
    fec_encoder_destroy(encoder);
    return 0;
}

static int test_queue_overflow_no_silent_loss(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecEncoder *encoder;
    uint8_t rec[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    FecStats stats;
    size_t i;
    int saw_full = 0;

    config_profile(&config, 4, 2);
    config.output_queue_packets = 4;
    encoder = fec_encoder_create(&config, &enc_cb);
    EXPECT(encoder);
    EXPECT(fec_encoder_reset(encoder, 1) == FEC_OK);
    for (i = 0; i < 8u; i++) {
        FecStatus st = fec_encoder_push(encoder, rec, sizeof(rec), i);

        if (st == FEC_ERR_QUEUE_FULL) {
            saw_full = 1;
            break;
        }
        EXPECT(st == FEC_OK);
    }
    EXPECT(saw_full);
    fec_encoder_get_stats(encoder, &stats);
    EXPECT(stats.queue_overflow_count >= 1);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    EXPECT(fec_encoder_push(encoder, rec, sizeof(rec), 99) == FEC_OK);
    fec_encoder_destroy(encoder);
    return 0;
}

static int test_partial_group_known_zeros(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    Capture app = {0};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecCallbacks dec_cb = {.output = capture_output, .ctx = &app};
    FecEncoder *encoder;
    FecDecoder *decoder;
    uint8_t rec[10];
    FecStats stats;

    memset(rec, 0xab, sizeof(rec));
    config_profile(&config, 4, 2);
    encoder = fec_encoder_create(&config, &enc_cb);
    decoder = fec_decoder_create(&config, &dec_cb);
    EXPECT(encoder && decoder);
    EXPECT(fec_encoder_reset(encoder, 5) == FEC_OK);
    EXPECT(fec_decoder_reset(decoder, 5) == FEC_OK);
    EXPECT(fec_encoder_push(encoder, rec, sizeof(rec), 1) == FEC_OK);
    EXPECT(fec_encoder_flush(encoder) == FEC_OK);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    EXPECT(feed_decoder(decoder, &wire, 0) == 0);
    EXPECT(app.count == 1);
    EXPECT(app.len[0] == sizeof(rec));
    EXPECT(memcmp(app.buf[0], rec, sizeof(rec)) == 0);
    fec_decoder_get_stats(decoder, &stats);
    EXPECT(stats.over_r_groups == 0);
    fec_encoder_destroy(encoder);
    fec_decoder_destroy(decoder);
    return 0;
}

static int test_pacing_budget(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecEncoder *encoder;
    FecStats stats;

    config_profile(&config, 4, 2);
    encoder = fec_encoder_create(&config, &enc_cb);
    EXPECT(encoder);
    EXPECT(fec_encoder_reset(encoder, 1) == FEC_OK);
    EXPECT(push_records(encoder, 3, 2) == 0);
    EXPECT(fec_encoder_has_pending(encoder));
    EXPECT(fec_encoder_drain(encoder, 1) == FEC_OK);
    EXPECT(wire.count == 1);
    EXPECT(fec_encoder_has_pending(encoder));
    EXPECT(fec_encoder_drain(encoder, 2) == FEC_OK);
    EXPECT(wire.count == 3);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    EXPECT(wire.count == 6);
    EXPECT(!fec_encoder_has_pending(encoder));
    fec_encoder_get_stats(encoder, &stats);
    EXPECT(stats.data_datagrams_tx == 3);
    EXPECT(stats.metadata_datagrams_tx == 1);
    EXPECT(stats.parity_datagrams_tx == 2);
    fec_encoder_destroy(encoder);
    return 0;
}

static int test_wire_rate_bps(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecEncoder *encoder;
    FecStats stats;
    uint64_t wake;

    config_profile(&config, 4, 2);
    config.wire_rate_bps = 8000ull;
    config.wire_burst_bytes = 1500ull;
    encoder = fec_encoder_create(&config, &enc_cb);
    EXPECT(encoder);
    EXPECT(fec_encoder_reset(encoder, 1) == FEC_OK);
    EXPECT(push_records(encoder, 3, 2) == 0);
    EXPECT(fec_encoder_update(encoder, 1) == FEC_OK);
    EXPECT(wire.count == 1);
    fec_encoder_get_stats(encoder, &stats);
    EXPECT(stats.wire_pacing_deferred >= 1);
    EXPECT(fec_encoder_has_pending(encoder));
    wake = fec_encoder_next_update_ns(encoder);
    EXPECT(wake > 1);
    EXPECT(fec_encoder_update(encoder, wake) == FEC_OK);
    EXPECT(wire.count >= 2);
    fec_encoder_destroy(encoder);
    return 0;
}

static int test_shard_size_1300(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    Capture app = {0};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecCallbacks dec_cb = {.output = capture_output, .ctx = &app};
    FecEncoder *encoder;
    FecDecoder *decoder;
    uint8_t rec[40];
    size_t i;

    memset(rec, 0x5a, sizeof(rec));
    config_profile(&config, 4, 2);
    config.shard_size = 1300;
    encoder = fec_encoder_create(&config, &enc_cb);
    decoder = fec_decoder_create(&config, &dec_cb);
    EXPECT(encoder && decoder);
    EXPECT(fec_encoder_reset(encoder, 1) == FEC_OK);
    EXPECT(fec_decoder_reset(decoder, 1) == FEC_OK);
    EXPECT(fec_encoder_push(encoder, rec, sizeof(rec), 1) == FEC_OK);
    EXPECT(fec_encoder_flush(encoder) == FEC_OK);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    EXPECT(wire.count >= 3);
    for (i = 0; i < wire.count; i++) {
        EXPECT(fec_decoder_input(decoder, wire.buf[i], wire.len[i], 2) == FEC_OK);
    }
    EXPECT(app.count == 1);
    EXPECT(app.len[0] == sizeof(rec));
    EXPECT(memcmp(app.buf[0], rec, sizeof(rec)) == 0);
    fec_encoder_destroy(encoder);
    fec_decoder_destroy(decoder);
    return 0;
}

static int test_profile_conflict(void)
{
    FecTransportConfig a;
    FecTransportConfig b;
    Capture wire = {0};
    FecCallbacks cb = {.output = capture_output, .ctx = &wire};
    FecEncoder *enc_a;
    FecEncoder *enc_b;

    config_profile(&a, 4, 2);
    config_profile(&b, 16, 2);
    enc_a = fec_encoder_create(&a, &cb);
    EXPECT(enc_a);
    enc_b = fec_encoder_create(&b, &cb);
    EXPECT(enc_b == NULL);
    fec_encoder_destroy(enc_a);
    enc_b = fec_encoder_create(&b, &cb);
    EXPECT(enc_b);
    fec_encoder_destroy(enc_b);
    return 0;
}

static int test_decoder_blocked_retry(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    Capture app = {0};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecCallbacks dec_cb = {.output = capture_output, .ctx = &app};
    FecEncoder *encoder;
    FecDecoder *decoder;

    config_profile(&config, 4, 2);
    encoder = fec_encoder_create(&config, &enc_cb);
    decoder = fec_decoder_create(&config, &dec_cb);
    EXPECT(encoder && decoder);
    EXPECT(fec_encoder_reset(encoder, 1) == FEC_OK);
    EXPECT(fec_decoder_reset(decoder, 1) == FEC_OK);
    EXPECT(push_records(encoder, 3, 4) == 0);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    app.block_next = 1;
    EXPECT(fec_decoder_input(decoder, wire.buf[0], wire.len[0], 1) == FEC_OK);
    EXPECT(app.count == 0);
    EXPECT(fec_decoder_update(decoder, 2) == FEC_OK);
    EXPECT(app.count == 1);
    fec_encoder_destroy(encoder);
    fec_decoder_destroy(decoder);
    return 0;
}

static int test_decoder_evict_blocked(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    Capture app = {0};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecCallbacks dec_cb = {.output = capture_output, .ctx = &app};
    FecEncoder *encoder;
    FecDecoder *decoder;
    WireHeader first;
    size_t i;
    int saw_full = 0;
    uint8_t extra[8];

    memset(extra, 0x11, sizeof(extra));
    config_profile(&config, 4, 2);
    config.group_window = 1;
    encoder = fec_encoder_create(&config, &enc_cb);
    decoder = fec_decoder_create(&config, &dec_cb);
    EXPECT(encoder && decoder);
    EXPECT(fec_encoder_reset(encoder, 1) == FEC_OK);
    EXPECT(fec_decoder_reset(decoder, 1) == FEC_OK);
    EXPECT(push_records(encoder, 3, 6) == 0);
    EXPECT(fec_encoder_push(encoder, extra, sizeof(extra), 10) == FEC_OK);
    EXPECT(fec_encoder_flush(encoder) == FEC_OK);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    EXPECT(wire_header_decode(&first, wire.buf[0], wire.len[0]) == 0);
    app.block_next = 100;
    EXPECT(fec_decoder_input(decoder, wire.buf[0], wire.len[0], 1) == FEC_OK);
    EXPECT(app.count == 0);
    for (i = 1; i < wire.count; i++) {
        WireHeader header;

        EXPECT(wire_header_decode(&header, wire.buf[i], wire.len[i]) == 0);
        if (header.block_id <= first.block_id) {
            continue;
        }
        EXPECT(fec_decoder_input(decoder, wire.buf[i], wire.len[i], 2) ==
               FEC_ERR_QUEUE_FULL);
        saw_full = 1;
        break;
    }
    EXPECT(saw_full);
    EXPECT(app.count == 0);
    app.block_next = 0;
    EXPECT(fec_decoder_update(decoder, 3) == FEC_OK);
    EXPECT(app.count >= 1);
    fec_encoder_destroy(encoder);
    fec_decoder_destroy(decoder);
    return 0;
}

static int test_drop_any_two_shards(void)
{
    FecTransportConfig config;
    size_t drop_a;
    size_t drop_b;

    config_profile(&config, 4, 2);
    for (drop_a = 0; drop_a < 6u; drop_a++) {
        for (drop_b = drop_a + 1u; drop_b < 6u; drop_b++) {
            Capture wire = {0};
            Capture app = {0};
            FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
            FecCallbacks dec_cb = {.output = capture_output, .ctx = &app};
            FecEncoder *encoder;
            FecDecoder *decoder;
            size_t i;

            encoder = fec_encoder_create(&config, &enc_cb);
            decoder = fec_decoder_create(&config, &dec_cb);
            EXPECT(encoder && decoder);
            EXPECT(fec_encoder_reset(encoder, 1) == FEC_OK);
            EXPECT(fec_decoder_reset(decoder, 1) == FEC_OK);
            EXPECT(push_records(encoder, 3, 9) == 0);
            EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
            for (i = 0; i < wire.count; i++) {
                WireHeader header;

                EXPECT(wire_header_decode(&header, wire.buf[i], wire.len[i]) == 0);
                if (header.shard_index == drop_a ||
                    header.shard_index == drop_b) {
                    continue;
                }
                EXPECT(fec_decoder_input(decoder, wire.buf[i], wire.len[i],
                                         1) == FEC_OK);
            }
            EXPECT(records_match(&app, 3, 9));
            fec_encoder_destroy(encoder);
            fec_decoder_destroy(decoder);
        }
    }
    return 0;
}

static int test_incompatible_wire(void)
{
    FecTransportConfig config;
    Capture app = {0};
    FecCallbacks dec_cb = {.output = capture_output, .ctx = &app};
    FecDecoder *decoder;
    uint8_t junk[8] = {0};
    uint8_t short_pkt[WIRE_HEADER_SIZE - 1u];

    config_profile(&config, 4, 2);
    decoder = fec_decoder_create(&config, &dec_cb);
    EXPECT(decoder);
    EXPECT(fec_decoder_reset(decoder, 1) == FEC_OK);
    memset(short_pkt, 0, sizeof(short_pkt));
    EXPECT(fec_decoder_input(decoder, short_pkt, sizeof(short_pkt), 1) ==
           FEC_ERR_NOT_FEC);
    EXPECT(fec_decoder_input(decoder, junk, sizeof(junk), 1) == FEC_ERR_NOT_FEC);
    fec_decoder_destroy(decoder);
    return 0;
}

typedef struct {
    Capture *app;
    Capture *acks;
} DualCap;

static FecOutputStatus dual_app_output(void *ctx, const uint8_t *data,
                                       size_t len)
{
    DualCap *dual = ctx;

    return capture_output(dual->app, data, len);
}

static FecOutputStatus dual_ack_output(void *ctx, const uint8_t *data,
                                       size_t len)
{
    DualCap *dual = ctx;

    return capture_output(dual->acks, data, len);
}

static void wirehair_cfg(FecTransportConfig *config)
{
    fec_transport_config_init(config);
    config->codec = FEC_CODEC_WIREHAIR;
    config->segment_bytes = 4096u;
    config->repair_percent = 50u;
    config->ack_enabled = 1u;
    config->flush_timeout_ns = 0;
    config->origin_node = 1u;
    config->final_dst = 4u;
    config->ttl = 8u;
    config->flow_id = 7u;
}

static int test_wirehair_roundtrip_and_drop(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    Capture app = {0};
    DualCap dual = {.app = &app, .acks = NULL};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecCallbacks dec_cb = {.output = dual_app_output, .ctx = &dual};
    FecEncoder *encoder;
    FecDecoder *decoder;
    uint8_t payload[3000];
    size_t i;
    FecStats stats;

    for (i = 0; i < sizeof(payload); i++) {
        payload[i] = (uint8_t)(i * 3u + 11u);
    }
    wirehair_cfg(&config);
    config.ack_enabled = 0u;
    encoder = fec_encoder_create(&config, &enc_cb);
    decoder = fec_decoder_create(&config, &dec_cb);
    EXPECT(encoder != NULL && decoder != NULL);
    EXPECT(fec_encoder_push(encoder, payload, sizeof(payload), 1) == FEC_OK);
    EXPECT(fec_encoder_flush(encoder) == FEC_OK);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    EXPECT(wire.count > 0);
    EXPECT(feed_decoder(decoder, &wire, -1) == 0);
    EXPECT(app.count == 1);
    EXPECT(app.len[0] == sizeof(payload));
    EXPECT(memcmp(app.buf[0], payload, sizeof(payload)) == 0);
    fec_encoder_get_stats(encoder, &stats);
    EXPECT(stats.parity_datagrams_tx > 0);
    fec_encoder_destroy(encoder);
    fec_decoder_destroy(decoder);

    memset(&wire, 0, sizeof(wire));
    memset(&app, 0, sizeof(app));
    encoder = fec_encoder_create(&config, &enc_cb);
    decoder = fec_decoder_create(&config, &dec_cb);
    EXPECT(encoder && decoder);
    EXPECT(fec_encoder_push(encoder, payload, sizeof(payload), 1) == FEC_OK);
    EXPECT(fec_encoder_flush(encoder) == FEC_OK);
    EXPECT(fec_encoder_drain(encoder, SIZE_MAX) == FEC_OK);
    EXPECT(feed_decoder(decoder, &wire, 0) == 0);
    EXPECT(app.count == 1 && app.len[0] == sizeof(payload));
    EXPECT(memcmp(app.buf[0], payload, sizeof(payload)) == 0);
    fec_encoder_destroy(encoder);
    fec_decoder_destroy(decoder);
    return 0;
}

static int test_wirehair_ack_stops_repair(void)
{
    FecTransportConfig config;
    Capture wire = {0};
    Capture app = {0};
    Capture acks = {0};
    DualCap dual = {.app = &app, .acks = &acks};
    FecCallbacks enc_cb = {.output = capture_output, .ctx = &wire};
    FecCallbacks dec_cb = {
        .output = dual_app_output,
        .ack_output = dual_ack_output,
        .ctx = &dual};
    FecEncoder *encoder;
    FecDecoder *decoder;
    uint8_t payload[2500];
    size_t i;
    size_t ack_seen = 0;
    FecStats stats;

    for (i = 0; i < sizeof(payload); i++) {
        payload[i] = (uint8_t)(i + 9u);
    }
    wirehair_cfg(&config);
    encoder = fec_encoder_create(&config, &enc_cb);
    decoder = fec_decoder_create(&config, &dec_cb);
    EXPECT(encoder && decoder);
    EXPECT(fec_encoder_push(encoder, payload, sizeof(payload), 1) == FEC_OK);
    EXPECT(fec_encoder_flush(encoder) == FEC_OK);
    while (fec_encoder_has_pending(encoder)) {
        size_t before = wire.count;
        size_t pkt;

        EXPECT(fec_encoder_drain(encoder, 1) == FEC_OK);
        if (wire.count == before) {
            break;
        }
        pkt = wire.count - 1u;
        EXPECT(fec_decoder_input(decoder, wire.buf[pkt], wire.len[pkt],
                                 2000u + pkt) == FEC_OK);
        while (ack_seen < acks.count) {
            EXPECT(fec_encoder_input_ack(encoder, acks.buf[ack_seen],
                                         acks.len[ack_seen]) == FEC_OK);
            ack_seen++;
        }
    }
    EXPECT(app.count == 1);
    EXPECT(app.len[0] == sizeof(payload));
    EXPECT(memcmp(app.buf[0], payload, sizeof(payload)) == 0);
    EXPECT(acks.count >= 1);
    fec_encoder_get_stats(encoder, &stats);
    EXPECT(stats.parity_datagrams_tx < 2);
    fec_encoder_destroy(encoder);
    fec_decoder_destroy(decoder);
    return 0;
}

int main(void)
{
    if (test_clean_roundtrip(4, 2) != 0 ||
        test_clean_roundtrip(8, 4) != 0 ||
        test_clean_roundtrip(16, 2) != 0 ||
        test_drop_one_data_shard() != 0 ||
        test_drop_metadata_and_one_data() != 0 ||
        test_over_r_does_not_forge() != 0 ||
        test_duplicate_and_reorder() != 0 ||
        test_stale_epoch() != 0 ||
        test_blocked_then_drain() != 0 ||
        test_queue_overflow_no_silent_loss() != 0 ||
        test_partial_group_known_zeros() != 0 ||
        test_pacing_budget() != 0 ||
        test_wire_rate_bps() != 0 ||
        test_shard_size_1300() != 0 ||
        test_profile_conflict() != 0 ||
        test_decoder_blocked_retry() != 0 ||
        test_decoder_evict_blocked() != 0 ||
        test_drop_any_two_shards() != 0 ||
        test_incompatible_wire() != 0 ||
        test_wirehair_roundtrip_and_drop() != 0 ||
        test_wirehair_ack_stops_repair() != 0) {
        return 1;
    }
    puts("fec_transport tests passed");
    return 0;
}
