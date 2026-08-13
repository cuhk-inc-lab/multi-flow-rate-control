/*
 * Phase A/C: out-of-order receive + per-group recovery + ordered emit,
 * reorder-window counters, and best-effort skip of unrecoverable heads.
 */
#include "codec.h"
#include "rs_codec.h"
#include "stream_config.h"
#include "wire_flow_decoder.h"
#include "wire_header.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_failures = 0;

#define EXPECT(cond)                                                           \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);    \
            g_failures++;                                                      \
        }                                                                      \
    } while (0)

typedef struct OutBuf {
    uint8_t *data;
    size_t   cap;
    size_t   len;
    int      writes;
} OutBuf;

static int outbuf_write(uint32_t flow_id, const uint8_t *data, size_t len,
                        void *ctx)
{
    OutBuf *ob = ctx;

    (void)flow_id;
    if (ob == NULL || data == NULL) {
        return -1;
    }
    if (ob->len + len > ob->cap) {
        return -1;
    }
    memcpy(ob->data + ob->len, data, len);
    ob->len += len;
    ob->writes++;
    return 0;
}

static int init_rs_geometry(size_t k, size_t r, uint16_t *expected_shards,
                            size_t *input_size)
{
    const Codec *codec;
    size_t out_size;

    if (RsCodec_set_params(k, r) != 0) {
        return -1;
    }
    codec = RsCodec_get();
    if (codec == NULL) {
        return -1;
    }
    *input_size = Codec_input_block_size(codec);
    out_size = Codec_output_block_size(codec);
    if (*input_size == 0 || out_size == 0 || out_size % PKG_SIZE != 0) {
        return -1;
    }
    *expected_shards = (uint16_t)(out_size / PKG_SIZE);
    return 0;
}

static void fill_header_data(WireHeader *hdr, uint32_t flow_id, uint64_t block_id,
                             uint16_t shard_index, uint16_t shard_count,
                             uint16_t valid_len)
{
    memset(hdr, 0, sizeof(*hdr));
    hdr->type = WIRE_TYPE_DATA;
    hdr->final_dst = 4;
    hdr->ttl = 8;
    hdr->flow_id = flow_id;
    hdr->block_id = block_id;
    hdr->shard_index = shard_index;
    hdr->shard_count = shard_count;
    hdr->valid_len = valid_len;
    hdr->payload_len = (uint16_t)PKG_SIZE;
}

static void fill_header_end(WireHeader *hdr, uint32_t flow_id,
                            uint64_t block_count, uint16_t shard_count)
{
    memset(hdr, 0, sizeof(*hdr));
    hdr->type = WIRE_TYPE_END;
    hdr->final_dst = 4;
    hdr->ttl = 8;
    hdr->flow_id = flow_id;
    hdr->block_id = block_count;
    hdr->shard_count = shard_count;
    hdr->valid_len = 0;
    hdr->payload_len = 0;
}

static int encode_rs_block(unsigned char *encoded, size_t encoded_cap,
                           const uint8_t *plaintext, size_t plen,
                           uint16_t *valid_len)
{
    const Codec *codec = RsCodec_get();
    size_t out_size;

    if (codec == NULL || plaintext == NULL || plen == 0) {
        return -1;
    }
    out_size = Codec_output_block_size(codec);
    if (out_size > encoded_cap || plen > Codec_input_block_size(codec)) {
        return -1;
    }
    memset(encoded, 0, out_size);
    memcpy(encoded, plaintext, plen);
    Codec_encode(codec, encoded, out_size);
    *valid_len = (uint16_t)plen;
    return 0;
}

static WireFlowDecoder *make_dec_mode(uint32_t flow_id, uint16_t expected,
                                     size_t input_size, OutBuf *ob,
                                     int best_effort)
{
    WireFlowDecoderConfig cfg;

    memset(&cfg, 0, sizeof(cfg));
    cfg.flow_id = flow_id;
    cfg.codec = RsCodec_get();
    cfg.expected_shards = expected;
    cfg.input_size = input_size;
    cfg.output_fn = outbuf_write;
    cfg.output_ctx = ob;
    cfg.best_effort = best_effort;
    return wire_flow_decoder_create(&cfg);
}

static WireFlowDecoder *make_dec(uint32_t flow_id, uint16_t expected,
                                 size_t input_size, OutBuf *ob)
{
    return make_dec_mode(flow_id, expected, input_size, ob, 0);
}

static int ingest_one_shard(WireFlowDecoder *dec, uint32_t flow_id,
                            uint64_t block_id, uint16_t shard_index,
                            uint16_t shard_count, uint16_t valid_len,
                            const unsigned char *encoded)
{
    WireHeader hdr;
    uint8_t payload[PKG_SIZE];

    fill_header_data(&hdr, flow_id, block_id, shard_index, shard_count, valid_len);
    memcpy(payload, encoded + (size_t)shard_index * PKG_SIZE, PKG_SIZE);
    return wire_flow_decoder_ingest(dec, &hdr, payload, PKG_SIZE);
}

static int ingest_rs_block(WireFlowDecoder *dec, uint32_t flow_id,
                           uint64_t block_id, const unsigned char *encoded,
                           uint16_t shard_count, uint16_t valid_len,
                           unsigned drop_shard)
{
    uint16_t shard;

    for (shard = 0; shard < shard_count; shard++) {
        if ((unsigned)shard == drop_shard) {
            continue;
        }
        if (ingest_one_shard(dec, flow_id, block_id, shard, shard_count,
                               valid_len, encoded) != 0) {
            return -1;
        }
    }
    return 0;
}

static int ingest_rs_block_drop_many(WireFlowDecoder *dec, uint32_t flow_id,
                                     uint64_t block_id,
                                     const unsigned char *encoded,
                                     uint16_t shard_count, uint16_t valid_len,
                                     const unsigned *drop_shards, size_t drop_n)
{
    uint16_t shard;
    size_t di;

    for (shard = 0; shard < shard_count; shard++) {
        for (di = 0; di < drop_n; di++) {
            if (drop_shards[di] == (unsigned)shard) {
                goto next_shard;
            }
        }
        if (ingest_one_shard(dec, flow_id, block_id, shard, shard_count,
                               valid_len, encoded) != 0) {
            return -1;
        }
    next_shard:
        continue;
    }
    return 0;
}

static void send_end(WireFlowDecoder *dec, uint32_t flow_id, uint64_t block_count,
                     uint16_t shard_count)
{
    WireHeader end;

    fill_header_end(&end, flow_id, block_count, shard_count);
    EXPECT(wire_flow_decoder_ingest(dec, &end, NULL, 0) == 0);
}

static void test_reverse_order_ingest(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    OutBuf ob;
    uint8_t out[16384];
    WireFlowDecoder *dec;
    unsigned char enc[3][CODEC_MAX_ENCODE_BLOCK];
    uint8_t plaintext[3][200];
    uint16_t valid[3];
    size_t i;

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    for (i = 0; i < 3; i++) {
        memset(plaintext[i], (uint8_t)(i + 1), sizeof(plaintext[i]));
        EXPECT(encode_rs_block(enc[i], sizeof(enc[i]), plaintext[i],
                               120u + (uint16_t)i, &valid[i]) == 0);
    }

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec(1, expected, input_size, &ob);
    EXPECT(dec != NULL);

    EXPECT(ingest_rs_block(dec, 1, 2, enc[2], expected, valid[2], (unsigned)-1) == 0);
    EXPECT(ingest_rs_block(dec, 1, 1, enc[1], expected, valid[1], (unsigned)-1) == 0);
    EXPECT(ingest_rs_block(dec, 1, 0, enc[0], expected, valid[0], (unsigned)-1) == 0);
    send_end(dec, 1, 3, expected);

    EXPECT(wire_flow_decoder_is_complete(dec));
    EXPECT(wire_flow_decoder_stats(dec)->decoded_blocks == 3);
    EXPECT(ob.len == (size_t)valid[0] + valid[1] + valid[2]);
    EXPECT(memcmp(out, plaintext[0], valid[0]) == 0);
    EXPECT(memcmp(out + valid[0], plaintext[1], valid[1]) == 0);
    EXPECT(memcmp(out + valid[0] + valid[1], plaintext[2], valid[2]) == 0);

    wire_flow_decoder_destroy(dec);
}

static void test_early_block_delayed_last(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    OutBuf ob;
    uint8_t out[16384];
    WireFlowDecoder *dec;
    unsigned char enc[3][CODEC_MAX_ENCODE_BLOCK];
    uint8_t plaintext[3][200];
    uint16_t valid[3];
    size_t i;

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    for (i = 0; i < 3; i++) {
        memset(plaintext[i], (uint8_t)(0x30u + i), sizeof(plaintext[i]));
        EXPECT(encode_rs_block(enc[i], sizeof(enc[i]), plaintext[i], 100u,
                               &valid[i]) == 0);
    }

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec(2, expected, input_size, &ob);
    EXPECT(dec != NULL);

    EXPECT(ingest_rs_block(dec, 2, 1, enc[1], expected, valid[1], (unsigned)-1) == 0);
    EXPECT(ingest_rs_block(dec, 2, 2, enc[2], expected, valid[2], (unsigned)-1) == 0);
    EXPECT(wire_flow_decoder_next_block(dec) == 0);
    EXPECT(wire_flow_decoder_stats(dec)->decoded_blocks == 0);
    EXPECT(wire_flow_decoder_stats(dec)->recovered_groups >= 2);

    EXPECT(ingest_rs_block(dec, 2, 0, enc[0], expected, valid[0], (unsigned)-1) == 0);
    send_end(dec, 2, 3, expected);

    EXPECT(wire_flow_decoder_is_complete(dec));
    EXPECT(wire_flow_decoder_stats(dec)->decoded_blocks == 3);
    EXPECT(ob.len == (size_t)valid[0] + valid[1] + valid[2]);

    wire_flow_decoder_destroy(dec);
}

static void test_later_recovered_while_head_missing(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoder *dec;
    unsigned char enc0[CODEC_MAX_ENCODE_BLOCK];
    unsigned char enc1[CODEC_MAX_ENCODE_BLOCK];
    uint8_t pt0[128];
    uint8_t pt1[128];
    uint16_t v0 = 0;
    uint16_t v1 = 0;
    const WireFlowDecoderStats *st;

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    memset(pt0, 0x11, sizeof(pt0));
    memset(pt1, 0x22, sizeof(pt1));
    EXPECT(encode_rs_block(enc0, sizeof(enc0), pt0, sizeof(pt0), &v0) == 0);
    EXPECT(encode_rs_block(enc1, sizeof(enc1), pt1, sizeof(pt1), &v1) == 0);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec(3, expected, input_size, &ob);
    EXPECT(dec != NULL);

    /* Block 1 complete; block 0 missing. */
    EXPECT(ingest_rs_block(dec, 3, 1, enc1, expected, v1, (unsigned)-1) == 0);
    st = wire_flow_decoder_stats(dec);
    EXPECT(st->recovered_groups >= 1);
    EXPECT(st->decoded_blocks == 0);
    EXPECT(st->groups_received >= 1);
    EXPECT(wire_flow_decoder_pending_recovered_groups(dec) >= 1);
    EXPECT(wire_flow_decoder_next_block(dec) == 0);

    send_end(dec, 3, 2, expected);
    EXPECT(!wire_flow_decoder_is_complete(dec));
    EXPECT(wire_flow_decoder_next_block(dec) == 0);
    EXPECT(st->decoded_blocks == 0);
    EXPECT(st->skipped_groups == 0);
    EXPECT(wire_flow_decoder_pending_recovered_groups(dec) >= 1);

    /* Head arrives after END — strict still incomplete until head recoverable. */
    EXPECT(ingest_rs_block(dec, 3, 0, enc0, expected, v0, (unsigned)-1) == 0);
    EXPECT(wire_flow_decoder_is_complete(dec));
    EXPECT(st->decoded_blocks == 2);
    EXPECT(ob.len == sizeof(pt0) + sizeof(pt1));
    EXPECT(memcmp(out, pt0, sizeof(pt0)) == 0);
    EXPECT(memcmp(out + sizeof(pt0), pt1, sizeof(pt1)) == 0);

    wire_flow_decoder_destroy(dec);
}

static void test_rs_loss_within_parity(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoder *dec;
    unsigned char encoded[CODEC_MAX_ENCODE_BLOCK];
    uint8_t plaintext[200];
    uint16_t valid_len = 0;

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    memset(plaintext, 0x55, sizeof(plaintext));
    EXPECT(encode_rs_block(encoded, sizeof(encoded), plaintext, 180u,
                           &valid_len) == 0);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec(4, expected, input_size, &ob);
    EXPECT(dec != NULL);

    EXPECT(ingest_rs_block(dec, 4, 0, encoded, expected, valid_len, 1u) == 0);
    send_end(dec, 4, 1, expected);

    EXPECT(wire_flow_decoder_is_complete(dec));
    EXPECT(wire_flow_decoder_stats(dec)->recovered_groups >= 1);
    EXPECT(ob.len == valid_len);
    EXPECT(memcmp(out, plaintext, valid_len) == 0);

    wire_flow_decoder_destroy(dec);
}

static void test_rs_loss_exceeds_parity_strict(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoder *dec;
    unsigned char encoded[CODEC_MAX_ENCODE_BLOCK];
    uint8_t plaintext[200];
    uint16_t valid_len = 0;
    unsigned drop[3] = {0, 1, 2};

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    memset(plaintext, 0x66, sizeof(plaintext));
    EXPECT(encode_rs_block(encoded, sizeof(encoded), plaintext, 180u,
                           &valid_len) == 0);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec(5, expected, input_size, &ob);
    EXPECT(dec != NULL);

    EXPECT(ingest_rs_block_drop_many(dec, 5, 0, encoded, expected, valid_len,
                                     drop, 3) == 0);
    send_end(dec, 5, 1, expected);

    EXPECT(!wire_flow_decoder_is_complete(dec));
    EXPECT(wire_flow_decoder_next_block(dec) == 0);
    EXPECT(wire_flow_decoder_stats(dec)->decoded_blocks == 0);
    EXPECT(wire_flow_decoder_stats(dec)->groups_failed >= 1);
    EXPECT(wire_flow_decoder_stats(dec)->skipped_groups == 0);
    EXPECT(ob.len == 0);

    wire_flow_decoder_destroy(dec);
}

static void test_duplicate_shard_after_recovery(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoder *dec;
    unsigned char enc1[CODEC_MAX_ENCODE_BLOCK];
    uint8_t pt1[128];
    uint16_t v1 = 0;
    uint64_t recovered_before;
    WireHeader hdr;
    uint8_t payload[PKG_SIZE];

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    memset(pt1, 0x77, sizeof(pt1));
    EXPECT(encode_rs_block(enc1, sizeof(enc1), pt1, sizeof(pt1), &v1) == 0);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec(6, expected, input_size, &ob);
    EXPECT(dec != NULL);

    /* Block 1 recovers while block 0 (head) is absent — stays RECOVERED, not emitted. */
    EXPECT(ingest_rs_block(dec, 6, 1, enc1, expected, v1, 1u) == 0);
    recovered_before = wire_flow_decoder_stats(dec)->recovered_groups;
    EXPECT(recovered_before >= 1);
    EXPECT(wire_flow_decoder_stats(dec)->decoded_blocks == 0);

    fill_header_data(&hdr, 6, 1, 0, expected, v1);
    memcpy(payload, enc1, PKG_SIZE);
    EXPECT(wire_flow_decoder_ingest(dec, &hdr, payload, PKG_SIZE) == 0);
    EXPECT(wire_flow_decoder_stats(dec)->duplicate_datagrams >= 1);
    EXPECT(wire_flow_decoder_stats(dec)->recovered_groups == recovered_before);
    EXPECT(wire_flow_decoder_stats(dec)->decoded_blocks == 0);

    wire_flow_decoder_destroy(dec);
}

static void test_late_shard_after_emit(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoder *dec;
    unsigned char encoded[CODEC_MAX_ENCODE_BLOCK];
    uint8_t plaintext[200];
    uint16_t valid_len = 0;
    WireHeader hdr;
    uint8_t payload[PKG_SIZE];

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    memset(plaintext, 0x88, sizeof(plaintext));
    EXPECT(encode_rs_block(encoded, sizeof(encoded), plaintext, 180u,
                           &valid_len) == 0);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec(7, expected, input_size, &ob);
    EXPECT(dec != NULL);

    EXPECT(ingest_rs_block(dec, 7, 0, encoded, expected, valid_len,
                           (unsigned)-1) == 0);
    send_end(dec, 7, 1, expected);
    EXPECT(wire_flow_decoder_is_complete(dec));
    EXPECT(wire_flow_decoder_next_block(dec) == 1);

    fill_header_data(&hdr, 7, 0, 2, expected, valid_len);
    memcpy(payload, encoded + 2 * PKG_SIZE, PKG_SIZE);
    EXPECT(wire_flow_decoder_ingest(dec, &hdr, payload, PKG_SIZE) == 0);
    EXPECT(wire_flow_decoder_stats(dec)->late_datagrams >= 1);
    EXPECT(ob.len == valid_len);

    wire_flow_decoder_destroy(dec);
}

static void test_window_overflow_beyond_reorder_limit(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoder *dec;
    unsigned char encoded[CODEC_MAX_ENCODE_BLOCK];
    uint8_t plaintext[128];
    uint16_t valid_len = 0;
    const WireFlowDecoderStats *st;
    uint64_t overflow_before;

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    memset(plaintext, 0x99, sizeof(plaintext));
    EXPECT(encode_rs_block(encoded, sizeof(encoded), plaintext, sizeof(plaintext),
                           &valid_len) == 0);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec(8, expected, input_size, &ob);
    EXPECT(dec != NULL);

    /* block 128 is outside [0, 128). */
    EXPECT(ingest_rs_block(dec, 8, (uint64_t)WIRE_FLOW_GROUP_WINDOW, encoded,
                           expected, valid_len, (unsigned)-1) == 0);
    st = wire_flow_decoder_stats(dec);
    EXPECT(st->window_overflow >= 1);
    EXPECT(st->groups_received == 0);
    EXPECT(st->decoded_blocks == 0);
    EXPECT(st->late_datagrams == 0);
    EXPECT(wire_flow_decoder_next_block(dec) == 0);
    overflow_before = st->window_overflow;

    /* block W-1 is inside the window and may recover without emitting. */
    EXPECT(ingest_rs_block(dec, 8, (uint64_t)WIRE_FLOW_GROUP_WINDOW - 1u, encoded,
                           expected, valid_len, (unsigned)-1) == 0);
    EXPECT(wire_flow_decoder_pending_recovered_groups(dec) >= 1);
    EXPECT(wire_flow_decoder_stats(dec)->decoded_blocks == 0);
    EXPECT(wire_flow_decoder_stats(dec)->groups_received >= 1);
    EXPECT(wire_flow_decoder_stats(dec)->window_overflow == overflow_before);

    wire_flow_decoder_destroy(dec);
}

static void test_best_effort_skips_missing_head_and_emits_later(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoder *dec;
    unsigned char enc1[CODEC_MAX_ENCODE_BLOCK];
    uint8_t pt1[128];
    uint16_t v1 = 0;
    const WireFlowDecoderStats *st;

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    memset(pt1, 0x22, sizeof(pt1));
    EXPECT(encode_rs_block(enc1, sizeof(enc1), pt1, sizeof(pt1), &v1) == 0);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec_mode(9, expected, input_size, &ob, 1);
    EXPECT(dec != NULL);

    EXPECT(ingest_rs_block(dec, 9, 1, enc1, expected, v1, (unsigned)-1) == 0);
    st = wire_flow_decoder_stats(dec);
    EXPECT(st->decoded_blocks == 0);
    EXPECT(st->skipped_groups == 0);
    EXPECT(wire_flow_decoder_pending_recovered_groups(dec) >= 1);
    EXPECT(!wire_flow_decoder_is_complete(dec));

    send_end(dec, 9, 2, expected);
    EXPECT(wire_flow_decoder_is_complete(dec));
    EXPECT(st->skipped_groups == 1);
    EXPECT(st->decoded_blocks == 1);
    EXPECT(st->dropped_groups == 0);
    EXPECT(wire_flow_decoder_next_block(dec) == 2);
    EXPECT(wire_flow_decoder_pending_recovered_groups(dec) == 0);
    EXPECT(ob.len == sizeof(pt1));
    EXPECT(memcmp(out, pt1, sizeof(pt1)) == 0);

    wire_flow_decoder_destroy(dec);
}

static void test_best_effort_skips_failed_head_and_emits_later(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoder *dec;
    unsigned char enc0[CODEC_MAX_ENCODE_BLOCK];
    unsigned char enc1[CODEC_MAX_ENCODE_BLOCK];
    uint8_t pt0[2000];
    uint8_t pt1[2000];
    uint16_t v0 = 0;
    uint16_t v1 = 0;
    unsigned drop[3] = {0, 1, 2};
    const WireFlowDecoderStats *st;

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    memset(pt0, 0x31, sizeof(pt0));
    memset(pt1, 0x32, sizeof(pt1));
    EXPECT(encode_rs_block(enc0, sizeof(enc0), pt0, sizeof(pt0), &v0) == 0);
    EXPECT(encode_rs_block(enc1, sizeof(enc1), pt1, sizeof(pt1), &v1) == 0);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec_mode(10, expected, input_size, &ob, 1);
    EXPECT(dec != NULL);

    EXPECT(ingest_rs_block_drop_many(dec, 10, 0, enc0, expected, v0, drop, 3) ==
           0);
    EXPECT(ingest_rs_block(dec, 10, 1, enc1, expected, v1, (unsigned)-1) == 0);
    send_end(dec, 10, 2, expected);

    st = wire_flow_decoder_stats(dec);
    EXPECT(wire_flow_decoder_is_complete(dec));
    EXPECT(st->skipped_groups == 1);
    EXPECT(st->groups_failed >= 1);
    EXPECT(st->decoded_blocks == 1);
    EXPECT(st->dropped_groups == 0);
    EXPECT(wire_flow_decoder_next_block(dec) == 2);
    EXPECT(ob.len >= sizeof(pt1));
    EXPECT(memcmp(out + (ob.len - sizeof(pt1)), pt1, sizeof(pt1)) == 0);

    wire_flow_decoder_destroy(dec);
}

int main(void)
{
    test_reverse_order_ingest();
    test_early_block_delayed_last();
    test_later_recovered_while_head_missing();
    test_rs_loss_within_parity();
    test_rs_loss_exceeds_parity_strict();
    test_duplicate_shard_after_recovery();
    test_late_shard_after_emit();
    test_window_overflow_beyond_reorder_limit();
    test_best_effort_skips_missing_head_and_emits_later();
    test_best_effort_skips_failed_head_and_emits_later();

    if (g_failures != 0) {
        fprintf(stderr, "wire_flow_decoder_reorder_tests: %d failure(s)\n",
                g_failures);
        return EXIT_FAILURE;
    }
    fprintf(stderr, "wire_flow_decoder_reorder_tests: ok\n");
    return EXIT_SUCCESS;
}
