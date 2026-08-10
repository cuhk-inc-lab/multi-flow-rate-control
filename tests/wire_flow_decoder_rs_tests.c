/*
 * P1a: WireFlowDecoder RS geometry is fixed at create time.
 * Packets must not call RsCodec_set_profile_from_shard_count / retune RS.
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

typedef struct RsGeoSnap {
    size_t k;
    size_t r;
} RsGeoSnap;

static void rs_snap(RsGeoSnap *s)
{
    RsCodec_get_params(&s->k, &s->r);
}

static int rs_snap_eq(const RsGeoSnap *a, const RsGeoSnap *b)
{
    return a->k == b->k && a->r == b->r;
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
    if (*expected_shards != (uint16_t)(k + r)) {
        return -1;
    }
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
    hdr->shard_index = 0;
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

static int ingest_rs_block(WireFlowDecoder *dec, uint32_t flow_id,
                           uint64_t block_id, const unsigned char *encoded,
                           uint16_t shard_count, uint16_t valid_len,
                           unsigned drop_shard /* UINT_MAX = none */)
{
    uint16_t shard;
    uint8_t payload[PKG_SIZE];
    WireHeader hdr;

    for (shard = 0; shard < shard_count; shard++) {
        if ((unsigned)shard == drop_shard) {
            continue;
        }
        fill_header_data(&hdr, flow_id, block_id, shard, shard_count, valid_len);
        memcpy(payload, encoded + (size_t)shard * PKG_SIZE, PKG_SIZE);
        if (wire_flow_decoder_ingest(dec, &hdr, payload, PKG_SIZE) != 0) {
            return -1;
        }
    }
    return 0;
}

static void test_rs_decoder_rejects_mismatched_wire_shard_count(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoderConfig cfg;
    WireFlowDecoder *dec;
    WireHeader hdr;
    uint8_t payload[PKG_SIZE];
    const WireFlowDecoderStats *st;

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    EXPECT(expected == 6);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);

    memset(&cfg, 0, sizeof(cfg));
    cfg.flow_id = 1;
    cfg.codec = RsCodec_get();
    cfg.expected_shards = expected;
    cfg.input_size = input_size;
    cfg.output_fn = outbuf_write;
    cfg.output_ctx = &ob;
    dec = wire_flow_decoder_create(&cfg);
    EXPECT(dec != NULL);

    memset(payload, 0x5a, sizeof(payload));
    fill_header_data(&hdr, 1, 0, 0, (uint16_t)(expected + 1u), 64);
    EXPECT(wire_flow_decoder_shard_count_ok(cfg.codec, hdr.shard_count,
                                            expected) == 0);
    EXPECT(wire_flow_decoder_ingest(dec, &hdr, payload, PKG_SIZE) == 0);

    st = wire_flow_decoder_stats(dec);
    EXPECT(st != NULL);
    EXPECT(st->malformed_datagrams >= 1);
    EXPECT(st->received_datagrams == 0);
    EXPECT(st->decoded_blocks == 0);
    EXPECT(ob.writes == 0);
    EXPECT(ob.len == 0);
    EXPECT(wire_flow_decoder_next_block(dec) == 0);
    EXPECT(!wire_flow_decoder_is_complete(dec));

    fill_header_end(&hdr, 1, 1, (uint16_t)(expected + 1u));
    EXPECT(wire_flow_decoder_ingest(dec, &hdr, NULL, 0) == 0);
    EXPECT(wire_flow_decoder_stats(dec)->malformed_datagrams >= 2);
    EXPECT(!wire_flow_decoder_end_seen(dec));
    EXPECT(wire_flow_decoder_next_block(dec) == 0);

    wire_flow_decoder_destroy(dec);
}

static void test_rs_mismatched_packet_does_not_reconfigure_profile(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    RsGeoSnap before;
    RsGeoSnap after;
    OutBuf ob;
    uint8_t out[256];
    WireFlowDecoderConfig cfg;
    WireFlowDecoder *dec;
    WireHeader hdr;
    uint8_t payload[PKG_SIZE];

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    rs_snap(&before);
    EXPECT(before.k == 4u && before.r == 2u);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    memset(&cfg, 0, sizeof(cfg));
    cfg.flow_id = 2;
    cfg.codec = RsCodec_get();
    cfg.expected_shards = expected;
    cfg.input_size = input_size;
    cfg.output_fn = outbuf_write;
    cfg.output_ctx = &ob;
    dec = wire_flow_decoder_create(&cfg);
    EXPECT(dec != NULL);

    memset(payload, 0, sizeof(payload));
    fill_header_data(&hdr, 2, 0, 0, 7 /* N+1 */, 32);
    EXPECT(wire_flow_decoder_ingest(dec, &hdr, payload, PKG_SIZE) == 0);

    /* Attempt via API alone would change profile; decoder must not call it. */
    rs_snap(&after);
    EXPECT(rs_snap_eq(&before, &after));
    EXPECT(RsCodec_get_profile() == RS_PROFILE_4_2);

    wire_flow_decoder_destroy(dec);
}

static void test_rs_valid_fixed_geometry_still_recovers(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoderConfig cfg;
    WireFlowDecoder *dec;
    unsigned char encoded[CODEC_MAX_ENCODE_BLOCK];
    uint8_t plaintext[4 * PKG_SIZE];
    uint16_t valid_len = 0;
    WireHeader end;
    size_t i;

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    for (i = 0; i < sizeof(plaintext); i++) {
        plaintext[i] = (uint8_t)(i * 17u + 5u);
    }
    EXPECT(encode_rs_block(encoded, sizeof(encoded), plaintext, 200u,
                           &valid_len) == 0);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    memset(&cfg, 0, sizeof(cfg));
    cfg.flow_id = 3;
    cfg.codec = RsCodec_get();
    cfg.expected_shards = expected;
    cfg.input_size = input_size;
    cfg.output_fn = outbuf_write;
    cfg.output_ctx = &ob;
    dec = wire_flow_decoder_create(&cfg);
    EXPECT(dec != NULL);

    /* Drop one parity shard; recover must still succeed under fixed geometry. */
    EXPECT(ingest_rs_block(dec, 3, 0, encoded, expected, valid_len, 5u) == 0);
    fill_header_end(&end, 3, 1, expected);
    EXPECT(wire_flow_decoder_ingest(dec, &end, NULL, 0) == 0);

    EXPECT(wire_flow_decoder_is_complete(dec));
    EXPECT(ob.len == valid_len);
    EXPECT(memcmp(out, plaintext, valid_len) == 0);
    EXPECT(wire_flow_decoder_stats(dec)->recovered_groups >= 1);
    EXPECT(wire_flow_decoder_stats(dec)->decoded_blocks == 1);

    wire_flow_decoder_destroy(dec);
}

static void test_rs_two_flows_same_fixed_geometry(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    OutBuf ob0;
    OutBuf ob1;
    uint8_t out0[4096];
    uint8_t out1[4096];
    WireFlowDecoderConfig cfg0;
    WireFlowDecoderConfig cfg1;
    WireFlowDecoder *dec0;
    WireFlowDecoder *dec1;
    unsigned char enc0[CODEC_MAX_ENCODE_BLOCK];
    unsigned char enc1[CODEC_MAX_ENCODE_BLOCK];
    uint8_t pt0[128];
    uint8_t pt1[128];
    uint16_t v0 = 0;
    uint16_t v1 = 0;
    WireHeader end;
    uint16_t shard;
    uint8_t payload[PKG_SIZE];
    WireHeader hdr;

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    memset(pt0, 0x10, sizeof(pt0));
    memset(pt1, 0x20, sizeof(pt1));
    EXPECT(encode_rs_block(enc0, sizeof(enc0), pt0, sizeof(pt0), &v0) == 0);
    EXPECT(encode_rs_block(enc1, sizeof(enc1), pt1, sizeof(pt1), &v1) == 0);

    memset(&ob0, 0, sizeof(ob0));
    ob0.data = out0;
    ob0.cap = sizeof(out0);
    memset(&ob1, 0, sizeof(ob1));
    ob1.data = out1;
    ob1.cap = sizeof(out1);

    memset(&cfg0, 0, sizeof(cfg0));
    cfg0.flow_id = 101;
    cfg0.codec = RsCodec_get();
    cfg0.expected_shards = expected;
    cfg0.input_size = input_size;
    cfg0.output_fn = outbuf_write;
    cfg0.output_ctx = &ob0;
    memset(&cfg1, 0, sizeof(cfg1));
    cfg1.flow_id = 202;
    cfg1.codec = RsCodec_get();
    cfg1.expected_shards = expected;
    cfg1.input_size = input_size;
    cfg1.output_fn = outbuf_write;
    cfg1.output_ctx = &ob1;

    dec0 = wire_flow_decoder_create(&cfg0);
    dec1 = wire_flow_decoder_create(&cfg1);
    EXPECT(dec0 != NULL && dec1 != NULL);

    for (shard = 0; shard < expected; shard++) {
        fill_header_data(&hdr, 101, 0, shard, expected, v0);
        memcpy(payload, enc0 + (size_t)shard * PKG_SIZE, PKG_SIZE);
        EXPECT(wire_flow_decoder_ingest(dec0, &hdr, payload, PKG_SIZE) == 0);

        fill_header_data(&hdr, 202, 0, shard, expected, v1);
        memcpy(payload, enc1 + (size_t)shard * PKG_SIZE, PKG_SIZE);
        EXPECT(wire_flow_decoder_ingest(dec1, &hdr, payload, PKG_SIZE) == 0);
    }
    fill_header_end(&end, 101, 1, expected);
    EXPECT(wire_flow_decoder_ingest(dec0, &end, NULL, 0) == 0);
    fill_header_end(&end, 202, 1, expected);
    EXPECT(wire_flow_decoder_ingest(dec1, &end, NULL, 0) == 0);

    EXPECT(wire_flow_decoder_is_complete(dec0));
    EXPECT(wire_flow_decoder_is_complete(dec1));
    EXPECT(ob0.len == sizeof(pt0) && memcmp(out0, pt0, sizeof(pt0)) == 0);
    EXPECT(ob1.len == sizeof(pt1) && memcmp(out1, pt1, sizeof(pt1)) == 0);

    wire_flow_decoder_destroy(dec0);
    wire_flow_decoder_destroy(dec1);
}

static void test_rs_mismatch_then_valid_flow_still_succeeds(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;
    RsGeoSnap before;
    RsGeoSnap after;
    OutBuf ob;
    uint8_t out[4096];
    WireFlowDecoderConfig cfg;
    WireFlowDecoder *dec;
    unsigned char encoded[CODEC_MAX_ENCODE_BLOCK];
    uint8_t plaintext[96];
    uint16_t valid_len = 0;
    WireHeader hdr;
    WireHeader end;
    uint8_t payload[PKG_SIZE];

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    rs_snap(&before);
    memset(plaintext, 0xab, sizeof(plaintext));
    EXPECT(encode_rs_block(encoded, sizeof(encoded), plaintext, sizeof(plaintext),
                           &valid_len) == 0);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    memset(&cfg, 0, sizeof(cfg));
    cfg.flow_id = 9;
    cfg.codec = RsCodec_get();
    cfg.expected_shards = expected;
    cfg.input_size = input_size;
    cfg.output_fn = outbuf_write;
    cfg.output_ctx = &ob;
    dec = wire_flow_decoder_create(&cfg);
    EXPECT(dec != NULL);

    fill_header_data(&hdr, 9, 0, 0, (uint16_t)(expected + 1u), valid_len);
    memset(payload, 0xff, sizeof(payload));
    EXPECT(wire_flow_decoder_ingest(dec, &hdr, payload, PKG_SIZE) == 0);
    rs_snap(&after);
    EXPECT(rs_snap_eq(&before, &after));

    EXPECT(ingest_rs_block(dec, 9, 0, encoded, expected, valid_len,
                           (unsigned)-1) == 0);
    fill_header_end(&end, 9, 1, expected);
    EXPECT(wire_flow_decoder_ingest(dec, &end, NULL, 0) == 0);

    EXPECT(wire_flow_decoder_is_complete(dec));
    EXPECT(ob.len == sizeof(plaintext));
    EXPECT(memcmp(out, plaintext, sizeof(plaintext)) == 0);
    rs_snap(&after);
    EXPECT(rs_snap_eq(&before, &after));

    wire_flow_decoder_destroy(dec);
}

static void test_shard_count_ok_requires_exact_match_for_rs(void)
{
    uint16_t expected = 0;
    size_t input_size = 0;

    EXPECT(init_rs_geometry(4u, 2u, &expected, &input_size) == 0);
    EXPECT(wire_flow_decoder_shard_count_ok(RsCodec_get(), expected, expected));
    EXPECT(!wire_flow_decoder_shard_count_ok(RsCodec_get(), expected + 1u,
                                             expected));
    EXPECT(!wire_flow_decoder_shard_count_ok(RsCodec_get(), 5u, expected));
    /* Old RS special-case accepted 5/6/7 for k=4; must not. */
    EXPECT(!wire_flow_decoder_shard_count_ok(RsCodec_get(), 5u, 6u));
    EXPECT(!wire_flow_decoder_shard_count_ok(RsCodec_get(), 7u, 6u));
}

int main(void)
{
    test_shard_count_ok_requires_exact_match_for_rs();
    test_rs_decoder_rejects_mismatched_wire_shard_count();
    test_rs_mismatched_packet_does_not_reconfigure_profile();
    test_rs_valid_fixed_geometry_still_recovers();
    test_rs_two_flows_same_fixed_geometry();
    test_rs_mismatch_then_valid_flow_still_succeeds();

    if (g_failures != 0) {
        fprintf(stderr, "wire_flow_decoder_rs_tests: %d failure(s)\n",
                g_failures);
        return EXIT_FAILURE;
    }
    fprintf(stderr, "wire_flow_decoder_rs_tests: ok\n");
    return EXIT_SUCCESS;
}
