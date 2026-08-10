/*
 * P1b: systematic codecs may complete a block when all original data shards
 * [0, Codec_data_shards()) are present, even if pad/parity shards are missing.
 */
#include "codec.h"
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
    if (ob == NULL || data == NULL || ob->len + len > ob->cap) {
        return -1;
    }
    memcpy(ob->data + ob->len, data, len);
    ob->len += len;
    ob->writes++;
    return 0;
}

static void fill_data(WireHeader *hdr, uint32_t flow_id, uint64_t block_id,
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

static void fill_end(WireHeader *hdr, uint32_t flow_id, uint64_t block_count,
                     uint16_t shard_count)
{
    memset(hdr, 0, sizeof(*hdr));
    hdr->type = WIRE_TYPE_END;
    hdr->final_dst = 4;
    hdr->ttl = 8;
    hdr->flow_id = flow_id;
    hdr->block_id = block_count;
    hdr->shard_count = shard_count;
}

static int encode_block(const Codec *codec, unsigned char *encoded,
                        size_t encoded_cap, const uint8_t *plaintext,
                        size_t plen, uint16_t *valid_len, uint16_t *shard_count)
{
    size_t out_size;
    size_t in_size;

    if (codec == NULL || plaintext == NULL || plen == 0) {
        return -1;
    }
    in_size = Codec_input_block_size(codec);
    out_size = Codec_output_block_size(codec);
    if (plen > in_size || out_size > encoded_cap || out_size % PKG_SIZE != 0) {
        return -1;
    }
    memset(encoded, 0, out_size);
    memcpy(encoded, plaintext, plen);
    Codec_encode(codec, encoded, out_size);
    *valid_len = (uint16_t)plen;
    *shard_count = (uint16_t)(out_size / PKG_SIZE);
    return 0;
}

static WireFlowDecoder *make_dec(const Codec *codec, uint32_t flow_id,
                                 OutBuf *ob)
{
    WireFlowDecoderConfig cfg;
    size_t out_size;

    memset(&cfg, 0, sizeof(cfg));
    cfg.flow_id = flow_id;
    cfg.codec = codec;
    cfg.input_size = Codec_input_block_size(codec);
    out_size = Codec_output_block_size(codec);
    cfg.expected_shards = (uint16_t)(out_size / PKG_SIZE);
    cfg.output_fn = outbuf_write;
    cfg.output_ctx = ob;
    return wire_flow_decoder_create(&cfg);
}

static int ingest_shard(WireFlowDecoder *dec, uint32_t flow_id,
                        uint64_t block_id, uint16_t shard_index,
                        uint16_t shard_count, uint16_t valid_len,
                        const unsigned char *encoded)
{
    WireHeader hdr;
    uint8_t payload[PKG_SIZE];

    fill_data(&hdr, flow_id, block_id, shard_index, shard_count, valid_len);
    memcpy(payload, encoded + (size_t)shard_index * PKG_SIZE, PKG_SIZE);
    return wire_flow_decoder_ingest(dec, &hdr, payload, PKG_SIZE);
}

static int ingest_data_shards_only(WireFlowDecoder *dec, uint32_t flow_id,
                                   uint64_t block_id,
                                   const unsigned char *encoded,
                                   uint16_t shard_count, uint16_t valid_len,
                                   size_t data_shards)
{
    size_t shard;

    for (shard = 0; shard < data_shards; shard++) {
        if (ingest_shard(dec, flow_id, block_id, (uint16_t)shard, shard_count,
                         valid_len, encoded) != 0) {
            return -1;
        }
    }
    return 0;
}

static void test_systematic_complete_without_nondata_shards_copy(void)
{
    const Codec *codec = CopyCodec_get();
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoder *dec;
    unsigned char encoded[ENCODE_BLOCK];
    uint8_t plaintext[200];
    uint16_t valid_len = 0;
    uint16_t shard_count = 0;
    WireHeader end;
    size_t i;
    size_t data_shards;

    EXPECT(codec != NULL && Codec_is_systematic(codec));
    data_shards = Codec_data_shards(codec);
    EXPECT(data_shards == 4);
    EXPECT(Codec_parity_shards(codec) == 4);

    for (i = 0; i < sizeof(plaintext); i++) {
        plaintext[i] = (uint8_t)(i * 3u + 1u);
    }
    EXPECT(encode_block(codec, encoded, sizeof(encoded), plaintext,
                        sizeof(plaintext), &valid_len, &shard_count) == 0);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec(codec, 1, &ob);
    EXPECT(dec != NULL);

    /* Drop all pad shards 4..7. */
    EXPECT(ingest_data_shards_only(dec, 1, 0, encoded, shard_count, valid_len,
                                   data_shards) == 0);
    fill_end(&end, 1, 1, shard_count);
    EXPECT(wire_flow_decoder_ingest(dec, &end, NULL, 0) == 0);

    EXPECT(wire_flow_decoder_is_complete(dec));
    EXPECT(ob.len == sizeof(plaintext));
    EXPECT(memcmp(out, plaintext, sizeof(plaintext)) == 0);
    EXPECT(wire_flow_decoder_stats(dec)->decoded_blocks == 1);

    wire_flow_decoder_destroy(dec);
}

static void test_systematic_complete_without_nondata_shards_xor(void)
{
    const Codec *codec = XorFecCodec_get();
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoder *dec;
    unsigned char encoded[XOR_FEC_ENCODE_BLOCK];
    uint8_t plaintext[180];
    uint16_t valid_len = 0;
    uint16_t shard_count = 0;
    WireHeader end;
    size_t i;

    EXPECT(codec != NULL && Codec_is_systematic(codec));
    EXPECT(Codec_data_shards(codec) == 4);
    EXPECT(Codec_parity_shards(codec) == 1);

    for (i = 0; i < sizeof(plaintext); i++) {
        plaintext[i] = (uint8_t)(0xa0 + i);
    }
    EXPECT(encode_block(codec, encoded, sizeof(encoded), plaintext,
                        sizeof(plaintext), &valid_len, &shard_count) == 0);
    EXPECT(shard_count == 5);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec(codec, 2, &ob);
    EXPECT(dec != NULL);

    /* Drop parity shard index 4. */
    EXPECT(ingest_data_shards_only(dec, 2, 0, encoded, shard_count, valid_len,
                                   4) == 0);
    fill_end(&end, 2, 1, shard_count);
    EXPECT(wire_flow_decoder_ingest(dec, &end, NULL, 0) == 0);

    EXPECT(wire_flow_decoder_is_complete(dec));
    EXPECT(ob.len == sizeof(plaintext));
    EXPECT(memcmp(out, plaintext, sizeof(plaintext)) == 0);

    wire_flow_decoder_destroy(dec);
}

static void test_systematic_data_shard_missing_does_not_emit(void)
{
    const Codec *codec = CopyCodec_get();
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoder *dec;
    unsigned char encoded[ENCODE_BLOCK];
    uint8_t plaintext[160];
    uint16_t valid_len = 0;
    uint16_t shard_count = 0;
    WireHeader end;

    EXPECT(codec != NULL && Codec_is_systematic(codec));
    memset(plaintext, 0x55, sizeof(plaintext));
    EXPECT(encode_block(codec, encoded, sizeof(encoded), plaintext,
                        sizeof(plaintext), &valid_len, &shard_count) == 0);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec(codec, 3, &ob);
    EXPECT(dec != NULL);

    /*
     * received_count == 4 (== data_shards) but missing data shard 1;
     * include pad shard 4 so a count-only check would wrongly pass.
     */
    EXPECT(ingest_shard(dec, 3, 0, 0, shard_count, valid_len, encoded) == 0);
    EXPECT(ingest_shard(dec, 3, 0, 2, shard_count, valid_len, encoded) == 0);
    EXPECT(ingest_shard(dec, 3, 0, 3, shard_count, valid_len, encoded) == 0);
    EXPECT(ingest_shard(dec, 3, 0, 4, shard_count, valid_len, encoded) == 0);
    fill_end(&end, 3, 1, shard_count);
    EXPECT(wire_flow_decoder_ingest(dec, &end, NULL, 0) == 0);

    EXPECT(!wire_flow_decoder_is_complete(dec));
    EXPECT(ob.len == 0);
    EXPECT(ob.writes == 0);
    EXPECT(wire_flow_decoder_stats(dec)->decoded_blocks == 0);

    wire_flow_decoder_destroy(dec);
}

static void test_systematic_out_of_order_blocks_stay_ordered(void)
{
    const Codec *codec = CopyCodec_get();
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoder *dec;
    unsigned char enc0[ENCODE_BLOCK];
    unsigned char enc1[ENCODE_BLOCK];
    uint8_t pt0[64];
    uint8_t pt1[64];
    uint8_t expect[128];
    uint16_t v0 = 0;
    uint16_t v1 = 0;
    uint16_t sc0 = 0;
    uint16_t sc1 = 0;
    WireHeader end;

    memset(pt0, 0x11, sizeof(pt0));
    memset(pt1, 0x22, sizeof(pt1));
    memcpy(expect, pt0, sizeof(pt0));
    memcpy(expect + sizeof(pt0), pt1, sizeof(pt1));
    EXPECT(encode_block(codec, enc0, sizeof(enc0), pt0, sizeof(pt0), &v0,
                        &sc0) == 0);
    EXPECT(encode_block(codec, enc1, sizeof(enc1), pt1, sizeof(pt1), &v1,
                        &sc1) == 0);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec(codec, 4, &ob);
    EXPECT(dec != NULL);

    /* Block 1 data-ready first (no pad), then block 0 data-ready. */
    EXPECT(ingest_data_shards_only(dec, 4, 1, enc1, sc1, v1, 4) == 0);
    EXPECT(ob.len == 0); /* must not emit block 1 early */
    EXPECT(ingest_data_shards_only(dec, 4, 0, enc0, sc0, v0, 4) == 0);

    fill_end(&end, 4, 2, sc0);
    EXPECT(wire_flow_decoder_ingest(dec, &end, NULL, 0) == 0);

    EXPECT(wire_flow_decoder_is_complete(dec));
    EXPECT(ob.len == sizeof(expect));
    EXPECT(memcmp(out, expect, sizeof(expect)) == 0);

    wire_flow_decoder_destroy(dec);
}

static void test_non_systematic_codec_unchanged(void)
{
    const Codec *codec = BlockCodec_get();
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoder *dec;
    unsigned char encoded[ENCODE_BLOCK];
    uint8_t plaintext[120];
    uint16_t valid_len = 0;
    uint16_t shard_count = 0;
    WireHeader end;
    size_t i;

    EXPECT(codec != NULL);
    EXPECT(!Codec_is_systematic(codec));
    EXPECT(Codec_data_shards(codec) == 4);
    EXPECT(Codec_parity_shards(codec) == 4);

    for (i = 0; i < sizeof(plaintext); i++) {
        plaintext[i] = (uint8_t)(i + 9u);
    }
    EXPECT(encode_block(codec, encoded, sizeof(encoded), plaintext,
                        sizeof(plaintext), &valid_len, &shard_count) == 0);

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    dec = make_dec(codec, 5, &ob);
    EXPECT(dec != NULL);

    /* Data shards only — must NOT fast-path (non-systematic). */
    EXPECT(ingest_data_shards_only(dec, 5, 0, encoded, shard_count, valid_len,
                                   4) == 0);
    fill_end(&end, 5, 1, shard_count);
    EXPECT(wire_flow_decoder_ingest(dec, &end, NULL, 0) == 0);

    EXPECT(!wire_flow_decoder_is_complete(dec));
    EXPECT(ob.len == 0);

    wire_flow_decoder_destroy(dec);
}

static void test_systematic_short_final_block_valid_len(void)
{
    const Codec *codec = CopyCodec_get();
    OutBuf ob;
    uint8_t out[8192];
    WireFlowDecoder *dec;
    unsigned char encoded[ENCODE_BLOCK];
    uint8_t plaintext[97]; /* short final block */
    uint16_t valid_len = 0;
    uint16_t shard_count = 0;
    WireHeader end;
    size_t i;

    for (i = 0; i < sizeof(plaintext); i++) {
        plaintext[i] = (uint8_t)(0x70 + (i % 17));
    }
    EXPECT(encode_block(codec, encoded, sizeof(encoded), plaintext,
                        sizeof(plaintext), &valid_len, &shard_count) == 0);
    EXPECT(valid_len == sizeof(plaintext));

    memset(&ob, 0, sizeof(ob));
    ob.data = out;
    ob.cap = sizeof(out);
    /* Poison remainder so pad leak would be visible. */
    memset(out, 0xee, sizeof(out));
    dec = make_dec(codec, 6, &ob);
    EXPECT(dec != NULL);

    EXPECT(ingest_data_shards_only(dec, 6, 0, encoded, shard_count, valid_len,
                                   4) == 0);
    fill_end(&end, 6, 1, shard_count);
    EXPECT(wire_flow_decoder_ingest(dec, &end, NULL, 0) == 0);

    EXPECT(wire_flow_decoder_is_complete(dec));
    EXPECT(ob.len == sizeof(plaintext));
    EXPECT(memcmp(out, plaintext, sizeof(plaintext)) == 0);

    wire_flow_decoder_destroy(dec);
}

int main(void)
{
    fprintf(stderr,
            "P1b codec map: copy sys=1 d=4 p=4; block sys=0 d=4 p=4; "
            "xor-fec sys=1 d=4 p=1; rs-fec sys=1 d=4 p=2; rs sys=1 d=k p=r\n");

    test_systematic_complete_without_nondata_shards_copy();
    test_systematic_complete_without_nondata_shards_xor();
    test_systematic_data_shard_missing_does_not_emit();
    test_systematic_out_of_order_blocks_stay_ordered();
    test_non_systematic_codec_unchanged();
    test_systematic_short_final_block_valid_len();

    if (g_failures != 0) {
        fprintf(stderr, "wire_flow_decoder_systematic_tests: %d failure(s)\n",
                g_failures);
        return EXIT_FAILURE;
    }
    fprintf(stderr, "wire_flow_decoder_systematic_tests: ok\n");
    return EXIT_SUCCESS;
}
