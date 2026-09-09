#include "recode.h"
#include "rs_bats_recoder.h"

#include "stream_config.h"

#include <stdlib.h>
#include <string.h>

static int payload_bounds_ok(size_t datagram_len, const WireHeader *hdr)
{
    if (hdr == NULL || datagram_len < WIRE_HEADER_SIZE) {
        return 0;
    }
    if (hdr->type != WIRE_TYPE_DATA) {
        return hdr->payload_len == 0u && datagram_len == WIRE_HEADER_SIZE;
    }
    return hdr->payload_len > 0u &&
           (size_t)hdr->payload_len == datagram_len - WIRE_HEADER_SIZE;
}

int relay_recode_identity(const uint8_t *in, size_t in_len,
                          uint8_t *out, size_t out_cap, size_t *out_len,
                          const WireHeader *hdr, void *ctx)
{
    (void)hdr;
    (void)ctx;

    if (in == NULL || out == NULL || out_len == NULL || in_len > out_cap) {
        return -1;
    }
    if (in_len > 0) {
        memcpy(out, in, in_len);
    }
    *out_len = in_len;
    return 0;
}

int relay_recode_payload_add1(const uint8_t *in, size_t in_len,
                              uint8_t *out, size_t out_cap, size_t *out_len,
                              const WireHeader *hdr, void *ctx)
{
    size_t byte;

    (void)ctx;
    if (in == NULL || out == NULL || out_len == NULL || in_len > out_cap ||
        !payload_bounds_ok(in_len, hdr)) {
        return -1;
    }
    memcpy(out, in, in_len);
    if (hdr->type == WIRE_TYPE_DATA) {
        for (byte = 0; byte < (size_t)hdr->payload_len; byte++) {
            out[WIRE_HEADER_SIZE + byte] =
                (uint8_t)(out[WIRE_HEADER_SIZE + byte] + 1u);
        }
    }
    *out_len = in_len;
    return 0;
}

int relay_egress_payload_sub1(uint8_t *datagram, size_t len,
                              const WireHeader *hdr, void *ctx)
{
    size_t byte;

    (void)ctx;
    if (datagram == NULL || !payload_bounds_ok(len, hdr)) {
        return -1;
    }
    if (hdr->type == WIRE_TYPE_DATA) {
        for (byte = 0; byte < (size_t)hdr->payload_len; byte++) {
            datagram[WIRE_HEADER_SIZE + byte] =
                (uint8_t)(datagram[WIRE_HEADER_SIZE + byte] - 1u);
        }
    }
    return 0;
}

const RsBatsRecoderStats *relay_rs_bats_recoder_stats(
    const RelayRsBatsRecoderCtx *ctx)
{
    if (ctx == NULL) {
        return NULL;
    }
    return &ctx->stats;
}

RelayRsBatsRecoderCtx *relay_rs_bats_recoder_ctx_create(void)
{
    RelayRsBatsRecoderCtx *ctx = calloc(1, sizeof(*ctx));

    return ctx;
}

void relay_rs_bats_recoder_ctx_destroy(RelayRsBatsRecoderCtx *ctx)
{
    free(ctx);
}

static int bats_already_emitted(const RelayRsBatsRecoderCtx *ctx,
                                uint32_t flow_id, uint64_t block_id)
{
    size_t i;

    for (i = 0; i < RS_BATS_EMITTED_RING; i++) {
        if (ctx->emitted[i].valid && ctx->emitted[i].flow_id == flow_id &&
            ctx->emitted[i].block_id == block_id) {
            return 1;
        }
    }
    return 0;
}

static void bats_mark_emitted(RelayRsBatsRecoderCtx *ctx, uint32_t flow_id,
                              uint64_t block_id)
{
    size_t idx = ctx->emitted_next % RS_BATS_EMITTED_RING;

    ctx->emitted[idx].flow_id = flow_id;
    ctx->emitted[idx].block_id = block_id;
    ctx->emitted[idx].valid = 1;
    ctx->emitted_next++;
}

RelayDecodeReencodeAction relay_rs_bats_identity_decode_reencode(
    const WireHeader *hdr,
    const uint8_t *datagram,
    size_t len,
    GenerationEntry *gen,
    GenerationInsertStatus insert_status,
    RelayDecodeReencodeEmitFn emit_fn,
    void *emit_ctx,
    void *ctx)
{
    RelayRsBatsRecoderCtx *bctx = ctx;
    RsBatsRecoderConfig cfg;
    uint8_t *row_blob = NULL;
    uint8_t **out_rows = NULL;
    uint16_t shard;
    uint16_t n;
    int rc;

    (void)datagram;
    (void)len;

    if (bctx == NULL || hdr == NULL || hdr->type != WIRE_TYPE_DATA) {
        return RELAY_DECODE_REENCODE_OPAQUE;
    }

    /* Cache admission failed: fall back to opaque forward. */
    if (insert_status == GEN_INSERT_MISMATCH ||
        insert_status == GEN_INSERT_ADMISSION_FAILED ||
        insert_status == GEN_INSERT_INVALID || gen == NULL) {
        return RELAY_DECODE_REENCODE_OPAQUE;
    }

    if (bats_already_emitted(bctx, gen->key.flow_id, gen->key.block_id)) {
        bctx->stats.hold_packets++;
        return RELAY_DECODE_REENCODE_HOLD;
    }

    if (gen->state != GEN_READY) {
        bctx->stats.hold_packets++;
        return RELAY_DECODE_REENCODE_HOLD;
    }

    /* GEN_READY: need emit_fn to replace the generation on the wire. */
    if (emit_fn == NULL) {
        return RELAY_DECODE_REENCODE_OPAQUE;
    }

    n = gen->shard_count;
    rs_bats_recoder_config_defaults(&cfg, n);
    cfg.payload_len = gen->payload_len;

    row_blob = malloc((size_t)n * (size_t)cfg.payload_len);
    out_rows = calloc(n, sizeof(*out_rows));
    if (row_blob == NULL || out_rows == NULL) {
        free(row_blob);
        free(out_rows);
        row_blob = NULL;
        out_rows = NULL;
        /* Fall through to verbatim cache emit below via rc != 0 path. */
        rc = -1;
    } else {
        for (shard = 0; shard < n; shard++) {
            out_rows[shard] = row_blob + (size_t)shard * cfg.payload_len;
        }
        rc = rs_bats_recode_generation_identity(&cfg, gen, out_rows,
                                                 &bctx->stats);
    }

    if (rc != 0) {
        /* Still replace the held generation: emit cached copies (identity). */
        for (shard = 0; shard < n; shard++) {
            const GenerationSlot *slot = &gen->slots[shard];

            if (!slot->present || slot->datagram_copy == NULL) {
                free(row_blob);
                free(out_rows);
                bctx->stats.hold_packets++;
                return RELAY_DECODE_REENCODE_HOLD;
            }
            if (emit_fn(slot->datagram_copy, slot->len, emit_ctx) != 0) {
                bctx->stats.emit_fail++;
                free(row_blob);
                free(out_rows);
                bctx->stats.hold_packets++;
                return RELAY_DECODE_REENCODE_HOLD;
            }
            bctx->stats.emit_datagrams++;
        }
        bats_mark_emitted(bctx, gen->key.flow_id, gen->key.block_id);
        free(row_blob);
        free(out_rows);
        return RELAY_DECODE_REENCODE_EMIT;
    }

    for (shard = 0; shard < n; shard++) {
        const GenerationSlot *slot = &gen->slots[shard];
        uint8_t *tmp;
        size_t dglen;

        if (!slot->present || slot->datagram_copy == NULL ||
            slot->len < WIRE_HEADER_SIZE + cfg.payload_len) {
            bctx->stats.emit_fail++;
            free(row_blob);
            free(out_rows);
            if (shard > 0) {
                bats_mark_emitted(bctx, gen->key.flow_id, gen->key.block_id);
                return RELAY_DECODE_REENCODE_EMIT;
            }
            bctx->stats.hold_packets++;
            return RELAY_DECODE_REENCODE_HOLD;
        }

        dglen = slot->len;
        tmp = malloc(dglen);
        if (tmp == NULL) {
            bctx->stats.emit_fail++;
            free(row_blob);
            free(out_rows);
            if (shard > 0) {
                bats_mark_emitted(bctx, gen->key.flow_id, gen->key.block_id);
                return RELAY_DECODE_REENCODE_EMIT;
            }
            bctx->stats.hold_packets++;
            return RELAY_DECODE_REENCODE_HOLD;
        }
        memcpy(tmp, slot->datagram_copy, dglen);
        memcpy(tmp + WIRE_HEADER_SIZE, out_rows[shard], cfg.payload_len);
        if (emit_fn(tmp, dglen, emit_ctx) != 0) {
            free(tmp);
            bctx->stats.emit_fail++;
            free(row_blob);
            free(out_rows);
            if (shard > 0) {
                bats_mark_emitted(bctx, gen->key.flow_id, gen->key.block_id);
                return RELAY_DECODE_REENCODE_EMIT;
            }
            bctx->stats.hold_packets++;
            return RELAY_DECODE_REENCODE_HOLD;
        }
        free(tmp);
        bctx->stats.emit_datagrams++;
    }

    bats_mark_emitted(bctx, gen->key.flow_id, gen->key.block_id);
    free(row_blob);
    free(out_rows);
    return RELAY_DECODE_REENCODE_EMIT;
}

RelayDecodeReencodeAction relay_decode_reencode_stub(
    const WireHeader *hdr,
    const uint8_t *datagram,
    size_t len,
    GenerationEntry *gen,
    GenerationInsertStatus insert_status,
    RelayDecodeReencodeEmitFn emit_fn,
    void *emit_ctx,
    void *ctx)
{
    (void)hdr;
    (void)datagram;
    (void)len;
    (void)gen;
    (void)insert_status;
    (void)emit_fn;
    (void)emit_ctx;
    (void)ctx;
    return RELAY_DECODE_REENCODE_OPAQUE;
}
