#include "recode.h"

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
    /* Phase 3A not implemented: never hold or replace. */
    return RELAY_DECODE_REENCODE_OPAQUE;
}
