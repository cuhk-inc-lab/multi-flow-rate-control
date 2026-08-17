#include "recode.h"

#include <string.h>

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
