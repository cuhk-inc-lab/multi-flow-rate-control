#ifndef WIRE_RELAY_RECODE_H
#define WIRE_RELAY_RECODE_H

#include "generation_cache.h"
#include "wire_header.h"

#include <stddef.h>
#include <stdint.h>

/*
 * Mid-hop transit hooks (non-local path after TTL--).
 *
 * Pipeline position:
 *   UDP in → ttl==0? drop → local? decode : TTL-- → [here] → egress
 *
 * Today neither hook performs real FEC work:
 *   - recode_fn: optional per-datagram transform (NULL = opaque forward)
 *   - decode_reencode_fn: reserved for Phase 3A generation-level
 *     decode-and-reencode (NULL = never invoked; stub returns OPAQUE)
 *
 * Phase 3B true network recode needs a future wire version with coding
 * vectors; do not overload these hooks for that.
 */

typedef int (*RelayRecodeFn)(const uint8_t *in, size_t in_len,
                             uint8_t *out, size_t out_cap, size_t *out_len,
                             const WireHeader *hdr, void *ctx);

/* Identity copy; available if a caller wants an explicit no-op hook. */
int relay_recode_identity(const uint8_t *in, size_t in_len,
                          uint8_t *out, size_t out_cap, size_t *out_len,
                          const WireHeader *hdr, void *ctx);

typedef enum {
    /* Keep current datagram on the opaque forward path. */
    RELAY_DECODE_REENCODE_OPAQUE = 0,
    /*
     * Future: suppress forwarding this packet and wait for a replacement
     * generation emit. Not implemented — treated as OPAQUE today.
     */
    RELAY_DECODE_REENCODE_HOLD = 1,
    /*
     * Future: replacement datagrams were emitted via the emit callback.
     * Not implemented — treated as OPAQUE today.
     */
    RELAY_DECODE_REENCODE_EMIT = 2
} RelayDecodeReencodeAction;

/*
 * Emit one already-encoded wire datagram produced by a future 3A transform.
 * Must be a complete wire v3 UDP payload. Return 0 on success.
 */
typedef int (*RelayDecodeReencodeEmitFn)(const uint8_t *datagram, size_t len,
                                         void *emit_ctx);

/*
 * Reserved Phase 3A hook. Called (when non-NULL) after GenerationCache
 * insert on DATA, with gen possibly NULL on admission failure.
 *
 * Contract today: implementations must return RELAY_DECODE_REENCODE_OPAQUE
 * and must not rely on emit_fn. Stub: relay_decode_reencode_stub.
 */
typedef RelayDecodeReencodeAction (*RelayDecodeReencodeFn)(
    const WireHeader *hdr,
    const uint8_t *datagram,
    size_t len,
    GenerationEntry *gen,
    GenerationInsertStatus insert_status,
    RelayDecodeReencodeEmitFn emit_fn,
    void *emit_ctx,
    void *ctx);

/* Always returns OPAQUE; reserved placeholder for Phase 3A wiring. */
RelayDecodeReencodeAction relay_decode_reencode_stub(
    const WireHeader *hdr,
    const uint8_t *datagram,
    size_t len,
    GenerationEntry *gen,
    GenerationInsertStatus insert_status,
    RelayDecodeReencodeEmitFn emit_fn,
    void *emit_ctx,
    void *ctx);

#endif /* WIRE_RELAY_RECODE_H */
