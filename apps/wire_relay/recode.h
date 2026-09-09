#ifndef WIRE_RELAY_RECODE_H
#define WIRE_RELAY_RECODE_H

#include "generation_cache.h"
#include "rs_bats_recoder.h"
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

/* In-place transform after EgressQueue dequeue and before send/capture. */
typedef int (*RelayEgressFn)(uint8_t *datagram, size_t len,
                             const WireHeader *hdr, void *ctx);

/* Identity copy; available if a caller wants an explicit no-op hook. */
int relay_recode_identity(const uint8_t *in, size_t in_len,
                          uint8_t *out, size_t out_cap, size_t *out_len,
                          const WireHeader *hdr, void *ctx);

/* Case-c arithmetic: DATA payload +1 at ingress, then -1 at egress. */
int relay_recode_payload_add1(const uint8_t *in, size_t in_len,
                              uint8_t *out, size_t out_cap, size_t *out_len,
                              const WireHeader *hdr, void *ctx);
int relay_egress_payload_sub1(uint8_t *datagram, size_t len,
                              const WireHeader *hdr, void *ctx);

typedef enum {
    /* Keep current datagram on the opaque forward path. */
    RELAY_DECODE_REENCODE_OPAQUE = 0,
    /*
     * Suppress forwarding this packet (e.g. wait for a full generation).
     * Caller frees the live datagram; cache may still hold a copy.
     */
    RELAY_DECODE_REENCODE_HOLD = 1,
    /*
     * Replacement datagrams were passed to emit_fn. Suppress forwarding the
     * live datagram; caller enqueues emitted packets outside ingress_mu.
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
 * Phase 3A generation-level hook. Called (when non-NULL) after
 * GenerationCache insert on DATA, with gen possibly NULL on admission failure.
 *
 * When emit_fn is non-NULL, HOLD/EMIT are honored by the relay:
 *   HOLD — do not forward the live datagram
 *   EMIT — emit_fn copies were queued; do not forward the live datagram
 * Stub always returns OPAQUE: relay_decode_reencode_stub.
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

#define RS_BATS_EMITTED_RING 64u

typedef struct RelayRsBatsEmittedKey {
    uint32_t flow_id;
    uint64_t block_id;
    int      valid;
} RelayRsBatsEmittedKey;

typedef struct RelayRsBatsRecoderCtx {
    RsBatsRecoderStats    stats;
    RelayRsBatsEmittedKey emitted[RS_BATS_EMITTED_RING];
    size_t                emitted_next;
} RelayRsBatsRecoderCtx;

RelayRsBatsRecoderCtx *relay_rs_bats_recoder_ctx_create(void);
void relay_rs_bats_recoder_ctx_destroy(RelayRsBatsRecoderCtx *ctx);
const RsBatsRecoderStats *relay_rs_bats_recoder_stats(
    const RelayRsBatsRecoderCtx *ctx);

/*
 * Identity-matrix BATS via decode_reencode_fn:
 *   collecting → HOLD; GEN_READY → I×M + emit_fn × n → EMIT.
 */
RelayDecodeReencodeAction relay_rs_bats_identity_decode_reencode(
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
