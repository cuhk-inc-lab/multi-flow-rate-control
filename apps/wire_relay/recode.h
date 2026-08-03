#ifndef WIRE_RELAY_RECODE_H
#define WIRE_RELAY_RECODE_H

#include "wire_header.h"

#include <stddef.h>
#include <stdint.h>

/*
 * Optional recode hook. Disabled by default (RelayConfig.recode_fn == NULL).
 * When set, must not alter opaque payload bytes in phase-1 identity mode.
 */
typedef int (*RelayRecodeFn)(const uint8_t *in, size_t in_len,
                             uint8_t *out, size_t out_cap, size_t *out_len,
                             const WireHeader *hdr, void *ctx);

/* Identity copy; available if a caller wants an explicit no-op hook. */
int relay_recode_identity(const uint8_t *in, size_t in_len,
                          uint8_t *out, size_t out_cap, size_t *out_len,
                          const WireHeader *hdr, void *ctx);

#endif /* WIRE_RELAY_RECODE_H */
