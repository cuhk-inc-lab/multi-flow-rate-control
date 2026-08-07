#ifndef WIRE_RELAY_PROCESS_H
#define WIRE_RELAY_PROCESS_H

#include "generation_cache.h"
#include "wire_header.h"

#include <stddef.h>
#include <stdint.h>

typedef enum {
    RELAY_PROCESS_FORWARD = 0, /* Phase-1 path: no GenerationCache */
    RELAY_PROCESS_CACHE = 1    /* observe/store + still opaque forward */
} RelayProcessMode;

typedef enum {
    RELAY_PROCESS_CONTINUE_FORWARD = 0,
    RELAY_PROCESS_DROP = 1
} RelayProcessAction;

/*
 * Optional hook under ingress_mu after cache admit attempt (CACHE mode).
 * gen may be NULL on admission failure. insert_status reports cache result.
 * Default (NULL): always CONTINUE_FORWARD (opaque).
 */
typedef RelayProcessAction (*RelayProcessFn)(
    const WireHeader *hdr,
    const uint8_t *datagram,
    size_t len,
    GenerationEntry *gen,
    GenerationInsertStatus insert_status,
    void *ctx);

#endif /* WIRE_RELAY_PROCESS_H */
