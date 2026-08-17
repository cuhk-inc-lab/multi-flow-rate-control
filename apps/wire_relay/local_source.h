#ifndef WIRE_RELAY_LOCAL_SOURCE_H
#define WIRE_RELAY_LOCAL_SOURCE_H

#include "codec.h"

#include <stddef.h>
#include <stdint.h>

/*
 * Local file / FIFO → encode → wire datagrams → emit callback.
 *
 * Fits the unified node pipeline:
 *   local ingress → encode → fill final_dst/ttl → same egress as UDP forward
 *
 * The emit callback typically wraps relay_inject_wire_datagram so encoded
 * packets share Destination Check / TTL / transit hooks / EgressQueue.
 * input_path may be a regular file or a named FIFO.
 */

typedef int (*RelayWireEmitFn)(const uint8_t *datagram, size_t len, void *ctx);

typedef struct LocalSourceConfig {
    const char *input_path;
    CodecKind   codec_kind;
    uint32_t    flow_id;
    uint8_t     final_dst;
    uint8_t     ttl;
    /* 0 => unpaced; otherwise source/payload Mbps pacing between blocks. */
    double      source_rate_mbps;
} LocalSourceConfig;

typedef struct LocalSourceStats {
    uint64_t blocks;
    uint64_t source_bytes;
    uint64_t wire_datagrams;
    uint64_t emit_errors;
} LocalSourceStats;

/*
 * Synchronously encode the whole input and emit DATA shards + one END.
 * Returns 0 on success, -1 on error.
 */
int local_source_run(const LocalSourceConfig *config,
                     RelayWireEmitFn emit_fn, void *emit_ctx,
                     LocalSourceStats *stats_out);

#endif /* WIRE_RELAY_LOCAL_SOURCE_H */
