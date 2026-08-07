#ifndef WIRE_RELAY_LOCAL_DECODE_H
#define WIRE_RELAY_LOCAL_DECODE_H

#include "codec.h"
#include "relay.h"
#include "wire_flow_decoder.h"

#include <stdio.h>
#include <stdint.h>

typedef struct LocalDecodeHubStats {
    uint64_t delivered;
    uint64_t metadata_mismatch;
    uint64_t flow_rejected;
    uint64_t ingest_error;
} LocalDecodeHubStats;

typedef struct LocalDecodeHub {
    FILE                    *output; /* owned if opened by hub */
    int                      close_output;
    uint8_t                  local_node_id;
    const Codec             *codec;
    uint16_t                 expected_shards;
    size_t                   input_size;
    int                      best_effort;
    WireFlowDecoder         *dec;
    uint32_t                 bound_flow_id;
    int                      flow_bound;
    LocalDecodeHubStats      stats;
} LocalDecodeHub;

typedef struct LocalDecodeHubConfig {
    CodecKind   codec_kind;
    const char *output_path; /* required unless output FILE* provided */
    FILE       *output;      /* optional pre-opened FILE; hub does not fclose */
    int         best_effort; /* L1 default 0 = strict */
    uint8_t     local_node_id;
} LocalDecodeHubConfig;

int local_decode_hub_init(LocalDecodeHub *hub, const LocalDecodeHubConfig *cfg);
void local_decode_hub_destroy(LocalDecodeHub *hub);

/* RelayDeliveryFn-compatible adapter. */
int local_decode_hub_delivery(const uint8_t *datagram, size_t len,
                              const WireHeader *hdr, void *ctx);

const LocalDecodeHubStats *local_decode_hub_stats(const LocalDecodeHub *hub);
int local_decode_hub_is_complete(const LocalDecodeHub *hub);

/*
 * Strict L1 exit criteria (used by wire_relay main after relay_run):
 * ingest_error / metadata_mismatch / flow_rejected => fail;
 * flow_bound && !complete => fail.
 * Returns 0 on strict success, -1 on strict failure (do not use as a
 * boolean "ok" predicate — zero means success).
 */
int local_decode_hub_strict_check(const LocalDecodeHub *hub);

#endif /* WIRE_RELAY_LOCAL_DECODE_H */
