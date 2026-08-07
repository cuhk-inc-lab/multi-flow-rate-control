#ifndef WIRE_RELAY_LOCAL_DECODE_H
#define WIRE_RELAY_LOCAL_DECODE_H

#include "codec.h"
#include "relay.h"
#include "wire_flow_decoder.h"

#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdint.h>

#ifndef LOCAL_DECODE_PATH_MAX
#ifdef PATH_MAX
#define LOCAL_DECODE_PATH_MAX PATH_MAX
#else
#define LOCAL_DECODE_PATH_MAX 4096
#endif
#endif

typedef enum LocalDecodeMode {
    LOCAL_DECODE_MODE_SINGLE_FILE = 1, /* L1: --output FILE */
    LOCAL_DECODE_MODE_OUTPUT_DIR = 2   /* L2: --output-dir DIR */
} LocalDecodeMode;

typedef struct LocalDecodeHubStats {
    uint64_t delivered;
    uint64_t metadata_mismatch;
    uint64_t flow_rejected;
    uint64_t ingest_error;
} LocalDecodeHubStats;

typedef struct LocalDecodeFlow {
    int              active;
    uint32_t         flow_id;
    FILE            *output;
    int              close_output; /* 1 => hub owns FILE* */
    WireFlowDecoder *dec;
    uint64_t         delivered;
    uint64_t         metadata_mismatch;
    uint64_t         ingest_error;
} LocalDecodeFlow;

typedef struct LocalDecodeHub {
    uint8_t             local_node_id;
    const Codec        *codec;
    uint16_t            expected_shards;
    size_t              input_size;
    int                 best_effort;

    LocalDecodeMode     mode;
    char                output_dir[LOCAL_DECODE_PATH_MAX];
    FILE               *single_output; /* L1 file; shared by the sole flow */
    int                 single_close_output;

    LocalDecodeFlow     flows[RELAY_MAX_FLOWS];
    LocalDecodeHubStats stats;

    /*
     * Serializes all delivery / ingest / stats mutation. delivery_fn runs
     * outside RelayCtx.ingress_mu (P0A); this mutex replaces that serialization
     * for the hub. Do not take ingress_mu while holding mu.
     */
    pthread_mutex_t     mu;
    int                 mu_inited;
} LocalDecodeHub;

typedef struct LocalDecodeHubConfig {
    CodecKind        codec_kind;
    LocalDecodeMode  mode;
    const char      *output_path; /* SINGLE_FILE path (unless output set) */
    const char      *output_dir;  /* OUTPUT_DIR path; must already exist */
    FILE            *output;      /* optional pre-opened SINGLE_FILE; not owned */
    int              best_effort; /* default 0 = strict */
    uint8_t          local_node_id;
} LocalDecodeHubConfig;

int local_decode_hub_init(LocalDecodeHub *hub, const LocalDecodeHubConfig *cfg);

/*
 * Destroy hub. Caller must ensure no concurrent local_decode_hub_delivery
 * (e.g. only after relay_run / harness close returns). Not safe vs active
 * delivery.
 */
void local_decode_hub_destroy(LocalDecodeHub *hub);

/*
 * RelayDeliveryFn-compatible adapter. Thread-safe vs other deliveries on the
 * same hub via hub->mu. Must not be called while holding RelayCtx.ingress_mu
 * with intent to re-lock ingress under hub->mu (lock order: ingress unlock
 * then hub->mu).
 */
int local_decode_hub_delivery(const uint8_t *datagram, size_t len,
                              const WireHeader *hdr, void *ctx);

/* Copy hub stats under hub->mu. Returns 0 on success, -1 on error. */
int local_decode_hub_get_stats(LocalDecodeHub *hub, LocalDecodeHubStats *out);

size_t local_decode_hub_active_count(LocalDecodeHub *hub);

/* 1 iff at least one active flow and every active flow is complete. */
int local_decode_hub_is_complete(LocalDecodeHub *hub);

/*
 * Strict exit criteria (used by wire_relay main after relay_run):
 * hub ingest_error / metadata_mismatch / flow_rejected => fail;
 * any active flow ingest_error => fail;
 * any active flow incomplete => fail;
 * zero active flows (idle) => success.
 * Returns 0 on strict success, -1 on strict failure (zero means success).
 */
int local_decode_hub_strict_check(LocalDecodeHub *hub);

/*
 * Optional weak test hooks around hub->mu critical section (after lock /
 * before unlock). Production provides empty weak stubs.
 */
void local_decode_test_locked_enter(void);
void local_decode_test_locked_leave(void);

#endif /* WIRE_RELAY_LOCAL_DECODE_H */
