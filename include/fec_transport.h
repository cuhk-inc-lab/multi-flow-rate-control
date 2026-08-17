#ifndef FEC_TRANSPORT_H
#define FEC_TRANSPORT_H

/*
 * Non-blocking FEC transport library for UDP relay.
 *
 * Independent of sockets, KCP/SCP, files, FIFOs, CircularBuffer, and the
 * wg_multi_pipeline / wire_relay binaries.  Callers own UDP sockets and
 * periodically pass now_ns into update() / drain().
 *
 * This library does not modify the existing encode/forward/decode pipeline.
 */

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FEC_TRANSPORT_MAX_SHARDS 255u

typedef enum {
    FEC_OK = 0,
    FEC_ERR_INVAL = 1,
    FEC_ERR_NOMEM = 2,
    FEC_ERR_QUEUE_FULL = 3,
    FEC_ERR_WIRE_HEADER = 4,
    FEC_ERR_METADATA = 5,
    FEC_ERR_CODEC = 6,
    FEC_ERR_DOWNSTREAM = 7,
    FEC_ERR_STALE = 8,
    FEC_ERR_NOT_FEC = 9,
    FEC_ERR_EXHAUSTED = 10
} FecStatus;

typedef enum {
    FEC_OUTPUT_OK = 0,
    FEC_OUTPUT_BLOCKED = 1,
    FEC_OUTPUT_ERROR = 2
} FecOutputStatus;

typedef enum {
    FEC_CODEC_RS = 0
} FecCodecKind;

typedef FecOutputStatus (*FecOutputFn)(void *ctx,
                                       const uint8_t *data,
                                       size_t length);

typedef struct {
    FecOutputFn output;
    void *ctx;
} FecCallbacks;

typedef struct {
    FecCodecKind codec;
    uint16_t data_shards;
    uint16_t parity_shards;
    uint16_t shard_size;
    uint64_t flush_timeout_ns;
    size_t group_window;
    size_t output_queue_packets;
    size_t output_queue_bytes;
    uint64_t wire_rate_bps;
    uint64_t wire_burst_bytes;
} FecTransportConfig;

typedef struct {
    uint64_t data_datagrams_tx;
    uint64_t parity_datagrams_tx;
    uint64_t metadata_datagrams_tx;
    uint64_t received_datagrams;
    uint64_t duplicate_datagrams;
    uint64_t stale_datagrams;
    uint64_t invalid_datagrams;
    uint64_t completed_groups;
    uint64_t recovered_groups;
    uint64_t recovered_shards;
    uint64_t unrecoverable_groups;
    uint64_t expired_groups;
    uint64_t evicted_groups;
    uint64_t missing_shards;
    uint64_t over_r_groups;
    uint64_t max_missing_run;
    uint64_t output_queue_packets;
    uint64_t output_queue_bytes;
    uint64_t blocked_count;
    uint64_t queue_overflow_count;
    uint64_t wire_pacing_deferred;
    uint64_t downstream_callback_errors;
} FecStats;

typedef struct FecEncoder FecEncoder;
typedef struct FecDecoder FecDecoder;

void fec_transport_config_init(FecTransportConfig *config);

FecEncoder *fec_encoder_create(const FecTransportConfig *config,
                               const FecCallbacks *callbacks);
void fec_encoder_destroy(FecEncoder *encoder);
FecStatus fec_encoder_reset(FecEncoder *encoder, uint32_t epoch);
FecStatus fec_encoder_push(FecEncoder *encoder,
                           const void *data,
                           size_t length,
                           uint64_t now_ns);
FecStatus fec_encoder_flush(FecEncoder *encoder);
FecStatus fec_encoder_update(FecEncoder *encoder, uint64_t now_ns);
FecStatus fec_encoder_drain(FecEncoder *encoder, size_t budget);
int fec_encoder_has_pending(const FecEncoder *encoder);
uint64_t fec_encoder_next_update_ns(const FecEncoder *encoder);
void fec_encoder_get_stats(const FecEncoder *encoder, FecStats *stats);

FecDecoder *fec_decoder_create(const FecTransportConfig *config,
                               const FecCallbacks *callbacks);
void fec_decoder_destroy(FecDecoder *decoder);
FecStatus fec_decoder_reset(FecDecoder *decoder, uint32_t epoch);
FecStatus fec_decoder_input(FecDecoder *decoder,
                            const void *datagram,
                            size_t length,
                            uint64_t now_ns);
FecStatus fec_decoder_update(FecDecoder *decoder, uint64_t now_ns);
void fec_decoder_get_stats(const FecDecoder *decoder, FecStats *stats);

#ifdef __cplusplus
}
#endif

#endif /* FEC_TRANSPORT_H */
