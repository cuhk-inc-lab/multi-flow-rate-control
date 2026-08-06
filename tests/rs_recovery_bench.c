#include "codec.h"
#include "rs_codec.h"
#include "stream_config.h"

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#define BENCH_ITERATIONS 1000u

static uint64_t monotonic_nanoseconds(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void print_result(const char *label, uint64_t elapsed_ns)
{
    double us_per_block = (double)elapsed_ns / 1000.0 / BENCH_ITERATIONS;
    double source_mbps = us_per_block > 0.0 ?
        (double)DECODE_BLOCK * 8.0 / us_per_block : 0.0;

    printf("%s: %.3f us/block, %.2f source Mbps\n",
           label, us_per_block, source_mbps);
}

static int benchmark_recovery(RsRecoverMode mode, uint16_t present_mask,
                              const char *label)
{
    unsigned char encoded[RS_FEC_ENCODE_BLOCK];
    unsigned char work[RS_FEC_ENCODE_BLOCK];
    uint8_t present_bits[codec_present_bytes(RS_FEC_TOTAL_SHARDS)];
    size_t byte;
    size_t shard;
    unsigned iteration;
    uint64_t started;
    uint64_t finished;

    for (byte = 0; byte < DECODE_BLOCK; byte++) {
        encoded[byte] = (unsigned char)(byte * 37u + 9u);
    }
    if (RsCodec_set_recover_mode(mode) != 0) {
        return -1;
    }
    Codec_encode(RsCodec_get(), encoded, sizeof(encoded));
    codec_present_from_u64(present_bits, RS_FEC_TOTAL_SHARDS, present_mask);

    started = monotonic_nanoseconds();
    for (iteration = 0; iteration < BENCH_ITERATIONS; iteration++) {
        memcpy(work, encoded, sizeof(work));
        for (shard = 0; shard < RS_FEC_TOTAL_SHARDS; shard++) {
            if (!codec_present_get(present_bits, shard)) {
                memset(work + shard * PKG_SIZE, 0, PKG_SIZE);
            }
        }
        if (Codec_recover(RsCodec_get(), work, present_bits,
                          RS_FEC_TOTAL_SHARDS) != CODEC_RECOVER_OK) {
            return -1;
        }
    }
    finished = monotonic_nanoseconds();
    if (finished < started) {
        return -1;
    }
    print_result(label, finished - started);
    return 0;
}

int main(void)
{
    uint64_t started = monotonic_nanoseconds();
    uint64_t finished;

    if (RsCodec_prepare_matrix() != 0) {
        fputs("matrix initialization failed\n", stderr);
        return 1;
    }
    finished = monotonic_nanoseconds();
    printf("matrix init: %.3f us (reported %.3f us)\n",
           (double)(finished - started) / 1000.0,
           (double)RsCodec_matrix_init_ns() / 1000.0);

    if (benchmark_recovery(RS_RECOVER_LEGACY, 0x3bu,
                           "legacy recover (1 loss)") != 0 ||
        benchmark_recovery(RS_RECOVER_LEGACY, 0x2du,
                           "legacy recover (2 losses)") != 0 ||
        benchmark_recovery(RS_RECOVER_MATRIX, 0x3bu,
                           "matrix recover (1 loss)") != 0 ||
        benchmark_recovery(RS_RECOVER_MATRIX, 0x2du,
                           "matrix recover (2 losses)") != 0 ||
        benchmark_recovery(RS_RECOVER_MATRIX, 0x3fu,
                           "matrix recover (no loss)") != 0) {
        fputs("recovery benchmark failed\n", stderr);
        return 1;
    }

    (void)RsCodec_set_recover_mode(RS_RECOVER_MATRIX);
    return 0;
}
