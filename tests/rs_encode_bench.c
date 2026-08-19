#include "codec.h"
#include "rs_codec.h"
#include "stream_config.h"

#include <pthread.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/*
 * RS encode throughput microbench.
 *
 * Project CLI geometry:
 *   RS(16,2) means --rs-k=16 --rs-parity=2
 *   → k=16 data shards, r=2 parity, n=18 total.
 * Classical RS(n,k) name for the same code is RS(18,16).
 */

#define DEFAULT_BLOCKS 2000u
#define DEFAULT_TRIALS 3u
#define MAX_TRIALS 8u

typedef struct BenchConfig {
    size_t k;
    size_t r;
    unsigned blocks;
    unsigned flows;
    unsigned trials;
    RsEncodeImpl impl;
    const char *impl_name;
} BenchConfig;

typedef struct FlowArg {
    unsigned char *base;
    size_t         block_bytes;
    unsigned       block_count;
} FlowArg;

static uint64_t monotonic_nanoseconds(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void fill_prefix(unsigned char *block, size_t input_bytes, unsigned seed)
{
    size_t i;
    unsigned state = seed;

    for (i = 0; i < input_bytes; i++) {
        state = state * 1103515245u + 12345u;
        block[i] = (unsigned char)(state >> 16);
    }
}

static uint64_t g_parity_sink;

static void *flow_worker(void *arg)
{
    FlowArg *flow = arg;
    unsigned i;
    uint64_t local = 0;

    for (i = 0; i < flow->block_count; i++) {
        unsigned char *block = flow->base + (size_t)i * flow->block_bytes;

        Codec_encode(RsCodec_get(), block, flow->block_bytes);
        local ^= block[flow->block_bytes - 1u];
        local = (local << 1) ^ block[flow->block_bytes / 2u];
    }
    __atomic_fetch_xor(&g_parity_sink, local, __ATOMIC_RELAXED);
    return NULL;
}

static void sort_doubles(double *values, unsigned n)
{
    unsigned i;
    unsigned j;

    for (i = 0; i < n; i++) {
        for (j = i + 1; j < n; j++) {
            if (values[j] < values[i]) {
                double tmp = values[i];

                values[i] = values[j];
                values[j] = tmp;
            }
        }
    }
}

static int run_trial(const BenchConfig *cfg, double *throughput_mbps_out,
                     double *elapsed_ms_out, RsEncodeStats *stats_out)
{
    size_t n = cfg->k + cfg->r;
    size_t input_bytes = cfg->k * PKG_SIZE;
    size_t block_bytes = n * PKG_SIZE;
    size_t total_blocks = (size_t)cfg->blocks * (size_t)cfg->flows;
    unsigned char *arena = NULL;
    pthread_t *threads = NULL;
    FlowArg *flows = NULL;
    unsigned f;
    uint64_t started;
    uint64_t finished;
    int rc = -1;

    if (RsCodec_set_params(cfg->k, cfg->r) != 0) {
        fputs("set_params failed\n", stderr);
        return -1;
    }
    RsCodec_set_encode_impl(cfg->impl);
    RsCodec_reset_encode_stats();

    arena = calloc(total_blocks, block_bytes);
    threads = calloc(cfg->flows, sizeof(*threads));
    flows = calloc(cfg->flows, sizeof(*flows));
    if (arena == NULL || threads == NULL || flows == NULL) {
        goto out;
    }

    for (f = 0; f < cfg->flows; f++) {
        unsigned b;

        flows[f].base = arena + (size_t)f * (size_t)cfg->blocks * block_bytes;
        flows[f].block_bytes = block_bytes;
        flows[f].block_count = cfg->blocks;
        for (b = 0; b < cfg->blocks; b++) {
            fill_prefix(flows[f].base + (size_t)b * block_bytes, input_bytes,
                        0xA500u + f * 97u + b);
        }
    }

    started = monotonic_nanoseconds();
    if (cfg->flows == 1u) {
        unsigned b;
        uint64_t local = 0;

        for (b = 0; b < cfg->blocks; b++) {
            unsigned char *block = arena + (size_t)b * block_bytes;

            Codec_encode(RsCodec_get(), block, block_bytes);
            local ^= block[block_bytes - 1u];
            local = (local << 1) ^ block[block_bytes / 2u];
        }
        g_parity_sink ^= local;
    } else {
        for (f = 0; f < cfg->flows; f++) {
            if (pthread_create(&threads[f], NULL, flow_worker, &flows[f]) != 0) {
                goto out;
            }
        }
        for (f = 0; f < cfg->flows; f++) {
            pthread_join(threads[f], NULL);
        }
    }
    finished = monotonic_nanoseconds();
    if (finished < started || finished == 0 || started == 0) {
        goto out;
    }

    *elapsed_ms_out = (double)(finished - started) / 1.0e6;
    *throughput_mbps_out =
        *elapsed_ms_out > 0.0
            ? ((double)total_blocks * (double)input_bytes / 1.0e6) /
                  (*elapsed_ms_out / 1.0e3)
            : 0.0;
    RsCodec_get_encode_stats(stats_out);
    rc = 0;
out:
    free(arena);
    free(threads);
    free(flows);
    return rc;
}

int main(int argc, char **argv)
{
    BenchConfig cfg = {
        .k = 16,
        .r = 2,
        .blocks = DEFAULT_BLOCKS,
        .flows = 1,
        .trials = DEFAULT_TRIALS,
        .impl = RS_ENCODE_GENERAL,
        .impl_name = "general",
    };
    int run_suite = 0;
    double throughputs[MAX_TRIALS];
    double elapsed[MAX_TRIALS];
    RsEncodeStats stats[MAX_TRIALS];
    unsigned t;
    size_t n;
    size_t input_bytes;
    size_t parity_bytes;
    double thr_sorted[MAX_TRIALS];
    double el_sorted[MAX_TRIALS];
    unsigned mid;

    for (int i = 1; i < argc; i++) {
        if (strncmp(argv[i], "--k=", 4) == 0) {
            cfg.k = (size_t)strtoul(argv[i] + 4, NULL, 10);
        } else if (strncmp(argv[i], "--r=", 4) == 0) {
            cfg.r = (size_t)strtoul(argv[i] + 4, NULL, 10);
        } else if (strncmp(argv[i], "--blocks=", 9) == 0) {
            cfg.blocks = (unsigned)strtoul(argv[i] + 9, NULL, 10);
        } else if (strncmp(argv[i], "--flows=", 8) == 0) {
            cfg.flows = (unsigned)strtoul(argv[i] + 8, NULL, 10);
        } else if (strncmp(argv[i], "--trials=", 9) == 0) {
            cfg.trials = (unsigned)strtoul(argv[i] + 9, NULL, 10);
        } else if (strcmp(argv[i], "--impl=general") == 0) {
            cfg.impl = RS_ENCODE_GENERAL;
            cfg.impl_name = "general";
        } else if (strcmp(argv[i], "--impl=scalar") == 0) {
            cfg.impl = RS_ENCODE_GENERAL;
            cfg.impl_name = "scalar";
        } else if (strcmp(argv[i], "--impl=simd") == 0) {
            cfg.impl = RS_ENCODE_SIMD;
            cfg.impl_name = "simd";
        } else if (strcmp(argv[i], "--impl=legacy") == 0) {
            cfg.impl = RS_ENCODE_LEGACY;
            cfg.impl_name = "legacy";
        } else if (strcmp(argv[i], "--impl=rscode") == 0) {
            cfg.impl = RS_ENCODE_RSCODE;
            cfg.impl_name = "rscode";
        } else if (strcmp(argv[i], "--impl=fast") == 0) {
            cfg.impl = RS_ENCODE_FAST_16_2;
            cfg.impl_name = "fast";
        } else if (strcmp(argv[i], "--impl=auto") == 0) {
            cfg.impl = RS_ENCODE_AUTO;
            cfg.impl_name = "auto";
        } else if (strcmp(argv[i], "--suite") == 0) {
            run_suite = 1;
        } else {
            fprintf(stderr,
                    "usage: %s [--k=16] [--r=2] [--blocks=N] [--flows=N] "
                    "[--trials=N] "
                    "[--impl=legacy|rscode|general|scalar|simd|fast|auto] "
                    "[--suite]\n"
                    "metrics: source_MB/s = aggregate across all flows "
                    "(total k*PKG_SIZE*blocks*flows / wall-clock);\n"
                    "  ns/block_* = encode_stats / encode_calls "
                    "(per Codec_encode / per block average).\n"
                    "  parity_sink is anti-DCE only; bit-exact uses memcmp "
                    "tests.\n",
                    argv[0]);
            return 1;
        }
    }

    if (cfg.trials == 0 || cfg.trials > MAX_TRIALS || cfg.blocks == 0 ||
        cfg.flows == 0) {
        fputs("invalid trials/blocks/flows\n", stderr);
        return 1;
    }

    if (run_suite) {
        struct {
            size_t k;
            size_t r;
            RsEncodeImpl impl;
            const char *name;
            unsigned flows;
        } cases[] = {
            {4, 2, RS_ENCODE_LEGACY, "legacy", 1},
            {4, 2, RS_ENCODE_RSCODE, "rscode", 1},
            {4, 2, RS_ENCODE_GENERAL, "general", 1},
            {4, 2, RS_ENCODE_AUTO, "auto", 1},
            {4, 2, RS_ENCODE_AUTO, "auto", 8},
            {4, 2, RS_ENCODE_GENERAL, "general", 8},
            {8, 2, RS_ENCODE_LEGACY, "legacy", 1},
            {8, 2, RS_ENCODE_GENERAL, "general", 1},
            {8, 2, RS_ENCODE_GENERAL, "general", 8},
            {16, 2, RS_ENCODE_LEGACY, "legacy", 1},
            {16, 2, RS_ENCODE_GENERAL, "general", 1},
            {16, 2, RS_ENCODE_FAST_16_2, "fast", 1},
            {16, 2, RS_ENCODE_GENERAL, "general", 8},
            {16, 2, RS_ENCODE_FAST_16_2, "fast", 8},
            {8, 4, RS_ENCODE_LEGACY, "legacy", 1},
            {8, 4, RS_ENCODE_GENERAL, "general", 1},
            {8, 4, RS_ENCODE_GENERAL, "general", 8},
            {16, 6, RS_ENCODE_GENERAL, "scalar", 1},
            {16, 6, RS_ENCODE_SIMD, "simd", 1},
            {16, 6, RS_ENCODE_AUTO, "auto", 1},
            {16, 6, RS_ENCODE_SIMD, "simd", 8},
        };
        size_t ci;

        printf("suite=1\n");
        printf("avx2_available=%d\n", RsCodec_avx2_available());
        printf("ssse3_available=%d\n", RsCodec_ssse3_available());
        printf("simd_available=%d\n", RsCodec_simd_available());
        printf("metric_notes=source_MB/s is aggregate across all flows "
               "(total input bytes / wall-clock); "
               "ns_per_block_* is mean over encode_calls "
               "(one call = one block); "
               "parity_sink is anti-DCE only\n");
        for (ci = 0; ci < sizeof(cases) / sizeof(cases[0]); ci++) {
            BenchConfig one = cfg;

            one.k = cases[ci].k;
            one.r = cases[ci].r;
            one.impl = cases[ci].impl;
            one.impl_name = cases[ci].name;
            one.flows = cases[ci].flows;
            if (one.flows > 1u && one.blocks > 500u) {
                one.blocks = 500u;
            }
            n = one.k + one.r;
            input_bytes = one.k * PKG_SIZE;
            parity_bytes = one.r * PKG_SIZE;
            printf("---\ngeometry=k=%zu,r=%zu,n=%zu\nimplementation=%s\n"
                   "flows=%u\n",
                   one.k, one.r, n, one.impl_name, one.flows);
            for (t = 0; t < one.trials; t++) {
                if (run_trial(&one, &throughputs[t], &elapsed[t], &stats[t]) !=
                    0) {
                    fputs("trial failed\n", stderr);
                    return 1;
                }
            }
            memcpy(thr_sorted, throughputs, sizeof(double) * one.trials);
            memcpy(el_sorted, elapsed, sizeof(double) * one.trials);
            sort_doubles(thr_sorted, one.trials);
            sort_doubles(el_sorted, one.trials);
            mid = one.trials / 2u;
            printf("median_throughput_MBps=%.2f\n", thr_sorted[mid]);
            printf("median_elapsed_ms=%.3f\n", el_sorted[mid]);
            printf("ns_per_block_body=%.1f\n",
                   stats[mid].encode_calls
                       ? (double)stats[mid].encode_body_ns /
                             (double)stats[mid].encode_calls
                       : 0.0);
            printf("ns_per_block_lock_wait=%.1f\n",
                   stats[mid].encode_calls
                       ? (double)stats[mid].lock_wait_ns /
                             (double)stats[mid].encode_calls
                       : 0.0);
            printf("ns_per_block_lock_hold=%.1f\n",
                   stats[mid].encode_calls
                       ? (double)stats[mid].lock_hold_ns /
                             (double)stats[mid].encode_calls
                       : 0.0);
            printf("matrix_rebuilds_per_encode=%.6f\n",
                   stats[mid].encode_calls
                       ? (double)stats[mid].rebuild_calls /
                             (double)stats[mid].encode_calls
                       : 0.0);
            printf("parity_bytes_per_sec=%.0f\n",
                   thr_sorted[mid] * 1.0e6 * (double)parity_bytes /
                       (double)input_bytes);
            printf("parity_sink=%llu\n",
                   (unsigned long long)g_parity_sink);
            g_parity_sink = 0;
        }
        return 0;
    }

    n = cfg.k + cfg.r;
    input_bytes = cfg.k * PKG_SIZE;
    parity_bytes = cfg.r * PKG_SIZE;

    printf("geometry=k=%zu,r=%zu,n=%zu\n", cfg.k, cfg.r, n);
    printf("implementation=%s\n", cfg.impl_name);
    printf("avx2_available=%d\n", RsCodec_avx2_available());
    printf("ssse3_available=%d\n", RsCodec_ssse3_available());
    printf("simd_available=%d\n", RsCodec_simd_available());
    printf("shard_bytes=%u\n", (unsigned)PKG_SIZE);
    printf("blocks=%u\n", cfg.blocks);
    printf("flows=%u\n", cfg.flows);
    printf("trials=%u\n", cfg.trials);

    for (t = 0; t < cfg.trials; t++) {
        if (run_trial(&cfg, &throughputs[t], &elapsed[t], &stats[t]) != 0) {
            fputs("trial failed\n", stderr);
            return 1;
        }
        printf("trial[%u]: elapsed_ms=%.3f throughput_MBps=%.2f "
               "encode_calls=%llu rebuild_calls=%llu "
               "lock_wait_ns=%llu lock_hold_ns=%llu body_ns=%llu "
               "general_calls=%llu simd_calls=%llu fast_calls=%llu "
               "legacy_calls=%llu "
               "last_impl=%d\n",
               t, elapsed[t], throughputs[t],
               (unsigned long long)stats[t].encode_calls,
               (unsigned long long)stats[t].rebuild_calls,
               (unsigned long long)stats[t].lock_wait_ns,
               (unsigned long long)stats[t].lock_hold_ns,
               (unsigned long long)stats[t].encode_body_ns,
               (unsigned long long)stats[t].general_path_calls,
               (unsigned long long)stats[t].simd_path_calls,
               (unsigned long long)stats[t].fast_path_calls,
               (unsigned long long)stats[t].legacy_path_calls,
               (int)RsCodec_last_encode_impl());
    }

    memcpy(thr_sorted, throughputs, sizeof(double) * cfg.trials);
    memcpy(el_sorted, elapsed, sizeof(double) * cfg.trials);
    sort_doubles(thr_sorted, cfg.trials);
    sort_doubles(el_sorted, cfg.trials);
    mid = cfg.trials / 2u;

    printf("median_elapsed_ms=%.3f\n", el_sorted[mid]);
    printf("median_throughput_MBps=%.2f\n", thr_sorted[mid]);
    printf("parity_bytes_per_sec=%.0f\n",
           thr_sorted[mid] * 1.0e6 * (double)parity_bytes /
               (double)input_bytes);
    if (stats[mid].encode_calls > 0) {
        printf("ns_per_block_body=%.1f\n",
               (double)stats[mid].encode_body_ns /
                   (double)stats[mid].encode_calls);
        printf("ns_per_block_lock_wait=%.1f\n",
               (double)stats[mid].lock_wait_ns /
                   (double)stats[mid].encode_calls);
        printf("ns_per_block_lock_hold=%.1f\n",
               (double)stats[mid].lock_hold_ns /
                   (double)stats[mid].encode_calls);
        printf("rebuild_calls_total_mid_trial=%llu\n",
               (unsigned long long)stats[mid].rebuild_calls);
        printf("parity_sink=%llu\n", (unsigned long long)g_parity_sink);
    }
    return 0;
}
