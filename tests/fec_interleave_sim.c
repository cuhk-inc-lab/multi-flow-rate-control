/*
 * Offline sim of per-group send vs cross-group (column) interleave.
 *
 * Does not change fec_transport, wire_udp, or the relay. Encodes D RS groups
 * with RsCodec, reorders already-encoded shards, punches a consecutive burst
 * of erasures, then asks Codec_recover.
 *
 * Sequential:  g0 all shards, g1 all shards, ...
 * Interleaved: shard 0 of every group, then shard 1, ...
 *
 * Delay is counted in send slots (one UDP shard = one slot). First shard of
 * group 0 is slot 0 in both orders: this is not the "buffer D groups first"
 * schedule.
 */

#include "codec.h"
#include "rs_codec.h"
#include "stream_config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define EXPECT(cond)                                                           \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);   \
            return -1;                                                         \
        }                                                                      \
    } while (0)

#define MAX_GROUPS 8u
#define MAX_N      32u
#define MAX_SLOTS  (MAX_GROUPS * MAX_N)
#define NOT_READY  (-1)

typedef enum {
    ORDER_SEQUENTIAL = 0,
    ORDER_INTERLEAVE
} SendOrder;

typedef struct {
    unsigned group;
    unsigned shard;
} Slot;

typedef struct {
    unsigned recovered;
    unsigned failed;
    int failed_group[MAX_GROUPS];
    int g0_first_slot;
    int g0_ready_slot;
    int ready_slot[MAX_GROUPS];
    int payload_ok;
} SimResult;

static const char *order_name(SendOrder order)
{
    return order == ORDER_INTERLEAVE ? "interleave" : "sequential";
}

static void fill_payload(unsigned char *data, size_t len, unsigned seed)
{
    size_t i;
    unsigned state = seed;

    for (i = 0; i < len; i++) {
        state = state * 1103515245u + 12345u;
        data[i] = (unsigned char)(state >> 16);
    }
}

static void build_order(Slot *slots, unsigned depth, unsigned n, SendOrder order)
{
    unsigned group;
    unsigned shard;
    unsigned i = 0;

    if (order == ORDER_SEQUENTIAL) {
        for (group = 0; group < depth; group++) {
            for (shard = 0; shard < n; shard++) {
                slots[i].group = group;
                slots[i].shard = shard;
                i++;
            }
        }
        return;
    }

    for (shard = 0; shard < n; shard++) {
        for (group = 0; group < depth; group++) {
            slots[i].group = group;
            slots[i].shard = shard;
            i++;
        }
    }
}

static int run_sim(const unsigned char *encoded, const unsigned char *original,
                   size_t k, size_t r, unsigned depth, unsigned burst,
                   unsigned burst_at, SendOrder order, SimResult *out)
{
    const size_t n = k + r;
    const size_t shard_bytes = PKG_SIZE;
    const size_t block_bytes = n * shard_bytes;
    const size_t input_bytes = k * shard_bytes;
    const unsigned total = (unsigned)(depth * n);
    const Codec *codec = RsCodec_get();
    Slot slots[MAX_SLOTS];
    unsigned received[MAX_GROUPS];
    uint8_t present[MAX_GROUPS][codec_present_bytes(MAX_N)];
    unsigned char *work = NULL;
    unsigned i;
    unsigned g;

    memset(out, 0, sizeof(*out));
    out->g0_first_slot = NOT_READY;
    out->g0_ready_slot = NOT_READY;
    out->payload_ok = 1;
    for (g = 0; g < depth; g++) {
        out->ready_slot[g] = NOT_READY;
        out->failed_group[g] = 0;
        received[g] = 0;
        codec_present_clear_all(present[g], n);
    }

    build_order(slots, depth, (unsigned)n, order);

    for (i = 0; i < total; i++) {
        unsigned group = slots[i].group;
        unsigned shard = slots[i].shard;

        if (group == 0u && shard == 0u) {
            out->g0_first_slot = (int)i;
        }
        if (i >= burst_at && i < burst_at + burst) {
            continue;
        }
        received[group]++;
        codec_present_set(present[group], shard);
        if (received[group] == k && out->ready_slot[group] == NOT_READY) {
            out->ready_slot[group] = (int)i;
        }
    }

    out->g0_ready_slot = out->ready_slot[0];
    work = malloc(block_bytes);
    if (work == NULL) {
        return -1;
    }

    for (g = 0; g < depth; g++) {
        CodecRecoverStatus status;

        memcpy(work, encoded + (size_t)g * block_bytes, block_bytes);
        for (i = 0; i < n; i++) {
            if (!codec_present_get(present[g], i)) {
                memset(work + (size_t)i * shard_bytes, 0, shard_bytes);
            }
        }
        status = Codec_recover(codec, work, present[g], n);
        if (status == CODEC_RECOVER_OK &&
            memcmp(work, original + (size_t)g * input_bytes, input_bytes) == 0) {
            out->recovered++;
            continue;
        }
        out->failed++;
        out->failed_group[g] = 1;
        out->payload_ok = 0;
        out->ready_slot[g] = NOT_READY;
        if (g == 0u) {
            out->g0_ready_slot = NOT_READY;
        }
    }

    free(work);
    return 0;
}

static int encode_groups(unsigned char *encoded, unsigned char *original,
                         size_t k, unsigned depth)
{
    const size_t n = Codec_output_block_size(RsCodec_get()) / PKG_SIZE;
    const size_t block_bytes = n * PKG_SIZE;
    const size_t input_bytes = k * PKG_SIZE;
    unsigned g;

    for (g = 0; g < depth; g++) {
        unsigned char *block = encoded + (size_t)g * block_bytes;

        fill_payload(original + (size_t)g * input_bytes, input_bytes,
                     0xA500u + g * 17u);
        memcpy(block, original + (size_t)g * input_bytes, input_bytes);
        memset(block + input_bytes, 0, block_bytes - input_bytes);
        Codec_encode(RsCodec_get(), block, block_bytes);
    }
    return 0;
}

static void print_row(size_t k, size_t r, unsigned depth, unsigned burst,
                      unsigned burst_at, SendOrder order, const SimResult *res)
{
    printf("  %zu+%zu  D=%u  burst %u@%u  %-12s  recovered %u/%u  "
           "g0_first=%d  g0_ready=%d%s\n",
           k, r, depth, burst, burst_at, order_name(order), res->recovered,
           depth, res->g0_first_slot, res->g0_ready_slot,
           res->failed_group[0] ? "  G0_FAIL" : "");
}

static int expect_no_loss_delay(size_t k, size_t r, unsigned depth,
                                const unsigned char *encoded,
                                const unsigned char *original)
{
    SimResult seq;
    SimResult intl;
    const int expected_extra = (int)((k - 1u) * (depth - 1u));

    EXPECT(run_sim(encoded, original, k, r, depth, 0, 0, ORDER_SEQUENTIAL,
                   &seq) == 0);
    EXPECT(run_sim(encoded, original, k, r, depth, 0, 0, ORDER_INTERLEAVE,
                   &intl) == 0);
    print_row(k, r, depth, 0, 0, ORDER_SEQUENTIAL, &seq);
    print_row(k, r, depth, 0, 0, ORDER_INTERLEAVE, &intl);

    EXPECT(seq.recovered == depth && intl.recovered == depth);
    EXPECT(seq.payload_ok && intl.payload_ok);
    EXPECT(seq.g0_first_slot == 0 && intl.g0_first_slot == 0);
    EXPECT(seq.g0_ready_slot == (int)(k - 1u));
    EXPECT(intl.g0_ready_slot == (int)((k - 1u) * depth));
    EXPECT(intl.g0_ready_slot - seq.g0_ready_slot == expected_extra);
    return 0;
}

static int expect_burst_kills_seq_not_intl(size_t k, size_t r, unsigned depth,
                                           unsigned burst,
                                           const unsigned char *encoded,
                                           const unsigned char *original)
{
    SimResult seq;
    SimResult intl;

    EXPECT(burst == r + 1u);
    EXPECT(depth >= burst);

    EXPECT(run_sim(encoded, original, k, r, depth, burst, 0, ORDER_SEQUENTIAL,
                   &seq) == 0);
    EXPECT(run_sim(encoded, original, k, r, depth, burst, 0, ORDER_INTERLEAVE,
                   &intl) == 0);
    print_row(k, r, depth, burst, 0, ORDER_SEQUENTIAL, &seq);
    print_row(k, r, depth, burst, 0, ORDER_INTERLEAVE, &intl);

    EXPECT(seq.failed_group[0]);
    EXPECT(seq.recovered == depth - 1u);
    EXPECT(intl.recovered == depth && intl.payload_ok);
    EXPECT(intl.g0_first_slot == 0);
    return 0;
}

static int print_burst_sweep(size_t k, size_t r, unsigned depth,
                             const unsigned char *encoded,
                             const unsigned char *original)
{
    unsigned burst;
    const unsigned max_burst = (unsigned)((r + 1u) * depth);

    printf("  burst sweep (offset 0): recovered sequential vs interleave\n");
    for (burst = 0; burst <= max_burst; burst++) {
        SimResult seq;
        SimResult intl;

        if (run_sim(encoded, original, k, r, depth, burst, 0, ORDER_SEQUENTIAL,
                    &seq) != 0 ||
            run_sim(encoded, original, k, r, depth, burst, 0, ORDER_INTERLEAVE,
                    &intl) != 0) {
            return -1;
        }
        printf("    B=%-2u  seq %u/%u%s  intl %u/%u%s\n", burst, seq.recovered,
               depth, seq.failed_group[0] ? " G0_FAIL" : "        ",
               intl.recovered, depth, intl.failed ? " FAIL" : "");
        if (seq.recovered == 0 && intl.recovered == 0) {
            break;
        }
    }
    return 0;
}

static int run_profile(size_t k, size_t r, unsigned depth)
{
    const size_t n = k + r;
    const size_t block_bytes = n * PKG_SIZE;
    const size_t input_bytes = k * PKG_SIZE;
    unsigned char *encoded;
    unsigned char *original;
    int rc;

    EXPECT(depth >= 2u && depth <= MAX_GROUPS);
    EXPECT(n <= MAX_N);
    EXPECT(RsCodec_set_params(k, r) == 0);
    EXPECT(Codec_data_shards(RsCodec_get()) == k);
    EXPECT(Codec_parity_shards(RsCodec_get()) == r);

    encoded = calloc(depth, block_bytes);
    original = calloc(depth, input_bytes);
    EXPECT(encoded != NULL && original != NULL);
    EXPECT(encode_groups(encoded, original, k, depth) == 0);

    printf("\nRS %zu+%zu  depth=%u  n=%zu  (%u slots)\n", k, r, depth, n,
           (unsigned)(depth * n));
    rc = expect_no_loss_delay(k, r, depth, encoded, original);
    if (rc == 0) {
        rc = expect_burst_kills_seq_not_intl(k, r, depth, (unsigned)(r + 1u),
                                             encoded, original);
    }
    if (rc == 0) {
        rc = print_burst_sweep(k, r, depth, encoded, original);
    }

    free(encoded);
    free(original);
    return rc;
}

int main(void)
{
    const double wire_bytes = 28.0 + 44.0 + (double)PKG_SIZE;
    const double gbps_us = wire_bytes * 8.0 / 1000.0;

    puts("cross-group interleave sim (offline RsCodec, no sockets)");
    puts("sequential:  group-major    interleaved: shard-major (columns)");
    puts("g0_first / g0_ready are send-slot indices; slot 0 is G0 shard 0.");

    if (run_profile(4u, 2u, 4u) != 0) {
        fputs("RS 4+2 depth 4 failed\n", stderr);
        return 1;
    }
    if (run_profile(16u, 2u, 4u) != 0) {
        fputs("RS 16+2 depth 4 failed\n", stderr);
        return 1;
    }

    printf("\n1 Gbps, ~%.0f B on wire: ~%.1f us/slot\n", wire_bytes, gbps_us);
    printf("4+2 D=4: G0 extra ready = %u slots (~%.1f us); 200 ms RTO ~ %.0f slots\n",
           (unsigned)((4u - 1u) * (4u - 1u)), 9.0 * gbps_us, 200000.0 / gbps_us);
    printf("16+2 D=4: G0 extra ready = %u slots (~%.1f us)\n",
           (unsigned)((16u - 1u) * (4u - 1u)), 45.0 * gbps_us);

    EXPECT(RsCodec_set_params(4u, 2u) == 0);
    puts("\nfec interleave sim passed");
    return 0;
}
