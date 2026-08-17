/*
 * Local source encode → harness inject → TX capture.
 * Also covers reserved transit hooks (identity + decode_reencode stub).
 */
#include "local_decode.h"
#include "local_source.h"
#include "relay.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define EXPECT(cond)                                                           \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);   \
            exit(1);                                                           \
        }                                                                      \
    } while (0)

typedef struct Capture {
    size_t   count;
    size_t   bytes;
    uint8_t  saw_end;
    uint32_t flow_id;
} Capture;

static void capture_tx(const uint8_t *datagram, size_t len, void *ctx)
{
    Capture *cap = ctx;
    WireHeader hdr;

    EXPECT(cap != NULL);
    EXPECT(datagram != NULL);
    EXPECT(wire_header_decode(&hdr, datagram, len) == 0);
    cap->count++;
    cap->bytes += len;
    cap->flow_id = hdr.flow_id;
    if (hdr.type == WIRE_TYPE_END) {
        cap->saw_end = 1;
    }
}

static int emit_inject(const uint8_t *datagram, size_t len, void *ctx)
{
    RelayCtx *relay = ctx;

    return relay_inject_wire_datagram(relay, datagram, len) == RELAY_INGRESS_OK
               ? 0
               : -1;
}

static void write_temp_input(char *path, size_t path_len, size_t nbytes)
{
    FILE *f;
    size_t i;
    int fd;

    snprintf(path, path_len, "/tmp/relay_local_source_XXXXXX");
    fd = mkstemp(path);
    EXPECT(fd >= 0);
    close(fd);
    f = fopen(path, "wb");
    EXPECT(f != NULL);
    for (i = 0; i < nbytes; i++) {
        unsigned char b = (unsigned char)(i * 17u + 3u);
        EXPECT(fwrite(&b, 1, 1, f) == 1);
    }
    fclose(f);
}

static void test_source_inject_forward(void)
{
    char path[128];
    RelayConfig cfg;
    RelayCtx *ctx = NULL;
    Capture cap;
    LocalSourceConfig src;
    LocalSourceStats stats;
    const RelayFlowStats *tot;

    write_temp_input(path, sizeof(path), 3000);

    memset(&cfg, 0, sizeof(cfg));
    memset(&cap, 0, sizeof(cap));
    cfg.local_node_id = 2;
    cfg.next_hop_host = "127.0.0.1";
    cfg.next_hop_port = 1;
    cfg.reject_local_encoder_loopback = 1;
    cfg.recode_fn = relay_recode_identity;
    cfg.decode_reencode_fn = relay_decode_reencode_stub;
    cfg.process_mode = RELAY_PROCESS_FORWARD;
    cfg.egress_capacity = 256;

    EXPECT(relay_harness_open(&ctx, &cfg, capture_tx, &cap) == RELAY_OK);

    memset(&src, 0, sizeof(src));
    src.input_path = path;
    src.codec_kind = CODEC_KIND_COPY;
    src.flow_id = 7;
    src.final_dst = 4; /* not local → forward */
    src.ttl = 8;
    EXPECT(local_source_run(&src, emit_inject, ctx, &stats) == 0);
    EXPECT(stats.blocks >= 1);
    EXPECT(stats.emit_errors == 0);

    {
        int spins;
        for (spins = 0; spins < 2000; spins++) {
            const RelayFlowStats *st = relay_total_stats(ctx);
            if (st != NULL && st->forward >= stats.wire_datagrams) {
                break;
            }
            usleep(1000);
        }
    }

    EXPECT(cap.saw_end == 1);
    EXPECT(cap.count == stats.wire_datagrams);
    EXPECT(cap.flow_id == 7);

    tot = relay_total_stats(ctx);
    EXPECT(tot != NULL);
    EXPECT(tot->forward == stats.wire_datagrams);
    EXPECT(tot->local_deliver == 0);

    relay_harness_close(ctx);
    unlink(path);
}

static void test_source_inject_local_decode(void)
{
    char in_path[128];
    char out_path[128];
    RelayConfig cfg;
    RelayCtx *ctx = NULL;
    LocalDecodeHub hub;
    LocalDecodeHubConfig hub_cfg;
    LocalSourceConfig src;
    LocalSourceStats stats;
    FILE *out;
    long out_len;

    write_temp_input(in_path, sizeof(in_path), 2000);
    snprintf(out_path, sizeof(out_path), "/tmp/relay_local_source_out_XXXXXX");
    {
        int fd = mkstemp(out_path);
        EXPECT(fd >= 0);
        close(fd);
    }

    memset(&hub_cfg, 0, sizeof(hub_cfg));
    hub_cfg.codec_kind = CODEC_KIND_COPY;
    hub_cfg.mode = LOCAL_DECODE_MODE_SINGLE_FILE;
    hub_cfg.output_path = out_path;
    hub_cfg.local_node_id = 4;
    hub_cfg.best_effort = 0;
    EXPECT(local_decode_hub_init(&hub, &hub_cfg) == 0);

    memset(&cfg, 0, sizeof(cfg));
    cfg.local_node_id = 4;
    cfg.next_hop_host = "127.0.0.1";
    cfg.next_hop_port = 1;
    cfg.reject_local_encoder_loopback = 0;
    cfg.delivery_fn = local_decode_hub_delivery;
    cfg.delivery_ctx = &hub;
    cfg.egress_capacity = 256;
    EXPECT(relay_harness_open(&ctx, &cfg, NULL, NULL) == RELAY_OK);

    memset(&src, 0, sizeof(src));
    src.input_path = in_path;
    src.codec_kind = CODEC_KIND_COPY;
    src.flow_id = 1;
    src.final_dst = 4;
    src.ttl = 4;
    EXPECT(local_source_run(&src, emit_inject, ctx, &stats) == 0);
    EXPECT(local_decode_hub_strict_check(&hub) == 0);

    out = fopen(out_path, "rb");
    EXPECT(out != NULL);
    EXPECT(fseek(out, 0, SEEK_END) == 0);
    out_len = ftell(out);
    fclose(out);
    EXPECT(out_len == 2000);

    relay_harness_close(ctx);
    local_decode_hub_destroy(&hub);
    unlink(in_path);
    unlink(out_path);
}

int main(void)
{
    test_source_inject_forward();
    test_source_inject_local_decode();
    printf("relay_local_source_tests: ok\n");
    return 0;
}
