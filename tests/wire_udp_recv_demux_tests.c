/*
 * Regression: wire_udp_recv must demux by WireHeader.flow_id, not UDP peer.
 * All packets in these tests are sent from one bound UDP socket (one source
 * port) so a peer-map demux would incorrectly merge flows.
 */
#include "codec.h"
#include "stream_config.h"
#include "wire_header.h"
#include "wire_udp.h"

#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#define MAX_DG (WIRE_HEADER_SIZE + PKG_SIZE)

static int g_failures = 0;

#define EXPECT(cond)                                                           \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);    \
            g_failures++;                                                      \
        }                                                                      \
    } while (0)

typedef struct RecvThreadArg {
    WireUdpRecvConfig cfg;
    int               rc;
} RecvThreadArg;

static void *recv_thread_main(void *arg)
{
    RecvThreadArg *a = arg;

    a->rc = wire_udp_recv(&a->cfg);
    return NULL;
}

static int build_copy_block_datagrams(uint8_t datagrams[][MAX_DG], size_t *lens,
                                      uint16_t *out_shards,
                                      const uint8_t *plaintext, size_t plen,
                                      uint32_t flow_id, uint64_t block_id,
                                      uint8_t final_dst, uint8_t ttl)
{
    const Codec *codec = CopyCodec_get();
    unsigned char *block;
    size_t input_size;
    size_t output_size;
    uint16_t shard_count;
    uint16_t shard;
    uint16_t valid_len;

    if (codec == NULL || plaintext == NULL || plen == 0) {
        return -1;
    }
    input_size = Codec_input_block_size(codec);
    output_size = Codec_output_block_size(codec);
    if (plen > input_size || output_size % PKG_SIZE != 0) {
        return -1;
    }
    valid_len = (uint16_t)plen;
    shard_count = (uint16_t)(output_size / PKG_SIZE);
    block = calloc(1, output_size);
    if (block == NULL) {
        return -1;
    }
    memcpy(block, plaintext, plen);
    Codec_encode(codec, block, output_size);

    for (shard = 0; shard < shard_count; shard++) {
        WireHeader hdr;
        size_t dlen = WIRE_HEADER_SIZE + PKG_SIZE;

        memset(&hdr, 0, sizeof(hdr));
        hdr.type = WIRE_TYPE_DATA;
        hdr.final_dst = final_dst;
        hdr.ttl = ttl;
        hdr.flow_id = flow_id;
        hdr.block_id = block_id;
        hdr.shard_index = shard;
        hdr.shard_count = shard_count;
        hdr.valid_len = valid_len;
        hdr.payload_len = (uint16_t)PKG_SIZE;
        wire_header_encode(datagrams[shard], &hdr);
        memcpy(datagrams[shard] + WIRE_HEADER_SIZE,
               block + (size_t)shard * PKG_SIZE, PKG_SIZE);
        lens[shard] = dlen;
    }
    free(block);
    *out_shards = shard_count;
    return 0;
}

static size_t make_end(uint8_t *out, size_t cap, uint32_t flow_id,
                       uint64_t block_count, uint16_t shard_count,
                       uint8_t final_dst, uint8_t ttl)
{
    WireHeader hdr;

    if (cap < WIRE_HEADER_SIZE) {
        return 0;
    }
    memset(&hdr, 0, sizeof(hdr));
    hdr.type = WIRE_TYPE_END;
    hdr.final_dst = final_dst;
    hdr.ttl = ttl;
    hdr.flow_id = flow_id;
    hdr.block_id = block_count;
    hdr.shard_index = 0;
    hdr.shard_count = shard_count;
    hdr.valid_len = 0;
    hdr.payload_len = 0;
    wire_header_encode(out, &hdr);
    return WIRE_HEADER_SIZE;
}

static int open_fixed_sport_sender(uint16_t bind_port)
{
    int sock;
    int enable = 1;
    struct sockaddr_in addr;

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        return -1;
    }
    (void)setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(bind_port);
    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(sock);
        return -1;
    }
    return sock;
}

static uint16_t sender_bound_port(int sock)
{
    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);

    if (getsockname(sock, (struct sockaddr *)&addr, &len) != 0) {
        return 0;
    }
    return ntohs(addr.sin_port);
}

static int send_all(int sock, const struct sockaddr_in *dst, const uint8_t *buf,
                    size_t len)
{
    ssize_t n;

    do {
        n = sendto(sock, buf, len, 0, (const struct sockaddr *)dst, sizeof(*dst));
    } while (n < 0 && errno == EINTR);
    return (n == (ssize_t)len) ? 0 : -1;
}

static int read_file(const char *path, uint8_t *buf, size_t cap, size_t *out_len)
{
    FILE *fp;
    size_t n;

    fp = fopen(path, "rb");
    if (fp == NULL) {
        return -1;
    }
    n = fread(buf, 1, cap, fp);
    fclose(fp);
    *out_len = n;
    return 0;
}

static int find_flow_output(const char *prefix, uint32_t flow_id, char *out,
                            size_t out_len)
{
    char pattern[256];
    char cmd[512];
    FILE *fp;
    char line[512];

    if (snprintf(pattern, sizeof(pattern), "%ssrc_*_flow_%u.ts", prefix,
                 flow_id) < 0) {
        return -1;
    }
    if (snprintf(cmd, sizeof(cmd), "ls -1 %s 2>/dev/null | head -n 1",
                 pattern) < 0) {
        return -1;
    }
    fp = popen(cmd, "r");
    if (fp == NULL) {
        return -1;
    }
    if (fgets(line, sizeof(line), fp) == NULL) {
        pclose(fp);
        return -1;
    }
    pclose(fp);
    line[strcspn(line, "\n")] = '\0';
    if (line[0] == '\0' || strlen(line) >= out_len) {
        return -1;
    }
    memcpy(out, line, strlen(line) + 1);
    return 0;
}

static void unlink_prefix_outputs(const char *prefix)
{
    char cmd[512];
    int st;

    if (snprintf(cmd, sizeof(cmd), "rm -f %ssrc_*_flow_*.ts", prefix) > 0) {
        st = system(cmd);
        (void)st;
    }
}

/*
 * Same UDP source port interleaves flow 101 and 202 (both block_id=0).
 * Old peer demux would merge them into one decoder and corrupt outputs.
 */
static void test_udp_recv_demuxes_by_wire_header_flow_id_not_udp_peer(void)
{
    const char *prefix = "build/wire_demux_out_";
    const uint16_t recv_port = 23111;
    const uint16_t send_bind = 23112;
    RecvThreadArg rarg;
    pthread_t th;
    int sock = -1;
    struct sockaddr_in dst;
    uint8_t pt101[64];
    uint8_t pt202[64];
    uint8_t d101[PACKAGES_PER_ENCODE_BLOCK][MAX_DG];
    uint8_t d202[PACKAGES_PER_ENCODE_BLOCK][MAX_DG];
    size_t l101[PACKAGES_PER_ENCODE_BLOCK];
    size_t l202[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t s101 = 0;
    uint16_t s202 = 0;
    uint8_t endbuf[WIRE_HEADER_SIZE];
    size_t end_len;
    uint16_t i;
    char path101[512];
    char path202[512];
    uint8_t got[128];
    size_t got_len = 0;
    uint16_t sport;

    memset(pt101, 0xA1, sizeof(pt101));
    memset(pt202, 0xB2, sizeof(pt202));
    unlink_prefix_outputs(prefix);

    EXPECT(build_copy_block_datagrams(d101, l101, &s101, pt101, sizeof(pt101),
                                      101, 0, 4, 8) == 0);
    EXPECT(build_copy_block_datagrams(d202, l202, &s202, pt202, sizeof(pt202),
                                      202, 0, 4, 8) == 0);
    EXPECT(s101 == s202);

    memset(&rarg, 0, sizeof(rarg));
    rarg.cfg.port = recv_port;
    rarg.cfg.output_path = prefix;
    rarg.cfg.codec_kind = CODEC_KIND_COPY;
    rarg.cfg.idle_sec = 3;
    rarg.cfg.max_flows = 2;
    rarg.cfg.local_node_id = 4;
    EXPECT(pthread_create(&th, NULL, recv_thread_main, &rarg) == 0);
    usleep(200000);

    sock = open_fixed_sport_sender(send_bind);
    EXPECT(sock >= 0);
    sport = sender_bound_port(sock);
    EXPECT(sport == send_bind);

    memset(&dst, 0, sizeof(dst));
    dst.sin_family = AF_INET;
    dst.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    dst.sin_port = htons(recv_port);

    /* Interleave shards from both flows on the same socket. */
    for (i = 0; i < s101; i++) {
        EXPECT(send_all(sock, &dst, d101[i], l101[i]) == 0);
        EXPECT(send_all(sock, &dst, d202[i], l202[i]) == 0);
    }
    end_len = make_end(endbuf, sizeof(endbuf), 101, 1, s101, 4, 8);
    EXPECT(end_len == WIRE_HEADER_SIZE);
    EXPECT(send_all(sock, &dst, endbuf, end_len) == 0);
    end_len = make_end(endbuf, sizeof(endbuf), 202, 1, s202, 4, 8);
    EXPECT(end_len == WIRE_HEADER_SIZE);
    EXPECT(send_all(sock, &dst, endbuf, end_len) == 0);

    EXPECT(pthread_join(th, NULL) == 0);
    EXPECT(rarg.rc == 0);

    EXPECT(find_flow_output(prefix, 101, path101, sizeof(path101)) == 0);
    EXPECT(find_flow_output(prefix, 202, path202, sizeof(path202)) == 0);
    EXPECT(strcmp(path101, path202) != 0);

    EXPECT(read_file(path101, got, sizeof(got), &got_len) == 0);
    EXPECT(got_len == sizeof(pt101));
    EXPECT(memcmp(got, pt101, sizeof(pt101)) == 0);

    EXPECT(read_file(path202, got, sizeof(got), &got_len) == 0);
    EXPECT(got_len == sizeof(pt202));
    EXPECT(memcmp(got, pt202, sizeof(pt202)) == 0);

    /* Same source port in both filenames (peer tag), different wire flow ids. */
    {
        char needle[64];

        snprintf(needle, sizeof(needle), "_p%u_flow_101.ts", (unsigned)sport);
        EXPECT(strstr(path101, needle) != NULL);
        snprintf(needle, sizeof(needle), "_p%u_flow_202.ts", (unsigned)sport);
        EXPECT(strstr(path202, needle) != NULL);
    }

    close(sock);
    unlink_prefix_outputs(prefix);
}

static void test_udp_recv_same_peer_same_flow_id_still_orders_blocks(void)
{
    const char *prefix = "build/wire_demux_order_";
    const uint16_t recv_port = 23121;
    const uint16_t send_bind = 23122;
    RecvThreadArg rarg;
    pthread_t th;
    int sock = -1;
    struct sockaddr_in dst;
    uint8_t pt0[32];
    uint8_t pt1[32];
    uint8_t d0[PACKAGES_PER_ENCODE_BLOCK][MAX_DG];
    uint8_t d1[PACKAGES_PER_ENCODE_BLOCK][MAX_DG];
    size_t l0[PACKAGES_PER_ENCODE_BLOCK];
    size_t l1[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t s0 = 0;
    uint16_t s1 = 0;
    uint8_t endbuf[WIRE_HEADER_SIZE];
    size_t end_len;
    uint16_t i;
    char path[512];
    uint8_t got[128];
    size_t got_len = 0;
    uint8_t expect[64];

    memset(pt0, 0x11, sizeof(pt0));
    memset(pt1, 0x22, sizeof(pt1));
    memcpy(expect, pt0, sizeof(pt0));
    memcpy(expect + sizeof(pt0), pt1, sizeof(pt1));
    unlink_prefix_outputs(prefix);

    EXPECT(build_copy_block_datagrams(d0, l0, &s0, pt0, sizeof(pt0), 77, 0, 4,
                                      8) == 0);
    EXPECT(build_copy_block_datagrams(d1, l1, &s1, pt1, sizeof(pt1), 77, 1, 4,
                                      8) == 0);

    memset(&rarg, 0, sizeof(rarg));
    rarg.cfg.port = recv_port;
    rarg.cfg.output_path = prefix;
    rarg.cfg.codec_kind = CODEC_KIND_COPY;
    rarg.cfg.idle_sec = 3;
    rarg.cfg.max_flows = 2;
    rarg.cfg.local_node_id = 4;
    EXPECT(pthread_create(&th, NULL, recv_thread_main, &rarg) == 0);
    usleep(200000);

    sock = open_fixed_sport_sender(send_bind);
    EXPECT(sock >= 0);
    memset(&dst, 0, sizeof(dst));
    dst.sin_family = AF_INET;
    dst.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    dst.sin_port = htons(recv_port);

    /* Deliver block 1 before block 0 (out of order). */
    for (i = 0; i < s1; i++) {
        EXPECT(send_all(sock, &dst, d1[i], l1[i]) == 0);
    }
    for (i = 0; i < s0; i++) {
        EXPECT(send_all(sock, &dst, d0[i], l0[i]) == 0);
    }
    end_len = make_end(endbuf, sizeof(endbuf), 77, 2, s0, 4, 8);
    EXPECT(end_len == WIRE_HEADER_SIZE);
    EXPECT(send_all(sock, &dst, endbuf, end_len) == 0);

    EXPECT(pthread_join(th, NULL) == 0);
    EXPECT(rarg.rc == 0);
    EXPECT(find_flow_output(prefix, 77, path, sizeof(path)) == 0);
    EXPECT(read_file(path, got, sizeof(got), &got_len) == 0);
    EXPECT(got_len == sizeof(expect));
    EXPECT(memcmp(got, expect, sizeof(expect)) == 0);

    close(sock);
    unlink_prefix_outputs(prefix);
}

static void test_udp_recv_capacity_reject_does_not_mix_existing_flows(void)
{
    const char *prefix = "build/wire_demux_cap_";
    const uint16_t recv_port = 23131;
    const uint16_t send_bind = 23132;
    RecvThreadArg rarg;
    pthread_t th;
    int sock = -1;
    struct sockaddr_in dst;
    uint8_t pt101[48];
    uint8_t pt202[48];
    uint8_t pt303[48];
    uint8_t d101[PACKAGES_PER_ENCODE_BLOCK][MAX_DG];
    uint8_t d202[PACKAGES_PER_ENCODE_BLOCK][MAX_DG];
    uint8_t d303[PACKAGES_PER_ENCODE_BLOCK][MAX_DG];
    size_t l101[PACKAGES_PER_ENCODE_BLOCK];
    size_t l202[PACKAGES_PER_ENCODE_BLOCK];
    size_t l303[PACKAGES_PER_ENCODE_BLOCK];
    uint16_t s101 = 0;
    uint16_t s202 = 0;
    uint16_t s303 = 0;
    uint8_t endbuf[WIRE_HEADER_SIZE];
    size_t end_len;
    uint16_t i;
    char path101[512];
    char path202[512];
    char path303[512];
    uint8_t got[128];
    size_t got_len = 0;
    FILE *lsfp;
    char line[512];
    int out_count = 0;

    memset(pt101, 0x31, sizeof(pt101));
    memset(pt202, 0x32, sizeof(pt202));
    memset(pt303, 0x33, sizeof(pt303));
    unlink_prefix_outputs(prefix);

    EXPECT(build_copy_block_datagrams(d101, l101, &s101, pt101, sizeof(pt101),
                                      101, 0, 4, 8) == 0);
    EXPECT(build_copy_block_datagrams(d202, l202, &s202, pt202, sizeof(pt202),
                                      202, 0, 4, 8) == 0);
    EXPECT(build_copy_block_datagrams(d303, l303, &s303, pt303, sizeof(pt303),
                                      303, 0, 4, 8) == 0);

    memset(&rarg, 0, sizeof(rarg));
    rarg.cfg.port = recv_port;
    rarg.cfg.output_path = prefix;
    rarg.cfg.codec_kind = CODEC_KIND_COPY;
    rarg.cfg.idle_sec = 3;
    rarg.cfg.max_flows = 2;
    rarg.cfg.local_node_id = 4;
    EXPECT(pthread_create(&th, NULL, recv_thread_main, &rarg) == 0);
    usleep(200000);

    sock = open_fixed_sport_sender(send_bind);
    EXPECT(sock >= 0);
    memset(&dst, 0, sizeof(dst));
    dst.sin_family = AF_INET;
    dst.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    dst.sin_port = htons(recv_port);

    for (i = 0; i < s101; i++) {
        EXPECT(send_all(sock, &dst, d101[i], l101[i]) == 0);
        EXPECT(send_all(sock, &dst, d202[i], l202[i]) == 0);
    }
    /* Third flow must be rejected without mixing into 101/202. */
    for (i = 0; i < s303; i++) {
        EXPECT(send_all(sock, &dst, d303[i], l303[i]) == 0);
    }
    end_len = make_end(endbuf, sizeof(endbuf), 303, 1, s303, 4, 8);
    EXPECT(send_all(sock, &dst, endbuf, end_len) == 0);

    end_len = make_end(endbuf, sizeof(endbuf), 101, 1, s101, 4, 8);
    EXPECT(send_all(sock, &dst, endbuf, end_len) == 0);
    end_len = make_end(endbuf, sizeof(endbuf), 202, 1, s202, 4, 8);
    EXPECT(send_all(sock, &dst, endbuf, end_len) == 0);

    EXPECT(pthread_join(th, NULL) == 0);
    EXPECT(rarg.rc == 0);

    EXPECT(find_flow_output(prefix, 101, path101, sizeof(path101)) == 0);
    EXPECT(find_flow_output(prefix, 202, path202, sizeof(path202)) == 0);
    EXPECT(find_flow_output(prefix, 303, path303, sizeof(path303)) != 0);

    lsfp = popen("ls -1 build/wire_demux_cap_src_*_flow_*.ts 2>/dev/null | wc -l",
                 "r");
    EXPECT(lsfp != NULL);
    if (lsfp != NULL && fgets(line, sizeof(line), lsfp) != NULL) {
        out_count = atoi(line);
    }
    if (lsfp != NULL) {
        pclose(lsfp);
    }
    EXPECT(out_count == 2);

    EXPECT(read_file(path101, got, sizeof(got), &got_len) == 0);
    EXPECT(got_len == sizeof(pt101));
    EXPECT(memcmp(got, pt101, sizeof(pt101)) == 0);
    EXPECT(read_file(path202, got, sizeof(got), &got_len) == 0);
    EXPECT(got_len == sizeof(pt202));
    EXPECT(memcmp(got, pt202, sizeof(pt202)) == 0);

    close(sock);
    unlink_prefix_outputs(prefix);
}

int main(void)
{
    if (mkdir("build", 0755) != 0) {
        /* ok if exists */
    }

    test_udp_recv_demuxes_by_wire_header_flow_id_not_udp_peer();
    test_udp_recv_same_peer_same_flow_id_still_orders_blocks();
    test_udp_recv_capacity_reject_does_not_mix_existing_flows();

    if (g_failures != 0) {
        fprintf(stderr, "wire_udp_recv_demux_tests: %d failure(s)\n",
                g_failures);
        return EXIT_FAILURE;
    }
    fprintf(stderr, "wire_udp_recv_demux_tests: ok\n");
    return EXIT_SUCCESS;
}
