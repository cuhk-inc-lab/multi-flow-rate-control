#ifndef WIRE_UDP_H
#define WIRE_UDP_H

#include "codec.h"

#include <stdbool.h>
#include <sys/socket.h>
#include <stdint.h>

typedef struct WireUdpSendConfig {
    const char *host;
    uint16_t    port;
    const char *input_path;
    CodecKind   codec_kind;
    double      source_rate_mbps;
    uint32_t    flow_id;
} WireUdpSendConfig;

typedef struct WireUdpRecvConfig {
    uint16_t    port;
    const char *output_path;
    CodecKind   codec_kind;
    unsigned    idle_sec;
    int         best_effort;
    uint32_t    max_flows;
    /* Demo/teaching: after Codec_decode path, append a text mark into the output. */
    int         decode_mark;
} WireUdpRecvConfig;

typedef struct WireUdpTx {
    int                     sock;
    struct sockaddr_storage address;
    socklen_t               address_len;
    uint32_t                flow_id;
    uint64_t                block_id;
    uint64_t                source_bytes;
    double                  source_rate_mbps;
    double                  started;
} WireUdpTx;

int wire_udp_send(const WireUdpSendConfig *config);
int wire_udp_recv(const WireUdpRecvConfig *config);
int wire_udp_tx_init(WireUdpTx *tx, const char *host, uint16_t port,
                     uint32_t flow_id, double source_rate_mbps);
void wire_udp_tx_destroy(WireUdpTx *tx);
bool wire_udp_tx_ready(const WireUdpTx *tx);
int wire_udp_tx_send_block(WireUdpTx *tx, const Codec *codec,
                           const unsigned char *encoded_block,
                           size_t valid_len, uint64_t encode_begin_ns,
                           uint64_t encode_end_ns);
int wire_udp_tx_send_end(WireUdpTx *tx, const Codec *codec);

#endif /* WIRE_UDP_H */
