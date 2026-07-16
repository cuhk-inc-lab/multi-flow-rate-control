#ifndef WIRE_UDP_H
#define WIRE_UDP_H

#include "codec.h"

#include <stdint.h>

typedef struct WireUdpSendConfig {
    const char *host;
    uint16_t    port;
    const char *input_path;
    CodecKind   codec_kind;
    double      source_rate_mbps;
} WireUdpSendConfig;

typedef struct WireUdpRecvConfig {
    uint16_t    port;
    const char *output_path;
    CodecKind   codec_kind;
    unsigned    idle_sec;
} WireUdpRecvConfig;

int wire_udp_send(const WireUdpSendConfig *config);
int wire_udp_recv(const WireUdpRecvConfig *config);

#endif /* WIRE_UDP_H */
