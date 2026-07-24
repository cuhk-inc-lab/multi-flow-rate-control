#ifndef WG_PIPELINE_H
#define WG_PIPELINE_H

#include "codec.h"
#include "flow_peer_map.h"

#include <stdint.h>

typedef enum {
    WG_PIPE_OK = 0,
    WG_PIPE_ERR = -1
} WgPipelineStatus;

typedef struct WgFlowPath {
    uint32_t    flow_id;
    const char *input_path;
    const char *output_path;
} WgFlowPath;

typedef struct WgPipelineConfig {
    const WgFlowPath *flows;
    uint32_t          flow_count;
    int               pacing_enabled;
    CodecKind         codec_kind;
} WgPipelineConfig;

typedef struct WgUdpConfig {
    uint16_t    port;
    const char *output_prefix;
    uint32_t    max_flows;
    unsigned    idle_sec;
    int         pacing_enabled;
    CodecKind   codec_kind;
} WgUdpConfig;

typedef struct WgWireFlowPath {
    uint32_t    flow_id;
    const char *input_path;
    const char *host;
    uint16_t    port;
    double      source_rate_mbps;
    /* When set, file ingress uses flow_peer_map + ingress_push_tuple. */
    int         use_tuple;
    FlowTuple   tuple;
} WgWireFlowPath;

typedef struct WgWireMultiSendConfig {
    const WgWireFlowPath *flows;
    uint32_t              flow_count;
    int                   pacing_enabled;
    CodecKind             codec_kind;
    /* Shared map for tuple-mode flows; NULL when no flow uses a fake 5-tuple. */
    FlowPeerMap          *peer_map;
} WgWireMultiSendConfig;

WgPipelineStatus wg_pipeline_run(const WgPipelineConfig *config);
WgPipelineStatus wg_pipeline_run_udp(const WgUdpConfig *config);
WgPipelineStatus wg_pipeline_run_wire_multi_send(const WgWireMultiSendConfig *config);

#endif /* WG_PIPELINE_H */
