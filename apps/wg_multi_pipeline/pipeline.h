#ifndef WG_PIPELINE_H
#define WG_PIPELINE_H

#include "codec.h"

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

WgPipelineStatus wg_pipeline_run(const WgPipelineConfig *config);
WgPipelineStatus wg_pipeline_run_udp(const WgUdpConfig *config);

#endif /* WG_PIPELINE_H */
