#ifndef WG_PIPELINE_H
#define WG_PIPELINE_H

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
} WgPipelineConfig;

WgPipelineStatus wg_pipeline_run(const WgPipelineConfig *config);

#endif /* WG_PIPELINE_H */
