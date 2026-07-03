#ifndef RELAY_H
#define RELAY_H

#include <stdint.h>

typedef enum {
    RELAY_OK = 0,
    RELAY_ERR = -1
} RelayStatus;

typedef struct FlowRelayPath {
    uint32_t    flow_id;
    const char *input_path;
    const char *output_path;
} FlowRelayPath;

typedef struct RelayConfig {
    const FlowRelayPath *flows;
    uint32_t             flow_count;
    int                  pacing_enabled;
} RelayConfig;

RelayStatus Relay_run(const RelayConfig *config);

#endif /* RELAY_H */
