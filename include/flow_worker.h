#ifndef FLOW_WORKER_H
#define FLOW_WORKER_H

#include "flow_context.h"

typedef enum {
    FW_OK = 0,
    FW_ERR_INVALID = -1,
    FW_ERR_SYSTEM = -2
} FlowWorkerStatus;

FlowWorkerStatus flow_worker_start(FlowContext *ctx);
FlowWorkerStatus flow_worker_join(FlowContext *ctx);

/* Thread entry point; normally invoked via flow_worker_start(). */
void *flow_worker_thread(void *arg);

#endif /* FLOW_WORKER_H */
