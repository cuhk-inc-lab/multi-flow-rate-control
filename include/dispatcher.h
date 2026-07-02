#ifndef DISPATCHER_H
#define DISPATCHER_H

#include "flow_manager.h"

typedef enum {
    DP_OK = 0,
    DP_ERR_INVALID = -1,
    DP_ERR_SYSTEM = -2
} DispatcherStatus;

DispatcherStatus dispatcher_start(FlowManager *mgr);
DispatcherStatus dispatcher_join(FlowManager *mgr);

void *dispatcher_thread(void *arg);

#endif /* DISPATCHER_H */
