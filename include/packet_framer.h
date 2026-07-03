#ifndef PACKET_FRAMER_H
#define PACKET_FRAMER_H

/*
 * Bridge from byte-oriented CircularBuffer to DataPacket push API.
 * Requires circular_buffer.h from buffer-management-module on the include path.
 */

#include <stdint.h>

#include "circular_buffer.h"
#include "flow_manager.h"

typedef enum {
    PF_OK = 0,
    PF_ERR_INVALID = -1,
    PF_ERR_EMPTY = -2,
    PF_ERR_IO = -3,
    PF_ERR_SHUTDOWN = -4
} PacketFramerStatus;

/*
 * Read block_size bytes from src, wrap as DataPacket, push into mgr.
 * Stamps enqueue_ts at push time if stamp_now is non-zero.
 */
PacketFramerStatus packet_framer_pump(CircularBuffer *src,
                                      FlowManager *mgr,
                                      uint32_t flow_id,
                                      size_t block_size,
                                      int stamp_now);

#endif /* PACKET_FRAMER_H */
