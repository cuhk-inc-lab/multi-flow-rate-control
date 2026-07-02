#ifndef FD_SINK_H
#define FD_SINK_H

/*
 * write()-based output sink with partial-write and EINTR retry.
 */

#include <stddef.h>
#include <sys/types.h>

#include "packet.h"

typedef enum {
    FD_SINK_OK = 0,
    FD_SINK_ERR_INVALID = -1,
    FD_SINK_ERR_IO = -2
} FdSinkStatus;

FdSinkStatus fd_sink_write(int fd, const void *buf, size_t len, ssize_t *written);
FdSinkStatus fd_sink_write_packet(int fd, const DataPacket *pkt, ssize_t *written);

#endif /* FD_SINK_H */
