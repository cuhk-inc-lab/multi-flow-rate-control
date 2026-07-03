#ifndef PIPE_IO_H
#define PIPE_IO_H

/*
 * Read/write exact byte counts on a pipe (for inter-stage transfer).
 */

#include <stddef.h>
#include <sys/types.h>

#include "circular_buffer.h"

typedef enum {
    PIPE_IO_OK = 0,
    PIPE_IO_ERR_INVALID = -1,
    PIPE_IO_ERR_IO = -2,
    PIPE_IO_ERR_SHORT = -3
} PipeIoStatus;

PipeIoStatus pipe_io_read_full(int fd, void *buf, size_t len);
PipeIoStatus pipe_io_write_full(int fd, const void *buf, size_t len);

/*
 * Read block_size bytes from pipe, write into CircularBuffer dst.
 * Returns PIPE_IO_ERR_SHORT if fewer than block_size bytes available without blocking.
 */
PipeIoStatus pipe_io_read_to_buffer(int fd, CircularBuffer *dst, size_t block_size,
                                    int blocking);

#endif /* PIPE_IO_H */
