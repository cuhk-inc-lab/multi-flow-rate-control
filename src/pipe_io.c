#include "pipe_io.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

PipeIoStatus pipe_io_read_full(int fd, void *buf, size_t len)
{
    unsigned char *cursor = buf;
    size_t remaining = len;
    ssize_t n;

    if (fd < 0 || (len > 0 && buf == NULL)) {
        return PIPE_IO_ERR_INVALID;
    }

    while (remaining > 0) {
        do {
            n = read(fd, cursor, remaining);
        } while (n < 0 && errno == EINTR);

        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return PIPE_IO_ERR_SHORT;
            }
            return PIPE_IO_ERR_IO;
        }
        if (n == 0) {
            return PIPE_IO_ERR_SHORT;
        }

        cursor += (size_t)n;
        remaining -= (size_t)n;
    }

    return PIPE_IO_OK;
}

PipeIoStatus pipe_io_write_full(int fd, const void *buf, size_t len)
{
    const unsigned char *cursor = buf;
    size_t remaining = len;
    ssize_t n;

    if (fd < 0 || (len > 0 && buf == NULL)) {
        return PIPE_IO_ERR_INVALID;
    }

    while (remaining > 0) {
        do {
            n = write(fd, cursor, remaining);
        } while (n < 0 && errno == EINTR);

        if (n < 0) {
            return PIPE_IO_ERR_IO;
        }
        if (n == 0) {
            return PIPE_IO_ERR_IO;
        }

        cursor += (size_t)n;
        remaining -= (size_t)n;
    }

    return PIPE_IO_OK;
}

PipeIoStatus pipe_io_read_to_buffer(int fd, CircularBuffer *dst, size_t block_size,
                                    int blocking)
{
    unsigned char *block;
    PipeIoStatus pst;
    CB_Status cb_st;
    int flags;
    int saved_flags = -1;

    if (fd < 0 || dst == NULL || block_size == 0) {
        return PIPE_IO_ERR_INVALID;
    }

    if (!blocking) {
        flags = fcntl(fd, F_GETFL);
        if (flags >= 0) {
            saved_flags = flags;
            fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        }
    }

    block = malloc(block_size);
    if (block == NULL) {
        if (saved_flags >= 0) {
            fcntl(fd, F_SETFL, saved_flags);
        }
        return PIPE_IO_ERR_IO;
    }

    pst = pipe_io_read_full(fd, block, block_size);
    if (saved_flags >= 0) {
        fcntl(fd, F_SETFL, saved_flags);
    }

    if (pst != PIPE_IO_OK) {
        free(block);
        return pst;
    }

    cb_st = Buffer_Write(dst, block, block_size);
    free(block);
    if (cb_st != CB_OK) {
        return PIPE_IO_ERR_IO;
    }

    return PIPE_IO_OK;
}
