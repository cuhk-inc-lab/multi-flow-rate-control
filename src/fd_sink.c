#include "fd_sink.h"

#include <errno.h>
#include <unistd.h>

FdSinkStatus fd_sink_write(int fd, const void *buf, size_t len, ssize_t *written)
{
    const unsigned char *cursor;
    size_t remaining;
    ssize_t total = 0;
    ssize_t n;

    if (fd < 0 || (len > 0 && buf == NULL)) {
        return FD_SINK_ERR_INVALID;
    }

    cursor = buf;
    remaining = len;

    while (remaining > 0) {
        do {
            n = write(fd, cursor, remaining);
        } while (n < 0 && errno == EINTR);

        if (n < 0) {
            if (written != NULL) {
                *written = total;
            }
            return FD_SINK_ERR_IO;
        }

        if (n == 0) {
            if (written != NULL) {
                *written = total;
            }
            return FD_SINK_ERR_IO;
        }

        cursor += (size_t)n;
        remaining -= (size_t)n;
        total += n;
    }

    if (written != NULL) {
        *written = total;
    }

    return FD_SINK_OK;
}

FdSinkStatus fd_sink_write_packet(int fd, const DataPacket *pkt, ssize_t *written)
{
    if (pkt == NULL) {
        return FD_SINK_ERR_INVALID;
    }

    if (pkt->payload_len == 0) {
        if (written != NULL) {
            *written = 0;
        }
        return FD_SINK_OK;
    }

    if (pkt->payload == NULL) {
        return FD_SINK_ERR_INVALID;
    }

    return fd_sink_write(fd, pkt->payload, pkt->payload_len, written);
}
