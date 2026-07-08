#ifndef BUFFER_TRANSFER_H
#define BUFFER_TRANSFER_H

#include "circular_buffer.h"

#include <stddef.h>

typedef enum {
    TRANSFER_OK        = 0,
    TRANSFER_SRC_EMPTY = 1,
    TRANSFER_DST_FULL  = 2,
    TRANSFER_ERR       = -1
} TransferStatus;

TransferStatus BufferTransfer_pump(CircularBuffer *src, CircularBuffer *dst,
                                 size_t max_bytes, size_t *moved);

#endif /* BUFFER_TRANSFER_H */
