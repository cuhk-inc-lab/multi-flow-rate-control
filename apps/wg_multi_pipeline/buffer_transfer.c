#include "buffer_transfer.h"

#include <stdlib.h>

static size_t min_size(size_t a, size_t b)
{
    return a < b ? a : b;
}

TransferStatus BufferTransfer_pump(CircularBuffer *src, CircularBuffer *dst,
                                   size_t max_bytes, size_t *moved)
{
    size_t        avail;
    size_t        n;
    unsigned char *tmp;
    CB_Status     rd;
    CB_Status     wr;

    if (src == NULL || dst == NULL) {
        return TRANSFER_ERR;
    }

    if (moved != NULL) {
        *moved = 0;
    }

    avail = src->size;
    if (avail == 0) {
        return TRANSFER_SRC_EMPTY;
    }

    if (dst->overflow_policy == CB_OVERFLOW_OVERWRITE) {
        n = min_size(avail, max_bytes);
    } else {
        size_t space = dst->capacity - dst->size;

        n = min_size(avail, space);
        n = min_size(n, max_bytes);
        if (n == 0) {
            return TRANSFER_DST_FULL;
        }
    }

    tmp = malloc(n);
    if (tmp == NULL) {
        return TRANSFER_ERR;
    }

    rd = Buffer_Read(src, tmp, n);
    if (rd != CB_OK) {
        free(tmp);
        return TRANSFER_ERR;
    }

    wr = Buffer_Write(dst, tmp, n);
    free(tmp);
    if (wr != CB_OK) {
        return TRANSFER_ERR;
    }

    if (moved != NULL) {
        *moved = n;
    }

    return TRANSFER_OK;
}
