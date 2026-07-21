#include "buffer_transfer.h"

#include <string.h>

static size_t min_size(size_t a, size_t b)
{
    return a < b ? a : b;
}

TransferStatus BufferTransfer_pump(CircularBuffer *src, CircularBuffer *dst,
                                   size_t max_bytes, size_t *moved)
{
    size_t        avail;
    size_t        n;
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

    if (max_bytes > 0) {
        if (avail < max_bytes) {
            return TRANSFER_SRC_EMPTY;
        }

        if (dst->overflow_policy != CB_OVERFLOW_OVERWRITE) {
            size_t space = dst->capacity - dst->size;

            if (space < max_bytes) {
                return TRANSFER_DST_FULL;
            }
        }

        n = max_bytes;
    } else if (dst->overflow_policy == CB_OVERFLOW_OVERWRITE) {
        n = avail;
    } else {
        size_t space = dst->capacity - dst->size;

        n = min_size(avail, space);
        if (n == 0) {
            return TRANSFER_DST_FULL;
        }
    }

    {
        size_t remaining = n;

        while (remaining > 0) {
            size_t src_chunk = src->head->capacity - src->head_offset;
            size_t dst_chunk = dst->tail->capacity - dst->tail_offset;
            size_t chunk = min_size(remaining, min_size(src_chunk, dst_chunk));

            memcpy(dst->tail->data + dst->tail_offset,
                   src->head->data + src->head_offset,
                   chunk);

            src->head_offset += chunk;
            if (src->head_offset == src->head->capacity) {
                src->head = src->head->next;
                src->head_offset = 0;
            }
            dst->tail_offset += chunk;
            if (dst->tail_offset == dst->tail->capacity) {
                dst->tail = dst->tail->next;
                dst->tail_offset = 0;
            }

            src->size -= chunk;
            dst->size += chunk;
            remaining -= chunk;
        }

        if (src->size == 0) {
            src->head = src->tail;
            src->head_offset = src->tail_offset;
        }
    }

    if (moved != NULL) {
        *moved = n;
    }

    return TRANSFER_OK;
}
