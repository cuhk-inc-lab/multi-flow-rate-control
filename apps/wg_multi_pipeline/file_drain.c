#include "file_drain.h"
#include "stream_config.h"

#include <stdio.h>
#include <string.h>

static size_t min_size(size_t a, size_t b)
{
    return a < b ? a : b;
}

DrainStatus FileDrain_open(FileDrain *drain, const char *path)
{
    if (drain == NULL || path == NULL) {
        return DRAIN_ERR;
    }

    memset(drain, 0, sizeof(*drain));
    strncpy(drain->path, path, sizeof(drain->path) - 1u);
    drain->path[sizeof(drain->path) - 1u] = '\0';

    drain->fp = fopen(path, "wb");
    if (drain->fp == NULL) {
        return DRAIN_ERR;
    }

    return DRAIN_OK;
}

DrainStatus FileDrain_pull_once(FileDrain *drain, CircularBuffer *buf,
                                size_t chunk_size, size_t *written)
{
    unsigned char chunk[PKG_SIZE];
    size_t        n;
    size_t        out;
    CB_Status     st;

    if (drain == NULL || buf == NULL || chunk_size == 0 || drain->fp == NULL) {
        return DRAIN_ERR;
    }

    if (Buffer_IsEmpty(buf)) {
        return DRAIN_EMPTY;
    }

    n = min_size(chunk_size, buf->size);
    if (n > sizeof(chunk)) {
        n = sizeof(chunk);
    }

    st = Buffer_Read(buf, chunk, n);
    if (st != CB_OK) {
        return DRAIN_ERR;
    }

    out = fwrite(chunk, 1, n, drain->fp);
    if (out != n) {
        return DRAIN_ERR;
    }

    if (written != NULL) {
        *written = n;
    }

    return DRAIN_OK;
}

void FileDrain_close(FileDrain *drain)
{
    if (drain == NULL) {
        return;
    }

    if (drain->fp != NULL) {
        fclose(drain->fp);
        drain->fp = NULL;
    }
}
