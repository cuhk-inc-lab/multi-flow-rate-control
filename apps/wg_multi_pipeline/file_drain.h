#ifndef FILE_DRAIN_H
#define FILE_DRAIN_H

#include "circular_buffer.h"

#include <stddef.h>
#include <stdio.h>

typedef enum {
    DRAIN_OK    = 0,
    DRAIN_EMPTY = 1,
    DRAIN_ERR   = -1
} DrainStatus;

typedef struct FileDrain {
    FILE *fp;
    char  path[512];
} FileDrain;

DrainStatus FileDrain_open(FileDrain *drain, const char *path);
DrainStatus FileDrain_pull_once(FileDrain *drain, CircularBuffer *buf,
                                size_t chunk_size, size_t *written);
void FileDrain_close(FileDrain *drain);

#endif /* FILE_DRAIN_H */
