#ifndef FILE_INGEST_H
#define FILE_INGEST_H

#include "circular_buffer.h"

#include <stddef.h>
#include <stdio.h>

typedef enum {
    INGEST_OK       = 0,
    INGEST_EOF      = 1,
    INGEST_BUF_FULL = 2,
    INGEST_ERR      = -1
} IngestStatus;

typedef struct FileIngest {
    FILE *fp;
    char  path[512];
} FileIngest;

IngestStatus FileIngest_open(FileIngest *ingest, const char *path);
IngestStatus FileIngest_pump_once(FileIngest *ingest, CircularBuffer *buf,
                                  size_t *pkg_len);
void FileIngest_close(FileIngest *ingest);

#endif /* FILE_INGEST_H */
