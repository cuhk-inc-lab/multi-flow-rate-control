#include "file_ingest.h"
#include "stream_config.h"

#include <stdio.h>
#include <string.h>

IngestStatus FileIngest_open(FileIngest *ingest, const char *path)
{
    if (ingest == NULL || path == NULL) {
        return INGEST_ERR;
    }

    memset(ingest, 0, sizeof(*ingest));
    strncpy(ingest->path, path, sizeof(ingest->path) - 1u);
    ingest->path[sizeof(ingest->path) - 1u] = '\0';

    ingest->fp = fopen(path, "rb");
    if (ingest->fp == NULL) {
        return INGEST_ERR;
    }

    return INGEST_OK;
}

IngestStatus FileIngest_pump_once(FileIngest *ingest, CircularBuffer *buf,
                                  size_t *pkg_len)
{
    unsigned char pkt[PKG_SIZE];
    size_t         n;
    CB_Status      st;

    if (ingest == NULL || buf == NULL || ingest->fp == NULL) {
        return INGEST_ERR;
    }

    n = fread(pkt, 1, PKG_SIZE, ingest->fp);
    if (n == 0) {
        if (feof(ingest->fp)) {
            return INGEST_EOF;
        }
        return INGEST_ERR;
    }

    st = Buffer_Write(buf, pkt, n);
    if (st == CB_ERR_FULL) {
        return INGEST_BUF_FULL;
    }
    if (st != CB_OK) {
        return INGEST_ERR;
    }

    if (pkg_len != NULL) {
        *pkg_len = n;
    }

    return INGEST_OK;
}

void FileIngest_close(FileIngest *ingest)
{
    if (ingest == NULL) {
        return;
    }

    if (ingest->fp != NULL) {
        fclose(ingest->fp);
        ingest->fp = NULL;
    }
}
