#include "packet.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int monotonic_now(struct timespec *ts)
{
    if (ts == NULL) {
        return -1;
    }

    if (clock_gettime(CLOCK_MONOTONIC, ts) != 0) {
        return -1;
    }

    return 0;
}

DataPacket *packet_create(uint32_t flow_id, const void *data, size_t len)
{
    DataPacket *pkt;

    if (len > 0 && data == NULL) {
        return NULL;
    }

    pkt = calloc(1, sizeof(*pkt));
    if (pkt == NULL) {
        return NULL;
    }

    pkt->flow_id = flow_id;
    pkt->payload_len = len;

    if (len > 0) {
        pkt->payload = malloc(len);
        if (pkt->payload == NULL) {
            free(pkt);
            return NULL;
        }
        memcpy(pkt->payload, data, len);
    }

    if (monotonic_now(&pkt->enqueue_ts) != 0) {
        free(pkt->payload);
        free(pkt);
        return NULL;
    }

    return pkt;
}

DataPacket *packet_adopt(uint32_t flow_id,
                         unsigned char *payload,
                         size_t len,
                         PacketPayloadFreeFn free_fn,
                         void *free_ctx)
{
    DataPacket *pkt;
    PacketAdoptCtx *adopt;

    if (len > 0 && payload == NULL) {
        return NULL;
    }

    pkt = calloc(1, sizeof(*pkt));
    if (pkt == NULL) {
        return NULL;
    }

    adopt = malloc(sizeof(*adopt));
    if (adopt == NULL) {
        free(pkt);
        return NULL;
    }

    adopt->free_fn = free_fn;
    adopt->free_ctx = free_ctx;

    pkt->flow_id = flow_id;
    pkt->payload_len = len;
    pkt->payload = payload;
    pkt->user_data = adopt;

    if (monotonic_now(&pkt->enqueue_ts) != 0) {
        free(adopt);
        free(pkt);
        return NULL;
    }

    return pkt;
}

void packet_free(DataPacket *pkt)
{
    PacketAdoptCtx *adopt;

    if (pkt == NULL) {
        return;
    }

    adopt = pkt->user_data;
    if (adopt != NULL) {
        if (adopt->free_fn != NULL) {
            adopt->free_fn(pkt->payload, adopt->free_ctx);
        }
        free(adopt);
    } else {
        free(pkt->payload);
    }

    free(pkt);
}

int packet_stamp_now(DataPacket *pkt)
{
    if (pkt == NULL) {
        errno = EINVAL;
        return -1;
    }

    return monotonic_now(&pkt->enqueue_ts) == 0 ? 0 : -1;
}
