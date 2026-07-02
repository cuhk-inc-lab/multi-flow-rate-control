#include "packet.h"
#include "time_utils.h"

#include <stdlib.h>
#include <string.h>

typedef struct PacketAdoptCtx {
    PacketPayloadFreeFn free_fn;
    void               *free_ctx;
} PacketAdoptCtx;

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

    if (time_utils_now_mono(&pkt->enqueue_ts) != TU_OK) {
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

    if (time_utils_now_mono(&pkt->enqueue_ts) != TU_OK) {
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

PacketStatus packet_stamp_now(DataPacket *pkt)
{
    if (pkt == NULL) {
        return PKT_ERR_INVALID;
    }

    if (time_utils_now_mono(&pkt->enqueue_ts) != TU_OK) {
        return PKT_ERR_SYSTEM;
    }

    return PKT_OK;
}
