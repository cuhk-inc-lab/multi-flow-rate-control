#include "packet_framer.h"

#include "packet.h"

#include <stdlib.h>
#include <string.h>

static void framer_payload_free(void *payload, void *ctx)
{
    (void)ctx;
    free(payload);
}

PacketFramerStatus packet_framer_pump(CircularBuffer *src,
                                      FlowManager *mgr,
                                      uint32_t flow_id,
                                      size_t block_size,
                                      int stamp_now)
{
    unsigned char *block;
    DataPacket *pkt;
    FlowManagerStatus fm_st;
    CB_Status cb_st;

    if (src == NULL || mgr == NULL || block_size == 0) {
        return PF_ERR_INVALID;
    }

    if ((size_t)src->size < block_size) {
        return PF_ERR_EMPTY;
    }

    block = malloc(block_size);
    if (block == NULL) {
        return PF_ERR_IO;
    }

    cb_st = Buffer_Read(src, block, block_size);
    if (cb_st != CB_OK) {
        free(block);
        return PF_ERR_IO;
    }

    pkt = packet_adopt(flow_id, block, block_size, framer_payload_free, NULL);
    if (pkt == NULL) {
        free(block);
        return PF_ERR_IO;
    }

    if (stamp_now) {
        packet_stamp_now(pkt);
    }

    fm_st = flow_manager_push(mgr, &pkt);
    if (fm_st == FM_ERR_SHUTDOWN) {
        packet_free(pkt);
        return PF_ERR_SHUTDOWN;
    }
    if (fm_st != FM_OK) {
        packet_free(pkt);
        return PF_ERR_IO;
    }

    return PF_OK;
}
