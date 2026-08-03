#include "wire_header.h"

#include <arpa/inet.h>
#include <string.h>

static uint64_t host_to_be64(uint64_t value)
{
    uint32_t high = htonl((uint32_t)(value >> 32));
    uint32_t low = htonl((uint32_t)value);

    return ((uint64_t)low << 32) | high;
}

static uint64_t be64_to_host(uint64_t value)
{
    uint32_t high = ntohl((uint32_t)value);
    uint32_t low = ntohl((uint32_t)(value >> 32));

    return ((uint64_t)high << 32) | low;
}

void wire_header_encode(unsigned char out[WIRE_HEADER_SIZE],
                        const WireHeader *header)
{
    uint32_t value32;
    uint64_t value64;
    uint16_t value16;

    if (out == NULL || header == NULL) {
        return;
    }

    value32 = htonl(WIRE_MAGIC);
    memcpy(out, &value32, sizeof(value32));
    out[4] = WIRE_VERSION;
    out[5] = header->type;
    out[6] = header->final_dst;
    out[7] = header->ttl;

    value32 = htonl(header->flow_id);
    memcpy(out + 8, &value32, sizeof(value32));
    value64 = host_to_be64(header->block_id);
    memcpy(out + 12, &value64, sizeof(value64));
    value16 = htons(header->shard_index);
    memcpy(out + 20, &value16, sizeof(value16));
    value16 = htons(header->shard_count);
    memcpy(out + 22, &value16, sizeof(value16));
    value16 = htons(header->valid_len);
    memcpy(out + 24, &value16, sizeof(value16));
    value16 = htons(header->payload_len);
    memcpy(out + 26, &value16, sizeof(value16));
    value64 = host_to_be64(header->encode_begin_ns);
    memcpy(out + 28, &value64, sizeof(value64));
    value64 = host_to_be64(header->encode_end_ns);
    memcpy(out + 36, &value64, sizeof(value64));
}

int wire_header_decode(WireHeader *header,
                       const unsigned char *data,
                       size_t len)
{
    uint32_t value32;
    uint64_t value64;
    uint16_t value16;

    if (header == NULL || data == NULL || len < WIRE_HEADER_SIZE) {
        return -1;
    }

    memcpy(&value32, data, sizeof(value32));
    if (ntohl(value32) != WIRE_MAGIC || data[4] != WIRE_VERSION) {
        return -1;
    }

    header->type = data[5];
    header->final_dst = data[6];
    header->ttl = data[7];
    memcpy(&value32, data + 8, sizeof(value32));
    header->flow_id = ntohl(value32);
    memcpy(&value64, data + 12, sizeof(value64));
    header->block_id = be64_to_host(value64);
    memcpy(&value16, data + 20, sizeof(value16));
    header->shard_index = ntohs(value16);
    memcpy(&value16, data + 22, sizeof(value16));
    header->shard_count = ntohs(value16);
    memcpy(&value16, data + 24, sizeof(value16));
    header->valid_len = ntohs(value16);
    memcpy(&value16, data + 26, sizeof(value16));
    header->payload_len = ntohs(value16);
    memcpy(&value64, data + 28, sizeof(value64));
    header->encode_begin_ns = be64_to_host(value64);
    memcpy(&value64, data + 36, sizeof(value64));
    header->encode_end_ns = be64_to_host(value64);
    return 0;
}

int wire_header_is_local(const WireHeader *header, uint8_t local_node_id)
{
    if (header == NULL || local_node_id == 0) {
        return 0;
    }
    return header->final_dst == local_node_id;
}
