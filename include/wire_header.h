#ifndef WIRE_HEADER_H
#define WIRE_HEADER_H

/*
 * Shared wire UDP header (WGP1).
 *
 * Layout (network byte order), WIRE_HEADER_SIZE = 44:
 *   0-3   magic
 *   4     version (= WIRE_VERSION)
 *   5     type
 *   6     final_dst   (node id; relay destination check)
 *   7     ttl
 *   8-11  flow_id
 *   12-19 block_id
 *   20-21 shard_index
 *   22-23 shard_count
 *   24-25 valid_len
 *   26-27 payload_len
 *   28-35 encode_begin_ns
 *   36-43 encode_end_ns
 */

#include <stddef.h>
#include <stdint.h>

#define WIRE_MAGIC          0x57475031u /* WGP1 */
#define WIRE_VERSION        3u
#define WIRE_TYPE_DATA      1u
#define WIRE_TYPE_END       2u
#define WIRE_HEADER_SIZE    44u

/* Default hop budget for linear VM1->VM2->VM3->VM4 paths. */
#define WIRE_DEFAULT_TTL    8u
/* Conventional final destination node id for the lab topology. */
#define WIRE_DEFAULT_FINAL_DST 4u

typedef struct WireHeader {
    uint8_t  type;
    uint8_t  final_dst;
    uint8_t  ttl;
    uint32_t flow_id;
    uint64_t block_id;
    uint16_t shard_index;
    uint16_t shard_count;
    uint16_t valid_len;
    uint16_t payload_len;
    /* Sender CLOCK_REALTIME timestamps; synchronize peers before comparing. */
    uint64_t encode_begin_ns;
    uint64_t encode_end_ns;
} WireHeader;

void wire_header_encode(unsigned char out[WIRE_HEADER_SIZE],
                        const WireHeader *header);

/* Returns 0 on success, -1 on malformed / wrong version. */
int wire_header_decode(WireHeader *header,
                       const unsigned char *data,
                       size_t len);

/* Returns 1 when header.final_dst matches local_node_id. */
int wire_header_is_local(const WireHeader *header, uint8_t local_node_id);

#endif /* WIRE_HEADER_H */
