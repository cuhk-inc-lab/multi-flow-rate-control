#ifndef STREAM_CONFIG_H
#define STREAM_CONFIG_H

#include "circular_buffer.h"

/*
 * Shard/payload size on the wire (plus 44 B WireHeader v3 per UDP datagram).
 * Wire v3 reuses the former reserved bytes at offsets 6-7 for final_dst + ttl;
 * offsets 8-43 and the payload layout are unchanged from v2.
 * 1400 keeps IP+UDP+header+payload under a typical 1500 B MTU while cutting
 * PPS vs the old MPEG-TS-sized 188 B shards.
 */
#define PKG_SIZE       1400u
#define PACKAGES_PER_DECODE_BLOCK  4u
#define PACKAGES_PER_ENCODE_BLOCK  8u
#define XOR_FEC_PARITY_SHARDS      1u
#define RS_FEC_DATA_SHARDS         4u
#define RS_FEC_PARITY_SHARDS       2u
#define RS_FEC_TOTAL_SHARDS        (RS_FEC_DATA_SHARDS + RS_FEC_PARITY_SHARDS)

#define DECODE_BLOCK   (PKG_SIZE * PACKAGES_PER_DECODE_BLOCK)
#define ENCODE_BLOCK   (PKG_SIZE * PACKAGES_PER_ENCODE_BLOCK)
#define XOR_FEC_ENCODE_BLOCK (DECODE_BLOCK + (PKG_SIZE * XOR_FEC_PARITY_SHARDS))
#define RS_FEC_ENCODE_BLOCK (RS_FEC_TOTAL_SHARDS * PKG_SIZE)
/* Absolute room for --codec rs: n <= 255 (GF(256) byte RS). */
#define CODEC_MAX_ENCODE_BLOCK (PKG_SIZE * 255u)

#define BUFFER_BLOCK_COUNT  2048u
#define BUFFER_BLOCK_SIZE   8192u
#define BUFFER_OVERFLOW_POLICY  CB_OVERFLOW_REJECT

#define MF_MAX_FLOWS        8u
#define MF_QUEUE_CAPACITY   2048u
#define MF_MIXED_CAPACITY   4096u

#endif /* STREAM_CONFIG_H */
