#ifndef XOR_FEC_CODEC_H
#define XOR_FEC_CODEC_H

#include "codec.h"

#include <stdbool.h>

/*
 * Systematic XOR FEC: four 188-byte data shards plus one parity shard.
 * It recovers exactly one missing shard when the remaining four are present.
 */
int XorFecCodec_recover_one(unsigned char *shards[5], const bool present[5]);

#endif /* XOR_FEC_CODEC_H */
