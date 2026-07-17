#include "codec.h"
#include "stream_config.h"
#include "xor_fec_codec.h"

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#define XOR_FEC_SHARD_COUNT \
    (PACKAGES_PER_DECODE_BLOCK + XOR_FEC_PARITY_SHARDS)

static void print_shard(const char *label, const unsigned char *shard)
{
    size_t i;

    printf("%-20s ascii: \"", label);
    for (i = 0; i < 8; i++) {
        unsigned char c = shard[i];

        putchar(c >= 32u && c <= 126u ? (int)c : '.');
    }
    printf("\"  hex:");
    for (i = 0; i < 8; i++) {
        printf(" %02X", shard[i]);
    }
    putchar('\n');
}

int main(void)
{
    const Codec *codec = XorFecCodec_get();
    unsigned char encoded[XOR_FEC_ENCODE_BLOCK] = {0};
    unsigned char original[DECODE_BLOCK];
    unsigned char shards[XOR_FEC_SHARD_COUNT][PKG_SIZE];
    unsigned char *shard_ptrs[XOR_FEC_SHARD_COUNT];
    bool present[XOR_FEC_SHARD_COUNT];
    const char labels[][8] = { "data A", "data B", "data C", "data D", "parity" };
    size_t shard;

    for (shard = 0; shard < PACKAGES_PER_DECODE_BLOCK; shard++) {
        memset(encoded + shard * PKG_SIZE, 'A' + (int)shard, PKG_SIZE);
    }
    memcpy(original, encoded, sizeof(original));

    printf("XOR FEC trace: 4 data shards + 1 parity shard (%u bytes each)\n\n",
           PKG_SIZE);
    puts("1. Original data shards");
    for (shard = 0; shard < PACKAGES_PER_DECODE_BLOCK; shard++) {
        print_shard(labels[shard], encoded + shard * PKG_SIZE);
    }

    Codec_encode(codec, encoded, sizeof(encoded));
    puts("\n2. After XOR FEC encode");
    for (shard = 0; shard < XOR_FEC_SHARD_COUNT; shard++) {
        print_shard(labels[shard], encoded + shard * PKG_SIZE);
        memcpy(shards[shard], encoded + shard * PKG_SIZE, PKG_SIZE);
        shard_ptrs[shard] = shards[shard];
        present[shard] = true;
    }
    puts("   parity byte = 0x41 XOR 0x42 XOR 0x43 XOR 0x44 = 0x04");

    puts("\n3. Simulated network loss: data C is absent");
    memset(shards[2], 0, PKG_SIZE);
    present[2] = false;
    puts("data C               <missing>");

    if (XorFecCodec_recover_one(shard_ptrs, present) != 1) {
        fputs("FEC recovery failed\n", stderr);
        return 1;
    }
    present[2] = true;

    puts("\n4. XOR recovery result");
    print_shard("recovered data C", shards[2]);
    if (memcmp(shards[2], original + 2 * PKG_SIZE, PKG_SIZE) != 0) {
        fputs("Recovered shard does not match the original\n", stderr);
        return 1;
    }

    for (shard = 0; shard < XOR_FEC_SHARD_COUNT; shard++) {
        memcpy(encoded + shard * PKG_SIZE, shards[shard], PKG_SIZE);
    }
    Codec_decode(codec, encoded, sizeof(encoded));

    puts("\n5. Decode output (the first four systematic shards)");
    for (shard = 0; shard < PACKAGES_PER_DECODE_BLOCK; shard++) {
        print_shard(labels[shard], encoded + shard * PKG_SIZE);
    }

    if (memcmp(encoded, original, sizeof(original)) != 0) {
        fputs("Decoded payload does not match the original\n", stderr);
        return 1;
    }

    puts("\nPASS: one missing data shard was reconstructed and decoded bytes match.");
    return 0;
}
