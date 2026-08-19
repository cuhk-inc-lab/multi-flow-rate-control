#ifndef RS_GF256_SIMD_H
#define RS_GF256_SIMD_H

#include <stddef.h>
#include <stdint.h>

/*
 * Encode one data-shard column into all parity rows.  Each coefficient owns
 * 32 bytes: products for low nibbles [0..15], followed by products for high
 * nibbles [0x00,0x10,..,0xf0].
 */
void rs_gf256_encode_column(uint8_t *parity_base,
                            size_t parity_stride,
                            const uint8_t *source,
                            const uint8_t *nibble_tables,
                            size_t nibble_table_stride,
                            size_t parity_rows,
                            size_t length);

void rs_gf256_encode_column_avx2(uint8_t *parity_base,
                                 size_t parity_stride,
                                 const uint8_t *source,
                                 const uint8_t *nibble_tables,
                                 size_t nibble_table_stride,
                                 size_t parity_rows,
                                 size_t length);

void rs_gf256_encode_column_ssse3(uint8_t *parity_base,
                                  size_t parity_stride,
                                  const uint8_t *source,
                                  const uint8_t *nibble_tables,
                                  size_t nibble_table_stride,
                                  size_t parity_rows,
                                  size_t length);

int rs_gf256_avx2_available(void);
int rs_gf256_ssse3_available(void);
int rs_gf256_simd_available(void);

#endif /* RS_GF256_SIMD_H */
