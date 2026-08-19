#include "rs_gf256_simd.h"

#if defined(__i386__) || defined(__x86_64__)

#include <tmmintrin.h>

void rs_gf256_encode_column_ssse3(uint8_t *parity_base,
                                  size_t parity_stride,
                                  const uint8_t *source,
                                  const uint8_t *nibble_tables,
                                  size_t nibble_table_stride,
                                  size_t parity_rows,
                                  size_t length)
{
    const __m128i nibble_mask = _mm_set1_epi8(0x0f);
    size_t offset = 0;
    size_t row;

    for (; offset + 16u <= length; offset += 16u) {
        __m128i input =
            _mm_loadu_si128((const __m128i *)(const void *)(source + offset));
        __m128i low_index = _mm_and_si128(input, nibble_mask);
        __m128i high_index =
            _mm_and_si128(_mm_srli_epi16(input, 4), nibble_mask);

        for (row = 0; row < parity_rows; row++) {
            const uint8_t *table = nibble_tables + row * nibble_table_stride;
            __m128i low_table =
                _mm_loadu_si128((const __m128i *)(const void *)table);
            __m128i high_table =
                _mm_loadu_si128((const __m128i *)(const void *)(table + 16u));
            __m128i product =
                _mm_xor_si128(_mm_shuffle_epi8(low_table, low_index),
                              _mm_shuffle_epi8(high_table, high_index));
            uint8_t *parity = parity_base + row * parity_stride + offset;
            __m128i current =
                _mm_loadu_si128((const __m128i *)(const void *)parity);

            _mm_storeu_si128((__m128i *)(void *)parity,
                             _mm_xor_si128(current, product));
        }
    }

    for (; offset < length; offset++) {
        uint8_t value = source[offset];

        for (row = 0; row < parity_rows; row++) {
            const uint8_t *table = nibble_tables + row * nibble_table_stride;

            parity_base[row * parity_stride + offset] ^=
                table[value & 0x0fu] ^ table[16u + (value >> 4)];
        }
    }
}

#else

void rs_gf256_encode_column_ssse3(uint8_t *parity_base,
                                  size_t parity_stride,
                                  const uint8_t *source,
                                  const uint8_t *nibble_tables,
                                  size_t nibble_table_stride,
                                  size_t parity_rows,
                                  size_t length)
{
    size_t offset;
    size_t row;

    for (offset = 0; offset < length; offset++) {
        uint8_t value = source[offset];

        for (row = 0; row < parity_rows; row++) {
            const uint8_t *table = nibble_tables + row * nibble_table_stride;

            parity_base[row * parity_stride + offset] ^=
                table[value & 0x0fu] ^ table[16u + (value >> 4)];
        }
    }
}

#endif
