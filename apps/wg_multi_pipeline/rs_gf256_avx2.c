#include "rs_gf256_simd.h"

#if defined(__i386__) || defined(__x86_64__)

#include <immintrin.h>

void rs_gf256_encode_column_avx2(uint8_t *parity_base,
                                 size_t parity_stride,
                                 const uint8_t *source,
                                 const uint8_t *nibble_tables,
                                 size_t nibble_table_stride,
                                 size_t parity_rows,
                                 size_t length)
{
    const __m256i nibble_mask = _mm256_set1_epi8(0x0f);
    size_t offset = 0;
    size_t row;

    for (; offset + 32u <= length; offset += 32u) {
        __m256i input =
            _mm256_loadu_si256((const __m256i *)(source + offset));
        __m256i low_index = _mm256_and_si256(input, nibble_mask);
        __m256i high_index =
            _mm256_and_si256(_mm256_srli_epi16(input, 4), nibble_mask);

        for (row = 0; row < parity_rows; row++) {
            const uint8_t *table = nibble_tables + row * nibble_table_stride;
            __m128i low128 =
                _mm_loadu_si128((const __m128i *)(const void *)table);
            __m128i high128 =
                _mm_loadu_si128((const __m128i *)(const void *)(table + 16u));
            __m256i low_table = _mm256_broadcastsi128_si256(low128);
            __m256i high_table = _mm256_broadcastsi128_si256(high128);
            __m256i product =
                _mm256_xor_si256(_mm256_shuffle_epi8(low_table, low_index),
                                 _mm256_shuffle_epi8(high_table, high_index));
            uint8_t *parity = parity_base + row * parity_stride + offset;
            __m256i current =
                _mm256_loadu_si256((const __m256i *)(const void *)parity);

            _mm256_storeu_si256((__m256i *)(void *)parity,
                                _mm256_xor_si256(current, product));
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

void rs_gf256_encode_column_avx2(uint8_t *parity_base,
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
