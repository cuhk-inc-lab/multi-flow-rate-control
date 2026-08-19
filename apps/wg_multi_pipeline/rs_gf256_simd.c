#include "rs_gf256_simd.h"

#include <pthread.h>

static pthread_once_t rs_cpu_once = PTHREAD_ONCE_INIT;
static int rs_cpu_avx2;
static int rs_cpu_ssse3;

static void rs_gf256_encode_column_scalar(uint8_t *parity_base,
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

static void rs_detect_cpu(void)
{
#if (defined(__i386__) || defined(__x86_64__)) && defined(__GNUC__)
    /*
     * GCC's CPU dispatcher checks CPUID bits and, for AVX/AVX2, XGETBV OS
     * support. SSSE3 needs only CPUID; it is the Ivy Bridge (i7-3770) path.
     * AVX 1.0 does not add 256-bit integer shuffle, so it is not used here.
     */
    __builtin_cpu_init();
    rs_cpu_avx2 = __builtin_cpu_supports("avx2") != 0;
    rs_cpu_ssse3 = __builtin_cpu_supports("ssse3") != 0;
#else
    rs_cpu_avx2 = 0;
    rs_cpu_ssse3 = 0;
#endif
}

static int rs_cpu_ready(void)
{
    return pthread_once(&rs_cpu_once, rs_detect_cpu) == 0;
}

int rs_gf256_avx2_available(void)
{
    return rs_cpu_ready() && rs_cpu_avx2;
}

int rs_gf256_ssse3_available(void)
{
    return rs_cpu_ready() && rs_cpu_ssse3;
}

int rs_gf256_simd_available(void)
{
    return rs_gf256_avx2_available() || rs_gf256_ssse3_available();
}

void rs_gf256_encode_column(uint8_t *parity_base,
                            size_t parity_stride,
                            const uint8_t *source,
                            const uint8_t *nibble_tables,
                            size_t nibble_table_stride,
                            size_t parity_rows,
                            size_t length)
{
    if (rs_gf256_avx2_available()) {
        rs_gf256_encode_column_avx2(parity_base, parity_stride, source,
                                    nibble_tables, nibble_table_stride,
                                    parity_rows, length);
        return;
    }
    if (rs_gf256_ssse3_available()) {
        rs_gf256_encode_column_ssse3(parity_base, parity_stride, source,
                                     nibble_tables, nibble_table_stride,
                                     parity_rows, length);
        return;
    }
    rs_gf256_encode_column_scalar(parity_base, parity_stride, source,
                                  nibble_tables, nibble_table_stride,
                                  parity_rows, length);
}
