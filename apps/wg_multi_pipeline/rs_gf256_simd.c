#include "rs_gf256_simd.h"

#include <pthread.h>

static pthread_once_t rs_cpu_once = PTHREAD_ONCE_INIT;
static int rs_cpu_avx2;

static void rs_detect_cpu(void)
{
#if (defined(__i386__) || defined(__x86_64__)) && defined(__GNUC__)
    /*
     * GCC's CPU dispatcher checks CPUID AVX/AVX2 bits and XGETBV OS support,
     * so a positive result means YMM instructions are safe to execute.
     */
    __builtin_cpu_init();
    rs_cpu_avx2 = __builtin_cpu_supports("avx2") != 0;
#else
    rs_cpu_avx2 = 0;
#endif
}

int rs_gf256_avx2_available(void)
{
    if (pthread_once(&rs_cpu_once, rs_detect_cpu) != 0) {
        return 0;
    }
    return rs_cpu_avx2;
}
