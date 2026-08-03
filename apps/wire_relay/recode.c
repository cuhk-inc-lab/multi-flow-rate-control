#include "recode.h"

#include <string.h>

int relay_recode_identity(const uint8_t *in, size_t in_len,
                          uint8_t *out, size_t out_cap, size_t *out_len,
                          const WireHeader *hdr, void *ctx)
{
    (void)hdr;
    (void)ctx;

    if (in == NULL || out == NULL || out_len == NULL || in_len > out_cap) {
        return -1;
    }
    if (in_len > 0) {
        memcpy(out, in, in_len);
    }
    *out_len = in_len;
    return 0;
}
