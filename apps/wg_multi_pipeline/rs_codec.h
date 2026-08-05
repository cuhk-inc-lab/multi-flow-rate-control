#ifndef RS_CODEC_H
#define RS_CODEC_H

#include "codec.h"

#include <stdint.h>

typedef enum RsRecoverMode {
    RS_RECOVER_LEGACY = 0,
    RS_RECOVER_MATRIX
} RsRecoverMode;

const Codec *RsCodec_get(void);

/*
 * Default is calibrated matrix recovery (erasure-only). Use
 * --rs-recover=legacy for the original per-column Berlekamp path.
 */
int RsCodec_set_recover_mode(RsRecoverMode mode);
RsRecoverMode RsCodec_get_recover_mode(void);

/* Test and benchmark hooks for the calibrated matrix repair plans. */
int RsCodec_prepare_matrix(void);
int RsCodec_matrix_ready(void);
uint64_t RsCodec_matrix_init_ns(void);

#endif /* RS_CODEC_H */
