#include "codec.h"

void Codec_encode(const Codec *codec, unsigned char *data, size_t len)
{
    if (codec == NULL || codec->vtable == NULL || codec->vtable->encode == NULL) {
        return;
    }
    codec->vtable->encode(codec, data, len);
}

void Codec_decode(const Codec *codec, unsigned char *data, size_t len)
{
    if (codec == NULL || codec->vtable == NULL || codec->vtable->decode == NULL) {
        return;
    }
    codec->vtable->decode(codec, data, len);
}
