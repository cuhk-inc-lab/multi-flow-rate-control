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

size_t Codec_input_block_size(const Codec *codec)
{
    if (codec == NULL || codec->vtable == NULL ||
        codec->vtable->input_block_size == NULL) {
        return 0;
    }
    return codec->vtable->input_block_size(codec);
}

size_t Codec_output_block_size(const Codec *codec)
{
    if (codec == NULL || codec->vtable == NULL ||
        codec->vtable->output_block_size == NULL) {
        return 0;
    }
    return codec->vtable->output_block_size(codec);
}

const Codec *Codec_get(CodecKind kind)
{
    switch (kind) {
    case CODEC_KIND_BLOCK:
        return BlockCodec_get();
    case CODEC_KIND_COPY:
        return CopyCodec_get();
    case CODEC_KIND_XOR_FEC:
        return XorFecCodec_get();
    case CODEC_KIND_NONE:
    default:
        return NULL;
    }
}
