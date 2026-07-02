#include "flow_context.h"

#include <stdlib.h>
#include <string.h>

FlowContextStatus flow_context_init(FlowContext *ctx,
                                    uint32_t flow_id,
                                    size_t queue_capacity,
                                    int output_fd,
                                    FlowManager *owner)
{
    if (ctx == NULL || owner == NULL || queue_capacity == 0) {
        return FC_ERR_INVALID;
    }

    memset(ctx, 0, sizeof(*ctx));
    ctx->flow_id = flow_id;
    ctx->output_fd = output_fd;
    ctx->owner = owner;

    if (flow_buffer_init(&ctx->queue, queue_capacity) != FB_OK) {
        return FC_ERR_ALLOC;
    }

    return FC_OK;
}

void flow_context_set_encoder(FlowContext *ctx,
                              PacketEncodeFn encode_fn,
                              void *encode_ctx,
                              size_t scratch_cap)
{
    if (ctx == NULL) {
        return;
    }

    free(ctx->encode_scratch);
    ctx->encode_scratch = NULL;
    ctx->encode_scratch_cap = 0;
    ctx->encode_fn = encode_fn;
    ctx->encode_ctx = encode_ctx;

    if (encode_fn == NULL || scratch_cap == 0) {
        return;
    }

    ctx->encode_scratch = malloc(scratch_cap);
    if (ctx->encode_scratch == NULL) {
        ctx->encode_fn = NULL;
        ctx->encode_ctx = NULL;
        return;
    }

    ctx->encode_scratch_cap = scratch_cap;
}

void flow_context_destroy(FlowContext *ctx)
{
    if (ctx == NULL) {
        return;
    }

    flow_buffer_destroy(&ctx->queue);
    free(ctx->encode_scratch);
    ctx->encode_scratch = NULL;
    ctx->encode_scratch_cap = 0;
    ctx->encode_fn = NULL;
    ctx->encode_ctx = NULL;
    ctx->worker_started = 0;
}
