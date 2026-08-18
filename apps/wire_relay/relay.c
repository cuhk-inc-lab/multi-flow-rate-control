#include "relay.h"

#include "egress_queue.h"
#include "relay_deferred.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <netdb.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define RELAY_UDP_SOCKBUF (8 * 1024 * 1024)

/*
 * TEST ONLY build switch for HOL baseline (Phase 0 single-RX behavior):
 *   make wire-relay-hol-baseline  → -DRELAY_TEST_INLINE_RX=1
 * Production wire_relay keeps deferred RX (default 0).
 */
#ifndef RELAY_TEST_INLINE_RX
#define RELAY_TEST_INLINE_RX 0
#endif

struct RelayCtx {
    RelayConfig                 config;
    EgressQueue                 egress;
    RelayDeferredHub            deferred;
    int                         deferred_inited;
    GenerationCache             gen_cache;
    int                         cache_enabled;
    pthread_mutex_t             ingress_mu;
    pthread_cond_t              ingress_idle;
    int                         inject_in_flight;
    int                         ingress_idle_inited;
    pthread_t                   tx_thread;
    int                         tx_started;
    pthread_t                   processing_thread;
    int                         processing_started;
    pthread_t                   source_thread;
    int                         source_started;
    volatile sig_atomic_t       source_done;
    LocalSourceStats            source_stats;
    int                         source_result;
    uint64_t                    last_activity_ns;
    int                         listen_sock;
    int                         send_sock;
    struct sockaddr_storage     next_hop;
    socklen_t                   next_hop_len;
    RelayTxCaptureFn            tx_capture_fn;
    void                       *tx_capture_ctx;
    RelayFlowStats              per_flow[RELAY_MAX_FLOWS];
    RelayFlowStats              total;
    volatile sig_atomic_t       stop;
};

static RelayCtx *g_signal_ctx = NULL;

static void relay_on_signal(int sig)
{
    (void)sig;
    if (g_signal_ctx != NULL) {
        g_signal_ctx->stop = 1;
    }
}

uint64_t relay_mono_ns(void)
{
    struct timespec now;

    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0) {
        return 0;
    }
    return (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
}

static void set_udp_buffers(int sock)
{
    int buf = RELAY_UDP_SOCKBUF;

    if (sock < 0) {
        return;
    }
    (void)setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf));
    (void)setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf));
}

static int open_listen_socket(uint16_t port)
{
    int sock;
    struct sockaddr_in address;

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("wire-relay: socket");
        return -1;
    }
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port);
    if (bind(sock, (struct sockaddr *)&address, sizeof(address)) != 0) {
        perror("wire-relay: bind");
        close(sock);
        return -1;
    }
    set_udp_buffers(sock);
    return sock;
}

static int resolve_next_hop(const char *host, uint16_t port,
                            struct sockaddr_storage *address,
                            socklen_t *address_len)
{
    struct addrinfo hints;
    struct addrinfo *result = NULL;
    struct addrinfo *entry;
    char port_text[16];
    int rc;

    if (host == NULL || address == NULL || address_len == NULL || port == 0) {
        return -1;
    }
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    snprintf(port_text, sizeof(port_text), "%u", (unsigned)port);
    rc = getaddrinfo(host, port_text, &hints, &result);
    if (rc != 0) {
        fprintf(stderr, "wire-relay: getaddrinfo(%s): %s\n", host,
                gai_strerror(rc));
        return -1;
    }
    for (entry = result; entry != NULL; entry = entry->ai_next) {
        if (entry->ai_addrlen > sizeof(*address)) {
            continue;
        }
        memcpy(address, entry->ai_addr, entry->ai_addrlen);
        *address_len = (socklen_t)entry->ai_addrlen;
        freeaddrinfo(result);
        return 0;
    }
    freeaddrinfo(result);
    return -1;
}

static RelayFlowStats *flow_stats_slot(RelayCtx *ctx, uint32_t flow_id)
{
    if (flow_id >= RELAY_MAX_FLOWS) {
        return &ctx->per_flow[RELAY_MAX_FLOWS - 1u];
    }
    return &ctx->per_flow[flow_id];
}

static void sync_cache_stats_to_total(RelayCtx *ctx)
{
    const GenerationCacheStats *gs;

    if (!ctx->cache_enabled) {
        return;
    }
    gs = generation_cache_stats(&ctx->gen_cache);
    if (gs == NULL) {
        return;
    }
    ctx->total.gen_created = gs->gen_created;
    ctx->total.gen_ready = gs->gen_ready;
    ctx->total.gen_timeout = gs->gen_timeout;
    ctx->total.gen_evicted = gs->gen_evicted;
    ctx->total.gen_admission_failed = gs->gen_admission_failed;
    ctx->total.gen_metadata_mismatch = gs->gen_metadata_mismatch;
    ctx->total.gen_duplicate = gs->gen_duplicate;
}

static void print_stats(const RelayCtx *ctx)
{
    uint32_t i;
    const GenerationCacheStats *gs = NULL;
    EgressQueueStats eq;
    RelayDeferredHubStats ds;

    if (ctx->cache_enabled) {
        gs = generation_cache_stats(&ctx->gen_cache);
    }
    egress_queue_stats_snapshot(&ctx->egress, &eq);
    memset(&ds, 0, sizeof(ds));
    if (ctx->deferred_inited) {
        relay_deferred_hub_stats_snapshot(&ctx->deferred, &ds);
    }

    fprintf(stderr,
            "wire-relay: local_node_id=%u summary egress_capacity=%zu "
            "egress_wait_ms=%u deferred_per_flow=%zu deferred_total=%zu "
            "max_active_flows=%u rx=%llu forward=%llu "
            "local=%llu drop_ttl=%llu drop_malformed=%llu drop_send=%llu "
            "drop_egress_full=%llu drop_egress_timeout=%llu "
            "drop_deferred_flow=%llu drop_deferred_total=%llu "
            "drop_deferred_table=%llu "
            "inject_ok=%llu inject_reject_loopback=%llu "
            "egress_immediate=%llu egress_waited=%llu egress_wait_ns_total=%llu "
            "egress_wait_ns_max=%llu egress_high_watermark=%llu "
            "deferred_hwm=%llu\n",
            (unsigned)ctx->config.local_node_id,
            ctx->config.egress_capacity,
            (unsigned)ctx->config.egress_wait_ms,
            ctx->config.deferred_per_flow,
            ctx->config.deferred_total,
            (unsigned)ctx->config.max_active_flows,
            (unsigned long long)ctx->total.rx,
            (unsigned long long)ctx->total.forward,
            (unsigned long long)ctx->total.local_deliver,
            (unsigned long long)ctx->total.drop_ttl,
            (unsigned long long)ctx->total.drop_malformed,
            (unsigned long long)ctx->total.drop_send,
            (unsigned long long)ctx->total.drop_egress_full,
            (unsigned long long)ctx->total.drop_egress_timeout,
            (unsigned long long)ds.drop_overflow_flow,
            (unsigned long long)ds.drop_overflow_total,
            (unsigned long long)ds.drop_table_full,
            (unsigned long long)ctx->total.inject_ok,
            (unsigned long long)ctx->total.inject_reject_loopback,
            (unsigned long long)eq.enqueue_immediate,
            (unsigned long long)eq.enqueue_waited,
            (unsigned long long)eq.wait_ns_total,
            (unsigned long long)eq.wait_ns_max,
            (unsigned long long)eq.high_watermark,
            (unsigned long long)ds.high_watermark);
    if (gs != NULL) {
        fprintf(stderr,
                "wire-relay: gen_cache created=%llu ready=%llu timeout=%llu "
                "evicted=%llu admission_failed=%llu mismatch=%llu "
                "duplicate=%llu bytes_cur=%llu bytes_peak=%llu live=%zu\n",
                (unsigned long long)gs->gen_created,
                (unsigned long long)gs->gen_ready,
                (unsigned long long)gs->gen_timeout,
                (unsigned long long)gs->gen_evicted,
                (unsigned long long)gs->gen_admission_failed,
                (unsigned long long)gs->gen_metadata_mismatch,
                (unsigned long long)gs->gen_duplicate,
                (unsigned long long)gs->gen_cached_bytes_current,
                (unsigned long long)gs->gen_cached_bytes_peak,
                generation_cache_count(&ctx->gen_cache));
    }
    for (i = 0; i < RELAY_MAX_FLOWS; i++) {
        const RelayFlowStats *s = &ctx->per_flow[i];

        if (s->rx == 0 && s->forward == 0 && s->local_deliver == 0 &&
            s->drop_ttl == 0 && s->drop_malformed == 0 && s->drop_send == 0 &&
            s->drop_egress_full == 0 && s->drop_egress_timeout == 0 &&
            s->inject_ok == 0 &&
            s->inject_reject_loopback == 0) {
            continue;
        }
        fprintf(stderr,
                "wire-relay: flow_id=%u rx=%llu forward=%llu local=%llu "
                "drop_ttl=%llu drop_malformed=%llu drop_send=%llu "
                "drop_egress_full=%llu drop_egress_timeout=%llu\n",
                i,
                (unsigned long long)s->rx,
                (unsigned long long)s->forward,
                (unsigned long long)s->local_deliver,
                (unsigned long long)s->drop_ttl,
                (unsigned long long)s->drop_malformed,
                (unsigned long long)s->drop_send,
                (unsigned long long)s->drop_egress_full,
                (unsigned long long)s->drop_egress_timeout);
        if (gs != NULL && i < 8u && gs->per_flow_created[i] != 0) {
            fprintf(stderr, "wire-relay: flow_id=%u gen_created=%llu\n", i,
                    (unsigned long long)gs->per_flow_created[i]);
        }
    }
}

static void apply_cache_config(GenerationCacheConfig *out,
                               const RelayConfig *config)
{
    generation_cache_config_defaults(out);
    if (config->gen_timeout_ms != 0) {
        out->gen_timeout_ms = config->gen_timeout_ms;
    }
    if (config->max_gens_global != 0) {
        out->max_gens_global = config->max_gens_global;
    }
    if (config->max_gens_per_flow != 0) {
        out->max_gens_per_flow = config->max_gens_per_flow;
    }
    if (config->max_cache_bytes != 0) {
        out->max_cache_bytes = config->max_cache_bytes;
    }
}

/*
 * Phase B forward: built under ingress_mu; enqueued outside ingress_mu.
 * relay_apply_forward_pending() owns freeing pkt.datagram on all paths.
 */
typedef struct ForwardPending {
    int               active;
    EgressPacket      pkt;
    RelayFlowStats   *slot;
    RelayPacketSource source;
} ForwardPending;

static void prepare_forward_pending(ForwardPending *pending,
                                    RelayFlowStats *slot,
                                    uint8_t **datagram_owned, size_t len,
                                    const WireHeader *header,
                                    RelayPacketSource source)
{
    memset(pending, 0, sizeof(*pending));
    pending->active = 1;
    pending->slot = slot;
    pending->source = source;
    pending->pkt.datagram = *datagram_owned;
    pending->pkt.len = len;
    pending->pkt.flow_id = header->flow_id;
    pending->pkt.generation_id = header->block_id;
    pending->pkt.enqueue_ns = relay_mono_ns();
    *datagram_owned = NULL;
}

/*
 * Timed/try egress enqueue outside ingress_mu.
 *
 * Ownership: on success pkt.datagram moves into EgressQueue (NULLed).
 * On drop/timeout/shutdown/error, relay_apply_forward_pending frees
 * pending->pkt.datagram and always clears pending->pkt.datagram before return.
 */
static void relay_apply_forward_pending(RelayCtx *ctx, ForwardPending *pending)
{
    EgressStatus est;

    if (ctx == NULL || pending == NULL || !pending->active) {
        return;
    }

    if (ctx->config.egress_wait_ms == 0) {
        est = egress_queue_try_enqueue(&ctx->egress, &pending->pkt);
        if (est == EGRESS_OK) {
            if (pending->source == RELAY_SRC_LOCAL_ENCODER) {
                pending->slot->inject_ok++;
                ctx->total.inject_ok++;
            }
        } else {
            pending->slot->drop_egress_full++;
            ctx->total.drop_egress_full++;
            free(pending->pkt.datagram);
            pending->pkt.datagram = NULL;
        }
    } else {
        est = egress_queue_timed_enqueue(&ctx->egress, &pending->pkt,
                                         ctx->config.egress_wait_ms);
        if (est == EGRESS_OK) {
            if (pending->source == RELAY_SRC_LOCAL_ENCODER) {
                pending->slot->inject_ok++;
                ctx->total.inject_ok++;
            }
        } else if (est == EGRESS_ERR_TIMEOUT) {
            pending->slot->drop_egress_timeout++;
            ctx->total.drop_egress_timeout++;
            free(pending->pkt.datagram);
            pending->pkt.datagram = NULL;
        } else {
            if (est == EGRESS_ERR_SHUTDOWN || est == EGRESS_ERR_INVALID ||
                est == EGRESS_ERR_FULL) {
                /* FULL should not occur; shutdown during teardown. */
            }
            free(pending->pkt.datagram);
            pending->pkt.datagram = NULL;
        }
    }

    pending->active = 0;
    pending->pkt.datagram = NULL;
    pending->pkt.len = 0;
}

/*
 * Local delivery selected under ingress_mu, executed after unlock (P0A).
 * When active != 0, caller owns datagram and must free it after callback.
 */
typedef struct DeferredLocalDelivery {
    int              active;
    RelayDeliveryFn  fn;
    void            *ctx;
    WireHeader       header;
    uint8_t         *datagram;
    size_t           len;
} DeferredLocalDelivery;

static void run_deferred_local_delivery(DeferredLocalDelivery *deferred)
{
    if (deferred == NULL || !deferred->active) {
        return;
    }
    if (deferred->fn != NULL && deferred->datagram != NULL) {
        (void)deferred->fn(deferred->datagram, deferred->len, &deferred->header,
                           deferred->ctx);
    }
    free(deferred->datagram);
    deferred->datagram = NULL;
    deferred->active = 0;
    deferred->fn = NULL;
    deferred->ctx = NULL;
    deferred->len = 0;
}

/*
 * Takes ownership of *datagram_owned on all paths that do not defer local
 * delivery (frees or moves to egress). When deferred->active is set, ownership
 * of the local datagram moves into *deferred for the caller to free after the
 * callback (outside ingress_mu).
 *
 * Caller must hold ingress_mu. Must NOT invoke delivery_fn here.
 * Does not enqueue to EgressQueue; sets *forward_pending when a packet should
 * be forwarded after ingress_mu is released.
 */
static RelayIngressStatus ingress_submit_owned(RelayCtx *ctx,
                                               uint8_t **datagram_owned,
                                               size_t len,
                                               RelayPacketSource source,
                                               DeferredLocalDelivery *deferred,
                                               ForwardPending *forward_pending)
{
    WireHeader header;
    RelayFlowStats *slot;
    uint8_t *datagram;
    uint8_t *recode_out = NULL;
    size_t out_len;
    uint64_t now_ns;
    GenerationEntry *gen = NULL;
    GenerationInsertStatus insert_st = GEN_INSERT_INVALID;
    RelayProcessAction action = RELAY_PROCESS_CONTINUE_FORWARD;

    if (deferred != NULL) {
        memset(deferred, 0, sizeof(*deferred));
    }
    if (forward_pending != NULL) {
        memset(forward_pending, 0, sizeof(*forward_pending));
    }

    if (ctx == NULL || datagram_owned == NULL || *datagram_owned == NULL ||
        len < WIRE_HEADER_SIZE) {
        if (datagram_owned != NULL && *datagram_owned != NULL) {
            free(*datagram_owned);
            *datagram_owned = NULL;
        }
        return RELAY_INGRESS_ERR_INVALID;
    }

    datagram = *datagram_owned;
    *datagram_owned = NULL;

    if (wire_header_decode(&header, datagram, len) != 0) {
        ctx->total.drop_malformed++;
        free(datagram);
        return RELAY_INGRESS_OK;
    }

    slot = flow_stats_slot(ctx, header.flow_id);
    slot->rx++;
    ctx->total.rx++;
    now_ns = relay_mono_ns();
    ctx->last_activity_ns = now_ns;

    if (source == RELAY_SRC_LOCAL_ENCODER &&
        wire_header_is_local(&header, ctx->config.local_node_id) &&
        ctx->config.reject_local_encoder_loopback) {
        slot->inject_reject_loopback++;
        ctx->total.inject_reject_loopback++;
        free(datagram);
        return RELAY_INGRESS_ERR_LOOPBACK;
    }

    if (header.ttl == 0) {
        slot->drop_ttl++;
        ctx->total.drop_ttl++;
        free(datagram);
        return RELAY_INGRESS_OK;
    }

    if (wire_header_is_local(&header, ctx->config.local_node_id)) {
        slot->local_deliver++;
        ctx->total.local_deliver++;
        /*
         * Select local delivery under ingress_mu; run callback after unlock.
         * Do not TTL--, cache, enqueue, or TX.
         */
        if (ctx->config.delivery_fn != NULL && deferred != NULL) {
            deferred->active = 1;
            deferred->fn = ctx->config.delivery_fn;
            deferred->ctx = ctx->config.delivery_ctx;
            deferred->header = header;
            deferred->datagram = datagram;
            deferred->len = len;
            return RELAY_INGRESS_OK;
        }
        free(datagram);
        return RELAY_INGRESS_OK;
    }

    if (header.ttl <= 1) {
        slot->drop_ttl++;
        ctx->total.drop_ttl++;
        free(datagram);
        return RELAY_INGRESS_OK;
    }

    /* TTL rewrite must land in the on-wire datagram bytes. */
    header.ttl = (uint8_t)(header.ttl - 1u);
    wire_header_encode(datagram, &header);

    out_len = len;
    if (ctx->config.recode_fn != NULL) {
        recode_out = malloc(RELAY_MAX_DATAGRAM);
        if (recode_out == NULL) {
            free(datagram);
            return RELAY_INGRESS_ERR_ALLOC;
        }
        if (ctx->config.recode_fn(datagram, len, recode_out, RELAY_MAX_DATAGRAM,
                                  &out_len, &header,
                                  ctx->config.recode_ctx) != 0 ||
            out_len == 0 || out_len > RELAY_MAX_DATAGRAM) {
            slot->drop_malformed++;
            ctx->total.drop_malformed++;
            free(datagram);
            free(recode_out);
            return RELAY_INGRESS_OK;
        }
        free(datagram);
        datagram = recode_out;
        recode_out = NULL;
        if (wire_header_decode(&header, datagram, out_len) != 0) {
            slot->drop_malformed++;
            ctx->total.drop_malformed++;
            free(datagram);
            return RELAY_INGRESS_OK;
        }
    }

    if (header.type != WIRE_TYPE_DATA) {
        /* END/control: never enter GenerationCache. Expire stale gens first. */
        if (ctx->cache_enabled && header.type == WIRE_TYPE_END) {
            (void)generation_cache_expire(&ctx->gen_cache, now_ns,
                                          (int32_t)header.flow_id);
            sync_cache_stats_to_total(ctx);
        }
        if (forward_pending != NULL) {
            prepare_forward_pending(forward_pending, slot, &datagram, out_len,
                                    &header, source);
        } else {
            free(datagram);
        }
        return RELAY_INGRESS_OK;
    }

    /* DATA path */
    if (ctx->cache_enabled) {
        insert_st = generation_cache_insert(&ctx->gen_cache, &header, datagram,
                                            out_len, now_ns, &gen);
        sync_cache_stats_to_total(ctx);
        if (ctx->config.process_fn != NULL) {
            action = ctx->config.process_fn(&header, datagram, out_len, gen,
                                            insert_st, ctx->config.process_ctx);
        }
        /*
         * Default Phase-2 policy: always opaque-forward the current packet
         * (including duplicate / mismatch / admission_failed). Cache holds an
         * independent copy when insert succeeded.
         */
        (void)insert_st;
        /*
         * Phase 3A reserved hook. Non-OPAQUE is not implemented yet: log once
         * via stderr and keep opaque forward so behavior stays unchanged.
         */
        if (ctx->config.decode_reencode_fn != NULL) {
            RelayDecodeReencodeAction dra;

            dra = ctx->config.decode_reencode_fn(
                &header, datagram, out_len, gen, insert_st,
                NULL, NULL, ctx->config.decode_reencode_ctx);
            if (dra != RELAY_DECODE_REENCODE_OPAQUE) {
                static int warned_3a;

                if (!warned_3a) {
                    fprintf(stderr,
                            "wire-relay: decode_reencode non-OPAQUE ignored "
                            "(Phase 3A not implemented); opaque forward\n");
                    warned_3a = 1;
                }
            }
        }
    } else if (ctx->config.decode_reencode_fn != NULL) {
        /*
         * Hook may still observe packets without cache; gen is NULL.
         * Stub / future 3A must return OPAQUE until implemented.
         */
        (void)ctx->config.decode_reencode_fn(
            &header, datagram, out_len, NULL, GEN_INSERT_INVALID, NULL, NULL,
            ctx->config.decode_reencode_ctx);
    }

    if (action == RELAY_PROCESS_DROP) {
        free(datagram);
        return RELAY_INGRESS_OK;
    }

    if (forward_pending != NULL) {
        prepare_forward_pending(forward_pending, slot, &datagram, out_len,
                                &header, source);
    } else {
        free(datagram);
    }
    return RELAY_INGRESS_OK;
}

RelayIngressStatus relay_inject_wire_datagram(RelayCtx *ctx,
                                              const uint8_t *datagram,
                                              size_t len)
{
    uint8_t *owned;
    RelayIngressStatus st;
    DeferredLocalDelivery deferred;
    ForwardPending forward_pending;

    if (ctx == NULL || datagram == NULL || len < WIRE_HEADER_SIZE ||
        len > RELAY_MAX_DATAGRAM) {
        return RELAY_INGRESS_ERR_INVALID;
    }

    owned = malloc(len);
    if (owned == NULL) {
        return RELAY_INGRESS_ERR_ALLOC;
    }
    memcpy(owned, datagram, len);
    memset(&deferred, 0, sizeof(deferred));
    memset(&forward_pending, 0, sizeof(forward_pending));

    pthread_mutex_lock(&ctx->ingress_mu);
    if (ctx->stop) {
        pthread_mutex_unlock(&ctx->ingress_mu);
        free(owned);
        return RELAY_INGRESS_ERR_SHUTDOWN;
    }
    ctx->inject_in_flight++;
    st = ingress_submit_owned(ctx, &owned, len, RELAY_SRC_LOCAL_ENCODER,
                              &deferred, &forward_pending);
    /*
     * P0A: release ingress_mu before local delivery (decode/I/O). Keep
     * inject_in_flight elevated so harness close still waits for callback.
     * Lock order: never take hub->mu while holding ingress_mu.
     */
    if (deferred.active) {
        pthread_mutex_unlock(&ctx->ingress_mu);
        run_deferred_local_delivery(&deferred);
        pthread_mutex_lock(&ctx->ingress_mu);
    }
    if (forward_pending.active) {
        pthread_mutex_unlock(&ctx->ingress_mu);
        relay_apply_forward_pending(ctx, &forward_pending);
        pthread_mutex_lock(&ctx->ingress_mu);
    }
    ctx->inject_in_flight--;
    if (ctx->inject_in_flight == 0) {
        pthread_cond_broadcast(&ctx->ingress_idle);
    }
    pthread_mutex_unlock(&ctx->ingress_mu);
    return st;
}

static void *tx_worker_main(void *arg);

static void sync_deferred_drops_to_total(RelayCtx *ctx)
{
    RelayDeferredHubStats ds;

    if (ctx == NULL || !ctx->deferred_inited) {
        return;
    }
    relay_deferred_hub_stats_snapshot(&ctx->deferred, &ds);
    ctx->total.drop_deferred_overflow_flow = ds.drop_overflow_flow;
    ctx->total.drop_deferred_overflow_total = ds.drop_overflow_total;
    ctx->total.drop_deferred_table_full = ds.drop_table_full;
}

#if !RELAY_TEST_INLINE_RX
/*
 * Minimal RX parse via wire_header_decode (length / magic / version / endian).
 * Does not perform destination, TTL, cache, or recode work.
 */
static int relay_parse_flow_id_min(const uint8_t *datagram, size_t len,
                                   uint32_t *flow_id_out)
{
    WireHeader header;

    if (flow_id_out == NULL || datagram == NULL) {
        return -1;
    }
    if (wire_header_decode(&header, datagram, len) != 0) {
        return -1;
    }
    *flow_id_out = header.flow_id;
    return 0;
}

static void account_deferred_push_fail(RelayCtx *ctx, RelayDeferredStatus st)
{
    /* Hub already counted the matching drop_* under deferred.mu. */
    (void)ctx;
    (void)st;
}

/*
 * RX fast path: enqueue owned datagram into deferred hub. Never waits on
 * egress. On any failure, frees ownership. Brief ingress_mu only for
 * malformed accounting (not for submit / egress).
 */
static void rx_enqueue_datagram(RelayCtx *ctx, uint8_t **datagram_owned,
                                size_t len)
{
    RelayDeferredPacket pkt;
    RelayDeferredStatus st;
    uint32_t flow_id = 0;

    if (ctx == NULL || datagram_owned == NULL || *datagram_owned == NULL) {
        return;
    }

    if (len < WIRE_HEADER_SIZE || len > RELAY_MAX_DATAGRAM ||
        relay_parse_flow_id_min(*datagram_owned, len, &flow_id) != 0) {
        pthread_mutex_lock(&ctx->ingress_mu);
        ctx->total.drop_malformed++;
        pthread_mutex_unlock(&ctx->ingress_mu);
        free(*datagram_owned);
        *datagram_owned = NULL;
        return;
    }

    memset(&pkt, 0, sizeof(pkt));
    pkt.datagram = *datagram_owned;
    pkt.len = len;
    pkt.flow_id = flow_id;
    pkt.enqueue_ns = relay_mono_ns();
    *datagram_owned = NULL;

    st = relay_deferred_hub_try_push(&ctx->deferred, &pkt);
    if (st != RELAY_DEFERRED_OK) {
        account_deferred_push_fail(ctx, st);
        free(pkt.datagram);
        pkt.datagram = NULL;
    }
}
#endif /* !RELAY_TEST_INLINE_RX */

static void process_one_deferred_packet(RelayCtx *ctx, RelayDeferredPacket *pkt)
{
    DeferredLocalDelivery deferred;
    ForwardPending forward_pending;
    uint8_t *owned;

    if (ctx == NULL || pkt == NULL || pkt->datagram == NULL) {
        return;
    }

    owned = pkt->datagram;
    pkt->datagram = NULL;
    memset(&deferred, 0, sizeof(deferred));
    memset(&forward_pending, 0, sizeof(forward_pending));

    pthread_mutex_lock(&ctx->ingress_mu);
    (void)ingress_submit_owned(ctx, &owned, pkt->len, RELAY_SRC_PREVIOUS_NODE,
                               &deferred, &forward_pending);
    pthread_mutex_unlock(&ctx->ingress_mu);

    run_deferred_local_delivery(&deferred);
    relay_apply_forward_pending(ctx, &forward_pending);
    if (owned != NULL) {
        free(owned);
        owned = NULL;
    }
}

static void *processing_worker_main(void *arg)
{
    RelayCtx *ctx = arg;

    while (1) {
        RelayDeferredStatus wst;

        wst = relay_deferred_hub_wait(&ctx->deferred);
        if (wst == RELAY_DEFERRED_ERR_SHUTDOWN) {
            break;
        }
        if (wst != RELAY_DEFERRED_OK) {
            break;
        }

        while (1) {
            RelayDeferredPacket pkt;
            RelayDeferredStatus pst;

            pst = relay_deferred_hub_try_pop(&ctx->deferred, &pkt);
            if (pst != RELAY_DEFERRED_OK) {
                break;
            }
            process_one_deferred_packet(ctx, &pkt);
            if (pkt.datagram != NULL) {
                free(pkt.datagram);
                pkt.datagram = NULL;
            }
        }
    }

    /* Final drain after shutdown raced with last push. */
    while (1) {
        RelayDeferredPacket pkt;
        RelayDeferredStatus pst;

        pst = relay_deferred_hub_try_pop(&ctx->deferred, &pkt);
        if (pst != RELAY_DEFERRED_OK) {
            break;
        }
        process_one_deferred_packet(ctx, &pkt);
        if (pkt.datagram != NULL) {
            free(pkt.datagram);
            pkt.datagram = NULL;
        }
    }
    return NULL;
}

static void tx_test_hold(const RelayCtx *ctx)
{
    uint32_t hold_us;

    if (ctx == NULL) {
        return;
    }
    hold_us = ctx->config.test_tx_hold_us;
    if (hold_us == 0) {
        return;
    }
    /* TEST ONLY: artificial TX delay to build egress backlog. */
    usleep(hold_us);
}

static void *tx_worker_main(void *arg)
{
    RelayCtx *ctx = arg;

    while (1) {
        EgressPacket pkt;
        EgressStatus st;
        ssize_t sent;
        RelayFlowStats *slot;

        st = egress_queue_dequeue(&ctx->egress, &pkt);
        if (st == EGRESS_ERR_SHUTDOWN) {
            break;
        }
        if (st != EGRESS_OK || pkt.datagram == NULL) {
            continue;
        }

        /* After ownership leaves the queue, before send/capture. */
        tx_test_hold(ctx);

        slot = flow_stats_slot(ctx, pkt.flow_id);
        if (ctx->config.egress_fn != NULL) {
            WireHeader header;

            if (wire_header_decode(&header, pkt.datagram, pkt.len) != 0 ||
                ctx->config.egress_fn(pkt.datagram, pkt.len, &header,
                                      ctx->config.egress_ctx) != 0) {
                slot->drop_malformed++;
                ctx->total.drop_malformed++;
                free(pkt.datagram);
                continue;
            }
        }

        if (ctx->tx_capture_fn != NULL) {
            ctx->tx_capture_fn(pkt.datagram, pkt.len, ctx->tx_capture_ctx);
            slot->forward++;
            ctx->total.forward++;
            free(pkt.datagram);
            continue;
        }

        do {
            sent = sendto(ctx->send_sock, pkt.datagram, pkt.len, 0,
                          (const struct sockaddr *)&ctx->next_hop,
                          ctx->next_hop_len);
        } while (sent < 0 && errno == EINTR);

        if (sent < 0 || (size_t)sent != pkt.len) {
            slot->drop_send++;
            ctx->total.drop_send++;
        } else {
            slot->forward++;
            ctx->total.forward++;
        }
        free(pkt.datagram);
    }
    return NULL;
}

static void relay_ctx_cleanup(RelayCtx *ctx)
{
    if (ctx == NULL) {
        return;
    }

    /*
     * Shutdown order (Phase 1):
     *  1) stop ingress / RX already exited
     *  2) join local source (injects fail with SHUTDOWN once stop set)
     *  3) deferred hub shutdown + join processing
     *  4) wait inject_in_flight; destroy generation cache
     *  5) egress shutdown + join TX
     *  6) destroy egress
     *  7) destroy deferred (free residual datagrams once)
     *  8) destroy mutexes / sockets
     */
    pthread_mutex_lock(&ctx->ingress_mu);
    ctx->stop = 1;
    pthread_mutex_unlock(&ctx->ingress_mu);

    if (ctx->source_started) {
        (void)pthread_join(ctx->source_thread, NULL);
        ctx->source_started = 0;
    }

    if (ctx->deferred_inited) {
        relay_deferred_hub_shutdown(&ctx->deferred);
    }
    if (ctx->processing_started) {
        (void)pthread_join(ctx->processing_thread, NULL);
        ctx->processing_started = 0;
    }

    pthread_mutex_lock(&ctx->ingress_mu);
    while (ctx->inject_in_flight > 0) {
        pthread_cond_wait(&ctx->ingress_idle, &ctx->ingress_mu);
    }
    if (ctx->cache_enabled) {
        generation_cache_destroy(&ctx->gen_cache);
        ctx->cache_enabled = 0;
    }
    pthread_mutex_unlock(&ctx->ingress_mu);

    egress_queue_shutdown(&ctx->egress);
    if (ctx->tx_started) {
        (void)pthread_join(ctx->tx_thread, NULL);
        ctx->tx_started = 0;
    }
    egress_queue_destroy(&ctx->egress);

    if (ctx->deferred_inited) {
        relay_deferred_hub_destroy(&ctx->deferred);
        ctx->deferred_inited = 0;
    }

    /* Quiesce any inject that raced into lock after cache destroy (stop-only). */
    pthread_mutex_lock(&ctx->ingress_mu);
    while (ctx->inject_in_flight > 0) {
        pthread_cond_wait(&ctx->ingress_idle, &ctx->ingress_mu);
    }
    pthread_mutex_unlock(&ctx->ingress_mu);

    if (ctx->ingress_idle_inited) {
        pthread_cond_destroy(&ctx->ingress_idle);
        ctx->ingress_idle_inited = 0;
    }
    pthread_mutex_destroy(&ctx->ingress_mu);
    if (ctx->send_sock >= 0) {
        close(ctx->send_sock);
        ctx->send_sock = -1;
    }
    if (ctx->listen_sock >= 0) {
        close(ctx->listen_sock);
        ctx->listen_sock = -1;
    }
}

static int relay_ctx_init_common(RelayCtx *ctx, const RelayConfig *config,
                                 int need_sockets)
{
    GenerationCacheConfig gcfg;
    RelayDeferredHubConfig dcfg;
    size_t egress_cap;

    memset(ctx, 0, sizeof(*ctx));
    ctx->config = *config;
    ctx->listen_sock = -1;
    ctx->send_sock = -1;

    if (ctx->config.egress_capacity == 0) {
        ctx->config.egress_capacity = RELAY_DEFAULT_EGRESS_CAPACITY;
    }
    if (ctx->config.deferred_per_flow == 0) {
        ctx->config.deferred_per_flow = RELAY_DEFAULT_DEFERRED_PER_FLOW;
    }
    if (ctx->config.deferred_total == 0) {
        ctx->config.deferred_total = RELAY_DEFAULT_DEFERRED_TOTAL;
    }
    if (ctx->config.max_active_flows == 0) {
        ctx->config.max_active_flows = RELAY_DEFAULT_MAX_ACTIVE_FLOWS;
    }
    if (ctx->config.max_active_flows < 1u ||
        ctx->config.max_active_flows > RELAY_MAX_ACTIVE_FLOWS_LIMIT) {
        return -1;
    }
    if (ctx->config.deferred_per_flow == 0 || ctx->config.deferred_total == 0) {
        return -1;
    }
    if (config->reject_local_encoder_loopback == 0 &&
        config->delivery_fn == NULL) {
        ctx->config.reject_local_encoder_loopback = 1;
    }
    egress_cap = ctx->config.egress_capacity;

    if (pthread_mutex_init(&ctx->ingress_mu, NULL) != 0) {
        return -1;
    }
    if (pthread_cond_init(&ctx->ingress_idle, NULL) != 0) {
        pthread_mutex_destroy(&ctx->ingress_mu);
        return -1;
    }
    ctx->ingress_idle_inited = 1;
    if (egress_queue_init(&ctx->egress, egress_cap) != EGRESS_OK) {
        pthread_cond_destroy(&ctx->ingress_idle);
        ctx->ingress_idle_inited = 0;
        pthread_mutex_destroy(&ctx->ingress_mu);
        return -1;
    }

    memset(&dcfg, 0, sizeof(dcfg));
    dcfg.max_active_flows = ctx->config.max_active_flows;
    dcfg.per_flow_capacity = ctx->config.deferred_per_flow;
    dcfg.total_capacity = ctx->config.deferred_total;
    if (relay_deferred_hub_init(&ctx->deferred, &dcfg) != RELAY_DEFERRED_OK) {
        egress_queue_destroy(&ctx->egress);
        pthread_cond_destroy(&ctx->ingress_idle);
        ctx->ingress_idle_inited = 0;
        pthread_mutex_destroy(&ctx->ingress_mu);
        return -1;
    }
    ctx->deferred_inited = 1;

    ctx->cache_enabled =
        (ctx->config.process_mode == RELAY_PROCESS_CACHE) ? 1 : 0;
    if (ctx->cache_enabled) {
        apply_cache_config(&gcfg, &ctx->config);
        if (generation_cache_init(&ctx->gen_cache, &gcfg) != 0) {
            relay_deferred_hub_destroy(&ctx->deferred);
            ctx->deferred_inited = 0;
            egress_queue_destroy(&ctx->egress);
            pthread_cond_destroy(&ctx->ingress_idle);
            ctx->ingress_idle_inited = 0;
            pthread_mutex_destroy(&ctx->ingress_mu);
            return -1;
        }
    }

    if (need_sockets) {
        if (resolve_next_hop(config->next_hop_host, config->next_hop_port,
                             &ctx->next_hop, &ctx->next_hop_len) != 0) {
            relay_ctx_cleanup(ctx);
            return -1;
        }
        ctx->listen_sock = open_listen_socket(config->listen_port);
        if (ctx->listen_sock < 0) {
            relay_ctx_cleanup(ctx);
            return -1;
        }
        ctx->send_sock = socket(ctx->next_hop.ss_family, SOCK_DGRAM, 0);
        if (ctx->send_sock < 0) {
            perror("wire-relay: send socket");
            relay_ctx_cleanup(ctx);
            return -1;
        }
        set_udp_buffers(ctx->send_sock);
    }

    if (pthread_create(&ctx->tx_thread, NULL, tx_worker_main, ctx) != 0) {
        perror("wire-relay: tx thread");
        relay_ctx_cleanup(ctx);
        return -1;
    }
    ctx->tx_started = 1;

    if (pthread_create(&ctx->processing_thread, NULL, processing_worker_main,
                       ctx) != 0) {
        perror("wire-relay: processing thread");
        relay_ctx_cleanup(ctx);
        return -1;
    }
    ctx->processing_started = 1;
    return 0;
}

RelayStatus relay_harness_open(RelayCtx **out, const RelayConfig *config,
                               RelayTxCaptureFn capture_fn, void *capture_ctx)
{
    RelayCtx *ctx;

    if (out == NULL || config == NULL || config->local_node_id == 0) {
        return RELAY_ERR;
    }
    ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        return RELAY_ERR;
    }
    if (relay_ctx_init_common(ctx, config, 0) != 0) {
        free(ctx);
        return RELAY_ERR;
    }
    ctx->tx_capture_fn = capture_fn;
    ctx->tx_capture_ctx = capture_ctx;
    ctx->stop = 0;
    *out = ctx;
    return RELAY_OK;
}

void relay_harness_close(RelayCtx *ctx)
{
    if (ctx == NULL) {
        return;
    }

    /*
     * Caller must have joined all injector threads. Wait only for injects that
     * already entered submit (hold ingress_mu); then require idle before
     * destroy. Does not cover threads still blocked acquiring ingress_mu.
     */
    pthread_mutex_lock(&ctx->ingress_mu);
    while (ctx->inject_in_flight > 0) {
        pthread_cond_wait(&ctx->ingress_idle, &ctx->ingress_mu);
    }
#ifndef NDEBUG
    assert(ctx->inject_in_flight == 0);
#else
    if (ctx->inject_in_flight != 0) {
        fprintf(stderr,
                "wire-relay: harness_close with inject_in_flight=%d "
                "(join all injectors before close)\n",
                ctx->inject_in_flight);
        abort();
    }
#endif
    pthread_mutex_unlock(&ctx->ingress_mu);

    relay_ctx_cleanup(ctx);
    free(ctx);
}

const RelayFlowStats *relay_total_stats(const RelayCtx *ctx)
{
    if (ctx == NULL) {
        return NULL;
    }
    /* Publish hub deferred drops into total for inject/RX test readers. */
    sync_deferred_drops_to_total((RelayCtx *)ctx);
    return &ctx->total;
}

const GenerationCacheStats *relay_cache_stats(const RelayCtx *ctx)
{
    if (ctx == NULL || !ctx->cache_enabled) {
        return NULL;
    }
    return generation_cache_stats(&ctx->gen_cache);
}

GenerationCache *relay_generation_cache(RelayCtx *ctx)
{
    if (ctx == NULL || !ctx->cache_enabled) {
        return NULL;
    }
    return &ctx->gen_cache;
}

void relay_egress_stats_snapshot(const RelayCtx *ctx, EgressQueueStats *out)
{
    if (ctx == NULL || out == NULL) {
        return;
    }
    egress_queue_stats_snapshot(&ctx->egress, out);
}

void relay_deferred_stats_snapshot(const RelayCtx *ctx,
                                   RelayDeferredHubStats *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (ctx == NULL || !ctx->deferred_inited) {
        return;
    }
    relay_deferred_hub_stats_snapshot(&ctx->deferred, out);
}

static int local_source_emit_inject(const uint8_t *datagram, size_t len,
                                    void *arg)
{
    RelayCtx *ctx = arg;
    RelayIngressStatus st;

    if (ctx == NULL) {
        return -1;
    }
    st = relay_inject_wire_datagram(ctx, datagram, len);
    return st == RELAY_INGRESS_OK ? 0 : -1;
}

static void *local_source_thread_main(void *arg)
{
    RelayCtx *ctx = arg;
    LocalSourceStats stats;
    int rc;

    memset(&stats, 0, sizeof(stats));
    rc = local_source_run(ctx->config.local_source, local_source_emit_inject,
                          ctx, &stats);
    ctx->source_stats = stats;
    ctx->source_result = rc;
    ctx->source_done = 1;
    fprintf(stderr,
            "wire-relay local-source: result=%d blocks=%llu source_bytes=%llu "
            "wire_datagrams=%llu emit_errors=%llu\n",
            rc, (unsigned long long)stats.blocks,
            (unsigned long long)stats.source_bytes,
            (unsigned long long)stats.wire_datagrams,
            (unsigned long long)stats.emit_errors);
    return NULL;
}

RelayStatus relay_run(const RelayConfig *config)
{
    RelayCtx ctx;
    RelayStatus status = RELAY_OK;
    unsigned char rxbuf[RELAY_MAX_DATAGRAM];

    if (config == NULL || config->local_node_id == 0 ||
        config->listen_port == 0 || config->next_hop_host == NULL ||
        config->next_hop_port == 0) {
        return RELAY_ERR;
    }
    if (config->local_source != NULL) {
        if (config->local_source->input_path == NULL ||
            config->local_source->final_dst == 0 ||
            config->local_source->ttl == 0) {
            return RELAY_ERR;
        }
    }

    memset(&ctx, 0, sizeof(ctx));
    if (relay_ctx_init_common(&ctx, config, 1) != 0) {
        return RELAY_ERR;
    }

    ctx.stop = 0;
    ctx.source_done = 0;
    ctx.last_activity_ns = relay_mono_ns();
    g_signal_ctx = &ctx;
    signal(SIGINT, relay_on_signal);
    signal(SIGTERM, relay_on_signal);

    fprintf(stderr,
            "wire-relay: local_node_id=%u listen=%u next-hop=%s:%u "
            "egress_capacity=%zu egress_wait_ms=%u "
            "deferred_per_flow=%zu deferred_total=%zu max_active_flows=%u "
            "test_tx_hold_us=%u inline_rx=%d process=%s recode=%s egress=%s "
            "decode_reencode=%s local_source=%s\n",
            (unsigned)ctx.config.local_node_id,
            (unsigned)ctx.config.listen_port,
            ctx.config.next_hop_host,
            (unsigned)ctx.config.next_hop_port,
            ctx.config.egress_capacity,
            (unsigned)ctx.config.egress_wait_ms,
            ctx.config.deferred_per_flow,
            ctx.config.deferred_total,
            (unsigned)ctx.config.max_active_flows,
            (unsigned)ctx.config.test_tx_hold_us,
            RELAY_TEST_INLINE_RX,
            ctx.cache_enabled ? "cache" : "forward",
            ctx.config.recode_fn != NULL ? "enabled" : "disabled",
            ctx.config.egress_fn != NULL ? "enabled" : "disabled",
            ctx.config.decode_reencode_fn != NULL ? "reserved" : "disabled",
            ctx.config.local_source != NULL ? "enabled" : "disabled");
    if (ctx.config.test_tx_hold_us != 0) {
        fprintf(stderr,
                "wire-relay: WARNING test-only TX hold enabled (%u us); "
                "not for production\n",
                (unsigned)ctx.config.test_tx_hold_us);
    }

    if (ctx.config.local_source != NULL) {
        if (pthread_create(&ctx.source_thread, NULL, local_source_thread_main,
                           &ctx) != 0) {
            perror("wire-relay: local source pthread_create");
            status = RELAY_ERR;
            ctx.stop = 1;
            g_signal_ctx = NULL;
            relay_ctx_cleanup(&ctx);
            return status;
        }
        ctx.source_started = 1;
    }

    while (!ctx.stop) {
        struct pollfd pfd = {.fd = ctx.listen_sock, .events = POLLIN};
        int poll_ms = 1000;
        int polled;
        ssize_t received;
        uint8_t *owned;

        if (ctx.cache_enabled) {
            pthread_mutex_lock(&ctx.ingress_mu);
            poll_ms = generation_cache_poll_timeout_ms(
                &ctx.gen_cache, relay_mono_ns(), 1000);
            pthread_mutex_unlock(&ctx.ingress_mu);
        }

        polled = poll(&pfd, 1, poll_ms);
        if (polled < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("wire-relay: poll");
            status = RELAY_ERR;
            break;
        }
        if (polled == 0) {
            if (ctx.cache_enabled) {
                pthread_mutex_lock(&ctx.ingress_mu);
                (void)generation_cache_expire(&ctx.gen_cache, relay_mono_ns(),
                                              -1);
                sync_cache_stats_to_total(&ctx);
                pthread_mutex_unlock(&ctx.ingress_mu);
            }
            if (ctx.config.idle_exit_sec > 0 &&
                !(ctx.source_started && !ctx.source_done)) {
                uint64_t now = relay_mono_ns();
                uint64_t idle_ns =
                    (uint64_t)ctx.config.idle_exit_sec * 1000000000ull;

                if (now >= ctx.last_activity_ns &&
                    now - ctx.last_activity_ns >= idle_ns) {
                    fprintf(stderr,
                            "wire-relay: idle for %u s; exiting\n",
                            ctx.config.idle_exit_sec);
                    break;
                }
            }
            continue;
        }
        if ((pfd.revents & POLLIN) == 0) {
            continue;
        }

        do {
            received = recvfrom(ctx.listen_sock, rxbuf, sizeof(rxbuf), 0,
                                NULL, NULL);
        } while (received < 0 && errno == EINTR);
        if (received < 0) {
            perror("wire-relay: recvfrom");
            status = RELAY_ERR;
            break;
        }
        ctx.last_activity_ns = relay_mono_ns();

        owned = malloc((size_t)received);
        if (owned == NULL) {
            pthread_mutex_lock(&ctx.ingress_mu);
            ctx.total.drop_malformed++;
            pthread_mutex_unlock(&ctx.ingress_mu);
            continue;
        }
        memcpy(owned, rxbuf, (size_t)received);
#if RELAY_TEST_INLINE_RX
        /*
         * TEST ONLY HOL baseline: process on the RX thread (may timed-wait
         * on egress). Production builds use deferred enqueue instead.
         */
        {
            DeferredLocalDelivery deferred;
            ForwardPending forward_pending;

            memset(&deferred, 0, sizeof(deferred));
            memset(&forward_pending, 0, sizeof(forward_pending));
            pthread_mutex_lock(&ctx.ingress_mu);
            (void)ingress_submit_owned(&ctx, &owned, (size_t)received,
                                       RELAY_SRC_PREVIOUS_NODE, &deferred,
                                       &forward_pending);
            pthread_mutex_unlock(&ctx.ingress_mu);
            run_deferred_local_delivery(&deferred);
            relay_apply_forward_pending(&ctx, &forward_pending);
        }
#else
        /* RX never waits on egress; processing owns ingress/egress work. */
        rx_enqueue_datagram(&ctx, &owned, (size_t)received);
#endif
    }

    ctx.stop = 1;
    g_signal_ctx = NULL;
    if (ctx.source_started) {
        (void)pthread_join(ctx.source_thread, NULL);
        ctx.source_started = 0;
        if (ctx.source_result != 0) {
            status = RELAY_ERR;
        }
    }
    sync_cache_stats_to_total(&ctx);
    print_stats(&ctx);
    relay_ctx_cleanup(&ctx);
    return status;
}
