#include "relay.h"

#include "egress_queue.h"

#include <arpa/inet.h>
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

struct RelayCtx {
    RelayConfig                 config;
    EgressQueue                 egress;
    GenerationCache             gen_cache;
    int                         cache_enabled;
    pthread_mutex_t             ingress_mu;
    pthread_cond_t              ingress_idle;
    int                         inject_in_flight;
    int                         ingress_idle_inited;
    pthread_t                   tx_thread;
    int                         tx_started;
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

    if (ctx->cache_enabled) {
        gs = generation_cache_stats(&ctx->gen_cache);
    }

    fprintf(stderr,
            "wire-relay: local_node_id=%u summary rx=%llu forward=%llu "
            "local=%llu drop_ttl=%llu drop_malformed=%llu drop_send=%llu "
            "drop_egress_full=%llu inject_ok=%llu inject_reject_loopback=%llu\n",
            (unsigned)ctx->config.local_node_id,
            (unsigned long long)ctx->total.rx,
            (unsigned long long)ctx->total.forward,
            (unsigned long long)ctx->total.local_deliver,
            (unsigned long long)ctx->total.drop_ttl,
            (unsigned long long)ctx->total.drop_malformed,
            (unsigned long long)ctx->total.drop_send,
            (unsigned long long)ctx->total.drop_egress_full,
            (unsigned long long)ctx->total.inject_ok,
            (unsigned long long)ctx->total.inject_reject_loopback);
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
            s->drop_egress_full == 0 && s->inject_ok == 0 &&
            s->inject_reject_loopback == 0) {
            continue;
        }
        fprintf(stderr,
                "wire-relay: flow_id=%u rx=%llu forward=%llu local=%llu "
                "drop_ttl=%llu drop_malformed=%llu drop_send=%llu "
                "drop_egress_full=%llu\n",
                i,
                (unsigned long long)s->rx,
                (unsigned long long)s->forward,
                (unsigned long long)s->local_deliver,
                (unsigned long long)s->drop_ttl,
                (unsigned long long)s->drop_malformed,
                (unsigned long long)s->drop_send,
                (unsigned long long)s->drop_egress_full);
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

static int enqueue_forward(RelayCtx *ctx, RelayFlowStats *slot,
                           uint8_t **datagram_owned, size_t len,
                           const WireHeader *header)
{
    EgressPacket eg;
    EgressStatus est;

    memset(&eg, 0, sizeof(eg));
    eg.datagram = *datagram_owned;
    eg.len = len;
    eg.flow_id = header->flow_id;
    eg.generation_id = header->block_id;
    eg.enqueue_ns = relay_mono_ns();
    *datagram_owned = NULL;

    est = egress_queue_try_enqueue(&ctx->egress, &eg);
    if (est != EGRESS_OK) {
        slot->drop_egress_full++;
        ctx->total.drop_egress_full++;
        free(eg.datagram);
        return -1;
    }
    return 0;
}

/*
 * Takes ownership of *datagram_owned on all paths (frees or moves to egress).
 * Caller must hold ingress_mu.
 */
static RelayIngressStatus ingress_submit_owned(RelayCtx *ctx,
                                               uint8_t **datagram_owned,
                                               size_t len,
                                               RelayPacketSource source)
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
        if (ctx->config.delivery_fn != NULL) {
            (void)ctx->config.delivery_fn(datagram, len, &header,
                                          ctx->config.delivery_ctx);
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
        if (enqueue_forward(ctx, slot, &datagram, out_len, &header) == 0 &&
            source == RELAY_SRC_LOCAL_ENCODER) {
            slot->inject_ok++;
            ctx->total.inject_ok++;
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
    }

    if (action == RELAY_PROCESS_DROP) {
        free(datagram);
        return RELAY_INGRESS_OK;
    }

    if (enqueue_forward(ctx, slot, &datagram, out_len, &header) == 0 &&
        source == RELAY_SRC_LOCAL_ENCODER) {
        slot->inject_ok++;
        ctx->total.inject_ok++;
    }
    return RELAY_INGRESS_OK;
}

RelayIngressStatus relay_inject_wire_datagram(RelayCtx *ctx,
                                              const uint8_t *datagram,
                                              size_t len)
{
    uint8_t *owned;
    RelayIngressStatus st;

    if (ctx == NULL || datagram == NULL || len < WIRE_HEADER_SIZE ||
        len > RELAY_MAX_DATAGRAM) {
        return RELAY_INGRESS_ERR_INVALID;
    }

    owned = malloc(len);
    if (owned == NULL) {
        return RELAY_INGRESS_ERR_ALLOC;
    }
    memcpy(owned, datagram, len);

    pthread_mutex_lock(&ctx->ingress_mu);
    if (ctx->stop) {
        pthread_mutex_unlock(&ctx->ingress_mu);
        free(owned);
        return RELAY_INGRESS_ERR_SHUTDOWN;
    }
    ctx->inject_in_flight++;
    st = ingress_submit_owned(ctx, &owned, len, RELAY_SRC_LOCAL_ENCODER);
    ctx->inject_in_flight--;
    if (ctx->inject_in_flight == 0) {
        pthread_cond_broadcast(&ctx->ingress_idle);
    }
    pthread_mutex_unlock(&ctx->ingress_mu);
    return st;
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

        if (ctx->tx_capture_fn != NULL) {
            ctx->tx_capture_fn(pkt.datagram, pkt.len, ctx->tx_capture_ctx);
            slot = flow_stats_slot(ctx, pkt.flow_id);
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

        slot = flow_stats_slot(ctx, pkt.flow_id);
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
     * Stop ingress under ingress_mu so concurrent inject observes stop before
     * touching cache. Wait until in-flight injects finish, then destroy cache
     * before releasing the lock for queue/TX teardown.
     */
    pthread_mutex_lock(&ctx->ingress_mu);
    ctx->stop = 1;
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
    size_t egress_cap;

    memset(ctx, 0, sizeof(*ctx));
    ctx->config = *config;
    ctx->listen_sock = -1;
    ctx->send_sock = -1;

    if (ctx->config.egress_capacity == 0) {
        ctx->config.egress_capacity = RELAY_DEFAULT_EGRESS_CAPACITY;
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

    ctx->cache_enabled =
        (ctx->config.process_mode == RELAY_PROCESS_CACHE) ? 1 : 0;
    if (ctx->cache_enabled) {
        apply_cache_config(&gcfg, &ctx->config);
        if (generation_cache_init(&ctx->gen_cache, &gcfg) != 0) {
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
    /* stop + cache destroy happen under ingress_mu inside relay_ctx_cleanup. */
    relay_ctx_cleanup(ctx);
    free(ctx);
}

const RelayFlowStats *relay_total_stats(const RelayCtx *ctx)
{
    return ctx != NULL ? &ctx->total : NULL;
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

RelayStatus relay_run(const RelayConfig *config)
{
    RelayCtx ctx;
    uint64_t last_rx_ns;
    RelayStatus status = RELAY_OK;
    unsigned char rxbuf[RELAY_MAX_DATAGRAM];

    if (config == NULL || config->local_node_id == 0 ||
        config->listen_port == 0 || config->next_hop_host == NULL ||
        config->next_hop_port == 0) {
        return RELAY_ERR;
    }

    if (relay_ctx_init_common(&ctx, config, 1) != 0) {
        return RELAY_ERR;
    }

    ctx.stop = 0;
    g_signal_ctx = &ctx;
    signal(SIGINT, relay_on_signal);
    signal(SIGTERM, relay_on_signal);

    fprintf(stderr,
            "wire-relay: local_node_id=%u listen=%u next-hop=%s:%u "
            "egress_capacity=%zu process=%s recode=%s\n",
            (unsigned)ctx.config.local_node_id,
            (unsigned)ctx.config.listen_port,
            ctx.config.next_hop_host,
            (unsigned)ctx.config.next_hop_port,
            ctx.config.egress_capacity,
            ctx.cache_enabled ? "cache" : "forward",
            ctx.config.recode_fn != NULL ? "enabled" : "disabled");

    last_rx_ns = relay_mono_ns();
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
            if (ctx.config.idle_exit_sec > 0) {
                uint64_t now = relay_mono_ns();
                uint64_t idle_ns =
                    (uint64_t)ctx.config.idle_exit_sec * 1000000000ull;

                if (now >= last_rx_ns && now - last_rx_ns >= idle_ns) {
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
        last_rx_ns = relay_mono_ns();

        owned = malloc((size_t)received);
        if (owned == NULL) {
            ctx.total.drop_malformed++;
            continue;
        }
        memcpy(owned, rxbuf, (size_t)received);

        pthread_mutex_lock(&ctx.ingress_mu);
        (void)ingress_submit_owned(&ctx, &owned, (size_t)received,
                                   RELAY_SRC_PREVIOUS_NODE);
        pthread_mutex_unlock(&ctx.ingress_mu);
    }

    ctx.stop = 1;
    g_signal_ctx = NULL;
    sync_cache_stats_to_total(&ctx);
    print_stats(&ctx);
    relay_ctx_cleanup(&ctx);
    return status;
}
