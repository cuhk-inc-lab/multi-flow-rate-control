#include "relay.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define RELAY_UDP_SOCKBUF (8 * 1024 * 1024)
#define RELAY_MAX_DATAGRAM (WIRE_HEADER_SIZE + 2048u)

static volatile sig_atomic_t g_relay_stop = 0;

static void relay_on_signal(int sig)
{
    (void)sig;
    g_relay_stop = 1;
}

static double monotonic_seconds(void)
{
    struct timespec now;

    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        return 0.0;
    }
    return (double)now.tv_sec + (double)now.tv_nsec / 1000000000.0;
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

static RelayFlowStats *flow_stats_slot(RelayFlowStats *stats, uint32_t flow_id)
{
    if (flow_id >= RELAY_MAX_FLOWS) {
        return &stats[RELAY_MAX_FLOWS - 1u];
    }
    return &stats[flow_id];
}

static void print_stats(const RelayConfig *config,
                        const RelayFlowStats *per_flow,
                        const RelayFlowStats *total)
{
    uint32_t i;

    fprintf(stderr,
            "wire-relay: local_node_id=%u summary rx=%llu forward=%llu "
            "local=%llu drop_ttl=%llu drop_malformed=%llu drop_send=%llu\n",
            (unsigned)config->local_node_id,
            (unsigned long long)total->rx,
            (unsigned long long)total->forward,
            (unsigned long long)total->local_deliver,
            (unsigned long long)total->drop_ttl,
            (unsigned long long)total->drop_malformed,
            (unsigned long long)total->drop_send);
    for (i = 0; i < RELAY_MAX_FLOWS; i++) {
        if (per_flow[i].rx == 0 && per_flow[i].forward == 0 &&
            per_flow[i].local_deliver == 0 && per_flow[i].drop_ttl == 0 &&
            per_flow[i].drop_malformed == 0 && per_flow[i].drop_send == 0) {
            continue;
        }
        fprintf(stderr,
                "wire-relay: flow_id=%u rx=%llu forward=%llu local=%llu "
                "drop_ttl=%llu drop_malformed=%llu drop_send=%llu\n",
                i,
                (unsigned long long)per_flow[i].rx,
                (unsigned long long)per_flow[i].forward,
                (unsigned long long)per_flow[i].local_deliver,
                (unsigned long long)per_flow[i].drop_ttl,
                (unsigned long long)per_flow[i].drop_malformed,
                (unsigned long long)per_flow[i].drop_send);
    }
}

RelayStatus relay_run(const RelayConfig *config)
{
    int listen_sock = -1;
    int send_sock = -1;
    struct sockaddr_storage next_hop;
    socklen_t next_hop_len = 0;
    RelayFlowStats per_flow[RELAY_MAX_FLOWS];
    RelayFlowStats total;
    unsigned char datagram[RELAY_MAX_DATAGRAM];
    unsigned char recode_buf[RELAY_MAX_DATAGRAM];
    double last_rx;
    RelayStatus status = RELAY_OK;

    if (config == NULL || config->local_node_id == 0 ||
        config->listen_port == 0 || config->next_hop_host == NULL ||
        config->next_hop_port == 0) {
        return RELAY_ERR;
    }

    memset(per_flow, 0, sizeof(per_flow));
    memset(&total, 0, sizeof(total));
    memset(&next_hop, 0, sizeof(next_hop));

    if (resolve_next_hop(config->next_hop_host, config->next_hop_port,
                         &next_hop, &next_hop_len) != 0) {
        return RELAY_ERR;
    }

    listen_sock = open_listen_socket(config->listen_port);
    if (listen_sock < 0) {
        return RELAY_ERR;
    }
    send_sock = socket(next_hop.ss_family, SOCK_DGRAM, 0);
    if (send_sock < 0) {
        perror("wire-relay: send socket");
        close(listen_sock);
        return RELAY_ERR;
    }
    set_udp_buffers(send_sock);

    g_relay_stop = 0;
    signal(SIGINT, relay_on_signal);
    signal(SIGTERM, relay_on_signal);

    fprintf(stderr,
            "wire-relay: local_node_id=%u listen=%u next-hop=%s:%u "
            "recode=%s\n",
            (unsigned)config->local_node_id,
            (unsigned)config->listen_port,
            config->next_hop_host,
            (unsigned)config->next_hop_port,
            config->recode_fn != NULL ? "enabled" : "disabled");

    last_rx = monotonic_seconds();
    while (!g_relay_stop) {
        struct pollfd pfd = {.fd = listen_sock, .events = POLLIN};
        int polled = poll(&pfd, 1, 1000);
        ssize_t received;
        WireHeader header;
        RelayFlowStats *slot;
        const uint8_t *out_ptr;
        size_t out_len;

        if (polled < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("wire-relay: poll");
            status = RELAY_ERR;
            break;
        }
        if (polled == 0) {
            if (config->idle_exit_sec > 0 &&
                monotonic_seconds() - last_rx >=
                    (double)config->idle_exit_sec) {
                fprintf(stderr,
                        "wire-relay: idle for %u s; exiting\n",
                        config->idle_exit_sec);
                break;
            }
            continue;
        }
        if ((pfd.revents & POLLIN) == 0) {
            continue;
        }

        do {
            received = recvfrom(listen_sock, datagram, sizeof(datagram), 0,
                                NULL, NULL);
        } while (received < 0 && errno == EINTR);
        if (received < 0) {
            perror("wire-relay: recvfrom");
            status = RELAY_ERR;
            break;
        }
        last_rx = monotonic_seconds();

        if (wire_header_decode(&header, datagram, (size_t)received) != 0) {
            total.drop_malformed++;
            continue;
        }
        slot = flow_stats_slot(per_flow, header.flow_id);
        slot->rx++;
        total.rx++;

        if (header.ttl == 0) {
            slot->drop_ttl++;
            total.drop_ttl++;
            continue;
        }

        if (wire_header_is_local(&header, config->local_node_id)) {
            slot->local_deliver++;
            total.local_deliver++;
            if (config->delivery_fn != NULL) {
                if (config->delivery_fn(datagram, (size_t)received, &header,
                                        config->delivery_ctx) != 0) {
                    status = RELAY_ERR;
                    break;
                }
            }
            continue;
        }

        if (header.ttl <= 1) {
            slot->drop_ttl++;
            total.drop_ttl++;
            continue;
        }

        header.ttl = (uint8_t)(header.ttl - 1u);
        wire_header_encode(datagram, &header);

        out_ptr = datagram;
        out_len = (size_t)received;
        if (config->recode_fn != NULL) {
            if (config->recode_fn(datagram, (size_t)received, recode_buf,
                                  sizeof(recode_buf), &out_len, &header,
                                  config->recode_ctx) != 0) {
                slot->drop_malformed++;
                total.drop_malformed++;
                continue;
            }
            out_ptr = recode_buf;
        }

        {
            ssize_t sent;

            do {
                sent = sendto(send_sock, out_ptr, out_len, 0,
                              (const struct sockaddr *)&next_hop, next_hop_len);
            } while (sent < 0 && errno == EINTR);
            if (sent < 0 || (size_t)sent != out_len) {
                slot->drop_send++;
                total.drop_send++;
                continue;
            }
        }
        slot->forward++;
        total.forward++;
    }

    print_stats(config, per_flow, &total);
    close(send_sock);
    close(listen_sock);
    return status;
}
