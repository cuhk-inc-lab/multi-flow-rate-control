#include "relay.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void print_usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s --local-node-id N --listen PORT --next-hop HOST:PORT "
            "[--idle-exit-sec N] [--egress-capacity N]\n"
            "\n"
            "Explicit-hop opaque UDP relay (wire header v3).\n"
            "RX -> destination check(final_dst) -> TTL-- -> global EgressQueue "
            "-> TX sendto(next-hop).\n"
            "Local injection API (relay_inject_wire_datagram) accepts only "
            "already-encoded wire v3 datagrams.\n"
            "Does not encode/decode; recode hook default disabled.\n",
            prog);
}

static int parse_host_port(const char *spec, char *host, size_t host_len,
                           uint16_t *port)
{
    const char *colon;
    char *end = NULL;
    unsigned long value;
    size_t host_part;

    if (spec == NULL || host == NULL || port == NULL || host_len == 0) {
        return -1;
    }
    colon = strrchr(spec, ':');
    if (colon == NULL || colon == spec || colon[1] == '\0') {
        return -1;
    }
    host_part = (size_t)(colon - spec);
    if (host_part >= host_len) {
        return -1;
    }
    memcpy(host, spec, host_part);
    host[host_part] = '\0';
    value = strtoul(colon + 1, &end, 10);
    if (end == colon + 1 || *end != '\0' || value == 0 || value > 65535ul) {
        return -1;
    }
    *port = (uint16_t)value;
    return 0;
}

int main(int argc, char **argv)
{
    RelayConfig cfg;
    char next_hop_host[256];
    uint8_t local_node_id = 0;
    uint16_t listen_port = 0;
    uint16_t next_hop_port = 0;
    unsigned idle_exit_sec = 0;
    size_t egress_capacity = RELAY_DEFAULT_EGRESS_CAPACITY;
    int argi = 1;
    int have_next_hop = 0;

    memset(&cfg, 0, sizeof(cfg));
    memset(next_hop_host, 0, sizeof(next_hop_host));

    if (argc < 2) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    while (argi < argc) {
        if (strcmp(argv[argi], "--local-node-id") == 0) {
            char *end = NULL;
            unsigned long parsed;

            if (argi + 1 >= argc) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            parsed = strtoul(argv[argi + 1], &end, 10);
            if (end == argv[argi + 1] || *end != '\0' || parsed == 0 ||
                parsed > 255ul) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            local_node_id = (uint8_t)parsed;
            argi += 2;
        } else if (strcmp(argv[argi], "--listen") == 0) {
            char *end = NULL;
            unsigned long parsed;

            if (argi + 1 >= argc) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            parsed = strtoul(argv[argi + 1], &end, 10);
            if (end == argv[argi + 1] || *end != '\0' || parsed == 0 ||
                parsed > 65535ul) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            listen_port = (uint16_t)parsed;
            argi += 2;
        } else if (strcmp(argv[argi], "--next-hop") == 0) {
            if (argi + 1 >= argc ||
                parse_host_port(argv[argi + 1], next_hop_host,
                                sizeof(next_hop_host), &next_hop_port) != 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            have_next_hop = 1;
            argi += 2;
        } else if (strcmp(argv[argi], "--idle-exit-sec") == 0) {
            char *end = NULL;
            unsigned long parsed;

            if (argi + 1 >= argc) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            parsed = strtoul(argv[argi + 1], &end, 10);
            if (end == argv[argi + 1] || *end != '\0') {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            idle_exit_sec = (unsigned)parsed;
            argi += 2;
        } else if (strcmp(argv[argi], "--egress-capacity") == 0) {
            char *end = NULL;
            unsigned long parsed;

            if (argi + 1 >= argc) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            parsed = strtoul(argv[argi + 1], &end, 10);
            if (end == argv[argi + 1] || *end != '\0' || parsed == 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            egress_capacity = (size_t)parsed;
            argi += 2;
        } else {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    if (local_node_id == 0 || listen_port == 0 || !have_next_hop) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    cfg.local_node_id = local_node_id;
    cfg.listen_port = listen_port;
    cfg.next_hop_host = next_hop_host;
    cfg.next_hop_port = next_hop_port;
    cfg.recode_fn = NULL;
    cfg.delivery_fn = NULL;
    cfg.idle_exit_sec = idle_exit_sec;
    cfg.egress_capacity = egress_capacity;
    cfg.reject_local_encoder_loopback = 1;

    return relay_run(&cfg) == RELAY_OK ? EXIT_SUCCESS : EXIT_FAILURE;
}
