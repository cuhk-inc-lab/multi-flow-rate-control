#include "relay.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void print_usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s --local-node-id N --listen PORT --next-hop HOST:PORT\n"
            "     [--idle-exit-sec N] [--egress-capacity N]\n"
            "     [--process forward|cache]\n"
            "     [--gen-timeout-ms N] [--max-gens N]\n"
            "     [--max-gens-per-flow N] [--max-cache-bytes N]\n"
            "\n"
            "Explicit-hop opaque UDP relay (wire header v3).\n"
            "RX -> destination check(final_dst) -> TTL-- ->\n"
            "  [--process cache: GenerationCache observe] ->\n"
            "  global EgressQueue -> TX sendto(next-hop).\n"
            "Local injection API accepts only already-encoded wire v3 datagrams.\n"
            "Default --process forward (Phase 1). cache still opaque-forwards.\n",
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

static int parse_ulong_arg(const char *text, unsigned long *out)
{
    char *end = NULL;
    unsigned long value;

    if (text == NULL || out == NULL) {
        return -1;
    }
    value = strtoul(text, &end, 10);
    if (end == text || *end != '\0') {
        return -1;
    }
    *out = value;
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
    RelayProcessMode process_mode = RELAY_PROCESS_FORWARD;
    uint32_t gen_timeout_ms = GEN_CACHE_DEFAULT_TIMEOUT_MS;
    size_t max_gens_global = GEN_CACHE_DEFAULT_MAX_GENS_GLOBAL;
    size_t max_gens_per_flow = GEN_CACHE_DEFAULT_MAX_GENS_PER_FLOW;
    uint64_t max_cache_bytes = GEN_CACHE_DEFAULT_MAX_BYTES;
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
            unsigned long parsed;

            if (argi + 1 >= argc ||
                parse_ulong_arg(argv[argi + 1], &parsed) != 0 || parsed == 0 ||
                parsed > 255ul) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            local_node_id = (uint8_t)parsed;
            argi += 2;
        } else if (strcmp(argv[argi], "--listen") == 0) {
            unsigned long parsed;

            if (argi + 1 >= argc ||
                parse_ulong_arg(argv[argi + 1], &parsed) != 0 || parsed == 0 ||
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
            unsigned long parsed;

            if (argi + 1 >= argc ||
                parse_ulong_arg(argv[argi + 1], &parsed) != 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            idle_exit_sec = (unsigned)parsed;
            argi += 2;
        } else if (strcmp(argv[argi], "--egress-capacity") == 0) {
            unsigned long parsed;

            if (argi + 1 >= argc ||
                parse_ulong_arg(argv[argi + 1], &parsed) != 0 || parsed == 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            egress_capacity = (size_t)parsed;
            argi += 2;
        } else if (strcmp(argv[argi], "--process") == 0) {
            if (argi + 1 >= argc) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            if (strcmp(argv[argi + 1], "forward") == 0) {
                process_mode = RELAY_PROCESS_FORWARD;
            } else if (strcmp(argv[argi + 1], "cache") == 0) {
                process_mode = RELAY_PROCESS_CACHE;
            } else {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            argi += 2;
        } else if (strcmp(argv[argi], "--gen-timeout-ms") == 0) {
            unsigned long parsed;

            if (argi + 1 >= argc ||
                parse_ulong_arg(argv[argi + 1], &parsed) != 0 || parsed == 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            gen_timeout_ms = (uint32_t)parsed;
            argi += 2;
        } else if (strcmp(argv[argi], "--max-gens") == 0) {
            unsigned long parsed;

            if (argi + 1 >= argc ||
                parse_ulong_arg(argv[argi + 1], &parsed) != 0 || parsed == 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            max_gens_global = (size_t)parsed;
            argi += 2;
        } else if (strcmp(argv[argi], "--max-gens-per-flow") == 0) {
            unsigned long parsed;

            if (argi + 1 >= argc ||
                parse_ulong_arg(argv[argi + 1], &parsed) != 0 || parsed == 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            max_gens_per_flow = (size_t)parsed;
            argi += 2;
        } else if (strcmp(argv[argi], "--max-cache-bytes") == 0) {
            unsigned long parsed;

            if (argi + 1 >= argc ||
                parse_ulong_arg(argv[argi + 1], &parsed) != 0 || parsed == 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            max_cache_bytes = (uint64_t)parsed;
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
    cfg.process_mode = process_mode;
    cfg.process_fn = NULL;
    cfg.idle_exit_sec = idle_exit_sec;
    cfg.egress_capacity = egress_capacity;
    cfg.reject_local_encoder_loopback = 1;
    cfg.gen_timeout_ms = gen_timeout_ms;
    cfg.max_gens_global = max_gens_global;
    cfg.max_gens_per_flow = max_gens_per_flow;
    cfg.max_cache_bytes = max_cache_bytes;

    return relay_run(&cfg) == RELAY_OK ? EXIT_SUCCESS : EXIT_FAILURE;
}
