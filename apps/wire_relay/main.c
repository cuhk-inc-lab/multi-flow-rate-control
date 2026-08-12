#include "local_decode.h"
#include "relay.h"
#include "relay_deferred.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void print_usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s --local-node-id N --listen PORT --next-hop HOST:PORT\n"
            "     [--idle-exit-sec N] [--egress-capacity N] [--egress-wait-ms N]\n"
            "     [--deferred-per-flow N] [--deferred-total N]\n"
            "     [--max-active-flows N]\n"
            "     [--process forward|cache]\n"
            "     [--gen-timeout-ms N] [--max-gens N]\n"
            "     [--max-gens-per-flow N] [--max-cache-bytes N]\n"
            "     [--local-decode --codec copy|xor-fec|rs-fec|rs\n"
            "         (--output FILE | --output-dir DIR)]\n"
            "\n"
            "Explicit-hop opaque UDP relay (wire header v3).\n"
            "RX -> per-flow deferred packet queue -> processing worker ->\n"
            "  destination check(final_dst) -> TTL-- ->\n"
            "  [--process cache: GenerationCache observe] ->\n"
            "  global EgressQueue -> TX sendto(next-hop).\n"
            "inject (harness) stays synchronous and does not enter deferred;\n"
            "mixing inject with UDP RX does not preserve per-flow order.\n"
            "With --local-decode: final_dst==local_node_id packets decode via\n"
            "LocalDecodeHub / WireFlowDecoder. Local packets skip TTL--,\n"
            "GenerationCache, EgressQueue, and next-hop send.\n"
            "Locality is only wire_header_is_local (never UDP/IP dst).\n"
            "  --output FILE      L1 single-flow sink\n"
            "  --output-dir DIR   L2 multi-flow: DIR/flow_<id>.bin\n",
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

static int parse_codec_kind(const char *text, CodecKind *out)
{
    if (text == NULL || out == NULL) {
        return -1;
    }
    if (strcmp(text, "copy") == 0) {
        *out = CODEC_KIND_COPY;
        return 0;
    }
    if (strcmp(text, "xor-fec") == 0) {
        *out = CODEC_KIND_XOR_FEC;
        return 0;
    }
    if (strcmp(text, "rs-fec") == 0) {
        *out = CODEC_KIND_RS_FEC;
        return 0;
    }
    if (strcmp(text, "rs") == 0) {
        *out = CODEC_KIND_RS;
        return 0;
    }
    return -1;
}

int main(int argc, char **argv)
{
    RelayConfig cfg;
    LocalDecodeHub hub;
    LocalDecodeHubConfig hub_cfg;
    char next_hop_host[256];
    uint8_t local_node_id = 0;
    uint16_t listen_port = 0;
    uint16_t next_hop_port = 0;
    unsigned idle_exit_sec = 0;
    size_t egress_capacity = RELAY_DEFAULT_EGRESS_CAPACITY;
    uint32_t egress_wait_ms = 0;
    size_t deferred_per_flow = RELAY_DEFAULT_DEFERRED_PER_FLOW;
    size_t deferred_total = RELAY_DEFAULT_DEFERRED_TOTAL;
    uint32_t max_active_flows = RELAY_DEFAULT_MAX_ACTIVE_FLOWS;
    RelayProcessMode process_mode = RELAY_PROCESS_FORWARD;
    uint32_t gen_timeout_ms = GEN_CACHE_DEFAULT_TIMEOUT_MS;
    size_t max_gens_global = GEN_CACHE_DEFAULT_MAX_GENS_GLOBAL;
    size_t max_gens_per_flow = GEN_CACHE_DEFAULT_MAX_GENS_PER_FLOW;
    uint64_t max_cache_bytes = GEN_CACHE_DEFAULT_MAX_BYTES;
    int argi = 1;
    int have_next_hop = 0;
    int local_decode = 0;
    int have_codec = 0;
    CodecKind codec_kind = CODEC_KIND_COPY;
    const char *output_path = NULL;
    const char *output_dir = NULL;
    int hub_inited = 0;
    RelayStatus st;

    memset(&cfg, 0, sizeof(cfg));
    memset(&hub, 0, sizeof(hub));
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
        } else if (strcmp(argv[argi], "--egress-wait-ms") == 0) {
            unsigned long parsed;

            if (argi + 1 >= argc ||
                parse_ulong_arg(argv[argi + 1], &parsed) != 0 ||
                parsed > UINT32_MAX) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            egress_wait_ms = (uint32_t)parsed;
            argi += 2;
        } else if (strcmp(argv[argi], "--deferred-per-flow") == 0) {
            unsigned long parsed;

            if (argi + 1 >= argc ||
                parse_ulong_arg(argv[argi + 1], &parsed) != 0 ||
                parsed == 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            deferred_per_flow = (size_t)parsed;
            argi += 2;
        } else if (strcmp(argv[argi], "--deferred-total") == 0) {
            unsigned long parsed;

            if (argi + 1 >= argc ||
                parse_ulong_arg(argv[argi + 1], &parsed) != 0 ||
                parsed == 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            deferred_total = (size_t)parsed;
            argi += 2;
        } else if (strcmp(argv[argi], "--max-active-flows") == 0) {
            unsigned long parsed;

            if (argi + 1 >= argc ||
                parse_ulong_arg(argv[argi + 1], &parsed) != 0 ||
                parsed < 1ul || parsed > (unsigned long)RELAY_MAX_ACTIVE_FLOWS_LIMIT) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            max_active_flows = (uint32_t)parsed;
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
        } else if (strcmp(argv[argi], "--local-decode") == 0) {
            local_decode = 1;
            argi += 1;
        } else if (strcmp(argv[argi], "--codec") == 0) {
            if (argi + 1 >= argc ||
                parse_codec_kind(argv[argi + 1], &codec_kind) != 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            have_codec = 1;
            argi += 2;
        } else if (strcmp(argv[argi], "--output") == 0) {
            if (argi + 1 >= argc || argv[argi + 1][0] == '\0') {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            output_path = argv[argi + 1];
            argi += 2;
        } else if (strcmp(argv[argi], "--output-dir") == 0) {
            if (argi + 1 >= argc || argv[argi + 1][0] == '\0') {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            output_dir = argv[argi + 1];
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
    if (local_decode) {
        if (!have_codec) {
            fprintf(stderr, "wire-relay: --local-decode requires --codec\n");
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        if ((output_path == NULL && output_dir == NULL) ||
            (output_path != NULL && output_dir != NULL)) {
            fprintf(stderr,
                    "wire-relay: --local-decode requires exactly one of "
                    "--output FILE or --output-dir DIR\n");
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
    } else if (have_codec || output_path != NULL || output_dir != NULL) {
        fprintf(stderr,
                "wire-relay: --codec/--output/--output-dir require "
                "--local-decode\n");
        return EXIT_FAILURE;
    }

    cfg.local_node_id = local_node_id;
    cfg.listen_port = listen_port;
    cfg.next_hop_host = next_hop_host;
    cfg.next_hop_port = next_hop_port;
    cfg.recode_fn = NULL;
    cfg.delivery_fn = NULL;
    cfg.delivery_ctx = NULL;
    cfg.process_mode = process_mode;
    cfg.process_fn = NULL;
    cfg.idle_exit_sec = idle_exit_sec;
    cfg.egress_capacity = egress_capacity;
    cfg.egress_wait_ms = egress_wait_ms;
    cfg.deferred_per_flow = deferred_per_flow;
    cfg.deferred_total = deferred_total;
    cfg.max_active_flows = max_active_flows;
    cfg.reject_local_encoder_loopback = 1;
    cfg.gen_timeout_ms = gen_timeout_ms;
    cfg.max_gens_global = max_gens_global;
    cfg.max_gens_per_flow = max_gens_per_flow;
    cfg.max_cache_bytes = max_cache_bytes;

    if (local_decode) {
        memset(&hub_cfg, 0, sizeof(hub_cfg));
        hub_cfg.codec_kind = codec_kind;
        hub_cfg.best_effort = 0;
        hub_cfg.local_node_id = local_node_id;
        if (output_dir != NULL) {
            hub_cfg.mode = LOCAL_DECODE_MODE_OUTPUT_DIR;
            hub_cfg.output_dir = output_dir;
        } else {
            hub_cfg.mode = LOCAL_DECODE_MODE_SINGLE_FILE;
            hub_cfg.output_path = output_path;
        }
        if (local_decode_hub_init(&hub, &hub_cfg) != 0) {
            fprintf(stderr, "wire-relay: local_decode hub init failed\n");
            return EXIT_FAILURE;
        }
        hub_inited = 1;
        cfg.delivery_fn = local_decode_hub_delivery;
        cfg.delivery_ctx = &hub;
        /* Allow LOCAL_ENCODER inject when final_dst==local for sink decode. */
        cfg.reject_local_encoder_loopback = 0;
    }

    st = relay_run(&cfg);

    if (hub_inited) {
        LocalDecodeHubStats hub_stats_snap;

        if (local_decode_hub_get_stats(&hub, &hub_stats_snap) == 0) {
            fprintf(stderr,
                    "wire-relay local-decode: delivered=%llu "
                    "metadata_mismatch=%llu flow_rejected=%llu "
                    "ingest_error=%llu active=%zu complete=%d\n",
                    (unsigned long long)hub_stats_snap.delivered,
                    (unsigned long long)hub_stats_snap.metadata_mismatch,
                    (unsigned long long)hub_stats_snap.flow_rejected,
                    (unsigned long long)hub_stats_snap.ingest_error,
                    local_decode_hub_active_count(&hub),
                    local_decode_hub_is_complete(&hub));
        }
        if (st == RELAY_OK && local_decode_hub_strict_check(&hub) != 0) {
            fprintf(stderr,
                    "wire-relay: local-decode strict incomplete or error; "
                    "exiting failure\n");
            st = RELAY_ERR;
        }
        local_decode_hub_destroy(&hub);
    }

    return st == RELAY_OK ? EXIT_SUCCESS : EXIT_FAILURE;
}
