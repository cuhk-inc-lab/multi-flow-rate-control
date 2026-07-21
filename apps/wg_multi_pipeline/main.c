#include "pipeline.h"
#include "wire_udp.h"

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

static int lock_process_memory(void)
{
    if (mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
        fprintf(stderr, "lock-memory: mlockall failed: %s\n", strerror(errno));
        return -1;
    }
    return 0;
}

static int parse_wire_flow_spec(const char *spec, WgWireFlowPath *path,
                                char **host_out, char **input_out,
                                double default_rate_mbps)
{
    const char *cursor = spec;
    char       *end = NULL;
    size_t      host_len;
    size_t      input_len;
    long        port;
    unsigned long flow_id;
    const char *port_start;
    const char *input_start;
    const char *rate_start;

    if (spec == NULL || path == NULL || host_out == NULL || input_out == NULL) {
        return -1;
    }

    *host_out = NULL;
    *input_out = NULL;
    memset(path, 0, sizeof(*path));
    path->source_rate_mbps = default_rate_mbps;

    flow_id = strtoul(cursor, &end, 10);
    if (end == cursor || *end != ':') {
        return -1;
    }
    path->flow_id = (uint32_t)flow_id;
    cursor = end + 1;

    port_start = strchr(cursor, ':');
    if (port_start == NULL) {
        return -1;
    }
    host_len = (size_t)(port_start - cursor);
    if (host_len == 0) {
        return -1;
    }
    *host_out = strndup(cursor, host_len);
    if (*host_out == NULL) {
        return -1;
    }
    path->host = *host_out;
    cursor = port_start + 1;

    port = strtol(cursor, &end, 10);
    if (end == cursor || *end != ':') {
        free(*host_out);
        *host_out = NULL;
        return -1;
    }
    if (port <= 0 || port > 65535) {
        free(*host_out);
        *host_out = NULL;
        return -1;
    }
    path->port = (uint16_t)port;
    cursor = end + 1;

    input_start = strchr(cursor, ':');
    if (input_start == NULL) {
        input_len = strlen(cursor);
        rate_start = NULL;
    } else {
        input_len = (size_t)(input_start - cursor);
        rate_start = input_start + 1;
    }
    if (input_len == 0) {
        free(*host_out);
        *host_out = NULL;
        return -1;
    }
    *input_out = strndup(cursor, input_len);
    if (*input_out == NULL) {
        free(*host_out);
        *host_out = NULL;
        return -1;
    }
    path->input_path = *input_out;

    if (rate_start != NULL && *rate_start != '\0') {
        double rate = strtod(rate_start, &end);

        if (end == rate_start || *end != '\0' || rate <= 0.0) {
            free(*host_out);
            free(*input_out);
            *host_out = NULL;
            *input_out = NULL;
            return -1;
        }
        path->source_rate_mbps = rate;
    }

    return 0;
}

static void print_usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s [--no-pace] [--codec block|copy|xor-fec|rs-fec|none] <input.ts> <output.ts>\n"
            "  %s [--no-pace] [--codec block|copy|xor-fec|rs-fec|none] --multi <in0.ts> <out0.ts> [<in1.ts> <out1.ts> ...]\n"
            "  %s [--no-pace] [--codec block|copy|xor-fec|rs-fec] --udp <port> <out_prefix> [--max-flows N] [--idle-sec N]\n"
            "  %s [--codec block|copy|xor-fec|rs-fec] [--rate-mbps N] [--flow-id N] --udp-send <host> <port> <input.ts>\n"
            "  %s [--codec block|copy|xor-fec|rs-fec] --udp-send-multi --flow <id:host:port:input[:rate-mbps]> ...\n"
            "  %s [--codec block|copy|xor-fec|rs-fec] --udp-recv <port> <output.ts|prefix> [--idle-sec N] [--best-effort] [--max-flows N]\n"
            "  %s [--lock-memory] <any mode above>\n"
            "\n"
            "Pipeline per flow (multi BEFORE encode):\n"
            "  ingress -> FlowManager (split + pacing) -> raw bytes\n"
            "         -> selected codec encode -> buffer transfer -> decode -> file\n"
            "\n"
            "Codecs: block (default, existing demo), copy (4-to-8 benchmark without arithmetic),\n"
            "        xor-fec (4 data + 1 XOR parity), rs-fec (RS 4 data + 2 parity),\n"
            "        none (file/FIFO relay mode; --no-codec alias)\n"
            "\n"
            "UDP: ingress_push_tuple via recvfrom; outputs <out_prefix>flow0_segment0.bin, ...\n"
            "     Per-flow idle timeout (default 3 s) flushes a segment; server stays running.\n"
            "\n"
            "Wire UDP modes use group/shard headers and support cross-host transfer.\n"
            "Verify output: cmp <input.ts> <output.ts>\n",
            prog, prog, prog, prog, prog, prog, prog);
}

int main(int argc, char **argv)
{
    int pacing = 1;
    CodecKind codec_kind = CODEC_KIND_BLOCK;
    double source_rate_mbps = 0.0;
    uint32_t flow_id = 0;
    int lock_memory = 0;
    int argi = 1;

    /* Closing one ffplay must not kill the whole multi-flow process. */
    signal(SIGPIPE, SIG_IGN);

    if (argc < 3) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    while (argi < argc) {
        if (strcmp(argv[argi], "--no-pace") == 0) {
            pacing = 0;
            argi++;
        } else if (strcmp(argv[argi], "--no-codec") == 0) {
            codec_kind = CODEC_KIND_NONE;
            argi++;
        } else if (strcmp(argv[argi], "--codec") == 0) {
            if (argi + 1 >= argc) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            if (strcmp(argv[argi + 1], "block") == 0) {
                codec_kind = CODEC_KIND_BLOCK;
            } else if (strcmp(argv[argi + 1], "copy") == 0) {
                codec_kind = CODEC_KIND_COPY;
            } else if (strcmp(argv[argi + 1], "xor-fec") == 0) {
                codec_kind = CODEC_KIND_XOR_FEC;
            } else if (strcmp(argv[argi + 1], "rs-fec") == 0) {
                codec_kind = CODEC_KIND_RS_FEC;
            } else if (strcmp(argv[argi + 1], "none") == 0) {
                codec_kind = CODEC_KIND_NONE;
            } else {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            argi += 2;
        } else if (strcmp(argv[argi], "--rate-mbps") == 0) {
            char *end = NULL;

            if (argi + 1 >= argc) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            source_rate_mbps = strtod(argv[argi + 1], &end);
            if (end == argv[argi + 1] || source_rate_mbps <= 0.0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            argi += 2;
        } else if (strcmp(argv[argi], "--flow-id") == 0) {
            char          *end = NULL;
            unsigned long  parsed;

            if (argi + 1 >= argc) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            parsed = strtoul(argv[argi + 1], &end, 10);
            if (end == argv[argi + 1] || *end != '\0') {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            flow_id = (uint32_t)parsed;
            argi += 2;
        } else if (strcmp(argv[argi], "--lock-memory") == 0) {
            lock_memory = 1;
            argi++;
        } else {
            break;
        }
    }

    if (lock_memory && lock_process_memory() != 0) {
        return EXIT_FAILURE;
    }

    if (argi < argc && strcmp(argv[argi], "--udp-send") == 0) {
        WireUdpSendConfig cfg;
        char *end = NULL;
        long port;

        if (codec_kind == CODEC_KIND_NONE || argc - argi != 4) {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        port = strtol(argv[argi + 2], &end, 10);
        if (end == argv[argi + 2] || port <= 0 || port > 65535) {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        cfg = (WireUdpSendConfig){
            .host = argv[argi + 1],
            .port = (uint16_t)port,
            .input_path = argv[argi + 3],
            .codec_kind = codec_kind,
            .source_rate_mbps = source_rate_mbps,
            .flow_id = flow_id,
        };
        return wire_udp_send(&cfg) == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
    }

    if (argi < argc && strcmp(argv[argi], "--udp-send-multi") == 0) {
        WgWireFlowPath *paths = NULL;
        char          **hosts = NULL;
        char          **inputs = NULL;
        uint32_t        flow_count = 0;
        uint32_t        flow_cap = 0;
        WgPipelineStatus status;

        if (codec_kind == CODEC_KIND_NONE) {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }

        argi++;
        while (argi < argc && strcmp(argv[argi], "--flow") == 0) {
            WgWireFlowPath path;
            char          *host = NULL;
            char          *input = NULL;

            if (argi + 1 >= argc) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            if (parse_wire_flow_spec(argv[argi + 1], &path, &host, &input,
                                     source_rate_mbps) != 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }

            if (flow_count == flow_cap) {
                uint32_t new_cap = flow_cap == 0 ? 4u : flow_cap * 2u;
                WgWireFlowPath *new_paths;
                char          **new_hosts;
                char          **new_inputs;

                new_paths = realloc(paths, (size_t)new_cap * sizeof(*paths));
                new_hosts = realloc(hosts, (size_t)new_cap * sizeof(*hosts));
                new_inputs = realloc(inputs, (size_t)new_cap * sizeof(*inputs));
                if (new_paths == NULL || new_hosts == NULL || new_inputs == NULL) {
                    free(paths);
                    free(hosts);
                    free(inputs);
                    free(host);
                    free(input);
                    return EXIT_FAILURE;
                }
                paths = new_paths;
                hosts = new_hosts;
                inputs = new_inputs;
                flow_cap = new_cap;
            }

            paths[flow_count] = path;
            hosts[flow_count] = host;
            inputs[flow_count] = input;
            flow_count++;
            argi += 2;
        }

        if (flow_count == 0) {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }

        status = wg_pipeline_run_wire_multi_send(
            &(WgWireMultiSendConfig){
                .flows = paths,
                .flow_count = flow_count,
                .pacing_enabled = pacing,
                .codec_kind = codec_kind,
            });
        for (uint32_t i = 0; i < flow_count; i++) {
            free(hosts[i]);
            free(inputs[i]);
        }
        free(paths);
        free(hosts);
        free(inputs);

        return status == WG_PIPE_OK ? EXIT_SUCCESS : EXIT_FAILURE;
    }

    if (argi < argc && strcmp(argv[argi], "--udp-recv") == 0) {
        WireUdpRecvConfig cfg;
        unsigned idle_sec = 3u;
        unsigned max_flows = 0u;
        int best_effort = 0;
        char *end = NULL;
        long port;
        const char *output_path;

        if (codec_kind == CODEC_KIND_NONE || argc - argi < 3) {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        port = strtol(argv[argi + 1], &end, 10);
        if (end == argv[argi + 1] || port <= 0 || port > 65535) {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        output_path = argv[argi + 2];
        argi += 3;
        while (argi < argc) {
            if (strcmp(argv[argi], "--idle-sec") == 0) {
                if (argi + 1 >= argc) {
                    print_usage(argv[0]);
                    return EXIT_FAILURE;
                }
                idle_sec = (unsigned)strtoul(argv[argi + 1], NULL, 10);
                if (idle_sec == 0) {
                    print_usage(argv[0]);
                    return EXIT_FAILURE;
                }
                argi += 2;
            } else if (strcmp(argv[argi], "--best-effort") == 0) {
                best_effort = 1;
                argi++;
            } else if (strcmp(argv[argi], "--max-flows") == 0) {
                if (argi + 1 >= argc) {
                    print_usage(argv[0]);
                    return EXIT_FAILURE;
                }
                max_flows = (unsigned)strtoul(argv[argi + 1], NULL, 10);
                if (max_flows == 0) {
                    print_usage(argv[0]);
                    return EXIT_FAILURE;
                }
                argi += 2;
            } else {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
        }
        cfg = (WireUdpRecvConfig){
            .port = (uint16_t)port,
            .output_path = output_path,
            .codec_kind = codec_kind,
            .idle_sec = idle_sec,
            .best_effort = best_effort,
            .max_flows = max_flows,
        };
        return wire_udp_recv(&cfg) == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
    }

    if (argi < argc && strcmp(argv[argi], "--udp") == 0) {
        WgUdpConfig cfg;
        unsigned    max_flows = 8u;
        unsigned    idle_sec = 3u;
        long        port;
        char       *end = NULL;
        const char *out_prefix;

        if (codec_kind == CODEC_KIND_NONE || argc - argi < 3) {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }

        port = strtol(argv[argi + 1], &end, 10);
        if (end == argv[argi + 1] || port <= 0 || port > 65535) {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }

        out_prefix = argv[argi + 2];
        argi += 3;

        while (argi < argc) {
            if (strcmp(argv[argi], "--max-flows") == 0) {
                if (argi + 1 >= argc) {
                    print_usage(argv[0]);
                    return EXIT_FAILURE;
                }
                max_flows = (unsigned)strtoul(argv[argi + 1], NULL, 10);
                if (max_flows == 0) {
                    print_usage(argv[0]);
                    return EXIT_FAILURE;
                }
                argi += 2;
            } else if (strcmp(argv[argi], "--idle-sec") == 0) {
                if (argi + 1 >= argc) {
                    print_usage(argv[0]);
                    return EXIT_FAILURE;
                }
                idle_sec = (unsigned)strtoul(argv[argi + 1], NULL, 10);
                argi += 2;
            } else {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
        }

        cfg = (WgUdpConfig){
            .port = (uint16_t)port,
            .output_prefix = out_prefix,
            .max_flows = max_flows,
            .idle_sec = idle_sec,
            .pacing_enabled = pacing,
            .codec_kind = codec_kind
        };

        if (wg_pipeline_run_udp(&cfg) != WG_PIPE_OK) {
            fprintf(stderr, "wg_multi_pipeline UDP mode failed\n");
            return EXIT_FAILURE;
        }

        return EXIT_SUCCESS;
    }

    {
        WgPipelineConfig cfg;
        WgFlowPath      *paths = NULL;
        int              multi = 0;
        int              pairs;
        int              i;

        if (strcmp(argv[argi], "--multi") == 0) {
            multi = 1;
            argi++;
        }

        if (multi) {
            if ((argc - argi) < 2 || ((argc - argi) % 2) != 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            pairs = (argc - argi) / 2;
        } else {
            if (argc - argi != 2) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            pairs = 1;
        }

        paths = calloc((size_t)pairs, sizeof(*paths));
        if (paths == NULL) {
            return EXIT_FAILURE;
        }

        for (i = 0; i < pairs; i++) {
            paths[i].flow_id = (uint32_t)i;
            paths[i].input_path = argv[argi + i * 2];
            paths[i].output_path = argv[argi + i * 2 + 1];
        }

        cfg = (WgPipelineConfig){
            .flows = paths,
            .flow_count = (uint32_t)pairs,
            .pacing_enabled = pacing,
            .codec_kind = codec_kind
        };

        if (wg_pipeline_run(&cfg) != WG_PIPE_OK) {
            fprintf(stderr, "wg_multi_pipeline failed\n");
            free(paths);
            return EXIT_FAILURE;
        }

        free(paths);
    }

    return EXIT_SUCCESS;
}
