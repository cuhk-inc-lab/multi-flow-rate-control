#include "pipeline.h"
#include "wire_udp.h"

#include "flow_peer_map.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>

static int lock_process_memory(void)
{
    if (mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
        fprintf(stderr, "lock-memory: mlockall failed: %s\n", strerror(errno));
        return -1;
    }
    return 0;
}

static int parse_u32_token(const char *text, uint32_t *out)
{
    char         *end = NULL;
    unsigned long value;

    if (text == NULL || out == NULL) {
        return -1;
    }
    value = strtoul(text, &end, 10);
    if (end == text || *end != '\0') {
        return -1;
    }
    *out = (uint32_t)value;
    return 0;
}

static int parse_ipv4_endpoint(const char *ip, const char *port_text,
                               struct sockaddr_in *out)
{
    char *end = NULL;
    long  port;

    if (ip == NULL || port_text == NULL || out == NULL) {
        return -1;
    }
    memset(out, 0, sizeof(*out));
    out->sin_family = AF_INET;
    if (inet_pton(AF_INET, ip, &out->sin_addr) != 1) {
        return -1;
    }
    port = strtol(port_text, &end, 10);
    if (end == port_text || *end != '\0' || port <= 0 || port > 65535) {
        return -1;
    }
    out->sin_port = htons((uint16_t)port);
    return 0;
}

static int parse_rate_token(const char *rate_text, double *out)
{
    char  *end = NULL;
    double rate;

    if (rate_text == NULL || out == NULL) {
        return -1;
    }
    rate = strtod(rate_text, &end);
    if (end == rate_text || *end != '\0' || rate <= 0.0) {
        return -1;
    }
    *out = rate;
    return 0;
}

/*
 * Formats:
 *   [id:]host:port:input[:rate-mbps]
 *   tuple:src_ip:src_port:dst_ip:dst_port:wire_host:wire_port:input[:rate-mbps]
 */
static int parse_wire_flow_spec(const char *spec, WgWireFlowPath *path,
                                char **host_out, char **input_out,
                                double default_rate_mbps,
                                int *has_explicit_flow_id)
{
    char       *copy = NULL;
    char       *save = NULL;
    char       *token;
    char       *parts[8];
    size_t      count = 0;
    uint32_t    parsed_flow_id = 0;
    long        port;
    char       *end = NULL;
    int         explicit_flow_id = 0;
    const char *host_text;
    const char *port_text;
    const char *input_text;
    const char *rate_text = NULL;

    if (spec == NULL || path == NULL || host_out == NULL || input_out == NULL ||
        has_explicit_flow_id == NULL) {
        return -1;
    }

    *host_out = NULL;
    *input_out = NULL;
    *has_explicit_flow_id = 0;
    memset(path, 0, sizeof(*path));
    path->source_rate_mbps = default_rate_mbps;

    if (strncmp(spec, "tuple:", 6) == 0) {
        struct sockaddr_in src;
        struct sockaddr_in dst;
        const char        *body = spec + 6;

        copy = strdup(body);
        if (copy == NULL) {
            return -1;
        }
        for (token = strtok_r(copy, ":", &save);
             token != NULL && count < (sizeof(parts) / sizeof(parts[0]));
             token = strtok_r(NULL, ":", &save)) {
            parts[count++] = token;
        }
        if (token != NULL || (count != 7 && count != 8)) {
            free(copy);
            return -1;
        }
        if (parse_ipv4_endpoint(parts[0], parts[1], &src) != 0 ||
            parse_ipv4_endpoint(parts[2], parts[3], &dst) != 0) {
            free(copy);
            return -1;
        }
        if (flow_tuple_set(&path->tuple,
                           (const struct sockaddr *)&src, sizeof(src),
                           (const struct sockaddr *)&dst, sizeof(dst),
                           IPPROTO_UDP) != 0) {
            free(copy);
            return -1;
        }
        host_text = parts[4];
        port_text = parts[5];
        input_text = parts[6];
        if (count == 8) {
            rate_text = parts[7];
        }
        path->use_tuple = 1;
    } else {
        copy = strdup(spec);
        if (copy == NULL) {
            return -1;
        }

        for (token = strtok_r(copy, ":", &save);
             token != NULL && count < 5;
             token = strtok_r(NULL, ":", &save)) {
            parts[count++] = token;
        }
        if (token != NULL || count < 3 || count > 5) {
            free(copy);
            return -1;
        }

        if ((count == 4 || count == 5) &&
            parse_u32_token(parts[0], &parsed_flow_id) == 0) {
            explicit_flow_id = 1;
            path->flow_id = parsed_flow_id;
            host_text = parts[1];
            port_text = parts[2];
            input_text = parts[3];
            if (count == 5) {
                rate_text = parts[4];
            }
        } else {
            host_text = parts[0];
            port_text = parts[1];
            input_text = parts[2];
            if (count == 4) {
                rate_text = parts[3];
            }
        }
    }

    if (host_text == NULL || host_text[0] == '\0' ||
        input_text == NULL || input_text[0] == '\0') {
        free(copy);
        return -1;
    }

    port = strtol(port_text, &end, 10);
    if (end == port_text || *end != '\0') {
        free(copy);
        return -1;
    }
    if (port <= 0 || port > 65535) {
        free(copy);
        return -1;
    }
    *host_out = strdup(host_text);
    *input_out = strdup(input_text);
    if (*host_out == NULL || *input_out == NULL) {
        free(*host_out);
        free(*input_out);
        *host_out = NULL;
        *input_out = NULL;
        free(copy);
        return -1;
    }

    path->host = *host_out;
    path->port = (uint16_t)port;
    path->input_path = *input_out;
    if (rate_text != NULL) {
        if (parse_rate_token(rate_text, &path->source_rate_mbps) != 0) {
            free(*host_out);
            free(*input_out);
            *host_out = NULL;
            *input_out = NULL;
            free(copy);
            return -1;
        }
    }

    *has_explicit_flow_id = explicit_flow_id;
    free(copy);
    return 0;
}

static void format_ipv4_endpoint(const struct sockaddr_storage *addr,
                                 socklen_t addr_len, char *out, size_t out_len)
{
    char host[INET_ADDRSTRLEN];
    unsigned port = 0;

    if (out == NULL || out_len == 0) {
        return;
    }
    out[0] = '\0';
    if (addr == NULL || addr_len < (socklen_t)sizeof(struct sockaddr_in) ||
        addr->ss_family != AF_INET) {
        snprintf(out, out_len, "?");
        return;
    }
    {
        const struct sockaddr_in *ipv4 = (const struct sockaddr_in *)addr;

        if (inet_ntop(AF_INET, &ipv4->sin_addr, host, sizeof(host)) == NULL) {
            snprintf(out, out_len, "?");
            return;
        }
        port = ntohs(ipv4->sin_port);
        snprintf(out, out_len, "%s:%u", host, port);
    }
}

/* flow_id taken by tuple flows, explicit flows, or autos with index < assigned_upto. */
static int wire_flow_id_taken(const WgWireFlowPath *paths,
                              const int *explicit_flags, uint32_t flow_count,
                              uint32_t assigned_upto, uint32_t self,
                              uint32_t flow_id)
{
    uint32_t j;

    for (j = 0; j < flow_count; j++) {
        if (j == self) {
            continue;
        }
        if (paths[j].use_tuple || explicit_flags[j] || j < assigned_upto) {
            if (paths[j].flow_id == flow_id) {
                return 1;
            }
        }
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
            "  %s [--codec block|copy|xor-fec|rs-fec] --udp-send-multi --flow <[id:]host:port:input[:rate-mbps]|tuple:src_ip:src_port:dst_ip:dst_port:host:port:input[:rate-mbps]> ...\n"
            "  %s [--codec block|copy|xor-fec|rs-fec] --udp-recv <port> <output.ts|prefix> [--idle-sec N] [--best-effort] [--max-flows N] [--decode-mark]\n"
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
        int            *explicit_flags = NULL;
        uint32_t        flow_count = 0;
        uint32_t        flow_cap = 0;
        uint32_t        next_auto_flow_id = 0;
        uint32_t        tuple_count = 0;
        uint32_t        i;
        FlowPeerMap    *peer_map = NULL;
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
            int            has_explicit_flow_id = 0;

            if (argi + 1 >= argc) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            if (parse_wire_flow_spec(argv[argi + 1], &path, &host, &input,
                                     source_rate_mbps,
                                     &has_explicit_flow_id) != 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            if (path.use_tuple && has_explicit_flow_id) {
                fprintf(stderr,
                        "tuple --flow cannot also set an explicit flow_id\n");
                free(host);
                free(input);
                free(paths);
                free(hosts);
                free(inputs);
                free(explicit_flags);
                return EXIT_FAILURE;
            }

            if (flow_count == flow_cap) {
                uint32_t new_cap = flow_cap == 0 ? 4u : flow_cap * 2u;
                WgWireFlowPath *new_paths;
                char          **new_hosts;
                char          **new_inputs;
                int            *new_flags;

                new_paths = realloc(paths, (size_t)new_cap * sizeof(*paths));
                new_hosts = realloc(hosts, (size_t)new_cap * sizeof(*hosts));
                new_inputs = realloc(inputs, (size_t)new_cap * sizeof(*inputs));
                new_flags = realloc(explicit_flags,
                                    (size_t)new_cap * sizeof(*explicit_flags));
                if (new_paths == NULL || new_hosts == NULL || new_inputs == NULL ||
                    new_flags == NULL) {
                    free(paths);
                    free(hosts);
                    free(inputs);
                    free(explicit_flags);
                    free(host);
                    free(input);
                    return EXIT_FAILURE;
                }
                paths = new_paths;
                hosts = new_hosts;
                inputs = new_inputs;
                explicit_flags = new_flags;
                flow_cap = new_cap;
            }

            paths[flow_count] = path;
            hosts[flow_count] = host;
            inputs[flow_count] = input;
            explicit_flags[flow_count] = has_explicit_flow_id;
            if (path.use_tuple) {
                tuple_count++;
            }
            flow_count++;
            argi += 2;
        }

        if (flow_count == 0) {
            print_usage(argv[0]);
            free(explicit_flags);
            return EXIT_FAILURE;
        }

        if (tuple_count > 0) {
            if (flow_peer_map_init(&peer_map, flow_count) != FPM_OK) {
                fprintf(stderr, "failed to init flow_peer_map for tuple flows\n");
                goto multi_fail;
            }
            for (i = 0; i < flow_count; i++) {
                char src_text[64];
                char dst_text[64];
                uint32_t mapped;

                if (!paths[i].use_tuple) {
                    continue;
                }
                mapped = flow_peer_map_lookup(peer_map, &paths[i].tuple);
                if (mapped == (uint32_t)-1) {
                    fprintf(stderr, "flow_peer_map full or invalid tuple\n");
                    goto multi_fail;
                }
                paths[i].flow_id = mapped;
                format_ipv4_endpoint(&paths[i].tuple.src, paths[i].tuple.src_len,
                                     src_text, sizeof(src_text));
                format_ipv4_endpoint(&paths[i].tuple.dst, paths[i].tuple.dst_len,
                                     dst_text, sizeof(dst_text));
                fprintf(stderr,
                        "wire-multi-send: tuple %s -> %s UDP => flow_id=%u "
                        "(wire %s:%u %s)\n",
                        src_text, dst_text, mapped,
                        paths[i].host, (unsigned)paths[i].port,
                        paths[i].input_path);
            }
        }

        for (i = 0; i < flow_count; i++) {
            if (paths[i].use_tuple) {
                continue;
            }
            if (explicit_flags[i]) {
                if (wire_flow_id_taken(paths, explicit_flags, flow_count, i, i,
                                       paths[i].flow_id)) {
                    fprintf(stderr, "duplicate flow_id in --flow specs: %u\n",
                            paths[i].flow_id);
                    goto multi_fail;
                }
            } else {
                while (wire_flow_id_taken(paths, explicit_flags, flow_count, i, i,
                                          next_auto_flow_id)) {
                    next_auto_flow_id++;
                }
                paths[i].flow_id = next_auto_flow_id++;
            }
        }

        /* Final uniqueness check across all flows (tuple + explicit). */
        for (i = 0; i < flow_count; i++) {
            uint32_t j;

            for (j = i + 1; j < flow_count; j++) {
                if (paths[i].flow_id == paths[j].flow_id) {
                    fprintf(stderr, "duplicate flow_id after tuple map: %u\n",
                            paths[i].flow_id);
                    goto multi_fail;
                }
            }
        }

        status = wg_pipeline_run_wire_multi_send(
            &(WgWireMultiSendConfig){
                .flows = paths,
                .flow_count = flow_count,
                .pacing_enabled = pacing,
                .codec_kind = codec_kind,
                .peer_map = peer_map,
            });
        flow_peer_map_destroy(peer_map);
        for (i = 0; i < flow_count; i++) {
            free(hosts[i]);
            free(inputs[i]);
        }
        free(paths);
        free(hosts);
        free(inputs);
        free(explicit_flags);

        return status == WG_PIPE_OK ? EXIT_SUCCESS : EXIT_FAILURE;

    multi_fail:
        flow_peer_map_destroy(peer_map);
        for (i = 0; i < flow_count; i++) {
            free(hosts[i]);
            free(inputs[i]);
        }
        free(paths);
        free(hosts);
        free(inputs);
        free(explicit_flags);
        return EXIT_FAILURE;
    }

    if (argi < argc && strcmp(argv[argi], "--udp-recv") == 0) {
        WireUdpRecvConfig cfg;
        unsigned idle_sec = 3u;
        unsigned max_flows = 0u;
        int best_effort = 0;
        int decode_mark = 0;
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
            } else if (strcmp(argv[argi], "--decode-mark") == 0) {
                decode_mark = 1;
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
            .decode_mark = decode_mark,
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
