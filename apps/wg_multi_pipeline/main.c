#include "pipeline.h"
#include "wire_udp.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void print_usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s [--no-pace] [--codec block|copy|xor-fec|none] <input.ts> <output.ts>\n"
            "  %s [--no-pace] [--codec block|copy|xor-fec|none] --multi <in0.ts> <out0.ts> [<in1.ts> <out1.ts> ...]\n"
            "  %s [--no-pace] [--codec block|copy|xor-fec] --udp <port> <out_prefix> [--max-flows N] [--idle-sec N]\n"
            "  %s [--codec block|copy|xor-fec] [--rate-mbps N] --udp-send <host> <port> <input.ts>\n"
            "  %s [--codec block|copy|xor-fec] --udp-recv <port> <output.ts> [--idle-sec N]\n"
            "\n"
            "Pipeline per flow (multi BEFORE encode):\n"
            "  ingress -> FlowManager (split + pacing) -> raw bytes\n"
            "         -> selected codec encode -> buffer transfer -> decode -> file\n"
            "\n"
            "Codecs: block (default, existing demo), copy (4-to-8 benchmark without arithmetic),\n"
            "        xor-fec (4 data + 1 XOR parity),\n"
            "        none (file/FIFO relay mode; --no-codec alias)\n"
            "\n"
            "UDP: ingress_push_tuple via recvfrom; outputs <out_prefix>flow0_segment0.bin, ...\n"
            "     Per-flow idle timeout (default 3 s) flushes a segment; server stays running.\n"
            "\n"
            "Wire UDP modes use group/shard headers and support cross-host transfer.\n"
            "Verify output: cmp <input.ts> <output.ts>\n",
            prog, prog, prog, prog, prog);
}

int main(int argc, char **argv)
{
    int pacing = 1;
    CodecKind codec_kind = CODEC_KIND_BLOCK;
    double source_rate_mbps = 0.0;
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
        } else {
            break;
        }
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
        };
        return wire_udp_send(&cfg) == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
    }

    if (argi < argc && strcmp(argv[argi], "--udp-recv") == 0) {
        WireUdpRecvConfig cfg;
        unsigned idle_sec = 3u;
        char *end = NULL;
        long port;

        if (codec_kind == CODEC_KIND_NONE ||
            ((argc - argi != 3) && (argc - argi != 5))) {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        port = strtol(argv[argi + 1], &end, 10);
        if (end == argv[argi + 1] || port <= 0 || port > 65535) {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        if (argc - argi == 5) {
            if (strcmp(argv[argi + 3], "--idle-sec") != 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
            idle_sec = (unsigned)strtoul(argv[argi + 4], NULL, 10);
            if (idle_sec == 0) {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
        }
        cfg = (WireUdpRecvConfig){
            .port = (uint16_t)port,
            .output_path = argv[argi + 2],
            .codec_kind = codec_kind,
            .idle_sec = idle_sec,
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
