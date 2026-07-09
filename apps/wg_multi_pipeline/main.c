#include "pipeline.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void print_usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s [--no-pace] <input.ts> <output.ts>\n"
            "  %s [--no-pace] --multi <in0.ts> <out0.ts> [<in1.ts> <out1.ts> ...]\n"
            "  %s [--no-pace] --udp <port> <out_prefix> [--max-flows N] [--idle-sec N]\n"
            "\n"
            "Pipeline per flow (multi BEFORE encode):\n"
            "  ingress -> FlowManager (split + pacing) -> raw bytes\n"
            "         -> BlockCodec encode -> buffer transfer -> decode -> file\n"
            "\n"
            "UDP: ingress_push_tuple via recvfrom; outputs <out_prefix>0.bin, ...\n"
            "     Stop after --idle-sec (default 3) with no packets.\n"
            "\n"
            "Verify file mode: cmp <input.ts> <output.ts>\n",
            prog, prog, prog);
}

int main(int argc, char **argv)
{
    int pacing = 1;
    int argi = 1;

    if (argc < 3) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    if (strcmp(argv[argi], "--no-pace") == 0) {
        pacing = 0;
        argi++;
    }

    if (argi < argc && strcmp(argv[argi], "--udp") == 0) {
        WgUdpConfig cfg;
        unsigned    max_flows = 8u;
        unsigned    idle_sec = 3u;
        long        port;
        char       *end = NULL;
        const char *out_prefix;

        if (argc - argi < 3) {
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
            .pacing_enabled = pacing
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
            .pacing_enabled = pacing
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
