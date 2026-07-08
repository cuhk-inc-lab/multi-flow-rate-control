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
            "\n"
            "Pipeline per flow (multi BEFORE encode):\n"
            "  ingress -> FlowManager (split + pacing) -> raw bytes\n"
            "         -> BlockCodec encode -> buffer transfer -> decode -> file\n"
            "\n"
            "File ingress mocks wg-obfs datagrams with a fixed flow_id per path.\n"
            "Future UDP: ingress_push_peer() maps src_addr to flow_id.\n"
            "\n"
            "Verify: cmp <input.ts> <output.ts>\n",
            prog, prog);
}

int main(int argc, char **argv)
{
    WgPipelineConfig cfg;
    WgFlowPath      *paths = NULL;
    int              pacing = 1;
    int              multi = 0;
    int              argi = 1;
    int              pairs;
    int              i;

    if (argc < 3) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    if (strcmp(argv[argi], "--no-pace") == 0) {
        pacing = 0;
        argi++;
    }

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
    return EXIT_SUCCESS;
}
