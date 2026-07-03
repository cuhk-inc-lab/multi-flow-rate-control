#include "relay.h"

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
            "Pipeline per flow:\n"
            "  file -> CircularBuffer -> encode -> packet_framer -> FlowManager\n"
            "       -> paced pipe -> decode -> CircularBuffer -> file\n"
            "\n"
            "Verify: cmp <input.ts> <output.ts>\n",
            prog, prog);
}

int main(int argc, char **argv)
{
    RelayConfig cfg;
    FlowRelayPath *paths = NULL;
    int pacing = 1;
    int multi = 0;
    int argi = 1;
    int pairs;
    int i;

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

    cfg = (RelayConfig){
        .flows = paths,
        .flow_count = (uint32_t)pairs,
        .pacing_enabled = pacing
    };

    if (Relay_run(&cfg) != RELAY_OK) {
        fprintf(stderr, "relay failed\n");
        free(paths);
        return EXIT_FAILURE;
    }

    fprintf(stderr, "done: %d flow(s)%s\n", pairs, pacing ? "" : " (pacing off)");
    free(paths);
    return EXIT_SUCCESS;
}
