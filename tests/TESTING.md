# Testing Guide

## Quick reference

| Command | What it runs |
|---------|----------------|
| `make test` | Unit/integration tests in `tests/run_tests.c` |
| `make integration-test` | **Multi-file** `wg_multi_pipeline` roundtrip (3 flows, `cmp`) |
| `make sanitize` | ASan + `test` + `integration-test` |
| `make tsan` | TSan + `test` + `integration-test` |

`make wg-demo-test` is an alias for `make integration-test`.

GitLab CI: `unit-test`, `integration-test`, `asan`, `tsan`.

---

## `make test` — library (`run_tests.c`)

Covers packets, queues, FlowManager, pacing, HOL avoidance, peer map, and multi-flow producers.

---

## `make integration-test` — multi-file pipeline

- **App:** `build/wg_multi_pipeline --multi`
- **Flows:** 3 input/output file pairs (`flow_id` 0, 1, 2)
- **Data:** flow 0: 20×188 B; flow 1: 5×752 B; flow 2: 40×188 B + 96 B tail
- **Path:** `ingress_push` → `FlowManager` → encode → transfer → decode → file
- **Assert:** `cmp` per pair (`--no-pace` for byte-exact)

Manual run:

```bash
make wg-demo
./build/wg_multi_pipeline --no-pace --multi \
  in0.ts out0.ts in1.ts out1.ts in2.ts out2.ts
```
