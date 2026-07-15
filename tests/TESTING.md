# Testing Guide

## Quick reference

| Command | What it runs |
|---------|----------------|
| `make test` | Unit tests in `tests/run_tests.c` (queues, pacing, **5-tuple peer map**, …) |
| `make integration-test` | Automated multi-file `wg_multi_pipeline` roundtrip (3 flows, `cmp`) |
| `make wg-demo` | Build `build/wg_multi_pipeline` only |
| `make sanitize` | ASan + `test` + `integration-test` |
| `make tsan` | TSan + `test` + `integration-test` |

`make wg-demo-test` is an alias for `make integration-test`.

GitLab CI: `unit-test`, `integration-test`, `asan`, `tsan`.

---

## Multi-file pipeline — your own files

Use `wg_multi_pipeline` when you have several input files and want each to run as a
separate flow through the full pipeline (split/pacing → encode → transfer → decode).

**File extension does not matter** — inputs are raw bytes (`a.txt`, `b.bin`, `c.ts`, …).

```bash
cd /path/to/multi-flow-rate-control
make wg-demo

./build/wg_multi_pipeline --no-pace --multi \
  a.txt out_a.txt \
  b.bin out_b.bin \
  c.ts  out_c.ts

cmp a.txt out_a.txt && echo "flow 0 OK"
cmp b.bin out_b.bin && echo "flow 1 OK"
cmp c.ts  out_c.ts  && echo "flow 2 OK"
```

- Pairs are `input output input output …`; the *i*-th pair gets internal `flow_id = i`.
- `--no-pace` disables rate matching so `cmp` can verify byte-exact roundtrip.
- Output directories must exist; the program creates the output **files**.
- Use absolute paths if inputs are not in the current directory.

**Single flow** (one input file):

```bash
./build/wg_multi_pipeline --no-pace input.bin output.bin
cmp input.bin output.bin
```

**With pacing enabled** (closer to production timing; output bytes may still match if
the run completes, but `--no-pace` is preferred for deterministic `cmp`):

```bash
./build/wg_multi_pipeline --multi a.txt out_a.txt b.bin out_b.bin
```

On exit, per-flow packet counts are printed to stderr.

---

## `make integration-test` — automated multi-file test

Same app as above, but Makefile generates random inputs and asserts `cmp` for you:

- **App:** `build/wg_multi_pipeline --no-pace --multi`
- **Flows:** 3 input/output pairs under `build/wg_test_in*.ts` / `build/wg_test_out*.ts`
- **Data:** flow 0: 20×188 B; flow 1: 5×752 B; flow 2: 40×188 B + 96 B tail
- **Path:** `ingress_push` → `FlowManager` → encode → transfer → decode → file

```bash
make integration-test
```

---

## UDP ingress — what you can test today

**Routing key:** full UDP 5-tuple `(src IP:port, dst IP:port, protocol)`.

The library maps a 5-tuple to an internal `flow_id` (array slot for queues/workers).
`flow_id` is not a second grouping rule — it is the compact index assigned on first
sight of each tuple. See `include/flow_peer_map.h` and `include/ingress_push.h`.

**Library test (5-tuple → slot assignment):**

```bash
make test    # runs test_flow_peer_map in run_tests.c
```

`test_flow_peer_map` checks that different source IPs, or the same source with
different destination ports, map to different slots.

**End-to-end UDP pipeline:** `wg_multi_pipeline --udp` binds a UDP socket,
routes each received datagram by its 5-tuple, and runs the normal pipeline.
For direct library integration, the equivalent ingress call is:

```c
FlowTuple tuple;
flow_tuple_set(&tuple, src, src_len, dst, dst_len, IPPROTO_UDP);
ingress_push_tuple(mgr, peer_map, &tuple, payload, len);
```

where `src` comes from `recvfrom` and `dst` is the local socket bind address.

The UDP demo treats `--idle-sec` as a per-flow segment boundary: it drains and
zero-pads a partial final codec block, then keeps the socket open for a later
segment from the same tuple.

The **multi-file demo** mocks UDP by assigning a fixed `flow_id` per input path
(`ingress_push(mgr, flow_id, …)`), so the rest of the pipeline is the same as after
a successful `ingress_push_tuple` lookup.

---

## `make test` — library (`run_tests.c`)

| Area | `test_*` (representative) |
|------|---------------------------|
| Packets / time | `test_packet`, `test_time_utils` |
| Queues | `test_flow_buffer`, `test_mixed_queue` |
| FlowManager | `test_flow_manager`, `test_hol_avoidance` |
| Pacing | `test_pacing_timeline`, `test_rate_match_5s` |
| Ingress | `test_flow_peer_map` (5-tuple peer map) |
| Multi-producer | concurrent push / drain scenarios |

All tests must pass before merge; CI runs them on every push.
