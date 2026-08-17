# multi-flow-rate-control

C11 / pthreads multi-flow rate-matched queueing module with CircularBuffer integration.

## Prerequisites — `buffer-management-module`

This repo **does not vendor** `CircularBuffer`. You must clone
[buffer-management-module](https://gitlab.hk/scyyy-group/buffer-management-module)
as a **sibling directory** (not a git submodule):

```
work/
├── multi-flow-rate-control/     ← this repo
└── buffer-management-module/    ← required at ../buffer-management-module
```

The Makefile resolves:

```
../buffer-management-module/include/circular_buffer.h
../buffer-management-module/src/circular_buffer.c   # compiled into this project
```

You **do not** need to `make` the buffer repo first — `multi-flow-rate-control`
compiles `circular_buffer.c` directly when you run `make` here.

### Clone (new machine)

```bash
mkdir -p ~/work && cd ~/work

git clone git@gitlab.hk:Scyyy/multi-flow-rate-control.git
git clone git@gitlab.hk:scyyy-group/buffer-management-module.git
```

If `buffer-management-module` is missing or not at `../buffer-management-module`,
build fails with errors such as `circular_buffer.h: No such file or directory`.

To use a different path, change `CB_DIR` in the `Makefile`.

## Module boundary

Core library path:

```
ingress_push / ingress_push_tuple
  → FlowManager (MixedQueue → per-flow queues)
  → paced worker → write(output_fd)
```

Local file encode/decode harness: `apps/wg_multi_pipeline/` (demo).
Per-node wire hop (encode / forward / decode): `apps/wire_relay/`.

## Application — `wg_multi_pipeline`

Ingress → FlowManager → per-flow encode → buffer transfer → decode → output.

```
ingress ──► FlowManager (split + pacing) ──► raw bytes
       ──► encode ──► transfer ──► decode ──► output file
```

**Multi-file:** fixed `flow_id` per input path (`ingress_push`).  
**UDP:** full 5-tuple routing (`ingress_push_tuple`). See `apps/wg_multi_pipeline/README.md`.

### Quick test

```bash
make wg-demo
./build/wg_multi_pipeline --no-pace --multi \
  a.txt out_a.txt b.bin out_b.bin c.ts out_c.ts
cmp a.txt out_a.txt && cmp b.bin out_b.bin && cmp c.ts out_c.ts
```

```bash
./build/wg_multi_pipeline --no-pace --udp 5000 /tmp/out_ --idle-sec 3
```

In UDP mode, `--idle-sec` flushes an idle flow segment while the server keeps
listening; a later packet for that flow begins a new segment. Output paths use
`<prefix>flow<id>_segment<id>.bin`.

Automated 3-flow roundtrip: `make integration-test`.

wg-obfs handoff: **[docs/INTEGRATION_BOUNDARIES.md](docs/INTEGRATION_BOUNDARIES.md)**.  
Demo walkthrough (offline + FIFO live): **[docs/DEMOS.md](docs/DEMOS.md)**.  
Testing: **[tests/TESTING.md](tests/TESTING.md)**.
Cross-VM TCP/UDP baseline and wire-codec benchmark:
**[docs/VM_NETWORK_BENCHMARK.md](docs/VM_NETWORK_BENCHMARK.md)**.
Script catalog (purpose + usage + key env):
**[docs/SCRIPTS.md](docs/SCRIPTS.md)**.
RS codec (architecture, API, runtime profiles):
**[docs/RS_CODEC.md](docs/RS_CODEC.md)**.
Per-node wire hop:
**[apps/wire_relay/README.md](apps/wire_relay/README.md)**,
**[docs/WIRE_RELAY_PIPELINE.md](docs/WIRE_RELAY_PIPELINE.md)**.

## Common script entry points

- Baseline path check (single sender/receiver):
  - `sh scripts/run_vm_baseline.sh VM2_IP 10`
- Single-flow codec matrix (Node1 -> Node4):
  - `CODECS="copy block xor-fec rs-fec rs none" RATES="20 24 28 32" ./scripts/run_wire_matrix.sh NODE4_SSH NODE4_DATA_IP input.ts`
- Multi-flow matrix (one file per flow):
  - `CODECS="copy xor-fec rs none" RATES="10 20" ./scripts/run_wire_multiflow_matrix.sh NODE4_SSH NODE4_DATA_IP a.bin b.bin`
- Configurable multi-stream stress (YAML/JSON; see `scripts/examples/stress_lab.yaml`):
  - `./scripts/run_wire_stress.sh scripts/examples/stress_lab.yaml`

For `--codec rs`, recover uses the matrix erasure path (see
[`docs/RS_CODEC.md`](docs/RS_CODEC.md)).
Details: [`apps/wg_multi_pipeline/README.md`](apps/wg_multi_pipeline/README.md).

## Build & test

```bash
make                  # libmulti_flow.a
make test             # unit tests (run_tests.c)
make integration-test # multi-file + wire + wire_relay loopback
make wg-demo          # build wg_multi_pipeline
make wire-relay       # build apps/wire_relay (per-node encode/forward/decode)
make sanitize         # ASan + test + integration-test
make tsan             # TSan + test + integration-test
make clean
```

Wire UDP uses header **v3** (`include/wire_header.h`): 44-byte header with
`final_dst` + `ttl` in the former reserved bytes. Each node runs one
`wire_relay`: local file/FIFO encode, destination check, optional decode,
opaque forward (recode / decode-reencode hooks reserved). Application-layer
multi-hop:

```text
VM1 --source  →  VM2 forward  →  VM3 forward  →  VM4 --local-decode
     encode         (ttl--)          (ttl--)          decode
```

`wg_multi_pipeline --udp-send` / `--udp-recv` still work as a source/sink.
See [`apps/wire_relay/README.md`](apps/wire_relay/README.md) and
[`docs/WIRE_RELAY_PIPELINE.md`](docs/WIRE_RELAY_PIPELINE.md).

## Library modules

| Module | Role |
|--------|------|
| `packet` | DataPacket alloc/free |
| `flow_buffer` | Per-flow blocking packet ring |
| `mixed_queue` | Upstream mixed input |
| `flow_manager` | Dispatcher + workers + lifecycle |
| `flow_worker` | Timeline pacing + write |
| `pipe_io` | Pipe I/O and drain-to-buffer glue |
| `fd_sink` | write() with partial retry |
| `flow_peer_map` | UDP 5-tuple → internal flow slot |
| `ingress_push` | Upstream bytes → `flow_manager_push` |
| `wire_header` | Shared WGP1 wire header encode/decode (v3) |

## Pacing

```
target_dequeue = stream_start_dequeue + (pkt_ts - stream_start_enqueue)
```

Reset stream anchor after idle dequeue wait.
