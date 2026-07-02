# multi-flow-rate-control

C11 / pthreads multi-flow rate-matched queueing module.

Mixed upstream packets are dispatched into per-flow isolated blocking rings.
Each flow worker dequeues with timestamp-based pacing and writes via `write()`.

## Architecture

```
Producer(s) → MixedQueue → Dispatcher → FlowCircularBuffer[flow_id]
                                              ↓
                                        FlowWorker (paced) → write(fd)
```

## Project layout

```
multi-flow-rate-control/
├── include/
│   ├── packet.h
│   ├── time_utils.h
│   ├── flow_buffer.h
│   ├── mixed_queue.h
│   ├── fd_sink.h
│   ├── flow_context.h
│   ├── flow_worker.h
│   ├── dispatcher.h
│   └── flow_manager.h
├── src/
├── tests/run_tests.c
├── apps/demo/main.c
├── Makefile
└── README.md
```

## Build

```bash
make          # static library → build/libmulti_flow.a
make test     # unit + integration tests
make demo     # demo app → build/multi_flow_demo
make sanitize # ASan/UBSan test run
make clean
```

## Demo

```bash
make demo
./build/multi_flow_demo /tmp/mfrc_demo
cat /tmp/mfrc_demo/flow_0.out
cat /tmp/mfrc_demo/flow_1.out
```

Two flows emit 8 packets each with 50 ms producer spacing; workers reproduce
inter-packet timing from `enqueue_ts`.

## Library usage (sketch)

```c
FlowManager mgr;
FlowManagerConfig cfg = {
    .max_flows = 4,
    .per_flow_queue_capacity = 32,
    .mixed_queue_capacity = 64,
    .default_output_fd = STDOUT_FILENO,
    .output_fds = NULL,
    .encode_scratch_cap = 0,
};

flow_manager_init(&mgr, &cfg);
flow_manager_start(&mgr);

DataPacket *pkt = packet_create(flow_id, data, len);
flow_manager_push(&mgr, &pkt);   /* blocks if mixed queue full */

flow_manager_stop(&mgr);
flow_manager_destroy(&mgr);
```

## Integration with circular buffer project

Upstream stages can keep using `CircularBuffer` (`buffer-management-module`).
At the boundary, read bytes, frame into `DataPacket`, and call `flow_manager_push()`.
No payload copy occurs inside this module after packet creation.

## Design notes

- Queues store `DataPacket*` only.
- Blocking uses `pthread_cond_wait` inside `while` loops.
- Timestamps use `CLOCK_MONOTONIC`.
- Optional `PacketEncodeFn` per flow for future encoder hookup.
- Shutdown broadcasts condition variables and joins all threads.

## Status

All core modules implemented with tests and demo application.
