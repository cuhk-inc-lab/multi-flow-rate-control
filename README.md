# multi-flow-rate-control

C11 / pthreads multi-flow rate-matched queueing module.

Packets from multiple flows enter a mixed upstream queue, are dispatched into
per-flow isolated blocking rings, and are dequeued by per-flow workers that pace
output according to enqueue timestamps.

## Project layout

```
multi-flow-rate-control/
├── include/
│   ├── packet.h          # DataPacket alloc/free
│   └── flow_buffer.h     # per-flow blocking packet ring
├── src/
│   ├── packet.c
│   └── flow_buffer.c
├── tests/
│   └── test_flow_buffer.c
├── Makefile
└── README.md
```

Planned modules (not yet implemented): `mixed_queue`, `dispatcher`, `flow_worker`,
`flow_manager`, `fd_sink`, `timespec_util`.

## Build

```bash
make          # build static library → build/libmulti_flow.a
make test     # compile and run unit tests
make sanitize # run tests with AddressSanitizer
make clean
```

## Design notes

- Queues store `DataPacket*` only — no payload copying on route.
- Blocking enqueue/dequeue use `pthread_cond_wait` in `while` loops.
- Timestamps use `CLOCK_MONOTONIC`.
- Output will go through `write()` on a file descriptor.

## Status

- [x] `packet` — alloc, adopt, free, monotonic timestamp
- [x] `flow_buffer` — per-flow blocking ring with shutdown
- [ ] `mixed_queue`
- [ ] `dispatcher`
- [ ] `flow_worker` (rate pacing)
- [ ] `flow_manager`
- [ ] demo application
