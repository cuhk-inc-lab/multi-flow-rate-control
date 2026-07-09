# Integration Boundaries

Pipeline order: **multi before encode**. Reference: `apps/wg_multi_pipeline/`.

```
UDP recvfrom
  → [1] ingress_push_tuple()
  → FlowManager → per-flow pipe (raw bytes)
  → pipe_io_drain_to_buffer() → post_multi_in
  → encode → sending_out
  → [2] transmit
  → receiver_in
  → [3] receive
  → decode → receiver_out
  → [4] downstream
```

Each flow has its own pipe and CircularBuffers.

---

## 1. Upstream ingress

**API:** `ingress_push_tuple()` — `include/ingress_push.h`

```c
flow_peer_map_init(&map, max_flows);
flow_manager_init(&mgr, &cfg);
flow_manager_start(&mgr);

/* per flow: mgr.flows[i].output_fd = pipe write end */

FlowTuple tuple;
flow_tuple_set(&tuple, src, src_len, dst, dst_len, IPPROTO_UDP);
ingress_push_tuple(&mgr, map, &tuple, payload, len);
```

Related: `flow_tuple_set()` — `include/flow_peer_map.h`

- `src` = `recvfrom` peer
- `dst` = local bind (`getsockname`)
- Output: raw bytes on each flow's `output_fd` (pipe)

**Pipe → CircularBuffer (before encode):**

```c
#include "pipe_io.h"

pipe_io_drain_to_buffer(pipe_read_fd, post_multi_in);
```

**UDP runnable demo:**

```bash
make wg-demo
./build/wg_multi_pipeline --no-pace --udp 5000 /tmp/out_ --idle-sec 3
# other terminal, different source ports → different flows:
echo -n flow-a | nc -u -p 4001 127.0.0.1 5000
echo -n flow-b | nc -u -p 4002 127.0.0.1 5000
# outputs: /tmp/out_0.bin, /tmp/out_1.bin, ...
```

File demo uses `ingress_push(mgr, flow_id, data, len)` instead.

---

## 2. Post-encode transmit

**Interface:** read from per-flow `sending_out` (`CircularBuffer`)

```c
Buffer_Read(sending_out, buf, ENCODE_BLOCK);   // circular_buffer.h
// → send over network
```

Demo: `BufferTransfer_pump(sending_out, receiver_in, …)` — `buffer_transfer.h`

---

## 3. Pre-decode receive

**Interface:** write into per-flow `receiver_in` (`CircularBuffer`)

```c
// recv from network
Buffer_Write(receiver_in, buf, n);
```

Decode reads `receiver_in`, writes to `receiver_out`.

---

## 4. Post-decode downstream

**Interface:** read from per-flow `receiver_out` (`CircularBuffer`)

```c
Buffer_Read(receiver_out, buf, n);
// → downstream consumer
```

Demo: `FileDrain_pull_once(&drain, receiver_out, …)` — `file_drain.h`

---

## Per-flow buffers (after multi)

| Buffer | Connects |
|--------|----------|
| `post_multi_in` | FM pipe → encode |
| `sending_out` | encode → **[2] transmit** |
| `receiver_in` | **[3] receive** → decode |
| `receiver_out` | decode → **[4] downstream** |

Block sizes (`stream_config.h`): `PKG_SIZE` 188, `DECODE_BLOCK` 752, `ENCODE_BLOCK` 1504.

---

## Headers

| Header | Symbols |
|--------|---------|
| `ingress_push.h` | `ingress_push_tuple`, `ingress_push` |
| `flow_peer_map.h` | `FlowTuple`, `flow_tuple_set` |
| `flow_manager.h` | `flow_manager_init`, `flow_manager_start` |
| `pipe_io.h` | `pipe_io_drain_to_buffer` |
| `circular_buffer.h` | `Buffer_Read`, `Buffer_Write` |
| `buffer_transfer.h` | `BufferTransfer_pump` (demo) |
