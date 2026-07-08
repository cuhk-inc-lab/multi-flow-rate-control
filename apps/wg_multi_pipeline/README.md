# wg_multi_pipeline

Canonical integration path for **multi-flow before encode**:

```
ingress (fixed flow_id or UDP peer map)
  -> FlowManager (split + pacing + backpressure)
  -> raw bytes per flow
  -> BlockCodec encode -> buffer transfer -> decode -> file
```

## Demo (files mock wg-obfs ingress)

```bash
make wg-demo
./build/wg_multi_pipeline --no-pace --multi \
  in0.ts out0.ts in1.ts out1.ts in2.ts out2.ts
```

## Future wg-obfs hook (no wg-obfs code changes)

```c
#include "ingress_push.h"

void on_udp_datagram(const struct sockaddr *src, const void *data, size_t len,
                     FlowManager *mgr, FlowPeerMap *map)
{
    (void)ingress_push_peer(mgr, map, src, salen, data, len);
}
```

See `include/flow_peer_map.h` and `include/ingress_push.h`.

Legacy `multi_flow_relay` keeps encode **before** FlowManager for comparison.
