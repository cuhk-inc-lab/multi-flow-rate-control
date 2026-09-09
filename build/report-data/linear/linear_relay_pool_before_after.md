# hop3_relay lowrate: before vs after datagram-pool
| variant | codec | before max_pass | before ceiling | after max_pass | after ceiling | Δ ceiling |
|---|---|---:|---:|---:|---:|---:|
| default | block | 1400 | 1399.9 | 1000 | 1000.0 | -400.0 |
| default | copy | 800 | 800.0 | 2000 | 1616.5 | 816.6 |
| egress_tuned | block | 1800 | 1653.6 | 1200 | 1200.0 | -453.6 |
| egress_tuned | copy | 800 | 800.0 | 800 | 800.0 | -0.0 |
