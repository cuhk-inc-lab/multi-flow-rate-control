# Phase 4A 多流饱和（hop3 kernel vs relay）

- 码率参考 phase3 hop3 上限；精度 50；wirehair seg=2 win=8

- 原始: `build/phase4_multiflow_sat.json`

## kernel

| flows | copy | block | xor-fec | rs-fec | rs | wirehair_noack | wirehair_ack |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | 700 | 800 | 550 | 450 | 550 | 1050 | 650 |
| 4 | 150 | 150 | 150 | 150 | 150 | 550 | 350 |
| 8 | 50 | 50 | 50 | 50 | 50 | 250 | 150 |

## relay

| flows | copy | block | xor-fec | rs-fec | rs | wirehair_noack | wirehair_ack |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | 500 | 250 | 300 | 200 | 200 | 400 | 500 |
| 4 | 150 | 150 | 150 | 100 | 50 | 150 | 250 |
| 8 | 50 | 50 | 50 | — | — | 50 | 100 |

