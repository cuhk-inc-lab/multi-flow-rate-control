# 阶段3 单流 codec×rate 最大通过吞吐

- 路径: VM1→VM4 e2e (`10.10.34.2`)，input 128MiB
- 原始: `build/wire-matrix-stage3-20260907-125206/`
- iperf3 e2e UDP 基线 (loss≤1%): **498 Mbps** @500M offered

| codec | 最大通过 target Mbps | actual source Mbps | wire Mbps | 占 UDP 基线 % | 下一档 FAIL |
|---|---:|---:|---:|---:|---|
| copy | 600 | 600.0 | 618.9 | 120.5 | 750 (FAIL) |
| block | 500 | 500.0 | 515.7 | 100.4 | 600 (FAIL) |
| xor-fec | 500 | 500.0 | 644.6 | 100.4 | 600 (FAIL) |
| rs-fec | 500 | 500.0 | 773.6 | 100.4 | 600 (FAIL) |
| rs | 400 | 400.0 | 618.9 | 80.3 | 500 (FAIL) |
| wirehair | 600 | NA | NA | 120.5 | 750 (FAIL) |

## 全矩阵 PASS/FAIL

| codec \ rate | 100 | 200 | 300 | 400 | 500 | 600 | 750 |
|---|---:|---:|---:|---:|---:|---:|---:|
| copy | PASS | PASS | PASS | PASS | PASS | PASS | FAIL |
| block | PASS | PASS | PASS | PASS | PASS | FAIL | FAIL |
| xor-fec | PASS | PASS | FAIL | PASS | PASS | FAIL | FAIL |
| rs-fec | PASS | PASS | PASS | PASS | PASS | FAIL | FAIL |
| rs | PASS | PASS | PASS | PASS | FAIL | FAIL | FAIL |
| wirehair | PASS | PASS | PASS | PASS | PASS | PASS | FAIL |
