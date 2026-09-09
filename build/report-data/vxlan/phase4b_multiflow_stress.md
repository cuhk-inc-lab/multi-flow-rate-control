# Phase 4B — 多 src/dst + 混合码率 stress

- 工具：`scripts/local/run_wire_stress.sh`（支持 `wg_send_extra` / `wg_recv_extra`）
- 配置：`scripts/local/stress_4b_batch{1,2,3}_*.json`
- 原始结果：`build/wire-stress-4b-batch{1,2,3}/`
- 口径：SHA256 全匹配 = PASS；路径经 kernel 转发（非 app relay）

## 总览

| 批次 | 场景 | codec | 流数 | 结果 |
|---:|---|---|---:|---|
| 1 | 混合路径（多 sink） | copy | 6 | **6/6 PASS** |
| 2 | 同 sink（全进 VM4）多码率 | copy | 6 | **6/6 PASS** |
| 3 | 混合路径 + ACK | wirehair (`seg=2,win=8,--wh-ack`) | 6 | **6/6 PASS** |

## 批次 1 — copy 混合路径

目标码率合计约 **510 Mbps**（交叉 1↔2↔3↔4）。

| path | rate (Mbps) | file | status |
|---|---:|---|---|
| node1→node4 | 150 | input_10m.ts | PASS |
| node2→node4 | 100 | input_10m.ts | PASS |
| node1→node2 | 80 | input_1m.ts | PASS |
| node1→node3 | 80 | input_1m.ts | PASS |
| node2→node3 | 50 | input_1m.ts | PASS |
| node1→node4 | 50 | input_1m.ts | PASS |

## 批次 2 — copy 同 sink（VM4）多码率

node1/2/3 同时打 node4；目标码率合计约 **700 Mbps**；单 recv 进程 demux 6 流。

| path | rate (Mbps) | file | status |
|---|---:|---|---|
| node1→node4 | 50 | input_1m.ts | PASS |
| node1→node4 | 100 | input_10m.ts | PASS |
| node2→node4 | 150 | input_10m.ts | PASS |
| node2→node4 | 200 | input_20m.ts | PASS |
| node3→node4 | 80 | input_1m.ts | PASS |
| node3→node4 | 120 | input_10m.ts | PASS |

## 批次 3 — wirehair_ack 混合路径

相对批次 1 码率打折（约 **400 Mbps** 合计）；`final_dst=4`，`local-node-id=4`。

| path | rate (Mbps) | file | status |
|---|---:|---|---|
| node1→node4 | 120 | input_10m.ts | PASS |
| node2→node4 | 80 | input_10m.ts | PASS |
| node1→node2 | 60 | input_1m.ts | PASS |
| node1→node3 | 60 | input_1m.ts | PASS |
| node2→node3 | 40 | input_1m.ts | PASS |
| node1→node4 | 40 | input_1m.ts | PASS |

## 结论

- 在 4A 饱和点以下的交叉/多码率负载下，**copy 与 wirehair_ack 均能完整送达**（18/18 PASS）。
- 同 sink 6 流 demux（批次 2）与多 sink 交叉（批次 1/3）均未出现校验失败。
- 监控图未生成（VM 缺 `wire_stress_charts`）；不影响 PASS/FAIL 结论。
- 相对 4A：4B 验证的是**异源异宿并发正确性**，不是再压单路径上限。
