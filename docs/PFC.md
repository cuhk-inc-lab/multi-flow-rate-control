# PFC（Piecewise Fountain Code）使用说明

CLI 名称：`--codec wirehair`  
线格式：wire header **v4**  
底层编解码：`third_party/wirehair`

本文档说明 **系统结构、运行逻辑、配置与用法**。读完应能独立部署直连 / relay / 多流场景。

---

## 目录

1. [整体结构](#1-整体结构)
2. [数据单元](#2-数据单元)
3. [线格式 v4](#3-线格式-v4)
4. [发送端逻辑](#4-发送端逻辑)
5. [接收端逻辑](#5-接收端逻辑)
6. [ACK 与补包](#6-ack-与补包)
7. [滑动窗口](#7-滑动窗口)
8. [部署拓扑](#8-部署拓扑)
9. [多流](#9-多流)
10. [Relay 行为](#10-relay-行为)
11. [限速](#11-限速)
12. [日志与指标](#12-日志与指标)
13. [命令行参数](#13-命令行参数)
14. [代码文件](#14-代码文件)
15. [测试](#15-测试)
16. [约束](#16-约束)

---

## 1. 整体结构

### 1.1 分层

```text
┌──────────────────────────────────────────────────────────┐
│  可执行程序                                                 │
│  wg_multi_pipeline   sender / receiver                   │
│  wire_relay          中转 / 本地 source / 本地 decode      │
└────────────┬─────────────────────────┬───────────────────┘
             │                         │
┌────────────▼────────────┐ ┌──────────▼──────────┐
│  wire_udp.c             │ │  relay.c            │
│  UDP 收发、限速、滑动窗口   │ │  转发、ACK 回程、双队列  │
│  recvmmsg、decode 线程    │ │  per-flow 路由学习    │
└────────────┬────────────┘ └──────────┬──────────┘
             │                         │
┌────────────▼─────────────────────────▼──────────┐
│  wirehair_segment.c                              │
│  分段、编码/解码、窗口槽位、ACK 生成               │
└────────────┬────────────────────────────────────┘
             │
┌────────────▼────────────┐
│  third_party/wirehair   │
│  单段喷泉编解码           │
└─────────────────────────┘
```

### 1.2 传输流程（单文件）

```text
文件
  → 按 segment_bytes 切段（segment 0, 1, 2, …）
  → 每段独立 Wirehair 编码 → 多个 UDP DATA 包
  → 接收端解码恢复 → 按 segment id 顺序写文件
  → 最后发 END（block_id = 总段数）
```

### 1.3 两个程序入口

| 程序 | 角色 |
| --- | --- |
| `wg_multi_pipeline` | 端到端 sender（`--udp-send`）/ receiver（`--udp-recv`）/ 多流（`--udp-send-multi`） |
| `wire_relay` | 中间跳透明转发；也可作 `--source` 发送端或 `--local-decode` 接收端 |

与 v3 编解码器（`copy` / `xor-fec` / `rs` 等）**不兼容**，收发必须同为 `--codec wirehair`。

---

## 2. 数据单元

### 2.1 段（Segment）

| 项 | 说明 |
| --- | --- |
| 默认大小 | 10 MiB（`--wh-segment-mib`） |
| 编号 | `block_id`，从 0 递增 |
| 编码 | 每段一个独立 Wirehair session |
| 最小填充 | 不足 `2 × 1370` 字节会 pad 到该长度再编码 |

### 2.2 包（Packet）

| 项 | 值 |
| --- | --- |
| 载荷上限 | 1370 字节（`WH_PACKET_SIZE`） |
| 段内编号 | `shard_index` = Wirehair packet id |
| 类型 | source 包（id < source_packets）或 repair 包（id ≥ source_packets） |

### 2.3 包数量计算

```text
source_packets = max(2, ceil(segment_bytes / 1370))
repair_packets = max(2, ceil(source_packets × repair_pct / 100))
```

| 模式 | 发送总量 |
| --- | --- |
| 无 ACK | `source_packets + repair_packets`，发完即进下一段 |
| 有 ACK | 先发完 source；repair 按 5% 微轮追加，直到收到 ACK 或达到 `source_packets` 上限 |

### 2.4 默认常量（`wirehair_segment.h`）

| 常量 | 值 |
| --- | --- |
| `WH_SEGMENT_WINDOW_DEFAULT` | 8 |
| `WH_SEGMENT_WINDOW_MAX` | 16 |
| `WH_ACK_REPAIR_ROUND_PCT` | 5（ACK 每轮补包比例） |
| `WH_ACK_INITIAL_WAIT_MS` | 50 |
| `WH_ACK_REPAIR_WAIT_MS` | 100 |

---

## 3. 线格式 v4

定义：`include/wire_header.h`

### 3.1 头字段（52 字节）

| 字段 | PFC 含义 |
| --- | --- |
| `version` | 4 |
| `type` | `DATA` / `END` / `ACK` |
| `final_dst` | 最终目的节点 id |
| `ttl` | 剩余跳数（relay 每跳减 1） |
| `flow_id` | 流 id（接收端解复用键） |
| `block_id` | **段 id** |
| `shard_index` | **段内包 id** |
| `shard_count` | 本段 advertised 包 id 上限 |
| `payload_len` | 载荷长度 |
| `origin_node` | 源节点 id（ACK 的 `final_dst`） |
| `flags` | `ACK_REQUEST` / `RETURN_PATH` |
| `segment_bytes` | 本段原始字节数 |

路由看 **`final_dst` + `ttl`**，不看 UDP 目的 IP。UDP 发往**下一跳**地址，`final_dst` 填最终 sink。

### 3.2 报文类型

| type | 载荷 | 作用 |
| --- | --- | --- |
| `DATA` | ≤1370 B | source 或 repair |
| `END` | 无 | `block_id` = 段总数 |
| `ACK` | 无 | 通知 `(flow_id, segment_id)` 已恢复 |

### 3.3 标志位

| flag | 谁设置 | 作用 |
| --- | --- | --- |
| `WIRE_FLAG_ACK_REQUEST` | 发送端 | 接收端恢复后发 ACK |
| `WIRE_FLAG_RETURN_PATH` | 接收端 / relay | 标记 ACK，沿回程转发 |

---

## 4. 发送端逻辑

实现：`wire_udp.c` → `wirehair_udp_send_file()`，段状态：`wirehair_segment.c` → `WirehairSegmentTx`

### 4.1 主循环

```text
while 有数据或窗口内还有未 ACK 段:
  轮询 ACK
  释放已 ACK 的 base_segment 槽位
  若窗口未满 → 读下一段、创建 WirehairSegmentTx
  对每个活跃槽位:
    source 未发完 → 批量发 source（每批最多 32 包）
    source 已发完且到 repair_due → 发一轮 5% repair
send END
```

### 4.2 无 ACK 模式

每段串行：

```text
发 source_packets 个包
  → 发 repair_packets 个包
  → 下一段
```

repair 总量由 `--wh-repair-pct` 决定，发完即结束。

### 4.3 有 ACK 模式（滑动窗口）

```text
允许在途段: [base_segment, base_segment + window)

槽位 ring: slots[segment_id % window]

每槽位状态机:
  创建 tx → 发 source → 等待 50ms → [无 ACK] 发 5% repair 轮 → 等待 100ms → 重复
  收到 ACK → 标记 acked
  base_segment 槽位 acked → 释放、base_segment++

repair 总量达到 source_packets 仍无 ACK → ack_timeout，发送失败
```

ACK 登记：`wirehair_udp_ack_mark` / `wirehair_udp_ack_drain`，支持乱序、重复 ACK。

多流时共享 pacer 的 `max_inflight == window`，全进程在途段总数 ≤ window。

### 4.4 发送端输出

```text
wirehair-send: source_bytes=… segments=… repair_sent=… wire_bytes=…
               ack=on|off status=ok|failed repair_rounds=… send_window_hwm=…
```

---

## 5. 接收端逻辑

实现：`wirehair_udp_recv_file()` + `wirehair_segment_receiver_*`

### 5.1 线程结构

```text
主线程（RX）
  poll + recvmmsg（每批最多 64 个 UDP 包）
  → 校验 v4 头
  → 按 flow_id 入队

每 flow 一个 decode worker 线程
  → wirehair_segment_receiver_ingest()
  → 恢复成功 → 写文件 + 发 ACK（到数据包来源地址）
```

### 5.2 每 flow 状态

| 状态 | 含义 |
| --- | --- |
| `next_emit_segment` | 下一个待写入磁盘的段 id |
| `slots[window]` | 最多 window 个并行解码槽 |
| `ahead_window_drops` | 超出窗口的包丢弃计数 |

### 5.3 收包处理表

| 条件 | 动作 |
| --- | --- |
| `segment_id < next_emit_segment` | 若带 `ACK_REQUEST` → 重发 ACK |
| `segment_id ≥ next_emit_segment + window` | 丢弃，`ahead_window_drops++` |
| 窗口内新段 | 分配 decoder，`wirehair_decode()` |
| 凑够符号 | `wirehair_recover()` → ACK → 尝试顺序写出 |
| 收到 `END` | 记录总段数；`next_emit_segment` 追平后完成 |

段可乱序恢复，但**按 segment id 顺序写文件**。

---

## 6. ACK 与补包

### 6.1 ACK 报文内容

```text
type         = ACK
final_dst    = origin_node（来自 DATA 头）
flags        = RETURN_PATH
flow_id      = 流 id
block_id     = 段 id
segment_bytes = 恢复的字节数
```

### 6.2 发送端 ACK 接收

- 绑定 `--ack-port`（默认系统分配）
- 发送过程中轮询 ACK socket
- 多流时各 flow 可有不同源端口；relay 按 `flow_id` 记回程

### 6.3 单段时序（ACK 模式）

```text
发完该段全部 source 包
  → 等 50ms
  → 无 ACK：发 5% repair 轮（repair_rounds++）
  → 等 100ms
  → 重复，直到 ACK 或 repair 达 source_packets 上限
```

| 参数 | ACK 模式作用 |
| --- | --- |
| `--wh-repair-pct` | 无 ACK 时的 repair 预算；ACK 模式仅参与 `shard_count` 校验与 100% 上限 |
| 5% 微轮 | 固定用 `WH_ACK_REPAIR_ROUND_PCT`，与 `--wh-repair-pct` 无关 |

### 6.4 `repair_sent` 含义

只统计 **repair 包 id** 的实际发送数（不含 source 包）。路径无丢包时常为 0。

---

## 7. 滑动窗口

发送端与接收端共用 `--wh-window`（默认 8，最大 16）。

```text
发送端准入: segment_id ∈ [base_segment, base_segment + window)
接收端接受: segment_id ∈ [next_emit_segment, next_emit_segment + window)
```

| 事件 | 发送端 | 接收端 |
| --- | --- | --- |
| 窗口满 | 停止读新段 | — |
| 超前发包 | — | 丢弃，`ahead_window_drops++` |
| repair 耗尽无 ACK | `ack_timeout`，失败退出 | — |

接收端每 flow 峰值内存约 `window × segment_bytes`（默认约 80 MiB）。

---

## 8. 部署拓扑

### 8.1 直连

```bash
# Node2 接收
./build/wg_multi_pipeline --codec wirehair --wh-ack \
  --udp-recv 9000 /tmp/out.bin --local-node-id 4 --idle-sec 5 --strict

# Node1 发送
./build/wg_multi_pipeline --codec wirehair --wh-ack --ack-port=9100 \
  --wh-segment-mib=2 --wh-repair-pct=20 --wh-window=8 \
  --final-dst 4 --ttl 8 --rate-mbps 1000 \
  --udp-send 10.10.12.2 9000 input.bin
```

```text
Node1 --udp-send--> Node2 --udp-recv
Node2 --ACK--------> Node1 --ack-port
```

### 8.2 Relay 三跳

```text
Node1 sender
  → Node2 relay（opaque，学 per-flow 回程）
  → Node3 receiver

ACK: Node3 → relay → Node1 --ack-port
```

```bash
# Node2 中转
./build/wire_relay --local-node-id 2 --listen 9000 \
  --next-hop 10.10.23.2:9000

# Node3 接收
./build/wg_multi_pipeline --codec wirehair --wh-ack \
  --udp-recv 9000 /tmp/out.bin --local-node-id 4 --strict

# Node1 发送
./build/wg_multi_pipeline --codec wirehair --wh-ack --ack-port=9100 \
  --final-dst 4 --ttl 8 --udp-send 10.10.12.2 9000 input.bin
```

`--return-hop` 仅在 relay **尚未学到** 该 `flow_id` 回程地址时使用。

### 8.3 实验 VM（矩阵脚本）

| 节点 | SSH | 数据网 |
| --- | --- | --- |
| Node1 | `fyp1@10.10.10.161` | `10.10.12.1` |
| Node2 | `fyp1@10.10.10.162` | `10.10.12.2` |
| Node3 | `fyp1@10.10.10.163` | `10.10.23.2` |

---

## 9. 多流

### 9.1 发送

```bash
./build/wg_multi_pipeline --codec wirehair --wh-ack --udp-send-multi \
  --flow "0:10.10.12.2:9000:input0.bin:500" \
  --flow "1:10.10.12.2:9000:input1.bin:500"
```

- `flow_id` 写在 wire 头里
- 限速：各 flow `rate_mbps` **相加**为聚合线速上限
- 在途段：全进程共享 window

### 9.2 接收

```bash
./build/wg_multi_pipeline --codec wirehair --wh-ack \
  --udp-recv 9000 /tmp/out_ --max-flows 4 --local-node-id 4
```

- 解复用只看 `flow_id`，不看 UDP 五元组
- 输出文件：`{prefix}src_{ip}_p{port}_flow_{id}.{suffix}`

---

## 10. Relay 行为

### 10.1 转发路径

```text
UDP 入 → ttl==0 丢弃
      → final_dst==local → local-decode / local-source ACK 投递
      → 否则 TTL-- → DATA 入 data egress → sendto(next-hop)
```

DATA / repair / END：**不解码**，透明转发。

### 10.2 ACK 回程

```text
ACK + RETURN_PATH 入 ack egress（容量 1024）
  → 查 per-flow 学到的上一跳 UDP 地址
  → sendto 回程
  → 若无路由，用 --return-hop
```

TX 调度：优先 ACK；连续 8 个 ACK 后若 DATA 在等，发 1 个 DATA。

### 10.3 Relay 作 source

`--source` 本地编码注入，ACK 到达本地时写入 `source_ack_valid[segment_id % 16]`，供编码线程查询。

---

## 11. 限速

`pace_to_source_rate()` 按 **wire 字节**（头+载荷）限速。

| 场景 | 行为 |
| --- | --- |
| `--udp-send --rate-mbps N` | 单流线速 ≤ N Mbps |
| `--udp-send-multi` | 聚合线速 ≤ 各 flow rate 之和 |
| `--no-pace` | 不限速 |
| repair 包 | 计入 wire_bytes |

---

## 12. 日志与指标

### 12.1 发送端 summary

| 字段 | 含义 |
| --- | --- |
| `source_bytes` | 文件有效载荷字节 |
| `segments` | 段数 |
| `repair_sent` | 发出的 repair 包数 |
| `wire_bytes` | 线上总字节（含头） |
| `repair_rounds` | ACK 补包轮数 |
| `send_window_hwm` | 在途段数峰值 |
| `status` | `ok` / `failed` |

### 12.2 脚本衍生指标

| 指标 | 计算 |
| --- | --- |
| goodput | `source_bytes × 8 / wall_s / 1e6` Mbps |
| wire | `wire_bytes × 8 / wall_s / 1e6` Mbps |

### 12.3 接收端

| 字段 | 含义 |
| --- | --- |
| `ahead_window_drops` | 超出接收窗口的丢包数 |
| `socket_rcvbuf` | 内核实际 SO_RCVBUF |

### 12.4 失败标志

| 日志 | 含义 |
| --- | --- |
| `ack_timeout=yes` | 段 repair 达上限仍无 ACK |
| `status=failed` | 发送失败 |

### 12.5 Relay summary

`ack_egress_*`、`data_egress_*`：队列高水位、等待时间、入队计数。

---

## 13. 命令行参数

### 13.1 `wg_multi_pipeline`

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `--codec wirehair` | — | 启用 PFC |
| `--wh-segment-mib=N` | 10 | 段大小（MiB） |
| `--wh-repair-pct=P` | 10 | 无 ACK repair 比例；ACK 模式参与上限计算 |
| `--wh-window=N` | 8 | 滑动窗口（最大 16） |
| `--wh-ack` / `--no-wh-ack` | 关 | 段级 ACK |
| `--ack-port=N` | 自动 | 发送端 ACK 监听端口 |
| `--final-dst=N` | 4 | 最终目的节点 |
| `--ttl=N` | 8 | 跳数预算 |
| `--local-node-id=N` | 4 | 本节点 id |
| `--flow-id=N` | 0 | 单流 wire flow id |
| `--rate-mbps=N` | 不限 | 线速上限 |
| `--udp-send H P FILE` | — | 发送 |
| `--udp-recv P OUT` | — | 接收 |
| `--udp-send-multi --flow …` | — | 多流发送 |
| `--max-flows=N` | — | 接收最大流数 |
| `--strict` | — | 输出不完整则失败 |
| `--idle-sec=N` | — | 接收空闲超时 |
| `--no-pace` | — | 关闭限速 |

### 13.2 `wire_relay`

| 参数 | 说明 |
| --- | --- |
| `--local-node-id` | 本节点 id |
| `--listen` / `--next-hop` | 监听与下一跳 |
| `--local-decode --codec wirehair` | 本地解码落盘 |
| `--source FILE --codec wirehair` | 本地编码发送 |
| `--wh-segment-mib` / `--wh-repair-pct` / `--wh-window` / `--wh-ack` | 同 wg_multi_pipeline |
| `--return-hop HOST:PORT` | ACK 回程 fallback |
| `--final-dst` / `--ttl` | `--source` 时必填 |

---

## 14. 代码文件

| 文件 | 职责 |
| --- | --- |
| `include/wire_header.h` | v4 头定义 |
| `wirehair_segment.h/c` | 段编解码、窗口、ACK |
| `wire_udp.h/c` | UDP 收发、滑动窗口、限速、recvmmsg |
| `pipeline.c` | 多流调度、共享 pacer |
| `main.c` | CLI |
| `relay.c` | 转发、ACK 回程、路由学习 |
| `egress_queue.c` | ACK/DATA 双队列 |
| `local_source.c` / `local_decode.c` | relay 内嵌收发 |
| `fec_transport.c` | 无 socket 库 API（见 `FEC_TRANSPORT.md`） |
| `scripts/vm_wirehair_full_matrix.py` | VM 回归矩阵 |

库嵌入（不用 `wg_multi_pipeline` 二进制）：见 [`FEC_TRANSPORT.md`](FEC_TRANSPORT.md)。  
库层无滑动窗口；窗口在 `wire_udp.c` 实现。

---

## 15. 测试

### 15.1 本地

```bash
make wg-demo wire-relay
make integration-test
./build/wirehair_segment_tests
```

### 15.2 VM 矩阵

```bash
python3 scripts/vm_wirehair_full_matrix.py
WH_MATRIX_ACK_ONLY=1 python3 scripts/vm_wirehair_full_matrix.py   # 只跑 ACK
WH_MATRIX_FRESH=1 python3 scripts/vm_wirehair_full_matrix.py      # 忽略旧结果
```

维度：direct / relay × ACK on/off × 1/2/4 流 × 500/1000/2000/5000 Mbps  
输出：`build/wirehair_full_matrix.json`

### 15.3 主机调优（跑满速前）

```bash
sudo sysctl -w net.core.rmem_max=67108864
sudo sysctl -w net.core.wmem_max=67108864
sudo sysctl -w net.core.netdev_max_backlog=5000
```

确认接收日志 `socket_rcvbuf` 接近 64M。

---

## 16. 约束

- 仅 wire v4；不可与 v3 codec 混用
- 每段 source_packets ≤ 64000
- `shard_count` ≤ uint16 上限
- window ≤ 16
- ACK 模式单段 repair 上限 = source_packets（100%）
- 无鉴权；用 `sha256sum` 或 `--strict` 校验完整性
- relay 队列满时丢包（`drop_egress_full`），不保证送达

---

## 相关文档

| 文档 | 内容 |
| --- | --- |
| [`apps/wg_multi_pipeline/README.md`](../apps/wg_multi_pipeline/README.md) | 程序 CLI 速查 |
| [`apps/wire_relay/README.md`](../apps/wire_relay/README.md) | relay CLI |
| [`WIRE_RELAY_PIPELINE.md`](WIRE_RELAY_PIPELINE.md) | relay pipeline 细节 |
| [`FEC_TRANSPORT.md`](FEC_TRANSPORT.md) | 库 API |
| [`SCRIPTS.md`](SCRIPTS.md) | 脚本说明 |
