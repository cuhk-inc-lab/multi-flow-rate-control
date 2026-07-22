# 六流并发 Wire 测试指南（iperf-like）

面向本实验室拓扑：**Node1 → Node2 → Node3 → Node4**。  
脚本从 **Node1** 一键拉起收发、采中继网卡、校验 hash，并落盘结果。

---

## 1. 拓扑与六条流

```
Node1 (10.10.12.1) ──ap/station──> Node2 (10.10.12.2 / 10.10.23.1)
                                      │
                                      ├──> Node3 (10.10.23.2 / 10.10.34.1) ──> Node4 (10.10.34.2)
                                      └──> 本机 loopback (127.0.0.1)
```

| Stream | 发送端 | 目的 | 默认端口 | 说明 |
| --- | --- | --- | ---: | --- |
| 1 | Node1 | Node4 | 9000 | 三跳 |
| 2 | Node2 | Node4 | 9000 | 两跳 |
| 3 | Node1 | Node2 | 9000 | 一跳 |
| 4 | Node2 | Node2 | **9001** | 本机 loopback（与 9000 分开，避免挤坏） |
| 5 | Node1 | Node3 | 9000 | 两跳 |
| 6 | Node1 | Node4 | 9000 | 三跳，时长更短 |

中继监控网卡（默认已写进脚本）：

- Node2: `ap0`（连 Node1）、`station1`（连 Node3）
- Node3: `ap1`（连 Node2）、`station2`（连 Node4）

---

## 2. 各节点准备（只需做一次 / 更新代码后）

在 **Node1 / Node2 / Node3 / Node4** 都执行：

```bash
cd ~/work/multi-flow-rate-control
git pull
make wg-demo
```

Node1 需要能免密 SSH 到另外三台（脚本用 `BatchMode=yes`）。

---

## 3. 跑一次（推荐基线）

在 **Node1**：

```bash
cd ~/work/multi-flow-rate-control

NODE2_SSH=fyp1@10.10.10.162 NODE2_IP=10.10.12.2 \
NODE3_SSH=fyp1@10.10.10.163 NODE3_IP=10.10.23.2 \
NODE4_SSH=fyp1@10.10.10.164 NODE4_IP=10.10.34.2 \
  ./scripts/run_iperf_like_wire.sh input-128m.ts
```

默认：

- 编码：`copy`（完整性基线，易 6/6 PASS）
- 码率：每路 `1` Mbps
- 时长：多数 5 s，stream6 为 3 s

成功时末尾会打印 `PASS: 6 / 6`，并给出结果目录，例如：

`build/iperf-like-wire-YYYYMMDD-HHMMSS/`

---

## 4. 结果怎么看

设 `R=build/iperf-like-wire-...`（换成你的目录）。

### 4.1 总览

```bash
cat "$R/summary.md"
column -t -s, "$R/streams.csv" | less -S
```

`streams.csv` 重要列：

| 列 | 含义 |
| --- | --- |
| `status` | PASS/FAIL（输出文件 sha256 是否与发送 payload 一致） |
| `e2e_p95_us` | 端到端延迟 p95（微秒） |
| `jitter_p95_us` | 抖动 p95 |
| `dropped_groups` | 丢组数（0 最好） |
| `recovered_groups` | FEC 恢复组数（`copy` 时一般为 0） |

大致跳数与延迟关系（`copy` @ 1 Mbps 参考）：一跳 < 两跳 < 三跳。

### 4.2 中继节点（Node2 / Node3）

```bash
# 每秒 rx/tx（bps）与 drop
column -t -s, "$R/monitor/node2-ifaces.csv" | less -S
column -t -s, "$R/monitor/node3-ifaces.csv" | less -S

# 只看高峰
awk -F, 'NR==1 || $3+0>1e6 || $4+0>1e6' "$R/monitor/node2-ifaces.csv"
awk -F, 'NR==1 || $3+0>1e6 || $4+0>1e6' "$R/monitor/node3-ifaces.csv"

# 测试前后计数器
diff -u "$R/monitor/node2-link-before.txt" "$R/monitor/node2-link-after.txt"
diff -u "$R/monitor/node3-link-before.txt" "$R/monitor/node3-link-after.txt"
```

`summary.md` 里的 **Relay peaks** 就是上述 CSV 的峰值摘要。

注意：`copy` / `block` 线速大约是源码率的 **2×**（4 数据包扩成 8）；`xor-fec` 约 **1.25×**。

### 4.3 收发日志与输出文件

```bash
grep -E "output_bytes|dropped_groups|latency end_to_end:" "$R/logs/"*-recv.log
ls -l "$R/out/n2" "$R/out/n3" "$R/out/n4"
```

---

## 5. 改编码 / 改码率（单次）

### 换编码

```bash
CODEC=copy    ...   # 基线（推荐先跑）
CODEC=block   ...   # 与 copy 同结构，多算术
CODEC=xor-fec ...   # 4 数据 + 1 XOR 校验
CODEC=rs-fec  ...   # RS(4,2)
```

示例：

```bash
CODEC=xor-fec RATE_MBPS=1 \
NODE2_SSH=fyp1@10.10.10.162 NODE2_IP=10.10.12.2 \
NODE3_SSH=fyp1@10.10.10.163 NODE3_IP=10.10.23.2 \
NODE4_SSH=fyp1@10.10.10.164 NODE4_IP=10.10.34.2 \
  ./scripts/run_iperf_like_wire.sh input-128m.ts
```

### 换码率 / 时长

| 环境变量 | 默认 | 含义 |
| --- | ---: | --- |
| `RATE_MBPS` | 1 | 六路统一源码率（Mbps） |
| `DURATION_S` | 5 | stream 1–5 时长（秒） |
| `DURATION_SHORT_S` | 3 | stream 6 时长（秒） |

```bash
CODEC=copy RATE_MBPS=2 \
NODE2_SSH=fyp1@10.10.10.162 NODE2_IP=10.10.12.2 \
NODE3_SSH=fyp1@10.10.10.163 NODE3_IP=10.10.23.2 \
NODE4_SSH=fyp1@10.10.10.164 NODE4_IP=10.10.34.2 \
  ./scripts/run_iperf_like_wire.sh input-128m.ts
```

建议：先 `copy` 从小码率往上加；再在同一码率下换 `xor-fec` / `rs-fec` 看丢包与恢复。

---

## 6. 一次扫多组（矩阵脚本）

在 Node1 用矩阵脚本自动扫 **编码 × 码率**，并生成总表：

```bash
CODECS="copy xor-fec" RATES="1 2" \
NODE2_SSH=fyp1@10.10.10.162 NODE2_IP=10.10.12.2 \
NODE3_SSH=fyp1@10.10.10.163 NODE3_IP=10.10.23.2 \
NODE4_SSH=fyp1@10.10.10.164 NODE4_IP=10.10.34.2 \
  ./scripts/run_iperf_like_matrix.sh input-128m.ts
```

输出目录示例：`build/iperf-like-matrix-YYYYMMDD-HHMMSS/`

- `matrix.csv` / `matrix.md`：每组 PASS 数、中继峰值、结果路径
- `runs/`：每一组完整的单次结果（同第 4 节结构）

可选环境变量：

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `CODECS` | `copy xor-fec` | 空格分隔 |
| `RATES` | `1 2` | 空格分隔的 `RATE_MBPS` |
| `EXTRA_ENV` | 空 | 额外传给单次脚本，如 `DURATION_S=4` |

---

## 7. 推荐实验顺序（报告可用）

1. **基线**：`CODEC=copy RATE_MBPS=1` → 期望 6/6 PASS，记录延迟与中继峰值  
2. **抬码率**：`copy` + `RATE_MBPS=2`（再 3…）→ 看从哪一档开始 FAIL / dropped 上升  
3. **FEC**：同码率下 `xor-fec`、`rs-fec` → 对比 PASS、`recovered_groups`、`dropped_groups`  
4. **中继**：对比 Node2/Node3 的 `*-ifaces.csv` 峰值与 drop  

报告里建议固定写清：git commit、CODEC、RATE_MBPS、PASS x/6、e2e p95、relay peaks、dropped_groups。

---

## 8. 常见问题

| 现象 | 可能原因 | 怎么办 |
| --- | --- | --- |
| 到 Node4 全 FAIL / 无包 | 码率过高或路径拥塞 | 降 `RATE_MBPS`；先确认 `ping 10.10.34.2` |
| 只有 Node2 的流 FAIL | 本机收发+转发过载 | 保持 loopback 用 9001；降码率 |
| hash FAIL 但长度对 | UDP 丢包/损坏超过 FEC | 看 `dropped_groups`；换 `copy` 对比 |
| Relay peaks = NA | 网卡名不对 | 在 Node2/3 执行 `ip -br a`，设 `NODE2_IFACES` / `NODE3_IFACES` |
| `git pull` 被未跟踪文件挡住 | 本地有同名未跟踪文件 | `rm` 或挪走后再 pull |

---

## 9. 相关文件

- 单次脚本：`scripts/run_iperf_like_wire.sh`
- 矩阵脚本：`scripts/run_iperf_like_matrix.sh`
- 串行单流矩阵（另一套）：`scripts/run_wire_matrix.sh`
- 更广的跨 VM 说明：`docs/VM_NETWORK_BENCHMARK.md`
