# VXLAN 时代实验汇总（`10.10.12/23/34` 数据面）

> 本文汇总 **切换到线性 virtio 桥（`10.20.20 / 10.30.30 / 10.40.40`）之前**，基于 VXLAN overlay 的全部主要结果。  
> 对应代码开关：`WH_TOPO=vxlan`（`scripts/local/topo_lab.py` 默认）。  
> 定稿时间口径：约 2026-09-07 ~ 2026-09-08。

---

## 1. 实验机与 IP 规划

### 1.1 管理面（SSH，始终不变）

| VM | 主机名（例） | SSH | 用途 |
|---|---|---|---|
| VM1 | fyp01 | `fyp1@10.10.10.161` | 源端 / 控制入口 |
| VM2 | fyp02 | `fyp1@10.10.10.162` | 1→2 跳中继 |
| VM3 | fyp03 | `fyp1@10.10.10.163` | 2→3 跳中继 |
| VM4 | fyp04 | `fyp1@10.10.10.164` | 汇聚接收端 |

四台仓库路径均为：`/home/fyp1/work/multi-flow-rate-control`。

### 1.2 VXLAN 数据面（实验业务流量）

多跳线性逻辑：`VM1 ↔ VM2 ↔ VM3 ↔ VM4`，三段独立 `/24`，中间节点做 IP 转发或 `wire_relay`。

| 段 | 网段 | VM 侧地址 | 数据接口（典型） |
|---|---|---|---|
| hop1 | `10.10.12.0/24` | VM1=`10.10.12.1`，VM2=`10.10.12.2` | VM1 `station0`；VM2 `ap0` |
| hop2 | `10.10.23.0/24` | VM2=`10.10.23.1`，VM3=`10.10.23.2` | VM2 `station1`；VM3 `ap1` |
| hop3 | `10.10.34.0/24` | VM3=`10.10.34.1`，VM4=`10.10.34.2` | VM3 `station2`；VM4 `ap2` |

脚本中的 sink / next-hop 约定（`topo_lab.py`，`WH_TOPO=vxlan`）：

| 符号 | IP | 含义 |
|---|---|---|
| N1 | `10.10.12.1` | VM1 数据入口 |
| N2 | `10.10.12.2` | hop1 下一跳 / hop1 sink |
| N3 | `10.10.23.2` | hop2 sink（VM3） |
| N4 | `10.10.34.2` | hop3 / e2e sink（VM4） |
| RELAY2_NEXT | `10.10.23.2` | 应用中继 VM2→VM3 |
| RELAY3_NEXT | `10.10.34.2` | 应用中继 VM3→VM4 |

丢包注入：在 **VM1 `station0`** 上用 `tc netem`。

### 1.3 路径命名（与后续 linear 实验同口径）

| path | 跳数 | 转发方式 | 实际下一跳 |
|---|---:|---|---|
| `hop1_direct` | 1 | 直达 | VM1→VM2 `10.10.12.2` |
| `hop2_kernel` | 2 | 内核 IP 转发 | →VM3 `10.10.23.2` |
| `hop2_relay` | 2 | `wire_relay` @VM2 | 同上，经应用中继 |
| `hop3_kernel` | 3 | 内核 IP 转发 | →VM4 `10.10.34.2` |
| `hop3_relay` | 3 | `wire_relay` @VM2+VM3 | 同上 |

---

## 2. 网络与 VXLAN 的局限（读结果前必读）

这段拓扑**适合做协议/路径对比**，不宜把数字直接当成物理无线或商用线路容量。

### 2.1 Overlay 开销与 MTU

- 数据面是 **VXLAN overlay**（实验侧常见 **MTU 1450**），比裸 virtio 多一层封装。
- 同机对（VM1↔VM2）对照：
  - VXLAN `station0/ap0`（`10.10.12.x`）TCP ≈ **2.9 Gbps**
  - 后来的 virtio 直连 `enp6s19`（`10.20.20.x`）TCP ≈ **13 Gbps**
- 结论：**宿主机/underlay 能力远高于 VXLAN 多跳数据面**；VXLAN 数字被 encapsulation + 虚拟交换路径压低。

### 2.2 多跳叠加与 e2e UDP 瓶颈

| 口径 | 数量级 |
|---|---|
| 单跳 TCP（VXLAN） | ~2.3–3.3 Gbps |
| e2e TCP VM1→VM4 | ~2.1–2.5 Gbps |
| 单跳 UDP（loss≲1%） | ~740–750 Mbps |
| **e2e UDP（loss≲1%）** | **~500–550 Mbps** |

端到端 UDP「可用容量」大约只有单跳的 **2/3**：三跳串联 + overlay + 中间转发队列，丢包在高压下急剧上升（offered 1G 时 e2e loss 已约 13%）。

### 2.3 虚拟网卡队列与缓冲

- virtio 队列曾观察到 **Combined max=1（单队列）**，高 pps 时易成瓶颈。
- hop3 早期上限偏低，很大一块是 **VM4 socket/netdev 缓冲偏小**；把 `rmem/wmem_max=64M`、`netdev_max_backlog=5000` 对齐后，hop3_kernel 明显回升（见 §4）。
- 应用层 `wire_relay` 额外吃 CPU/拷贝，3hop relay 上限长期低于同路径 kernel。

### 2.4 对解读的约束

1. **goodput ≠ iperf UDP 峰值**：应用层还有编码开销、ACK、checksum；wirehair_ack 常「设定速率很高仍 PASS」，但实测 goodput 平台更低。
2. **早期扫到 750 / `seg=10,win=4` 的矩阵不是最终上限**；定稿以 `seg=2,win=8` + ceiling 搜索为准。
3. VXLAN 结果与后续 **linear 桥** 不可直接横向比绝对 Gbps，只能比相对趋势（kernel vs relay、ACK vs noack 等）。

---

## 3. 裸链路基线（iperf3）

### 3.1 分跳 / e2e（2026-09-07）

UDP 规则：loss≤1% 下最高接收速率。

| 链路 | TCP Mbps | UDP Mbps | offered | loss% | RTT avg ms |
|---|---:|---:|---:|---:|---:|
| VM1→VM2 (`10.10.12.2`) | 2900 | 748 | 750M | 0.29 | 0.974 |
| VM2→VM3 (`10.10.23.2`) | 3310 | 743 | 750M | 0.92 | 0.681 |
| VM3→VM4 (`10.10.34.2`) | 2290 | 748 | 750M | 0.30 | 0.996 |
| **VM1→VM4 e2e** | **2040** | **498** | 500M | 0.28 | 2.002 |

### 3.2 VM1→VM4 复测（2026-09-08）

| 口径 | 结果 |
|---|---|
| TCP | 2.06–2.49 Gbps |
| UDP 低丢包（loss≲1%） | **500–550 Mbps** |
| UDP 接收峰值（高丢包） | ~1.7 Gbps @ offered 2G |

原始：`build/report-data/vxlan/1d_iperf3_baseline.md`，`build/report-data/vxlan/vm1_vm4_link_capacity.md`。

---

## 4. 阶段 3 — 单流吞吐上限（goodput + 应用层 wire）

- Wirehair 吞吐口径：`--wh-segment-mib=2 --wh-window=8`
- payload 200 MiB；精度 ≤50 Mbps
- hop3 表为 **VM4 缓冲调优后**定稿
- **指标说明（VXLAN 时代）**：
  - **goodput**：有效源数据吞吐（checksum PASS 下最大实测）
  - **wire**：同探针应用层发出字节折算（含 FEC/编码开销，≈协议占线）
  - **NIC link_tx/rx**：当时 ceiling 探测**未采集**网卡计数；裸链路请看 §3 的 iperf3 TCP/UDP
- 主表单元格：`goodput / wire`（Mbps）；取自 **best-goodput 那次 PASS 探针**

### 4.1 上限 goodput / wire (Mbps)

| path | copy | block | xor-fec | rs-fec | rs | wh_noack | wh_ack |
|---|---:|---:|---:|---:|---:|---:|---:|
| hop1_direct | 1350 / 1392 | 1598 / 1648 | 1100 / 1418 | 711 / 1100 | 908 / 1404 | 1583 / 1808 | 1398 / 1451 |
| hop2_kernel | 1872 / 1930 | 1250 / 1289 | 833 / 1074 | 750 / 1160 | 1099 / 1700 | 1766 / 2017 | 1332 / 1382 |
| hop2_relay | 750 / 774 | 850 / 877 | 550 / 709 | 700 / 1083 | 450 / 696 | 1252 / 1430 | 1055 / 1095 |
| **hop3_kernel** | **1150 / 1186** | **1289 / 1329** | **992 / 1279** | **881 / 1364** | **882 / 1365** | **1712 / 1956** | **1069 / 1109** |
| hop3_relay | 800 / 825 | 650 / 670 | 450 / 580 | 350 / 542 | 400 / 619 | 746 / 852 | 831 / 862 |

### 4.2 仅 goodput (Mbps)（与旧表一致）

| path | copy | block | xor-fec | rs-fec | rs | wh_noack | wh_ack |
|---|---:|---:|---:|---:|---:|---:|---:|
| hop1_direct | 1350 | 1598 | 1100 | 711 | 908 | 1583 | 1398 |
| hop2_kernel | 1872 | 1250 | 833 | 750 | 1099 | 1766 | 1332 |
| hop2_relay | 750 | 850 | 550 | 700 | 450 | 1252 | 1055 |
| **hop3_kernel** | **1150** | **1289** | **992** | **881** | **882** | **1712** | **1069** |
| hop3_relay | 800 | 650 | 450 | 350 | 400 | 746 | 831 |

### 4.3 仅应用层 wire (Mbps)（同 §4.1 探针）

| path | copy | block | xor-fec | rs-fec | rs | wh_noack | wh_ack |
|---|---:|---:|---:|---:|---:|---:|---:|
| hop1_direct | 1392 | 1648 | 1418 | 1100 | 1404 | 1808 | 1451 |
| hop2_kernel | 1930 | 1289 | 1074 | 1160 | 1700 | 2017 | 1382 |
| hop2_relay | 774 | 877 | 709 | 1083 | 696 | 1430 | 1095 |
| hop3_kernel | 1186 | 1329 | 1279 | 1364 | 1365 | 1956 | 1109 |
| hop3_relay | 825 | 670 | 580 | 542 | 619 | 852 | 862 |

### 4.4 与裸链路对照（便于读「链路」）

| 口径 | 数量级 | 说明 |
|---|---|---|
| iperf e2e TCP | ~2.1–2.5 Gbps | 内核路径链路天花板（含 VXLAN） |
| iperf e2e UDP 低丢包 | ~500–550 Mbps | 可靠 UDP 可用容量 |
| 应用 hop3_kernel 最高 goodput | wh_noack ~1712 | 可高于「低丢包 UDP」——因允许更高瞬时占线/不同计时 |
| 应用 hop3_kernel 对应 wire | wh_noack ~1956 | 协议发出量；仍低于 e2e TCP |
| 应用 hop3_relay 最高 goodput | ~800–831 | 中继 CPU/拷贝瓶颈，远低于 kernel |

> 注意：应用 goodput/wire **不是** iperf 的替换指标；对外讲「链路上限」应同时引用 §3 iperf，讲「业务有效吞吐」用 goodput，讲「协议占线」用 wire。

### hop3 缓冲调优前后（摘录，goodput）

| path | mode | 调前 | 调后 | Δ |
|---|---|---:|---:|---:|
| hop3_kernel | copy | 500 | 1150 | +650 |
| hop3_kernel | wh_noack | 482 | 1712 | +1230 |
| hop3_kernel | wh_ack | 523 | 1069 | +546 |
| hop3_relay | copy | 450 | 800 | +350 |
| hop3_relay | wh_ack | 640 | 831 | +190 |

要点：

- 1hop 约 **1.1–1.6 Gbps** goodput（copy/block/wirehair）；wire 通常略高。
- FEC/rs 的 **wire ≫ goodput**（冗余开销），copy/block 二者接近。
- 2hop **kernel** 仍可 >1.5 Gbps；**relay** 多数掉到亚 Gbps。
- 3hop **kernel** 在缓冲对齐后可回升到 Gbps 级；**relay** 仍受中继限制。
- `wirehair_ack` 常扫到 5000 仍 PASS，对外应报 **实测 goodput / wire 平台**，不要报设定 rate。

原始：`build/report-data/vxlan/phase3_summary.md`，`build/wire_ceiling_search*.json` / `*_probes.json`。

---

## 5. 阶段 2 — 丢包下 ACK vs 固定冗余（Wirehair PFC）

- 条件：`seg=10, win=4`，40 MiB，rate 100 Mbps；loss 打在 VM1 `station0`
- 180/180；checksum OK 142 / FAIL 38

### direct：完成率（OK/3）

| loss% | ACK | fix 5% | fix 10% | fix 20% | fix 30% |
|---:|---|---|---|---|---|
| 0–3 | 3/3 | 3/3 | 3/3 | 3/3 | 3/3 |
| 5 | 3/3 | **0/3** | 3/3 | 3/3 | 3/3 |
| 10 | 3/3 | 0/3 | **0/3** | 3/3 | 3/3 |
| 20 | 3/3 | 0/3 | 0/3 | 0/3 | 1/3 |

### direct：ACK goodput（Mbps，全 OK）

| loss% | 0 | 1 | 3 | 5 | 10 | 20 |
|---:|---:|---:|---:|---:|---:|---:|
| ACK | 96.1 | 90.9 | 90.9 | 85.4 | 80.7 | 70.8 |

结论：**高丢包下 ACK 完成率明显强于低固定冗余**；固定冗余需抬到约 ≥2×loss 才稳。relay 拓扑趋势相同。

原始：`build/report-data/vxlan/1c_pfc_ack_vs_fixed_repair.md`。

---

## 6. 阶段 4A — 多流饱和（hop3，每流最大 PASS 码率 Mbps）

| path | flows | copy | block | rs | wh_noack | wh_ack |
|---|---:|---:|---:|---:|---:|---:|
| kernel | 2 | 700 | 800 | 550 | 1050 | 650 |
| kernel | 4 | 150 | 150 | 150 | 550 | 350 |
| kernel | 8 | 50 | 50 | 50 | 250 | 150 |
| relay | 2 | 500 | 250 | 200 | 400 | 500 |
| relay | 4 | 150 | 150 | 50 | 150 | 250 |
| relay | 8 | 50 | 50 | — | 50 | 100 |

流数↑ → 每流码率快速掉到几十～百 Mbps；relay 更早触顶。

---

## 7. 阶段 4B / 5（摘要）

- **4B stress**：三批混合路径，合计 **18/18 PASS**（copy / wirehair_ack）。
- **5 资源**：
  - kernel 单流 recv peak CPU 可到 ~80–140%；
  - 8 流 wh_noack recv peak CPU ~330%，RSS ~77 MB；
  - relay 单流中继 peak CPU 常 ~200%（双核占满量级）。

详见：`phase4a_multiflow_sat.md` / `phase4b_multiflow_stress.md` / `phase5_resource_profile.md`。

---

## 8. 一页总览（VXLAN 时代）

| 问题 | 答案（数量级） |
|---|---|
| 管理 IP | `10.10.10.161–164` |
| 数据 IP | `10.10.12/23/34` + `station*/ap*` |
| 裸链路 e2e TCP（链路） | ~2.1–2.5 Gbps |
| 裸链路 e2e UDP 低丢包（链路） | ~500–550 Mbps |
| 应用单流 hop3_kernel 最高 | goodput wh_noack **~1712** / wire **~1956**；copy **~1150 / 1186** |
| 应用单流 hop3_relay 最高 | goodput wh_ack **~831** / wire **~862**；copy **~800 / 825** |
| NIC link_tx/rx（应用探针） | **未采集**（VXLAN 时代 ceiling 无此字段） |
| 多流 8 流仍 PASS（每流） | kernel copy/rs ~50；wh_noack ~250 |
| 交叉多源多宿 | 18/18 完整送达 |
| VXLAN 相对 virtio 直连 | 同跳 TCP 约 **2.9 vs 13 Gbps**，overlay 是主瓶颈之一 |

---

## 9. 与后续 linear 拓扑的关系

| 项 | VXLAN（本文） | linear（后续） |
|---|---|---|
| 开关 | `WH_TOPO=vxlan` | `WH_TOPO=linear` |
| 数据网段 | `10.10.12/23/34` | `10.20.20 / 10.30.30 / 10.40.40` |
| 接口 | `station*/ap*` | `enp6s19/20/21` |
| 目的 | 协议与多跳语义验证 | 抬高链路天花板，减少 overlay 假象 |

**不要把两套拓扑的绝对 goodput 直接并列成「升级了多少倍」**；先比路径/codec 相对关系，再各自报链路基线。

---

## 10. 原始报告索引

| 文件 | 内容 |
|---|---|
| `build/report-data/vxlan/experiment_summary_tables.md` | VXLAN 总表（同代） |
| `build/report-data/vxlan/1d_iperf3_baseline.md` | 分跳 iperf |
| `build/report-data/vxlan/vm1_vm4_link_capacity.md` | e2e 容量复测 |
| `build/report-data/linear/iperf_10_20_20_vm1_vm2.md` | VXLAN vs virtio 对照（测在 linear 侧） |
| `build/report-data/vxlan/1c_pfc_ack_vs_fixed_repair.md` | 阶段2 loss 矩阵 |
| `build/report-data/vxlan/phase3_summary.md` | 阶段3 上限定稿 |
| `build/report-data/vxlan/1d_ceiling_search.md` | 调优前 ceiling |
| `build/report-data/vxlan/1d_hop3_buffer_retune.md` | hop3 缓冲前后 |
| `build/report-data/vxlan/phase4a_multiflow_sat.md` | 阶段4A |
| `build/report-data/vxlan/phase4b_multiflow_stress.md` | 阶段4B |
| `build/report-data/vxlan/phase5_resource_profile.md` | 阶段5 |
| `scripts/local/topo_lab.py` | IP / 路径定义源 |
