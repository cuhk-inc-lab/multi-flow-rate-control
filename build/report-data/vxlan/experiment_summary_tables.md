# 实验数据总表（汇总）

- 环境：4×QEMU VM，数据面 VXLAN（MTU 1450），路径 VM1↔VM2↔VM3↔VM4
- Wirehair 吞吐口径（阶段3起）：`seg=2, win=8`；阶段2 loss 矩阵用 `seg=10, win=4`
- 单位：Mbps（另有说明除外）

---

## 0. 裸链路 iperf3

### 0.1 分跳 / 端到端（2026-09-07，loss≤1% 取最高）

| 链路 | TCP | UDP（低丢包） | UDP offered | loss% |
|---|---:|---:|---:|---:|
| VM1→VM2 | 2900 | 748 | 750M | 0.29 |
| VM2→VM3 | 3310 | 743 | 750M | 0.92 |
| VM3→VM4 | 2290 | 748 | 750M | 0.30 |
| **VM1→VM4 e2e** | **2040** | **498** | 500M | 0.28 |

### 0.2 VM1→VM4 复测（2026-09-08）

| 口径 | 结果 |
|---|---|
| TCP | 2.06–2.49 Gbps |
| UDP 低丢包（loss≲1%） | **500–550 Mbps** |
| UDP 接收峰值（高丢包） | ~1.7 Gbps @ offered 2G |
| 单跳 TCP VM1→VM2 | ~2.87 Gbps |
| virtio 队列 | Combined **max=1**（单队列） |

---

## 1. 阶段2 — Wirehair ACK vs 固定冗余（loss 矩阵）

- 180/180；checksum OK 142 / FAIL 38；rate 100 Mbps；`seg=10,win=4`

### direct：完成率速览（OK/3）

| loss% | ACK | fix 5% | fix 10% | fix 20% | fix 30% |
|---:|---|---|---|---|---|
| 0–3 | 3/3 | 3/3 | 3/3 | 3/3 | 3/3 |
| 5 | 3/3 | **0/3** | 3/3 | 3/3 | 3/3 |
| 10 | 3/3 | 0/3 | **0/3** | 3/3 | 3/3 |
| 20 | 3/3 | 0/3 | 0/3 | 0/3 | 1/3 |

### direct：ACK goodput（Mbps，全 OK）

| loss% | 0 | 1 | 3 | 5 | 10 | 20 |
|---:|---:|---:|---:|---:|---:|---:|
| ACK goodput | 96.1 | 90.9 | 90.9 | 85.4 | 80.7 | 70.8 |

结论：**高丢包下 ACK 完成率明显强于低固定冗余**；固定冗余要抬到 ≥2×loss 量级才稳。

---

## 2. 阶段3 — 单流吞吐上限（checksum 通过下的最大 goodput）

Wirehair：`seg=2, win=8`；hop3 为 VM4 缓冲调优后。

| path | copy | block | xor-fec | rs-fec | rs | wh_noack | wh_ack |
|---|---:|---:|---:|---:|---:|---:|---:|
| hop1_direct | 1350 | 1598 | 1100 | 711 | 908 | 1583 | 1398 |
| hop2_kernel | 1872 | 1250 | 833 | 750 | 1099 | 1766 | 1332 |
| hop2_relay | 750 | 850 | 550 | 700 | 450 | 1252 | 1055 |
| **hop3_kernel** | **1150** | **1289** | **992** | **881** | **882** | **1712** | **1069** |
| hop3_relay | 800 | 650 | 450 | 350 | 400 | 746 | 831 |

### hop3 缓冲调优前后（goodput）

| path | mode | 调前 | 调后 | Δ |
|---|---|---:|---:|---:|
| hop3_kernel | copy | 500 | 1150 | +650 |
| hop3_kernel | wh_noack | 482 | 1712 | +1230 |
| hop3_kernel | wh_ack | 523 | 1069 | +546 |
| hop3_relay | copy | 450 | 800 | +350 |
| hop3_relay | wh_noack | 1049 | 746 | −303 |

---

## 3. 阶段4A — 多流饱和（每流最大 PASS 码率 Mbps）

路径 hop3；精度 50。

### kernel

| flows | copy | block | xor-fec | rs-fec | rs | wh_noack | wh_ack |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | 700 | 800 | 550 | 450 | 550 | 1050 | 650 |
| 4 | 150 | 150 | 150 | 150 | 150 | 550 | 350 |
| 8 | 50 | 50 | 50 | 50 | 50 | 250 | 150 |

### relay

| flows | copy | block | xor-fec | rs-fec | rs | wh_noack | wh_ack |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | 500 | 250 | 300 | 200 | 200 | 400 | 500 |
| 4 | 150 | 150 | 150 | 100 | 50 | 150 | 250 |
| 8 | 50 | 50 | 50 | — | — | 50 | 100 |

---

## 4. 阶段4B — 多 src/dst stress

| 批次 | 场景 | codec | 合计目标码率 | 结果 |
|---:|---|---|---:|---|
| 1 | 混合路径多 sink | copy | ~510 | **6/6 PASS** |
| 2 | 全进 VM4 多码率 | copy | ~700 | **6/6 PASS** |
| 3 | 混合路径 | wirehair_ack | ~400 | **6/6 PASS** |

合计 **18/18 PASS**。

---

## 5. 阶段5 — 资源剖面（摘要）

### 5.1 单流 recv（kernel，峰值附近）

| codec | rate | verify | goodput | recv peak CPU% | recv peak RSS MB |
|---|---:|---|---:|---:|---:|
| copy | 1150 | OK | 866 | 136 | 7.3 |
| rs | 900 | OK | 735 | 136 | 7.5 |
| wh_noack | 1700 | OK | 1309 | 96 | 13.7 |
| wh_ack | 1050 | OK | 1013 | 84 | 11.7 |

### 5.2 多流 recv RSS 随流数（kernel）

| codec | 2 流 RSS | 4 流 RSS | 8 流 RSS | 8 流 peak CPU% |
|---|---:|---:|---:|---:|
| copy | 11.0 | 17.7 | 31.5 | 132 |
| rs | 11.5 | 18.6 | 31.8 | 184 |
| wh_noack | 19.4 | 35.0 | **77.4** | **331** |
| wh_ack | 22.4 | 34.6 | 65.1 | 243 |

### 5.3 relay 中继 CPU（单流，peak）

| codec @ rate | relay2 peak CPU% | relay3 peak CPU% |
|---|---:|---:|
| copy @800 | 204 | 200 |
| rs @400 | 224 | 201 |
| wh_noack @750 | 220 | 219 |

### 5.4 应用向抽检（4B batch1）主机 peak CPU%

| n1 | n2 | n3 | n4 | n4 NIC peak |
|---:|---:|---:|---:|---:|
| 31 | 30 | 38 | 15 | ~2560 Mbps |

---

## 总览对照

| 问题 | 答案（数量级） |
|---|---|
| 裸链路 e2e TCP 上限 | ~2.1–2.5 Gbps |
| 裸链路 e2e UDP 低丢包 | ~500–550 Mbps |
| 应用单流 hop3_kernel 最高 | wh_noack **~1712**；copy **~1150** |
| 应用单流 hop3_relay 最高 | wh_ack **~831**；copy **~800** |
| 多流 8 流每流还能 PASS | kernel copy/rs ~50；wh_noack ~250 |
| 交叉多源多宿 | 18/18 完整送达 |
| 资源热点 | recv 随流数涨 RSS；relay soft/CPU ~200% |

## 原始报告索引

| 文件 | 内容 |
|---|---|
| `build/report-data/vxlan/1d_iperf3_baseline.md` | 分跳基线 |
| `build/report-data/vxlan/1c_pfc_ack_vs_fixed_repair.md` | 阶段2 全表 |
| `build/report-data/vxlan/phase3_summary.md` | 阶段3 上限 |
| `build/report-data/vxlan/phase4a_multiflow_sat.md` | 阶段4A |
| `build/report-data/vxlan/phase4b_multiflow_stress.md` | 阶段4B |
| `build/report-data/vxlan/phase5_resource_profile.md` | 阶段5 |
| `build/report-data/vxlan/vm1_vm4_link_capacity.md` | e2e 容量复测 |
| `build/report-data/vxlan/experiment_summary_tables.md` | **本总表** |
| `build/report-data/vxlan/vxlan_era_experiment_summary.md` | VXLAN 时代长文汇总 |
