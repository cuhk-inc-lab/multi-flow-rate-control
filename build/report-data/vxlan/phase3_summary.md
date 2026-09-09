# 阶段 3 汇总：单流吞吐上限（hop × kernel/relay × codec × wirehair ACK）

## 实验设定（最终口径）

- **目标**：测 checksum 通过前提下的吞吐上限（最大实测 goodput），精度 ≤50 Mbps
- **Wirehair**：`--wh-segment-mib=2 --wh-window=8 --wh-repair-pct=10`（吞吐最优）
- **区分 ACK**：`wirehair_ack` / `wirehair_noack`
- **路径**：
  - `hop1_direct`：VM1→VM2
  - `hop2_kernel` / `hop3_kernel`：仅 IP 转发
  - `hop2_relay` / `hop3_relay`：应用层 `wire_relay`
- **payload**：200 MiB；扫速至 5000 Mbps
- **VM4 缓冲**：已与 VM1–3 对齐（`rmem/wmem_max=64M`, `backlog=5000`）；下表 hop3 用调优后结果

## 主表：上限 goodput (Mbps)

| path | copy | block | xor-fec | rs-fec | rs | wh_noack | wh_ack |
|---|---:|---:|---:|---:|---:|---:|---:|
| hop1_direct | 1350 | 1598 | 1100 | 711 | 908 | 1583 | 1398 |
| hop2_kernel | 1872 | 1250 | 833 | 750 | 1099 | 1766 | 1332 |
| hop2_relay | 750 | 850 | 550 | 700 | 450 | 1252 | 1055 |
| hop3_kernel | 1150 | 1289 | 992 | 881 | 882 | 1712 | 1069 |
| hop3_relay | 800 | 650 | 450 | 350 | 400 | 746 | 831 |

## 明细：ceiling (max_pass_rate 边界)

| path | copy | block | xor-fec | rs-fec | rs | wh_noack | wh_ack |
|---|---|---|---|---|---|---|---|
| hop1_direct | 1350 (pass≤1350) | 1598 (pass≤1600) | 1100 (pass≤1150) | 711 (pass≤1200) | 908 (pass≤1200) | 1583 (pass≤1800) | 1398 (≤5000+) |
| hop2_kernel | 1872 (pass≤2100) | 1250 (pass≤1250) | 833 (pass≤900) | 750 (pass≤750) | 1099 (pass≤1100) | 1766 (pass≤2750) | 1332 (≤5000+) |
| hop2_relay | 750 (pass≤750) | 850 (pass≤850) | 550 (pass≤550) | 700 (pass≤700) | 450 (pass≤450) | 1252 (pass≤1650) | 1055 (≤5000+) |
| hop3_kernel | 1150 (pass≤1150) | 1289 (pass≤1300) | 992 (pass≤1050) | 881 (pass≤950) | 882 (pass≤1200) | 1712 (pass≤1950) | 1069 (≤5000+) |
| hop3_relay | 800 (pass≤800) | 650 (pass≤650) | 450 (pass≤450) | 350 (pass≤350) | 400 (pass≤400) | 746 (pass≤850) | 831 (≤5000+) |

## hop3：VM4 缓冲调优前后

| path | mode | 调前 | 调后 | Δ |
|---|---|---:|---:|---:|
| hop3_kernel | copy | 500 | 1150 | +650 |
| hop3_kernel | block | 450 | 1289 | +839 |
| hop3_kernel | xor-fec | 550 | 992 | +442 |
| hop3_kernel | rs-fec | 300 | 881 | +581 |
| hop3_kernel | rs | 400 | 882 | +482 |
| hop3_kernel | wirehair_noack | 482 | 1712 | +1230 |
| hop3_kernel | wirehair_ack | 523 | 1069 | +546 |
| hop3_relay | copy | 450 | 800 | +350 |
| hop3_relay | block | 400 | 650 | +250 |
| hop3_relay | xor-fec | 500 | 450 | -50 |
| hop3_relay | rs-fec | 300 | 350 | +50 |
| hop3_relay | rs | 300 | 400 | +100 |
| hop3_relay | wirehair_noack | 1049 | 746 | -303 |
| hop3_relay | wirehair_ack | 640 | 831 | +190 |

## 结论要点

1. **1hop** 可到约 **1.1–1.6 Gbps**（copy/block/wirehair），接近旧 wirehair full matrix 量级。
2. **2hop kernel** 部分 codec（如 copy、wirehair_noack）仍可 >1.5 Gbps；**2hop app relay** 明显更低（多数 450–850，wirehair 约 0.7–1.3 Gbps）。
3. **3hop** 在 VM4 补齐 64M 缓冲后大幅回升（kernel copy 500→1150，wirehair_noack 482→1712）；**app relay 3hop** 仍受中继瓶颈限制。
4. **`wirehair_ack`** 常在设定 5000 仍 checksum 通过，上限看 goodput 平台而非 fail 边界。
5. 早期仅扫到 750 / segment=10·window=4 的矩阵**不能**代表上限，已废弃作定稿。

## 原始数据

- 全路径上限：`build/wire_ceiling_search.json`
- hop3 调优后：`build/wire_ceiling_search_hop3_retune.json`
- hop×relay×ack 档位扫描（非上限）：`build/wire_hop_relay_ack_matrix.json`
- 最初 e2e 无分 hop 矩阵：`build/wire-matrix-stage3-20260907-125206/`
