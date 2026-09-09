# Phase3 单流上限 — `linear_phase3_ceil_4codec`

- PASS: checksum OK 或完成率 ≥ 98%
- payload: 200 MiB；精度 ≤50 Mbps；max_rate=10000
- Wirehair: seg=2 win=8
- **goodput** = PASS 探针中最大实测有效吞吐（源数据）
- **link_tx / link_rx** = 同一次 best-goodput 探针的 NIC 计数吞吐（Mbps）
- 主表单元格格式：`goodput / link`（单位 Mbps；link 默认取 link_tx）

## 上限 goodput / link (Mbps)

| path \ codec | copy | rs | wirehair_noack | wirehair_ack |
|---|---:|---:|---:|---:|
| hop1_direct | 1550 / 1649 | 1728 / 2754 | 1974 / 2321 | 1539 / 1645 |
| hop2_kernel | 2726 / 2919 | 1450 / 2303 | 2071 / 2436 | 1498 / 1601 |
| hop2_relay | 1050 / 1113 | 1000 / 1590 | 1141 / 1342 | 1190 / 1272 |
| hop3_kernel | 2700 / 2872 | 1877 / 3001 | 2179 / 2563 | 1332 / 1423 |
| hop3_relay | 1200 / 1272 | 1000 / 1590 | 1353 / 1591 | 822 / 879 |

## 上限 goodput only (Mbps)

| path \ codec | copy | rs | wirehair_noack | wirehair_ack |
|---|---:|---:|---:|---:|
| hop1_direct | 1550 (pass≤1550) | 1728 (pass≤4250) | 1974 (pass≤2250) | 1539 (pass≤10000) |
| hop2_kernel | 2726 (pass≤2750) | 1450 (pass≤1450) | 2071 (pass≤4750) | 1498 (pass≤10000) |
| hop2_relay | 1050 (pass≤1050) | 1000 (pass≤1000) | 1141 (pass≤1300) | 1190 (pass≤10000) |
| hop3_kernel | 2700 (pass≤2700) | 1877 (pass≤4950) | 2179 (pass≤10000) | 1332 (pass≤10000) |
| hop3_relay | 1200 (pass≤1200) | 1000 (pass≤1000) | 1353 (pass≤2000) | 822 (pass≤10000) |

## 同探针链路吞吐 link_tx (Mbps)

| path \ codec | copy | rs | wirehair_noack | wirehair_ack |
|---|---:|---:|---:|---:|
| hop1_direct | 1648.9 | 2753.8 | 2321.4 | 1644.8 |
| hop2_kernel | 2919.4 | 2302.8 | 2436.1 | 1600.8 |
| hop2_relay | 1113 | 1590 | 1342.3 | 1271.5 |
| hop3_kernel | 2872.3 | 3001.4 | 2562.6 | 1422.9 |
| hop3_relay | 1272 | 1590 | 1591.3 | 878.8 |

## 明细

| path | codec | max_pass_rate | goodput | wire | link_tx | link_rx | first_fail |
|---|---|---:|---:|---:|---:|---:|---:|
| hop1_direct | copy | 1550 | 1549.9 | 1598.7 | 1648.9 | 1648.9 | 1600 |
| hop1_direct | rs | 4250 | 1728.3 | 2323.2 | 2753.8 | 2753.8 | 4300 |
| hop1_direct | wirehair_noack | 2250 | 1973.8 | 2254.8 | 2321.4 | 2321.4 | 2300 |
| hop1_direct | wirehair_ack | 10000 | 1539.2 | 1527.6 | 1644.8 | 1644.8 | — |
| hop2_kernel | copy | 2750 | 2726.4 | 2812.2 | 2919.4 | 2919.4 | 2800 |
| hop2_kernel | rs | 1450 | 1450.0 | 2243.4 | 2302.8 | 2302.8 | 1500 |
| hop2_kernel | wirehair_noack | 4750 | 2071.3 | 2129.6 | 2436.1 | 2436.1 | 4800 |
| hop2_kernel | wirehair_ack | 10000 | 1498.0 | 1514.3 | 1600.8 | 1600.8 | — |
| hop2_relay | copy | 1050 | 1050.0 | 1083.0 | 1113.0 | 1113.0 | 1100 |
| hop2_relay | rs | 1000 | 1000.0 | 1547.1 | 1590.0 | 1590.0 | 1050 |
| hop2_relay | wirehair_noack | 1300 | 1141.3 | 1303.8 | 1342.3 | 1342.3 | 1350 |
| hop2_relay | wirehair_ack | 10000 | 1189.9 | 1109.2 | 1271.5 | 1271.5 | — |
| hop3_kernel | copy | 2700 | 2699.9 | 2784.8 | 2872.3 | 2872.3 | 2750 |
| hop3_kernel | rs | 4950 | 1877.0 | 2368.3 | 3001.4 | 3001.4 | 5000 |
| hop3_kernel | wirehair_noack | 10000 | 2178.9 | 2366.2 | 2562.6 | 2562.6 | — |
| hop3_kernel | wirehair_ack | 10000 | 1331.5 | 1280.4 | 1422.9 | 1422.9 | — |
| hop3_relay | copy | 1200 | 1199.9 | 1237.6 | 1272.0 | 1272.0 | 1250 |
| hop3_relay | rs | 1000 | 999.9 | 1547.1 | 1590.0 | 1590.0 | 1050 |
| hop3_relay | wirehair_noack | 2000 | 1353.0 | 1485.7 | 1591.3 | 1591.3 | 2050 |
| hop3_relay | wirehair_ack | 10000 | 822.4 | 725.6 | 878.8 | 878.8 | — |

