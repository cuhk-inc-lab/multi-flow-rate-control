# Phase3 单流上限 — `linear_phase3_ceil_retest_h3k_copy`

- PASS: checksum OK 或完成率 ≥ 98%
- payload: 200 MiB；精度 ≤50 Mbps；max_rate=10000
- Wirehair: seg=2 win=8
- **goodput** = PASS 探针中最大实测有效吞吐（源数据）
- **link_tx / link_rx** = 同一次 best-goodput 探针的 NIC 计数吞吐（Mbps）
- 主表单元格格式：`goodput / link`（单位 Mbps；link 默认取 link_tx）

## 上限 goodput / link (Mbps)

| path \ codec | copy |
|---|---:|
| hop3_kernel | 2700 / 2872 |

## 上限 goodput only (Mbps)

| path \ codec | copy |
|---|---:|
| hop3_kernel | 2700 (pass≤2700) |

## 同探针链路吞吐 link_tx (Mbps)

| path \ codec | copy |
|---|---:|
| hop3_kernel | 2872.3 |

## 明细

| path | codec | max_pass_rate | goodput | wire | link_tx | link_rx | first_fail |
|---|---|---:|---:|---:|---:|---:|---:|
| hop3_kernel | copy | 2700 | 2699.9 | 2784.8 | 2872.3 | 2872.3 | 2750 |

