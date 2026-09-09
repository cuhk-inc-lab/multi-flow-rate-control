# Phase3 单流上限 — `linear_phase3_ceil_retest_h2r_rs`

- PASS: checksum OK 或完成率 ≥ 98%
- payload: 200 MiB；精度 ≤50 Mbps；max_rate=10000
- Wirehair: seg=2 win=8
- **goodput** = PASS 探针中最大实测有效吞吐（源数据）
- **link_tx / link_rx** = 同一次 best-goodput 探针的 NIC 计数吞吐（Mbps）
- 主表单元格格式：`goodput / link`（单位 Mbps；link 默认取 link_tx）

## 上限 goodput / link (Mbps)

| path \ codec | rs |
|---|---:|
| hop2_relay | 1000 / 1590 |

## 上限 goodput only (Mbps)

| path \ codec | rs |
|---|---:|
| hop2_relay | 1000 (pass≤1000) |

## 同探针链路吞吐 link_tx (Mbps)

| path \ codec | rs |
|---|---:|
| hop2_relay | 1590 |

## 明细

| path | codec | max_pass_rate | goodput | wire | link_tx | link_rx | first_fail |
|---|---|---:|---:|---:|---:|---:|---:|
| hop2_relay | rs | 1000 | 1000.0 | 1547.1 | 1590.0 | 1590.0 | 1050 |

