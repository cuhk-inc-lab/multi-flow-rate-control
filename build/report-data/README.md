# 实验报告目录

按**数据面拓扑**拆分，避免 VXLAN 与 linear 数字混读。

| 目录 | 拓扑 | 数据面 | 接口 |
|---|---|---|---|
| [`vxlan/`](vxlan/) | `WH_TOPO=vxlan` | `10.10.12 / 23 / 34` | `station*` / `ap*` |
| [`linear/`](linear/) | `WH_TOPO=linear` | `10.20.20 / 30.30 / 40.40` | `enp6s19/20/21` |

## 入口推荐

- VXLAN 总览：[`vxlan/vxlan_era_experiment_summary.md`](vxlan/vxlan_era_experiment_summary.md)
- Linear Phase3 四 codec 上限（含 goodput/link）：[`linear/linear_phase3_ceil_4codec.md`](linear/linear_phase3_ceil_4codec.md)
- Linear Phase4B stress（含图表）：[`linear/commercial_pool_20260908-173026_phase4b_stress.md`](linear/commercial_pool_20260908-173026_phase4b_stress.md)

## 归类说明

- **vxlan/**：阶段 1c/1d、phase3–5 定稿、iperf e2e（`10.10.34.2`）等，均在 VXLAN overlay 上测得。
- **linear/**：线性 virtio 桥上的 phase0–5 / commercial / pool 优化；`iperf_10_20_20_*` 为 virtio 直连（并含与 VXLAN 的对照段）。

管理面 SSH（`10.10.10.161–164`）两套拓扑相同，不作为分类依据。
