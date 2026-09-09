# Phase 5 — 运行时资源剖面（CPU / 内存 / NIC）

- 原始: `build/commercial_pool_20260908-173026_phase5_resource/`
- 采样: 进程 `proc_resource_monitor.py` + 主机 `iperf_like_monitor.py` @ 0.25 Hz
- wirehair: seg=2 win=8
- payload: 64 MiB/flow

## Part A — 单流（相对 phase3 上限 50%/100%）

| path | codec | rate | verify | goodput | send avg/peak CPU | send peak RSS (MB) | recv avg/peak CPU | recv peak RSS (MB) | relay2/3 peak CPU |
|---|---|---:|---|---:|---|---:|---|---:|---|
| kernel | copy | 600 | OK | 590.0 |  |  | 47.85/71.77 | 7.2 |  |
| kernel | copy | 1150 | OK | 1032.4 |  |  | 74.44/111.67 | 7.3 |  |
| kernel | rs | 450 | OK | 443.7 |  |  | 63.75/83.74 | 7.4 |  |
| kernel | rs | 900 | OK | 624.3 |  |  | 67.8/95.69 | 7.5 |  |
| kernel | wirehair_noack | 850 | OK | 745.7 |  |  | 44.89/63.83 | 11.7 |  |
| kernel | wirehair_noack | 1700 | OK | 1491.3 |  |  | 71.8/119.62 | 11.7 |  |
| kernel | wirehair_ack | 550 | OK | 526.3 |  |  | 39.88/51.85 | 12.4 |  |
| kernel | wirehair_ack | 1050 | OK | 1013.0 |  |  | 50.48/71.72 | 11.7 |  |
| relay | copy | 400 | OK | 397.7 |  |  | 16.28/43.84 | 7.3 | 111.82/95.77 |
| relay | copy | 800 | OK | 679.6 |  |  | 19.14/75.82 | 7.3 | 167.69/163.59 |
| relay | rs | 200 | OK | 199.6 |  |  | 17.59/39.89 | 7.4 | 119.56/47.91 |
| relay | rs | 400 | OK | 394.8 |  |  | 21.59/59.81 | 7.4 | 143.69/151.53 |
| relay | wirehair_noack | 350 | OK | 306.8 |  |  | 15.95/35.9 | 13.6 | 79.86/47.9 |
| relay | wirehair_noack | 750 | OK | 654.7 |  |  | 19.13/67.79 | 11.7 | 143.63/159.65 |
| relay | wirehair_ack | 400 | OK | 383.5 |  |  | 18.1/51.86 | 11.7 | 83.84/95.63 |
| relay | wirehair_ack | 850 | OK | 679.6 |  |  | 21.14/75.8 | 13.7 | 139.72/143.52 |

## Part B — 多流扩展（2/4/8，码率=4A PASS 点）

| path | codec | flows | rate/flow | verify | send peak CPU/RSS(MB) | recv peak CPU/RSS(MB) | relay2 peak CPU/RSS |
|---|---|---:|---:|---|---|---|---|
| kernel | copy | 2 | 700 | OK | None/ | 127.66/10.5 | None/ |
| kernel | copy | 4 | 150 | OK | None/ | 135.54/17.7 | None/ |
| kernel | copy | 8 | 50 | OK | None/ | 119.61/31.6 | None/ |
| kernel | rs | 2 | 550 | OK | None/ | 195.39/11.0 | None/ |
| kernel | rs | 4 | 150 | OK | None/ | 163.53/17.8 | None/ |
| kernel | rs | 8 | 50 | OK | None/ | 135.63/31.7 | None/ |
| kernel | wirehair_noack | 2 | 1050 | OK | None/ | 99.68/19.4 | None/ |
| kernel | wirehair_noack | 4 | 550 | OK | None/ | 215.32/34.6 | None/ |
| kernel | wirehair_noack | 8 | 250 | OK | None/ | 319.31/68.2 | None/ |
| kernel | wirehair_ack | 2 | 650 | OK | None/ | 119.67/19.4 | None/ |
| kernel | wirehair_ack | 4 | 350 | OK | None/ | 163.6/34.6 | None/ |
| kernel | wirehair_ack | 8 | 150 | OK | None/ | 207.49/75.4 | None/ |
| relay | copy | 2 | 500 | FAIL | None/ | 115.71/10.2 | 231.46/38.2 |
| relay | copy | 4 | 150 | OK | None/ | 111.71/17.7 | 211.58/11.8 |
| relay | copy | 8 | 50 | OK | None/ | 91.79/31.6 | 135.59/5.5 |
| relay | rs | 2 | 200 | OK | None/ | 111.68/10.9 | 211.25/18.5 |
| relay | rs | 4 | 50 | OK | None/ | 63.86/17.9 | 87.84/5.4 |
| relay | wirehair_noack | 2 | 400 | OK | None/ | 91.72/23.4 | 203.27/6.1 |
| relay | wirehair_noack | 4 | 150 | OK | None/ | 103.49/34.6 | 171.63/6.1 |
| relay | wirehair_noack | 8 | 50 | OK | None/ | 127.66/65.1 | 135.75/5.5 |
| relay | wirehair_ack | 2 | 500 | OK | None/ | 131.59/19.9 | 155.58/6.6 |
| relay | wirehair_ack | 4 | 250 | OK | None/ | 163.53/42.1 | 159.68/6.5 |
| relay | wirehair_ack | 8 | 100 | OK | None/ | 219.24/65.0 | 215.62/13.7 |

## Part C — 4B 应用向抽检（batch1 copy mixed）

- status: **PASS** (6/6 PASS)
- host peak CPU: n1=28.15 n2=30.37 n3=15.67 n4=11.3
- n4 NIC peak Mbps: 2550.1
- artifacts: `/home/scy/work/multi-flow-rate-control/build/commercial_pool_20260908-173026_phase5_resource/part_c_stress`

## 读数说明

- **进程 CPU%**：相对单核（可 >100% 若多核）；avg/peak 来自传输窗口采样。
- **RSS**：进程常驻内存峰值；wirehair 随 window×segment×flows 上升。
- **主机 CPU**：整机利用率；含内核转发开销。

