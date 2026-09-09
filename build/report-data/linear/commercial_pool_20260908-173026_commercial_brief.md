# 商业技术验证简报 — `commercial_pool_20260908-173026`

## 一句话结论

- **内核转发（hop3_kernel）**：在无丢包线性链路上，copy/block 可打到 **多 Gbps 级 goodput**，适合峰值吞吐卖点。
- **应用中继（hop3_relay）**：吞吐明显更低（约亚 Gbps～1Gbps 量级），但 **PFC/ACK（wirehair_ack）稳定可靠**；适合可控多跳与可靠性卖点。
- **编码/FEC**：换的是抗丢包能力，不是峰值 goodput；无丢包时 goodput 通常低于 copy/block。

## 指标定义（必须分清）

| 指标 | 含义 | 用途 |
|---|---|---|
| **goodput** | 有效源数据吞吐 (Mbps) | 用户感知传完多快 |
| **wire throughput** | 应用编码后发出字节折算 (Mbps) | 协议/FEC 开销 |
| **link_tx / link_rx** | 网卡计数吞吐 (Mbps) | 真实链路占用（≈speedometer） |

> 商业结论一律以 **PASS（checksum 或完成率≥98%）下的 goodput** 为主指标；
> wire/link 用于解释开销与瓶颈，**不可与 goodput 直接等同**。

## Phase3 上限摘要（PASS 下最好 goodput）

| path | codec | ceiling goodput | max_pass_rate | pass/total |
|---|---|---:|---:|---:|

## 指标对照样例（同一次探测）

| path | codec | rate | verify | goodput | wire | link_tx | link_rx |
|---|---|---:|---|---:|---:|---:|---:|

## 优点（对外可讲）

1. **端到端可验证**：checksum / 完成率门槛明确，不是只看仪表瞬时速率。
2. **双路径能力**：kernel 路径打吞吐，relay 路径承载应用语义（FEC/ACK/多跳策略）。
3. **PFC（wirehair_ack）**：在中继过载场景仍能稳住完成率，体现流量控制价值。
4. **可观测性完整**：同时采集 goodput / app-wire / NIC link，避免 speedometer 误读。
5. **工程优化可见**：relay datagram pool + 指针移交，降低 RX 深拷贝开销。

## 不足与边界（必须坦诚）

1. **relay 峰值远低于 kernel**：应用层中继是明确瓶颈，不能按 kernel 数字对外夸大。
2. **无 ACK 的重编码（如 rs）在高设定速率下易 FAIL**：需要 pacing 或降速，否则完成率崩溃。
3. **FEC 在无丢包时拉低 goodput**：卖点应是可靠性场景，不是裸吞吐冠军。
4. **高速率下结果可能抖动**：偶发 PASS/FAIL 并存时，应用「稳定可通过」而不是单次峰值。
5. **实验环境为 virtio 线性桥**：数字用于方案对比，迁移到真实 Wi‑Fi/广域网需复测。

## Phase4 / Phase5 索引

- Phase4A: `build/commercial_pool_20260908-173026_phase4_multiflow_sat.json`（若存在）
- Phase4B: `build/report-data/linear/commercial_pool_20260908-173026_phase4b_stress.md`
- Phase5: `build/report-data/linear/commercial_pool_20260908-173026_phase5_resource_profile.md`
- Ceil overlay: `build/commercial_pool_20260908-173026_ceil_for_p4.json` / `{'kernel': {}, 'relay': {}}`

## 建议对外演示口径

1. 先展示 **kernel copy/block 峰值 goodput**（性能上限）。
2. 再展示 **relay + PFC 稳定传完**（可控与可靠）。
3. 用同一张表并排 goodput vs link，解释「为什么 speedometer 更高/更低」。
4. 最后用 Phase2/丢包或 stress 说明 FEC/ACK 的适用边界。

