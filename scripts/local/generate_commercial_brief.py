#!/usr/bin/env python3
"""Generate commercial brief from Phase3–5 artifacts (goodput vs throughput)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def load_json(p: Path):
    if not p.exists():
        return None
    return json.loads(p.read_text())


def fmt(v, nd=1):
    if v is None:
        return "—"
    try:
        return str(round(float(v), nd))
    except (TypeError, ValueError):
        return str(v)


def summarize_grid(rows: list[dict]) -> list[dict]:
    return rows if rows and "ceiling_mbps" in rows[0] else []


def probe_metric_note() -> list[str]:
    return [
        "## 指标定义（必须分清）",
        "",
        "| 指标 | 含义 | 用途 |",
        "|---|---|---|",
        "| **goodput** | 有效源数据吞吐 (Mbps) | 用户感知传完多快 |",
        "| **wire throughput** | 应用编码后发出字节折算 (Mbps) | 协议/FEC 开销 |",
        "| **link_tx / link_rx** | 网卡计数吞吐 (Mbps) | 真实链路占用（≈speedometer） |",
        "",
        "> 商业结论一律以 **PASS（checksum 或完成率≥98%）下的 goodput** 为主指标；",
        "> wire/link 用于解释开销与瓶颈，**不可与 goodput 直接等同**。",
        "",
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", required=True)
    args = ap.parse_args()
    tag = args.tag
    repo = Path(__file__).resolve().parents[2]
    report = repo / "build" / "report-data" / "linear"
    report.mkdir(parents=True, exist_ok=True)

    k = load_json(repo / "build" / f"{tag}_p3_kernel.json") or []
    r = load_json(repo / "build" / f"{tag}_p3_relay.json") or []
    k_probes = load_json(repo / "build" / f"{tag}_p3_kernel_probes.json") or []
    r_probes = load_json(repo / "build" / f"{tag}_p3_relay_probes.json") or []
    ceil = load_json(repo / "build" / f"{tag}_ceil_for_p4.json") or {}
    p4 = load_json(repo / "build" / f"{tag}_phase4_multiflow_sat.json")
    # also accept default name
    if p4 is None:
        p4 = load_json(repo / "build" / "linear_phase4_multiflow_sat.json")

    lines: list[str] = []
    lines += [
        f"# 商业技术验证简报 — `{tag}`",
        "",
        "## 一句话结论",
        "",
        "- **内核转发（hop3_kernel）**：在无丢包线性链路上，copy/block 可打到 **多 Gbps 级 goodput**，适合峰值吞吐卖点。",
        "- **应用中继（hop3_relay）**：吞吐明显更低（约亚 Gbps～1Gbps 量级），但 **PFC/ACK（wirehair_ack）稳定可靠**；适合可控多跳与可靠性卖点。",
        "- **编码/FEC**：换的是抗丢包能力，不是峰值 goodput；无丢包时 goodput 通常低于 copy/block。",
        "",
    ]
    lines += probe_metric_note()

    lines += ["## Phase3 上限摘要（PASS 下最好 goodput）", ""]
    lines += [
        "| path | codec | ceiling goodput | max_pass_rate | pass/total |",
        "|---|---|---:|---:|---:|",
    ]
    for rows, path_label in ((k, "hop3_kernel"), (r, "hop3_relay")):
        for row in summarize_grid(rows):
            lines.append(
                f"| {path_label} | {row.get('codec_mode')} | {fmt(row.get('ceiling_mbps'))} | "
                f"{row.get('max_pass_rate_mbps')} | {row.get('n_pass_ge98')}/{row.get('n_probes')} |"
            )

    # sample probe with all three metrics
    lines += ["", "## 指标对照样例（同一次探测）", ""]
    lines += [
        "| path | codec | rate | verify | goodput | wire | link_tx | link_rx |",
        "|---|---|---:|---|---:|---:|---:|---:|",
    ]
    samples = []
    for p in k_probes + r_probes:
        if p.get("pass_ge98") and p.get("goodput_mbps") is not None:
            samples.append(p)
    samples = sorted(samples, key=lambda x: float(x.get("goodput_mbps") or 0), reverse=True)[:8]
    for p in samples:
        lines.append(
            f"| {p.get('path')} | {p.get('codec_mode')} | {p.get('rate')} | {p.get('verify')} | "
            f"{fmt(p.get('goodput_mbps'))} | {fmt(p.get('wire_mbps'))} | "
            f"{fmt(p.get('link_tx_mbps'))} | {fmt(p.get('link_rx_mbps'))} |"
        )

    lines += ["", "## 优点（对外可讲）", ""]
    lines += [
        "1. **端到端可验证**：checksum / 完成率门槛明确，不是只看仪表瞬时速率。",
        "2. **双路径能力**：kernel 路径打吞吐，relay 路径承载应用语义（FEC/ACK/多跳策略）。",
        "3. **PFC（wirehair_ack）**：在中继过载场景仍能稳住完成率，体现流量控制价值。",
        "4. **可观测性完整**：同时采集 goodput / app-wire / NIC link，避免 speedometer 误读。",
        "5. **工程优化可见**：relay datagram pool + 指针移交，降低 RX 深拷贝开销。",
        "",
        "## 不足与边界（必须坦诚）",
        "",
        "1. **relay 峰值远低于 kernel**：应用层中继是明确瓶颈，不能按 kernel 数字对外夸大。",
        "2. **无 ACK 的重编码（如 rs）在高设定速率下易 FAIL**：需要 pacing 或降速，否则完成率崩溃。",
        "3. **FEC 在无丢包时拉低 goodput**：卖点应是可靠性场景，不是裸吞吐冠军。",
        "4. **高速率下结果可能抖动**：偶发 PASS/FAIL 并存时，应用「稳定可通过」而不是单次峰值。",
        "5. **实验环境为 virtio 线性桥**：数字用于方案对比，迁移到真实 Wi‑Fi/广域网需复测。",
        "",
    ]

    lines += ["## Phase4 / Phase5 索引", ""]
    lines += [
        f"- Phase4A: `build/{tag}_phase4_multiflow_sat.json`（若存在）",
        f"- Phase4B: `build/report-data/linear/{tag}_phase4b_stress.md`",
        f"- Phase5: `build/report-data/linear/{tag}_phase5_resource_profile.md`",
        f"- Ceil overlay: `build/{tag}_ceil_for_p4.json` / `{ceil}`",
        "",
        "## 建议对外演示口径",
        "",
        "1. 先展示 **kernel copy/block 峰值 goodput**（性能上限）。",
        "2. 再展示 **relay + PFC 稳定传完**（可控与可靠）。",
        "3. 用同一张表并排 goodput vs link，解释「为什么 speedometer 更高/更低」。",
        "4. 最后用 Phase2/丢包或 stress 说明 FEC/ACK 的适用边界。",
        "",
    ]

    out = report / f"{tag}_commercial_brief.md"
    out.write_text("\n".join(lines) + "\n")
    print("wrote", out)


if __name__ == "__main__":
    main()
