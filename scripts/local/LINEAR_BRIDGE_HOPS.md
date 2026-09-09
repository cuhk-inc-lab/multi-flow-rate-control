# 线性 Bridge Hop 改造说明

目标（保留 SSH `10.10.10.16x`）：

```
VM1 --10.20.20-- VM2 --10.30.30-- VM3 --10.40.40-- VM4
.1               .2/.1            .2/.1            .2
enp6s19          enp6s19/20       enp6s20/21       enp6s21
```

VXLAN（`station*/ap*` / `10.10.12|23|34`）先保留，不删。

## 0. 先决：宿主机 L2 必须两两相通

| 链路 | 两端网卡 | 必须在同一 Proxmox bridge |
|---|---|---|
| hop1 | VM1 `enp6s19` ↔ VM2 `enp6s19` | 例如 `vmbr20` |
| hop2 | VM2 `enp6s20` ↔ VM3 `enp6s20` | 例如 `vmbr30` |
| hop3 | VM3 `enp6s21` ↔ VM4 `enp6s21` | 例如 `vmbr40` |

### 当前状态（改造前）

IP 呈 **星型**（VM1 同时在 20/30/40）：

- `10.20.20`: VM1↔VM2
- `10.30.30`: VM1↔VM3
- `10.40.40`: VM1↔VM4

若 Proxmox 上是「每台 VM 的 net1/2/3 都挂到同一组 vmbr」，则 **只改 guest IP/路由即可** 做成逻辑线性。  
若物理上只有 VM1 插在 30/40 上，则必须在 Proxmox 把 VM2/VM3/VM4 对应网卡接到对应 bridge。

### Proxmox 检查/改法（需你在宿主机操作）

1. 打开各 VM Hardware → Network Device  
2. 确认：
   - net1（多为 `enp6s19`）→ 同一 bridge（hop1）
   - net2（`enp6s20`）→ 同一 bridge（hop2）
   - net3（`enp6s21`）→ 同一 bridge（hop3）
3. **不要**改 net0 / `10.10.10.0/24`（SSH）

本助手环境：**无法登录 `10.10.10.1`（Proxmox）**，且 VM 上 `sudo` 需要密码，故 guest 配置需你在四台机执行。

## 1. Guest 一键脚本

仓库：`scripts/local/setup_linear_bridge_hops.sh`

在 **VM1–VM4** 上分别（有 sudo）：

```bash
cd ~/work/multi-flow-rate-control   # 或同步后的路径

# 先看计划
ROLE=vm1 APPLY=0 sudo -E bash scripts/local/setup_linear_bridge_hops.sh

# 应用
ROLE=vm1 sudo -E bash scripts/local/setup_linear_bridge_hops.sh
ROLE=vm2 sudo -E bash scripts/local/setup_linear_bridge_hops.sh
ROLE=vm3 sudo -E bash scripts/local/setup_linear_bridge_hops.sh
ROLE=vm4 sudo -E bash scripts/local/setup_linear_bridge_hops.sh
```

地址规划：

| 节点 | 接口 | 地址 |
|---|---|---|
| VM1 | enp6s19 | 10.20.20.1/24 |
| VM2 | enp6s19 | 10.20.20.2/24 |
| VM2 | enp6s20 | 10.30.30.1/24 |
| VM3 | enp6s20 | 10.30.30.2/24 |
| VM3 | enp6s21 | 10.40.40.1/24 |
| VM4 | enp6s21 | 10.40.40.2/24 |

中间 VM2/VM3 开 `ip_forward=1`。

## 2. 验证

```bash
# 逐跳
ping -c2 10.20.20.2   # on VM1
ping -c2 10.30.30.2   # on VM2
ping -c2 10.40.40.2   # on VM3

# 端到端（应经 VM2、VM3）
ping -c2 10.40.40.2   # on VM1
traceroute -n 10.40.40.2

# 容量（对照旧 VXLAN）
# VM4: iperf3 -s -B 10.40.40.2 -p 5211
# VM1: iperf3 -c 10.40.40.2 -p 5211 -t 10
```

期望：`traceroute` 为 `10.20.20.2 → 10.30.30.2 → 10.40.40.2`。

## 3. 持久化（可选）

脚本只改运行时配置；重启会丢。若要用 netplan/ifupdown，把同样地址和路由写进各发行版网络配置，并保留 `enp6s18` 的 `10.10.10.16x`。
