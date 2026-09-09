# 阶段3（重测）hop × kernel/relay × codec × wirehair ACK

- 原始: `build/wire_hop_relay_ack_matrix.json` / `.csv` / `_run.log`
- 64 MiB；rates 200/400/500/600/750；wirehair segment=10 window=4 repair=10
- `*_kernel`=IP 转发；`*_relay`=应用层 wire_relay；1hop 仅 direct

## 各配置最大 PASS target Mbps（及 goodput）

| path \ mode | copy | block | xor-fec | rs-fec | rs | wirehair_noack | wirehair_ack |
|---|---|---|---|---|---|---|---|
| hop1_direct | 750 (750) | 750 (750) | 750 (750) | 750 (708) | 750 (750) | 750 (655) | 750 (716) |
| hop2_kernel | 750 (750) | 750 (750) | 750 (750) | 750 (750) | 600 (600) | 750 (655) | 750 (716) |
| hop2_relay | 750 (750) | 750 (750) | 600 (600) | 750 (749) | 600 (600) | 750 (655) | 750 (716) |
| hop3_kernel | 750 (750) | 600 (600) | 600 (600) | 400 (400) | 500 (500) | 600 (526) | 750 (314) |
| hop3_relay | 600 (600) | 600 (600) | 400 (400) | 500 (500) | 400 (400) | 600 (526) | 750 (471) |

## FAIL 点（rate）

- hop2_kernel / rs: [750]
- hop2_relay / rs: [750]
- hop2_relay / xor-fec: [750]
- hop3_kernel / block: [750]
- hop3_kernel / rs: [400, 600, 750]
- hop3_kernel / rs-fec: [500, 600, 750]
- hop3_kernel / wirehair_noack: [750]
- hop3_kernel / xor-fec: [750]
- hop3_relay / block: [750]
- hop3_relay / copy: [500, 750]
- hop3_relay / rs: [500, 600, 750]
- hop3_relay / rs-fec: [600, 750]
- hop3_relay / wirehair_noack: [750]
- hop3_relay / xor-fec: [500, 600, 750]
