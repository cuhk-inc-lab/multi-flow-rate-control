# 吞吐上限搜索（精度 ≤50 Mbps）

- Wirehair: `--wh-segment-mib=2 --wh-window=8`；payload 200 MiB
- `ceiling` = 该配置所有 PASS 探针中的 **最大实测 goodput**
- 括号: `pass≤R` = checksum 仍过的最大设定速率（±50）；`≤R+` = 到 5000 仍全过
- 原始: `build/wire_ceiling_search.json` / `_probes.json` / `_run.log`

## 上限 goodput (Mbps)

| path \ mode | copy | block | xor-fec | rs-fec | rs | wirehair_noack | wirehair_ack |
|---|---|---|---|---|---|---|---|
| hop1_direct | 1350 (pass≤1350) | 1598 (pass≤1600) | 1100 (pass≤1150) | 711 (pass≤1200) | 908 (pass≤1200) | 1583 (pass≤1800) | 1398 (≤5000+) |
| hop2_kernel | 1872 (pass≤2100) | 1250 (pass≤1250) | 833 (pass≤900) | 750 (pass≤750) | 1099 (pass≤1100) | 1766 (pass≤2750) | 1332 (≤5000+) |
| hop2_relay | 750 (pass≤750) | 850 (pass≤850) | 550 (pass≤550) | 700 (pass≤700) | 450 (pass≤450) | 1252 (pass≤1650) | 1055 (≤5000+) |
| hop3_kernel | 500 (pass≤500) | 450 (pass≤450) | 550 (pass≤550) | 300 (pass≤300) | 400 (pass≤400) | 482 (pass≤550) | 523 (≤5000+) |
| hop3_relay | 450 (pass≤450) | 400 (pass≤400) | 500 (pass≤500) | 300 (pass≤300) | 300 (pass≤300) | 1049 (pass≤1200) | 640 (≤5000+) |
