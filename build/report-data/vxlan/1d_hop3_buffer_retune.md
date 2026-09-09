# hop3 上限：VM4 缓冲调优前后对比

- 调优: VM4 `rmem_max/wmem_max=64M`, `netdev_max_backlog=5000`
- 调前: `build/wire_ceiling_search.json`；调后: `build/wire_ceiling_search_hop3_retune.json`

| path | mode | 调前 ceil | 调后 ceil | Δ | max_pass_rate 前→后 |
|---|---|---:|---:|---:|---|
| hop3_kernel | copy | 500 | 1150 | +650 | 500→1150 |
| hop3_kernel | block | 450 | 1289 | +839 | 450→1300 |
| hop3_kernel | xor-fec | 550 | 992 | +442 | 550→1050 |
| hop3_kernel | rs-fec | 300 | 881 | +581 | 300→950 |
| hop3_kernel | rs | 400 | 882 | +482 | 400→1200 |
| hop3_kernel | wirehair_noack | 482 | 1712 | +1230 | 550→1950 |
| hop3_kernel | wirehair_ack | 523 | 1069 | +546 | 5000→5000 |
| hop3_relay | copy | 450 | 800 | +350 | 450→800 |
| hop3_relay | block | 400 | 650 | +250 | 400→650 |
| hop3_relay | xor-fec | 500 | 450 | -50 | 500→450 |
| hop3_relay | rs-fec | 300 | 350 | +50 | 300→350 |
| hop3_relay | rs | 300 | 400 | +100 | 300→400 |
| hop3_relay | wirehair_noack | 1049 | 746 | -303 | 1200→850 |
| hop3_relay | wirehair_ack | 640 | 831 | +190 | 5000→5000 |
