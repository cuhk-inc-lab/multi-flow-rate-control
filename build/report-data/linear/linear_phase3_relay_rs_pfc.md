# Phase3 hop3_relay rs + PFC(wirehair_ack) (linear_phase3_relay_rs_pfc)

- rates: `[400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000]`
- codecs: `['rs', 'wirehair_ack']` (PFC = wirehair_ack)
- pass: checksum OK or completion ≥ 98%

## Summary

| variant | codec | max_pass_rate | ceiling goodput | first_fail | pass/total |
|---|---|---:|---:|---:|---:|
| default | rs | None | None | 400 | 0/9 |
| default | wirehair_ack | 2000 | 798.9 | None | 9/9 |
| egress_tuned | rs | 600 | 600.0 | 800 | 2/9 |
| egress_tuned | wirehair_ack | 2000 | 969.8 | None | 9/9 |

## Probes

| variant | codec | rate | verify | completion% | goodput | pass≥98 |
|---|---|---:|---|---:|---:|---|
| default | rs | 400 | FAIL | 23.95 | 294.4 | False |
| default | rs | 600 | FAIL | 21.707 | 599.99 | False |
| default | rs | 800 | FAIL | 6.465 | 799.97 | False |
| default | rs | 1000 | FAIL | 11.01 | 999.96 | False |
| default | rs | 1200 | FAIL | 9.194 | 1193.36 | False |
| default | rs | 1400 | FAIL | 5.458 | 1358.88 | False |
| default | rs | 1600 | FAIL | 6.724 | 1327.81 | False |
| default | rs | 1800 | FAIL | 3.765 | 1343.54 | False |
| default | rs | 2000 | FAIL | 8.836 | 1264.15 | False |
| default | wirehair_ack | 400 | OK | 100.0 | 384.7985321100917 | True |
| default | wirehair_ack | 600 | OK | 100.0 | 576.5366323024055 | True |
| default | wirehair_ack | 800 | OK | 100.0 | 630.7224060150376 | True |
| default | wirehair_ack | 1000 | OK | 100.0 | 630.7224060150376 | True |
| default | wirehair_ack | 1200 | OK | 100.0 | 642.8052107279694 | True |
| default | wirehair_ack | 1400 | OK | 100.0 | 637.916958174905 | True |
| default | wirehair_ack | 1600 | OK | 100.0 | 798.9150476190476 | True |
| default | wirehair_ack | 1800 | OK | 100.0 | 630.7224060150376 | True |
| default | wirehair_ack | 2000 | OK | 100.0 | 619.0854612546126 | True |
| egress_tuned | rs | 400 | OK | 100.0 | 399.99 | True |
| egress_tuned | rs | 600 | OK | 100.0 | 599.98 | True |
| egress_tuned | rs | 800 | FAIL | 8.729 | 799.97 | False |
| egress_tuned | rs | 1000 | FAIL | 7.423 | 999.95 | False |
| egress_tuned | rs | 1200 | FAIL | 10.011 | 1163.2 | False |
| egress_tuned | rs | 1400 | FAIL | 7.13 | 1399.95 | False |
| egress_tuned | rs | 1600 | FAIL | 7.498 | 1599.88 | False |
| egress_tuned | rs | 1800 | FAIL | 4.377 | 1777.66 | False |
| egress_tuned | rs | 2000 | FAIL | 5.431 | 1373.97 | False |
| egress_tuned | wirehair_ack | 400 | OK | 100.0 | 384.7985321100917 | True |
| egress_tuned | wirehair_ack | 600 | OK | 100.0 | 576.5366323024055 | True |
| egress_tuned | wirehair_ack | 800 | OK | 100.0 | 590.7470422535212 | True |
| egress_tuned | wirehair_ack | 1000 | OK | 100.0 | 633.1024905660378 | True |
| egress_tuned | wirehair_ack | 1200 | OK | 100.0 | 953.2509090909091 | True |
| egress_tuned | wirehair_ack | 1400 | OK | 100.0 | 906.8765405405404 | True |
| egress_tuned | wirehair_ack | 1600 | OK | 100.0 | 947.8653107344633 | True |
| egress_tuned | wirehair_ack | 1800 | OK | 100.0 | 969.7812716763007 | True |
| egress_tuned | wirehair_ack | 2000 | OK | 100.0 | 942.540224719101 | True |
