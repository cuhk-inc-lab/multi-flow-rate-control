# Phase3 hop3_relay low-rate A/B (goodput + throughput)



| path/variant | codec | rate | verify | completion% | goodput | wire_thru | link_tx | link_rx | pass≥98 |
|---|---|---|---|---|---|---|---|---|---|
| default | copy | 400 | FAIL | 1.794 | 313.6 | 323.4 |  |  | False |
| default | copy | 600 | OK | 100.0 | 600.0 | 618.9 |  |  | True |
| default | copy | 800 | OK | 100.0 | 800.0 | 825.1 |  |  | True |
| default | copy | 1000 | FAIL | 47.908 | 1000.0 | 1031.4 |  |  | False |
| default | copy | 1200 | FAIL | 23.037 | 1200.0 | 1237.7 |  |  | False |
| default | copy | 1400 | FAIL | 10.561 | 1399.9 | 1444.0 |  |  | False |
| default | copy | 1600 | FAIL | 41.451 | 1599.9 | 1650.2 |  |  | False |
| default | copy | 1800 | FAIL | 25.587 | 1799.7 | 1856.3 |  |  | False |
| default | copy | 2000 | FAIL | 12.072 | 1911.6 | 1971.7 |  |  | False |
| default | block | 400 | OK | 100.0 | 400.0 | 412.6 |  |  | True |
| default | block | 600 | OK | 100.0 | 600.0 | 618.9 |  |  | True |
| default | block | 800 | OK | 100.0 | 800.0 | 825.1 |  |  | True |
| default | block | 1000 | FAIL | 90.397 | 1000.0 | 1031.4 |  |  | False |
| default | block | 1200 | FAIL | 35.507 | 1200.0 | 1237.7 |  |  | False |
| default | block | 1400 | OK | 100.0 | 1399.9 | 1444.0 |  |  | True |
| default | block | 1600 | FAIL | 25.718 | 1599.9 | 1650.2 |  |  | False |
| default | block | 1800 | FAIL | 9.616 | 1799.9 | 1856.5 |  |  | False |
| default | block | 2000 | FAIL | 7.645 | 1970.8 | 2032.8 |  |  | False |
| egress_tuned | copy | 400 | OK | 100.0 | 400.0 | 412.6 |  |  | True |
| egress_tuned | copy | 600 | OK | 100.0 | 600.0 | 618.9 |  |  | True |
| egress_tuned | copy | 800 | OK | 100.0 | 800.0 | 825.1 |  |  | True |
| egress_tuned | copy | 1000 | FAIL | 92.598 | 1000.0 | 1031.4 |  |  | False |
| egress_tuned | copy | 1200 | FAIL | 30.65 | 1199.9 | 1237.6 |  |  | False |
| egress_tuned | copy | 1400 | FAIL | 86.042 | 1399.9 | 1444.0 |  |  | False |
| egress_tuned | copy | 1600 | FAIL | 8.457 | 1600.0 | 1650.3 |  |  | False |
| egress_tuned | copy | 1800 | FAIL | 8.222 | 1799.9 | 1856.5 |  |  | False |
| egress_tuned | copy | 2000 | FAIL | 31.242 | 1985.6 | 2048.1 |  |  | False |
| egress_tuned | block | 400 | OK | 100.0 | 400.0 | 412.6 |  |  | True |
| egress_tuned | block | 600 | OK | 100.0 | 600.0 | 618.9 |  |  | True |
| egress_tuned | block | 800 | OK | 100.0 | 800.0 | 825.1 |  |  | True |
| egress_tuned | block | 1000 | OK | 100.0 | 1000.0 | 1031.4 |  |  | True |
| egress_tuned | block | 1200 | FAIL | 31.969 | 1200.0 | 1237.7 |  |  | False |
| egress_tuned | block | 1400 | FAIL | 30.804 | 1399.6 | 1443.7 |  |  | False |
| egress_tuned | block | 1600 | FAIL | 15.018 | 1599.6 | 1649.9 |  |  | False |
| egress_tuned | block | 1800 | OK | 100.0 | 1653.6 | 1705.6 |  |  | True |
| egress_tuned | block | 2000 | FAIL | 8.994 | 1999.8 | 2062.7 |  |  | False |

字段说明：
- **goodput**：有效源数据吞吐 (Mbps)
- **wire_thru**：应用层编码后发出字节折算 (Mbps)
- **link_tx/link_rx**：网卡计数吞吐 (Mbps)；旧数据可能为空，新探测会填

