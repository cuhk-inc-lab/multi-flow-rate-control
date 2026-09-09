# Phase3 hop3_relay low-rate A/B (linear_phase3_relay_lowrate_after_pool)

- file: `200` MiB
- rates: `[400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000]`
- codecs: `['copy', 'block']`
- pass: checksum OK or completion ≥ 98%

## Summary

| variant | codec | max_pass_rate | ceiling goodput | first_fail | pass/total | relay_extra |
|---|---|---:|---:|---:|---:|---|
| default | block | 1000 | 1000.0 | 1200 | 4/9 | `` |
| default | copy | 2000 | 1616.5 | 400 | 4/9 | `` |
| egress_tuned | block | 1200 | 1200.0 | 1400 | 5/9 | `--egress-wait-ms 5 --egress-capacity 65536` |
| egress_tuned | copy | 800 | 800.0 | 1000 | 3/9 | `--egress-wait-ms 5 --egress-capacity 65536` |

## Probes

| variant | codec | rate | verify | completion% | goodput | wire_thru | link_tx | link_rx | pass≥98 |
|---|---|---:|---|---:|---:|---:|---:|---:|---|
| default | copy | 400 | FAIL | 2.029 | 301.5 | 311.0 | 318.7 | 280.5 | False |
| default | copy | 600 | OK | 100.0 | 599.9 | 618.8 | 636.0 | 636.0 | True |
| default | copy | 800 | OK | 100.0 | 800.0 | 825.1 | 852.1 | 852.1 | True |
| default | copy | 1000 | OK | 100.0 | 999.9 | 1031.4 | 1060.0 | 1060.0 | True |
| default | copy | 1200 | FAIL | 17.445 | 1199.9 | 1237.7 | 1272.0 | 1245.4 | False |
| default | copy | 1400 | FAIL | 42.076 | 1399.9 | 1443.9 | 1484.0 | 1293.0 | False |
| default | copy | 1600 | FAIL | 13.325 | 1599.7 | 1650.0 | 1696.0 | 1143.4 | False |
| default | copy | 1800 | FAIL | 9.399 | 1636.5 | 1688.0 | 1745.9 | 1108.4 | False |
| default | copy | 2000 | OK | 100.0 | 1616.5 | 1667.4 | 1712.3 | 1712.3 | True |
| default | block | 400 | OK | 100.0 | 400.0 | 412.6 | 425.0 | 425.0 | True |
| default | block | 600 | OK | 100.0 | 600.0 | 618.9 | 638.3 | 638.3 | True |
| default | block | 800 | OK | 100.0 | 800.0 | 825.1 | 848.0 | 848.0 | True |
| default | block | 1000 | OK | 100.0 | 1000.0 | 1031.4 | 1060.0 | 1060.0 | True |
| default | block | 1200 | FAIL | 40.434 | 1199.9 | 1237.7 | 1272.0 | 1166.5 | False |
| default | block | 1400 | FAIL | 14.585 | 1400.0 | 1444.0 | 1484.0 | 1292.2 | False |
| default | block | 1600 | FAIL | 26.105 | 1576.5 | 1626.1 | 1680.0 | 1390.9 | False |
| default | block | 1800 | FAIL | 25.138 | 1637.4 | 1689.0 | 1745.9 | 1360.1 | False |
| default | block | 2000 | FAIL | 30.14 | 1760.4 | 1815.8 | 1874.6 | 1505.8 | False |
| egress_tuned | copy | 400 | OK | 100.0 | 400.0 | 412.6 | 425.0 | 425.0 | True |
| egress_tuned | copy | 600 | OK | 100.0 | 600.0 | 618.9 | 636.0 | 636.0 | True |
| egress_tuned | copy | 800 | OK | 100.0 | 800.0 | 825.1 | 852.1 | 852.1 | True |
| egress_tuned | copy | 1000 | FAIL | 87.708 | 1000.0 | 1031.4 | 1060.0 | 1057.0 | False |
| egress_tuned | copy | 1200 | FAIL | 8.654 | 1200.0 | 1237.7 | 1272.0 | 1100.2 | False |
| egress_tuned | copy | 1400 | FAIL | 17.552 | 1400.0 | 1444.0 | 1484.0 | 1357.8 | False |
| egress_tuned | copy | 1600 | FAIL | 9.835 | 1599.4 | 1649.7 | 1696.0 | 1491.4 | False |
| egress_tuned | copy | 1800 | FAIL | 13.789 | 1745.3 | 1800.2 | 1855.0 | 1405.5 | False |
| egress_tuned | copy | 2000 | FAIL | 7.127 | 1685.4 | 1738.4 | 1798.8 | 1168.0 | False |
| egress_tuned | block | 400 | OK | 100.0 | 400.0 | 412.6 | 425.0 | 425.0 | True |
| egress_tuned | block | 600 | OK | 100.0 | 600.0 | 618.9 | 638.3 | 638.3 | True |
| egress_tuned | block | 800 | OK | 100.0 | 800.0 | 825.1 | 848.0 | 848.0 | True |
| egress_tuned | block | 1000 | OK | 100.0 | 999.9 | 1031.4 | 1060.0 | 1060.0 | True |
| egress_tuned | block | 1200 | OK | 100.0 | 1200.0 | 1237.7 | 1272.0 | 1272.0 | True |
| egress_tuned | block | 1400 | FAIL | 32.409 | 1399.9 | 1444.0 | 1484.0 | 1365.5 | False |
| egress_tuned | block | 1600 | FAIL | 8.937 | 1599.9 | 1650.2 | 1696.0 | 1342.7 | False |
| egress_tuned | block | 1800 | FAIL | 24.137 | 1651.5 | 1703.4 | 1763.2 | 1497.5 | False |
| egress_tuned | block | 2000 | FAIL | 6.139 | 1999.9 | 2062.8 | 2120.0 | 1372.4 | False |
