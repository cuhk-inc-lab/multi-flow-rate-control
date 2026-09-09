# Linear topology Phase0–1

- path: VM1 10.20.20.1 — VM2 — VM3 — VM4 10.40.40.2
- SSH: 10.10.10.161–164

## Phase 0 — addressing

```
===== node .161 =====
fyp01
enp6s18          UP             10.10.10.161/24 
enp6s19          UP             10.20.20.1/24 
enp6s20          UP             
enp6s21          UP             
10.20.20.0/24 dev enp6s19 proto kernel scope link src 10.20.20.1 
10.30.30.0/24 via 10.20.20.2 dev enp6s19 
10.40.40.0/24 via 10.20.20.2 dev enp6s19 
===== node .162 =====
fyp02
enp6s18          UP             10.10.10.162/24 
enp6s19          UP             10.20.20.2/24 
enp6s20          UP             10.30.30.1/24 
enp6s21          UP             
10.20.20.0/24 dev enp6s19 proto kernel scope link src 10.20.20.2 
10.30.30.0/24 dev enp6s20 proto kernel scope link src 10.30.30.1 
10.40.40.0/24 via 10.30.30.2 dev enp6s20 
===== node .163 =====
fyp03
enp6s18          UP             10.10.10.163/24 
enp6s19          UP             
enp6s20          UP             10.30.30.2/24 
enp6s21          UP             10.40.40.1/24 
10.20.20.0/24 via 10.30.30.1 dev enp6s20 
10.30.30.0/24 dev enp6s20 proto kernel scope link src 10.30.30.2 
10.40.40.0/24 dev enp6s21 proto kernel scope link src 10.40.40.1 
===== node .164 =====
fyp04
enp6s18          UP             10.10.10.164/24 
enp6s19          UP             
enp6s20          UP             
enp6s21          UP             10.40.40.2/24 
10.20.20.0/24 via 10.40.40.1 dev enp6s21 
10.30.30.0/24 via 10.40.40.1 dev enp6s21 
10.40.40.0/24 dev enp6s21 proto kernel scope link src 10.40.40.2 
```

## Phase 0 — ping / traceroute

```
PING 10.20.20.2 (10.20.20.2) 56(84) bytes of data.
64 bytes from 10.20.20.2: icmp_seq=1 ttl=64 time=0.783 ms
64 bytes from 10.20.20.2: icmp_seq=2 ttl=64 time=0.662 ms
64 bytes from 10.20.20.2: icmp_seq=3 ttl=64 time=0.652 ms

--- 10.20.20.2 ping statistics ---
3 packets transmitted, 3 received, 0% packet loss, time 2041ms
rtt min/avg/max/mdev = 0.652/0.699/0.783/0.059 ms
PING 10.40.40.2 (10.40.40.2) 56(84) bytes of data.
64 bytes from 10.40.40.2: icmp_seq=1 ttl=62 time=1.45 ms
64 bytes from 10.40.40.2: icmp_seq=2 ttl=62 time=2.86 ms
64 bytes from 10.40.40.2: icmp_seq=3 ttl=62 time=1.50 ms

--- 10.40.40.2 ping statistics ---
3 packets transmitted, 3 received, 0% packet loss, time 2003ms
rtt min/avg/max/mdev = 1.447/1.936/2.860/0.653 ms
traceroute to 10.40.40.2 (10.40.40.2), 30 hops max, 60 byte packets
 1  10.20.20.2  0.180 ms
 2  10.30.30.2  0.764 ms
 3  10.40.40.2  1.720 ms
```

## Phase 1 — iperf3

| link | TCP Gbps | UDP@500M recv/loss | UDP@1G recv/loss | UDP@2G recv/loss |
|---|---:|---|---|---|
| hop1_VM1_VM2 | 18.93 | 499.1/0.15% | 956.5/4.31% | 1423.8/28.71% |
| hop2_VM2_VM3 | 24.396 | 499.7/0.05% | 961.6/3.81% | 1282.8/35.85% |
| hop3_VM3_VM4 | 13.889 | 499.5/0.06% | 929.8/6.99% | 1547.5/22.60% |
| e2e_VM1_VM4 | 8.971 | 499.5/0.01% | 904.6/9.49% | 1209.4/39.50% |
