# Cross-VM Network Benchmark

Use two VMs for the first test: VM1 sends and VM2 receives. The remaining VMs
are useful for repeat runs or later multi-hop and multi-flow tests.

## 1. Establish the network baseline

On VM2, start one persistent `iperf3` server per port in separate terminals:

```bash
iperf3 -s -p 5201   # TCP test
iperf3 -s -p 5202   # UDP test
```

On VM1, replace `VM2_IP` with VM2's reachable private IP:

```bash
sh scripts/run_vm_baseline.sh VM2_IP 10
```

The script runs TCP once and sweeps UDP offered rates. Set `UDP_RATES` to
override its default rate list. Record the highest offered
rate with acceptable loss, plus receiver bitrate, jitter, packet loss, VM
instance types, MTU, and the `traceroute` hop count.

## 2. Build on both VMs

Both VMs need this repository and `buffer-management-module` as sibling
directories:

```bash
cd ~/work/multi-flow-rate-control
make wg-demo
```

Allow the chosen UDP port in both VM firewalls/security groups.

## 3. Run the application transfer

The cross-VM wire modes use a UDP header containing flow ID, block ID, shard
index/count, payload length, and original valid length. They support the
following codecs:

- `copy`: preserves the existing 4-input-packet to 8-output-packet block
  geometry without `+1/-1` arithmetic.
- `block`: the existing `+1/-1` BlockCodec.
- `xor-fec`: four data shards plus one XOR parity shard. The receiver restores
  one missing shard; `--best-effort` writes available data shards from groups
  that still cannot be recovered after the idle timeout.
- `rs-fec`: RS(4,2), with four data shards and two parity shards. The receiver
  restores up to two missing shards when at least four shards arrive; its
  systematic `--best-effort` behavior is the same as `xor-fec`.

On VM2:

```bash
./build/wg_multi_pipeline --codec copy \
  --udp-recv 9000 received-copy.ts --idle-sec 5
```

On VM1:

```bash
./build/wg_multi_pipeline --codec copy --rate-mbps 100 \
  --udp-send VM2_IP 9000 input.ts
```

After the receiver exits, compare SHA-256 hashes or copy the output back to
VM1 and use `cmp`:

```bash
sha256sum input.ts
ssh VM2 'sha256sum ~/work/multi-flow-rate-control/received-copy.ts'
```

Repeat with `--codec block` and a separate output file/port. Increase
`--rate-mbps` until output validation fails or receiver counters report
missing/dropped groups. Repeat the highest passing rate several times before
reporting it as reliable.

### Multi-flow across VMs (single sender/receiver process)

To run multiple concurrent streams without starting multiple app processes,
use the wire multi-flow mode:

- Sender (VM1): `--udp-send-multi` with repeated `--flow` specs
- Receiver (VM2): `--udp-recv <port> <out_prefix> --max-flows N`

Each `--flow` spec format is:

`<flow_id>:<receiver_ip>:<port>:<input_path>[:rate_mbps]`

`flow_id` must match the slot index you expect on the receiver side (the
receiver output includes `flow_id`). The receiver writes one output file per
flow keyed by the sender peer (IP+port) and `flow_id`, for example:

`{out_prefix}src_<sender_ip>_p<sender_port>_flow_<flow_id>.ts`

Example: 2 flows (VM1 -> VM2) with `copy` codec.

On VM2 (receiver):

```bash
./build/wg_multi_pipeline --codec copy \
  --udp-recv 9000 /tmp/out_multi_ --idle-sec 5 --max-flows 2
```

On VM1 (sender):

```bash
./build/wg_multi_pipeline --codec copy --udp-send-multi \
  --flow "0:VM2_IP:9000:input0.ts:32" \
  --flow "1:VM2_IP:9000:input1.ts:32"
```

After the receiver exits, locate and validate outputs (wildcard is used
because the sender source port may vary across flows):

```bash
cmp input0.ts /tmp/out_multi_src_*_flow_0.ts
cmp input1.ts /tmp/out_multi_src_*_flow_1.ts
```

### Automated codec/rate matrix

Run this script on VM1 after VM1 can use key-based SSH to VM4. It starts a
fresh receiver on VM4 for every codec/rate pair, verifies the output hash, and
writes CSV and Markdown tables containing throughput, delay, and jitter
statistics under `build/wire-matrix-*`.

```bash
CODECS="copy block xor-fec rs-fec" RATES="20 24 28 32" \
  ./scripts/run_wire_matrix.sh fyp1@VM4_MANAGEMENT_IP VM4_DATA_IP input.ts
```

Both endpoints must run the same wire protocol version. Synchronize VM1 and
VM4 clocks before interpreting the cross-host transfer or end-to-end delay.

### Concurrent iperf-like multi-destination run

For the supervisor-style 6-stream concurrent test (Node1/Node2 senders,
Node2/Node3/Node4 receivers, plus relay iface monitoring), use:

```bash
NODE2_SSH=fyp1@NODE2_MGMT NODE2_IP=NODE2_DATA_IP \
NODE3_SSH=fyp1@NODE3_MGMT NODE3_IP=NODE3_DATA_IP \
NODE4_SSH=fyp1@NODE4_MGMT NODE4_IP=NODE4_DATA_IP \
  ./scripts/run_iperf_like_wire.sh input.ts
```

Relay iface defaults match the lab topology (`NODE2_IFACES="ap0 station1"`,
`NODE3_IFACES="ap1 station2"`). Override only if your names differ.

The script:

1. builds sized payloads for each stream (`rate * duration`) on Node1
   (including Node2's s2/s4), then scp's s2/s4 onto Node2 for sending
2. starts receivers on Node2/3/4 (`--codec` default `copy` for integrity;
   use `CODEC=xor-fec` for FEC stress)
3. starts relay monitors on Node2/3
4. starts Node1 + Node2 senders at a shared `START_AT` barrier
   (default rates: 1 Mbps each, ~6 Mbps aggregate; Node2 loopback on PORT+1)
5. collects logs and writes `streams.csv` + `summary.md` under
   `build/iperf-like-wire-*`

Receiver flow mapping uses UDP 5-tuple allocation. Pass/fail is decided by
payload hash match against fetched outputs.

## 4. Report

Keep TCP and loss-free UDP `iperf3` results separate from application results.
For each codec, report source bitrate, encoded wire bitrate, output checksum,
receiver loss counters, and hop count. `copy` versus `block` isolates the
cost of `+1/-1` arithmetic while preserving the same block and buffer path.
