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

## 4. Report

Keep TCP and loss-free UDP `iperf3` results separate from application results.
For each codec, report source bitrate, encoded wire bitrate, output checksum,
receiver loss counters, and hop count. `copy` versus `block` isolates the
cost of `+1/-1` arithmetic while preserving the same block and buffer path.
