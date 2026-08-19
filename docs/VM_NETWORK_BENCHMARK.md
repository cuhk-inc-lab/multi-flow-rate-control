# Cross-VM Network Benchmark

Use two VMs for the first test: VM1 sends and VM2 receives. The remaining VMs
are useful for repeat runs or later multi-hop and multi-flow tests.

For a full script catalog (what each shell script does, required env, and
examples), see **[SCRIPTS.md](SCRIPTS.md)**.

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
make wg-demo       # optional: still usable as source/sink
make wire-relay    # per-node encode / forward / decode
```

Allow the chosen UDP port in both VM firewalls/security groups.

## 3. Run the application transfer

The cross-VM wire modes use wire header **v3** (44 bytes): flow ID, block ID,
shard index/count, payload/valid length, timestamps, plus `final_dst` and `ttl`
in the former reserved bytes. They support the following codecs:

- `copy`: four input shards copied unchanged to four wire shards; no
  redundancy or arithmetic.
- `block`: four input shards transformed byte-wise with uniform `+1`; decode
  applies uniform `-1`. Same 4-to-4 geometry as `copy`. Not systematic, but
  `--best-effort` is allowed: skip stuck groups and write decoded present
  shards (do not emit raw `+1` bytes).
- `xor-fec`: four data shards plus one XOR parity shard. The receiver restores
  one missing shard; `--best-effort` writes available data shards from groups
  that still cannot be recovered after the idle timeout.
- `rs-fec`: RS(4,2), with four data shards and two parity shards. The receiver
  restores up to two missing shards when at least four shards arrive; its
  systematic `--best-effort` behavior is the same as `xor-fec`.
- `rs`: RS(4,2) via vendored hqm/rscode (column-wise encode; GPL). Same shard
  geometry and erasure capability as `rs-fec`, but **not wire-compatible**
  (different parity). Recovery uses the matrix erasure path; see
  `docs/RS_CODEC.md` for arbitrary `(k,r)`.

On VM2 (direct two-node test; `local_node_id` defaults to 4, so set it to match
`--final-dst` or use the defaults with `final_dst=4` even on a two-node run):

```bash
./build/wg_multi_pipeline --codec copy --local-node-id 4 \
  --udp-recv 9000 received-copy.ts --idle-sec 5
```

On VM1:

```bash
./build/wg_multi_pipeline --codec copy --rate-mbps 100 \
  --final-dst 4 --ttl 8 \
  --udp-send VM2_IP 9000 input.ts
```

### Application-layer four-hop relay (VM1 → VM2 → VM3 → VM4)

UDP next-hop and header `final_dst` are different. Each node can run the
same `wire_relay` binary:

- VM1 `--source`: encode, fill `final_dst=4`, `sendto(VM2)`
- VM2/VM3: `final_dst != me` → TTL-- and opaque forward (recode hooks reserved)
- VM4 `--local-decode`: `final_dst == 4` → decode to file

Do **not** rely on kernel `ip_forward` to skip the relay processes.

```bash
# VM4 (sink)
./build/wire_relay --local-node-id 4 --listen 9000 --next-hop 127.0.0.1:9 \
  --local-decode --codec copy --output /tmp/out.ts --idle-exit-sec 30

# VM3 (forward)
./build/wire_relay --local-node-id 3 --listen 9000 --next-hop VM4_IP:9000

# VM2 (forward)
./build/wire_relay --local-node-id 2 --listen 9000 --next-hop VM3_IP:9000

# VM1 (source)
./build/wire_relay --local-node-id 1 --listen 9000 --next-hop VM2_IP:9000 \
  --source input.ts --final-dst 4 --ttl 8 --codec copy --rate-mbps 100 \
  --idle-exit-sec 30
```

`wg_multi_pipeline --udp-send` / `--udp-recv` remain valid as source/sink if
you prefer the older CLI. Pipeline design:
[`docs/WIRE_RELAY_PIPELINE.md`](WIRE_RELAY_PIPELINE.md). CLI:
[`apps/wire_relay/README.md`](../apps/wire_relay/README.md).

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

`flow_id` in each `--flow` spec is written into `WireHeader.flow_id`. The
receiver (`--udp-recv`) demuxes **only** by that wire `flow_id` (not UDP peer /
5-tuple). Output files look like:

`{out_prefix}src_<sender_ip>_p<sender_port>_flow_<wire_flow_id>.<suffix>`

The peer tag in the filename is cosmetic (logging); two wire flows from the
same UDP source port still get separate files / decoders.

Use `--out-suffix <flow_id>:<ext>` on the receiver to match each sender input
(e.g. `.txt` / `.ts`). Default remains `.ts`.

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
writes a lean report under `build/wire-matrix-*`:

- `results.md` — status + est. datagram loss% / late% / drop% / recovered% + key latency
- `results.csv` — full counters and latency percentiles
- `logs/` — per-case sender/receiver logs
- Receiver files kept on VM4 under `build/wire-matrix-<ts>-*<input-suffix>`
  (same suffix as the input file; set `KEEP_REMOTE_OUTPUT=0` to delete after hash check)

```bash
CODECS="copy block xor-fec rs-fec rs" RATES="20 24 28 32" \
  ./scripts/run_wire_matrix.sh fyp1@VM4_MANAGEMENT_IP VM4_DATA_IP input.ts
```

Optional teaching mode: append a `[WG_DECODE_MARK]` footer after `Codec_decode`
into the received file (hash will no longer match; status becomes `MARKED`):

```bash
DECODE_MARK=1 CODECS="copy" RATES="10" \
  ./scripts/run_wire_matrix.sh fyp1@VM4_MANAGEMENT_IP VM4_DATA_IP input.ts
# On VM4: tail -c 300 build/wire-matrix-*-copy-10m.<same-suffix-as-input>
```

### Multi-flow Node1 → Node4 matrix

Concurrent flows with explicit `flow_id` (Node1→Node4 only). Writes
`results.md` / `results.csv` / `flows.csv` under `build/wire-multiflow-*`:

> Tip: `run_wire_multiflow_matrix.sh` also supports relay NIC monitoring
> (Node2/Node3 peak+avg Mbps) and keeps artifacts lean by default. See
> **[SCRIPTS.md](SCRIPTS.md)** for all toggles (`MONITOR_RELAYS`, `FETCH_OUTPUT`,
> `KEEP_REMOTE_OUTPUT`, interface overrides, etc.).

```bash
# One local file per flow (recommended)
CODECS="copy xor-fec" RATES="10 20" \
  ./scripts/run_wire_multiflow_matrix.sh fyp1@VM4_MANAGEMENT_IP VM4_DATA_IP \
    a.bin b.bin c.bin d.bin

# Or synthesize N payloads from one seed
FLOWS=4 DURATION_S=10 RATES="10 20" \
  ./scripts/run_wire_multiflow_matrix.sh fyp1@VM4_MANAGEMENT_IP VM4_DATA_IP seed.ts
```

Optional teaching mode (same as single-flow matrix): append a `[WG_DECODE_MARK]`
footer after `Codec_decode`. Hash will not match; successful cases report
`MARKED`:

```bash
DECODE_MARK=1 KEEP_REMOTE_OUTPUT=1 CODECS="xor-fec" RATES="10" \
  ./scripts/run_wire_multiflow_matrix.sh fyp1@VM4_MANAGEMENT_IP VM4_DATA_IP \
    a.bin b.bin
# On VM4: grep -n '\[WG_DECODE_MARK\]' build/wire-multiflow-*-*/out_*
```

Both endpoints must run the same wire protocol version. Synchronize VM1 and
VM4 clocks before interpreting the cross-host transfer or end-to-end delay.

## 4. Report

Keep TCP and loss-free UDP `iperf3` results separate from application results.
For each codec, report source bitrate, encoded wire bitrate, output checksum,
receiver loss counters, and hop count. `copy` versus `block` isolates the
cost of `+1/-1` arithmetic while preserving the same block and buffer path.
