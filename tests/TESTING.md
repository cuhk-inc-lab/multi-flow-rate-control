# Testing Guide

How to run tests and what each target covers.

## Quick reference

| Command | What it runs | Typical duration |
|---------|----------------|------------------|
| `make test` | 12 unit/integration tests in `run_tests.c` | ~12 s |
| `make spec-test` | `spec_pipeline` byte-exact (20×188B) | <1 s |
| `make app-test` | `multi_flow_relay` single-flow roundtrip | <1 s |
| `make app-test-multi` | `multi_flow_relay` dual-flow roundtrip | <1 s |
| `make complex-test` | `make test` + file scenarios below | ~12 s |
| `make sanitize` | ASan build + `test` + `spec-test` + `app-test` | ~15 s |
| `make tsan` | TSan build + `test` + `app-test-multi` | varies |
| `make demo` | Manual 2-flow demo (not automated assert) | <1 s |

GitLab CI (`.gitlab-ci.yml`): `unit-test`, `asan`, `tsan`, `app-test` (includes `spec-test`).

---

## Layer 1: `make test` (`tests/run_tests.c`)

### Foundation

| Test | Verifies |
|------|----------|
| `test_packet_create_free` | `DataPacket` alloc, fields, `packet_free` |
| `test_time_utils_sub_add` | Monotonic timespec add/subtract |
| `test_time_utils_sleep_for` | `clock_nanosleep` sleeps at least ~30 ms |
| `test_flow_buffer_roundtrip` | One `FlowCircularBuffer`: 12 packets, producer + consumer threads |
| `test_flow_buffer_shutdown` | After `flow_buffer_shutdown`, enqueue/dequeue return `FB_ERR_SHUTDOWN` |
| `test_mixed_queue_push_pop` | `MixedQueue` FIFO push/pop for two flow IDs |

### FlowManager integration

| Test | Verifies |
|------|----------|
| `test_flow_manager_invalid_route` | Invalid `flow_id` increments `route_errors`, packet dropped |
| `test_flow_manager_e2e` | Two flows, 4 packets each @ 20 ms, pacing **off**, output written to temp fds |

### Spec acceptance

| Test | Verifies |
|------|----------|
| `test_pacing_timeline` | Single flow, pacing **on**, 100 ms between 5 enqueues; pipe read timestamps match interval |
| `test_rate_match_5s` | Single flow, pacing **on**, ~6 s steady ingress; dequeue/enqueue bps within **±1%** over 5 s window |

### Concurrency / routing

| Test | Verifies |
|------|----------|
| `test_dispatcher_avoids_hol` | Flow 0 queue size 1 full; flow 1 packet still dequeues (no head-of-line block) |
| `test_complex_multi_flow` | Two concurrent producers: 40×96 B @ 25 ms + 28×160 B @ 40 ms, pacing **on**; packet and byte counts match |

---

## Layer 2: Application file tests (`cmp`)

All use `--no-pace` for byte-exact comparison unless noted.

### `make spec-test` — spec-aligned path

- **App:** `build/spec_pipeline`
- **Data:** 20 random transport packets (20×188 B)
- **Path:** `FileIngest` → `CircularBuffer` → `packet_framer` → `FlowManager` → `write(fd)`
- **Assert:** `cmp input output`

### `make app-test` — integration harness (single flow)

- **App:** `build/multi_flow_relay`
- **Data:** 20×188 B + 96 B tail (non–block-aligned)
- **Path:** file → encode → `FlowManager` → pipe → decode → file
- **Assert:** `cmp input output`

### `make app-test-multi` — integration harness (dual flow)

- **App:** `build/multi_flow_relay --multi`
- **Data:** two flows, 5×752 B each (encode blocks)
- **Assert:** `cmp` per flow input/output pair

---

## Layer 3: `make complex-test`

Runs `make test` first, then:

| Step | Scenario | Assert |
|------|----------|--------|
| Spec multi-flow | Flow 0: 50×188 B; flow 1: 30×188 B + 127 B tail | `cmp` both pairs |
| Spec large | Single flow, 200×188 B | `cmp` |
| Spec paced smoke | 30×188 B, pacing **on** | Output byte count equals input |
| Relay multi uneven | Flow 0: 8×752 B; flow 1: 3×752 B + 400 B tail | `cmp` both pairs |

---

## Memory and thread safety

| Target | Sanitizer | Scope |
|--------|-----------|--------|
| `make sanitize` | AddressSanitizer + UBSan | `test`, `spec-test`, `app-test` |
| `make tsan` | ThreadSanitizer | `test`, `app-test-multi` |

Note: TSan may fail on some hosts with `unexpected memory mapping`; CI on Ubuntu 22.04 is the reference environment.

---

## Manual demo

```bash
make demo
./build/multi_flow_demo [output_dir]
```

Two in-memory producer threads (8 packets each @ 50 ms), pacing on by default. Prints per-flow metrics; not used in CI.

---

## What is not covered

- Parsing `flow_id` from packet payload (apps assign `flow_id` explicitly)
- Dedicated metrics background daemon thread
- Long-running soak / burst stress beyond `complex-test` sizes
- Pacing byte-exact timing on file-based `app-test` paths (pacing disabled there for `cmp`)
