# Phase 18 Offline Benchmark Baseline

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-13 at revision `a8c510d1cbc7`. These are local baseline observations,
not performance thresholds. They describe this machine and toolchain only.

## Environment

- OS and architecture: macOS, AArch64
- CPU: Apple M5 Max, 18 logical CPUs
- Rust: `rustc 1.85.0 (4d91de4e4 2025-02-17)`
- Profile: optimized release benchmark profile
- Fixed inputs: checked-in compressed-message fixture, fixed Remoting command,
  fixed 512-byte message body, 128-entry route table, and local plain-TCP
  request/response peer

## Results

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,943.6 ns | 39.00 (1,000 samples) | 4,612.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,250.8 ns | 20.00 (1,000 samples) | 197,407.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 990.7 ns | 43.00 (1,000 samples) | 3,247.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 15.2 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 62,282.0 ns | 77.00 (100 samples) | 9,234.0 (100 samples) |

Allocation figures are aggregate allocator calls and requested bytes divided by
the number of sampled operations; they are not retained-heap measurements.
The loopback result includes local TCP and Tokio scheduling. Repeat measurements
on the same host before comparing revisions. This baseline sets no acceptance
threshold.

## Final current-revision rerun

Captured with the same command and environment on 2026-10-13 at revision
`97e27ff9`. This rerun includes the TopicValidator nullable-input change; its
benchmarks do not target TopicValidator. The results remain machine-specific
observations.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,999.9 ns | 39.00 (1,000 samples) | 4,612.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,282.7 ns | 20.00 (1,000 samples) | 197,407.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,077.6 ns | 43.00 (1,000 samples) | 3,247.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 63,866.0 ns | 77.00 (100 samples) | 9,234.0 (100 samples) |

The sample timing varies from the earlier run while allocation counts and byte
totals match. This is not evidence of a performance regression or improvement.

## Post Producer async executor API revision

Captured with the same command and environment on 2026-10-13 at revision
`0a3d949c`. These measurements include the Producer callback and async sender
executor API change. They remain machine-specific observations, not thresholds.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,468.0 ns | 39.00 (1,000 samples) | 4,612.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,098.5 ns | 20.00 (1,000 samples) | 197,407.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 929.2 ns | 43.00 (1,000 samples) | 3,247.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.3 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 65,799.4 ns | 77.00 (100 samples) | 9,234.0 (100 samples) |

Allocation counts and byte totals match the earlier captures. Timing variation
is not evidence of a performance regression or improvement.

## CheckForbidden error-parity revision

Captured with the same command and environment on 2026-10-14 at revision
`39afef81`. This includes the CheckForbidden retry/error mapping changes. The
measurements remain machine-specific observations, not acceptance thresholds.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,895.0 ns | 39.00 (1,000 samples) | 4,612.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,250.7 ns | 20.00 (1,000 samples) | 197,407.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 999.4 ns | 43.00 (1,000 samples) | 3,247.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 16.0 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 63,480.1 ns | 77.00 (100 samples) | 9,234.0 (100 samples) |

Allocation counts and byte totals match the earlier captures. Timing variation
is not evidence of a performance regression or improvement.

## Reproduction

```bash
cargo bench -p rocketmq-classic --bench offline --locked
cargo clippy -p rocketmq-classic --bench offline --all-features --locked -- -D warnings
cargo fmt --all -- --check
```

## Current Phase 18 code revision

Captured with the reproduction command on 2026-10-16 at code revision
`15c6ca4f` (documentation-only commits follow it). Environment and fixed
inputs match the earlier captures above. This is one same-host sample; timing
is not an acceptance threshold or a cross-machine comparison.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,932.5 ns | 39.00 (1,000 samples) | 4,612.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,162.8 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,176.4 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 15.1 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 59,048.3 ns | 77.00 (100 samples) | 9,234.0 (100 samples) |

The sample meets the Phase 18 offline measurement evidence requirement. Changes
in timing/allocation observations from earlier revisions are recorded without
claiming regression or improvement; this benchmark defines no performance
threshold.

## Remoting custom-header revision — Linux container sample

Captured with the reproduction command on 2026-10-17 at code revision
`b6da737e`. The benchmark ran in the Rust 1.85.0 Bookworm container on Linux
AArch64 with 18 logical CPUs; the container reported only `aarch64` as its
hardware label. This sample is kept separate from the native macOS results
above and must not be compared with them as a performance change.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,197.1 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 712.7 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 806.0 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 12.2 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 5,409.3 ns | 79.00 (100 samples) | 9,442.0 (100 samples) |

These measurements satisfy the current-revision offline benchmark run; they
are observations, not acceptance thresholds. Allocation totals are aggregate
allocator calls and requested bytes divided by sampled operations.

## Phase 18 compatibility-closure revision

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-19 at code revision `aaff724edb116c87e73c2040b73c56841d18ab31`.
Environment and fixed inputs match the native macOS samples above. This is a
machine-specific observation, not a performance threshold or cross-machine
comparison.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,437.1 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,183.8 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,222.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 16.1 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 67,389.6 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing and allocation observations are recorded without an acceptance
threshold. Allocation totals are aggregate allocator calls and requested bytes
divided by sampled operations; loopback timing includes local TCP and Tokio
scheduling.

## Current Phase 18 server-free gate revision

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-21 at code revision `da6345bb`. The benchmark ran on macOS AArch64,
Apple M5 Max, 18 logical CPUs, with Rust 1.85.0. It follows the Remoting
header-decode inventory test update; this machine-specific run sets no
acceptance threshold and is not a cross-machine comparison.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,668.9 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,832.1 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,274.1 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 26.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 44,968.6 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are allocator calls and requested bytes divided by sampled
operations. Timing and allocation observations are not acceptance thresholds;
loopback time includes local TCP and Tokio scheduling.

## Current compatibility-closure rerun

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on 2026-10-23 at revision `bd2fc5b6966bb9efc0cc1f26e0f9653a5164ff65`.
Environment and fixed inputs match the native macOS samples above. The sample
was taken after the PopProcessQueue facade parity change; the benchmarked
paths do not exercise that API. It is a machine-specific observation, not a
performance threshold or a cross-machine comparison.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,409.0 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,494.0 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 3,325.4 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 33.4 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 111,293.8 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are allocator calls and requested bytes divided by sampled
operations. Timing varies by run; this table sets no acceptance threshold.
Loopback timing includes local TCP and Tokio scheduling.

## Concurrent expiry cleanup and nested API inventory revision

Captured with the same command on 2026-10-21 at code revision
`bbcc88ae89c7fb4cf73f59f6abf7290458e217ad`. Environment and fixed inputs match
the native macOS samples above. This is a machine-specific observation, not a
performance threshold or cross-machine comparison.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,545.8 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,408.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,837.6 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 34.7 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 88,039.0 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

The latest timing sample varies from earlier runs; allocation observations are
stable. The benchmark does not exercise concurrent consume expiry and sets no
acceptance threshold.

## Cluster-topic header inventory revision

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-21 at code revision `6441ca56`. The benchmark ran on macOS AArch64,
Apple M5 Max, 18 logical CPUs, with Rust 1.85.0. This machine-specific sample
does not set an acceptance threshold or support cross-machine comparisons.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,746.2 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,994.4 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,407.3 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 29.6 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 77,380.9 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing and allocation observations are recorded without an acceptance
threshold. Allocation totals are aggregate allocator calls and requested bytes
divided by sampled operations; loopback timing includes local TCP and Tokio
scheduling.

## Lite group-info and latest compatibility-closure revision

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-21 at code revision `06ce7eb4`. The benchmark ran on macOS AArch64,
Apple M5 Max, 18 logical CPUs, with Rust 1.85.0. Fixed inputs match the native
macOS captures above. This sample is machine-specific and sets no acceptance
threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,201.2 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,186.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,646.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 29.5 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 58,945.6 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are aggregate allocator calls and requested bytes divided by
sampled operations. Timing variation across runs is not evidence of a
regression or improvement; loopback timing includes local TCP and Tokio
scheduling.

## Client message subtype closure revision

Captured with the same command and environment on 2026-10-21 at code revision
`6e2e3d09`. These observations include the `MessageClientExt` runtime-subtype
adapter and are machine-specific; they set no acceptance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,469.9 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,387.5 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,622.5 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 28.1 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 80,951.7 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation figures are aggregate allocator calls and requested bytes divided by
sampled operations. Timing varies between runs; loopback timing includes local
TCP and Tokio scheduling.

## Null/empty membership rebalance revision

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-21 at code revision `9008c10e`. The benchmark ran on macOS AArch64,
Apple M5 Max, 18 logical CPUs, with Rust 1.85.0. Fixed inputs match the native
macOS captures above. This is a machine-specific sample and sets no acceptance
threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,311.3 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,116.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,187.6 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 15.2 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 69,798.0 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are aggregate allocator calls and requested bytes divided by
sampled operations. Timing variation across runs is not evidence of a
regression or improvement; loopback timing includes local TCP and Tokio
scheduling.

## ProcessQueue expiry error and retry revision

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-21 at code revision `9147a902`. The benchmark ran on macOS AArch64,
Apple M5 Max, 18 logical CPUs, with Rust 1.85.0. Fixed inputs match the native
macOS captures above. This sample is machine-specific and sets no acceptance
threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,347.7 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,136.5 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,187.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.4 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 71,564.5 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are aggregate allocator calls and requested bytes divided by
sampled operations. Timing variation across runs is not evidence of a
regression or improvement; loopback timing includes local TCP and Tokio
scheduling. The offline benchmark does not measure ProcessQueue expiry.

## Caller-defined MessageExt subtype revision

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-21 at code revision `7fd89286`. The benchmark ran on macOS AArch64,
Apple M5 Max, 18 logical CPUs, with Rust 1.85.0. Fixed inputs match the native
macOS captures above. This machine-specific sample sets no acceptance
threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,251.8 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,154.5 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,178.6 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 13.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 62,831.9 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are aggregate allocator calls and requested bytes divided by
sampled operations. Timing varies between runs; loopback timing includes local
TCP and Tokio scheduling. The offline benchmark does not measure subtype
dispatch directly.

## Current full server-free refresh

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-21 at revision `0f6a0244`. The environment is macOS AArch64, Apple M5
Max, 18 logical CPUs, Rust 1.85.0. Fixed inputs match the native macOS
captures above. This is one machine-specific observation, not an acceptance
threshold or a cross-revision performance claim.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,021.8 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,115.0 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,174.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 17.1 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 66,419.3 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are aggregate allocator calls and requested bytes divided by
sampled operations. Timing varies between runs; loopback timing includes local
TCP and Tokio scheduling.

## Current revision refresh — 2026-10-22

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
revision `d030c74d2be4404db8ee2ac5323ab6bc44f5d8ff`. Environment: macOS
AArch64, Apple M5 Max, 18 logical CPUs, Rust 1.85.0. This is a same-host
observation with the fixed inputs above; it sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,692.2 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,993.0 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,182.5 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 27.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 81,564.5 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation counts and byte totals match the preceding current-revision sample.
Timing is higher in this run and is recorded without interpreting the single
sample as a regression or improvement.

## Current revision after failed-factory lifecycle parity — 2026-10-22 (`cd212ffb`)

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
code revision `cd212ffb`. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. This is a same-host observation with the fixed inputs
above; it sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,194.0 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,101.1 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,243.4 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 13.8 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 69,537.5 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation counts and byte totals match the preceding samples. Timing varies
between runs and is recorded without interpreting the single sample as a
regression or improvement.

## Current revision after push/pull Consumer factory-start parity — 2026-10-22 (`b46612bf`)

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
code revision `b46612bf`. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. This is a same-host observation with the fixed inputs
above; it sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,173.0 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,115.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,246.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.2 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 67,130.6 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation counts and byte totals match the preceding samples. Timing varies
between runs and is recorded without interpreting the single sample as a
regression or improvement.

## Current revision after Query Admin header parity fix — 2026-10-22

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
revision `80faab5a`. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. Inputs match the fixed fixtures above. This is a single
host observation and sets no acceptance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,349.2 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,341.8 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,533.5 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 34.0 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 85,556.8 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are per-operation observations. Timing varies between runs;
the sample is not interpreted as a cross-revision regression or improvement.

## Current revision after Producer startup-route parity — 2026-10-22

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
revision `5400211e`. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. Inputs and iteration counts match the fixed fixtures above.
This is a single-host observation and sets no acceptance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,299.3 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,165.8 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,203.1 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 13.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 73,612.1 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation observations match earlier captures. Timing varies across runs and
does not imply a cross-revision regression or improvement; loopback time
includes local TCP and Tokio scheduling.

## Historical benchmark sample after Producer startup-route test checkpoint — 2026-10-22

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
code revision `561bae6d`. Its startup transport-error regression asserted lease
rollback at the time; a later pinned Java probe found that assertion differed
from Java, and the corrected behavior is recorded at `4cabbffb`. The measured
operation inputs and outputs below remain a valid single-host observation and
set no threshold. Environment, inputs, and iteration counts match the fixed
fixtures above.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,267.9 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,138.2 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,190.4 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 15.2 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 67,646.8 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation observations match the prior capture. Timing varies between runs
and does not imply a regression or improvement; loopback time includes local
TCP and Tokio scheduling.

## Current revision after LitePull factory-start parity — 2026-10-22 (`ad7a0523`)

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
code revision `ad7a0523`. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. Inputs and iteration counts match the fixed fixtures above;
this single-host observation sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,949.2 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,089.1 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,223.0 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.2 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 63,999.6 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation observations match earlier captures. Timing varies between runs
and is recorded without interpreting the sample as a regression or improvement.

## Current revision after LitePull listener-route failure parity — 2026-10-22 (`aab73bb3`)

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
code revision `aab73bb3`. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. Inputs and iteration counts match the fixed fixtures above;
this single-host observation sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,982.8 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,358.2 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,896.7 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.5 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 80,473.8 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation observations match earlier captures. Timing varies between runs
and is recorded without interpreting the sample as a regression or improvement.

## Current revision after LitePull schedule-failure and validation-order parity — 2026-10-22 (`c38f4bc9`)

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
code revision `c38f4bc9`. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. Inputs and iteration counts match the fixed fixtures above;
this single-host observation sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,331.9 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,383.4 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,641.4 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 28.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 95,294.2 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation observations match earlier captures. Timing is higher in this run
and is recorded without interpreting the single sample as a regression or
improvement; loopback time includes local TCP and Tokio scheduling.

## Current revision after trace identity and ACL signing parity — 2026-10-22 (`1eb5b6ba`)

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
code revision `1eb5b6ba`. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. Inputs and iteration counts match the fixed fixtures above;
this single-host observation sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,252.8 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,123.6 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,196.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.7 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 61,246.3 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation observations match earlier captures. Timing varies between runs
and is recorded without interpreting this sample as a regression or
improvement; loopback time includes local TCP and Tokio scheduling.

## Metadata wrapper identity gate refresh

Captured on 2026-10-22 at code revision `91f842f4` with the same command and
environment. The DTO identity change does not exercise a benchmark-specific
path; these are local observations rather than performance thresholds.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,304.8 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,186.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,208.1 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 15.3 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 69,611.5 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing variation across runs is not evidence of a performance regression or
improvement.


## Locale-sensitive topic metrics parity gate — 2026-10-22 (`2887a032`)

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
code revision `2887a032`. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. Inputs and iteration counts match the fixed fixtures above;
this single-host observation sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,654.9 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,104.4 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,204.1 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.7 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 58,916.9 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation observations match earlier captures. Timing varies between runs and
is recorded without interpreting the single sample as a regression or
improvement; loopback time includes local TCP and Tokio scheduling.

## Message decoder cursor parity current-tree sample — 2026-10-22 (`2e1122cf`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `2e1122cf`, after preserving Java
`MessageDecoder.decodeProperties(ByteBuffer)` cursor and truncation behavior.
Environment: macOS AArch64, Apple M5 Max, 18 logical CPUs, Rust 1.85.0. This
benchmark does not isolate the changed property decoder path; the run records
the current release baseline and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,306.4 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,137.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,334.5 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 64,807.2 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Phase 18 shared-accumulator current-tree gate sample — 2026-10-24 (`49fffa75`)

Captured with `cargo bench --offline --locked -p rocketmq-classic --bench
offline` on code revision `49fffa75`, using
`/tmp/rocketmq-phase18-shared-accumulator`. Environment: macOS AArch64, Apple
M5 Max, 18 logical CPUs, release profile, Rust 1.85.0. This general benchmark
does not isolate the accumulator cache change and sets no performance
threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,495.8 ns | 62.00 (1,000 samples) | 6,787.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,108.2 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,197.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 15.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 66,026.8 ns | 125.00 (100 samples) | 12,873.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Phase 18 GO_AWAY current-tree gate sample — 2026-10-24 (`dbbed745`)

Captured with `CARGO_TARGET_DIR=/tmp/rocketmq-phase18-target cargo bench
--offline --locked -p rocketmq-classic --bench offline` on code/test revision
`dbbed745`, after matching explicit `closeChannels` behavior for retained
GO_AWAY generations. Environment: macOS 26.6.1 AArch64, Apple M5 Max, 18
logical CPUs, release profile, Rust 1.85.0. This general benchmark does not
isolate the remoting lifecycle change and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 8,725.7 ns | 62.00 (1,000 samples) | 6,787.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,323.9 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,635.5 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 30.4 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 195,145.2 ns | 121.00 (100 samples) | 12,779.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Phase 18 decoder-cursor current-tree gate sample — 2026-10-24 (`924466d6`)

Captured with `cargo bench --offline --locked -p rocketmq-classic --bench
offline` on code/test revision `924466d6`, after the public message-decoder
cursor changes. Environment: macOS 26.6.1 AArch64, Apple M5 Max, 18 logical
CPUs, release profile, Rust 1.85.0. This general benchmark does not isolate
the decoder change and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 4,769.6 ns | 62.00 (1,000 samples) | 6,787.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,790.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,035.4 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 24.5 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 48,577.8 ns | 121.00 (100 samples) | 12,779.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Phase 18 committed-tree gate sample — 2026-10-24 (`9333cbc2`)

Captured with `cargo bench --offline --locked -p rocketmq-classic --bench
offline` on code/test revision `9333cbc2`, after the Fastjson leading-dot and
incomplete-exponent compatibility correction. Environment: macOS 26.6.1
AArch64, Apple M5 Max, 18 logical CPUs, release profile, Rust 1.85.0. This
general benchmark does not isolate the parser change and sets no performance
threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,181.1 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,553.9 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,558.8 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 28.1 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 57,067.3 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Request-header API inventory current-tree sample — 2026-10-23 (`e7d779af`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `e7d779af`, after completing the public header API
inventory and adding the inherited RPC routing fields to
`NotifyUnsubscribeLiteRequestHeader`. Environment: macOS 26.6.1 AArch64, Apple
M5 Max, 18 logical CPUs, Rust 1.85.0. This benchmark does not isolate the
header change and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,192.7 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,104.1 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,187.1 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.4 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 67,394.2 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Current server-free gate — 2026-10-23 (`98457158`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `98457158`. Environment and fixed inputs match the
native macOS samples above: macOS 26.6.1 AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. This is a machine-specific observation, not a performance
threshold or cross-machine comparison.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,157.1 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,159.5 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,249.7 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.3 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 62,842.8 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Allocation values are aggregate calls and requested bytes divided by sampled
operations. Timing varies by run; loopback time includes local TCP and Tokio
scheduling. No acceptance threshold is set.

## MQClientAPIFactory current-tree sample — 2026-10-23 (`805619a9`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `805619a9`, after adding the direct client API
factory and correcting its nullable NameServer callback behavior. Environment:
macOS 26.6.1 AArch64, Apple M5 Max, 18 logical CPUs, Rust 1.85.0. This benchmark
does not isolate the factory changes and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,359.6 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,365.6 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,639.6 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 30.0 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 50,180.2 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## ProcessQueue lock fields and `sendSmartMsg` current-tree sample — 2026-10-23 (`9fdea903`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on Phase 18 code revision `9fdea903584f893158a4ab1d5622c36b27fbf910`.
Environment and fixed inputs match the native macOS samples above. This sample
does not isolate ProcessQueue or Producer compatibility branches and sets no
performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,943.2 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,429.5 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,642.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 20.5 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 62,463.3 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling. Allocation totals are
aggregate allocator calls and requested bytes divided by sampled operations.

## ProcessQueue lifecycle parity current-tree sample — 2026-10-23 (`2ce652e7`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `2ce652e7`, after matching Java 5.5.1's
`consuming` and `lastConsumeTimestamp` transitions across public
ProcessQueue operations. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. The benchmark does not isolate ProcessQueue operations and
sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,077.4 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,278.5 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,218.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.2 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 65,508.0 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Declared message-size parity current-tree sample — 2026-10-22 (`8acbf137`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on the source committed as `8acbf137`, after full message decoding
was changed to use parsed field lengths instead of the declared total size.
Environment: macOS AArch64, Apple M5 Max, 18 logical CPUs, Rust 1.85.0. The
benchmark does not isolate this decoder boundary; it records the current
release baseline and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,506.3 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,142.5 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,363.3 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.1 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 74,448.9 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Lithuanian ACL casing parity current-tree sample — 2026-10-22 (`505bbe97`)

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
code revision `505bbe97`. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. The benchmark does not exercise ACL IPv6 expansion; this
sample records the current release baseline and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,605.9 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,144.1 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,251.1 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 15.3 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 60,195.1 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation observations match earlier captures. Timing varies between runs and
is recorded without interpreting the single sample as a regression or
improvement; loopback time includes local TCP and Tokio scheduling.


## ACL IPv6 expansion locale parity current-tree sample — 2026-10-22 (`6155f4cc`)

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
code revision `6155f4cc`. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. The benchmark does not exercise ACL IPv6 expansion; this
sample records the current release baseline and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,598.7 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,153.2 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,278.5 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 15.6 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 61,025.0 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation observations match earlier captures. Timing varies between runs and
is recorded without interpreting the single sample as a regression or
improvement; loopback time includes local TCP and Tokio scheduling.


## Remoting code-description locale parity current-tree sample — 2026-10-22 (`a4136667`)

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
code revision `a4136667`, after matching Java remoting code descriptions under
Turkish/Azeri locales. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. This offline suite does not directly benchmark metric-label
description lookup; the sample keeps the release baseline current and sets no
performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,633.0 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,135.2 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,252.9 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.3 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 62,983.8 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing variation across runs is not evidence of a performance regression or
improvement; loopback time includes local TCP and Tokio scheduling.


## Compression parser locale parity current-tree sample — 2026-10-22 (`c9a6f939`)

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
code revision `c9a6f939`. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. The benchmark does not directly exercise compression-name
parsing; this sample records the current release baseline and sets no
performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,731.5 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,153.5 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,265.4 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.4 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 64,963.9 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation observations match earlier captures. Timing varies between runs and
is recorded without interpreting the single sample as a regression or
improvement; loopback time includes local TCP and Tokio scheduling.

## Nonpositive decoder length parity current-tree sample — 2026-10-22 (`8ca83fc8`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on source revision `8ca83fc8`, after matching Java's handling of
nonpositive full-decode body/property lengths. Environment: macOS AArch64,
Apple M5 Max, 18 logical CPUs, Rust 1.85.0. The benchmark does not isolate the
changed branches; it records the current release baseline and sets no
performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,786.3 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,085.0 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,278.4 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 13.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 67,460.8 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Dropped POP callback parity release sample — 2026-10-22 (`ead33f69`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on production code revision `ead33f69`, which contains the POP
in-flight callback fix. The following commit `60238931` expands regression
coverage and updates documentation only. Environment: macOS AArch64, Apple M5
Max, 18 logical CPUs, Rust 1.85.0. This is a single-host release observation;
it does not set a performance threshold or isolate the POP path.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,115.7 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,120.2 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,187.7 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 69,666.2 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## ProcessQueue removal parity current-tree sample — 2026-10-22 (`aa3cec73`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `aa3cec73`, after matching Java 5.5.1's ready versus
in-flight `removeMessage` behavior and preservation of the dispatch flag.
Environment: macOS AArch64, Apple M5 Max, 18 logical CPUs, Rust 1.85.0. The
benchmark does not isolate ProcessQueue operations; this is a current release
observation and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,795.5 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,130.8 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,222.1 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.5 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 65,963.9 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## ProcessQueue input-error parity current-tree sample — 2026-10-22 (`267173dd`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `267173dd`, after matching Java's malformed
`MAX_OFFSET` error flow and keeping `lastPullTimestamp` changes at the pull
request boundary. Environment: macOS AArch64, Apple M5 Max, 18 logical CPUs,
Rust 1.85.0. This benchmark does not isolate ProcessQueue operations and sets
no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,161.8 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,123.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,536.8 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 36.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 60,805.7 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## ProcessQueue empty-commit parity current-tree sample — 2026-10-22 (`e1b8ffdb`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `e1b8ffdb`, after matching Java 5.5.1's empty
ordered-consume `commit` exception and unchanged queue state. Environment:
macOS AArch64, Apple M5 Max, 18 logical CPUs, Rust 1.85.0. The benchmark does
not isolate ProcessQueue operations and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,319.4 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,264.0 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,673.9 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 28.5 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 71,359.5 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## ProcessQueue duplicate-offset parity current-tree sample — 2026-10-23 (`8bf6ee40`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `8bf6ee40`, after preserving ready and in-flight
messages at a duplicate queue offset. Environment: macOS AArch64, Apple M5
Max, 18 logical CPUs, Rust 1.85.0. The benchmark does not isolate ProcessQueue
operations and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,558.6 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,104.9 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,231.6 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 16.5 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 63,067.8 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## ProcessQueue clear parity current-tree sample — 2026-10-23 (`85f210bd`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `85f210bd`, after preserving Java 5.5.1's
`consuming` flag across `clear`. Environment: macOS AArch64, Apple M5 Max, 18
logical CPUs, Rust 1.85.0. The benchmark does not isolate ProcessQueue
operations and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,689.7 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,077.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,192.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 16.8 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 61,103.2 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## ProcessQueue try-unlock overflow parity current-tree sample — 2026-10-23 (`47cfbf13`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `47cfbf13`, after matching Java 5.5.1's signed
`AtomicLong` try-unlock counter wrap. Environment: macOS AArch64, Apple M5 Max,
18 logical CPUs, Rust 1.85.0. The benchmark does not isolate ProcessQueue
operations and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,241.8 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,121.8 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,202.0 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 15.8 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 68,521.4 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## ProcessQueue negative batch-size parity current-tree sample — 2026-10-23 (`c3b53fcb`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `c3b53fcb`, after returning a typed error for negative
`ProcessQueue.take_messages` batch sizes. Environment: macOS AArch64, Apple M5
Max, 18 logical CPUs, Rust 1.85.0. This benchmark does not isolate
ProcessQueue operations and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,106.5 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,294.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,541.3 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 29.1 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 49,172.5 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Signed timeout and MQClientAPI getter parity current-tree sample — 2026-10-23 (`05e51237`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `05e51237`, after matching Java's signed synchronous
RPC timeout connection order and exposing the `MQClientAPIImpl` remoting and
live NameServer-list getters. Environment: macOS 26.6.1 AArch64, Apple M5 Max,
18 logical CPUs, Rust 1.85.0. The benchmark does not isolate these API changes
and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,149.6 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,225.2 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,213.8 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.6 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 62,571.8 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Scheduler lifecycle current-tree sample — 2026-10-23 (`4c337b1e`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `4c337b1e`, after adding lifecycle controls to the
two runtime-owned scheduled executors. Environment: macOS 26.6.1 AArch64,
Apple M5 Max, 18 logical CPUs, Rust 1.85.0. This benchmark does not isolate the
scheduler API changes and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,351.6 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,367.9 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,605.3 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 34.0 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 47,649.6 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Scheduler invocation and wait current-tree sample — 2026-10-23 (`04eec879`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `04eec879`, after adding timed bulk invocation and
scheduled-task wait/status methods. Environment: macOS 26.6.1 AArch64, Apple
M5 Max, 18 logical CPUs, Rust 1.85.0. This benchmark does not isolate the
scheduler API changes and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,977.1 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,064.2 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,396.3 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 31.7 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 51,556.0 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Inherited ServiceThread control current-tree sample — 2026-10-23 (`bd4e073c`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `bd4e073c`, after mapping the inherited
RebalanceService and PullMessageService controls. Environment: macOS 26.6.1
AArch64, Apple M5 Max, 18 logical CPUs, Rust 1.85.0. This benchmark does not
isolate the service-control changes and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,079.6 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,178.2 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,588.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 32.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 49,220.8 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Service-handle owner retention current-tree sample — 2026-10-23 (`1f6cd086`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `1f6cd086`, after making each service handle retain
its client runtime owner like the pinned Java service objects. Environment:
macOS 26.6.1 AArch64, Apple M5 Max, 18 logical CPUs, Rust 1.85.0. The benchmark
does not isolate this lifetime change and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,403.5 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,859.1 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,928.1 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 24.1 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 33,905.0 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Backpressure read-lock lifetime current-tree sample — 2026-10-23 (`be58bf98`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `be58bf98`, after retaining the Java-compatible
backpressure read lock across pending async semaphore acquisition. Environment:
macOS 26.6.1 AArch64, Apple M5 Max, 18 logical CPUs, Rust 1.85.0. This benchmark
does not isolate the backpressure change and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,356.6 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,302.9 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,594.6 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 16.8 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 60,372.0 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Factory null-scheduler parity current-tree sample — 2026-10-23 (`8fd5822e`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on code revision `8fd5822e`, after adding explicit Java-null
`ScheduledExecutorService` behavior to `MQClientApiFactory`. Environment:
macOS 26.6.1 AArch64, Apple M5 Max, 18 logical CPUs, Rust 1.85.0. The benchmark
does not isolate this factory behavior and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,649.1 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,131.9 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,278.9 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 16.0 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 65,261.8 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Controller API parity current-tree sample — 2026-10-23 (`3478c725`)

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
the controller/replica API implementation tree before the later test-only
stress commit. Environment: macOS AArch64, Apple M5 Max, 18 logical CPUs,
release profile, Rust 1.85.0. This sample does not isolate the controller API
and sets no acceptance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,267.1 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,124.8 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,195.1 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 13.5 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 75,512.4 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## NameServer scan and offset-lock current-tree sample — 2026-10-23 (`875e47d9`)

Captured with `cargo bench --offline --locked -p rocketmq-classic --bench
offline` on code revision `875e47d9`, after aligning NameServer scan admission
with Java's bounded executor order and switching Unix offset-store ownership
to POSIX record locks. Environment: macOS 26.6.1 AArch64, Apple M5 Max, 18
logical CPUs, Rust 1.85.0. This general benchmark does not isolate either
change and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,230.5 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,128.5 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,153.3 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.0 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 62,364.0 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Consumer statistics logging current-tree sample — 2026-10-23 (`4251b2b3`)

Captured with `cargo bench --offline -p rocketmq-classic --bench offline
--locked` on implementation revision `4251b2b3`, after adding scheduled
consumer statistics events. Environment: macOS 26.6.1 AArch64, Apple M5 Max,
18 logical CPUs, Rust 1.85.0. The benchmark does not isolate statistics
logging and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,322.3 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,182.5 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,195.6 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 16.2 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 61,575.2 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Current-tree server-free gate sample — 2026-10-23 (`ea17b8fc`)

Captured with `cargo bench --offline --locked -p rocketmq-classic --bench
offline` on code/test revision `ea17b8fc`, after adding the internal Producer
latency-fault regression. Environment: macOS 26.6.1 AArch64, Apple M5 Max, 18
logical CPUs, Rust 1.85.0. This general benchmark does not isolate that change
and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,575.2 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,088.0 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,195.4 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.5 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 66,750.9 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Phase 18 current-tree gate sample — 2026-10-24 (`23ef8749`)

Captured with `cargo bench --offline --locked -p rocketmq-classic --bench
offline` on code/test revision `23ef8749`, after the Fastjson EOF-marker
compatibility correction. Environment: macOS 26.6.1 AArch64, Apple M5 Max, 18
logical CPUs, release profile, Rust 1.85.0. This general benchmark does not
isolate the parser change and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,067.8 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,115.8 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,737.5 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 27.5 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 69,602.0 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Phase 18 committed-tree gate sample — 2026-10-24 (`8b375eac`)

Captured with `cargo bench --offline --locked -p rocketmq-classic --bench
offline` on code/test revision `8b375eac`, after expanding Fastjson numeric
suffix and string-field coercion coverage. Environment: macOS 26.6.1 AArch64,
Apple M5 Max, 18 logical CPUs, release profile, Rust 1.85.0. This general
benchmark does not isolate the parser change and sets no performance
threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,370.7 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,435.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,646.8 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 30.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 81,298.5 ns | 82.00 (100 samples) | 9,561.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.


## Phase 18 current-tree outbound-write gate sample — 2026-10-24 (`719e5aa3`)

Captured with `CARGO_TARGET_DIR=/tmp/rocketmq-phase18-target cargo bench
--offline --locked -p rocketmq-classic --bench offline` on code/test revision
`719e5aa3`, after the Remoting outbound write-flow change. Environment:
macOS 26.6.1 AArch64, Apple M5 Max, 18 logical CPUs, release profile, Rust
1.85.0. This general benchmark does not isolate the transport change and sets
no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 6,079.4 ns | 62.00 (1,000 samples) | 6,787.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,583.9 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,618.4 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 36.6 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 71,769.8 ns | 121.00 (100 samples) | 12,779.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Phase 18 malformed-count current-tree gate sample — 2026-10-24 (`0cfb20ad`)

Captured with `CARGO_TARGET_DIR=/tmp/rocketmq-phase18-target cargo bench
--offline --locked -p rocketmq-classic --bench offline` on code/test revision
`0cfb20ad`, after the compact inner-count cursor parity change. Environment:
macOS 26.6.1 AArch64, Apple M5 Max, 18 logical CPUs, release profile, Rust
1.85.0. The benchmark does not isolate the malformed-input behavior and sets
no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 5,997.5 ns | 62.00 (1,000 samples) | 6,787.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,536.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,904.1 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 28.7 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 76,442.3 ns | 121.00 (100 samples) | 12,779.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Phase 18 ErrorSource identity current-tree gate sample — 2026-10-24 (`1e928372`)

Captured with `CARGO_TARGET_DIR=/tmp/rocketmq-phase18-target cargo bench
--offline --locked -p rocketmq-classic --bench offline` on code/test revision
`1e928372`. Environment: macOS 26.6.1 AArch64, Apple M5 Max, 18 logical CPUs,
release profile, Rust 1.85.0. The benchmark does not isolate the error-equality
change and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 5,357.9 ns | 62.00 (1,000 samples) | 6,787.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,021.6 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,292.4 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 27.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 88,290.4 ns | 121.00 (100 samples) | 12,779.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Phase 18 current-tree full-gate sample — 2026-10-24 (`38a7bd83`)

Captured with `cargo bench --offline --locked -p rocketmq-classic --bench
offline` on the current Phase 18 tree. Cargo used the isolated target
`/tmp/rocketmq-phase18-current-gates-20261024T172656Z`. Environment: macOS
26.6.1 AArch64, Apple M5 Max, 18 logical CPUs, release profile, Rust 1.85.0.
This general benchmark does not isolate the ResponseFuture rendering change
and sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,729.1 ns | 62.00 (1,000 samples) | 6,787.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,139.5 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,242.5 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.4 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 65,505.8 ns | 125.00 (100 samples) | 12,873.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Phase 18 current-tree constructor API gate sample — 2026-10-24 (`c497f376`)

Captured with `cargo bench --offline --locked -p rocketmq-classic --bench
offline` after exposing the constructor-only `ProduceAccumulator` type. Cargo
used `/tmp/rocketmq-phase18-current-gates-20261024T172656Z`. Environment:
macOS 26.6.1 AArch64, Apple M5 Max, 18 logical CPUs, release profile, Rust
1.85.0. This general benchmark does not isolate the public API addition and
sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,053.2 ns | 62.00 (1,000 samples) | 6,787.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,111.6 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,315.1 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 15.1 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 63,448.4 ns | 125.00 (100 samples) | 12,873.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.

## Phase 18 final current-head refresh — 2026-10-24 (`4b7d0f27`)

Captured after the Phase 18 compatibility closure with
`cargo bench --offline --locked -p rocketmq-classic --bench offline`. Cargo
used the release profile on macOS 26.6.1 AArch64, Apple M5 Max, 18 logical
CPUs, with `rustc 1.85.0 (4d91de4e4 2025-02-17)`. This records the final
tracked-code revision; it is an observational baseline and sets no performance
threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,575.4 ns | 62.00 (1,000 samples) | 6,787.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,152.1 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,176.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.1 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 78,032.1 ns | 125.00 (100 samples) | 12,873.0 (100 samples) |

Timing varies between runs and is not evidence of a regression or improvement;
loopback time includes local TCP and Tokio scheduling.
