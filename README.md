# RocketMQ Classic Rust Client

A Rust port of the Apache RocketMQ Java client 5.5.1. The Rust workspace packages use version 5.5.1 to identify the Java client release they port; RocketMQ server 5.3.4 is the planned local/live validation baseline. Compatibility work is tracked phase by phase in [`plan/2026/09/README.md`](plan/2026/09/README.md); model and protocol status is documented in [`docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md`](docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md).

## Current workspace

The seven library crates provide common models, protocol codecs, remoting,
Core RPC/runtime services, Producer, Consumer and the public facade. Implementation
covers phases 0–18; dated Phase 19 local Broker results and remaining release
requirements are in [current compatibility status](docs/compatibility/CURRENT_STATUS.md).
See the [compatibility matrix](docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md),
[known differences](docs/compatibility/KNOWN_DIFFERENCES.md) and
[release checklist](docs/release-checklist.md) for the scope of compatibility claims.

The toolchain is pinned to Rust 1.85.0 (edition 2024). Remoting clients must call
`start()` within an active Tokio runtime before transport operations; constructors
do not start background workers. TLS settings reproduce Java defaults; review
[TLS behavior](docs/security.md) before configuring a deployment.

## Producer usage and delivery lifecycle

The facade follows the Java producer lifecycle: create a `ProducerConfig` and
`Producer`, call `start()`, send a mutable `Message`, then await
`shutdown()`. The [compiled producer example](examples/producer.rs) shows this
flow; set `ROCKETMQ_NAMESRV_ADDR` and `ROCKETMQ_EXAMPLE_TOPIC` to run it against
a configured cluster.

Send timeouts and retries do not guarantee that a timed-out message was not
stored: a Broker can accept the message before its response is lost, and a
retry can store it again. Applications that retry after an ambiguous result
should use stable business identifiers and idempotent handling. Consumer
redelivery has the same practical requirement: message handlers should tolerate
receiving a message again before its offset is durably advanced.

Await producer and consumer `shutdown()` before exiting so owned tasks stop,
clients unregister, and offsets can be persisted. Push consumer shutdown uses
`awaitTerminationMillisWhenShutdown` to bound how long it waits for in-flight
consume work; work still running when that wait expires can be cancelled and
delivered again later. Check shutdown results because offset persistence,
orderly unlock, or runtime unregistration can fail.

The Java API reference remains RocketMQ client 5.5.1. RocketMQ Broker 5.3.4 is
the Phase 19 interoperability baseline. Phase 18 source/API audit and live
validation are still in progress; these usage notes do not claim full
compatibility.

Run workspace checks with:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
scripts/check-workspace-metadata.py
```
