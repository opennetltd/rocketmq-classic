# RocketMQ Classic Rust Client

A Rust port of the Apache RocketMQ Java client 5.5.1. The Rust workspace packages use version 5.5.1 to identify the Java client release they port; RocketMQ server 5.3.4 is the planned local/live validation baseline. Compatibility work is tracked phase by phase in [`plan/2026/09/README.md`](plan/2026/09/README.md); model and protocol status is documented in [`docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md`](docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md).

## Current workspace

Implementation work covers phases 0–18. The current parity-remediation source revision is `7e0e9955`, reviewed against Apache RocketMQ Java client 5.5.1. Review findings R01–R09 have code or documentation fixes and local verification: locked workspace tests, strict all-target/all-feature Clippy, offline Maven verify, metadata, formatting, and diff checks passed. The latest complete local aggregate is historical: 23/23 gates and all 37/37 Phase 19 scenarios passed on `413f735a` on 2026-10-26, before the current remediation. Three affected live scenarios were rerun on `7e0e9955` on 2026-10-27: `P5-V02-consumer`, `P9-V03`, and `P13-V02` all passed against 5.3.4; the full 37-scenario matrix was not rerun. GitHub Actions is no longer a validation requirement per the user's 2026-10-27 direction. Final compatibility/release approval still awaits owner inputs MAN-01 through MAN-04. See [current compatibility status](docs/compatibility/CURRENT_STATUS.md), the [Phase 6 audit](docs/compatibility/PHASE_6_REQUIREMENT_AUDIT.md), and the [release checklist](docs/release-checklist.md).

The historical 2026-10-26 aggregate on `413f735a` recorded 23/23 local gates
and all 37/37 Phase 19 live scenarios passing, including the P17 request/reply,
P12 orderly handoff, and P16 ACL/mutual-TLS cases. Raw historical aggregate
output was not retained. Current-revision live validation reran the affected
Pull/Lite/POP scenarios `P9-V03`, `P5-V02-consumer`, and `P13-V02`; their raw
outputs remain local and uncommitted. Other remediation checks are listed in
[current compatibility status](docs/compatibility/CURRENT_STATUS.md). See
[server results](docs/compatibility/SERVER_5_3_4_RESULTS.md) for the dated
scenario records and their limits.

Phase 5's `rocketmq-classic-core` provides the typed one-attempt RPC façade over the common/protocol/remoting crates. Ordinary/Lite pull, producer sync/future/oneway, lifecycle and POP/ACK extension calls have 148 pinned Java invocation comparisons plus scripted status/error/deadline/pending tests. The early P5-V02 extension attempt that received an unsupported Lite subscription response was superseded by the later clean-source Phase 19 aggregate, which recorded all 37 scenarios passing on `413f735a`; the full aggregate has not been repeated on `7e0e9955`. The current `P5-V02-consumer` replay passed, including raw Lite Pull RPC outcomes. Pull/POP RPCs return raw message bytes; Phase 8 provides the bounded decoder and the consumer crates own higher-level processing. See the [Phase 5 method inventory](docs/compatibility/PHASE_5_OFFLINE_METHOD_MATRIX.md) and [handoff](docs/compatibility/PHASE_5_HANDOFF.md).

The lower-level workspace crates are `rocketmq-classic-common`, `rocketmq-classic-protocol`, and `rocketmq-classic-remoting` (bounded codecs and the locally verified TCP/TLS/SOCKS5 remoting transport; Broker/live validation is deferred to Phase 19). Construct a `RemotingClient` and call `start()` from an active Tokio runtime before invoking transport operations; `start()` is idempotent and constructors do not start background workers. The transport exposes async callbacks with configurable callback workers or a caller-supplied callback executor (`set_callback_executor`/`get_callback_executor`; disabled mode ignores custom executors and runs inline), sync futures, deadline-bound admission controls, request pipeline (legacy and context-aware; supports typed abort/error outcomes, Java-order stage composition via `pipe_request_pipeline`), backward-compatible fallible RPC hooks with typed abort/system-error mapping, optional Java-style equality keys for deduplication, and explicit `add_rpc_hook`/`get_rpc_hooks`/`remove_rpc_hook`/`clear_rpc_hooks` (snapshot inspection/removal without exposing a live mutable hook list), outbound hooks after successful connection establishment, Java-style pipeline/after-hook handling after a before-hook failure, and after-hook notification when a processor returns no response, exact-code/default request processors with optional per-registration executors (inbound one-way requests are processed without replies), bounded asynchronous channel/idle events, NameServer list/selection/availability scanning, reachability/writability/explicit-close helpers, configured TCP socket buffers, Java-shaped `socks_proxy_config` JSON CIDR routing, and one GO_AWAY retry that reconstructs a fresh request while preserving its code/ext fields/body. It does not claim Java 5.5.1 parity: callback context now carries the request, optional response/error, send status (Java default-true on timeout after channel establishment and false on established-channel close/shutdown/write failure), timeout flag and elapsed duration, and async response waiting includes Java's +1-second scan grace; the remaining Java/Netty differences are explicitly scoped: Java send-failure/error text taxonomy and executor/thread lifecycle are not reproduced exactly; processor rejection, channel-event ordering, and NameServer wrapper/scan scheduling use documented Tokio behavior; pooled allocator and worker-group controls have Rust-native implementations (bounded per-connection frame reuse and direct inbound dispatch); TLS Java-compatible property/config-file loading, reload at TLS context construction, and process-wide key-decryption strategy registration via `register_decryption_strategy` are implemented, with provider-supported encrypted-key limits. Existing clients retain their TLS context settings. These differences do not imply missing Phase 3 local features and do not claim full Java parity. Rust clears chosen/available NameServer state on socket close. TLS explicitly models Java's client `tls.test.mode.enable` (default `true`) and `tls.client.authServer` (default `false`) trust behavior, including trust-all when either Java condition disables verification; set `test_mode: false, auth_server: true` to validate the CA chain and certificate validity. This mode does not verify peer hostnames or endpoint identity, matching Java 5.5.1; the existing transport regression confirms both clients accept a trusted wrong-host certificate and reject an untrusted CA. See [TLS behavior](docs/compatibility/KNOWN_DIFFERENCES.md) and the [wrong-host transport test](crates/rocketmq-classic-remoting/tests/transport.rs). The scoped Java public remoting API/configuration inventory now covers the listed public/protected client declarations, settings, TLS controls, and Rust public surface in both directions; inventory closure does not mean semantic parity, and the remaining differences are enumerated in the Phase 3 inventory. Configured write watermarks track queued outbound bytes with high/low hysteresis; they do not represent Netty's event-loop write-buffer accounting. The Rust pooled mode reuses up to four frame buffers per connection, and `disable_netty_worker_group` switches inbound dispatch to the connection task; neither reproduces Netty's allocator caches or event-loop thread lifecycle. It is pinned to Rust 1.85.0 (MSRV 1.85) and uses edition 2024. Producer and consumer processing runtimes, namespace/security/trace/operational features, and admin/query surfaces have implementation work through Phase 17. Phase 18 audits and the Phase 19 local live scenario results have dated records; their current-revision scope and outstanding owner inputs are summarized in [current compatibility status](docs/compatibility/CURRENT_STATUS.md). See the [compatibility matrix](docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md), [Phase 3 remoting inventory](docs/compatibility/PHASE_3_REMOTING_INVENTORY.md), and ADR-0001.

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
