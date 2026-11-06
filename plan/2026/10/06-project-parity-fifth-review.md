# Fifth parity review: trace identity, shutdown, and failure isolation

## 1. Baseline, scope, and result

Date: 2026-10-28. Reviewed revision: `dbbabeb0cb88c503ff79182f2497b5252eb1c3c1`. Production changes reviewed: `83536bab` and `400774e1`. The contract remains public API/input/settings parity and execution-logic parity with Java RocketMQ client 5.5.1, allowing Rust-native components.

This round inspected the complete fourth-review remediation diff and neighboring trace hooks, public group mutations, message/trace publication, shutdown, and encoder failure handling. Unchanged Protocol/Remoting and other project subsystems were not exhaustively re-audited. The pinned client source JAR SHA-256 is `de63ae377bcc9b461b0d65b4cb77673d9daabb8f46caabef903826ea7c627e76`.

**Three new findings are confirmed: one P1 and two P2.** H01's namespace cases now pass; the findings below concern separate capture and lifecycle boundaries. This review adds documents and local evidence only, preserving earlier records and production code.

| ID | Priority | Finding |
| --- | --- | --- |
| I01 | P2 | Producer Pub/EndTransaction trace uses the startup group instead of the current hook context |
| I02 | P1 | General/Lite trace shutdown waits outside its timeout and grows with queued sends |
| I03 | P2 | A trace encoding failure terminates the general dispatcher, rejecting subsequent valid traces |

P1 denotes a service-shutdown delay that can grow with the trace backlog during Broker failure. P2 denotes incorrect trace attribution or persistent loss of tracing after one failed batch.

## 2. Verification and evidence

| Check | Result |
| --- | --- |
| H01 namespace matrix | Replayed the corrected probe against this revision: all four Lite foreign/nested/configured/absent namespace cases match pinned Java before/after groups, using actual Rust trace SEND rows. |
| Fourth-review implementation | Consumer/Lite/Producer helpers now use the unrestricted NamespaceUtil operation. Lite correctly separates before and after normalization. Group/topic regression tests pass. |
| Workspace | `cargo test --workspace --offline` exits 0: **2,067 passed, 0 failed, 0 ignored**, across 94 result summaries, including doctests and earlier LZ4/Lite regressions. |
| New probes | All eight Rust/Java executions complete and assert the three observed differences. |

Local artifacts follow the repository's ignored `/evidence/` convention: [results](../../../evidence/plan/2026/10/06-project-parity-fifth-review-evidence/results.json), [previous-case replay](../../../evidence/plan/2026/10/06-project-parity-fifth-review-evidence/previous-review-check.json), and [workspace summary](../../../evidence/plan/2026/10/06-project-parity-fifth-review-evidence/workspace-test-summary.json). Results retain revision, source/probe hashes, toolchains, stdout/stderr, and execution limits. Evidence remains local and must not be staged or committed.

Reproduce with cached Rust/Maven dependencies, JDK, Maven, and the repository toolchain:

```sh
python3 evidence/plan/2026/10/06-project-parity-fifth-review-evidence/run_probes.py --output /tmp/rocketmq-fifth-review-results.json
```

Rust uses production libraries and scripted loopback NameServer/Broker peers. Identity checks read actual business and trace SEND commands; EndTransaction uses the public hook execution helper without a transaction Broker RPC. Java invokes actual hooks with synthetic send context/result and a reflected client factory. Shutdown compares actual Rust stalled network sends with Java's actual dispatcher shutdown over injected blocking executor tasks; these are different controlled fixtures, not a live cross-client outage test. Isolation invokes Java's actual AsyncDataSendTask/executor and queue APIs without starting its internal producer; Java trace delivery is not claimed. No live RocketMQ service, full Maven fixture verify, Clippy, or cross-platform check was run in this review.

## 3. Findings and required improvements

### 3.1 I01 — Producer trace bypasses the current hook group

**Locations:** [startup installation](../../../crates/rocketmq-classic-producer/src/producer.rs#L1249), [Pub group](../../../crates/rocketmq-classic-producer/src/trace.rs#L171), [EndTransaction hook](../../../crates/rocketmq-classic-producer/src/trace.rs#L228), [live setter](../../../crates/rocketmq-classic-producer/src/producer.rs#L1011), and [current end-transaction context](../../../crates/rocketmq-classic-producer/src/producer.rs#L311). Java: `trace/hook/SendMessageTraceHookImpl.java:57`, `EndTransactionTraceHookImpl.java:60`, and `impl/producer/DefaultMQProducerImpl.java:971,1198`.

Both Rust trace hooks retain a startup `producer_group` and use it instead of `context.producer_group`. Java reads the group from each callback's context. Namespace normalization is now correct, but its source value remains stale.

**Executed cases:** Start a tracing-enabled Producer as `initial-producer`, call `set_producer_group(Some("later-producer"))`, send one message, and execute its end-transaction hooks. Rust's business SEND header and both hook contexts use `later-producer`; its Pub and EndTransaction trace rows use `initial-producer`. Java hooks produce both trace groups as `later-producer`. A second run registers a custom hook before startup that changes the callback group to `hook-producer`: Rust contexts show that value but traces still use `initial-producer`; Java traces use `hook-producer`.

**Improvement:** Source each trace group from the actual nullable hook context at its Java capture stage. Preserve Pub's before snapshot for its matching after callback. EndTransaction must use that invocation's context. Remove startup group snapshots from these decisions; do not change the internal trace-producer identity, runtime registrations, or business RPC-header capture boundaries.

**Acceptance:** Cover post-start group mutation and earlier-hook mutation for Pub and EndTransaction, including null through callable hook APIs and plain/foreign/nested namespace cases. Verify actual Pub/EndTransaction trace SEND rows, hook context values, and business SEND group independently. Preserve H01 normalization and Pub before/after correlation.

### 3.2 I02 — Trace shutdown's drain is outside the timeout

**Locations:** [general shutdown](../../../crates/rocketmq-classic-producer/src/trace.rs#L579), [Lite shutdown](../../../crates/rocketmq-classic-consumer/src/lite/trace.rs#L432), [serial trace publication](../../../crates/rocketmq-classic-producer/src/trace.rs#L748), and owner shutdown paths in Producer `shutdown_trace_dispatcher`, Push `shutdown_trace_dispatcher`, and Lite `shutdown`. Java: `trace/AsyncTraceDispatcher.java:205` and Common `utils/ThreadUtils.java:185`.

Both Rust shutdown methods first await channel admission for `Shutdown`, then await its oneshot reply. Only afterward do they start the 5-second general or 6-second Lite worker-join timeout. The reply is sent after preceding trace sends and internal-producer shutdown complete, so the advertised timeout does not limit those waits. A full command queue adds another unbounded-by-grace admission wait. The serial worker awaits each send's own deadline, making total drain time grow with queued work.

**Executed case:** Configure trace batch size 1, enqueue four valid contexts, wait for the first trace SEND, and withhold replies. General and Lite shutdown both remain pending after 12 seconds. Releasing replies then lets both finish at approximately 12 seconds. Java's actual dispatcher shutdown, with two interruptible 30-second tasks in its trace executor, returns after approximately 5 seconds and terminates that executor. Java's source uses a shared 5-second graceful executor wait followed by a bounded 5-second cancellation wait, rather than waiting for each queued send to finish serially.

**Impact:** Producer/Push/Lite shutdown inherits this trace delay during an outage. Large trace queues can extend service termination far beyond a collective grace interval. The executed result establishes the 12-second boundary; longer backlog amplification is source-derived, not a measured hours-long run.

**Improvement:** Implement a bounded drain/cancel sequence consistent with Java's collective wait policy. Include control-message admission, pending sends, reply wait, worker termination, and internal-producer cleanup within that sequence. Use shared deadlines across these stages; do not restart a full grace period for each queued batch. On expiry, cancel remaining work, join the worker, and stop/unregister the owned trace producer. Retain worker ownership or use a cancellation guard so dropping the shutdown future cannot detach it after the shutdown flag is set. An early channel/worker error must also reach cleanup.

**Acceptance:** Test stalled Broker sends with one/multiple batches and a saturated trace queue for both dispatchers. Termination must remain bounded independently of backlog size, stop accepting new contexts, leave no owned task/producer registration behind, and remain safe on repeated shutdown or caller cancellation. Verify the Producer, Push, and Lite owner paths. A responsive Broker must still flush queued valid traces before termination.

### 3.3 I03 — One failed encoding batch permanently stops the general dispatcher

**Locations:** [unprotected encoding](../../../crates/rocketmq-classic-producer/src/trace.rs#L755), [worker loop](../../../crates/rocketmq-classic-producer/src/trace.rs#L611), and [nullable SubAfter access channel](../../../crates/rocketmq-classic-core/src/trace.rs#L410). Java: `trace/AsyncTraceDispatcher.java:289,304` and `TraceDataEncoder.java:208`.

The public general dispatcher accepts a TraceContext whose SubAfter access channel is null. Its encoder intentionally panics where Java's encoder throws. Rust performs that encoding directly in the sole long-lived worker without a failure boundary. Java submits each AsyncDataSendTask through its executor; an encoding exception fails that task while the dispatcher/executor remain usable.

**Executed case:** Append one SubAfter context with valid region/topic/bean and absent access channel, then flush and append a valid SubBefore context. Rust's worker panics, flush returns `WorkerUnavailable`, and the later append returns `false`. Java's actual task fails with `NullPointerException`; subsequent append returns `true`, and flushing subsequent work leaves its executor able to terminate normally. The Java fixture does not establish Broker delivery of that later trace.

**Impact:** One malformed context permanently disables tracing for a general dispatcher shared by Producer or Push trace paths. This differs from Java's failed-task isolation; it does not require changing public encoder error behavior.

**Improvement:** Isolate serialization/task failure at the batch boundary corresponding to Java's appender task and continue processing later queued work. An internal fallible encoder or a targeted unwind boundary may be used while preserving public encode semantics. Keep the worker and owned producer cleanup reliable if a terminal failure does occur. Do not change the business operation's result because its trace encoding failed.

**Acceptance:** Compare Java/Rust malformed-batch outcomes followed by a separately queued valid batch. The valid append/flush must succeed and its trace SEND must be observed. Cover absent access channel and missing required Pub/EndTransaction bean fields, plus a malformed/valid mixed backlog and final shutdown cleanup. Preserve current valid trace encoding and namespace/group behavior.

## 4. Implementation order and verification

1. Fix I02's shutdown ownership and shared drain/cancel deadlines in both dispatchers, including failure/cancellation cleanup.
2. Fix I03's general appender failure boundary and verify valid work after a failed batch.
3. Fix I01's Pub/EndTransaction context sourcing and retain H01's unrestricted, stage-specific normalization.
4. Run focused Java/Rust identity, stalled-shutdown, and failure-isolation probes. Convert evidence assertions from observed defects to Java-compatible outcomes. Then run:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
mvn -o -f compat/java-reference/pom.xml verify
scripts/check-workspace-metadata.py
```

The 2,067 passing tests remain the non-regression baseline. Retain earlier LZ4 delivery/commit, nullable Lite poll, live-group, and H01 trace cases. Environment-dependent live trace/outage checks may run last where available; deterministic loopback and pinned Java tests establish these client boundaries without production credentials or matching Java executor components.
