# Sixth parity review: trace bean selection

## 1. Baseline, scope, and result

Date: 2026-10-28. Reviewed revision: `1ceff1eb64660e7505ec04ce70d9d138b1157aea`. This round reviewed the complete fifth-review remediation (`672139dc`, `35f1ae96`, `3314237b`, `224b5c26`) and adjacent Core trace encoding, Producer/Consumer dispatcher behavior, hook context capture, and shared-runtime shutdown. Unchanged project subsystems were not exhaustively re-audited.

The contract remains public API/input/settings parity and execution-logic parity with Java RocketMQ client 5.5.1, allowing Rust-native components. The pinned client source JAR SHA-256 is `de63ae377bcc9b461b0d65b4cb77673d9daabb8f46caabef903826ea7c627e76`.

**One new P2 finding is confirmed: J01, non-consume trace encoding processes beans that Java does not serialize.** This concerns the accepted input domain of public TraceContext/dispatcher APIs, not a reversal of I01–I03. This review adds documents and local evidence only; production code and earlier records remain unchanged.

## 2. Previous-fix verification and evidence

| Check | Result on this revision |
| --- | --- |
| I01, callback group | Replayed Java/Rust probes: Pub and EndTransaction use `later-producer`, or `hook-producer` after earlier-hook mutation. Business SEND independently retains `later-producer`. |
| I02, bounded shutdown | General and Lite owner shutdown finish in approximately 5 seconds with stalled trace sends for batch sizes 1 and 4. Their internal trace producers reach ShutdownAlready. Full-queue/caller-cancellation and owner regressions pass. |
| I03, failure isolation | Malformed-context flush succeeds; later valid append/flush succeeds and emits a trace SEND. The internal producer is stopped on final shutdown. J01 concerns a Java-readable context incorrectly classified as malformed. |
| Shared runtime | A separate loopback probe shuts down the trace dispatcher while its host Producer stays Running and successfully completes a subsequent business SEND. |
| Workspace | `cargo test --workspace --offline` exits 0: **2,072 passed, 0 failed, 0 ignored**, across 94 result summaries, including doctests. |

Local evidence follows the ignored `/evidence/` convention: [new results](../../../evidence/plan/2026/10/07-project-parity-sixth-review-evidence/results.json), [previous-case replay](../../../evidence/plan/2026/10/07-project-parity-sixth-review-evidence/previous-review-check.json), and [workspace summary](../../../evidence/plan/2026/10/07-project-parity-sixth-review-evidence/workspace-test-summary.json). Results retain revision, source/probe hashes, direct encoder outputs, actual Rust trace rows, and execution boundaries. Do not stage or commit evidence.

Reproduce with cached Rust/Maven dependencies, JDK, Maven, and the repository toolchain:

```sh
python3 evidence/plan/2026/10/07-project-parity-sixth-review-evidence/run_probes.py --output /tmp/rocketmq-sixth-review-results.json
```

Java directly invokes pinned `TraceDataEncoder.encoderFromContextBean`; Rust directly invokes production `TraceContext::encode` and also publishes through its production general dispatcher to scripted loopback peers. No live RocketMQ service or Java trace Broker delivery is claimed. Full Maven fixture verification, Clippy, and cross-platform checks were not rerun.

## 3. J01 — Pub, EndTransaction, and Recall serialize the entire bean list

**Priority: P2. Locations:** [shared encoding loop](../../../crates/rocketmq-classic-core/src/trace.rs#L378), [Pub required field](../../../crates/rocketmq-classic-core/src/trace.rs#L380), [EndTransaction required fields](../../../crates/rocketmq-classic-core/src/trace.rs#L433), [dispatcher error handling](../../../crates/rocketmq-classic-producer/src/trace.rs#L842), and [transfer keys](../../../crates/rocketmq-classic-core/src/trace.rs#L473). Java source: client `trace/TraceDataEncoder.java:168,217,234,249`.

Rust places all five trace types inside one `for bean in trace_beans` loop. Java serializes only `traceBeans.get(0)` for Pub, EndTransaction, and Recall; SubBefore/SubAfter serialize every bean. Java separately collects transfer keys from every bean for all trace types. These are distinct selection rules.

The probes create two beans with message IDs `first`/`second` and keys `first-key`/`second-key`. Context timestamps, request ID, region, group, and required first-bean fields are fixed for byte comparison.

| Input | Java encoder | Rust encoder | Actual Rust dispatcher output |
| --- | --- | --- | --- |
| Pub, two complete beans | 1 row for `first` | 2 rows | 2 rows |
| EndTransaction, two complete beans | 1 row for `first` | 2 rows | Not separately replayed on the wire |
| Recall, two complete beans | 1 row for `first` | 2 rows | Not separately replayed on the wire |
| Pub, complete first bean; second has no message type | 1 row for `first` | Panics on the unused second bean | 0 rows after I03 isolates the panic |
| EndTransaction, complete first bean; second has no message type/state | 1 row for `first` | Panics on the unused second bean | 0 rows after I03 isolates the panic |
| SubBefore/SubAfter, two complete beans | 2 rows | 2 byte-identical rows | SubBefore: 2 rows |

For the complete-bean cases, both implementations collect all four transfer keys, including the second bean's ID/key. The evidence label `invalid_tail` means required serialization fields are absent in a trailing bean; Java accepts the context because those trailing fields are never serialized. The required first bean remains complete.

**Impact:** Publicly constructed multi-bean non-consume contexts create extra trace records or lose the entire valid first record in Rust. I03 correctly keeps the dispatcher alive, but cannot correct an encoder that inspects fields outside Java's selected bean. Default built-in Pub/EndTransaction hooks normally create one bean, so this finding does not claim ordinary one-bean sends always duplicate traces or affect business message delivery.

**Implementation:** Select serialization beans by TraceType: first bean only for Pub/EndTransaction/Recall, all beans for SubBefore/SubAfter. Reuse the existing record formatting without reading or validating nonselected serialization fields. Keep transfer-key collection over the complete original list; limiting the entire context/list to one bean would fix row count while breaking key behavior. Retain empty-list/null/required-first-field failures corresponding to Java, and preserve I03's isolation for genuinely invalid selected data. Lite's consume-only encoder should continue processing all consume beans.

## 4. Verification and acceptance

1. Extend `compat/java-reference/src/main/java/compat/TraceFixtures.java` and the Core trace fixture tests with zero/one/two beans for all five trace types. Include missing message type/state only in a trailing Pub/EndTransaction bean, and missing required fields in the selected first bean. Pin expected records and transfer keys to actual Java invocation.
2. Verify non-consume encoders emit only the first row, ignore trailing serialization-only fields, and still retain all beans' IDs/keys. Verify consume encoders still emit all rows with byte-identical delimiters, fields, access-channel behavior, and correlation data.
3. Add a general-dispatcher loopback regression: Pub/EndTransaction contexts accepted by Java must emit their valid first record instead of being dropped; multi-bean Pub/Recall must not produce extra records. Follow a genuinely invalid selected record with valid work to retain I03 failure isolation. Preserve I01 callback groups, H01 namespace handling, and I02 shutdown/cleanup/shared-runtime behavior.
4. Update the local probe's defect assertions to corrected Java outcomes, then run:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
mvn -o -f compat/java-reference/pom.xml verify
scripts/check-workspace-metadata.py
```

Acceptance requires matching Java's per-type row selection and all-bean transfer keys, retaining the 2,072-test non-regression baseline and previous fixes. Existing live trace scenarios may run last where available; scripted peers cover custom public-context shapes that normal built-in hooks do not generate. No new setting or Java component emulation is needed.
