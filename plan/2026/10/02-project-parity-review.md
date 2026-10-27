# Project review: Java 5.5.1 parity and improvement plan

## 1. Contract, baseline, and conclusion

Review date: 2026-10-27. Rust revision: `a91e91bcdcfe92b01a351c56c7ce0dd4ff393e96`; the worktree was clean before this review. This change adds review documents and reproducible probes only.

The contract is the user's rule: public API capabilities, input domains, and settings must correspond to Java; other behavior must preserve the execution logic. Tokio, rustls, Rust collections, futures, and ownership are valid implementation choices. Netty objects, JVM thread identity, and Java exception/stack formatting are not requirements by themselves.

The reference is the locally cached `org.apache.rocketmq:rocketmq-client:5.5.1` source/binary and its Common/Remoting dependencies, rather than the active Java checkout. Client source JAR SHA-256: `de63ae377bcc9b461b0d65b4cb77673d9daabb8f46caabef903826ea7c627e76`, matching the [oracle provenance](../../../compat/java-reference/README.md). Below, `J:` identifies a path inside that client source JAR under `org/apache/rocketmq/client/`; Common source paths are written explicitly.

**Ten actionable findings remain: five P1 and five P2.** The strongest reproduced defect is silent truncation of a valid compressed pull batch: Java delivers 24 messages, while Rust delivers 21 and preserves `nextBeginOffset=24`. Existing audit closure and passing tests do not cover every execution branch. This review does not establish release readiness.

P1 means possible message omission, unexpected user-code execution, work escaping shutdown, or materially incorrect security guidance. P2 means a public API/settings, retry, shutdown-budget, or evidence-maintenance defect. Source-confirmed findings are distinguished from executed reproductions; no live Broker or production workload was exercised.

## 2. Coverage and verification

| Area | Inspected boundaries and evidence | Result / limit |
| --- | --- | --- |
| Common | Message representation, full/compact encoding and decoding, compression, limits, existing fixtures | R01; D02 |
| Protocol | Typed header parsing/checking, signed Java numbers, DTO/route mappings and inventories | No additional confirmed defect in the inspected slices; not an exhaustive proof for every DTO/input |
| Remoting | Frame limits, pending-request cancellation, connection/GO_AWAY ownership, callbacks, TLS verification and inventories | R09 is documentation; selected runtime differences already have explicit dispositions |
| Core | RPC route fallback, shared runtime/config, route publication, offset RPCs, admin fan-out, ACL/trace boundaries | Checked downstream effects of R01 and consumer configuration; no separate confirmed Core defect |
| Producer | Send retry/deadline paths, default-route lookup, message preparation, transactions, request/reply, trace | No additional confirmed defect in the inspected paths |
| Consumer | Pull/Push/Lite, offsets, assignment/rebalance, concurrent/orderly/POP dispatch and shutdown | R01–R08; D01 |
| Facade, tests, release records | Re-exports, API crosswalk tests, integration/live records, README and compatibility status | R06/R08 API accessibility; R10 evidence consistency |

This was a project-wide, risk-oriented source review with targeted deep dives, not a claim that every line or overload was exhaustively proved equivalent. Public API presence checks, field-copy tests, and historical live summaries were treated as evidence with limits, not substitutes for behavioral verification.

Executed on this revision:

- `cargo test --workspace --locked`: exit 0; 94 test-result summaries, **2,032 passed, 0 failed, 0 ignored**, including doctests. Counts sum the reported harness results.
- Offline Java/Rust probes: exit 0. Identical Rust-encoded bytes were decoded through pinned Java `MessageDecoder.decodesBatch`; Rust used the actual `PullAPIWrapper::process_pull_result`.
- Java Lite initial-offset calls used controlled `OffsetStore` and `MQAdminImpl` implementations. Nullable Pull/Lite group constructors were invoked directly. Rust comparisons for these cases are source-based.
- POP lifecycle findings are source-confirmed; this review did not run a new Java/Rust POP lifecycle differential. Full Maven fixture regeneration, Docker/live scenarios, stress tests, Clippy, and cross-platform runs were not rerun. Existing historical passes are not relabeled as failures.

The [probe results](02-project-parity-review-evidence/results.json) retain the revision, hashes, outputs, and verification boundaries. Reproduce with locally cached Maven/Rust dependencies, a JDK, Maven, and the repository Rust toolchain:

```sh
python3 plan/2026/10/02-project-parity-review-evidence/run_probes.py --output /tmp/rocketmq-parity-review-results.json
```

The script copies the workspace lock into an external temporary crate, uses offline dependency resolution, contacts no Broker, and leaves production sources and the workspace lock unchanged. Its assertion intentionally detects the current R01 defect; after fixing R01, replace that assertion with the corrected expectation when promoting the probe into regression coverage.

## 3. Findings

| ID | Priority | Finding | Evidence |
| --- | --- | --- | --- |
| R01 | P1 | Rust-only decode limits silently discard valid messages while keeping the Broker's next offset | Java/Rust executed reproduction plus caller trace |
| R02 | P1 | POP orderly dispatch invokes and acknowledges user work absent from the pinned Java path | Source-confirmed |
| R03 | P1 | POP listener child tasks escape cancellation of their owning worker | Source-confirmed |
| R04 | P2 | POP shutdown grants the full wait budget separately to every worker | Source-confirmed |
| R05 | P1 | Lite initial-offset policy changes retry-topic and negative-offset branches | Java invoked; Rust source comparison |
| R06 | P2 | Some Java live settings become inaccessible/frozen after Rust facade construction | Source-confirmed public API/read-site comparison |
| R07 | P2 | Lite pull ignores configured exception delay and Broker FLOW_CONTROL classification | Source-confirmed |
| R08 | P2 | Pull/Lite group APIs cannot represent Java's null input | Java invoked; Rust signature comparison |
| R09 | P1 | README promises TLS hostname verification that the implementation deliberately does not perform | Source/test/document comparison |
| R10 | P2 | Current compatibility status conflicts across user-facing and audit documents | Document-confirmed |

### 3.1 R01 — Silent omission at decode limits

**Locations:** [DecodeLimits and prefix decoder](../../../crates/rocketmq-classic-common/src/message/decoder.rs#L203), [PullAPIWrapper](../../../crates/rocketmq-classic-consumer/src/pull/result.rs#L162), [Push offset progression](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L3321), [Lite acceptance](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1672). Java: `common/org/apache/rocketmq/common/message/MessageDecoder.java`, `decodesBatch`; `J:impl/consumer/PullAPIWrapper.java`, `processPullResult`.

Rust fixes per-record bytes at 16 MiB, decompressed body at 64 MiB, and aggregate decoded data at 64 MiB. `decode_many_compatible` discards the error returned by `decode_many_prefix_with_limits`; the pull wrapper returns FOUND with a decoded prefix and unchanged `next_begin_offset`. Java's malformed-record prefix handling does not justify rejecting valid records at additional Rust-only limits.

**Reproduction:** 24 records with 3 MiB bodies, individually below the default Producer 4 MiB body limit, compress to a combined 76,032 bytes. Java decodes 24; Rust returns FOUND with 21 and next offset 24. Separately, a 65 MiB body compresses to 66,312 bytes; Java decodes it, while Rust returns FOUND with zero messages and next offset 1. The second case requires a raised sender body-size setting; the first does not.

**Impact:** Push advances its pull request before dispatch and can later persist an offset beyond the omitted messages; Lite advances fetched position, and explicit Pull returns an incomplete success. This is confirmed at the decode/wrapper boundary and source-traced through offset progression, not an executed Broker persistence test. POP/admin also call the same lossy decoder; their failure consequences differ and need dedicated regressions.

**Improvement / acceptance:** Remove undocumented fixed-limit rejection of valid Java-compatible batches, or restructure decoding/delivery to preserve all records. Any retained internal resource guard must propagate a distinct failure and prevent offset advancement/ACK of unprocessed records; malformed Java-input behavior must remain separately covered. Do not introduce an unrelated public configuration key merely to hide this mismatch. The 24-record reproduction must deliver 24; add Push/Lite offset and POP/query coverage proving that no valid suffix disappears as success.

### 3.2 R02 — POP orderly execution differs from Java

**Locations:** [POP dispatch](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L5375), [listener handling](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L5425). Java: `J:impl/consumer/ConsumeMessagePopOrderlyService.java:200,354`.

Pinned Java `submitPopConsumeRequest` creates a request without retaining the message list. `ConsumeRequest.run` checks dropped state and obtains a sharding-lock object, then ends; it never invokes the orderly listener or ACKs these messages. Rust splits the list into batches, spawns workers, invokes the orderly listener, and ACKs success. Those workers also lack the normal orderly path's per-process-queue consume gate. This is observable additional execution, not merely a different executor implementation.

**Improvement / acceptance:** Restore the pinned Java control flow on the compatibility path; do not present a functional POP-orderly extension as Java 5.5.1 parity. Add a direct differential for a non-dropped POP queue asserting listener-call count, hooks, and emitted ACK/visibility requests. The pinned path must have zero listener calls and no listener-result ACKs. Functional POP-orderly behavior, if desired, needs a separately documented product decision and ordering contract.

### 3.3 R03 — POP listener futures survive worker shutdown

**Locations:** [POP callback spawns](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L5464), [POP worker shutdown](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L7096). Compare [normal concurrent callback ownership](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L3670).

POP creates bare `tokio::spawn` listener handles and awaits them. Shutdown aborts only the outer handles in `pop_consume_workers`; dropping a Tokio JoinHandle detaches its child. A cooperative listener can therefore continue producing application side effects after shutdown while its completion/ACK owner is gone. The normal Pull concurrent/orderly paths already wrap listener handles in `AbortOnDrop`.

**Improvement / acceptance:** Apply the existing owned-child pattern to POP and audit direct-consume callbacks for the same ownership boundary. A blocked async listener with a drop sentinel must be cancelled/dropped when its worker is aborted; releasing its barrier after shutdown must not produce a late side effect or ACK. Preserve normal panic/result classification and completed-work handling. Do not promise forcible termination of arbitrary blocking application code.

### 3.4 R04 — POP shutdown wait grows with worker count

**Location:** [shutdown worker loop](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L7102). Java: `J:impl/consumer/ConsumeMessagePopConcurrentlyService.java:87`; `common/org/apache/rocketmq/common/utils/ThreadUtils.java:185`.

Rust applies `timeout(await_termination_millis, worker)` afresh inside a sequential loop. With N permanently pending workers, the POP wait can approach N × T before other shutdown cleanup. Java awaits the executor as a whole for T, then requests cancellation and awaits it as a whole for another T. This is not the same bounded service-level flow.

**Improvement / acceptance:** Establish phase deadlines once for the POP service and wait/cancel its workers collectively. Account for Java's graceful and cancellation phases; do not renew the allowance per worker. With paused time, compare 1 and 20 stalled workers at T=100 ms: POP waiting must be bounded by the chosen service phases (at most 2T plus bounded cleanup), independent of N. Verify alongside R03 so detached listeners cannot make the apparent bound misleading.

### 3.5 R05 — Lite initial-offset branches differ

**Location:** [initial_offset](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1641). Java: `J:impl/consumer/RebalanceLitePullImpl.java:89`.

| Input after offset lookup | Java | Rust |
| --- | --- | --- |
| LAST, missing offset `-1`, `%RETRY%…` topic | Start at 0 | Query MAX_OFFSET |
| TIMESTAMP, missing offset, retry topic | Query MAX_OFFSET directly | Query SEARCH_OFFSET; fall back to MAX only for a negative result |
| TIMESTAMP, regular topic, SEARCH_OFFSET returns `-1` | Preserve `-1`; do not start a valid pull from it | Replace it with MAX_OFFSET |
| Supported policy, custom store returns a value below `-1` | Return invalid/unavailable `-1` | Treat it as missing and apply the initialization policy |

The Java probe confirms retry LAST=0, unavailable stored offsets remaining invalid, and regular TIMESTAMP search `-1` remaining `-1`. Retry TIMESTAMP request selection is also explicit in pinned source. Rust's fallback can skip retry history or start at the end when Java would retry initialization.

**Improvement / acceptance:** Implement the supported policies with separate stored-valid, missing (`-1`), and unavailable (`< -1`) branches and Java's retry-topic special cases. Preserve negative successful search results. Add a Lite-specific invoked matrix with request counters for MAX versus SEARCH, all three supported policies, retry/regular topics, stored `>=0`/`-1`/`-2`, RPC failures, and negative search results. Existing Push initial-offset fixtures cannot stand in for Lite.

### 3.6 R06 — Live settings lack a callable Rust mutation path

**Locations:** [Lite config setters](../../../crates/rocketmq-classic-consumer/src/lite/config.rs#L289), [Lite read-only config](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L204), [Lite initial-offset live read](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1641), [Pull config](../../../crates/rocketmq-classic-consumer/src/config.rs#L98), [Pull timeout read](../../../crates/rocketmq-classic-consumer/src/pull/consumer.rs#L1450). Java: `J:consumer/DefaultLitePullConsumer.java:575,588`; `J:impl/consumer/RebalanceLitePullImpl.java:90`; `J:consumer/DefaultMQPullConsumer.java:212,228,236`.

Lite's `consume_from_where` and `consume_timestamp` setters require mutable access to a config moved into the consumer; `config()` returns only `&LitePullConsumerConfig`, and there are no equivalent consumer setters. Java reads these values when initializing a newly assigned queue, so changing them while running can affect later queues. Pull's suspend/pull timeout settings have the same inaccessible ownership shape even though subsequent pulls read them. Having setter names on a pre-construction builder does not preserve these live capabilities.

**Improvement / acceptance:** Provide synchronized facade setters or a retained live config handle, using Java's actual read/capture boundaries. Do not dynamically rebuild constructor-captured executors as a side effect. Add external compile-and-run cases that mutate after facade construction; while running, change Lite timestamp/policy before adding a queue and Pull timeouts before the next call, then compare requests with Java. Inventory tests must verify receiver accessibility and runtime effects, not only method names or copied fields.

### 3.7 R07 — Lite exception retry settings are ignored

**Location:** [pull_loop](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1541), particularly its unconditional one-second sleep on pull errors. Java: `J:impl/consumer/DefaultLitePullConsumerImpl.java:168,1002`.

Java captures the inherited `pullTimeDelayMillsWhenException` during implementation construction and uses it for ordinary pull failures; Broker FLOW_CONTROL uses a separate 20 ms branch. Rust forwards ClientConfig into its Pull engine but its Lite worker sleeps one second for every error. Setting the public delay to 4,500 ms has no corresponding worker effect, and flow control receives the wrong retry category.

**Improvement / acceptance:** Retain the configured Java delay at the corresponding boundary and classify the typed nested Broker error before rescheduling. Use paused-time scripted tests for ordinary failure at 4,500 ms, default failure at 1,000 ms, and FLOW_CONTROL at 20 ms, with cancellation and successful immediate retry paths. Allow scheduler tolerance; the requirement is the configured branch/budget, not identical thread timing.

### 3.8 R08 — Nullable Pull/Lite group inputs are missing

**Locations:** [Pull group constructor/setter](../../../crates/rocketmq-classic-consumer/src/config.rs#L47), [Lite group constructor/setter](../../../crates/rocketmq-classic-consumer/src/lite/config.rs#L82). Compare [Push nullable setter](../../../crates/rocketmq-classic-consumer/src/push/config.rs#L197). Java: `J:consumer/DefaultMQPullConsumer.java`, `J:consumer/DefaultLitePullConsumer.java`, constructors and `setConsumerGroup`.

Both Java facades accept a null String group and retain it before startup validation. The probe constructs each with `(String)null` and observes a null getter. Rust only accepts `impl Into<String>` for Pull/Lite groups; neither a nullable constructor nor a nullable setter is available. Push and Producer already model this input with `Option<String>`.

**Improvement / acceptance:** Add an explicit nullable Rust adapter using the existing project convention, preserving the common non-null convenience API where practical. Keep null distinct from an empty string and retain validation at Java's corresponding lifecycle step. Invoke constructor, getter/setter, namespace/start, and failure-state cases for null, empty, default, and valid groups; do not equate Java null with a Rust empty string.

### 3.9 R09 — TLS guidance promises a nonexistent hostname check

**Locations:** [README transport guidance](../../../README.md), [TlsConfig contract](../../../crates/rocketmq-classic-remoting/src/client/tls.rs#L270), [ChainOnlyServerVerifier](../../../crates/rocketmq-classic-remoting/src/client/tls.rs#L857), [existing TLS test](../../../crates/rocketmq-classic-remoting/tests/transport.rs#L2387).

README states that `test_mode: false, auth_server: true` enables CA and hostname checks. The implementation intentionally ignores `_server_name` and verifies chain/validity only, matching Java 5.5.1's lack of endpoint identification. The test and Known Differences document correctly reflect acceptance of a trusted wrong-host certificate. A user relying on README would assume a stronger authentication guarantee than the code provides.

**Improvement / acceptance:** Correct README and all usage/security guidance to distinguish chain/validity verification from endpoint identification, and link the existing tested Java disposition. Preserve the Java-compatible default behavior; adding hostname enforcement is not the fix for this documentation defect. Search all TLS guidance for the contradictory claim and keep the trusted wrong-host/untrusted-CA test expectations explicit.

### 3.10 R10 — Current status and historical evidence are conflated

**Locations:** [README](../../../README.md), [Known Differences](../../../docs/compatibility/KNOWN_DIFFERENCES.md), [Phase 9 matrix](../../../docs/compatibility/PHASE_9_OFFLINE_METHOD_MATRIX.md), [Phase 13 matrix](../../../docs/compatibility/PHASE_13_LITE_POP_MATRIX.md), [release checklist](../../../docs/release-checklist.md), [current-tree audit](../../../docs/compatibility/evidence/owner/MAN-05/current-tree-audit-20261026.md).

README still reports P18-V01 partial, P18-V04 open/current Ubuntu CI unverified, and aggregate local gates open. Later Known Differences/current-tree records say the broad audit closed, the local aggregate passed, all 37 scenarios have recorded passes, and CI was removed as a requirement. Some phase matrices still label their live gates pending. Historical records also explicitly state that raw live outputs were not retained; those summaries cannot be independently rechecked here.

**Improvement / acceptance:** Publish one concise current status keyed to source revision, oracle version, scenario result, and evidence location; link to dated history without rewriting historical facts. Reconcile README/matrix current summaries and mark old pending rows as historical or superseded. Add R01–R09 as new open findings rather than claiming previous closure proves the current tree defect-free. Retain reproducible raw evidence/checksums on future reruns. This does not require restoring GitHub Actions or rerunning owner-only production scenarios as part of the review.

## 4. Design follow-ups, not confirmed parity defects

### 4.1 D01 — Check bounded consume admission under backlog

[ConcurrentConsumeService](../../../crates/rocketmq-classic-consumer/src/consume/concurrent.rs#L20) adds a fixed 256-entry channel; full submission requeues the batch and schedules a delayed retry in [PushConsumer](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L3434). Java uses an unbounded `LinkedBlockingQueue`. A bounded Rust queue is a valid component choice if it preserves required delivery/progress behavior. This review confirms the extra admission branch, but has not demonstrated message loss or an unacceptable ordering/progress change from it.

Follow up with blocked listeners and more than 256 ready batches: verify no lost/duplicate in-flight batch, eventual progress, fair queue participation, and bounded retry-task growth. Adjust pending-work/admission mechanics only if this evidence shows a flow defect; do not copy Java's executor merely for structural similarity.

### 4.2 D02 — Deliberately reproduced malformed-input loops deserve isolation

[Common count_inner_messages](../../../crates/rocketmq-classic-common/src/message/compact.rs#L128) can loop indefinitely for zero-size or cyclic negative-size input; Known Differences records that tests intentionally reproduce Java's behavior. This is **not a Java/Rust mismatch**, and no production call site beyond the exposed helper was found in the inspected tree. It is a design/hardening concern for direct callers handling untrusted bytes.

Consider a checked internal parser with forward-progress detection, while making any change to the public malformed-input contract explicit. Keep bounded subprocess probes for the legacy behavior; do not run these inputs in the test runner or Tokio worker directly. Prioritize delivery and lifecycle defects above reproducing further JVM-specific malformed-input pathologies.

## 5. Implementation order and acceptance gates

1. **Message integrity:** R01, then R05. Add the smallest failing behavioral regressions before changing the code. Check full decode delivery and the absence of offset advancement on a guard/error path.
2. **POP control flow/lifecycle:** R02, R03, R04. Establish the pinned orderly branch, owned listener cancellation, and collective shutdown budgets; test concurrent POP separately so fixing the orderly branch cannot hide R03/R04.
3. **Public settings and inputs:** R06, R07, R08. Build a callable mapping with type/nullability, allowed lifecycle, Java read boundary, Rust mutation path, and observed effect. Cover execution rather than expanding source-name scans.
4. **Documentation and evidence:** R09 can be corrected independently; finish R10 after recording the final implementation revision/results. Keep D01/D02 separate from release-blocking defect counts until their scope is decided or a behavioral failure is reproduced.

Run focused Common/Consumer/Remoting regressions per change. After the implementation phases, use the existing workspace gates:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
mvn -o -f compat/java-reference/pom.xml verify
scripts/check-workspace-metadata.py
```

Offline Maven needs the pinned dependencies already cached. Preserve existing tests; supplement them with real public API calls, exact request/outcome comparisons, and offset/lifecycle assertions. The current 2,032 passing tests remain a non-regression baseline, not evidence that these new scenarios pass.

Final environment-dependent verification belongs to the last implementation phase: run the affected existing local 5.3.4 pull/Lite/POP scenarios and retain raw revision-linked evidence. Supplement scripted compressed/offset cases where the baseline Broker cannot expose a branch. Production network/security/HA/workload checks remain owner-executed only where those environments are actually required; source-reproducible findings above do not depend on those inputs.

A finding closes only when its stated behavior/acceptance case passes against the pinned oracle where applicable, the callable API/settings remain compatible, and the current status cites the tested revision. No replacement of Tokio/rustls with Java-like components is required.
