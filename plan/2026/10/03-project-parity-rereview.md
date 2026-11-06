# Follow-up review: parity fixes and newly discovered gaps

## 1. Baseline and result

Date: 2026-10-27. Reviewed revision: `9b006383fd8c81490c673244d69853b3d8ee89eb`; the production-source delta from the previous review consists of `db941d9f`, `c0960129`, and `7e0e9955`, followed by documentation commit `9b006383`. The pre-existing untracked `02-project-parity-review-evidence/` directory was preserved.

The contract remains: public API capabilities, settings, and input domains correspond to Java RocketMQ client 5.5.1; execution logic corresponds without requiring Java/Netty/JVM components. The local pinned source JAR matches SHA-256 `de63ae377bcc9b461b0d65b4cb77673d9daabb8f46caabef903826ea7c627e76`. Java source paths below refer to `org/apache/rocketmq/client/` within that JAR, except explicitly named Common paths.

**Five newly discovered findings remain: two P1 and three P2.** They include regressions in the remediation and previously uncovered adjacent paths. The previous document is retained as its dated review; this document owns the new findings. Only review documents and local probe artifacts were added; production code and existing tests were not edited.

The follow-up examined the complete remediation diff and neighboring Common, Consumer, Core, Producer, facade, and documentation paths, with execution focused on message decoding, public compressor behavior, Lite state/offset transitions, and POP lifecycle accounting. It is not a new exhaustive proof of every unchanged Protocol/Remoting method.

| ID | Priority | Newly discovered issue | Origin / relation to prior review |
| --- | --- | --- | --- |
| F01 | P1 | A nullable live Lite group causes failed `poll()` to consume a batch and advance its delivered offset | Introduced by the live-group handling in `7e0e9955`; adjacent to R06/R08 |
| F02 | P1 | Consumer's separate inner-batch decoder retains fixed limits and fails to inherit message state | Previously uncovered path adjacent to R01; outer-batch fix does not cover it |
| F03 | P2 | POP orderly no-op incorrectly releases the wait-ACK count | Introduced in `c0960129` while fixing R02 |
| F04 | P2 | Public compressor still rejects valid decompressed output above 64 MiB | Previously uncovered public API adjacent to R01 |
| F05 | P2 | Lite's public group getter does not reflect Java's namespace mutation during `start()` | Newly exposed by the group getter in `7e0e9955`; start-state gap adjacent to R06/R08 |

P1 means confirmed delivery/offset corruption risk; P2 means observable API, state, or flow-control divergence. The priorities do not depend on identical scheduler timing or Java exception text.

## 2. Revalidation of the previous fixes

| Previous finding | Result of this follow-up |
| --- | --- |
| R01, ordinary full-record batches | Reran the updated original Java/Rust probe: all 24 separate 3 MiB messages and the 65 MiB full record are now delivered. Remaining inner-container/public-compressor cases are F02/F04. |
| R02, unexpected POP orderly listener work | The no-listener/no-hook branch is present and its regression passes. Counter state still differs: F03. |
| R03, detached listener task | POP and direct-consume callbacks now use `AbortOnDrop`; the POP cancellation regression passes. |
| R04, multiplied POP shutdown wait | Collective two-phase waiting is present; the worker-count-independent regression passes. |
| R05, Lite initial offsets | Source branches now distinguish stored valid/missing/unavailable values and retry topics; existing focused tests pass. The original Java probe also reran. |
| R06/R08, live settings and nullable groups | Setters and nullable construction are present, and API/lifecycle tests pass. Additional running-poll and namespace transitions reveal F01/F05. |
| R07, retry delay | Configured/default/FLOW_CONTROL branches and paused-time tests are present and pass. |
| R09/R10, TLS guidance and current status | README now describes chain/validity without hostname checking; current status distinguishes historical full results from the three remediation live replays. No new finding on these corrections. |

`cargo test --workspace --locked` completed with exit 0: **2,052 passed, 0 failed, 0 ignored**, across 94 test-result summaries, including doctests. This includes the current POP orderly test that asserts the incorrect zero counter; a passing test with the wrong expected state does not establish Java parity.

The [current probe results](03-project-parity-rereview-evidence/results.json) retain outputs, revision, source/probe hashes, and verification limits. The [previous-case replay](03-project-parity-rereview-evidence/previous-review-check.json) records the ordinary-batch fixes passing. Reproduce the new cases with cached dependencies, the repository Rust toolchain, Maven, and a JDK:

```sh
python3 plan/2026/10/03-project-parity-rereview-evidence/run_probes.py --output /tmp/rocketmq-rereview-results.json
```

The script builds a temporary crate against the actual current libraries, reuses the existing Lite scripted-peer helpers without modifying them, and invokes pinned Java classes. Rust Lite uses loopback TCP peers and a controlled `OffsetStore`; Java's null-poll case injects running/cache state to isolate the method. No live RocketMQ Broker/NameServer was contacted. POP counter evidence combines direct Java invocation with Rust production source and its existing passing assertion; no new public Rust POP-dispatch probe was executed. Full Maven verification, Clippy, Docker/live scenarios, and cross-platform checks were not rerun in this follow-up. Script assertions describe the current defects and must change when promoted into fixed-behavior regressions.

## 3. Findings and required improvements

### 3.1 F01 — Failed Lite poll loses the batch and advances a committable offset

**Locations:** [poll](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1113), [dequeue and delivered-offset update](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1923), [commit_queues](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1314). Java: `impl/consumer/DefaultLitePullConsumerImpl.java:606`, `poll(long)`.

The new `poll()` obtains a batch through `take_ready_batch_with_queue()`, which removes it and advances `delivered_offset`, then requires `get_consumer_group()` to be non-null. A running caller can now use the new `set_consumer_group_nullable(None)` API. Even with no consume hooks installed, this makes `poll()` return `InvalidConsumerGroup` after the messages have already been removed. Java accepts the nullable setter; its poll does not inspect the group unless constructing hook context, and null is representable there as well.

**Executed reproduction:** Start a namespaced Lite consumer with automatic commit disabled, assign one queue, and script a valid message at queue offset 0 with next offset 1. Set the live group to null, then poll. Rust returns `Err(Pull(InvalidConsumerGroup))`. Restore the group and poll with zero timeout: zero messages remain. Calling `commit_queues([queue], true)` persists offset 1 in the controlled store, although the application received no message. Pinned Java poll with a queued message, null group, and no hooks returns one message and consume offset 1.

**Impact:** An error result hides an already consumed batch; explicit commit, later auto-commit, or offset persistence can make that omission durable. Actual commit to the controlled OffsetStore is reproduced; no live Broker persistence is claimed.

**Improvement:** Preserve Java's null acceptance in consume context and avoid performing group-dependent fallible work after destructive dequeue. Construct hook context only where applicable, keeping nullable state. Audit other errors between dequeue and successful delivery so an error cannot silently convert fetched work into delivered work. Moving the same erroneous validation before dequeue would prevent loss but would still leave a public behavior mismatch.

**Acceptance:** Exercise actual running `poll()`, with and without hooks, after null/valid group mutations. Null must deliver the message as Java does; hook group must preserve null. No error path may advance or persist an undelivered batch. Retain normal namespace reset, auto-commit timing, and the fetched/delivered offset distinction. The external reproduction must return one message rather than an error and an empty recovery poll.

### 3.2 F02 — A duplicate inner-batch decoder still changes valid delivery and state

**Locations:** [unwrap call and swallowed failure](../../../crates/rocketmq-classic-consumer/src/pull/result.rs#L184), [consumer inner decoder](../../../crates/rocketmq-classic-consumer/src/pull/result.rs#L356), [shared Common implementation](../../../crates/rocketmq-classic-common/src/message/compact.rs#L181). Java: `impl/consumer/PullAPIWrapper.java:77`; `common/org/apache/rocketmq/common/message/MessageDecoder.java:761`, `decodeMessage(MessageExt,List)`.

The outer decoder now accepts large valid batches, but Consumer retains a separate compact parser with 16 MiB per-record and 64 MiB aggregate limits. If unwrapping fails, `process_pull_result` keeps the original outer messages and returns FOUND. The same private parser also builds new `MessageExt` values without inheriting the outer `WAIT` setting. Common already exports a Java-shaped `decode_inner_batch` that parses by field lengths, inherits `is_wait_store_msg_ok`, and marks client subtypes.

**Executed comparison using identical encoded records:**

| Input | Java PullAPIWrapper | Rust PullAPIWrapper |
| --- | --- | --- |
| One compressed inner container containing 24 compact 3 MiB bodies, both `INNER_BATCH_FLAG` and `NEED_UNWRAP_FLAG` set | 24 logical messages, next offset 24 | One packed outer message, next offset 24 |
| Two small compact messages; outer `WAIT=false`, inner records omit WAIT | Two messages, inherited WAIT=false | Two messages, WAIT=true |

These are Java-readable offline records. The review did not verify that an unmodified 5.3.4 Broker emits the large-container case under its default configuration. The small inherited-state case does not need a large message.

**Impact:** Applications receive a packed envelope instead of logical messages, while callers retain an offset beyond the container's logical contents. Inner tag filtering can also operate on the wrong envelope. The smaller case changes an exposed message property even when unwrapping succeeds.

**Improvement:** Reuse the shared Common Java-compatible inner decoder, retaining Java's all-or-original behavior for genuinely malformed compact data. Remove consumer-only rejection of valid compact records and preserve inherited WAIT/client-subtype semantics. A different internal Rust parser is acceptable only if it produces the same logical messages and inherited state; a second divergent compatibility implementation is unnecessary.

**Acceptance:** Add public PullAPIWrapper comparisons for the two inputs above, plus a single compact body over 16 MiB, mixed ordinary/inner records, offsets, tags, IDs/client subtype, and outer WAIT true/false. Preserve the existing malformed-inner fallback separately. Extend Push/Lite tests to verify logical message delivery and committed offsets rather than only outer decode counts.

### 3.3 F03 — POP orderly no-op releases unacknowledged work

**Locations:** [orderly early return](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L5481), [flow-control read](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L5170), [existing regression expectation](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L9257). Java: `impl/consumer/ConsumeMessagePopOrderlyService.java:200,354`; `impl/consumer/DefaultMQPushConsumerImpl.java:524,564`.

The remediation correctly skips the orderly listener, but subtracts `messages.len()` from the POP queue's wait-ACK count before returning. Java's no-op request does not acknowledge or release that count. A direct Java submission with count 1 ends with zero listener calls and count 1. Rust's existing passing test explicitly requires count 0. The original remediation probe already records Java count 1, so its outcome and the Rust assertion disagree.

**Impact:** Running-info state and the `popThresholdForQueue` admission branch differ. Java accumulates outstanding work until the threshold blocks further POP; Rust frees the allowance despite emitting no ACK, permitting additional POP/redelivery traffic. This is a state/control-flow difference, not a demand to copy Java's executor.

**Improvement / acceptance:** Keep the outstanding count on the pinned orderly no-op path, while preserving zero listener/hook calls and zero listener-result ACK/visibility requests. Change the incorrect regression expectation to the Java outcome. Add a threshold case that starts at 96, adds one outstanding message, and remains at 97 with threshold 96, so another POP is not admitted. Verify concurrent consume, expiry handling, and real ACK paths still release counts at their corresponding lifecycle events.

### 3.4 F04 — Public compressor retains an undocumented 64 MiB limit

**Locations:** [public Common decompression](../../../crates/rocketmq-classic-common/src/compression.rs#L33), [public ProducerCompressor](../../../crates/rocketmq-classic-producer/src/compressor.rs#L31). Java: `common/org/apache/rocketmq/common/compression/ZlibCompressor.java`, `decompress(byte[])`.

The previous fix bypasses fixed limits in the message-record decoder, but `Producer::compressor().decompress()` still delegates to `compression::decompress`, which hard-caps output at 64 MiB. This is the public counterpart of Java's compressor API, not merely a private safety helper.

**Executed reproduction:** Compress 65 MiB through the producer's default compressor, then decompress those bytes. Java returns 68,157,440 bytes; Rust returns `InvalidData: decompressed message body exceeds configured limit`. Thus the public API cannot round-trip bytes produced by its own compression method. The standalone operation is independent of Producer send-size validation and Broker settings.

**Improvement / acceptance:** Give public compatibility decompression Java's accepted output range; retain explicit bounded internal helpers only at intended internal call sites. Do not add a Rust-only public setting to substitute for the missing Java capability. Add a 65 MiB public round-trip and pinned Java differential; broaden to the supported Zlib/LZ4/Zstd paths and malformed compressed data. Preserve earlier compressor snapshots after changing the producer compression type.

### 3.5 F05 — Lite group getter misses namespace mutation at startup

**Locations:** [live group getter](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L286), [startup namespace calculation](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L735). Java: `consumer/DefaultLitePullConsumer.java:233`, `start()`.

Java wraps `consumerGroup` with the namespace and writes it back to the facade before calling implementation startup. Rust computes `effective_group` for validation and prepares a namespaced Pull engine, but never writes that value into its new live group field. Public getter state and runtime group identity therefore diverge.

**Executed reproduction:** Namespace `tenant`, group `invalid-thread-group`, pull thread count `-1`. Java startup fails locally with `IllegalArgumentException`, but its getter already returns `tenant%invalid-thread-group`. Rust returns `InvalidPullThreadCount` and its getter still returns `invalid-thread-group`. The scripted successful-start case likewise reports raw `review-null-poll` while its requests/store use the namespaced group. The difference is the retained group value, not the language-specific error class.

**Improvement / acceptance:** Persist the namespace-wrapped group at the Java facade startup boundary, preserving null/empty values and namespace idempotence. Keep constructor-captured runtime ownership separate from the facade's mutable field where Java does so; do not normalize arbitrary later setter inputs differently from Java. Test getter state before start, after successful start, after startup validation failure, after repeated start, with no namespace/already-wrapped/null/empty groups, and after a later setter.

## 4. Implementation order and verification

1. Fix F01 before other Lite changes; add an end-to-end scripted poll/commit regression using the new public nullable setter. Verify delivery and persisted offsets together.
2. Consolidate the inner decoder for F02 and fix public decompression for F04. Keep ordinary full-record, compact inner-record, and public-compressor tests distinct so one passing boundary cannot mask another.
3. Fix F03's counter lifecycle and its existing wrong expectation; compare Java no-op counters and flow-control decisions, retaining the listener/cancellation/shutdown fixes.
4. Fix F05's public startup mutation and test it alongside F01, including failed starts. Update current status only after recording the revision that passes these acceptance cases.

For each change, run focused Common/Consumer/Producer tests and pinned Java cases. At completion, run the existing gates:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
mvn -o -f compat/java-reference/pom.xml verify
scripts/check-workspace-metadata.py
```

The 2,052 passing tests are a non-regression baseline. Preserve existing cases, correct assertions proven inconsistent with the oracle, and add real callable-boundary tests rather than only checking helper return values or getter/setter presence.

Place environment-dependent validation last: rerun affected existing local 5.3.4 Lite/Pull/POP scenarios and retain revision-linked output. Cover large-container and nullable-state branches with scripted peers where the Broker baseline cannot generate the input. Owner-only production scenarios remain owner-executed when actually required; these five local findings do not depend on production credentials or infrastructure. This review does not change CI policy or claim release approval.
