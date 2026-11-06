# Third parity review: LZ4 stream decoding and live trace identity

## 1. Baseline, scope, and result

Date: 2026-10-27. Reviewed revision: `cbc1bae1fa98e2ef19a33580c3852b77b6ceb1bc`. The latest production fixes are `92940332`, `79cd6caf`, `9d944e78`, and `08255f32`; `cbc1bae1` records their validation. The two existing untracked review-evidence directories were preserved.

The contract remains the user's rule: public API capabilities, input domains, and settings correspond to Java RocketMQ client 5.5.1; execution logic corresponds while Rust may choose its own components. No Java/Netty/JVM component emulation is required. The local pinned client source JAR SHA-256 is `de63ae377bcc9b461b0d65b4cb77673d9daabb8f46caabef903826ea7c627e76`; Java's LZ4 dependency is `org.lz4:lz4-java:1.8.0`.

**Two new findings are confirmed: one P1 and one P2.** Both concern previously uncovered adjacent paths, rather than a reversal of the five concrete fixes from the preceding review. This document owns the new findings; earlier review documents remain dated history. This change adds documents and probe artifacts only.

| ID | Priority | Finding | Evidence |
| --- | --- | --- | --- |
| G01 | P1 | Valid concatenated/skippable LZ4 streams are rejected; a valid Pull record becomes an empty FOUND result | Identical bytes passed through actual Java/Rust compressor and PullAPIWrapper implementations |
| G02 | P2 | Lite consume trace keeps the startup group after the public live group setter changes it | Actual Rust outbound trace SEND rows versus pinned Java poll/trace-hook append calls |

P1 denotes a confirmed message-delivery omission at the client boundary, with offset consequences traced in source. P2 denotes incorrect observable trace attribution. Neither finding depends on matching executor structure or incidental timing.

Scope: the complete latest remediation diff and neighboring Common compression/message decoding, Consumer Pull/Lite/trace, Producer compressor/trace publication, and their runtime/RPC boundaries. Unchanged Protocol/Remoting implementations were not exhaustively re-audited in this round.

## 2. Previous-fix verification and execution evidence

| Previous finding | Verification on this revision |
| --- | --- |
| F01, nullable Lite poll loses a batch | Updated original probe now returns one message with null group and persists offset 1 after delivery. The hook regression also passes. |
| F02, inner decoder limits/inherited state | Updated original probe returns 24 large inner messages; small inner messages inherit WAIT=false. Consumer now uses Common's decoder. Large/mixed/subtype/fallback regressions pass. |
| F03, orderly wait-ACK release | The decrement is removed. Existing regression passes with count 97 retained at threshold 96; the original Java probe retains its one outstanding message. |
| F04, public decompression cap | Updated original probe round-trips 65 MiB. Common's all-codec and Producer snapshot/large-body regressions pass. G01 concerns stream structure, not the removed size cap. |
| F05, startup namespace mutation | Updated original probe observes the wrapped group after failed/successful startup; lifecycle tests pass. G02 concerns later trace attribution. |

`cargo test --workspace --locked` exited 0: **2,059 passed, 0 failed, 0 ignored**, across 94 test-result summaries, including doctests. The [previous-case replay](04-project-parity-third-review-evidence/previous-review-check.json) records the five fixed boundaries. The [new probe results](04-project-parity-third-review-evidence/results.json) retain source/probe hashes, revision, outputs, and execution limits.

Reproduce the new findings with cached Maven/Rust dependencies, a JDK, Maven, and the repository toolchain:

```sh
python3 plan/2026/10/04-project-parity-third-review-evidence/run_probes.py --output /tmp/rocketmq-third-review-results.json
```

The probe builds a temporary crate against current production libraries and reuses existing scripted Lite TCP helpers without editing them. Rust trace rows are captured from real outbound SEND commands to a loopback peer. Java invokes the pinned compressor/PullAPIWrapper; its Lite poll uses reflected running/cache state and captures real trace-hook append calls. No live RocketMQ Broker/NameServer was contacted. The script's assertions intentionally describe current defects and must be updated when converted into corrected-behavior regressions. Full Maven fixture verification, Clippy, Docker/live scenarios, and cross-platform checks were not rerun in this review.

## 3. Findings and improvements

### 3.1 G01 — LZ4 stream support stops at one ordinary frame

**Locations:** [LZ4 parser](../../../crates/rocketmq-classic-common/src/compression.rs#L100), [global end-of-input check](../../../crates/rocketmq-classic-common/src/compression.rs#L232), [compatible batch decoder](../../../crates/rocketmq-classic-common/src/message/decoder.rs#L585), [Pull processing](../../../crates/rocketmq-classic-consumer/src/pull/result.rs#L162). Java source: Common `org/apache/rocketmq/common/compression/Lz4Compressor.java`; dependency source `net/jpountz/lz4/LZ4FrameInputStream.java` at 1.8.0.

Rust parses one LZ4 frame, rejects a nonordinary initial magic, and requires `cursor == frame.len()` after that frame. Java's RocketMQ compressor wraps `LZ4FrameInputStream` in its default multiple-frame mode and reads to the end, including skippable frames. Removing the decompressed-size cap did not correct this input-domain difference.

**Executed cases:**

| Input | Java result | Rust result |
| --- | --- | --- |
| Two individually valid LZ4 frames containing `first-` and `second` | `first-second` | InvalidData: `invalid LZ4 frame` |
| A valid skippable frame with three payload bytes, followed by those frames | `first-second` | InvalidData: `invalid LZ4 frame` |
| The concatenated bytes as the compressed body of one otherwise valid MessageExt record, next offset 1 | FOUND, one message, body `first-second`, next offset 1 | FOUND, zero messages, next offset 1 |

The record uses `COMPRESSED_FLAG` and the LZ4 compression flag; it is encoded without recompressing the already encoded stream. Both wrappers receive identical bytes. This does not claim the default Java producer emits concatenated frames or that the live 5.3.4 Broker generated this case.

**Impact:** Public decompression rejects valid Java-readable streams. In consumer processing, that rejection is converted to the decoder's prefix/stop behavior, hiding the valid record as an empty successful pull. Push/Lite callers retain the Broker's next offset, creating the same omission risk as other false malformed-record classifications. The empty FOUND boundary is executed; durable Broker-offset advancement is source-traced, not tested against a live Broker.

**Implementation:** Parse the LZ4 stream as a sequence of ordinary/skippable frames, matching the pinned Java dependency's accepted framing. Append ordinary-frame output; safely skip declared skippable payloads. Check headers, lengths, content sizes, and checksums at their frame boundaries. Apply an explicit internal output budget cumulatively across the entire stream rather than resetting it for each frame. Rust may use its own parser/library; the observable accepted input and result must correspond to Java. Keep genuinely malformed-stream behavior separate from valid multi-frame handling.

**Acceptance:** Add pinned Java/public Rust comparisons for one frame, two frames, skippable-before/between frames, empty data frames, truncated headers/skip payloads, bad checksums, and invalid following frames. Compare malformed outcomes with Java before fixing expectations. Verify the valid compressed-record case returns one logical message and its complete body; add a Push/Lite offset regression so a legal stream cannot disappear as empty success. Preserve the 65 MiB and explicit internal-bound tests from prior fixes.

### 3.2 G02 — Lite consume traces ignore the live group/context

**Locations:** [startup hook construction](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L812), [captured group field](../../../crates/rocketmq-classic-consumer/src/lite/trace.rs#L116), [after-hook use of the captured group](../../../crates/rocketmq-classic-consumer/src/lite/trace.rs#L143), [poll context normalization](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1134). Java source: client `impl/consumer/DefaultLitePullConsumerImpl.java:606`, `trace/hook/ConsumeMessageTraceHookImpl.java:55,100`.

`LiteConsumeTraceHook` retains a startup `consumer_group` and uses it to build both trace rows. Java's before hook takes the group from the current `ConsumeMessageContext`, strips its namespace for trace output, and the after hook reuses that before-trace state. A later supported group mutation therefore affects Java trace attribution but not Rust's.

**Executed reproduction:** Enable Lite tracing, start with namespace `tenant` and group `initial-group`, then call `set_consumer_group("later-group")` and poll one region-tagged message. Rust delivers the message, but its outbound trace SEND contains `SubBefore` and `SubAfter` rows with group `initial-group`. Pinned Java poll/hooks produce both rows with group `later-group`; the consume context also contains `later-group`. The probe validates trace contents, not a transfer of Broker registrations/subscriptions to a new group.

The related poll code also calls `with_namespace` on a later non-null group before building hook context. Java passes its current facade field directly through `groupName()`. This context rewrapping is source-confirmed; Rust custom-hook context values were not separately observed by the external probe. The trace discrepancy itself is directly observed on the wire.

**Implementation:** Derive consume-trace identity from the actual before-hook context, preserving Java's nullable group and namespace-removal behavior. Retain the resulting before-trace identity for the matching after row; do not reread a changed group after the before stage. Remove the startup-group snapshot as the decision source for per-poll tracing. Pass the current facade group into Lite hook context with Java's setter/start mutation boundaries; leave runtime registration ownership at its own audited capture boundary.

**Acceptance:** Compare before/after trace group and hook context for startup group, later plain group, later namespace-qualified group, and null group, with and without a configured namespace. Both rows of one consume event must retain the Java before-stage identity. Preserve message-ID, topic/namespace, region, TRACE_ON filtering, success, access-channel, and timing semantics. Keep the nullable-poll delivery/commit test passing and verify tracing changes do not move consumer registrations or offset-store ownership.

## 4. Implementation order and verification

1. Fix G01 framing and add its public decompressor plus full-record pull regressions. Extend the private bounded path's tests to cover a cumulative budget across frames.
2. Fix G02's per-consume trace identity and hook-context sourcing; verify real outbound trace rows and the existing live-group/null-poll lifecycle together.
3. After focused Common/Consumer/Producer checks and pinned Java invocation cases, run the existing gates:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
mvn -o -f compat/java-reference/pom.xml verify
scripts/check-workspace-metadata.py
```

The current 2,059 passing tests remain the non-regression baseline. Keep previous fixes and malformed-input contracts covered; validate accepted stream shapes and dynamic identities at their callable/output boundaries instead of relying only on own-compressor round-trips or startup-state assertions.

Environment-dependent validation comes last: rerun affected existing local Pull/Lite and trace scenarios where available and retain revision-linked evidence. Use scripted peers for valid stream shapes the baseline Broker does not generate. These findings do not require owner production credentials/infrastructure; this review does not change CI policy or establish release readiness.
