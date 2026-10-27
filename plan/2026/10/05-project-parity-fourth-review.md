# Fourth parity review: trace namespace semantics

## 1. Baseline, scope, and result

Date: 2026-10-27. Reviewed revision: `bee48c30a59660accfdb05676756b8d8668408aa`. This round reviews the complete third-review remediation diff and adjacent compression, message decoding, Pull/Lite delivery, consume hooks, and Consumer/Producer trace paths. Unchanged Protocol/Remoting behavior was not exhaustively re-audited.

The user's contract remains public API/input/settings parity and execution-logic parity with Java RocketMQ client 5.5.1, allowing Rust-native components. The pinned client source JAR SHA-256 is `de63ae377bcc9b461b0d65b4cb77673d9daabb8f46caabef903826ea7c627e76`.

**One new P2 finding is confirmed: H01, trace namespace normalization differs from Java.** It affects trace attribution for legal names containing `%`; no new delivery or offset defect was confirmed. Earlier documents remain historical records. This review changes documents and probe artifacts only.

## 2. Previous-fix verification and execution evidence

| Check | Result on this revision |
| --- | --- |
| G01, LZ4 streams | Replayed public decompression and Pull probes: single/concatenated/skippable/empty frames succeed; malformed headers, skip payloads, checksum, and following-frame cases fail on both Java and Rust. Pull delivers one `first-second` message at next offset 1. |
| G01, delivery/commit regression | The new scripted Lite test passes, delivering the concatenated-frame record before persisting offset 1. Source inspection confirms cumulative output limits and per-frame checks. |
| G02, live group | The prior probe now emits `later-group` in both Rust outbound trace rows, matching Java. Live facade group and nullable/startup hook tests pass. H01 concerns previously uncovered namespace input shapes. |
| Workspace | `cargo test --workspace --offline` exits 0: **2,067 passed, 0 failed, 0 ignored**, across 94 result summaries, including doctests. |

[Previous-case replay](05-project-parity-fourth-review-evidence/previous-review-check.json), [new differential results](05-project-parity-fourth-review-evidence/results.json), and [test summary](05-project-parity-fourth-review-evidence/workspace-test-summary.json) retain revision, source/probe hashes, outcomes, and execution boundaries. Existing evidence directories were preserved.

Reproduce H01 using cached dependencies, JDK, Maven, and the repository toolchain:

```sh
python3 plan/2026/10/05-project-parity-fourth-review-evidence/run_probes.py --output /tmp/rocketmq-fourth-review-results.json
```

Rust uses current production libraries and scripted loopback NameServer/Broker peers; the probe reads actual outbound trace SEND rows. Java invokes actual Lite poll/trace hooks with reflected running/cache state, and `Validators.checkGroup` accepts every tested group. Each poll delivers one message. No live RocketMQ service was contacted. Full Maven fixture verification, Clippy, Docker/live scenarios, and cross-platform checks were not rerun in this review.

## 3. H01 — Trace namespace removal uses the wrong overload and stage

**Priority: P2.** Incorrect trace group/topic attribution can split searches and monitoring between names that Java normalizes differently.

**Rust locations:** Lite [before snapshot](../../../crates/rocketmq-classic-consumer/src/lite/trace.rs#L138), [context construction](../../../crates/rocketmq-classic-consumer/src/lite/trace.rs#L187), and [namespace helper](../../../crates/rocketmq-classic-consumer/src/lite/trace.rs#L264); general Consumer [trace helper](../../../crates/rocketmq-classic-consumer/src/consume/trace.rs#L146); Producer [trace helper](../../../crates/rocketmq-classic-producer/src/trace.rs#L363). The existing [NamespaceUtil](../../../crates/rocketmq-classic-protocol/src/namespace.rs#L33) already provides both Java-compatible operations.

**Pinned Java source:** client `trace/hook/ConsumeMessageTraceHookImpl.java:55,68,100`, `SendMessageTraceHookImpl.java:57,60`, `EndTransactionTraceHookImpl.java:60,63`, and `DefaultRecallMessageTraceHook.java:63,64`; remoting `protocol/NamespaceUtil.java`, both `withoutNamespace` overloads.

All three Rust trace helpers delegate to `ClientConfig::without_namespace`, which removes only the configured namespace. Java trace hooks call the one-argument `NamespaceUtil.withoutNamespace`, removing a resource's namespace independently of client configuration. Thus `other%later-group` remains unchanged in Rust when the configured namespace is `tenant` or absent; Java emits `later-group`.

Lite has an additional stage mismatch: its before callback removes a namespace, then `consume_contexts` removes it again and assigns that same result to both rows. Java normalizes the live group once for `SubBefore`, then normalizes the stored before group once for `SubAfter`. Those rows can legitimately differ for nested names.

**Executed Lite matrix** (`before / after`):

| Configured namespace | Live group after startup | Java trace group | Rust trace group |
| --- | --- | --- | --- |
| `tenant` | `later-group` | `later-group / later-group` | `later-group / later-group` |
| `tenant` | `other%later-group` | `later-group / later-group` | `other%later-group / other%later-group` |
| absent | `other%later-group` | `later-group / later-group` | `other%later-group / other%later-group` |
| `tenant` | `tenant%tenant%later-group` | `tenant%later-group / later-group` | `later-group / later-group` |

The facade setter and Java consume context retain the supplied live group; normalization belongs to tracing. `%` is allowed by Java group validation. This is an accepted-input/output mismatch, independent of executor or library choices.

**Related paths confirmed by source:** general Consumer trace applies its configured-namespace helper to before/after groups and bean topics. Producer Pub, EndTransaction, and Recall trace apply the same restricted operation to group/topic fields. Their Java counterparts use the unrestricted operation. These related paths were inspected in source; this round's executed wire comparison covers Lite only.

**Correction to the previous acceptance wording:** preserving before-stage identity means sourcing after state from the before snapshot, not forcing identical before/after strings for every valid name. The previous plain/one-prefix test cases masked Java's second normalization. Match the stage-specific Java results above while retaining the fixed live-group capture boundary.

## 4. Implementation and acceptance

1. Use the existing `NamespaceUtil::without_namespace` operation at the Consumer/Lite/Producer trace call sites corresponding to Java's one-argument overload. Cover group and topic fields, including Pub, consume, EndTransaction, and Recall. Keep `ClientConfig::without_namespace` and namespace-sensitive facade/RPC transformations unchanged; they have a different contract.
2. In Lite, capture the once-normalized nullable before group. Construct `SubBefore` using that captured value; derive `SubAfter` by applying the unrestricted operation to the captured value once more. Do not normalize twice before constructing the before row, and do not reread a later facade group. Adjust `consume_contexts` and its tests so direct-helper and installed-hook paths use the same stage contract.
3. Add Java-backed comparisons for absent/configured namespace, plain/matching/foreign/nested prefixes, null/empty values where accepted, retry/DLQ prefixes, and system resources. Verify group and bean-topic values in each affected hook, and retain at least one actual Lite SEND comparison. A public Producer group such as `other%workers` with no configured namespace must trace as `workers`; compare Pub topic normalization as well.
4. Preserve G01 framing/size/delivery/commit regressions and G02 live-group/null-poll tests. Mutation between callbacks must leave the after row derived from the captured before group. Keep message IDs, region, TRACE_ON filtering, access channel, status, and request correlation covered.
5. After focused trace tests and pinned Java comparisons, run:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
mvn -o -f compat/java-reference/pom.xml verify
scripts/check-workspace-metadata.py
```

Acceptance requires matching the complete matrix, stage-specific semantics, and all affected trace helpers without changing consumer registration or offset ownership. The current 2,067 passing tests are the non-regression baseline. Probe assertions currently describe the observed discrepancy; update them to Java's expected values when converting this evidence into repair validation. Existing environment-dependent live trace scenarios may be rerun last where available; they supplement the deterministic namespace matrix.
