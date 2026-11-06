# Phase 6 requirement and handoff audit

## P18-V01 Java common thread-pool diagnostics source disposition — 2026-10-24

The pinned 5.5.1 common module has a public `ThreadPoolMonitor` with its
implicit public constructor, `config`, three `createAndMonitor` overloads,
`logThreadPoolStatus`, `init` and `shutdown`; `ThreadPoolWrapper` exposes a
builder, getters/setters and value methods, and `ThreadPoolStatusMonitor`
declares three methods. Source hashes are `91a953b534d3a753d2b1085cb08705cfd212ebd5e5fd7a7c5a254608fd155df9`
(`ThreadPoolMonitor.java`),
`244773886ec16a908ab031b447951070d74a2cae9abe20e49b982f9741ec4f76`
(`ThreadPoolWrapper.java`), and
`d8d9983890425406e26665e357a45186b7ec8ca9cb194135dfb6a7c7b0e86c2a`
(`ThreadPoolStatusMonitor.java`).

A pinned production-source search found no call sites in the Java client or
remoting modules; references are confined to the common monitor/wrapper
implementation. Rust has no equivalent monitoring utility. This records an
unmapped common diagnostic API outside the client/remoting execution flows,
not a mapped client feature. Direct callers of that Java common utility do not
have Rust API parity; broad P18-V01 remains `PARTIAL`.

## P18-V01 Java `ProduceAccumulator` constructor-only surface — 2026-10-24 (`945812b4`)

The pinned client class declares one public constructor,
`ProduceAccumulator(String)`, and zero public methods. Its batching operations
are package-private/private and are reached through `DefaultMQProducer`; Rust
keeps the corresponding accumulator private inside `Producer` and exposes the
mapped batching settings and send operations there. The invoked Producer core
fixture records the constructor and empty public-method list in
`produceAccumulatorApi`; fixture SHA-256 is
`e7756ff8a7da4bac09de942e5eebf3e4886f933fb5f8483e4c7c7a8f61e228ff`.
The updated fixture generator source SHA-256 is
`3cbfb2c20ac7cc799331c24a5e1c9c9f999e908c899d009fe4cd9c3c7f1b3102`.
`pinned_java_produce_accumulator_declared_surface_is_constructor_only` checks
the inventory. The implementation source SHA-256 is
`89f2fcb62e9654aed52a190aa040ce4fd80eb62779e607910fdccb328d08df1b`.
This records the constructor-only internal type without exporting a Rust
wrapper; broad P18-V01 remains `PARTIAL`.

## P18-V01 `ResponseFuture.toString()` nested response rendering — 2026-10-24 (`669d8a0c`)

The pinned Java `ResponseFuture.toString()` embeds the populated command through
`RemotingCommand.toString()`. Rust `ResponseFuture` previously embedded the
command's `Debug` output; its `Display` implementation now uses the command's
Java-compatible display format. The invoked `response-future-api.json` fixture
records a deterministic response command rendering with its version, opaque
ID and serialization mode pinned; fixture SHA-256 is
`afdd50f4a3410b74e7f641bd89f6b9627a7509f99f13efc6bc680657d1df6656`. The
updated fixture generator SHA-256 is
`f76653a22f69143d52f87c8355a09c7c684e4e7761654c08dfe4c9975a623066`.
`response_future_display_preserves_java_to_string_state_fields` checks the
nested command string against the Java value and its embedding. Implementation
commit: `669d8a0c`; strict Remoting Clippy, the focused test and pinned offline
Maven `verify` pass.

This closes nested response-command rendering only. For a non-null callback,
Java includes the callback object's runtime identity string while Rust uses a
Rust-native callback marker; non-null Java `Throwable.toString()` class/message
formatting also has no universal equivalent for Rust `TransportError`. Those
runtime-dependent details remain open along with the broad P18-V01 audit.

## P18-V01 `ConsumeMessagePopOrderlyService` public surface disposition — 2026-10-24 (`43ab1572`)

The pinned Java 5.5.1 class has one public constructor and 15 declared public
methods. The invoked `PushImplementationAccessorFixtures` records the complete
signature list in `fixtures/consumer/push-implementation-accessor.json`; its
SHA-256 is
`be16e9671f172e534116f6b575d278a8105a883a2b2a7a847931fa4e656e8b15`. The
fixture source SHA-256 is
`42ba03bb1f206cba0e71277aab1df308037280e0650b7fb8a09449103154cfc1`, and the
pinned production source SHA-256 is
`0ac19d46f50110b5b5f2e6b3e21d4b6ac5e71ee33a81f63bfd24f5508803ad08`.

`DefaultMQPushConsumerImpl` stores this service in a private POP field and
exposes no getter for it. Its active calls are startup, shutdown, scheduled
lock renewal and POP request submission; Rust routes those responsibilities
through `PushConsumer`, `OrderlyConsumeService` and its internal POP worker.
There is no standalone Rust concrete service type. In pinned Java,
`submitConsumeRequest` throws `UnsupportedOperationException`,
`processConsumeResult` is an unconditional `true`, and nested
`ConsumeRequest.run` ends after checking the dropped flag and acquiring the
sharded lock. `sendMessageBack` and `resetNamespace` are not reached from that
request path. This records an implementation-class disposition and does not
claim one-to-one direct construction or helper-method parity; broad P18-V01
remains `PARTIAL`.

## P18-V01 Java `PullRequest` public value API — 2026-10-24

The pinned Java `client.impl.consumer.PullRequest` declares one public
no-argument constructor and 14 public methods. Rust now exports
`rocketmq_classic_consumer::PullRequest`; the invoked consumer facade fixture
pins null/zero/false defaults, nullable group/queue/process-queue state, `PULL`
mode, equality/hash selection, UTF-16 String hashing, shared mutable queue and
process-queue references, and Java-style `toString`. Equality and hashing use
only consumer group and message queue, matching Java's exclusion of the offset,
lock flag, and process queue. The direct value adapter is separate from Rust's
crate-private scheduler request. Implementation commit: `25b1a8e2`. Broad
P18-V01 source/settings/input/error/control-flow and reverse-export
reconciliation remains `PARTIAL`.

## P18-V01 Java `PopRequest` public value API — 2026-10-24

The pinned Java `client.impl.consumer.PopRequest` declares one public
no-argument constructor and 16 public methods. Rust now exports
`rocketmq_classic_consumer::PopRequest`; the invoked consumer facade fixture
pins defaults, all exposed fields, `POP` mode, equality/hash selection, UTF-16
String hashing, shared `MessageQueue` and `PopProcessQueue` references, and
Java-style `toString`. Java equality intentionally ignores the process queue,
`lockedFirst`, and `initMode`; Rust does the same. Its `MessageRequest` adapter
retains nullable consumer groups, and runtime dispatch passes them through the
nullable consumer selector. Code commit `f5ecf2ca` closes the direct
value/API gap; broad P18-V01 source/settings/input/error/control-flow and
reverse-export reconciliation remains `PARTIAL`.

## P18-V01 direct `MQClientAPIImpl` overload evidence — 2026-10-24 (`091eba55`)

The overload scan is anchored to pinned Java source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9` (RocketMQ 5.5.1), whose
`MQClientAPIImpl.java` SHA-256 is
`bfb0ebf8e4b4c0fd4ad5a4db46d64a7c8897250fb411487793a23f29aa094737`. The
separate Java checkout's current worktree revision is 5.3.3, so the review
read the pinned commit object and left that checkout, including its untracked
test file, unchanged.

Ten new direct invocations cover the previously under-evidenced overload
members: full `sendMessage`; `getTopicRouteInfoFromNameServer` with
`allowTopicNotExist = false`; deprecated and default-boundary `searchOffset`;
the default, topic-only, and list-only `getConsumeStats`; topic-only and
cluster-qualified `deleteTopicInNameServer`; and the default
`invokeBrokerToResetOffset`. The two `batchAckMessageAsync` members and the
other `searchOffset`, `getConsumeStats`, and reset-offset members already had
direct fixtures. The complete crosswalk, exact method/declaration counts,
per-member Rust mapping, and fixture IDs are in
[`PHASE_6_API_CONFIG_MAP.md`](PHASE_6_API_CONFIG_MAP.md).

`cargo test --locked -p rocketmq-classic-core --test rpc_invoked_java_fixtures`
passes all 29 invoked RPC cases. `mvn -f compat/java-reference/pom.xml verify`
passes fixture-drift checks, all 37 manifest scenarios, Java/Rust cross-decodes,
and the 11 message, 106 typed-header and 19 typed-body fixture suites.
`cargo fmt --check` and `git diff --check` pass. Code commit: `091eba55`.

This closes the seven-group `MQClientAPIImpl` overload evidence slice only.
Broad P18-V01 source/API/settings/input/error/control-flow and reverse-export
reconciliation remains `PARTIAL`; native Windows runtime and current-revision
remote CI are still unverified at this revision.

## Phase 18 current-code full-feature and server-free refresh — 2026-10-24 (`a460d966`)

On code revision `a460d966`, locked all-feature workspace tests and doctests
pass, as does strict all-target/all-feature workspace Clippy. Windows GNU
workspace `cargo check`, strict Clippy, and `cargo test --no-run` pass on the
macOS host; harnesses compile and link but were not executed. Offline
deterministic race/leak smoke passes 20 cases × 3 repetitions. Fixed-seed fuzz
passes 163 frame seeds, 11 message seeds, 11 property seeds, 2,960 mutations,
and 564/192/176 frame/message/property decodes. Differential comparator
self-test and validation of all 37 manifest scenarios pass. Pinned offline
Maven `verify` on this revision passes 37 scenarios, 11 message fixtures, 106
typed headers, and 19 typed bodies. Formatting and diff checks pass. This
refreshes local P18-V02 evidence only: broad P18-V01 remains `PARTIAL`, native
Windows execution and current-revision remote Ubuntu CI remain unverified, and
Phase 19 Broker/Docker evidence remains separate. No push or remote CI ran;
Phase 19-owned worktree paths were left untouched.

## Phase 18 Java `Comparable` declaration inventory — 2026-10-24

A source scan of the pinned Java 5.5.1 client/common/remoting modules found
seven declared `Comparable` implementations: `MessageQueue`,
`MessageQueueForC`, `TraceContext`, `SubscriptionData`, `BrokerData`,
`QueueData`, and nested `LatencyFaultToleranceImpl.FaultItem`. All seven map to
Rust Java-compatible comparison methods with invoked fixture coverage,
including null argument/field failures, UTF-16 ordering, availability
precedence, and numeric boundaries where applicable. The direct nested helper
API is closed in the 2026-10-24 follow-up below. This closes the
declaration-level comparator inventory for those modules only; inherited enum
comparison, other public API surfaces, and broad P18-V01
source/settings/input/error/control-flow/reverse-export reconciliation remain
separate.

## Phase 18 default config, packaging metadata, and P18-05 benchmark — 2026-10-24 (`a460d966`)

On current code revision `a460d966`, default-feature locked workspace tests
and doctests pass. The release facade examples compile with the release
profile; the offline benchmark compiles and completes. Locked Cargo metadata
contains 8 workspace members and 216 packages. `cargo package --workspace
--list --allow-dirty --no-verify --locked --offline` and
`scripts/check-workspace-metadata.py` pass; the latter verifies all 7 product
packages' `LICENSE`/`NOTICE`, package license metadata, and dependency direction.

The fixed-input microbenchmark ran on macOS ARM64, Apple M5 Max, 18 logical
CPUs, `rustc 1.85.0 (4d91de4e4 2025-02-17)`, release profile. These bounded
observations have no acceptance thresholds:

| Operation | Input | Iterations | ns/iteration | Allocation calls/iteration | Bytes/iteration |
| --- | ---: | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 B | 20,000 | 6,775.3 | 62 | 6,787 |
| Zstd message-record decode | 146 B | 10,000 | 2,240.9 | 17 | 197,493 |
| Producer send-request construction | 512 B | 20,000 | 2,898.3 | 54 | 3,665 |
| Route publish-address lookup (128 entries) | 10 B | 100,000 | 28.4 | 0 | 0 |
| Loopback pending-request completion | 128 B | 500 | 77,655.6 | 125 | 12,873 |

These checks supplement the full current-code P18-V02 suite recorded above.
Broad P18-V01 remains `PARTIAL`; the packaging checks do not establish
registry dependency availability or publication readiness.

## Phase 18 nested producer `FaultItem` reverse-surface audit — 2026-10-24

The pinned client source declares `LatencyFaultToleranceImpl.FaultItem` as a
public non-static nested class implementing `Comparable`. Its callable methods
include `FaultItem(String)`, `updateNotAvailableDuration(long)`,
`compareTo(FaultItem)`, `setReachable`, `setCheckStamp`, `isAvailable`,
`isReachable`, name/current-latency/start-timestamp getters, the current-latency
and start-timestamp setters, and `equals`, `hashCode`, and `toString`. The
enclosing implementation retains items only in private `faultItemTable`; its
public `LatencyFaultTolerance` operations do not return item objects. The
production source has no `compareTo` call site; `pickOneAtLeast` shuffles the
private values. Producer maps the operational fault-selection flow through
its owner and keeps its fault records private. The directly constructible
nested Java helper is scoped with the raw strategy/filter handles as an
implementation-detail disposition because it exposes no returned value or
client selection branch. This closes the helper inventory disposition only;
broad P18-V01 remains `PARTIAL` for the wider
source/API/settings/input/error/control-flow and reverse-export reconciliation.

## Phase 18 nested producer `FaultItem` public API parity — 2026-10-24

The earlier strategy-owner disposition above is superseded. Rust now exports
`FaultItem` and `FaultItemCompareError` from Producer and the facade. The Java
fixture records the nested class constructor and all 15 declared public
methods, including its erased `Comparable.compareTo(Object)` bridge. Direct
behavior coverage matches defaults, mutable latency/start/check/reachability
state, availability updates and wrapping, signed comparison, equality and
hash fields, UTF-16 String hashing, null/wrong-type failures, and `toString`.
Rust omits only the implicit enclosing-instance reference; no `FaultItem`
method reads it. `latency_fault_item_public_surface_and_behavior_match_invoked_java`
and the facade public-type test cover the mapping. This closes the nested
`FaultItem` API row only; broad P18-V01 remains `PARTIAL`.

## Phase 18 `MessageQueueForC` public value API — 2026-10-24 (`a460d966`)

The pinned Java 5.5.1 `MessageQueueForC` public value class maps to
`rocketmq_classic_common::message::MessageQueueForC`. The invoked Java fixture
and Rust differential cover its four-argument constructor, four getter/setter
pairs, value equality (including `offset`), Java's hash that omits `offset`,
`toString`, and 12 `compareTo` pairs. Comparison preserves Java UTF-16 string
ordering, wrapping `int` queue-id subtraction, the sign of wrapping `long`
offset subtraction, and `NullPointerException` outcomes for null arguments or
fields. The fixture also records the null `compareTo` argument; `route.json`
SHA-256 is
`9c068b67324f45510dab1aa4df3c2943b54944a9a56dcf8accc24f89f6612246`.
Common all-feature tests/doctests, strict Common Clippy, pinned offline Maven
`verify` (37 scenarios, 11 message fixtures, 106 typed headers and 19 typed
bodies), formatting, and diff checks pass. This is the value-class method
surface only. Java's `Serializable` marker/object-stream behavior and its
Broker2Client / `ResetOffsetBodyForC` C-client request path do not have a Rust
client counterpart in this slice. Broad P18-V01 source/API/settings/input/error/
control-flow and reverse-export reconciliation remains `PARTIAL`.

## Phase 18 `TraceContext.compareTo(null)` input parity — 2026-10-24 (`3427d269`)

Pinned Java 5.5.1 `TraceContext.compareTo(null)` throws
`NullPointerException`. Rust now exposes `java_compare_to_nullable`, which
preserves the existing timestamp ordering and maps a null argument to
`TraceContextCompareError::NullOther`. The invoked trace fixture records the
Java exception class; `trace-records.json` SHA-256 is
`e56224bb6c926d53ab4c118fcb38d5a6067e10e252bd42817f2be68c1f7a3f5e`.
Core all-feature tests/doctests, strict all-target/all-feature Core Clippy,
pinned offline Maven `verify` (37 scenarios, 11 message fixtures, 106 typed
headers and 19 typed bodies), formatting, and diff checks pass. This closes
the null argument branch only; broad P18-V01 remains PARTIAL. The latest full
workspace P18-V02 refresh predates this code commit and will be repeated after
the remaining source-audit slices.

## Phase 18 current-code full-feature and server-free refresh — 2026-10-24 (`80acf42f`)

On code revision `80acf42f`, locked all-feature workspace tests and doctests
pass, as does strict all-target/all-feature workspace Clippy. Windows GNU
workspace `cargo check`, strict Clippy and `cargo test --no-run` also pass on
the macOS host; the Windows harnesses were compiled and linked, not executed.
The offline deterministic race/leak smoke passes 20 cases × 3 repetitions;
fixed-seed fuzz passes 163 frame seeds, 11 message seeds, 11 property seeds,
2,960 mutations and 564/192/176 frame/message/property decodes; comparator
self-test and manifest validation pass for all 37 scenarios. The pinned
offline Maven `verify` on this same code revision passes 37 scenarios, 11
message fixtures, 106 typed headers and 19 typed bodies. Formatting and diff
checks pass. This refreshes local P18-V02 evidence only: broad P18-V01 remains
PARTIAL, native Windows execution and current-revision remote Ubuntu CI remain
unverified, and Phase 19 Broker/Docker evidence remains separate. No push or
remote CI was run.

## Phase 18 route DTO `compareTo(null)` input parity — 2026-10-24 (`80acf42f`)

Pinned Java 5.5.1 `MessageQueue.compareTo(null)`, `QueueData.compareTo(null)`,
and `BrokerData.compareTo(null)` each throw `NullPointerException`. Rust now
exposes `java_compare_to_nullable` with typed `NullOther` errors for all three
types, while retaining their existing non-null comparison methods. Invoked
`RuntimeRouteFixtures` records each Java exception; `route.json` SHA-256 is
`fa4ee2e7b22496a561998501ca464455ebe0fc489a005868c96521e20efb4732`.
Common and Protocol all-feature package tests/doctests, strict all-target /
all-feature Common and Protocol Clippy, pinned offline Maven `verify` (37
scenarios, 11 message fixtures, 106 typed headers and 19 typed bodies),
formatting, and diff checks pass. This closes the null argument branch for
these three comparators only; broad P18-V01 remains PARTIAL.

## Phase 18 `SubscriptionData.compareTo(null)` input parity — 2026-10-24 (`fdfdbaea`)

Pinned Java 5.5.1 `SubscriptionData.compareTo(null)` throws
`NullPointerException`. Rust now exposes `java_compare_to_nullable` and the
typed `SubscriptionDataCompareError::NullOther` result while preserving the
existing non-null `java_compare_to` API. The invoked heartbeat fixture records
the Java exception class; `heartbeat.json` SHA-256 is
`7329d0dc1c886c1110a9e81661f72984087f8ecf86fd8a46120a8b86cfe415a8`.
Focused Protocol regression, Protocol all-feature tests/doctests (168 unit,
165 doctests), strict all-target/all-feature Protocol Clippy, Maven `verify`
(37 scenarios, 106 typed headers, 19 typed bodies), formatting, and diff checks
pass. This closes the null argument branch only; broad P18-V01 remains PARTIAL.

## Phase 18 current-revision race, fuzz, and handoff smoke — 2026-10-24

On code commit `33c57f09`, the offline deterministic race/leak suite passes 20
cases × 3 repetitions, including the 2,000 concurrent-send case. The fixed-seed
fuzz smoke passes with 163 frame seeds, 11 message seeds, 11 property seeds,
2,960 mutations, and 564/192/176 frame/message/property decodes. The
differential comparator self-test passes and the handoff manifest validates all
37 scenarios. Together with the full all-feature workspace test/Clippy run
recorded above, this refreshes the local P18-V02 gate. P18-V01 remains PARTIAL;
native Windows runtime and current-revision remote Ubuntu CI remain unverified.
No push or remote CI was run.

## Phase 18 Windows GNU all-feature workspace compile/link — 2026-10-24

On macOS ARM64 with Rust 1.85.0 and the installed MinGW linker, the current
Phase 18 code passes Windows GNU all-feature workspace `cargo check`, strict
all-target/all-feature Clippy, and `cargo test --no-run` for every workspace
test harness. The test executables compile and link; they were not executed.
This adds compile-only Windows evidence. Native Windows runtime and
current-revision remote Ubuntu CI remain unverified, and broad P18-V01 remains
PARTIAL. No push or remote CI was run.

## Phase 18 property decoder fuzz seeds — 2026-10-24

P18-V02 deterministic fuzz smoke now seeds the property decoder directly from
invoked Java property-string inputs and decoded message properties, in addition
to mutating complete Remoting frames and message records. The corpus has 11
unique encoded property seeds, each receiving 16 deterministic mutations for
byte flips, truncation, U+0001/U+0002 separator changes, and control/invalid
suffix bytes. On code commit `67ee9ec4`, the smoke passes with 163 frame seeds,
11 message seeds, 11 property seeds, and 2,960 total mutations; it completes
564 frame decodes, 192 message decodes, and 176 property decodes. The full
all-feature workspace tests/doctests and strict all-target/all-feature workspace
Clippy pass on docs revision `1f9f1ce7`; formatter and diff checks pass as well.
This also verifies the unit-only workspace without Docker or a Broker. It closes
the direct property-seed corpus gap only; broad P18-V01 reconciliation and
P18-V04 native/remote evidence remain open.

## Phase 18 PULL assignment expiry and retention — 2026-10-24

The invoked Java `RebalanceImpl.updateMessageQueueAssignment` fixture now
records four PULL assignment outcomes: an expired assigned queue is dropped
and replaced, an active assigned queue remains the same object, a dropped
assigned queue remains present, and an empty assignment drops and removes the
queue. Rust regression
`assigned_pull_queue_expiry_matches_java_replacement_and_retention` compares
the same states while driving an authoritative route assignment. The combined
`fixtures/runtime/consumer-operations.json` SHA-256 is
`892956f5ef65b5418bf637a5065f4697f4342a7926d9e04d69cdd657a7d3a911`.
Focused Rust coverage, locked all-feature workspace tests/doctests, strict
all-target/all-feature workspace Clippy, pinned Java Maven `verify`, formatter,
and diff checks pass on code commit `33c57f09`. This closes the assigned PULL
expiry/retention slice only; broad P18-V01 source/API/input/error/control-flow/
reverse-export reconciliation and current native/remote platform evidence
remain open.

## Phase 18 Fastjson numeric-token continuation — 2026-10-24

Added fourteen pinned Java 5.5.1 heartbeat DTO inputs, covering accepted
uppercase `L`/`F`/`D`/`B`/`S` suffixes, exponent-plus-suffix,
leading/trailing decimal points, rejected lowercase `f`, and suffix coercion
to a string field. The previous Rust differential failed on the nine accepted
numeric forms; the expanded 1,102-case differential and focused numeric parser
regressions now pass. The parser keeps the tested integer-field results,
including truncation of `-.5` to zero and `1.e2D` to 100, and matches Java
Float rounding and Float/Double exponent strings for the tested topic values.
Fixture SHA-256 is recorded in the heartbeat audit. This closes these tested
grammar and coercion forms only. Broad P18-V01 and Phase 6
source/API/input/reverse-export reconciliation remain open.

## Phase 18 Fastjson trailing EOF-marker continuation — 2026-10-24

Pinned Java 5.5.1 Fastjson accepts one or more trailing U+001A markers after
a DTO document, including markers separated by JSON whitespace. Adding two
invoked heartbeat inputs first made the Rust differential fail with trailing
characters; the shared protocol normalizer now removes marker characters only
from the trailing whitespace/marker suffix. Markers in the root prefix and
structural positions remain rejected, while raw markers inside strings retain
their existing accepted behavior. The
expanded 1088-case Rust differential and a focused parser regression pass.
`fixtures/runtime/heartbeat.json` SHA-256 is
`2f55c06fd34dabb708de5c5455da71bbcc3ec19f0a5df19b0e9e65e9d18b706b`.
This closes that input boundary only; broad P18-V01 and Phase 6
source/API/input/reverse-export reconciliation remain open.

## Current Phase 18 server-free rerun — 2026-10-23

At code revision `c3b53fcb`, the server-free gates pass on macOS AArch64 with
Rust 1.85.0 and pinned Java 5.5.1. Default and all-feature locked workspace
tests, strict all-target/all-feature workspace Clippy,
`cargo fmt --all -- --check`, and `git diff --check` pass. Offline Maven
`verify` passes 37 differential scenarios, 11 message fixtures/runtime cases,
36 route/VIP cases, and Java cross-decodes 106 typed headers and 19 typed
bodies produced by Rust. Differential self-test and 37-scenario manifest
validation pass. Fuzz smoke covers 163 frame seeds, 11 message seeds and 2,784
bounded mutations, yielding 245 frame decodes and 192 message decodes.
Deterministic race/leak smoke passes 17 cases repeated three times. Release
examples build and the offline benchmark runs; its current observation is
recorded in the [Phase 18 benchmark baseline](PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).

Follow-up code revision `612f9c71` adds the nullable ProcessQueue message
lookup adapter. On that revision, the focused facade comparison, all-feature
Consumer package tests, strict Consumer Clippy, formatting/diff checks, and
pinned Maven verify pass. The full workspace, fuzz, race/leak, release-example,
and benchmark gate above remains the recorded run on `c3b53fcb`.

Follow-up revision `e5075bfe` also preserves Java's swallowed null-info input
for `fillProcessQueueInfo`; the focused facade comparison, pinned Java facade
fixture verification, strict Consumer Clippy, and formatting/diff checks pass.

Follow-up revision `90f9e2d1` maps `cleanExpiredMsg(null)` to a typed
`NullPointerException` cleanup error before scanning. The direct Java expiry
fixture, Rust expiry comparison, strict Consumer Clippy, and formatting/diff
checks pass.

Follow-up revision `e8a4811f` adds the `putMessage(null)` adapter. Its focused
facade comparison, pinned Java facade fixture verification, strict Consumer
Clippy, and formatting/diff checks pass.

Follow-up revision `bb167942` models the caught `removeMessage(null)` path:
consume timestamp update, return offset, and retained queue state. Its focused
facade comparison, pinned Java facade fixture verification, strict Consumer
Clippy, and formatting/diff checks pass.

Follow-up revision `0418761f` maps `makeMessageToConsumeAgain(null)` to the
uncaught Java null-list failure. Its focused facade comparison, pinned Java
facade fixture verification, strict Consumer Clippy, and formatting/diff checks
pass.

Follow-up revision `68f554e3` preserves the valid-prefix mutations from
`putMessage([message, null])` before Java's `NullPointerException`. Its focused
facade comparison, pinned Java facade fixture verification, strict Consumer
Clippy, and formatting/diff checks pass.

Follow-up revision `9be7efed` requeues the valid prefix from
`makeMessageToConsumeAgain([message, null])` before the Java exception. The
focused facade comparison, pinned Java fixture verification, strict Consumer
Clippy, and formatting/diff checks pass.

Follow-up revision `463db95a` preserves `removeMessage([message, null])`'s
caught partial effects: the ready-tree prefix and byte size change, aggregate
count remains unchanged, and Java returns the precomputed next offset. The
focused facade comparison, targeted pinned Java fixture verification, all 260
Consumer library tests and 69 integration tests with all features, strict
Consumer Clippy, and formatting/diff checks pass.

At revision `533d9713`, default and all-feature locked workspace tests pass,
including the exact 32-method ProcessQueue crosswalk regression. Strict
all-target/all-feature workspace Clippy and full offline Maven `verify` pass;
Maven validates 37 differential scenarios and Java cross-decodes all 106 typed
headers and 19 typed bodies. Fuzz, race/leak, release-example, and offline
benchmark results remain the recorded runs at `c3b53fcb`.

Follow-up revision `0a88fcaa` maps Java's `PopProcessQueue.fillPopProcessQueueInfo(null)`
failure and records the complete 11-method PopProcessQueue crosswalk. All 260
Consumer library tests and 73 Consumer integration tests with all features,
strict Consumer Clippy, formatting/diff checks, and full offline Maven `verify`
pass. Maven revalidates 37 scenarios, 106 typed headers, and 19 typed bodies.

Locked Cargo metadata, workspace metadata/license/dependency checks, seven
package-content listings with `LICENSE`/`NOTICE`, and UTC plus Asia/Taipei
Unique ID fixtures passed at revision `60238931`. At that revision the Consumer
library also passed Windows GNU cross-target all-target/all-feature check,
test executable linking and strict target Clippy. The MSVC cross-check was
retried after installing the pinned Rust 1.85 target, then stopped at missing
Windows SDK headers (`stdlib.h` and `windows.h`). Native Windows runtime
execution and current-revision remote Ubuntu CI remain unverified. The
referenced external Java checkout is at
`2238256de1d227a4384ba969dfa31473187b4d08` (RocketMQ 5.3.3) and contains a
pre-existing untracked `broker/src/test/java/org/apache/rocketmq/broker/aaaa.java`;
it was left untouched. Compatibility source review and fixtures use pinned
5.5.1 commit `efa1be59d6fc747ad220f036c86030f05f0419f9` and the pinned 5.5.1
artifacts.

The broad P18-V01 Java source/API/settings/input/error/control-flow and Rust
reverse-export reconciliation remains PARTIAL. The manifest registers 37
scenarios, with 30 live handoff scenarios still `PENDING_PHASE_19`; native
Windows, current remote Ubuntu CI, and Phase 19 Broker/Docker evidence remain
open. This rerun records server-free gates only and is not a phase-closure or
compatibility claim.


P18-V01 `ProcessQueue.removeMessage` in-flight and dispatch-state parity
(`aa3cec73`): the invoked Java 5.5.1 fixture exposed that removal only mutates
the ready message map, leaving in-flight count/size available to `commit`, and
that removing the last ready entry does not clear the `consuming` dispatch
flag. Rust now preserves both state transitions. The expanded `processQueue`
fixture SHA-256 is
`67aac16425d8ac98e2d2cde4ff53a91e68b080b429711852979eb302d173dc5e`; the
Consumer facade comparison and two focused regressions pass. This closes the
two named removal flows only; broad P18-V01 source and reverse-export review
remains open.

P18-V01 `ProcessQueue.putMessage` parse-error and pull-timestamp parity
(`267173dd`): the invoked Java 5.5.1 fixture pins `NumberFormatException` for
malformed `MAX_OFFSET` after insertion and dispatch-state mutation, then checks
the next valid insertion does not request another dispatch. Rust now exposes
the parse failure as `ProcessQueueError::InvalidMaxOffset` while retaining the
same queue state. The fixture also proves that `putMessage` preserves a
caller-set `lastPullTimestamp`; Rust records pull time at the owner request
boundary. Fixture SHA-256:
`a334ff119c014259676917531ce4032160b10209cdac027d9963d2c1327d5d6a`. The
facade comparison and both workspace test configurations pass. This closes
these ProcessQueue input/timestamp cases only; broad P18-V01 review remains
open.

P18-V01 `ProcessQueue.commit` empty-input parity (`e1b8ffdb`): pinned Java
calls `consumingMsgOrderlyTreeMap.lastKey()` before mutating the queue, so an
empty ordered-consume set throws `NoSuchElementException` and leaves count,
size, and tree unchanged. Rust now returns
`ProcessQueueError::EmptyOrderedConsumeSet` rather than `-1`; successful commits
return `Ok(offset)`. The invoked facade fixture records the Java exception
class and unchanged queue state. Fixture SHA-256:
`aa3a16d255ba6d331f5cb84e068db3b14e45f49c605671feebc97abc3aba8f81`. Default
and all-feature workspace tests, strict Clippy, pinned Maven verify, and the
focused facade comparison pass. This closes the empty-commit input only; broad
P18-V01 reconciliation remains PARTIAL.

P18-V01 `ProcessQueue` dispatch and timestamp transitions (`2ce652e7`): pinned
Java changes `consuming` in `putMessage` when scheduling and clears it only
when `takeMessages` returns empty. Nonempty takes, `commit`, `rollback`, and
`makeMessageToConsumeAgain` preserve the current value; `commit` also leaves
`lastConsumeTimestamp` unchanged. Rust now follows these public transitions.
The invoked fixture checks subsequent dispatch after commit, rollback, and
requeue, including a nonempty take while `consuming` is false, plus timestamp
preservation. Fixture SHA-256:
`2e43cc737fa784cdaec0488252000e82360ccd0a4b121e6ee28d2dccf704791e`. Default
and all-feature workspace tests, strict Clippy, pinned Maven verify, and the
focused facade comparison pass. This closes these named state transitions
only; broad P18-V01 reconciliation remains PARTIAL.

P18-V01 `ProcessQueue` duplicate-offset parity (`8bf6ee40`): pinned Java uses
separate ready and orderly in-flight maps, so `putMessage` can insert a ready
copy at an offset that is still being consumed. Rust now retains that copy,
exposes it through the message-tree view, and matches Java's commit, rollback,
remove, counter, and `queueOffsetMax` transitions. Invoked facade cases check
the ready body before/after commit, rollback overwrite, removal of the ready
copy, and an existing-offset replacement. An internal batch regression checks
that terminal completion retains a repulled message. Fixture SHA-256:
`f4c918d58b0fd0104511281b8380a13356cd0dfb5bd1d2d370fe9b890e32bb31`. Default
and all-feature workspace tests, strict Clippy, pinned Maven verify, fuzz and
race/leak smoke, release examples, and the offline benchmark pass. This closes
these duplicate-offset flows only; broad P18-V01 reconciliation remains
PARTIAL.

P18-V01 `ProcessQueue.clear` dispatch-state parity (`85f210bd`): pinned Java
clears both message maps, counters, and `queueOffsetMax` while leaving
`consuming` unchanged. Rust now preserves that flag, so the next `putMessage`
reports dispatch according to the pre-clear state. The invoked facade fixture
checks the subsequent dispatch, empty-to-refilled tree, and reset count/size.
Fixture SHA-256:
`8ab5b2da6cd00449b2af89e5f4a3879608e00990cd2b5562cc2eef2dc23f18ff`. Default
and all-feature workspace tests, strict Clippy, pinned Maven verify,
differential self-test/manifest, fuzz and race/leak smoke, release examples,
and the offline benchmark pass. This closes this clear-state input only; broad
P18-V01 reconciliation remains PARTIAL.

## Direct runtime API reconciliation — 2026-10-20

The direct public runtime `consumerRunningInfo`, `consumeMessageDirectly` and
`resetOffset` entrypoints now exist. Commits `7f3bd2d9`, `4eee824f`,
`4bf3e83c` and `6b5129f1` add their owner adapters and close the discovered
shared-map/store read timing, per-queue failure and runtime-monitor ownership
gaps. [The scoped operation audit](PHASE_18_RUNTIME_CONSUMER_OPERATIONS.md)
records Java source, nine Running Info cases, 26 direct-consume cases,
fifteen reset cases plus a three-store handoff, real resume/rebalance probes,
and cancellation/cleanup tests. Continuation `c99eff31` preserves raw ordinary
versus client message IDs and nullable topics; `98fb3feb` preserves offsets
when rebalance truncates unsubscribed queues. Reset probes now invoke real
resume, including its final target rebalance before returning. The separate
resume probe checks repeated unpaused invocation and sibling isolation.
The earlier [consumer-status audit](PHASE_18_RUNTIME_CONSUMER_STATUS.md) covers
the fourth direct runtime operation and its 54 group/topic cases.

P18-V01 `MqClientAdmin` Rust wrapper closure (2026-10-21):
`MQClientApi::mq_client_admin()` now exposes all 19 pinned Java interface
operations, including the 11 methods that previously existed only through
other `MQClientApi` paths. The invoked Java fixture records the complete
interface reflection inventory, and
`mq_client_admin_surface_matches_pinned_java_method_inventory` checks all 19
Rust call sites against it. The topic-update wrapper sends the supplied header
directly, preserving Java's lack of local permission validation; a fixture
case and Rust loopback guard that behavior. Fixture SHA-256:
`16f61c5e859f44062a2ec732e2603ad97b38719b9a0f8fa23fc09ffa5112dfd3`.
This closes the named wrapper-surface gap only; broader DTO/config/input/error/
flow/reverse-export work and current platform CI evidence remain open.

P18-V01 `MqClientAdmin.queryMessage` key filtering (2026-10-21): invoked Java
cases pin empty query-key handling, NBSP, em-space, U+0085 and U+001C keys.
Rust now follows `Character.isWhitespace` via the pinned Apache Commons
`StringUtils.isNotBlank` path and Java `String.split(" ")` token handling in
both direct Broker results and cross-Broker merge filtering. The direct
fixture comparison and Rust unit test pass. This closes those filter inputs
only; the broader message/API/input audit remains open.

P18-V01 POP retry-assignment transition slice (2026-10-21): the invoked Java
`RebalanceImpl.updateMessageQueueAssignment` fixture now covers push-to-pop,
pop-to-push, mixed, retry-topic and empty assignments. It exposed that Java
removes the retry subscription while retaining the cached route on push-to-pop;
Rust now matches both observable tables. The runtime POP assignment fixture
also records expired-assigned replacement, active-assigned same-object
retention, dropped-assigned retention and empty-assignment removal
(`fixtures/runtime/consumer-operations.json`, SHA-256
`a8d82baca384b9b7932b4961b051902336155543428047d891eb9f84052d166d`). Rust's
owner-flow regression checks all four states, including retention of the active
worker after an assignment error. This closes these recorded transition cases
only. Assigned PULL expiry and retention are covered by the 2026-10-24
follow-up above; remaining assignment expiry/retention dimensions and the
broader inherited/source audit remain open.

P18-V01 direct-consume runtime subtype slice (2026-10-21): `MessageClientExt`
can be supplied through Java's `MessageExt` direct-consume parameter and remains
the runtime class observed by the listener. Rust now preserves the subtype when
converting its `MessageClientExt` wrapper to the base input type, retains live
`UNIQ_KEY` lookup and the virtual message-ID fallback, and uses the same subtype
state for client decoding, pull results and admin-decoded messages. The invoked
fixture records callback class and the Rust comparison checks all 26 direct
consume cases. Fixture SHA-256:
`d0b4e39ba340e509d1ddc2665da122a0cf161ed757cc1d9ac2cbd022edb7d83f`.
The full inherited/public/DTO/config and reverse-export audit remains open;
this closes only the `MessageClientExt` direct-consume path.

P18-V01 caller-defined `MessageExt` subtype adapter (2026-10-21): public Rust
`MessageExtSubtype` retains application fields for typed callback downcasts and
provides virtual hooks for topic, broker name, queue ID, message ID,
reconsume-times getters/setters, and display. The invoked
`concurrent-custom-subtype` Java fixture checks the runtime subclass, overridden
retry-topic and namespace setters, broker assignment, message/queue values,
reconsume getter/setter, custom field, and `toString`; the Rust direct-consume
comparison matches all of them. Current fixture SHA-256:
`c05bde846eae0f39cc654cf49cfa33f624be62165c17cbc2da46fa80d1a77c0a`. Subtype
state is runtime-only and is omitted by serialization. This closes the
direct-consume custom-field and selected-override slice; it does not inventory
every inherited `Message`/`MessageExt` virtual method.

P18-V01 caller-defined subtype queue/body follow-up (2026-10-21):
`MessageExtSubtype` now maps `getQueueOffset`/`setQueueOffset` and
`getBody`/`setBody`. The invoked Java fixture compares the custom values through
direct consumption and `ProcessQueue.putMessage`; Rust matches the callback
values, message-tree key, dispatch state, message count, byte size and setter
counts. Fixture SHA-256:
`9b67a09bc850192e2856bc908c10a6dfa6590db3f924b32fe62b6e97edcc510f`. Other
inherited `Message`/`MessageExt` virtual methods remain open.

P18-V01 caller-defined subtype flag follow-up (2026-10-21):
`MessageExtSubtype` now dispatches Java's virtual `getFlag`/`setFlag` methods.
The direct-consume Java fixture records the overridden flag `71` and the
custom setter invocation; Rust matches both. This covers the flag reads in
Push retry/send-back message copies, while broad subtype and P18-V01 review
remain open. Fixture SHA-256:
`8afd6c83b5085d3c74cbcdd919bca3257514abbde3c7f3ec70847dd08a1a2f7f`.

P18-V01 caller-defined subtype property follow-up (2026-10-21):
`MessageExtSubtype` now dispatches Java's virtual `getProperty(String)`.
`ProcessQueue.putMessage` reads the overridden `MAX_OFFSET` property and both
Java and Rust compute `msgAccCnt` as `12`; the fixture also compares the getter
call count. Fixture SHA-256:
`2a70e2a142e6adfa8b43baf7e396515630fcd6dc2231e8d8867e8eab3a4b2db4`.

P18-V01 subtype property-derived getter follow-up (2026-10-21): Rust
`MessageExt::tags` and `keys` now route through the subtype's virtual property
hook, matching Java `getTags`/`getKeys` calls to `getProperty`. The invoked
direct-consume callback compares `subtype-tag` and `subtype-key` and both read
counts. Fixture SHA-256:
`7a5fb470b3e3ad32f55b02d9ac77894b7a98439aff695f4c2a635c4f5515e9d4`.

P18-V01 caller-defined subtype property-map follow-up (2026-10-21):
`MessageExtSubtype` now maps nullable `getProperties()` snapshots, and
`MessageExt::nullable_property_entries()` uses that override for Push retry
property copies. The invoked direct-consume fixture compares a custom map value
and getter count with Java. At this snapshot-only checkpoint, Java's live map
identity and mutation aliasing were not yet preserved; the `live_properties`
follow-up below closes that gap. Fixture SHA-256:
`348f65888fbce4d8d36fec26ac88c9279deacb6bbbcaf3514646e2efa3f460e5`.

P18-V01 caller-defined subtype store-host follow-up (2026-10-21):
`MessageExtSubtype` now dispatches virtual `getStoreHost`/`setStoreHost`.
Push send-back uses the custom store host when no broker name is available;
the Rust send-back test confirms the endpoint comes from the override. The
invoked Java direct-consume fixture compares the getter presence and setter
count. Fixture SHA-256:
`3633f3ac0098815df76790a969a15e7a9e2edda45f30723bd67a2753468facab`.

These operation-presence and tested-flow slices do not close broad P18-V01.
Remaining work includes the full inherited/public/DTO/configuration and
reverse-export review, subtype behavior beyond the adapter's selected hooks,
and the broader rebalance assignment/pull-expiry branches beyond the cases
above. Native platform CI evidence is still outstanding. Historical checkpoints
below retain their original scope and results; none is a current full-phase
acceptance claim.

P18-V01 `MQClientAPIExt` public-operation closure slice (2026-10-19): the
pinned 5.5.1 `javap -public` inventory has 29 declared non-constructor methods.
Every operation now has a Rust owner: existing Producer/Consumer/Core APIs
cover the inherited send, POP/ACK, pull, recall, Lite-subscription and admin
paths; `MQClientApi` now also covers heartbeat future/oneway, raw async
send-back and invoke, consumer-offset futures, callback consumer-list lookup,
nullable offset futures, lock future, and unlock oneway; `ClientRuntime`
reapplies the retained nullable NameServer setting; notification and Lite topic
information have typed RPC/DTO paths. Thirty-five future/oneway/config cases
plus 7 Lite-topic-info cases cover the added branches, nullable offsets, raw
response preservation, callback modes, and NameServer null/empty transitions.
The fixture suite also checks success with no consumer-list body falling
through to an empty list.
The method/branch crosswalk is in the [Phase 5 matrix](PHASE_5_OFFLINE_METHOD_MATRIX.md).
The external Java checkout currently has a different HEAD; provenance and
behavior are taken from pinned source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9` and the 5.5.1 dependency artifact,
not the mutable checkout. This closes the `MQClientAPIExt` operation slice
only; broad P18-V01 DTO/config/input/error/control-flow/reverse-export
reconciliation remains PARTIAL.

P18-V01 protocol response-header visibility slice (2026-10-19): the pinned
response-header inventory records no Java public fields for
`PullMessageResponseHeader`, `SendMessageResponseHeader`,
`CheckTransactionStateResponseHeader`, `NotificationResponseHeader`,
`PopMessageResponseHeader`, `PopLiteMessageResponseHeader`, and
`PollingInfoResponseHeader`. Rust storage is crate-private with Java-mapped
accessors, and compile-fail/API inventory plus accessor/default tests guard the
surface. This closes the field-visibility/accessor slice only; the broad audit
remains PARTIAL.

P18-V01 Fastjson writer-feature closure (`07d2fba1`, 2026-10-17): the pinned
48-value `JSONWriter.Feature` inventory is mapped 48/48. The final
`WriteClassName` implementation compares invoked Java and Rust output for
root and nested DTO metadata, polymorphic collection members, typed map values
and `MessageQueue` keys, `Set` markers, long-field suffixes, and the combined
class-name suppression flags. See the current writer-feature summary in the
[API/config map](PHASE_6_API_CONFIG_MAP.md). This closes the writer-feature
slice only; the broader P18-V01 bidirectional API/settings/source/input/error/
flow/reverse-export audit remains PARTIAL.

P18-V01 `MqClientAdmin` inventory reconciliation (2026-10-18): the separate
Java `MqClientAdmin` interface now has all 19 declared operations recorded in
the [Phase 17 admin matrix](PHASE_17_ADMIN_MATRIX.md), with source-backed typed
RPC slices and invoked Java/Rust evidence. This supersedes the earlier finding
below that the interface had no complete Rust crosswalk, at the operation-
presence level only. The reverse-surface pass also found the public
`ConsumerConnection.computeMinVersion()` method; its Rust mapping and invoked
Java fixture now cover the method and null/empty/version boundary cases. Full
reconciliation of each DTO's public methods and
defaults, overload/input/error/control-flow branches, and Rust reverse exports
remains open P18-V01 work; this does not close the broader audit.

## Current closure state (2026-10-16)

The Phase 6 local behavioral gates P6-V01/V02/V03 and P6-U01 remain verified; P6-V04 is a Phase 19 live Broker check. The broad compatibility audit is still **PARTIAL**, for these concrete non-manual reasons:

The governing parity rule is exact equality for public APIs and settings, with flow-logic parity for other behavior; Java and Rust need not use the same implementation components. Under that rule, Java's `UtilAll.exceptionSimpleDesc` versus Rust's error-text formatting is an accepted diagnostic-only variation when error category, response code, and processing order match. This does not waive any public surface, setting, category, or flow gap recorded below.

- Current server-free P18-V02 gate on source commit `787157a0` (2026-10-16,
  macOS ARM64), after the Common `MessageUtils`, `CorrelationIdUtil`, and
  `NameServerAddressUtils` API slices and the lifecycle/thread-helper audit:
  default and all-feature locked workspace tests, strict
  all-target/all-feature Clippy, workspace formatting, pinned Maven `verify`
  (37 scenarios, fixture drift and Java/Rust cross-decodes), differential
  self-test and 37-scenario manifest, deterministic race/leak smoke (16 cases
  × 3), fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations),
  release examples, offline benchmark compilation, locked Cargo metadata,
  workspace metadata/license/dependency checks, seven package listings with
  LICENSE/NOTICE, and the Asia/Taipei Unique ID fixture all pass. Metadata
  reports 7 workspace packages, 215 packages total, and 208 registry packages;
  all workspace packages are version `0.1.0`. This closes P18-V02 on this
  revision. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 Docker/Broker automation, and release registry/legal preflight
  remain open.

- ClientConfig field visibility (`2026-10-15`, pinned Java source
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): the 33 Rust backing fields are
  now crate-private, matching Java's private/protected storage boundary while
  retaining all mapped getter/setter pairs. Cross-crate Consumer running-info
  serialization uses the hidden read-only bridge for Java
  `MixAll.object2Properties`; it preserves raw values and null omission. An
  external compile-fail doctest guards the boundary. The field inventory is now
  8 public-field structs / 41 fields, down from the historical 9 / 74. Focused
  Core, Consumer, and Producer checks pass; this closes only this surface slice
  and does not close P18-V01.
- Current Phase 18 server-free gates (`3f57e7ec`, 2026-10-14): default and all-feature workspace tests, strict all-target/all-feature Clippy, Producer tests, pinned Maven verify (37 scenarios, fixture drift and Java/Rust cross-decodes), comparator self-test and 37-scenario manifest, deterministic race/leak smoke (16 cases × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release examples, offline benchmark compilation, workspace metadata, locked Cargo metadata and seven package-content listings pass. This closes P18-V02 at this code revision. P18-V01 remains PARTIAL; current-revision GitHub Ubuntu evidence, Phase 19 Docker/Broker execution and registry dependency resolution remain outstanding.
- Latest Phase 18 server-free gates (`133d4f54`, 2026-10-15), after the mutable reserved-property correction: sequential default and all-feature workspace tests, strict all-target/all-feature Clippy, formatting/diff checks, Maven verify (37 scenarios, fixture drift and Java/Rust cross-decodes), comparator self-test and 37-scenario manifest, deterministic race/leak smoke (16 cases × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release examples, offline benchmark compilation, workspace metadata/Cargo metadata and seven package-content listings pass. This closes P18-V02 on this revision. Broad P18-V01 remains PARTIAL; the current-revision GitHub Ubuntu result and Phase 19 Docker/Broker execution remain outstanding.
- Current Phase 18 server-free gates (`51be0d20`, 2026-10-15), after the nullable reserved-property-set correction: UTC default and all-feature workspace tests, strict all-target/all-feature Clippy, Maven verify (37 scenarios with fixture drift and Java/Rust cross-decodes), comparator self-test and manifest, 16 race/leak cases × 3, fuzz smoke (1,824 mutations), release examples, workspace benchmark compilation, locked Cargo metadata, workspace metadata and seven package-content listings all pass; format and diff checks also pass. P18-V02 server-free gates pass on this revision. P18-V01 remains PARTIAL; current-revision GitHub Ubuntu evidence and Phase 19 Docker/Broker validation remain open.
- Message equality reverse-surface slice (`2026-10-15`, pinned Java 5.5.1): `Message` and `MessageExt` inherit `Object.equals`/`hashCode`; separate Java objects with default fields compare unequal. Rust `Message` no longer implements structural `PartialEq`/`Eq`; `MessageExt` equality now checks a hidden identity token that survives `Clone`. The invoked `messagePublicApi` fixture and Rust API tests cover same-instance and distinct-default behavior. Workspace all-feature tests and Maven verify pass after this correction; P18-V01 remains open for the remaining Common and workspace source/API/input/reverse-export inventory.
- Message user-property whitespace inputs (`2026-10-15`, pinned Java 5.5.1): `putUserProperty` uses `String.trim()`, which removes only U+0000–U+0020. Rust now follows this boundary instead of Unicode-wide whitespace trimming. The invoked Java fixture and Rust tests verify non-breaking-space keys and em-space values are accepted while ASCII control/space-only input is rejected. This closes that validation input slice only; broader Message input/API coverage remains part of P18-V01.
- Mutable `MessageConst.STRING_HASH_SET` behavior (`2026-10-15`, pinned Java 5.5.1): Java exposes a public mutable HashSet and `Message.putUserProperty` consults it on each insertion. Rust now exposes a mutable lock-protected global set and observes live additions/removals. The invoked Java fixture and Rust test verify a dynamically added reserved name is rejected and removal of `KEYS` allows the same user-property operation. This closes only the mutable-set behavior; broader Common reverse-surface reconciliation remains open.
- Mutable reserved-set nullable membership and validation precedence (`2026-10-15`, pinned Java 5.5.1): the public Java HashSet accepts null and the empty string; `putUserProperty(null, value)` and `putUserProperty("", value)` hit the reserved check first and throw the base `RuntimeException`. Rust now exposes `HashSet<Option<String>>`, checks the live set before blank validation for both nullable and ordinary APIs, and maps these cases to `ReservedProperty`. The invoked Java fixture and Rust differential verify membership, exception class, mutation restoration, and that ordinary null-name validation still rejects when null is absent from the set. Fixture SHA-256: `cd48ccada3c107facb4ec332316ef29e569cca7c50cd3b0711152b7234cd6b98`. Common all-feature tests, strict Common Clippy, both workspace test configurations, strict workspace Clippy, Maven verify (37 scenarios), comparator/manifest, race/leak and fuzz smoke, release examples, benchmark compilation, metadata, and package listings pass on the current tree. This closes the nullable-membership and precedence slice only; broad P18-V01 remains PARTIAL.
- `Message` priority accessors (`2026-10-15`, pinned Java 5.5.1): the public `setPriority`/`getPriority` pair had no Rust counterpart. Rust now rejects negative setter inputs with a typed error, preserves the property on rejection, and maps absent/malformed/overflow getter inputs to Java's `-1` fallback while accepting Java decimal digits. Invoked Java and Rust fixtures cover the property-map initialization side effect, signed boundaries, malformed inputs and Arabic-Indic digits. This closes the priority accessor slice only; P18-V01 remains PARTIAL.

- Caller-defined `MessageExt` priority overrides (2026-10-21, pinned Java 5.5.1): Rust subtype dispatch now covers virtual `getPriority()` and `setPriority(int)`. When a subtype only overrides `getProperty`, the inherited getter reads that virtual property; the inherited setter retains Java base validation and property storage. The invoked direct-consume Java/Rust fixture also overrides both priority methods, accepts `-7`, and compares getter, property, and dispatch counts. A second invoked fixture checks inherited property lookup, base storage of `17`, and negative-input rejection. `fixtures/runtime/consumer-operations.json` SHA-256: `44eb8359e7df5735cb527a59942190fb74dd4191306a4290ea3bba644a777a5c`. This closes these override paths only; broad P18-V01 remains PARTIAL.

- Request/reply host conversion and `MessageExt` host API (`2026-10-14`, pinned Java 5.5.1): the invoked `ClientRemotingProcessor.processRequest` fixture exercises `NetworkUtil.string2SocketAddress` for valid IPv4/unbracketed IPv6, missing separators, malformed and overflowing ports, Unicode decimal digits, and socket-port boundaries. Rust now applies Java signed-decimal parsing/range behavior and retains a resolved or unresolved `MessageHostAddress` on the reply. The new Java `messageExtHostApi` fixture covers `get/setBornHost`, `get/setStoreHost`, the socket-address constructor, Java host string helpers, IPv4/IPv6 `socketAddress2String`, host-byte methods and their caller-buffer overloads; `message_ext_socket_address_api_matches_java_551` and `reply_host_addresses_match_invoked_java_validation` pass. This closes the host API and malformed reply-host slices only; the broader P18-V01 source/API/input/reverse-export reconciliation remains open. `fixtures/runtime/inbound-routing.json` SHA-256: `614adec3fe0eb41a261000744602f0247aa3ddaf03fdd42fdabf9ee9b8785eb2`; `fixtures/messages/records.json` also includes the invoked host API outcomes.
- Request/reply public message type (`2026-10-16`, pinned Java 5.5.1): `request-reply.json` declares base `Message` for all six Producer request results, `RequestCallback.onSuccess`, and the `RequestResponseFuture` response getter/setter/wait/put methods. Rust now exposes `Message` at those boundaries and retains `MessageExt` only for inbound decoded replies before upcasting. `message_ext_upcasts_to_java_message_surface` and facade signature checks pass; default/all-feature workspace tests, strict all-target/all-feature Clippy, formatting, and diff checks pass for this slice. This closes the request/reply type mismatch only; broad P18-V01 remains open.
- Current-revision P18-V02 server-free gates (`3b0af61d`, 2026-10-16): default and all-feature locked workspace tests, strict all-target/all-feature Clippy, formatting/diff checks, pinned Maven `verify` (37 scenarios, fixture drift and Java/Rust cross-decodes), comparator self-test and 37-scenario manifest, deterministic race/leak smoke (16 cases × 3), fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations), release examples, offline benchmark compilation, locked Cargo/workspace metadata, dependency/license checks, seven package listings with LICENSE/NOTICE, and Asia/Taipei Unique ID fixture all pass. This closes P18-V02 on this revision. Broad P18-V01 and current-revision GitHub Ubuntu/P18-V04 evidence remain open; Phase 19 Docker/Broker automation and release version/registry preflight remain separate open work.
- `MessageExt` metadata and diagnostic helpers (`2026-10-14`): Java `messageExtSystemFlagsApi` invokes topic/group flag get/set, null clearing, empty and malformed values, Unicode decimal parsing, both V6 bit setters, `parseTopicFilterType`, and `toString()`. Rust matches these cases in `message_ext_system_flag_helpers_match_java_551`, reusing the pinned Java integer parser. The diagnostic string is compared with one property; for multiple properties Rust emits deterministic map order because Java `HashMap` iteration order is unspecified. The broad P18-V01 source/API/input/reverse-export reconciliation remains open.
- Java `Message.getProperties()` null-entry enumeration (`2026-10-14`): the invoked `messagePublicApi` fixture records all three nullable system-property entries via the Java map's `entrySet()`. Rust adds `Message::property_entries()` to enumerate present and null-valued rows in deterministic key order while preserving the existing `MessageProperties = BTreeMap<String, String>` API. `message_public_api_matches_java_551_fixture` compares the full rows; focused test passes. This closes only the null-entry visibility slice. P18-V01's broad inherited API, overload/input/control-flow, and Rust reverse-export reconciliation remains open.
- Current Phase 18 server-free rerun (`efabea43`, 2026-10-14): default and all-feature workspace tests, strict all-target/all-feature Clippy, focused Message fixture tests, pinned Maven verify (37 scenarios and cross-decodes), comparator self-test/37-scenario manifest, 16 race/leak cases repeated three times, 1,824-mutation fuzz smoke, release examples, workspace bench compilation, workspace metadata and seven package-content listings all pass. This closes the current local P18-V02 rerun; broad P18-V01, current-revision GitHub Ubuntu CI, Phase 19 Docker/Broker execution and pre-publication registry checks remain open.
- Producer CheckForbidden retry and error parity (`2026-10-14`, pinned Java client 5.5.1): `checkForbiddenRetry` covers two sync calls and Java's terminal `MQClientException`; `checkForbiddenDefaultAsync` proves the default ASYNC path wraps one hook failure as response code 10003 even with three async retries configured; `checkForbiddenExplicitAsync` proves direct kernel ASYNC preserves the hook exception; and `checkForbiddenDeadline` covers total-deadline exhaustion before a second sync hook call. Rust matches those mode-specific outcomes, avoids RPC and send hooks on rejection, updates the reachable-client fault entry when latency-fault mode is enabled, and preserves the typed hook cause. Focused regressions cover all six related Rust test cases. The Producer suite (138 unit tests plus 55 integration targets), strict Producer Clippy, and Maven verify (37 scenarios with fixture drift/cross-decodes) pass. `fixtures/producer/core.json` SHA-256: `d4ef907983f88f7948c4b143fbcd061115cca4ebdc03c19e6acae52e0dc30408`. This closes these bounded error/retry branches only; broad P18-V01 input/control-flow and reverse-export reconciliation remains PARTIAL.
- Producer implementation declaration inventory (`2026-10-14`, pinned Java 5.5.1): `ProducerCoreFixtures` records both public constructors and all 82 declared public methods on `DefaultMQProducerImpl`; `pinned_java_default_producer_impl_public_surface_is_recorded` pins the reflected list/count and selected raw helper/accessor signatures. The current `fixtures/producer/core.json` SHA-256 is `41dd9cca0b559e68397eb2d7bd41769596417ac0d0be605b8cd586423524b1d1`. This makes the Java declaration denominator explicit; direct hook helper methods, mutable route/state accessors, raw runtime handles, and per-overload input/control-flow dispositions still need reconciliation under P18-V01.
- Producer group, VIP, and topics setter reconciliation (`f3d5b44d`, 2026-10-14): nullable group updates reach subsequent send/end-transaction headers and null header fields are omitted; topic getters reflect changes while the started runtime retains the list captured at start; VIP getters reflect post-start mutation while send routing retains the copied startup `ClientConfig`, matching Java's `MQClientManager.getOrCreateMQClientInstance` clone. Recall uses the raw publish address in Java and Rust. Fixture SHA-256: `1ed0060666e8ee2a6b5b82b3088b22cb41a9a690bc291a96e56e51d16e7f7d76`. Shutdown lookup after group mutation is now closed by the separate fixture-backed Producer lifecycle slice below; broad P18-V01 overload/input/branch and reverse-export audits remain open.
- Producer end-transaction hook parity (`52ef36ab`, 2026-10-14): against pinned Java client 5.5.1 source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`, Rust now exposes the hook context, registration/query/direct-execution methods, invokes hooks on local transaction and Broker-check paths, and routes the trace hook through the same ordered list. The fixture compares context values, transaction IDs read from the message, mutable message visibility, order, and continuation after a throwing hook. `fixtures/producer/transaction.json` SHA-256: `69185081811cea48e5b0f3f0f6bc17fd37e7faec6f802a98ec670e4d5d355a47`. The 113-test Producer suite, default and all-feature workspace tests, strict workspace Clippy, Maven verify (37 scenarios), comparator/manifest checks, 16 race/leak cases repeated three times, 1,824-mutation fuzz smoke, release examples, benchmark compilation, format, and diff checks pass on this revision. An initial default-workspace run overlapped the all-feature run and two Pull tests reported `OffsetStore(AlreadyOpen)`; the default suite passed when rerun serially. Broad P18-V01 source/API/input/reverse-export reconciliation remains PARTIAL.
- Latest local Producer revision (`f3d5b44d`, 2026-10-14): default and all-feature workspace tests, strict all-target/all-feature workspace Clippy, formatting/diff checks, pinned Maven verify (37 scenarios, fixture drift and Java/Rust cross-decodes), comparator self-test and 37-scenario manifest validation, 16 race/leak cases repeated three times, 1,824-mutation fuzz smoke, release examples, offline benchmark compilation, and workspace metadata checks pass. This closes the P18-V02 server-free rerun at this revision. No current-revision GitHub Ubuntu workflow result exists; P18-V01 remains PARTIAL.
- Current Producer lifecycle revision `9834d96d` (2026-10-14): sequential default and all-feature workspace tests pass; after a test-only Clippy cleanup, Producer tests (132 unit tests and all integration/doc targets) and Core library tests (156) also pass. Strict all-target/all-feature Clippy, format/diff, pinned Maven verify (37 scenarios, fixture drift and Java/Rust cross-decodes), comparator self-test, 37-scenario manifest, 16 race/leak cases repeated three times, 1,824-mutation fuzz smoke, release examples, offline benchmark compilation, workspace metadata, and package contents pass. Lifecycle fixture SHA-256: `9d5063e8ae538f7da3f957c874dfb95dd81a6a85bf113a947a796de1815001d8`. This closes the current server-free P18-V02 run only; broad P18-V01 remains PARTIAL and there is no current-revision GitHub Ubuntu workflow result.
- Producer runtime fault-strategy settings (`1845c55a`): the invoked Java fixture and Rust differentials cover runtime fault-enable and detector-enable toggles, mutable `long[]` aliasing and replacement for latency/unavailability tables, subsequent Broker selection, null setter/getter behavior, deferred NPE when a null table is read, and AIOOBE only when a matching latency threshold indexes a short duration array. Extra duration entries are ignored. The detector task is created at Producer start, skips probes while its live flag is disabled, and is joined during shutdown. Fixture SHA-256: `2f8c6d40c4669a6a662085e8cddef749338c91fcbb1bf64829ef1f59f77a284e`. This bounded addition does not complete the broader Java reverse-export, inherited/trait API, or source/input/branch reconciliation in P18-V01.
- Prior full local rerun (2026-10-13, `1a741f97`, macOS ARM64, Rust 1.85.0): default and all-feature workspace tests, strict all-target/all-feature Clippy, format/diff checks, Maven verify (37 scenarios and fixture drift/cross-decodes), comparator self-test, 37-scenario manifest, fuzz smoke, 16 deterministic race/leak cases repeated three times, release examples, benchmark compilation, package metadata, and locked Cargo metadata passed. A native Ubuntu 24.04 ARM64 container passed the 14-test Core RPC integration suite. An Ubuntu 24.04 x86_64 container under Docker emulation passed strict Clippy but its workspace test run was inconclusive: three 100 ms loopback/deadline cases failed before any request frame arrived. No GitHub Actions run exists for this local branch; these results do not close P18-V04's Ubuntu CI requirement or P18-V01.
- Latest focused rerun (2026-10-13): `cargo test -p rocketmq-classic-common --locked` and `cargo test -p rocketmq-classic-protocol --locked` pass, including the Heartbeat Fastjson differential over 1086 invoked Java inputs. `mvn -q -f compat/java-reference/pom.xml verify` passes 37 scenarios, fixture drift checks, and Java/Rust cross-decodes. The corpus adds 810 whitespace-position probes and confirms raw U+0000–U+001F in string keys/values is accepted by Java and preserved by the Rust normalizer. Fixture SHA-256: `7ee5f23d02df0033c914e6ea772f133dc29013e1061444a4490ea18e63bdaf6f`. This targeted rerun does not close the broad source/API/input/reverse-export audit.
- Latest local rerun (2026-10-12): Linux ARM64 `cargo test --workspace --locked` passed against the regenerated route fixture; offline Maven `verify` passed with 31 scenarios and fixture drift checks. Workspace all-target/all-feature Clippy, facade example build, comparator self-test, 31-scenario manifest validation, formatting, and `git diff --check` also pass. This does not close the broad audit or platform-runtime items below.
- The public-name/field inventory is complete, but name/signature counts do not prove every overload's input, validation, default, error, and live-configuration branch. Continue the behavior pass in [the API/config map](PHASE_6_API_CONFIG_MAP.md), especially the overload groups and the owner/source branches it marks as evidence-scoped.
- Consumer statistics scope (2026-10-13): Java constructs a distinct `ConsumerStatsManager` for each `MQClientInstance`, even when the instances share the same `clientId`. A Rust regression using two explicit runtime managers reproduced cross-runtime TPS leakage through the previous process-global statistics table (0.4 TPS appeared in the second runtime). Statistics now belong to the owning PushConsumer; the direct Java fixture and `independent_runtime_owners_with_same_client_id_isolate_consumer_stats` verify isolation. This closes that bounded ownership case only; the broad overload/source/input/reverse-export audit remains `PARTIAL`.

- Consumer statistics API and sharing follow-up (2026-10-21): `ConsumerStatsManager` now belongs to `ClientRuntime`, matching the Java `MQClientInstance` boundary. Push owners sharing one runtime receive the same manager `Arc`; separate runtimes with the same client ID retain separate managers. Rust exposes the Java metric update/status methods through the facade, uses Java's `topic@group` stats key, and retains entries because the client manager has no automatic idle cleanup. The pinned Java fixture inventories the constructor and methods. This supersedes the intermediate per-Push ownership recorded above; the broad overload/source/input/reverse-export audit remains `PARTIAL`.
- Consumer statistics constructor surface follow-up (`df5c3c59`, 2026-10-23): removed Rust's public no-scheduler `ConsumerStatsManager::new` and `Default` constructors, which have no pinned Java public counterpart. Standalone callers use the Java-mapped `with_scheduler` constructor; Core creates its runtime-owned manager through a runtime-module-private allocator. PushConsumer has no separate fallback manager before runtime binding, so its public manager getter returns `None` before runtime creation and internal stats updates are skipped until binding. Compile-fail doctests guard both removed constructors and the private allocator. This closes only the constructor-surface parity slice; broad overload/source/input/reverse-export reconciliation remains `PARTIAL`.
- Consumer statistics sampling cadence (2026-10-22, `782a9cdb`): pinned Java creates five `StatsItemSet`s that sample every 10 seconds and 10 minutes on the supplied executor. A new stats item has no snapshots until the shared task visits it; `consumeStatus` therefore reports zero before the first sample, then derives TPS/RT from sampled totals. Failed-message totals stay zero until the first hour-window sample. Rust now registers the matching per-set sample jobs on the client scheduler and also catches up elapsed sampling ticks lazily on metric updates and status reads. The invoked fixture captures all five 10-second samplers and records pull TPS as `0.0` before sampling, `2.0` after sampling, and still `2.0` after a later unsampled increment. Core deterministic tests and consumer flow tests cover these transitions. This closes only the sampling cadence slice; broad overload/source/input/reverse-export reconciliation remains `PARTIAL`.
- The previously reported route collection-state gap is already covered: the invoked Java fixture has 527 DTO inputs, including 128 raw U+0000–U+001F probes across route string values, field names, and filter-map keys/values (current `route.json` SHA-256 `1ac509576845e9f9d78fc4c677702b6c21cf1195d711cb0437dacb48e8e14da1`), a 256-case top-level collection product, a 625-case nested collection product, seven nested-consumer cases, 21 raw-cleanup shapes, 36 route-conversion cases, and nine direct Broker-address-resolution cases. The Rust `route_` test group passes, including both collection products; the converter corpus covers both empty-mapping branches (unmatched writable broker and matching ordinary route), and the runtime address corpus covers every Java branch in publish/admin/subscribe lookup and broker-version selection. These bounded states are closed; the broader method/source/input reconciliation remains open outside those enumerated domains. Heartbeat now has 1086 DTO inputs, including systematic whitespace token/string probes and the earlier escape/comment/duplicate/scalar cases; `heartbeat.json` SHA-256 is `7ee5f23d02df0033c914e6ea772f133dc29013e1061444a4490ea18e63bdaf6f`. Additional method/source branches and Fastjson grammar outside these corpora still need reconciliation. See [the route audit](PHASE_6_ROUTE_AUDIT.md) and [heartbeat audit](PHASE_6_HEARTBEAT_AUDIT.md).
- Linux Rust production interface enumeration now runs against the pinned Linux Java loopback-scope fixture. `cargo test -p rocketmq-classic-core --lib --locked` passed on Linux ARM64 with 130 tests, including `linux_production_enumerator_matches_invoked_java_loopback_scope`; the automatic `ClientConfig::new_auto()` resolver test also passed there. In a Linux ARM64 Docker IPv6-only network namespace, pinned Java 5.5.1 `NetworkUtil.getLocalAddress()` and Rust `ClientConfig::new_auto()` selected the same normalized address, `[fd42:6:0:0:0:0:0:2%eth0]`. The Linux ARM64 candidate sequence is now directly compared in the same namespace: pinned Java and the production Rust enumerator match on a default bridge (3 candidates) and an IPv6-only Docker network with Docker IPv4 disabled (4 candidates). This adds two Linux container topologies, not a real IPv6-only host. macOS production enumeration is covered on the repository host. Windows IP Helper runtime enumeration, non-macOS BSD runtime enumeration, and a real IPv6-only host remain unverified. Windows structure conversion tests and a Windows-target module compile are not runtime evidence.

The runtime client-config oracle was made host-independent on 2026-10-12. Java records whether the no-argument `ClientConfig` address equals `NetworkUtil.getLocalAddress()`, rather than serializing the generating host's IP and interface list. Rust also tests the injected resolver result and exercises the real resolver without asserting another machine's interface order. `fixtures/runtime/client-config.json` SHA-256: `c775a513f9f22b5556ea1acaec3aaa423ee3ddbdb87b0920893938b6a3fa9acb`. This removes the earlier fixed-host-address test failure; it does not close the platform/source-input items above.

Earlier local gate rerun at the 249-case heartbeat DTO checkpoint (2026-10-12, macOS 26.6.1 arm64): `cargo fmt --check`, `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`, `cargo test --workspace --locked`, `cargo build -p rocketmq-classic --examples --locked`, `mvn -f compat/java-reference/pom.xml verify`, comparator self-test, 22-scenario manifest validation, and `git diff --check` all pass. This verifies the current offline workspace and fixture state on this host. It does not provide Windows/BSD runtime enumeration, IPv6-only host, or live Broker/TLS/cross-language evidence.

Earlier full local gate rerun (2026-10-12 03:31 UTC, macOS 26.6.1 arm64) uses the 276-case Heartbeat DTO fixture (`67b844d3a61ee31ac12c340cf09f30d81083435635d6f17bebc44206b65d37cc`). Workspace tests, strict all-target/all-feature Clippy, examples build, offline Maven verify, comparator self-test, 22-scenario manifest validation, formatting, and diff check all passed after the source changes at that checkpoint. The rerun found and fixed one Clippy `collapsible_if` warning in the Fastjson syntax normalizer. See the [gate log](../../rlogs/2026/10/12-phase-00-11-gate-rerun__20261012T033153Z.rlog). These results do not close the broad Phase 6 method/source/input/platform audit described above.

Full local gate rerun after the Phase 7 detector-probe regression (2026-10-12 04:41 UTC, macOS 26.6.1 arm64): `cargo test --workspace --locked`, strict all-target/all-feature Clippy, facade examples build, offline Maven `verify` (including 31 scenarios, fixture drift checks and Java cross-decodes), comparator self-test, 31-scenario manifest validation, formatting, and `git diff --check` all pass. The new scripted `MQClientApi::probe_broker` test passes within the workspace run. These local gates do not close the broad Phase 6 source/API/input audit or Windows/BSD/real-host IPv6 runtime evidence gaps.

Linux runtime-enumeration evidence follow-up (2026-10-12): the Linux ARM64 local run passed `cargo test -p rocketmq-classic-core --lib --locked` with 130 tests, including `linux_production_enumerator_matches_invoked_java_loopback_scope` and the automatic `ClientConfig::new_auto()` resolver test. This supersedes the older dated notes below that say the Rust Linux production enumerator had not run. It closes Linux runtime evidence for the recorded host/loopback fixture only; Windows IP Helper runtime enumeration, non-macOS BSD runtime enumeration, and a real IPv6-only host remain unverified. The broad source/API/input reconciliation remains PARTIAL.

IPv6-only Linux container follow-up (2026-10-12): on Linux ARM64, ran the ignored `automatic_client_ip_constructor_supports_ipv6_only_interfaces` test inside Docker network `rocketmq-p6-ipv6-only` (`fd42:6::/64`, Docker IPv4 disabled). The production `ClientConfig::new_auto()` path returned a bracketed, non-loopback IPv6 value that parsed after applying Java 5.5.1 `NetworkUtil.denormalizeHostAddress` semantics. The first run exposed a test-only parser mismatch: the test attempted to parse Java's bracketed IPv6 form directly. It now strips the required brackets and then the interface scope before parsing; the targeted test passes (1/1) with Rust 1.85.0. This verifies the production Linux path in an IPv6-only container, not a real IPv6-only host or a same-interface Java/Rust selection comparison; Windows/BSD runtime enumeration and broad source/API/input reconciliation remain open.

Public result/route/config field inventory (`2026-10-10`): reverse-scanned 22 core API/runtime/route/config source files. Nine named-field public structs expose 74 public fields, and exact Rust field tokens now all appear in the API map. `ClientConfig`, `SendResult`, pull/pop/ack results, heartbeat V2 and route snapshot projections are included. The scan checks field-name completeness only, not behavior parity for each field; broad source/input/platform reconciliation remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

Pinned Java runtime public-method inventory (`2026-10-10`): `javap -public` on the seven classes listed in the Phase 6 child plan finds 169 public method declarations and no method name missing from the API map. This is a name check, not proof that every overload or protected/private source branch is behaviorally covered. Whole Phase 6 source/input/platform reconciliation remains PARTIAL; P6-V04 is still `PENDING_PHASE_19`.

Change-invisibility `AckResult` field mapping (`4da236e1`, 2026-10-10): bytecode inspection confirmed Java's `changeInvisibleTimeAsync` sets status and pop time, then builds `extraInfo` with `ExtraInfoUtil` using retry-topic classification, broker name, response metadata and the request offset. Rust now accepts broker name and returns the same `extra_info` string for ordinary, classic retry and retry-V2 topics; its direct helper test and Java invoked oracle cover all three. The existing callback differential verifies success and malformed numeric defaults. Focused extension, POP/ACK, RPC and full offline Maven tests pass. This closes that callback result field only. Other Pull/POP results remain at the raw RPC decoding boundary until their consumer owner phases; broad Phase 6 public-struct, source/input/platform reconciliation remains PARTIAL, and P6-V04 remains `PENDING_PHASE_19`.

Producer result public-field reconciliation (`b2a00689`, 2026-10-10): pinned Java 5.5.1 `SendResult` exposes `rawRespBody` as a nullable byte array and has no `batchUniqId` accessor. Rust now represents raw response bytes as `Option<Vec<u8>>`, initializes it to `None` on send results, and removes the unrelated `batch_uniq_id` from `SendResult`; that field remains on `SendMessageResponseHeader`. The invoked Java fixture directly checks the ordinary and batch send defaults, and Rust loopback/differential tests assert `None`. Producer tests and offline Maven verify (22 scenarios) pass. This closes the `SendResult` field boundary only; full public-struct and whole Phase 6 source/API/input/platform reconciliation remains PARTIAL, and P6-V04 remains `PENDING_PHASE_19`.

Public enum variant inventory (`2026-10-10`): scanned all 17 public enum declarations under the core crate, totaling 72 declared variants including the `#[cfg(test)]` startup-failure hook. The map now lists the exact Rust variants for AccessChannel and its lookup errors, Pull/Pop/Ack/Send status and lookup errors, SendKind/SendMode, RuntimeState, ConsumerOperation, RuntimeError, and RpcError. Existing direct Java enum/RPC fixtures, lifecycle tests, and inbound matrix evidence cover their behavior and ownership; Rust-only selectors/errors are identified as such rather than presented as Java enum classes. This closes variant-name documentation, not every error/input source branch; full Phase 6 reconciliation remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

ClientConfig error-result reconciliation (`89e23102`, `c48db47d`, 2026-10-10): reverse export review found public `ConfigError` missing from the API map. The map now names all four source-backed variants and their Java call sites/outcomes: invalid instance endpoint (`StringIndexOutOfBoundsException`), null instance name (`NullPointerException`), and null queue collection/element (`NullPointerException` at Java's lazy traversal boundary). The unused `LocalAddress` variant was removed because Java catches automatic address-discovery failure and retains null `clientIP`; a direct Java 5.5.1 fixture now verifies `changeInstanceNameToPID(null)` and Rust compares the typed `NullInstanceName` result. Focused config regression and full pinned offline Maven verify (22 scenarios, fixture checks and cross-decodes) pass; `client-config.json` SHA-256: `eb4028b6c5ce5cfce6e86ae0ec08110200c4d8bcebf67bcc2854b2e929d87ff0`. This closes ConfigError variant documentation and the null-PID branch only; full Phase 6 reconciliation remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

Nullable raw route-map callback arguments (`7fafce29`, 2026-10-10): pinned Java map calls with null functions for `forEach`, `replaceAll`, `computeIfAbsent`, `computeIfPresent`, `compute`, and `merge` each throw NPE before traversal or mutation. Rust adds nullable callback adapters to those six raw-route methods, returning `InvalidRoute("null topic route callback")`; the differential confirms the mapping remains. Seven outcomes raise `runtimeTopicRouteTable.primitives` from 142 to 149; `route.json` SHA-256 is `2be7e35eea6de3a0b16eb04d18eb39e579dd8ad359002760c12e6bca0bbb84e9`. Full locked workspace tests, strict Clippy, pinned offline Maven verify (22 scenarios and cross-decodes), comparator self-test/manifest22, formatting and diff checks pass. This closes these null callback inputs only; whole Phase 6 source/API/input/platform reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Nullable raw route-view `removeIf` predicates (`db33a61e`, 2026-10-10): pinned Java `ConcurrentHashMap` key, values, and entry views each throw NPE for `removeIf(null)` before traversal or mutation. Rust adds `remove_if_nullable` to all three route views, returning typed `InvalidRoute("null topic route predicate")`; the differential confirms the existing mapping remains. Four outcomes raise `runtimeTopicRouteTable.primitives` from 138 to 142; `route.json` SHA-256 is `99c9efe5783c7b7d91f20d762b16e869a0e93f819315e8a90a8cb01d2acc7103`. Full locked workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting and diff checks pass. This closes this callback-null input only; full Phase 6 source/API/input/platform reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Linux IPv6 loopback scope parser oracle (2026-10-10): invoked the pinned RocketMQ 5.5.1 `NetworkUtil.getLocalInetAddressList()` on Linux/OpenJDK 8 and captured the `lo` `::1%lo` candidate, scope ID 1, and its `/proc/net/if_inet6` row. `RuntimeLinuxAddressFixtures --verify` reproduces the pinned Java result; Rust's Linux row parser and host-address normalizer match that row in `linux_loopback_proc_scope_matches_invoked_java_551`. Fixture SHA-256: `3f064eed94e36753c4de9f4a9025f5cf1cd4c75041dfa5cd654c0171064a0a4b`. This is a Linux Java observation and parser regression only; the Linux Rust production enumerator did not execute on Linux, so Linux runtime enumeration, Windows runtime enumeration, non-macOS BSD execution, and a real IPv6-only host remain unverified. Full Phase 6 source/API/input/platform reconciliation remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.

Nullable raw route view bulk collections (`78da092c`, 2026-10-10): Java `removeAll(null)` and `retainAll(null)` on keySet, values, and entrySet each throw NPE before mutation. Rust adds all six nullable live-view adapters and maps null collections to typed `InvalidRoute("null topic route collection")`; the differential verifies the original mapping remains. Six exception outcomes plus the retained-mapping outcome raise `runtimeTopicRouteTable.primitives` from 131 to 138. `route.json` SHA-256 is `947f53b3743fb46c264856b5912d7cf3f01583efa9570ba5eb83d99627e41743`. Focused and full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Nullable raw route collection-view arguments (`8a0dedb0`, 2026-10-10): Java key-view null `contains` / `remove` and values-view null `contains` throw NPE; values-view `remove(null)` returns false. Entry-view operations return false for a null-key or null-value entry. Rust nullable adapters match, and the differential verifies the map is retained. Nine outcomes raise `runtimeTopicRouteTable.primitives` from 122 to 131; `route.json` SHA-256 is `625af93e11f18a2391bce16cf2f1188e7e3fc66f2c8988da420d49550ea634ba`. Focused and full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Nullable raw route remapping arguments (`91edf0da`, 2026-10-10): pinned Java null keys for `computeIfAbsent`, `computeIfPresent`, `compute`, and `merge` throw `NullPointerException`; a null merge value does as well. All fail before callbacks and keep the existing mapping. Rust nullable adapters return typed errors. Seven outcomes raise `runtimeTopicRouteTable.primitives` from 115 to 122; `route.json` SHA-256 is `9a4c8a5f5fa0f8cfdb42c0d620d4e8d470fdfc25535b7b1d78e9d39bdde1d0c6`. Focused and full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Nullable raw route map arguments (`dcf1c85b`, 2026-10-10): pinned Java null inputs to `containsValue`, `put`, `putIfAbsent`, and both `replace` overloads throw `NullPointerException`; `remove(existingKey, null)` returns false. Rust nullable adapters match and the fixture verifies one retained mapping after all calls. Twelve outcomes raise `runtimeTopicRouteTable.primitives` from 103 to 115; `route.json` SHA-256 is `0dd69fb5f280f8c7d1721f6275a01da810d87a55576aacc1517f9291d0db5923`. Focused and full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Raw route nullable `containsKey` and `remove` keys (`bd33f32d`, 2026-10-10): pinned Java `ConcurrentHashMap.containsKey(null)` and `remove(null)` both throw `NullPointerException` before modifying the route table. Rust adds nullable-key counterparts returning typed `InvalidRoute("null topic route key")`; four fixture outcomes verify both errors and retained mappings, increasing `runtimeTopicRouteTable.primitives` from 99 to 103. `route.json` SHA-256 is `d32ce77c95736db747c5302041eb840ebf965e449418b8f5fbcb0ccd3f2153f8`. Focused and full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Raw route nullable `get` key (`7f33ecba`, 2026-10-10): pinned Java `ConcurrentHashMap.get(null)` throws `NullPointerException`; Rust adds `TopicRouteTable::get_nullable`, which maps `None` to typed `InvalidRoute("null topic route key")`. The direct Java/Rust primitive differential increases `runtimeTopicRouteTable.primitives` from 98 to 99; `route.json` SHA-256 is `cffedc220092c4ad3dc0648feb88fbd2ac416f7bcd561d7bb2a0760b7304e75f`. Focused and full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Nullable raw route `getOrDefault` default and key (`a7d4b3e0`, 2026-10-10): pinned Java `ConcurrentHashMap.getOrDefault` returns the mapped route for a present key even with a null default, returns null for an absent key and null default, and throws `NullPointerException` for a null key. Rust adds `TopicRouteTable::get_or_default_nullable`, preserves present/absent behavior, and returns typed `InvalidRoute("null topic route key")` for a null key. Three outcomes raise `runtimeTopicRouteTable.primitives` from 95 to 98; `route.json` SHA-256 is `451afcb6ca7fab4d332dc4a95f07afc6138c311a96b6623e555246d998c53ffd`. Focused and full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Nullable raw route `replaceAll` results (`c179246f`, 2026-10-10): the pinned Java `ConcurrentHashMap.replaceAll` visits its observed route order, updates the first entry, throws `NullPointerException` when the second callback result is null, leaves that entry unchanged, and skips the remainder. Rust adds `replace_all_with_nullable_results`, returns typed `InvalidRoute`, and preserves the prefix update; non-null `replace_all` forwards through the same retry path. The test captures Java and Rust traversal independently because CHM iteration order is unspecified, then checks prefix/failure/suffix effects relative to each order. Three outcomes raise `runtimeTopicRouteTable.primitives` from 92 to 95; `route.json` SHA-256 is `2779d3c69caa8c7e03a4b096aa1bbb447c65735a65f55a52998b142d05c0e525`. Focused and full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Raw route entry `setValue(null)` (`b4101e0b`, 2026-10-10): the pinned Java raw-map entry throws `NullPointerException` before updating either the entry snapshot or backing mapping. Rust adds `TopicRouteEntryHandle::set_value_nullable(None)` and reports `InvalidRoute("null topic route value")`; the fixture-backed differential verifies the same previous value through both views. Three boolean outcomes increase `runtimeTopicRouteTable.primitives` from 89 to 92; `route.json` SHA-256 is `e6e229c060801b2507e99c85ee4716480c58403d334e2d3338a4abcd7130b789`. Focused and full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, route fixture drift, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Raw route null-source `putAll` (`3e708ca6`, 2026-10-10): direct pinned Java `ConcurrentHashMap.putAll(null)` throws `NullPointerException` before any source traversal and preserves the original mappings. Rust's `put_all_nullable_rows(None)` returns `InvalidRoute("null topic route putAll map")`; its differential compares the retained key set. Two outcomes increase `runtimeTopicRouteTable.primitives` from 87 to 89; `route.json` SHA-256 is `efb0b0baa346358bb5dfa4f18cd631d069967444c1f64884efd86cc43dee0fb0`. Focused and full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Raw route null-key nullable `putAll` (`88ccdd55`, 2026-10-10): a pinned Java `TreeMap` with a null-tolerant comparator visits `a`, `null`, `z`; `ConcurrentHashMap.putAll` throws `NullPointerException` at the null key, keeps the first route, and skips the last. Rust's nullable iterator accepts the same ordered rows, maps the null key to typed `InvalidRoute`, and preserves the earlier write. Three primitive outcomes raise `runtimeTopicRouteTable.primitives` from 84 to 87; `route.json` SHA-256 is `e0986004e2f019366d2deca75f670b0c7a7c395894b24b1ef42857b353d45b92`. The focused differential, full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 source/API/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Nullable raw route `putAll` source iteration (`5e1df2e4`, 2026-10-10): the pinned Java 5.5.1 `TreeMap` source visits `a-route-put-all-before-null-value`, `m-route-put-all-null-value`, and `z-route-put-all-after-null-value` in order. `ConcurrentHashMap.putAll` throws `NullPointerException` at the null route value, retains the first mapping, and does not apply the last. Rust adds generic `TopicRouteTable::put_all_nullable_iter` plus the `put_all_nullable_rows` slice adapter and matches the failure and post-call keys using a `BTreeMap` iterator. Three boolean primitive outcomes raise `runtimeTopicRouteTable.primitives` from 81 to 84; `route.json` SHA-256 is `9e26380877df8f38523baea0a5706de99303efa058f24f7296c0bc2afb7fc306`. Focused route-table differential, full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Broader Phase 6 source/API/input/platform reconciliation remains open; P6-V04 remains a Phase 19 handoff.

Nullable source-map `putAll` traversal (`3a8e9cf9`, 2026-10-10): the pinned Java 5.5.1 `TreeMap` fixture builds keys `a-put-all-before-null-value`, `m-put-all-null-value`, and `z-put-all-after-null-value`; `ConcurrentHashMap.putAll` visits that order, throws `NullPointerException` at the null value, keeps the first row, and leaves the last unapplied. The Rust `put_all_nullable_iter` methods accept any iterator of nullable row tuples; the prior slice APIs forward to the generic path. BTreeMap-backed factory, producer, and consumer differentials match Java's partial writes. The fixture adds 12 outcomes across three tables; `client-config.json` SHA-256 is `fd34d154ef813646235c850ad21318c33e1052d31926db36cc2fa70429827d99`. Full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture verification and Java cross-decodes), comparator self-test/manifest22, format, and diff checks pass. This closes nullable iterator input shape for these three tables; whole Phase 6 source/API/input/platform reconciliation remains open and P6-V04 stays a Phase 19 handoff.

Raw route reentrant `clear` inside compute (2026-10-10): the pinned Java 5.5.1 fixture invokes `ConcurrentHashMap.clear()` from a same-key `compute` callback. Clear returns normally and the callback observes the mapping absent before returning its replacement; `compute` returns that replacement while the mapping remains absent. Rust now bypasses only a key gate already owned by the current thread during `TopicRouteTable::clear`; cross-thread clear still waits for the active compute. Four fixture outcomes raise `runtimeTopicRouteTable.primitives` from 77 to 81; `route.json` SHA-256 is `1eb5f057f1a1855674fddda7313ec63874bbf3462228de03d8d03da50189803e`. Focused and full locked workspace/all-target tests, strict workspace/all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, route fixture verification, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Broader route malformed-input and whole Phase 6 source/API/input/platform reconciliation remain open; P6-V04 stays a Phase 19 handoff.

Raw route `clear` ordering with compute (`4182fef0`, 2026-10-10): the pinned Java 5.5.1 race blocks a same-key `compute` callback, starts `ConcurrentHashMap.clear()`, then releases the callback. Java's clear waits; compute runs once and returns its value, after which the computed mapping is absent. The initial Rust differential failed because `TopicRouteTable::clear` bypassed topic gates and returned during the callback. Rust now snapshots the visible keys and removes them one at a time under the same per-topic gates used by remapping and route publication, preserving weak concurrent-map rather than whole-table transaction semantics. The expanded `runtimeTopicRouteTable.primitives` fixture has 77 outcomes; `route.json` SHA-256: `f2c35bdda057ec82d400af935b9c16d91a62ec987e9e7e8a8a7b545fb84ea341`. The focused raw-table differential, full locked workspace/all-target suite, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, route fixture drift checks, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Broader route malformed-input and whole Phase 6 source/API/input/platform reconciliation remain open; P6-V04 stays a Phase 19 handoff.

Generic `putAll` source-map API (`715c1dee`, 2026-10-10): Java's public `Map.putAll` accepts arbitrary Map implementations, while the Rust tables had been restricted to `BTreeMap`. `FactoryTable`, producer/consumer registry tables, and `TopicRouteTable` now accept `IntoIterator` key/value rows and apply each row in source iteration order. HashMap-backed regressions cover all four table families; existing nullable ordered-row adapters retain partial writes at null failures. Manager-table (17 tests), registry-table (19 tests), route-table (6 tests), full locked workspace/all-target tests, strict workspace/all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. A full `cargo check -p rocketmq-classic-core --tests --target x86_64-pc-windows-gnu --locked` attempt stops before checking core because the host lacks `x86_64-w64-mingw32-gcc`, required by `aws-lc-sys`; the earlier isolated Windows address-module type-check is narrower and does not prove whole-core Windows compilation. Windows runtime enumeration and broader Phase 6 source/API/input/platform reconciliation remain open; P6-V04 stays a Phase 19 handoff.

Nullable `putAll` HashMap source traversal (`6391d854`, 2026-10-10): pinned Java 5.5.1 fixtures for factory, producer, and consumer tables record `HashMap` iteration across buckets 2, 5, and 9. A null value on the second visited row throws `NullPointerException`; the earlier valid mapping persists and the final row is not applied. Rust's ordered nullable-row adapter receives that captured order and matches the resulting keys. Fixture SHA-256: `437c23de4e128513fcf91e211d231d4fc5691eec1b6314458c8b5916c4a64126`. Manager-table (17 tests), registry-table (19 tests), locked workspace/all-target tests with local socket access, strict workspace/all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, runtime fixture drift checks, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. One initial full-suite run exposed an assertion that compared unordered Rust snapshot keys directly to sorted fixture keys; sorting the Rust keys fixed the test, and the full suite then passed. Other source-map traversal variants and broader Phase 6 source/API/input/reverse-export reconciliation remain open; P6-V04 stays a Phase 19 handoff.

Nullable runtime creation configuration (`5ec79e92`, 2026-10-10): direct pinned Java 5.5.1 calls to both `getOrCreateMQClientInstance` overloads with a null config throw `NullPointerException`, including the overload with a null hook. Rust adds nullable config adapters for both forms; invalid calls return typed `InvalidRoute` without publishing a factory-table entry, while valid optional configs continue to reuse one runtime. Fixture SHA-256: `72749d6fb0b3a3a28a2d78ba70eaab17135012831d38b9e230a5c3f8d1bd034c`. Manager-table (16 tests), registry-table (18 tests), full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Broader Phase 6 source/API/input/reverse-export reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Nullable `replaceAll` results (`2cf01322`, 2026-10-10): pinned Java 5.5.1 probes make the first visited key return a replacement and the next return null. `replaceAll` throws `NullPointerException`; the first update persists and the null-result entry retains its original mapping on factory, producer, and consumer tables. Rust exposes `replace_all_with_nullable_results`, maps null callback results to typed `InvalidRoute`, and preserves writes before failure. Java records its CHM traversal order (`a`, `b` in this fixture); Rust factory snapshots can choose another order, which is allowed by the Java map contract, so regressions assert partial updates relative to the actual visit sequence. Fixture SHA-256: `268b67ceae2e2cb5026a5a91f5af939efca78be5d9f58a89394707549f554d19`. Manager-table (15 tests), registry-table (18 tests), full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Broader Phase 6 source/API/input/reverse-export reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Nullable live-table entry values (`80d34846`, 2026-10-10): pinned Java 5.5.1 `ConcurrentHashMap` entry probes confirm `setValue(null)` throws `NullPointerException` for factory, producer, and consumer tables. Each entry snapshot and mapped value remains unchanged after failure. Rust adds nullable `set_value_nullable` adapters with typed `InvalidRoute` errors and fixture-backed identity assertions. Fixture SHA-256: `2a7e7c96c8eb449f172a4f3e2a4ebd392de1b9f6b38349f10d6d1d1276725244`. Manager-table (14 tests), registry-table (17 tests), the full locked workspace/all-target suite, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. The first full-suite run had one unrelated transport callback-order failure; the focused test passed and the full suite passed on retry. Other Phase 6 source/API/input/reverse-export reconciliation and source-map traversal variants remain open; P6-V04 stays a Phase 19 handoff.

Nullable `putAll` source rows (`fd4fc086`, 2026-10-10): pinned Java 5.5.1 `LinkedHashMap` probes place a valid row before a null key/value and another valid row after it. `putAll` throws `NullPointerException`, retains the earlier write, and never applies the later row. Rust adds an ordered nullable-row adapter for all three table families, mapping null key/value failures to typed `InvalidRoute` without rolling back earlier writes. `client-config.json` SHA-256: `fae76c809c567ea32cc432c98943286e87b7cbb55db5ec0faf63dcb306d59d07`. Manager-table (14 tests), registry-table (16 tests), locked workspace/all-target tests with local socket access, strict workspace/all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. This covers ordered source-map rows only; arbitrary source-map iteration and broader Phase 6 source/API/input/reverse-export reconciliation remain under review, with P6-V04 a Phase 19 handoff.

Nullable live-table bulk inputs (`dbd2920d`, 2026-10-10): direct pinned Java 5.5.1 probes show `putAll(null)`, `forEach(null)`, and `replaceAll(null)` throw `NullPointerException` for the factory, producer, and consumer tables. Rust adds nullable source-map and callback adapters that map those failures to typed `InvalidRoute`; regressions verify invalid calls preserve mappings and valid maps/callbacks are forwarded. `client-config.json` SHA-256: `ee369565a2fab6abb99261cc46f74f442fd4ddaf7c69a3f92316f19fbc79d66a`. Manager-table (14 tests), registry-table (16 tests), locked workspace/all-target tests with local socket access, strict workspace/all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Ordered null-key/value rows in non-null source maps are covered by the newer audit entry above; other source-map traversal variants and broader Phase 6 source/API/input/reverse-export reconciliation remain open. P6-V04 remains a Phase 19 handoff.

Nullable live-table view collection elements (`35c73c00`, 2026-10-10): pinned Java 5.5.1 probes exercise `removeAll` and `retainAll` with null-only lists, null before/after a matching member, and null plus a match on factory, producer, and consumer key/value/entry views. Key-view `removeAll` throws `NullPointerException` on null and retains earlier removals when the matching key precedes null; values/entry `removeAll` ignore null; null is a nonmatch for all `retainAll` views. Rust adds element-aware adapters, typed null-key errors, and order-preserving partial mutation for key removal. The fixture maps are serialized deterministically after replacing unordered `Map.of` containers with `TreeMap`. `client-config.json` SHA-256: `08c47a329a57317a88d2dc84e6fe781cea4a1a0cf38bff4caeece60958219f51`. Manager-table (13 tests), registry-table (15 tests), locked workspace/all-target tests with local socket access, strict workspace/all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift, and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. This closes the probed collection-element cases only; broader source/API/input/reverse-export reconciliation remains PARTIAL, and P6-V04 remains a Phase 19 handoff.

Nullable live-table remapping inputs (`e9773251`, 2026-10-10): direct pinned Java 5.5.1 probes show that `computeIfAbsent`, `computeIfPresent`, `compute`, and `merge` throw `NullPointerException` for each null key, required value, or remapping function tested on the factory, producer, and consumer tables (nine input positions per table family). Rust adds nullable adapters for all four operations and typed `InvalidRoute` mappings; valid callback forwarding and unchanged state after invalid inputs are covered. `client-config.json` SHA-256: `43c64a75c7bcb93d2ff3d4ecb577c28ef5fc40d3b48365c66b522db5764adead`. Manager-table (12 tests), registry-table (14 tests), locked workspace/all-target tests with local socket access, strict workspace/all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. This covers nullable remapping inputs only; null elements inside non-null collection inputs, other map/view surfaces, full Phase 6 source/API/input/reverse-export reconciliation, and P6-V04 remain open (`P6-V04` is a Phase 19 handoff).

Nullable live-table view inputs (`2dd1ad82`, 2026-10-10): direct pinned Java 5.5.1 probes on the factory, producer, and consumer table views show key-set `contains(null)`/`remove(null)` and values `contains(null)` throw `NullPointerException`; values `remove(null)` returns false; entry-set null-entry membership/removal and entries with null key or value return false; null `removeIf` predicates and null `removeAll`/`retainAll` collection references throw `NullPointerException`. Rust adds nullable adapters on all three table-view families, mapping throwing boundaries to typed `InvalidRoute` and preserving false outcomes. `client-config.json` SHA-256: `59e055cf4285877bd25b77fa0f09596b4ce0e251b8351fa2c5aa3cddb519f8ab`. Manager-table (11 tests), registry-table (13 tests), full locked workspace/all-target tests with local socket access, strict workspace/all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. This covers null inputs and null collection references on these three view families; collections containing null elements, other map/view surfaces, full Phase 6 source/API/input/reverse-export reconciliation, and P6-V04 remain open.

Nullable live-table mutations (`46ab9111`, 2026-10-10): direct pinned Java 5.5.1 calls on `MQClientManager.getFactoryTable()`, `getProducerTable()`, and `getConsumerTable()` show that `put`, `putIfAbsent`, both `replace` overloads, and both `remove` overloads throw `NullPointerException` for null keys; put/putIfAbsent and replace overloads also throw for null required values, including either expected or replacement values for conditional replace. The notable exception is conditional `remove(key, null)`, which returns false. Rust adds nullable wrappers for those live-table mutation primitives; null keys and required null values return typed `InvalidRoute`, while nullable conditional-remove values return `Ok(false)`. `client-config.json` SHA-256: `429fa8f06d1543507a8a6eb7a6b833d239c11cea9eead41b4354bd04e645064c`. The manager-table suite (10 tests), registry-table suite (12 tests), full locked workspace/all-target tests with loopback access, strict workspace/all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. This closes direct null-key/value boundaries for these primitive mutations only; compute/merge/view input boundaries, broader Phase 6 source/API/input/reverse-export reconciliation, and P6-V04 remain open.

Nullable table values and `getOrDefault` fallback (`8483bc6f`, 2026-10-10): invoked Java 5.5.1 calls show `containsValue(null)` throws `NullPointerException` and `getOrDefault(key, null)` returns the mapped value for a hit and null for a miss on the manager, producer, and consumer tables. Rust adds `contains_value_nullable(Option<&Arc<_>>)` and represents a nullable fallback/result in `get_or_default_nullable`; null values map to typed `InvalidRoute`, while `Some(key), None` returns the exact mapping or `None`. `client-config.json` SHA-256: `ee610882368960ea764649c26fa6a9154046f480cf6a6615e45c2c8e5ca60683`. Focused nullable table tests, both table suites (20 total), full locked workspace/all-target tests with local socket access, strict workspace/all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. This closes null-value `containsValue` and null default results for those map reads only; remaining mutation/view input boundaries, broader Phase 6 source/API/input/reverse-export reconciliation, and P6-V04 remain open.

Nullable runtime table read keys (`440ce388`, 2026-10-10): invoked Java 5.5.1 calls show `get(null)`, `containsKey(null)`, and `getOrDefault(null, default)` each throw `NullPointerException` on `MQClientManager.getFactoryTable()`, `MQClientInstance.getProducerTable()`, and `getConsumerTable()`. Rust adds `contains_key_nullable` and `get_or_default_nullable` beside the existing `get_nullable` adapters on those live tables; null keys produce typed `InvalidRoute`, while non-null missing keys preserve false/None/default behavior. `client-config.json` SHA-256: `10ee4111901faff32dc956a58bf9bfd69745320629b296a7cf73c883e7d71cb7`. The full manager-table (9) and registry-table (11) suites, full locked workspace/all-target tests with local socket access, strict workspace/all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. This closes these three nullable read methods only; other table mutation inputs, whole Phase 6 source/API/input/reverse-export reconciliation, and P6-V04 remain open.

Windows address-table candidate pipeline coverage (`47e85563`, 2026-10-10): extracted the production Windows table-processing algorithm behind host-testable records for `GetIfTable2`, unicast/anycast address tables, and `GetIfEntry2` state snapshots. `windows_interface_candidate_pipeline_joins_filters_and_orders_tables` exercises LUID joins, missing rows, down and point-to-point exclusion, Preferred/Deprecated DAD acceptance, anycast/unicast and interface ordering, IPv4-only filtering, and scoped-address/name conversion. All 18 focused local-address tests pass; the exact module and tests pass an isolated Windows-target type-check. The full locked workspace/all-target suite also passes with local socket access for its loopback gates, as do strict workspace/all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks. This verifies the combined projection/join/filter/order algorithm with synthetic rows, not actual Windows IP Helper enumeration. Windows runtime enumeration, non-macOS Unix ordering, a real IPv6-only host, broader Phase 6 reconciliation, and P6-V04 remain open.

Nullable identity-removal instance input (`b2951a58` Java oracle / `8c62a7d9` Rust update and regression, 2026-10-10): pinned Java `removeClientFactory(id, null)` returns normally and preserves the mapping, whereas `removeClientFactory(null, null)` throws `NullPointerException` at the null key. Rust's identity-aware nullable adapter accepts an optional instance, returns `false` for a null instance and valid ID, and returns typed `InvalidRoute` for a null ID before considering the instance. `client-config.json` SHA-256: `a1e8747165443ca0dbac9bc2f998042507ade61d9173d37cb66ca7b13f8b2238`. Focused regression, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes the nullable instance input for identity-aware removal only; full Phase 6 source/API/input/reverse-export reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Nullable manager factory removal (`33eaeafc` Java oracle / `42ecbba5` Rust implementation and regression, 2026-10-10): pinned Java `removeClientFactory(null)` and `removeClientFactory(null, instance)` calls both throw `NullPointerException`. Rust adds nullable adapters for the unconditional and identity-aware overloads and maps a null ID to typed `InvalidRoute`; the fixture-backed test confirms the preexisting runtime remains mapped. `client-config.json` SHA-256: `d07554187630871a570e4b26eb68963510eccc492e7933d049a7c3182ee356c2`. Focused regression, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes null-ID behavior for the two manager removal overloads only; full Phase 6 source/API/input/reverse-export reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Nullable `FactoryTable.get` (`83cb6584` Java oracle / `e927ab94` Rust implementation and regression, 2026-10-10): the pinned `MQClientManager.getFactoryTable().get(null)` call throws `NullPointerException`. Rust adds `FactoryTable::get_nullable`, mapping null to typed `InvalidRoute`; the regression confirms a missing non-null key still returns `None`. `client-config.json` SHA-256: `876a923aa55f58d249e62e19286b9e80926ead6593c00cad22a51357306a3c1c`. Focused regression, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes null-key `get` only; full Phase 6 source/API/input/reverse-export reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Nullable producer/consumer table reads (`b2975140` Java oracle / `a2a97161` Rust implementation and regression, 2026-10-10): direct pinned `getProducerTable().get(null)` and `getConsumerTable().get(null)` calls both throw `NullPointerException`. Rust adds `ProducerTable::get_nullable` and `ConsumerTable::get_nullable`, which map null keys to typed `InvalidRoute`; the test confirms non-null missing keys still return `None` and the runtime remains Created. `client-config.json` SHA-256: `f65b145c91889231da58de9fab635872196f28cdba33cbe2c68913785cceae82`. Focused regression, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes the null-key `get` boundary only; full Phase 6 source/API/input/reverse-export reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Nullable runtime selectors (`cb5c4b80` Java oracle / `4394a4c5` Rust implementation and regression, 2026-10-10): direct pinned `MQClientInstance.selectProducer(null)` and `selectConsumer(null)` calls throw `NullPointerException` through the producer/consumer `ConcurrentHashMap` lookups. Rust adds `select_producer_nullable` and `select_consumer_nullable`, which map null groups to typed `InvalidRoute`; the fixture-backed regression also confirms non-null absent lookups remain `None` and runtime state stays Created. `client-config.json` SHA-256: `408db9671aeb2731146244e105f0e76efc21fdef01ad772ce55bd9131d3e035f`. Focused regression, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes only null-key behavior for these two selectors; full Phase 6 source/API/input/reverse-export reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Nullable registration and group removal (`50b83107` oracle / `9ea85053` implementation and regression, 2026-10-10): pinned Java 5.5.1 returns `false` when consumer/producer registration receives a null group or owner, and throws `NullPointerException` for null consumer/producer unregister keys before issuing Broker RPC. Rust's nullable registration methods return `Ok(None)` and nullable group removals map the null key to typed `InvalidRoute`; the fixture-backed test also verifies the runtime remains Created. `lookup.json` SHA-256: `de2d3104c034955172e04f2ea73bd67f1981ad27664b2fe413b0ac98c18835b6`. Focused registration regression, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes only null registration/unregister boundaries. Full Phase 6 source/API/input/reverse-export reconciliation remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.

Empty-topic runtime lookup (`dd9f15ff` / `cb7ce626`, 2026-10-10): actual pinned Java `MQClientInstance.findConsumerIdList("", "consumer")` treats the empty topic as a valid route-table key, refreshes that topic once, resolves `broker:10911`, and forwards the consumer group; `queryAssignment("", ...)` reuses the cached route and forwards the empty topic. The oracle records consumer IDs `b,a,b`, an empty assignment Set, and `refreshTopics=[""]`. Rust's `empty_topic_uses_normal_route_refresh_and_broker_lookup_flow` loopback verifies the NameServer route header carries `topic=""`, then checks the consumer-list request and query-assignment body reach the Broker and decode successfully. `lookup.json` SHA-256: `fe7d8781b68bfa52408aea98aa6c903b86dbd100e6a724841de891d19db1ef92`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes empty-topic routing for these two lookup paths only; full Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

Empty-string query-assignment lookup fields (`9a264e0b` / `2aae44ac`, 2026-10-10): the actual pinned Java `MQClientInstance.queryAssignment(topic, "", "", BROADCASTING, timeout)` forwards empty group and strategy strings. The actual `MQClientAPIImpl` invocation sends request code 400 with `consumerGroup` and `strategyName` present as empty strings and `messageModel` as `BROADCASTING`; the empty successful assignment response decodes to an empty Set. Rust's nullable entry point retains `Some("")` through body serialization. `empty_assignment_fields_match_java_request_body` checks the Java runtime/API observations and loopback wire body/result. `lookup.json` SHA-256: `5ea388cd3b1f87f768d0f7e9de20f710faa7657ee4cee745fed51a6624fafd88`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. Correction: the preceding 2026-10-10 15:22 session log called the request code 301; the pinned invocation records code 400. This closes the empty-string assignment field boundary only; full Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

Empty consumer-list group field (`e26b1449` oracle, corrected by `a33ac3fe`; Rust regression `a8e8b6a9`, 2026-10-10): actual pinned Java `MQClientInstance.findConsumerIdList(topic, "")` forwards the empty group to `MQClientAPIImpl`; Java's custom-header materialization includes `consumerGroup=""` in request code 38. The null-group path still omits the field. The first proxy observation read `extFields` before custom-header materialization; the corrected fixture invokes `makeCustomHeaderToNet()` before recording fields, matching the encoder step used by the standard path. Rust's `find_consumer_id_list_nullable(Some(topic), Some(""))` preserves the empty header value, and `empty_consumer_group_matches_java_rpc_and_preserves_blank_header_field` verifies the actual loopback command and decoded consumer IDs. `lookup.json` SHA-256: `53ff77d17eac5dec1f2bdbc05a84ea691f0cd6452da8f5dd1ae9a356a10e130c`. Focused regression, strict core Clippy, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes), comparator/manifest22, formatting, and diff checks pass. This closes the empty-group wire boundary only; full Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

Nullable query-assignment request fields (`93339604` / `c29c5690`, 2026-10-10): the actual pinned Java `MQClientInstance.queryAssignment(topic, null, null, null, timeout)` forwards null consumer group, strategy, and message model to `MQClientAPIImpl`; an actual API invocation sends request code 400's body with only `topic` and `clientId`, then decodes an empty success response as an empty assignment set. Rust's `query_assignment_nullable` accepts each nullable input, omits the corresponding body fields, and decodes the same response. The Rust NameServer/Broker loopback asserts the wire body and empty result. `lookup.json` SHA-256: `6d9092f000f4c463e9d5c851f93d0c8d17509ad2b2af1bf80cb48116de41ff7a`. `null_assignment_fields_match_java_request_body`, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes these nullable assignment RPC inputs only; full Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

Nullable consumer group on consumer-list RPC (`6bb3727a` / `4f4d9b16`, 2026-10-10): the actual pinned Java `MQClientInstance.findConsumerIdList(topic, null)` forwards the null group to `MQClientAPIImpl.getConsumerIdListByGroup`; an actual API invocation returns the consumer IDs while omitting `consumerGroup` from request code 38's ext fields. Rust's nullable runtime/API path carries `Option<&str>`, omits that field for `None`, and decodes the response normally. The Java API fixture also records `broker:10911`, `consumerIds=["client"]`, and `consumerGroupFieldPresent=false`; the Rust NameServer/Broker loopback asserts the same header omission and result. `lookup.json` SHA-256: `888820da6c417fc04ac09835afc0f74c04795d36142f6bd9c5c88daa485db68b`. Focused regression, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes the nullable consumer-group boundary only; full Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

Null-topic consumer-list and assignment lookups (`b06a18a2` / `f8bfaf9b`, 2026-10-10): actual pinned Java 5.5.1 `MQClientInstance.findConsumerIdList(null, group)` and `queryAssignment(null, …)` both throw `NullPointerException` from `findBrokerAddrByTopic` before issuing a Broker RPC. Rust now exposes nullable topic entry points for both methods and returns `InvalidRoute("null broker lookup topic")` for `None`; named-topic calls delegate to the existing flow. `lookup.json` SHA-256: `faa61d981bb7e8aa362bb5060716a6752813259a1dd7dd03ac074320f7d59e43`. The fixture-backed regression, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. The requested Java checkout `/Users/nanashi07/Projects/3rd/rocketmq` was absent from this environment; the direct fixture uses the pinned 5.5.1 artifact and records its source commit. This closes null-topic handling for these two RPC entry points only; full Phase 6 reconciliation remains open, and P6-V04 stays `PENDING_PHASE_19`.

Nullable topic route lookup control flow (`14d67704` / `f01a663e`, 2026-10-10): actual pinned Java 5.5.1 `getAnExistTopicRouteData(null)` and `queryTopicRouteData(null)` throw `NullPointerException` before any refresh. `updateTopicRouteInfoFromNameServer(null)` instead sends the null topic to its API: a null route returns `false`, while a returned route reaches the null-key route table and throws `NullPointerException`. Rust now exposes nullable counterparts: read/query return `InvalidRoute("null route lookup topic")`; update issues the null-topic route request, returns `false` when no route is returned, and maps the successful-route failure to `InvalidRoute("null topic route key")`. The loopback differential verifies both refresh responses and confirms the wire header omits the null topic field. `route.json` SHA-256: `d0053d63f30af9f67dbe6f576560e1d77ddeb3a61d6d3d893e9ca441a099ab45`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, route fixture drift and cross-decodes), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes these null-topic route lookup branches only; full Phase 6 reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

Nullable broker-address lookup boundary (`8be43939` / `c171474e`, 2026-10-10): actual pinned Java 5.5.1 `MQClientInstance.findBrokerAddressInAdmin(null)`, `findBrokerAddressInPublish(null)`, and `findBrokerAddressInSubscribe(null, 0L, true)` all return null before reading the address map. Rust now provides nullable counterparts on `RouteSnapshot`, returning `None` for a null broker name and delegating non-null names to the existing selectors. `route.json` SHA-256: `864f45911570fed18cb5768b06094be1dc1c8e50e82151014ab2210b10c9d002`. The fixture-backed regression and full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes the null broker-name inputs only; full Phase 6 method/input/platform reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

Null topic broker selection boundary (`7c137f27` / `b99880ee`, 2026-10-10): pinned Java 5.5.1 `findBrokerAddrByTopic(null)` dereferences the null key in its concurrent route table and throws `NullPointerException`, even with an empty table. Rust now provides `ClientRuntime::find_broker_address_by_topic_nullable(Option<&str>)`, which returns `InvalidRoute("null broker lookup topic")` for `None` and delegates non-null calls to the existing lookup. `route.json` SHA-256: `d961d16d35c8dfadfc4cd092167d5b04044eb7c25c7d798dbbdbda92ebc94e7a`. Workspace all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift checks, and cross-decodes), comparator self-test and manifest validation (22 scenarios), formatting, and diff checks pass. This closes the null topic input shape only; full Phase 6 method/input/platform reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

Null queue broker-name lookup boundary (`a2ba77ec` / `de514b32`, 2026-10-10): the actual pinned Java `MQClientInstance.getBrokerNameFromMessageQueue(null)` call raises `NullPointerException`. Rust now exposes `RouteSnapshot::broker_name_nullable(Option<&MessageQueue>)`, mapping `None` to `InvalidRoute("null message queue")` while retaining the existing non-null API. `route.json` SHA-256: `b79868807ee49e5b362aa05d0dbf0e94576296345a22d84e2860b72bde698e52`. Workspace all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes the null-queue argument shape only; full Phase 6 method/input/platform reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

Heartbeat address preserves its explicit endpoint with VIP enabled (`71eea91e` / `a7e1eecc`, 2026-10-10): pinned Java `MQClientInstance.sendHeartbeatToBroker` passes the supplied address into `MQClientAPIImpl.sendHeartbeat`, which forwards that same value to `RemotingClient.invokeSync`. An actual Java runtime with `vipChannelEnabled=true` records `127.0.0.1:10911` unchanged; the Rust loopback differential also reaches the supplied listener while VIP is enabled. `heartbeat.json` SHA-256: `ad12d7c32697ec2459b725599c0e92d4cb7581a16f1c7187fa03887c1712f643`. This closes the heartbeat-specific VIP boundary while check-client, consumer-list, and assignment continue to apply VIP conversion; wider Phase 6 reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

Callback executor construction boundary (`1f62b2b3` / `c29e2358`, 2026-10-10): pinned Java 5.5.1 constructs `MQClientInstance` with `clientCallbackExecutorThreads=1`, then changes the retained `ClientConfig` value to 2. The config getter reflects 2 while `NettyClientConfig` and the already-constructed public executor remain at 1. The Rust loopback differential changes the shared runtime config to 2 and observes callback concurrency still capped at 1. Current `client-config.json` SHA-256: `ef3c51a7a3f8e796def2f9d6aae0267703f99b4ae5987c17d7fcf2fdf4640bb1`; the six constructor cases also record zero active Java executor threads before first callback use. Workspace all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and fixture drift checks), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. The construction-sampled setting branch is covered; full Phase 6 source/API/input/platform reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

Maximum callback capacity starts lazily (`4beb7658` / `804e9b45`, 2026-10-10): Java's actual `NettyRemotingClient.publicExecutor` has core capacity `Integer.MAX_VALUE` while `getPoolSize()` remains zero at `MQClientInstance` construction. Rust previously started one Tokio callback worker task per configured slot, so starting a client with this valid Java value could attempt billions of tasks before receiving work. The remoting dispatcher now starts one consumer and schedules blocking callback tasks on demand, capped by the configured capacity; a fixture-backed test starts with `i32::MAX` and executes two callbacks concurrently. Existing two-worker concurrency and queued-callback shutdown drain tests pass. This removes eager allocation proportional to the public capacity; Tokio's blocking-pool concurrency ceiling and Java executor lifecycle/queue details remain documented Rust-native differences in [the Phase 3 remoting inventory](PHASE_3_REMOTING_INVENTORY.md). Workspace all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. P6-V04 remains `PENDING_PHASE_19`, and whole Phase 6 reconciliation remains open.

Java `ConcurrentHashMap.replaceAll` contract correction (`2026-10-10`): direct 5.5.1 races on `getProducerTable()`, `getConsumerTable()`, `getFactoryTable()`, and the raw topic-route map show that a same-key writer completes during the callback. Java conditionally replaces using value equality: a changed unequal value triggers a callback retry, while a distinct equal value does not. A new producer/consumer case mutates the captured owner's equality key inside the blocked callback, then writes a distinct owner equal to the updated key; Java still calls the callback once. Rust registry `replace_all` compares the live owner equality under the key gate and commits against the stable current mapping by identity; FactoryTable and raw route `replace_all` use their corresponding Java value equality (runtime identity and route content equality). Native regressions compare writer completion, equality-sensitive retry input/count, previous value and final mapping. The extended producer/consumer fixture has `client-config.json` SHA-256 `5f6a28c0199a64a905a5f642501745ce98eae57f55e19b84f38c0c1d0c55e890`; `route.json` SHA-256 is `4fd7e22a55485451500375b6c9e9d30cfb154c1b15925ecbccaf9c1295414a8c`. Focused registry tests (8/8), strict workspace Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest validation (22 scenarios), format and diff checks pass. P6-V04 remains `PENDING_PHASE_19`, and broader Phase 6 source/API/input/platform reconciliation remains open. See [ADR-0080](../architecture/ADR-0080-factory-table-remapping-atomicity.md), [ADR-0081](../architecture/ADR-0081-registry-table-remapping-atomicity.md), and [ADR-0082](../architecture/ADR-0082-raw-route-table-remapping-atomicity.md).

Producer/consumer registry remapping atomicity (`948f67e7`, 2026-10-10): pinned Java 5.5.1 races a blocked `ConcurrentHashMap.compute` callback with a same-key writer on both `getProducerTable()` and `getConsumerTable()`. Both oracles show the writer waits, the callback runs once, a callback-time `get` sees the old mapping, and the writer receives the remapping result before publishing its own value. Rust uses weak exact-key gates shared by remapping, public table writes, registration/unregister actor commands, and pending registration cleanup; the actor releases lifecycle gates after the registry-map update and before Broker RPC. All four Java/native outcomes match for each table. `client-config.json` SHA-256: `807ad17c66ba5e775e141630744209ac129be897977879d1af616d652979fe6b`. Full workspace/all-target Rust tests, strict workspace/all-target/all-feature Clippy, focused registry-table tests, pinned offline Maven verify (22 scenarios), comparator self-test, manifest validation (22 scenarios), formatting, and diff checks pass. This closes producer/consumer registry remapping callback contention only; raw-route callback concurrency and full Phase 6 source/API/input/platform reconciliation remain open, and P6-V04 remains `PENDING_PHASE_19`. See [ADR-0081](../architecture/ADR-0081-registry-table-remapping-atomicity.md).

FactoryTable remapping atomicity correction (`e9405bcf` / `53787b3e`, 2026-10-10): the pinned Java 5.5.1 `ConcurrentHashMap.compute` race holds a same-key writer until its remapping callback returns and invokes that callback once. The native regression first failed because its writer completed during the callback. `KeyMutationLocks` now serialize each FactoryTable remapping read/callback/write with async table writes by key, while the entries mutex remains free and unrelated keys use separate gates. Existing per-method Java/native outcomes plus the new actual race regression pass. Full workspace/all-target tests, strict all-feature Clippy, formatting/diff, pinned offline Maven verify, comparator self-test, and manifest validation (22 scenarios) pass. `client-config.json` SHA-256: `87e5b80ab12add8c386a13a4bc6fbbc738a780140724db3fca15c145f038dba6`. This closes the FactoryTable remapping race only; registry callback contention was still open at that checkpoint and is closed by `948f67e7` above. Raw-route callback concurrency and full Phase 6 source/API/input/platform reconciliation remain open, and P6-V04 remains `PENDING_PHASE_19`. See [ADR-0080](../architecture/ADR-0080-factory-table-remapping-atomicity.md).

FactoryTable manager-removal contention follow-up (`8734cca5`, 2026-10-10): the invoked Java fixture now races `removeClientFactory(id, instance)` with `computeIfPresent`, observing that identity-safe removal waits, the callback runs once, and the replacement remains mapped. The first native regression was red because this synchronous overload bypassed the async table gate. `KeyMutationLocks` now use 256 fixed hash shards with async and synchronous gates: FactoryTable mutations/remapping hold both, while synchronous identity-safe removal shares the synchronous gate. Callback execution leaves the entries mutex free; the gate's state mutex is released before callbacks and is non-poisoning, so callback panic does not strand future mutations. The manager table fixture hash is `7c84370c42228a4f3676fde47af11656a3f20ab545d036b79febf13338b9909a`. The focused manager table suite, full workspace/all-target tests, strict all-feature Clippy, fmt/diff, pinned offline Maven verify (22 scenarios), differential comparator self-test, and manifest validation (22 scenarios) pass. Registry callback contention was still open at that checkpoint and is closed by `948f67e7` above; raw-route callback concurrency and full Phase 6 source/API/input/platform reconciliation remain open, and P6-V04 remains `PENDING_PHASE_19`. See [ADR-0080](../architecture/ADR-0080-factory-table-remapping-atomicity.md).

FactoryTable callback read-visibility continuation (`d837e460` Java oracle, `44fa7836` native differential, 2026-10-10): the pinned Java `ConcurrentHashMap.compute` callback calls `get` on its own key while blocked and observes the pre-compute value. The native race test now makes the same callback-time read and compares its identity to the Java fixture. The regenerated `client-config.json` SHA-256 is `3d75b063598a9fb1d0fc5300af80fdfff265c05ebf813e675bce9276fa4a0e4b`. All six `manager_table` tests, strict workspace Clippy, Maven verify (22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. This verifies read visibility only while callback exclusion remains held; full Phase 6 source/API/input/platform reconciliation remains open and P6-V04 remains `PENDING_PHASE_19`. See [ADR-0080](../architecture/ADR-0080-factory-table-remapping-atomicity.md).

FactoryTable unrelated-key concurrency correction (2026-10-10): the synchronous manager-removal fix initially used 256 fixed hash shards, which could serialize distinct IDs that happened to collide. Replaced those shards with weakly retained gates keyed by the exact map key; the async/synchronous coordination and same-key oracle behavior remain unchanged. `unrelated_keys_construct_concurrently_and_same_key_constructs_once` now finds two distinct IDs that collide under the old mask and proves both constructors reach a two-party barrier. Full workspace/all-target tests, strict all-target/all-feature Clippy, formatting/diff, pinned offline Maven verify (22 scenarios), comparator self-test and manifest22 pass. This restores the plan's independent-key construction requirement. Registry callback contention was still open at that checkpoint and is closed by `948f67e7` above; raw-route callback contention and full Phase 6 API/source/input/platform reconciliation remain open, and P6-V04 remains `PENDING_PHASE_19`. See [ADR-0080](../architecture/ADR-0080-factory-table-remapping-atomicity.md).

All-broker V2 wrapper mutation and precedence (`01706576`, extended by `f16cea61`, 2026-10-10): the actual pinned Java fixture invokes both `sendHeartbeatToAllBrokerWithLock()` and `sendHeartbeatToAllBrokerWithLockV2(false)` with V2 disabled and concurrent V1 enabled, then mutates the retained V2 setting and invokes both again. Its capturing API records modes `V1,V1,V2,V2`; only ordinary WithLock V1 runs on the concurrent executor, while WithLockV2 V1 and both V2 calls run on the caller. The native all-broker wrapper regression consumes those Java fields and sees two zero V1 fingerprints followed by two computed V2 fingerprints. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting and diff checks pass. `heartbeat.json` SHA-256: `1f045253723ebfdca0414fcbe07783c671791db7163a26eb81f6adce02c7c44f`. This closes live V2 selection and precedence for both wrappers; broader Phase 6 source/API/input/platform reconciliation remains open and P6-V04 remains `PENDING_PHASE_19`.

Concurrent-heartbeat executor width capture (`2f34823c`, 2026-10-10): the pinned Java fixture constructs a fixed concurrent-heartbeat executor at size 2, mutates `ClientConfig.concurrentHeartbeatThreadPoolSize` to 1, and observes `ThreadPoolExecutor.getCorePoolSize()` remain 2. `concurrent_v1_uses_parallel_peers_and_busy_heartbeat_gate_skips_a_round` consumes those observations and demonstrates both peers receive the heartbeat before the first response even after the Rust config is changed from 2 to 1. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting and diff checks pass. `heartbeat.json` SHA-256: `e26cecfee46a47c3a3ef6635927852f25edffaa09a74bcf103cadb25c8f2624a`. The heartbeat flag and its executor width now have separate live-read versus constructor-capture evidence; broader Phase 6 API/source/input/platform reconciliation remains open and P6-V04 remains `PENDING_PHASE_19`.

Concurrent-heartbeat mutation fixture correction and reverse transition (`73a8821a`, 2026-10-10): the initial `false→true` fixture used a raw `Object` in Java's typed producer map, so heartbeat preparation failed before reaching the missing-executor sender path. It now calls `registerProducer` with a real `DefaultMQProducerImpl` and records `preparedProducerCount=1`; the wrapper returns false with the executor absent. The paired `true→false` Java case starts with a two-thread executor, retains it after mutation, then invokes the public wrapper through a capturing API and records a successful V1 call. Native tests retain their consumer owners and registration leases; the false→true case proves there is no Broker connection, while the true→false scripted two-peer test holds the first reply and proves the second Broker receives nothing until the first response releases the serial loop. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting and diff checks pass. `heartbeat.json` SHA-256: `c0f8d61a5adbf9e1963ced575226cddf60422ef85f12afb80e7d29f655327dec`. Both live setting transitions now have source and native evidence; broader Phase 6 source/API/input/reverse-export and platform-runtime reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

Concurrent-heartbeat setting mutation boundary (`85935c7f`, 2026-10-10): the actual pinned Java 5.5.1 fixture constructs `MQClientInstance` with concurrent heartbeats disabled, injects one producer entry and a broker address without starting transport, flips the retained setting to true, then calls `sendHeartbeatToAllBrokerWithLock()`. The executor remains absent and the public wrapper catches the null-executor failure and returns false. `enabling_concurrent_heartbeat_after_construction_has_no_executor` consumes those fixture fields and confirms the native call also returns false without connecting to its loopback listener. Full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting and diff checks pass. `heartbeat.json` SHA-256: `a526986c45476367ae82e592a5fcf53ca3c9938aa8f603697433302e2f3e937f`. This distinguishes the live sender-selection flag from the construction-captured executor; broader Phase 6 source/API/input/reverse-export and platform-runtime reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

Stopped-runtime `start()` no-op correction (`47f36e0a`, 2026-10-10): actual pinned Java 5.5.1 `shutdown()` from its `RUNNING` state branch transitions to `SHUTDOWN_ALREADY` and shuts down the scheduler; a later public `start()` returns normally and does not recreate resources. The fixture seeds the Java state-machine branch without activating workers, then invokes both public methods; the native differential performs real start, shutdown and restart-call sequencing. Before correction, Rust returned `InvalidState(Stopped)`; it now joins the terminal control owner and returns success without restarting transport or workers. Focused red/green, full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, format and diff checks pass. Broader source/API/input and non-macOS runtime reconciliation remains open; P6-V04 remains `PENDING_PHASE_19`.

Runtime schedule validation and startup rollback (`29d74adc`, 2026-10-10): actual pinned Java 5.5.1 calls cover invalid route, heartbeat, and persistence periods independently. Each `MQClientInstance.start()` fails during fixed-rate task registration with `IllegalArgumentException`; Java transitions to `START_FAILED` and closes the scheduler. Native tests match the invalid-schedule error, terminal `Failed` state, cleanup, and manager replacement behavior. Constructor probes also confirm that concurrent-heartbeat executor sizes 0 and -1 fail while 1 succeeds; native construction matches. Focused runtime tests, full workspace/all-target tests, strict Clippy, offline Maven verify, format, and diff checks pass. This is source-branch evidence, not phase completion; cross-platform automatic-IP execution, the full public API/input/reverse-export audit, and P6-V04 remain open.

macOS automatic-IP candidate ordering correction (2026-10-10): an exact ordered differential against the invoked Java 21 `NetworkUtil.getLocalInetAddressList()` fixture exposed that Rust matched macOS IPv4 candidates but left the IPv6 candidates in raw `getifaddrs` order. OpenJDK's `_ALLBSD_SOURCE` backend runs separate IPv4 and IPv6 `getifaddrs` passes; `addif` prepends interfaces and addresses, and the IPv6 pass replaces scope IDs with interface indices. Rust now applies that BSD ordering and scope mapping on macOS too. The regression compares the full IPv4 and IPv6 address sequences, then confirms selected address and IPv6-only-subset results; before the fix, the IPv6 sequence assertion failed, and all 16 focused local-address tests pass after it. Full locked workspace/all-target tests pass (including 82 runtime integration cases), strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting and diff checks pass. OpenJDK source: [BSD `getifaddrs` family passes and scope assignment](https://github.com/openjdk/jdk21u/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L1548-L1648), [BSD interface-index fallback](https://github.com/openjdk/jdk21u/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L1651-L1667), and [head-inserting `addif`](https://github.com/openjdk/jdk21u/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L905-L948). This closes the observed macOS candidate order/scope branch; Linux and BSD target runtime enumeration, Windows runtime enumeration, a real IPv6-only host, broader Phase 6 reconciliation, and P6-V04 remain open.

Automatic client-IP JVM address-family preference (`fdb118ba`, 2026-10-10): OpenJDK reads `java.net.preferIPv4Stack` once with `Boolean.getBoolean` while loading its network library, then caches IPv6 availability; `NetworkInterface.enumInterfaces` omits its IPv6 enumeration when IPv6 is disabled. Rust now snapshots the same dotted process-environment key once, applies Java's case-insensitive exact-`true` parsing, omits IPv6 interface candidates across Unix, Linux and Windows adapters, and limits hostname fallback to IPv4 in that mode. No public setting was added. Property parsing, subprocess snapshot, and IPv4-only enumeration tests pass. A fresh Java 21 probe observed 4 IPv4/10 IPv6 interface addresses by default and 4 IPv4/0 IPv6 with `-Djava.net.preferIPv4Stack=true`; an isolated Rust process using the same environment key returned its selected IPv4 address. Full locked workspace tests, strict workspace Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, fmt and diff checks pass. The actual address module and tests also pass an isolated Windows-target `cargo check`. Sources: [OpenJDK one-time property snapshot and cached IPv6 availability](https://github.com/openjdk/jdk21u/blob/master/src/java.base/share/native/libnet/net_util.c#L50-L77), [OpenJDK IPv6 enumeration gate](https://github.com/openjdk/jdk21u/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L802-L838), and [JDK 21 networking properties](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/net/doc-files/net-properties.html#IPv4/IPv6). This closes the property-controlled family branch at source level; production Windows, Linux, and BSD target enumeration, behavior on a real IPv6-only host, whole Phase 6 reconciliation, and P6-V04 remain open.

BSD automatic-IP enumeration order and IPv6 scope (`96b70d15`, 2026-10-10): OpenJDK's BSD backend calls `getifaddrs` for an IPv4 pass and again for IPv6, sets every IPv6 scope from the interface index, then uses `addif` to prepend interfaces and addresses. Rust now groups the available `getifaddrs` rows by family and reconstructs that same list order; it also replaces each BSD IPv6 scope with the interface index and retains a named scope. The two BSD-focused synthetic tests cover family/cross-interface head insertion and scope replacement, and pass on macOS with the pure BSD logic test-included. The full locked workspace suite, strict workspace Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, fmt and diff checks pass. Sources: [OpenJDK BSD family passes and scope assignment](https://github.com/openjdk/jdk21u/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L1576-L1648), [BSD scope-index lookup](https://github.com/openjdk/jdk21u/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L1651-L1667), and [OpenJDK head insertion](https://github.com/openjdk/jdk21u/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L925-L948). This host has no BSD Rust target, so the production BSD branch has not executed on BSD; Linux target execution, Windows runtime enumeration, a live IPv6-only/global-address comparison, whole Phase 6 reconciliation, and P6-V04 remain open.

Linux automatic-IP enumeration order (`b5917fc2`, 2026-10-10): OpenJDK enumerates IPv4 rows before IPv6 rows, using Linux `SIOCGIFCONF` for IPv4 and `/proc/net/if_inet6` for IPv6. Its `addif` prepends newly seen interfaces and addresses; Java then visits each interface's IPv6 addresses before IPv4 addresses. Rust now reads those same Linux sources, maps aliases to accessible roots, and reconstructs the same per-root candidate order before applying interface-state and RocketMQ filters. The Linux backend's three focused tests cover ABI layout, IPv6 row scope parsing, and cross-interface/cross-family head insertion order; they run on macOS, where the Linux-specific implementation is test-included. Full locked workspace tests, strict Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, fmt and diff checks pass. Sources: [OpenJDK family enumeration](https://github.com/openjdk/jdk21u/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L802-L838), [Linux IPv4 SIOCGIFCONF enumeration](https://github.com/openjdk/jdk21u/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L1103-L1122), [Linux IPv6 proc enumeration](https://github.com/openjdk/jdk21u/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L1177-L1196), and [OpenJDK interface/address insertion](https://github.com/openjdk/jdk21u/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L905-L990). The installed Rust targets do not include Linux, so the production Linux backend has not been executed on Linux; the BSD order is now source-mapped but awaits BSD target execution. A live IPv6-only/global-address comparison, Windows runtime enumeration, and whole Phase 6 reconciliation remain open. P6-V04 remains `PENDING_PHASE_19`.

Unix automatic-IP logical alias correction (`b2589280`, 2026-10-10): OpenJDK's `addif` maps `eth0:1`-style addresses to the parent interface when its parent lookup succeeds, inserts the address into the parent's address list, and also creates a virtual child. The pinned RocketMQ `NetworkUtil` skips virtual interfaces while it collects candidates, so the address remains visible through the parent. Rust now resolves the parent with `if_nametoindex` and keeps the address under that root name; it skips the alias when that lookup fails. The new parent/virtual-root cases pass; all 12 focused address tests, full workspace tests, strict Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, fmt and diff checks pass. Source: [OpenJDK alias parent mapping](https://github.com/openjdk/jdk21u/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L905-L923) and [parent plus virtual-child address insertion](https://github.com/openjdk/jdk21u/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L946-L990). This closes logical alias inclusion at source level; BSD candidate order is source-mapped but awaits BSD target execution, while a live Linux Rust comparison on an IPv6-only/global-address host, Windows runtime enumeration, and whole Phase 6 reconciliation remain open; P6-V04 remains `PENDING_PHASE_19`.

Linux automatic-IP IPv6 scope correction (`a24c3bd1`, 2026-10-10): OpenJDK 21u reads Linux IPv6 rows from `/proc/net/if_inet6` and assigns each row's interface index to `sin6_scope_id`; glibc `getifaddrs` only sets that scope for link-local and link-scoped multicast addresses. Rust now resolves the interface index with `if_nametoindex` for Linux IPv6 candidates, so Java's `%interface-name` suffix is retained even for global IPv6 values. The focused regression passes, including a global IPv6 candidate with zero native scope; a network-disabled cached Linux Java runtime also reported `::1%lo` with scope 1. The focused address suite (11 tests), full workspace/all-target tests, strict workspace Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting and diff checks pass. Sources: [OpenJDK Linux IPv6 enumeration](https://github.com/openjdk/jdk21u/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L1177-L1196), [glibc `getifaddrs` scope assignment](https://codebrowser.dev/glibc/glibc/sysdeps/unix/sysv/linux/ifaddrs.c.html#613), and [Java IPv6 host-address rendering](https://github.com/openjdk/jdk21u/blob/master/src/java.base/share/classes/java/net/Inet6Address.java#L247-L254). This closes Linux IPv6 interface-scope formatting at source level; an actual IPv6-only/global-address Rust runtime comparison remains open; Linux and BSD source order is implemented but awaits target runtime execution. Windows runtime enumeration and whole Phase 6 reconciliation also remain open. P6-V04 remains `PENDING_PHASE_19`.

Windows client-IP enumeration source correction (2026-10-10): compared the pinned Java 21 runtime behavior with OpenJDK `jdk21u`'s Windows `NetworkInterface` implementation. Java obtains interface rows from `GetIfTable2`, then unicast and anycast rows from the two address tables; it keeps only Preferred/Deprecated unicast rows, includes anycast rows, prepends address rows (reverse table order, anycast before unicast), and separately queries `GetIfEntry2` for `isUp()` and `isPointToPoint()`. Rust now follows that source path and releases all tables with `FreeMibTable`. The two new host-run tests cover DAD acceptance and per-interface address order; all 10 focused address tests pass. The isolated actual-module Windows-target check and `/private/tmp/p6-windows-core-ring` core Windows-target `cargo check` pass. The ordinary production-feature cross-build remains blocked in `aws-lc-sys` because MinGW GCC is absent; Windows runtime enumeration remains unverified. Full workspace tests, strict Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, fmt and diff checks pass. OpenJDK source references: [table acquisition and table cleanup](https://github.com/openjdk/jdk21u/blob/master/src/java.base/windows/native/libnet/NetworkInterface.c#L1706-L1754), [DAD filtering and prepend order](https://github.com/openjdk/jdk21u/blob/master/src/java.base/windows/native/libnet/NetworkInterface.c#L1872-L1928), [interface-table traversal and current-state checks](https://github.com/openjdk/jdk21u/blob/master/src/java.base/windows/native/libnet/NetworkInterface.c#L2477-L2593). This closes source-mapped Windows enumeration semantics only; Windows runtime, non-macOS Unix ordering, IPv6-only-host evidence, and whole Phase 6 reconciliation remain open; P6-V04 remains `PENDING_PHASE_19`.

Automatic client-IP discovery failure (`2026-10-10`): pinned Java `NetworkUtil.getLocalInetAddress()` catches interface enumeration/selection and hostname-resolution exceptions and returns null; `ClientConfig()` stores that result in `clientIP`. Rust `new_auto()` now keeps its existing `Result` signature but initializes `client_ip=None` on discovery failure, matching the Java null setting and `null@DEFAULT` identity. `automatic_client_ip_failure_preserves_java_null_default` passes. Workspace/all-target tests pass after granting the existing loopback suite local-bind access; strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes; logger root under `/private/tmp`), comparator self-test/manifest22, fmt and diff checks pass. This closes only the automatic-IP failure branch; Phase 6 source/API/input/platform/reverse-export reconciliation remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

Unix automatic-IP interface state (`2026-10-10`): RocketMQ calls `NetworkInterface.isUp()` before accepting candidate addresses. OpenJDK's Unix native implementation requires both `IFF_UP` and `IFF_RUNNING`; Rust now applies both flags, and `interface_filter_matches_java_up_running_and_exclusions` checks UP-only, RUNNING-only, combined, alias, point-to-point, and bridge cases. The eight focused `local_address` tests pass on macOS. This closes the Unix flag predicate at source level; Linux and BSD candidate order is source-mapped but awaits target runtime execution; Windows runtime enumeration and a real IPv6-only host remain unverified; full Phase 6 reconciliation remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

Registered-consumer demand republishes an equal route (`f941287a` Java oracle, `f9cc0474` Rust differential, 2026-10-10): the direct Java runtime publishes an initial route, then receives an equal DTO while the registered consumer changes `isSubscribeTopicNeedUpdate` to true. The second call returns changed=true without changing the cached DTO; producer and consumer update callbacks both advance from one to two, and the need-check runs once. The Rust two-response loopback consumes the same Java DTO twice and compares return value, cached route value and callback counts. Workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, route fixture drift and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Current `route.json` SHA-256: `2f3f8d919a4eebef5beb79797bc8841fb2ef9d561d93bb43d00bfdb42233d9c0`. This closes the registered-consumer demand branch for equal routes only; broader Phase 6 gaps remain and P6-V04 stays `PENDING_PHASE_19`.

Route recovery and broker-address visibility after malformed refresh (`0d4c4d43`/`af3903fe` Java oracle, `062a6bd7`/`356189b0` Rust differential, 2026-10-10): one runtime processes valid, null-address-map failure, then valid recovery. The failure retains the prior snapshot and producer/consumer callback counts and preserves publish lookup `broker -> old:10911`; recovery returns changed=true, publishes `recovery-broker -> recovered:10911`, advances both callbacks from one to two, and still returns the old broker address. The Rust loopback replays all three Java DTO bodies and compares failure, snapshot identity, old/new broker lookups, recovery address, and callback counts. Workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, route fixture drift and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Current `route.json` SHA-256: `a356ccfd8021f3e7795a4db62d4d41a4d864ccb6f87fed3b068bd427c2e6c38b`. This covers address visibility for the null-address-map recovery sequence only; broader Phase 6 gaps remain and P6-V04 stays `PENDING_PHASE_19`.

Producer and consumer callbacks on malformed-route continuation (`7cbda1d8` Java oracle, `c25ff661` Rust differential, 2026-10-10): direct pinned Java refreshes on one runtime call both producer publish-info and consumer subscription-info callbacks once for the initial valid route. A second DTO with `BrokerData.brokerAddrs=null` throws `NullPointerException`, keeps the prior route, and leaves both callback counts unchanged at one. The Rust two-response loopback test compares those counts and snapshot identity. Workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, route fixture drift and Java cross-decodes), comparator self-test/manifest22, formatting and diff checks pass. Current `route.json` SHA-256: `3f5566c986850f40a2e1e36f9fa53df90ab1dcb4272c32d1e20bd3681c147c46`. This narrows only the callback retention behavior for this malformed second refresh; broader Phase 6 gaps remain and P6-V04 stays `PENDING_PHASE_19`.

Live scheduled-task configuration continuation (`b9544aad` Java oracle, `a5fa6f96` Rust differential, 2026-10-10): direct pinned Java calls to `MQClientInstance.startScheduledTask()` now include mutations made after construction. Null→configured omits the NameServer discovery registration; configured→null adds it. Both cases schedule route/heartbeat/persist periods from the mutated values 71/83/97ms instead of the construction values 23/47/59ms. `scheduled_task_delays_and_periods_match_invoked_java_registration` consumes all four baseline/live Java call lists. Focused Rust scheduler unit, full workspace/all-target tests, strict all-target/all-feature Clippy, full pinned offline Maven verify (22 scenarios with lookup fixture drift and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Current `lookup.json` SHA-256: `c69e8f62f0d8d5d6198b3c93e91f3540b444ecb2d3193f36340ec1bdebfbac5c`. This verifies these three scheduler period settings and the discovery-presence check at task registration only; other Phase 6 gaps remain and P6-V04 remains `PENDING_PHASE_19`.

Registered-consumer cache preservation on malformed-route continuation (`8d0a696f` Java oracle extension, `49ff2c1d` Rust differential, 2026-10-10): two route RPCs hit one runtime with an active consumer. A valid initial DTO is published and calls the subscription update callback once. A subsequent non-null BrokerData with `brokerAddrs=null` throws Java `NullPointerException`, retains the original route DTO, and leaves need-check and update counts unchanged. The Rust loopback test compares both response bodies, snapshot identity, and callback counters. Full workspace/all-target tests, strict all-target/all-feature Clippy, full pinned offline Maven verify (22 scenarios with route fixture drift and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Current `route.json` SHA-256: `7cc17ef57754595f2d7292c8374148640a41a6ec9bf973e775d7c77f6a48783b`. This closes the consumer/cache preservation boundary for this malformed refresh only; other Phase 6 gaps remain and P6-V04 stays `PENDING_PHASE_19`.

Registered-consumer null Broker address-map route continuation (`eef46440` Java oracle, `34e9448d` Rust differential, 2026-10-10): a direct pinned Java route refresh with populated queue/filter/static-mapping collections and a non-null BrokerData whose `brokerAddrs` is null throws `NullPointerException` before publication. With a registered consumer, neither `isSubscribeTopicNeedUpdate` nor `updateTopicSubscribeInfo` runs. The Rust loopback differential consumes the exact Java DTO body and matches failure, absent cache, and zero callbacks. Full workspace/all-target tests, strict all-target/all-feature Clippy, full pinned offline Maven verify (22 scenarios, route fixture drift and Java cross-decodes), comparator self-test/manifest22, formatting and diff checks pass. Current `route.json` SHA-256: `30a80b5e3caf7bf6855acfced83638bf1b6d542f65c84bae0a5d7ad48b76362e`. This closes this malformed Broker-map callback boundary only; other Phase 6 gaps remain and P6-V04 remains `PENDING_PHASE_19`.

Registered-consumer null queue-list route continuation (`816658be` Java oracle, `be83c42e` Rust differential, 2026-10-10): direct pinned Java `MQClientInstance.updateTopicRouteInfoFromNameServer("topic")` with a registered consumer, null `queueDatas`, populated Broker/filter collections, and one valid static mapping returns `true`, caches the route with a null queue list, performs zero `isSubscribeTopicNeedUpdate` checks, and calls `updateTopicSubscribeInfo` once. The Rust loopback differential replays the Java route body and matches publication, collection sizes, and callbacks. Full workspace/all-target tests, strict all-target/all-feature Clippy, full pinned offline Maven verify (22 scenarios with route fixture drift and Java cross-decodes), comparator self-test/manifest22, formatting and diff checks pass. Current `route.json` SHA-256: `d04a6a5d407230fc773eac04eec63faf14bb7e7129c2b79cbc365181c6ef093e`. This closes this outer-null/static-route registered-consumer interaction only; broader Phase 6 gaps remain and P6-V04 remains `PENDING_PHASE_19`.

NameServer start mutation continuation (`36d680d5` initial Java oracle, `9af93678` initial Rust differential, `ff06d4b8` two-direction Java oracle, `6998cdfb` reverse-branch Rust differential, `88de2cb7` fixture-write guard, `036429a0` empty-string continuation, `b49179e3` scheduler-count differential, 2026-10-10): forked pinned Java cases directly invoke `MQClientInstance.start()` after changing `namesrvAddr` in both null/configured directions and after changing a configured value to the empty string. Null at construction→configured before start produces zero `fetchNameServerAddr` calls and keeps the active list absent. Configured at construction→null before start invokes discovery once and keeps `configured-at-construction:9876` in the active list. Configured→empty before start makes zero discovery calls and retains that active address. All three cases reach RUNNING and shutdown returns. The Rust differentials consume the fixture and exercise the same mutations; follow-up commit `60e4e9d2` adds an explicit counting `TopAddressing` provider to the null→configured case, proving zero calls, while configured→null proves exactly one call. Commit `036429a0` adds the configured→empty Java/native case and proves zero calls with the constructor-time active address retained; follow-up `b49179e3` matches the Java probe's four scheduled registrations with native `schedules()`. An injected Java-blank provider result avoids external HTTP fallback in the configured→null case. Workspace all-target tests, strict all-target/all-feature Clippy, full pinned offline Maven verify (22 scenarios, including the forked fixture and Java cross-decodes), comparator self-test, manifest22, formatting and diff checks pass; the later fixture-write guard also passes the targeted Maven compile/drift check. `start.json` SHA-256: `89c41d361d878842cd59128cd197a8b59790080e2aeaa43b30868d8b5b22a364`. This closes these three post-construction `namesrvAddr` startup-mutation cases only; other Phase 6 gaps remain, and P6-V04 remains `PENDING_PHASE_19`.

Heartbeat V2 setting activation continuation (`1688e167` Java oracle, `ea65afc4` Rust differential, `8f24c067` direct-entrypoint alignment, 2026-10-10): the Java fixture invokes `MQClientInstance.sendHeartbeatToBroker` with V2 disabled, mutates the retained config to true, then records calls to `MQClientAPIImpl.sendHeartbeat` and `sendHeartbeatV2` in that order. `heartbeat_protocol_mode_reads_live_v2_setting_after_construction` consumes those Java outcomes, calls the matching direct single-Broker runtime entrypoint before and after mutation, and checks the wire fingerprint changes from zero to the computed V2 value. All 14 heartbeat tests, workspace all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and fixture drift/cross-decodes), comparator self-test, manifest22, formatting and diff checks pass. Current `heartbeat.json` SHA-256: `c4eb61ad100b22414ed510061929e2c3bae22f3077fcd901947d56dba2214805`. This closes the live V2 selection branch only; Phase 6 remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

Runtime API timeout mutation continuation (`74c12165` Java oracle, `9b30acb3` Rust differential, 2026-10-10): the direct Java `MQClientInstance.findConsumerIdList` invocation records the configured default timeout of 3000ms, then records 150ms after mutation of the same retained `ClientConfig`. `consumer_lookup_uses_timeout_mutated_after_runtime_construction` consumes that Java value, delays the Broker's successful response to 450ms, and confirms the native RPC times out under the updated 150ms budget. All six focused `runtime_lookup` tests, workspace all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and fixture drift/cross-decodes), comparator self-test, manifest22, formatting and diff checks pass. Current `lookup.json` SHA-256: `aadc780284fd8fe5f5b51d33e45bc62c6bf88607ba2fec38db69be0fef5133b3`. This closes the post-construction timeout read at this lookup call site only; Phase 6 remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

VIP lookup setting mutation continuation (`4945f123` Java oracle, `995596ff` Rust differential, 2026-10-10): the direct Java fixture invokes both lookup methods through `MQClientInstance` and the actual `MQClientAPIImpl` with a capturing remoting client. With the retained `ClientConfig` setting initially false, each call uses `127.0.0.1:10913`; after setting it true, each uses `127.0.0.1:10911`. The Rust test consumes all four observations, asserts its initial setting, mutates it after runtime construction, and verifies both RPCs arrive at the VIP port over loopback. Java compilation, fixture generation and drift verification, and all five focused `runtime_lookup` tests pass. `lookup.json` SHA-256 at the VIP checkpoint: `c58bda7b7f80109eaea602003033294224b51ad6aa816e15e00238546434f78d`. This closes the live VIP lookup activation branch only; Phase 6 remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

Runtime client-ID snapshot continuation (`6c58ec1d` Java oracle, `dc79903d` native differential, 2026-10-10): the pinned Java fixture directly calls `MQClientInstance.getClientId()` before and after mutating `clientIP`, `instanceName`, and `unitName` through its retained `ClientConfig`. The runtime ID remains the constructor value while `ClientConfig.buildMQClientId()` reflects the changed fields. `runtime_client_id_remains_constructor_snapshot_after_config_mutation` compares both Java-observed values. Full workspace all-target tests, strict all-target/all-feature Clippy, full pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. `client-config.json` SHA-256 at that checkpoint: `cf4d7191c787efa2f08afb956ee456368d35159c1766b6c79006a43e43fcd4ce`. This closes the mutable-config/runtime-ID boundary only; broader Phase 6 source/API/input/platform reconciliation remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

Runtime NameServer setting snapshot continuation (`b2cc4e21` Java oracle, `b790edc2` native differential, 2026-10-10): pinned constructor bytecode seeds `MQClientAPIImpl`'s active NameServer list once when the initial `namesrvAddr` is non-null. Direct `getNameServerAddressList()` cases confirm that changing the retained config later neither adds a server to an initially empty list nor replaces an initially configured address. `active_nameserver_addresses_remain_constructor_snapshot_after_config_mutation` consumes both Java observations. At that historical checkpoint the fixture did not invoke `start()` because its internal producer could leave the Maven JVM alive; the later forked `RuntimeStartMutationProbe` entry below halts its child after recording start and closes that invocation boundary. `client-config.json` SHA-256: `b3001689346cae2ff4749fcd9db9100eb944d801062c21ff811c1fe05a59fca1`. Config integration17, strict core all-target/all-feature Clippy, full pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes), comparator/manifest22, formatting and diff checks pass. This closes the active-list constructor boundary only; broader Phase 6 reconciliation remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

Heartbeat listener setting activation continuation (`27cb60bf` Java oracle, `ab1c3949` native differential, 2026-10-10): direct pinned Java 5.5.1 construction observes the actual listener with `enableHeartbeatChannelEventListener` false and true, then mutates the same retained `ClientConfig` to the opposite value. Listener absence/presence and identity remain fixed after construction. `disabled_or_unknown_channel_does_not_send_unsolicited_heartbeat` consumes both Java cases and compares native listener presence and `Arc` identity before and after mutation. Current `heartbeat.json` SHA-256: `7ff0e28d36ed191002871ac36f00a60ecee5edc64f183565b93e4161fa71032b`. Focused test and full pinned offline Maven verify passed (22 scenarios, fixture drift and cross-decodes). This documents a source-time activation boundary; the overall Phase 6 matrix remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

Runtime boot timestamp getter continuation (`0b8ac492` Java oracle, `3ab4e236` native differential, 2026-10-10): the pinned Java 5.5.1 fixture now calls `MQClientInstance.getBootTimestamp()` directly, verifies its value falls between wall-clock samples immediately before and after construction, and confirms the getter remains stable after the lookup fixture calls. `runtime_metadata.rs` consumes those Java observations and retains Rust constructor-window, shared-runtime, start, shutdown and replacement checks. Focused test and full pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes) pass. Current `lookup.json` SHA-256: `c69e8f62f0d8d5d6198b3c93e91f3540b444ecb2d3193f36340ec1bdebfbac5c`. This closes the direct getter invocation evidence gap, not the remaining source/API/input/platform reconciliation or Phase 6 acceptance.

Runtime lookup invocation continuation (`a5d9a91c`/`7c7965ba`/`c3c71d04` Java oracles, `cb37ec00`/`8a9fe417`/`16e6f153` Rust differentials, 2026-10-10): direct pinned Java 5.5.1 calls through `MQClientInstance.findConsumerIdList/queryAssignment` record route refresh, Broker address, request fields, default 3000ms consumer timeout, signed assignment timeout forwarding (`0` and `-1` unchanged), consumer-list error swallowing, assignment error propagation, and empty-Broker results. With no Broker, Java refreshes for each method and makes no API calls. Rust loopback differentials consume the oracle's consumer IDs and assignment arguments, compare both null results, and verify expired assignment deadlines send no frame while continuing to exercise assignment decoding. All five `runtime_lookup` tests and pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes) passed at the timeout-forwarding checkpoint, when `lookup.json` SHA-256 was `b133d3cd549191797c205d410e61d4928f2c922e33bd8fbeb8bf0a9cf993b6bf`. This closes direct runtime-entrypoint evidence for these two methods only; full Phase 6 reconciliation remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

Admin registration/shutdown ownership continuation (`78eff96c`/`18fc5709` Java oracles, `c60cbba8` implementation, `83a75070`/`0ca939d3` differential assertions, 2026-10-10): the pinned Java 5.5.1 public methods retain the first non-null `MQAdminExtInner` by group, reject null/duplicate registration with `false`, and remove unconditionally by group. Fifteen invoked registration outcomes cover null group/admin, empty table after rejection, retained first owner, duplicate owner release, null unregister failure, missing group, empty group and re-registration. Two more invoked outcomes start an unconnected runtime and prove that `shutdown()` preserves the factory mapping while an admin is registered, then removes it after unregister. Rust stores an opaque strong owner until group removal, includes admin entries in the active-user shutdown guard, and maps Java's null-key exception to `InvalidRoute("null admin group")`. Workspace all-target tests, strict all-feature Clippy, pinned offline Maven verify (22 scenarios and fixture drift), comparator self-test/manifest22, formatting and diff checks pass. This closes admin owner/shutdown gating only; admin operations remain with Phase 17, the full Phase 6 matrix remains PARTIAL, and P6-V04 remains `PENDING_PHASE_19`.

Admin owner destructor re-entry continuation (`bbc05ff9`, 2026-10-10): because Rust runs `Drop` synchronously, removing a retained admin `Arc` or clearing the registry while its mutex is held can invoke user code under that lock. A red test used an owner destructor that re-enters registration and timed out on both `unregister_admin_ext` and startup-failure `GroupRegistry::clear`. Removal paths now detach admin entries under lock and drop them after unlock; both regressions pass. Workspace all-target tests (109 core unit tests), strict all-target/all-feature Clippy, formatting and diff checks pass. Java behavior is unchanged; this closes the Rust destructor/re-entry ownership edge, not the remaining API/input audit or Phase 6 as a whole.

Admin back-reference lifecycle continuation (`bcc98f46`, 2026-10-10): the public registration method now documents `Weak<ClientRuntime>` for owner back-references so the table's required strong admin owner cannot create a Rust `Arc` cycle. The lifecycle differential's admin stores a weak runtime handle; after group removal and shutdown, dropping the explicit manager and both runtime handles leaves the runtime `Weak` un-upgradeable. Focused lifecycle test and strict core Clippy pass. This verifies the supported cycle-free ownership pattern; arbitrary external owner types remain responsible for following it.

`TopicRouteData` collection alias continuation (`e47f4517` oracle, `45b8a5fc` implementation): pinned Java confirms list/map object retention plus nested QueueData, BrokerData, TopicQueueMappingInfo and filter-list identity on all four collection setters/getters. Rust stores the same collection shapes through shared handles, with snapshots used at runtime conversion/read boundaries. The test covers getter identity and mutation visibility in both directions; fixture hash at that checkpoint was `960d8c5d82cefa1a4dc0eeddfee74693ea64e2d7ac679f396e06a20c3dfadd4b`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator/manifest22, formatting, and diff checks pass. This is one DTO API correction only. The full Phase 6 matrix remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

TopicRouteData shallow-copy and runtime-publication identity continuation (Java oracles `d437ab34` and `827f6510`; Rust implementations `8b69c09e` and `a87e3726`): direct Java tests cover the copy constructor, `cloneTopicRouteData`, `deepCloneTopicRouteData`, and an actual route refresh. Java-compatible copies use new outer containers while retaining nested QueueData, BrokerData, mapping DTO, and filter-list references; deep clone remains independent. The runtime oracle confirms raw-table and published route DTOs have distinct outer containers with shared nested route elements. Rust trait `Clone` remains a deep value copy. Current route fixture SHA-256 is `d01c51ec40fdcf96931dd0191271a2b23fc396ec00f18f949aa629e678172e2b`; see [ADR-0077](../architecture/ADR-0077-route-copy-element-aliases.md). Focused protocol differential, full workspace tests with loopback access, strict Clippy, Maven verify/cross-decodes, comparator/manifest22, fmt, and diff checks pass. The full Phase 6 matrix remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

Route DTO mutable-map alias correction (`65cdabce`, 2026-10-10): pinned RocketMQ 5.5.1 calls establish that `BrokerData.brokerAddrs` and `TopicQueueMappingInfo.currIdMap` retain their assigned Maps and return the same Maps from getters. The Java oracle checks caller/getter mutation visibility in both directions. Rust now wraps both fields in shared handles, keeps DTO cloning deep and equality content-based, and preserves the JSON shape. `route.json` SHA-256: `bf3ce0cb617989cf7e331dabfd738b8fc5a650cdc4b36037eae30f8add8960d3`; see [ADR-0075](../architecture/ADR-0075-shared-route-map-aliases.md). Full workspace/all-target tests with loopback access, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test, manifest validation (22 scenarios), formatting, and diff checks pass. This closes only these two route-map alias rows; the Phase 6 matrix remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Heartbeat DTO Set alias correction (`7e5b3fa6`, 2026-10-10): pinned RocketMQ 5.5.1 API cases establish that `SubscriptionData.tagsSet/codeSet`, `ConsumerData.subscriptionDataSet`, and `HeartbeatData.producerDataSet/consumerDataSet` retain their assigned mutable Sets and return those same Sets from getters. The Java oracle also checks nested SubscriptionData identity, identity-based ProducerData/ConsumerData elements, and mutation visibility in both directions. Rust now stores each Set in a shared handle, wraps decoded elements, and preserves the relevant Java value or identity equality in HashSet membership; owned setters remain available. The transparent JSON shape and null/default rules remain unchanged. `heartbeat.json` SHA-256: 7c50b6ff17d46cc6659ab69e75a69c56d28a7a187ea198440c093acefa1f39b3. See [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md). Protocol tests (108 unit tests, integrations, 15 doctests), workspace all-target tests with local socket access, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test, manifest validation (22 scenarios), formatting, and diff checks pass. This closes the heartbeat Set alias rows only; broader Phase 6 reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Status/reset Map alias correction (`2f07fe94`, 2026-10-10): pinned Java 5.5.1 confirms `GetConsumerStatusBody` retains its outer `messageQueueTable` and `consumerTable` Maps plus each assigned nested consumer Map, and `ResetOffsetBody` retains `offsetTable`. Rust now uses shared outer and nested table handles; tests assert `ptr_eq` and caller/getter mutations in both directions. Fastjson decode wraps each table level, while wire JSON remains unchanged. No identity claim is made for individual `MessageQueue` keys. `lookup.json` SHA-256 remains `7d56171dba60258e9ec45ad91c483f51c0a049a65adaf2f62212c416e780094a`; see [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md). Protocol tests/doctests, full workspace/all-target tests with local socket access, strict all-feature Clippy, pinned offline Maven verify (22 scenarios/cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. At this checkpoint, heartbeat collection aliases remained open and were later closed by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md); broader Phase 6 reconciliation remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.

LiteSubscription getter alias correction (`e33c43b2`, 2026-10-10): pinned Java 5.5.1 confirms every `getLiteTopicSet()` call returns the same mutable Set; changes through a retained getter result and later subscription API calls are mutually visible. `setLiteTopicSet` copies and adds members without retaining the input Set, clearing existing topics, or changing `updateTime`. Rust now stores the member Set in `SharedLiteSubscriptionTopicNames`, and add/remove methods mutate that shared value. JSON and `Display` output remain unchanged. `lookup.json` SHA-256: `7d56171dba60258e9ec45ad91c483f51c0a049a65adaf2f62212c416e780094a`; see [ADR-0072](../architecture/ADR-0072-shared-lite-subscription-getter-alias.md). Protocol tests/doctests, full workspace/all-target tests with local socket access, strict all-feature Clippy, pinned offline Maven verify (22 scenarios/cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. At this checkpoint, heartbeat Set aliases remained open and were later closed by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md); broader Phase 6 reconciliation remains PARTIAL; status/reset Map aliases were closed by `2f07fe94` / [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md); P6-V04 stays `PENDING_PHASE_19`.
LiteSubscriptionCtlRequestBody Set and element alias correction (`66adb0c4`, 2026-10-10): the pinned Java 5.5.1 fixture proves the body retains its assigned Set and the same supplied DTO objects; mutations and element membership are visible through caller-held and getter-held references. Rust uses `SharedLiteSubscriptionSet` with identity-based shared DTO elements and wraps each decoded element. Existing Fastjson tests still cover null deduplication and Set wire behavior. `lookup.json` SHA-256 is `7d56171dba60258e9ec45ad91c483f51c0a049a65adaf2f62212c416e780094a`; see [ADR-0071](../architecture/ADR-0071-shared-lite-subscription-set-aliases.md). Protocol tests/doctests, full workspace/all-target tests with local socket access, strict Clippy, pinned offline Maven verify (22 scenarios/cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. At this checkpoint, heartbeat collection aliases remained open and were later closed by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md); GetConsumerStatusBody/ResetOffsetBody map aliases were closed by `2f07fe94` / [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md); whole Phase 6 reconciliation remains PARTIAL; P6-V04 remains `PENDING_PHASE_19`.

LiteSubscriptionDTO nested alias correction (`60251cee`, 2026-10-10): the pinned Java 5.5.1 API fixture confirms the DTO returns the exact mutable `Set` and `OffsetOption` objects passed to its setters, with mutations visible from either side. Rust represents these fields with `SharedLiteSubscriptionTopicSet` and `SharedOffsetOption`; the fixture-backed Rust check asserts handle identity and both mutation directions while preserving JSON/`Display`. The DTO-specific observations remain unchanged; current `lookup.json` SHA-256 is `7d56171dba60258e9ec45ad91c483f51c0a049a65adaf2f62212c416e780094a`; see [ADR-0070](../architecture/ADR-0070-shared-lite-subscription-dto-aliases.md). Focused API/wire tests, workspace/all-target tests with local socket access, strict Clippy, workspace doctests, pinned offline Maven verify (22 scenarios/cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. At this checkpoint, heartbeat collection aliases remained open and were later closed by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md); GetConsumerStatusBody/ResetOffsetBody map aliases were closed by `2f07fe94` / [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md); whole Phase 6 reconciliation remains PARTIAL; P6-V04 remains `PENDING_PHASE_19`.

MessageRequestModeSerializeWrapper nested Map alias correction (`853b3fdd`, 2026-10-10): the direct pinned Java 5.5.1 oracle confirms the wrapper retains its assigned topic Map, each topic Map retains its consumer-group Map, and each group Map retains the exact mutable `SetMessageRequestModeRequestBody` objects; getters at all three levels return the same objects and caller/getter changes are visible in both directions. Rust now uses `SharedMessageRequestModeMap`, shared nested group maps, and shared request-body values, wrapping all levels during Fastjson decode. The public wrapper matches Java `Object.equals/hashCode` identity. The empty constructor default, null state, 33 Fastjson cases, and JSON wire shape remain covered. `lookup.json` SHA-256 at the wrapper alias checkpoint: `1a0df76edff7063ea8790fc0cfbf0321eca6469c766a9be191e6596f7403d799`; see [ADR-0069](../architecture/ADR-0069-shared-request-mode-wrapper-aliases.md). Workspace/all-target tests with local socket access, strict all-feature Clippy, workspace doctests, pinned offline Maven verify (22 scenarios/cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Other Phase 6 alias and source/input gaps remain; P6-V04 stays `PENDING_PHASE_19`.

QueryAssignment response Set element alias correction (`d312bb6c`, 2026-10-10): direct pinned Java 5.5.1 calls confirm that `QueryAssignmentResponseBody.setMessageQueueAssignments` retains the exact mutable `MessageQueueAssignment` element and the getter returns that same object; caller-held and getter-held mode mutations are visible in both directions. Rust now uses `HashSet<Option<SharedQueryAssignment>>` inside the shared outer Set, wraps decoded elements, and implements shared-handle value-based `Eq`/`Hash` to preserve the existing Rust Set membership semantics. The core runtime snapshots the outer Set and elements into its existing owned return type. Serialization is unchanged; mutating an assignment after Set insertion retains the Java-compatible mutable-key hash hazard. `lookup.json` SHA-256: `a4d4c85dc1ab17a240906f0e48348ebf2644c31bfcdd987d958fc1368b0e20de`; see [ADR-0068](../architecture/ADR-0068-shared-query-assignment-elements.md). Workspace/all-target tests with local socket access, strict all-feature Clippy, pinned offline Maven verify (22 scenarios and cross-decodes), comparator self-test/manifest22, format, and diff checks pass. Other Phase 6 aliases and whole-source/input reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

MessageQueueAssignment nested queue and attachments alias correction (`aa4803df`, 2026-10-10): pinned Java 5.5.1 confirms setter/getter identity for the nested mutable `MessageQueue` and attachments Map, with mutation visible through either reference. Rust uses `SharedAssignmentMessageQueue` and `SharedAssignmentAttachments`, wraps decoded nested values, and verifies handle identity and both mutation directions in the direct method differential. Fixture SHA-256 at this nested-field checkpoint: `cf03a6b58e3f312d8f1a4bc95a0384bc004338bc2d529141694bbf1810b63a71`. The response Set element identity gap in this checkpoint is closed by `d312bb6c` and [ADR-0068](../architecture/ADR-0068-shared-query-assignment-elements.md); P6-V04 remains `PENDING_PHASE_19`. See [ADR-0066](../architecture/ADR-0066-shared-assignment-nested-aliases.md).

Historical outer-Set checkpoint, superseded for element identity by `d312bb6c`: QueryAssignmentResponse Set alias and identity correction (`dbaed386`, 2026-10-10): pinned Java 5.5.1 confirms `setMessageQueueAssignments` retains its exact mutable Set, getter returns that same Set, and caller/getter mutations are bidirectional. Reflection confirms the DTO inherits `Object.equals`/`hashCode`; same-instance equality is true and equal-content distinct-instance equality is false. Rust now represents the outer collection as `Option<SharedValue<HashSet<Option<SharedQueryAssignment>>>>`, wraps Fastjson results and each element, returns cloned handles, verifies outer and element `ptr_eq`, and snapshots the Set and elements in the core runtime adapter to preserve its owned return type. Serialization is unchanged. Element identity is closed by `d312bb6c` and [ADR-0068](../architecture/ADR-0068-shared-query-assignment-elements.md). Fixture `lookup.json` SHA-256 at the outer-Set checkpoint `fb436699a2c7d128fd96c73e3f106add77904444fa581485a5ddc1d929bc251e`; see [ADR-0065](../architecture/ADR-0065-shared-query-assignment-set-alias.md). Focused protocol tests, workspace checks/tests, strict Clippy, pinned offline Maven verify, comparator/manifest, formatting, and diff checks pass. Other DTO aliases and full Phase 6 reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Consumer-ID notification correction (`17ecacc3`, Java oracle and differential `adbfa4c6`, runtime-state fix `362d6d32`): pinned Java 5.5.1 `ClientRemotingProcessor.notifyConsumerIdsChanged` decodes and logs the optional `consumerGroup`, calls `MQClientInstance.rebalanceImmediately()` for the whole runtime, catches handler exceptions, and returns null. The invoked fixture constructs a real `MQClientInstance` and `ClientRemotingProcessor`, sends code 40 through `EmbeddedChannel`, then observes `ServiceThread.hasNotified` change false→true with and without the group before the service starts; both Java calls return null. Rust now wakes its runtime-wide rebalance worker and returns no response without selecting a consumer capability, including in Created/Failed/Stopped states. Its fixture-backed paused-time test verifies a Created-state notification remains sticky until the worker starts, both registered consumers wake, and a second notification with no group wakes both again. The loopback test verifies wire silence. The updated `inbound-routing.json` SHA-256 is `81ebd0267107a1c01efff218e3d06dd659b5afc4c0aaf061466c60006ef62f51`. This closes code 40's Phase 6 runtime flow only. Full API/source/input reconciliation remains open and P6-V04 remains `PENDING_PHASE_19`.

Check-client public alias correction (`a732974a`, 2026-10-10): direct Java 5.5.1 `CheckClientRequestBody` cases now assert that `setSubscriptionData`/`getSubscriptionData` return the exact assigned `SubscriptionData`, that caller-side and getter-side mutations are mutually visible, and that `equals(Object)`/`hashCode()` are inherited from `java.lang.Object` (same instance true, equal-fields distinct instance false). Rust adds `SharedValue<T>` and migrates only this nested field; its fixture-backed test uses `ptr_eq` and both mutation paths. `CheckClientRequestBody` no longer has Rust structural `PartialEq`/`Eq`/`Hash` traits. Fixture `lookup.json` SHA-256: `dcb46e55ac972d2d621585f61fb28cc67fd398d8d5ada31bd1586a5abe07a8b3`. Protocol tests (108), full workspace/all-target tests, strict workspace Clippy, pinned offline Maven verify, comparator/manifest22, format, and diff checks pass. Other caller-owned DTO aliases and full Phase 6 source/API/input reconciliation remain open; P6-V04 remains `PENDING_PHASE_19`.
Consumer-list setter/getter alias correction (`456849fb`, 2026-10-10): the pinned Java `consumerListApiCase` proves that `GetConsumerListByGroupResponseBody.setConsumerIdList` retains the identical List returned by its getter; caller-side and getter-side mutations are mutually visible. Reflection records inherited `Object.equals` and `Object.hashCode`, and equal-content distinct DTOs remain unequal. Rust now stores `Option<SharedValue<Vec<Option<String>>>>`; its fixture-backed test verifies `ptr_eq`, both mutation paths, and identity methods. `MQClientApi::get_consumer_list_by_group` snapshots the list at its existing owned return boundary. Fixture `lookup.json` SHA-256: `372a5250f6af3ced4c17ef23eb9e05dbe78523ceb6b75a4cd03fe1589fabf80e`. Full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify, comparator/manifest22, fmt, and diff checks pass. Other DTO aliases and whole Phase 6 reconciliation remain open; P6-V04 remains `PENDING_PHASE_19`. See [ADR-0061](../architecture/ADR-0061-shared-consumer-list-alias.md).
Historical outer-container checkpoint, superseded for element aliases by `625122ee`: Lock-batch Set-container alias correction (`045b46e7`, 2026-10-10): direct Java calls prove each lock/unlock request `mqSet` and response `lockOKMQSet` getter returns the identical Set supplied to its setter; mutations through either reference are mutually visible. Reflection confirms all three DTOs inherit `Object.equals` and `Object.hashCode`. Rust now stores each nullable Set in `SharedValue<HashSet<Option<MessageQueue>>>`; fixture-backed tests assert `ptr_eq`, both mutation directions, defaults, null setters, Java text, and identity-method semantics. `MQClientApi::lock_batch_mq` snapshots to the established owned Set result. Fixture `lookup.json` SHA-256: `e2275802581a7a8c9d5271349377bf5b347d59d45af269e8e6569f1f682674b8`. Full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify, comparator/manifest22, fmt, and diff checks pass. Outer Set identity was resolved at this checkpoint; nested mutable `MessageQueue` element identity is now resolved by `625122ee` and [ADR-0067](../architecture/ADR-0067-shared-lock-set-element-aliases.md). See [ADR-0062](../architecture/ADR-0062-shared-lock-set-aliases.md).

Pre-start wakeup correction (`362d6d32`, 2026-10-10): Java's invoked fixture observed the sticky `ServiceThread` permit before worker startup, while Rust's processor guard previously returned `SYSTEM_ERROR` before reaching code 40 in non-running states. Moved code 40's runtime-wide signal ahead of the generic availability guard; other dispatched feature slots still return `SYSTEM_ERROR` while inactive. Focused inbound unit tests (9), loopback tests (2), and `cargo test --workspace --all-targets --locked` passed; strict workspace Clippy, formatting and diff checks also passed. Pinned offline Maven verification passed on the immediately preceding Java-fixture commit (`e3923960`); the state-order change does not alter the Java fixture. Full API/source/input reconciliation and non-macOS/real IPv6-only platform evidence remain open; P6-V04 remains `PENDING_PHASE_19`.

Post-correction gates (2026-10-10): `cargo test --workspace --all-targets --locked` exit0 with local socket access; the later fixture-backed inbound unit module passes all 9 tests. `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`, `cargo fmt --all -- --check`, `git diff --check`, differential comparator self-test, and `--validate-manifest` (22 scenarios) all exit0. Pinned offline Maven `verify` also exits0 with local interface access and the updated direct code 40 fixture; its existing Rust-generated cross-decodes and all 22 scenarios pass. Verification used the pre-existing artifact cache copied to `/private/tmp/p6-maven-repository` and set RocketMQ's log root to `/private/tmp/p6-rocketmq-logs`.

Earlier no-dispatch workspace gate record (before code 40 correction, 2026-10-10): `cargo test --workspace --all-targets --locked` passed with local socket access and the core library suite had 104 tests. Clippy, comparator self-test, manifest validation (22 scenarios), format and diff checks also passed. The current post-correction gate result is recorded above.

Pinned Java suite verification continuation (2026-10-10): reran `mvn -o -q -Dmaven.repo.local=/private/tmp/p6-maven-repository -f compat/java-reference/pom.xml verify` after the no-dispatch fixture update; it passed all 22 scenarios, runtime fixture verification and Java cross-decodes. The temporary repository copy avoided writing lock metadata to the read-only home cache; local-interface access was required by the pre-existing ClientConfig constructor oracle. No generated fixture drift remained.

Registered no-dispatch Lite request correction (2026-10-10): direct Java `ClientRemotingProcessor.processRequest` invocation for code 200073 returns `null`; bytecode shows seven switch codes and a `default: aconst_null`. Rust previously emitted `REQUEST_CODE_NOT_SUPPORTED`, so a Java-backed differential failed with response-present `true` versus Java `false`. The handler now returns `None` for this registered-but-undispatched code; loopback verifies no wire response while the seven dispatched inactive slots still return phase errors. Oracle/fixture `323e7ecb`; red regression `09d7acb2`; fix/wire check `ad411223`; fixture SHA-256 `85e0a87778cdfbca475d540e364c03352f017bda161e8773fea25692e2d837e4`. Direct `javac/java` fixture generation and `--verify`, eight private inbound unit tests, two loopback tests with local socket access, strict core Clippy, format and diff checks pass. Broader Phase 6 source/input/API reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Runtime inbound failed-state continuation (2026-10-10): extended the runtime availability guard regression to force startup failure with a zero route-poll interval and verify a bound processor in `RuntimeState::Failed` returns `SYSTEM_ERROR`. The test now covers unbound, Created, Failed, and Stopped states and is named `unbound_and_non_running_states_return_system_error`; commit `c8d8c50b`. Focused test, strict core Clippy, format check, and diff check pass. This appends failure-state coverage to the earlier unbound/Created/Stopped result; no production behavior changed.

Inactive producer inbound capability continuation (2026-10-10): added `registered_producer_without_transaction_or_reply_capability_returns_phase_errors`. A registered producer with the default absent callbacks returns `REQUEST_CODE_NOT_SUPPORTED` for transaction state (Phase 14) and reply correlation both without and with a correlation property (Phase 15). Test commit `c89adaba`; the seven-test inbound unit module, strict core/all-target/all-feature Clippy, format check, and diff check pass. No production behavior changed. Full Phase 6 source/input/API reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Inactive inbound capability continuation (2026-10-10): added `absent_consumer_group_and_handler_capabilities_return_phase_errors`. For all five consumer operation codes, it checks both an absent required group field and a registered consumer with no inbound capability; each returns `REQUEST_CODE_NOT_SUPPORTED` with the mapped Phase 9 or Phase 17 remark. Test commit `aee49ffa`; the six-test inbound unit module, strict core/all-target/all-feature Clippy, format check, and diff check pass. No production behavior changed. Full Phase 6 source/input/API reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Runtime inbound availability continuation (2026-10-10): added `unbound_created_and_stopped_processors_return_system_error`, directly invoking an unbound processor and the bound processor before startup and after shutdown. Each returns `SYSTEM_ERROR` with the corresponding unavailable/not-running remark. This closes a specific uncovered guard branch in the inbound matrix; no production behavior changed. Test commit `614487a6`; the focused test, strict core/all-target/all-feature Clippy, format check, and diff check pass. Whole Phase 6 source/input/API reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Pinned runtime public-method inventory continuation (2026-10-10): `javap -public` on the pinned `rocketmq-client-5.5.1.jar` (SHA-256 `1c39c94a28e59b292e29d699872d693cdc2ce30bd54c4e0f95a4c377fd9dff84`) lists 53 `MQClientInstance` declaration lines, including constructors and overloads, and seven `MQClientManager` method declarations; every distinct Java declaration name appears in the compatibility map. The planned `ClientRemotingProcessor`, `TopicPublishInfo`, `consumer.RebalanceService`, and `consumer.PullMessageService` inventories contribute 6, 18, 3, and 11 declarations respectively, and each class's distinct method names also appears in the map. Clarified the literal `replace_if_equal`/`remove_if_equal` names in the Rust table reverse inventory. This is a name-level check only and does not close full source branches, DTO inputs/aliases, non-macOS/real IPv6-only host evidence, or P6-V04, which remains `PENDING_PHASE_19`. No production behavior changed.

Periodic schedule Java call-site continuation (2026-10-10): invoked `MQClientInstance.startScheduledTask()` through reflection with a recording executor for absent and configured NameServer inputs. The pinned call records five/four `scheduleAtFixedRate` registrations: discovery 10000/120000ms, route 10ms/configured period, heartbeat 1000ms/configured period, persist 10000ms/configured period, and adjustment 1m/1m. The same pinned 5.5.1 bytecode confirms the runtime constructs a single-thread scheduled executor and wraps each periodic runnable with `Throwable` handling. Rust `scheduled_task_delays_and_periods_match_invoked_java_registration` compares both call sequences; the paused-clock suite still verifies default periods, catch-up, capability absence, and cancellation. Oracle/fixture `2f57160d`; Rust regression `8e0c5610`; fixture SHA-256 `98bc999e3880f25eefc1980ff55eeec7dac9264ddbec98c7b0db669fd6cdce82`; rocketmq-client jar SHA-256 `1c39c94a28e59b292e29d699872d693cdc2ce30bd54c4e0f95a4c377fd9dff84`. Core library suite (100 tests), strict core Clippy, pinned offline Maven verify, comparator self-test, manifest22, formatting, and diff checks pass. Broader source/API/input/reverse-export reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Periodic offset persistence capability continuation (2026-10-10): added `offset_persistence_skips_missing_capabilities_and_continues_ticking`, which registers one consumer returning no persistence future and another with a persistence callback. Paused time verifies both offsets at the 10-second first tick and 15-second next tick, while the task continues after skipping the absent operation. Test commit `e75f4042`; focused regression, strict core Clippy, formatting, and diff checks pass. Concrete offset stores remain Phase 9-owned; broader Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

BatchAck Base64 padding and trailing-bit continuation (2026-10-10): five direct Java 5.5.1 cases show unpadded `"BQE"` is accepted and re-encoded as `"BQE="`, noncanonical pad-bit forms `"BR=="` and `"BFB="` are accepted and normalized, while partial padding `"BQ="` and an internal tab reject. The initial Rust differential failed on `"BR=="`; `bitset_base64` now uses standard-alphabet engines that ignore unused trailing bits but retain separate strict padded and unpadded forms. Oracle/fixture `63dfa47f`; red test `ff888cff`; fix `2677a08b`; 111-case lookup fixture SHA-256 `e364ce565615327672ddacc78b05c1aa94322095fdba5ef78388605e5ff435ec`. Pinned offline Maven verification, the complete protocol crate suite, full workspace/all-target tests, strict all-feature Clippy, formatting, and diff checks pass. Broader source/API/input/reverse-export reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

BatchAck Base64 whitespace boundary continuation (2026-10-10): four new direct Java 5.5.1 inputs establish that leading tab, newline, carriage return, and nonbreaking space are rejected; the native differential agrees on all four. The 106-case corpus retains Java unpadded `"BQ"` acceptance and strict malformed-input rejection. Oracle/fixture `32ce689a`; native corpus assertion `238923da`; lookup fixture SHA-256 `beb05f8547613758325d0cd6638696d91bd04d78aee832ddd479b5cd280b3576`. Pinned offline Maven verification and the focused Java/native differential pass. Broader source/API/input/reverse-export reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

BatchAck BitSet Base64 continuation (2026-10-10): seven new direct Java 5.5.1 cases establish that the standard Base64 decoder accepts unpadded `"BQ"` and serializes it back as `"BQ=="`, while rejecting malformed length, excess padding, whitespace, URL-safe symbols, and invalid characters. The original native differential failed on unpadded input; Rust now falls back from padded to strict unpadded standard decoding. Oracle/fixture `c34f9dba`; red test `0350546f`; fix `cc3ebc70`; lookup fixture SHA-256 `b403bee9204f0adb029bb0be52320c542739612dabfc883621b8908b3b525440`. Maven fixture verification, focused BatchAck differential/tests, and strict protocol Clippy pass. Broader source/API/input/reverse-export reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

MessageQueue public API map continuation (2026-10-10): pinned `javap -public` confirms the no-argument, copy, and `(String, String, int)` constructors; three bean getter/setter pairs; `equals`, `hashCode`, `toString`, the typed `compareTo(MessageQueue)`, and its erased `compareTo(Object)` bridge. The invoked Java fixtures cover the typed comparison and other method outcomes; bridge bytecode first casts `Object` to `MessageQueue`, so a wrong runtime type raises `ClassCastException`. Rust's typed comparison cannot receive that case. Native comparisons now construct both Rust constructor forms from all three Java method rows, in addition to the existing setter, equality/hash/text, comparison, and decode inputs. Added dedicated API-map rows for those surfaces. No production behavior changed; whole Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

ServiceThread fixed join interval continuation (2026-10-10): exposed the pinned `getJoinTime()` value as public `runtime::SERVICE_THREAD_JOIN_TIME_MILLIS` and tied it to both invoked Java worker cases. Focused native oracle test, `cargo fmt --all -- --check`, and strict all-target/all-feature Clippy pass; implementation/test commit `5ce83461`. This records a constant only; per-worker join controls and broader worker lifecycle parity remain unimplemented. Full Phase 6 source/API/input/reverse-export reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Null offset-map namespace-branch continuation (2026-10-10): the null-table fixture now invokes Java with null, empty, and nonempty namespace values. All three cases throw `NullPointerException`, confirming `parseOffsetTableFromBroker` dereferences the table before its namespace branch. Oracle/fixture commit `ce3f2af2`; `offsets.json` SHA-256 `28d80e23c0f64a0e52a4a11020c24d79d58639e9955972a74c2ee04a2828f0b8`. Focused Java `--verify`, the native seven-nullable-case differential, and full offline Maven verify pass. Phase 6 source/API/input/reverse-export reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Nullable offset-table input correction (2026-10-10): five direct pinned Java 5.5.1 cases show that a null table throws `NullPointerException`; null keys and `Long` values pass through for null/empty namespace; nullable `MessageQueue` topic/broker fields pass through namespacing; and a null key throws when a nonempty namespace dereferences it. Rust now accepts the equivalent nullable table/key/value shapes and returns typed `RuntimeError::InvalidOffsetTable` for Java's null-table/key failures. The Rust method consumes the map, so Java's caller-visible in-place key mutation—including partial mutation before a later null-key failure—is outside the ownership contract. Oracle/fixture commit `73e6778e`; implementation/regression `f75dbe53`; `offsets.json` SHA-256 `059bb9872da7ffd85f1bd84e12c09787e0ed6cae6d5db6b113c02027b2902ac1`. Focused nullable differential, full workspace tests with localhost access, strict all-target/all-feature Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 source/API/input/reverse-export reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Windows interface enumeration compile-check continuation (2026-10-09): the host has the `x86_64-pc-windows-gnu` Rust target installed. The full core cross-check stops in `aws-lc-sys` because `x86_64-w64-mingw32-gcc` is unavailable before the core crate is checked. An isolated temporary Cargo crate including the actual `config/local_address.rs`, with pinned `windows-sys 0.61.2` features and `hostname 0.4.2`, passes `cargo check --target x86_64-pc-windows-gnu --tests --offline`. This proves the Windows-specific code and test module type-check; it does not execute `GetAdaptersAddresses`. Windows runtime enumeration, non-Windows-Unix behavior, and a real IPv6-only host remain unverified.


Null-topic runtime broker lookup continuation (2026-10-09): pinned 5.5.1 bytecode and a direct invocation show `MQClientInstance.getBrokerNameFromMessageQueue` looks up `topicEndPointsTable` with the queue topic before applying fallback; a null topic throws `NullPointerException` both before and after the retained-static-endpoint refresh. The red native regression showed Rust incorrectly returned `fallback-broker`; `RouteSnapshot::broker_name` now returns `Result<Option<&str>, RuntimeError>` and maps this input to `InvalidRoute("null message queue topic")`. Oracle/fixture `5125b311`; fix/regression `e323c8da`; current `route.json` SHA-256 `780023c54340d92c538a3b106affdafb657c88378c221add2dd224faa0fc8fb1`. Full workspace tests (with loopback access), strict all-target/all-feature Clippy, pinned offline Maven verify (25 route inputs, 22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 source/API/input/reverse-export reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.


Static endpoint unmapped-queue lookup continuation (2026-10-09): pinned 5.5.1 bytecode and an invoked `MQClientInstance.getBrokerNameFromMessageQueue` case confirm a nonempty endpoint map returns its direct lookup result, including null for an unmapped queue whose `brokerName` is `fallback-broker`; the result remains null after static-to-ordinary refresh retains that map. Oracle/fixture `c7b04506`; native differential `6242ec3b`; no production change. Current `route.json` SHA-256 `85f56d6b92fefbc0117bfe0a5118c8a3ba1b3cd6375a59e6d45877a41cf2f6a0`. Focused regression, pinned offline Maven verify (25 route inputs, 22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 source/API/input/reverse-export reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.


Public route-conversion API continuation (2026-10-09): pinned `javap -public` confirms both static `MQClientInstance` conversion methods are public. Rust exposes `TopicSnapshot::from_route_data` for non-null inputs and `from_nullable_route_data` for nullable Java arguments, sharing conversion with runtime publication. Ten direct Java cases show null topic remains null in dynamic, static and ordered publish/subscribe queues; null route throws `NullPointerException`, including when topic is also null, and Rust reports a typed `RuntimeError::InvalidRoute`. The 25 standard cases capture Java `TopicPublishInfo.getTopicRouteData()` and the Rust snapshot matches all DTO field values; all ten nullable cases also pass. Native implementation `92f8b358`, nullable converter `71903ef7`; oracle/input fixture commits `bc0972ab`/`e7b559d4`/`25647b57`/`96950fd1`; native differential `a858b6f7`/`88df2164`; current `route.json` SHA-256 `7c5f6b1c09937d21a63252e9c1359485ebde19b68284f63de1215781185ad6db`. Full workspace tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Other method/source/input reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Raw route cleanup null-broker-ID continuation (2026-10-09): the 21-case direct Java/native `runtimeRawCleanupShapes` matrix now includes a `brokerAddrs` entry with a null `Long` key and live `master:10911` value. `cleanOfflineBroker()` retains `broker/0 -> master:10911` and removes the stale slave, confirming cleanup treats the advertised address value as live without requiring a non-null broker ID. Oracle/fixture `90aab86e`; native regression `5dd34bc3`; current `route.json` SHA-256 `7c5f6b1c09937d21a63252e9c1359485ebde19b68284f63de1215781185ad6db`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Topic broker lookup null-ID continuation (2026-10-09): the pinned Java `MQClientInstance.findBrokerAddrByTopic` oracle now covers null `Long` broker IDs when their address is the only entry, when a master is also present, and when a slave is also present. Java accepts all three shapes: it returns the sole address, prefers the master, or returns one of the two slave candidates. The native randomized selection differential accepts the same candidate sets; no production change was needed. Oracle/fixture `63d8828f`; native regression `b3967a11`; current `lookup.json` SHA-256 `c4cb769d2d68a0f85c7008bd0bf56dca4f37bfee6a4736b7f76a0f801196ac01`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Topic broker lookup null-list continuation (2026-10-09): the direct Java `MQClientInstance.findBrokerAddrByTopic` oracle also covers a null `brokerDatas` list and a null BrokerData row; each throws `NullPointerException`. The new differential first exposed that Rust treated the null list like an empty list; `RouteSnapshot::broker_address_by_topic` now maps the null list to `RuntimeError::InvalidRoute`, matching the null-row error mapping. Oracle/fixture `9fea2779`; native regression `0a092277`; current `lookup.json` SHA-256 `c4cb769d2d68a0f85c7008bd0bf56dca4f37bfee6a4736b7f76a0f801196ac01`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Topic broker lookup missing-list continuation (2026-10-09): the pinned Java oracle distinguishes an omitted `brokerDatas` field from explicit JSON null: omission defaults to an empty broker list and returns no address, while explicit null throws `NullPointerException`. Rust's DTO decoder already preserves the same absent-versus-null distinction, so no production change was needed. Oracle/fixture `7ee83759`; native differential `9b7eb5a2`; current `lookup.json` SHA-256 `c4cb769d2d68a0f85c7008bd0bf56dca4f37bfee6a4736b7f76a0f801196ac01`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

BrokerData selector null-ID continuation (2026-10-09): the direct Java `BrokerData.selectBrokerAddr` matrix expands from seven to ten cases with a null ID as the only key, alongside a slave, and alongside a master plus slave. Java returns the sole address, either allowed value in random fallback, or the configured master; the native direct DTO method matches these candidate sets. No production change was needed. Oracle/fixture `0492c418`; native regression `5f481f17` (test cleanup `b07fecaf`); current `route.json` SHA-256 `7c5f6b1c09937d21a63252e9c1359485ebde19b68284f63de1215781185ad6db`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Raw route cleanup mutation-order continuation (2026-10-09): actual pinned Java cleanup removes an empty outer broker-address row immediately, but defers updates to nonempty inner maps until successful traversal. A later malformed BrokerData row therefore leaves the earlier empty-row removal visible; the separate failure-after-retained-candidate case still preserves the original table. The 20-case native differential matches both effects. Oracle/fixture `892db0a9`; Rust regression `bf3a8fce`; `route.json` SHA-256 `712d6d7c473c0211b01e12067e42b6c014a774407aef5f9dab650ecce99924cb`. Full workspace, strict Clippy, pinned Maven verify (22 scenarios), comparator/manifest22, formatting, and diff checks pass. Other raw/DTO combinations and whole Phase 6 reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded for caller-held BitSet alias identity by `7ba69080` / [ADR-0064](../architecture/ADR-0064-shared-batch-ack-nested-aliases.md): BatchAck BitSet clone-capacity continuation (2026-10-09): the pinned Java oracle shows cloning an empty `BitSet(130)` keeps `size() == 192`; setting bit 7 on the clone preserves the source's empty value. Rust matches both capacities, direct `toByteArray()` Base64 values (`""` and `gA==`), and source isolation. Oracle/fixture `85512a05`; native regression `24fb9f1b`; `lookup.json` SHA-256 `20f0de11e7d95a25143a5a1dcda2a07762f64177c8cf3303cecd1db2ee5192a7`. Full workspace, strict Clippy, Maven verify (22 scenarios), comparator/manifest22, formatting, and diff checks pass. Caller-owned BitSet alias identity was open at that checkpoint and was closed by `7ba69080`; whole Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded for caller-object alias status by `7ba69080`: BatchAck BitSet clone continuation (2026-10-09): pinned Java and Rust differentials confirm clone isolation: source bytes remain `JQ==` with bit 0 set and bit 7 clear, while the clone after `clear(0)`/`set(7)` is `pA==`. This verifies value behavior without claiming caller-owned Java object identity across Rust ownership. Oracle/fixture commit `b0dd6722`; native regression `6fb451f7`; `lookup.json` SHA-256 `df9a8c8523476111b203c6c3bfadfe1e65b444878b57befc896849724faf19db`. Full workspace tests, strict Clippy, Maven verify (22 scenarios), comparator/manifest22, formatting, and diff checks pass; whole Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

Raw cleanup null-map continuation (2026-10-09): after a valid row retains `master:10911`, the pinned Java oracle encounters a later BrokerData row with `brokerAddrs: null`, skips it, completes cleanup, retains the master and removes the stale slave. This contrasts with a later null BrokerData row, which throws before publication. The 18-case Rust differential matches. Oracle `6f34d97a`; native regression `301b5aee`; route fixture SHA-256 `a51795ae0027fbc9484383aeee94745d8a7c355ab895fb532496959c568a0c14`. Focused differential, full workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, formatting and diff checks pass. Other raw route combinations and whole Phase 6 source/API/input/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Raw route cleanup multi-topic and version-retention continuation (2026-10-09): the pinned Java oracle places no live address in one raw topic and retains `master:10911` only in a sibling topic; cleanup keeps the old broker-table master, removes its stale slave, and leaves version `551` unchanged before/after. The 17-case Rust differential matches both address and version tables. Oracle commits `b3361999`/`8568be4f`; native regressions `4f4e15ae`/`e9e48ce4`; route fixture SHA-256 `f56204a9b4161da0d54d1c432328d7bc74e676006de41b198b74a623558552b8`. Focused differential, full workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, formatting and diff checks pass. Other route shapes and whole Phase 6 source/API/input/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Raw route cleanup address-reuse continuation (2026-10-09): a pinned Java case advertises `master:10911` under a different broker name and ID than the existing table. `cleanOfflineBroker` retains the old `broker/0` mapping by address value and removes the stale slave, confirming cleanup liveness is address-only. The 16-case Rust differential matches. Oracle `32b66c4c`; native regression `fff6d696`; route fixture SHA-256 `2e4b3617e106fc212ca712bb7f51e0100afd7ab0dccb386947b74a494e16b0b3`. Focused differential, full workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, formatting and diff checks pass. Other malformed route combinations and whole Phase 6 source/API/input/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Raw cleanup deferred-failure continuation (2026-10-09): direct Java calls prove a master address is retained by the cleanup candidate table before a later stale-address check encounters a null BrokerData row and throws `NullPointerException`; cleanup leaves the original master/slave table unchanged. Rust's 15-case differential requires `RuntimeError::InvalidRoute` and the same snapshot Arc. Oracle `abc9bd41`; native regression `71388a9a`; route fixture SHA-256 `d012dbfdc0aa089c8dc9a63a5e46e0ba67907cf1537fa8c893e6649546197eac`. Full workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, formatting, and diff checks pass. Other malformed route shapes and whole Phase 6 source/API/input reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical value-API checkpoint; current shared-handle API is recorded above: BatchAck Java BitSet functional API continuation (2026-10-09): the owned Rust value now matches pinned Java cases for constructors and `size`, byte/long-array conversion, signed `toLongArray` words, index and range mutation including boolean overloads, `clear()`, `and/or/xor/andNot`, set-bit iteration, `intersects`, equality independent of capacity, Java hash and text, and capacity growth/clear behavior. Oracle commits `4196f933`, `2953f259`, `9dd083ed`, `79328f27`, and `a5385223`; implementation/test `0f600297`; checkpoint `lookup.json` SHA-256 `4a8f9a0b49c6e5acdcef48ee9da7bc9d81c5d28361d79f8a8400dd6636ae0719`. Full workspace tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, formatting, and diff checks pass. Rust slices cover Java byte/long-array values; NIO buffer cursor/identity behavior remains an ownership/API boundary; caller-held BitSet alias identity was closed later by `7ba69080` / [ADR-0064](../architecture/ADR-0064-shared-batch-ack-nested-aliases.md). Whole Phase 6 source/API/input reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Historical value-API checkpoint; current shared-handle API is recorded above: BatchAck BitSet range/search continuation (2026-10-09): the pinned Java oracle invokes range `set/clear/flip`, range `get`, next/previous set/clear searches, `intersects`, invalid ranges, and operations across bits 63–65. Rust's owned `BitSet` matches these results, including Java's `previous*Bit(-1)` behavior and typed invalid-input errors; BatchAck Base64 output is unchanged. Oracle commits `24bdefa4`/`1a4031d7`; implementation/test `6ccdafcd`; checkpoint `lookup.json` SHA-256 `5450d735158f927a4685d49fb17204bec2958129a6d2f15ba160b1d1653068ce`. Workspace tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, formatting, and diff checks pass. Caller-owned `BitSet` alias identity was open at that checkpoint and was closed by `7ba69080`; Java `ByteBuffer`/`LongBuffer` cursor mutation is not modeled by the Rust value API; Phase 6 stays PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Historical value-API checkpoint; current shared-handle API is recorded above: BatchAck BitSet API continuation (2026-10-09): pinned Java cases now cover `get`, `set`, `clear`, `flip`, cardinality, length, canonical byte encoding, word boundaries through bit 127, and `IndexOutOfBoundsException` for negative indexes. Rust replaces the raw byte-only mutation model with an owned `BitSet` and matches these outcomes while retaining the same Base64 wire body. Oracle/test commits `ce97d07c`/`98d26175`; implementation/test `7d2f2b98`; `lookup.json` SHA-256 `4ebfec17860c569431bfb2b7b952f59c83b3eb08d683b6198e4f24d6143cb400`. Full workspace tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, formatting, and diff checks pass. Caller-owned BitSet alias identity was open at that checkpoint and was closed by `7ba69080`; Java NIO buffer cursor behavior remains outside the value-level mapping, and wider public API/source reconciliation remains open; Phase 6 stays PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Raw route cleanup null-list continuation (2026-10-09): one additional actual pinned Java 5.5.1 `MQClientInstance.cleanOfflineBroker` case sets `brokerDatas` to null. Java throws `NullPointerException` and leaves the existing broker-address table intact; the Rust cleanup differential matches and requires the typed `RuntimeError::InvalidRoute` mapping for Java's null-dereference cases. Oracle/test commits `21d0f14b` and `8c7dc5f8`; route fixture SHA-256 `89067fe4aaeed5a57c5d557fe6006f81a3a91d6bb58974f9a415e4fbbe762944`. Full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, formatting and diff checks passed after the fixture addition; the tightened error-category regression and strict Clippy also pass. No production change was needed. Phase 6 remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded for caller/getter identity by `aa4803df` / [ADR-0066](../architecture/ADR-0066-shared-assignment-nested-aliases.md): MessageQueueAssignment nested queue getter continuation (2026-10-09): pinned Java confirms `getMessageQueue()` returns the object assigned by the setter and that queue-id mutation through the getter is visible through the assigned object. Rust adds `message_queue_mut()` and the differential matches the mutation through the owned nested queue; external Java object identity was not retained at that checkpoint. Oracle/test `caff0e22`; implementation `8fff4781`; checkpoint lookup SHA-256 `9cdbcae68d20569b3d0cfdbf774aaf4183a63430438d9e3d74e8ad2e64b863d1`. Full gates pass; Phase 6 remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded for caller/getter identity by `aa4803df` / [ADR-0066](../architecture/ADR-0066-shared-assignment-nested-aliases.md): MessageQueueAssignment attachment getter mutation continuation (2026-10-09): pinned Java confirms `getAttachments()` returns the Map assigned by `setAttachments()` and getter mutations are visible through both references. Rust already had `attachments_mut()`; the new differential verifies mutation through the DTO-owned map and its final contents. Rust did not preserve the caller-owned Java Map identity at that checkpoint. Oracle/test `359ed253`; checkpoint lookup SHA-256 `8b6dab97da3ebca80f494b07ebcc7b12da396d700587137e136ea8620ed8d972`. Full gates pass; Phase 6 remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded by `7ba69080`: BatchAck mutable BitSet getter continuation (2026-10-09): pinned Java proves `getBitSet()` returns the `BitSet` assigned by the setter; setting bit 5 through the getter is visible through both references. Rust adds `get_bit_set_mut()` for the owned byte representation and the differential matches the updated serialized bytes. The Rust `Vec<u8>` does not preserve the external Java object identity or expose the full Java `BitSet` API. Oracle/test `bd3d2e71`; implementation `d1dc7dfd`; current lookup SHA-256 `abfe31546b68799aced43ff4f631f5dea37a4c14efac1dfc60a1ed637a22b970`. Full gates pass; Phase 6 remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Historical getter-mutation checkpoint, superseded for caller aliases by `853b3fdd`: MessageRequestModeSerializeWrapper mutable Map getter continuation (2026-10-09): pinned Java mutations through the getter are visible in both the outer map and nested group map. Rust adds `get_message_request_mode_map_mut()` and the differential matches those mutations through its owned nested maps; Java's alias to the caller-owned Map is not preserved across Rust move ownership. Oracle/test commits `94a0d62d` and `0614eda5`; implementation `6c19de4c`; checkpoint lookup SHA-256 `c0ba2b2cf51eb3c900a90e57a807edb25bda2cee0fed1b9e54a303680814c6bb`. Workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting, and diff checks pass. Other DTO aliases and full source/reverse-export reconciliation remain open; Phase 6 stays PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded for caller aliases by `60251cee`: LiteSubscriptionDTO mutable OffsetOption getter continuation (2026-10-09): direct Java calls mutate the assigned `OffsetOption` and the DTO getter's returned object, confirming both views reach the same value. Rust adds `offset_option_mut()` and matches getter-side mutation; original Java object identity is not preserved by Rust ownership. Oracle/test `373dec0c`; implementation `6612a7fe`; current `lookup.json` SHA-256 `0880db9cf0e22cf22580c4c176e3413c7e3b2a2336fcc63dc6f81b616126fcaa`; focused differential, full workspace/all-target tests, strict Clippy, pinned Maven verify, comparator/manifest22, format, and diff checks pass. Other DTO aliases and full source/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical mutable Set getter checkpoint, superseded for caller/setter/getter identity by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md): Heartbeat DTO mutable Set getter continuation (2026-10-09): pinned Java cases now confirm changes to `SubscriptionData` tag/code Sets, `ConsumerData.subscriptionDataSet`, and both `HeartbeatData` collections through assigned Sets and getters. Rust exposed mutable borrows for owned HashSets/vectors and matched getter-side mutations at that checkpoint; the later shared-handle correction closes caller-owned Set identity. Oracle/test `33829680`; implementation `5151a308`; checkpoint `heartbeat.json` SHA-256 `a03cd054471d1782251f8635adcc35d6da29f67f9900c215eadf67f76f5d9c30`; [ADR-0058](../architecture/ADR-0058-heartbeat-dto-public-accessors.md). Focused differential, full workspace/all-target tests, strict Clippy, pinned Maven verify, comparator/manifest22, format, and diff checks pass. Other aliases and full source/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded for LiteSubscriptionDTO fields by `60251cee` and the control-body Set by `66adb0c4`: Lite subscription Set getter continuation (2026-10-09): pinned Java calls mutate `LiteSubscriptionDTO.liteTopicSet` and `LiteSubscriptionCtlRequestBody.subscriptionSet` through both setter-held and getter-returned Sets; Rust's existing mutable borrows match the final collections. Caller-owned Set identity remains an ownership difference. Oracle/test `4fd32a88`; current `lookup.json` SHA-256 `99d189c5eeba83f01b53a5ec23ab56cf5b902043db0849bd54a93ac2a3f3d743`. Focused differential, full workspace/all-target tests, strict Clippy, pinned Maven verify, comparator/manifest22, format, and diff checks pass. Other DTO aliases and full source/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded for caller Map identity by `2f07fe94` / [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md): ResetOffsetBody mutable Map getter continuation (2026-10-09): Java directly mutates the assigned `offsetTable` Map and the Map returned by its getter; both references observe additions. Rust adds `get_offset_table_mut` and the differential matches mutation through the owned rows. Java's alias to the caller-owned Map remains a Rust ownership difference. Oracle/test `a6e6d723`; implementation `ff81313a`; current lookup fixture SHA-256 `b77cd6ce3487c18e1c2d9b34c06ff2354f9c025ab65ac15a1255a3ed9fb31bc3`; [ADR-0057](../architecture/ADR-0057-runtime-map-body-public-api.md). Focused differential, full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, format, and diff checks pass. Other DTO aliases and whole-source/API/input/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded for caller Map identity by `2f07fe94` / [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md): Consumer status mutable Map getter continuation (2026-10-09): Java now directly verifies changes through both assigned Maps, getter-returned Maps, and a nested consumer Map. Rust adds `get_message_queue_table_mut` and `get_consumer_table_mut`; the differential matches mutation through the Rust-owned maps and nested rows. Java's alias to the caller-owned Maps remains a Rust ownership difference. Oracle/test `89aef2e2`; implementation `3d5cfd44`; current `lookup.json` SHA-256 `9fa29ff2a82f4227ebb8ea83c74c32702b6cf60f2b11e1eebbc8f0b1fff57da7`; [ADR-0057](../architecture/ADR-0057-runtime-map-body-public-api.md). Focused differential, full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, format, and diff checks pass. ResetOffsetBody and remaining DTO aliases plus full source/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded by `a732974a` (2026-10-10): the pinned Java body retains and returns the same `SubscriptionData`; mutation through the setter object and getter is mutually visible. At this checkpoint Rust added `get_subscription_data_mut`, and the differential checked changed `subVersion`, changed `subString`, and final nested fields, but caller-held identity remained unmatched. The later SharedValue correction closes this CheckClient alias; see the current audit entry above and [ADR-0060](../architecture/ADR-0060-shared-java-dto-reference-aliases.md). Oracle `cd8e761e`; implementation at that checkpoint `ecf19a87`; checkpoint lookup fixture SHA-256 `f3d9e92e8965faa082e06f075755bed7b9fc7d0435698b35a58d19ac809d8c29`. Other DTO aliases, malformed inputs, and source/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded by `a32d00f7` and then `7ba69080`: BatchAck mutable List getter continuation (2026-10-09): actual Java calls confirm the `acks` List can be mutated through the setter argument and getter, with changes visible through both references and final queue id 4. Rust adds `get_acks_mut()` for mutation through the body owner; the external List identity is still not represented under move ownership. Oracle `51b92e77`; implementation `f4d85af0`; lookup fixture SHA-256 `9a9c052d3d57d0c4e54e6f8adc1147c71bbfa1fbc07745d63e0920ab670f4442`. Focused differential, full workspace/all-target tests, strict Clippy, pinned Maven verify, comparator/manifest22, format, and diff checks pass. Remaining aliases, malformed inputs, and full source/reverse-export reconciliation are open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded by the QueryAssignment Set alias correction: Assignment response mutable Set getter continuation (2026-10-09): Java confirms mutations through the setter's `Set<MessageQueueAssignment>` and `getMessageQueueAssignments()` are mutually visible; native mutation through `get_message_queue_assignments_mut()` matches the final queue id 4. The caller-owned Set alias is not carried across Rust's move. Oracle `1bfe20bf`; implementation `b280111c`; lookup SHA-256 `ce344c0aa35c634a730f97cf706fba3b41313fd12a2fac294d3e01250f7e9b2c`. Focused differential, full workspace/all-target tests, strict Clippy, pinned Maven verify, comparator/manifest22, format, and diff checks pass. Other collection aliases, malformed inputs, and source/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded by `045b46e7` (2026-10-10): Java mutation through the setter and getter Set references was mutually visible for the lock request and response DTOs. Rust then used mutable borrows over owned HashSets, so its move setters lost external identity. The later SharedValue correction preserves all three outer Set aliases and matches the Java identity methods; nested `MessageQueue` object aliases remain open. Oracle `f80a844d`; implementation at that checkpoint `c8aeb821`; checkpoint lookup SHA-256 `8f8f823b9a63d14dd5cf4d9b86d0538ad5ebe142138d4316639ce3e60bc5fd6c`. Current evidence is recorded above and in [ADR-0062](../architecture/ADR-0062-shared-lock-set-aliases.md). Other DTO aliases, malformed inputs, and full source/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded by `456849fb` (2026-10-10): the pinned Java oracle mutates one List through setter and getter. At that checkpoint Rust had only `get_consumer_id_list_mut` over its owned Vec, so caller/getter identity remained a semantic gap. The later SharedValue correction closes this field alias; see the current audit entry above and [ADR-0061](../architecture/ADR-0061-shared-consumer-list-alias.md). Oracle `0877435c`; implementation at that checkpoint `dd4156c5`; checkpoint lookup fixture SHA-256 `3d031033fb4f04d0ab78df0bf7818827dec7a460e8264812f303667ee42c020b`. Other aliases, malformed inputs, and full source/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

SOCKS Fastjson bare-reference continuation (2026-10-09): four more actual Java `getProxy` cases cover empty, one-space, dotted-name, and numeric-name reference values. All retain a matching CIDR entry with no address; the bare numeric case has no matching root key, separating it from `$[0]`'s array-index failure. Rust already matches these outcomes. Oracle/test `47d09d76`; fixture has 499 constructor and 32 private selection cases at SHA-256 `fd46551f8755e1ca55a6434bdf15eabe58014077a402e8e17928b9eb8d9039d7`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Remaining Fastjson grammar and complete source/API/input/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

SOCKS Fastjson reference-path continuation (2026-10-09): nine actual pinned Java `getProxy` cases test dot-path, double-quoted and whitespace-bearing bracket-path resolution, numeric-index, self/parent/recursive forms, `#-2`, and a bare identifier. Java resolves the first three to the target proxy; the latter two accepted values retain the matched CIDR without an address; numeric-index, self, parent, and recursive forms fail. Rust matches the observable selection outcomes; the native differential first failed on bracket whitespace. Oracle/test `c90a1bec`; implementation `27700865`; 28 selection cases and 499 constructor cases at fixture SHA-256 `2099360007a6e46dbba89ba22c70d7ca7354fe49b2b5129a082bd42ff9f2b011`. Full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting, and diff checks pass. This closes only these path forms; broader grammar and final source/branch/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

SOCKS Java HashMap selection-order correction (2026-10-09): actual pinned private `getProxy` cases prove that among overlapping CIDRs Java chooses the first route in `HashMap` iteration order. Three JSON field permutations of the same `/8` through `/11` entries all select `/9`; a fourth case confirms source-bucket ordering when two keys collide in the destination bucket. The red native differential selected `/11`. Rust now retains root-map order and models Java source iteration and destination `putAll` bucket order. Oracle commits `99284f59` and `6f790b8a`; implementation `965109bd`; 19 selection cases at fixture SHA-256 `3e46fee1230a4dbf66837deca0a17015aeea87913fa9f3b08ad79e1aa0f90c42`. Full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test and manifest22 validation, formatting, and diff checks pass. This closes these selection-order cases; broader source/API/input and final-plan reconciliation remain open, and P6-V04 stays `PENDING_PHASE_19`.

SOCKS matched-route selection correction (2026-10-09): Java `getProxy` retains a matching CIDR entry when the route bean has no address; the oracle returned that CIDR with a null address. Native selection previously reported an error and lost the matched CIDR. Rust now carries CIDR and route separately and handles missing address at connection setup. Oracle/test `1ca984ac`; implementation `c4bcbbbb`; fixture SHA-256 `f3a1558b2722c74fe917bcba299484563ddcc4990b3db58419ac403fa2fb4a69`; 15 private selection cases. Full workspace, strict Clippy, pinned Maven verify, comparator/manifest22, format, and diff checks pass. Other source/API/input branches remain open; P6-V04 stays `PENDING_PHASE_19`.

SOCKS Fastjson special-reference correction (2026-10-09): direct Java construction and private proxy selection prove that `$ref` `#-1` is accepted as a no-address route, while root `$` fails construction with `ClassCastException`. Rust now reproduces the successful no-proxy selection and rejection boundary. Oracle/test `9a8dc90d`; implementation `8cc47fc8`; 499-case fixture SHA-256 `15e8e57a07b7a488f45750d10f5820ed41a49a2aba198254d7e2b85ba84f871a`. Full workspace, strict Clippy, pinned Maven verify, comparator/manifest22, format, and diff checks pass. Broader Fastjson and Phase 6 source/API/input reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

SOCKS Fastjson cycle correction (2026-10-09): pinned Java construction and private `getProxy` cases show successful construction and no-proxy selection for both self-references and two-route cycles. The expected-red native differential showed Rust rejected both; it now maps detected cycles to the equivalent direct/no-proxy route. Oracle/test commit `115a2cb1`; implementation `2f60397e`; the 497-case client-config fixture SHA-256 is `0434b8f5de37827ad98c145d60e9888a1d7e851f79e7872742047b4e760d8c03`. Focused tests, full workspace/all-target tests, strict all-feature Clippy, offline Maven verify (22 scenarios), comparator/manifest22, format, and diff checks pass. Broader Fastjson grammar and full Phase 6 source/API/input reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

SOCKS Fastjson object-reference correction (2026-10-09): direct pinned Java `MQClientInstance` construction and `NettyRemotingClient.getProxy` cases confirm `$ref` resolution for root-key paths, chained references, and unresolved keys. Resolved routes share the target `SocksProxyConfig` and preserve address/username/password; an unresolved path selects no proxy. Rust now resolves `$.key` and quoted `$['key']`/`$["key"]` root-key references before CIDR selection, materializing equivalent owned route values. The red differential failed on both reference address lookup and proxy selection. Oracle/test commit `80ba235d`; implementation `9cf5a8fb`; `client-config.json` SHA-256 `0cafefb33a4611746f78005582a3394e7a6de8818932ea3d9d947e99d3d1eb5e`. Full workspace tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, format and diff checks pass. Other Fastjson grammar and full Phase 6 reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

OffsetOption equals/hash contract update (2026-10-09): `OffsetOption.equals(Object)` now has direct pinned Java cases for equal/different values and types, null/wrong-type inputs, hash consistency/stability, and HashSet deduplication; Rust maps it with `java_equals(Option<&dyn Any>)`. The exact Java hash number is process-local through enum identity, while equality and collection behavior match. Oracle/implementation `e7fb09f6`; current lookup fixture SHA-256 `72e92cb461d8b4842622850296c01e0fd604b4d49250990bc3beea2cfcefc544`; [ADR-0059](../architecture/ADR-0059-offset-option-java-equality.md). Full workspace tests, strict Clippy, pinned offline Maven verify, differential comparator self-test and manifest22 validation, format, and diff checks pass. This resolves the portable equality/hash contract; broader Phase 6 source/API/input branches remain open and P6-V04 stays `PENDING_PHASE_19`.

Heartbeat DTO public API update (2026-10-09): direct pinned Java calls and a Rust differential now cover all declared getters/setters for `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData`, including defaults, populated values, and null setter outcomes. Java retains mutable Set aliases; Rust uses owned collections and immutable borrows. Oracle/implementation `21591fc7`; current heartbeat fixture SHA-256 `7da03948a79dd50a476ba900198731af6186efd735c473e54688853f3209cd87`; [ADR-0058](../architecture/ADR-0058-heartbeat-dto-public-accessors.md). Full workspace tests, strict Clippy, pinned offline Maven verify, format and diff checks pass. This closes the four DTO method surfaces while leaving collection alias behavior and wider DTO/input/source branches under audit; Phase 6 stays INCOMPLETE and P6-V04 remains `PENDING_PHASE_19`.

Historical checkpoint, superseded for caller Map identity by `2f07fe94` / [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md): Consumer status/reset-offset public API update (2026-10-09): direct pinned Java invocations now cover all map getters/setters, constructor-empty defaults, populated and nullable states, nested empty/null maps, and mutable Map identity for `GetConsumerStatusBody` and `ResetOffsetBody`. Rust method differentials match values/defaults/nulls but do not preserve Java's assigned Map aliases. Oracle/test `471b7160`; current `lookup.json` SHA-256 `7b2ce204660f804147471cb2d6d220e669e33991285b64cc68faf699c15a8aff`; [ADR-0057](../architecture/ADR-0057-runtime-map-body-public-api.md). Protocol all-target tests, pinned offline Maven verify, formatting, and diff checks pass. Phase 6 remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.

Runtime body public API update (2026-10-09; alias status updated 2026-10-10): direct pinned Java calls and Rust differentials cover constructor defaults, getters/setters, null setters, and Java text for CheckClientRequestBody, GetConsumerListByGroupResponseBody, LockBatchRequestBody, UnlockBatchRequestBody, LockBatchResponseBody, and ConsumeMessageDirectlyResult. Rust exposes each mapped accessor and matches the three diagnostic strings. The later `a732974a`, `456849fb`, and `045b46e7` SharedValue corrections match caller/getter identity and bidirectional mutation for CheckClientRequestBody.subscriptionData, GetConsumerListByGroupResponseBody.consumerIdList, and the three lock Set containers; `625122ee` closes the lock Sets’ nested mutable MessageQueue element aliases (ADR-0067). Other DTO aliases remain open. Oracle `7e1155f8`; implementation at that checkpoint `bca4329c`; checkpoint `lookup.json` SHA-256 `7b2ce204660f804147471cb2d6d220e669e33991285b64cc68faf699c15a8aff`; [ADR-0055](../architecture/ADR-0055-runtime-body-public-api.md) and [ADR-0060](../architecture/ADR-0060-shared-java-dto-reference-aliases.md), [ADR-0061](../architecture/ADR-0061-shared-consumer-list-alias.md), and [ADR-0062](../architecture/ADR-0062-shared-lock-set-aliases.md). Workspace/all-target tests, strict Clippy, pinned offline Maven verify, fmt and diff checks passed at the checkpoint. Phase 6 remains PARTIAL; P6-V04 remains `PENDING_PHASE_19`.

Historical checkpoint, superseded by the QueryAssignment Set alias correction: Assignment DTO public API update (2026-10-09): direct pinned Java cases now exercise constructor defaults, all getters/setters, and null setter behavior for `QueryAssignmentRequestBody`, `QueryAssignmentResponseBody`, and `SetMessageRequestModeRequestBody`. Rust matches the observed values and defaults, including `PULL` and zero for request mode. Java returns the same mutable assignment Set passed to its setter; Rust owns a HashSet and lends a reference, leaving that alias behavior as an explicit semantic gap. Oracle `7c7d8b5b`; implementation `fc5f2336`; checkpoint lookup.json SHA-256 `7b2ce204660f804147471cb2d6d220e669e33991285b64cc68faf699c15a8aff`; [ADR-0054](../architecture/ADR-0054-assignment-body-public-api.md). Workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes), fmt and diff checks pass. Other protocol DTO/enum/raw inputs and overall Phase 6 reconciliation remain open; Phase 6 stays PARTIAL and P6-V04 stays PENDING_PHASE_19.

Historical input/API checkpoint, superseded by `a32d00f7` and then `7ba69080`: BatchAck API/input correction (2026-10-09): the pinned Java 5.5.1 corpus has 95 direct BatchAckMessageRequestBody.decode/encode cases across roots, every token type for brokerName, acks, and compact ack fields, nullable rows, singleton conversion, duplicate-last behavior, and BitSet Base64. A direct API invocation also covers constructor defaults, all BatchAck getters/setters, body getters/setters, and `toString()`. Rust exposes `get_acks_mut()` and matches List mutation through the getter; identity with the external List passed into the owned Vec remains different. Original API oracle/implementation `7f3a4f0e` / `c709e743`; mutable List oracle/implementation `51b92e77` / `f4d85af0`; checkpoint lookup.json SHA-256 `9a9c052d3d57d0c4e54e6f8adc1147c71bbfa1fbc07745d63e0920ab670f4442`; see [ADR-0052](../architecture/ADR-0052-batch-ack-fastjson-input.md) and [ADR-0053](../architecture/ADR-0053-batch-ack-public-api.md). Workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios, fixture drift and cross-decodes), fmt and diff checks pass. Other protocol DTO/enum/raw inputs and overall Phase 6 reconciliation remain open; Phase 6 stays PARTIAL and P6-V04 stays PENDING_PHASE_19.

Unregister request API correction (2026-10-09): pinned RocketMQ 5.5.1 artifacts contain neither `UnregisterClientRequestBody` nor `UnregisterClientBody`; `MQClientAPIImpl.unregisterClient` sends command 35 with `UnregisterClientRequestHeader` and no body. Removed both Rust-only public DTOs and corrected the request-code inventory; the Java-shaped header remains. Implementations `0c60e2b1`/`a1f8d747`; full workspace tests, strict Clippy, formatting and diff checks pass. [ADR-0051](../architecture/ADR-0051-remove-non-java-unregister-body.md) records the artifact and bytecode evidence. This narrows one public API mismatch; Phase 6 stays PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

MessageRequestModeSerializeWrapper default, API, and Fastjson correction (2026-10-09): the pinned Java 5.5.1 oracle now invokes the public constructor/getter/setter and exercises 33 JSON input cases across root shapes, nested map levels, duplicate properties/keys, nulls, and invalid shapes. Java's constructor supplies an empty non-null map; missing input retains it, duplicate wrapper fields are last-wins, and null maps are omitted on encoding. Rust now matches these behaviors with an explicit default, Java-shaped accessors, and the shared raw-object last-wins decoder. Oracle corpus/API commits 4456695d/910e6838; implementation 33b993b8; lookup.json SHA-256 9d4472bee58e739dbb0efa4c18a3e4405f7b58fd1e1975f862bbc431b61a016f; [ADR-0050](../architecture/ADR-0050-message-request-mode-wrapper-fastjson-input.md) records the boundary. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios with fixture checks), formatting and diff checks pass. This closes only the tested wrapper boundary; Phase 6 remains PARTIAL and P6-V04 stays PENDING_PHASE_19.

Reset offset DTO map-input correction (2026-10-09): the expanded direct pinned Java corpus has 77 cases and establishes constructor-empty default versus explicit-null table state, Fastjson object-valued `MessageQueue` keys, quoted-key rejection, duplicate-key/body-field last-wins behavior, and nullable `Long` values omitted during encoding. Added value probes show booleans convert to 1/0, numeric strings including a leading plus are accepted, decimal strings/fractions truncate toward zero, empty strings become null, whitespace-surrounded numeric strings and overflowing integer tokens reject, plain decimal overflow wraps to 64 bits, and extreme exponent tokens/strings saturate or underflow to zero like Java double-to-long conversion. Rust preserves nullable offsets, exposes Java-shaped accessors, and routes body input/output through explicit Fastjson-compatible methods. Fixture `lookup.json` SHA-256: `202dc2d502473e2e2a73861bfcc82e359668c8f5222744b619052be04c9c014f`; test commits `76cc4eb`/`c7e1e8c`/`9cfd106`/`2260129`/`85cd833`, implementations `f6a75eb`/`eadcb33`; [ADR-0048](../architecture/ADR-0048-reset-offset-fastjson-map-input.md). Full workspace/all-target tests with loopback permission, strict Clippy, pinned offline Maven verify (22 scenarios and Java fixture drift/cross-decodes), comparator/manifest22, fmt and diff pass. This closes only the tested DTO boundary; overall reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Consumer status DTO map-input correction (2026-10-09): the expanded direct pinned Java corpus has 143 cases and exposed non-standard object-valued `MessageQueue` map keys, explicit-null map states, and nullable `Long` values that the prior Rust fields could not represent. Rust now preserves constructor-empty versus explicit-null outer maps, nullable nested consumer maps and offsets, queue-key equality deduplication, duplicate-field last-wins, and Java body encoding omissions. Added value probes confirm booleans map to 1/0; numeric strings, including a leading plus, and decimal strings truncate as Java does; whitespace-surrounded numeric strings reject and empty strings become null; fractions truncate toward zero; plain decimal overflow wraps to 64 bits; extreme exponent tokens/strings follow double-to-long saturation or underflow; and integer overflow rejects. Null values survive decode and are omitted when encoded; null queue names are omitted from object-valued keys while queueId remains present. All seven value token kinds were exercised for each MessageQueue key field. The corpus covers every JSON token type for both tables and non-object roots, and confirms quoted serialized-queue keys and malformed table shapes reject. Fixture SHA-256: `202dc2d502473e2e2a73861bfcc82e359668c8f5222744b619052be04c9c014f`; test commits `76cc4eb`/`c7e1e8c`/`9cfd106`/`2260129`/`85cd833`, implementations `f6a75eb`/`eadcb33`; [ADR-0047](../architecture/ADR-0047-consumer-status-fastjson-map-input.md). Workspace/all-target tests with loopback permission, strict Clippy, pinned offline Maven verify (22 scenarios and Java fixture drift), comparator/manifest22, fmt and diff pass. This closes only the tested DTO boundary; overall source/API/settings/input reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

ConsumeMessageDirectlyResult field-input correction (2026-10-09): the 66-case pinned Java matrix adds seven token shapes for all five fields, seven roots, and repeated-field last-wins probes. Java's primitive defaults/coercions, Fastjson String conversion for `remark`, numeric/null/Boolean handling for `spentTimeMills`, and enum conversion all match Rust after the custom last-wins decoder fix; a follow-up regression caught and fixed the public `Default` value (`autoCommit=true`) to match Java construction. Oracle/test `910cbd2`; implementation `00d2ea8`; constructor-default regression/fix `6d53470`/`b4ec6f4`; lookup fixture SHA-256 `c7662fe1bbbe62726eef0eda14774a3bc08b3910581d771153c3b7b04eccaf40`; [ADR-0049](../architecture/ADR-0049-consume-result-fastjson-input.md). Workspace tests with loopback access, strict Clippy, pinned offline Maven verify (22 scenarios and fixture checks), fmt and diff pass. This closes the tested DTO input surface only; Phase 6 remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Assignment response fraction field continuation (2026-10-09): pinned Java rejects a fractional scalar value at `messageQueueAssignments`; the expanded 30-case field matrix now includes `1.5`, and Java/Rust both reject it. Seven root-shape cases bring the full response corpus to 37. Test commit `3e58bc1`; lookup fixture SHA-256 `4723f8ad95ae556dbc99db83ef81cc311f0aae128cf59e23f8bbda6b1045618a`. No production change was required; Phase 6 remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Lite identity, API, and class-name reconciliation (2026-10-09): pinned 5.5.1 direct Java cases confirm identity-based equality for `LiteSubscriptionDTO`, `LiteSubscription`, and `LiteSubscriptionCtlRequestBody`; Rust structural equality was removed. Added `LiteSubscriptionDTO` and `GetConsumerListByGroupResponseBody` aliases. Rust now maps LiteSubscription fields, defaults, mutation and timestamp methods; DTO and OffsetOption accessors/constructors/constants; control-body Set accessors; and the tested `toString` forms. The direct input corpus covers 16 LiteSubscription Fastjson cases, and OffsetOption value equality collapses duplicates in Rust HashSet as Java does. Fixture SHA-256 at that update: `d618ea8b60671b98fc7c8ec53746c2a0f4bddcb7c744d061b934d38cd6981538`. The enum identity-derived Java hash number is process-local; same-process stability and equal-hash contract cases now pass, while Rust HashSet membership matches Java. Broader source/API/settings/input/branch/reverse-export work remains open; P6-V04 stays `PENDING_PHASE_19`.

Lock/unlock batch DTO and reverse-export continuation (2026-10-09): pinned Java 5.5.1 Set declarations and 117 direct field/member/root cases are now represented by Rust nullable HashSets with Java equality deduplication, Fastjson coercions, duplicate last-wins handling, and Java constructor defaults. `MQClientApi::lock_batch_mq` retains the nullable Set result. The pinned client/common/remoting artifacts include `LockBatchResponseBody` and no `UnlockBatchResponseBody`; the unused Rust-only DTO has been removed; both response paths use the Java `LockBatchResponseBody`. Lookup fixture SHA-256: `29211416d6c3f8c9af305fbe18e66b8ad1ada7c251333f69b8cd2f7970938d49`; [ADR-0046](../architecture/ADR-0046-lock-batch-set-fastjson-input.md). Broader method/setting/source/input/reverse-export audit remains open; P6-V04 stays `PENDING_PHASE_19`.

Lite subscription DTO source/input continuation (2026-10-09): pinned Java declarations show `subscriptionSet` is `Set<LiteSubscriptionDTO>`, while both `LiteSubscriptionDTO.liteTopicSet` and `LiteSubscription.liteTopicSet` are `Set<String>`. A combined 114-case oracle covers all DTO field token shapes, roots, Set element/duplicate behavior, primitive long conversion, enum/default/duplicate-field cases. Red tests exposed Java String coercions, primitive long coercions, and duplicate null handling; the native decoder now matches. Java's default identity equality retains separate equal-valued DTO instances, while repeated null Set members collapse; the native vector preserves object occurrences and one null member, with array order normalized because Java hash order is unspecified. Current lookup fixture hash: `ecb83ddd48d7d19fd3e8e015bc6d281b67f96d1c99e59091ace43d4d86c41756`; see [ADR-0040](../architecture/ADR-0040-lite-subscription-list-fastjson-input.md). Other protocol DTO/raw/source/branch reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Runtime DTO root-shape continuation (2026-10-09): seven source-used root probes each were added for `QueryAssignmentResponseBody.decode`, `GetConsumerListByGroupResponseBody.decode`, and the `CheckClientRequestBody` first-access path. Java rejects null at the getter and rejects array/scalar roots; `{}` decodes. Rust differentials match acceptance, fields, and response re-encoding. Current counts are 37 assignment-response, 23 consumer-list, and 43 check-client body inputs. Lookup fixture SHA-256 `9d4472bee58e739dbb0efa4c18a3e4405f7b58fd1e1975f862bbc431b61a016f`; client-check fixture `cdd164bbcaf8cb7a096828b289f69c56c2706c965e97bfe4f228b8ad08c16ee2`. Full workspace tests, strict all-feature Clippy, offline Maven verify, comparator/manifest22, fmt and diff checks pass. No production change was needed; other DTO/raw/source/branch reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

Route nullable-key continuation (2026-10-09): four direct Java 5.5.1 DTO inputs show that quoted and unquoted `null` keys are accepted as an actual null `Long` key by `BrokerData.brokerAddrs`, but rejected as `Integer` keys by `TopicQueueMappingInfo.currIdMap`. Rust stores nullable broker IDs as `Option<i64>` and preserves the Java distinction through parsing and JSON output. Pinned Java bytecode confirms null-ID unboxing failures in admin/fallback lookup and heartbeat paths; native route methods return `InvalidRoute` for the same branches. Route DTO coverage is 399 cases at fixture SHA-256 `0472ed8171207d35519adae7521741d359e343a3c378cf75537cb5c29cb41313`; focused route and core regressions pass. `P6-V04` stays `PENDING_PHASE_19`; final Phase 6 source/API/input/reverse-export reconciliation remains open.

Assignment request DTO field/root continuation (2026-10-09): expanded the pinned Java 5.5.1 lookup oracle to cover all seven JSON value kinds for every field in `QueryAssignmentRequestBody` (35 cases) and `SetMessageRequestModeRequestBody` (28 cases), plus seven root shapes for each. Java's String scalar/collection coercions and `popShareQueueNum` object-to-zero behavior exposed Rust mismatches; the implementation now uses the shared Fastjson String adapter and a narrow primitive integer adapter. Existing enum/default cases remain 10 and 14. All four new field/root differentials pass; current `lookup.json` SHA-256 is `67d4aaf007e77c7feae7e5db937a72869ffa421c77563fd47e9cc826624fb335`. Full workspace, strict Clippy, pinned Maven fixture verify, comparator/manifest22, fmt and diff pass. Other DTO/raw input and platform-specific address selection audits remain open; P6-V04 remains `PENDING_PHASE_19`.

Heartbeat DTO root-token input continuation (2026-10-09): expanded the pinned Fastjson matrix by 28 cases, testing seven root JSON forms for each of four public heartbeat DTOs. `JSON.parseObject` returns null for null roots, and empty objects decode; arrays and scalar roots reject. Rust matches with optional DTO decoding; the empty SubscriptionData timestamp is excluded from value comparison because Java assigns it from wall clock. The full corpus is 241 cases at fixture SHA-256 `385fc38890b6f18fc9ea74a4a4d2a47f52c27bae5ffc7851064c594cef426803`. Focused differential passes with no production change; other DTO/raw inputs and final Phase 6 reconciliation remain open.

Route DTO root-token input continuation (2026-10-09): direct Java 5.5.1 probes cover empty object, null, empty/singleton array, number, boolean, and string roots for each of five route DTO types. Empty objects decode; null DTOs fail on field access and other non-object roots reject. Rust matches the native decoder outcome and fields for the full 35-case matrix without an implementation change. The route DTO differential now covers 395 inputs at fixture SHA-256 `4c5634c4ddce8a311db77fe458e0bf16f63288e0bcd116f53af1c294700ed323`. Other malformed DTO/raw combinations and final source/API/reverse-export reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

ClientConfig TLS property lifecycle continuation (2026-10-09): `TlsSystemConfig.tlsEnable` is a Java class-initialized snapshot, so changing `tls.enable` after the first ClientConfig construction does not affect later ClientConfig objects. The expanded fresh-JVM oracle records false both before and after the mutation; Rust's new `OnceLock` snapshot matches in an isolated child-process regression. Test commit `ddb56eb`, implementation `78df3d7`; `client-config-tls.json` SHA-256 `018510bf6f67c1e6a556fb0dffc1ed50ba64cab2930f4785dfa421cd466b0c68`. Full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify (22 scenarios and deterministic fixtures), comparator self-test/manifest22, fmt and diff pass. This closes the TLS property lifecycle boundary only; remaining constructor/activation and platform settings paths remain under audit, and P6-V04 stays `PENDING_PHASE_19`.

Automatic client-IP candidate-selection continuation (2026-10-09): inspected the pinned Java 5.5.1 `NetworkUtil.getLocalInetAddressList/getLocalInetAddress` bytecode and used the fixture's direct ordered candidate list. New macOS test `local_address_selection_matches_invoked_java_candidates_and_ipv6_fallback` confirms the native adapter's IPv4 sequence and Java-equivalent selected address for the full current host list and its IPv6-only subset. Java and native full enumeration order differs; the bounded selected results match. Pinned 5.5.1 bytecode establishes first-non-internal IPv6 selection and first-candidate fallback; native regression 8632ec69 distinguishes the first of two external candidates. Non-macOS interface enumeration and a real IPv6-only host remain unverified; no production change was needed. Settings activation paths and whole-phase source/API/input reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Windows interface eligibility follow-up (2026-10-09): pinned RocketMQ 5.5.1 `NetworkUtil` bytecode invokes `NetworkInterface.isUp()` and `isPointToPoint()`. Windows implementation `2d7540ba` now queries `MIB_IF_ROW2` by adapter LUID with `GetIfEntry2`, then requires administrative and operational UP states and excludes `NET_IF_ACCESS_POINT_TO_POINT`. The predicate unit test, full workspace tests, strict all-target/all-feature Clippy, formatting/diff checks, and isolated cross-target type-check of the actual Windows module/tests pass. A shadow workspace under `/private/tmp` also passes a whole-core Windows-target `cargo check` after its temporary manifest disables the crypto provider and its temporary `namesrv.rs` omits only the AWS-LC provider-install line; this type-checks the core Windows cfg paths but is not a production TLS-provider build. The ordinary production-feature full core cross-target build still stops in `aws-lc-sys` because MinGW GCC is unavailable. The native behavior was corroborated against current OpenJDK upstream Windows code and Microsoft API documentation; the pinned JDK 27 source archive lacks native C, so this is not an exact pinned-JDK native-source comparison. Windows enumeration did not run. Actual Windows enumeration, other non-macOS ordering, and an IPv6-only host remain unverified; no Phase 6 acceptance claim is made.

Route DTO numeric-key continuation (2026-10-09): expanded direct Java probes to cover unquoted signed integers, leading zeros, plus signs, fractions, and exponents in numeric-key maps, plus numeric keys in string-keyed route maps. Java integer maps coerce these values to Long/Integer keys; string maps canonicalize using Java number text (`+1` to `1`, `1e2` to `100.0`). Root bean numeric keys reject, while unknown/object-to-String nested objects accept numeric map keys. A red differential exposed failures on plus/fraction/exponent keys and root-key over-acceptance; the context-aware route normalizer and Fastjson object stringifier now match. All 360 route DTO inputs pass; fixture SHA-256 `e0a36ac2b1139acacd11536ba9a22bc3532d4b200ec8eabb21b1d0f6a82995e4`. Focused differential, full workspace tests, strict Clippy, pinned Maven verify (22 scenarios, drift/cross-decodes), comparator self-test, manifest22, fmt and diff pass. The sandboxed test attempt denied three loopback binds; the rerun with local socket access passed. Other DTO combinations and complete source/API/reverse-export reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Nested route collection/member continuation (2026-10-09): extended the Java route DTO corpus to 337 cases. New token-shape cases cover `queueDatas` and `brokerDatas` elements, filter-server map values, and topic-mapping map values; Java accepts only source-compatible null/object DTO rows and map values, while filter strings/arrays/objects coerce to lists and empty string yields empty. Rust initially failed Java-accepted string/object/empty filter values; the shared Fastjson route-list decoder now matches them. All 337 route DTO inputs at that checkpoint passed; fixture SHA-256 `3d631d0c40c1a6f96a1a4250d1e1c7012c2062551d65d073a84b8f9a8c8ac375`. Other DTO combinations and complete source/API/reverse-export reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Static mapping entry-value continuation (2026-10-09, prior checkpoint): seven pinned Java cases exercised all token kinds inside `TopicQueueMappingInfo.currIdMap` values. Java rejected null, nonnumeric text, and arrays; it accepted integers, fraction truncation, Boolean true to one, and `{}` to zero. The 308-case checkpoint fixture SHA-256 was `936ee195d1e2a02268a59c210e5b2732f16b1725904b1d9e78bf8418e063b071`.

Nested route-collection member continuation (2026-10-09): added 14 pinned Java Fastjson cases for each of the seven JSON value kinds inside `BrokerData.brokerAddrs` map values and `TopicRouteData.filterServerTable` list members. All are accepted; null remains null and non-string scalar/array/object members use Java string coercion. The Rust route differential matches all 301 cases without a production change. Fixture SHA-256 `8de4b09d06779e0f5bbe11d3bad07fbbadf792a2eb2dfa39af1bbf1158c43a0c`. Other DTO combinations and full source/API/reverse-export reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Raw route cleanup default-input continuation (2026-10-09): expanded direct Java `cleanOfflineBroker` evidence from 8 to 13 runtime input shapes. Added absent route fields, an empty broker list, a missing address map, a null broker name with a live address, and ignored null queue/filter collections with a live address. Java/native results match for all 13; fixture SHA-256 `b7f7653977913cd8a545f9d9d59153a459ca343ced1dcc897d92302a4b926df0`. Other malformed DTO/raw combinations and full source/reverse-export reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Assignment response nested-queue continuation (2026-10-09): expanded the pinned `QueryAssignmentResponseBody.decode` oracle from 22 to 29 cases with null, string, number, Boolean, array, empty-object, and populated-object values for `MessageQueueAssignment.messageQueue`. Java accepts null and both object forms, decodes an empty queue to null topic/broker and queueId zero, and rejects the scalar/array forms. The native differential matches decoded accessor values and re-encoded shape; no production change was required. Current `lookup.json` SHA-256 `a9d27f0941069a6424a74dcfe6e70bc7f9cff21acdbe74d85da342423ef6b5d0`. Focused protocol differential passes. Other DTO/input combinations and source/branch reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Earlier assignment response array-element continuation (2026-10-09): expanded the pinned `QueryAssignmentResponseBody.decode` oracle from 15 to 22 cases. Java rejects string, number, Boolean, and nested-array non-null members of `messageQueueAssignments`, accepts `[null,{}]` while preserving the null set member, and rejects a mixed scalar/object array. Rust's `assignment_body_inputs_match_invoked_java_fastjson` matches all 22 acceptance, decoded-value, and re-encoding outcomes; no production change was required. At that checkpoint `lookup.json` SHA-256 was `778b74e60be12bd4a29efd484e467ae055ed525a1a8f8207c04182b580627bde`. Other DTO/input combinations and source/branch reconciliation remained open; P6-V04 stays PENDING_PHASE_19.

Registered-consumer static-route null-queue continuation (2026-10-09): added a direct pinned Java runtime case for null `queueDatas` with a nonempty static mapping and registered consumer. Java refresh succeeds and caches the copy-normalized empty queue list; the ordered null-queue consumer case remains a `NullPointerException`. The native loopback test first exposed a broad consumer guard, which now exempts static-mapping routes; native assertions cover callback effects. Both focused consumer runtime cases pass. `route.json` SHA-256 `add6716236d15355b415cdf77a994bc1bd7ccf36135f11ac29f5b052a3a07355`. This closes that route branch only; malformed route combinations and full source/API/input/reverse-export reconciliation remain open, and P6-V04 remains PENDING_PHASE_19.

SOCKS Fastjson escape-follower continuation (2026-10-09): the Java oracle scans all 65,536 BMP UTF-16 units and all 1,048,576 supplementary scalars after a backslash inside a route address. Java accepts 15,872 BMP units and 253,952 supplementary scalars; Rust matches parsed values and rejection for all 63,488 representable BMP scalars and every supplementary scalar. Isolated Java surrogate units are not representable in Rust strings. Both focused remoting tests pass; `client-config.json` SHA-256 `bf9b63dfb9c38a4f4090a4326a5d14bc9bff62478e4df4330825cb5009bc08f3`. Other Fastjson grammar and complete source/reverse-export reconciliation remain open.

Raw route cleanup input continuation (2026-10-09, prior checkpoint): the direct Java runtime matrix had eight cases for null route collections, null `BrokerData` rows, null/empty address maps, and null/live address values. Java throws before mutating `brokerAddrTable` for null broker lists/rows, skips null address maps, and retains null address values when present in both route states. Route fixture SHA-256 at that checkpoint: `340c6a0e79a2657efb0de3496e5da003ff821fda8434d3572a3e45c407b69102`.

CheckClientRequestBody input continuation (2026-10-09): expanded direct Java Fastjson evidence from 8 to 36 cases with null, numeric string, integer, fraction, Boolean, array, and object values on `clientId`, `group`, `subscriptionData`, and `namespace`. All Java/native accepted values and rejection outcomes match; nested `SubscriptionData.subVersion` is pinned for deterministic output. The focused test passes with no production change. Fixture SHA-256 `7e9d758ce72e0282d484dc41404e77a318d922662def78988855fe4dc8958570`. Other DTO families and whole-phase source/reverse-export reconciliation remain open.

Heartbeat DTO field-shape continuation (2026-10-09): direct pinned Java input coverage grew from 73 to 213 cases with all seven JSON token kinds across 20 fields in four heartbeat DTOs. A first comparison had nondeterministic timestamp defaults because unrelated `SubscriptionData` probes omitted `subVersion`; the generator now pins that field to zero except when directly probing `subVersion`. Java explicit null and the existing native decoder both produce zero, while omission retains the timestamp. All 213 cases pass with no production change. Heartbeat fixture SHA-256 `4c6aee94aaec527dfb864bea1bed8d6f2903132e6e73e3a353c3ce08a77affdf`. Other DTO/input and Phase 6 source/reverse-export reconciliation remain open.

SOCKS proxy DTO field-shape continuation (2026-10-09): direct pinned Java Fastjson coverage now includes all seven JSON value kinds for `addr`, `username`, and `password` (21 cases). All inputs parse; native conversion matches Java's string coercions. The initial comparison also found that native missing-address entries discarded otherwise decoded credentials, so `ParsedProxyRoute::MissingAddress` now retains them. Focused differential passes. `client-config.json` SHA-256 `d86eef5bdf622121721fe6c0663879b9704b709cb84e21ca0aab6198e3b18174`. This closes these field shapes only; broader Fastjson grammar and Phase 6 reconciliation remain open.

Route DTO field-shape continuation (2026-10-09, prior checkpoint): expanded pinned Java Fastjson inputs from 133 to 287, adding all seven JSON kinds for every field in `QueueData`, `BrokerData`, `TopicQueueMappingInfo`, and `TopicRouteData` (154 direct cases). The first native comparison failed because empty-object `TopicQueueMappingInfo.epoch` became 0; both the public DTO decoder and last-wins intermediate reject the object, matching Java. Nested missing/null serialization is normalized in this input-only comparison; DTO encoding remains separately tested. The 287-case checkpoint route fixture SHA-256 was `294e3cd94cad4df71d55c1ce7c7060096993749b1f5932c7089f778226212a48`.

Static topic endpoint continuation (2026-10-09): expanded the direct Java `topicRouteData2EndpointsForStaticTopic` matrix from three to ten cases. New inputs cover sparse/disjoint/out-of-total IDs, per-ID epoch selection, aliased `__global__`/`global__` scopes, the -1 epoch threshold, nonpositive totals, null scope, and null/empty owner names on inputs that do not dereference a null owner. Rust's existing overlap oracle matches all normalized endpoint rows. The first differential exposed only unspecified Java HashMap tie ordering; the fixture now canonicalizes rows by queue ID and broker name. Separate null-ID-map/null-owner mapped-ID cases retain Java NPE outcomes. Route fixture SHA-256 `a17b806643611eb03c4e11cbcd92ca81700b2384a2547e9370aff102e9eaceb9`; focused protocol tests and pinned fixture generation pass. This closes these combinations only; broader route DTO/input and whole Phase 6 reconciliation remain open.

Endpoint host-input continuation (2026-10-09): pinned Java bytecode confirms `NettyRemotingClient.getHostAndPort` uses the last colon then substring extraction. The direct Java oracle inserts each BMP UTF-16 code unit and every Unicode scalar value into a non-bracketed hostname, then calls the splitter, `Integer.parseInt`, and `InetSocketAddress.createUnresolved`; Java accepts and preserves all 65,536 code units and all 1,112,064 scalar values. Native test `every_unicode_scalar_host_matches_pinned_java_endpoint_split` compares the full scalar domain for raw key retention and exact split host/port; the existing BMP test separately covers all non-surrogate code units and documents that Rust `str` cannot represent isolated surrogates. Address fixture SHA-256 `1ed29bbb157c3f42af594cffc033e4ff1b7b39fe8b94af71367782f385640a64`. Focused tests and pinned fixture generation pass; DNS and interface resolution are separate remaining variants.

SOCKS Fastjson BMP boundary correction (2026-10-09): the pinned Java fixture invokes Fastjson 2.0.63 for all BMP units before both a route and `null`, C0 units `0..=32` at six token boundaries, and every BMP unit at those six boundaries while recording parsed keys and proxy fields. Leading acceptance is limited to NUL, backspace, tab, LF, form feed, CR, space, BOM, and U+FFFE. Within the root object, Fastjson accepts U+0080, U+0088–U+008A, U+008C–U+008D, NBSP, U+00C0, U+00C8–U+00CA, U+00CC–U+00CD, and U+00E0 as trivia; trailing commas are accepted at the two closing-token positions. U+001A ends parsing at the two recorded root boundaries. The first all-BMP internal scan found seven accepted Latin-1 units beyond the whitespace/control candidates; normalization now matches them. `every_bmp_leading_code_unit_matches_invoked_java_fastjson` compares all leading units and 393,216 internal inputs including parsed route values. Pinned fixture generation and the focused native test pass. `client-config.json` SHA-256 is `5675aa477cde1b80fa7c9e4664c93c0b425eba0ff60df7ae7947be354d539221`; the curated constructor matrix remains 492 cases. This closes only the probed BMP positions and EOF behavior; other Fastjson grammar and full source/API/input reconciliation remain open. P6-V04 remains PENDING_PHASE_19.

Address port-domain continuation (2026-10-09): the Maven Java fixture records its oracle runtime (Java 27, Homebrew). Its `Integer.parseInt` accepts 370 BMP decimal-digit code units; the fixture directly records all accepted code units and enumerates the complete legal socket-port range, 0 through 65535. Native tests compare every BMP code unit and every legal port against that oracle. Fixture SHA-256 `bcd1d0075bc9fb54fd94225193e13cba2082cefad02f3fed32af80acb784f85a`. Pinned Maven verify (22 scenarios), remoting all-target tests (71 unit/82 integration), strict remoting Clippy, formatter and diff checks pass. This closes those numeric-input domains under the recorded runtime; other address/transport and full Phase 6 source/API/input reconciliation remain open, and P6-V04 stays PENDING_PHASE_19.

Local gate rerun (2026-10-09): `cargo test --workspace --all-targets --locked` passes with loopback permission (all targets; the restricted attempt denied three scripted socket binds), `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` passes, pinned offline `mvn -o -q -f compat/java-reference/pom.xml verify` exits 0 with 22 scenarios and fixture drift checks, comparator self-test and manifest22 validation pass, and fmt/diff checks pass. P6-V01, P6-V02, P6-V03 and P6-U01 are checked in both Phase 6 plans. The active hook-retention evidence uses `client-config.json` SHA-256 `d4e01085e23e4e73f6192fbe84d64700a05eee28b4fe2523aa48b21a7edfc06e`. These local gates do not complete remaining source/API/input and reverse-export reconciliation; P6-V04 remains reserved for Phase 19.

Route DTO accessor continuation (2026-10-09): the pinned Java fixture now invokes all getter/setter pairs for `BrokerData` (5), `QueueData` (5), `TopicQueueMappingInfo` (7), and `TopicRouteData` (5), collecting populated and null/default outputs. Rust's `route_dto_public_fields_match_invoked_java_getter_setter_values` compares the matching public-field projections, including nullable collection fields and numeric-key maps. Fixture SHA-256 `5b146407b8001de64dd1ad4612d521697e5539a39686e22e43dc4b626de46dcf`. Focused protocol test, pinned offline Maven verify (22 scenarios), fmt and diff pass. This closes only these four DTO accessor rows; broader API/input reconciliation remains open.

Phase plan source correction (2026-10-09): exact 5.5.1 `MQClientManager` source performs `get(clientId)` and only calls `computeIfAbsent` when the ID is absent. The invoked `managerSharing` fixture varies NameServer, namespaceV2, TLS and timeout while returning the first runtime, retaining its original config, and leaving the later config unchanged. A follow-up passes distinct first/later `RPCHook` instances and inspects the actual remoting client's registered list; only the first is present. Native `p6_v02_same_client_id_reuses_first_java_manager_runtime` verifies both runtime and hook identity. Expanded fixture SHA-256 `d4e01085e23e4e73f6192fbe84d64700a05eee28b4fe2523aa48b21a7edfc06e` supersedes the prior config-only hash `54ac6d1c912a19ee61f660a427969fb3e5e9b61656f57a03ffd4654bc30cff63`. The earlier requirement to reject incompatible same-ID configuration was removed from both Phase 6 plans and replaced with first-runtime retention. The `localServeAddr` item was also corrected to client `clientIP`/`NetworkUtil` behavior because the pinned setting belongs to Proxy `ProxyConfig` and is outside the client-only phase. This supersedes the historical note below that the plan remained unmodified; this source/plan correction changed no runtime code. Whole Phase 6 reconciliation remains open.

Named IPv6 scope correction (2026-10-09): the direct Java address fixture adds a loopback-scoped `Inet6Address` normalized as `%lo0`, retaining the separate numeric `%7` oracle. Rust's first differential produced `%1`; `c3101f4` now preserves interface labels from Unix enumeration and maps Windows adapter LUIDs using the OpenJDK name API. Oracle/test commits `3c3a57e` and `0ca2d6b`; `address.json` SHA-256 `89143f5f832705d4d86cdfc24f0dc36a7fe2cf7a803c66e28ed37f45ff9e354a`. Core crate tests, strict Clippy, pinned Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. The Windows-specific production address module and its tests type-check in an isolated `x86_64-pc-windows-gnu` harness; a whole-workspace cross-check stops at the unrelated AWS-LC C build because MinGW GCC is absent. Windows runtime enumeration and general interface ordering remain unverified. Full Phase 6 reconciliation and P6-V04 remain open.

TopicPublishInfo route-presence continuation (2026-10-09): the actual Java route callback oracle now observes `haveTopicRouterInfo=true` with a non-null route and `ok=false`/zero queues, while an absent route sends no callback. Rust's `TopicSnapshot` presence and empty queue state match in `accepted_empty_route_keeps_route_presence_separate_from_publish_queue_ok`. Oracle/test `8529ab7`; route fixture SHA-256 `649c4ce7ce97e0ddc619330082eaa7c7d8f81af0c3040e56d06f9ff14416214d`. Core crate tests, strict core Clippy, pinned Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. This closes the recorded route-presence boundary, not producer queue selection/fallback or overall Phase 6 reconciliation; P6-V04 remains PENDING_PHASE_19.

SOCKS exponent underflow route-key correction (2026-10-09): eleven pinned Java constructor cases distinguish numeric overflow/underflow, signed zero, `D`/`F` suffixes and route-key collisions. Java emits `-0.0` only for a nonzero negative exponent value that rounds to zero; literal `-0e-1000` and `-0.0` yield `0.0`. The red differential exposed sign loss in Rust; `f5b02d4` adds a root-key-specific correction. Oracle/test `bc5c6dd`; 492 cases at fixture SHA-256 `fcc525eb617ab9d5f257eac11ed9ae10fb6e55fd0c7035603e105d40b665007d`. Focused and full remoting tests, strict Clippy, Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. Broader Fastjson and Phase 6 reconciliation remain open.

SOCKS special/identifier root-key continuation (2026-10-09): nine direct pinned Java inputs reject `NaN`, `Infinity`, `-Infinity`, `undefined`, Boolean/null case variants, and `falsehood` as unquoted route keys. The existing Rust literal guard matches all outcomes. Test commit `1843b1c`; 481 constructor cases at fixture SHA-256 `0dcc04b4ce7d41cbd9dc3b38750c52e492f527345078e0ac60ca4c7187129ee5`. Focused differential, pinned Maven verify (22 scenarios), strict workspace Clippy, comparator/manifest22, fmt and diff pass. This adds only these grammar inputs; broader Phase 6 reconciliation remains open.

SOCKS numeric route-key grammar continuation (2026-10-09): 18 direct pinned Java constructor inputs cover signed base-prefix rejection, unsigned leading-dot rejection, and canonicalization for negative decimals, signed leading-dot decimals, exponent signs, and `L`/`D`/`F` suffixes. Java rejects `.5` and `.5F` but accepts `+.5`/`-.5`; the first native differential showed `.5` being accepted. Oracle/test `afbca6d`; implementation `30556c2`; the 472-case fixture SHA-256 is `37110dd0267bd8c6e07568cce2330519cc28e6e1116719aa03dd9325234e06f0`. Focused differential, full remoting crate suite, strict workspace Clippy, Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. This closes only these route-key forms; remaining grammar and full Phase 6 reconciliation remain open.

SOCKS base-prefixed route-key rejection continuation (2026-10-09): direct Java constructor cases show unquoted hexadecimal `0x2a`, binary `0b10`, and octal `0o10` route keys reject. The Rust 454-case differential matches all three `JSONException` outcomes. Test commit `a72996e`; fixture SHA-256 `213e2d8880557161c187fe449e668e36495c2acd17164e6288b1f70840dc3651`. Pinned offline Maven verify, strict all-target/all-feature Clippy, comparator/manifest22, fmt and diff pass; no implementation change was required. Broader Phase 6 reconciliation remains open.

SOCKS cross-type scalar-key continuation (2026-10-09): Java merges numeric `1` with string `"1"` and Boolean `true` with string `"true"` in the string-keyed route map. Reverse order keeps the final route value. A null key and string `"null"` remain separate and preserve their respective route addresses. The native 451-case differential matches. Test commit `9e27434`; fixture SHA-256 `86c8c9369e43579c6f30f486045c9b8c2106076613e6e8627a029af5dfce9452`. Focused differential, strict Clippy, pinned offline Maven verify (22 scenarios and fixture check), comparator/manifest22, fmt and diff pass. Full Phase 6 reconciliation remains open.

SOCKS canonical numeric-key collision continuation (2026-10-09): Java maps unquoted `1` and `01` to key `"1"`; four direct cases with forward/reverse order and interleaved exact duplicates retain the final `addr` value `last`. Native route parsing matches the collapsed key and value. Test commit `a3a1367`; 446 constructor cases at fixture SHA-256 `f73d93400a223d511d4d745eb8140d97fd455d8632b0b35eacd889d010754033`. Focused differential, strict Clippy, pinned offline Maven verify (22 scenarios plus fixture checks), comparator/manifest22, fmt, and diff pass. This extends only the tested collision forms; full Phase 6 reconciliation remains open and P6-V04 stays PENDING_PHASE_19.

SOCKS numeric route-key formatting continuation (2026-10-09): five actual Java cases show leading-zero and `L`/`D`/`F` numeric tokens canonicalize to string-map keys `123`, `123`, `1.0`, `1.25`, and `1.0E7`. Rust already matches these additional forms. Test commit `a373244`; the constructor fixture now has 442 cases at SHA-256 `ec33171ed7bd799715775ddb08e3dbce00c7bd1ff4f349efde3de1a6759b39e1`. Focused differential, pinned offline Maven verify, comparator/manifest22, fmt and diff pass; full Rust workspace and strict Clippy passed with unchanged implementation. Broader Fastjson and Phase 6 reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

SOCKS unquoted scalar route-key correction (2026-10-09): seven direct Java-accepted root keys (numeric, Boolean, and null forms) plus one rejected ordinary identifier extend the constructor fixture to 437 cases. Java canonicalizes `1e2` to `"100.0"` and `+10` to `"10"`, stringifies Boolean keys, and preserves null as a null map key. The native parser matches all outcomes and reflected route keys after `605b87d`/`fc03fa6`. Fixture SHA-256 `7dd846612cfd649ef77ea0b8fd3bf3f75aad5504b4952f6dd0f6d183724ae6e0`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios plus fixture checks), comparator/manifest22, fmt, and diff pass. This closes the recorded key forms only; null-key selection exceptions and broader Phase 6 reconciliation remain open, and P6-V04 stays PENDING_PHASE_19.

SOCKS EOF line-comment boundary (2026-10-09): three direct pinned Java cases confirm that an EOF comment after a complete route object may terminate an unclosed root object; the same EOF inside an unclosed nested route object or array rejects. Native SOCKS parsing already matches all cases. Oracle/test commit `b468c58`; 429 constructor inputs at `client-config.json` SHA-256 `1f25f9939e05b869005252f52fa9bb65510486e8f3caa74d51559c64ec91409b`. The focused differential and pinned Java fixture generation pass with no implementation change. This closes only this comment/EOF boundary; broader Fastjson and full Phase 6 reconciliation remain open, and P6-V04 stays PENDING_PHASE_19.

Numeric map-key normalization correction (2026-10-09): eight pinned Java 5.5.1 DTO inputs cover alias collisions and exact duplicates interleaved with aliases; both numeric maps retain the value from the last property occurrence. `0bc9d7b` fixed sorted raw-key traversal, then the expanded oracle found exact-key deduplication retaining the key's first position; `f137d2e` preserves every raw occurrence through conversion. Oracle/test commits `62d9454` and `5d82500`; the route corpus now contains 133 inputs at SHA-256 `edb7150e939a36d7d5b570da13d08f20d3a385d8e992f7074d3fbf4a92c0e26b`. The focused differential, full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt, and diff checks pass. This closes the tested collision rule only; broader route/API/input reconciliation remains open, and P6-V04 stays PENDING_PHASE_19.

Automatic ClientConfig IP selection correction (2026-10-09): the direct Java `NetworkUtil.getLocalAddress()` and no-argument `ClientConfig` oracle selects `10.10.10.111`, while pre-fix Rust selected `192.168.139.3` because the macOS IPv4 candidate order differed. `5791e28` restores the observed Java candidate order only in the macOS OS adapter, then applies the shared selection logic. Oracle/test `482e02d`; fixture SHA-256 at that checkpoint `138164047171a6e1bb649d3b70239a023245341852cd4036148d80d3c722b5ea`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios and fixture drift), comparator/manifest22, fmt and diff pass. This closes the observed macOS IPv4 constructor case only; at that checkpoint the candidate-list and IPv6 subset comparison had not yet been added. Broader Phase 6 source/API/input reconciliation and P6-V04 Phase 19 work remained open. See [ADR-0045](../architecture/ADR-0045-automatic-client-ip-java-order.md).

SOCKS nested bean Fastjson input continuation (2026-10-09): four pinned Java constructor cases extend the SOCKS corpus to 426. Fastjson keeps the last exact bean property, null clears the credential, Boolean values coerce to strings, and uppercase/camel-case aliases are ignored. The Rust differential matches all outcomes without a production change. Test commit `5b44517`; fixture SHA-256 `478a909c8a7fea8f439e3fafc3ecbdc8a27a8aede6d515dc6f63f59552abb333`. Focused native test and Maven verify (22 scenarios and fixture drift) pass. This closes only these nested bean inputs; the broader SOCKS grammar and full Phase 6 source/API/input reconciliation remain open. P6-V04 stays PENDING_PHASE_19.

ClientConfig TLS property-mode correction (2026-10-09): with only `tls.server.mode=enforcing`, pinned Java reports `TlsSystemConfig.tlsEnable=false`, `ClientConfig.useTLS=false`, standalone `NettyClientConfig.useTLS=true`, then `MQClientInstance`-style copied useTLS=false. The native regression failed before `4084689` changed the ClientConfig default to `tls.enable`/false; oracle and regression commit `cbac242`. Fixture SHA-256 `8ac6b07475ea0af746b70541678355e3f244fffcf6b6bb0bd70d9a7e60a14b46`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios and fixture drift), comparator/manifest22, fmt and diff pass. This closes one constructor input boundary only; TLS provisioning remains P6-V04 Phase 19 work and full Phase 6 source/API/input reconciliation remains partial. See [ADR-0044](../architecture/ADR-0044-clientconfig-tls-property-default.md).

ClientConfig direct system-property input audit (2026-10-09): a fresh-JVM Java fixture and native differential cover defaults and configured values for the eight fields initialized from direct `System.getProperty` calls. Cases include case-insensitive true booleans, whitespace-padded false, and explicit empty SOCKS config. Java/Rust test `java_property_backed_client_fields_match_pinned_constructor_fixture` passes; fixture SHA-256 `16d5a1d2b90bb79b494bf2bca4e97a73117d031ee445147720fad907b1ac5e52`; test commit `3f2ebdf`. Pinned offline Maven verify (22 scenarios and deterministic fixture verification), focused native regression, fmt and diff pass; no implementation change was needed. This closes those eight property-backed fields only. Other constructor/activation and full Phase 6 reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

ClientConfig NameServer constructor-input correction (2026-10-09): Java 5.5.1 uses the `rocketmq.namesrv.addr` system property followed by the `NAMESRV_ADDR` environment fallback. Rust previously used a non-source alias, and the new native test failed when given the pinned environment key. `07f8dee` corrects the fallback; Java oracle/test commit `e630688`; fixture SHA-256 `8db2067f605af9abdabfd212cf02da379f986f6021fb8a456bc50c5da8b7ecdf`. Full workspace, strict Clippy, pinned Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. This closes only this constructor input; other ClientConfig inputs and full Phase 6 reconciliation remain partial. P6-V04 stays PENDING_PHASE_19. See [ADR-0043](../architecture/ADR-0043-clientconfig-nameserver-environment-fallback.md).

Check-client body Fastjson input correction (2026-10-09): eight direct pinned Java inputs expose last-wins duplicate properties across all four `CheckClientRequestBody` fields, nested subscription replacement, scalar-to-string conversion, explicit-null omission, scalar nested-data rejection, and ignored unknown fields. The native differential failed on duplicate `clientId`; `516f9e0` adds the raw-object decoder and all eight cases now pass. Oracle/test `3cba9ba`; client-check fixture SHA-256 `ef84e70eb27c60a9ceab9cfe81f2922295a0ff71bead0159eb606085b7e61277`. Workspace, strict Clippy, pinned Maven verify (22 scenarios), comparator/manifest22, fmt and diff checks pass. This closes one DTO input boundary only; full API/source/input reconciliation remains partial and P6-V04 stays PENDING_PHASE_19. See [ADR-0042](../architecture/ADR-0042-check-client-body-fastjson-input.md).

Heartbeat DTO null/shape oracle extension (2026-10-09): two actual Java inputs bring the direct DTO corpus to 73. Explicit-null `SubscriptionData.codeSet` is omitted on encode; scalar string `ConsumerData.subscriptionDataSet` rejects. Native differential already matched, so this is evidence-only. Test commit `489a13d`; heartbeat fixture SHA-256 `4303f486a9ea17eb0717ed95535e1ccb721c0eda326c4a486d8b64c7312dac9f`. Focused DTO differential, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. This closes only these two inputs; whole Phase 6 audit remains partial and P6-V04 stays PENDING_PHASE_19.

Route DTO List-input correction (2026-10-09): four pinned Java cases prove singleton-object conversion and null-row retention for both `TopicRouteData.queueDatas` and `brokerDatas`. Native derived decoding failed on the singleton object; shared raw-token list helper `399dbc1` now matches all 125 route DTO inputs. Oracle/test `05fa719`; fixture SHA-256 `a733fa2d1a13e8aed4237bd2e848b1908b262dbc9f724c0e52b6b60d56495b77`. Full workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. This closes these collection shapes only; overall Phase 6 source/API/input reconciliation remains partial and P6-V04 stays PENDING_PHASE_19. See [ADR-0041](../architecture/ADR-0041-route-dto-list-fastjson-input.md).

Lite subscription collection-input correction (2026-10-09): ten pinned Java 5.5.1 cases prove object-to-singleton-Set conversion and null-member retention for `subscriptionSet`; missing/null/empty forms and rejected scalar shapes are recorded too. Rust uses `Option<Vec<Option<LiteSubscriptionDto>>>` as its wire representation, preserving distinct DTO objects and collapsing repeated nulls. The native test failed on a singleton object before fix `5462e2f`; oracle/test commit `27a55e3`. The combined Lite fixture has 33 cases at SHA-256 `1074c30d2d829dc4dd46b68f1fbdd1f66f7b4a87a65c40f9174aff806658f619`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. This closes the invoked Lite list-shape inputs only; whole Phase 6 reconciliation remains partial and P6-V04 stays PENDING_PHASE_19. See [ADR-0040](../architecture/ADR-0040-lite-subscription-list-fastjson-input.md).

Runtime parser evidence continuation (2026-10-09): the Java address fixture now covers 49 endpoint splits and exposes last-colon parsing plus Java's permissive host/bracket handling; the native fix is `be43200`, following oracle/test commit `8c285a0`. Address fixture SHA-256 `2c95c3a72f496e24306bcdd5b347436b322cdf5d5796f5c416f11cc48f8e77f6`. The SOCKS Fastjson constructor fixture has 422 cases after ten additional grammar inputs; native parsing matched without production changes. Oracle/test commit `a64e048`; config fixture SHA-256 `0b7cbe9560fae03c4efabb9347ebbfc424ea27b70c873cdb2145c1c914cdaf75`. Focused differential tests, full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff checks pass. These close only recorded parser inputs; whole Phase 6 method/field/branch/source/input reconciliation remains partial and P6-V04 stays PENDING_PHASE_19.

LiteSubscription DTO duplicate-property correction (2026-10-09): ten pinned Java inputs prove last-wins for every decoded field of the Lite subscription control body, nested DTO, and offset option, including enum type and primitive value. Native derive rejected duplicate `action`; implementation `5edf8e5` applies shared raw-object decoding. Lookup fixture now contains 23 Lite input cases at SHA-256 `fa96140798a6a9342f19d3acd5aa703604c1ad512793ef029954fdd33f7afabe`; oracle/test `5e185cc`. Focused red/green, full workspace, strict all-feature Clippy, pinned Maven verify (22 scenarios), comparator/manifest, fmt and diff pass. This closes only the tested Lite DTO duplicate fields; full Phase 6 source/API/input reconciliation remains partial and P6-V04 stays PENDING_PHASE_19.

Heartbeat DTO duplicate-property correction (2026-10-09): pinned Java accepts repeated fields on `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData`, retaining the final value. Nineteen serializable fields are directly compared; a separate getter probe verifies `filterClassSource`. Native decoding initially failed on duplicate `classFilterMode`; `9d8b031` adds shared raw-object last-wins decoding. Heartbeat fixture has 71 DTO inputs at SHA-256 `5597c2bb980302e0c4ce50a6fae1524b037b66b9dbb138f6af31721085a9929c`; oracle/test `43bc1d6`. Focused red/green, full workspace, strict all-feature Clippy, pinned Maven verify (22 scenarios), comparator/manifest, fmt and diff pass. This closes these duplicate inputs only; full Phase 6 source/API/input reconciliation remains partial and P6-V04 stays PENDING_PHASE_19.

BrokerData duplicate-property correction (2026-10-09): five pinned Java 5.5.1 inputs prove last-wins decoding for every `BrokerData` field; a duplicate `brokerAddrs` object replaces the full prior map. The native differential failed on duplicate `cluster`; implementation `6c66258` now applies raw-object last-wins decoding. Route fixture has 109 DTO inputs at SHA-256 `c2b85d5e374dd27f93653485824394c019f2ea2ce06e712acdde20fb18c24c01`; oracle/test `131bf44`. Focused red/green route test, full workspace, strict Clippy, pinned Maven verify (22 scenarios), comparator/manifest, fmt and diff pass. This closes only these BrokerData duplicate fields; full Phase 6 source/API/input reconciliation remains partial and P6-V04 stays PENDING_PHASE_19.

TopicQueueMappingInfo and TopicRouteData duplicate-property correction (2026-10-09): seven mapping-field and five route-field Java inputs prove Fastjson keeps the final field value, with duplicate maps/lists replaced wholesale. Rust's derived decoders failed on duplicate `topic`; `76e6ca8` adds last-wins raw-object decoding to both DTOs. Route fixture now has 121 DTO inputs at SHA-256 `621d817bee937e0ddddca352747f79a783a1ba672e8e782a3fad7866b9602949`; oracle/test `5ff1d9b`. Focused red/green route differential, full workspace, strict all-feature Clippy, pinned Maven verify (22 scenarios), comparator/manifest, fmt and diff pass. This closes only these route DTO duplicate fields; full Phase 6 source/API/input reconciliation remains partial and P6-V04 stays PENDING_PHASE_19.

QueueData duplicate-property correction (2026-10-09): five pinned Java 5.5.1 `QueueData` inputs confirm the final duplicate value is coerced using Java Fastjson rules for each field. The native differential failed on duplicate `brokerName`; `1d146f6` applies last-wins raw-object decoding across the DTO. Route fixture has 104 DTO inputs, SHA-256 `967c44455135c142ebeb311aad00d612d868cd785f936d1362c79cf989e36699`. Oracle/test `3fdf225`. Red/green route test, all-target workspace, strict Clippy, pinned Maven verify, comparator self-test, fmt and diff pass. This closes only the QueueData duplicate-field cases; full Phase 6 source/API/input reconciliation remains partial and P6-V04 stays PENDING_PHASE_19.

MessageQueue duplicate-property correction (2026-10-09): pinned Java 5.5.1 accepts duplicate `topic`, `brokerName`, and `queueId` fields and retains the last raw value before applying field coercion. Three direct `MessageQueue` inputs plus one nested assignment-body input exposed Serde's duplicate-field rejection; implementation `29928ba` uses the shared raw-object decoder. At that update the lookup fixture had 19 MessageQueue and 15 assignment-body cases, SHA-256 `46a03b750d8cc28b1ef93fc126673262a15946055f8eba9f929838f58a3f3f29`; the later array-element continuation and current hash are recorded below. Oracle/test commit `8512e45`. Focused red/green differentials, full all-target workspace, strict Clippy, pinned Maven verify, comparator self-test, fmt and diff pass. This closes these DTO duplicate fields only; broad Phase 6 source/API/input reconciliation remains partial and P6-V04 stays PENDING_PHASE_19.

Assignment response duplicate-property correction (2026-10-09): two direct Java 5.5.1 inputs prove that Fastjson retains the last duplicate root `messageQueueAssignments` value and the last nested `MessageQueueAssignment.mode` value. Native derived decoding rejected the duplicates; implementation `c2051a8` now uses the shared raw-object decoder at both DTO boundaries. The assignment body corpus has 14 cases at fixture SHA-256 `c8d813d210219b68c3c48a121a1fa0eb903ebd31c10ea241bfe1b4ba6aafbb95`. Oracle/test commits `d23cc55` and `0d3fa37`. Focused and full protocol DTO differentials, workspace, strict Clippy, pinned offline Maven verify, comparator self-test, fmt and diff pass. This closes only these duplicate fields; whole Phase 6 source/API/input reconciliation remains partial and P6-V04 stays PENDING_PHASE_19.

Plan cross-check (2026-10-09): reread the master plan, the English and Traditional Chinese Phase 6 plans, and both execution-index versions. The child plan's P6-V02 text still says to reject incompatible same-ID configurations; direct pinned `MQClientManager` evidence and correction `b18771d` show the source returns the first mapped runtime via `get`/`computeIfAbsent`, so later config is ignored. The plan remains read-only. P6-V01/V02/V03/U01 have executable local gates in the audit; P6-V04 stays PENDING_PHASE_19. Remaining method/field/branch/reverse-export and source/input reconciliation is still PARTIAL, so this is not a phase acceptance handoff.

Assignment request duplicate-property correction (2026-10-09): four direct Java 5.5.1 cases establish Fastjson last-wins behavior for `messageModel` and `mode` when enum names and ordinals are duplicated in either order. Native derived decoding failed; the shared raw-object helper in `07c952e` preserves the last raw field value before coercion. The updated lookup fixture has 10 QueryAssignmentRequestBody and 14 SetMessageRequestModeRequestBody cases at SHA-256 `a6b63fd046c9b022bb41749af52dbce051a3662158dbfc33d980c4ec0b376cb8`. Oracle/test `5388a09`; implementation `07c952e`. Full workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. This closes only these duplicate request fields; whole Phase 6 source/API/input reconciliation remains partial and P6-V04 stays pending.

Consumer-list duplicate-property correction (2026-10-09): three direct Java 5.5.1 inputs expose Fastjson last-wins behavior for duplicate `consumerIdList` fields. The native Serde decoder failed with a duplicate-field error; the differential identified that mismatch, and `ea6d6de` now retains the last raw value before coercion. All 16 consumer-list DTO inputs match. Oracle/test commit `e03a999`; lookup fixture SHA-256 `943fc1e79fa2709fa4199ff8b173f4cd5625b67942c7fb1549078180682c27e1`. Full workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. This closes only this DTO input gap; full Phase 6 source/API/requirement reconciliation remains partial and P6-V04 stays pending.

Endpoint port input continuation (2026-10-09): fourteen direct Java 5.5.1 `getHostAndPort` inputs extend the address fixture to 38 endpoint cases. Java accepts plus signs, `-0`, long leading-zero forms and BMP decimal digits after an ASCII sign; whitespace and Unicode signs reject. I32 extrema parse as integers but fail socket-port range validation. Rust matches the Java results without implementation changes. Fixture SHA-256 `07ac76295a0df3827be9fce5a4b703749bb0822338f5fbca5a8de6bb8adb4c15`. Focused differential, full workspace, strict Clippy, offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff checks pass. This closes only the invoked port forms; full Phase 6 source/API/requirement reconciliation stays partial and P6-V04 remains pending.

SOCKS root-shape input audit (2026-10-09): ten new pinned Java 5.5.1 constructor probes extend the Fastjson corpus to 412 cases. Java rejects nine non-object root/route-value shapes and accepts duplicate route keys with last-value retention; the Rust differential matches all ten without a production change. Fixture SHA-256 `279d650d5d7fc57c3e3250b8bec36e199fcd48f8b01f5a0ebb1b52b8f0caeaca`. Focused differential, full workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff checks pass. This closes only these input shapes; full Phase 6 source/API/requirement reconciliation stays open and P6-V04 remains `PENDING_PHASE_19`.

Route numeric-map-key input audit (2026-10-09): twelve new direct Java 5.5.1 DTO cases cover string keys for `BrokerData.brokerAddrs` (`Long`) and `TopicQueueMappingInfo.currIdMap` (`Integer`). Java and Rust accept plus/zero-padded, fractional, and exponent forms, including maximum signed keys; both reject whitespace and overflow. No production change was needed. Oracle/test commit `7db339a`; the route DTO corpus now has 99 cases and fixture SHA-256 `06b96e4e50223a3b1d8633317ff309ecf759ef7e74e238a76670bcb5ad1d2e31`. Full workspace tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, fmt and diff checks pass. Other DTO/input and whole Phase 6 reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

Raw endpoint-key parity (2026-10-09): pinned Java `NettyRemotingClient` passes the raw address string to `channelTables.get(addr)`. The address oracle now has 24 endpoint splits and four actual channel-table key pairs; different host casing, IPv6 hex casing, and leading-zero port spellings remain distinct keys. Rust previously normalized these forms and reused one connection. Oracle/test commit `f0f1d41` reproduced the collision; fix `67a57ad` preserves the validated raw address as the connection key and parses the dial target separately, retaining the empty-host→127.0.0.1 rule. Fixture SHA-256 `b7112911b84f023d1b32408a01b4571c0086570a96857e2e24b12526b40de9f4`. Full workspace tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, fmt and diff checks pass; a socket test confirms alternate spellings open two channels. No Broker was contacted. Other source/API/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

Earlier empty-host endpoint correction (2026-10-09): the pinned Java 5.5.1 NettyRemotingClient endpoint oracle then had 22 cases. Java splits :10911 to an empty host and port 10911; its JDK resolves the empty host to 127.0.0.1. The native differential failed first because Address::parse rejected the host, then showed localhost selected ::1 before 127.0.0.1 on this machine. At that fix checkpoint Rust canonicalized the empty host to the JDK loopback target; the raw-key continuation above now preserves the original cache key and applies the loopback mapping only to the dial target. Red oracle/test commits 5de98b9 and d1fd8d0; fix 018e9d3; loopback transport regression 159f43c. The then-current address fixture SHA-256 was 3f66b46e922d4c278510c8e2ea29ae26423cf30dec6cc2e5e304087260c37c71. Focused differential and loopback transport tests passed; at that checkpoint the remoting crate suite had 67 library and 81 integration tests. No Broker was contacted; full Phase 6 reconciliation remains open and P6-V04 stays PENDING_PHASE_19.

Address classification boundary update (2026-10-09): 16 added pinned Java addresses extend the NetworkUtil/UtilAll differential to 42 numeric inputs. The new values straddle IPv4 10/8, 127/8, 172.16/12 and 192.168/16 boundaries and cover nearby external/special ranges plus IPv6 link/site-local cutovers, ULA and mapped IPv6. Rust matches Java classification, loopback and normalization for all 42; no implementation change was needed. Oracle/test commit 560da5d; address fixture SHA-256 at that update 7a385ac782f2d27c5bb9f6fc9a7dee284833115c33acb28b692c6aea44dcb89b. Focused address helper test, offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. Remaining endpoint/transport variants and final source/API reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Manager first-runtime correction (2026-10-09): pinned Java 5.5.1 `MQClientManager.getOrCreateMQClientInstance` returns the mapped instance for an existing client ID and only clones config/invokes `computeIfAbsent` when absent. A direct unstarted Java fixture now proves same-ID calls with changed NameServer, namespaceV2, TLS and timeout return the same instance, retain the first config, and leave the later caller config unchanged. The native test reproduced `ConfigurationConflict` before fix `b18771d`; oracle/test commit `204de99`, fixture SHA-256 `54ac6d1c912a19ee61f660a427969fb3e5e9b61656f57a03ffd4654bc30cff63`. The conflict guard/error was removed; construction-time environment properties and hook are also captured only for the first mapping. Focused tests, full workspace, strict Clippy, Maven verify (22 scenarios), comparator/manifest22, fmt and diff checks pass. This supersedes ADR-0004's former conflict policy; broad Phase 6 reconciliation remains partial and P6-V04 stays PENDING_PHASE_19.

Inbound property-parser correction (2026-10-09): nine pinned Java 5.5.1 `MessageDecoder.string2messageProperties` cases expose empty-valued records being ignored, including an empty duplicate leaving its prior value intact. The native routing metadata parser previously retained and overwrote with empty values; oracle/test commit `0041553` failed on `PGROUP\u0001\u0002` before fix `df63dae`. Fixture SHA-256 `534f1d4be1d82abd4c9d18c11265dae4445344cb434c7483f55874f3f7d25669`. Focused common tests, full workspace regression, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, fmt, and diff checks pass. This closes the invoked property-record cases only; full Phase 6 reconciliation remains open and P6-V04 stays PENDING_PHASE_19.

Endpoint Unicode-port continuation (2026-10-09): the pinned `NettyRemotingClient.getHostAndPort` oracle now has 22 cases and records Java `Integer.parseInt` plus socket-address port range behavior. It accepts BMP Arabic-Indic, fullwidth, mixed, and Devanagari decimal digits and rejects negative, >65535, i32-overflow, and supplementary-digit ports. The native address parser differential failed on Arabic-Indic digits before fix `51cef1d`; probe/test commit `c8d0171` and implementation `51cef1d` now pass. Address fixture SHA-256 `3f66b46e922d4c278510c8e2ea29ae26423cf30dec6cc2e5e304087260c37c71`. Full workspace, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, fmt and diff checks pass. This closes the invoked address split/port cases only; other address/transport variants and full Phase 6 reconciliation remain open, and P6-V04 stays PENDING_PHASE_19.

Exact numeric coercion and route property-name continuation (2026-10-09): the pinned Java oracle covers 99 route DTO inputs, 20 assignment-mode cases, 13 consumer-list cases, seven assignment-attachment cases, and 52 heartbeat cases. Seven route inputs across four DTO types confirm uppercase, title-case, snake-case, and case-collision spellings are ignored by Fastjson and Rust. High-precision strings preserve exact decimal digits and Java exponent formatting across route maps/lists and lookup DTOs. Assignment mode now uses raw numeric tokens so exact decimal truncation and low-32-bit wrapping match Java; the `0.9999999999999999999` f64 mismatch is fixed, decimal `4294967296.0` maps to PULL, and plain integer `4294967296` rejects. A high-precision decimal boolean input also confirms Java and Rust both return false for `1.0000000000000000001`; no boolean implementation change was needed. Probe/test commits `1695b9f`, `8f7ad7f`, `a18ce02`, `beb6fe7`, and `21d5b70`; implementations `428dde9` and `dcc822b` build on the earlier numeric conversion fix `5e30b73`; property-name oracle/test commits `7985ef7` and `675b73b`. Current fixture SHA-256: route `06b96e4e50223a3b1d8633317ff309ecf759ef7e74e238a76670bcb5ad1d2e31`, lookup `42a05d175175b94dc7e1bfc051940f70b16a01cbba0345ff2a35a5a0fa2d57e1`, heartbeat `e183f318134bd5dfa85bcbd80276c3e85c444f3ca0ebbe8231696ef2b4305db7`. Full workspace tests, strict Clippy, pinned Maven verify, comparator self-test, fmt and diff checks pass. Phase 6 remains open; P6-V04 stays PENDING_PHASE_19.

SOCKS escape grammar continuation (2026-10-08): the pinned Java constructor and reflected `proxyMap` oracle now covers 397 cases, including all UTF-16 escape followers U+0000–U+00FF and 15 additional Unicode/control probes. Fastjson `char1` masks the first UTF-16 code unit to seven bits before table lookup. `\u` consumes four UTF-16 units even when a supplementary scalar crosses its final digit; native parsing preserves a valid decoded high/leftover low pair and maps an unpaired unit to U+FFFD. Oracle/test commits `5f6e48c`/`823cc02`/`7cb2552`/`4ae4dc9`, implementation commits `e59475f`/`77f6bd8`; fixture SHA-256 `782e9c5ef3c8523d7de1ee0d61c9ad0cf16fc8036e65e6c296a26a1e6df824ab`. The focused differential, full locked workspace suite, strict all-feature Clippy, pinned offline Maven verify, comparator self-test/manifest22, fmt and diff checks pass. Other Fastjson grammar and broad API/source/branch reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Route DTO integer-string input checkpoint (2026-10-09): seven actual Java 5.5.1 `MessageQueue.queueId` inputs show `+2`, `002`, `-1.9`, and `1e2` strings convert to 2, 2, -1, and 100; whitespace-padded and out-of-range strings reject. Native Fastjson integer coercion matches all seven, so no production change was needed. Probe/regression commit `6493e76`; the 66-input checkpoint route fixture SHA-256 was `3e96c4cbb68dcc8f83f5390106c4ce173afac0fc69826c81a6aa003a192d786a`.

Nullable route response update (2026-10-08): pinned Java utility invocations establish null roots for null decode/fromJson input and JSON `null`, plus null decodeList results and preserved `[null]` members. Follow-up calls directly exercise both `TopicRouteData` and `TopicQueueMappingInfo` (`c00e1a8`); native adapter assertions are in `8420373`. The API reports a successful JSON-null route as `Ok(None)`; runtime refresh returns false and preserves its cached route snapshot. `all_topic_refresh_unions_registered_topics_and_continues_after_rejection` also verifies the null-topic union member's JSON-null no-op and continuation to later topics (`95cda6c`). Missing response bodies remain malformed responses. Oracle `5eaccba`/`f007a4a`/`c00e1a8`, tests `ffc5c8c`/`722b5bd`/`8420373`, implementation `bdfa5cb`; route fixture SHA-256 `55ff6cf477d53e78f5715294613e3a98d21796b729118a04feb999b213c17245`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (manifest22), comparator self-test, fmt and diff checks pass. [ADR-0039](../architecture/ADR-0039-nullable-route-decode.md) records the boundary. Full Phase 6 reconciliation stays open and P6-V04 remains PENDING_PHASE_19.

Earlier ASCII SOCKS escape update (superseded by the Unicode continuation above): the Java 5.5.1 constructor differential at that checkpoint covered 254 inputs, including a 128-entry matrix with one input per ASCII escape follower. Java accepts 12 additional identity escapes, octal `\1`–`\7`, and uppercase `\F` as form feed. The native differential first failed on backslash-space and reported 20 mismatches; implementation `8ac0ea3` normalizes those accepted values and now matches constructor and reflected proxy outcomes. Complete `\x`/`\u` escapes map non-hex ASCII digits to zero, while truncated escapes and backslash line continuations reject. Oracle commit `72396f7`; regression commit `17fd56e`; fixture SHA-256 `34e3b6600611e97fe5ac5c3009bcd5d95e2c9489456ac9cc91e9b5f5a9ab2f25`. Full workspace tests, strict Clippy, offline Maven verify (manifest22), comparator self-test, formatting, and diff checks pass. Remaining Fastjson grammar and whole Phase 6 reconciliation stay open; P6-V04 remains PENDING_PHASE_19. See [ADR-0038](../architecture/ADR-0038-fastjson-socks-escape-grammar.md).

Route DTO inherited serialization update (2026-10-08): direct pinned Java invocations confirm both route DTOs inherit Fastjson2-backed `RemotingSerializable` methods. Rust adapters reproduce compact/pretty JSON and exact encoded bytes, including the empty `JSONWriter.Feature[]` overload, decode Java's unquoted numeric map keys, and cover decode/list/fromJson. The core route API now uses the nullable-compatible decoder; a loopback scripted response carries actual Java route bytes. Six cases cover default/null/populated `TopicRouteData` and `TopicQueueMappingInfo`. The initial serializer fixture hash `935426217262daeb92860ef9b90cc61b5cde8aa71d3042ca6f4713582568e2b3` is extended by the null utility oracle to the then-current hash `55ff6cf477d53e78f5715294613e3a98d21796b729118a04feb999b213c17245`; oracle `c8bb22a`, implementation `4a271dd`. Workspace tests, strict Clippy, offline Maven verify (manifest22), and comparator self-test pass. Native map ordering is deterministic; Java map iteration is unspecified. Phase 6 reconciliation remains open and P6-V04 remains PENDING_PHASE_19.

Heartbeat inherited serialization correction (2026-10-08): actual pinned Java 5.5.1 `RemotingSerializable` calls cover compact `encode()`/`toJson()`, pretty output, static encode/toJson, decode, list decode, and fromJson. Native `HeartbeatData::java_encode/java_to_json` matches exact compact UTF-8 bytes in default, explicit-null and nested-populated cases; value-first Serde output matches Fastjson2's sorted keys. New `java_decode*`/`java_from_json*` adapters preserve Java's null-byte/string, JSON-null, null-list-input, and null-list-member behavior. The pinned client calls no-argument `HeartbeatData.encode()` for V1/V2 heartbeat bodies; core uses the tested adapter. Heartbeat fixture SHA-256 at that checkpoint: `bc147764feb4ddfca190c098eb014f585148e7b8989c804922acae5611384612`. Full Phase 6 source/API/field/branch reconciliation remains open and P6-V04 stays PENDING_PHASE_19.

Typed request serialization errors (2026-10-08): the outbound heartbeat and extension JSON body helpers now propagate Serde failures as `RpcError::RequestSerialization { request_code, source }`; they return before invoking remoting. The failure-path unit test confirms request identity and original error retention. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (manifest22), comparator self-test, fmt and diff checks pass. This closes the panic/error disposition for these JSON request paths only; broader API/source reconciliation remains open, and P6-V04 remains PENDING_PHASE_19.

Heartbeat DTO identity-method correction (2026-10-08): Java reflection shows `ProducerData`, `ConsumerData`, and `HeartbeatData` inherit `equals`/`hashCode` from `Object`. Actual invocations confirm same-instance equality, unequal equal-field instances, and two retained Java `HashSet` members. Rust removed structural `PartialEq/Eq/Hash` from these DTOs, guarded by compile-fail doctests; `SubscriptionData` retains value equality. Fixture SHA-256 at that update: `f32c42d5f1f71f8da573237504e8312cb8495209bf59cb7d54c0960533c0e308`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (manifest22), differential self-test, fmt and diff checks pass. This closes only these inherited DTO methods; Phase 6 source/API/field/branch reconciliation remains open and P6-V04 stays PENDING_PHASE_19.

VIP helper correction (2026-10-08): direct Java 5.5.1 cases show `MixAll.brokerVIPChannel` splits at the last colon, parses a signed Java int, subtracts 2 with Java overflow, and returns the host portion unchanged without validating it. Rust `broker_vip_channel` now uses the shared Java decimal parser and wrapping subtraction; the transport layer handles socket validity. Fifteen new Java edge cases and three existing hostname/IPv6 cases match. Rust maps Java split failures and port parse failures to distinct typed errors. Fixture SHA-256: `ce2afec05859a761ce2fd82a9034ef147b333f077a999b03d17209a305254fb0`; full Phase 6 source/API/branch reconciliation remains open.

Route DTO input continuation (2026-10-08): 12 additional actual Java 5.5.1 inputs show route DTO `JSON.parseObject` rejects single quotes, comments, unquoted keys, trailing commas, `L`/`D`/`F` suffixes, scalar filter-list values, and array-shaped maps/rows. Unicode probes `b7ea5a8`/`45e24d4` add five direct cases for fullwidth/Arabic-Indic strings in integer fields and numeric map keys; Java rejects them with `JSONException` and native decoding rejects them too (`b1f36d5`, `0fbd164`). Numeric probes `c67038f`/`dbaa192` then exposed the leading-plus/zero-prefix mismatch: Fastjson accepts these forms for integers, decimals, and exponents, but rejects NaN/Infinity/hex tokens. The differential failed before implementation `ff494d8`, then passes with normalization in the Java-compatible route parser; tests `54d6d4a`/`ac959a5` retain all required IDs. Whitespace probes (`ea81038`, `ccd39c4`) add eight cases: Fastjson accepts form feed around tokens/punctuation and one BOM at the document start, while rejecting vertical tab, NBSP, and an embedded BOM; regression IDs are required by `7436e73`/`5c36612`, and parser normalization is in `79c9aa7`. The route DTO oracle now covers 59 inputs at SHA-256 `55ff6cf477d53e78f5715294613e3a98d21796b729118a04feb999b213c17245`; pinned Maven verify, full workspace/all-target tests, strict Clippy, comparator, fmt and diff checks pass. See [ADR-0014](../architecture/ADR-0014-route-dto-fastjson-input.md). Broader DTO input, Fastjson grammar, source/API/branch reconciliation, and P6-V04 remain open.

Runtime `ClientConfig` mutability update (2026-10-08): the pinned manager clones caller input, `MQClientInstance` retains that config object, and `getClientConfig()` returns the same object. Rust `ClientRuntime::config()` now returns a shared mutable handle, and its runtime-created API/namespace hook read the audited Java live settings from that handle. Transport construction, concurrent-heartbeat executor presence/width, and start-time schedules preserve their source-time capture boundaries, while the heartbeat sender still reads `enableConcurrentHeartbeat` live; the false→true mutation with an absent executor returns false, and the true→false mutation selects serial V1 while retaining its executor, as recorded in the 2026-10-10 fixture updates above. The Java getter-identity/mutation oracle and native live language/namespace/timeout/VIP/heartbeat tests pass. Current `client-config.json` SHA-256 `ad49c9b917811d1b542bf7f8b2af9ffa07ba3cde22105a00a69bd02f2ade093a`; [ADR-0037](../architecture/ADR-0037-live-runtime-client-config.md) records the decision. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify, comparator/manifest checks, formatter, and diff checks pass. This closes the public runtime config mutability gap for audited fields, not the full Phase 6 source/API/branch reconciliation; P6-V04 remains PENDING_PHASE_19.

MessageQueueRouteState public enum API update (2026-10-08): Rust now exports `remoting::MessageQueueRouteState` with Java's `Expired`, `ReadOnly`, `Normal`, and `WriteOnly` declaration order, enum methods, and typed lookup errors. Direct Java cases invoke all four values and null/unknown/empty/case-mismatched lookups. The pinned artifact scan found no call sites outside the enum, so no route-state transition behavior is claimed. Current `client-config.json` SHA-256 `be97b56070efbdc49ed2294b06f36f6848a152ddb310fba30d349a32a95b52f3`; see [ADR-0035](../architecture/ADR-0035-message-queue-route-state-enum-api.md). Full Phase 6 reconciliation remains open.

RequestSource public enum API update (2026-10-08): Rust now exports `remoting::RequestSource` with Java declaration order, enum methods and lookup errors, `get_value`, `is_valid`, `parse_integer`, and the exact `SYSTEM_PROPERTY_KEY` constant. Direct pinned Java cases cover 23 enum/integer inputs and the key. The artifact reference scan found no call sites outside the enum, so this maps its public surface without claiming that the constant activates property loading. Current `client-config.json` SHA-256 `1544344bd90fbefe577643e3ec7af1a8211dcc7bc780c2f39fef7e5ca9fcd391`; see [ADR-0034](../architecture/ADR-0034-request-source-java-enum-api.md). Full Phase 6 reconciliation remains open.

Assignment response Set edge update (2026-10-08): the 12-case Java 5.5.1 decoder matrix now includes same-queue/different-mode assignments and retains both ordinary rows. A separate invoked Java oracle computes an attachment string whose hash cancels the process-local `MessageRequestMode` identity-hash difference; the two distinct-metadata assignments then compare equal, have equal hashes, and Java `HashSet` retains one. The native all-fields Eq/Hash set does not claim this JVM-specific hash collision behavior. `assignment_body_inputs_match_invoked_java_fastjson` and `assignment_set_hash_collision_oracle_records_the_java_boundary` pass. Current lookup fixture SHA-256 `428599d773432f8a93edf86723e3594ade337a383fd16fbb6a5f4e99315bc8c3`; [ADR-0019](../architecture/ADR-0019-assignment-response-set-input.md) records evidence.

Netty event enum API mapping (2026-10-08): direct pinned Java cases invoke all `NettyEventType` constants, generated/inherited enum methods, defensive `values()` copies, and null/unknown/case-mismatched lookups. Rust exposes `client::NettyEventType` in Java declaration order and uses it for channel event queue entries; the native differential checks 12 cases. Fixture SHA-256 at that update `1fe6cf206b07e5f304634d7128ca4ca4c56a0114f010f21a0eeebb785529e3b5`; [ADR-0033](../architecture/ADR-0033-netty-event-type-java-enum-api.md) records the mapping and the `NettyEvent.getChannel()` carrier-object limitation. Full Phase 6 reconciliation remains open.


Remoting command API mapping (2026-10-08): the pinned Java fixture invokes `RemotingCommandType` and `RequestType` generated/inherited enum methods, `RequestType.valueOf(byte)` for signed input boundaries, and `RemotingCommand.getType()` before and after response marking. Rust exposes both enum surfaces, nullable byte lookup, and `get_type()` derived from the response flag; the stream hook uses `RequestType::Stream.get_code()`. The native differential checks 22 Java cases. Fixture SHA-256 at that update `6d02747d6a267631d2bf98d064967bead5a4a168411a28cb15f34bfa46d0c764`; [ADR-0032](../architecture/ADR-0032-remoting-command-enum-java-api.md) records the mapping. Full Phase 6 reconciliation remains open.

TLS enum API mapping (2026-10-08): direct pinned Java cases invoke all `TlsMode` constants, generated/inherited enum methods, `getName()`, and `parse(String)` across null, exact, case-mismatched, whitespace, empty, and unknown inputs. Rust exposes the matching enum surface and uses its exact parser when `tls.enable` is absent; 20 Java cases pass the native differential. Fixture SHA-256 at that update `948fcc7ca474d0ea48b39b1f60e075fa2213ff40290d51ec801c14af0e8fbad9`; [ADR-0031](../architecture/ADR-0031-tls-mode-java-enum-api.md) records the mapping. TLS provisioning remains Phase 19 work; full Phase 6 reconciliation is still open.

Protocol enum API mapping (2026-10-08): direct pinned Java cases invoke generated and inherited methods for `MessageRequestMode`, `LiteSubscriptionAction`, `OffsetOption.Type`, `CMResult`, `ConsumeType`, `MessageModel`, and `ConsumeFromWhere`. Rust adds the matching values/order/name/text/comparison and typed name lookup surfaces; `MessageModel` ordering follows Java ordinals. All 65 native differential cases pass alongside existing custom `getName`/`getTypeCN`/`getModeCN` checks. Full workspace/all-target tests, strict Clippy, pinned offline Maven verification, formatter and diff checks pass. Fixture SHA-256 `6de87735e30feb4fdca7c041d207326dd4718f0fe76b749c9c96b5e59214c747`; [ADR-0030](../architecture/ADR-0030-protocol-enum-java-api.md) records scope. Full Phase 6 reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

Client status enum API mapping (2026-10-08): direct Java 5.5.1 cases cover `PullStatus`, `PopStatus`, `AckStatus`, and `SendStatus` values/order, defensive copies, inherited enum methods, and exact null/invalid string lookup errors. Rust exposes corresponding methods and typed errors; `PopStatus` order now matches Java (`POLLING_FULL` before `POLLING_NOT_FOUND`). The native differential checks all 40 cases. Full workspace/all-target tests, strict Clippy, pinned offline Maven verification, formatter and diff checks pass. Fixture SHA-256 `de88ee1ad2ce408aee43b6614feecc3c9cf1c7f25426b89ddb13feff03a09f0e`; [ADR-0029](../architecture/ADR-0029-client-status-java-enum-api.md) records the mapping. Full Phase 6 reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

CompressionType enum surface correction (2026-10-08): pinned Java has exactly `LZ4`, `ZSTD`, and `ZLIB`; the Rust-only `Legacy` and `Unknown(i32)` variants are removed. The Java oracle invokes generated/inherited enum methods, `getValue`, `getCompressionFlag`, `of(String)`, `findByValue(int)`, and `MessageSysFlag.getCompressionType`. Native flag lookup now maps 0 and 3 to `ZLIB` and returns a typed error for unknown masked values 4–7. The common-crate differential covers all cases, including Java's trim boundary and stable lookup messages. Focused common tests, full workspace/all-target tests, strict Clippy, pinned offline Maven verification, formatter and diff checks pass. Fixture SHA-256 `ab635ab3ac993723c769fa19264fe63e593990da3655b9d1f26e0cad48dac3bf`; [ADR-0028](../architecture/ADR-0028-compression-type-java-enum-api.md) records the mapping. Full Phase 6 reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

SerializeType enum surface correction (2026-10-08): pinned Java's `SerializeType` exposes exactly `JSON` and `ROCKETMQ`; Rust's public `Unknown(u8)` and raw constructors are removed. The Java fixture directly invokes `values`, string and byte lookups, `getCode`, `name`, `ordinal`, `toString`, and `compareTo`, with exact null/invalid lookup messages. The Rust frame parser returns `UnsupportedSerialization(code)` for an unknown marker rather than constructing a public unknown enum value. Protocol tests, remoting codec tests, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verification, formatter, and diff checks pass. Fixture SHA-256 `6008a989876a0541a2d1ed8a036ca0dc37663a3ef0551400b76e5e5a480e95ae`. Phase 6 source/API/input reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

LanguageCode generated enum method mapping (2026-10-08): pinned Java calls now record `values()`, string `valueOf`, `name`, `ordinal`, `toString`, and `compareTo`, in addition to Java's custom code lookups and unknown-value behavior. Rust tests compare all 32 fixture cases, including value order/array copy, exact lookup errors, signed bytes, and null wire decode. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verification, formatter and diff checks pass. Fixture SHA-256 `00deca75109f3fa4b7a97314bdb9b567453e3ed14fe298201f7caca62411ef4d`. Broader Phase 6 source/API/input reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

LanguageCode API surface correction (2026-10-08): the public enum now has exactly the pinned Java constants; `from_java_byte(i8) -> Option<LanguageCode>` matches Java's nullable byte lookup, and the prior raw unknown variant/helper aliases are removed. Unknown private-header and JSON inputs still decode to `None`. The existing Java fixture checks all 14 values and unknown/signed byte boundaries. Focused protocol/remoting/config tests and full workspace, strict Clippy, pinned offline Maven, formatter and diff gates are recorded with the implementation commit. Phase 6 source/API/input reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

ClientConfig public accessor reconciliation (2026-10-08): pinned Java 5.5.1 `javap -public` exposes 78 method declarations and seven public string constants. All declaration names and constants are present in the API map; an exact 33-pair table maps all 66 setting `get`/`is`/`set` methods to Rust accessors. The existing Java fixture directly invokes every getter on source, clone, and reset objects, and the focused native differential compares all three snapshots. A static audit found no declaration-name omissions. The accessor matrix adds no runtime behavior; broader Phase 6 source/API/input reconciliation remains open, and P6-V04 remains PENDING_PHASE_19.

Inherited ServiceThread API continuation (2026-10-08): actual Java 5.5.1 cases call `getJoinTime`, daemon/stopped accessors, `wakeup`, `makeStop`, `start`, and both shutdown forms. Unstarted cases record 90,000ms join time, default false flags, daemon setter behavior, no stopped-state change before start, and PullMessageService's executor shutdown. Controlled subclasses show idempotent `start`, post-start `makeStop` setting stopped without joining, and `shutdown(true)` joining the thread; their `run` methods do not execute RocketMQ worker loops. Rust maps lifecycle and work signaling through ClientRuntime and owned tasks, not individual JDK thread controls; `getJoinTime`, daemon flags, the interrupt option, and standalone `makeStop` have no direct Rust API counterpart. Existing runtime lifecycle, pull shutdown, and rebalance wakeup tests cover the owned-task behavior; no worker-loop/thread equivalence is claimed. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verification, comparator/manifest self-test, formatter and diff checks pass. Current client-config fixture SHA-256 `631b1b9537c13cf1f5943b0e958b214a6bf4099b4edc6cd8643a02f4c207d4c0`; [ADR-0026](../architecture/ADR-0026-service-thread-api-boundary.md) records scope. Whole Phase 6 reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

Runtime service-name method continuation (2026-10-08): actual Java 5.5.1 calls to `RebalanceService.getServiceName()` and `PullMessageService.getServiceName()` return their simple class names. Rust exports matching constants, and the Java fixture calls each method directly on an unstarted service. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verification, comparator/manifest self-test, formatter and diff checks pass. Client-config fixture SHA-256 at that update `57114f6198944de08fa6801207525abbbfd9e1532efe93fa02d2c8c64ab98fa4`; [ADR-0025](../architecture/ADR-0025-runtime-service-name-methods.md) records scope. Whole Phase 6 reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

AccessChannel enum-method continuation (2026-10-08): actual Java 5.5.1 cases cover `values()`, `valueOf(String)`, `name()`, `ordinal()`, `toString()`, and `compareTo()`, including array defensive-copy behavior and null/unknown/case-mismatched lookups. Rust maps declaration order, text, comparisons, and the observed `NullPointerException`/`IllegalArgumentException` messages through typed errors. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verification, comparator/manifest self-test, formatter and diff checks pass. Client-config fixture SHA-256 at that update `aa8e0e0e60096bdc7ffa9f7312331ef226e8254f9bd7aeccf9f9a85be1bd3d2f`; [ADR-0024](../architecture/ADR-0024-access-channel-enum-api.md) records scope. Whole Phase 6 reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

Initial LanguageCode public-method continuation (superseded by the correction above; 2026-10-08): actual Java 5.5.1 method calls cover `getCode()`, `getCode(String)`, custom `valueOf(byte)`, all 14 enum constants, null/unknown/case-mismatched names, signed byte boundaries, and unknown-name JSON decoding. The initial Rust mapping preserved a raw unknown-code extension; the correction above removes that extra surface and retains Java's nullable lookup behavior. Client-config fixture SHA-256 at that update was `33441a65926214a006702b46369bfab8f44b35fc1951c1ab17a45166f0408fdf`; [ADR-0023](../architecture/ADR-0023-language-code-java-methods.md) records scope. Full Phase 6 reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

Heartbeat enum public-method continuation: six actual Java 5.5.1 enum cases cover `ConsumeType.getTypeCN()` values `PULL`/`PUSH`/`POP` and `MessageModel.getModeCN()` values `BROADCASTING`/`CLUSTERING`/`LITE_SELECTIVE`. Rust `type_cn()` and `mode_cn()` match each value. Fixture SHA-256 at that update: `7e9521037d5c7b9904ea25ae18ec9923bf6c42d3f9aa65ecf7abb80dd4388589`; [ADR-0022](../architecture/ADR-0022-heartbeat-enum-name-methods.md) records the public-method scope.

MessageRequestMode public-method continuation: two actual Java 5.5.1 enum cases invoke `getName()` for `PULL` and `POP`; Rust adds `MessageRequestMode::name()` and matches both values. Lookup fixture SHA-256 at that update `0c9ccc65b6c95d28d90d8de1bbac000436fe4468f1e7ebace6449ca1cf25c8e2`. [ADR-0021](../architecture/ADR-0021-request-mode-name-method.md) records the scope.

MessageQueueAssignment public-method continuation: two actual Java 5.5.1 method cases cover constructor/default and populated getters/setters, exact `toString`, queue-only `equals` despite different mode/attachments, false for a different queue, and NullPointerException for distinct assignments whose receiver queue is null. Rust adds accessors, `Display`/`java_to_string`, and typed `java_equals`; derived `PartialEq`/`Hash` remains a lawful all-fields Rust collection contract. Java enum `hashCode` uses object identity and is not a stable cross-run numeric oracle. Lookup fixture SHA-256 at that update `0c9ccc65b6c95d28d90d8de1bbac000436fe4468f1e7ebace6449ca1cf25c8e2`. [ADR-0020](../architecture/ADR-0020-assignment-method-api.md) records the public-method scope and limit.

Assignment response Set input continuation: 12 actual Java 5.5.1 cases establish missing/null/empty behavior, object-to-singleton conversion, null members, repeated-null and identical-row deduplication, and rejection of string/number/Boolean top-level values. Native `QueryAssignmentResponseBody` and `ClientRuntime::query_assignment` now preserve `Option<HashSet<Option<MessageQueueAssignment>>>`; decode, set contents, and response encoding match these cases. Lookup fixture SHA-256 at that update `0c9ccc65b6c95d28d90d8de1bbac000436fe4468f1e7ebace6449ca1cf25c8e2`; [ADR-0019](../architecture/ADR-0019-assignment-response-set-input.md) records scope and the Java equals/hash collision limit.

Consumer-list DTO input continuation: 12 actual Java 5.5.1 cases establish missing/null/empty behavior, list order and duplicates, scalar and nested item conversion, null member retention, and top-level string/object/number/fraction/Boolean behavior. The decoded DTO now keeps the nullable List in `SharedValue<Vec<Option<String>>>`; its JSON decoder/encoder preserves the same wire values, while `MQClientApi::get_consumer_list_by_group` and `ClientRuntime::find_consumer_id_list` return the existing owned `Option<Vec<Option<String>>>` by snapshotting. Lookup fixture SHA-256 at that update `0c9ccc65b6c95d28d90d8de1bbac000436fe4468f1e7ebace6449ca1cf25c8e2`; [ADR-0018](../architecture/ADR-0018-consumer-list-fastjson-input.md) records scope and limits.

Assignment attachment DTO input continuation: six actual Java 5.5.1 `QueryAssignmentResponseBody.decode` cases show that numbers, Booleans, arrays, and objects in `attachments` are converted to strings, null map entries remain null in the decoded map, and the default encoder omits those entries. Missing and explicit-null maps both decode as null; non-object shapes reject. Native `MessageQueueAssignment.attachments` now preserves nullable values and matches the recorded response encoding. Lookup fixture SHA-256 at that update `0c9ccc65b6c95d28d90d8de1bbac000436fe4468f1e7ebace6449ca1cf25c8e2`; [ADR-0017](../architecture/ADR-0017-assignment-attachment-fastjson-input.md) records scope and limits.

Assignment response DTO input continuation: 20 actual Java 5.5.1 `QueryAssignmentResponseBody.decode` cases establish that missing `mode` defaults to `PULL`, explicit null and unknown names become null, enum names are case-insensitive, integer ordinals and integer numeric strings map by declaration order, and JSON fractions truncate toward zero. Exact decimal cases show `0.9999999999999999999` maps to PULL, `4294967296.0` wraps to PULL, and plain integer `4294967296` rejects. Fractional strings become null; other out-of-range ordinals and Boolean inputs reject. Native `MessageQueueAssignment.mode` now preserves null and the constructor default, and its decoded/re-encoded values match. Current lookup fixture SHA-256 `42a05d175175b94dc7e1bfc051940f70b16a01cbba0345ff2a35a5a0fa2d57e1`; [ADR-0016](../architecture/ADR-0016-assignment-mode-fastjson-input.md) records the scope. Other assignment DTO combinations and whole-phase source/API/input reconciliation remain open.

Heartbeat DTO Fastjson input continuation: 52 actual Java 5.5.1 inputs cover scalar coercions and collection shapes in `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData`. Java accepts singleton string/object `tagsSet`, rejects numeric/Boolean `tagsSet`, converts scalar `codeSet` Boolean/object inputs to 1/0, and applies HashSet deduplication. Numeric probes include i32 boundaries and overflow, exponent strings, wide and high-precision decimals, plus high-precision numeric-to-string conversions, and `subVersion` empty object/array rejection. The differential matches the pinned acceptance and values; Serde errors do not claim Java exception-class compatibility. Inherited heartbeat decode/list/fromJson null results remain covered. Earlier oracle/test commits `53add8a`/`f55e95b`, implementation commits `478d6e0`/`f78269f`; precision probe/test commits `51191e9`, `cb75740`, `f4272fd`, and `17b46bd`; shared implementations `e39abf5` and `5e30b73`; current fixture SHA-256 `e183f318134bd5dfa85bcbd80276c3e85c444f3ca0ebbe8231696ef2b4305db7`; [ADR-0015](../architecture/ADR-0015-heartbeat-dto-fastjson-input.md) records scope. Other DTO combinations, broader Fastjson grammar, and full Phase 6 method/field/branch reconciliation remain open.

Earlier route DTO Fastjson input continuation (expanded by the opening update): the original 25 actual Java 5.5.1 inputs exercise `QueueData`, `MessageQueue`, `BrokerData`, `TopicQueueMappingInfo`, and nested `TopicRouteData`. Five direct `MessageQueue.queueId` cases cover explicit null to zero, signed integer boundaries, fractional truncation, and overflow rejection. The Rust DTO decoders match the observed scalar-to-string, primitive-null, numeric string/fraction, boolean, numeric map-key, nullable address, and filter-list outcomes. The original route fixture SHA-256 was `5cc8ace8e2a4622f7aa849270b77ff6b3adf2f04cd99dd6d6563566744c8a104`; see [ADR-0014](../architecture/ADR-0014-route-dto-fastjson-input.md). Other enum/DTO combinations, broader Fastjson grammar, final method/field/branch reconciliation, and P6-V04 remain open.

Latest SOCKS and ClientConfig copy-field oracles: the pinned Java constructor grammar fixture has 492 cases. Separate exhaustive escape-follower scans cover every representable BMP scalar and all 1,048,576 supplementary scalars after a backslash; Java accepts 253,952 supplementary scalars and native parsing matches all outcomes and values. Java-only isolated surrogate units cannot be represented by Rust `String`. Java `NettyRemotingClient.start/invokeSync` passes both username/password and no-auth scripted SOCKS5 paths; paired Rust tests cover both. The fixture also snapshots all 33 non-internal `ClientConfig` fields on source, clone, and reset objects. Current `client-config.json` SHA-256: `bf9b63dfb9c38a4f4090a4326a5d14bc9bff62478e4df4330825cb5009bc08f3`; [ADR-0036](../architecture/ADR-0036-fastjson-surrogate-boundary.md) and [ADR-0038](../architecture/ADR-0038-fastjson-socks-escape-grammar.md) record the limits and mapping. Remaining Fastjson grammar and whole-phase source/API/input reconciliation remain open; P6-V04 remains PENDING_PHASE_19.

`ClientConfig.toString()` update: native `Display` matches 16 actual Java 5.5.1 cases covering null/populated values, namespace-cache state, unescaped string contents, signed numeric boundaries, and every Java `LanguageCode` constant. Java includes `namespaceInitialized` and the raw SOCKS proxy config in its text; native `Debug` remains separately redacted. Focused differential `client_config_to_string_matches_invoked_java` passes. The fixture SHA-256 at that update was `ff0edae74d25a91bd4a54ca2c01e06c12715d891fe7cbaff956a7a1bb06b2578`; the current client-config fixture hash is recorded in the opening update above. Full Phase 6 source/API/input reconciliation remains open.

`ClientConfig.queuesWithNamespace`/`MessageQueue` update: the Java fixture invokes six collection cases and queue setters. Native matches same-collection identity, null/empty namespace short-circuiting, Java null failures, partial mutation, and topic-only wrapping; setter snapshots include nullable strings and signed queue IDs. Focused tests `queues_with_namespace_matches_invoked_java_collection_and_null_behavior` and `message_queue_setters_match_invoked_java` pass. The fixture SHA at this update was `4c0b831de2f3139b5be32e59fede60d639296e8dbffa20a9924c08fbabdab4bb`; [ADR-0011](../architecture/ADR-0011-message-queue-namespace-mutation.md) records the source contract.

`ClientConfig` accessor update: the pinned Java fixture directly invokes all 33 public setting getters and setters, in addition to comparing source/clone/reset fields. The native differential now constructs through Rust setters and reads through Rust getters, matching all three Java getter snapshots, including nullable strings/enums and integer boundaries. Fixture SHA-256 `2824c98496001ab46cc7137400359f51b2afe443d6fee779946606d78f379463`; [ADR-0012](../architecture/ADR-0012-client-config-accessors.md) records the API mapping. This closes the named accessor gap; full Phase 6 source/API/input reconciliation remains open.

`MessageQueue` Java method update: actual Java method cases cover defaults, populated values, supplementary Unicode, signed queue IDs, self/copy/null/wrong-type/changed-field equality, `hashCode`, and `toString`. Rust `Default`, same-type `PartialEq`, `java_hash_code()`, and `Display` match the supported value outcomes; `message_queue_hash_equals_and_text_match_invoked_java` passes. Route fixture SHA-256 `9b3f4be54464f7718632f7bcd7d5bde88559234f2c42141a15f9f48d48bc3a1d`; [ADR-0013](../architecture/ADR-0013-message-queue-java-methods.md) records the mapping and Rust `Hash` distinction. Broader DTO/API/input reconciliation remains open.

Heartbeat DTO public API update: actual Java 5.5.1 cases now cover `SubscriptionData` constructors/`SUB_ALL`/`equals`/`hashCode`/`compareTo`/`toString` and ProducerData, ConsumerData, and HeartbeatData `toString`. The native red differential reproduced that derived Rust equality incorrectly included `filterClassSource`; Java ignores it, compares `subVersion` but omits it from hash, and compares concatenated `topic@subString` in UTF-16. Native DTO equality, Java hash/text/comparator methods, and `SUB_ALL`/`new` now match the recorded outcomes; the method oracle also covers null-versus-empty sets and `filterClassSource` omission from text. Heartbeat fixture SHA-256 `8846dc5542616264485fa10759f101d7af14588869d954cbbe73bfcbc3dd4f45`; all 10 focused protocol heartbeat tests pass. This closes the named DTO methods only; whole-phase reconciliation remains open.

Heartbeat consumer enum correction: actual Java cases cover all 12 constants across the three `ConsumerData` enum fields. Fastjson maps case-insensitive names and valid integer ordinals, truncates fractional numbers toward zero before ordinal lookup, maps unknown names to null, and rejects out-of-range values; native optional enum deserialization now matches. The red native test reproduced the unknown-string mismatch before the fix, and the expanded oracle exposed the additional ordinal/name coercions. This narrows the enum-input gap only; other DTO inputs and the whole Phase 6 method/field/branch audit remain open.

Heartbeat ProducerData collection correction: actual Java `HeartbeatData` decode keeps duplicate producer rows because `ProducerData` inherits Object identity equality. Native decoding previously used a structural HashSet and collapsed the two equal-valued fixture entries; the red `toString` comparison showed one native row versus two Java rows. The producer collection now preserves rows through JSON round-trip, and its duplicate-input fingerprint matches Java. This addresses that DTO input case only; full source/API/branch reconciliation remains open.

Heartbeat property-name correction: actual Fastjson ignores `clientId`, `isWithoutSub`, and tested mixed/uppercase spellings. Native had accepted `isWithoutSub` through a serde alias; the red fixture case found the extra behavior, and the alias is removed. Exact property spellings now match for the tested heartbeat fields; DTO/source/API reconciliation remains open.

Heartbeat nullable Set member update: actual Fastjson retains one null in producer, consumer and nested subscription sets, re-encodes it, and deduplicates repeated nulls. Rust now preserves nullable rows while retaining identity-distinct non-null producer/consumer duplicates and value-deduplicating nested SubscriptionData. A null producer computes the invoked Java fingerprint; null consumer/subscription fingerprint cases match Java NullPointerException with typed native errors. `heartbeat_null_set_members_match_invoked_java` covers fields, encoded JSON, text and fingerprint outcomes. Fixture SHA-256 `6299d2625866d7c72eb0c97c17bcfc3dcd8bcab9cf382f4c04d1174e1ff5106f`; full workspace, Clippy, Maven, comparator, manifest and formatting checks pass. Whole Phase 6 reconciliation and P6-V04 remain open.

Static mapping map input probe: six actual Java `currIdMap` cases reject null/malformed entries and accept signed i32 boundary keys/values. Native BTreeMap decoding matches all accepted/rejected outcomes, though Rust parse errors do not expose the same Java exception classes. `static_mapping_map_entry_inputs_match_invoked_java_acceptance` verifies them; route fixture SHA-256 `962d63f6f615b552410a5717298cd2f8e10e67be3e26ad897633ce231db6020e`. This closes only these inputs; Phase 6 source/API reconciliation remains open.

`BrokerData.selectBrokerAddr`, nullable nested route DTOs, the direct `TopicRouteData.topicRouteDataChanged` counterpart, and copy-constructor null defaults are covered by pinned Java fixtures. Ten actual selection cases exercise master-first, random fallback, null-address, null-map, and empty-map outcomes; the Rust DTO method matches allowed outputs and typed error branches. Native route values preserve null list elements and null nested lists/map values; cache refresh rejects null queue, broker, and static mapping values before publication while retaining accepted null filter lists. `java_topic_route_data_changed` matches 11 actual Java outcomes, including `old == null`, single null entries, and multi-entry sort-time `NullPointerException` cases; route publication, route comparison, and `RouteCache` use the shared `java_copy_constructed` normalization. Current route fixture SHA-256 `7c5f6b1c09937d21a63252e9c1359485ebde19b68284f63de1215781185ad6db`; focused protocol and core regressions pass. The earlier shallow-clone nested-identity caveat is superseded by the later shared-handle implementation and oracle differential recorded below. Full Phase 6 API/input reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

TopicRouteData clone helper update: eight actual Java cases exercise copy/clone/deep-clone field values and null failures. Native `clone_topic_route_data` and `deep_clone_topic_route_data` pass those outcomes; live route-table identity remains shared through `TopicRouteDataHandle`. The shallow helper's nested-object identity is checked by the fixture alias map; the later shared-handle correction below closes that identity slice. Current route fixture SHA-256 at this update `08038bfb606aaa057869cb3e41256a410ec03d8c4f85003aa32055be69ea21a2`; focused protocol test passes. Whole-phase API/input reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

Route DTO text update `07e62e8`/`449e97e`/`1d6f799`: actual Java fixtures capture two `BrokerData.toString` and three `TopicRouteData.toString` cases, and native `java_to_string` matches nested DTO/list/map text. Rust emits maps by deterministic key order because Java map iteration order is unspecified. Route fixture SHA-256 `d26481b826d4323e5e1f017108de2772051b0caf968257642a101a4f48537fef`; full workspace/all-target tests, strict all-target/all-feature Clippy, pinned Maven verify, comparator/manifest22, fmt and diff pass. Additional DTO/input combinations and full Phase 6 source/API/input reconciliation remain open; P6-V04 remains PENDING_PHASE_19.

SOCKS numeric/scalar extensions `457d16b`/`37af420`/`cc6422e`/`09d0878`/`6c4d59b` expand the pinned Java 5.5.1 constructor/parser differential to 73 `socksConstruction` cases. Twelve additional actual runtime inputs cover exponent thresholds, signed/unsigned leading-dot syntax, decimal scale, overflow, and underflow. Java formats `1e-4` as `1.0E-4`, `1e-7` as `1.0E-7`, `1e7` as `1.0E7`, keeps `1e6` as `1000000.0`, preserves `1.2300`, and rejects unsigned `.5e-3` while accepting signed forms. Native coercion matches all 73 constructor and reflected proxy-map outcomes, including exact decimal text, signed zero and infinity. Client-config fixture SHA-256 `a1054ec9af61312d25037da4470d17629babee026d4e6da6371cb9555b81e175`. Focused remoting tests, full workspace/all-target tests, strict all-target/all-feature Clippy, offline Maven verify (route25/config73/manifest22), comparator/manifest validation, fmt and diff checks pass. Broader grammar, end-to-end SOCKS connection behavior and full source/API/input reconciliation remain open.

Route DTO method updates `04848b0`/`dcfa7da`/`2c6a476`/`9dfaa53`/`ac3b604`/`533600a`/`13248bf`/`cec1821`/`c7b32bc`/`f0607f9`/`20cff94`/`e6a3389`/`329b0f9`/`b04b2d9`/`1df78b5`/`f6add71`/`18f8565`/`af642aa`/`38ec7a8`/`f9f6450`/`c7d8c01`/`21cc626`: six Java BrokerData equality, six comparison, three hash, five QueueData method, four TopicQueueMappingInfo equality/hash/text, four TopicRouteData equality/hash, and six route-change sort outcomes are captured. Rust equality/hash/text behavior matches, and route cache sorting uses fallible Java comparators for both sortable DTOs, including verified null-name behavior. Route fixture SHA-256 `3dc47fdc1e774357747853f4dfe270f4f1684a8a302d5502f905109a35b76891`; focused protocol/core regressions, full workspace/all-target tests, strict Clippy, pinned Maven verify, comparator/manifest22, fmt and diff pass. Other DTO method/input surfaces and full Phase 6 source/API/input reconciliation remain open.

ConcurrentMap traversal mapping: the Java 5.5.1 registry and route tables use `ConcurrentHashMap`. Its public iterator contract guarantees entries present at iterator construction, permits but does not require later changes, and forbids fail-fast exceptions ([Java SE 8 `ConcurrentHashMap`](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentHashMap.html)). Native detached snapshots taken when traversal starts provide one permitted outcome; retained view handles remain live for later reads and writes. Existing Java iterator-removal oracle outcomes map to equivalent explicit Rust view removals. This closes weak-iteration visibility as an open mismatch; exact Java iterator object and Collection overload signatures remain Rust ownership/API mapping boundaries.

Raw TopicRouteTable map/view update `d689453`/`f7574f5`/`0a03138`/`8666128`/`ee6d660` invokes Java keySet, values and entrySet contains/remove/removeIf/removeAll/retainAll/clear plus map null-return remapping branches and iterator removal. All59 actual Java outcomes are true; native view methods match all59 observable map/view effects using source route-content equality, with Java iterator removal represented by equivalent Rust live-view remove methods. Route fixture SHA-256 `f579c47db7ee87efdd315d71d82e918e3cccf7b782505562b459822a29b4e083`. Full workspace tests, strict Clippy, pinned Maven, fmt/diff, comparator and manifest22 pass. The later ConcurrentMap contract audit above maps traversal snapshots to an allowed weakly consistent iterator outcome; other defaults and broad DTO/input/source audit remain open.

Mutable Java route DTO alias update `c085d03`/`82430b5`/`03e3925`: the invoked runtime proves `put` retains the passed `TopicRouteData` object, and mutation through either source or table getter is visible through the other alias. Fixture SHA-256 `27d8e568e0e5042aa891cdd08af94b7ffd28667e1dd02d3df5d94d7650646e65`; native `TopicRouteDataHandle` matches all three outcomes through shared identity, `snapshot()` and `with_mut()`. The raw map retains that handle; the separately published `TopicSnapshot` has its own outer route collections but shares nested mutable DTO handles, as confirmed by the later runtime refresh oracle `827f6510` and implementation `a87e3726`. Full workspace/all-target tests and exact all-target/all-feature Clippy pass. Offline Maven verify passes with route25/config73 and manifest22; the differential comparator self-test, manifest check, fmt and diff checks pass. This closes the mutable route alias item only.

SOCKS proxy-selection update `47f3fa0`/`8bf543b` invokes private Java `NettyRemotingClient.getProxy` for eight real unstarted runtime cases. Native matches subnet hit/miss, default-route hostname/IPv6, masked `/33`, direct null route, invalid CIDR failure and empty-map behavior. Its then-current client-config fixture SHA-256 `46762834bf876a467f64bbb01e19327f765fcbb21e4c110189c1173a0f14cdf3` is superseded by later parser fixtures above. Focused remoting selection/CIDR/parser regressions pass. Later authenticated/no-auth Java SOCKS5 round trips close the handshake item; broader parser grammar remains open.

SOCKS CIDR update `1e88146`/`d77c43c`/`c57c8ce` invokes pinned Java `RemotingHelper.ipInCIDR` for 12 host/CIDR inputs, including extra dot components, trailing dots, signed/out-of-range octets, Java shift-masked prefixes, extra slash suffixes and malformed input. Native now matches all boolean/error outcomes; the red regression caught Java's ignored fifth address component before correction. Its then-current fixture SHA-256 `07e8ca582ea56b813a69d3cbedd6031d93f8b74f6dbec245e6d4d7670a3df3c9` is superseded by later parser fixtures above. Full workspace/all-target tests, exact all-feature Clippy, pinned Maven, fmt/diff, comparator and manifest22 pass.

SOCKS Fastjson grammar update `bc5a10d`/`21c9fcb`/`3bf035d` compares pinned Java constructor and reflected parser outcomes with native parsing for 33 inputs. The added cases establish five rejections (unquoted identifier values, hexadecimal number, `undefined`, `NaN`, `new Date(...)`) and three accepted forms (duplicate-field last value, line comment, `\x` string escape); all outcomes and parsed values match. The then-current fixture SHA-256 `1c3c4a9f023952b5d18b3d503c1441f9d6af70a5ccf397680c758e23b1e8f78c` is superseded by the later parser fixture above. Additional grammar remains open; the Java SOCKS5 connection path is covered by later authenticated/no-auth round-trip cases.

FactoryTable iterator/remapping update `8153350`/`53a386f`/`3d247a3`/`cdc6dcd` invokes Java iterator removal on keySet, values and entrySet and null-return `computeIfAbsent`, `computeIfPresent`, absent `compute`, and present-key `merge` removal. All48 `managerTable` outcomes match at SHA-256 `27ef695764fb71aa7bcb707279f4bc4e992f3b8fde61223ff58d3effd977272f`; Rust maps iterator-removal effects to live-view `remove` methods. Full workspace tests, strict Clippy, pinned Maven, fmt/diff, comparator and manifest22 pass. The later ConcurrentMap contract audit above maps traversal snapshots to an allowed weakly consistent iterator outcome; exact generic overload signatures and broad input/source audit remain open.

Mutable registry owner equality update `c47d07b`/`4659e00`/`d4d8fdc`/`f3f1bc1`: pinned Java 5.5.1 mutates owner equality keys and verifies `containsValue` before/after owner/probe changes plus conditional replacement against stale/current expected values. Rust samples current `map_value_equality_key` values outside locks and commits conditional mutations by observed Arc identity with retries. The corrected native regression fails against insertion-time key caching and passes with the fix. Fixture SHA-256 `6bfe7bb6ce6bd07bb75fb8a817524603dbfbc5f83e0c9ac7db6808f48cb4bb64`; full workspace, strict Clippy, Maven, comparator/manifest22, fmt and diff pass. This resolves the mutable key case; broad API/source/input reconciliation remains open.

Raw TopicRouteTable membership/traversal update `8c5fba9`/`ccfb1e6`/`06cb233` invokes Java `containsValue` for an equal route copy and an absent broker, and `forEach` across all three raw mappings. Native membership uses the pinned route-content equality; traversal visits a detached snapshot and permits callback reentry without a cache lock. Route fixture SHA-256 at that stage was `42ab9c7753aeadfe57f83e111bcbb9f28e35db889d825d57ddb7335be3bfe0cd`, with all34 captured primitive outcomes true. The following view update supersedes this entry's open view-operation status. Full workspace, exact all-feature Clippy, pinned Maven, fmt/diff, comparator and manifest22 passed; the later source-contract audit above resolves the weak-iteration mapping, while broad input/source audit remains open.

Raw topic-route map remapping update `2c29dab`/`8ca9976`/`612911d`/`a973104` invokes Java `computeIfAbsent`, `computeIfPresent`, `compute` insert/remove, `merge` present/absent and `replaceAll`. Native `TopicRouteTable` uses source route-content equality for conditional updates; callbacks run outside cache locks, and writes remain confined to raw routes. The route fixture captures 31 primitive outcomes at SHA-256 `b78d731d0756b0a0af3667253e51a92b655fd7198e450a98813997c461743d75`. Focused route-table, full workspace, exact all-feature Clippy, pinned Maven, fmt/diff, comparator and manifest22 pass. Live collection views and weakly consistent Java iteration, plus broader DTO/input reconciliation, remain open.

FactoryTable remapping update `d3e4646`/`fe2ca09`/`6d6229a`/`53576aa` invokes Java `computeIfAbsent`, `computeIfPresent`, `compute` (insert and null-result removal), `merge` (existing and absent keys), and `replaceAll` on the live map. Native counterparts use identity-conditional writes, keep mapping callbacks outside locks, and may retry a callback when another writer wins. Client-config fixture SHA-256 `0c0bac433748e8638ca172a6efc68af43d81bfa3b860ac7490c761d3fe65cbf2`. Focused manager_table3, full workspace, exact all-feature Clippy, pinned Maven, fmt/diff, comparator and manifest22 pass. Iterator removal and Java weak consistency remain open; full Phase 6 source/API/input reconciliation remains incomplete.

FactoryTable view membership update `f90b31c`/`3785c27`/`d7e3ad8`/`0478998` invokes Java key-view removeAll, values-view removeAll with identity-equal runtimes, and entry-view retainAll with an exact key/value pair. Native views now provide matching `remove_all`/`retain_all` operations over detached snapshots, with conditional value/entry removal protecting concurrent replacements. Fixture outcomes are true at SHA-256 `aebdcf72e4903bbbfdda6429b386aaabae873bbc0125f5374c5e570da5993354`. Focused manager_table3, full workspace, exact all-feature Clippy, pinned Maven, fmt/diff, comparator and manifest22 pass. Java iterator behavior and remaining `ConcurrentMap` remapping defaults (`compute*`, `merge`, `replaceAll`) remain open.

FactoryTable view-predicate follow-up `c4b6eec`/`da7eff9`/`4c23e3b` invokes Java `removeIf` on keySet, values and entrySet. Native live views now provide `remove_if` over detached snapshots; values/entry removals compare the observed runtime identity before mutation. The existing Java fixture outcomes remain all true at SHA-256 `f00c06f343103c7523cbc4722cecc0a960a01a9402f74de5f0c658a5ad880f73`. Focused manager_table3, full workspace, exact all-feature Clippy, pinned Maven, fmt/diff, comparator and manifest22 pass. `removeAll`/`retainAll`, Java iterator behavior, weakly consistent traversal and remaining ConcurrentMap defaults still require reconciliation.

FactoryTable retained-view update `3071a9a`/`7f1f139`/`55dae87`/`061cd72` invokes actual Java keySet/values/entrySet retention, membership, key/entry/value removal and clear outcomes. Native cloneable live view handles read the current table and remove through per-key mutations; clear does not shut down runtimes. View iteration returns detached copies, and these APIs do not claim Java weakly consistent iterators. Client-config fixture SHA-256 `f00c06f343103c7523cbc4722cecc0a960a01a9402f74de5f0c658a5ad880f73`. Focused manager_table3, full workspace, exact all-feature Clippy, pinned Maven, fmt/diff, comparator and manifest22 gates pass. `removeIf`/bulk view operations and other inherited ConcurrentMap defaults remain open.

FactoryTable value-membership update `ddbdba9`/`2628d3e`/`35aab7f` invokes Java `containsValue` for a mapped runtime and a removed runtime, then implements native identity-based lookup consistent with `MQClientInstance` Object equality. Client-config fixture SHA-256 `75343b533a7bcdb2a1ef83317c498c2d678e39309b7ef7011a2ac71bd284b6cd`. Focused manager_table3, full workspace, exact all-feature Clippy, pinned Maven, fmt/diff, comparator and manifest22 gates pass. Other FactoryTable collection views/defaults and the wider Phase 6 source/API/input audit remain open.

FactoryTable traversal follow-up `c7795d6`/`582d1d6`/`59480b8` invokes actual Java `ConcurrentMap.forEach` over four live mappings and adds native `for_each` over one detached snapshot. Native callbacks run outside manager locks and may reenter; weakly consistent Java iteration is explicitly not claimed. Client-config fixture SHA-256 `c92e2d70e5c5e0ba82685cc03751ab89f871bc82730f9130f8299ee85da5c62b`. Focused manager_table3, full workspace, exact all-feature Clippy, pinned Maven, fmt/diff, comparator and manifest22 gates pass. FactoryTable live collection views and other inherited defaults remain open, so Phase 6 is incomplete.

FactoryTable default-helper update `9c8633b`/`422f85e`/`879e510` captures pinned Java `getOrDefault` present/absent and `putAll` identity outcomes, then adds native counterparts using published reads and existing per-key mutation/initialization handling. This does not make bulk insertion a transaction; live collection views and other inherited `ConcurrentMap` defaults remain open. Client-config fixture SHA-256 `e392f68d549d90ef188a3f8fa39210b2fd2a331f789ffe8bb2568612bf457cc6`. Full workspace, exact all-feature Clippy, pinned Maven, fmt/diff, comparator and manifest22 gates pass. Phase 6 remains incomplete pending the remaining source/API/input audit.

Raw topic route map follow-up `f5431b6` adds native `get_or_default` and `put_all` after invoking the same operations on the pinned Java live `ConcurrentMap`; the initial bulk helper applied the entries in one raw snapshot update. The later atomicity correction changes `put_all` to independent keyed writes, matching the Java map contract while preserving derived-route separation; see the latest route-table continuation and [ADR-0082](../architecture/ADR-0082-raw-route-table-remapping-atomicity.md). Route fixture SHA-256 `34f3d9b5be82c3989f90d1f3d1116ee105c9c6d4346b6a7a5a0a4a8203061f43` is the earlier checkpoint. The expanded route fixture/current gates are recorded in the route audit. This closes those two invoked helpers only; other map defaults and DTO/input/source reconciliation remain open, so Phase 6 is incomplete.

`checkClientInBroker` null-topic oracle `d591091` adds a pinned Java non-TAG case: null topic fails before route lookup or broker RPC. Native typed invalid-route result likewise makes no request; fixture SHA-256 `73990a351770ca0d8451590eef16bc3931115f90853193f37f3e4e18d61c841d`. Full API/source-input reconciliation remains open; Phase 6 is incomplete.

Java owner-null preparation oracle `1aac047` confirms `prepareHeartbeatData(false)` fails when a registered consumer returns null subscriptions, while simple mode produces an empty set. Native's typed error and empty-simple behavior match; focused runtime regression and pinned Maven verify pass. Fixture SHA-256 at that update: `189e4a80c6f7f02083efc15dfe39535321872860a3d058c77eca975564193036`. Other API/source/input reconciliation remains open; Phase 6 is incomplete.

Public heartbeat API correction `91702a2`/`3c4f444` adds the missing `HeartbeatData::compute_heartbeat_fingerprint()` counterpart for Java's public `computeHeartbeatFingerprint()`. Seven invoked Java values match at protocol level, and core uses that same public method. Heartbeat fixture SHA-256 `118b49612461931280cf65e7912ecceb7deb1d30869a82e7e2d27b71b8afc53c`; full workspace, Maven and strict Clippy pass. Other public/source/input reconciliation remains open; Phase 6 is incomplete.

Null subscription topic route correction `b57594c`/`98556c1` matches actual 5.5.1 all-topic refresh: null enters the union, emits a route request with absent `topic`, and a rejected response permits other lookups. A successful route follows Java's NullPointerException with a typed native error before cache publication. Route fixture SHA-256 `a8c6be7d5fb98f4a660f15835dfea6c74c03464c63d958c48f0d5c8c227a4b1d`; focused sockets, full workspace, Maven and strict Clippy pass. This closes the recorded null-topic branch; remaining full API/flow audit keeps Phase 6 incomplete.

Null-input fingerprint and primitive decode correction `4cbd803`/`5fe4045` matches four invoked Java 5.5.1 fingerprint calls and five explicit-null primitive inputs. JSON clone restores null collections to constructor-empty arrays; primitive null becomes zero/false, distinct from absent SubscriptionData subVersion's current-time default. Fixture SHA-256 `118b49612461931280cf65e7912ecceb7deb1d30869a82e7e2d27b71b8afc53c`; full workspace, Maven, strict Clippy and differential gates pass. Null-topic runtime routing and remaining public API/flow audit keep Phase 6 incomplete.
Null-input fingerprint and primitive decode correction `4cbd803`/`5fe4045` matches four invoked Java 5.5.1 fingerprint calls and five explicit-null primitive inputs. JSON clone restores null collections to constructor-empty arrays; primitive null becomes zero/false, distinct from absent SubscriptionData subVersion's current-time default. Fixture SHA-256 `118b49612461931280cf65e7912ecceb7deb1d30869a82e7e2d27b71b8afc53c`; full workspace, Maven, strict Clippy and differential gates pass. The then-open null-topic branch is resolved by the newer oracle above; remaining public API/flow audit keeps Phase 6 incomplete.

HeartbeatData public collection correction `6b355b8`/`41a5fd2` preserves Java's missing-empty versus explicit-null producer/consumer sets and null clientID JSON omission in three invoked cases. Native consumer Vec retains the prior duplicate-metadata guarantee. Initial heartbeat fixture SHA-256 `970674db0da0cc0a3e051f13082c30276113c10dff86309c349e0cd05f975bbb`; full workspace, Maven, strict Clippy and differential gates pass. The then-open fingerprint/primitive branches are resolved by the newer oracle above; Phase 6 remains incomplete.

ProducerData/ConsumerData correction `f8b745a`/`633a4a5` closes the demonstrated null group/enum/subscription-set DTO mismatch through six invoked Java decode/JSON cases and native preparation tests. Missing consumer subscriptions default empty; explicit null remains distinct. The newer `1aac047` actual runtime invocation confirms full-mode NullPointerException and simple-mode empty behavior. Heartbeat fixture SHA-256 `ce4d66d25c1031e36531ac184125120d480b3f715ffa3c944e63155c16cfc030`; full workspace, Maven, strict Clippy and differential gates pass. Other heartbeat DTO/input branches remain open; Phase 6 is incomplete.

Public SubscriptionData correction `1dce870`/`40f6f65` preserves Java 5.5.1 nullable topic, subString, expression type and tag/code collections, plus constructor defaults and wall-clock subVersion. Three invoked decode/JSON cases give focused red/green evidence; heartbeat fixture SHA-256 `b257ae6b52a097a871279df308e985849db0844b20fd78182e59c8f1fb488ace`. Full workspace, pinned Maven, strict Clippy and differential gates pass. Null-topic runtime routing and primitive-null decoding are resolved by the newer oracles above; the other heartbeat DTO/input surfaces remain open and Phase 6 is incomplete.

Overlapping static owner evidence `cb33e1b` originally added three invoked Java endpoint conversion inputs. The ten-case continuation is recorded at the top of this audit and supersedes that coverage count. Equal-epoch winner ordering remains unspecified by Java HashMap; malformed input and full Phase 6 reconciliation remain open.

MessageQueue comparison `c2c34e3`/`b001f65` closes the demonstrated public null comparison and exact result gap through eleven invoked Java 5.5.1 cases. A separate fallible `java_compare_to` retains Java null failure and String/Integer comparison results while internal `Ord` keeps valid tree ordering. Pinned tag commit `efa1be59` and 5.5.1 bytecode both use `Integer.compare`; the earlier claimed divergence came from the separate Java checkout's 5.3.3 HEAD and is withdrawn. Route fixture SHA-256 `a709df838628378dbca6044027cd873fd0b6a80c90591a24bf395207228c3269`; full workspace, Maven, strict Clippy, comparator and manifest gates pass. Full API/source/input reconciliation and Phase 6 acceptance remain open.

SOCKS private route-field correction `34ce95a`/`b3ada16` closes the six invoked Java numeric, Boolean, null and empty-array `addr`/credential constructor cases. Reflected Java proxy values and native private parser output match; the previous native construction golden failed before the fix and passes after it. Config fixture SHA-256 `eefbb3c681864effb187cbabcef7e70750b079876d5e45e138d83f6aa6a5264d`; full workspace, strict Clippy, pinned Maven, comparator and manifest gates pass. Other Fastjson input grammar, connection paths and complete API/source/input audit remain open; Phase 6 is incomplete.

Public MessageQueue DTO correction `3577198`/`e242764` closes the demonstrated null topic/default queueId mismatch. Native topic/broker getters and JSON now preserve Java nullable states, and namespace/offset queue copies retain them. Route fixture SHA-256 `e5520a19c71483fce328312f9d4263c6af7a0bfff4c712e40660ca90626c3453`; focused red/green, full workspace, Maven and strict Clippy pass. The later 11-case `java_compare_to` oracle also covers null topic/broker failures; broader API/source/input reconciliation remains open.

Nested nullable route DTO update `132f838`/`02e95f2` follows invoked Java missing/null BrokerData and QueueData fields, zero primitive defaults, subscribe queue null broker identity, and four runtime refresh branches. Native public DTO/MessageQueue now represent nullable broker names; focused red/green, full workspace, Maven and exact Clippy pass. MessageQueue topic null/default and other public input surfaces still prevent Phase 6 acceptance.

Registered-consumer nullable route branch `c4b6e17` supersedes the source-only consumer caveat below: actual Java throws on ordered null queue with a consumer, and the scripted native runtime returns an error without publishing or notifying. Fixture SHA-256 `e9303fc3ceac0fc13ddf6181163847c3b85c1d2c684f9a3fd4c71ab1968f82d5`; full workspace, Maven and strict Clippy pass. Other malformed DTO/input and whole-phase API checks remain open.

Nullable route refresh branches `2b0d6f0`/`65eb745`: five actual Java unstarted runtime cases distinguish ordinary null queue/broker failures from ordered/static null queue and null filter success; successful cached copies normalize collections. Native focused red/green, full workspace, Maven and strict Clippy pass. At this stage, the consumer-bearing ordered case had not yet been invoked; the later continuation at the top of this audit covers both ordered failure and static-mapping success with a registered consumer. Full Phase 6 input/source audit remains incomplete.

Static mapping DTO/input correction `f1f53f4`/`9285a7a` follows actual Java missing/null field and converter cases. Public topic/broker-name nullability, absent/default ID map and scope, JSON omission, and four endpoint outcomes now have native red/green evidence. Full workspace, pinned Maven and strict Clippy pass. Overlapping malformed mapping owners and the broader API/source/input audit remain open; Phase 6 is incomplete.

Nullable mapping wire correction `680798f`/`044de86`: pinned Java omits absent topic queue mapping from route JSON. The native public DTO now matches, with a red/green fixture regression and passing full workspace, Maven and strict Clippy gates. Other API/input/default-map cases remain in the open Phase 6 audit.

Null route wire correction `d6a44ad`/`09af643` adds actual Java `toJson()` evidence: null collections are omitted from JSON, decoded null remains distinct in memory, and omitted fields reinitialize empty. The native regression was red before serializer correction and passes now; full workspace, pinned Maven and strict Clippy pass. This narrows only the demonstrated DTO/wire case, leaving the broader source/input audit open.

Nullable route collection update `0d8daec`/`86af455`: pinned Java Fastjson and copy-constructor cases establish explicit null versus empty for three public route collections. Native DTO, route comparison and cache publication now preserve those observed distinctions; protocol/core fixture tests, full workspace and strict Clippy pass. This closes the demonstrated DTO mismatch. The consumer-free static-mapping case is represented in the runtime fixture, and its registered-consumer counterpart is covered by the later continuation at the top of this audit. The broader API/source/input audit remains open, so Phase 6 is still incomplete.

Status: **INCOMPLETE — not Phase 6 acceptance or Phase 7 handoff**. Current nullable Broker correction is `8fb1ec8`; the earlier `dd7a569` red/ignored oracle status is historical. Inspected production baseline: `a2779c3`; live handoff registration is `5b3212b`; blank-serve ownership mapping is `c3eb2f8`. Client oracle is `rocketmq-all-5.5.1`, commit `efa1be59d6fc747ad220f036c86030f05f0419f9`, artifact `org.apache.rocketmq:rocketmq-client:5.5.1`. Java repository is read-only. Server 5.3.4 is a Phase 19 execution baseline, not a Phase 6 prerequisite.

Requirements come from the entire [Phase 6 child plan](../../plan/2026/09/07-phase-06-runtime-routing-heartbeat.md), [execution index](../../plan/2026/09/README.md) and the master Phase 6 tasks/Definition of Done. This audit distinguishes a demonstrated unit gate from complete public API/settings/source parity. Green suite output does not waive a missing input, method or branch.

## Numbered tasks

Paths in the owner column are relative to `crates/rocketmq-classic-core/src/`; integration test paths are relative to that crate. Each UNIT_GATE entry covers only its explicitly described invariant.

| Requirement | Current owner and inspected executable evidence | Audit result |
| --- | --- | --- |
| P6-01: IP/instance/unit/stream identity and constructor configuration | `config.rs`, `config/local_address.rs`, `config/namespace.rs`; 73 Java identity cases; direct NameServer property/environment precedence and active-list snapshot under retained config mutation; TLS mode/default probe; all eight direct system-property fields; automatic Java local-IP constructor and current host candidate-list selection fixtures; defaults/resources/manager/nullable/SOCKS/callback cases; `copyFields` oracle for all 33 non-internal settings across source/clone/reset; actual runtime `getClientId` remains fixed after retained config identity fields mutate; private exhaustive constructor-field and namespace-state tests | UNIT_GATE for documented identity/copy branches, macOS IPv4 selection, Unix IFF_UP|IFF_RUNNING eligibility, the observed macOS host's IPv6-only candidate subset, bytecode-backed IPv6 selection order, the Linux ARM64 IPv6-only Docker constructor test (IPv4 disabled; container evidence only), runtime ID snapshot, active NameServer-list construction boundary, and three post-construction `namesrvAddr` start mutations (null→configured skips discovery; configured→null fetches once; configured→empty skips discovery), retaining each constructor-time active address list. Windows enumeration, non-macOS BSD enumeration, a real IPv6-only host, and other settings activation paths remain under audit. The blank-local-serve phrase is source-mapped to Proxy server configuration outside the client-only scope. |
| P6-02: own key before construction, one runtime, unrelated-key parallelism, retry and stale failure cleanup | `runtime/manager.rs`; `unrelated_keys_construct_concurrently_and_same_key_constructs_once`, `stale_failed_entry_cannot_remove_a_replacement_entry`, `cancelled_initialization_releases_key_for_retry_without_publishing_resources`; cumulative V01 below; `tests/manager_table.rs` live-map mutation/ownership checks and factory `Map.Entry.setValue` captured-value cases | UNIT_GATE for explicit-manager ownership and the mapped factory table entry operations; facade default manager and ProduceAccumulator activation belong to Phase 7. [ADR-0004](../architecture/ADR-0004-client-runtime-sharing.md) uses the next free number because ADR-0002 was already occupied; factory entry replacement is in [ADR-0079](../architecture/ADR-0079-runtime-map-entry-set-value.md). |
| P6-03: serialized, idempotent or explicit-error state transitions | `runtime/control.rs`, `runtime/runtime.rs`; `tests/lifecycle.rs` concurrent start/stop, shared-user guard, admin-owner shutdown gate and cancellation; `cancelled_terminal_join_keeps_the_owned_handle_for_another_joiner` | UNIT_GATE for Created/Starting/Running/ShuttingDown/Stopped/Failed ownership; admin registration keeps Running and manager identity live until its removal; no detached terminal join handle. |
| P6-04: duplicate groups, identity leases, shared-user independence, no owner cycles | `runtime/registry.rs`, control commands; registration/lifecycle plus registry custom-equality/default-operation/live-view oracle and private atomic/reentry checks | UNIT_GATE: Java map/four consumer method outcomes, getOrDefault/putAll, custom-equality contains/replace/remove and values/entry view operations including mutable equality keys, compute/merge/remapping operations, detached iteration, live key/value/entry view handles with predicate and collection membership mutations, six producer and consumer `Map.Entry.setValue` outcomes each, direct Java registration retention after helper-local references are dropped, 15 invoked admin registration outcomes plus two started-runtime shutdown-gate outcomes (admin presence blocks removal; unregister permits shutdown), producer/consumer/admin owner destructor re-entry checks for lease/group/map/clear removals, weak runtime back-reference and final-release lifecycle check, fresh leases, ordered/cancelled writes and separate unconditional group unregister APIs. Entry handles retain their captured value and still replace the current key mapping; a canceled async write leaves the captured entry unchanged while waiting behind unregister. Iterator-removal effects map to live-view `remove`; detached traversal snapshots are a permitted weak-iterator outcome; [ADR-0009](../architecture/ADR-0009-live-runtime-registry-tables.md) and [ADR-0079](../architecture/ADR-0079-runtime-map-entry-set-value.md). |
| P6-05: coherent raw/Broker/publish/subscribe/static route publication | `route/cache.rs`, runtime refresh and existing common converters; coherent snapshot, null-map failure with prior-cache retention and subsequent recovery, consumer-requested republishing when route data is equal, old/new broker-address lookup visibility across failure and recovery, nullable-value Java golden, static-to-ordinary endpoint retention, mutable raw DTO alias golden, 149 raw table/map-view outcomes including distinct same-key `compute` and `replaceAll` ordering, six null collection arguments to live-view `removeAll`/`retainAll`, three null `removeIf` predicate failures and six null map-callback failures with mapping retention, `Map.Entry.setValue`, nullable key/default behavior across `get`, `getOrDefault`, `containsKey`, `remove`, and nullable value/key arguments across `containsValue`, `put`, `putIfAbsent`, `replace`, conditional mutations, `computeIfAbsent`, `computeIfPresent`, `compute`, and `merge`, 256 actual-runtime combinations of absent/null/empty/populated outer route collections, 625 actual-runtime combinations across absent/null/empty/populated/null-nested states for all four outer route collections, seven registered-consumer cases over route RPC with callback counts including null `queueDatas` plus valid static mapping and null `BrokerData.brokerAddrs` failure, three-refresh producer+consumer case proving prior-route retention after malformed refresh and republishing after recovery, Java `BrokerData.equals/hashCode/compareTo/toString`, `QueueData` methods, `TopicQueueMappingInfo` and `TopicRouteData` equality/hash/string methods, six route-change sort outcomes, unchanged-owner republication, converter/Integer golden tests; runtime failure/serialized-reader tests | UNIT_GATE for [documented route branches](PHASE_6_ROUTE_AUDIT.md), including mutable raw aliases, nullable nested values/lists, outer and nested collection state interactions, producer and consumer callback publication/failure/recovery and consumer-requested republication boundaries, same-key raw-map `compute` blocking and `replaceAll` retry ordering, broker-address lookups across failure/recovery, entry replacement, Java equality/hash/string results, UTF-16 comparison and null-name sort errors; broader malformed input combinations remain open. |
| P6-06: defaults, initial delays, fixed-rate catch-up, cancellation/join, optional persistence | `runtime/tasks.rs`; direct Java `runtimeScheduledTaskCases` invokes `MQClientInstance.startScheduledTask` in four baseline/live cases, recording five/four fixed-rate calls and post-construction null→configured/configured→null plus route/heartbeat/persist interval rereads; `scheduled_task_delays_and_periods_match_invoked_java_registration` compares them. The forked `start()` probe also captures four registrations after configured→empty mutation, and `empty_nameserver_start_schedule_count_matches_java_probe` compares the native count. `defaults_have_java_initial_delays_periods_and_no_ticks_after_stop`, `slow_callback_serializes_jobs_then_catches_up_from_prior_deadlines`, `shutdown_cancels_and_joins_pending_callback`, `offset_persistence_skips_missing_capabilities_and_continues_ticking`; private persistence/adjustment owner tests; constructor/method bytecode confirms single-thread executor and scheduleAtFixedRate runnable wrappers | UNIT_GATE: Java discovery presence and the three changed periods are read at task registration; default delays/periods, catch-up, absent capability skipping, and cancellation/join verified. Actual offset stores/consumer adjustment remain their owning phases. |
| P6-07: current producer/consumer combinations, subscriptions/unregister, V1/V2 | `heartbeat.rs`, registration snapshots; `tests/registrations.rs`, heartbeat and runtime_hooks wire cases, Vec multiplicity and nullable language corrections, U01 below | UNIT_GATE for [documented heartbeat branches](PHASE_6_HEARTBEAT_AUDIT.md), including duplicate consumer metadata, configured null language JSON omission/hook mutation, all four DTO getter/setter default/value/null outcomes, Java mutable Set plus nested DTO alias identity (`7e5b3fa6`, ADR-0074), and direct Java/Rust V1→V2 selection after mutating retained runtime config. Concurrent-heartbeat setting behavior is also exercised: the configured fixed-pool width remains the constructor snapshot; the live enable flag selects the concurrent path; late enabling with no constructed executor sends nothing; and the rebalance wrapper keeps its V1 path serial. Full phase acceptance remains separate. |
| P6-08: publish/admin/subscribe master/slave/fallback/version, pure VIP | `RouteSnapshot` methods and `broker_vip_channel`; address resolution, null/adjacent/fallback, VIP, invoked lookup candidates and runtime lookup peers | UNIT_GATE including actual Java nullable-address decode/refresh/lookup golden and private source branch checks; remaining DTO/input families still require reconciliation. |
| P6-09: inbound slots with explicit inactive errors, group/correlation/channel routing | `runtime/inbound.rs`; two `tests/inbound.rs` peers plus private PGROUP/correlation/consumer/reentry checks; eight owned processor leases | SLOT_UNIT_VERIFIED only. [Inbound activation matrix](PHASE_6_INBOUND_MATRIX.md) names Phases 9/13/14/15/17; no stub is COMPATIBLE, no transaction/reply/admin success is inferred from a mock response. |
| P6-10: every startup effect rolls back, no leaked work/socket, unregister before close | `runtime/control.rs` reverse cleanup, lower shutdown reuse; 47-scenario V03, `tests/lifecycle.rs::cancelled_unregister_finishes_before_replacement_registration_and_last_shutdown_closes_after_unregister`, U01 | UNIT_GATE for current resources/checkpoints. [Lifecycle audit](PHASE_6_LIFECYCLE_AUDIT.md) maps every owned effect and cleanup path; it does not promise stopping arbitrary synchronous blocking user code. |

## Explicit verification IDs

| ID | Current proof and result scope |
| --- | --- |
| P6-V01 | `runtime::control::tests::p6_v01_running_first_lookups_share_identity_task_set_and_actual_socket`: 64 simultaneous first lookups/start/RPCs, one Arc, one periodic/one rebalance/two pull workers, eight processors, one socket and 64 distinct correlated requests, then EOF/zero workers. Manager rendezvous test independently proves different keys progress concurrently. UNIT_GATE verified. |
| P6-V02 | Manager failed-entry/cancellation/replacement tests, first-runtime reuse for same-ID config/hook changes, duplicate/stale group leases, cross-manager live-table metadata/cleanup; latest retry uses source-invalid enabled concurrent-heartbeat pool0 then pool1. UNIT_GATE verified; callback negative is no longer fabricated as an invalid constructor input. |
| P6-V03 | `runtime::control::tests::p6_v03_every_failure_closes_pending_socket_and_drops_owned_work`: 17 explicit/18 discovery injected effects plus 12 real invalid-period cases =47. An actual unanswered lower RPC terminates Shutdown, peer observes EOF, queued future drops once, no worker/control/registration/processor/delayed work remains, prior snapshot Arc survives, stale cleanup cannot remove replacement. Paused periodic and coherent-reader tests complement this. UNIT_GATE verified. |
| P6-U01 | `runtime::control::tests::p6_u01_periodic_subscription_wire_and_unregister_keep_shared_user_running`: explicit 999+1/29999+1ms boundaries, subscription version41→42, producer unregister while consumer stays Running, next heartbeat omits removed producer, consumer/internal unregister precede EOF, zero workers and no late frames after120s. UNIT_GATE verified. |
| P6-V04 | Registered in `tests/differential/scenarios.yaml` with original ID, phase6/execution_phase19, shared producers/consumer and all three required checks. Comparator self-test first failed the missing ID, then validates retention and pending status. **PENDING_PHASE_19**, never PASS. The future live runner/environment belongs to Phase 19; no Docker/Broker script is required for Phase 6. |

## Named classes and bidirectional mapping

| Pinned Java class | Current native mapping / remaining reconciliation |
| --- | --- |
| `NettyRemotingClient.getHostAndPort` / `channelTables` | The direct 5.5.1 address oracle covers IPv4, hostname, bracketed/unbracketed IPv6, numeric and named scoped IPv6, 49 explicit endpoint splits, and every BMP code unit inserted into a non-bracketed host; Java preserves all units, while Rust compares all representable BMP scalars. A separate direct Java oracle under the recorded Maven runtime (Java 27, Homebrew) enumerates all 65,536 valid socket ports and every BMP UTF-16 code unit passed to `Integer.parseInt` (370 accepted decimal digits), matched by exhaustive native tests. A reflected table probe plus `getAndCreateChannelAsync` bytecode confirms exact raw address strings are connection keys. Native `Address::parse` validates but preserves the key; `connection::split_address` parses the dial target separately, and empty host maps to Java's IPv4 loopback. Numeric scopes and conventional interface-name scopes remain supported; the named-scope Java cases verify unresolved endpoint construction, not live interface resolution. |
| `MQClientManager` | Explicit shareable manager, creation/removal overloads/live FactoryTable; [API map](PHASE_6_API_CONFIG_MAP.md). Producer accumulator is Phase 7. Source map and native ownership details are in ADR-0006. |
| `MQClientInstance` | Runtime/control/route/heartbeat/discovery/check-client/lookup methods and owned optional capabilities; API map plus lifecycle/route/heartbeat/inbound audits. All 53 declared public method/constructor signatures now have a named API-map row, including exact names for overloaded heartbeat and broker-address lookup methods. Returned topic/producer/consumer registries are mapped as live handles with source collection-view operations in ADR-0009/0010. Unsupported/malformed inputs and source-branch/reverse-export behavior still need method-by-method reconciliation. |
| `MessageQueue` (common value used by Phase 6 route/protocol APIs) | `rocketmq-classic-common::message::MessageQueue`; constructors, getters/setters, equality/hash/text and both `compareTo` declarations are mapped with direct Java method fixtures and native differentials in the [API map](PHASE_6_API_CONFIG_MAP.md). Other malformed DTO and whole-source reconciliation remains separate. |
| `ClientConfig` | Exhaustive field/property/copy/default/reverse map; nullable AccessChannel/SOCKS, empty SOCKS, nonpositive callback values and nullable language have invoked evidence. ADR-0007 records the completed null-language correction. |
| `TopicPublishInfo` | Phase 6 uses `TopicSnapshot::{route,publish_queues,order_topic}` and current common converters for route-derived publication. All 18 pinned public declarations, including its constructor and overloaded methods, are assigned to route publication in Phase 6 or producer-local state/selection in Phase 7; the API map lists every getter/setter, all three selection overloads, `QueueFilter`, `ThreadLocalIndex`, and diagnostics. This ownership map does not prove producer behavior, which remains Phase 7 work. |
| `ClientRemotingProcessor` | Constructor and five public methods are individually mapped by exact name in the [API map](PHASE_6_API_CONFIG_MAP.md): `processRequest`, `checkTransactionState`, `notifyConsumerIdsChanged`, `resetOffset`, and `getConsumeStatus`. Code 40 wakes the whole runtime rebalance service and returns null. Transaction handling, Push reset/status/running-info/direct-consume, and reply correlation are activated by their owning phases; remaining inactive slots retain their source routing metadata and explicit phase errors. Matrix distinguishes registered-but-undispatched lite unsubscribe. |
| `RebalanceService` | One owned wakeup worker, source properties/signed-long parser, throttled sticky signal and serial delayed wakeup; `getServiceName()` maps to `REBALANCE_SERVICE_NAME` with an actual Java method case. Real allocation/assignment is Phase 11, not a synthetic Phase 6 consumer scheduler. |
| `PullMessageService` | Owned FIFO plus independent serial delayed worker, current-owner lookup, optional pull/pop capabilities and source1000ms graceful shutdown; `getServiceName()` maps to `PULL_MESSAGE_SERVICE_NAME` with an actual Java method case. Concrete requests/process queues/fetching belong to consumer phases. |

Reverse exports and field/property use are listed in the API map. They are inspected mappings, not certification that every method/overload, nullable DTO or inherited/deprecated surface has final acceptance. Before closing Phase 6, unresolved Phase 6 public boundaries must be resolved; later owning-phase operations must remain explicit, never success stubs.

Pinned declaration audit (Java 5.5.1 `javap -public`, constructors, overloads and public constants): a fresh scan against the pinned client/common/remoting 5.5.1 jars confirms `MQClientInstance` 53 declarations (49 distinct names), `MQClientManager` 7 (5 names), `ClientConfig` 78 signatures plus seven constants (85 public entries), `TopicPublishInfo` 18 (16 names), `RebalanceService` 3, `PullMessageService` 11, and `ClientRemotingProcessor` one constructor plus five public methods. A declaration-name comparison found abbreviated labels for `sendHeartbeatToAllBrokerWithLockV2` and `findBrokerAddressInSubscribe`; both API-map rows now spell out their exact names and overload parameters. The `TopicPublishInfo` owner map now names every getter/setter, selection overload, `QueueFilter`, `ThreadLocalIndex`, and diagnostic method assigned to Phase 7. All declared public method names are represented in the compatibility maps. `ClientRemotingProcessor`'s exact handler names/status and `MQClientInstance`'s five anonymous channel-listener callbacks are mapped separately. The inherited public `ServiceThread` API remains mapped in [ADR-0026](../architecture/ADR-0026-service-thread-api-boundary.md), including started lifecycle probes and JDK thread controls with no direct Rust equivalent. This closes declaration-name coverage only; it does not verify every signature's inputs or source branch, so the broader acceptance status stays PARTIAL.

Native runtime reverse-export audit: walked `runtime/mod.rs` reexports and the public methods of `ClientRuntime`, `ClientRuntimeManager`, `FactoryTable`/its live views, registry tables, `GroupLease`, `MessageRequest`, and the producer/consumer capability traits against the API/config map. This found two native method names that were only described generically: `ClientRuntime::execute_pull_request_later`/`execute_pop_pull_request_later` and `FactoryTable::compute_if_absent`/`compute_if_present`. Their Java-facing ownership is now explicit in the worker and map rows. Internal `pub(crate)`/`pub(super)` helpers and methods on private `ManagerLink`, `GroupRegistry`, and `PullService` types are not externally reachable exports. The reverse-name check is clean; this does not settle the documented source-input branches, Rust error-shape differences, or the full acceptance audit.

## Commands and current provenance

Actual results inspected during this audit:

- Endpoint parsing follow-up (2026-10-08): `NettyRemotingClient.getHostAndPort` is called directly for thirteen fixture inputs, including raw and bracketed named IPv6 interface scopes. Native parsing accepts port zero, unbracketed IPv6 and numeric or named scopes; it rejects port values outside Java's unresolved socket-address range. Focused `address_splitting_matches_pinned_java_last_colon_oracle` and the local interface resolver test passed. Full `cargo test --workspace --all-targets --locked` passed with loopback access, strict all-target/all-feature Clippy passed, pinned Maven verify exited 0 with the updated address fixture and all existing runtime/frame/body checks, comparator self-test and manifest22 validation passed, and `cargo fmt --all -- --check` plus `git diff --check` passed. Fixture SHA-256 is `1c1e768c5444be6671b526f7e8754c7f23fb2ba610e1a848b00e814ff5e475bc`. This closes the observed endpoint split/port/scope-name gaps; other address/transport variants and full Phase 6 reconciliation remain open, and P6-V04 remains PENDING_PHASE_19.

- Earlier 2026-10-08 run after the 109-case SOCKS grammar matrix, local connection oracle, and ClientConfig copy-field oracle: `cargo test --workspace --all-targets --locked` exit0 (with loopback permission); `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` exit0; `mvn -o -f compat/java-reference/pom.xml verify` BUILD SUCCESS with route25, ClientConfig identity/default and manifest22; differential self-test and manifest validation22 pass; `cargo fmt --all --check` and `git diff --check` pass. The pinned Java constructor fixture covers 109 cases and `copyFields` records 33 non-internal settings on source, clone and reset at SHA-256 `8a7a34c2268f78d46e9b7b909015db4217156d6040ee6b3908b31caa7e5c485a`; native differentials pass constructor results, reflected values and all field snapshots. Both Java `socksConnection` records report successful authenticated/no-auth SOCKS5, matched local target, request/response forwarding, code/body and client-returned opaque correlation. The run also covers the unique TLS test-directory fix (process ID plus atomic sequence) and paired SOCKS transport tests. The first sandboxed loopback failures were resolved by rerunning with local socket access.

- `cargo test -p rocketmq-classic-core --lib --test config --test runtime --locked`: core69/config11/runtime7, exit0; `/tmp/p6-callback-threads-focused.log`. This includes the plan's runtime target.
- `cargo test --workspace --locked`: exit0, named V01/V02/V03/U01, registration/lifecycle/route tests present in output; `/tmp/p6-callback-threads-workspace.log`.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: exit0, `/tmp/p6-requirement-audit-clippy-all-features.log`. Earlier latest all-targets-only logs are not substituted for this exact index gate.
- `mvn -o -q -f compat/java-reference/pom.xml verify`: exit0, `/tmp/p6-callback-threads-java-verify.log`; offline artifact/drift and Java decoding, no server execution.
- `python3 tests/differential/compare.py --self-test` and `--validate-manifest`: pass;22 scenarios after P6-V04 registration. `cargo fmt --all --check` and `git diff --check`: pass.

Historical audit snapshot: fixture hashes listed here and the then-current core count69pass/1review-ignored predate the later nullable Broker DTO correction and live route-table/view implementation. At that point the ignored null-value golden failed and the native live table was missing. ADR-0008 subsequently enabled and passed the nullable test; `dae27fb`/`0a03138` implemented the live raw table and collection-view effects, now covered by the59-outcome route oracle above. Other fixture families retain their individual provenance in the inventories/harness. Full source/input and final-plan reconciliation remain open.

The implementation remains on `07-phase-06-runtime-routing-heartbeat`, with separate probe/test, implementation, and evidence commits. The append-only [latest execution log](../../rlogs/2026/09/07-phase-06-runtime-routing-heartbeat__20261010T015552Z.rlog) records the consumer notification source correction, validation gates and pending work. No Java checkout, live server or deployment state was modified.

## Blank local serve address: source mapping resolved

The child plan's “blank local serve address treated as unset” comes from upstream commit `00e45b8a6db23efbe756d0306f10716156cfd4dd`, issue10025/PR10663, titled “Treat blank local serve address as unset.” Its actual diff changes **only Proxy server configuration and its test**, not the client or remoting client:

- `proxy/src/main/java/org/apache/rocketmq/proxy/config/ProxyConfig.java::initData`: changes `StringUtils.isEmpty(localServeAddr)` to `StringUtils.isBlank(localServeAddr)`; blank uses `NetworkUtil.getLocalAddress()`, still-blank throws ProxyException, blank remotingAccessAddr then inherits it.
- `proxy/src/test/java/org/apache/rocketmq/proxy/config/ProxyConfigTest.java::initDataShouldResolveBlankLocalServeAddr`: assigns whitespace localServeAddr/remotingAccessAddr, calls initData, asserts resolved nonblank address and inheritance. This upstream test was read, **not executed or claimed as a client oracle**.

Read-only `git merge-base --is-ancestor` returned0 for that fix against the pinned 5.5.1 commit. The pinned Proxy source retains the exact branch and test. Exact-word `git grep -w -e ProxyConfig -e localServeAddr` across pinned client/common/remoting returned no matches (exit1); this corroborates the inspected change, rather than substituting a failed name search for source evidence.

The master/child execution contract explicitly implements the client only and excludes Proxy/server implementation. Therefore this is **SOURCE_MAPPING_RESOLVED / PROXY_SERVER_OUT_OF_SCOPE**, not a missing Rust ClientConfig field or a waived client capability. Do not add a localServeAddr setting, a Proxy dependency, or a client-IP rewrite. Actual client identity fixtures intentionally retain explicitly empty client IP; those semantics are separate and unchanged. The blank-address child plan is not modified for this server-only finding. Full client API/settings acceptance still depends on the remaining work below.

## Remaining work preventing acceptance

Consume-result and MessageQueue DTO update (2026-10-09): pinned Java input oracles now exercise all ConsumeMessageResult values, coercion/rejection cases, and MessageQueue scalar/numeric boundary behavior. The ConsumeMessageDirectlyResult enum decoder was corrected; MessageQueue required no production change across its 16 cases. Lookup fixture SHA-256 910e5dd57337348c1ff9c5b466a0240ab57763cc3d10b25749e87ccd22c29dd1. Workspace, strict Clippy, Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. These cases do not complete full source/API/input reconciliation.

Assignment request DTO update (2026-10-09): pinned Java oracles cover messageModel input on QueryAssignmentRequestBody and nullable mode/default behavior on SetMessageRequestModeRequestBody. Rust now matches all 20 accepted/rejected outcomes and exposes the Java-nullable mode as Option<MessageRequestMode>. Lookup fixture SHA-256 baac7eea9e0073bd3e22fc9e9b230b77565560500f967795406ab019e771e05f. Workspace, strict Clippy, Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. This closes only these DTO inputs; complete source/API/input reconciliation remains open.

Lite subscription DTO update (2026-10-09): direct Java Fastjson probes exposed missing primitive defaults, numeric enum ordinal decoding, and null enum omission on encoding. The Rust DTOs now match these LiteSubscriptionAction and OffsetOption.Type cases; all 13 Java/Rust inputs pass. Lookup fixture SHA-256 d653604bc9ece695d09aafdc5685598d9c278c612c7af9c7642c1f9924c010a4. Workspace tests, strict Clippy, pinned Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. This closes only these DTO cases; full API/source/input reconciliation remains open.

Consumer enum input update (2026-10-09): the pinned Java oracle now covers 65 ConsumerData enum inputs, including integer-string ordinals, invalid numeric strings, all three enum fields, plain decimal precision beyond f64, exponent extremes, nulls and invalid JSON types. The new "1" probe failed in Rust before the raw-token parser update; all 65 cases now pass. Fixture SHA-256 6b2ca3ef46594d0f1dbf63587baab0c70a552dd777672b147acb250fd6b9b146. Workspace, strict Clippy, Maven verify (22 scenarios), comparator/manifest22, fmt and diff checks pass. This closes these enum input cases only; broader enum/DTO, Fastjson, transport and whole-phase reconciliation remain open.

Nullable command language correction `40bd7cf`/`63b7feb` closes the confirmed config/command mismatch: actual Java omitted versus explicit-null decode and binary failure are represented by public Option fields; config clone/reset/runtime and scripted hook/wire checks pass. Full Rust workspace, exact all-feature Clippy, pinned Maven, fmt/diff and comparator/manifest exit0. [ADR-0007](../architecture/ADR-0007-nullable-command-language.md) records scope. Earlier missing-language statements below are historical. Both confirmed nullable lower-model mismatches are now corrected, but full source/API/input reconciliation and final phase audit remain open.

Nullable Broker correction `8fb1ec8` closes the confirmed address-value mismatch. The prior ignored red golden is enabled and passes; public `BrokerData`/`RouteSnapshot` preserve null values, and all route/heartbeat/unregister call sites were adapted. Protocol, lookup, cleanup and scripted peer tests plus full workspace/exact Clippy/Maven/fmt/diff/comparator passed. [ADR-0008](../architecture/ADR-0008-nullable-broker-address-values.md) records scope. Historical `1ignored` counts below predate this correction. The nullable command-language and Broker-value gaps are closed; broad API/source/input reconciliation remains open.

SOCKS constructor input update `0c61f34`/`7899cea` closes six new source-confirmed successful inputs (single quotes, trailing comma, comment, unknown field, missing address, null route) and one source-confirmed rejection (unquoted CIDR key). The client-config fixture SHA-256 at that update was `27caa16b8d8701c5a10153f2592ca3ffffaef72482957b80e433a424e10be7e2`; actual Java construction preserves raw settings and native focused differential passes all19 cases. Private matching route tests distinguish direct null from deferred missing-address error. Full workspace, exact Clippy, fmt/diff, comparator/manifest and Maven verify exit0. Other Fastjson grammar, connection paths and complete source/API/input reconciliation remain open; the nullable command-language and Broker-value corrections are implemented.

Null raw-map cleanup update `75c2729` closes that bounded branch, not a hypothetical production error: pinned helper null guard, two real runtime cases and native golden all skip null maps, remove historical addresses and retain raw data. Route hash at that stage was `a1e55afbdda8db505efe05f13eb20687a21aa88ecd108bcfc1df6676a20ef558`; reviewed core74pass/1ignored/runtime7/topicTable3/workspace/exact Clippy/Maven exit0 (`/tmp/p6-raw-offline-cleanup-*.log` reviewed cargo/Java verification variants), fmt/diff/comparator/manifest22 pass. Other malformed DTO/null-list input, mutable getter and broad source-input reconciliation remain open; nullable lower-model corrections are implemented.

Raw table counterpart `dae27fb` closes tested getter/primitive mapping, independent raw/derived state, content-conditional comparisons, cached/refetched queries, offline cleanup for valid routes and early unchanged invalid refresh. Oracle `fe71e82` corrects stale counter bookkeeping and adds the invalid unchanged branch. Route hash at that stage was `dbea24db04ab83de918333ec35e4356ee6e610523051eeb77f7d4d477c10e0e3`; core73pass/1ignored/topicTable3/runtime7/workspace/exact Clippy/reviewed Maven exit0, fmt/diff/comparator/manifest22 pass. The collection-view update `0a03138` later adds key/value/entry membership and bulk mutations; latest status and hash are in the opening audit entries. At that stage weakly consistent iteration, mutable DTO alias behavior, malformed raw cleanup/input and source-input reconciliation remained open; the later source-contract audit maps the iteration snapshots to a permitted weak-iterator outcome. ADR0010 does not claim full Phase6 acceptance.

Historical route evidence/fix (`3f71c78`/`9241c6e`) corrected private metadata-only change comparison and added the live raw map oracle before the native API was implemented. Route hash at that stage was `c88c250c9303128b818f2d5f1e7a2a0b6cf3da4a8691bc70564b842566add615`; core72pass/1lower-review-ignored/runtime7/full workspace/exact Clippy/reviewed Java drift exit0, fmt/diff/comparator/manifest22 pass. Later commits `dae27fb` and `0a03138` implement and test the raw table/view effects. Remaining route and Phase 6 source/input work is listed in the current entries above; the nullable lower-model corrections are complete.

Group-unregister update `3ccaf54` closes the separately identified producer/consumer unconditional removal capability, not arbitrary map/input or whole-phase acceptance. Four source consumer method outcomes now match. Scripted peer verifies missing-key RPC, rejection, producer/consumer headers, queued cancellation, weak-expired bindings, ordering and terminal no-revival. Final workspace71pass/1lower-review-ignored in core, registry4/runtime7, exact all-feature Clippy and offline Maven exit0 (`/tmp/p6-group-unregister-workspace-final.log`, `-clippy-final.log`, `-java-verify.log`); fmt/diff/comparator/manifest22 pass. Supersedes the following historical statement that group unregister is missing. Nullable lower-model fixes are complete; full source/input audit remains open.

Historical native table update `bf4a859`: workspace core71pass/1lower-review-ignored, registry3/runtime7, exact all-target/all-feature Clippy and Maven exit0 (`/tmp/p6-live-registry-{workspace,clippy,java-verify}-final.log`); format/diff/comparator/manifest22 pass. At that point an ignored nullable-Broker failure and missing group-unregister operation remained. Later ADR-0008 and `3ccaf54` close those gaps; remaining full source-input/map-surface reconciliation is still required.

1. Close full API/source/input reconciliation rather than treating documented typed bounds as exclusions: route DTO inputs now have 399 direct cases across scalar coercions, every field's seven token kinds, nested collection members, quoted and unquoted numeric keys across map types, Fastjson grammar, duplicate fields, numeric-key collisions, and root JSON shapes. Heartbeat DTO inputs have 276 cases; the direct Lite subscription DTO matrix has 114; lock/unlock batch inputs now have 46, 46, and 25 cases for the request and response bodies; and the check-client, consumer-list, and assignment-response matrices have 43, 25, and 37 cases respectively, including root shapes. Assignment request inputs cover every field token shape and root shape on both runtime request DTOs (52 and 49 cases), plus direct public-method oracles for `QueryAssignmentRequestBody`, `QueryAssignmentResponseBody`, and `SetMessageRequestModeRequestBody`. The `GetConsumerStatusBody`, `ResetOffsetBody`, and `MessageRequestModeSerializeWrapper` matrices contain 143, 77, and 33 direct Java input cases; `BatchAckMessageRequestBody` has 111 direct Java input cases plus public constructor/getter/setter/`toString()` oracles for BatchAck and twelve runtime DTOs, including the four heartbeat DTOs. Current `lookup.json` SHA-256 is `85c4496ce6e9055da78b4f7624e6713115b90d8c4e7beea60a332c810b5e5bf2`. `BatchAckMessageRequestBody.acks`, each contained `BatchAck` object, and each `BatchAck.bitSet` object now preserve caller/setter/getter identity through nested `SharedValue` handles (`7ba69080`); the GetConsumerStatusBody and ResetOffsetBody outer and nested Map aliases now use shared handles (`2f07fe94`, ADR-0073); the heartbeat external setter/getter identity gap is closed by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md). `LiteSubscriptionCtlRequestBody.subscriptionSet` and each supplied DTO element now preserve caller/setter/getter identity (`66adb0c4`, ADR-0071). `LiteSubscriptionDTO.liteTopicSet/offsetOption` now preserve caller/setter/getter identity (`60251cee`, ADR-0070). `LiteSubscription.liteTopicSet` now preserves stable getter identity while retaining its copy-and-add setter (`e33c43b2`, ADR-0072). For the three lock request/response Set containers, `SharedValue` and `ptr_eq` preserve caller/getter alias identity and both mutation directions, and nested shared queue handles preserve each mutable `MessageQueue` element identity in both directions (`625122ee`, ADR-0067). Value-based queue `Eq`/`Hash` retains Java Set membership and its mutable-key hash hazard. For `CheckClientRequestBody.subscriptionData`, `SharedValue` and `ptr_eq` now match setter/getter alias identity and both mutation directions; for `GetConsumerListByGroupResponseBody.consumerIdList`, `SharedValue` and `ptr_eq` preserve caller/getter identity and both mutation directions; for `BatchAck.bitSet` through shared getter handles, `MessageQueueAssignment.attachments/message_queue` through shared handles that preserve caller/getter identity and both mutation directions, `QueryAssignmentResponseBody.messageQueueAssignments` outer Set through shared getter handles, both lock request DTOs, and `LockBatchResponseBody.lockOKMQSet`, `GetConsumerStatusBody.messageQueueTable/consumerTable` and `ResetOffsetBody.offsetTable` now preserve outer and nested Map handles (`2f07fe94`, ADR-0073); `MessageRequestModeSerializeWrapper.messageRequestModeMap` outer Map, nested group Maps, and row DTO values now use shared handles (`853b3fdd`, ADR-0069). `SubscriptionData.tagsSet/codeSet`, `ConsumerData.subscriptionDataSet`, and `HeartbeatData.producerDataSet/consumerDataSet` mutable getters now expose and pass mutation-through-getter cases. The Lite control-body Set and its DTO elements now use shared handles (`66adb0c4`, ADR-0071). `LiteSubscriptionDTO.liteTopicSet/offsetOption` additionally preserve the caller aliases under `60251cee` / ADR-0070. For heartbeat collection fields, Java caller/setter/getter Set identity and the relevant nested DTO identities are now represented by shared handles (`7e5b3fa6`, [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md)); the QueryAssignment response Set container and each assignment element now use shared handles (`d312bb6c`, ADR-0068). The public borrow-returning collection and nested-object getters found in the protocol body, heartbeat, and route modules now have matching mutable accessors, with direct Java mutation cases for the alias-bearing DTO surfaces reviewed here. This sweep does not close other DTO fields/APIs, malformed inputs, or whole-source/reverse-export reconciliation. The Java `LiteSubscription` field/getter/setter surface is now mapped: pinned `javap` signatures match the Rust methods, the direct method oracle passes, and all twelve focused Lite protocol/API tests pass. Other protocol DTO/enum combinations and malformed DTO/raw inputs remain open. Fastjson SOCKS currently has 499 constructor cases and 32 actual private selection cases covering resolved, chained, unresolved, cyclic, root, dot, quoted-bracket, whitespace, bare-name, relative, and selected failure references; other Fastjson paths and grammar remain open despite exhaustive leading/internal BMP scans, all representable BMP and every supplementary scalar escape follower. Address/transport evidence includes 49 endpoint shapes, the complete `0..=65535` socket-port range, all 370 BMP decimal digits, every Java UTF-16 host code unit, and every Unicode scalar host value; Linux production enumeration has passed the Linux ARM64 loopback-scope runtime test, and the IPv6-only Docker resolver test passes with Docker IPv4 disabled; Windows IP Helper runtime enumeration, non-macOS BSD runtime enumeration, and an actual IPv6-only host remain unverified. Windows address mapping and helper/type-check evidence do not substitute for those runtime checks. Remaining source families require authoritative source/oracle evidence and correction or explicit ownership mapping. The raw route cleanup matrix covers 21 input shapes, including absent/default fields, malformed collections, both null-address-map orderings, a null broker ID whose live address retains the master and removes a stale slave, immediate empty-row removal before a later traversal failure, cross-name/ID and sibling-topic address retention, historical Broker version retention, and a null broker list; nonempty row updates remain deferred on traversal failure, as covered by the existing retained-candidate failure case. Other raw/DTO combinations remain open. One address-family boundary is assigned from pinned source call sites: reply-header `NetworkUtil.string2SocketAddress` behavior belongs to Phase 15, MessageId `socketAddress2String` formatting belongs to Phase 17, and `convert2IpString` has no `rocketmq-client` call site. Phase 6's inbound reply slot remains routing-only. `TopicPublishInfo` route conversion/publication is owned by Phase 6, while producer-local route-presence fallback, queue filters/selection/index state, and broker-specific write-queue behavior are assigned to Phase 7; route presence remains distinct from an empty publish queue list. Authenticated and no-auth Java SOCKS5 remoting paths, mutable route aliases, registry owner equality keys, and iterator-removal live-view effects have matching scoped evidence. The current general matrix stays PARTIAL.
2. Re-read the complete English/Traditional child plans, master Phase 6 section, and paired execution indexes on 2026-10-09. Fresh workspace tests, strict Clippy, pinned Maven verify, comparator/manifest22, format and diff gates pass for the latest changes. Final method/field/branch/reverse-export reconciliation and the completed handoff remain open until item 1 is resolved. Leave P6-V04 `PENDING_PHASE_19`; this audit is a gap report, not a waiver or completion claim.
3. macOS address enumeration was rechecked on Darwin 25.6.0 arm64 (2026-10-10) against the pinned `rocketmq-common-5.5.1.jar` (SHA-256 `6a15b581269c0e06d9508d284c84c05a8578ffa1b16db3e60ce837c97f684b97`). A live call to `NetworkUtil.getLocalInetAddressList()` returned nine candidates, exactly matching the captured Java candidate list; `cargo test -p rocketmq-classic-core --locked local_address::tests::` passed all eight focused tests, including the macOS native-enumeration/order/selection comparison. This closes the macOS host check only. The Linux IPv6-only Docker resolver check passes; Windows runtime enumeration, non-macOS Unix enumeration, and an actual IPv6-only host remain unverified; full source/API/input/reverse-export reconciliation remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.
4. The pinned `rocketmq-client-5.5.1.jar` bytecode shows that `MQClientInstance` creates the concurrent heartbeat fixed pool only when `enableConcurrentHeartbeat` is true at construction, using the then-current pool size; each `sendHeartbeatToAllBrokerWithLock` call re-reads the mutable enable flag. Rust coverage passes for parallel two-peer delivery, constructor-snapshot pool width, busy-lock skipping, serial V1 wrapper delivery, nonpositive enabled pool rejection/retry, and late enabling after construction without an executor (which sends no peer request). This closes those concurrent-heartbeat setting branches only; other setting activation and full source/API/input reconciliation remain under audit; P6-V04 remains `PENDING_PHASE_19`.
5. Check-client VIP activation (`2026-10-10`): pinned `MQClientAPIImpl.checkClientInBroker` bytecode reads `clientConfig.isVipChannelEnabled()` at each API call. The new actual Java runtime/API fixture constructs with VIP disabled, invokes `checkClientInBroker`, mutates the retained config to enabled, and invokes it again; captured addresses change from `127.0.0.1:10911` to `127.0.0.1:10909`. The Rust loopback differential retains the consumer owner, makes both requests over their corresponding ports, and verifies the same request body. No production correction was needed. `client-check.json` SHA-256 `13995e39a081b83f763d5437370f64fa5ba93027464a3c32d1903de98b9656f9`. Focused client-check tests 6/6, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, formatting and diff checks pass. Other setting activation and whole source/API/input/platform reconciliation remain open; P6-V04 remains `PENDING_PHASE_19`.
6. Windows address enumeration compile audit (`2026-10-10`): the full `cargo check -p rocketmq-classic-core --all-targets --target x86_64-pc-windows-gnu --locked` could not reach Rust checking because `aws-lc-sys` needs `x86_64-w64-mingw32-gcc`, which is absent. To isolate the platform code, a temporary crate under `/private/tmp` referenced the repository's exact `config/local_address.rs` and ran `cargo check --tests --target x86_64-pc-windows-gnu --offline`; Windows production enumeration and its Windows-gated helpers/tests type-check successfully. Logs: `/private/tmp/p6-windows-core-check.log` and `/private/tmp/p6-windows-address-module-check.log`. This is compile evidence only: Windows IP Helper runtime enumeration, non-macOS Unix enumeration, and a real IPv6-only host remain unverified; full Phase 6 reconciliation remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.
7. Windows socket-address conversion unit (`b77b9719`, extended by `e2795a8a`, `2026-10-10`): the repository's `windows_address` adapter is host-testable and a unit test constructs actual `windows-sys` `SOCKADDR_INET` IPv4/IPv6 members. It verifies IPv4 network-byte preservation, IPv6 octets, zero and nonzero scope/interface-name mapping, and rejection of an unknown family. The focused test and all 17 `local_address` tests pass on macOS; an isolated Windows-target check compiles the repository module and tests. Full workspace/all-target tests, workspace all-target/all-feature strict Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting and diff checks also pass; the workspace and Maven commands needed approval to access local loopback and create a Maven cache lock file. This exercises the pure structure conversion only and is not Java runtime or Windows IP Helper evidence. Windows runtime enumeration, non-macOS Unix enumeration, and an actual IPv6-only host remain unverified; full Phase 6 reconciliation remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.
8. Pull and rebalance phase-owner reconciliation (`2026-10-10`): corrected a stale inbound-matrix sentence that assigned consumer allocation to Phase 9 and called pull-request ownership open. The pinned plans assign explicit/manual pull RPCs and offset stores to Phase 9, Push `PullRequest`/`PopRequest`, `ProcessQueue` and bounded fetch/consume scheduling to Phase 10, and assignment/initial-offset activation to Phase 11. Phase 6's verified surface remains the shared PullMessageService FIFO/delayed worker and current-owner dispatch boundary, plus runtime-wide code-40 wakeup. The inbound matrix, runtime inventory and API map now state the same owner split. This closes an owner-mapping ambiguity only; downstream implementation gates and broad Phase 6 source/API/input/reverse-export reconciliation remain open, and P6-V04 remains `PENDING_PHASE_19`.
9. Public declaration/overload inventory (`2026-10-10`): re-ran `javap -public` on the pinned 5.5.1 client/common/remoting jars for `MQClientInstance`, `MQClientManager`, `ClientConfig`, `ClientRemotingProcessor`, `TopicPublishInfo`, `RebalanceService`, `PullMessageService`, and inherited `ServiceThread`. The 187 declared signatures and their overload groups match the corresponding API/config map rows, and every declaration name is present. Output: `/private/tmp/p6-javap-public-signatures.txt`. This closes declaration inventory at the signature/overload-count level only; it does not verify every input, source branch, Rust ownership adaptation or whole-phase reverse export. The remaining Phase 6 reconciliation stays PARTIAL and P6-V04 remains `PENDING_PHASE_19`.
10. ClientConfig reverse-field check (`2026-10-10`): a source scan compared the 33 public `ClientConfig` fields in `crates/rocketmq-classic-core/src/config.rs` with the Rust-field column in the API map. The sets match exactly: no unmapped public Rust setting and no public map row without a Rust field. The map's extra `namespace_initialized` row corresponds to Java's private `namespaceInitialized` cache and is not a public setting. Constructor/property behavior and all 33 Java accessor pairs remain covered by their named fixture and tests. This closes the reverse field-inventory check only; activation/source-branch reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.
11. Runtime and route public-method reverse scan (`2026-10-10`): compared public methods on `ClientRuntime`, manager/table views, registry owners, route snapshots, and route-table handles against the API/config map. Added the exact Rust names for nullable route address, consumer lookup, and cached/query route adapters; distinguished `ClientRuntime` admin `GroupLease` lifecycle helpers from the direct Java-style `register_admin_ext` owner-table adapters; and mapped `TopicRouteDataHandle::downgrade` / `WeakTopicRouteDataHandle::upgrade` as Rust Arc/Weak lifetime utilities used by retention tests, not route operations. The remaining unlisted `ManagerLink::same_table` and `GroupRegistry::has_active_users` are methods on internal types (`ManagerLink` is `pub(super)` and `GroupRegistry` is not publicly re-exported); `GroupRegistry::register_admin` is likewise internal, while the matching map row names the public `ClientRuntime::register_admin`. This closes a bounded reverse-name scan only; all-input/source-branch reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.
12. Linux IPv6 loopback parser oracle (`2026-10-10`): the pinned RocketMQ 5.5.1 `NetworkUtil.getLocalInetAddressList()` ran under Linux/OpenJDK 8 and reported `lo ::1%lo`, scope ID 1; the captured address row is present in `/proc/net/if_inet6`. The new Java fixture has a Linux-only Maven verify profile and its `--verify` mode passed in the Linux container; the Rust `parse_ipv6_row`/normalizer unit matches the same row. This adds one Linux parser case, not Rust Linux production enumeration evidence. Linux and Windows runtime enumeration, BSD target execution, real IPv6-only behavior, full Phase 6 reconciliation and P6-V04 remain open.
13. Nullable route-view predicate boundary (`2026-10-10`): pinned Java `ConcurrentHashMap.keySet()`, `values()`, and `entrySet()` each throw `NullPointerException` for `removeIf(null)` before traversal or mutation. Rust adds `remove_if_nullable` to the corresponding three views; `None` maps to a typed `InvalidRoute`, and the differential confirms the route remains. This raises the primitive/view oracle from 138 to 142 outcomes and `route.json` SHA-256 is `99c9efe5783c7b7d91f20d762b16e869a0e93f819315e8a90a8cb01d2acc7103`. It closes this input branch only; broad Phase 6 reconciliation remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.
14. Nullable raw-map callback arguments (`2026-10-10`): the pinned Java 5.5.1 `ConcurrentHashMap` throws before mutation when `forEach`, `replaceAll`, `computeIfAbsent`, `computeIfPresent`, `compute`, or `merge` receives a null callback. Rust adds typed nullable adapters and direct Java/Rust differential outcomes for all six NPE branches plus retained mapping. The route-table oracle rises from 142 to 149; current fixture SHA-256 is `2be7e35eea6de3a0b16eb04d18eb39e579dd8ad359002760c12e6bca0bbb84e9`. Other callback/input combinations and whole Phase 6 reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.
15. Native runtime export-name reconciliation (`2026-10-10`): scanned the public `pub use` symbols and callback aliases from `runtime/mod.rs` and `route/mod.rs` against the API map. The first pass found nine exported names that were described only generically: all six concrete producer/consumer key/value/entry view wrappers, plus `TableForEach`, `TableReplaceAll`, and `TableReplaceAllOptionalResult`. The API map now lists those exact Rust names and maps each view to Java `ConcurrentMap.keySet()`/`values()`/`entrySet()` and each callback alias to its `Map`/`Collection` function contract. This closes the bounded export-name inventory only; method/input/source-branch and whole Phase 6 reconciliation remain PARTIAL, with P6-V04 `PENDING_PHASE_19`.
16. Exported nullable registry entry-view methods (`2026-10-10`): a reverse scan of public methods found `contains_entry_nullable` and `remove_entry_nullable` on the exported Factory/producer/consumer entry-set views were described only as generic nullable membership/removal. The API map now names all six methods and maps independently null entry keys/values to Java `ConcurrentHashMap.entrySet().contains/remove` returning false. The scan's `stop_accepting`/rebalance matches are on non-exported internal types. This closes method-name documentation for this bounded entry-view surface only; whole Phase 6 method/input/source reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.
17. Public ClientConfig error result mapping (`2026-10-10`): added exact documentation for the four public `ConfigError` variants and their source Java exceptions/short-circuit behavior. `LocalAddress` had no call sites and was removed; automatic Java address discovery catches failures and retains null. A fixture-backed `changeInstanceNameToPID(null)` Java NPE now compares with Rust `ConfigError::NullInstanceName`. Focused Rust test and full pinned offline Maven verify pass. This closes the bounded result-carrier/export inventory only; whole Phase 6 input/source/platform reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.
18. Runtime/route/config declaration reverse scan (`2026-10-10`): scanned all 16 `runtime/*.rs` and `route/*.rs` source files plus `config.rs` (17 files total), comparing 562 `pub` declarations against exact API-map names. The nine names absent from the map are methods on implementation-only `pub(super)` types: `ControlPlane::{sender,join_terminal}`, `ManagerLink::same_table`, `PullService::stop_accepting`, `RebalanceSignal::{from_properties,next_due,fire_due,close_delayed}`, and `GroupRegistry::has_active_users`. No externally reachable runtime/route/config type or method name remains unmatched in this bounded scan. It does not prove input or source-branch parity; whole Phase 6 reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.
19. Public enum variant reverse scan (`2026-10-10`): the 17 public enum declarations under `crates/rocketmq-classic-core/src` have 72 declared variants, including test-only `RuntimeError::StartupFailure`. The API map now lists every exact Rust variant and its Java enum/status/request-code counterpart where one exists; it distinguishes the Rust-only `SendKind`, `SendMode`, lifecycle states and error carriers. Existing fixture-backed enum/RPC tests and lifecycle/inbound audits provide the behavior evidence. This closes names only; broader error/input/source/platform reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.
20. Public struct field reverse scan (`2026-10-10`): scanned 22 core API/runtime/route/config source files, finding nine named-field public structs with 74 public fields. Every exact Rust field token appears in the API map. This closes field-name inventory only; it does not certify every field's behavior or the remaining source/input/platform matrix.
21. Pinned Java public-method reverse scan (`2026-10-10`): `javap -public` on the seven Phase 6 reference classes found 169 public method declarations, all of whose exact method names appear in the API map. Constructors are excluded from the count. This closes method-name inventory for those seven classes only; overload/input behavior and source branches remain under audit. Phase 6 stays PARTIAL and P6-V04 remains `PENDING_PHASE_19`.
22. Pinned Java private/protected method reverse scan (`2026-10-10`): `javap -p` on the seven Phase 6 reference classes plus inherited `ServiceThread` found 26 hand-written private/protected declarations and 11 compiler-generated lambda methods. Each hand-written declaration is now mapped to its Rust flow or named downstream owner in the API map: manager single-flight, lifecycle rollback, scheduled tasks, raw-route cleanup and update decisions, persistence capability, heartbeat construction/fingerprints, group unregister, broker-version lookup, inactive reset-offset handling, producer-local queue selection, inbound reply/pull/pop owners, and inherited worker wait hooks. The method-name scan found no unmapped non-public declaration in this bounded set. This closes only the source-name/owner inventory; private branch behavior remains evidence-scoped, later-phase owners remain pending their phases, non-macOS runtime address enumeration remains unverified, and P6-V04 remains `PENDING_PHASE_19`.
23. Rust runtime RPC API-module reverse scan (`2026-10-10`): scanned the five `crates/rocketmq-classic-core/src/api/*.rs` files, totaling 62 public declarations and 47 distinct names. Two methods were absent from the Phase 5/6 compatibility maps: `get_default_topic_route` and `get_consumer_list_by_group_nullable`; the map now links them to `MQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer` (`TBW102`) and nullable `getConsumerIdListByGroup` request-header behavior respectively. Re-running the exact-name scan across the Phase 5 method/handoff maps and Phase 6 API map yields zero missing names. Existing runtime route and lookup wire cases provide behavior evidence; this source-name pass is not an overload/input audit, and broad Phase 6 reconciliation/platform gaps plus P6-V04 remain as recorded above.
24. Rust core public-declaration reverse name scan (`2026-10-10`): scanned all 30 `crates/rocketmq-classic-core/src/**/*.rs` files for `pub` functions/types/constants/traits, finding 643 declarations and 366 distinct names. Nine exact names were absent from the Phase 5/6 API maps: seven NameServer-addressing declarations (`NameServerUpdateCallback`, `DefaultTopAddressing::{with_parameters,with_provider,ns_addr,set_ns_addr,fetch_ns_addr_with_timeout}`) and the explicit heartbeat overload adapters (`send_heartbeat_to_broker_with_lock_mode`, `send_heartbeat_to_all_brokers_with_rebalance`). The API map now ties each to the pinned Java SPI/constructor/method or identifies the explicit provider-injection adaptation. Re-running the same name check yields zero missing declarations. This does not replace the earlier 74-field/72-variant scans or prove every method's inputs/behavior, and Phase 6 remains PARTIAL for the remaining source/input/platform evidence; P6-V04 stays `PENDING_PHASE_19`.
25. Invoked default-topic route API oracle (`2026-10-10`): `RuntimeLookupFixtures.defaultTopicRouteApiCase` calls pinned Java 5.5.1 `MQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(1234)` directly through a proxy `RemotingClient`, without a broker or socket. The fixture records both request envelopes (code 105, topic `TBW102`, timeout 1234), the decoded success route, and the `TOPIC_NOT_EXIST` outcome (`MQClientException`, response code 17, remark `topic not exist`). Maven's fixture `--verify` mode passes. This adds direct API evidence for the fixed topic and error result; Rust differential verification is tracked separately, and whole Phase 6 source/input/platform reconciliation remains PARTIAL with P6-V04 `PENDING_PHASE_19`.
26. Default-topic route Java/Rust differential (`2026-10-10`): `default_topic_route_api_matches_invoked_java_api_fixture` reads item 25's direct Java API fixture, calls `MQClientApi::get_default_topic_route` against a scripted loopback name server, and compares the request code/topic, decoded broker and queue counts, plus Java `TOPIC_NOT_EXIST` code/remark with Rust's typed response error. The focused locked Rust test passes. This closes the direct default-route API request/result differential only; route conversion/fallback behavior and whole Phase 6 source/input/platform reconciliation remain separately scoped, with P6-V04 `PENDING_PHASE_19`.
27. Java NameServer provider iteration oracle (`2026-10-10`): `RuntimeDiscoveryFixtures.providerSequence` installs three scripted providers in `DefaultTopAddressing.topAddressingList` and invokes Java `fetchNSAddr` and `registerChangeCallBack` directly. The first provider returns null, the second returns `second:9876`, and Java skips the third for fetch; callback registration still reaches all three. The pinned discovery fixture's Maven `--verify` mode passes with 20 recorded runtime discovery cases. This closes the direct Java observation of provider ordering only; Rust's builder currently retains one provider and still needs the matching chained-provider behavior and differential regression.
28. Ordered NameServer provider fallback (`2026-10-10`): `DefaultTopAddressing` now retains chained `with_provider` values in insertion order, tries until the first non-null/non-empty address, and registers each change callback with all configured providers. `chained_addressing_providers_match_java_service_loader_order` compares fetch order, selected address, callback order and callback identity with audit item 27; it first failed because Rust selected the last provider, then passed after the fix. The focused discovery test and strict Clippy pass. This closes ordered SPI provider iteration only; the remaining Phase 6 source/input/platform audit remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.
29. NameServer nullable address accessors (`2026-10-10`): the pinned Java discovery fixture directly observes `DefaultTopAddressing.getNsAddr/setNsAddr` at the null default, after setting `discovered:9876`, and after clearing back to null. `nameserver_address_property_matches_invoked_java_accessors` compares the native `ns_addr/set_ns_addr` sequence with those values. The pinned discovery fixture's Maven `--verify` mode passes with 21 recorded cases. This closes these accessor values only; broader Phase 6 method/source/input/platform reconciliation remains open and P6-V04 remains `PENDING_PHASE_19`.
30. Runtime RPC API reverse-scan continuation (`2026-10-13`): the public API module adds `MQClientApi::query_consume_time_span`, mapped to pinned Java `MQClientAPIImpl.queryConsumeTimeSpan` and RequestCode 303. `PHASE_6_API_CONFIG_MAP.md` now records the API; its invoked Java fixture and Rust scripted loopback cover the header, decoded response and Broker error. The five-file API scan therefore has one additional public method; this bounded name/behavior mapping does not close broad Phase 6 input/source/platform reconciliation, which remains PARTIAL, and P6-V04 remains `PENDING_PHASE_19`.
31. Tools-only admin helper disposition (`2026-10-13`): pinned-source call-site scan found public `MQClientAPIImpl.checkRocksdbCqWriteProgress` and `exportRocksDBConfigToJson`, plus `CheckRocksdbCqWriteResult`, in the tools-backed `MQAdminExt`/`DefaultMQAdminExt` path. They are not methods on the client `MQAdmin` interface or Producer/Push/Pull facades, and Phase 17 explicitly excludes `rocketmq-tools`; the Phase 17 matrix now records them as `TOOLS_OUT_OF_SCOPE`. This resolves one source-family boundary only; P18-V01 remains PARTIAL for the remaining source/API/input/reverse-export audit and P6-V04 remains `PENDING_PHASE_19`.

Linux production address-order parity follow-up (2026-10-12): compiled the current `RuntimeLinuxAddressFixtures` and captured the full pinned Java 5.5.1 `NetworkUtil.getLocalInetAddressList()` sequence in two Linux ARM64 Docker namespaces. The ignored Rust production-enumerator regression `linux_production_candidate_order_matches_invoked_java_runtime_snapshot` passed against both snapshots: 3 candidates on the default bridge and 4 candidates on an IPv6-only network created with Docker IPv4 disabled. This closes same-namespace Linux candidate-order comparison for those two container topologies. Windows IP Helper runtime enumeration, non-macOS BSD enumeration, an actual IPv6-only host, and the broader Phase 6 source/API/input/reverse-export reconciliation remain open.

Historical Rustdoc re-export-aware Producer/Consumer method inventory (2026-10-13): the first pass generated 203 Producer and 437 Consumer occurrences, then counted 137 and 157 exact-token candidates absent from compatibility Markdown. The follow-up crosswalk on 2026-10-13 expanded the inventory to 183 Producer occurrences across 13 pages and 457 Consumer occurrences across 27 pages; exact-identifier comparison then found zero unmatched method names. The 2026-10-23 declaration review also found no Rust declaration-name discrepancy. The historical candidates were mapping-format false positives, not confirmed missing APIs. The scoped API-name crosswalks now account for the Producer, Consumer, Common, Protocol, Core, Remoting and facade exports, while method behavior, overload/input/error/control-flow semantics and Java-side omission review remain P18-V01 work.

Rustdoc re-export-aware producer/consumer inherent-method crosswalk refinement (2026-10-13): the later scan included re-exported public type pages and matched every Rust inherent method name to a compatibility Markdown mapping or explicit Rust-only disposition: 183 method occurrences across 13 Producer type pages (156 distinct names) and 457 occurrences across 27 Consumer type pages (287 distinct names), with zero unmatched names in either set. This supersedes the earlier token-only unmatched counts above for this bounded inventory. It does not close Java-side reverse-export reconciliation, inherited/Deref/trait APIs, Java source branch/input equivalence, or P18-V01; those remain PARTIAL.

Pinned Java Pull wrapper declared-public method reverse scan (2026-10-13): scanned `client/.../impl/consumer/PullAPIWrapper.java` at commit `efa1be59d6fc747ad220f036c86030f05f0419f9`. The class declares 14 public method signatures across 13 method names plus its public constructor; `pullKernelImpl` has two signatures. Every declared method maps to `PullAPIWrapper` accessors/processing, Pull/Push/Lite owner RPC flows, or the Phase 13 Push POP owner rows in `PHASE_9_OFFLINE_METHOD_MATRIX.md`. No declared public method name is absent from those mappings. The result-processing/state methods have the existing Java fixture, `pull_api_wrapper_mutators_and_live_hook_list_match_java_entrypoints`, `get/setPullAPIWrapper` identity fixture, and scripted Pull/Push/Lite owner evidence. This closes only the bounded declared-method name inventory; overload inputs and every source branch remain part of P18-V01.

Pinned Java `NamespaceUtil` reverse scan (2026-10-13): scanned `remoting/.../protocol/NamespaceUtil.java` at commit `efa1be59d6fc747ad220f036c86030f05f0419f9`. All four public constants and nine declared public method signatures (nine names; `withoutNamespace` has two overloads) map to `NamespaceUtil`'s Rust associated constants/methods, including the explicit-namespace adapter `without_namespace_for`. The existing Java fixture differential invokes every declared method and checks the constant values. This closes the bounded declared-name inventory only; the observed fixture corpus remains the authority for input/branch coverage, and P18-V01 remains open outside this slice.

Pinned Java `TopicValidator.ValidateResult` constructor reconciliation (2026-10-13): the source declares a public `(boolean, String)` constructor and returns the supplied nullable remark unchanged. Rust `ValidateResult::new(bool, Option<String>)` and `remark() -> Option<&str>` now preserve that surface; the Java fixture directly invokes six cases covering both valid flags with null, empty, and non-empty remarks, and `public_topic_validator_surface_matches_pinned_java_fixture` compares both getters. Fixture SHA-256: `7ea74b6b60b54f60c1b32de5b770474c849f143f4794b4f3aee05c48305dd5ce`. This closes the constructor/nullable-result gap only; broad Phase 6 source/API/input/reverse-export reconciliation remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

Pinned Java `TopicValidator` null-input and nullable-set reconciliation (2026-10-13): `isTopicOrGroupIllegal(null)` throws `NullPointerException`; `isSystemTopic(null)` throws until null is inserted into the live system-topic set, after which membership returns true; the send-block set likewise accepts and tests a null member. Rust adds `TopicValidatorError::NullString`, nullable input adapters, and nullable live-set entries. The invoked Java fixture and `public_topic_validator_surface_matches_pinned_java_fixture` compare these outcomes plus present-string cases. Fixture SHA-256: `33a3617cca49b8803a457b28990529abdcb2f32888d35e585138251964bcfef1`. This closes only the observed TopicValidator null branches; broad Phase 6 source/API/input/reverse-export reconciliation remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

Pinned Java `TopicValidator.ValidateResult` inherited equality reconciliation (2026-10-15): `javap -public` and reflection on the invoked Java fixture show that `equals` and `hashCode` are inherited from `java.lang.Object`; two instances with identical `valid`/`remark` fields are unequal while an instance equals itself. Rust removes derived structural `PartialEq`/`Eq` from `ValidateResult` and adds a compile-fail rustdoc guard. The direct Java fixture and Rust topic-validator test capture the identity behavior and API boundary. Fixture SHA-256: `5f6b913ce7174c578fe069cd3d1b6badb44c971b8c16c164378ab6129dc23c55`. This closes one inherited-trait discrepancy only; broad Phase 6/P18-V01 source/API/input/reverse-export reconciliation remains PARTIAL.

Pinned Producer executor API reconciliation (2026-10-13): the Java 5.5.1 public `DefaultMQProducer.setCallbackExecutor` and `setAsyncSenderExecutor` methods were absent from the Rust Producer facade. Rust now exposes `set_callback_executor` through the started shared remoting client and `set_async_sender_executor` plus an effective-executor accessor; the facade re-exports both executor traits. The Java producer fixture records the stable default async executor, custom-executor retention, null reset, and pre-start callback-setter `NullPointerException`. Rust tests cover executor forwarding, future-mode scheduling, continued send execution after dropping the result future, rejection with/without backpressure fallback, permit retention and expiration while queued. The Rust default dispatches onto the caller's Tokio runtime instead of Java's private fixed-size thread pool; this is recorded in the Phase 7 matrix. `fixtures/producer/core.json` SHA-256: `98151cdc15e623ab57b35246db63a8e49ff9d391a60d82fd4c6622bd7cc932c5`. This closes the identified Producer executor method omission only; P18-V01 still requires Java-side reverse-export, inherited/trait, and source/input branch reconciliation.

Pinned Producer runtime-settings reverse scan (2026-10-13, source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): Java's `setBackPressureForAsyncSendNum/Size` resizes live semaphores while accounting for acquired permits; `setEnableBackpressureForAsyncMode` changes the branch selected by later async sends; `getRetryResponseCodes` returns the live set consumed by sync retry handling and `addRetryResponseCode` mutates it. Rust currently permits these settings only on `ProducerConfig` before `Producer::new`; constructed producers expose read-only config and create fixed-capacity semaphores. These are local P18-V01 implementation gaps, not Phase 19 manual checks. Java's public `setBackPressure...InsideAdjust` and read-lock helpers are called by its implementation to maintain permits; Rust encapsulates this bookkeeping, but those raw public entrypoints still need an explicit disposition. `getCompressor` and `initProduceAccumulator` are also public Java methods called by implementation paths and need a source-backed disposition. The reverse API/source/input audit remains PARTIAL.

Producer runtime backpressure/retry settings gap closure (2026-10-13): Rust `Producer` now provides runtime mode and capacity getters/setters, with a fair adjustable weighted semaphore that preserves acquired permits when resized, including a negative available count when in-flight work exceeds a lowered capacity. Runtime setters clamp to Java minima and update the same values returned by `Producer::config()`. `RetryResponseCodes` is now a shared live handle returned by both the config and Producer; mutating it changes later synchronous retry decisions, while cloning a `ProducerConfig` creates an independent settings snapshot. Java's public `setBackPressure...InsideAdjust` methods map to corresponding Rust methods, and the count/size read-lock helpers map to `BackpressureReadGuard` whose drop releases the lock. `fixtures/producer/core.json` directly invokes the Java runtime setters and live set operations, and reflects the Java count semaphore to verify 7 in-flight permits produce 5 available after changing the limit to 12, then 12 after completion. Fixture SHA-256: `a6dcc14b5bf14cf1a00c4f5c6cb970c8255d20637fa7b2304cf196be7a0c47cc`. Rust tests cover permit resizing, queued-waiter wakeup/cancellation, public lock interaction, runtime toggles and retry decisions. This closes those Producer settings/helper rows only; `getCompressor`, `initProduceAccumulator`, Java reverse exports and the broad source/input/branch audit remain open under P18-V01.

Producer compressor and accumulator API mapping (2026-10-13): the pinned Java fixture now invokes `getCompressor`/`setCompressType` for LZ4, ZSTD and ZLIB, checks each implementation class and compress/decompress round trip, and verifies an already returned compressor keeps its original type after the producer switches backends. Rust exposes `Producer::compressor()` as a `ProducerCompressor` snapshot with `compress`/`decompress`, and `Producer::set_compress_type()` updates later sends. The auto-batch fixture checks `getAutoBatch()` before accumulator initialization, after initialization, and after runtime off/on toggles. Rust exposes `init_produce_accumulator`, runtime `auto_batch`/`set_auto_batch`, and calls initialization during `start` at the source-backed lifecycle point; scripted sends verify enabling batching after start and switching subsequent sends back to direct mode. Fixture SHA-256: `da417c35e77ed3c6c8edff59ea226d8fd4e9607f43a6eef085889223dfe1d3ec`. Maven fixture verification and the Producer crate tests pass. This closes these Producer API and lifecycle rows only; Java reverse exports, inherited/trait APIs, and broad source/input/branch reconciliation remain PARTIAL under P18-V01.

Producer runtime compression setting parity (2026-10-13): Java `DefaultMQProducer.setCompressMsgBodyOverHowmuch` and `setCompressLevel` are read by `DefaultMQProducerImpl.tryToCompressMessage` on each send. Rust now exposes matching `Producer` getters/setters backed by live atomic config values; cloning `ProducerConfig` retains an independent snapshot. The invoked Java fixture directly changes the threshold and level, verifies threshold boundary behavior and distinct level 1/9 ZLIB output. Rust `runtime_compression_settings_match_java_and_config_clones_are_snapshots` verifies subsequent messages use the changed threshold and compression level, decompression round trips, and clone independence. `fixtures/producer/core.json` SHA-256: `3316032f16cb1c70c03669b65408db60c7c8fa5bab792640aabfbb4e017bfefe`. Code commit `da683076`; workspace default/all-feature tests, strict workspace Clippy, formatting/diff, Maven fixture verification, comparator/manifest, race/leak smoke, fuzz smoke, release examples and benchmark compilation pass on this revision. The broad Java reverse-export, inherited/trait API, and source/input/branch audit remains PARTIAL under P18-V01.

Nullable Producer group compatibility (2026-10-14, source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): pinned Java `DefaultMQProducer` accepts null in a group constructor or `setProducerGroup`; `start()` applies namespace wrapping, then `Validators.checkGroup(null)` reports `the specified group is blank` and leaves service state `START_FAILED` before client-factory creation. Rust `ProducerConfig` now stores `Option<String>`, offers `new_nullable`, accepts the same nullable `Option<String>` in `set_producer_group`, and exposes `producer_group() -> Option<&str>`; `Producer::producer_group()` preserves the nullable value through pre-start and failed-start states. `Producer::start` returns `ProducerError::BlankGroup` with Java's exact message and the matching service state, with or without a namespace. The invoked Java fixture covers constructor, setter, namespaced constructor and both start outcomes; `pinned_java_producer_constructor_values_map_to_rust_configuration` and `null_producer_group_start_matches_pinned_java_validation` compare the Rust results. Fixture SHA-256: `b0a59a0c084fc24ddff5c4947e7064d20311fe0dcf38306b8ac45ee15546c4fc`. Producer and workspace tests, strict workspace Clippy, Maven fixture verification, format and diff checks pass. This closes the nullable group slice only; broad P18-V01 source/API/overload/input/branch and reverse-export reconciliation remains open.

Pinned Producer live scalar settings (2026-10-14; Java source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): Java `DefaultMQProducer` exposes live setters for create-topic key, default queue count, send timeout, per-request retry timeout, sync/async retry count, retry-after-non-OK-store, and max message size; `DefaultMQProducerImpl` reads them on subsequent send/header/validation paths. Rust `Producer` now exposes corresponding getters/setters backed by thread-safe live config values, while `ProducerConfig::clone` keeps an independent snapshot. The invoked Java `runtimeSettings` fixture calls every setter/getter. Rust regressions cover later send-header values, default send deadline changes, retry attempt count, retry to another Broker after a non-OK store response, live message-size validation, and detached snapshots. Code commit `781bf6f0`; `fixtures/producer/core.json` SHA-256 `46c9c9522317d72d2942fab4abae0caf9c6fd9bf76a179ea90a6ec453b041f3c`. The 127-test Producer suite, default/all-feature workspace tests, strict workspace Clippy, format/diff checks, and Maven fixture verification (37 scenarios and cross-decodes) pass. This closes these scalar setter rows only; Producer group, VIP-channel and topic-list runtime setter semantics, Java reverse exports, inherited/trait APIs, and broad source/input/branch reconciliation remain open under P18-V01.

Producer setter follow-up (`f3d5b44d`, 2026-10-14): the group, VIP and topic setter semantics above now have fixture-backed Rust coverage. The fixture hash is `1ed0060666e8ee2a6b5b82b3088b22cb41a9a690bc291a96e56e51d16e7f7d76`. Java copies ClientConfig when it creates the shared `MQClientInstance`, so post-start VIP changes update the Producer getter but leave active routing on the startup snapshot. Topic changes update the getter but not the started runtime's topic set. Group changes update later request headers; the shutdown group-key behavior and its effect on registry ownership are closed by the appended lifecycle differential. Java reverse-export, inherited/trait API, and broad source/input/branch reconciliation remain open under P18-V01.

Producer shutdown and request-future lifecycle (`2026-10-14`, pinned Java 5.5.1): `DefaultMQProducerImpl.shutdown(boolean)` looks up `defaultMQProducer.getProducerGroup()` at shutdown time and changes service state only after unregister succeeds. Java therefore removes the current group-table entry after a group mutation (even if that key belongs to another Producer); an absent key is a no-op; a null group throws `NullPointerException` and leaves the implementation RUNNING. `RequestFutureHolder.shutdown(producer)` leaves pending replies and callbacks untouched and stops its timeout scanner when the last Producer is removed. Rust's public Producer shutdown now follows the group-key behavior, preserves RUNNING on null-group removal failure, and its by-group unregister path preserves pending correlations; its lower-level `GroupLease` remains identity-bound. `ProducerLifecycleFixtures` invokes these Java paths and `producer_shutdown_matches_java_pending_request_and_timeout_scanner_state` compares the behavior. Fixture SHA-256: `9d5063e8ae538f7da3f957c874dfb95dd81a6a85bf113a947a796de1815001d8`. Focused Producer/Core tests and pinned Maven verify pass. This closes only the lifecycle discrepancy; broad P18-V01 reverse-export, inherited/trait, and source/input/branch reconciliation remains PARTIAL.

Rust Fastjson adapter reverse-surface closure (2026-10-14): the common crate exposed 24 helper types/functions under `rocketmq_classic_common::serde_fastjson`, while protocol DTOs used them as implementation plumbing. The pinned Java 5.5.1 public utility surface consists of `FastJsonSerializer` (`serialize`/`deserialize`) and `RemotingSerializable` (`encode`/`decode`/`decodeList`/`fromJson` and instance JSON methods); it has no public per-field coercion helpers corresponding to those Rust functions. The common and protocol modules are now private to their owning crates; protocol keeps an internal adapter implementation and common retains only the `MessageQueue` conversion subset. Compile-fail rustdoc examples check both old downstream import paths. Code commit `b62e66c1`; common/protocol tests and strict all-target/all-feature Clippy pass. This closes that Rust-only public-surface discrepancy only; broad P18-V01 Java API/input/branch and reverse-export reconciliation remains open.

Producer per-request timeout/retry closure (`2026-10-14`, pinned Java source `efa1be59d6fc747ad220f036c86030f05f0419f9`): Java caps only synchronous attempts that have a later retry, only when the remaining send budget exceeds `SendMsgMaxTimeoutPerRequest`; `-2` and `-1` disable the cap, `0` expires the current attempt, and positive values clamp to the overall deadline. `sendKernelImpl` reports a request-scoped local timeout as `RemotingTooMuchRequestException`, which `sendDefaultImpl` catches and retries while the overall deadline remains. The invoked `perRequestTimeoutRetry` fixture uses a 5 ms hook to force that pre-transport timeout and records two before/after hook invocations for one configured retry; its setter inputs are `-2`, `-1`, `0`, `125`, and `6000`. Rust now retries a local `ProducerError::Timeout` only when the request cap is earlier than the overall deadline and time remains. `per_request_timeout_only_limits_sync_attempts_that_have_a_retry_after_them` covers cap boundaries and mode/final-attempt exclusions; `zero_per_request_timeout_retries_sync_attempts_within_total_budget` compares the retry count to Java. Fixture SHA-256: `c303c79a59a2bc7220aae6c488d9dc6cfda18d28590aa3f4d8a50af55e8c4dc8`. This closes the timeout/retry slice only; full P18-V01 source/API/input/reverse-export reconciliation remains PARTIAL.
- Java `Message` public API slice (`2026-10-14`, pinned client 5.5.1): Rust maps the default constructor and all four parameterized constructor forms including nullable topic/body, property reads and mutable map access, Buyer ID and instance ID, collection-based keys including null collections/elements, delay-level and timer-delay getters/setters, Java signed integer parsing, nullable property-map state and null-valued system-property setters, and inherited `Message` access on `MessageExt`. The invoked `messagePublicApi` fixture checks default null state, lazy property-map creation after null-name lookup, nullable constructors/setters and `toString`, user-property null rejection, nullable serialization round-trips, `Integer.parseInt`/`Long.parseLong` bounds, Unicode decimal digits, malformed/overflow rejection and map mutation. `message_public_api_matches_java_551_fixture` passes; fixture SHA-256: `6ec3f130f86c25b12b08f5d5d51f5b43c0bab1a2e77738208909959bbef79db5`. Broader Java surface/inherited API and reverse-export reconciliation stay under P18-V01.
- Producer implementation accessors (`2026-10-14`, pinned Java client 5.5.1): added `Producer::default_mq_producer_impl()` and `Producer::default_mq_producer()` as borrowed aliases to the same Rust owner, matching `DefaultMQProducer.getDefaultMQProducerImpl()` and `DefaultMQProducerImpl.getDefaultMQProducer()` without a second wrapper allocation. The invoked Java fixture records both identity checks; `implementation_state_accessors_match_pinned_java_fixture` compares those outcomes and Rust pointer identity. Fixture SHA-256: `12c6f78ba79bda9d50ccec5ebeacbfbcf3ce102e60b9ee5ad05f83510a3fe0b1`. This closes these accessor identities only; broad P18-V01 source/API/input/reverse-export reconciliation remains PARTIAL.
- Producer facade callable method-name crosswalk (`2026-10-14`, pinned Java 5.5.1): `producerFacadeApi` records 183 callable `DefaultMQProducer`, 42 callable `MQProducer`, and 8 `MQAdmin` signatures, including inherited `ClientConfig`/`MQAdmin` methods. The new `pinned_java_producer_callable_method_names_have_phase_crosswalks` guard checks every method-name token against the Phase 6/7/14–17 mappings. Its initial failures exposed exact-name documentation omissions hidden by `get/set` and slash shorthand (`getAutoBatch`, `getProducerGroup`); the matrices now spell out the corresponding Producer runtime getters/setters and boundary helpers. This closes the callable method-name crosswalk only; per-overload input/error/control-flow coverage and reverse-audit of the complete Rust public surface remain open under P18-V01. Fixture SHA-256: `12c6f78ba79bda9d50ccec5ebeacbfbcf3ce102e60b9ee5ad05f83510a3fe0b1`.
- Producer send input mutation (`2026-10-14`, pinned Java 5.5.1): direct `sendKernelImpl` writes `UNIQ_KEY` to the caller message after resolving a Broker address and before send hooks/transport; eligible `sendByAccumulator` writes it after validation but before accumulator dispatch. Rust now accepts `&mut Message` on single-message send APIs, preserves an existing ID, writes the generated ID to both the caller message and wire copy, and keeps one ID across retries. The invoked `sendInputMutation` fixture checks both accumulator failure after caller mutation and direct-send service-state failure before mutation. Rust regression `accumulator_send_mutates_caller_before_runtime_dispatch_like_java` compares both outcomes. `auto_batch_flushes_one_message_batch_and_fans_out_results`, `sync_send_retries_across_two_brokers_and_preserves_one_prepared_message`, and `direct_send_preserves_existing_unique_id_on_caller_and_wire` cover auto-batch IDs, retry reuse, and supplied-ID preservation. Fixture SHA-256: `079e9fcac47f97f4921a823c10b02a3b0ec882c42b85c85695663530047cd23d`. Producer-scoped tests pass; the broad P18-V01 source/API/overload/input/reverse-export reconciliation remains open.
- Producer public API reverse-scan correction (`2026-10-14`): `TopicPublishInfo::from_snapshot` assembled a public Producer value from the internal shared-runtime `TopicSnapshot`, but the pinned Java API exposes no matching public constructor. `ProducerQueueIndex::current`, `TopicPublishInfo::snapshot`, and `TopicPublishInfoTable::{snapshot,keys}` likewise have no Java public method counterpart and are only used by internal formatting, runtime snapshot, and topic-inventory code. These four helpers are now crate-private. The Phase 7 matrix also uses the exact Rust names for the public Java `setBackPressureForAsyncSend*InsideAdjust` equivalents. This closes those surfaced crosswalk issues only; P18-V01's full reverse-export and behavior reconciliation remains open.
- Producer publish-info Rust-to-Java name crosswalk (`2026-10-14`, pinned Java client 5.5.1): the Phase 7 matrix now names Rust `TopicPublishInfo` queue-list/cursor/selector APIs and the nullable `Map.replaceAll`, live-view `removeIf`, and values-view iterator `removeAll` adapters beside their exact Java counterparts. `rust_publish_info_method_names_have_java_crosswalks` guards all 11 surfaced Rust API rows. This closes those name mappings only; broad Rust reverse-export, overload/input/error/flow reconciliation remains open under P18-V01.
- Producer publish-info Java surface and cursor overflow (`2026-10-14`, pinned Java client 5.5.1): invoked fixture inventories all 17 public `TopicPublishInfo` methods, its nested `QueueFilter.filter` method, and all 3 public `ThreadLocalIndex` methods. Rust `ProducerQueueIndex::increment_and_get` now applies Java's `0x7fffffff` return mask after storing the wrapped `i32`; `Display` matches Java `ThreadLocalIndex.toString()`, including `null` before initialization. The fixture forces overflow from `i32::MAX`, records Java returns `0` then `1`, and records the raw post-overflow string. Surface/crosswalk tests and `producer_queue_index_overflow_matches_pinned_java_fixture` guard the mapped APIs and behavior. Fixture SHA-256: `f998d5e4d5d716d3cb0aa4f510f574960cb499a80b728f24ea7b8b150114e04e`. This closes these adjacent producer API slices only; broad P18-V01 remains open.
- Producer/route handle API reverse-scan closure (`2026-10-14`, pinned Java client 5.5.1): `ptr_eq` was a Rust-only inherent method on `ProducerPublishQueueList`, `ProducerQueueIndex`, `TopicPublishInfo`, and `TopicRouteDataHandle`; none is present in the corresponding Java `List`, `ThreadLocalIndex`, `TopicPublishInfo`, or route-data API. The four methods are no longer publicly callable; producer map/view operations that require Java reference identity retain their crate-internal checks. Integration coverage now verifies retained aliases through shared mutation, and compile-fail doctests guard the removed methods. Producer publish-info integration tests and Producer/Core doctests pass. This closes these surfaced Rust-only methods only; broad P18-V01 API/input/flow reconciliation remains open.
- Common message API reverse-scan correction (`2026-10-14`, pinned Java client 5.5.1): the public Rust-only `Message::{take_body,take_body_nullable,set_property,properties_mut}` helpers and `MessageQueue::new_nullable_broker` have no distinct Java public methods. The `take_body*` methods had no product call sites; `set_property` duplicated `put_user_property`; the mutable property map test now uses `get_properties_mut`; and internal nullable queue construction uses `MessageQueue::new_nullable`. Compile-fail rustdoc examples guard all five removed method boundaries. `cargo test --workspace --all-features --locked`, strict workspace Clippy, rustfmt, and rustdoc JSON surface inspection pass on the resulting tree. This closes those surfaced Common API extras only; P18-V01's complete Java/Rust reverse-export and behavioral reconciliation remains open. Code commit: `2618fd1c`.
- ACL public-surface reverse-scan correction (`2026-10-14`, pinned Java 5.5.1 commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): reviewed the declared `AclSigner`, `AclUtils`, and `SessionCredentials` APIs against `auth.rs` and the invoked ACL fixture. Java's explicit `(RemotingCommand, SortedMap)` canonicalization overload remains exposed as `AclUtils::combine_request_content_with_fields`; the non-Java one-argument associated adapter and duplicate module-level utility functions are now internal. `SessionCredentials::with_security_token` and its YAML factories are no longer public because Java exposes the token constructor on `SessionCredentials` and YAML factories on `AclUtils`; Rust's three-argument constructor and `AclUtils` reader/file methods remain. `AclSigner::cal_signature_with_material` remains the Rust byte-input adapter for Java's explicit Charset outcomes. Separate compile-fail doctests guard the removed API boundaries. Code commit `6893f14b`; `cargo test -p rocketmq-classic-core --locked` (157 unit tests, 1 ACL profile test and all crate integration/doctests), strict core Clippy, `cargo fmt --all -- --check`, and `git diff --check` pass. This closes only this ACL API slice; P18-V01's full source/overload/input/error/flow and reverse-export audit remains open.
- Core admin-query request API (`2026-10-14`, pinned Java `MQAdminImpl.queryMessage` overload): `AdminQueryRequest` previously exposed every field, including derived route and LMQ metadata, while `index_type: String` could not express Java's nullable argument. Its fields are now crate-private; public builders preserve nullable `clusterName`, `indexType`, and `lastKey`, and derive route/LMQ state from the topic. Null/empty index types now produce Java's no-match result and are omitted from the wire header when null. Unit and scripted RPC tests cover ordinary/LMQ/wheel-timer routes, cluster selection, LMQ request topic and cluster-filter bypass, and null index/pagination inputs. Code commits `e7dcdf63` and `daef67eb`. Locked tests for Core, Producer, and Consumer, strict workspace Clippy, formatting/diff checks, and nightly workspace rustdoc JSON generation pass; the JSON confirms no public `AdminQueryRequest` fields remain. This closes the query-envelope slice only; broad P18-V01 API, settings, overload/error/flow, and reverse-export reconciliation remains open.
- `QueryResult` Java API and alias alignment (`2026-10-14`, pinned client source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): the Java DTO has private final fields, one public `(long, List<MessageExt>)` constructor, two getters, and `toString`. Rust fields are now crate-private; its public constructor/getters and `Display` match that surface. `QueryMessageList` and shared element handles preserve nullable list/elements, constructor list identity, getter aliasing, caller mutation, and Java `toString` output. The invoked Java fixture records reflection surface and behavior; `query_result_surface_and_behavior_match_pinned_java_fixture` compares Rust. Fixture SHA-256: `8cf1f3168dba8087f542a6c694fc4ad040353f27d1439da46a36f36d1412fd19`. Code commits `ba2589ec` and `57fe1527`; default/all-feature workspace locked tests, strict workspace Clippy, Maven verification of all 37 scenarios and fixture drift, formatting/diff checks, and Core/Producer/Consumer rustdoc JSON generation pass; Core JSON reports stripped private fields. This closes the QueryResult DTO slice only; the broader P18-V01 public API/settings/input/flow/reverse-export audit remains open.
- Pull facade registration helper reverse-scan correction (`2026-10-14`, pinned Java client 5.5.1): Java `DefaultMQPullConsumer` exposes `registerMessageQueueListener`, register-topic getters/setters, and explicit subscription operations; it has no public singular `registerTopic` or combined `unregisterTopic` operation. Rust's singular registration helper is now crate-private, the Rust-only combined unregistration method is crate-private for Lite's internal adapter, and separate Java-mapped registration/subscription/clear operations remain. Two compile-fail doctests guard both removed public method names. The pinned-source scan finds only the `registerTopics` field/getter/setter and the Java listener method. Code commit `2bfa4c16`; Consumer locked tests/doctests, strict Consumer Clippy, formatting, and diff checks pass. This closes those surfaced Pull API extras only; broad P18-V01 overload/input/flow and reverse-export reconciliation remains open.
- Local offset store public API reverse-scan (`2026-10-14`, pinned Java 5.5.1): `LocalFileOffsetStore` has no public group-name or storage-path getter; Java exposes its constructor and `OffsetStore` operations only. Rust's unused `group_name()` and `storage_path()` inspection accessors are removed, and the stored group-name field is no longer retained after path construction. Compile-fail doctests guard both public method boundaries; tests inspect the private path only inside the crate. Code commit `e8582a22`; Consumer locked tests/doctests, strict Consumer Clippy, rustdoc JSON, formatting, and diff checks pass. This closes these local-store accessor extras only; broader P18-V01 reconciliation remains open.
- Consumer facade inventory and `viewMessage` compatibility (`2026-10-14`, pinned Java 5.5.1): `fixtures/consumer/facade-api.json` records declared and inherited Push/Pull/Lite facade signatures and constructor counts; `pinned_java_consumer_facade_inventory_covers_declared_and_inherited_methods` pins those counts and confirms the public Push/Pull facades do not declare `queryMessageByUniqKey`. Rust removed the extra `query_message_by_unique_key` facade methods. Producer, Push, and Pull `view_message` now match Java's physical-ID attempt followed by unique-key fallback on parse/RPC failure, including max 32, a three-day lower bound, `Long.MAX_VALUE`, first-result selection, and nullable no-result behavior. Scripted tests cover direct lookup, fallback fields, namespaced topic handling, and terminal errors. The Pull example now uses the Java-mapped `register_message_queue_listener` API. Fixture SHA-256: `6f1c84a130e13bfbf2bac19a85baa5978784d2c41689fc7f0b09b7d9b2803f55`. Code commit `f98bdaa8`; default/all-feature workspace locked tests, strict all-target/all-feature Clippy, pinned Maven verification (37 scenarios), comparator/manifest, race/leak and fuzz smoke, release examples, benchmark compilation, metadata, format, and diff checks pass. This closes these facade signatures and message-view branches only; full P18-V01 source/input/error/flow and reverse-export reconciliation remains open.
- Push live pull batch settings (`2026-10-14`, pinned Java client source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): `DefaultMQPushConsumerImpl.pullMessage` obtains `pullBatchSize` and `pullBatchSizeInBytes` from the consumer for each request. Rust now reads both atomic config values when building each pull request, so updates after `PushConsumer` construction affect the wire header. The invoked Java orderly fixture pins defaults/configured values; `push_pull_request_uses_live_batch_settings_from_consumer_config` verifies `maxMsgNums=7` and `maxMsgBytes=12345` after construction. Fixture SHA-256: `c00fbd654d1966bc7d4a870c03dca739d0b9465367b9bad239383dc6c11478c5`. Code commit `7ece0e66`; default/all-feature workspace tests, strict all-target/all-feature Clippy, pinned Maven verification (37 scenarios), formatting and diff checks pass. This closes only the two pull batch-setting timing rows; broad P18-V01 source/API/settings/input/error/flow and reverse-export reconciliation remains open.
- Push runtime-setting read-timing audit checkpoint (`2026-10-14`, pinned Java client commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): source search found additional Push runtime reads in `DefaultMQPushConsumerImpl` (pull interval, flow-control thresholds, message model, POP request settings), `ConsumeMessageConcurrentlyService` (consume timeout and thread maximum), and `RebalancePushImpl` (consume origin and per-queue thresholds). Their Java callsites and Rust mutation/read timing still require row-by-row reconciliation under P18-V01; this inventory is a pending audit list, not a claim that each item is already a defect.
- Push runtime-setting read timing (`f0eaf53e`, 2026-10-14; pinned Java source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): mutable Push settings now permit post-construction updates through the retained config reference. Pull interval, flow-control thresholds, message model facade reads, POP threshold/batch/invisibility, consume timeout/thread maximum, consume origin/timestamp, unit mode, retry limit, suspend delay, consume batch size, pull subscription mode, shutdown wait, and adjustment threshold read the live value at Java-equivalent runtime call sites. `RebalancePushImpl.clientRebalance` and `messageQueueChanged` read the current boolean/listener on later calls; Rust uses an atomic and lock-backed setting. Java `DefaultMQPushConsumerImpl.start()` copies the configured allocation strategy and message model into `RebalanceImpl`; Rust now snapshots both in `RebalanceService` at start while retaining live facade getters/heartbeat settings. `PullAPIWrapper` unit mode remains a start-time snapshot, as in Java. Regression coverage includes `push_live_runtime_settings_drive_owner_flow_helpers`, `push_pull_outcomes_insert_found_messages_and_correct_only_drained_offsets`, `push_pull_request_uses_live_batch_settings_from_consumer_config`, `initial_offsets_prefer_store_and_never_turn_query_failures_into_zero`, `broadcasting_rebalance_activates_every_queue_with_local_offsets_and_notifies`, `client_rebalance_false_uses_broker_assignment_unless_orderly_forces_local`, and `startup_snapshots_the_current_rebalance_settings`. `cargo test -p rocketmq-classic-consumer --all-features --locked` passes (205 unit tests plus crate integration/doc targets); strict all-target/all-feature Consumer Clippy, format and diff checks pass. This closes these identified timing rows only; P18-V01's complete source/API/settings/input/error/control-flow and Rust reverse-export reconciliation remains PARTIAL.

- Consumer Running Info inherited-trait reconciliation (`2026-10-15`, pinned Java client 5.5.1): `ConsumerRunningInfo`, `ProcessQueueInfo`, `PopProcessQueueInfo`, and `ConsumeStatus` do not override `equals/hashCode`; all inherit `java.lang.Object` identity. Rust's corresponding body/status models had derived structural equality. Removed those `PartialEq`/`Eq` implementations, added Java reflection/identity cases and compile-fail doctests, and changed the body differential to compare normalized wire JSON. Fixture SHA-256: `a16ba3fd660aa1b8ad6ef2150163a9657283ada56c728f6769f39f98b0a5fbe8`. This closes four related inherited-trait discrepancies only; broad P18-V01 source/API/input/reverse-export reconciliation remains PARTIAL.
- Protocol DTO inherited-trait reconciliation (`2026-10-15`, pinned Java client 5.5.1): `QueryAssignmentRequestBody`, `SetMessageRequestModeRequestBody`, `GetConsumerStatusBody`, `ResetOffsetBody`, and `ConsumeMessageDirectlyResult` do not override `equals/hashCode`; both methods resolve to `java.lang.Object`, self-equality is true, and distinct default instances are unequal. Rust removes structural `PartialEq`/`Eq` from the five DTOs and adds compile-fail doctests plus the invoked `ProtocolDtoIdentityFixtures` differential. Fixture SHA-256: `11f97e14fc3386c4ef61a6d82aa27f8e8b9b723a965fb3ddfb3b30233f0be0d9`. This closes five inherited-trait discrepancies only; broad P18-V01 source/API/input/reverse-export reconciliation remains PARTIAL.
- Protocol response-header inherited-trait reconciliation (`2026-10-15`, pinned Java client 5.5.1): 16 public Rust response-header DTOs had derived structural equality although their Java classes inherit `equals/hashCode` from `java.lang.Object`. Removed `PartialEq`/`Eq`, added one compile-fail doctest per type, and added invoked reflection coverage in `HeaderResponseIdentityFixtures`; round-trip tests now compare encoded header fields instead of DTO values. Fixture SHA-256: `d618338067333760a74c956d3ca677d4acf0c44acef87b4f9607395fb96f6e6f`. Request headers inheriting the custom `RpcRequestHeader` equality behavior are outside this slice; broad P18-V01 remains PARTIAL.
- Protocol request-header inherited-trait reconciliation (2026-10-15, pinned Java client 5.5.1): all 31 currently modeled request-header types whose Java classes inherit `RpcRequestHeader.equals/hashCode` now retain and encode/decode nullable `ns`, `nsd`, `bname`, and `oway`; Rust `PartialEq`/`Eq`/`Hash` compare only those base values and ignore subclass fields. The Java fixture mutates each inherited field reflectively, verifies payload-only equality/hash behavior, and pins the Rust-to-Java class crosswalk. `UnlockBatchMqRequestHeader` is now distinct from the Lock class; `ConsumeMessageDirectlyResultRequestHeader` stores inherited `bname` separately from its subclass `brokerName`. Rust tests cover all 31 types and base-field wire round-trips. Fixture: `fixtures/protocol/request-header-identity.json`, SHA-256 `4cc545d6d1fc0efdb2cdd74b6a1eb0cd0b00b98ad028766f8bd7d2c05bd75973`. This closes the modeled request-header inherited-equality slice only; the broad P18-V01 API/source/input/flow/reverse-export audit remains PARTIAL.

- Common `MessageExt` reverse-export follow-up (`fcab764e`, 2026-10-15, pinned Java client source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): Java `MessageExt` extends `Message` and declares no public `message()` or `message_mut()` composition accessors. Rust already uses `Deref`/`DerefMut` for the inherited `Message` methods, so the two extra public wrappers were removed and internal call sites migrated to the inherited methods. Compile-fail doctests guard both absent names; default and all-feature workspace tests, strict all-target/all-feature Clippy, rustfmt, and `git diff --check` pass. This closes only this surfaced Common API discrepancy; full P18-V01 reverse-export and behavioral reconciliation remains open.
- Common `MessageExt` declared-surface sweep (2026-10-15, pinned Java source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): re-enumerated the public constructors, `parseTopicFilterType`, both `socketAddress2ByteBuffer` overloads, born/store-host byte accessors and buffer overloads, broker/queue/host/timestamp/message-ID/system-flag/CRC/offset/size/reconsume/prepared-offset getters and setters, both IPv6 flag helpers, topic/group system-flag accessors, and `toString`. The Phase 8 matrix maps those declarations to Rust constructors, `MessageHostAddress` network-byte methods, `MessageExt` accessors, `Deref`/`DerefMut`, and `Display`. Invoked Java/Rust fixture coverage is recorded for scalar and host APIs; no additional missing `MessageExt` declaration surfaced. This closes that class's declared-method inventory only; the complete P18-V01 API/settings/input/error/flow/reverse-export audit remains open.
- `MessageClientExt` decoded offset-ID parity (`e15f0417`, 2026-10-15, pinned Java 5.5.1): the Java subclass's `getOffsetMsgId()` reads the inherited physical `msgId`, while `getMsgId()` reads live `UNIQ_KEY` first and then falls back to that offset. Rust client decode stores both values on its base `MessageExt`; the `MessageClientExt` adapter previously returned the client-visible ID as the offset when wrapping a decoded message. It now reads/writes the retained offset field and resolves message ID as live unique key, retained offset, then base ID. The red differential showed `producer-unique-1` instead of `C633641400002A9F0000018BCFE56829`; expanded scalar/batch client-decode tests now cover wrapping, offset replacement while a unique key is present, and clearing the unique key. `cargo test --workspace --locked`, all-feature workspace tests, strict workspace Clippy, Maven verify (37 scenarios/cross-decodes), comparator/manifest, race/leak smoke (16 × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), examples, benches, metadata, seven package listings, UTC/Asia-Taipei unique-ID checks, format and diff checks pass. This closes the MessageClientExt decoded offset flow only; broad P18-V01 reconciliation remains open.
- Protocol header helper reverse-export correction (`60a62324`, 2026-10-15, pinned Java client 5.5.1): the public Rust-only `required_string`, `optional_string`, integer/boolean field parsers, and synthetic `ProofHeader` were implementation or test helpers without Java client API counterparts. No product consumer outside the Protocol crate used the parser helpers. The helpers are private; remoting codec and fuzz tests now decode the Java-mapped `GetRouteInfoRequestHeader` from the existing fixture instead of exporting a synthetic type. A crate-level compile-fail doctest guards the public boundary. Default and all-feature workspace tests, strict all-target/all-feature Clippy, formatting, and diff checks pass. This closes those header exports only; broad P18-V01 reconciliation remains open.
- Physical message-ID decoder parity (`fa6c0ff0`, 2026-10-15, pinned Java client 5.5.1): Producer, Push, and Pull `view_message` now use the Common `MessageDecoder.decodeMessageId` mapping rather than Core's duplicate strict parser. Java's `UtilAll.string2bytes` maps an invalid hex nibble to `-1`, producing `0xFF` for that byte; the invoked scripted regressions replace the physical ID's last offset nibble with `G`, verify the physical lookup still reaches `VIEW_MESSAGE_BY_ID` at offset `255`, and check each facade's subsequent success/fallback behavior. The duplicate Core decoder DTO/error/functions are removed and a compile-fail doctest guards those Rust-only exports. Focused Producer and Consumer facade tests, default/all-feature workspace tests, strict workspace Clippy, format/diff checks, Maven verify (37 scenarios and Java/Rust cross-decodes), comparator self-test/manifest, 16 race/leak cases × 3, fuzz smoke (1,824 mutations), release examples, benchmark compilation, and workspace metadata checks pass. This closes the duplicate-parser/export and malformed-nibble view-path slice only; broad P18-V01 reconciliation remains open.
- TLS `tls.config.file` initialization snapshot (`886c5095`, 2026-10-15, pinned Java client 5.5.1): Java initializes `TlsSystemConfig.tlsConfigFile` from the initial system property and continues using that path after the property changes. The invoked Java fixture initializes against a first properties file, changes the property to a second file with an opposite `tls.test.mode.enable` value, then builds a TLS context and records that the first path remains active. The Rust integration regression starts an isolated copy of its test process so the process-wide property `OnceLock` captures the first path, then changes the supplied properties to point at the second file and compares the outcome to the Java fixture. Fixture SHA-256: `0bfdd291c36efbab3f66f7943cc484ad6a1c441f9853db0e1f9ee4fc08b35b9d` (defaults) and `70877c872888015cf00e6957bdc3eeea3152abda835b64d384a9d0021b24c91a` (system properties). The isolated Rust regression, rustfmt check, diff check, and pinned Maven verify (37 scenarios) pass. This closes only the initial TLS config-file path snapshot slice; broad P18-V01 API/settings/input/error/control-flow and reverse-export reconciliation remains open.
- P18-V01 Remoting Rustdoc reverse-name follow-up (2026-10-15, pinned Java client 5.5.1 source `efa1be59d6fc747ad220f036c86030f05f0419f9`): the Rust public-name scan surfaced `ProcessorLease`, `register_owned_processor[_with_executor]`, and `TimeoutPhase` as names without exact Java declarations. Source review maps owned processor registration to Java `RemotingClient.registerProcessor` and the public live `NettyRemotingAbstract.getProcessorTable()` map; Core retains the Rust lease while inbound processors are installed and releases it on failed start or shutdown, with identity fencing so a stale lease cannot remove a replacement. `owned_processor_cleanup_preserves_replacement_even_for_same_handle` covers replacement and drop behavior. `TimeoutPhase` is typed context on existing operation, connection, admission, write, and response deadline outcomes; Java reports these through `RemotingTimeoutException` without a corresponding phase enum. These are Rust ownership/diagnostic forms of existing registration and timeout paths, not new transport operations. Added the explicit crosswalk to the Phase 3 Remoting inventory. This closes these named Remoting reverse-scan dispositions only; full bidirectional P18-V01 source/API/settings/input/error/control-flow/inherited-API reconciliation remains open.
- P18-V01 Core request/reply public-carrier reverse-scan follow-up (2026-10-15, pinned Java client 5.5.1 source `efa1be59d6fc747ad220f036c86030f05f0419f9`): `runtime::PendingReplyRequest`, `PendingReplyControl`, and `PendingReplyCompletion` are named in the Phase 15 matrix and mapped to Java `RequestResponseFuture` plus `RequestFutureHolder.getRequestFutureTable()`. Pinned Java exposes mutable future state and the live correlation map; Rust uses identity-bound controls and receivers. The high-level Producer.shutdown behavior remains fixture-aligned. Dropping a Rust async request removes its correlation, matching Java synchronous finally cleanup after interruption; explicit control cancellation removes the owned correlation. `ClientRuntime.register_reply_request` now requires Running, preventing request registration before startup completes. Startup rollback drops internal senders without exposing a Rust-only ProducerShutdown completion. This closes that lifecycle disposition only; full bidirectional P18-V01 remains open.
- P18-V01 Producer facade request/reply export follow-up (2026-10-16, pinned Java client 5.5.1 source `efa1be59d6fc747ad220f036c86030f05f0419f9`): the primary `rocketmq-classic` crate omitted the public `RequestCallback` type and the `MessageUtil.createReplyMessage` equivalent, even though the Producer crate exposed them. The facade now re-exports `RequestCallback`, `create_reply_message`, and `ReplyMessageError`; a facade-level integration test imports and exercises those names. This closes crate-root availability for these request/reply APIs only; broad bidirectional P18-V01 remains open.
- P18-V01 request/reply helper declaration inventory (2026-10-16, pinned Java client 5.5.1): the invoked request/reply fixture now records all declared public method signatures for `RequestCallback` (2), `RequestFutureHolder` (4), and `RequestResponseFuture` (18). The facade integration test pins the callback declarations and helper method counts, while the Phase 15 matrix maps operational behavior to Producer request methods and the runtime-owned correlation/scheduler. Mutable helper state and direct live-map access remain explicit in the review; this inventory does not close broad P18-V01.
- Primary facade mapped-type availability (2026-10-16): `rocketmq-classic` now re-exports the Java-mapped consumer/producer service states and default groups, POP result/status, end-transaction hook API, query result, producer publish-info table/value/views, and route DTO/alias handle. The `facade_public_types` integration test checks those names through the root crate and exercises the Producer state/table accessors. Their Java behavior crosswalks remain in the owning Phase 6, 7, 10, 13, 14, and 17 matrices; this closes root import reachability for the listed slice only, not broad P18-V01 source/input/reverse-export reconciliation.
- P18-V01 request-future nullable latch slice (`310b6cd6`, pinned Java client 5.5.1): `RequestResponseFuture.setCountDownLatch(null)` is now represented by `Option`; response insertion stores the response before returning a typed missing-latch error, and waiting with no latch also returns a typed error. Inbound reply completion preserves the correlation entry and maps the error to Java's `SYSTEM_ERROR` acknowledgement. `RequestFutureLatch::wait_forever` covers the no-deadline latch wait. The invoked Java fixture records the getter, response-before-`NullPointerException`, and wait failure outcomes (`3be59a09`). Core tests check the retained response/table entry and exactly-once reply races; the facade test checks the public nullable API. This closes only this request/reply behavior slice; broad P18-V01 source/API/input/error/control-flow and reverse-export reconciliation remains open.
- P18-V01 Common `MessageUtils` declaration follow-up (2026-10-16, pinned Java client source `efa1be59d6fc747ad220f036c86030f05f0419f9`): Java `common.utils.MessageUtils` declares four public methods, none of which had a Rust mapping. Common now exposes `get_sharding_key_index`, `get_sharding_key_index_by_msg`, `get_sharding_key_indexes`, and `delete_property`. A pure Murmur3-32 implementation matches Guava over UTF-8; property lookup uses the Java empty-key fallback; collection results deduplicate; deletion follows the Java U+0001/U+0002 encoded-string boundaries. The invoked `MessageUtilsFixtures` records all four declarations and checks the sharding hashes, zero-size exception, null/missing/empty values, set results, repeated and Unicode property deletion, partial-key matches, and a final entry without a separator. Rust `message_utils_match_pinned_java_551_fixture` covers the same outputs; `fixtures/messages/message-utils.json` SHA-256: `e01bed78a2a2290a82991fd68d308d77c283475e82b32d32d3939d045099f5f8`. Common tests, default/all-feature workspace tests, strict workspace Clippy, formatting, Maven verify (37 scenarios plus the new MessageUtils fixture), and diff checks pass on the current worktree. This closes the surfaced MessageUtils API slice only; full bidirectional P18-V01 remains open.
- P18-V01 Common `CorrelationIdUtil` declaration follow-up (2026-10-16, pinned Java client source `efa1be59d6fc747ad220f036c86030f05f0419f9`): Java `common.utils.CorrelationIdUtil` exposes `public static String createCorrelationId()`, returning `UUID.randomUUID().toString()`. Common now exports `utils::create_correlation_id`; Producer request preparation calls that shared function, and the redundant Producer-only `rand` dependency was removed. `RequestReplyFixtures` inventories the exact Java signature in `fixtures/producer/request-reply.json`; the Common integration test checks the declaration and lowercase UUID v4 text layout. Pinned Java fixture generation, the Common fixture test, and the Producer UUID format test pass. This closes the surfaced correlation-ID utility mapping only; full bidirectional P18-V01 remains PARTIAL.
- P18-V01 Common `NameServerAddressUtils` declaration follow-up (2026-10-16, pinned Java client source `efa1be59d6fc747ad220f036c86030f05f0419f9`): Common now exports the five endpoint/pattern values and all four public helper methods through `utils::name_server_address`. Java Pattern matching and its null/substring failure branches map to Rust match helpers and typed errors; `get_name_server_addresses` reads the exact Java property key before the `NAMESRV_ADDR` fallback. The invoked `RuntimeConfigFixtures` records all five public fields and four methods and covers null, empty, malformed, Unicode, path, and line-terminator inputs. Core ClientConfig endpoint extraction, instance validation, and ID parsing now call the same Common implementation. `name_server_address_public_surface_and_inputs_match_invoked_java`, the child-process property-precedence probe, all 21 Core config integration tests, all 168 Core library tests, and Common tests pass. Fixture SHA-256: `39b4944cf67a65dc9c86e203826ceb8e43b3650d72e4ad5cb19af9234f17e19c`. This closes only this Common utility slice; broad Phase 6/P18-V01 source/API/input/error/reverse-export reconciliation remains PARTIAL.
- P18-V01 Common `MessageAccessor.setProperties` caller-map alias follow-up (2026-10-16, pinned Java client source `efa1be59d6fc747ad220f036c86030f05f0419f9`): Java stores the supplied map object, so caller and message mutations remain visible through either reference. Rust adds `SharedMessageProperties`, `Message::set_properties_shared`, and `Message::shared_properties` to represent that behavior with a cloneable synchronized handle, while preserving the existing owned-map setter. The invoked Java fixture checks map identity and mutation in both directions; the Common differential checks the same visible outcomes, null key/value entries, and the root facade export. Fixture SHA-256: `02be176d528051f874a159a7fc2867484a8e1c2f8a7cffcf92c588ec8853db91`. Focused and full Common verification plus the complete Maven verify pass. This closes the caller-map alias slice only; broad P18-V01 remains open.
- P18-V01 Remoting `SemaphoreReleaseOnlyOnce` API slice (`978791b3`, 2026-10-16, pinned Java client source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): the public Java helper constructor, `release()`, and `getSemaphore()` map to `client::SemaphoreReleaseOnlyOnce::{new, release, semaphore}` using a nullable shared Tokio semaphore handle. The wrapper retains Java's one-release behavior under races and remains attached to `ResponseFuture` through explicit release; Tokio `OwnedSemaphorePermit` construction is private. `RemotingApiSurfaceFixtures` reflects the two `ResponseFuture` constructors and all 20 declared methods plus the helper's exact constructor/method signatures and release/null/identity outcomes. Fixture SHA-256: `4fc0509fee67637d2e1655a39dfb810ecac19e264289c6634448fa85f38aab4c`. The Remoting all-feature suite passes (105 unit, 8 codec, 1 fuzz, 7 security, 107 transport, 10 doctests), strict Remoting Clippy, and formatting/diff checks. This closes only the named helper/constructor surface; the broad P18-V01 audit remains PARTIAL.
- P18-V01 Remoting custom-header alias and factory behavior (`b6da737e`, 2026-10-17, pinned Java client source `efa1be59d6fc747ad220f036c86030f05f0419f9`): `SharedCommandHeader<T>` uses a cloneable synchronized handle so Java's caller and `RemotingCommand` observe mutations through either reference before and after header materialization. The invoked Java fixture also confirms that failed reflective construction of a response header (missing/inaccessible default constructor or constructor exception) returns null after consuming an opaque ID, while `buildErrorResponse` dereferences that null and raises `NullPointerException`. Rust maps those paths through fallible response/error header factories. Rust verifies bidirectional alias mutation, JSON and RocketMQ binary serialization, each construction-failure branch, and the request-ID delta. Fixture SHA-256: `ed7f13b1ba8b0d91ef6dd63683ff3630c9b8b9212ae2770484bdfc29f15469be`. Remoting all-feature tests pass (120 unit, 8 codec, 1 fuzz regression, 7 security, 107 transport, 16 doctests), strict all-target/all-feature Remoting Clippy passes, formatting/diff checks pass, and pinned Maven verify passes all 37 scenarios plus fixture drift and Java/Rust cross-decodes. This closes the custom-header alias/factory slice only; broad P18-V01 remains PARTIAL.
- P18-V01 RemotingCommand identity equality (`4eb99671`, 2026-10-17, pinned Java client source `efa1be59d6fc747ad220f036c86030f05f0419f9`): the invoked fixture reflects inherited `equals` and `hashCode` as `java.lang.Object`, compares two distinct commands with the same visible state, and observes both in a `HashSet`. Rust removes structural `PartialEq`/`Eq` from `RemotingCommand`; a compile-fail doctest guards the public trait surface, and codec/transport tests now assert wire fields or error variants directly. Fixture SHA-256: `a539bc204e2f2c03077af07dad37bc4a58c631ce2fa24d8e9e69f50999e4c9b0`. Remoting all-feature tests pass (123 unit, 8 codec, 1 fuzz regression, 7 security, 107 transport, 17 doctests); workspace default/all-feature tests and strict all-target/all-feature Clippy pass; format/diff checks and Maven verify (37 scenarios, fixture drift, and cross-decodes) pass. This closes only the RemotingCommand equality slice; broad P18-V01 remains open.
- P18-V01 Remoting `ProcessTimer` identity equality (`4858ebdd`, 2026-10-17, pinned Java client source `efa1be59d6fc747ad220f036c86030f05f0419f9`): Guava `Stopwatch` inherits `equals` and `hashCode` from `java.lang.Object`; the invoked fixture confirms equal stopped state, getter identity, and inequality/hash-set separation for distinct instances. Rust removes structural `PartialEq`/`Eq` from `ProcessTimer`, with a compile-fail doctest and pointer-identity assertion. Fixture SHA-256: `2680e925aa7c21cd5f2f5ea8055ee986bec5fc174601c612cb8ae469481d2cf0`. Linux Remoting all-feature tests, strict all-target/all-feature Remoting Clippy, format/diff checks, and Maven verify (37 scenarios, fixture drift and cross-decodes) pass. The macOS all-feature workspace run was interrupted after Rust compiler processes made no progress for more than eight minutes; it is not counted as a pass, and the current-revision P18-V02 workspace rerun remains open. This closes only the timer equality slice; broad P18-V01 remains PARTIAL.
- P18-V01 `SendMessageRequestHeaderV2` decode/conversion slice (`227876c1`, 2026-10-17, pinned Java client source `efa1be59d6fc747ad220f036c86030f05f0419f9`): invoked Java 5.5.1 fixtures capture nullable `a`–`n`, fast versus reflective inherited `RpcRequestHeader` fields, malformed numeric-field outcomes, and V1-to-V2 conversion/encoding. Rust now matches the mode-specific decode behavior, puts V1 `brokerName` in compact field `n`, does not copy inherited fields during conversion, and removes the Rust-only public `normalized_v1` helper. Fixture SHA-256: `dcf64f14fd84859e5a731d46368b59fc1c850bb511b38c6e16e398bacd153098`. Protocol/remoting targeted Linux tests, Maven verify (37 scenarios and fixture drift), formatting, and diff checks pass. This closes only the compact-header decoder/conversion slice; broad P18-V01 remains PARTIAL.
- P18-V01 `PullMessageResponseHeader` nullable and mode-specific decoding (`fcc0f3f1`, 2026-10-17, pinned Java client source `efa1be59d6fc747ad220f036c86030f05f0419f9`): the invoked Java 5.5.1 fixture records direct decode of missing nullable `Long`/`Integer` wrappers, valid numeric values, and invalid numeric values in both fast and reflective modes. Java fast decoding rejects malformed numbers; reflection decoding catches an individual conversion failure and leaves that field null. Rust response-header fields now preserve `Option` values and follow those mode-specific outcomes. The Pull RPC checks the required `nextBeginOffset`, `minOffset`, `maxOffset`, and `suggestWhichBrokerId` at the Java-equivalent unboxing boundary; other wrapper fields remain nullable. Fixture SHA-256: `9ba19f1b5394e7bd6e60366770990e034e75e98f2ecff04c82059d712dc8db59`. The fixture's direct-RPC case was corrected to expect Java's null-preserving decode, and the validation matrix was corrected to classify wrapper fields as nullable; Maven verify (37 scenarios, fixture drift and Java/Rust cross-decodes), Linux default/all-feature workspace tests, all-target/all-feature check and strict Clippy, formatting, and diff checks pass. This closes only this Pull response-header slice; broad P18-V01 and other header mode differences remain PARTIAL.
- P18-V01 `FastCodesHeader` decoder-mode source inventory (`fcc0f3f1`, 2026-10-17, pinned Java client source `efa1be59d6fc747ad220f036c86030f05f0419f9`): a read-only scan of the complete Java client `remoting` production sources found exactly four `FastCodesHeader` implementations: `PullMessageRequestHeader`, `PullMessageResponseHeader`, `SendMessageResponseHeader`, and `SendMessageRequestHeaderV2`. Each has an invoked Java 5.5.1 fast/reflection fixture and a Rust comparison covering valid and missing fields plus representative per-type numeric conversion failures; the Send V2 fixture also covers inherited-field behavior and V1 conversion. Rust defines `decode_fields_with_mode` for all four types. The P18-V02 fixture verification on `fcc0f3f1` passed. This closes the Java `FastCodesHeader` implementor inventory and those mode-specific slices; generic reflective decoding for other typed headers and the broad P18-V01 audit remain open.
- P18-V01 consumer offset typed-header reflection decode (code commit b59431e4, 2026-10-17, pinned Java client source efa1be59d6fc747ad220f036c86030f05f0419f9): QueryConsumerOffsetRequestHeader, UpdateConsumerOffsetRequestHeader, and QueryConsumerOffsetResponseHeader do not implement FastCodesHeader, so both useFastEncode hints take Java's generic reflective decoder. Their boxed/reference fields remain null when absent; reflection catches a missing or malformed field independently, and malformed setZeroIfNotFound follows Boolean.parseBoolean and becomes false. Their checkFields methods are no-ops despite CFNotNull annotations. Rust now keeps those values nullable, ignores per-field reflection failures, and validates QueryConsumerOffsetResponseHeader.offset at the consumer RPC result boundary. The invoked fixture covers missing, valid, malformed numeric, and malformed boolean cases in both hints. Fixture SHA-256: 2e911e6369ef5260ac73ad7ecbd3db9b38d190f8b080b5d36fc46997e7f20ddb. Maven verify and the current-revision P18-V02 gates pass on 3beabab0. This closes only these header decoder/API and owner-boundary slices; broad P18-V01 remains PARTIAL.
- P18-V01 Phase 9 offset-boundary response-header reflection decode (code commit 9513e4da, 2026-10-17, pinned Java client source efa1be59d6fc747ad220f036c86030f05f0419f9): `SearchOffsetResponseHeader`, `GetMaxOffsetResponseHeader`, `GetMinOffsetResponseHeader`, and `GetEarliestMsgStoretimeResponseHeader` use generic reflection rather than `FastCodesHeader`. Missing or invalid boxed `Long` fields remain null in either `useFastEncode` hint; the owning RPC fails when it unboxes a required result. Rust now preserves nullable fields on direct decode and validates them at those owner boundaries. The invoked fixture covers missing, valid, and malformed values and Java owner outcomes. Fixture SHA-256: c4891f046639eb0f9a82e221b1605d296aed1314fb70e37c22ea514d240a9e95. This closes these four header/owner slices only; broad P18-V01 remains PARTIAL.

P18-V01 `ChangeInvisibleTimeResponseHeader` public-surface closure (2026-10-17): pinned Java 5.5.1 exposes a public constructor, three primitive fields through getter/setter pairs, and inherits identity-based `equals`/`hashCode`. Rust now exports the corresponding `CommandHeader` type, encodes all three primitive values, and preserves per-field zero defaults for missing or malformed reflective inputs in both mode hints. The direct Java fixture also checks signed boundaries and Unicode decimal digits; Rust uses Java-compatible UTF-16 decimal parsing. Fixture SHA-256: `967d3146d0da1467714c96a1fa7464c7375888e2ba09f6fc0c097126bab67c2a`. Remoting's direct header fixture and Core's invoked POP/ACK callback regression pass. This closes the missing typed header surface only; broad P18-V01 remains PARTIAL.

P18-V01 Phase 17 query/admin header reflection slice (code commit `66334aa9`, 2026-10-18, pinned Java client source `efa1be59d6fc747ad220f036c86030f05f0419f9`): `QueryMessageRequestHeader`, `QueryMessageResponseHeader`, and `ViewMessageRequestHeader` are not `FastCodesHeader`; both decode hints use Java's per-field reflection conversion. Rust now models nullable Java `String`/boxed-number fields with `Option`, retains missing or malformed numeric values instead of failing decode, and removes value equality from `ViewMessageRequestHeader`. `QUERY_MESSAGE` validates the nullable index timestamp at the RPC result/unboxing boundary. The invoked fixture covers both hints and valid, missing, and malformed fields; SHA-256 `abc37ff23edd774910af9093b96fce95ce5e2270c5db7996fb0607df1ef3cc68`. Focused Protocol, Remoting, Core, Producer, and Consumer tests and strict Clippy pass; this closes only this decoder/owner-boundary slice and leaves broad P18-V01 PARTIAL.

P18-V01 `MqClientAdmin` public surface inventory (2026-10-18, pinned Java
client source `efa1be59d6fc747ad220f036c86030f05f0419f9`): source inspection
found 19 declared methods in `client/MqClientAdmin.java` and the public
`MQClientAPIExt.getMqClientAdmin()` accessor returning `MqClientAdminImpl`.
The existing Phase 17 matrix does not include this interface, and the Rust
`MQClientApi`/facade method inventory has no complete one-to-one crosswalk for
it. This is a newly confirmed P18-V01 audit gap, not a conclusion that every
method is absent: similar Rust methods exist for some operations, but request
types, results, error mapping and control flow have not been reconciled. The
19-method inventory and required follow-up are recorded in
`PHASE_17_ADMIN_MATRIX.md`; implementation and invoked Rust/Java evidence are
still required. This remains local parity work, not manual validation.

P18-V01 query/admin header continuation (`c091d507`, pinned Java client source `efa1be59d6fc747ad220f036c86030f05f0419f9`): `QueryTopicsByConsumerRequestHeader`, `QuerySubscriptionByConsumerRequestHeader`, and `QueryTopicConsumeByWhoRequestHeader` now share the Java `RpcRequestHeader` equality/hash contract; the invoked identity fixture and Rust tests cover 35 request-header classes. Fixture SHA-256: `2849daef72f0bbc29c64f311b8dc41761602845ef1256f12c6937e2ccdbea48f`. The new `queryTopicConsumeByWho` RPC uses request code 300 and a Java-backed `GroupList` DTO fixture (SHA-256 `d033bf2e2cf2e866ba854bf2152ab21f66f43a2bb0212caaf9dbdc4d88f5c38e`). This closes those query/admin header and RPC slices only; broad P18-V01 and Phase 6 API/source/input/platform reconciliation remain open, and P6-V04 remains a Phase 19 handoff.

P18-V01 follow-up — `MqClientAdmin` direct RPC inventory closure (2026-10-18):
the earlier discovery above is historical. The Phase 17 matrix now records
source-backed typed RPC slices and invoked Java/Rust evidence for all 19
declared `MqClientAdmin` operations; `getConsumerRunningInfo` closed the final
method slice in code commit `f05833a7`. Later DTO API work includes the
`QueueTimeSpan`/`QueryConsumeTimeSpanBody` slice in code commit `423bcdbc`.
There is no remaining missing direct RPC method in this 19-method inventory.
The broader P18-V01 audit remains open for exhaustive public DTO/API,
settings, input/error/flow, and Rust reverse-export reconciliation.

- P18-V01 Lite topic-info request-header fixture closure (`d578db50`, pinned
  Java 5.5.1): `GetLiteTopicInfoRequestHeader` already had a Core RPC mapping
  and Rust header type, but the typed-header reverse inventory omitted it.
  Added the invoked Java frame, Java API/nullability assertions, Rust header
  inventory entry, validation-matrix row, and reverse-frame generator entry.
  The Java cross-decode gate now requires exactly 67 typed-header fixtures,
  so a future inventory omission cannot pass the former minimum-count check.
  Maven verify passes all 37 scenarios, 67 typed-header frames, and 15 typed
  body frames; Protocol header inventory and header tests pass. Frame fixture
  SHA-256: `92e15800cdfb810908a8a016582189b5523bdc2d7dbbfc8ff69523f2a7184884`.
  This closes only this header evidence gap; the broad bidirectional P18-V01
  audit remains open.

- P18-V01 Common reverse-export correction (2026-10-19, pinned Java client
  source `efa1be59d6fc747ad220f036c86030f05f0419f9`): Java
  `MessageDecoder` declares `MESSAGE_MAGIC_CODE` and `MESSAGE_MAGIC_CODE_V2`,
  but no `MESSAGE_MAGIC_CODE_V1`. Rust now keeps the V1 discriminator private
  and exposes the Java-named alias only. The direct Java API fixture still
  checks both public magic constants; a compile-fail doctest guards the
  removed export. This closes this constant mismatch only; the aggregate
  P18-V01 reverse-export reconciliation remains in progress.

- P18-V01 Lite topic-info `ClientGroup` field mutability correction (2026-10-19,
  pinned Java 5.5.1 fixture): Java exposes `clientId` and `group` as public
  `final String` fields, with no setters. Rust now keeps the values private and
  exposes read-only `client_id()` and `group()` accessors; compile-fail doctests
  guard direct reads and mutation. The protocol fixture test confirms the
  constructor values and nullable representation remain intact. This closes
  only the extra Rust mutation capability on `ClientGroup`; broad P18-V01
  reverse-export reconciliation remains open.

- P18-V01 cross-crate Rustdoc field inventory (2026-10-19): generated
  workspace Rustdoc JSON and enumerated public named-struct fields in Common,
  Consumer, Core, Producer, Protocol, and Remoting. The counts were 4, 30, 66,
  32, 58, and 55 (245 total across 41 structs); there are no named public
  enum-variant fields. The handoff's stated total of 340 was an addition error.
  After the `ClientGroup` correction, 243 public fields remain. The map records
  each type/count and its Java setting/result/hook/route disposition. The
  reviewed remoting `ClientConfig` fields map `NettyClientConfig`, and the
  remaining mutable bean-like values provide the same read/write properties
  through idiomatic Rust fields. This closes the field inventory and the
  surfaced `ClientGroup` mutability gap only; broad P18-V01 method, input,
  error, flow, and reverse-export reconciliation remains open.

- P18-V01 Producer permission validation and Broker config operation slice
  (2026-10-21): pinned Java `Validators`/`PermName` reflection and input fixtures
  exposed two permission checks and the `MQClientAPIImpl.updateBrokerConfig`
  request path absent from the Rust mapping. Common now maps all permission
  constants and methods, Producer exposes the validator counterparts, and Core
  validates topic permission and implements the Broker config call. Scripted
  tests cover Java exception text, malformed integers, no-op properties,
  request bytes, VIP routing and Broker errors. See the [Phase 6 API map](PHASE_6_API_CONFIG_MAP.md),
  [Phase 7 producer matrix](PHASE_7_PRODUCER_MATRIX.md), and release checklist.
  This closes those bounded items only; broad P18-V01 method, setting,
  overload/input/error/flow and reverse-export reconciliation remains open.

- P18-V01 adjacent Broker config read operation (2026-10-21): the pinned
  public `MQClientAPIImpl.getBrokerConfig(String,long)` method was absent from
  the runtime RPC crosswalk. Rust now sends request 26 without a header/body,
  preserves the supplied Broker address, parses Java Properties semantics,
  and retains Broker response errors. The Java fixture covers empty input,
  escaped separators, Unicode escapes, malformed escapes, raw non-ASCII text,
  and a trailing backslash; the Rust parser and scripted RPC tests compare the
  outcomes. An absent response body maps to structured `MalformedResponse`,
  while Java dereferences null before property parsing. At this operation
  checkpoint the fixture SHA-256 was
  `53f8d3ae387f5df155d0c10484815f3a71d00debafb70c82ed0912f3848bca2f`.
  This closes the named operation/property-input slice only; broad P18-V01 and
  the final current-worktree P18-V02 refresh remain open.

- P18-V01 adjacent cold-data flow and read-ahead RPCs (2026-10-21): pinned
  source review found four neighboring public `MQClientAPIImpl` operations
  absent from the Core API map. Rust now maps request codes 2001–2004,
  including VIP selection on the two mutation calls, empty/null no-op inputs,
  the `READ_AHEAD_MODE` field, nullable body/remark results, and Broker error
  handling. The Java fixture directly invokes all four methods and records
  request envelopes and response outcomes; Rust scripted RPCs compare them,
  and a VIP-enabled loopback test checks update/remove address transformation.
  Java's null read-ahead field is omitted by the wire encoder; Rust's nullable
  adapter emits no ext fields for that input. At this slice checkpoint, fixture SHA-256 was
  `c20f4474d674bd4e0001f19c82fed8c5e2dfbe6f5c911ee6eade0fb1842c7cb0`.
  The high-level `rocketmq-tools` facade stays outside Phase 17. This closes
  these low-level RPC operations only; broad P18-V01 and the final current-tree
  P18-V02 refresh remain open.

- P18-V01 `registerClient` heartbeat result contract (2026-10-21): the pinned
  public `MQClientAPIImpl.registerClient(String, HeartbeatData, long)` method
  shares request 34 and the encoded heartbeat body with `sendHeartbeat`, but
  passes the supplied address directly and returns a boolean for `SUCCESS`;
  all other Broker response codes return `false`. Core now provides the
  separately named `register_client` API rather than reusing
  `send_heartbeat`, whose Java-facing contract returns the Broker version and
  raises on non-success. The Java invocation fixture and Rust scripted RPC
  compare signature, body, empty header, timeout, and both response branches.
  At this slice checkpoint, producer core fixture SHA-256 was
  `434a3fe8b1cba71732f41ccb710e601e0d8ce4cbcf196265695cef7641b90905`.
  This closes the direct registration method only; broad P18-V01 and the final
  current-worktree P18-V02 refresh remain open.

- P18-V01 `getTopicListFromNameServer` (2026-10-21): the pinned public
  `MQClientAPIImpl.getTopicListFromNameServer(long)` method sends request 206
  through the configured NameServer selector with no header or body. Core now
  decodes `TopicList`, retaining its topic set and Broker address, and preserves
  response errors. The direct Java invocation fixture and Rust scripted
  NameServer differential compare signature, request envelope and success and
  error outcomes. A missing body on `SUCCESS` maps to structured
  `MalformedResponse`; Java raises `MQClientException` with code 0. At this
  slice checkpoint, producer core fixture SHA-256 was
  `6e0a6f94f934f9e173aa76a5e4e3ab18c70ee1121f41f1dfbb52fbbac303a2ef`.
  This closes the single NameServer API call only; `getSystemTopicList` and the
  broad P18-V01 audit remain open.

- P18-V01 `getSystemTopicListFromBroker` (2026-10-21): the pinned public
  `MQClientAPIImpl.getSystemTopicListFromBroker(String,long)` method sends
  request 305 without a header or body, applies the configured VIP address
  transform and decodes `TopicList`. Core now exposes the direct Broker RPC;
  the Java invocation fixture and Rust differential compare the exact
  signature, request envelope, topic set, Broker error and missing-body branch;
  Java reports `MQClientException` with code 0 for `SUCCESS` without a body,
  while Rust returns `MalformedResponse`. A VIP-enabled Core loopback confirms
  the address transform. At this slice checkpoint, producer core fixture
  SHA-256 was `fbe6f70615e8004c26bdba90fdc824241415d19557b57853a6a563f24becbd32`.

- P18-V01 `getSystemTopicList` (2026-10-21): the pinned public
  `MQClientAPIImpl.getSystemTopicList(long)` sends request 304 through the
  NameServer path, then calls request 305 only for a nonempty NameServer topic
  set and nonblank Broker address. Core now preserves the NameServer set handle
  while merging nonempty Broker topics. The Java fixture and Rust differential
  compare both request envelopes, call order, timeout, merge, short-circuit
  cases, NameServer/Broker errors and missing-body handling. Current producer
  core fixture SHA-256 at this slice checkpoint was
  `38b2eac88f25aa04674f469b4e7a685dc599834969f6a1d24613de8c98efdb23`.
  The broader P18-V01 public API inventory remains open.

- P18-V01 `createSubscriptionGroupList` (2026-10-21): the pinned public
  `MQClientAPIImpl.createSubscriptionGroupList(String,List,long)` sends request
  225 with a `SubscriptionGroupList` body to the configured VIP Broker address.
  Core now serializes the Java body and preserves Broker response errors. The
  invoked Java fixture and Rust differential compare populated/empty bodies,
  the exact request bytes and success/error outcomes; a VIP-enabled Core
  loopback confirms the address transform. Current producer core fixture
  SHA-256: `2479e9af49895298bf807b04775db0e3acef6ad2b6a0a6e2cf59a830ca89169e`.
  The broader P18-V01 public API inventory remains open.

- P18-V01 `getTopicsByCluster` (2026-10-21): the pinned public
  `MQClientAPIImpl.getTopicsByCluster(String,long)` sends request 224 through
  NameServer selection with the cluster in its request header. Its no-op
  `checkFields()` accepts a null cluster, which Java omits from the wire
  ext-fields map; Rust exposes `get_topics_by_cluster_nullable` for that case.
  Core decodes `TopicList` and preserves response errors. The invoked Java
  fixture and Rust differential compare the signature, NameServer request,
  nonnull/null cluster values, success, response error and missing-body
  outcomes; Java raises
  `MQClientException` with code 0 for `SUCCESS` without a body, while Rust
  returns structured `MalformedResponse`. Current producer core fixture
  SHA-256: `ac3462b7351f327839c4a79db5a8a499f6fdbbd56f667f74b2ac5f18fb368fee`.
  The broader P18-V01 public API inventory remains open.

- P18-V01 `createTopicList` (2026-10-21): the pinned public
  `MQClientAPIImpl.createTopicList(String,List,long)` sends request 18 with a
  `CreateTopicListRequestBody` to the configured VIP Broker address. Core now
  encodes Java-compatible `TopicConfig` values and preserves null-list and
  null-element serialization. The invoked Java fixture and Rust differential
  compare populated/default/custom, empty, null-list and null-element bodies,
  success/error responses and exact request bytes; a VIP-enabled Core loopback
  confirms the port transform. Current producer core fixture SHA-256:
  `fcf27463aa5fde04867de50543847ad53dabc1b5c3b526ae8ac2406481514ec7`.
  The broader P18-V01 public API inventory remains open.

- P18-V01 batch delete RPCs (2026-10-21): the pinned public
  `MQClientAPIImpl.deleteTopicInBrokerList(String,List,long)` request 5002 and
  `deleteSubscriptionGroupList(String,List,boolean,long)` request 5003 both send
  body DTOs through the configured VIP Broker address. Rust now preserves topic
  and group-list body encoding, including null lists and null entries, plus the
  group's `cleanOffset` flag. Invoked Java fixtures and Rust differential tests
  compare signatures, exact bodies, empty headers, success/errors and nullable
  inputs; a VIP-enabled loopback checks both port transforms. Current producer
  core fixture SHA-256: `33b7788c15092caaeb2e5cc42ac69eb7bcda5baf5ebc27c2caf01aac9c5e9ff3`.
  The broader P18-V01 public API inventory remains open.

- P18-V01 unit topic-list RPCs (2026-10-21): the pinned public
  `MQClientAPIImpl.getUnitTopicList(boolean,long)`,
  `getHasUnitSubTopicList(boolean,long)`, and
  `getHasUnitSubUnUnitTopicList(boolean,long)` methods send requests 311, 312
  and 313 through NameServer selection without headers or bodies. Core maps all
  three methods and preserves Java's local filtering rule: false `isUnitMode`
  removes `%RETRY%` topics; true leaves them in the returned `TopicList`. The
  invoked Java fixture and Rust differential cover both boolean values for all
  three methods, response errors, and missing-body behavior. Java reports
  `MQClientException` code 0 for a missing successful response body; Rust
  reports structured `MalformedResponse`. Producer core fixture SHA-256:
  `bb5a19cb131f922828ff5effb8eb52ac4a0d95e8bae2863775214da4d6664e5b`.
  This closes these three RPC mappings only; the broad P18-V01 inventory
  remains open.

- P18-V01 NameServer configuration RPCs (2026-10-21): the pinned public
  `MQClientAPIImpl.updateNameServerConfig(Properties,List,long)` request 318
  sends serialized properties to each selected server in list order. Empty
  Properties are a no-op; missing/empty address lists use the configured
  NameServers; all responses are visited and the last non-success response is
  reported. `getNameServerConfig(List,long)` request 319 reads each selected
  server in order, parses its Properties, and stops at the first response
  error. Core maps both operations, including absent/default address lists,
  empty/no-server paths, malformed property values retained as null map
  entries, and the missing-body branch (Java throws `NullPointerException`; Rust
  returns structured `MalformedResponse`). Invoked Java fixtures and Rust
  differentials check signatures, requests, body values, per-server results,
  call order and error selection. Producer core fixture SHA-256:
  `4276123552ab57632da4a10177b1b529266707234e95785668cfcd12177e783c`.
  These two RPC mappings are closed; the broader P18-V01 inventory remains
  open.

- P18-V01 `resumeCheckHalfMessage` (2026-10-21): the pinned public
  `MQClientAPIImpl.resumeCheckHalfMessage(String,String,String,long)` sends
  request 323 with nullable `topic` and `msgId` fields to the configured VIP
  Broker address. It returns `true` only for `SUCCESS`; every other Broker
  response returns `false`. Core now exposes required and nullable Rust
  adapters with that behavior. Invoked Java fixtures and a Rust loopback
  differential compare the signature, request fields, empty body, success and
  non-success result, nullable fields and VIP address transform. Producer core
  fixture SHA-256: `bbeea411ebb9a1cbf177a4f0efddbf85023a475288bd123764959acca248efc6`.
  This closes the named RPC mapping only; the broader P18-V01 inventory remains
  open.

- P18-V01 `exportPopRecord` (2026-10-21): the pinned public
  `MQClientAPIImpl.exportPopRecord(String,long)` sends request 200056 (the
  registry's `POP_ROLLBACK` name) with no header or body, passes the supplied
  Broker address unchanged even when VIP mode is enabled, and reports non-
  success response codes as Broker errors. Core now exposes `export_pop_record`
  with those semantics. Invoked Java fixtures and a VIP-enabled Rust loopback
  compare the signature, exact address behavior, request envelope, success and
  Broker rejection. Producer core fixture SHA-256:
  `d94fd1f95eba2caf6ce77e312cd53a4109be74048b23db5ad694a2c5b7109242`.
  This closes the named RPC mapping only; the broader P18-V01 inventory remains
  open.

- P18-V01 `switchTimerEngine` (2026-10-21): the pinned public
  `MQClientAPIImpl.switchTimerEngine(String,String,long)` sends request 5001
  with `E_T` in the ext-fields map, including an empty string, uses the supplied
  Broker address unchanged with VIP enabled, and reports non-success replies as
  Broker errors. Core now exposes `switch_timer_engine` with those semantics.
  Invoked Java fixtures and a VIP-enabled Rust loopback compare the signature,
  field value, request body, address and response outcomes. Producer core
  fixture SHA-256:
  `bb28b038fdd66160007d8c757e53ed4cb78ebcad0a91b4ff9d950fa04eccee6c`.
  This closes the named RPC mapping only; the broader P18-V01 inventory remains
  open.


- P18-V01 direct `createSubscriptionGroup` RPC (2026-10-21): the pinned public
  `MQClientAPIImpl.createSubscriptionGroup(String,SubscriptionGroupConfig,long)`
  sends request 200 synchronously, serializes the nullable config as a body,
  applies the configured VIP transform, and returns on `SUCCESS` or reports a
  Broker response error. Rust exposes `MQClientApi::create_subscription_group`
  with `Option<&SubscriptionGroupConfig>` so Java's null-config/no-body path is
  preserved. This is distinct from asynchronous
  `MqClientAdmin.updateOrCreateSubscriptionGroup`, which sends to the supplied
  address unchanged. Invoked Java cases and Rust scripted RPC checks cover the
  exact signature, populated/default/null bodies, success/error outcomes, and a
  VIP-enabled loopback. Producer core fixture SHA-256:
  `aaab68664cf25ff86c9b19a48c27e4691596b31124123fdbd83f7ceb93f89b57`.
  This closes the direct RPC only; broad P18-V01 remains open.

- NameServer KV direct API closure (`2026-10-21`): added the missing low-level `MQClientAPIImpl.getKVConfigValue`, `putKVConfigValue`, `deleteKVConfigValue`, and `getKVListByNamespace` operations as `MQClientApi::{get_kv_config_value,put_kv_config_value,delete_kv_config_value,get_kv_list_by_namespace}`. The direct put/delete paths broadcast to all configured NameServers and preserve Java's last-error selection; get/list retain nullable results. Added typed headers for the pinned Get/Put/List requests and Get response, plus Java→Rust and Rust→Java frame fixtures. `fixtures/producer/core.json` hashes to `7e6aecbca17b9711ed3276de4aae2c500fdd8f90bdeb1bde7a7c59b0d3c402af`. The focused Core differential passes, Protocol tests pass (148 including doctests), strict all-feature workspace Clippy passes, and Maven `verify` passes with 37 RPC cases, 72 typed headers and 15 typed bodies. This closes the NameServer KV slice only; broad P18-V01 source/API/settings/input/error/flow/reverse-export reconciliation remains open.

- P18-V01 Broker maintenance RPCs (2026-10-21): direct public methods
  `MQClientAPIImpl.cleanExpiredConsumeQueue`, `deleteExpiredCommitLog`, and
  `cleanUnusedTopicByAddr` now map requests 306, 329, and 316. Java uses empty
  request envelopes and configured VIP address conversion; `SUCCESS` returns
  true and other response codes throw `MQClientException`. Java invoked
  fixtures and the Rust differential compare signatures, request codes,
  Java language, empty fields/body, and both response outcomes. A VIP-enabled
  loopback checks the address offset. Producer core fixture digest and final
  per-slice validation are in the release checklist. The broader P18-V01
  source/API/settings/input/error/control-flow/reverse-export audit remains
  open.

- P18-V01 direct subscription-group configuration lookup (2026-10-21): the
  pinned `MQClientAPIImpl.getSubscriptionGroupConfig(String,String,long)` now
  maps request 352 with nullable group input, inherited `RpcRequestHeader`
  fields, configured VIP routing, nullable `SubscriptionGroupConfig` response
  bodies, and Broker error mapping. Added the header model to the Java/Rust
  header inventory and reverse-frame fixtures. Invoked Java cases and Rust
  differentials cover populated/default body decoding, null/empty group, absent
  body, request fields, and Broker rejection; a VIP loopback checks the address
  transform. The current fixture digest and full gate evidence are recorded in
  the release checklist. Broad P18-V01 remains open.

## Direct `getClusterList` stub — 2026-10-21

The pinned public `MQClientAPIImpl.getClusterList(String,long)` ignores both
arguments, sends no request, and returns the shared immutable Java empty set.
Core maps this as `MQClientApi::get_cluster_list`; invoked Java cases and the
Rust fixture-backed regression compare the empty result, singleton behavior,
mutation rejection, and absence of RPCs. This closes the named stub only.
Broad P18-V01 source/API/settings/input/error/flow/reverse-export review and
the platform evidence gates remain open.

## Direct `resetMasterFlushOffset` API — 2026-10-21

The pinned public `MQClientAPIImpl.resetMasterFlushOffset(String,long)` maps
request 908 with the supplied address unchanged, `masterFlushOffset` as a
signed decimal field, and Java's fixed 3000 ms timeout. `SUCCESS` returns;
other Broker responses preserve the response error. Rust includes the
`ResetMasterFlushOffsetHeader` public accessor surface and nullable reflection
decoding. Java fixtures and Rust tests compare a maximum signed-long value, a
negative value, the typed frame, timeout, address, and both outcomes. This
closes the direct API/header slice only; broad P18-V01 reconciliation and
platform evidence gates remain open.

## Direct `exportRocksDBConfigToJson` API — 2026-10-21

Pinned `MQClientAPIImpl.exportRocksDBConfigToJson(String,List,long)` sends
request 355 to the supplied address with the supplied timeout. The Rust header
and enum map Java's nullable `configType`, type-name conversion, case-insensitive
trimmed parsing, and trailing-semicolon serialization. `SUCCESS` returns;
other Broker response codes preserve the response error. The Rust nullable-list
adapter rejects a null list before any transport request. Invoked Java fixtures,
typed header frames, Protocol tests, and the Core loopback comparison cover the
API signature, parser results, request envelope, empty/null lists, and both
response outcomes. This closes the named API/header slice only; broad P18-V01,
P18-V02 and platform evidence gates remain open.

## Direct NameServer write-permission APIs — 2026-10-21

Pinned `MQClientAPIImpl.wipeWritePermOfBroker(String,String,long)` and
`addWritePermOfBroker(String,String,long)` map requests 205 and 327. Both send
the nullable `brokerName` header to the supplied NameServer address with the
caller timeout, then unbox `wipeTopicCount` or `addTopicCount` on `SUCCESS`.
Rust preserves nullable and empty input, returns the signed `i32`, and reports
missing/malformed success counts as malformed responses. Invoked Java
operations, four typed header frames, decode-mode comparisons and a Core
loopback test cover the signature, request envelope, result, and failure
branches. This closes the named direct method/header slices only; broad
P18-V01, P18-V02 and platform evidence gates remain open.

## Direct `checkRocksdbCqWriteProgress` API — 2026-10-21

Pinned `MQClientAPIImpl.checkRocksdbCqWriteProgress(String,String,long,long)`
sends request 354 to the supplied Broker address and parses
`CheckRocksdbCqWriteResult` from the nullable response body. Rust maps the
request header, nullable result DTO and `CheckStatus` enum, including primitive
defaults, direct address/deadline use, Broker errors, malformed JSON and
successful null/absent/empty bodies. Java invoked fixtures, Java→Rust and
Rust→Java header frames, both header decode modes, Protocol tests and a Core
loopback comparison cover the named mapping. Producer core fixture SHA-256:
`6f853c6e49ad04a2fe2d1d7d202c992d1088e17da1fc248330bef7de968603a0`. This
closes the direct API/header slice only; broad P18-V01, final P18-V02 and
platform evidence gates remain open.

P18 follow-up to the Phase 17 tools boundary note: both
`exportRocksDBConfigToJson` and `checkRocksdbCqWriteProgress` are now mapped as
public low-level `MQClientAPIImpl` operations. The Phase 17
`TOOLS_OUT_OF_SCOPE` disposition continues to apply to their `MQAdminExt`
wrappers; it does not remove either client operation from the Phase 18
source/API inventory.

## P18-V01 caller-defined `MessageExt` subtype adapter — 2026-10-21

Rust now provides `MessageExtSubtype` for caller-defined runtime message
subtypes passed through base-typed APIs. The adapter retains custom fields for
typed callback downcasts and dispatches selected Java-virtual topic, broker,
queue ID, message ID, setter and display methods. The invoked Java fixture's
`concurrent-custom-subtype` case checks topic assignment, retry-topic restore,
namespace removal, broker assignment, callback-visible custom values and the
subclass display string; the Rust direct-consume fixture matches those
observations. Fixture SHA-256:
`4246e4a2563d79e9c8e78b2405941a850ad904d48e121e0b3fe009e9ae366263`.
Subtype state is runtime-only and excluded from serialization. This closes
the direct-consume custom-field and selected-override slice. The full inherited
`Message`/`MessageExt` override inventory, broad P18-V01 audit and P18-V04
platform evidence remain open.

## P18-V01 Push `ConsumeMessageService` replacement seam — 2026-10-21

`DefaultMQPushConsumerImpl.get/setConsumeMessageService` now maps to public
Rust `PushConsumer` accessors and a public `ConsumeMessageService` trait with
the nine methods inventoried from pinned Java 5.5.1. Replacements receive
direct-consume requests, normal Pull submissions, accepted pool updates,
thread-adjustment hooks and shutdown. POP dispatch remains on the separate
internal POP service, matching Java's `consumeMessagePopService`; replacing the
primary service does not intercept POP submissions. The Java fixture records
all service method signatures, and Rust API/owner tests cover identity/null
access, replacement routing, POP separation and shutdown. `ProcessQueue` removal
also now removes in-flight entries and updates cached bytes using the supplied
message body, matching Java's `removeMessage` map behavior. Focused accessor,
owner-flow and in-flight-removal tests pass; fixture SHA-256 is
`2ceab3283085fa8335cd3e7a965c1698e10960ae720c76b148af4cb52a196b4b`.

This closes the named raw service-replacement seam only. Broad P18-V01
source/API/settings/input/error/control-flow/reverse-export reconciliation,
`MessageExt` virtual methods beyond the selected subtype adapter hooks, and
P18-V04 platform evidence remain open.

P18-V01 caller-defined subtype metadata follow-up (2026-10-21):
`MessageExtSubtype` now dispatches Java virtual born/store timestamp, born-host,
sysFlag, body-CRC, commit-log-offset, store-size and prepared-transaction
offset accessors. The invoked direct-consume fixture checks the custom values
and eight setter calls. It also distinguishes the virtual host getters from
Java's host string/name/byte helpers, which read the stored host fields
directly. Fixture SHA-256:
`aa202880244ea5606fbebc63d8558965fddede4a95be58baacd56320f8f16571`.
Other inherited methods, the broad P18-V01 audit and P18-V04 platform evidence
remained open at this checkpoint; the live `getProperties()` map identity gap
was closed by the later `158a049f` follow-up below.

P18-V01 caller-defined subtype transient sysFlag follow-up (2026-10-21):
`topic_sys_flag()` and `group_sys_flag()` now use the subtype's virtual
property hook, matching the pinned Java getter delegation; both Java setter
methods also dispatch to subtype hooks. The direct-consume fixture compares
custom values and setter counts. Fixture SHA-256:
`71cf67f0bf813630fe084e2be78ea4275a1f04f49063c88a83606e909bb47dbe`.

P18-V01 inherited message-property follow-up (code revision `c6048c18`,
2026-10-21): `MessageExt` now routes wait-store, buyer ID, delay-level,
delay-seconds, delay-milliseconds and delivery-milliseconds getters through
the subtype's virtual `getProperty`. Wait-store, buyer ID, instance ID and the
four delay/delivery setters dispatch through subtype hooks. The invoked
Java/Rust direct-consume fixture compares these getter values and seven setter
calls. Fixture SHA-256:
`5148df4ac22baa84eb75326238a8d5903927ba1c0380dc34b7cc4093a47bf446`.
The remaining inherited override inventory, broad P18-V01 audit and P18-V04
platform evidence remained open at this checkpoint; the live custom
`getProperties()` map identity gap was later closed by `158a049f` below.

P18-V01 inherited user-property and transaction metadata follow-up (code
revision `27c872bc`, 2026-10-21): subtype hooks now cover `getUserProperty`,
`hasProperty`, transaction-ID accessors, `putUserProperty`, `setTags` and
`setKeys`. The Collection `setKeys` path dispatches through the String setter.
The invoked Java/Rust fixture verifies custom getter results and setter counts,
including blank and null inputs handled by an overridden `putUserProperty`.
Fixture SHA-256:
`33de50fa577340aa50df1637f1d2f6aa32ae8d23e5e03ba21a2a012630e37542`.
Other inherited methods, broad P18-V01 and P18-V04 platform evidence remained
open at this checkpoint; custom live property-map identity was later closed by
`158a049f` below.

P18-V01 MessageExt IPv6 host-flag setters (code revision `c7354649`,
2026-10-21): `setBornHostV6Flag` and `setStoreHostAddressV6Flag` now dispatch
to subtype hooks. The Java/Rust direct-consume fixture confirms both virtual
calls; base messages still set their corresponding sysFlag bits. Fixture
SHA-256: `7cf4e8040b8e040117207c8819d67f1dd7bf7f9f0daa6374ce0f7c345bf9171c`.
Other inherited methods, broad P18-V01 and P18-V04 platform evidence remained
open at this checkpoint; custom live property-map identity was later closed by
`158a049f` below.

P18-V01 live subtype `getProperties()` map follow-up (code revision
`158a049f`, 2026-10-21): `MessageExtSubtype::live_properties` now exposes a
borrowed `SharedMessageProperties` handle. `MessageExt` property views,
nullable entries and `shared_properties()` use that handle, so mutations
through either map reference remain visible and repeated getter reads observe
the same contents. The direct Java/Rust consumer fixture checks map identity,
mutations through the returned map and subtype field, and visibility through a
later getter. The older snapshot hook remains available for subtypes that do
not expose a live map. Fixture SHA-256:
`8782177ecb34305bc00642fffa09420c1fd12ecfe4c8744d450bd4a290ff0d01`.
Other inherited methods, broad P18-V01 reconciliation and P18-V04 platform
evidence remain open.

P18-V01 `MessageExt` born-host string getter override follow-up (code revision
`7473872c`, 2026-10-21): `MessageExtSubtype` now dispatches virtual
`getBornHostString()` and `getBornHostNameString()` overrides, while the base
path continues to derive both results from its stored born-host address. The
Java/Rust direct-consume fixture overrides both getters and compares their
custom values. Fixture SHA-256:
`dec0f7c46539bec116f6430c7b02fdde406a289929fc093d5694dd4c9369bd54`.

P18-V01 `MessageExt` born/store host-byte override follow-up (code revision
`625e80c6`, 2026-10-21): `MessageExtSubtype` now dispatches the virtual
no-argument `getBornHostBytes()` and `getStoreHostBytes()` results and their
`ByteBuffer` overloads. The invoked Java/Rust direct-consume fixture compares
both returned byte arrays and caller-buffer writes. Rust maps each Java
`ByteBuffer` overload to a caller-provided mutable byte slice and returns the
written byte count; Java buffer identity and position metadata are not
represented. Fixture SHA-256:
`30af1bcbb2cfbf967f636fbbf7cbf9d54ba886a68c25fbe92df5ddf88000946c`.
Other inherited methods, broad P18-V01 reconciliation and P18-V04 platform
evidence remain open.

## Direct Broker registration APIs — 2026-10-21

The pinned Java `MQClientAPIImpl.addBroker` and `removeBroker` methods map to
requests 902 and 903. Rust's Core API preserves their nullable request-header
fields, signed 64-bit broker ID, supplied-address behavior, caller timeout,
success result, and Broker error boundary. Invoked Java fixtures, typed
header cross-decodes, loopback request checks, and delayed-peer timeout
coverage exercise the operations. Producer core fixture SHA-256:
`9510b46415e7f00c7932c95d9e33dbb02e9b9cc56d1afecec01dfede60ae5e6a`.
Default and all-feature workspace tests pass on the current code revision;
pinned Maven `verify` passes with 37 scenarios, 85 typed headers, and 16 typed
bodies. This closes the direct Broker registration slice; broad P18-V01
remains open.

## P18-V01 direct `cloneGroupOffset` Broker API — 2026-10-22

The pinned Java client method sends request 314 using nullable source group,
destination group, and topic fields, a primitive `offline` value, the configured
VIP-channel transform, and the supplied timeout. Rust now provides
`MQClientApi::clone_group_offset` and a typed request header preserving Java's
inherited RPC fields and no-op validation. Java invocation fixtures and a Rust
loopback test compare exact request fields, null inputs, success, and response
errors; typed header cross-decodes cover all fields. Producer core fixture
SHA-256: `c7895cfff96e0150878d761336c34b4ddfec8064249d4af994a3f6796959a8d9`.
This closes this surfaced operation only; exhaustive P18-V01 API/input/reverse
export reconciliation remains open, as do P18-V04 native Windows and current
revision GitHub CI evidence.

## P18-V01 direct Broker consumer-status API — 2026-10-22

`MQClientAPIImpl.invokeBrokerToGetConsumerStatus` now maps to
`MQClientApi::invoke_broker_to_get_consumer_status`. The implementation keeps
nullable request strings, request 223, the configured VIP conversion and
caller timeout, and decodes the nullable `consumerTable` returned by
`GetConsumerStatusBody`. Its invoked fixture and loopback test cover populated
and empty results, null inputs, error responses, and success without a body.
Producer core fixture SHA-256:
`7de333d7c61d4235a6e134e5bdf7dc97cf03c12024dd320b9b96ff3a02beb183`.
This closes this surfaced operation only; the remaining direct client API,
settings, input/error, control-flow and reverse-export audit remains open.

## P18-V01 pinned `MQClientAPIImpl` reverse-audit remainder — 2026-10-22

The pinned Java 5.5.1 `MQClientAPIImpl` public surface was checked with
`javap -public` against the current Rust APIs and compatibility evidence.
The initial candidate list contained 18 direct operations with no mapped Rust
operation or fixture-backed owner path. Since this list was captured,
`queryCorrectionOffset`, `updateAndGetGroupForbidden`, `getBrokerHAStatus`,
`fetchConsumeStatsInBroker`, `viewBrokerStatsData`, and `getTopicConfig` have
been implemented and checked against the pinned Java fixture. The initial list
of 12 direct operations has since been reduced by the `getAllTopicConfig`,
`getAllSubscriptionGroup`, `queryConsumeQueue`, `createStaticTopic`,
`createUser`, `updateUser`, `deleteUser`, and `listUser` slices below; 4 of
that original candidate list remained: `createAcl`, `updateAcl`, `deleteAcl`,
and `listAcl`. Those four slices are now closed below. A pinned-source review
of the adjacent auth methods also found `getUser` and `getAcl` were omitted
from the candidate list; the current table now has 0 open operations.

| Request | Java method | Code | Status |
| --- | --- | ---: | --- |
| Topic configuration snapshot | `getAllTopicConfig` | 21 | Closed by `get_all_topic_config` |
| Subscription-group configuration snapshot | `getAllSubscriptionGroup` | 201 | Closed by `get_all_subscription_group` |
| Broker consume statistics | `fetchConsumeStatsInBroker` | 317 | Closed by `fetch_consume_stats_in_broker` |
| Broker statistics view | `viewBrokerStatsData` | 315 | Closed by `view_broker_stats_data` |
| Consume-queue query | `queryConsumeQueue` | 321 | Closed by `query_consume_queue` |
| Correction-offset query | `queryCorrectionOffset` | 308 | Closed by `query_correction_offset` |
| Static-topic configuration lookup | `getTopicConfig` | 351 | Closed by `get_topic_config` |
| Group-forbidden update/read | `updateAndGetGroupForbidden` | 353 | Closed by `update_and_get_group_forbidden` |
| Broker HA status | `getBrokerHAStatus` | 907 | Closed by `get_broker_ha_status` |
| Static-topic creation/update | `createStaticTopic` | 513 | Closed by `create_static_topic` |
| User creation | `createUser` | 3001 | Closed by `create_user` |
| User update | `updateUser` | 3002 | Closed by `update_user` |
| User deletion | `deleteUser` | 3003 | Closed by `delete_user` |
| User retrieval | `getUser` | 3004 | Closed by `get_user` |
| User listing | `listUser` | 3005 | Closed by `list_user` |
| ACL creation | `createAcl` | 3006 | Closed by `create_acl` |
| ACL update | `updateAcl` | 3007 | Closed by `update_acl` |
| ACL deletion | `deleteAcl` | 3008 | Closed by `delete_acl` |
| ACL retrieval | `getAcl` | 3009 | Closed by `get_acl` |
| ACL listing | `listAcl` | 3010 | Closed by `list_acl` |

The audited Java declarations are in
`org.apache.rocketmq.client.impl.MQClientAPIImpl` at pinned commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`. Every operation in this table
and all 18 original direct-operation candidates now have fixture-backed Rust
DTO and RPC owners. This closes that direct-operation candidate list, not the
broader source/API/input/error/control-flow/reverse-export audit. Earlier
closed direct rows include `cloneGroupOffset` (314) and
`invokeBrokerToGetConsumerStatus` (223). `deleteTopicInNameServer` maps through
`delete_topic_in_nameserver`; `getRemotingClient` maps to
`ClientRuntime::remoting`; and `recallMessageAsync` maps to the Future-based
recall path. The `GroupForbidden` DTO also matches Java's nullable accessors,
value equality, `hashCode`, and `toString` behavior. This inventory narrows the
direct-operation gap; it does not close P18-V01's broader settings, inputs,
errors, control flows or reverse-export audit.

## P18-V01 direct `getAllTopicConfig` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.getAllTopicConfig(String,long)` sends
request 21 with `GetAllTopicConfigRequestHeader` fields `topicSeq`,
`dataVersion`, and `maxTopicNum`. Core exposes
`MQClientApi::get_all_topic_config`; it uses the configured metadata page size
(default 2000), configured VIP transformation, and one caller deadline across
all pages. `DataVersion` and `TopicConfigSerializeWrapper` preserve Java JSON
field names, defaults, and null states. Pagination appends topic configs while
the version is stable; a changed version clears the collected table and starts
again at page zero. A missing or reflectively invalid `totalTopicNum` follows
the Java legacy-Broker path and accepts that page as complete. Java fixtures
and the Rust loopback test cover one/multiple pages, version restart, legacy
response, malformed/missing/null body cases, Broker errors and timeout.
Bidirectional typed-header and body fixtures cover both headers and the
wrapper. Producer core fixture SHA-256 is recorded in the Phase 18 execution
log. This closes this direct RPC/header/DTO slice only; broad P18-V01 remains
open.

## P18-V01 `MessageClientExt` inherited offset-ID overrides — 2026-10-21

Rust now dispatches Java `MessageClientExt` subclasses' virtual
`getOffsetMsgId()` and `setOffsetMsgId(String)` methods through
`MessageExtSubtype`. The inherited `getMsgId()` path first reads `UNIQ_KEY`
and then calls the virtual offset getter, matching Java when only the offset
method is overridden. The invoked 29-case direct-consume fixture checks the
custom offset-only subtype both with and without `UNIQ_KEY`; a Common test also
checks wrapper setters, no-op base `setMsgId`, live unique-ID precedence, and
conversion to `MessageExt`. A follow-up `setOffsetMsgId(null)` check confirms
that the offset and effective ID clear and the base-typed conversion preserves
that state. `records.json` SHA-256:
`1adc11e14bf1af0d39966ecda166a3ddb8a24c0e7d13bbf2d5a9830bdaac64dc`; runtime
consumer fixture SHA-256:
`403b79b69f30957ae2f935be37ec0b8537696909d91066d2126f216eff31996d`.
The default/all-feature workspace suites, strict workspace Clippy, and pinned
Maven verify pass on code revision `f7d7e8f1`; broader P18-V01 remains open.

P18-V01 custom `MessageClientExt.getMsgId`/`setMsgId` overrides (test revision
`5e12a174`, 2026-10-21): invoked Java confirms a subclass getter bypasses the
base live `UNIQ_KEY` lookup and its setter owns custom state, including a null
result, without changing the physical offset ID. The Common differential
compares `MessageExtSubtype::message_id` and `set_message_id` with those calls;
all 26 tests in the Common message API test file pass. Current `records.json`
SHA-256: `d0cdfbf2599adb0242a1df0dfa8523818a018c35cb34dac2b8418f8ce4bc972d`.
Strict all-target/all-feature Clippy, formatting, diff checks, and pinned Maven
verify pass; broad P18-V01 remains open.

## P18-V01 complete declared message-family method crosswalk — 2026-10-21

Audited the pinned 5.5.1 source at
`efa1be59d6fc747ad220f036c86030f05f0419f9`, not the separate 5.3.3 Java
working tree. The 34 `Message`, 41 `MessageExt`, and 4 `MessageClientExt`
declared public non-static instance methods are mapped to Rust APIs and
`MessageExtSubtype` hooks in
`PHASE_18_MESSAGE_SUBTYPE_API_CROSSWALK.md`. The source's constructors are
crosswalked separately; the three public static `MessageExt` helpers
(`parseTopicFilterType` and both `socketAddress2ByteBuffer` overloads) map to
Rust helpers there as well. This crosswalk covers all 82 declared public
methods, with no unreviewed declared
message-family methods; it supersedes earlier notes leaving other inherited
message methods and custom live properties-map identity open.

The invoked direct-consume fixture exercises custom `MessageExt` metadata and
property overrides plus custom `MessageClientExt` offset/effective-ID
overrides. `consumer-operations.json` SHA-256 is
`a8d82baca384b9b7932b4961b051902336155543428047d891eb9f84052d166d` and
`records.json` SHA-256 is
`d0cdfbf2599adb0242a1df0dfa8523818a018c35cb34dac2b8418f8ce4bc972d`.
Common message tests pass (26), direct-consume tests pass (4), and pinned Maven
`verify` passes with 29 direct-consume cases and Java/Rust cross-decodes. This
closes the declared message-family method and selected virtual-override slice;
the broader P18-V01 settings/API/flow/input/error/reverse-export audit and
current GitHub Ubuntu/native Windows P18-V04 evidence remain open.

## P18-V01 direct `queryCorrectionOffset` Broker API — 2026-10-22

The pinned `MQClientAPIImpl.queryCorrectionOffset(String,String,String,Set,long)`
path now maps to `MQClientApi::query_correction_offset` for request 308. Its
typed `QueryCorrectionOffsetRequestHeader` preserves nullable topic, compared
group, filter set and inherited request fields; the response DTO retains the
default empty map and nullable map/value behavior. The Java oracle fixture and
Rust loopback test cover six cases, including null arguments, empty filter
value, absent default map, null map, null Long value, Broker rejection and a
successful reply without a body. Protocol header inventory and wire-field
matrix checks pass. This closes only this direct method/header/body slice;
broad P18-V01 remains open.

## P18-V01 direct `updateAndGetGroupForbidden` Broker API — 2026-10-22

The pinned `MQClientAPIImpl.updateAndGetGroupForbidden` path now maps to
`MQClientApi::update_and_get_group_forbidden` for request 353. Rust adds the
typed request header and nullable `GroupForbidden` response DTO. The Java
fixture and Rust loopback test cover nullable header fields, inherited RPC
fields, populated and default-null result objects, a success without a body,
and Broker rejection. Header inventory and field-validation matrix checks
pass. This closes only this direct API/header/DTO slice; broad P18-V01 remains
open.

## P18-V01 direct `getAllSubscriptionGroup` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.getAllSubscriptionGroup(String,long)` sends
request 201 with `groupSeq`, `dataVersion`, and `maxGroupNum`. Rust exposes
`MQClientApi::get_all_subscription_group`, applies configured VIP conversion
and metadata page size (default 2000), and uses one deadline for all pages.
The new `SubscriptionGroupWrapper` retains both the subscription-group
configuration map and nested forbidden-topic map, plus `DataVersion` and Java
default/null semantics. A version change clears both accumulated maps and
restarts from group sequence zero. Missing or invalid `totalGroupNum` accepts
the reply as a complete legacy Broker result. Eleven invoked Java cases and the
Rust admin RPC replay cover one/multiple pages, a version restart that removes
stale rows from both maps, legacy replies, null/malformed bodies, Broker error,
and timeout. Bidirectional typed-header and wrapper-body fixture checks pass;
the focused VIP routing test also checks request 201. This closes one direct
API slice and reduces the reverse-audit list from 11 to 10 open rows; broad
settings/API/input/error/control-flow/reverse-export reconciliation remains
open.

## P18-V01 direct `queryConsumeQueue` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.queryConsumeQueue(String,String,int,long,int,String,long)`
sends request 321 with nullable topic and consumer group, primitive queue ID,
index, and count, plus inherited RPC fields. Rust exposes
`MQClientApi::query_consume_queue`, applies configured VIP conversion, uses the
caller deadline, maps Broker rejection to `RpcError::Response`, and returns
`None` for an absent or JSON-null body. `QueryConsumeQueueResponseBody` and
`ConsumeQueueData` preserve Java field names, primitive defaults, nullable
fields, nested `SubscriptionData`, and mutable object/list aliases. Invoked
Java and Rust fixtures cover populated/default/null/missing/malformed body
cases, null request strings, Broker rejection, and timeout; both typed header
decode modes and bidirectional typed header/body fixtures pass. The configured
VIP port transformation is checked by the core loopback test. This closes one
direct API/header/DTO slice and reduces the reverse-audit list from 10 to 9
open rows; broad settings/API/input/error/control-flow/reverse-export
reconciliation remains open.

## P18-V01 direct `createStaticTopic` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.createStaticTopic(String,String,TopicConfig,
TopicQueueMappingDetail,boolean,long)` sends request 513 using
`CreateTopicRequestHeader` and the encoded `TopicQueueMappingDetail` body. Core
exposes `MQClientApi::create_static_topic`, maps topic configuration into the
header, applies configured VIP conversion, and uses the supplied timeout. It
does not add local permission validation: the invoked Java fixture confirms
that an invalid permission value is still sent. `TopicQueueMappingDetail`
encoding preserves Fastjson's unquoted integer keys for both `currIdMap` and
`hostedQueues`. Java and Rust fixtures cover populated/default mapping bodies,
null and empty default topics, force/order/filter fields, invalid permissions,
success, Broker rejection, exact request bytes, timeout, and VIP routing. This
closes only this direct API/header/body slice; broad P18-V01 remains open.

## P18-V01 direct `createUser` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.createUser(String,UserInfo,long)` sends
request 3001 with `CreateUserRequestHeader.username` and the encoded `UserInfo`
body. Core exposes `MQClientApi::create_user`, uses the caller timeout, and
sends the supplied Broker address unchanged even when VIP channels are
enabled. `UserInfo` preserves nullable `password`, `userStatus`, `userType`,
and `username` fields, null omission, and Java identity equality; the request
header preserves nullable username and the no-op validation boundary. Invoked
Java/Rust fixtures cover populated/default/null-field inputs, success, Broker
rejection, exact body bytes, timeout, header inventory/cross-decode, and the
VIP-enabled unchanged address. This closes only the direct user-creation
slice; broad P18-V01 remains open.

## P18-V01 direct `updateUser` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.updateUser(String,UserInfo,long)` sends
request 3002 with `UpdateUserRequestHeader.username` and the encoded `UserInfo`
body. Core exposes `MQClientApi::update_user`, uses the caller timeout, and
sends the supplied Broker address unchanged even when VIP channels are
enabled. It reuses `UserInfo` and preserves the update header's nullable
username and no-op validation boundary. Invoked Java/Rust fixtures cover
populated/default/null-username inputs, success, Broker rejection, exact body
bytes, timeout, typed-header inventory/cross-decode, and the VIP-enabled
unchanged address. This closes one direct API/header slice and reduces the
reverse-audit list from seven to six open rows; broad P18-V01 remains open.

## P18-V01 direct `deleteUser` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.deleteUser(String,String,long)` sends request
3003 with `DeleteUserRequestHeader.username`, no body, and the caller timeout.
Core exposes `MQClientApi::delete_user`, preserves Java's nullable username and
no-op header validation, and sends the supplied Broker address unchanged even
when VIP channels are enabled. Invoked Java/Rust fixtures cover populated,
null, and empty usernames, success, Broker rejection, absent body, timeout,
typed-header inventory/cross-decode, and the VIP-enabled unchanged address.
This closes one direct API/header slice and reduces the reverse-audit list
from six to five open rows; broad P18-V01 remains open.

## P18-V01 direct `listUser` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.listUser(String,String,long)` sends request
3005 with `ListUsersRequestHeader.filter`, no body, and the caller timeout.
Core exposes `MQClientApi::list_user`, preserves Java's nullable filter,
nullable response list and nullable list members, and sends the supplied
Broker address unchanged even when VIP channels are enabled. Invoked Java/Rust
fixtures cover populated/empty/null filters, null body and JSON null, empty and
populated lists, null list members, success, Broker rejection, timeout,
typed-header inventory/cross-decode, and unchanged-address behavior. The
invoked Fastjson object-root case returns a list containing the string `{}`;
the typed Rust API returns a malformed-response error at that boundary rather
than exposing a non-`UserInfo` value. This closes the direct API/header and
valid-list response slice. The full auth-method scan also found the previously
omitted `getUser` and `getAcl` operations; the current auth reverse-audit list
has six open rows. Broad P18-V01 remains open.

## P18-V01 direct `getUser` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.getUser(String,String,long)` sends request
3004 with `GetUserRequestHeader.username`, no body, and the caller timeout.
Core exposes `MQClientApi::get_user`, preserves Java's nullable username and
nullable `UserInfo` response, and sends the supplied Broker address unchanged
even when VIP channels are enabled. Invoked Java fixtures and Rust loopback
tests cover populated/default/null-body/JSON-null responses, null and empty
usernames, success, Broker rejection, timeout, typed-header inventory and
cross-decode, and the VIP-enabled unchanged address. This closes one direct
API/header slice and reduces the auth reverse-audit list from six to five open
rows; broad P18-V01 remains open.

## P18-V01 direct `getAcl` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.getAcl(String,String,long)` sends request
3009 with `GetAclRequestHeader.subject`, no body, and the caller timeout. Core
exposes `MQClientApi::get_acl`, returns nullable `AclInfo`, and leaves the
supplied Broker address unchanged with VIP channels enabled. `AclInfo` and its
nested policy and entry DTOs preserve nullable lists and list members. Invoked
Java fixtures and Rust loopback tests cover populated/default/null/empty
policies, null nested members, null and empty subjects, success, Broker
rejection, timeout, typed-header inventory/cross-decode, and unchanged-address
behavior. This closes one direct API/header/DTO slice and reduces the auth
reverse-audit list from five to four open rows; broad P18-V01 remains open.

## P18-V01 direct `createAcl` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.createAcl(String,AclInfo,long)` sends
request 3006 with `CreateAclRequestHeader.subject`, a serialized `AclInfo`
body, and the caller timeout. Core exposes `MQClientApi::create_acl`, derives
the header subject from the ACL DTO, and leaves the supplied Broker address
unchanged when VIP channels are enabled. Invoked Java fixtures and Rust
loopback tests compare exact body bytes, nullable subject/policy inputs,
success, Broker rejection, timeout, typed-header inventory/cross-decode, and
unchanged-address behavior. This closes one direct API/header slice and
reduces the auth reverse-audit list from four to three open rows; broad P18-V01
remains open.

## P18-V01 direct `updateAcl` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.updateAcl(String,AclInfo,long)` sends
request 3007 with `UpdateAclRequestHeader.subject`, the Java-compatible
serialized `AclInfo` body, and the caller timeout. Core exposes
`MQClientApi::update_acl`, preserves nullable subjects and nested ACL fields,
and leaves the supplied Broker address unchanged with VIP channels enabled.
Invoked Java fixtures and Rust loopback tests compare exact body bytes,
null/empty subjects, nullable policy members, success, Broker rejection,
timeout, typed-header inventory/cross-decode, and unchanged-address behavior.
This closes one direct API/header slice and reduces the auth reverse-audit
list from three to two open rows; broad P18-V01 remains open.

## P18-V01 direct `deleteAcl` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.deleteAcl(String,String,String,long)` sends
request 3008 with `DeleteAclRequestHeader.subject` and `.resource`, no body,
and the caller timeout; its `policyType` field remains unset. Core exposes
`MQClientApi::delete_acl`, preserves nullable subject/resource values, and
leaves the supplied Broker address unchanged with VIP channels enabled.
Invoked Java fixtures and Rust loopback tests cover populated/default/null/
empty fields, absent body, success, Broker rejection, timeout, typed-header
inventory/cross-decode for all three fields, and unchanged-address behavior.
This closes one direct API/header slice and reduces the auth reverse-audit
list from two to one open row; broad P18-V01 remains open.

## P18-V01 direct `listAcl` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.listAcl(String,String,String,long)` sends
request 3010 with `ListAclsRequestHeader.subjectFilter` and `.resourceFilter`,
no body, and the caller timeout. Core exposes `MQClientApi::list_acl`, accepts
nullable filters, and preserves nullable list roots and `AclInfo` members. It
leaves the supplied Broker address unchanged with VIP channels enabled.
Invoked Java fixtures and Rust loopback tests cover populated/default/null/
empty filters, null body/list, object-root coercion and its typed malformed-
response error, nullable list members, success, Broker rejection, timeout,
typed-header inventory/cross-decode, and unchanged-address behavior. This
closes the final direct auth API/header slice; the broader P18-V01 audit
remains open.

## P18-V01 Query Admin typed-header decode continuation — 2026-10-22

The current Remoting library test run exposed that the expanded pinned Java
`query-admin-header-decode-modes.json` fixture included three header families
not dispatched by `query_admin_header_reflection_modes_match_invoked_java_551`:
`GetAllSubscriptionGroupRequestHeader`,
`GetAllSubscriptionGroupResponseHeader`, and `QueryConsumeQueueRequestHeader`.
The test now decodes and re-encodes all three in both fast and reflection modes.
Those cases also showed that malformed optional integer fields on the two
subscription-group headers decode as null in Java; Rust now preserves that
behavior. Fixture SHA-256:
`ce98f356e351ec5028e8f01f2ba36612306a4559278006599e8c44ff125b9766`.
`cargo test -p rocketmq-classic-remoting --lib
query_admin_header_reflection_modes_match_invoked_java_551 --locked` passes.
This closes the surfaced header decode slice only; full P18-V01 and current
revision CI verification remain open.

## P18-V04 Windows cross-compilation update — 2026-10-22

The previous Windows address-enumeration compile note records that the
MinGW-w64 linker was unavailable on its October 10 host. It is installed on
the current macOS host. The full
`cargo check -p rocketmq-classic-core --all-targets --target
x86_64-pc-windows-gnu --locked` now passes, covering the repository's
Windows-gated address enumeration and test helpers. The exact native CI test
also cross-compiles successfully with
`cargo test --target x86_64-pc-windows-gnu -p rocketmq-classic-consumer
--lib stack::tests::jstack_includes_current_and_other_named_threads
--no-run --locked`. These are compile checks; the stack-capture assertion
still requires execution on Windows. Broad P18-V01 and native Windows
P18-V04 evidence remain open.

## P18-V01 TopicConfig and TopicMessageType model API — 2026-10-22

Pinned Java 5.5.1 `TopicConfig` and `TopicMessageType` source and reflected
public API surfaces exposed model methods missing from the Rust protocol
surface. Added the TopicConfig constructor variants and mutable default queue
counts; permission/system-flag setters; copy, equality, hash, and string
behavior; semicolon encoding/decoding; nullable attribute values with shared
reference identity; topic-message-type and Lite-expiration helpers; and the
`TopicConfigFilterType` enum methods. Added `TopicMessageType` values, metrics
labels, nullable message-property lookup, and Java precedence among transaction,
delay, FIFO, priority, Lite, and normal messages. Java fixtures and Rust tests
pin defaults, mutation, copy aliasing, encoded attribute order, malformed and
partial decode behavior, trailing values, Unicode decimal integers, null
attributes, enum errors, expiration parsing, and message-type precedence.
Protocol tests and strict Common/Protocol Clippy pass for this slice.

This closes the TopicConfig/TopicMessageType model-method slice only. Other
Protocol DTO source/default/validation/error/control-flow families remain
under the P18-V01 audit; P18-V01 and native Windows runtime evidence remain
open.

## P18-V01 direct derived-getter subtype overrides — 2026-10-22

`MessageExtSubtype` now dispatches direct caller overrides of Java's
`getTags`, `getKeys`, `isWaitStoreMsgOK`, `getBuyerId`, `getDelayTimeLevel`,
`getDelayTimeSec`, `getDelayTimeMs`, `getDeliverTimeMs`, `getTopicSysFlag`,
and `getGroupSysFlag`. The inherited Rust methods first use these hooks, then
retain the existing virtual-property behavior when a hook is absent. This
fills the difference between overriding `getProperty` and directly
overriding each public getter. The invoked Java fixture assigns different
getter and property results and checks all ten getter calls; the Rust test
compares those results, property fallback, and call count. Fixture SHA-256:
`0d818f3b8a711661a382de19a07db7292c5976f20ddee9a6106b78b9e8860f82`.
Common tests, direct-consume tests, full default/all-feature workspace tests,
strict workspace Clippy, and the pinned Maven verify passed. This closes only
the named subtype getter slice; full P18-V01 reconciliation and native
Windows execution remain open.

Nullable subtype getter continuation (2026-10-22): the invoked Java fixture
adds direct overrides returning null for `getTags`, `getKeys`, `getBuyerId`,
`getTopicSysFlag`, and `getGroupSysFlag`, while `getProperty` returns non-null
values and malformed sysflag text. Rust's `Some(None)` hooks preserve those
direct nulls and avoid parsing the malformed fallback values. The test also
compares all five property values and the virtual-call count. Fixture
SHA-256: `8640635c5eb044d1485c182be377a308f909885477d01916ba234fb34ef70b7b`.
The six direct-consume tests, focused Common/Consumer Clippy, formatting and
Java fixture verification pass. Broad P18-V01 and native Windows execution
remain open.

## P18-V01 Producer live runtime setter flow — 2026-10-22

Pinned Java source confirms three distinct post-start behaviors. `sendKernelImpl`
reads `DefaultMQProducer.isSendMessageWithVIPChannel()` for each producer send,
so changing true to false changes the next broker address from VIP port 10909 to
the normal port 10911. Later sends also read the current producer group. In
contrast, `initTopicRoute` reads `getTopics()` during startup; replacing the
facade list later updates its getter but leaves the initialized topic routes
unchanged. The invoked `runtimeSetterFlows` fixture seeds a route, invokes the
startup initializer, changes all three settings, and captures send-hook group
and address before a short timeout, without contacting a Broker.

Rust now converts producer send addresses with the live Producer VIP flag
instead of the shared runtime's startup `ClientConfig` snapshot. Rust tests
compare the changed send group and normal broker port to the fixture and
compare post-setter topics to the retained startup route set. Fixture
`fixtures/producer/core.json` SHA-256:
`1c3ac406157b2ad70f5ffd438b893e1b8c39a62c83a7339a819e752cfa587acb`.
Producer crate tests, strict Producer Clippy, formatting, diff checks, and
pinned Maven `verify` pass. This closes the named Producer group/VIP/topic
setter flow slice; broad P18-V01 source/API/input/reverse-export reconciliation
and native Windows runtime evidence remain open.

## P18-V01 Producer topic-list alias semantics — 2026-10-22

The pinned constructor and setter retain the supplied `List<String>` object,
and `getTopics()` returns that same object. The `runtimeSetterFlows` fixture
now mutates the list held by the caller after `setTopics` and records the
updated getter contents, while also proving that post-start list mutation does
not rerun `initTopicRoute`. Rust replaces copied `Vec<String>` access with
`ProducerTopicList`, a shared lock-protected handle returned by `topics()` and
retained by `set_topics()`. Cloning the handle preserves aliases; cloning a
`ProducerConfig` keeps its documented detached settings snapshot. The type is
re-exported by both Producer and facade crates. Rust constructor/config/runtime
tests compare external mutations and the startup route snapshot. Fixture
`fixtures/producer/core.json` SHA-256:
`ae8e09607bfaac817640a03dc596675757a511c26e03e7d9f0ca7e8a51201cd8`.
Producer crate tests, facade crate tests, strict Producer/facade Clippy,
formatting, diff checks, and pinned Maven `verify` pass. This entry also
corrects the 2026-10-14 Producer group/VIP/topics note: the statement that
producer sends retained the startup VIP snapshot was incorrect. Java sends
read the live facade flag; only the shared runtime's `ClientConfig` remains a
startup clone. The correction is independently covered by the
`runtimeSetterFlows` case above. This closes the named topic-list alias gap
only; broad P18-V01 reconciliation remains open.

## P18-V01 push/pull Consumer factory-start failure — 2026-10-22 (`b46612bf`)

Pinned Java `DefaultMQPushConsumerImpl.start()` and
`DefaultMQPullConsumerImpl.start()` register their consumer group before calling
`MQClientInstance.start()`. An invoked probe sets the NameServer poll interval
to zero and observes `IllegalArgumentException`; both Consumer and factory
remain `START_FAILED`, and the group remains in the factory's consumer table.
`fixtures/consumer/lifecycle.json` records both paths (SHA-256
`3dab90cd55b37eb428f7a09cda4c6302345b13df867ef1a4e58f34a7ee856b3b`).

Rust now retains those Consumer registrations when the shared runtime reaches
its failed startup state, while stopping the owned push workers. The tests
`factory_start_failure_preserves_java_push_consumer_registration` and
`factory_start_failure_preserves_java_pull_consumer_registration` compare
service state, failed runtime state, registration and manager identity with
the invoked fixture; terminal cleanup removes the retained group. This closes
the push/pull factory-start branch only. LitePull lifecycle, post-start
consumer failures, and broad P18-V01 input/error/control-flow and
reverse-export reconciliation remain open.

## P18-V01 LitePull factory-start failure — 2026-10-22 (`ad7a0523`)

Pinned Java 5.5.1 `DefaultLitePullConsumerImpl.start()` registers its group
before starting `MQClientInstance`. With NameServer poll interval zero, the
first start throws `IllegalArgumentException`; the LitePull Consumer and
factory remain `START_FAILED`, the registration remains in the consumer table,
and a retry throws `MQClientException`. The invoked oracle is
`litePullFactoryStartFailure` in `fixtures/consumer/lifecycle.json` (SHA-256
`26fadf1fbcc7b51c64e17c10626347dbc59a520279948f030ff6359b15195387`).

Rust now keeps a distinct failed-start lifecycle for LitePull. A failed
factory start retains the registered group, a later start reports the failed
state, and shutdown leaves that failed state intact, matching the Java
implementation's state transitions. The regression also checks the failed
runtime state, runtime-manager identity and explicit terminal cleanup. This
closes the LitePull factory-start failure slice; post-start Consumer failures
and broad P18-V01 input/error/control-flow and reverse-export reconciliation
remain open.

## P18-V01 LitePull startup listener-route failure — 2026-10-22 (`aab73bb3`)

Pinned Java 5.5.1 `operateAfterRunning()` synchronously fetches message queues
for listeners registered before start. A route transport failure makes
`DefaultLitePullConsumer.start()` throw `MQClientException`; its implementation
shuts down, moves to `SHUTDOWN_ALREADY`, removes the Consumer registration and
rejects retry with `MQClientException`. The invoked case is
`litePullListenerRouteFailure` in `fixtures/consumer/lifecycle.json` (SHA-256
`841aac58bbeaf12908aebc79796f75885f00e5356ad61dd06aed4cbf4e0ef9f3`).

Rust previously ignored this route lookup error and continued startup. It now
returns the error, shuts down the started pull engine, removes the group and
retains a terminal LitePull lifecycle. The regression compares retry and
registration outcomes with the fixture and verifies the runtime stops. This
closes the pre-registered listener route-failure branch only; post-start
Consumer failures and broad P18-V01 input/error/control-flow and reverse-export
reconciliation remain open.

## P18-V01 LitePull post-start listener-route failure — 2026-10-22 (`148308fb`)

Pinned Java 5.5.1 `registerTopicMessageQueueChangeListener()` stores the
listener before fetching the topic route. If that fetch fails while the
Consumer is running, the call throws `MQClientException`, the listener stays
registered, and both Consumer and client factory remain `RUNNING`; another
registration attempt also throws. The invoked case is
`litePullListenerRegistrationRouteFailure` in
`fixtures/consumer/lifecycle.json` (SHA-256
`a17cfdae54f83a7a1ee3d1612f9bb5509175936da1adf289f799a48e232c4db5`). The
pinned polling loop later compares all registered listener topics, including
topics that are not subscribed, and calls a retained listener on its first
successful route fetch after the failed registration.

Rust now fetches the route during registration, keeps the listener on error,
and scans every registered listener topic during reconciliation. A successful
registration seeds the initial queue set; after a failed registration, the
first successful fetch calls the listener. The regression checks the error on
the first and repeated registration, verifies the Consumer remains running,
then switches from an unreachable to a scripted NameServer and observes the
callback. Since route fetching uses async I/O, the public Rust registration
method is now async and callers must `.await` it. This closes the LitePull
post-start listener route and retry branch only; broad P18-V01
source/API/settings/input/error/control-flow and reverse-export reconciliation
remains open.

## P18-V01 LitePull broker subscription-check startup failure — 2026-10-22 (`c04e57ee`)

Pinned Java 5.5.1 LitePull startup refreshes routes for subscribed topics,
then calls `MQClientInstance.checkClientInBroker()`. For a SQL92 subscription,
the invoked fixture returns Broker response code 44 to `CHECK_CLIENT_CONFIG`;
`start()` throws `MQClientException`, shuts down the Consumer and factory to
`SHUTDOWN_ALREADY`, and removes the group registration. Fixture
`litePullBrokerSubscriptionCheckFailure` in
`fixtures/consumer/lifecycle.json` has SHA-256
`0087e05cd0480a8a6cc6b993324f0ee8ad2ce6d8f58ab8ef8175813908023369`.

Rust now refreshes subscription routes before checking the Broker, invokes
the runtime's Java-compatible client check, and shuts down the Lite engine
and any started workers when the Broker rejects the subscription. The
integration regression uses a SQL92 subscription without a queue listener,
checks the emitted request against the Java fixture, and verifies terminal
state and removed registration. This closes the LitePull broker subscription
check failure slice only; broad P18-V01 source/API/settings/input/error/
control-flow and reverse-export reconciliation remains open.

## P18-V01 LitePull listener-registration lifecycle serialization — 2026-10-22 (`33b8084d`)

Pinned Java 5.5.1 declares LitePull `start()`, `shutdown()`, and
`registerTopicMessageQueueChangeListener()` synchronized on the same
implementation monitor. `litePullMethodSynchronization` in
`fixtures/consumer/lifecycle.json` records those reflected modifiers (fixture
SHA-256 `4972ae18d32a4aa337b1c4aad81153b81fa86a28112e505bceab8a660693ac5b`).
Rust listener registration now acquires the lifecycle gate before the
reconciliation gate, matching the lifecycle-to-reconciliation lock order used
by start. A scripted Broker holds startup inside `CHECK_CLIENT_CONFIG`; the
regression verifies concurrent listener registration remains pending until
start returns, then fetches and records the listener route. Consumer package
tests, strict Clippy, formatting/diff checks, and pinned Maven verification
pass. This closes the LitePull lifecycle/listener serialization race only;
broad P18-V01 source/API/settings/input/error/control-flow and reverse-export
reconciliation remains open.

## P18-V01 LitePull running subscribe heartbeat and route refresh — 2026-10-22 (`f24f35d2`)

Pinned Java 5.5.1 synchronizes all three `DefaultLitePullConsumerImpl.subscribe`
overloads. When the Consumer is running, each overload stores the subscription,
installs the relevant listener, sends a locked heartbeat, and immediately
refreshes routes for every subscription. Fixture
`litePullMethodSynchronization` now records all three synchronized overloads
and has SHA-256
`ec312b4648fc4942286ffbb1b329591e15327a9f43f27070b1a110048571d96a`.

Rust LitePull subscription entry points are now async so they can complete the
same runtime flow. They take the lifecycle gate, register the subscription and
optional listener, suppress heartbeat failures like Java, then refresh every
currently subscribed route before returning. The regression checks that the
Broker heartbeat contains the added subscription and that NameServer receives
route queries for both the original and added topic without waiting for the
metadata poll. A lifecycle race regression also verifies subscriptions wait
while startup holds the Java-equivalent monitor. Consumer package tests,
strict Clippy, formatting/diff checks, and pinned Java Maven verification pass.
This closes the LitePull live-subscription refresh slice only; broad P18-V01
source/API/settings/input/error/control-flow and reverse-export reconciliation
remains open.

## P18-V01 LitePull unsubscribe lifecycle serialization — 2026-10-22 (`fa43e761`)

Pinned Java 5.5.1 declares `DefaultLitePullConsumerImpl.unsubscribe(String)`
synchronized on the same monitor as `start`, `shutdown`, and subscription
methods. Fixture `litePullMethodSynchronization` now records the reflected
modifier; its SHA-256 is
`cc3ae6b4f1b6adb53dd1b32a4314cbf2252dabff22655dab4102c6a8473152ec`.

Rust `unsubscribe` now takes the lifecycle gate followed by the reconciliation
gate before changing subscriptions, queue state, workers, and engine
registrations. The lifecycle regression holds startup at the Broker check and
verifies concurrent unsubscribe remains pending, then removes the original
subscription after startup completes. The focused regression, strict Clippy,
format/diff checks, and Java lifecycle fixture verification pass. This closes
only the LitePull unsubscribe synchronization slice; broad P18-V01
source/API/settings/input/error/control-flow and reverse-export reconciliation
remains open.

## P18-V01 LitePull assignment lifecycle serialization — 2026-10-22 (`e41e6aab`)

Pinned Java 5.5.1 synchronizes `assign(Collection)` and
`setSubExpressionForAssign(String,String)` on the same implementation monitor
as `start`. Fixture `litePullMethodSynchronization` now records both reflected
modifiers and has SHA-256
`3ebb12d617b9cc73a1be52cf3469323ada4c83aa4b1a9c494a5d0899080f8fd6`.

Rust `assign` and `set_sub_expression_for_assign` now take the lifecycle gate,
so their async entry points serialize with startup and shutdown. The paused
Broker-check regression proves both remain pending during startup and then
return the expected subscription-mode and post-start validation errors. All
LitePull integration tests, strict Clippy, format/diff checks, and Java
lifecycle fixture verification pass. This closes these assignment
synchronization branches only; broad P18-V01 source/API/settings/input/error/
control-flow and reverse-export reconciliation remains open.

## P18-V01 LitePull poll lifecycle serialization — 2026-10-22 (`1deb11a3`)

Pinned Java 5.5.1 declares `DefaultLitePullConsumerImpl.poll(long)` synchronized;
the monitor remains held while poll waits for a consume request and runs its
delivery hooks. Fixture `litePullMethodSynchronization` records this modifier
and has SHA-256
`7d6bd48e1cea531ea6b5a626d9a9dfe042ba8514bccad0d242558cad249cfca5`.

Rust `poll` now holds the lifecycle gate through auto-commit, message waiting,
and delivery hooks. The scripted flow starts a timed poll with no messages and
verifies concurrent unsubscribe stays pending until poll times out. The
regression, strict Clippy, formatting/diff checks, and Java lifecycle fixture
verification pass. This closes the LitePull poll serialization slice only;
broad P18-V01 source/API/settings/input/error/control-flow and reverse-export
reconciliation remains open.

## P18-V01 LitePull synchronized runtime operations — 2026-10-22 (`49bcfa19`)

The pinned `DefaultLitePullConsumerImpl` also synchronizes `isRunning`,
`assignment`, `seek`, `commitAll`, and both `commit` overloads. The lifecycle
fixture now records those modifiers alongside the previously covered methods;
its SHA-256 is
`09682534479d8b1459eee8945bfccf5e1411d6fe37c9bbb45233907f1883193b`.

Rust lifecycle-gates the corresponding async operations and their queue/map
commit adapters. Auto-commit now calls the unlocked internal commit helper so
poll can retain the monitor-equivalent gate without recursively acquiring it.
The paused-start regression confirms `is_running`, `assignment`, and `commit`
wait until start completes. All LitePull integration tests, strict Clippy,
format/diff checks, and Java lifecycle fixture verification pass. This closes
these LitePull synchronized-operation branches only; broad P18-V01
source/API/settings/input/error/control-flow and reverse-export reconciliation
remains open.

## P18-V01 LitePull metadata refresh and direct NameServer lookup — 2026-10-22 (`b161c9a0`)

Pinned Java `MQAdminImpl.fetchSubscribeMessageQueues` queries NameServer on
every call and converts the returned route directly to subscribe queues
without publishing it to the runtime route cache. Java's LitePull scheduled
metadata check starts after 10 seconds, then runs at the configured fixed-rate
period; its synchronized callback only checks registered queue-change
listeners. Shared subscription route refresh remains owned by the runtime's
route schedule.

Rust `PullConsumer::fetch_message_queues` now performs the corresponding fresh
NameServer query through `ClientRuntime::get_topic_subscribe_queues`, which
does not alter the shared cache. LitePull now checks queue-change listeners
only on the matching 10-second-initial-delay, fixed-rate schedule, under the
lifecycle gate. It no longer checks listeners on each 200 ms worker
reconciliation or refreshes subscription routes from the metadata timer. The
regression changes the scripted NameServer route from one to two queues and
verifies the direct lookup sees both while the cached snapshot remains at one.
Full locked core and consumer tests, strict core/consumer Clippy,
`cargo fmt --all`, and `git diff --check` pass. The pinned Java source was
unchanged; no Java fixture was regenerated for this source-level control-flow
correction. This closes the LitePull metadata polling and direct queue-lookup
slice only; broad P18-V01 source/API/settings/input/error/control-flow and
reverse-export reconciliation remains open.

## P18-V01 LitePull NameServer address update flow — 2026-10-22

The pinned Java `DefaultLitePullConsumerImpl.updateNameServerAddr(String)` is
not synchronized; it only replaces the NameServer address list on the client
API. The Lite metadata callback and the shared runtime route-refresh schedule
observe that update on their own timers. Java also has the runtime object only
after startup.

Rust `LitePullConsumer::update_name_server_address` now updates the running
engine's NameServer list without taking the lifecycle gate, refreshing routes,
or invoking listeners immediately. Before an engine exists, Rust returns its
typed `NotStarted` error in place of Java's null-factory failure. The lifecycle
fixture records the unsynchronized modifier (`fixtures/consumer/lifecycle.json`
SHA-256 `a7c5e13bf3d67ec8402082800068ded16d49893c51eac9123fb3fa8776482a36`).
The regression verifies the update completes while synchronized `poll` is
waiting, no listener runs on the update call, and the registered callback sees
the route change at Java's scheduled metadata check. The full locked Lite/POP
integration target (23 tests), strict Consumer Clippy, formatting/diff checks,
and pinned Java lifecycle fixture verification pass. This closes the LitePull
NameServer update concurrency and callback-timing slice only; broad P18-V01
source/API/settings/input/error/control-flow and reverse-export reconciliation
remains open.

## P18-V01 LitePull metadata timer registration point — 2026-10-22 (`85f547e3`)

Java `DefaultLitePullConsumerImpl.start()` registers the Lite metadata task
immediately after `MQClientInstance.start()` and before its remaining
post-start route and Broker checks. The scheduler's 10-second initial delay
and configured fixed-rate period are therefore measured/snapshotted before
those operations, while its synchronized callback waits for `start()` to
release the implementation monitor.

Rust now captures the metadata deadline and period immediately after the
client runtime starts, then spawns the monitor after post-start initialization.
The regression changes the NameServer route while a scripted Broker holds
startup in `CHECK_CLIENT_CONFIG` for longer than 10 seconds; after startup
returns, the queue-change callback runs within two seconds, proving the timer
did not restart its initial delay at task-spawn time. All 24 Lite/POP
integration tests, strict Consumer Clippy, formatting, and diff checks pass.
This closes the Lite metadata timer registration-point slice only; broad
P18-V01 source/API/settings/input/error/control-flow and reverse-export
reconciliation remains open.

## P18-V01 LitePull metadata schedule validation and failure lifecycle — 2026-10-22 (`b8626d35`)

Pinned Java does not validate `topicMetadataCheckIntervalMillis` in
`checkConfig()`. It starts and registers the consumer runtime, then
`scheduleAtFixedRate` throws `IllegalArgumentException` for a nonpositive
period. The Lite consumer remains `START_FAILED`, the consumer registration is
retained, the client factory remains `RUNNING`, and a retry throws
`MQClientException`.

Rust now checks the interval at that scheduling point after engine startup,
retains the started engine and registration on failure, and preserves the
failed lifecycle for retry and shutdown behavior. The pinned Java lifecycle
fixture records all four observed states; its SHA-256 is
`c1c4a4dde2987f7d2d1c077e739847c79392ae57d34d218d6036ece550120013`.
The fixture-driven Rust regression verifies runtime state, registration,
retry, and cleanup. Full locked Consumer tests (252 unit tests plus all
integration tests and doctests), strict Core/Consumer Clippy, formatting/diff
checks, and Java lifecycle fixture drift verification pass. This closes this
LitePull startup-failure slice only; broad P18-V01
source/API/settings/input/error/control-flow and reverse-export reconciliation
remains open.

## P18-V01 LitePull config validation before executor construction — 2026-10-22 (`9db5c40d`)

Pinned Java `DefaultLitePullConsumerImpl.start()` runs `checkConfig()` before
constructing its `ScheduledThreadPoolExecutor`. Its consumer-group and suspend
timeout checks therefore take precedence over a negative `pullThreadNums`.
Rust previously rejected the negative thread count first.

Rust now validates the effective namespaced group and suspend timeout before
checking the thread count, matching Java's start-failure order. The pinned
Java lifecycle fixture combines an invalid suspend timeout and negative
thread count, records that timeout validation wins, and confirms no runtime or
consumer registration was created; fixture SHA-256:
`706d8348e770dfedb5cf3759f22090e97d7f6c83a9817f7ec662a98c4f1d4fa0`.
The fixture-driven regression passes alongside the full locked Consumer
package tests (253 unit tests, all integration tests and doctests), strict
Core/Consumer Clippy, formatting/diff checks, and Java lifecycle fixture drift
verification. This closes this validation-order branch only; broad P18-V01
source/API/settings/input/error/control-flow and reverse-export reconciliation
remains open.

## P18-V02 LitePull route observer stability — 2026-10-22 (`5c61aa83`)

The running-subscription regression observed NameServer requests in a fixed
two-message window. Concurrent route refresh could emit a duplicate topic
notification before the second distinct topic, making the assertion flaky.
The observer now collects until it has seen both subscribed topics or the
existing two-second deadline expires.

No client behavior changed. The focused Lite/POP integration target passes,
and the full `cargo test --workspace --all-features --locked` gate passes
after the observer update. This closes the test-observer flake only; full
P18-V02 fuzz/performance checks and platform CI evidence remain open.

## P18-V01 `listUser` object-root result parity — 2026-10-22 (`c6ffd6bf`)

Follow-up to the `listUser` entry above: pinned Fastjson 2.0.63's
`RemotingSerializable.decodeList` returns the original object-root JSON text as
a String element, so the `{}` fixture result serializes as `["{}"]`. Rust now
carries that opaque value through the `UserInfo` Java serialization adapter.
The ordinary `UserInfo` fields, camelCase names, and omission behavior are
unchanged. The Core loopback differential now compares the Java result for all
eight fixture outcomes, including this object-root case. Fixture
`fixtures/producer/core.json` SHA-256:
`ae8e09607bfaac817640a03dc596675757a511c26e03e7d9f0ca7e8a51201cd8`.

The focused Core differential, full Protocol all-feature tests, default and
all-feature workspace tests, strict workspace Clippy, formatting, and diff
checks pass. The pinned Maven verification (37 scenarios, fixture drift and
Java/Rust cross-decodes), differential self-test/manifest (37 scenarios),
fuzz smoke (163 frame seeds, 11 message seeds, 2,784 mutations), race/leak
smoke (16 cases × 3), release examples, offline benchmark build, workspace
metadata/package checks, and Consumer Windows GNU cross-check also pass on
this source revision. Native Windows execution and current-revision Ubuntu CI
remain unverified. This closes the object-root response-result mismatch only;
broad P18-V01 source/API/settings/input/error/control-flow/reverse-export
reconciliation and Phase 19 Broker/Docker validation remain open.

## P18-V01 Fastjson writer-feature inventory and `UserInfo` surface — 2026-10-22 (`c5892d2c`)

The older writer-feature note that lists `WriteClassName` as unmapped is stale:
the current body, route, and heartbeat adapters implement all 48 pinned
Fastjson writer features. The enum-to-fixture test now asserts every feature is
implemented, and Java differential tests compare each feature's output across
41 body, 8 route, and 3 heartbeat cases.

Pinned Java `UserInfo` is not a `RemotingSerializable` subclass and does not
have the instance `encode(JSONWriter.Feature...)` overload. Rust removed that
unmatched public method and added a compile-fail API guard. Full Protocol
all-feature tests, strict Protocol Clippy, formatting, and Maven verification
(37 scenarios and fixture cross-decodes) pass. This closes those two named
writer/API rows only; broad P18-V01 reconciliation, current-revision Ubuntu CI,
native Windows execution, and Phase 19 Broker/Docker validation remain open.

## P18-V01 NamespaceUtil reverse-export correction — 2026-10-22 (`5024360d`)

Pinned-source inspection found four Java public constants and nine declared
public method signatures on `NamespaceUtil`. Rust's Java-shaped associated
constants and methods match that declaration inventory, but the Rust module
also made the same four constants public at
`rocketmq_classic_protocol::namespace::{...}`. No workspace caller used those
aliases. They are private now, and compile-fail rustdoc cases guard against
reintroducing those unmatched exports. The direct namespace behavior and
associated constants remain unchanged.

On code revision `5024360d`, default and all-feature locked workspace tests,
strict all-target/all-feature workspace Clippy, formatting/diff checks, and
pinned Maven `verify` pass. The Maven run covers 37 scenarios, 36 runtime
route/VIP cases, and Java cross-decodes 106 typed headers and 19 typed bodies.
This closes the four extra module exports only; broad P18-V01 reconciliation,
current-revision Ubuntu CI, and native Windows execution remain open.

## P18-V01 Rustdoc reverse-scan count correction — 2026-10-22 (`5024360d`)

Regenerated all-feature Rustdoc JSON after the Protocol static-topic wrapper
and NamespaceUtil export changes. Remoting remains at 399 public item records,
338 distinct names including its generated crate marker, and 337 actual
identifiers; the earlier `399` to `402` change incorrectly added three
Protocol wrapper functions to the Remoting count. Protocol has 1,525 distinct
public names including its generated crate marker (1,524 without it). A raw
substring join against `PHASE_6_API_CONFIG_MAP.md` leaves 966 names without a
literal mention, excluding the generated crate marker. These counts are
crosswalk diagnostics; Java naming, inherited methods, DTO fields, and Rust
ownership adapters need semantic mapping. The static-topic wrapper's three
associated functions are listed in the Protocol crosswalk; the four private
namespace module constants no longer appear as public Rustdoc names.

## P18-V01 TopicValidator and SessionCredentials export correction — 2026-10-22 (`87900fb8`)

The reverse surface review found duplicate module-level constants alongside
Java-shaped associated constants. Fourteen TopicValidator values now remain
public only on `TopicValidator`; the public `rocketmq_classic_common::topic`
aliases are private. Five credential constants remain public only on
`SessionCredentials`; the aliases in `rocketmq_classic_core::auth` and the
Rust-only `KEY_FILE_PROPERTY` are private. `SessionCredentials::key_file_path`
continues to map Java's public `KEY_FILE` path value. Two compile-fail rustdoc
examples cover the removed paths, and the ACL fixture test now reads
`ACCESS_KEY` and `SIGNATURE` through `SessionCredentials`.

Validation on code revision `87900fb8`: locked default and all-feature
workspace tests pass; strict all-target/all-feature workspace Clippy,
format/diff checks, pinned Maven `verify` (37 scenarios; 36 route/VIP cases;
106 typed headers and 19 typed bodies decoded by Java), and all-feature
workspace Rustdoc JSON generation pass. Broad P18-V01 source/settings/input/
error/control-flow reconciliation, current-revision Ubuntu CI, and native
Windows execution remain open.

## P18-V01 trace record identity equality — 2026-10-22 (`be37e031`)

Pinned Java 5.5.1 `TraceBean` and `TraceContext` override neither
`Object.equals` nor `Object.hashCode`. `TraceContext.compareTo` compares only
timestamps, so distinct contexts with equal timestamps can compare as zero
while remaining unequal. Rust no longer derives structural `PartialEq`/`Eq`
for either trace record; compile-fail doctests protect the public boundary.
The invoked trace fixture now checks equal-value distinct beans and contexts,
same-reference equality, and the equal-timestamp `compareTo` result. Fixture
SHA-256: `f88931ceeca238b857ff1367ec846c4f7639da7e1ae43580e22611a12966ac43`.

Core tests, locked default and all-feature workspace tests, strict
all-target/all-feature workspace Clippy, formatting/diff checks, and pinned
Maven `verify` pass. Maven verifies 37 scenarios, 36 runtime route/VIP cases,
and Java decoding of 106 typed headers and 19 typed bodies. This closes only
the trace equality slice; broad P18-V01 source/API/input/error/flow and
reverse-export reconciliation remains open. Full P18-V02 fuzz/race/benchmark
refresh, current-revision Ubuntu CI, native Windows execution, and Phase 19
Broker/Docker validation remain separate open gates.

## P18-V01 ACL hook custom-header signing — 2026-10-22 (`1eb5b6ba`)

Pinned Java `AclClientRPCHook.doBeforeRequest` calls `makeCustomHeaderToNet`
from `parseRequestContent` before building the sorted signature input. Rust's
hook now materializes its pending typed command header at the same point. The
invoked Java fixture and Core differential compare the resulting extension
fields, canonical bytes (including the body), and HMAC signature for a
`CreateTopicRequestHeader`. ACL fixture SHA-256:
`56689d2cf7a9ca9e6c39f5dd62cef73aa268b81890fb1b073510e3ad4ebbf2ba`.

The locked Core suite, strict all-target/all-feature workspace Clippy,
format/diff checks, and pinned Maven verification pass. This closes only the
typed-header signing branch; broad P18-V01 source/API/input/error/flow and
reverse-export reconciliation remains open. Full current-revision workspace
P18-V02 refresh, Ubuntu CI, native Windows execution, and Phase 19 Broker/
Docker validation remain open.

## P18-V02 current local server-free gate refresh — 2026-10-22 (`1eb5b6ba`)

The current code revision passed locked default and all-feature workspace
tests, strict all-target/all-feature workspace Clippy, formatting and diff
checks, pinned Maven `verify` (37 scenarios, 36 route/VIP cases, Java decoding
of 106 typed headers and 19 typed bodies), differential self-test and
37-scenario manifest validation, fixed-seed fuzz smoke (163 frame seeds, 11
message seeds, 2,784 mutations), and race/leak smoke (16 cases x 3).

Release facade examples built in release mode; the offline benchmark executed
and passed strict benchmark-target Clippy. Workspace metadata, dependency
direction and license checks passed; all seven product package listings
contained `LICENSE` and `NOTICE`. Four Core Unique ID tests passed under both
UTC and `Asia/Taipei`. The Windows GNU Core all-target check and the Consumer
stack-capture test cross-compile passed.

This refresh closes the local server-free P18-V02 gate on code revision
`1eb5b6ba`. Broad P18-V01 source/API/settings/input/error/control-flow and
reverse-export reconciliation remains open. Current-revision Ubuntu CI and
native Windows execution remain unverified; no push or external CI was run.
Phase 19 Broker/Docker validation remains separate.

## P18-V01 metadata wrapper identity equality — 2026-10-22

The pinned Java `TopicConfigSerializeWrapper` and `SubscriptionGroupWrapper`
declare neither `equals` nor `hashCode`; both inherit identity behavior from
`Object`. The invoked producer-core fixture constructs two independently
populated wrappers with identical serialized bytes, confirms that they remain
unequal, and confirms that each wrapper equals itself. It records
`java.lang.Object` as the declaring owner for both methods. Rust no longer
derives structural `PartialEq`/`Eq` for these DTOs, with compile-fail doctests
guarding that API boundary. Producer core fixture SHA-256:
`f0b625d90d319a81070a3cc342f1e655c67d74aa747e0934f12c4b5ed0911095`.

Protocol tests (160 unit tests, all integration targets, and 166 doctests),
the focused producer-core fixture generation and verification, pinned Maven
`verify` (37 scenarios, 36 route/VIP cases, and Java decoding of 106 typed
headers and 19 typed bodies), formatting, and diff checks pass. This closes
only the identity-equality slice. Broad P18-V01 source/API/settings/input/
error/control-flow and reverse-export reconciliation, refreshed current-tree
P18-V02 gates, current-revision Ubuntu CI, and native Windows execution remain
open.

## P18-V02 current local server-free gate refresh — 2026-10-22 (`91f842f4`)

On code revision `91f842f4`, locked default and all-feature workspace tests,
strict all-target/all-feature workspace Clippy, formatting/diff checks, pinned
Maven `verify`, and explicit Java producer-core fixture verification pass.
Maven verifies 37 scenarios, 36 runtime route/VIP cases, and Java decoding of
106 typed headers and 19 typed bodies. Differential comparator self-test and
37-scenario manifest validation pass; fixed-seed fuzz smoke passes with 163
frame seeds, 11 message seeds, 2,784 mutations, 245 frame decodes, and 112
message decodes. Deterministic race/leak smoke passes 16 cases x 3.

Release facade examples build; the offline benchmark runs and strict
benchmark-target Clippy passes. Workspace metadata, dependency direction, and
license checks pass; all seven product package listings include `LICENSE`
and `NOTICE`. Four Core Unique ID tests pass under UTC and `Asia/Taipei`.
Windows GNU Core all-target checking and Consumer stack-capture `--no-run`
cross-compilation pass.

This closes the local server-free P18-V02 gate on `91f842f4`. Broad P18-V01
source/API/settings/input/error/control-flow and reverse-export reconciliation
remains open. Current-revision Ubuntu CI and native Windows execution remain
unverified. No push or external CI was run; Phase 19 Broker/Docker validation
remains separate. Run details are in
`rlogs/2026/10/22-phase-18-metadata-wrapper-identity-gate__20261022T163414Z.rlog`.


## P18-V01 `TopicMessageType.getMetricsValue()` default-locale parity — 2026-10-22 (`2887a032`)

Pinned Java 5.5.1 `TopicMessageType.getMetricsValue()` delegates to
`String.toLowerCase()` without a locale argument, so the JVM default locale
affects enum labels. Rust previously used ASCII-only lowercase and returned
`priority` under Turkish locale where Java returns `prıorıty`. Rust now samples
`LC_ALL`, `LC_CTYPE`, then `LANG` on each call and applies Unicode lowercase
with Turkish/Azeri `I` handling. The producer-core Java fixture temporarily
sets `Locale` to `tr-TR` and records all eight labels; a Rust subprocess test
sets `LC_ALL=tr_TR.UTF-8` and compares the complete vector. Fixture SHA-256:
`3743dd869f394f995422d6849e67741a14d44a643f74b3c9e23a253aaac52919`.

The focused Protocol API suite (4 tests), Common suite (87 unit, integration,
and doctest cases), default/all-feature locked workspace tests, strict
all-target/all-feature workspace Clippy, formatting/diff checks, and pinned
Maven `verify` with producer-core fixture verification pass. The Maven run
checks 37 scenarios, 36 route/VIP cases, and Java decoding of 106 typed headers
and 19 typed bodies. This closes the locale-sensitive metrics-label slice
only; broad P18-V01 source/API/settings/input/error/control-flow and
reverse-export reconciliation remains open.

## P18-V01 `RemotingHelper` default-locale code-description parity — 2026-10-22 (`a4136667`)

Pinned Java 5.5.1 builds `REQUEST_CODE_MAP` and `RESPONSE_CODE_MAP` by applying
no-argument `String.toLowerCase()` to each public code-field name. The map
values are static, and are observable in the public description helpers and
remoting metric labels. Under Turkish/Azeri default locale, ASCII `I` becomes
dotless `ı`; Rust previously returned ASCII-lowercase descriptions. Rust now
snapshots `LC_ALL`, `LC_CTYPE`, then `LANG` once and applies that mapping to
the existing ASCII descriptions. The pinned Java fixture sets `Locale` to
`tr-TR`, verifies all 166 request and 68 response codes plus unknown-code
fallbacks, and restores the original locale in `finally`. Fixture SHA-256:
`f6f8ebff1b2cfb33bd7234d8268b5747583f18f98f57fa4017d2af4f204a2573`.

The default and all-feature locked workspace suites, strict all-target/all-
feature Clippy, formatting/diff checks, pinned Maven `verify`, differential
self-test and 37-scenario manifest validation, fuzz smoke, deterministic
race/leak smoke (16 cases × 3), release examples, offline benchmark and strict
benchmark Clippy pass on `a4136667`. Workspace metadata/license/dependency
checks and all seven product package listings pass. Four Core Unique ID tests
pass under UTC and `Asia/Taipei`; Windows GNU Core all-target checking and the
Consumer stack-capture cross-compilation pass. The Maven verification run
also confirmed the locale fixture restores global JVM state: it leaves no
unrelated typed-header fixture drift.

This closes only the remoting code-description locale slice. Broad P18-V01
source/API/settings/input/error/control-flow and reverse-export
reconciliation, current-revision remote Ubuntu CI, native Windows execution,
and Phase 19 Broker/Docker validation remain open. No push or external CI was
run.

## P18-V01 `CompressionType.of` default-locale parity — 2026-10-22 (`c9a6f939`)

Pinned Java 5.5.1 trims the compression name and applies no-argument
`String.toUpperCase()`, which makes the public parser depend on the JVM default
locale. Under Turkish locale, `zlib` and `zLiB` uppercase to strings that do
not match `ZLIB`, while `zlıb` uppercases to `ZLIB`. Rust now applies the same
Turkish/Azeri `i`/`ı` mappings through the shared default-locale helper. The
Java runtime fixture records six inputs and their decoded values or exact
runtime-error messages; a Rust child test runs with `LC_ALL=tr_TR.UTF-8` and
compares each case.

The focused compression tests, locked default and all-feature workspace tests,
strict workspace Clippy, formatting/diff checks, and pinned Maven `verify`
pass. Maven verifies 37 scenarios, 36 route/VIP cases, and Java decoding of
106 typed headers and 19 typed bodies. This closes the compression parser
locale slice only. Broad P18-V01 API/settings/input/error/control-flow and
reverse-export reconciliation remains open, along with current-revision
remote Ubuntu CI and native Windows runtime evidence.

## P18-V01 `AclUtils.expandIP` default-locale parity — 2026-10-22 (`6155f4cc`)

Pinned Java 5.5.1 `AclUtils.expandIP` applies no-argument
`String.toUpperCase()` before expanding address segments. The public helper
does not validate input first, so Turkish locale changes direct calls such as
`expandIP("i::1", 8)` to contain dotted `İ`; dotless `ı` maps to `I`. Rust
uses the same `LC_ALL`, `LC_CTYPE`, then `LANG` locale selection and casing
rules. The Java fixture sets `tr-TR`, records two Turkish-letter cases plus a
valid IPv6 case, and restores the previous locale in `finally`.

Focused ACL utility and locale subprocess tests, locked default/all-feature
workspace suites, strict all-target Clippy, formatting/diff checks, and pinned
Maven `verify` pass. Maven verifies 37 scenarios, 36 route/VIP cases, and Java
decoding of 106 typed headers and 19 typed bodies. This closes only the ACL
helper locale slice; broad P18-V01 API/settings/input/error/control-flow and
reverse-export reconciliation, remote Ubuntu CI, and native Windows runtime
evidence remain open.

## P18-V01 `AclUtils.expandIP` Lithuanian combining-dot parity — 2026-10-22 (`505bbe97`)

Pinned Java `String.toUpperCase()` under `lt-LT` removes U+0307 immediately
following lowercase `i` or `j`. Because public `AclUtils.expandIP` accepts
unvalidated text and pads the resulting segments, this changes its returned
string. Rust now reproduces those observed sequences. The invoked ACL fixture
also checks `x` and dotless `ı` controls, where Java retains the combining dot;
the fixture restores the prior JVM locale in `finally`. Fixture SHA-256:
`da5c9ee1920ed89a997268afcc7eabd77fe53cb91315289179f5d305379ba340`.

The focused ACL locale tests, pinned Maven `verify`, locked default and
all-feature workspace tests, strict workspace Clippy, formatting/diff checks,
differential self-test and 37-scenario manifest, fuzz smoke, race/leak smoke,
release examples, offline benchmark and strict benchmark Clippy, workspace
metadata/package checks, timezone Unique ID tests, and Windows GNU cross-checks
pass on the source tree committed as `505bbe97`. This closes the observed
Lithuanian input slice only; broad P18-V01 reconciliation, current-revision
remote Ubuntu CI, native Windows execution, and Phase 19 Broker/Docker evidence
remain open.

## P18-V01 Lithuanian ACL casing context and UTF-16 segment padding — 2026-10-22 (`e6b31e97`)

The pinned Java Lithuanian uppercase rule removes U+0307 after any Unicode
`Soft_Dotted` character unless an intervening canonical combining class 0 or
230 blocks the context. Rust now uses ICU4X's `Soft_Dotted` and canonical
combining-class properties for this context. Invoked cases cover `i`/`j`,
precomposed `į`, Cyrillic `і`, nonblocking marks, blocking acute/CGJ marks,
repeated dots, and `x`/dotless-`ı` controls. The same direct `expandIP` fixture
also covers a supplementary character: Java pads by UTF-16 `String.length`,
so Rust now measures segment length in UTF-16 code units. The fixture restores
the original JVM locale. Fixture SHA-256:
`3364a617fc25de94be5ef2bae8342de404b48f9c7eb1548d20fcf13e6dc5412`.

Both Turkish and Lithuanian ACL locale subprocess tests pass. Default and
all-feature locked workspace tests, strict all-target/all-feature Clippy,
format/diff checks, and pinned Maven `verify` pass; Maven verifies 37
scenarios, 36 route/VIP cases, and Java decoding of 106 typed headers and 19
typed bodies. This closes only the tested casing-context and segment-padding
slice. Broad P18-V01 reconciliation, current-revision remote Ubuntu CI, native
Windows execution, and Phase 19 Broker/Docker evidence remain open.

## P18-V01 TopicConfig HashMap collision order — 2026-10-22 (`2f4daa9a`)

Pinned Java 5.5.1 `TopicConfig.encode()` and `toString()` iterate the supplied
attributes map. For Java `HashMap` keys `"BB"` and `"Aa"`, whose Java string
hashes collide, the observable output retains their bucket insertion order.
Rust's previous `BTreeMap` sorted them lexically and emitted a different
encoded string. `TopicConfigAttributeMap` now preserves insertion order with
`IndexMap`; serialization applies Java's hash spreading and bucket ordering,
with stable insertion order inside a bucket. The Java fixture covers both
insertion orders, direct `setAttributes`, `decode`, `encode`, and `toString`.
Rust equality/hash checks also confirm that map equality stays independent of
insertion order. Fixture SHA-256:
`1382505e8f49138746501dd77760358ae286e285f762b152f2223b912c8e6167`.

The focused TopicConfig API suite, locked default/all-feature workspace tests,
strict all-target/all-feature Clippy, pinned Maven `verify` (37 scenarios, 36
route/VIP cases, 106 typed-header cross-decodes, and 19 typed-body
cross-decodes), formatting/diff checks, workspace metadata/license checks,
and all seven product package listings pass. This closes only the tested
TopicConfig attribute-order slice. Broad P18-V01 reconciliation, current
Ubuntu/native Windows P18-V04 evidence, and Phase 19 Broker/Docker validation
remain open.

## P18-V01 `TopicConfig.attributes` nullable map keys — 2026-10-23

The Java setter accepts and retains a map containing a null key; the getter
returns that same map. The direct fixture verifies the null key separately from
the literal string key `"null"`, bidirectional mutation visibility, Fastjson
encoding, `toString()`, and decode semantics. Fastjson renders the null key as
`"null"`, allowing it and the string key to produce duplicate JSON property
names; it omits null-valued entries from JSON but retains them in the map and
string representation. A decoded JSON key `"null"` is an ordinary Java string
key. Rust's `TopicConfigAttributeMap` represents keys as `Option<String>` and
uses the same Java HashMap bucket order. The focused Protocol API test, full
Protocol package suite, and pinned Java fixture verification pass. Fixture
SHA-256: `d166aa9c873ed7f2ccf26a25dc5d43850e10b7eabcc15a62535c9d265fd9b237`.

This closes the direct nullable-attribute-key gap only; broad P18-V01 remains
PARTIAL, Phase 6 remains PARTIAL, and P6-V04 remains `PENDING_PHASE_19`.

## P18-V01 `MessageDecoder.decodeProperties(ByteBuffer)` input boundary — 2026-10-22 (`67d0d049`)

Pinned Java extracts properties using record offsets and ignores the declared
total store size. Rust now does the same instead of requiring full-record
validation first. Direct Java fixtures verify extraction when the stored size
is zero and verify that a positive-length malformed property string yields an
empty map. Fixture SHA-256:
`e2fab00b22e5954f54095fe7215848e4e317d6f62461cea166b5cd8be715b317`.

The focused Common differential, Common suite, default/all-feature workspace
tests, strict all-target/all-feature Clippy, format/diff checks, and pinned
Maven `verify` pass (37 scenarios, 36 route/VIP cases, 106 typed headers, 19
typed bodies). This closes this method's record-position and malformed-property
slice only. Broad P18-V01 source/API/settings/input/error/control-flow and
reverse-export reconciliation, current-revision remote Ubuntu CI, native
Windows runtime, and Phase 19 Broker/Docker evidence remain open.

## P18-V01 `MessageDecoder.decodeProperties(ByteBuffer)` cursor and truncation — 2026-10-22 (`2e1122cf`)

Java advances a positive-length property buffer to the end of its bytes,
returns null for zero or negative lengths with the cursor after the length
field, and throws `BufferUnderflowException` for truncated positive input with
the cursor at the property start. Rust now preserves the byte count alongside
the decoded map and matches these outcomes. Invoked fixtures cover the normal
record inputs and the zero-store-size, malformed text, nonpositive-length,
and truncated boundaries. Fixture SHA-256:
`699dfaae816b753105119193e3b268fe16fb2b83ad0199473a5665a7d6b76436`.

The Common suite, locked default/all-feature workspace tests, strict
all-target/all-feature Clippy, format/diff checks, and pinned Maven `verify`
pass. Maven verified 37 scenarios, 11 Java message-decoder fixtures, 106 typed
headers, and 19 typed bodies. Differential self-test and 37-scenario manifest,
bounded fuzz smoke, race/leak smoke, release examples, benchmark execution,
workspace metadata/license/package checks, UTC and Asia/Taipei Unique ID tests,
and Windows GNU cross-compiles also pass on this source revision. Broad P18-V01
reconciliation, remote Ubuntu CI, native Windows runtime, and Phase 19
Broker/Docker evidence remain open.

## P18-V01 full `MessageDecoder.decode(ByteBuffer)` store-size boundary — 2026-10-22 (`8acbf137`)

The pinned decoder reads the declared size into `MessageExt.storeSize` but
continues parsing from field lengths; zero, negative, and `Integer.MAX_VALUE`
headers do not stop a valid record. Rust now preserves that value as metadata,
uses parsed fields to determine consumed bytes, and applies the configured
size limit to those bytes rather than the untrusted declaration. The invoked
Java fixture captures decoded values and cursor positions for all three
declared sizes. A Rust batch regression also confirms the parsed positions
separate two such records. Fixture SHA-256:
`41ed0208cdf79da2491554ed08683e4f18b40d6bce1d21d246771dd58f0e15ae`.

The Common all-feature test suite, default and all-feature workspace suites,
strict workspace Clippy, formatting/diff checks, pinned Maven verification,
differential checks, fuzz and race/leak smoke, release examples, offline
benchmark, metadata/package checks, timezone tests and Windows GNU
cross-compiles pass. Broad P18-V01 reconciliation, remote Ubuntu CI, native
Windows runtime, and Phase 19 Broker/Docker evidence remain open.

## P18-V01 full message decode nonpositive body/property lengths — 2026-10-22 (`8ca83fc8`)

Java only reads body and property bytes when the respective signed length is
positive. Negative values are accepted as absent fields; the final buffer
position remains after that length field. Rust now follows the same branches,
while positive field lengths keep their existing bounds and limits. Invoked
fixtures compare full decoded snapshots and cursor positions for negative body
and property lengths. Fixture SHA-256:
`9cd0b64fe95643177c533d6cc2fd1f33c957e54cd3bd9559ecb07f6083939938`.

Common differential, default/all-feature workspace tests, strict workspace
Clippy, pinned Maven verification, differential checks, fuzz and race/leak
smoke, release examples and benchmark, workspace/package checks, timezone
tests, and Windows GNU cross-compiles pass. Broad P18-V01 reconciliation,
remote Ubuntu CI, native Windows runtime, and Phase 19 Broker/Docker evidence
remain open.

## P18-V01 Common `MessageDecoder` public-method surface crosswalk — 2026-10-22

Compared the fixture's 26 pinned Java `MessageDecoder` public signatures with
the Common module surface. The methods map to Rust's wire/compact encoders,
record and batch decoders, property helpers, message ID helpers, CRC helper,
and inner-message counter as summarized in
`docs/compatibility/PHASE_6_API_CONFIG_MAP.md`. The Java no-state utility
constructor is represented by Rust module functions. This is a signature
crosswalk only; existing invoked tests cover mapped cases, but broad input,
error, control-flow, settings and reverse-export reconciliation remains open.
At this 2026-10-22 checkpoint, the `createMessageId(ByteBuffer,ByteBuffer,long)`
helper's mutable scratch-buffer positions were not represented by the immutable
Rust host-byte slice. Source inspection found that pinned production call sites
discarded those local cursors, so the checkpoint recorded the difference as an
API-shape adaptation. The 2026-10-24 cursor follow-up below supersedes that
disposition; the history here records the earlier state, not the current API.

## P18-V01 compact inner-batch `MessageClientExt` subtype — 2026-10-22 (`adf93b21`)

Pinned Java `MessageDecoder.decodeMessage(MessageExt,List)` constructs a new
`MessageClientExt` for each compact inner message. Rust `decode_inner_batch`
previously returned a base `MessageExt`, so callers lost Java's virtual
`getMsgId`/`getOffsetMsgId` behavior and `setMsgId` no-op on the expanded
values. Rust now retains the `MessageClientExt` runtime marker on each result.
The invoked Java fixture and Common regression compare initial IDs, ignored
`setMsgId`, offset fallback, and live `UNIQ_KEY` precedence. Updated
`fixtures/messages/records.json` SHA-256:
`43a9740f697078e145e93eee6b9b3671482c4577a91a82d2fb72576b68feba65`.
Focused Common test, default and all-feature workspace tests, strict workspace
Clippy, formatting/diff checks, and pinned Maven verify (37 scenarios, 36
route/VIP cases, 106 typed headers, 19 typed bodies) pass. This closes the
compact expansion subtype slice only; broad P18-V01 reconciliation, current
remote Ubuntu CI, native Windows runtime, and Phase 19 Broker/Docker evidence
remain open.

## P18-V01 `MessageDecoder.countInnerMsgNum` cursor disposition — 2026-10-22

Pinned Java advances the supplied buffer while counting, leaving it at the
limit for valid compact batches. The only pinned production caller in
`SendMessageProcessor` wraps `messageExtBatch.getBody()` in a temporary
`ByteBuffer`, uses the returned count to set `PROPERTY_INNER_NUM`, then drops
the buffer; no production flow observes its final position. At this 2026-10-22
checkpoint, Rust's `count_inner_messages(&[u8])` used an immutable slice and the
cursor difference was recorded as an API-shape adaptation. The 2026-10-24
cursor follow-up below supersedes that disposition; the history here records
the earlier state, not the current API.

## P18-V01 Java `MessageDecoder.decodesBatch` malformed-tail control flow — 2026-10-22 (`4e718e35`)

Pinned Java `decode(ByteBuffer,...)` catches record exceptions, moves the
input position to its limit, and returns null; `decodesBatch` then stops and
returns its already-decoded prefix. An invoked Java fixture appends one
malformed byte to a valid encoded `MessageExt` and records the retained
`MessageClientExt`, topic, message IDs, count, and final buffer position/limit.
Rust adds `decode_many_as_client_compatible` while leaving strict
`decode_many` intact. Pull-result, POP reply, and both admin query owners use
the compatible path; pull processing reapplies IDs after compact expansion to
preserve inner-message ID semantics. `view_message` remains on one-record
decoding. Fixture SHA-256:
`d96af9e17c6a06adb1519eefcb66b546cea5c0e6bda52b3233a6b2fe5440ee6c`.

Default and all-feature workspace tests, strict Clippy, formatting/diff
checks, and pinned Maven verify pass. The Maven gate validates 37 scenarios,
36 route/VIP cases, 106 typed headers, 19 typed bodies, and all 11 message
decode fixtures. This closes the batch malformed-tail behavior slice only;
broad P18-V01 reconciliation, current-revision remote Ubuntu CI, native
Windows runtime, and Phase 19 Broker/Docker evidence remain open.

## P18-V01 Java `decodes` and generic `decodesBatch` malformed-tail parity — 2026-10-22 (`2c1f0a73`)

The pinned Java `MessageDecoder.decodes(ByteBuffer, readBody)` overload also
stops when record decoding returns null and preserves the successfully decoded
prefix. The invoked fixture now checks that overload plus `decodesBatch` with
`isClient=true` and `isClient=false`, including the result class and final
buffer position/limit. Fixture SHA-256:
`2be37d4f72d8e088a7bc52eb77de5d36192d48ba17de849619c02da31d6c2b97`.

Rust adds `decode_many_compatible` for the non-client Java batch mode and
routes `client_decodes` and `client_decodes_with_body` through the client
prefix-compatible helper. Strict `decode_many` and `decode_many_as_client`
continue returning errors. Common regressions cover prefix length, runtime
subtype, omitted body, and strict behavior.

Default and all-feature workspace tests, strict all-target/all-feature Clippy,
format/diff checks, and pinned Maven verify pass. The current-tree gates also
include the differential self-test and 37-scenario manifest, bounded fuzz
smoke, 16-case-by-3 race/leak smoke, release examples, and offline benchmark.
Workspace metadata and all seven package listings pass; four Core Unique ID
tests pass under both UTC and Asia/Taipei; Windows GNU Core all-target check
and Consumer test cross-compilation pass. This closes this decoder behavior
slice only; broad P18-V01 reconciliation, current-revision remote Ubuntu CI,
native Windows runtime, and Phase 19 Broker/Docker evidence remain open.

## P18-V01 `MessageDecoder.decodes(ByteBuffer)` default overload — 2026-10-22 (`2c1cd60a`)

The invoked Java malformed-tail fixture directly calls the no-argument
`decodes(ByteBuffer)` overload and records its valid prefix count, first result
class, and final cursor. The Common regression verifies the Java count, the
`MessageClientExt` type, and the cursor-at-limit outcome, then checks Rust
`client_decodes` returns the same prefix. Fixture SHA-256:
`46a18677fc673222639e53ee2871f4836fa427f97d284f918c1bdeb57129beb0`.

Common crate tests, Common all-target/all-feature strict Clippy, format/diff
checks, and pinned Maven verify pass. This closes this no-argument overload
slice only; broad P18-V01 reconciliation, current-revision remote Ubuntu CI,
native Windows runtime, and Phase 19 Broker/Docker evidence remain open.

## P18-V01 `MessageDecoder` body-skip malformed-tail coverage — 2026-10-22 (`608fe801`)

The invoked Java fixture additionally runs `decodes(..., false)` and
`decodesBatch(..., readBody=false, isClient=true/false)` on the valid-prefix
plus malformed-tail input. The Rust regression compares those prefix counts
with `client_decodes_with_body(false)` and `decode_many_compatible` configured
with `read_body=false`, while asserting Java leaves each buffer at its limit.
It also confirms the Rust client subtype and omitted body; Rust's byte-slice
APIs expose no mutable cursor. Fixture SHA-256:
`039d8dba7a268b43bc7e171f4f84bd739a0a3695c357e4ef7be35ded29c71a53`.

The focused Common test, default and all-feature workspace tests, strict
all-target/all-feature Clippy, formatting/diff checks, and pinned Maven verify
pass. This closes only the body-skip dimension of this malformed-tail decoder
slice; broad P18-V01 reconciliation, current-revision remote Ubuntu CI, native
Windows runtime, and Phase 19 Broker/Docker evidence remain open.

## P18-V01 strict batch helper reverse-export correction — 2026-10-22 (`95525c06`)

Reverse API review found public Rust `decode_many` and
`decode_many_as_client` strict helpers with no equivalent Java public batch
methods. The strict helpers were unused by production owners and gave Rust a
different public malformed-tail contract. They are removed from the public
Common API; production, tests, and fuzz harnesses now use Java-compatible
prefix-returning operations. Strict failure details remain internal to the
decoder. Generated Common Rustdoc confirms neither strict name is exported.

Default and all-feature workspace tests, strict workspace Clippy,
format/diff checks, Common Rustdoc, differential self-test and manifest
validation, bounded fuzz smoke, and pinned Maven verify pass. This closes this
public API mismatch only; broad P18-V01 reconciliation, current-revision
remote Ubuntu CI, native Windows runtime, and Phase 19 Broker/Docker evidence
remain open.

## P18-V02 current server-free gate refresh after decoder API correction — 2026-10-22 (`95525c06`)

The current revision passes default and all-feature workspace tests, strict
all-target/all-feature Clippy, format/diff checks, Common Rustdoc, pinned Maven
verify (37 scenarios, 11 message fixtures, 36 route/VIP cases, 106 typed
headers, 19 typed bodies), differential self-test and manifest validation
(37 scenarios). The validator confirms all pending prior-phase handoff source
IDs are registered; 30 live scenarios remain `PENDING_PHASE_19`.

The offline deterministic race/leak smoke passes 16 cases x 3 repetitions.
Bounded fuzz smoke passes 163 frame seeds, 11 message seeds, 2,784 mutations,
245 frame decodes, and 192 message decodes. Release facade examples and the
offline benchmark pass. Workspace metadata and all seven package listings
pass; four Core Unique ID tests pass under both UTC and Asia/Taipei. Windows
GNU Core all-target check and Consumer unit-test cross-compilation pass;
native Windows execution and remote Ubuntu CI remain unverified. This closes
the local P18-V02 gate on this revision only; broad P18-V01 source/API/settings/
input/error/control-flow and reverse-export reconciliation remains open.

## P18-V02 Remoting shutdown boundary verification — 2026-10-22 (`477825cc`)

After the public-executor shutdown-order correction, the Remoting library suite
passes (140 tests), its transport integration target passes (107 tests), and
the all-feature workspace library suites pass. Strict all-target/all-feature
workspace Clippy, formatting, and diff checks pass. The offline race/leak
runner now includes the shutdown-admission regression and completed 17 cases x
3 repetitions; its final count text was corrected and shell syntax/case count
were rechecked afterward. This is current-revision evidence for the affected
shutdown boundary, not a full P18-V02 refresh. Broad P18-V01 reconciliation,
remote Ubuntu CI, native Windows execution, and Phase 19 Broker/Docker
validation remain open.

## P18-V01 POP unsubscribe truncation — 2026-10-22

The pinned Java 5.5.1 `RebalanceImpl.truncateMessageQueueNotMyTopic` marks an
unsubscribed `PopProcessQueue` dropped and removes it from the POP table. The
invoked `popUnsubscribedTruncation` fixture records both outcomes. Rust's
rebalance regression compares those states and verifies the associated POP
worker is cancelled and removed from the worker map. The
`consumer-operations.json` fixture SHA-256 is
`a40640a3ee3a5e17d6845421ffd9808e0cad6152ac2952c8c8002fc168cb6aa1`.

The focused regression and all 255 Consumer library tests pass. Pinned Maven
verify passes, including Java fixture drift checks and Java decoding of 106
typed headers and 19 typed bodies. Workspace formatting and `git diff
--check` pass. This closes the unsubscribed POP cleanup branch only; broad
P18-V01 source/API/configuration/flow and reverse-export reconciliation,
current-revision remote Ubuntu CI, native Windows execution, and Phase 19
Broker/Docker evidence remain open.

## P18-V01 POP-to-PULL assignment transition — 2026-10-22

The pinned Java 5.5.1 assignment updater treats a PULL assignment as no longer
assigned to the POP table. The invoked `popToPullTransition` fixture seeds the
same queue in both owner tables, applies a PULL assignment, and records that
Java drops/removes the POP state while retaining the existing PULL queue. The
Rust rebalance regression checks those table outcomes and verifies POP worker
cancellation. The combined `consumer-operations.json` fixture SHA-256 is
`b414477a4a64b2c252d704704a70c7b1a89f2b6a5b1014531ca0200690edda36`.

All 256 Consumer library tests pass, pinned Maven verify passes with Java
fixture drift checks and Java cross-decodes of 106 typed headers and 19 typed
bodies, and formatting/diff checks pass. This closes the tested POP-to-PULL
cleanup branch only; broad P18-V01 reconciliation and Phase 19 live/platform
evidence remain open.

## P18-V01 POP in-flight callback after assignment removal — 2026-10-22

Review of the pinned Java 5.5.1 `DefaultMQPushConsumerImpl.popMessage` callback
showed that queue removal does not discard an already-issued POP response. A
FOUND response still records pull statistics, filters messages, ACKs filtered
messages, and submits accepted messages. The POP consume request then checks
`PopProcessQueue.isDropped()` before listener dispatch. Rust previously aborted
the queue worker at removal, losing this callback flow. Rebalance now removes
the worker from the assignment map and retires its task until the in-flight
response completes; the dropped-state check then ends the task before another
POP request. Shutdown and destroy drain retired task handles.

`removed_pop_queue_finishes_inflight_found_callback_before_retiring` scripts a
FOUND response with a filtered message and valid POP receipt, removes the queue
before releasing the response, and verifies the filtered-message ACK still
reaches the Broker and the retired task finishes. The existing
`popUnsubscribedTruncation` and `popToPullTransition` fixtures are unchanged;
`consumer-operations.json` SHA-256 remains
`b414477a4a64b2c252d704704a70c7b1a89f2b6a5b1014531ca0200690edda36`.

The focused regression and all 257 Consumer library tests pass. All-feature
workspace library suites pass (27, 257, 215, 154, 160 and 140 tests in the
nonempty suites). Strict all-target/all-feature workspace Clippy, formatting,
and diff checks pass. This closes the in-flight callback behavior for this
assignment-removal case only; broad P18-V01 reconciliation, current-revision
remote Ubuntu CI, native Windows execution, and Phase 19 Broker/Docker evidence
remain open.

## P18-V01 POP accepted-message consume guard after assignment removal — 2026-10-22

The in-flight FOUND regression now includes both a filtered message and an
accepted `blue` message. After rebalance drops and removes the POP queue, the
late callback ACKs only the filtered message and dispatches the accepted
message to a consume request. The request observes the dropped state and exits
before invoking the listener; the regression asserts zero listener calls and
exactly one Broker ACK. This matches Java's callback filtering/ACK order and
`ConsumeMessagePopConcurrentlyService.ConsumeRequest.run` dropped-queue guard.
The Java fixtures are unchanged; `consumer-operations.json` SHA-256 remains
`b414477a4a64b2c252d704704a70c7b1a89f2b6a5b1014531ca0200690edda36`.

The focused regression and all 257 Consumer library tests pass. Strict
all-target/all-feature workspace Clippy, Windows GNU Consumer test linking and
strict Clippy, formatting and diff checks pass. This closes the accepted
message dropped-queue guard for this POP removal path only; broad P18-V01,
current-revision remote Ubuntu CI, native Windows execution, and Phase 19 live
Broker/Docker evidence remain open.

## P18-V01 ProcessQueue try-unlock AtomicLong overflow — 2026-10-23 (`47cfbf13`)

Pinned Java stores `tryUnlockTimes` in an `AtomicLong` and calls
`incrementAndGet()` from the public `incTryUnlockTimes()` method and the Push
rebalance retry path. The increment wraps from `Long.MAX_VALUE` to
`Long.MIN_VALUE`. Rust now uses signed `i64` state with wrapping increments in
both public and generation-checked paths; getters, snapshots, and
`ConsumerProcessQueueInfo` retain the signed value. The invoked Java fixture
seeds the atomic counter at `Long.MAX_VALUE` and records the public increment
result. The Rust unit regression compares the Java result across the public
increment, queue-info fill, internal retry increment, and snapshot. Fixture
SHA-256: `9b14dc24b4f3214bfb9fab7d83ebe966083685c327409c7e76b1303de63d00db`.

Default and all-feature workspace tests, strict all-target/all-feature
workspace Clippy, formatting, and diff checks pass. Pinned Maven verify passes
37 differential scenarios and cross-decodes 106 typed headers and 19 typed
bodies. Differential self-test and 37-scenario manifest validation pass; fuzz
smoke passes 163 frame seeds, 11 message seeds, 2,784 mutations, 245 frame
decodes, and 192 message decodes; offline race/leak smoke passes 17 cases × 3.
Release examples and the offline benchmark pass, with the benchmark observation
recorded in `PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`. This closes the
try-unlock overflow path only; broad P18-V01 source/API/settings/input/error/
control-flow and reverse-export reconciliation remains PARTIAL.

## P18-V01 ProcessQueue null `cleanExpiredMsg` consumer — 2026-10-23

Pinned Java calls `pushConsumer.isConsumeOrderly()` before scanning the queue,
so a null consumer throws `NullPointerException` before any expiry work. Rust
adds `clean_expired_msg_nullable(None)` and returns the matching typed cleanup
error. The direct Java expired-message fixture and Rust cleanup comparison
check the exception class. Fixture SHA-256:
`2934eb15b2fb599bd6309fd8d8dc394ad0deef817060bc3883373da0942ce40c`.

The focused Rust expiry comparison, targeted Java expired-message fixture
verification, strict Consumer Clippy, and formatting/diff checks pass. This
closes the null-consumer input branch only; broad P18-V01 source/API/settings/
input/error/control-flow and reverse-export reconciliation remains PARTIAL.

## P18-V01 ProcessQueue negative batch-size error — 2026-10-23 (`c3b53fcb`)

Pinned Java `ProcessQueue.takeMessages(int)` constructs an `ArrayList` with the
requested capacity before reading the clock or locking the queue. A negative
batch size throws `IllegalArgumentException: Illegal Capacity: -1` and leaves
`lastConsumeTimestamp` unchanged. Rust previously asserted and panicked; the
public `take_messages` now returns `ProcessQueueError::InvalidBatchSize` with
the Java exception text before changing queue state. Its internal positive
batch call site handles the typed result. The invoked Java fixture and Rust
facade test compare the error text and seeded timestamp. Fixture SHA-256:
`63be1fa9e086d577e01c6a5ac1eba6089e30f237f9bf03b178832dc22496772d`.

Default and all-feature workspace tests, strict all-target/all-feature
workspace Clippy, formatting, and diff checks pass. Pinned Maven verify passes
37 differential scenarios and cross-decodes 106 typed headers and 19 typed
bodies. Differential self-test and manifest validation pass for 37 scenarios;
fuzz smoke passes 163 frame seeds, 11 message seeds, 2,784 mutations, 245 frame
decodes, and 192 message decodes; offline race/leak smoke passes 17 cases × 3.
Release examples and the offline benchmark pass, with the current benchmark
recorded in `PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`. This closes the negative
batch-size error path only; broad P18-V01 source/API/settings/input/error/
control-flow and reverse-export reconciliation remains PARTIAL.

## P18-V01 ProcessQueue null `containsMessage` input — 2026-10-23

Pinned Java `ProcessQueue.containsMessage(null)` returns `false` before it
acquires the tree lock. Rust adds `contains_message_nullable(Option<&MessageExt>)`
to model this explicit null input while retaining the non-null method. The
invoked Java fixture and Rust facade test compare the `false` result. Fixture
SHA-256: `edb43a544374bb1c5ae969bcdaa09ea3746a310b3d28dc86b459a18ecfcff551`.

The focused Consumer facade test, all 260 Consumer unit tests and remaining
Consumer integration tests (including all features), strict Consumer Clippy,
format/diff checks, and pinned Maven verify pass. This closes the null-message
input branch only; broad P18-V01 source/API/settings/input/error/control-flow
and reverse-export reconciliation remains PARTIAL.

## P18-V01 ProcessQueue null `fillProcessQueueInfo` argument — 2026-10-23

Pinned Java catches and logs the exception raised when
`fillProcessQueueInfo(null)` dereferences the DTO, releases its read lock, and
returns normally. Rust adds `fill_process_queue_info_nullable(None)` as an
observable no-op. The invoked Java fixture and Rust facade comparison exercise
the null input. Fixture SHA-256:
`57e2890ebeace93ecf00e2509e2bf7557c28f681ad19141f81430fecfe57ddf3`.

The focused Consumer facade test, strict Consumer Clippy, formatting/diff
checks, and targeted pinned Java facade fixture verification pass. This closes
the null-info input branch only; broad P18-V01 source/API/settings/input/error/
control-flow and reverse-export reconciliation remains PARTIAL.

## P18-V01 ProcessQueue null `putMessage` list — 2026-10-23

Pinned Java throws `NullPointerException` when the enhanced-for loop begins on
a null list; no queue entries or counters have changed at that point. Rust adds
`put_message_nullable(None)` and returns the matching typed error before
mutation. The invoked Java fixture and Rust facade comparison check the
exception class, empty tree, count, and size. Fixture SHA-256:
`be3d3a818e11a39838b83bafed81ec0f67bec266c200a5d13af374a69af91bb4`.

The focused ProcessQueue facade test, targeted pinned Java facade fixture
verification, strict Consumer Clippy, and formatting/diff checks pass. This
closes the null-list input branch only; broad P18-V01 source/API/settings/input/
error/control-flow and reverse-export reconciliation remains PARTIAL.

## P18-V01 ProcessQueue null `removeMessage` list — 2026-10-23

Pinned Java catches the null-list exception after it updates
`lastConsumeTimestamp`. With ready messages, it returns the precomputed
`queueOffsetMax + 1` and preserves queue entries and counters. Rust adds
`remove_message_nullable(None)` with the same result and state transition. The
invoked Java fixture and Rust facade comparison check the return offset,
timestamp update, count, size, and tree. Fixture SHA-256:
`357c266f6bea2c4df5b066d334b75b28314202d1d198a34bd270115345b9096f`.

The focused ProcessQueue facade test, targeted pinned Java facade fixture
verification, strict Consumer Clippy, and formatting/diff checks pass. This
closes the null-list removal branch only; broad P18-V01 source/API/settings/
input/error/control-flow and reverse-export reconciliation remains PARTIAL.

## P18-V01 ProcessQueue null `makeMessageToConsumeAgain` list — 2026-10-23

Pinned Java propagates the null-list exception from its enhanced-for loop
before changing queue state. Rust adds
`make_message_to_consume_again_nullable(None)` and returns the typed Java error
with the current queue preserved. The invoked Java fixture and Rust facade
comparison check exception class, count, size, and tree. Fixture SHA-256:
`a9fb84f276c05f4f55481b03fa2316cf28ec0ca691ecfbe1ae027522abfd6365`.

The focused ProcessQueue facade test, targeted pinned Java facade fixture
verification, strict Consumer Clippy, and formatting/diff checks pass. This
closes the null-list requeue branch only; broad P18-V01 source/API/settings/
input/error/control-flow and reverse-export reconciliation remains PARTIAL.

## P18-V01 ProcessQueue null `putMessage` element and partial state — 2026-10-23

Pinned Java inserts valid prefix messages and updates `msgSize` immediately,
then a null element throws before `msgCount.addAndGet`, dispatch-state update,
or final `MAX_OFFSET` parsing. Rust adds `put_message_items_nullable` and
preserves those prefix mutations while returning a typed null-element error.
The invoked Java fixture and Rust facade comparison check the immediate
count/size/tree state and the subsequent valid insertion's dispatch outcome.
Fixture SHA-256:
`01133da592b2f5c73b5fb379e14626651887cf8c0543875b928f266f02221aa1`.

The focused ProcessQueue facade test, targeted pinned Java facade fixture
verification, strict Consumer Clippy, and formatting/diff checks pass. This
closes the null-element prefix branch only; broad P18-V01 source/API/settings/
input/error/control-flow and reverse-export reconciliation remains PARTIAL.

## P18-V01 ProcessQueue null requeue element and prefix state — 2026-10-23

Pinned Java moves valid prefix messages from the consuming map to the ready map
before a later null element throws. Rust adds
`make_message_to_consume_again_items_nullable`; it applies that prefix, clears
its in-flight ownership, keeps counters and dispatch state, then returns a
typed null-element error. The invoked Java fixture and Rust facade comparison
check the requeued prefix and following insertion outcome. Fixture SHA-256:
`2b1daf803430969e4b7bd57f4bc54e9794b3d107b7cc0a1fd5fb1062764efdb7`.

The focused ProcessQueue facade test, targeted pinned Java facade fixture
verification, strict Consumer Clippy, and formatting/diff checks pass. This
closes the null-element requeue branch only; broad P18-V01 source/API/settings/
input/error/control-flow and reverse-export reconciliation remains PARTIAL.

## P18-V01 ProcessQueue null removal element and prefix state — 2026-10-23 (`463db95a`)

Pinned Java catches the exception from a null element in `removeMessage` after
removing valid prefix entries from the ready tree and subtracting their body
sizes. It skips the aggregate count update and returns the precomputed
`queueOffsetMax + 1`; the Rust `remove_message_items_nullable` adapter matches
these partial effects. The invoked Java fixture and Rust facade comparison
check timestamp, tree, count, size, return offset, and the next insertion's
dispatch behavior. Fixture SHA-256:
`29cc00f7cf9b8194f08007a12c5e75f0499a5601c140a78855b14cb244bef771`.

The focused Consumer facade test, targeted pinned Java facade fixture
verification, all 260 Consumer library tests and all 69 Consumer integration
tests with all features, strict Consumer Clippy, formatting, and diff checks
pass. This closes the null-element removal branch only; broad P18-V01
source/API/settings/input/error/control-flow and reverse-export reconciliation
remains PARTIAL.

## P18-V01 ProcessQueue declared-method crosswalk — 2026-10-23

The pinned Java facade fixture's 32 declared public `ProcessQueue` methods now
have an explicit Rust owner mapping, including nullable-input adapters and
Rust-specific result wrappers. The focused audit regression verifies the
fixture denominator, every Java method name, each documented mapping, and the
corresponding Rust owner method. The facade fixture is unchanged at SHA-256
`29cc00f7cf9b8194f08007a12c5e75f0499a5601c140a78855b14cb244bef771`.

The focused crosswalk regression and formatting/diff checks pass. This closes
the `ProcessQueue` method-inventory slice only; broad P18-V01 source/API/
settings/input/error/control-flow and reverse-export reconciliation remains
PARTIAL.

## P18-V01 ProcessQueue Rust public support-surface reverse audit — 2026-10-23

The Rust ProcessQueue constructor, nullable adapters, AtomicLong-style
counter handle, lock-backed tree view and guards, and typed error wrappers now
have documented Java mappings or lifecycle dispositions. The focused reverse
audit regression checks each listed helper surface against the public owner
source and compatibility table.

At revision `533d9713`, the focused Java-to-Rust method crosswalk, strict
workspace Clippy, formatting/diff checks, default and all-feature workspace
suites, and full offline Maven `verify` pass. The new reverse-support-surface
regression and strict Consumer Clippy also pass. This closes the ProcessQueue
reverse-support-surface slice only; broad P18-V01
source/API/settings/input/error/control-flow and reverse-export reconciliation
remains PARTIAL.

## P18-V01 PopProcessQueue null queue-info argument — 2026-10-23 (`0a88fcaa`)

Pinned Java `fillPopProcessQueueInfo(null)` throws
`java.lang.NullPointerException`. Rust now exposes
`fill_pop_process_queue_info_nullable` and returns the same exception class as
`PopProcessQueueError::NullInfo`; the invoked Java facade fixture and Rust
comparison cover the result. The regenerated facade fixture SHA-256 is
`a32d9a980e5079fa5fccaa6f6d46287946883099550e0bc4f04854892b0ca065`.

## P18-V01 PopProcessQueue declared and reverse API crosswalk — 2026-10-23

The pinned Java facade fixture records 11 declared public `PopProcessQueue`
methods. The audit maps all 11 to Rust methods or `Display::to_string`, checks
the complete method-name denominator, and records the constructor, nullable
DTO adapter, and public error type in the Rust-side reverse inventory.

The direct PopProcessQueue comparison, Java-to-Rust method crosswalk, reverse
support-surface regression, targeted Java facade fixture verification, strict
Consumer Clippy, and formatting/diff checks pass. This closes the PopProcessQueue
null-info and method-inventory slices only; broad P18-V01 source/API/settings/
input/error/control-flow and reverse-export reconciliation remains PARTIAL.

## P18-V02 current offline rerun — 2026-10-23 (`bd2fc5b6`)

On macOS AArch64 with Rust 1.85.0 and the pinned Java 5.5.1 reference, both
`cargo test --offline --workspace --locked` and
`cargo test --offline --workspace --all-features --locked` pass. Strict
all-target/all-feature workspace Clippy, workspace format check, and
`git diff --check` pass. Differential self-test and manifest validation pass
for all 37 scenarios. Offline Maven `verify` passes, including 37 scenarios,
106 Java-decoded typed headers, 19 Java-decoded typed bodies, and fixture drift
checks. The bounded fuzz smoke reports 163 frame seeds, 11 message seeds, 2,784
mutations, 245 frame decodes, and 192 message decodes. The deterministic race /
leak harness passes 17 cases × 3 repetitions. Release facade examples compile.
The fixed-input offline benchmark completes; its machine-specific observations
are appended to
[`PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`](PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).

This refresh closes the current macOS offline P18-V02 checks and records the
37-scenario handoff manifest for P18-V04. It does not close P18-V01: broad Java
5.5.1 source/API/settings/input/error/control-flow and Rust reverse-export
reconciliation remains PARTIAL. Current-revision remote Ubuntu CI, native
Windows runtime execution, and Phase 19 Broker/Docker evidence remain open.

## P18-V01 ProcessQueue / PopProcessQueue `Default` adapter reverse audit — 2026-10-23 (`6371afa5`)

Both public Rust queue types implement `Default`, and each implementation
delegates directly to its Java-mapped no-argument constructor. The reverse
crosswalk now records these as idiomatic constructor adapters. A facade
regression compares their stable initial flags, message state, and counters to
`new()`; time-varying timestamp fields are excluded. The reverse-surface tests
also require the source implementation and documented disposition to remain
present.

All 260 Consumer library tests and 74 Consumer integration tests pass with all
features. Strict all-target/all-feature Consumer Clippy, workspace format
check, and `git diff --check` pass. This closes the two queue `Default`
dispositions only; broad P18-V01 source/API/settings/input/error/control-flow
and reverse-export reconciliation remains PARTIAL.

## P18-V01 Remoting `ResponseFuture` pinned-source reverse audit — 2026-10-23

The exact Java source at pinned client commit
`efa1be59d6fc747ad220f036c86030f05f0419f9` confirms the two declared
`ResponseFuture` constructors and all 20 public methods recorded by
`response-future-api.json`. The Rust constructor adapters collapse the
request-optional overload into `Option<RemotingCommand>` and expose the nullable
release-once helper separately; the transport-owned `OwnedSemaphorePermit`
constructor remains private. Callback failure ordering, timeout comparison,
response-latch mutation, release-once behavior, and interruption mapping are
already covered by the response-future fixture/tests and compatibility tables.
The only recorded differences remain JVM-thread interruption of
`waitResponse` and diagnostic formatting of Java object identities. No
unmapped Java capability or extra Rust transport capability was found in this
slice; broad P18-V01 reconciliation remains PARTIAL.

## P18-V01 `ProcessQueue` public lock fields — 2026-10-23

Pinned Java `ProcessQueue` exposes `REBALANCE_LOCK_MAX_LIVE_TIME` and
`REBALANCE_LOCK_INTERVAL` as public static fields. Rust previously consumed
these settings internally but did not expose the public values. `ProcessQueue`
now provides `rebalance_lock_max_live_time_millis()` and
`rebalance_lock_interval_millis()` accessors. Both values are captured together
on first ProcessQueue access and shared by orderly scheduling and lock-expiry
checks, matching Java class initialization; malformed values return a Rust
`Result` at the accessor boundary. The common property parser now accepts
Unicode decimal digits as Java `Long.parseLong` does.

The pinned orderly fixture comparison, reverse-surface crosswalk check, all
260 Consumer library tests and 74 Consumer integration tests pass with all
features. Strict all-target/all-feature Consumer Clippy, workspace formatting,
and `git diff --check` pass. This closes the two `ProcessQueue` public-field
mapping rows; broad P18-V01 remains PARTIAL.

## P18-V01 `MQClientAPIImpl.sendSmartMsg` and Producer request-code selection — 2026-10-23

Pinned-source settings reconciliation found that the private Java
`sendSmartMsg` switch was not applied by the Rust Producer owner and that a
reply message marked `MSG_TYPE=reply` still selected the ordinary send code.
Rust now caches `org.apache.rocketmq.client.sendSmartMsg` when the Core API is
constructed, defaults it to true, and follows Java `Boolean.parseBoolean`
handling. The hidden Core selector maps normal, reply and batch code branches;
Producer preparation recognizes Java's exact `reply` marker and uses that
selector before invoking the typed RPC.

The pinned Java `InvokedRpcOracle` directly exercises normal, reply and batch
calls with the static switch false and true. Isolated Rust Core subprocesses
cover missing, false, uppercase true, whitespace and malformed values; an
isolated Producer subprocess verifies normal/reply/batch selection under both
switch values. Focused Core and Producer tests pass. This closes the named
setting and high-level request-code gap only; broad P18-V01 source/API/settings/
input/error/control-flow and reverse-export reconciliation remains PARTIAL.

## P18-V02 current macOS offline gates — 2026-10-23 (`9fdea903`)

On macOS AArch64 with Rust 1.85.0, the default and all-features locked
workspace tests pass, as do strict all-target/all-feature workspace Clippy,
format checking, and `git diff --check`. The pinned Java 5.5.1 Maven `verify`
passes with 37 scenarios, 106 typed headers, 19 typed bodies, and fixture drift
checks. Differential comparator self-test and all 37 manifest scenarios pass.
Fixed-seed fuzz smoke passes with 163 frame seeds, 11 message seeds, 2,784
mutations, 245 frame decodes, and 192 message decodes. The deterministic
race/leak smoke passes 17 cases × 3 repetitions, release workspace examples
build, and the offline benchmark completes; its current-tree sample is recorded
in [`PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`](PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).

These results close the current local P18-V02 checks at this code revision.
Broad P18-V01 Java source/API/settings/input/error/control-flow and Rust
reverse-export reconciliation remains PARTIAL. Current-revision remote Ubuntu
CI, native Windows runtime execution, and Phase 19 Broker/Docker evidence
remain open.

## P18-V02 supplementary Windows cross-target and rustdoc checks — 2026-10-23 (`9fdea903`)

With the installed MinGW cross-compiler selected through
`CC_x86_64_pc_windows_gnu`,
`cargo check --offline --workspace --all-features --locked --target
x86_64-pc-windows-gnu` passes on the current Phase 18 code. The native Windows
runtime check remains a separate requirement and is still open. A Windows
MSVC cross-check could not start because the host clang invocation lacks the
Windows C runtime headers (`stdlib.h`); this is an environment limitation.

The targeted public API documentation command
`RUSTDOCFLAGS='-D warnings' cargo doc --offline -p rocketmq-classic-core -p
rocketmq-classic-consumer --all-features --locked --no-deps` passes. The wider
workspace documentation command reaches the integration crate, then triggers
a rustdoc metadata-decoder compiler ICE in Rust 1.85.0 while indexing an opaque
`Message::body` return type; this toolchain failure does not occur in the
targeted Core and Consumer documentation run.

The follow-up command
`CC_x86_64_pc_windows_gnu=/opt/homebrew/bin/x86_64-w64-mingw32-gcc cargo
check --offline --workspace --all-targets --all-features --locked --target
x86_64-pc-windows-gnu` also passes, compiling all test and example targets for
the Windows GNU target. It adds cross-target compile evidence only; the native
Windows runtime check remains open.

## P18-V01 Producer async backpressure read-lock lifetime — 2026-10-23 (`be58bf98`)

Pinned Java `DefaultMQProducerImpl.sendMessageAsync` holds the producer's
count or byte read lock across the timed semaphore acquisition, then releases
it after the acquisition succeeds or times out. Runtime limit setters take
the corresponding write lock before updating the configured limit and
semaphore permits. Rust previously held its read lock only while checking
permit state, so a concurrent limit increase could wake an acquisition that
Java would keep pending behind the read lock.

`AdjustableSemaphore::acquire` now retains a `Send` internal read lease for
the complete async permit wait, and releases a canceled waiter from the FIFO
before dropping that lease. Public `BackpressureReadGuard` remains
thread-bound. The source-derived regression
`runtime_resize_waits_for_a_pending_acquisition_like_java` verifies that a
resize waits for the in-progress acquisition to time out before applying the
new limit. The existing invoked Java `runtimeSettings` fixture and
`runtime_resize_preserves_in_flight_count_like_the_java_fixture` continue to
cover the adjusted permit counts and subsequent in-flight release.

The Core ACL timeout-only test now cancels its scripted peer before client
shutdown. Under parallel all-features execution, its 25 ms budget can expire
before the local peer receives a complete request frame; awaiting that peer
could then stall until the idle connection closed. The assertion remains on
the typed timeout result, and the peer no longer delays the suite.

The default and all-features locked workspace suites pass. Strict workspace
Clippy, formatting and diff checks pass; Producer package tests pass (155
library, 66 integration, and 7 doctests). Pinned Maven `verify` passes all 37
scenarios and Java cross-decodes 106 typed headers and 19 typed bodies.
Differential self-test and all 37 manifest scenarios pass; fuzz smoke covers
163 frame seeds, 11 message seeds and 2,784 mutations; deterministic race/leak
smoke passes 17 cases × 3. Windows GNU all-target/all-feature cross-compilation,
release example compilation, the offline benchmark and workspace metadata
checks pass. This closes the pending-acquisition resize slice only; broad
P18-V01 source/API/settings/input/error/control-flow and reverse-export
reconciliation remains PARTIAL. Current-revision remote Ubuntu CI, native
Windows runtime execution and Phase 19 Broker/Docker evidence remain open.

## P18-V01 inherited `ServiceThread` controls — 2026-10-23 (`bd4e073c`)

Pinned Java 5.5.1 exposes `ServiceThread.start`, both shutdown overloads,
`getJoinTime`, `makeStop`, `wakeup`, `isStopped`, `isDaemon`, and
`setDaemon`. The Rust Core now returns `RuntimeServiceThread` handles for the
runtime-owned rebalance and pull services. They map the 90,000ms join interval,
idempotent start/shutdown state, stop and wake behavior, daemon configuration
and start-time daemon snapshot, bounded join, pull queue interruption, and the
PullMessageService scheduled-executor shutdown override. Java's private wait
point hooks remain internal implementation details.

At code revision `bd4e073c`, the default and all-feature locked workspace test
suites pass, as do strict all-target/all-feature workspace Clippy, formatting,
`git diff --check`, pinned offline Maven `verify` (37 scenarios, 106 typed
headers and 19 typed bodies), comparator self-test and 37-scenario manifest
validation, workspace metadata checks, bounded fuzz smoke (163 frame seeds,
11 message seeds and 2,784 mutations), 17 deterministic race/leak cases × 3,
release example compilation, and the offline benchmark. The service-specific
regressions cover inherited controls, pre-start PullMessageService shutdown,
stopped-service task admission, and restart after full runtime shutdown.

This closes the inherited ServiceThread API/control slice only. P18-V01's broad
Java source/API/settings/input/error/control-flow and Rust reverse-export
reconciliation remains PARTIAL. Current-revision remote Ubuntu CI, native
Windows runtime execution, and Phase 19 Broker/Docker evidence also remain
open.

## P18-V01 retained service-owner lifetime — 2026-10-23 (`1f6cd086`)

Pinned Java `PullMessageService` and `RebalanceService` each hold a strong
reference to their `MQClientInstance`. Rust's `RuntimeServiceThread` previously
held only a `Weak<ClientRuntime>`, so retaining the Java-equivalent service
handle did not retain its owner. The handle now stores a strong `Arc` and
continues to share the same per-service lifecycle state. The regression
`pull_service_handle_retains_its_client_runtime_owner` drops the manager and
runtime variables before starting and shutting down the retained service.

At code revision `1f6cd086`, the default and all-feature workspace suites,
strict all-target/all-feature workspace Clippy, format/diff checks, offline
Maven `verify` (37 scenarios, 106 typed headers and 19 typed bodies),
differential comparator self-test and 37-scenario manifest validation,
workspace metadata, bounded fuzz smoke (163 frame seeds, 11 message seeds,
2,784 mutations), race/leak smoke (17 cases × 3), release examples, the offline
benchmark, Windows GNU all-target/all-feature cross-check, and targeted
warning-free Core rustdoc pass.

This closes the service-handle owner-retention slice only. Broad P18-V01 Java
source/API/settings/input/error/control-flow and Rust reverse-export
reconciliation remains PARTIAL; remote Ubuntu CI and native Windows runtime
execution remain open. Phase 19 Broker/Docker validation is not claimed.

## P18-V01 Rustdoc workspace reverse-name continuation — 2026-10-23

The nightly workspace Rustdoc JSON scan found 22 public item names absent from
the exact-name compatibility crosswalk. Each was checked against Java source
at pinned commit `efa1be59d6fc747ad220f036c86030f05f0419f9`: 17 Protocol
collection aliases map Java DTO fields; `GetKvConfigResponseHeader` maps Java
`GetKVConfigResponseHeader`; two Core task result/error types map the
`ScheduledExecutorService` / `Future` behavior already exposed by the client
and pull schedulers; and two Common types map the Java topic-property map and
`TopicMessageType` exception paths. The crosswalk is recorded in
[`PHASE_6_API_CONFIG_MAP.md`](PHASE_6_API_CONFIG_MAP.md). No new wire
operation, persistent setting, or client capability was found in these names.

This closes the reverse-name disposition for this scanned set only. Broad
P18-V01 Java source/API/settings/input/error/control-flow and Rust
reverse-export reconciliation remains PARTIAL.

## P18-V01 Remoting command version parity — 2026-10-23 (`838286ca`)

Pinned Java 5.5.1 `MQClientAPIImpl` sets `RemotingCommand.REMOTING_VERSION_KEY`
to `MQVersion.CURRENT_VERSION` (515). The Java command factories apply the
cached `RemotingCommand.configVersion` to API requests and response commands.
Rust now applies that version to every Core `MQClientApi` request builder and
to Core runtime replies, Consumer admin processor replies, and generic
Remoting processor errors. The hidden
`RemotingCommand::initialize_client_api_version` hook maps the Java static
initializer while preserving cached property and initialization order.
`InvokedRpcOracle` asserts the Java version on each dispatched request, the
Rust wire regression asserts version 515, and a subprocess regression covers
request and response factory ordering.

Verification on this revision: `cargo fmt --all -- --check`; strict
all-target/all-feature workspace Clippy; locked default and all-feature
workspace tests; the Asia/Taipei unique-ID fixture; differential self-test and
37-scenario manifest validation; fuzz smoke (163 frame seeds, 11 message
seeds, 2,784 mutations, 245 frame decodes, 192 message decodes); deterministic
race/leak smoke (17 cases × 3); pinned offline Maven `verify` (37 scenarios,
106 typed headers, 19 typed bodies); release examples; benchmark compilation;
and workspace metadata all passed. This closes the command-version parity
slice only. Broad P18-V01 reconciliation, current-revision remote Ubuntu and
native Windows evidence, and Phase 19 Broker/Docker evidence remain open.

## P18-V01 `MQClientAPIFactory` per-client domain discovery — 2026-10-23 (`df87b7b2`)

Pinned Java `MQClientAPIFactory.createAndStart` invokes `fetchNameServerAddr`
on each newly constructed API client and schedules refresh against that same
instance. `MQClientAPIImpl.nameSrvAddr` is instance state. Rust previously
kept one factory-wide current address, so a later domain response overwrote
the NameServer list on all earlier clients. Core now tracks one address per
pool slot and applies startup/refresh discovery results only to that client;
the explicit factory address-change API still updates every slot.

The invoked `RuntimeApiFactoryFixtures` case serves distinct responses from a
local HTTP endpoint and records the resulting address list for both clients.
The Rust `factory_domain_discovery_keeps_each_clients_address_independent`
test consumes the same fixture. Core all-feature library tests (238), strict
all-target/all-feature Core Clippy, formatting and diff checks, and pinned
offline Maven `verify` (37 scenarios) pass. This closes the per-client domain
discovery behavior slice only; broad P18-V01 source/API/settings/input/error/
control-flow and reverse-export reconciliation remains PARTIAL. Native
Windows runtime, current-revision remote Ubuntu CI, and Phase 19 Broker/Docker
evidence remain open.

## P18-V01 `MQClientAPIFactory` per-client refresh scheduling — 2026-10-23 (`681a8acf`)

Pinned Java creates a `ScheduledExecutorService.scheduleAtFixedRate` task for
each API client, with a 10-second initial delay and 120-second period. Rust
previously used one timer worker to fetch all pool slots sequentially. Core now
owns one cancellable Tokio worker per pool slot; each applies discovery only
to its own client address, and shutdown signals and joins every worker.

The invoked Java factory fixture records two scheduled tasks after starting
two domain clients. A paused-time Core regression compares the Rust per-slot
task count with that fixture, advances through the first refresh, and checks
that the clients keep distinct results. All-feature workspace tests (239 Core
unit tests), strict all-target/all-feature workspace Clippy, formatter and
diff checks, and pinned offline Maven `verify` (37 scenarios) pass. This
closes the per-client refresh schedule slice only. Broad P18-V01 source/API/
settings/input/error/control-flow and reverse-export reconciliation remains
PARTIAL; native Windows runtime, current-revision remote Ubuntu CI, and Phase
19 Broker/Docker evidence remain open.

## P18-V01 `MQClientAPIFactory` provider callbacks per client — 2026-10-23 (`88a9578e`)

Pinned `MQClientAPIImpl` constructs a `DefaultTopAddressing` instance and
registers itself as that instance's change callback during API construction.
The factory does this for each pool client before its initial domain lookup.
Rust previously registered one callback during factory construction and
broadcast provider callback events to the entire client pool. Rust now
registers an indexed callback before each slot's first fetch and applies the
event only to that slot. Direct factory address-change calls still broadcast
to all slots, and zero-client starts register no callbacks.

`factory_registers_and_dispatches_name_server_callbacks_per_client` checks
callback count/timing, individual address updates and return values, and the
zero-client path. Core all-feature tests (240), strict all-target/all-feature
Core Clippy, formatting and diff checks pass. This closes the provider
callback ownership slice only. Broad P18-V01 source/API/settings/input/error/
control-flow and reverse-export reconciliation remains PARTIAL; native
Windows runtime, current-revision remote Ubuntu CI, and Phase 19 Broker/Docker
evidence remain open.

## P18-V01 `MQClientAPIFactory` null client-array slots — 2026-10-23 (`1b1d4e2a`)

Pinned Java `onNameServerAddressChange` iterates the public mutable client
array in order. A null slot raises `NullPointerException` when reached, after
any earlier client has already received the address update. The same failure
occurs for a null address because Java still invokes each client in sequence.
Rust now walks the same slot order, preserves earlier updates, and returns
`NullClient(index)` at the first null slot.

The invoked factory fixture records both error names and each partially
updated address array. The Core factory regression consumes those values for
both a non-null address and a null address, then restores the slot for the
existing update/shutdown assertions. Default/all-feature workspace tests,
strict workspace Clippy, formatter/diff checks, offline Maven `verify` (37
scenarios), differential self-test/manifest validation (37 scenarios),
metadata/package checks, fuzz and race/leak smoke, release examples, offline
benchmark, and Windows GNU cross-compilation passed. This closes the null-slot
failure-order slice only; broad P18-V01 reconciliation remains PARTIAL, with
native Windows execution, remote Ubuntu CI, and Phase 19 Broker/Docker
evidence open.

## P18-V01 `MQClientAPIFactory` provider-injection export — 2026-10-23

The Rust-only `MQClientApiFactory::with_top_addressing` setter was public even
though pinned Java 5.5.1 has no corresponding factory API. It is now compiled
only for unit tests and remains private; its provider-backed tests still
exercise Java's per-client lookup, refresh, callback, and shutdown behavior.
A compile-fail Rustdoc example verifies an external caller cannot use this
setter. Seven factory tests, all 20 Core doctests, strict all-target/all-feature
Core Clippy, all-feature workspace Rustdoc JSON, formatting, and diff checks
passed. This closes the provider-injection reverse-export discrepancy only;
the Phase 6 runtime row and broad P18-V01 source/API/settings/input/error/flow
audit remain PARTIAL.

## P18-V01 `MQClientAPIFactory.getClient` empty-pool outcome — 2026-10-23

Pinned Java `getClient()` on a started zero-length client array reaches
`ThreadLocalRandom.nextInt(0)` and throws `IllegalArgumentException`. The
invoked fixture now records that exact exception; Rust returns its typed
`EmptyClientArray` error for the same state. The refreshed factory fixture
SHA-256 is `fcbb745a4eab18d4fa8e5163fa69deeffa935375b660fde8e915c7dab9e2da38`.
All seven Core factory tests, direct Java fixture verification, formatting and
diff checks pass. This closes the zero-pool selection evidence slice only;
broad P18-V01 remains PARTIAL.

## P18-V01 `MQClientAPIFactory.shutdown` null and empty-array behavior — 2026-10-23

Pinned `AsyncShutdownHelper` starts one worker per array slot and catches a
null target's `NullPointerException` inside that worker, so other clients still
shut down and the factory call returns normally. With a started zero-client
pool, the helper leaves its latch unset and `await` throws
`NullPointerException`. Rust now shuts down every non-null slot and maps the
empty-array case to `EmptyClientArray`. The invoked fixture records both Java
outcomes in `fixtures/runtime/api-factory.json` (SHA-256
`cf2dc5b293ea6620fe44732518e24ee97212b4584816083fe672223b4b7ff573`); its
cleanup explicitly closes the Java client removed from the public array after
the factory's behavior is captured. All seven Core factory tests and the
direct Java fixture verification pass. This closes the factory shutdown
failure-order slice only; broad P18-V01 remains PARTIAL.


## P18-V01 `MQClientAPIFactory.init` process-property side effects — 2026-10-23 (`34cbfb8b`)

Pinned Java 5.5.1 writes `com.rocketmq.sendMessageWithVIPChannel=false`
before selecting the NameServer path. Static mode then validates and writes
`rocketmq.namesrv.addr`; domain mode writes `rocketmq.namesrv.domain` before
`rocketmq.namesrv.domain.subgroup`, so a null subgroup throws while retaining
the domain update and prior subgroup value. Rust now records those writes in
a hidden shared in-process property overlay. Later `ClientConfig` construction
and Common NameServer lookup see the Java-equivalent values, including writes
that occur before constructor failure, while the operating-system environment
is unchanged. The Common module is a hidden cross-crate implementation bridge
and is not re-exported by the public facade; a compile-fail doctest guards that
boundary.

The invoked Java factory fixture records both constructor failures and their
property side effects. `fixtures/runtime/api-factory.json` SHA-256:
`cd41d77583fc72ec9949d09de805a3055a5303b9f3f514ad8986a3c495af2e6b`.
The single-process Core integration regression verifies that later clients and
Common lookup observe factory writes. It passed along with all 240 Core unit
tests, both default and all-feature workspace suites, strict workspace Clippy,
Maven `verify` (37 scenarios), differential self-test and manifest (37
scenarios), workspace metadata checks, deterministic race/leak smoke (17
cases x 3), bounded fuzz smoke (163 frame seeds, 11 message seeds, 2,784
mutations), release examples, fixed-input benchmark, Windows GNU
cross-compilation, formatting, and diff checks. This closes the factory
global-property side-effect slice only; broad P18-V01 source/API/settings/
input/error/control-flow/reverse-export reconciliation remains PARTIAL.
Native Windows execution, current-revision remote Ubuntu CI, and Phase 19
Broker/Docker evidence remain open. Run record:
`rlogs/2026/09/19-phase-18-factory-process-properties__20261023T111654Z.rlog`.

## P18-V01 factory constructor scheduler injection — 2026-10-23

The public Java `MQClientAPIFactory` constructors both accept a
`ScheduledExecutorService`. The Rust factory previously used Tokio workers
without exposing an equivalent scheduling dependency. Core now accepts a
caller-owned `FactoryRefreshScheduler` through `with_refresh_scheduler` and
registers one async NameServer refresh callback per client at Java's 10-second
initial delay and 120-second fixed rate. Java's optional
`ObjectCreator<RemotingClient>` constructor remains mapped separately through
`with_remoting_client_creator`; the Rust-only `TopAddressing` test seam remains
private.

The injected scheduler and registration-rejection regressions pass. The
Rustdoc reverse scan now has 744 Core and 460 Consumer distinct public names,
with all 62 Core and 15 Consumer additions since `1dc3a7e2` mapped to pinned
Java behavior or explicit Rust ownership/error adaptations. Core all-feature
tests, default and all-feature workspace tests/doctests, strict workspace
Clippy, formatting, and diff checks pass. This closes the factory
scheduler-injection gap and current reverse-name delta only; broad P18-V01
source/API/settings/input/error/control-flow review remains PARTIAL. Native
Windows runtime, current-revision
remote Ubuntu CI, and Phase 19 Broker/Docker validation remain open. Run record:
`rlogs/2026/10/23-phase-18-factory-scheduler-injection__20261023T114031Z.rlog`.

## P18-V01 Common topic and Producer topic-list Rust-name disposition — 2026-10-23

A compatibility-document token scan flagged five Common names
(`TOPIC_MESSAGE_TYPE_ATTRIBUTE`, `LITE_EXPIRATION_ATTRIBUTE`,
`topic_message_type_set`, `parse_from_message_property`, and `metrics_value`)
and Producer's `with_read`. Review against pinned Java commit
`efa1be59d6fc747ad220f036c86030f05f0419f9` found direct counterparts in
`TopicAttributes`, `TopicMessageType`, and `DefaultMQProducer.getTopics()` /
`setTopics(List<String>)`. The current Common topic crosswalk now names the
Rust identifiers explicitly. The Producer `with_read` and `with_mut` methods
are lock-scoped accessors over the same aliased list retained by the producer;
they adapt Java's live `List<String>` reference to the synchronized Rust
handle. `snapshot` is the owned point-in-time read form. No uncovered Java
capability or Rust-only Producer operation was found in these names. Mapping:
[`PHASE_6_API_CONFIG_MAP.md`](PHASE_6_API_CONFIG_MAP.md).

This closes only this reverse-name disposition. The token scan is not a
semantic API proof; broad P18-V01 Java source/API/settings/input/error/
control-flow and reverse-export reconciliation remains PARTIAL.

## P18-V01 `MQClientAPIFactory` protected-hook scope — 2026-10-23

Pinned Java source declares protected `init()` and `createAndStart(String)`.
The factory invokes `init` from its constructor and calls `createAndStart`
once per pool slot from `start`. A source/test scan at
`efa1be59d6fc747ad220f036c86030f05f0419f9` found no factory subclass in the
pinned client module and no other `createAndStart` callsite. The base
constructor/start flows map to Rust `build`/`start`; Java's remoting creator
and scheduler dependencies have their own Rust injection points.

The protected override seam is outside the Java-public client API parity
contract. Rust does not support Java-style subclass dispatch, and this
disposition makes no claim of third-party Java subclass source compatibility.
The direct public constructor, pool, address-change, scheduler, and lifecycle
surfaces remain mapped. The source/scope record is in
[`PHASE_6_RUNTIME_INVENTORY.md`](PHASE_6_RUNTIME_INVENTORY.md) and the
[`PHASE_6_API_CONFIG_MAP.md`](PHASE_6_API_CONFIG_MAP.md). Broad P18-V01
reconciliation remains PARTIAL.

## P18-V01 `MQClientAPIFactory` refresh rejection order — 2026-10-23

The pinned `createAndStart` order is API-client construction, callback
registration, initial NameServer discovery, refresh scheduling, API start,
then assignment into the factory array. Rust previously started and stored
every client before attempting refresh registration. It now performs that
sequence per slot, preserving Java's partial state when registration rejects.
The invoked Java fixture records `RejectedExecutionException`, one schedule
attempt, and a null slot; the Core regression compares the slot and verifies
the remoting creator ran before scheduling. Refresh callbacks retain the
client remoting object and update only that slot. The focused Core factory
suite (9), locked default/all-feature workspace suites, strict workspace
Clippy, offline Maven verify, differential self-test/37-scenario manifest,
metadata checks, race/leak smoke (17 × 3), fuzz smoke, examples, benchmark,
Windows GNU cross-check, formatting and diff checks pass. This closes the
factory schedule-rejection ordering gap only; broad P18-V01 remains PARTIAL.


## P18-V01 `MQClientAPIFactory` null scheduler input — 2026-10-23

Pinned Java accepts a null scheduler in either constructor. Static-address
startup never schedules a NameServer refresh and completes; domain startup
constructs the API client, registers its callback, completes initial discovery,
then throws `NullPointerException` at refresh scheduling before remoting start
and factory-array assignment. Rust now represents this input explicitly through
`with_optional_refresh_scheduler(None)`: static mode succeeds, while domain
mode returns `NullRefreshScheduler` after discovery and retains a null slot.
The invoked `RuntimeApiFactoryFixtures` records both Java outcomes, and the
Core regression compares the domain error/slot and static success. This closes
only the null-scheduler input branch; broad P18-V01 reconciliation remains
PARTIAL.

## P18-V01 `MQClientAPIFactory` nullable access config and name prefix — 2026-10-23

Pinned Java's constructor accepts a null `NameserverAccessConfig`, writes the
VIP property to `false`, then throws `NullPointerException` while reading the
config. Java also accepts a null `namePrefix`; client startup concatenates it
as the literal `null`, producing `nullN_0`. Rust's `new_nullable` adapter
preserves both observations with `NullNameServerAccessConfig`, the same
property write, and the generated instance name. The invoked
`RuntimeApiFactoryFixtures` records these inputs; the Core property test and
factory unit test compare them. This closes these nullable constructor input
branches only; broad P18-V01 reconciliation remains PARTIAL.

## P18-V01 `MQClientAPIFactory` nullable seven-argument constructor — 2026-10-23

The pinned seven-argument constructor passes its optional
`ObjectCreator<RemotingClient>` through to `MQClientAPIExt`; a null creator uses
the standard Netty remoting client. Rust now exposes
`new_nullable_with_remoting_client_creator`, preserving null config, prefix,
and scheduler behavior while accepting an optional injected creator. The
invoked Java fixture records the creator call count and `nullN_0` instance
name; the Core regression checks both a supplied creator and the default
creator fallback. Full current-revision local gates pass as recorded in the
Phase 18 execution log. This closes this nullable constructor overload only;
broad P18-V01 reconciliation remains PARTIAL.

## P18-V01 `MQClientAPIFactory` negative client count — 2026-10-23

Java stores a negative `clientNum` during construction and fails later in
`start()` when allocating `MQClientAPIExt[]`, with
`NegativeArraySizeException`; assignment does not occur, so `getClients()` is
still null. Rust already returns `NegativeClientCount` before publishing its
shared array. The invoked Java fixture and Core test now compare that failure
boundary and retained null state. This closes the negative-count start branch
only; broad P18-V01 reconciliation remains PARTIAL.

## P18-V01 runtime NameServer provider export crosswalk — 2026-10-23

The current Rustdoc export inventory contains a public
`ClientRuntimeManager::with_top_addressing` method. The prior API-map note
described the factory's same-named provider seam after it became private, but
did not give the runtime-manager method its own exact crosswalk entry. The
manager method maps the Java `TopAddressing` ServiceLoader extension: pinned
`DefaultTopAddressing` loads the first provider and uses it for discovery and
callback registration. Rust accepts that capability explicitly per manager,
and cloned managers retain it; the direct `MQClientApiFactory` test hook stays
private and remains a separate surface.

The API map now distinguishes and records both surfaces. This corrects one
reverse-export documentation omission; it does not identify a new runtime
behavior or close the broad P18-V01 source/API/settings/input/error/control-
flow audit.

## P18-V01 Remoting response/error factory reverse-name crosswalk — 2026-10-23

An exact-token scan of the compatibility Markdown found three public Remoting
names not written out in the map: `create_response_command`,
`create_response_command_with_header`, and `build_error_response`. Source
review confirms these are the direct Rust forms of pinned Java 5.5.1
`RemotingCommand.createResponseCommand(int, String)`,
`createResponseCommandWithHeader(int, CommandCustomHeader)`, and
`buildErrorResponse(int, String)`. They were covered by an existing grouped
response/error factory row and invoked fixture, but the Rust declarations were
not spelled out. The API map now lists these plus the typed-header factory
adapters explicitly.

This closes an exact-name crosswalk omission only; no runtime behavior or
tests changed, and broad P18-V01 remains PARTIAL.

## P18-V01 public tuple error payload privacy — 2026-10-23 (`65cf3cad`)

The workspace Rustdoc export contained two public tuple fields named `0`:
`RequestSendFailureCause(pub String)` and
`TransactionCallbackError(pub String)`. Java exposes throwable messages through
`getMessage()` and does not make that storage a writable public field. The
Rust wrappers now keep the backing strings private and expose `new` and
read-only `message()` methods; production call sites and transaction callback
examples/tests use those APIs. Compile-fail doctests guard against external
tuple-field access.

The affected Core and Producer test suites and doctests passed, as did strict
all-target/all-feature Clippy for both crates. Workspace all-feature Rustdoc
JSON generation passed and shows no public tuple-field `0`; current distinct
public-name counts are Common 439, Consumer 460, Core 746, Producer 361,
Protocol 1,524, and Remoting 337. This closes two extra public mutation/access
surfaces only; broad P18-V01 remains PARTIAL.

## P18-V01 transaction callback error identity semantics — 2026-10-23 (`a4228eb2`)

After the tuple payload was made private, `TransactionCallbackError` still
derived `Clone`, `PartialEq`, and `Eq` over its message. Java callback
exceptions inherit identity-based `equals` from `Object`; message equality
would report two distinct errors as equal. The Rust wrapper no longer exposes
those value traits, with a compile-fail doctest guarding the trait boundary.
Transaction callback failures still resolve to `Unknown` exactly as before.

The full Producer all-feature test suite and doctests passed, including the
trait-bound compile-fail case, and strict all-target/all-feature Producer
Clippy passed. This closes one error-wrapper identity mismatch only; broad
P18-V01 remains PARTIAL.

## P18-V01 Lite subscription control request-header inheritance — 2026-10-23 (`5acaab89`)

Pinned Java 5.5.1 `LiteSubscriptionCtlRequestHeader` inherits namespace,
namespaced, broker-name, and oneway state plus field-based equality, hash, and
string behavior from `RpcRequestHeader`. Rust previously modeled it as a unit
header and dropped all four inherited wire fields. It now stores those fields
privately, exposes the inherited accessor set, encodes/decodes `ns`, `nsd`,
`bname`, and `oway`, and uses the same four fields for equality and hashing.
The identity fixture now includes this class and records 46 request-header
API/identity entries; its SHA-256 is
`421e054be509f5f56fb5f1fe0cd5870745c83e956f56831082858e9780b4a10d`.

Protocol header inventory and validation tests, all Protocol all-feature tests
and doctests, strict Protocol Clippy, formatting/diff checks, and pinned Java
5.5.1 Maven verification passed. This closes the Lite request-header mapping
gap only. Broad P18-V01 remains PARTIAL; current-revision remote Ubuntu CI and
native Windows runtime evidence remain open, and Phase 19 Broker/Docker work
stays outside scope.

## P18-V01 remaining RPC request-header identity/API coverage — 2026-10-23 (`082809d1`)

The Rust `RpcRequestHeader` equality/hash implementation list contained 48
header types, while the Java identity/API fixture omitted
`CloneGroupOffsetRequestHeader`, `GetSubscriptionGroupConfigRequestHeader`,
and `QueryConsumeQueueRequestHeader`. The Rust identity test also explicitly
exercised only 43 fixture entries. Added the three pinned Java classes to the
invoked fixture and added base-field, payload-ignore, hash, and codec checks
for all six previously unexercised Rust headers. The fixture and Rust checks
now cover 49 entries with no uncovered class; fixture SHA-256:
`7e3f828aca4785153cc98b2f71bb38a93b33421df55bb3cf1cb5338a68e1b594`.

The Java fixture drift verifier, Protocol header inventory/validation tests,
request-header equality/hash test, strict Protocol Clippy, formatting/diff
checks, and full offline Maven verification passed. No production behavior
changed. This closes the request-header identity/API inventory gap only; broad
P18-V01 remains PARTIAL, with native Windows runtime and current-revision
remote Ubuntu CI open. Phase 19 Broker/Docker work remains outside scope.

## P18-V02 current-tree server-free gate — 2026-10-23 (`98457158`)

On a clean temporary Cargo target directory, default and all-feature locked
workspace tests/doctests, strict all-target/all-feature workspace Clippy,
formatting, and diff checks passed. Offline Maven `verify` passed 37 scenarios
and fixture drift/cross-decode checks; the Java oracle decoded 11 message
fixtures, 106 typed headers, and 19 typed bodies. The differential comparator
self-test and 37-scenario manifest, workspace metadata/dependency checks,
seven package listings with `LICENSE`/`NOTICE`, 17 deterministic race/leak
cases × 3, and bounded fuzz smoke (163 frame seeds, 11 message seeds, 2,784
mutations) passed. Release examples and the offline benchmark passed.

All-target/all-feature Windows GNU `cargo check` and strict Clippy passed for
the complete workspace; this is compile-only evidence. Current Rustdoc counts
are Common 439, Consumer 460, Core 747, Producer 361, Protocol 1,524, and
Remoting 337. One earlier default workspace run saw a single
`LocalFileOffsetStore` `AlreadyOpen` test failure on reopen. A full default
rerun, the all-feature run, three complete Consumer unit runs, and ten
concurrent offset-store test-group runs passed without a code change. This is
recorded as a transient test observation, not suppressed. Native Windows
execution, current-revision remote Ubuntu CI, broad P18-V01 reconciliation,
and Phase 19 Broker/Docker evidence remain open. No push or remote CI was run;
the four user-owned Phase 19 paths remain untouched and unstaged.

## P18-V01 response-header identity and API inventory — 2026-10-23

The Java 5.5.1 response-header fixture previously inventoried 16 Rust
response-header types while `header/basic.rs` exposes 22. Added the six
missing Java classes (`AddWritePermOfBrokerResponseHeader`,
`ChangeInvisibleTimeResponseHeader`, `GetAllSubscriptionGroupResponseHeader`,
`GetAllTopicConfigResponseHeader`, `GetKVConfigResponseHeader`, and
`WipeWritePermOfBrokerResponseHeader`) to the invoked fixture. The Java
NameServer package and `GetKV` spelling are recorded explicitly. Java
confirms all 22 inherit identity `equals`/`hashCode`; Protocol tests compare
their API inventories and check Rust accessor mappings. Fixture SHA-256:
`4e17f64398e2ae742f1fae55841e365eb7aed89b3f6aaad7eb191e8d282dd8ae`.
This closes the response-header inventory omission only; broad P18-V01
remains PARTIAL.

## P18-V04 current workspace Windows GNU cross-target check — 2026-10-23 (`77587adc`)

On the current macOS AArch64 tree, the complete workspace passed
`cargo check` and strict Clippy for `x86_64-pc-windows-gnu`, with all targets
and features enabled. This extends the earlier Core-only Windows GNU compile
check to every workspace crate, including integration tests. It is compile
evidence only; native Windows execution and current-revision remote Ubuntu CI
remain unverified. Phase 19 Broker/Docker work remains outside scope.

## P18-V01 Lite request-header Core call-site update — 2026-10-23 (`8288cc98`)

Changing `LiteSubscriptionCtlRequestHeader` from a unit struct to a value with
four inherited RPC fields exposed six Core call sites that still passed the
unit type as a value. Updated check-client, query-assignment, request-mode,
Lite subscription-control, and batch-ack requests to pass a default header
instance. The default still emits no ext fields, preserving their existing
empty-header wire behavior.

The full workspace all-feature test suite and doctests passed after the call
site updates, as did formatting and diff checks. This closes the downstream
construction fallout from the Lite header mapping only; broad P18-V01 remains
PARTIAL, and native Windows runtime and current-revision remote Ubuntu CI
remain open. Phase 19 Broker/Docker work remains outside scope.

## P18-V01 RPC request-header source/fixture parity guard — 2026-10-23 (`7885fbe5`)

The 49-entry Java fixture and Rust equality tests now remain tied to the Rust
`impl_rpc_request_header_semantics!` list: the test compares both name sets,
with `ConsumeMessageDirectlyResultRequestHeader` handled by its separate
custom implementation. Adding an RPC-equality header without its pinned Java
identity/API entry now fails the test instead of silently leaving an
unexercised type.

The request-header equality/hash/codec test and strict Protocol Clippy passed,
as did formatting and diff checks. This adds a coverage invariant only; broad
P18-V01 remains PARTIAL, with native Windows runtime and current-revision
remote Ubuntu CI open. Phase 19 Broker/Docker work remains outside scope.

## P18-V01 complete CommandHeader API inventory and Notify RPC inheritance — 2026-10-23 (`e7d779af`)

Added pinned Java 5.5.1 constructor, public-field, declared-method, and
callable-method inventories for the 25 request-header classes that lacked
class API evidence. A five-fixture parity check now requires exact coverage of
all 103 public Rust `CommandHeader` implementations, accounting for the
NameServer `DeleteKVConfigRequestHeader` spelling and excluding the unrelated
`ExportRocksDBConfigType` enum. The updated request identity fixture covers
50 headers. Fixture SHA-256 values are
`54df2a5effcb30ecdab9303c78d404a05916576bec60d1f33e540ec28ae6b688` for
`remaining-header-api.json` and
`38c163fea5ca151b00c450e709cc7006cb442e39799c34b3eecde930d4eeb10d` for
`request-header-identity.json`.

The API check exposed that Java `NotifyUnsubscribeLiteRequestHeader` inherits
`RpcRequestHeader`, while Rust had omitted the four inherited routing fields.
Rust now provides their accessors and nullable wire mappings, and matches
Java's base-only equality and hashing. Default and all-feature Protocol
tests/doctests, strict all-target/all-feature Protocol Clippy, formatting,
diff checks, and offline Maven `verify` passed. Maven verified 37 scenarios,
fixture drift, 11 message fixtures, 106 typed headers, and 19 typed bodies.
Run record:
`rlogs/2026/10/23-phase-18-complete-header-api-inventory__20261023T153747Z.rlog`.

This closes the public header API inventory gap and the identified
`NotifyUnsubscribeLiteRequestHeader` inherited-routing mismatch. Broad P18-V01
remains PARTIAL; native Windows runtime and current-revision remote Ubuntu CI
remain open. Phase 19 Broker/Docker work remains out of scope.

## P18-V02 current-tree server-free gate — 2026-10-23 (`e7d779af`)

On the current code revision, default and all-feature workspace tests and
doctests passed when run sequentially. Strict all-target/all-feature workspace
Clippy, the 37-scenario differential self-test/manifest, workspace metadata,
all seven package-content listings, 17 deterministic race/leak cases × 3,
bounded fuzz smoke (163 frame seeds, 11 message seeds, 2,784 mutations),
release examples, offline benchmark, Java fixture verification, and complete
workspace Windows GNU check/Clippy passed. Rustdoc public-name counts remain
Common 439, Consumer 460, Core 747, Producer 361, Protocol 1,524, and Remoting
337. The benchmark observation is recorded in
`docs/compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`.

An initial parallel launch of the default and all-feature workspace suites
reported three Consumer `OffsetStore(AlreadyOpen)` failures; each complete
suite passed when rerun sequentially, with no code change. Broad P18-V01
remains PARTIAL. Native Windows runtime and current-revision remote Ubuntu CI
remain open; Phase 19 Broker/Docker work remains out of scope. No push or
remote CI was run. Run record:
`rlogs/2026/10/23-phase-18-current-tree-server-free-gate__20261023T154720Z.rlog`.

## P18-V01 `ConsumerConnection` and `ProducerConnection` Set mismatch — 2026-10-23 (`05207a17`)

Direct pinned-Java API/behavior probes confirmed both DTOs expose
`HashSet<Connection>` and that `Connection` inherits identity-based
`Object.equals/hashCode`. The previous Rust `Vec` representation retained
duplicate same-reference and null entries. Rust now uses
`HashSet<Option<SharedValue<Connection>>>` with pointer identity for
`SharedValue<Connection>` equality/hash, reuses the identity-set JSON decoder,
and keeps the getter/setter container alias. Consumer and Producer fixtures
record duplicate insertion, distinct same-field objects, null membership,
and decode outcomes. Fixture SHA-256 values:
`9e011a55e633140ed06209762aec715cf88b14dfd8724f7239fe24effee7309c`
(`fixtures/admin/query-consume-time-span.json`) and
`d635048972229fa1ba2c03b3e2ad0e4600c6de32493ad927543be75a9aaa6de2`
(`fixtures/producer/core.json`).

Default and all-feature workspace tests passed; the final Protocol library
suite passed 160 tests; the ConsumerConnection admin RPC regression passed;
strict all-target/all-feature workspace Clippy and offline Maven `verify`
passed. Maven verified the 37-scenario manifest, fixture drift, and Java/Rust
cross-decodes. The change closes this two-DTO collection mismatch only; broad
P18-V01 reconciliation, current-revision remote CI, and native Windows runtime
evidence remain open. Phase 19 remains out of scope.

## P18-V01 `ProducerConnection` Set alias regression — 2026-10-23 (`b41efa8b`)

The prior Set correction preserved `ProducerConnection`'s setter/getter
container alias in Rust, but its direct proof exercised only
`ConsumerConnection`. Added a pinned Java probe showing that
`ProducerConnection.getConnectionSet()` returns the exact `HashSet` assigned
by `setConnectionSet`, and that adding through the returned set mutates the
assigned instance. An isolated DTO keeps this probe from changing the
separately captured RPC response. Rust asserts shared-handle pointer identity
and visible mutation. Updated `fixtures/producer/core.json` SHA-256:
`412fb8f5d9805edb2f994876d81ca980bfccc2963a1aff2bea19b98319a1eb5a`.

The Protocol library suite (160 tests), strict all-target/all-feature workspace
Clippy, offline Maven `verify`, formatting, and diff checks pass. This closes
the ProducerConnection assigned-Set alias evidence gap only; broad P18-V01
reconciliation remains PARTIAL.

Offset-map alias and partial-mutation correction (2026-10-23): pinned Java
5.5.1 `MQClientInstance.parseOffsetTableFromBroker` mutates each input
`MessageQueue` key and inserts that same object into a new `HashMap`; if a later
null key fails, earlier queue mutations remain visible. Commit `38194e8a`
changes Rust to borrow a shared ordered input table, mutate the same shared
queue keys, return a new HashMap retaining those queue handles, and preserve
the prefix mutation on failure. The seventeen existing Java cases now compare
both the mutated input and returned map and assert returned-key identity.
`fixtures/runtime/offsets.json` remains unchanged (SHA-256:
`f8298b31303536f9d97d172444bc63bb8a63708f7e1e93e33c148d01e3511af2`). On the
final borrowed signature, the focused metadata differential, full offline
workspace tests, strict all-target/all-feature Clippy, formatting, and diff
checks pass. Java oracle source and fixtures did not change, so Maven fixture
verification was not rerun. This closes the recorded offset-table alias gap;
broad P18-V01 source/API/settings/input/error/control-flow reconciliation
remains PARTIAL, and P6-V04 remains `PENDING_PHASE_19`.

TopicRouteData shallow-clone alias correction (2026-10-23): the pinned Java
5.5.1 copy constructor and `cloneTopicRouteData` allocate new outer lists/maps
and retain nested DTO/list references through `addAll`/`putAll`;
`deepCloneTopicRouteData` creates distinct nested values. Rust's shared route
DTOs match those identities: `java_copy_constructed` and
`clone_topic_route_data` create new outer containers while retaining nested
`QueueData`, `BrokerData`, filter-list, and mapping-value handles; the deep
clone creates distinct handles. The invoked Java fixture compares those
references, and
`topic_route_data_copy_and_clone_methods_match_invoked_java_values_and_failures`
checks the same alias flags with `SharedValue::ptr_eq`. Implementation commits
`45b8a5fc` and `8b69c09e` provide the shared-handle behavior and differential.
The focused Protocol test passes on the current tree. Current
`fixtures/runtime/route.json` SHA-256:
`0fcad65ecc00a9c55657113ed494d0d818621f5dcd4fa11e5da87e49c61d9470`.
This closes the stale documented clone-identity caveat only; broad P18-V01
reconciliation remains PARTIAL.

## P18-V01 `MessageQueueAssignment` nullable attachment keys — 2026-10-23 (`5dffd30b`)

Pinned Java `setAttachments(Map<String, String>)` accepts a null key and
`getAttachments()` returns the same map. Its fixture distinguishes that null
key from a literal `"null"` key and records mutation visibility and Fastjson
output, including duplicate `"null"` properties when both keys coexist.
`AssignmentAttachments` now represents nullable keys explicitly inside the
shared setter/getter handle. The DTO serializer writes a null key as `"null"`,
preserves a decoded property named `"null"` as a string key, retains null map
values in snapshots, and omits those values from the encoded DTO as Java does.
The refreshed `fixtures/runtime/lookup.json` SHA-256 is
`42af689aed0d150ee19fef5d8138627bb09db9085139b5221d579e57a0053283`.
Test follow-up `1b8c0d7d` also checks the raw encoded JSON text retains both
duplicate `"null"` properties rather than relying only on a parsed JSON value.

Full serial offline workspace tests, Protocol/Core package tests, strict
all-target/all-feature workspace Clippy, pinned offline Maven `verify`, and
format/diff checks pass. A parallel workspace attempt first hit the consumer
local-offset test's `AlreadyOpen` error; that test passed alone, and the full
serial workspace run passed. This closes this attachment-key API/input/output
gap only; broad P18-V01 source/API/settings/input/error/control-flow and
reverse-export reconciliation remains PARTIAL. Phase 6 remains PARTIAL and
P6-V04 remains `PENDING_PHASE_19`.

## P18-V01 NameServer scan admission and offset-store process locks — 2026-10-23 (`875e47d9`)

Pinned RocketMQ 5.5.1 `NettyRemotingClient.java` (SHA-256
`b9822a1abb0710070de051e7f94e0ba5facbf3284f3ae09977de14aee952476d`) submits
NameServer probes in configured order to a 4-core/10-max executor with a
32-entry queue. `ThreadUtils.java` (SHA-256
`3aef35aa3f9f42eeb8da63e89f0a47b1eecfbe0e13ca2e460cf6be520a48d479`) confirms
the default `AbortPolicy`: a rejected submission stops the remainder of that
scan pass. Rust now admits only the configured-order prefix that fits its 42
active-plus-queued bound; each later pass starts at the first address, and
completed probe tasks are applied before capacity is recalculated. The
`name_server_scan_batch_matches_java_abort_policy_submission_order` test pins
the saturation and restart ordering.

The local offset-store writer lease also switched on Unix from inherited
`flock` locks to POSIX `fcntl(F_SETLK)` record locks, which are not inherited
across `fork`; the lock file remains linked to avoid replacement-inode races,
and drop explicitly unlocks it. A subprocess regression verifies a second
process is rejected while the writer owns the store and can open it after the
owner drops. Three repeated parallel all-feature Consumer library suites and
the serial default/all-feature workspace suites pass after the change.

The offline stress suite now runs 20 cases three times. Strict workspace
Clippy, Windows GNU check and Clippy, offline Maven verification, differential
self-test/37-scenario manifest, fuzz smoke, release examples, offline
benchmark, metadata checks, formatting, and diff checks pass. This closes the
NameServer scan submission-order mismatch and strengthens offset-store lock
release behavior only; broad P18-V01 source/API/settings/input/error/flow and
reverse-export reconciliation remains PARTIAL. Native Windows runtime,
current-revision remote CI, and Phase 19 live Broker/Docker evidence remain
separate.

## P18-V01 inbound remoting shutdown version gate — 2026-10-23 (`a2eb40c2`)

Pinned RocketMQ 5.5.1 `NettyRemotingAbstract.java` (SHA-256
`f4c5daa4a9a560df4bf92c5b8d0e058accc54a84ef491e530559280ef0457d7e`) looks up
the request processor first. If a processor exists and shutdown is active, a
request version newer than `MQVersion.Version.V5_3_1.ordinal()` receives
`GO_AWAY` (`1500`, remark `please go away`) before `rejectRequest()` or
executor submission. The pinned `MQVersion.java` source
(SHA-256 `8b9a6a4b728f6b74d41f8ab19aac5cb5a39665cd0c05bf227ccba8d571ae5f82`)
and the invoked Java 5.5.1 fixture confirm the cutoff is ordinal 475.

Rust `process_one` now applies the same gate after processor lookup and before
rejection. Unsupported codes therefore retain priority; version 475 follows
the processor's normal rejection/submission path, while version 476 gets the
shutdown response. The fixture checks both boundary values, the running-client
path, unsupported-code ordering, response code, remark, opaque, and rejection
counts. Rust checks include the boundary helper and direct dispatch-order
regression.

Sequential default and all-feature workspace test suites, all 143 remoting
library tests, strict workspace Clippy, Windows GNU workspace check and
Clippy, formatting, and diff checks pass. The dedicated offline Java fixture
verification passes all four cases. A full `mvn -o ... verify` attempt was
stopped after an existing `ConsumerLifecycleFixtures` case entered DNS lookup
through Java's default NameServer resolver; it did not report a shutdown
fixture failure. Broad P18-V01 source/API/settings/input/error/control-flow
and reverse-export reconciliation remains PARTIAL. Native Windows runtime,
current-revision remote CI, and Phase 19 Broker/Docker scenarios remain
separate.

### Offline Maven gate follow-up — 2026-10-23 (`e6e1d62e`)

The lifecycle fixture that had triggered default NameServer DNS discovery now
sets `127.0.0.1:1` explicitly before its expected zero-interval startup failure.
This keeps its branch deterministic without contacting a NameServer. The
consumer lifecycle fixture's `--verify` run and full offline Maven
`mvn -o -q -f compat/java-reference/pom.xml verify` both pass on the updated
tree. The full run validates 37 scenarios, the new four-case shutdown fixture,
11 message fixtures, 106 typed headers, 19 typed bodies, and Java/Rust
cross-decodes. The earlier interrupted Maven attempt is superseded by this
successful run; the broad P18-V01 audit remains PARTIAL.

## P18-V01 scheduler immediate-shutdown returned tasks — 2026-10-23

Pinned Java 5.5.1 probes show that both `MQClientInstance` and
`PullMessageService` expose a `ScheduledExecutorService` whose `shutdownNow()`
returns queued futures without marking them cancelled or done. The two
submitted one-day futures are present in each returned list and no callback
runs. The client scheduler returns 32 entries in the pre-start fixture: two
submitted tasks and 30 tasks from the five constructor-created `StatsItemSet`s
(six recurring tasks each). The pull scheduler returns the two submitted
futures.

Rust `RuntimeScheduler::shutdown_now()` now returns task handles for queued
jobs represented in its scheduler. Submitted call handles compare by task
identity, remain pending after shutdown, and can be explicitly cancelled from
the returned list. Queued client periodic work and delayed pull request/task
entries receive pending handles as well. Running worker cancellation remains a
separate Rust behavior.

Follow-up revisions `644fff21`, `4251b2b3`, `b0189bd3`, `3b9d9473`, and
`4d32bc97` close the constructor-queue and statistics-callback gaps described
in the original run: the runtime-owned client scheduler now registers the
same 30 constructor-time jobs as Java, each retains its owning `StatsItemSet`,
samples only that series, and emits only its matching minute/hour/day
statistics event. The scheduler-injected `ConsumerStatsManager::with_scheduler`
constructor registers the same 30 jobs on the supplied scheduler. The
pre-start differential now returns the same 32 entries as Java, including the
two submitted futures. Queue count, callback ownership, and statistics-event
gaps are closed for this slice. Focused client and pull scheduler tests pass.
Default and all-feature workspace tests, strict all-target/all-feature
Clippy, Windows GNU workspace check/Clippy, direct fixture verification, and
full offline Maven verify pass on the recorded revisions. Broad P18-V01
source/API/input/reverse-export reconciliation remains PARTIAL. The detailed
run record is
`rlogs/2026/10/23-phase-18-scheduler-shutdown-return-list__20261023T200933Z.rlog`.

## P18-V01 internal producer retry-topic publication — 2026-10-23

The pinned `rocketmq-all-5.5.1` tag (`2ed890cd27b047cc124638aa3c80e391dde942b8`)
constructs `MQClientInstance.defaultMQProducer` with
`CLIENT_INNER_PRODUCER`. `DefaultMQProducer.java` sets a 3000 ms send timeout,
two synchronous retries, `retryAnotherBrokerWhenNotStoreOK=false`, and the
default retry-response set `{TOPIC_NOT_EXIST, SERVICE_NOT_AVAILABLE,
SYSTEM_ERROR, SYSTEM_BUSY, NO_PERMISSION, NO_BUYER_ID, NOT_IN_CURRENT_UNIT,
GO_AWAY}`. `DefaultMQProducerImpl.sendDefaultImpl` spends one total timeout
budget across route lookup and up to three synchronous send attempts, choosing
another Broker when one is available. It retries transport/client failures and
only that response-code set. Source SHA-256 values are recorded in the run log.

Rust retry-topic fallback now uses the same default timeout and attempt count,
skips the previous Broker when another routed Broker is available, and applies
the pinned response-code retry rule. The response-code, queue-selection, and
scripted third-attempt-success regressions pass. This maps the default
`sendLatencyEnable=false` selection path at this checkpoint. The optional
Java latency-fault selector was still open at this point; the later
`P18-V01 internal producer latency-fault branch closure` entry records its
follow-up coverage.

`cargo test -p rocketmq-classic-consumer` passes, including 266 library tests,
the Consumer integration suites, and 13 doctests. Strict Consumer all-target,
all-feature Clippy and formatting pass. Java source and fixture files were not
changed, so Maven fixture verification was not rerun. Broad P18-V01
source/API/settings/input/error/control-flow and reverse-export reconciliation
remains PARTIAL. Native Windows runtime and current-revision remote CI remain
separate; Phase 19 Broker/Docker work is outside this slice.

## P18-V01 `MQClientInstance.getDefaultMQProducer()` source/API disposition — 2026-10-23

The pinned Java 5.5.1 source declares public
`MQClientInstance.getDefaultMQProducer()` on the implementation class in
`client.impl.factory`; its getter returns the internal producer registered as
`CLIENT_INNER_PRODUCER`. Source SHA-256:
`55619893df475f32332d4a49787ac4a8add0d881de76922d528ba6eb7f3e267d`.
`git grep` across pinned `client/src/main/java` found the getter used by the
four consumer retry/send-back paths in `ConsumeMessageOrderlyService`,
`ConsumeMessagePopOrderlyService`, `DefaultMQPullConsumerImpl`, and
`DefaultMQPushConsumerImpl`. Factory start and shutdown invoke the same inner
producer's implementation lifecycle. No user-facing producer/consumer facade
uses this getter.

The Rust crosswalk now has a distinct row for this getter, separate from
`DefaultMQProducer.getDefaultMQProducerImpl()` and
`DefaultMQProducerImpl.getDefaultMQProducer()`. Rust keeps the inner group as
an internal Core registration and exposes the in-tree send/lifecycle behavior
through Producer, Consumer, and ClientRuntime owner operations; it does not
expose a mutable raw inner-producer wrapper or claim Java object-identity
parity. This records the getter as an implementation-accessor disposition,
while its default retry-topic flow is covered by the preceding retry-fallback
slice at this checkpoint. The source-private/configurable latency-fault branch
is superseded by the later `P18-V01 internal producer latency-fault branch
closure` entry below.

## P18-V01 `MessageQueueAssignment` HashSet collision disposition — 2026-10-23

Pinned Java 5.5.1 `MessageQueueAssignment.equals` compares only its queue, but
`hashCode` also includes mode and attachments. `MessageRequestMode` inherits
`Object.hashCode`, so the invoked fixture must read that JVM's enum identity
hash and construct an attachment hash that cancels the difference between two
assignments. In the same JVM, the fixture confirms same-queue equality, equal
hashes, and `HashSet` size one. Java's ordinary same-queue/different-mode case
with different hashes remains two entries. Source SHA-256 values are
`7c3c9563e8bd0a2d8c14d188efac66703be87cec9b7892ecc5fd7b071a4f6b61` for
`MessageQueueAssignment.java` and
`3b29c84d21cd16c02af8e1836cc4aa5dc0557e8b3e40022490506ef7dcba5c43` for
`MessageRequestMode.java`; `lookup.json` SHA-256 is
`42af689aed0d150ee19fef5d8138627bb09db9085139b5221d579e57a0053283`.

Rust maps Java's public `equals` through `java_equals` and keeps lawful
all-fields `Eq`/`Hash` for Rust collections. A Java Set serialized after
insertion already reflects that JVM's membership result; a manually supplied
raw collection engineered to collide depends on the receiving JVM's
process-local enum identity hash and cannot have deterministic cross-process
parity. The Rust-native collection trait behavior is therefore recorded as an
implementation-local boundary, not a portable wire-flow difference; see
`KNOWN_DIFFERENCES.md` and the assignment rows in
`PHASE_6_API_CONFIG_MAP.md`.

`cargo test -p rocketmq-classic-protocol assignment_set_hash_collision_oracle_records_the_java_boundary`
passes. No production code or Java fixture changed; Maven fixture verification
was not rerun. The stable same-mode collision gap recorded here is superseded by
the 2026-10-24 follow-up below; the cross-mode enum identity-hash boundary and
broad P18-V01 reconciliation remain open.

## P18-V01 assignment HashSet stable collision follow-up — 2026-10-24

The earlier process-local fixture did not cover a portable collision: Java
strings `Aa` and `BB` have the same `String.hashCode()`. Two attachment maps
using those different keys and the same value therefore have the same
`Map.hashCode()`. With the same queue and same mode, Java assignment hashes
match, `equals` returns true, and `QueryAssignmentResponseBody.decode` retains
one Set row. Rust's lawful all-fields `Eq`/`Hash` kept both rows before this
follow-up.

The response JSON decoder now indexes stable Java hash buckets by queue hash,
mode value, and attachment-map hash, then applies Java `equals` within a
matching bucket. This reproduces same-mode collisions and Java's
`NullPointerException` when duplicate equal-hash rows have null queues without
changing `MessageQueueAssignment`'s lawful Rust traits. The invoked lookup
fixture adds a same-mode collision, a different-hash control, and the duplicate
null-queue failure. Its assignment response input corpus now has 33 Set-field
cases and seven whole-root cases. The separate different-mode collision still
depends on JVM-local enum identity hashes and remains nonportable.

The focused assignment-body differential and all 168 Protocol unit tests pass.
Default and all-feature locked/offline workspace tests and doctests, strict
all-target/all-feature host Clippy, offline Maven `verify`, formatting and diff
checks pass. Windows GNU workspace check, strict Clippy, and all-feature test
compile/link pass; no Windows runtime execution is claimed. Broad P18-V01
method/overload/input/error/control-flow and reverse-export reconciliation
remains PARTIAL.

## P18-V01 internal producer latency-fault path follow-up — 2026-10-23

The pinned `MQClientInstance` copies its `ClientConfig` into the
`CLIENT_INNER_PRODUCER`; `MQFaultStrategy` reads `sendLatencyEnable` and
`startDetectorEnable` from that configuration. Rust retry-topic publication
now routes through the Producer send loop using the Consumer's existing
`ClientRuntime`. `ClientRuntime` retains one typed internal Producer extension,
so Consumer owners sharing a runtime also share the producer's latency-fault
table and detector settings. The existing Producer selector, fault updates,
response classification, default synchronous retry budget, message
compression, and route lookup are reused.

The Core runtime-extension identity test, Consumer owner-sharing/runtime-change
test, and Producer internal-send test verify runtime scoping, fault-based Broker
selection, and detector startup. `cargo test -p rocketmq-classic-core
-p rocketmq-classic-producer -p rocketmq-classic-consumer`, the same command with
`--all-features`, and strict all-target/all-feature Clippy pass on revision
`ea17b8fc`; formatting and `git diff --check` pass. Broad P18-V01 reconciliation
and P18-V04 current-revision platform evidence remain open.

## P18-V02 current-tree server-free gate — 2026-10-23 (`ea17b8fc`)

On the current code/test revision `ea17b8fc` (documentation head at run start:
`9a4ffdea`), sequential offline default and all-feature locked workspace
tests/doctests passed. Strict all-target/all-feature workspace Clippy,
formatting, and `git diff --check` passed. Offline Maven `verify` passed all 37
scenarios, Java fixture drift checks, and cross-decodes for 11 message
fixtures, 106 typed headers, and 19 typed bodies. Comparator self-test passed;
the 37-scenario manifest validator confirmed coverage of every pending source
gate ID collected from the Phase 0–17 plans. Workspace metadata, dependency,
and license checks and all seven product package listings with `LICENSE` and
`NOTICE` passed. Deterministic
race/leak smoke passed 20 cases × 3; bounded fuzz smoke passed 163 frame seeds,
11 message seeds, and 2,784 mutations (245 frame and 192 message decodes).
Release examples and the offline benchmark passed. Whole-workspace Windows GNU
all-target/all-feature check and strict Clippy passed as compile-only evidence.

The offline benchmark observations are in
`PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`. This refresh does not close broad
P18-V01 bidirectional source/API/settings/input/error/control-flow and
reverse-export reconciliation. Native Windows runtime and current-revision
remote Ubuntu CI remain unverified; no push or remote CI was run. Phase 19
Broker/Docker evidence remains outside this phase. Run record:
`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T221410Z.rlog`.

## P18-V01 cross-crate internal-producer helper exports — 2026-10-23 (`325c77fe`)

The internal Producer path adds two Rust `pub` helpers across crate
boundaries: `ClientRuntime::get_or_insert_extension<T>` and
`Producer::send_internal_with_runtime`. The API/config crosswalk maps the first
to the per-`MQClientInstance` `CLIENT_INNER_PRODUCER` identity and the second
to Java's internal `DefaultMQProducerImpl.sendDefaultImpl` retry path. Both
names are `#[doc(hidden)]`; current all-feature Rustdoc JSON omits them. The
first is not on the high-level facade, while the second is callable through
the facade's re-exported `Producer` type. Their in-tree ownership, runtime
replacement, fault selection, and detector tests pass in the full current-tree
default/all-feature workspace runs. This closes the reverse-export disposition
for these two cross-crate helpers only; broad P18-V01 source/API/settings/input/
error/control-flow and reverse-export reconciliation remains PARTIAL.

## P18-V01 Cargo feature-flag reverse scan — 2026-10-23 (`ea17b8fc`)

`cargo metadata --offline --format-version 1 --no-deps` reports an empty
`features` map for each of the eight workspace packages. There are no
workspace-defined Cargo feature flags or optional feature branches that add a
Rust-only client capability; the `--all-features` test configuration selects
no additional workspace feature set. This closes the Cargo feature-flag
inventory only; broad P18-V01 settings, public API, input, error, control-flow,
and reverse-export reconciliation remains PARTIAL.

## P18-V01 `doc(hidden)` Rust API reverse scan — 2026-10-23 (`df5c3c59`)

A source scan found 29 `#[doc(hidden)]` annotations across 15 library source
files. Because `#[doc(hidden)]` affects generated documentation only, each
`pub` item was reviewed as part of the Rust API. The crosswalk in
[`PHASE_6_API_CONFIG_MAP.md`](PHASE_6_API_CONFIG_MAP.md) maps these items to
Java property side effects, client/runtime owner adapters, request/reply
completion, internal producer and trace flows, remoting command storage, and
signed-timeout invocation. The scan found no additional client setting or
wire operation in this set; it records adapter disposition rather than
claiming the broad P18-V01 reverse-export audit is complete.

## P18-V01 current Rustdoc export and controller DTO reverse scan — 2026-10-23 (`df5c3c59`)

Regenerated all-feature Rustdoc JSON after the ConsumerStatsManager visibility
follow-up. Distinct public item-name counts, excluding each crate root, are
Common 439, Consumer 460, Core 788, Producer 361, Protocol 1,524, and Remoting
337. These are name inventories, not evidence of Java behavior parity.

The Core controller module declares 41 distinct public names. Its RPC method
surface and DTO types/field aliases are now recorded in the Phase 6 API map.
Review against the pinned `rocketmq-remoting-5.5.1-sources.jar` found remaining
DTO behavior gaps: `BrokerReplicasInfo.addReplicaInfo` and the three
`ReplicasInfo.isExistIn*` methods have no Rust counterparts, Rust's derived
`ReplicaIdentity` equality includes `alive` although Java equality/hash code
ignore it, and Rust `BrokerMemberGroup::minimum_broker_id` returns `None` for
an absent address map where Java dereferences the map and throws. Java DTO
hash-code and string-rendering methods also need explicit Rust dispositions.
The source artifact SHA-256 is
`ac2d2ad39cbe35e2125ce8cfdd2dd1c20ff7007f9960c1c4b597e292123a6bf6`.

`RUSTDOCFLAGS='-Z unstable-options --output-format json' cargo +nightly doc
--offline --workspace --all-features --locked --no-deps` passes. This pass
records the exported-name and source-surface audit only; no unit tests or
behavioral controller DTO verification were run. Broad P18-V01 reconciliation
remains PARTIAL, and native Windows runtime, current-revision remote Ubuntu
CI, and Phase 19 Broker/Docker validation remain open.

## P18-V01 Controller DTO behavior closure — 2026-10-23

Pinned Java 5.5.1 `BrokerReplicasInfo`, `ReplicasInfo`, `ReplicaIdentity`,
`BrokerMemberGroup`, `EpochEntry`, `EpochEntryCache`, and controller response
headers were reverse-audited against the Rust controller module. Rust now
preserves nullable map/list entries, maps `addReplicaInfo` and all three
`isExistIn*` membership methods, reproduces Java equality/hash inputs for
`ReplicaIdentity`, keeps Java's null-dereference and short-circuit paths
through `ControllerDtoError`, maps `minimumBrokerId`, and implements the
available Java hash/string methods and epoch copy constructor. Rust DTOs
whose Java classes inherit identity equality no longer receive structural
`PartialEq`/`Eq` derives.

`fixtures/producer/core.json` now contains `controllerDtoApi` and
`controllerDtoOperations`; its SHA-256 is
`b45df3622bca04a1ea4ba93e5700abe235745908ea2342af4b6ad51ca75fdeb6`.
The Java source artifact remains
`ac2d2ad39cbe35e2125ce8cfdd2dd1c20ff7007f9960c1c4b597e292123a6bf6`.
The fixture-backed `controller_api` integration test and direct controller
unit tests cover constructor/method inventory, nullable serialization
round-trips, membership short-circuits, null errors, equality/hash behavior,
and Java display strings. The current Core Rustdoc JSON scan records 799
distinct public names overall and 52 in `api/controller.rs`.

Validation on the current source revision passes: full
`cargo test --offline --locked -p rocketmq-classic-core` (including doctests),
strict all-target/all-feature Core Clippy with `-D warnings`, and pinned
offline Maven `verify` (including producer-core fixture verification). This
closes the controller DTO gaps identified in the previous scan only; broad
P18-V01 API/settings/flow and reverse-export reconciliation remains
`PARTIAL`, and Phase 18 acceptance is not yet established.

## P18-V01 internal producer latency-fault branch closure — 2026-10-23 (`ea17b8fc`)

The earlier retry-topic audit entries left `sendLatencyEnable=true` open. The
follow-up on `ea17b8fc` routes retry-topic publication through the Consumer's
existing `ClientRuntime` internal Producer extension, so Producer's
`MQFaultStrategy` reads the copied Java `ClientConfig`, owns the same
per-runtime fault table, applies the same response/transport retry rules, and
starts the latency detector when enabled. The tests
`inner_producer_is_shared_by_runtime_and_replaced_on_runtime_change`,
`internal_runtime_send_uses_the_latency_fault_selector`, and the internal
Producer send tests compare runtime identity, fault-based Broker choice,
detector startup, retry budget, and outcomes. Default and all-feature Core,
Producer, and Consumer suites and strict all-target/all-feature Clippy passed
on that code revision. The earlier “latency-fault branch remains open” wording
is historical and no longer identifies an active implementation gap. Broad
P18-V01 reconciliation remains `PARTIAL` for other inventory rows.

## P18-V01 inbound Remoting failure-flow closure — 2026-10-23

The pinned `NettyRemotingAbstract.processRequestCommand` source
(`b21a504b5310f8096cca443ee64ff733a6feaaea7fbd22ddd214821fa561275d`) was
invoked with `EmbeddedChannel`, a direct executor, and actual 5.5.1 hook,
pipeline, and processor interfaces. `RuntimeHookFixtures` records four
ordinary failure stages, aborts at before/pipeline/after, and one-way processor
failure. Its fixture-backed Rust loopback test verifies response presence,
response code and opaque, callback order, processor suppression, and one-way
silence. Both paths return SYSTEM_ERROR for ordinary failures and preserve
the caller-supplied abort codes. Java exception remarks are normalized to the
exception class because first-stack-frame text is diagnostic-only.
`fixtures/runtime/hooks.json` SHA-256:
`b1d92bab58838debc816ccce321a48876aaf72fb6addfe9aa6b6168620eedc5b`;
Java fixture source SHA-256:
`522e20cdf8bd69ea43a82683fc4a85fb843639d077f33fbfc9dd59dc6d48fd76`.

Focused remoting comparison and pinned offline fixture generation pass. This
closes the previously vague hook/pipeline/processor response-category and
control-flow concern only; the broad P18-V01 bidirectional audit remains
`PARTIAL`.

## Phase 18 `getUser` reply Fastjson decoding — 2026-10-24 (`41854b80`)

The remaining production `serde_json::from_slice` response path in Core's API
module was `MQClientAPIImpl.getUser`. Core now dispatches its nullable body to
`UserInfo::java_decode_nullable`, matching the Java Fastjson decoder while
preserving absent-body and JSON-null results. The invoked Java API matrix adds
two suffix cases (trailing text and a second JSON document after raw U+001A),
and the focused Core differential passes all ten cases. Producer core fixture
SHA-256 is
`ac4d435472279c6cdfefadf2811422359bc927223733c3521484c1c533d31f36`. This
closes this response-input slice only; broad P18-V01 remains `PARTIAL`.

## Phase 18 JSON remoting header Fastjson input — 2026-10-24 (`d897a9e2`)

Pinned 5.5.1 `RemotingCommand.headerDecode` delegates JSON headers to
`RemotingSerializable.decode`. Rust `json_header::decode` now parses through
the shared Fastjson-compatible `Value` decoder. Direct Java
`RemotingCommand.decode` fixture cases and Rust codec tests cover trailing
text and a second JSON document after raw U+001A, plus single-quoted fields
with a trailing comma. `response-future-api.json` SHA-256 is
`f0b5433e0c7c9f4566dee85048196860c47b948e4204cb08aa09b062bb628c0c`. The
Remoting unit suite passed 143 tests and the codec integration suite passed
nine tests. The package run remained unverified after macOS `dyld` stalled
while launching `fuzz_regressions`; strict Clippy also stalled in rustc
dependency-directory enumeration. Maven `verify` and focused formatting checks
pass. This closes these JSON-header inputs only; broad P18-V01 remains
`PARTIAL`.

## P18-V01 Remoting outbound write-flow parity — 2026-10-24 (`719e5aa3`)

Pinned Java 5.5.1 `NettyRemotingAbstract` was invoked with `EmbeddedChannel`
and a delayed outbound write promise. The `outboundWriteFlows` fixture verifies
that sync RPC response waiting does not wait for local write completion, and
that one-way invocation returns while its write remains pending with admission
held until that write completes. Rust now queues client frames without using
the RPC deadline to cancel socket progress, concurrently reads responses while
a frame is pending, and retains the one-way permit through local write
completion or failure. The connection writer tracks partial-write offsets so
selecting inbound reads cannot restart or duplicate bytes. Fixture SHA-256:
`6818a879b34ecab39936c1636c659ca3157e2d52fbd4f9e50f5bcadc49a9e09d`.

The Rust regressions `sync_response_is_read_while_request_write_is_pending`,
`oneway_write_remains_queued_after_the_call_deadline`, and
`p3_inbound_responses_survive_unbounded_outbound_backlog` pass. Locked default
and all-feature workspace tests/doctests, strict all-target/all-feature
Clippy, release examples, formatting, diff checks, comparator self-test, and
pinned offline Maven `verify` pass on this revision. Maven verified 36 runtime
hook cases, 37 differential scenarios, fixture drift, 11 message fixtures,
106 typed headers, and 19 typed bodies. This closes the client write-flow
deadline/completion slice only; queued-byte writability remains a documented
framework difference and broad P18-V01 reconciliation remains `PARTIAL`.


## P18-V02 current-tree server-free gate — 2026-10-24 (`719e5aa3`)

On current code/test revision `719e5aa3`, locked default and all-feature
workspace tests/doctests, strict all-target/all-feature workspace Clippy,
release examples, benchmark Clippy, formatting, diff checks, comparator
self-test and 37-scenario manifest validation passed. The deterministic
race/leak suite passed all 20 cases × 3 repetitions. Bounded fuzz smoke passed
163 frame seeds, 11 message seeds, and 2,784 mutations (564 frame decodes and
192 message decodes). The offline benchmark ran and is recorded in
`PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`. Workspace metadata/dependency/license
checks and all seven product package listings with `LICENSE`/`NOTICE` passed.
Windows GNU all-target/all-feature workspace check and strict Clippy passed as
compile-only evidence. Pinned offline Maven `verify` passed fixture drift, 37
scenarios, 11 message fixtures, 106 typed headers and 19 typed bodies. This
closes the current-tree local P18-V02 gates only. P18-V01 remains PARTIAL;
native Windows runtime and current-revision remote Ubuntu CI remain
unverified, while Phase 19 Broker/Docker scenarios remain separate.

## P18-V01 `MessageDecoder` buffer cursor parity — 2026-10-24 (`b2326a3a`)

The public Rust message ID helper now accepts Java-style output and address
cursors, preserving `flip`, current source position, output position, returned
backing-array bytes, and Java's put failure order. Its invoked fixture covers
IPv4/IPv6, positioned and short addresses, output-capacity failure, address
put overflow, and offset put overflow. `count_inner_messages` now accepts a
mutable cursor and reaches the limit on success; fixture cases cover a
nonzero start, a truncated size prefix, an incomplete positive size, and a
sub-four size followed by Java's next read/position failure. Negative sizes at
position zero compare the Java exception boundary and cursor position. At code
revision `b2326a3a`, negative sizes that Java could use to rewind from a
nonzero position were also rejected. The following 2026-10-24 follow-up closes
the finite rewind path and adds a cycle guard for malformed loops. Zero-size
and cyclic behavior remain recorded in `KNOWN_DIFFERENCES.md`.

Code commit: `b2326a3a`. `fixtures/messages/records.json` SHA-256:
`8447099d7fa609a9898cffb6045432d7a0252a94785aa118ed08ca7ecbf7ed2f`;
Java fixture source SHA-256:
`cdef8c8a47a751a4c1030777eacc97678972bf5a74207cc5e461adb05f9c0b05`.
Execution record: `../../rlogs/2026/10/24-phase-18-message-decoder-buffer-cursors__20261024T083406Z.rlog`.
The focused Common message fixture passes (27 tests), as do default and
all-feature locked workspace tests/doctests, strict all-target/all-feature
Clippy, release facade examples, Windows GNU all-target/all-feature
compile-only check and strict Clippy, comparator self-test and 37-scenario
manifest validation, format/diff checks, and pinned offline Maven `verify`
(37 scenarios, 11 message fixtures, 106 typed headers, 19 typed bodies). The
API matrix and older cursor dispositions have been updated. This closes these
two cursor review items only; broad P18-V01 remains PARTIAL. Native Windows runtime and
current-revision remote Ubuntu CI remain unverified; Phase 19 Broker/Docker
evidence remains separate.

## P18-V01 finite `countInnerMsgNum` rewind parity — 2026-10-24 (`924466d6`)

Rust now follows finite negative-size cursor rewinds when Java's target position
is in range. The invoked fixture starts with size 8, rewinds from position 8 to
5 with size -3, reads the overlapping 255 size, and compares the resulting
out-of-range position failure at cursor 9. Rust returns its typed incomplete
record error at the same cursor position. A negative-size cycle fixture checks
the Rust guard returns `InvalidInnerSize` at the repeated cursor rather than
looping forever; Java's cyclic call is intentionally not invoked. Zero-size
handling remains guarded for the same no-progress reason.

Code commit: `924466d6`. `fixtures/messages/records.json` SHA-256:
`e96844dffe42973d3ea77255b197f2d8f5ccb1c43fd3674eacd500687afa8ea7`;
Java fixture source SHA-256:
`31018415889b798bf558ae3e8e5e13ab513aaaed9af26b082609dcc1b00b7a82`.
The focused Common message fixture and Common default/all-feature tests and
doctests pass, as do all-feature workspace tests/doctests, strict workspace
Clippy, release facade examples, Windows GNU all-target/all-feature check and
strict Clippy as compile-only evidence, Maven `verify` (37 scenarios, 11
message fixtures, 106 typed headers, 19 typed bodies), comparator self-test
and manifest validation, format and diff checks. This closes the finite
negative-rewind slice only; zero/cycle safe guards and broad P18-V01 remain
documented/open. Native Windows runtime and current-revision remote Ubuntu CI
remain unverified; Phase 19 Broker/Docker evidence remains separate. Execution
record: `../../rlogs/2026/10/24-phase-18-count-inner-negative-rewind__20261024T085047Z.rlog`.

## P18-V01 `countInnerMsgNum` zero-size and cyclic input flow — 2026-10-24 (`0cfb20ad`)

The remaining malformed-count disposition now follows pinned Java 5.5.1 exactly:
zero-size input restores the same cursor position and loops, while a finite
in-range negative cycle repeats the same cursor path. Rust no longer returns a
guard error for either path. The Java fixture observes each call in a daemon
thread; the Rust test invokes each call in a child process, confirms it remains
active after the bounded interval, then terminates the child. Ordinary finite
rewinds and out-of-range negative positions retain the previously verified
cursor/error behavior.

Code commit: `0cfb20ad`. `fixtures/messages/records.json` SHA-256:
`bd128623b1e311bcc095f73802325403d79487e2559442fcdeb8aae62f41a133`;
Java fixture source SHA-256:
`f8e30456418e5462ac03e9b42311f527667a745931bbbc4d5a1c17b657ee5ca0`.
Focused message tests (28), default and all-feature locked workspace
tests/doctests, strict all-target/all-feature workspace Clippy, release facade
examples, Windows GNU all-target/all-feature check and strict Clippy
(compile-only), deterministic race/leak smoke (20 cases × 3), bounded fuzz
smoke (163 frame seeds, 11 message seeds, 2,784 mutations, 564 frame decodes,
192 message decodes), offline Maven `verify` (37 scenarios, 11 message
fixtures, 106 typed headers, 19 typed bodies), comparator/manifest, formatting,
and diff checks passed. The full default workspace rerun used
`CARGO_TARGET_DIR=/tmp/rocketmq-phase18-target` after the repository-target
`fuzz_regressions` launcher was slow to start; the independent-target run
passed. The offline benchmark is recorded in
`PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`.

This closes the recorded `countInnerMsgNum` cursor discrepancy. Broad P18-V01
source/API/settings/input/error/control-flow and reverse-export reconciliation
remains PARTIAL. Native Windows runtime and current-revision remote Ubuntu CI
remain unverified; Phase 19 Broker/Docker evidence remains separate. Run
record: `../../rlogs/2026/10/24-phase-18-malformed-inner-count-parity__20261024T093158Z.rlog`.

## P18-V01 caller-defined message body-array alias follow-up — 2026-10-24 (`950cf356`)

The Phase 10 `MessageExtSubtype` adapter now preserves shared Java body-array
identity through virtual `getBody()`/`setBody()` using explicit Rust shared
handles. The invoked direct-consume fixture compares caller-to-subtype,
getter-to-subtype and subtype-to-later-getter mutations. The Common message
suite passes 28 cases, the consumer direct-consume suite passes six, and the
runtime fixture SHA-256 is
`4ba7bf76f4ef8ebb1ecc116f253d5b8e64100cf83096e65ff86958425f403b5c`.
This closes that subtype alias slice only. P18-V01 remains PARTIAL for the
remaining source/API/settings/input/error/control-flow and reverse-export
reconciliation; P18-V04 remains open for native Windows runtime and current
revision remote Ubuntu CI evidence.

## P18-V01 public `LatencyFaultToleranceImpl.FaultItem` API — 2026-10-24 (`62f67cbb`)

The pinned Producer source exposes `LatencyFaultToleranceImpl.FaultItem` as a
public nested value class with one public inner constructor and 15 declared
public methods, including typed `compareTo(FaultItem)` and the compiler
bridge `compareTo(Object)`. Rust now exports `FaultItem` and
`FaultItemCompareError` from Producer and the facade. Its invoked Java fixture
checks the public signatures and behavior for initial values, nullable names,
positive/zero/negative and wrapping duration arithmetic, availability order,
signed latency/timestamp comparison, mutable value equality/hash fields,
UTF-16 String hashing, null/cast errors, reachability text, and the constructor's
implicit owner disposition. No helper method reads the enclosing strategy, so
the Rust value carries no unused owner reference.

Code commit: `62f67cbb`. `fixtures/producer/core.json` SHA-256:
`d6c23e4c1d213f02fd257abd0526b525d18f33167f1f26bda8510b75d189f398`.
Focused Producer/facade tests, default and all-feature locked workspace
tests/doctests, strict workspace Clippy, release examples, differential
self-test and 37-scenario manifest validation, all 20 offline stress cases × 3,
bounded fuzz smoke (2,960 mutations), pinned offline Maven `verify`, Windows
GNU workspace check/Clippy/test compile-link, formatting, and diff checks pass.
This closes the public nested value API row only. Broad P18-V01 method,
overload, input, error, control-flow, and reverse-export reconciliation remains
PARTIAL. Different-mode `MessageQueueAssignment` equal-hash outcomes remain
JVM-process-local; native Windows runtime and current-revision remote Ubuntu CI
remain unverified. Phase 19 paths are unchanged and unstaged. Run record:
`../../rlogs/2026/10/24-phase-18-fault-item-public-api__20261024T153809Z.rlog`.

## P18-V01 `ResponseFuture.waitResponse` external interruption — 2026-10-24 (`ba2fb950`)

The Java `ResponseFuture.waitResponse(long)` fixture externally interrupts a
waiting JVM thread and records `InterruptedException`, a null response, and the
cleared interrupt flag. Rust now exports `ResponseWaitInterrupt` as an explicit
per-wait signal and `ResponseWaitError::Interrupted`; calling
`wait_response_interruptible` wakes the blocked Condvar wait, consumes the
signal, and returns the typed error. The existing `wait_response` remains a
noninterruptible Rust convenience. Unit coverage compares the Java exception
class and tests wakeup, signal consumption, and an interrupt already pending
before the response. An external ambient thread-interrupt primitive is not
fabricated in Rust.

Code commit: `ba2fb950`. Default and all-feature locked workspace tests and
doctests, strict all-target/all-feature Clippy, Windows GNU all-target check,
strict Clippy and test compile/link, pinned offline Maven `verify` (37
scenarios, 11 message fixtures, 106 typed headers and 19 typed bodies),
20-case × 3 offline race/leak smoke, bounded fuzz smoke, release examples,
differential self-test, 37-scenario manifest validation, Rustdoc JSON, package
metadata/license/dependency checks, all seven product package listings,
formatting, and diff checks pass. Rustdoc reports 405 public item records and
341 distinct Remoting identifiers. This closes the external wait-interruption
behavior row only. The `ResponseFuture.toString()` JVM identity/rendering gap,
broad P18-V01 API/overload/input/error/control-flow and reverse-export audit,
native Windows runtime, and current-revision remote Ubuntu CI remain open.
Phase 19 paths are unchanged and unstaged. Run record:
`../../rlogs/2026/10/24-phase-18-response-wait-interruption__20261024T155648Z.rlog`.

## P18-V01 public `ProduceAccumulator(String)` constructor — 2026-10-24 (`c497f376`)

Rust now exports `rocketmq_classic_producer::ProduceAccumulator::new` and
re-exports the constructor-only type from the top-level facade. It accepts
`Option<String>` so Java null remains representable. The public Rust type has
no batching methods; Producer startup and sends continue to use the private
Rust-owned auto-batch engine. The invoked fixture inventories Java's one
public `ProduceAccumulator(String)` constructor and zero declared public
methods. Its SHA-256 is
`e7756ff8a7da4bac09de942e5eebf3e4886f933fb5f8483e4c7c7a8f61e228ff`, and the
pinned Java source SHA-256 is
`89f2fcb62e9654aed52a190aa040ce4fd80eb62779e607910fdccb328d08df1b`.
`pinned_java_produce_accumulator_declared_surface_is_constructor_only`
constructs both non-null and null Rust values; the facade API test verifies
both constructor cases through `rocketmq_classic`.

This closes the directly constructible helper type's public constructor row.
The separate `MQClientManager.getOrCreateProduceAccumulator` cache is keyed by
Java client ID and is used during producer startup; cross-Producer accumulator
sharing and its observable batching ownership remain part of the broad
P18-V01 flow audit. Broad P18-V01 remains `PARTIAL`.

## P18-V01 `MQClientManager` producer accumulator cache and ownership — 2026-10-24 (`49fffa75`)

Pinned `MQClientManager.getOrCreateProduceAccumulator(ClientConfig)` keys its
`ProduceAccumulator` map by `ClientConfig.buildMQClientId()`. The invoked
`produceAccumulatorClientIdCache` fixture records that repeated lookups for
the same ID return the same accumulator and a different ID returns a distinct
one. Fixture SHA-256 is
`e285d6e3408c9824feb957fb8cbf16f0209c9b345343a6db8a1ca3a954f40eba`; the
fixture generator SHA-256 is
`647201a9c1439e69982145cf2de616a2f562bb78edcc66fc799622eeefd643cd`.

Rust now keeps a manager-scoped cache keyed by the Java-compatible client ID.
Producers with that identity share live accumulator limits, held batches and
worker state. Each pending batch retains the first Producer as its send owner,
matching Java's `MessageAccumulation` owner reference. Producer shutdown stops
shared accumulator workers while retaining queued entries; a later Producer
start restarts the workers and re-schedules their original deadlines. The
`auto_batch_is_shared_by_client_id_and_keeps_first_producer_owner` scripted
test checks cache identity, shared settings, first-owner transport routing,
and restart behavior. The accumulator unit test
`stopped_accumulator_retains_pending_batches_for_restart` checks retained
entries and deadlines directly. The pinned Maven `verify` and the current
server-free P18-V02 gates pass; see
`rlogs/2026/10/24-phase-18-shared-accumulator-client-id__20261024T183639Z.rlog`.

This closes the manager cache and cross-Producer batching ownership slice
only. Broad P18-V01 method/overload/input/error/control-flow and
reverse-export reconciliation remains `PARTIAL`.

## P18-V01 final client-surface reconciliation — 2026-10-24

This section supersedes the earlier checkpoint notes in this append-only
audit. The oracle is RocketMQ Java 5.5.1 source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`; the separate Java checkout's
current worktree was not used as the source of truth.

The Phase 6 API/configuration crosswalk and the Phase 7–17 method matrices now
account for the scoped client methods, inherited and overloaded members,
settings and their Java read boundaries, input/error branches, and owner
control flows. The pinned `MQClientAPIImpl` scan covers all 132 public
declarations, 121 distinct names, and seven overloaded groups; every overload
member is crosswalked, and all 29 direct invocation fixtures pass. Reverse
Rust-surface review includes facade/crate exports, Rustdoc JSON,
`doc(hidden)` items, Cargo feature flags, environment inputs, and controller
DTOs. No unexplained client capability or setting remains in the audited
surface. Exact-name and export counts are inventories; behavior evidence is
linked from the rows that map those names.

`ConsumeMessagePopOrderlyService` is mapped through `PushConsumer`'s
`ConsumeMessageService` owner, orderly lock task and internal POP workers; the
method-by-method disposition is appended to
[`PHASE_10_OFFLINE_METHOD_MATRIX.md`](PHASE_10_OFFLINE_METHOD_MATRIX.md).
The three generic `ThreadPoolMonitor`, `ThreadPoolWrapper`, and
`ThreadPoolStatusMonitor` types live in Java `common`, but pinned-source search
finds no production references in `client/src/main` or `remoting/src/main`.
Their production callers are in Proxy/server components. They are outside the
Rust client API scope, not missing client operations.

Remaining documented boundaries are classified in
[`KNOWN_DIFFERENCES.md`](KNOWN_DIFFERENCES.md): Tokio/Netty and Rust/JVM
representation differences, provider-specific RustCrypto/rustls algorithms
that the user accepted, diagnostic stack/exception/object-identity rendering,
and JVM-process-local enum identity hashes for deliberately engineered raw
`MessageQueueAssignment` bucket collisions. They are not omitted client
capabilities or unexplained setting differences. No live Broker/Docker result
is inferred from offline fixtures; those execution obligations remain in
Phase 19.


## Current-tree post-closure source/API review — 2026-10-25

The final P18-V01 closure above remains the baseline for the complete Java
5.5.1 client-surface audit. The production crate delta from that closure to
code revision 1e909440b39720e814a1db9d79d92bbc6d733713 was reviewed: it is
limited to PushConsumer subscription-response handling and the Remoting
lifecycle diagnostic extension. The subscription retry timing is recorded as
a narrow documented difference; the added read-only lifecycle counts are
listed in the Remoting Rust-only public API inventory. No other public client
API or setting was added in this range.

The source-backed MAN-05 disposition is N/A for any additional
feature-specific owner validation because every registered feature scenario
was locally runnable and all 37 passed in historical runs. The checked-in
manifest records those outcomes as `PASS_RECORDED`; raw outputs are not
retained. Owner-network, organization security, HA fault-injection, and
workload/SLO evidence remain separate MAN-01 through MAN-04 requirements. See
[MAN-05 current-tree audit](evidence/owner/MAN-05/current-tree-audit-20261025.md).
