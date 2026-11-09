# Release preparation checklist

This checklist records Phase 18 preparation and current Phase 19 validation.
It does not authorize a compatibility claim, registry publication, release
tag, or deployment. The Java client baseline is RocketMQ 5.5.1; the Phase 19
Broker baseline is 5.3.4.

The dated entries below describe the state of each validation run at that
time. Raw Phase 19 run outputs were removed from the repository on 2026-10-27;
see [the test artifact policy](compatibility/TEST_ARTIFACTS.md) for current
storage rules.

## Compatibility and validation

### Current status — 2026-10-27

The R01–R09 parity-review fixes are locally verified on source revision
`7e0e9955`; the consumer crate passed 287 unit tests, all integration suites,
and 12 doctests, and the workspace tests, strict Clippy, offline Maven verify,
metadata, formatting, and diff checks passed on that revision. Targeted clean-
source live replays also passed: `P9-V03` (115 seconds), `P5-V02-consumer`
(76 seconds), and `P13-V02` (118 seconds) against the pinned 5.3.4 image. Raw
outputs are retained locally and uncommitted under
`plan/2026/10/02-project-parity-review-evidence/live/`. The complete local
aggregate and all 37/37 Phase 19 scenario passes remain historical results from
`413f735a` on 2026-10-26; the full matrix was not rerun on `7e0e9955`. GitHub
Actions was removed as a validation requirement on 2026-10-27. MAN-01 through
MAN-04 remain open for owner inputs, so final compatibility/release approval is
not recorded. See [current compatibility status](compatibility/CURRENT_STATUS.md)
and the [2026-10-27 execution log](../rlogs/2026/10/02-project-parity-review__20261027T062249Z.rlog).

The dated checklist entries below are preserved as run history. Their older
`PENDING` or `open` statements are snapshots from those runs and are superseded
where the current-status entry above records a later result.

- [x] P18-V02 full current-code server-free refresh (`c497f376`, 2026-10-24):
  default/all-feature workspace tests and doctests, strict host Clippy, Windows
  GNU check/strict Clippy/test-harness compile-link, pinned offline Maven
  verification, Rustdoc JSON, comparator/37-scenario manifest, 20-case × 3
  race/leak smoke, bounded fuzz, release examples, offline benchmark and strict
  benchmark Clippy, metadata, seven product package listings, formatting and
  diff checks pass. The Producer constructor-only `ProduceAccumulator` API is
  also available from the facade. Native Windows runtime and current-revision
  remote Ubuntu CI remain open; broad P18-V01 remains PARTIAL. Run record:
  `rlogs/2026/10/24-phase-18-produce-accumulator-constructor-api__20261024T175426Z.rlog`.

- [x] P18-V02 current-tree server-free refresh (`38a7bd83`, 2026-10-24):
  locked default and all-feature workspace tests/doctests, strict all-target/
  all-feature host Clippy, focused `ResponseFuture` display test, Windows GNU
  workspace check/strict Clippy/test-harness compile-link, pinned offline Maven
  verification, differential comparator and 37-scenario manifest, deterministic
  race/leak smoke, bounded fuzz, release examples, offline benchmark, metadata,
  seven product package listings, formatting and diff checks all pass. The
  `ResponseFuture` test verifies nested Java-compatible command rendering.
  Native Windows execution and current-revision remote Ubuntu CI remain open;
  broad P18-V01 remains PARTIAL. Phase 19-owned worktree files remain untouched.
  Full command evidence is in
  `rlogs/2026/10/24-phase-18-current-tree-full-server-free-gate__20261024T174002Z.rlog`.

- [x] P18-V02 full current-code refresh (`a460d966`, 2026-10-24): locked
  default and all-feature workspace tests/doctests and strict all-target/
  all-feature workspace Clippy pass. Windows GNU workspace check, strict
  Clippy, and `cargo test --no-run` pass; test harnesses compile/link but were
  not run. Release facade examples compile, and the offline benchmark compiles
  and runs with current-revision observations recorded in the requirement audit.
  Offline deterministic race/leak smoke passes 20 cases × 3; fixed-seed fuzz
  passes 163 frame, 11 message, and 11 property seeds with 2,960 mutations and
  564/192/176 decoder calls; comparator self-test and 37-scenario manifest
  validation pass. Pinned offline Maven `verify` passes 37 scenarios, 11
  message fixtures, 106 typed headers, and 19 typed bodies. Locked metadata
  reports eight workspace members and 216 packages; package listings and the
  metadata/license/dependency-direction check pass. Formatting and diff checks
  pass. Broad P18-V01 source/settings/input/error/reverse-export audit
  remains PARTIAL; native Windows runtime and current-revision remote Ubuntu CI
  remain unverified. Phase 19-owned worktree paths remain untouched.

- [x] P18-V01 Java `Comparable` declaration inventory (`2026-10-24`): the
  pinned client/common/remoting source scan found `MessageQueue`,
  `MessageQueueForC`, `TraceContext`, `SubscriptionData`, `BrokerData`,
  `QueueData`, and nested `LatencyFaultToleranceImpl.FaultItem`. All seven have
  Rust Java-compatible comparison methods and invoked fixture coverage. The
  direct `FaultItem` API mapping is recorded in the 2026-10-24 follow-up below.
  This closes the declared-comparator scan only; broad P18-V01 remains PARTIAL.

- [x] P18-V01 assignment DTO mode accessor assertion (`88061e7a`, 2026-10-24):
  corrected `assignment_methods_match_invoked_java_public_api`, whose previous
  `mode()` self-comparison did not verify the Java setter/getter value. The
  focused Protocol test, strict Protocol Clippy, formatting, and diff checks
  pass. The decoder now matches stable same-mode assignment hash collisions;
  different-mode collisions still depend on Java enum identity hashes. Broad
  P18-V01 remains PARTIAL.

- [x] P18-V01 `LatencyFaultToleranceImpl.FaultItem` scope disposition
  (2026-10-24): this historical helper-only disposition is superseded by the
  public API implementation and fixture recorded in the current follow-up.

- [x] P18-V01 `MessageQueueForC` public value API (`a460d966`, 2026-10-24):
  Common exports the Java 5.5.1 four-argument constructor, getter/setter
  equivalents, value equality, Java hash code and text form, plus comparison
  results matching UTF-16 strings and Java integer/long subtraction behavior.
  The invoked route fixture covers API values, setters, equality (including
  offset-sensitive equality and offset-insensitive hash), 12 comparison pairs,
  and null failures. Common all-feature tests/doctests, strict Common Clippy,
  pinned offline Maven `verify` (37 scenarios, 11 message fixtures, 106 typed
  headers and 19 typed bodies), formatting and diff checks pass. `route.json`
  SHA-256: `9c068b67324f45510dab1aa4df3c2943b54944a9a56dcf8accc24f89f6612246`.
  Java object-stream serialization and the C-client `ResetOffsetBodyForC` wire
  path are outside this value-class slice; broad P18-V01 remains PARTIAL.

- [x] P18-V01 `TraceContext.compareTo(null)` (`3427d269`, 2026-10-24): invoked
  Java records `NullPointerException`; Rust maps it to
  `TraceContextCompareError::NullOther` through `java_compare_to_nullable`.
  Core all-feature tests/doctests, strict Core Clippy, pinned offline Maven
  `verify` (37 scenarios, 11 message fixtures, 106 typed headers and 19 typed
  bodies), formatting and diff checks pass. `trace-records.json` SHA-256:
  `e56224bb6c926d53ab4c118fcb38d5a6067e10e252bd42817f2be68c1f7a3f5e`. This
  closes this null argument branch only; broad P18-V01 remains PARTIAL.

- [x] P18-V02 full current-code refresh (`80acf42f`, 2026-10-24): locked
  all-feature workspace tests/doctests and strict workspace Clippy pass.
  Windows GNU workspace check, strict Clippy, and test-harness compile/link
  pass; Windows executables were not run. Deterministic race/leak smoke passes
  20 cases × 3; fixed-seed fuzz passes 163 frame, 11 message, and 11 property
  seeds with 2,960 mutations and 564/192/176 decoder calls; comparator
  self-test and 37-scenario manifest validation pass. Pinned offline Maven
  `verify` passes 37 scenarios, 11 message fixtures, 106 typed headers, and
  19 typed bodies. Formatting and diff checks pass. Native Windows runtime and
  current-revision remote Ubuntu CI remain unverified; broad P18-V01 remains
  PARTIAL.

- [x] P18-V01 route DTO `compareTo(null)` (`80acf42f`, 2026-10-24): invoked
  Java records `NullPointerException` for `MessageQueue`, `QueueData`, and
  `BrokerData`; Rust maps these to each comparator's `NullOther` error through
  `java_compare_to_nullable`. Common and Protocol all-feature package
  tests/doctests, strict package Clippy, pinned offline Maven `verify` (37
  scenarios, 11 message fixtures, 106 typed headers and 19 typed bodies),
  formatting and diff checks pass. `route.json` SHA-256:
  `fa4ee2e7b22496a561998501ca464455ebe0fc489a005868c96521e20efb4732`. This
  closes these null argument branches only; broad P18-V01 remains PARTIAL.

- [x] P18-V01 `SubscriptionData.compareTo(null)` (`fdfdbaea`, 2026-10-24):
  invoked Java records `NullPointerException`; Rust maps it to
  `SubscriptionDataCompareError::NullOther`. Protocol all-feature tests and
  doctests, strict Protocol Clippy, pinned Maven `verify` (37 scenarios, 106
  typed headers and 19 typed bodies), formatting and diff checks pass. Fixture
  SHA-256: `7329d0dc1c886c1110a9e81661f72984087f8ecf86fd8a46120a8b86cfe415a8`.
  This closes the null argument branch only; broad P18-V01 remains PARTIAL.

- [x] P18 Windows GNU all-feature compile/link slice (2026-10-24): on macOS
  ARM64 with Rust 1.85.0, Windows GNU workspace `cargo check`, strict
  all-target/all-feature Clippy, and `cargo test --no-run` passed. All workspace
  test harnesses link, but no Windows binary was run. Native Windows runtime and
  current-revision remote Ubuntu CI remain open; this does not close P18-V04.

- [x] P18-V02 current-revision supplemental smoke (`33c57f09`, 2026-10-24):
  offline deterministic race/leak smoke passed 20 cases × 3; fixed-seed fuzz
  passed 163 frame, 11 message, and 11 property seeds with 2,960 mutations and
  564/192/176 decoder calls; comparator self-test and 37-scenario manifest
  validation passed. Together with the all-feature workspace suite above, this
  refreshes the local P18-V02 gate. Native Windows runtime, current remote
  Ubuntu CI, and broad P18-V01 remain open.
- [x] P18-V02 Remoting property-decoder fuzz follow-up (`67ee9ec4`,
  2026-10-24): the deterministic smoke now seeds the property parser directly
  from 11 unique invoked-Java encoded property strings and decoded message
  properties, and applies 16 deterministic mutations per seed. The result is
  163 frame seeds, 11 message seeds, 11 property seeds, 2,960 total mutations,
  and 564/192/176 frame/message/property decoder calls. Full all-feature
  workspace tests/doctests, strict all-target/all-feature workspace
  Clippy, formatting, and diff checks pass on revision `1f9f1ce7`, using an
  isolated Cargo target after the repository target's codec process stalled in
  the macOS dynamic loader before test execution. This closes direct
  property-seed coverage only; broad P18-V01 remains PARTIAL and native
  Windows/current remote Ubuntu evidence remains open.
- [x] P18-V01 assigned PULL queue expiry/retention (`33c57f09`, 2026-10-24):
  the invoked Java fixture and Rust owner regression compare expired-assigned
  replacement, active and dropped assignment retention, and empty-assignment
  removal. Focused Rust coverage, offline Maven `verify` (37 scenarios, 106
  typed headers and 19 typed bodies), formatting, and diff checks pass. The
  combined `consumer-operations.json` SHA-256 is
  `892956f5ef65b5418bf637a5065f4697f4342a7926d9e04d69cdd657a7d3a911`. This
  closes this assignment slice only; broad P18-V01 and current native/remote
  platform evidence remain open.
- [x] P18-V02 post-PULL full-feature refresh (`33c57f09`, 2026-10-24): locked
  all-feature workspace tests and doctests pass, including the new assignment
  regression; strict all-target/all-feature workspace Clippy passes. The run
  used `CARGO_TARGET_DIR=/tmp/rocketmq-phase18-allfeat-target` after the prior
  repository target stalled in the macOS loader. Formatting and diff checks
  pass. Broad P18-V01 and current native Windows/current remote Ubuntu
  evidence remain open.
- [x] Keep the Java client and Broker baselines separate in docs and fixture
  provenance.
- P18-V01 Fastjson leading-dot and incomplete-exponent follow-up (`9333cbc2`,
  2026-10-24): the 1,111-case Java heartbeat DTO corpus confirms that unsigned
  `.5`/`.5F` is accepted for integer `subVersion` but rejected for String
  `topic`, while `+.5` remains accepted. Incomplete exponent forms `1e+`,
  `1e+F`, and `1.E` match the Java field conversions. Rust preserves token
  provenance for typed DTOs and keeps generic `serde_json::Value` numeric.
  Default/all-feature workspace tests and doctests, strict Clippy, offline
  Maven `verify` (37 scenarios and Java cross-decodes), differential self-test
  and manifest, 20 race/leak cases × 3, fuzz smoke, Windows GNU workspace check
  and Clippy, release examples, offline benchmark, metadata, and all seven
  package listings pass. P18-V01 remains PARTIAL; native Windows runtime and
  current-revision remote Ubuntu CI were not run. Phase 19 paths remain
  untouched.
- [x] Record the server-free tests, fuzz/race smoke runs, benchmark observations,
  and CI commands in the Phase 18 execution log and compatibility docs.
- [x] Reconcile the Phase 15–17 local matrices and execution records against
  current artifacts. Phase 15's P15-V01/V02/U01 local gates are verified;
  P15-V03 remains Phase 19. Phase 16 records all local acceptance gates as
  passed; Broker and owner-environment cases remain Phase 19. Phase 17 records
  P17-V01/V03/U01 local evidence; `jstackEnable` enumerates threads on macOS,
  Linux and Windows, with Linux runtime coverage on both architectures and
  Windows cross-target compile evidence. The CI workflow now includes a native
  Windows stack runtime job; its current-revision result and Broker-backed
  query checks remain pending. Plans remain unchanged.
- P18-V01 factory nullable-constructor follow-up (`0d6d9b79`, 2026-10-23):
  the invoked Java fixture and Rust regressions now cover null
  `NameserverAccessConfig` property-write/error order and null `namePrefix`
  conversion to `nullN_0`. The default and all-feature workspace suites,
  strict Clippy, offline Maven/differential fixtures, metadata/package checks,
  stress/fuzz smoke, release examples, benchmark, Windows GNU cross-check,
  Rustdoc JSON, formatting, and diff checks pass on this revision. This closes
  those factory input branches only; broad P18-V01 remains open.
- P18-V01 nullable seven-argument factory follow-up (`8f30b6fb`, 2026-10-23):
  the nullable constructor adapter now carries a custom remoting creator or
  uses the Java null-creator default, with fixture and Rust coverage for the
  generated `nullN_0` instance name. Default/all-feature workspace tests,
  strict Clippy, offline Maven verification, and the remaining local P18-V02
  gates pass on this revision. The Rustdoc name scan reports Core 747; broad
  P18-V01 remains open.
- P18-V01 negative `MQClientAPIFactory` client count (`42fa4f87`, 2026-10-23):
  the invoked fixture and Core test confirm Java's deferred
  `NegativeArraySizeException` from `start()` and a still-null client array.
  Both workspace suites, strict Clippy, Maven verification, differential,
  stress/fuzz, packaging, examples, benchmark, Windows cross-check, Rustdoc,
  formatting, and diff checks pass on this revision. This closes the negative
  count branch only; broad P18-V01 remains open.
- P18-V01 `TopicConfig.decode(null)` (`19b674ab`, 2026-10-23): the invoked
  Java fixture records `NullPointerException` before decode mutation, and the
  Protocol regression compares Rust's `TopicConfigError::NullInput`. The
  focused test, all-feature Protocol suite (including 166 doctests), strict
  Protocol Clippy, formatting/diff checks, and offline Maven verification
  pass. This closes the null-input branch only; broad P18-V01 remains open.
- P18-V01 inherited `ServiceThread.makeStop()` wake behavior (`4c74d38e`,
  2026-10-23): pinned Java wakes a started worker after setting its stop flag.
  Rust now wakes the Rebalance worker; PullMessageService's blocking queue wait
  retains its source behavior. A Java `waitForRunning` probe and Rust worker
  regression cover prompt exit. Core all-feature tests (247 unit tests, all
  integration targets, 21 doctests), strict Core Clippy, formatting/diff
  checks, and offline Maven verification pass. This closes the wake branch
  only; broad P18-V01 remains open.
- P18-V01 `ServiceThread.shutdown()` overload mapping (`6e994989`, 2026-10-23):
  Rust now exposes no-argument `shutdown()` and maps Java's boolean overload to
  `shutdown_with_interrupt(bool)`. Core tests cover the no-interrupt default,
  PullMessageService pre-start executor closure, and explicit interruption.
  Core all-feature tests (247 unit tests, all integration targets, 21
  doctests), strict Core Clippy, all-feature workspace Rustdoc JSON,
  formatting/diff checks, and offline Maven verification pass. This closes the
  overload mapping only; broad P18-V01 remains open.
- Current-revision server-free P18-V02 gate rerun (`0c372d0a`, 2026-10-23):
  default and all-feature workspace tests/doctests, strict all-target/all-feature
  workspace Clippy, offline Maven `verify` (37 scenarios and Java cross-decodes),
  differential self-test and 37-scenario manifest, 17 deterministic race/leak
  cases x 3, fuzz smoke (163 frame seeds, 11 message seeds, 2,784 mutations),
  release facade examples, offline benchmark, locked metadata, package/license/
  dependency checks, seven product package listings, Core Windows GNU
  cross-check, workspace Rustdoc JSON, format and diff checks all pass. The
  Rustdoc export counts are Common 439, Consumer 460, Core 747, Producer 361,
  Protocol 1,524 and Remoting 337; these are name inventories, not behavioral
  proof. The benchmark records machine-specific observations with no threshold.
  Native Windows execution, current-revision remote Ubuntu CI, Phase 19 Broker/
  Docker evidence, and broad P18-V01 source/API/settings/input/error/control-
  flow and reverse-export reconciliation remain open.
- P18-V01 client stats scheduler queue (`644fff21`, 2026-10-23): Rust now
  creates the 30 Java `StatsItemSet` maintenance jobs in the client scheduler
  queue before runtime start. Sampling jobs advance metric windows; the minute,
  hour, and day log jobs are queued but do not yet emit Java's statistics log
  lines. The Rust pre-start `shutdownNow` regression reads the Java fixture and
  returns the same 32 pending entries with two submitted futures. Default and
  all-feature workspace tests/doctests, strict all-target Clippy, Windows GNU
  workspace check/Clippy, offline Maven `verify` (37 scenarios, including both
  scheduler shutdown fixtures), formatting, and diff checks pass. This closes
  the constructor-queue count delta only; statistics log output and broad
  P18-V01 reconciliation remain open. Native Windows runtime and
  current-revision remote CI were not run.
- P18-V01 client stats log follow-up (`4251b2b3`, 2026-10-23): the scheduled
  minute, hour, and day print callbacks now emit `tracing::info!` events under
  Java's stats logger target. The callbacks emit all five metric groups across
  the five per-set jobs; the daily window keeps 25 hourly samples. Workspace
  default and all-feature tests/doctests, strict all-target/all-feature Clippy,
  Windows GNU workspace check/Clippy, offline Maven `verify`, formatting, and
  diff checks pass. Backend timestamp/thread formatting may differ; this closes
  the missing stats-event gap only, and broad P18-V01 remains open.
- P18-V01 stats callback ownership (`b0189bd3`, 2026-10-23): all 30 scheduled
  jobs now retain the owning Java `StatsItemSet`; sampling advances only that
  metric series and each print callback emits only its matching metric group,
  preventing fivefold log fan-out. Focused sampling, print-event, and schedule
  identity regressions pass, as do workspace default/all-feature tests and
  doctests, strict all-target/all-feature Clippy, Windows GNU workspace check
  and Clippy, offline Maven `verify`, formatting, and diff checks. This closes
  callback ownership only; broad P18-V01 remains PARTIAL. Native Windows runtime
  and current-revision remote CI remain open.
- P18-V01 statistics scheduler reconciliation (`3b9d9473`, `4d32bc97`,
  2026-10-23): `ConsumerStatsManager::with_scheduler` maps Java's public
  scheduler-injected constructor and registers all 30 jobs; runtime-owned and
  standalone managers share the same schedule specification. The 32-entry
  pre-start client `shutdownNow` result, per-set sampling ownership, and
  per-set minute/hour/day log events are all represented in Rust. The
  scheduler queue and statistics callback gaps are closed for this slice;
  broad P18-V01 remains PARTIAL.
- P18-V01 hidden-public Rust API reverse scan (`df5c3c59`, 2026-10-23): reviewed
  all 29 `#[doc(hidden)]` annotations across the library crates. The Phase 6
  crosswalk maps the property/runtime bridges, request/reply controls, inner
  producer path, trace helpers, typed remoting state, and signed-timeout
  adapter to Java owners. No additional client setting or wire operation was
  found in this set; the broader reverse-export audit remains open.
- P18-V02 server-free refresh on code revision `4251b2b3` (documentation
  revision `23265067`, 2026-10-23): default/all-feature workspace tests and
  doctests, strict workspace Clippy, offline Maven `verify` (37 scenarios and
  Java/Rust cross-decodes), differential self-test and 37-scenario manifest,
  20 deterministic race/leak cases × 3, fuzz smoke (163 frame seeds, 11 message
  seeds, 2,784 mutations), release examples, workspace benchmark compilation
  and offline sample, metadata/license/dependency checks, workspace package
  listings, Windows GNU all-target/all-feature check and Clippy, Rustdoc JSON,
  formatting, and diff checks pass. Rustdoc name counts are Common 440,
  Consumer 461, Core 788, Producer 362, Protocol 1,525, and Remoting 338; they
  are inventories, not behavior evidence. Benchmark observations are recorded
  without thresholds. Native Windows runtime and current-revision remote CI
  remain open; broad P18-V01 remains PARTIAL.
- P18-V01 `ConsumerStatsManager` scheduler constructor parity (`3b9d9473`, 2026-10-23):
  Java's public constructor accepts a `ScheduledExecutorService` and registers
  five sets of six jobs. Rust now exposes
  `ConsumerStatsManager::with_scheduler(&RuntimeScheduler)`, which registers
  the same 30 jobs; the `ClientRuntime`-owned manager continues to use its
  built-in queue. A paused-time regression verifies the added queue entries.
  Default/all-feature workspace tests and doctests, strict all-target/all-feature
  Clippy, Windows GNU workspace check/Clippy, offline Maven `verify` (37
  scenarios, 11 message fixtures, 106 headers, 19 bodies), formatting, and diff
  checks pass. Rust `new`/`Default` remained an unscheduled convenience at this
  revision. This closes constructor scheduling only; broad P18-V01 remains
  PARTIAL. Native Windows runtime and current-revision remote CI remain open.
- P18-V01 constructor surface follow-up (`df5c3c59`, 2026-10-23): removed
  Rust-only public `ConsumerStatsManager::new` and `Default`, retained
  `with_scheduler` as the Java-mapped public construction path, and kept the
  unscheduled runtime allocator private to the runtime module. PushConsumer no
  longer allocates a separate fallback manager before runtime binding.
  Default/all-feature tests/doctests for Core, Consumer, and the facade pass;
  strict all-target/all-feature Clippy, formatting, and diff checks pass. Broad
  P18-V01 remains PARTIAL; native Windows runtime and current-revision remote CI
  remain open.
- [ ] Close P18-V01: complete bidirectional Java source/API/input reconciliation
  and reverse-audit all Rust public APIs and settings. Remoting after-response
  hooks now mutate request/response commands on outbound and inbound paths;
  request-pipeline stages mutate requests before later stages/processors, with
  the mutated opaque verified through the response. Java fixture verification
  and the focused Rust regression pass. The Java `UtilAll.exceptionSimpleDesc`
  versus Rust error-remark difference is accepted as diagnostic text only under
  the core flow-parity rule; response code and control-flow paths are mapped,
  while text-for-text equivalence is not claimed.
  The broad Phase 6 audit
  remains `PARTIAL`; Phase 9 now has fixture/unit coverage for raw Pull offset
  entrypoints, `consumerRunningInfo`, the construction-time start timestamp,
  `get/setPullAPIWrapper`, and Pull's `get/setServiceState`. The Push
  `getDefaultMQPushConsumerImpl()` alias, service-state, pause,
  consume-orderly, exception-delay, and message-queue-listener accessors,
  effective retry-limit getter,
  offset-store accessors, direct/periodic/shutdown offset persistence scoped to
  active pull queues, timestamp offset reset, `queryConsumeTimeSpan` direct
  route lookup and ordered per-Broker requests with VIP handling, consumer
  settings/subscriptions (including the class-filter overload's namespace,
  source retention and heartbeat path), and direct consume-hook entrypoints now
  have Java fixture/Rust evidence. Push filter-hook registration and POP invocation,
  `parseSubscribeMessageQueues`, `tryResetPopRetryTopic`, and
  `resetRetryAndNamespace` also have invoked Java fixture/Rust checks. Push
  raw listener registration/getter and facade-owner identity also have direct
  Java fixture/Rust checks. Push subscribe-route update and paused `tryRebalance` entrypoints now have direct
  fixture checks; active assignment behavior is covered by Phase 11 tests. The
  `pullMessage` row also now covers Java's one-time
  first-lock offset refresh for orderly clustering pulls; its Java fixture and
  Rust regression pass. Push reset-offset behavior now also matches Java's
  active-queue selection, null-offset drop/clear, callback drain and orderly
  lock windows, conditional offset-store cleanup, and late-work generation
  fencing; paused-time reset regressions cover the timeout and removal paths.
  The returned Push implementation's 71 declared public
  signatures are now captured from the pinned Java fixture and count-pinned in
  Rust. Its `consumerRunningInfo` method now has direct Rust owner coverage
  during runtime and after shutdown; the latter compares retained properties,
  subscription/status state and shutdown-cleared queue tables with Java. Before
  startup, the Rust owner returns typed `NotStarted`; pinned Java throws
  `NullPointerException` for its absent consume service. Its
  `ConsumerRunningInfo` body and
  `ProcessQueueInfo`/`PopProcessQueueInfo`/`ConsumeStatus` models now also match
  inherited Java object-identity equality instead of exposing structural
  `PartialEq`/`Eq`. `QueryAssignmentRequestBody`,
  `SetMessageRequestModeRequestBody`, `GetConsumerStatusBody`,
  `ResetOffsetBody`, and `ConsumeMessageDirectlyResult` now also omit Rust
  structural equality, matching inherited Java `Object.equals/hashCode`; their
  invoked identity fixture is `fixtures/protocol/dto-identity.json`.
  Sixteen typed response-header DTOs now also omit structural equality,
  matching the pinned Java classes' inherited Object identity; their fixture
  is `fixtures/protocol/header-response-identity.json`. The 31 modeled request
  headers inheriting `RpcRequestHeader.equals/hashCode` now retain and encode
  all four nullable base fields and compare/hash only those fields; the invoked
  Java fixture is `fixtures/protocol/request-header-identity.json`. The
  `ConsumeMessageDirectlyResultRequestHeader` base `bname` is separate from its
  subclass `brokerName`, and Lock/Unlock request headers have distinct Rust
  types. This closes the inherited request-header equality slice only; the
  broad P18-V01 audit remains open. The compact `SendMessageRequestHeaderV2`
  slice (`227876c1`) now also matches Java's nullable compact fields, inherited-field
  handling by fast versus reflective decode, malformed-number behavior, and
  V1 conversion of `brokerName` to `n`; the invoked fixture is
  `fixtures/remoting/send-message-request-v2-decode-modes.json`. This closes
  only that header slice and leaves the broad P18-V01 audit open.
  `adjustThreadPool` has a direct owner
  mapping and fixture evidence for the pinned built-in services' no-op
  behavior. The raw `executeTask`/`executeTaskLater` methods map to the
  owner-scoped runtime scheduler and now have invoked-fixture plus lifecycle /
  delay regressions. The raw consume-service, stats-manager, and client-factory
  accessors have source-backed internal-entrypoint dispositions: their owner
  flows are covered, while mutable Java service/factory handles stay internal
  to the Rust lifecycle owner. `RebalancePushImpl.messageQueueChanged` has an
  invoked Java fixture and Rust regression for global queue/size threshold
  scaling across multiple topics, later assignment changes, empty-table
  retention, pull backpressure, and running-info values; fixture SHA-256
  `c531359f6121cd35087a50150bebc14064f4a9d423825f2fdbb16aa7629adf67`. The
  raw Push inventory now has a method-level mapping or reviewed disposition.
  Heartbeat Fastjson input coverage now includes 1086 invoked Java cases with
  systematic whitespace positions and raw control characters in string keys
  and values; the Rust differential and Maven fixture-drift gate pass for that
  corpus. Route DTO Fastjson input coverage now includes 527 invoked Java
  cases, including 128 raw U+0000–U+001F probes across values, field names,
  and filter-map keys/values; the Rust differential and fixture-drift gate
  pass. These bounded inputs do not close the broad Phase 6
  source/API/input/reverse-export audit. Pull `getRebalanceImpl`, both
  `pullKernelImpl` overloads, `popAsync`, and the `RebalanceImpl`/`RebalancePullImpl`
  surface now have source-based owner-flow mappings or internal-entrypoint
  dispositions. Pull
  `sendMessageBack`/`maxReconsumeTimes` covers the 3000 ms timeout and logical
  Broker route lookup offline; its live Broker handoff remains pending.
  The public `TopicValidator.ValidateResult(boolean, String)` constructor,
  including null remarks and inherited identity-based `Object.equals`/
  `hashCode`, also has direct pinned-Java fixture and Rust API-boundary
  coverage. Rust intentionally omits structural `PartialEq`/`Eq` for this
  Java identity-valued result. `TopicValidator` null-input behavior and nullable
  system/send-block set members now have fixture-backed Rust differentials too;
  this closes those specific branches only, while the broad API/source/input
  audit remains open. A re-export-aware Rustdoc scan now has exact crosswalks
  for all producer and consumer inherent method names; Java-side reverse
  exports, inherited/trait methods, and source/input branch equivalence remain
  part of P18-V01. The previously unmapped Producer
  `setCallbackExecutor`/`setAsyncSenderExecutor` surface now has pinned Java
  fixture coverage, Rust facade methods, rejection/backpressure/deadline tests,
  and a Phase 7 matrix entry. Rust keeps caller-Tokio execution as its default;
  Java's private fixed-size sender pool is not reproduced. The runtime
  backpressure setters, in-flight permit adjustment, mode toggle, live retry
  response-code set and public adjustment/lock helpers found by the reverse scan
  now have Rust APIs and fixture-backed unit evidence. `getCompressor`,
  `setCompressType`, `initProduceAccumulator`, and runtime auto-batch state now
  have Producer APIs, invoked Java fixtures and Rust differential tests. The
  runtime batch delay, per-batch byte threshold, and aggregate hold cap also
  have invoked Java fixture coverage and live Producer APIs. Rust tests compare
  pre-init/default/runtime values, preserve effective settings after invalid
  updates, flush pending batches after a lower byte threshold, reschedule a
  pending delay, and apply aggregate-cap changes to later enqueue decisions.
  The implementation-level `setSemaphoreAsyncSendNum/Size` helpers now also
  have direct Rust APIs and invoked fixture evidence for raw replacement,
  negative permits, releases into the replacement semaphore, later facade
  resize accounting, and unchanged facade limits. The updated Producer core
  fixture SHA-256 is
  `defd34c1ebf1fc81bb6cdbed4406c40bf5794c73bdf7ef6a85d1e37b84cecbe5`.
  Reinitializing after an invalid runtime setter retains the already active
  accumulator values, matching Java. Fixture SHA-256 is
  `4b166c789a8698ac5600667e2594d7025f7c1b90dd67b2949719269cb6d8160c`.
  The
  runtime `setCompressMsgBodyOverHowmuch` and `setCompressLevel` settings also
  have live Producer APIs and Java/Rust regression coverage. The
  broad Java reverse-export, inherited/trait API, and source/input branch
  reconciliation remains local P18-V01 work, not manual validation. Producer
  selector failures now have invoked Java evidence for selector-only and sync
  send: both wrap the thrown selector failure with the Java client exception
  and cause. Rust catches selector panics as `ProducerError::SelectorPanicked`;
  the scripted regression compares the exact Java message and cause text and
  confirms that neither path sends a Broker request. `fixtures/producer/core.json`
  SHA-256: `d1b05a04ed53dde2820647bd964ea2092592b35d51de90273f5b22f77ded0fe7`.
  This closes that selector failure branch only; broad P18-V01 reconciliation
  remains open. Reply-host conversion and `MessageExt` host getter/setter types
  now have invoked Java differentials for resolved/unresolved addresses,
  IPv4/unbracketed IPv6, Unicode decimal ports, range boundaries, missing
  separators, invalid/out-of-range ports, the socket-address constructor,
  host-byte methods and `socketAddress2String`; these close only the host API
  and reply-processing slices. `fixtures/runtime/inbound-routing.json` SHA-256:
  `614adec3fe0eb41a261000744602f0247aa3ddaf03fdd42fdabf9ee9b8785eb2`.
  A follow-up Java `messageExtSystemFlagsApi` fixture now covers topic/group
  transient system-flag get/set/clear and malformed/Unicode integer inputs,
  V6 flag setters, and topic-filter parsing. Rust matches those cases in
  `message_ext_system_flag_helpers_match_java_551`, which also compares
  `MessageExt.toString()` for a deterministic one-property message. For
  messages with multiple properties, Rust uses its stable map order because
  Java `HashMap` order is unspecified. The broad source/API/input/reverse-export
  reconciliation remains open.
  The Java `Message` user API now also has direct Rust mappings for property
  lookup/mutation, Buyer ID and instance ID, collection-based keys, delay-level
  and timer-delay accessors, signed Java integer parsing and `toString()`;
  `MessageExt` exposes inherited Message methods through Rust deref. The
  invoked `messagePublicApi` fixture checks default null topic/body/properties,
  default plus all four parameterized constructor forms, nullable setter values, lazy property-map
  creation after null-name lookups, null-valued system properties, user-property
  null rejection, null collection/null-element keys, nullable JSON round-trips,
  numeric bounds, Unicode digits, malformed/overflow values and map mutation;
  fixture SHA-256 is
  `6ec3f130f86c25b12b08f5d5d51f5b43c0bab1a2e77738208909959bbef79db5`.
  This closes the Message constructor/null-state slice only; broader inherited
  API and reverse-export reconciliation remain local P18-V01 work. The pinned
  Common API reverse scan also removed Rust-only `Message::{take_body,
  take_body_nullable,set_property,properties_mut}` and
  `MessageQueue::new_nullable_broker`; compile-fail doctests guard those five
  public boundaries. The exact Java user-property and nullable queue APIs
  remain available.
  The public-surface reverse scan also found caller-configurable decoder limits
  and a limited decompression function with no Java API counterpart. Those
  controls are now internal fixed safety bounds; decoder and pull-result public
  calls no longer accept them. Low-cap regressions run inside the common crate.
  This closes the identified decoder API difference only; full P18-V01 remains
  open. The reverse-scan finding that `serde_fastjson` was an externally public
  helper module with no matching Java utility API is now closed: both the common
  and protocol adapters are crate-private, and rustdoc compile-fail cases check
  that downstream imports fail. Code commit: `b62e66c1`. Producer
  `EndTransactionHook` registration, context and direct execution
  now have fixture-backed local and Broker-check path coverage, including ordered
  execution and hook-failure isolation; this closes that API slice only and
  leaves broad P18-V01 reconciliation open. The pinned
  `DefaultMQProducerImpl` fixture now records its 2 public constructors and 82
  declared public methods. Direct CheckForbidden/SendMessage presence and
  execution helpers now map to public Rust methods, with fixture-backed tests
  for hook ordering, rejection short-circuit, context mutation and panic
  isolation. The latest fixture adds default-route ASYNC CheckForbidden
  wrapping and explicit-queue ASYNC pass-through cases. Producer service-state
  getters/setters now map to a nullable Rust
  enum with fixture-backed Java names/ordinals and lifecycle transitions;
  `isUnitMode` maps to the Producer config view. `getTopicPublishInfoTable`
  now returns a shared `TopicPublishInfoTable`; Java fixture/Rust tests cover
  live map/value/list/route/cursor aliases and a later scripted send observes a
  changed queue list. Its `ConcurrentMap` operations, nullable callback/key/
  value/result branches, ordered `putAll` null-row behavior, and retained
  key/value/entry view single, bulk, `removeIf`, and `setValue` mutations now
  have invoked Java fixture and Rust differential evidence. Fixture SHA-256 is
  `3815a09b647c39d68ae6bb3538e3d026eb48dd9dc12135c16d50c2a1c9f04ca2`.
  `TopicPublishInfo.selectOneMessageQueue(QueueFilter...)` also has direct
  null/empty array, null filter, null/empty queue-list and non-short-circuit
  filter-order differentials in that fixture.
  The pinned fixture now inventories the `MQClientInstance` returned by the
  Producer client-factory getter and the `MQFaultStrategy`/`BrokerFilter`
  surfaces. Their raw mutable factory, filter, and ThreadLocal objects remain
  owned by Rust runtime/Producer lifecycle owners; each pinned Producer call
  site maps to an owner flow. This closes those surface-disposition slices.
  The boolean `start`/`shutdown` overloads and raw
  `executeAsyncMessageSend` helper now also have pinned call-site and owner
  dispositions in the Phase 7 matrix; their use is confined to shared factory
  lifecycle and typed async Producer send paths.
  The facade inventory now pins 11 `DefaultMQProducer` constructors,
  107 declared/183 callable methods, 34 declared/42 callable `MQProducer`
  methods, and 8 `MQAdmin` methods. Rust also exposes direct-send and forced
  accumulator paths, with scripted coverage for both choices under opposite
  `autoBatch` settings. All 11 constructor signatures are individually pinned,
  and their group, RPC-hook, topic, namespace and trace-field mappings are now
  invoked against Java and differentially checked against Rust configuration,
  including nullable group, namespace, topic and trace values. `ProducerConfig`
  preserves nullable groups with an `Option<String>` representation, and null
  group construction/setter/start behavior matches Java's blank-group error
  and `START_FAILED` state through the nullable `set_producer_group` API.
  Public Producer hook contexts also preserve nullable group fields.
  Java's retained topic-list identity maps to
  Rust-owned values. This closes the constructor and null-group lifecycle
  slice; malformed-input and broad P18-V01 reconciliation remain open.
  Per-overload input/branch mappings, Java reverse-export, inherited/trait API,
  and broad source/input reconciliation remain local P18-V01 work.
  A targeted trait pass now maps the custom consistent-hash constructor,
  `CommandHeader`, consumer runtime/request adapters, TLS decrypt strategy, and
  listener/transaction executor callbacks; no missing capability was confirmed
  in those slices. Phase 10 and 12 now include method-level crosswalks for the
  public concurrent and orderly listener traits, their returned status/context,
  null/panic disposition, and owning callback tests. The full inherited and
  Rust/Java reverse-export audit is
  still open.
  Runtime Producer scalar settings for create-topic key/queue count, send and
  per-request timeouts, sync/async retry counts, retry-after-non-OK-store, and
  message-size limit now have live `Producer` setters and fixture-backed send,
  retry, deadline, and validation regressions (code commit `781bf6f0`; fixture
  SHA-256 `46c9c9522317d72d2942fab4abae0caf9c6fd9bf76a179ea90a6ec453b041f3c`).
  Follow-up oracle and regressions now cover per-request timeout setter inputs
  `-2`, `-1`, `0`, `125`, and `6000`; a deterministic zero-cap case confirms
  sync retries continue while the outer timeout remains. This closes that
  timeout/retry slice only; broad P18-V01 reconciliation remains open.
  Live Producer group and topics setters now have fixture-backed behavior:
  group changes reach subsequent headers (including null-field omission), and
  topic getters reflect setter calls while the started runtime retains its
  startup topic snapshot. VIP getter mutation is also covered; the active
  runtime keeps the ClientConfig snapshot copied at start, matching Java's
  `MQClientManager.getOrCreateMQClientInstance`. The Producer fixture SHA-256
  is `1ed0060666e8ee2a6b5b82b3088b22cb41a9a690bc291a96e56e51d16e7f7d76`.
  Post-start Producer-group mutation now has direct Java lifecycle fixture and
  Rust differential coverage: shutdown unregisters by the current group key,
  including the Java null-group exception and service-state result; pending
  request entries remain after producer shutdown. Fixture
  `fixtures/producer/lifecycle.json` SHA-256 is
  `9d5063e8ae538f7da3f957c874dfb95dd81a6a85bf113a947a796de1815001d8`.
  Per-overload input/branch mappings, Java reverse-export, inherited/trait API,
  and broad source/input reconciliation remain local P18-V01 work.
- [x] Map runtime Producer fault-strategy settings: live latency-fault and
  detector toggles, mutable `long[]` table aliasing/replacement, and subsequent
  Broker selection. Null setter/getter behavior, deferred null-table NPEs,
  short duration-table AIOOBE, and ignored extra duration entries now have
  invoked Java fixtures and Rust differentials; see the Phase 7 matrix and
  Phase 6 audit.
- [x] Register live cases in the Phase 19 scenario manifest and validate the
  manifest locally.
- [x] Re-run full workspace/all-feature P18-V02 gates on Producer revision
  `1845c55a`. Default/all-feature workspace tests, the 112-unit-test
  Producer suite and integration targets, strict workspace Clippy, fmt/diff,
  Maven verify (37 scenarios), comparator self-test and 37-scenario manifest validation, the
  16-case race/leak smoke repeated three times, 1,824-mutation fuzz smoke,
  release examples and benchmark compilation pass. The new runtime fault
  fixture SHA-256 is
  `2f8c6d40c4669a6a662085e8cddef749338c91fcbb1bf64829ef1f59f77a284e`.
- [x] Repeat full workspace/all-feature P18-V02 gates on current revision
  `6c62ebf2`. Sequential default and all-feature workspace tests pass; the
  Producer suite passes 120 unit tests and all integration/doc targets. Strict
  workspace Clippy, Maven verify (37 scenarios), comparator self-test and
  manifest validation, 16 race/leak cases repeated three times, 1,824-mutation
  fuzz smoke, release examples, benchmark compilation, formatting and diff
  checks pass. Workspace metadata and all seven package contents also pass.
- [x] Re-run server-free P18-V02 gates with the runtime Producer accumulator
  parity change on the worktree based on `454d57cf`, before its feature commit.
  Default and all-feature workspace tests, Producer tests (123 unit tests and
  all integration/doc targets), strict workspace Clippy, default Maven verify
  with fixture drift checking (37 scenarios), comparator self-test and
  37-scenario manifest validation, 16 race/leak cases repeated three times,
  1,824-mutation fuzz smoke, release examples, benchmark compilation, package
  metadata/content checks, formatting and diff checks all pass. The updated
  Producer fixture SHA-256 is
  `4b166c789a8698ac5600667e2594d7025f7c1b90dd67b2949719269cb6d8160c`.
- [x] Re-run the server-free P18-V02 gates on revision `281dd9e0` after
  nullable Producer group compatibility. Default and all-feature workspace
  tests, strict workspace Clippy, formatting/diff checks, Maven fixture drift
  and Java/Rust cross-decodes (37 scenarios), comparator self-test,
  37-scenario manifest validation, 16 deterministic race/leak cases repeated
  three times, 1,824-mutation fuzz smoke, release facade examples, offline
  benchmark compilation, and workspace metadata/license/dependency checks pass.
  The Producer fixture SHA-256 is
  `b0a59a0c084fc24ddff5c4947e7064d20311fe0dcf38306b8ac45ee15546c4fc`.
- [x] Re-run current-revision server-free P18-V02 gates on `f3d5b44d` after
  Producer group, VIP-channel and topic setter compatibility updates. Default
  and all-feature workspace tests, strict all-target/all-feature Clippy,
  formatting/diff checks, Maven verify (37 scenarios, fixture drift and Java /
  Rust cross-decodes), comparator self-test, 37-scenario manifest validation,
  16 race/leak cases repeated three times, 1,824-mutation fuzz smoke, release
  example build, offline benchmark compilation, and workspace metadata checks
  pass. Producer fixture SHA-256:
  `1ed0060666e8ee2a6b5b82b3088b22cb41a9a690bc291a96e56e51d16e7f7d76`.
- [x] Re-run server-free P18-V02 gates on lifecycle revision `9834d96d`.
  Default and all-features workspace tests, strict all-target /
  all-feature Clippy, formatting/diff checks, Maven verify (37 scenarios with
  fixture drift and Java/Rust cross-decodes), comparator self-test, 37-scenario
  manifest validation, 16 race/leak cases repeated three times, 1,824-mutation
  fuzz smoke, release example build, offline benchmark compilation, and
  workspace/package metadata checks pass. The lifecycle fixture SHA-256 is
  `9d5063e8ae538f7da3f957c874dfb95dd81a6a85bf113a947a796de1815001d8`.
- [x] Re-run current-tree P18-V02 server-free gates at revision `450ed704`
  after Producer send-overload coverage. Default and all-features workspace
  tests, strict all-target/all-feature workspace Clippy, formatting/diff,
  Maven verify (37 scenarios, fixture drift and cross-decodes), comparator
  self-test, manifest validation (37 scenarios), deterministic race/leak smoke
  (16 cases × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824
  mutations), release examples, offline benchmark compilation, workspace
  metadata, and seven package-content listings all completed successfully.
- [x] Re-run server-free P18-V02 gates for the CheckForbidden synchronous
  retry slice on the worktree based on `3695b821` (2026-10-14). Default and
  all-features workspace tests, strict all-target/all-feature workspace
  Clippy, fmt/diff checks, Maven verify (37 scenarios with fixture drift and
  Java/Rust cross-decodes), comparator self-test and 37-scenario manifest,
  16 deterministic race/leak cases repeated three times, 1,824-mutation fuzz
  smoke, release examples, offline benchmark compilation, and workspace
  metadata all pass. `fixtures/producer/core.json` SHA-256:
  `ec20ff084ee066fc71de15e9d744bc091aaced07d1038a8389f12a79d5985b08`.
- [x] Re-run current-worktree server-free P18-V02 gates after default-route
  ASYNC and explicit-queue ASYNC CheckForbidden parity (`39afef81`, based on
  `051c01a3`, 2026-10-14). Default and all-features workspace tests, the
  Producer suite (138 unit tests and 55 integration/doc targets), strict
  all-target/all-feature workspace Clippy, fmt/diff, Maven verify (37
  scenarios with fixture drift and cross-decodes), comparator self-test and
  37-scenario manifest validation, deterministic race/leak smoke (16 cases ×
  3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release
  examples, offline benchmark compilation, workspace metadata/license checks,
  and seven package-content listings pass. `fixtures/producer/core.json`
  SHA-256: `d4ef907983f88f7948c4b143fbcd061115cca4ebdc03c19e6acae52e0dc30408`.
- [x] Re-run Producer-scoped checks after the final ASYNC fault-entry assertion
  (`f314116a`, 2026-10-14): `cargo test -p rocketmq-classic-producer
  --locked` passes (138 unit tests and 55 integration/doc targets), as do
  strict all-target/all-feature Producer Clippy, formatting and diff checks.
  The full workspace/all-feature, Maven, race/leak, fuzz and package gates above
  were run on parent `39afef81`; this follow-up only changes a Producer test.
- [x] Close the missing Broker publish-address branch (`e48fb323`, 2026-10-14).
  The default send path now retries the Java `MQClientException` and returns the
  response-code-10003 wrapper with attempt/Broker metadata; explicit queue and
  selector sends expose Java's `The broker[broker] not exist` error. The route
  fixture preserves the writable queue with a null master address, and asserts
  no redundant NameServer fetch. Default/all-feature workspace tests, strict
  workspace Clippy, Maven fixture/differential verification, comparator and
  37-scenario manifest checks, race/leak and fuzz smoke, release examples,
  benchmark compilation, metadata and package-content checks all pass on
  `e48fb323`; the selector regression and full Producer suite also pass on
  follow-up test commit `28d28377`. Producer core fixture SHA-256:
  `725af3b3887f7a4ec6f6d72a23188e4fb80206e50c98b113b015d76917fedfd8`.
- [x] Re-run Producer checks after caller-visible `UNIQ_KEY` mutation parity
  (`4b311231`, 2026-10-14). `cargo test -p rocketmq-classic-producer --locked`
  passes (139 unit tests and 60 integration tests); strict all-target/all-feature
  workspace Clippy, Maven verify (37 scenarios, fixture drift and Java/Rust
  cross-decodes), formatting, and diff checks pass. The invoked Java fixture
  verifies accumulator mutation before a failed dispatch and direct-send
  service-state failure before mutation; scripted Rust tests cover these
  boundaries, direct-send timing, retry ID reuse, auto-batch IDs, and
  caller-supplied ID preservation. Producer fixture SHA-256:
  `079e9fcac47f97f4921a823c10b02a3b0ec882c42b85c85695663530047cd23d`.
- [ ] Obtain a current GitHub Ubuntu CI result for the Linux-specific Java
  address profile. A native Ubuntu ARM64 container passes the Core RPC
  integration suite; this does not replace the required GitHub Ubuntu result.
- [ ] Complete Phase 19 Broker/Docker scenarios before final compatibility or
  release acceptance. The local Docker runner and scenario execution are still
  implementation/automation work and cannot be reassigned to the manual list.
  The owner must provide any deployment-only credentials, topology, and
  maintenance inputs at that phase.

- P18-V01 ClientConfig reverse-surface follow-up (`2026-10-15`, pinned Java
  source `efa1be59d6fc747ad220f036c86030f05f0419f9`): all 33 Rust
  `ClientConfig` backing fields are crate-private; the mapped getter/setter
  surface remains public. Consumer running-info uses a read-only hidden bridge
  for Java `MixAll.object2Properties`, retaining raw values and null omission.
  The external compile-fail doctest passes. Core config differential passes
  (21/21); Consumer all-feature tests pass (219 unit tests and all integration
  and doc targets); Producer all-feature tests pass (142 unit tests and all
  integration/doc targets). This closes the ClientConfig field-visibility
  discrepancy only; broad P18-V01 remains open.

- P18-V01 Remoting per-channel setting timing (`b37d8522`, 2026-10-15):
  `clientChannelMaxIdleTimeSeconds` and `disableNettyWorkerGroup` now have live
  adapters sampled for each new channel. Rust preserves the initialized idle
  handler and dispatch path on existing channels; its worker pool starts once
  so later channels can select either worker dispatch or direct dispatch.
  Rust loopback tests cover post-start mutation and old-channel retention; the
  Java `RemotingLiveConfigProbe` verifies the corresponding idle handler and
  event executor for channels created after mutation. The two setting-timing
  rows are closed; broad P18-V01 remains open.
- PASS on `b37d8522`: `cargo test -p rocketmq-classic-remoting
  --all-features --locked` (89 unit, 6 codec, 1 fuzz regression, 5 security,
  95 transport, 5 doctests), strict Remoting all-target/all-feature Clippy,
  `cargo fmt --all -- --check`, Maven `verify` (37 scenarios, live-config
  probe, Java/Rust cross-decodes and fixture checks), and `git diff --check`.
  These are scoped checks, not the final full-workspace P18-V02 rerun.

## Phase 18 verification disposition

- [ ] **P18-V01** — Broad Java/Rust source, overload input/error/flow, inherited
  API, and reverse-export reconciliation remains open; this is local parity
  work, not final manual validation. Earlier Phase 3 notes that accepted
  unmatched Rust-only APIs, immutable config snapshots, or selected observable
  differences are historical and do not waive the current parity contract.
  Remoting `clientCloseSocketIfTimeout` now has a live Rust update adapter and
  Java/Rust post-start timeout evidence; `clientWorkerThreads`, `enableReconnectForGoAway`,
  `disableCallbackExecutor`, and `isScanAvailableNameSrv` also have live
  adapters and timing regressions. These close their setting rows only.
  The producer send mutation branch is now
  oracle-backed, and surfaced Rust-only publish-info constructors/snapshot
  helpers are crate-private. Producer publish-info queue/cursor/selector and
  live-view callback adapters now have exact Rust-to-Java matrix rows guarded
  by Producer tests. The full 17-method `TopicPublishInfo` and 3-method
  `ThreadLocalIndex` Java surfaces are fixture-pinned; cursor overflow and
  `toString()` formatting are checked against Java. The broad reverse-export
  and overload/input/error/flow audit remains open. The Phase 16 ACL reverse
  scan also removed duplicate public module helpers, the non-Java one-argument
  `AclUtils::combine_request_content`, and `SessionCredentials` builder/YAML
  exports while preserving the Java static `AclUtils`/`AclSigner` mapping and
  pre-encoded Java-Charset signer adapter; compile-fail doctests guard the
  removed boundaries. Code commit `6893f14b`; the Phase 16 ACL matrix and
  security usage guide name the retained Java-mapped entrypoints. The full
  Core locked test suite, strict Core Clippy, format and diff checks pass for
  this slice. Core `AdminQueryRequest` now keeps derived route/LMQ metadata
  private, preserves Java's nullable cluster/index-type/pagination inputs with
  builders, and matches the null-index result filter; LMQ and wheel-timer route
  tests cover the derived behavior. Code commits `e7dcdf63` and `daef67eb`;
  Core/Producer/Consumer locked tests, strict workspace Clippy, format, diff,
  and rustdoc JSON surface inspection pass for this slice. `QueryResult` now
  also matches Java's private fields, public constructor/getters, nullable
  list/elements, shared mutable list alias, and `toString`; invoked Java
  reflection/behavior fixtures guard the surface. Code commits `ba2589ec` and
  `57fe1527`. Default and all-feature workspace tests, strict workspace
  Clippy, Maven fixture verification, and Core/Producer/Consumer rustdoc JSON
  generation pass; Core JSON shows stripped fields. These bounded corrections
  leave P18-V01 open. The Pull facade's Rust-only singular `register_topic`
  helper is now private, and the combined `unregister_topic` convenience
  method was removed because pinned Java has no corresponding public API; the
  Java `registerMessageQueueListener`, register-topic set, subscription, and
  clear-subscriptions entrypoints remain available. Code commit `2bfa4c16`;
  Consumer locked tests/doctests, strict Consumer Clippy, formatting, and diff
  checks pass. `LocalFileOffsetStore::group_name` and `storage_path` were also
  removed from the public surface because pinned Java exposes neither getter;
  compile-fail doctests guard both boundaries. Code commit `e8582a22`; Consumer
  locked tests/doctests, strict Clippy, rustdoc JSON, formatting, and diff
  checks pass. The pinned Java Push/Pull facade inventory now records declared
  and inherited signatures and confirms neither facade declares a separate
  `queryMessageByUniqKey`; Rust's extra `query_message_by_unique_key` methods
  are removed. All three Rust `view_message` facades now fall back from an
  invalid physical ID or failed physical lookup to Java's unique-key query
  defaults (32 results, three-day lower bound, `Long.MAX_VALUE` upper bound),
  returning the first nullable result. Scripted tests cover fallback fields,
  namespaced topics, physical lookup, and terminal errors. The Pull example
  now registers through the Java-mapped listener API. This closes those facade
  methods and branches only; broad P18-V01 reconciliation remains open.
- Common message formatting reverse-surface slice: `Message::java_to_string`,
  `MessageExt::java_to_string`, and `MessageHostAddress::java_to_string` are
  now internal helpers; Java `toString()` behavior remains exposed through
  Rust `Display`/`ToString`. Compile-fail doctests guard all three boundaries.
  Common all-feature tests, strict Clippy, formatting, and diff checks pass.
  Constructor/property accessors and the wider Common/Core/API reconciliation
  remain open P18-V01 work.
- Common message equality reverse-surface slice (initial implementation): Java
  `Message` and `MessageExt` inherit identity-based `Object.equals`/`hashCode`;
  Rust equality used a hidden `MessageExt` identity token, while `Message` had
  no equality trait. The later 2026-10-16 identity/hash slice below supersedes
  this implementation and its compile-fail doctest. Default/all-feature
  workspace tests, strict workspace Clippy, Maven verify (37 scenarios and
  cross-decodes), formatting, and diff checks pass for this slice. The full
  P18-V02 rerun, including race/fuzz, example, benchmark, metadata, and
  package-content gates, remains required on the final revision. Broader
  Common/Core/API reconciliation remains open P18-V01 work.
- Common `putUserProperty` whitespace input slice: Rust now follows Java
  `String.trim()` (U+0000–U+0020 only), preserving Java acceptance of
  non-breaking-space keys and em-space values. Invoked Java and Rust fixture
  cases cover those values and rejection of ASCII control/space-only input.
  This input boundary is closed; broad Message overload/input/API
  reconciliation remains open P18-V01 work.
- Common `MessageConst.STRING_HASH_SET` mutation: Rust now exposes a
  lock-protected mutable set with nullable members, and `putUserProperty`
  checks live membership before blank validation. Invoked Java and Rust
  fixtures verify dynamic additions/removals plus null/empty-string entries;
  null and empty reserved keys produce Java's reserved-property failure before
  blank-name validation. Fixture SHA-256:
  `cd48ccada3c107facb4ec332316ef29e569cca7c50cd3b0711152b7234cd6b98`.
  This closes that mutable-set behavior slice; broad Common reverse-surface
  reconciliation remains open P18-V01 work.
- Common `Message` priority accessors: `priority`/`set_priority` now match the
  pinned Java getter's `-1` fallback and decimal parsing, and setter rejection
  for negative values. Invoked Java/Rust fixtures cover map initialization,
  malformed/overflow/Unicode-digit inputs and no mutation after rejection.
  This closes that API slice only; P18-V01 remains open.
- Common MessageId API slice: Rust now exposes both Java `createMessageId`
  address forms, `decodeMessageId`, and the mutable nullable `MessageId` model.
  The invoked Java fixture covers IPv4/IPv6, negative 64-bit offsets, invalid
  hex conversion, trailing bytes, short input and invalid ports. This closes
  the MessageId surface only; fixture SHA-256 is
  `f58a8b10bf498cf00e3914a9fbc2a83016fb6b3bcbb8db9933403d834c1c121a`;
  P18-V01 remains open.
- Common CRC property helper slice: `append_crc32_property` matches both Java
  `createCrc32` overloads for zero, positive and negative inputs. The invoked
  fixture checks ByteBuffer and ByteBuf output equality; P18-V01 remains open.
- Common record-property decoder slice: `decode_record_properties` maps
  Java's public `decodeProperties(ByteBuffer)` behavior for the pinned V1/V2
  records, including null when no properties are present; P18-V01 remains open.
- Common compact-message API slice: public encode/decode, concatenated batch,
  inner-batch expansion and count helpers now match the invoked Java 5.5.1
  fixture, including copied outer metadata and checked malformed inputs.
  P18-V01 remains open for the other MessageDecoder methods and broad audit.
- Common `MessageExt` record encoder slice: public standard and unique encoders
  match the invoked Java 5.5.1 bytes for IPv4/IPv6, configured-size padding and
  undersized buffers; compressed ZLIB/LZ4/ZSTD records decode to Java's expected
  body. Rust regression and the Java fixture are updated. This closes those two
  encoder methods only; at that checkpoint, the remaining decoder overloads
  and broad P18-V01 audit remained open. Current message fixture SHA-256:
  `c7bf73f5baa095242502d97e06953e2ef2a2c4ea774ed69923aeef586fde940a`.
- Common client decode and `MessageDecoder` constant slice: Rust now exposes
  client ID fallback helpers, client batch decoding, Java record-offset and
  magic constants, and the UTF-8 name. Invoked fixture checks `clientDecode`,
  `decodes`, `isClient` IDs, and all public decoder constants. Regression tests
  pass for the client decode overloads and constants. This slice does not close
  the remaining P18-V01 audit. Current message fixture SHA-256:
  `756381ff5802566bf8236bae22c9e8ea1b1d008a27a728436ed6bb8e384a1746`.
- Common message property codec slice: nullable Java map/string inputs and
  nullable key/value entries now have explicit Rust adapters. The invoked
  fixture covers null maps, skipped null values, Java's literal `null` key,
  null decode input and permissive duplicate/malformed entries. This closes the
  property codec input slice only; P18-V01 remains open. Current message
  fixture SHA-256:
  `b4ae563511220dbf008a4ef0bc30d82f2c325005e36eb52b15a5ee87bff59d40`.
- Common `MessageExt` broker-name accessors now have direct Java fixture and
  Rust getter/setter coverage for default, non-null and cleared values. This
  closes this accessor slice only; P18-V01 remains open. Fixture at this slice
  SHA-256: `bb5fd8ba6d671739bb352e7780f29661e759c53dce7fe3887e1726149660ad1e`.
- Common `MessageVersion` now exposes Java's enum metadata and V1/V2 signed
  topic-length reads/writes. The invoked fixture and Rust regression cover enum
  order/names, magic conversion, signed narrowing, read/write cursors and
  truncated-read errors. Current message fixture SHA-256:
  `3d867fbd1ace33d2c95561ec12d7ac2f9f709fddc742c754459c7f75e08d563d`.
- Common message constants now have a direct Java-to-Rust value audit: the
  fixture reflects all 76 public `MessageConst` string fields, pins the two
  `MixAll` reply constants, and records the initial mutable reserved-property
  set. `message_constants_match_java_551` compares every value and reserved
  name. P18-V01 remains open for the broader source/API/input/flow audit.
- Common flag handling no longer exports Rust-only `has_compressed_flag`.
  Producer checks use Java's `MessageSysFlag.check(flag, COMPRESSED_FLAG)`
  counterpart, and a compile-fail rustdoc case guards the removed export.
  Broader P18-V01 reconciliation remains open.
- The five `PullSysFlag` bit masks are private in Rust, matching Java. Consumer
  and Core call sites now build flags through the mapped Java helper methods;
  a compile-fail rustdoc case guards the private constants. Broader P18-V01
  reconciliation remains open.
- `MessageExt::set_message` is removed: Java inherits `Message` state directly,
  and Rust already exposes those inherited setters through `DerefMut`. A
  compile-fail rustdoc case guards the removed Rust-only method; broader P18-V01
  reverse-export reconciliation remains open.
- `MessageAccessor.setProperties` now maps to `Message::set_properties` for
  replacing, emptying, and clearing the property map, with pinned Java fixture
  coverage. The selector path now matches `cloneMessage`'s observable fields:
  it gives callbacks an unnamespaced snapshot with the source body, flag and
  properties, and no transaction ID. Java's shared body/property references
  are recorded in the fixture; Rust callbacks receive an immutable reference,
  so a deep-owned snapshot preserves the observable values. Null-key maps now use `NullableMessageProperties`
  and `Message::set_nullable_properties`, with nullable lookup/clear APIs and
  encoded-key coverage across compact/stored records, producer send/batch, and
  consumer retry-topic fallback. Current message fixture SHA-256:
  `7c61fee7311b7926b92b845899083e5f80a2ba8e67105ed7656de3d5e3a17a22`.
  The same invoked fixture now calls Java's transfer/correction/origin-ID/MQ2/
  reconsume/max-reconsume/consume-start helper pairs and compares the Rust
  property-constant mappings, including a present null transfer flag. This
  closes those helper getter/setter value paths; broad P18-V01 remains open.
  `MessageExt` scalar getters/setters also now have a pinned Java fixture for
  zero/null defaults and signed boundary values, compared against all Rust
  scalar owner methods. This closes that API value slice only.
  Message tags, string keys, wait-store and transaction-ID setters also have
  direct Java/Rust getter checks, including nullable state and overwrite
  behavior.
  A Rust `MessageClientExt` view now preserves the Java no-op `setMsgId`, live
  `UNIQ_KEY` getter and mutable offset-ID fallback semantics. Its direct
  fixture/differential closes that subclass API slice only; broader P18-V01
  remains open.
  The Java `MessageQueue` constructors, fields, copy, equality/hash/text and
  ordering methods now have a method-level Phase 8 crosswalk to their existing
  invoked fixtures and Rust differential tests; broad P18-V01 remains open.
  `MessageClientIDSetter`'s public ID decoding helpers now map to Rust APIs and
  Java differential cases; the generator and near-time reconstruction use the
  process-local month boundary. The focused differential also passes with
  `TZ=Asia/Taipei`. The Java fixture now stores the near-time delta from its
  selected month start instead of an absolute epoch, avoiding timezone and
  month-rollover drift; the focused Rust differential passes under UTC and
  `TZ=Asia/Taipei`. CI runs the non-UTC case explicitly. This closes that
  helper slice only; broad P18-V01 remains open.
- [x] **P18-V02** — Re-run the complete server-free suite on revision
  `51be0d20` (2026-10-15). Default workspace tests under UTC, all-feature
  workspace tests, strict all-target/all-feature Clippy, Maven `verify` (37
  scenarios, fixture checks, and Java/Rust cross-decodes), comparator
  self-test and 37-scenario manifest, 16 deterministic race/leak cases × 3,
  fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release
  examples, workspace benchmark compilation, locked Cargo metadata, workspace
  metadata/license/dependency checks, seven package-content listings, format,
  and diff checks pass. The near-time ClientID differential also passes under
  UTC and Asia/Taipei. P18-V01, current-revision GitHub Ubuntu address-profile
  evidence, and Phase 19 Broker/Docker validation remain open.
- [x] Re-run the full server-free P18-V02 gates on current revision `15f8494b`
  after the Push runtime-setting correction (2026-10-14): default and
  all-feature workspace tests, strict all-target/all-feature Clippy, Maven
  verify (37 scenarios and Java/Rust fixture cross-decodes), comparator
  self-test, 37-scenario manifest, 16 race/leak cases × 3, fuzz smoke (104
  frame seeds, 10 message seeds, 1,824 mutations), release examples, offline
  benchmark compilation, workspace metadata/license/dependency checks, seven
  package-content listings, formatting, and diff checks all pass.
- Follow-up commit `7ece0e66` pins Java's per-pull reads of `pullBatchSize` and
  `pullBatchSizeInBytes` and adds a scripted wire assertion for post-construction
  updates. Default and all-feature workspace tests, strict Clippy, Maven
  verification (37 scenarios), format, and diff checks pass for this correction.
- Follow-up commit `f0eaf53e` reconciles Push runtime setting read timing against
  the pinned Java call sites. Pull/flow-control/POP/consume/heartbeat settings
  now read live values where Java does; `PullAPIWrapper` unit mode and the
  rebalance message model/allocation strategy take Java's start-time snapshots.
  Queue listener and `clientRebalance` changes after consumer construction
  reach later rebalance calls. `cargo test -p rocketmq-classic-consumer
  --all-features --locked` passes (205 unit tests, all consumer integration
  targets and doctests), as does strict all-target/all-feature Consumer Clippy.
  Formatting and diff checks pass. These setting-timing rows are closed; the
  broader P18-V01 audit remains open.
- [x] **P18-V03** — Facade examples, package graph/content/provenance, license
  files, compatibility baselines, and existing usage/behavior documentation
  have been reviewed. No release or compatibility claim is made.
- [ ] **P18-V04** — The 37-scenario handoff manifest and server-free local
  gates pass on code revision `de2d1a8f` (documentation HEAD `15f9705c`):
  default/all-feature workspace tests, strict workspace Clippy/check, release
  examples, offline benchmark compilation, locked metadata for all seven
  `0.1.0` packages, and package-content listings with `LICENSE`/`NOTICE`.
  Current-revision GitHub Ubuntu evidence for the Linux address profile and
  native Windows stack-runtime evidence are still absent, so P18-V04 remains
  open.

The Push runtime-setting timing rows listed in the prior audit checkpoint were
reconciled in commit `f0eaf53e`. P18-V01 remains open for the full Java/Rust
source, public API/settings, overload/input/error/control-flow and reverse
export audit; the current consumer-scoped test and Clippy pass do not replace
the required full Phase 18 gates on the final revision.

- Push consumer-group follow-up (`bd869fae`, 2026-10-14): the retained facade
  group is nullable and live; startup writes back the namespaced group,
  heartbeat preparation and shutdown use the current group, and consume-hook
  context preserves Java null. Pinned Java heartbeat and Push implementation
  fixtures cover mutation, unregister-by-current-group, and nullable context.
  Consumer all-feature tests (209 unit tests plus integration targets and
  doctests), strict Consumer Clippy, Maven verify (37 scenarios, fixture drift
  and Java/Rust cross-decodes), formatting, and diff checks pass for this
  slice. This does not close P18-V01 or replace full workspace gates on the
  final revision.
- Query-consume-time-span nullable group follow-up (`633b94bc`, 2026-10-14):
  the public request header retains nullable topic/group fields, and Push uses
  the current group. Pinned Java header conversion and the Rust loopback
  request both verify that a null group is omitted from ext fields. Protocol
  tests (including header inventory/validation), the Core API regression, the
  Consumer loopback case, strict Clippy for Protocol/Core/Consumer, Maven
  verify (37 scenarios and fixture/cross-decode checks), formatting, and diff
  checks pass. P18-V01 remains open beyond this path.
- Pull statistics group follow-up (`b05dff72`, 2026-10-14): Push now indexes a
  successful pull's statistics with the group retained by its `PullRequest`,
  matching Java's `incPullRT`/`incPullTPS` call sites. The invoked Java fixture
  mutates the facade group after creating the request and confirms the request
  keeps the original group; the Rust regression confirms statistics use that
  request group after facade mutation. Consumer all-feature tests (210 unit
  tests plus integration targets and doctests), strict Consumer Clippy, Maven
  verify (37 scenarios and fixture/cross-decode checks), formatting, and diff
  checks pass. This closes only the Pull statistics key path; consume-service
  statistics and the broad P18-V01 audit remain open.
- Consume-service statistics group follow-up (`edef3bd3`, 2026-10-14): Rust
  snapshots the namespaced group when Push services are created and uses it for
  concurrent/orderly consume metrics and POP-concurrent batch metrics. The
  pinned Java fixture constructs all four services, mutates the facade group,
  and confirms their retained group. POP-orderly consume metrics remain absent,
  matching its Java service flow; RunningInfo continues to look up the current
  facade group. Consumer all-feature tests (213 unit tests plus integration
  targets and doctests), strict Consumer Clippy, Maven verify (37 scenarios and
  fixture/cross-decode checks), formatting, and diff checks pass. This closes
  the identified consume-stat group timing paths only; P18-V01 remains open.
- Live consumer-group flow follow-up (2026-10-14): Push callback hook contexts
  and trace records now read the current nullable facade group; Broker send-back
  uses the live group while fallback retry topics apply the configured
  namespace. Pull send-back keeps the current/default or explicit group in the
  Broker request, while a failed request falls back to Pull's default facade
  group even when the request used a custom override. Invoked Java fixtures and
  Rust regressions cover those paths, including retry-topic fallback after
  facade mutation. Consumer all-feature tests (219 unit tests, all integration
  targets and doctests), strict Consumer Clippy, Maven verify (37 scenarios,
  fixture drift and Java/Rust cross-decodes), formatting and diff checks pass.
  Push fixture SHA-256 `fdc94afde6f4fdd74ffeab6fae12fad521a831178e070c4e072c25ef718a1f82`;
  Pull fixture SHA-256 `1090617637533587bc425acaf1862521b7d2826c8747839a14a6e985fbdd4c78`.
  This closes only the audited group-timing paths; the broad P18-V01 audit and
  full workspace gates on the final revision remain open.
- Current-revision server-free gate rerun (`2c87e4ed`, 2026-10-14): default and
  all-feature workspace tests, strict all-target/all-feature workspace Clippy,
  `cargo fmt --all -- --check`, pinned Maven `verify` (37 scenarios, fixture
  drift and Java/Rust cross-decodes), differential self-test and 37-scenario
  manifest, fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations),
  16 deterministic race/leak cases repeated three times, release examples,
  offline benchmark compilation, workspace metadata/license/dependency checks,
  seven package-content listings, and `git diff --check` all pass. This closes
  the current-revision P18-V02 server-free rerun; broad P18-V01 and the required
  GitHub Ubuntu address-profile result remain open.
- Current-revision server-free gate rerun (`8f530402`, 2026-10-14): default and
  all-feature workspace tests, strict all-target/all-feature workspace Clippy,
  formatting and diff checks, Maven `verify` (37 scenarios, fixture drift and
  Java/Rust cross-decodes), differential self-test and 37-scenario manifest,
  fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), 16
  deterministic race/leak cases repeated three times, release examples, offline
  benchmark compilation, workspace metadata/license/dependency checks, and
  seven package-content listings all pass. This closes the current-revision
  P18-V02 server-free rerun only; P18-V01, the GitHub Ubuntu address-profile
  result, and Phase 19 Broker/Docker validation remain open.
- Current-revision server-free gate rerun (`7a4bfef5`, 2026-10-15), after the
  ACL public-surface correction: default and all-feature workspace tests,
  strict all-target/all-feature workspace Clippy, formatting and diff checks,
  Maven `verify` (37 scenarios, fixture drift and Java/Rust cross-decodes),
  differential self-test and 37-scenario manifest, 16 deterministic race/leak
  cases repeated three times, fuzz smoke (104 frame seeds, 10 message seeds,
  1,824 mutations), release examples, offline benchmark compilation, workspace
  metadata/license/dependency checks, and seven package-content listings all
  pass. P18-V02 server-free gates pass on this revision. P18-V01, the required
  current-revision GitHub Ubuntu address-profile result, and Phase 19
  Broker/Docker validation remain open.
- Current-revision server-free gate rerun (`133d4f54`, 2026-10-15), after the
  mutable `MessageConst.STRING_HASH_SET` correction: default and all-feature
  workspace tests, strict all-target/all-feature workspace Clippy,
  format/diff checks, Maven `verify` (37 scenarios, fixture drift and
  Java/Rust cross-decodes), comparator self-test and 37-scenario manifest,
  fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), 16
  deterministic race/leak cases × 3, release examples, offline benchmark
  compilation, workspace metadata and locked Cargo metadata, and all seven
  package-content listings pass. P18-V02 server-free gates pass on this
  revision. P18-V01, the current-revision GitHub Ubuntu address-profile result,
  and Phase 19 Broker/Docker validation remain open.
- Selector message snapshot (`2026-10-15`, pinned Java 5.5.1): the invoked
  `ProducerCoreFixtures.implementationSelectorMessageSnapshot` case confirms
  that `MessageAccessor.cloneMessage` passes a distinct selector argument,
  removes the configured namespace, retains body/flag/properties, shares body
  and property references, and leaves `transactionId` null. Rust now prepares
  the same readable selector view and preserves the input message; its closure
  receives `&Message`, so reference identity and mutation cannot be observed.
  The Java/Rust fixture and focused scripted-selector regression pass. Fixture
  SHA-256: `41dd9cca0b559e68397eb2d7bd41769596417ac0d0be605b8cd586423524b1d1`.
  This closes only the selector snapshot slice; broad P18-V01, the full gate on
  this revision, current-revision GitHub Ubuntu evidence, and Phase 19 local
  Broker/Docker automation remain open.
- The public `MessageAccessor.cloneMessage` helper now maps directly to
  `Message::clone_message`. Its invoked Java `messagePublicApi` fixture and Rust
  differential check copied topic/body/flag/property values, distinct message
  identity, omitted transaction ID, and unchanged source transaction ID.
  Producer selector preparation uses this same projection. The Java fixture
  confirms shared body-array/property-map identity, bidirectional mutation
  visibility, and per-message detachment after `setBody`/`setProperties`.
  Rust `clone_message()` now shares synchronized body/property storage and the
  replacement setters detach the receiver; ordinary Rust `Clone` remains a
  deep, independent copy. Rust accessors use borrow guards or owned property
  strings to keep those alias semantics safe. This closes the direct alias
  slice only; broad P18-V01 remains open. Fixture SHA-256 at that checkpoint:
  `b9f6f527720c425b17af57367a524f65e1f16da20acfc369502d64fa37b821e7`.
  The current-revision P18-V02 rerun is recorded below; broad P18-V01, GitHub
  Ubuntu evidence, and Phase 19 gates remain open.
- Current-revision server-free P18-V02 rerun (`1aae4772`, 2026-10-15):
  `TZ=UTC cargo test --workspace --locked` and
  `cargo test --workspace --all-features --locked` pass; strict workspace
  Clippy, Maven verify (37 scenarios and Java/Rust cross-decodes), comparator
  self-test and manifest validation, race/leak smoke (16 cases × 3), fuzz
  smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release examples,
  workspace benchmark compilation, locked Cargo metadata, workspace metadata,
  all seven package listings, format and diff checks pass. The near-time
  ClientID differential passes under UTC and Asia/Taipei. This closes P18-V02
  server-free gates on this revision only; P18-V01, current-revision GitHub
  Ubuntu address-profile evidence, Phase 19 Broker/Docker validation, and
  release version/registry checks remain open.
- P18-V02 scoped follow-up (2026-10-15): after protocol DTO identity changes,
  `cargo test -p rocketmq-classic-protocol --all-features --locked` passes all
  115 unit tests, integration targets, and 25 doctests; strict Protocol Clippy,
  pinned Maven verify (37 scenarios and fixture/cross-decode checks), format,
  and diff checks pass. The complete workspace P18-V02 gate must still be
  rerun on the final P18 revision.
- P18-V02 scoped response-header follow-up (2026-10-15): after removing
  structural equality from 16 Java-identity response headers,
  `cargo test -p rocketmq-classic-protocol --all-features --locked` passes 116
  unit tests, all integration targets, and 41 doctests; strict Protocol Clippy,
  Maven verify (37 scenarios and fixture/cross-decodes), format, and diff
  checks pass. The complete workspace gate remains pending on the final P18
  revision.
- Current-revision server-free P18-V02 follow-up (`0a7c94ca`,
  `8418d163`, 2026-10-15): after reconciling the 31 modeled request headers
  with `RpcRequestHeader` equality/hash behavior and updating their call sites,
  default workspace tests under UTC and all-feature workspace tests pass.
  Strict all-target/all-feature workspace Clippy, Maven verify (37 scenarios
  and Java/Rust cross-decodes), comparator self-test and manifest validation,
  race/leak smoke (16 cases × 3), fuzz smoke (104 frame seeds, 10 message
  seeds, 1,824 mutations), release examples, workspace benchmark compilation,
  locked Cargo metadata, metadata/license/dependency checks, seven package
  listings, UTC/Asia-Taipei near-time ClientID differential, formatting, and
  diff checks pass. This closes the server-free P18-V02 gates on these commits
  only; broad P18-V01, current-revision GitHub Ubuntu evidence, P18-V04 CI
  evidence, and Phase 19 Broker/Docker scenarios remain open.

- Current-revision server-free P18-V02 run (`cfed06f6`, 2026-10-15): default
  and all-feature workspace tests, strict all-target/all-feature Clippy, pinned
  Maven `verify` (37 scenarios and fixture/cross-decodes), differential
  comparator self-test and 37-scenario manifest, deterministic race/leak smoke
  (16 cases × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824
  mutations), release examples, workspace benchmark compilation, locked Cargo
  metadata, workspace metadata/license/dependency checks, seven package
  listings, Asia/Taipei unique-ID regression, formatting, and diff checks pass.
  The direct `cloneMessage` alias behavior is covered in this run. Broad
  P18-V01, current-revision GitHub Ubuntu/P18-V04 CI evidence, Phase 19 local
  Broker/Docker automation, and pre-publication version/registry resolution
  checks remain open.

- P18-V01 Common API reverse-scan follow-up (`fcab764e`, 2026-10-15): the
  pinned Java `MessageExt` inherits `Message` methods and exposes no
  `message()`/`message_mut()` composition accessors. Rust removes those two
  extra public methods and migrates internal callers to inherited methods via
  `Deref`/`DerefMut`; compile-fail doctests guard both removed boundaries.
  Default and all-feature workspace tests, strict all-target/all-feature
  Clippy, formatting, and diff checks pass. This closes the surfaced
  `MessageExt` reverse-export discrepancy only; P18-V01 remains open for the
  complete Java/Rust source, overload/input/error/flow, inherited API, and
  reverse-export audit.

- P18-V02 fixture isolation follow-up (`21f13891`, 2026-10-15): a final
  all-features workspace rerun exposed interference between the message
  constants fixture and the message API fixture, which temporarily mutates
  Java's process-wide reserved-property set. A test mutex now serializes those
  two fixtures. The focused 19-test message suite, default and all-features
  workspace suites, strict all-target/all-feature Clippy, formatting, and diff
  checks pass on the corrected test harness.
- Current-revision server-free P18-V02 rerun (`c8bdae9d`, 2026-10-15):
  default and all-feature workspace tests, strict all-target/all-feature
  Clippy, Maven verify (37 scenarios and Java/Rust cross-decodes), comparator
  self-test and 37-scenario manifest, deterministic race/leak smoke (16 cases
  × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations),
  release examples, workspace benchmark compilation, locked Cargo metadata,
  workspace metadata/license/dependency checks, all seven package listings,
  the `Asia/Taipei` unique-ID fixture, formatting, and diff checks pass.
  P18-V02 server-free gates are complete on this revision. P18-V01, current
  GitHub Ubuntu/P18-V04 CI evidence, Phase 19 Broker/Docker automation, and
  pre-publication version/registry resolution checks remain open.
- Current-revision server-free P18-V02 rerun (`78bf82ca`, 2026-10-15), after
  the Netty frame-limit parity change: default and all-feature workspace tests,
  strict all-target/all-feature Clippy, Maven verify (37 scenarios and
  Java/Rust cross-decodes, including default/configured frame-limit probes),
  comparator self-test and 37-scenario manifest, deterministic race/leak smoke
  (16 cases × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824
  mutations), release examples, workspace benchmark compilation, locked Cargo
  metadata, workspace metadata/license/dependency checks, all seven package
  listings, unique-ID fixture tests under UTC and Asia/Taipei, formatting, and
  diff checks pass. P18-V02 server-free gates are complete on `78bf82ca`.
  Broad P18-V01, current GitHub Ubuntu/P18-V04 CI evidence, Phase 19 local
  Broker/Docker automation, and pre-publication version/registry resolution
  checks remain open.
- Common `MessageExt` source-surface sweep (2026-10-15): all public Java
  declarations, including both socket-address buffer helpers and inherited
  `Message` behavior, have Rust owner mappings and invoked fixture coverage in
  the Phase 8 matrix. `cargo test -p rocketmq-classic-common --test
  message_decode --locked message_ext_` passes all four matching fixture
  tests. No additional gap surfaced in this class inventory; this closes one
  bounded P18-V01 audit slice only.
- `MessageClientExt` decoded offset-ID correction (`e15f0417`, 2026-10-15):
  wrapping client-decoded messages now preserves Java's physical offset ID
  separately from the live `UNIQ_KEY` getter, including setter and key-clear
  fallback behavior. Workspace default/all-feature tests and the current-tree
  server-free P18-V02 gates pass; broad P18-V01 remains open.
- Protocol header helper reverse-export correction (`60a62324`, 2026-10-15):
  parser helpers and the synthetic `ProofHeader` used only by tests are no
  longer public. Remoting codec and fuzz tests use the Java-mapped
  `GetRouteInfoRequestHeader`; a compile-fail doctest guards the removed exports.
  Default/all-feature workspace tests, strict all-target/all-feature Clippy,
  formatting, and diff checks pass. This closes those header exports only;
  broad P18-V01 remains open.
- Physical message-ID decoder parity (`fa6c0ff0`, 2026-10-15): Producer, Push,
  and Pull `view_message` now use the Java-mapped Common decoder. Pinned Java
  `UtilAll.string2bytes` turns an invalid nibble into `0xFF`; scripted facade
  tests mutate the physical ID's final nibble to `G` and verify `VIEW_MESSAGE_BY_ID`
  with offset `255`, including the Producer's later unique-key fallback. The
  duplicate Core parser and Rust-only DTO/errors are removed with a compile-fail
  export check. Default/all-feature workspace tests, strict Clippy, Maven verify
  (37 scenarios/cross-decodes), comparator/manifest, race/leak (16 × 3), fuzz
  smoke (1,824 mutations), release examples, benchmark compilation, metadata,
  format, and diff checks pass on this revision. This closes only the decoder
  and view-path slice; broad P18-V01 and current-revision GitHub Ubuntu/P18-V04
  evidence remain open.

- Netty decoder frame-limit setting (`754d5050`, corrected by `a06853ba`,
  2026-10-15): Rust reads the Java `com.rocketmq.remoting.frameMaxLength`
  equivalent when creating its first connection, defaults to `16777216`, and
  applies the selected limit to inbound decoding only. The pinned Java
  `NettyEncoder` has no such limit; Rust allows outbound frames above the
  configured inbound limit while respecting the signed Java length field.
  Separate Java JVM probes verify default/configured `4096` decoder limits and
  that the Java encoder emits a larger frame; a Rust child-process loopback
  test verifies the same inbound/outbound behavior.
  PASS: `cargo test -p rocketmq-classic-remoting --locked` (84 unit, 6 codec,
  1 fuzz regression, 5 security, 86 transport tests); strict all-target/all-
  feature Remoting Clippy; Maven verify (37 scenarios, both frame-limit probes,
  and Rust-to-Java cross-decodes); format and diff checks. This closes the
  frame-limit setting slice only; broad P18-V01 remains open.

## Package graph and metadata

Current Cargo metadata reports seven product crates, all at version `5.5.1`,
all with `publish = false`, and all declaring `Apache-2.0`. Repository URL and
package descriptions are now present. The package dependency order for a
future leaf-to-facade publication is:

1. `rocketmq-classic-common`
2. `rocketmq-classic-protocol`
3. `rocketmq-classic-remoting`
4. `rocketmq-classic-core`
5. `rocketmq-classic-producer`
6. `rocketmq-classic-consumer` (depends on producer)
7. `rocketmq-classic` (facade)

Before any publication:

- [x] Set internal workspace dependency requirements to the currently aligned
  `5.5.1` package versions in dependency order while retaining local paths for
  workspace builds. This prepares crate manifests for registry publication;
  it does not prove registry availability.
- [ ] Confirm `5.5.1` remains the intended release version set and validate
  registry dependency resolution before publication.
- [x] Add package descriptions and repository metadata; verify them with
  `cargo metadata --locked`.
- [x] Review package contents and dependency license/provenance metadata.
  `cargo package --workspace --list --locked --allow-dirty` lists 7 crate
  archives with per-crate `LICENSE` and `NOTICE`; contents are crate source,
  examples/benches, tests, and synthetic TLS/fuzz fixtures. Locked Cargo
  metadata reports 208 external crates, all from crates.io registry sources
  with license metadata and no git/path overrides. The manifest license
  expressions use permissive alternatives. Internal workspace dependencies
  now pair local paths with `5.5.1` version requirements. This does not establish
  future registry resolution; confirm the version set and validate the registry
  graph before publication.
- [ ] Keep each package unpublished until all of its dependencies are available
  and its package contents have been inspected.

## Apache license files

- [x] Add the full Apache License 2.0 text at the repository root, matching
  the workspace manifests' `Apache-2.0` declaration.
- [x] Preserve the Apache RocketMQ reference `NOTICE` at the repository root
  for the Java 5.5.1 compatibility harness and its source-derived fixtures.
  Package contents and external dependency provenance are reviewed above;
  registry resolution of workspace dependencies remains release preparation.

## Final release boundary

- [ ] Keep `publish = false` until Phase 19 has passed and package/legal review is
  complete.
- [ ] Do not create or push a release tag as part of Phase 18.
- [ ] Re-run the full release checks from the final revision before publication.

- Remoting codec API reverse-export correction (`c27b60e4`, 2026-10-15):
  public frame and header encoders no longer accept caller-selected size bounds;
  public frame decoding no longer exposes a per-call bound, and Java's private
  16 MiB default is crate-private. The live connection decoder still applies
  the Java property to inbound frames only; outbound framing keeps the signed
  Java length bound. Compile-fail doctests guard the removed signatures and
  constant, and an internal unit test preserves bounded transport behavior.
  This closes the remoting codec API disposition slice only; broad P18-V01
  remains open.
- Current-revision server-free P18-V02 rerun on `c27b60e4` (2026-10-15): all
  server-free P18-V02 gates pass after the codec API change, including default
  and all-feature workspace suites, strict Clippy, Maven verify (37 scenarios
  and cross-decodes), differential self-test/manifest, race/leak smoke,
  fuzz smoke, examples, benchmark compilation, metadata/package checks,
  UTC/Asia-Taipei differential, formatting, and diff checks. P18-V01, current
  GitHub Ubuntu address-profile/P18-V04 evidence, Phase 19 local Docker/Broker
  automation, and pre-publication version/registry checks remain open.

- Current-revision P18-V02 server-free rerun (`d165ba45`, 2026-10-15):
  formatting, strict all-target/all-feature workspace Clippy, UTC workspace
  tests, all-feature workspace tests, Maven verify (37 scenarios and Java/Rust
  cross-decodes including frame-limit probes), differential self-test and
  manifest validation, deterministic race/leak smoke (16 cases × 3), fuzz
  smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release examples,
  workspace benchmark compilation, locked Cargo metadata, workspace
  metadata/license/dependency checks, seven package listings, and the Java
  unique-ID fixture under UTC and Asia/Taipei all pass. `git diff --check`
  passes. This closes P18-V02 server-free gates on `d165ba45`; P18-V01,
  current-revision GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker
  automation, and pre-publication version/registry resolution remain open.

- Current-worktree P18-V02 rerun (`95548e49`, 2026-10-15): after the live
  `clientCloseSocketIfTimeout` setting change, default and all-feature workspace
  tests, strict workspace Clippy, Maven verify (37 scenarios plus the Java
  live-config probe), differential self-test/manifest, deterministic race/leak
  smoke (16 cases × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824
  mutations), release examples, benchmark compilation, locked Cargo metadata,
  workspace metadata/license/dependency checks, seven package listings, UTC
  and Asia/Taipei unique-ID fixture runs, formatting, and `git diff --check` all
  pass. This closes P18-V02 server-free gates on `95548e49`; broad P18-V01,
  current-revision GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker
  automation, and pre-publication version/registry resolution remain open.

- P18-V01 Remoting callback executor timing slice (`9881c230`, 2026-10-15):
  `disableCallbackExecutor` is now read live for the callback-executor getter
  and callback completion dispatch. Setting a custom executor while disabled
  retains it; the getter remains hidden and dispatch runs inline until the
  setting is re-enabled. The Rust regression exercises both completion paths
  and executor identity; Maven's pinned Java live-config probe verifies the
  matching getter and retention behavior. This closes that remoting setting
  slice only; the broad P18-V01 audit remains open.
- P18-V01 Remoting GO_AWAY reconnect timing slice (`f506067d`, 2026-10-15):
  the retry decision now reads the live `enableReconnectForGoAway` value when a
  GO_AWAY response arrives, with a Rust post-start mutation regression and the
  Java `RemotingLiveConfigProbe`. The current-revision server-free gates on
  `9881c230` revalidate this slice. The Phase 3 inventory now records the live
  setting adapter; this closes the setting row only.
- PASS on `9881c230`: formatting, strict all-target/all-feature workspace
  Clippy, UTC default and all-feature workspace suites, full Remoting tests,
  Maven verify (37 scenarios and live remoting config probe), differential
  self-test/37-scenario manifest, deterministic race/leak smoke (16 cases × 3),
  fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release
  examples, benchmark compilation, workspace metadata/license/dependency
  checks, seven package listings, Asia/Taipei unique-ID differential and diff
  checks. Locked Cargo metadata reports 208 external packages, all registry
  sourced with license metadata.
- P18-V01 Remoting NameServer scan timing slice (`6ffccd1b`, 2026-10-15):
  `isScanAvailableNameSrv` now has a live atomic Rust adapter, read by `start()`
  when deciding whether to schedule reachability scans. A Rust loopback test
  and the Java `RemotingLiveConfigProbe` both change the value after client
  construction and before `start()`, then verify the scan connects. This
  closes that setting row only; scan cadence/topology and broad P18-V01 remain
  separately described in the remoting inventory.
- PASS on `6ffccd1b`: UTC default and all-feature workspace tests, strict
  all-target/all-feature Clippy, Maven verify (37 scenarios, live remoting
  config probe and Java/Rust cross-decodes), differential self-test and
  37-scenario manifest, deterministic race/leak smoke (16 cases × 3), fuzz
  smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release examples,
  benchmark compilation, locked Cargo metadata, metadata/license/dependency
  checks, seven package listings, Asia/Taipei unique-ID differential, format,
  and diff checks all pass. Full Remoting results include 85 unit, 6 codec,
  1 fuzz regression, 5 security, 90 transport, and 5 doctest cases.
- P18-V01 Remoting worker-count timing slice (`e7e5728a`, 2026-10-15):
  `clientWorkerThreads` now has a live atomic Rust adapter, read at `start()`
  when creating request workers. The Rust regression covers post-construction
  mutation, startup validation and worker count; the Java live-config probe
  mutates `NettyClientConfig` after client construction and checks the executor
  group created by `start()`. This closes that setting row only.
- PASS on `e7e5728a`: UTC default and all-feature workspace tests, strict
  all-target/all-feature workspace Clippy, Maven verify (37 scenarios, Java
  fixtures, live remoting config probe and Java/Rust cross-decodes), comparator
  self-test/37-scenario manifest, deterministic race/leak smoke (16 cases × 3),
  fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release
  examples, benchmark compilation, locked Cargo metadata, workspace
  metadata/license/dependency checks, seven package listings, Asia/Taipei
  unique-ID differential, format and diff checks all pass.
- Remaining non-manual work: complete broad bidirectional P18-V01 source/API,
  setting, input/error/control-flow, inherited API and reverse-export
  reconciliation; obtain current-revision GitHub Ubuntu address-profile
  evidence for P18-V04; and implement/run Phase 19 local Docker/Broker
  automation. Confirm the release version and registry dependency resolution
  before publication. Owner-run deployment/live Broker checks remain separate
  Phase 19 manual validation.
- P18-V01 Remoting `connectTimeoutMillis` Bootstrap timing slice (`aca955cd`,
  2026-10-15): direct connections retain the value captured by `start()`;
  each matched proxy CIDR captures the live timeout on first use and reuses
  that cached value, while later CIDRs read the newer value. Reachability scans
  use the selected path's connection timeout without imposing a second
  operation deadline; zero disables the separate connection cap. Rust tests
  cover direct start-time capture, proxy first-use/cache behavior, scan timing,
  and zero-timeout scanning. Java `RemotingLiveConfigProbe` verifies the direct
  Bootstrap and first/reused/new-CIDR proxy Bootstraps.
- PASS on `aca955cd`: Remoting all-feature tests (88 unit, 6 codec, 1 fuzz
  regression, 5 security, 93 transport, 5 doctests), strict Remoting
  all-target/all-feature Clippy, formatting, Maven `verify` (37 scenarios,
  live-config probe and cross-decodes), and `git diff --check`.
- This closes the `connectTimeoutMillis` Bootstrap timing row only. Remaining
  work includes broad bidirectional P18-V01 reconciliation (including the
  `clientChannelMaxIdleTimeSeconds` and `disableNettyWorkerGroup` timing rows),
  full workspace P18-V02 gates on the eventual final revision, current-revision
  GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  release version/registry dependency checks. Owner-run deployment/live Broker
  checks remain Phase 19 manual validation.

- P18-V01 Remoting Bootstrap-option timing (`7b00a52f`, 2026-10-15): socket
  send/receive buffers now use live adapters; direct options are captured by
  `start()`, while proxy values are captured and cached per CIDR on first use.
  Direct watermarks and the Rust frame-pool switch are also captured at start.
  Proxy connections retain default watermarks and disable the frame pool,
  matching options omitted by Java's proxy Bootstrap. Rust unit coverage and
  the Java `RemotingLiveConfigProbe` verify direct, cached-proxy, and new-CIDR
  behavior.
- PASS on `7b00a52f`: Remoting all-feature tests (91 unit, 6 codec, 1 fuzz
  regression, 5 security, 95 transport, 5 doctests), strict Remoting
  all-target/all-feature Clippy, formatting, Maven `verify` (37 scenarios,
  live-config probe and Java/Rust cross-decodes), and `git diff --check`.
  The frame-limit child test was updated to call `start()` before its internal
  connection helper; the initial failure was test setup, and the targeted
  regression passed after cleanup was added.

- P18-V01 Remoting `useTLS` setting timing (`04c219dc`, 2026-10-15): the TLS
  context remains captured at construction, while the live `useTLS` adapter is
  sampled for each new connection before handshake. Existing channels retain
  their mode; enabling TLS without a captured context leaves new connections
  plain. Rust loopback tests cover post-start mutation and absent-context
  behavior; Maven `RemotingLiveConfigProbe` verifies Java's post-start disable
  on a newly created channel.
- PASS on `04c219dc`: Remoting all-feature tests (91 unit, 6 codec, 1 fuzz
  regression, 5 security, 97 transport, 5 doctests), strict Remoting
  all-target/all-feature Clippy, `cargo fmt --all -- --check`, Maven `verify`
  (37 scenarios, live-config probe and Java/Rust cross-decodes), and
  `git diff --check`.
- This closes the Remoting `useTLS` setting-timing row only. Broad P18-V01,
  final-revision full-workspace P18-V02, current-revision GitHub Ubuntu/P18-V04,
  Phase 19 local Docker/Broker automation, and release version/registry checks
  remain open. Owner-run deployment/live Broker checks remain Phase 19 manual
  validation.

- Current-revision server-free P18-V02 rerun on code revision `04c219dc`
  (2026-10-15; documentation-only follow-up `44d69079`): UTC default and
  all-feature workspace tests, strict all-target/all-feature Clippy, Maven
  `verify` (37 scenarios, live Remoting config probe and Java/Rust cross-decodes),
  comparator self-test and 37-scenario manifest, 16 race/leak cases × 3,
  fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release
  examples, workspace benchmark compilation, full locked Cargo metadata,
  workspace metadata/license/dependency checks, seven package listings, and
  unique-ID fixture checks under UTC and Asia/Taipei all pass. Formatting and
  diff checks pass. The 208 external Cargo packages are registry-sourced and
  carry license metadata.
- This closes the current-revision P18-V02 server-free gate. Broad P18-V01,
  current-revision GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker
  automation, and release version/registry-resolution checks remain open.
  Owner-run deployment/live Broker checks remain Phase 19 manual validation.

- P18-V01 Remoting callback/channel API slice (`a74171df`, 2026-10-15): Rust
  callbacks now receive a Java-shaped `ResponseFuture` with mutable response
  state, Java error-category selection, callback-once execution and owned
  permit release. Channel event callbacks receive a `ChannelHandle` that can
  inspect, close and write through its connection; closing a pending handle
  cancels connection setup. Callback timeout metadata now follows the Java
  success and exceptional-wrapper paths, and result closures receive connect
  failures exactly once. This closes these bounded Remoting API/flow rows only.
- PASS on `a74171df`: Remoting all-feature tests (96 unit, 6 codec, 1 fuzz
  regression, 5 security, 99 transport, 5 doctests); Core all-feature tests
  (159 unit and all integration/doctests); strict workspace all-target/all-
  feature Clippy; Maven `verify` (37 scenarios, Java/Rust cross-decodes and
  live Remoting config probe); format and diff checks.
- Broad P18-V01, the final full-workspace P18-V02 rerun, current-revision GitHub
  Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and release
  version/registry-resolution checks remain open. Owner-run deployment/live
  Broker checks remain Phase 19 manual validation.

- P18-V01 Remoting NameServer live-list slice (`a5ef99ae`, 2026-10-15): Java
  returns the nullable backing list from `getNameServerAddressList()` and later
  selection/scanning observes caller mutations. Rust now preserves `None` versus
  `Some(empty)` and provides `with_name_server_address_list` for lock-scoped
  mutation of the stored list. `p18_nameserver_getter_exposes_live_nullable_list_with_lock_scope`
  checks live mutation, getter state, empty-update behavior, operation behavior,
  and replacement. Remoting all-feature tests pass (96 unit, 6 codec, 1 fuzz
  regression, 5 security, 100 transport, 5 doctests); strict Remoting Clippy,
  formatting and `git diff --check` pass. Phase 3 inventory also now reflects
  the `ResponseFuture` and `ChannelHandle` callback API slice from `a74171df`.
  This closes only the listed Remoting rows; broad P18-V01, current-revision
  full-workspace P18-V02, GitHub Ubuntu/P18-V04 evidence, Phase 19 local
  Docker/Broker automation, and release version/registry-resolution checks
  remain open. Owner-run deployment/live Broker checks remain manual.

- Current-revision server-free **P18-V02** gates on `ae08e42d` (2026-10-15):
  default and all-feature workspace tests, strict all-target/all-feature
  Clippy, pinned Maven `verify` (37 scenarios and Java/Rust cross-decodes),
  differential self-test and 37-scenario manifest, race/leak smoke (16 cases
  × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations),
  release examples, workspace bench compilation, locked Cargo metadata,
  workspace metadata/license/dependency checks, package listings, UTC default
  fixture tests and the unique-ID fixture under Asia/Taipei all pass. Rustfmt
  and `git diff --check` pass. This closes server-free P18-V02 only on
  `ae08e42d`; any later implementation revision needs another full rerun.
  Broad P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence, Phase 19
  local Docker/Broker automation, and release version/registry-resolution
  checks remain open. Owner-run deployment/live Broker checks remain manual.

- Current-tree server-free **P18-V02** rerun after the ClientConfig visibility
  slice (2026-10-15; `fc8ce548` plus the tested worktree): default and
  all-feature workspace tests, workspace check, strict all-target/all-feature
  Clippy, Maven verify (37 scenarios and Java/Rust cross-decodes), comparator
  self-test and 37-scenario manifest, deterministic race/leak smoke (16 cases
  × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations),
  release examples, workspace benchmark compilation, locked Cargo metadata,
  workspace metadata/license/dependency checks, all seven package listings,
  UTC and Asia/Taipei unique-ID tests, formatting, and diff checks pass. This
  closes server-free P18-V02 on the tested tree. Broad P18-V01, current-revision
  GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  pre-publication version/registry-resolution checks remain open. Owner-run
  deployment/live Broker checks remain manual validation.

- P18-V01 Common `MessageDecoder` overload/body-null slice (2026-10-15): the
  Java 5.5.1 fixture now invokes every generic `decode` overload and both
  `clientDecode` variants against eleven records, including an empty body.
  Rust now preserves Java's null body when body reading is disabled or the
  record body is empty, and preserves a null properties map when the record has
  no properties section. The Common crate suite passes (21 unit tests, 20
  `message_decode` integration tests, one topic-validator test, and 17 doctests),
  as do strict Common Clippy and the full offline Maven verify (37 scenarios,
  Java/Rust cross-decodes and fixture checks). This closes this decoder slice
  only; broad P18-V01 remains open. At this checkpoint, full-workspace P18-V02
  gates still needed a rerun; the rerun is recorded below. Current GitHub Ubuntu/P18-V04 evidence,
  Phase 19 local Docker/Broker automation, and release version/registry checks
  remain open. Owner-run deployment/live Broker checks remain Phase 19 manual
  validation. Current message fixture SHA-256:
  `d11f8c88e44711cffbd574beab5946943b00a8d350fe05ec40ff1b744320f321`.

- Current-revision server-free **P18-V02** gates on `f14d7c6a` (2026-10-15):
  default and all-feature workspace tests; workspace all-target check; strict
  all-target/all-feature Clippy; pinned offline Maven verify (37 scenarios,
  Java/Rust cross-decodes and fixture checks); differential self-test and
  37-scenario manifest; deterministic race/leak smoke (16 cases × 3); fuzz
  smoke (104 frame seeds, 11 message seeds, 1,840 bounded mutations); release
  examples; workspace bench compilation; locked Cargo metadata; workspace
  metadata/license/dependency checks; seven package listings; Asia/Taipei
  unique-ID fixture; rustfmt and diff checks all pass. This closes server-free
  P18-V02 on `f14d7c6a`. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04
  evidence, Phase 19 local Docker/Broker automation, and release version/
  registry-resolution checks remain open. Owner-run deployment/live Broker
  checks remain Phase 19 manual validation.

- P18-V01 Remoting private-header narrowing (`c0878056`, 2026-10-15): pinned
  Java 5.5.1 `RocketMQSerializable` writes request code and version with
  `writeShort`, retaining the low 16 bits. The Rust private-header encoder now
  matches this behavior. The new invoked Java fixture uses `987654` for both
  fields and records `4614` after decode; Rust encoding matches the complete
  Java frame byte-for-byte. Before the fix, the regression failed with
  `FieldTooWide("code")`. Remoting all-feature tests pass (96 unit, 7 codec,
  1 fuzz regression, 5 security, 100 transport and 5 doctests); strict
  all-target/all-feature Remoting Clippy, Maven verify (37 scenarios),
  formatting, diff checks and fixture regeneration comparison pass. This
  closes the private-header narrowing slice only; broad P18-V01, current
  GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation and
  release version/registry resolution remain open. Fixture SHA-256:
  `033e89778d92def6f5e1875211ea4ca7b59facaf813a72a57de8c386c5e4acf8`.

- P18-V01 Remoting private key-length narrowing (`c62e0740`, 2026-10-15):
  pinned Java 5.5.1 `RocketMQSerializable.writeStr(true, key)` stores the UTF-8
  key length with `setShort`. A 32,768-byte key therefore writes `0x8000`
  while retaining all key bytes. Rust now matches the emitted Java frame;
  before the fix its encoder returned `FieldTooWide("ext field key")`. The
  direct Java fixture lives under `fixtures/remoting/edge-cases/` because its
  signed length is negative and the case verifies encoding. Remoting
  all-feature tests pass (96 unit, 8 codec, 1 fuzz regression, 5 security,
  100 transport and 5 doctests); strict Remoting Clippy, Maven verify (37
  scenarios), formatting, diff checks and fresh-fixture comparison pass.
  This closes the private key-length slice only; broad P18-V01, current GitHub
  Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation and release
  version/registry resolution remain open. Fixture SHA-256:
  `7af64d29f396b2e7a1dd70a57066920890559fce1f170844ca8d4e1ba3a453ca`.

- P18-V01 Common formatted-string reverse map (2026-10-15): the Phase 8
  Rust-to-Java table now lists `Message`/`MessageExt` `Display` and blanket
  `ToString` as the public mapping for Java `toString()`. It no longer lists
  private `java_to_string` implementation helpers as public exports. This is
  a documentation correction only; broader inherited API and reverse-export
  reconciliation remains open.

- Current-revision server-free **P18-V02** gates on code revision `ae416072`
  (tested at HEAD `a6f3010c`, whose only later change is this run's execution
  log; 2026-10-15): UTC default and all-feature workspace tests pass when run
  serially; strict all-target/all-feature Clippy and workspace all-target
  check pass. Two concurrent full test processes initially collided on shared
  offset-store fixture paths (`OffsetStore(AlreadyOpen)`); both suites pass on
  the serialized rerun. Pinned Maven `verify` passes (37 scenarios and
  Java/Rust cross-decodes); comparator self-test and 37-scenario manifest pass;
  deterministic race/leak smoke passes (16 cases × 3); fuzz smoke passes (105
  frame seeds, 11 message seeds, 1,856 bounded mutations); release examples and
  workspace benches compile; locked metadata reports seven workspace crates
  and 208 crates.io dependencies; metadata/license/dependency checks and all
  seven package listings pass with LICENSE/NOTICE; the unique-ID fixture passes
  under UTC and Asia/Taipei; rustfmt and `git diff --check` pass. This closes
  server-free P18-V02 on the tested code. Broad P18-V01, current-revision
  GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  release version/registry-resolution checks remain open. Owner-run deployment
  and live Broker checks remain Phase 19 manual validation.

- P18-V01 `MessageClientExt` constructor API (`8b91e933`, 2026-10-15): pinned
  Java 5.5.1 reflection confirms exactly one public no-argument constructor.
  Rust `MessageClientExt::new()` and `Default` provide that construction path;
  the Rust-only `new(MessageExt)` and `From<MessageExt>` conversion are removed.
  Compile-fail doctests guard both absent conversion surfaces, and the invoked
  Java fixture checks the public constructor count/signature. The constructor
  parity slice passes Common all-feature tests (21 unit, 20 message-decode
  integration, 1 topic-validator and 20 doctests), strict Common Clippy,
  Maven verify (37 scenarios), formatting and diff checks. Fixture SHA-256:
  `cd7329a631de04393ceb4fdb7eff701ceec05728213605e9efd07eceaf184ccb`.
  This closes the constructor slice only; broad P18-V01 remains open, and the
  full-workspace P18-V02 gate must be rerun after the remaining code changes.

- P18-V01 Common public inherent-method crosswalk (`e4593202`, 2026-10-15): a
  re-export-aware Rustdoc JSON name sweep covered 26 public aggregate types.
  The Common compatibility matrices now explicitly map mutable body access,
  nullable property lookup/user-property validation, `MessageHostAddress`
  adapters, `MessageId` setters, and the Java `TopicValidator` mutable-set
  getters. The scan found no public inherent method name absent from the
  compatibility docs; it does not close trait/inherited API or input/error/
  control-flow reconciliation. Common all-feature tests (21 unit, 20
  message-decode integration, 1 topic-validator and 20 doctests), strict Common
  Clippy, formatting and diff checks pass.

- P18-V01 `MessageExt` constructor parity (`7665b769`, 2026-10-15): the pinned
  Java fixture now reflects both public constructors: no-argument and
  `(int, long, SocketAddress, long, SocketAddress, String)`. Rust `new()` and
  `Default` map to the no-argument constructor; the prior public
  `new(Message)` conversion was removed. Common retains a crate-private
  `from_message` helper for decoder internals, while Consumer/Core product paths
  construct through `new()` and inherited setters.
  A compile-fail rustdoc example protects the absent conversion API. This
  closes the constructor slice only; broad P18-V01 remains open. Fixture SHA-256:
  `9400beefa451ed1e29cdda0cb1171387aacd6929d5e2a144de3a837f6ca840e0`.

- Current-worktree server-free P18-V02 rerun at `bb93560e` (2026-10-15):
  workspace tests pass with default and all features; workspace all-target
  check and strict all-target/all-feature Clippy pass; pinned Maven `verify`
  passes all 37 scenarios and fixture drift checks. Differential self-test and
  37-scenario manifest validation pass. Offline race/leak smoke passes 16 cases
  × 3; fuzz smoke passes 105 frame seeds, 11 message seeds and 1,856 bounded
  mutations. Release examples, workspace benches, locked Cargo metadata,
  workspace metadata/license/dependency checks and package listings pass; all
  seven package contents include LICENSE/NOTICE. The Java-backed Unique ID
  fixture passes under `Asia/Taipei`; the full workspace runs used the host UTC
  timezone. Formatting and diff checks pass. This closes local server-free
  P18-V02 only. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 local Docker/Broker automation, and pre-publication version/registry
  resolution remain open. Owner-run deployment/live Broker checks remain
  Phase 19 manual validation.

- P18-V01 TLS decryptor registration (`d163a143`, 2026-10-15, pinned Java
  source `efa1be59d6fc747ad220f036c86030f05f0419f9`): Rust now exposes the
  Java-shaped process-wide `register_decryption_strategy` API; the callback
  takes a key path and client/server role, and null registration makes a later
  key load fail. Invoked Java fixtures pin the public nested interface,
  registration method, callback signature, default strategy, callback values,
  and null behavior. TLS configuration no longer exposes per-config strategy
  injection. This closes the decryptor registration API slice only. Java
  property-file reload timing and broad P18-V01 reconciliation remain open.
  TLS fixture SHA-256 values: defaults
  `1ef41b86ef30bb65c912889b091924c7c15390a460eaf0c74aef6bd10e539db1`,
  system properties
  `c0267cd68a2dd32cb6adb9b9bdc17d6340220928d2090d970a5223df9de8b5a7`.

- Current-revision server-free P18-V02 rerun on code revision `d163a143`
  (2026-10-15): default and all-feature workspace tests, all-target/all-feature
  workspace check and strict Clippy, Maven `verify` (37 scenarios and fixture
  checks), comparator self-test and 37-scenario manifest, deterministic
  race/leak smoke (16 cases × 3), fuzz smoke (105 frame seeds, 11 message
  seeds, 1,856 bounded mutations), release examples, facade offline benchmark
  compilation, locked Cargo metadata, workspace metadata/license/dependency
  checks, seven package listings with LICENSE/NOTICE, Asia/Taipei Unique ID
  fixture, formatting, and diff checks pass. This closes local server-free
  P18-V02 only. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 local Docker/Broker automation, and pre-publication version/registry
  resolution remain open. Owner-run deployment/live Broker checks remain
  Phase 19 manual validation.

- P18-V01 TLS property-file reload timing (`83afee2a`, 2026-10-15, pinned
  Java source `efa1be59d6fc747ad220f036c86030f05f0419f9`): the invoked Java
  fixture changes `tls.test.mode.enable` across three context builds and
  confirms `true → false → true`; it also confirms that an omitted
  `tls.client.authServer` property retains its previous value. Rust reloads a
  retained file source immediately before each new RemotingClient builds its
  TLS connector; an existing client keeps its constructed settings. Clones of
  one `TlsConfig` share the last-loaded values. Java's TLS values are
  process-static, while separately created Rust `TlsConfig` sources retain
  independent explicit property maps; that cross-config boundary stays in the
  broad P18-V01 audit. The Rust differential
  `java_tls_property_file_is_reloaded_for_each_client_context` matches the
  fixture. Updated TLS fixture SHA-256 values: defaults
  `c5db30257df68c1e1926a811549bb534210a4e3a70df95aed1cde3195c764528`,
  system properties
  `099916a84c78f2b5b1f4b434eec904cc556779271e43db39831ae7208f7d8951`.

- Current-revision server-free P18-V02 rerun on code revision `83afee2a`
  (2026-10-15): default and all-feature workspace tests, all-target/all-feature
  workspace check and strict Clippy, Maven `verify` (37 scenarios and fixture
  checks), comparator self-test and 37-scenario manifest, deterministic
  race/leak smoke (16 cases × 3), fuzz smoke (105 frame seeds, 11 message
  seeds, 1,856 bounded mutations), release examples, offline benchmark
  compilation, locked Cargo metadata, workspace metadata/license/dependency
  checks, seven package listings with LICENSE/NOTICE, Asia/Taipei Unique ID
  fixture, formatting, and diff checks pass. This closes local server-free
  P18-V02 only. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 local Docker/Broker automation, and pre-publication version/registry
  resolution remain open. Owner-run deployment/live Broker checks remain
  Phase 19 manual validation.

- P18-V01 TLS process-wide property state (`2d16427f`, 2026-10-15, pinned Java
  source `efa1be59d6fc747ad220f036c86030f05f0419f9`): the invoked Java fixture
  builds contexts from two successive property files; the second omits
  `tls.client.authServer` and retains `true` from the first. Rust's
  Java-compatible property sources now share retained values process-wide in
  normal builds, so distinct `TlsConfig` sources inherit omitted settings;
  existing RemotingClient instances keep their constructed connector. The
  new `tls_property_file_settings_are_process_wide_across_config_sources`
  integration test compares both property values with Java. This closes the
  TLS cross-source state slice only; broad P18-V01 remains open. TLS fixture
  SHA-256 values: defaults
  `48e1ac15883eaa795a7c3a2eefbf71752c3a9415d1f01171348e0a8b4d9d48a6`,
  system properties
  `81a4325d704f37713033dc5a6d7f6a2c8ffe5f24aa8e49bececf9ca5c33acd4a`.
- PASS on `2d16427f`: Remoting all-feature tests (97 unit, 8 codec, 1 fuzz,
  6 security, 100 transport and 7 doctests); strict all-target/all-feature
  Remoting Clippy; Maven `verify` (37 scenarios and Java fixture checks),
  formatting and diff checks.
- P18-V01 TLS static-property initialization (`e314b5e1`, 2026-10-15, pinned
  Java source `efa1be59d6fc747ad220f036c86030f05f0419f9`): the Java fixture
  initializes its TLS class state, then changes `tls.client.authServer` and
  verifies later contexts retain the original static value. Rust now seeds its
  process-wide property state only on first access, including when the first
  TLS mode selection is Plain; later maps cannot replace it, while subsequent
  file reloads still update present keys and retain omitted keys. The invoked
  Java fixture and Rust cross-source regression pass. Fixture SHA-256 values:
  defaults `4e1603df11cdb4054f8f3950ddc27cb5e219ad89f93d91071c9478d0d80ad648`,
  system properties `b4ca8ef0433b88014c28168be8bdb335ff93f2eeccae24f467ef5c72cb79c2b9`.
  Validation on the code commit: Remoting all-feature tests (97 unit, 8 codec,
  1 fuzz, 6 security, 100 transport, 7 doctests), strict all-target/all-feature
  Remoting Clippy, pinned Maven `verify` (37 scenarios), formatting and diff
  checks pass. This closes only the TLS initialization slice; broad P18-V01,
  final-revision workspace P18-V02, current-revision GitHub Ubuntu/P18-V04,
  Phase 19 local Docker/Broker automation, and pre-publication version/registry
  resolution remain open. Owner-run deployment and production checks remain
  Phase 19 manual validation.

- Current-revision server-free **P18-V02** gate on code revision `e314b5e1`
  (2026-10-15): `cargo test --workspace --locked` and
  `cargo test --workspace --all-features --locked` pass;
  `cargo check --workspace --all-targets --all-features --locked` and strict
  all-target/all-feature workspace Clippy pass; pinned Maven `verify` passes
  all 37 differential scenarios and fixtures; comparator self-test, release
  facade example build, formatting, and diff checks pass. This closes
  server-free P18-V02 on `e314b5e1`. Broad P18-V01, current-revision GitHub
  Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  pre-publication version/registry resolution remain open. Owner-run
  deployment and production checks remain Phase 19 manual validation.
- P18-V01 TLS mode selection from static properties (`746c35bc`, 2026-10-15):
  the Java oracle keeps `TlsSystemConfig.tlsEnable=false` after a later
  `tls.enable=true` system-property change, and ignores `tls.enable` in TLS
  files. Rust `TransportMode` now reads `tls.enable` from the one-time initial
  property snapshot; the default `tls.config.file` path uses that same
  snapshot. The cross-source regression reproduced the mismatch before the
  fix and passes now. Fixture SHA-256: defaults
  `223ed13d44d2a448b9c5193435a5dcb8c04940c42a9109196ad55eebeacbbc82`, system
  properties `67bab119ca6084783ee19bdca2badd2bd62fb9534a3b44382fcf16c5e918dba2`.
  Broad P18-V01 remains open.
- Current-revision server-free **P18-V02** gate on code revision `746c35bc`
  (2026-10-15): default and all-feature locked workspace tests, strict
  all-target/all-feature workspace Clippy, all-target workspace check, pinned
  Maven `verify` (37 scenarios and fixture checks), differential self-test,
  release facade example build, formatting, and diff checks pass. This closes
  server-free P18-V02 on `746c35bc`. Broad P18-V01, current-revision GitHub
  Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  pre-publication version/registry resolution remain open. Owner-run
  deployment and production checks remain Phase 19 manual validation.
- P18-V01 request/reply unregister flow (`2026-10-15`, pinned Java source
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): Java
  `MQClientInstance.unregisterProducer` removes the producer table entry and
  unregisters it with Brokers; it does not clear
  `RequestFutureHolder.getRequestFutureTable()`. Rust lease-based producer
  unregister now preserves pending correlations, matching group-based
  unregister and the Java path. Core registry and inbound regressions verify
  that an outstanding correlation accepts a late reply after unregister.
  Rust future-drop cancellation maps to Java synchronous request interruption
  and its `finally` removal; explicit cancellation maps to removing an entry
  from Java's public live correlation map. Core tests, Producer tests, strict
  Core Clippy, formatting, and diff checks pass. This closes normal unregister
  and cancellation mappings only. Rust `ProducerShutdown` on startup-failure
  registry clear remains a ClientRuntime-only path for the broad P18-V01
  reverse audit.

- P18-V01 oneway completion boundary (19989176, 2026-10-15; pinned Java
  source efa1be59d6fc747ad220f036c86030f05f0419f9): Rust invoke_oneway
  now returns only call-boundary errors while the client owns scheduled
  transmission; later connection, admission, and socket-write outcomes are not
  returned, matching Java's caller-visible boundary. The Core call sites and
  scripted transport expectations were updated, and the inventory, ADR, and
  compatibility overview now describe the same contract. Remoting tests pass
  (100 transport tests and 7 doctests); Core tests pass (159 unit tests, every
  integration target, and 13 doctests); strict all-target/all-feature Clippy
  passes for both crates, as do format and diff checks. This closes the scoped
  oneway result-surface difference only. Broad P18-V01 remains open, including
  the pre-start ClientRuntime pending-reply ProducerShutdown path. The full
  final-revision P18-V02 workspace rerun and current-revision GitHub Ubuntu
  address-profile result remain outstanding.

- P15 request/reply startup rollback follow-up (182f8f69, 2026-10-15,
  pinned Java source efa1be59d6fc747ad220f036c86030f05f0419f9): reply
  registration now requires a Running ClientRuntime, so a failed startup cannot
  leave a request to receive a Rust-only ProducerShutdown completion.
  GroupRegistry clear drops internal senders, and the public
  RequestReplyShutdown error and ProducerShutdown completion variant are
  removed. The Phase 15 matrix marks this local lifecycle slice verified; the
  full P18-V01 reverse API audit remains open. cargo test for Core and Producer
  passes (Core: 160 unit tests, all integration targets, 13 doctests; Producer:
  142 unit tests, all integration targets, 6 doctests). Strict all-target,
  all-feature Clippy for both crates, format, and diff checks pass.

- Current-revision server-free **P18-V02** rerun on code revision `182f8f69`
  (2026-10-15): the default and all-feature workspace tests, all-target
  workspace check, strict all-target/all-feature Clippy, pinned Maven verify
  (37 scenarios and Java/Rust cross-decodes), comparator self-test and
  37-scenario manifest validation pass. The 16-case race/leak smoke passes at
  three repetitions; fuzz smoke covers 105 frame seeds, 11 message seeds, and
  1,856 bounded mutations; release examples and workspace benchmarks compile.
  Locked Cargo metadata, workspace metadata/license/dependency checks, all
  seven package listings (including LICENSE and NOTICE), and the Java-backed
  Unique ID differential under `TZ=Asia/Taipei` pass. Formatting and diff
  checks pass. This closes the server-free P18-V02 gate on `182f8f69`;
  P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence, Phase 19 local
  Docker/Broker automation, and pre-publication version/registry resolution
  remain open. Owner-run deployment and production checks remain Phase 19
  manual validation.

- Current-revision server-free **P18-V02** rerun on code revision `4c142334`
  (2026-10-15): on macOS ARM64, UTC default and all-feature workspace tests,
  all-target/all-feature workspace check and strict Clippy, pinned Maven verify
  (37 scenarios and Java/Rust cross-decodes), comparator self-test and
  37-scenario manifest validation all pass. Race/leak smoke passes (16 cases ×
  3); fuzz smoke passes (105 frame seeds, 11 message seeds, 1,856 mutations);
  release examples and workspace benchmark compilation pass. Locked metadata,
  workspace metadata/license/dependency checks, seven package listings with
  LICENSE/NOTICE, the Asia/Taipei Unique ID fixture, formatting and diff checks
  pass. This closes the macOS server-free P18-V02 gate on `4c142334`. The
  Linux-only signal fallback is type-checked in an isolated target harness but
  has not been run on Linux; current-revision GitHub Ubuntu/P18-V04 evidence,
  broad P18-V01, Phase 19 Docker/Broker automation, and pre-publication
  version/registry resolution remain open. Owner-run deployment and production
  checks remain Phase 19 manual validation.

- Current-revision server-free **P18-V02** gate on `526a0b34` (2026-10-16,
  macOS ARM64): default and all-feature locked workspace tests, strict
  all-target/all-feature workspace Clippy, formatting, and diff checks pass.
  The pinned Maven `verify` passes its 37-scenario manifest, Java fixture
  probes, fixture drift checks, and Java/Rust cross-decodes. Comparator
  self-test and manifest validation pass; deterministic race/leak smoke passes
  16 cases × 3; fuzz smoke covers 105 frame seeds, 11 message seeds, and 1,856
  bounded mutations. Release examples, offline benchmark compilation, and
  workspace metadata/license/dependency checks pass. This closes server-free
  P18-V02 on `526a0b34`. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04
  evidence, Phase 19 local Docker/Broker automation, and pre-publication
  version/registry resolution remain open. Owner-run deployment and production
  checks remain Phase 19 manual validation.

- P18-V01 Remoting config validator reverse-surface follow-up (`375aeff5`,
  2026-10-16, pinned Java source
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): `ClientConfig::validate` is
  internal because Java `NettyClientConfig` has no public standalone
  validator; `RemotingClient::with_config` still validates before construction.
  A compile-fail rustdoc example protects the public boundary, and transport
  validation tests now exercise the public constructor. Remoting all-feature
  tests pass (97 unit, 8 codec, 1 fuzz regression, 7 security, 100 transport,
  8 doctests), as do strict all-target/all-feature Remoting Clippy, formatting,
  and diff checks. This closes this API mismatch only; broad P18-V01 remains
  open.

- Current-revision server-free **P18-V02** rerun on `375aeff5`
  (2026-10-16, macOS ARM64): default and all-feature locked workspace tests,
  all-target/all-feature workspace check and strict Clippy, pinned Maven
  `verify` (37 scenarios and Java/Rust cross-decodes), differential self-test
  and 37-scenario manifest validation all pass. Race/leak smoke passes (16
  cases × 3); fuzz smoke covers 105 frame seeds, 11 message seeds, and 1,856
  bounded mutations. Release examples and workspace benchmarks compile.
  Locked Cargo metadata reports 215 packages, including 208 registry packages
  with license metadata; workspace metadata/license/dependency checks and all
  seven package listings with LICENSE/NOTICE pass. The Asia/Taipei Unique ID
  fixture, formatting and diff checks pass. This closes server-free P18-V02 on
  `375aeff5`. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 Docker/Broker automation, and pre-publication version/registry
  resolution remain open. Owner-run deployment and production checks remain
  Phase 19 manual validation.

- Primary facade mapped-type availability (2026-10-16): the main
  `rocketmq-classic` crate now re-exports the phase-owned consumer/producer
  states and default groups, POP result/status, end-transaction hook API,
  `QueryResult`, Producer publish-info maps/views/adapters, and route DTO/alias
  handle. `tests/facade_public_types.rs` checks the root import paths and
  Producer state/table accessors. This closes that facade reachability slice
  only; broad P18-V01 and current-revision release gates remain open.

- Current-revision server-free **P18-V02** rerun on `fd809302`
  (2026-10-16, macOS ARM64): UTC default and all-feature locked workspace tests,
  all-target/all-feature check and strict Clippy pass. Pinned Maven `verify`
  passes the 37-scenario manifest and all Java/Rust fixture checks; comparator
  self-test and manifest validation pass. Offline race/leak smoke passes 16
  cases × 3; fuzz smoke passes 105 frame seeds, 11 message seeds, and 1,856
  mutations. Release examples and workspace benchmarks compile. Locked
  metadata reports 7 workspace packages, 215 total packages, and 208 registry
  packages with license metadata; workspace metadata/dependency/license checks
  and all seven package listings with LICENSE/NOTICE pass. The Asia/Taipei
  Unique ID fixture, formatting, and diff checks pass. This closes the
  server-free P18-V02 gate on `fd809302`. Broad P18-V01, current-revision GitHub
  Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  pre-publication version/registry resolution remain open. Owner-run deployment
  and production checks remain Phase 19 manual validation.

- P18-V01 Remoting timeout-property follow-up (`b5106ebc`, 2026-10-16): the
  pinned Java fixture now records `connectTimeoutMillis=-13` and
  `clientChannelMaxIdleTimeSeconds=-7` from their system properties. Rust
  seeds these signed values from the same process environment keys on first
  `ClientConfig::default()`, retaining the process-wide result. The isolated
  subprocess test checks these values alongside the semaphore and
  close-socket properties. Remoting all-feature tests pass (99 unit, 8 codec,
  1 fuzz regression, 7 security, 100 transport, 8 doctests); strict Remoting
  Clippy, Maven verify (37 scenarios, fixture drift, and Java/Rust
  cross-decodes), formatting, and diff checks pass. This closes these two
  property-source rows only. Worker-count, socket-buffer, and write-watermark
  property sources and broad P18-V01 remain open, as do final-revision
  full-workspace P18-V02, current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 local Docker/Broker automation, and pre-publication version/registry
  resolution. Owner-run deployment and production checks remain Phase 19
  manual validation.

- P18-V01 Remoting close-socket property-source follow-up (`280bcfe0`,
  2026-10-16): the pinned Java fixture now sets
  `com.rocketmq.remoting.client.closeSocketIfTimeout=false` and records the
  resulting `NettyClientConfig` value. Rust reads the same process environment
  key on first `ClientConfig::default()` and matches Java's case-insensitive
  `Boolean.parseBoolean` behavior. The isolated subprocess test checks this
  alongside the semaphore properties. Remoting all-feature tests pass (99
  unit, 8 codec, 1 fuzz regression, 7 security, 100 transport, 8 doctests);
  strict all-target/all-feature Remoting Clippy, Maven verify (37 scenarios,
  fixture drift, and Java/Rust cross-decodes), formatting, and diff checks pass.
  This closes this property-source slice only. Other `NettySystemConfig`
  default seeds and broad P18-V01 remain open, as do final-revision
  full-workspace P18-V02, current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 local Docker/Broker automation, and pre-publication version/registry
  resolution. Owner-run deployment and production checks remain Phase 19
  manual validation.

- P18-V01 request/reply helper behavior fixture (2026-10-16): the invoked
  Java 5.5.1 fixture now exercises `RequestResponseFuture` timeout, response,
  latch, send-status, cause and callback behavior, including its private
  exactly-once callback guard, and verifies `RequestFutureHolder` singleton/live
  table identity and mutation. The pinned commit source and Maven 5.5.1 artifact
  were used because the separate local Java checkout is at `2238256d` and has a
  newer extra method. Rust fixture assertions and Maven drift verification pass.
  This adds evidence only: Rust still lacks the public mutable future/table API,
  so P18-V01 remains open.

- P18-V01 request/reply future/table implementation follow-up (2026-10-16):
  Core now exposes mutable `RequestResponseFuture` state and a live
  `RequestFutureTable` over the same process-wide correlation entries used by
  Producer registration, inbound replies, send failures, and timeout scans.
  Producer callback requests use the future callback guard, while Producer
  startup/shutdown manages the shared three-second/one-second scanner. Scoped
  runtime timeout scans preserve registry isolation; the latch countdown is
  atomic under concurrent callers. The focused Core all-feature suite passes
  (163 unit tests and all Core integration/doc targets); Producer and facade
  all-feature suites pass (142 Producer unit tests, request/reply timeout/send
  failure/shutdown tests, facade table tests, all Producer integration/doc
  targets); strict workspace all-target/all-feature Clippy, formatting and
  diff checks pass. This closes the public mutable future/live-table request/
  reply slice only. Broad P18-V01, a full current-revision P18-V02 rerun,
  current GitHub Ubuntu/P18-V04 evidence, Phase 19 Docker/Broker automation,
  and pre-publication version/registry resolution remain open; owner-run
  deployment and production checks remain Phase 19 manual validation.

- P18-V01 request-future scheduler clock-scope follow-up (2026-10-16): each
  Producer timeout scanner is associated with its Tokio runtime ID, and each
  scan expires only entries registered on that runtime. This prevents an
  independent paused/runtime clock from expiring another runtime's future. A
  two-runtime paused-clock regression passes. The combined Core/Producer/
  facade all-feature gate passes (164 Core unit tests, 142 Producer unit tests,
  and all selected integration/doc targets); strict workspace Clippy,
  formatting, and diff checks pass. The full current-tree P18-V02 suite and
  other release gates listed above remain open.

- Current-revision server-free **P18-V02** gate on `479b9a7b` (2026-10-16,
  macOS arm64): default and all-feature workspace tests, workspace check,
  strict all-target/all-feature Clippy, Maven verify (37 scenarios and Java/Rust
  cross-decodes), comparator self-test and manifest validation, 16 deterministic
  race/leak cases × 3, fuzz smoke (105 frame seeds, 11 message seeds, 1,856
  mutations), release examples, workspace benchmark compilation, locked Cargo
  metadata/package listings, the Asia/Taipei Unique ID differential, formatting,
  and diff checks all pass. Broad P18-V01, current GitHub Ubuntu/P18-V04 evidence,
  Phase 19 Docker/Broker automation, and pre-publication version/registry
  resolution remain open. Owner-run deployment and production checks remain
  Phase 19 manual validation.

- P18-V01 Remoting signed semaphore config follow-up (`e4fd4062`, code
  `c0f5c13d`, 2026-10-16): Rust async/oneway config fields now use Java's signed
  `i32` range and accept negative values. The invoked Java fixture records
  defaults, setter/getter round trips, and timed-acquire outcomes for
  `Integer.MIN_VALUE`, its neighbor, `-2`, `-1`, zero, one, and `Integer.MAX_VALUE`.
  Rust maps ordinary negative capacities to closed admission and reproduces
  Java's `Integer.MIN_VALUE` first-acquire overflow as an effective `2^31`
  permits. `signed_semaphore_settings_match_java_fixture` checks both async and
  oneway admission, immediate saturation and positive-deadline timeout.
  Remoting all-feature tests pass (98 unit, 8 codec, 1 fuzz regression, 7
  security, 100 transport, 8 doctests); strict Remoting Clippy, formatting,
  diff checks and Maven verify (37 scenarios and fixture/cross-decode checks)
  pass. This closes the signed semaphore value/admission slice only. Java
  system-property seeding for these fields and the broad P18-V01 audit remain
  open, as do current GitHub Ubuntu/P18-V04 evidence, Phase 19 Docker/Broker
  automation, and release version/registry resolution. Owner-run deployment
  and production checks remain Phase 19 manual validation.

- P18-V01 Remoting semaphore property-source follow-up (`6497d5b4`,
  2026-10-16): the pinned Java fixture now records the async and oneway
  semaphore values when their Java system properties are set. Rust seeds
  `ClientConfig::default()` from the same process environment keys on first
  use, applies Java signed-decimal integer parsing, and retains the first
  result process-wide. An isolated subprocess test covers the fixture values
  and malformed leading-whitespace input. Remoting all-feature tests pass
  (99 unit, 8 codec, 1 fuzz regression, 7 security, 100 transport, 8 doctests),
  as do strict all-target/all-feature Remoting Clippy, formatting, diff checks,
  and Maven verify (37 scenarios, fixture drift, and Java/Rust cross-decodes).
  This closes the property-source slice for these two settings only. Broad
  P18-V01, final-revision full-workspace P18-V02, current-revision GitHub
  Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  pre-publication version/registry resolution remain open. Owner-run deployment
  and production checks remain Phase 19 manual validation.

- P18-V01 Remoting worker-thread setting/source follow-up (`85dccf7f`,
  2026-10-16): Java fixture coverage now includes the `clientWorkerThreads`
  system-property value `-3` and setter/getter round trips for
  `Integer.MIN_VALUE`, `-1`, zero, one, and `Integer.MAX_VALUE`. Rust's public
  `ClientConfig.worker_threads` and live `RemotingClient` getter/setter now use
  Java's signed `i32` domain; nonpositive values remain observable and fail
  when `start()` creates workers. The default reads the same process
  environment key on first construction with Java integer parsing. Remoting
  all-feature tests pass (99 unit, 8 codec, 1 fuzz regression, 7 security,
  100 transport, 8 doctests); strict Remoting Clippy, Maven verify (37
  scenarios, fixture drift, and Java/Rust cross-decodes), formatting, and diff
  checks pass. This closes the worker-count API/domain/property slice only.
  Socket-buffer and write-watermark property sources and signed API domains,
  broad P18-V01, final-revision full-workspace P18-V02, current-revision
  GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  pre-publication version/registry resolution remain open. Owner-run deployment
  and production checks remain Phase 19 manual validation.

- P18-V01 Remoting socket-buffer and write-watermark setting/source follow-up
  (`45ea5b54`, 2026-10-16): Java property fixtures now cover
  `com.rocketmq.remoting.socket.sndbuf.size`,
  `com.rocketmq.remoting.socket.rcvbuf.size`, and both write-watermark
  properties, plus signed setter/getter round trips at
  `Integer.MIN_VALUE`, `-1`, zero, one, and `Integer.MAX_VALUE`. Rust's
  `ClientConfig` fields and live `RemotingClient` accessors now use signed
  `i32`, seed defaults from those same process environment keys with Java
  `Integer.parseInt` behavior, preserve the values, and match Java's direct
  start-time versus proxy-CIDR first-use settings. Enabled invalid watermark
  ordering is rejected at `start()`. Remoting all-feature tests pass (102
  unit, 8 codec, 1 fuzz regression, 7 security, 100 transport, 8 doctests);
  strict all-target/all-feature Remoting Clippy, Maven verify (37 scenarios,
  fixture drift, and Java/Rust cross-decodes), formatting, and diff checks
  pass. This closes these four setting rows only. Broad P18-V01, a final-revision
  full-workspace P18-V02 rerun, current-revision GitHub Ubuntu/P18-V04
  evidence, Phase 19 local Docker/Broker automation, and pre-publication
  version/registry resolution remain open. Owner-run deployment and production
  checks remain Phase 19 manual validation.

- P18-V01 Remoting channel capability identity (`463c1460`, 2026-10-16):
  pinned Java `ResponseFuture.getChannel()` and `ChannelEventListener` channel
  callbacks are represented by Rust `ResponseFuture::channel()` and
  identity-preserving `ChannelHandle` values. Unit and loopback assertions now
  cover future identity plus shared CONNECT/ACTIVE callback identity. Corrected
  stale Java 5.5.1, Phase 3/6/16 inventory, known-difference and ADR claims that described
  the API as address-only or absent. The Remoting all-feature suite (102 unit,
  8 codec, 1 fuzz regression, 7 security, 101 transport, 8 doctests), strict
  Remoting Clippy, formatting and diff checks pass. Broad P18-V01 remains open;
  final-revision full-workspace P18-V02, GitHub Ubuntu/P18-V04 evidence, Phase
  19 Docker/Broker automation, and release version/registry checks remain
  pending.

- P18-V01 Remoting request-context slice (`eadcdb55`, 2026-10-16): pinned Java
  `NettyRequestProcessor.processRequest` and `RequestPipeline.execute` both
  receive `ChannelHandlerContext`. Rust processors and each piped pipeline stage
  now receive the same identity-preserving `RequestContext`, with peer address,
  channel close, and immediate no-deadline write enqueue. Existing address-only
  processor/pipeline implementations keep their default adapters. The loopback
  regression verifies shared handle identity, an extra context write, and the
  regular processor response; a unit regression verifies the legacy processor
  adapter. Default and all-feature workspace tests, strict all-target/all-feature
  workspace Clippy, formatting and diff checks pass on this change. This closes
  only this context slice: the broader P18-V01 bidirectional audit, complete
  Netty context surface, final P18-V02 evidence, GitHub Ubuntu/P18-V04 result,
  Phase 19 Docker/Broker matrix, and release version/registry resolution remain
  open.

- P18-V01 Remoting channel no-deadline write follow-up (`0b873da1`, 2026-10-16): Java 5.5.1 exposes immediate `Channel.writeAndFlush` through the channel supplied to remoting callbacks. Rust `ChannelHandle` now provides `write_and_flush_without_deadline`; `RequestContext::write_and_flush` delegates to it. The local result future reports socket-write completion and does not imply remote receipt. The loopback tests cover deadline and no-deadline channel writes, context channel state/close, shared identity, context-write completion, and the regular processor response. Pinned source: `NettyRequestProcessor.processRequest(ChannelHandlerContext, RemotingCommand)`, `RequestPipeline.execute(ChannelHandlerContext, RemotingCommand)`, and `NettyRemotingAbstract.writeResponse(Channel, ...)`. Focused Remoting all-feature tests, strict all-target/all-feature Remoting Clippy, formatting, and diff checks pass. This closes the no-deadline channel-write capability slice; generic Netty handler-context APIs and the broad bidirectional P18-V01 audit remain open, as do the final-revision full P18-V02 gates, GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and release version/registry checks.

- Current-revision server-free **P18-V02** rerun on source commit `4d49cd7e`
  (2026-10-16, macOS ARM64): default and all-feature locked workspace tests,
  strict all-target/all-feature workspace Clippy, format/diff checks, Maven
  `verify` (37 scenarios and Java/Rust fixture cross-decodes), comparator
  self-test and 37-scenario manifest validation, 16 race/leak cases × 3,
  fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations), release
  examples, offline benchmark compilation, locked Cargo metadata, workspace
  metadata/license/dependency checks, seven package listings with LICENSE and
  NOTICE, and the Asia/Taipei Unique ID fixture all pass. Metadata reports 7
  workspace packages, 215 packages total, and 208 registry packages with
  license metadata. This closes the server-free P18-V02 rerun on this source
  revision. Broad P18-V01 and current-revision GitHub Ubuntu/P18-V04 evidence
  remain open. The latest listed branch CI run is for older commit `cc79eb6e`
  and failed in the Linux consumer stack-capture test; it is not evidence for
  `4d49cd7e`. Phase 19 Docker/Broker automation, the owner-validation handoff,
  and pre-publication version/registry checks remain open; owner-run deployment
  and production checks remain Phase 19 manual validation.

- P18-V01 Remoting request-context API disposition (2026-10-16): pinned Java
  `NettyRemotingAbstract.buildProcessRequestHandler` reads `ctx.channel()` for
  peer metadata and responses and passes the same context into the
  `RequestPipeline` and `NettyRequestProcessor` callbacks. Maven dependency
  resolution pins Netty `netty-transport` 4.1.130.Final. Its generic
  `ChannelHandlerContext` handler metadata, event propagation, allocator,
  attributes, raw socket operations, arbitrary-object writes and promise
  factories are framework facilities; the Rust API instead maps the
  RocketMQ-level contract through shared `RequestContext`/`ChannelHandle`,
  typed command writes, `RequestExecutor`, and ordered `RequestPipeline` stages.
  Existing request-context, pipeline-order and executor-rejection tests cover
  these mappings. This closes the context framework disposition only; broad
  P18-V01 source/API/input/error/control-flow and reverse-export reconciliation
  remains open.

- P18-V01 RequestResponseFuture nullable latch slice (`310b6cd6`, fixture
  commit `3be59a09`, 2026-10-16): the pinned Java fixture confirms that a null
  latch getter returns null, `putResponseMessage` stores its response before
  throwing `NullPointerException`, and `waitResponseMessage` also throws. Rust
  uses `Option` and `RequestFutureError`; inbound reply processing leaves the
  pending table entry in place and returns `SYSTEM_ERROR` after preserving the
  response. Core's 167 unit tests, Producer request/reply's 3 tests, the facade
  request/reply's 5 tests, strict Clippy for Core/Producer/facade targets,
  formatting, diff checks, and pinned Maven `verify` (37 scenarios and fixture
  drift checks) pass. This closes this latch slice only. Broad P18-V01, a full
  P18-V02 gate rerun on this code revision, current-revision GitHub Ubuntu /
  P18-V04 evidence, Phase 19 Docker/Broker automation and scenario execution,
  `docs/owner-validation.md`, and pre-publication version/registry checks
  remain incomplete. Owner deployment and production checks remain Phase 19
  manual validation.
- P18-V01 request/reply public message type (`c6d852df`, 2026-10-16): the
  pinned Java signatures use base `Message` for Producer request results,
  `RequestCallback.onSuccess`, and `RequestResponseFuture` response methods.
  Rust now uses `Message` on those public boundaries. A later reverse-export
  review found public `From<MessageExt>` and `From<&MessageExt>` implementations
  that contradicted the documented boundary; the follow-up below removes them
  and records its scoped validation. At `c6d852df`, `cargo fmt --all -- --check`,
  strict workspace all-target/all-feature Clippy, default and all-feature
  workspace tests, and `git diff --check` passed. This closes that type slice
  only; broad P18-V01 and the full P18-V02 gate suite remain open, along with
  the Ubuntu/P18-V04 result, Phase 19 Docker/Broker automation, owner-validation
  instructions, and pre-publication version/registry checks.
- [x] **P18-V02** — Complete server-free gate rerun on revision `3b0af61d`
  (2026-10-16, macOS ARM64): default/all-feature locked workspace tests,
  strict all-target/all-feature Clippy, format/diff checks, pinned Maven verify
  (37 scenarios, fixture drift and cross-decodes), differential self-test and
  37-scenario manifest, deterministic race/leak smoke (16 cases × 3), fuzz
  smoke (105 frame seeds, 11 message seeds, 1,856 mutations), release examples,
  offline benchmark compilation, Cargo/workspace metadata and dependency/license
  checks, all seven package listings with LICENSE/NOTICE, and the
  `TZ=Asia/Taipei` Unique ID fixture all pass. Broad P18-V01 and the current
  GitHub Ubuntu/P18-V04 result remain open; this entry makes no live Broker,
  release, or compatibility claim.

- P18-V01 Push `updateCorePoolSize` slice (`8105fd3b`, Java fixture SHA-256
  `29ed1e78da3f3dfe6f0d334d78e8f97e4178537a7105419c6fd907fc0aa9a023`): Rust
  no longer exposes Java's void operation as a boolean. It ignores invalid
  facade-bound values, reports an uncreated service as `NotStarted`, maps a
  raised facade maximum exceeding the constructed executor capacity to
  `InvalidSetting`, handles concurrent and orderly services, and keeps the
  effective pool size updateable after shutdown. The pinned fixture checks the
  Java null-service failure, valid/invalid boundaries, live maximum mutation,
  executor-capacity exception, and post-shutdown paths. The final consumer
  suite passes (220 unit tests plus all integration and doc targets); strict
  all-target/all-feature Consumer Clippy, formatting, diff checks, and pinned
  Maven `verify` (37 scenarios, fixture drift and cross-decodes) pass. This
  closes this API slice only; broad P18-V01 remains open, and the complete
  workspace P18-V02 gate must run again on the final revision.

- P18-V01 request/reply table removal semantics (`aef6b86a`, `65becc27`,
  2026-10-16): the pinned Java 5.5.1 fixture confirms that removing a
  correlation from `RequestFutureHolder` leaves the future latch unsignaled
  and its own timed wait returns null. Rust now preserves the original
  `PendingReplyRequest` deadline when public table removal, replacement, or
  clear drops the lookup sender; explicit owner cancellation and registry
  clear close the receiver and map to `RequestReplyCancelled` without adding
  a public completion variant. Regressions cover removal-to-deadline behavior,
  cancellation, and the Java fixture observations. Core/Producer/facade
  all-feature tests pass (168 Core unit tests, 142 Producer unit tests, all
  scoped integration and doc targets); strict all-target/all-feature Clippy,
  formatting, `git diff --check`, and pinned Maven `verify` (37 scenarios,
  fixture drift and Java/Rust cross-decodes) pass. Fixture SHA-256:
  `b5c6e39d24e197d678fd214418f4f18b4fabb9f63ef27e74d3e152c7616ef5ef`.
  This closes the table-removal waiter slice only; broad P18-V01 and final
  workspace P18-V02 remain open.

- P18-V01 request/reply completion API reverse-surface correction
  (`2026-10-16`): the first implementation added a `Cancelled` variant to the
  public `PendingReplyCompletion` enum. The pinned Java API has no such public
  completion, so cancellation now uses private state and preserves the
  existing receiver-error mapping to `RequestReplyCancelled`; the public enum
  shape is unchanged. Core/Producer/facade all-feature tests pass (168 Core
  unit tests, 142 Producer unit tests, all scoped integration and doc targets),
  as do strict scoped all-target/all-feature Clippy, format and diff checks.
  This closes the extra-variant discrepancy only; broad P18-V01 and final
  workspace P18-V02 remain open.

- [x] **P18-V02** — Revalidated the complete server-free gate on revision
  `ac26a5ae` (2026-10-16, macOS ARM64): default and all-feature locked workspace
  tests, strict all-target/all-feature Clippy, workspace formatting, pinned
  Maven `verify` (37 scenarios, fixture drift and Java/Rust cross-decodes),
  differential self-test and 37-scenario manifest, deterministic race/leak
  smoke (16 cases × 3), fuzz smoke (105 frame seeds, 11 message seeds, 1,856
  mutations), release examples, offline benchmark compilation, locked Cargo
  metadata, workspace metadata/license/dependency checks, seven package
  listings with LICENSE/NOTICE, and the `TZ=Asia/Taipei` Unique ID fixture all
  pass. P18-V01, current GitHub Ubuntu/P18-V04 evidence, Phase 19 automation,
  and release preflight remain open; this makes no live Broker, release, or
  compatibility claim.

- P18-V01 Common `MessageUtils` slice (2026-10-16): the four pinned Java
  public methods now map to Rust Common helpers. Invoked Java fixture coverage
  and the Rust differential cover Murmur3 sharding indexes, message-property
  fallback, index deduplication, and Java encoded-property deletion. Fixture
  SHA-256: `e01bed78a2a2290a82991fd68d308d77c283475e82b32d32d3939d045099f5f8`.
  Default/all-feature locked workspace tests, strict all-target/all-feature
  Clippy, formatting, diff checks, and pinned Maven `verify` (37 scenarios and
  the new fixture) pass on the current worktree. This closes that API slice
  only; broad P18-V01 and current-revision GitHub Ubuntu/P18-V04 evidence remain
  open.

- [x] **P18-V02** — Revalidated the full server-free gate on the worktree
  based on `12db9def` with the Common `MessageUtils` parity update. Default and
  all-feature locked workspace tests, strict all-target/all-feature Clippy,
  formatting and diff checks, Maven `verify` (37 scenarios and fixture
  cross-decodes), differential self-test and manifest, deterministic race/leak
  smoke (16 cases × 3), fuzz smoke (105 frame seeds, 11 message seeds, 1,856
  mutations), release examples, offline benchmark compilation, workspace
  metadata/license/dependency checks, seven package listings, and the
  Asia/Taipei Unique ID fixture all pass. Broad P18-V01 and current-revision
  GitHub Ubuntu/P18-V04 evidence remain open; this makes no Broker, release, or
  compatibility claim.

- P18-V01 Common `CorrelationIdUtil` slice (2026-10-16): Rust Common now exports
  `utils::create_correlation_id`, Producer request preparation uses it, and the
  Producer no longer directly depends on `rand`. The invoked Java 5.5.1
  request/reply fixture records `java.lang.String createCorrelationId()`;
  the Common integration test checks the signature and lowercase UUID v4 text
  format. Common and Producer focused tests, Maven fixture generation, and
  format/diff checks pass. This closes this API slice only; broad P18-V01 and a
  full P18-V02 rerun on the resulting commit remain open.

- P18-V01 Common `NameServerAddressUtils` slice (2026-10-16): Common now
  exposes all five Java public values and four methods, with direct Java fixture
  inventory and Rust input/output checks. Core ClientConfig shares the Common
  matcher and extraction implementation. Common fixture tests, all Core
  configuration and library tests, and Java fixture generation pass. This
  closes this utility slice only; broad P18-V01 remains open.

- [x] **P18-V02** — Revalidated the complete server-free gate on revision
  `787157a0` (2026-10-16, macOS ARM64), after the Common API additions and
  lifecycle/thread-helper audit. Default and all-feature locked workspace
  tests, strict all-target/all-feature Clippy, formatting, pinned Maven
  `verify` (37 scenarios, fixture drift and Java/Rust cross-decodes),
  differential self-test and 37-scenario manifest, race/leak smoke (16 cases
  × 3), fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations),
  release examples, offline workspace benchmark compilation, locked Cargo
  metadata, workspace metadata/license/dependency checks, seven package
  listings with LICENSE/NOTICE, and the `TZ=Asia/Taipei` Unique ID fixture all
  pass. Broad P18-V01, current GitHub Ubuntu/P18-V04 evidence, Phase 19
  Broker/Docker automation, and release preflight remain open.

- P18-V01 platform cross-target continuation (2026-10-16): the production Core
  target passes Windows GNU all-target/all-feature `cargo check`, strict
  Windows-target Clippy, and `cargo test --no-run` with all Core test binaries
  linked. A temporary crate including the actual local-address module passes
  FreeBSD-target check and strict Clippy. The full Core FreeBSD check lacks a
  FreeBSD C sysroot, and neither Windows nor BSD binaries ran. Windows runtime
  enumeration, non-macOS BSD runtime behavior, and a real IPv6-only host remain
  open P18-V01 evidence; broad API/source/input/flow reconciliation remains
  open as well.

- P18-V01 Common `Message`/`MessageAccessor` declared-surface slice (2026-10-16):
  the pinned Java fixture now records all 5 public `Message` constructors, 34
  declared public `Message` methods, and 20 declared public `MessageAccessor`
  methods; Rust pins those signature lists and differentially checks
  `MessageAccessor.setLiteTopic` through `PROPERTY_LITE_TOPIC`. Fixture SHA-256:
  `d11f8c88e44711cffbd574beab5946943b00a8d350fe05ec40ff1b744320f321`.
  Pinned offline Maven `verify` (37 scenarios), the focused Common fixture test,
  Common all-target/all-feature strict Clippy, formatting, and diff checks pass.
  This closes only this API inventory slice. Broad P18-V01, a full P18-V02 run
  on the eventual final revision, current-revision GitHub Ubuntu/P18-V04
  evidence, Phase 19 local Docker/Broker automation, and release
  version/registry checks remain open. Owner-run deployment and live Broker
  checks remain Phase 19 manual validation.

- P18-V01 Common `MessageExt` reverse-export correction (2026-10-16): the
  pinned Java class inherits `Message`; Rust already models that access through
  `Deref`, but public `From<MessageExt>` and `From<&MessageExt>` implementations
  also exposed lossy conversions to the base type. Both conversions are removed,
  Core clones the inherited `Message` for its internal response-future boundary,
  and compile-fail doctests guard the absent conversions. Common all-feature
  tests pass (23 unit tests, all integration targets, 24 doctests); Core
  all-feature tests and strict Common/Core all-target Clippy also pass, as do
  formatting and diff checks. This closes only this reverse-export discrepancy;
  broad P18-V01, final-revision P18-V02, current-revision GitHub Ubuntu/P18-V04,
  Phase 19 local Docker/Broker automation, and release preflight remain open.

- P18-V01 Common inherited `Object.equals/hashCode` parity (2026-10-16): Java
  `Message`, `MessageExt`, and `MessageClientExt` inherit identity equality and
  stable identity hash codes from `Object`. Rust now uses a shared hidden
  identity token for `Message`/`MessageExt`/`MessageClientExt` `PartialEq`/`Eq`/
  `Hash`, including comparisons through base/subclass views. Rust `Clone`
  preserves its handle token; `Message::clone_message`, decode, and
  deserialization create new identities. Mutation leaves the hash stable. The
  pinned fixture records the declaring class, self/distinct equality, hash
  stability, and inherited-view comparisons; Rust covers the same behavior and
  checks clone token preservation, distinct `clone_message`/decoded/deserialized
  identities, and identity-hash stability across mutation. Fixture SHA-256:
  `5158dad83a14f0de57ba842134466e82331716f2c171b963e31ae227d9d66890`.
  Maven `verify` (37 scenarios), Common all-feature tests (23 unit, 21 message
  decode, 6 other integration, 24 doctests), Core all-feature tests (168 unit,
  all integration targets, 13 doctests), strict Common/Core Clippy, formatting,
  and diff checks pass. This closes only this inherited trait slice; broad
  P18-V01 and final-revision P18-V02 remain open.

- P18-V01 Common `MessageExt`/`MessageClientExt` declared method inventory
  (2026-10-16): the pinned Java fixture now records all 44 declared public
  `MessageExt` methods and all four declared public `MessageClientExt` methods.
  Rust's Common fixture tests pin both sorted signature lists alongside the
  constructor, scalar, host-buffer, and client-ID behavior checks. Fixture
  SHA-256: `2118277b7aad68e1077d92371d5ec14bd8cb77535e272dca406bc3e716d85792`.
  Maven `verify` passes all 37 scenarios. Common all-feature tests pass (23
  unit tests, 21 `message_decode`, six other integration, 24 doctests), as do
  strict Common Clippy, formatting, checksum, and diff checks. This closes the
  method inventory slice only; broad P18-V01 remains open.

- P18-V01 identity-handle follow-up (2026-10-16): the first default workspace
  rerun after removing `From<MessageExt> for Message` found a stale Producer
  `.into()` call; after that was fixed, Consumer filter-hook coverage showed
  that Rust `Clone` must preserve the message identity token when it snapshots
  the same Java object. Producer now clones through `Deref`, ordinary `Clone`
  preserves the token, and `Message::clone_message` creates a distinct object.
  Producer all-feature tests/strict Clippy and the focused Common/Consumer
  regressions pass; the full workspace rerun is pending.

- P18-V02 identity-handle final-gate follow-up (2026-10-16; worktree based on
  `0f1435db`): the default and all-feature locked workspace tests, strict
  workspace Clippy, formatting, and diff checks pass after the Producer clone
  correction. Offline Maven `verify` passes all 37 scenarios and message fixture
  drift checks. The differential self-test and 37-scenario manifest pass; the
  deterministic race/leak smoke passes 16 cases × 3, and fuzz smoke passes 105
  frame seeds, 11 message seeds, and 1,856 mutations. Release examples and the
  offline benchmark compile. Workspace metadata reports seven members and 215
  total packages; metadata/license/dependency checks and seven package-content
  listings pass. The Asia/Taipei Unique ID fixture passes. This closes the
  server-free P18-V02 gates on this worktree only; broad P18-V01, current-revision
  GitHub Ubuntu/P18-V04 evidence, Phase 19 Docker/Broker automation and scenario
  execution, owner-validation instructions, and pre-publication version/registry
  checks remain open. Owner-run deployment and live Broker checks remain Phase
  19 manual validation.

- P18-V01 Common `MessageDecoder` declared-surface inventory (`9004bcc9`,
  2026-10-16): the pinned Java 5.5.1 fixture now records its public no-argument
  constructor and all 26 declared public method signatures. The Common
  differential pins both sorted lists alongside the existing constant checks;
  the Phase 8 matrix maps each method family to its Rust owner and behavior
  evidence. Fixture SHA-256:
  `6cead986404276aa6ff3f11e73c04ba69d936619160351703e3e782205a6a73d`.
  Common `message_decode` tests pass (21 tests), strict all-target/all-feature
  Common Clippy passes, and pinned Maven `verify` passes all 37 scenarios and
  fixture drift checks. Formatting, checksum, and diff checks pass. This closes
  the declaration-inventory slice only; broad P18-V01 remains open.

- P18-V01 Common `MessageId` declared-surface inventory (`cc961818`,
  2026-10-16): the pinned Java 5.5.1 fixture now records its public
  `MessageId(SocketAddress, long)` constructor and all four declared public
  methods. Rust's `MessageId::new`, address/offset getters, and setters map to
  those signatures; the fixture test pins the exact sorted lists. Fixture
  SHA-256: `c2591c4d8f477becd4a96150df971da9ab09dcd6458be83ce0e9cc3c33759bc8`.
  Common `message_decode` tests pass (21 tests), strict all-target/all-feature
  Common Clippy passes, and pinned Maven `verify` passes all 37 scenarios and
  fixture drift checks. Formatting, checksum, and diff checks pass. This closes
  only the MessageId declaration-inventory slice; broad P18-V01 remains open.

- P18-V01 Common `MessageVersion` declared-surface inventory (`84462565`,
  2026-10-16): the pinned Java 5.5.1 fixture now records all eight declared
  public methods and confirms the enum has no public constructors. The Rust
  fixture test pins the sorted signatures alongside the existing enum values,
  comparison, lookup, byte-width, and error-boundary cases. Fixture SHA-256:
  `ddef0ea5e007f41e8e03c983081de27fe53de2df2d369dfefb98dab775a19516`.
  Common `message_decode` tests pass (21 tests), strict all-target/all-feature
  Common Clippy passes, and pinned Maven `verify` passes all 37 scenarios and
  fixture drift checks. Formatting, checksum, and diff checks pass. This closes
  only the MessageVersion declaration-inventory slice; broad P18-V01 remains
  open.

- P18-V01 Common `MessageSysFlag` declared-surface inventory (`1e6b07bd`,
  2026-10-16): the pinned Java fixture records all 14 public integer constants,
  the no-argument utility constructor, and all five declared public methods.
  Rust's `sysflag::message` constants and functions match the listed values and
  method behavior; the stateless Java constructor is represented by this Rust
  namespace rather than a constructible object. Common `message_decode` tests
  pass (22 tests), strict all-target/all-feature Common Clippy passes, and
  pinned Maven `verify` passes all 37 scenarios and fixture drift checks.
  Fixture SHA-256: `1b9590233baae11bb8705795bc357c84b12fa8cf2be978d69267d2f4e2fc1fe2`.
  Formatting, checksum, and diff checks pass. This closes only this utility
  declaration slice; broad P18-V01 remains open.

- P18-V01 Common `PullSysFlag` declared-surface inventory (`d0a36f9e`,
  2026-10-16): the pinned Java fixture records its empty public-constant map,
  public no-argument constructor, and all 10 declared public methods. Rust pins
  these signatures and checks the two flag builders, predicates, clearers, and
  subscription builder through `sysflag::pull`; the stateless Java utility
  constructor is represented by that Rust namespace. Common `message_decode`
  passes (23 tests), strict all-target/all-feature Common Clippy passes, and
  pinned Maven `verify` passes all 37 scenarios, fixture drift, and Java/Rust
  cross-decodes. Formatting, checksum, and diff checks pass. Fixture SHA-256:
  `be0af6c701fbebef014ba1724670d126d5ec70ff73060c990d934e1721d7a3b9`. This
  closes only the PullSysFlag declaration slice; broad P18-V01 remains open.

- P18-V01 Common `MessageConst` declared-surface inventory (`337160d5`,
  2026-10-16): the pinned Java fixture now records all 77 public fields (76
  `String` fields and `HashSet<String> STRING_HASH_SET`), the public
  no-argument constructor, and zero declared public methods. Rust pins the
  field signatures, constructor disposition, method list, all 76 constant
  values, and the initial reserved-property names; the stateless constructor is
  represented by the `message::constants` namespace. Common `message_decode`
  passes (23 tests), strict all-target/all-feature Common Clippy passes, and
  pinned Maven `verify` passes all 37 scenarios, fixture drift, and Java/Rust
  cross-decodes. Formatting, checksum, and diff checks pass. Fixture SHA-256:
  `59309d6426aa55b398addea84e0e1fdab886bc8daa3388b32c4e27005eae07c5`. This
  closes only the MessageConst declaration slice; broad P18-V01 remains open.

- P18-V01 Common `MessageQueue` declared-surface inventory (`d16f7f19`,
  2026-10-16): the pinned Java fixture records all three public constructors,
  all 11 declared public methods (including the compiler bridge
  `compareTo(Object)`), and no public fields. Rust pins these declarations and
  maps the stateless construction/copy/comparison forms to `Default`, `Clone`,
  and `java_compare_to`; existing route differentials cover method behavior.
  Common `message_decode` passes (24 tests), strict all-target/all-feature
  Common Clippy passes, and pinned Maven `verify` passes all 37 scenarios,
  fixture drift, and Java/Rust cross-decodes. Formatting and diff checks pass.
  Fixture SHA-256: `85feb19197afb046354bb4c88aed4ad1008dcfe235f7bf781817ffa7eb1edd2f`.
  This closes only the MessageQueue declaration slice; broad P18-V01 remains
  open.

- P18-V01 Producer Common `MessageClientIDSetter` declared-surface inventory
  (`9329f424`, 2026-10-16): the pinned Java fixture records its public
  no-argument constructor, no public fields, and all eight declared/callable
  public methods. The Rust test pins the signatures while retaining the
  existing IPv4/IPv6, PID, near-time, invalid-hex and fake-IP differentials;
  the stateless Java constructor maps to the `unique_id` namespace. Core's 168
  library tests pass, strict all-target/all-feature Core Clippy passes, and
  pinned Maven `verify` passes all 37 scenarios and fixture drift/cross-decodes.
  Formatting and diff checks pass. `fixtures/producer/core.json` SHA-256:
  `af16e63df4a3506daaf22162afdf7b52655861bbf0cffea072983a4dacfa25b6`. This
  closes only the MessageClientIDSetter declaration slice; broad P18-V01 remains
  open.

- P18-V01 Producer `MessageBatch` declared-surface inventory and implementation
  (`28253d80`, 2026-10-16): the pinned Java 5.5.1 fixture records the public
  `List<Message>` constructor and all three declared methods. Rust exposes
  `MessageBatch::{new,generate_from_list,encode}`, inherited `Message` access,
  and `IntoIterator`; Java-compatible validation and encoded record bytes are
  covered by Producer tests and a facade import test. Fixture SHA-256:
  `f8acdeae600bef3a56250c4f07c144860c6c22fbd9f84fe8bfe14d930a497b8d`.
  Producer/facade tests, strict crate Clippy, and Maven `verify` (37 scenarios,
  fixture drift, and Java/Rust cross-decodes) pass.
- The server-free P18-V02 gate passes on `28253d80`: default and all-feature
  workspace tests, all-target workspace check and strict Clippy, comparator
  self-test and 37-scenario manifest validation, 16 race/leak cases × 3,
  bounded fuzz smoke, release-example and benchmark compilation, locked
  metadata and package-content checks, and the Asia/Taipei Unique ID check all
  pass. Maven `verify`, formatting, and diff checks pass. This is server-free
  evidence only; it does not close broad P18-V01, current-revision Ubuntu CI /
  P18-V04, Phase 19 Docker/Broker automation, or pre-publication registry
  resolution. Owner deployment and production checks remain manual.

- P18-V01 Producer `MessageBatch` nullability/validation follow-up
  (`8f94b206`, 2026-10-16): pinned Java 5.5.1 fixtures now invoke null and empty
  collections, null elements, and all four delay properties, recording exact
  exception classes/messages. Rust's nullable adapters preserve constructor
  failure timing and iterator null elements; batch generation matches the
  delay, retry-topic, topic and wait-store rejection messages. Producer tests,
  facade import test, strict Producer Clippy, and pinned Maven `verify` pass.
  Fixture SHA-256: `3d8ae4a75e76e1c2636de3633dbf2f04c61c8058076e2c639d63fec17fc5c88d`.
- Current-revision server-free P18-V02 rerun (`8f94b206`, 2026-10-16): default
  and all-feature workspace tests, all-target workspace check and strict
  Clippy, Maven `verify` (37 scenarios and fixture/cross-decode checks),
  differential self-test and 37-scenario manifest, 16 deterministic race/leak
  cases × 3, fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations),
  release examples, workspace benchmark compilation, locked metadata and
  package-content checks, and the Asia/Taipei Unique ID differential pass.
  Broad P18-V01 remains open; current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 Docker/Broker automation and scenario execution, and pre-publication
  version/registry resolution remain outstanding.
- P18-V01 Producer collection-send mutation follow-up (`3624d0b8`, 2026-10-16):
  the pinned Java 5.5.1 oracle confirms that `send(Collection<Message>)` assigns
  missing unique IDs and namespaces each caller message before downstream
  dispatch. Rust `send_batch*` now accepts mutable message slices and preserves
  Java's structural precheck and per-element validation mutation timing. The
  fixture covers dispatch failure after preparation, mixed-topic rejection
  before mutation, and a later element validation failure after an earlier
  element was mutated. Producer all-feature tests (146 unit, 65 integration,
  and 6 doc tests), strict Producer Clippy, all-target/all-feature workspace
  check, Maven `verify` (37 scenarios and fixture/cross-decodes), formatting,
  and diff checks pass. Current Producer core fixture SHA-256:
  `fd92f9e88ab615288cfc5b25b8ed594c000fc2b3c07b9952d6bc0fd790cdbde4`. This
  closes the collection-send mutation slice only; broad P18-V01 and the final
  full-workspace P18-V02 rerun remain open.
- P18-V01 Producer batch error/order follow-up (`c4251219`, 2026-10-16): Java
  wraps batch-construction failures with `Failed to initiate the MessageBatch`;
  Rust now preserves that message and exposes the typed source error. Batch
  encoding no longer applies the aggregate-size check before producer-state
  validation, matching Java's call order. The invoked fixture covers wrapper
  messages, per-element partial mutation, and an unstarted oversized batch.
  Producer all-feature tests (146 unit, 65 integration, and 6 doc tests), strict
  Producer Clippy, all-target/all-feature workspace check, the pinned Producer
  core fixture verification, formatting, and diff checks pass. Current Producer
  core fixture SHA-256: `bfa2d5f8f411c4648670d024e44b0e637ab793873a9eeb0c7ff75bb2a82651c5`.
  This closes only these collection-send error/order slices; broad P18-V01 and
  the final full-workspace P18-V02 rerun remain open.
- P18-V01 Producer auto-batch aggregate-size ordering follow-up (2026-10-16):
  explicit and auto-batch sends now share the batch encoder and apply aggregate
  message-size validation after Producer-state validation, before Broker I/O.
  Producer all-feature tests (147 unit, 65 integration, and 6 doc tests),
  strict all-target/all-feature workspace Clippy, all-target/all-feature
  workspace check, formatting, and diff checks pass. The final full-workspace
  P18-V02 rerun remains open.
- Current-revision server-free P18-V02 rerun (`00254e6d`, 2026-10-16): default
  and all-feature workspace tests, all-target/all-feature workspace check and
  strict Clippy, Maven `verify` (37 scenarios, fixture drift checks and
  Java/Rust cross-decodes), comparator self-test and 37-scenario manifest,
  16 deterministic race/leak cases × 3, fuzz smoke (105 frame seeds, 11
  message seeds, 1,856 mutations), release example build, offline benchmark
  compilation, workspace metadata/license/dependency checks, seven package
  listings, the Asia/Taipei Unique ID differential, formatting, and diff checks
  pass. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence, Phase 19
  Docker/Broker runner and scenario execution, and pre-publication
  dependency/version resolution remain outstanding.
- P18-V01 Common `MessageAccessor` reverse-surface slice (2026-10-16):
  `MessageAccessor`'s 20 public methods now have an explicit Java-to-Rust
  crosswalk to `Message`, nullable property operations, message property
  constants, and Rust `Clone`. The existing invoked Java fixture pins every
  declared signature and its Rust differential covers helper values, nullable
  maps, deep-copy independence, and `cloneMessage` aliasing. The focused Common
  API test passes; broad Common/P18-V01 reconciliation remains open.
- P17 native Windows stack runtime CI path (2026-10-16): `.github/workflows/ci.yml`
  now runs `jstack_includes_current_and_other_named_threads` on
  `windows-latest` and fails if the exact test does not report one pass. The
  local host cannot execute Windows binaries; the workflow result remains
  pending a run on the current revision.
- P18-V01 Common `MessageAccessor.setProperties` caller-map alias follow-up
  (2026-10-16, pinned Java client source `efa1be59d6fc747ad220f036c86030f05f0419f9`):
  Java retains the supplied `Map` object and exposes mutations through both the
  caller and `Message`. Rust now provides `SharedMessageProperties`,
  `Message::set_properties_shared`, and `Message::shared_properties`, and the
  primary facade re-exports the shared handle. The invoked fixture checks
  identity and bidirectional mutation; Common and facade regressions exercise
  the mapped behavior. Fixture SHA-256:
  `02be176d528051f874a159a7fc2867484a8e1c2f8a7cffcf92c588ec8853db91`.
  Focused Common API and facade tests, all Common tests/Clippy, pinned Maven
  verify (37 scenarios, fixture checks, and Java/Rust cross-decodes), format,
  and diff checks pass. Broader P18-V01 reconciliation remains open.
- Current-revision server-free P18-V02 rerun (`830969e8`, 2026-10-16): default
  and all-feature workspace tests, all-target/all-feature check and strict
  Clippy, Maven `verify` (37 scenarios, fixture checks, Java/Rust cross-decodes),
  comparator self-test and 37-scenario manifest, 16 deterministic race/leak
  cases × 3, fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations,
  146 frame decodes, 113 message decodes), release examples, benchmark
  compilation, locked metadata/package checks, all seven package listings,
  UTC and Asia/Taipei Unique ID tests, formatting, and diff checks pass. Broad
  P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence, and Phase 19
  Docker/Broker automation and scenario execution remain open.
- P18-V01 Common `Message.setBody(byte[])` caller-array alias follow-up
  (`d3532c6e`, 2026-10-16; pinned Java source
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): the invoked fixture verifies
  retained array identity, bidirectional mutation visibility, and detachment
  after replacement. Rust exposes `SharedMessageBody` with lock-scoped mutable
  bytes, `Message::set_body_shared`, and `Message::shared_body`; the facade
  re-exports the handle. Fixture SHA-256:
  `efb8b02c5eb9fec2e78459ec63e97144fdf633d964bd4a2d6e276fc60b13e39d`.
- Current-revision server-free P18-V02 rerun (`d3532c6e`, 2026-10-16): default
  and all-feature workspace tests, all-target/all-feature check and strict
  Clippy, pinned Maven `verify` (37 scenarios and Java/Rust cross-decodes),
  comparator self-test and 37-scenario manifest, deterministic race/leak smoke
  (16 cases × 3), fuzz smoke (105 frame seeds, 11 message seeds, 1,856
  mutations), release examples, workspace benchmark compilation, locked
  metadata and seven package listings, Asia/Taipei Unique ID tests (4),
  formatting, and diff checks pass. Broad P18-V01, current-revision GitHub
  Ubuntu/P18-V04 evidence, native Windows runtime CI result, Phase 19
  Docker/Broker automation, `docs/owner-validation.md`, and pre-publication
  version/dependency resolution remain open.
- P18-V01 Common `Message` constructor body-array aliases (`b03d7fe9`, 2026-10-16;
  pinned Java source `efa1be59d6fc747ad220f036c86030f05f0419f9`): the invoked
  fixture verifies the two-argument, tags, tags-and-keys, and six-argument
  constructors retain their caller's exact array, including mutation after
  construction. Rust now exposes shared-body variants for each constructor
  shape and nullable fields. Fixture SHA-256:
  `0346a246d0f22440ed71773ac1854ad510acf857954d85f7ee9b8a451afc6471`.
- Current-revision server-free P18-V02 rerun (`b03d7fe9`, 2026-10-16): default
  and all-feature workspace tests, all-target/all-feature check and strict
  Clippy, pinned Maven `verify` (37 scenarios and updated fixture/cross-decodes),
  comparator self-test and 37-scenario manifest, deterministic race/leak smoke
  (16 cases × 3), fuzz smoke (105 frame seeds, 11 message seeds, 1,856
  mutations), release examples, workspace benchmark compilation, locked
  metadata and seven package listings, Asia/Taipei Unique ID tests (4),
  formatting and diff checks pass. The first default workspace run exposed a
  flaky paused-time test; `f04c4384` makes its three-second initial delay and
  one-second scan period deterministic, and the corrected test passes in the
  repeated smoke and full workspace suites. Broad P18-V01, current-revision
  Ubuntu/Windows CI evidence, Phase 19 Docker/Broker automation,
  `docs/owner-validation.md`, and pre-publication version/dependency resolution
  remain open.
- P18-V01 Remoting request-context capability disposition (`b03d7fe9`,
  2026-10-16, pinned Java source `efa1be59d6fc747ad220f036c86030f05f0419f9`):
  `NettyRequestProcessor` declares `processRequest(ChannelHandlerContext,
  RemotingCommand)` and default `rejectRequest()`. Rust maps those operations
  to the shared identity-preserving `RequestContext` and `reject_request()`;
  generic allocator, handler propagation, arbitrary-object write, promise, and
  raw Netty APIs remain framework-specific under the Rust/Tokio API contract.
  The Phase 3 source/call-site mapping and Common API map now record this
  disposition. Remoting all-feature `p18_` tests pass (11), including context
  identity and no-deadline channel writes; processor executor/rejection and
  pipeline-order regressions pass (1 each). This closes the Netty context
  capability question only; processor error-remark formatting is accepted as
  diagnostic-only under the core flow-parity rule. Broad P18-V01 reconciliation
  remains open.
- P18-V01 Common shared-alias reverse-export disposition (`b03d7fe9`,
  2026-10-16, pinned Java source `efa1be59d6fc747ad220f036c86030f05f0419f9`):
  the Phase 8 method matrix now records all public `SharedMessageBody` and
  `SharedMessageProperties` operations, the shared `Message` constructors and
  accessors, and their mapping to Java array/Map alias behavior. These are
  synchronized Rust ownership handles for existing Java capabilities, not
  additional RocketMQ operations. The Common fixture differential and facade
  public-type test pass (1 each); fixture SHA-256:
  `0346a246d0f22440ed71773ac1854ad510acf857954d85f7ee9b8a451afc6471`.
  This closes the named alias-handle reverse-export slice only; broad P18-V01
  reconciliation remains open.
- P18-V01 Remoting GO_AWAY post-swap retry follow-up (`bc970e10`, 2026-10-16,
  pinned Java
  client source `efa1be59d6fc747ad220f036c86030f05f0419f9`):
  `NettyRemotingClient.invokeImpl` looks up the wrapper by the responding old
  channel; after `ChannelWrapper.reconnect` swaps its current future, a later
  GO_AWAY from that old channel sees the wrapper's different current channel
  and retries through it. Rust now retries that retired generation through the
  shared replacement. The regression receives the first replacement retry
  before sending the second old-channel GO_AWAY, then verifies both requests
  complete through that one replacement. Java's `tryLock` can still fail for
  a callback racing before the current-channel swap; this test does not claim
  that every simultaneous ordering retries. Remoting all-features tests pass
  (104 unit, 8 codec, 1 fuzz regression, 7 security, 102 transport, 8 doctests),
  as do strict Remoting Clippy, formatting, and diff checks. This closes the
  deterministic post-swap behavior slice only; broad P18-V01 and final-revision
  full-workspace P18-V02 remain open.
- Current-revision server-free P18-V02 rerun (code `bc970e10`; evidence tree
  `464fad73`, 2026-10-16): default and all-features locked workspace tests,
  all-target/all-feature locked check and strict Clippy, pinned Maven `verify`
  (37 scenarios, Java fixture checks and Java/Rust cross-decodes), comparator
  self-test and 37-scenario manifest, offline race/leak smoke (16 cases × 3),
  and fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations, 146 frame
  decodes, 113 message decodes) pass. Release examples and workspace benches
  compile; locked metadata resolves 215 packages including 208 registry
  packages with license metadata; all seven package listings include LICENSE
  and NOTICE; workspace metadata/dependency checks pass. UTC and Asia/Taipei
  Unique ID fixture tests pass (4 each); formatting and diff checks pass. This
  closes P18-V02 server-free gates on the tested code. Broad P18-V01,
  current-revision GitHub Ubuntu/P18-V04 and native Windows runtime CI evidence,
  Phase 19 Docker/Broker automation and scenario execution,
  `docs/owner-validation.md`, and pre-publication version/dependency resolution
  remain open.
- Current-revision server-free P18-V02 rerun (code `ff138fa3`, 2026-10-16):
  default and all-feature locked workspace tests, strict all-target/all-feature
  Clippy, pinned Maven `verify` (37 scenarios, updated mutable-hook/pipeline
  fixture and Java/Rust cross-decodes), comparator self-test and manifest,
  offline race/leak smoke (16 cases × 3), fuzz smoke (105 frame seeds, 11
  message seeds, 1,856 mutations, 146 frame decodes, 113 message decodes),
  release examples, workspace bench compilation, formatting, and diff checks
  pass. Remoting after-response mutation now covers request/response commands;
  request-pipeline mutation now checks response opaque propagation. Broad
  P18-V01 remains open for the broad public API/settings and source-flow audit.
  The Java `UtilAll.exceptionSimpleDesc` versus Rust error-remark difference is
  accepted as diagnostic text only; text-for-text equivalence is not claimed.
  Current-revision GitHub Ubuntu/P18-V04 and
  native Windows runtime CI evidence, Phase 19 Docker/Broker automation and
  scenario execution, `docs/owner-validation.md`, and pre-publication
  version/dependency resolution remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T174604Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T174604Z.rlog).
- Current-revision server-free P18-V02 rerun (code `15c6ca4f`, 2026-10-16;
  compatibility-document disposition committed separately as `ebeb6460`):
  formatting, default and all-features locked workspace tests, strict
  all-target/all-feature workspace Clippy, pinned Maven `verify` (37 scenarios,
  Java runtime probes and Java/Rust fixture cross-decodes), comparator self-test,
  37-scenario manifest validation, offline race/leak smoke (16 cases × 3), fuzz
  smoke (105 frame seeds, 11 message seeds, 1,856 mutations, 146 frame and
  113 message decodes), release example build, workspace benchmark compilation,
  locked Cargo metadata, and final diff checks all pass. This closes the
  current-revision P18-V02 server-free gates. Broad P18-V01 source/API/settings,
  overload/input/error/control-flow and reverse-export reconciliation remains
  open. Current-revision GitHub Ubuntu address-profile and native Windows
  runtime CI evidence, Phase 19 Docker/Broker automation and live scenario
  execution, `docs/owner-validation.md`, and pre-publication version/registry
  dependency resolution remain open.
- Phase 18 offline performance measurement (`15c6ca4f`, 2026-10-16): ran the
  release benchmark on the recorded Apple M5 Max / Rust 1.85.0 environment for
  all five fixed-input operations and allocation sampling. Input sizes,
  iteration counts, timing and allocation results are recorded in
  [PHASE_18_OFFLINE_BENCHMARK_BASELINE.md](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
  These are machine-specific observations without a pass/fail threshold.
- P18-V01 Remoting `RemotingClient` public inherent-method scan (`f853cb69`,
  2026-10-16): all-feature Rustdoc JSON enumerated 64 public inherent methods.
  The unused public `invoke_name_server_with_peer` helper was made private; a
  compile-fail doctest guards that boundary. The compatibility crosswalk now
  names every remaining inherent method, including the connect-timeout and
  GO_AWAY live accessors and both owned-processor registration methods. The
  exact-name scan reports zero unlisted `RemotingClient` methods. Remoting
  doctests (9), strict Remoting Clippy, and Core route RPC regressions (5) pass.
  This closes that bounded method-name/public-surface slice only; the broader
  Java/Rust source, overload/input/error/control-flow, inherited API, and
  cross-crate reverse-export audit remains open. See
  [PHASE_3_REMOTING_INVENTORY.md](compatibility/PHASE_3_REMOTING_INVENTORY.md).
- P18-V01 Remoting semaphore helper API slice (`978791b3`, 2026-10-16, pinned
  Java source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): added the
  Rust `client::SemaphoreReleaseOnlyOnce` equivalent for Java's public
  constructor, `release`, and `getSemaphore`; `ResponseFuture::new` and
  `new_with_release_once` now map the pinned Java constructor overloads, while
  the Tokio-only `new_with_permit` remains private under a compile-fail doctest.
  `fixtures/remoting/response-future-api.json` pins both Java constructor/method
  inventories and release/null/identity behavior (SHA-256
  `4fc0509fee67637d2e1655a39dfb810ecac19e264289c6634448fa85f38aab4c`). The
  Remoting all-feature suite passes (105 unit, 8 codec, 1 fuzz, 7 security,
  107 transport, and 10 doctests), as do strict Remoting Clippy, Maven verify
  (37 scenarios and the new reflection fixture), formatting, and diff checks.
  This closes the helper and constructor slice only; broad P18-V01 and the
  final-revision workspace gates remain open.
- P18-V01 Remoting `ResponseFuture` callback failure continuation
  (`b597ccda`, 2026-10-16):
  the pinned Java fixture covers cause retention for send, timeout, and generic
  callback failures, fail-before-complete order, and the null-request generic
  failure before callback dispatch. Rust now retains the cause in all three
  typed error paths and preserves the null-request failure. Remoting all-feature
  tests pass (106 unit, 8 codec, 1 fuzz regression, 7 security, 107 transport,
  and 10 doctests); strict Remoting Clippy, formatting, diff checks, and full
  Maven `verify` (37 scenarios and runtime fixtures) pass. This closes this
  callback-failure slice only. `ResponseFuture.toString`, `waitResponse`
  interruption semantics, broad P18-V01, final-revision workspace gates,
  current-revision GitHub Ubuntu/P18-V04 evidence, Phase 19 Docker/Broker
  automation, and release version/registry checks remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T185506Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T185506Z.rlog).
- P18-V01 Remoting `ResponseFuture.toString` and `waitResponse` disposition
  (`db03c206`, 2026-10-16, pinned Java source
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): Rust `Display` now includes
  Java's stable state fields in the same order and follows the response-latch
  count. The fixture records Java's external-thread `InterruptedException`
  and cleared interrupt flag; Rust's standard Condvar has no external thread
  interrupt facility, while the separate `ResponseFuture::interrupt()` remains
  mapped. Stable display/latch and interruption-boundary fixtures pass.
  Fixture SHA-256: `4739b82304af6d251fe963f87f90f829215add80632976ccd6d9ef411c6494f4`.
  Remoting all-feature tests pass (108 unit, 8 codec, 1 fuzz regression, 7
  security, 107 transport, 10 doctests); strict Remoting Clippy, format/diff
  checks, and Maven `verify` (37 scenarios, including the new Java fixture)
  pass. Broad P18-V01 and full workspace P18-V02 remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T190618Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T190618Z.rlog).
- P18-V01 Remoting `TransportError` construction-helper surface (`c3e669d6`,
  2026-10-16, pinned Java source `efa1be59d6fc747ad220f036c86030f05f0419f9`):
  `timeout`, `timeout_with_cause`, `connect_source`, and `tls_source` are now
  crate-internal helpers; the redundant `send_source` factory was removed.
  Public typed variants remain available. `ErrorSource` is mapped to Java
  `Throwable` message/cause/type-inspection capability, and the Rust value
  equality difference is recorded as non-flow diagnostic behavior. Five
  compile-fail doctests guard helper visibility. Remoting all-feature tests
  pass (108 unit, 8 codec, 1 fuzz regression, 7 security, 107 transport, and
  15 doctests); strict all-target/all-feature Remoting Clippy, workspace format
  check, and diff check pass. This closes only the identified error-factory
  visibility slice; broad P18-V01 and final-revision full-workspace P18-V02
  remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T191756Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T191756Z.rlog).
- P18-V01 Remoting `ResponseFuture` declared-method crosswalk (2026-10-16,
  pinned source `efa1be59d6fc747ad220f036c86030f05f0419f9`): the inventory now
  lists the Rust mapping for every Java-declared method, including nullable
  command/channel values, mutable-command closures, timeout/latch methods, and
  the `Display` mapping for `toString`. The source file at the pinned commit and
  the current Java worktree have the same SHA-256
  `63789320d327604995dadfa80ae126f9b321e50571fca68d67d371fb5fa065bf`; the
  invoked fixture pins both constructors and all 20 declared public methods
  (fixture SHA-256
  `4739b82304af6d251fe963f87f90f829215add80632976ccd6d9ef411c6494f4`). This
  closes the method-crosswalk documentation slice only; broad P18-V01 and the
  final-revision full-workspace P18-V02 remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T192429Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T192429Z.rlog).
- P18-V01 ACL `AclUtils` reflected public surface (`abde18c6`, 2026-10-16,
  pinned Java source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): the
  invoked Java fixture records the public no-argument constructor and all 22
  declared public static signatures. The Core regression asserts that exact
  list and exercises each corresponding Rust wrapper; whitespace and out-of-
  range `isScope(String)` inputs are included. Pinned and current Java
  `AclUtils.java` SHA-256: `86a4a1f968685a6ef3392a0d8b67f0af8301a70e2dbace74de0908a4be7f7fa3`;
  fixture SHA-256: `bb2e97058c0d8509d8143e0a21bd67bad51c0079d8753627db86086973667c4f`.
  Focused Core test, strict all-target/all-feature Core Clippy, workspace format
  and diff checks pass; Maven `verify` passes. This closes the declared
  `AclUtils` surface slice only; broad P18-V01 and final-revision full-workspace
  P18-V02 remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T194232Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T194232Z.rlog).
- P18-V01 request/reply timeout-helper reverse scan (`aa4ce064`, 2026-10-16,
  pinned Java source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): the
  public Rust `RequestResponseFuture::is_timeout_at(Instant)` helper was only
  used by the Core runtime registry and had no Java public counterpart. It is
  now runtime-internal; a Core compile-fail doctest guards the public boundary,
  and the Phase 15 matrix records the disposition. The pinned Java
  `RequestResponseFuture.java` SHA-256 is
  `08f2a1eec7e492d95fd9502e3b942960df948674182de5632b7b02d0514d95d4`; the
  pinned source and Maven 5.5.1 artifact both omit the newer-worktree-only
  `acquireCountDownLatch` method. Core all-feature tests (168 unit plus all
  integration tests and 14 doctests), strict Core Clippy, Core Rustdoc JSON,
  format and diff checks pass. This closes only this Rust-only helper surface;
  broad P18-V01 and final-revision full-workspace P18-V02 remain open. Execution
  log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T195635Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T195635Z.rlog).
- P18-V01 `RequestResponseFuture` method-level crosswalk (2026-10-16): the
  Phase 15 matrix now maps the constructor and every one of the 18 public Java
  methods to the exact Rust API, including the `CountDownLatch` adapter and
  async wait representation. The pinned Java source SHA-256 is
  `08f2a1eec7e492d95fd9502e3b942960df948674182de5632b7b02d0514d95d4`; the
  invoked fixture SHA-256 is
  `295307a1f559b7afa30179b48a83b0ac008716a8a1b421872b440c6ff097153c`.
  This closes the declared-method crosswalk slice only; broad P18-V01 and the
  final-revision P18-V02 workspace gate remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T195832Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T195832Z.rlog).

- P18-V01 BatchAck inherited serialization (`bd228919`, 2026-10-16, pinned
  Java source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`):
  `BatchAckMessageRequestBody` now maps inherited compact/pretty JSON, encode,
  decode/list decode, fromJson and their nullable results. The fixture pins
  both constructors, all 19 declared `BatchAck` methods, the four declared body
  methods, and all 13 callable body methods including inherited overloads.
  Three serializer cases and the 111-case body input matrix pass through the
  Java-compatible Rust adapters. Protocol all-feature tests (118 unit tests,
  integration targets and 42 doctests), strict Protocol Clippy, Maven verify
  (37 scenarios), formatting and diff checks pass. Empty
  `JSONWriter.Feature[]` is covered; non-empty writer features remain outside
  this adapter. Broad P18-V01 and the final-revision full-workspace P18-V02
  gate remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T201832Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T201832Z.rlog).
- P18-V01 CheckClient inherited serialization continuation (2026-10-16, pinned
  Java source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): Rust now
  exposes inherited compact/pretty JSON, instance/static encode, decode/list
  decode, `fromJson`, and nullable adapters for `CheckClientRequestBody`. The
  invoked fixture pins all nine inherited Java signatures and covers default,
  explicit-null, populated nested subscription data, and Java null utility
  results including a null list member. Protocol all-feature tests (119 unit
  tests, integration targets and 42 doctests), strict Protocol Clippy, Maven
  verify (37 scenarios), formatting and diff checks pass. Empty
  `JSONWriter.Feature[]` is recorded; non-empty writer features and the broader
  inherited DTO audit remain open. Pinned Java source SHA-256:
  `769c533d9d6940c97ffcca009b6b2089a1e2c6eb251b517b9dbb247f5085505a`;
  fixture SHA-256:
  `b40aca9ee7eecbcfccb272757fd0f7371f953ffece7634352b0bcd91079d24c0`.
  The final-revision full-workspace P18-V02 rerun remains open.
- P18-V01 inherited RemotingSerializable body adapters (`96756a8e`,
  2026-10-16, pinned Java source commit
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): adapters now cover the mapped
  body DTOs, including `GetConsumerStatusBody` and `ResetOffsetBody` through
  their existing MessageQueue-key codecs. `QueueTimeSpan` timestamp getters
  and derived JSON fields match Java across UTC and Asia/Taipei, including
  signed-millisecond extremes. Protocol all-feature tests (120 unit tests,
  integration targets, and 42 doctests), strict Protocol Clippy, Maven verify
  (37 scenarios), and the Core focused regression under both timezones pass.
  Empty `JSONWriter.Feature[]` is covered; non-empty writer features remain
  unresolved. Fixture SHA-256:
  `468bcf8a39cfb5f8c9706a474501897f370c42dd8f747b414c3a1d1d91a2402e`.
  This closes only the no-feature inherited serializer slice. Broad P18-V01,
  the final-revision full-workspace P18-V02 run, current-revision platform CI
  evidence, and Phase 19 local Docker/Broker automation remain open.
- P18-V01 inherited RemotingSerializable writer-feature inventory (worktree,
  2026-10-16, pinned Java source commit
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): the invoked fixture enumerates
  all 48 `JSONWriter.Feature` values and records feature-specific output for 32
  representative cases across 15 inherited body DTO types. Twenty-two
  features changed at least one sampled output; the other 26 made no change
  in these samples, which does not establish that they are inert for every
  valid input. The probe explicitly passes a `Feature[]` to resolve the Java
  instance varargs overload. Pinned offline Maven verify passes all 37
  scenarios. Fixture SHA-256:
  `5a10759a1b73504c0476b0d08c0f922ab74c5aa04f41b9203e6bfd8a8a7d2d6c`;
  Java fixture source SHA-256:
  `b5bdc4a43c7f28d05ef6b91b546c62e3b48124b683e8440176d419689b3962e7`.
  This is probe evidence only: Rust has no feature-aware encoder yet, and the
  P18-V01 serializer API/behavior slice remains open. Broad P18-V01, the
  final-revision P18-V02 workspace gate, current-revision platform CI evidence,
  and Phase 19 local Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T210508Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T210508Z.rlog).
- P18-V01 inherited RemotingSerializable writer-feature implementation
  (commit `2261c5b1`, 2026-10-16; pinned Java source commit
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): added the public
  `JavaJsonWriterFeature` inventory with all 48 Java names and ordinals, and
  `java_encode_with_features` adapters for 15 body DTOs, both route DTOs, and
  `HeartbeatData`. `WriteNonStringValueAsString` now matches invoked Java
  output, including special `MessageQueue` and route numeric map keys. The
  other 47 features are explicitly rejected as unmapped; enum inventory and
  probe coverage do not close their behavior. The current worktree passes
  `cargo test -p rocketmq-classic-protocol --all-features --locked` (121 unit
  tests, integration targets, 42 doctests), strict all-target/all-feature
  Protocol Clippy, pinned Maven `verify` (37 scenarios), `cargo fmt --all
  -- --check`, and `git diff --check`. Fixture SHA-256 values: `lookup.json`
  `5a10759a1b73504c0476b0d08c0f922ab74c5aa04f41b9203e6bfd8a8a7d2d6c`,
  `route.json` `189b34d28d4719b44839a7cc08ab8e209f711b613285a6a6baad194378829cd4`,
  and `heartbeat.json`
  `459adaf81a93ca4d1cb08075fcc65c4db7b75c6a9582ffd0500b499283585bda`.
  This closes one writer-feature behavior slice only. Broad P18-V01, the
  final-revision full-workspace P18-V02 gate, current-revision platform CI
  evidence, package/pre-publication review, and Phase 19 local Docker/Broker
  automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T214928Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T214928Z.rlog).
- P18-V01 inherited RemotingSerializable `PrettyFormat` feature continuation
  (commit `cf51df04`, 2026-10-16): `java_encode_with_features` now applies the
  Java tab-indented pretty output on all 15 body DTOs, both route DTOs, and
  `HeartbeatData`. The Java writer-feature output fixture is compared byte for
  byte, including `QueryConsumeTimeSpanBody` timezone projections. Protocol
  all-feature tests (121 unit tests, all integration targets, 42 doctests),
  strict Protocol Clippy, formatting, and diff checks pass. Two of 48 writer
  features now have behavior mappings; 46 remain unmapped. The final-revision
  full-workspace P18-V02 gate must be rerun after this commit. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T220515Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T220515Z.rlog).
- P18-V01 Fastjson2 space-indent writer features (commit `c9d502a3`,
  2026-10-16): `PrettyFormatWith2Space` and `PrettyFormatWith4Space` now match
  the invoked outputs byte for byte across the 15 body DTOs, both route DTOs,
  and `HeartbeatData`. The source-backed precedence test confirms 4-space
  formatting wins when both options are supplied. Protocol all-feature tests
  (121 unit tests, all integration targets, 42 doctests), strict Protocol
  Clippy, format, and diff checks pass. Four of 48 features now have behavior
  mappings; 44 remain unmapped. The final-revision full-workspace P18-V02 gate
  is still pending after this change. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T221138Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T221138Z.rlog).
- P18-V01 `WriteBooleanAsNumber` and combined feature precedence (2026-10-16):
  Java fixtures now pin the individual and combined outputs across inherited
  body, route, and heartbeat serializers. Fastjson2 stringifies booleans when
  both `WriteBooleanAsNumber` and `WriteNonStringValueAsString` are supplied;
  Rust matches that precedence. Five of 48 writer features now have behavior
  mappings; the other 43 remain unmapped. Protocol all-feature tests (121 unit
  tests, all integration targets, 42 doctests), strict Protocol Clippy, Maven
  verify (37 scenarios), format, and diff checks pass. Fixture SHA-256 values:
  `lookup.json` `c0398f15a506db742fff5bc9c019ffe5b944649c08cce8b97847cc6403bffcdf`,
  `route.json` `ad46456edced9bd7152c27ad4af06c45273f9a3ea715067d507fc732c551b4d7`,
  `heartbeat.json`
  `b9ffb50d9624ca531076444b2529e3a729baacf0edfc8361d93a19d1ab7df797`.
  Broad P18-V01, final-revision full-workspace P18-V02, current-revision
  platform CI evidence, package/pre-publication review, and Phase 19 local
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T222453Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T222453Z.rlog).
- P18-V01 `EscapeNoneAscii` writer feature (commit `2dcbb98e`, 2026-10-16):
  inherited body, route, and heartbeat serializers now match the pinned Java
  output. Invoked Unicode cases cover a Latin-1 character that remains raw, a
  high BMP character escaped as `\uXXXX`, and supplementary characters emitted
  as surrogate pairs. Six of 48 writer features now have behavior mappings;
  the other 42 remain unmapped. Protocol all-feature tests (122 unit tests, all
  integration targets, 42 doctests), strict Protocol Clippy, Maven verify (37
  scenarios), format, and diff checks pass. Fixture SHA-256 values: `lookup.json`
  `ad6ab8868be11ad924d10ff34a9542e59018fb883a355dc6ac2fe0fca7182977`,
  `route.json` `e7199dea0c92a60fecbc569a048981511c74ba6281e873e2a7d1ea84135d7e56`.
  Broad P18-V01, final-revision full-workspace P18-V02, current-revision
  platform CI evidence, package/pre-publication review, and Phase 19 local
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T223610Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T223610Z.rlog).
- P18-V01 Fastjson empty-value writer features (code commit `dc791e63`,
  2026-10-16): `NotWriteEmptyArray` now omits empty collection and map
  properties; `IgnoreEmpty` also omits empty strings. Invoked body, route, and
  heartbeat fixture cases cover both features, including empty route maps and
  nested DTO collections. Eight of 48 writer features now have behavior
  mappings; the other 40 remain unmapped. Protocol all-feature tests (122 unit
  tests, all integration targets, 42 doctests), strict Protocol Clippy, Maven
  verify (37 scenarios), formatting, and diff checks pass. The prior
  full-workspace default/all-feature runs were on parent `c7c26cc0`; the final
  workspace P18-V02 gate must be rerun after this code commit. Broad P18-V01,
  current-revision platform CI evidence, package/pre-publication review, and
  Phase 19 local Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T225647Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T225647Z.rlog).
- P18-V01 `NotWriteDefaultValue` writer feature (code commit `9f7870fe`,
  2026-10-16): inherited body, route, and heartbeat serialization now omits
  Java default-valued bean fields while retaining map entry values and the
  pinned `autoCommit` output. The Java fixtures cover populated nested DTOs,
  false/zero primitive defaults, route mapping IDs, and explicit class defaults.
  Nine of 48 writer features now have behavior mappings; the other 39 remain
  unmapped. Protocol all-feature tests (122 unit tests, all integration
  targets, 42 doctests), strict Protocol Clippy, Maven verify (37 scenarios),
  formatting, and diff checks pass. The final full-workspace P18-V02 gate must
  be rerun after this code commit. Broad P18-V01, current-revision platform CI
  evidence, package/pre-publication review, and Phase 19 local Docker/Broker
  automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T230936Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T230936Z.rlog).
- P18-V01 Fastjson writer feature continuation (commits `c3842532` and
  `de2d1a8f`, 2026-10-16): `WriteNonStringKeyAsString` now quotes numeric route
  map keys; `WriteLongAsString` stringifies mapped Java long-valued DTO fields
  and offset-map values, quotes long-keyed route `brokerAddrs` entries, and
  leaves integer-keyed `currIdMap` entries numeric. Eleven of 48 writer
  features now have behavior mappings; the other 37 remain unmapped. Protocol
  all-feature tests (122 unit tests, all integration targets and 42 doctests),
  strict all-target/all-feature Protocol Clippy, pinned Maven verify (37
  scenarios), formatting and `git diff --check` pass on `de2d1a8f`. The final
  workspace P18-V02 gate, broad P18-V01 audit, current-revision platform CI,
  package/pre-publication review and Phase 19 Docker/Broker automation remain
  open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T232526Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T232526Z.rlog).
- P18-V01 Fastjson field-name writer features (code commit `9026c715`,
  2026-10-16): `UseSingleQuotes` and `UnquoteFieldName` now match the invoked
  outputs for all mapped body DTOs, route DTOs, and `HeartbeatData`. The writer
  preserves Fastjson's boolean/enum property-name behavior, distinguishes map
  entries from bean properties, and handles single-quote escaping together
  with `EscapeNoneAscii`. Thirteen of 48 writer features now have behavior
  mappings; the other 35 remain unmapped. Protocol all-feature tests (123 unit
  tests, all integration targets, 42 doctests), strict all-target/all-feature
  Protocol Clippy, Maven verify (37 scenarios), formatting, and diff checks
  pass. The final-revision workspace P18-V02 rerun remains due after the
  remaining Phase 18 code changes. Broad P18-V01, current-revision platform CI,
  Phase 19 Docker/Broker automation, owner-validation handoff, and pre-release
  version/registry checks remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T235429Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T235429Z.rlog).
- P18-V01 Fastjson non-serializable writer features (code commit `bc5642df`,
  2026-10-17): `IgnoreNoneSerializable` returns Java's fixture-pinned `null`,
  `ErrorOnNoneSerializable` maps the fixture-pinned `JSONException` failure,
  and `IgnoreNoneSerializable` takes precedence when both are set. Body, route,
  and heartbeat fixtures cover default, null, and populated DTO cases. Fifteen
  of 48 writer features now have behavior mappings; 33 remain unmapped.
  Protocol tests (123 unit tests, all integration targets, 42 doctests),
  default/all-feature workspace tests, strict workspace Clippy, pinned Maven
  verify (37 scenarios), differential self-test/manifest, race/leak smoke
  (16 cases x 3), fuzz smoke (1,856 mutations), release examples, workspace
  bench compilation, metadata/package checks, formatting, and diff checks pass.
  P18-V02 server-free local gates are complete on `bc5642df`; broad P18-V01,
  current-revision Ubuntu/Windows CI results, Phase 19 Docker/Broker automation,
  owner-validation handoff, and pre-publication version/registry checks remain
  open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T001244Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T001244Z.rlog).
- P18-V01 `WriteEnumUsingOrdinal` (code commit `5f08a240`, 2026-10-17): enum-valued
  protocol bean fields now serialize as Java ordinals; same-named string values
  in map properties are preserved, including nested `messageRequestModeMap`.
  Sixteen of 48 writer features have behavior mappings; 32 remain unmapped.
  Protocol all-feature tests (125 unit tests, all integration targets, and 42
  doctests), strict Protocol Clippy, Maven verify (37 scenarios), formatting,
  and diff checks pass on `5f08a240`. The full-workspace P18-V02 gate remains
  due after the remaining Phase 18 code changes. Broad P18-V01, current-revision
  Ubuntu/Windows CI, package/provenance review, owner-validation handoff, and
  Phase 19 Docker/Broker automation and scenario execution remain open.
  Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).
- P18-V01 Fastjson `BrowserCompatible` writer feature (code commit `8c22e355`,
  2026-10-17): mapped serializers now quote numeric route-map keys and emit
  mapped integral values outside JavaScript's exact integer range as strings.
  Invoked Java fixtures cover both safe boundaries and values just outside
  them, plus route-map keys. Seventeen of 48 writer features now have behavior
  mappings; 31 remain unmapped. Protocol all-feature tests (125 unit tests, all
  integration targets, and 42 doctests), strict Protocol Clippy, Maven verify
  (37 scenarios), formatting, and diff checks pass. Broad P18-V01, final
  full-workspace P18-V02, current-revision Ubuntu/Windows CI, P18-V03 package
  review, P18-V04 handoff, and Phase 19 Docker/Broker automation remain open.
  Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).
- P18-V01 Fastjson `OptimizedForAscii` writer feature (code commit `8a9939b9`,
  2026-10-17): accepted as an output-preserving option. Fastjson 2.0.63 uses a
  UTF-8 writer path for the option; Rust's protocol JSON writer already emits
  UTF-8. Java/Rust fixture comparisons remain byte-identical for ASCII and
  Unicode values across body, route, and heartbeat serializers. Eighteen of 48
  writer features now have behavior mappings; 30 remain unmapped. Protocol
  all-feature tests (125 unit tests, all integration targets, and 42 doctests),
  strict Protocol Clippy, Maven verify (37 scenarios), formatting, and diff
  checks pass. Broad P18-V01, final full-workspace P18-V02, current-revision
  Ubuntu/Windows CI, P18-V03/V04, and Phase 19 Docker/Broker automation remain
  open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).

- P18-V01 Fastjson `WriteNameAsSymbol` mapping (code commit `4d07ad5a`,
  2026-10-17): pinned RocketMQ 5.5.1 `RemotingSerializable.encode` writes
  UTF-8 text JSON through Fastjson2 `JSON.toJSONBytes`/`JSONWriter.ofUTF8`;
  `WriteNameAsSymbol` affects binary JSON type-name paths, so this text output
  remains byte-identical. Existing invoked Java feature fixtures cover the
  mapped body DTOs, route DTOs, and HeartbeatData; Rust assertions compare the
  output against each Java baseline. No fixture regeneration was needed.
- Validation on `4d07ad5a`: Protocol all-feature suite passed (125 unit tests,
  all integration targets, 42 doctests); strict all-target/all-feature
  Protocol Clippy, formatting, `git diff --check`, and Maven verify (37
  scenarios, fixture drift and cross-decodes) passed. Writer feature mapping is
  now 19/48; 29 remain unmapped. Broad P18-V01, the final full-workspace
  P18-V02 rerun, current-revision Ubuntu/Windows CI, P18-V03/V04, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).

- P18-V01 Fastjson `NotWrite*ClassName` writer flags (code commit `812a3ff9`,
  2026-10-17): the four suppression flags are output-preserving when passed
  alone because `WriteClassName` remains disabled in this UTF-8 text encoder.
  Pinned Fastjson 2.0.63 checks three flags only while writing type metadata;
  `NotWriteSetClassName` has no writer callsite. Existing Java fixture output
  cases cover CheckClient, BatchAck, all mapped body adapters, both route DTOs,
  and HeartbeatData. The Rust differential compares each output to its Java
  baseline. Combinations with the still-unmapped `WriteClassName` remain
  unsupported.
- Validation on `812a3ff9`: Protocol all-feature suite passed (125 unit tests,
  all integration targets, 42 doctests); strict all-target/all-feature
  Protocol Clippy, formatting, `git diff --check`, and Maven verify (37
  scenarios, fixture drift and cross-decodes) passed. Writer feature mapping is
  now 23/48; 25 remain unmapped. Broad P18-V01, the final full-workspace
  P18-V02 rerun, current-revision Ubuntu/Windows CI, P18-V03/V04, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).
- P18-V01 CheckClient/BatchAck writer-flag test follow-up (code commit
  `4ae3735a`, 2026-10-17): their dedicated inherited-serialization tests now
  compare Java fixture baselines for `WriteNameAsSymbol` and all four
  `NotWrite*ClassName` flags across default, explicit-null, and populated
  cases. The Java oracle probes every pinned writer feature and records changed
  output or exceptions; an absent override therefore represents the unchanged
  Java baseline. The focused Protocol command
  `cargo test -p rocketmq-classic-protocol --all-features --locked
  inherited_remoting_serialization_matches_invoked_java` passed (3 matching
  tests, including the two dedicated DTO tests); formatting and diff checks
  passed. This closes the named test-coverage slice only. The final full-workspace
  P18-V02 rerun, broad P18-V01 audit, 25 unmapped writer features,
  current-revision Ubuntu/Windows CI, P18-V03/V04, and Phase 19 Docker/Broker
  automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).
- P18-V01 inert DTO writer-feature mapping (code commit `b627c15e`,
  2026-10-17): `WriteByteArrayAsBase64`, `WritePairAsJavaBean`,
  `WriteThrowableClassName`, and `WriterUtilDateAsMillis` preserve output for
  the mapped body, route, and heartbeat DTO graph. Pinned Fastjson2 call sites
  limit these options to byte arrays, Apache Pair values, Throwable values,
  and `java.util.Date`; the mapped graph has none of those fields. BatchAck's
  BitSet uses RocketMQ's custom serializer, and QueueTimeSpan has long fields
  with derived date strings. Invoked Java fixtures show no output change, and
  Rust now compares each adapter against those baselines. Protocol all-feature
  tests (125 unit tests, all integration targets, 42 doctests), strict Protocol
  Clippy, Maven verify (37 scenarios, fixture checks and cross-decodes),
  formatting, and diff checks pass. Mapping count is now 27/48; 21 writer
  features remain unmapped. The final full-workspace P18-V02 rerun, broad
  P18-V01 audit, current-revision Ubuntu/Windows CI, P18-V03/V04, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).
- P18-V01 numeric writer-format mapping (code commit `b25f6a79`, 2026-10-17):
  `WriteBigDecimalAsPlain` and `WriteFloatSpecialAsString` preserve output for
  the mapped body, route, and heartbeat DTO graph. Pinned Fastjson2 uses these
  options only for BigDecimal, Float, and Double values; none appear in this
  graph's fields or map-value types, and the invoked Java fixture records no
  output changes. Rust now compares every mapped adapter against the Java
  baseline. Protocol all-feature tests (125 unit tests, all integration
  targets, 42 doctests), strict Protocol Clippy, formatting, and diff checks
  pass. Mapping count is now 29/48; 19 writer features remain unmapped. The
  final full-workspace P18-V02 rerun, broad P18-V01 audit, current-revision
  Ubuntu/Windows CI, P18-V03/V04, and Phase 19 Docker/Broker automation remain
  open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).
- P18-V01 Fastjson `BrowserSecure` writer-feature mapping (code commit
  `02ae239d`, 2026-10-17): standalone `BrowserSecure` escapes `<`, `>`, `(`,
  and `)` in mapped string values and quoted field names as uppercase Unicode
  escapes. The invoked body, route, and heartbeat fixtures also cover
  `BrowserSecure+UnquoteFieldName`: arbitrary route map keys remain raw, while
  string values keep the Java escapes. The `UnquoteFieldName` route case pins
  Fastjson's unquoted output for a non-identifier map key. Protocol all-feature
  tests (126 unit tests, all integration targets, 42 doctests), strict Protocol
  Clippy, format/diff checks, and Maven verify (37 scenarios, fixture checks,
  and cross-decodes) pass on `02ae239d`. The writer-feature mapping count is now
  30/48; 18 remain unmapped. The final full-workspace P18-V02 rerun, broad
  P18-V01 audit, current-revision Ubuntu/Windows CI, P18-V04 handoff, and Phase
  19 Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T014126Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T014126Z.rlog).
- P18-V01 Fastjson enum-name writer flags (code commit `d13636e8`,
  2026-10-17): `WriteEnumsUsingName` and `WriteEnumUsingToString` preserve the
  pinned Java output across body, route, and heartbeat DTO cases: the covered
  Java enum name and `toString()` values match their existing encoded strings.
  Rust now accepts both flags and checks their output against each applicable
  Java fixture. Protocol all-feature tests (126 unit tests, all integration
  targets, 42 doctests), strict Protocol Clippy, formatting, and diff checks
  pass. Maven verify passed on fixture revision `02ae239d`; this change adds no
  Java fixture changes. Mapping count is now 32/48; 16 writer features remain
  unmapped. Final full-workspace P18-V02, broad P18-V01, current-revision
  Ubuntu/Windows CI, P18-V04 handoff, and Phase 19 Docker/Broker automation
  remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T014736Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T014736Z.rlog).

- P18-V01 Fastjson `IgnoreErrorGetter` writer-feature mapping (code commit
  `cceed2ab`, 2026-10-17): the pinned Java oracle explicitly invokes the flag
  for 41 body, 8 route, and 3 heartbeat DTO cases; every encoding equals its
  default output. Rust now accepts the flag and compares each applicable DTO
  adapter with those Java fixture values. Protocol all-feature tests (126 unit
  tests, all integration targets, and 42 doctests), strict Protocol Clippy,
  formatting, and diff checks pass. Maven verify passes with 37 scenarios,
  fixture drift checks, and cross-decodes. The mapping count is now 33/48; 15
  writer features remain unmapped. Final full-workspace P18-V02, broad P18-V01,
  current-revision Ubuntu/Windows CI, P18-V04 handoff, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog).

- P18-V01 Fastjson `IgnoreNonFieldGetter` writer-feature mapping (code commit
  `333e24a8`, 2026-10-17): the Java oracle records the feature's filtered output
  for getter-only `QueueTimeSpan` timestamp strings and for `MessageQueue` and
  numeric route-map keys. Rust omits the three derived timestamp properties,
  serializes the affected map keys using the Java string form, and compares
  those results across the body, route, and heartbeat fixture suites. Protocol
  all-feature tests (126 unit tests, all integration targets, and 42 doctests),
  strict Protocol Clippy, formatting, diff checks, and Maven verify (37
  scenarios, fixture drift, and cross-decodes) pass. The mapping count is now
  34/48; 14 writer features remain unmapped. Final full-workspace P18-V02,
  broad P18-V01, current-revision Ubuntu/Windows CI, P18-V04 handoff, and Phase
  19 Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog).

- P18-V01 Fastjson `FieldBased` writer-feature mapping (code commit `6bce768d`,
  2026-10-17): the pinned Java fixture generator invokes every writer feature
  for 41 body, 8 route, and 3 heartbeat cases; `FieldBased` has no output
  overrides, including for `QueueTimeSpan`'s getter-derived timestamps. Rust
  accepts the flag and compares it with the default fixture bytes in those
  serializer suites. Protocol all-feature tests (126 unit tests, all
  integration targets, and 42 doctests), strict Protocol Clippy, formatting,
  diff checks, and Maven verify (37 scenarios, fixture drift, and cross-decodes)
  pass. Mapping count is now 35/48; 13 writer features remain unmapped. Final
  full-workspace P18-V02, broad P18-V01, current-revision Ubuntu/Windows CI,
  P18-V04 handoff, and Phase 19 Docker/Broker automation remain open. Execution
  log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T021935Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T021935Z.rlog).
- P18-V01 Fastjson `WriteNullBooleanAsFalse` writer-feature mapping (code
  commit `bd3e7ced`, 2026-10-17): the pinned Java oracle probes the feature for
  41 body, 8 route, and 3 heartbeat DTO cases; none records an output override.
  Rust accepts the flag and compares each current adapter's output with the
  corresponding fixture. The no-op claim is limited to these adapters; Java
  `BrokerReplicasInfo` contains a boxed Boolean, but its
  `getInSyncStateData` entrypoint belongs to the `rocketmq-tools` `MQAdminExt`
  extension, which Phase 17 explicitly excludes. Protocol all-feature tests
  (126 unit tests, all
  integration targets, and 42 doctests), strict Protocol Clippy, formatting,
  diff checks, and Maven verify (37 scenarios, fixture drift, and cross-decodes)
  pass. Mapping count is now 36/48; 12 writer features remain unmapped. Broad
  P18-V01, final full-workspace P18-V02, current-revision Ubuntu/Windows CI,
  P18-V04 handoff, and Phase 19 Docker/Broker automation remain open. Execution
  log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T022954Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T022954Z.rlog).
- P18-V01 Fastjson map-sorting writer flags (code commit `b1a071fd`,
  2026-10-17): `MapSortField` and `SortMapEntriesByKeys` sort serialized map
  entries by key. Rust's `serde_json::Value` maps are key-ordered in the
  configured dependency, so the current body, route, and heartbeat adapters
  already produce the feature output. The Java oracle records no overrides
  across 41 body, 8 route, and 3 heartbeat cases, including populated route
  maps. Protocol all-feature tests (126 unit tests, all integration targets,
  and 42 doctests), strict Protocol Clippy, formatting, diff checks, and Maven
  verify (37 scenarios, fixture drift, and cross-decodes) pass. Mapping count
  is now 38/48; 10 writer features remain unmapped. Broad P18-V01, final
  full-workspace P18-V02, current-revision Ubuntu/Windows CI, P18-V04 handoff,
  and Phase 19 Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T022954Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T022954Z.rlog).
- P18-V01 Fastjson `LargeObject` writer flag (code commit `fc4d2e30`,
  2026-10-17): Fastjson2 raises its writer array-capacity ceiling from 64 MiB
  to 1 GiB for this flag. The Rust adapter uses a growable `String` with no
  corresponding fixed ceiling, so accepting the flag does not alter its output
  or allocation policy; Rust's default large-output capacity is more permissive
  than Fastjson's default. The Java oracle records no output overrides across
  41 body, 8 route, and 3 heartbeat cases. Protocol all-feature tests (126 unit
  tests, all integration targets, and 42 doctests), strict Protocol Clippy,
  formatting, diff checks, and Maven verify (37 scenarios, fixture drift, and
  cross-decodes) pass. Mapping count is now 39/48; 9 writer features remain
  unmapped. Broad P18-V01, final full-workspace P18-V02, current-revision
  Ubuntu/Windows CI, P18-V04 handoff, and Phase 19 Docker/Broker automation
  remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T022954Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T022954Z.rlog).
- P18-V01 Fastjson `BeanToArray` writer flag (code commit `acd40aff`,
  2026-10-17): the adapters serialize mapped DTOs in Java bean-property order,
  including nested DTOs in collections and typed maps, while preserving user
  maps as maps. The transform retains null fields, applies enum ordinal/name
  behavior, and preserves queue-table map-key encoding. Body, route, and
  heartbeat fixture loops compare the feature output with invoked Java 5.5.1.
  Protocol all-feature tests (126 unit tests, all integration targets, and 42
  doctests), strict Protocol Clippy, formatting, diff checks, and Maven verify
  (37 scenarios, fixture checks, and Java/Rust cross-decodes) pass. Mapping
  count is now 40/48; the eight remaining flags are `WriteNulls`,
  `WriteMapNullValue`, `NullAsDefaultValue`, `WriteClassName`,
  `ReferenceDetection`, `WriteNullListAsEmpty`, `WriteNullStringAsEmpty`, and
  `WriteNullNumberAsZero`. Broad P18-V01, final full-workspace P18-V02,
  current-revision Ubuntu/Windows CI, P18-V04 handoff, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `WriteMapNullValue` writer flag (code commit `af0baada`,
  2026-10-17): the pinned Java oracle emits nullable DTO fields with this flag,
  matching its `WriteNulls` output for the mapped body, route, and heartbeat
  fixture cases. The schema-backed adapter restores those nullable fields;
  map values already represented as JSON null remain present in the Rust value
  tree. Protocol all-feature tests (126 unit tests, all integration targets,
  and 42 doctests), strict Protocol Clippy, formatting, and diff checks pass.
  Mapping count is now 42/48; six flags remain unmapped:
  `NullAsDefaultValue`, `WriteClassName`, `ReferenceDetection`,
  `WriteNullListAsEmpty`, `WriteNullStringAsEmpty`, and
  `WriteNullNumberAsZero`. Broad P18-V01, final full-workspace P18-V02,
  current-revision Ubuntu/Windows CI, P18-V04 handoff, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `WriteNulls` writer flag (code commit `e7b0acde`,
  2026-10-17): schema-backed serialization restores omitted nullable fields as
  JSON `null` across mapped body, route, and heartbeat DTOs, including nested
  typed DTOs. The pinned Java fixture comparisons cover all current feature
  samples in those adapters. Protocol all-feature tests (126 unit tests, all
  integration targets, and 42 doctests), strict Protocol Clippy, formatting,
  diff checks, and Maven verify (37 scenarios, fixture checks, and Java/Rust
  cross-decodes) pass. Mapping count is now 41/48; seven flags remain unmapped:
  `WriteMapNullValue`, `NullAsDefaultValue`, `WriteClassName`,
  `ReferenceDetection`, `WriteNullListAsEmpty`, `WriteNullStringAsEmpty`, and
  `WriteNullNumberAsZero`. Broad P18-V01, final full-workspace P18-V02,
  current-revision Ubuntu/Windows CI, P18-V04 handoff, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `WriteNullListAsEmpty` writer flag (code commit `038bf5f8`,
  2026-10-17): schema-backed traversal converts nullable collection properties
  to empty arrays, including nested typed DTO collections, without emitting
  unrelated missing nullable fields. Protocol all-feature tests (126 unit
  tests, all integration targets, and 42 doctests), strict Protocol Clippy,
  formatting, and diff checks pass. Mapping count is now 43/48; five flags
  remain unmapped: `NullAsDefaultValue`, `WriteClassName`,
  `ReferenceDetection`, `WriteNullStringAsEmpty`, and `WriteNullNumberAsZero`.
  Broad P18-V01, final full-workspace P18-V02, current-revision Ubuntu/Windows
  CI, P18-V04 handoff, and Phase 19 Docker/Broker automation remain open.
  Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `WriteNullStringAsEmpty` writer flag (code commit
  `68941489`, 2026-10-17): schema-backed serialization replaces nullable
  String fields and nullable String members in typed collections with empty
  strings across mapped body, route, and heartbeat DTOs. Protocol all-feature
  tests (126 unit tests, all integration targets, and 42 doctests), strict
  Protocol Clippy, formatting, and diff checks pass. Mapping count is now
  44/48; four flags remain unmapped: `NullAsDefaultValue`, `WriteClassName`,
  `ReferenceDetection`, and `WriteNullNumberAsZero`. Broad P18-V01, final
  full-workspace P18-V02, current-revision Ubuntu/Windows CI, P18-V04 handoff,
  and Phase 19 Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `WriteNullNumberAsZero` writer flag (code commit
  `63a90423`, 2026-10-17): this is output-preserving for the mapped DTO graph.
  The mapped bean schemas contain no nullable numeric fields; pinned Fastjson2
  map and collection writers serialize null entries with `writeNull()` without
  applying the number-null feature. Java feature fixtures record no output
  overrides. Protocol all-feature tests (126 unit tests, all integration
  targets, and 42 doctests), strict Protocol Clippy, formatting, and diff
  checks pass. Mapping count is now 45/48; three flags remain unmapped:
  `NullAsDefaultValue`, `WriteClassName`, and `ReferenceDetection`. Broad
  P18-V01, final full-workspace P18-V02, current-revision Ubuntu/Windows CI,
  P18-V04 handoff, and Phase 19 Docker/Broker automation remain open. Execution
  log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `NullAsDefaultValue` writer flag (code commit `a54512da`,
  2026-10-17): DTO-schema traversal emits empty strings for nullable String
  fields, empty arrays for nullable collections, and empty objects for nullable
  maps and nested DTO references. Nullable enum fields remain `null`, matching
  Fastjson2 2.0.63. Invoked Java writer fixtures cover body, route, and
  heartbeat defaults, explicit nulls, nested DTOs, and nullable String
  collection members. Protocol all-feature tests (126 unit tests, all
  integration targets, and 42 doctests), strict all-target/all-feature Protocol
  Clippy, formatting, and diff checks pass. Mapping is now 46/48; `WriteClassName`
  and `ReferenceDetection` remain unmapped. Broad P18-V01, final full-workspace
  P18-V02, current-revision Ubuntu/Windows CI, P18-V04 handoff, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `ReferenceDetection` writer flag (code commit `eb991847`,
  2026-10-17): serialization tracks shared `SharedValue` handles and emits the
  pinned Fastjson `$ref` path for repeated DTO instances. The invoked Java route
  fixture reuses one `BrokerData` object in two list positions; Rust matches
  `$.brokerDatas[0]`. Protocol all-feature tests (127 unit tests, all
  integration targets, and 42 doctests), strict all-target/all-feature Protocol
  Clippy, formatting, diff checks, and Maven verify (37 scenarios, fixture
  checks, and Java/Rust cross-decodes) pass. The final internal-marker refactor
  also passes the focused shared-route reference test and strict Protocol
  checks. Mapping is now 47/48; `WriteClassName` remains unmapped. Broad P18-V01,
  final full-workspace P18-V02, current-revision Ubuntu/Windows CI, P18-V04
  handoff, and Phase 19 Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `WriteClassName` writer flag (code commit `07d2fba1`,
  2026-10-17): serialization now emits Java root and polymorphic DTO type
  metadata, typed map metadata and MessageQueue map keys, `Set` markers, and
  `L` suffixes for small values held in Java `long` fields. The combined
  `NotWriteRootClassName`, `NotWriteHashMapArrayListClassName`, and
  `NotWriteSetClassName` policies are also mapped. Body, route, and heartbeat
  fixture loops compare the feature output with invoked Java 5.5.1. Protocol
  all-feature tests pass (127 unit tests, all integration targets, 42 doctests);
  strict all-target/all-feature Protocol Clippy, formatting, and diff checks
  pass. Writer feature inventory is now 48/48. The broad P18-V01 API/source/
  input/reverse-export audit, final full-workspace P18-V02, current-revision
  Ubuntu/Windows CI evidence, and Phase 19 local Docker/Broker automation
  remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).

- [x] **P18-V02 current-revision rerun** — Code commit `e9fe95cd` (2026-10-17):
  default and all-feature workspace tests, strict workspace Clippy, Maven
  `verify` (37 scenarios, fixture drift checks, Java/Rust cross-decodes),
  comparator self-test and 37-scenario manifest, race/leak smoke (16 cases ×
  3), fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations), release
  examples, workspace bench compilation, workspace metadata/dependency/license
  checks, seven package listings with `LICENSE`/`NOTICE`, format and diff checks
  all pass. The race rerun first exposed drift between a request's captured
  absolute deadline and its mutable future timeout. Registry expiration now
  honors the captured deadline; the focused regression and full smoke pass.
  This closes P18-V02's server-free local gates on this revision. Broad P18-V01,
  current-revision Ubuntu/Windows CI evidence, P18-V04, Phase 19 Docker/Broker
  automation and local scenario execution remain open. The pinned official
  RocketMQ 5.3.4 image digest was verified; basic Compose setup/readiness was
  subsequently checked as recorded below.
  Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).

- Phase 19 initial basic Docker profile (`2026-10-17`): Compose uses the pinned
  official RocketMQ 5.3.4 image and a project-scoped network/store volume,
  loopback-bound host ports, explicit disposable-test marker/profile, bounded
  readiness polling, and project-only volume cleanup. The NameServer and Broker
  became healthy; `mqadmin clusterList` reported Broker `broker-a` at
  `broker:10911`, version `V5_3_4`. Cleanup removed this run's containers,
  network, and volume. A root-owned fresh-volume issue was fixed with a
  one-shot volume owner initialization service. This verifies only the basic
  environment startup and cleanup path; Java/Rust client scenarios, runner,
  resource reset/evidence reconciliation, and two-Broker/TLS/auth profiles
  remain automatic Phase 19 work. No scenario had been marked as passed at this initial setup checkpoint. Evidence:
  [`integration-tests/README.md`](../integration-tests/README.md),
  [`rlogs/2026/10/17-phase-19-basic-broker-profile__20261017T043317Z.rlog`](../rlogs/2026/10/17-phase-19-basic-broker-profile__20261017T043317Z.rlog).

## Phase 19 local progress (2026-10-17)

- [x] Add an unpublished `rocketmq-classic-integration` workspace package,
  Java 5.5.1 route probe, and fail-closed differential selector. Code commits:
  `72266b44`, `0fa704f8`, `21ef9f4e`, `adae9d1f`, and `1a77ac68` (Maven JDK 27 /
  Rust 1.85.0 gate, evidence path redaction, and live heartbeat support).
- [x] Run `ROUTE-001-get-existing-topic` against the pinned Apache RocketMQ
  5.3.4 profile. Java and Rust returned the same four `broker-a` queue rows
  (IDs 0–3); scenario-topic deletion and project-only Compose cleanup passed.
  The runner marked this manifest row `PASS`. Latest evidence:
  `20261017T052003Z` (raw output not retained).
- [x] Run `ROUTE-002-topic-not-exist`; Java and Rust both returned `NO_ROUTE`,
  and project-only Compose cleanup passed. Evidence:
  `20261017T051903Z` (raw output not retained).
- [x] Run `HEARTBEAT-001`; Java and Rust sent the same encoded producer
  heartbeat and received Broker version `481`. Project-only Compose cleanup
  passed. Evidence:
  `20261017T053144Z` (raw output not retained).
- [x] Run `PROD-001-sync-send` against the pinned basic profile. Java and Rust
  both returned `SEND_OK`; each client read the other's message by physical offset
  and matched topic, queue, queue offset, and body. Topic and project-only
  Compose cleanup passed. Code/evidence commits: `95e38101`, `398c8ebd`. Evidence:
  `20261017T054628Z` (raw output not retained).
- [x] Run `PROD-002-async-send` against the pinned basic profile. Java's
  callback fired once; Rust's Future resolved once; both returned `SEND_OK` and
  cross-read the other's message by physical offset. Topic and project-only
  Compose cleanup passed. Code/evidence commits: `6f2a0c12`, `aa35cc45`. Evidence:
  `20261017T055737Z` (raw output not retained).
- [x] Run `PROD-003-oneway-send` against the pinned basic profile. Both clients
  returned without a Broker response; the queue max offset advanced to 1 and 2,
  and each language read the other's message at queue offsets 0 and 1. Topic,
  containers, network, and volume cleanup passed. Code commit: `48ed3f58`.
  Evidence: `20261017T063258Z` (raw output not retained).
- [x] Run `PROD-004-batch-send` against the pinned basic profile. Both languages
  returned `SEND_OK` with two physical offset IDs; each read both messages from
  the other client with matching bodies and queue offsets. Topic and Compose
  project/network/volume cleanup passed. Code commit: `8262d722`.
  Evidence: `20261017T065458Z` (raw output not retained).
- [x] After adding the integration package, rerun default and all-feature
  workspace tests, strict workspace Clippy, Maven `verify`, format, workspace
  metadata/license checks, comparator self-test, and 37-row manifest
  validation. Seven publishable packages each list `LICENSE` and `NOTICE`; the
  integration runner remains `publish = false`.
- [ ] Close broad P18-V01 Java source/API/settings/input/error/control-flow and
  reverse-export audit. The 48/48 JSON writer-feature mapping is complete, but
  it does not close the remaining API and behavior inventory.
- [x] Rerun the remaining Phase 18 offline checks on the revision containing
  the integration package: `bash tests/stress/run-offline-smoke.sh` passed 16
  race/leak cases × 3; fuzz smoke passed 105 frame seeds, 11 message seeds,
  1,856 mutations, 146 frame decodes, and 113 message decodes; release examples
  built; the offline bench target compiled. This closes the local server-free
  P18-V02 rerun on the integration-package revision.
- [x] Recheck the server-free P18-V02 gates after the batch probe change on
  `5e677be3` (probe source `8262d722`): default and all-feature workspace tests,
  all-target/all-feature strict Clippy, format, timezone-sensitive unique-ID
  test, workspace metadata/license check, 16 race/leak cases × 3, fuzz smoke,
  release examples, and offline bench compilation all passed. Maven `verify`
  passed on the same probe source commit; final comparator and 37-row manifest
  validation passed after evidence reconciliation.
- [ ] Obtain current-revision Ubuntu/Windows CI evidence and complete P18-V04.
- [ ] Implement and run the remaining 30 of 37 Phase 19 live scenarios, the
  required two-Broker/TLS/ACL/feature profiles, and full `run-local.sh --all`
  evidence reconciliation. `tests/differential/run.sh --all` currently fails
  closed on unimplemented IDs.
- [ ] Complete pre-publication version and registry dependency-resolution
  checks. No publish, tag, or push was performed.
- [ ] Owner checks MAN-01–MAN-04 are blocked pending approved topology,
  identity/resources, maintenance procedure, and workload inputs; MAN-05 is
  pending classification after local matrix execution. See
  [`docs/owner-validation.md`](owner-validation.md).

The seven live PASS results do not constitute Phase 19 acceptance. All
remaining locally reproducible cases stay automatic work; only the
owner-specific checks belong in the manual handoff.

## Phase 18 API audit update (2026-10-17)

- P18-V01 Remoting command flag-helper slice (test commit `f017a1df`): the
  pinned Java 5.5.1 fixture now invokes response/oneway marking and predicates
  for default, oneway, combined, repeated, and unrelated-high-bit cases. The
  Rust fixture test checks flags, command type, and both predicates; only
  `remotingEnumMethodCases.commandFlags` was added to the generated fixture.
  The full Remoting all-feature suite passed (108 unit, 8 codec, 1 fuzz, 7
  security, 107 transport tests, 15 doctests), strict Remoting Clippy, format,
  diff, and Maven `verify` (37 scenarios and fixture/cross-decode checks).
  This closes the flag-helper test slice only. The broad P18-V01 source/API/
  settings/input/error/control-flow and reverse-export audit remains open.
  Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog).
- P18-V01 `RemotingCommand` suspended-state API slice (code commit `811d6dda`):
  Rust now exposes the Java false-default boolean state with getter/setter
  methods, and the invoked Java fixture compares default, true, and reset-to-
  false outcomes. Encoding keeps this non-wire state out of command headers.
  Remoting all-feature tests, strict Clippy, format, diff, and Maven `verify`
  passed. The pinned Java `writeResponse` flow also emits the suspended value
  as the long-polling RPC metric label; Rust has no corresponding remoting
  metric emission yet, so that behavior and the broader P18-V01 audit remain
  open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog).

- P18-V01 `RemotingCommand` public method inventory (test commit `b6129174`):
  the pinned Java fixture now reflects all 57 declared public methods and
  confirms there is no public constructor. The audit identifies remaining
  factory/request-ID helpers, serialization metadata/header helpers, attached
  custom-header accessors, process timer/callback-list methods, command display
  behavior, and long-poll RPC metric emission. The matching crosswalk marks
  each family OPEN or PARTIAL; the inventory is a guard against omission, not
  compatibility evidence for those families. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog).
- P18-V01 `RemotingCommand` protocol-helper slice (code commit `1d0d50b7`):
  Rust now exposes request-ID generation, protocol-type marking/lookup, header
  length extraction, and serialization-type configuration matching the pinned
  Java setting precedence and validation. Invoked Java fixtures compare the
  helper results, including unknown protocol bytes and isolated property/env
  cases. Remoting all-feature tests, strict Clippy, formatting, diff checks,
  and Maven `verify` (37 scenarios, fixture drift and Java/Rust cross-decodes)
  pass. This closes those helper rows only: Java command factories and opaque
  initialization, attached custom-header behavior, process timer/callback-list,
  long-poll RPC metric emission, and the broad P18-V01 audit remain open. The
  complete workspace P18-V02 gate must be rerun after this revision. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T075137Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T075137Z.rlog).
- P18-V01 `RemotingCommand.toString()` slice (code commit `dfcfc890`): Rust
  `Display` matches invoked Java output for null fields, populated state,
  single-entry ext-fields, and negative 32-bit flag formatting. Remoting
  all-feature tests (110 unit, 8 codec, 1 fuzz regression, 7 security, 107
  transport, 15 doctests), strict Clippy, format/diff checks, and Maven `verify`
  (37 scenarios and fixture/cross-decode gates) pass. Multi-entry ext-field
  rendering still follows Rust's sorted map order instead of Java's
  `HashMap` iteration order, so this method remains PARTIAL. Process timer and
  callback-list APIs, factories, long-poll metric emission, the broad P18-V01
  audit, and the final full-workspace P18-V02 rerun remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T075137Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T075137Z.rlog).

- P18-V01 `RemotingCommand` factory slice (code/test commit `a0fe9c3e`,
  2026-10-17): Rust now has request, response, and error factories, typed
  request/response header overloads, class-header `Default` forms, configured
  version handling, and process-wide request-ID increments. Eight invoked Java
  fixture cases compare flags, fields, version, ext-fields, serialization
  type, and request-ID deltas. Full Remoting all-feature tests passed (111
  unit, 8 codec, 1 fuzz regression, 7 security, 107 transport, and 15 doctests);
  strict all-target/all-feature Clippy, formatting, diff checks, and Maven
  `verify` passed (37 scenarios, fixture drift, and Java/Rust cross-decodes).
  `Default` still has Rust-specific language/opaque initialization, custom
  header retention/reflection failure behavior is not equivalent, and the
  multi-entry `toString()` ordering, process timer/callback-list, long-poll
  metric, and broad P18-V01 audit remain open. This is a method-family slice,
  not closure of P18-V01 or the final P18-V02 gate. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T083404Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T083404Z.rlog).
- P18-V02 Remoting TLS test portability follow-up (test commit `48482b49`,
  2026-10-17): the property-file reload test now uses the repository's pinned
  TLS certificate as a local trust anchor. This keeps the test focused on
  property reload and lets it run on hosts without native system trust roots.
  The test and the complete Remoting all-feature suite pass with this change.
- P18-V01 `RemotingCommand` custom-header slice (code/test commit `d0ecaf1e`,
  2026-10-17): typed headers remain attached until explicit materialization or
  encoding; both JSON and RocketMQ codecs include the header fields without
  changing the pre-encode ext-field map. Rust mutable borrows preserve the
  attached header's in-command mutation flow, and cached decode returns the
  same typed instance when requested. Invoked Java cases cover attach/read,
  null clearing, post-attach mutation, direct and cached decoding, ext-field
  merge, `addExtField`, `addExtFieldIfNotExist`, and its null-map exception.
  Rust maps the null-map failure to `ProtocolError` and uses owned typed
  headers with a compile-time trait contract instead of Java aliases and
  reflection. The fixture SHA-256 is
  `48b1ac80746dbb12d22b4da2c6a100b0a19f19e78b65035e5d88243c82d6e582`.
  Remoting all-feature tests pass (112 unit, 8 codec, 1 fuzz regression, 7
  security, 107 transport, and 15 doctests); strict all-target/all-feature
  Remoting Clippy, workspace all-feature check, formatting, diff checks, and
  Maven `verify` pass (37 scenarios, fixture drift, and Java/Rust cross-decodes).
  `Default` initialization, process timer/callback-list, long-poll metric,
  multi-entry `toString()` ordering, and broad P18-V01 remain open. The final
  full-workspace P18-V02 run is still due after the remaining Phase 18 changes.
  Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T090444Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T090444Z.rlog).
- P18-V01 `RemotingCommand` process timer and callback-list slice (pending code
  and documentation commits, 2026-10-17): Rust now exposes timer and callback
  getters/setters, maps the Java stopwatch start/stop/reset state behavior to
  `ProcessTimer`, and attaches a running timer to each inbound decoded command.
  The invoked Java fixture covers null defaults/clears, same-instance access,
  timer state/errors, callback-list mutation, and callback invocation. The Rust
  transport regression confirms inbound requests reach processors with a
  running timer. Fixture SHA-256:
  `8a7ae4f5b258cff9ad87462c5568fe363d079605c487e5327dc672d69981b4c8`.
  Remoting all-feature tests pass (113 unit, 8 codec, 1 fuzz regression, 7
  security, 107 transport, and 15 doctests); focused transport coverage,
  strict Remoting Clippy, workspace all-feature check, formatting, diff checks,
  and Maven `verify` pass (37 scenarios, fixture drift and Java/Rust
  cross-decodes). This closes the timer/callback-list method slice only.
  `Default` initialization, long-poll RPC metric emission, multi-entry
  `toString()` ordering, custom-header representation details, broad P18-V01,
  and final-revision full-workspace P18-V02 remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T091405Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T091405Z.rlog).
- P18-V01 `RemotingCommand::default()` follow-up (pending code and
  documentation commits, 2026-10-17): default commands now match the pinned
  Java constructor's `JAVA` language, zero-valued fields, configured
  serialization type, null transient fields, and process-wide opaque ID
  increment. Factory construction now consumes one opaque ID per command.
  The invoked fixture reflects the protected Java constructor and the Rust
  child-process regression compares its defaults without global request-ID or
  serialization-property interference. Fixture SHA-256:
  `b1dc81c3c70894dfd4e669a59027590bb6d79499a8c34e4c87ca1b6d60cb70ab`.
  Remoting all-feature tests pass (113 unit, 8 codec, 1 fuzz regression, 7
  security, 107 transport, and 15 doctests); strict Remoting Clippy, workspace
  all-feature check, format/diff checks, and Maven `verify` pass (37 scenarios,
  fixture drift, and Java/Rust cross-decodes). This closes the default-state
  difference only. Long-poll RPC metric emission, multi-entry `toString()` map
  ordering, custom-header representation details, broad P18-V01, and final
  full-workspace P18-V02 remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T092123Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T092123Z.rlog).
- P18-V01 Remoting RPC-latency metrics and code-description slice (code commit
  `0d72386c`, 2026-10-17): Rust now has a per-client metrics manager and records
  inbound oneway, successful/failed/cancelled response writes, executor-submit
  failures, long-poll state, and Java-compatible request/response code labels.
  The invoked Java 5.5.1 fixture covers the manager API, metric-view boundaries,
  result labels, 166 request codes, 68 response codes, and unknown-code decimal
  fallback. The Rust fixture differential and loopback test cover supplied
  attributes and observed response metrics. Full Remoting all-feature tests
  pass (115 unit, 8 codec, 1 fuzz regression, 7 security, 107 transport, and 15
  doctests); strict all-target/all-feature Remoting Clippy, format/diff checks,
  and Maven `verify` pass (37 scenarios, fixture drift, and Java/Rust
  cross-decodes). This closes only the Remoting metrics/code-description slice;
  multi-entry `toString()` ordering, custom-header representation details,
  broad P18-V01, and final-revision full-workspace P18-V02 remain open. Execution
  log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T100912Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T100912Z.rlog).
- [x] Run the current-revision server-free P18-V02 gate on `bdf56557` after
  Remoting metrics: default and all-feature workspace tests, workspace
  all-target/all-feature strict Clippy, format/diff, pinned Maven verify (37
  scenarios, fixture drift and cross-decodes), comparator self-test, 37-row
  scenario manifest, race/leak smoke (16 cases × 3), bounded fuzz smoke,
  release examples, workspace bench compilation and benchmark, locked Cargo
  metadata, metadata/license/dependency-direction checks, and workspace package
  file lists all passed. This is current-revision evidence; rerun the gate after
  any later source change. The integration test package remains unpublished;
  Cargo emitted a non-blocking missing-description warning for it during the
  package-list command. Broad P18-V01, current-revision platform CI/P18-V04,
  P18-V03 registry resolution, and locally runnable Phase 19 Broker/Docker
  scenarios remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T100912Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T100912Z.rlog).
- P18-V01 RemotingCommand display-order audit (test commit `a0ee6aef`,
  2026-10-17): an invoked Java 5.5.1 fixture now includes two ext-fields whose
  observed `HashMap` rendering is `{a=lower, Z=upper}`. The Rust differential
  verifies that Java's rendered entries equal the field map, then compares the
  complete display after normalizing only entry order. The pinned source uses
  `HashMap` and its iteration order is unspecified; Rust retains deterministic
  `BTreeMap` order. The API crosswalk now records this as a verified
  representation disposition. It also now reflects completed command defaults,
  process timer/callback APIs, and long-poll metrics. The broad P18-V01 audit
  remains open; direct Java null ext-field key/value inputs are a newly
  identified surface Rust's current `HeaderMap<String, String>` cannot
  represent. Full workspace P18-V02 must be rerun after this test/fixture
  revision. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T103126Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T103126Z.rlog).
- P18-V03 unpublished integration package metadata (commit `9e8476c4`,
  2026-10-17): added a package description to remove Cargo's package-list
  warning. Integration package tests, locked workspace metadata, the
  dependency-direction/license check, and workspace package file listing pass;
  `rocketmq-classic-integration` remains unpublished. Registry dependency
  resolution remains pending after Phase 19.
- P18-V01 Remoting nullable ext-field input slice (code/test commit `c07d3ac2`,
  2026-10-17): `ExtFields` now preserves Java `HashMap` null-key/null-value
  entries, nullable `addExtField` adapters match `putIfAbsent`, JSON encodes a
  null key as the string `"null"` and omits null values, and RocketMQ binary
  map encoding omits null entries. The invoked Java fixture and Rust
  differential pass; the targeted Remoting all-feature test passes, and all
  119 Remoting unit tests pass. The complete Remoting test command then stalled
  while launching the `codec` integration binary at macOS `_dyld_start`; a
  process sample confirmed the launch stall, so the suite is not recorded as a
  pass. Maven `verify` passes all 37 scenarios, fixture checks, and Java/Rust
  cross-decodes. `cargo test --workspace --all-features --locked --no-run`
  completed successfully on this code revision in 38 minutes, compiling every
  workspace test target including `live_client`. Full test execution and the
  current-revision P18-V02 gate remain open. Broad P18-V01 remains open.
- [x] Current-revision server-free P18-V02 rerun on code commit `b6da737e`
  (documentation HEAD `2b059963`): default and all-feature workspace tests,
  all-target/all-feature strict workspace Clippy, formatting, Maven `verify`
  (37 scenarios, fixture drift, and Java/Rust cross-decodes), comparator
  self-test, 37-scenario manifest, 16 deterministic race/leak cases × 3,
  bounded fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations),
  release examples, offline benchmark, locked metadata, workspace
  metadata/license/dependency checks, seven package file lists, and the
  Asia/Taipei Unique ID tests all pass. Rust tests ran with Rust 1.85.0 in an
  offline Linux AArch64 container; `--init` was required to restore unblocked
  signal defaults inherited by the container runtime. The host macOS workspace
  test launch remains blocked by AMFI rejecting its newly built unsigned
  `codec` test binary, so that launch is not counted as a pass. Broad P18-V01
  and current-revision GitHub Ubuntu/native Windows evidence for P18-V04 remain
  open. No Broker, release, push, tag, or publication action occurred.

## Current code revision gate — 2026-10-17

- [x] P18-V02 server-free gate passed on code revision `05448c25`, containing
  `40ebb7ba` (custom-header validation), `fcc001a7` (header-prefix encoders),
  and `05448c25` (heartbeat-tolerant admin test). Default and all-feature
  workspace tests passed with Rust 1.85.0 in `rust:1.85.0-bookworm` using
  `docker run --init` and the offline cargo cache. Workspace all-target,
  all-feature strict Clippy, formatting, and diff checks passed. Maven
  `verify` passed on host OpenJDK 27, including 37 scenarios, Java fixture
  drift checks, and Java/Rust cross-decodes. The differential comparator
  self-test and 37-row manifest validation passed; deterministic race/leak
  smoke passed 16 cases × 3, and fuzz smoke passed 105 frame seeds, 11 message
  seeds, and 1,856 mutations. Release examples and the offline benchmark ran
  successfully. Locked workspace metadata, metadata/license/dependency checks,
  seven publishable-package file lists, and four Asia/Taipei Unique ID tests
  passed. Detailed commands and benchmark observations are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T141001Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T141001Z.rlog).
- These code changes close only the custom-header validation and Remoting
  header-prefix API slices. Broad P18-V01 source/API/settings/input/error/flow
  and reverse-export reconciliation remains open. Current-revision GitHub
  Ubuntu/native Windows evidence for P18-V04 is still absent. The Phase 19
  manifest has 37 cases; 30 local Broker/Docker scenarios and required
  two-Broker/TLS/ACL profiles remain automatic work. Release version and
  registry dependency-resolution checks remain pending before publication.
  Owner-specific MAN checks remain separate final validation. No plan files
  changed; no push, tag, publication, or deployment occurred.

- P18-V01 RemotingCommand identity equality (`4eb99671`, 2026-10-17): the
  pinned Java 5.5.1 fixture resolves `equals` and `hashCode` to `java.lang.Object`
  and shows two distinct commands with the same visible state remain unequal
  and occupy two `HashSet` entries. Rust no longer derives structural
  `PartialEq`/`Eq`; a compile-fail doctest guards that API, and affected codec,
  response-future, and transport assertions now compare wire fields or error
  variants. Fixture SHA-256:
  `a539bc204e2f2c03077af07dad37bc4a58c631ce2fa24d8e9e69f50999e4c9b0`.
  Remoting tests pass (123 unit, 8 codec, 1 fuzz regression, 7 security, 107
  transport, 17 doctests), default/all-feature workspace tests pass on
  `rust:1.85.0-bookworm` with `--init`, strict workspace Clippy, formatting,
  diff checks, and Maven `verify` (37 scenarios, fixture drift, and
  cross-decodes) pass. The final P18-V02 gate remains subject to rerun after the
  remaining Phase 18 implementation work. Broad P18-V01, current-revision
  Ubuntu/Windows P18-V04 evidence, Phase 19 live scenarios, and pre-publication
  checks remain open.

- P18-V01 `ProcessTimer` identity equality (`4858ebdd`, 2026-10-17): Guava
  `Stopwatch` inherits identity equality from `Object`; the invoked fixture
  verifies equal stopped state, getter identity, and two entries for distinct
  timers in a `HashSet`. Rust omits structural `PartialEq`/`Eq` from
  `ProcessTimer` and guards the public trait boundary with a compile-fail
  doctest. Fixture SHA-256:
  `2680e925aa7c21cd5f2f5ea8055ee986bec5fc174601c612cb8ae469481d2cf0`.
  Linux Remoting all-feature tests, strict Remoting Clippy, format/diff checks,
  and Maven verify pass. The macOS all-feature workspace run stalled in Rust
  compilation and was interrupted; the later Linux ARM64 current-revision
  rerun is recorded below. Details for this identity slice are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T144935Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T144935Z.rlog).

## Current code revision gate — 2026-10-17 (`ac0a16f3`)

- [x] P18-V02 server-free local gates pass on code revision `ac0a16f3`,
  including ProcessTimer identity parity (`4858ebdd`). Default and all-feature
  workspace tests and strict all-target/all-feature workspace Clippy pass on
  Rust 1.85.0 in a Linux ARM64 `rust:1.85.0-bookworm` container with
  `--init`. The initial default run exposed a transient `/proc/self/task`
  `ESRCH` race in Linux jstack enumeration; `ac0a16f3` skips entries for
  disappeared tasks, and both workspace modes pass after the fix. Format and
  diff checks pass.
- Maven `verify` passes all 37 scenarios, fixture drift checks and Java/Rust
  cross-decodes. Differential comparator self-test and the 37-row manifest
  validate; deterministic race/leak smoke passes 16 cases × 3; fuzz smoke
  passes 105 frame seeds, 11 message seeds, 1,856 mutations, 146 frame decodes
  and 113 message decodes. Release examples build and the offline benchmark
  runs. Locked Cargo metadata, workspace metadata/license/dependency checks,
  all seven package file lists, and four Asia/Taipei Unique ID tests pass.
  Detailed output and benchmark observations are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T144935Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T144935Z.rlog).
- The macOS workspace test process still stalled during compilation; it is not
  counted as a pass. The Linux ARM64 run closes P18-V02's local server-free
  gate on this code revision. Broad P18-V01 reconciliation, current-revision
  GitHub Ubuntu/native Windows evidence for P18-V04, 30 remaining Phase 19
  Docker/Broker scenarios and required multi-Broker/TLS/ACL profiles, and
  release version/registry checks remain open. Owner-specific final checks
  remain a separate manual handoff. No plan, Java oracle, remote branch, tag,
  or publication target was changed.

## Current code revision gate — 2026-10-17 (`56ca072f`)

- [x] P18-V02 server-free local gate passed on code commit `56ca072f`. Default
  and all-feature workspace tests, all-target/all-feature workspace check and
  strict Clippy, formatting, and diff checks pass. Rust tests/checks ran with
  Rust 1.85.0 in a Linux ARM64 `rust:1.85.0-bookworm` container with `--init`;
  workspace tests were serialized with `--test-threads=1`.
- Maven `verify` passes all 37 scenarios, fixture drift checks and Java/Rust
  cross-decodes. Differential comparator self-test and the 37-row manifest
  validate; deterministic race/leak smoke passes 16 cases × 3; fuzz smoke
  passes 105 frame seeds, 11 message seeds, 1,856 mutations, 146 frame decodes
  and 113 message decodes. Release examples build and the offline benchmark
  runs. Locked Cargo metadata, workspace metadata/license/dependency checks,
  all seven package file lists, and UTC plus Asia/Taipei Unique ID tests pass.
  Maven emitted existing JDK `Unsafe` and reflective-final-field warnings but
  exited successfully. Detailed commands and benchmark observations are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T152044Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T152044Z.rlog).
- Broad P18-V01 source/API/settings/input/error/flow and reverse-export
  reconciliation remains open. Current-revision GitHub Ubuntu/native Windows
  evidence for P18-V04, 30 Phase 19 Docker/Broker scenarios and required
  multi-Broker/TLS/ACL profiles, and release version/registry checks remain
  open. Owner-specific deployment/live Broker checks remain a separate manual
  handoff. No plan, Java oracle, remote branch, tag, publication, Broker
  operation, or deployment was changed.

## Current code revision gate — 2026-10-17 (`86e7cb34`)

- [x] P18-V02 server-free gates pass on `86e7cb34`. The default and
  all-feature workspace test suites, all-target/all-feature workspace check
  and strict Clippy, formatting, and diff checks passed. Rust checks ran with
  Rust 1.85.0 in Linux ARM64 `rust:1.85.0-bookworm` using `--init`.
- `PullMessageRequestHeader` no longer exposes Java-absent `lite_topic`, and
  encoding omits `liteTopic`; decoding ignores an incoming value as an unknown
  field. The protocol crate tests pass, including the updated Java-header
  inventory. Maven `verify` passes all 37 scenarios, fixture checks and
  Java/Rust cross-decodes; comparator self-test and 37-row manifest validation
  pass. Race/leak smoke passes 16 cases × 3 and fuzz smoke passes 105 frame
  seeds, 11 message seeds, 1,856 mutations, 146 frame decodes and 113 message
  decodes. Release examples and the offline benchmark run. Locked metadata,
  workspace metadata/license/dependency checks, all seven package listings
  with `LICENSE`/`NOTICE`, and four Unique ID tests each under UTC and
  Asia/Taipei pass. Detailed results are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T153927Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T153927Z.rlog).
- Broad P18-V01 source/API/settings/input/error/flow and reverse-export
  reconciliation remains open. In particular, the Rust direct custom-header
  decoder still ignores the Java `useFastEncode` hint; Java's reflection path
  catches per-field missing/conversion failures before `checkFields()`, while
  Rust decoding is strict. Current-revision GitHub Ubuntu/native Windows
  evidence for P18-V04, 30 remaining Phase 19 Docker/Broker scenarios and the
  required multi-Broker/TLS/ACL/feature profiles, and pre-publication version
  and registry dependency-resolution checks remain open. Owner-specific
  deployment/live Broker checks remain the separate final manual handoff. No
  plan, Java oracle, remote branch, tag, publication, Broker operation, or
  deployment was changed.

## Current code revision gate — 2026-10-17 (`1db3a1c4`)

- [x] P18-V02 server-free gates pass on code commit `1db3a1c4`. Default and
  all-feature workspace tests, all-target/all-feature workspace check and
  strict Clippy pass on Rust 1.85.0 in Linux ARM64 `rust:1.85.0-bookworm`
  using `--init`. Host format and diff checks pass.
- Maven `verify` passes all 37 scenarios, fixture drift checks, and Java/Rust
  cross-decodes, including the new Pull header fast/reflection fixture for
  missing fields, valid fields, and invalid numeric conversions. Differential
  comparator self-test and manifest validation pass (37 scenarios). Race/leak
  smoke passes 16 cases × 3; fuzz smoke passes 105 frame seeds, 11 message
  seeds, 1,856 mutations, 146 frame decodes, and 113 message decodes. Workspace
  examples and benchmarks compile; the offline benchmark runs on macOS ARM64.
  Locked Cargo metadata, workspace metadata/license/dependency checks, seven
  package listings with `LICENSE`/`NOTICE`, and four Core Unique ID tests under
  both UTC and Asia/Taipei pass. The Java output includes existing JDK
  `Unsafe` and reflective-final-field warnings but exits successfully.
- Pull fast/reflection mode handling closes the specific decoder discrepancy
  recorded under `86e7cb34`; other header mode differences and the broad
  bidirectional P18-V01 source/API/input/error/flow and reverse-export audit
  remain open. Current-revision Ubuntu/native Windows evidence for P18-V04,
  30 remaining Phase 19 Docker/Broker scenarios and required profiles, and
  pre-publication version/registry checks remain open. Owner-specific final
  production checks remain a separate manual handoff. No plan, Java oracle,
  remote branch, tag, publication, Broker operation, or deployment was changed.

## Current code revision gate — 2026-10-17 (`01e02a5c`)

- [x] P18-V02 server-free local gates pass on code commit `01e02a5c`. Default
  and all-feature workspace tests pass in a clean Linux ARM64
  `rust:1.85.0-bookworm` container with serialized tests. All-target,
  all-feature check and strict Clippy pass with Rust 1.85.0 on macOS ARM64;
  formatting and diff checks pass.
- `SendMessageResponseHeader` now preserves Java nullable `msgId`, `queueId`,
  and `queueOffset` on direct decode, including Java's fast/reflection
  conversion behavior. The send owner still requires queueId and queueOffset;
  `SendResult.offset_msg_id` preserves absent `msgId` as `None`. Its invoked
  Java fixture SHA-256 is
  `c2962fc0c34868c36dd67ef7ef33e4694c59904b679de665ef401d302fe3818a`.
- Maven `verify` passes all 37 scenarios, fixture-drift checks, and Java/Rust
  cross-decodes. Differential self-test and the 37-row manifest validate;
  race/leak smoke passes 16 cases × 3, and fuzz smoke passes 105 frame seeds,
  11 message seeds, 1,856 mutations, 146 frame decodes, and 113 message
  decodes. Release examples and workspace benches compile; the offline
  benchmark runs. Locked metadata, package/license/dependency checks, all
  seven package listings with `LICENSE`/`NOTICE`, and four Unique ID tests
  under both UTC and Asia/Taipei pass. A first all-feature run had one stack
  capture test fail to find an unblocked signal; the isolated test, full
  consumer suite, and final default/all-feature workspace reruns passed.
  Detailed results are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T184946Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T184946Z.rlog).
- Broad P18-V01 bidirectional Java source/API/settings/input/error/flow
  reconciliation and Rust reverse-export audit remain open. Current-revision
  GitHub Ubuntu/native Windows evidence for P18-V04, 30 remaining Phase 19
  Docker/Broker scenarios and required multi-Broker/TLS/ACL profiles, and
  pre-publication version/registry dependency checks remain open. Owner-only
  production checks are the separate final manual handoff. No plan, Java
  oracle, remote branch, tag, publication, Broker operation, or deployment
  was changed.

## Latest code revision follow-up — 2026-10-17 (`227876c1`)

- [x] Default and all-feature workspace tests pass in the Linux
  `rust:1.85.0-bookworm` container. All-target/all-feature `cargo check` and
  strict Clippy pass there; formatting and diff checks pass.
- [x] Maven `verify` passed all 37 scenarios and fixture-drift/cross-decode
  checks on the worktree containing this code revision.
- [ ] Refresh the remaining P18-V02 server-free evidence on this revision:
  differential comparator and manifest checks, race/leak and fuzz smoke,
  release examples and benchmark build, locked metadata/package checks, and
  timezone-specific Unique ID checks. The previous complete run is recorded on
  `01e02a5c`; these later V2 header edits have not yet had those gates repeated.

## Pull response header follow-up — 2026-10-17 (`fcc0f3f1`)

- [x] P18-V01 `PullMessageResponseHeader` nullable and mode-specific decoding:
  the Java 5.5.1 fixture covers absent wrappers, valid numbers, and malformed
  numeric fields in fast and reflection modes. Rust preserves nullable wrapper
  values on direct decode, rejects invalid numbers in fast mode, and ignores an
  individual reflection conversion failure as Java does. The Pull RPC validates
  `nextBeginOffset`, `minOffset`, `maxOffset`, and `suggestWhichBrokerId` at the
  Java-equivalent unboxing boundary. Fixture SHA-256:
  `9ba19f1b5394e7bd6e60366770990e034e75e98f2ecff04c82059d712dc8db59`.
  Maven verify (37 scenarios, fixture drift and cross-decodes), Linux default
  and all-feature workspace tests, all-target/all-feature check and strict
  Clippy, formatting, and diff checks pass. Two test expectations were corrected
  to match Java: direct decode preserves null, and the wrapper fields are
  nullable in the validation matrix. This closes this header slice only.
- [x] P18-V01 `FastCodesHeader` production-source inventory: pinned Java
  `remoting` contains exactly four implementations, and all four have invoked
  fast/reflection fixtures plus Rust comparisons: Pull request, Pull response,
  Send response, and Send V2 request headers. Rust provides per-mode decoders
  for each; Send V2 also covers inherited fields and V1 conversion. This closes
  that bounded inventory only. Other headers' generic reflection decoding and
  broad P18-V01 reconciliation remain open.
- [x] P18-V02 server-free gates complete for code revision `fcc0f3f1`
  (documentation HEAD `29b38b4b`): comparator self-test and 37-row manifest;
  16 race/leak cases × 3; fuzz smoke (105 frame seeds, 11 message seeds, 1,856
  mutations, 146 frame decodes, 113 message decodes); release examples;
  workspace benchmark compile and offline run; locked Cargo metadata, workspace
  metadata/license/dependency checks, and all seven product package listings;
  four Core Unique ID tests under each of UTC and Asia/Taipei. Formatting and
  diff checks pass. The benchmark ran on macOS ARM64, Apple M5 Max, Rust 1.85.0:
  frame encode/decode 1,954.6 ns/iteration, Zstd message decode 1,038.8 ns,
  Producer request construction 1,181.7 ns, route lookup 15.5 ns, and loopback
  pending completion 64,728.4 ns. These are observations, not thresholds.
- Broad P18-V01 reconciliation, current-revision GitHub Ubuntu/native Windows
  P18-V04 evidence, 30 Phase 19 local Docker/Broker scenarios and required
  profiles, and pre-publication version/registry checks remain open. Owner-only
  production checks remain separate final manual items.

## Consumer offset header follow-up — 2026-10-17 (3beabab0)

- [x] P18-V01 typed consumer-offset decoder slice: the Java 5.5.1 fixture compares query/update request and query response headers with missing fields, malformed numbers, malformed setZeroIfNotFound, and valid values under both useFastEncode hints. Rust retains nullable wrapper fields, skips per-field reflection failures, and validates response offset at the RPC result boundary. Fixture SHA-256: 2e911e6369ef5260ac73ad7ecbd3db9b38d190f8b080b5d36fc46997e7f20ddb. Code commit: b59431e4.
- [x] P18-V02 server-free gates pass on code revision 3beabab0. Linux ARM64 rust:1.85.0-bookworm default and all-feature workspace tests, all-target/all-feature check and strict Clippy, and Maven verify (37 scenarios, fixture drift and Java/Rust cross-decodes) pass. Comparator self-test and the 37-row manifest pass; 16 race/leak cases × 3 and fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations, 146 frame decodes, 113 message decodes) pass. Release examples and the offline benchmark compile and run. Locked Cargo metadata, workspace metadata/license/dependency checks, seven package-content listings, UTC/Asia-Taipei Unique ID tests, format, and diff checks pass. The benchmark ran on Linux ARM64 with Rust 1.85.0; results are observations, not thresholds.
- [x] Stabilized the live-timeout regression after the first parallel workspace run exposed a route-refresh timeout: the test now loads and checks the Broker route before mutating the timeout, then applies the Java fixture's 150 ms value to the Broker RPC. All 17 runtime_lookup integration tests pass. Test-only commit: 3beabab0.
- A macOS Remoting package run was stopped after its codec test child remained in dyld startup before Rust main; a live process sample showed _dyld_start. The complete default/all-feature workspace suites passed in the Linux container on 3beabab0.
- Broad P18-V01 bidirectional Java/Rust source/API/settings/input/error/flow and reverse-export audit remains open. Current-revision GitHub Ubuntu/native Windows P18-V04 evidence, 30 Phase 19 Docker/Broker scenarios and required profiles, and pre-publication version/registry checks remain open. Owner-specific production checks remain the separate final manual handoff. Plans and the read-only Java oracle were unchanged.

## Offset-boundary reflection follow-up and current gate — 2026-10-17 (`9513e4da`)

- [x] P18-V01 offset-boundary response-header decoding: Java 5.5.1 direct
  reflection leaves absent or malformed boxed `Long` fields null per field,
  regardless of the fast-encode hint. Rust now preserves those nullable values
  for `SearchOffsetResponseHeader`, `GetMaxOffsetResponseHeader`,
  `GetMinOffsetResponseHeader`, and `GetEarliestMsgStoretimeResponseHeader`;
  each RPC owner checks the required value at the Java-equivalent unboxing
  boundary. Invoked fixture SHA-256:
  `c4891f046639eb0f9a82e221b1605d296aed1314fb70e37c22ea514d240a9e95`.
- [x] P18-V02 server-free local gates pass on code revision `9513e4da` in a
  serialized Linux ARM64 `rust:1.85.0-bookworm` container: default/all-feature
  workspace tests, all-target/all-feature check and strict Clippy, Maven verify
  (37 scenarios, fixture drift and Java/Rust cross-decodes), differential
  self-test/manifest validation, 16 race/leak cases × 3, fuzz smoke, release
  examples, benchmark build/run, locked metadata/package checks, workspace
  metadata/license/dependency checks, seven package listings with `LICENSE` and
  `NOTICE`, UTC/Asia-Taipei Unique ID tests, formatting, and diff checks. A
  first parallel workspace run had one Consumer stack-capture failure because
  the environment exposed no unblocked signal; serialized default/all-feature
  reruns passed. Benchmark values are observations, not thresholds.
- Broad P18-V01 source/API/settings/overload/input/error/flow and Rust
  reverse-export reconciliation remains open. Current-revision GitHub Ubuntu
  and native Windows P18-V04 evidence, 30 Phase 19 Docker/Broker scenarios and
  required multi-Broker/TLS/ACL profiles, and pre-publication version/registry
  checks remain open. Owner-only production connectivity, credentials/CA,
  approved HA/fault injection, and capacity/soak checks remain the final manual
  handoff. No plan, Java oracle, remote branch, tag, publication, Broker
  operation, or deployment was changed.

## Remoting suspended response metric — 2026-10-17 (`0fdbc1d4`)

- [x] Closed the P18-V01 long-poll metric slice. Pinned Java
  `NettyRemotingAbstract.writeResponse` records `request.isSuspended()` in the
  `is_long_polling` RPC latency attribute. Rust's inbound `send_response`
  builds the same attribute from the request and records it after response
  writes or for oneway requests. The transport integration test marks an
  inbound request suspended in a hook and verifies the recorded response
  metric contains `is_long_polling=true`.
- Focused verification passed:
  `cargo test --locked -p rocketmq-classic-remoting --test transport
  p3_v03_inbound_dispatch_and_reordered_parallel_responses`, formatting, and
  `git diff --check`. Test-only commit: `0fdbc1d4`.
- Earlier checklist notes that listed long-poll metric emission as open are
  superseded by this evidence. Broad P18-V01 remains open for its other API and
  behavior rows.

## POP change-invisibility response header — 2026-10-17 (`dff482d9`)

- [x] Closed the missing `ChangeInvisibleTimeResponseHeader` public typed
  header slice. Pinned Java 5.5.1 declares its constructor, `get/setPopTime`,
  `get/setInvisibleTime`, and `get/setReviveQid`, with identity-based inherited
  equality. Rust now exports and directly decodes/encodes the header, retains
  Java's independent primitive zero defaults for absent or malformed fields,
  and matches signed limits and BMP decimal digits. The invoked fixture covers
  both decode hints, null/empty ext-field maps, malformed and valid fields,
  identity, accessors, and encoded fields. SHA-256:
  `967d3146d0da1467714c96a1fa7464c7375888e2ba09f6fc0c097126bab67c2a`.
- [x] Current-revision P18-V02 server-free gates pass on `dff482d9`:
  default/all-feature workspace tests, all-target/all-feature check and strict
  Clippy, UTC Maven `verify` (37 scenarios, fixture drift and cross-decodes),
  differential self-test and 37-row manifest, 16 deterministic race/leak cases
  × 3, fuzz smoke (106 frame seeds, 11 message seeds, 1,872 mutations, 156
  frame decodes, 110 message decodes), release examples, all workspace bench
  compilation and offline benchmark run, locked Cargo metadata, metadata/
  license/dependency-direction checks, seven package-content listings,
  UTC/Asia-Taipei Unique ID tests, formatting, and diff checks.
- The benchmark ran on macOS ARM64, Apple M5 Max, Rust 1.85.0: frame encode /
  decode 2,140.8 ns per iteration, Zstd message decode 1,104.1 ns, Producer
  request construction 1,191.8 ns, route lookup 16.4 ns, and loopback pending
  completion 64,665.5 ns. Allocation observations were 4,676; 197,493; 3,665;
  0; and 9,546 bytes per iteration. These are observations, not thresholds.
- Broad P18-V01 source/API/settings/overload/input/error/flow and Rust
  reverse-export reconciliation remains open. Current-revision GitHub Ubuntu
  and native Windows P18-V04 evidence, 30 Phase 19 Docker/Broker scenarios and
  required profiles, and pre-publication version/registry checks also remain
  open. Owner-only production checks remain the final manual handoff. No plan,
  remote branch, tag, publication, Broker operation, or deployment was touched.

## Heartbeat request header API follow-up — 2026-10-17

- [x] Closed the P18-V01 `HeartbeatRequestHeader` slice against the pinned
  Java 5.5.1 source. `sendHeartbeat` and `sendHeartbeatV2` use this
  `RpcRequestHeader` subtype, with inherited namespace, broker-name, and
  oneway fields. Rust now exports the typed header, preserves all four ext
  fields and inherited equality/hash/toString behavior, and uses it for both
  heartbeat RPCs. Java typed-header fixture SHA-256:
  `d52b9cfbb0e640702e0eb6cea45430a237ac7120513854b36f677447fc3cfe46`.
- [x] Pinned Java Maven `verify` passes all 37 scenarios and cross-decodes 56
  Rust typed-header frames. Focused Rust verification passes the 3 header
  inventory tests, the request-header identity test, the 14 Core extension
  tests, and strict all-target/all-feature Clippy for Protocol and Core.
- Broad P18-V01 reconciliation remains open. Current-revision GitHub Ubuntu
  and native Windows P18-V04 evidence, 30 Phase 19 local Broker scenarios and
  required profiles, and pre-publication registry resolution remain open.

## Current-revision P18-V02 gate — 2026-10-17 (`5e5775fa`)

- [x] The current-revision server-free gate passes after adding
  `HeartbeatRequestHeader` to the Protocol header-validation matrix. The first
  full default workspace run exposed the missing row; the focused matrix test
  and both complete default and all-feature workspace suites passed after the
  correction. Commit: `5e5775fa`.
- [x] Also passed all-target/all-feature workspace check and strict Clippy,
  format and diff checks, UTC Maven `verify` (37 scenarios, fixture drift,
  56 typed-header and 14 typed-body cross-decodes), differential comparator
  self-test and 37-row manifest validation, 16 race/leak cases × 3, fuzz smoke
  (107 frame seeds, 11 message seeds, 1,888 mutations, 165 frame decodes,
  116 message decodes), release examples, workspace bench compilation and
  offline benchmark, locked Cargo metadata, workspace metadata/license/
  dependency-direction checks, workspace package-content listing, and
  UTC/Asia-Taipei unique-ID tests. Benchmark observations and toolchain details
  are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T230853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T230853Z.rlog).
- Broad P18-V01 Java/Rust source/API/settings/input/error/flow and
  reverse-export audit, current-revision Ubuntu/native Windows P18-V04 CI
  evidence, final P18-V03 package review, and Phase 19 live scenarios/profiles
  remain open. This rerun closes P18-V02 only.

## Current-revision P18-V02 and OpenTelemetry Attributes API — 2026-10-17 (`7e7689f6`)

- [x] P18-V01 OpenTelemetry 1.47 `AttributeKey`, immutable `Attributes`, and
  `AttributesBuilder` surface compared with the invoked Java 5.5.1 fixture.
  Rust maps all eight scalar/array kinds, typed lookup, `to_builder`, `put_all`,
  type-aware `remove`, and `remove_if`. Fixture SHA-256:
  `e15d03aac196916a0592a21a8deb2440fbce694e20bc3b0e050ba88e798a966e`.
- [x] Full P18-V02 server-free rerun on source commit `7e7689f6`: default and
  all-feature locked offline workspace tests; all-target/all-feature workspace
  check and strict Clippy; Remoting all-feature tests; Maven `verify` (37
  scenarios, fixture checks, 56 typed-header and 14 typed-body cross-decodes);
  comparator self-test and 37-row manifest; 16 race/leak cases × 3; fuzz smoke
  (107 frame seeds, 11 message seeds, 1,888 mutations); release examples,
  workspace benchmark build/run, locked Cargo and workspace metadata/package
  checks, UTC/Asia/Taipei Unique ID tests, formatting, and diff checks. The
  benchmark ran on macOS ARM64 Apple M5 Max with Rust 1.85.0; observations are
  recorded in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T235517Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T235517Z.rlog).
- Broad P18-V01 source/API/settings/input/error/control-flow and reverse-export
  reconciliation remains open. Current-revision Ubuntu/native Windows CI
  evidence for P18-V04 and final P18-V03 package/provenance review remain open.
  The Phase 19 live Broker matrix remains a separate phase handoff; owner-only
  production checks remain in the final manual handoff.

## CreateTopicRequestHeader reflection decoding — 2026-10-18 (`4f7f9435`)

- [x] Closed a P18-V01 header-decoding slice against the pinned Java 5.5.1
  implementation. `CreateTopicRequestHeader` does not implement
  `FastCodesHeader`, so both decode hints use reflection. Conversion failures
  are caught per field, nullable Java fields remain null, Boolean parsing maps
  non-`true` text to false, and `checkFields()` runs after the loop. Rust now
  models the corresponding nullable payload fields as `Option`, skips failed
  numeric conversions, preserves the Java default values for `order`/`force`,
  and validates `topicFilterType` after decoding. The invoked fixture covers
  both mode hints, valid/missing/malformed fields, and final validation. SHA-256:
  `20e0a1c2dd78af55b220976ad6faecc37673d0d6951954a4a0c2bc433a7053a7`.
- [x] Refreshed current-revision P18-V02 server-free gates on `4f7f9435`:
  default/all-feature workspace tests, all-target/all-feature check and strict
  Clippy, Maven `verify` (37 scenarios, fixture checks, 56 typed-header and 14
  typed-body cross-decodes), differential self-test and 37-row manifest,
  deterministic race/leak smoke (16 cases × 3), bounded fuzz smoke (107 frame
  seeds, 11 message seeds, 1,888 mutations), release examples, workspace bench
  compilation and offline benchmark, locked Cargo/workspace metadata and
  package-content checks, UTC/Asia-Taipei Unique ID tests, formatting, and diff
  checks. Full command/results are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261018T002552Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261018T002552Z.rlog).
- The broader bidirectional P18-V01 audit remains open. Current-revision
  Ubuntu/native Windows P18-V04 evidence and the final P18-V03 package and
  provenance review remain open. Phase 19 Broker scenarios/profiles and owner
  checks remain separate obligations; no live Broker was contacted.

## EndTransactionRequestHeader reflection decoding — 2026-10-18 (`0961d24d`)

- [x] Closed this P18-V01 header-decoding slice against the pinned Java 5.5.1
  implementation. Both decode-mode hints use the reflection path. Per-field
  missing/invalid conversion failures are skipped; nullable offset, group, and
  message-ID values remain absent; `fromTransactionCheck` defaults to false;
  malformed Boolean values decode as false; and commit state is validated
  after field decoding. The invoked fixture covers 13 cases in both modes.
  Fixture SHA-256: `78605a0c0cd70f33553813559e2e6b4c9fe82589a08acd83ca18a9ebc616ab5d`.
- [x] Protocol header tests (12), Producer tests, Core invoked-producer fixture,
  Remoting mode fixture, Maven `verify`, affected-crate strict Clippy, format,
  and diff checks pass on code commit `0961d24d`. The Maven run verified the
  new Java fixture, 37 differential scenarios, 56 typed-header cross-decodes,
  and 14 typed-body cross-decodes.
- This closes only the EndTransaction decode slice. Broad P18-V01 remains open;
  the full workspace P18-V02 gate must be refreshed after remaining Phase 18
  implementation work. Current-revision Ubuntu/native Windows P18-V04 evidence
  and final P18-V03 package/provenance review also remain open. The Phase 19
  runner worktree changes were excluded from this commit.

## Query/admin header reflection decoding — 2026-10-18 (`66334aa9`)

- [x] Closed a P18-V01 query/admin decoder slice against pinned Java 5.5.1. `QueryMessageRequestHeader`, `QueryMessageResponseHeader`, and `ViewMessageRequestHeader` use Java's reflection decoder under both `useFastEncode` hints; Rust now preserves nullable wrapper fields and per-field malformed numeric outcomes. `ViewMessageRequestHeader` keeps Java identity equality, and Core checks the nullable query index timestamp at the Java-equivalent unboxing boundary. The invoked fixture covers both modes and valid, missing, and malformed fields. SHA-256: `abc37ff23edd774910af9093b96fce95ce5e2270c5db7996fb0607df1ef3cc68`.
- [x] Maven `verify` passed (37 offline scenarios, fixture drift, 56 typed-header cross-decodes, 14 typed-body cross-decodes). Protocol all-feature tests passed (128 unit tests, header matrix/inventory, 44 doctests); Remoting all-feature tests passed (137 unit, 8 codec, 1 fuzz-regression, 7 security, 107 transport, 18 doctests). Core `admin_rpc` (19), Producer admin (11), and Consumer admin (9) tests passed; strict all-target/all-feature Clippy passed for the five affected crates. `cargo fmt --all -- --check` and `git diff --check` pass.
- [ ] Refresh full-workspace P18-V02 gates after the latest code slice. Broad P18-V01 remains open; current-revision Ubuntu/native Windows P18-V04 evidence and final P18-V03 package/provenance review remain open. Phase 19 still lacks 30 of 37 local scenario runners, the required profiles, and a passing `run-local.sh --all` reconciliation. Pre-publication release-version and registry dependency checks remain pending. Owner-specific manual checks remain a separate handoff.

## P18-V01 `MqClientAdmin` 19-method inventory reconciliation — 2026-10-24

- [x] The 19 public methods declared by Java 5.5.1 `client/MqClientAdmin.java`
  now have source-backed typed RPC slices and invoked Java fixtures. Code
  commits `f05833a7` and `4ddd9266` complete the final `getConsumerRunningInfo`
  slice and expose all 19 methods through `MQClientApi::mq_client_admin()`;
  the Rust API test checks the exact method-name set and compile-calls every
  wrapper. This supersedes the 2026-10-18 discovery note, which correctly
  identified the initial inventory gap. The Phase 17 matrix still records the
  remaining full defaults/input/error/DTO API and reverse-export audit; broad
  P18-V01 remains open.

## P18-V01 `queryTopicsByConsumer` slice — 2026-10-18

- [x] Code commit `07865bde` adds `MQClientApi::query_topics_by_consumer`,
  `QueryTopicsByConsumerRequestHeader`, and Java-compatible `TopicList` defaults,
  null handling, Set aliasing, and JSON fields. The invoked Java fixture covers
  both decoder mode hints, inherited request fields, and `TopicList` accessor
  identity/default behavior. Rust covers the decoded fields and a scripted
  successful request/response exchange.
- [x] Targeted Protocol, Remoting, and Core tests pass; strict all-target,
  all-feature Clippy passes for those three crates; Maven `verify` passes all
  37 scenarios and Java/Rust cross-decodes 57 typed headers and 14 typed bodies.
  Formatting and diff checks pass.
- [x] Follow-up code commit `59c0d8fa` adds a scripted `TOPIC_NOT_EXIST`
  response test for the error code, peer address, and remark mapping. The Core
  test, strict all-target/all-feature Core Clippy, format, and diff checks pass.
- The other 18 `MqClientAdmin` methods and broad P18-V01 reverse/API/settings
  reconciliation remain open. Refresh the full current-revision P18-V02
  workspace gates after these changes. Current-revision Ubuntu/Windows CI and
  final P18-V03 package/provenance review also remain open.

## P18-V01 `querySubscriptionByConsumer` slice — 2026-10-18 (`2ac9351b`)

- [x] Added `MQClientApi::query_subscription_by_consumer`, the typed request
  header, and `QuerySubscriptionResponseBody`. The body retains Java's nullable
  `SubscriptionData`, null-omitting JSON, and shared getter alias. Invoked Java
  fixtures cover the response DTO and both query-header decode hints, including
  missing and malformed Boolean fields. The Rust header inventory and
  validation matrix include both `queryTopicsByConsumer` and
  `querySubscriptionByConsumer`.
- [x] Protocol all-feature tests pass (130 unit tests, header inventory and
  validation, 46 doctests); Remoting all-feature tests pass (137 unit tests,
  107 transport tests, 18 doctests); Core admin RPC tests pass (22 cases).
  Strict all-target/all-feature Clippy passes for Protocol, Remoting, and Core.
  Maven `verify` passes 37 scenarios, fixture drift checks, and Java/Rust
  cross-decodes for 58 typed headers and 14 typed bodies. Formatting and diff
  checks pass.
- The other 17 `MqClientAdmin` methods and broad P18-V01 reconciliation remain
  open. Refresh the full-workspace P18-V02 gate after remaining implementation
  work. Current-revision Ubuntu/native Windows P18-V04 evidence, final P18-V03
  package/provenance review, Phase 19 local Docker/Broker scenarios and
  required profiles, and pre-publication checks also remain open. Owner-only
  production checks stay in the separate manual handoff.

## P18-V01 `queryTopicConsumeByWho` admin RPC — 2026-10-18 (`c091d507`)

- [x] Added `MQClientApi::query_topic_consume_by_who`, the typed request
  header, and Java-compatible `GroupList` body for request code 300. Invoked
  Java fixtures cover the header under both decode hints and the DTO's empty
  default, nullable Set/elements, JSON shape, and getter alias identity.
  Scripted Core RPC cases cover success, nullable response data, and Broker
  error mapping. Fixture SHA-256 values: `group-list.json`
  `d033bf2e2cf2e866ba854bf2152ab21f66f43a2bb0212caaf9dbdc4d88f5c38e`;
  request-header identity `2849daef72f0bbc29c64f311b8dc41761602845ef1256f12c6937e2ccdbea48f`.
- [x] Protocol all-feature tests pass (131 unit tests, header inventory and
  validation, 47 doctests); the Remoting query/admin decode-mode fixture and
  Core `admin_rpc` suite (25 tests) pass. Strict all-target/all-feature
  Clippy passes for Protocol, Remoting, and Core. Maven `verify` passes all
  37 scenarios and fixture checks, with 59 typed-header and 14 typed-body
  cross-decodes. Formatting and diff checks pass.
- The `RpcRequestHeader` identity fixture now covers 35 classes. This brings
  the `MqClientAdmin` source-backed method count to three complete slices,
  with 16 methods remaining to reconcile.
- [ ] Refresh full-workspace P18-V02 gates after this code slice. Broad
  P18-V01 reconciliation remains open; current-revision Ubuntu/native Windows
  P18-V04 evidence and final P18-V03 package/provenance review remain open.
  Phase 19 still needs the remaining 30 local scenario runners, required
  profiles, and passing `run-local.sh --all` reconciliation. Pre-publication
  version/registry checks remain pending; owner-environment checks stay in the
  separate manual handoff.

## P18-V01 `getTopicStatsInfo` admin RPC — 2026-10-18 (`e53ad60c`)

- [x] Added `MQClientApi::get_topic_stats_info`, request code 202's typed
  `GetTopicStatsInfoRequestHeader`, and Java-compatible `TopicStatsTable` and
  `TopicOffset` DTOs. Invoked Java fixtures cover public methods, primitive
  defaults, nullable map/accessor aliasing, MessageQueue object-key JSON and
  null map-value omission. Rust RPC regressions cover success, absent/JSON-null
  bodies, Broker errors, and malformed JSON. Fixture SHA-256:
  `12c3a17f32ffb2b3b60e1e27d57ce8a96a199cb3ba0c7b1201790461b5ecd669`.
- [x] Protocol all-feature tests passed (132 unit tests, all integration
  targets, 49 doctests); Core `admin_rpc` passed (28 tests). Strict
  all-target/all-feature Clippy passed for Protocol, Remoting, and Core after
  deriving the private fixture fields' `Default` implementation. The first
  Clippy run reported `derivable_impls`; the correction removed the warning.
  Formatting and `git diff --check` pass.
- [x] Pinned Maven `verify` passed all 37 scenarios, fixture drift checks,
  Java/Rust typed-header cross-decodes (60) and typed-body cross-decodes (14).
  Code commit: `e53ad60c`.
- The source-backed `MqClientAdmin` count is now 4/19; the other 15 methods
  and the broad P18-V01 public API/settings/input/error/flow/reverse-export
  audit remain open. Full-workspace P18-V02 must be refreshed after the
  remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and `run-local.sh --all`, and pre-publication
  version/registry checks remain open. Owner-only production validation stays
  in the separate manual handoff.

## P18-V01 `queryConsumeTimeSpan` admin RPC — 2026-10-18 (`72a37b17`)

- [x] Added an invoked Java 5.5.1 `MqClientAdminImpl.queryConsumeTimeSpan`
  fixture for request code 303. It records the direct broker address and
  timeout, all typed request fields, the decoded `QueueTimeSpan` result, and
  `MQClientException` code/remark. Fixture SHA-256:
  `5689eea4d53809d6ce2caa6bebc14ba4ebeb8b49b8fffe3000432bbe73dfaa4e`.
- [x] The Rust Core method now sends to the supplied address directly, as the
  Java `MqClientAdmin` adapter does. Push Consumer applies the configured VIP
  mapping before invoking Core, preserving the Java push facade's Broker
  address behavior. Tests cover the direct method with VIP enabled, success
  decoding and Broker error mapping, plus Push Consumer's VIP route.
- [x] Core query-time-span tests passed (2); Core `admin_rpc` passed (29); the
  focused Consumer VIP route test passed. Strict all-target/all-feature
  Clippy passed for Core and Consumer. Pinned Maven `verify` passed all 37
  scenarios and fixture checks, including 60 typed-header and 14 typed-body
  cross-decodes. Formatting and `git diff --check` pass. Code commit:
  `72a37b17`.
- The source-backed `MqClientAdmin` method count is now 5/19; the remaining 14
  methods and broad P18-V01 API/settings/input/error/flow/reverse-export audit
  remain open. Full-workspace P18-V02, current-revision Ubuntu/native Windows
  P18-V04, final P18-V03 package/provenance review, Phase 19 local Docker
  scenarios/profiles and full runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation remains the
  separate manual handoff.

## P18-V01 `updateOrCreateTopic` admin RPC — 2026-10-18 (`24eae6bf`)

- [x] Extended the invoked Java `MqClientAdminImpl` fixture with
  `updateOrCreateTopic`, request code 17, full `CreateTopicRequestHeader`
  fields, direct address, null request body, successful completion and
  `MQClientException` code/remark. The fixture is
  `fixtures/admin/query-consume-time-span.json` (shared with the preceding
  method slice), SHA-256
  `16939c20d844e02921a2634dc32af32521b8a36b5d0de27b256ae88c777056aa`.
- [x] Rust's existing `MQClientApi::create_topic` matches the Java request
  and response paths. Core admin RPC tests now compare the successful header
  and Broker error to the Java fixture; the existing permission-error test
  remains. Core `admin_rpc` passed all 30 tests; strict all-target/all-feature
  Core Clippy, formatting, and diff checks passed.
- [x] Pinned Maven `verify` passed after fixture regeneration: 37 scenarios,
  fixture drift checks, 60 typed-header cross-decodes and 14 typed-body
  cross-decodes. Test/fixture commit: `24eae6bf`.
- The source-backed `MqClientAdmin` method count is now 6/19; 13 methods and
  the broad P18-V01 public API/settings/input/error/flow/reverse-export audit
  remain open. Full-workspace P18-V02, current-revision Ubuntu/native Windows
  P18-V04, final P18-V03 package/provenance review, Phase 19 local Docker
  scenarios/profiles and full runner reconciliation, and pre-publication
  version/registry checks remain open. Owner-only validation stays separate.

## P18-V01 `deleteTopicInBroker` admin RPC — 2026-10-18 (`74c52143`)

- [x] Added `DeleteTopicRequestHeader` with Java's nullable topic and inherited
  `RpcRequestHeader`/`TopicRequestHeader` fields. Java's `checkFields()` is a
  no-op, and inherited equality/hash behavior only uses `ns`, `nsd`, `bname`,
  and `oway`. Added `MQClientApi::delete_topic_in_broker` using request code
  215, the direct address, and no request body.
- [x] The invoked Java 5.5.1 fixture covers `deleteTopicInBroker` request
  fields, timeout, success completion, and `MQClientException` response code
  and remark. Rust scripted Core tests compare the request and Broker error to
  the fixture. Java/Rust typed-header cross-decode and the request-header
  identity inventory include the new header. Shared admin fixture SHA-256:
  `2a113cbc01a9e5bdce353b6705c6ebd734a5dd33832b147135eebdf6030d177b`.
- [x] Protocol all-feature tests passed (132 unit tests, all integration
  targets, 49 doctests); Core `admin_rpc` passed all 32 tests. Strict
  all-target/all-feature Clippy passed for Protocol, Remoting, and Core.
  Pinned Maven `verify` passed all 37 scenarios and fixture checks, including
  61 typed-header and 14 typed-body cross-decodes. Formatting and diff checks
  passed.
- The source-backed `MqClientAdmin` method count is now 7/19; 12 methods and
  broad P18-V01 reconciliation remain open. Refresh full-workspace P18-V02
  after the remaining implementation work. Current-revision Ubuntu/native
  Windows P18-V04 evidence, final P18-V03 package/provenance review, Phase 19
  local Docker scenarios/profiles and runner reconciliation, and
  pre-publication version/registry checks also remain open. Owner-only
  validation stays in the separate manual handoff.

## P18-V01 `deleteTopicInNameserver` admin RPC — 2026-10-18 (`fb1e43b5`)

- [x] Added `DeleteTopicFromNamesrvRequestHeader` with nullable `topic`,
  `clusterName`, and inherited topic/RPC fields. Java's `checkFields()` is a
  no-op, and inherited equality/hash behavior uses only `ns`, `nsd`, `bname`,
  and `oway`. Added `MQClientApi::delete_topic_in_nameserver` using request
  code 216, the supplied NameServer address, and no request body.
- [x] The invoked Java 5.5.1 fixture covers the exact method signature, header
  fields, timeout, success completion, and `MQClientException` response code
  and remark. Rust scripted Core tests compare the request and error result to
  the fixture. The header is included in Java/Rust typed-header cross-decode
  and request-header identity inventory. Shared admin fixture SHA-256:
  `ee0c842383a4150957dddd40baa9a948cf536b0e00a72989ee8ee62fc71cdcc3`.
- [x] Protocol all-feature tests passed (132 unit tests, all integration
  targets, 49 doctests); Core `admin_rpc` passed all 34 tests. Strict
  all-target/all-feature Clippy passed for Protocol, Remoting, and Core.
  Pinned Maven `verify` passed all 37 scenarios and fixture checks, including
  62 typed-header and 14 typed-body cross-decodes. Formatting and diff checks
  passed.
- The source-backed `MqClientAdmin` method count is now 8/19; 11 methods and
  broad P18-V01 reconciliation remain open. Refresh full-workspace P18-V02
  after remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation stays in
  the separate manual handoff.

## P18-V01 `deleteKvConfig` admin RPC — 2026-10-18 (`7cb6fb49`)

- [x] Added `DeleteKvConfigRequestHeader` with nullable `namespace` and `key`,
  matching Java's no-op `checkFields()`. Added `MQClientApi::delete_kv_config`
  using request code 102, the supplied address, and no request body.
- [x] The invoked Java 5.5.1 fixture covers the method signature, header
  fields, timeout, success completion, and `MQClientException` response code
  and remark. Rust scripted Core tests compare the request and error mapping
  to the fixture. Java/Rust typed-header cross-decode and header inventory
  coverage include the new header. Shared admin fixture SHA-256:
  `ceafece5e4c70efab31e34760b8485b631a5691a46000c8d6bff32448fedbb0c`.
- [x] Protocol all-feature tests passed (132 unit tests, all integration
  targets, 49 doctests); Core `admin_rpc` passed all 36 tests. Strict
  all-target/all-feature Clippy passed for Protocol, Remoting, and Core.
  Pinned Maven `verify` passed all 37 scenarios and fixture checks, including
  63 typed-header and 14 typed-body cross-decodes. Formatting and diff checks
  passed. Code commit: `7cb6fb49`.
- The source-backed `MqClientAdmin` method count is now 9/19; 10 methods and
  broad P18-V01 reconciliation remain open. Refresh full-workspace P18-V02
  after remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation stays in
  the separate manual handoff.

## P18-V01 `deleteSubscriptionGroup` admin RPC — 2026-10-18 (`ec8b7c41`)

- [x] Added `DeleteSubscriptionGroupRequestHeader` with nullable `groupName`,
  Java primitive `cleanOffset` defaulting to `false`, and inherited nullable
  RPC fields. Its equality and hash behavior uses the four inherited RPC
  fields. Added `MQClientApi::delete_subscription_group` using request code
  207, the supplied Broker address, and no request body.
- [x] The invoked Java 5.5.1 fixture covers the method signature, typed header,
  timeout, success completion, and `MQClientException` response code and
  remark. Rust scripted Core tests compare the exact request and error mapping.
  Java/Rust typed-header cross-decode, the header validation matrix, and the
  request-header identity fixture cover the new header. Shared admin fixture
  SHA-256: `d2da8387de557de716a1a8eb87c4aabde190d6409ef677ddacc735c5467cac8c`;
  identity fixture SHA-256:
  `81325ef5905ee81e3fbc6af951568afdf88851220a7a0d0ecb0c3b3989d1f2ef`.
- [x] Protocol all-feature tests passed (132 unit tests, all integration
  targets, 49 doctests); Core `admin_rpc` passed all 38 tests. Strict
  all-target/all-feature Clippy passed for Protocol, Remoting, and Core.
  Pinned Maven `verify` passed all 37 scenarios and fixture checks, including
  64 typed-header and 14 typed-body cross-decodes. Formatting and diff checks
  passed. Code commit: `ec8b7c41`.
- The source-backed `MqClientAdmin` method count is now 10/19; nine methods
  and broad P18-V01 reconciliation remain open. Refresh full-workspace P18-V02
  after remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation stays in
  the separate manual handoff.

## P18-V01 `invokeBrokerToResetOffset` admin RPC — 2026-10-18 (`859c152a`)

- [x] Added `MQClientApi::invoke_broker_to_reset_offset` with request code 222
  and Java `ResetOffsetRequestHeader`. It decodes the Java `ResetOffsetBody`
  object-key map into shared queue/offset entries, preserves a nullable
  offset table, and maps a successful response without a body to Java's
  `MQClientException` response code 0.
- [x] The invoked Java fixture covers the method signature, full request
  fields, timeout, successful offset table, Broker error, and success-without-
  body error. Core scripted tests compare request fields, result contents,
  response code/remark, and the null-body branch. Shared admin fixture
  SHA-256: `dc98c4a44b5994bb30bdf55b72abae40727e852e6b7dafdd4e7b4b8e8747b9c0`.
- [x] Core `admin_rpc` passed all 41 tests. Strict all-target/all-feature
  Clippy passed for Protocol, Remoting, and Core. Pinned Maven `verify` passed
  all 37 scenarios and fixture checks, including 64 typed-header and 14
  typed-body cross-decodes. Formatting and diff checks passed. Code commit:
  `859c152a`.
- The source-backed `MqClientAdmin` method count is now 11/19; eight methods
  and broad P18-V01 reconciliation remain open. Refresh full-workspace P18-V02
  after remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation stays in
  the separate manual handoff.

## P18-V01 `queryMessage` direct admin adapter — 2026-10-18 (`ae420dc6`)

- [x] Added the Rust `MqClientAdmin` adapter and
  `MQClientApi::mq_client_admin().query_message`. It sends Java's request code
  12 through asynchronous remoting, preserves `_UNIQUE_KEY_QUERY`, applies the
  caller's body-decompression setting, filters by topic and exact key or
  unique message ID, returns an empty list for `QUERY_NOT_FOUND`, and preserves
  other Broker response codes and remarks.
- [x] The invoked Java 5.5.1 fixture covers matching and nonmatching messages,
  compressed and decompressed bodies, key and unique-key query modes,
  `QUERY_NOT_FOUND`, and Broker failure. Fixture SHA-256:
  `09f2aac28d54e335d591f92359863f5ebae152e19a9d806881bddb6c5c93c320`.
  Core scripted differential compares request fields and decoded message
  fields/body against the fixture.
- [x] Core `admin_rpc` passed all 42 tests. Core all-feature tests passed 171
  unit tests, all integration targets, and 14 doctests. Strict all-target,
  all-feature Core Clippy passed. Pinned Maven `verify` passed all 37 scenarios
  and fixture checks, including 64 typed-header and 14 typed-body cross-decodes.
  Formatting and `git diff --check` passed. Code commit: `ae420dc6`.
- The source-backed `MqClientAdmin` count is now 12/19. Seven methods remain:
  `updateOrCreateSubscriptionGroup`, `viewMessage`, `getBrokerClusterInfo`,
  `getConsumerConnectionList`, `getConsumeStats`, `getConsumerRunningInfo`,
  and `consumeMessageDirectly`. Broad P18-V01 API/settings/input/error/flow and
  reverse-export reconciliation remains open. Refresh full-workspace P18-V02
  after remaining implementation work; current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation remains the
  separate manual handoff.

## P18-V01 `viewMessage` direct admin adapter — 2026-10-18 (`4d6cb941`)

- [x] Added `MQClientApi::mq_client_admin().view_message` using Java's direct
  asynchronous request code 33 path and `ViewMessageRequestHeader`. The API
  decodes one Broker message and preserves response-code/remark errors.
- [x] The invoked Java 5.5.1 fixture records the method signature, timeout,
  request fields, encoded response body, decoded message fields/body, and
  Broker error. The scripted Core differential compares request and result
  against that fixture. Fixture SHA-256:
  `7786a6c5d13b32a109f17f90878ab953b5a7a25392f4cebb514f9c0c8bc148d4`.
- [x] Core `admin_rpc` passed all 43 tests. Core all-feature tests passed 171
  unit tests, all integration targets, and 14 doctests. Strict all-target,
  all-feature Core Clippy passed. Pinned Maven `verify` passed all 37 scenarios
  and fixture checks, including 64 typed-header and 14 typed-body cross-decodes.
  Formatting and `git diff --check` passed. Code commit: `4d6cb941`.
- The source-backed `MqClientAdmin` count is now 13/19. Six methods remain:
  `updateOrCreateSubscriptionGroup`, `getBrokerClusterInfo`,
  `getConsumerConnectionList`, `getConsumeStats`, `getConsumerRunningInfo`,
  and `consumeMessageDirectly`. Broad P18-V01 API/settings/input/error/flow and
  reverse-export reconciliation remains open. Refresh full-workspace P18-V02
  after remaining implementation work; current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation remains the
  separate manual handoff.

## P18-V01 `consumeMessageDirectly` admin RPC — 2026-10-18 (`183660a5`)

- [x] Added `MQClientApi::mq_client_admin().consume_message_directly` using
  Java's asynchronous request code 309 path and
  `ConsumeMessageDirectlyResultRequestHeader`. It decodes the typed result and
  preserves Broker response-code/remark errors.
- [x] The invoked Java 5.5.1 fixture records the method signature, timeout,
  direct and inherited request fields, response body/result, and Broker error.
  Scripted Core tests compare the request/result and reject malformed success
  bodies. Fixture SHA-256:
  `604d2e9f75cfb87e23e7e7fd230a5307ddb51f2356370eda2d3778ae4e7316ef`.
- [x] Core `admin_rpc` passed all 45 tests. Core all-feature tests passed 171
  unit tests, all integration targets, and 14 doctests. Strict all-target,
  all-feature Core Clippy passed. Pinned Maven `verify` passed all 37 scenarios
  and fixture checks, including 64 typed-header and 14 typed-body cross-decodes.
  Formatting and `git diff --check` passed. Code commit: `183660a5`.
- The source-backed `MqClientAdmin` count is now 14/19. Five methods remain:
  `updateOrCreateSubscriptionGroup`, `getBrokerClusterInfo`,
  `getConsumerConnectionList`, `getConsumeStats`, and
  `getConsumerRunningInfo`. Broad P18-V01 API/settings/input/error/flow and
  reverse-export reconciliation remains open. Refresh full-workspace P18-V02
  after remaining implementation work; current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation remains the
  separate manual handoff.

## P18-V01 `getBrokerClusterInfo` admin RPC — 2026-10-18 (`b274784c`)

- [x] Added `MQClientApi::mq_client_admin().get_broker_cluster_info` using
  Java's headerless asynchronous request code 106. The typed `ClusterInfo`
  decoder handles unquoted numeric Broker-address keys and maps a null
  response body to `None`.
- [x] The invoked Java 5.5.1 fixture records the method signature, direct
  address/timeout, response tables, null-body outcome, and Broker error.
  Scripted Core tests compare request shape and decoded tables and cover null
  and error outcomes. Fixture SHA-256:
  `3c3c9e711dbb34e9eb97824cbec676e131990e4a03f9628ab76a119d2b9356d5`.
- [x] Core `admin_rpc` passed all 46 tests; Core all-feature tests passed 171
  unit tests, all integration targets, and 14 doctests. Protocol all-feature
  tests passed 132 unit tests, all integration targets, and 49 doctests.
  Strict all-target/all-feature Core and Protocol Clippy passed. Pinned Maven
  `verify` passed all 37 scenarios and fixture checks, including 64 typed-header
  and 14 typed-body cross-decodes. Formatting and `git diff --check` passed.
  Code commit: `b274784c`.
- The source-backed `MqClientAdmin` count is now 15/19. Four methods remain:
  `updateOrCreateSubscriptionGroup`, `getConsumerConnectionList`,
  `getConsumeStats`, and `getConsumerRunningInfo`. Broad P18-V01
  API/settings/input/error/flow and reverse-export reconciliation remains
  open. Refresh full-workspace P18-V02 after remaining implementation work;
  current-revision Ubuntu/native Windows P18-V04 evidence, final P18-V03
  package/provenance review, Phase 19 local Docker scenarios/profiles and
  runner reconciliation, and pre-publication version/registry checks also
  remain open. Owner-only validation remains the separate manual handoff.

## P18-V01 `getConsumerConnectionList` admin RPC — 2026-10-18 (`eb627fcc`)

- [x] Added `MQClientApi::mq_client_admin().get_consumer_connection_list`
  using Java's asynchronous request code 203 and
  `GetConsumerConnectionListRequestHeader`. The typed response preserves the
  nullable body, connection set, subscription table, and consumer enums.
- [x] The invoked Java 5.5.1 fixture records request code/fields, the encoded
  response body, decoded connection and subscription values, null-body result,
  and Broker error. Core scripted tests compare the request and result and
  cover null bodies, Broker errors, and malformed successful bodies. Fixture
  SHA-256: `63fedb0e501c7539fcbf9495a639c41ec59aa8e3e8db39a14a75674c1a1f2a05`.
- [x] Added bidirectional header fixtures for `GetConsumerConnectionList`
  and the in-progress `GetConsumeStats` request header. Protocol all-feature
  tests passed (133 unit tests, all integration targets, 49 doctests); Core
  `admin_rpc` passed all 47 tests. Strict all-target/all-feature Clippy passed
  for Protocol, Remoting and Core. Pinned Maven `verify` passed all 37
  scenarios, 66 typed-header cross-decodes and 14 typed-body cross-decodes.
  Formatting and `git diff --check` passed.
- The source-backed `MqClientAdmin` count is now 16/19. Three methods remain:
  `updateOrCreateSubscriptionGroup`, `getConsumeStats`, and
  `getConsumerRunningInfo`. Broad P18-V01 API/settings/input/error/flow and
  reverse-export reconciliation remains open. Refresh full-workspace P18-V02
  after remaining implementation work; current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks remain open. Owner-only validation remains the
  separate manual handoff.

## P18-V01 `getConsumeStats` admin RPC — 2026-10-18 (`7375c283`)

- [x] Added `MQClientApi::mq_client_admin().get_consume_stats` for Java 5.5.1
  request code 208. `ConsumeStats` and `OffsetWrapper` preserve Java defaults,
  nullable MessageQueue-keyed offset tables, shared map/entry mutation, public
  accessors, and both wrapping signed-long diff calculations.
- [x] The invoked Java fixture records the interface signature, request fields,
  Fastjson response body/result, DTO public methods/defaults/alias behavior,
  computed totals, null-body result, and Broker error. Scripted Core tests
  compare request/result data and cover null bodies, Broker errors, and
  malformed successful bodies. Fixture SHA-256:
  `707e5c298cab50f1f985bbe41d5da2de70db34670ec67742fbb8fc65b84da9c7`.
- [x] Protocol all-feature tests passed (134 unit tests, all integration
  targets, 51 doctests); Core `admin_rpc` passed all 48 tests. Strict
  all-target/all-feature Clippy passed for Protocol, Remoting, and Core.
  Pinned Maven `verify` passed all 37 scenarios, 66 typed-header cross-decodes,
  and 15 typed-body cross-decodes. `cargo fmt --all -- --check` and
  `git diff --check` passed. Rust body fixture SHA-256:
  `60927f5f6dc23903346f62856021ca84bbf0bc37ae90c17978a2b064dff705e9`.
- The source-backed `MqClientAdmin` count is now 17/19. The remaining methods
  are `updateOrCreateSubscriptionGroup` and `getConsumerRunningInfo`; broad
  P18-V01 reconciliation remains open. Refresh full-workspace P18-V02 after
  remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation stays in
  the separate manual handoff.

## P18-V01 `updateOrCreateSubscriptionGroup` admin RPC — 2026-10-18 (`10db6f10`)

- [x] Added `MQClientApi::mq_client_admin().update_or_create_subscription_group`
  for Java request code 200. It sends the provided address, no ext-fields, and
  the Fastjson `SubscriptionGroupConfig` body; success returns `()` and Broker
  errors preserve the response code/remark.
- [x] Added `SubscriptionGroupConfig` and its retry-policy DTOs with the Java
  public accessor surface, defaults, equality/hash, Lite attribute helpers,
  alias behavior, display and serialization. `SubscriptionDataSet` now has
  insertion-ordered Java Set membership: equality ignores subscription
  `version`, duplicate equal values and duplicate nulls collapse, and Set
  equality ignores iteration order.
- [x] The invoked pinned Java fixture covers the interface method, request
  body, defaults, DTO methods, alias semantics, and Set duplicate/null cases.
  Core `admin_rpc` passed all 49 tests; Protocol all-feature tests passed 141
  unit tests, all integration targets and 51 doctests. Strict all-target/all-
  feature Clippy passed for Protocol, Remoting and Core. Maven `verify` passed
  all 37 scenarios, 66 typed-header cross-decodes and 15 typed-body
  cross-decodes. Formatting and `git diff --check` passed. Fixture SHA-256:
  `21985c21ea6f73efda1c5eceeff45359b937f112d181321b8237a1f431fee8a2`.
- Code commit: `10db6f10`. The source-backed `MqClientAdmin` count is now
  18/19; `getConsumerRunningInfo` is the remaining method slice. Broad P18-V01
  reconciliation, current-revision full-workspace P18-V02, P18-V03 package/
  provenance review, P18-V04 Ubuntu/Windows evidence, and Phase 19 local
  Docker/Broker scenarios and profiles remain open. Owner-only validation
  remains in the separate manual handoff.

## P18-V01 `getConsumerRunningInfo` admin RPC — 2026-10-18 (`f05833a7`)

- [x] Added `MQClientApi::mq_client_admin().get_consumer_running_info` for Java
  request code 307. The adapter sends `GetConsumerRunningInfoRequestHeader`,
  decodes Java's object-valued `MessageQueue` map keys into the typed running-
  info snapshot, returns `None` for a success response without a body, and
  preserves Broker response codes and remarks.
- [x] Extended the pinned Java fixture with the direct async RPC signature,
  request fields, existing rich running-info body, decoded result, null-body
  result and Broker error. Rust tests verify properties, subscriptions,
  normal/POP queue rows, status, user info and jstack; they also cover missing
  body, Broker error and malformed success body. Fixture SHA-256:
  `ae970b21d7822f9d213477bf47a227125b5d6242634a18c490001837b5828851`.
- [x] Protocol all-feature tests passed (141 unit tests, all integration
  targets and 51 doctests); Core `admin_rpc` passed all 50 tests. Strict
  all-target/all-feature Clippy passed for Protocol, Remoting and Core. Pinned
  Maven `verify` passed all 37 scenarios, 66 typed-header cross-decodes and 15
  typed-body cross-decodes. Formatting and `git diff --check` passed.
- Code commit: `f05833a7`. All 19 source-backed `MqClientAdmin` RPC slices are
  present. Broad P18-V01 public DTO/API, settings, input/error/flow and reverse-
  export audit remains open, as do current-revision full-workspace P18-V02,
  P18-V03 package/provenance, P18-V04 Ubuntu/Windows evidence and Phase 19
  local Docker/Broker scenarios and profiles. Owner-only validation remains
  in the separate manual handoff.

## P18-V01 ConsumerRunningInfo property-key correction — 2026-10-18 (`a56ee59d`)

- [x] Added Rust associated constants for all six pinned Java
  `ConsumerRunningInfo` property keys. Push/Pull Running Info generation uses
  these constants; the orderly property now uses Java's exact value,
  `PROP_CONSUMEORDERLY`.
- [x] Expanded the Java Running Info fixture and its dependent MqClientAdmin
  fixture. The Protocol body test checks the constant values and encoded
  fields; the Push generator regression checks the correct key and absence of
  the former misspelling. Fixture SHA-256 values:
  `fixtures/consumer/running-info.json`:
  `54ba7d8dba8b942528eed61d3d15a273a23b944ba4653824300d09c579ef026e`;
  `fixtures/admin/query-consume-time-span.json`:
  `ae161fbf6d2ba14453c7ff91b99ca5be3ae9aaae7f55d46026276ade1de6fe60`.
- [x] Protocol and Push targeted tests passed. Strict all-target/all-feature
  Clippy passed for Protocol and Consumer; formatting and `git diff --check`
  passed. Pinned Java `mvn -f compat/java-reference/pom.xml verify` passed 37
  scenarios, 66 typed-header cross-decodes and 15 typed-body cross-decodes.
- `analyzeSubscription`, `isPushType`, `analyzeRebalance`,
  `analyzeProcessQueue`, and `formatString` are used by `rocketmq-tools`, which
  Phase 17 excludes; pinned-source review found no client call sites. Broader
  P18-V01 API/settings/input/error/flow/reverse-export reconciliation remains
  open, as do current-revision workspace/P18-V02, P18-V03 package/provenance,
  P18-V04 platform evidence and the Phase 19 local scenarios.

## P18-V01 `ConsumerConnection.computeMinVersion` DTO method — 2026-10-18 (`d8f84f2c`)

- [x] Added the Rust `ConsumerConnection::compute_min_version` API for the
  pinned Java method. Empty sets return `i32::MAX`, versions use Java's signed
  minimum rule, and null sets/elements return typed errors at the Java
  `NullPointerException` boundaries. The invoked Java fixture covers defaults,
  empty sets, default/mixed versions, integer limits, nulls, and the reflected
  method signature. Fixture SHA-256:
  `748138c8020551f397ab68bbb24e8e858b5345e95fb78998d0051cea5bbd5f20`.
- [x] Protocol all-feature tests pass (142 unit tests, all integration targets
  and 51 doctests); strict Protocol Clippy passes. Pinned Maven `verify` passes
  all 37 scenarios, 66 typed-header and 15 typed-body cross-decodes. Formatting
  and diff checks pass.
- [x] Current-worktree Linux ARM64 Rust 1.85.0 default workspace tests pass.
  All-feature workspace unit and integration tests pass when the temporary
  Cargo runner clears its inherited signal mask before launching test binaries.
  Workspace all-target/all-feature check and strict Clippy also pass. A full
  all-feature workspace test rerun was stopped during Consumer doctests after
  the unchanged Common compile-fail doctests took 306 seconds; this is not
  recorded as a full P18-V02 pass. The latest complete P18-V02 gate remains
  `411d4efb`, before this DTO slice.
- The P18-V01 broad source/API/settings/input/error/flow and reverse-export
  audit remains open. Current-revision P18-V02 full rerun, final P18-V03
  package/provenance review, P18-V04 Ubuntu/native Windows CI evidence, Phase
  19 local runner/profile completion, and pre-publication release-version and
  registry dependency checks remain open. Owner-environment checks stay in the
  separate manual handoff.

## P18-V02 current-worktree server-free gate — 2026-10-18 (`d8f84f2c`)

- [x] The current code revision passes the full Linux ARM64 Rust 1.85.0
  default workspace tests, the full all-feature workspace tests including
  integration tests and doctests, workspace all-target/all-feature `cargo
  check`, and strict workspace Clippy. The full all-feature test used
  `CARGO_TARGET_DIR=/tmp/rocketmq-phase18-target` inside the container and a
  temporary test runner that clears the inherited signal mask; it exited 0.
- [x] On macOS ARM64 Apple M5 Max with Rust 1.85.0, differential comparator
  self-test and 37-scenario manifest validation pass. The deterministic
  race/leak suite passes 16 cases × 3; fuzz smoke passes 120 frame seeds, 11
  message seeds, 2,096 bounded mutations, 164 frame decodes, and 120 message
  decodes. Release examples build and the offline benchmark completes.
- [x] Maven 3.10.0 / OpenJDK 27 `verify` passes all 37 scenarios, fixture
  checks, 66 typed-header and 15 typed-body cross-decodes. Workspace metadata,
  license and dependency-direction checks pass; each of the seven publishable
  package listings includes `LICENSE` and `NOTICE`. UTC and Asia/Taipei
  Unique ID fixture tests each pass. Formatting and `git diff --check` pass.
- The offline benchmark ran on macOS ARM64 Apple M5 Max with Rust 1.85.0:
  frame encode/decode 2,552 ns/iteration, zstd message decode 1,193 ns,
  producer request construction 1,274 ns, route lookup 14.6 ns, and loopback
  pending completion 77,248 ns. Allocation observations were recorded by the
  harness; these are measurements, not acceptance thresholds.
- This closes the current-revision server-free P18-V02 rerun. Broad P18-V01
  bidirectional source/API/settings/input/error/flow and reverse-export audit,
  final P18-V03 package/provenance review, current-revision P18-V04 Ubuntu and
  native Windows evidence, Phase 19 local runners/profiles, and pre-publication
  release-version and registry dependency checks remain open. Owner checks
  MAN-01–04 remain in the separate handoff; MAN-05 awaits local-matrix
  classification. The pre-existing uncommitted Phase 19 runner files were
  left untouched.

## P18-V01 optional numeric header reflection decoding — 2026-10-18 (`1a4924a3`)

- [x] Pinned Java 5.5.1 fixture covers both decode hints for five non-fast
  headers. Malformed/overflow nullable numeric values are skipped per field,
  valid Arabic-Indic digits decode like Java, and malformed primitive
  `ResetOffsetRequestHeader.queueId` preserves Java's initial `-1`. Fixture
  SHA-256: `f4bc4d7c18fc82a43a07f45f052f710644dcd950f7a5c5c0a0333a25435580f5`.
- [x] Protocol all-feature tests passed (142 unit tests, all integration
  targets, 51 doctests); strict all-target/all-feature Protocol Clippy passed.
  Maven `verify` passed 37 scenarios, fixture checks, 66 typed-header and 15
  typed-body cross-decodes. Formatting and `git diff --check` passed.
- This closes only the named header fields. Other primitive/required numeric
  header branches and broad P18-V01 remain open. Refresh full-workspace P18-V02
  after remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19's 30
  remaining local scenarios and required profiles, and pre-publication version/
  registry checks remain open. Owner-only checks stay in the separate handoff.

## P18-V01 `ChangeInvisibleTimeRequestHeader` nullable reflection fields — 2026-10-18 (`8f64dde3`)

- [x] Pinned Java 5.5.1 fixture covers both decode hints. Nullable strings and
  boxed numeric fields remain unset for missing or invalid input; Arabic-Indic
  decimal digits decode, and primitive `suspend` defaults to false. Fixture
  SHA-256: `01b6c6fdbcc09ef278509f92e58e117c025222ca1e99755ff23168b570c95890`.
- [x] Rust header and POP request construction preserve the nullable fields.
  Core returns typed missing-field errors for absent success-path `offset`,
  `topic`, or `queueId`; non-success responses return `NoExist` without reading
  those fields. The regression covers each missing-field branch.
- [x] Four affected crates pass all-feature unit, integration, and doctests in
  a Rust 1.85.0 Linux container. Strict all-target/all-feature Clippy passes
  for those crates; Maven `verify` passes 37 scenarios, 66 typed-header and 15
  typed-body cross-decodes. Formatting and diff checks pass. The initial
  macOS run stalled at a launched-suspended codec test process; the Linux
  container run completed the full affected-crate test command.
- This closes only the named header and callback paths. Other header branches
  and broad P18-V01 remain open. Refresh the full-workspace P18-V02 gate after
  remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19's 30
  remaining local scenarios and required profiles, and pre-publication
  version/registry checks remain open. Owner-only checks stay in the separate
  handoff.

## P18-V01 `PopMessageRequestHeader` reflection defaults — 2026-10-18

- [x] The pinned Java 5.5.1 oracle confirms both decode hints use reflection:
  the header does not implement `FastCodesHeader`, its `checkFields()` is a
  no-op, nullable group/topic remain null, primitive numeric fields retain zero
  on absent or malformed input, and primitive `order` defaults to false.
  Fixtures cover defaults, malformed/overflow values, and valid Arabic-Indic
  numerals. Fixture: `fixtures/remoting/optional-numeric-header-decode-modes.json`.
- [x] Rust now represents nullable group/topic as `Option<String>`, preserves
  Java primitive defaults, and the consumer POP request path supplies present
  values explicitly.
- [x] Current-tree default and all-feature workspace tests, strict
  all-target/all-feature Clippy, Maven `verify` (37 scenarios, 66 typed-header
  and 15 typed-body cross-decodes), formatting, and diff checks pass.
- This closes only the `PopMessageRequestHeader` decode/default slice. The
  complete P18-V02 supplementary race/fuzz/example/benchmark/package gate must
  be refreshed after implementation work. Broad P18-V01, final P18-V03
  package/provenance and pre-publication checks, current-revision P18-V04
  Ubuntu/native Windows CI evidence, and Phase 19's remaining local scenarios
  and profiles remain open. Owner-only checks stay in the separate handoff.

## P18-V01 Lite POP header parity — 2026-10-18

- [x] `PopLiteMessageRequestHeader` now follows Java reflection defaults:
  nullable strings remain null; primitive numbers default to zero or retain
  zero after invalid/overflow input; the four inherited `RpcRequestHeader`
  fields are represented and encoded. The invoked Java fixture covers both
  decode hints, Arabic-Indic numerals, nullable fields, inherited identity, and
  `isTimeoutTooMuch()` elapsed/future cases. The matching timeout helper is
  implemented for both POP request header types using Java signed arithmetic.
  Fixture hashes: `fixtures/remoting/optional-numeric-header-decode-modes.json`
  (`2c7095818dfb39cfd892f84e585b57c4a61cf09f60f4d6dfc5489674ec340424`) and
  `fixtures/protocol/request-header-identity.json`
  (`cb0a9c871e95bdb006f7bdb5a8a54c2cf74a214ee79729a6e7470f8b62bce9c1`).
- [x] Protocol all-feature tests, workspace all-feature tests, strict
  all-target/all-feature workspace Clippy, Maven `verify` (37 scenarios, 66
  typed-header and 15 typed-body cross-decodes), formatting, and diff checks
  pass.
- This closes only these POP header decode/default/inherited-equality and
  timeout-helper slices. Broad P18-V01 API/input/flow/reverse-export
  reconciliation, the complete P18-V02 supplementary gates, final P18-V03
  package/provenance and pre-publication checks, current-revision P18-V04
  Ubuntu/native Windows CI evidence, and Phase 19 local runners/profiles remain
  open. Owner-only checks stay in the separate handoff.

## P18-V01 POP header public helper methods — 2026-10-18

- [x] Pinned Java behavior is now fixture-backed for negative `queueId`
  normalization, nullable `order` getter/predicate behavior, and both POP
  request headers' custom `toString()` output. Rust exposes matching
  `get_queue_id`, `get_order`, `is_order`, `set_order`, and Java-format string
  helpers. The expanded optional-header fixture SHA-256 is
  `5876a0b734633ff2488d5487df7104fb35ee350b970e1556b0e8c881346a3150`.
- [x] Protocol all-feature tests and the focused Java-fixture differential
  pass; strict all-target/all-feature Protocol Clippy, Maven `verify` (37
  scenarios, 66 typed headers and 15 typed bodies), formatting, and diff checks
  pass.
- This closes only these POP public helper branches. Broad P18-V01 API,
  settings, input/error/flow and reverse-export reconciliation; complete P18-V02
  supplementary gates; final P18-V03 package/provenance and pre-publication
  checks; current-revision P18-V04 Ubuntu/native Windows CI evidence; and Phase
  19 local runners/profiles remain open. Owner-only checks stay in the separate
  handoff.

## P18-V01 POP response header reflection defaults — 2026-10-18 (`f5a924fa`)

- [x] Pinned Java 5.5.1 fixture now covers both decode hints for `PopMessageResponseHeader` and `PopLiteMessageResponseHeader`: absent, malformed, and overflowing primitive values retain zero; Arabic-Indic decimal digits decode; default and valid response fields encode identically. Fixture SHA-256: `1a6bd80ad46c645132412e217517de98e7242e88d457034215c921aca4a4e23f`.
- [x] Rust response-header reflection decoding and the header validation matrix now preserve Java's primitive defaults. Protocol all-feature tests pass (143 unit tests, all integration targets, 51 doctests); strict all-target/all-feature Protocol Clippy passes; Maven `verify` passes the 37-scenario manifest, fixture checks, 66 typed-header and 15 typed-body cross-decodes. Formatting and `git diff --check` pass.
- This closes only POP response header primitive defaults. Broad P18-V01 source/API/settings/input/error/flow/reverse-export reconciliation remains open. The full-workspace P18-V02 supplement, current-revision P18-V04 Ubuntu/native Windows evidence, final P18-V03 package/provenance and version/registry preflight, and Phase 19 local scenario runners/profiles remain open. MAN-01–04 remain owner-environment handoff items; MAN-05 awaits local-matrix classification.

## Current-worktree P18-V02 server-free gate — 2026-10-18 (`f5a924fa`)

- [x] Linux ARM64, Rust 1.85.0: default and all-feature workspace tests pass,
  including integration targets and doctests. All-target/all-feature workspace
  `cargo check` and strict Clippy pass. The container used `--init`, offline
  dependencies, a temporary runner to clear the inherited signal mask, and the
  cached `/workspace` mount path. Initial attempts without the runner or with a
  mismatched cached path were environment failures; neither is counted as a
  gate pass.
- [x] Maven `verify` passes all 37 scenarios, fixture checks, and Java/Rust
  cross-decodes for 66 typed headers and 15 typed bodies. Differential
  comparator self-test and 37-row manifest validation pass.
- [x] Deterministic race/leak smoke passes 16 cases × 3. Fuzz smoke passes 120
  frame seeds, 11 message seeds, 2,096 bounded mutations, 164 frame decodes,
  and 120 message decodes. Release examples build; workspace benchmarks
  compile and the offline benchmark runs.
- [x] Locked Cargo metadata, workspace license/dependency checks, seven
  publishable package listings with `LICENSE` and `NOTICE`, UTC and
  Asia/Taipei Unique ID tests, formatting, and `git diff --check` pass.
- Linux ARM64 Rust 1.85.0 benchmark observations: frame encode/decode 1,217.0
  ns/iteration; Zstd message decode 711.2 ns; producer request construction
  906.9 ns; 128-entry route lookup 14.1 ns; pending completion 5,894.3 ns.
  These are observations, not acceptance thresholds.
- P18-V02 server-free local gates are complete on this revision. Broad P18-V01
  source/API/settings/input/error/flow and reverse-export reconciliation,
  final P18-V03 package/provenance and pre-publication checks, current-revision
  Ubuntu/native Windows P18-V04 evidence, and Phase 19's 30 remaining local
  scenario runners and required profiles remain open. MAN-01–04 remain owner
  checks; MAN-05 awaits local-matrix classification.

## P18-V01 pinned `Message` declaration crosswalk — 2026-10-18

- The pinned Java 5.5.1 `Message.java` declares five public constructors and
  34 public methods. The compatibility map now records a Rust counterpart and
  observed behavior for every declaration, including nullable input, the
  `String`/`Collection` key overloads, lazy property-map state, property/body
  aliases, numeric parse/error paths, and `toString()`.
- On code revision `f5a924fa`,
  `cargo test -p rocketmq-classic-common --test message_decode
  message_public_api_matches_java_551_fixture` passed. Offline Maven `verify`
  passed with 37 scenarios, 11 Java message decode fixtures, fixture drift
  checks, and Java/Rust cross-decodes. The fixture hash is
  `0346a246d0f22440ed71773ac1854ad510acf857954d85f7ee9b8a451afc6471`.
- This closes the declared `Message` API slice. `MessageExt` inherited API and
  workspace-wide P18-V01 reconciliation remain open.

## P18-V01 `MessageExt` and `MessageClientExt` surface crosswalk — 2026-10-18

- The pinned Java 5.5.1 fixtures enumerate both `MessageExt` constructors and
  all 44 declared methods, plus the `MessageClientExt` no-argument constructor
  and four declared methods. The compatibility map now reconciles scalar and
  inherited accessors, socket/address and byte-buffer overloads, system flags,
  client message-ID selection, formatting, and identity behavior.
- On code revision `f5a924fa`, four `message_ext_*` differential tests and
  `message_client_ext_api_matches_java_551_fixture` passed. The current-revision
  offline Maven `verify` passed with 37 scenarios, 11 Java message decode
  fixtures, fixture drift checks, and Java/Rust cross-decodes. Fixture SHA-256:
  `0346a246d0f22440ed71773ac1854ad510acf857954d85f7ee9b8a451afc6471`.
- This closes the `MessageExt`/`MessageClientExt` declared and inherited API
  slice. Broad P18-V01 bidirectional reconciliation remains open.

## P18-V01 `MessageExt` host-byte failure ordering — 2026-10-18

- [x] Pinned Java 5.5.1 confirms absent born/store hosts fail with
  `NullPointerException`. Caller buffers sized 7 bytes for IPv4 and 19 bytes
  for IPv6 overflow while writing the port after the address, retaining the
  first 4/16 bytes and advancing to that position. The fixture also covers
  constructor preservation of null hosts.
- [x] Rust returns typed missing-address/short-buffer errors and preserves the
  address prefix on short-buffer failure. The focused differential and Common
  all-feature suite pass; strict all-target/all-feature Common Clippy, format,
  `git diff --check`, and offline Maven `verify` pass. Fixture SHA-256:
  `6b1a19fef78c1f88f52d7e0851905a6c4a1d6adb98de90fe2028642880a513b3`.
- This closes only this host-byte error-ordering slice. Broad P18-V01 remains
  open; the current-revision full-workspace P18-V02 rerun is still required
  after this source change.

## Current-worktree checks after `ClusterInfo` DTO slice — 2026-10-18 (`6b40e86c`)

- [x] Locked default and all-feature workspace tests, all-target/all-feature
  workspace check and strict Clippy pass on macOS ARM64. `cargo fmt --all --
  --check` and `git diff --check` pass.
- [x] Offline Maven `verify` passes the 37-scenario fixture manifest and
  Java/Rust cross-decodes (66 typed headers and 15 typed bodies).
- The current default/all-feature test and lint gates include the
  `ClusterInfo` DTO change. The P18-V02 supplementary race/fuzz/example/
  benchmark/package gate has not been refreshed after the latest Phase 18
  implementation and remains open.
- Broad P18-V01 bidirectional source/API/settings/input/error/flow and
  reverse-export reconciliation, final P18-V03 package/provenance and
  version/registry preflight, and current-revision Ubuntu/native Windows
  P18-V04 evidence remain open. Phase 19 still needs 30 local Docker/Broker
  scenario runners and required profiles. MAN-01–04 remain owner-environment
  checks; MAN-05 awaits the completed local matrix.

## P18-V02 current-revision supplementary gate — 2026-10-18 (`6b40e86c`)

- [x] On macOS ARM64 Apple M5 Max with Rust 1.85.0, default and all-feature
  workspace tests pass, including integration targets and doctests. Workspace
  all-target/all-feature `cargo check` and strict Clippy pass.
- [x] Maven offline `verify` passes all 37 scenarios, fixture checks, 66
  typed-header and 15 typed-body Java/Rust cross-decodes. Differential
  comparator self-test and 37-row manifest validation pass.
- [x] Deterministic race/leak smoke passes 16 cases × 3. Bounded fuzz smoke
  passes 120 frame seeds, 11 message seeds, 2,096 mutations, 164 frame decodes,
  and 120 message decodes. Release examples and all workspace benchmark targets
  build; the offline benchmark runs.
- [x] Locked workspace metadata, package dependency/license checks and seven
  package-content listings pass. Four Unique ID tests pass under both UTC and
  Asia/Taipei. Formatting and `git diff --check` pass.
- Benchmark observations (not thresholds): frame encode/decode 2,367.5
  ns/iteration; Zstd message decode 1,220.3 ns; Producer request construction
  1,244.3 ns; route lookup 17.1 ns; loopback pending completion 28,162.1 ns.
  Allocation observations are recorded by the benchmark harness.
- P18-V02 server-free gates are now complete on this code revision. Broad
  P18-V01 bidirectional reconciliation, final P18-V03 package/provenance and
  version/registry preflight, and current-revision Ubuntu/native Windows
  P18-V04 evidence remain open. Phase 19 still needs 30 local Docker/Broker
  scenario runners and required profiles. MAN-01–04 remain owner-environment
  checks; MAN-05 awaits the completed local matrix.

## P18-V01 `Connection` and `ConsumerConnection` DTO API — 2026-10-18 (`160f099a`)

- The pinned Java 5.5.1 oracle fixture now inventories the public no-argument
  constructors and all 8 declared methods on `Connection` and 11 declared
  methods on `ConsumerConnection`. Differential coverage records their default
  values, setter/getter values, nullable table behavior, and collection/value
  identity and mutation visibility. Rust `SharedValue` accessors preserve the
  mutable set/map and nested DTO aliases.
- The focused Rust Protocol differential passed. Offline Maven `verify` passed
  all 37 scenarios, fixture drift checks, 66 typed-header and 15 typed-body
  cross-decodes. `cargo fmt --all -- --check` and `git diff --check` passed.
  Fixture SHA-256: `a2702341491aea02ece8494c65ac0dc5544e69947d42987414c171f2fb96cc31`.
- This closes only the two DTO API slices. Broad P18-V01 bidirectional
  reconciliation remains open, as does the full current-revision P18-V02
  rerun. Final P18-V03 package/provenance and version/registry preflight,
  current-revision Ubuntu/native Windows P18-V04 evidence, and Phase 19's 30
  local Docker/Broker scenario runners and required profiles remain open.
  MAN-01–04 remain owner-environment checks; MAN-05 awaits local-matrix
  completion.

## P18-V01 `QueueTimeSpan` DTO API — 2026-10-18 (`423bcdbc`)

- [x] Aligned Rust field visibility with Java: `QueueTimeSpan` fields are
  private, and `QueryConsumeTimeSpanBody.consumeTimeSpanSet` is private. Its
  default empty list, explicit null, nullable rows, and shared identity/mutation
  behavior are covered by an invoked Java fixture and Rust differentials.
- [x] Core preserves its owned response type by snapshotting decoded shared
  DTO handles. Protocol doctests passed (53); focused Protocol/Core Java fixture
  and admin RPC tests passed; strict all-target/all-feature Clippy passed for
  Protocol and Core. Offline Maven `verify` passed all 37 scenarios, fixture
  checks, 66 typed-header and 15 typed-body cross-decodes. Formatting and diff
  checks passed. Fixture SHA-256:
  `3012f8dfb49f4bd404e1d29a7e27cdbe14d47435347bcb55ac807e5649085af5`.
- Code commit: `423bcdbc`. Broad P18-V01 bidirectional reconciliation remains
  open. P18-V02 must be rerun on this newer revision; final P18-V03
  package/provenance and version/registry preflight, current-revision Ubuntu
  and native Windows P18-V04 evidence, and Phase 19's 30 local Docker/Broker
  scenario runners and required profiles remain open. MAN-01–04 remain
  owner-environment checks; MAN-05 awaits completion of the local matrix.

## P18-V01 `GroupList` DTO API inventory — 2026-10-18 (`3b8a62b3`)

- [x] The invoked Java fixture now captures its public no-argument constructor,
  declared getter/setter signatures and empty public-field set. Rust asserts
  that inventory, calls the matching accessors, and compile-fail checks that
  the DTO field is private. Existing alias/default/null/nullable-element
  differentials remain in place. Fixture SHA-256:
  `5961395fa36f199d8b3745f91d1dcc4941c1d2d8748f272e119ed9c3ae1ad73a`.
- [x] The focused Protocol differential, 54 Protocol doctests, strict
  all-target/all-feature Protocol Clippy, pinned Maven `verify` (37 scenarios,
  fixture checks, 66 typed-header and 15 typed-body cross-decodes), formatting,
  and diff checks passed. Test/fixture commit: `3b8a62b3`.
- Broad P18-V01 reconciliation remains open. P18-V02 needs a final rerun on
  this newer revision; P18-V03 package/provenance and version/registry
  preflight, current-revision Ubuntu/native Windows P18-V04 evidence, and Phase
  19's 30 local Docker/Broker runners and required profiles also remain open.

## P18-V01 `TopicList` DTO API inventory — 2026-10-18 (`5233eb2f`)

- [x] Added Java constructor reflection to the existing API fixture. The Rust
  Protocol differential now checks the constructor, four declared accessor
  signatures, empty public-field list, mapped accessors and private field
  visibility, alongside existing default/null/alias behavior. Fixture
  SHA-256: `11d0a8919e272d3f3ed347695a06220a3688c3437805a0e5737ba4fccd43726b`.
- [x] The focused Protocol differential, 55 Protocol doctests, strict
  all-target/all-feature Protocol Clippy, pinned Maven `verify` (37 scenarios,
  fixture checks, 66 typed-header and 15 typed-body cross-decodes), formatting,
  and diff checks passed. Test/fixture commit: `5233eb2f`.
- Broad P18-V01 reconciliation remains open; P18-V02 requires another final
  rerun on this newer revision. P18-V03 package/provenance and registry
  preflight, current-revision Ubuntu/native Windows P18-V04 evidence, and Phase
  19's 30 local Docker/Broker runners and required profiles remain open.

## P18-V01 `TopicStatsTable` and `TopicOffset` API inventory — 2026-10-18 (`93412b9a`)

- [x] Pinned Java fixtures now record public no-argument constructors for both
  DTOs. Rust asserts the constructor, method and field inventories, guards
  field privacy, and exercises all `TopicOffset` getters/setters at signed
  64-bit boundaries. Existing offset-table JSON/null/alias checks remain.
  Fixture SHA-256:
  `9b1bd6d1cebd844db4415a45e60cee14442e76dc235662d5189056977da0aeb6`.
- [x] The focused Protocol differential, 57 Protocol doctests, strict
  all-target/all-feature Protocol Clippy, pinned Maven `verify` (37 scenarios,
  fixture checks, 66 typed-header and 15 typed-body cross-decodes), formatting,
  and diff checks passed. Test/fixture commit: `93412b9a`.
- Broad P18-V01 remains open. P18-V02 needs a final rerun on this latest code
  revision; P18-V03 package/provenance and registry preflight,
  current-revision Ubuntu/native Windows P18-V04 evidence, and Phase 19's 30
  local Docker/Broker runners and required profiles remain open.

## P18-V01 `QuerySubscriptionResponseBody` API inventory — 2026-10-18 (`67edffe7`)

- [x] The invoked Java fixture now includes the public no-argument constructor,
  six declared accessor signatures and empty public-field list. Rust asserts
  those declarations, checks field privacy, directly exercises the setters and
  null reset, and retains the shared `SubscriptionData` identity/mutation
  differential. Fixture SHA-256:
  `e5c6405810fbe550a58bd7ce3b843b3e21410fc3e1674c1692f18836a2e969af`.
- [x] Focused Protocol differential, 58 Protocol doctests, strict all-target /
  all-feature Protocol Clippy, pinned Maven `verify` (37 scenarios, fixture
  checks, 66 typed-header and 15 typed-body cross-decodes), formatting and diff
  checks passed. Test/fixture commit: `67edffe7`.
- Broad P18-V01 remains open. P18-V02 requires another final rerun on this
  latest revision. P18-V03 package/provenance and registry preflight,
  current-revision Ubuntu/native Windows P18-V04 evidence, and Phase 19's 30
  local Docker/Broker runners and required profiles also remain open.

## P18-V02 current-worktree server-free gate — 2026-10-18 (`117ae342`)

- [x] Default and all-feature `cargo test --workspace --locked` pass, including
  workspace integration targets and doctests. Strict all-target/all-feature
  workspace Clippy and `cargo check` pass.
- [x] Maven `verify` passes all 37 scenarios, fixture checks, 66 typed-header
  and 15 typed-body cross-decodes. Differential comparator self-test and
  37-row manifest validation pass.
- [x] Offline race/leak smoke passes 16 cases × 3. Fixed-seed fuzz smoke passes
  120 frame seeds, 11 message seeds, 2,096 mutations, 164 frame decodes, and
  120 message decodes. Release examples and all workspace benchmark targets
  build.
- [x] The offline benchmark runs on macOS ARM64 Apple M5 Max with Rust 1.85.0.
  Observations per iteration: frame encode/decode 2,353.0 ns; Zstd message
  decode 1,145.1 ns; Producer request construction 1,178.5 ns; 128-entry route
  lookup 14.4 ns; loopback pending completion 65,594.1 ns. Allocation
  observations are emitted by the benchmark harness; these measurements are
  not thresholds.
- [x] Workspace metadata and seven product package-content listings pass;
  every listed product package includes `LICENSE` and `NOTICE`. All eight
  workspace packages report version `0.1.0`. Four Core Unique ID tests pass
  under both UTC and Asia/Taipei. Formatting and `git diff --check` pass.
- The P18-V02 local server-free gate is complete on this revision. All 19
  source-backed `MqClientAdmin` RPC slices are present; an earlier unchecked
  19-method inventory note is superseded by the Phase 17 matrix. Broad P18-V01
  API/settings/input/error/flow and reverse-export reconciliation remains
  open. Final P18-V03 version/registry and provenance preflight and current-
  revision Ubuntu/native Windows P18-V04 CI evidence also remain open. Phase
  19 still needs 30 of 37 local Docker/Broker scenario runners, the required
  profiles, and passing `run-local.sh --all` reconciliation. MAN-01–04 remain
  owner-environment checks; MAN-05 awaits the local matrix classification.
- The consumer test call-site adjustment is in code commit `117ae342`. Existing
  Phase 19 runner/README changes remain unstaged and outside this Phase 18
  gate. No plan, Java oracle, remote CI, Broker/Docker state, publication,
  release tag, or push changed.

## P18-V01 `GetConsumerListByGroupResponseBody` API inventory — 2026-10-18 (`e5887718`)

- [x] The invoked Java 5.5.1 fixture records the public no-argument constructor,
  two declared public accessor signatures, and empty public-field inventory.
  Rust asserts this inventory and compile-fail checks that `consumer_id_list`
  is private. Existing default/null, alias mutation, and Java identity checks
  remain covered. Fixture SHA-256:
  `21f6d58affb63528aad0ccc25ac21e4b8e2131c8521ccbdffeb8c68eccf8408c`.
- [x] Protocol/core tests passed; all 59 Protocol doctests passed; strict
  all-target/all-feature Clippy passed for Protocol, Core, and Remoting; the
  Remoting fixture generator example compiled; Maven `verify` passed all 37
  scenarios, fixture drift checks, 66 typed-header and 15 typed-body
  cross-decodes. Formatting and diff checks passed. Code commit: `e5887718`.
- Broad P18-V01 bidirectional reconciliation remains open. P18-V02's full
  server-free gate, P18-V03 provenance/version/registry preflight, current
  Ubuntu/native Windows P18-V04 evidence, and Phase 19's 30 local Docker/Broker
  scenario runners and required profiles remain open.

## P18-V01 LockBatchResponseBody API inventory — 2026-10-18 (43a7cebb)

- [x] The invoked Java 5.5.1 fixture records the public no-argument
  constructor, getter/setter signatures, and empty public-field inventory.
  Rust asserts those declarations and compile-fail checks that
  lock_okmq_set is private. Existing default/null, shared Set/element alias,
  and Java identity behavior remain covered. Fixture SHA-256:
  bf612d3a20c17748bd2026256383e92dfb8f0dcde455677583036fcdbc92543c.
- [x] Protocol/Core tests and doctests pass; strict all-target/all-feature
  Clippy passes for Protocol, Core, and Remoting; the Remoting fixture
  generator example compiles; pinned Maven verify passes all 37 scenarios,
  fixture checks, 66 typed-header and 15 typed-body cross-decodes. Formatting
  and diff checks pass.
- Broad P18-V01 bidirectional reconciliation remains open. P18-V02's complete
  workspace server-free gate must be refreshed after this change. P18-V03
  provenance/version/registry preflight, current Ubuntu/native Windows P18-V04
  evidence, and Phase 19's 30 local Docker/Broker scenario runners and required
  profiles remain open.

## P18-V02 current-worktree full server-free gate — 2026-10-18 (43a7cebb)

- [x] Default and all-feature locked workspace tests, all-target/all-feature
  workspace check and strict Clippy pass. Maven verify passes all 37
  scenarios, fixture drift checks, 66 typed-header and 15 typed-body
  cross-decodes. Differential comparator self-test and 37-scenario manifest
  validation pass.
- [x] Offline race/leak smoke passes 16 cases × 3. Fixed-seed fuzz smoke
  passes 120 frame seeds, 11 message seeds, 2,096 mutations, 164 frame
  decodes, and 120 message decodes. Release facade examples and all workspace
  benchmark targets compile.
- [x] The offline benchmark ran on macOS ARM64 Apple M5 Max (18 logical CPUs)
  with Rust 1.85.0. Per-iteration observations: frame encode/decode
  2,237.8 ns; Zstd message decode 1,143.4 ns; Producer request construction
  1,195.5 ns; 128-entry route lookup 14.8 ns; loopback pending completion
  65,795.8 ns. Allocation counts/bytes per iteration were respectively
  40/4,676; 17/197,493; 54/3,665; 0/0; and 81/9,546. These are observations,
  not thresholds.
- [x] Locked Cargo metadata resolves 216 packages (208 registry packages);
  workspace metadata/license/dependency checks and seven product package
  listings pass, with LICENSE and NOTICE in each listing. Four Unique ID tests
  pass under both UTC and Asia/Taipei. Formatting and diff checks pass.
- This closes the P18-V02 server-free gate on code revision 43a7cebb. Broad
  P18-V01 bidirectional API/settings/input/error/flow reconciliation and Rust
  reverse-export audit remain open. P18-V03 Java artifact/source provenance,
  version and registry preflight, and current Ubuntu/native Windows P18-V04
  evidence remain open. Phase 19's remaining local scenario runners and
  profiles are separate work.

## P18-V03 Java provenance and package preparation — 2026-10-18

- [x] Reconfirmed the pinned Maven dependency tree is Java client 5.5.1,
  remoting 5.5.1, and common 5.5.1. Local binary JAR, source JAR and POM
  checksums match Maven Central's published SHA-1 values. Four representative
  source-JAR files byte-match the exact Apache RocketMQ 5.5.1 tag commit.
  Details and SHA-256 values are in
  [PHASE_18_JAVA_PROVENANCE.md](compatibility/PHASE_18_JAVA_PROVENANCE.md).
- [x] Reviewed the locked Rust package graph, product package contents,
  LICENSE/NOTICE inclusion, dependency direction, package order and compatibility
  baselines. Facade examples and Phase 18 server-free checks pass. The source
  tag's OpenPGP signature is present but could not be verified because gpg is
  unavailable in this environment; the exact tag target and source-file
  comparisons are recorded.
- P18-V03 preparation review is complete. Workspace crates remain explicitly
  unpublished at 0.1.0. Confirming the release version, checking registry
  resolution, enabling publication, publishing or tagging remains
  post-Phase-19 work. Broad P18-V01 and current Ubuntu/native Windows P18-V04
  evidence remain open.

## P18-V01 Java body DTO public-field inventory — 2026-10-18 (`2b15d393`)

- [x] Reconciled ten body DTOs whose Rust fields were public while the Java
  fields are private: `CheckClientRequestBody`, `QueryAssignmentRequestBody`,
  `QueryAssignmentResponseBody`, `SetMessageRequestModeRequestBody`,
  `LockBatchRequestBody`, `UnlockBatchRequestBody`,
  `ConsumeMessageDirectlyResult`, `MessageRequestModeSerializeWrapper`,
  `GetConsumerStatusBody`, and `ResetOffsetBody`. Rust callers now use the
  Java-equivalent accessors. Compile-fail doctests guard representative
  private fields; the invoked Java fixture records empty public-field lists.
  Fixture SHA-256:
  `dd9fb47b7ee42cf67e602e691baf52d0a1b4ba7dffdb0bef0fa158412a2aba16`.
- [x] Code and fixture commit: `2b15d393`.
- This closes the identified body DTO field-visibility slice. Broad P18-V01
  source/API/settings/input/error/flow and reverse-export reconciliation
  remains open.

## P18-V02 current-worktree server-free gate — 2026-10-18 (`2b15d393`)

- [x] Default and all-feature locked workspace tests, all-target/all-feature
  workspace check, and strict workspace Clippy pass. Offline Maven `verify`
  passes all 37 scenarios, fixture drift checks, and Java/Rust cross-decodes
  (66 typed headers and 15 typed bodies). Differential comparator self-test
  and 37-row scenario-manifest validation pass.
- [x] Deterministic race/leak smoke passes 16 cases × 3. Fixed-seed fuzz
  smoke passes 120 frame seeds, 11 message seeds, 2,096 mutations, 164 frame
  decodes, and 120 message decodes. Release facade examples and the offline
  benchmark compile; the benchmark runs on macOS ARM64 Apple M5 Max
  (18 logical CPUs), Rust 1.85.0. Observations: frame encode/decode 2,058.9
  ns; Zstd message decode 1,079.7 ns; Producer request construction 1,189.2
  ns; 128-entry route lookup 15.6 ns; loopback pending completion 67,962.2
  ns. These are observations, not thresholds.
- [x] Locked Cargo metadata resolves 216 packages (208 registry packages).
  Workspace metadata/license/dependency checks and seven package listings
  pass with `LICENSE` and `NOTICE`; four Unique ID tests pass under both UTC
  and Asia/Taipei. Formatting and diff checks pass.
- P18-V02 server-free gates are complete on `2b15d393`. Broad P18-V01,
  current-revision GitHub Ubuntu address-profile evidence, and native Windows
  stack-runtime evidence remain open. Phase 19 still needs 30 of 37 local
  Docker/Broker scenario runners and required profiles; this is Phase 19
  automation work.

## P18-V01 `MessageQueueAssignment` field visibility — 2026-10-18 (`40ccbae8`)

- [x] Pinned Java 5.5.1 reflection confirms `MessageQueueAssignment` exposes
  no public fields, a no-argument constructor, and getters/setters for its
  three properties. Rust fields are private; existing shared alias, null,
  default, and JSON behavior now uses the matching accessors. The invoked Java
  runtime fixture records the public API inventory. Fixture SHA-256:
  `b264398de11245733fc53f99db5573b7e8a28bf58fb2e32d5b383993bcfa60f3`.
- [x] All-feature tests passed for Protocol, Consumer, and Core, including
  71 Protocol doctests; strict all-target/all-feature Clippy passed for those
  three crates. Pinned offline Maven `verify` passed all 37 scenarios, fixture
  checks, 66 typed-header and 15 typed-body cross-decodes. Formatting and diff
  checks passed. Code/fixture commit: `40ccbae8`.
- This closes only the `MessageQueueAssignment` field-visibility slice. Broad
  P18-V01 source/API/settings/input/error/flow and Rust reverse-export
  reconciliation remains open. The complete current-revision P18-V02 gate,
  current Ubuntu/native Windows P18-V04 evidence, and remaining Phase 18
  acceptance work are still pending.

## P18-V01 POP batch ACK DTO field visibility — 2026-10-18 (`a300224f`)

- [x] Pinned Java 5.5.1 reflection confirms `BatchAck` and
  `BatchAckMessageRequestBody` expose no public fields, with their existing
  constructors and getter/setter inventories. Rust fields are now private;
  producer code, test fixtures, and integration call sites use the existing
  Java-equivalent accessors. Compile-fail doctests guard both DTOs. Fixture
  SHA-256: `1eea96b0635b4a8f8c8f2a82187c1702ac0335f529fa2b6790d0ca2ddab2c53f`.
- [x] All-feature tests passed for Protocol, Consumer, Remoting, and Core;
  Protocol passed 143 unit tests and 73 doctests. Strict all-target/all-feature
  Clippy passed for the four crates. Pinned offline Maven `verify` passed all
  37 scenarios, fixture checks, 66 typed-header and 15 typed-body
  cross-decodes. Formatting and diff checks passed. Code/fixture commit:
  `a300224f`.
- This closes only the two POP batch ACK DTO field-visibility rows. Broad
  P18-V01 API/settings/input/error/flow and Rust reverse-export reconciliation,
  the final workspace P18-V02 gate, and current Ubuntu/native Windows P18-V04
  evidence remain open.

## P18-V01 Consumer Running Info DTO field visibility — 2026-10-18 (`8e9b8821`)

- [x] Pinned Java 5.5.1 reflection confirms `ConsumerRunningInfo` exposes its
  six documented public constants while `ProcessQueueInfo`,
  `PopProcessQueueInfo`, and `ConsumeStatus` expose no public fields. Rust
  instance fields are private and callers use accessors. The invoked fixture
  records constructor, public-field, declared-method, and callable-method
  inventories plus identity semantics. Fixture SHA-256:
  `728c824865b335c102c653e36f19676d6bfd935bad1b473f6ce0676f461091de`.
- [x] All-feature locked/offline tests passed for Protocol, Consumer, and Core;
  strict all-target/all-feature Clippy passed for Protocol, Consumer, Remoting,
  and Core. Offline Maven `verify` passed all 37 scenarios, fixture checks,
  66 typed-header and 15 typed-body cross-decodes. Formatting and diff checks
  passed. Code commit: `8e9b8821`.
- This closes only these DTO field-visibility rows. The captured method
  inventory is evidence for continued API reconciliation, not a claim that the
  broad P18-V01 API/settings/input/error/flow and Rust reverse-export audit is
  complete. The final current-revision P18-V02 gate and current Ubuntu/native
  Windows P18-V04 evidence remain open.

## P18-V01 Running Info nested DTO `toString` — 2026-10-18 (`4953cde5`)

- [x] Added Java-compatible Rust `Display`/`ToString` implementations for
  `ProcessQueueInfo` and `PopProcessQueueInfo`. The pinned Java fixture records
  both outputs, including local timestamp formatting for `ProcessQueueInfo`;
  the Protocol differential checks exact strings. Fixture SHA-256:
  `cefcb0d2f68f88acc12996a5068e9f12449676d8313b502fe8d2e9eeccbc69c5`.
- [x] The focused Protocol test and strict all-target/all-feature Protocol
  Clippy passed. Offline Maven `verify` passed all 37 scenarios, fixture
  checks, 66 typed-header and 15 typed-body cross-decodes. Formatting and diff
  checks passed. Code commit: `4953cde5`.
- This closes only the two nested DTO text methods. Full P18-V01 and the final
  workspace P18-V02 gate remain open.

## P18-V01 consumer-offset header visibility and API — 2026-10-18 (`b71406fa`)

- [x] Pinned Java 5.5.1 inventories confirm that
  `QueryConsumerOffsetRequestHeader`, `UpdateConsumerOffsetRequestHeader`, and
  `QueryConsumerOffsetResponseHeader` expose no public fields. Rust fields are
  crate-private, Java-equivalent accessors are available, and the two request
  headers match Java `toString()`. Update requests also encode/decode inherited
  `lo`. Fixture SHA-256:
  `2a62dd6f4171bd043a21c9f9fbf3f79299a07723d165540a1589d5d7312abab1`.
- [x] Workspace all-target/all-feature `cargo check`, focused Protocol/Core/
  Consumer offset and RPC tests, strict Clippy for those three crates, offline
  Maven `verify` (37 scenarios, fixture drift and 66 typed-header/15 typed-
  body cross-decodes), formatting, and diff checks pass. Code commit:
  `b71406fa`.
- This closes only the three consumer-offset header rows. Broad P18-V01
  bidirectional source/API/settings/input/error/flow and reverse-export
  reconciliation, the final current-revision P18-V02 gate, current GitHub
  Ubuntu and native Windows P18-V04 evidence, and Phase 19's 30 missing local
  Broker/Docker scenario runners/profiles remain open. P18-V03 tag-signature
  verification is unavailable without GPG; release version/registry preflight
  is deferred until after Phase 19.

## P18-V01 query/admin header field visibility — 2026-10-18 (`78ac649f`)

- [x] Pinned Java 5.5.1 inventories confirm that
  `QueryTopicsByConsumerRequestHeader`,
  `QuerySubscriptionByConsumerRequestHeader`, and
  `QueryTopicConsumeByWhoRequestHeader` expose no public fields. Rust fields
  are crate-private and the Java-equivalent getters/setters cover owned and
  inherited request values. Compile-fail doctests guard representative fields;
  decode-mode fixture checks both reflection modes and the Java API inventory.
  Fixture SHA-256:
  `1a526e62792d4207d3d0455ff47d9ba86e23a162a368255b59656701eb1f8e1b`.
- [x] Workspace all-target/all-feature check, focused Remoting and Core admin
  tests, Protocol doctests, strict Clippy for Protocol/Remoting/Core, offline
  Maven `verify` (37 scenarios, fixture drift and 66 typed-header/15 typed-body
  cross-decodes), formatting, and diff checks pass. Code commit: `78ac649f`.
- This closes only these three header visibility rows. Broad P18-V01
  bidirectional reconciliation, the final current-revision P18-V02 gate,
  current GitHub Ubuntu/native Windows P18-V04 evidence, and Phase 19's 30
  missing local Broker/Docker scenario runners/profiles remain open.

## P18-V01 admin header field visibility and inherited `lo` — 2026-10-18 (`3b475951`)

- [x] Pinned Java 5.5.1 inventories confirm no public fields on
  `QueryConsumeTimeSpanRequestHeader`, `GetTopicStatsInfoRequestHeader`,
  `DeleteTopicRequestHeader`, `DeleteTopicFromNamesrvRequestHeader`, and
  `DeleteSubscriptionGroupRequestHeader`. Rust fields are crate-private with
  Java-mapped getters/setters and compile-fail guards. The inherited `lo`
  property on `QueryConsumeTimeSpanRequestHeader` is now preserved by Rust
  encode/decode and included in the Java typed-header fixture and validation
  matrix. Query-admin inventory fixture SHA-256:
  `6e92e3d073dcb3b13abbc9c93b94955fd306a5b47576b5c8c52c472a7f668655`.
- [x] Workspace all-target/all-feature check; Protocol header tests and 88
  doctests; Remoting query-admin differential; Core's 50 admin RPC tests and
  consume-time-span tests; Consumer consume-time-span test; strict Clippy for
  Protocol/Remoting/Core/Consumer; offline Maven `verify` (37 scenarios,
  fixture drift, 66 typed-header and 15 typed-body cross-decodes); format and
  diff checks pass. Code commit: `3b475951`.
- This closes only these five header rows and the `QueryConsumeTimeSpan.lo`
  mapping. Broad P18-V01 reconciliation, the final current-revision P18-V02
  gate, current GitHub Ubuntu/native Windows P18-V04 evidence, and Phase 19's
  30 missing local Broker/Docker scenario runners/profiles remain open.

## P18-V01 `DeleteKVConfigRequestHeader` visibility — 2026-10-18 (`cd3742dc`)

- [x] Pinned Java 5.5.1 inventory confirms `DeleteKVConfigRequestHeader`
  exposes no public fields. Rust `namespace` and `key` are crate-private with
  Java-equivalent accessors; a compile-fail doctest guards field privacy. The
  invoked fast/reflection decode fixture now captures this API and both decode
  modes. Query-admin fixture SHA-256:
  `645d41ced3b7bb8ccb355be4546867207335bf7eb2f40aab889d2583aac5d461`.
- [x] Workspace all-target/all-feature check, Protocol header validation and
  89 doctests, Remoting query/admin differential, both Core Delete KV RPC
  regressions, strict Clippy for Protocol/Remoting/Core, offline Maven
  `verify` (37 scenarios, fixture checks, 66 typed-header and 15 typed-body
  cross-decodes), formatting, and diff checks pass. Code commit: `cd3742dc`.
- This closes only the `DeleteKVConfigRequestHeader` visibility row. Broad
  P18-V01 bidirectional reconciliation, the final current-revision P18-V02
  gate, current GitHub Ubuntu/native Windows P18-V04 evidence, and Phase 19's
  30 missing local Broker/Docker scenario runners/profiles remain open.

## P18-V01 offset-boundary header field visibility — 2026-10-18 (`c648552f`)

- [x] Pinned Java 5.5.1 inventories confirm no public fields on the four
  offset-boundary request headers and four response headers. Rust fields are
  crate-private with Java-equivalent getters/setters, including inherited
  topic/queue/request properties and the Search boundary getter's `LOWER`
  fallback. The invoked fast/reflection fixture now captures full Java public
  API inventories and asserts the empty public-field lists. Fixture SHA-256:
  `ab124c979a93556d65660bcc8b9aeaf680649a3c7eb787dc21cb1ef843c167ad`.
- [x] Workspace all-target/all-feature check; offset-boundary Protocol tests
  and 97 doctests; Remoting request/response decode-mode differentials; Core
  invoked offset RPC and admin regressions; Producer and Consumer offset facade
  regressions; strict workspace Clippy; offline Maven `verify` (37 scenarios,
  fixture checks, 66 typed-header and 15 typed-body cross-decodes); formatting
  and diff checks pass. Code commit: `c648552f`.
- This closes only these eight header field-visibility rows. Broad P18-V01
  bidirectional reconciliation, the final current-revision P18-V02 gate,
  current GitHub Ubuntu/native Windows P18-V04 evidence, and Phase 19's 30
  missing local Broker/Docker scenario runners/profiles remain open.

## P18-V01 `QueryMessageResponseHeader` field visibility — 2026-10-18 (`85c9bae5`)

- [x] Pinned Java 5.5.1 inventory confirms
  `QueryMessageResponseHeader` exposes no public fields. Rust watermark fields
  are crate-private with Java-equivalent getters/setters; Core, Consumer,
  Remoting and Protocol callers use the accessors. The invoked Java API inventory
  and Rust field-visibility differential include the class. Query-admin fixture
  SHA-256: `482d0d317162c15b8d159326f5ef59822b3142241c0dad00ea2ba293121d35e0`.
- [x] Workspace all-target/all-feature check, Protocol's 15 header integration
  tests and 98 doctests, Remoting query/admin differential, six Core query RPC
  regressions, Consumer query facade regression, strict workspace Clippy,
  offline Maven `verify` (37 scenarios, fixture checks, 66 typed-header and 15
  typed-body cross-decodes), formatting, and diff checks pass. Code commit:
  `85c9bae5`.
- This closes only the `QueryMessageResponseHeader` visibility row. Broad
  P18-V01 bidirectional reconciliation, the final current-revision P18-V02
  gate, current GitHub Ubuntu/native Windows P18-V04 evidence, and Phase 19's
  30 missing local Broker/Docker scenario runners/profiles remain open.

## P18-V01 remaining response-header field visibility — 2026-10-19

- [x] Pinned Java 5.5.1 inventories confirm that
  `PullMessageResponseHeader`, `SendMessageResponseHeader`,
  `CheckTransactionStateResponseHeader`, `NotificationResponseHeader`,
  `PopMessageResponseHeader`, `PopLiteMessageResponseHeader`, and
  `PollingInfoResponseHeader` expose no public fields. The separate
  `ChangeInvisibleTimeResponseHeader` fixture also reports zero public fields.
  Their Rust backing fields are now crate-private with the Java getter/setter pairs public;
  nullable wrappers remain nullable, and primitive defaults are preserved.
  The response identity fixture records the 16-header inventory; the separate
  change-invisibility fixture records its exact method inventory.
  Compile-fail doctests guard field visibility, and Protocol tests exercise
  accessors/null defaults while Remoting/Core callers use them.
- [x] `cargo fmt --all -- --check`, `git diff --check`, and pinned offline
  Maven `verify` pass (37 scenarios, 66 typed-header and 15 typed-body
  cross-decodes). The full Remoting all-feature suite passes (138 unit, 8
  codec, 1 fuzz-regression, 7 security, 107 transport, and 18 doctests),
  including the corrected Query Admin and Offset Boundary API-inventory
  assertions. Protocol and full-workspace gates remain to be rerun before this
  slice is finalized.
- This closes the response-header field-visibility/API slice only. Broad
  P18-V01 source/API/settings/input/error/flow and Rust reverse-export
  reconciliation, current-revision Ubuntu/native Windows evidence, and Phase
  19 live Broker validation remain separate gates.

## P18-V01 Lite topic-info header inventory follow-up — 2026-10-19

- [x] The Rust `GetLiteTopicInfoRequestHeader` and Core RPC were implemented,
  but the Rust-to-Java typed-header fixture inventory did not include the
  public Java header. Added its Java API/nullable-field fixture assertions,
  typed-frame generation, reverse decode, and validation-matrix row. Code
  commits: `d578db50`, `d4d7588e`.
- [x] Maven `verify` passes all 37 differential scenarios, fixture checks, 67
  exact typed-header frames, and 15 typed-body frames. Protocol header
  inventory (5 tests), header/API/validation tests (19 tests), rustfmt, and
  `git diff --check` pass. Frame SHA-256:
  `92e15800cdfb810908a8a016582189b5523bdc2d7dbbfc8ff69523f2a7184884`.
- This closes one previously unrecorded header fixture. Full P18-V01
  source/API/settings/input/error/flow and reverse-export reconciliation plus
  final workspace validation remain open.

## P18-V01 `ClientGroup` public-final field parity — 2026-10-19

- [x] The pinned Lite topic-info fixture reports `ClientGroup.clientId` and
  `ClientGroup.group` as `public final String`, with a public two-argument
  constructor and no setters. Rust fields are now private with read-only
  `client_id()`/`group()` accessors; two compile-fail doctests reject direct
  field access and mutation. The existing Protocol differential also asserts
  both accessor values for the populated constructor case.
- [x] `cargo fmt --all -- --check` passed. The locked Protocol `--tests` suite
  passed 145 unit tests and all 39 integration tests. The `ClientGroup`-filtered
  rustdoc run passed both new compile-fail guards. Workspace Rustdoc JSON
  generation succeeded and reports 243 remaining public fields after this
  correction. Code commit: `f44b942a`.
- This closes only the `ClientGroup` mutability discrepancy. Broad P18-V01
  bidirectional API/settings/flow and reverse-export reconciliation remains
  open.

## P18-V02/V03 current-worktree server-free gate — 2026-10-19 (`aaff724e`)

- [x] On code revision `aaff724edb116c87e73c2040b73c56841d18ab31`, the locked
  default and all-feature workspace tests, `cargo fmt --all -- --check`, strict
  all-target/all-feature Clippy, the Asia/Taipei unique-ID fixture, differential
  comparator self-test and 37-scenario manifest validation passed.
- [x] The fixed-seed fuzz smoke passed with 121 frame seeds, 11 message seeds,
  2,112 bounded mutations, 170 frame decodes and 109 message decodes. Offline
  deterministic race/leak smoke passed all 16 cases three times each.
- [x] Offline Maven `verify` passed all 37 scenarios and fixture checks,
  including 67 exact typed-header and 15 typed-body cross-decodes. The facade
  examples built in release mode; the offline benchmark compiled and ran. Its
  current-revision observations are in
  [PHASE_18_OFFLINE_BENCHMARK_BASELINE.md](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
- [x] Workspace metadata/dependency-direction checks and package listings for
  all seven product crates passed; each listing includes `LICENSE`, `NOTICE`,
  and `src/lib.rs`. The Java tag still resolves to pinned source commit
  `efa1be59d6fc747ad220f036c86030f05f0419f9`, and the harness pins client
  `5.5.1`. No registry resolution, publication or release claim is made.
- This closes the local server-free P18-V02 gate and current-tree package
  preparation slice. Broad P18-V01 bidirectional source/API/settings/input/
  error/flow and reverse-export reconciliation remains open. Current-revision
  GitHub Ubuntu and native Windows P18-V04 evidence is not available from this
  local run; no CI was triggered. Phase 19 live Broker/Docker validation was
  not run. See the execution log for command and environment details.

## P18-V02 current-revision refresh — 2026-10-21 (`bbcc88ae`)

- [x] Default and all-feature locked workspace tests, strict workspace Clippy,
  formatting, Maven `verify` (37-scenario manifest and fixture drift checks),
  differential comparator self-test, 37-scenario manifest validation, and
  the Asia/Taipei Unique ID fixture pass on the current revision.
- [x] Offline deterministic race/leak smoke passes all 16 cases three times.
  Bounded fuzz smoke passes with 121 frame seeds, 11 message seeds, 2,112
  mutations, 170 frame decodes and 109 message decodes. Release facade examples
  build; the fixed-input offline benchmark runs. Current sample is recorded in
  [PHASE_18_OFFLINE_BENCHMARK_BASELINE.md](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
- [x] Locked Cargo metadata, workspace license/dependency-direction checks and
  all seven product package listings pass; each listing contains `LICENSE`,
  `NOTICE`, and library source. The package graph remains unpublished at
  version `0.1.0`; no registry or release check is claimed.
- The 37 live handoff rows validate and the checked-in CI workflow defines
  server-free Linux gates. No remote workflow or native Windows run was
  triggered. Broad P18-V01 remains open because Rust still does not expose the
  pinned Java mutable nested rebalance tables and concrete process queue API;
  Phase 19 Broker/Docker validation remains separate.


## P18-V01 nested POP API slice — 2026-10-21

- Exposed the Java `PopProcessQueue` methods and added a pinned direct Java
  behavior fixture for its signed counter updates, ACK return, queue-info fill
  and text representation. Push/Pull owners expose live subscription/topic
  tables; Push exposes the live POP table; both create the public POP queue.
- The regenerated facade API fixture is stable across two generations and
  passes its focused Java drift verifier. SHA-256:
  `e9690f9738fb8ccbfe81b8187e4b92b4676deca106a29b2dced86774e88a97e4`.
- Full pinned Maven `verify`, default and all-feature locked workspace tests,
  strict workspace Clippy, Consumer all-feature tests (236 unit tests plus
  integration/doctest targets), the focused facade API suite, rustfmt and
  `git diff --check` pass on this worktree.
- This closes only the exposed POP queue slice. P18-V01 remains open for the
  public `ProcessQueue` API, live process-queue tables, Pull's POP table and
  complete inherited rebalance method correspondence. Refresh the remaining
  P18-V02 supplementary gates after implementation work. P18-V03 package and
  provenance review and current Ubuntu/native Windows P18-V04 evidence remain
  open; Phase 19 Broker/Docker validation was not run.

## P18 ProcessQueue API slice — 2026-10-21

- [x] Exposed Java-shaped `ProcessQueue` methods, live message-tree and atomic
  counter handles, and the same consume lock used by orderly processing. Push
  and Pull owners expose live process-queue tables; Pull assignment updates its
  process table as queues are added and removed. Added direct Java behavior and
  Rust facade tests. The regenerated fixture SHA-256 is
  `d2a38d2f6f18436517520b28213e81f8d7b267895b0b6ba194f31fa4a4b4b3b6`.
- [x] Consumer all-feature tests (236 unit tests plus integration and doctest
  targets), strict all-target/all-feature Consumer Clippy, focused Java fixture
  drift verification, pinned Maven `verify` (37 scenarios and fixture checks),
  formatting and `git diff --check` pass on this worktree.
- P18-V01 remains open for inherited rebalance call correspondence and Pull POP
  assignment flow. Refresh the full current-revision P18-V02 supplementary
  gates after implementation; P18-V03 review and current Ubuntu/native Windows
  P18-V04 evidence remain open. Phase 19 Broker/Docker validation was not run.

## P18 Pull rebalance behavior slice — 2026-10-21

- [x] Matched pinned `RebalancePullImpl` behavior: client rebalance is enabled,
  both pull-offset helpers return zero, `consumeType` is active Pull, process
  queue creation succeeds, POP queue creation returns null, and POP init mode
  reports `no initMode for Pull`. The Pull POP table remains empty. Java fixture
  SHA-256: `fe4523a2b16b7737e977dd6b5ff7beec9b4cb2e6c16b15aac6b43a6cd1f16d3a`.
- [x] The seven-test Consumer facade API suite, focused Java fixture drift
  verifier, formatting and `git diff --check` pass.
- P18-V01 remains open for the remaining inherited rebalance method/settings
  correspondence and the other full client audit rows. P18-V02 refresh and
  P18-V03/V04 review remain outstanding; Phase 19 Broker/Docker validation was
  not run.

## P18 Pull rebalance settings slice — 2026-10-21

- [x] Pull rebalance model and allocation-strategy values now live in shared
  state used by the active registration. Their setters accept a shared owner
  and request an immediate rebalance after startup. The pinned Java fixture
  now checks Pull group/model/strategy setter results and strategy identity;
  regenerated `fixtures/consumer/facade-api.json` SHA-256:
  `7a7234d87c260b398f73bbf4b868714f87c5c9f27a6f0659cb0b93a7e9e834f0`.
- [x] Focused facade tests (9), Consumer all-feature tests (236 unit tests,
  all integration targets and 13 doctests), strict Consumer Clippy, pinned Java
  fixture drift verification, formatting and `git diff --check` pass.
- This closes the Pull model/strategy mutability slice only. Broad P18-V01
  source/API/settings/input/error/flow and reverse-export reconciliation,
  current-revision full-workspace P18-V02, final P18-V03 package/provenance
  review and current Ubuntu/native Windows P18-V04 evidence remain open.
  Phase 19 Broker/Docker validation was not run.

## P18 inherited rebalance method crosswalk — 2026-10-21

- [x] Added a method-level crosswalk for all 32 pinned Java
  `RebalanceImpl` callable methods, including the raw factory replacement and
  mutable dispatch-list adaptations. A Rust fixture test now rejects any
  inherited Java method absent from the crosswalk or its owner source mapping.
- [x] Focused Consumer facade API suite passes all 10 tests. The full Consumer
  all-feature suite, strict Consumer Clippy, pinned fixture drift check,
  formatting and diff checks passed immediately before this crosswalk-only
  follow-up.
- This closes the inherited rebalance crosswalk inventory. Broad P18-V01 still
  covers the rest of the Java client source, input/error/control-flow and
  reverse-export audit.

## P18 `MQHelper` timestamp reset overloads — 2026-10-21

- [x] Added the public `MqHelper` utility to the Consumer crate and top-level
  facade. Its deprecated overload selects instance `DEFAULT`; the explicit
  overload configures the requested model and instance, searches queue offsets
  in Java `TreeSet` order, updates only nonnegative offsets, persists the
  fetched queue set, and shuts down after the operation.
- [x] Pinned Java reflection now records both `MQHelper.resetOffsetByTimestamp`
  overloads in `fixtures/consumer/facade-api.json`; SHA-256:
  `6d38837ca80e7012a35069800144888fa0980c5e2783785b7e998c1d2efa2b1c`.
  Rust tests cover sorted queue visitation, negative-offset filtering and
  stopping at the first search/update error.
- [x] Consumer all-feature tests (239 unit tests, integration targets and 13
  doctests), strict Consumer Clippy, the facade public-type test, Java fixture
  drift verification, formatting and diff checks pass.
- This closes the `MQHelper` utility API slice only. Broad P18-V01 remains open
  for Java source/API/settings/input/error/flow and reverse-export
  reconciliation; current-revision full-workspace P18-V02, final P18-V03
  review, and current Ubuntu/native Windows P18-V04 evidence remain outstanding.
  Phase 19 Broker/Docker validation was not run.

## P18-V01 Validator permission and Broker config RPC slice — 2026-10-21

- [x] Pinned Java reflection now records all seven `Validators` public methods,
  all eight `PermName` methods and permission constants, and numeric/string
  validation outcomes. The Producer fixture SHA-256 is
  `8216bf9c1a66b57a61e88af448f61ab38dc84015ffd872ee340d68896d3ec9b5`.
- [x] Common now exposes the mapped permission masks, predicates, formatter
  and Java-compatible string parser. Producer validation maps topic and Broker
  config checks, including code-16 exception text and malformed-integer
  behavior. Core validates supplied topic permission and implements
  `update_broker_config` with the empty-map no-op, UTF-8 Properties body,
  configured VIP offset and Broker response path.
- [x] Common/Core/Producer all-feature test suites, focused permission and
  Broker RPC regressions, strict all-target/all-feature Clippy for Common,
  Core, Producer and the facade, formatting, and pinned Maven `verify` (37
  scenarios, fixture drift and Java/Rust cross-decodes) pass.
- This closes the listed permission methods and Broker-config operation only.
  Broad P18-V01 remains open, and the full current-revision P18-V02 refresh,
  P18-V03 package/provenance review and current Ubuntu/native Windows P18-V04
  evidence remain outstanding. No remote CI or Phase 19 Broker/Docker run was
  triggered.

## P18-V01 `getBrokerConfig` Properties read slice — 2026-10-21

- [x] Added the Java 5.5.1 `MQClientAPIImpl.getBrokerConfig(String,long)`
  mapping to Core request 26. The request has no header/body, preserves the
  supplied address with VIP enabled, and decodes Java Properties behavior for
  escaped separators, Unicode escapes, malformed escapes, raw non-ASCII text,
  and a final backslash. Missing response bodies map to a structured Rust
  `MalformedResponse`; Java dereferences null before parsing.
- [x] At the read-operation checkpoint, `fixtures/producer/core.json` SHA-256:
  `53f8d3ae387f5df155d0c10484815f3a71d00debafb70c82ed0912f3848bca2f`.
- [x] Core all-feature locked tests (176 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy for Common/Core/
  Producer/facade, Producer fixture comparison, format checks, and pinned Java
  Maven `verify` (37 scenarios, fixture drift and cross-decodes) pass.
- This closes only the Broker config read operation and Properties parser
  inputs. Broad P18-V01, the final current-revision P18-V02 refresh, P18-V03
  package/provenance review and current Ubuntu/native Windows P18-V04 evidence
  remain open. Phase 19 Broker/Docker validation was not run.

## P18-V01 cold-data flow and CommitLog read-ahead RPC slice — 2026-10-21

- [x] Added the four pinned `MQClientAPIImpl` low-level operations: request
  2001 updates cold-data group Properties; request 2002 removes a group;
  request 2003 reads nullable status text; request 2004 sets CommitLog
  read-ahead mode and returns a nullable remark. Tests cover no-op inputs,
  exact request bodies/ext fields, VIP transformation for mutations, direct
  address for reads, empty/null results and Broker errors. The null mode is
  omitted from the remoting wire fields, matching Java's serializer.
- [x] Java invocation fixture and Rust scripted differential cover all four
  methods and their named response branches. Current producer core fixture
  SHA-256 at this slice checkpoint:
  `c20f4474d674bd4e0001f19c82fed8c5e2dfbe6f5c911ee6eade0fb1842c7cb0`.
- [x] Core all-feature locked suite (177 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy for Common/Core/
  Producer/facade, Producer fixture comparison, pinned Java Maven `verify`
  (37 scenarios, fixture drift and cross-decodes), formatting, and scoped
  whitespace checks pass.
- The high-level `rocketmq-tools` facade remains outside Phase 17. This closes
  the Core transport-operation mappings only; broad P18-V01, current-revision
  P18-V02, P18-V03 package/provenance review and current Ubuntu/native Windows
  P18-V04 evidence remain open. No remote CI or Phase 19 Broker/Docker run was
  triggered.

## P18-V01 `registerClient` heartbeat result slice — 2026-10-21

- [x] Added `MQClientApi::register_client` for pinned Java
  `MQClientAPIImpl.registerClient(String, HeartbeatData, long)`: request 34,
  encoded heartbeat body, empty header, supplied address, and synchronous
  boolean result (`true` only for `SUCCESS`; any other Broker code returns
  `false`).
- [x] Direct Java signature/invocation fixture and Rust scripted RPC compare
  both response branches, request code, exact body and empty ext fields.
  Fixture SHA-256 at this slice checkpoint:
  `434a3fe8b1cba71732f41ccb710e601e0d8ce4cbcf196265695cef7641b90905`.
- [x] Core all-feature locked suite (177 unit tests, all integration targets,
  and 16 doctests), focused Producer fixture comparison, strict all-target/
  all-feature Clippy for Common/Core/Producer/facade, pinned Java Maven
  `verify` (37 scenarios, fixture drift, 67 typed-header frames, 15 typed-body
  fixtures and cross-decodes), formatting, and scoped whitespace checks pass.
- This closes the direct registration method only. Broad P18-V01,
  current-revision P18-V02, P18-V03 and P18-V04 remain open; no remote CI or
  Phase 19 Broker/Docker validation ran.

## P18-V01 NameServer topic-list RPC slice — 2026-10-21

- [x] Added `MQClientApi::get_topic_list_from_nameserver` for the pinned
  `MQClientAPIImpl.getTopicListFromNameServer(long)` method. It sends request
  206 with no header/body through the NameServer selector, decodes `TopicList`,
  and preserves error responses. The Java direct invocation fixture records
  the exact method signature, null address, request envelope, topic set,
  Broker address and error result.
- [x] Rust NameServer differential checks success, error, code, Java language,
  empty ext fields and missing request body. The protocol `TopicList` now has
  Java `RemotingSerializable` encode/decode adapters. Fixture SHA-256 at this
  slice checkpoint:
  `6e0a6f94f934f9e173aa76a5e4e3ab18c70ee1121f41f1dfbb52fbbac303a2ef`.
- [x] Core all-feature locked suite (177 unit tests, all integration targets,
  and 16 doctests), Producer fixture comparison, strict all-target/all-feature
  Clippy for Common/Core/Producer/facade, pinned Java Maven `verify` (37
  scenarios, fixture drift, 67 typed-header frames, 15 typed-body fixtures
  and Java/Rust cross-decodes), formatting, and scoped whitespace checks pass.
- At this NameServer-only checkpoint, the composite `getSystemTopicList`
  operation remained open. This closes the direct topic-list RPC only; broad
  P18-V01, current-worktree P18-V02, P18-V03 and P18-V04 remain open.

## P18-V01 Broker system-topic-list RPC slice — 2026-10-21

- [x] Added `MQClientApi::get_system_topic_list_from_broker` for pinned Java
  `MQClientAPIImpl.getSystemTopicListFromBroker(String,long)`: request 305,
  VIP address transform, empty header/body, Java `TopicList` decode, and Broker
  errors. The direct Java invocation fixture and Rust scripted RPC compare the
  exact signature, address, timeout, response topic set, Broker error and
  `SUCCESS` without a body (Java `MQClientException` code 0).
- [x] Core VIP-enabled loopback confirms this operation connects through the
  configured two-port address offset.
- [x] Fixture SHA-256 at this Broker-only slice checkpoint:
  `fbe6f70615e8004c26bdba90fdc824241415d19557b57853a6a563f24becbd32`.
- [x] Core all-feature locked suite (178 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, and Producer pinned
  fixture comparison passed.
- [x] Pinned Java Maven `verify` passed (37 scenarios, 67 typed-header frames,
  15 typed-body fixtures, and Java/Rust cross-decodes); formatting and scoped
  `git diff --check` passed.
- At this Broker-only slice checkpoint, the composite `getSystemTopicList`
  call remained open. This closes the direct Broker operation only; broad
  P18-V01 and P18-V02/P18-V03/P18-V04 gates remain open.

## P18-V01 composite system-topic-list RPC slice — 2026-10-21

- [x] Added `MQClientApi::get_system_topic_list` for pinned Java
  `MQClientAPIImpl.getSystemTopicList(long)`: request 304 through NameServer
  selection, conditional request 305 through the returned Broker address, and
  in-place merge of the nonempty Broker topic set.
- [x] The Java invocation fixture and Rust differential cover both request
  codes and envelopes, timeout and call order, merge, empty/null topic sets,
  blank Broker address, NameServer/Broker errors and missing NameServer body.
- [x] Fixture SHA-256 at this composite slice checkpoint:
  `38b2eac88f25aa04674f469b4e7a685dc599834969f6a1d24613de8c98efdb23`.
- [x] Core all-feature locked suite (178 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, and Producer pinned
  fixture comparison passed.
- [x] Pinned Java Maven `verify` passed (37 scenarios, 67 typed-header frames,
  15 typed-body fixtures, and Java/Rust cross-decodes); formatting and scoped
  `git diff --check` passed.
- The composite system-topic-list operation is closed. Broad P18-V01 remains
  open, as do P18-V02/P18-V03/P18-V04 gates.

## P18-V01 batched subscription-group creation RPC slice — 2026-10-21

- [x] Added `MQClientApi::create_subscription_group_list` for pinned Java
  `MQClientAPIImpl.createSubscriptionGroupList(String,List,long)`: request 225,
  Java `SubscriptionGroupList` body, configured VIP transform, and Broker
  error handling.
- [x] Invoked Java fixture and Rust differential compare the exact encoded
  body for populated and empty lists, timeout, success and Broker-error
  outcomes; a VIP-enabled loopback verifies the two-port address offset.
- [x] Current producer core fixture SHA-256:
  `2479e9af49895298bf807b04775db0e3acef6ad2b6a0a6e2cf59a830ca89169e`.
- [x] Core all-feature locked suite (179 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, and Producer pinned
  fixture comparison passed.
- [x] Pinned Java Maven `verify` passed (37 scenarios, 67 typed-header frames,
  15 typed-body fixtures, and Java/Rust cross-decodes); formatting and scoped
  `git diff --check` passed.
- The batch subscription-group method is closed. The sibling batch topic
  method and broader P18-V01 API inventory remain open; P18-V02/P18-V03/P18-V04
  gates remain open.

## P18-V01 cluster topic-list RPC slice — 2026-10-21

- [x] Added `MQClientApi::get_topics_by_cluster` for pinned Java
  `MQClientAPIImpl.getTopicsByCluster(String,long)`: request 224 through
  NameServer selection with the `cluster` header field. The nullable adapter
  preserves Java's accepted null cluster and omits the null wire field.
- [x] The invoked Java fixture and Rust differential compare the signature,
  request envelope, cluster field, decoded topic list, response error and
  successful reply without a body, including null cluster behavior.
- [x] Fixture SHA-256 at this slice checkpoint:
  `ac3462b7351f327839c4a79db5a8a499f6fdbbd56f667f74b2ac5f18fb368fee`.
- [x] Core all-feature locked suite, strict all-target/all-feature Clippy,
  Producer pinned fixture comparison, pinned Java Maven `verify` (37 scenarios,
  67 typed-header frames, 15 typed-body fixtures and Java/Rust cross-decodes),
  formatting and scoped `git diff --check` passed.
- The cluster topic-list RPC is closed. Broader P18-V01 and P18-V02/P18-V03/
  P18-V04 gates remain open.

## P18-V01 batched topic creation RPC slice — 2026-10-21

- [x] Added `MQClientApi::create_topic_list` for pinned Java
  `MQClientAPIImpl.createTopicList(String,List,long)`: request 18 with a Java
  `CreateTopicListRequestBody`, configured VIP transform and Broker-error
  handling. The nullable adapter preserves Java's null-list and null-element
  body encoding.
- [x] Invoked Java fixture and Rust differential compare populated/default/
  custom, empty, null-list and null-element body bytes, request code, empty
  header, success and Broker-error outcomes; a VIP-enabled loopback verifies
  the two-port transform.
- [x] Current producer core fixture SHA-256:
  `fcf27463aa5fde04867de50543847ad53dabc1b5c3b526ae8ac2406481514ec7`.
- [x] Core all-feature locked suite (180 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, and Producer pinned
  fixture comparison passed.
- [x] Pinned Java Maven `verify` passed (37 scenarios, 67 typed-header frames,
  15 typed-body fixtures and Java/Rust cross-decodes); formatting and scoped
  `git diff --check` passed.
- The batch topic method is closed. Broader P18-V01 and P18-V02/P18-V03/P18-V04
  gates remain open.

## P18-V01 batch topic and subscription-group deletion RPC slice — 2026-10-21

- [x] Added `delete_topic_in_broker_list` for pinned Java
  `deleteTopicInBrokerList(String,List,long)`, request 5002, and
  `delete_subscription_group_list` for
  `deleteSubscriptionGroupList(String,List,boolean,long)`, request 5003. Both
  use Java-compatible request bodies and the configured VIP address; nullable
  adapters preserve null lists and list elements.
- [x] Invoked Java fixtures and Rust differentials compare populated, empty,
  null-list, null-element, success and Broker-error cases; group deletion also
  covers both `cleanOffset` values. A VIP-enabled loopback verifies both
  two-port transforms.
- [x] Current producer core fixture SHA-256:
  `33b7788c15092caaeb2e5cc42ac69eb7bcda5baf5ebc27c2caf01aac9c5e9ff3`.
- [x] Core all-feature locked suite (181 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, and Producer pinned
  fixture comparison passed.
- [x] Pinned Java Maven `verify` passed (37 scenarios, 67 typed-header frames,
  15 typed-body fixtures and Java/Rust cross-decodes); formatting and scoped
  `git diff --check` passed.
- The batch deletion RPCs are closed. Broader P18-V01 and P18-V02/P18-V03/
  P18-V04 gates remain open.

## P18-V01 unit topic-list RPC slice — 2026-10-21

- [x] Added the three pinned Java methods `getUnitTopicList`,
  `getHasUnitSubTopicList`, and `getHasUnitSubUnUnitTopicList` as NameServer
  requests 311, 312 and 313. Rust filters `%RETRY%` topics only when
  `isUnitMode` is false.
- [x] Invoked Java fixtures and Rust differential compare all three request
  envelopes, both filtering modes, response errors and missing-body behavior.
- [x] Current producer core fixture SHA-256:
  `bb5a19cb131f922828ff5effb8eb52ac4a0d95e8bae2863775214da4d6664e5b`.
- [x] Core all-feature locked suite (181 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, and Producer pinned
  fixture comparison passed.
- [x] Pinned Java Maven `verify` passed (37 scenarios, 67 typed-header frames,
  15 typed-body fixtures and Java/Rust cross-decodes); formatting and scoped
  `git diff --check` passed.
- These three RPC mappings are closed. Broad P18-V01 and P18-V02/P18-V03/
  P18-V04 gates remain open.

## P18-V01 NameServer configuration RPC slice — 2026-10-21

- [x] Added `update_name_server_config` for pinned Java request 318 and
  `get_name_server_config` for request 319, including explicit/configured
  NameServer selection, empty input behavior and Java Properties conversion.
- [x] Invoked Java fixtures and Rust differential compare per-server call
  order, fanout/short-circuit errors, property values, malformed content and
  missing-body behavior.
- [x] Current producer core fixture SHA-256:
  `4276123552ab57632da4a10177b1b529266707234e95785668cfcd12177e783c`.
- [x] Core all-feature locked suite, strict all-target/all-feature Clippy,
  Producer pinned fixture comparison, pinned Java Maven `verify`, formatting
  and `git diff --check` passed.
- These two RPC mappings are closed. Broad P18-V01 and P18-V02/P18-V03/P18-V04
  gates remain open.

## P18-V01 transaction resume-check RPC slice — 2026-10-21

- [x] Added `resume_check_half_message` and its nullable adapter for pinned
  Java `MQClientAPIImpl.resumeCheckHalfMessage(String,String,String,long)`,
  request 323. Null header fields are omitted; the request uses the configured
  VIP Broker address and returns `false` for every non-success response.
- [x] Invoked Java fixture and Rust loopback differential compare the method
  signature, request fields and empty body, success/non-success results,
  individually nullable fields and both-null input, and the VIP port offset.
- [x] Current producer core fixture SHA-256:
  `bbeea411ebb9a1cbf177a4f0efddbf85023a475288bd123764959acca248efc6`.
- [x] Core all-feature locked suite (182 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, Producer pinned
  fixture comparison, pinned Java Maven `verify`, formatting and scoped
  `git diff --check` passed.
- The named RPC mapping is closed after validation. Broad P18-V01 and
  P18-V02/P18-V03/P18-V04 gates remain open.

## P18-V01 Broker and Producer diagnostics RPC slice — 2026-10-21

- [x] Added `MQClientApi::get_producer_connection_list`,
  `get_all_producer_info`, and `get_broker_runtime_info` for pinned Java
  requests 204, 328, and 28. All three use the configured VIP address,
  preserve the supplied Broker address in response errors, and decode nullable
  success bodies. The producer-group nullable adapter follows Java's no-op
  header validation and omits null from the wire fields.
- [x] Added Java-compatible `ProducerConnection`, `ProducerTableInfo`,
  `ProducerInfo`, and `KvTable` models, including defaults, explicit null
  setters, mutable collection aliases, public accessor/constructor inventories,
  and `ProducerInfo.toString()` formatting and null-language failure behavior.
- [x] Invoked Java fixtures and Rust scripted RPC tests compare all three
  signatures/envelopes, populated and absent bodies, null producer group,
  Broker errors, typed response DTOs, and VIP address translation. Producer
  core fixture SHA-256:
  `85de9ff0aeeb0236d72fa0ecff85ec01cc24ff1b75a64308eddbc0ac5cfe077d`.
- [x] `cargo test -p rocketmq-classic-protocol --all-features --locked
  producer_diagnostic_dtos_match_java_api_and_response_bodies` passes.
- [x] `cargo test -p rocketmq-classic-core --all-features --locked` passes
  (185 unit tests, all integration targets, and 16 doctests).
- [x] Strict all-target/all-feature Clippy for Common, Protocol, Remoting, Core,
  Producer, Consumer, and the facade passes with `-D warnings`.
- [x] Pinned Java Maven `verify` passes, including the producer core fixture,
  37 invoked RPC scenarios, 67 typed-header frames, 15 typed-body fixtures,
  and Java/Rust cross-decodes.
- [x] `cargo fmt --all -- --check` and `git diff --check` pass.
- This closes only the named diagnostic RPC/DTO slice. Broad P18-V01 and
  P18-V02/P18-V03/P18-V04 remain open.

## P18-V01 timer-engine switch RPC slice — 2026-10-21

- [x] Added `MQClientApi::switch_timer_engine` for pinned Java
  `MQClientAPIImpl.switchTimerEngine(String,String,long)`, request 5001. It
  sends `E_T` including an empty string, uses the supplied address unchanged
  when VIP is enabled, and preserves Broker errors.
- [x] Invoked Java fixture and Rust VIP-enabled loopback compare signature,
  field value, empty body, address behavior, success and Broker rejection.
- [x] Current producer core fixture SHA-256:
  `bb28b038fdd66160007d8c757e53ed4cb78ebcad0a91b4ff9d950fa04eccee6c`.
- [x] Core all-feature locked suite (184 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, Producer pinned
  fixture comparison, pinned Java Maven `verify` (37 scenarios, 67 typed-header
  frames, 15 typed-body fixtures and Java/Rust cross-decodes), formatting and
  scoped `git diff --check` passed.
- The named RPC mapping is closed after validation. Broad P18-V01 and
  P18-V02/P18-V03/P18-V04 gates remain open.

## P18-V01 POP record export RPC slice — 2026-10-21

- [x] Added `MQClientApi::export_pop_record` for pinned Java
  `MQClientAPIImpl.exportPopRecord(String,long)`, request 200056
  (`POP_ROLLBACK`). The request has no header or body, uses the supplied
  address unchanged even when VIP is enabled, and reports non-success replies.
- [x] Invoked Java fixture and Rust VIP-enabled loopback compare the exact
  signature, request envelope, address behavior, success and Broker rejection.
- [x] Current producer core fixture SHA-256:
  `d94fd1f95eba2caf6ce77e312cd53a4109be74048b23db5ad694a2c5b7109242`.
- [x] Core all-feature locked suite (183 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, Producer pinned
  fixture comparison, pinned Java Maven `verify` (37 scenarios, 67 typed-header
  frames, 15 typed-body fixtures and Java/Rust cross-decodes), formatting and
  scoped `git diff --check` passed.
- The named RPC mapping is closed after validation. Broad P18-V01 and
  P18-V02/P18-V03/P18-V04 gates remain open.

## P18-V01 cluster-topic header inventory slice — 2026-10-21

- [x] Added the implemented `GetTopicsByClusterRequestHeader` to the exhaustive
  Rust header inventory and header validation matrix. The Java API inventory
  now records its constructor, `getCluster`, `setCluster`, and no-op
  `checkFields` methods in the query-admin decode fixture.
- [x] Added the request-224 typed frame with `cluster=cluster-a` to the Rust to
  Java cross-decode set and its fixture mapping. Maven now requires exactly 68
  typed header frames; Java decoded all 68 Rust-generated frames.
- [x] Made the Producer core fixture's `KVTable` entry order deterministic by
  inserting keys explicitly. The fixture SHA-256 is
  `861cc12c939270c338349d9ec343a4aaceb8e987031c99c016f75e03df4537c1`.
- [x] Focused header validation and inventory tests, `cargo test --workspace
  --locked`, and `cargo test --workspace --all-features --locked` pass.
- [x] `cargo clippy --workspace --all-targets --all-features --locked --
  -D warnings` passes.
- [x] Pinned Java Maven `verify` passes with 37 invoked RPC scenarios, 68 typed
  header frames, 15 typed body fixtures, and Java/Rust cross-decodes.
- [x] `cargo fmt --all -- --check` and `git diff --check` pass.
- This closes the missing header inventory and reverse-frame evidence only.
  Broad P18-V01 remains open; current P18-V02 server-free disposition is
  recorded below.

## P18-V02 current-revision server-free gate — 2026-10-21 (`6441ca56`)

- [x] Default and all-feature locked workspace tests, strict all-target and
  all-feature workspace Clippy, and pinned Java Maven `verify` pass. Maven
  checked 37 invoked RPC scenarios, 68 typed-header frames, 15 typed-body
  fixtures, and Java/Rust cross-decodes.
- [x] Differential comparator self-test and the 37-row handoff manifest pass.
  Offline race/leak smoke passes 16 cases × 3; fuzz smoke passes 121 frame
  seeds, 11 message seeds, 2,112 mutations, 170 frame decodes, and 109 message
  decodes.
- [x] Release facade examples build; the workspace benchmark target compiles
  and the offline benchmark runs. Its macOS AArch64, Apple M5 Max, 18 logical
  CPU, Rust 1.85.0 observations are recorded in
  `PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`.
- [x] Locked Cargo metadata and the workspace metadata/license/dependency
  checker pass. The Cargo workspace listing has eight entries: seven product
  crates, each containing `LICENSE` and `NOTICE`, plus the unpublished
  `rocketmq-classic-integration` helper, which is excluded from product package
  checks. The Unique ID differential passes under UTC and `TZ=Asia/Taipei`;
  formatting and diff checks pass.
- This closes the current-revision local P18-V02 server-free gate. Broad
  P18-V01 remains open; current Ubuntu/native Windows evidence and the
  remaining Phase 19 Broker/Docker scenarios remain open.

## P18-V01 writer-feature crosswalk reconciliation — 2026-10-21

- [x] Reconciled the Phase 6 API map and Rust source documentation with the current 48/48 Fastjson writer-feature implementation. Removed stale statements that 13 features remain unmapped and repaired the malformed `RemotingSerializable` table row. This documentation-only follow-up does not close the broader P18-V01 API/source/settings/input/error/control-flow/reverse-export audit.
- [x] Protocol all-feature tests, strict all-target/all-feature Protocol Clippy, `cargo fmt --all -- --check`, and `git diff --check` pass on code revision `6441ca56`; the current-revision full-workspace and Maven evidence is recorded in the P18-V02 section above.


## P18-V03 current revision package and provenance recheck — 2026-10-21 (`6441ca56`)

- [x] Rechecked the nine cached Java 5.5.1 binary/source JAR and POM artifacts:
  their SHA-256 and published SHA-1 values match the provenance record. Four
  selected source-JAR entries match their recorded SHA-256 and the exact
  `rocketmq-all-5.5.1` tag target (`efa1be59d6fc747ad220f036c86030f05f0419f9`).
  The tag contains an OpenPGP signature, but this environment still lacks GPG
  for independent signature verification.
- [x] Locked Cargo metadata resolves 216 packages (208 registry packages) and
  all eight workspace packages have publication disabled at version `0.1.0`.
  The workspace metadata/license/dependency-direction checker passes. Cargo
  lists eight workspace packages: seven product crates with `LICENSE` and
  `NOTICE`, plus the development-only integration crate without product
  package metadata.
- [x] The current-revision facade example build and package-content checks are
  recorded in the P18-V02 gate above. The documentation keeps Java client
  5.5.1 separate from the Phase 19 Broker 5.3.4 baseline and makes no release
  compatibility claim.
- P18-V03 preparation review is current for code revision `6441ca56`. Release
  version selection and checking published registry dependency resolution
  remain post-Phase-19 work, as specified by the Phase 18 plan. Broad P18-V01,
  current Ubuntu/native Windows P18-V04 evidence, and Phase 19 live scenarios
  remain open.

## P18-V01 direct subscription-group RPC closure — 2026-10-21

- [x] Added the missing low-level Java `MQClientAPIImpl.createSubscriptionGroup`
  mapping (request 200), including Java DTO body encoding, absent body for a
  null config, configured VIP address transformation, and Broker error mapping.
  The Java fixture captures the exact public signature and four invoked cases;
  its SHA-256 is
  `aaab68664cf25ff86c9b19a48c27e4691596b31124123fdbd83f7ceb93f89b57`.
- [x] The focused Rust filter passes (2 unit tests and 3 integration tests,
  including the new Java differential); strict all-target/all-feature Core
  Clippy, formatting, and diff checks pass. Pinned Java Maven `verify` passes,
  including the RPC fixture harness and Rust/Java cross-decodes for 68 typed
  headers and 15 typed bodies.
- This closes the direct low-level RPC gap only. The broader P18-V01
  API/source/settings/input/error/control-flow/reverse-export audit, current
  Ubuntu/native Windows P18-V04 evidence, and remaining Phase 19 live Broker
  scenarios remain open.

## P18-V01 direct NameServer KV APIs — 2026-10-21

- [x] Added `MQClientApi` mappings for Java `getKVConfigValue`,
  `putKVConfigValue`, `deleteKVConfigValue`, and `getKVListByNamespace`.
  Nullable fields/results, NameServer selection, put/delete broadcast order,
  empty server lists, and Java's last non-success response behavior have invoked
  fixture and scripted-peer coverage. The direct low-level deletion remains
  distinct from `MqClientAdmin.deleteKvConfig`'s explicit-address operation.
- [x] Added typed Java KV headers with Java→Rust and Rust→Java fixtures; the
  typed header inventory now covers 72 frames. Producer core fixture SHA-256:
  `7e6aecbca17b9711ed3276de4aae2c500fdd8f90bdeb1bde7a7c59b0d3c402af`.
- [x] `cargo test -p rocketmq-classic-protocol --locked` passes (148 tests,
  including doctests); the focused Core differential passes; strict locked
  workspace/all-feature Clippy passes. Pinned Maven `verify` passes with 37 RPC
  cases and Java cross-decodes for 72 typed headers and 15 typed bodies.
- This closes the NameServer KV slice only. The remaining broad P18-V01
  API/source/settings/input/error/control-flow/reverse-export audit and the
  current-revision Ubuntu/native Windows P18-V04 evidence remain open.

## P18-V01 direct Broker maintenance RPCs — 2026-10-21

- [x] Added `MQClientApi::{clean_expired_consume_queue,
  delete_expired_commit_log,clean_unused_topic_by_addr}` for pinned Java
  `MQClientAPIImpl` requests 306, 329, and 316. All send empty synchronous
  requests through configured VIP conversion, return `true` on `SUCCESS`, and
  preserve non-success response errors.
- [x] Java fixture signatures and six invoked success/error cases match Rust
  request codes, Java language, empty headers/body, and results. A configured
  VIP loopback verifies the address offset. Producer core fixture SHA-256:
  `212642c83a082b267df62d23621105ae2826266993e07e5778bf24179d96ae8f`.
- [x] The focused Core test filters pass (one VIP unit test and one differential
  test covering six fixture cases); strict locked all-target/all-feature
  workspace Clippy, formatting, and diff checks pass. Pinned Java Maven
  `verify` passes with 37 manifest scenarios, 72 Rust→Java typed headers, and
  15 typed bodies.
- This closes the three direct RPC mappings only. Broad P18-V01, current
  Ubuntu/native Windows P18-V04 evidence, and Phase 19 live Broker scenarios
  remain open.

## P18-V01 direct subscription-group configuration lookup — 2026-10-21

- [x] Added `MQClientApi::{get_subscription_group_config,
  get_subscription_group_config_nullable}` for pinned Java request 352. The
  typed header carries nullable `group` and inherited namespace, broker-name,
  and oneway fields. Rust applies VIP conversion, decodes an optional
  `SubscriptionGroupConfig`, and preserves Broker response errors.
- [x] Java invoked fixtures and Rust differential compare populated/default
  bodies, null and empty group values, absent body, response error, and request
  fields. A configured-VIP loopback confirms address conversion. The typed
  header has Java→Rust and Rust→Java fixtures; the inventory now covers 73
  headers. Producer core fixture SHA-256:
  `b689e56eaba8e03838703da3b83a8333d5ae59aeb88591d38e2ec032955974d9`.
- [x] Focused Core differential and VIP tests pass; Protocol header inventory
  tests pass (8 tests), as does `header_validation_matrix`. Strict locked
  all-target/all-feature workspace Clippy, formatting, and diff checks pass.
  Pinned Maven `verify` passes with 37 manifest scenarios, 73 typed headers,
  and 15 typed bodies.
- This closes this lookup and typed-header slice only. Broad P18-V01, current
  Ubuntu/native Windows P18-V04 evidence, and Phase 19 live Broker scenarios
  remain open.

## P18-V01 `getClusterList` immutable-empty stub — 2026-10-21

- [x] Mapped pinned `MQClientAPIImpl.getClusterList(String,long)` to
  `MQClientApi::get_cluster_list`. Java ignores both arguments, sends no RPC,
  and returns the shared immutable `Collections.EMPTY_SET`; Rust returns a
  shared immutable empty `HashSet` reference.
- [x] Invoked Java fixture cases cover nonempty, null, and empty topics,
  singleton identity, mutation failure, and zero requests. The Rust
  fixture-backed regression checks the empty result, stable singleton identity,
  and no request for all cases. Producer core fixture SHA-256:
  `42244fafb6e6b824db21d17ba6d3e420d6f6dc57baeabd26b5a2573178da528f`.
- [x] `get_cluster_list_matches_java_immutable_empty_singleton_without_rpc`
  passes. Strict locked all-target/all-feature workspace Clippy, `cargo fmt
  --all -- --check`, and `git diff --check` pass. Pinned Maven `verify` passes
  with 37 manifest scenarios, 73 typed headers, and 15 typed bodies.
- Broad P18-V01, current Ubuntu/native Windows P18-V04 evidence, and Phase 19
  live Broker scenarios remain open. Four preserved Phase 19 worktree paths
  remain unstaged and untouched.

## P18-V01 `exportRocksDBConfigToJson` Broker API — 2026-10-21

- [x] Mapped pinned Java request 355 and the
  `ExportRocksDBConfigToJsonRequestHeader`/`ConfigType` public API. The Core
  call preserves the supplied Broker address and caller deadline, serializes
  the Java enum type names with a trailing semicolon, returns on `SUCCESS`, and
  reports Broker rejection. A nullable-list adapter rejects null before RPC.
- [x] Java invoked fixtures cover the method and enum API inventories, valid,
  empty, case/whitespace parsing, unknown type, all/empty/null lists, exact
  request fields, success, and Broker rejection. The typed header fixture and
  decode-mode fixture cover Java↔Rust fields including empty and missing
  `configType`. Producer core fixture SHA-256:
  `a7188f46b4dae2f2a509e4caf5f96aaba00f74964afabc1454e5ca92b23bd344`.
- [x] Protocol `header_inventory` passes (10 tests), Protocol `headers` passes
  (21 tests, including `header_validation_matrix`), and the focused Core RPC
  differential passes. Strict locked workspace Clippy, formatting, and diff
  checks pass. Pinned Maven `verify` passes with 37 manifest scenarios, 75
  typed headers, and 15 typed bodies.
- Broad P18-V01, final current-revision P18-V02, Ubuntu/native Windows P18-V04,
  and Phase 19 live Broker scenarios remain open. Four preserved Phase 19
  worktree paths remain unstaged and untouched.

## P18-V01 `checkRocksdbCqWriteProgress` Broker API — 2026-10-21

- [x] Added `MQClientApi::check_rocksdb_cq_write_progress` for pinned request
  354. It sends the supplied Broker address with the caller deadline, retains
  nullable `topic`, serializes primitive `checkStoreTime` including zero, and
  sends no request body. The return value is an optional typed
  `CheckRocksdbCqWriteResult`; the nested `CheckStatus` enum preserves Java
  names, ordinals and values.
- [x] Added Java invoked cases and a typed header fixture. The cases cover
  nullable/empty topics, default primitive fields, populated and missing DTO
  properties, JSON null, absent/empty/malformed response bodies, and Broker
  rejection. Successful null/absent/empty bodies return `None`; malformed JSON
  becomes `MalformedResponse`. Header fixtures cover fast/reflection decode.
  Producer core fixture SHA-256:
  `6f853c6e49ad04a2fe2d1d7d202c992d1088e17da1fc248330bef7de968603a0`.
- [x] Protocol `header_inventory` passes (12 tests), Protocol `headers` passes
  (23 tests, including `header_validation_matrix`), and Protocol doctests pass
  (151). The full Core `admin_rpc` suite passes (80 tests). Strict locked
  all-target/all-feature workspace Clippy, `cargo fmt --all -- --check`, and
  `git diff --check` pass. Pinned Maven `verify` passes with 37 manifest
  scenarios, 80 Java-decoded typed headers, and 15 typed bodies.
- Broad P18-V01, final current-revision P18-V02, Ubuntu/native Windows P18-V04,
  and Phase 19 live Broker scenarios remain open. Four preserved Phase 19
  worktree paths remain unstaged and untouched.

## P18-V01 `resetMasterFlushOffset` Broker API — 2026-10-21

- [x] Added `MQClientApi::reset_master_flush_offset` for pinned Java request
  908. It sends `masterFlushOffset` to the supplied address without VIP
  conversion, uses Java's fixed 3000 ms timeout, returns on `SUCCESS`, and
  preserves non-success Broker response errors.
- [x] Added the public `ResetMasterFlushOffsetHeader` getter/setter surface and
  nullable boxed-`Long` decoding. Java→Rust and Rust→Java typed-frame fixtures
  cover the header; the inventory now covers 74 typed headers. Java invoked
  fixture cases and Rust differential tests cover `Long.MAX_VALUE`, a negative
  offset, request fields, fixed timeout, address, and success/error outcomes.
  Producer core fixture SHA-256:
  `b5104de29ba8a8870269174ad866ae1277daae60aca3439f4af67aa62d11ee8e`.
- [x] Protocol header inventory passes (9 tests), the Java nullable-long
  decode comparison and `header_validation_matrix` pass, and the focused Core
  RPC differential passes. Strict locked all-target/all-feature workspace
  Clippy, `cargo fmt --all -- --check`, and `git diff --check` pass. Pinned
  Maven `verify` passes with 37 manifest scenarios, 74 typed headers, and 15
  typed bodies.
- Broad P18-V01, current Ubuntu/native Windows P18-V04 evidence, and Phase 19
  live Broker scenarios remain open. Four preserved Phase 19 worktree paths
  remain unstaged and untouched.

## P18-V01 NameServer write-permission APIs — 2026-10-21

- [x] Added `MQClientApi::wipe_write_perm_of_broker` for request 205 and
  `MQClientApi::add_write_perm_of_broker` for request 327. Both use the supplied
  NameServer address and caller deadline, preserve nullable/empty broker names,
  return the boxed topic count, and report non-success replies. Missing or
  malformed success counts fail at the result boundary.
- [x] Added Java-mapped request/response header models, Java→Rust and
  Rust→Java typed frame fixtures, and fast/reflection decode comparisons for
  nullable broker names and boxed counts. Invoked Java cases and the Rust Core
  differential cover both operations, null/empty input, positive/negative
  counts, success, Broker error, and missing/malformed response counts.
  Producer core fixture SHA-256:
  `4ee0b364d81e1d886e040b895a12ae46c676b8bd4a9648580f710e36627caec0`.
- [x] Protocol `header_inventory` passes (11 tests), Protocol `headers` passes
  (22 tests, including `header_validation_matrix`), and the focused Core RPC
  differential passes. Strict locked workspace Clippy, formatting, and diff
  checks pass. Pinned Maven `verify` passes with 37 manifest scenarios, 79
  typed headers, and 15 typed bodies.
- Broad P18-V01, final current-revision P18-V02, Ubuntu/native Windows P18-V04,
  and Phase 19 live Broker scenarios remain open. Four preserved Phase 19
  worktree paths remain unstaged and untouched.

## P18-V02 and P18-V03 current server-free gate — 2026-10-21 (`da6345bb`)

- [x] `cargo fmt --all -- --check` and strict locked workspace Clippy pass.
- [x] `cargo test --workspace --locked` and
  `cargo test --workspace --all-features --locked` pass, including all
  workspace unit, integration, and doctests. The current Remoting
  query/admin-header Java decode-mode comparison covers seven request/response
  header types in fast and reflection modes.
- [x] Pinned Java Maven `verify` passes: 37 scenario-manifest cases, 80
  Java-decoded Rust typed header frames, and 15 typed body frames.
- [x] Differential comparator self-test and all 37 manifest rows validate.
  Unique ID passes with `TZ=Asia/Taipei`.
- [x] Fuzz smoke passes with 133 frame seeds, 11 message seeds, 2,304 bounded
  mutations, 189 frame decodes, and 116 message decodes. Offline deterministic
  race/leak smoke passes 16 cases × 3 repetitions.
- [x] Facade examples build in release mode. The offline benchmark runs and
  its output for this code revision is appended to
  `PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`; the bench target also compiles and
  passes strict all-feature Clippy.
- [x] Workspace metadata/dependency/license checks pass. `cargo package
  --list` includes both `LICENSE` and `NOTICE` for all seven product crates.
  No publish, release-tag, or compatibility claim is made.
- P18-V02's current server-free gate and P18-V03's local preparation checks
  are current for code revision `da6345bb`. The source/API/settings/input/error
  and reverse-export audit P18-V01 remains open. Current-revision Ubuntu and
  native Windows Actions evidence remains open: no remote branch exists for
  `phase/18-phase-18-compatibility-closure`, and the latest listed workflow
  runs are for earlier revisions. The 37-row Phase 19 handoff inventory
  validates, but live Broker/Docker work stays in Phase 19.

## P18-V01 RPC slice: `triggerLiteDispatch` — 2026-10-21

- [x] Added `MQClientApi::trigger_lite_dispatch` for request 200079. It sends
  nullable `group` and `clientId` header fields, no body, applies the configured
  VIP address transform, uses the caller deadline, and reports Broker errors.
- [x] Pinned Java invoked cases and the Rust loopback differential cover the
  API signature, populated/null inputs, decoded header fields, address, request
  code, empty body, success, and Broker rejection. Producer core fixture
  SHA-256: `7846aa7100a8f2339917fc71ad0018e32786692e4b3cff3c1bfafe1c6621372f`.
- [x] `cargo fmt --all -- --check`, strict all-target/all-feature workspace
  Clippy, both locked workspace test suites (default and all-features), and
  pinned Java Maven `verify` pass for this RPC addition.
- The broad bidirectional P18-V01 audit and current Ubuntu/native Windows
  P18-V04 evidence remain open. Phase 19 live Broker scenarios remain in
  Phase 19. The four preserved Phase 19 worktree paths remain untouched and
  unstaged.

## P18-V01 RPC slice: `getBrokerLiteInfo` — 2026-10-21

- [x] Added `MQClientApi::get_broker_lite_info` for request 200074 with an
  empty request, configured VIP conversion, caller deadline and nullable
  `GetBrokerLiteInfoResponseBody` result. The DTO preserves primitive defaults,
  nullable metadata maps and mutable map aliases.
- [x] Invoked Java fixtures cover the pinned RPC/DTO API surface, populated and
  default response bodies, absent and malformed bodies, and Broker errors.
  Rust fixture differential checks response fields and outcomes; a VIP loopback
  verifies the transformed address. Success fixture SHA-256:
  `57eff87ee4fd5bc652076a744109cdc2fb4d395362558b6ed5802e5dc166fe0e`.
- [x] `cargo fmt --all -- --check`, the focused protocol/Core regressions and
  invoked fixture differential pass. Full workspace Clippy/test and Maven
  verification results are recorded in the Phase 18 execution log.
- The broad bidirectional P18-V01 audit, current Ubuntu/native Windows P18-V04
  evidence and Phase 19 live Broker scenarios remain open.

## P18-V01 RPC slice: `getParentTopicInfo` — 2026-10-21

- [x] Added `MQClientApi::get_parent_topic_info` for request 200075 with a
  nullable `topic` header, synchronous invocation, configured VIP conversion,
  caller deadline, and nullable `GetParentTopicInfoResponseBody` decoding. The
  DTO preserves primitive defaults, nullable topic/groups, and mutable Set
  aliases.
- [x] Seven invoked Java cases pin the RPC/header/DTO API surface and cover
  populated/default responses, absent/malformed bodies, null topic, and Broker
  errors. The Rust fixture differential checks request fields and outcomes; a
  VIP loopback checks the transformed address. Success fixture SHA-256:
  `1977e67d8e3c856784cf2628b82bcd14429e2118fb1a3e001db1e4ba2f2ad943`.
- [x] Focused Protocol and Core tests, header inventory, and the invoked
  fixture differential pass. Both locked full-workspace suites, strict
  all-target/all-feature Clippy, formatting, and pinned Maven `verify` pass;
  Maven decoded 81 Rust typed header frames. Full evidence is in the Phase 18
  execution log.
- The broad bidirectional P18-V01 audit, current Ubuntu/native Windows P18-V04
  evidence and Phase 19 live Broker scenarios remain open.

## P18-V01 RPC slice: `getLiteClientInfo` — 2026-10-21

- [x] Added `MQClientApi::get_lite_client_info` for request 200077 with nullable
  topic/group/client inputs, Java's default `maxCount=1000`, synchronous
  invocation, configured VIP conversion, caller deadline and nullable response
  body decoding. The body preserves nullable Set elements and mutable aliases.
- [x] Seven invoked Java cases pin the RPC/header/DTO surface and cover
  populated/default responses, null inputs/body, malformed body and Broker
  errors. The Rust fixture differential checks request/response values; a VIP
  loopback verifies address conversion. Success fixture SHA-256:
  `9e4e1ad1abd344d58ba5939b2fd204a4237c4e60570ceba9c878790aee85ae8b`.
- [x] Focused body/header/Core loopback and invoked fixture differential pass.
  Pinned Maven `verify` passes with 37 scenario cases, 82 Java-decoded Rust
  typed header frames, and 15 typed body frames. Full workspace tests and
  strict Clippy will be recorded with the remaining Lite discovery slice.
- The broad bidirectional P18-V01 audit, current Ubuntu/native Windows P18-V04
  evidence and Phase 19 live Broker scenarios remain open.

## P18-V01 RPC slice: `getLiteGroupInfo` — 2026-10-21

- [x] Added `MQClientApi::get_lite_group_info` for request 200078 with nullable
  group/liteTopic inputs, primitive `topK` defaulting to 0, synchronous
  invocation, configured VIP conversion, caller deadline and nullable response
  body decoding. The header accepts negative `topK`; the response preserves
  nested offset/list aliases and null list elements. `LiteLagInfo.consumerOffset`
  defaults to -1 on decode and is omitted from JSON serialization.
- [x] Seven invoked Java cases pin the RPC/header/DTO API surface and cover
  populated/default responses, nullable inputs/list elements, null/malformed
  bodies and Broker errors. The Rust fixture differential checks request and
  response values; a VIP loopback verifies address conversion. Success fixture
  SHA-256: `da245ed2dece655a09e8f05ab70fec33c513dbc194b35aab12eb35ed74ef608d`.
- [x] Pinned Maven `verify` passes with 37 scenario cases, 83 Java-decoded Rust
  typed header frames and 16 typed body frames. The locked default and
  all-features workspace suites, strict Clippy, formatting and diff checks pass.
  Full evidence is in the Phase 18 execution log.
- The broad bidirectional P18-V01 audit, current Ubuntu/native Windows P18-V04
  evidence and Phase 19 live Broker scenarios remain open. The four preserved
  Phase 19 worktree paths remain untouched and unstaged.

## P18-V01 `MqClientAdmin` Rust wrapper surface — 2026-10-21

- [x] `MQClientApi::mq_client_admin()` now exposes all 19 methods in the pinned
  Java `MqClientAdmin` interface. The invoked fixture records the exact
  reflection signatures; Rust checks all 19 names and compile-calls every
  wrapper.
- [x] The topic-update wrapper preserves Java's direct request behavior. An
  invalid permission value is sent to the Broker instead of triggering the
  local validation used by `MQClientAPIImpl.createTopic`; the Java fixture and
  Rust scripted peer compare the request.
- [x] Fixture SHA-256:
  `16f61c5e859f44062a2ec732e2603ad97b38719b9a0f8fa23fc09ffa5112dfd3`.
  `cargo test -p rocketmq-classic-core --test admin_rpc --locked` passes all
  82 tests; strict Core Clippy passes; pinned Maven `verify` passes 37 scenarios,
  83 typed-header frames and 16 typed-body frames. Formatting and diff checks
  pass.
- The broad bidirectional P18-V01 audit and final full-workspace P18-V02 rerun
  remain open, as do current-revision Ubuntu/native Windows evidence and Phase
  19 live Broker/Docker scenarios. Preserved Phase 19 paths remain untouched.

## P18-V01 POP retry assignment transitions — 2026-10-21

- [x] Matched the pinned Java `RebalanceImpl.updateMessageQueueAssignment`
  subscription transitions for pop-to-push, push-to-pop, mixed mode, retry
  topics and empty assignments. Java removes the retry subscription on
  push-to-pop but retains its route cache; Rust now preserves the cached route.
- [x] Added the invoked Java `popRetryTransitions` fixture and Rust regression
  `broker_assignment_modes_match_java_pop_retry_subscription_transitions`.
  Fixture SHA-256:
  `104b9fd505b227abc8a8d1f5e35e036ba89989cbfbb4d9725deb13fc87889565`.
- [x] The focused Rust regression passes; strict all-target/all-feature
  consumer Clippy passes; pinned Maven `verify` passes with 37 scenarios, 83
  Java-decoded typed-header frames and 16 typed-body frames. Formatting and
  diff checks pass.
- The broader bidirectional P18-V01 audit, final full-workspace P18-V02 rerun,
  current-revision Ubuntu/native Windows evidence and Phase 19 live
  Broker/Docker scenarios remain open. Preserved Phase 19 paths remain
  untouched and unstaged.

## P18-V01 POP assignment retention — 2026-10-21

- [x] Extended the invoked Java `RebalanceImpl.updateMessageQueueAssignment`
  fixture to record expired-assigned replacement, active-assigned same-object
  retention, dropped-assigned retention and empty-assignment removal.
- [x] Rust regression
  `broker_assignment_error_preserves_worker_until_empty_assignment` matches
  those cases and verifies the active POP worker remains live across an
  assignment error, then stops after empty assignment. Current
  `fixtures/runtime/consumer-operations.json` SHA-256:
  `a8d82baca384b9b7932b4961b051902336155543428047d891eb9f84052d166d`.
- [x] Focused consumer regression, strict all-target/all-feature Clippy,
  formatting, diff checks and pinned Maven `verify` pass. Maven reports 37
  scenarios, 9 Running Info, 29 direct-consume, 15 reset cases, 85 typed
  headers and 16 typed bodies.
- The broader P18-V01 audit, final full-workspace P18-V02 refresh and
  current-revision Ubuntu/native Windows P18-V04 evidence remain open.

## P18-V01 `MqClientAdmin.queryMessage` key-filter inputs — 2026-10-21

- [x] Matched pinned Java `StringUtils.isNotBlank` / `Character.isWhitespace`
  behavior and `String.split(" ")` token handling for empty query keys,
  NBSP, em-space, U+0085 and U+001C keys, internal empty tokens and trailing
  empty tokens. Both the direct Broker result filter and cross-Broker merger
  use the same Rust helper.
- [x] Added invoked Java fixture outcomes and Rust checks
  `mq_client_admin_query_message_matches_invoked_java_fixture` and
  `query_message_keys_match_java_whitespace_and_split_semantics`. Current
  `fixtures/admin/query-consume-time-span.json` SHA-256:
  `16f61c5e859f44062a2ec732e2603ad97b38719b9a0f8fa23fc09ffa5112dfd3`.
- The bounded filter input cases are closed. Broad P18-V01, final
  full-workspace P18-V02, current native platform evidence and Phase 19 live
  Broker/Docker scenarios remain open.

## P18-V01 direct-consume `MessageClientExt` subtype — 2026-10-21

- [x] Direct consumption now retains Java's runtime subtype when a
  `MessageClientExt` is passed through the `MessageExt` input. The listener can
  inspect `is_message_client_ext()` and obtain the virtual message ID with live
  `UNIQ_KEY` lookup through `message_id_for_runtime_type()`.
- [x] Client decode, pull result and admin decode paths use the same subtype
  state. The invoked Java fixture records callback class, and the 26-case Rust
  direct-consume comparison checks class, effective ID, offset ID and offset
  isolation. Fixture SHA-256:
  `d0b4e39ba340e509d1ddc2665da122a0cf161ed757cc1d9ac2cbd022edb7d83f`.
- [x] Focused Common tests, the full Common suite, direct-consume fixture
  comparison and Maven `verify` pass. Code commits: `40a02244` and
  `6e2e3d09`.
- [x] Pinned subtype inventory follow-up: client decoding with
  `MessageDecoder.decode(..., isClient=true)` and `decodeMessage` produces
  `MessageClientExt`; the client PullAPIWrapper invokes `decodeMessage`. The
  only other built-in subclasses in the pinned tree are broker-side
  `MessageExtBrokerInner` and `MessageExtBatch`, not constructed by client
  sources. Standard client-produced subtype paths are covered above.
- Rust does not preserve arbitrary application-defined Java `MessageExt`
  subclasses or custom virtual methods/fields. That adaptation boundary, the
  broader inherited/public/DTO/config and reverse-export audit, and remaining
  rebalance assignment branches remain open P18-V01 work.

## Current Phase 18 local gate — 2026-10-21

- [x] Complete server-free P18-V02 gate on code revision `6e2e3d09` (macOS
  AArch64, Apple M5 Max, 18 logical CPUs, Rust 1.85.0, OpenJDK 21): sequential
  default and all-feature locked workspace tests; strict all-target/all-feature
  Clippy; fmt and diff checks; pinned Maven `verify` (37 scenarios, 83 typed
  header frames, 16 typed body frames); differential self-test and 37-row
  manifest; deterministic race/leak smoke (16 cases × 3); fuzz smoke (137 frame
  seeds, 11 message seeds, 2,368 mutations); release examples; offline
  workspace benchmark compilation and offline measurements; locked Cargo
  metadata; workspace metadata/license/dependency checks; seven package
  listings with `LICENSE`/`NOTICE`; and the Unique ID fixture under UTC and
  `TZ=Asia/Taipei` all pass.
- Cargo metadata reports 8 workspace members, 216 total packages, and 208
  registry packages. Benchmark observations and exact outputs are recorded in
  the linked execution log and benchmark baseline. No benchmark threshold is
  asserted.
- [x] P18-V03 package graph/content, source provenance, license files and
  compatibility documentation review remain complete; no release or final
  compatibility claim is made.
- [ ] P18-V04 handoff manifest and local server-free gates pass on this code
  revision. Current-revision GitHub Ubuntu address-profile evidence and native
  Windows stack-runtime evidence remain absent. The broad P18-V01 audit and
  Phase 19 Broker/Docker scenarios also remain open. The preserved Phase 19
  worktree paths remain untouched and unstaged.
- Full command outcomes, first-attempt lint/fixture/metadata diagnostics and
  recovery actions are recorded in
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261021T145812Z.rlog`.

## P18-V01 Push implementation factory setter — 2026-10-21

- [x] Added `get_implementation_client_runtime` and
  `set_implementation_client_runtime` for the pinned
  `DefaultMQPushConsumerImpl.getmQClientFactory/setmQClientFactory` field. The
  implementation factory slot stays distinct from the nested rebalance factory;
  startup initializes both, and Push runtime contexts use an explicit
  implementation override.
- [x] Added a regression that checks initial null, runtime identity replacement,
  explicit null, and independence from the rebalance factory slot. Updated the
  Phase 10 and Phase 18 compatibility crosswalks.
- [x] Focused setter and raw implementation accessor tests pass; all 244
  consumer library tests pass. Strict consumer Clippy, formatting and diff
  checks pass. The full workspace P18-V02 suite must be refreshed after this
  implementation slice.
- Broad P18-V01 source/settings/input/error/flow/reverse-export audit remains
  open; current Ubuntu/native Windows P18-V04 evidence and Phase 19 Broker
  scenarios remain pending. The preserved Phase 19 worktree paths were not
  changed.

## P18-V02 current-revision refresh — 2026-10-21

- [x] Refreshed the server-free verification gate on code revision `659ec260`
  after the Push implementation factory setter. Default and all-feature
  locked workspace tests, strict all-target/all-feature Clippy, formatting and
  diff checks pass. Pinned Maven `verify` passes all 37 scenarios and
  Rust/Java fixture cross-decodes.
- [x] Differential self-test and 37-row manifest, 16-case × 3 offline stress
  smoke, release fuzz smoke, release examples, offline benchmark, locked
  metadata, workspace license/dependency checks, seven package listings with
  `LICENSE`/`NOTICE`, and four Unique ID tests in both UTC and Asia/Taipei pass.
- Benchmark observations and command outcomes are recorded in
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261021T154619Z.rlog`.
  No benchmark threshold is asserted.
- Broad P18-V01 remains open, including arbitrary raw consume-service
  replacement and the shared mutable stats-manager accessor. Current-revision
  GitHub Ubuntu and native Windows P18-V04 evidence and Phase 19 live Broker
  scenarios remain pending; protected Phase 19 worktree paths remain untouched
  and unstaged.

## P18-V02 refresh after rebalance membership fix — 2026-10-21

- [x] Verified code revision `9008c10e`. Default and all-feature locked
  workspace tests, strict all-target/all-feature Clippy, pinned Maven
  `verify` (37 scenarios and Rust/Java fixture cross-decodes), differential
  self-test and 37-row manifest, offline stress smoke (16 cases × 3), release
  fuzz smoke, release examples, offline benchmark, metadata/dependency checks,
  seven package listings with `LICENSE`/`NOTICE`, and Unique ID tests under UTC
  and Asia/Taipei pass.
- The workspace suites, strict Clippy and Maven ran after the behavior change.
  The only later code edit restored the test's historical name for the stress
  runner; that focused regression and the full stress smoke passed afterward.
  `cargo fmt --all --check` and `git diff --check` pass.
- Benchmark observations and detailed outcomes are recorded in
  `docs/compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md` and
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261021T160047Z.rlog`.
  No benchmark threshold is asserted.
- Broad P18-V01 remains open, including raw Push consume-service replacement,
  the shared mutable stats-manager accessor, the arbitrary Java subclass
  boundary and reverse API coverage. Current Ubuntu/native Windows P18-V04
  evidence and Phase 19 live Broker scenarios remain pending. Protected Phase
  19 worktree paths remain untouched and unstaged.

## P18-V01 ProcessQueue expiry error and retry slice — 2026-10-21 (`9147a902`)

- [x] Matched pinned Java `ProcessQueue.cleanExpiredMsg`: malformed non-empty
  `CONSUME_START_TIME` fails before send-back and keeps the head queued; the
  scheduled wrapper logs the error and aborts that sweep. Rust preserves the
  malformed value, returns `ExpiredMessageCleanupError` from direct cleanup,
  and logs the scheduled error. Direct send-back failures retry the same head
  through the original queue-size loop, capped at 16.
- [x] Extended the pinned Java expiry fixture and Rust comparison for malformed
  timestamps, unchanged count/bytes, no send-back attempt, direct repeated
  send-back failure, and the scheduled error path. Updated the Phase 10 and
  Phase 18 compatibility matrices.
- [x] Current-revision server-free P18-V02 gates pass: default/all-feature
  locked workspace tests, strict workspace Clippy, pinned Maven verify (37
  scenarios, 83 typed headers and 16 typed bodies), differential self-test and
  37-row manifest, stress smoke (16 cases × 3), release fuzz smoke, release
  examples, workspace benchmark compilation, offline benchmark, metadata and
  seven package-content checks, and UTC/Asia/Taipei Unique ID checks.
- Benchmark sample and full outcomes are recorded in
  `docs/compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md` and
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261021T162010Z.rlog`.
  No benchmark threshold is asserted.
- Broad P18-V01 remains open, including raw Push consume-service replacement,
  the shared mutable stats-manager accessor, arbitrary Java `MessageExt`
  subclasses, and the remaining source/settings/input/error/control-flow and
  reverse-export audit. Current Ubuntu/native Windows P18-V04 evidence and
  Phase 19 live Broker/Docker scenarios remain pending. Protected Phase 19
  worktree paths were left untouched and unstaged.

## P18-V01 ProcessQueue timestamp input branches — 2026-10-21 (`e7dd0a9b`)

- [x] Added pinned Java and Rust comparisons for absent and empty
  `CONSUME_START_TIME`, Unicode decimal digits accepted by `Long.parseLong`,
  and orderly-mode early return before malformed timestamp parsing. Fixture
  SHA-256: `0ecf6ca6795174a96a90123cebb5415c2108cc81842ce84469e721d599f90d01`.
- [x] Default/all-feature workspace tests, strict workspace Clippy, pinned
  Maven verify (37 scenarios, 83 typed headers, 16 typed bodies), differential
  self-test/manifest, stress smoke, fuzz smoke, release examples, workspace
  bench compilation, package/metadata checks, and UTC/Asia/Taipei Unique ID
  checks pass on this follow-up tree. The earlier current-code offline benchmark
  remains applicable because this commit changes only fixtures, assertions,
  and documentation.
- Full outcomes are recorded in
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261021T163042Z.rlog`.
  Broad P18-V01, GitHub Ubuntu/native Windows P18-V04 evidence, and Phase 19
  live Broker/Docker scenarios remain open. Protected Phase 19 worktree paths
  were left untouched and unstaged.

## P18-V01 Push `ConsumeMessageService` replacement seam — 2026-10-21

- [x] Added the public Rust `ConsumeMessageService` trait and nullable raw
  Push-owner accessors. Custom services receive direct-consume and normal Pull
  work, pool updates and adjustment hooks, and shutdown. POP remains routed to
  the separate internal POP service as in Java 5.5.1. The Java fixture now
  inventories all nine public service methods.
- [x] Matched `ProcessQueue.removeMessage` for in-flight entries, including
  cached count/bytes and offset return behavior.
- [x] Slice verification passes: default and all-feature locked workspace
  tests; strict all-target/all-feature Clippy; Maven verify (37 scenarios, 83
  typed headers, 16 typed bodies); differential self-test and 37-row manifest;
  deterministic stress smoke (16 cases × 3); release fuzz smoke (137 frame
  seeds, 11 message seeds, 2,368 mutations); release examples; workspace bench
  compilation and offline benchmark; formatting and diff checks.
- Fixture SHA-256:
  `2ceab3283085fa8335cd3e7a965c1698e10960ae720c76b148af4cb52a196b4b`.
  Full command outcomes and benchmark observations are recorded in
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261021T174315Z.rlog`.
  This closes the named replacement seam only. Broad
  P18-V01 source/API/settings/input/error/control-flow/reverse-export review,
  arbitrary Java `MessageExt` subclasses, current-revision Ubuntu/native
  Windows P18-V04 evidence, and Phase 19 Broker/Docker scenarios remain open.
  Protected Phase 19 working-tree paths remain untouched and unstaged.


## P18-V01 caller-defined `MessageExt` subtype adapter — 2026-10-21

- [x] Added public `MessageExtSubtype`, retained by shared `Arc` through
  base-typed `MessageExt` APIs. Applications can downcast their custom fields;
  selected virtual hooks cover topic, broker name, queue ID, message ID,
  setters, and display. Clones share runtime subtype state. Serialization and
  deserialization omit that runtime-only adapter.
- [x] The invoked Java `concurrent-custom-subtype` case covers subtype topic,
  retry restoration, namespace stripping, overridden queue/broker/message ID,
  custom fields and `toString`. Rust direct-consume fixture comparison matches
  those callback outcomes. Fixture SHA-256:
  `4246e4a2563d79e9c8e78b2405941a850ad904d48e121e0b3fe009e9ae366263`.
- [x] Focused Rust Common and Consumer verification, Maven fixture verification,
  formatting and strict Clippy passed; full workspace P18-V02 refresh is still
  required after this code change. Full outcomes are recorded in the
  [execution log](../rlogs/2026/09/phase-18-compatibility-release-gate__20261021T180738Z.rlog).
- Broad P18-V01 remains open for inherited `Message`/`MessageExt` overrides
  outside the selected hooks, source/settings/input/error/flow and reverse
  export review. Current Ubuntu/native Windows P18-V04 evidence and Phase 19
  Broker/Docker scenarios remain open; protected Phase 19 files remain
  untouched and unstaged.

## P18-V02 server-free refresh after subtype adapter — 2026-10-21 (`7fd89286`)

- [x] Refreshed default and all-feature locked workspace tests, strict
  all-target/all-feature Clippy, formatting and diff checks on the current
  code revision.
- [x] Pinned Maven `verify` passes all 37 scenarios, including the current
  27-case direct-consume fixture, with 83 typed header frames and 16 typed body
  frames. Differential self-test and 37-row manifest, 16-case × 3 stress
  smoke, release fuzz smoke, release examples, workspace bench compilation,
  and offline benchmark pass.
- [x] Locked Cargo metadata reports 8 workspace members, 216 total packages
  and 208 registry packages. The workspace metadata/dependency/license check,
  all seven product package listings with `LICENSE`/`NOTICE`, and four Unique
  ID tests under both UTC and Asia/Taipei pass. Benchmark observations and
  detailed command outcomes are in the
  [execution log](../rlogs/2026/09/phase-18-compatibility-release-gate__20261021T180738Z.rlog).
- [x] Current local P18-V02 server-free gates and package-content checks pass.
  No release or final compatibility claim is made.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow/reverse-export
  reconciliation remains open. Current GitHub Ubuntu and native Windows
  P18-V04 runtime evidence remains absent; Phase 19 live Broker/Docker
  scenarios remain separate. Protected Phase 19 working-tree paths remain
  untouched and unstaged.

## P18-V01 custom `MessageExt` reconsume-counter hooks — 2026-10-21 (`bcf66469`)

- [x] `MessageExtSubtype` now dispatches Java's virtual
  `getReconsumeTimes` and `setReconsumeTimes` methods. Those getters and setters
  participate in Push retry and send-back decisions.
- [x] The invoked Java custom-subtype fixture calls its setter and observes the
  overridden getter value `13` from the direct-consume callback. Rust compares
  that value and the setter side effect. Fixture SHA-256:
  `c05bde846eae0f39cc654cf49cfa33f624be62165c17cbc2da46fa80d1a77c0a`.
- [x] Default and all-feature locked workspace tests, Common and Consumer
  package tests, strict all-target/all-feature Clippy, format/diff checks, and
  pinned Maven `verify` pass on code revision `bcf66469`.
- This closes the two reconsume-counter virtual hooks. Other inherited
  `Message`/`MessageExt` overrides and broad P18-V01 reconciliation remain
  open; supplemental P18-V02 release gates must still be refreshed after this
  code revision. Current Ubuntu/native Windows P18-V04 evidence and Phase 19
  Broker/Docker scenarios remain separate. Protected Phase 19 paths were not
  changed or staged.

## P18-V01 custom `MessageExt` queue-offset hooks — 2026-10-21 (`a5bfd35b`)

- [x] `MessageExtSubtype` now dispatches Java's virtual `getQueueOffset` and
  `setQueueOffset` methods. Rust `ProcessQueue` uses the custom offset as its
  message-tree key, matching Java `ProcessQueue.putMessage`.
- [x] The invoked Java fixture compares callback offset `987654321` and a real
  ProcessQueue insertion: dispatch result, message count, body size, tree key
  and setter count all match. Fixture SHA-256:
  `00a09f96bb75f96f2ae81111e83c916af4dc47999b7462efdb82163ab8e1fe54`.
- [x] Default and all-feature locked workspace tests, Common and Consumer
  package tests, strict all-target/all-feature Clippy, format/diff checks, and
  pinned Maven `verify` pass on code revision `a5bfd35b`.
- This closes the queue-offset virtual hooks only. Other inherited
  `Message`/`MessageExt` overrides and broad P18-V01 reconciliation remain
  open; supplemental P18-V02 release gates must still be refreshed after this
  code revision. Current Ubuntu/native Windows P18-V04 evidence and Phase 19
  Broker/Docker scenarios remain separate. Protected Phase 19 paths were not
  changed or staged.

## P18-V01 custom `MessageExt` body hooks — 2026-10-21 (`81d67a5e`)

- [x] `MessageExtSubtype` now dispatches Java's virtual `getBody` and
  `setBody` calls. `ProcessQueue.putMessage` reads the subtype body when
  calculating retained message bytes.
- [x] The invoked Java custom-subtype fixture compares body length in the
  callback and byte-size accounting during a real `ProcessQueue.putMessage`.
  Rust matches the setter count and queue byte size. Fixture SHA-256:
  `9b67a09bc850192e2856bc908c10a6dfa6590db3f924b32fe62b6e97edcc510f`.
- [x] Default and all-feature locked workspace tests, Common and Consumer
  package tests, strict all-target/all-feature Clippy, format/diff checks, and
  pinned Maven `verify` pass on code revision `81d67a5e`.
- This closes body getter/setter dispatch for the audited flows. Mutable
  custom body-array aliasing, other inherited `Message`/`MessageExt` overrides
  and broad P18-V01 reconciliation remain open. Supplemental P18-V02 release
  gates must still be refreshed after this code revision. Current
  Ubuntu/native Windows P18-V04 evidence and Phase 19 Broker/Docker scenarios
  remain separate. Protected Phase 19 paths were not changed or staged.

## P18-V01 custom `MessageExt` flag hooks — 2026-10-21 (`a24fba3a`)

- [x] `MessageExtSubtype` now dispatches Java virtual `getFlag` and `setFlag`.
  Push retry/send-back message creation reads the subtype value through the
  virtual getter.
- [x] The invoked Java direct-consume fixture verifies custom flag `71` in the
  callback and records the custom setter call. Rust matches both. Fixture
  SHA-256: `8afd6c83b5085d3c74cbcdd919bca3257514abbde3c7f3ec70847dd08a1a2f7f`.
- [x] Focused direct-consume/ProcessQueue tests, default and all-feature locked
  workspace tests, strict all-target/all-feature Clippy, format/diff checks,
  and pinned Maven `verify` pass on code revision `a24fba3a`.
- This closes the `getFlag`/`setFlag` adapter hooks. Arbitrary subtype behavior,
  mutable custom body-array aliasing, and broad P18-V01 source/API/settings/
  input/error/control-flow/reverse-export reconciliation remain open. The
  supplemental P18-V02 differential, stress, fuzz, example, benchmark,
  metadata/package and time-zone gates still need a current-revision refresh.
  Current Ubuntu/native Windows P18-V04 evidence and Phase 19 Broker/Docker
  scenarios remain separate; protected Phase 19 paths were not changed or
  staged.

## P18-V01 custom `MessageExt.getProperty` hook — 2026-10-21 (`d70771a2`)

- [x] `MessageExtSubtype` now dispatches Java virtual `getProperty(String)`;
  nullable hook results can explicitly return Java null or fall back to the
  stored base properties.
- [x] The invoked Java `ProcessQueue.putMessage` fixture overrides
  `getProperty(MAX_OFFSET)`, records one virtual read, and computes
  `msgAccCnt=12`. Rust matches the count, tree key, queue size and custom getter
  call. Fixture SHA-256:
  `2a70e2a142e6adfa8b43baf7e396515630fcd6dc2231e8d8867e8eab3a4b2db4`.
- [x] Focused Rust `runtime_direct_consume` integration tests and Java fixture
  generation pass on code revision `d70771a2`; full workspace and Maven
  verification are scheduled with the next current-tree gate refresh.
- This closes only the property getter used by ProcessQueue's backlog
  accounting. Other inherited methods, broad P18-V01 reconciliation and
  supplemental P18-V02 gates remain open. Current Ubuntu/native Windows
  P18-V04 evidence and Phase 19 Broker/Docker scenarios remain separate.
  Protected Phase 19 paths were not changed or staged.

## P18-V01 property-derived `MessageExt` tag/key getters — 2026-10-21 (`3230a7c2`)

- [x] Rust `MessageExt::tags()` and `keys()` now call the subtype's virtual
  property hook, matching Java `getTags()`/`getKeys()` delegation to
  `getProperty(String)`.
- [x] The invoked Java custom subtype returns `subtype-tag` and `subtype-key`;
  direct-consume callbacks compare both values and read counts. The ProcessQueue
  `MAX_OFFSET` override still compares `msgAccCnt=12`. Fixture SHA-256:
  `7a5fb470b3e3ad32f55b02d9ac77894b7a98439aff695f4c2a635c4f5515e9d4`.
- [x] Focused `runtime_direct_consume` integration tests and Java fixture
  generation pass on code revision `3230a7c2`; full workspace and Maven
  verification remain part of the next current-tree gate refresh.
- Other inherited getter/setter behavior, broad P18-V01 reconciliation and
  supplemental P18-V02 gates remain open. Current Ubuntu/native Windows
  P18-V04 evidence and Phase 19 Broker/Docker scenarios remain separate;
  protected Phase 19 paths were not changed or staged.

## P18-V01 custom `MessageExt.getProperties` map — 2026-10-21 (`13a68d1d`)

- [x] `MessageExtSubtype` now provides a nullable `getProperties()` map
  snapshot. `MessageExt::properties`, nullable getters and
  `nullable_property_entries()` dispatch to it; Push retry-property copies use
  those entries.
- [x] The invoked Java custom subtype returns a map with `subtype-only=visible`.
  Direct-consume callbacks compare the value and getter count; the ProcessQueue
  `MAX_OFFSET` case still matches `msgAccCnt=12`. Fixture SHA-256:
  `348f65888fbce4d8d36fec26ac88c9279deacb6bbbcaf3514646e2efa3f460e5`.
- [x] Focused `runtime_direct_consume` integration tests and Java fixture
  generation pass on code revision `13a68d1d`; full workspace and Maven
  verification remain part of the current-tree gate refresh.
- Java returns the mutable map reference from `getProperties`; Rust exposes an
  owned subtype snapshot, so map identity and mutation aliasing remain open.
  Other inherited behavior, broad P18-V01 reconciliation and supplemental
  P18-V02 gates remain open. Current Ubuntu/native Windows P18-V04 evidence and
  Phase 19 Broker/Docker scenarios remain separate; protected Phase 19 paths
  were not changed or staged.

## P18-V01 custom `MessageExt` store-host hooks — 2026-10-21 (`ac565eea`)

- [x] `MessageExtSubtype` now dispatches Java virtual `getStoreHost` and
  `setStoreHost`. Push send-back uses the custom host when the broker name is
  absent.
- [x] The Rust send-back regression sets base host `127.0.0.1:10911` but reads
  the subtype override `127.0.0.1:10912` as the request endpoint. The invoked
  Java direct-consume fixture compares the host getter's presence and setter
  count. Fixture SHA-256:
  `3633f3ac0098815df76790a969a15e7a9e2edda45f30723bd67a2753468facab`.
- [x] Focused Rust send-back and direct-consume tests, Java fixture generation,
  formatting and diff checks pass on code revision `ac565eea`; full workspace
  and Maven verification remain part of the current-tree gate refresh.
- Other inherited methods, broad P18-V01 reconciliation and supplemental
  P18-V02 gates remain open. Current Ubuntu/native Windows P18-V04 evidence and
  Phase 19 Broker/Docker scenarios remain separate; protected Phase 19 paths
  were not changed or staged.

## P18-V01 custom `MessageExt` metadata hooks — 2026-10-21 (`8425a30d`)

- [x] `MessageExtSubtype` now dispatches virtual born/store timestamps, born
  host, sysFlag, body CRC, commit-log offset, store size and prepared
  transaction offset getters and setters.
- [x] The invoked Java direct-consume fixture compares each custom getter,
  eight setter calls, virtual host values, and host string/name/byte helpers
  that read the stored host fields directly. Fixture SHA-256:
  `aa202880244ea5606fbebc63d8558965fddede4a95be58baacd56320f8f16571`.
- [x] Focused direct-consume tests, default/all-feature locked workspace tests,
  strict all-target/all-feature Clippy, format/diff checks, and pinned Maven
  `verify` pass on code revision `8425a30d`.
- The supplemental P18-V02 gates were refreshed on `bf126205` after the next
  subtype slice. Other inherited methods and the mutable custom
  `getProperties()` map identity gap remain open. Current Ubuntu/native
  Windows P18-V04 evidence and Phase 19 Broker/Docker scenarios remain
  separate; protected Phase 19 paths were not changed or staged.

## P18-V01 custom `MessageExt` transient sysFlag hooks — 2026-10-21 (`bf126205`)

- [x] `topic_sys_flag()` and `group_sys_flag()` read through the virtual
  property hook; `set_topic_sys_flag()` and `set_group_sys_flag()` dispatch to
  the subtype's Java-virtual setter hooks.
- [x] The invoked Java fixture compares the overridden values `73` and `-19`
  and both setter counts. Fixture SHA-256:
  `71cf67f0bf813630fe084e2be78ea4275a1f04f49063c88a83606e909bb47dbe`.
- [x] Focused direct-consume tests, default/all-feature locked workspace
  tests, strict all-target/all-feature Clippy, format/diff checks, and pinned
  Maven `verify` pass on code revision `bf126205`.
- [x] Refreshed P18-V02 gates on `bf126205`: differential self-test and 37
  scenarios; stress smoke (16 cases × 3); release fuzz (2,368 mutations);
  release examples, workspace benches, metadata/package/license checks, and
  both Unique ID time zones. Full details are in
  `rlogs/2026/09/phase-18-compatibility-release-gate__20261021T192551Z.rlog`.
- [x] Offline benchmark on Apple M5 Max, macOS aarch64, Rust 1.85.0: frame
  encode/decode 2,395.8 ns/iteration; zstd decode 1,172.6 ns; producer request
  construction 1,196.6 ns; route lookup 15.0 ns; loopback completion
  67,861.2 ns. Measurements are observations, not thresholds.
- This closes the inherited property-delegation and setter hooks only; other
  custom `MessageExt` overrides, mutable `getProperties()` map identity, broad
  P18-V01 reconciliation and P18-V04 platform evidence remain open. Phase 19
  Broker/Docker scenarios remain separate.

## P18-V01 inherited scheduling properties and current-tree P18-V02 — 2026-10-21 (`c6048c18`)

- [x] Added subtype `getProperty` dispatch for wait-store, buyer ID, delay
  level, delay seconds, delay milliseconds and delivery milliseconds; added
  subtype setter hooks for wait-store, buyer ID, instance ID, and those delay
  and delivery properties. The Java direct-consume fixture compares the
  overridden values and seven setter calls. Fixture SHA-256:
  `5148df4ac22baa84eb75326238a8d5903927ba1c0380dc34b7cc4093a47bf446`.
- [x] Focused direct-consume tests, serial default and all-feature workspace
  tests, strict all-target/all-feature Clippy, format and diff checks, and
  pinned Maven `verify` pass on code revision `c6048c18`. Maven verified 37
  scenarios, 9 Running Info, 27 direct-consume, 15 reset cases, 83 typed
  headers, 16 typed bodies and Java/Rust cross-decodes.
- [x] Refreshed current-revision P18-V02 server-free gates on `c6048c18`:
  differential self-test and 37 manifest scenarios; offline stress (16 cases
  × 3); release fuzz smoke (137 frame seeds, 11 message seeds, 2,368
  mutations); release examples and workspace benches; metadata, dependency,
  package and license checks; four Unique ID tests under UTC and Asia/Taipei.
  Full record: `rlogs/2026/09/phase-18-compatibility-release-gate__20261021T194233Z.rlog`.
- [x] Offline benchmark on Apple M5 Max, macOS aarch64, Rust 1.85.0: frame
  encode/decode 2,265.5 ns/iteration; zstd decode 1,146.5 ns; producer
  request construction 1,179.5 ns; route lookup 15.7 ns; loopback completion
  71,501.4 ns. These measurements are observations, not thresholds.
- Broad P18-V01 inherited-method/settings/input/error/control-flow/reverse
  export reconciliation and mutable custom `getProperties()` map identity
  remain open. Current-revision GitHub Ubuntu/native Windows P18-V04 evidence
  remains absent. No push, remote CI, Broker or Docker run was performed;
  protected Phase 19 paths were left untouched and unstaged.

## P18-V01 user-property and transaction virtual methods; P18-V02 refresh — 2026-10-21 (`27c872bc`)

- [x] Added subtype dispatch for `getUserProperty`, `hasProperty`, transaction
  ID getter/setter, `putUserProperty`, `setTags` and `setKeys`. The Java
  Collection `setKeys` overload routes through the virtual String overload.
  The direct-consume fixture compares custom values and setter calls and
  verifies that an override handles blank and null `putUserProperty` inputs
  before base validation. Fixture SHA-256:
  `33de50fa577340aa50df1637f1d2f6aa32ae8d23e5e03ba21a2a012630e37542`.
- [x] Focused direct-consume tests, serial default/all-feature workspace
  tests, strict all-target/all-feature Clippy, format/diff checks and pinned
  Maven `verify` pass on `27c872bc`. Maven verified 37 scenarios, 9 Running
  Info, 27 direct-consume, 15 reset cases, 83 typed headers, 16 typed bodies
  and Java/Rust cross-decodes.
- [x] Refreshed current-revision P18-V02 gates on `27c872bc`: differential
  self-test and 37 manifest scenarios; offline stress (16 cases × 3); release
  fuzz (137 frame seeds, 11 message seeds, 2,368 mutations); release examples,
  workspace benches, metadata/package/license/dependency checks, and four
  Unique ID tests under UTC and Asia/Taipei. Full record:
  `rlogs/2026/09/phase-18-compatibility-release-gate__20261021T200032Z.rlog`.
- [x] Offline benchmark on Apple M5 Max, macOS aarch64, Rust 1.85.0: frame
  encode/decode 2,402.6 ns/iteration; zstd decode 1,169.0 ns; producer
  request construction 1,273.6 ns; route lookup 16.2 ns; loopback completion
  62,964.1 ns. Measurements are observations, not thresholds.
- Broad P18-V01 API/settings/input/error/control-flow/reverse-export audit,
  mutable custom `getProperties()` map identity, and current-revision GitHub
  Ubuntu/native Windows P18-V04 evidence remain open. No push, remote CI,
  Broker or Docker run was performed; protected Phase 19 paths were untouched.

## P18-V01 IPv6 host-flag setters and current-revision P18-V02 — 2026-10-21 (`c7354649`)

- [x] Added subtype dispatch for Java `setBornHostV6Flag()` and
  `setStoreHostAddressV6Flag()`. The invoked direct-consume fixture overrides
  both and compares their call count; the base path continues to set the
  corresponding sysFlag bits. Fixture SHA-256:
  `7cf4e8040b8e040117207c8819d67f1dd7bf7f9f0daa6374ce0f7c345bf9171c`.
- [x] Focused direct-consume tests, serial default/all-feature workspace
  tests, strict all-target/all-feature Clippy, format/diff checks and pinned
  Maven `verify` pass on `c7354649`. Maven verified 37 scenarios, 9 Running
  Info, 27 direct-consume, 15 reset cases, 83 typed headers, 16 typed bodies
  and Java/Rust cross-decodes.
- [x] Refreshed current-revision P18-V02 gates on `c7354649`: differential
  self-test and 37 manifest scenarios; offline stress (16 cases × 3); release
  fuzz (137 frame seeds, 11 message seeds, 2,368 mutations); release examples,
  workspace benches, metadata/package/license/dependency checks, and four
  Unique ID tests under UTC and Asia/Taipei. Full record:
  `rlogs/2026/09/phase-18-compatibility-release-gate__20261021T201000Z.rlog`.
- [x] Offline benchmark on Apple M5 Max, macOS aarch64, Rust 1.85.0: frame
  encode/decode 2,392.3 ns/iteration; zstd decode 1,211.7 ns; producer
  request construction 1,294.3 ns; route lookup 14.8 ns; loopback completion
  63,931.1 ns. Measurements are observations, not thresholds.
- Broad P18-V01 API/settings/input/error/control-flow/reverse-export audit,
  mutable custom `getProperties()` map identity, and current-revision GitHub
  Ubuntu/native Windows P18-V04 evidence remain open. No push, remote CI,
  Broker or Docker run was performed; protected Phase 19 paths were untouched.

## P18-V01 live subtype property-map alias and current-revision P18-V02 — 2026-10-21 (`158a049f`)

- [x] Added `MessageExtSubtype::live_properties` for a borrowed shared mutable
  map, and routed subtype property views, nullable entries and
  `MessageExt::shared_properties()` through it. The pinned Java/Rust direct-
  consume fixture verifies map identity, mutation visibility in both
  directions and visibility through a later getter. Snapshot-only subtype
  hooks remain supported. Fixture SHA-256:
  `8782177ecb34305bc00642fffa09420c1fd12ecfe4c8744d450bd4a290ff0d01`.
- [x] Focused Java/Rust direct-consume comparison; serial default and
  all-feature workspace tests; strict all-target/all-feature Clippy;
  formatting/diff checks; and pinned Maven `verify` pass on `158a049f`.
  Maven verified 37 scenarios, 9 Running Info, 27 direct-consume, 15 reset
  cases, 83 typed headers, 16 typed bodies and Java/Rust cross-decodes.
- [x] Current-revision server-free P18-V02 passes: differential comparator
  self-test and 37-scenario manifest; offline stress (16 cases × 3); release
  fuzz (137 frame seeds, 11 message seeds, 2,368 bounded mutations); release
  examples and workspace benches; locked metadata, package/license checks;
  and four Unique ID tests under UTC and Asia/Taipei. Full record:
  `rlogs/2026/09/phase-18-compatibility-release-gate__20261021T202313Z.rlog`.
- [x] Offline benchmark on Apple M5 Max, macOS aarch64, Rust 1.85.0: frame
  encode/decode 2,314.1 ns/iteration; zstd decode 1,132.7 ns; producer
  request construction 1,304.0 ns; route lookup 15.1 ns; loopback completion
  77,214.8 ns. Measurements are observations, not thresholds.
- Broad P18-V01 API/settings/input/error/control-flow/reverse-export audit
  and current-revision GitHub Ubuntu/native Windows P18-V04 evidence remain
  open. No push, remote CI, Broker or Docker run was performed; protected
  Phase 19 paths were untouched.

## P18-V01 born-host string getter overrides and current-revision P18-V02 — 2026-10-21 (`7473872c`)

- [x] Added independent subtype dispatch for Java
  `MessageExt.getBornHostString()` and `getBornHostNameString()`. The invoked
  Java/Rust direct-consume fixture overrides both and compares the custom
  values; the base path keeps reading the stored born-host field. Fixture
  SHA-256: `dec0f7c46539bec116f6430c7b02fdde406a289929fc093d5694dd4c9369bd54`.
- [x] Focused direct-consume comparison, serial default and all-feature
  workspace tests, strict all-target/all-feature Clippy, format/diff checks,
  and pinned Maven `verify` pass on `7473872c`. Maven verified 37 scenarios,
  9 Running Info, 27 direct-consume, 15 reset cases, 83 typed headers, 16
  typed bodies and Java/Rust cross-decodes.
- [x] Current-revision server-free P18-V02 passes: differential self-test
  and 37-scenario manifest; offline stress (16 cases × 3); release fuzz
  (137 frame seeds, 11 message seeds, 2,368 bounded mutations); release
  examples and workspace benches; metadata/package/license checks; and four
  Unique ID tests under UTC and Asia/Taipei. Full record:
  `rlogs/2026/09/phase-18-compatibility-release-gate__20261021T203358Z.rlog`.
- [x] Offline benchmark on Apple M5 Max, macOS aarch64, Rust 1.85.0: frame
  encode/decode 2,477.7 ns/iteration; zstd decode 1,194.2 ns; producer
  request construction 1,367.4 ns; route lookup 15.4 ns; loopback completion
  72,055.8 ns. Measurements are observations, not thresholds.
- Overrides of the host-byte getters and ByteBuffer overloads, broad P18-V01
  API/settings/input/error/control-flow/reverse-export audit, and
  current-revision GitHub Ubuntu/native Windows P18-V04 evidence remain open.
  No push, remote CI, Broker or Docker run was performed; protected Phase 19
  paths were untouched.

## P18-V01 born/store host-byte overrides and current-revision P18-V02 — 2026-10-21 (`625e80c6`)

- [x] Added independent subtype hooks for Java's no-argument
  `getBornHostBytes()` and `getStoreHostBytes()` getters and their
  `ByteBuffer` overloads. The invoked direct-consume fixture overrides all
  four calls and compares returned byte content and caller-buffer writes.
  Rust represents Java `ByteBuffer` with a mutable output slice and reports
  the written byte count; Java object identity and position metadata are not
  represented. Fixture SHA-256:
  `30af1bcbb2cfbf967f636fbbf7cbf9d54ba886a68c25fbe92df5ddf88000946c`.
- [x] Focused direct-consume comparison; default and all-feature workspace
  tests; strict all-target/all-feature Clippy; formatting/diff checks; and
  pinned Maven `verify` pass on `625e80c6`. Maven verified 37 scenarios,
  9 Running Info, 27 direct-consume, 15 reset cases, 83 typed headers, 16
  typed bodies and Java/Rust cross-decodes. Existing JDK Unsafe, Netty Unsafe
  and reflective-final-field warnings were emitted.
- [x] Current-revision server-free P18-V02 passes: differential self-test
  and 37-scenario manifest; offline stress (16 cases × 3); release fuzz
  (137 frame seeds, 11 message seeds, 2,368 bounded mutations); release
  examples and workspace benches; locked metadata/package/license checks;
  and four Unique ID tests under UTC and Asia/Taipei. Full record:
  `rlogs/2026/09/phase-18-compatibility-release-gate__20261021T204424Z.rlog`.
- [x] Offline benchmark on Apple M5 Max, macOS aarch64, Rust 1.85.0: frame
  encode/decode 2,228.9 ns/iteration; zstd decode 1,140.1 ns; producer
  request construction 1,182.9 ns; route lookup 14.7 ns; loopback completion
  62,624.2 ns. Measurements are observations, not thresholds.
- Broad P18-V01 API/settings/input/error/control-flow/reverse-export audit,
  remaining inherited methods, and current-revision GitHub Ubuntu/native
  Windows P18-V04 evidence remain open. No push, remote CI, Broker or Docker
  run was performed; protected Phase 19 paths were left untouched.

## P18-V01 custom `MessageExt` priority overrides and current-revision P18-V02 — 2026-10-21 (`da1f4b1e`)

- [x] Added `MessageExtSubtype` dispatch for Java's virtual `getPriority()`
  and `setPriority(int)`. The inherited getter falls through subtype-aware
  `getProperty(PROPERTY_PRIORITY)`; the base setter preserves negative-value
  rejection and property storage. The invoked Java direct-consume fixture
  overrides both methods, accepts `-7`, and compares the custom value,
  property view, and read/write counts. A property-only subtype verifies
  inherited getter dispatch, base storage of `17`, and negative rejection.
  Fixture SHA-256:
  `44eb8359e7df5735cb527a59942190fb74dd4191306a4290ea3bba644a777a5c`.
- [x] Default and all-feature locked workspace tests; strict all-target,
  all-feature Clippy; format and diff checks; and pinned Maven `verify` pass.
  Maven verifies 37 scenarios, 9 Running Info, 27 direct-consume, 15 reset
  cases, 83 typed headers, 16 typed bodies and Java/Rust cross-decodes.
- [x] Current-revision server-free P18-V02 passes: differential comparator
  self-test and 37-scenario manifest; offline race/leak smoke (16 cases × 3);
  release fuzz smoke (137 frame seeds, 11 message seeds, 2,368 bounded
  mutations, 201 frame decodes, 118 message decodes); facade examples and
  workspace bench compilation; locked Cargo metadata, workspace metadata and
  dependency-direction checks; seven package listings with `LICENSE` and
  `NOTICE`; and four Core Unique ID tests under UTC and Asia/Taipei.
- [x] Offline benchmark on Apple M5 Max, macOS aarch64, Rust 1.85.0:
  frame encode/decode 2,266.9 ns/iteration; zstd decode 1,153.2 ns; producer
  request construction 1,297.8 ns; route lookup 15.7 ns; loopback completion
  69,060.5 ns. Allocation counts/bytes per iteration: 40/4,676; 17/197,493;
  54/3,665; 0/0; 81/9,546. Measurements are observations, not thresholds.
- Broad P18-V01 source/API/settings/input/error/control-flow/reverse-export
  reconciliation and current-revision GitHub Ubuntu/native Windows P18-V04
  evidence remain open. No push, remote CI, Broker or Docker run was performed;
  protected Phase 19 paths were left untouched.

## P18-V01 Broker registration APIs and current-revision workspace gates — 2026-10-21 (`ada87ea9`)

- [x] Added `MQClientApi::add_broker` and `remove_broker` for pinned Java
  requests 902/903, including nullable public typed headers, signed broker ID,
  direct address handling, caller timeout, and success/error handling.
  Invoked Java fixtures cover header fields, nulls, ID boundaries, and
  operation results; Rust loopbacks verify requests, outcomes, and timeout
  enforcement. Producer core fixture SHA-256:
  `9510b46415e7f00c7932c95d9e33dbb02e9b9cc56d1afecec01dfede60ae5e6a`.
- [x] `cargo test --workspace --locked --quiet` and
  `cargo test --workspace --all-features --locked --quiet` pass on
  `ada87ea9`. Pinned `mvn -q -f compat/java-reference/pom.xml verify` passes
  with 37 scenarios, 85 typed headers, 16 typed bodies, and Java/Rust
  cross-decodes. Existing JDK/Netty `Unsafe` and reflective-final-field
  warnings were emitted.
- [x] Strict all-target/all-feature Clippy passed on `ada87ea9`; comparator
  self-test and manifest validation passed on the current P18 test inventory.
  Documentation whitespace checks are recorded in the slice log.
- [x] Pinned-source review classifies the seven controller administration
  facade workflows (`getControllerMetaData`, `getInSyncStateData`,
  `getBrokerEpochCache`, `getControllerConfig`, `updateControllerConfig`,
  `electMaster`, `cleanControllerBrokerData`) as
  `TOOLS_OUT_OF_SCOPE`, consistent with Phase 17's separate
  `rocketmq-tools` exclusion.
- Broad P18-V01 source/API/settings/input/error/control-flow/reverse-export
  reconciliation and current-revision GitHub Ubuntu/native Windows P18-V04
  evidence remain open. No push, remote CI, Broker, or Docker run was
  performed; protected Phase 19 paths remain untouched.

## P18-V01 `MessageClientExt` offset-ID subtype overrides — 2026-10-21 (`f7d7e8f1`)

- [x] `MessageExtSubtype` now dispatches `MessageClientExt.getOffsetMsgId()`
  and `setOffsetMsgId(String)` for caller-defined subtypes. The inherited
  `getMsgId()` path retains Java's live `UNIQ_KEY` precedence and virtual
  offset-ID fallback. Rust's `MessageClientExt` wrapper now routes these
  getters/setters through the retained runtime subtype while the base
  `setMsgId` remains a no-op.
- [x] Invoked Java direct-consume fixtures add offset-only custom subclasses
  with and without `UNIQ_KEY`; the Rust wrapper test also covers setter
  dispatch, unique-ID mutation/precedence, and conversion to `MessageExt`.
  The direct runtime fixture now has 29 cases. SHA-256:
  `403b79b69f30957ae2f935be37ec0b8537696909d91066d2126f216eff31996d`.
- [x] `setOffsetMsgId(null)` clears the offset value and effective fallback;
  the base-typed `MessageExt` view keeps both cleared. `records.json` SHA-256:
  `1adc11e14bf1af0d39966ecda166a3ddb8a24c0e7d13bbf2d5a9830bdaac64dc`.
- [x] Default and all-feature locked workspace tests; strict all-target,
  all-feature Clippy; formatting/diff checks; and pinned Maven `verify` pass
  on `f7d7e8f1`. Maven verified 37 scenarios, 9 Running Info, 29 direct-consume,
  15 reset cases, 85 typed headers, 16 typed bodies, and Java/Rust
  cross-decodes. Existing JDK/Netty `Unsafe` and reflective-final-field
  warnings were emitted.
- P18-V01 inherited-method, public API, settings/input/error/control-flow and
  reverse-export reconciliation remains open. The complete supplemental
  current-revision P18-V02 stress/fuzz/benchmark/package refresh and GitHub
  Ubuntu/native Windows P18-V04 evidence remain open. No push, remote CI,
  Broker or Docker run was performed; protected Phase 19 paths remain untouched.

## P18-V01 custom `MessageClientExt` message-ID overrides — 2026-10-21 (`5e12a174`)

- [x] Invoked Java and Rust fixtures cover custom `getMsgId` and `setMsgId`
  overrides. The getter bypasses base `UNIQ_KEY` lookup; the setter handles a
  replacement and null without mutating the physical offset ID. The Common
  message API test file passes all 26 cases. `records.json` SHA-256:
  `d0cdfbf2599adb0242a1df0dfa8523818a018c35cb34dac2b8418f8ce4bc972d`.
- [x] Strict all-target/all-feature Clippy, formatting/diff checks, and pinned
  Maven `verify` pass. Maven verified 37 scenarios, 9 Running Info, 29 direct-
  consume cases, 15 reset cases, 85 typed headers, 16 typed bodies, and
  Java/Rust cross-decodes. Existing JDK/Netty `Unsafe` and reflective-final-
  field warnings were emitted.
- Broad P18-V01 source/API/settings/input/error/control-flow/reverse-export
  reconciliation remains open. The current-revision P18-V02 full refresh and
  P18-V04 platform evidence remain open; no remote CI, Broker, or Docker run
  was performed.

## P18-V02 full current-revision server-free refresh — 2026-10-21 (`0f6a0244`)

- [x] Default and all-feature locked workspace test suites, strict
  all-target/all-feature Clippy, formatting and diff checks pass on current
  revision `0f6a0244`. Pinned Maven `verify` passes with 37 scenarios, 9
  Running Info, 29 direct-consume, 15 reset cases, 85 typed headers, 16 typed
  bodies, fixture drift checks and Java/Rust cross-decodes.
- [x] Differential comparator self-test and 37-scenario manifest validation
  pass. The deterministic offline race/leak smoke passes 16 cases × 3;
  release fuzz smoke passes 139 frame seeds, 11 message seeds and 2,400
  bounded mutations (200 frame decodes, 113 message decodes). Core Unique ID
  fixture tests pass with `TZ=UTC` and `TZ=Asia/Taipei`.
- [x] Release examples build; all workspace benches compile; the offline
  benchmark runs. Locked Cargo metadata reports 8 workspace packages, 216
  total packages and 208 registry packages. Workspace metadata/license/
  dependency-direction checks pass; all seven product package listings
  contain `LICENSE`, `NOTICE` and `src/lib.rs`. Phase 18 benchmark observations
  are appended to
  [the offline baseline](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
- This closes the current server-free P18-V02 refresh only. Broad P18-V01
  reconciliation and current-revision GitHub Ubuntu/native Windows P18-V04
  evidence remain open. Phase 19 Broker/Docker checks remain separate; no
  remote CI, Broker or Docker run was performed. Protected Phase 19 paths
  remain untouched and unstaged.

## P18-V03 current-revision provenance and package recheck — 2026-10-21 (`79406d19`)

- [x] Offline Maven dependency tree resolves
  `rocketmq-client:5.5.1 -> rocketmq-remoting:5.5.1 -> rocketmq-common:5.5.1`.
  All nine documented local artifact SHA-256 values match, and four selected
  source-JAR files byte-match the files at pinned tag commit
  `efa1be59d6fc747ad220f036c86030f05f0419f9`.
- [x] Locked Cargo metadata, workspace license/dependency-direction checks and
  package-content lists pass: eight workspace members, 216 total packages,
  208 registry packages, and seven product packages each containing
  `LICENSE`, `NOTICE` and `src/lib.rs`. The leaf-to-facade package order and
  Java 5.5.1 / Broker 5.3.4 baseline remain documented in the provenance file.
- [x] The README and compiled facade Producer example now document the Java
  start/send/shutdown usage flow, ambiguous timeout and duplicate-delivery
  behavior, idempotent handling, and bounded Push consumer shutdown.
- P18-V03 local preparation review is complete. The signed tag's target and
  source-JAR bytes are verified, but its OpenPGP signature is not independently
  verified because GPG is unavailable. Release version selection and registry
  dependency resolution remain post-Phase-19 tasks; no publication or release
  claim is made. Broad P18-V01 and current-revision GitHub Ubuntu/native
  Windows P18-V04 evidence remain open.

## P18-V01 message-family method crosswalk — 2026-10-21

- [x] Crosswalk all pinned 5.5.1 public instance methods declared by `Message`
  (34), `MessageExt` (41), and `MessageClientExt` (4), with constructors and
  virtual subtype hooks mapped separately, plus all three `MessageExt` static
  helpers (82 method declarations total):
  [message-family crosswalk](compatibility/PHASE_18_MESSAGE_SUBTYPE_API_CROSSWALK.md).
- [x] Pinned Java consumer-operation fixture hash:
  `a8d82baca384b9b7932b4961b051902336155543428047d891eb9f84052d166d`;
  Common message tests 26/26, direct-consume tests 4/4, and offline Maven
  `verify` passed (29 direct-consume cases and bidirectional fixture
  cross-decodes).
- This closes the declared message-family method slice and supersedes its
  stale inherited-method/live-map open notes. Other P18-V01 public API,
  settings, control-flow, input/error and reverse-export reconciliation, plus
  current-revision GitHub Ubuntu/native Windows P18-V04 evidence, remain open.

## P18-V02 current committed refresh — 2026-10-22 (`80faab5a`)

- [x] The server-free Rust refresh passed on the source tree committed as
  `80faab5a`: formatting, strict all-target/all-feature Clippy, default and
  all-feature workspace tests, UTC and Asia/Taipei Unique ID regression,
  deterministic fuzz smoke, offline race/leak smoke, release facade examples,
  and workspace bench compilation. The fuzz run exercised 163 frame seeds,
  11 message seeds and 2,784 bounded mutations; the race/leak run passed 16
  cases × 3 repeats.
- [x] Differential comparator self-test and 37-scenario manifest validation,
  workspace metadata and package checks, and `git diff --check` passed.
- [x] Pinned offline `mvn -o -q -f compat/java-reference/pom.xml verify`
  passed at `80faab5a`, including 37 scenarios, 73 ClientConfig identity/
  default/clone checks, typed header decode fixtures, and Java decoding 106
  Rust-generated header fixtures and 19 typed body fixtures. The offline
  benchmark was captured at this revision and appended to the benchmark
  baseline.
- [x] The five named Java 5.5.1 regressions have local source/test coverage:
  request/reply exact-once completion and timeout/failure races
  (`concurrent_reply_and_timeout_claim_exactly_one_terminal_owner`,
  `reply_races_with_send_failure_or_cancellation_without_double_claim`, and
  `send_failure_completes_request_and_callback_once`); same-key and
  unrelated-key runtime construction (`p6_v01_many_first_lookups_share_one_unstarted_runtime_and_transport_owner`,
  `unrelated_keys_construct_concurrently_and_same_key_constructs_once`);
  blank local serve address mapped to Proxy-only source behavior (outside the
  client implementation); optimized Remoting header encoding
  (`p2_v01_private_codec_matches_java_fast_and_ordinary_header_layouts`,
  `header_prefix_apis_match_invoked_java_551`); and producer batch validation
  (`message_batch_generate_from_list_matches_java_validation_order`,
  `batch_validation_decisions_match_pinned_java_fixtures`).
- [x] Cross-compilation of the exact native Windows stack regression test
  binary and `rocketmq-classic-core --all-targets` passed for
  `x86_64-pc-windows-gnu`. The native Windows stack assertion itself remains
  for the Windows CI runner to execute; this macOS host has no Windows runtime.
- This closes the listed server-free refresh only. Broad P18-V01 source,
  settings, input/error, control-flow and reverse-export reconciliation
  remains open. Current remote Ubuntu/native Windows CI evidence is also
  pending; no Broker, Docker or remote CI run was performed.

## P18-V02 current-revision verification — 2026-10-22 (`7bca8336`)

- [x] On the current Phase 18 branch, `cargo fmt --all -- --check`, strict
  workspace/all-feature Clippy, default and all-feature locked workspace
  tests, the Asia/Taipei Unique ID regression, comparator self-test,
  37-scenario handoff validation, bounded fuzz smoke, and 16-case × 3
  deterministic race/leak smoke passed. Workspace metadata, license and
  dependency-direction checks passed.
- [x] Maven `verify` passed under the pinned local Azul JDK 21 after the
  JDK-27-specific runtime address oracle was temporarily regenerated for the
  local host; the checked-in oracle was restored byte-for-byte. Producer
  fixtures, Java self-tests, and Java decoding of Rust-generated headers and
  bodies passed. The execution log records the JDK handling and full command
  output.
- [x] Facade examples build in release mode. The offline benchmark compiles
  and runs; its fixed inputs, iteration counts, Apple M5 Max / ARM64 / rustc
  1.85.0 environment and measured timings/allocation observations are in the
  Phase 18 execution log. No throughput acceptance threshold was introduced.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation remains open. Current-revision remote Ubuntu CI and
  native Windows stack-capture execution also remain pending; no Broker,
  Docker, publication, or release action was performed.

## P18-V04 current-tree Windows compile refresh — 2026-10-22 (`8215f1d9`)

- [x] `cargo check -p rocketmq-classic-core --all-targets --target
  x86_64-pc-windows-gnu --locked` passed. The exact consumer stack-capture test
  cross-compiled with `cargo test --target x86_64-pc-windows-gnu -p
  rocketmq-classic-consumer --lib
  stack::tests::jstack_includes_current_and_other_named_threads --no-run
  --locked`.
- [ ] The cross-target compile does not execute the Windows stack assertion.
  Native Windows test execution and current-revision GitHub CI evidence remain
  pending; broad P18-V01 remains open.

## P18-V01 subtype getter and local gate refresh — 2026-10-22 (`2f65745b`)

- [x] The direct Java `Message` derived-getter override slice is recorded in
  the subtype crosswalk and API map. The invoked fixture and Rust differential
  cover ten virtual getters and their call count.
- [x] `cargo fmt --all -- --check`, strict workspace all-target/all-feature
  Clippy, default and all-feature locked workspace tests, and `git diff --check`
  passed. Common tests and the direct-consume integration suite also
  passed.
- [x] Pinned Maven `verify` passed under Azul JDK 21 after the runtime address
  fixture was temporarily regenerated for that JDK. The original address
  oracle and the two unrelated generated subscription-group wrapper fixtures
  were restored byte-for-byte. Java fixture verification, Java self-tests,
  37 differential scenarios, and Java decoding of Rust-generated headers and
  bodies passed.
- [x] Differential comparator self-test and the 37-scenario manifest passed;
  deterministic race/leak smoke passed (16 cases × 3); fuzz smoke passed
  (163 frame seeds, 11 message seeds, 2,784 mutations, 245 frame decodes, 112
  message decodes). Workspace metadata, package licenses, and dependency
  direction passed.
- [x] Facade examples built in release mode and the offline benchmark compiled.
  Core and Consumer Windows cross-target checks passed; the exact stack test
  cross-compiled. These checks do not execute the stack assertion on Windows.
- [ ] Broad P18-V01 settings/source/input/error/control-flow and reverse-export
  reconciliation remains open. Current-revision remote Ubuntu CI and native
  Windows stack-capture execution also remain pending. No Broker, Docker,
  publication, or release action was performed.

## P18-V01 nullable subtype getter continuation — 2026-10-22 (`d4f1a4f1`)

- [x] An invoked Java subtype returns null from five overridden getters while
  `getProperty` returns non-null or malformed values. Rust `Some(None)` results
  preserve direct nulls and skip malformed transient sysflag parsing. Fixture
  SHA-256: `8640635c5eb044d1485c182be377a308f909885477d01916ba234fb34ef70b7b`.
- [x] Java fixture verification, all six direct-consume tests, focused
  Common/Consumer strict Clippy, formatting, and `git diff --check` passed.
  Production code is unchanged since `2f65745b`, whose full workspace/Maven
  gates are recorded above.
- [ ] Broad P18-V01 reconciliation, current-revision remote Ubuntu CI, and
  native Windows stack execution remain pending.

## P18-V02 full current-revision server-free refresh — 2026-10-22 (`af04d054`; docs `5400211e`)

- [x] `cargo fmt --all -- --check`, strict all-target/all-feature workspace
  Clippy, default locked workspace tests, and all-feature locked workspace
  tests pass. The full test run includes the new Producer startup-route and
  cache-hit regressions. The Unique ID fixture passes under both UTC and
  `TZ=Asia/Taipei`.
- [x] Differential comparator self-test and 37-scenario handoff manifest
  validation pass. Fixed-seed fuzz smoke covers 163 frame seeds, 11 message
  seeds, 2,784 bounded mutations, 245 frame decodes, and 112 message decodes.
  The deterministic race/leak smoke passes all 16 cases three times.
- [x] Pinned `mvn -f compat/java-reference/pom.xml verify` passes on Java 27,
  including fixture self-tests, runtime config/route/heartbeat probes, 37
  scenarios, Java decoding of 106 Rust-generated typed headers and 19 typed
  bodies. Maven regenerated no tracked fixture changes.
- [x] Workspace metadata/license/dependency-direction checks pass. Facade
  examples build in release mode; the offline benchmark compiles and runs.
  Measurements are recorded in
  [the benchmark baseline](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md)
  for macOS AArch64 / Apple M5 Max / Rust 1.85.0; no performance threshold is
  asserted.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation remains open. Current-revision GitHub Ubuntu CI and
  native Windows stack-capture execution remain pending. No Broker, Docker,
  publication, or release action was performed; protected Phase 19 worktree
  files remain unstaged.

## P18-V04 Windows cross-target refresh — 2026-10-22 (`af04d054`)

- [x] On code revision `af04d054`, `cargo check -p rocketmq-classic-core
  --all-targets --target x86_64-pc-windows-gnu --locked` passed. The exact
  Consumer stack-capture test cross-compiled with `cargo test --target
  x86_64-pc-windows-gnu -p rocketmq-classic-consumer --lib
  stack::tests::jstack_includes_current_and_other_named_threads --no-run
  --locked`.
- [ ] Cross-compilation does not execute the Windows stack assertion.
  Native Windows execution and current-revision GitHub Ubuntu CI evidence
  remain pending. Broad P18-V01 source/API/settings/input/error/control-flow
  and reverse-export reconciliation remains open.

## P18-V02 startup-route regression gate refresh — 2026-10-22 (`561bae6d`)

- [x] The test at this checkpoint confirmed a NameServer transport failure
  during configured-topic initialization failed Producer startup and asserted
  that its runtime registration lease was released. The lease assertion was
  later found to differ from pinned Java and is superseded by the
  `4cabbffb` correction below. Producer package tests passed (154 unit, 11
  admin, 27 facade, 3 request/reply, 18 scripted-send, 7 transaction, and 7
  doctest cases); strict Producer Clippy passed.
- [x] Formatting, strict workspace all-target/all-feature Clippy, default and
  all-feature locked workspace tests, differential self-test and 37-scenario
  manifest validation pass. Fixed-seed fuzz smoke and the 16-case × 3
  deterministic race/leak smoke pass.
- [x] Pinned Java `mvn -f compat/java-reference/pom.xml verify` passes under
  Java 27, including runtime probes, 37 scenarios, Java decoding of 106 typed
  headers and 19 typed bodies. Maven regenerated no tracked fixture changes.
  Workspace metadata/license/dependency checks and release example builds
  pass; the offline benchmark compiled and ran. Its sample is recorded in
  [the benchmark baseline](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation remains open. Current-revision GitHub Ubuntu CI and
  native Windows stack-capture execution remain pending. No Broker, Docker,
  publication, or release action was performed; protected Phase 19 worktree
  files remain unstaged.

## P18-V01 startup-route error parity correction — 2026-10-22 (`4cabbffb`)

- [x] A pinned Java 5.5.1 loopback probe confirms that a configured-topic
  route transport failure throws `IllegalStateException` caused by
  `RemotingSendRequestException`, leaves Producer state `START_FAILED`, and
  keeps the Producer group in the factory registry. Rust now preserves that
  registration after the same post-start route failure. The invoked result is
  in `fixtures/producer/lifecycle.json`, and the Rust test compares it.
- [x] The Producer package suite passes (154 unit, 11 admin, 27 facade, 3
  request/reply, 18 scripted-send, 7 transaction, and 7 doctest cases), strict
  Producer Clippy, formatting, and `git diff --check`. The exact Producer
  regression test cross-compiles for `x86_64-pc-windows-gnu`.
- [ ] At this checkpoint, full workspace and Maven refresh for `4cabbffb` was
  pending; the branch-level refresh is recorded in the later
  `cd212ffb` section below. Broad P18-V01 source/API/settings/input/error/
  control-flow and reverse-export reconciliation, current-revision GitHub
  Ubuntu CI, and native Windows stack-capture execution remain open. No
  Broker, Docker, publication, or release action was performed; protected
  Phase 19 worktree files remain unstaged.

## P18-V01 Producer factory-start failure parity and gate refresh — 2026-10-22 (`cd212ffb`)

- [x] A pinned Java 5.5.1 fixture confirms `IllegalArgumentException` during
  client factory startup leaves Producer and factory in `START_FAILED` and
  retains the Producer group in the factory table. Rust now preserves the
  registration and failed factory identity while cleaning runtime-owned
  transport/workers. Fixture: `startupFactoryStartFailure` in
  `fixtures/producer/lifecycle.json`; regression:
  `runtime_start_failure_preserves_java_factory_registration`.
- [x] Default and all-feature locked workspace tests and strict workspace
  all-target/all-feature Clippy pass. Formatting, `git diff --check`,
  differential comparator self-test, all 37 manifest scenarios, and workspace
  metadata/license/dependency checks pass.
- [x] Java reference `mvn -f compat/java-reference/pom.xml verify` passes,
  including Producer lifecycle verification, 37 scenarios, Java decoding of
  106 typed headers and 19 typed bodies. Fixed-seed fuzz smoke passes with 163
  frame seeds, 11 message seeds, 2,784 mutations, 245 frame decodes and 112
  message decodes. Offline deterministic race/leak smoke passes all 16 cases
  × 3 repetitions.
- [x] Release facade examples build; the offline benchmark compiles and runs.
  The current sample is recorded in
  [the benchmark baseline](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
  Core all-target Windows cross-compilation and the exact Producer lifecycle
  regression cross-compile for `x86_64-pc-windows-gnu`.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation, current-revision GitHub Ubuntu CI, and native
  Windows stack-capture execution remain open. No Broker, Docker, publication,
  or release action was performed; protected Phase 19 worktree files remain
  unstaged.

## P18-V01 push/pull Consumer factory-start parity and gate refresh — 2026-10-22 (`b46612bf`)

- [x] Pinned Java 5.5.1 probes confirm that both push and pull Consumer
  startup throw `IllegalArgumentException` for an invalid factory schedule,
  retain the consumer group, and leave Consumer and factory in
  `START_FAILED`. Rust preserves the registration after failed runtime start
  and stops owned push workers. The Java oracle is
  `fixtures/consumer/lifecycle.json`; push and pull regression tests compare
  the lifecycle results.
- [x] Default and all-feature locked workspace tests and strict workspace
  all-target/all-feature Clippy pass. Formatting, `git diff --check`,
  differential comparator self-test, all 37 manifest scenarios, and workspace
  metadata/license/dependency checks pass.
- [x] Pinned Java `mvn -f compat/java-reference/pom.xml verify` passes,
  including Producer and Consumer lifecycle fixtures, 37 scenarios, Java
  decoding of 106 typed headers and 19 typed bodies. Fixed-seed fuzz smoke
  passes with 163 frame seeds, 11 message seeds, 2,784 mutations, 245 frame
  decodes and 112 message decodes. Deterministic race/leak smoke passes all 16
  cases × 3 repetitions.
- [x] Release facade examples build; the offline benchmark compiles and runs.
  Its current sample is in
  [the benchmark baseline](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
  Core all-target Windows checking and the exact Consumer lifecycle
  regressions cross-compile for `x86_64-pc-windows-gnu`.
- [ ] LitePull lifecycle, broad P18-V01 source/API/settings/input/error/
  control-flow and reverse-export reconciliation, current-revision GitHub
  Ubuntu CI, and native Windows stack-capture execution remain open. No Broker,
  Docker, publication or release action was performed; protected Phase 19
  worktree files remain unstaged.

## P18-V01 LitePull factory-start parity and gate refresh — 2026-10-22 (`ad7a0523`)

- [x] The invoked Java 5.5.1 LitePull probe confirms that an invalid factory
  schedule throws `IllegalArgumentException`, leaves Consumer and factory in
  `START_FAILED`, retains the consumer group, and makes a retry throw
  `MQClientException`. Fixture:
  `litePullFactoryStartFailure` in `fixtures/consumer/lifecycle.json` (SHA-256
  `26fadf1fbcc7b51c64e17c10626347dbc59a520279948f030ff6359b15195387`). Rust
  now preserves that failed lifecycle and registration; the regression checks
  runtime identity and explicit cleanup.
- [x] `cargo test --workspace --locked` and
  `cargo test --workspace --all-features --locked` pass. Strict workspace
  all-target/all-feature Clippy, formatting, diff checks, differential
  comparator self-test, all 37 manifest scenarios, and workspace
  metadata/license/dependency checks pass.
- [x] Pinned Java `mvn -f compat/java-reference/pom.xml verify` passes,
  including Producer, push/pull Consumer and LitePull lifecycle fixtures,
  37 scenarios, Java decoding of 106 typed headers and 19 typed bodies.
  Fixed-seed fuzz smoke passes with 163 frame seeds, 11 message seeds, 2,784
  mutations, 245 frame decodes and 112 message decodes. Deterministic
  race/leak smoke passes all 16 cases × 3 repetitions.
- [x] Release facade examples build; the offline benchmark compiles and runs.
  Its current sample is recorded in
  [the benchmark baseline](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
  Core all-target Windows checking and the exact LitePull lifecycle regression
  cross-compile for `x86_64-pc-windows-gnu`.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation, current-revision GitHub Ubuntu CI, and native
  Windows stack-capture execution remain open. No Broker, Docker, publication
  or release action was performed; protected Phase 19 worktree files remain
  unstaged.

## P18-V01 LitePull listener-route failure parity and gate refresh — 2026-10-22 (`aab73bb3`)

- [x] The invoked Java 5.5.1 probe confirms that a route failure while loading
  pre-registered LitePull listener queues throws `MQClientException`, shuts
  down Consumer and factory to `SHUTDOWN_ALREADY`, removes the Consumer group,
  and rejects retry. Fixture: `litePullListenerRouteFailure` in
  `fixtures/consumer/lifecycle.json` (SHA-256
  `841aac58bbeaf12908aebc79796f75885f00e5356ad61dd06aed4cbf4e0ef9f3`). Rust
  now propagates the route error and rolls back runtime and registration.
- [x] Default and all-feature locked workspace tests and strict workspace
  all-target/all-feature Clippy pass. Formatting, diff checks, differential
  comparator self-test, all 37 manifest scenarios, and workspace
  metadata/license/dependency checks pass.
- [x] Pinned Java `mvn -f compat/java-reference/pom.xml verify` passes,
  including all Producer and Consumer lifecycle fixtures, 37 scenarios, Java
  decoding of 106 typed headers and 19 typed bodies. Fixed-seed fuzz smoke
  passes (163 frame seeds, 11 message seeds, 2,784 mutations, 245 frame
  decodes and 112 message decodes). Deterministic race/leak smoke passes all
  16 cases × 3 repetitions.
- [x] Release facade examples build; the offline benchmark compiles and runs.
  Its current sample is recorded in
  [the benchmark baseline](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
  Core all-target Windows checking and the exact LitePull route-failure
  regression cross-compile for `x86_64-pc-windows-gnu`.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation, current-revision GitHub Ubuntu CI, and native
  Windows stack-capture execution remain open. No Broker, Docker, publication
  or release action was performed; protected Phase 19 worktree files remain
  unstaged.

## P18-V01 LitePull post-start listener-route parity and gate refresh — 2026-10-22 (`148308fb`)

- [x] Pinned Java 5.5.1 confirms a failed route lookup during listener
  registration throws `MQClientException`, retains the listener, leaves
  Consumer and factory `RUNNING`, and throws again on retry. Fixture
  `litePullListenerRegistrationRouteFailure` in
  `fixtures/consumer/lifecycle.json` has SHA-256
  `a17cfdae54f83a7a1ee3d1612f9bb5509175936da1adf289f799a48e232c4db5`.
  Rust registration now returns the route error while retaining the listener;
  the regression verifies retry and callback recovery after NameServer
  recovery. The public method is now async, so callers need to `.await` it.
- [x] `cargo test -p rocketmq-classic-consumer --locked` passes (251 unit
  tests, package integration tests and 13 doctests); strict Consumer
  all-target/all-feature Clippy, formatting, diff checks, and the exact
  regression cross-compile for `x86_64-pc-windows-gnu` pass.
- [x] Pinned Java `mvn -f compat/java-reference/pom.xml verify` passes,
  including lifecycle fixture verification, 37 scenarios, and Java decoding
  of 106 typed headers and 19 typed bodies.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation, current-revision GitHub Ubuntu CI, and native
  Windows stack-capture execution remain open. No Broker, Docker, publication
  or release action was performed; protected Phase 19 worktree files remain
  unstaged.

## P18-V01 LitePull broker subscription-check parity and gate refresh — 2026-10-22 (`c04e57ee`)

- [x] The invoked Java 5.5.1 SQL92 startup probe returns Broker response code
  44 to `CHECK_CLIENT_CONFIG`. Java throws `MQClientException`, shuts down
  Consumer and factory, and removes the group. Fixture
  `litePullBrokerSubscriptionCheckFailure` in
  `fixtures/consumer/lifecycle.json` has SHA-256
  `0087e05cd0480a8a6cc6b993324f0ee8ad2ce6d8f58ab8ef8175813908023369`.
  Rust refreshes subscription routes before the check and matches the
  shutdown and registration result.
- [x] `cargo test -p rocketmq-classic-consumer --locked` passes (251 unit
  tests, package integration tests and 13 doctests); strict Consumer
  all-target/all-feature Clippy, formatting, diff checks, and the exact
  SQL92 regression cross-compile for `x86_64-pc-windows-gnu` pass.
- [x] Pinned Java `mvn -f compat/java-reference/pom.xml verify` passes,
  including lifecycle fixture verification, 37 scenarios, and Java decoding
  of 106 typed headers and 19 typed bodies.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation, current-revision GitHub Ubuntu CI, and native
  Windows stack-capture execution remain open. No Broker, Docker, publication
  or release action was performed; protected Phase 19 worktree files remain
  unstaged.

## P18-V01 LitePull listener-registration lifecycle serialization — 2026-10-22 (`33b8084d`)

- [x] Pinned Java reflection confirms `start()`, `shutdown()`, and
  `registerTopicMessageQueueChangeListener()` synchronize on the LitePull
  implementation monitor. Fixture `litePullMethodSynchronization` is in
  `fixtures/consumer/lifecycle.json` (SHA-256
  `4972ae18d32a4aa337b1c4aad81153b81fa86a28112e505bceab8a660693ac5b`). Rust
  listener registration now takes the lifecycle gate before the reconciliation
  gate; a paused-Broker regression proves registration waits for startup to
  finish.
- [x] `cargo test -p rocketmq-classic-consumer --locked` passes (251 unit
  tests, all package integration tests including 22 Lite/POP tests, and 13
  doctests). Strict Consumer all-target/all-feature Clippy, formatting, and
  diff checks pass. Pinned Java `mvn -f compat/java-reference/pom.xml verify`
  passes with all 37 scenarios and Java decoding of 106 typed headers and 19
  typed bodies.
- [ ] Broad P18-V01 reconciliation, current-revision GitHub Ubuntu CI, and
  native Windows stack-capture execution remain open. No Broker, Docker,
  publication, or release action was performed; protected Phase 19 worktree
  files remain unstaged.

## P18-V01 `listUser` object-root parity and package recheck — 2026-10-22 (`c6ffd6bf`; docs `ae8854f3`)

- [x] Rust preserves pinned Fastjson 2.0.63's `decodeList` object-root
  behavior as a serialized String list element. The Core loopback differential
  compares all eight Java fixture outcomes, including `{}` -> `["{}"]`.
  `fixtures/producer/core.json` SHA-256:
  `ae8e09607bfaac817640a03dc596675757a511c26e03e7d9f0ca7e8a51201cd8`.
- [x] The focused Core differential, Protocol all-feature tests, locked
  default/all-feature workspace tests, strict all-target/all-feature Clippy,
  formatting/diff checks, pinned Maven verify (37 scenarios), differential
  self-test/manifest, fuzz smoke (163 frame seeds, 11 message seeds, 2,784
  mutations), race/leak smoke (16 cases × 3), release examples, and offline
  benchmark build pass on source revision `c6ffd6bf`.
- [x] Workspace metadata/dependency/license checks and all seven locked
  product package listings pass; each package includes `LICENSE` and
  `NOTICE`. Consumer all-target/all-feature Windows GNU cross-check passes.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation, current-revision GitHub Ubuntu CI, and native
  Windows stack-capture execution remain open. Phase 19 Broker/Docker
  validation remains separate. No push, remote CI, Broker, Docker,
  publication, or release action was performed; protected Phase 19 worktree
  files remain unstaged.

## P18-V01 writer-feature inventory and `UserInfo` surface — 2026-10-22 (`c5892d2c`)

- [x] Reconciled the stale `47/48` inventory: Rust implements all 48 pinned
  Fastjson writer features for the mapped body, route, and heartbeat adapters.
  Java fixture differentials compare feature output across 41 body, 13 route,
  and 3 heartbeat cases; the enum test asserts all pinned variants are mapped.
- [x] Removed `UserInfo::java_encode_with_features`, which has no matching
  pinned Java instance API because `UserInfo` is not a `RemotingSerializable`
  subclass. A compile-fail Rustdoc guard protects the public surface. Protocol
  all-feature tests pass (156 unit tests, all integration targets, 158
  doctests), as do strict Protocol Clippy, formatting, and Maven `verify`
  (37 scenarios, Java fixture checks, and Rust/Java cross-decodes).
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation, current-revision GitHub Ubuntu CI, and native
  Windows stack-capture execution remain open. Phase 19 Broker/Docker
  validation remains separate. No push, remote CI, Broker, Docker,
  publication, or release action was performed; protected Phase 19 worktree
  files remain unstaged.

## P18-V01 inherited writer API and static-topic mapping — 2026-10-22 (`4a0074f6`)

- [x] Pinned Java inheritance review removed the feature-aware instance
  encoder from five plain Java classes and added compile-fail API guards.
  `TopicQueueMappingDetail` and nested/direct-root `LogicQueueMappingItem`
  now use their inherited RemotingSerializable writer-feature schema; Java
  fixtures compare default, populated, and all 48 writer-feature outputs.
- [x] `TopicQueueMappingDetail` equality/hash behavior and public mapping
  helpers now match Java for metadata exclusion, collection aliasing,
  defaults, and null boundaries. Protocol all-feature tests pass (158 unit
  tests, all integration targets, 163 doctests), as do strict Protocol
  Clippy, format/diff checks, and pinned Maven `verify` (37 scenarios,
  runtime route fixture checks, and Java decoding of 106 typed headers and
  19 typed bodies). Runtime route/VIP fixture cases verified: 36.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation, current-revision GitHub Ubuntu CI, and native
  Windows stack-capture execution remain open. Phase 19 Broker/Docker
  validation remains separate. No push, remote CI, Broker, Docker,
  publication, or release action was performed; protected Phase 19 worktree
  files remain unstaged.

## P18-V02 current server-free refresh after static-topic mapping — 2026-10-22 (`4a0074f6`)

- [x] Default and all-feature locked workspace tests, strict workspace
  all-target/all-feature Clippy, formatting, differential self-test and
  37-scenario manifest validation pass. Pinned Java `mvn verify` passes with
  37 scenarios, 36 runtime route/VIP cases, and Java decoding of 106 typed
  headers and 19 typed bodies. Bounded fuzz smoke passes (163 frame seeds,
  11 message seeds, 2,784 mutations, 245 frame decodes and 112 message
  decodes); race/leak smoke passes 16 cases × 3. Release facade examples,
  workspace bench compilation, workspace metadata/license/dependency checks,
  seven package-content checks, and UTC/Asia/Taipei Unique ID tests pass.
- [x] Windows GNU all-target Core check and the Consumer stack-capture test
  cross-compile with `--no-run` pass on this revision.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation remains open. Current-revision GitHub Ubuntu CI and
  native Windows stack-capture execution remain pending. Phase 19 Broker/
  Docker validation remains separate; no push or remote CI was run.

## P18-V01 static-topic public API continuation — 2026-10-22 (`e415be18`)

- [x] Added and Java-checked `LEVEL_0` on `TopicQueueMappingInfo` and
  `TopicQueueMappingDetail`. The `LogicQueueMappingItem` Java method matrix
  now covers properties, setters, offset conversion and decision helpers,
  offset delta, hash, and text for default, populated, and undecided cases.
  Java's `checkIfLogicoffsetDecided` maps to Rust's
  `check_if_logic_offset_decided`.
- [x] Protocol all-feature tests pass (159 unit tests, all integration
  targets, 163 doctests); strict Protocol Clippy and format/diff checks pass.
  Direct Java route generation verifies 36 invoked cases. Pinned Maven
  `verify` passes, including Java decoding 106 typed headers and 19 typed
  bodies. Route fixture SHA-256:
  `e1b5ca4167b237c2011e8ecd62600a54ac0b43e522e4ae164157db2c7ddb5716`.
- [ ] The full workspace gate from the preceding revision is stale for this
  code change and has not yet been rerun. Broad P18-V01 source/API/settings/
  input/error/control-flow and reverse-export reconciliation, current-revision
  GitHub Ubuntu CI, and native Windows stack-capture execution remain open.
  No push or external CI was run; protected Phase 19 files remain unstaged.

## P18-V02 current-revision workspace gate after static-topic API update — 2026-10-22 (`e415be18`)

- [x] Refreshed locked default and all-feature workspace tests, strict
  all-target/all-feature workspace Clippy, formatting, and diff checks after
  the static-topic API update. Protocol all-feature tests and strict Protocol
  Clippy also pass; pinned Maven `verify` passes on the same code revision.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation remains open. Current-revision GitHub Ubuntu CI and
  native Windows stack-capture execution remain unverified. No push or remote
  CI was run; Phase 19 worktree changes remain unstaged.

## P18-V01 direct static mapping equality probe — 2026-10-22 (`70671a33`)

- [x] Added direct Java `LogicQueueMappingItem.equals` outcomes for equal
  defaults/populated values, differences in each stored field, and null versus
  empty `bname`. The focused Protocol test, strict Protocol Clippy,
  formatting/diff checks, and pinned Maven `verify` pass. The route fixture
  SHA-256 is
  `0fcad65ecc00a9c55657113ed494d0d818621f5dcd4fa11e5da87e49c61d9470`.
- [ ] The full workspace gate was last refreshed after the preceding API
  update, before this fixture/test-only extension. Broad P18-V01
  source/API/settings/input/error/control-flow and reverse-export
  reconciliation, current-revision GitHub Ubuntu CI, and native Windows
  stack-capture execution remain open. Phase 19 changes remain unstaged.

## P18-V02 current-tree gate and P18-05 offline benchmark — 2026-10-22 (`70671a33`; docs `e897168c`)

- [x] Current-tree locked default/all-feature workspace tests, strict
  all-target/all-feature Clippy, formatting/diff checks, pinned Maven verify,
  comparator self-test and 37-scenario manifest, fuzz smoke (163 frame seeds,
  11 message seeds, 2,784 mutations), and race/leak smoke (16 cases × 3) pass.
  Maven verifies 37 scenarios, 36 route/VIP cases, and Java decoding of 106
  typed headers and 19 typed bodies.
- [x] Release examples, metadata/license/dependency checks, seven product
  package listings with LICENSE/NOTICE, four Core Unique ID tests under both
  UTC and Asia/Taipei, Windows GNU Core all-target check, and Consumer
  stack-capture `--no-run` cross-compile pass.
- [x] Executed the fixed-input `cargo bench -p rocketmq-classic --bench
  offline --locked` benchmark on Apple M5 Max, macOS AArch64, 18 logical CPUs,
  Rust 1.85.0. One-run observations (ns/op; allocations/op; bytes/op):

  | Workload | Iterations × input | ns/op | allocations/op | bytes/op |
  | --- | ---: | ---: | ---: | ---: |
  | Remoting frame encode/decode | 20,000 × 427 B | 2,254.5 | 40.00 | 4,676.0 |
  | Zstd message decode | 10,000 × 146 B | 1,155.5 | 17.00 | 197,493.0 |
  | Producer send construction | 20,000 × 512 B | 1,182.1 | 54.00 | 3,665.0 |
  | Route lookup, 128 entries | 100,000 × 10 B | 14.1 | 0.00 | 0.0 |
  | Loopback pending request completion | 500 × 128 B | 69,111.1 | 81.00 | 9,546.0 |

  These are single-run observations, not acceptance thresholds or optimization
  claims. The request-completion benchmark uses a local loopback peer and no
  Broker.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and
  reverse-export reconciliation remains open. The server-free CI workflow was
  reviewed but remote Ubuntu CI was not run; the Windows test cross-compiled
  but native Windows execution remains pending. No push or external CI was
  run; protected Phase 19 files remain unstaged.

## P18-V01 `TopicConfigAndQueueMapping` direct API probe — 2026-10-22 (`7397bb16`)

- [x] Matched the pinned wrapper's two public constructors, five declared
  methods, and 27 public methods including inherited `TopicConfig` behavior.
  The direct Java fixture covers defaults, copy and shared-alias behavior,
  mapping handle identity/mutation, null handling, `equals`/`hashCode`, and
  `toString`. Java confirms mapping metadata outside `hostedQueues` is excluded
  from mapping equality.
- [x] Protocol all-feature tests pass (159 unit tests, all integration
  targets, 163 doctests), as do strict Protocol Clippy, formatting/diff checks,
  the focused wrapper differential, and pinned Maven `verify` (37 scenarios,
  36 route/VIP cases, 106 typed headers and 19 typed bodies decoded by Java).
  Producer core fixture SHA-256:
  `39cf5836801a5d5664a61199200dd96b77084cdec557bcc6581eee353967bb39`.
- [x] The full workspace P18-V02 gate was refreshed after the wrapper source
  change below and after the null-field extension. The focused Protocol gate
  and Maven oracle were refreshed again after the inherited-setter extension
  in the latest entry. Broad P18-V01 source/API/settings/input/error/control-flow and
  reverse-export reconciliation, current-revision remote Ubuntu CI, and native
  Windows stack-capture execution remain open. No push or external CI was run;
  protected Phase 19 files remain unstaged.

## P18-V02 current-revision workspace gate after static-topic wrapper — 2026-10-22 (`7397bb16`)

- [x] Locked default and all-feature workspace tests, strict all-target/
  all-feature workspace Clippy, formatting, and `git diff --check` pass.
  Protocol all-feature tests pass (159 unit tests, all integration targets,
  163 doctests); strict Protocol Clippy also passes.
- [x] Pinned Maven `verify` passes on this source revision (37 scenarios,
  36 route/VIP cases, 106 typed headers and 19 typed bodies decoded by Java).
  Differential self-test and the 37-row manifest pass. Fuzz smoke passes
  163 frame seeds, 11 message seeds, 2,784 mutations, 245 frame decodes, and
  112 message decodes; race/leak smoke passes 16 cases × 3.
- [x] Release facade examples and workspace bench compilation pass. Workspace
  metadata/license/dependency checks pass; all seven product package listings
  include `LICENSE` and `NOTICE`. Four Core Unique ID tests pass under UTC and
  Asia/Taipei. Windows GNU Core all-target checking and Consumer stack-capture
  `--no-run` cross-compilation pass.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation remains open. Remote Ubuntu CI and native Windows
  stack-capture execution remain unverified. No push or external CI was run;
  protected Phase 19 files remain unstaged.

## P18-V01 static-topic wrapper reverse-export crosswalk — 2026-10-22 (`7397bb16`)

- [x] Added `TopicConfigAndQueueMapping`'s new constructor, nullable adapter,
  string adapter, and Rust equality/hash implementations to the Rust-to-Java
  Protocol API crosswalk. Its three associated functions map to the Java
  constructors and methods already listed in that Protocol family.
- [ ] This records only the new exports from the static-topic wrapper slice;
  broad P18-V01 input/error/control-flow and whole-surface reverse-export
  reconciliation remains open.

## P18-V01 static-topic wrapper null-field differential and P18-V02 refresh — 2026-10-22 (`c8356edf`)

- [x] Added a direct Java/Rust comparison for a wrapped `TopicConfig` whose
  topic name, filter type, and attributes are explicitly null. Both copied
  fields and the complete Java-compatible wrapper text match. The producer
  core fixture changes only by adding this `nullFields` observation; SHA-256:
  `b64b02f271052b9e402bdebf4dca4ad61f4f40690d936e6f4a4f31de61351d2f`.
- [x] `cargo test --workspace --locked` and
  `cargo test --workspace --all-features --locked` pass. Strict workspace
  all-target/all-feature Clippy, formatting, and `git diff --check` pass.
  Pinned Maven `verify` passes (37 scenarios, 36 route/VIP cases, 106 typed
  headers and 19 typed bodies decoded by Java).
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation remains open. Current-revision remote Ubuntu CI and
  native Windows stack-capture execution remain unverified. No push or
  external CI was run; protected Phase 19 files remain unstaged.

## P18-V01 static-topic wrapper inherited-setter differential — 2026-10-22 (`ab46b125`)

- [x] Invoked the inherited `TopicConfig` setters directly through Java
  `TopicConfigAndQueueMapping`; the Rust wrapper's inherited setters produce
  the same copied fields and complete `toString`, including the retained
  mapping detail. Producer core fixture SHA-256:
  `f46d7f15b0a1425822af97c42b9d639f82bb287f1a4d604e19db2edf150c3186`.
- [x] Protocol all-feature tests pass (159 unit tests, all integration
  targets, 163 doctests); strict all-target/all-feature Protocol Clippy,
  formatting, and diff checks pass. Pinned Maven `verify` passes (37 scenarios,
  36 route/VIP cases, 106 typed headers and 19 typed bodies decoded by Java).
- [ ] This closes only inherited setter dispatch for this wrapper. Broad
  P18-V01 source/API/settings/input/error/control-flow and reverse-export
  reconciliation, current-revision remote Ubuntu CI, and native Windows
  stack-capture execution remain open.

## P18-V01 NamespaceUtil reverse-export correction — 2026-10-22 (`5024360d`)

- [x] Remove four duplicate public module-level constants that were not part
  of the pinned Java `NamespaceUtil` surface. Keep its four Java-shaped
  associated constants and nine declared method signatures. Add compile-fail
  rustdoc guards for the module-root imports.
- [x] Locked default and all-feature workspace tests, strict workspace
  all-target/all-feature Clippy, format/diff checks, and pinned Maven `verify`
  pass. Maven reports 37 scenarios, 36 runtime route/VIP cases, and 106 typed
  headers plus 19 typed bodies decoded by Java.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and
  reverse-export reconciliation, current-revision remote Ubuntu CI, and
  native Windows execution remain open. No push or external CI was run.

## P18-V01 trace record identity equality — 2026-10-22 (`be37e031`)

- [x] Removed structural `PartialEq`/`Eq` from `TraceBean` and `TraceContext`.
  The pinned Java fixture confirms equal-value distinct objects compare
  unequal, the same reference compares equal, and distinct equal-timestamp
  contexts compare as zero. Compile-fail doctests guard the Rust API boundary.
  Trace fixture SHA-256:
  `f88931ceeca238b857ff1367ec846c4f7639da7e1ae43580e22611a12966ac43`.
- [x] Locked default/all-feature workspace tests, Core tests, strict
  all-target/all-feature workspace Clippy, formatting/diff checks, and pinned
  Maven `verify` pass. Maven verifies 37 scenarios, 36 runtime route/VIP
  cases, 106 typed headers and 19 typed bodies.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation remains open. Full P18-V02 fuzz/race/benchmark refresh,
  current-revision remote Ubuntu CI, native Windows execution, and Phase 19
  Broker/Docker validation remain open. No push or external CI was run.

## P18-V01 TopicValidator and SessionCredentials export correction — 2026-10-22 (`87900fb8`)

- [x] Keep the 14 Java TopicValidator constants public on `TopicValidator`
  while making their duplicate module aliases private. Keep the five Java
  SessionCredentials constants on `SessionCredentials`; make their duplicate
  module aliases and the Rust-only `KEY_FILE_PROPERTY` private. Preserve the
  Java `KEY_FILE` mapping through `SessionCredentials::key_file_path`.
- [x] Default and all-feature locked workspace suites, strict all-target/
  all-feature workspace Clippy, formatting/diff checks, pinned Maven `verify`
  (37 scenarios, 36 route/VIP cases, 106 typed headers and 19 typed bodies),
  and all-feature workspace Rustdoc JSON generation pass. Compile-fail
  rustdoc examples guard the removed module paths.
- [ ] Broad P18-V01 source/settings/input/error/control-flow reconciliation,
  current-revision remote Ubuntu CI, and native Windows execution remain open.
  No push or external CI was run.

## P18-V01 ACL hook custom-header signing — 2026-10-22 (`1eb5b6ba`)

- [x] Match Java's `AclClientRPCHook.parseRequestContent` ordering by
  materializing typed command headers before canonicalizing request fields and
  body. The pinned `CreateTopicRequestHeader` fixture compares the resulting
  fields, canonical bytes, and signature. ACL fixture SHA-256:
  `56689d2cf7a9ca9e6c39f5dd62cef73aa268b81890fb1b073510e3ad4ebbf2ba`.
- [x] Locked Core tests, strict all-target/all-feature workspace Clippy,
  formatting/diff checks, and pinned Maven verification pass. Maven verifies
  37 scenarios, 36 runtime route/VIP cases, 106 typed headers and 19 typed
  bodies.
- [ ] This closes only the typed-header signing branch. Broad P18-V01
  source/API/settings/input/error/control-flow and reverse-export reconciliation,
  full current-revision workspace P18-V02 refresh, Ubuntu CI, native Windows,
  and Phase 19 Broker/Docker validation remain open. No push or external CI was
  run.

## P18-V02 current local server-free gate refresh — 2026-10-22 (`1eb5b6ba`)

- [x] Locked default and all-feature workspace tests, strict all-target/
  all-feature workspace Clippy, formatting/diff checks, pinned Maven `verify`
  (37 scenarios, 36 route/VIP cases, 106 typed headers and 19 typed bodies),
  differential self-test and 37-scenario manifest, fuzz smoke (163 frame
  seeds, 11 message seeds, 2,784 mutations), and race/leak smoke (16 cases x
  3) pass.
- [x] Release facade examples build; the offline benchmark runs and strict
  benchmark-target Clippy passes. Workspace metadata/license/dependency
  checks, seven product package listings with `LICENSE`/`NOTICE`, and four
  Core Unique ID tests under both UTC and `Asia/Taipei` pass.
- [x] Windows GNU Core all-target check and Consumer stack-capture test
  cross-compile pass. This is compile evidence only; native Windows execution
  remains open.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation remains open. Current-revision remote Ubuntu CI,
  native Windows execution, and Phase 19 Broker/Docker validation remain open.
  No push or external CI was run; protected Phase 19 worktree files remain
  unstaged.

## P18-V01 metadata wrapper identity equality — 2026-10-22

- [x] Match pinned Java identity behavior for `TopicConfigSerializeWrapper`
  and `SubscriptionGroupWrapper` by removing derived structural equality.
  The direct fixture confirms equal-state encodings, distinct-instance
  inequality, same-reference equality, and `Object` ownership of `equals` and
  `hashCode`. Producer core fixture SHA-256:
  `f0b625d90d319a81070a3cc342f1e655c67d74aa747e0934f12c4b5ed0911095`.
- [x] Protocol tests pass (160 unit tests, all integration targets, and 166
  doctests). The named Java producer-core fixture verification and pinned
  Maven `verify` pass (37 scenarios, 36 route/VIP cases, 106 typed headers
  and 19 typed bodies decoded by Java); formatting and diff checks pass.
- [ ] This closes only the wrapper identity-equality slice. The refreshed
  current-tree P18-V02 gate, broad P18-V01 reconciliation, current-revision
  Ubuntu CI, and native Windows execution remain open. No push or external CI
  was run.

## P18-V02 current local server-free gate refresh — 2026-10-22 (`91f842f4`)

- [x] On code revision `91f842f4`, locked default and all-feature workspace
  tests, strict all-target/all-feature workspace Clippy, formatting/diff
  checks, pinned Maven `verify`, and explicit producer-core fixture
  verification pass. Maven verifies 37 scenarios, 36 runtime route/VIP cases,
  and Java decoding of 106 typed headers and 19 typed bodies.
- [x] Differential self-test and 37-scenario manifest validation pass.
  Fixed-seed fuzz smoke passes with 163 frame seeds, 11 message seeds, 2,784
  mutations, 245 frame decodes, and 112 message decodes; deterministic
  race/leak smoke passes 16 cases x 3.
- [x] Release facade examples build; the offline benchmark runs and strict
  benchmark-target Clippy passes. Workspace metadata, dependency direction,
  and license checks pass; all seven product package listings include
  `LICENSE` and `NOTICE`. Four Core Unique ID tests pass under UTC and
  `Asia/Taipei`.
- [x] Windows GNU Core all-target checking and Consumer stack-capture
  `--no-run` cross-compilation pass. This is compile evidence only.
- [ ] Broad P18-V01 reconciliation, current-revision Ubuntu CI, native Windows
  execution, and Phase 19 Broker/Docker validation remain open. No push or
  external CI was run; protected Phase 19 files remain unstaged. Detailed run
  record: `rlogs/2026/10/22-phase-18-metadata-wrapper-identity-gate__20261022T163414Z.rlog`.


## P18-V01 locale-sensitive topic metrics and current-tree gate — 2026-10-22 (`2887a032`)

- [x] `TopicMessageType::metrics_value` now follows pinned Java 5.5.1
  `String.toLowerCase()` behavior for locale values from POSIX process locale
  selectors. The invoked Java fixture records all eight labels under `tr-TR`;
  the Rust subprocess regression matches the Turkish output. Producer-core
  fixture SHA-256: `3743dd869f394f995422d6849e67741a14d44a643f74b3c9e23a253aaac52919`.
- [x] On code revision `2887a032`, default and all-feature locked workspace
  tests, strict all-target/all-feature Clippy, formatting/diff checks, pinned
  Maven `verify` with producer-core fixture verification, differential
  self-test and 37-scenario manifest, fuzz smoke (163 frame seeds, 11 message
  seeds, 2,784 mutations), and race/leak smoke (16 cases × 3) pass.
- [x] Release examples and offline benchmark pass; strict benchmark Clippy,
  workspace metadata/license/dependency checks, seven package listings,
  UTC/Asia-Taipei Unique ID tests, Windows GNU Core all-target checking, and
  Consumer stack-capture cross-compilation pass. Benchmark observations are
  appended to `PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and
  reverse-export reconciliation, current-revision remote Ubuntu CI, and native
  Windows execution remain open. No push or external CI was run; protected
  Phase 19 worktree files remain unstaged.

## P18-V01 locale-sensitive remoting code descriptions and current local gate — 2026-10-22 (`a4136667`)

- [x] Rust remoting code descriptions now match the pinned Java `RemotingHelper`
  static maps under Turkish/Azeri default locale, including dotless `ı` for
  uppercase `I`. The invoked Java fixture compares all 166 request and 68
  response codes and both unknown-code fallbacks. The fixture restores
  `Locale` in `finally`; a full Maven verify confirmed no unrelated header
  fixture drift. Fixture SHA-256:
  `f6f8ebff1b2cfb33bd7234d8268b5747583f18f98f57fa4017d2af4f204a2573`.
- [x] On `a4136667`, locked default and all-feature workspace tests, strict
  all-target/all-feature Clippy, formatting/diff checks, pinned Maven `verify`
  (37 scenarios, 36 route/VIP cases, 106 typed headers and 19 typed bodies),
  differential self-test/37-scenario manifest, fuzz smoke (163 frame seeds,
  11 message seeds, 2,784 mutations), and race/leak smoke (16 cases × 3) pass.
- [x] Release examples and the offline benchmark pass; strict benchmark
  Clippy, workspace metadata/license/dependency checks, seven product package
  listings, UTC/Asia-Taipei Unique ID tests, Windows GNU Core all-target
  checking, and Consumer stack-capture cross-compilation pass. The benchmark
  observation is recorded in `PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`.
- [ ] Broad P18-V01 reconciliation, current-revision remote Ubuntu CI, and
  native Windows execution remain open. Phase 19 Broker/Docker validation
  remains separate. No push or external CI was run; protected Phase 19 files
  remain unstaged. Run details:
  `rlogs/2026/10/22-phase-18-remoting-locale-gate__20261022T171233Z.rlog`.

## P18-V01 locale-sensitive compression parsing and current local gate — 2026-10-22 (`c9a6f939`)

- [x] `CompressionType.of` now follows pinned Java 5.5.1's default-locale
  `String.toUpperCase()` behavior for Turkish/Azeri locale selectors. The Java
  runtime fixture covers six inputs, including Turkish-specific acceptance of
  `zlıb` and rejection of `zlib`/`zLiB`; the Rust subprocess test compares each
  outcome and error message.
- [x] On `c9a6f939`, default and all-feature locked workspace tests, strict
  all-target/all-feature Clippy, formatting/diff checks, and pinned Maven
  `verify` pass. Maven confirms 37 scenarios, 36 route/VIP cases, and Java
  decoding of 106 typed headers and 19 typed bodies.
- [x] Differential comparator self-test and 37-scenario manifest validation,
  fixed-seed fuzz smoke (163 frame seeds, 11 message seeds, 2,784 mutations),
  and deterministic race/leak smoke (16 cases × 3) pass. Release examples,
  offline benchmark and strict benchmark Clippy, metadata/license/dependency
  checks, and all seven product package listings pass.
- [x] Four Core Unique ID tests pass under each of `TZ=UTC` and
  `TZ=Asia/Taipei`. Windows GNU Core all-target checking and Consumer test
  cross-compilation pass; these are compile checks only.
- [ ] Broad P18-V01 reconciliation, current-revision remote Ubuntu CI, native
  Windows execution, and Phase 19 Broker/Docker validation remain open. The
  differential `--check-all` reports 30 manifest scenarios without live
  runners. No push or external CI was run; protected Phase 19 worktree files
  remain unstaged. Run details:
  `rlogs/2026/10/22-phase-18-compression-locale-gate__20261022T172709Z.rlog`.

## P18-V01 locale-sensitive ACL IPv6 expansion and current local gate — 2026-10-22 (`6155f4cc`)

- [x] `AclUtils.expandIP` now follows pinned Java default-locale uppercase
  behavior under Turkish/Azeri locale selectors. The Java fixture covers
  `i::1`, `ı::1`, and a valid IPv6 input under `tr-TR`; Rust subprocess and
  existing ACL utility fixture tests pass. ACL fixture SHA-256:
  `eaaa69bf97320ab0eb4b957415effe0c6b06e9d55ab0c5ec642e46f8ec3a1e94`.
- [x] On `6155f4cc`, locked default and all-feature workspace tests, strict
  all-target/all-feature Clippy, formatting/diff checks, and pinned Maven
  `verify` pass. Maven verifies 37 scenarios, 36 route/VIP cases, 106 typed
  headers, and 19 typed bodies; direct ACL fixture verification passes.
- [x] Differential comparator self-test and 37-scenario manifest validation,
  fuzz smoke (163 frame seeds, 11 message seeds, 2,784 mutations), and
  deterministic race/leak smoke (16 cases × 3) pass. Release examples,
  offline benchmark and strict benchmark Clippy, metadata/license/dependency
  checks, and all seven product package listings pass.
- [x] Four Core Unique ID tests pass under each of `TZ=UTC` and
  `TZ=Asia/Taipei`. Windows GNU Core all-target checking and Consumer test
  cross-compilation pass; these are compile checks only.
- [ ] Broad P18-V01 reconciliation, current-revision remote Ubuntu CI, native
  Windows execution, and Phase 19 Broker/Docker validation remain open. The
  differential `--check-all` still reports 30 manifest scenarios without
  live runners. No push or external CI was run; protected Phase 19 worktree
  files remain unstaged. Run details:
  `rlogs/2026/10/22-phase-18-acl-locale-gate__20261022T174131Z.rlog`.

## P18-V01 Lithuanian ACL IPv6 expansion locale parity — 2026-10-22 (`505bbe97`)

- [x] `AclUtils.expandIP` now removes U+0307 after lowercase `i` and `j` under
  Lithuanian locale casing, matching the pinned Java output before segment
  padding. The oracle covers both triggering letters and `x`/dotless-`ı`
  controls. ACL fixture SHA-256:
  `da5c9ee1920ed89a997268afcc7eabd77fe53cb91315289179f5d305379ba340`.
- [x] Focused locale tests and pinned Maven verification pass. The complete
  locked default/all-feature workspace tests, strict workspace Clippy,
  formatting/diff checks, differential self-test and 37-scenario manifest,
  fuzz smoke (163 frame seeds, 11 message seeds, 2,784 mutations), and
  deterministic race/leak smoke (16 cases × 3) pass.
- [x] Release examples, offline benchmark and strict benchmark Clippy pass.
  Workspace metadata/dependency/license checks and all seven product package
  listings pass; four Core Unique ID tests pass under UTC and Asia/Taipei.
  Windows GNU Core checking and Consumer stack-capture cross-compilation pass.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation, current-revision remote Ubuntu CI, native Windows
  execution, and Phase 19 Broker/Docker validation remain open. The live-runner
  check still reports 30 scenarios without local runners. No push or external
  CI was run; protected Phase 19 files remain unstaged. Run record:
  `rlogs/2026/10/22-phase-18-acl-lithuanian-locale-gate__20261022T180412Z.rlog`.

## P18-V01 Lithuanian ACL context and segment-width parity — 2026-10-22 (`e6b31e97`)

- [x] `AclUtils.expandIP` now applies the full Lithuanian `After_Soft_Dotted`
  context for U+0307 and preserves blocking combining marks. The oracle covers
  Latin/Cyrillic soft-dotted letters, nonblocking and blocking marks, repeated
  dots, and control letters. Segment padding now uses Java UTF-16 length; a
  supplementary-character case verifies the width boundary. ACL fixture
  SHA-256: `3364a617fc25de94be5ef2bae8342de404b48f9c7eb1548d20fcf13e6dc5412`.
- [x] Turkish and Lithuanian ACL locale subprocess tests, locked default and
  all-feature workspace tests, strict all-target/all-feature Clippy,
  formatting/diff checks, and pinned Maven `verify` pass (37 scenarios, 36
  route/VIP cases, 106 typed headers, and 19 typed bodies).
- [ ] Broad P18-V01 reconciliation, current-revision remote Ubuntu CI, native
  Windows execution, and Phase 19 Broker/Docker validation remain open. The
  live-runner check still reports 30 scenarios without local runners. No push
  or external CI was run; protected Phase 19 files remain unstaged. Run record:
  `rlogs/2026/10/22-phase-18-lithuanian-acl-context-gate__20261022T182318Z.rlog`.

## P18-V02 current local server-free gate refresh — 2026-10-22 (`505bbe97`)

- [x] On the source tree committed as `505bbe97`, locked default and
  all-feature workspace tests, strict all-target/all-feature Clippy,
  formatting/diff checks, pinned Maven `verify` (37 scenarios, 36 route/VIP
  cases, 106 typed headers and 19 typed bodies), differential self-test and
  manifest validation pass.
- [x] Fixed-seed fuzz smoke passes with 163 frame seeds, 11 message seeds,
  2,784 mutations, 245 frame decodes and 112 message decodes. Deterministic
  race/leak smoke passes 16 cases × 3. Release examples and offline benchmark
  pass with strict benchmark Clippy.
- [x] Workspace metadata/license/dependency checks and all seven product
  package listings pass. Four Core Unique ID tests pass under UTC and
  Asia/Taipei. Windows GNU Core all-target checking and Consumer stack-capture
  `--no-run` cross-compilation pass; these do not establish native Windows
  runtime behavior.
- [ ] Broad P18-V01 reconciliation, remote Ubuntu CI, native Windows execution,
  and Phase 19 Broker/Docker validation remain open. The differential live
  runner check still reports 30 scenarios without local runners. No push or
  external CI was run; protected Phase 19 worktree files remain unstaged.
  Detailed run record:
  `rlogs/2026/10/22-phase-18-acl-lithuanian-locale-gate__20261022T180412Z.rlog`.

## P18-V01 TopicConfig attribute iteration parity — 2026-10-22 (`2f4daa9a`)

- [x] Pinned Java `TopicConfig.encode()` and `toString()` retain Java
  `HashMap` bucket insertion order for colliding keys `BB` and `Aa`; Rust's
  prior lexical `BTreeMap` order differed. `TopicConfigAttributeMap` now
  preserves input order and serializes by Java hash bucket. The fixture covers
  forward/reverse insertion, setter and decode paths. SHA-256:
  `1382505e8f49138746501dd77760358ae286e285f762b152f2223b912c8e6167`.
- [x] Focused TopicConfig API tests, locked default/all-feature workspace
  tests, strict all-target/all-feature Clippy, pinned Maven `verify` (37
  scenarios, 36 route/VIP cases, 106 typed headers, 19 typed bodies), fmt/diff,
  metadata/license checks, and all seven product package listings pass.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation, current-revision remote Ubuntu CI, native Windows
  execution, and Phase 19 Broker/Docker validation remain open. No push or
  external CI was run; the four protected Phase 19 worktree paths remain
  unstaged. Detailed run record:
  `rlogs/2026/10/22-phase-18-topic-config-hash-order-gate__20261022T183948Z.rlog`.

## P18-V01 ConsumerStatsManager sampling cadence — 2026-10-22 (`782a9cdb`)

- [x] Rust consumer statistics now follow the pinned Java manager's sampled
  snapshots: pull/consume metrics remain zero before the first 10-second
  sample, TPS/RT update at sample ticks, and failed-message totals wait for
  the first 10-minute hour-window sample. The invoked Java fixture controls
  the five 10-second samplers and records `0.0`, `2.0`, and `2.0` around the
  first sample and a pending increment. Fixture SHA-256:
  `57b6f19d68fb3460e7fa86bd6b08bf16f43a03d28cce2558716f88c110ccb369`.
- [x] Core sampling tests, Consumer stats flow tests, default and all-feature
  locked workspace suites, strict all-target/all-feature Clippy, pinned Maven
  `verify` (37 scenarios, 36 route/VIP cases, 106 typed headers and 19 typed
  bodies), workspace metadata/license checks, all seven product package
  listings, formatting and diff checks pass on `782a9cdb`.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation, current-revision remote Ubuntu CI, native Windows
  runtime execution, and Phase 19 Broker/Docker validation remain open. The
  differential live-runner check still reports 30 cases without local
  runners. No push or external CI was run; protected Phase 19 files remain
  unstaged. Detailed run record:
  `rlogs/2026/10/22-phase-18-consumer-stats-sampling-gate__20261022T190117Z.rlog`.

## P18-V04 current-tree Windows GNU cross-compilation refresh — 2026-10-22 (`5965b62f`)

- [x] On `5965b62f402a2e56ddf82870a771832569d390ff`,
  `cargo check -p rocketmq-classic-core --all-targets --target
  x86_64-pc-windows-gnu --locked` passes, and the Consumer
  `stack::tests::jstack_includes_current_and_other_named_threads` test binary
  cross-compiles with `--no-run` for the same target.
- [ ] These are compile checks only. Native Windows execution, current-revision
  remote Ubuntu CI, broad P18-V01 reconciliation, and Phase 19 Broker/Docker
  validation remain open. No external CI or push was run. The protected Phase
  19 worktree files remain unstaged. Run record:
  `rlogs/2026/10/22-phase-18-windows-gnu-cross-compile-refresh__20261022T190406Z.rlog`.

## P18-V01 current Rustdoc export inventory — 2026-10-22 (`1dc3a7e2`)

- [x] All-feature Rustdoc JSON generation passes on the current tree. The
  refreshed distinct public-name counts, excluding each generated crate-root
  marker, are Common 439, Consumer 445, Core 682, Producer 362, Protocol
  1,524, and Remoting 337. The Producer count supersedes the earlier 357-name
  record; this is an inventory correction, not a behavior-coverage claim.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and reverse-
  export reconciliation, current-revision remote Ubuntu CI, native Windows
  runtime, and Phase 19 Broker/Docker validation remain open. Rustdoc emitted
  the existing workspace-manifest and Consumer `fetch_update` warnings. No
  external CI or push was run. Run record:
  `rlogs/2026/10/22-phase-18-rustdoc-export-inventory__20261022T190913Z.rlog`.

## P18-V01 `MessageDecoder.decodeProperties(ByteBuffer)` record-input parity — 2026-10-22 (`67d0d049`)

- [x] Rust now matches Java's direct property extraction when declared store
  size is zero and returns an empty map for a positive-length malformed
  property string. Invoked fixture SHA-256:
  `e2fab00b22e5954f54095fe7215848e4e317d6f62461cea166b5cd8be715b317`.
- [x] Focused differential and Common tests, locked default/all-feature
  workspace tests, strict all-target/all-feature Clippy, fmt/diff checks, and
  pinned Maven `verify` pass (37 scenarios, 36 route/VIP cases, 106 typed
  headers, 19 typed bodies).
- [ ] Broad P18-V01 reconciliation, current-revision remote Ubuntu CI, native
  Windows runtime, and Phase 19 Broker/Docker validation remain open. No push
  or external CI was run; the four protected Phase 19 worktree paths remain
  unstaged. Run record:
  `rlogs/2026/10/22-phase-18-message-decoder-properties-gate__20261022T192507Z.rlog`.

## P18-V01 `MessageDecoder.decodeProperties(ByteBuffer)` cursor parity — 2026-10-22 (`2e1122cf`)

- [x] Match Java buffer advancement for positive, zero, and negative property
  lengths, and match the cursor position and typed truncation outcome for a
  short positive-length block. Invoked fixture SHA-256:
  `699dfaae816b753105119193e3b268fe16fb2b83ad0199473a5665a7d6b76436`.
- [x] Common tests, locked default/all-feature workspace tests, strict
  all-target/all-feature Clippy, formatting/diff checks, pinned Maven `verify`,
  differential manifest/self-test, fuzz and race/leak smoke, release examples,
  offline benchmark, metadata/package checks, timezone checks and Windows GNU
  cross-compiles pass on this source revision.
- [ ] Broad P18-V01 reconciliation, current-revision remote Ubuntu CI, native
  Windows runtime, and Phase 19 Broker/Docker validation remain open. Windows
  results are cross-compilation only. No push or external CI was run; the four
  protected Phase 19 worktree paths remain unstaged. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261022T194830Z.rlog`.

## P18-V01 full message decode declared-size parity — 2026-10-22 (`8acbf137`)

- [x] Rust retains Java's declared `storeSize` field but parses the record by
  body/topic/property lengths and returns the corresponding consumed byte
  count. Direct Java cases verify declared sizes `0`, `-1`, and
  `Integer.MAX_VALUE`; a Rust concatenated-record test verifies the resulting
  boundary behavior. Fixture SHA-256:
  `41ed0208cdf79da2491554ed08683e4f18b40d6bce1d21d246771dd58f0e15ae`.
- [x] Default and all-feature workspace tests, final Common all-feature tests,
  strict all-target/all-feature Clippy, pinned Maven `verify`, differential
  checks, fuzz smoke (245 frame and 188 message decodes), race/leak smoke
  (16 cases × 3), release examples, benchmark, workspace metadata, seven
  package listings, timezone tests and Windows GNU cross-compiles pass.
- [ ] Broad P18-V01 reconciliation, remote Ubuntu CI, native Windows runtime,
  and Phase 19 Broker/Docker validation remain open. Windows checks are
  compile-only. No push or external CI was run; the four protected Phase 19
  worktree paths remain unstaged. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261022T200111Z.rlog`.

## P18-V01 full message decode nonpositive field lengths — 2026-10-22 (`8ca83fc8`)

- [x] Full decode accepts nonpositive body and property lengths as absent
  fields and returns Java's position after the corresponding length field.
  Invoked cases compare complete decoded snapshots. Fixture SHA-256:
  `9cd0b64fe95643177c533d6cc2fd1f33c957e54cd3bd9559ecb07f6083939938`.
- [x] Common differential, default/all-feature workspace suites, strict
  Clippy, Maven `verify`, differential checks, fuzz (245 frame and 189 message
  decodes), race/leak (16 cases × 3), release examples and benchmark, metadata
  and seven package listings, UTC/Asia-Taipei checks, and Windows GNU
  cross-compiles pass.
- [ ] Broad P18-V01 reconciliation, remote Ubuntu CI, native Windows runtime,
  and Phase 19 Broker/Docker validation remain open. Windows checks are
  compile-only. No push or external CI was run; the four protected Phase 19
  worktree paths remain unstaged. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261022T201155Z.rlog`.

## P18 current-tree server-free verification — 2026-10-23 (`05e51237`)

- [x] Default and all-feature locked workspace tests, strict workspace
  all-target/all-feature Clippy, formatting, and `git diff --check` passed.
  The first default workspace attempt encountered a test child stuck before
  Rust test startup in macOS `dyld`; the isolated codec suite passed, and the
  complete workspace rerun passed.
- [x] Offline Maven `verify` passed with the pinned Java 5.5.1 source, including
  37 Phase 19 scenarios, 73 ClientConfig identity/default/clone checks, runtime
  fixtures, and Java decoding of Rust-generated typed headers and bodies.
  Comparator self-test and 37-scenario manifest validation passed.
- [x] Fixed-seed fuzz smoke passed with 163 frame seeds, 11 message seeds,
  2,784 bounded mutations, 245 frame decodes, and 192 message decodes.
  Deterministic race/leak smoke passed 17 cases × 3 repetitions. The pinned
  Asia/Taipei Unique ID regression passed.
- [x] Release facade examples, workspace metadata/license/dependency checks,
  and the release offline benchmark passed. Current benchmark observations are
  appended to `compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`.
- [ ] Broad P18-V01 Java source/API/settings/input/error/control-flow and Rust
  reverse-export reconciliation remains open. Current-revision remote Ubuntu
  CI and native Windows runtime execution remain pending. Phase 19 Broker/Docker
  evidence remains in its own phase. No remote CI, Broker, Docker, publication,
  or release action was performed; the four protected Phase 19 worktree paths
  remain untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V04 current-tree Windows GNU cross-compile — 2026-10-23 (`05e51237`)

- [x] `cargo check --offline -p rocketmq-classic-core --all-targets --target
  x86_64-pc-windows-gnu --locked` and the exact Consumer stack-capture test
  binary build with `--no-run` passed on the current code revision.
- [ ] These are compile checks only. Native Windows test execution and
  current-revision remote Ubuntu CI remain pending. No external CI or push was
  run. Follow-up record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V01 client scheduled-executor caller tasks — 2026-10-23 (`5c808754`)

- [x] Added `ClientRuntime::scheduled_executor_service()` and the public
  `RuntimeScheduler` handle for immediate/delayed async tasks, fixed-rate and
  fixed-delay callbacks, task cancellation, and runtime-owned shutdown. The
  worker is the same serial scheduler that runs the client's internal periodic
  callbacks and can run caller work before `ClientRuntime::start()`.
- [x] The core library suite passed (218 tests), strict all-target/all-feature
  Core Clippy passed, and formatting plus `git diff --check` passed on the code
  change. The new paused-time regression covers pre-start execution, delays,
  repeating modes, cancellation, and shutdown.
- [ ] Pinned Java also exposes executor-wide lifecycle and result-bearing
  `Future` methods; those capabilities remain unreconciled. The separate
  `PullMessageService.getScheduledExecutorService()` handle is not exposed yet.
  Broad P18-V01 source/API/settings/input/error/control-flow and reverse-export
  reconciliation, current-revision remote Ubuntu CI, native Windows runtime,
  and Phase 19 Broker/Docker validation remain open. No remote CI, push, Broker,
  or Docker action was performed. The protected Phase 19 worktree files remain
  untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V01 PullMessageService scheduler getter — 2026-10-23 (`2c52ac4d`)

- [x] Added `ClientRuntime::pull_scheduled_executor_service()` as a distinct
  handle over the PullMessageService scheduled queue. It supports the same
  Rust-native immediate/delayed, fixed-rate/fixed-delay, and per-task
  cancellation operations, while retaining the existing one-second graceful
  shutdown behavior. The pre-start regression also checks cancellation removes
  the delayed task from that queue.
- [x] The Core library suite passed (219 tests), strict all-target/all-feature
  Core Clippy passed, formatting and `git diff --check` passed on `2c52ac4d`.
- [ ] Java's executor-wide controls and result-bearing `Future` operations,
  inherited thread controls, broad P18-V01 source/API/settings/input/error/
  control-flow and reverse-export reconciliation, current-revision remote
  Ubuntu CI, native Windows runtime, and Phase 19 Broker/Docker validation
  remain open. No remote CI, push, Broker, or Docker action was performed. The
  protected Phase 19 worktree files remain untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V01 result-bearing scheduled calls — 2026-10-23 (`235d05c9`)

- [x] Both scheduler handles now expose `submit` and delayed result-bearing
  calls. `RuntimeScheduledCall::join()` returns the callback value and reports
  cancellation, panic, or runtime shutdown through a closed oneshot result.
  Paused-time tests cover returned values and cancellation on both scheduler
  owners.
- [x] The Core library suite passed (220 tests), strict all-target/all-feature
  Core Clippy passed, and formatting plus `git diff --check` passed on the code
  change.
- [ ] Executor-wide shutdown/status/termination APIs and final P18-V01 source,
  settings, input, error, control-flow, and reverse-export reconciliation are
  still open. Remote Ubuntu CI, native Windows execution, and Phase 19
  Broker/Docker validation remain pending. No external CI, push, Broker, or
  Docker action was performed; protected Phase 19 worktree files remain
  untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V01 scheduled-executor lifecycle — 2026-10-23 (`4c337b1e`)

- [x] Both public scheduler handles now expose graceful and immediate
  shutdown, shutdown/termination status, and an async termination wait.
  Graceful shutdown drains accepted one-shot work and stops recurring work;
  immediate shutdown reports the pending queue size and signals cancellation
  without waiting for the scheduler worker to join itself. Pull scheduler
  shutdown leaves immediate FIFO pull dispatch available.
- [x] The Core library suite passed (227 tests), strict all-target/all-feature
  Core Clippy passed, and formatting plus `git diff --check` passed on
  `4c337b1e`. Regressions cover both scheduler owners, timeout/status
  transitions, runtime shutdown, and shutdown requested from the scheduler
  worker itself.
- [ ] This closes the scheduler-lifecycle slice only. Broad P18-V01 Java
  source/API/settings/input/error/control-flow and Rust reverse-export
  reconciliation, current-revision remote Ubuntu CI, native Windows runtime,
  and Phase 19 Broker/Docker validation remain open. No remote CI, push, Broker,
  or Docker action was performed; protected Phase 19 worktree files remain
  untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V01 inherited scheduler bulk calls — 2026-10-23 (`6a960144`, refined in `e5bbe8df`)

- [x] Both `RuntimeScheduler` owners now provide `invoke_all` and
  `invoke_any`, mapping the inherited Java `ExecutorService` bulk operations.
  `invoke_all` yields per-task results/errors in submission order. `invoke_any`
  continues after a panicking task, returns the first callback value, and
  suppresses queued callbacks after that value is produced. Empty task sets
  and sets where no callback returns a value produce explicit `RuntimeError`
  variants. An `Err` value inside `Result` is still a normally returned task
  value. Dropping a bulk-call future cancels accepted tasks; callers can use
  Tokio timeout for a chosen deadline.
- [x] All 230 Core library tests and all 23 scheduler tests passed, including
  pull-owner bulk calls and cancellation of active and queued work. Strict
  all-target/all-feature Core Clippy, formatting, and `git diff --check`
  passed on the committed code.
- [ ] This closes the inherited executor bulk-call slice only. Broad P18-V01
  bidirectional source/API/settings/input/error/control-flow reconciliation,
  current-revision remote Ubuntu CI, native Windows runtime, and Phase 19
  Broker/Docker validation remain open. No external CI, push, Broker, or Docker
  action was performed; protected Phase 19 worktree files remain untouched.
  Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V01 timed scheduler and `ScheduledFuture` surface — 2026-10-23 (`c0cb787b`)

- [x] Both scheduler owners now expose timed `invoke_all_with_timeout` and
  `invoke_any_with_timeout`. Timed invoke-all retains one outcome per input:
  completed values, failed/cancelled callbacks, and tasks cancelled at the
  deadline. Timed invoke-any returns `RuntimeError::InvocationTimeout` and
  cancels accepted tasks if no callback returns before its deadline.
- [x] Scheduled task handles expose cancellation/completion status,
  `remaining_delay`, async `wait`, and `wait_with_timeout`. Result-bearing
  calls expose `is_cancelled` and repeatable `join_with_timeout`; a wait timeout
  leaves the task and result receiver available. Callback failure is distinct
  from cancellation in the task-handle wait result.
- [x] All 233 Core library tests and all 26 scheduler tests passed. Strict
  all-target/all-feature Core Clippy, formatting, and `git diff --check` also
  passed on `c0cb787b`.
- [x] Follow-up `04eec879` exercises both timed bulk methods through the
  separate PullMessageService queue; the pull-owner regression, all 26
  scheduler tests, and strict all-target/all-feature Core Clippy passed.
- [ ] The scheduler inheritance slice is locally mapped. Other P18-V01
  bidirectional source/API/settings/input/error/control-flow reconciliation,
  inherited `ServiceThread` controls, current-revision remote Ubuntu CI, native
  Windows runtime, and Phase 19 Broker/Docker validation remain open. No
  external CI, push, Broker, or Docker action was performed; protected Phase 19
  worktree files remain untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V02 current-tree server-free refresh — 2026-10-23 (`4c337b1e`)

- [x] Default and all-feature locked workspace tests passed; strict
  all-target/all-feature workspace Clippy passed. Pinned offline Maven verify
  passed with all 37 scenarios, comparator self-test and manifest validation
  passed, the 2,784-mutation fuzz smoke and 17-case × 3 repetition
  race/leak smoke passed, and workspace metadata checks passed.
- [x] Release example build, offline benchmark, formatting and diff checks
  passed. The benchmark recorded 34.0 ns per 128-entry route lookup and
  47,649.6 ns per loopback pending-request completion on the listed M5 Max /
  Rust 1.85.0 environment; inputs, iterations, and allocation observations
  are recorded in [the benchmark
  baseline](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
- [ ] P18-V01 full bidirectional source/API/settings/control-flow
  reconciliation remains open. Current-revision remote Ubuntu CI, native
  Windows runtime, and Phase 19 Broker/Docker validation remain pending. No
  remote CI, push, Broker, or Docker action was performed; protected Phase 19
  worktree files remain untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V02 scheduler-invocation revision refresh — 2026-10-23 (`04eec879`)

- [x] Default and all-feature locked workspace tests, strict all-target and
  all-feature workspace Clippy, formatter and diff checks passed. Offline
  Maven verify passed its pinned Java reference checks and all 37 scenarios;
  the differential comparator self-test and 37-scenario manifest validation
  passed.
- [x] Workspace metadata check passed. Fuzz smoke passed 163 frame seeds,
  11 message seeds, 2,784 bounded mutations, 245 frame decodes, and 192 message
  decodes. The deterministic race/leak smoke passed all 17 cases for three
  repetitions. Release examples built successfully.
- [x] The offline benchmark completed on macOS 26.6.1 AArch64 / Apple M5 Max /
  Rust 1.85.0. This sample recorded 31.7 ns for the 128-entry publish-address
  lookup and 51,556.0 ns for loopback pending-request completion; all operation
  and allocation observations are in the [benchmark baseline](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
- [ ] P18-V01 bidirectional source/API/settings/input/error/control-flow and
  reverse-export reconciliation remains open, including inherited
  `ServiceThread` controls. Current-revision remote Ubuntu CI, native Windows
  runtime, and Phase 19 Broker/Docker validation remain pending. No remote CI,
  push, Broker, or Docker action was performed; protected Phase 19 worktree
  files remain untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V01 inherited ServiceThread API slice — 2026-10-23 (`bd4e073c`)

- [x] Rust service handles expose inherited RebalanceService and
  PullMessageService controls: async start/shutdown, join interval, stopped
  state, stop/wakeup, and daemon getter/setter. Pull shutdown also closes its
  separate scheduled executor, including when called before worker start.
- [x] Current-tree default/all-feature workspace tests, strict workspace
  Clippy, format/diff checks, pinned Maven verification, differential
  self-test/37-scenario manifest validation, metadata, fuzz smoke, 17-case × 3
  race/leak smoke, release examples, and offline benchmark passed. The current
  benchmark sample is in the [baseline](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
- [ ] This closes only the inherited thread-control slice. Broad P18-V01
  bidirectional source/API/settings/input/error/control-flow reconciliation
  remains open. Current-revision remote Ubuntu CI, native Windows runtime, and
  Phase 19 Broker/Docker validation remain pending. No remote CI, push, Broker,
  or Docker action was performed. Protected Phase 19 worktree files remain
  untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V04 Windows GNU and public Core rustdoc refresh — 2026-10-23 (`bd4e073c`)

- [x] Current-tree
  `CC_x86_64_pc_windows_gnu=/opt/homebrew/bin/x86_64-w64-mingw32-gcc cargo
  check --offline --workspace --all-targets --all-features --locked --target
  x86_64-pc-windows-gnu` passes on `bd4e073c`, including test and example
  targets. `RUSTDOCFLAGS='-D warnings' cargo doc --offline
  -p rocketmq-classic-core --all-features --locked --no-deps` also passes.
- [ ] These provide GNU cross-compilation and targeted public-doc evidence;
  native Windows runtime and current-revision remote Ubuntu CI remain pending.
  No external CI or push was run. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V01 strong service-owner retention — 2026-10-23 (`1f6cd086`)

- [x] `RuntimeServiceThread` now retains `Arc<ClientRuntime>`, matching the
  strong `MQClientInstance` field in pinned Java service instances. A regression
  verifies that the PullMessageService handle remains usable after its manager
  and runtime variables leave scope.
- [x] Default/all-feature workspace tests, strict workspace Clippy, pinned
  Maven verification, differential self-test/37-row manifest, workspace
  metadata, fuzz, 17-case × 3 race/leak smoke, release examples, benchmark,
  Windows GNU cross-check, targeted Core rustdoc, formatting, and diff checks
  passed on this code revision.
- [ ] This closes the service-handle lifetime slice only. Broad P18-V01
  bidirectional reconciliation remains open, as do remote Ubuntu CI and native
  Windows runtime execution. Phase 19 Broker/Docker evidence is not claimed.
  No external CI or push was run. Protected Phase 19 worktree files remain
  untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V04 Windows stack alignment and CI artifact budget — 2026-10-23 (`f375e3f0`, `7a4f132e`)

- [x] Remote run `39730752050` exposed that the named Windows worker thread
  was enumerated but its native stack was unavailable. Both supported Windows
  stack-capture paths now pass a 16-byte-aligned `CONTEXT` to
  `GetThreadContext`. The focused macOS stack regression passes; the complete
  Windows GNU workspace cross-check passes for all targets and features. A
  focused ARM64/MSVC compile of the context and unwind calls passes. A full
  ARM64/MSVC workspace check cannot run on this host because its Windows C
  toolchain and headers are absent.
- [x] The Ubuntu CI job disables debug information for dev/test profiles and
  incremental compilation. With the reduced test profile, all seven Remoting
  security integration tests pass. The Windows GNU and security commands are
  local checks; the updated GitHub workflow has not been rerun.
- [x] Linux ARM64 server-free refresh on `73721ae5` passed formatting,
  all-target/all-feature Clippy, default and all-feature workspace tests,
  differential self-test and manifest validation, fuzz and race/leak smokes,
  release example build, benchmark compilation, and metadata checks. The
  Windows-only source fix then passed its focused Linux regression and Windows
  GNU cross-check.
- [ ] Current-revision GitHub Ubuntu/Windows results and P18-V04 remain open.
  Broad P18-V01 bidirectional Java/Rust source/API/settings/input/error/flow
  and reverse-export reconciliation also remains open. Phase 19 Docker/Broker
  work and protected user changes are out of scope and were left untouched.
  No external CI run or push was performed. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V02 pinned Java oracle refresh — 2026-10-23 (`bc58a528`)

- [x] `mvn -o -f compat/java-reference/pom.xml verify` passed on the current
  tree. The Java self-test and 37-scenario manifest passed; runtime fixtures
  verified route/VIP, config, address, consumer operations, offsets, hooks,
  lookup, admin, protocol identities, and Java-to-Rust/Rust-to-Java typed
  serialization. Java decoded all 106 typed headers and 19 typed bodies.
- [ ] This refresh does not close broad P18-V01 reconciliation or establish
  current-revision GitHub Ubuntu/native Windows evidence for P18-V04. No
  external CI or push was performed. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V02 Linux ARM64 low-artifact refresh — 2026-10-23 (`0095d478`)

- [x] On Linux ARM64, the current tree passed formatting, strict workspace
  Clippy, default/all-features workspace tests, the Asia/Taipei unique-ID
  fixture, differential self-test and 37-scenario manifest, fuzz smoke,
  17 deterministic race/leak cases × 3, release examples, benchmark compile,
  and workspace metadata. The CI profile settings
  (`CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`,
  `CARGO_INCREMENTAL=0`) were active for every Cargo command.
- [ ] This verifies the Rust server-free pipeline locally under the reduced
  profile; it is not a current GitHub Actions result. Broad P18-V01 and
  current-revision GitHub Ubuntu/native Windows P18-V04 evidence remain open.
  No push or remote run was performed. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V01 Remoting command version parity — 2026-10-23 (`838286ca`)

- [x] Java and Rust request/response factories now apply the pinned 5.5.1
  client API version (515) through the Java static-initialization behavior.
  The Java oracle asserts all dispatched API request versions; Rust tests
  cover wire version and static configuration ordering.
- [x] Formatting, strict all-target/all-feature Clippy, default and
  all-feature workspace tests, timezone fixture, differential validation,
  fuzz and race/leak smokes, Maven verify, release examples, benchmark compile,
  and workspace metadata passed.
- [ ] This closes the command-version parity slice only. Broad P18-V01,
  current-revision remote Ubuntu/native Windows evidence, and Phase 19
  Broker/Docker validation remain open. No push or remote CI was run. Phase 19
  user-owned worktree files remain untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V04 current-revision Windows GNU compile — 2026-10-23 (`838286ca`)

- [x] `cargo check --offline --workspace --all-targets --all-features
  --target x86_64-pc-windows-gnu --locked` passed on the command-version
  implementation revision.
- [ ] This is a cross-compile only. Native Windows execution and
  current-revision remote Ubuntu CI remain open, as does broad P18-V01. No
  push or remote CI was run. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V01 Java client process-property census — 2026-10-23 (`ed909c11`)

- [x] Pinned Java `System` property/environment reads and Rust production
  environment-backed settings were cross-checked. Each active client setting
  maps to its existing Rust owner and source evidence.
- [x] The initial scope review recorded `MQClientAPIFactory` as an orphaned
  public helper with no pinned client construction/callsite. That exclusion is
  superseded by the direct Rust API mapping recorded below. Broad P18-V01
  source/API/settings/input/error/control-flow and reverse-export audit remains
  partial. No remote CI or push was run. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V01 LocalFileOffsetStore public static field — 2026-10-23 (`6d0f4523`)

- [x] Added `LocalFileOffsetStore::local_offset_store_dir()` and made `new()`
  use the same first-access snapshot as Java's public static field.
- [x] Invoked Java fixture verifies class-initialization snapshot behavior;
  isolated Rust subprocess cases cover property override, property mutation,
  default home, blank property, blank home, and absent home.
- [x] Focused Rust test, full Consumer all-features package tests, strict
  Consumer Clippy, focused Java runtime-offset fixture verify, and full offline
  Maven `verify` passed.
- [ ] Broad P18-V01 and final reverse API review remain open; the direct
  `MQClientAPIFactory` API slice is now mapped in the Phase 6 inventory.
  Native Windows runtime, current-revision remote Ubuntu CI, and Phase 19
  Broker/Docker evidence are pending. No push or remote CI was run. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V01 MQClientAPIFactory public API mapping — 2026-10-23 (`ac3a8ed5`)

- [x] Added Core `MQClientApiFactory` and `NameserverAccessConfig` with
  Java-shaped pool selection, shared mutable client-array behavior, NameServer
  updates, injected remoting-client creation, domain refresh, and lifecycle.
- [x] Added an invoked Java 5.5.1 fixture for constructor/method inventory,
  initial property values, client names/addresses, selection, array aliasing,
  address updates, and shutdown. Rust tests compare the resulting fixture.
- [x] Full workspace all-feature tests, strict all-target/all-feature
  workspace Clippy, formatting, diff checks, and full offline Maven `verify`
  passed (exit 0).
- [ ] Broad P18-V01 and final reverse API review remain open. Protected Java
  subclass hooks, native Windows runtime, current-revision remote Ubuntu CI,
  and Phase 19 Broker/Docker evidence remain pending. No push or remote CI was
  run. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V01 MQClientAPIFactory nullable callback and discovery parity — 2026-10-23 (`805619a9`)

- [x] `onNameServerAddressChange(null)` now preserves the current address list
  after startup; before startup, the Java fixture records its null-client
  failure. Discovery ignores Java-blank address results before publishing
  them to the client pool.
- [x] The expanded invoked Java fixture and Core differential cover both
  boundaries. Core library all-feature tests (237), strict Core Clippy,
  formatting, diff checks, focused Java fixture verification, and full offline
  Maven `verify` passed (exit 0).
- [ ] Broad P18-V01 and final reverse API review remain open. Native Windows
  runtime, current-revision remote Ubuntu CI, and Phase 19 Broker/Docker
  evidence remain pending. No push or remote CI was run. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T034516Z.rlog`.

## P18-V01 current-tree Rustdoc public-name refresh — 2026-10-23 (`668dfb1e`)

- [x] All-feature Rustdoc JSON generation and distinct public-name counting
  pass on the current tree. Counts (crate-root marker excluded): Common 439,
  Consumer 460, Core 740, Producer 362, Protocol 1,524, and Remoting 337.
  This supersedes the 2026-10-22 Consumer/Core counts and includes the new
  `MQClientApiFactory` export.
- [ ] Export counts are inventory only. Broad P18-V01 source/API/settings/
  input/error/control-flow and reverse-export reconciliation, current-revision
  remote Ubuntu CI, native Windows runtime, and Phase 19 Broker/Docker evidence
  remain open. Rustdoc emitted the existing workspace-manifest and Consumer
  `fetch_update` warnings. No external CI or push was run. Run record:
  `rlogs/2026/10/23-phase-18-rustdoc-export-refresh__20261023T091621Z.rlog`.

## P18-V02 current-tree server-free refresh after factory parity — 2026-10-23 (`e8066804`)

- [x] Default and all-feature locked workspace tests, strict all-target/
  all-feature workspace Clippy, formatter, and diff checks passed. Offline
  Maven `verify` passed, including 37 scenarios and the expanded invoked
  `MQClientAPIFactory` fixture; differential self-test and 37-scenario manifest
  validation passed.
- [x] Workspace metadata and all seven product package listings passed with
  `LICENSE`/`NOTICE`; bounded fuzz smoke passed with 163 frame seeds, 11
  message seeds, 2,784 mutations, 245 frame decodes, and 192 message decodes;
  deterministic race/leak smoke passed 17 cases × 3 repetitions.
- [x] Release examples and offline benchmark passed. The current benchmark
  sample is recorded in
  `compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`. Current all-target,
  all-feature Windows GNU cross-compilation passed.
- [ ] Broad P18-V01 bidirectional source/API/settings/input/error/control-flow
  and reverse-export reconciliation remains open. Native Windows runtime,
  current-revision remote Ubuntu CI, and Phase 19 Broker/Docker evidence remain
  pending. No external CI, push, Broker, or Docker action was performed; the
  existing Phase 19 worktree files remain untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-current-tree-server-free-gate__20261023T092907Z.rlog`.

## P18-V01 MQClientAPIFactory per-client domain discovery — 2026-10-23 (`df87b7b2`)

- [x] Domain startup lookups and periodic refresh results now update only their
  corresponding API client slot, matching Java's per-instance NameServer state.
- [x] The invoked Java fixture serves distinct responses for two clients and
  captures both resulting address lists; the Rust Core parity test consumes
  that fixture.
- [x] Core all-feature library tests (238), strict all-target/all-feature Core
  Clippy, formatting, diff checks, and full offline Maven `verify` passed
  (37 scenarios, including the expanded factory fixture).
- [ ] Broad P18-V01 and final reverse API review remain open. Native Windows
  runtime, current-revision remote Ubuntu CI, and Phase 19 Broker/Docker
  evidence remain pending. No push or remote CI was run. Existing Phase 19
  worktree files remain untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-factory-per-client-discovery__20261023T093842Z.rlog`.

## P18-V01 MQClientAPIFactory per-client refresh schedules — 2026-10-23 (`681a8acf`)

- [x] Rust starts one cancellable refresh worker per pool slot with the
  Java-matched 10-second initial delay and 120-second fixed rate. Shutdown
  signals and joins all factory-owned refresh tasks.
- [x] The invoked Java fixture records two schedule registrations for two
  domain clients; the paused-time Rust test checks task count and distinct
  per-client refreshed addresses.
- [x] All-feature workspace tests passed (239 Core unit tests); strict
  all-target/all-feature workspace Clippy, format/diff checks, and full offline
  Maven `verify` passed (37 scenarios, including the schedule-count fixture).
- [ ] Broad P18-V01 and final reverse API review remain open. Native Windows
  runtime, current-revision remote Ubuntu CI, and Phase 19 Broker/Docker
  evidence remain pending. No push or remote CI was run. Existing Phase 19
  worktree files remain untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-factory-refresh-schedules__20261023T095628Z.rlog`.

## P18-V01 MQClientAPIFactory provider callbacks per client — 2026-10-23 (`88a9578e`)

- [x] Domain provider callbacks register once per active API client before its
  initial lookup and update only that client slot. Explicit factory address
  changes continue to apply to all clients; zero-client starts register none.
- [x] A Core regression checks per-slot callback registration, update/return
  behavior, and zero-client behavior.
- [x] Core all-feature library tests (240), strict all-target/all-feature Core
  Clippy, formatting, and diff checks passed.
- [ ] Broad P18-V01 and final reverse API review remain open. Native Windows
  runtime, current-revision remote Ubuntu CI, and Phase 19 Broker/Docker
  evidence remain pending. No push or remote CI was run. Existing Phase 19
  worktree files remain untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-factory-per-client-callbacks__20261023T100045Z.rlog`.

## P18-V01 MQClientAPIFactory null client-array slots — 2026-10-23 (`1b1d4e2a`)

- [x] Explicit NameServer changes now follow Java's mutable-array iteration:
  earlier slots retain updates, then the first null slot raises the matching
  Rust `NullClient(index)` error. Null-address calls follow the same slot
  order.
- [x] The invoked Java fixture and Core regression compare both errors and
  partial address-array states.
- [x] Default and all-feature workspace tests, strict workspace Clippy,
  formatting/diff, pinned offline Maven `verify` (37 scenarios), differential
  self-test/37-scenario manifest, workspace metadata, all seven package
  license lists, fuzz smoke, race/leak smoke (17 cases × 3), release examples,
  offline benchmark, and Windows GNU cross-compilation passed.
- [ ] Broad P18-V01 remains open. Native Windows runtime, current-revision
  remote Ubuntu CI, and Phase 19 Broker/Docker evidence remain pending. No
  remote CI or push was run; the four existing Phase 19 worktree paths remain
  untouched. Run record:
  `rlogs/2026/10/23-phase-18-factory-null-slot-parity__20261023T101220Z.rlog`.

## P18-V01 MQClientAPIFactory public-provider surface correction — 2026-10-23

- [x] `with_top_addressing` is now a private unit-test seam. The pinned Java
  factory has no public custom-provider injection method; the default domain
  discovery behavior and provider callback tests remain unchanged.
- [x] A compile-fail Rustdoc example guards the external API boundary. All
  seven factory unit tests, all 20 Core doctests, strict Core all-target /
  all-feature Clippy, workspace all-feature Rustdoc JSON, formatting, and
  diff checks passed. The distinct Core public-name count remains 740 because
  the `with_top_addressing` name still appears on a separate Java-mapped
  runtime manager method.
- [ ] This closes one reverse-export discrepancy only. Broad P18-V01,
  current-revision remote Ubuntu CI, native Windows execution, and Phase 19
  Broker/Docker evidence remain open. No remote CI or push was run; existing
  Phase 19 worktree files remain untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T102257Z.rlog`.

## P18-V01 MQClientAPIFactory shutdown parity — 2026-10-23

- [x] `shutdown` skips null slots and still joins all non-null clients;
  a started zero-client pool returns `EmptyClientArray`, matching Java's
  `AsyncShutdownHelper.await` null-latch failure.
- [x] The pinned Java fixture invokes both cases. Fixture hash:
  `cf2dc5b293ea6620fe44732518e24ee97212b4584816083fe672223b4b7ff573`.
  It explicitly shuts down the Java client removed from the public array
  after recording factory behavior so the fixture process exits cleanly.
- [x] All seven Core factory tests, strict Core all-target/all-feature
  Clippy, direct Java fixture generation/verification, formatting, and diff
  checks pass. A first fixture run exposed the removed client's still-live
  Netty threads; fixture cleanup now closes it explicitly after observation.
- [ ] This closes the factory shutdown slice only. Broad P18-V01 and the
  final full-workspace P18-V02 gate remain open, as do native Windows runtime,
  remote Ubuntu CI, and Phase 19 Broker/Docker evidence. No push or remote CI
  was run; existing Phase 19 worktree files remain untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T103539Z.rlog`.

## P18-V01 MQClientAPIFactory empty-pool selection — 2026-10-23

- [x] The invoked Java fixture now records `getClient()` on a started empty
  pool throwing `IllegalArgumentException` from `ThreadLocalRandom.nextInt(0)`;
  Rust reports the typed `EmptyClientArray` outcome.
- [x] Seven Core factory tests, direct Java fixture generation and
  verification, formatting, and diff checks pass. Fixture SHA-256:
  `fcbb745a4eab18d4fa8e5163fa69deeffa935375b660fde8e915c7dab9e2da38`.
- [ ] This closes only the zero-pool selection evidence slice. Broad P18-V01,
  final full-workspace P18-V02, native Windows runtime, remote Ubuntu CI, and
  Phase 19 Broker/Docker evidence remain open. No push or remote CI was run;
  the existing Phase 19 worktree files remain untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T103825Z.rlog`.

## P18-V02 current server-free workspace gate — 2026-10-23 (`74c236ab`)

- [x] Locked default and all-feature workspace tests pass, including doctests;
  strict all-target/all-feature workspace Clippy passes.
- [x] Offline Maven `verify` passes with 37 scenarios and the refreshed
  factory fixture; differential self-test and the 37-scenario manifest pass.
- [x] Metadata/license/dependency checks, deterministic race/leak smoke
  (17 cases × 3), bounded fuzz smoke (163 frame seeds, 11 message seeds,
  2,784 mutations), release examples, and fixed-input offline benchmark pass.
- [x] Core all-target/all-feature Windows GNU cross-compilation, formatting,
  and diff checks pass. This is compile evidence only, not native Windows
  runtime evidence.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and
  reverse-export reconciliation remains open. Native Windows execution and
  remote Ubuntu CI were not run; Phase 19 Broker/Docker remains a separate
  handoff. No push or remote CI was run, and the existing Phase 19 worktree
  files remain untouched. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T104953Z.rlog`.


## P18-V01 factory process-property parity and P18-V02 server-free refresh — 2026-10-23 (`34cbfb8b`)

- [x] `MQClientAPIFactory.init` now leaves Java-equivalent shared property
  effects visible to subsequent `ClientConfig` instances and Common NameServer
  lookup, including failure-order side effects. It does not mutate the OS
  environment. The invoked Java fixture SHA-256 is
  `cd41d77583fc72ec9949d09de805a3055a5303b9f3f514ad8986a3c495af2e6b`.
- [x] Focused property integration test, 240 Core unit tests, default and
  all-feature locked workspace tests, strict all-target/all-feature Clippy,
  Maven `verify` (37 scenarios), differential self-test and manifest (37
  scenarios), workspace metadata/license/dependency check, race/leak smoke
  (17 cases x 3), bounded fuzz smoke, release examples, fixed-input offline
  benchmark, Core Windows GNU cross-compilation, formatter, and diff checks
  passed.
- [ ] Broad P18-V01 reconciliation remains PARTIAL. Native Windows execution,
  current-revision remote Ubuntu CI, and Phase 19 Broker/Docker evidence remain
  open. No push or remote CI was run. The four existing user-owned Phase 19
  worktree paths remain untouched and unstaged. Run record:
  `rlogs/2026/09/19-phase-18-factory-process-properties__20261023T111654Z.rlog`.

## P18-V01 factory scheduler constructor parity — 2026-10-23 (`099fd61b`)

- [x] Mapped the `ScheduledExecutorService` parameter accepted by both pinned
  Java `MQClientAPIFactory` constructors. Rust now exposes
  `FactoryRefreshScheduler` through `with_refresh_scheduler`; domain startup
  registers one callback per client with the Java 10-second initial delay and
  120-second fixed rate. The caller retains scheduler ownership.
- [x] Factory regressions cover registration count, timing, slot-local address
  updates and rejected schedule registration. The Rustdoc reverse-name audit
  now maps 62 Core and 15 Consumer names added since `1dc3a7e2`, with no
  removed names. Current counts: Common 439, Consumer 460, Core 744, Producer
  362, Protocol 1,524, Remoting 337.
- [x] Core all-feature tests (242), Core doctests (20), strict Core Clippy,
  Rustdoc JSON, formatting and diff checks passed. Full default and all-feature
  workspace tests/doctests, strict workspace Clippy, and offline Maven verify
  also passed.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow review remains
  PARTIAL. Native Windows execution and current-revision remote Ubuntu CI were
  not run. Phase 19 Broker/Docker validation remains separate. No push or
  remote CI was run; user-owned Phase 19 worktree paths remain untouched.
  Run record:
  `rlogs/2026/10/23-phase-18-factory-scheduler-injection__20261023T114031Z.rlog`.

## P18-V02 current server-free workspace gate after scheduler API — 2026-10-23 (`099fd61b`)

- [x] Locked default and all-feature workspace tests and doctests passed.
- [x] Strict all-target/all-feature workspace Clippy, format/diff checks,
  offline Java `verify` (37 scenarios), differential self-test and manifest
  validation (37 scenarios), and workspace metadata/license/dependency checks
  passed.
- [x] Deterministic race/leak smoke passed 17 cases × 3; bounded fuzz smoke
  passed 163 frame seeds, 11 message seeds and 2,784 mutations; release
  examples and the fixed-input offline benchmark passed.
- [x] All-target/all-feature Core Windows GNU cross-compilation passed. This
  is compile evidence only, not native Windows runtime evidence.
- [ ] This refresh closes the local P18-V02 server-free gate only. Broad
  P18-V01 remains PARTIAL; current-revision remote Ubuntu CI and native Windows
  execution remain open. Phase 19 Broker/Docker validation is outside this
  local gate. No push or remote CI was run. Run record:
  `rlogs/2026/10/23-phase-18-factory-scheduler-injection__20261023T114031Z.rlog`.

## P18-V01 factory refresh rejection ordering and P18-V02 local refresh — 2026-10-23

- [x] Corrected factory startup to register each domain refresh callback after
  its client is constructed and discovered, before starting the client and
  assigning the returned instance into that slot. A rejected schedule now
  leaves the failing slot null and preserves earlier completed slots, matching
  pinned Java `createAndStart` ordering.
- [x] Added an invoked Java rejection case and Rust differential for the
  exception type, registration count, remoting-creator order and null slot.
- [x] Locked default/all-feature workspace tests and doctests, strict workspace
  Clippy, pinned Maven `verify` (37 scenarios), the factory-fixture verifier,
  differential self-test/37-scenario manifest, workspace metadata, 17 race/
  leak cases × 3, fuzz smoke, release examples, benchmark, Windows GNU
  cross-check, formatting and diff checks passed.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and
  reverse-export reconciliation remains PARTIAL. Native Windows runtime and
  current-revision remote Ubuntu evidence were not run. Phase 19 Broker/Docker
  validation remains separate. No push or remote CI was run; the four
  user-owned Phase 19 paths remain untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-factory-refresh-rejection-order__20261023T121346Z.rlog`.


## P18-V01 MQClientAPIFactory null scheduler input and P18-V02 local refresh — 2026-10-23 (`8fd5822e`)

- [x] Added `with_optional_refresh_scheduler(None)` to represent Java's nullable
  constructor input. Static-address startup succeeds without using the
  scheduler; domain startup returns the Java-equivalent null-pointer outcome
  after initial discovery and leaves the current slot null.
- [x] The invoked Java fixture covers both cases; its SHA-256 is
  `747b7af85d91eab83ac1764e2521754dbbe0f2eec183582af0c324a012404af2`. The
  focused Core factory suite passed (10 tests), as did both locked workspace
  suites/doctests, strict workspace Clippy, Maven `verify` (37 scenarios),
  direct fixture verification, differential self-test/37-scenario manifest,
  metadata and seven package listings, stress smoke (17 cases × 3), fuzz
  smoke (163 frame seeds, 11 message seeds, 2,784 mutations), release examples,
  benchmark, Core Windows GNU cross-check, Rustdoc JSON, format and diff checks.
- [ ] Broad P18-V01 reconciliation remains PARTIAL. Current-revision remote
  Ubuntu CI and native Windows execution remain open; Phase 19 Broker/Docker
  work remains outside scope. No push or remote CI was run. The four existing
  user-owned Phase 19 paths remain untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-factory-null-scheduler__20261023T123306Z.rlog`.

## P18-V01 runtime NameServer provider export crosswalk — 2026-10-23

- [x] Added the exact `ClientRuntimeManager::with_top_addressing` mapping to
  Java's `TopAddressing` ServiceLoader extension and distinguished the
  manager's injected capability from the now-private factory test seam.
- [x] Reviewed the Rust public declaration, the Java `DefaultTopAddressing`
  provider-loading path, the existing manager/provider tests, and the refreshed
  all-feature Rustdoc JSON inventory. `git diff --check` passed; no runtime
  code or tests changed.
- [ ] This closes one API-map omission only. Broad P18-V01 remains open;
  native Windows execution and current-revision remote Ubuntu CI remain
  pending. Phase 19 Broker/Docker validation remains outside scope. No push or
  remote CI was run; the four existing user-owned Phase 19 paths remain
  untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-manager-addressing-export-crosswalk__20261023T132628Z.rlog`.

## P18-V01 Remoting response/error factory reverse-name crosswalk — 2026-10-23

- [x] Added explicit Rust method names for response/error factory helpers and
  their Java `RemotingCommand` overloads. The existing grouped fixture and
  differential evidence already covers the corresponding behavior.
- [x] Reviewed pinned Java declarations, Rust methods/call sites, the fixture
  coverage, and `git diff --check`. No runtime code or tests changed.
- [ ] This closes one exact-name crosswalk omission only; broad P18-V01 remains
  open. Native Windows execution and current-revision remote Ubuntu CI remain
  pending. Phase 19 Broker/Docker validation remains outside scope. No push or
  remote CI was run; the four existing user-owned Phase 19 paths remain
  untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-remoting-factory-export-crosswalk__20261023T132731Z.rlog`.

## P18-V01 public tuple error payload privacy — 2026-10-23 (`65cf3cad`)

- [x] Made `RequestSendFailureCause` and `TransactionCallbackError` payloads
  private, while retaining public `new` and read-only `message()` methods.
  Compile-fail doctests guard the removed public tuple fields.
- [x] Core and Producer all-feature test suites/doctests, strict all-target /
  all-feature Clippy for both crates, workspace all-feature Rustdoc JSON,
  formatting, and diff checks passed. Rustdoc confirms no public tuple field
  named `0`; Core exports 746 distinct names and Producer 361.
- [ ] This closes two extra error-payload access surfaces only. Broad P18-V01
  remains open; native Windows execution and current-revision remote Ubuntu CI
  remain pending. Phase 19 Broker/Docker validation remains outside scope. No
  push or remote CI was run; the four existing user-owned Phase 19 paths remain
  untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-error-payload-privacy__20261023T133226Z.rlog`.

## P18-V01 transaction callback error identity semantics — 2026-10-23 (`a4228eb2`)

- [x] Removed `Clone`, `PartialEq`, and `Eq` from `TransactionCallbackError`;
  Java callback exceptions use inherited identity equality. A compile-fail
  doctest guards against adding value traits over the message.
- [x] Producer all-feature tests/doctests and strict all-target/all-feature
  Clippy passed; formatting and diff checks passed.
- [ ] This closes one error-wrapper identity mismatch only. Broad P18-V01
  remains open; native Windows execution and current-revision remote Ubuntu CI
  remain pending. Phase 19 Broker/Docker validation remains outside scope. No
  push or remote CI was run; the four existing user-owned Phase 19 paths remain
  untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-callback-error-identity__20261023T133454Z.rlog`.

## P18-V01 Lite subscription control request-header inheritance — 2026-10-23 (`5acaab89`)

- [x] Mapped the pinned Java `LiteSubscriptionCtlRequestHeader` inherited
  `RpcRequestHeader` fields, accessors, equality/hash behavior, and string form.
  Rust now encodes and decodes nullable `ns`, `nsd`, `bname`, and `oway`.
- [x] Updated the pinned Java identity/API fixture and added Rust checks for
  accessor inventory, base-field differences, hash/string values, and wire
  round-trip. Fixture SHA-256:
  `421e054be509f5f56fb5f1fe0cd5870745c83e956f56831082858e9780b4a10d`.
- [x] Protocol header inventory/matrix tests, Protocol all-feature tests and
  doctests, strict all-target/all-feature Protocol Clippy, formatting/diff
  checks, and offline Maven `verify` passed. Maven verified 37 scenarios,
  11 message fixtures, 106 typed headers, and 19 typed bodies.
- [ ] This closes one Protocol inheritance mapping only. Broad P18-V01 remains
  open; native Windows runtime and current-revision remote Ubuntu CI remain
  pending. Phase 19 Broker/Docker validation remains outside scope. No push or
  remote CI was run; the four existing user-owned Phase 19 paths remain
  untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-lite-rpc-header-inheritance__20261023T143107Z.rlog`.

## P18-V01 remaining RPC request-header identity/API coverage — 2026-10-23 (`082809d1`)

- [x] Added `CloneGroupOffsetRequestHeader`,
  `GetSubscriptionGroupConfigRequestHeader`, and
  `QueryConsumeQueueRequestHeader` to the pinned Java identity/API oracle.
  Added explicit Rust base-field, payload-ignore, hash, and wire round-trip
  checks for all six previously unexercised request headers; all 49 fixture
  entries are now covered. Fixture SHA-256:
  `7e3f828aca4785153cc98b2f71bb38a93b33421df55bb3cf1cb5338a68e1b594`.
- [x] Java fixture drift verification, Protocol header inventory/validation,
  request-header identity/hash tests, strict all-target/all-feature Protocol
  Clippy, formatting/diff checks, and offline Maven `verify` passed. The Maven
  run verified 37 scenarios, 11 message fixtures, 106 typed headers, and
  19 typed bodies.
- [ ] This closes the request-header identity/API inventory slice only. Broad
  P18-V01 remains open; native Windows runtime and current-revision remote
  Ubuntu CI remain pending. Phase 19 Broker/Docker validation remains outside
  scope. No push or remote CI was run; the four existing user-owned Phase 19
  paths remain untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-rpc-header-identity-inventory__20261023T143907Z.rlog`.

## P18-V01 Lite request-header Core call-site update — 2026-10-23 (`8288cc98`)

- [x] Updated all six Core uses of `LiteSubscriptionCtlRequestHeader` to pass
  `Default`; the empty default header emits no ext fields and preserves the
  existing request wire shape.
- [x] Full workspace all-feature tests/doctests and strict workspace Clippy
  passed after the changes. `cargo fmt --all -- --check` and `git diff --check`
  passed.
- [ ] This closes downstream call-site fallout from the Lite header mapping
  only. Broad P18-V01 remains open; native Windows runtime and current-revision
  remote Ubuntu CI remain pending. Phase 19 Broker/Docker validation remains
  outside scope. No push or remote CI was run; the four existing user-owned
  Phase 19 paths remain untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-lite-core-header-call-sites__20261023T144643Z.rlog`.

## P18-V01 RPC request-header source/fixture parity guard — 2026-10-23 (`7885fbe5`)

- [x] Added a set-parity assertion between the Java request-header identity
  fixture and Rust's `RpcRequestHeader` equality mapping list. The existing
  separately implemented `ConsumeMessageDirectlyResultRequestHeader` remains
  explicitly excluded from the macro comparison.
- [x] The request-header equality/hash/codec test, strict all-target/all-feature
  Protocol Clippy, formatting, and diff checks passed.
- [ ] This prevents future inventory omissions but closes no additional runtime
  behavior. Broad P18-V01 remains open; native Windows runtime and
  current-revision remote Ubuntu CI remain pending. Phase 19 Broker/Docker
  validation remains outside scope. No push or remote CI was run; the four
  existing user-owned Phase 19 paths remain untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-rpc-header-parity-guard__20261023T145016Z.rlog`.

## P18-V04 Windows GNU workspace cross-target check — 2026-10-23 (`77587adc`)

- [x] On macOS AArch64, the complete workspace passed all-target/all-feature
  `cargo check` and strict Clippy for `x86_64-pc-windows-gnu`, including the
  Integration crate and test targets.
- [ ] This is compile-only cross-target evidence. Native Windows runtime and
  current-revision remote Ubuntu CI remain open; Phase 19 Broker/Docker work
  remains outside scope. No push or remote CI was run. The four existing
  user-owned Phase 19 paths remain untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-windows-gnu-workspace-cross-target__20261023T145158Z.rlog`.

## P18-V01 response-header identity and API inventory — 2026-10-23 (`e555c725`)

- [x] Expanded the invoked pinned Java 5.5.1 identity and API inventory from
  16 to all 22 Rust response headers, adding the six omitted types. Recorded
  the NameServer package and Java `GetKVConfigResponseHeader` spelling. The
  fixture confirms each inherits `Object.equals/hashCode`; the Protocol test
  checks every class's constructor, fields, and callable method inventory
  against the Rust API.
- [x] Full Protocol tests passed (160 unit tests, integration targets, and
  166 doctests), strict all-target/all-feature Protocol Clippy passed, and
  formatting/diff checks passed. Offline Maven verify passed (37 scenarios,
  fixture drift, and Java/Rust cross-decodes of 11 message fixtures, 106 typed
  headers, and 19 typed bodies). Fixture SHA-256:
  `4e17f64398e2ae742f1fae55841e365eb7aed89b3f6aaad7eb191e8d282dd8ae`.
- [ ] This closes the response-header identity/API inventory omission only;
  broad P18-V01 remains PARTIAL. Native Windows runtime and current-revision
  remote Ubuntu CI remain open; Phase 19 Broker/Docker validation remains
  outside scope. No push or remote CI was run. The four existing user-owned
  Phase 19 paths remain untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-response-header-api-inventory__20261023T145913Z.rlog`.

## P18-V02 current-tree server-free gate — 2026-10-23 (`98457158`)

- [x] On a clean temporary Cargo target directory, default and all-feature
  locked workspace tests/doctests and strict all-target/all-feature workspace
  Clippy passed. Offline Maven `verify` passed 37 scenarios, fixture drift,
  and Java/Rust cross-decodes of 11 message fixtures, 106 typed headers, and
  19 typed bodies. Differential self-test and 37-scenario manifest validation,
  workspace metadata/dependency checks, all seven product package listings
  with `LICENSE`/`NOTICE`, 17 race/leak cases × 3, and bounded fuzz smoke (163
  frame seeds, 11 message seeds, 2,784 mutations) passed. Release examples,
  the offline benchmark, formatting, and diff checks passed.
- [x] Full-workspace all-target/all-feature Windows GNU check and strict
  Clippy passed as cross-compilation evidence. Rustdoc JSON export counts are
  Common 439, Consumer 460, Core 747, Producer 361, Protocol 1,524, and
  Remoting 337. Benchmark observations were appended to
  `PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`; no performance threshold is set.
- [ ] One earlier default run encountered a single transient
  `LocalFileOffsetStore` `AlreadyOpen` failure when reopening after drop. The
  full default rerun, all-feature rerun, three complete Consumer unit runs,
  and ten concurrent offset-store test-group runs all passed without a code
  change. Retain this as a test observation for follow-up. Broad P18-V01 remains
  PARTIAL; native Windows runtime, current-revision remote Ubuntu CI, and
  Phase 19 Broker/Docker evidence remain open. No push or remote CI was run;
  the four existing user-owned Phase 19 paths remain untouched and unstaged.
  Run record:
  `rlogs/2026/10/23-phase-18-current-tree-server-free-gate__20261023T152405Z.rlog`.

## P18-V01 complete CommandHeader API inventory and Notify RPC inheritance — 2026-10-23 (`e7d779af`)

- [x] Added a pinned Java 5.5.1 API inventory for the 25 request-header
  classes missing class-level API evidence. Constructor, public-field,
  declared-method, and callable-method entries are recorded. The Rust parity
  test now checks the five-fixture union against all 103 public
  `CommandHeader` implementations, with an explicit NameServer `DeleteKV`
  spelling map and the non-header `ExportRocksDBConfigType` excluded.
- [x] The expanded request-header identity fixture now has 50 entries.
  Inventory review also found that Java
  `NotifyUnsubscribeLiteRequestHeader` inherits `RpcRequestHeader`; Rust now
  models its nullable routing fields, accessors, codec mapping, and base-field
  equality/hash behavior.
- [x] Default and all-feature Protocol tests/doctests, strict all-target and
  all-feature Protocol Clippy, format, and diff checks passed. Offline Maven
  verify passed 37 scenarios, fixture drift, 11 message fixtures, 106 typed
  headers, and 19 typed bodies. Fixture SHA-256 values:
  `54df2a5effcb30ecdab9303c78d404a05916576bec60d1f33e540ec28ae6b688`
  (`remaining-header-api.json`) and
  `38c163fea5ca151b00c450e709cc7006cb442e39799c34b3eecde930d4eeb10d`
  (`request-header-identity.json`).
- [ ] This closes the public header API inventory gap and the identified
  Notify Lite inherited-routing mismatch. Broad P18-V01 remains PARTIAL.
  Native Windows runtime and current-revision remote Ubuntu CI remain open;
  Phase 19 Broker/Docker validation remains out of scope. No push or remote CI
  was run. The four existing user-owned Phase 19 paths remain untouched and
  unstaged. Run record:
  `rlogs/2026/10/23-phase-18-complete-header-api-inventory__20261023T153747Z.rlog`.

## P18-V02 current-tree server-free gate — 2026-10-23 (`e7d779af`)

- [x] Current-revision sequential default and all-feature workspace tests and
  doctests passed. Strict all-target/all-feature workspace Clippy, Java
  fixture verification (37 scenarios, 11 messages, 106 typed headers, and 19
  typed bodies), differential self-test and 37-scenario manifest, workspace
  metadata, and all seven product package listings with `LICENSE`/`NOTICE`
  passed.
- [x] Deterministic race/leak smoke passed 17 cases × 3; bounded fuzz smoke
  passed 163 frame seeds, 11 message seeds, and 2,784 mutations. Release
  examples and the offline benchmark passed. Full-workspace Windows GNU
  all-target/all-feature check and strict Clippy passed as cross-compile
  evidence. Rustdoc public-name counts remain Common 439, Consumer 460, Core
  747, Producer 361, Protocol 1,524, and Remoting 337.
- [ ] Running default and all-feature workspace suites concurrently produced
  three Consumer `OffsetStore(AlreadyOpen)` failures. Both complete suites
  passed when rerun sequentially, with no code change. Retain this as a
  concurrent-run observation. Broad P18-V01 remains PARTIAL; native Windows
  runtime and current-revision remote Ubuntu CI remain open. Phase 19
  Broker/Docker evidence is outside scope. No push or remote CI was run. The
  four existing user-owned Phase 19 paths remain untouched and unstaged. Run
  record:
  `rlogs/2026/10/23-phase-18-current-tree-server-free-gate__20261023T154720Z.rlog`.

## P18-V01 Consumer/Producer connection Set DTO parity — 2026-10-23 (`05207a17`)

- [x] Replaced the two Rust `Vec` DTO fields with an identity-keyed HashSet
  representation matching Java `HashSet<Connection>`, including repeated
  references, null members, decoded same-field objects, and shared-container
  getter/setter behavior. The Java oracle records direct API and JSON outcomes.
- [x] Default and all-feature workspace tests, the final Protocol library
  suite (160 tests), the ConsumerConnection admin RPC regression, strict
  all-target/all-feature workspace Clippy, offline Maven `verify`, formatting,
  and diff checks passed. Maven verified 37 scenarios and Java/Rust fixture
  cross-decodes.
- [ ] This closes the two DTO Set collection mismatch only. Broad P18-V01
  remains open; current-revision remote CI and native Windows runtime evidence
  remain outstanding. Phase 19 remains out of scope. No push or remote CI was
  run. The four protected Phase 19 worktree paths remain untouched and
  unstaged. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T162602Z.rlog`.

## P18-V01 ProducerConnection assigned-Set alias — 2026-10-23 (`b41efa8b`)

- [x] Added direct pinned-Java evidence that `ProducerConnection` retains the
  assigned `HashSet` reference and exposes that same mutable set from its
  getter. The Java probe uses an isolated DTO so it does not change the RPC
  response fixture.
- [x] Rust verifies `SharedValue::ptr_eq` and mutation visibility through the
  assigned set. Protocol library tests (160), strict workspace Clippy, offline
  Maven `verify` (37 scenarios and cross-decodes), formatting, and diff checks
  pass. Updated producer fixture SHA-256:
  `412fb8f5d9805edb2f994876d81ca980bfccc2963a1aff2bea19b98319a1eb5a`.
- [ ] This closes this assigned-Set alias evidence gap only. Broad P18-V01
  remains PARTIAL; current-revision remote CI and native Windows runtime
  evidence remain open. Phase 19 is out of scope. No push or remote CI was run;
  the four protected Phase 19 paths remain untouched and unstaged. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T163451Z.rlog`.

## P18-V02 current-tree server-free refresh after 2,000 concurrent sends — 2026-10-23 (`148b2528`)

- [x] The producer loopback suite now starts 2,000 async sends concurrently
  against a scripted Broker, checks all 2,000 unique bodies and message IDs,
  and asserts no retry or duplicate request. The 18-case deterministic
  race/leak runner passed three repetitions for every case.
- [x] Current-tree default and all-feature workspace tests/doctests, strict
  all-target/all-feature Clippy, formatting, diff checks, pinned offline Maven
  `verify` (37 scenarios, 11 message fixtures, 106 typed headers, 19 typed
  bodies and Rust/Java cross-decodes), differential self-test and 37-scenario
  manifest, package metadata/license/dependency checks, fuzz smoke (163 frame
  seeds, 11 message seeds, 2,784 mutations), release examples, offline
  benchmark, Unique ID under `Asia/Taipei`, and workspace Windows GNU
  all-target/all-feature cross-check passed.
- [x] The benchmark observations are appended to
  `PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`; they set no performance threshold.
  Native Windows runtime and remote CI are not claimed.
- [ ] This closes the P18-04 thousands-of-concurrent-sends smoke item and
  refreshes the local P18-V02 server-free gate only. Broad P18-V01 Java
  source/API/settings/input/error/control-flow and reverse-export audit remains
  PARTIAL. Native Windows runtime, current-revision remote Ubuntu CI, and
  Phase 19 live Broker/Docker scenarios remain separate. No push or remote CI
  was run; the four existing Phase 19 worktree paths remain untouched and
  unstaged. Run record:
  `rlogs/2026/09/phase-18-compatibility-release-gate__20261023T183115Z.rlog`.

## P18-V02 current-tree server-free refresh after sustained offset persistence — 2026-10-23 (`d4e9c7ac`)

- [x] Added a 128-round local-offset persistence regression over eight queues.
  Each round verifies the atomic snapshot and restart recovery verifies the
  final offsets. The deterministic race/leak runner passed 19 cases × 3.
- [x] Sequential default and all-feature workspace tests/doctests passed at
  this revision. Strict all-target/all-feature Clippy, formatting, diff check,
  workspace metadata, fuzz smoke (163 frame seeds, 11 message seeds, 2,784
  mutations), differential self-test and 37-scenario manifest, pinned offline
  Maven `verify` (37 scenarios and Java cross-decodes of 11 messages, 106
  typed headers, and 19 typed bodies), and Windows GNU all-target/all-feature
  cross-check passed. Windows is compile evidence only.
- [x] P18-04 start/stop coverage was audited: remoting start/shutdown ownership
  is repeated by the stress runner, alongside concurrent runtime construction,
  reconnect, callback shutdown, pending cleanup, producer sends, and consumer
  ownership races. No additional producer lifecycle test was needed for that
  plan item.
- [ ] Running default and all-feature workspace suites concurrently again
  caused two pull-consumer tests to fail with `OffsetStore(AlreadyOpen)`;
  both suites passed sequentially with no code change. Broad P18-V01 Java
  source/API/settings/input/error/control-flow and reverse-export audit
  remains PARTIAL. Native Windows runtime, current-revision remote CI, and
  Phase 19 live Broker/Docker scenarios remain open or out of scope. Release
  examples and the offline benchmark retain their earlier results because
  this revision changes only tests and stress documentation. No plan was
  edited, no remote CI or push was run, and the four existing Phase 19 paths
  remain untouched and unstaged. Run record:
  `rlogs/2026/09/phase-18-compatibility-release-gate__20261023T184207Z.rlog`.

## P18-V02 current-tree server-free refresh after compatibility corrections — 2026-10-23 (`875e47d9`)

- [x] NameServer scan admission now follows Java's configured-order bounded
  executor submission and `AbortPolicy` retry ordering. Unix offset-store
  process ownership now uses non-inherited POSIX record locks; a subprocess
  test checks exclusion during ownership and acquisition after drop. The
  deterministic race/leak runner passed 20 cases × 3 repetitions.
- [x] Three consecutive parallel all-feature Consumer library runs passed.
  Sequential default and all-feature workspace tests/doctests, strict
  all-target/all-feature workspace Clippy, formatting/diff checks, Windows GNU
  all-target/all-feature check and strict Clippy, offline Maven `verify` (37
  scenarios, 11 message fixtures, 106 typed headers, 19 typed bodies, and
  Java/Rust cross-decodes), differential self-test and 37-scenario manifest,
  fuzz smoke (163 frame seeds, 11 message seeds, 2,784 mutations), release
  examples, offline benchmark, and workspace metadata/license/dependency
  checks passed.
- [x] Rustdoc JSON generation passed. Its inventory remains name-level
  evidence, not behavioral proof. Benchmark observations are machine-specific
  and set no threshold.
- [ ] Broad P18-V01 bidirectional source/API/settings/input/error/control-flow
  and reverse-export reconciliation remains PARTIAL. Native Windows runtime,
  current-revision remote CI, and Phase 19 live Broker/Docker scenarios remain
  open or out of scope. No plan was edited, no remote CI or push was run, and
  the four pre-existing Phase 19 worktree paths remain untouched and
  unstaged. Run record:
  `rlogs/2026/09/phase-18-compatibility-release-gate__20261023T192026Z.rlog`.

## P18-V01 inbound remoting shutdown version gate — 2026-10-23 (`a2eb40c2`)

- [x] Pinned Java 5.5.1 `processRequestCommand` returns `GO_AWAY` (1500,
  `please go away`) for a request above `V5_3_1` ordinal 475 during shutdown,
  after processor lookup and before request rejection. Added an invoked Java
  fixture for versions 475/476, a running-client case, unsupported-code
  ordering, and response opaque, plus matching Rust version and dispatch-order
  checks.
- [x] Sequential default and all-feature workspace suites, all 143 remoting
  tests, strict workspace Clippy, Windows GNU all-target/all-feature check and
  Clippy, formatting/diff checks, and the dedicated four-case Java fixture
  verification passed.
- [x] The first full offline Maven `verify` attempt stopped at the pre-existing
  `ConsumerLifecycleFixtures` default NameServer DNS lookup. Follow-up commit
  `e6e1d62e` pins that fixture to loopback; the full Maven gate then passed, as
  recorded in the next entry. Broad P18-V01 reconciliation remains PARTIAL;
  native Windows runtime, current-revision remote CI, and Phase 19 live
  Broker/Docker scenarios remain separate. No plan was edited, no remote CI or
  push was run, and the four Phase 19 worktree paths remain untouched and
  unstaged. First-attempt run record:
  `rlogs/2026/09/phase-18-compatibility-release-gate__20261023T193945Z.rlog`.

## P18-V02 offline Maven fixture isolation refresh — 2026-10-23 (`e6e1d62e`)

- [x] `ConsumerLifecycleFixtures.failedLitePullMetadataSchedule` now uses an
  explicit loopback NameServer address, preventing the expected startup-failure
  probe from attempting default external DNS discovery. Its fixture verification
  and the full offline Maven `verify` pass; the full run validated 37 scenarios,
  11 message fixtures, 106 typed headers, 19 typed bodies, and Java/Rust
  cross-decodes, including the four new shutdown cases.
- [x] Current-tree sequential default/all-feature workspace tests, strict
  all-target/all-feature Clippy, Windows GNU all-target/all-feature check and
  Clippy, formatting, and diff checks also pass. The current P18-V01 shutdown
  behavior remains a narrow closure only; the broad API/settings/input/error/
  control-flow and reverse-export audit remains PARTIAL. Native Windows runtime,
  current-revision remote CI, and Phase 19 live Broker/Docker scenarios remain
  separate. No plan was edited, no push or remote CI was run, and the four
  pre-existing Phase 19 paths remain untouched and unstaged. Run record:
  `rlogs/2026/09/phase-18-compatibility-release-gate__20261023T194523Z.rlog`.

## P18-V01 scheduler `shutdownNow()` returned-task semantics — 2026-10-23 (`124afee8`)

- [x] Pinned Java 5.5.1 `ScheduledExecutorService.shutdownNow()` returns the
  queued future objects and leaves them neither cancelled nor done; queued
  callbacks do not run. `fixtures/runtime/scheduled-executor-shutdown.json`
  captures both scheduler owners. The client scheduler returns 32 tasks in
  this pre-start probe: two submitted futures plus 30 recurring
  `ConsumerStatsManager` jobs; the pull scheduler returns the two submitted
  futures.
- [x] Rust `RuntimeScheduler::shutdown_now()` now returns queued task handles.
  Submitted calls preserve handle identity, queued handles remain pending,
  and callers may cancel them after shutdown. Client periodic work and delayed
  pull request/task entries are also represented in the returned list.
- [x] Focused client and pull scheduler regressions pass. Default and
  all-feature `cargo test --workspace`, strict all-target/all-feature
  workspace Clippy, Windows GNU workspace check/Clippy, formatting, diff
  checks, and direct Java fixture verification pass. The full offline Maven
  `verify` also passes with 37 scenarios, 11 message fixtures, 106 typed
  headers, 19 typed bodies, and Rust/Java cross-decodes.
- [x] At this revision, Java's 30 `StatsItemSet` jobs were missing from the
  Rust client scheduler's `shutdownNow()` returned list. The runtime scheduler
  now returns all 30 manager jobs alongside the two submitted futures, and the
  standalone constructor schedules the same 30 jobs on a supplied scheduler.
  The queue-content delta was closed by commits `3b9d9473` and `4d32bc97`, as
  recorded in the later stats schedule entry. Broad P18-V01 reconciliation
  remains PARTIAL. No plan was edited, no remote CI or push was run, and the
  four Phase 19 worktree paths remain untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-scheduler-shutdown-return-list__20261023T200933Z.rlog`.

## P18-V01 stats schedule single source and P18-V02 current-tree gates — 2026-10-23 (`2598c76f`)

- [x] Built-in runtime scheduling and public standalone
  `ConsumerStatsManager::with_scheduler` construction now use the same
  `stats_schedule_specs()` source for the 30 jobs, their order, cadence, and
  local-boundary delays. The schedule identity regression checks each set's
  three sampler periods and three print windows. Commit `4d32bc97` contains
  this refactor.
- [x] Full default workspace tests/doctests passed after the scheduler-spec
  refactor. The complete all-features workspace suite/doctests passed on the
  current tree. The all-features run initially exposed a process-global
  `TopicConfig` default mutation race in its API fixture; three tests that read
  or mutate those defaults now share a test mutex (`2598c76f`). The focused
  all-features protocol fixture passed.
- [x] Strict all-target/all-feature workspace Clippy, Windows GNU all-target/
  all-feature check and strict Clippy, formatting/diff checks, and offline
  Maven `verify` passed. Maven validated 37 scenarios, 11 message fixtures,
  106 typed headers, 19 typed bodies, and Java/Rust cross-decodes.
- [ ] Broad P18-V01 bidirectional API/settings/input/error/control-flow and
  reverse-export reconciliation remains PARTIAL. Native Windows runtime and
  current-revision remote CI remain open; Phase 19 Broker/Docker work remains
  out of scope. No plan was edited, no push or remote CI was run, and the four
  pre-existing Phase 19 paths remain untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-stats-schedule-single-source__20261023T212953Z.rlog`.

## P18-V02 current-tree server-free gate after inner-producer fault coverage — 2026-10-23 (`ea17b8fc`)

- [x] Sequential offline default and all-feature locked workspace tests and
  doctests passed on code/test revision `ea17b8fc`; strict all-target/all-feature
  Clippy, formatting, and diff checks passed.
- [x] Offline Maven `verify` passed 37 scenarios, fixture drift, and
  Java/Rust cross-decodes for 11 message fixtures, 106 typed headers, and 19
  typed bodies. Comparator self-test and 37-scenario manifest validation,
  including every pending source gate ID collected from the Phase 0–17 plans,
  workspace metadata/dependency/license checks, and seven product package
  listings containing `LICENSE` and `NOTICE` passed.
- [x] The manifest has 37 live scenario rows; seven have archived Phase 19
  evidence and 30 remain `PENDING_PHASE_19`. Current local server-free tests
  run without Docker or a Broker. No remote CI was run.
- [x] Deterministic race/leak smoke passed 20 cases × 3; fixed-seed fuzz smoke
  passed 163 frame seeds, 11 message seeds, and 2,784 mutations (245 frame and
  192 message decodes). Release examples and the offline benchmark passed.
  Whole-workspace Windows GNU all-target/all-feature check and strict Clippy
  passed as compile-only evidence.
- [ ] Broad P18-V01 bidirectional source/API/settings/input/error/control-flow
  and reverse-export reconciliation remains PARTIAL. Native Windows runtime
  and current-revision remote Ubuntu CI remain unverified; no push or remote CI
  was run. Phase 19 Broker/Docker evidence remains out of scope. The four
  pre-existing Phase 19 worktree paths remain untouched and unstaged. Run
  record: `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T221410Z.rlog`.

## P18-V03 documentation and package preparation review — 2026-10-23 (`ea17b8fc`)

- [x] The facade README documents the Java 5.5.1 client baseline separately
  from the RocketMQ 5.3.4 Phase 19 Broker baseline, send timeout and duplicate
  delivery semantics, consumer redelivery, and shutdown/offset behavior. It
  continues to state that full compatibility and release readiness are not
  claimed.
- [x] The release checklist and compatibility docs keep unit status distinct
  from pending live evidence; the Phase 19 manifest remains the handoff for
  server-backed cases.
- [x] `scripts/check-workspace-metadata.py` confirms Apache-2.0 metadata,
  `publish = false`, package direction, and exact `LICENSE`/`NOTICE` contents.
  All seven product `cargo package --list --offline --locked` checks include
  both documents. The release example build passes.
- [x] No publication, release tag, or compatibility claim was made. This
  closes the P18-V03 local preparation review only; broad P18-V01 and P18-V04
  remain open. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T221410Z.rlog`.

## P18-V01 Controller DTO behavior closure — 2026-10-23

- [x] Rust Controller DTOs map `BrokerReplicasInfo.addReplicaInfo`, all three
  `ReplicasInfo.isExistIn*` methods, Java nullable nested map/list state,
  `ReplicaIdentity.equals`/`hashCode`, `BrokerMemberGroup.minimumBrokerId`,
  Java-overridden DTO hash/string methods, and `EpochEntry` copy construction.
- [x] The fixture-backed API and operation cases are recorded in
  `fixtures/producer/core.json` (`controllerDtoApi` and
  `controllerDtoOperations`; SHA-256
  `b45df3622bca04a1ea4ba93e5700abe235745908ea2342af4b6ad51ca75fdeb6`).
- [x] Full Core tests and doctests, strict all-target/all-feature Core Clippy,
  and pinned offline Maven `verify` pass on the current source revision.
- [ ] Broad P18-V01 reconciliation remains PARTIAL; native Windows runtime,
  current-revision remote Ubuntu CI, and Phase 19 Broker/Docker validation
  remain open. This DTO slice does not establish Phase 18 acceptance. Run log:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T231037Z.rlog`.

## P18-V01 internal producer latency-fault branch closure — 2026-10-23 (`ea17b8fc`)

- [x] The earlier retry-topic entry left the configured
  `sendLatencyEnable=true` selector open. The internal Producer follow-up now
  reuses the copied Java `ClientConfig`, a runtime-shared fault table, the
  pinned latency selector, and detector startup through the Consumer's
  existing `ClientRuntime` extension.
- [x] Runtime identity/isolation, fault-based Broker choice, detector startup,
  retry budget, and outcomes are covered by the Core, Producer, and Consumer
  regressions. Default and all-feature Core/Producer/Consumer suites and strict
  all-target/all-feature Clippy pass on revision `ea17b8fc`.
- [ ] This closes the internal producer latency-fault branch only. Broad
  P18-V01 reconciliation remains PARTIAL. See the source-backed entry in
  `docs/compatibility/PHASE_6_REQUIREMENT_AUDIT.md`.

## P18-V01 inbound Remoting failure-flow comparison — 2026-10-23

- [x] Invoked pinned Java `NettyRemotingAbstract.processRequestCommand` with a
  local `EmbeddedChannel`, direct executor, and real hook/pipeline/processor
  implementations. The fixture records ordinary failures at all four stages,
  aborts at before/pipeline/after, one-way silence, response codes/opaque, and
  callback order.
- [x] Rust fixture-backed loopback test passes for all eight cases. Ordinary
  failures map to SYSTEM_ERROR, aborts keep their chosen codes, processor and
  after-hook execution matches Java, and one-way failures write no response.
  The Java exception class is compared while exact first-stack-frame remarks
  remain a diagnostic difference.
- [ ] This closes the remoting error-flow concern only. Broad P18-V01 remains
  PARTIAL; native Windows runtime, current-revision remote Ubuntu CI, and
  Phase 19 Broker/Docker scenarios remain separate. No plan was edited, no
  push or remote CI was run, and the four protected Phase 19 worktree paths
  remain untouched and unstaged. Run record:
  `rlogs/2026/10/23-phase-18-remoting-error-flow__20261023T234626Z.rlog`.

## P18-V01 Rustdoc export and controller DTO reverse scan — 2026-10-23 (`df5c3c59`)

- [x] Current all-feature Rustdoc JSON generation passes. Distinct public name
  counts are Common 439, Consumer 460, Core 788, Producer 361, Protocol 1,524,
  and Remoting 337; these counts inventory names only.
- [x] The 41 public names declared in Core's controller module now have a
  Rust-to-Java DTO/type/field crosswalk, backed by the pinned
  `rocketmq-remoting-5.5.1-sources.jar` (SHA-256
  `ac2d2ad39cbe35e2125ce8cfdd2dd1c20ff7007f9960c1c4b597e292123a6bf6`).
- [x] The DTO behavior gaps found by this scan were closed in the later
  `P18-V01 Controller DTO behavior closure` entry below: the Rust counterparts,
  nullable/error ordering, Java equality/hash behavior, string methods, and
  absent-map failure are covered by the invoked fixture and Rust tests. This
  scan itself ran no behavioral tests; the later entry records that evidence.
- [ ] Broad P18-V01 remains PARTIAL. Native Windows runtime, current-revision
  remote Ubuntu CI, and Phase 19 Broker/Docker validation remain open. No plan
  files were changed and no remote CI or push was run. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261023T221410Z.rlog`.

## P18-V01 Fastjson repeated trailing EOF marker parity — 2026-10-24 (`053ea2d3`)

- [x] Added two invoked Java 5.5.1 heartbeat DTO cases for repeated U+001A
  terminators, adjacent and separated by JSON whitespace. The Rust differential
  failed before the fix and passes with the expanded 1,088-case corpus after
  the normalizer correction.
- [x] Focused parser regression, fixture-backed differential, and offline
  Maven `verify` pass; Maven validates 37 scenarios and Java decodes 106 typed
  headers and 19 typed bodies. Fixture and run details are in
  `rlogs/2026/10/24-phase-18-fastjson-trailing-eof__20261024T000554Z.rlog`.
- [ ] This closes the repeated terminal-marker boundary only. Broad P18-V01
  remains open.

## P18-V01 current-tree Rustdoc export reconciliation — 2026-10-24 (`053ea2d3`)

- [x] Current all-feature Rustdoc JSON counts distinct public names excluding
  the crate-root marker: Common 439, Consumer 460, Core 798, Producer 361,
  Protocol 1,524, and Remoting 337. Including Core's generated crate root
  yields 799, reconciling the differing Core number.
- [x] Against the prior 747-name Core inventory at `e7d779af`, the current
  Core surface has 51 added names and no removals. All additions are controller
  API names present in the compatibility crosswalk. This is name inventory
  evidence, not behavioral closure. Run record:
  `rlogs/2026/10/24-phase-18-rustdoc-export-reconcile__20261024T000638Z.rlog`.
- [ ] Broad P18-V01 behavior/settings/input/error/control-flow and reverse
  export reconciliation remains open. Native Windows runtime and current
  remote Ubuntu CI remain pending; Phase 19 Broker/Docker evidence is separate.

## P18-V02 current-tree server-free gate — 2026-10-24 (`23ef8749`)

- [x] Strict workspace all-target/all-feature Clippy, formatter, default
  workspace tests, and all-feature workspace tests/doctests passed. The two
  workspace test configurations were run sequentially.
- [x] Offline Maven `verify` passed with 37 scenarios, fixture-drift checks,
  11 message fixtures, and Java cross-decodes for 106 typed headers and 19
  typed bodies. Differential self-test and 37-scenario manifest validation
  passed.
- [x] Deterministic race/leak smoke passed 20 cases × 3 repetitions. Bounded
  fuzz smoke passed 163 frame seeds, 11 message seeds, and 2,784 mutations
  (245 frame decodes, 192 message decodes).
- [x] Workspace metadata and all seven package-content listings passed with
  `LICENSE` and `NOTICE`. Facade release examples built; the offline benchmark
  ran and is recorded in
  `compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`.
- [x] Windows GNU all-target/all-feature workspace check and strict Clippy
  passed as compile-only evidence.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and
  reverse-export reconciliation remains open. Native Windows runtime and
  current-revision remote Ubuntu CI remain unverified; Phase 19 Broker/Docker
  validation remains separate. No push or remote CI was run. Run record:
  `rlogs/2026/10/24-phase-18-current-tree-server-free-gate__20261024T001852Z.rlog`.

## P18-V01 Fastjson numeric suffix and string coercion — 2026-10-24 (`8b375eac`)

- [x] Fourteen invoked Java 5.5.1 heartbeat DTO cases cover uppercase `L`,
  `F`, `D`, `B`, and `S` suffixes, exponent and decimal-point forms,
  lowercase `f` rejection, and numeric suffix conversion to `SubscriptionData.topic`.
  Rust now matches the tested integer results, Float rounding, and Float/Double
  exponent strings. The expanded 1,102-case heartbeat differential and focused
  formatter tests pass. Fixture SHA-256:
  `69ab7ea24c30ad628e6f2b519268edeebee9060d7013b6becdfad88adc658f49`.
- [x] Committed-revision server-free gates pass: strict workspace Clippy,
  sequential default/all-feature workspace tests and doctests, Maven verify
  (37 scenarios, 11 message fixtures, 106 typed headers, 19 typed bodies),
  comparator self-test, 37-scenario manifest, metadata and seven package
  license listings, stress smoke (20 cases x 3), fuzz smoke (163 frame seeds,
  11 message seeds, 2,784 mutations), release examples, offline benchmark,
  Windows GNU check/Clippy, and the Asia/Taipei Unique ID fixture.
- [ ] This closes the tested Fastjson token and string-field cases only. Broad
  P18-V01 reconciliation remains PARTIAL; native Windows runtime and current-
  revision remote Ubuntu CI remain unverified. Phase 19 Broker/Docker
  validation remains separate. No push or remote CI was run. Run record:
  `rlogs/2026/10/24-phase-18-fastjson-numeric-suffix__20261024T010523Z.rlog`.

## P18-V02 committed-tree server-free gate — 2026-10-24 (`8b375eac`)

- [x] Strict workspace all-target/all-feature Clippy, formatter, default and
  all-feature workspace tests/doctests passed. The test configurations ran
  sequentially. Pinned offline Maven verification passed with 37 scenarios,
  fixture drift checks, 11 message fixtures, and Java cross-decodes for 106
  typed headers and 19 typed bodies.
- [x] Differential self-test and 37-scenario manifest passed. Workspace
  metadata and all seven package listings include `LICENSE` and `NOTICE`.
  Stress smoke passed 20 cases x 3; bounded fuzz smoke passed 163 frame seeds,
  11 message seeds, and 2,784 mutations (245 frame decodes, 192 message
  decodes). Facade release examples, the offline benchmark, and Windows GNU
  all-target/all-feature check and strict Clippy passed. The Asia/Taipei Unique
  ID fixture passed.
- [ ] P18-V01 remains PARTIAL, native Windows runtime and current-revision
  remote Ubuntu CI remain unverified, and Phase 19 Broker/Docker evidence is
  separate. No push or remote CI was run. Run record:
  `rlogs/2026/10/24-phase-18-fastjson-numeric-suffix__20261024T010523Z.rlog`.

## P18-V01 Fastjson U+001A end-of-input semantics — 2026-10-24 (`12de2474`)

- [x] Added 110 invoked Java heartbeat DTO cases: 108 cover each of 54
  control/Unicode separators after or between U+001A markers; two verify that
  ordinary trailing text and a second JSON object are ignored after the marker.
  The previous Rust differential failed on all 108 separator cases. Rust now
  truncates at the first raw marker after string normalization. The expanded
  DTO corpus has 1,221 cases; fixture SHA-256 is
  `3841442edc031bc70a028f0c2476f8b77f9ead0ec2eb959ace5a1ab2bad3f9c6`.
- [x] Focused parser regression, 1,221-case Java/Rust heartbeat DTO
  differential, default workspace tests/doctests, strict all-target/all-feature
  Clippy, formatting, diff checks, and offline Maven `verify` pass. Maven
  verified 37 scenarios, fixture drift, 11 message fixtures, 106 typed headers,
  and 19 typed bodies.
- [ ] The all-features workspace run passed the Protocol test suite and several
  Remoting integration targets, then stalled while launching
  `rocketmq-classic-remoting --test codec`: the test executable
  remained in macOS `dyld` for over two minutes with no loaded images or test
  threads. Launching that binary with `--list` stalled the same way. This leaves
  the current-revision all-features workspace gate unverified; it is recorded
  without classifying the loader stall as a code-test failure.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and
  reverse-export reconciliation remains open. Native Windows runtime and
  current-revision remote Ubuntu CI remain unverified; Phase 19 Broker/Docker
  validation is separate. No push or remote CI was run. The four protected
  Phase 19 worktree items remain unchanged and unstaged. Run record:
  `rlogs/2026/10/24-phase-18-fastjson-eof-termination__20261024T020248Z.rlog`.

## P18-V01 ConsumerRunningInfo Fastjson raw decoder — 2026-10-24 (`9180556a`)

- [x] The pinned Java `RemotingSerializable.decode` fixture covers four raw
  body inputs: text after U+001A, a second JSON object after U+001A, U+001A
  inside a string, and single quotes with a trailing comma. Rust now runs the
  shared Fastjson normalizer before converting object-valued `MessageQueue`
  map keys. The populated Java body also round-trips through the typed decoder.
  Fixture SHA-256:
  `495f040623c010c13a4d70f004db4d0529bd8fcda93530671015c69cb49f1e06`.
- [x] The focused Rust differential and full locked workspace tests/doctests
  passed. Strict all-target/all-feature Clippy, formatting, diff checks, and
  pinned offline Maven `verify` passed; Maven verified 37 scenarios, fixture
  drift, 11 message fixtures, 106 typed headers, and 19 typed bodies.
- [ ] The current revision's all-features workspace test was not rerun. The
  preceding attempt stalled at macOS `dyld` while launching Remoting `codec`;
  this gate remains unverified. Broad P18-V01 remains PARTIAL, native Windows
  runtime and current-revision remote Ubuntu CI remain unverified, and Phase 19
  Broker/Docker evidence remains separate. No push or remote CI was run. The
  four protected Phase 19 worktree paths remain unchanged and unstaged. Run
  record: `rlogs/2026/10/24-phase-18-fastjson-eof-termination__20261024T020248Z.rlog`.

## P18-V01 Typed RPC response Fastjson decoders — 2026-10-24 (`9a13acb7`)

- [x] Nine Core RPC response variants now call their DTO Java Fastjson
  decoders explicitly. The three controller body paths use nullable Java
  Fastjson decoding, including JSON `null` roots. Pinned Java invoked inputs
  cover EOF trailing text and a second JSON document for seven RPC response
  DTOs (14 suffix cases) and three controller DTOs (six suffix cases). Runtime
  lookup fixture SHA-256:
  `85c4496ce6e9055da78b4f7624e6713115b90d8c4e7beea60a332c810b5e5bf2`.
- [x] Full locked Protocol and Core package tests/doctests passed. The
  controller API integration suite passed all nine tests. Strict
  all-target/all-feature workspace Clippy, formatting, diff checks, and pinned
  offline Maven `verify` passed; Maven verified 37 scenarios, fixture drift,
  11 message fixtures, 106 typed headers, and 19 typed bodies.
- [ ] The all-features workspace test gate remains unverified on this revision;
  a preceding run stalled during macOS `dyld` launch of the Remoting `codec`
  binary. Broad P18-V01 remains PARTIAL. Native Windows runtime and
  current-revision remote Ubuntu CI remain unverified; Phase 19 Broker/Docker
  evidence is separate. No push or remote CI was run. The four protected Phase
  19 worktree paths remain unchanged and unstaged. Run record:
  `rlogs/2026/10/24-phase-18-fastjson-eof-termination__20261024T020248Z.rlog`.

## P18-V01 Consumer-list response Fastjson decoder — 2026-10-24 (`99f12b98`)

- [x] The three Core consumer-list response variants now use the DTO's Java
  Fastjson decoder. The invoked matrix covers 18 raw inputs plus seven root
  shapes (25 total), including text and a second JSON document after U+001A.
  Both sync and future Core loopback calls accept the Java EOF suffix cases.
  Runtime lookup fixture SHA-256:
  `b3b2efa341e83f40d9b18f345ee175796d8891d84208b2f396f6cf8da162ee33`.
- [x] Focused Protocol differential and Core loopback tests passed. Full locked
  Protocol and Core package tests/doctests passed. Strict all-target/all-feature
  workspace Clippy, formatting, diff checks, and pinned offline Maven `verify`
  passed; Maven verified 37 scenarios, fixture drift, 11 message fixtures, 106
  typed headers, and 19 typed bodies.
- [ ] The current revision's all-features workspace test was not rerun. The
  preceding attempt stalled at macOS `dyld` while launching Remoting `codec`,
  so that gate remains unverified. Broad P18-V01 remains PARTIAL; native
  Windows runtime and current-revision remote Ubuntu CI remain unverified, and
  Phase 19 Broker/Docker evidence remains separate. No push or remote CI was
  run. The four protected Phase 19 worktree paths remain unchanged and
  unstaged. Run record:
  `rlogs/2026/10/24-phase-18-fastjson-eof-termination__20261024T020248Z.rlog`.

## P18-V01 `getUser` reply Fastjson decoder — 2026-10-24 (`41854b80`)

- [x] Core `get_user` now uses the nullable Java Fastjson decoder on the reply
  body. The invoked Java fixture adds trailing-text and second-document cases
  after raw U+001A; both decode to the same `UserInfo`. The fixture preserves
  absent-body and JSON-null behavior. Producer core fixture SHA-256:
  `ac4d435472279c6cdfefadf2811422359bc927223733c3521484c1c533d31f36`.
- [x] Focused Core `get_user` differential passed all ten cases. Strict
  all-target/all-feature workspace Clippy, formatting, diff checks, and pinned
  offline Maven `verify` passed. Maven verified 37 scenarios, fixture drift,
  11 message fixtures, 106 typed headers, and 19 typed bodies.
- [ ] Full Core package tests and the all-features workspace test gate were not
  rerun on this revision. Broad P18-V01 remains PARTIAL. Native Windows runtime
  and current-revision remote Ubuntu CI remain unverified. No push or remote
  CI was run; the four protected Phase 19 worktree paths remain unchanged and
  unstaged. Run record:
  `rlogs/2026/10/24-phase-18-fastjson-eof-termination__20261024T020248Z.rlog`.

## P18-V01 JSON remoting header Fastjson input — 2026-10-24 (`d897a9e2`)

- [x] `json_header::decode` now uses the shared Fastjson-compatible decoder.
  Pinned Java `RemotingCommand.decode` fixture cases cover trailing text and a
  second JSON document after raw U+001A, plus single quotes with a trailing
  comma. Fixture SHA-256:
  `f0b5433e0c7c9f4566dee85048196860c47b948e4204cb08aa09b062bb628c0c`.
- [x] Remoting unit tests passed 143/143 and codec integration tests passed
  9/9. Maven `verify` passed with fixture drift, 37 scenarios, 11 message
  fixtures, 106 typed headers, and 19 typed bodies. Direct rustfmt checks and
  `git diff --check` passed.
- [ ] The full Remoting package run stopped at the existing `fuzz_regressions`
  binary, which remained in macOS `dyld` startup; that regression test is
  unverified. Strict all-target/all-feature Clippy is unverified on this
  revision because rustc stalled enumerating its dependency search directory.
  Broad P18-V01 remains PARTIAL; native Windows runtime and current-revision
  remote Ubuntu CI remain unverified. No push or remote CI was run. The four
  protected Phase 19 worktree paths remain unchanged and unstaged. Run record:
  `rlogs/2026/10/24-phase-18-fastjson-eof-termination__20261024T020248Z.rlog`.

## P18-V02 current-tree workspace gate continuation — 2026-10-24 (`d897a9e2`)

- [x] The bounded `fuzz_regressions` test passed when the same binary was
  copied to `/tmp`, confirming the earlier launch stall was tied to its
  repository target path. Clean-target locked default and all-feature
  workspace tests both passed, including all package tests and doctests.
  Strict all-target/all-feature workspace Clippy and `cargo fmt --all --
  --check` passed.
- [x] The differential comparator and manifest self-test passed. Facade
  release examples built. Pinned offline Maven `verify` on this source revision
  passed fixture-drift validation, all 37 scenarios, 11 message fixtures,
  106 typed headers, and 19 typed bodies. `response-future-api.json` remains
  SHA-256 `f0b5433e0c7c9f4566dee85048196860c47b948e4204cb08aa09b062bb628c0c`.
- [ ] Broad P18-V01 source/API/settings/input/error/control-flow and
  reverse-export reconciliation remains PARTIAL. Native Windows runtime and
  current-revision remote Ubuntu CI are unverified; Phase 19 Broker/Docker
  evidence remains separate. No push or remote CI was run. The four protected
  Phase 19 worktree paths remain unchanged and unstaged. Run record:
  `rlogs/2026/10/24-phase-18-current-tree-clean-target-gate__20261024T072654Z.rlog`.

## P18-V01 Remoting outbound write-flow parity — 2026-10-24 (`719e5aa3`)

- [x] Pinned Java 5.5.1 `EmbeddedChannel` fixture verifies sync RPC returns
  from a received response while the local request write is pending, and
  one-way returns before write completion while holding admission until the
  write completes. Rust now reads inbound frames during partial outbound
  progress, does not cancel an enqueued client write at the RPC deadline, and
  holds one-way admission through local write completion/failure. Runtime
  fixture SHA-256:
  `6818a879b34ecab39936c1636c659ca3157e2d52fbd4f9e50f5bcadc49a9e09d`.
- [x] Default/all-feature locked workspace tests and doctests, strict
  all-target/all-feature Clippy, release examples, formatting, diff checks,
  differential comparator self-test, and pinned offline Maven `verify` passed.
  Maven verified 36 runtime hook cases, 37 scenarios, 11 message fixtures, 106
  typed headers, and 19 typed bodies. The initial cancellation-unsafe
  `write_all` experiment exposed the existing backlog regression; the final
  offset-preserving partial-write loop passes that regression and both new
  Java-fixture-backed tests.
- [ ] Broad P18-V01 bidirectional API/settings/input/error/control-flow and
  reverse-export audit remains PARTIAL. Native Windows runtime and
  current-revision remote Ubuntu CI remain unverified; Phase 19 Broker/Docker
  validation is separate. No push or remote CI was run. Protected Phase 19
  worktree items remain untouched and unstaged. Run record:
  `rlogs/2026/10/24-phase-18-remoting-write-flow-parity__20261024T075744Z.rlog`.


## P18-V02 current-tree supplemental local gates — 2026-10-24 (`719e5aa3`)

- [x] Current code revision `719e5aa3`: default/all-feature locked workspace
  tests/doctests, strict all-target/all-feature Clippy, release examples,
  benchmark Clippy, comparator self-test, 37-scenario manifest validation,
  formatting and diff checks passed. Pinned Maven `verify` confirmed fixture
  drift, 37 scenarios, 11 message fixtures, 106 typed headers and 19 typed
  bodies.
- [x] Deterministic race/leak smoke passed 20 cases × 3. Bounded fuzz smoke
  passed 163 frame seeds, 11 message seeds, 2,784 mutations, 564 frame decodes
  and 192 message decodes. Offline benchmark ran; workspace metadata/dependency
  checks and `LICENSE`/`NOTICE` listings passed for all seven product crates.
  Windows GNU all-target/all-feature check and strict Clippy passed as
  compile-only evidence. Benchmark record is in
  `compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`.
- [ ] P18-V01 bidirectional API/settings/input/error/control-flow and
  reverse-export audit remains PARTIAL. Native Windows runtime and
  current-revision remote Ubuntu CI remain unverified; Phase 19 Broker/Docker
  validation is separate. No push or remote CI was run. The integration
  workspace contains protected Phase 19 edits, so package listing was scoped
  to the seven product crates and those edits remain untouched. Run record:
  `rlogs/2026/10/24-phase-18-current-tree-final-server-free-gate__20261024T080301Z.rlog`.

## P18-V01 `MessageDecoder` cursor parity — 2026-10-24 (`b2326a3a`)

- [x] Rust now exposes the two buffer-sensitive helpers through mutable and
  positioned `Cursor` inputs and records Java success and failure positions in
  the invoked message fixture. This corrects the earlier immutable-slice API
  mapping. The focused Common fixture, default/all-feature workspace tests and
  doctests, strict workspace Clippy, release facade examples, Windows GNU
  compile-only check/Clippy, differential self-test/manifest, format/diff
  checks, and pinned offline Maven verification all pass; the fixture
  contains 11 message scenarios.
- [ ] Zero and negative count sizes retain bounded Rust errors where Java can
  loop or rewind, as recorded in `KNOWN_DIFFERENCES.md`. This closes these
  cursor items only; broad P18-V01 remains open. Native Windows runtime,
  current-revision remote Ubuntu CI, and Phase 19 Broker/Docker evidence remain
  unverified or separate. Code commit: `b2326a3a`; run record:
  `rlogs/2026/10/24-phase-18-message-decoder-buffer-cursors__20261024T083406Z.rlog`.

## P18-V01 finite inner-message rewind — 2026-10-24 (`924466d6`)

- [x] `count_inner_messages` now follows Java's finite in-range negative-size
  rewind. Its invoked fixture compares the 8 → 5 rewind and later failure at
  cursor 9. Rust also detects repeated offsets and returns a typed error
  instead of looping indefinitely; zero-size input remains guarded. Common
  tests/doctests in default and all-feature modes, full workspace all-feature
  tests/doctests, strict workspace Clippy, release examples, Windows GNU
  compile-only check/Clippy, Maven verify, comparator/manifest, format and diff
  checks pass. Code commit: `924466d6`; run record:
  `rlogs/2026/10/24-phase-18-count-inner-negative-rewind__20261024T085047Z.rlog`.
- [ ] Zero-size and cyclic count inputs remain intentional bounded Rust
  outcomes; broad P18-V01, native Windows runtime, current-revision remote CI,
  and Phase 19 Broker/Docker evidence remain open or separate.

## P18-V02 current-tree server-free gate after message cursor changes — 2026-10-24 (`924466d6`)

- [x] Current code/test revision `924466d6`: locked default workspace tests
  and doctests passed. Deterministic race/leak smoke passed 20 cases × 3;
  bounded fuzz smoke passed 163 frame seeds, 11 message seeds, 2,784
  mutations, 564 frame decodes and 192 message decodes. The release offline
  benchmark ran and is recorded in
  `compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`.
- [x] All-feature workspace tests/doctests, strict workspace Clippy, release
  examples, Windows GNU all-target/all-feature compile-only checks, pinned
  offline Maven `verify`, comparator/manifest validation, formatting and diff
  checks passed on the same code revision in the earlier decoder verification
  run. The latest stress, fuzz, default workspace and benchmark commands also
  passed. Protected Phase 19 worktree paths remain unchanged and unstaged.
- [ ] Broad P18-V01 API/settings/input/error/control-flow and reverse-export
  reconciliation remains PARTIAL. Native Windows runtime and current-revision
  remote Ubuntu CI remain unverified; Phase 19 Broker/Docker evidence remains
  separate. No push or remote CI was run. Run record:
  `rlogs/2026/10/24-phase-18-current-tree-supplemental-gates__20261024T090226Z.rlog`.

## P18-V01 zero-size and cyclic inner-count flow parity — 2026-10-24 (`0cfb20ad`)

- [x] Rust now follows Java `countInnerMsgNum` for zero-size and finite
  in-range negative-cycle inputs, which continue looping. The pinned Java
  fixture observes daemon-thread liveness; the Rust regression probes each
  behavior in a child process and terminates it after the observation window.
  Finite rewind and out-of-range failure cases remain covered by the message
  fixture. Fixture hashes and implementation details are in the Phase 6 audit.
- [x] Focused message tests, full default/all-feature workspace tests and
  doctests, strict workspace Clippy, release examples, Windows GNU compile-only
  checks/Clippy, 20 × 3 stress smoke, fixed-seed fuzz smoke, Maven `verify`,
  comparator/manifest, formatting/diff checks and the offline benchmark passed.
- [ ] This closes the recorded message-count cursor discrepancy only. Broad
  P18-V01 remains PARTIAL; native Windows runtime and current-revision remote
  Ubuntu CI remain unverified. Phase 19 Broker/Docker validation remains
  separate. No push or remote CI was run, and the protected Phase 19 worktree
  paths remain untouched. Run record:
  `rlogs/2026/10/24-phase-18-malformed-inner-count-parity__20261024T093158Z.rlog`.

## P18-V01 GO_AWAY explicit-close generation parity — 2026-10-24 (`dbbed745`)

- [x] Pinned Java 5.5.1 `NettyRemotingClient.closeChannels(List)` looks up each
  `ChannelWrapper` and closes its current channel only. An older GO_AWAY
  generation remains open as `channelToClose`; Rust now preserves that
  generation and its pending requests through explicit address close. Shutdown
  still closes the retained generation. The focused scripted-peer regression
  passed and verifies an old request can finish after `close_channels`, then
  verifies shutdown closes its socket.
- [x] The post-commit Remoting package run passed 145 unit, 9 codec, 1 fuzz,
  7 security, 109 transport tests and 20 doctests. Strict all-target/all-feature
  Remoting Clippy passed. Formatting and `git diff --check` passed. Code
  commit: `dbbed745`; pinned Java source commit:
  `efa1be59d6fc747ad220f036c86030f05f0419f9`. This checklist and run record
  are in the separate documentation commit.
- [ ] This closes the explicit-close GO_AWAY lifecycle discrepancy only. Broad
  P18-V01 remains PARTIAL. Native Windows runtime and current-revision remote
  Ubuntu CI remain unverified; Phase 19 Broker/Docker evidence remains
  separate. No push or remote CI was run. Run record:
  `rlogs/2026/10/24-phase-18-go-away-explicit-close-parity__20261024T093914Z.rlog`.

## P18-V02 current-tree server-free gate after GO_AWAY close parity — 2026-10-24 (`dbbed745`)

- [x] Default and all-feature locked workspace tests/doctests passed. Strict
  all-target/all-feature workspace Clippy, `cargo fmt --all -- --check`,
  `git diff --check`, differential comparator self-test and 37-scenario
  manifest validation passed. Pinned offline Maven `verify` passed with 37
  scenarios, 11 message fixtures, 106 typed headers, 19 typed bodies and Java
  cross-decodes.
- [x] Deterministic race/leak smoke passed 20 cases × 3; bounded fuzz smoke
  passed 163 frame seeds, 11 message seeds, 2,784 mutations, 564 frame decodes
  and 192 message decodes. Release facade examples built. Windows GNU
  all-target/all-feature workspace check and strict Clippy passed as
  compile-only evidence. Workspace metadata and dependency-direction checks
  passed. The offline benchmark sample is recorded in
  `compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`.
- [ ] Broad P18-V01 API/settings/input/error/control-flow and reverse-export
  reconciliation remains PARTIAL. Native Windows runtime and current-revision
  remote Ubuntu CI remain unverified; Phase 19 Broker/Docker evidence remains
  separate. No push or remote CI was run. The four protected Phase 19 worktree
  paths remain untouched and unstaged. Run record:
  `rlogs/2026/10/24-phase-18-go-away-explicit-close-parity__20261024T093914Z.rlog`.

## P18-V01 Remoting `ErrorSource` identity equality — 2026-10-24 (`1e928372`)

- [x] `ErrorSource` now compares wrapped error identity, matching Java
  `Throwable.equals`; cloning an `ErrorSource` preserves that identity, while
  separate causes with the same message compare unequal. The transport error
  classification test now checks variant, address, and cause text without
  accidentally comparing distinct cause identities.
- [x] Remoting package tests passed: 146 unit, 9 codec, 1 fuzz-regression, 7
  security, 109 transport tests, and 20 doctests. Strict all-target/all-feature
  Remoting Clippy, formatting, and `git diff --check` passed. Code commit:
  `1e928372`; run record:
  `rlogs/2026/10/24-phase-18-remoting-error-identity__20261024T100726Z.rlog`.
- [ ] This closes the `ErrorSource` equality mismatch only. Broad P18-V01
  remains PARTIAL. The workspace-wide P18-V02 gates must be refreshed for this
  code revision; native Windows runtime and current-revision remote Ubuntu CI
  remain unverified, and Phase 19 Broker/Docker evidence remains separate. No
  push or remote CI was run.

## P18-V02 current-tree server-free gates after ErrorSource identity parity — 2026-10-24 (`1e928372`)

- [x] On code/test revision `1e928372`, locked offline default and all-feature
  workspace tests/doctests passed: 93 test suites, 2,019 passed and none
  failed in each configuration. Strict all-target/all-feature workspace
  Clippy, formatting, and `git diff --check` passed.
- [x] Differential comparator self-test and the 37-scenario manifest passed.
  Pinned offline Maven `verify` passed with 37 scenarios, 11 message fixtures,
  106 typed headers, 19 typed bodies, fixture-drift checks, and Java/Rust
  cross-decodes. Workspace metadata, dependency direction, package licenses,
  and all seven product package listings passed; each includes `LICENSE` and
  `NOTICE`.
- [x] Deterministic race/leak smoke passed 20 cases × 3; bounded fuzz smoke
  passed 163 frame seeds, 11 message seeds, 2,784 mutations, 564 frame decodes,
  and 192 message decodes. Release facade examples built. Windows GNU
  all-target/all-feature workspace check and strict Clippy passed as
  compile-only evidence. The offline benchmark sample is recorded in
  `compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`.
- [ ] Broad P18-V01 API/settings/input/error/control-flow and reverse-export
  reconciliation remains PARTIAL. Native Windows runtime and current-revision
  remote Ubuntu CI remain unverified; Phase 19 Broker/Docker evidence remains
  separate. No push or remote CI was run. The four protected Phase 19 worktree
  paths remain untouched and unstaged. Run record:
  `rlogs/2026/10/24-phase-18-current-tree-error-identity-gates__20261024T102143Z.rlog`.

## P18-V01 Remoting and Phase 6 inventory review — 2026-10-24 (`1e928372`)

- [x] Rechecked the Phase 3 Remoting surface after aligning `ErrorSource`
  identity. No `NOT MAPPED` row remains in that inventory. At this review
  revision the Java `ChannelWrapper` `tryLock` race still limited exact GO_AWAY
  interleaving coverage; the follow-up `P18-V01 Remoting GO_AWAY tryLock
  parity` entry below records the fix and regression coverage at `3b7cb5d2`.
- [x] Removed the stale Known Differences sentence claiming no Rust client
  implementation existed at Phase 0. No production code changed and no tests
  were run for this documentation-only correction; `git diff --check` passed.
- [ ] Phase 6 still explicitly calls for the complete Java method/overload,
  input/default/error/control-flow, and reverse-export audit across its DTO,
  route, heartbeat, enum, configuration, and subscription surfaces. The
  documented GO_AWAY contention slice is closed by the follow-up below, while
  broad P18-V01 remains PARTIAL.
  Run record:
  `rlogs/2026/10/24-phase-18-cross-inventory-review__20261024T102515Z.rlog`.

## P18-V03 documentation, provenance, and package review — 2026-10-24 (`1e928372`)

- [x] Reviewed the Java reference README and workspace metadata/provenance;
  the oracle remains pinned to client 5.5.1/source commit
  `efa1be59d6fc747ad220f036c86030f05f0419f9`, with Broker 5.3.4 reserved for
  Phase 19. The README continues to make no full-compatibility or release
  readiness claim and now points to the 2026-10-24 P18-V02 refresh.
- [x] `scripts/check-workspace-metadata.py`, offline Cargo metadata, and all
  seven locked product `cargo package --list` checks passed. Every product
  listing contains `LICENSE` and `NOTICE`; workspace dependency direction and
  unpublished-package metadata passed. Release facade examples built.
- [x] This closes P18-V03 documentation/package review on code revision
  `1e928372`. P18-V01 and P18-V04 remain open. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261024T102707Z.rlog`.

## P18-V01 Remoting GO_AWAY tryLock parity — 2026-10-24 (`3b7cb5d2`)

- [x] Rust now models the Java `ChannelWrapper.reconnect()` contention path
  with weakly cached per-address wrapper locks. Current-channel lookup uses a
  read lock; GO_AWAY reconnect tries the write lock and checks the current
  generation after contention. When the old generation remains current, it
  returns the GO_AWAY error without retiring the connection. An already
  replaced or previously retired generation retries through the shared
  replacement. GO_AWAY received after socket-close cleanup still retries when
  no current connection entry remains.
- [x] `go_away_try_lock_skips_current_channel_and_retries_replaced_channel`
  covers the same-generation contention and replacement paths. Remoting
  all-feature tests passed (147 unit, 9 codec, 1 fuzz-regression, 7 security,
  109 transport tests, and 20 doctests); strict all-target/all-feature
  Remoting Clippy passed.
- [x] On code revision `3b7cb5d2`, default and all-feature locked workspace
  tests/doctests, strict all-target/all-feature workspace Clippy, formatting,
  and diff checks passed. The Asia/Taipei Unique ID test, differential
  self-test, 37-scenario manifest, 20-case × 3 race/leak smoke, bounded fuzz
  smoke (163 frame seeds, 11 message seeds, 2,784 mutations, 564 frame
  decodes, 192 message decodes), offline Maven `verify` (37 scenarios, 11
  message fixtures, 106 typed headers, 19 typed bodies), release examples,
  benchmark compilation, metadata and seven package listings with `LICENSE`/
  `NOTICE` also passed. Windows GNU all-target/all-feature check and strict
  Clippy passed as compile-only evidence.
- [ ] This closes the recorded GO_AWAY contention discrepancy only. Broad
  P18-V01 Phase 6 method/overload/input/error/control-flow and reverse-export
  reconciliation remains PARTIAL. Native Windows runtime and current-revision
  remote Ubuntu CI remain unverified; no push or remote CI was run. Phase 19
  Broker/Docker evidence remains separate. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261024T102707Z.rlog`.

## P18-V01 scoped Rustdoc name-crosswalk review — 2026-10-24

- [x] Corrected the historical 2026-10-13 Rustdoc candidate counts in the
  Phase 6 audit: the later Producer/Consumer crosswalk comparison found zero
  unmatched exact method identifiers. The current Core, Producer, Consumer,
  Common, Protocol, Remoting, and facade inventories account for the scoped
  export names; the earlier 137/157 candidates were mapping-format false
  positives.
- [ ] This closes exact-name inventory only. Method behavior, overload/input,
  error/control-flow semantics, and Java-side omission review remain P18-V01
  work. Run record:
  `rlogs/2026/09/19-phase-18-compatibility-release-gate__20261024T102707Z.rlog`.

## P18-V02 post-assignment-accessor current-tree refresh — 2026-10-24 (`88061e7a`)

- [x] Locked default and all-feature workspace tests/doctests pass on code/test
  revision `88061e7a`. Strict workspace all-target/all-feature Clippy, formatting,
  diff checks, differential comparator self-test, and 37-scenario manifest
  validation pass.
- [x] Pinned offline Maven `verify` passes on the same tree with 37 scenarios,
  11 message fixtures, 106 typed headers, 19 typed bodies, fixture-drift checks,
  and Java/Rust cross-decodes.
- [x] Windows GNU all-feature workspace check, strict all-target/all-feature
  Clippy, and `cargo test --no-run` pass as compile/link-only evidence. No Windows
  runtime is available in this environment.
- [ ] Broad P18-V01 method/overload/input/error/control-flow and reverse-export
  reconciliation remains PARTIAL. The Java `MessageQueueAssignment` equal-hash
  fixture depends on the JVM's process-local enum identity hash and has no stable
  cross-process bucket outcome. Native Windows runtime and current-revision remote
  Ubuntu CI remain unverified; no push or remote CI was run. The four protected
  Phase 19 paths remain unchanged and unstaged. Run record:
  `rlogs/2026/10/24-phase-18-post-assignment-accessor-gates__20261024T135900Z.rlog`.

## P18-V01 `MessageQueueAssignment.equals(Object)` type input — 2026-10-24 (`455110fc`)

- [x] `MessageQueueAssignment::java_equals` now accepts Java-object-shaped input
  via `Any`, returns false for a non-assignment value before queue dereference,
  and preserves Java's null, same-instance, and null-queue behavior. The invoked
  Java fixture's wrong-type result is now compared directly in the Rust test.
- [x] The focused assignment API fixture test and current strict workspace
  all-target/all-feature Clippy, formatting, and diff checks pass. Default and
  all-feature workspace tests passed after the adapter change; the focused test
  was rerun after it was tightened to consume the fixture value. Windows GNU
  workspace check, strict Clippy, and all-target `cargo test --no-run` passed
  after the adapter change as compile/link-only evidence.
- [ ] This closes the wrong-type `equals(Object)` input branch only. The
  process-local assignment HashSet collision behavior, broad P18-V01
  API/overload/input/error/control-flow and reverse-export reconciliation,
  native Windows runtime, and current-revision remote Ubuntu CI remain open.
  No push or remote CI was run; protected Phase 19 paths remain untouched. Run
  record:
  `rlogs/2026/10/24-phase-18-assignment-equals-object-inputs__20261024T141418Z.rlog`.

## P18-V01 `ReplicaIdentity.equals(Object)` type input — 2026-10-24 (`b5cbf46b`)

- [x] `ReplicaIdentity::java_equals` now models Java's `equals(Object)` type
  check and returns false for unrelated values. The invoked Java fixture now
  records that branch and the Rust API test compares its result directly.
- [x] Focused Controller API tests, default and all-feature workspace tests and
  doctests, strict all-target/all-feature Clippy, formatting, and diff checks
  pass. Offline Maven verification passes with the regenerated fixture.
- [x] Current-revision Windows GNU workspace check, strict all-target/all-feature
  Clippy, and all-feature `cargo test --no-run` pass as compile/link-only
  evidence. Native Windows execution is unavailable.
- [ ] This closes one Controller equality input branch only. Broad P18-V01
  method/overload/input/error/control-flow and reverse-export reconciliation,
  the process-local `MessageQueueAssignment` hash-set boundary, native Windows
  runtime, and current-revision remote Ubuntu CI remain open. No push or remote
  CI was run; protected Phase 19 paths remain unchanged and unstaged. Run record:
  `rlogs/2026/10/24-phase-18-replica-identity-equals-object__20261024T143249Z.rlog`.

## P18-V01 assignment stable HashSet bucket parity — 2026-10-24 (`cc49b573`)

- [x] Protocol JSON decoding now matches stable same-mode Java `HashSet`
  collisions by queue hash, mode value, and attachment-map hash. The fixture
  covers the `Aa`/`BB` attachment-key collision, a different-hash control, and
  Java's null-queue `NullPointerException`. The DTO retains lawful all-fields
  Rust `Eq`/`Hash`.
- [x] Default and all-feature locked workspace tests/doctests, strict host
  all-target/all-feature Clippy, and offline Maven verification pass. The Java
  reference verification covers 37 scenarios, 40 assignment response input
  cases, 106 typed headers, and 19 typed bodies.
- [x] Windows GNU all-target/all-feature check, strict Clippy, and all-feature
  `cargo test --no-run` pass as compile/link-only evidence.
- [ ] Different-mode hash collisions remain JVM-local because enum identity
  hashes are process-local. Broad P18-V01 method/overload/input/error/control-
  flow and reverse-export reconciliation remains PARTIAL. Native Windows
  runtime and current-revision remote Ubuntu CI remain unverified; no push or
  remote CI was run. Phase 19 user-owned paths remain unchanged and unstaged.
  Run record:
  `rlogs/2026/10/24-phase-18-assignment-stable-hash-buckets__20261024T151417Z.rlog`.

## P18-V01 public `LatencyFaultToleranceImpl.FaultItem` API — 2026-10-24 (`62f67cbb`)

- [x] Producer exports the public nested Java value as `FaultItem` and maps
  all 15 public methods, including the erased `Comparable.compareTo(Object)`
  bridge. The invoked Java fixture compares constructor/method signatures,
  duration arithmetic and overflow, availability and signed comparison,
  equality/hash fields, null and wrong-type exceptions, Unicode hashing, and
  text. The facade re-export is covered.
- [x] Focused Producer/facade tests, default and all-feature locked workspace
  tests/doctests, strict workspace all-target/all-feature Clippy, release
  examples, differential self-test, 37-scenario manifest validation,
  20-case × 3 offline stress smoke, bounded fuzz smoke, pinned offline Maven
  verification, Windows GNU workspace check/Clippy/test compile-link, format,
  and diff checks pass on `62f67cbb`.
- [ ] This closes the public `FaultItem` API row only. Broad P18-V01
  method/overload/input/error/control-flow and reverse-export reconciliation
  remains PARTIAL. Different-mode assignment hash collisions remain dependent
  on JVM-process-local enum identity hashes. Native Windows runtime and
  current-revision remote Ubuntu CI remain unverified; no push or remote CI
  was run. Phase 19 user-owned paths remain unchanged and unstaged. Run record:
  `rlogs/2026/10/24-phase-18-fault-item-public-api__20261024T153809Z.rlog`.

## P18-V01 `ResponseFuture.waitResponse` external interruption — 2026-10-24 (`ba2fb950`)

- [x] Rust exposes `ResponseWaitInterrupt` and `ResponseWaitError` and adds
  `ResponseFuture::wait_response_interruptible`. Signaling a blocked wait
  wakes it and returns `Interrupted`; the signal is consumed, matching Java's
  `InterruptedException` and cleared thread flag. The existing
  `wait_response` remains a noninterruptible convenience.
- [x] Focused Java-fixture and public-export tests, default/all-feature locked
  workspace tests/doctests, strict workspace Clippy, pinned offline Maven
  verification, Windows GNU check/Clippy/test compile-link, 20-case × 3 stress
  smoke, fuzz smoke, release examples, comparator/manifest validation,
  Rustdoc JSON, workspace/package metadata, seven package listings,
  formatting, and diff checks pass on `ba2fb950`.
- [ ] This closes the external-wait interruption row only. The JVM-specific
  `ResponseFuture.toString()` identity/rendering, broad P18-V01
  method/overload/input/error/control-flow and reverse-export audit, native
  Windows runtime, and current-revision remote Ubuntu CI remain open. Phase 19
  user-owned paths remain unchanged and unstaged. Run record:
  `rlogs/2026/10/24-phase-18-response-wait-interruption__20261024T155648Z.rlog`.

## P18-V02 full current-tree server-free gate after ResponseFuture rendering — 2026-10-24 (`38a7bd83`)

- [x] Locked default and all-feature workspace tests/doctests pass using the
  fresh Cargo target `/tmp/rocketmq-phase18-current-gates-20261024T172656Z`.
  Strict workspace all-target/all-feature Clippy and the focused
  `response_future_display_preserves_java_to_string_state_fields` test pass.
  The original workspace-target test launch stalled before its Remoting codec
  harness entered `main`; the clean-target full rerun completed without test
  failures.
- [x] Windows GNU all-feature workspace check and strict all-target/all-feature
  Clippy pass. Windows GNU all-feature `cargo test --no-run` compiles and links
  every workspace test harness; no Windows binary was executed.
- [x] Pinned offline Maven `verify` passes fixture-drift checks, 37 scenarios,
  11 message fixtures, 106 typed headers, 19 typed bodies, and Java/Rust
  cross-decodes. Differential comparator self-test and 37-scenario manifest
  validation pass.
- [x] Deterministic race/leak smoke passes 20 cases × 3. Fixed-seed fuzz passes
  163 frame seeds, 11 message seeds, 11 property seeds, 2,960 mutations, and
  564/192/176 frame/message/property decoder calls. Release facade examples,
  offline benchmark, and strict benchmark Clippy pass. Benchmark observations
  are appended in
  `docs/compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`.
- [x] Workspace metadata, package licenses/dependency direction, and offline
  package listings for all seven product crates pass; each listing contains
  `LICENSE` and `NOTICE`. Formatting and `git diff --check` pass.
- [ ] Broad P18-V01 method/overload/input/error/control-flow and reverse-export
  reconciliation remains PARTIAL. The cross-process Java enum identity-hash
  behavior for assignment HashSet collisions remains JVM-local. Native Windows
  runtime and current-revision remote Ubuntu CI remain unverified; no push or
  remote CI ran. Phase 19 user-owned paths remain unchanged and unstaged. Run
  record: `rlogs/2026/10/24-phase-18-current-tree-full-server-free-gate__20261024T174002Z.rlog`.

## P18-V01 `ProduceAccumulator(String)` public constructor and P18-V02 refresh — 2026-10-24 (`c497f376`)

- [x] Producer exports `ProduceAccumulator::new(Option<String>)`, and the
  facade re-exports the constructor-only helper. Java's public constructor
  accepts nullable `String` and declares no batching methods. The Java fixture
  SHA-256 is
  `e7756ff8a7da4bac09de942e5eebf3e4886f933fb5f8483e4c7c7a8f61e228ff`; the
  pinned Java class SHA-256 is
  `89f2fcb62e9654aed52a190aa040ce4fd80eb62779e607910fdccb328d08df1b`.
  Producer API and facade tests construct both a name and null. Code commit:
  `c497f376`.
- [x] Default/all-feature locked workspace tests and doctests, strict host
  all-target/all-feature Clippy, Windows GNU all-feature workspace check and
  strict Clippy, and Windows GNU all-feature test-harness compile/link pass.
  The Windows binaries were not run.
- [x] Pinned Maven `verify` passes fixture drift, 37 scenarios, 11 message
  fixtures, 106 typed headers, 19 typed bodies, and Java/Rust cross-decodes.
  Rustdoc JSON generation, differential self-test and 37-scenario manifest
  validation pass.
- [x] Deterministic race/leak smoke passes 20 cases × 3. Fixed-seed fuzz passes
  163 frame, 11 message, and 11 property seeds with 2,960 mutations and
  564/192/176 frame/message/property decoder calls. Release examples, benchmark,
  strict benchmark Clippy, workspace metadata, all seven product package
  listings with `LICENSE`/`NOTICE`, formatting and diff checks pass. Benchmark
  observations are in
  `docs/compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`.
- [ ] This closes the directly constructible helper's constructor row only.
  `MQClientManager.getOrCreateProduceAccumulator` shares accumulators by Java
  client ID; cross-Producer batching ownership remains in broad P18-V01 flow
  reconciliation. Different-mode assignment hash collisions still depend on
  JVM identity hashes. Native Windows runtime and current-revision remote
  Ubuntu CI remain unverified; Phase 19 paths remain untouched. Run record:
  `rlogs/2026/10/24-phase-18-produce-accumulator-constructor-api__20261024T175426Z.rlog`.

## P18-V01 client-ID-scoped Producer accumulator and P18-V02 current-tree gates — 2026-10-24 (`49fffa75`)

- [x] Rust matches Java `MQClientManager.getOrCreateProduceAccumulator`
  identity caching by `buildMQClientId`. Producers sharing a manager and client
  ID share live limits, queued batches and worker state. Pending entries retain
  the first Producer owner. Shutdown stops workers without clearing entries;
  a later start restarts workers and schedules their retained deadlines. The
  invoked `produceAccumulatorClientIdCache` fixture and
  `auto_batch_is_shared_by_client_id_and_keeps_first_producer_owner` cover
  cache identity, live settings and owner routing; a unit test covers
  stop-retain-restart deadlines. Code commit: `49fffa75`.
- [x] Default and all-feature locked workspace tests/doctests pass. Strict
  host all-target/all-feature Clippy passes. Windows GNU all-feature workspace
  check, strict Clippy, and test harness compile/link pass; Windows executables
  were not run. Pinned offline Maven `verify` passes fixture drift, 37
  scenarios, 11 message fixtures, 106 typed headers, 19 typed bodies, and
  Java/Rust cross-decodes.
- [x] Differential self-test and 37-scenario manifest validation pass.
  Deterministic race/leak smoke passes 20 cases × 3; fixed-seed fuzz passes
  163 frame seeds, 11 message seeds, 11 property seeds, 2,960 mutations and
  564/192/176 frame/message/property decoder calls. Rustdoc JSON generation,
  release examples, offline benchmark, strict benchmark Clippy, workspace
  metadata, and all seven offline package listings with `LICENSE` and `NOTICE`
  pass. Formatting and diff checks pass.
- [ ] The accumulator cache/ownership slice is closed; broad P18-V01
  method/overload/input/error/control-flow and reverse-export reconciliation
  remains `PARTIAL`. `ResponseFuture.toString()` JVM identity rendering and
  different-mode assignment hash collisions based on JVM process-local
  identity hashes remain known gaps. Native Windows runtime and
  current-revision remote Ubuntu CI remain unverified; P18-V04 remains open.
  No push, remote CI, Broker or Docker run was performed. The Phase 19
  user-owned worktree paths and Java checkout `aaaa.java` remain untouched.
  Run record:
  `rlogs/2026/10/24-phase-18-shared-accumulator-client-id__20261024T183639Z.rlog`.

## P18-V01 direct `MQClientAPIImpl` overload evidence — 2026-10-24 (`091eba55`)

- [x] The pinned 5.5.1 source scan's seven overloaded groups are crosswalked
  member by member in `PHASE_6_API_CONFIG_MAP.md`. Ten direct invocation
  fixtures cover the remaining overload members; all 29 `rpc-invoked` cases
  pass against the Rust request/outcome harness. Existing POP ACK fixtures
  cover both `batchAckMessageAsync` members.
- [x] Java provenance is pinned to source commit
  `efa1be59d6fc747ad220f036c86030f05f0419f9` and the 5.5.1 Maven artifact;
  the external checkout's 5.3.3 worktree and pre-existing untracked file remain
  untouched. Pinned `MQClientAPIImpl.java` SHA-256:
  `bfb0ebf8e4b4c0fd4ad5a4db46d64a7c8897250fb411487793a23f29aa094737`.
- [x] `cargo test --locked -p rocketmq-classic-core --test
  rpc_invoked_java_fixtures`, Maven `verify` (37 manifest scenarios, fixture
  drift, Java/Rust cross-decodes, 11 message fixtures, 106 typed headers and 19
  typed bodies), `cargo fmt --check`, and `git diff --check` pass. Code commit:
  `091eba55`.
- [ ] This closes the seven overload groups only. Broad P18-V01
  source/settings/input/error/control-flow/reverse-export reconciliation and
  P18-V04 current-revision remote CI/native Windows runtime remain open.
  Phase 19 user-owned files and the Java checkout's untracked `aaaa.java` are
  unchanged. Execution record will be added after the current-tree gates.

## P18 final compatibility closure — 2026-10-24

This final roll-up supersedes earlier unchecked P18 checkpoint notes above.
The Java oracle is RocketMQ 5.5.1 source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`.

- [x] P18-V01 client-surface reconciliation is complete. The pinned
  `MQClientAPIImpl` inventory covers 132 declarations and seven overload
  groups; all 29 direct RPC invocation cases pass. API/configuration rows,
  inherited members, settings and read boundaries, input/error/control-flow
  behavior, Rust reverse exports, facade/crate surfaces, Rustdoc,
  `doc(hidden)`, features, environment settings and Controller DTOs are
  reconciled in the Phase 6–17 matrices and final audit. POP orderly service
  methods are mapped to the existing PushConsumer owner; common thread-pool
  monitors without client/remoting production callers are documented as
  outside client scope. Accepted provider/runtime/diagnostic and JVM identity
  boundaries are classified in `KNOWN_DIFFERENCES.md`.
- [x] P18-V03 provenance, documentation and package review are complete for
  the pinned Java source and current Rust surface.
- [x] P18-V02/P18-V04 final current-code remote validation passed on
  `0cf294b710d06d40bd72ccec1ca5d1df121c5225` in
  [GitHub Actions run 40136756812](https://github.com/triotous/rocketmq-classic/actions/runs/40136756812).
  Both Ubuntu and native Windows jobs passed: formatting, strict all-feature
  Clippy, default/all-feature workspace tests, Taipei unique-ID regression,
  differential self-test and 37-scenario manifest validation, fuzz/stress
  smoke, pinned Maven verification, release examples, benchmark compile,
  metadata checks, and Windows stack capture. The detailed run record is
  `rlogs/2026/10/24-phase-18-final-compatibility-closure__20261024T224144Z.rlog`.
- [x] Phase 19 handoff audit confirms that all 17 validation IDs from Phases
  0–17 are registered among 37 differential scenarios. This records fixture
  inventory only; live Broker/Docker execution remains in Phase 19.
- Phase 19 user-owned worktree paths remain untouched and unstaged. The
  Java checkout's pre-existing untracked `broker/src/test/java/org/apache/rocketmq/broker/aaaa.java`
  remains untouched.

## P18-05 final current-head benchmark refresh — 2026-10-24 (`4b7d0f27`)

- [x] After the compatibility closure, the bounded release benchmark was run
  on the final tracked code revision with fixed fixture inputs. The baseline
  now records hardware, toolchain, input sizes, iterations, timing and
  allocation observations in
  `docs/compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md`; no performance
  threshold is inferred from these machine-specific samples. Run record:
  `rlogs/2026/10/24-phase-18-final-benchmark-refresh__20261024T233124Z.rlog`.

## P18-V02 paginated timeout fixture determinism — 2026-10-25

GitHub run 40143142994 exposed millisecond drift in the pinned Java pagination
fixtures: `MQClientAPIImpl` derives each page timeout from `System.nanoTime()`,
and a remaining timeout of 2344 ms differed from the recorded 2345 ms. The
topic and subscription-group fixture recorders now capture whether each
attempted request has a positive budget. Their short delayed-timeout cases
continue to verify that an exhausted budget prevents a later page request.
The producer fixture was regenerated. Maven verification passes both while
regenerating the fixture and in default verification mode, and the focused
Rust pagination test passes. Current-head remote validation is triggered by
the fixture fix commit. Details: `rlogs/2026/10/25-phase-18-timeout-fixture-determinism__20261025T002216Z.rlog`.

## P19 Docker validation checkpoint — 2026-10-25 (`553564a2`)

- [x] P19-01 pinned the official ASF `apache/rocketmq:5.3.4` image index at
  `sha256:d57ee4ebbd9d7bb6e185775bde41c128e98f185b25bf0b05c77d4e6c010572b8`.
  The arm64 image reports Ubuntu 24.04/OpenJDK 8u472, and Broker startup
  reported `V5_3_4`; the Java client probe used 5.5.1 on Maven JDK 27 and the
  Rust runner used 1.85.0.
- [x] P19-02 basic single-Broker profile readiness, route lookup, scenario-owned
  resource cleanup, and host-to-advertised-Broker interoperability passed in
  seven fresh isolated runs. Current evidence is linked from
  `docs/compatibility/SERVER_5_3_4_RESULTS.md`.
- [x] P19-03 ran 19 server-free gates on the committed runner. Default and
  all-feature workspace suites, check, strict Clippy, Maven verify, comparator
  self-test/manifest, race/leak and fuzz smoke, examples, benchmark build/run,
  metadata/package checks, formatting, and UTC/Taipei ID checks passed.
  Evidence: `raw local output (not retained)`.
- [ ] P19-03/P19-V02 full live inventory remains BLOCKED: 30 of 37 scenario
  IDs have no runner. The fail-closed coverage gate ran after the offline
  gates, listed every missing ID, and started no Docker resources.
- [ ] P19-04/P19-V03/P19-V04 remain open. Only the seven listed live cases have
  Java/Rust comparisons; the broad P18-V01 source/API/settings/flow reverse
  audit and consumer, retry, rebalance, transaction, request/reply, TLS/auth,
  admin, and stress matrices are incomplete.
- [ ] P19-05/P19-V05 remain blocked. No matched Java/Rust outcomes or success
  path for the 30 missing cases is claimed. MAN-01 through MAN-05 remain
  BLOCKED in `docs/owner-validation.md`; no owner environment was accessed.

- [ ] P19-07 Docker CI job is present but has not been run remotely. Current
  local matrix correctly returns BLOCKED, so final compatibility/release
  approval is not supported.

Run record: `rlogs/2026/10/25-phase-19-docker-local-validation__20261025T013808Z.rlog`.

## P19 basic Broker coverage addendum — 2026-10-25 (`cb351d8a`)

- [x] Added Java 5.5.1 and Rust direct probes for consumer-offset query/update
  and pull requests. Four fresh basic-profile runs pass with clean committed
  source evidence: reciprocal offsets, a `FOUND` pull with exact response-body
  comparison, and an empty `NO_NEW_MSG` pull. Evidence IDs are recorded in
  `docs/compatibility/SERVER_5_3_4_RESULTS.md`.
- At `cb351d8a`, P19-03/P19-V02 remained blocked: 26 of 37 live cases lacked
  runners.
  The two-Broker, TLS/auth, transaction, request/reply, admin, stress and broad
  P18-V01 audit work remains incomplete. MAN-01 through MAN-05 remain blocked,
  and remote Docker CI has not run.
- [x] Reran `integration-tests/run-local.sh --all` on `70e9a4d8`. All 19
  server-free gates passed; the final coverage check returned `BLOCKED` with
  26 missing live scenarios and started no Docker resources. Evidence:
  `raw local output (not retained)`.
- [x] Added `P5-V02-route` on `63000299`. Java and Rust matched both the
  existing four-queue route and the missing-topic result on a fresh basic
  profile; clean evidence is linked in
  `docs/compatibility/SERVER_5_3_4_RESULTS.md`. Live coverage is now 12/37;
  25 remain pending. The full local gates have not yet been rerun after this
  runner addition.

## P19 two-Broker lock coverage — 2026-10-25 (`6684ff8c`)

- [x] Added the isolated `two-broker` profile and readiness check. A fresh
  profile validation reported `broker-a` and `broker-b` at server version
  `V5_3_4`; the disposable project, volumes, and network were removed.
- [x] Added Java/Rust `ORDER-001-lock-unlock` probes. Both clients acquired
  and released an open `broker-a` queue; the Rust client was denied while
  Java held the lock and acquired after Java's synchronous unlock. Java then
  acquired the queue after Rust released it. Clean evidence at revision
  `6684ff8c22c92fdb0fc3af8f97574fd3e34c9a82` is
  `raw local output (not retained)`.
  The earlier dirty-source rehearsal is retained separately and is not the
  manifest evidence pointer.
- Live coverage is now 13/37; 24 cases remain without runners. This does not
  verify replicated HA/failover, remaining two-Broker cases, TLS/ACL/features,
  or close P18-V01. The full `run-local.sh --all` rerun on `6684ff8c` is
  outstanding; MAN-01 through MAN-05 and remote Docker CI remain open.

## P19 current full local gates — 2026-10-25 (`a96aad3d`)

- [x] Reran `ROCKETMQ_CLASSIC_TEST_ENV=local-disposable bash
  integration-tests/run-local.sh --all` on committed revision
  `a96aad3d4df378e7b38ed5ebd0e819f764a89dc4`. All 19 server-free gates
  passed: formatting, diff, default/all-feature workspace tests, check,
  strict Clippy, pinned Maven verify, differential self-test/manifest,
  race/leak and fuzz smoke, release examples, benchmark build/run, metadata,
  package content, and UTC/Taipei Unique ID checks.
- [ ] The final live coverage gate returned `BLOCKED` with 24 missing scenario
  runners and started no Docker resources. Evidence:
  `raw local output (not retained)`.
  Live coverage remains 13/37; the other 24 local scenarios, broad P18-V01,
  MAN-01 through MAN-05, and remote Docker CI remain open.

## P19 two-Broker average queue assignment — 2026-10-25 (`b347d11c`)

- [x] Added the Java 5.5.1 and Rust live rebalance probes and a two-Broker
  `REBAL-001-average-10q-3c` runner. Commit: `b347d11c`.
- [x] The first dirty rehearsal exposed a false-positive setup path: it created
  the topic and cleaned up without executing the probes. Its manifest PASS
  pointer was removed; `20261025T085632Z/diagnostic-note.txt` records why the
  run is invalid. The runner now fails closed when an assertion handler is
  missing.
- [x] A corrected dirty rehearsal passed with identical assignment output and
  a 4/3/3 split; it is retained as diagnostic evidence at
  `20261025T085935Z/` and is not the manifest pointer.
- [x] Clean-source run `20261025T090131Z` passed on
  `b347d11c5e33c18d3e443c852bc8680e83892c2f`, with
  `source_code_dirty=false`. It checked identical Java/Rust output, all ten
  unique queues across both Brokers, the three sorted consumer rows, and the
  4/3/3 split. Topic and Compose resources were removed after capture.
- Live coverage is now 14/37; 23 cases remain without runners. The last full
  `run-local.sh --all` gate was at `a96aad3d` before this probe and must be
  rerun. Broad P18-V01, MAN-01 through MAN-05, and remote Docker CI remain open.

## P19 transactional commit and rollback visibility — 2026-10-25 (`1cc67ce3`)

- [x] Added transaction-group input validation, isolated consumer-group setup,
  and Java/Rust pull assertions to `TX-001-commit` and `TX-002-rollback`.
  Implementation commits: `c6084967` (transaction probes), `ab7539ca`
  (host-routed transaction profile), and `1cc67ce3` (visibility assertions).
- [x] Clean-source TX-001 run `20261025T091920Z` passed on
  `1cc67ce358d4f191b20dc33e71b59a175a57eb59` with
  `source_code_dirty=false`. Both producers committed; Java/Rust pulls matched
  byte-for-byte, returned both message bodies, and the queue max offset was 2.
- [x] Clean-source TX-002 run `20261025T092046Z` passed on the same revision.
  Both producers rolled back; Java/Rust pulls matched with `NO_NEW_MSG`, empty
  bodies, and queue max offset 0. Both isolated topics, projects, networks,
  and volumes were removed after evidence capture.
- Earlier callback/offset-only runs are retained as historical evidence; the
  manifest points to the stronger consumer-visibility runs.
- Live coverage is 16/37, with 21 scenarios still pending. The full local gate
  last ran before the rebalance and transaction additions and must be rerun.
  Broad P18-V01, MAN-01 through MAN-05, and remote Docker CI remain open.

## P19 Phase 0 aggregate — 2026-10-25 (`34f14b72`)

- [x] Added `P0-V03` as a fail-closed aggregate over 11 existing route,
  producer, consumer, heartbeat, offset, and lock scenarios. The parent records
  only children with PASS results, matching source revision, clean-source
  metadata, and successful cleanup. Implementation commit: `34f14b72`.
- [x] The aggregate passed at `20261025T092635Z` on
  `34f14b727e200016961d5189f160dc96ceb3a06e`, with `source_code_dirty=false`.
  All 11 child runs also passed on that revision; the evidence table and logs
  are archived under `raw local output (not retained)`.
- [x] Refreshed the 11 child manifest pointers to the fresh evidence collected
  by this aggregate. No Phase 19 Compose resources remained after teardown.
- Live coverage is 17/37, with 20 scenarios pending. The full local gate must
  be rerun after these additions; broad P18-V01, MAN-01..05, and remote Docker
  CI remain open.

## P19 P7-V02 high-level producer modes — 2026-10-25 (`f9bfeb2a`)

- [x] Added a host-routed disposable Broker profile and Java 5.5.1/Rust probes
  for sync, async, oneway, explicit-queue, selector, valid-batch, and invalid
  mixed-topic batch behavior. Implementation commit: `f9bfeb2a`.
- [x] `P7-V02` passed at `20261025T094705Z` on
  `f9bfeb2aa120638a12ca0eba87c8fb2f7a4ddcec` with `source_code_dirty=false`.
  Java and Rust matched all seven outcomes; the scenario topic and Compose
  project, network, and volume were removed after evidence capture.
- Live coverage is 18/37, with 19 scenarios pending. The full local gate must
  be rerun after these additions; broad P18-V01, MAN-01..05, and remote Docker
  CI remain open.

## P19 P6-V04 shared-runtime heartbeat and unregister — 2026-10-25 (`1261cedd`)

- [x] Added Java 5.5.1 and Rust shared-runtime probes that change the active
  push-consumer tag filter, verify Broker-side filtering, unregister producer
  A, and confirm producer B remains usable. Implementation commit: `1261cedd`.
- [x] `P6-V04` passed at `20261025T095527Z` on
  `1261cedd9245fe9d38b32d3ff34333bad8179284` with `source_code_dirty=false`.
  Both clients consumed the initial `DROP` message, consumed `KEEP` after the
  subscription update, did not receive the later `DROP`, and received a
  producer B message after producer A unregistered.
- [x] The manifest points to the clean run. Topic deletion and host-routed
  Compose teardown passed; no project containers, volume, or network remained.
- Live coverage is 19/37, with 18 scenarios pending. The full local gate must
  be rerun after the remaining runner additions; broad P18-V01, MAN-01..05,
  and remote Docker CI remain open.

## P19 P9-V03 explicit pull and offset resume — 2026-10-25 (`3343347d`)

- [x] Added the Java 5.5.1 `DefaultMQPullConsumer` probe and Rust `PullConsumer`
  probe for manual pull, all pull statuses, suggested-broker selection,
  offset-boundary RPCs, and restart offset resumption. Implementation commit:
  `3343347d`.
- [x] `P9-V03` passed at `20261025T100359Z` on
  `3343347dab7c640cd9fdbf24467c08dac4175580` with `source_code_dirty=false`.
  Java and Rust matched all four statuses, broker ID 0, min/max/search offsets
  `0/2/0`, the seeded body, and resumed offset 1.
- [x] The manifest points to the clean run. Topic deletion and host-routed
  Compose teardown passed; no project containers, volume, or network remained.
- Live coverage is 20/37, with 17 scenarios pending. Full local gates must be
  rerun after the remaining runner additions; broad P18-V01, MAN-01..05, and
  remote Docker CI remain open.

## P19 REQ-001 cross-language request/reply — 2026-10-25 (`bb775486`)

- [x] Isolated the two request directions on scenario-owned Java and Rust
  topics so each responder observes only its paired requester. The runner
  verifies the received request, reply body, correlation ID, reply-to, TTL,
  and reply message type.
- [x] `REQ-001-request-reply-success` passed at `20261025T103216Z` on
  `bb77548634ac623bd621a1afbb87a4558163c6c5` with `source_code_dirty=false`.
  Both responders sent one reply, and each requester returned its matching
  reply. Topic deletion and host-routed Compose teardown passed.
- Live coverage is 21/37, with 16 scenarios pending. Full local gates must be
  rerun after the remaining runner additions; broad P18-V01, MAN-01..05, and
  remote Docker CI remain open.

## P19 PROD-005 producer timeout — 2026-10-25 (`77afd66e`)

- [x] Added a local delayed-response proxy and direct Java/Rust send probes.
  Each Broker response was delayed by 600 ms against the reviewed 100 ms send
  deadline; both clients reported timeout.
- [x] `PROD-005-send-timeout` passed at `20261025T104539Z` on
  `77afd66edd59a2257d1c5772d68e86262bbd148e` with `source_code_dirty=false`.
  The sequential sends began at queue offsets 0 and 1, and both Java and Rust
  pull results matched on exactly two committed messages, each body present
  once. Topic deletion and two-Broker Compose teardown passed.
- Live coverage is 22/37, with 15 scenarios pending. Full local gates must be
  rerun after the remaining runner additions; broad P18-V01, MAN-01..05, and
  remote Docker CI remain open.

## P19 P5-V02 producer RPC variants — 2026-10-25 (`e00e8c4f`)

- [x] Added direct Java 5.5.1/Rust probes for send V1, send V2, batch, reply
  V1/V2, and end-transaction RPCs. Reply variants ran in one-way mode and the
  scenario provisioned a reply consumer group and heartbeat identity.
- [x] `P5-V02-producer` passed at `20261025T110552Z` on
  `e00e8c4f161509c392b6116a17a5968dd39a95e2` with `source_code_dirty=false`.
  Java and Rust matched `SEND_OK`, topic, queue, and sequential offsets for
  the three stored sends. Both returned one-way results for reply V1/V2 and
  `endTransactionOneway`; each language stored three messages.
  Topic deletion and basic-profile Compose teardown passed.
- Live coverage is 23/37, with 14 scenarios pending. Full local gates must be
  rerun after the remaining runner additions; broad P18-V01, MAN-01..05, and
  remote Docker CI remain open.

## P19 P5-V02 consumer RPC variants — 2026-10-25 (`df61d2f7`)

- [x] Added direct Java 5.5.1/Rust probes for ordinary pull, Lite pull, offset
  update/query, consumer-list, send-back, and queue lock/unlock on the
  two-Broker profile.
- [x] `P5-V02-consumer` passed at `20261025T111709Z` on
  `df61d2f784176a0841839be47c8c9f02183e5948` with `source_code_dirty=false`.
  Both probes matched `FOUND` with two messages and offsets 0/2 for both pull
  methods; the remaining six RPCs also matched their successful outcomes.
  Topic deletion and two-Broker Compose teardown passed.
- Live coverage is 24/37, with 13 scenarios pending. Full local gates must be
  rerun after the remaining runner additions; broad P18-V01, MAN-01..05, and
  remote Docker CI remain open.

## P19 P3-V04 raw route and TLS lifecycle — 2026-10-25 (`d673e6ff`)

- [x] Added an isolated TLS profile and Java 5.5.1/Rust live probes. The raw
  route calls the local NameServer. TLS requests go through a locally generated
  TLS-terminating proxy to the disposable plain Broker; this validates client
  TLS behavior, not TLS configuration in the Broker.
- [x] `P3-V04` passed at `20261025T113910Z` on
  `d673e6ff6aefcd12831203a5b131c72a37c22678` with `source_code_dirty=false`.
  Java and Rust matched the route and trusted/untrusted CA outcomes; Rust
  rejected a trusted wrong-host certificate while Java 5.5.1 accepted it
  because the client TLS handler has no peer hostname. All clients reported
  zero pending requests, retained channels, or worker tasks.
- [x] The runner deleted `compat_p3_tls` and removed its Compose containers,
  network, and volume after evidence capture.
- Live coverage is 25/37 with 12 cases still lacking runners:
  `P5-V02-extensions`, `P7-V03`, `P7-V04`, `P10-V03`, `P11-V03`, `P11-V04`,
  `P12-V03`, `P13-V02`, `P14-V03`, `P15-V03`, `P16-V02`, and `P17-V02`.
  The full `run-local.sh --all` gate must be rerun after the remaining runner
  additions; broad P18-V01, MAN-01 through MAN-05, and remote Docker CI remain
  open.

## P19 P5-V02 extension RPC attempt — 2026-10-25 (`91b0bc56`)

- [x] Added Java 5.5.1 and Rust live probes and a fail-closed runner for the
  Phase 5 heartbeat, assignment, request-mode, Lite subscription, POP, ACK,
  batch ACK, and visibility-change RPC methods.
- [ ] The clean-source attempt `20261025T115801Z` stopped after Java had
  completed heartbeat, heartbeat V2, assignment, and request-mode. Broker
  5.3.4 returned code 3, `request type 200071 not supported`, for
  `syncLiteSubscriptionAsync`. No Rust comparison or successful POP/ACK path
  ran, so `P5-V02-extensions` remains pending. Diagnostic evidence is under
  `raw local output (not retained)`.
- [x] The failed run deleted the parent and derived Lite topics and removed
  its Compose containers, network, and volume. Live coverage remains 25/37,
  with 12 Phase 19 scenarios pending; full local gates must be rerun after
  additional passing scenarios, and broad P18-V01, MAN-01..05, and remote
  Docker CI remain open.

## P19 P13-V02 POP lifecycle first attempt — 2026-10-25 (`61d46867`)

- [ ] Runs `20261025T121016Z`, `20261025T121249Z`, and `20261025T121555Z`
  seeded queue 0 and queue 1, then Java POP returned `POLLING_NOT_FOUND`. The
  third run prepared both clients' POP modes before seeding and still got the
  response. Run `20261025T122129Z` stopped before POP: Java requested queue 0,
  but the Broker returned queue 1 and queue 0 remained empty. No POP/ACK
  lifecycle result passed; the P13 manifest row remains pending while seed
  routing and the live POP exchange are corrected. Diagnostic evidence is under
  `raw local output (not retained)`.
- [ ] Route-aware retry `20261025T122748Z` sent to queues 0 and 1; queue 0's
  max offset advanced, but queue 1's immediate max-offset read still returned
  zero after `SEND_OK`. The run stopped before POP and cleaned the isolated
  resources. The next attempt will wait for Broker offset visibility.
- [ ] Run `20261025T123103Z` passed both seed checks at max offset 1, then Java
  POP returned `POLLING_NOT_FOUND`. The next probe will use only the pre-seed
  POP mode setup and explicitly set `initMode=0`.
- [ ] Run `20261025T123522Z` used the pre-seed mode and `initMode=0`, passed both
  seed checks, and still returned `POLLING_NOT_FOUND`. Capture queue min/max and
  group offset in the next diagnostic.
- [ ] Run `20261025T123824Z` captured queue min/max `0/1` and group offset 1
  before and after the first POP. The group offset was already past the seeded
  message; trace when it advances before changing lifecycle assertions.
- [ ] Run `20261025T124251Z` saw group offsets `NOT_FOUND` after prepare and
  seeding, then 1 before POP. Initialize scenario group offsets at 0 after
  seeding and verify before continuing.
- [x] The runner deleted the scenario topic and removed its isolated
  two-Broker containers, network, and volumes. Coverage remains 25/37 PASS
  with 12 Phase 19 scenarios pending.

## P19 P13-V02 POP offset reset retry — 2026-10-25 (`d68d8f12`)

- [ ] Clean-source run `20261025T124657Z` initialized and verified offset 0 on
  both queues for all six Java/Rust POP groups after seeding. The separate Java
  consume process observed offset 1 before its first POP and received
  `POLLING_NOT_FOUND`; no lifecycle assertion passed and Rust consumption did
  not run. The exact transition after reset remains unlocalized. Evidence is
  under `raw local output (not retained)`.
- [x] The runner deleted the scenario topic and removed its isolated two-Broker
  containers, network, and volumes. P13 remains pending; coverage remains
  25/37 with 12 Phase 19 scenarios pending.

## P19 P13-V02 initial POP delivery confirmation — 2026-10-25 (`9568a2bb`)

- [ ] Clean-source run `20261025T125153Z` traced group offset 0 after API
  startup and immediately before the first Java POP request. That request
  delivered the seeded message; the offset became 1 afterward. The following
  expiry-redelivery POP returned `POLLING_NOT_FOUND`, so the lifecycle did not
  pass and Rust consumption did not run. This supersedes the earlier
  interpretation that offset 1 preceded initial delivery. Evidence is under
  `raw local output (not retained)`.
- [x] The runner deleted the topic and removed its isolated two-Broker
  containers, network, and volumes. P13 remains pending; live coverage remains
  25/37 with 12 Phase 19 scenarios pending.

## P19 P13-V02 Java lifecycle and Rust receipt mapping — 2026-10-25 (`89f481e7`)

- [ ] Clean-source run `20261025T125713Z` passed all Java lifecycle assertions
  after a 6-second expiry wait. Rust POP returned the seeded message, but its
  low-level probe lacked Java's `POP_CK` decoration derived from response-header
  offsets; it stopped before Rust lifecycle assertions. The probe now mirrors
  that mapping, and a clean Java/Rust comparison remains pending. Evidence is
  under `raw local output (not retained)`.
- [x] The runner deleted the scenario topic and removed its isolated two-Broker
  containers, network, and volumes. P13 remains pending at 25/37 live PASS.


## P19 P13-V02 POP lifecycle pass — 2026-10-25 (`99d2dc68`)

- [x] Clean-source run `20261025T130156Z` on
  `99d2dc68484ab8de43da86b997dc20dc49267c3c` passed the Java/Rust POP
  lifecycle comparison: delivery, expiry redelivery, ACK, visibility change,
  replacement receipt ACK, mixed queue mapping, and malformed receipt
  rejection. The manifest records PASS. Evidence is under
  `raw local output (not retained)`.
- [x] The runner deleted the scenario topic and removed the two-Broker Compose
  containers, network, and volumes. Live coverage is 26/37 with 11 cases
  pending: `P5-V02-extensions`, `P7-V03`, `P7-V04`, `P10-V03`, `P11-V03`,
  `P11-V04`, `P12-V03`, `P14-V03`, `P15-V03`, `P16-V02`, and `P17-V02`.
  The P5 extension attempt remains blocked at unsupported Lite subscription
  request 200071 on Broker 5.3.4; the other listed cases remain unrun.

## P19 P5-V02-extensions parity pass — 2026-10-25 (`d93c978e`)

- [x] Clean-source run `20261025T132049Z` on
  `d93c978e04c3619d3c8fe6e1c0d82c4de05f050b` passed Java/Rust heartbeat,
  heartbeat V2, assignment, request-mode, standard POP, ACK, batch ACK, and
  visibility-change checks. Both clients received the pinned 5.3.4
  unsupported response (code 3) for Lite subscription request 200071 and
  Lite POP request 200070. Evidence is under
  `raw local output (not retained)`.
- [x] The runner deleted the parent and derived Lite topics and removed its
  isolated Compose containers, network, and volume. Live coverage is 27/37
  with 10 runnerless cases pending: `P7-V03`, `P7-V04`, `P10-V03`,
  `P11-V03`, `P11-V04`, `P12-V03`, `P14-V03`, `P15-V03`, `P16-V02`, and
  `P17-V02`. Positive Lite success-path evidence is recorded separately under
  P19-05; it does not alter the 5.3.4 manifest row.

## P19-05 supplementary Lite success-path pass — 2026-10-25 (`3eb70c19`)

- [x] Clean-source run `20261025T135033Z` on
  `3eb70c192a885ea831cdc496ca24399a182552b5` passed against the unmodified
  official `apache/rocketmq:5.5.0` image pinned by index digest
  `sha256:7e8f6c9dbd9df742ed26ba69c00d4ad69e2f86a56f3ca7782ff8144dd0798132`.
  Java 5.5.1 and Rust matched heartbeat, heartbeat V2, assignment, request
  mode, Lite subscription, Lite POP `FOUND`, and the seeded message body.
  The isolated profile enabled LMQ and multi-dispatch; `source_code_dirty=false`,
  exit status 0, duration 100 seconds. Evidence:
  `raw local output (not retained)`.
- [x] The runner deleted the derived Lite topic and parent topic, then removed
  its containers, network, and volume. Filtered Docker queries returned no
  remaining project resources. This supplementary result does not modify the
  required 5.3.4 `P5-V02-extensions` manifest row or establish production
  support. Ten runnerless manifest cases remain pending.

## P19-04 P7-V04 clean-source lifecycle pass — 2026-10-25 (`816e2e10`)

- [x] Clean-source run `20261025T141436Z` on
  `816e2e10a30408aa4303b923a99892e6be4b06c5` passed against the pinned
  unmodified RocketMQ 5.3.4 image. Java 5.5.1 and Rust each sent 20 messages
  in three producer start/shutdown cycles; pending requests, connections, and
  owned task groups were zero after each cycle. Evidence is under
  `raw local output (not retained)`.
- [x] The runner deleted the scenario topic from both host-routed Brokers and
  removed the isolated containers, NameServer, volumes, and network. Exit
  status 0, duration 88 seconds, `source_code_dirty=false`; filtered Docker
  queries found no project resources.
- [ ] Live coverage is now 28/37. Nine cases remain without runners:
  `P7-V03`, `P10-V03`, `P11-V03`, `P11-V04`, `P12-V03`, `P14-V03`,
  `P15-V03`, `P16-V02`, and `P17-V02`. Full local gates and owner manual
  evidence remain pending.

## P19-04 P15-V03 clean-source request/reply pass — 2026-10-25 (`4eeac3a3`)

- [x] `P15-V03` passed against the pinned unmodified RocketMQ 5.3.4 image on
  committed revision `4eeac3a32a417bd3f2dc5c4d031db9eb0f5f9c02`; Java 5.5.1 and
  Rust matched both successful requester/responder directions, no-responder
  timeouts in both clients, and delayed opposite-language replies after timeout.
  Each late timeout retained one terminal callback and no pending correlation.
  Evidence is under
  `raw local output (not retained)`.
- [x] `source_code_dirty=false`, exit status 0, duration 280 seconds. The runner
  removed all six scenario topics from both Brokers and removed the isolated
  containers, NameServer, volumes, and network. Filtered Docker queries found
  no project resources.
- [ ] Live coverage is now 29/37. Eight cases remain without runners:
  `P7-V03`, `P10-V03`, `P11-V03`, `P11-V04`, `P12-V03`, `P14-V03`,
  `P16-V02`, and `P17-V02`. Full local gates, reverse audit, and owner manual
  evidence remain pending.

## P19-04 P7-V03 clean-source retry and deadline pass — 2026-10-25 (`0656dabc`)

- [x] Clean-source run `20261025T150708Z` on committed revision
  `0656dabc02247ac0965b24b3d5d27a360b32ca1f` passed against the pinned
  unmodified RocketMQ 5.3.4 image. Java 5.5.1 and Rust each received injected
  retryable code-16 responses from both Brokers, avoided the previously failed
  Broker, and succeeded on attempt three with a stable body and unique ID.
  Both then hit the 900 ms deadline against a delayed real Broker response.
  Evidence is under
  `raw local output (not retained)`.
- [x] Metadata records `source_code_dirty=false`, exit status 0, and duration
  91 seconds. The runner deleted the scenario topic from both Brokers and
  removed the isolated containers, NameServer, volumes, and network. Filtered
  Docker queries found no project resources.
- [ ] Live coverage is now 30/37. Seven runnerless cases remain:
  `P10-V03`, `P11-V03`, `P11-V04`, `P12-V03`, `P14-V03`, `P16-V02`, and
  `P17-V02`. Full local gates, reverse audit, and owner manual evidence remain
  pending.

## P19-05 P10-V03 clean-source Push retry and DLQ pass — 2026-10-25

- [x] Clean-source run `20261025T153016Z` on revision
  `d3a0778bf47a4bd20b4a5129911318cd194ff742` passed against the pinned,
  unmodified RocketMQ 5.3.4 image. Java 5.5.1 seeded the test topic; Rust Push
  verified listener success, partial acknowledgement, a retried partial
  message, and a failure message delivered at listener retry counts 0 and 1.
  The configured DLQ received the original body and key with Broker
  `reconsumeTimes=2` for group retry maximum 1.
- [x] Evidence is under
  `raw local output (not retained)`;
  metadata records `source_code_dirty=false`, exit status 0, and duration
  124 seconds. The runner deleted the owned topic from both Brokers and removed
  the isolated containers, NameServer, volumes, and network. Filtered Docker
  queries found no project resources.
- [ ] Live coverage is now 31/37. Six runnerless cases remain:
  `P11-V03`, `P11-V04`, `P12-V03`, `P14-V03`, `P16-V02`, and `P17-V02`.
  Full local gates, reverse audit, and owner manual evidence remain pending.

## P19-05 P11-V03 clean-source mixed-group convergence pass — 2026-10-25

- [x] Clean-source run `20261025T154631Z` on committed revision
  `7d03c66189d2de34d9d523ff35b8e6b302cc5087` passed against the pinned,
  unmodified RocketMQ 5.3.4 image. Java initially owned all eight scenario
  queues. With Rust joined, the clients converged to four disjoint queues each;
  one transient overlapping snapshot occurred during convergence and was
  allowed. After Java left, Rust converged to all eight queues.
- [x] Evidence is under
  `raw local output (not retained)`;
  metadata records `source_code_dirty=false`, exit status 0, and duration
  93 seconds. The runner deleted the owned topic from both Brokers and removed
  the isolated containers, NameServer, volumes, and network. Filtered Docker
  queries found no project resources.
- [ ] Live coverage is now 32/37. Five runnerless cases remain:
  `P11-V04`, `P12-V03`, `P14-V03`, `P16-V02`, and `P17-V02`. Full local gates,
  reverse audit, and owner manual evidence remain pending.

## P19-05 P11-V04 clean-source bidirectional Push pass — 2026-10-25

- [x] Clean-source run `20261025T155944Z` on committed revision
  `a7066e9fb6fd0739700046e6106f8c7ee4ef1516` passed against the pinned,
  unmodified RocketMQ 5.3.4 image. Java producer to Rust Push on Broker A and
  Rust producer to Java Push on Broker B each observed retry counts 0/1 and
  completed successfully. Java and Rust offset queries matched at 1 on both
  Brokers.
- [x] Each Push client completed three start/shutdown cycles. Java's runtime
  and pull scheduled executors terminated; Rust's ClientRuntime stopped, the
  factory table emptied, and pending requests, connections, and owned remoting
  tasks returned to zero. Evidence is under
  `raw local output (not retained)`;
  metadata records `source_code_dirty=false`, exit status 0, and duration 168
  seconds.
- [x] The runner deleted `compat_p11_push` and `%RETRY%compat_p11_push` from
  both Brokers and removed the isolated containers, NameServer, volumes, and
  network. Filtered Docker queries found no project resources.
- [ ] Live coverage is now 33/37. Four runnerless cases remain:
  `P12-V03`, `P14-V03`, `P16-V02`, and `P17-V02`. Full local gates, reverse
  audit, and owner manual evidence remain pending.

## P19-05 P17-V02 clean-source admin query pass — 2026-10-25

- [x] Clean-source run `20261025T172205Z` on committed revision
  `aff298c39e17aa1f00ed3b021a87e1318587593c` passed against the pinned
  RocketMQ 5.3.4 image (`source_code_dirty=false`, exit 0, 112 seconds). Java
  5.5.1 and Rust matched duplicate-key results from the two explicitly seeded
  Brokers and the timestamp-bounded result. An injected Broker B failure
  preserved Broker A's result; injected failures at both Brokers mapped to
  `NO_MESSAGE` response code 208 in both clients.
- [x] The runner deleted `compat_p17_admin_query` from both Brokers and removed
  the isolated containers, NameServer, volumes, and network. Filtered exact-
  project Docker queries found no resources. Evidence is under
  `raw local output (not retained)`.
- [ ] Live coverage is 35/37 PASS. `P12-V03` and `P16-V02` remain pending;
  full local gates, the reverse audit, owner manual evidence, and final Phase
  19 acceptance remain open.

## P19-05 P12-V03 clean-source orderly handoff pass — 2026-10-25 (`de4212ff`)

- [x] Clean-source run `20261025T173114Z` on committed revision
  `de4212ffed4013b91f2fc3d694c6c4148eeee295` passed against the pinned
  unmodified RocketMQ 5.3.4 image (`source_code_dirty=false`, exit 0,
  141 seconds). Java held callbacks on both Brokers while Rust joined and
  acquired the transferred locks. Rust processed redelivered messages and
  successors and committed offset 2 before Java's callbacks were released;
  final offsets stayed at 2 after both clients stopped.
- [x] The Java probe's successful consumer path now ends its isolated Maven
  process after the public consumer shutdown returns, avoiding Maven waiting
  forever on an idle RocketMQ scheduler thread. The runner confirmed both
  Broker queues became lockable and releasable afterward.
- [x] Scenario topic and groups were deleted. The isolated Compose containers,
  volumes, and network were removed; filtered Docker queries found no resources.
  Evidence is under
  `raw local output (not retained)`.
- [ ] Live coverage is 36/37 PASS. `P16-V02`, all local suite reconciliation,
  the reverse audit, owner manual evidence, and final Phase 19 acceptance
  remain open.

## P19-05 P16-V02 clean-source ACL and TLS pass — 2026-10-25 (`ba9480cf`)

- [x] Clean-source run `20261025T174649Z` on committed revision
  `ba9480cff5f607f105c5df6a9578eeff089fa19c` passed (`source_code_dirty=false`,
  exit 0, 106 seconds). Java 5.5.1 and Rust accepted valid ACL signatures and
  rejected wrong secrets and tampered request bodies. Both clients accepted
  the trusted mutual-TLS endpoint, rejected untrusted CAs and missing client
  certificates, and matched the Java-specific wrong-host behavior.
- [x] The Broker used the SHA-512-verified official Apache 5.3.4 binary
  distribution in a digest-pinned amd64 Temurin JDK container because the
  Linux ARM64 release image lacks its required RocksDB JNI library. The
  NameServer remained on the pinned 5.3.4 image. Evidence records both image
  IDs, the distribution checksum, config hashes, toolchains, and redacted
  Broker logs at
  `raw local output (not retained)`.
- [x] The per-run synthetic credentials and private keys were removed. The
  isolated project containers, volume, and network were removed; exact-project
  Docker queries found no resources.
- [ ] All 37 live scenarios now have passing evidence. The aggregate
  `run-local.sh --all` gate, Phase 18 reverse audit, owner manual evidence, and
  final Phase 19 acceptance remain open.

## P19-03 aggregate gate diagnostic — 2026-10-25 (`1850d789`)

- [x] The first current-revision aggregate attempt passed formatting, diff
  hygiene, both default and all-features workspace suites, all-target check,
  strict Clippy, and Java `mvn verify`.
- [x] The differential self-test exposed a stale assertion that required
  `P6-V04` to remain pending even after the manifest recorded its passing live
  evidence. The self-test now accepts the valid `PENDING_PHASE_19` or `PASS`
  states. Standalone differential self-test, 37-row manifest validation, and
  `git diff --check` pass with this correction.
- [ ] The aggregate attempt stopped at the differential self-test before
  subsequent server-free gates or Docker replay. Rerun `run-local.sh --all`
  from the committed correction before Phase 19 acceptance.

## P19-03 live runner and evidence reconciliation corrections — 2026-10-25 (`f048f09d`)

- [x] The corrected aggregate attempt passed all server-free gates and started
  `tests/differential/run.sh --all`. Inspection of its archived output showed
  that the parent manifest loop executed only `ROUTE-001`: its child command
  inherited the manifest input descriptor and consumed the remaining rows.
  The live step's exit status alone did not prove all 37 rows ran.
- [x] The final reconciliation also assumed each manifest evidence path was a
  parent directory, although entries point directly to a run directory with a
  top-level `result.txt`. The checker now supports both direct and nested run
  evidence paths; a local check reconciled all 37 current PASS records.
- [x] The `--all` child invocation now reads from `/dev/null`, preserving the
  parent's selected-row input. Shell syntax, direct evidence reconciliation,
  and diff hygiene pass with both fixes.
- [x] Full aggregate replay at code revision
  1e909440b39720e814a1db9d79d92bbc6d733713 passed. The run log names all
  37 live scenario IDs and final evidence reconciliation passed.
- [x] All 23 recorded local gates passed, including default/all-feature
  workspace tests, check, strict Clippy, Java verification, race/leak and fuzz
  smoke, examples, benchmark build/run, metadata/package checks, UTC/Taipei ID
  checks, Docker toolchain, live scenarios, and evidence reconciliation.
  Evidence: <code>raw local output (not retained)</code>.
- [x] Post-closure P18 current-tree public-surface delta reviewed and indexed;
  MAN-05 is N/A with source-backed evidence. MAN-01 through MAN-04 remain
  owner-input blocked.
- [x] Post-run Docker queries found no Phase 19 containers, volumes, or
  networks. The nine generated private-key copies in archived P3-V04 run
  directories were removed.

## P19 final corrected current-tree local validation — 2026-10-26

- [x] The final full local replay passed all 23 gates at workspace revision
  `c877869bd1f9bfacf288a27593649cca12ed9c5d`, containing production code
  revision `d3c7d2e845364e40ad72f0540a0a498fc1582247`. Evidence is
  `raw local output (not retained)`.
  Gates passed formatting and diff hygiene, default and all-feature workspace
  suites, workspace check, strict Clippy, Java verification, differential
  self-test and manifest validation, race/leak and fuzz smoke, release
  examples, benchmark build and run, Cargo/workspace metadata, package content,
  UTC and Taipei ID checks, Docker toolchain, all live scenarios, and final
  evidence reconciliation.
- [x] The aggregate executed all 37 registered live scenario IDs. Final
  reconciliation confirms every manifest row points to a PASS result with
  exit status 0 and `source_code_dirty=false`. The evidence and manifest
  pointers were committed in `f92b7917`.
- [x] Two earlier corrected-TLS aggregate attempts are retained as diagnostics:
  `20261025T223516Z` passed tests, check, and other preceding gates but strict
  Clippy found one needless borrow in `consumer.rs`; commit `21430d58` fixed
  it. `20261025T224821Z` then passed preceding gates but found three needless
  borrows in `live_client.rs`; commit `d3c7d2e8` fixed them. The subsequent
  aggregate passed strict Clippy and all remaining gates.
- [x] Fresh clean-source live evidence was recorded for
  P3-V04 (raw output not retained),
  P6-V04 (raw output not retained),
  and P16-V02 (raw output not retained).
  Each records `source_code_dirty=false` and exit status 0. Synthetic per-run
  private-key files were removed from the current worktree after evidence
  capture.
- [x] Post-run Docker queries found no Phase 19 containers, volumes, or
  networks. No owner or production environment was accessed.
- [ ] The remote Docker CI job has not been run. Remote CI and the owner-only
  checks remain outside this local aggregate's evidence.
- [ ] MAN-01 through MAN-04 remain pending owner-provided network paths,
  security material and policy, authorized HA topology, and representative
  workload/SLO inputs. This local replay does not provide owner approval or
  final production release approval.

Run record:
`rlogs/2026/10/26-phase-19-final-local-aggregate__20261026T002813Z.rlog`.

## P19 P7-V04 Broker-backed producer stress extension — 2026-10-26 (`5e1c80b1`)

- [x] Added a clean-source P7-V04 workload with fixed seed `1592594996`,
  8,192 variable-size messages per language, a maximum of 32 concurrent sends,
  and a 120-second deadline. Java and Rust each sent 6,292,961 payload bytes
  with checksum `f47e95bbe41fd746`; Java queue offsets were 0–8,191 and Rust
  offsets were 8,192–16,383.
- [x] Both clients reported zero pending requests, connections, and owned task
  groups after the workload, then completed three additional 20-send
  start/shutdown cycles with the same zero-resource result after every cycle.
  Clean-source evidence: `raw local output (not retained)`
  (`source_code_dirty=false`, exit status 0, duration 102 seconds).
- [x] The scenario-owned topic and two-Broker disposable Compose project were
  removed after evidence capture. No owner or production environment was
  accessed.
- [ ] Rerun `integration-tests/run-local.sh --all` on the new stress-probe
  revision and reconcile all 37 scenario evidence pointers. The prior 23-gate
  aggregate applies to the earlier source revision.
- [ ] Push the completed local candidate and await the configured GitHub
  Actions workflow. Run `40249722222` was triggered for earlier commit
  `1e4152b9`, before this stress extension; it does not validate `5e1c80b1`.
- [ ] MAN-01 through MAN-04 remain pending owner-provided network, security,
  HA topology, and representative workload/SLO inputs. P7-V04 supplies bounded
  local load evidence but does not establish production capacity or replicated
  HA failover.

Run record:
`rlogs/2026/10/26-phase-19-p7-v04-broker-backed-stress__20261026T010127Z.rlog`.

## P19 final bounded-stress aggregate — 2026-10-26 (`5764ec84`)

- [x] The fresh `ROCKETMQ_CLASSIC_TEST_ENV=local-disposable bash
  integration-tests/run-local.sh --all` replay passed all 23 recorded gates at
  source revision `5764ec8488c3319768246bb9218616c339d433f6`. The live runner
  executed all 37 registered Phase 19 scenarios, and the final reconciliation
  passed all 37 manifest rows against their PASS evidence. Aggregate evidence:
  `raw local output (not retained)`.
- [x] P7-V04 passed in the aggregate at
  `raw local output (not retained)` with clean
  source and exit status 0. Java and Rust each sent 8,192 fixed-seed messages
  (6,292,961 bytes; checksum `f47e95bbe41fd746`) with at most 32 concurrent
  sends and contiguous Broker offsets. Three further 20-send lifecycle cycles
  ended with zero pending requests, connections, or owned task groups.
- [x] The prior aggregate at
  `raw local output (not retained)` stopped
  during P6-V04's Rust probe wait after earlier gates passed. A focused P6-V04
  rerun passed at `20261026T021437Z`, and P6-V04 passed again in the complete
  aggregate at `20261026T032432Z`. The earlier failure remains archived as a
  diagnostic; it did not recur.
- [x] Removed generated TLS private-key copies from archived evidence. Exact
  Phase 19 Docker project queries found no leftover containers, volumes, or
  networks.
- [ ] Push the evidence and manifest-pointer commit, then await GitHub Actions
  on the final source revision. The earlier run `40249722222` was for
  `1e4152b9` and exposed a CI Java-version check that rejected Temurin's
  `27.0.1.0.2` version string. Commit `5764ec84` now checks the Java major
  version; the fresh workflow must confirm the fix.
- [ ] MAN-01 through MAN-04 remain pending owner-provided network paths,
  security material and policy, authorized HA topology, and representative
  workload/SLO inputs. The bounded local stress case does not establish
  production capacity or replicated HA failover. No owner or production
  environment was accessed.

Run record:
`rlogs/2026/10/26-phase-19-final-stress-aggregate__20261026T035914Z.rlog`.

## P19 Linux stack-capture CI concurrency correction — 2026-10-26

- [x] GitHub Actions run `40266865631` on `187b0427` passed
  `windows-stack-runtime` but the parallel Linux workspace test job failed in
  `stack::tests::concurrent_jstack_captures_are_serialized_without_losing_threads`.
  The process had no available default-ignore signal among the current Linux
  stack-capture candidates while unrelated tests ran concurrently.
- [x] Added `SIGIO` as a fourth safe default-ignore signal and added candidate
  selection coverage. A Linux Rust 1.85.0 container passed all 275 consumer
  tests with `--test-threads=1` and strict Clippy. The parallel suite continued
  to show the process-wide signal-mask limitation, so the general CI default
  and all-feature workspace test commands now use the same serial test mode as
  `integration-tests/run-local.sh`. Clean-source Linux evidence is in
  `raw local output (not retained)`.
- [x] Pushed the Linux signal-candidate and serialized-test correction. GitHub
  Actions later passed the general Rust test job on `b2e2c54e`; its required
  Docker matrix is tracked in the follow-up records below.
- [ ] MAN-01 through MAN-04 remain owner-input blocked as recorded above. No
  plan files were changed.

Run record:
`rlogs/2026/10/26-phase-19-ci-stack-signal-recovery__20261026T042217Z.rlog`.

## P19 required Docker matrix follow-up — 2026-10-26 (`b2e2c54e`)

- [x] On source revision `b2e2c54e90f922c6b28960c42b608d49a22e3801`, the
  Windows stack-runtime and general Rust test jobs passed. All 23 server-free
  gates passed on each Docker matrix attempt that reached the aggregate step.
- [x] The two live-scenario failures were locally reproduced successfully:
  `P5-V02-extensions` passed twice, and
  `REQ-001-request-reply-success` passed in 103 seconds with clean source.
  Evidence is archived under the scenario directories and manifest pointers
  refer to the latest local passes.
- [x] Archived all three GitHub job logs at
  `raw local output (not retained)`.
  The first matrix attempt failed at `P5-V02-extensions`; the second stopped at
  `REQ-001-request-reply-success` without a compiler or assertion diagnostic.
  The third attempt was terminated during `live-scenarios` after all server-
  free gates passed; GitHub reported `The operation was canceled`.
- [x] Pushed the local scenario evidence and retry record in `c24044f5`; the
  fresh push-triggered run `40299960470` is documented below. Its matrix failed
  in the all-feature workspace test gate before Docker-backed scenarios began.
- [ ] MAN-01 through MAN-04 remain owner-input blocked as recorded above. No
  plan files were changed; no tag or package was created or published.

## 2026-10-26 push-triggered CI follow-up on `c24044f5`

- GitHub Actions run `40299960470` checked
  `c24044f5376f2860121a95af032406ace41c1ab5`. The required Docker matrix passed
  `workspace-tests-default` and failed `workspace-tests-all-features` in the
  Linux stack-capture tests; Docker-backed gates and live scenarios did not
  start. The general test job failed its default workspace suite with the same
  lack of an available safe signal. Its all-feature suite and later checks were
  skipped.
- The Windows stack-runtime job failed the isolated named-thread regression.
  The stack dump omitted `jstack-worker` and contained two remaining thread
  entries with fallback names and unavailable native stacks. Inspection found
  the global capture deadline could be spent on earlier threads before the
  remaining thread names were read.
- Raw logs and job metadata are archived in
  `raw local output (not retained)`.
- A corrective worktree change now chooses Linux capture signals per target
  thread and preserves partial dumps when every safe signal is blocked. Windows
  capture now resolves names before stack walking, prioritizes named threads,
  and gives each thread a 100 ms unwind budget within the 5-second snapshot
  deadline. The fresh push-triggered validation and its remaining timeout gates
  are documented in the following section.
- Linux ARM64 Rust 1.85.0 focused stack tests passed (7/7), strict Clippy
  passed, and the full consumer library suite passed all 276 tests. The
  Windows GNU cross-check could not run because `x86_64-w64-mingw32-gcc` is
  unavailable in the container.
- MAN-01 through MAN-04 remain owner-input blocked. No plan files were edited;
  no tag or package was created or published.

## 2026-10-26 push-triggered CI revalidation on `fadc19d9`

- GitHub Actions run `40308005851` checked
  `fadc19d9a8934362fca20e8e9ecfce2be46ce4af`. The Windows stack-runtime
  regression passed.
- The general test job passed formatting, strict Clippy, both default and
  all-feature workspace suites, timezone-specific unique-ID coverage,
  differential self-tests and manifest validation, fuzz smoke, race/leak
  smoke, and Maven verify. GitHub canceled the job at its 60-minute limit
  during `cargo build -p rocketmq-classic --examples --release --locked`;
  benchmark and metadata steps were skipped.
- The Docker matrix passed both workspace suites and every server-free gate,
  including workspace check and Clippy, Java verify, release examples,
  benchmark build and run, package content, metadata, timezone-specific tests,
  differential live-coverage validation, and Docker toolchain validation. It
  entered `live-scenarios` at `2026-10-26T11:37:16Z`; GitHub canceled the
  aggregate step at `2026-10-26T13:32:06Z` when the job reached its
  180-minute limit. The captured log contains no scenario assertion failure.
  The full live scenario set and final evidence reconciliation did not finish.
- All three job logs and run metadata are archived in
  `raw local output (not retained)`.
- The general test job limit is raised from 60 to 90 minutes to give the
  release examples build more time. The matrix job limit is raised from 180 to
  GitHub's 360-minute maximum because its live scenario stage exceeded the
  prior limit. A fresh push-triggered workflow is required to validate these
  timeout changes and complete the live scenarios.

## 2026-10-26 CI follow-up on `485fb99a`

- GitHub Actions run `40331947315` checked
  `485fb99a43b739e5d936bd472efdee8b0ba40313`. The Windows stack-runtime job
  passed. The general test job passed formatting, strict Clippy, both
  workspace suites, timezone-specific unique-ID coverage, differential checks,
  fuzz and race/leak smoke, and Maven verify. It reached the release examples
  build, then was canceled at its 90-minute limit; benchmark and metadata steps
  were skipped.
- The Docker matrix passed both workspace suites and all server-free gates,
  including release examples, benchmark build and run, packaging, metadata,
  and Docker toolchain validation. Live scenarios `P3-V04` and `P5-V02-route`
  passed. `P5-V02-producer` failed its consecutive queue-offset assertion:
  the runner reported that Java and Rust did not start from the expected
  offsets. The matrix failed at that scenario after about 1 hour 53 minutes;
  the job did not reach its 360-minute limit.
- The CI log did not include the Java and Rust TSV probe rows, so the exact
  offset values are unavailable. An isolated local run of the old probe passed
  at `20261026T153132Z`. This points to an intermittent queue-visibility race
  as a likely cause, but the remote values cannot confirm that diagnosis.
- The P5 runner now waits up to 15 seconds for the Broker's visible maximum
  queue offset to reach Java's returned maximum before Rust sends. On timeout,
  it reports the last observed offset. The failure path also prints both TSV
  probe outputs. A clean focused run on commit `92cae47d` passed at
  `20261026T153933Z`; Java stored offsets 0–2 and Rust stored offsets 3–5.
  The manifest points to this clean-source evidence.
- Raw logs and run metadata are archived in
  `raw local output (not retained)`.
  The clean focused P5 evidence is in
  `raw local output (not retained)`.
- The general test limit is raised from 90 to 120 minutes because the release
  examples build was still compiling after 90 minutes. The Docker matrix stays
  at 360 minutes. A fresh push-triggered run is required to validate the P5
  offset wait, finish the release examples build, and continue the full live
  scenario inventory.

## 2026-10-26 CI follow-up on `5eb2cb24`

- GitHub Actions run `40352003113` checked `5eb2cb2450ec6dbb936bf2b69e7acbfbbb759edc`. The general test job passed in 55m41s, including both workspace suites, strict Clippy, Maven verify, release examples, benchmark compilation, and metadata checks. The Windows stack-runtime job passed in 7m06s.
- The Docker matrix passed all server-free gates and reached the live Phase 19 scenarios. `P5-V02-producer` and `P5-V02-consumer` both passed. The run failed during `P5-V02-extensions`; its captured output ends after the generic live-probe compilation message and contains no probe error or TSV rows. The matrix completed in 1h57m58s, well within its 360-minute limit.
- A focused clean-source run of `P5-V02-extensions` on `5eb2cb24` passed in 151 seconds, including Docker cleanup. Its evidence is `raw local output (not retained)`, and the manifest points to that run. The remote failure cause remains undetermined.
- To make another CI failure diagnosable, the extension runner now prints progress after Java/Rust probe builds and before each extension substep. The Docker workflow also uploads the run-local logs and P5 producer, consumer, and extension evidence on failure, with 14-day retention.
- Raw CI logs and run metadata are archived in `raw local output (not retained)`. The next push is needed to validate the additional diagnostics and complete the remaining live scenarios.

## 2026-10-26 CI follow-up on `bdae2cd2`

- GitHub Actions run `40372271928` checked
  `bdae2cd28bd9ab6be9d994f0b6c240ffdb1f3bc0`. The general test job passed
  in 62m47s and the Windows stack-runtime job passed in 5m51s. The Docker
  matrix stopped during `java-verify`, before live scenarios, because the
  push-consumer accessor fixture differed. The matrix failure artifact is
  archived in `raw local output (not retained)`.
- Reproducing the fixture in 30 fresh Temurin 27.0.1 Linux x86 JVMs produced
  two JSON variants. The drifting values were
  `consumerStatsSamplingAfterFirstSamplePullTPS` and
  `consumerStatsSamplingAfterPendingUpdatePullTPS`: one-millisecond wall-clock
  differences around RocketMQ's ten-second sample interval changed `2.0` to
  `1.9998000199980002`. The fixture generator now rounds those sampled rates to
  RocketMQ's two-decimal display precision. All 30 exact-toolchain generations
  match the checked-in fixture, and local offline Maven verification passes.
- The updated fixture probe and the full Docker matrix still need a fresh
  push-triggered CI run.


## 2026-10-26 Phase 19 current-revision local result

- [x] `413f735ad0ab41717acb9d76e2cb664b42076f4d` passed
  `ROCKETMQ_CLASSIC_TEST_ENV=local-disposable bash integration-tests/run-local.sh --all`.
  All 23 aggregate gates passed, including default/all-feature workspace
  suites, strict Clippy, Maven verification, offline race/fuzz/example/benchmark
  gates, timezone coverage, Docker profile preflight, the complete live
  scenario matrix, and final evidence reconciliation. The aggregate evidence is
  `raw local output (not retained)`.
- [x] The manifest reconciliation selected current clean-source PASS evidence
  for all 37/37 scenario rows. Post-run Docker queries found no Phase 19 project
  containers, volumes, or networks.
- [x] GitHub Actions CI was removed per the user's 2026-10-27 direction;
  remote CI is no longer a validation gate. Historical run `40383732405`
  started zero job steps and cites failed account payments or an insufficient
  Actions spending limit; its record remains at
  `raw local output (not retained)`.
- [ ] MAN-01 through MAN-04 remain BLOCKED pending owner-provided network,
  security, HA, and representative workload/SLO inputs. MAN-05 remains N/A per
  the current-tree source/API delta audit. Final compatibility/release approval
  remains blocked; no package, tag, or deployment was created.
