# Plan: Phase 6 — Client runtime, routing and heartbeat

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 6; execution index: [`README.md`](README.md).
> Translation: [`07-phase-06-runtime-routing-heartbeat.zh-tw.md`](07-phase-06-runtime-routing-heartbeat.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## Execution contract

- Port **all Java client 5.5.1 settings, features and control flows**, including defaults, validation, callbacks, errors, retries and lifecycle. Map Java source/method/configuration to Rust and unit-test IDs in both directions; no extra public Rust setting/feature and no missing Java client capability. Rust naming/ownership may differ, not observable semantics. Internal test seams stay private.
- Implement the **client only**. Java client 5.5.1 remains the oracle; **RocketMQ server V5_3_4 (5.3.4)** is the production/local Docker baseline, not the client source baseline. A server limitation does not authorize deleting a client feature; compare both clients against the same server and retain source/fixture coverage for unavailable success paths.
- **Phase 0–18 primarily use unit tests**, supplemented by Java golden/offline differential tests and small scripted loopback tests where transport requires them. No live Broker or Docker is a phase prerequisite. Broker, TLS/auth cluster, cross-language, end-to-end and server stress execution belongs to [Phase 19](20-phase-19-docker-local-validation.md); non-local checks become owner-executed manual items there. Phase acceptance is not final compatibility/release acceptance.

## 1. Goal and scope

Own and share runtime resources by client identity, provide coherent routing and heartbeat snapshots, and prove complete startup rollback/shutdown.

## 2. Prerequisites and current-state analysis

Phase 5 RPCs are accepted. Consumer pull/rebalance implementations do not exist yet; runtime exposes narrow registration and wakeup contracts without inventing a Consumer scheduler here.

- Read/complete first: [Phase 5](06-phase-05-typed-rpc-api.md).

Source mapping correction (2026-10-09): pinned `MQClientManager.getOrCreateMQClientInstance` first reads the client-ID table and returns an existing instance; only a missing key reaches `computeIfAbsent`. Later config and hook arguments do not replace or conflict-check the first runtime. The former `localServeAddr` phrase belongs to Proxy `ProxyConfig.initData`, outside the client-only phase; use the source-backed `ClientConfig.clientIP`/`NetworkUtil.getLocalAddress` behavior instead. See the [requirement audit](../../docs/compatibility/PHASE_6_REQUIREMENT_AUDIT.md).

### 2.1 Java references to inspect

- The source mapping correction above is grounded in the pinned 5.5.1 artifact; continue resolving each listed class/method and call site using the pinned-source rules in the execution index before changing its behavior.
- `MQClientManager`
- `MQClientInstance`
- `ClientConfig`
- `TopicPublishInfo`
- `ClientRemotingProcessor`
- `RebalanceService`
- `PullMessageService`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-core/src/config.rs
crates/rocketmq-classic-core/src/runtime/manager.rs
crates/rocketmq-classic-core/src/runtime/runtime.rs
crates/rocketmq-classic-core/src/runtime/tasks.rs
crates/rocketmq-classic-core/src/route/cache.rs
crates/rocketmq-classic-core/src/heartbeat.rs
crates/rocketmq-classic-core/tests/runtime.rs
docs/architecture/ADR-0002-client-runtime-sharing.md
```

ClientRuntimeManager is an explicit shareable handle, not an unresettable global singleton. The facade later shares a default manager while allowing a private test-only manager override. A client-ID entry owns single initialization. Match Java's first-mapping behavior: return the existing runtime for a repeated ID even when later config or hook inputs differ; retain the first runtime's config/property/hook snapshot and leave the later caller config unchanged. Do not reject a later same-ID call for a configuration conflict. Model Created→Starting→Running→ShuttingDown→Stopped and startup failure. Group registration is identity-based and separate from Arc strong counts.

## 4. Ordered implementation tasks

### 4.1 P6-01 / P6-02 — Build identity and single-flight creation

Fixture-test ClientConfig ID construction, including instance/unit components, and source-backed automatic `clientIP` initialization. `ProxyConfig.localServeAddr` is server-side and outside this client phase. Use per-key OnceCell or entry ownership before constructing sockets/tasks. Allow unrelated keys to initialize concurrently. Failed initialization removes only its own entry and permits a documented retry. Keep runtime ownership explicit in ADR-0002.

### 4.2 P6-03 / P6-04 — Implement lifecycle and group leases

Make start/shutdown transitions serialized and idempotent where documented. Producer/consumer registrations return an identity token for rollback/unregister; reject duplicate groups according to Java. One producer shutting down must not stop a runtime still used by another registration. Avoid strong-reference cycles between registries and runtime handles.

### 4.3 P6-05 / P6-08 — Publish coherent route snapshots

Fetch without holding cache locks, convert route fully, then replace a coherent snapshot of topic/broker/publish/subscribe state. Use Phase 1 conversions, including static/ordered routes. Implement master/slave publish/subscribe resolution and VIP port transformation as pure functions with checked ports and IPv6 support. Do not discard a valid cached route on a transient refresh error unless Java requires it.

### 4.4 P6-06 / P6-07 — Own periodic tasks and heartbeat snapshots

Use centralized defaults: route poll 30000 ms, heartbeat 30000 ms, offset persistence 5000 ms; verify initial delays and missed-tick behavior from Java. Each task has cancellation and join ownership. Offset ticks call registered consumer persistence capabilities only when present. Construct heartbeat from a snapshot outside locks; test subscription versions, V1/V2 configuration, unregister and producer-only/consumer-only combinations.

### 4.5 P6-09 / P6-10 — Register inbound slots and roll back failures

Route inbound check-transaction by producer/body group; consumer-change notifications wake runtime-wide rebalance (the header group is only logged); reset-offset, consumer-status, running-info and direct-consume by consumer group; and reply-push by correlation identity. Until an owning phase activates a feature handler, return an explicit supported-protocol error, never fake success. Inject failures after registration, transport start and each task start; reverse owned effects and join all work. Last-registration shutdown unregisters from Brokers before closing transport within a bounded deadline.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Run these checks without Docker or a live Broker; use Java fixtures, controlled clocks and scripted peers. Shared commands and evidence rules are in the execution index.

- [x] **P6-V01** — Many concurrent get_or_create calls for one ID yield one identity, one task set and one socket pool; different IDs are not globally serialized.
- [x] **P6-V02** — Stale failed creation and unregister tokens cannot remove replacements; repeated IDs return the first Java runtime and retain its config/hook despite later differing inputs.
- [x] **P6-V03** — Paused-time timers, route snapshot readers and all startup-failure points leave no partial state or leaked tasks.

- [x] **P6-U01** — Scripted responses and paused time verify heartbeat/subscription/unregister bytes and independent shutdown of shared users.

### 5.1 Local commands after implementation

Verified 2026-10-10 after the direct default-topic-route API differential, ordered NameServer provider parity, and nullable NameServer address accessor oracle. The full locked workspace/all-target suite, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. These are local checks, not a live Broker run; **P6-V04 remains a Phase 19 handoff**, not a Phase 6 gate. The broader compatibility audit still records actual Linux/Windows/BSD runtime address enumeration and residual source/input branches as open. See the [requirement audit](../../docs/compatibility/PHASE_6_REQUIREMENT_AUDIT.md) for per-ID evidence and fixture provenance.

```bash
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
mvn -o -q -Dmaven.repo.local=/private/tmp/p6-maven-repository -f compat/java-reference/pom.xml verify
python3 tests/differential/compare.py --self-test
python3 tests/differential/compare.py --validate-manifest
cargo fmt --all -- --check
git diff --check
```

### 5.2 Phase 19 live-test handoff

Retain these case IDs and behavior requirements for Phase 19 execution against Docker server 5.3.4; they are not this phase's pass conditions and must not be marked PASS early.

- [ ] **P6-V04** — Local Broker sees correct heartbeat/subscription changes and unregister; shutting one shared user leaves the other functional.

## 6. Acceptance and handoff

Phase 7 receives lifecycle registration, route resolution and shared transport ownership. No stub handler may be marked COMPATIBLE, and the matrix names its activation phase.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 7](08-phase-07-producer-core.md).
