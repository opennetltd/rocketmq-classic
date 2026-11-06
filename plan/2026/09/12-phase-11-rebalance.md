# Plan: Phase 11 — Rebalance and assignment lifecycle

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 11; execution index: [`README.md`](README.md).
> Translation: [`12-phase-11-rebalance.zh-tw.md`](12-phase-11-rebalance.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## Execution contract

- Port **all Java client 5.5.1 settings, features and control flows**, including defaults, validation, callbacks, errors, retries and lifecycle. Map Java source/method/configuration to Rust and unit-test IDs in both directions; no extra public Rust setting/feature and no missing Java client capability. Rust naming/ownership may differ, not observable semantics. Internal test seams stay private.
- Implement the **client only**. Java client 5.5.1 remains the oracle; **RocketMQ server V5_3_4 (5.3.4)** is the production/local Docker baseline, not the client source baseline. A server limitation does not authorize deleting a client feature; compare both clients against the same server and retain source/fixture coverage for unavailable success paths.
- **Phase 0–18 primarily use unit tests**, supplemented by Java golden/offline differential tests and small scripted loopback tests where transport requires them. No live Broker or Docker is a phase prerequisite. Broker, TLS/auth cluster, cross-language, end-to-end and server stress execution belongs to [Phase 19](20-phase-19-docker-local-validation.md); non-local checks become owner-executed manual items there. Phase acceptance is not final compatibility/release acceptance.

## 1. Goal and scope

Complete public Push startup and deterministic queue assignment, including safe queue removal/addition under concurrent pulls and callbacks.

## 2. Prerequisites and current-state analysis

Phase 10 private activation/removal seam and Phase 9 offset-boundary RPCs are ready. Orderly lock-specific integration remains pending until Phase 12; do not advertise orderly ownership yet.

- Read/complete first: [Phase 10](11-phase-10-push-consumer-pipeline.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `RebalanceImpl`
- `RebalancePushImpl`
- `RebalanceService`
- `AllocateMessageQueueAveragely`
- `AllocateMessageQueueAveragelyByCircle`
- `consistent-hash and machine-room allocation classes`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-consumer/src/rebalance/mod.rs
crates/rocketmq-classic-consumer/src/rebalance/strategy/
crates/rocketmq-classic-consumer/src/rebalance/service.rs
crates/rocketmq-classic-consumer/src/push/consumer.rs
crates/rocketmq-classic-consumer/tests/rebalance.rs
examples/push_consumer.rs
```

One rebalance execution mutates a consumer at a time; wakeups coalesce. Sort MessageQueue and client IDs using Java-compatible comparisons before strategy calls. Build desired assignment from a consistent snapshot, then reconcile against current queue generations. A failed consumer-list query is not an authoritative empty group and must not cause arbitrary reassignment. No local state is removed until its ownership/offset obligations are fulfilled.

## 4. Ordered implementation tasks

### 4.1 P11-01 / P11-02 — Implement average allocation as pure logic

Use the master-plan strategy signature and fixture expected errors/empty results for invalid input/current-client-missing cases. Port Java index/start/range arithmetic exactly, including fewer queues than consumers. Test all clients together: assignments are disjoint and cover eligible queues, not just the current client result.

### 4.2 P11-03 — Inventory and implement all Java allocation strategies

Inventory and implement every Java 5.5.1 public allocation strategy, including circle and all source-confirmed consistent-hash/machine-room variants. Match hash bytes, virtual nodes, sorting, defaults and errors. No deferred or excluded strategy is acceptable; unit-test every strategy against Java fixtures.

### 4.3 P11-04 / P11-05 — Resolve clustering and broadcasting assignments

Broadcasting assigns all subscribed queues and uses local offsets. Clustering fetches consumer IDs, sorts them and invokes the selected strategy; distinguish unavailable, empty and missing-current-client results from fixtures. Update subscription snapshots and retry failed fetches on the next owned cycle. Integrate startup step 12 so real rebalance replaces Phase 10 test assignment.

### 4.4 P11-06 — Remove an assignment in ownership order

Mark dropped before cancelling pending pulls; stop new consume work, preserve unresolved offsets and persist safe progress. If cleanup fails, retain removal state for retry rather than deleting evidence. A replacement generation must not receive old listener/pull completion. Install the orderly unlock seam now, activating it in Phase 12. Remove registry state by identity only after cleanup succeeds.

### 4.5 P11-07 — Compute a safe initial offset and activate

Remove stale in-memory offsets only at the Java-defined point, then read the authoritative stored offset. If absent, implement each ConsumeFromWhere policy using Phase 9 RPCs, including retry-topic and timestamp/time-zone behavior captured from Java. Failure to obtain a start offset must not become zero. Create one ProcessQueue generation, activate one PullRequest and roll back if activation fails. Phase 12 inserts Broker locking before orderly activation.

### 4.6 P11-08 / P11-09 — Wire triggers and prove race safety

Own one periodic service with immediate startup, subscription-change and NOTIFY_CONSUMER_IDS_CHANGED wakeups. Use source-confirmed timing and bounded/coalesced signals. Reject work after shutdown begins. Add barrier-driven remove-versus-pull, remove-versus-listener, concurrent-trigger and rapid join/leave tests; finish the real public Push example and facade exports.

### 4.7 P11-10 — Complete Pull consumer rebalance ownership

Port the Pull consumer's Java allocation-strategy and `enableRebalance` defaults, queue-listener registration, topic/subscription registration, and live in-balance query. Wire these through the same runtime-owned serialized rebalance service: broadcasting assigns all subscribed queues; clustering uses broker membership and retains the current assignment when membership is unavailable. Namespace queue names consistently at subscription and query boundaries. Persist safe offsets before removing queues, retain failed removals for retry, notify listeners after assignment changes, and fence callbacks during shutdown. Keep this Pull facade work under P11-U02 and in the Phase 11 compatibility matrix.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Run these checks without Docker or a live Broker; use Java fixtures, controlled clocks and scripted peers. Shared commands and evidence rules are in the execution index.

- [x] **P11-V01** — Java/Rust allocations match for 0/1/N queues, 1/N clients, 10q/3c, missing client, Unicode order and every Java strategy.
- [x] **P11-V02** — Initial-offset policies match Java; query/persist failure never resets to zero or removes a live replacement.

- [x] **P11-U01** — Java allocation fixtures, scripted membership changes and barrier tests verify join/leave convergence, startup, retry, safe offsets and shutdown ownership.
- [x] **P11-U02** — Pull rebalance defaults, public listener/registration/query APIs, namespace mapping, broadcasting/clustering assignments, offset-safe removal and unavailable-membership retention are covered by local tests.

### 5.1 Local commands after implementation

The implementation run executed the focused commands below. Current package/workspace/Maven gate outcomes are recorded in the [Phase 11 compatibility matrix](../../../docs/compatibility/PHASE_11_OFFLINE_METHOD_MATRIX.md) and the later [Phase 11 continuation log](../../../rlogs/2026/09/12-phase-11-rebalance__20261012T011432Z.rlog).

```bash
cargo test -p rocketmq-classic-consumer --test rebalance --locked
cargo build -p rocketmq-classic --examples --locked
```

### 5.2 Phase 19 live-test handoff

Retain these case IDs and behavior requirements for Phase 19 execution against Docker server 5.3.4; they are not this phase's pass conditions and must not be marked PASS early.

- [ ] **P11-V03** — Local mixed Java/Rust group join/leave converges within a documented timeout with no persistent duplicate queue ownership; transient redelivery remains allowed.
- [ ] **P11-V04** — Real public Java producer→Rust Push and Rust producer→Java Push paths pass with retries/offsets; repeated shutdown leaves no scheduler tasks.

## 6. Acceptance and handoff

Close the cumulative Phase 10–11 public Push gate. Keep orderly rows PARTIAL until lock renewal, expiry and callback completion fencing are accepted in Phase 12.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 12](13-phase-12-orderly-consumption.md).
