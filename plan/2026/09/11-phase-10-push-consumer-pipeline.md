# Plan: Phase 10 — Push consumer core pipeline

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 10; execution index: [`README.md`](README.md).
> Translation: [`11-phase-10-push-consumer-pipeline.zh-tw.md`](11-phase-10-push-consumer-pipeline.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## Execution contract

- Port **all Java client 5.5.1 settings, features and control flows**, including defaults, validation, callbacks, errors, retries and lifecycle. Map Java source/method/configuration to Rust and unit-test IDs in both directions; no extra public Rust setting/feature and no missing Java client capability. Rust naming/ownership may differ, not observable semantics. Internal test seams stay private.
- Implement the **client only**. Java client 5.5.1 remains the oracle; **RocketMQ server V5_3_4 (5.3.4)** is the production/local Docker baseline, not the client source baseline. A server limitation does not authorize deleting a client feature; compare both clients against the same server and retain source/fixture coverage for unavailable success paths.
- **Phase 0–18 primarily use unit tests**, supplemented by Java golden/offline differential tests and small scripted loopback tests where transport requires them. No live Broker or Docker is a phase prerequisite. Broker, TLS/auth cluster, cross-language, end-to-end and server stress execution belongs to [Phase 19](20-phase-19-docker-local-validation.md); non-local checks become owner-executed manual items there. Phase acceptance is not final compatibility/release acceptance.

## 1. Goal and scope

Implement bounded pull scheduling, ProcessQueue and concurrent listener/retry semantics. Multi-client assignment is intentionally delivered by the immediately following Phase 11.

## 2. Prerequisites and current-state analysis

Phases 7–9 are accepted. The master plan places rebalance after Push startup, creating a dependency gap. Resolve it with a private assignment-activation seam and test-only fixed assignment: do not expose a fake production rebalance implementation. The complete public Push startup gate is cumulative across Phases 10–11.

- Read/complete first: [Phase 9](10-phase-09-pull-offset-store.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `DefaultMQPushConsumerImpl`
- `PullMessageService`
- `PullRequest`
- `ProcessQueue`
- `ConsumeMessageConcurrentlyService`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-consumer/src/push/consumer.rs
crates/rocketmq-classic-consumer/src/push/pull_service.rs
crates/rocketmq-classic-consumer/src/process_queue.rs
crates/rocketmq-classic-consumer/src/consume/concurrent.rs
crates/rocketmq-classic-consumer/tests/push_pipeline.rs
docs/architecture/ADR-0003-consumer-process-queue.md
```

Use `BTreeMap<queue_offset, message>` for ordered local state and explicit cached count/bytes, dropped flag, assignment generation, last-pull time, consume state and lock fields. In-flight listener batches remain accounted for until their terminal consume outcome; duplicate pulls cannot double-count. A pull/consume work item carries the ProcessQueue generation. Late work cannot mutate replacement queues or commit after losing ownership. The next committed offset is the lowest unresolved offset, or the Java-compatible next offset if no unresolved message remains.

## 4. Ordered implementation tasks

### 4.1 P10-01 / P10-02 — Validate configuration and implement reversible startup

Validate group/model/from-policy/subscriptions/concurrency/batch/listener before registration. Follow the master-plan 12-step startup order; create the rebalance integration state now but activate real allocation only in Phase 11. Select/load local or remote store, construct consume service, register/start runtime, update routes and heartbeat. Each acquired registration/task has rollback; failed startup must not expose a Running handle. Reject unsupported listener modes explicitly until Phase 12.

### 4.2 P10-03 — Implement ProcessQueue operations first

Add insert/deduplicate, take-batch, complete/requeue and drop operations with short atomic state transitions. Keep offsets ordered and counters exact when bodies are shared. Snapshot data before invoking user code; no global or synchronous state lock crosses callbacks. Write tests for out-of-order completion, gaps, duplicate insertion and dropped-generation completion before adding scheduling; document the ownership model in ADR-0003.

### 4.3 P10-04 / P10-08 — Build one bounded scheduler

Maintain one logical scheduled pull per queue/generation with immediate/delayed wakeups, pause/resume and cancellation. Use a bounded worker set and due-time queue rather than detached spawn per iteration. Apply Java count/MiB/span/backlog thresholds before pulling; in-flight listener messages count toward pressure. Publish tracing counters for delayed pulls and backlog without message-body logging.

### 4.4 P10-05 — Handle all pull outcomes without skipping messages

FOUND updates the next pull offset, inserts messages and submits bounded consume work. NO_NEW/NO_MATCHED applies Java tag-offset correction only when safe with unresolved messages. OFFSET_ILLEGAL drops the generation, schedules corrected-offset persistence and requests removal via the assignment seam; never immediately resurrect it. Late pull responses recheck generation before insertion. Transient pull failures reschedule with the source-confirmed delay.

### 4.5 P10-06 / P10-07 — Complete concurrent batches and retries safely

Model Java success/reconsume-later and ackIndex partial acknowledgement explicitly. Clustering failures use send-back; only remove failed messages after Broker accepts retry responsibility, otherwise retain/requeue locally. Implement Java fallback retry-message production if send-back fails, preserving origin IDs, reconsume count, delay and max-reconsume properties. Broadcasting failure follows its distinct Java semantics, not the clustering retry path. Listener error/panic must not become success; isolate execution and retain messages. Leave a hook context seam; actual public hooks arrive in Phase 16.

### 4.6 P10-09 — Quiesce before shutdown

Stop new pull/consume work, drop assignments, cancel long polls and wait a bounded time for owned listener work. Persist only completed offsets, retain unresolved work for redelivery, then unregister and join tasks. Slow callbacks cannot block the shared reader or other queues. Document cancellation/panic policy and ensure shutdown does not claim successful processing of abandoned messages.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Run these checks without Docker or a live Broker; use Java fixtures, controlled clocks and scripted peers. Shared commands and evidence rules are in the execution index.

- [x] **P10-V01** — ProcessQueue duplicate/gap/out-of-order completion tests prove counters and no commit past unresolved offsets.
- [x] **P10-V02** — Paused-time scheduler tests prove bounded tasks, threshold behavior, dropped-generation rejection and retry retention on send-back failure.
- [x] **P10-V04** — Fault injection at every startup step and shutdown with slow callbacks leave no lost unacknowledged work or owned-task leaks.

- [x] **P10-U01** — Private fixed-assignment tests inject Java message fixtures and scripted send-back responses to verify listener success/partial/failure, retry and DLQ decisions.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-consumer --test push_pipeline --locked
```

### 5.2 Phase 19 live-test handoff

Retain these case IDs and behavior requirements for Phase 19 execution against Docker server 5.3.4; they are not this phase's pass conditions and must not be marked PASS early.

- [ ] **P10-V03** — Test-only fixed assignment consumes Java-produced messages and exercises listener success/partial/failure, retry count and configured DLQ transition against the local Broker.

## 6. Acceptance and handoff

The pipeline may be merged with public Push marked PARTIAL; proceed directly to Phase 11 before advertising normal Push startup or mixed-group compatibility. Keep fixed assignment private to tests.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 11](12-phase-11-rebalance.md).
