# Plan: Phase 7 — Producer core

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 7; execution index: [`README.md`](README.md).
> Translation: [`08-phase-07-producer-core.zh-tw.md`](08-phase-07-producer-core.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## Execution contract

- Port **all Java client 5.5.1 settings, features and control flows**, including defaults, validation, callbacks, errors, retries and lifecycle. Map Java source/method/configuration to Rust and unit-test IDs in both directions; no extra public Rust setting/feature and no missing Java client capability. Rust naming/ownership may differ, not observable semantics. Internal test seams stay private.
- Implement the **client only**. Java client 5.5.1 remains the oracle; **RocketMQ server V5_3_4 (5.3.4)** is the production/local Docker baseline, not the client source baseline. A server limitation does not authorize deleting a client feature; compare both clients against the same server and retain source/fixture coverage for unavailable success paths.
- **Phase 0–18 primarily use unit tests**, supplemented by Java golden/offline differential tests and small scripted loopback tests where transport requires them. No live Broker or Docker is a phase prerequisite. Broker, TLS/auth cluster, cross-language, end-to-end and server stress execution belongs to [Phase 19](20-phase-19-docker-local-validation.md); non-local checks become owner-executed manual items there. Phase acceptance is not final compatibility/release acceptance.

## 1. Goal and scope

Deliver sync/future/oneway/batch sends with Java-compatible validation, queue selection, compression and retry budgets. Transaction and request/reply remain later phases.

## 2. Prerequisites and current-state analysis

Phases 0–6 are accepted. Add producer and thin facade crates now. Phase 16 adds namespace/security/trace parity: passing this phase alone does not satisfy the master-plan v0.1 namespace/security release claims.

- Read/complete first: [Phase 6](07-phase-06-runtime-routing-heartbeat.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `DefaultMQProducer`
- `DefaultMQProducerImpl`
- `Validators`
- `MessageClientIDSetter`
- `MessageBatch`
- `TopicPublishInfo`
- `MQFaultStrategy`
- `latency fault classes`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-producer/Cargo.toml
crates/rocketmq-classic-producer/src/producer.rs
crates/rocketmq-classic-producer/src/config.rs
crates/rocketmq-classic-producer/src/send.rs
crates/rocketmq-classic-producer/src/selector.rs
crates/rocketmq-classic-producer/src/fault.rs
crates/rocketmq-classic-producer/src/batch.rs
crates/rocketmq-classic-common/src/compression/
crates/rocketmq-classic/Cargo.toml
crates/rocketmq-classic/src/lib.rs
crates/rocketmq-classic-producer/tests/producer.rs
examples/producer.rs
```

Centralize defaults: 3000 ms send timeout, 4096-byte compression threshold, sync retries=2, async retries=2, retryAnotherBrokerWhenNotStoreOK=false, max message size=4 MiB and default queue count=4. Use one deadline from public entry through routing/compression/attempts. Rust send().await represents blocking-result Java send semantics; expose a separately named future-returning async-mode method if its Java retry policy differs. Do not silently conflate the two modes just because both Rust methods are async.

## 4. Ordered implementation tasks

### 4.1 P7-01 / P7-02 — Create lifecycle and validate inputs before I/O

Build validated configuration and explicit start/shutdown, register with the Phase 6 runtime and roll back failed start. Add facade re-exports and a runnable example without exposing internal crates. Register root `examples/*.rs` explicitly with `[[example]]` paths in the facade Cargo.toml so `cargo build -p rocketmq-classic --examples` actually builds them; follow this convention for later examples. Port topic/body/property/system-topic validators from Java, testing empty body versus missing body and encoded size boundaries. Keep user Message immutable through retries.

### 4.2 P7-03 / P7-04 — Acquire routes and choose queues

Use cached publish info, then refresh; follow Java default-topic fallback/queue-count behavior only where the pinned client uses it. Return a typed no-route error when no writable queue exists. Implement round-robin overflow-safe selection and avoidance of the previous failed broker, then explicit queue and custom selector paths with membership validation. Selector ordering and retry behavior need dedicated Java fixtures.

### 4.3 P7-05 / P7-06 / P7-07 — Build one immutable send payload

Add Java-compatible unique-ID format and fixtures, not a random UUID substitution. Build headers/properties/sysFlags with original timestamp/ID retained across eligible retries. Compress exactly at the source-confirmed threshold rule using the selected flag/backend; compare decompressed semantics when compressed bytes are not deterministic. Keep original body for hooks, cache compressed bytes once and avoid recursive compression. Implement V2 selection as Java does.

### 4.4 P7-08 / P7-09 / P7-11 — Implement mode-specific attempts and results

Extract a pure retry-decision table from Java exceptions/statuses and test it first. Total sync attempts are 1+retries; async and oneway follow their own Java policies. After each attempt update fault state, preserve Broker code/remark and consume only remaining deadline. A storage-status response is a SendResult when Java returns one, not automatically an exception. Document timeout-after-store duplicate delivery and no exactly-once guarantee.

### 4.5 P7-10 — Validate and encode batches

Capture the 5.5.1 batch-validation changes in negative Java fixtures: empty batch, mixed topics, unsupported delay/retry/transaction forms as confirmed by source, inconsistent properties and encoded total-size limits. Reject before network I/O. Encode the Java batch body layout, not concatenated single-message Remoting commands; validate both aggregate and element lengths. Test boundary sizes after property/ID insertion.

### 4.6 P7-12 — Add latency fault behavior only after basic sends pass

Port latency isolation tables, unavailable-broker fallback and optional detector behavior from the pinned source using an injectable clock in pure tests. Any detector task belongs to Producer shutdown. Register first-attempt failure and route-change cases for the Phase 19 two-Broker profile; do not replace compatibility with a new circuit-breaker library.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Run these checks without Docker or a live Broker; use Java fixtures, controlled clocks and scripted peers. Shared commands and evidence rules are in the execution index.

- [x] **P7-V01** — Default values and validation boundaries match Java, including threshold−1/threshold/threshold+1 and max-size ±1.

- [x] **P7-U01** — Scripted two-address failures and Java decision fixtures verify retries, statuses, deadlines, body/ID preservation and repeated lifecycle cleanup.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-producer --locked
cargo build -p rocketmq-classic --examples --locked
```

### 5.2 Phase 19 live-test handoff

Retain these case IDs and behavior requirements for Phase 19 execution against Docker server 5.3.4; they are not this phase's pass conditions and must not be marked PASS early.

- [ ] **P7-V02** — Sync, async-mode, oneway, explicit queue, selector and valid/invalid batch scenarios pass Java differential comparison.
- [ ] **P7-V03** — Two-Broker failure tests prove attempt counts, status classification, deadline exhaustion and unmodified original body/unique ID.
- [ ] **P7-V04** — Sustained local sends and repeated start/shutdown leave zero pending requests, connections and producer-owned tasks.

## 6. Acceptance and handoff

Producer compatibility is the gate before advanced Consumer work. Record any postponed namespace/hook exposure honestly; Phase 8 reuses the common compression registry rather than inventing another one.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 8](09-phase-08-message-decode-pull-results.md).
