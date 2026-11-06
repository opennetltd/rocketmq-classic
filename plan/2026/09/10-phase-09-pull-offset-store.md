# Plan: Phase 9 — Pull consumer and offset stores

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 9; execution index: [`README.md`](README.md).
> Translation: [`10-phase-09-pull-offset-store.zh-tw.md`](10-phase-09-pull-offset-store.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## Execution contract

- Port **all Java client 5.5.1 settings, features and control flows**, including defaults, validation, callbacks, errors, retries and lifecycle. Map Java source/method/configuration to Rust and unit-test IDs in both directions; no extra public Rust setting/feature and no missing Java client capability. Rust naming/ownership may differ, not observable semantics. Internal test seams stay private.
- Implement the **client only**. Java client 5.5.1 remains the oracle; **RocketMQ server V5_3_4 (5.3.4)** is the production/local Docker baseline, not the client source baseline. A server limitation does not authorize deleting a client feature; compare both clients against the same server and retain source/fixture coverage for unavailable success paths.
- **Phase 0–18 primarily use unit tests**, supplemented by Java golden/offline differential tests and small scripted loopback tests where transport requires them. No live Broker or Docker is a phase prerequisite. Broker, TLS/auth cluster, cross-language, end-to-end and server stress execution belongs to [Phase 19](20-phase-19-docker-local-validation.md); non-local checks become owner-executed manual items there. Phase acceptance is not final compatibility/release acceptance.

## 1. Goal and scope

Provide explicit pull and crash-safe offset persistence before any Push scheduler. Pulling a batch must not implicitly acknowledge it.

## 2. Prerequisites and current-state analysis

Phase 8 decoding and Phase 6 runtime registration are accepted. Implement min/max/search/earliest-time RPCs now because Phase 11 start-offset policies need them; Phase 17 reuses these instead of delaying them.

- Read/complete first: [Phase 8](09-phase-08-message-decode-pull-results.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `Pull consumer implementations`
- `PullAPIWrapper`
- `OffsetStore`
- `RemoteBrokerOffsetStore`
- `LocalFileOffsetStore`
- `MQClientAPIImpl offset boundary methods`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-consumer/src/pull/
crates/rocketmq-classic-consumer/src/offset/mod.rs
crates/rocketmq-classic-consumer/src/offset/remote.rs
crates/rocketmq-classic-consumer/src/offset/local.rs
crates/rocketmq-classic-core/src/api/consumer.rs
crates/rocketmq-classic-protocol/src/header/
crates/rocketmq-classic-consumer/tests/offsets.rs
examples/pull_consumer.rs
```

Use an OffsetStore trait because there are two real implementations. Distinguish Found(offset), NotFound and failure internally, converting Java sentinel values only at the compatibility boundary. Remote/local stores share update/read-mode/increase-only semantics where Java does. State transitions for freeze or reset are atomic under the same short lock as offset update; never hold it across RPC/file I/O. Persist snapshots, not live locked maps.

## 4. Ordered implementation tasks

### 4.1 P9-01 — Specify and test offset transitions

Implement load, update, update-and-freeze when source requires it, read by mode, persist one/all, remove, snapshot/clone and update-to-Broker. Write tables for memory-only, store-only and memory-first reads, missing values, stale increase-only updates and reset/freeze ordering. Expose typed outcomes so a timeout can never be mistaken for no stored offset.

### 4.2 P9-02 — Implement the remote store

Use Phase 5 query/update RPCs and runtime broker resolution. Read modes determine when cache is populated and when Broker is queried. Match sync versus oneway persistence call sites; oneway success means dispatched, not durably acknowledged. Failed persistence retains in-memory progress for retry and does not remove an offset entry prematurely.

### 4.3 P9-03 — Persist local offsets without corrupting the previous file

Fixture the Java JSON layout, `rocketmq.client.localOffsetStoreDir` process-property override, default path derivation and backup recovery. Expose only that Java-equivalent process-level root setting; keep explicit root injection private for tests. Validate/encode group and client-ID path components against traversal. Write a unique temporary file in the same directory, flush/sync it, retain the last valid backup, rename atomically and sync the directory where supported. On malformed primary, use a valid backup; if both are unreadable return an error instead of resetting to zero. Define single-writer ownership or reject concurrent writers to one file.

### 4.4 P9-04 — Expose explicit pull and offset control

Validate queue, expression, nonnegative requested offset/count/response size and timeout/suspend relationships before I/O, preserving Java sentinel handling only in offset-store APIs. Expose Java's custom `maxSizeInBytes` pull overload alongside the default `Integer.MAX_VALUE`. Resolve suggested broker per queue and apply pull flags/commit-offset rules from Java. Return Phase 8 decoded outcomes without committing offsets. Register/unregister the pull consumer cleanly, expose configured/selected offset-store access, and expose explicit persist/commit operations through the facade.

### 4.5 P9-05 — Add offset-boundary primitives for later rebalance

Add typed header/response fixtures and single-attempt RPCs for search offset, min/max offset and earliest store time. Keep timestamp units signed Java milliseconds and distinguish empty queue from failed lookup. The runnable example pulls, processes successfully, updates the next offset and then persists; document at-least-once recovery when a process crashes before persistence.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Run these checks without Docker or a live Broker; use Java fixtures, controlled clocks and scripted peers. Shared commands and evidence rules are in the execution index.

- [x] **P9-V01** — All read/update/freeze modes match Java, including missing offsets, increase-only races and transport failure distinction.
- [x] **P9-V02** — Fault injection before/after each local-file write/rename step preserves a recoverable old or new snapshot; corrupt primary/backup and path traversal are tested.

- [x] **P9-U01** — Scripted pull/offset responses and temporary-file restart tests verify every pull status, next-offset recovery, broker selection and boundary RPC.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-consumer --test offsets --locked
cargo build -p rocketmq-classic --examples --locked
```

### 5.2 Phase 19 live-test handoff

Retain these case IDs and behavior requirements for Phase 19 execution against Docker server 5.3.4; they are not this phase's pass conditions and must not be marked PASS early.

- [ ] **P9-V03** — Local Broker manual pull/restart resumes from the persisted next offset; all pull statuses, suggested broker and boundary RPCs match Java.

## 6. Acceptance and handoff

Phase 10 receives an explicit pull wrapper and durable stores. Phase 11 can compute initial offsets without depending on Phase 17.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 10](11-phase-10-push-consumer-pipeline.md).
