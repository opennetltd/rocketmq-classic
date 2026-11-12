# Plan 02 — Match Java Pull-RPC Fan-Out With Per-Request Owned Push Dispatch

## 1. Goal and Scope

Replace the fixed `PULL_RPC_WORKER_COUNT = 4` / `PULL_RPC_QUEUE_CAPACITY = 64` Pull-RPC worker pool in the Push consumer with **admission-immediate, per-request owned task dispatch**, so the number of concurrently outstanding `PULL_MESSAGE` requests scales with the assigned queue count the way Java 5.5.1's async-callback fan-out does.

In scope:

- `crates/rocketmq-classic-consumer/src/push/pull_service.rs` Pull-RPC dispatch internals;
- one admission call site in `src/push/consumer/runtime.rs`;
- unit tests plus one new scripted-peer integration test;
- compatibility docs, one ADR and an execution log.

Out of scope: the POP worker model (already per-queue owned tasks), the consume-service pool, the Phase 6 runtime `PullMessageService` FIFO worker and its unbounded queue, public API/settings (none added or removed), live Broker scenarios (Phase 19).

## 2. Context

### 2.1 Java 5.5.1 reference behavior (pinned source)

| Java element | Pinned fact |
| --- | --- |
| `PullMessageService` | One `ServiceThread` plus an **unbounded** `LinkedBlockingQueue<MessageRequest>`; delayed requests come from a single-thread scheduler (`PullMessageServiceScheduledThread`) that re-enqueues into the same FIFO. |
| `DefaultMQPushConsumerImpl.pullMessage(PullRequest)` | Ends in `pullAPIWrapper.pullKernelImpl(..., BROKER_SUSPEND_MAX_TIME_MILLIS = 20_000, CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 30_000, CommunicationMode.ASYNC, pullCallback)`. The dispatch thread returns immediately, so **every assigned queue can hold one outstanding long-poll at the same time**. |
| Response handling | `NettyRemotingAbstract.processResponseCommand` → `executeInvokeCallback` → client `getCallbackExecutor()`, which is `callbackExecutor ?: publicExecutor`; `publicExecutor = Executors.newFixedThreadPool(clientCallbackExecutorThreads)` and `ClientConfig.clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors()`. |
| Settings | Java exposes **no** pull worker-count setting. Consumer thread knobs (`consumeThreadMin/Max`) belong to the consume side, not the pull side. |

### 2.2 Current Rust behavior (current tree)

1. `PullScheduler` runs one owned due-time worker (`run`, `src/push/pull_service.rs:581`) that hands ready requests to `ClientRuntime::execute_pull_request_immediately` (`crates/rocketmq-classic-core/src/runtime/runtime.rs:561`), whose FIFO is an unbounded `VecDeque` (`crates/rocketmq-classic-core/src/runtime/pull.rs:55`) — this stage already matches Java.
2. `PushConsumerRegistration::pull_message` (`src/push/consumer/runtime.rs:111-125`) does not await; it calls `PullRpcWorkers::try_submit`.
3. `PullRpcWorkers` is **4 tasks plus a bounded `mpsc(64)` channel** (`src/push/pull_service.rs:107-190`). Each task awaits `PushConsumer::run_pull_worker_request` → `fetch_push_result` → `runtime.api().pull_message(...).await`, i.e. it holds the task for the whole long-poll (20 s broker suspend, 30 s client deadline; constants at `src/push/consumer.rs:235-236`, RPC await at `src/push/consumer/processing.rs:292`).
4. When the 64-slot channel is full, `try_submit` fails and the request is rescheduled with `PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL = 50 ms` (`src/push/consumer/runtime.rs:118-124`). Java has no such path because its queue is unbounded.

Observable consequence for `N` assigned, mostly idle queues (each poll parks at the broker until the 20 s suspend expires, then reschedules with delay 0 — `src/push/consumer/processing.rs:366, 397`):

| | Concurrent outstanding `PULL_MESSAGE` | First-message latency for an idle queue |
| --- | --- | --- |
| Java 5.5.1 | `N` (one per assigned queue) | ≈ network RTT |
| Rust today | `min(N, 4)` live + up to 64 queued | ≈ `ceil(N / 4) × 20 s` (16 queues → ~80 s, 32 queues → ~160 s) |

### 2.3 Why the difference exists, and its contract status

- Root cause: Rust awaits the RPC inside the calling task. Awaiting on the shared runtime FIFO worker would serialize every consumer and queue of one `ClientRuntime`, so the implementation moved the await into a private pool — but chose a fixed width that Java never had.
- Plan pressure that produced it: master plan P10-04 — "Do not create an unbounded Tokio task for every pull iteration."; Phase 10 plan §4.3 — "Use a bounded worker set and due-time queue rather than detached spawn per iteration."
- Provenance: commit `62253614 feat(consumer): wire bounded push pull pipeline`; the only record is `rlogs/2026-09-11-phase-10-push-consumer-pipeline__20261011T135145Z.rlog:46` — "Added bounded pull RPC workers so broker long polls do not occupy the shared runtime pull-dispatch worker."
- Contract status today: **no ADR, no `KNOWN_DIFFERENCES.md` entry**; the compatibility docs mention it only as evidence text (`docs/compatibility/PHASE_10_OFFLINE_METHOD_MATRIX.md:56`, `docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md:844`). The master-plan contract says Tokio primitives "may replace JVM internals, not change observable semantics", internal class layout is "Not required" to mirror, and "no observable difference may be waived". Because the cap changes an observable quantity (concurrent outstanding pulls → delivery latency under many queues), it is treated here as a parity defect to fix, not as an accepted framework difference.

### 2.4 Existing precedent in this repository

The POP path already uses owned, joinable, cancellable per-assignment tasks: `pop_workers: Mutex<HashMap<MessageQueue, JoinHandle<()>>>` plus `pop_retired_workers` (`src/push/consumer.rs:665-666`), spawn/retire during rebalance (`src/push/consumer/rebalance.rs:305-317, 470-485, 520-528`), and abort/join at shutdown through `shutdown_pop_worker_handles` (`src/push/consumer.rs:336-352`). The new Pull dispatch reuses that ownership pattern, which also satisfies the master-plan rule "Implement these as cancellable tasks owned by [the owner], not detached Tokio tasks."

See `diagrams/02-pull-dispatch-fanout.svg` for the Java / current-Rust / target comparison.

## 3. Target Design

- **Admission-immediate dispatch**: `try_submit` registers the request identity and spawns one owned task, then returns without waiting — the Rust analogue of Java's `CommunicationMode.ASYNC` hand-off.
- **Fan-out bound** = live identities ≤ assigned queue generations. This is already guaranteed by `PullScheduler`: each `PullIdentity {queue, generation}` holds at most one scheduled/in-flight request (`in_flight: HashSet<PullIdentity>`, `src/push/pull_service.rs:301-305`; release through `PullScheduler::finish`, line 512). No fixed worker count, no channel capacity, no admission back-off delay.
- **Exactly-once slot release** through an RAII guard created inside the task: on completion, cancellation, panic or abort it (a) removes its identity from the live map and (b) calls `PullScheduler::finish`. The guard drops after the pull future, so today's "reschedule first, then finish" ordering (`src/push/pull_service.rs:170-178`) is unchanged.
- **Preserved lifecycle**: `watch` cancellation; `stop(wait_millis)` = close admission → signal cancellation → join each live handle within the remaining deadline → abort stragglers and await them; `Drop` aborts. Startup acquisition order, rollback path and `StartupStage::PullRpcWorkersStarted` (including its `"pull-rpc-workers-started"` string) stay unchanged because those names appear in existing evidence.
- **Rejected alternative**: one long-lived loop task per assigned Pull queue (POP-style). It would duplicate Phase 6 `PullMessageService` ownership and bypass the `PullScheduler` delay/coalescing/pause/cancel invariants that Java's single service thread provides.

## 4. Implementation Plan

### 4.1 Replace the pool internals — `src/push/pull_service.rs`

- Delete `PULL_RPC_WORKER_COUNT` and `PULL_RPC_QUEUE_CAPACITY` (lines 107-108) and drop the now-unused `tokio::sync::mpsc` import.
- Change `PullRpcWorkerSet` to `{ live: HashMap<PullIdentity, JoinHandle<()>>, cancellation: watch::Sender<bool> }`; keep `PullRpcWorkers { state: Mutex<Option<PullRpcWorkerSet>> }`. Admission is closed exactly when `state` is `None`.
- `start(consumer, scheduler)`: same signature and same `AlreadyStarted` / `Stopped` outcomes; it only installs the state (no spawn loop). Wrap the set in an `Arc` so completion guards can outlive a later `stop()` take.
- `try_submit(request) -> Result<(), Arc<PullRequest>>`: under the state lock — return `Err(request)` when there is no state; otherwise prune finished handles (`live.retain(|_, handle| !handle.is_finished())`), spawn the task and insert `(identity, handle)`; return `Ok(())`. Never hold the state lock across an `await` or while calling into `PullScheduler`.
- Task body: upgrade the `Weak<PushConsumer>` (on failure release the slot and exit); install the completion guard; then `tokio::select! { biased; _ = cancellation.changed() => {}, _ = consumer.clone().run_pull_worker_request(request) => {} }`, keeping the existing cancel-releases-slot semantics.
- Add `struct PullCompletionGuard` whose `Drop` removes its identity from the live map and then calls `scheduler.finish(&request)`; it must be async-free (std mutex + `Notify` only) so it is abort-safe.
- `stop(wait_millis)` and `Drop`: same algorithm as today, iterating `live.values()` instead of a fixed vector.
- Rename the test-only accessor `worker_count()` to `active_admissions() -> usize` returning the live-task count; keep it `#[cfg(test)]`.

### 4.2 Remove the Rust-only admission back-off — `src/push/consumer/runtime.rs:111-125`

When `try_submit` returns an error (admission closed during shutdown or startup rollback), call `self.pull_scheduler.finish(&request)` and drop the request instead of rescheduling it after 50 ms. This mirrors Java, where a stopped `PullMessageService` logs and drops (`executePullRequestLater` / `executePullRequestImmediately` guard on `isStopped()`). Keep `PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL` in place for the Java-mapped ProcessQueue cache flow control at `src/push/consumer/processing.rs:134, 462`.

### 4.3 Call-site audit (compile-only changes)

Verify these sites still compile and keep their ordering; no signature change is expected: `src/push/consumer.rs` lines 377, 442, 579/595/611, 2562, 2599, 2618, 2627-2630 (startup rollback), 2721-2728 (start + checkpoint), 2740, 3579-3609 , 3728 and 3658 (shutdown); `src/push/consumer/tests.rs` has about 40 `Arc::new(PullRpcWorkers::new())` fixture sites (first ones at lines 457, 623, 961, 1012, 1806, 2284, 2491, 2753, 2848, 2926); they need no edit unless an assertion changes. Enumerate them with `rg -n 'PullRpcWorkers::new|worker_count\(\)' crates/rocketmq-classic-consumer/src` before finishing.

### 4.4 Unit tests

- In `src/push/pull_service.rs`, replace `pull_rpc_workers_have_a_fixed_owned_set_and_join_on_stop` (line 910) with `pull_dispatch_admits_every_distinct_identity_without_a_worker_cap_and_joins_on_stop`: submit more than 64 distinct identities and assert every `try_submit` returned `Ok`; a second `start` still returns `AlreadyStarted`; after `stop(100)`, `active_admissions() == 0`.
- Add `pull_dispatch_releases_the_scheduler_slot_when_a_pull_future_panics`: drive the completion guard with a panicking future and assert the scheduler's logical pull count returns to zero (no leaked in-flight identity).
- In `src/push/consumer/tests.rs`, update the three worker-count assertions: `startup_registers_push_consumer_and_shutdown_releases_it` (line 8322 asserts `== 4`; replace with a no-cap assertion — after start with no assignment `active_admissions() == 0`, and it must not be a fixed constant), line 8381 (`== 0` after shutdown, renamed accessor), `shutdown_aborts_a_slow_listener_without_committing_its_batch` (line 9166) and `shutdown_with_explicit_wait_overrides_configured_wait` (line 9275) keep their zero-after-shutdown assertions with the renamed accessor.

### 4.5 New scripted-peer fan-out test — `crates/rocketmq-classic-consumer/tests/push_pull_fanout.rs`

Model it on the existing harness in `crates/rocketmq-classic-consumer/tests/pull_consumer.rs` (`TcpListener::bind("127.0.0.1:0")`, NameServer peer replying with a route body, `read_command`, `frame::encode`, `respond_empty`). Route fixture: one broker with **16 queues**, real rebalance assignment (Phase 11 path).

Assertions:

1. Before the peer answers anything, it observes at least 16 distinct `queueId` values outstanding on `PULL_MESSAGE`, exactly one request per queue — proving fan-out equals assignment count.
2. No repeated request for the same `queueId` before its first response (this is what fails today: only 4 arrive and the rest re-enter every 50 ms).
3. After replying `NO_NEW_MSG` to all, a second pull round starts for each queue.

### 4.6 Evidence and documentation

- Add `docs/architecture/ADR-0085-push-pull-dispatch-fanout.md`: decision, Java source facts from §2.1, the rejected fixed-cap and per-queue-loop alternatives, and the ownership/join contract.
- Update `docs/compatibility/PHASE_10_OFFLINE_METHOD_MATRIX.md` line 56 (replace "bounded RPC workers" with per-request owned dispatch whose fan-out equals assigned queues) and line 63 ("pull-RPC … workers" wording), plus the evidence cell of `docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md:844`.
- Record this analysis, the change and its verification in a new execution log `rlogs/2026/11/02-push-pull-dispatch-fanout-parity__<UTC>.rlog`. Do not rewrite the historical Phase 10 plan or rlog text.

### 4.7 Register The Phase 19 Live Scenario `P10-V05`

Registered per owner decision (see §7). The live scenario inventory is `tests/differential/scenarios.yaml` (37 entries today; the Push entry `P10-V03` is line 33), and every scenario id is additionally hard-listed in `tests/differential/run.sh`. Registration points:

- Manifest: append one object after the `P10-V03` row with `"id":"P10-V05"`, `"phase":10`, `"execution_phase":19`, `"profile":"basic"` (one Broker), inputs `{"topic":"compat_p10_fanout","group":"compat_p10_push_fanout","queues":16,"messages_per_queue":1}`, assertions `["every-queue-first-delivery-inside-broker-suspend-window","java-and-rust-per-queue-first-delivery-latency-comparable"]`, cleanup for that topic and group, `"normalization":["opaque","timestamps","message_id"]`, and `"live_status":"PENDING_PHASE_19"`.
- `tests/differential/run.sh` id lists to update: manifest input validation (the `by_id` block near line 148 and the per-scenario input guard at lines 189-199); the `implemented=(...)` list at line 343; the topic guard `case "$scenario_id:$topic"` at line 426 (add `P10-V05:compat_p10_fanout`); a per-scenario profile/group guard next to `P10-V03:compat_p10_retry)` at line 546; the long scenario-id exclusion chain at line 625; and the per-scenario command script block (model it on the `P11-V04` heredoc near lines 1074-1090), which must create the topic with `mqadmin updateTopic ... -r 16 -w 16`. The profile allowlist at line 412 already accepts `basic:*`, and the two-Broker override list at line 638 needs no change.
- Probes: add a `p10-fanout <namesrv> <topic> <group> <ready-file>` subcommand to `integration-tests/src/bin/live_client.rs` (model on `p10_push_probe`, lines 135 and 453) that records one `(queueId, first-delivery timestamp)` row per assigned queue; add the matching Java probe under `compat/java-reference/src/main/java/compat/` plus its `exec:java@...` id in `compat/java-reference/pom.xml`.
- Checker: new `tests/differential/check_p10_fanout.py <evidence-dir> <topic>` comparing per-queue first-delivery latency between Java and Rust. Assertions must be timing-tolerant (every queue delivered inside the 20 s suspend window plus a fixed margin; no queue starved beyond a bounded multiple of the median), never an exact millisecond value.
- Add the new checker and probe paths to the `code_dirty_paths` list in `tests/differential/run.sh` (lines 760-775) so clean-source accounting stays correct.
- Docs: add a `P10-V05` row to the "Case ownership and status" table of `docs/compatibility/PHASE_10_OFFLINE_METHOD_MATRIX.md` (after line 213) with status `PENDING_PHASE_19`, and record the added pending scenario in `docs/compatibility/CURRENT_STATUS.md`. Historical counts such as "37/37 live scenarios" in `docs/owner-validation.md` stay untouched as historical text.

## 5. Verification

```sh
cargo fmt --all -- --check
cargo test -p rocketmq-classic-consumer push::pull_service::tests --locked
cargo test -p rocketmq-classic-consumer --test push_pull_fanout --locked
cargo test -p rocketmq-classic-consumer --all-targets --locked
cargo clippy -p rocketmq-classic-consumer --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

Expected: no regression in the existing Push lifecycle/shutdown tests (`startup_registers_push_consumer_and_shutdown_releases_it`, `shutdown_aborts_a_slow_listener_without_committing_its_batch`, `shutdown_with_explicit_wait_overrides_configured_wait`), and the new fan-out test passes.

Live gate (registered scenario, executed in Phase 19): `ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic tests/differential/run.sh --scenario P10-V05` runs it against the disposable 5.3.4 Docker profile, and `bash integration-tests/run-local.sh --all` folds it into the aggregate. The scenario publishes one message per queue to a quiescent 16-queue topic and compares Java and Rust per-queue first-delivery latency; every queue must be delivered inside the broker suspend window instead of scaling with queue count. Until that run happens `P10-V05` stays `PENDING_PHASE_19`; because `run.sh --all`/`--check-all` fail closed on unimplemented ids, §4.7 must land in the same change as the code.

## 6. Acceptance Criteria

1. `PULL_RPC_WORKER_COUNT` and `PULL_RPC_QUEUE_CAPACITY` no longer exist, and no fixed Pull concurrency constant remains in the consumer crate.
2. With `N` assigned queues against a scripted peer that holds long-polls open, `N` concurrent `PULL_MESSAGE` requests are observed, one per queue; the 50 ms admission back-off path is gone.
3. Each submitted request releases its scheduler slot exactly once on completion, cancellation, panic and abort (`active_admissions()` and the scheduler logical count both return to zero).
4. Shutdown keeps its current contract: admission closes, live tasks are joined within `awaitTerminationMillisWhenShutdown`, stragglers are aborted, unresolved queue contents and offsets are retained as today.
5. No public API, setting or default is added, removed or changed; Java-mapped delay constants (`20 ms / 50 ms / 1 s / 3 s`) keep their current call sites except the deleted admission back-off use.
6. ADR-0085 plus the Phase 10 matrix and top-level compatibility rows describe the new dispatch, and the rlog records the parity reasoning that was previously missing.
7. `P10-V05` exists in `tests/differential/scenarios.yaml`, is wired into every scenario-id list of `tests/differential/run.sh`, has both language probes plus `check_p10_fanout.py`, and carries `PENDING_PHASE_19` until a live run records the outcome.

## 7. Decisions

- Decision: keep the existing names `PullRpcWorkers`, the `pull_rpc_workers` field and `StartupStage::PullRpcWorkersStarted` / `"pull-rpc-workers-started"`. Only internals change; renaming would invalidate strings already cited in compatibility docs and historical logs.
- Decision: no fixed upper bound on live Pull tasks. Java has none, fan-out is bounded by assignment count, and every task is owned and joinable. A larger fixed cap (for example 32) was rejected because it reproduces the same observable divergence at a different queue count.
- Decision (owner, 2026-11-12): register a permanent Phase 19 scenario `P10-V05` instead of leaving an owner-executed note. This defect class is invisible to unit and scripted-peer timing assertions, so only a live long-poll run proves fan-out parity; §4.7 registration work therefore belongs to this plan, while the live execution itself stays a Phase 19 obligation and does not block merging the code change.
