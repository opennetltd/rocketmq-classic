# Plan 02 — Match Java Pull-RPC Fan-Out With Per-Request Owned Push Dispatch

## 1. Goal and Scope

Replace the Push consumer's fixed `PULL_RPC_WORKER_COUNT = 4` / `PULL_RPC_QUEUE_CAPACITY = 64` pool with admission-immediate, owned task dispatch. With a stable assignment of `N` eligible Pull queues and sufficient transport permits, all `N` queues must be able to issue a long-poll concurrently, as in Java 5.5.1.

In scope: Pull dispatch in `crates/rocketmq-classic-consumer/src/push/pull_service.rs`, its admission call site in `src/push/consumer/runtime.rs`, the existing async Pull API selection and private Pull suspend constant, lifecycle/regression tests, one scripted-peer integration test, documentation/ADR/rlog, and a complete permanent Phase 19 runner for `P10-V05`. Paths beginning `src/push/` below are relative to the consumer crate.

Out of scope: POP dispatch and polling behavior, the consume-service pool, Phase 6 runtime FIFO ownership, public API/settings, transport admission policy, and executing the live Broker scenario during implementation. The small suspend-constant correction in §4.2 is explicit; it must not change POP's shared-constant use.

## 2. Current-State Analysis

### 2.1 Java 5.5.1 reference behavior

Reference commit: `efa1be59d6fc747ad220f036c86030f05f0419f9`.

| Element | Confirmed behavior |
| --- | --- |
| `PullMessageService` | One service thread drains an unbounded `LinkedBlockingQueue<MessageRequest>`; a single-thread scheduler re-enqueues delayed requests. |
| `DefaultMQPushConsumerImpl.pullMessage` | Uses `CommunicationMode.ASYNC`, a **15,000 ms** Broker suspend and **30,000 ms** client timeout. It does not occupy the dispatch thread throughout the long-poll. |
| Transport limits | `NettyRemotingAbstract.invokeImpl` acquires `semaphoreAsync`; `NettySystemConfig.CLIENT_ASYNC_SEMAPHORE_VALUE` defaults to **65,535**, configurable through `com.rocketmq.remoting.clientAsyncSemaphoreValue`. Java has no separate four-worker Pull limit, but its total async RPC concurrency is not unlimited. Channel establishment and permit acquisition can delay hand-off. |
| Response processing | Normally uses the callback executor or `publicExecutor`; callback thread count is not the number of outstanding long-polls. The default callback thread setting is in `NettyClientConfig`; `ClientConfig` can configure it. |
| Stopped service | `executePullRequestLater` checks `isStopped()`. `executePullRequestImmediately` simply puts into the FIFO; it has no stopped guard. Do not justify Rust shutdown behavior with a nonexistent Java guard. |

Pinned sources: [PullMessageService](https://github.com/apache/rocketmq/blob/efa1be59d6fc747ad220f036c86030f05f0419f9/client/src/main/java/org/apache/rocketmq/client/impl/consumer/PullMessageService.java), [DefaultMQPushConsumerImpl](https://github.com/apache/rocketmq/blob/efa1be59d6fc747ad220f036c86030f05f0419f9/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java), [NettyRemotingAbstract](https://github.com/apache/rocketmq/blob/efa1be59d6fc747ad220f036c86030f05f0419f9/remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java), [NettySystemConfig](https://github.com/apache/rocketmq/blob/efa1be59d6fc747ad220f036c86030f05f0419f9/remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettySystemConfig.java), [NettyClientConfig](https://github.com/apache/rocketmq/blob/efa1be59d6fc747ad220f036c86030f05f0419f9/remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyClientConfig.java).

### 2.2 Current Rust behavior

1. `PullScheduler::{run,take_due_inner}` submits ready requests into `ClientRuntime::execute_pull_request_immediately`. The core runtime FIFO uses an unbounded `VecDeque`; no change is needed to that stage.
2. `PushConsumerRegistration::pull_message` immediately calls `PullRpcWorkers::try_submit`; it does not await the RPC on the shared runtime worker.
3. `PullRpcWorkers` owns four tasks and `mpsc(64)`. Each worker awaits `PushConsumer::run_pull_worker_request` through the whole RPC. Current Rust Pull headers use a **20,000 ms** Broker suspend; the client timeout is 30,000 ms.
4. Channel overflow reschedules after the 50 ms cache-flow-control delay. This happens only after capacity is exhausted, not merely because `N > 4`; 16 queues do not fill the channel.
5. `fetch_push_result` currently calls `MQClientApi::pull_message`, which selects synchronous wait/socket-timeout semantics. The existing `pull_message_future` represents Java ASYNC Pull, including cleanup grace (see `docs/architecture/ADR-0003-typed-rpc-invocation-modes.md`). Select that existing path for Push; leave synchronous Pull API callers unchanged.
6. The remoting crate already has configurable async admission (`client/config.rs`, `client/invocation.rs`) and cancellation cleanup (`client/pending.rs::PendingGuard`). Keep these protections.

For stable empty queues whose polls last the full suspend window `S`, the last queue's **initial request** can wait approximately `(ceil(N / 4) - 1) × S`: with current Rust `S = 20 s`, 16 queues imply about 60 s and 32 about 140 s, excluding setup and scheduling. This is an illustration, not a guaranteed first-message latency. Publishing messages can complete earlier polls immediately and free workers, so publishing to every queue and measuring delivery alone can miss the defect. Java can maintain one outstanding poll per eligible queue while permits remain available.

### 2.3 Contract and ownership

The Phase 10 bounded-pool choice is recorded by commit `62253614` and `rlogs/2026/09/11-phase-10-push-consumer-pipeline__20261011T135145Z.rlog`. The master-plan constraint against unbounded detached tasks does not require a fixed four-RPC limit: owned tasks admitted through scheduler identities satisfy the ownership requirement. Keep historical plans/logs unchanged and document this superseding decision.

`PullScheduler` coalesces work by `PullIdentity { queue, generation }`. A running pull can also have one deferred follow-up, released by `finish`. Rebalance cancellation marks an old generation but does not immediately stop its RPC. Therefore the precise bound is one active request per admitted queue-generation, **including retiring generations**, not strictly the current assignment count during rebalance. `logical_pull_count()` excludes cancelled identities and is cleared by scheduler shutdown; zero alone cannot prove tasks were joined.

The POP path provides an ownership precedent, but a task must not remove/drop its own unfinished `JoinHandle`: that detaches the tail of execution and hides it from shutdown. A completion guard created only inside the async body also misses abort-before-first-poll. The target below avoids both problems.

See [the dispatch comparison](diagrams/02-pull-dispatch-fanout.svg).

## 3. Target Design

- Keep the scheduler and runtime FIFO. `try_submit` synchronously registers one task and returns; it adds no fixed Pull concurrency/channel cap. Transport permits, cache flow control, pause, and stale-assignment checks remain authoritative.
- Use an owned `tokio::task::JoinSet<()>` for task handles. Reap completed results on admission and drain everything at stop; no task owns or mutates the handle collection. The existing Tokio dependency is sufficient. Retained completed entries may remain until the next admission or stop, but must not accumulate with total pull iterations.
- Construct `PullCompletionGuard` **before spawning**, move it into the future, and keep it alive around the entire pull/cancellation scope. It owns only the request, scheduler, and a separate active counter. It calls `scheduler.finish` once after the pull future has been dropped, including failed `Weak` upgrade, panic unwinding, cancellation, and abort before the first poll. No guard acquires the dispatcher state lock or retains a strong `PushConsumer`/dispatcher reference.
- Preserve reschedule-before-finish: `run_pull_worker_request` may record a deferred follow-up; only the guard releases the scheduler slot. A finished task may still be awaiting join when its successor is admitted; this is safe because the earlier pull future has ended.
- Stop closes admission atomically, signals cancellation, then joins within the existing local wait budget. Abort all remaining tasks at the deadline and drain their join results, allowing cancellation cleanup to complete. `Drop` aborts through the owned set; asynchronous cleanup is observed after the runtime polls the aborts. Do not claim a synchronous `Drop` joins tasks or that abort cleanup has a hard wall-clock bound.
- Keep startup/shutdown acquisition order, rollback, `PullRpcWorkers`, `pull_rpc_workers`, and `StartupStage::PullRpcWorkersStarted` / `"pull-rpc-workers-started"`. A per-queue permanent loop is unnecessary and would duplicate scheduler ownership.

## 4. Implementation Plan

### 4.1 Replace the pool internals — `src/push/pull_service.rs`

1. Remove `PULL_RPC_WORKER_COUNT`, `PULL_RPC_QUEUE_CAPACITY`, and the unused `mpsc` import. Keep `PullRpcWorkers { state: Mutex<Option<PullRpcWorkerSet>> }`.
2. Store `Weak<PushConsumer>`, `Arc<PullScheduler>`, the watch sender, `JoinSet<()>`, and an `Arc<AtomicUsize>` active counter in `PullRpcWorkerSet`. No `Arc` around the entire set is required. `None` means admission is closed.
3. `start(consumer, scheduler)` installs an empty set without pre-spawned workers. Preserve `AlreadyStarted` when a set exists. The current dispatcher does **not** return `Stopped` after `stop`; do not accidentally copy the scheduler's different restart contract.
4. Under the state lock, `try_submit` rejects absent state, drains available results with `try_join_next` (report unexpected panics), clones cancellation/consumer/scheduler/counter, increments active count, creates the guard, and spawns into the set before releasing the lock. Production requests must still arrive through `PullScheduler`; do not bypass its identity admission. No await or scheduler method call occurs while holding the dispatcher lock.
5. The future owns the pre-created guard before any await or `Weak` upgrade. Check the cancellation receiver's current value before entering a biased `select!` between `changed()` and `run_pull_worker_request`. A task first polled after cancellation must not issue a new RPC. Scope the pull future so it is dropped before the guard. Guard drop decrements active count, then calls `scheduler.finish`, without accessing dispatcher state.
6. `stop(wait_millis)` takes the set under the lock, releases the lock, signals cancellation, and repeatedly uses `join_next` under one deadline. Handle successful, cancelled, and panicked results without abandoning other tasks. At expiry call `abort_all`, then drain `join_next` until empty. `Drop` signals cancellation and lets the owned `JoinSet` abort outstanding tasks. Do not introduce a set–guard reference cycle; the set stores the consumer weakly and upgrades only for execution.
7. Replace only the dispatcher's test accessor `worker_count()` with `active_admissions()`. Read the separate counter for active requests, not `JoinSet::len()` (which includes completed unjoined tasks). Keep `PullScheduler::worker_count()` unchanged. Tests must retain the counter/set's cleanup witness across `stop` taking the state; an accessor returning zero solely because state is `None` is insufficient evidence.

### 4.2 Align hand-off, async RPC selection, and the private Pull suspend constant

- In `src/push/consumer/runtime.rs::pull_message`, closed admission calls `self.pull_scheduler.finish(&request)` and drops the request; remove only the overflow retry. This is Rust lifecycle cleanup, not a claim that Java's immediate enqueue has a stopped guard. Keep the 50 ms constant and its genuine ProcessQueue cache-flow-control uses.
- In `src/push/consumer/processing.rs::fetch_push_result`, select `runtime.api().pull_message_future` instead of `pull_message`. Preserve the absolute 30 s request deadline; allow the existing async transport cleanup grace and failure mapping rather than inheriting sync socket-close-on-timeout behavior. Add a paused-time/scripted-peer mode regression modeled on the core `rpc_modes` tests. Do not alter the core API or other callers.
- In `src/push/consumer.rs`, add `PULL_BROKER_SUSPEND_MAX_TIME_MILLIS = 15_000` and use it in the Pull header in `src/push/consumer/processing.rs`. Keep the existing 20,000 ms constant for `run_pop_worker` unchanged; it is currently shared by Pull and POP. Keep the Pull client timeout at 30,000 ms. This removes the independently confirmed private Pull wire mismatch without altering POP or public settings.

### 4.3 Audit lifecycle and call sites

Check `PushConsumer::{start,start_registered,shutdown_with_await_termination_millis}`, the error/rollback arm in `start`, and test fixtures; preserve the existing scheduler-stop → dispatcher-stop order. No signature change is expected. Use:

```sh
rg -n 'PullRpcWorkers::new|pull_rpc_workers|worker_count\(\)|BROKER_SUSPEND_MAX_TIME_MILLIS' crates/rocketmq-classic-consumer/src
```

Only dispatcher-count assertions change; consume-service and scheduler counts remain meaningful. Update comments describing a bounded RPC pool. Do not mechanically rename every `worker_count()`.

### 4.4 Unit and lifecycle tests

Factor the owned-task wrapper into a private helper usable with controlled futures; tests must exercise the same guard/spawn/join path as production, without exposing a public testing API.

- Replace `pull_rpc_workers_have_a_fixed_owned_set_and_join_on_stop`: pass **80 distinct scheduler-admitted identities** through `schedule`/`take_due`, hold their task futures on barriers, and prove all 80 enter concurrently. Submitting to an expired `Weak` and only checking `Ok` is not a concurrency test. Assert second `start` is `AlreadyStarted`, then cancel/join and inspect retained cleanup witnesses.
- Cover normal completion with deferred rescheduling, expired `Weak`, panic, watch cancellation, abort of a polled pending future, **abort before first poll**, and dispatcher `Drop`. For completion paths without deferred work, verify the raw `in_flight` set and active counter reach zero while the scheduler is still running; observe panic/cancellation join results. For deferred work, verify exactly one follow-up becomes due.
- Exercise admission racing stop and a zero wait budget: each request is either owned and drained or rejected and finished by the caller. Check cancellation already signalled before first poll and repeated stop. Abort tests must await join or a drop witness; do not infer success from `state == None` or `scheduler.stop()` clearing counters.
- Verify rapid same-identity follow-ups and cancel/reassign with different generations; the old completion must not release the new generation's slot. Across repeated rounds, prove completed task entries are reaped rather than retained per iteration.
- In `src/push/consumer/tests.rs`, `startup_registers_push_consumer_and_shutdown_releases_it` expects zero active admissions before any assignment, rather than four idle workers. Rename its shutdown assertion and those in `shutdown_aborts_a_slow_listener_without_committing_its_batch` and `shutdown_with_explicit_wait_overrides_configured_wait`; retain unresolved-message/offset and rollback checks. Keep the former scheduler/consume-worker assertions.
- Assert Pull `suspendTimeoutMillis == 15000` and its 30 s deadline in a scripted request test; retain a POP assertion that `pollTime == 20000`.

### 4.5 Scripted-peer integration test — `crates/rocketmq-classic-consumer/tests/push_pull_fanout.rs`

Reuse frame/route helpers from `tests/pull_consumer.rs` and Push startup helpers in `src/push/consumer/tests.rs`. One broker exposes 16 read/write queues; one clustering Push consumer uses real rebalance and acquires all 16 target-topic queues. Handle route lookup, heartbeat, consumer-list lookup, offset queries/initial-offset lookup, client configuration checks, and unregister; filter retry-topic pulls separately. Do not block peer reads while awaiting a held Pull response. Track requests across connections by `(connection, opaque)` and queue-generation where available.

1. Reply to setup/control requests, but **hold every target-topic Pull response**. Under one short test timeout (e.g. 5 s), require exactly queues `0..15` outstanding once each, before responding to any target Pull. The old four-worker pool fails this barrier; no 20 s wait or delivery-latency estimate is needed. For 16 queues, neither overflow nor 50 ms retry is expected in the old implementation.
2. Send `response::PULL_NOT_FOUND` (decoded as `PullStatus::NoNewMessage`) with valid `suggestWhichBrokerId`, `nextBeginOffset`, `minOffset`, and `maxOffset` headers. Empty success replies are not valid Pull replies. Check the 15 s suspend header.
3. Hold the second round and again require one outstanding request per target queue. This proves slot release and rescheduling, as well as initial fan-out.
4. Shut down while polls remain open; ensure consumer shutdown and all peer tasks complete, with no further admissions or unresolved test tasks. Use bounded waits and explicit peer teardown.

### 4.6 Evidence and documentation

- Add `docs/architecture/ADR-0085-push-pull-dispatch-fanout.md` if that number remains available; otherwise select the next unused ADR number. Explain the superseding Phase 10 ownership decision, transport-limit qualification, 15 s Pull/20 s POP distinction, and join/guard contract.
- Update the Pull and shutdown rows of `docs/compatibility/PHASE_10_OFFLINE_METHOD_MATRIX.md`, the Phase 10 evidence in `docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md`, and `docs/compatibility/CURRENT_STATUS.md`. Record local evidence separately from the pending live result; do not upgrade broader phase status or rewrite historical validation counts.
- Record implementation and verification in `rlogs/2026/11/02-push-pull-dispatch-fanout-parity__<UTC>.rlog`. Update the shared SVG to match these bounds and corrected facts.

### 4.7 Complete the permanent Phase 19 scenario `P10-V05`

Retain the recorded decision to register this scenario. Scripted peers already prove concurrency; the live case adds real Broker/client evidence. Registering only metadata or a command transcript is insufficient because `run.sh --all` / `--check-all` fail closed on unsupported IDs.

**Manifest and environment**

- Add `P10-V05` to `tests/differential/scenarios.yaml` with `source_case_id: P10-V05`, phase 10, execution phase 19, **profile `host-routed`**, inputs `topic: compat_p10_fanout`, `group: compat_p10_push_fanout`, `queues: 16`, `messages_per_queue: 1`, `admission_window_ms: 5000`, and `delivery_window_ms: 5000`. Host-run Java/Rust Push clients must receive reachable route addresses; the `basic` Broker advertises Docker-only `broker`.
- Use separate `${topic}_java` / `${topic}_rust` topics and `${group}_java` / `${group}_rust` groups. Run one consumer at a time with real rebalance, no fixed allocation strategy. Create each topic with `-r 16 -w 16` on `broker-host` and create both subscription groups (automatic creation is disabled). Manage scenario retry topics through the existing Push setup/cleanup pattern.
- Assertions: full target-topic assignment, all 16 empty-queue pulls initiated before any target Pull response, one active pull per queue, every seeded queue delivered within the declared bound, and recorded per-queue Java/Rust elapsed times. Include both client directions, complete derived-topic/group cleanup, and `live_status: PENDING_PHASE_19`.

**Probes and measurement**

- Add Rust `p10-fanout <namesrv> <topic> <group> <ready-file>` in `integration-tests/src/bin/live_client.rs` and Java `LivePullFanoutProbe` under `compat/java-reference/src/main/java/compat/`, with a matching Maven exec id. Use concurrent listeners that immediately acknowledge; initialize offsets consistently with `ConsumeFromFirstOffset` on fresh topics.
- Observe Pull start/response through the existing `RpcHook::{before_request,after_response}` / Java `RPCHook`, filtering by exact topic and group. Log assignment keys, Pull events, and requested suspend time. Maintain active queue counts; duplicate active starts fail. Hooks observe invocation boundaries, not literal wire arrival; the scripted-peer test supplies wire-level proof. Keep transport permits above the test's demand.
- Start with an empty topic. Allow a separate bounded startup period for assignment. From the first target Pull invocation, require all 16 distinct queues to have active pulls within **5 s and before any target Pull response**; write the ready file only then. A four-worker implementation fails this gate even if all messages could otherwise be consumed quickly. Do not seed before this gate or use an indefinite settling sleep.
- After the gate, a reusable producer in the same probe process explicitly sends one unique message to each assigned `MessageQueue`. Record monotonic send-start and first-listener-delivery elapsed times per queue in that process (Java `System.nanoTime`, Rust `Instant`). Measure each queue from its own send start, allow callback-before-send-return, and include client/run/topic/broker/queue/message keys in the evidence. Separate topics avoid consumed offsets/backlog contaminating the next language run.
- New `tests/differential/check_p10_fanout.py <evidence-dir> <topic-base>` requires complete, unique 16-queue assignment/send/delivery sets for both languages, validates event ordering and the 5 s admission/delivery bounds, and reports per-queue elapsed values and summary statistics. Do not subtract clocks across processes or demand an exact Java/Rust latency ratio or a multiple of a near-zero median. The barrier is the regression gate; latency is bounded supporting evidence.

**Runner wiring**

Audit with `rg -n 'P10-V03|P11-V04|P9-V03|implemented=|code_dirty_paths|topic_queue_count|group_created' tests/differential/run.sh`; update every applicable dispatch point, not every occurrence indiscriminately:

- manifest input guards, `implemented`, profile/topic/group allowlists (`host-routed:P10-V05`), and the supported-ID rejection chain;
- actual host-routed topic/subgroup creation, 16-queue override, command transcript, **executable** scenario branch that runs both probes and checker, exit/result/status recording, and process/topic/group cleanup on failure as well as success;
- source-dirty accounting for the new checker (the `compat/java-reference/` and `integration-tests/` directory entries already cover the probes).

Add a `P10-V05` ownership row to the Phase 10 matrix and explain the pending case in `CURRENT_STATUS.md`. Add checker self-tests using synthetic evidence: valid run, only four pulls before the deadline, a response before the admission barrier, missing/duplicate queue, and invalid timing must produce the expected pass/fail results. The live runner must be complete in the same implementation change as its manifest entry.

## 5. Verification

Implementation checks (these are future implementation requirements, not claims that code changed during plan review):

```sh
cargo fmt --all -- --check
cargo test -p rocketmq-classic-consumer push::pull_service::tests --locked
cargo test -p rocketmq-classic-consumer --test push_pull_fanout --locked
cargo test --workspace --all-targets --locked
cargo test --workspace --doc --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
mvn -f compat/java-reference/pom.xml verify
bash -n tests/differential/run.sh
bash tests/differential/run.sh --list
bash tests/differential/run.sh --check-all
python3 tests/differential/check_p10_fanout.py --self-test
git diff --check
```

Use the repository's pinned Java/Maven environment; record environment limitations without claiming an unrun check passed. Show that the held-response integration test fails with the old four-worker dispatcher and passes with the replacement. Preserve startup rollback, slow-listener shutdown, explicit wait, pause/resume, cache flow control, rebalance generation isolation, and transport cleanup tests. The workspace checks include compiling the new Rust probe; Maven verification compiles the Java probe.

Final, owner-executed Phase 19 gate:

```sh
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=host-routed bash tests/differential/run.sh --scenario P10-V05
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable bash integration-tests/run-local.sh --all
```

Archive both language logs and checker results outside the repository through the existing evidence mechanism. Until live execution records a result, keep `P10-V05` pending and do not present the previous 37/37 live result as covering this new case. Live execution remains a Phase 19 obligation and does not block merging locally verified implementation.

## 6. Acceptance Criteria

1. No extra fixed Pull dispatch width/channel capacity remains. With stable `N` eligible assigned queues and available transport permits, the peer sees `N` outstanding target-topic pulls before any response, one per queue. The overflow retry is removed; transport admission and genuine cache flow control remain.
2. Every admitted request owns one guard and remains tracked until joined. Completion, expired owner, panic, cancellation, and abort-before-first-poll release its slot once. Deferred follow-ups and old/new generations remain isolated; completed-task retention does not grow with iterations.
3. Stop closes admission, cancels, joins until the existing local deadline, then aborts/drains stragglers. Rollback/shutdown retain existing unresolved messages and offsets. Tests prove cleanup with live-scheduler/retained witnesses, not cleared-state counters alone.
4. Push selects the existing async Pull API with its cleanup semantics. Pull headers use the pinned 15 s Broker suspend and 30 s client timeout; POP keeps 20 s polling. Public API/settings and other Java-mapped flow-control/retry delays remain unchanged.
5. ADR, bilingual plan/SVG, compatibility rows, and rlog describe the qualified fan-out and lifecycle contract without claiming unlimited Java transport concurrency or full-phase parity.
6. `P10-V05` has a complete executable host-routed runner, isolated probes, checker with negative self-tests, and cleanup. Offline manifest/runner checks pass; the live result stays `PENDING_PHASE_19` until executed.

## 7. Decisions

- Keep existing dispatcher and startup-stage names to avoid unrelated churn.
- Remove the Pull-specific fixed cap; retain existing transport limits. Steady-state fan-out scales with eligible assignments, while retiring generations can briefly coexist during rebalance.
- Use `JoinSet` ownership and a pre-spawn guard, rather than a self-removing `HashMap<PullIdentity, JoinHandle<()>>` that can detach tasks or miss early abort cleanup.
- Preserve the recorded owner decision (2026-11-12) to add permanent `P10-V05`. Correct its rationale: local scripted peers can detect this defect deterministically; the live case confirms the same behavior with the actual Broker, and must gate on empty-queue concurrency before publishing.
