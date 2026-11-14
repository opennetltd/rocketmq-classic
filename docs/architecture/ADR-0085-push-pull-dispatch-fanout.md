# ADR-0085: Own one Push Pull task per admitted queue-generation

Status: implemented locally; the live Broker confirmation `P10-V05` is pending Phase 19.

## Source contract

Java 5.5.1 push consumption hands a Pull request from a single dispatch thread to an
asynchronous RPC and never occupies that thread for the long-poll window. Reference commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`:

- `client/.../consumer/PullMessageService.java` drains one unbounded
  `LinkedBlockingQueue<MessageRequest>` on a single `ServiceThread`; a single-thread
  scheduler re-enqueues delayed requests. There is no fixed Pull worker count.
- `client/.../consumer/DefaultMQPushConsumerImpl.pullMessage` uses
  `CommunicationMode.ASYNC`, a **15,000 ms** Broker `suspendTimeoutMillis` and the
  **30,000 ms** `CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND` client timeout. Response handling
  moves to the callback executor, so callback width is not the number of outstanding
  long-polls.
- Transport concurrency in Java is bounded but wide: `NettyRemotingAbstract.invokeImpl`
  acquires `semaphoreAsync`, whose default `CLIENT_ASYNC_SEMAPHORE_VALUE` is **65,535**
  (`com.rocketmq.remoting.clientAsyncSemaphoreValue`). Channel establishment and permit
  acquisition can still delay hand-off. Java parity here means "one outstanding poll per
  eligible queue while permits remain", not unlimited transport concurrency.

Rust records the earlier opposite choice in commit `62253614` and
`rlogs/2026/09/11-phase-10-push-consumer-pipeline__20261011T135145Z.rlog`: Push Pull RPCs
went through `PullRpcWorkers`, a private pool of four owned tasks fed by an `mpsc(64)`
channel, and each worker awaited the whole RPC. A full channel re-queued the request after
the 50 ms cache-flow-control delay, which has no Java counterpart. Push Pull headers also
reused the POP long-poll constant `BROKER_SUSPEND_MAX_TIME_MILLIS = 20_000` instead of the
Pull/simple-consumer value.

## Decision

- Supersede the Phase 10 fixed Pull pool. `PullRpcWorkers` keeps its name, its startup
  stage (`StartupStage::PullRpcWorkersStarted`) and its shutdown position, but stores an
  owned `tokio::task::JoinSet<()>`. `try_submit` registers one task under the state lock
  and returns; no fixed Pull width or channel capacity remains. Transport permits, cache
  flow control, pause and stale-assignment checks stay authoritative.
- Ownership without self-removal: a task never drops its own unfinished handle. A
  self-removing `HashMap<PullIdentity, JoinHandle<()>>` can detach the tail of execution
  from shutdown and misses abort-before-first-poll cleanup, so it was rejected in favour of
  the owned set plus a `PullCompletionGuard` constructed **before** spawn. The guard holds
  only the request, the scheduler and an active counter; it releases the scheduler slot
  exactly once after the pull future has been dropped, covering normal completion, expired
  consumer owner, panic unwinding, cancellation and abort before the first poll. It never
  takes the dispatcher lock and keeps no strong `PushConsumer` or dispatcher reference.
- Keep `PullScheduler` as the only admission point: one active request per admitted
  queue-generation identity plus at most one deferred follow-up, released by the guard. The
  bound includes retiring generations during rebalance, so it is not identical to the
  current assignment count. A finished task may still be awaiting join when its successor
  is admitted; that is safe because its pull future already ended.
- Closed admission releases the slot and drops the request instead of retrying with the
  cache-flow-control delay. Re-queueing after closed admission would let one admitted
  identity span unbounded attempts, which Java never does. Genuine ProcessQueue
  cache-flow-control delays and the Java-mapped failure delays are unchanged.
- Push Pull selects the existing `pull_message_future` API (Java ASYNC semantics with
  transport cleanup grace) while keeping the absolute 30 s client deadline. Synchronous
  `pull_message` callers elsewhere keep their socket-close-on-timeout behavior.
- Separate the two suspend constants: `PULL_BROKER_SUSPEND_MAX_TIME_MILLIS = 15_000` for
  Push Pull headers, and `BROKER_SUSPEND_MAX_TIME_MILLIS = 20_000` retained only as the POP
  worker's long-poll wait feeding its `pollTime + 10 s` deadline. POP dispatch is otherwise
  untouched.
- Stop closes admission atomically, signals cancellation, joins until the existing local
  wait budget, then aborts every remaining task and drains all join results so guards run.
  `Drop` signals cancellation and lets the owned set abort outstanding tasks; cleanup is
  observed once the runtime polls those aborts. No claim is made that `Drop` joins
  synchronously or that abort cleanup has a hard wall-clock bound.

## Consequences

- Steady-state Pull fan-out scales with eligible assignments rather than a private
  constant, so an empty-queue assignment of `N` queues issues `N` concurrent long-polls
  subject to transport permits. Retiring generations can briefly overlap during rebalance.
- Task count is bounded by admitted queue-generation identities, satisfying the master-plan
  constraint against unbounded detached tasks without reintroducing a fixed RPC cap.
- A Pull that would previously have been delayed by a full internal channel now reaches the
  transport layer, where the existing permit and cancellation-cleanup protections decide
  admission.

## Evidence

- `push::pull_service::tests` covers admission without a fixed width, concurrent entry of
  admitted tasks, slot release on completion/expired owner/panic/cancellation, abort before
  and after the first poll, dispatcher drop, admission racing with stop, old-generation
  completion not releasing a new-generation slot, and bounded reaping of completed entries.
- `push::consumer::tests` adds runtime-level proof that closed admission releases the
  scheduler slot without any RPC attempt or re-queue, that exactly one Pull RPC is issued
  per admission with the follow-up visible only after release, that Push Pull uses the
  future RPC mode inside the 30 s deadline, and that Pull keeps 15 s while POP keeps 20 s.
- `crates/rocketmq-classic-consumer/tests/push_pull_fanout.rs` drives a real clustering
  consumer against scripted nameserver/broker peers with 16 queues, holds every target-topic
  Pull response, and requires all 16 queue ids outstanding exactly once (with the 15 s
  suspend header) before any response, then again after one release round, then shuts down
  while polls are outstanding. With the pre-change pool restored the same test fails with
  `only 4 of 16 queues started a Pull within 5s`.
- The scripted peer owns every accepted connection task (reader, writer, handler), and that
  ownership lives outside the fallible scenario body: the scenario runs in its own task so neither
  an assertion failure nor a panic can bypass cleanup. Teardown closes acceptance, cancels each
  connection cooperatively, then drains every recorded task under one cleanup bound — a timed-out
  handle is aborted and still awaited — and only afterwards are the scenario failure (with its
  original message) and any cleanup failures reported together. The live-task witness must reach
  zero in every case. `peer_teardown_finishes_every_task_after_an_abandoned_connection` covers a
  client that never closes its socket, `peer_cleanup_drains_every_task_after_an_injected_scenario_failure`
  an injected assertion failure while a connection stays open,
  `peer_cleanup_reports_forced_timeouts_after_draining_every_child` uncooperative children under a
  forced timeout, and `an_aborted_peer_task_releases_its_witness_before_the_first_poll` the
  witness built before its task starts.
- The permanent live case keeps its own contract: both probes start their producer before route
  lookup (the pinned 5.5.1 client rejects `fetchPublishMessageQueues` for a non-RUNNING
  producer), track active Pulls per queue-generation and request identity under one lock, sample
  the monotonic clock inside that same lock so each row carries the timestamp of its own state
  transition, latch the admission gate atomically with the last admission, and write their ready
  file only after that latch. `run.sh` requires both fan-out topics to report zero max offsets before publishing,
  records only the subgroups it actually created, and refuses a PASS line on a probe exit, a
  missing ready file or a checker failure. `check_p10_fanout.py` rejects pre-gate publishing,
  missing/duplicate/mismatched send completions, duplicate deliveries, any target response that
  lands before the barrier, and a gate reached outside the admission window measured from the
  earliest recorded Pull start.
- These are local scripted-peer results. Live Broker/client evidence is registered as
  `P10-V05` and stays `PENDING_PHASE_19`; it does not retroactively validate the earlier
  Phase 19 live run.
