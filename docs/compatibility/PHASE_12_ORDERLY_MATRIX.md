# Phase 12 orderly-consumer compatibility matrix

Oracle: RocketMQ Java client 5.5.1, source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`. The production Broker baseline is
5.3.4; this phase's local checks use the pinned Java fixture and scripted RPC
results without requiring a Broker.

## Java source to Rust mapping

| Java source surface | Rust owner | Local evidence |
| --- | --- | --- |
| `DefaultMQPushConsumer.maxReconsumeTimes`, `suspendCurrentQueueTimeMillis`, `consumeTimeout`, `consumeMessageBatchMaxSize` | `PushConsumerConfig` | `fixtures/consumer/orderly-settings.json`; `tests/orderly.rs` |
| `ConsumeOrderlyContext` defaults, getters/setters, and `MessageQueue` | `ConsumeOrderlyContext` | Java fixture and `tests/orderly.rs` |
| `MessageListenerOrderly.consumeMessage(List<MessageExt>, ConsumeOrderlyContext)`; inherited marker `MessageListener` has no methods | `MessageListenerOrderly::consume_message(Vec<MessageExt>, ConsumeOrderlyContext) -> OrderlyListenerFuture`; the returned status and owned context drive queue commit/suspend behavior | SOURCE_RECONCILED — pinned Java's `ConsumeRequest.run` passes an unmodifiable list and catches `Throwable`; Rust preserves the assigned queue through its owned snapshot, maps absent status and callback panic to suspension, and applies auto-commit/rollback/commit rules in the same queue owner. `tests/orderly.rs` pins all four Java status names and the context fixture; `orderly_success_holds_until_commit_and_rollback_replays_in_order` verifies returned context mutation and commit/rollback behavior, while same-queue serialization and lock-loss tests cover callback lifecycle. |
| `ConsumeOrderlyStatus.SUCCESS`, `ROLLBACK`, `COMMIT`, `SUSPEND_CURRENT_QUEUE_A_MOMENT`; nullable listener result | `ConsumeOrderlyStatus`, `OrderlyListenerFuture` | Java fixture, listener-result table tests |
| `ProcessQueue.REBALANCE_LOCK_MAX_LIVE_TIME`, `REBALANCE_LOCK_INTERVAL`, `isLockExpired` | `ProcessQueue::{rebalance_lock_max_live_time_millis, rebalance_lock_interval_millis}` plus internal `OrderlyLockSettings` and monotonic lock state | Public accessors expose the Java static-field values; the pinned fixture and exact-boundary paused-time test cover defaults and expiry. Both properties are captured together on first ProcessQueue access, matching Java class initialization. |
| `ConsumeMessageOrderlyService.MAX_TIME_CONSUME_CONTINUOUSLY`; `RebalancePushImpl.UNLOCK_DELAY_TIME_MILLS` | private `OrderlyLockSettings` and dropped-queue cleanup | Java fixture reflects the pinned static values; strict-boundary and slow-callback cleanup tests |
| `RebalanceImpl.lock`, `lockAll`, `unlock`, `unlockAll` | `PushConsumer::{lock_orderly_queues_raw, lock_orderly_process_queues, unlock_orderly_queues_raw, unlock_orderly_process_queues}` | Scripted batch-lock, partial-result, error-preservation, stale-response, and per-Broker response-time tests |
| `ConsumeMessageOrderlyService.start`, lock renewal, `tryLockLaterAndReconsume`, bounded queue retry | `OrderlyConsumeService` and PushConsumer-owned retry tasks | Initial-delay/interval constants; scripted renewal and retry tests |
| `ProcessQueue.takeMessages`, `commit`, `rollback`, `makeMessageToConsumeAgain` | `ProcessQueue::{take_batch, complete_orderly_batch}` | Held/commit/rollback/requeue state tests |
| `ConsumeMessageOrderlyService.processConsumeResult` and `checkReconsumeTimes` | `orderly_disposition`, `check_orderly_reconsume_times`, orderly retry-topic send | Java source cross-check; auto-commit/status table and retry fallback tests |
| `ConsumeRequest.run`: per-queue serialization, lock freshness, continuous-consume limit, callback, and dropped-generation checks | per-queue async gate and `PushConsumer::run_orderly_consume_request` | Same-queue callback serialization/order, independent-queue progress, lock-loss barrier, and slow-callback rebalance tests |
| `DefaultMQPushConsumerImpl` orderly gain/loss and shutdown cleanup | Push rebalance and shutdown lifecycle | Lock-before-pull, persist-before-unlock, retryable cleanup and no-stale-commit tests |

## Settings and behavior boundaries

- The Java consumer defaults to `maxReconsumeTimes=-1`, effective orderly
  limit `Integer.MAX_VALUE`, suspend delay `1000ms`, consume timeout `15min`,
  and batch size `1`. `ConsumeOrderlyContext` defaults to `autoCommit=true`
  and suspend delay `-1ms`.
- Java lock defaults are `30000ms` maximum live time and `20000ms` renewal
  interval; clustering renewal starts after `1000ms`. Expiry is strict: a lock
  is valid at exactly the maximum age and expires after it. Batch lock/unlock
  RPC timeout is `1000ms`.
- Java defaults `rocketmq.client.maxTimeConsumeContinuously` to `60000ms` and
  `rocketmq.client.unlockDelayTimeMills` to `20000ms`; both affect the orderly
  consume loop and dropped-queue cleanup. Rust reads these and the lock properties
  `rocketmq.client.rebalance.lockMaxLiveTime` and
  `rocketmq.client.rebalance.lockInterval` from process environment variables
  with the exact Java property names, because Rust has no JVM system-property
  table. The two public `ProcessQueue` lock values are captured together on
  first ProcessQueue access; the other orderly runtime properties are captured
  when an orderly consumer starts.
- Only returned queue IDs from a successful lock response are activated. A
  transport error or nullable lock-set response does not overwrite an existing
  local lock result. RPC requests use the configured VIP-channel address.
- Each queue serializes callbacks independently. Listener code runs without
  the ProcessQueue state mutex. The callback result is checked against the
  assignment generation and lock freshness before offset commit.
- `autoCommit=false` success holds messages until a later COMMIT; ROLLBACK
  restores held/current messages in offset order; retryable SUSPEND requeues
  and suspends that queue. At the retry limit, orderly consumption publishes
  directly to `%RETRY%{consumerGroup}`; it does not use the concurrent
  Broker send-back request.
- Java's `consumeTimeout` classifies completed callbacks for statistics; it
  does not interrupt them. Rust keeps the orderly callback alive until it
  returns. Rebalance waits at most `500ms` for the queue gate per attempt; once
  the last confirmed lock exceeds Java's configured unlock delay, cleanup may
  persist and release the dropped assignment while the callback is still
  running. Generation fencing prevents that callback from committing; its
  application side effects may be repeated on redelivery.
- Shutdown persists offsets before attempting confirmed unlock. A failed
  persist prevents releasing the lock; an unconfirmed unlock returns
  `OrderlyUnlockFailed`.
- `P12-V03` mixed Java/Rust Broker contention and slow-callback live behavior
  remain registered for Phase 19. No live Broker result is claimed here.

## Verification status

| Case | Scope | Status |
| --- | --- | --- |
| P12-V01 | Partial lock results, renewal races and response-time expiry, exact boundary, and all listener/context outcomes | UNIT_VERIFIED — scripted partial/error/null results, stale renewal, cross-Broker response timing, paused exact-expiry, and status/context table tests pass |
| P12-V02 | Same-queue serialization and order; independent queues; suspend, rollback, retry, and lock loss | UNIT_VERIFIED after the Phase 12 test run |
| P12-U01 | Scripted lock results, paused time, slow callbacks, rebalance and shutdown without stale commits | UNIT_VERIFIED after the Phase 12 test run |
| P12-V03 | Live mixed-client orderly contention and Broker lock convergence | PENDING_PHASE_19 — registered in `tests/differential/scenarios.yaml` for the Phase 19 two-Broker profile |

## Final local validation

On 2026-10-12, the following passed on the repository host:

- `cargo test --workspace --locked` — PASS, including 102 consumer unit tests,
  the consumer integration tests, and all workspace doctests.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
  — PASS.
- `cargo build -p rocketmq-classic --examples --locked` — PASS.
- `mvn -o -q -f compat/java-reference/pom.xml verify` — PASS; the 32-scenario
  manifest, Java fixture drift checks, and Java decoding of Rust-generated
  frames/bodies completed.
- `python3 tests/differential/compare.py --self-test` and
  `--validate-manifest` — PASS; 32 scenarios validated.
- `cargo fmt --all -- --check` and `git diff --check` — PASS.

No live Broker, TLS cluster, or mixed Java/Rust orderly-consumer scenario was
run. P12-V03 remains pending Phase 19.

Fixture provenance: `compat/java-reference/src/main/java/compat/OrderlyConsumerFixtures.java`
invokes the pinned 5.5.1 classes. Maven `verify` checks fixture drift.
