# ADR-0026: Map the inherited Java ServiceThread API boundary

Status: inherited public controls are mapped by the Rust service handles;
whole-phase source and API reconciliation remains open.

## Source contract

In Java 5.5.1, `RebalanceService` and `PullMessageService` inherit public
`ServiceThread` methods `start`, `shutdown()`, `shutdown(boolean)`,
`getJoinTime`, `makeStop`, `wakeup`, `isStopped`, `isDaemon`, and
`setDaemon(boolean)`. `PullMessageService` is reachable through
`MQClientInstance.getPullMessageService()`. `getServiceName()` is overridden on
both classes and mapped in [ADR-0025](ADR-0025-runtime-service-name-methods.md).
The join interval is 90,000ms, new threads default to non-daemon and not
stopped, and `makeStop()` is a pre-start no-op. PullMessageService's overridden
shutdown also closes its separate scheduled executor with a one-second grace.

The Java `serviceThreadCases` fixture invokes state/configuration methods on
both unstarted workers. `serviceLifecycleCases` uses latch-controlled run
bodies to verify idempotent start, a stop flag that does not join, and
`shutdown(true)` joining the worker. The probes do not run either RocketMQ
worker loop or contact a Broker.

## Rust mapping

`ClientRuntime::{rebalance_service,pull_message_service}` returns a cloneable
`RuntimeServiceThread` handle for the corresponding runtime-owned worker. Its
`start()` and `shutdown(interrupt)` are async because Rust owns Tokio tasks.
Runtime-wide `ClientRuntime::start/shutdown` continues to start and stop all
client-owned workers.

Each handle retains a strong `Arc<ClientRuntime>`. This matches the Java
service object's strong `MQClientInstance` field: retaining the service handle
keeps its client owner available for lifecycle calls.

The handle exposes `service_name`, `join_time_millis`, `is_stopped`,
`make_stop`, `wakeup`, `is_daemon`, and `set_daemon`; `shutdown()` and
`shutdown_with_interrupt(bool)` map Java's two shutdown overloads. Initial
stopped/daemon values, pre-start stop/shutdown behavior, repeated start, daemon
changes, restart after a full runtime shutdown, and retained-owner behavior
are covered by the runtime integration tests. `make_stop()` is a pre-start
no-op. On a started service it sets the stop flag and wakes the rebalance
worker without joining it. PullMessageService's inherited wakeup does not
release its blocking message-queue take; a queue item that wakes an already-
blocked pull worker is processed before the stop flag takes effect.

`shutdown(interrupt)` sets the stopped flag, performs the source worker's
wakeup, and waits up to the pinned 90,000ms interval when the thread's daemon
value captured at start is false. For PullMessageService, `interrupt=true`
releases a worker blocked on the queue; an in-flight callback completes before
the stop flag is observed. Pull shutdown closes its scheduled executor even
when the inherited service shutdown is a pre-start no-op. It preserves the
one-second scheduled-work grace before cancellation and join. The `isDaemon`
getter and mutable daemon setting match Java; because Tokio tasks do not own
process-exit policy, the setting controls the corresponding per-service join
wait rather than an OS thread flag.

The inherited PullMessageService `wakeup()` operates on ServiceThread's
wait-point, but its concrete `run()` blocks on `LinkedBlockingQueue.take()` and
does not use that wait-point. The Rust handle therefore does not notify the
pull queue for `wakeup()`; queued work remains the event that releases that
wait. The rebalance handle signals the sticky rebalance notification.

`ServiceThread.waitForRunning(long)` and `onWaitEnd()` are protected methods,
not public API rows. Concrete worker loops and their runtime effects are mapped
separately in the Phase 6 runtime inventory.

## Evidence

`inherited_service_thread_oracle_records_java_thread_controls` and
`inherited_service_thread_oracle_records_java_start_stop_contract` pin the
source observations. `runtime_service_thread_handles_match_inherited_controls`,
`pull_service_prestart_shutdown_keeps_service_flag_and_closes_scheduler`, and
`service_threads_can_restart_after_client_runtime_shutdown` exercise the Rust
handle behavior. The pull-dispatch regressions cover stopped-service task
admission and scheduler shutdown. Full Phase 6 reconciliation and P6-V04 Phase
19 evidence remain open.
