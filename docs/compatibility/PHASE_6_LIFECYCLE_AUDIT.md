# Phase 6 lifecycle resource audit

Baseline: client 5.5.1, `efa1be59d6fc747ad220f036c86030f05f0419f9`. Source: `client/.../impl/factory/MQClientInstance.java` construction, `cleanupAfterConstructionFailure`, `start`, `cleanupAfterStartFailure`, `shutdown`; `MQClientManager` identity-aware removal; `PullMessageService` and `RebalanceService` lifecycle. Rust ownership replaces JVM executors/threads, not their client flows.

## Resource-to-cleanup inventory

| Source resource / effect | Native owner | Failure cleanup / executable evidence |
| --- | --- | --- |
| manager factory entry, construction ownership | per-key manager Entry/OnceLock, weak manager binding | constructor error/cancellation removes only its entry; startup failure removes only its runtime; manager identity/parallelism tests and P6-V03 replacement/stale-remove assertion |
| ClientConfig/boot metadata | immutable runtime value, constructor-entry wall clock | ordinary owned drop on failed construction; boot metadata tests; no constructor task needs a join |
| Netty config, API/remoting client, hooks | existing RemotingClient and typed MQClientApi | construction owns no started transport workers; invalid TLS/SOCKS/enabled concurrent-heartbeat pool and callback-registration failure tests; callback thread count <=0 uses source fallback4, not constructor rejection; start failure awaits lower shutdown |
| addressing SPI/update callback | owned addressing/DiscoveryState, callback holds Weak state | registration panic fails construction; retained callback cannot keep old runtime/state alive; discovery failure/weak ownership tests |
| channel-active heartbeat listener | runtime-owned listener, weak runtime binding | no strong cycle; lower channel-event worker belongs to remoting shutdown; heartbeat listener ownership/wire tests |
| default producer | control actor owns the internal `CLIENT_INNER_PRODUCER` registration and identity lease; retry-topic send behavior is implemented by Consumer owners | Pinned Java `MQClientInstance.getDefaultMQProducer()` is an implementation accessor used by the four retry/send-back call sites plus factory start/shutdown. Rust keeps the internal registration marker in Core and maps sends through Consumer/Producer owner paths without exposing a mutable wrapper; startup failure clears registration, and normal shutdown unregisters it before transport closes. |
| registered producer/consumer/admin owners | weak group registry, identity leases, serialized control commands; Java-style admin owner retention by group | failed factory cannot serve users; native failure cleanup invalidates all leases; stale unregister does not affect replacement; P6-V03 checks all three kinds while external Arcs remain alive. Java's invoked started-runtime gate and `tests/lifecycle.rs::admin_registration_blocks_shutdown_until_owner_is_unregistered` verify that an admin keeps the running factory alive until explicit group removal. Admin owner `Drop` runs after releasing the registry lock, verified by reentrant destructors on unregister and clear; owner back-references use `Weak<ClientRuntime>`, and the lifecycle regression proves final runtime release |
| eight inbound processor registrations | owned lower processor leases in control actor | reverse lease drop only removes matching identities; startup failure and shutdown clear runtime slots; lower identity tests, inbound tests and P6-V03 table-empty check |
| scheduled executor / periodic jobs | one OwnedTasks worker, serial fixed-rate schedules | cancellation then awaited join; pending callback dropped; no late ticks; timer and rollback probes |
| concurrent heartbeat executor | bounded futures inside the owned heartbeat round, no detached heartbeat executor | cancellation of periodic owner drops round; private gate and capacity/wire tests; no extra worker or checkpoint |
| pull service / its scheduled executor | two OwnedPullWorker handles, queue-owned requests/futures | failure closes queues/delayed admission and joins workers in reverse actual creation order; normal shutdown cancels dispatch and permits scheduled worker its source 1000ms grace before cancellation; pull tests and P6-V03 queued-future Drop marker |
| rebalance service / delayed wakeup | OwnedRebalance handle, weak runtime, delayed state in RebalanceSignal | close delayed wakeups, cancel/join worker; timer/rebalance checks and P6-V03 no-due/live-zero assertions |
| connected sockets / outstanding RPCs / lower transport workers | existing remoting lifecycle owner | lower shutdown closes active/retired connections, fails pending responses, joins owned tasks/connections/callback workers under one bounded deadline; P6-V03 observes actual EOF and pending Shutdown at every failure point |
| runtime control actor | retained JoinHandle plus async join gate | caller cancellation cannot detach only handle; terminal start awaits join in place; P6-V03 task=None and existing cancelled-terminal-join check |
| route/heartbeat caches | coherent Arc-published route/index snapshot with Java-compatible nested route DTO aliases; owned heartbeat state | startup does not partially publish a route; cache retains the prior published Arc on failure; route refresh identity oracle plus read/conversion tests and P6-V03 Arc identity assertion; failed transport cannot emit more frames |
| MQAdminImpl / ConsumerStatsManager | concrete admin/statistics implementations remain owning phases | not active resources or implemented compatibility claims in Phase 6; see API map/inbound activation matrix |

Lower shutdown uses the already accepted Phase 3 native lifecycle contract and its bounded shared deadline. This does not promise forcibly stopping arbitrary synchronous blocking user code or recreating Netty/JVM thread identity. Cooperative async callbacks are owned and dropped/joined through their worker. No new public tuning setting is introduced.

## Startup checkpoints and ordering

The private `ControlPlane::checkpoint` seam runs after these effects:

| ID | Completed effect |
| --- | --- |
| 1 | internal producer registration |
| 10–17 | each of the eight inbound processor slots, in source API registration order |
| 2 | remoting start (startup discovery, when required, has already completed) |
| 3–6, explicit NameServer | route / heartbeat / persist / adjust schedule registrations |
| 3–7, discovery | discovery / route / heartbeat / persist / adjust schedule registrations |
| 21 | pull dispatch worker creation |
| 22 | pull scheduled worker creation or retention of its Created-state lazy activation |
| 20 | rebalance worker creation |

Periodic/rebalance/new pull workers activate only after the startup transaction reaches Running. Created-state scheduled work may already own the pull scheduled worker, so rollback follows actual creation order rather than assuming a fixed array order. Scheduler construction/registration is synchronous and uses the active Tokio runtime; there is no untracked yield between creating OwnedTasks and registering its first schedule. Positive period validation occurs before OwnedTasks creation. The tests also cover real validation failures, not only injected errors.

Failure cleanup closes pull queues and delayed rebalance admission; joins rebalance, pull workers in reverse creation order, then periodic work; awaits remoting shutdown; drops inbound leases; clears failed registrations; publishes Failed; removes its own manager identity; delivers the startup error; exits and joins the control actor. Normal shutdown first refuses while real users remain, stops services, unregisters the internal producer before closing transport, clears slots, publishes Stopped and identity-removes the factory. The native serialized actor provides ownership/cancellation guarantees without a JVM monitor.

## P6-V03 executable acceptance evidence

`runtime::control::tests::p6_v03_every_failure_closes_pending_socket_and_drops_owned_work` runs **47 scenarios**: all 17 explicit / 18 discovery checkpoints, plus zero and negative values for each of the three configurable periodic intervals in both modes (12 actual InvalidSchedule cases).

Every scenario registers producer, consumer and admin owners, queues a 60s future with an observable destructor and a delayed rebalance, opens a real lower loopback socket, and leaves a correlated 60s RPC deliberately unanswered. The public lower API is started directly before runtime startup to ensure even earliest checkpoints have actual I/O to clean up; ordinary runtime start alone creates no Broker socket. The test's provider returns blank and never contacts an external endpoint. A 5s observation bound verifies each failure returns, the pending RPC terminates with Shutdown and the peer sees EOF without a second connection. It then proves zero live runtime workers, joined control handle, exactly one queued-future drop without execution, empty queues/no delayed wakeup, empty processors and all user registrations, unchanged coherent route Arc, unusable old transport, fresh manager replacement and stale-removal safety. External user/runtime Arcs intentionally remain alive throughout the assertions, so cleanup is not merely a consequence of dropping the test's last reference.

Complementary checks:

- `every_initial_startup_failure_cleans_transport_groups_control_and_manager_identity` and `discovery_schedule_failure_joins_all_owned_work`: private checkpoint resource probes, failed-state restart rejection and replacement.
- `tasks::tests::defaults_have_java_initial_delays_periods_and_no_ticks_after_stop`, `slow_callback_serializes_jobs_then_catches_up_from_prior_deadlines`, `shutdown_cancels_and_joins_pending_callback`: paused timers, serial callback/catch-up behavior and terminal cancellation.
- `tests/runtime.rs::different_topic_refreshes_serialize_wire_fetches_without_blocking_snapshot_readers`, `route_refresh_failure_preserves_last_coherent_snapshot`, private route conversion/budget/reader checks: coherent readers and no partial/error overwrite.
- Cumulative P6-V01 and P6-U01 Running/shared-socket tests: successful task-set creation, independent-user guards, unregister-before-EOF and no late frames/ticks.

The reviewed 47-scenario focused command, full workspace, Clippy and fmt/diff results are recorded in the runtime inventory/log. These collectively close the current **P6-V03 unit resource/timer/reader gate**, not complete Phase 6 source parity or live compatibility. Full route/heartbeat/source-branch and final whole-plan reconciliation remains open. P6-V04 remains PENDING_PHASE_19 against server 5.3.4.
