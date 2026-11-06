# Phase 11 offline method and allocation matrix

Java behavior source: RocketMQ client 5.5.1, commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`. Allocation fixtures invoke the
published Java client strategies directly; no Broker was contacted.

## Phase 18 continuation — 2026-10-20

The historical method rows below describe their listed tests, not exhaustive
branch coverage. The runtime operation continuation corrects unsubscribed
truncation (`98fb3feb`) and awaits the target rebalance on resume (`ee7e6d7f`).
Three additional Java reset/resume cases exercise dropped queue retention
when its topic remains subscribed: a missing route, an unchanged broadcast
assignment, and a stale pull timestamp. Rust must not run persistence/removal
solely because a queue is dropped. Their values and store events are checked in
`runtime_reset_matches_java_live_maps_nulls_and_per_queue_failures` using
`rebalanceRetention` in `fixtures/runtime/consumer-operations.json`.
The Java pull-idle property and strict expiry path now match for Push and POP
queues. Concurrent `ProcessQueue.cleanExpiredMsg` now records the Java
consume-start property and runs on the Java initial-delay/period schedule;
success, send-back failure, orderly skip, late completion and cleanup timing
are checked against an invoked Java fixture. Push POP retry-subscription
transitions for push-to-pop, pop-to-push, mixed, retry-topic and empty
assignments now match the invoked Java implementation, including retention of
the cached retry route after unsubscribe. Other assignment expiry/retention
dimensions, inherited rebalance call correspondence and the full
inherited/source audit remain under review; P18-V01 is open. Public `ProcessQueue` and
`PopProcessQueue` methods and selected live rebalance tables now have direct
Java/Rust checks. See the
[scoped runtime operation audit](PHASE_18_RUNTIME_CONSUMER_OPERATIONS.md) and
[nested implementation API audit](PHASE_18_REBALANCE_PROCESS_QUEUE_API.md).

## Java method to Rust implementation and check

| Java 5.5.1 source and method | Rust owner and check | Status |
| --- | --- | --- |
| `AllocateMessageQueueStrategy.allocate/getName`; `AbstractAllocateMessageQueueStrategy.check` | `rebalance::strategy::AllocateMessageQueueStrategy` and `RebalanceError`; `tests/rebalance.rs` | UNIT_VERIFIED — exact input-validation precedence, missing-current result and all strategy names match Java fixtures |
| `AllocateMessageQueueAveragely.allocate` | `AllocateMessageQueueAveragely`; `tests/rebalance.rs::average_allocation_matches_java_551_fixtures_and_partition_invariants` | UNIT_VERIFIED — empty input errors, missing current client, 1/N queues and clients, 10 queues/3 clients, Java UTF-16 ordering, disjointness and coverage |
| `AllocateMessageQueueAveragelyByCircle.allocate` | `AllocateMessageQueueAveragelyByCircle`; P11-V01 Java differential | UNIT_VERIFIED — queue modulo consumer assignment matches Java |
| `AllocateMessageQueueByConfig.allocate/getMessageQueueList/setMessageQueueList` | `AllocateMessageQueueByConfig::{message_queue_list,set_message_queue_list}`; P11-V01 Java differential | UNIT_VERIFIED — configured queues are returned without filtering; unset list maps to empty assignment |
| `AllocateMessageQueueByMachineRoom.allocate/getConsumeridcs/setConsumeridcs` | `AllocateMessageQueueByMachineRoom::{consumeridcs,set_consumeridcs}`; P11-V01 Java differential and split edge checks | UNIT_VERIFIED — Java room filtering, sorting and quotient/remainder assignment match |
| `AllocateMessageQueueConsistentHash.allocate`; constructors `()`, `(int)`, `(int, HashFunction)`; `ConsistentHashRouter.routeNode`; `VirtualNode.getKey`; default MD5 and custom `HashFunction.hash(String) -> long` | `AllocateMessageQueueConsistentHash::{new,with_virtual_node_count,with_hash_function,virtual_node_count}`; `with_hash_function` maps Java's `(int, HashFunction)` constructor, and the Rust trait's `i64` result matches Java `long` | UNIT_VERIFIED — Java fixture invokes both default and custom-hash constructors; Rust differential uses Java-compatible `String.hashCode` and covers ten virtual nodes, signed custom hashes, ring collisions and queue-string hash inputs |
| `DefaultMQPullConsumer.get/setAllocateMessageQueueStrategy`, `isEnableRebalance/setEnableRebalance`, queue-listener and registration methods | `PullConsumerConfig` plus `PullConsumer::{register_message_queue_listener, set_message_queue_listener, register_topics, set_register_topics, add_register_subscriptions, clear_register_subscriptions, enable_rebalance}`; P11-U02 default, namespace, selector and runtime tests | UNIT_VERIFIED — average allocation and enabled rebalance are defaults; listeners, registered topics/subscriptions and runtime enablement are wired |
| `MQPullConsumer.registerMessageQueueListener`; `DefaultMQPullConsumerImpl.fetchMessageQueuesInBalance` | `PullConsumer::{register_message_queue_listener, fetch_message_queues_in_balance}`; P11-U02 lifecycle and namespace tests | UNIT_VERIFIED — callbacks carry the all/assigned sets; the in-balance query requires a running consumer and removes the configured namespace from returned queues |
| `RebalanceImpl.doRebalance/rebalanceByTopic/updateProcessQueueTableInRebalance` | Push and Pull `ConsumerRuntime::try_rebalance`; P11-U01/P11-U02 membership, empty-route, broadcast, listener, persistence and generation-fencing tests | UNIT_VERIFIED — route absence/emptiness is handled before membership lookup; null/empty membership preserves existing Push assignments and Java's default `balanced=true` result, while membership RPC errors preserve assignments and request retry; LiteSelective follows Java's no-op default branch |
| `ProcessQueue.isPullExpired/cleanExpiredMsg`; `ConsumeMessageConcurrentlyService.cleanExpireMsg` | Push `ProcessQueue::expired_head_message/remove_head_message`; concurrent-service owned periodic sweep and `PushConsumer::clean_expired_process_queue_messages_at`; `expired_concurrent_messages_match_java_process_queue_cleanup`, `concurrent_consume_start_timestamp_allows_expiry_while_listener_is_running`, `concurrent_consume_service_runs_expiry_sweep_after_java_initial_delay` | UNIT_VERIFIED — strict timeout, Java-style first-read cache for `ProcessQueue.PULL_MAX_IDLE_TIME` (separate from the POP static cache), consume-start property, oldest-message order, delay level 3, send-back failure retention/retry, orderly skip, 16-attempt cap and late listener completion match the invoked Java cleanup cases. `process_queue_pull_idle_property_keeps_its_first_java_class_value` and `pop_process_queue_pull_idle_property_keeps_its_first_java_class_value` check cache lifetime; `broker_assignment_error_preserves_worker_until_empty_assignment` checks that POP rebalance uses its own timeout slot. Direct nested public API access remains open per the Phase 18 API audit. |
| `RebalancePushImpl.computePullFromWhereWithException/removeUnnecessaryMessageQueue/messageQueueChanged` | Push initial offset and assignment lifecycle; P11-V02/P11-U01 | UNIT_VERIFIED — Java initial-offset fixtures match, query failures do not fall back to zero, failed removal persistence retains the dropped generation, and assignment changes notify the listener |
| `RebalancePullImpl.removeUnnecessaryMessageQueue/messageQueueChanged` | Pull assignment table reconciliation; P11-U02 broadcast/clustering lifecycle tests | UNIT_VERIFIED — removed offsets persist before removal, newly assigned queues clear stale offsets, callback failures are isolated, and unavailable membership keeps the old assignment |
| `RebalanceService.run` | Runtime-owned coalesced rebalance worker and Push/Pull `try_rebalance`; P11-U01/P11-U02 serialization, membership-transition, race and shutdown tests | UNIT_VERIFIED — startup, topic updates and membership notifications reach serialized reconciliation; retries retain safe queue state and owned workers stop on shutdown |

## Java allocation fixture

`fixtures/rebalance/allocation.json` is generated and drift-checked by
`compat/java-reference/src/main/java/compat/RebalanceFixtures.java` using the
actual 5.5.1 classes. It covers all five Java strategies plus the custom-hash
constructor; zero/one/ten queues;
one/three consumers; the 10q/3c case; missing current client; invalid empty
inputs; Unicode topic, broker and client ordering; config-selected queues;
machine-room filters; and default consistent hashing. Java source paths are
recorded in fixture metadata. Fixture SHA-256:
`9a0cfabcf4234e955f19c5d5cebd438de25a0b5f45b5386d4c880549c91a0d8b`.

`fixtures/rebalance/initial-offsets.json` is generated and drift-checked by
`compat/java-reference/src/main/java/compat/RebalanceInitialOffsetFixtures.java`
against the same pinned Java client. Fixture SHA-256:
`6db5b5ddc9ba68c85dd51ef769394ae8551f678412db5f1cae83b05f118435a4`.

## Case status

| Case | Local evidence | Status |
| --- | --- | --- |
| P11-V01 | Java fixture plus direct Rust differential for all five strategies and custom consistent-hash function | UNIT_VERIFIED |
| P11-V02 | Java initial-offset fixture differential; failed query/persist and replacement-generation fencing | UNIT_VERIFIED — `java_initial_offset_fixtures_match_policy_and_retry_topic_selection`, `initial_offsets_prefer_store_and_never_turn_query_failures_into_zero`, `failed_offset_persist_retains_the_dropped_generation_for_retry`, and `activation_fences_old_generations_and_rolls_back_scheduler_failure` |
| P11-U01 | Membership transitions, bounded retry, pull/listener barrier races, concurrent triggers and shutdown ownership | UNIT_VERIFIED — consumer library tests cover join/leave convergence, missing-route notifications, in-flight pull/listener fencing, persist retry, serialized triggers, startup and shutdown |
| P11-U02 | Pull rebalance settings and public registration/query API; broadcasting and clustering assignment, listener callbacks, safe offset removal and retry on unavailable membership | UNIT_VERIFIED — `pull_rebalance_settings_default_to_java_values_and_registration_sets_listener`, `pull_runtime_rebalance_assigns_all_broadcast_queues_and_persists_removals`, `pull_clustering_rebalance_uses_membership_and_keeps_assignment_when_unavailable`, and `startup_registration_and_shutdown_obey_consumer_lifecycle` |
| P11-V03 | Live mixed-client Push group join/leave convergence | LIVE_VERIFIED — clean-source run `20261025T154631Z` (raw output not retained) confirmed Java owned all eight scenario queues, joined ownership settled to four queues per client with no overlap across four stable samples, and Rust owned all eight after Java left. One transient overlapping snapshot during convergence was tolerated; message redelivery remains allowed. |
| P11-V04 | Live bidirectional Java/Rust Push producer/consumer paths, retries, offsets, and shutdown | LIVE_VERIFIED — clean-source run `20261025T155944Z` (raw output not retained) passed one retry in each direction, matching offset 1 on both Brokers, and three quiescent shutdown cycles per client. |

Orderly assignment was partial at the Phase 11 checkpoint and was completed by
Phase 12, which activates Java-compatible Broker queue locks and verifies lock
renewal, expiry, queue serialization, callback outcomes, and shutdown cleanup
with scripted RPC and paused-time tests. Live mixed-client Broker contention
remains pending Phase 19 (`P12-V03`).

## Validation record

Latest full local gates passed on 2026-10-12 on the repository host (ARM64
macOS; Maven 3.10.0 using JDK 27):

- `cargo fmt --all -- --check`
- `cargo test --workspace --locked` (including 130 core tests and 81 consumer library tests)
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `cargo build -p rocketmq-classic --examples --locked`
- `mvn -o -q -f compat/java-reference/pom.xml verify` (31 differential scenarios, pinned Java fixtures and Rust-generated fixture cross-decodes)
- `python3 tests/differential/compare.py --self-test`
- `python3 tests/differential/compare.py --validate-manifest` (31 scenarios)
- `git diff --check`

A subsequent Linux ARM64 `cargo test --workspace --locked` run also passed on
2026-10-12 after correcting two order/timing assumptions in existing remoting
integration assertions. It exercises the Phase 11 local suite on Linux; it does
not add Broker or cross-language evidence.

The earlier workspace failure depended on a fixture storing the generating
host's interface address; the runtime fixture now compares resolver behavior
without storing host-specific interface data. The earlier Maven fixture drift
is also resolved, and the full offline verify now passes. P11-V03/V04 remain
pending Phase 19; these offline gates do not claim live Broker results.

The preceding complete gate run used the 249-case Heartbeat DTO checkpoint
(fixture SHA-256
`98b5029aafaa9146a801e3a68d7edc70707f25ba4c17d3ceca40012e4673e0b3`). The
latest rerun uses the current 276-case fixture, SHA-256
`67b844d3a61ee31ac12c340cf09f30d81083435635d6f17bebc44206b65d37cc`; fmt,
workspace tests, strict Clippy, example build, offline Maven verify,
comparator self-test, manifest validation, and diff check all pass. See the
[latest local gate record](../../rlogs/2026/10/12-phase-00-11-gate-rerun__20261012T033153Z.rlog).

## Phase 19 source-case handoff coverage

A handoff inventory audit on 2026-10-12 found that the manifest omitted the
original pending IDs P0-V03, P3-V04, P7-V02/V03/V04, P9-V03, P10-V03, and
P11-V03/V04. These nine cases are now registered as `PENDING_PHASE_19`;
P5-V02 remains split into four detailed rows linked by `source_case_id`, and
P6-V04 remains registered. `compare.py` now reads unchecked verification IDs
from the Phase 0–11 plans and requires them in the manifest; the current set
has eleven IDs. Its self-test verifies that dropping P11-V04 fails. The
manifest validator passes with 31 scenarios. This records the full handoff
inventory; none of these changes claim live Broker execution.
