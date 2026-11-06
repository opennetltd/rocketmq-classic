# Phase 13 Lite Pull and POP compatibility matrix

Java reference: `rocketmq-client:5.5.1`, source tag commit `efa1be59d6fc747ad220f036c86030f05f0419f9`. The checkout at `/Users/bruce.tsai/Projects/3rd/rocketmq` is on 5.3.3; phase source inspection must use `git show <pinned-commit>:<path>`. Maven fixtures execute the pinned 5.5.1 artifact.

This matrix tracks Phase 13 implementation evidence. A lower-level request/header model does not establish completion of the high-level consumer flow.

## Current status — 2026-10-27

`P13-V02` passed on the current parity-remediation revision
`7e0e9955f591f773c0f4b2f816951cf57dfd7580` on 2026-10-27 (118 seconds,
clean source). Java and Rust matched POP delivery, expiry redelivery, ACK,
visibility changes, replacement receipt ACK, mixed queue mapping, and
malformed receipt rejection. Raw output is retained locally and uncommitted at
`plan/2026/10/02-project-parity-review-evidence/live/P13-V02/20261027T070426Z/`.
Earlier clean-source pass `99d2dc68` and the full 37/37 aggregate on
`413f735a` remain historical records. Intermediate failed attempts below are
superseded by the clean passes. See
[current compatibility status](CURRENT_STATUS.md).

| Java source/API | Behavior owned by Phase 13 | Rust mapping | Local evidence | Status |
| --- | --- | --- | --- | --- |
| `LitePullConsumer` / `DefaultLitePullConsumer` lifecycle | Constructors, start/shutdown/running state, namespace, group and lifecycle validation | `LitePullConsumer` facade and config | `fixtures/consumer/lite-pop.json`; `tests/lite_pop.rs` covers all five constructor forms, default group, running state, RPC-hook forwarding and namespaced requests | Locally covered |
| Lite subscription and heartbeat | Tag/SQL selector conversion, `buildSubscriptionsForHeartbeat`, subscription/assignment exclusivity, subscribe/unsubscribe and metadata change callback | Lite subscription registry and existing heartbeat/rebalance runtime | `lite::tests::default_subscribe_uses_java_tag_all_subscription`; heartbeat builder success/null-selector cases; `tests/lite_pop.rs::scripted_lite_subscription_unsubscribe_and_metadata_callbacks_match_java` covers selector dispatch, assignment callback, no initial metadata event, changed-route callback and unsubscribe results | Locally covered |
| Lite assigned queue and polling | Assignment replacement, bounded asynchronous prefetch, poll timeout/batch, pause/resume, late result invalidation | Generation-scoped queue state and polling buffer; explicit assignment retains caller queue keys for the public assignment result while using namespaced internal keys | `lite::tests` covers delivered/fetched offsets, stale generations and flow-control boundaries; scripted `lite_pop` cases cover namespace assignment, mode exclusivity, both poll forms, seek versus a late pull, pause/resume and commit/restart delivery | Locally covered |
| Lite offsets | Commit set/map/single/all and deprecated `commitSync` aliases, auto-commit deadline, committed read, timestamp/min/max seek helpers | Lite offset state over existing offset store and admin RPCs | Scripted `lite_pop` cases cover invalid/valid seek, seek-to-begin/end bounds, timestamp lookup success/error, committed Broker read success/error, commit map/set/aliases, persisted offsets and restart position; unit cases cover best-effort auto-commit and empty commits | Locally covered |
| Lite public settings | Defaults and validation for pull threads, batch, flow thresholds, poll timeout, long-poll timeout, consume origin/timestamp, offset store, selector strategy, unit mode, trace controls, broker selection | `LitePullConsumerConfig` configures construction; consumer getters/setters retain Java's post-construction behavior for settings Java reads while running | Java fixture records constructors and `enableStreamRequestType=true`; config tests cover defaults/configured values and validation; post-construction scripted tests verify pull batch changes reach the next request and `poll()` uses its updated timeout; engine setup test compares every inherited `ClientConfig` field; trace tests cover dispatcher lifecycle and wire rows | Locally covered |
| Push POP assignment | `QUERY_ASSIGNMENT`, per-queue PULL/POP assignment, mode transitions, POP retry-topic subscription and assignment callback | Existing typed assignment RPC plus POP queue runtime | `RuntimeConsumerOperationsFixtures.popRebalance` invokes pinned `RebalanceImpl.updateMessageQueueAssignment` for expired assigned, dropped assigned and empty-assignment cases. `broker_assignment_error_preserves_worker_until_empty_assignment` compares those rows, scripts an expired queue replacement, checks a failed assignment leaves its live replacement in place, and verifies dropped-only retention until an authoritative empty assignment. Also `pop_to_pull_transition_*`, core `tests/extensions.rs` and `tests/runtime_lookup.rs`. Consumer operations fixture SHA-256: `3d7af953b38b28a742a0d3eeb2671e8484066203f06ade2c829e2485ead1de4f`. | Local expiry, error-preservation, dropped-retention, empty-assignment and mode-transition paths are covered |
| Push POP pull/consume | POP scheduling, invisible-time bounds/default, batch size, queue flow control, subscription filtering, retry cutoff and listener dispatch | POP request worker and consumer service | `pop::tests` response mapping; `tests/lite_pop.rs` POP settings; `broker_assignment_error_preserves_worker_until_empty_assignment` observes worker `POP_MESSAGE`; `pop_consume_dispatches_ack_redelivery_and_replacement_receipt` scripts listener success and redelivery, ACK and replacement receipt | Local worker and consume paths are covered; current `P13-V02` live replay passed on `7e0e9955`; full 37/37 matrix remains historical |
| POP receipt operations | Preserve `PROPERTY_POP_CK`, parse checkpoint/pop/invisible/revive/retry/Broker/queue/offset data, ACK, source-used batch ACK and change-invisibility receipt replacement | `PopReceipt` retains and validates the raw token; Push invokes ACK/change-invisibility RPCs; batch ACK body construction follows pinned merge keys and bit positions | `pop::tests` (receipt parsing, retry topic, mixed-queue mapping, fallback metadata, batch grouping/range); `pop_batch_expiry_*` checks expiry and malformed receipts; `expired_pop_batch_*` checks listener suppression and found-count release; `pop_consume_dispatches_ack_redelivery_and_replacement_receipt` scripts ACK, redelivery extension, replacement receipt and final ACK; core `tests/extensions.rs` and `tests/rpc_invoked_pop_ack_fixtures.rs` | Local receipt paths are covered; current `P13-V02` live replay passed on `7e0e9955`; full 37/37 matrix remains historical |

## Lite public API crosswalk

| Pinned Java API | Rust mapping | Local evidence |
| --- | --- | --- |
| `start`, `shutdown`, `isRunning`, `isAutoCommit`, `poll()`, `poll(long)` | `start`, `shutdown`, `is_running`, `is_auto_commit`, `poll(None)`, `poll(Some(timeout))` | Lifecycle, timeout, scripted polling and shutdown tests |
| `subscribe(topic)`, `subscribe(topic, expression)`, `subscribe(topic, selector)`, `subscribe(topic, expression, listener)`, `unsubscribe(topic)` | `subscribe`, `subscribe_with_expression`, `subscribe_with_selector`, `subscribe_with_listener`, `unsubscribe` | Default-selector unit test; subscription/selector/listener and unsubscribe scripted test; invalid topic and mode-conflict cases |
| `assign`, `setSubExpressionForAssign`, `assignment`, `buildSubscriptionsForHeartbeat` | `assign`, `set_sub_expression_for_assign`, `assignment`, `build_subscriptions_for_heartbeat` | Assignment expression is observed in a pull request; assignment replacement, empty/late inputs and heartbeat success/error are tested |
| `seek`, `seekToBegin`, `seekToEnd`, `fetchMessageQueues`, `offsetForTimestamp`, `committed` | `seek`, `seek_to_begin`, `seek_to_end`, `fetch_message_queues`, `offset_for_timestamp`, `committed` | Scripted offset bounds, timestamp and metadata successes; invalid seek, RPC failure and pre-start errors |
| `commit()`, `commit(Map, persist)`, `commit(Set, persist)`, `commitSync()`, `commitSync(Map, persist)` | `commit`, `commit_offsets`, `commit_queues`, `commit_sync`, `commit_sync_offsets` | Best-effort automatic/all commit, map/set aliases, persistence and restart position |
| `pause`, `resume`, `setAutoCommit`, `registerTopicMessageQueueChangeListener`, `updateNameServerAddress` | `pause`, `resume`, `set_auto_commit`, `register_topic_message_queue_change_listener`, `update_name_server_address` | Pause/resume, live auto-commit toggle, metadata callback on route change and new name-server route are scripted |
| `PopResult` accessors for messages, status, pop time, invisibility and remaining count | `PopResult::{new,messages,set_messages,status,set_status,pop_time,set_pop_time,invisible_time,set_invisible_time,rest_num,set_rest_num}` map the Java POP result getters/setters; the Rust slice getter borrows the decoded message list | `pop::tests` covers response decoration, found/non-found statuses, fallback metadata, decoded messages and field values |
| POP receipt token parsing and request-header construction | `PopReceipt::{parse,raw,checkpoint_queue_offset,pop_time,invisible_time,revive_queue_id,retry,broker_name,queue_id,queue_offset,real_topic,ack_header,change_invisible_header}`; the last three helpers build Java-compatible retry-topic routing and ACK/change-invisibility headers | `pop::tests` covers all parsed receipt fields, retry-topic forms, ACK headers, mixed-queue batch grouping, range errors and receipt replacement |
| Lite heartbeat subscription inspection | `LitePullConsumer::subscriptions_for_heartbeat` exposes the Rust-owned mutable shared set used by `build_subscriptions_for_heartbeat`; this is a Rust handle accessor, not a separate Java facade operation | `heartbeat_builder_mutates_the_retained_subscription_set_in_place` checks replacement and stable handle identity; `heartbeat_builder_rejects_null_selectors_like_java` checks the error boundary |
| Five `DefaultLitePullConsumer` constructors, including RPC-hook and deprecated namespace overloads | `LitePullConsumerConfig::new_default/new`, `ClientConfig` namespace fields, `LitePullConsumer::new/new_with_rpc_hook` | Pinned constructor inventory and RPC-hook/namespace integration test |
| Public Lite settings and inherited `ClientConfig` getters/setters | `LitePullConsumerConfig` (exact public accessor inventory below); the facade also exposes consumer-level getters/setters for Java settings read dynamically during operation | Pinned defaults/configured values, stream-request default, validation, name-server update, namespace, trace lifecycle, plus post-construction setter/getter and scripted pull/poll checks |

## Rust public-surface reverse-scan follow-up

`rocketmq_classic_consumer::pop::build_batch_ack_body` had no corresponding
Java public consumer method and was used only by the Rust unit tests. It is now
compiled only for tests and is private; the module doctest verifies external
code cannot import it. Production batch-ACK behavior remains covered by the
receipt and scripted ACK tests above.

## Pinned Java public surface and defaults

The fixture records all `LitePullConsumer` interface signatures, all declared public `DefaultLitePullConsumer` method signatures and the five public constructors. It invokes selected public getters/setters and captures config validation. Rust's `new_default` preserves Java's no-argument constructor group (`DEFAULT_CONSUMER`), which is rejected at startup; `set_consume_from_where` rejects the three enum values Java rejects. Defaults observed from the 5.5.1 artifact include clustering mode, auto-commit enabled, 20 pull threads, a 5-second auto-commit interval, batch 10, all-queue threshold 10000, queue count threshold 1000, queue body-size threshold 100 MiB, max consume span 2000, 5-second default poll timeout, 30-second metadata polling, last-offset consumption and a timestamp 30 minutes in the past. Setting an auto-commit interval below 1000 ms is ignored. The heartbeat builder replaces its retained subscription set with the selector map's built subscriptions. Inherited `ClientConfig` settings are exposed through `LitePullConsumerConfig::client_config` and copied into the runtime configuration.

Exact public `LitePullConsumerConfig` accessors:

- Construction and inherited transport: `new_default`, `new`, `client_config`, `client_config_mut`, `tls_config`, `set_tls_config`, `consumer_group`, `set_consumer_group`.
- Consumer and assignment: `broker_suspend_max_time_millis` (getter only), `consumer_timeout_millis_when_suspend`, `set_consumer_timeout_millis_when_suspend`, `consumer_pull_timeout_millis`, `set_consumer_pull_timeout_millis`, `message_model`, `set_message_model`, `message_queue_listener`, `set_message_queue_listener`, `offset_store`, `set_offset_store`, `allocate_message_queue_strategy`, `set_allocate_message_queue_strategy`, `unit_mode`, `set_unit_mode`, `consume_from_where`, `set_consume_from_where`, `consume_timestamp`, `set_consume_timestamp`.
- Polling and flow control: `auto_commit`, `set_auto_commit`, `pull_thread_nums`, `set_pull_thread_nums`, `auto_commit_interval_millis`, `set_auto_commit_interval_millis`, `pull_batch_size`, `set_pull_batch_size`, `pull_threshold_for_all`, `set_pull_threshold_for_all`, `consume_max_span`, `set_consume_max_span`, `pull_threshold_for_queue`, `set_pull_threshold_for_queue`, `pull_threshold_size_for_queue`, `set_pull_threshold_size_for_queue`, `poll_timeout_millis`, `set_poll_timeout_millis`, `topic_metadata_check_interval_millis`, `set_topic_metadata_check_interval_millis`.
- Broker selection and trace: `connect_broker_by_user`, `set_connect_broker_by_user`, `default_broker_id`, `set_default_broker_id`, `enable_msg_trace`, `set_enable_msg_trace`, `customized_trace_topic`, `set_customized_trace_topic`.

The post-construction setter audit used the pinned `DefaultLitePullConsumer` call graph. Java reads pull batch size, all/queue/size flow thresholds, consume max span, `consumerTimeoutMillisWhenSuspend` and auto-commit interval during worker/poll operation, and reads poll timeout on each no-argument `poll()`. Rust exposes consumer-level atomic getters/setters for these live values; pull thread count and metadata interval are sampled when `start()` creates workers/schedules. Auto-commit and broker-selection settings already have consumer-level setters, and changing the message-queue listener updates the active Pull runtime. Most other Lite settings are consumed when Java starts or constructs its runtime and remain configurable through `LitePullConsumerConfig` before Rust constructs the consumer. Java's `consumerPullTimeoutMillis` is passed into the Lite pull helper, but the blocking Lite path uses `consumerTimeoutMillisWhenSuspend`; Rust retains the setting and the same effective timeout behavior. Java exposes `getBrokerSuspendMaxTimeMillis` without a setter, so Lite config now keeps the 20000 ms default fixed and exposes only the getter.

Inherited Java `ClientConfig` settings map to the same-named Rust properties and accessors: name-server address, client IP, instance name, callback thread count, namespace/namespace-v2, access channel, name-server/heartbeat/offset intervals, pull exception delay, trace batch count, unit mode/name, body decode/decompress, VIP channel, heartbeat v2, TLS, SOCKS proxy, MQ API timeout, detector timeout/interval, language, stream request type, send-latency/start-detector/channel-event/concurrent-heartbeat controls, trace enable/topic, and metadata page size. Lite's `unitMode`, `enableMsgTrace`, and customized trace topic use the corresponding `ClientConfig` fields as Java does. `inherited_java_client_config_is_copied_to_the_pull_engine` sets these values and checks the complete cloned config, including the Lite unit-mode value passed to the Pull runtime.

The pinned `DefaultLitePullConsumer` getter/setter names also map directly to
the config and facade accessors: `getAllocateMessageQueueStrategy`,
`getAutoCommitIntervalMillis`, `getConsumeFromWhere`, `getConsumeMaxSpan`,
`getConsumeTimestamp`, `getConsumerGroup`, `getConsumerPullTimeoutMillis`,
`getConsumerTimeoutMillisWhenSuspend`, `getCustomizedTraceTopic`,
`getDefaultBrokerId`, `getMessageModel`, `getPollTimeoutMillis`,
`getPullBatchSize`, `getPullThreadNums`, `getPullThresholdForAll`,
`getPullThresholdForQueue`, `getPullThresholdSizeForQueue`,
`getSubscriptionsForHeartbeat`, `getTopicMetadataCheckIntervalMillis`,
`getTraceDispatcher`, `isConnectBrokerByUser`, and `isEnableMsgTrace`, plus
`setAutoCommitIntervalMillis`, `setConsumeFromWhere`, `setConsumeMaxSpan`,
`setConsumeTimestamp`, `setConsumerGroup`, `setConsumerPullTimeoutMillis`,
`setConsumerTimeoutMillisWhenSuspend`, `setCustomizedTraceTopic`,
`setEnableMsgTrace`, `setMessageModel`, `setMessageQueueListener`,
`setPollTimeoutMillis`, `setPullBatchSize`, `setPullThreadNums`,
`setPullThresholdForAll`, `setPullThresholdForQueue`,
`setPullThresholdSizeForQueue`, and
`setTopicMetadataCheckIntervalMillis`. Configuration methods map to
`LitePullConsumerConfig`'s corresponding snake-case accessors. The facade's
live settings map to `LitePullConsumer` getters/setters for auto-commit
interval, pull threads/batch, queue thresholds, consume span, poll and
consumer timeouts, broker selection and message-queue listener;
`getSubscriptionsForHeartbeat` and `getTraceDispatcher` map to the retained
subscription handle and owned dispatcher. The pinned Java reflection
inventory is `fixtures/consumer/facade-api.json`; defaults/validation,
post-construction reads, runtime effects, trace lifecycle, and subscription
identity are covered by the existing Lite config, `lite_pop`, and trace tests.
The callable-name crosswalk does not replace the broader P18-V01
per-input/branch review.

The Push POP defaults are queue wait-ACK threshold 96, invisibility 60000 ms, and POP batch count 32. The Java `ExtraInfoUtil` fixture records the unmodified receipt string and each decoded routing/offset field. `PopResult` exposes `FOUND`, `NO_NEW_MSG`, `POLLING_FULL`, and `POLLING_NOT_FOUND` plus pop time, invisibility, remaining count and message list.

Typed request/response bodies and low-level POP/ACK RPCs are already present in protocol/core crates. The Rust tree has a Lite scheduler/polling facade with consume trace dispatch and a Push POP worker/receipt path, backed by pinned Java fixtures, state tests, controlled expiry checks and scripted peers. Local P13-V01 and P13-V03 Lite API/configuration gates and P13-U01 expiry/redelivery/replacement checks are covered. P13-V02 now passes as a Phase 19 live Broker/Docker gate; its local scripted POP evidence and live Java/Rust comparison are recorded above. The first attempt omitted POP mode setup, the second set mode too late, and the third prepared both Java and Rust groups before seeding but still got `POLLING_NOT_FOUND`. A fourth attempt found that direct Java queue 0 seeding was returned on queue 1. A fifth used the route-aware selector and exposed the max-offset visibility delay. A sixth passed both seed checks with max offset 1 but Java POP still returned `POLLING_NOT_FOUND`. A seventh passed the same seed checks and used only the pre-seed request mode with init mode 0, but POP still returned `POLLING_NOT_FOUND`. An eighth capture showed queue 0 min/max offsets `0/1` while the Java POP group already had committed offset 1 before the first POP request. A ninth showed all six group offsets were `NOT_FOUND` after prepare and seeding, then 1 before Java POP. Run `20261025T124657Z` verified the explicit post-seed reset; later runs traced initial delivery and expiry redelivery, ending with the successful Java/Rust run recorded below.

Run `20261025T124657Z` verified offset 0 for both queues of all six Java/Rust
groups after seeding, but the separate Java consume process observed offset 1
before its first POP and got `POLLING_NOT_FOUND`. This failed intermediate
attempt was superseded by the clean-source passing run recorded below.

Run `20261025T125153Z` clarifies the offset reset result: Java's first POP
request began at offset 0 and returned the seeded message. Offset 1 appeared
afterward; the following expiry redelivery request returned
`POLLING_NOT_FOUND`. Earlier failure diagnostics identify the failing request,
not necessarily the initial delivery. In that run, the remaining failure was expiry redelivery rather than initial
seed consumption.

Run `20261025T125713Z` passed all seven Java lifecycle assertions, including
expiry redelivery after a 6-second wait. Rust received a POP message but the
live low-level probe omitted Java's response-header-to-`POP_CK` decoration and
stopped before Rust lifecycle assertions. The probe now applies this mapping;
the subsequent clean Java/Rust run passed.

## Phase 19 live Broker result

The clean-source run `20261025T130156Z` on `99d2dc68484ab8de43da86b997dc20dc49267c3c` passed `P13-V02`; the later 2026-10-26 aggregate recorded 37/37 on `413f735a`. The scenario was rerun on current revision `7e0e9955` in `20261027T070426Z` and passed the same Java/Rust receipt lifecycle assertions. Its raw output is retained locally and uncommitted under `plan/2026/10/02-project-parity-review-evidence/live/P13-V02/`. Each run's runner removed its scenario topic/groups and isolated containers, volumes, and network. The full 37-scenario matrix was not rerun on `7e0e9955`.
