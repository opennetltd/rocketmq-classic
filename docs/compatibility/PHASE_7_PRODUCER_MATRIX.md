# Phase 7 Producer compatibility matrix

Oracle: pinned client `org.apache.rocketmq:rocketmq-client:5.5.1`, source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`; the production server baseline remains
5.3.4. Java source and offline fixtures use the pinned tag. No live Broker or
NameServer was contacted.

## Pinned source paths and call sites

All Java references below are read from the pinned commit above, not the newer
HEAD in the Java checkout.

| Java source | Methods and call sites checked | Rust mapping |
| --- | --- | --- |
| `client/src/main/java/org/apache/rocketmq/client/producer/DefaultMQProducer.java` | `start`, `shutdown`, `fetchPublishMessageQueues`; default, explicit-queue, selector, oneway, and collection `send` overloads; wrapper dispatch to `sendDirect` / `sendByAccumulator`; `batch(Collection)`; producer setting getters/setters | `Producer::{start,shutdown,fetch_publish_message_queues}`; `send*` and `send_batch*`; `ProducerConfig` |
| `client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java` | `start`, `shutdown`, `sendDefaultImpl`, `sendSelectImpl`, `sendKernelImpl`, `tryToFindTopicPublishInfo`, `selectOneMessageQueue`, `updateFaultItem`, `pickTopic`; sync/future/oneway mode branches and the `ServiceDetector.detect` callback | `Producer` lifecycle, route cache, `send`, `selector`, `fault`, and `detect_brokers`; the detector calls `MQClientApi::probe_broker` |
| `client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java` | `resumeCheckHalfMessage` sends request 323 with a nullable transaction topic/message ID and uses `brokerVIPChannel`; the result is true only for `SUCCESS` | `MQClientApi::{resume_check_half_message,resume_check_half_message_nullable}`; the invoked fixture and Rust loopback differential cover the request fields, nullable inputs, result code and VIP address transform |
| `client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java` | `exportPopRecord` sends request 200056 without header/body, passes the supplied Broker address unchanged even with VIP enabled, and raises on non-success | `MQClientApi::export_pop_record`; the invoked fixture and VIP-enabled Rust loopback compare the request envelope, address and outcomes |
| `client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java` | `switchTimerEngine` sends request 5001 with the `E_T` field, passes the supplied Broker address unchanged even with VIP enabled, and raises on non-success | `MQClientApi::switch_timer_engine`; the invoked fixture and VIP-enabled Rust loopback compare nonempty/empty field values, request body, address and outcomes |
| `client/src/main/java/org/apache/rocketmq/client/Validators.java`; `common/.../constant/PermName.java`; `common/.../topic/TopicValidator.java` | `checkGroup`, `checkMessage`, `checkTopic`, `checkTopicConfig`, `checkBrokerConfig`, `isSystemTopic`, `isNotAllowedSendTopic`; topic classification remains distinct from the send-forbidden check | Public `validation::{is_system_topic,validate_group,validate_message,validate_send_topic,validate_system_topic,validate_topic,check_topic_config,check_broker_config}`; Common `PermName` maps all eight Java constants and methods, including the string overload's Java integer parsing. `MQClientApi::{create_topic,update_broker_config}` maps topic permission validation and the Broker config RPC, including empty-properties no-op, UTF-8 properties body, configured VIP port offset, and Broker errors. Fixture `validatorsApi`, `permNameApi`, `permNameCases`, `permNameStringCases`, `topicPermissions`, and `brokerPermissions` in `fixtures/producer/core.json` had SHA-256 `8216bf9c1a66b57a61e88af448f61ab38dc84015ffd872ee340d68896d3ec9b5` at that slice; subsequent admin RPC fixture additions advanced it through `53f8d3ae387f5df155d0c10484815f3a71d00debafb70c82ed0912f3848bca2f`, `c20f4474d674bd4e0001f19c82fed8c5e2dfbe6f5c911ee6eade0fb1842c7cb0`, `434a3fe8b1cba71732f41ccb710e601e0d8ce4cbcf196265695cef7641b90905`, `6e0a6f94f934f9e173aa76a5e4e3ab18c70ee1121f41f1dfbb52fbbac303a2ef`, `c81645968935a1bb317952c0ea32c7bbf4a029e62fa879af1863a06a40d70f28`, and `fbe6f70615e8004c26bdba90fdc824241415d19557b57853a6a563f24becbd32`, `38b2eac88f25aa04674f469b4e7a685dc599834969f6a1d24613de8c98efdb23`, `2479e9af49895298bf807b04775db0e3acef6ad2b6a0a6e2cf59a830ca89169e`, `9906c82ae37e41e26b29bfc35c6c12790fbb5c31944a2f66b8096a73572f79f5`, `0e053e88a7059cebc127410b9d1de1f35c899e52892e7bfc256dde1aa91e653b`, and `ac3462b7351f327839c4a79db5a8a499f6fdbbd56f667f74b2ac5f18fb368fee`, and `fcf27463aa5fde04867de50543847ad53dabc1b5c3b526ae8ac2406481514ec7`, `33b7788c15092caaeb2e5cc42ac69eb7bcda5baf5ebc27c2caf01aac9c5e9ff3`, and `bb5a19cb131f922828ff5effb8eb52ac4a0d95e8bae2863775214da4d6664e5b`, and `4276123552ab57632da4a10177b1b529266707234e95785668cfcd12177e783c` to current SHA-256 `bb28b038fdd66160007d8c757e53ed4cb78ebcad0a91b4ff9d950fa04eccee6c`. `perm_name_*_match_the_pinned_java_fixture`, `pinned_java_producer_core_fixture_matches_defaults_and_validation`, and Core admin RPC tests compare the API, values, failures, request bytes and response paths. Collection sends still validate original elements before `batch::encode_prepared_batch` constructs the wire message. |
| `common/src/main/java/org/apache/rocketmq/common/message/MessageClientIDSetter.java` | `createUniqID`, `setUniqID`, `getUniqID`, `getNearlyTimeFromID`, `getIPStrFromID`, `getIPFromID`, `getPidFromID`, `createFakeIP` | `UniqueIdGenerator::{create, set_if_missing}`, `Message::get_property`, and `unique_id::{get_nearly_time_from_id, get_ip_str_from_id, get_ip_from_id, get_pid_from_id, create_fake_ip}`. `uniqueIdHelpers.publicApi` reflects the public no-argument constructor, zero public fields, and all eight declared/callable public methods; `public_unique_id_helpers_match_the_pinned_java_fixture` pins those signatures and existing behavior checks cover IPv4/IPv6 fields and address text, near-time reconstruction relative to the selected month start, short/odd/invalid hex inputs, and fake-IP length. The stateless Java utility constructor is represented by the Rust `unique_id` namespace. Fixture SHA-256: `af16e63df4a3506daaf22162afdf7b52655861bbf0cffea072983a4dacfa25b6`. The Java fixture stores the near-time delta rather than an absolute epoch, so it remains stable across timezones and month rollovers. Month boundaries use the process-local timezone like Java `Calendar`; the differential passes in UTC and `TZ=Asia/Taipei`. |
| `common/src/main/java/org/apache/rocketmq/common/message/MessageBatch.java` | Public `List<Message>` constructor, `generateFromList`, `encode`, `iterator`; inherited `Message` API | `MessageBatch::{new,new_nullable,generate_from_list,generate_from_list_nullable,encode}`, `IntoIterator` for owned/shared/mutable batches, and `Deref<Target = Message>`. Nullable adapters represent Java null collection/elements; constructor input remains unchecked and null failures occur at encode/iteration, while `generateFromList` rejects null/empty inputs. Invoked fixture and Rust tests cover four delay properties, retry topics, topic/wait-store mismatches, null inputs, exception classes/messages and compact encoding; Producer collection sends call `generate_from_list` before input mutation. |
| `client/src/main/java/org/apache/rocketmq/client/impl/producer/TopicPublishInfo.java` | `ok`, queue-list/order/router-info/route/cursor getters and setters, filtered selection, previous-broker avoidance, reset, write-queue count, diagnostic text | public `TopicPublishInfo`; live queue-list, route-data, and cursor handles; `FaultStrategy` reads the retained per-info cursor |
| `client/src/main/java/org/apache/rocketmq/client/latency/MQFaultStrategy.java`; `LatencyFaultToleranceImpl.java` | `selectOneMessageQueue`, `updateFaultItem`, `isAvailable`, `detectByOneRound`; `DefaultMQProducerImpl` supplies `ServiceDetector.detect` using `MQClientAPIImpl.getMaxOffset(endpoint, queue, timeoutMillis)` | `FaultStrategy` and producer-owned detector task; `MQClientApi::probe_broker` sends `GET_MAX_OFFSET` for queue 0 with `committed=true` and treats only a successful, valid offset response as reachability. The public nested `LatencyFaultToleranceImpl.FaultItem` value API is also exported and verified below. |

`TopicPublishInfo::from_snapshot` is an internal adapter from the shared runtime
route snapshot and has no Java public constructor equivalent. It is
crate-private so it does not expand the exported Producer API.
The implementation-only `ProducerQueueIndex::current`,
`TopicPublishInfo::snapshot`, and `TopicPublishInfoTable::{snapshot,keys}` are
also crate-private. Java exposes `ThreadLocalIndex.incrementAndGet/reset`, the
individual `TopicPublishInfo` state accessors, and live Map views; Rust uses
these snapshot/current helpers only for diagnostic text and internal runtime
route/topic enumeration.

Java callback overloads are represented by Rust future-mode methods as allowed
by the Phase 7 plan; future scheduling is selected by the caller's Tokio
runtime. Rust `Producer` is itself the implementation handle. Its
`default_mq_producer_impl()` and `default_mq_producer()` accessors return
borrowed references to that same owner, preserving Java's facade/implementation
identity without allocating a separate wrapper.

## `DefaultMQProducerImpl` declared-public surface review

The invoked Java fixture now records both public constructors and all declared
public method signatures on the pinned implementation class. The pinned artifact
has 2 public constructors and 82 declared public methods; the Rust inventory
regression pins the count and the presence of direct hook and state/table
accessors. This is a declaration inventory, not proof that every entrypoint's
inputs and branches are equivalent.

`pinned_java_producer_impl_method_names_have_phase_crosswalks` checks all 82
fixture signatures (61 distinct Java method names) against the Phase 7 and
owning Phase 14–17 matrices. This guards method-name inventory coverage only;
per-overload input, error and control-flow branches remain part of P18-V01.

| Java implementation entrypoints | Rust owner mapping | P18-V01 disposition |
| --- | --- | --- |
| `start`, `start(boolean)`, `shutdown`, `shutdown(boolean)`, `send*`, queue selection, route refresh, fault updates, topic/admin queries | `Producer::{start,start_with_factory,shutdown,shutdown_with_factory}`; route and RPC operations are mapped in this matrix and Phase 17 | The no-argument methods delegate with `true`; `start_with_factory(false)` registers Producer-owned work without starting the shared runtime, synchronously initializes configured topic routes, skips lookup for usable cached publish info, falls back through Java's default topic when needed, and sends the immediate heartbeat using cached routes; `shutdown_with_factory(false)` completes Producer cleanup without shutting down that runtime. The pinned Java lifecycle fixture records the factory still RUNNING after `shutdown(false)`; `factory_lifecycle_overloads_leave_runtime_control_to_the_caller`, `producer_start_initializes_configured_topic_routes_and_default_fallback` (including the live default queue-count cap), `producer_start_skips_route_lookup_for_cached_publish_info`, `producer_start_propagates_route_transport_failure_and_releases_lease`, and `producer_start_sends_a_heartbeat_to_cached_brokers` cover these branches. Per-overload input/error branches for other raw helpers remain open. |
| `getPublishTopicList`, `isPublishTopicNeedUpdate(String)`, `updateTopicPublishInfo(String, TopicPublishInfo)` | `ProducerRuntime::{publish_topics,is_publish_topic_need_update,update_topic_publish_info}` plus the Producer-owned publish-info cache | Pinned `MQClientInstance` calls these methods to enumerate producer topics, decide whether an empty/missing route needs refresh, and publish accepted route snapshots back to the owning Producer. Rust uses the same owner flow through `ProducerRuntime`; tests cover configured/dynamic topic inventory, namespace wrapping, empty-route state, and subsequent route publication. These lifecycle callbacks do not add user Producer settings. |
| Implemented `MQProducerInner` methods: `checkListener`, `getCheckListener`, `checkTransactionState`, `isUnitMode`, plus the route methods above | `Producer::{transaction_check_listener,transaction_listener,is_unit_mode}`; `ProducerRuntime::transaction_processor` and the runtime's owned inbound dispatch; route callbacks above | The pinned interface declares seven methods; all seven are present on the 82-signature implementation inventory. Rust keeps inbound transaction dispatch under runtime ownership, while the two listener getters expose the registered callback handles. Transaction lifecycle, inbound check/no-listener behavior, topic inventory and route publication have Phase 14 / Phase 6 fixture and scripted coverage. This accounts for the Java interface methods without exposing a mutable implementation object. |
| `invokeMessageQueueSelector(Message, MessageQueueSelector, Object, timeout)`, `send(Message, MessageQueue[, timeout])`, `sendOneway(Message, MessageQueue)`, and selector send overloads | `Producer::invoke_message_queue_selector` plus `Producer::{send_to_queue*,send_with_selector*,send_future_with_selector*,send_oneway_with_selector}` | Rust's selector closure captures Java's `arg`. The standalone API passes a Java-equivalent selector message view with its configured namespace removed and returns the selected queue with the producer namespace applied. The invoked `implementationSelectorMessageSnapshot` fixture verifies Java passes a distinct `Message`, shares body/property references, copies topic/flag/properties, and leaves `transactionId` null; the Rust selector test verifies the same readable values and that the input remains unchanged. The Rust callback takes `&Message`, so its deep-owned snapshot has the same observable read behavior without exposing Java-style mutable aliases. The test also exercises an already-namespaced input. The standalone API does not send. Pinned Java `send(Message, MessageQueue, timeout)` validates the message and calls `sendKernelImpl` directly; `invokeMessageQueueSelector` does not require its selected queue to be an element of `TopicPublishInfo.messageQueueList`. Rust resolves the caller-selected Broker through the current route and preserves its queue ID. `invoke_message_queue_selector_returns_the_namespaced_queue_without_sending` covers selector inputs, the returned queue, null selection, timeout, and no send; `explicit_and_selected_queue_ids_outside_the_route_are_sent_to_the_routed_broker` covers explicit and selector sync/future/oneway sends using IDs 7–12 when the route advertises only ID 0; an unknown Broker still fails before a send attempt. Invoked Java `implementationSelectorFailure` exercises selector-only and sync send failures, including the wrapped FAQ message and cause; Rust catches selector panics as `ProducerError::SelectorPanicked` and verifies both fail before Broker I/O. Java's public method signature is pinned in `producerImplementationApi`. |
| `registerCheckForbiddenHook`, `hasCheckForbiddenHook`, `executeCheckForbiddenHook`; `registerSendMessageHook`, `hasSendMessageHook`, `executeSendMessageHookBefore`, `executeSendMessageHookAfter` | `Producer::{register_check_forbidden_hook,has_check_forbidden_hook,execute_check_forbidden_hook,register_send_message_hook,has_send_message_hook,execute_send_message_hook_before,execute_send_message_hook_after}`; send flows share the same snapshot runners | Direct helper behavior now has invoked Java fixture and Rust integration evidence for registration order, check rejection short-circuit, context mutation, and send-hook panic isolation. `send.rs` uses the same snapshot runners; broad overload/input reconciliation remains open. |
| `registerEndTransactionHook`, `hasEndTransactionHook`, `executeEndTransactionHook`, `doExecuteEndTransactionHook` | `Producer` end-transaction hook APIs and public context | Fixture-backed local, Broker-check, direct execution and trace cases are recorded above and in Phase 14. |
| `getTopicPublishInfoTable` | `Producer::get_topic_publish_info_table()` returns the shared `TopicPublishInfoTable`; entries are shared `TopicPublishInfo` values whose live queue-list, route-data, order/router-info flags, and cursor feed later route snapshots and send selection | Invoked Java fixture and Rust differential cover getter/map identity, put/get/replace/remove, retained value/list/route/cursor aliases, direct queue selection, write-queue count, and a scripted default send after queue-list mutation. The table methods now include `getOrDefault`, `putIfAbsent`, `containsValue`, conditional removal/replacement, all compute/merge variants, `forEach`, `replaceAll`, and ordered `putAll`; fixtures cover nullable key/value/callback/result behavior, including Java's null-conditional-remove and values-view-remove false results, partial `putAll`, and null `replaceAll` result. Retained key/value/entry views cover single/bulk mutation, `removeIf`, nullable inputs, and `Map.Entry.setValue`. Filtered queue selection covers null/empty varargs, a null filter (NPE only when a nonempty queue list evaluates it), null/empty queue lists, and non-short-circuit filter order. Current `fixtures/producer/core.json` SHA-256 `bfa2d5f8f411c4648670d024e44b0e637ab793873a9eeb0c7ff75bb2a82651c5`. These close bounded mapping slices only; broad P18-V01 remains open. |
| `getServiceState`, `setServiceState(ServiceState)` | `Producer::{service_state,set_service_state}`; public `ProducerServiceState` | The invoked Java fixture and Rust tests cover initial state, all four Java names/ordinals, null assignment, start failure, duplicate-group reset, normal start and shutdown transitions. |
| `getDefaultMQProducer`, `isUnitMode` | `Producer` itself owns the Java default-producer behavior; `Producer::config` and `Producer::is_unit_mode` expose its configuration view | Java identity/configuration and unit-mode values are captured in `implementationStateAccessors`; Rust verifies the same group and unit-mode value without adding a wrapper object. |
| `getMqFaultStrategy` | Producer fault settings are exposed through the direct live `Producer` getters/setters; `FaultStrategy::select` applies availability, reachability and previous-broker rules inside the owner | Pinned fixture inventories `MQFaultStrategy`'s 2 constructors, 16 declared/17 Java-visible public methods (including inherited `StartAndShutdown.start` and `preShutdown`), and `BrokerFilter`'s constructor and 2 methods. Source search shows `getAvailableFilter`, `getReachableFilter`, `getThreadBrokerFilter`, and `BrokerFilter` are used only by the strategy's own queue-selection method. Rust keeps the raw strategy/filter and Java `ThreadLocal` handles inside the Producer owner. Fault-array aliasing, toggles, selection and null/length failures have fixture/unit evidence; direct Java object access is an internal-entrypoint disposition, not a Rust raw-handle promise. |
| `getMqClientFactory`, deprecated `getmQClientFactory` | `ClientRuntime` owns shared runtime behavior; Producer, Core, consumer and admin APIs call the runtime for lifecycle, route, RPC, heartbeat, registration, rebalance and query work | Pinned fixture inventories the returned `MQClientInstance`'s 2 constructors and 51 Java-visible public methods. Producer call sites map client ID to runtime identity, route lookup/refresh to the shared route cache, heartbeat and send/transaction/recall operations to Core runtime APIs, NameServer validation to the shared client/transport, and callback/RPC-hook access to the owner remoting client. Cross-phase owner mappings are linked above and in Phases 6, 9–11, 13, 15–17. Rust intentionally does not return raw mutable `MQClientInstance`, `MQClientAPIImpl`, route/registry maps, executor, or Netty config handles; those remain owned by `ClientRuntime` and group owners. This closes the returned-surface disposition only; broad P18-V01 input/branch and reverse-export reconciliation remains open. |
| `executeAsyncMessageSend`, `getAsyncSenderExecutor`, `setAsyncSenderExecutor(ExecutorService)`, semaphore helpers | `AsyncSenderExecutor`, Producer runtime backpressure APIs and owned send lifecycle | Source review finds exactly three internal `executeAsyncMessageSend` call sites for the default, explicit-queue and selector async paths; its `BackpressureSendCallBack` parameter type is package-private. Rust keeps the raw `Runnable` helper internal and routes typed async sends through the owner executor/backpressure path. Submission, rejection, backpressure and deadline behavior are fixture/unit covered (`configured_async_sender_executor_runs_future_send_attempts`, `rejected_async_sender_executor_follows_java_backpressure_fallback`, `queued_async_sender_work_obeys_the_send_deadline`). This closes the raw-helper disposition only; broader overload/branch reconciliation remains open. |
| `initTransactionEnv`, `destroyTransactionEnv`, `checkTransactionState`, `endTransaction`, transaction sends | Phase 14 `TransactionProducer` and producer-owned transaction runtime | Transaction worker/lifecycle and request flows map in the Phase 14 matrix; broad overload/input reconciliation remains part of P18-V01. |
| `request*`, `recallMessage`, trace hooks | Phase 15 request/reply, Producer recall, and Phase 16 trace owners | Owner-level behavior is mapped in their phase matrices; inherited/helper overload and reverse-export reconciliation remains open. |

### Public nested `LatencyFaultToleranceImpl.FaultItem` API

The pinned Java class has one public constructor, including the implicit outer
`LatencyFaultToleranceImpl` argument, and 15 public methods including the
`Comparable` bridge overload. Rust exports `FaultItem::new(Option<String>)`,
Java-compatible mutable field methods, `java_compare_to_nullable` and
`java_compare_to_object`, `java_equals`, `java_hash_code`, and
`java_to_string`. Atomic fields preserve Java's volatile reads/writes. The
unobservable outer-instance reference is omitted because no `FaultItem` method
reads it.

`fixtures/producer/core.json` records the exact constructor/method signatures
and direct Java behavior for defaults, signed comparisons, availability
precedence, duration updates and overflow, mutable equality/hash fields,
UTF-16 String hashing, null/wrong-type inputs, and text. Rust verifies that
oracle in `latency_fault_item_public_surface_and_behavior_match_invoked_java`
and exports both `FaultItem` and `FaultItemCompareError` through the facade.
This closes the nested value API row; broader P18-V01 source/API/settings/input/
error/control-flow and reverse-export reconciliation remains open.
Code commit: `62f67cbb`; fixture SHA-256:
`d6c23e4c1d213f02fd257abd0526b525d18f33167f1f26bda8510b75d189f398`. Run
record: `../../rlogs/2026/10/24-phase-18-fault-item-public-api__20261024T153809Z.rlog`.

## Rust publish-info reverse crosswalk

The Java-side fixture guards Java method names against the phase matrices. The
reverse scan now has exact Rust-to-Java rows for all 17 public `TopicPublishInfo`
methods, its nested `QueueFilter.filter` method, all 3 public `ThreadLocalIndex`
methods, and the surfaced Rust adapters. Several Java owners are inherited
`Map`/`Collection` methods on the live table views. The guards
`pinned_java_topic_publish_info_surface_has_phase_crosswalks` and
`pinned_java_topic_publish_info_queue_filter_surface_has_phase_crosswalks`,
`pinned_java_thread_local_index_surface_and_format_match_rust`, and
`rust_publish_info_method_names_have_java_crosswalks` pin these source inventories
and require each listed Rust API and Java counterpart on the same matrix row.
The Java `ThreadLocalIndex` class is signature-pinned; its increment masking,
reset, and `toString()` behavior are compared with `ProducerQueueIndex`.
Rust-only `ptr_eq` methods were removed from the public surfaces of
`ProducerPublishQueueList`, `ProducerQueueIndex`, `TopicPublishInfo`, and
`TopicRouteDataHandle`; crate-internal identity checks remain for Java
map/view semantics. Producer/Core compile-fail doctests and mutation-
based integration assertions guard the external boundary and retained-alias
behavior.

| Rust public API | Java 5.5.1 API | Mapping note |
| --- | --- | --- |
| `TopicPublishInfo::is_order_topic` | `TopicPublishInfo.isOrderTopic()` | JavaBean getter for ordered-topic routing. |
| `TopicPublishInfo::set_order_topic` | `TopicPublishInfo.setOrderTopic(boolean)` | Updates the ordered-topic flag. |
| `TopicPublishInfo::ok` | `TopicPublishInfo.ok()` | True only when the queue list exists and is nonempty. |
| `TopicPublishInfo::message_queue_list` | `TopicPublishInfo.getMessageQueueList()` | Returns the retained live queue-list handle; `None` maps to Java null. |
| `TopicPublishInfo::set_message_queue_list` | `TopicPublishInfo.setMessageQueueList(List<MessageQueue>)` | Installs or clears the retained live queue-list handle. |
| `TopicPublishInfo::send_which_queue` | `TopicPublishInfo.getSendWhichQueue()` | Returns the retained cursor handle. |
| `TopicPublishInfo::set_send_which_queue` | `TopicPublishInfo.setSendWhichQueue(ThreadLocalIndex)` | Replaces the retained cursor handle. |
| `TopicPublishInfo::have_topic_router_info` | `TopicPublishInfo.isHaveTopicRouterInfo()` | JavaBean getter for route-data presence. |
| `TopicPublishInfo::set_have_topic_router_info` | `TopicPublishInfo.setHaveTopicRouterInfo(boolean)` | Updates route-data presence. |
| `TopicPublishInfo::select_one_message_queue` | `TopicPublishInfo.selectOneMessageQueue()` | Increment-first cursor selection; Rust returns typed errors for null/empty lists. |
| `TopicPublishInfo::select_one_message_queue_with_nullable_filters` | `TopicPublishInfo.selectOneMessageQueue(QueueFilter...)` | Preserves nullable varargs and nullable filter elements. |
| `TopicPublishInfo::select_one_message_queue_avoiding` | `TopicPublishInfo.selectOneMessageQueue(String lastBrokerName)` | Avoids the previous Broker when possible. |
| `TopicPublishInfo::reset_index` | `TopicPublishInfo.resetIndex()` | Resets the retained `ThreadLocalIndex`. |
| `TopicPublishInfo::get_write_queue_nums_by_broker` | `TopicPublishInfo.getWriteQueueNumsByBroker(String)` | Reads the Broker write-queue count from retained route data. |
| `Display for TopicPublishInfo` | `TopicPublishInfo.toString()` | Provides the pinned Java diagnostic string through Rust formatting. |
| `TopicPublishInfo::topic_route_data` | `TopicPublishInfo.getTopicRouteData()` | Returns the retained route-data handle; `None` maps to Java null. |
| `TopicPublishInfo::set_topic_route_data` | `TopicPublishInfo.setTopicRouteData(TopicRouteData)` | Installs or clears the retained route-data handle. |
| `PublishInfoQueueFilter<'_>` | `TopicPublishInfo.QueueFilter.filter(MessageQueue)` | Rust callback alias for the Java nested functional interface. |
| `NullablePublishInfoQueueFilters<'_>` | `TopicPublishInfo.selectOneMessageQueue(QueueFilter...)` | Represents nullable Java varargs and null filter elements. |
| `ProducerQueueIndex::increment_and_get` | `ThreadLocalIndex.incrementAndGet()` | Stores the wrapped `i32` value and returns `value & 0x7fffffff`; the fixture pins overflow from `i32::MAX` to `0`, then `1`. |
| `ProducerQueueIndex::reset` | `ThreadLocalIndex.reset()` | Replaces the current thread's cursor with a nonnegative random value. |
| `Display for ProducerQueueIndex` | `ThreadLocalIndex.toString()` | Preserves Java's `null` before the first initialization and formats the raw current `i32`. |
| `TopicPublishInfoTable::replace_all_with_nullable_callback` | `Map.replaceAll(BiFunction)` | Nullable callback adapter on the live `ConcurrentMap` counterpart. |
| `TopicPublishInfoKeySetView::remove_if_with_nullable_callback` | `Map.keySet().removeIf(Predicate)` | Nullable callback adapter on the live key view. |
| `TopicPublishInfoValuesView::remove_if_with_nullable_callback` | `Map.values().removeIf(Predicate)` | Nullable callback adapter on the live values view. |
| `TopicPublishInfoEntrySetView::remove_if_with_nullable_callback` | `Map.entrySet().removeIf(Predicate)` | Nullable callback adapter on the live entry view. |
| `TopicPublishInfoValuesView::remove_all_iter` | `Map.values().removeAll(Collection)` | Iterator input adapter to the inherited collection operation. |

This closes only these surfaced name crosswalks and ThreadLocalIndex overflow
formatting; Rust public API reverse export and Java overload/input/error/flow
reconciliation remain open under P18-V01.

## Pinned producer API and settings inventory

This inventory was checked against the pinned `DefaultMQProducer`,
`DefaultMQProducerImpl`, `Validators`, `MessageBatch`, `TopicPublishInfo`, and
`MQFaultStrategy` source. The high-level facade re-exports the public types
required to construct/configure a producer, including `CompressionType`.

The pinned Java facade inventory records 11 public `DefaultMQProducer`
constructors, 107 declared and 183 callable public methods (including
`ClientConfig`), 34 declared and 42 callable `MQProducer` methods (including
`MQAdmin`), and 8 `MQAdmin` methods. The Rust fixture test pins these counts
and representative inherited signatures. This fixes the reflection denominator;
it does not close the method-by-method P18-V01 mapping. In particular, Java's
public `sendDirect` can bypass auto-batching and `sendByAccumulator` can enqueue
an eligible message through the accumulator regardless of the wrapper's
`autoBatch` branch. Rust exposes direct-send and forced-accumulator owner
methods; the scripted regression below checks their sync/future, default/queue,
and live-setting behavior. Remaining overload/input/branch crosswalks still
belong to P18-V01.

The primary `rocketmq-classic` facade also re-exports the producer's Java-mapped
service state, default group, transaction hooks/context, query result, and the
`TopicPublishInfo` value/table/collection-view types and their Java collection
adapters. `tests/facade_public_types.rs` checks these root import paths and
exercises `Producer::service_state` and
`Producer::get_topic_publish_info_table`; this verifies facade reachability,
while the behavior mappings remain in this matrix and Phases 14 and 17.

The 11 facade constructor signatures are pinned individually by
`pinned_java_default_producer_facade_and_inherited_interfaces_are_recorded`.
`producerConstructorCases` invokes all 11 constructors and records group,
namespace, RPC-hook retention by both facade and implementation, initial topic
values, and trace settings. The Rust differential
`pinned_java_producer_constructor_values_map_to_rust_configuration` compares
those values with `ProducerConfig`/`ClientConfig`; an owner-level regression
also proves the supplied Rust RPC hook is retained. Invoked nullable cases
cover null producer group in constructors and the setter, namespaced null
groups, null namespace, null topic list, and enabled tracing with a null trace
topic. Public send, forbidden-check and end-transaction hook contexts preserve
their Java-null group fields as Rust `Option<String>` values. Rust preserves
nullable construction configuration through `ProducerConfig::new_nullable`,
its `producer_group() -> Option<&str>` getter and
`set_producer_group(Option<String>)`; `new` remains convenient for non-null
construction. The live `Producer` exposes an owned group snapshot and nullable
setter. Java start wraps the group with the
namespace and then rejects null as blank before creating a client factory. Rust
matches the exact error text and `START_FAILED` state for both namespaced and
un-namespaced null groups. Java retains its caller-owned topic `List`; Rust
owns topic values and snapshots the configured list at start. The Producer
getter reflects later setter calls, while the started runtime keeps using its
startup list, matching Java's `initTopicRoute()` read timing. Fixture SHA-256:
`1ed0060666e8ee2a6b5b82b3088b22cb41a9a690bc291a96e56e51d16e7f7d76`.

Their parameter-to-owner mapping is:

| Java `DefaultMQProducer` constructor | Java field effect | Rust construction mapping |
| --- | --- | --- |
| `()`; `(RPCHook)`; `(String producerGroup)`; `(String producerGroup, RPCHook)` | Default group is `DEFAULT_PRODUCER` when omitted; otherwise set the group; hook is null unless supplied | `ProducerConfig::new(DEFAULT_PRODUCER_GROUP)` or `ProducerConfig::new(group)`, then `Producer::new` or `Producer::new_with_rpc_hook` |
| `(String producerGroup, RPCHook, List<String> topics)` | Set group, hook and initial topic list; trace remains disabled | Set `ProducerConfig::producer_group` and `ProducerTopicList`; pass hook to `Producer::new_with_rpc_hook`; cloned list handles preserve caller aliases |
| `(String producerGroup, boolean enableMsgTrace, String customizedTraceTopic)`; `(String producerGroup, RPCHook, boolean enableMsgTrace, String customizedTraceTopic)` | Set group, trace flag and optional trace topic; the second form also sets the hook | Set group, `ClientConfig::set_enable_trace` and `set_trace_topic`; pass the optional hook to `Producer::new_with_rpc_hook` |
| `(String producerGroup, RPCHook, List<String> topics, boolean enableMsgTrace, String customizedTraceTopic)` | Set group, hook, initial topic list and trace settings | Set `ProducerConfig` group/`ProducerTopicList` and `ClientConfig` trace settings; pass hook to `Producer::new_with_rpc_hook` |
| Deprecated `(String namespace, String producerGroup)`; `(String namespace, String producerGroup, RPCHook)` | Set legacy namespace and group; optional hook is retained | Set `ClientConfig::set_namespace` and Producer group; optional hook goes to `Producer::new_with_rpc_hook` |
| Deprecated `(String namespace, String producerGroup, RPCHook, boolean enableMsgTrace, String customizedTraceTopic)` | Set namespace, group, hook and trace settings | Set `ClientConfig` namespace/trace settings and Producer group; pass hook to `Producer::new_with_rpc_hook` |

Rust represents these Java overloads as configuration plus owner construction,
not matching constructor overloads. The table and fixture cover the constructor
field effects and listed null branches; untested malformed inputs and the
broader overload/source branch audit remain P18-V01 work.

| Java 5.5.1 source surface | Rust mapping | Evidence / scope |
| --- | --- | --- |
| `DefaultMQProducer` constructors, `start`, `shutdown`, `fetchPublishMessageQueues` | `ProducerConfig::new`, `Producer::{start,shutdown,fetch_publish_message_queues}` | `invalid_group_fails_before_creating_or_registering_runtime`, `invalid_accumulator_settings_fail_before_runtime_creation`, `repeated_start_is_rejected_and_shutdown_is_idempotent`, `shutdown_before_start_keeps_producer_startable`; scripted route cases below. `ProducerConfig` carries constructor settings; start validates and registers. |
| `send` sync overloads, `sendOneway`; explicit `MessageQueue` and `MessageQueueSelector` overloads | `Producer::{send,send_with_timeout,send_future,send_future_with_timeout,send_oneway,send_oneway_with_timeout,send_to_queue,send_to_queue_with_timeout,send_to_queue_future,send_to_queue_future_with_timeout,send_to_queue_oneway,send_with_selector,send_with_selector_with_timeout,send_future_with_selector,send_future_with_selector_with_timeout,send_oneway_with_selector}` take `&mut Message` to represent Java's caller-visible message preparation | Scripted cases cover retry, one-way, explicit queue, selector, and mode policy; `mode_specific_attempts_match_java_retry_budgets`, `sync_retries_configured_broker_codes_while_future_retries_transport_only`, `per_request_timeout_only_limits_sync_attempts_that_have_a_retry_after_them`, `zero_per_request_timeout_retries_sync_attempts_within_total_budget`, and `default_sync_send_retries_check_forbidden_hook_errors_like_java`. The pinned `perRequestTimeoutRetry` fixture proves a local zero-cap timeout still consumes the configured retry budget while total time remains; `checkForbiddenRetry` proves Java repeats CheckForbidden after a synchronous hook exception without reaching send hooks or RPC. Rust futures are polled on the caller-selected Tokio runtime, replacing callback methods while preserving the mode-specific send/result flow. Direct sends write `UNIQ_KEY` back after route/address resolution and before the first send attempt, so retries reuse it. |
| Public `sendDirect` / `sendByAccumulator` helper paths | `Producer::{send_direct,send_direct_with_timeout,send_direct_future,send_direct_future_with_timeout,send_to_queue_direct,send_to_queue_direct_with_timeout,send_to_queue_direct_future,send_to_queue_direct_future_with_timeout,send_by_accumulator,send_by_accumulator_future,send_to_queue_by_accumulator,send_to_queue_by_accumulator_future}` | Pinned source confirms `sendDirect` always calls the underlying send path, while `sendByAccumulator` validates and writes `UNIQ_KEY` to the caller message before dispatching eligible individual messages. `direct_and_forced_accumulator_helpers_keep_java_path_choices` checks sync/future and default/selected-queue paths against scripted transport, including bypass with auto-batch enabled and forced accumulation with it disabled. `sendInputMutation` records accumulator mutation and direct service-state failure before mutation on an unstarted Java producer; `accumulator_send_mutates_caller_before_runtime_dispatch_like_java` compares both outcomes, valid ID layouts, and caller topic. `autoBatchEligibility` and `auto_batch_eligibility_matches_java_direct_send_fallbacks` cover ineligible direct fallback. |
| `send(Collection<Message>, ...)`, `MessageBatch`, `batch(Collection)` | `Producer::{send_batch,send_batch_with_timeout,send_batch_future,send_batch_future_with_timeout,send_batch_to_queue,send_batch_to_queue_with_timeout,send_batch_to_queue_future,send_batch_to_queue_future_with_timeout}` take mutable message slices so caller-visible ID and namespace changes match Java; `batch::encode_prepared_batch`; public `MessageBatch::{new,generate_from_list,encode}` and `IntoIterator` | `message_batch_public_api_matches_java_551` pins the Java constructor/method signatures; `message_batch_generate_from_list_matches_java_validation_order` checks validation and inherited metadata; `batch_record_bytes_match_pinned_java_message_decoder_fixture` compares `MessageBatch::encode` bytes and iteration count to the pinned fixture; `collection_send_mutates_batch_messages_before_dispatch_like_java` compares Java's batch-preparation wrapper message, structural rejection before mutation, per-element validation's partial mutation boundary, aggregate-size versus lifecycle-check ordering, caller-visible IDs/topics, and the unstarted dispatch failure with the invoked Java fixture. The low-level `encode_prepared_batch` encoder itself leaves borrowed inputs unchanged and defers aggregate-size validation to the send path. This closes the MessageBatch public declaration and send-mutation slices only; broad P18-V01 remains open. |
| `getProducerGroup()`, `setProducerGroup(String)`, `CreateTopicKey`, `Topics`, `DefaultTopicQueueNums`, `SendMsgTimeout`, `SendMsgMaxTimeoutPerRequest`, `CompressType/Level/MsgBodyOverHowmuch`, `RetryTimesWhenSendFailed/AsyncFailed`, `RetryResponseCodes`, `RetryAnotherBrokerWhenNotStoreOK`, `MaxMessageSize`, `SendMessageWithVIPChannel`, `LatencyMax`, `NotAvailableDuration`, `SendLatencyFaultEnable`, `StartDetectorEnable`, `AutoBatch`, batch delay/byte limits, async backpressure enable/limits | `ProducerConfig` exposes construction-time settings. `Producer` adds live group, VIP-send, and topics accessors; other Producer-owned runtime setters are listed below. | `producer_defaults_match_pinned_java_551`, `remaining_producer_settings_preserve_java_values_and_clamps`, `accumulator_setting_validation_matches_java_ranges_and_sentinels`, `compression_system_properties_match_java_defaults_and_parsing`, fixture tests, and retry/fault/backpressure tests. Construction-time backpressure limit setters clamp to Java minima. |
| `getCreateTopicKey`, `setCreateTopicKey`, `getDefaultTopicQueueNums`, `setDefaultTopicQueueNums`, `getSendMsgTimeout`, `setSendMsgTimeout`, `getSendMsgMaxTimeoutPerRequest`, `setSendMsgMaxTimeoutPerRequest`, `getRetryTimesWhenSendFailed`, `setRetryTimesWhenSendFailed`, `getRetryTimesWhenSendAsyncFailed`, `setRetryTimesWhenSendAsyncFailed`, `isRetryAnotherBrokerWhenNotStoreOK`, `setRetryAnotherBrokerWhenNotStoreOK`, `getMaxMessageSize`, `setMaxMessageSize` at runtime | `Producer::{create_topic_key,set_create_topic_key,default_topic_queue_nums,set_default_topic_queue_nums,send_timeout_millis,set_send_timeout_millis,send_msg_max_timeout_per_request,set_send_msg_max_timeout_per_request,retry_times_when_send_failed,set_retry_times_when_send_failed,retry_times_when_send_async_failed,set_retry_times_when_send_async_failed,retry_another_broker_when_not_store_ok,set_retry_another_broker_when_not_store_ok,max_message_size,set_max_message_size}` | The invoked Java `runtimeSettings` fixture calls every setter and getter. Rust tests check detached `ProducerConfig` snapshots, changing default-topic fields in a later send header, send deadline changes, retry attempt count and retry-after-store-status behavior, and live message-size validation. See `runtimeSettings` in `fixtures/producer/core.json` and `runtime_default_topic_settings_reach_later_send_headers`, `runtime_send_timeout_applies_to_later_default_send`, `mode_specific_attempts_match_java_retry_budgets`, `runtime_non_ok_store_retry_switches_brokers_on_later_sends`, and `runtime_message_size_limit_applies_to_later_validation`. |
| `getProducerGroup()`, `setProducerGroup(String)` and subsequent send/hook headers, including null | `Producer::{producer_group,set_producer_group}`; send and end-transaction header group fields are optional | `runtimeSettings` and `runtimeSetterFlows` in `fixtures/producer/core.json`; `producer_group_setter_updates_later_send_headers_including_null`; nullable V1/V2 and end-transaction header tests | UNIT_VERIFIED for live getter/setter behavior and Java's omission of null group fields on the wire; full overload/input audit remains open |
| `setSendMessageWithVIPChannel` and later sends | `Producer::{send_message_with_vip_channel,set_send_message_with_vip_channel}`; send address selection reads the Producer's live flag, while shared-runtime APIs retain their startup `ClientConfig` snapshot | `runtimeSettings` and `runtimeSetterFlows`; `vip_channel_setter_after_start_changes_later_send_addresses`; Maven fixture verification; Java 5.5.1 `DefaultMQProducerImpl.sendKernelImpl` | UNIT_VERIFIED for Java's post-start behavior: changing true to false updates the getter and changes subsequent send broker addresses from VIP port 10909 to normal port 10911. |
| `setTopics` during Producer lifetime | `Producer::{topics,set_topics}` with a startup topic snapshot for runtime registration | `runtimeSettings`; `topic_setter_after_start_changes_getter_but_not_runtime_topics`; Maven fixture verification | UNIT_VERIFIED for startup-list capture and post-start setter visibility without changing the started runtime topic set |
| `setBackPressureForAsyncSendNum/Size` at runtime | `Producer::{backpressure_async_send_num,set_backpressure_async_send_num,backpressure_async_send_size,set_backpressure_async_send_size}` plus construction-time `ProducerConfig` setters | Java adjusts live semaphore permits using the in-flight count. Rust's fair `AdjustableSemaphore` preserves in-flight count across resizing and clamps to Java minima. Invoked Java fixture records 7 held permits becoming 5 available after resizing 1024→12, then 12 after completion; `runtime_resize_preserves_in_flight_count_like_the_java_fixture` compares these outcomes. |
| `is/setEnableBackpressureForAsyncMode` at runtime | `Producer::{enable_backpressure_for_async_mode,set_enable_backpressure_for_async_mode}` plus construction-time config | The Java `runtimeSettings` fixture directly checks the mutable getter/setter. `producer_runtime_backpressure_settings_update_live_limits_and_mode` checks that later sends observe toggles and that already admitted sends retain their permits. |
| `getRetryResponseCodes()` live set / `addRetryResponseCode(int)` | `Producer::retry_response_codes` returns `RetryResponseCodes`, a shared live handle; `Producer::add_retry_response_code`; config getter returns the same handle | Java fixture mutates the returned set, mutates it through `addRetryResponseCode`, then removes through the retained set. Rust `producer_retry_response_codes_are_live_for_sync_retry_decisions` checks those mutations affect subsequent retry decisions; config clones remain independent snapshots. |
| `setBackPressureForAsyncSendNum/SizeInsideAdjust` and count/size read-lock helpers | Producer `*_inside_adjust` setters update the reported values directly; `acquire_backpressure_for_async_send_{num,size}_lock` returns `BackpressureReadGuard`, released by drop | The Rust APIs preserve the Java helper semantics while the fair limiter owns its internal state. `runtime_resize_waits_for_a_public_read_lock_guard` checks setters wait for a held public guard; `runtime_resize_waits_for_a_pending_acquisition_like_java` checks a runtime resize waits for an in-progress permit wait to succeed or time out, following Java's `DefaultMQProducerImpl` read-lock lifetime; `producer_runtime_backpressure_settings_update_live_limits_and_mode` covers both raw adjustment setters. |
| `setCallbackExecutor` | `Producer::set_callback_executor(Option<Arc<dyn CallbackExecutor>>)` forwarded to the started shared `RemotingClient` | The producer fixture confirms Java's pre-start null dereference; Rust reports `ProducerError::NotStarted` before start. A configured executor is retained by the shared client, and `None` restores its default callback executor. The facade re-exports `CallbackExecutor`. |
| `setAsyncSenderExecutor`, `DefaultMQProducerImpl.getAsyncSenderExecutor` / `executeAsyncMessageSend` | `Producer::{set_async_sender_executor,async_sender_executor}` and `AsyncSenderExecutor` / `AsyncSenderTask` | `fixtures/producer/core.json` directly checks Java's stable default, custom executor retention and null reset. Rust future-mode send attempts use the effective executor; rejection returns `AsyncSenderExecutorRejected`, while enabled async backpressure runs a rejected task inline, matching Java. Queued work observes the send deadline and retains its permits until it completes. Once submitted, a send continues if its result future is dropped, matching Java's callback submission lifetime. With no custom executor, Rust dispatches on the caller's Tokio runtime; it does not recreate Java's private fixed-size thread pool. The facade re-exports the executor trait and task alias. |
| `get/setCompressType`, `getCompressor` | `Producer::{compressor,set_compress_type}` returns a `ProducerCompressor` snapshot with `compress`/`decompress`; `ProducerConfig` keeps its construction-time setting | The Java fixture invokes all three compressor types, records their concrete class and round-trip result, and checks that an earlier compressor object keeps its type after `setCompressType`. Rust `producer_compressor_snapshot_and_operations_match_java_fixture` checks all types and runtime replacement. |
| `get/setCompressMsgBodyOverHowmuch`, `get/setCompressLevel` at runtime | `Producer::{compress_threshold,set_compress_threshold,compress_level,set_compress_level}`; config accessors remain available before construction | `runtimeCompressionSettings` directly updates Java, checks the threshold boundary and distinct ZLIB outputs at levels 1/9. Rust `runtime_compression_settings_match_java_and_config_clones_are_snapshots` verifies subsequent compression behavior, getters, and detached config-clone values. Java applies both settings to later sends without range validation. |
| `is/setSendLatencyFaultEnable`, `get/setLatencyMax`, `get/setNotAvailableDuration`, and `is/setStartDetectorEnable` at runtime | `Producer::{send_latency_fault_enable,set_send_latency_fault_enable,latency_max_millis,set_latency_max_millis,not_available_duration_millis,set_not_available_duration_millis,start_detector_enable,set_start_detector_enable}`; public `ProducerLongArray::{is_null,get,set,len,is_empty}` represents a nullable Java `long[]` and preserves shared mutation | `runtimeFaultStrategy` and `runtimeFaultStrategyNullArrays` in `fixtures/producer/core.json` cover input-array/getter aliases, live mutation, replacement, enable toggles, subsequent Broker selection, null setter/getter behavior, deferred null-table NPE, short-duration AIOOBE and ignored extra entries. `runtime_fault_strategy_null_and_mismatched_arrays_match_java` and `runtime_detector_toggle_skips_disabled_due_work_and_joins_on_shutdown` cover the Rust behavior. Maven fixture verification passes; current fixture SHA-256 `2f8c6d40c4669a6a662085e8cddef749338c91fcbb1bf64829ef1f59f77a284e`. |
| `initProduceAccumulator`, `getAutoBatch()`, `setAutoBatch(boolean)` | `Producer::{init_produce_accumulator,auto_batch,set_auto_batch}`; `Producer::start` initializes the accumulator at the Java startup point | The Java lifecycle fixture checks the pre-initialization false getter, initialization, and runtime toggles. Rust `producer_auto_batch_getter_and_setter_follow_accumulator_lifecycle` checks accessor behavior; `auto_batch_flushes_one_message_batch_and_fans_out_results` toggles batching after start and verifies the later direct-send path. |
| `ProduceAccumulator(String)` concrete class surface | `rocketmq_classic_producer::ProduceAccumulator::new(Option<String>)`, re-exported by the facade, maps the Java constructor and nullable String input. Producer batching remains owned by the private Rust accumulator and the Java-mapped `Producer` methods; the public Java helper declares no batching methods. | `produceAccumulatorApi` in `fixtures/producer/core.json` inventories one public constructor and zero declared/public API methods after excluding `Object`; `pinned_java_produce_accumulator_declared_surface_is_constructor_only`, Producer constructor test, and facade public-type test. |
| `MQClientManager.getOrCreateProduceAccumulator(ClientConfig)` | `Producer` initialization uses a manager-scoped accumulator cache keyed by `build_client_id()`. Producers with the same manager and client ID share live limits, pending batches and workers; a pending batch retains its first Producer owner. Shutdown stops workers but retains queued entries, and a later Producer start resumes their deadlines. | `produceAccumulatorClientIdCache` records same-ID identity reuse and different-ID separation; `auto_batch_is_shared_by_client_id_and_keeps_first_producer_owner` checks shared settings, first-owner send routing and restart; `stopped_accumulator_retains_pending_batches_for_restart` checks retained entries/deadlines. |
| `request*`, `sendMessageInTransaction`, admin/query/recall, trace/namespace/security | Later phase APIs | Request/reply Phase 15; transaction Phase 14; admin/query Phase 17; namespace/security/trace Phase 16. These are intentionally not claimed as Phase 7 deliverables. |

The reverse inventory found the facade's compression setting type was not
reachable from `rocketmq-classic`; `CompressionType` is now re-exported by both
producer and facade crates and is used directly by the facade example. The
Rust-only `remove_retry_response_code` operation corresponds to removal from
Java's live `getRetryResponseCodes()` set; public validation functions map the
Java `Validators` entry points. Private scripted transport seams remain
test-only.

The re-exported `CompressionType::{find_by_value,get_compression_flag}` map to
Java `CompressionType.findByValue` and `getCompressionFlag`; the enum's values,
names, ordinals, and formatting are covered by the Phase 6 compression API
entry.

The complete public `ProducerConfig` accessor surface is grouped above by its
Java property, including `set_tls_config`/`tls_config` for the shared TLS
configuration adapter. Its API and fixture values are covered by the named
configuration tests; inherited `ClientConfig` accessors are audited in the
Phase 6 API/config map.

Exact producer configuration methods in that mapping are `new`,
`client_config`/`client_config_mut`, `tls_config`/`set_tls_config`,
`producer_group`/`set_producer_group`, `create_topic_key`/`set_create_topic_key`,
`topics`/`set_topics`,
`default_topic_queue_nums`/`set_default_topic_queue_nums`,
`send_timeout_millis`/`set_send_timeout_millis`,
`send_msg_max_timeout_per_request`/`set_send_msg_max_timeout_per_request`,
`compress_type`/`set_compress_type`, `compress_level`/`set_compress_level`,
`compress_threshold`/`set_compress_threshold`,
`retry_times_when_send_failed`/`set_retry_times_when_send_failed`,
`retry_times_when_send_async_failed`/`set_retry_times_when_send_async_failed`,
`retry_response_codes`/`add_retry_response_code`/`remove_retry_response_code`,
`retry_another_broker_when_not_store_ok`/`set_retry_another_broker_when_not_store_ok`,
`max_message_size`/`set_max_message_size`,
`send_message_with_vip_channel`/`set_send_message_with_vip_channel`,
`latency_max_millis`/`set_latency_max_millis`,
`not_available_duration_millis`/`set_not_available_duration_millis`,
`send_latency_fault_enable`/`set_send_latency_fault_enable`,
`start_detector_enable`/`set_start_detector_enable`,
`detect_timeout_millis`/`set_detect_timeout_millis`,
`detect_interval_millis`/`set_detect_interval_millis`,
`auto_batch`/`set_auto_batch`,
`batch_max_delay_millis`/`set_batch_max_delay_millis`,
`batch_max_bytes`/`set_batch_max_bytes`,
`total_batch_max_bytes`/`set_total_batch_max_bytes`,
`enable_backpressure_for_async_mode`/`set_enable_backpressure_for_async_mode`,
`backpressure_async_send_num`/`set_backpressure_async_send_num`, and
`backpressure_async_send_size`/`set_backpressure_async_send_size`.

`ProducerConfig` is a pre-start builder, so its accumulator accessors expose
the configured values as `Option`s. The pinned Java API has a getter/setter
name mismatch for `totalBatchMaxBytes`: `totalBatchMaxBytes(value)` sets the
accumulator-wide hold cap, but `getTotalBatchMaxBytes()` returns the per-batch
hold size, matching `getBatchMaxBytes()`. The fixture records both that getter
result and the actual aggregate hold cap; Rust's `accumulator_settings()` maps
the setter's effect to the aggregate hold limit. The public Rust
`ProducerConfig::total_batch_max_bytes()` accessor mirrors that getter by
returning the configured per-batch value; `set_total_batch_max_bytes()` remains
the input for the aggregate cap consumed by the private accumulator settings.

## Implemented and locally verified

| Java source and behavior | Rust surface | Evidence | State |
| --- | --- | --- | --- |
| Producer defaults, retry codes, latency tables/detector defaults, compression system properties | `ProducerConfig` | `fixtures/producer/core.json`; producer unit and fixture tests; Maven fixture verification | UNIT_VERIFIED |
| Runtime create-topic key/queue count, send/per-request timeouts, sync/async retry counts, non-OK store retry flag, and message-size limit | `Producer` live accessors and `ProducerConfig` shared setting values | `runtimeSettings` and `perRequestTimeoutRetry` in `fixtures/producer/core.json`; `per_request_timeout_only_limits_sync_attempts_that_have_a_retry_after_them`; `zero_per_request_timeout_retries_sync_attempts_within_total_budget`; Maven fixture verification | UNIT_VERIFIED for setter values `-2/-1/0/125/6000`, sync-only capping before an eligible retry, the final-attempt/future/oneway exclusions, total-deadline clamping, and retrying a local request-scoped timeout while total budget remains; broader P18-V01 source/input reconciliation remains open |
| Live producer group and VIP send-address updates; topics getter versus startup route snapshot | `Producer::{producer_group,set_producer_group,send_message_with_vip_channel,set_send_message_with_vip_channel,topics,set_topics}`; nullable send/end-transaction headers | `runtimeSetterFlows` and `runtimeSettings` in `fixtures/producer/core.json`; live group/VIP/topic regressions; nullable-header tests; Maven fixture verification | UNIT_VERIFIED for live group headers, live VIP address conversion on later sends, null-field wire omission, and startup-only topic-list effects; broad P18-V01 overload/source/input reconciliation remains open |
| `get/setCompressType`, `getCompressor`, `Compressor.compress/decompress` | `Producer::{compressor,set_compress_type}`; public `ProducerCompressor` | `compressorSettings` in `fixtures/producer/core.json`; `producer_compressor_snapshot_and_operations_match_java_fixture`; Maven fixture verification | UNIT_VERIFIED for all three Java compression backends, round trips and snapshot behavior after a runtime type update |
| `get/setCompressMsgBodyOverHowmuch`, `get/setCompressLevel` at runtime | `Producer::{compress_threshold,set_compress_threshold,compress_level,set_compress_level}` | `runtimeCompressionSettings` in `fixtures/producer/core.json`; `runtime_compression_settings_match_java_and_config_clones_are_snapshots`; Maven fixture verification | UNIT_VERIFIED for threshold boundary updates, live level updates and independent `ProducerConfig` clones |
| Runtime latency fault settings and Broker detector toggle | `Producer` live getters/setters; public nullable `ProducerLongArray` shared handle | `runtimeFaultStrategy` and `runtimeFaultStrategyNullArrays` in `fixtures/producer/core.json`; `runtime_fault_strategy_settings_match_java_and_change_later_selection`; `runtime_detector_toggle_skips_disabled_due_work_and_joins_on_shutdown`; `runtime_fault_strategy_null_and_mismatched_arrays_match_java`; Maven fixture verification | UNIT_VERIFIED for alias mutation/replacement, live enable flags, subsequent Broker selection and detector task cancellation; null setter/getter behavior; deferred NPE when null tables are read; AIOOBE only when a matching latency threshold indexes a short duration array; extra duration entries are ignored |
| `initProduceAccumulator`, runtime `getAutoBatch()`, `setAutoBatch(boolean)` | `Producer::{init_produce_accumulator,auto_batch,set_auto_batch}`; `Producer::start` performs the automatic initialization | `autoBatchLifecycle` in `fixtures/producer/core.json`; `producer_auto_batch_getter_and_setter_follow_accumulator_lifecycle`; `auto_batch_flushes_one_message_batch_and_fans_out_results` | UNIT_VERIFIED for pre-init getter, initialization and later runtime batching/direct-send transitions |
| Runtime accumulator delay, per-batch byte threshold and aggregate hold cap | `Producer::{batch_max_delay_millis,set_batch_max_delay_millis,batch_max_bytes,set_batch_max_bytes,total_batch_max_bytes,set_total_batch_max_bytes,init_produce_accumulator}`; the accumulator timer observes live delay/threshold changes and later enqueues observe the live aggregate cap | `runtimeAccumulatorSettings` in `fixtures/producer/core.json` (current file SHA-256 `e285d6e3408c9824feb957fb8cbf16f0209c9b345343a6db8a1ca3a954f40eba`); `producer_runtime_accumulator_settings_match_java_fixture`; `lowering_runtime_batch_byte_limit_flushes_pending_batch`; `runtime_batch_delay_reschedules_and_aggregate_limit_gates_next_enqueue`; Maven fixture verification | UNIT_VERIFIED for zero getters before initialization, Java defaults, pre-init setters, post-init validation and effective-value retention, repeated initialization after an invalid setting, pending-batch flush/rescheduling, and later enqueue admission after aggregate-cap changes |
| Producer runtime async backpressure limits, enable toggle, live retry-code set and public adjustment/lock helpers | `Producer` runtime accessors; `RetryResponseCodes`; `BackpressureReadGuard` | `runtimeSettings` in `fixtures/producer/core.json`; `producer_runtime_backpressure_settings_update_live_limits_and_mode`; `runtime_resize_preserves_in_flight_count_like_the_java_fixture`; `runtime_resize_waits_for_a_public_read_lock_guard`; `runtime_resize_waits_for_a_pending_acquisition_like_java`; `producer_retry_response_codes_are_live_for_sync_retry_decisions`; Maven fixture verification | UNIT_VERIFIED for the recorded runtime toggles, clamping, in-flight permit adjustment, setters blocked by a public guard or pending count/byte acquisition, and live-set retry behavior |
| `setSemaphoreAsyncSendNum(int)`, `setSemaphoreAsyncSendSize(int)`, `getSemaphoreAsyncSendNumAvailablePermits()`, `getSemaphoreAsyncSendSizeAvailablePermits()` | `Producer::{set_semaphore_async_send_num,set_semaphore_async_send_size,semaphore_async_send_num_available_permits,semaphore_async_send_size_available_permits}` | `implementationSemaphoreSettings` in `fixtures/producer/core.json`; `implementation_raw_semaphore_setters_match_pinned_java_fixture`; `raw_semaphore_replacement_preserves_java_in_flight_accounting`; Maven fixture verification | UNIT_VERIFIED for direct permit replacement (including negative counts), unchanged facade limit, releases into the replacement semaphore, and a later facade resize using Java's `configuredLimit - availablePermits` accounting |
| Producer callback/sender executor API and submission outcomes | `Producer::{set_callback_executor,set_async_sender_executor,async_sender_executor}`; `AsyncSenderExecutor` | Java `executorSettings` fixture; `pinned_java_producer_core_fixture_matches_defaults_and_validation`; `producer_callback_executor_requires_an_initialized_client`; `producer_callback_executor_forwards_to_shared_remoting_client`; `configured_async_sender_executor_runs_future_send_attempts`; `dropping_future_after_async_submission_does_not_cancel_the_send`; `rejected_async_sender_executor_follows_java_backpressure_fallback`; `queued_async_sender_work_obeys_the_send_deadline`; Maven fixture verification | UNIT_VERIFIED for the recorded setter, forwarding, submission lifetime, rejection, backpressure fallback, permit retention and deadline cases; Tokio task placement is the Rust default |
| Producer implementation CheckForbidden and SendMessage hook helpers | `Producer::{has_check_forbidden_hook,execute_check_forbidden_hook,has_send_message_hook,execute_send_message_hook_before,execute_send_message_hook_after}` | `implementationHookExecution` in `fixtures/producer/core.json` (SHA-256 `ca42f599afeb81e9a977c7ae88547fe7ce2b3b7fa9dec3940055962219a9cb9e`); `implementation_hook_helpers_match_pinned_java_fixture`; existing ordered send-path, rejection and panic-isolation tests; Maven fixture verification | UNIT_VERIFIED for direct hook presence, registration order, mutable context, check-error short-circuit and per-hook send panic isolation; Java implementation method inventory remains 82 methods |
| Producer implementation lifecycle state and unit-mode accessors | `Producer::{service_state,set_service_state,is_unit_mode}`; public `ProducerServiceState` | `implementationStateAccessors` in `fixtures/producer/core.json` (current fixture SHA-256 `079e9fcac47f97f4921a823c10b02a3b0ec882c42b85c85695663530047cd23d`); `implementation_state_accessors_match_pinned_java_fixture`; Producer lifecycle tests | UNIT_VERIFIED for the Java enum values/order, nullable setter, unit-mode/config values, failed-start guard, duplicate-group reset, and normal lifecycle transitions |
| `DefaultMQProducer.getDefaultMQProducerImpl()` / `DefaultMQProducerImpl.getDefaultMQProducer()` | `Producer::{default_mq_producer_impl,default_mq_producer}`; both return borrowed references to the same `Producer` owner | `defaultProducerImplIdentity` in `fixtures/producer/core.json`; `implementation_state_accessors_match_pinned_java_fixture` checks both Java identity outcomes and Rust pointer identity | UNIT_VERIFIED; Java wrapper behavior is represented by the single Rust owner |
| Transaction completion hook API on the Producer | `Producer::{register_end_transaction_hook,has_end_transaction_hook,execute_end_transaction_hook,do_execute_end_transaction_hook}`; public `EndTransactionHook` and `EndTransactionContext` | `endTransactionHooks` in `fixtures/producer/transaction.json`; `p18_end_transaction_hooks_match_java_context_order_and_exception_isolation`; `end_transaction_hook_context_and_methods_are_available_from_the_public_crate_api`; `end_transaction_trace_client_host_matches_java_host_producer_client_id`; Maven fixture verification | UNIT_VERIFIED for registration order, local/Broker-check context, transaction ID from the message, mutable message visibility, per-hook panic isolation, direct execution and trace-hook integration; transaction request details are in Phase 14 |
| Group/topic/message validation and reserved/system topics | `validation` | Pinned Java fixture and Rust differential tests, including UTF-16 length and size boundaries | UNIT_VERIFIED for representable Rust inputs; a null Java body maps to empty-body rejection because Rust `Message` owns a `Vec<u8>` |
| Unique-ID layout, caller mutation timing, and supplied-ID preservation | `UniqueIdGenerator` and Producer send preparation | Java `uniqueId` and `sendInputMutation` fixture sections; `accumulator_send_mutates_caller_before_runtime_dispatch_like_java`; `sync_send_retries_across_two_brokers_and_preserves_one_prepared_message`; `auto_batch_flushes_one_message_batch_and_fans_out_results`; `direct_send_preserves_existing_unique_id_on_caller_and_wire` | UNIT_VERIFIED for supported 32/56-character layouts, accumulator mutation before dispatch, direct-send state/address validation before mutation, one ID across retries, per-input auto-batch IDs, and preservation of caller-supplied IDs. Java and Rust can select different local IP families, so host-dependent ID length is checked against the valid layouts rather than forced to equal the fixture host's length. |
| Compression threshold and selected compression backend | send preparation and common compression module | Java threshold−1/threshold/threshold+1 fixture; config parsing fixture; Rust unit tests | UNIT_VERIFIED for covered cases |
| Batch validation and Java record encoding | `MessageBatch::generate_from_list`, `batch::encode_prepared_batch`, and downstream `validation::validate_message` | Pinned invalid/valid generation cases, Java `MessageBatch.encode` golden bytes, and a scripted send proving aggregate-size rejection after Producer start and before Broker I/O | UNIT_VERIFIED for covered cases |
| Retry attempt counts, retryable status codes, request deadlines, broker avoidance and fault classification | `send` and `FaultStrategy` | Private scripted transport test exercises two-broker retry with identical body/ID; deterministic tests cover status decisions, deadlines and fault tables | UNIT_VERIFIED for covered flows and decisions |
| Auto-batch defaults, delay/byte limits, eligible-message fallbacks, grouping, result fan-out and shutdown | `ProducerConfig` and manager-scoped `AutoBatchAccumulator` | Java fixture covers initialized defaults, eligibility and client-ID cache identity; Rust unit tests cover limits, grouping, timers, result fan-out, shared first-owner routing and stop-retain-restart behavior; private scripted transport covers flush/send behavior | UNIT_VERIFIED for local accumulation and scripted send decisions; Docker Broker validation remains in Phase 19 |
| Async-mode backpressure defaults, clamps, permit acquisition and release | `ProducerConfig` and `Producer` | Java defaults/clamp fixture and Rust semaphore unit test | UNIT_VERIFIED for the static settings and permit lifecycle |
| Producer registration cleanup and optional detector task cancellation | `Producer::start` / `shutdown` | `runtime_start_failure_unregisters_the_producer_lease`, `repeated_start_is_rejected_and_shutdown_is_idempotent`, `shutdown_before_start_keeps_producer_startable`, scripted loopback send cases, and live `P7-V04` | LIVE_VERIFIED — clean-source run `20261026T005812Z` sent 8,192 fixed-seed variable-size messages per language with at most 32 concurrent sends, matching payload byte totals/checksums and contiguous Broker offsets; three additional 20-send Java/Rust producer cycles returned pending requests, connections, and owned task groups to zero after every shutdown; evidence (raw output not retained) |
| Producer shutdown after a live group change; request/reply pending state | `Producer::shutdown` looks up the current nullable group and removes that registry key; by-group removal leaves pending replies intact | Pinned Java `ProducerLifecycleFixtures` plus `producer_shutdown_matches_java_pending_request_and_timeout_scanner_state` cover unchanged, missing, another Producer's, and null groups, service state, pending-entry retention, callback count, and scanner stop. Java sets `SHUTDOWN_ALREADY` only after successful unregister; a null group throws first and leaves it RUNNING. `RequestFutureHolder.shutdown` leaves pending entries and callbacks alone. Fixture `fixtures/producer/lifecycle.json` SHA-256 `9d5063e8ae538f7da3f957c874dfb95dd81a6a85bf113a947a796de1815001d8`. This describes the public Producer path; low-level identity `GroupLease` cleanup remains a Rust lifecycle helper. |
| Default-route selection, sync/future/oneway, explicit queue and selector send APIs | `Producer` send methods | Implemented; five scripted TCP loopback tests cover route fallback, sync/future/oneway, queue/selector/batch sends, retries, auto-batch unit behavior and lifecycle | UNIT_VERIFIED for control flow and scripted transport |
| Missing Broker publish address after a writable queue is cached | Default send retries and explicit queue/selector send paths | Pinned Java `missingBrokerAddressRetry` clears the Broker address table after installing a valid publish route; `missingBrokerAddressExplicit` verifies the direct kernel error. Rust `default_send_missing_broker_addresses_matches_java_retry_error`, `explicit_queue_missing_broker_address_matches_java_error`, `selector_send_missing_broker_address_matches_java_error`, and `oneway_sends_once_and_unknown_explicit_broker_fails_before_io` preserve the writable queue when master ID `0` has a null address. Default exhaustion returns the Java client-error wrapper with response code `10003`, attempts and Broker list; direct queue/selector errors retain `The broker[broker] not exist`. Route request counts confirm the existing cached publish information is rechecked without another NameServer fetch. Fixture `fixtures/producer/core.json` SHA-256: `725af3b3887f7a4ec6f6d72a23188e4fb80206e50c98b113b015d76917fedfd8`. | UNIT_VERIFIED; implementation `e48fb323`, selector regression `28d28377`; Java source path is `DefaultMQProducerImpl.tryToFindTopicPublishInfo` / `sendKernelImpl` |
| Default-route CheckForbidden retry exhaustion and mode-specific errors | `Producer::send_default_until`, `send_future_attempts_inline`, `send_one_attempt_with_hooks`, and `ProducerError::SendRetriesExhausted` | Pinned Java `checkForbiddenRetry` records sync retry count and final `MQClientException`; `checkForbiddenDefaultAsync` records one attempt, response code 10003 and the wrapper cause despite three configured async retries; `checkForbiddenExplicitAsync` records the unwrapped kernel error; `checkForbiddenDeadline` records overall-deadline precedence. Rust regressions compare attempt count, broker list, cause, response code and no-RPC/no-send-hook behavior. Default future errors also update the reachable-client fault entry when latency-fault mode is enabled. | UNIT_VERIFIED for these sync/default-future/explicit-future rejection branches; broad P18-V01 input/branch reconciliation remains open |
| Timeout after a Broker stores a message but before its acknowledgement reaches the client | `send` retry loop | Scripted two-Broker regression records the received request and drops its first acknowledgement; compatibility consequence documented below | UNIT_VERIFIED; both Broker peers received the unchanged body and unique ID |

## Compatibility consequence

A timeout does not establish whether the Broker stored the message. The sync
retry loop can submit the same prepared body and unique ID to another Broker;
applications can therefore observe duplicate delivery. The Producer does not
provide exactly-once delivery.

## Remaining inventory and case ownership

The Phase 7 send-settings inventory is split between `ProducerConfig`
construction-time values and `Producer` runtime setters, with the Java callback paths represented by Rust futures on the caller's Tokio
runtime. Java's configurable callback executors have no per-producer Rust
setting; scheduling follows the caller's runtime. Transaction and request/reply
operations belong to later phases. Namespace, security and tracing integration
remain assigned to Phase 16.

- **P7-V01:** UNIT_VERIFIED for captured producer and initialized accumulator
  defaults, validation boundaries, compression thresholds, message size and
  batch size cases.
- **P7-U01:** UNIT_VERIFIED using both private scripted transport and the five
  TCP loopback cases in `scripts/phase7-producer-scripted-tests.sh`: two-broker
  retry, response policy, oneway, explicit queue/selector/batch modes, body/ID
  preservation, and lost-ack duplicate delivery. All five loopback cases passed.
- **P7-V02:** LIVE_VERIFIED — clean-source high-level send modes passed against
  Broker 5.3.4; see run evidence (raw output not retained).
- **P7-V03:** LIVE_VERIFIED — clean-source two-Broker retry and send-deadline
  checks passed; see run evidence (raw output not retained).
- **P7-V04:** LIVE_VERIFIED — clean-source bounded server-backed stress and
  producer lifecycle checks passed; see
  run evidence (raw output not retained).

The producer detector's local probe adapter has direct scripted RPC coverage
for the queue-0 request fields, Broker error response, and missing/invalid
`offset` response branches. This is covered by
`producer_broker_probe_matches_get_max_offset_request_and_response_validation`
in `crates/rocketmq-classic-core/tests/rpc.rs`: the peer verifies GET_MAX_OFFSET,
topic, queue ID 0, and `committed=true`, and the test checks valid, Broker-error,
missing-offset, and invalid-offset outcomes. The focused test passes. This local
check does not change the Phase 19 live Broker evidence recorded above.

The dedicated offline Java producer fixture generator and its drift verification
pass. `mvn -f compat/java-reference/pom.xml verify` also passed, including the
runtime-config loopback fixture and Java decoding of Rust-generated fixtures.

## P18-V01 producer facade reverse-scan follow-up

The pinned `ProducerCoreFixtures.producerFacadeApi()` reflection inventory
records `DefaultMQProducer` (107 declared / 183 callable methods), `MQProducer`
(34 declared / 42 callable methods), and `MQAdmin` (8 methods). The Rust test
`pinned_java_default_producer_facade_and_inherited_interfaces_are_recorded`
pins the declaration and callable counts. The regression
`pinned_java_producer_callable_method_names_have_phase_crosswalks` checks every
callable method name across the 183 `DefaultMQProducer`, 42 `MQProducer`, and 8
`MQAdmin` signatures against the owning Phase 6/7/14–17 matrices, including
inherited `ClientConfig` and `MQAdmin` methods. Re-export-aware Rustdoc scanning
found no unmatched inherent Producer method names. The exact Java-token scan
produced the following naming candidates; each was checked against its owner
API and the evidence rows above. These are JavaBean/fluent names whose Rust
equivalents use property-oriented accessors, units, futures, or ownership
guards:

| Java 5.5.1 names | Rust owner mapping | Evidence |
| --- | --- | --- |
| `getBackPressureForAsyncSendNum`, `getBackPressureForAsyncSendSize`, `setBackPressureForAsyncSendNum`, `setBackPressureForAsyncSendSize`, `setBackPressureForAsyncSendNumInsideAdjust`, `setBackPressureForAsyncSendSizeInsideAdjust`, `acquireBackPressureForAsyncSendNumLock`, `acquireBackPressureForAsyncSendSizeLock`, `releaseBackPressureForAsyncSendNumLock`, `releaseBackPressureForAsyncSendSizeLock` | `Producer::{backpressure_async_send_num,backpressure_async_send_size,set_backpressure_async_send_num,set_backpressure_async_send_size,set_backpressure_async_send_num_inside_adjust,set_backpressure_async_send_size_inside_adjust,acquire_backpressure_for_async_send_num_lock,acquire_backpressure_for_async_send_size_lock}`; dropping `BackpressureReadGuard` releases the corresponding Java read lock | `runtimeSettings`; `producer_runtime_backpressure_settings_update_live_limits_and_mode`; `runtime_resize_preserves_in_flight_count_like_the_java_fixture`; `runtime_resize_waits_for_a_public_read_lock_guard` |
| `batchMaxDelayMs(int)`, `getBatchMaxDelayMs()`, `batchMaxBytes(long)`, `getBatchMaxBytes()`, `getTotalBatchMaxBytes()`, `totalBatchMaxBytes(long)` | `Producer::{batch_max_delay_millis,set_batch_max_delay_millis,batch_max_bytes,set_batch_max_bytes,total_batch_max_bytes,set_total_batch_max_bytes}`; the Java getter/setter naming mismatch for the aggregate hold cap is documented above | `runtimeAccumulatorSettings`; `producer_runtime_accumulator_settings_match_java_fixture`; `lowering_runtime_batch_byte_limit_flushes_pending_batch`; `runtime_batch_delay_reschedules_and_aggregate_limit_gates_next_enqueue` |
| `getCompressType`, `setCompressType`, `getCompressor`, `getCompressLevel`, `getCompressMsgBodyOverHowmuch` | `Producer::{compressor,set_compress_type,compress_level,set_compress_level,compress_threshold,set_compress_threshold}` and `ProducerCompressor::compression_type`; `ProducerConfig` supplies construction-time getters | `compressorSettings`; `runtimeCompressionSettings`; `producer_compressor_snapshot_and_operations_match_java_fixture`; `runtime_compression_settings_match_java_and_config_clones_are_snapshots` |
| `getCreateTopicKey`, `setCreateTopicKey`, `getDefaultTopicQueueNums`, `setDefaultTopicQueueNums`, `getMaxMessageSize`, `setMaxMessageSize`, `getRetryTimesWhenSendFailed`, `setRetryTimesWhenSendFailed`, `getRetryTimesWhenSendAsyncFailed`, `setRetryTimesWhenSendAsyncFailed`, `getSendMsgTimeout`, `setSendMsgTimeout`, `getSendMsgMaxTimeoutPerRequest`, `setSendMsgMaxTimeoutPerRequest`, `isRetryAnotherBrokerWhenNotStoreOK`, `setRetryAnotherBrokerWhenNotStoreOK`, `isSendMessageWithVIPChannel` | Matching `Producer` live getters/setters and `ProducerConfig` construction-time accessors; `DefaultMQProducerImpl.sendKernelImpl` reads the current VIP flag while preparing each send address; time values are explicitly named in milliseconds | `runtimeSettings` and `runtimeSetterFlows`; `runtime_default_topic_settings_reach_later_send_headers`; `runtime_send_timeout_applies_to_later_default_send`; `mode_specific_attempts_match_java_retry_budgets`; `runtime_non_ok_store_retry_switches_brokers_on_later_sends`; `runtime_message_size_limit_applies_to_later_validation`; `vip_channel_setter_after_start_changes_later_send_addresses` |
| `getLatencyMax`, `setLatencyMax`, `getNotAvailableDuration`, `setNotAvailableDuration`, `isSendLatencyFaultEnable`, `setSendLatencyFaultEnable` | `Producer::{latency_max_millis,not_available_duration_millis,send_latency_fault_enable,set_latency_max_millis,set_not_available_duration_millis,set_send_latency_fault_enable}`; nullable mutable arrays use `ProducerLongArray` | `runtimeFaultStrategy` and `runtimeFaultStrategyNullArrays`; `runtime_fault_strategy_settings_match_java_and_change_later_selection`; `runtime_fault_strategy_null_and_mismatched_arrays_match_java` |
| `getTopics`, `setTopics`, `getTraceDispatcher` | `Producer::{topics,set_topics,trace_dispatcher}` with shared mutable `ProducerTopicList` handles; topic updates after start affect the live getter list while runtime route registration retains its startup snapshot | `runtimeSettings` and `runtimeSetterFlows`; `topic_setter_after_start_changes_getter_but_not_runtime_topics`; `ProducerTopicList` alias/mutator tests; `enabled_trace_starts_and_stops_its_owned_dispatcher` |
| `isEnableBackpressureForAsyncMode`, `setEnableBackpressureForAsyncMode` | `Producer::{enable_backpressure_for_async_mode,set_enable_backpressure_for_async_mode}` | `runtimeSettings`; `producer_runtime_backpressure_settings_update_live_limits_and_mode` |

The pinned facade inventory contains 20 `send` overloads and 3 `sendOneway`
overloads on both `DefaultMQProducer` and `MQProducer`; the six `request`
overloads are tracked separately in the Phase 15 matrix. Their behavior groups
map as follows:

| Java overload family | Rust owner methods | Local evidence |
| --- | --- | --- |
| Default-route `send(Message[, timeout])`, sync and callback forms | `send`, `send_with_timeout`, `send_future`, `send_future_with_timeout`; Java callbacks are represented by future-mode sends | `default_topic_route_fallback_and_async_mode_response_policy_match_java`; `sync_send_retries_across_two_brokers_and_preserves_one_prepared_message`; callback executor submission/lifetime/rejection/deadline tests |
| Explicit-queue `send(Message, MessageQueue[, timeout])`, sync and callback forms | `send_to_queue*`, `send_to_queue_future*` | `public_queue_selector_and_batch_modes_use_the_scripted_route`; `explicit_and_selected_queue_ids_outside_the_route_are_sent_to_the_routed_broker` |
| Selector `send(Message, MessageQueueSelector, arg[, timeout])`, sync and callback forms | `send_with_selector*`, `send_future_with_selector*` | `public_queue_selector_and_batch_modes_use_the_scripted_route`; `invoke_message_queue_selector_returns_the_namespaced_queue_without_sending`; `explicit_and_selected_queue_ids_outside_the_route_are_sent_to_the_routed_broker` |
| `sendOneway(Message)`, explicit-queue and selector overloads; Rust also provides timeout-taking variants | `send_oneway*`, `send_to_queue_oneway`, `send_oneway_with_selector` | `public_queue_selector_and_batch_modes_use_the_scripted_route`; `oneway_sends_once_and_unknown_explicit_broker_fails_before_io` |
| `send(Collection<Message>[, timeout])` and explicit-queue collection overloads, sync and callback forms | `send_batch*`, `send_batch_future*`, `send_batch_to_queue*`, `send_batch_to_queue_future*`; callback forms use future mode. Collection-send APIs take `&mut [Message]` to preserve Java's per-element unique-ID and namespace mutation before dispatch; batch-preparation failures map to `BatchInitiationFailed` with Java's wrapper message, while aggregate-size validation follows producer-state validation. Explicit and auto-batch paths share `encode_prepared_batch`. | `public_queue_selector_and_batch_modes_use_the_scripted_route` exercises default/queue and sync/future with and without an explicit timeout; `collection_send_mutates_batch_messages_before_dispatch_like_java`; `batch_body_matches_message_decoder_record_layout_and_does_not_mutate_inputs` covers the low-level encoder; `encoded_batch_size_is_checked_by_send_validation` |
| Implementation `sendDirect` and `sendByAccumulator` paths | `send_direct*`, `send_to_queue_direct*`, `send_by_accumulator*`, `send_to_queue_by_accumulator*` | `direct_and_forced_accumulator_helpers_keep_java_path_choices` directly calls all 12 public helpers, checks sync/future mode, default/selected queue and bypass/forced-batch decisions under both `autoBatch` settings |

The Rust source-level reverse check found all 37 public send methods directly
referenced by Producer crate test targets. The updated scripted tests assert
all 16 generated route requests, including seven batch sends and three oneway
sends; the forced/direct helper test asserts twelve attempts. Java callback
overloads map to Rust futures, with executor submission, cancellation,
rejection, and deadline behavior covered by the named executor tests. The
inherited `MQAdmin` methods map to all three client facades in the Phase 17
matrix; transaction and namespace/security/trace owners are mapped in Phases
14 and 16.

This closes the reviewed Producer facade token-scan candidates and the bounded
callable method-name reverse scan only. It does not close per-overload
input/error/control-flow crosswalks, the full Rust public API reverse export,
or whole-client source/input reconciliation required by P18-V01.

## P18-V01 `Validators` permission methods and broker config RPC — 2026-10-21

Pinned Java `Validators` declares seven public methods, including
`checkTopicConfig(TopicConfig)` and `checkBrokerConfig(Properties)`. The former
accepts permissions 0 through 7 and throws `MQClientException` code 16 for
other values. The latter is a no-op when `brokerPermission` is absent, parses
the present value with `Integer.parseInt`, and then applies the same 0 through
7 range check. Rust exposes these checks as `validation::check_topic_config`
and `validation::check_broker_config`; `PermName` maps the Java masks, tests,
string overload and public constants.

`MQClientApi::create_topic` validates an explicitly supplied `perm` before
network I/O. `MQClientApi::update_broker_config` validates before encoding,
returns without RPC for an empty map, sends non-empty values as UTF-8
`key=value\n` lines with request code 25 and no header fields, applies the
configured Java VIP port offset, and preserves Broker response errors. Invalid
permissions format the same code-16 `MQClientException` message, while
malformed strings retain Java's `NumberFormatException` input text. The pinned
Java fixture records all seven `Validators` signatures, all eight `PermName`
signatures and public fields, numeric/string permission inputs, and the
validation outcomes. Fixture SHA-256:
`8216bf9c1a66b57a61e88af448f61ab38dc84015ffd872ee340d68896d3ec9b5`.
Common permission differentials, Producer fixture comparisons and scripted
Core admin RPC tests cover the values and branches listed above. This closes
these bounded APIs and operation paths only; broad P18-V01 and the final
current-revision P18-V02 refresh remain open.

## P18-V01 configured-topic startup transport failure — 2026-10-22

Pinned `DefaultMQProducerImpl.initTopicRoute()` calls
`tryToFindTopicPublishInfo()` for each namespace-wrapped configured topic.
`MQClientInstance.updateTopicRouteInfoFromNameServer()` suppresses
`MQClientException` route/client failures and returns false, allowing the
default-topic fallback and warning path to continue. It converts a
`RemotingException` to an uncaught `IllegalStateException`, so startup fails
on a route transport failure. The scripted Rust regression
`producer_start_preserves_java_registration_after_route_transport_failure`
receives the route request, closes the NameServer connection without a
response, and compares the failed service state and retained registration to
`startupRouteTransportFailure` in `fixtures/producer/lifecycle.json`. The
invoked Java probe observes `IllegalStateException` caused by
`RemotingSendRequestException`, one route request, `START_FAILED`, and the
Producer group still present in the factory table. Rust now retains that
registration after the same post-start route failure. The earlier
`561bae6d` regression incorrectly expected lease rollback; this correction is
at `4cabbffb`. Producer package tests, strict Producer Clippy, and Java fixture
verification pass. This closes the named transport error/registration branch
only; broad P18-V01 source/API/settings/input/error/control-flow and reverse-
export reconciliation remains open.

## P18-V01 Producer factory-start failure — 2026-10-22 (`cd212ffb`)

Pinned Java 5.5.1 `DefaultMQProducerImpl.start(boolean)` registers the
Producer with `MQClientInstance` before calling `MQClientInstance.start()`.
When an invalid periodic interval makes the client factory start throw
`IllegalArgumentException`, Java leaves both Producer and factory in
`START_FAILED` and retains the Producer group in the factory table. The
invoked result is `startupFactoryStartFailure` in
`fixtures/producer/lifecycle.json` (SHA-256
`6f1cdb10b68a13e58fd7ca981155006f9e2061ace654bf13f1c09573d9e448ad`).

Rust now keeps external Producer/Consumer registrations while it tears down
failed runtime-owned transport and worker state, and keeps the failed runtime
published in its client factory table. A later unregister can remove a retained
group. `runtime_start_failure_preserves_java_factory_registration` compares
the Producer state and table with the Java fixture; core startup-failure tests
cover retained groups, failed factory identity, and resource cleanup. This
closes this factory-start failure branch only; broad P18-V01 input/error and
reverse-export reconciliation remains open.
