# Phase 17 — Java admin API coverage audit

This audit is against Java client source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`. It separates facade coverage,
single-Broker RPCs, and local verification from Phase 19 Broker evidence.

## Public MQAdmin methods

The Java producer, push consumer, and pull consumer expose the shared
`MQAdmin` methods. A method on one Rust facade does not complete the other two.

| Java capability | Rust implementation | Status and remaining work |
| --- | --- | --- |
| `createTopic(key, topic, queues, attributes)` and `createTopic(..., topicSysFlag, attributes)` | `Producer::{create_topic,create_topic_with_sys_flag}`, `PushConsumer::{create_topic,create_topic_with_sys_flag}`, `PullConsumer::{create_topic,create_topic_with_sys_flag}`; shared helper in consumer crate | Implemented on all three facades. Java wrappers apply the namespace to the new topic and discard the attributes argument. Tests cover topic headers, system flag, successful routes and Broker permission errors on all three facades. |
| `searchOffset`, `maxOffset`, `minOffset`, `earliestMsgStoreTime` | `search_offset`, `get_max_offset`, `get_min_offset`, `get_earliest_msg_store_time` on Producer, Push, and Pull; Core single-Broker RPCs | Implemented on all three facades. Core tests cover successful values, Broker errors, and absent response fields. Public facade success and permission-error forwarding are covered on all three. |
| `queryMessage(topic, key, max, begin, end)` | `query_message` on Producer, Push, and Pull; `QueryResult::new`, `QueryResult::{get_index_last_update_timestamp,get_message_list}` and `Display` for Java `toString`; Core single-Broker RPC and bounded fan-out | Implemented. `QueryResult` keeps its result fields private and preserves Java's nullable list/elements and mutable list alias through `QueryMessageList`. It selects Brokers by Java cluster rules, passes `lastKey`, filters returned keys/topics, merges messages and the maximum index watermark, tolerates per-Broker failures, and returns Java's `NO_MESSAGE` result when no messages survive. P17-U01's scripted tests cover ordering, time bounds, partial and terminal failure against pinned Java callback aggregation; all three facades test terminal `NO_MESSAGE` forwarding. The constructor/getter/`toString` surface and alias behavior are checked against the invoked Java fixture. |
| `MQAdminImpl.queryMessage(clusterName, topic, key, max, begin, end, isUniqKey, indexType, lastKey)` | Core `AdminQueryRequest::{by_key,by_unique_key,in_cluster,with_cluster_name,with_index_type,with_last_key}` | The request keeps `route_topic` and `logical_message_queue` internal; builders preserve nullable Java `clusterName`, `indexType`, and `lastKey` inputs. A null or empty `indexType` yields no key matches, matching the pinned Java result filter. Unit and scripted RPC regressions cover ordinary, logical-topic, and wheel-timer route selection, LMQ cluster filtering/request topic, nullable index/pagination inputs, and cluster `lastKey` forwarding. This closes the request-builder surface only; broad P18-V01 reconciliation remains open. |
| `viewMessage(topic, messageId)` | `view_message` on Producer, Push, and Pull; Core physical-offset RPC and the internal `MQAdminImpl.queryMessageByUniqKey` fallback | Java first attempts physical lookup and falls back to a unique-key query if message-ID parsing or the physical RPC throws. The fallback uses max 32, the last three days through `Long.MAX_VALUE`, and returns the first result. Producer and Pull pass the raw topic to physical lookup; Push applies its namespace. All three namespace the fallback query. Rust follows the same branches and returns `Option<MessageExt>` for Java's nullable first result. Scripted tests cover malformed IDs, physical lookup failure, successful direct lookup, query fields/default window, terminal `NO_MESSAGE`, and each facade's topic handling. |
| Public query facade surface | `query_message` on Producer, Push, and Pull; `query_message_by_unique_key` is not exposed | Pinned Java reflection inventories the complete declared and inherited Push/Pull facade methods and confirms there is no public `queryMessageByUniqKey` method. Unique-key lookup remains the behavior of `viewMessage` and the internal implementation method. Rust compile-fail doctests guard against re-exposing a separate facade method. |

Query header decode follow-up (2026-10-18, code commit `66334aa9`): the pinned Java `QueryMessageRequestHeader`, `QueryMessageResponseHeader`, and `ViewMessageRequestHeader` keep reference/wrapper fields nullable and use reflection under both `useFastEncode` hints. Rust now preserves absent or malformed boxed numeric fields through decode, retains Java identity-based equality for `ViewMessageRequestHeader`, and reports a missing/invalid query timestamp at the RPC result boundary where Java unboxes it. The invoked fixture and Rust differential cover both hints; this closes only this header-decoding slice.

## Additional public client API found during the Phase 18 audit

The pinned Java `client/MqClientAdmin.java` declares 19 public asynchronous
operations and `MQClientAPIExt.getMqClientAdmin()` exposes its public
`MqClientAdminImpl`. The interface includes query, topic/subscription-group
updates and deletion, offset reset, cluster/consumer inspection, statistics,
and direct-consume operations. The earlier `MQAdmin` and Producer/Push/Pull
facade rows did not crosswalk this separate interface. The matrix now records
source-backed typed RPC slices and invoked Java fixtures for all 19 methods,
and the Rust wrapper surface exposes all 19 through
`MQClientApi::mq_client_admin()`. The full method-by-method audit of defaults,
input/error branches, response DTO APIs and reverse exports remains open.
Public status and proxy call sites do not establish an exclusion from the
client parity contract.

The Java method inventory is `queryMessage`, `getTopicStatsInfo`,
`queryConsumeTimeSpan`, `updateOrCreateTopic`,
`updateOrCreateSubscriptionGroup`, `deleteTopicInBroker`,
`deleteTopicInNameserver`, `deleteKvConfig`, `deleteSubscriptionGroup`,
`invokeBrokerToResetOffset`, `viewMessage`, `getBrokerClusterInfo`,
`getConsumerConnectionList`, `queryTopicsByConsumer`,
`querySubscriptionByConsumer`, `getConsumeStats`, `queryTopicConsumeByWho`,
`getConsumerRunningInfo`, and `consumeMessageDirectly`.

Twelfth reconciled method slice (code commit `ae420dc6`): Rust exposes
`MQClientApi::mq_client_admin().query_message`, matching Java's direct
`MqClientAdmin.queryMessage` path. The request preserves the supplied address
and unique-key flag, uses the asynchronous remoting path, and decodes each
message with the requested decompression setting. Returned messages are
filtered by topic and either unique message ID or exact key token. Broker
`QUERY_NOT_FOUND` returns an empty list; other response codes and remarks are
preserved. The invoked Java fixture and scripted Core differential cover
compressed/uncompressed bodies, key and unique-key filtering, no result, and a
Broker error.

Thirteenth reconciled method slice (code commit `4d6cb941`): Rust exposes
`MQClientApi::mq_client_admin().view_message`, matching Java's direct
`MqClientAdmin.viewMessage(address, requestHeader, timeoutMillis)` path. It
sends request code 33 with `ViewMessageRequestHeader` through asynchronous
remoting, decodes the returned Broker message body, and preserves Broker
response codes and remarks. The invoked Java fixture records the signature,
timeout, request fields, encoded response body, decoded message, and Broker
error. Scripted Core tests compare the request and returned message to the
fixture and cover the Broker error path.

Fourteenth reconciled method slice (code commit `183660a5`): Rust exposes
`MQClientApi::mq_client_admin().consume_message_directly`, matching Java's
direct asynchronous request code 309 path. It sends
`ConsumeMessageDirectlyResultRequestHeader`, decodes the
`ConsumeMessageDirectlyResult` body, and preserves Broker response codes and
remarks. The invoked Java fixture records the method signature, timeout,
inherited and direct header fields, response body/result, and Broker error.
Scripted Core tests compare the request/result and reject malformed success
bodies.

Fifteenth reconciled method slice (code commit `b274784c`): Rust exposes
`MQClientApi::mq_client_admin().get_broker_cluster_info`, matching Java's
headerless asynchronous request code 106. The typed `ClusterInfo` decoder
handles Fastjson's unquoted numeric Broker-address keys and preserves a null
response body as `None`. The invoked Java fixture records the method signature,
address, timeout, response tables, null-body outcome, and Broker error.
Scripted Core tests compare the request and decoded tables and cover both
nullable-success and Broker-error paths.

Sixteenth reconciled method slice (code commit `eb627fcc`): Rust exposes
`MQClientApi::mq_client_admin().get_consumer_connection_list`, matching Java's
asynchronous request code 203 and `GetConsumerConnectionListRequestHeader`.
The response decoder preserves Java's nullable body, nullable set/table
properties, connection metadata and `SubscriptionData` values. The invoked
Java fixture records the request, body and decoded result, null-body outcome,
and Broker error. Scripted Core coverage compares request fields and all
returned data, and rejects malformed success bodies. Both the connection-list
and consume-stats request headers now have Java→Rust and Rust→Java typed-frame
fixtures; the consume-stats helper methods are covered against the Java header
fixture. Fixture SHA-256 is recorded in the Phase 18 release checklist.

Seventeenth reconciled method slice (code commit `7375c283`): Rust exposes
`MQClientApi::mq_client_admin().get_consume_stats`, matching Java's asynchronous
request code 208 and `GetConsumeStatsRequestHeader`. Its `ConsumeStats` and
`OffsetWrapper` models preserve zero defaults, nullable `offsetTable`, shared
map/entry aliasing, the Java public accessor/method surface, and the signed-long
wrapping results of both total-diff calculations. The invoked Java fixture
records the method signature, typed request fields, Fastjson MessageQueue-keyed
body, DTO methods/defaults/alias behavior, computed totals, null-body result,
and Broker error. Core scripted tests compare request/result data and cover
null bodies, Broker errors, and malformed successful bodies. Protocol tests
verify byte-for-byte body round trips and DTO defaults/aliases. The Java body
cross-decode inventory now contains 15 Rust typed bodies.

First reconciled method slice (code commit `07865bde`): Java
`queryTopicsByConsumer(address, requestHeader, timeoutMillis)` sends request
code 343 with `QueryTopicsByConsumerRequestHeader` and decodes a `TopicList`
only on `SUCCESS`; other response codes complete exceptionally. Rust now has
`MQClientApi::query_topics_by_consumer`, the typed header, and a `TopicList`
model. The Java header's `group` stays nullable because `checkFields()` is a
no-op despite its annotation; its inherited `ns`, `nsd`, `bname`, and `oway`
fields are preserved. `TopicList` defaults to an empty mutable Set, permits a
null Set through its setter, and retains the same Set object through its
getter. Invoked Java fixtures pin both decode hints, missing and malformed
Boolean fields, the request frame, and the response DTO defaults, JSON and
alias behavior. Rust tests cover the scripted successful RPC and DTO
null/alias behavior. A scripted Broker failure regression also verifies the
method's response code, peer address, and remark mapping.

Second reconciled method slice (code commit `2ac9351b`): Java
`querySubscriptionByConsumer(address, requestHeader, timeoutMillis)` sends
request code 345 with `QuerySubscriptionByConsumerRequestHeader` and returns
the nullable `SubscriptionData` from `QuerySubscriptionResponseBody` only on
`SUCCESS`. Rust exposes `MQClientApi::query_subscription_by_consumer`, the
typed header, and response body. The header preserves nullable `group`,
`topic`, and inherited `ns`, `nsd`, `bname`, `oway`, and `lo` fields; Rust's
decode-mode fixture covers valid, missing, and malformed Boolean fields under
both Java hints. The body fixture covers default/null JSON, all getters and
setters, and the shared nested object returned by the getter. Scripted Rust
RPC tests cover request fields, successful subscription decoding, nullable
subscription data, and Broker error mapping. Typed Java/Rust header
cross-decode and Rust header-inventory/validation rows are included.

Third reconciled method slice (code commit `c091d507`): Java
`queryTopicConsumeByWho(address, requestHeader, timeoutMillis)` sends request
code 300 with `QueryTopicConsumeByWhoRequestHeader` and decodes a `GroupList`
only on `SUCCESS`. Rust exposes `MQClientApi::query_topic_consume_by_who`,
the typed header, and `GroupList`. The header keeps topic and inherited
`ns`, `nsd`, `bname`, `oway`, and `lo` values nullable; the invoked
decode-mode fixture covers valid, missing, and malformed Boolean fields under
both Java hints. The GroupList fixture pins the empty-set default, nullable Set
and elements, JSON shape, and getter alias identity. Scripted Rust RPC tests
cover request fields, successful decoding, nullable response data, and Broker
error mapping. Header identity and validation rows include this third query
header.

Fourth reconciled method slice (code commit `e53ad60c`): Java
`getTopicStatsInfo(address, requestHeader, timeoutMillis)` sends request code
202 with `GetTopicStatsInfoRequestHeader` and decodes `TopicStatsTable` only on
`SUCCESS`. Rust exposes the typed header, `TopicOffset` and `TopicStatsTable`,
and `MQClientApi::get_topic_stats_info`. The DTO fixture pins Java's public
methods, primitive zero defaults, empty mutable offset-map default, nullable
map setter/getter alias, MessageQueue object-key JSON, and omission of null map
values. Header fixtures cover both decoder hints and inherited RPC fields.
Scripted RPC tests cover success, absent/JSON-null response bodies, Broker
errors, and malformed JSON. Fixture SHA-256:
`12c3a17f32ffb2b3b60e1e27d57ce8a96a199cb3ba0c7b1201790461b5ecd669`.

Fifth reconciled method slice (code commit `72a37b17`): Java
`queryConsumeTimeSpan(address, requestHeader, timeoutMillis)` sends request
code 303 and returns the decoded `QueryConsumeTimeSpanBody` list only on
`SUCCESS`. Rust reuses the typed header, `QueueTimeSpan` model, and
`MQClientApi::query_consume_time_span`; the Core call now uses the supplied
address unchanged to match `MqClientAdminImpl`. Push Consumer applies its
configured VIP address mapping before calling Core, preserving the Java
`MQClientAPIImpl` route used by that facade. The invoked Java fixture records
the method signature, direct address, timeout, inherited request fields,
response body/list values, and Broker exception. Rust tests cover those fields,
the returned values, exact Broker error forwarding, direct addressing with VIP
enabled, and Push Consumer VIP routing. Fixture SHA-256:
`16939c20d844e02921a2634dc32af32521b8a36b5d0de27b256ae88c777056aa`.

Sixth reconciled method slice (test/fixture commit `24eae6bf`): Java
`updateOrCreateTopic(address, requestHeader, timeoutMillis)` sends request code
17 with `CreateTopicRequestHeader` and completes with `null` only on `SUCCESS`.
Rust reuses `MQClientApi::create_topic`, which sends the same header directly
and maps Broker response code/remark into `RpcError`. The invoked Java fixture
records the exact method signature, request fields, null body, success result,
and `MQClientException` error code/remark. Rust tests compare success fields
and error mapping with that fixture while retaining the separate permission-
error regression. Both methods are recorded in
`fixtures/admin/query-consume-time-span.json`; SHA-256:
`16939c20d844e02921a2634dc32af32521b8a36b5d0de27b256ae88c777056aa`.

Seventh reconciled method slice (code/test commit `74c52143`): Java
`deleteTopicInBroker(address, requestHeader, timeoutMillis)` sends request code
215 with `DeleteTopicRequestHeader` and completes with `null` only on
`SUCCESS`. Rust exposes the nullable Java header (including inherited `ns`,
`nsd`, `bname`, `oway`, and `lo`) and
`MQClientApi::delete_topic_in_broker`; it sends the request directly to the
provided Broker address with no body. The invoked Java fixture records the
method signature, timeout, request fields, success result, and
`MQClientException` response code/remark. Rust scripted tests compare the
request fields and Broker error mapping to that fixture. Typed-header
cross-decode and inherited `RpcRequestHeader` equality inventory coverage are
included. The shared invoked Java fixture is
`fixtures/admin/query-consume-time-span.json`, SHA-256
`2a113cbc01a9e5bdce353b6705c6ebd734a5dd33832b147135eebdf6030d177b`.

Eighth reconciled method slice (code/test commit `fb1e43b5`): Java
`deleteTopicInNameserver(address, requestHeader, timeoutMillis)` sends request
code 216 with the Namesrv
`DeleteTopicFromNamesrvRequestHeader` and completes with `null` only on
`SUCCESS`. Rust exposes the nullable `topic`, `clusterName`, and inherited
`ns`, `nsd`, `bname`, `oway`, and `lo` fields through its typed header and
`MQClientApi::delete_topic_in_nameserver`; the RPC sends directly to the
provided NameServer address with no body. The invoked Java fixture records the
method signature, timeout, request fields, success result, and
`MQClientException` response code/remark. Rust scripted tests compare the
request fields and NameServer response mapping to that fixture. Typed-header
cross-decode and inherited `RpcRequestHeader` equality inventory coverage are
included. Shared admin fixture SHA-256:
`ee0c842383a4150957dddd40baa9a948cf536b0e00a72989ee8ee62fc71cdcc3`.

Ninth reconciled method slice (code/test commit `7cb6fb49`): Java
`deleteKvConfig(address, requestHeader, timeoutMillis)` sends request code 102
with the Namesrv `DeleteKVConfigRequestHeader` and completes with `null` only
on `SUCCESS`. Rust exposes nullable `namespace` and `key` fields through its
typed header and `MQClientApi::delete_kv_config`; the RPC sends directly to the
provided address with no body. The invoked Java fixture records the method
signature, timeout, request fields, success result, and `MQClientException`
response code/remark. Rust scripted Core tests compare the request fields and
error mapping to that fixture. Java/Rust typed-header cross-decode and the
header inventory include this request. Shared admin fixture SHA-256:
`ceafece5e4c70efab31e34760b8485b631a5691a46000c8d6bff32448fedbb0c`.

Status: **OPEN P18-V01 local parity work**. All 19 direct RPC method slices
have source-backed typed implementations and invoked fixtures. Reconcile the
remaining public DTO methods, exact overload/default/input/error behavior, and
Rust reverse exports. This is not a Phase 19 manual handoff.

## P18-V01 `updateOrCreateSubscriptionGroup` admin RPC — 2026-10-18 (`10db6f10`)

Rust exposes `MQClientApi::mq_client_admin().update_or_create_subscription_group`
for Java request code 200. The request sends the supplied address without
ext-fields and Fastjson-encodes `SubscriptionGroupConfig`; success completes
with `()`, while Broker errors preserve their response code and remark.
`SubscriptionDataSet` keeps insertion order for deterministic wire text while
matching Java Set membership, equality, null-member and duplicate behavior;
`SimpleSubscriptionData` equality and hash ignore `version` as in Java.

The invoked Java fixture records the interface signature, request body,
success/error behavior, DTO public methods/defaults, and Set duplicate/null
semantics. Core scripted tests cover request code/body and Broker errors;
Protocol tests cover serialization, DTO equality/hash and mutable shared
aliases. Fixture SHA-256 is `21985c21ea6f73efda1c5eceeff45359b937f112d181321b8237a1f431fee8a2`.
Protocol all-feature tests passed (141 unit tests, all integration targets and
51 doctests); Core `admin_rpc` passed all 49 tests. Strict all-target/all-feature
Clippy passed for Protocol, Remoting and Core. Pinned Maven `verify` passed all
37 scenarios, 66 typed-header cross-decodes and 15 typed-body cross-decodes;
format and diff checks passed. This closes the `updateOrCreateSubscriptionGroup`
RPC slice. Broad P18-V01 reconciliation, current-revision full-workspace
P18-V02, P18-V03 package/provenance review, P18-V04 Ubuntu/Windows evidence
and Phase 19 local scenarios remain open.

## P18-V01 `getConsumerRunningInfo` admin RPC — 2026-10-18 (`f05833a7`)

Rust exposes `MQClientApi::mq_client_admin().get_consumer_running_info` for
Java request code 307 and `GetConsumerRunningInfoRequestHeader`. Its typed body
decoder handles Fastjson's object-valued `MessageQueue` map keys and returns
`None` for a successful response without a body. Broker errors retain the
response code and remark.

The invoked Java fixture records the method signature, header fields, rich
running-info body, decoded snapshot, no-body result and Broker error. Core
scripted tests verify the request and result fields and cover no body, Broker
errors and malformed bodies. The existing Protocol fixture now also exercises
typed decoding of properties, subscriptions, normal/POP queue tables, status,
user info and jstack. Fixture SHA-256 is
`ae970b21d7822f9d213477bf47a227125b5d6242634a18c490001837b5828851`.
Protocol all-feature tests passed (141 unit tests, all integration targets and
51 doctests); Core `admin_rpc` passed all 50 tests. Strict all-target/all-feature
Clippy passed for Protocol, Remoting and Core. Pinned Maven `verify` passed all
37 scenarios, 66 typed-header cross-decodes and 15 typed-body cross-decodes;
format and diff checks passed.

All 19 source-backed `MqClientAdmin` RPC slices are now present. P18-V01's
broader API, settings, input/error/flow and reverse-export audit remains open,
as do current-revision full-workspace P18-V02, P18-V03 package/provenance,
P18-V04 Ubuntu/Windows evidence, and Phase 19 local Docker/Broker scenarios.

## P18-V01 Running Info property-key follow-up — 2026-10-18 (`a56ee59d`)

Pinned `ConsumerRunningInfo` declares six public property-key constants. The
Rust body now exposes matching associated constants, and Push/Pull Running
Info generation uses them. This caught and fixed a wire mismatch:
`PROP_CONSUME_ORDERLY`'s Java value is `PROP_CONSUMEORDERLY`, without the
underscore. The Java fixture now records all six names/values and includes the
orderly and thread-pool properties. The dependent MqClientAdmin fixture was
regenerated from the pinned oracle.

The Protocol Running Info body test and Push property-generation regression
both passed; strict all-target/all-feature Clippy passed for Protocol and
Consumer; formatting and diff checks passed. Pinned Maven `verify` passed 37
scenarios, 66 typed-header cross-decodes and 15 typed-body cross-decodes.
Fixture SHA-256 values are recorded in the [DTO mapping](PHASE_6_API_CONFIG_MAP.md#running-info-public-property-key-constants).

The class's analysis/formatting helpers are called only from the separately
excluded `rocketmq-tools` artifact. The broader P18-V01 client DTO/API,
settings, input/error/flow and reverse-export audit remains open.

## Tools-only admin extension scope audit

The pinned Java client implementation also contains public
`MQClientAPIImpl.checkRocksdbCqWriteProgress`,
`MQClientAPIImpl.exportRocksDBConfigToJson`, and
`MQClientAPIImpl.getInSyncStateData` methods. At source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`, these are called by
`tools/.../DefaultMQAdminExtImpl` and exposed by the `tools/.../MQAdminExt`
interface and `DefaultMQAdminExt` facade. They are not `client` `MQAdmin`
operations or Producer/Push/Pull client-facade methods. The result DTOs
`CheckRocksdbCqWriteResult` and `BrokerReplicasInfo` likewise surface through
the tools extension. The Phase 17 plan explicitly excludes the separate
`rocketmq-tools` artifact, so these helpers are `TOOLS_OUT_OF_SCOPE`, not
missing Rust client capabilities. Rust still carries request code 354 as a
protocol constant; this does not expose a RocksDB administration API.

The same pinned-source call-site scan places
`updateColdDataFlowCtrGroupConfig`, `removeColdDataFlowCtrGroupConfig`,
`getColdDataFlowCtrInfo`, and `setCommitLogReadAheadMode` behind
`tools/.../MQAdminExt`. Their low-level `MQClientAPIImpl` request operations
are mapped by Core for the Phase 18 client API audit; the tools facade methods
and command-line commands remain outside Phase 17.

Pinned-source check:

```bash
git -C /Users/bruce.tsai/Projects/3rd/rocketmq grep -n -E 'checkRocksdbCqWriteProgress|exportRocksDBConfigToJson|CheckRocksdbCqWriteResult|getInSyncStateData|BrokerReplicasInfo' efa1be59d6fc747ad220f036c86030f05f0419f9 -- client/src/main/java tools/src/main/java common/src/main/java remoting/src/main/java
```

This closes the scope disposition for these three tools-backed helpers only; it
does not close P18-V01's broader source/API/input/reverse-export audit.

## Concrete facade methods outside `MQAdmin`

| Java method | Rust counterpart | Status and remaining work |
| --- | --- | --- |
| Producer `fetchPublishMessageQueues(topic)` | `Producer::fetch_publish_message_queues` | Implemented. Scripted tests cover route-not-found, empty-route, malformed-route, and successful queue conversion. |
| Push/Pull `fetchSubscribeMessageQueues(topic)` | `PushConsumer::fetch_subscribe_message_queues`, `PullConsumer::fetch_subscribe_message_queues` | Implemented. Both remove the namespace from returned queues. Tests cover missing and malformed routes. Push returns an empty set for an empty route as Java does; Pull returns the Java admin error when NameServer returns an empty queue set. |

## Push consumer offset reset

The pinned Java `MQClientInstance.resetOffset` walks the active Push process
queues, drops and clears matching queues whose keys occur in the supplied
offset table, then processes only non-null offsets. Non-orderly reset waits
10 seconds before updating offsets; orderly reset waits up to 10 seconds for
the consume write lock. Orderly clustered removal then waits up to 500 ms for
that lock, unless the dropped queue's unlock delay has elapsed. Java removes
the local queue even when that removal attempt times out; it removes the stored
offset only on the successful orderly unlock path. A null offset therefore
drops and clears a matching queue but does not update its offset or remove it.

Rust matches these transitions: reset cancels the old queue generation so late
pull replies and consume callbacks cannot update the reset offset, does not
freeze the reset value, and removes only queues with a non-null mapped offset.
The orderly removal path persists before and during removal and preserves the
Java distinction between local queue removal and stored-offset cleanup. Paused
time regressions cover the 10-second and 500-ms windows, callback fencing,
available-lock unlock, null offsets, and queue rows without a live process
queue. The pinned source is `MQClientInstance.resetOffset` and
`waitResetOffsetReady`, plus `RebalancePushImpl.removeUnnecessaryMessageQueue`
and `tryRemoveOrderMessageQueue`.

## Client inbound administrative requests

Java `ClientRemotingProcessor` implements `RESET_CONSUMER_CLIENT_OFFSET`,
`GET_CONSUMER_STATUS_FROM_CLIENT`, `GET_CONSUMER_RUNNING_INFO`, and
`CONSUME_MESSAGE_DIRECTLY`. Rust now registers Push handlers for all four and
Pull handlers for status/running-info; Pull reset stays one-way and Pull direct
consume returns Java's missing-push-consumer error. Dispatch uses a bounded
executor whose accepted work is joined during runtime shutdown. Push queue
snapshots, Pull properties, JSON layout and credential omission have local
coverage. Push/Pull consume and POP paths now populate Java-style per-topic
success/failure counts, 10-second and 10-minute TPS/RT snapshots, and the
one-hour failure count; Running Info has non-zero statistics coverage. The
direct Java fixture confirms each `MQClientInstance` has an instance-scoped
`ConsumerStatsManager`, including separate instances with the same client ID.
Rust stores the manager on `ClientRuntime`; Push owners on the same runtime
share it, while separate runtimes with the same client ID remain isolated.
`jstackEnable` dump enumerates process threads on macOS ARM64 and x86_64,
Linux AArch64 and x86_64, and Windows x86_64 and ARM64. Linux sends a queued
signal to each target thread, reads its `ucontext`, and walks readable frame
records; it does not use ptrace or change `PR_SET_PTRACER`. It restores the
previous signal handler after capture. Windows enumerates with Toolhelp,
suspends one thread at a time, reads its architecture-specific context, and
resumes it through an RAII guard. If the shared deadline expires, all remaining
enumerated thread IDs are still included with an unavailable-stack marker.
Thread captures are serialized on each supported platform, so simultaneous
running-info requests do not receive a degraded current-thread-only dump or
interleave process-wide signal-handler changes. Linux stack tests cover the two
concurrent readers and signal-handler restoration on ARM64; Linux stack tests
pass on both architectures. The complete Windows consumer
crate checks and strict Clippy pass for x86_64; the isolated stack module and
tests type-check with warnings denied for x86_64 and ARM64. Native Windows
runtime behavior has not been executed on this macOS host and stays an
environment check for final validation.

Linux signal-availability follow-up (`4c142334`, 2026-10-15): the latest
inspectable GitHub Ubuntu run for older revision `cc79eb6e` failed
`jstack_includes_current_and_other_named_threads` because neither existing
default-ignore candidate (`SIGWINCH`, `SIGURG`) was usable. Added `SIGCHLD` as
another default-ignore candidate and regressions for fallback selection and
the all-candidates-blocked case. The pinned Java 5.5.1 path uses
`Thread.getAllStackTraces()` and does not have this signal-selection boundary.
The updated Linux module and tests pass an isolated x86_64 Linux target
type-check; a complete workspace cross-check cannot run on this Mac because
the Linux C sysroot/compiler needed by AWS-LC and zstd is unavailable. The
current revision subsequently passed default/all-feature workspace tests and
strict Clippy in a Linux ARM64 container after the transient-task follow-up
below. Current-revision GitHub Ubuntu and native Windows job results remain
pending.

Linux transient-task exit follow-up (`ac0a16f3`, 2026-10-17): the default
workspace run exposed a race where `/proc/self/task` iteration can report
`ESRCH` while a short-lived thread exits. Thread enumeration now skips only
the disappeared task (`ENOENT`/`ESRCH`) and still reports unrelated I/O
errors. A unit test pins the error classification. The focused regression,
default/all-feature workspace suites, and strict all-target/all-feature
workspace Clippy pass on Linux ARM64. GitHub Ubuntu and Windows execution
remain separate P17/P18 platform evidence.

## Verification status

- **P17-V01:** Local API coverage passes. Route-not-found, empty-route and
  malformed-route behavior is covered for publish/subscribe queue queries.
  Offset and create-topic permission errors and unique-key terminal
  `NO_MESSAGE` are forwarded by all three facades. Core tests cover Broker
  errors and malformed responses for offset RPCs, create-topic permission
  errors, query failures/malformed bodies, and view-message not-found/malformed
  responses. Native Windows stack runtime execution remains a final platform
  check. The GitHub workflow now includes a fail-closed Windows job that runs
  the native stack capture regression; current-revision runtime evidence is
  still pending that job. Cross-target compile and strict lint evidence is
  recorded above.
- **P17-V03:** Reset fencing has late-pull and slow-callback regressions;
  direct-consume offset isolation, slow-callback status responsiveness, and
  executor shutdown joining also have targeted tests. The full consumer suite
  passes on macOS ARM64, Linux AArch64, and Linux x86_64.
- **P17-U01:** Complete. Scripted multi-Broker tests cover duplicate results,
  completion-order merge, bounded time fields, partial failure, maximum
  watermark, cluster/key filtering, `lastKey`, empty results and terminal
  Broker failures. Ordering was checked against the pinned Java callback
  aggregation in `MQAdminImpl.queryMessage`.

**P17-V02** passed against the pinned RocketMQ 5.3.4 image with Java 5.5.1 and
Rust. The clean run at
`20261025T172205Z` (raw output not retained) seeded
duplicate keys on both Brokers and an in-range/out-of-range timestamp pair.
Both clients returned the same two broad duplicate results and the same single
bounded result. With Broker B query replies changed to `SYSTEM_ERROR`, both
clients retained the message seeded on Broker A; with both Brokers failing,
both mapped the terminal result to `NO_MESSAGE` (response code 208). Query
message records omit the Broker name (`null` in Java and empty in Rust), so the
live comparison normalizes that absent field and checks the distinct seed
bodies. The runner removed the scenario topic and isolated Compose resources.

Phase 18 server-free gates have since been implemented and run locally. The
remaining parity and release-preparation work is tracked in the
[Phase 18 release checklist](../release-checklist.md); final Phase 19
acceptance remains open.

Latest local gates passed: locked consumer tests and strict all-target/all-
feature consumer Clippy on macOS ARM64, Linux AArch64, and Linux x86_64 (164
unit tests plus all consumer integration/doctests per platform); core
`admin_rpc` tests (16 cases); producer tests (92 unit, 10 admin, 5 facade,
3 request/reply, 10 scripted-send, and 6 transaction cases) and strict Clippy;
pinned Maven `verify` (32 scenarios and
Java/Rust fixture cross-decodes); differential comparator self-test; workspace
metadata; `cargo fmt --all -- --check`; and `git diff --check`. The complete
consumer crate also passes locked Windows x86_64 GNU `cargo check` and strict
all-target/all-feature Clippy in a Rust 1.85.0 MinGW container. An isolated
stack harness with warnings denied passes for Windows x86_64 GNU and ARM64
MSVC. No native Windows runtime or Broker-backed Phase 19 validation was run.

## P18-V01 `ConsumerConnection.computeMinVersion` DTO method — 2026-10-18 (`d8f84f2c`)

The pinned Java `ConsumerConnection.computeMinVersion()` initializes its
minimum to `Integer.MAX_VALUE` and returns that for an empty connection set;
otherwise it returns the minimum `Connection.version`. A null set or null
member throws `NullPointerException`. Rust now exposes
`ConsumerConnection::compute_min_version`, preserves the empty-set result and
reports typed errors at those two null boundaries. The invoked Java fixture
and Protocol regression cover default/explicit empty sets, default and mixed
versions, signed integer bounds, null sets, null members, and the reflected
method signature. Fixture SHA-256:
`748138c8020551f397ab68bbb24e8e858b5345e95fb78998d0051cea5bbd5f20`.

This closes this response DTO method slice only. All 19 source-backed
`MqClientAdmin` RPC slices are present, but the broader P18-V01 Java/Rust
source/API/settings/input/error/flow and reverse-export audit remains open.

## P18-V01 `ConsumerConnection` and `ProducerConnection` Set semantics — 2026-10-23 (`05207a17`)

Pinned Java exposes `HashSet<Connection>` from both DTOs. `Connection` inherits
`Object.equals` and `Object.hashCode`, so set membership follows object
identity: adding one reference twice or adding null twice keeps one member,
while separate objects with identical fields remain separate. Rust previously
used `Vec` for these fields. Both DTOs now use an identity-keyed
`HashSet<Option<SharedValue<Connection>>>`; cloned handles represent one Java
object, independently decoded objects remain distinct, and nullable Set
fields plus getter/setter aliasing are preserved. The invoked fixtures test
direct Java membership and decode `[null,null,{"version":7},{"version":7}]`;
Protocol regressions cover both DTO decoders and the ConsumerConnection RPC
path. Fixture SHA-256 values are
`9e011a55e633140ed06209762aec715cf88b14dfd8724f7239fe24effee7309c`
(`query-consume-time-span.json`) and
`d635048972229fa1ba2c03b3e2ad0e4600c6de32493ad927543be75a9aaa6de2`
(`core.json`).

Default and all-feature workspace tests, the final Protocol unit suite (160
tests), strict all-target/all-feature Clippy, offline Maven `verify` (37
scenarios and Java/Rust fixture cross-decodes), formatting, and diff checks
pass. This closes the two DTO Set collection mappings only; broad P18-V01
reconciliation remains open.

## P18-V01 `ProducerConnection` assigned-Set alias — 2026-10-23 (`b41efa8b`)

The pinned Java `ProducerConnection` setter retains the supplied
`HashSet<Connection>` reference, and its getter returns that same object; a
mutation through the getter is visible through the caller's set. Rust now has
a direct regression for `set_connection_set` / `get_connection_set` that
checks `SharedValue::ptr_eq` and observes mutation through the assigned
container. The Java fixture invokes the DTO accessors directly using an
isolated DTO, so the probe does not alter the separately captured RPC response.
Updated `fixtures/producer/core.json` SHA-256:
`412fb8f5d9805edb2f994876d81ca980bfccc2963a1aff2bea19b98319a1eb5a`.

The Protocol library suite (160 tests), strict all-target/all-feature workspace
Clippy, offline Maven `verify`, formatting, and diff checks pass. This closes
the ProducerConnection assigned-Set alias proof only; broad P18-V01 remains
open.

## P18-V01 `ClusterInfo` DTO surface — 2026-10-18 (`6b40e86c`)

Pinned Java `ClusterInfo` declares private broker/cluster tables with public
getters and setters, `retrieveAllAddrByCluster`,
`retrieveAllClusterNames`, `equals`, and `hashCode`. Rust now exposes the
matching accessors and retrieval methods, preserves mutable aliases for both
tables and their nested values, and implements the Java value/hash behavior.
Null table, null broker-name set, null broker-address table, and null
`BrokerData.brokerAddrs` failures map to typed Rust errors; missing brokers
remain skipped and nullable returned addresses remain present. The Rust
retrieval methods return `Result<Vec<Option<String>>, _>` to express Java's
null members and failure boundary.

The invoked Java fixture and Protocol tests cover the reflected method list,
defaults, setter/getter identity, nested alias mutation, null/missing lookup
cases, `equals`, and `hashCode`; the scripted Core test continues to cover the
`getBrokerClusterInfo` RPC. Fixture SHA-256:
`56e50086556b77e98b2829708b41a6131909dcbf855109364b49cee73097dfc5`.
Java `HashMap`/`HashSet` iteration order is unspecified; the retrieval
differential uses singleton collections and does not claim cross-language
multi-element ordering. This closes only the `ClusterInfo` DTO slice; broad
P18-V01 reconciliation remains open.

## P18-V01 `QueueTimeSpan` and `QueryConsumeTimeSpanBody` DTO API — 2026-10-18 (`423bcdbc`)

The pinned Java `QueueTimeSpan` fields are private, and
`QueryConsumeTimeSpanBody.consumeTimeSpanSet` is package-private. Rust now
matches those visibility boundaries and exposes the Java getter/setter surface.
`SharedValue` preserves the assigned `MessageQueue`, list, and non-null list-row
identities, including mutations through either side; explicit null remains
null, and the default list remains empty. Core snapshots the decoded rows into
its existing owned return type.

The invoked Java fixture records constructors, declared public methods,
defaults, null setters, object/list identity, element identity, and mutations.
Protocol and Core differentials cover the DTO and admin RPC boundaries. Fixture
SHA-256: `3012f8dfb49f4bd404e1d29a7e27cdbe14d47435347bcb55ac807e5649085af5`.
This closes only the consume-time-span DTO API slice; broad P18-V01
reconciliation remains open.

## P18-V01 `GroupList` DTO API inventory — 2026-10-18 (`3b8a62b3`)

The Java fixture now records the public no-argument constructor, empty public
field set, and the exact declared `getGroupList`/`setGroupList` signatures.
Rust's differential asserts that inventory and exercises the mapped accessors;
a compile-fail doctest guards the private DTO field. The existing tests retain
coverage for the default empty Set, explicit null, nullable elements, and
bidirectional Set alias mutation. Fixture SHA-256:
`5961395fa36f199d8b3745f91d1dcc4941c1d2d8748f272e119ed9c3ae1ad73a`.
This closes only the `GroupList` public API slice; broad P18-V01 reconciliation
remains open.

## P18-V01 `TopicList` DTO API inventory — 2026-10-18 (`5233eb2f`)

The Java fixture now includes the public no-argument constructor alongside its
four declared public accessor signatures and empty public-field set. Rust
asserts the exact inventory, exercises topic-set and broker-address accessors,
and compile-fail checks the private topic-set field. Existing tests cover the
empty default, explicit null, duplicate-set semantics and bidirectional
set-alias mutation. Fixture SHA-256:
`11d0a8919e272d3f3ed347695a06220a3688c3437805a0e5737ba4fccd43726b`.
This closes only the `TopicList` public API slice; broad P18-V01 reconciliation
remains open.

## P18-V01 `TopicStatsTable` and `TopicOffset` API inventory — 2026-10-18 (`93412b9a`)

The pinned Java fixture now records both public no-argument constructors in
addition to the declared method and public-field inventories. Rust asserts
those constructor signatures, guards the private DTO fields with compile-fail
doctests, and directly checks all three `TopicOffset` setters/getters at
`i64::MIN`, `i64::MAX`, and a negative timestamp. Existing tests continue to
cover Java defaults, formatting, nullable map values, map/entry aliasing, and
`TopicStatsTable` accessors. Fixture SHA-256:
`9b1bd6d1cebd844db4415a45e60cee14442e76dc235662d5189056977da0aeb6`.
This closes only these response DTO API inventory items; broad P18-V01
reconciliation remains open.

## P18-V01 `QuerySubscriptionResponseBody` API inventory — 2026-10-18 (`67edffe7`)

The Java fixture now includes its public no-argument constructor and exact
declared getter/setter signatures alongside the empty public-field inventory.
Rust asserts that inventory, compile-fail checks its private fields, and
directly exercises the subscription, group and topic setters including null
reset. Existing tests cover defaults, JSON behavior, and shared
`SubscriptionData` identity/mutation. Fixture SHA-256:
`e5c6405810fbe550a58bd7ce3b843b3e21410fc3e1674c1692f18836a2e969af`.
This closes only this DTO API inventory slice; broad P18-V01 reconciliation
remains open.

## P18-V01 `MQClientAPIExt` notification operations

The pinned `MQClientAPIExt` declares `notification` and
`notificationWithPollingStats` using request 200054. Rust now implements both
through `MQClientApi`: `notification` returns the Java `hasMsg` Boolean, while
`notification_with_polling_stats` returns `NotifyResult` with both response
flags. The DTO keeps fields private, matches Java's public constructor and
accessor inventory, preserves false defaults for missing or malformed boolean
fields, and renders the pinned `toString()` form. Non-success response codes
and remarks retain the common typed Broker error path.

The invoked Java fixtures record the request ext fields, async invoke mode,
response flags, `NotifyResult` public API, defaults and string form for both
entrypoints, plus known and unknown Broker errors. Rust's scripted peer
compares all five cases. The `POLLING_INFO` request code remains a wire registry
entry; pinned client sources contain no outbound callsite for it.

## P18-V01 `MqClientAdmin` Rust wrapper surface — 2026-10-21

`MQClientApi::mq_client_admin()` now exposes all 19 methods declared by the
pinned Java `MqClientAdmin` interface. Eleven wrappers delegate to the
corresponding source-audited `MQClientApi` methods; `update_or_create_topic`
uses the direct request path so it does not inherit `MQClientAPIImpl`'s
separate local permission validation. The Java fixture records the full
interface method signatures and an invalid-permission request. The Core test
checks the exact 19 Java method names and compile-calls every Rust wrapper; the
scripted RPC test verifies topic-update request fields. Fixture SHA-256:
`16f61c5e859f44062a2ec732e2603ad97b38719b9a0f8fa23fc09ffa5112dfd3`.
All 82 Core `admin_rpc` tests and Core strict Clippy pass; Maven `verify`
passes with 37 scenarios, 83 typed header frames and 16 typed body frames.
This closes the wrapper surface; the broader P18-V01 source/input/error/flow
and reverse-export audit remains open.

P18-V01 query-key filter inputs: the invoked Java `MqClientAdmin.queryMessage`
fixture now checks an empty query key against an internal empty token, NBSP,
em-space, U+0085 and U+001C keys. Rust matches Apache Commons
`StringUtils.isNotBlank` / Java `Character.isWhitespace` and Java
`String.split(" ")` trailing-token behavior in both the direct wrapper and
multi-Broker result merge. `mq_client_admin_query_message_matches_invoked_java_fixture`
and `query_message_keys_match_java_whitespace_and_split_semantics` compare the
same outcomes. Expanded fixture SHA-256:
`16f61c5e859f44062a2ec732e2603ad97b38719b9a0f8fa23fc09ffa5112dfd3`.

## Tools-only controller administration scope

The pinned Java `MQClientAPIImpl` also has public methods
`getControllerMetaData`, `getInSyncStateData`, `getBrokerEpochCache`,
`getControllerConfig`, `updateControllerConfig`, `electMaster`, and
`cleanControllerBrokerData`. The pinned-source call-site scan finds these
operations are exposed through `tools/.../MQAdminExt` and
`DefaultMQAdminExt`, and invoked by `tools/.../DefaultMQAdminExtImpl` and
controller/HA administration commands. `getInSyncStateData`, `electMaster`,
and `cleanControllerBrokerData` also compose `getControllerMetaData` inside
`MQClientAPIImpl`. These are not `rocketmq-client` `MQAdmin` methods or
Producer/Push/Pull runtime operations. Following the Phase 17 contract to
exclude the separate `rocketmq-tools` suite, the seven controller admin
facade workflows are `TOOLS_OUT_OF_SCOPE`. The earlier
`getInSyncStateData` classification is unchanged. The operation-specific
call-site evidence was checked at pinned source commit `efa1be59d6fc747ad220f036c86030f05f0419f9` with:

```bash
git -C /Users/bruce.tsai/Projects/3rd/rocketmq grep -n -E 'getControllerMetaData|getInSyncStateData|getBrokerEpochCache|getControllerConfig|updateControllerConfig|electMaster|cleanControllerBrokerData' efa1be59d6fc747ad220f036c86030f05f0419f9 -- client/src/main/java tools/src/main/java common/src/main/java remoting/src/main/java
```

This narrows the tools-only disposition to these admin facade workflows; the
broader P18-V01 client source/API/settings/input/error/reverse-export audit
remains open.
