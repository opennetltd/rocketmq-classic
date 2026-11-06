# Phase 5 offline typed RPC evidence

Phase scope: one-attempt typed client RPCs, independent of public Producer/Consumer runtimes. Source baseline is Java client 5.5.1, tag `rocketmq-all-5.5.1`, commit `efa1be59d6fc747ad220f036c86030f05f0419f9` in the read-only reference repository. Server 5.3.4 is not contacted by these offline tests. Phase 19 has live evidence for the P5-V02 route, producer, and consumer subcases. The first extension attempt reached heartbeat, heartbeat V2, assignment, and request-mode success, then Broker 5.3.4 rejected Lite subscription request 200071 as unsupported; the extension subcase remains pending because its successful Lite/POP path was not established.

At Phase 5 acceptance there were 22 public domain methods, one constructor and no new core configuration settings. `request`, `oneway`, mode selection and response decoders are private implementation helpers. Java ASYNC completion is represented by Rust futures, as permitted by the Rust-native implementation contract; request mode, response interpretation, timeout behavior and pending ownership remain mapped. Phase 7 owns producer selection/retry policy; Phase 8 owns binary message decoding; Phase 13 owns public POP/Lite consumer logic. No missing Phase 5 RPC is deferred to those phases.

Later source-owned runtime RPC additions are tracked in the [Phase 6 inventory](PHASE_6_RUNTIME_INVENTORY.md), including default-topic route lookup, `MQClientApi::check_client_in_broker` (request 46), and consume timestamp span query (request 303). These additions use the existing one-attempt request/classification pipeline and invoked Java runtime/API fixtures; they are not included in the historical 22-method count or 148-case Phase 5 corpus below.

## Bidirectional method and branch inventory

The table records Java `MQClientAPIImpl` methods and its `MQClientAPIExt` extensions. Fixture names are under `fixtures/`; each operation family has its `rpc_invoked*_fixtures.rs` scripted comparison in Core tests. SUCCESS with no decoded payload has no malformed-payload branch; those cases instead assert that unused payload bytes are ignored. Oneway has no Broker response branch.

| Public Rust API | Java entrypoint / mode | Persisted invocation and scripted branch evidence |
| --- | --- | --- |
| `MQClientApi::new` | API construction with an injected RemotingClient | All domain suites construct independent clients, explicitly start and shut down transport; no singleton |
| `get_topic_route` | `getTopicRouteInfoFromNameServer`, SYNC | `rpc-invoked/route-{success,not-found,unknown,no-body,invalid-json}`; `rpc.rs:route_success_unknown_status_and_malformed_success` |
| `send_message` | `sendMessage`, SYNC / ASYNC / ONEWAY | `rpc-invoked-producer`: all four send statuses, error, oneway, end transaction; `rpc-invoked-producer-more`: V1/V2/reply/batch in all modes, V2 malformed/unknown sync and async. `producer.rs:every_send_variant_classifies_reply_before_decoding` crosses five variants, two response modes, four statuses, unknown and missing success headers. Invoked Java `checkSendModes` crosses the same dispatch/status families, including async transport failure |
| `end_transaction_oneway` | `endTransactionOneway`, ONEWAY | `end-transaction-oneway`; `producer.rs:oneway_send_and_end_transaction_do_not_wait_for_reply`: header, remark and no response wait |
| `pull_message` | `pullMessage`, SYNC | `rpc-invoked-consumer-more/pull-*` and `lite-pull-*`: FOUND, NO_NEW_MSG, NO_MATCHED_MSG, OFFSET_ILLEGAL, error, unknown, missing and invalid header |
| `pull_message_future` | `pullMessage`, ASYNC | Same cases with `-async` suffix; actual Java callback driven by mock. Lite flag selects code 361 in both modes; otherwise code 11. Raw message bytes, offsets and request-code error context compare exactly |
| `query_consumer_offset` | `queryConsumerOffset`, SYNC | `rpc-invoked/offset-{success,not-found,unknown,no-header}`; QUERY_NOT_FOUND is retained as a structured response error, distinct from transport failure |
| `query_consumer_offset_with_future` | `MQClientAPIExt.queryConsumerOffsetWithFuture`, ASYNC | `rpc-invoked-ext-more/query-offset-future-{success,null-offset,not-found}`; success may complete with a nullable boxed offset, and QUERY_NOT_FOUND retains the distinct `OffsetNotFound` error outcome |
| `query_consume_time_span` | `queryConsumeTimeSpan`, SYNC | `fixtures/runtime/lookup.json:queryConsumeTimeSpanApiCase` and scripted loopback; code/header/body fields plus `TOPIC_NOT_EXIST` request and error behavior |
| `update_consumer_offset` | `updateConsumerOffset` / `updateConsumerOffsetOneway` | `rpc-invoked/update-{success,error,unknown,oneway}`; `consumer_void_success_has_no_malformed_response_payload` |
| `update_consumer_offset_async`, `update_consumer_offset_oneway` | `MQClientAPIExt.updateConsumerOffsetAsync` / `updateConsumerOffsetOneWay`, ASYNC / ONEWAY | `rpc-invoked-ext-more/update-offset-future-{success,error,unknown}` and `update-offset-oneway-success`; request mode and Broker-code completion compare |
| `get_consumer_list_by_group` | `getConsumerIdListByGroup`, SYNC | `rpc-invoked-consumer/consumer-list-{populated,empty,no-body,error,unknown,invalid-json}`; absent body is a domain error even with SUCCESS |
| `get_consumer_list_by_group_future` | `MQClientAPIExt.getConsumerListByGroupAsync`, ASYNC callback | `rpc-invoked-ext-more/consumer-list-future-{populated,empty,success-empty-body,null-list,error}`; populated/null-list decode, Java `SYSTEM_ERROR` empty list, and SUCCESS/no-body switch fall-through |
| `consumer_send_message_back` | `consumerSendMessageBack`, SYNC | `rpc-invoked-consumer/send-back-{success,error,unknown}`; no decoded success payload |
| `consumer_send_message_back_async` | `MQClientAPIExt.sendMessageBackAsync`, ASYNC | `send-back-future-{success,error}`; preserves raw `RemotingCommand` status and remark instead of classifying it |
| `lock_batch_mq` | `lockBatchMQ`, SYNC | `rpc-invoked-consumer-more/lock-{success,error,unknown,no-body,invalid-json}`; queue set and JSON body compare semantically |
| `lock_batch_mq_with_future` | `MQClientAPIExt.lockBatchMQWithFuture`, ASYNC | `rpc-invoked-ext-more/lock-batch-future-{success,error}`; response queue set and body compare semantically |
| `unlock_batch_mq` | `unlockBatchMQ`, SYNC / ONEWAY | `unlock-sync-{success,error,unknown}`, `unlock-oneway`; no decoded success payload |
| `unlock_batch_mq_oneway` | `MQClientAPIExt.unlockBatchMQOneway`, ONEWAY | `unlock-batch-oneway-success`; request body and no-response completion |
| `search_offset_future`, `get_max_offset_future`, `get_min_offset_future` | `MQClientAPIExt.searchOffset`, `getMaxOffset`, `getMinOffset`, ASYNC / ASYNC callback | Corresponding `*-offset-future-{success,null-offset,error}` fixtures; nullable boxed response values, request headers and Broker errors |
| `send_heartbeat` | `sendHeartbeat`, SYNC | `rpc-invoked-ext-more/heartbeat-{success,error,unknown}`; return wire version; unused body ignored |
| `send_heartbeat_async`, `send_heartbeat_oneway` | `MQClientAPIExt.sendHeartbeatAsync`, ASYNC / `sendHeartbeatOneway`, ONEWAY | `rpc-invoked-ext-more/heartbeat-future-{success,error,unknown}` and `heartbeat-oneway-success`; request language/body and response version/status; one-way never waits for a reply |
| `send_heartbeat_v2` | `sendHeartbeatV2`, SYNC | `heartbeat-v2-{success,default,error,unknown}`; uppercase extension keys, optional booleans, version, exact heartbeat JSON `withoutSub`. `assignment_invalid_json_and_heartbeat_v2_default_flags` covers invalid flag defaults |
| `unregister_client` | `unregisterClient`, SYNC | `unregister-{success,error,unknown}`; no decoded success payload |
| `query_assignment` | `queryAssignment`, SYNC | `assignment-{empty,populated,error,unknown,no-body,invalid-json}`; mode, attachments and queues compare |
| `set_message_request_mode` | `setMessageRequestMode`, SYNC | `request-mode-{success,error,unknown}`; exact typed request body, no decoded success payload |
| `lite_subscription_ctl` | `MQClientAPIExt.syncLiteSubscriptionAsync`, ASYNC | `rpc-invoked-ext/lite-subscription-{success,error,unknown}`; exact header/body, future completion, no decoded success payload |
| `notification`, `notification_with_polling_stats` | `MQClientAPIExt.notification` / `notificationWithPollingStats`, ASYNC | `rpc-invoked-ext-more/notification-{stats-success,bool-success,error,unknown,defaults}`; exact request code/header, Boolean convenience result, `NotifyResult` accessors/toString/defaults, Broker status mapping and Java malformed-boolean defaults |
| `get_lite_topic_info` | `MQClientAPIExt.getLiteTopicInfoAsync`, ASYNC | `rpc-invoked-ext-more/lite-topic-info-{success,defaults,null-body,null-inputs,error,unknown,invalid-body}`; nullable inputs/body, DTO defaults and aliases, Broker errors and malformed JSON |
| `invoke`, `invoke_oneway` | `MQClientAPIExt.invoke` / `invokeOneway`, ASYNC / ONEWAY | `invoke-raw-error-response` preserves the raw non-success response; `invoke-oneway-success` compares request body and call-boundary completion |
| `ClientRuntime::update_name_server_address_list_from_config` | `MQClientAPIExt.updateNameServerAddressList`, local | `nameserver-refresh-{present,null,empty}`; live setting read, null no-op/false, empty-string update and resulting list membership |
| `pop_message` | `popMessageAsync`, ASYNC | `rpc-invoked-pop-ack/pop-*`: FOUND, POLLING_FULL, POLLING_TIMEOUT, PULL_NOT_FOUND, error/unknown, absent FOUND body and invalid/missing primitive header defaults; Java-encoded binary message retained unchanged |
| `pop_lite_message` | `popLiteMessageAsync`, ASYNC | Corresponding `pop-lite-*` cases; header metadata and exact raw binary response compare |
| `ack_message` | `ackMessageAsync` / `ackLiteMessageAsync`, ASYNC | `ack-*` and `ack-lite-*`: SUCCESS maps OK, all other codes map NO_EXIST. Both Java entrypoints dispatch ACK_MESSAGE, not ACK_LITE_MESSAGE; no decoded success payload |
| `batch_ack_message` | `batchAckMessageAsync` body overload, ASYNC | `batch-ack-*`: OK / NO_EXIST including unknown; exact short-key/Base64 body representation inherited from Phase 4, no decoded success payload |
| `change_invisible_time` | `changeInvisibleTimeAsync`, ASYNC | `change-*`: OK, error/unknown NO_EXIST, invalid-number SUCCESS defaults. Typed pop time/invisibility/revive queue metadata compares against Java extra-info components |

Shared branch checks are in `consumer.rs:every_consumer_rpc_rejects_unexpected_status_before_decoding`, `successful_consumer_responses_decode_only_required_payloads`, `extensions.rs:synchronous_extension_status_matrix`, `async_extension_status_and_malformed_matrix`, `void_extension_success_ignores_unread_payload`, and `heartbeat_and_ack_success_have_no_malformed_payload`. These execute the per-operation differences rather than assuming a common SUCCESS/error policy.

## Reverse public type/configuration audit

- `SendKind` maps Java smart-send/reply/batch request selection; `SendMode` maps CommunicationMode SYNC/ASYNC/ONEWAY. `SendStatus` and `SendResult` map Java send outcomes, identifiers, queue, transaction/recall, region and trace metadata. Caller-supplied unique IDs and batch bytes come from the later producer layer.
- `PullStatus` and `PullResultExt` map Java pull outcomes, next/min/max offsets, suggested broker, offset delta and raw message bytes. The existing header's lite sysflag controls code 361; no Rust-only Lite setting was introduced.
- `HeartbeatV2Result` maps Java version/sub-change/support fields. `PopStatus` maps the Java enum; `PopReply<H>` is the Phase 5 raw-body/header representation before message decoding. `AckStatus` and `ChangeInvisibleTimeResult` represent Java AckResult metadata before consumer extra-info attachment.
- `RpcError` preserves typed transport/protocol sources and structured response request code/peer/raw code/remark. Malformed header/body errors remain structured. `Header` and `Unsupported` are error taxonomy variants; all scoped RPC families are implemented, so there is no invented unsupported-operation branch. Java sync exceptional channel completion uses the inherited SendRequest wrapper; core does not invent a wire-close cause.
- Public modules are namespace organization. There is no core runtime singleton, callback executor setting, retry count setting or separate transport abstraction. Transport configuration/lifecycle remains with the established RemotingClient, with absolute-deadline sync overload representations documented in ADR-0003.
- Known typed inputs reuse Phase 4 headers/bodies. `NotificationRequestHeader` and `NotificationResponseHeader` are used by the public `MQClientAPIExt.notification` and `notificationWithPollingStats` methods; Phase 18 adds their typed RPC and `NotifyResult` DTO mapping. `PollingInfoRequestHeader` and `PollingInfoResponseHeader` have no outbound client callsite in pinned 5.5.1 and remain wire models only. Public admin/runtime/consumer inventory remains owned by its later phase.

## Typed command-header trait boundary

Rust `rocketmq_classic_protocol::header::CommandHeader` packages typed header
encoding and decoding as `encode_fields` / `decode_fields`. Its Java counterpart
is `CommandCustomHeader` together with `RemotingCommand`'s reflective
ext-field conversion: the Java interface itself only declares `checkFields`,
while Rust keeps conversion in the trait contract and validates required or
malformed fields in each decoder. This is the same typed RPC header boundary,
not an additional client operation. `tests/header_inventory.rs` pins the set of
basic Rust implementations against the Java fixture, and `tests/headers.rs`
checks their field, default, and malformed-value behavior.

## Deadline and pending audit (P5-V03)

`rpc.rs:broker_methods_use_one_attempt_and_honor_deadline` covers every Broker method, all send modes/variants, ordinary and Lite pull sync/future, and oneway update/unlock/end transaction. A silent peer checks that no second frame or connection occurs. The transport's source-confirmed GO_AWAY reconnect is an existing Phase 3 transport recovery under the original deadline, not producer retry policy.

NameServer cases cover no configured address, unavailable-first failover, successful second peer, selected-peer error context, exhausted deadline and a stalled first peer that must not trigger another attempt. `rpc_modes.rs` distinguishes sync wait deadline from future cleanup grace for send, pull, ACK and route. ADR-0003 keeps the absolute deadline through selection/recovery.

`expired_oneway_variants_emit_no_request`, `oneway_offset_returns_without_reply_and_expired_deadline_emits_nothing` and `unsolicited_oneway_reply_without_pending_does_not_poison_next_request` cover facade paths. Lower-layer tests `oneway_never_registers_pending_even_when_peer_replies` and `late_oneway_response_does_not_complete_concurrent_pending_request` inspect actual pending ownership and concurrent reply correlation.

Pending source audit: registration has a generation-scoped PendingGuard; response, scanner, explicit close/shutdown and cancellation remove only that generation. Sync wait expiry transfers admission to the still-live pending entry; it remains held until late response/scanner/channel termination. `sync_wait_timeout_retains_pending_admission_until_terminal_cleanup` checks transfer and exactly-once release. `wire_response_timeout_cancellation_and_shutdown_race_leaves_no_pending`, `pending_terminal_race_and_opaque_wrap_never_replace_live_entries`, `cancelled_invoke_guard_removes_only_its_pending_generation`, `response_timeout_and_shutdown_race_has_one_terminal_completion` and `pending_calls_complete_on_shutdown_and_owned_tasks_join` cover terminal races and owned task cleanup. User callbacks remain in the established transport and execute outside shared locks.

## Fixture provenance and phase gates

148 persisted invoked cases are checked by Maven verify and scripted Rust peers: rpc-invoked (16), consumer (9), consumer-more (41), producer (9), producer-more (22), ext (3), ext-more (19), pop-ack (29). `P5_INVOKED_SHA256SUMS.txt` records the complete corpus. Standalone `fixtures/rpc` frames are supplementary; they are not used as a substitute for invocation evidence.

The test evidence above closes P5-V01, P5-V03 and P5-U01 for the Phase 5 one-attempt raw-message boundary. Error mapping is additionally exercised by `rpc_errors.rs` scripted connect/close/malformed-frame/source and lifecycle/configuration checks. See `PHASE_5_HANDOFF.md` for final commands/results and downstream ownership. In Phase 19, `P5-V02-route`, `P5-V02-producer`, and `P5-V02-consumer` have live evidence; `P5-V02-extensions` remains pending. The pinned server's unsupported Lite subscription request prevents this attempt from establishing the required successful extension path. This does not close the P5-V02 parent or establish overall Broker compatibility or release acceptance.
