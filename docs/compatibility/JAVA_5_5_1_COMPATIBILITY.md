# Java 5.5.1 compatibility evidence

Current source and validation scope as of 2026-10-27 are summarized in
[CURRENT_STATUS.md](CURRENT_STATUS.md). Phase histories and fixture inventories
below retain the state recorded for their named revisions; they do not imply
that the full live matrix was rerun after source revision `7e0e9955`. Older
`PENDING_PHASE_19` cells are dated snapshots; where the later aggregate
recorded a pass, it supersedes those pending labels for revision `413f735a`.

The pinned `Message`, `MessageExt`, and `MessageClientExt` public-method and
virtual-subtype crosswalk is recorded in
[PHASE_18_MESSAGE_SUBTYPE_API_CROSSWALK.md](PHASE_18_MESSAGE_SUBTYPE_API_CROSSWALK.md).
It inventories 79 declared public instance methods against the Rust API and
the invoked Java/Rust direct-consume evidence. The broad P18-V01 audit was
closed on its recorded revision; the later R01–R09 remediation and current
validation limits are listed in [CURRENT_STATUS.md](CURRENT_STATUS.md).

## Historical Phase 19 live status — 2026-10-25

Thirty-five of 37 registered local scenarios have passing Java 5.5.1/Rust
evidence against the pinned 5.3.4 image. Four consumer cases cover reciprocal
offset updates and queries, a found pull with message-body comparison, and an
empty pull; `P5-V02-route` covers matching results for an existing and missing
topic; `ORDER-001-lock-unlock` compares queue acquisition, cross-client
contention, and unlock release; `REBAL-001-average-10q-3c` compares Java/Rust
average allocation of ten live queues across three consumers on the two-Broker
profile. `TX-001-commit` and `TX-002-rollback` compare Java/Rust transaction
callbacks, Broker queue offsets, and consumer pulls; committed message bodies
are visible and rolled-back messages remain absent. `P0-V03` reruns the Phase
0 route, producer, consumer, heartbeat, offset, and lock constituents in fresh
projects and records their clean evidence. `P7-V02` compares Java/Rust high-level sync, async,
oneway, explicit-queue, selector, valid-batch, and invalid-batch send outcomes
on a host-routed Broker. `P6-V04` changes the live push-consumer tag filter,
checks that the Broker suppresses a later nonmatching message, unregisters one
producer group, and confirms the other producer and consumer remain active.
`P9-V03` compares explicit pull results, all four pull statuses, suggested
broker selection, offset-boundary RPCs, and persisted offset resume after
restart. `REQ-001-request-reply-success` also passes the two cross-language
request/reply directions with matching correlation and reply metadata.
`P15-V03` verifies both request/reply directions on two Brokers, both clients'
no-responder timeouts, and late replies after timeout with one callback and no
retained correlation.
`P7-V03` verifies both clients retry across two Brokers after retryable
`NO_PERMISSION` responses, preserve the body and unique message ID, and then
time out at 900 ms against a delayed real Broker response.
`P10-V03` verifies Java-produced messages through Rust Push success, partial
acknowledgement, retry callbacks at counts 0/1, and the configured 5.3.4 DLQ
transition with the original body/key. The terminal DLQ copy records
`reconsumeTimes=2` when the group's retry maximum is 1.
`P10-V05` is registered as a permanent host-routed scenario for Push Pull dispatch
fan-out on 16 queues. Its focused live run passed at clean revision `6d6db7ed`
on 2026-11-14 against the 5.3.4 Broker, including comparison and cleanup; see the
[re-review log](../../rlogs/2026/11/02b-push-pull-fanout-ordering-cleanup__20261114T004734Z.rlog). Local evidence
covers the same assertions against scripted peers: every assigned queue admits one
Pull before any of those responses completes, each queue keeps one active Pull at
`suspendTimeoutMillis=15000`, and the removed four-worker pool fails the same
barrier with only four queues in flight. `tests/differential/check_p10_fanout.py`
validates the Java/Rust probe evidence format and passes its synthetic self-test.
`P11-V03` verifies mixed Java/Rust Push group join and leave: the clients
settled to disjoint ownership of eight scenario queues, then Rust recovered all
eight after Java shut down. One overlapping snapshot during convergence was
allowed; four stable disjoint snapshots were required before the joined state
passed.
`P11-V04` verifies both public producer-to-Push directions across the two
Brokers. Each listener observed retry counts 0 and 1; Java and Rust offset
queries matched at 1 on both Brokers. Three Push start/shutdown cycles per
client completed with the Java scheduled executors terminated and Rust runtime
and remoting resources quiesced.
`P16-V02` passes Java/Rust ACL signature acceptance and rejection, plus mutual
TLS trust, hostname, and client-certificate outcomes. The ACL Broker uses the
SHA-512-verified Apache 5.3.4 binary distribution in an amd64 JDK container;
the NameServer remains the pinned 5.3.4 image. See P16-V02 live evidence (raw output not retained).
`P12-V03` passes the mixed Java/Rust orderly handoff: Rust acquired each
transferred Broker lock, advanced both queue offsets to 2 before Java's held
callbacks returned, and no stale completion regressed either offset. Both
clients shut down and each Broker queue became lockable and releasable.
Evidence: P12-V03 live run (raw output not retained).
`P5-V02-producer` matches raw V1, V2, batch, one-way reply V1/V2, and
end-transaction RPC outcomes.
`P5-V02-consumer` matches raw pull/Lite pull, offset update/query,
consumer-list, send-back, and lock/unlock outcomes on the two-Broker profile.
`P5-V02-extensions` matches heartbeat, heartbeat V2, assignment, request-mode,
standard POP, ACK, batch ACK, and visibility-change outcomes. Broker 5.3.4
returns its source-confirmed unsupported-request response for Lite subscription
and Lite POP; this records the matched 5.3.4 rejection path. A separate clean
run on official RocketMQ 5.5.0 image digest
`sha256:7e8f6c9dbd9df742ed26ba69c00d4ad69e2f86a56f3ca7782ff8144dd0798132`
matched Java/Rust Lite subscription and Lite POP success, including the seeded
message body. This supplementary result does not alter the 5.3.4 manifest row
or establish production support; evidence is under
`supplementary-lite-5.5.0/20261025T135033Z` (raw output not retained).
`PROD-005-send-timeout` delays the Broker responses beyond 100 ms, confirms
both clients report timeout, and verifies each message is committed once by
matching Java/Rust pull results. `P3-V04` compares raw NameServer routing, CA
trust, and client shutdown through a local TLS-terminating proxy. Java 5.5.1
and Rust accept a CA-trusted wrong-host certificate because the Java TLS
handler does not receive a peer hostname; Rust verified mode now matches this
behavior while still rejecting an untrusted CA. P7-V04 now has current
clean-source live evidence for 8,192 fixed-seed variable-size sends per client,
with matching payload bytes/checksum and contiguous Broker offsets. Three
additional shutdown cycles returned pending requests, connections, and owned
task groups to zero
(evidence (raw output not retained)).
`P14-V03` passes cross-language commit visibility, rollback invisibility with
unchanged normal offsets, and bounded Unknown resolution through actual Broker
checks in both directions. `P17-V02` passes duplicate-key and bounded-time
queries, preserves Broker A results when Broker B fails, and maps terminal
query failures to `NO_MESSAGE`; see the
live evidence (raw output not retained).
All 37 registered Phase 19 live scenarios have passing manifest evidence,
including corrected clean-source runs for P3-V04, P6-V04, and P16-V02. The
latest `run-local.sh --all` replay passed all 23 gates at workspace revision
`413f735ad0ab41717acb9d76e2cb664b42076f4d`; evidence reconciliation verified
all 37 manifest rows have current clean-source PASS results and exit status 0
(aggregate evidence (raw output not retained)). The Phase
18 source/API/settings reverse audit and current-tree delta review are recorded.
MAN-01 through MAN-04 remain blocked on owner inputs.
`P13-V02` and `P5-V02-extensions` now have clean Java/Rust evidence; the
latter records the two unsupported Lite request paths on 5.3.4 and separate
positive Lite success evidence on 5.5.0. These results do not establish owner
environment behavior or production approval. See
[`SERVER_5_3_4_RESULTS.md`](SERVER_5_3_4_RESULTS.md) for the executed server
cases and [`../owner-validation.md`](../owner-validation.md) for the blocked
owner inputs. Historical GitHub Actions run `40383732405` ran zero steps because
GitHub reported an account payment/spending-limit issue; its captured record is
preserved at
`CI run evidence` (raw output not retained).
Per the user's 2026-10-27 direction, the repository GitHub Actions CI workflow
has been removed and remote CI is no longer a validation requirement. Final
compatibility and release approval remain blocked only on MAN-01 through
MAN-04 owner inputs.

P13 follow-up: run `20261025T124657Z` initialized and verified offset 0 on
both queues for all six groups after seeding. The separate Java consume probe
observed offset 1 before its first POP and again returned
`POLLING_NOT_FOUND`; the exact transition after reset remains unlocalized.
All ten P13 diagnostic runs were cleaned up. At that point, P13-V02 remained pending; run `20261025T130156Z` below records the later pass.

Phase 18 Fastjson U+001A end-of-input continuation (`12de2474`, 2026-10-24):
the pinned Java 5.5.1 heartbeat DTO oracle accepts 108 new inputs with control
or Unicode characters after U+001A and between repeated markers, and two more
with ordinary text or a second JSON object after the marker. Fastjson treats
U+001A as end-of-input and ignores its entire suffix. Rust now matches this
while retaining raw U+001A characters inside strings. The expanded 1,221-case
heartbeat DTO differential and focused parser regression pass. Fixture
SHA-256: `3841442edc031bc70a028f0c2476f8b77f9ead0ec2eb959ace5a1ab2bad3f9c6`.
This closes the tested terminator boundary only; broad P18-V01 remains open.

Phase 18 Fastjson numeric-token continuation (2026-10-24): the pinned Java
5.5.1 heartbeat DTO oracle accepts uppercase numeric suffixes `L`, `F`, `D`,
`B`, and `S` in integer fields, exponent-plus-suffix values, and leading or
trailing decimal points; lowercase `1f` rejects. Four additional cases cover
suffix coercion to the `SubscriptionData.topic` string field, including Java
Float rounding, Float/Double exponent strings, and Long string conversion. The
native normalizer matches those observed forms and integer-field results. The
expanded 1,102-case heartbeat DTO differential and focused parser regressions
pass. The fixture SHA-256 is recorded in the heartbeat audit. This closes the
tested numeric token forms only; broader Fastjson and P18-V01 reconciliation
remain open.

Phase 18 Fastjson leading-dot and incomplete-exponent continuation (`9333cbc2`,
2026-10-24): the pinned Java 5.5.1 heartbeat DTO oracle accepts unsigned `.5`
and `.5F` for the integer `subVersion` field, but rejects those tokens for the
String `topic` field. An explicit `+.5` remains accepted for `topic`. Fastjson
also accepts incomplete exponent markers such as `1e+`, `1e+F`, and `1.E`,
coercing them to the numeric prefix with the observed Java String formatting.
Rust now preserves leading-dot token provenance through DTO decoding, keeps
general `serde_json::Value` parsing numeric, and maps the tested incomplete
exponents. Nine new Java cases expand the heartbeat DTO corpus to 1,111 cases.
Fixture SHA-256:
`0e32da4ec9a0c121617bb8f1684e2f9ddc9a568bfe992eb3ce3be6aa8c1b3e76`. This
closes the tested numeric token branches only; broad P18-V01 remains open.

Phase 18 Fastjson trailing EOF-marker continuation (2026-10-24): the pinned
Java 5.5.1 heartbeat DTO oracle accepts repeated U+001A document terminators,
with or without intervening JSON whitespace. Two new invoked cases first
failed the native differential because Rust removed only one terminal marker;
the shared protocol normalizer now removes the complete trailing marker run.
Existing root-prefix/structural rejection cases and raw string controls remain
unchanged. The expanded
1088-case heartbeat DTO differential and focused parser regression pass.
Fixture SHA-256: `2f55c06fd34dabb708de5c5455da71bbcc3ec19f0a5df19b0e9e65e9d18b706b`.
This closes the named grammar boundary only; broad P18-V01 remains open.

Phase 18 `exportRocksDBConfigToJson` (2026-10-21): Rust maps pinned
`MQClientAPIImpl.exportRocksDBConfigToJson(String,List,long)` request 355 and
the `ExportRocksDBConfigToJsonRequestHeader`/`ConfigType` APIs. Enum parsing
preserves Java's case-insensitive trimmed names and semicolon serialization,
including the trailing delimiter. The direct Broker address and caller timeout
are retained; empty lists send an empty `configType`, and null lists fail
locally before transport. Invoked Java and Rust fixtures cover request fields,
typed header encoding, parsing, success, and Broker rejection. Fixture digest
and gate results are in the Phase 18 release checklist. This closes this
operation/header slice only; broad P18-V01 remains open.

Phase 18 NameServer write-permission APIs (2026-10-21): Rust maps pinned
`MQClientAPIImpl.wipeWritePermOfBroker(String,String,long)` request 205 and
`addWritePermOfBroker(String,String,long)` request 327. Both send the nullable
`brokerName` header to the supplied NameServer address with the caller timeout,
return the boxed topic count on success, and preserve non-success response
errors. Missing or malformed success counts are nullable during header decode
and fail at the Rust RPC result boundary, matching Java's unboxing failure.
Java and Rust fixtures cover request/response headers, nullable and empty names,
signed counts, errors and missing/malformed response fields. Fixture digest and
validation results are in the Phase 18 release checklist. This closes these
direct APIs and header rows only; broad P18-V01 remains open.

Phase 18 `resetMasterFlushOffset` (2026-10-21): the pinned direct
`MQClientAPIImpl.resetMasterFlushOffset(String,long)` operation maps request
908 with `masterFlushOffset`, uses the Broker address unchanged even with VIP
mode enabled, and uses a fixed 3000 ms timeout. Rust exposes
`MQClientApi::reset_master_flush_offset` with the nullable
`ResetMasterFlushOffsetHeader` model; missing and malformed boxed `Long`
fields decode to null, matching Java reflection. Invoked Java and Rust cases
cover `Long.MAX_VALUE`, a negative offset, exact request fields, the fixed
timeout, success, and Broker rejection. The fixture digest and complete
validation record appear in the Phase 18 release checklist. This closes these
direct API/header rows only; broad P18-V01 remains open.

Phase 18 `getClusterList` stub (2026-10-21): pinned
`MQClientAPIImpl.getClusterList(String,long)` ignores its topic and timeout,
issues no RPC, and always returns Java's shared immutable
`Collections.EMPTY_SET`. `MQClientApi::get_cluster_list` exposes a shared
immutable empty set with the same no-request behavior. The invoked Java fixture
checks nonempty, null, and empty topics, singleton identity, and mutation
failure; the Rust fixture-backed regression checks each result and verifies no
request is sent. Producer core fixture SHA-256 and gate evidence appear in the
Phase 18 release checklist. This closes this stub only; broad P18-V01 remains
open.

Phase 18 direct subscription-group configuration lookup (2026-10-21): Rust
maps `MQClientAPIImpl.getSubscriptionGroupConfig(String,String,long)` to request
352. The group header accepts null and empty values, preserves inherited
namespace/broker/oneway fields, and carries its own Java header fixture. The
RPC applies configured VIP conversion, decodes a nullable
`SubscriptionGroupConfig` body, and returns Broker response errors. Invoked
Java cases and Rust scripted peers cover populated/default bodies, null and
empty groups, a missing body, Broker rejection, and the exact request fields;
a VIP-enabled loopback verifies the configured address transform. Current
Producer core fixture SHA-256 and validation details appear in the Phase 18
release checklist. This closes this lookup and header slice only; broad P18-V01
remains open.

Phase 18 `getLiteGroupInfo` RPC and response DTO (2026-10-21): pinned
`MQClientAPIImpl.getLiteGroupInfo(String,String,String,int,long)` sends request
200078 with nullable `group` and `liteTopic`; `topK` defaults to 0 and
`checkFields()` accepts negative values. Rust invokes it synchronously with
the configured VIP transform and caller deadline. The nullable
`GetLiteGroupInfoResponseBody` preserves nullable strings, an aliased
`OffsetWrapper`, nullable lag lists and nullable list elements, and Java's
primitive defaults. `LiteLagInfo.consumerOffset` defaults to -1 when decoded
but Fastjson omits it when serialized. Seven invoked Java cases pin the
RPC/header/DTO surface and cover populated/default data, nullable inputs and
list elements, null/malformed bodies, and Broker errors. The Rust differential
checks the request and decoded results; a VIP loopback verifies address
conversion. Success fixture SHA-256:
`da245ed2dece655a09e8f05ab70fec33c513dbc194b35aab12eb35ed74ef608d`. This
closes this RPC/DTO slice only; broad P18-V01 remains open.

Phase 18 Broker maintenance RPCs (2026-10-21): Rust maps the pinned public
`MQClientAPIImpl.cleanExpiredConsumeQueue`, `deleteExpiredCommitLog`, and
`cleanUnusedTopicByAddr` methods to requests 306, 329, and 316. Each operation
sends an empty synchronous request through the configured VIP address
transform, returns `true` for `SUCCESS`, and preserves Broker response errors.
The invoked Java fixture records all three signatures and success/error
requests; the Rust differential checks request codes, Java language, empty
headers/body, and outcomes. A configured-VIP loopback checks the port offset.
Producer core fixture SHA-256 is recorded in the Phase 18 release checklist.
This closes these direct RPCs only; broad P18-V01 remains open.

Phase 18 direct `createSubscriptionGroup` RPC (2026-10-21): Rust now maps the
pinned `MQClientAPIImpl.createSubscriptionGroup(String, SubscriptionGroupConfig,
long)` operation on request 200. It sends the Java `SubscriptionGroupConfig`
encoding, treats a null config as an absent body, applies the configured VIP
address transform, and returns success or a typed response error. The
asynchronous `MqClientAdmin.updateOrCreateSubscriptionGroup` path remains a
separate API with its own address behavior. Invoked Java cases cover populated,
default, null-config and Broker-error inputs; the Rust differential compares
the exact signature, request envelope/body and outcomes, and a VIP loopback
checks the address transform. Producer core fixture SHA-256:
`aaab68664cf25ff86c9b19a48c27e4691596b31124123fdbd83f7ceb93f89b57`. This
closes the named direct RPC only; broad P18-V01 remains open.

Phase 18 direct NameServer KV configuration APIs (2026-10-21): Rust maps
`MQClientAPIImpl.getKVConfigValue`, `putKVConfigValue`,
`deleteKVConfigValue`, and `getKVListByNamespace` for request codes 101, 100,
102, and 219. Nullable request fields follow Java's no-op header validation;
get operations select a NameServer, while put/delete broadcast in configured
list order and return the last non-success response after contacting every
server. Missing `value` and list body remain null. The invoked Java fixture and
Rust loopback differential cover success, absent values/body, Broker errors,
broadcast ordering, last-error selection, and empty server lists. Producer
core fixture SHA-256: `7e6aecbca17b9711ed3276de4aae2c500fdd8f90bdeb1bde7a7c59b0d3c402af`.
This closes those low-level KV operations only; broad P18-V01 remains open.

Phase 18 cluster-topic header inventory follow-up (2026-10-21): the already
implemented `GetTopicsByClusterRequestHeader` now appears in the exhaustive
Rust inventory and field-validation matrix, with its Java constructor and
method inventory recorded beside the query-admin decode fixture. Request 224
is included in the typed Rust-to-Java frame set, bringing Maven's exact header
count to 68; all 68 frames decode in Java. This closes the inventory and
reverse-frame evidence gap only; broad P18-V01 remains open.

Phase 18 Broker and Producer diagnostics RPCs (`GET_BROKER_RUNTIME_INFO` 28,
`GET_PRODUCER_CONNECTION_LIST` 204, and `GET_ALL_PRODUCER_INFO` 328): Rust maps
the pinned `MQClientAPIImpl.getBrokerRuntimeInfo`,
`getProducerConnectionList`, and `getAllProducerInfo` synchronous operations.
All three apply Java's configured Broker VIP address transformation and retain
the supplied Broker address in typed response errors. The producer-group
header accepts null because the pinned header's `checkFields()` is a no-op;
null is omitted from the wire fields. Added `KvTable`, `ProducerConnection`,
`ProducerTableInfo`, and `ProducerInfo` response models, including default and
explicit-null table/set behavior, shared map/set aliases, scalar accessors,
and `ProducerInfo.toString()` formatting/null-language failure behavior.
Invoked Java fixtures cover exact method signatures, request envelopes,
populated and absent bodies, Broker errors, null producer group, DTO APIs and
body values. Rust scripted peers compare all three response DTOs and outcomes;
a VIP-enabled loopback checks all request codes through the two-port transform.
Producer core fixture SHA-256 is recorded in the release checklist. This closes
only these three RPC/DTO slices; broad P18-V01 remains open.

Phase 18 Lite topic-info request-header fixture closure (`d578db50`): the
`GetLiteTopicInfoRequestHeader` API, nullable default fields, no-op
`checkFields`, typed Java frame, and Rust reverse frame are now covered in the
header inventory and validation matrix. The Maven gate requires exactly 68
typed-header frames and passes with 15 typed-body frames. Rust frame SHA-256:
`92e15800cdfb810908a8a016582189b5523bdc2d7dbbfc8ff69523f2a7184884`. This
closes only the Lite topic-info header fixture slice; broad P18-V01
reconciliation remains open.

Phase 18 Producer permission validation and Broker config RPC (2026-10-21):
the pinned `Validators` and `PermName` inventories now include all seven
validator methods, eight permission constants and methods, and valid, invalid,
Arabic-digit, malformed and overflow inputs. Rust maps the permission helpers,
formats invalid permission errors with Java's code-16 `MQClientException`
message, and preserves `NumberFormatException` text for malformed strings.
`MQClientApi::create_topic` validates a supplied permission before network I/O;
`update_broker_config` maps the empty-properties no-op, UTF-8 properties body,
VIP address offset and Broker error path. Producer fixture SHA-256:
`8216bf9c1a66b57a61e88af448f61ab38dc84015ffd872ee340d68896d3ec9b5`. These
bounded API/operation paths are covered locally; broad P18-V01 and the final
current-revision P18-V02 refresh remain open.

Phase 18 Broker config read RPC (2026-10-21): the pinned public
`MQClientAPIImpl.getBrokerConfig(String,long)` method is mapped to
`MQClientApi::get_broker_config`. Rust sends request 26 without a header or
body, uses the supplied Broker address unchanged, and parses the response with
the Java `Properties.load(InputStream)` character and escape rules after
RocketMQ's UTF-8 decode/re-encode step. The fixture covers empty Properties,
escaped separators, Unicode escapes, malformed escapes, raw non-ASCII text,
and a final backslash. A missing response body produces Rust's structured
malformed-response error; Java dereferences the missing body before parsing.
At this read-operation checkpoint the producer core fixture SHA-256 was
`53f8d3ae387f5df155d0c10484815f3a71d00debafb70c82ed0912f3848bca2f`. The
focused Core RPC/parser tests and Producer fixture test pass; broad P18-V01 and
the final current-revision P18-V02 refresh remain open.

Phase 18 adjacent cold-data flow and CommitLog read-ahead RPCs (2026-10-21):
the four pinned `MQClientAPIImpl` methods now map to Core request codes
2001–2004. Cold-data flow updates serialize non-empty Properties and use the
configured VIP address; empty Properties and null/empty group removal inputs
produce no request. Cold-data status reads the supplied address and returns
`None` for an absent or empty body. Read-ahead mode uses the
`READ_AHEAD_MODE` extension field, returns only a non-empty Broker remark, and
omits Java's null field value on the wire. Scripted tests compare success,
nullable results, Broker errors and request fields; a VIP-enabled loopback test
checks both mutation operations. Current producer core fixture SHA-256 is
`c20f4474d674bd4e0001f19c82fed8c5e2dfbe6f5c911ee6eade0fb1842c7cb0` at this
slice checkpoint. The subsequent `registerClient` addition advances the
fixture SHA-256 at the registration checkpoint to
`434a3fe8b1cba71732f41ccb710e601e0d8ce4cbcf196265695cef7641b90905`; the
later NameServer topic-list fixture advances the current digest to
`6e0a6f94f934f9e173aa76a5e4e3ab18c70ee1121f41f1dfbb52fbbac303a2ef`. The
tools facade remains outside Phase 17; the Core low-level operation mappings
do not claim that facade's methods or command-line tools.

Phase 18 `registerClient` heartbeat result contract (2026-10-21): the pinned
public `MQClientAPIImpl.registerClient(String, HeartbeatData, long)` operation
shares request 34 and the heartbeat body with `sendHeartbeat`, but synchronously
uses the supplied address and returns a boolean for response-code equality
with `SUCCESS`; other Broker response codes return `false`. Rust now exposes
`MQClientApi::register_client` with that result behavior, separate from
`send_heartbeat`'s Broker-version/error contract. The direct Java fixture and
Rust scripted RPC compare the exact signature, timeout, body, and success and
non-success results. At this slice checkpoint, the producer core fixture
SHA-256 was `434a3fe8b1cba71732f41ccb710e601e0d8ce4cbcf196265695cef7641b90905`.

Phase 18 NameServer topic-list RPC (2026-10-21): Rust now maps the pinned
`MQClientAPIImpl.getTopicListFromNameServer(long)` request code 206 through
NameServer selection, with no header or body, and decodes the returned
`TopicList`. The Java direct invocation fixture and Rust scripted NameServer
case compare the signature, request envelope, topic set, Broker address,
response code and remark. Rust maps a successful reply without a body to a
structured `MalformedResponse`; Java throws `MQClientException` with response
code 0. At that slice checkpoint, producer core fixture SHA-256 was
`6e0a6f94f934f9e173aa76a5e4e3ab18c70ee1121f41f1dfbb52fbbac303a2ef`. The
Broker system-topic-list additions advanced that
checkpoint to `c81645968935a1bb317952c0ea32c7bbf4a029e62fa879af1863a06a40d70f28`;
the added missing-body outcome advanced that RPC slice checkpoint to
`fbe6f70615e8004c26bdba90fdc824241415d19557b57853a6a563f24becbd32`.
This closes the direct NameServer RPC only; the composite
`getSystemTopicList` operation and broad P18-V01 remain open, as do the final
P18-V02 refresh and later gates.

Phase 18 Broker system-topic-list RPC (2026-10-21): Rust now maps the pinned
`MQClientAPIImpl.getSystemTopicListFromBroker(String,long)` request code 305,
including the configured VIP address transform, no-header/no-body envelope,
`TopicList` decoding and Broker response errors. The direct Java invocation
fixture and Rust scripted RPC compare the exact signature, request fields,
topic set, Broker error and `SUCCESS` without a body. Java reports
`MQClientException` with response code 0 for the missing body; Rust returns a
structured `MalformedResponse`. At this direct-RPC checkpoint, producer core
fixture SHA-256 was `fbe6f70615e8004c26bdba90fdc824241415d19557b57853a6a563f24becbd32`.

Phase 18 composite system-topic-list RPC (2026-10-21): Rust now maps pinned
`MQClientAPIImpl.getSystemTopicList(long)`, sending request 304 through the
NameServer selector and conditionally querying request 305 from the Broker.
It preserves the NameServer topic set, merges nonempty Broker topics, and
skips the Broker call for empty/null sets or blank addresses. The invoked Java
fixture and Rust differential compare the exact method signature, request
sequence, success/error bodies, merged set and short-circuit behavior. Producer
core fixture SHA-256 at this composite slice checkpoint:
`38b2eac88f25aa04674f469b4e7a685dc599834969f6a1d24613de8c98efdb23`. The
composite system-topic-list method is closed; broad P18-V01 and later gates
remain open.

Phase 18 batched subscription-group creation (2026-10-21): Rust now maps
pinned `MQClientAPIImpl.createSubscriptionGroupList(String,List,long)`,
sending request 225 with a Java `SubscriptionGroupList` body through the
configured VIP address. The invoked Java fixture and Rust scripted RPC compare
the exact serialized body, timeout, success/error results and empty-list case;
a VIP-enabled loopback verifies the port transform. Current producer core
fixture SHA-256:
`2479e9af49895298bf807b04775db0e3acef6ad2b6a0a6e2cf59a830ca89169e`. The
composite method is closed; broader P18-V01 and later gates remain open.

Phase 18 cluster topic-list RPC (2026-10-21): Rust now maps pinned
`MQClientAPIImpl.getTopicsByCluster(String,long)`, sending request 224 through
NameServer selection with the cluster request-header field and decoding
`TopicList`. The invoked Java fixture and Rust differential compare the method
signature, request envelope, returned topics, response error and missing-body
branch. Java raises `MQClientException` with response code 0 for a successful
reply without a body; Rust returns structured `MalformedResponse`. The Java
header's no-op validation accepts a null cluster, which the nullable Rust API
omits from the wire fields. Producer core fixture SHA-256 at this slice
checkpoint: `ac3462b7351f327839c4a79db5a8a499f6fdbbd56f667f74b2ac5f18fb368fee`.
This closes that RPC method only; broader P18-V01 and later gates remain open.

Phase 18 batched topic creation RPC (2026-10-21): Rust now maps pinned
`MQClientAPIImpl.createTopicList(String,List,long)`, sending request 18 with a
Java-compatible `CreateTopicListRequestBody` through the configured VIP
address. The fixture and Rust differential compare exact body bytes for
populated/default/custom and empty lists, Java's omitted null list and encoded
null elements, plus success and Broker-error outcomes. A VIP-enabled loopback
checks the two-port transform. Producer core fixture SHA-256 at this slice
checkpoint: `fcf27463aa5fde04867de50543847ad53dabc1b5c3b526ae8ac2406481514ec7`.
This closes the batch topic RPC only; broader P18-V01 and later gates remain
open.

Phase 18 batched topic and subscription-group deletion RPCs (2026-10-21): Rust
now maps pinned `deleteTopicInBrokerList(String,List,long)` request 5002 and
`deleteSubscriptionGroupList(String,List,boolean,long)` request 5003, with
Java-compatible body DTOs, VIP address transforms and Broker error handling.
Invoked Java fixtures and Rust differentials compare exact request bytes for
populated, empty, null-list and null-element cases; group deletion also checks
both `cleanOffset` values. A VIP-enabled loopback verifies both operations.
Producer core fixture SHA-256 at this slice checkpoint:
`33b7788c15092caaeb2e5cc42ac69eb7bcda5baf5ebc27c2caf01aac9c5e9ff3`. These
RPC methods are closed; broader P18-V01 and later gates remain open.

Phase 18 unit topic-list RPCs (2026-10-21): Rust now maps pinned
`MQClientAPIImpl.getUnitTopicList(boolean,long)`,
`getHasUnitSubTopicList(boolean,long)`, and
`getHasUnitSubUnUnitTopicList(boolean,long)`, using NameServer requests 311,
312 and 313 with no header or body. The boolean is evaluated locally after
decoding: Java preserves `%RETRY%` topics when `isUnitMode` is true and removes
them when false. The invoked Java fixture and Rust differential compare all
three signatures and request envelopes, both filtering modes, response errors
and missing bodies. Java throws `MQClientException` code 0 for a successful
reply without a body; Rust returns structured `MalformedResponse`. Producer
core fixture SHA-256 at this slice checkpoint:
`bb5a19cb131f922828ff5effb8eb52ac4a0d95e8bae2863775214da4d6664e5b`. These
RPCs are closed; broader P18-V01 and later gates remain open.

Phase 18 NameServer configuration RPCs (2026-10-21): Rust maps pinned
`MQClientAPIImpl.updateNameServerConfig(Properties,List,long)` request 318 and
`getNameServerConfig(List,long)` request 319. Updates send UTF-8 Properties to
all explicit or configured NameServers in order, skip empty Properties, and
return the last non-success response after fanout. Reads query each server in
order, return address-keyed Properties (including null values for malformed
content), and stop on the first response error. Missing/empty server lists use
the configured list; no selected servers yields a no-op update or null read.
Invoked Java fixtures and Rust differentials compare signatures, request
envelopes, property body values, per-server results, ordering and errors.
Java throws `NullPointerException` for a successful read response without a
body; Rust returns structured `MalformedResponse`. Producer core fixture
SHA-256 at this slice checkpoint:
`4276123552ab57632da4a10177b1b529266707234e95785668cfcd12177e783c`. These
RPC mappings are closed; broader P18-V01 and later gates remain open.

Phase 19 live baseline checkpoint (2026-10-17): seven of 37 live cases pass
against the pinned RocketMQ 5.3.4 Broker: existing-topic route lookup,
missing-topic `NO_ROUTE`, heartbeat request/response parity, synchronous send,
asynchronous send with one Java callback / Rust Future completion, and oneway
send with both keyed messages read back by the other language, plus batch send
with both messages read by physical offset from the other language. All four
producer cases include bidirectional Java/Rust message readback. Their
server-side evidence and the remaining local coverage are tracked in
[SERVER_5_3_4_RESULTS.md](SERVER_5_3_4_RESULTS.md).
This does not close the broad Phase 18 API/settings/flow audit or the other
Phase 19 cases.

## Java 5.5.1 release provenance (rechecked 2026-10-18)

The Java harness pins org.apache.rocketmq:rocketmq-client:5.5.1. Its resolved
RocketMQ dependency tree is rocketmq-client 5.5.1 → rocketmq-remoting 5.5.1 →
rocketmq-common 5.5.1. Each resolved binary JAR's embedded Maven pom.properties
reports the same group, artifact, and version. The corresponding source JARs
and POMs are also present. SHA-256 values for these nine artifacts and
comparisons against Maven Central's published SHA-1 files are recorded in
[PHASE_18_JAVA_PROVENANCE.md](PHASE_18_JAVA_PROVENANCE.md).

The Apache RocketMQ repository's annotated rocketmq-all-5.5.1 tag resolves to
commit efa1be59d6fc747ad220f036c86030f05f0419f9, and that commit object is
present. Four representative Java files from the client, common, and remoting
source JARs byte-match files at that tag: MQClientAPIImpl,
DefaultMQPushConsumerImpl, MessageExt, and LockBatchResponseBody. The external
checkout's current HEAD is not substituted for this release tag. The tag
contains an OpenPGP signature, but local signature verification was unavailable
because this environment has no gpg executable; artifact checksums and tagged
source byte comparisons are the provenance evidence recorded here.

This client baseline remains separate from the Phase 19 RocketMQ Broker 5.3.4
baseline. The Rust workspace metadata review and package-content checks are
summarized in the provenance record. Rust workspace packages use version 5.5.1
to identify the Java client release they port, and remain unpublished. Registry
availability checks remain pre-publication work after Phase 19.

Phase 8 message records and pull-result processing: source/field mappings and P8-V01–V03 evidence are recorded in [PHASE_8_OFFLINE_METHOD_MATRIX.md](PHASE_8_OFFLINE_METHOD_MATRIX.md). Pinned fixture bytes and source hash are in `fixtures/messages/records.json` and `P8_MESSAGE_FIXTURE_SHA256SUMS.txt`; tests cover both versions, host and compression variants, limits and pull post-processing. No live Broker or Filter Server was contacted; those checks remain Phase 19.

Phase 9 pull consumers, offset stores and boundary RPCs: source mappings and P9-V01/V02/U01 evidence are recorded in [PHASE_9_OFFLINE_METHOD_MATRIX.md](PHASE_9_OFFLINE_METHOD_MATRIX.md). Offline pull/offset checks pass, including Java 5.5.1 boundary request codes 29–32 and successful `-1` offsets. P9-V03 live Broker pull/restart remains `PENDING_PHASE_19`.

Phase 18 offset-boundary header follow-up (`9513e4da`, 2026-10-17): the four Phase 9 boundary response headers use Java's reflective decoder even when the fast-encode hint is set. Missing or invalid boxed `Long` fields remain null per field, and the corresponding RPC owner fails on unboxing. The Java 5.5.1 invoked fixture covers both hints, valid/missing/malformed fields, and owner outcomes; its SHA-256 is `c4891f046639eb0f9a82e221b1605d296aed1314fb70e37c22ea514d240a9e95`. This closes the named header/owner paths only; the broad P18-V01 audit remains open.

Phase 18 offset-boundary request-header continuation (2026-10-17): the four Phase 9 request headers now preserve Java's nullable `String`/boxed numeric fields during reflection decoding under either `useFastEncode` hint, including inherited `lo`; `SearchOffsetRequestHeader.liteTopic` is present in the pinned 5.5.1 source, and its getter defaults a null boundary type to `LOWER`. `GetMaxOffsetRequestHeader.committed` remains primitive with its Java `true` default and `Boolean.parseBoolean` conversion. The invoked fixture now compares missing, valid, and malformed request fields for search/max/min/earliest headers in both modes, alongside the response cases. Fixture SHA-256: `f6cf1cc07b0f9c0c3263f7210393595cfb0c8898257dfdd3fc81f39afebe71a3`. The Java source oracle is the exact 5.5.1 release commit `efa1be59d6fc747ad220f036c86030f05f0419f9`; the current external checkout HEAD is older and must not be used as a substitute. This closes only these header decoding cases; broad P18-V01 remains open.

Phase 18 heartbeat request-header API follow-up (2026-10-17): pinned Java `MQClientAPIImpl.sendHeartbeat` and `sendHeartbeatV2` construct `HeartbeatRequestHeader`, which inherits nullable `ns`, `nsd`, `bname`, and `oway` fields plus structural equality/hash and `toString` from `RpcRequestHeader`. Rust now publicly exports and encodes/decodes the same typed header, checks the inherited methods and value semantics against Java, and uses it for both heartbeat RPCs. The typed frame fixture hash is `d52b9cfbb0e640702e0eb6cea45430a237ac7120513854b36f677447fc3cfe46`. This closes the heartbeat-header slice only; the broad P18-V01 API and reverse-export audit remains open. See [ADR-0084](../architecture/ADR-0084-heartbeat-request-header-api.md).

Phase 18 `ChangeInvisibleTimeRequestHeader` decode follow-up (`8f64dde3`, 2026-10-18): pinned Java 5.5.1 source at `efa1be59d6fc747ad220f036c86030f05f0419f9` confirms this header uses reflective decoding for both `useFastEncode` hints, leaves absent or malformed nullable strings/boxed numbers null, defaults primitive `suspend` to false, and performs no required-field check. Rust now represents those nullable fields with `Option` and omits unset fields when encoding. The success callback maps absent `offset`, `topic`, or `queueId` to a typed Rust header error; a non-success reply still returns `NO_EXIST` without reading those request fields. The invoked Java fixture covers both decode modes, missing fields, malformed/overflow numbers, valid Arabic-Indic digits, and encoded fields. Fixture SHA-256: `01b6c6fdbcc09ef278509f92e58e117c025222ca1e99755ff23168b570c95890`. This closes only this header and callback slice; broad P18-V01 remains open.

Automatic client-IP fixture portability (2026-10-12): the Java runtime-config fixture now records `clientConfigAutoIPUsesNetworkUtil` instead of the fixture host's IP and interface list. Rust compares the constructor result through a resolver seam and exercises the real host resolver without assuming another machine has the same addresses. Linux ARM64 production enumeration also passes against the pinned Linux Java `::1%lo` scope fixture (core library: 130 tests). A later same-network-namespace Linux ARM64 Docker check found Java `NetworkUtil.getLocalAddress()` and Rust `ClientConfig::new_auto()` selecting the same normalized IPv6 address; this covers that container topology and selected value only. Windows IP Helper execution, non-macOS BSD enumeration, a real IPv6-only host, and the broader Phase 6 overload/input audit remain open. See the [Phase 6 requirement audit](PHASE_6_REQUIREMENT_AUDIT.md).

Heartbeat DTO Set alias correction (`7e5b3fa6`, 2026-10-10): the pinned Java 5.5.1 API oracle now proves Set and supplied mutable element identity for all five heartbeat collection fields, including both mutation directions. Rust shared handles preserve the same caller/setter/getter aliases; nested Set equality follows Java `SubscriptionData.equals/hashCode` or inherited DTO identity. Wire arrays and null handling are unchanged. Fixture `heartbeat.json` SHA-256: 7c50b6ff17d46cc6659ab69e75a69c56d28a7a187ea198440c093acefa1f39b3; [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md). Workspace all-target tests with local socket access, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting and diff checks pass. Broader Phase 6 source/API/input reconciliation remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.

Consumer-list alias parity (`456849fb`, 2026-10-10): Java retains the assigned List and returns the same object; caller and getter mutations are mutually visible. Its body also inherits `Object.equals`/`hashCode`. Rust `SharedValue` plus `ptr_eq` matches those alias and identity semantics, while the existing core API returns an owned snapshot. Pinned fixture hash: `372a5250f6af3ced4c17ef23eb9e05dbe78523ceb6b75a4cd03fe1589fabf80e`. Full workspace/all-target tests, strict all-feature Clippy, offline Maven verification, comparator/manifest22, formatting, and diff checks pass. Remaining Phase 6 DTO aliases and source/API/input reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.
Lock-batch Set alias parity (`045b46e7`, 2026-10-10): pinned Java retains the assigned Set for both lock request bodies and the lock response body; getter and caller mutations share one container. The direct fixture also confirms all three DTOs inherit identity-based `Object.equals`/`hashCode`. Rust `SharedValue<HashSet<Option<MessageQueue>>>` and `ptr_eq` match the outer Set aliases; the core RPC returns an owned snapshot. Mutable MessageQueue element references inside a Set remain an open alias boundary. Fixture SHA-256 `07fb5f45492598ce7ca844903bd0440bbbaabd7baddb9bdfbbb5c96a5b39a7ad`. Full workspace/all-target tests, strict all-feature Clippy, offline Maven verification, comparator/manifest22, formatting, and diff checks pass. Broader DTO aliases and Phase 6 reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Null-topic runtime broker lookup continuation (2026-10-09): pinned 5.5.1 bytecode and a direct invocation show `MQClientInstance.getBrokerNameFromMessageQueue` looks up `topicEndPointsTable` with the queue topic before applying fallback; a null topic throws `NullPointerException` both before and after the retained-static-endpoint refresh. The red native regression showed Rust incorrectly returned `fallback-broker`; `RouteSnapshot::broker_name` now returns `Result<Option<&str>, RuntimeError>` and maps this input to `InvalidRoute("null message queue topic")`. Oracle/fixture `5125b311`; fix/regression `e323c8da`; current `route.json` SHA-256 `780023c54340d92c538a3b106affdafb657c88378c221add2dd224faa0fc8fb1`. Full workspace tests (with loopback access), strict all-target/all-feature Clippy, pinned offline Maven verify (25 route inputs, 22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 source/API/input/reverse-export reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.


Static endpoint unmapped-queue lookup continuation (2026-10-09): pinned 5.5.1 bytecode and an invoked `MQClientInstance.getBrokerNameFromMessageQueue` case confirm a nonempty endpoint map returns its direct lookup result, including null for an unmapped queue whose `brokerName` is `fallback-broker`; the result remains null after static-to-ordinary refresh retains that map. Oracle/fixture `c7b04506`; native differential `6242ec3b`; no production change. Current `route.json` SHA-256 `85f56d6b92fefbc0117bfe0a5118c8a3ba1b3cd6375a59e6d45877a41cf2f6a0`. Focused regression, pinned offline Maven verify (25 route inputs, 22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 source/API/input/reverse-export reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.


MessageQueueAssignment nested queue getter continuation (2026-10-09): Java `getMessageQueue()` returns the object passed to `setMessageQueue()`; changing queue id through the getter updates that object. Rust adds `message_queue_mut()` and matches the mutation through the owned nested DTO. The caller-owned Java object identity is not retained by Rust's move setter. Oracle/test `caff0e22`; implementation `8fff4781`; `lookup.json` SHA-256 `9cdbcae68d20569b3d0cfdbf774aaf4183a63430438d9e3d74e8ad2e64b863d1`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Phase 6 remains PARTIAL; P6-V04 remains `PENDING_PHASE_19`.

MessageQueueAssignment attachment getter mutation continuation (2026-10-09): the pinned Java `getAttachments()` returns the Map passed to `setAttachments()`; getter-side insertion is visible through the setter-held Map. Rust already exposes `attachments_mut()`, and the new public-method differential verifies the resulting contents. Rust's move-based setter does not preserve external Map identity. Oracle/test `359ed253`; current `lookup.json` SHA-256 `8b6dab97da3ebca80f494b07ebcc7b12da396d700587137e136ea8620ed8d972`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Phase 6 remains PARTIAL; P6-V04 remains `PENDING_PHASE_19`.

BatchAck mutable BitSet getter continuation (2026-10-09): the pinned Java getter returns the assigned mutable `BitSet`; setting bit 5 through that getter is visible through the original setter reference and yields byte array `JQ==`. Rust adds `get_bit_set_mut()` for its owned little-endian `BitSet.toByteArray()` bytes and matches the resulting serialized byte value. The Java `BitSet` object alias and full object API remain unrepresented by Rust's `Vec<u8>` model. Oracle/test `bd3d2e71`; implementation `d1dc7dfd`; `lookup.json` SHA-256 `abfe31546b68799aced43ff4f631f5dea37a4c14efac1dfc60a1ed637a22b970`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Phase 6 remains PARTIAL; P6-V04 remains `PENDING_PHASE_19`.

MessageRequestModeSerializeWrapper mutable Map getter continuation (2026-10-09): the pinned 5.5.1 getter returns a live mutable nested map; Java verifies both outer and inner mutations through that reference. Rust adds `get_message_request_mode_map_mut()` and the focused differential confirms the final map. Rust owns its `BTreeMap`s and does not retain the external map alias from the setter. Oracle/test commits `94a0d62d` and `0614eda5`; implementation `6c19de4c`; `lookup.json` SHA-256 `c0ba2b2cf51eb3c900a90e57a807edb25bda2cee0fed1b9e54a303680814c6bb`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Phase 6 remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

Historical LiteSubscriptionDTO mutable OffsetOption getter checkpoint, superseded by ADR-0070: LiteSubscriptionDTO mutable OffsetOption getter continuation (2026-10-09): pinned Java calls mutate the `OffsetOption` supplied to the DTO setter and then mutate it through the DTO getter; both views observe value 13. At that checkpoint Rust added `offset_option_mut()` and matched getter-side value mutation; ADR-0070 later preserves the caller-owned object identity. Oracle/test `373dec0c`; implementation `6612a7fe`; current `lookup.json` SHA-256 `0880db9cf0e22cf22580c4c176e3413c7e3b2a2336fcc63dc6f81b616126fcaa`. Focused differential, full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Other aliases and full Phase 6 API/source/input reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical getter-mutation checkpoint, superseded for caller/setter/getter identity by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md): Heartbeat DTO mutable Set getter continuation (2026-10-09): direct pinned Java calls mutate `SubscriptionData.tagsSet/codeSet`, `ConsumerData.subscriptionDataSet`, and both `HeartbeatData` collections through assigned Set references and getters; all views observe the final members. At that checkpoint, Rust exposed mutable borrows for owned Sets/vectors and had not yet preserved caller-owned identity; the later shared-handle correction now preserves those aliases. Oracle/test `33829680`; implementation `5151a308`; checkpoint `heartbeat.json` SHA-256 `a03cd054471d1782251f8635adcc35d6da29f67f9900c215eadf67f76f5d9c30`; [ADR-0058](../architecture/ADR-0058-heartbeat-dto-public-accessors.md). Full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Broader DTO aliases and source/API/input reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical Lite subscription mutable Set getter checkpoint, superseded by ADR-0071/ADR-0072: Lite subscription mutable Set getter continuation (2026-10-09): the direct Java oracle now mutates `LiteSubscriptionDTO.liteTopicSet` and `LiteSubscriptionCtlRequestBody.subscriptionSet` through both assigned Set references and getters. At that checkpoint, those Rust mutable accessors matched getter-side values but did not retain caller-owned Set identity; later shared-handle corrections preserve the reviewed Set aliases. Oracle/test `4fd32a88`; checkpoint `lookup.json` SHA-256 `99d189c5eeba83f01b53a5ec23ab56cf5b902043db0849bd54a93ac2a3f3d743`. Focused differential, full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Other aliases and Phase 6 source/API/input reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Check-client nested DTO mutable getter continuation (2026-10-09): actual Java calls establish that changing `SubscriptionData.subVersion` through the object passed to `setSubscriptionData` is visible through the body getter, and changing `subString` through the getter is visible through the original object. Rust adds `get_subscription_data_mut()` and matches the getter-side changes; cross-owner identity is not represented after the owned setter. Oracle `cd8e761e`; implementation `ecf19a87`; `lookup.json` SHA-256 `f3d9e92e8965faa082e06f075755bed7b9fc7d0435698b35a58d19ac809d8c29`. Focused differential, full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting, and diff checks pass. Other DTO aliases and broad API/source/input reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Consumer status mutable Map getter continuation (2026-10-09): pinned Java cases mutate `messageQueueTable`, `consumerTable`, and a nested consumer table through the returned Maps; Java also exposes each same Map assigned through its setter. Rust adds `get_message_queue_table_mut()` and `get_consumer_table_mut()` and the differential verifies matching mutations through its owned maps and nested rows. Rust does not retain caller-owned Map identity. Oracle/test `89aef2e2`; implementation `3d5cfd44`; `lookup.json` SHA-256 `9fa29ff2a82f4227ebb8ea83c74c32702b6cf60f2b11e1eebbc8f0b1fff57da7`. Focused differential, full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. ResetOffsetBody and other DTO collection aliases, plus broad API/source/input reconciliation, remain open; P6-V04 stays `PENDING_PHASE_19`.

ResetOffsetBody mutable Map getter continuation (2026-10-09): pinned Java calls mutate `offsetTable` through the setter Map and through `getOffsetTable()`; both references observe queue ids 4 and 5. Rust adds `get_offset_table_mut()` and matches the final values through its owned entry rows while retaining nullable offsets; identity with the original Java Map is not preserved. Oracle/test `a6e6d723`; implementation `ff81313a`; current `lookup.json` SHA-256 `b77cd6ce3487c18e1c2d9b34c06ff2354f9c025ab65ac15a1255a3ed9fb31bc3`. Focused differential, full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Other DTO aliases and broad API/source/input reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

BatchAck mutable List getter continuation (2026-10-09): the pinned Java API mutates the `List<BatchAck>` through both the setter argument and getter; mutations are mutually visible, ending with queue id 4. Rust adds `get_acks_mut()` to expose the DTO-owned Vec mutably. The List identity with the caller's pre-move object remains different. Oracle `51b92e77`; implementation `f4d85af0`; `lookup.json` SHA-256 `9a9c052d3d57d0c4e54e6f8adc1147c71bbfa1fbc07745d63e0920ab670f4442`. Focused differential, full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, format, and diff checks pass. Other DTO alias surfaces and complete source/API/input reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical assignment response Set getter checkpoint, superseded by ADR-0065/ADR-0068: Assignment response mutable Set getter continuation (2026-10-09): direct pinned Java cases mutate `QueryAssignmentResponseBody.messageQueueAssignments` through both the setter argument and getter; both directions are visible, with queue id 4 as the final member. Rust added `get_message_queue_assignments_mut()` and matched getter mutation at that checkpoint; ADR-0065/ADR-0068 later preserve the outer Set and element identities. Oracle `1bfe20bf`; implementation `b280111c`; `lookup.json` SHA-256 `ce344c0aa35c634a730f97cf706fba3b41313fd12a2fac294d3e01250f7e9b2c`. Focused protocol differential, full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting, and diff checks pass. Broader DTO/API/source reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded by `045b46e7` (2026-10-10): Java confirmed caller/getter changes to the three lock Sets are mutually visible. Rust then used mutable borrows of owned HashSets, so caller identity did not cross the move-based setter. The current SharedValue handle retains outer Set identity; contained mutable MessageQueue aliases remain open. Oracle `f80a844d`; implementation at that checkpoint `c8aeb821`; checkpoint lookup SHA-256 `8f8f823b9a63d14dd5cf4d9b86d0538ad5ebe142138d4316639ce3e60bc5fd6c`. Other DTO aliases and broad Phase 6 reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded by `456849fb` (2026-10-10): Java proves setter/getter changes to the same List are mutually visible. At that checkpoint Rust used `get_consumer_id_list_mut()` on an owned Vec, so caller-held identity was not preserved. The current SharedValue handle keeps the assigned List alias, permits mutation through either handle, and matches the DTO's inherited identity equality/hash behavior; the core RPC boundary returns an owned snapshot. Oracle `0877435c`; implementation at that checkpoint `dd4156c5`; checkpoint `lookup.json` SHA-256 `3d031033fb4f04d0ab78df0bf7818827dec7a460e8264812f303667ee42c020b`. Other DTO alias surfaces and whole-phase reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

SOCKS bare-reference continuation (2026-10-09): four more actual Java private `getProxy` cases test empty and one-space references, bare `target.addr`, and bare `0` with no root key named `0`. All retain the matching CIDR with a null address; this distinguishes bare `0` from the separately failing `$[0]` array-index path. Rust already matches these outcomes. Oracle/test commit `47d09d76`; current fixture has 499 constructor and 32 selection cases at SHA-256 `fd46551f8755e1ca55a6434bdf15eabe58014077a402e8e17928b9eb8d9039d7`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting, and diff checks pass. Broader Fastjson and whole-phase reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

SOCKS Fastjson reference-path continuation (2026-10-09): nine actual Java private `getProxy` cases cover `$.target`, double-quoted and whitespace-bearing bracket paths, `$[0]`, `@`, `..`, `#-2`, a bare identifier, and recursive descent. Java resolves the first three to a target proxy; `#-2` and a bare identifier retain the matching CIDR with a null address; the remaining paths fail. Rust now matches the successful proxy and missing-address outcomes, and rejects the same unsuccessful forms. The native differential first failed on `$[ 'target' ]`. Oracle/test `c90a1bec`; implementation `27700865`; 499 constructor and 28 selection cases; `client-config.json` SHA-256 `2099360007a6e46dbba89ba22c70d7ca7354fe49b2b5129a082bd42ff9f2b011`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. Broader Fastjson grammar and Phase 6 reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

SOCKS route iteration-order correction (2026-10-09): direct Java private `getProxy` cases prove selection follows Java `HashMap` bucket iteration. Four overlapping-CIDR cases include three permutations of `/8`, `/9`, `/10`, and `/11`, all selecting `/9`, plus a source-bucket/destination-bucket collision where a later JSON key wins due to source-map iteration. Rust now preserves parsed root-map order and reproduces Java source traversal and destination `putAll` ordering; the red differential had selected `/11`. Oracle commits `99284f59` and `6f790b8a`; implementation `965109bd`; 19 private selection cases; `client-config.json` SHA-256 `3e46fee1230a4dbf66837deca0a17015aeea87913fa9f3b08ad79e1aa0f90c42`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 source/API/input reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

SOCKS matched-route selection correction (2026-10-09): the invoked Java private `getProxy` returns the matching CIDR entry even when its bean has no address; its observable selection has the CIDR and a null `proxyAddress`. Native selection previously errored and discarded the CIDR. Rust now carries the selected CIDR and route through the connection boundary. Oracle/test `1ca984ac`; implementation `c4bcbbbb`; `client-config.json` SHA-256 `f3a1558b2722c74fe917bcba299484563ddcc4990b3db58419ac403fa2fb4a69` (15 selection cases). Full workspace tests, strict Clippy, pinned Maven verify (22 scenarios), comparator/manifest22, fmt, and diff checks pass. Remaining Phase 6 reconciliation stays open.

SOCKS Fastjson special-reference correction (2026-10-09): actual constructor and private `getProxy` cases show `$ref` `#-1` constructs as a null/no-address route and selects no proxy, while root `$` fails construction with `ClassCastException`. Rust now matches those acceptance and selection outcomes. Oracle/test `9a8dc90d`; implementation `8cc47fc8`; 499 constructor cases at `client-config.json` SHA-256 `15e8e57a07b7a488f45750d10f5820ed41a49a2aba198254d7e2b85ba84f871a`. Full workspace tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, fmt, and diff checks pass. Other Fastjson paths and whole-phase reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

SOCKS Fastjson cyclic `$ref` correction (2026-10-09): two actual constructor and private `getProxy` cases show that self-references and two-route cycles construct successfully and select no proxy. The first native differential rejected both cycles; the resolver now materializes the same direct/no-proxy outcome. Oracle/test `115a2cb1`; implementation `2f60397e`; the 497-case constructor fixture SHA-256 is `0434b8f5de37827ad98c145d60e9888a1d7e851f79e7872742047b4e760d8c03`. Focused tests, full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt, and diff checks pass. Other Fastjson grammar and whole-phase reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

SOCKS Fastjson `$ref` correction (2026-10-09): the direct constructor oracle covers resolved, chained, and unresolved root-key references; `NettyRemotingClient.getProxy` confirms resolved credential/address values and no-proxy selection for an unresolved target. Rust now resolves root-key paths to owned equivalent route values. Oracle/test `80ba235d`; implementation `9cf5a8fb`; `client-config.json` SHA-256 `0cafefb33a4611746f78005582a3394e7a6de8818932ea3d9d947e99d3d1eb5e`. Full workspace, strict Clippy, pinned Maven verify, comparator/manifest22, fmt and diff checks pass. This closes only the tested reference forms; other Fastjson grammar and whole-phase source/input reconciliation remain open.

OffsetOption equality/hash API continuation (2026-10-09): direct pinned Java calls now exercise `equals(Object)` with equal/different values and enum types, null and unrelated objects, plus hash stability/equal-hash consistency and Set deduplication. Rust exposes `java_equals(Option<&dyn Any>)` and its Eq/Hash collection behavior matches. Java `hashCode()` includes enum object identity, so its numeric output is JVM-local rather than a portable cross-process fixture value. Oracle/implementation `e7fb09f6`; current `lookup.json` SHA-256 `72e92cb461d8b4842622850296c01e0fd604b4d49250990bc3beea2cfcefc544`; [ADR-0059](../architecture/ADR-0059-offset-option-java-equality.md). Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes), differential comparator self-test and manifest22 validation, formatting, and diff checks pass. Broader Phase 6 reconciliation remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.

Historical heartbeat DTO accessor checkpoint, superseded for Set aliases by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md): Heartbeat DTO public accessor update (2026-10-09): direct pinned 5.5.1 calls and a Rust differential now cover every declared getter/setter on `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData`, including constructor defaults, assigned values, and null setter results. Java collection getters return the same mutable Set supplied to the setter; at that checkpoint Rust owned collections and lent immutable borrows, before the later shared-handle alias correction. Oracle/implementation `21591fc7`; checkpoint `heartbeat.json` SHA-256 `7da03948a79dd50a476ba900198731af6186efd735c473e54688853f3209cd87`; [ADR-0058](../architecture/ADR-0058-heartbeat-dto-public-accessors.md). Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes), formatting, and diff checks pass. Other DTO/input/source reconciliation remains open; Phase 6 stays PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Consumer status/reset-offset API update (2026-10-09): pinned `javap` signatures for `GetConsumerStatusBody` and `ResetOffsetBody` are now invoked directly for constructor defaults, every getter/setter, populated values, and null setters. Java retains the assigned mutable outer and nested Maps; Rust owns row vectors/maps and exposes immutable borrows. Oracle/test `471b7160`; checkpoint aggregate lookup fixture SHA-256 `7b2ce204660f804147471cb2d6d220e669e33991285b64cc68faf699c15a8aff`; see [ADR-0057](../architecture/ADR-0057-runtime-map-body-public-api.md). Protocol all-target tests and pinned offline Maven verify pass. Broader reconciliation remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.

Runtime body public method update (2026-10-09): direct pinned Java 5.5.1 calls now cover defaults, all exposed properties, populated/null setter outcomes, and diagnostic text for six runtime DTOs: CheckClientRequestBody, GetConsumerListByGroupResponseBody, LockBatchRequestBody, UnlockBatchRequestBody, LockBatchResponseBody, and ConsumeMessageDirectlyResult. Rust maps their getters/setters and matches the three tested `toString()` forms. At this checkpoint Rust had not yet preserved the aliases of the assigned SubscriptionData, List, or Set values. ADR-0060, ADR-0061, and ADR-0062 now cover the CheckClient subscription, consumer-list, and outer lock Set aliases; nested queue-element and other DTO aliases remain open. Oracle `7e1155f8`; implementation `bca4329c`; current aggregate fixture SHA-256 `7b2ce204660f804147471cb2d6d220e669e33991285b64cc68faf699c15a8aff`; see [ADR-0055](../architecture/ADR-0055-runtime-body-public-api.md). Workspace tests, strict Clippy, pinned offline Maven verify, fmt, and diff pass. Broader source/API/input reconciliation remains open and P6-V04 remains `PENDING_PHASE_19`.

Historical assignment DTO public API checkpoint, superseded for Set aliases by ADR-0065/ADR-0068: Assignment DTO public API update (2026-10-09): pinned 5.5.1 method invocations cover defaults and getter/setter values for `QueryAssignmentRequestBody`, `QueryAssignmentResponseBody`, and `SetMessageRequestModeRequestBody`, including null setter behavior. `SetMessageRequestModeRequestBody` defaults to `PULL` and zero; the other optional properties default to null. At that checkpoint Java retained the setter-supplied `Set` while Rust owned its HashSet; ADR-0065/ADR-0068 later preserve outer Set and element identity. Oracle `7c7d8b5b`; implementation `fc5f2336`; checkpoint aggregate lookup fixture SHA-256 `7b2ce204660f804147471cb2d6d220e669e33991285b64cc68faf699c15a8aff`; [ADR-0054](../architecture/ADR-0054-assignment-body-public-api.md). Full workspace tests, strict Clippy, pinned Maven verify with fixture drift/cross-decodes, fmt, and diff pass. Other protocol DTO/input combinations remain open; Phase 6 stays PARTIAL and P6-V04 remains pending Phase 19.

BatchAck API and Fastjson input update (2026-10-09): actual pinned RocketMQ 5.5.1 `BatchAckMessageRequestBody.decode`/`encode` have a 95-case direct differential covering JSON token kinds, root shapes, defaults/coercions, nullable list rows, singleton conversion, duplicate-last fields, and BitSet Base64/empty omission. A separate public API invocation captures no-argument defaults, all nine BatchAck properties through getters/setters, body getters/setters, and exact `toString()`. Rust matches the observed values and null states. Java preserves mutable List identity across body setter/getter; Rust takes an owned Vec and returns a borrowed immutable slice, so this alias behavior remains a semantic difference. Oracle commits 58ad8480/7f3a4f0e; implementation commits 381b6edf/c709e743; checkpoint aggregate lookup fixture SHA-256 `7b2ce204660f804147471cb2d6d220e669e33991285b64cc68faf699c15a8aff`; see [ADR-0052](../architecture/ADR-0052-batch-ack-fastjson-input.md) and [ADR-0053](../architecture/ADR-0053-batch-ack-public-api.md). Full workspace tests, strict Clippy, pinned Maven verify with fixture drift/cross-decodes, fmt, and diff pass. Other protocol DTO/input combinations remain open; Phase 6 stays PARTIAL and P6-V04 remains pending Phase 19.

Unregister request representation update (2026-10-09): scans of the pinned client/common/remoting 5.5.1 jars find neither `UnregisterClientRequestBody` nor `UnregisterClientBody`. `javap -c` for `MQClientAPIImpl.unregisterClient` shows request code 35 created with `UnregisterClientRequestHeader` and no body; `javap` confirms the header's no-argument constructor and clientID/producerGroup/consumerGroup accessors. Rust removed both non-Java public body DTOs in `0c60e2b1`/`a1f8d747`; the request-code inventory now correctly records no body. Full workspace tests, strict Clippy, formatter and diff checks pass. [ADR-0051](../architecture/ADR-0051-remove-non-java-unregister-body.md) records the evidence. This bounded correction leaves Phase 6 PARTIAL and P6-V04 pending Phase 19.

MessageRequestModeSerializeWrapper API/default/input update (2026-10-09): pinned Java 5.5.1 declares the no-argument wrapper constructor and public messageRequestModeMap getter/setter. Direct invocations confirm the constructor initializes an empty map, the getter returns the assigned value, and a null setter value remains null. Its 33-case Fastjson corpus confirms missing-field constructor defaults, last-wins wrapper duplicates, map token acceptance/rejection at each nested level, and null-map omission on encoding. Rust now matches through an explicit Default, Java-shaped accessors, optional-field omission, and shared last-wins object decoding. Oracle corpus/API commits 4456695d/910e6838; implementation 33b993b8; then-current lookup.json SHA-256 9d4472bee58e739dbb0efa4c18a3e4405f7b58fd1e1975f862bbc431b61a016f. Workspace tests, strict Clippy, pinned Maven verify (22 scenarios and fixture drift/cross-decodes), format, and diff checks pass. Other source/API/input reconciliation remains open and P6-V04 stays PENDING_PHASE_19.

Reset offset body input correction (2026-10-09): pinned Java `javap` confirms `ResetOffsetBody.offsetTable` is `Map<MessageQueue, Long>` with an empty constructor default and public table accessors. The 77-case actual Fastjson corpus now matches Rust, covering every JSON token type, non-object roots, explicit null, object-valued MessageQueue keys, quoted serialized-key rejection, duplicate queue/body last-wins, nullable Long values, numeric coercions, and nullable/missing MessageQueue key fields and all token shapes for topic, brokerName and queueId inside object keys. Java maps booleans to 1/0, accepts numeric strings (including `+9`), truncates decimal strings and fractions toward zero, maps empty strings to null, rejects whitespace-surrounded numeric strings and overflowing integer tokens, wraps plain decimal overflow to the low 64 bits, and converts exponent tokens and numeric strings through Java double-to-long behavior, including extreme saturation and underflow. Null Long entries decode and are omitted on encode. Fastjson coerces topic/brokerName scalar, array and object values to strings, omits null names, and emits primitive queueId; queueId fractions truncate, booleans convert to 1/0, null/object values become zero, and arrays reject. `decode_java_json` and `encode_java_json` preserve these Java body semantics; the normal Serde form remains Rust-owned JSON. Lookup fixture SHA-256: `202dc2d502473e2e2a73861bfcc82e359668c8f5222744b619052be04c9c014f`; test commits `76cc4eb`/`c7e1e8c`/`9cfd106`/`2260129`/`85cd833`, implementations `f6a75eb`/`eadcb33`; [ADR-0048](../architecture/ADR-0048-reset-offset-fastjson-map-input.md). Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios and fixture drift/cross-decodes), comparator/manifest22, fmt and diff pass. Broader Phase 6 source/API/input audit remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.

Consumer status body input correction (2026-10-09): pinned `javap` confirms Java `Map<MessageQueue, Long>` and nested consumer maps, with constructor-empty defaults and public getter/setter methods. The 143-case actual Fastjson decode/encode corpus now matches Rust, covering all JSON token types for both tables, non-object roots, object-valued MessageQueue keys, explicit null maps, nested null maps, duplicate queue keys and duplicate body fields, plus Java `Long` coercions. Boolean values map to 1/0; numeric strings including a leading plus and decimal strings truncate as Java does; whitespace-surrounded numeric strings reject and an empty string becomes null. Fractions truncate toward zero, plain decimal overflow wraps to 64 bits, extreme exponent values and strings follow double-to-long conversion, including saturation and underflow, and integer overflow rejects. Null `Long` values survive decode and are omitted by Java encoding. Rust's public fields preserve null and empty states; `decode_java_json` handles Java's non-standard object-key form and `encode_java_json` matches the observed body bytes. Lookup fixture SHA-256: `202dc2d502473e2e2a73861bfcc82e359668c8f5222744b619052be04c9c014f`; test commits `76cc4eb`/`c7e1e8c`/`9cfd106`/`2260129`/`85cd833`, implementations `f6a75eb`/`eadcb33`; [ADR-0047](../architecture/ADR-0047-consumer-status-fastjson-map-input.md). Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios and fixture drift/cross-decodes), comparator/manifest22, fmt and diff pass. The wider Phase 6 source/API/input audit remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.

`ConsumeMessageDirectlyResult` field input update (2026-10-09): the pinned Fastjson 2.0.63 oracle now covers 66 cases: all seven JSON token kinds for each field, null/object/array/scalar roots, and repeated-field last-wins. Primitive `order`/`autoCommit` null and numeric inputs become false; missing `autoCommit` remains true. `remark` converts non-null JSON values to Java String text. `spentTimeMills` maps null to zero, truncates fractions, maps booleans to 1/0, and rejects strings/arrays/objects. The existing `consumeResult` name/ordinal cases remain included. Rust's custom deserializer and explicit `Default` match these normalized outputs and the Java no-argument constructor (`autoCommit=true`). Oracle/test `910cbd2`, implementation `00d2ea8`, constructor-default regression/fix `6d53470`/`b4ec6f4`; `lookup.json` SHA-256 `c7662fe1bbbe62726eef0eda14774a3bc08b3910581d771153c3b7b04eccaf40`; [ADR-0049](../architecture/ADR-0049-consume-result-fastjson-input.md). Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios and fixture drift/cross-decodes), fmt and diff pass. Broader Phase 6 reconciliation remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.

Lite subscription API/input continuation (2026-10-09): pinned 5.5.1 bytecode confirms `LiteSubscription` fields/defaults, fluent setters, add/remove timestamp behavior and text format; `LiteSubscriptionDTO` fluent setters and text format; `OffsetOption` constructors/constants/accessors/equality; and control-body Set getter/setter behavior. Direct Java cases verify each API surface, OffsetOption HashSet equality, and 16 Fastjson inputs for LiteSubscription, including concurrent Set null rejection, duplicate Set accumulation, duplicate scalar last-wins, and primitive timestamp coercion. Rust maps these APIs; the OffsetOption Hash trait preserves value-based Set membership, while the numeric Java hash remains JVM-specific; direct same-process stability and equal-hash contract checks now pass. Fixture SHA-256 at that update: `d618ea8b60671b98fc7c8ec53746c2a0f4bddcb7c744d061b934d38cd6981538`. Workspace tests, strict Clippy, Maven offline verify (22 scenarios and fixture drift), comparator/manifest22, doctests, fmt, and diff checks pass. Broader Phase 6 reconciliation remains partial; P6-V04 stays `PENDING_PHASE_19`.

`QueryAssignmentResponseBody.decode` field-shape continuation (2026-10-09): the Set-field input corpus now has 30 cases, adding a fractional numeric value for `messageQueueAssignments`; pinned Java rejects it with `JSONException` and Rust matches. Together with seven root-shape cases, the current assignment response corpus has 37 inputs. Oracle/test commit `3e58bc1`; `lookup.json` SHA-256 `4723f8ad95ae556dbc99db83ef81cc311f0aae128cf59e23f8bbda6b1045618a`. No implementation change was needed; broader Phase 6 source/API/input reconciliation remains open.

Runtime DTO root-shape continuation (2026-10-09): seven direct Java 5.5.1 cases each cover roots for `QueryAssignmentResponseBody`, `GetConsumerListByGroupResponseBody`, and `CheckClientRequestBody`. Empty objects decode, null roots fail on the source-used getter, and array/scalar roots reject; native tests match acceptance, fields, and response encoding. Current counts are 37 assignment-response, 23 consumer-list, and 43 check-client body inputs. Fixture hashes: lookup `9d4472bee58e739dbb0efa4c18a3e4405f7b58fd1e1975f862bbc431b61a016f`; client-check `cdd164bbcaf8cb7a096828b289f69c56c2706c965e97bfe4f228b8ad08c16ee2`. Other DTO/source reconciliation remains open.

Nullable route-map key continuation (2026-10-09): four actual Fastjson 2.0.63 calls prove quoted and unquoted null map keys both decode as a null `Long` key for `BrokerData.brokerAddrs`; the corresponding `Integer` map in `TopicQueueMappingInfo.currIdMap` rejects both. The native DTO preserves null and numeric broker IDs separately and serializes the null key as `"null"`. Pinned `MQClientInstance` bytecode shows that admin lookup, arbitrary subscribe fallback and heartbeat send paths unbox IDs when their address conditions select an entry; native route and heartbeat regressions preserve those failure branches with a typed error. The full route DTO differential contains 399 cases at fixture SHA-256 `0472ed8171207d35519adae7521741d359e343a3c378cf75537cb5c29cb41313`.

Heartbeat DTO root-token input continuation (2026-10-09): 28 direct Fastjson cases test seven root forms across `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData`. Empty objects and JSON null are accepted; arrays and scalar roots reject. Rust optional decoding matches, with the wall-clock default `subVersion` omitted from the empty-subscription comparison. Total heartbeat DTO inputs: 241; fixture SHA-256 `385fc38890b6f18fc9ea74a4a4d2a47f52c27bae5ffc7851064c594cef426803`. Focused differential passes without a production change.

Route DTO root-token input continuation (2026-10-09): 35 actual Fastjson 2.0.63 calls exercise seven root JSON shapes for the five route DTOs. Empty objects decode to defaults; null values reach the probe's getter and throw `NullPointerException`; arrays and scalar roots reject with `JSONException`. Native differential matches all acceptance/field outcomes without a production change. The route corpus is 395 cases; fixture SHA-256 `4c5634c4ddce8a311db77fe458e0bf16f63288e0bcd116f53af1c294700ed323`. Focused route differential and pinned Maven verify pass; broader DTO/raw/API reconciliation remains open.

ClientConfig TLS property lifecycle continuation (2026-10-09): the fresh JVM first initializes `TlsSystemConfig.tlsEnable` with `tls.enable` absent, then sets the property to true and constructs another ClientConfig. Both report `useTLS=false`, proving that the value is captured at class initialization. Rust now caches `tls.enable` at the first `ClientConfig::new`; its isolated child-process regression matches the invoked Java fixture at SHA-256 `018510bf6f67c1e6a556fb0dffc1ed50ba64cab2930f4785dfa421cd466b0c68`. Test commit `ddb56eb`, implementation `78df3d7`. Workspace/all-target tests, strict Clippy, offline Maven verify (22 scenarios), comparator/manifest22, formatting and diff checks pass. TLS provisioning remains Phase 19 work and broader Phase 6 reconciliation remains open.

Nested route collection/member continuation (2026-10-09): expanded the route DTO matrix to 337 direct Java Fastjson cases. New cases apply all seven JSON token kinds to `queueDatas`/`brokerDatas` elements, `filterServerTable` values, and `topicQueueMappingByBroker` values, plus empty-string filter values. Java accepts null/object DTO elements, null/object mapping values, and filter lists supplied as string, array, or object; empty string becomes an empty list. Numeric/Boolean filter map values and scalar/array DTO elements reject. Rust initially rejected the Java-accepted filter map string/object/empty-string shapes; `deserialize_optional_string_list_map` now matches their singleton/empty/null outcomes. Focused differential passes; no other implementation changed. Current route fixture SHA-256 `3d631d0c40c1a6f96a1a4250d1e1c7012c2062551d65d073a84b8f9a8c8ac375`. Other route inputs and full Phase 6 source/API reconciliation remain open.

Static mapping entry-value continuation (2026-10-09, prior checkpoint): seven pinned Java cases covered all JSON token kinds inside `TopicQueueMappingInfo.currIdMap` values. Java rejected null, nonnumeric string, and array values; it accepted integer, fractional-number truncation, Boolean-to-integer, and empty-object-to-zero. The 308-case checkpoint fixture SHA-256 was `936ee195d1e2a02268a59c210e5b2732f16b1725904b1d9e78bf8418e063b071`.

Nested route-collection member continuation (2026-10-09, prior checkpoint): added 14 direct Java Fastjson cases for the seven JSON token kinds at `BrokerData.brokerAddrs` map values and `TopicRouteData.filterServerTable` list members. Java accepted all cases and converted non-null scalar/array/object values to Java strings; null values remained null. The 301-case checkpoint route fixture SHA-256 was `8de4b09d06779e0f5bbe11d3bad07fbbadf792a2eb2dfa39af1bbf1158c43a0c`.

SOCKS escape-follower continuation (2026-10-09): invoked Fastjson for every BMP code unit and all 1,048,576 supplementary scalars after a backslash. Java accepted 15,872 BMP units and 253,952 supplementary scalars; Rust matched all 63,488 representable BMP scalar followers and every supplementary scalar. Isolated Java surrogate units cannot be represented by Rust `String`. Focused exhaustive tests pass; `client-config.json` SHA-256 `bf9b63dfb9c38a4f4090a4326a5d14bc9bff62478e4df4330825cb5009bc08f3`. Broader Fastjson and Phase 6 source reconciliation remain open.

Raw route cleanup default-input continuation (2026-10-09): expanded the invoked Java `MQClientInstance.cleanOfflineBroker` matrix from 8 to 13 shapes. New cases cover absent route fields, an empty broker list, a missing broker address map, a null broker name with a live slave address, and null queue/filter collections that cleanup ignores. Java leaves the address table empty for the first three; the latter two retain the live slave address. Native `malformed_raw_route_cleanup_shapes_match_invoked_java_runtime` passes all 13 outcomes. Route fixture SHA-256 `b7f7653977913cd8a545f9d9d59153a459ca343ced1dcc897d92302a4b926df0`. Other raw/DTO inputs and full source/API reconciliation remain open.

Raw route cleanup continuation (2026-10-09, prior checkpoint): invoked Java `MQClientInstance.cleanOfflineBroker` on eight null/malformed raw route shapes. Null broker lists and rows throw `NullPointerException` before the broker address table changes; null address maps are skipped, and null address values are retained when present in both published and raw routes. Native cleanup matches these outcomes and leaves the published cache generation in place on malformed lists/rows; nested route DTO handles can still reflect mutations through Java-compatible aliases. Route fixture SHA-256 at that checkpoint: `340c6a0e79a2657efb0de3496e5da003ff821fda8434d3572a3e45c407b69102`.

CheckClientRequestBody field-shape continuation (2026-10-09): expanded its actual Java Fastjson input matrix from 8 to 36 by applying seven JSON token kinds to `clientId`, `group`, `subscriptionData`, and `namespace`. Java and native acceptance/normalized results match, with nested `SubscriptionData.subVersion` pinned for deterministic output. No production change was needed. Fixture SHA-256 `7e9d758ce72e0282d484dc41404e77a318d922662def78988855fe4dc8958570`. Other DTO/source reconciliation remains open.

Heartbeat DTO field-shape continuation (2026-10-09): expanded the direct Java Fastjson matrix from 73 to 213 inputs, adding all seven JSON value kinds to each field in four heartbeat DTOs (140 new cases). The fixture generator pins `subVersion` on unrelated `SubscriptionData` inputs so the wall-clock constructor default cannot make results drift. Explicit-null `subVersion` returns zero in both Java and the existing native decoder; omission retains the timestamp. All cases pass with no production change. Fixture SHA-256 `4c6aee94aaec527dfb864bea1bed8d6f2903132e6e73e3a353c3ce08a77affdf`. Remaining source/input reconciliation stays open.

SOCKS proxy DTO field-shape continuation (2026-10-09): added 21 direct Fastjson cases for null, numeric string, integer, fraction, Boolean, array, and object values across `addr`, `username`, and `password`. All are accepted and converted to Java string values; native results match, including credential retention when the address is null. Fixture SHA-256 `d86eef5bdf622121721fe6c0663879b9704b709cb84e21ca0aab6198e3b18174`. Other grammar and Phase 6 source/API/input reconciliation remain open.

Route DTO shape-matrix correction (2026-10-09, prior checkpoint): expanded direct Java Fastjson coverage from 133 to 287 cases by testing null, numeric string, integer, fraction, Boolean, array, and object values on every field across four route DTOs (154 new cases). The red comparison found empty object accepted as `TopicQueueMappingInfo.epoch=0`; both native `epoch` decode paths reject it like Java. The 287-case checkpoint fixture SHA-256 was `294e3cd94cad4df71d55c1ce7c7060096993749b1f5932c7089f778226212a48`.

Static route-owner continuation (2026-10-09): the Java 5.5.1 endpoint converter corpus now has ten overlap/scope/epoch/ID/owner cases and matches native `TopicRouteData::static_queue_endpoints` after sorting rows by queue ID and broker name; Java map iteration order remains unspecified. Direct coverage includes sparse and out-of-total IDs, separate per-ID owners, aliased global scopes, epoch threshold -1, nonpositive totals, null scope, and null/empty owner values. Fixture SHA-256 `a17b806643611eb03c4e11cbcd92ca81700b2384a2547e9370aff102e9eaceb9`; focused native regression and Java generation pass. General malformed route combinations and Phase 6 source/API/input reconciliation remain open.

Endpoint host-scalar continuation (2026-10-09): pinned Java 5.5.1 `NettyRemotingClient.getHostAndPort` accepts and preserves all 65,536 BMP UTF-16 code units and all 1,112,064 Unicode scalar values inserted into a non-bracketed hostname. Rust compares raw key retention and host/port splitting for every Rust Unicode scalar; isolated surrogates remain Java-only `String` inputs. The direct address fixture also exhausts all legal socket ports and all BMP `Integer.parseInt` digit inputs. `address.json` SHA-256 `1ed29bbb157c3f42af594cffc033e4ff1b7b39fe8b94af71367782f385640a64`; focused and full workspace tests, strict Clippy, Maven verify, comparator/manifest22, fmt and diff pass. DNS/interface resolution and broader source/API/input reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Named IPv6 scope address fix (2026-10-09): the actual Java address oracle now carries a loopback `NetworkInterface` case and confirms that normalization prints `%lo0`; Rust preserves that name from Unix enumeration instead of emitting numeric `%1`. Numeric scope `%7` behavior remains covered. Oracle/test commits `3c3a57e` and `0ca2d6b`, implementation `c3101f4`, fixture SHA-256 `89143f5f832705d4d86cdfc24f0dc36a7fe2cf7a803c66e28ed37f45ff9e354a`. Core tests, Clippy, pinned offline Maven verify, comparator/manifest22, fmt and diff pass. Windows API mapping was reviewed against OpenJDK source but not compiled or run.

TopicPublishInfo route-presence callback (2026-10-09): an actual Java `MQClientInstance` refresh delivers `haveTopicRouterInfo=true` and non-null route data with an empty publish queue list (`ok=false`); an absent route produces no callback. Rust's `TopicSnapshot` route presence matches that distinction in `accepted_empty_route_keeps_route_presence_separate_from_publish_queue_ok`. Oracle/test commit `8529ab7`; current `route.json` SHA-256 `649c4ce7ce97e0ddc619330082eaa7c7d8f81af0c3040e56d06f9ff14416214d`. Full core crate tests, strict core Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. Producer-local selection, queue filters/index, and fallback remain Phase 7 work; Phase 6 remains partial.

SOCKS exponent underflow route-key correction (2026-10-09): eleven direct Java inputs cover numeric overflow/underflow, signed zero, `D`/`F` suffixes and canonical-key collisions. Java preserves `-0.0` for a negative nonzero exponent value that rounds to zero, but maps lexical negative zero to `0.0`; native behavior now matches. Oracle/test `bc5c6dd`; implementation `f5b02d4`; `client-config.json` has 492 cases at SHA-256 `fcc525eb617ab9d5f257eac11ed9ae10fb6e55fd0c7035603e105d40b665007d`. Focused and remoting crate tests, pinned offline Maven verify (22 scenarios), strict Clippy, comparator/manifest22, fmt and diff pass. Broader Phase 6 reconciliation remains open.

SOCKS special-key rejection continuation (2026-10-09): nine direct pinned Java `MQClientInstance` inputs reject `NaN`, `Infinity`, `-Infinity`, `undefined`, mixed-case Boolean/null tokens and `falsehood` as unquoted route keys. Rust's existing literal guard matches; no production change was needed. Test commit `1843b1c`; `client-config.json` has 481 cases at SHA-256 `0dcc04b4ce7d41cbd9dc3b38750c52e492f527345078e0ac60ca4c7187129ee5`. The focused differential, Maven verify (22 scenarios), strict Clippy, comparator/manifest22, fmt and diff pass. Broader Phase 6 reconciliation remains open.

SOCKS numeric route-key grammar continuation (2026-10-09): 18 direct Java `MQClientInstance` inputs cover signed base-prefix rejection, unsigned leading-dot rejection, and canonical numeric spelling for negative/signed fractions, exponent signs, and `L`/`D`/`F` suffixes. Java accepts `+.5` and `-.5` but rejects `.5` and `.5F`; the native regression first failed on `.5` and now matches. Oracle/test `afbca6d`; implementation `30556c2`; `client-config.json` SHA-256 `37110dd0267bd8c6e07568cce2330519cc28e6e1116719aa03dd9325234e06f0`. Focused and crate tests, pinned offline Maven verify (22 scenarios), strict Clippy, comparator/manifest22, fmt, and diff pass. Broader Fastjson and Phase 6 reconciliation remain open.

SOCKS base-prefixed route-key rejection continuation (2026-10-09): three direct Java `MQClientInstance` inputs reject unquoted hexadecimal, binary, and octal route keys. The native 454-case constructor differential matches these outcomes. Test commit `a72996e`; `client-config.json` SHA-256 `213e2d8880557161c187fe449e668e36495c2acd17164e6288b1f70840dc3651`. Pinned offline Maven verify (22 scenarios and fixture drift), strict all-target/all-feature Clippy, comparator/manifest22, fmt, and diff pass; no implementation change was required.

SOCKS cross-type route-key continuation (2026-10-09): five direct Java constructions prove numeric/string and Boolean/string keys merge with equal key text, preserving the final property value, while null and string `"null"` keys coexist with separate route values. Rust's 451-case differential matches. Test commit `9e27434`; `client-config.json` SHA-256 `86c8c9369e43579c6f30f486045c9b8c2106076613e6e8627a029af5dfce9452`. Pinned offline Maven verify (22 scenarios and fixture determinism), strict all-target/all-feature Clippy, comparator/manifest22, fmt, and diff pass. No implementation change was needed; broader Phase 6 input/source reconciliation remains open.

SOCKS numeric key-collision continuation (2026-10-09): four direct Java `MQClientInstance` constructions cover numeric aliases `1`/`01` in both orders and interleaved duplicate sequences. Java retains the final proxy address `last` for key `"1"` in all four cases; Rust's 446-case differential matches. Test commit `a3a1367`; `client-config.json` SHA-256 `f73d93400a223d511d4d745eb8140d97fd455d8632b0b35eacd889d010754033`. Strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and fixture determinism), comparator/manifest22, fmt and diff pass. No implementation change was needed; broader Phase 6 source/input reconciliation remains open.

SOCKS numeric route-key formatting continuation (2026-10-09): five direct Java constructor cases confirm canonicalization of leading-zero and `L`/`D`/`F` numeric keys: `0123`→`123`, `123L`→`123`, `1.25F`→`1.25`, `1D`→`1.0`, and `1e7F`→`1.0E7`. The Rust differential passes all five with the existing implementation. Test commit `a373244`; 442 constructor cases at `client-config.json` SHA-256 `ec33171ed7bd799715775ddb08e3dbce00c7bd1ff4f349efde3de1a6759b39e1`. Pinned offline Maven verify, comparator/manifest22, fmt and diff pass; full Rust workspace and strict Clippy passed on the unchanged implementation. Broader SOCKS/Phase 6 input reconciliation remains open; P6-V04 stays pending Phase 19.

SOCKS unquoted scalar route-key correction (2026-10-09): direct Java `MQClientInstance` construction accepts unquoted numeric, Boolean, and null route keys, and rejects an ordinary identifier. Java formats numeric keys before storing them in the string-keyed route map (`1e2`→`"100.0"`, `+10`→`"10"`), stringifies Boolean keys, and retains null as an actual null key. The Rust parser now matches the eight outcomes and reflected key shapes. Oracle/test `605b87d`; implementation `fc03fa6`; 437 constructor cases at `client-config.json` SHA-256 `7dd846612cfd649ef77ea0b8fd3bf3f75aad5504b4952f6dd0f6d183724ae6e0`. Full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify (22 scenarios and deterministic fixtures), comparator/manifest22, fmt and diff pass. Null-key selection exception-class parity and remaining Fastjson/Phase 6 source/input reconciliation remain open; P6-V04 stays pending Phase 19.

SOCKS EOF line-comment boundary (2026-10-09): three direct Java constructor inputs show that an EOF line comment after a complete nested proxy route accepts an unclosed root object, while the same EOF inside an open route object or array rejects. Rust's existing recovery behavior matches all three. Oracle/test commit `b468c58`; SOCKS constructor corpus now has 429 cases at SHA-256 `1f25f9939e05b869005252f52fa9bb65510486e8f3caa74d51559c64ec91409b`. Focused differential and pinned Java fixture generation pass; no implementation change was needed. Other Fastjson grammar and Phase 6 source/API/input reconciliation remain open; P6-V04 stays pending Phase 19.

Numeric map-key normalization collisions (2026-10-09): eight direct pinned Java Fastjson inputs cover forward/reverse alias order and interleaved exact duplicates for broker-address `Long` and mapping-ID `Integer` keys. Java retains the final property's value. The first Rust fix `0bc9d7b` preserved alias order; the expanded oracle found that exact duplicates still retained their first position, fixed by `f137d2e` using raw entries without deduplication. Oracle/test commits `62d9454` and `5d82500`; route DTO corpus has 133 cases at SHA-256 `edb7150e939a36d7d5b570da13d08f20d3a385d8e992f7074d3fbf4a92c0e26b`. The focused red/green test, full workspace/all-targets, strict all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. Other DTO/input reconciliation remains open and P6-V04 stays pending Phase 19.

Automatic ClientConfig IP selection (2026-10-09): Java's direct `NetworkUtil.getLocalAddress()` and no-argument `ClientConfig` fixture select `10.10.10.111` on the pinned macOS/JDK host. Rust `new_auto()` previously selected `192.168.139.3`; the macOS adapter now reverses only IPv4 candidates from `getifaddrs` into the observed Java order before applying the existing selection rule. Red/green oracle/test `482e02d`, implementation `5791e28`; `client-config.json` SHA-256 `138164047171a6e1bb649d3b70239a023245341852cd4036148d80d3c722b5ea`. Full workspace, strict Clippy, pinned offline Maven verify, comparator/manifest22, fmt and diff pass. Non-macOS and IPv6-only host ordering remain under reconciliation; P6-V04 stays pending Phase 19. See [ADR-0045](../architecture/ADR-0045-automatic-client-ip-java-order.md).

SOCKS nested proxy-bean input continuation (2026-10-09): four direct pinned Java `MQClientInstance` constructions extend the SOCKS input corpus to 426. Last duplicate values win on `addr`, `username`, and `password`; a final null clears username, Boolean password coerces to the text false, and uppercase/camel-case aliases are ignored. The native constructor/reflection differential matches all cases. Test commit `5b44517`; `client-config.json` SHA-256 `478a909c8a7fea8f439e3fafc3ecbdc8a27a8aede6d515dc6f63f59552abb333`. Focused test and pinned Maven verify pass. Other Fastjson grammar remains under reconciliation; P6-V04 remains pending Phase 19.

ClientConfig direct system-property inputs (2026-10-09): `ClientConfigPropertiesProbe` runs in a fresh JVM and compares defaults plus configured values for all eight fields initialized from direct Java `System.getProperty` calls. The differential covers case-insensitive true, whitespace-padded false, and explicit empty SOCKS config; Java and Rust match without an implementation change. Test commit `3f2ebdf`; fixture `client-config-properties.json` SHA-256 `16d5a1d2b90bb79b494bf2bca4e97a73117d031ee445147720fad907b1ac5e52`. Pinned Maven verify checks fixture determinism alongside 22 scenarios. TLS and NameServer constructor properties are separately covered; full Phase 6 reconciliation remains open and P6-V04 stays pending Phase 19.

ClientConfig TLS constructor input (2026-10-09): a fresh pinned Java 5.5.1 JVM with `tls.server.mode=enforcing` and no `tls.enable` records `TlsSystemConfig.tlsEnable=false` and `ClientConfig.useTLS=false`; a standalone `NettyClientConfig` defaults true, but `MQClientInstance` copies the ClientConfig value into it. Rust now reads only `tls.enable` for `ClientConfig.use_tls`, defaulting false. Red/green test commit `cbac242`, implementation `4084689`, fixture `client-config-tls.json` SHA-256 `8ac6b07475ea0af746b70541678355e3f244fffcf6b6bb0bd70d9a7e60a14b46`; full workspace, strict Clippy, pinned offline Maven verify, comparator/manifest22, fmt and diff pass. This does not cover TLS provisioning; P6-V04 remains pending Phase 19 and full Phase 6 reconciliation remains open. See [ADR-0044](../architecture/ADR-0044-clientconfig-tls-property-default.md).

Route numeric-map-key input audit (2026-10-09): twelve new direct Java 5.5.1 DTO cases cover string keys for `BrokerData.brokerAddrs` (`Long`) and `TopicQueueMappingInfo.currIdMap` (`Integer`). Java and Rust accept plus/zero-padded, fractional, and exponent forms, including maximum signed keys; both reject whitespace and overflow. No production change was needed. Oracle/test commit `7db339a`; the route DTO corpus now has 99 cases and fixture SHA-256 `06b96e4e50223a3b1d8633317ff309ecf759ef7e74e238a76670bcb5ad1d2e31`. Full workspace tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, fmt and diff checks pass. Other DTO/input and whole Phase 6 reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

Raw endpoint-key parity (2026-10-09): Java's client `channelTables` retains exact raw address keys. The pinned oracle distinguishes four pairs that differ in port padding, host case, or IPv6 hex case; the native client previously merged these spellings. Oracle/test commit `f0f1d41`; fix `67a57ad` preserves the raw key while parsing the dial target separately and keeps empty-host IPv4-loopback behavior. Address fixture SHA-256 `b7112911b84f023d1b32408a01b4571c0086570a96857e2e24b12526b40de9f4`; 42 IP-classifier rows, 24 endpoint rows, four key pairs. Full workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. Whole Phase 6 reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

Earlier empty-host endpoint correction (2026-10-09): the pinned Java endpoint oracle then had 22 cases. Java splits :10911 to an empty host and port 10911; its JDK resolves the empty host to 127.0.0.1. The native differential failed first because Address::parse rejected the host, then showed localhost selected ::1 before 127.0.0.1 on this machine. At that fix checkpoint Rust canonicalized the empty host to the JDK loopback target; the raw-key update above now preserves the original cache key and applies the loopback mapping only to the dial target. Red oracle/test commits 5de98b9 and d1fd8d0; fix 018e9d3; loopback transport regression 159f43c. The then-current address fixture SHA-256 was 3f66b46e922d4c278510c8e2ea29ae26423cf30dec6cc2e5e304087260c37c71. Focused differential and loopback tests passed; the remoting suite then had 67 library and 81 integration tests. Full Phase 6 reconciliation remains open and P6-V04 stays PENDING_PHASE_19.

Address classification boundary update (2026-10-09): the pinned Java NetworkUtil/UtilAll oracle and Rust differential now cover 42 numeric IP inputs, including IPv4 private-range cutovers and nearby external/special ranges, IPv6 link/site-local boundaries, ULA, and mapped IPv6. Internal/loopback classification and normalized values match in all cases. Oracle/test commit 560da5d; address fixture SHA-256 7a385ac782f2d27c5bb9f6fc9a7dee284833115c33acb28b692c6aea44dcb89b. Focused address helper test, pinned offline Maven verify (22 scenarios), differential comparator/manifest22, formatting and diff checks pass. Phase 6 remains open.

SOCKS document-whitespace boundary update (2026-10-09): the pinned Java constructor oracle and native differential now cover 402 socksConstruction cases. Java accepts a leading BOM and rejects NBSP U+00A0, EM SPACE U+2003, IDEOGRAPHIC SPACE U+3000, U+2029, and trailing U+FEFF around an otherwise valid object. The native differential failed on those five cases before 9aa960c; its pre-parser now rejects non-ASCII whitespace outside strings/comments and permits a BOM only at document byte zero. Oracle/test commit ab07387; current client-config.json fixture SHA-256 acc9fc36854200847ce88a3864d68136410387adcc45cc968947cc9040251ed9. Focused SOCKS differential, remoting library tests (67/67), and pinned offline Maven verify (22 scenarios) pass, along with formatting and diff checks. P6-V04 remains PENDING_PHASE_19; Phase 6 reconciliation is still open.

Exact decimal coercion checkpoint (2026-10-09): route input coverage reached 77 pinned Java cases and heartbeat DTO input coverage reached 47. Java `TopicQueueMappingInfo.epoch` decimal/exponent values truncate and wrap to the low 64 bits, while overflowing integer tokens reject; `currIdMap` decimals narrow to the low 32 bits. Heartbeat `codeSet` high-precision decimals match Java at Long boundaries, including `9223372036854775807.0`→-1, `9223372036854775808.0`→0, and `-9223372036854775809.0`→-1. Route fixture SHA-256 `d53061010582605048578e1d4d7a794764ce5be14a4616cd9c1db72e1a17b725`; 47-case heartbeat fixture SHA-256 `387338054f1043053dbeebc14a3dc20893ac450dce19ce5dfebb99657bd7c412`. Probe/test commits `96e29d2`, `27366c2`, and `51191e9`; implementation `e39abf5`.

Numeric-to-string update (2026-10-09): the pinned oracle adds five heartbeat inputs for high-precision decimal values and exponent-form values in `topic` and `tagsSet`. Plain decimal `9223372036854775807.123456789012345678901` preserves all digits; `1e2` and `1.0e2` become `100.0`, while `1.234567890123456789e2` becomes `123.45678901234568`. The raw-number string decoder now retains exact integer/plain-decimal tokens and follows Fastjson's floating representation for exponents. Probe commits `cb75740`, `f4272fd`, `17b46bd`; implementation `5e30b73`. Current heartbeat fixture has 52 DTO inputs and SHA-256 `e183f318134bd5dfa85bcbd80276c3e85c444f3ca0ebbe8231696ef2b4305db7`. Full workspace tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator self-test, formatting and diff checks pass. Phase 6 remains incomplete; P6-V04 remains PENDING_PHASE_19.

Nullable route decode update (2026-10-08): pinned Java `RemotingSerializable` returns null for null route decode/fromJson inputs and JSON `null`; `decodeList` returns null for null input and preserves null members. Follow-up oracle calls invoke these null cases directly on both `TopicRouteData` and `TopicQueueMappingInfo` (`c00e1a8`), and Rust adapter tests cover both (`8420373`). The Rust route API now returns `Option<TopicRouteData>` inside its RPC result. A successful JSON-null refresh leaves the cached route snapshot untouched, and the null-topic member of an all-topic refresh continues without publication (`95cda6c`); a missing body remains a malformed response. Route fixture SHA-256 `55ff6cf477d53e78f5715294613e3a98d21796b729118a04feb999b213c17245`; [ADR-0039](../architecture/ADR-0039-nullable-route-decode.md) records the evidence. Full workspace tests, strict Clippy, pinned offline Maven verify (manifest22), comparator self-test, fmt and diff pass. Phase 6 reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

SOCKS Fastjson escape update (2026-10-08): the pinned Java constructor oracle and Rust differential now cover 397 `socksConstruction` cases, including every escape follower U+0000–U+00FF and 15 Unicode/control probes. Fastjson 2.0.63 `char1` masks each UTF-16 code unit to its low seven bits before table lookup. `\u` consumes exactly four UTF-16 units even when a supplementary scalar crosses the final digit: native normalization retains a valid decoded high/leftover low pair and uses U+FFFD for a remaining unpaired unit. Constructor outcomes and normalized proxy values match. Client-config fixture SHA-256 `782e9c5ef3c8523d7de1ee0d61c9ad0cf16fc8036e65e6c296a26a1e6df824ab`; [ADR-0038](../architecture/ADR-0038-fastjson-socks-escape-grammar.md) records the boundary. Full workspace tests, strict Clippy, pinned offline Maven verify (manifest22), comparator, formatter, and diff checks pass. Other Fastjson grammar and whole Phase 6 reconciliation remain open; P6-V04 remains PENDING_PHASE_19.

Prior Heartbeat DTO Set input checkpoint (2026-10-08): the pinned Fastjson differential covered 43 actual inputs. `tagsSet` singleton strings and objects are accepted (objects compact-stringify in input key order); numeric/Boolean scalars reject, while array values convert and deduplicate. `codeSet` accepts singleton number/string/Boolean/object values using Fastjson's integer coercion. Field-specific `subVersion` decoding rejects object and array inputs without changing the shared route integer adapter. Checkpoint fixture SHA-256 `bc147764feb4ddfca190c098eb014f585148e7b8989c804922acae5611384612`; [ADR-0015](../architecture/ADR-0015-heartbeat-dto-fastjson-input.md) records the cases.

Route DTO inherited serialization update (2026-10-08): pinned Java 5.5.1 invokes inherited `RemotingSerializable` methods on `TopicRouteData` and `TopicQueueMappingInfo`. Rust matches tested compact/pretty JSON and encoded bytes, including Fastjson2's sorted bean fields and unquoted numeric keys, and the route API decodes those Java response bytes through its nullable adapter. Six cases cover default, explicit-null, and populated DTOs; decode, list decode, and fromJson also match. Follow-up direct Java null utility calls cover both concrete DTO classes (`c00e1a8`), with Rust adapter tests in `8420373`. The initial serializer fixture hash `935426217262daeb92860ef9b90cc61b5cde8aa71d3042ca6f4713582568e2b3` is extended by null utility cases to the then-current SHA-256 `55ff6cf477d53e78f5715294613e3a98d21796b729118a04feb999b213c17245`; Java map iteration is unspecified while native output is deterministic. Whole Phase 6 source/API/field/branch reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

Heartbeat serialization API update (2026-10-08): pinned `HeartbeatData` extends `RemotingSerializable`; source at `efa1be59` and the 5.5.1 artifact both use Fastjson2. The Java oracle exercises default and explicit-null DTOs plus nested populated metadata through compact/pretty `toJson`, no-arg/static `encode`, `decode`, `decodeList`, and `fromJson`; direct null probes cover JSON `null`, null byte/string input, null list input, and preservation of null list members. Rust adds `java_encode/java_to_json`, `java_decode*`, and `java_from_json*` adapters; V1/V2 sends use the byte-compatible encoder, and the decode adapters preserve the tested null results. The original 21-case input set included seven grammar probes and four object/array-to-String cases preserving Java's input key order; the later Set/numeric continuation brought the fixture to 43 inputs, decimal boundary probes brought it to 47, and the numeric-to-string cases bring it to 52. Rust serialization failures in heartbeat and typed extension request bodies return `RpcError::RequestSerialization` before the RPC starts. Broader Phase 6 source/API/branch reconciliation remains open.

Heartbeat DTO identity-method correction (2026-10-08): pinned Java reflection confirms `ProducerData`, `ConsumerData`, and `HeartbeatData` inherit `equals`/`hashCode` from `Object`. Equal-field distinct objects compare false and remain separate in a Java Set. Native DTOs no longer expose structural `PartialEq/Eq/Hash`; compile-fail doctests lock this API boundary while nullable vectors preserve identity-distinct rows. The actual oracle and native differential cover all three classes. Fixture SHA-256 at that update: `f32c42d5f1f71f8da573237504e8312cb8495209bf59cb7d54c0960533c0e308`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (manifest22), differential self-test, fmt and diff checks pass. This closes the named identity-method surface only; Phase 6 remains partial and P6-V04 remains pending Phase 19.

VIP helper correction (2026-10-08): direct Java 5.5.1 cases show `MixAll.brokerVIPChannel` splits at the last colon, parses a signed Java int, subtracts 2 with Java overflow, and returns the host portion unchanged without validating it. Rust `broker_vip_channel` now uses the shared Java decimal parser and wrapping subtraction; the transport layer handles socket validity. Fifteen new Java edge cases and three existing hostname/IPv6 cases match; Rust preserves the split-versus-number-parse distinction with separate typed errors. Fixture SHA-256: `ce2afec05859a761ce2fd82a9034ef147b333f077a999b03d17209a305254fb0`; full Phase 6 source/API/branch reconciliation remains open.

Route DTO input continuation (2026-10-08): the pinned Java DTO parser and native decoder now match across 59 actual cases, including twelve rejected syntax/shape inputs, five Unicode decimal string/map-key rejections, six accepted leading-plus/zero-prefixed integer/decimal/exponent values, three rejected NaN/Infinity/hex tokens, and eight whitespace/BOM cases. Fastjson accepts form feed at document/value/punctuation positions and one BOM at document start; it rejects vertical tab, NBSP, and a BOM within the document. Route parsing retains the tested JSON5-style rejection boundaries; the Rust Java-compatible parser normalizes accepted numeric values and form feed outside quoted strings and strips only a leading BOM. SOCKS construction has its own separate grammar oracle. Route fixture SHA-256 `55ff6cf477d53e78f5715294613e3a98d21796b729118a04feb999b213c17245`; [ADR-0014](../architecture/ADR-0014-route-dto-fastjson-input.md) records the exact scope. Broader Phase 6 source/API/input reconciliation remains open.

Route DTO integer-string input checkpoint (2026-10-09): seven direct Java `MessageQueue.queueId` inputs establish that `+2`, `002`, `-1.9`, and `1e2` convert to 2, 2, -1, and 100; whitespace-padded and out-of-range values reject. Native Fastjson coercion matches each without a production change. Probe/regression commit `6493e76`; at this 66-input checkpoint route fixture SHA-256 was `3e96c4cbb68dcc8f83f5390106c4ce173afac0fc69826c81a6aa003a192d786a`.

Runtime `ClientConfig` mutability update (2026-10-08): Java's manager clones caller input, `MQClientInstance` retains the cloned object, and `getClientConfig()` returns that same mutable object. Rust `ClientRuntime::config()` now returns a shared `Arc<RwLock<ClientConfig>>`; runtime-created APIs and namespace hooks use it for audited live reads. Transport options, concurrent executor construction/width, and start-time schedules retain Java's constructor/start capture boundaries. The Java fixture records getter identity and mutation visibility; native tests cover language, namespace hook, RPC timeout, VIP port and heartbeat V2/concurrency behavior. Fixture SHA-256 `ad49c9b917811d1b542bf7f8b2af9ffa07ba3cde22105a00a69bd02f2ade093a`; [ADR-0037](../architecture/ADR-0037-live-runtime-client-config.md) records the behavior. Whole Phase 6 reconciliation remains open; P6-V04 remains pending Phase 19.

`MessageQueueRouteState` public enum API update (2026-10-08): Java `Expired`, `ReadOnly`, `Normal`, and `WriteOnly` values/order, generated enum methods, defensive values, and lookup failures are mapped to public `remoting::MessageQueueRouteState`. The direct native differential invokes all cases. No pinned artifact call site consumes this enum, so the mapping does not claim route-state transition behavior. Current client-config fixture SHA-256 `be97b56070efbdc49ed2294b06f36f6848a152ddb310fba30d349a32a95b52f3`; [ADR-0035](../architecture/ADR-0035-message-queue-route-state-enum-api.md) records the scope.

`RequestSource` public enum API update (2026-10-08): direct Java fixture calls cover all four enum values, generated/inherited methods, exact lookup errors, `getValue`, `isValid(Integer)`, `parseInteger(Integer)` boundaries, and `SYSTEM_PROPERTY_KEY` (23 enum/integer cases plus the constant). Rust now exposes the matching `remoting::RequestSource` methods. No pinned 5.5.1 client/common/remoting class references this enum outside its own declaration, so no system-property activation path is claimed. Fixture SHA-256 at that update `1544344bd90fbefe577643e3ec7af1a8211dcc7bc780c2f39fef7e5ca9fcd391`; [ADR-0034](../architecture/ADR-0034-request-source-java-enum-api.md) records the API mapping.

`QueryAssignmentResponseBody` Set edge update (2026-10-08): the pinned Java decoder now covers 12 response cases, including same-queue/different-mode retention. A direct in-process Java probe computes attachments to force an equal hash between two queue-equal assignments using that JVM's enum identity hash; `HashSet` then deduplicates the pair. Native `HashSet` keeps its all-fields equality/hash contract and does not claim this process-local outcome. Lookup fixture SHA-256 `428599d773432f8a93edf86723e3594ade337a383fd16fbb6a5f4e99315bc8c3`; [ADR-0019](../architecture/ADR-0019-assignment-response-set-input.md) records the behavior and limit.

`NettyEventType` API update (2026-10-08): Java `CONNECT`, `CLOSE`, `IDLE`, `EXCEPTION`, and `ACTIVE` order, generated/inherited enum methods, defensive `values()` behavior, and null/unknown/case-mismatched name lookups are mapped to public `client::NettyEventType`. The native event queue stores this public type directly and 12 direct Java cases pass. Fixture SHA-256 at that update `1fe6cf206b07e5f304634d7128ca4ca4c56a0114f010f21a0eeebb785529e3b5`; [ADR-0033](../architecture/ADR-0033-netty-event-type-java-enum-api.md) records the mapping. Java `NettyEvent.getChannel()` has no native callback socket-handle equivalent.

`RemotingCommandType`, `RequestType`, and `RemotingCommand.getType()` update (2026-10-08): direct Java 5.5.1 cases invoke both enum method surfaces, `RequestType.valueOf(byte)` for signed boundaries and the unknown→null outcome, and request/response type selection before and after response marking. Rust now exposes those enum types and `get_type()`; the stream hook obtains `ReqT` from `RequestType::Stream.get_code()`. Fixture SHA-256 at that update `6d02747d6a267631d2bf98d064967bead5a4a168411a28cb15f34bfa46d0c764`; [ADR-0032](../architecture/ADR-0032-remoting-command-enum-java-api.md) records the mapping. Full Phase 6 source/API/input reconciliation remains open.

`TlsMode` public API update (2026-10-08): the pinned Java fixture invokes `values`, `valueOf`, inherited enum methods, `getName`, and `parse` for all constants and null/exact/case/whitespace/unknown inputs. Rust now exports the matching enum surface and uses Java's exact fallback parser in TLS transport selection. Fixture SHA-256 at that update `948fcc7ca474d0ea48b39b1f60e075fa2213ff40290d51ec801c14af0e8fbad9`; [ADR-0031](../architecture/ADR-0031-tls-mode-java-enum-api.md) records the method mapping. End-to-end TLS provisioning remains Phase 19 work; full Phase 6 reconciliation remains open.

Latest runtime update (2026-10-09): the SOCKS constructor/parser fixture covers 492 curated cases, including comment placement and termination, numeric suffixes, escaped quote forms, malformed escapes, every U+0000–U+00FF escape follower, and supplementary/control probes. Separate exhaustive scans invoke Fastjson for every BMP code unit and all 1,048,576 supplementary scalar escape followers; Java accepts 253,952 supplementary scalars, and native parsing matches every representable outcome. Isolated Java surrogate units cannot be represented by Rust `String`. Fastjson `char1` applies a low-seven-bit lookup to UTF-16 code units; \u reads four UTF-16 units even across supplementary scalars, and native parsing preserves a valid resulting pair while normalizing an unpaired low unit to U+FFFD. See [ADR-0036](../architecture/ADR-0036-fastjson-surrogate-boundary.md) and [ADR-0038](../architecture/ADR-0038-fastjson-socks-escape-grammar.md). Java's `NettyRemotingClient.start/invokeSync` completes local username/password and no-auth SOCKS5 remoting round trips through scripted proxies. The fixture verifies negotiation, target address, request code/body, successful response, client-returned opaque correlation, and bidirectional traffic; paired Rust tests cover both paths. The `copyFields` oracle snapshots all 33 non-internal ClientConfig fields on source, clone, and reset; native snapshots match. Current client-config fixture SHA-256: `bf9b63dfb9c38a4f4090a4326a5d14bc9bff62478e4df4330825cb5009bc08f3`. Broader Fastjson grammar, other transport/address variants, and full Phase 6 source/API/input reconciliation remain open; Phase 6 is PARTIAL and P6-V04 remains pending Phase 19.

`ClientConfig.toString()` update (2026-10-08): native `Display` matches 16 actual Java 5.5.1 cases for null and populated fields, namespace-cache state, raw quote handling, signed numeric boundaries, and all 14 `LanguageCode` values. Java's text includes the private `namespaceInitialized` field and raw `socksProxyConfig`; Rust `Debug` remains separately redacted. `client_config_to_string_matches_invoked_java` passes. The fixture SHA-256 at that update was `ff0edae74d25a91bd4a54ca2c01e06c12715d891fe7cbaff956a7a1bb06b2578`; the current fixture hash is recorded in `PHASE_6_RUNTIME_INVENTORY.md`. Broader Phase 6 API/source/input reconciliation remains open.

`ClientConfig.queuesWithNamespace` and `MessageQueue` setter update (2026-10-08): six actual Java cases confirm the method returns the exact collection, skips null rows when namespace is null/empty, and throws on a null collection/element only when iteration is required; earlier row mutations remain visible after the null-element exception. The Java queue setters preserve nullable topic/broker strings and signed IDs. Native slice identity, field outcomes, and error branches match through `queues_with_namespace_matches_invoked_java_collection_and_null_behavior` and `message_queue_setters_match_invoked_java`. Fixture SHA at that update: `4c0b831de2f3139b5be32e59fede60d639296e8dbffa20a9924c08fbabdab4bb`.

`ClientConfig` getter/setter update (2026-10-08): Rust exposes an idiomatic getter/setter pair for all 33 non-internal Java settings. The Java oracle invokes all getters/setters on source, clone, and reset values; native accessor snapshots match Java getter results and reflected fields, including nullable strings/enums and signed primitive boundaries. Fixture SHA-256 `2824c98496001ab46cc7137400359f51b2afe443d6fee779946606d78f379463`; [ADR-0012](../architecture/ADR-0012-client-config-accessors.md) records the mapping. Broader source/API/input reconciliation remains open.

`MessageQueue` method update (2026-10-08): three actual Java cases invoke equals, hashCode, toString, and the no-argument constructor across defaults, populated text, supplementary Unicode, and signed queue IDs. Rust `Default`, same-type `PartialEq`, Java hash helper, and `Display` match; the route fixture SHA-256 is `9b3f4be54464f7718632f7bcd7d5bde88559234f2c42141a15f9f48d48bc3a1d`. [ADR-0013](../architecture/ADR-0013-message-queue-java-methods.md) records the public method mapping. Broader source/API/input reconciliation remains open.

Heartbeat DTO update (2026-10-08): actual 5.5.1 Fastjson input with repeated nulls confirms Java `HashSet<String>` and `HashSet<Integer>` retain one null member in `tagsSet` and `codeSet`, then emit that member as JSON null. Java `SubscriptionData.equals` compares these members, `hashCode` adds zero for null, and `toString` emits `null`. Rust `SubscriptionData` now preserves nullable set members with `Option` elements and matches field/JSON/equality/hash/text observations. Fixture SHA-256 `653976b51cbcabb695bcdc76afa9250f4f74cd1068b79efedbced0da7eb203e3`; focused heartbeat tests pass. Phase 6 source/API/input and final plan reconciliation remain open.

Heartbeat nullable DTO Set members: actual Fastjson retains one null row in producer/consumer metadata Sets and nested subscription Sets, serializes it, and deduplicates repeated nulls. ProducerData/ConsumerData remain identity-based (equal non-null rows are retained); SubscriptionData remains value-based. Native heartbeat vectors and nested HashSet now represent nullable members. The actual producer-null fingerprint is `-994804270`; Java throws NullPointerException when fingerprinting null consumer or SubscriptionData rows, matched by typed native errors. Fixture SHA-256 `6299d2625866d7c72eb0c97c17bcfc3dcd8bcab9cf382f4c04d1174e1ff5106f`; full workspace, strict Clippy, Maven and differential gates pass. Broader Phase 6 reconciliation remains open.

Route mapping input update: actual `TopicQueueMappingInfo` rejects `currIdMap` null keys/values with NullPointerException and malformed/overflow numeric inputs with Fastjson `JSONException`; both signed Integer key/value boundaries are accepted. Rust's `BTreeMap<i32,i32>` decoder matches all six acceptance outcomes, while its rejected JSON inputs return serde parse errors. Route fixture SHA-256 `962d63f6f615b552410a5717298cd2f8e10e67be3e26ad897633ce231db6020e`; focused differential and full Maven fixture verification pass. Exact exception-class parity and broader Phase 6 input audit remain open.

Historical SOCKS numeric/scalar extension `457d16b`/`37af420`/`cc6422e`/`09d0878`/`6c4d59b` expanded the pinned constructor/parser differential to 73 `socksConstruction` cases. Twelve actual runtime inputs cover exponent thresholds, signed/unsigned leading-dot syntax, decimal scale, overflow, and underflow. Java formats `1e-4` as `1.0E-4`, `1e-7` as `1.0E-7`, `1e7` as `1.0E7`, keeps `1e6` as `1000000.0`, preserves `1.2300`, and rejects unsigned `.5e-3` while accepting signed forms. Native coercion matches all 73 constructor and reflected proxy-map outcomes, including exact decimal text, signed zero and infinity. Its fixture hash `a1054ec9af61312d25037da4470d17629babee026d4e6da6371cb9555b81e175` is superseded by the latest summary above. Broader Fastjson grammar and transport/address variants remain open; Phase 6 is PARTIAL and P6-V04 remains pending Phase 19.

Public `BrokerData` equality/hash/comparison updates `04848b0`/`dcfa7da`/`2c6a476`/`9dfaa53`/`ac3b604`/`533600a`/`13248bf`/`cec1821`/`c7b32bc`/`f0607f9`/`20cff94`/`e6a3389` capture six equals pairs, six raw UTF-16 compareTo pairs, three exact hashCode values, and three route-change sort outcomes. Rust `PartialEq`, `java_hash_code`, fallible `java_compare_to`, and route sorting now match these cases, including metadata-ignored equality and null-name sort errors.

Public route DTO method update `329b0f9`/`b04b2d9`/`1df78b5`/`f6add71` adds direct `QueueData` method parity. Five actual Java cases cover its all-field equality, hashCode, toString, raw UTF-16 compareTo results and null-name exception. Six `TopicRouteData.topicRouteDataChanged` cases now cover both QueueData and BrokerData sorting, including null names and reordered lists. Rust `QueueData` implements the exact hash/text methods and a fallible comparator, and route-change sorting uses both DTO comparators. Final route fixture SHA-256 `6ebf2aa430d170e84b3eae36baffb359a4a7e8e148acaca4ecb27e1bb7c61c8e`; full workspace/all-target tests, strict all-target/all-feature Clippy, offline Maven verify, comparator/manifest22, fmt and diff pass. Remaining DTO methods/input combinations and the broader source/API audit remain open; Phase 6 is PARTIAL and P6-V04 remains pending Phase 19.

Static mapping DTO update `18f8565`/`af642aa`/`38ec7a8` captures four actual Java `TopicQueueMappingInfo.equals/hashCode/toString` pairs across constructor defaults, explicit null versus empty maps, populated IDs and dirty-state changes. Rust equality matches; `java_hash_code` reproduces the exact Java map/string hashes. `java_to_string` matches the captured text and emits currIdMap keys in deterministic Rust order; Java's ConcurrentHashMap iteration order is unspecified. Final route fixture SHA-256 `dae0e00eaa1d698f00b3f6fe1a9fae1905a2b273bd70369a0b359a09ef926299`; full workspace/all-target tests, strict all-target/all-feature Clippy, offline Maven verify, comparator/manifest22, fmt and diff pass. Other static mapping DTO inputs and whole Phase 6 API reconciliation remain open.

`TopicRouteData.equals/hashCode` update `f9f6450`/`c7d8c01`/`21cc626` captures four Java pairs for defaults, explicit null versus empty collections, metadata-only BrokerData changes, and a changed queue. Rust's existing derived equality matches the nested Java DTO contract, and `java_hash_code` matches all exact hash values using each field's Java collection/string hash. Final route fixture SHA-256 `3dc47fdc1e774357747853f4dfe270f4f1684a8a302d5502f905109a35b76891`; full workspace/all-target tests, strict all-target/all-feature Clippy, offline Maven verify, comparator/manifest22, fmt and diff pass. `TopicRouteData.toString` and unrepresented nullable collection elements remain in the wider route DTO/input audit.

Actual `checkClientInBroker()` null-topic case `d591091`: with a non-TAG expression, Java throws before route lookup or Broker RPC. Native returns typed `InvalidRoute` and makes no network request. Fixture SHA-256 `73990a351770ca0d8451590eef16bc3931115f90853193f37f3e4e18d61c841d`; focused regression and pinned Maven verify pass.

Null consumer subscription runtime preparation `1aac047` directly invokes the pinned method: full mode throws NullPointerException on a registered consumer returning null from `subscriptions()`, and simple mode builds empty subscription metadata. Native private preparation regression and Maven fixture verification pass. Heartbeat fixture SHA-256 `189e4a80c6f7f02083efc15dfe39535321872860a3d058c77eca975564193036`; wider Phase 6 audit remains open.

Public `HeartbeatData.computeHeartbeatFingerprint()` is represented by `HeartbeatData::compute_heartbeat_fingerprint()` (`91702a2`/`3c4f444`). Its protocol regression matches seven invoked Java 5.5.1 runtime values, including the JSON round-trip defaults used for null collections. Runtime fingerprinting delegates to this method. Heartbeat fixture SHA-256 `118b49612461931280cf65e7912ecceb7deb1d30869a82e7e2d27b71b8afc53c`; full workspace, pinned Maven and strict Clippy pass. Broader Phase 6 API audit remains open.

Null subscription route refresh `b57594c`/`98556c1` invokes the actual unstarted 5.5.1 runtime with null and named subscriptions. It issues both lookups, omits the null topic ext field, continues after rejected route, and fails before cache publication on a successful null-topic route. Route fixture SHA-256 `a8c6be7d5fb98f4a660f15835dfea6c74c03464c63d958c48f0d5c8c227a4b1d`; socket red/green, workspace, pinned Maven and strict Clippy pass. Broader Phase 6 API audit remains open.

Null fingerprint and primitive decoding `4cbd803`/`5fe4045` match four actual Java 5.5.1 fingerprint calls and five explicit-null primitive inputs. Java's JSON clone restores null collections to empty before hashing; explicit-null primitive values become zero/false while absent SubscriptionData subVersion still starts at wall time. Fixture SHA-256 `118b49612461931280cf65e7912ecceb7deb1d30869a82e7e2d27b71b8afc53c`; full workspace, pinned Maven and strict Clippy pass. Null-topic runtime routing and broader Phase 6 audit remain open.
Null fingerprint and primitive decoding `4cbd803`/`5fe4045` match four actual Java 5.5.1 fingerprint calls and five explicit-null primitive inputs. Java's JSON clone restores null collections to empty before hashing; explicit-null primitive values become zero/false while absent SubscriptionData subVersion still starts at wall time. Fixture SHA-256 `118b49612461931280cf65e7912ecceb7deb1d30869a82e7e2d27b71b8afc53c`; full workspace, pinned Maven and strict Clippy pass. The then-open null-topic runtime routing is resolved by the newer oracle above; broader Phase 6 audit remains open.

HeartbeatData public collections `6b355b8`/`41a5fd2` match three invoked Java missing/null/populated decode and JSON cases, preserving constructor-empty sets, explicit null and consumer multiplicity. Initial heartbeat fixture SHA-256 `970674db0da0cc0a3e051f13082c30276113c10dff86309c349e0cd05f975bbb`; workspace, pinned Maven and strict Clippy pass. The then-open fingerprint and primitive-null branches are resolved by the newer oracle above; Phase 6 remains PARTIAL.

ProducerData/ConsumerData public model `f8b745a`/`633a4a5` matches six invoked Java missing/null/populated DTO cases. Native heartbeat preparation also follows the pinned source's null subscription-set failure/empty-simple branches, tested privately. Heartbeat fixture SHA-256 `ce4d66d25c1031e36531ac184125120d480b3f715ffa3c944e63155c16cfc030`; workspace, pinned Maven and strict Clippy pass. At that checkpoint, HeartbeatData collection aliases remained open and were later closed by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md); other input branches remain open and Phase 6 is PARTIAL.

SubscriptionData public model `1dce870`/`40f6f65` now matches three invoked Java missing/null/populated field and JSON cases, including nullable strings/sets and current-time default subVersion. Heartbeat fixture SHA-256 `b257ae6b52a097a871279df308e985849db0844b20fd78182e59c8f1fb488ace`; focused red/green, workspace, pinned Maven and strict Clippy pass. Null-topic runtime routing and other heartbeat DTO inputs still require audit; Phase 6 remains PARTIAL.

Overlapping static owner cases `cb33e1b` originally compared three Java endpoint conversions. The later ten-case continuation at the top expands sparse/disjoint/overshoot IDs, per-ID epoch precedence, aliased scopes, epoch threshold and nullable owner boundaries; equal-epoch selection remains subject to Java HashMap order. Broader malformed inputs and Phase 6 remain PARTIAL.

MessageQueue `compareTo` `c2c34e3`/`b001f65` is covered by eleven invoked 5.5.1 cases and public fallible native `java_compare_to`. Pinned tag commit `efa1be59` and the jar both use `Integer.compare` for queue IDs; the earlier source/artifact divergence claim mistakenly read the separate checkout at 5.3.3 HEAD. Route fixture SHA-256 `a709df838628378dbca6044027cd873fd0b6a80c90591a24bf395207228c3269`; full workspace, pinned Maven and strict Clippy pass. Internal Rust `Ord` remains total for tree collections; Phase 6 remains PARTIAL.

SOCKS field coercion `34ce95a`/`b3ada16` matches six invoked Java constructor and reflected proxy-map cases for numeric, Boolean, null and empty-array route fields. Config fixture SHA-256 `eefbb3c681864effb187cbabcef7e70750b079876d5e45e138d83f6aa6a5264d`; native red/green, full workspace, pinned Maven and strict Clippy pass. Other Fastjson grammar and connection paths remain open; Phase 6 is PARTIAL.

MessageQueue public DTO correction `3577198`/`e242764` represents Java nullable topic/brokerName, JSON omission, and absent queueId zero. Fixture SHA-256 `e5520a19c71483fce328312f9d4263c6af7a0bfff4c712e40660ca90626c3453`; native red/green, full workspace, pinned Maven and strict Clippy pass. Null compareTo and broader API/input reconciliation remain open; Phase 6 is PARTIAL.

Nested route DTO correction `132f838`/`02e95f2` covers nullable BrokerData/QueueData names, queue primitive defaults, nullable MessageQueue broker name in subscription conversion, and Java refresh failure/success branches. Route fixture SHA-256 `20f8e96c893ccfad3fc66e1b5057d4123fb3a70f1d856c652429a088f697793e`; Rust workspace, pinned Maven and strict Clippy pass. Other public DTO/input reconciliation remains; Phase 6 is PARTIAL.

Registered-consumer null route case `c4b6e17` adds actual Java failure/no-cache evidence and a matching scripted native runtime check for ordered null queue with a consumer. Route fixture SHA-256 `e9303fc3ceac0fc13ddf6181163847c3b85c1d2c684f9a3fd4c71ab1968f82d5`; full workspace, pinned Maven and strict Clippy pass. Phase 6 remains PARTIAL for broader API/source/input reconciliation.

Nullable route refresh correction `2b0d6f0`/`65eb745` follows five invoked unstarted Java runtime outcomes, including ordered/static null queue success and normalized cached collections. Fixture SHA-256 `d612e9b9a06c83fec863ce543b04ffa06f7b48b6a61238d75fe2c96c7a23af1a`; focused red/green, full workspace, Maven and strict Clippy pass. The later 2026-10-09 continuation confirms ordered null-queue failure and static-mapping success with a registered consumer; other malformed route cases remain open, and Phase 6 is PARTIAL.

Static mapping DTO correction `f1f53f4`/`9285a7a` covers Java missing/null fields, constructor defaults, JSON omission, and four invoked endpoint conversion cases. The later ten-case overlap/scope matrix is recorded at the top; other malformed route combinations and full Phase 6 source/input reconciliation remain open.

Route mapping JSON omission `680798f`/`044de86` matches actual Java `TopicRouteData.toJson()` for an absent `topicQueueMappingByBroker` field. Fixture SHA-256 `9268caad9acde84decaeb6eee29afea3c77a1867cc3cb6fc54e4e757ff18cfb0`; native red/green, workspace, Maven and strict Clippy pass. Phase 6 remains PARTIAL.

Null route JSON correction `d6a44ad`/`09af643` supersedes the earlier wording below: explicit null stays null in the decoded public DTO, and Java/default Rust JSON output **omits** that field. Re-reading the omitted field initializes an empty collection. Java fixture SHA-256 `dd857cbf868945eecc96f05b8d041c5191aec430594e15daca69edce8a875ab0`; native red/green golden, full workspace, Maven and strict Clippy pass.

Nullable route collections `0d8daec`/`86af455`: the public `TopicRouteData` preserves explicit null queue, broker and filter collections on decode/serialize, and route comparison/cache publication use the Java copy constructor's empty normalization. The Java fixture, native focused checks, full workspace and strict Clippy pass. The later 2026-10-09 continuation adds registered-consumer static-mapping behavior; the broader source/input audit remains open, and Phase 6 remains PARTIAL.

Nullable command language `40bd7cf`/`63b7feb` supersedes the historical missing-language clause in the Phase 6 row. Public config and command models now retain null, JSON omission versus explicit null decoding follows the invoked Java fixture, binary null encode errors, and heartbeat hook/wire tests pass. Both confirmed nullable lower-model mismatches are corrected. Phase 6 remains PARTIAL for the remaining API/source/input audit; P6-V04 remains pending Phase 19.

Phase 6 nullable Broker address correction `8fb1ec8` supersedes the historical missing/ignored Broker DTO clause in the Phase 6 row below. The public protocol DTO and route snapshot now preserve null values; the actual Java route golden, lookup/cleanup and scripted heartbeat tests pass without ignore. Nullable command language is also implemented; Phase 6 remains PARTIAL because broader source/API/input reconciliation is unfinished. P6-V04 remains pending Phase 19.

## Baseline

- Java client: `org.apache.rocketmq:rocketmq-client:5.5.1`
- Source tag/commit: `rocketmq-all-5.5.1` / `efa1be59d6fc747ad220f036c86030f05f0419f9`
- Server baseline: 5.3.4 (`V5_3_4`), reserved for live validation
- Source paths, artifact hashes and tool versions: `compat/java-reference/README.md`
- Status vocabulary: implementation `TODO / IN_PROGRESS / PARTIAL / UNIT_VERIFIED / COMPATIBLE`; live `PENDING_PHASE_19 / PASS / FAIL / BLOCKED`. Fixture existence is not implementation or live-server evidence.

## Phase 1 model and registry status

Phase 1 provides the initial Rust 2024 workspace (`rocketmq-classic-common` and `rocketmq-classic-protocol`), pinned to Rust 1.85.0/MSRV 1.85. The common crate contains message/property models and message/pull flags; protocol contains the full pinned 166-request/68-response integer registry, language and serialization tags, route conversion models, and heartbeat JSON models. Unit coverage cross-checks the Phase 0 fixture sidecars, Java UTF-16 queue ordering, malformed property handling, static/ordered route cases, Java-compatible JSON field names, signed values, and flag preservation. Registry and offline-fixture coverage are not client feature completion or live Broker validation.

## Phase 2 Remoting codec status

The `rocketmq-classic-remoting` crate now contains JSON and RocketMQ private-header codecs plus bounded incremental full-frame encoding/decoding. Public `frame::{encode_header, encode_header_for_body, encode_header_into}` also map Java `RemotingCommand.encodeHeader()`, `encodeHeader(int)`, and the append-buffer behavior of `fastEncodeHeader(ByteBuf)`: signed body lengths retain Java `int` wrapping, and the prefix is returned without the body. The invoked `remotingHeaderEncodingBehavior` fixture and `header_prefix_apis_match_invoked_java_551` compare both codecs at negative, zero, one, and signed-`int` boundary body lengths, the no-argument form, and appending after existing bytes. The 16 MiB maximum inbound frame limit includes the four-byte prefix, matching Java Netty's default. The pinned Java `NettyDecoder` reads `com.rocketmq.remoting.frameMaxLength` at class initialization; Rust reads the same-named process environment setting when creating its first connection and applies it to inbound decoding. Java's `NettyEncoder` does not use that decoder limit, so Rust outbound encoding allows a frame larger than the configured inbound limit while respecting the signed Java length-prefix range. The runtime fixture probes Java's default and configured `4096` values in separate JVMs, verifies the Java encoder emits a frame larger than `4096`, and a Rust subprocess loopback test verifies that a configured connection rejects an oversized inbound frame while allowing that outbound frame size. Java fixture coverage includes transaction, request/reply, large request-code narrowing evidence, and optimized/ordinary `FastCodesHeader` equivalence. Maven verify also decodes two Rust-generated frames using the pinned Java client. P2-V01/V02/V03 are unit-verified; this does not represent live Broker execution or complete client compatibility. Java absent/empty remark and body distinctions that are not represented on the wire are documented in the crate codec behavior.

Custom-header decoding now invokes the Rust `CommandHeader::check_fields` hook after decoding fields, matching Java `RemotingCommand.decodeCommandCustomHeader` followed by `CommandCustomHeader.checkFields`. The invoked `commandCustomHeaders.checkFields` fixture and Rust regression cover `CreateTopicRequestHeader` filter types, transaction status values, and valid, negative, or missing values on a custom header. This closes the named validation cases only; it does not close the broader P18-V01 API/input/control-flow audit.

## Initial bidirectional inventory

The matrix starts with the source areas required by the master plan. Work on each phase branch must add each public method/overload, inherited/deprecated setting, source branch, configuration default/type/unit/range/validation, Rust mapping, fixture and unit-test ID as applicable. Commits within a phase branch may record partial progress; they do not signal phase acceptance. Phase 18 closes both directions, including proof that Rust exposes no Java-incompatible public setting. `TODO` means not yet implemented; `PARTIAL` means required behavior remains; no feature completion is claimed here.

| Java class/source (5.5.1 tag) | Java method/setting scope | Rust mapping / test ID | Request code / fixture | Implementation | Live / server |
|---|---|---|---|---|---|
| `remoting/.../protocol/RemotingCommand.java` | `encode`, `decode`, `headerEncode`, `headerDecode`, `encodeHeader()`, `encodeHeader(int)`, `fastEncodeHeader`, response/oneway bits | `crates/rocketmq-classic-remoting/src/{command.rs,codec/frame.rs,codec/json_header.rs}`; P2-V01–V03 plus `header_prefix_apis_match_invoked_java_551` | route/heartbeat/send/pull/offset/lock/transaction/reply fixture families; `remotingHeaderEncodingBehavior` | UNIT_VERIFIED | PENDING_PHASE_19 / 5.3.4 |
| `remoting/.../protocol/RocketMQSerializable.java` | `rocketMQProtocolEncode/Decode`, `mapSerialize/Deserialize`, `writeStr/readStr` | `crates/rocketmq-classic-remoting/src/codec/rocketmq_header.rs`; P2-V01/V02 | dual-codec Java fixtures, private-large-code/version and edge-cases/private-large-key (signed-16-bit narrowing), FastCodesHeader equivalence | UNIT_VERIFIED | PENDING_PHASE_19 / 5.3.4 |
| `remoting/.../netty/NettyEncoder.java`, `NettyDecoder.java` | `encode/decode`; 16 MiB default inbound limit and `com.rocketmq.remoting.frameMaxLength` system property | Public `codec/frame.rs::{encode, encode_into, decode_next}` maps command/frame operations without a caller-selected bound; `client/config.rs::java_frame_max_length` applies the Java property to inbound connections only; outbound uses the signed Java length bound; P2-V02/V03 | frame limit/fragmentation tests; default/configured Java probes including encoder-over-limit case; Rust loopback verifies configured inbound rejection and larger outbound encoding; compile-fail doctests guard the removed per-call limit and private default constant | UNIT_VERIFIED | PENDING_PHASE_19 / 5.3.4 |
| `remoting/.../protocol/FastCodesHeader.java` | `encode/decode` fast custom-header path | Java fixture generator `writeFastHeaderFixtures`; P2-V01 | `header-fast-001.bin` vs `header-ordinary-001.bin` | UNIT_VERIFIED | PENDING_PHASE_19 / 5.3.4 |
| Phase 2 Rust → Java decode | Java `RemotingCommand.decode` | `compat.Main.verifyRustFixtures`; Maven verify | `fixtures/remoting/rust-generated/*.bin` | UNIT_VERIFIED | PENDING_PHASE_19 / 5.3.4 |
| `remoting/.../protocol/RequestCode.java` | Complete 166-name public numeric request registry | Phase 1; Java reflection fixture and `code_registry::public_code_registries_match_all_pinned_java_constants` | `REQUEST_CODES.md`, `fixtures/protocol/request-response-codes.json` | UNIT_VERIFIED — all names and values match pinned Java; registry presence does not imply RPC support | PENDING_PHASE_19 / 5.3.4 |
| `remoting/.../protocol/ResponseCode.java`, `RemotingSysResponseCode.java` | Complete 63-name response registry plus 5 system response names | Phase 1; Java reflection fixture and `code_registry::public_code_registries_match_all_pinned_java_constants` | `REQUEST_CODES.md`, `fixtures/protocol/request-response-codes.json` | UNIT_VERIFIED — all names, values, and intentional duplicate-value prefixes match pinned Java | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/MQClientAPIImpl.java` / `impl/mqclient/MQClientAPIExt.java` | Phase 5 one-attempt typed RPC calls and response interpretation, including Phase 18 notification, Lite-topic-info, and full async/oneway extension surfaces | `crates/rocketmq-classic-core/src/api/`; runtime config refresh, domain/mode/error tests, and `rpc_invoked*` scripted peers | 202 pinned invoked cases and SHA-256 fixture inventory; ordinary/Lite pull, all send modes, lifecycle, raw invoke, nullable offset futures, callback/oneway variants, notification and Lite-topic-info DTO/RPC families | UNIT_VERIFIED (Phase 5 boundary): P5-V01/V03/U01 plus Phase 18 extension-API closure; complete scoped public/method/branch inventory in `PHASE_5_OFFLINE_METHOD_MATRIX.md` | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/factory/MQClientInstance.java` | shared runtime, routing, heartbeat, lifecycle | `core/src/runtime`, `core/src/route/cache.rs`; [Phase 6 inventory](PHASE_6_RUNTIME_INVENTORY.md), [requirement audit](PHASE_6_REQUIREMENT_AUDIT.md), API/config and branch/resource audits | P6-V01/V02/V03/U01 gates; oracles/scripted peers; approved Vec/SOCKS/callback corrections; live owner tables/group unregister; live raw topic table/derived separation/metadata equality/unchanged-invalid fix; 21 raw cleanup input shapes; 399 route DTO, 276 heartbeat DTO inputs, 114 Lite subscription DTO plus 3 identity-method cases, 46 LockBatchRequestBody, 46 UnlockBatchRequestBody, 25 LockBatchResponseBody, 25 consumer-list, 37 assignment response-body, 52 QueryAssignmentRequestBody, 49 SetMessageRequestModeRequestBody, 43 CheckClientRequestBody, 4 ConsumerRunningInfo raw Fastjson decode inputs, 499 SOCKS constructor, 21 SOCKS field-shape and 32 SOCKS proxy-selection cases, all-BMP token observations, every representable BMP and all 1,048,576 supplementary scalar escape followers, 49 endpoint-shape cases, all legal ports, BMP port digits and all Unicode scalar host values; exact `NAMESRV_ADDR` constructor fallback; V04 retained | PARTIAL — remaining DTO/input combinations, other malformed raw inputs, other defaults and final whole-plan audit remain incomplete; tested route aliases, DTO coercions and map/view primitives do not prove remaining collection/input parity | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/producer/DefaultMQProducerImpl.java` | validation, send modes, retries, queue routing, auto-batch and batch encoding | Phase 7; see [producer compatibility matrix](PHASE_7_PRODUCER_MATRIX.md) | P7-V01/U01 | UNIT_VERIFIED: Java fixture and five scripted TCP loopback tests passed, including retries, mode policies, batch paths and lost-ack duplicate delivery | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/consumer/PullAPIWrapper.java` | pull results, decoding, filtering | Phase 8; see [Phase 8 offline method matrix](PHASE_8_OFFLINE_METHOD_MATRIX.md) | P8-V01–V03 | UNIT_VERIFIED | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/consumer/DefaultMQPullConsumerImpl.java` | explicit pull and offset control | Phase 9; see [Phase 9 offline method matrix](PHASE_9_OFFLINE_METHOD_MATRIX.md) | P9-U01 | UNIT_VERIFIED | PENDING_PHASE_19 / 5.3.4 |
| `client/MQHelper.java` | deprecated/default-instance and explicit-instance `resetOffsetByTimestamp` overloads | Phase 10; [offline method/configuration matrix](PHASE_10_OFFLINE_METHOD_MATRIX.md); `fixtures/consumer/facade-api.json`; `mq_helper::tests` | Java reflection pins both overloads; Rust tests cover ordered queue search, negative-offset filtering and fail-fast errors | UNIT_VERIFIED — `MqHelper` starts a Pull owner, searches and updates each fetched queue, persists the fetched set, and shuts down | PENDING_PHASE_19 / 5.3.4 |
| `client/.../consumer/DefaultMQPullConsumer.java`, `impl/consumer/RebalancePullImpl.java` | Pull allocation/listener settings, topic and subscription registration, active assignment query and Pull rebalance lifecycle | Phase 11; see [Phase 11 offline method matrix](PHASE_11_OFFLINE_METHOD_MATRIX.md) | P11-U02 | UNIT_VERIFIED — Java defaults, namespace handling, broadcast/clustering assignment, queue callbacks and safe offset removal are covered | PENDING_PHASE_19 / 5.3.4 |
| `client/.../consumer/ProcessQueue.java` | message ownership, offsets, lifecycle and expired-message cleanup | Phase 10; `crates/rocketmq-classic-consumer/src/process_queue.rs`; [Phase 18 ProcessQueue audit](PHASE_18_REBALANCE_PROCESS_QUEUE_API.md); [ADR-0083](../architecture/ADR-0083-process-queue-offset-ownership.md) | P10-V01 duplicate/gap/out-of-order/drop-generation tests; P10-V02/U01 scheduling/listener checks; Java expiry fixture covers send-back retry and malformed timestamp | UNIT_VERIFIED — ordered queue state, offset accounting, generation fencing and expiry error/retry behavior pass local checks; Broker retry/DLQ behavior remains P10-V03 in Phase 19 | PENDING_PHASE_19 / 5.3.4 |
| `client/.../consumer/DefaultMQPushConsumer.java`, `impl/consumer/DefaultMQPushConsumerImpl.java`, `impl/consumer/ConsumeMessageConcurrentlyService.java` | Push configuration/defaults, listener and subscription accessors, send-back overloads, direct pool-size update, pause/orderly status, `queryConsumeTimeSpan`, and lifecycle | Phase 10; [offline method/configuration matrix](PHASE_10_OFFLINE_METHOD_MATRIX.md); P10-V04; `tests/push_pipeline.rs` | P10-01/02/05/09; P10-V04; direct API and worker-bound tests | UNIT_VERIFIED — accessors, Java-bounded direct pool resizing, namespaced subscription copy, ordered direct-route consume-time-span query with VIP handling, send-back/fallback path, reversible startup, quiescent shutdown, and the returned implementation's 71 public signatures have method-level mappings or reviewed internal-entrypoint dispositions; automatic thread adjustment retains Java's no-op behavior; Phase 11 assignment behavior is locally verified | P10-V03 Broker retry/DLQ validation remains PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/consumer/PullMessageService.java`, `PullRequest.java` | Push queue pull scheduling and request generation | Phase 10; [offline method/configuration matrix](PHASE_10_OFFLINE_METHOD_MATRIX.md); [ADR-0085](../architecture/ADR-0085-push-pull-dispatch-fanout.md) | P10-04/05/08; P10-V02; P11-U01 assignment tests; `crates/rocketmq-classic-consumer/tests/push_pull_fanout.rs` scripted-peer fan-out barrier | UNIT_VERIFIED — owned scheduler, one owned task per admitted Pull request with exactly-once slot release on completion, panic, cancellation and abort, closed admission releasing the slot without an RPC attempt, future-mode Pull under the 30 s client deadline, Pull `suspendTimeoutMillis=15000` kept separate from POP `20000`, pause/resume/cancel, Java flow-control thresholds, pull outcomes, retry delays, failed-send-back retention, generation-safe activation and real assignment reconciliation are covered locally; all 16 assigned queues admit at once where the removed four-worker pool admitted four | P10-V05 live fan-out PASS_RECORDED at clean revision `6d6db7ed` / 5.3.4; broader phase closure remains separate |
| `client/.../impl/consumer/RebalanceImpl.java`, `RebalancePushImpl.java`, `RebalancePullImpl.java`, `RebalanceService.java` | strategy allocation, Push/Pull assignment add/remove, queue listeners and triggers | Phase 11; [offline method/allocation matrix](PHASE_11_OFFLINE_METHOD_MATRIX.md) | P11-V01/V02, P11-U01/U02 | UNIT_VERIFIED — all five Java strategies, initial offsets, Push race fencing, Pull registration and assignment lifecycle, empty-route and unavailable-membership behavior pass local checks | PENDING_PHASE_19 — mixed-client join/leave and bidirectional live Push remain V03/V04 |
| `client/.../consumer/store/RemoteBrokerOffsetStore.java` | remote offset lifecycle | Phase 9; see [Phase 9 offline method matrix](PHASE_9_OFFLINE_METHOD_MATRIX.md) | P9-V01/U01; `tests/offsets.rs` | UNIT_VERIFIED | PENDING_PHASE_19 / 5.3.4 |
| `client/.../consumer/store/LocalFileOffsetStore.java` | local persistence/recovery | Phase 9; see [Phase 9 offline method matrix](PHASE_9_OFFLINE_METHOD_MATRIX.md) | P9-V02/U01; local offset fixture/tests | UNIT_VERIFIED | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/MQClientAPIImpl.java` | search/min/max/earliest-time offset RPCs | Phase 9; see [Phase 9 offline method matrix](PHASE_9_OFFLINE_METHOD_MATRIX.md) | P9-U01; request codes 29–32 | UNIT_VERIFIED | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/consumer/ConsumeMessageConcurrentlyService.java` | concurrent callbacks/ackIndex/retry/send-back | Phase 10; [offline method/configuration matrix](PHASE_10_OFFLINE_METHOD_MATRIX.md) | P10-06/07/09; P10-U01/V04 | UNIT_VERIFIED — Java-fixture tests cover callback success/partial/failure, panic isolation, scripted send-back/fallback metadata and failed-retry retention; shutdown retains unresolved work. Live Broker DLQ remains P10-V03 | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/consumer/ConsumeMessageOrderlyService.java`, orderly `ProcessQueue` methods, `RebalanceImpl` lock branches | Per-queue ordered callbacks, Broker lock/renewal/expiry, retry and offset-safe lifecycle | Phase 12; see [orderly compatibility matrix](PHASE_12_ORDERLY_MATRIX.md) | P12-V01/V02/U01 | UNIT_VERIFIED — public defaults/context/statuses, partial batch locks, per-response lock freshness, retry/rollback, independent and serialized queues, loss, rebalance and shutdown pass local checks | P12-V03 PASS — mixed-client lock handoff, stale-offset prevention, and post-shutdown lock recovery on 5.3.4; run evidence (raw output not retained) |
| transaction producer/check APIs | half message, check, commit/rollback | Phase 14; [transaction matrix](PHASE_14_TRANSACTION_MATRIX.md) | P14-V01/V02/U01 | UNIT_VERIFIED — half-send, end-transaction fields, inbound checks, callback ownership and shutdown are locally covered | P14-V03 PASS — cross-language commit/rollback visibility and bounded Unknown Broker checks on 5.3.4; run evidence (raw output not retained) |
| request/reply APIs | correlation, callback completion and responder | Phase 15; [request/reply matrix](PHASE_15_REQUEST_REPLY_MATRIX.md) | P15-V01/V02/U01/V03 | UNIT_VERIFIED — all six overloads, correlation lifecycle, reply handling, timeout/cancellation races and Java metadata fixtures are locally covered; P15-V03 passes bidirectional 5.3.4 Broker request/reply, timeout, and late-reply checks | P15-V03 PASS (raw output not retained) |
| Lite/POP APIs and configuration | complete public methods and receipt lifecycle | Phase 13; [Lite/POP matrix](PHASE_13_LITE_POP_MATRIX.md) | P13-V01/V03/U01/V02 | UNIT_VERIFIED — Lite API/config audit, POP assignment/receipt lifecycle, expiry and replacement cases pass local checks | P13-V02 PASS — Java/Rust live POP lifecycle on pinned 5.3.4; run evidence (raw output not retained) |
| namespace, hooks, ACL/TLS, trace | settings, validation, ordering and lifecycle | Phase 16; [security and operations guide](../security.md), [Phase 16 API matrix](PHASE_16_NAMESPACE_SECURITY_OPERATIONS_MATRIX.md), ACL/TLS/trace and namespace fixtures | P16-U01; `namespace_util_fixture_matches_java_5_5_1`, `public_topic_validator_surface_matches_pinned_java_fixture`, `fixtures/namespace/namespace-util.json` and `fixtures/namespace/topic-validator.json`; `auth::tests`, `trace::tests`, `message_types_match_java_names_ordinals_and_short_name_fallback`, `consume_message_hooks_wrap_the_listener_and_share_context_state`, `pull_consume_hooks_receive_empty_pull_context_with_java_success_state`, `lite_poll_consume_hooks_run_around_delivery_with_queue_metadata`, `internal_trace_producer_uses_host_namespace_once_and_keeps_namespace_v2`, `lite_internal_trace_producer_uses_host_namespace_once_and_keeps_namespace_v2`, `p16_slow_channel_observer_*`, scripted TLS peers, `namespaced_send_uses_wire_names_and_unwraps_the_result`, `retry_and_dlq_messages_restore_logical_topics_for_orderly_and_pop`, `namespaced_request_wraps_its_topic_once_before_send`, `transaction_half_send_wraps_topic_and_group_once`, `recall_wraps_topic_and_group_once_at_the_namespace_wire_boundary`, `pull_routes_per_queue_returns_decoded_messages_and_only_explicitly_persists_offsets`, `trace_flush_waits_for_scripted_broker_delivery`, `trace_shutdown_waits_for_scripted_broker_delivery` | PARTIAL — ACL HMAC/key-file/YAML fixtures, all public `NamespaceUtil` methods/constants including retry/DLQ, system-resource and dynamic system-topic classification, Push/Pull/Lite hook context defaults and callback timing, TLS positive/negative peers, trace queue/chunk/flush/shutdown delivery, trace internal producer namespace-v2 propagation, Producer request/reply/transaction/recall namespace, Push retry/DLQ restoration and response unwrapping, namespaced offset persistence, and the complete Java MessageType enum have local evidence; the full Java source/API/settings-to-test audit and phase acceptance remain open | P16-V02 PASS — Java/Rust ACL and mutual-TLS outcomes on 5.3.4; live evidence (raw output not retained) |
| MQAdmin/query public API | methods, result models, fan-out | Phase 17; [admin/query matrix](PHASE_17_ADMIN_MATRIX.md) | P17-V01/V02/V03/U01; core admin RPC and Producer/Push/Pull facade cases | UNIT_VERIFIED — all in-scope Java client MQAdmin/query operations and concrete facade counterparts have local evidence in the Phase 17 matrix | P17-V02 PASS — duplicate-key, bounded-time, partial Broker failure, and terminal `NO_MESSAGE` mapping on 5.3.4; run evidence (raw output not retained) |

Phase 16 TLS setting continuation (2026-10-12): pinned `TlsSystemConfig` declares `tls.ciphers` and `tls.protocols`, and `extractTlsConfigFromFile` lets the properties file override JVM values. The invoked Java `TlsHelper.buildSslContext(true)` path returns before `moreTlsConfig`, so those settings do not alter the Java client handshake. Rust `TlsConfig` retains both property values for inspection but likewise does not apply or reject them in the client connector. Java fixtures cover defaults, system properties, file precedence, and the client's inert setting behavior; `java_client_tls_ignores_cipher_and_protocol_properties` and `java_tls_cipher_protocol_settings_match_pinned_fixture_and_file_loading` cover the Rust mapping. This closes the client-side TLS setting mapping only; Phase 16 remains PARTIAL pending the full Java source/API/settings-to-test audit and phase gates.

Correction (2026-10-13): the preceding TLS disposition was based on an incorrect source-path reading. The pinned `TlsHelper.buildSslContext(true)` applies `moreTlsConfig` to client contexts. The regenerated Java fixture records enabled suites/protocols and the emitted ClientHello; a configured TLS 1.2-only cipher limits the ClientHello to TLS 1.2 even when TLS 1.3 is also enabled. Rust now applies both properties and compares ClientHello cipher/version IDs with that fixture. Unsupported names fail context construction, matching Java. The earlier inert-setting test name cited above was stale; `java_client_tls_applies_cipher_and_protocol_properties_to_client_hello` is the current direct check. Phase 16 remains PARTIAL pending the rest of the API audit and phase gates.

Phase 16 async producer hook/retry audit (2026-10-12): pinned `DefaultMQProducerImpl.sendKernelImpl` invokes the after-hook once when an ASYNC `sendMessage` call returns, then pinned `MQClientAPIImpl.sendMessageAsync` invokes it again with the terminal success or error; async retries reuse the same context and do not repeat before-hooks. Rust Future sends now preserve that lifecycle, retry broker-response and transport failures up to `retryTimesWhenSendAsyncFailed` except remoting backpressure rejection, reroute default sends to another Broker when a route is available, and retry explicit-queue/selector sends on their chosen Broker. Tests `async_send_retries_share_one_hook_context_until_terminal_result`, `explicit_queue_future_retries_on_the_same_broker_with_one_hook_lifecycle`, `async_send_hooks_report_submission_and_terminal_failure_once_each`, `scripted_async_and_oneway_failures_keep_their_java_retry_policies`, and `sync_and_future_retry_broker_responses_using_java_policies` cover these branches. Producer crate tests and strict producer Clippy pass; Phase 16 remains PARTIAL pending the remaining source/API/settings-to-test audit, workspace gates and phase commit.

## Phase 3 Remoting transport status

The current Java-to-Rust and Rust-to-Java remoting API/configuration ledger, including concrete method, setting, test-ID, and implementation-difference mappings, is maintained in [`PHASE_3_REMOTING_INVENTORY.md`](PHASE_3_REMOTING_INVENTORY.md). Historical acceptance labels do not waive the current Phase 18 public API/settings and observable-behavior parity contract. The ledger also records the Phase 19 live-server handoff; it does not claim live Broker validation.

Rust remoting implements deadline-based sync-future and duration-based wrapper, async-callback and oneway APIs; response-table scanning starts after 3 seconds and repeats every second, matching Java's nominal scan cadence and `begin + timeout + 1s` cleanup threshold; future-based invoke waits through the scanner cleanup grace while retaining its pending entry/async permit (and can accept a late response); sync invoke returns at its own wait deadline while scanner cleanup remains pending; the NameServer address-list getter preserves Java's unconfigured-null versus configured-list behavior with `Option<Vec<String>>`, and empty list updates preserve the existing configuration; async/oneway semaphore admission; TCP/TLS/SOCKS5 connection reuse; raw-code request processors with reject hook, shared default pool, and optional per-registration executor; request pipeline with owned-request input plus identity-preserving Rust RequestContext (channel state, peer address, close, and immediate no-deadline command write with a future for the local write result), typed abort/error outcomes and RPC hooks (including clear-all and Java-order pipeline composition); idle timeout, basic channel events with constructor-injected immutable listener and GO_AWAY reconnect with one retry under the original deadline; idempotent shutdown. Callback execution has configurable worker count, unbounded callback queue matching Java's public executor backlog, disable-executor mode (which ignores custom executors and forces inline dispatch), a caller-supplied `CallbackExecutor` matching Java `setCallbackExecutor`/`getCallbackExecutor` when enabled (with `get_callback_executor`), inline fallback when a custom executor rejects, public callback complete-before-success/failure ordering after channel establishment (matching Java `invokeAsyncImpl`'s `whenComplete` then `thenAccept`/`exceptionally` chain); initial channel connect/TLS setup failures invoke fail without complete, as in Java, and async admission waits within the operation deadline. Those initial connection failures carry Java's address-bearing connect-error category and null cause, covered by `p3_future_connect_failure_includes_java_remote_address_and_null_cause`. Client config also maps Java callback/worker thread counts, socket buffer sizes, write watermarks, pooled allocator flag, idle timeout, timeout socket close, GO_AWAY/scan flags, and the inert `maxReconnectIntervalTimeSeconds` setting (default 60; no pinned Java client read site). TCP and SOCKS5 socket buffers use socket-level options; configured write high/low watermarks track queued outbound bytes with hysteresis. The Java pooled allocator option maps to a bounded per-connection frame-buffer reuse pool, not Netty allocator semantics. NameServer list/read/availability, chosen-address tracking, randomized round-robin selection and failover across configured addresses exist; availability scan starts immediately and then runs at connect-timeout intervals, probing up to 10 configured addresses concurrently with each scan batch capped at 42 (Java scan executor equivalent: 10 maximum workers plus 32 queued jobs). Each Rust scan submits the configured-order prefix that fits the 42 active-plus-queued capacity and starts later ticks from the beginning, matching Java's `AbortPolicy` rejection and scan-loop abort order. Timer/executor overlap timing remains Tokio-specific. The client now exposes `is_channel_writable`, `is_address_reachable` and `close_channels`; outbound frames use an unbounded per-connection FIFO enqueue, while writability uses queued-byte watermarks (Netty 4.1 defaults 64 KiB high / 32 KiB low unless both custom marks are positive), not Netty event-loop write-buffer accounting, and reachability is an async connect operation. Inbound response writes have no fixed activity deadline, matching Java's `writeAndFlush`; client-originated operations remain deadline-bound. Tests P3-V01/V02/V03/U01 plus `p3_oneway_starts_connection_acquisition_at_call_time`, `p3_oneway_reports_invalid_address_synchronously`, client units `oneway_connection_acquisition_enters_connect_before_return` and `spawned_connection_future_is_polled_before_return`, `p3_oneway_returns_after_scheduling_before_peer_receives_frame`, `p3_outbound_rpc_hook_runs_before_oneway_admission_wait` (hook sees the unmarked request; transport marks it before admission), `p3_sync_invoke_obeys_async_admission_and_runs_hooks_first`, `p3_async_go_away_retry_callback_reports_reconstructed_request`, `p3_async_second_go_away_callback_retains_original_request`, `p3_go_away_retry_reacquires_async_admission`, `p3_nameserver_scan_probes_addresses_concurrently_and_tracks_results`, `p3_nameserver_channel_interval_has_no_implicit_scan_behavior`, `p3_pipeline_abort_preserves_java_style_code_and_message`, `p3_rpc_hook_abort_and_failures_map_to_java_response_paths`, `p3_rpc_hooks_deduplicate_and_clear`, `p3_rpc_hook_equality_keys_match_java_contains_semantics`, `p3_outbound_hook_failures_are_not_ignored`, `p3_request_pipeline_pipe_runs_new_stage_before_existing_stage`, `p3_before_hook_failure_runs_pipeline_and_after_hook_but_skips_processor`, `p3_after_hook_runs_when_processor_returns_no_response` and `p3_go_away_retries_once_on_a_new_connection` (including Java-compatible outer after-hook arguments following retry) cover terminal races, cancellation, opaque wrap, outbound backlog beyond the former 128-frame capacity, authenticated SOCKS5 scripted proxy, callback executor concurrency/custom-executor routing, rejection/panic fallback, and callback panic containment, admission deadline, hook ordering including the pre-ONEWAY request view, inbound dispatch/pipeline/rejection including one-way processor execution without response, default processor fallback, Java-style GO_AWAY retry request reconstruction, NameServer invoke/availability, channel management and socket buffer options, TLS trust/hostname and task shutdown. Scripted peers are not a live Broker. Accepted boundary differences and known non-parity in this status summary (with scope/test evidence in the per-row inventory): Java shuffles the supplied NameServer list when its membership changes; Rust now randomizes changed lists and retains duplicates, although it does not mutate the caller's list in place. Removing a non-chosen address now leaves its cached channel open as Java does (`p3_nameserver_update_keeps_non_chosen_removed_connection_open`); Rust now retains removed addresses in the exposed available set until the next scan prunes them, matching Java's stale-key cleanup timing (`p3_nameserver_update_prunes_availability_on_next_scan`). Rust oneway synchronously reports validation/shutdown/deadline errors; otherwise it starts connection acquisition before return and schedules a client-owned operation; later connection, admission and socket-write results are not returned. It never waits for a Broker response, and local write success does not imply Broker receipt/storage. Java returns before its channel listener and discards most post-return transport outcomes; Rust's additional result visibility is the agreed more-informative Rust-native contract. Immediate semaphore saturation maps to the Java too-much-request category and positive-budget admission expiry to timeout; Java exception classes/messages and remaining timeout/shutdown details still differ. Exact NameServer channel-wrapper recovery and timer/executor micro-timing remain implementation-specific; Rust probes concurrently and retains removed availability until scan-start pruning. Java's response-age check is in an uncalled protected scan method and Rust does not synthesize it. The configured `channelNotActiveInterval` only feeds a protected Java scan method with no call site, so Rust does not apply an automatic NameServer response-age close. The newest close-channel behavior clears chosen/available state when the selected socket actually closes; `p3_nameserver_recovers_selection_after_chosen_channel_closes` covers a scripted peer close, while connected peers are kept open in selection/failover tests to avoid conflating successful response availability with later close cleanup. Callback context exposes request, response/error, send status, timeout flag and elapsed duration, and async pending-response handling waits through Java's +1-second scan grace; public async callback dispatch invokes complete before success/failure after channel establishment, while initial connect/setup failure invokes fail only; the GO_AWAY retry connect failure follows the same complete-before-fail chain (covered by `p3_async_go_away_retry_connect_failure_completes_before_failure`); after a successful GO_AWAY retry, callback request metadata reflects Java's reconstructed retry request (`p3_async_go_away_retry_callback_reports_reconstructed_request`); retry-connect failure and a second GO_AWAY retain the original outer callback request metadata (`p3_async_go_away_retry_connect_failure_completes_before_failure`, `p3_async_second_go_away_callback_retains_original_request`). The public async callback status follows Java's wrapper-created default `ResponseFuture` (`sendRequestOK=true`) on exceptional completion, including connection/peer close; Java exception subclass/message taxonomy and callback scheduling/dispatch details remain different; pending lifetime, late-response grace and exact-once admission release are covered by the response-table tests listed in the inventory. Initial connect/TLS failures carry the Java address-bearing, null-cause connect category, and post-registration frame encoding/asynchronous write failures carry the Java send-request category with no cause. Java `invokeSync` dispatches through the overridden `NettyRemotingClient.invokeImpl`, so outbound hooks run; Rust `invoke_sync` follows that virtual dispatch path, running hooks before async semaphore admission (`p3_sync_invoke_obeys_async_admission_and_runs_hooks_first`). Java `invokeSync` closes the channel on exceptional send completion (including async-admission failure), and separately applies the configured close heuristic to response timeout; these behaviors exist only on Rust `invoke_sync`. Non-sync future and async-callback response timeouts leave the channel open for reuse. Covered by `p3_u01_inflight_invoke_accepts_late_response_before_scanner_cleanup` and `p3_sync_admission_failure_maps_to_send_request_and_closes_socket`. Channel idle tracking exists; no automatic stale-NameServer response-age close is applied because the corresponding Java protected scan method has no call site. Channel events use a bounded single-worker queue of capacity 10001, matching Java's manual admission limit; polling/scheduling remain Tokio-specific. `shutdown().await` waits up to 90 seconds for the synchronous event worker, matching Java's `ServiceThread.shutdown()` join, while transport cleanup and accepted callbacks continue in the background after return. Per-processor/default executor lifecycle and thread semantics, full Netty `ChannelHandlerContext` internals beyond the Rust RequestContext adapter, Java-style `RPCHook.equals` deduplication is represented through the accepted opt-in equality key (custom object equality requires an application-provided matching key); Rust exposes snapshot inspection, safe removal by identity/equality key, and lock-protected live-list mutation. Java's allocator and worker-group switches do not control Netty-style runtime facilities in Tokio; the pooled allocator option maps to the documented bounded frame-buffer reuse pool, not Netty allocator semantics. Java client TLS selection and trust modes are mapped through `TransportMode::from_java_properties`, `TlsConfig::from_java_properties`, `test_mode` and `auth_server`; Java-compatible property maps seed retained process-wide TLS property state; each new RemotingClient reloads its retained file source before constructing a connector, file values override supplied properties, and omitted keys preserve prior process values. In Java-compatible test mode, client trust/cert/key paths are ignored without file I/O or decryptor invocation, matching Java's trust-all early return. In verified mode, the process-wide `register_decryption_strategy` hook receives client role and key path; configured password metadata is applied by the built-in PKCS#8 loader. Rust verified mode checks the CA chain and certificate validity but does not verify peer hostname or endpoint identity, matching Java 5.5.1; P3-V04 confirmed both clients accept a trusted wrong-host certificate and reject an untrusted CA because Java creates its outbound `SslHandler` without a peer host. Rust shares Java-compatible TLS property state process-wide and preserves constructed settings in existing clients; it supports Java TLS fixtures and provider-supported encrypted key formats, while unsupported provider algorithms/key sizes remain outside the required compatibility scope. Strategy and TLS error representations are Rust-native and listed in the inventory. The scoped public API/config inventory enumerates Java `RemotingClient`/`RemotingService`, concrete and inherited Netty remoting declarations, `ResponseFuture`, callbacks/events, every `NettyClientConfig` getter/setter, relevant `NettySystemConfig` properties and TLS controls, plus Rust public declarations and config validation/defaults in both directions. Earlier Phase 3 dispositions described ChannelEventListener callbacks as address-only. Rust now passes the identity-preserving `ChannelHandle` through all five callbacks and exposes it through `ResponseFuture::channel()`; the callback and future identity tests record that mapping. The handle provides Rust-native address, generation, state, close, deadline-bound and no-deadline command-write operations without reproducing the Netty object; generic Netty methods remain part of the broad P18-V01 source/API disposition. Tokio worker/thread/polling/scheduling and background-cleanup differences remain subject to the current Phase 18 public-capability and observable-behavior contract. Error exception subclasses/text and TLS provider limitations are accepted only within explicit row-level boundaries; transport control-flow categories and retained diagnostic context remain verified per path. Java's live `getRPCHook()` and `getProcessorTable()` collection inspection is represented by Rust lock-protected live collection accessors. The original Phase 3 inventory recorded acceptance of Rust-only API items, a fixed-config snapshot, and Tokio executor/scheduling/lifecycle differences. Phase 18's current parity contract supersedes blanket acceptance of unmatched public APIs/settings: public capabilities and settings must correspond to Java 5.5.1, while Rust may express them with idiomatic types and components. Recheck observable setting-update and operation behavior before closing those inventory rows; the historical acceptance does not close P18-V01. Actual Netty channel/context objects are framework-specific, while Java TLS/control and error-path outcomes remain subject to the source-backed dispositions in the inventory. P3-V04 passed on 2026-10-25. See [current compatibility status](CURRENT_STATUS.md) for the later tree review and limits of this historical live evidence; this result does not establish current-tree release approval.

| Java source scope (5.5.1) | Rust mapping / test IDs | Status / known gap |
|---|---|---|
| `NettyRemotingClient.invoke`, `invokeSync`, `invokeAsync`, `invokeOneway`, `setCallbackExecutor`; `NettyRemotingAbstract.invokeSyncImpl`, `invokeImpl`, `invokeOnewayImpl`, `ResponseFuture` | `client::RemotingClient::{invoke,invoke_sync,invoke_async,invoke_oneway,set_callback_executor}`, `client::ResponseFuture`, `client::InvokeCallback`, `CallbackExecutor`; P3 controls/U01, `response_future_accessors_mutations_and_callback_order_match_java_surface`, `put_response_unblocks_waiters_and_setter_alone_does_not`, `interruption_and_release_are_idempotent`, `p3_caller_callback_executor_routes_callbacks_to_owned_executor`, `p3_callback_executor_rejection_runs_callback_inline`, `p3_async_callback_contains_response_future_metadata_and_timeout_grace` | MAPPED / ACCEPTED DIFFERENCE: `ResponseFuture` provides Java's callback metadata, mutable response/cause/send state, wait/put response, interrupt, exactly-once callback execution and release; `ChannelHandle` provides Rust-native channel address, state, close and command write; connection generation remains private transport bookkeeping. Configured/caller-owned callback executors and rejection order are covered; oneway returns call-boundary errors synchronously and otherwise schedules a client-owned operation without waiting for or exposing connection/admission/local-write completion or any Broker response; GO_AWAY retry releases and reacquires admission; callback order and +1-second async grace are covered. Rust typed errors and timeout state use Rust types; exception subclass/text and Tokio scheduling details are accepted language-level/runtime differences. |
| `ChannelWrapper`, address connection/close, `SocksProxyConfig`, channel events, NameServer methods | internal `connection_manager::Address`, `SocksProxyConfig`, `client::ChannelHandle`, `client::NettyEventType`, `ChannelEventListener`, `connection_manager`; P3-V02/U01 + `p3_nameserver_selection_fails_over_after_connect_failure`, `p3_nameserver_recovers_selection_after_chosen_channel_closes`, `p18_channel_events_preserve_channel_identity_and_redact_peer` | MAPPED / ACCEPTED DIFFERENCE: per-address reuse, SOCKS5, idle/NameServer scans, channel-bearing event callbacks, list/selection/failover APIs exist; `ChannelHandle` exposes the Java callback channel capabilities through Rust-owned connection operations. Actual Netty object/thread semantics differ; NameServer list scheduling/wrapper recovery and event worker lifecycle are detailed in the inventory. Reachability and explicit close helpers exist; writability uses configured outbound-byte high/low watermarks with hysteresis or Netty's 64 KiB / 32 KiB default watermarks, not Netty event-loop write-buffer accounting; outbound enqueue is unbounded. |
| `NettyRemotingAbstract.processResponseCommand`, response table scan | `PendingEntry`, `PendingGuard`, owned timeout scheduler; P3-V01 | MAPPED / ACCEPTED DIFFERENCE: opaque/generation completion, cancellation guard, scanner cleanup, callback timeout completion, +1-second grace and permit release are covered by named tests in the inventory; Tokio tick precision differs and is accepted. |
| `NettyRemotingAbstract.processRequestCommand`, processor table, `RequestPipeline`, `RPCHook`, `GO_AWAY` | `RequestProcessor`, default processor registration, `RequestPipeline`, `RpcHook`, `TransportError::SendRequest`; P3-V03 + `p3_default_processor_handles_unknown_codes_but_exact_registration_wins`, `p3_inbound_oneway_runs_processor_but_never_writes_response`, `p3_pipeline_abort_preserves_java_style_code_and_message`, `p3_rpc_hook_abort_and_failures_map_to_java_response_paths`, `p3_before_hook_failure_runs_pipeline_and_after_hook_but_skips_processor`, `p18_request_context_preserves_channel_identity_through_pipeline_and_processor`, `p3_connect_failure_does_not_run_before_rpc_hooks` | MAPPED / ACCEPTED DIFFERENCE: one-way inbound commands execute processor/hooks without a response; exact-code processor takes precedence over default fallback; pipeline receives the owned request and supports typed abort response code/remark or SYSTEM_ERROR mapping; RPC hooks preserve the legacy infallible API and now support typed abort/SYSTEM_ERROR outcomes; ordinary before-hook failures still run the pipeline, skip the processor and invoke after hooks with SYSTEM_ERROR; after hooks also run when processors return no response; processors and pipeline stages receive the same identity-preserving Rust RequestContext, while the remaining Netty handler-context internals are framework-specific; Java-style `RPCHook.equals` deduplication is mapped by the accepted optional equality-key API; outbound hook error propagation, mutable lock-scoped hook-list inspection/removal, inbound abort/system-error mapping, executor rejection and retry paths have named coverage. Tokio ownership/scheduling differences are accepted. |
| `NettyRemotingClient` TLS setup, `TlsSystemConfig`, `TlsHelper` | `client::tls::{TransportMode,TlsConfig,PrivateKeyDecryptionStrategy,register_decryption_strategy}`; P3-U01, `java_tls_property_file_is_reloaded_for_each_client_context`, and TLS configuration tests | MAPPED: Java client TLS settings, trust modes, process-wide decryptor registration, and property-file reload at TLS context construction are represented; existing clients retain their constructed TLS settings. Distinct Java-compatible Rust `TlsConfig` sources share retained process-wide properties like Java's static fields; `tls_property_file_settings_are_process_wide_across_config_sources` covers the cross-source behavior. Verified rustls roots, certificate validity, SNI, and client identity are selected with `test_mode=false, auth_server=true`; peer hostname/endpoint identification is intentionally absent to match Java 5.5.1 and is covered by the trusted-wrong-host transport regression. Provider-supported key formats and Rust error types are documented boundaries. This row records its historical scope; see [current compatibility status](CURRENT_STATUS.md) for the later source review. |
| `NettyRemotingClient.shutdown`, client-owned timeout/processor/connection tasks | `RemotingClient::shutdown`, task handles; P3-V01/U01 | ACCEPTED DIFFERENCE: explicit repeatable shutdown, pending completion and tracked owned-task joins are verified locally; Tokio drain/abort behavior and Netty resource/thread lifecycle differ as documented and accepted. |

## Phase 0 evidence

The Java harness emits offline Remoting request frames for route, heartbeat, send, pull, offset query/update and lock operations. Each `.bin` has a JSON sidecar with source/client/server version labels, scenario ID, codec, code, normalized decoded fields, body length and frame SHA-256. These are protocol command fixtures, not executed RPC results. Live Broker scenarios in `tests/differential/scenarios.yaml` have Java 5.5.1/Rust results against Broker 5.3.4 for the Phase 0 route, heartbeat, and producer cases, plus the later producer, consumer, transaction, request/reply, and timeout probes summarized above; their evidence is recorded in [`SERVER_5_3_4_RESULTS.md`](SERVER_5_3_4_RESULTS.md). These named cases do not establish broader Broker interoperability.

Phase 11 local gate (2026-10-12): Java 5.5.1 strategy fixtures, initial-offset policies, Push reconciliation/race fencing and Pull registration/allocation/query lifecycle are UNIT_VERIFIED. Empty and absent clustering routes follow Java's callback and membership-query order; unavailable membership and offset persistence failures retain existing ownership state. On the repository host, the locked workspace suite, strict all-target/all-feature Clippy, full offline Maven `verify`, comparator self-test, and 31-scenario manifest validation all pass. P11-V03/V04 remain PENDING_PHASE_19. See [PHASE_11_OFFLINE_METHOD_MATRIX.md](PHASE_11_OFFLINE_METHOD_MATRIX.md) and the Phase 11 rlog.

Phase 18 TLS static-property continuation (2026-10-15): pinned Java 5.5.1 `TlsSystemConfig` reads system properties into static fields at class initialization; later JVM property mutations do not update those fields. Rust now initializes its process-wide property state from the first explicit map, including a Plain-mode selection, and does not let later maps replace the retained values. TLS property files are still reloaded for each newly built client context, with omitted keys retaining the current process value. The Java fixture and `tls_property_file_settings_are_process_wide_across_config_sources` cover both boundaries. Fixture SHA-256: defaults `4e1603df11cdb4054f8f3950ddc27cb5e219ad89f93d91071c9478d0d80ad648`; system properties `b4ca8ef0433b88014c28168be8bdb335ff93f2eeccae24f467ef5c72cb79c2b9`. This closes only the TLS process-property slice; broad P18-V01 remains open.

Phase 18 TLS enable-selection continuation (`746c35bc`, 2026-10-15): Java's `tls.enable` and `tls.config.file` are class-initialized fields excluded from `TlsHelper.extractTlsConfigFromFile`. Rust now selects `TransportMode` and the default file path from the immutable first property snapshot; later maps and file contents cannot switch TLS on. The Java fixture changes `tls.enable` to true and includes it in both files while preserving the original false field. The cross-source regression first failed because Rust reread the later map, then passed after the correction. Fixture SHA-256: defaults `223ed13d44d2a448b9c5193435a5dcb8c04940c42a9109196ad55eebeacbbc82`; system properties `67bab119ca6084783ee19bdca2badd2bd62fb9534a3b44382fcf16c5e918dba2`. Broad P18-V01 remains open.

Phase 18 `ResponseFuture.executeInvokeCallback` continuation (2026-10-16): the pinned Java 5.5.1 direct callback path retains the original cause in send, timeout, and generic failures, calls failure before completion, and dereferences a null request before invoking callbacks in the generic-failure branch. Rust now retains typed cause context for these three categories and preserves the null-request pre-callback failure. The Java fixture and `response_future_callback_failure_causes_and_null_request_match_java_fixture` cover those branches. This closes only the callback-failure context slice; `toString`, `waitResponse` interruption behavior, and the broader P18-V01 audit remain open.

Phase 18 `ResponseFuture.toString` and wait-interruption disposition (2026-10-16): Rust `Display` now emits Java's stable state fields in the same order and tracks the response latch count; `responseFutureToStringBehavior` and `response_future_display_preserves_java_to_string_state_fields` compare response/cause/callback nullability, opaque, timeout, begin timestamp presence, and latch transition. Nested Rust command/error and callback rendering plus JVM latch identity are diagnostic-only representation differences. Java `waitResponse` propagates external JVM thread interruption as `InterruptedException` and clears the interrupt flag; Rust's standard Condvar has no external thread-interrupt facility. Response/timeout behavior and the separate future `interrupt()` operation remain mapped; the Java fixture and `wait_response_timeout_and_internal_interrupt_match_rust_mapping` document this runtime boundary. These dispositions close only the named `ResponseFuture` observations; broad P18-V01 remains open.

Phase 18 `resumeCheckHalfMessage` RPC (2026-10-21): pinned
`MQClientAPIImpl.resumeCheckHalfMessage(String,String,String,long)` sends
request 323 with nullable `topic` and `msgId` header fields through the
configured VIP address. Rust returns `true` only for `SUCCESS` and returns
`false` for any other Broker response. The invoked Java fixture and Rust
loopback differential cover success, non-success, each null field, both fields
null, the empty body and the VIP port offset. Producer core fixture SHA-256:
`bbeea411ebb9a1cbf177a4f0efddbf85023a475288bd123764959acca248efc6`. This
closes this RPC mapping only; broad P18-V01 and the final P18-V02 refresh remain
open.

Phase 18 `exportPopRecord` RPC (2026-10-21): pinned
`MQClientAPIImpl.exportPopRecord(String,long)` sends request 200056 (registry
name `POP_ROLLBACK`) with no header/body and uses the supplied address unchanged,
including when VIP is enabled. Rust returns success for `SUCCESS` and preserves
non-success responses as RPC errors. The invoked Java fixture and Rust
VIP-enabled loopback compare the signature, address, request envelope and both
response outcomes. Producer core fixture SHA-256:
`d94fd1f95eba2caf6ce77e312cd53a4109be74048b23db5ad694a2c5b7109242`. This
closes this RPC mapping only; broad P18-V01 and the final P18-V02 refresh remain
open.

Phase 18 `switchTimerEngine` RPC (2026-10-21): pinned
`MQClientAPIImpl.switchTimerEngine(String,String,long)` sends request 5001 with
the engine type in the `E_T` ext field, preserves empty strings, uses the
supplied address unchanged when VIP is enabled, and raises on non-success
responses. The invoked Java fixture and Rust loopback compare the signature,
field, empty body, address and success/error results. Producer core fixture
SHA-256: `bb28b038fdd66160007d8c757e53ed4cb78ebcad0a91b4ff9d950fa04eccee6c`.
This closes this RPC mapping only; broad P18-V01 and the final P18-V02 refresh
remain open.

Phase 18 `checkRocksdbCqWriteProgress` RPC (2026-10-21): pinned
`MQClientAPIImpl.checkRocksdbCqWriteProgress(String,String,long,long)` sends
request 354 to the supplied address, serializes nullable `topic` and primitive
`checkStoreTime`, and parses a nullable `CheckRocksdbCqWriteResult`. Rust maps
the status DTO and `CheckStatus` names/values, preserves null, absent and empty
successful bodies as `None`, reports malformed JSON as a malformed response,
and preserves Broker errors. Java invoked cases and Rust loopback tests cover
the request, status values, defaults and response outcomes; header fixtures
cover fast/reflection decoding. Producer core fixture SHA-256:
`6f853c6e49ad04a2fe2d1d7d202c992d1088e17da1fc248330bef7de968603a0`. This
closes this RPC/header slice only; broad P18-V01 and the final P18-V02 refresh
remain open.

Phase 18 `triggerLiteDispatch` RPC (2026-10-21): pinned
`MQClientAPIImpl.triggerLiteDispatch(String,String,String,long)` sends request
200079 with nullable `group` and `clientId` fields and an empty body. Its
request header has a no-op `checkFields()`. Rust applies the configured VIP
address transform, uses the caller deadline, and reports non-success Broker
responses. Invoked Java cases cover populated and null inputs, request code,
target address, timeout and response outcomes; the Rust loopback differential
checks the decoded extension fields and empty body. Producer core fixture
SHA-256: `7846aa7100a8f2339917fc71ad0018e32786692e4b3cff3c1bfafe1c6621372f`.
This closes this RPC mapping only; broad P18-V01 remains open.

Phase 18 `getBrokerLiteInfo` RPC (2026-10-21): pinned
`MQClientAPIImpl.getBrokerLiteInfo(String,long)` sends request 200074 with an
empty envelope through the configured VIP address transform. Rust decodes a
nullable `GetBrokerLiteInfoResponseBody`, including primitive counter defaults
and nullable `topicMeta`/`groupMeta` maps with mutable getter aliases; absent
success bodies return `None`, malformed bodies are reported, and Broker errors
preserve the source address. Invoked Java fixtures compare the method and DTO
accessor inventories, defaults, populated metadata, null body, malformed body,
and Broker rejection; a Rust VIP loopback verifies the address transform.
Fixture SHA-256:
`57eff87ee4fd5bc652076a744109cdc2fb4d395362558b6ed5802e5dc166fe0e`. This
closes this RPC/DTO slice only; broad P18-V01 remains open.

Phase 18 `getParentTopicInfo` RPC and response DTO (2026-10-21): pinned
`MQClientAPIImpl.getParentTopicInfo(String,String,long)` sends request 200075
with nullable `topic`; `GetParentTopicInfoRequestHeader.checkFields()` is a
no-op despite the field annotation. Rust uses synchronous invocation, the
configured VIP address transform and caller deadline, and decodes a nullable
`GetParentTopicInfoResponseBody` with primitive defaults, nullable topic and
groups, and a mutable Set getter alias. Seven invoked Java cases compare the
RPC/header/DTO public surface and populated/default, null-body, null-topic,
malformed-body and Broker-error outcomes. A Rust differential checks the
request and decoded results; a loopback verifies VIP conversion. Success
fixture SHA-256: `1977e67d8e3c856784cf2628b82bcd14429e2118fb1a3e001db1e4ba2f2ad943`. This closes this RPC/DTO slice only; broad
P18-V01 remains open.

Phase 18 `getLiteClientInfo` RPC and response DTO (2026-10-21): pinned
`MQClientAPIImpl.getLiteClientInfo(String,String,String,String,long)` sends
request 200077 with nullable `parentTopic`, `group`, and `clientId`; its header
defaults `maxCount` to 1000 and rejects nonpositive explicit values. Rust uses
synchronous invocation, the configured VIP address transform, and the caller
deadline. `GetLiteClientInfoResponseBody` preserves nullable strings, a nullable
Set with nullable members, primitive defaults, and the mutable Set getter
alias; an absent success body returns `None`. Seven invoked Java cases compare
the RPC/header/DTO surface and cover populated/default bodies, null inputs,
null/malformed bodies, and Broker errors. The Rust fixture differential and
VIP loopback verify the request and decoded response. Success fixture
SHA-256: `9e4e1ad1abd344d58ba5939b2fd204a4237c4e60570ceba9c878790aee85ae8b`. This closes this RPC/DTO slice only; broad P18-V01
remains open.

Phase 18 Broker registration RPCs (2026-10-21): Rust maps the pinned
`MQClientAPIImpl.addBroker(String,String,long)` and
`removeBroker(String,String,String,long,long)` calls to requests 902 and 903.
Their typed headers preserve nullable Java values and the signed boxed-`Long`
broker ID. Both operations use the supplied Broker address unchanged, honor
the caller timeout, return on success, and preserve Broker rejection errors.
Invoked Java and Rust fixtures cover exact signatures, null fields,
`Long.MIN_VALUE`/`Long.MAX_VALUE`, address and timeout values, and outcomes;
a delayed scripted peer checks timeout enforcement. Producer core fixture
SHA-256 and gate details are in the Phase 18 release checklist. This closes
these direct RPC/header rows only; broad P18-V01 remains open.

Phase 18 caller-defined `MessageClientExt` offset-ID overrides (2026-10-21):
Rust now dispatches Java subclasses' virtual `getOffsetMsgId()` and
`setOffsetMsgId(String)` hooks. Inherited `getMsgId()` still prefers `UNIQ_KEY`
and then observes the overridden offset getter. Invoked Java and Rust cases
cover custom offset-only subclasses, unique-ID precedence, setter dispatch,
base `setMsgId` no-op, and conversion to `MessageExt`. Fixture SHA-256 and
validation details are in the Phase 18 release checklist. This closes the
selected `MessageClientExt` subtype path only; broad P18-V01 remains open.
The Java fixture records `setOffsetMsgId(null)` clearing both the offset ID
and the effective client ID, including after conversion to the base type.
The follow-up fixture also overrides `getMsgId` and `setMsgId` on a custom
`MessageClientExt`: the override bypasses the base unique-key fallback, setter
dispatch updates only subtype state, and a null setter result remains null.

Phase 18 `MessageDecoder.decodeProperties(ByteBuffer)` cursor behavior
(2026-10-22): pinned Java consumes positive-length property bytes, leaves the
cursor immediately after the length field for zero/negative lengths, and
throws `BufferUnderflowException` at the property start when a positive-length
block is truncated. Rust now returns the matching byte count with the decoded
properties. The invoked record fixture covers ordinary and boundary inputs;
its SHA-256 is
`699dfaae816b753105119193e3b268fe16fb2b83ad0199473a5665a7d6b76436`. This
closes the tested decoder cursor/error slice only; broad P18-V01 reconciliation
remains open.

Phase 18 `MessageDecoder.decode(ByteBuffer)` declared-size handling
(2026-10-22): Java stores the first integer as `MessageExt.storeSize` but does
not use it as a parsing boundary. Invoked cases with declared size `0`, `-1`,
and `Integer.MAX_VALUE` decode the same fields and consume through the
properties. Rust now follows those field boundaries while retaining the
declared value and enforcing its configured limit on actual bytes parsed.
Fixture SHA-256:
`41ed0208cdf79da2491554ed08683e4f18b40d6bce1d21d246771dd58f0e15ae`. This
closes the tested store-size boundary only; broad P18-V01 reconciliation
remains open.

Phase 18 `MessageDecoder.decode(ByteBuffer)` nonpositive field lengths
(2026-10-22): Java treats body and property lengths `<= 0` as absent without
consuming bytes beyond their length fields. Rust now returns a decoded message
with no body/properties and the matching consumed-byte count. Invoked cases
cover a negative body with its bytes omitted and a negative property length
whose original property bytes remain in the input. Fixture SHA-256:
`9cd0b64fe95643177c533d6cc2fd1f33c957e54cd3bd9559ecb07f6083939938`. This
closes the tested nonpositive-length branches only; broad P18-V01
reconciliation remains open.

Phase 18 inbound Remoting failure flow (2026-10-23): the pinned
`NettyRemotingAbstract.processRequestCommand` was invoked with local Netty
`EmbeddedChannel` requests and actual Java hook, pipeline, and processor
interfaces. The fixture covers ordinary before-hook, pipeline, processor, and
after-hook failures; `AbortProcessException` at before, pipeline, and after
stages; and one-way processor failure. Rust loopback cases compare response
presence/code/opaque and stage call order. The checks confirm SYSTEM_ERROR for
ordinary failures, explicit abort codes, Java's processor and after-hook skip
ordering, and one-way silence. Java's exception class plus first stack frame
in the remark remains diagnostic-only; the fixture normalizes this field to
the Java exception class. Source artifact: `NettyRemotingAbstract.java` at
5.5.1 commit `efa1be59d6fc747ad220f036c86030f05f0419f9`, SHA-256
`b21a504b5310f8096cca443ee64ff733a6feaaea7fbd22ddd214821fa561275d`.

Phase 18 `ConsumerRunningInfo` inherited Fastjson decoder inputs (2026-10-24):
the pinned `RemotingSerializable.decode(byte[], ConsumerRunningInfo.class)`
delegates to Fastjson `parseObject`. Rust now normalizes the Fastjson document
before converting object-valued `MessageQueue` map keys. Four invoked cases
cover text after U+001A, a second JSON object after U+001A, U+001A inside a
string, and single quotes with a trailing comma. A populated Java body also
round-trips its object-valued queue keys. Fixture SHA-256:
`495f040623c010c13a4d70f004db4d0529bd8fcda93530671015c69cb49f1e06`. This
closes this decoder-input slice only; broad P18-V01 remains open.

Phase 18 `getConsumerIdListByGroup` response decoding (2026-10-24): the three
Core consumer-list response variants now call
`ConsumerListByGroupResponseBody::java_decode`, preserving the pinned
`RemotingSerializable.decode` Fastjson behavior instead of routing through
generic Serde decoding. The invoked DTO matrix has 18 raw inputs plus seven
root-shape inputs (25 total); the added `eof-trailing-text` and
`eof-trailing-json` cases verify that text or another JSON document after raw
U+001A is ignored. Sync and future Core loopback calls both accept these
responses. Runtime lookup fixture SHA-256:
`b3b2efa341e83f40d9b18f345ee175796d8891d84208b2f396f6cf8da162ee33`. This
closes the tested consumer-list decoding paths only; broad P18-V01 remains
open.

Phase 18 typed RPC response Fastjson decoding (2026-10-24, `9a13acb7`): nine
Core response variants across QueryAssignment, query consume time span,
lock-batch, GroupList, TopicList, query subscription, and direct consume now
pass their DTO's Java Fastjson decoder explicitly. Controller sync-state,
epoch-cache, and election bodies use nullable Fastjson decoding, preserving
JSON `null` results. The invoked Java lookup fixture adds 14 EOF suffix cases
for seven RPC response DTOs and six cases for three controller DTOs. These
cover trailing text and a second JSON document after raw U+001A. Runtime lookup
fixture SHA-256: `85c4496ce6e9055da78b4f7624e6713115b90d8c4e7beea60a332c810b5e5bf2`.
The focused controller API suite, full locked Protocol and Core package suites
with doctests, strict all-target/all-feature workspace Clippy, formatting,
diff checks, and pinned offline Maven `verify` pass. This closes the tested
typed response decoder paths only; broad P18-V01 remains open.

Phase 18 `getUser` reply Fastjson decoding (2026-10-24, `41854b80`): Core now
uses `UserInfo::java_decode_nullable` for the response body, matching the
invoked Java API while preserving absent-body and JSON-null results. The
`getUser` oracle adds two suffix cases: trailing text and a second JSON
document after raw U+001A. Both return the same populated `UserInfo` as Java;
the focused Core API differential passes all ten cases. Producer core fixture
SHA-256: `ac4d435472279c6cdfefadf2811422359bc927223733c3521484c1c533d31f36`.
This closes the tested `getUser` response decoder slice only; broad P18-V01
remains open.

Phase 18 JSON remoting header Fastjson input (2026-10-24, `d897a9e2`): pinned
5.5.1 `RemotingCommand.headerDecode` delegates JSON headers to
`RemotingSerializable.decode`, which uses Fastjson. Rust's `json_header::decode`
now parses its `Value` through the shared Java-compatible input layer. The
direct Java `RemotingCommand.decode` fixture and Rust codec differential cover
trailing text and a second JSON document after raw U+001A, plus single-quoted
fields with a trailing comma. Fixture SHA-256:
`f0b5433e0c7c9f4566dee85048196860c47b948e4204cb08aa09b062bb628c0c`. This
closes these JSON-header input cases only; broad P18-V01 remains open.

Phase 18 `LatencyFaultToleranceImpl.FaultItem` public API follow-up (2026-10-24):
Rust now exports the public nested Java helper as `FaultItem`, with Java-compatible
latency/start/check/reachability state, availability updates, signed comparison,
value equality/hash, wrong-type and null behavior, UTF-16 name hashing, and
`toString`. The invoked Java fixture inventories all 15 declared public methods
including the erased `Comparable` bridge. Rust omits only the implicit outer
strategy reference, which no helper method reads. Producer/facade tests and
pinned offline Maven verification cover the mapping. Fixture SHA-256:
`d6c23e4c1d213f02fd257abd0526b525d18f33167f1f26bda8510b75d189f398`. This
closes this public nested value API row only; broad P18-V01 reconciliation
remains open.
Code commit: `62f67cbb`. The full local verification and remaining audit status
are recorded in `../../rlogs/2026/10/24-phase-18-fault-item-public-api__20261024T153809Z.rlog`.

P13 interpretation update: run `20261025T125153Z` showed offset 0 at the first
Java POP request, which returned the seeded message; offset 1 appeared after
that delivery. The failing `POLLING_NOT_FOUND` was the next POP for expiry
redelivery. Earlier diagnostic rows were emitted only for a failing POP, so
their label did not prove that initial delivery failed. Initial Java POP delivery
worked with the post-seed offset reset; expiry redelivery failed in that run.

P13 follow-up: run `20261025T125713Z` passed all seven Java POP lifecycle
outcomes after the 6-second expiry wait. Rust received the seed but its direct
POP probe did not decorate `POP_CK` from response-header offsets and stopped
before the Rust lifecycle. The Rust probe now mirrors Java's client-side
receipt mapping; a clean Java/Rust comparison subsequently passed in run
`20261025T130156Z`.

P13-V02 live result: clean-source run `20261025T130156Z` on
`99d2dc68484ab8de43da86b997dc20dc49267c3c` passed. Java and Rust matched
POP delivery, expiry redelivery, ACK, change-invisibility, replacement-
receipt ACK, mixed queue mapping, and malformed receipt rejection. The
manifest records PASS; live coverage is 26/37 with 11 pending. The runner
removed the topic, two-Broker containers, network, and volumes. Evidence
is under `20261025T130156Z` (raw output not retained).

P5-V02-extensions live result: clean-source run `20261025T132049Z` on
`d93c978e04c3619d3c8fe6e1c0d82c4de05f050b` passed. Java and Rust matched
heartbeat, heartbeat V2, assignment, request-mode, standard POP, ACK, batch
ACK, and visibility change. Both received response code 3 for unsupported
Lite subscription request 200071 and Lite POP request 200070. This proves
the 5.3.4 rejection mapping; the Lite success path remains to be checked on
a supplementary unmodified server. Evidence is under
`20261025T132049Z` (raw output not retained).
