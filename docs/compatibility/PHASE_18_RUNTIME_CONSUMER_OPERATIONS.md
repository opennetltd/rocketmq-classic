# Phase 18 direct runtime consumer operations

Reference: Java 5.5.1 tag `efa1be59d6fc747ad220f036c86030f05f0419f9`,
`client/src/main/java/org/apache/rocketmq/client/impl/factory/MQClientInstance.java`.
The external checkout's HEAD is a different revision; this audit uses the tag
source and the pinned 5.5.1 Maven artifact.

## Running Info

Java `consumerRunningInfo(String)` maps to
`ClientRuntime::consumer_running_info(Option<&str>)`. The selected
`ConsumerRuntime::consumer_running_info` supplies the raw owner snapshot;
`consume_type` maps the separate Java `MQConsumerInner.consumeType()` call.
Pull and Push registrations supply their actual owner snapshots.

| Java behavior | Rust behavior / evidence |
| --- | --- |
| Missing group returns null; null group fails lookup | `Ok(None)` / typed `InvalidRoute`; direct Java fixture and Core test |
| Snapshot obtained before consume type | Core fixture comparison checks `info` then `type`; a snapshot error prevents the type call |
| NameServer address list order and duplicates retained, each with trailing semicolon | Current remoting list is read at the call boundary; null/empty and ordered duplicate-list cases |
| Shared properties overwritten in place | `SharedValue` properties retain caller alias; Java identity and Rust `ptr_eq` checks |
| Null properties fail before type lookup | Typed `ConsumerOperation`; callback ordering comparison |
| Null consume type fails after NameServer mutation, before type/version mutation | Core test compares the retained properties after failure, including old type/version values |
| Client version is `V5_5_1` and consume type comes from selected owner | Direct active/passive Java cases and actual Pull/Push facade integration |
| Runtime does not inject a thread dump | Builtin integration checks absent jstack; the inbound jstack option is separate |

`RuntimeConsumerOperationsFixtures` invokes all nine cases directly on the Java
runtime without starting it. Its fixture is
`fixtures/runtime/consumer-operations.json`, checkpoint SHA-256
`8ddd9f2aefb202c545466c37b4e8162b71e618b8e0bb545f3e1d504c31aed9bd`.
`runtime_consumer_operations::running_info_matches_java_enrichment_aliases_and_partial_failure_order`
checks the same inputs, results, call ordering and partial mutations.
`runtime_consumer_status` exercises both registered Rust consumer types.
Java exceptions map to Rust typed errors; no extra client setting is added.

## Direct consumption

Java `consumeMessageDirectly(MessageExt, String, String)` maps to async
`ClientRuntime::consume_message_directly(Option<MessageExt>, Option<&str>,
Option<&str>)`. `ConsumerRuntime::consume_message_directly` is the Push service
adapter; its optional future preserves the non-Push null-result branch without
inspecting the message. `ConsumerOperationFuture` expresses the asynchronous
Rust owner result, not a new client feature or setting.

The Java oracle calls the actual runtime and concurrent/orderly consume
services for 29 cases. `runtime_direct_consume` starts real Rust facades and
checks the same result categories, order/auto-commit flags, callback counts,
callback message/queue topics and broker names. It covers success, retry,
commit, rollback, suspend, null listener result and listener failure; nullable
broker/message, missing/Pull/custom/null group, and unavailable Push service.
Namespace conversion affects callback messages while queue context retains the
original topic. Concurrent consumption writes the message broker; orderly
consumption retains its original broker. Every case checks offset isolation.

The extended fixture checkpoint SHA-256 is
`403b79b69f30957ae2f935be37ec0b8537696909d91066d2126f216eff31996d`.
Only elapsed duration and runtime-specific exception rendering are excluded
from equality: Java/Rust both assert nonnegative duration, identical failure
result category and presence of a diagnostic remark. Broker/offset/callback
relationships are compared directly.

Raw-input continuation (2026-10-20): the owner no longer converts every
`MessageExt` to client-message IDs. Ordinary messages retain their supplied
physical `msgId`, even with an `UNIQ_KEY` property; client-decoded Rust snapshots
retain their effective and offset IDs. The inbound processor now invokes the
existing `client_decode` adapter at Java's `MessageDecoder.clientDecode`
boundary. A separate inbound regression checks both IDs after a full encoded
record crosses the processor/owned task/listener path.

Runtime subtype continuation (2026-10-21): Java passes an input
`MessageClientExt` through the `MessageExt` direct-consume parameter without
wrapping it, so the listener still observes the subtype and virtual
`getMsgId()` result. Rust now keeps that subtype marker when converting
`MessageClientExt` to the base input type, exposes a runtime-type ID accessor,
and keeps the `UNIQ_KEY` lookup live. Client decoding, pull results and admin
message decoding apply the same subtype state. The invoked Java fixture records
the callback class; the 26-case Rust comparison checks class, effective ID,
offset ID and offset isolation. Fixture SHA-256:
`d0b4e39ba340e509d1ddc2665da122a0cf161ed757cc1d9ac2cbd022edb7d83f`.

Pinned subtype inventory follow-up (2026-10-21): at source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`, client decoding with
`MessageDecoder.decode(..., isClient=true)` creates `MessageClientExt`, and
`MessageDecoder.decodeMessage(MessageExt, List)` creates a `MessageClientExt`
for each inner batch message; `PullAPIWrapper` invokes the latter. The only
other built-in subclasses in the pinned source tree are broker-side
`MessageExtBrokerInner` and `MessageExtBatch`; the client sources do not
construct them. The standard client-produced subtype paths are covered by the
marker and direct-consume fixture above. Rust `MessageExt` does not preserve
caller-defined subtype data at this checkpoint; the follow-up below records
the later runtime adapter slice.

Caller-defined subtype continuation (2026-10-21): Rust now exposes
`MessageExtSubtype`, retained by `Arc` across the `MessageExt` direct-consume
input. Applications can downcast custom fields in the callback and provide
virtual overrides for topic, broker name, queue ID, queue offset, body, message
ID, flag, `getProperty`, reconsume-times getters/setters, and display. The
adapter dispatches virtual `getBody`/`setBody` and
`getReconsumeTimes`/`setReconsumeTimes` calls
used by ProcessQueue byte accounting and Push retry/send-back logic. It also
maps `getFlag`/`setFlag`, so Push retry/send-back message copies read the
subtype's virtual flag value.
It also maps `getProperty`; ProcessQueue uses an overridden `MAX_OFFSET` value
to update its accumulated-message count.
Rust `tags()` and `keys()` route through this same hook, matching Java's
property-derived `getTags()` and `getKeys()` methods.
The subtype may also provide a nullable `getProperties()` map snapshot; Rust
uses its entries for Push retry-property copies. Java's mutable map identity is
not retained by the snapshot.
`getStoreHost`/`setStoreHost` are dispatched as well; Push send-back reads the
custom host for broker-address fallback when no broker name is available.
`ProcessQueue.putMessage` also uses the subtype's virtual `getQueueOffset` as
its message-tree key. The
`concurrent-custom-subtype` case executes Java overrides during initial topic
assignment, retry-topic restoration, namespace removal, broker assignment and
callback formatting; it checks the custom reconsume value `13`, queue offset
`987654321`, and overridden body length. A Java `ProcessQueue.putMessage` case
compares dispatch, count, body size, tree key and setter side effects against
Rust. The callback records custom flag `71` and the setter count. Current
fixture verifies the custom `MAX_OFFSET` getter and `msgAccCnt` value `12`.
Current fixture SHA-256:
`3633f3ac0098815df76790a969a15e7a9e2edda45f30723bd67a2753468facab`.

Caller-defined subtype metadata follow-up (2026-10-21): the adapter now
dispatches the remaining stored `MessageExt` metadata getter/setter pairs for
born/store timestamps, born host, sysFlag, body CRC, commit-log offset, store
size and prepared-transaction offset. The invoked custom subtype returns
distinct values and counts all eight setters. Its born/store host overrides
also differ from their stored host fields: Rust preserves virtual
`getBornHost`/`getStoreHost`, while Java's host string/name/bytes helpers read
the stored fields directly, so the Rust derived helpers do the same. The
fixture compares both virtual host values and the field-derived IPv4 byte
arrays. Current fixture SHA-256:
`aa202880244ea5606fbebc63d8558965fddede4a95be58baacd56320f8f16571`.
Focused direct-consume tests, both locked workspace test modes, strict
all-target/all-feature Clippy, and pinned Maven `verify` pass on code revision
`8425a30d`. This extends the selected override slice only; map identity and
other inherited virtual methods remain open.

Transient sysFlag follow-up (2026-10-21): Rust `topic_sys_flag()` and
`group_sys_flag()` now read through the subtype's virtual `getProperty` hook,
matching Java's inherited `getTopicSysFlag()`/`getGroupSysFlag()` delegation.
The matching `setTopicSysFlag(Integer)`/`setGroupSysFlag(Integer)` methods
also dispatch to subtype setters. The invoked direct-consume fixture compares
overridden values `73` and `-19` and both setter counts. Fixture SHA-256:
`71cf67f0bf813630fe084e2be78ea4275a1f04f49063c88a83606e909bb47dbe`.

The adapter is runtime-only and does not serialize custom subtype state. This
closes the direct-consume custom-field and selected-override slice; inherited
`Message`/`MessageExt` override coverage beyond the mapped hooks and the broader
P18-V01 audit remain open.

Callback message/queue topics retain null versus empty input. The retry-topic
property is read for every message, preserving Java `getProperty`'s observable
initialization of a null properties map. The ten added raw/client ID and topic
cases compare IDs, property-map presence, callback fields and offset isolation.
Fixture checkpoint SHA-256:
`c7cda96e0bf61f51c9e848c7290859382964c01cd2c4db52b34e8fed2eb00c76`.

The raw `get/setConsumeMessageService` seam now maps to a public Rust
`ConsumeMessageService` trait and nullable Push owner accessors. A replacement
service receives direct-consume calls and normal Pull submissions; explicit
pool updates, adjustment hooks and shutdown also target that service. Java
routes POP work through a separate private service, so Rust keeps POP dispatch
on its own listener path after primary-service replacement. The fixture records
all nine Java service methods, and Rust owner tests check direct/Pull routing,
POP separation and null replacement behavior. Fixture SHA-256:
`2ceab3283085fa8335cd3e7a965c1698e10960ae720c76b148af4cb52a196b4b`.

## Offset reset

Java `resetOffset(String, String, Map<MessageQueue, Long>)` maps to async
`ClientRuntime::reset_offset(Option<&str>, Option<&str>,
Option<ConsumerOffsetTable>)`. `ConsumerOffsetTable` is a shared nullable-key,
nullable-offset map; it retains the caller alias during the drain wait. The
Push owner supplies the operation; other/missing consumer types ignore both
nullable inputs. A null group still fails lookup.

| Java branch / boundary | Rust mapping and evidence |
| --- | --- |
| Runtime monitor serializes reset, producer/consumer registration/removal and start | Private async monitor shared by those methods; Core test blocks a second reset, null registration and start while permitting raw map mutation and unsynchronized admin registration |
| Shutdown checks user tables before its synchronized block | An active-user shutdown returns false during the blocked reset; terminal shutdown uses the monitor |
| Suspend before traversal; resume in finally | Normal/error returns await the target owner's rebalance after restoring pause/scheduler state; a cancellation-only guard restores scheduling if the Rust future is dropped early |
| First traversal uses topic equality then `containsKey` | Null-topic/table branches and null-valued matching keys match Java; matching queues are dropped/cleared even when the offset value is null |
| Concurrent drain waits ten seconds; orderly reset waits on each queue's consume lock | Paused-time tests exercise the full drain and existing orderly lock windows; Java oracle uses its explicitly ignored interrupt path to bound wall-clock time |
| Second traversal reads the current map and current queue table | Live map value changes and queue insertion are checked against direct Java execution, without reusing first-pass values |
| Per-queue persistence failure is logged and does not abort sibling queues | Java/Rust compare queue/value state, persisted values and update→persist→remove order within each queue; the final rebalance can truncate a failed queue whose topic is no longer subscribed |
| Missing/null offsets skip reset's update/removal stage | Null-key, null-value, absent-assignment and empty-input cases; finally still runs rebalance |
| Store selected at each virtual-call boundary | Reset rereads the live field for update, persist and remove; a Java/Rust store-handoff case changes the field in both update and persist callbacks |

The same public runtime entrypoint now owns inbound reset and owner timestamp
reset calls. The fifteen Java cases invoke the real runtime and rebalance
removal path, including the real consumer resume and its target rebalance.
Events from distinct
ConcurrentHashMap queues are grouped by queue ID; ordering within each queue,
all values, dropped/presence states and errors remain compared.

The initial reset fixture checkpoint SHA-256 is
`594e1e51bfd5f0d3598d309ae8932b9c596a83969ab6c6f8b43f0ac347daaf06`.
`runtime_reset_matches_java_live_maps_nulls_and_per_queue_failures` compares
all fifteen cases. The reset filter also runs existing callback/pull fencing,
unassigned/null-value, cache-clear and orderly-removal regressions.

The additional `resetStoreHandoff` case records update in the first store,
persist in the second and remove in the third, with final values `[7, 60, null]`
and a persisted value of 60. It prevents a single captured store from silently
changing the Java setter's activation timing. The Rust regression also verifies
that the custom removal callback can inspect the process-queue map before its
entry is removed. No user store callback runs under that map's write lock.
Orderly persistence exceptions leave the queue registered; unsuccessful lock
or unlock attempts retain Java's separate local-removal behavior.

The final extended fixture SHA-256 is
`ed123fb25e329d0166329dea123add868ac9846ffc251e2469970493691b9665`.
Live Java map/queue changes wait for the observed drain-sleep stack frame after
the clear latch, then mutate and interrupt the worker; first-pass iterator
prefetch timing is not used as synchronization.

The private monitor guard is shared by the caller and its owned control
command. Cancelling a caller waiting for an unregister response cannot let a
reset overtake the still-running RPC. An undelivered registration reply carries
that guard through identity cleanup, including time waiting for its raw-map
key gate. Normal caller post-processing also retains admission. These ownership
rules are checked by the scripted Broker cancellation case in `lifecycle` and
`undelivered_registration_retains_monitor_until_identity_cleanup_finishes`.
The monitor/envelope/guard are private transport-runtime machinery.

## Audit boundary

The additional `unsubscribedTruncation` Java probe invokes the real
`doRebalance` after a nullable reset leaves an unsubscribed queue dropped.
Java removes that queue from the table without persisting or removing its
offset. Rust now performs the same separate truncation, cancelling the native
pull assignment while retaining the offset. The comparison failed with a lost
offset before the fix and checks table presence, dropped state, retained value,
persisted value and store callback events. Fixture checkpoint SHA-256:
`2935595b5c94dc9496d342d004f3869cf3d66d5e5ca042c6ad445bae1ec712b4`.

The `resume` fixture also checks a suspended invocation followed by an already
unpaused invocation, with a new unsubscribed queue inserted before each call.
Both calls complete the selected owner's round before returning, while leaving
a sibling owner's queue alone. Rust compares both rounds using two consumers
sharing one runtime. A private weak owner reference preserves the existing
`resume(&self)` API without looking the owner up through replaceable raw group
registrations or introducing a strong reference cycle. No state lock is held
across the rebalance callbacks. Reset's cancellation guard is disarmed before
that round, so a callback's new suspend remains effective.
An additional native callback regression checks that state inspection is
available during the round and that a new pause survives finally's return.
The real-resume fixture checkpoint SHA-256 is
`ee07499b6b8eb58ea9cefc013592013a2c378e97709f4fd0096cd2ba81ec2eea`.

The first two `rebalanceRetention` cases keep the topic subscribed after a
null-value reset. A missing route or an unchanged broadcast assignment retains
the dropped queue and its in-memory offset, with no persist/remove events.
The Java probe explicitly installs its rebalance model and runtime to exercise
those branches. The Rust comparison initially lost the retained offset on the
missing-route case. Cleanup now requires an authoritative assignment excluding
the queue; the dropped flag alone does not authorize removal. Fixture SHA-256:
`0ed55128b1905a5d0c451ca93c0fbb33b6c5089f2a0a8c5e188770354293a04b`.

The third retention case exercises Java's `ProcessQueue.isPullExpired` path
with the topic subscribed and queue still assigned. Java removes the expired
queue, clears its dirty offset before trying to read a replacement start
offset, and leaves the table entry absent when the fixture offset-store read
fails. Rust now applies the same strict `elapsed > pullMaxIdleTime` condition
to push and POP process queues, using Java-compatible signed-millisecond
subtraction. The shared Java property is
`rocketmq.client.pull.pullMaxIdleTime`, default `120000`; Rust reads the same
dotted property name from the process environment when the ProcessQueue cache
is first initialized, normally during Push startup, and reports invalid
Java-long values as a startup error. Java has separate static fields on
`ProcessQueue` and `PopProcessQueue`; Rust likewise caches each owner path
independently and uses the matching cache when expiring Push versus POP
assignments. The Java fixture records the default and expired-assignment
lifecycle; the runtime comparison checks queue presence, dropped state, offsets
and callback order. Rust unit coverage also checks the strict boundary, signed
wrapping, Java-long parsing, and that later environment reads do not replace
either queue owner's cached value. A POP rebalance regression also sets
different ProcessQueue and POP timeout slots to check that queue cleanup uses
the POP value.
Fixture SHA-256:
`3d7af953b38b28a742a0d3eeb2671e8484066203f06ade2c829e2485ead1de4f`.

The `popRebalance` fixture invokes the Java assignment update for an expired
assigned POP queue, a dropped queue that remains assigned, and an authoritative
empty assignment. Rust compares table presence, old-generation dropped state
and identity replacement, then also scripts a failed assignment lookup to
verify the current queue and worker are retained without authority. The added
`popUnsubscribedTruncation` fixture invokes Java's topic-truncation helper with
an unsubscribed POP queue; the Rust rebalance regression checks that it marks
the queue dropped, removes its table entry, and retires its worker. Any already
issued response still follows Java's callback path: a FOUND result updates
pull statistics, filters messages, ACKs filtered messages, and dispatches
accepted messages to a consume request that exits on the dropped queue. The
worker then exits before issuing another POP request. A scripted Rust
regression verifies the filtered-message ACK after removal and confirms an
accepted message does not reach the listener after removal. The
`popToPullTransition` fixture invokes a PULL assignment while the same queue
has an active entry in both tables; Java and Rust both remove the POP entry,
retire its worker, and retain the existing PULL queue. Remaining POP assignment
branches and the public Rebalance/ProcessQueue API audit remain open. Current
fixture SHA-256 is
`b414477a4a64b2c252d704704a70c7b1a89f2b6a5b1014531ca0200690edda36`.

The `popRetryTransitions` fixture now invokes Java's actual
`RebalanceImpl.updateMessageQueueAssignment` for pop-to-push, push-to-pop,
mixed-mode, retry-topic and empty assignments. The five cases compare retry
subscription presence and cached route presence. Java removes the retry
subscription on push-to-pop but retains the cached route; Rust now preserves
that route and matches the subscription branches. The added
`broker_assignment_modes_match_java_pop_retry_subscription_transitions` test
checks the same cases. Current fixture SHA-256:
`104b9fd505b227abc8a8d1f5e35e036ba89989cbfbb4d9725deb13fc87889565`.
Uncovered assignment expiry/retention dimensions and the public
Rebalance/ProcessQueue API audit remain open.

The nested ProcessQueue review found a missing concurrent-consume expiry flow.
Java `ConsumeMessageConcurrentlyService` records `CONSUME_START_TIME`, then
runs `cleanExpiredMsg` after one `consumeTimeout` period and at that interval.
Rust now records the same property for listener messages, tracks it on the
queue-owned entry, and runs an owned concurrent-service timer. The sweep checks
only the oldest cached message, uses Java's strict signed-millisecond timeout,
sends back at delay level 3, removes the head only after successful send-back,
caps attempts at 16 per queue, and skips orderly consume. Java invokes the
actual `ProcessQueue.cleanExpiredMsg` with scripted outcomes; Rust compares
success, repeated send-back failure and orderly skip, plus the 16-attempt cap,
strict boundary, late listener completion and initial timer delay. Fixture
SHA-256:
`0ecf6ca6795174a96a90123cebb5415c2108cc81842ce84469e721d599f90d01`.

The expiry audit also covers malformed `CONSUME_START_TIME`: pinned Java's
`Long.parseLong` raises `NumberFormatException` before its send-back catch,
leaving the head in the queue and recording no attempt. Rust retains the raw
value and returns `ExpiredMessageCleanupError` from the public direct cleanup;
the scheduled owner logs that same error and skips the rest of that sweep, as
Java's outer scheduled `catch(Throwable)` does. A direct send-back failure
retries the unchanged head until the original queue-size loop ends, up to 16
attempts. Additional Java/Rust cases verify that missing and empty timestamps
skip cleanup, Java Unicode decimal digits parse as timestamps, and orderly
mode returns before parsing even a malformed value. The fixture records the
malformed timestamp, empty attempts, and unchanged queue count/bytes.

The pinned nested public API denominator and its current owner/queue mappings
are recorded in the [Rebalance and ProcessQueue API audit](PHASE_18_REBALANCE_PROCESS_QUEUE_API.md).
Rust now exposes both queue types, direct Java-shaped queue behavior, mutable
message-tree/counter handles and live Push/Pull process-queue tables. Pull
assignment reconciliation updates its process table. Pull POP behavior now
matches Java's no-queue/no-dispatch implementation and is fixture checked.
The inherited rebalance operations map to owner methods; Java's raw factory
replacement and mutable request-list dispatch stay represented by the managed
runtime and owned request scheduler. The broader Java client source/settings/
flow and reverse API audit under P18-V01 remains open.

The direct Running Info, direct-consume and reset runtime rows above are now
implemented with scoped offline evidence. Their existence does not prove
completion of the complete Java/Rust public API/settings/control-flow audit.
Broad P18-V01 remains open and live evidence remains `PENDING_PHASE_19`.

P18-V01 caller-defined inherited scheduling-property follow-up (code revision
`c6048c18`, 2026-10-21): the direct-consume `MessageExtSubtype` adapter routes
wait-store, buyer ID, delay-level, delay-seconds, delay-milliseconds and
delivery-milliseconds reads through the virtual property hook. It dispatches
the matching wait-store, buyer ID, instance ID, delay-level, delay-seconds,
delay-milliseconds and delivery-milliseconds setters. The Java fixture invokes
the pinned 5.5.1 methods and records custom getter values and seven setter
calls; Rust matches them. Fixture SHA-256:
`5148df4ac22baa84eb75326238a8d5903927ba1c0380dc34b7cc4093a47bf446`.
Other inherited methods, custom live property-map identity, broad P18-V01 and
current-revision P18-V04 platform evidence remain open.

P18-V01 caller-defined user-property and transaction follow-up (code revision
`27c872bc`, 2026-10-21): `MessageExtSubtype` supports independently overridden
`getUserProperty`, `hasProperty`, and transaction-ID getter/setter methods.
It dispatches `putUserProperty`, `setTags` and `setKeys`; Java's collection
`setKeys` overload forwards through the String setter. The direct-consume
fixture compares custom results and write counts, including empty and null
user-property inputs accepted by the custom override. Fixture SHA-256:
`33de50fa577340aa50df1637f1d2f6aa32ae8d23e5e03ba21a2a012630e37542`.
The remaining inherited methods, Java live property-map identity, broad
P18-V01 audit and P18-V04 platform evidence remain open.

P18-V01 caller-defined IPv6 host-flag setter follow-up (code revision
`c7354649`, 2026-10-21): Rust routes `setBornHostV6Flag` and
`setStoreHostAddressV6Flag` through subtype hooks. The Java fixture overrides
both methods and compares the custom write count with Rust. The base path still
sets each Java sysFlag bit. Fixture SHA-256:
`7cf4e8040b8e040117207c8819d67f1dd7bf7f9f0daa6374ce0f7c345bf9171c`.
The remaining inherited methods, Java live property-map identity, broad
P18-V01 audit and P18-V04 platform evidence remain open.

P18-V01 live subtype property-map follow-up (code revision `158a049f`,
2026-10-21): Rust subtype adapters can return a borrowed
`SharedMessageProperties` from `MessageExtSubtype::live_properties`. The
invoked Java fixture and Rust direct-consume path verify that the returned map
and subtype-owned map share identity, mutations are visible in both
directions, and a later `getProperties()` read sees prior changes. Fixture
SHA-256: `8782177ecb34305bc00642fffa09420c1fd12ecfe4c8744d450bd4a290ff0d01`.
The broad inherited-method/API audit and P18-V04 platform evidence remain open.

P18-V01 born-host string getter override follow-up (code revision `7473872c`,
2026-10-21): the Java fixture overrides `getBornHostString()` and
`getBornHostNameString()` on its custom `MessageExt`; Rust dispatches both
through subtype hooks and matches the returned strings. The base fallback
continues to derive values from the stored host address. Fixture SHA-256:
`dec0f7c46539bec116f6430c7b02fdde406a289929fc093d5694dd4c9369bd54`.

P18-V01 born/store host-byte override follow-up (code revision `625e80c6`,
2026-10-21): Rust dispatches Java's no-argument `getBornHostBytes()` and
`getStoreHostBytes()` plus both `ByteBuffer` overloads through subtype hooks.
The invoked Java fixture overrides all four calls; direct-consume comparison
matches returned bytes and caller-buffer writes. Rust represents the
`ByteBuffer` overloads with mutable output slices and a returned byte count,
without Java buffer identity or position state. Fixture SHA-256:
`30af1bcbb2cfbf967f636fbbf7cbf9d54ba886a68c25fbe92df5ddf88000946c`.
Other inherited methods, broad P18-V01 and P18-V04 platform evidence remain
open.

P18-V01 caller-defined priority override follow-up (code revision `da1f4b1e`,
2026-10-21): the runtime subtype adapter dispatches Java's virtual
`getPriority()` and `setPriority(int)`. The Java direct-consume fixture
overrides both, accepts `-7`, and compares the custom value, property lookup,
and read/write counts. A property-only Java subtype checks that inherited
`getPriority()` calls virtual `getProperty`; its inherited setter stores `17`
in the base map and rejects negative input. Rust matches both paths. Fixture
SHA-256: `44eb8359e7df5735cb527a59942190fb74dd4191306a4290ea3bba644a777a5c`.
This closes priority override behavior only; the broader inherited-method and
P18-V01 source/API/input/error/flow/reverse-export audits remain open.

`MessageClientExt` subtype offset-ID continuation (2026-10-21): a custom Java
`MessageClientExt` can override `getOffsetMsgId` and `setOffsetMsgId`. Rust now
dispatches both through `MessageExtSubtype`; inherited `getMsgId` reads the
live `UNIQ_KEY` first and falls back through the virtual offset getter. The
invoked fixture adds two direct-consume cases, with and without `UNIQ_KEY`, and
the Rust Common test checks wrapper dispatch, base no-op `setMsgId`, live
unique-ID precedence, and conversion to `MessageExt`. The current direct
runtime fixture has 29 cases and SHA-256
`403b79b69f30957ae2f935be37ec0b8537696909d91066d2126f216eff31996d`.
Follow-up code revision `f7d7e8f1` also pins `setOffsetMsgId(null)`: it clears
the offset ID and effective fallback, and conversion to a base-typed
`MessageExt` preserves the cleared state. The Common message fixture SHA-256
is `1adc11e14bf1af0d39966ecda166a3ddb8a24c0e7d13bbf2d5a9830bdaac64dc`.
The Common `messageClientExtApi` fixture additionally covers a custom
`getMsgId`/`setMsgId` override: the custom getter wins over `UNIQ_KEY`, and the
custom setter handles null without changing the offset. Test revision
`5e12a174`; current Common fixture SHA-256:
`d0cdfbf2599adb0242a1df0dfa8523818a018c35cb34dac2b8418f8ce4bc972d`.

## P18-V01 message-family method and override audit follow-up — 2026-10-23

The earlier entries above that leave inherited `Message`/`MessageExt` methods
open are superseded for the declared message-family inventory by
[PHASE_18_MESSAGE_SUBTYPE_API_CROSSWALK.md](PHASE_18_MESSAGE_SUBTYPE_API_CROSSWALK.md).
That crosswalk is based on pinned Java 5.5.1 and accounts for the 82 declared
public methods across `Message`, `MessageExt`, and `MessageClientExt`, their
constructors, and the three `MessageExt` static helpers. It also records the
Rust subtype hooks for virtual calls, nullable getter results, live property
map aliases, and the language-specific ByteBuffer slice mapping.

Current-tree focused validation passed at code revision `34cbfb8b` (HEAD
`5e98f701` adds only the preceding documentation audit):
`cargo test --offline -p rocketmq-classic-common --test message_decode --locked`
(27 passed) and
`cargo test --offline -p rocketmq-classic-consumer --test runtime_direct_consume --locked`
(6 passed). Current aggregate fixture SHA-256 values are
`consumer-operations.json`:
`b414477a4a64b2c252d704704a70c7b1a89f2b6a5b1014531ca0200690edda36`, and
`records.json`:
`46a18677fc673222639e53ee2871f4836fa427f97d284f918c1bdeb57129beb0`.
The direct-consume fixture includes 29 Java cases. This closes the declared
message-family inventory and these selected subtype dispatch paths; the broad
P18-V01 settings/API/flow/input/error/reverse-export audit and P18-V04 platform
evidence remain open.

## Caller-defined `MessageExt` body-array aliases — 2026-10-24 (`950cf356`)

`MessageExtSubtype::{shared_body,set_shared_body}` and the corresponding
`MessageExt` shared-handle methods preserve the byte-array aliases that Java
subclasses expose through `getBody()` and `setBody()`. The direct-consume Java
fixture mutates the setter's input array before the callback, then mutates the
getter result and subtype field in turn. The Rust callback checks shared
identity and visibility in both directions. The regenerated
`consumer-operations.json` fixture SHA-256 is
`4ba7bf76f4ef8ebb1ecc116f253d5b8e64100cf83096e65ff86958425f403b5c`.
`cargo test --offline --locked -p rocketmq-classic-common --test
message_decode` passes 28 cases; the matching `runtime_direct_consume` suite
passes six. The broad P18-V01 source/API/input/error/flow/reverse-export audit
and P18-V04 platform evidence remain open. On code revision `950cf356`, both
default and all-feature workspace test/doctest runs pass 93 suites and 2,019
tests. Strict workspace Clippy, release examples, Windows GNU check/Clippy
(compile-only), and offline Maven `verify` pass as well. Maven verified 37
scenarios, 11 message fixtures, 106 typed headers, and 19 typed bodies. Native
Windows runtime and current-revision remote Ubuntu CI remain unverified.
