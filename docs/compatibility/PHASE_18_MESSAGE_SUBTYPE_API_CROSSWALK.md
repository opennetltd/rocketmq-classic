# Phase 18 Java message-family API crosswalk

## Source boundary

This inventory uses the pinned RocketMQ Java 5.5.1 source at commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`, not the separate Java working tree.
The source-declared public, non-static instance-method counts are 34 for
`Message`, 41 for `MessageExt`, and 4 for `MessageClientExt`. Constructors are
listed separately: `Message` declares five, `MessageExt` declares two, and
`MessageClientExt` inherits its public no-argument constructor from Java's
implicit default-constructor rule. `MessageExt` also declares three public
static methods; the other two classes declare none. Together the classes
declare 82 public methods (79 instance and 3 static), excluding constructors.

## `Message`

| Pinned Java methods | Rust API and behavior |
| --- | --- |
| Constructors `Message()`, `(String, byte[])`, `(String, String, byte[])`, `(String, String, String, byte[])`, and `(String, String, String, int, byte[], boolean)` | `Message::default`, `Message::new`, `with_tags`, `with_tags_and_keys`, and `with_fields`, respectively; nullable/shared variants retain Java null and byte-array aliasing where Rust callers need them. |
| `getTopic`, `setTopic`, `getBody`, `setBody`, `getFlag`, `setFlag` | `topic[_nullable]`, `set_topic[_nullable]`, `body[_nullable]`, `set_body[_nullable]`, `flag`, and `set_flag`. `MessageExtSubtype` supplies virtual getter/setter hooks when the value crosses a base-typed consumer API; `shared_body` and `set_body_shared` preserve caller/subtype array aliases. |
| `putUserProperty`, `getUserProperty`, `getProperty`, `hasProperty`, `getProperties` | `put_user_property[_nullable]`, `get_user_property`/`get_nullable_user_property`, `get_property`/`get_nullable_property`, `has_property`/`has_nullable_property`, and the nullable properties views. Subtypes can override `property`, `user_property`, `has_property`, and `properties` or provide a live map with `live_properties`. |
| `getTags`, `setTags`, `getKeys`, `setKeys(String)`, `setKeys(Collection<String>)` | `tags`, `set_tags[_nullable]`, `keys`, `set_keys[_nullable]`, and `set_keys_from[_nullable]`; direct getter overrides map through `MessageExtSubtype::{tags,keys}`. The collection setter funnels through the String setter, preserving Java virtual dispatch. |
| `getDelayTimeLevel`, `setDelayTimeLevel`, `setPriority`, `getPriority`, `isWaitStoreMsgOK`, `setWaitStoreMsgOK`, `setInstanceId`, `setDelayTimeSec`, `getDelayTimeSec`, `setDelayTimeMs`, `getDelayTimeMs`, `setDeliverTimeMs`, `getDeliverTimeMs` | Matching `Message`/`MessageExt` methods preserve property names, defaults and Java parse/validation behavior; malformed numeric properties return typed Rust parse errors at the Rust API boundary. Direct getter overrides map through `MessageExtSubtype::{delay_time_level,delay_time_sec,delay_time_ms,deliver_time_ms,wait_store_msg_ok}`; the inherited implementation uses the virtual property path. |
| `getBuyerId`, `setBuyerId`, `getTransactionId`, `setTransactionId`, `toString` | `buyer_id`, `set_buyer_id[_nullable]`, `transaction_id`, `set_transaction_id`, and `Display`/`.to_string()`. The subtype adapter dispatches the transaction accessors and display override. |

## `MessageExt`

| Pinned Java methods | Rust API and behavior |
| --- | --- |
| Constructors `MessageExt()` and `(int, long, SocketAddress, long, SocketAddress, String)` | `MessageExt::new`/`Default` and `with_socket_addresses`. `MessageClientExt::new` preserves the client-extension runtime type. |
| Broker and queue accessors: `getBrokerName`, `setBrokerName`, `getQueueId`, `setQueueId`, `getQueueOffset`, `setQueueOffset` | `broker_name`, `set_broker_name`, `queue_id`, `set_queue_id`, `queue_offset`, and `set_queue_offset`; matching subtype hooks dispatch Java virtual overrides. |
| Message metadata: born/store timestamp, `get/setSysFlag`, IPv6 host-flag setters, body CRC, commit-log offset, store size, reconsume times, prepared transaction offset | Rust accessors and setters with the corresponding names in `MessageExt`; `MessageExtSubtype` contains paired override hooks for every getter/setter and both IPv6 flag setters. |
| `getBornHost`, `setBornHost`, `getStoreHost`, `setStoreHost`, `getBornHostString`, `getBornHostNameString` | `born_host`, `set_born_host`, `store_host`, `set_store_host`, `born_host_string`, and `born_host_name_string`, including nullable host values and subtype dispatch. |
| `getBornHostBytes()`, `getBornHostBytes(ByteBuffer)`, `getStoreHostBytes()`, `getStoreHostBytes(ByteBuffer)` | `born_host_bytes`, `write_born_host_bytes`, `store_host_bytes`, and `write_store_host_bytes`. The byte-array results and bytes written are compared; Java `ByteBuffer` identity and position are represented by a Rust mutable slice and returned byte count. |
| Static `parseTopicFilterType(int)`, `socketAddress2ByteBuffer(SocketAddress)`, `socketAddress2ByteBuffer(SocketAddress, ByteBuffer)` | `parse_topic_filter_type`, `MessageHostAddress::network_bytes`, and `MessageHostAddress::write_network_bytes`. |
| `getMsgId`, `setMsgId`, `getTopicSysFlag`, `setTopicSysFlag`, `getGroupSysFlag`, `setGroupSysFlag`, `toString` | `message_id_for_runtime_type`, `set_message_id`, `topic_sys_flag`, `set_topic_sys_flag`, `group_sys_flag`, `set_group_sys_flag`, and `Display`/`.to_string()`. Direct transient-flag getter overrides map through `MessageExtSubtype::{topic_sys_flag,group_sys_flag}`; the runtime subtype adapter dispatches the other virtual methods. |

`MessageExt` carries its `Message` fields and exposes the same property, body,
topic, keys/tags, priority and delay behavior. Property-derived Java getters
therefore use the subtype's virtual `getProperty` hook rather than reading a
detached property snapshot. For caller-defined subtypes, `shared_body()` and
`set_body_shared()` retain the live byte-array identity exposed by virtual
`getBody()`/`setBody()`; the snapshot-returning hooks remain available.

## `MessageClientExt`

| Pinned Java methods | Rust API and behavior |
| --- | --- |
| `getOffsetMsgId`, `setOffsetMsgId` | `offset_message_id`, `set_offset_message_id`, with `offset_message_id` and `set_offset_message_id` subtype hooks. |
| `getMsgId`, `setMsgId` | `message_id`, `set_message_id`, with matching subtype hooks. The base getter first checks `UNIQ_KEY`, then calls the virtual offset-ID getter; the base setter is a no-op, matching Java. |

`MessageClientExt` dereferences to `MessageExt` for inherited methods. Conversion
to `MessageExt` retains the runtime client/subtype state, as a Java subclass
passed through a base-typed parameter does.

## P18-V01 direct derived-getter overrides — 2026-10-22

`MessageExtSubtype` now separately represents direct overrides of Java's
`getTags`, `getKeys`, `isWaitStoreMsgOK`, `getBuyerId`, the three delay
getters, `getDeliverTimeMs`, `getTopicSysFlag`, and `getGroupSysFlag`. These
hooks take precedence over the equivalent base getters' `getProperty` path;
without a direct override the inherited property lookup and parsing behavior
is unchanged. The `DerivedGetterMessageExt` Java oracle returns values distinct
from its property values and records all ten calls. The Rust test compares all
ten outputs, property lookups, and the call count. Fixture SHA-256:
`0d818f3b8a711661a382de19a07db7292c5976f20ddee9a6106b78b9e8860f82`.
Common tests, the direct-consume suite, full default/all-feature workspace
tests, strict Clippy, and pinned Maven verification pass for this tree. The
direct-override case closes only this virtual-getter slice; the broader
Phase 18 audit remains open.

## Override and flow evidence

The complete subtype adapter lives in
`crates/rocketmq-classic-common/src/message/types.rs` as
`MessageExtSubtype`. Its nullable hook results distinguish “inherit the base
value” from an explicit Java `null`. The direct-consume Java oracle invokes
caller-defined `MessageExt` and `MessageClientExt` subclasses through
`MQClientInstance.consumeMessageDirectly`; the Rust owner path is exercised by
`crates/rocketmq-classic-consumer/tests/runtime_direct_consume.rs`.

Evidence checked on 2026-10-21:

- `fixtures/runtime/consumer-operations.json` SHA-256:
  `a8d82baca384b9b7932b4961b051902336155543428047d891eb9f84052d166d`.
- `fixtures/messages/records.json` SHA-256:
  `d0cdfbf2599adb0242a1df0dfa8523818a018c35cb34dac2b8418f8ce4bc972d`.
- `cargo test -p rocketmq-classic-common --test message_decode --locked`:
  26 passed.
- `cargo test -p rocketmq-classic-consumer --test runtime_direct_consume --locked`:
  4 passed.
- `mvn -f compat/java-reference/pom.xml verify`: passed; regenerated and
  checked 29 direct-consume cases, message fixtures, typed-header/body
  cross-decodes and the rest of the pinned offline corpus.

This closes the declared public method and selected virtual-override inventory
for these three message classes. It supersedes earlier notes that left other
`Message`/`MessageExt` overrides or the live subtype properties-map identity
open. It does not close the broader Phase 18 audit of client settings, other
Java classes, call flows, inputs/errors, or reverse Rust exports.

The nullable-getter continuation adds direct Java overrides that return null
for `getTags`, `getKeys`, `getBuyerId`, `getTopicSysFlag`, and
`getGroupSysFlag`, despite non-null or malformed values from `getProperty`.
The Rust fixture confirms explicit null wins over the property path and avoids
parsing malformed transient sysflags. It also compares the five override-call
count. The updated runtime fixture SHA-256 is
`8640635c5eb044d1485c182be377a308f909885477d01916ba234fb34ef70b7b`.

## P18-V01 caller-defined body-array aliases — 2026-10-24 (`950cf356`)

The `MessageExtSubtype` adapter now has shared setter/getter hooks for custom
`Message.getBody()` arrays. `MessageExt::set_body_shared` forwards the caller's
handle through the subtype setter; `shared_body` returns the live subtype
handle. The invoked Java direct-consume case verifies the original array is
retained by `setBody`, caller mutation reaches the subtype getter, mutation
through `getBody` reaches the subtype field, and subtype mutation is visible
through a later getter. The Rust differential checks the same identity and
mutation directions. Fixture SHA-256:
`4ba7bf76f4ef8ebb1ecc116f253d5b8e64100cf83096e65ff86958425f403b5c`.
`message_decode` passes 28 cases and `runtime_direct_consume` passes six.
This closes body-array aliasing for the mapped custom subtype adapter only;
the broader P18-V01 source/input/control-flow/reverse-export audit stays open.
