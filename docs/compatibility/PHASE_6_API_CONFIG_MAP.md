# Phase 6 API and configuration map

## Current Fastjson writer-feature closure (2026-10-21)

The pinned Fastjson 2.0.63 `JSONWriter.Feature` surface is mapped 48/48.
Commit `07d2fba1` closes the last feature, `WriteClassName`: Java and Rust
fixtures compare root DTO metadata, polymorphic DTO/list/set members, typed
maps and `MessageQueue` map keys, `Set` markers, long-field `L` suffixes, and
the combined `NotWriteRootClassName`,
`NotWriteHashMapArrayListClassName`, and `NotWriteSetClassName` policies.
The current implementation coverage is in
`crates/rocketmq-classic-protocol/src/writer_feature.rs`, the body/route/
heartbeat feature loops, and the Phase 18 entry in `docs/release-checklist.md`.
This supersedes earlier writer-feature status paragraphs below. The broad
P18-V01 API/source/input/error/control-flow/reverse-export audit remains open.

## P18-V01 Rustdoc public-field reverse inventory (2026-10-19)

`RUSTDOCFLAGS='-Z unstable-options --output-format json' cargo +nightly doc
--workspace --no-deps --locked` exposed 245 public struct fields across 41
named-field structs before the `ClientGroup` correction below; no public named
enum-variant fields were present. The first inventory's crate counts were
Common 4, Consumer 30, Core 66, Producer 32, Protocol 58, and Remoting 55.
After hiding the two `ClientGroup` fields, the current total is 243: Protocol
56 and the other crate counts are unchanged.

| Crate | Rust types and field counts | Java/API disposition |
| --- | --- | --- |
| Common | `DecodeOptions` (4) | Per-call decode options map Java decoder overloads; these do not add a persistent setting. |
| Consumer | `ProcessedPullResult` (2), `LiteTraceBean` (7), `ConsumeMessageContext` (9), `LiteTraceContext` (10), `FilterMessageContext` (2) | Consumer result, hook, and trace values carried through existing Java consumer flows. |
| Core | `PullResultExt` (7), `ChangeInvisibleTimeResult` (5), `TopicSnapshot` (5), `RouteSnapshot` (3), `SendResult` (12), `TraceContext` (11), `PopReply` (3), `HeartbeatV2Result` (3), `BrokerAddress` (3), `TraceBean` (14) | Java pull/POP/send/trace values and route/cache views. `SendResult` keeps the Java `MessageQueue` values as Rust `topic`/`broker_name`/`queue_id` fields; this is a flattened carrier, not another send option. |
| Producer | `EndTransactionContext` (7), `CheckForbiddenContext` (10), `TransactionSendResult` (2), `SendMessageContext` (13) | Producer hook contexts and transaction results correspond to existing Java hook/result properties. |
| Protocol | `OffsetOption` (2), `LiteSubscriptionCtlRequestBody` (1), `LiteSubscriptionDto` (7), `LiteSubscription` (4), `ClientGroup` (2 before correction), `SubscriptionData` (8), `QueueData` (5), `TopicQueueMappingInfo` (7), `ProducerData` (1), `ConsumerData` (6), `TopicRouteData` (5), `BrokerData` (5), `HeartbeatData` (5) | Typed wire/route/heartbeat values. Java bean getter/setter properties use direct Rust fields as their idiomatic read/write path; `ClientGroup` is the exception because Java's two public fields are `final`, so Rust now provides read-only accessors. |
| Remoting | `TlsConfig` (8), `ChannelEvent` (4), `RemotingCommand` (10), `MetricView` (5), `ProcessorRegistration` (2), `ClientConfig` (20), `GoAwayCause` (2), `SocksProxyConfig` (3) | TLS, transport config, command, event, registration, metrics, and diagnostic carriers map to existing Java remoting settings/operations. `ClientConfig` settings map Java `NettyClientConfig`; no additional Rust-only persistent transport setting was found. |

This is a surface inventory and a disposition of field-shaped APIs, not a claim
that every mapped method, null boundary, input shape, error, or control-flow
branch has passed the broader P18-V01 audit.

ClientConfig field-visibility reconciliation (2026-10-15, pinned Java source
`efa1be59d6fc747ad220f036c86030f05f0419f9`): Java 5.5.1 keeps the 33
configuration values behind getter/setter methods. Rust now keeps the backing
fields crate-private and preserves the mapped accessors; an external compile-
fail doctest guards against direct field access. The separate Consumer package
uses the hidden, read-only `ClientConfig::to_java_properties` bridge to model
Java `MixAll.object2Properties` without exposing mutable storage. The current
reverse field inventory is eight public-field structs / 41 public fields; the
older 9 / 74 count below predates this correction. Core configuration
differentials (21 tests), Consumer all-feature tests (219 unit tests plus all
integration/doc targets), and Producer all-feature tests pass. This closes the
ClientConfig field-visibility slice only; broad P18-V01 source/API/input/flow
and reverse-export reconciliation remains open.

Heartbeat DTO Fastjson syntax continuation (2026-10-12): the invoked Java corpus now has 276 cases, extending the previous 249-case checkpoint by 27. The additions pin Fastjson 2.0.63's `char1` escape table, `\x` lookup boundaries, and parser-state-specific comment positions. The differential covers accepted `\0`–`\7`, `\v`, `\F`, identity escapes, `\'`, and `\xG1` (U+0001); it also confirms rejection of `\8`, `\q`, and `\xg1`. Single-quoted keys/strings and plain object/array trailing commas accept; unquoted keys reject. Comments are position-sensitive: the recorded object/array starts, object-member boundary, nested-object-value boundary, and root boundaries accept, while comments after a colon, after an array comma, after ordinary values, or after a trailing comma reject. The 276-case Rust differential passes. Current `heartbeat.json` SHA-256: 67b844d3a61ee31ac12c340cf09f30d81083435635d6f17bebc44206b65d37cc. This closes only these corpus cases; broader Fastjson and Phase 6 source/input reconciliation remain open.

Earlier heartbeat DTO syntax checkpoint (2026-10-12; superseded by the 276-case continuation below): eight direct pinned Java cases extend the heartbeat input corpus from 241 to 249. Fastjson accepts single-quoted strings (including escaped apostrophes and embedded double quotes), line/block comments and object/array trailing commas for these DTOs, while an unquoted field name rejects. Comment markers within quoted values remain string content. At this checkpoint, the shared `serde_fastjson` normalizer converted those observed forms before Serde parsing; the 249-case DTO differential and two normalizer unit tests passed. Checkpoint `heartbeat.json` SHA-256: `98b5029aafaa9146a801e3a68d7edc70707f25ba4c17d3ceca40012e4673e0b3`. This closes these grammar cases only; broader Fastjson and Phase 6 source/input reconciliation remain open.

Historical public struct field reverse scan (`2026-10-10`): the core `api/`, `runtime/`, and `route/` modules plus `config.rs` contained nine named-field structs and 74 public fields across 22 source files. Every field name appeared in this map, including the result and `TopicSnapshot` tables below and the complete `ClientConfig` table above. The 2026-10-15 correction above removes the 33 `ClientConfig` fields from the public-field count; field behavior, remaining source branches, malformed inputs, and platform-specific runtime enumeration remain separately tracked, and P6-V04 remains a Phase 19 handoff.

Pinned Java public method reverse scan (`2026-10-10`): `javap -public` over the seven phase-reference classes `MQClientManager`, `MQClientInstance`, `ClientConfig`, `TopicPublishInfo`, `ClientRemotingProcessor`, `RebalanceService`, and `PullMessageService` found 169 public method declarations; each method name appears in this map. Constructor declarations are excluded. This closes method-name inventory for those classes only; overload/input behavior, protected/private source branches, and platform runtime evidence remain separately tracked, and P6-V04 remains a Phase 19 handoff.

Change-invisible-time callback result (`4da236e1`, 2026-10-10): Java `MQClientAPIImpl.changeInvisibleTimeAsync` builds `AckResult.extraInfo` from the request offset/topic/queue, broker name, and response pop/invisible/revive fields. Rust's `change_invisible_time` now accepts the broker name and returns nullable `extra_info` alongside the parsed response metadata. It matches the Java retry discriminator (`0` ordinary, `1` `%RETRY%group`, `2` `%RETRY%group+...`), and leaves status/error results without extra info. Java invoked callbacks cover ordinary and both retry-topic forms; the existing response fixture differential checks success and malformed primitive defaults. Focused extension (14), POP/ACK differential (1), RPC (11), helper unit and pinned offline Maven verify (22 scenarios) pass. This closes the change-invisibility result branch only; wider result-layer and source/input/platform reconciliation remains open, and P6-V04 stays a Phase 19 handoff.

`SendResult` public field alignment (`b2a00689`, 2026-10-10): the pinned Java 5.5.1 result has public getter/setter pairs for send status, IDs, message queue, queue offset, transaction ID, recall handle, raw response body, region and trace flag. Rust now exposes nullable `raw_resp_body: Option<Vec<u8>>` and initializes it to `None`, as the Java `MQClientAPIImpl` send path does. Rust's former `batch_uniq_id` result field was removed: `batchUniqId` belongs to `SendMessageResponseHeader`, not Java `SendResult`; the protocol header mapping remains. A direct Java invoked fixture checks null raw-body defaults for sync, async and batch sends; Rust producer regressions assert the same. Producer loopback tests and pinned offline Maven verify pass. This reconciles this result type only; broader public-struct field and Phase 6 source/input/platform scans remain open, and P6-V04 remains a Phase 19 handoff.

Public enum variant reverse scan (`2026-10-10`): a variant-level pass over the core crate's 17 public enums found that several maps named the Java enum values or Rust enum type but omitted the exact Rust variants. The API map now records all 72 declared variants, including the Rust-only send selectors and lifecycle/error carriers; `RuntimeError::StartupFailure` is explicitly marked test-only. Existing invoked enum, RPC, lifecycle and inbound fixtures remain the behavior evidence. This closes the variant-name inventory only; broader input/source/platform reconciliation remains open and P6-V04 remains a Phase 19 handoff.

Public `ConfigError` result mapping (`89e23102`, `c48db47d`, 2026-10-10): the Rust error carrier now lists only the four source-backed Java failure branches and maps each to its originating ClientConfig operation. The unused `LocalAddress` variant was removed because Java catches address-discovery failures and keeps the null `clientIP` default; a new pinned Java oracle records `changeInstanceNameToPID()` throwing `NullPointerException` for a null instance name, and the Rust differential maps it to `NullInstanceName`. The complete variant mapping and lazy queue short-circuit behavior are listed below. `client-config.json` SHA-256: `eb4028b6c5ce5cfce6e86ae0ec08110200c4d8bcebf67bcc2854b2e929d87ff0`. Focused Rust regression and full pinned offline Maven verify (22 scenarios and fixture checks) pass. Whole Phase 6 API/settings/source/input/platform reconciliation remains open; P6-V04 remains a Phase 19 handoff.

Nullable raw route-map callback arguments (`7fafce29`, 2026-10-10): direct pinned Java calls show `forEach(null)`, `replaceAll(null)`, `computeIfAbsent(key, null)`, `computeIfPresent(key, null)`, `compute(key, null)`, and `merge(key, value, null)` each throw `NullPointerException` without changing the mapping. Rust adds nullable callback adapters for all six methods; `None` maps to `InvalidRoute("null topic route callback")`. The differential checks all failures and retained mapping, adding seven outcomes to reach 149 `runtimeTopicRouteTable.primitives`. `route.json` SHA-256: `2be7e35eea6de3a0b16eb04d18eb39e579dd8ad359002760c12e6bca0bbb84e9`. Full locked workspace tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Full Phase 6 API/settings/source/input/platform reconciliation remains open; P6-V04 remains a Phase 19 handoff.

Nullable raw route-view `removeIf` predicates (`db33a61e`, 2026-10-10): pinned Java `keySet`, `values`, and `entrySet` each throw `NullPointerException` for `removeIf(null)` before visiting or changing the map. Rust adds `remove_if_nullable` to all three `TopicRoute` views and maps `None` to `InvalidRoute("null topic route predicate")`; the differential confirms the original mapping remains. Four outcomes raise `runtimeTopicRouteTable.primitives` from 138 to 142. `route.json` SHA-256: `99c9efe5783c7b7d91f20d762b16e869a0e93f819315e8a90a8cb01d2acc7103`. Focused route-table, full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting and diff checks pass. Whole Phase 6 API/settings/source/input/platform reconciliation remains open; P6-V04 remains a Phase 19 handoff.

Nullable raw route view bulk collections (`78da092c`, 2026-10-10): pinned Java `removeAll(null)` and `retainAll(null)` on keySet, values, and entrySet each throw `NullPointerException` before mutation. Rust adds `remove_all_nullable` and `retain_all_nullable` to all three live view types; a null collection maps to `InvalidRoute("null topic route collection")`. Six exception outcomes and the retained-mapping check raise `runtimeTopicRouteTable.primitives` from 131 to 138; `route.json` SHA-256 is `947f53b3743fb46c264856b5912d7cf3f01583efa9570ba5eb83d99627e41743`. Focused differential, full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Full Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Nullable raw route collection-view arguments (`8a0dedb0`, 2026-10-10): pinned Java key-view null `contains` / `remove` and values-view null `contains` throw `NullPointerException`; values-view `remove(null)` returns false. Entry-set `contains` / `remove` return false for entries with null keys or values. Rust adds nullable adapters on the matching live views, and the differential verifies no mapping changes. Nine outcomes raise the route-table oracle from 122 to 131; `route.json` SHA-256 is `625af93e11f18a2391bce16cf2f1188e7e3fc66f2c8988da420d49550ea634ba`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Nullable raw route remapping arguments (`91edf0da`, 2026-10-10): pinned Java `computeIfAbsent`, `computeIfPresent`, `compute`, and `merge` reject null keys before invoking callbacks; `merge` also rejects a null value before callback. Rust adds nullable-key adapters and a nullable-value adapter for merge. Seven outcomes verify typed errors, zero callback calls, and mapping retention; the route-table oracle rises from 115 to 122. `route.json` SHA-256 is `9a4c8a5f5fa0f8cfdb42c0d620d4e8d470fdfc25535b7b1d78e9d39bdde1d0c6`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Nullable raw route map arguments (`dcf1c85b`, 2026-10-10): pinned Java `ConcurrentHashMap` rejects null inputs to `containsValue`, `put`, `putIfAbsent`, and both `replace` overloads with `NullPointerException`; conditional `remove(key, null)` instead returns false. Rust exposes nullable adapters for these operations, preserving the existing mapping on all rejected inputs. Twelve outcomes raise the route-table oracle from 103 to 115; `route.json` SHA-256 is `0dd69fb5f280f8c7d1721f6275a01da810d87a55576aacc1517f9291d0db5923`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Raw route nullable `containsKey` and `remove` keys (`bd33f32d`, 2026-10-10): pinned Java `ConcurrentHashMap.containsKey(null)` and `remove(null)` throw `NullPointerException`; both calls leave the existing route mapping intact. Rust adds `TopicRouteTable::contains_key_nullable` and `remove_nullable`, mapping null keys to typed `InvalidRoute("null topic route key")`. Four primitive outcomes raise the route table oracle from 99 to 103; `route.json` SHA-256 is `d32ce77c95736db747c5302041eb840ebf965e449418b8f5fbcb0ccd3f2153f8`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Raw route nullable `get` key (`7f33ecba`, 2026-10-10): pinned Java `ConcurrentHashMap.get(null)` throws `NullPointerException`. Rust adds `TopicRouteTable::get_nullable`, returning typed `InvalidRoute("null topic route key")` for `None` and forwarding present keys to the existing lookup. The Java/Rust differential adds one outcome, bringing the route table oracle from 98 to 99; `route.json` SHA-256 is `cffedc220092c4ad3dc0648feb88fbd2ac416f7bcd561d7bb2a0760b7304e75f`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Nullable raw route `getOrDefault` default and key (`a7d4b3e0`, 2026-10-10): pinned Java `ConcurrentHashMap.getOrDefault` returns the mapped route when present even with a null default, returns null for an absent key with a null default, and throws `NullPointerException` for a null key. Rust adds `TopicRouteTable::get_or_default_nullable`, preserving those present/absent results and mapping a null key to typed `InvalidRoute("null topic route key")`. Three primitive outcomes raise the route-table oracle from 95 to 98; `route.json` SHA-256 is `451afcb6ca7fab4d332dc4a95f07afc6138c311a96b6623e555246d998c53ffd`. Focused and full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Nullable raw route `replaceAll` results (`c179246f`, 2026-10-10): `TopicRouteTable::replace_all_with_nullable_results` accepts optional callback values, returns typed `InvalidRoute("null topic route value")` for `None`, and retains replacements completed earlier in its traversal. The existing non-null `replace_all` delegates to the same retry/order implementation. Pinned Java `ConcurrentHashMap.replaceAll` throws `NullPointerException` at a null callback result, retains the updated prefix and unchanged failing entry, and skips later entries; Java and Rust traversal orders are recorded independently because the map contract does not promise one common order. Three primitive outcomes raise the route table oracle from 92 to 95; `route.json` SHA-256 is `2779d3c69caa8c7e03a4b096aa1bbb447c65735a65f55a52998b142d05c0e525`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 source/API/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Raw route `Map.Entry.setValue(null)` (`b4101e0b`, 2026-10-10): pinned Java `ConcurrentHashMap` entry handles throw `NullPointerException` for a null replacement and leave both the captured entry value and current mapping unchanged. Rust adds `TopicRouteEntryHandle::set_value_nullable`; `None` returns typed `InvalidRoute("null topic route value")` before either value changes. Three primitive outcomes raise the route table oracle from 89 to 92; `route.json` SHA-256 is `e6e229c060801b2507e99c85ee4716480c58403d334e2d3338a4abcd7130b789`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Broader Phase 6 public API/settings/source/input/platform reconciliation remains open; P6-V04 stays a Phase 19 handoff.

Raw route null-source `putAll` (`3e708ca6`, 2026-10-10): pinned Java `ConcurrentHashMap.putAll(null)` throws `NullPointerException` and leaves all existing mappings intact. Rust's `put_all_nullable_rows(None)` returns typed `InvalidRoute("null topic route putAll map")`; the differential compares the post-call key set and confirms the original mappings remain. Two primitive outcomes bring the route oracle from 87 to 89; `route.json` SHA-256 is `efb0b0baa346358bb5dfa4f18cd631d069967444c1f64884efd86cc43dee0fb0`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 reconciliation remains open; P6-V04 remains a Phase 19 handoff.

Raw route null-key nullable `putAll` (`88ccdd55`, 2026-10-10): at that update, the pinned Java source was a `TreeMap` with a null-tolerant comparator whose observed iteration was `a`, `null`, `z`. `ConcurrentHashMap.putAll` threw `NullPointerException` at the null key, retained the earlier route, and skipped the later route. Rust's `put_all_nullable_iter` mapped that row to `InvalidRoute("null topic route key")` after applying earlier rows. Three primitive outcomes brought the route table oracle from 84 to 87; `route.json` SHA-256 was `e0986004e2f019366d2deca75f670b0c7a7c395894b24b1ef42857b353d45b92`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks passed. Broader route and Phase 6 source/API/input/platform reconciliation remain open; P6-V04 stays a Phase 19 handoff.

Nullable raw route `putAll` source iteration (`5e1df2e4`, 2026-10-10): at that update `TopicRouteTable` exposed `put_all_nullable_iter` for any iterator of nullable key/route-handle rows, with `put_all_nullable_rows` as the slice adapter. The pinned Java 5.5.1 `TreeMap` visited `a`, `m`, `z`; a null route value at `m` threw `NullPointerException`, preserved the earlier `a` row, and skipped `z`. Rust's `BTreeMap` iterator differential matched the visit order, typed null-value failure, and post-failure key set. `runtimeTopicRouteTable.primitives` then had 84 outcomes; `route.json` SHA-256 was `9e26380877df8f38523baea0a5706de99303efa058f24f7296c0bc2afb7fc306`. Full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks passed. Broader route and Phase 6 source/API/input/platform reconciliation remain open; P6-V04 remains a Phase 19 handoff.

Generic `putAll` source iteration (`715c1dee`, 2026-10-10): Java `Map.putAll` accepts any source Map and visits its entry iteration order. Rust `FactoryTable`, producer/consumer tables, and `TopicRouteTable` now accept any `IntoIterator<Item = (String, value)>`; each mapping is applied independently in the iterator's order, with no multi-key transaction. Existing `BTreeMap` callers remain valid, and `HashMap` regressions prove the broader source shape compiles and preserves all mappings. Nullable keys/values continue through `put_all_nullable_rows`, which accepts an explicitly ordered row sequence. Broader API/input/source reconciliation remains open; P6-V04 remains a Phase 19 handoff.

Nullable arbitrary-source `putAll` iteration (`3a8e9cf9`, 2026-10-10): `FactoryTable`, producer tables, and consumer tables now expose `put_all_nullable_iter`, accepting any iterator of nullable key/value rows while retaining `put_all_nullable_rows` as a slice adapter. Pinned Java `TreeMap` inputs traverse `a`, `m`, `z`; a null value at `m` throws `NullPointerException`, keeps the earlier `a` write, and leaves `z` unapplied. Rust `BTreeMap` iterator differentials match this partial-write behavior for factory, producer, and consumer tables. `client-config.json` SHA-256: `fd34d154ef813646235c850ad21318c33e1052d31926db36cc2fa70429827d99`. Full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Other source/API/input branches remain under review; P6-V04 stays a Phase 19 handoff.

Nullable `putAll` HashMap traversal (`6391d854`, pinned Java 5.5.1 oracle, 2026-10-10): factory, producer, and consumer probes build `HashMap` source maps whose rows occupy buckets 2, 5, and 9, then record the actual Java iteration order. `ConcurrentHashMap.putAll` throws `NullPointerException` at the null value; the preceding row remains applied and the following row is absent. Rust's existing `put_all_nullable_rows` adapter reproduces that result when supplied the captured source order. Fixture SHA-256: `437c23de4e128513fcf91e211d231d4fc5691eec1b6314458c8b5916c4a64126`. This adds a HashMap source-class probe; other source-map traversal variants and broader Phase 6 source/API/input/reverse-export reconciliation remain under review. P6-V04 remains a Phase 19 handoff.

Nullable runtime creation configuration (`5ec79e92`, pinned Java 5.5.1 oracle, 2026-10-10): both `getOrCreateMQClientInstance(null)` overloads, with and without a null hook, throw `NullPointerException` before creating a runtime. Rust adds `get_or_create_nullable` and `get_or_create_with_hook_nullable`, mapping null config to typed `InvalidRoute`; valid optional configs still reuse the shared runtime. Fixture SHA-256: `72749d6fb0b3a3a28a2d78ba70eaab17135012831d38b9e230a5c3f8d1bd034c`. Broader Phase 6 source/API/input/reverse-export reconciliation remains open; P6-V04 is a Phase 19 handoff.

Nullable `replaceAll` results (`2cf01322`, pinned Java 5.5.1 oracle, 2026-10-10): if a factory, producer, or consumer table callback returns null, `ConcurrentHashMap.replaceAll` throws `NullPointerException` at that entry. Updates completed earlier in the traversal remain, and the failing entry is unchanged. Rust adds `replace_all_with_nullable_results` and typed `InvalidRoute` errors for all three table families. The Java fixture records its observed traversal and post-failure values; Rust checks the same partial-update rule in its own unspecified snapshot order. `client-config.json` SHA-256: `268b67ceae2e2cb5026a5a91f5af939efca78be5d9f58a89394707549f554d19`. Broader Phase 6 source/API/input/reverse-export reconciliation remains open; P6-V04 is a Phase 19 handoff.

Nullable live-table entry values (`80d34846`, pinned Java 5.5.1 oracle, 2026-10-10): `Map.Entry.setValue(null)` from factory, producer, and consumer table views throws `NullPointerException`; the detached entry and backing mapping retain their prior values. Rust adds `set_value_nullable` to each entry-handle family and maps null to the table's typed `InvalidRoute` value error. `client-config.json` SHA-256: `2a7e7c96c8eb449f172a4f3e2a4ebd392de1b9f6b38349f10d6d1d1276725244`. Broader Phase 6 source/API/input/reverse-export reconciliation and source-map traversal variants remain under review; P6-V04 is a Phase 19 handoff.

Nullable `putAll` source rows (`fd4fc086`, pinned Java 5.5.1 oracle, 2026-10-10): an insertion-ordered Java source map writes entries before a null key/value, then `putAll` throws `NullPointerException` and leaves later entries unapplied. Rust adds `put_all_nullable_rows(Option<&[NullableTableEntry<T>]>)`; a null row key/value maps to typed `InvalidRoute` at that position and prior writes remain. Factory, producer, and consumer table regressions compare the post-failure key sets to the Java fixture. `client-config.json` SHA-256: `fae76c809c567ea32cc432c98943286e87b7cbb55db5ec0faf63dcb306d59d07`. Source maps with other iteration behavior and broader Phase 6 source/API/input/reverse-export reconciliation remain under review; P6-V04 is a Phase 19 handoff.

Nullable live-table bulk inputs (`dbd2920d`, pinned Java 5.5.1 oracle, 2026-10-10): `putAll(null)`, `forEach(null)`, and `replaceAll(null)` throw `NullPointerException` on the factory, producer, and consumer tables. Rust adds `put_all_nullable`, `for_each_nullable`, and `replace_all_nullable`; null map/action/function inputs map to typed `InvalidRoute`. Callback aliases preserve borrowed lifetimes, and fixture-backed tests verify that null inputs leave mappings unchanged while valid callbacks and maps forward to the existing operations. `client-config.json` SHA-256: `ee369565a2fab6abb99261cc46f74f442fd4ddaf7c69a3f92316f19fbc79d66a`. Ordered null-key/value rows in a non-null source map are covered in the newer entry above; source-map classes with different iteration behavior and broader Phase 6 source/API/input/reverse-export reconciliation remain under review. P6-V04 is a Phase 19 handoff.

Nullable live-table view collection elements (`35c73c00`, pinned Java 5.5.1 oracle, 2026-10-10): Factory, producer, and consumer key/value/entry views now expose `remove_all_with_nullable_elements` and `retain_all_with_nullable_elements`. For Java `ArrayList` inputs containing null, key-view `removeAll` throws `NullPointerException`; candidates before the null remain removed, while candidates after it are not processed. Values/entry `removeAll` ignore null candidates, and all three views treat null as a nonmatching element in `retainAll`. Rust maps the key failure to typed `InvalidRoute` and preserves already-applied removals. `NullableTableEntry<T>` represents an entry candidate whose key or value may also be null. Fixture SHA-256: `08c47a329a57317a88d2dc84e6fe781cea4a1a0cf38bff4caeece60958219f51`. The pinned Java fixture verifier now passes deterministically; broader Phase 6 source/API/input/reverse-export reconciliation remains under review, and P6-V04 is a Phase 19 handoff.

Nullable live-table remapping inputs (`e9773251`, pinned Java 5.5.1 oracle, 2026-10-10): `FactoryTable` and the live producer/consumer tables now provide `compute_if_absent_nullable`, `compute_if_present_nullable`, `compute_nullable`, and `merge_nullable`. The adapters accept optional keys, values, and callback references; Java throws `NullPointerException` for each of the nine null-key/value/function positions probed on all three table families, and Rust maps those boundaries to typed `InvalidRoute`. Public callback aliases `TableComputeIfAbsent`, `TableComputeIfPresent`, `TableCompute`, and `TableMerge` preserve borrowed callback lifetimes. Fixture-backed regressions also verify that valid callbacks are forwarded and results are retained. `client-config.json` SHA-256: `43c64a75c7bcb93d2ff3d4ecb577c28ef5fc40d3b48365c66b522db5764adead`. Broader Phase 6 source/API/input/reverse-export reconciliation remains open; P6-V04 remains a Phase 19 handoff.

Nullable identity-removal instance input (`b2951a58` Java oracle, `8c62a7d9` Rust update, 2026-10-10): direct pinned Java `removeClientFactory(id, null)` returns normally and keeps the mapping; `removeClientFactory(null, null)` still throws `NullPointerException` because the null ID is rejected first. Rust's identity-aware nullable adapter now accepts `Option<&Arc<ClientRuntime>>`: `None` instance returns `false` for a non-null ID, while a null ID maps to typed `InvalidRoute` regardless of instance. `client-config.json` SHA-256: `a1e8747165443ca0dbac9bc2f998042507ade61d9173d37cb66ca7b13f8b2238`. Focused removal regression, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. This closes nullable instance input for identity-aware removal only; broader Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

Nullable manager factory removal (`33eaeafc` Java oracle, `42ecbba5` Rust implementation/test, 2026-10-10): pinned Java calls to both `removeClientFactory(null)` and `removeClientFactory(null, instance)` throw `NullPointerException`. Rust adds `remove_client_factory_by_id_nullable(Option<&str>)` and `remove_client_factory_nullable(Option<&str>, Option<&Arc<ClientRuntime>>)`; a null ID maps to typed `InvalidRoute`, while the later oracle slice confirms a null instance with a valid ID is a no-op. The regression confirms invalid inputs leave the existing runtime mapped. `client-config.json` SHA-256 at this commit: `d07554187630871a570e4b26eb68963510eccc492e7933d049a7c3182ee356c2`; current fixture hash follows in the newer entry above. Focused manager-table regression, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. This closes null-ID behavior for both removal overloads only; broader Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

Nullable `FactoryTable.get` (`83cb6584` Java oracle, `e927ab94` Rust implementation/test, 2026-10-10): the pinned Java `MQClientManager.getFactoryTable().get(null)` call throws `NullPointerException`. Rust's `FactoryTable::get_nullable(Option<&str>)` maps a null key to typed `InvalidRoute` and still returns `None` for an absent non-null key. `client-config.json` SHA-256: `876a923aa55f58d249e62e19286b9e80926ead6593c00cad22a51357306a3c1c`. Focused manager-table regression, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. This closes null-key `get` only; broader Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

Nullable producer/consumer table reads (`b2975140` Java oracle, `a2a97161` Rust implementation/test, 2026-10-10): actual pinned `getProducerTable().get(null)` and `getConsumerTable().get(null)` calls both throw `NullPointerException`. The live Rust tables now expose `get_nullable(Option<&str>)`, mapping a null group key to typed `InvalidRoute` while preserving `None` for a missing non-null key. `client-config.json` SHA-256: `f65b145c91889231da58de9fab635872196f28cdba33cbe2c68913785cceae82`. Focused table regression, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. This closes null-key reads only; broader Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

Nullable runtime selectors (`cb5c4b80` Java oracle, `4394a4c5` Rust implementation/test, 2026-10-10): direct pinned `MQClientInstance.selectProducer(null)` and `selectConsumer(null)` calls both throw `NullPointerException` through their `ConcurrentHashMap` lookups. Rust exposes `select_producer_nullable(Option<&str>)` and `select_consumer_nullable(Option<&str>)`, mapping a null group to a typed `InvalidRoute` while preserving absent and present non-null lookups. `client-config.json` SHA-256: `408db9671aeb2731146244e105f0e76efc21fdef01ad772ce55bd9131d3e035f`. Focused selector regression, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. This closes the null-key boundary for these selectors only; broader Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

Nullable registration/removal boundary (`50b83107` Java oracle, `9ea85053` Rust implementation/test, 2026-10-10): the pinned `MQClientInstance` returns `false` for consumer/producer registration when either group or owner is null; calling consumer/producer unregister with a null group throws `NullPointerException` before Broker RPC. Rust exposes `register_consumer_nullable` / `register_producer_nullable` (`None` for a null group or owner) and `_by_group_nullable` removals (typed `InvalidRoute` for null groups). The fixture-backed regression verifies both outcomes and confirms null unregister does not start the runtime. `lookup.json` SHA-256: `de2d3104c034955172e04f2ea73bd67f1981ad27664b2fe413b0ac98c18835b6`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. This closes null registration/unregister boundaries only; full Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

Repeated `start()` after shutdown correction (`47f36e0a`, 2026-10-10): pinned Java 5.5.1 leaves `serviceState=SHUTDOWN_ALREADY` and the scheduled executor shut down after `shutdown()`, then returns normally from a later public `start()` call without restarting resources. The direct Java fixture invokes `shutdown()` from the actual `RUNNING` state branch without activating workers, then invokes `start()` and records both terminal-state observations. The Rust regression uses a real started runtime and shared-user cleanup; it previously failed with `InvalidState(Stopped)`. `ClientRuntime::start()` now joins/recognizes the terminal owner and returns `Ok(())` without reviving resources, including when a queued Start observes shutdown completion. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, formatting, and diff checks pass. This closes one lifecycle control-flow branch; broader source/API/platform reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

Runtime startup schedule failure differential (`29d74adc`, 2026-10-10): the pinned Java 5.5.1 oracle now constructs `MQClientInstance` with concurrent-heartbeat pool sizes 0, -1, and 1 (the first two throw `IllegalArgumentException`; 1 succeeds), and calls `start()` with nonpositive route, heartbeat, and persistence intervals. All three Java starts throw `IllegalArgumentException`, leave `serviceState=START_FAILED`, and shut down the scheduled executor. Rust's constructor boundary matches the three pool-size outcomes; its runtime differential maps the invalid intervals to `InvalidSchedule`, ends in `Failed`, and verifies a later manager lookup creates a replacement. No production change was needed. `fixtures/runtime/client-config.json` carries both cases. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), formatting, and diff checks pass. This closes these constructor/startup input branches only; broader source/API/rollback reconciliation remains open and P6-V04 remains `PENDING_PHASE_19`.

Admin owner back-reference ownership (`bcc98f46`, 2026-10-10): `register_admin_ext` documentation now requires owners that refer back to the runtime to use `Weak<ClientRuntime>`, preventing the retained Java-style admin owner from creating an `Arc` cycle. The lifecycle differential uses a weak back-reference and proves the runtime is released after group removal, shutdown, and dropping the manager/runtime handles. Focused lifecycle test, strict core Clippy, fmt, and diff checks pass. User-provided owners remain responsible for their own back-reference ownership.

Admin owner destructor re-entry correction (`bbc05ff9`, 2026-10-10): Java-like admin owner removal and registry clear now detach entries while locked, then drop owners after unlocking. A red/green regression uses an owner `Drop` implementation that re-enters registration; the previous code timed out on both unregister and clear, and both pass after the fix. The same out-of-lock drop rule covers admin lease/group removal. Workspace all-target tests, strict Clippy, formatting, and diff checks pass. This protects the Rust owner lifecycle while preserving the Java table behavior; broader Phase 6 reconciliation remains open.

Admin registration/shutdown continuation (2026-10-10): pinned Java 5.5.1 `registerAdminExt` rejects null group/admin inputs and duplicates, retains the first owner, and `unregisterAdminExt` removes unconditionally by group. A started, unconnected Java runtime also refuses `shutdown()` while `adminExtTable` is nonempty, remains in the manager table, and shuts down after that group is removed. Java oracle commits `78eff96c`/`18fc5709`; native implementation `c60cbba8`, registration assertion `83a75070`, and lifecycle differential `0ca939d3`. The fixture-backed test compares 15 registration observations plus both shutdown-gate outcomes; Rust maps the null unregister failure to `InvalidRoute("null admin group")`. Workspace tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Concrete admin operations remain Phase 17; full Phase 6 reconciliation remains open and P6-V04 remains `PENDING_PHASE_19`.

`TopicRouteData` mutable collection alias continuation (`e47f4517` oracle, `45b8a5fc` implementation): direct pinned Java 5.5.1 calls establish that `setQueueDatas`, `setBrokerDatas`, `setFilterServerTable`, and `setTopicQueueMappingByBroker` retain the supplied collection objects and return them from their getters. Queue/Broker rows and mapping DTO values also retain identity; the filter table retains its nested lists. Caller/getter mutations are visible both ways. Rust wraps each container and mutable DTO element in shared handles. The Java copy/clone oracle (`d437ab34`) and Rust implementation (`8b69c09e`) show that Java-compatible copies create new outer containers but retain nested DTO/list identities; native `TopicRouteData::Clone` remains deep. Actual runtime Java oracle/implementation `827f6510`/`a87e3726` confirms raw-table and published route copies also use distinct outer containers with shared nested queue/broker/filter objects. The wire representation is unchanged. Current `route.json` SHA-256: `d01c51ec40fdcf96931dd0191271a2b23fc396ec00f18f949aa629e678172e2b`. See [ADR-0076](../architecture/ADR-0076-shared-topic-route-data-collections.md) and [ADR-0077](../architecture/ADR-0077-route-copy-element-aliases.md). Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test, manifest validation (22 scenarios), formatting, and diff checks pass. This closes only these route collection/copy/publication alias rows; broader Phase 6 reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

| Java TopicRouteData collection API | Rust counterpart | Invoked alias evidence |
| --- | --- | --- |
| `getQueueDatas` / `setQueueDatas` | `get_queue_datas` / `set_queue_datas`; owned setter and `queue_datas_snapshot` | same list and `QueueData` element handles; both mutation directions |
| `getBrokerDatas` / `setBrokerDatas` | `get_broker_datas` / `set_broker_datas`; owned setter and `broker_datas_snapshot` | same list and `BrokerData` element handles; both mutation directions |
| `getFilterServerTable` / `setFilterServerTable` | `get_filter_server_table` / `set_filter_server_table`; owned setter and `filter_server_table_snapshot` | same outer map and nested filter-list handles; both mutation directions |
| `getTopicQueueMappingByBroker` / `setTopicQueueMappingByBroker` | `get_topic_queue_mapping_by_broker` / `set_topic_queue_mapping_by_broker`; owned setter and `topic_queue_mapping_by_broker_snapshot` | same map and `TopicQueueMappingInfo` element handles; both mutation directions |

Heartbeat DTO Set alias correction (`7e5b3fa6`, 2026-10-10): pinned RocketMQ 5.5.1 API cases establish that `SubscriptionData.tagsSet/codeSet`, `ConsumerData.subscriptionDataSet`, and `HeartbeatData.producerDataSet/consumerDataSet` retain their assigned mutable Sets and return those same Sets from getters. The Java oracle also checks nested SubscriptionData identity, identity-based ProducerData/ConsumerData elements, and mutation visibility in both directions. Rust now stores each Set in a shared handle, wraps decoded elements, and preserves the relevant Java value or identity equality in HashSet membership; owned setters remain available. The transparent JSON shape and null/default rules remain unchanged. `heartbeat.json` SHA-256: 7c50b6ff17d46cc6659ab69e75a69c56d28a7a187ea198440c093acefa1f39b3. See [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md). Protocol tests (108 unit tests, integrations, 15 doctests), workspace all-target tests with local socket access, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test, manifest validation (22 scenarios), formatting, and diff checks pass. This closes the heartbeat Set alias rows only; broader Phase 6 reconciliation remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Status/reset Map alias correction (`2f07fe94`, 2026-10-10): pinned Java 5.5.1 confirms `GetConsumerStatusBody` retains both outer Maps and each supplied nested consumer Map, while `ResetOffsetBody` retains its supplied offset Map. Rust fields now use `SharedQueueOffsetEntries` and `SharedConsumerQueueOffsets`; getters and setters preserve handle identity, and tests assert `ptr_eq` plus caller/getter mutation in both directions. Fastjson decoding wraps outer and nested tables; serialization remains unchanged. Individual `MessageQueue` key identity is not claimed. `lookup.json` SHA-256 remains `7d56171dba60258e9ec45ad91c483f51c0a049a65adaf2f62212c416e780094a`; see [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md). Protocol tests/doctests, full workspace/all-target tests with local socket access, strict all-feature Clippy, pinned offline Maven verify (22 scenarios/cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. At this checkpoint, heartbeat Set aliases remained open and were later closed by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md); broader Phase 6 reconciliation remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.

LiteSubscription getter alias correction (`e33c43b2`, 2026-10-10): pinned Java 5.5.1 confirms every `getLiteTopicSet()` call returns the same mutable Set; changes through a retained getter result and later subscription API calls are mutually visible. `setLiteTopicSet` copies and adds members without retaining the input Set, clearing existing topics, or changing `updateTime`. Rust now stores the member Set in `SharedLiteSubscriptionTopicNames`, and add/remove methods mutate that shared value. JSON and `Display` output remain unchanged. `lookup.json` SHA-256: `7d56171dba60258e9ec45ad91c483f51c0a049a65adaf2f62212c416e780094a`; see [ADR-0072](../architecture/ADR-0072-shared-lite-subscription-getter-alias.md). Protocol tests/doctests, full workspace/all-target tests with local socket access, strict all-feature Clippy, pinned offline Maven verify (22 scenarios/cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. At this checkpoint, heartbeat Set aliases remained open and were later closed by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md); broader Phase 6 reconciliation remains PARTIAL; status/reset Map aliases were closed by `2f07fe94` / [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md); P6-V04 stays `PENDING_PHASE_19`.
LiteSubscriptionCtlRequestBody Set and element alias correction (`66adb0c4`, 2026-10-10): pinned Java 5.5.1 confirms the body retains its assigned mutable Set, each DTO element remains the same object supplied by the caller, and caller/getter additions are mutually visible. Rust now uses `SharedLiteSubscriptionSet` around an identity-based `LiteSubscriptionDtoSet` of shared DTO elements; decode wraps each row and collapses duplicate null elements while retaining distinct DTO identities. The JSON array shape is unchanged. Current `lookup.json` SHA-256: `7d56171dba60258e9ec45ad91c483f51c0a049a65adaf2f62212c416e780094a`; see [ADR-0071](../architecture/ADR-0071-shared-lite-subscription-set-aliases.md). Protocol tests, full workspace/all-target tests with local socket access, strict all-feature Clippy, workspace doctests, pinned offline Maven verify (22 scenarios/cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. At this checkpoint, heartbeat Set aliases remained open and were later closed by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md); broader Phase 6 reconciliation remains PARTIAL; status/reset Map aliases were closed by `2f07fe94` / [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md); P6-V04 stays `PENDING_PHASE_19`.

LiteSubscriptionDTO nested alias correction (`60251cee`, 2026-10-10): direct pinned Java 5.5.1 calls confirm `setLiteTopicSet`/`getLiteTopicSet` and `setOffsetOption`/`getOffsetOption` preserve the exact mutable Set and `OffsetOption` objects, with changes visible through both references. Rust now stores `Option<SharedLiteSubscriptionTopicSet>` and `Option<SharedOffsetOption>`, wraps decoded nested values, and returns cloned shared handles; serialization and `Display` keep the existing fields and wire shape. The Java identity-based DTO equality contract is unchanged. The DTO-specific observations are unchanged; current `lookup.json` SHA-256 is `7d56171dba60258e9ec45ad91c483f51c0a049a65adaf2f62212c416e780094a`; see [ADR-0070](../architecture/ADR-0070-shared-lite-subscription-dto-aliases.md). Focused API/wire tests, workspace/all-target tests with local socket access, strict all-feature Clippy, workspace doctests, pinned offline Maven verify (22 scenarios/cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. At this checkpoint, heartbeat Set aliases remained open and were later closed by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md); broader Phase 6 reconciliation remains PARTIAL; status/reset Map aliases were closed by `2f07fe94` / [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md); P6-V04 stays `PENDING_PHASE_19`.

MessageRequestModeSerializeWrapper nested Map alias correction (`853b3fdd`, 2026-10-10): the direct pinned Java 5.5.1 oracle confirms the wrapper retains its assigned topic Map, each topic Map retains its consumer-group Map, and each group Map retains the exact mutable `SetMessageRequestModeRequestBody` objects; getters at all three levels return the same objects and caller/getter changes are visible in both directions. Rust now uses `SharedMessageRequestModeMap`, shared nested group maps, and shared request-body values, wrapping all levels during Fastjson decode. The public wrapper matches Java `Object.equals/hashCode` identity. The empty constructor default, null state, 33 Fastjson cases, and JSON wire shape remain covered. `lookup.json` SHA-256 at the wrapper alias checkpoint: `1a0df76edff7063ea8790fc0cfbf0321eca6469c766a9be191e6596f7403d799`; see [ADR-0069](../architecture/ADR-0069-shared-request-mode-wrapper-aliases.md). Workspace/all-target tests with local socket access, strict all-feature Clippy, workspace doctests, pinned offline Maven verify (22 scenarios/cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Other Phase 6 alias and source/input gaps remain; P6-V04 stays `PENDING_PHASE_19`.

QueryAssignment response Set element alias correction (`d312bb6c`, 2026-10-10): direct pinned Java 5.5.1 calls confirm that `QueryAssignmentResponseBody.setMessageQueueAssignments` retains the exact mutable `MessageQueueAssignment` element and the getter returns that same object; caller-held and getter-held mode mutations are visible in both directions. Rust now uses `HashSet<Option<SharedQueryAssignment>>` inside the shared outer Set, wraps decoded elements, and implements shared-handle value-based `Eq`/`Hash` to preserve the existing Rust Set membership semantics. The core runtime snapshots the outer Set and elements into its existing owned return type. Serialization is unchanged; mutating an assignment after Set insertion retains the Java-compatible mutable-key hash hazard. `lookup.json` SHA-256: `a4d4c85dc1ab17a240906f0e48348ebf2644c31bfcdd987d958fc1368b0e20de`; see [ADR-0068](../architecture/ADR-0068-shared-query-assignment-elements.md). Workspace/all-target tests with local socket access, strict all-feature Clippy, pinned offline Maven verify (22 scenarios and cross-decodes), comparator self-test/manifest22, format, and diff checks pass. Other Phase 6 aliases and whole-source/input reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Lock-batch Set element alias correction (`625122ee`, 2026-10-10): direct pinned Java 5.5.1 calls confirm that each of `LockBatchRequestBody.mqSet`, `UnlockBatchRequestBody.mqSet`, and `LockBatchResponseBody.lockOKMQSet` returns the identical mutable `MessageQueue` element supplied through its Set, with caller/getter mutations visible in both directions. Rust now uses `SharedMessageQueueSet` (`MessageQueueSet = HashSet<Option<SharedAssignmentMessageQueue>>`) and value-based `Eq`/`Hash` on shared queue handles; decoding wraps each element, preserving Java membership/deduplication and object identity. The core RPC API keeps its owned return by snapshotting handles at the boundary. Wire JSON is unchanged. Mutating a queue after HashSet insertion changes its hash in both Java and Rust, so tests restore key values before lookup. `lookup.json` SHA-256 at the lock-element correction checkpoint: `4b4d410671a673533e1238a8bcf8e939134884296610ec6d1045c1022c6818c6`; see [ADR-0067](../architecture/ADR-0067-shared-lock-set-element-aliases.md). Workspace/all-target tests and check, strict all-feature Clippy, pinned offline Maven verify (22 scenarios/cross-decodes), comparator self-test/manifest22, fmt, and diff checks pass. Other Phase 6 alias/source/input gaps remain; P6-V04 stays `PENDING_PHASE_19`.

MessageQueueAssignment nested queue and attachments alias correction (`aa4803df`, 2026-10-10): pinned Java 5.5.1 confirms each setter retains the exact mutable `MessageQueue`/Map and each getter returns that same object; caller-side and getter-side mutations are bidirectionally visible. Rust now stores these nullable fields as `Option<SharedAssignmentMessageQueue>` and `Option<SharedAssignmentAttachments>`, wraps decoded values, and clones shared getter handles. The fixture-backed differential checks `ptr_eq` and both mutation directions. Wire JSON is unchanged. `MessageQueueAssignment` retains lawful all-fields Rust `Eq`/`Hash` using value snapshots; mutating a nested value after the assignment is inserted in a HashSet can change its bucket hash, matching Java's mutable-key hazard. Assignment objects inside the response Set preserve element identity under `d312bb6c` and [ADR-0068](../architecture/ADR-0068-shared-query-assignment-elements.md). `lookup.json` SHA-256 at the nested-field checkpoint: `cf03a6b58e3f312d8f1a4bc95a0384bc004338bc2d529141694bbf1810b63a71`; see [ADR-0066](../architecture/ADR-0066-shared-assignment-nested-aliases.md). Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios/cross-decodes), comparator self-test/manifest22, formatting, and diff checks pass. Other alias surfaces and full Phase 6 reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical outer-Set checkpoint, superseded for element identity by `d312bb6c`: QueryAssignmentResponse Set alias and identity correction (`dbaed386`, 2026-10-10): pinned Java 5.5.1 confirms `setMessageQueueAssignments` retains its exact mutable Set, getter returns that same Set, and caller/getter mutations are bidirectional. Reflection confirms the DTO inherits `Object.equals`/`hashCode`; same-instance equality is true and equal-content distinct-instance equality is false. Rust now represents the outer collection as `Option<SharedValue<HashSet<Option<SharedQueryAssignment>>>>`, wraps Fastjson results and each element, returns cloned handles, verifies outer and element `ptr_eq`, and snapshots the Set and elements in the core runtime adapter to preserve its owned return type. Serialization is unchanged. Element identity is closed by `d312bb6c` and [ADR-0068](../architecture/ADR-0068-shared-query-assignment-elements.md). Fixture `lookup.json` SHA-256 at the outer-Set checkpoint `fb436699a2c7d128fd96c73e3f106add77904444fa581485a5ddc1d929bc251e`; see [ADR-0065](../architecture/ADR-0065-shared-query-assignment-set-alias.md). Focused protocol tests, workspace checks/tests, strict Clippy, pinned offline Maven verify, comparator/manifest, formatting, and diff checks pass. Other DTO aliases and full Phase 6 reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

BatchAck element and BitSet alias correction (`7ba69080`, 2026-10-10): pinned Java 5.5.1 confirms the exact `BatchAck` object supplied in the `acks` List is returned from the body, mutations through either reference reach the same object, and `BatchAck.getBitSet()` returns the same assigned mutable `BitSet`. Reflection also confirms BatchAck inherits `Object.equals`/`hashCode`; equal-content distinct BatchAck instances are unequal. Rust now stores `acks` as `Option<SharedValue<Vec<Option<SharedValue<BatchAck>>>>>` and `BatchAck.bit_set` as `Option<SharedValue<BitSet>>`; decoding wraps each DTO and BitSet, and cloned getter handles preserve identity. Fixture-backed tests verify `ptr_eq`, caller/getter mutation in both directions, defaults, values, and Java identity-method declarations. The DTOs have no structural `PartialEq`, `Eq`, or `Hash`; Base64 and compact JSON wire shapes are unchanged. `fixtures/runtime/lookup.json` SHA-256: `e2275802581a7a8c9d5271349377bf5b347d59d45af269e8e6569f1f682674b8`. Full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verification (22 scenarios and Java cross-decodes), protocol tests (including 13 doc tests), workspace target checking, comparator self-test, manifest validation (22 scenarios), formatting, and diff checks pass. This closes the BatchAck outer List, element object, and nested BitSet alias surfaces; other DTO aliases and full Phase 6 reconciliation remain open. See [ADR-0064](../architecture/ADR-0064-shared-batch-ack-nested-aliases.md).

Historical checkpoint, superseded by `7ba69080`: BatchAck outer List alias and identity correction (`a32d00f7`, 2026-10-10): pinned Java 5.5.1 confirms `BatchAckMessageRequestBody.setAcks` retains the assigned List, `getAcks` returns that same List, and mutations through either reference are mutually visible. The API fixture records `ptr_eq`-equivalent identity observations plus inherited `Object.equals`/`hashCode` behavior: the same body equals itself, while an equal-content distinct body does not. Rust stores `acks` as `Option<SharedValue<Vec<Option<BatchAck>>>>`; cloned getter handles retain identity, Fastjson decoding wraps the decoded nullable List, and serialization keeps the existing JSON array/null shape. The body has no structural `PartialEq`, `Eq`, or `Hash` traits. Its differential checks setter/getter identity, both mutation directions, values, and Java identity semantics. `fixtures/runtime/lookup.json` SHA-256: `0f0c32b433f81c717aab9fe6bcb4767466eacb4e08343d8c76b3c965dc9e1654`. Full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verification (22 scenarios and Java cross-decodes), protocol tests, comparator self-test, manifest validation (22 scenarios), formatting, and diff checks pass. The outer `acks` List alias is closed; mutable `BatchAck` element references and caller-owned `BatchAck.bit_set` identity remain open. See [ADR-0063](../architecture/ADR-0063-shared-batch-ack-list-alias.md).

Check-client nested-reference and identity correction (`a732974a`, 2026-10-10): Java `CheckClientRequestBody.setSubscriptionData` retains the assigned `SubscriptionData`; its getter returns the identical instance, so mutation through either reference is visible through the other. Rust now stores this field as `Option<SharedValue<SubscriptionData>>`; handle clones preserve shared identity, and `with`/`with_mut` access the same object. The direct Java fixture also confirms `CheckClientRequestBody` inherits `Object.equals` and `Object.hashCode`; Rust no longer adds structural `PartialEq`/`Eq`/`Hash`. The lookup fixture records pointer identity, both mutation directions, and both declaring classes; SHA-256 `dcb46e55ac972d2d621585f61fb28cc67fd398d8d5ada31bd1586a5abe07a8b3`. Protocol suite (108), full workspace/all-target tests, strict workspace Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. This closes only the `CheckClientRequestBody.subscriptionData` alias; other DTO caller aliases and full Phase 6 reconciliation remain open.
Consumer-list setter/getter alias and identity correction (`456849fb`, 2026-10-10): pinned Java proves `setConsumerIdList` retains the same mutable List returned by `getConsumerIdList`; mutation through either reference is visible through the other. The direct API fixture also confirms inherited `Object.equals`/`hashCode` identity. Rust now stores `Option<SharedValue<Vec<Option<String>>>>`; cloned getter handles satisfy `ptr_eq`, and the Java alias test mutates through caller and getter handles in both directions. The core RPC method snapshots this DTO handle to preserve its existing owned result type. Fixture `lookup.json` SHA-256 `372a5250f6af3ced4c17ef23eb9e05dbe78523ceb6b75a4cd03fe1589fabf80e`. Full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify, comparator/manifest22, format, and diff checks pass. Together with `a732974a`, this closes two caller-alias surfaces; the remaining DTO aliases and full Phase 6 reconciliation stay open.
Historical outer-container checkpoint, superseded for element aliases by `625122ee`: Lock-batch Set-container alias and identity correction (`045b46e7`, 2026-10-10): pinned Java confirms `LockBatchRequestBody.mqSet`, `UnlockBatchRequestBody.mqSet`, and `LockBatchResponseBody.lockOKMQSet` retain the assigned Set and return that same object through getters. Mutations through caller and getter references are mutually visible. All three classes inherit `Object.equals`/`hashCode`. Rust now stores these fields in `SharedValue<HashSet<Option<MessageQueue>>>`; cloned getter handles match `ptr_eq`, while JSON wire shape, Set membership, defaults, null states, and Java `toString()` remain verified. Core lock results snapshot back to the existing owned Set return type. Fixture `lookup.json` SHA-256 `07fb5f45492598ce7ca844903bd0440bbbaabd7baddb9bdfbbb5c96a5b39a7ad`. Full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify, comparator/manifest22, format, and diff checks pass. The outer Set aliases were closed at this checkpoint; nested mutable `MessageQueue` element aliases are now closed by `625122ee` and [ADR-0067](../architecture/ADR-0067-shared-lock-set-element-aliases.md). Other DTO aliases remain open. See [ADR-0062](../architecture/ADR-0062-shared-lock-set-aliases.md).

Runtime scheduled-task oracle continuation (2026-10-10): `RuntimeLookupFixtures.runtimeScheduledTaskCases` replaces the actual private executor with a recording `ScheduledExecutorService`, invokes Java `MQClientInstance.startScheduledTask()`, and captures five `scheduleAtFixedRate` calls without a configured NameServer and four with one. Direct arguments confirm discovery 10s/120s, route 10ms/configured period, heartbeat 1s/configured period, persistence 10s/configured period, and adjustment 1m/1m. Pinned 5.5.1 bytecode shows constructor use of `Executors.newSingleThreadScheduledExecutor`, all five scheduler registrations use `scheduleAtFixedRate`, and each task wrapper catches `Throwable`. The Rust differential compares both NameServer cases after normalizing Java units to milliseconds. Java oracle/fixture `2f57160d`; Rust comparison `8e0c5610`; lookup SHA-256 `98bc999e3880f25eefc1980ff55eeec7dac9264ddbec98c7b0db669fd6cdce82`; artifact SHA-256 `1c39c94a28e59b292e29d699872d693cdc2ce30bd54c4e0f95a4c377fd9dff84`. Core library tests, strict core Clippy, Maven verify, comparator/manifest22, formatting, and diff checks pass. Whole Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

BatchAck Base64 pad-bit input continuation (2026-10-10): expanded the invoked Java 5.5.1 `BatchAckMessageRequestBody.decode/encode` corpus from 106 to 111 cases. Java accepts unpadded two- and three-sextet inputs and ignores unused low pad bits (`"BR=="`, `"BFB="`), then emits canonical padded Base64; it rejects partial padding (`"BQ="`) and internal tab input. The Rust differential first failed on noncanonical pad bits. Separate padded/no-padding standard Base64 engines now permit ignored trailing bits while retaining strict padding and alphabet validation. Oracle/fixture `63dfa47f`; red regression `ff888cff`; implementation `2677a08b`; current lookup SHA-256 `98bc999e3880f25eefc1980ff55eeec7dac9264ddbec98c7b0db669fd6cdce82`. Protocol/workspace tests, strict workspace Clippy, pinned offline Maven verify, formatting, and diff checks pass. Whole Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

BatchAck Base64 input continuation (2026-10-10): expanded the direct Java 5.5.1 `BatchAckMessageRequestBody.decode/encode` corpus from 95 to 106 cases. Java accepts unpadded standard Base64 (`"BQ"`) and normalizes it to padded `"BQ=="`; it rejects one-character, excess-padding, spaces, tabs, line breaks, carriage returns, nonbreaking spaces, URL-safe-alphabet, and invalid-symbol inputs. The native test failed on the new unpadded case before `bitset_base64` added a strict `STANDARD_NO_PAD` fallback; padded and malformed-input cases now match. Oracle/fixture `c34f9dba`, whitespace extension `32ce689a`; red regression `0350546f`, whitespace corpus assertion `238923da`; implementation `cc3ebc70`; current lookup SHA-256 `beb05f8547613758325d0cd6638696d91bd04d78aee832ddd479b5cd280b3576`. Pinned offline Maven verification, the focused Java/native differential, and `git diff --check` pass. Whole Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

ServiceThread fixed join interval continuation (2026-10-10): `runtime::SERVICE_THREAD_JOIN_TIME_MILLIS` now exposes the 90,000ms result returned by both pinned workers' inherited Java `getJoinTime()` method; the existing direct Java fixture asserts this value against the Rust constant. This is a fixed module constant, not a per-worker getter or a Java thread-join implementation. The focused native fixture test, formatting check, and strict all-target/all-feature Clippy pass; implementation/test commit `5ce83461`. Whole Phase 6 API/input/reverse-export reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Null-topic runtime broker lookup continuation (2026-10-09): pinned 5.5.1 bytecode and a direct invocation show `MQClientInstance.getBrokerNameFromMessageQueue` looks up `topicEndPointsTable` with the queue topic before applying fallback; a null topic throws `NullPointerException` both before and after the retained-static-endpoint refresh. The red native regression showed Rust incorrectly returned `fallback-broker`; `RouteSnapshot::broker_name` now returns `Result<Option<&str>, RuntimeError>` and maps this input to `InvalidRoute("null message queue topic")`. Oracle/fixture `5125b311`; fix/regression `e323c8da`; current `route.json` SHA-256 `780023c54340d92c538a3b106affdafb657c88378c221add2dd224faa0fc8fb1`. Full workspace tests (with loopback access), strict all-target/all-feature Clippy, pinned offline Maven verify (25 route inputs, 22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 source/API/input/reverse-export reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.


Static endpoint unmapped-queue lookup continuation (2026-10-09): pinned 5.5.1 bytecode and an invoked `MQClientInstance.getBrokerNameFromMessageQueue` case confirm a nonempty endpoint map returns its direct lookup result, including null for an unmapped queue whose `brokerName` is `fallback-broker`; the result remains null after static-to-ordinary refresh retains that map. Oracle/fixture `c7b04506`; native differential `6242ec3b`; no production change. Current `route.json` SHA-256 `85f56d6b92fefbc0117bfe0a5118c8a3ba1b3cd6375a59e6d45877a41cf2f6a0`. Focused regression, pinned offline Maven verify (25 route inputs, 22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 source/API/input/reverse-export reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.


Public route-conversion API continuation (2026-10-09): pinned `javap -public` confirms `MQClientInstance` declares both static `topicRouteData2TopicPublishInfo` and `topicRouteData2TopicSubscribeInfo`. Rust exposes `TopicSnapshot::from_route_data(topic, route)` for non-null inputs and `from_nullable_route_data(topic, route)` for the Java nullable argument surface; both share the cache publication converter. Ten direct Java cases show null topic is retained in dynamic, static and ordered publish/subscribe queues, while null route throws `NullPointerException` even when topic is null; Rust returns `RuntimeError::InvalidRoute` for null route. The standard 25 cases also capture `TopicPublishInfo.getTopicRouteData()`; the native snapshot matches its complete DTO field values, and all ten nullable cases pass the public integration differential. Native API `92f8b358`, nullable converter `71903ef7`; oracle/input fixture commits `bc0972ab`/`e7b559d4`/`25647b57`/`96950fd1`; native differential `a858b6f7`/`88df2164`; current `route.json` SHA-256 `7c5f6b1c09937d21a63252e9c1359485ebde19b68284f63de1215781185ad6db`. Full workspace tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Other method/input reconciliation remains open.

Raw route cleanup null-broker-ID continuation (2026-10-09): the 21-case direct Java/native `runtimeRawCleanupShapes` matrix now includes a `brokerAddrs` entry with a null `Long` key and live `master:10911` value. `cleanOfflineBroker()` retains `broker/0 -> master:10911` and removes the stale slave, confirming cleanup treats the advertised address value as live without requiring a non-null broker ID. Oracle/fixture `90aab86e`; native regression `5dd34bc3`; current `route.json` SHA-256 `7c5f6b1c09937d21a63252e9c1359485ebde19b68284f63de1215781185ad6db`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Topic broker lookup null-ID continuation (2026-10-09): the pinned Java `MQClientInstance.findBrokerAddrByTopic` oracle now covers null `Long` broker IDs when their address is the only entry, when a master is also present, and when a slave is also present. Java accepts all three shapes: it returns the sole address, prefers the master, or returns one of the two slave candidates. The native randomized selection differential accepts the same candidate sets; no production change was needed. Oracle/fixture `63d8828f`; native regression `b3967a11`; current `lookup.json` SHA-256 `c4cb769d2d68a0f85c7008bd0bf56dca4f37bfee6a4736b7f76a0f801196ac01`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Topic broker lookup null-list continuation (2026-10-09): the direct Java `MQClientInstance.findBrokerAddrByTopic` oracle also covers a null `brokerDatas` list and a null BrokerData row; each throws `NullPointerException`. The new differential first exposed that Rust treated the null list like an empty list; `RouteSnapshot::broker_address_by_topic` now maps the null list to `RuntimeError::InvalidRoute`, matching the null-row error mapping. Oracle/fixture `9fea2779`; native regression `0a092277`; current `lookup.json` SHA-256 `c4cb769d2d68a0f85c7008bd0bf56dca4f37bfee6a4736b7f76a0f801196ac01`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Topic broker lookup missing-list continuation (2026-10-09): the pinned Java oracle distinguishes an omitted `brokerDatas` field from explicit JSON null: omission defaults to an empty broker list and returns no address, while explicit null throws `NullPointerException`. Rust's DTO decoder already preserves the same absent-versus-null distinction, so no production change was needed. Oracle/fixture `7ee83759`; native differential `9b7eb5a2`; current `lookup.json` SHA-256 `c4cb769d2d68a0f85c7008bd0bf56dca4f37bfee6a4736b7f76a0f801196ac01`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

BrokerData selector null-ID continuation (2026-10-09): the direct Java `BrokerData.selectBrokerAddr` matrix expands from seven to ten cases with a null ID as the only key, alongside a slave, and alongside a master plus slave. Java returns the sole address, either allowed value in random fallback, or the configured master; the native direct DTO method matches these candidate sets. No production change was needed. Oracle/fixture `0492c418`; native regression `5f481f17` (test cleanup `b07fecaf`); current `route.json` SHA-256 `7c5f6b1c09937d21a63252e9c1359485ebde19b68284f63de1215781185ad6db`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass. Whole Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Raw route cleanup mutation-order continuation (2026-10-09): the 20-case pinned Java/native matrix confirms empty outer broker-address rows are removed immediately during traversal, while updates to nonempty inner maps are deferred until the scan succeeds. If a later malformed raw broker row throws, the earlier empty-row removal remains visible; the existing valid-candidate-then-null-row case still preserves the whole table. Oracle/fixture `892db0a9`; native change/test `bf3a8fce`; `route.json` SHA-256 `712d6d7c473c0211b01e12067e42b6c014a774407aef5f9dab650ecce99924cb`. Full workspace tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, formatting, and diff checks pass. Whole Phase 6 reconciliation remains open.

Historical value-API checkpoint; caller-object alias status was superseded by `7ba69080`: BatchAck BitSet clone-capacity continuation (2026-10-09): pinned Java confirms cloning an empty `BitSet(130)` retains its 192-bit capacity; setting bit 7 on the clone leaves the source empty and yields `gA==`. Rust's owned clone matches capacity and value isolation. Oracle/fixture `85512a05`; native regression `24fb9f1b`; checkpoint `lookup.json` SHA-256 `20f0de11e7d95a25143a5a1dcda2a07762f64177c8cf3303cecd1db2ee5192a7`. Full workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, formatting, and diff checks pass. Caller-owned BitSet alias identity was open at that checkpoint and is covered by the later shared-handle correction; whole Phase 6 reconciliation remains open.

Historical value-API checkpoint; caller-object alias status was superseded by `7ba69080`: BatchAck BitSet clone continuation (2026-10-09): pinned Java confirms `BitSet.clone()` returns an independent value: clearing bit 0 and setting bit 7 on the clone leaves the source unchanged (`JQ==`) and yields clone bytes `pA==`. The Rust-owned `BitSet::clone()` differential matches both serialized values and source-bit observations; clone isolation matches Java; the DTO field alias is covered by the later shared-handle correction. Oracle/fixture commit `b0dd6722`; native regression `6fb451f7`; checkpoint `lookup.json` SHA-256 `df9a8c8523476111b203c6c3bfadfe1e65b444878b57befc896849724faf19db`. Full workspace tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, formatting, and diff checks pass. Full Phase 6 reconciliation remains open.

Raw route cleanup continuation (2026-10-09): the 18-case direct Java/native runtime matrix confirms null address maps are skipped after earlier valid candidates, address-only matching spans BrokerData name/ID changes and sibling topics, and cleanup leaves historical Broker version `551` untouched. It retains the live master and removes stale addresses. Latest oracle `6f34d97a`; native regression `301b5aee`; route fixture SHA-256 `a51795ae0027fbc9484383aeee94745d8a7c355ab895fb532496959c568a0c14`. Full workspace, strict Clippy, pinned Maven verify (22 scenarios), comparator/manifest22, format and diff checks pass; whole source/API/input/reverse-export reconciliation remains open.

Historical value-API checkpoint; caller-object alias status was superseded by `7ba69080`: BatchAck Java BitSet functional API continuation (2026-10-09): the owned Rust value now matches pinned Java cases for constructors/`size`, byte and long-array conversion, signed `toLongArray`, index/range and boolean mutations, clear-all, bulk Boolean operations, searches, intersections, equality, hash, text, iteration, and capacity growth. Oracle commits `4196f933`, `2953f259`, `9dd083ed`, `79328f27`, and `a5385223`; implementation/test `0f600297`; checkpoint `lookup.json` SHA-256 `4a8f9a0b49c6e5acdcef48ee9da7bc9d81c5d28361d79f8a8400dd6636ae0719`. Java NIO buffer overloads map at the value boundary through slices, without preserving buffer cursor or identity. Caller-owned `BitSet` alias identity was open at that checkpoint and is covered by the later shared-handle correction; whole-phase source/API/input reconciliation remains open.

Historical range/search API checkpoint, superseded for caller-held BitSet identity by `7ba69080`: BatchAck BitSet range/search API continuation (2026-10-09): the Rust-owned `BitSet` now matches direct Java cases for range `set/clear/flip/get`, next/previous set/clear searches, `intersects`, invalid ranges and word-boundary operations across bits 63–65. Java's `previous*Bit(-1)` returns `-1`, and the BatchAck Base64 body stays unchanged. Oracle commits `24bdefa4`/`1a4031d7`; implementation/test `6ccdafcd`; checkpoint `lookup.json` SHA-256 `5450d735158f927a4685d49fb17204bec2958129a6d2f15ba160b1d1653068ce`. Caller-held BitSet identity was closed later by `7ba69080`; Java `ByteBuffer`/`LongBuffer` cursor mutation is not modeled by the Rust value API; overall Phase 6 source/API/input reconciliation remains partial.

Historical value-API checkpoint; caller-object alias status was superseded by `7ba69080`: BatchAck Java BitSet surface continuation (2026-10-09): `BatchAck.bit_set` is now a Rust-owned `BitSet` value with indexed get/set/clear/flip, cardinality, length, emptiness, and canonical Java byte-array encoding. The pinned Java API oracle verifies single-byte and word-boundary values, negative-index failures, and unchanged Base64 wire output. At that checkpoint, Rust mapped negative indexes to `BitSetError` and used move ownership; the later shared-handle correction closes the DTO field alias. Oracle/test commits `ce97d07c`/`98d26175`; implementation/test `7d2f2b98`; `lookup.json` SHA-256 `4ebfec17860c569431bfb2b7b952f59c83b3eb08d683b6198e4f24d6143cb400`. Full workspace, strict Clippy, pinned offline Maven verify, comparator/manifest22, format and diff gates pass. Full Phase 6 API/source/input reconciliation remains open.

Raw route cleanup null-list continuation (2026-10-09): actual pinned Java `cleanOfflineBroker()` throws `NullPointerException` for a raw route whose `brokerDatas` is null and leaves the current broker-address table unchanged. The Rust cleanup differential now covers and matches this outcome, mapping Java's null-dereference cases to `RuntimeError::InvalidRoute`; the runtime cleanup input matrix has 14 shapes. Oracle/test commits `21d0f14b` and `8c7dc5f8`; route fixture SHA-256 `89067fe4aaeed5a57c5d557fe6006f81a3a91d6bb58974f9a415e4fbbe762944`. Full API/source/input reconciliation remains open.

Historical checkpoint, superseded by `aa4803df`: MessageQueueAssignment nested queue getter continuation (2026-10-09): pinned Java returns the same `MessageQueue` passed to `setMessageQueue()`; the owned-value implementation reproduced getter mutation but not setter-held identity. See the current nested alias correction above and [ADR-0066](../architecture/ADR-0066-shared-assignment-nested-aliases.md). Oracle/test `caff0e22`; implementation at that checkpoint `8fff4781`; checkpoint `lookup.json` SHA-256 `9cdbcae68d20569b3d0cfdbf774aaf4183a63430438d9e3d74e8ad2e64b863d1`.

Historical checkpoint, superseded by `aa4803df`: MessageQueueAssignment attachment getter mutation continuation (2026-10-09): Java returns the same Map passed to `setAttachments()`, while Rust's owned `attachments_mut()` then matched only getter-side value mutation. See the current nested alias correction above and [ADR-0066](../architecture/ADR-0066-shared-assignment-nested-aliases.md). Oracle/test `359ed253`; checkpoint `lookup.json` SHA-256 `8b6dab97da3ebca80f494b07ebcc7b12da396d700587137e136ea8620ed8d972`.

Historical checkpoint, superseded by `7ba69080`: BatchAck mutable BitSet getter continuation (2026-10-09): pinned Java 5.5.1 returns the same `BitSet` passed to `setBitSet`; setting bit 5 through `getBitSet()` is visible through both references. Rust adds `get_bit_set_mut()` for its owned `BitSet.toByteArray()` byte representation, and the differential confirms the updated wire bytes. Rust does not preserve identity with the caller-owned Java `BitSet` or expose the full Java `BitSet` object API. Oracle/test `bd3d2e71`; implementation `d1dc7dfd`; current `lookup.json` SHA-256 `abfe31546b68799aced43ff4f631f5dea37a4c14efac1dfc60a1ed637a22b970`. Workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting, and diff checks pass. Full Phase 6 source/API/input reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Historical getter-mutation checkpoint, superseded for caller aliases by `853b3fdd`: MessageRequestModeSerializeWrapper mutable Map getter continuation (2026-10-09): pinned Java mutations through `getMessageRequestModeMap()` are visible at both the outer topic map and nested group map. Rust adds `get_message_request_mode_map_mut()` and the differential matches both mutations. At this checkpoint, Rust-owned nested `BTreeMap`s did not preserve identity with the Map originally passed to Java's setter; `853b3fdd` and [ADR-0069](../architecture/ADR-0069-shared-request-mode-wrapper-aliases.md) now preserve outer Map, group Map, and row DTO aliases. Oracle/test commits `94a0d62d` and `0614eda5`; implementation `6c19de4c`; checkpoint `lookup.json` SHA-256 `c0ba2b2cf51eb3c900a90e57a807edb25bda2cee0fed1b9e54a303680814c6bb`. Workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator self-test/manifest22, formatting, and diff checks pass. Other collection alias gaps and full source/API/input reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded for caller/setter/getter identity by `60251cee` / [ADR-0070](../architecture/ADR-0070-shared-lite-subscription-dto-aliases.md): LiteSubscriptionDTO mutable OffsetOption getter continuation (2026-10-09): pinned Java mutates the `OffsetOption` passed to the DTO setter and mutates it again through the DTO getter; both references observe the value 13. Rust adds `offset_option_mut()` and the differential matches getter-side mutation through its owned value. Caller-owned object identity remained an ownership difference at that checkpoint. Oracle/test `373dec0c`; implementation `6612a7fe`; checkpoint lookup SHA-256 `0880db9cf0e22cf22580c4c176e3413c7e3b2a2336fcc63dc6f81b616126fcaa`; focused differential, full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, format, and diff checks pass. Other DTO aliases and broader Phase 6 reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical mutable Set getter checkpoint, superseded for caller/setter/getter identity by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md): Heartbeat DTO mutable Set getter continuation (2026-10-09): pinned Java cases now prove mutation through the assigned Sets and getter-returned Sets for `SubscriptionData.tagsSet/codeSet`, `ConsumerData.subscriptionDataSet`, and `HeartbeatData.producerDataSet/consumerDataSet`. Rust adds mutable borrows for its owned HashSets and vectors; the differential matched final members and counts at that checkpoint, before the later shared-handle alias correction. Oracle/test `33829680`; implementation `5151a308`; checkpoint `heartbeat.json` SHA-256 `a03cd054471d1782251f8635adcc35d6da29f67f9900c215eadf67f76f5d9c30`; [ADR-0058](../architecture/ADR-0058-heartbeat-dto-public-accessors.md). Focused differential, full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, format, and diff checks pass. Broader Phase 6 reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded for LiteSubscriptionDTO aliases by `60251cee` and the control-body Set by `66adb0c4`: Lite subscription collection getter continuation (2026-10-09): pinned Java calls mutate `LiteSubscriptionDTO.liteTopicSet` and `LiteSubscriptionCtlRequestBody.subscriptionSet` through setter-held and getter-returned Sets. Rust mutable getters matched mutation through owned collections at this checkpoint; caller-owned Set identity remained open then. Those aliases are now closed for the DTO fields by `60251cee` and for the control body and its elements by `66adb0c4`; `LiteSubscription.liteTopicSet` getter identity is now closed by `e33c43b2` and [ADR-0072](../architecture/ADR-0072-shared-lite-subscription-getter-alias.md). Oracle/test `4fd32a88`; checkpoint lookup SHA-256 `99d189c5eeba83f01b53a5ec23ab56cf5b902043db0849bd54a93ac2a3f3d743`. Focused differential, workspace tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, format, and diff checks passed at that checkpoint. Other DTO aliases and broader Phase 6 reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded for caller Map identity by `2f07fe94` / [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md): ResetOffsetBody mutable Map getter continuation (2026-10-09): pinned Java calls mutate `offsetTable` through both the assigned Map and getter; both views see the changes. Rust adds `get_offset_table_mut()` and the differential matches mutation through the owned rows; caller-owned Map identity remains an ownership difference. Oracle/test `a6e6d723`; implementation `ff81313a`; current lookup SHA-256 `b77cd6ce3487c18e1c2d9b34c06ff2354f9c025ab65ac15a1255a3ed9fb31bc3`; [ADR-0057](../architecture/ADR-0057-runtime-map-body-public-api.md). Focused differential, full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, format, and diff checks pass. Other DTO collection aliases and broader source/API/input reconciliation remain open; P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded for caller Map identity by `2f07fe94` / [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md): Consumer status mutable Map getter continuation (2026-10-09): pinned Java calls mutate both assigned outer Maps and a nested consumer Map; the same values remain visible through Java getters. Rust adds `get_message_queue_table_mut()` and `get_consumer_table_mut()` and the differential verifies changes through the owned maps and nested rows. Caller-owned Map identity remains an ownership difference. Oracle/test `89aef2e2`; implementation `3d5cfd44`; lookup SHA-256 at that update: `9fa29ff2a82f4227ebb8ea83c74c32702b6cf60f2b11e1eebbc8f0b1fff57da7`; [ADR-0057](../architecture/ADR-0057-runtime-map-body-public-api.md). The ResetOffsetBody update above records the current aggregate hash and latest gates.

Historical checkpoint, superseded by `a732974a` (2026-10-10): pinned Java mutates `SubscriptionData.subVersion` through the setter argument and then changes `subString` through `CheckClientRequestBody.getSubscriptionData()`; both views observe the same final object. At this checkpoint Rust exposed only `get_subscription_data_mut()`, so caller-held identity remained different. The later SharedValue correction below and [ADR-0060](../architecture/ADR-0060-shared-java-dto-reference-aliases.md) now cover the caller/getter alias. Oracle `cd8e761e`; implementation at that checkpoint `ecf19a87`; checkpoint `lookup.json` SHA-256 `f3d9e92e8965faa082e06f075755bed7b9fc7d0435698b35a58d19ac809d8c29`. Other DTO aliases and broad source/API/input reconciliation remain open; Phase 6 stays PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded by `a32d00f7`: BatchAck mutable List getter continuation (2026-10-09): pinned Java mutates the List passed to `setAcks` and the List returned by `getAcks`; both views observe add/remove operations, ending with queue id 4. Rust adds `BatchAckMessageRequestBody::get_acks_mut()` for mutations through the DTO getter. Java's identity with the caller's original List remains an ownership difference. Oracle `51b92e77`; implementation `f4d85af0`; current `lookup.json` SHA-256 `9a9c052d3d57d0c4e54e6f8adc1147c71bbfa1fbc07745d63e0920ab670f4442`. Focused differential, full workspace/all-target tests, strict Clippy, pinned Maven verify, comparator/manifest22, format, and diff checks pass. Other DTO collection aliases and whole-phase reconciliation remain open; Phase 6 stays PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded by the QueryAssignment Set alias correction: Assignment response mutable Set getter continuation (2026-10-09): direct Java mutation through both the setter's `Set<MessageQueueAssignment>` and `getMessageQueueAssignments()` is mutually visible; the final member uses queue id 4. Rust adds `get_message_queue_assignments_mut()` to mutate the owned HashSet through its DTO getter. Rust's move-based setter still does not retain Java identity with the caller-owned Set. Oracle `1bfe20bf`; implementation `b280111c`; checkpoint `lookup.json` SHA-256 `ce344c0aa35c634a730f97cf706fba3b41313fd12a2fac294d3e01250f7e9b2c`. The focused protocol differential, workspace tests, strict Clippy, pinned offline Maven verify, comparator self-test/manifest22, format, and diff checks pass. Other DTO aliases and whole-phase reconciliation remain open; Phase 6 stays PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Historical checkpoint, superseded by `045b46e7` (2026-10-10): Java proved setter/getter mutations of the three lock-batch Sets were mutually visible. At that checkpoint Rust exposed mutable borrows of owned HashSets, but caller-held identity remained different. The later SharedValue correction now preserves each outer Set alias and removes structural DTO equality/hash traits; nested `MessageQueue` element identities remain open. Oracle `f80a844d`; implementation at that checkpoint `c8aeb821`; checkpoint lookup fixture SHA-256 `8f8f823b9a63d14dd5cf4d9b86d0538ad5ebe142138d4316639ce3e60bc5fd6c`. See current correction above and [ADR-0062](../architecture/ADR-0062-shared-lock-set-aliases.md). Other aliases and Phase 6 reconciliation remain open.

Historical checkpoint, superseded by `456849fb` (2026-10-10): direct Java calls showed mutation through both the setter List and getter List reached the same final contents. At that checkpoint Rust had only `get_consumer_id_list_mut()` over an owned Vec, so caller-held identity remained different. The later SharedValue correction now preserves the caller/getter alias and bidirectional mutation; see the current correction above and [ADR-0061](../architecture/ADR-0061-shared-consumer-list-alias.md). Oracle `0877435c`; implementation at that checkpoint `dd4156c5`; checkpoint lookup fixture SHA-256 `3d031033fb4f04d0ab78df0bf7818827dec7a460e8264812f303667ee42c020b`. Other alias-bearing DTOs and full Phase 6 reconciliation remain open.

SOCKS bare-reference continuation (2026-10-09): Java private `getProxy` now has four more cases for empty, one-space, dotted bare-name, and numeric bare-name `$ref` values. Each produces a matching CIDR with no proxy address; the numeric bare-name input contains no corresponding root route key. The Rust resolver already matches these outcomes. Oracle/test `47d09d76`; fixture SHA-256 `fd46551f8755e1ca55a6434bdf15eabe58014077a402e8e17928b9eb8d9039d7`, with 499 construction and 32 selection cases. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, format, and diff checks pass. Other Fastjson paths and Phase 6 source/API/input reconciliation remain open.

SOCKS Fastjson reference-path continuation (2026-10-09): direct Java private-selection evidence covers nine more `$ref` forms. Dot notation, double-quoted bracket notation, and whitespace inside a quoted bracket path resolve to the referenced proxy. `#-2` and a bare identifier instead retain a matched CIDR with no address; numeric-index, self, parent, and recursive-descent forms fail selection. Rust now matches those success/missing-address/failure outcomes; the initial native differential failed on the whitespace-bearing path. Oracle/test `c90a1bec`; implementation `27700865`; 499 constructor and 28 selection cases at fixture SHA-256 `2099360007a6e46dbba89ba22c70d7ca7354fe49b2b5129a082bd42ff9f2b011`. Full workspace tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator self-test and manifest22, formatting, and diff checks pass. Other Fastjson forms and Phase 6 source/API/input reconciliation remain open.

SOCKS route iteration-order correction (2026-10-09): direct private Java `NettyRemotingClient.getProxy` calls establish that overlapping matches use Java `HashMap` traversal, not CIDR specificity or JSON field order. Three route-field permutations of `/8`, `/9`, `/10`, and `/11` all select `/9`; another case with distinct source buckets merged into one destination bucket confirms the source traversal order. The Rust differential first selected `/11`; `ProxyRoutes` now preserves root-map order and Java-compatible source/destination bucket iteration. Oracle commits `99284f59` and `6f790b8a`; implementation `965109bd`; 19 selection cases; fixture SHA-256 `3e46fee1230a4dbf66837deca0a17015aeea87913fa9f3b08ad79e1aa0f90c42`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator self-test and manifest22 validation, formatting, and diff checks pass. Broader API/source/input reconciliation remains open.

SOCKS matched-route selection correction (2026-10-09): the pinned private `getProxy` selects a matching CIDR even when the `SocksProxyConfig` bean has no address, returning the CIDR and null address. Rust now preserves the CIDR/route selection separately and defers missing-address handling to connection setup. Oracle/test `1ca984ac`; implementation `c4bcbbbb`; fixture SHA-256 `f3a1558b2722c74fe917bcba299484563ddcc4990b3db58419ac403fa2fb4a69`; 15 selection cases. Full workspace tests, strict Clippy, pinned Maven verify (22 scenarios), comparator/manifest22, format, and diff checks pass. Broader Phase 6 reconciliation remains open.

SOCKS Fastjson special-reference correction (2026-10-09): the Java constructor accepts `$ref` `#-1` and the private proxy lookup selects no proxy; root `$` fails construction with `ClassCastException`. Rust now matches both outcomes after a red native differential. Oracle/test `9a8dc90d`; implementation `8cc47fc8`; the 499-case constructor fixture SHA-256 is `15e8e57a07b7a488f45750d10f5820ed41a49a2aba198254d7e2b85ba84f871a`. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, formatting, and diff checks pass. Other Fastjson reference paths and Phase 6 reconciliation remain open.

SOCKS Fastjson cycle correction (2026-10-09): two direct Java constructor/private-selection cases cover self-reference and two-route cycles; both construct successfully and select no proxy. The red native differential exposed two incorrectly rejected cycles, now resolved to direct routes. Oracle/test `115a2cb1`; implementation `2f60397e`; the constructor corpus has 497 cases at `client-config.json` SHA-256 `0434b8f5de37827ad98c145d60e9888a1d7e851f79e7872742047b4e760d8c03`. Focused constructor/selection tests, full workspace/all-target tests, strict Clippy, pinned Maven verify (22 scenarios), comparator/manifest22, format, and diff checks pass. Other Fastjson grammar and Phase 6 reconciliation remain open.

SOCKS Fastjson reference correction (2026-10-09): actual Java construction and private proxy selection now have three `$ref` cases: a route reference, a chained reference, and an unresolved target. Native parsing maps each resolved target's address and credentials into the referencing route and maps unresolved targets to direct selection; tests confirm the selected CIDR behavior. Java shares referenced bean identity, while Rust stores equivalent owned route values. Oracle/test `80ba235d`; implementation `9cf5a8fb`; current `client-config.json` SHA-256 `0cafefb33a4611746f78005582a3394e7a6de8818932ea3d9d947e99d3d1eb5e`. The corpus now has 495 constructor cases; broader Fastjson grammar and Phase 6 reconciliation remain open.

OffsetOption equality/hash API continuation (2026-10-09): pinned Java and Rust method cases now cover `equals(Object)` for equal/different values and types, null/wrong-type inputs, stable same-object hash behavior, equal-object hash consistency, and `HashSet` deduplication. Rust adds `OffsetOption::java_equals(Option<&dyn Any>)`; derived Eq/Hash preserves Java Set membership. Java numeric `hashCode()` is process-local because it includes enum identity and is not compared across processes. Oracle/implementation `e7fb09f6`; current `lookup.json` SHA-256 `72e92cb461d8b4842622850296c01e0fd604b4d49250990bc3beea2cfcefc544`; [ADR-0059](../architecture/ADR-0059-offset-option-java-equality.md). Full workspace tests, strict Clippy, pinned offline Maven verify, differential comparator self-test and manifest22 validation, formatting, and diff checks pass. Wider source/API/input reconciliation remains open; Phase 6 stays PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

Historical heartbeat DTO accessor checkpoint, superseded for Set aliases by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md): Heartbeat DTO public accessor update (2026-10-09): direct pinned Java calls and a Rust differential now cover every declared getter/setter on `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData`, including defaults, assigned values, and null setter outcomes. Java returns the identical mutable Set assigned to each collection setter; at that checkpoint Rust owned collections and lent immutable borrows, before the later shared-handle alias correction. Oracle and implementation `21591fc7`; checkpoint `heartbeat.json` SHA-256 `7da03948a79dd50a476ba900198731af6186efd735c473e54688853f3209cd87`; [ADR-0058](../architecture/ADR-0058-heartbeat-dto-public-accessors.md). Full workspace tests, strict Clippy, pinned offline Maven verify, formatting, and diff checks pass. Other DTO/input/source reconciliation remains open; Phase 6 stays PARTIAL and P6-V04 stays `PENDING_PHASE_19`.


Historical checkpoint, superseded for caller Map identity by `2f07fe94` / [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md): Map body public method continuation (2026-10-09): direct pinned Java calls and a Rust differential now invoke every getter/setter on `GetConsumerStatusBody` and `ResetOffsetBody`. Both Java constructors supply empty maps; populated/null states match, including nested empty/null consumer maps and nullable offsets. Java returns the exact assigned mutable outer/nested Map; Rust owns its row vectors/maps and returns immutable views. Oracle/test `471b7160`; current `lookup.json` SHA-256 `7b2ce204660f804147471cb2d6d220e669e33991285b64cc68faf699c15a8aff`; [ADR-0057](../architecture/ADR-0057-runtime-map-body-public-api.md). Protocol all-target tests and pinned offline Maven verify pass. Phase 6 remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.

| Java DTO surface | Rust counterpart | Evidence / remaining difference |
| --- | --- | --- |
| `GetConsumerStatusBody.messageQueueTable` and `.consumerTable` getters/setters | `get_message_queue_table` / `get_message_queue_table_mut` / `set_message_queue_table`; `get_consumer_table` / `get_consumer_table_mut` / `set_consumer_table` | `Option<SharedQueueOffsetEntries>` and `Option<SharedConsumerQueueOffsets>` preserve outer and non-null nested Map handles; `ptr_eq` and bidirectional mutation cases match Java. See [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md). |
| `ResetOffsetBody.offsetTable` getter/setter | `get_offset_table` / `get_offset_table_mut` / `set_offset_table` | `Option<SharedQueueOffsetEntries>` preserves the outer Map handle; `ptr_eq` and bidirectional mutation cases match Java while defaults, nulls, and nullable offsets remain covered. See [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md). |

Runtime body public API continuation (2026-10-09; alias status updated 2026-10-10): direct pinned Java 5.5.1 invocations and the Rust differential cover constructor defaults, getter/setter values, null setters, and diagnostic text for `CheckClientRequestBody`, `GetConsumerListByGroupResponseBody`, `LockBatchRequestBody`, `UnlockBatchRequestBody`, `LockBatchResponseBody`, and `ConsumeMessageDirectlyResult`. Rust exposes the corresponding snake_case accessors; lock/unlock request bodies and direct-consume results match Java `toString()`. The later `a732974a`, `456849fb`, and `045b46e7` SharedValue corrections preserve caller/getter aliases for `CheckClientRequestBody.subscriptionData`, `GetConsumerListByGroupResponseBody.consumerIdList`, and the three lock Set containers; `625122ee` additionally closes their nested `MessageQueue` element aliases (ADR-0067). Other DTO aliases remain open. Oracle `7e1155f8`; implementation at that checkpoint `bca4329c`; checkpoint aggregate lookup fixture SHA-256 `7b2ce204660f804147471cb2d6d220e669e33991285b64cc68faf699c15a8aff`; [ADR-0055](../architecture/ADR-0055-runtime-body-public-api.md) and [ADR-0060](../architecture/ADR-0060-shared-java-dto-reference-aliases.md), [ADR-0061](../architecture/ADR-0061-shared-consumer-list-alias.md), and [ADR-0062](../architecture/ADR-0062-shared-lock-set-aliases.md). Full workspace/all-target tests, strict Clippy, pinned offline Maven verify, fmt, and diff checks passed at the checkpoint. Broader Phase 6 reconciliation remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.

| Java DTO surface | Rust counterpart | Evidence / remaining difference |
| --- | --- | --- |
| `CheckClientRequestBody` getters/setters | `client_id`, `group`, `subscription_data: Option<SharedValue<SubscriptionData>>`, `namespace`, and shared-handle accessors | Constructor/null/value cases, `ptr_eq` identity, caller/getter mutation in both directions, and Java `Object.equals`/`hashCode` identity semantics match; see [ADR-0060](../architecture/ADR-0060-shared-java-dto-reference-aliases.md). |
| `GetConsumerListByGroupResponseBody` consumer ID List | Private `consumer_id_list: Option<SharedValue<Vec<Option<String>>>>`; public Rust getter/setter use cloned/shared handles | Java 5.5.1 constructor/method/public-field reflection inventory matches; the Rust field is compile-fail checked as private. Nullable/default values and wire JSON shape match; getter handles retain setter identity (`ptr_eq`) and both mutation directions are visible. Core RPC results snapshot back to an owned `Vec`; Java `Object.equals`/`hashCode` identity semantics match. See [ADR-0061](../architecture/ADR-0061-shared-consumer-list-alias.md). |
| `LockBatchRequestBody`, `UnlockBatchRequestBody` | `mq_set: Option<SharedMessageQueueSet>` with shared Set and shared queue-element handles, plus `Display` | Defaults, field values, nullable Set state, Java `toString()`, container and nested-element `ptr_eq`, both mutation directions, Set value membership, and Java `Object.equals`/`hashCode` identity semantics match. Mutating an element after insertion retains Java HashSet mutable-key behavior; see [ADR-0062](../architecture/ADR-0062-shared-lock-set-aliases.md) and [ADR-0067](../architecture/ADR-0067-shared-lock-set-element-aliases.md). |
| `LockBatchResponseBody.lockOKMQSet` | Private `lock_okmq_set: Option<SharedMessageQueueSet>` with public getter/setter | Pinned Java 5.5.1 inventory is one public no-argument constructor, `getLockOKMQSet()` and `setLockOKMQSet(Set)`, and no public fields; Rust checks the same surface and compile-fail checks field privacy. Empty default, populated/null state, container and nested-element `ptr_eq`, both mutation directions, Set value membership, and Java `Object.equals`/`hashCode` identity semantics match. Mutating an element after insertion retains Java HashSet mutable-key behavior; see [ADR-0062](../architecture/ADR-0062-shared-lock-set-aliases.md) and [ADR-0067](../architecture/ADR-0067-shared-lock-set-element-aliases.md). |
| `ConsumeMessageDirectlyResult` | matching snake_case accessors and `Display` | Constructor defaults, nullable fields, populated values, and Java `toString()` match. |

Historical checkpoint, superseded by `dbaed386` for the response Set and `aa4803df` for nested assignment values: Assignment DTO public API update (2026-10-09): direct pinned Java calls cover constructor defaults, getters/setters, and nullable setter results for `QueryAssignmentRequestBody`, `QueryAssignmentResponseBody`, and `SetMessageRequestModeRequestBody`. Rust exposes each accessor and matches values/defaults, including the request body's `PULL` mode and zero queue count. At this checkpoint `QueryAssignmentResponseBody` exposed `get_message_queue_assignments_mut()` over an owned Set; the later corrections preserve caller/getter identity for the Set and each nested assignment queue/attachments value. Original oracle/implementation `7c7d8b5b` / `fc5f2336`; mutable Set oracle/implementation `1bfe20bf` / `b280111c`; checkpoint lookup fixture SHA-256 `ce344c0aa35c634a730f97cf706fba3b41313fd12a2fac294d3e01250f7e9b2c`; [ADR-0054](../architecture/ADR-0054-assignment-body-public-api.md). Phase 6 remains PARTIAL; P6-V04 stays PENDING_PHASE_19.

Historical input/API checkpoint; the List alias status below was superseded by `a32d00f7`: BatchAck API/input update (2026-10-09): 95 direct pinned Java 5.5.1 cases cover BatchAckMessageRequestBody.decode/encode, root and field token shapes, Java primitive defaults/String coercions, nullable List members, singleton object conversion, duplicate-last properties, and BitSet Base64. A separate invoked API case covers constructor defaults, every BatchAck getter/setter, body getters/setters, and `toString()`. Rust matches values/null states and exposes `get_acks_mut()` for getter mutations; the caller's pre-move List identity remains a Rust ownership difference. Original API oracle/implementation `7f3a4f0e` / `c709e743`; mutable-list oracle/implementation `51b92e77` / `f4d85af0`; checkpoint aggregate lookup fixture SHA-256 `9a9c052d3d57d0c4e54e6f8adc1147c71bbfa1fbc07745d63e0920ab670f4442`; [ADR-0052](../architecture/ADR-0052-batch-ack-fastjson-input.md) records input behavior and [ADR-0053](../architecture/ADR-0053-batch-ack-public-api.md) records the API boundary. Phase 6 remains PARTIAL; P6-V04 stays PENDING_PHASE_19.

Unregister wire/API correction (2026-10-09): pinned 5.5.1 artifacts and bytecode show `MQClientAPIImpl.unregisterClient` sends request code 35 with `UnregisterClientRequestHeader` and no body. The Rust-only `UnregisterClientRequestBody` and unused `UnregisterClientBody` were removed; the Java-shaped header DTO remains. The registry's consumer/producer unregister methods are separately mapped lifecycle operations. See [ADR-0051](../architecture/ADR-0051-remove-non-java-unregister-body.md). Phase 6 remains PARTIAL; P6-V04 stays PENDING_PHASE_19.

MessageRequestModeSerializeWrapper source/API/input checkpoint, with later alias closure in `853b3fdd` / ADR-0069 (2026-10-09): pinned 5.5.1 confirms a public no-argument constructor plus messageRequestModeMap getter/setter. Java initializes the map to empty; absent input preserves that default, duplicate root properties are last-wins, and a null map is omitted during encoding. The 33-case Fastjson corpus covers roots and token shapes at each nested map level, duplicate keys/properties, and malformed null/scalar/array shapes. Rust defaults to Some(empty), uses the shared last-wins object decoder, skips an explicit null map when encoding, and exposes Java-shaped accessors. The later `853b3fdd` / [ADR-0069](../architecture/ADR-0069-shared-request-mode-wrapper-aliases.md) correction preserves setter/getter identity for the outer topic Map, nested consumer-group Maps, and each request-body row; direct Java cases also verify Java Object identity methods. Oracle corpus/API commits 4456695d/910e6838; implementation 33b993b8; then-current lookup.json SHA-256 9d4472bee58e739dbb0efa4c18a3e4405f7b58fd1e1975f862bbc431b61a016f; [ADR-0050](../architecture/ADR-0050-message-request-mode-wrapper-fastjson-input.md) records the boundary. Workspace tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and fixture drift/cross-decodes), formatting, and diff checks pass. Broader Phase 6 reconciliation remains partial; P6-V04 stays PENDING_PHASE_19.

Historical API/input checkpoint, superseded for caller Map identity by `2f07fe94` / [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md): Reset offset body API/input continuation (2026-10-09): `ResetOffsetBody.offsetTable` maps Java `Map<MessageQueue, Long>` to `Option<Vec<(MessageQueue, Option<i64>)>>`, preserving the constructor-empty default, explicit null table, and nullable offsets. The 77-case actual Java corpus covers every JSON token type plus non-object roots, object-valued queue keys, quoted-key rejection, duplicate last-wins behavior, malformed tables, nullable queue-key names omitted on encode, primitive queueId defaulting to zero, all seven queue-key field token shapes, and exact encoded output. It also pins Java Long conversion: booleans become 1/0, numeric and decimal strings truncate, empty strings become null, whitespace strings and integer overflow reject, decimal fractions truncate toward zero, plain decimal overflow wraps to 64 bits, and extreme exponent tokens and strings follow Java double-to-long saturation/underflow. The public API differential also proves mutation visibility through the assigned and returned Maps; Rust adds `get_offset_table_mut()` for mutations through its owned rows but does not preserve the caller's Map identity. Dedicated `decode_java_json` / `encode_java_json` paths match Java's body boundary. Current aggregate lookup fixture SHA-256: `b77cd6ce3487c18e1c2d9b34c06ff2354f9c025ab65ac15a1255a3ed9fb31bc3`; [ADR-0048](../architecture/ADR-0048-reset-offset-fastjson-map-input.md) and [ADR-0057](../architecture/ADR-0057-runtime-map-body-public-api.md). Broader DTO/source/input coverage remains incomplete and P6-V04 stays `PENDING_PHASE_19`.

Historical API/input checkpoint, superseded for caller Map identity by `2f07fe94` / [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md): Consumer status body API/input continuation (2026-10-09): `GetConsumerStatusBody` exposes Java-shaped getters/setters and nullable queue/consumer maps; its Rust fields use optional row tables and optional offsets to preserve Java's omitted/default versus explicit-null states and nullable `Long` values. The direct 143-case Java corpus covers every JSON token type for both tables and non-object roots, plus non-standard MessageQueue object keys with null/absent fields, duplicate last-wins decoding, nested nulls, invalid shapes, all seven queue-key field token shapes at both table depths, and exact `encode()` output. Long coercions match Java for booleans, numeric/decimal strings, fractions, decimal overflow, extreme exponent values/strings and integer overflow. Key encoding omits null topic/brokerName while keeping queueId; key-field coercions match the standalone Java MessageQueue DTO rules. Rust's BTreeMap traversal is deterministic while Java HashMap order is unspecified. Lookup fixture SHA-256: `202dc2d502473e2e2a73861bfcc82e359668c8f5222744b619052be04c9c014f`. See [ADR-0047](../architecture/ADR-0047-consumer-status-fastjson-map-input.md); broader DTO/source/input coverage remains incomplete and P6-V04 stays `PENDING_PHASE_19`.

ConsumeMessageDirectlyResult field-shape and duplicate-input correction (2026-10-09): the pinned Java 5.5.1 corpus now has 66 cases, adding all seven token kinds across its five fields, seven root shapes, and duplicate-last cases for every field. Java defaults missing `autoCommit` to true; explicit null and numeric values for primitive booleans become false. `remark` uses Fastjson String coercion, while `spentTimeMills` maps null to zero, truncates fractions, converts booleans to 1/0, and rejects strings/arrays/objects. `consumeResult` retains its enum coercions. Rust now uses the shared raw-object last-wins decoder and field coercers; its explicit `Default` implementation also matches Java's no-argument constructor. Oracle/test `910cbd2`, implementation `00d2ea8`, constructor-default regression/fix `6d53470`/`b4ec6f4`, fixture SHA-256 `c7662fe1bbbe62726eef0eda14774a3bc08b3910581d771153c3b7b04eccaf40`; [ADR-0049](../architecture/ADR-0049-consume-result-fastjson-input.md). Full workspace tests, strict Clippy, pinned offline Maven verify (22 scenarios and fixture checks), fmt and diff pass. Broader Phase 6 source/API/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

Lite subscription identity, API, and reverse-export continuation (2026-10-09): pinned 5.5.1 `LiteSubscriptionDTO`, `LiteSubscription`, and `LiteSubscriptionCtlRequestBody` inherit identity equality; Rust structural `PartialEq`/`Eq` was removed, with direct Java cases plus compile-fail API checks. Added Rust aliases `body::LiteSubscriptionDTO` and `header::GetConsumerListByGroupResponseBody` for exact Java simple class names. Rust maps the LiteSubscription field/method/input surface, DTO and OffsetOption constructors/accessors, control-body Set accessors, toString forms, and OffsetOption value-based HashSet behavior. Fixture SHA-256 at that update: `d618ea8b60671b98fc7c8ec53746c2a0f4bddcb7c744d061b934d38cd6981538`. The Java numeric hash is JVM-local; its equal-hash contract and Set membership are now verified, while broader source/config/input audit remains open. P6-V04 stays `PENDING_PHASE_19`.

Lock batch Set DTO source/input closure (2026-10-09): pinned 5.5.1 declares lock and unlock `mqSet` plus response `lockOKMQSet` as `Set<MessageQueue>`. Direct Java coverage now includes 46 LockBatchRequestBody, 46 UnlockBatchRequestBody, and 25 LockBatchResponseBody cases. At this input-matrix checkpoint Rust matched singleton object conversion, null members, equality deduplication, last-wins fields, primitive boolean/String coercions, and constructor Set defaults. At the current implementation, those nullable Set values use `SharedMessageQueueSet`, sharing both the outer Set and nested queue objects; `lock_batch_mq` snapshots the nullable Set result at its existing owned boundary. See [ADR-0062](../architecture/ADR-0062-shared-lock-set-aliases.md) and [ADR-0067](../architecture/ADR-0067-shared-lock-set-element-aliases.md). Pinned Java artifacts contain no `UnlockBatchResponseBody`; the unused Rust-only DTO was removed in `a5fb87ce`, and unlock responses use `LockBatchResponseBody`. Lookup fixture SHA-256: `29211416d6c3f8c9af305fbe18e66b8ad1ada7c251333f69b8cd2f7970938d49`; [ADR-0046](../architecture/ADR-0046-lock-batch-set-fastjson-input.md). Other source/API/input reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Lite subscription DTO source/input closure (2026-10-09): pinned 5.5.1 declarations confirm `subscriptionSet`, `LiteSubscriptionDTO.liteTopicSet`, and `LiteSubscription.liteTopicSet` are Java `Set` fields. The combined direct Java oracle now has 114 cases: 33 prior enum/default/duplicate/collection inputs plus every `LiteSubscriptionDTO` field token, wrapper roots, `subscriptionSet` value shapes, `liteTopicSet` member shapes/deduplication, and `OffsetOption.value` conversions. Red tests found mismatched String coercions, primitive long conversion, and duplicate null Set members; Rust now matches and uses `BTreeSet<Option<String>>` for nullable string sets. The outer DTO set keeps separately decoded DTO instances and deduplicates nulls; its `Vec` wire representation preserves input order because Java HashSet order is unspecified. Lookup fixture SHA-256: `ecb83ddd48d7d19fd3e8e015bc6d281b67f96d1c99e59091ace43d4d86c41756`; [ADR-0040](../architecture/ADR-0040-lite-subscription-list-fastjson-input.md). Other DTO/source/input reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Runtime DTO root-shape continuation (2026-10-09): seven direct Java 5.5.1 cases each cover roots for `QueryAssignmentResponseBody`, `GetConsumerListByGroupResponseBody`, and `CheckClientRequestBody`. Empty objects decode, null roots fail at the source-used getter, and array/scalar roots reject; native acceptance, fields, and response re-encoding match. Counts are 37 assignment-response, 23 consumer-list, and 43 check-client body cases. Fixture hashes: lookup `9d4472bee58e739dbb0efa4c18a3e4405f7b58fd1e1975f862bbc431b61a016f`, client-check `cdd164bbcaf8cb7a096828b289f69c56c2706c965e97bfe4f228b8ad08c16ee2`.

Assignment response fraction field continuation (2026-10-09): the direct Java `QueryAssignmentResponseBody.decode` matrix now has 30 Set-field inputs plus seven root-shape inputs. A fractional number at `messageQueueAssignments` rejects with `JSONException`; the Rust differential rejects it too. Oracle/test commit `3e58bc1`; the full 37-case corpus has `lookup.json` SHA-256 `4723f8ad95ae556dbc99db83ef81cc311f0aae128cf59e23f8bbda6b1045618a`. No production change was needed. Broader input/source reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.

Route nullable-map-key continuation (2026-10-09): Java 5.5.1 Fastjson decodes quoted and unquoted `null` keys as an actual null `Long` key in `BrokerData.brokerAddrs`; `TopicQueueMappingInfo.currIdMap` rejects them because its key type is `Integer`. Rust represents broker IDs as `Option<i64>` in the public DTO and route snapshot, serializes a null key as `"null"`, and preserves Integer-map rejection. Pinned bytecode shows Java unboxes selected IDs in admin/fallback lookups and for heartbeat sends; Rust's matching lookup methods return typed route errors at those branches. The route input matrix now has 399 cases at fixture SHA-256 `0472ed8171207d35519adae7521741d359e343a3c378cf75537cb5c29cb41313`.

Heartbeat DTO root-token input continuation (2026-10-09): the current 241-case Java Fastjson corpus includes 28 root-shape cases across all four heartbeat DTOs. Java accepts empty-object and null roots, rejecting arrays and scalar roots; native optional DTO decoding matches and normalizes the constructor-time `SubscriptionData.subVersion` for the empty-object case. No production change was needed. Fixture SHA-256 `385fc38890b6f18fc9ea74a4a4d2a47f52c27bae5ffc7851064c594cef426803`; broader DTO and source reconciliation remains open.

Route DTO numeric-key continuation (2026-10-09): direct Java probes cover unquoted signed integers, leading zeros, plus signs, fractions, and exponents in numeric-key maps, plus numeric keys in string-keyed route maps. Integer maps retain Java integer coercions; string maps canonicalize number text (`+1` to `1`, `1e2` to `100.0`); root bean numeric keys reject. The native route parser now applies those conversions by map context, and object-to-String conversion emits nested numeric JSONObject keys in Fastjson's unquoted form. All 360 route DTO inputs pass; current fixture SHA-256 `e0a36ac2b1139acacd11536ba9a22bc3532d4b200ec8eabb21b1d0f6a82995e4`.

Nested route collection/member continuation (2026-10-09): expanded direct Java route DTO evidence to 337 cases. Seven token kinds cover `queueDatas` and `brokerDatas` elements, `filterServerTable` map values, and `topicQueueMappingByBroker` map values, alongside the previously tested list and map entries; an empty-string filter value is also pinned. Java accepts object/null DTO elements, object/null mapping values, and string/array/object filter map values, converting empty string to an empty list. Rust initially rejected map-level string/object/empty-string filters; `deserialize_optional_string_list_map` now matches Java. All 337 route DTO cases at that checkpoint passed; fixture SHA-256 `3d631d0c40c1a6f96a1a4250d1e1c7012c2062551d65d073a84b8f9a8c8ac375`.

Static mapping entry-value continuation (2026-10-09, prior checkpoint): the Java oracle covered all seven JSON token kinds inside `TopicQueueMappingInfo.currIdMap` values. Null, nonnumeric string, and array were rejected; integer, fraction, Boolean, and object `{}` were accepted as 7, truncated 1, 1, and 0. The 308-case checkpoint fixture SHA-256 was `936ee195d1e2a02268a59c210e5b2732f16b1725904b1d9e78bf8418e063b071`.

Nested route-collection member continuation (2026-10-09, prior checkpoint): 14 direct Java cases covered the seven JSON token kinds in broker-address map values and filter-server list members. Java accepted all, converting non-null structured/scalar values to strings and preserving null. Route fixture SHA-256 at that checkpoint: `8de4b09d06779e0f5bbe11d3bad07fbbadf792a2eb2dfa39af1bbf1158c43a0c`.

Fastjson escape-follower continuation (2026-10-09): the pinned SOCKS parser oracle scans every BMP code unit and every supplementary scalar after a backslash. Java accepts 15,872 BMP units and 253,952 of 1,048,576 supplementary scalars; native parsed values and rejections match for every representable input. Isolated surrogate units remain Java-only because Rust `String` cannot represent them. Both focused tests pass; `client-config.json` SHA-256 `bf9b63dfb9c38a4f4090a4326a5d14bc9bff62478e4df4330825cb5009bc08f3`. Other Fastjson grammar and whole-phase source/input reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Raw route cleanup default-input continuation (2026-10-09): direct Java `cleanOfflineBroker` coverage expanded from 8 to 13 shapes. Five additions cover absent route fields, an empty broker list, a missing address map, a null broker name with a live address, and ignored null queue/filter collections with a live address. Java returns an empty address table for the first three and retains the live slave address for the latter two; the native focused test matches all 13. `route.json` SHA-256 `b7f7653977913cd8a545f9d9d59153a459ca343ced1dcc897d92302a4b926df0`. Other route inputs and full source/API reconciliation remain open.

Raw route cleanup input continuation (2026-10-09, prior checkpoint): the direct Java matrix had eight malformed/null-shape cases. Null broker lists and rows throw before Java mutates the broker address table; null address maps are skipped, while null address values are retained when present in both the published and raw routes. Route fixture SHA-256 at that checkpoint: `340c6a0e79a2657efb0de3496e5da003ff821fda8434d3572a3e45c407b69102`.

CheckClientRequestBody field-shape continuation (2026-10-09): direct Java Fastjson coverage now has 36 cases, including all seven token kinds on each of `clientId`, `group`, `subscriptionData`, and `namespace` (28 new cases). The generator pins nested `SubscriptionData.subVersion` for deterministic DTO snapshots. Java/native acceptance and normalized values match; no production change was needed. `check_client_body_inputs_match_invoked_java_fastjson` passes. `client-check.json` SHA-256 `7e9d758ce72e0282d484dc41404e77a318d922662def78988855fe4dc8958570`. Other protocol DTO inputs and full Phase 6 reconciliation remain open.

Heartbeat DTO field-shape continuation (2026-10-09): the pinned Fastjson corpus now has 213 inputs, adding 140 cases across 20 fields in `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData` (null, numeric string, integer, fraction, Boolean, array, object). A first comparison mixed constructor-time `subVersion` values because those defaults are wall-clock based; the generator now pins `subVersion: 0` on unrelated SubscriptionData field probes. The direct explicit-null case confirms Java and the existing native decoder both produce zero, while an omitted property retains the constructor timestamp. Focused differential passes; no production change was needed. Current `heartbeat.json` SHA-256 `4c6aee94aaec527dfb864bea1bed8d6f2903132e6e73e3a353c3ce08a77affdf`. Broader DTO/Fastjson and source reconciliation remain open.

SOCKS proxy DTO field-shape continuation (2026-10-09): added 21 direct Java Fastjson cases for null, numeric string, integer, fraction, Boolean, array, and object values across `SocksProxyConfig.addr`, `username`, and `password`. Java accepts all 21 and coerces non-null values to its string form; Rust matches them. Native `MissingAddress` now retains username/password when `addr` is null, matching the decoded DTO. Focused differential passes; `client-config.json` SHA-256 `d86eef5bdf622121721fe6c0663879b9704b709cb84e21ca0aab6198e3b18174`. Other Fastjson grammar and full Phase 6 input/source reconciliation remain open.

Route DTO field-shape continuation (2026-10-09, prior checkpoint): the direct Java input corpus had 287 cases. Seven-value-shape probes covered every field across `QueueData`, `BrokerData`, `TopicQueueMappingInfo`, and `TopicRouteData` (22 fields × 7 JSON kinds). The red differential found `TopicQueueMappingInfo.epoch: {}` incorrectly decoded as zero; both DTO decode paths reject object values like Fastjson. The native comparison treats nested omitted/null DTO fields as the same null value, with JSON encoding checked by separate fixtures. Route fixture SHA-256 at that checkpoint: `294e3cd94cad4df71d55c1ce7c7060096993749b1f5932c7089f778226212a48`.

Static route-owner continuation (2026-10-09): the direct Java endpoint converter matrix now has ten cases for sparse/disjoint/overshoot IDs, per-ID epoch overlap, `__global__`/`global__` logical-scope aliases, epoch threshold -1, nonpositive totals, null scope, and nullable/empty owner names on non-dereferencing inputs. Rust `overlapping_static_owners_follow_invoked_java_epochs_and_fallback` matches canonicalized endpoint rows. Null `currIdMap` and null owner dereference failures remain in the separate direct Java conversion oracle. Route fixture SHA-256 `a17b806643611eb03c4e11cbcd92ca81700b2384a2547e9370aff102e9eaceb9`; broader route inputs and Phase 6 source/API reconciliation remain open.

Endpoint host-input continuation (2026-10-09): pinned Java `NettyRemotingClient.getHostAndPort` uses the last colon and substring extraction; a direct fixture inserts all 65,536 BMP UTF-16 units and all 1,112,064 Unicode scalar values into non-bracketed hosts, then applies Java `Integer.parseInt` plus `InetSocketAddress.createUnresolved`. Java accepts and preserves every input. Native tests compare address-key retention and host/port splitting for every Unicode scalar value; isolated Java surrogates are covered by the UTF-16-unit fixture but cannot be represented in Rust `str`. Address fixture SHA-256 `1ed29bbb157c3f42af594cffc033e4ff1b7b39fe8b94af71367782f385640a64`. Pinned offline Maven verify and focused native tests pass. This closes host substring passthrough only; DNS/interface behavior and broader transport variants remain under reconciliation.

Fastjson SOCKS trivia correction (2026-10-09): direct Fastjson 2.0.63 Java probes cover every BMP UTF-16 unit before a route and `null`, C0 units `0..=32` at six token boundaries, and then every BMP unit at all six boundaries with parsed route-key/proxy-field observations. The leading probes accept only NUL, backspace, tab, LF, form feed, CR, space, BOM, and U+FFFE. Inside a root object, Fastjson accepts U+0080, U+0088–U+008A, U+008C–U+008D, NBSP, U+00C0, U+00C8–U+00CA, U+00CC–U+00CD, and U+00E0 as trivia; it also accepts trailing commas in the two matching close-token positions. U+001A acts as EOF at the two recorded root boundaries. `every_bmp_leading_code_unit_matches_invoked_java_fastjson` compares all leading units and 393,216 boundary inputs including parsed route values. `client-config.json` SHA-256 `5675aa477cde1b80fa7c9e4664c93c0b425eba0ff60df7ae7947be354d539221`; the curated SOCKS constructor corpus remains 492 cases. Other Fastjson grammar and whole Phase 6 reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Address numeric-input continuation (2026-10-09): the direct Java address fixture probes all 65,536 legal socket ports and all 65,536 UTF-16 code units as single-character `Integer.parseInt` inputs; the Maven oracle runtime is Java 27, vendor Homebrew, and accepts 370 BMP decimal-digit code units. Rust exhaustively matches both sets in `every_java_valid_socket_port_is_accepted_and_preserved` and `every_bmp_port_digit_matches_pinned_java_integer_parse_int`. Pinned offline Maven verify, remoting all-target tests, strict remoting Clippy, fmt and diff pass. `address.json` SHA-256 `bcd1d0075bc9fb54fd94225193e13cba2082cefad02f3fed32af80acb784f85a` at that checkpoint; the later host continuation updates it to the hash above. This closes the port range and single-BMP-digit mapping under the recorded oracle runtime, while other address/transport variants and full Phase 6 reconciliation remain open.

Named IPv6 scope correction (2026-10-09): the Java 5.5.1 address fixture now includes a loopback `NetworkInterface` case whose `Inet6Address` normalizes with its interface name (`%lo0`), alongside the existing numeric `%7` input. The Rust oracle first failed with `%1` and now preserves the scope name through Unix interface enumeration; Windows uses OpenJDK's `ConvertInterfaceLuidToNameW` source mapping. Oracle/test commits `3c3a57e` and `0ca2d6b`; implementation `c3101f4`; `address.json` SHA-256 `89143f5f832705d4d86cdfc24f0dc36a7fe2cf7a803c66e28ed37f45ff9e354a`. Core tests, strict Clippy, offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. The Windows-specific production module and its tests now type-check against `x86_64-pc-windows-gnu` in an isolated harness. The full workspace target check stops in unrelated `aws-lc-sys` because `x86_64-w64-mingw32-gcc` is absent; Windows runtime enumeration and general interface ordering remain unverified. Phase 6 reconciliation remains open, and P6-V04 stays PENDING_PHASE_19.

Route-presence publication oracle (2026-10-09): an actual unstarted Java `MQClientInstance` route update delivers `TopicPublishInfo` with `haveTopicRouterInfo=true`, a non-null route, and `ok=false`/zero queues; a missing route returns false and delivers no update. Oracle/test commit `8529ab7`; `route.json` SHA-256 `649c4ce7ce97e0ddc619330082eaa7c7d8f81af0c3040e56d06f9ff14416214d`. Rust `accepted_empty_route_keeps_route_presence_separate_from_publish_queue_ok` verifies an accepted empty `TopicSnapshot` is distinct from a missing route over loopback. Full core crate tests, strict core Clippy, Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. This verifies only the route-presence boundary; Phase 7 still owns producer-local `TopicPublishInfo` methods and fallback.

SOCKS exponent underflow key correction (2026-10-09): eleven pinned Java constructor inputs cover exponent overflow/underflow, negative zero, `D`/`F` underflow suffixes and collisions. Java maps `1e309`/`-1e309` to `Infinity`/`-Infinity`, positive underflow to `0.0`, and nonzero negative underflow to `-0.0`; lexical `-0e-1000` remains `0.0`. The first native differential failed because negative underflow lost its sign. Test commit `bc5c6dd`; implementation `f5b02d4` preserves `-0.0` only for negative nonzero exponent values that round to zero. The corpus now has 492 cases at SHA-256 `fcc525eb617ab9d5f257eac11ed9ae10fb6e55fd0c7035603e105d40b665007d`. Focused differential, full remoting crate tests, pinned offline Maven verify (22 scenarios), strict workspace Clippy, comparator/manifest22, fmt and diff pass. Broader grammar and Phase 6 reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

SOCKS special-key rejection continuation (2026-10-09): nine direct pinned Java constructor inputs reject unquoted `NaN`, `Infinity`, `-Infinity`, `undefined`, case variants of Boolean/null tokens, and `falsehood`. Rust's existing Fastjson literal guard matches all nine; no implementation change was needed. Test commit `1843b1c`; the constructor corpus now has 481 cases at SHA-256 `0dcc04b4ce7d41cbd9dc3b38750c52e492f527345078e0ac60ca4c7187129ee5`. The focused differential, pinned offline Maven verify (22 scenarios), strict all-target/all-feature Clippy, comparator/manifest22, fmt and diff pass. Remaining Fastjson and Phase 6 reconciliation stay open; P6-V04 stays PENDING_PHASE_19.

SOCKS numeric route-key grammar continuation (2026-10-09): 18 direct pinned Java constructor inputs cover signed base-prefixed rejection, unsigned leading-dot rejection, negative and signed decimals, exponent signs, and `L`/`D`/`F` suffixes. Java rejects unquoted `.5` and `.5F` while accepting `+.5`/`-.5` as `0.5`/`-0.5`; the first native differential exposed `.5` being accepted by JSON5. `afbca6d` records the oracle and regression; `30556c2` rejects only unsigned leading-dot root route keys. The constructor corpus now has 472 cases at SHA-256 `37110dd0267bd8c6e07568cce2330519cc28e6e1116719aa03dd9325234e06f0`. The focused differential, remoting crate suite (69 unit and 82 integration tests), pinned offline Maven verify (22 scenarios), strict all-target/all-feature Clippy, comparator/manifest22, fmt and diff pass. Broader grammar and Phase 6 reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

SOCKS base-prefixed root-key rejection continuation (2026-10-09): direct pinned Java construction rejects unquoted hexadecimal `0x2a`, binary `0b10`, and octal `0o10` keys as `JSONException`; the native differential rejects all three too. Test commit `a72996e`; the constructor fixture now contains 454 cases at SHA-256 `213e2d8880557161c187fe449e668e36495c2acd17164e6288b1f70840dc3651`. Focused 454-case differential, pinned offline Maven verify (22 scenarios and deterministic fixtures), strict all-target/all-feature Clippy, comparator/manifest22, fmt and diff pass. No implementation change was needed; broader grammar and Phase 6 reconciliation remain open.

SOCKS cross-type route-key continuation (2026-10-09): five direct Java inputs confirm numeric `1` merges with string `"1"`, Boolean `true` merges with string `"true"`, and actual null remains distinct from string `"null"`. Both collision orders retain the final proxy value; the null and string-null entries coexist with their own values. Rust matches all outcomes. Test commit `9e27434`; implementation remains `fc03fa6`; the constructor corpus has 451 cases at SHA-256 `86c8c9369e43579c6f30f486045c9b8c2106076613e6e8627a029af5dfce9452`. The focused differential, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and deterministic fixtures), comparator/manifest22, fmt and diff pass. Broader Fastjson and Phase 6 reconciliation remain open; P6-V04 stays pending Phase 19.

SOCKS numeric route-key continuation (2026-10-09): four direct Java inputs collide distinct root numeric spellings (`1`/`01`) after Java converts them to string-map keys. Forward, reverse, and interleaved duplicate orders all leave the final property value at key `"1"`; Rust matches the collapsed key and last route address. Oracle/test commit `a3a1367`; implementation remains `fc03fa6`; the full constructor corpus had 446 cases at SHA-256 `f73d93400a223d511d4d745eb8140d97fd455d8632b0b35eacd889d010754033`. The focused differential, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and deterministic fixtures), comparator/manifest22, fmt and diff passed. Broader Fastjson and Phase 6 reconciliation remain open; P6-V04 stays pending Phase 19.

SOCKS unquoted scalar route-key correction (2026-10-09): Java accepts unquoted numeric, Boolean, and null keys at the root route map, while rejecting an ordinary identifier such as `default`. Java exposes numeric keys as canonical strings (`1e2`→`100.0`, `+10`→`10`), Boolean keys as `"true"`/`"false"`, and preserves `null` as an actual null key. Five added padding/suffix cases confirm `0123`→`123`, `123L`→`123`, `1.25F`→`1.25`, `1D`→`1.0`, and `1e7F`→`1.0E7`. Rust normalizes these root keys before JSON5 parsing and stores null separately from every string key. Oracle/test commits `605b87d` and `a373244`; implementation `fc03fa6`; the constructor corpus had 442 cases at SHA-256 `ec33171ed7bd799715775ddb08e3dbce00c7bd1ff4f349efde3de1a6759b39e1`. The focused 442-case differential, pinned offline Maven verify (22 scenarios and deterministic fixtures), comparator/manifest22, fmt and diff passed; the full workspace/all-target and strict Clippy gates passed with the unchanged implementation. Null-key route selection exception-class parity is not established; broader Fastjson and Phase 6 reconciliation remain open, with P6-V04 pending Phase 19.

SOCKS EOF line-comment boundary (2026-10-09): three direct pinned Java 5.5.1 constructor inputs show that an EOF comment after a complete nested route accepts an unclosed root object, while EOF inside an unclosed route object or array rejects. The native parser already matches all three through its EOF-comment recovery path. Oracle/test commit `b468c58`; `client-config.json` now contains 429 SOCKS constructor cases at SHA-256 `1f25f9939e05b869005252f52fa9bb65510486e8f3caa74d51559c64ec91409b`. The focused 429-case differential and pinned Java fixture generation pass; no production change was needed. Other Fastjson grammar and full Phase 6 reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Numeric map-key normalization correction (2026-10-09): eight direct pinned Java 5.5.1 cases prove that `BrokerData.brokerAddrs` (`Long`) and `TopicQueueMappingInfo.currIdMap` (`Integer`) retain the final property value across key aliases and interleaved exact duplicates, such as `"5"`/`"05"` or `"1"`/`"01"`. `0bc9d7b` first preserved property order for normalized keys; the expanded oracle then exposed repeated exact keys retaining their original position, fixed by `f137d2e` using an ordered raw-entry visitor that retains every occurrence. Oracle/test commits `62d9454` and `5d82500`; route DTO corpus has 133 cases at SHA-256 `edb7150e939a36d7d5b570da13d08f20d3a385d8e992f7074d3fbf4a92c0e26b`. Focused red/green differential, full workspace/all-targets tests, strict all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. Other Fastjson grammar and full Phase 6 reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Automatic ClientConfig IP selection correction (2026-10-09): pinned Java `NetworkUtil.getLocalAddress()` and no-argument `ClientConfig` both select `10.10.10.111` on this macOS host; Rust previously selected `192.168.139.3` because macOS `getifaddrs` orders the two private IPv4 candidates opposite to the observed Java list. Commit `5791e28` reverses only the macOS IPv4 candidate sequence before the shared Java selection rule; the native regression now passes. Oracle/test commit `482e02d`; current `client-config.json` SHA-256 `138164047171a6e1bb649d3b70239a023245341852cd4036148d80d3c722b5ea`. Full workspace, strict Clippy, pinned Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. The evidence is bounded to this pinned macOS/JDK IPv4 ordering; non-macOS and IPv6-only fallback remain under reconciliation. P6-V04 stays PENDING_PHASE_19. See [ADR-0045](../architecture/ADR-0045-automatic-client-ip-java-order.md).

Automatic address candidate-selection audit (2026-10-09): the direct Java `NetworkUtil.getLocalInetAddressList()` candidates are now compared against native macOS selection. The IPv4 order after the OS adapter matches Java, and selection matches Java for both the full host candidate list and its IPv6-only subset. Overall Java and native interface enumeration order differs; only the selected results required by the fixture are asserted. Pinned 5.5.1 bytecode shows that IPv6 selection returns the first non-internal candidate or, if all candidates are internal, the first candidate. A native ordered-candidate regression distinguishes the first of two external IPv6 addresses. Non-macOS adapter ordering and a real IPv6-only interface remain open; no production change was needed.

SOCKS nested bean Fastjson input continuation (2026-10-09): four actual Java `MQClientInstance` construction inputs extend `socksConstruction` from 422 to 426. They cover duplicate-last `addr`/`username`/`password`, duplicate username ending in null, boolean-to-string password coercion, and ignored uppercase/camel-case aliases. Java and the native parser match all 426 constructor and reflected `proxyMap` outcomes; no implementation change was needed. Test commit `5b44517`; current `client-config.json` SHA-256 `478a909c8a7fea8f439e3fafc3ecbdc8a27a8aede6d515dc6f63f59552abb333`. Focused differential, pinned offline Maven verify (22 scenarios and deterministic fixture checks), fmt and diff pass. More Fastjson grammar and full Phase 6 source/API/input reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

ClientConfig NameServer fallback correction (2026-10-09): pinned Java `NameServerAddressUtils` reads `rocketmq.namesrv.addr` first and `NAMESRV_ADDR` second. Rust previously read the non-Java `ROCKETMQ_NAMESRV_ADDR`; oracle/test commit `e630688` reproduced the missing fallback and implementation `07f8dee` fixes it while preserving explicit blank precedence. The client-config fixture now records both Java property keys and constructor outcomes at SHA-256 `8db2067f605af9abdabfd212cf02da379f986f6021fb8a456bc50c5da8b7ecdf`. Full workspace, strict Clippy, pinned Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. Other constructor/property inputs and settings activation remain open; P6-V04 stays PENDING_PHASE_19. See [ADR-0043](../architecture/ADR-0043-clientconfig-nameserver-environment-fallback.md).

Check-client request body Fastjson input correction (2026-10-09): eight direct pinned Java 5.5.1 inputs cover last-wins duplicates on all four fields, nested subscription replacement, scalar-to-string conversion, explicit null, unsupported scalar nested data, and unknown properties. Native derived decoding failed on duplicate `clientId`; `516f9e0` now uses shared raw-object last-wins decoding and Fastjson nullable-string conversion. Oracle/test `3cba9ba`; client-check fixture SHA-256 `ef84e70eb27c60a9ceab9cfe81f2922295a0ff71bead0159eb606085b7e61277`. Full workspace, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt, and diff checks pass. Other protocol DTO inputs and complete Phase 6 reconciliation remain open; P6-V04 stays PENDING_PHASE_19. See [ADR-0042](../architecture/ADR-0042-check-client-body-fastjson-input.md).

Heartbeat DTO null/shape input continuation (2026-10-09): two direct Java 5.5.1 inputs extend `heartbeatDtoInputCases` from 71 to 73. Java decodes explicit-null `SubscriptionData.codeSet` as absent on re-encode and rejects a scalar string for `ConsumerData.subscriptionDataSet`; the existing native decoders match both, so no production change was needed. Oracle/test commit `489a13d`; heartbeat fixture SHA-256 `4303f486a9ea17eb0717ed95535e1ccb721c0eda326c4a486d8b64c7312dac9f`. Focused heartbeat DTO differential, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff checks pass. Other heartbeat/API/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

Route DTO collection-input correction (2026-10-09): four direct Java cases cover `TopicRouteData.queueDatas` and `brokerDatas` singleton-object conversion plus null rows. Fastjson wraps an object value as a singleton list; native `Vec` decoding previously rejected it. Shared decoder `399dbc1` now parses each raw DTO token, preserving null rows and nested last-wins behavior. Oracle/test commit `05fa719`; route DTO corpus now has 125 cases at SHA-256 `a733fa2d1a13e8aed4237bd2e848b1908b262dbc9f724c0e52b6b60d56495b77`. Focused route/Lite differentials, full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff checks pass. Other route DTO/input/source reconciliation remains open; P6-V04 stays PENDING_PHASE_19. See [ADR-0041](../architecture/ADR-0041-route-dto-list-fastjson-input.md).

Lite subscription collection input correction (2026-10-09): ten pinned Java Fastjson cases cover null/empty Set values, object-to-singleton-Set conversion, null members, and rejected scalar/member shapes. Java retains one null Set member and omits a null field on re-encode; the native `LiteSubscriptionCtlRequestBody.subscription_set` uses `Option<Vec<Option<LiteSubscriptionDto>>>` as its wire representation and matches the observed membership. Oracle/test `27a55e3`; implementation `5462e2f`; the 33-case checkpoint lookup fixture SHA-256 was `1074c30d2d829dc4dd46b68f1fbdd1f66f7b4a87a65c40f9174aff806658f619`. The focused differential, full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff checks pass. Other DTO/source/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19. See [ADR-0040](../architecture/ADR-0040-lite-subscription-list-fastjson-input.md).

SOCKS Fastjson grammar continuation (2026-10-09): ten direct Java 5.5.1 constructor cases extend the parser corpus from 412 to 422 inputs. Java and native both reject unquoted values/keys, block comments, CR-delimited line comments, and repeated commas; both accept single-quoted strings, array trailing commas, and a line comment at EOF. No production change was needed. Oracle/test commit `a64e048`; `client-config.json` SHA-256 `0b7cbe9560fae03c4efabb9347ebbfc424ea27b70c873cdb2145c1c914cdaf75`. Focused differential, full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff checks pass. Remaining Fastjson grammar and Phase 6 reconciliation stay open; P6-V04 remains PENDING_PHASE_19. See [ADR-0038](../architecture/ADR-0038-fastjson-socks-escape-grammar.md).

Endpoint split continuation (2026-10-09): eleven direct Java 5.5.1 cases extend `getHostAndPort` coverage from 38 to 49 splits. Java splits at the final colon, accepts empty/whitespace host portions and several malformed bracket spellings when the resulting port parses, and reports split errors for empty/no-colon input; socket construction separately rejects empty or whitespace ports. Native `Address::parse` and `connection::split_address` now preserve that split behavior while retaining raw channel keys and unwrapping only a fully enclosing IPv6 bracket pair. Oracle/test commit `8c285a0`; implementation `be43200`; address fixture SHA-256 `2c95c3a72f496e24306bcdd5b347436b322cdf5d5796f5c416f11cc48f8e77f6`. Focused differential, full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff checks pass. Other endpoint/transport variants and full Phase 6 reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

LiteSubscription DTO duplicate-property correction (2026-10-09): ten pinned Java 5.5.1 cases cover every decoded field in the control body, nested `LiteSubscriptionDto`, and `OffsetOption`; Fastjson keeps the last value, including whole-list and nested-object replacement. Native derive rejected duplicate `action`; `5edf8e5` applies raw-object last-wins decoding while preserving enum/default behavior. Oracle/test `5e185cc`; lookup fixture SHA-256 `fa96140798a6a9342f19d3acd5aa703604c1ad512793ef029954fdd33f7afabe`. Focused red/green, full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest, fmt and diff checks pass. Other protocol DTO/source/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

Heartbeat DTO duplicate-property correction (2026-10-09): nineteen direct Java 5.5.1 cases cover every serializable field across `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData`; a separate direct getter case covers last-wins `filterClassSource`, which Java omits from JSON. Native derive rejected duplicates; `9d8b031` applies the shared raw-object decoder while retaining current defaults and coercions. Oracle/test `43bc1d6`; 71 heartbeat DTO cases, fixture SHA-256 `5597c2bb980302e0c4ce50a6fae1524b037b66b9dbb138f6af31721085a9929c`. Focused red/green, full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest, fmt and diff checks pass. Other API/source/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

BrokerData duplicate-property correction (2026-10-09): five direct pinned Java 5.5.1 inputs establish last-wins decoding for `cluster`, `brokerName`, `brokerAddrs`, `zoneName`, and `enableActingMaster`; the final broker-address map replaces the earlier map. The native derived decoder rejected duplicates; `6c66258` uses the shared raw-object decoder. Oracle/test commit `131bf44`; route DTO corpus has 109 inputs at SHA-256 `c2b85d5e374dd27f93653485824394c019f2ea2ce06e712acdde20fb18c24c01`. Focused red/green differential, full workspace/all-target tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest, fmt and diff checks pass. Other DTO/source/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

TopicQueueMappingInfo and TopicRouteData duplicate-property correction (2026-10-09): twelve pinned Java 5.5.1 inputs cover every decoded field across both DTOs and confirm last-wins replacement, including maps and lists. The native derived decoders rejected duplicate `topic`; `76e6ca8` applies the shared raw-object decoder while preserving each field's defaults and coercions. Oracle/test `5ff1d9b`; route DTO corpus has 121 inputs at SHA-256 `621d817bee937e0ddddca352747f79a783a1ba672e8e782a3fad7866b9602949`. Focused red/green differential, full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest, fmt and diff checks pass. Other DTO/source/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

QueueData duplicate-property correction (2026-10-09): five direct Java 5.5.1 route DTO inputs establish last-wins decoding for all `QueueData` fields (`brokerName`, queue counts, `perm`, and `topicSysFlag`), with scalar-to-string, decimal-to-i32, and null-to-zero coercions applied to the final token. Serde derive rejected duplicates; `1d146f6` now deserializes through the shared raw-object helper. Oracle/test commit `3fdf225`; the route DTO corpus has 104 cases at SHA-256 `967c44455135c142ebeb311aad00d612d868cd785f936d1362c79cf989e36699`. Red/green route differential, full all-target workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator self-test, fmt and diff checks pass. Other DTO/source/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

MessageQueue duplicate-property correction (2026-10-09): three direct Java 5.5.1 cases prove last-wins decoding for `topic`, `brokerName`, and `queueId`; the final decimal `queueId` still uses Java's existing narrowing behavior. A fourth case confirms this for nested `MessageQueue` inside `QueryAssignmentResponseBody`. Rust's derived decoder rejected duplicates; `29928ba` now uses the shared raw-object decoder while retaining nullable-string/i32 coercions. Direct MessageQueue inputs total 19 and assignment body inputs total 15. Oracle/test `8512e45`; lookup fixture SHA-256 `46a03b750d8cc28b1ef93fc126673262a15946055f8eba9f929838f58a3f3f29`. Red/green focused tests, full all-target workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator self-test, fmt and diff checks pass. Broader source/API/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

Assignment response duplicate-property correction (2026-10-09): two direct Java 5.5.1 inputs establish last-wins behavior for duplicate root `messageQueueAssignments` and nested `MessageQueueAssignment.mode` fields. Native derived decoding rejected duplicates; `c2051a8` routes both DTOs through the shared raw-object decoder while preserving existing field coercions. The assignment body fixture now has 14 cases. Oracle/test commits `d23cc55` and `0d3fa37`; fixture SHA-256 `c8d813d210219b68c3c48a121a1fa0eb903ebd31c10ea241bfe1b4ba6aafbb95`. Focused and full protocol DTO differentials, full workspace, strict Clippy, pinned offline Maven verify, comparator self-test, fmt and diff checks pass. Other DTO/input combinations and whole Phase 6 reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Assignment request duplicate-property correction (2026-10-09): four direct Java 5.5.1 inputs prove last-wins decoding for `QueryAssignmentRequestBody.messageModel` and `SetMessageRequestModeRequestBody.mode`, with enum names and ordinals in both orders. Rust's derived DTO decoders rejected duplicates; a shared raw-object deserializer now retains the final raw values before the existing field coercions. The request DTO fixtures now have 10 and 14 cases respectively. Oracle/test commit `5388a09`; implementation `07c952e`; lookup fixture SHA-256 `a6b63fd046c9b022bb41749af52dbce051a3662158dbfc33d980c4ec0b376cb8`. Full workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. Other DTO/input combinations and whole Phase 6 reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Consumer-list duplicate-property correction (2026-10-09): three direct Java 5.5.1 inputs prove that duplicate `consumerIdList` properties retain the final raw value, including scalar/array order changes. Rust's derived decoder rejected duplicates; the custom decoder now stores the last raw occurrence and applies the existing Fastjson-compatible coercion once. All 16 consumer-list DTO inputs match. Oracle/test commit `e03a999`; implementation `ea6d6de`; lookup fixture SHA-256 `943fc1e79fa2709fa4199ff8b173f4cd5625b67942c7fb1549078180682c27e1`. Full workspace, strict Clippy, offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. Other DTO/input combinations and whole Phase 6 reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Endpoint port input continuation (2026-10-09): fourteen direct Java 5.5.1 cases extend `NettyRemotingClient.getHostAndPort` coverage from 24 to 38 endpoint splits. Java accepts ASCII plus/minus, negative zero, zero padding, long zero prefixes, and BMP Arabic-Indic digits after `+`; it rejects surrounding/tab whitespace and Unicode plus/minus signs. `Integer.parseInt` accepts both signed i32 extrema, while `InetSocketAddress` rejects them as ports. Rust matches all 38 cases without a production change. Address fixture SHA-256 `07ac76295a0df3827be9fce5a4b703749bb0822338f5fbca5a8de6bb8adb4c15`. Focused differential, full workspace, strict Clippy, offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff checks pass. Other endpoint/transport variants and whole Phase 6 reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

SOCKS root-shape input audit (2026-10-09): ten new pinned Java 5.5.1 constructor cases extend the Fastjson corpus to 412 inputs. Java rejects root arrays, strings, booleans and numbers, a root array containing a route, and array/string/number/boolean route values; duplicate route keys succeed with the last route value retained. Rust matches all ten without a production change. Fixture SHA-256 `279d650d5d7fc57c3e3250b8bec36e199fcd48f8b01f5a0ebb1b52b8f0caeaca`. Focused differential, full workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff checks pass. Other Fastjson inputs and whole Phase 6 reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Consume-result and MessageQueue DTO inputs (2026-10-09): 19 direct Java 5.5.1 ConsumeMessageDirectlyResult cases cover all CMResult names and ordinals, fractions, numeric strings, unknown names, null, and rejected Boolean/out-of-range values; Rust now uses the raw-token enum decoder. Sixteen MessageQueue cases cover scalar string coercion, exact high-precision numbers, i32 boundaries, integer overflow, decimal wrapping, exponent strings, null/Boolean/empty-object conversion, and array rejection; the existing native decoder matched them. Lookup fixture SHA-256 910e5dd57337348c1ff9c5b466a0240ab57763cc3d10b25749e87ccd22c29dd1. Workspace tests, strict Clippy, offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. Broader source/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

Consumer enum input continuation (2026-10-09): 26 added direct Java 5.5.1 probes raise ConsumerData enum inputs from 39 to 65. They cover nulls, integer strings and their sign/zero-padding/overflow boundaries, nonnumeric JSON types, decimal precision beyond f64, exponent extremes, and numeric strings for all three enum fields. Rust now keeps raw JSON number tokens: plain decimals use exact Java intValue narrowing, exponent tokens use Java double-to-int behavior, and valid integer strings use ordinal lookup. The original fixture exposed the "1" mismatch; all 65 Java/Rust outcomes now match. Fixture SHA-256 6b2ca3ef46594d0f1dbf63587baab0c70a552dd777672b147acb250fd6b9b146. Full workspace tests, strict all-target/all-feature Clippy, offline Maven verify (22 scenarios), comparator self-test/manifest22, fmt and diff checks pass. Other source/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

Assignment request DTO enum audit (2026-10-09): eight direct Java cases cover QueryAssignmentRequestBody.messageModel, and twelve cover SetMessageRequestModeRequestBody.mode. Both accept enum names, ordinals, numeric strings, and fractional numeric tokens; unknown names/strings and explicit null become absent values, while out-of-range numeric ordinals reject. The setter DTO also defaults missing mode to PULL and missing popShareQueueNum to zero. Rust now uses the shared raw-token enum decoder and exposes the setter mode as Option<MessageRequestMode> to preserve Java's nullable value. Lookup fixture SHA-256 baac7eea9e0073bd3e22fc9e9b230b77565560500f967795406ab019e771e05f. Full workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. Whole source/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

Lite subscription DTO input audit (2026-10-09): 13 direct Java 5.5.1 cases cover LiteSubscriptionAction and OffsetOption.Type names, ordinals, numeric strings, fractional numbers, unknown names and out-of-range values. The oracle also records omitted primitive defaults and null-enum omission during encoding. Rust shares the raw-token enum decoder with heartbeat models, defaults absent version/value to zero, and omits a null offset type. Lookup fixture SHA-256 d653604bc9ece695d09aafdc5685598d9c278c612c7af9c7642c1f9924c010a4. Full workspace, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. Other source/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

Route numeric-map-key input audit (2026-10-09): twelve new direct Java 5.5.1 DTO cases cover string keys for `BrokerData.brokerAddrs` (`Long`) and `TopicQueueMappingInfo.currIdMap` (`Integer`). Java and Rust accept plus/zero-padded, fractional, and exponent forms, including maximum signed keys; both reject whitespace and overflow. No production change was needed. Oracle/test commit `7db339a`; the route DTO corpus now has 99 cases and fixture SHA-256 `06b96e4e50223a3b1d8633317ff309ecf759ef7e74e238a76670bcb5ad1d2e31`. Full workspace tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, fmt and diff checks pass. Other DTO/input and whole Phase 6 reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

Raw endpoint-key parity (2026-10-09): the pinned Java `channelTables` lookup uses the raw address argument. Four reflected key pairs prove that different hostname case, IPv6 hex case, and leading-zero port strings remain separate. Native endpoint normalization had collapsed them; oracle/test commit `f0f1d41` and fix `67a57ad` now preserve the raw connection key while dialing the validated parsed target. The `address.json` fixture has 42 classification addresses, 24 endpoint splits, four key pairs, and SHA-256 `b7112911b84f023d1b32408a01b4571c0086570a96857e2e24b12526b40de9f4`. A loopback test verifies two alternate spellings produce two channels. Workspace tests, strict Clippy, pinned offline Maven verify (22 scenarios), comparator/manifest22, fmt and diff pass. Broader Phase 6 API/source/input reconciliation remains open; P6-V04 stays PENDING_PHASE_19.

SOCKS document-whitespace boundary update (2026-10-09): the pinned Java constructor differential now covers 402 cases. Java accepts a leading BOM and rejects NBSP U+00A0, EM SPACE U+2003, IDEOGRAPHIC SPACE U+3000, U+2029, and trailing U+FEFF around an otherwise valid object; the native test failed on those five cases before implementation 9aa960c. Rust now rejects non-ASCII whitespace outside strings/comments and permits a BOM only at document byte zero. Oracle/test commit ab07387; current client-config.json fixture SHA-256 acc9fc36854200847ce88a3864d68136410387adcc45cc968947cc9040251ed9. Focused differential and all 67 remoting library tests passed; pinned offline Maven verify (22 scenarios), formatting, and diff checks passed. This closes only the recorded constructor whitespace boundary; broader Fastjson and Phase 6 reconciliation remain open.

Manager first-runtime correction (2026-10-09): direct pinned Java 5.5.1 manager invocation proves that an existing client ID returns the first `MQClientInstance`, without comparing later config or hook inputs. Changed NameServer, namespaceV2, TLS and timeout values do not replace the retained first config; the later caller's config remains unchanged. A follow-up passes distinct first/later `RPCHook` objects and inspects the actual remoting client's registered hook list: the first remains and the later one is absent. Native `p6_v02_same_client_id_reuses_first_java_manager_runtime` checks runtime identity and hook `Arc` identity against the Java fixture. Native red test `204de99` reproduced `ConfigurationConflict`; fix `b18771d` removes the conflict guard and captures environment properties/hook only on initial mapping. The expanded config fixture SHA-256 is `d4e01085e23e4e73f6192fbe84d64700a05eee28b4fe2523aa48b21a7edfc06e` (superseding the original config-only fixture hash). The focused manager test and pinned Maven verify (22 scenarios, including fixture drift) pass; full workspace, strict Clippy, comparator/manifest22, fmt and diff are part of the final gate rerun. This supersedes the former sharing-conflict policy in ADR-0004; whole Phase 6 source/API/input reconciliation remains open and P6-V04 stays PENDING_PHASE_19.

Inbound property-parser correction (2026-10-09): the pinned Java 5.5.1 `MessageDecoder.string2messageProperties` and native differential now cover nine wire-record inputs. Empty values are ignored, so a later empty duplicate does not overwrite a prior nonempty value. Probe/test commit `0041553` exposed the mismatch; fix `df63dae` matches the fixture. Fixture SHA-256 `534f1d4be1d82abd4c9d18c11265dae4445344cb434c7483f55874f3f7d25669`. Focused tests, full workspace, strict Clippy, Maven verify (22 scenarios), comparator self-test/manifest22, fmt and diff checks pass. Other inbound message/transport variants and full Phase 6 source/API/input reconciliation remain open; P6-V04 stays PENDING_PHASE_19.

Exact numeric coercion and DTO property-name continuation (2026-10-09): pinned Java fixtures cover 99 route DTO inputs, 20 assignment-mode inputs, 13 consumer-list inputs, seven assignment-attachment inputs, and 52 heartbeat inputs. Route and lookup probes prove that broker addresses, filter-server lists, consumer IDs, and assignment attachments retain exact high-precision plain decimal strings and Java exponent formatting (`1e2`→`100.0`). A decimal boolean probe confirms `1.0000000000000000001` decodes as `false` in both Java and Rust. Assignment-mode probes exposed f64 rounding before truncation; raw numeric tokens now use exact decimal truncation and Java's low-32-bit conversion for decimal ordinals, while out-of-range plain integer ordinals still reject. Seven route DTO cases across `QueueData`, `BrokerData`, `TopicQueueMappingInfo`, and `TopicRouteData` confirm uppercase, title-case, snake-case, and case-collision spellings are ignored by Java and Rust. Oracle/test commits `7985ef7` and `675b73b`; current route fixture SHA-256 `06b96e4e50223a3b1d8633317ff309ecf759ef7e74e238a76670bcb5ad1d2e31`. Earlier probe/test commits `1695b9f`, `8f7ad7f`, `a18ce02`, `beb6fe7`, and `21d5b70`; implementations `428dde9` and `dcc822b` (with earlier shared coercion fix `5e30b73`). Lookup fixture SHA-256 `42a05d175175b94dc7e1bfc051940f70b16a01cbba0345ff2a35a5a0fa2d57e1`, heartbeat `e183f318134bd5dfa85bcbd80276c3e85c444f3ca0ebbe8231696ef2b4305db7`. Full workspace tests, strict Clippy, pinned Maven verify, comparator self-test, formatting, and diff checks pass. Phase 6 remains open; P6-V04 stays PENDING_PHASE_19.

Endpoint Unicode-port continuation (2026-10-09): 22 direct Java 5.5.1 `NettyRemotingClient.getHostAndPort` cases now record the parsed `Integer.parseInt` value and unresolved socket-port acceptance. Arabic-Indic, fullwidth, mixed BMP, and Devanagari digits match Java; negative, >65535, i32 overflow, and supplementary mathematical digits reject. The new empty-host case :10911 resolves to Java's 127.0.0.1 loopback target, and Rust matches that resolution through the remoting client. Rust `Address::parse` and `connection::split_address` now share the Java UTF-16 decimal-port parser. Probe/test commit `c8d0171`; implementation `51cef1d`; address fixture SHA-256 `3f66b46e922d4c278510c8e2ea29ae26423cf30dec6cc2e5e304087260c37c71`. Full workspace, strict all-target/all-feature Clippy, pinned Maven verify (22 scenarios), comparator self-test/manifest22, fmt and diff pass. This closes the invoked split/port cases; full Phase 6 source/API/input reconciliation remains open and P6-V04 stays PENDING_PHASE_19.

SOCKS Unicode escape continuation (2026-10-08): the pinned Java 5.5.1 constructor oracle and native differential now cover 397 inputs: 126 prior cases, every escape follower in U+0000–U+00FF, and 15 additional Unicode/control-unit probes. Fastjson `\u` consumes four UTF-16 code units even when a supplementary scalar crosses that boundary; native parsing preserves a valid pair and maps only an unpaired unit to U+FFFD. The `char1` path masks non-ASCII UTF-16 units to seven bits, including supplementary followers. Current fixture SHA-256 `782e9c5ef3c8523d7de1ee0d61c9ad0cf16fc8036e65e6c296a26a1e6df824ab`; oracle/test commits `5f6e48c`, `823cc02`, `7cb2552`, and `4ae4dc9`; implementation `e59475f` and `77f6bd8`. See [ADR-0038](../architecture/ADR-0038-fastjson-socks-escape-grammar.md). Other grammar and full Phase 6 reconciliation remain open; P6-V04 remains PENDING_PHASE_19.

Nullable route decode update (2026-10-08): the route RPC API now exposes `Result<Option<TopicRouteData>, RpcError>` and decodes successful JSON `null` as `None`, matching Java's inherited `RemotingSerializable` behavior. The follow-up oracle directly invokes null decode/fromJson and decodeList cases on both `TopicRouteData` and `TopicQueueMappingInfo` (`c00e1a8`); Rust nullable adapter coverage is in `8420373`. Runtime route refresh returns false without replacing the cached snapshot; the all-topic null-key response is also a no-op and the topic loop continues (`95cda6c`). Missing bodies and error status responses keep their existing error paths. See [ADR-0039](../architecture/ADR-0039-nullable-route-decode.md). Fixture SHA-256 at this update: `55ff6cf477d53e78f5715294613e3a98d21796b729118a04feb999b213c17245`.

Route DTO integer-string input checkpoint (2026-10-09): pinned Java `MessageQueue.queueId` parsing and the native differential add seven cases. Java converts string values `+2`, `002`, `-1.9`, and `1e2` to 2, 2, -1, and 100, and rejects whitespace-padded, overflowing, and underflowing strings. Existing native Fastjson integer coercion matches all seven, so no production change was needed. Probe/test commit `6493e76`; this 66-input checkpoint had route fixture SHA-256 `3e96c4cbb68dcc8f83f5390106c4ce173afac0fc69826c81a6aa003a192d786a`.

Earlier ASCII SOCKS escape update (superseded by the Unicode continuation above): the pinned Java 5.5.1 constructor oracle now covers 254 inputs, including one input for each ASCII escape follower. Java accepts 12 additional identity escapes, octal `\1`–`\7`, and uppercase `\F` as form feed; native normalization now matches these values and outcomes, along with the prior malformed `\x`/`\u` cases. The constructor and reflected `proxyMap` differential passes; then-current fixture SHA-256 `34e3b6600611e97fe5ac5c3009bcd5d95e2c9489456ac9cc91e9b5f5a9ab2f25`. See [ADR-0038](../architecture/ADR-0038-fastjson-socks-escape-grammar.md). Remaining grammar and full Phase 6 reconciliation remain open; P6-V04 remains PENDING_PHASE_19.

Route DTO inherited serialization update (2026-10-08): `TopicRouteData` and `TopicQueueMappingInfo` inherit Fastjson2-backed `RemotingSerializable` methods. Rust exposes compact/pretty JSON and encode/decode/list/fromJson adapters, including nullable decode/list/fromJson forms. Six direct pinned Java cases cover default, explicit-null, and populated objects per DTO across compact/pretty JSON, instance/static encode (including the empty `JSONWriter.Feature[]` overload), decode, list decode, and fromJson; exact compact UTF-8 bytes match. Additional direct Java null utility calls cover both concrete DTO classes (`c00e1a8`), with native nullable adapter tests in `8420373`. The adapter sorts bean properties, emits numeric keys unquoted for `brokerAddrs`/`currIdMap`, and matches Fastjson2 tab indentation and empty-container formatting. Route RPC response decoding uses `TopicRouteData::java_decode_nullable`. Rust map key output is deterministic; Java `HashMap`/`ConcurrentHashMap` iteration order is unspecified. Oracle commit `c8bb22a`, implementation commit `4a271dd`; the initial serializer fixture hash `935426217262daeb92860ef9b90cc61b5cde8aa71d3042ca6f4713582568e2b3` is extended by null utility cases to the then-current SHA-256 `55ff6cf477d53e78f5715294613e3a98d21796b729118a04feb999b213c17245`. Full Phase 6 reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

VIP helper correction (2026-10-08): Rust `broker_vip_channel` now matches the pinned Java `MixAll.brokerVIPChannel` last-colon split, signed `Integer.parseInt` port handling, wrapping i32 subtraction, distinct `RuntimeError::InvalidBrokerAddress` and `RuntimeError::InvalidBrokerPort` failures for missing separators versus invalid ports, and disabled identity. Fifteen Java edge inputs cover empty/malformed hosts, negative and out-of-range ports, Unicode decimal digits, and parse errors; three existing cases cover hostname and IPv6 outputs. Socket validation remains in remoting. Current route fixture SHA-256 `ce2afec05859a761ce2fd82a9034ef147b333f077a999b03d17209a305254fb0`; see [the route audit](PHASE_6_ROUTE_AUDIT.md). Full Phase 6 source/API/branch reconciliation remains open.

Route DTO input continuation (2026-10-08): the pinned Java oracle and native differential now cover 59 inputs, including 12 rejection cases for JSON5-style syntax/malformed collection shapes, five Unicode decimal string/map-key rejections, nine numeric-token cases, and eight whitespace/BOM cases. Fastjson accepts leading-plus and zero-prefixed integer, decimal, and exponent values, form feed around JSON tokens, and a BOM only at document start; it rejects NaN, Infinity, hex, vertical tab, NBSP, and an embedded BOM. The Rust Java-compatible route parser normalizes only the tested numeric value tokens and form feed outside strings, and strips a single leading BOM. It matches all recorded outcomes. The route DTO parser still rejects the tested single quotes, comments, unquoted nonnumeric keys, trailing commas, suffixes, scalar filter lists, array-shaped maps, and Unicode numeric forms; SOCKS constructor parsing remains separately audited. Fixture SHA-256 at that update: `55ff6cf477d53e78f5715294613e3a98d21796b729118a04feb999b213c17245`; see [ADR-0014](../architecture/ADR-0014-route-dto-fastjson-input.md). Broader source/API/input reconciliation remains open.

Runtime config mutability update (2026-10-08): `ClientRuntime::config()` now returns a shared `SharedClientConfig` (`Arc<RwLock<ClientConfig>>`) corresponding to Java `MQClientInstance.getClientConfig()` returning its retained object. Manager-created runtimes still own a clone of caller input. Runtime API and namespace hooks share the handle for audited live reads; constructor-built transport/listener/executor resources keep their source-time settings. The Java oracle confirms getter identity and mutation visibility, and native tests cover live language, namespace, timeout, VIP and heartbeat settings. Current client-config fixture SHA-256 `ad49c9b917811d1b542bf7f8b2af9ffa07ba3cde22105a00a69bd02f2ade093a`; see [ADR-0037](../architecture/ADR-0037-live-runtime-client-config.md). Full Phase 6 reconciliation remains open; P6-V04 remains PENDING_PHASE_19.

MessageQueueRouteState public enum API update (2026-10-08): Rust now exports `remoting::MessageQueueRouteState` with Java's `Expired`, `ReadOnly`, `Normal`, and `WriteOnly` declaration order, enum methods, and typed lookup errors. Direct Java cases invoke all four values and null/unknown/empty/case-mismatched lookups. The pinned artifact scan found no call sites outside the enum, so no route-state transition behavior is claimed. Current `client-config.json` SHA-256 `be97b56070efbdc49ed2294b06f36f6848a152ddb310fba30d349a32a95b52f3`; see [ADR-0035](../architecture/ADR-0035-message-queue-route-state-enum-api.md). Full Phase 6 reconciliation remains open.

RequestSource public enum API update (2026-10-08): Rust now exports `remoting::RequestSource` with Java declaration order, enum methods and lookup errors, `get_value`, `is_valid`, `parse_integer`, and the exact `SYSTEM_PROPERTY_KEY` constant. Direct pinned Java cases cover 23 enum/integer inputs and the key. The artifact reference scan found no call sites outside the enum, so this maps its public surface without claiming that the constant activates property loading. Current `client-config.json` SHA-256 `1544344bd90fbefe577643e3ec7af1a8211dcc7bc780c2f39fef7e5ca9fcd391`; see [ADR-0034](../architecture/ADR-0034-request-source-java-enum-api.md). Full Phase 6 reconciliation remains open.

Netty event enum API update (2026-10-08): Rust now exports `client::NettyEventType` in Java order (`CONNECT`, `CLOSE`, `IDLE`, `EXCEPTION`, `ACTIVE`) with generated/inherited enum methods and typed string lookup; the channel event queue uses this public enum. Twelve direct Java method cases pass. Fixture SHA-256 at that update: `1fe6cf206b07e5f304634d7128ca4ca4c56a0114f010f21a0eeebb785529e3b5`. See [ADR-0033](../architecture/ADR-0033-netty-event-type-java-enum-api.md). Java `NettyEvent.getChannel()` returns a Netty `Channel`; the native callbacks do not expose a Tokio socket handle, so the event enum mapping does not assert carrier-object parity. Full Phase 6 reconciliation remains open.

Remoting command API update (2026-10-08): Rust now exposes `RemotingCommandType`, `RequestType`, and `RemotingCommand::get_type()` with Java declaration order, exact names, byte lookup behavior, and flag-derived type selection. The stream request hook obtains `ReqT` from `RequestType::Stream.get_code()`. Direct Java cases cover 22 enum, byte, and command-type outcomes. Fixture SHA-256 at that update: `6d02747d6a267631d2bf98d064967bead5a4a168411a28cb15f34bfa46d0c764`. See [ADR-0032](../architecture/ADR-0032-remoting-command-enum-java-api.md). Full Phase 6 reconciliation remains open.

TLS enum API update (2026-10-08): Java `TlsMode` values/order, generated/inherited methods, `getName()`, and exact fallback `parse(String)` are mapped to the public Rust enum and transport-selection path. Its fixture SHA-256 at that update was `948fcc7ca474d0ea48b39b1f60e075fa2213ff40290d51ec801c14af0e8fbad9`; see [ADR-0031](../architecture/ADR-0031-tls-mode-java-enum-api.md). TLS provisioning remains Phase 19 work.

Protocol enum API update (2026-10-08): `MessageRequestMode`, `LiteSubscriptionAction`, `OffsetOptionType`, `ConsumeMessageResult`/Java `CMResult`, `ConsumeType`, `MessageModel`, and `ConsumeFromWhere` now map Java enum values, order, names, text, comparisons, and lookup errors. `MessageModel`'s Rust ordering follows Java declaration ordinals. Direct Java enum cases cover 65 outcomes. Fixture SHA-256 at that update: `6de87735e30feb4fdca7c041d207326dd4718f0fe76b749c9c96b5e59214c747`. See [ADR-0030](../architecture/ADR-0030-protocol-enum-java-api.md). Full Phase 6 reconciliation remains open.

Client status enum API update (2026-10-08): Rust `PullStatus`, `PopStatus`, `AckStatus`, and `SendStatus` now expose Java's declaration order, `name`/`ordinal`/text/comparison methods, `values()` and `valueOf(String)` counterparts, and typed lookup errors. `PopStatus.values()` explicitly follows Java's `POLLING_FULL` before `POLLING_NOT_FOUND` order. Forty direct Java method cases pass. Fixture SHA-256 at that update: `de88ee1ad2ce408aee43b6614feecc3c9cf1c7f25426b89ddb13feff03a09f0e`. See [ADR-0029](../architecture/ADR-0029-client-status-java-enum-api.md). Full Phase 6 reconciliation remains open.

CompressionType public-surface correction (2026-10-08): Rust now exposes exactly Java's `LZ4`, `ZSTD`, and `ZLIB` enum values, with matching value/name/order/text/comparison methods and `getValue`/`getCompressionFlag`/`of`/`findByValue` counterparts. The flag helper maps code 0 to `ZLIB`, like Java, and returns a typed error for unsupported values 4–7. The direct Java fixture covers enum methods, Java trim boundaries, lookup errors, and `MessageSysFlag.getCompressionType`. Client-config fixture SHA-256: `ab635ab3ac993723c769fa19264fe63e593990da3655b9d1f26e0cad48dac3bf`. See [ADR-0028](../architecture/ADR-0028-compression-type-java-enum-api.md). Full Phase 6 reconciliation remains open.

SerializeType public-surface correction (2026-10-08): Rust now exposes only Java's `JSON` and `ROCKETMQ` variants. `SerializeType::from_java_byte(i8) -> Option<SerializeType>` maps Java's nullable `valueOf(byte)`; raw-code constructors and `Unknown(u8)` are removed. The frame decoder retains a typed unsupported-tag error when Java's lookup yields null. Direct Java enum method cases and native tests cover the remaining public enum surface. Full workspace, strict Clippy, offline Maven, formatter and diff checks pass. Fixture SHA-256 at that update: `6008a989876a0541a2d1ed8a036ca0dc37663a3ef0551400b76e5e5a480e95ae`. See [ADR-0027](../architecture/ADR-0027-serialize-type-java-enum-api.md).

LanguageCode public-surface correction (2026-10-08): the native enum now has exactly Java's 14 constants. `from_java_byte(i8) -> Option<LanguageCode>` represents Java's nullable `valueOf(byte)` result; the extra `Unknown` variant, `LanguageCode::from_code`/`java_byte` helpers, and duplicate `java_value_of_byte` method are removed. JSON and binary decoders keep mapping unknown input to `None`. The pinned fixture exercises all constants and signed unknown-byte cases. [ADR-0023](../architecture/ADR-0023-language-code-java-methods.md) records the API mapping. Full Phase 6 source/API reconciliation remains open.

LanguageCode generated enum method update (2026-10-08): the Java fixture now directly captures `values()`, `valueOf(String)`, `name()`, `ordinal()`, `toString()`, and `compareTo()` alongside the custom code lookups. Rust exposes counterparts for those Java methods: `values`, `value_of_name`, `name`, `ordinal`, `Display`, and `java_compare_to`. The custom Java `getCode(String)` lookup is named `get_code_by_name` and keeps its nullable result. Focused Rust tests and pinned offline Maven fixture verification passed. Fixture SHA-256 at that update: `00deca75109f3fa4b7a97314bdb9b567453e3ed14fe298201f7caca62411ef4d`. [ADR-0023](../architecture/ADR-0023-language-code-java-methods.md) records the complete enum surface and limits.

ServiceThread inherited API update (2026-10-08): actual Java 5.5.1 cases invoke `getJoinTime`, daemon/stopped accessors, `wakeup`, `makeStop`, and shutdown on both service classes. Unstarted instances record the 90,000ms join value, default flags, setter effect, no-op pre-start stopped state, and PullMessageService's executor shutdown. Controlled subclasses with a blocked no-op `run` also show that `start()` is idempotent, `makeStop()` sets stopped without joining, and `shutdown(true)` joins; they do not run the RocketMQ worker loops. Rust routes worker lifetime through `ClientRuntime` and owned Tokio tasks, so Java thread daemon/join flags and `makeStop` have no direct counterpart. [ADR-0026](../architecture/ADR-0026-service-thread-api-boundary.md) records the mapping and limits. Fixture SHA-256 at that update: `00deca75109f3fa4b7a97314bdb9b567453e3ed14fe298201f7caca62411ef4d`.

Runtime service-name update (2026-10-08): actual Java 5.5.1 calls to `RebalanceService.getServiceName()` and `PullMessageService.getServiceName()` return `RebalanceService` and `PullMessageService`. Rust exports matching `REBALANCE_SERVICE_NAME` and `PULL_MESSAGE_SERVICE_NAME` constants. Client-config fixture SHA-256 at that update: `57114f6198944de08fa6801207525abbbfd9e1532efe93fa02d2c8c64ab98fa4`; [ADR-0025](../architecture/ADR-0025-runtime-service-name-methods.md) records the boundary. This maps only the name methods; worker execution is audited separately below.

AccessChannel public-enum update (2026-10-08): the pinned Java 5.5.1 enum cases invoke generated `values()` and `valueOf(String)` plus inherited `name()`, `ordinal()`, `toString()`, and `compareTo()`. Rust now exposes declaration-order `values()`, names, ordinals, display text, ordinal comparison, and typed null/unknown lookup errors. The actual oracle covers both constants, defensive array copying, null, empty, lowercase, and unknown names. [ADR-0024](../architecture/ADR-0024-access-channel-enum-api.md) records the boundary. Client-config fixture SHA-256 at that update: `aa8e0e0e60096bdc7ffa9f7312331ef226e8254f9bd7aeccf9f9a85be1bd3d2f`.

Initial LanguageCode public-method update (superseded by the correction above; 2026-10-08): actual Java 5.5.1 calls cover `getCode()`, exact-name `getCode(String)`, custom `valueOf(byte)`, all 14 constants, unknown/negative byte values, null and case-mismatched names, plus `RemotingSerializable.decode` of an unknown JSON name. The initial Rust mapping kept an additional raw-code variant and helper; the correction above removes those extras while preserving the nullable Java lookup behavior. Client-config fixture SHA-256 at that update: `33441a65926214a006702b46369bfab8f44b35fc1951c1ab17a45166f0408fdf`.

Heartbeat enum method update: six actual Java 5.5.1 values cover `ConsumeType.getTypeCN()` and `MessageModel.getModeCN()` for every constant. Rust adds `type_cn()` and `mode_cn()`; both methods match the recorded Java strings. Fixture SHA-256 at that update: `7e9521037d5c7b9904ea25ae18ec9923bf6c42d3f9aa65ecf7abb80dd4388589`; [ADR-0022](../architecture/ADR-0022-heartbeat-enum-name-methods.md) records the mapping.

`MessageRequestMode.getName()` method update: two actual Java 5.5.1 enum cases verify the names of `PULL` and `POP`; Rust adds `MessageRequestMode::name()` with those values. Lookup fixture SHA-256 at that update `0c9ccc65b6c95d28d90d8de1bbac000436fe4468f1e7ebace6449ca1cf25c8e2`; [ADR-0021](../architecture/ADR-0021-request-mode-name-method.md) records the method mapping.

MessageQueueAssignment public-method update: two actual Java 5.5.1 cases verify the default/populated accessor values, exact `toString`, queue-only `equals`, null/wrong-type behavior, and the null-queue exception. Rust adds idiomatic getters/setters, Java-shaped `Display`/`java_to_string`, and `java_equals`; its lawful all-fields `PartialEq`/`Eq`/`Hash` implementation now snapshots the shared nested values. [ADR-0020](../architecture/ADR-0020-assignment-method-api.md) and [ADR-0066](../architecture/ADR-0066-shared-assignment-nested-aliases.md) record the method and alias boundaries.

Assignment response Set input update: the original 30 actual Java 5.5.1 `QueryAssignmentResponseBody.decode` cases cover missing/null/empty, object-to-singleton conversion, null members, identical-row deduplication, duplicate fields, scalar field rejection including fractional values, root array-member token kinds, and nested `MessageQueue` field token kinds. Java accepts null, empty-object, and populated-object queue values; it rejects string, number, Boolean, and array queue values. The Rust differential matches decoded accessor values and encoded shape. At this input checkpoint the native surface used `Option<HashSet<Option<MessageQueueAssignment>>>`; the current `QueryAssignmentResponseBody` uses the shared outer Set and each assignment wraps its nested queue and attachments in `SharedValue` handles. Checkpoint lookup fixture SHA-256 `9d4472bee58e739dbb0efa4c18a3e4405f7b58fd1e1975f862bbc431b61a016f`; [ADR-0019](../architecture/ADR-0019-assignment-response-set-input.md) records the Set input boundary. The corpus now has 33 cases, adding same-mode attachment-hash collisions, different attachment hashes, and duplicate null-queue rows. The Rust decoder matches stable same-mode Java bucket collisions and the null-queue `NullPointerException`; it retains lawful all-fields Rust `Eq`/`Hash` for native collections. A separate different-mode collision oracle still uses the invoking JVM’s enum identity hash and remains process-local.

Consumer-list response input update: 12 actual Java 5.5.1 cases cover missing/null/empty lists, duplicates and order, nullable/scalar/nested elements, and string/object/number/fraction/Boolean top-level values. Native DTO, API, and runtime types preserve Java's nullable list members; Fastjson scalar-to-string conversions and response encoding match the fixture. Lookup fixture SHA-256 at that update `0c9ccc65b6c95d28d90d8de1bbac000436fe4468f1e7ebace6449ca1cf25c8e2`; [ADR-0018](../architecture/ADR-0018-consumer-list-fastjson-input.md) records the boundary.

Assignment attachment input update: six actual Java 5.5.1 cases cover missing/null maps, scalar and nested values, null entries, and invalid top-level shapes. Native decoding preserves nullable strings and coerces non-null scalar values; re-encoding omits null map entries as Java does. Lookup fixture SHA-256 at that update `0c9ccc65b6c95d28d90d8de1bbac000436fe4468f1e7ebace6449ca1cf25c8e2`; [ADR-0017](../architecture/ADR-0017-assignment-attachment-fastjson-input.md) records the boundary.

Assignment response enum input update: 20 actual Java 5.5.1 `QueryAssignmentResponseBody.decode` cases cover `MessageQueueAssignment.mode` defaults, nulls, names, ordinals, integer numeric strings, numeric fractions, exact decimal truncation/wrapping, and rejection boundaries. Native `MessageQueueAssignment.mode` is nullable and defaults to `Some(Pull)`; decode and re-encode match the fixture. Current lookup fixture SHA-256 `42a05d175175b94dc7e1bfc051940f70b16a01cbba0345ff2a35a5a0fa2d57e1`; [ADR-0016](../architecture/ADR-0016-assignment-mode-fastjson-input.md) records the boundary.

Historical Route DTO input update: 37 actual Java 5.5.1 Fastjson cases cover scalar coercions for `QueueData`, `MessageQueue`, `BrokerData`, `TopicQueueMappingInfo`, and nested `TopicRouteData`, plus 12 syntax/shape rejections. Native decoding matches the recorded fields and accept/reject outcomes, including numeric strings/fractions, boolean coercion, numeric map keys, nullable addresses, filter-list elements, rejected JSON5-style syntax, and malformed collection shapes. Fixture SHA-256 at that update `ce2afec05859a761ce2fd82a9034ef147b333f077a999b03d17209a305254fb0`; later 59- and 66-case inputs are recorded above and in [ADR-0014](../architecture/ADR-0014-route-dto-fastjson-input.md). Other DTO inputs and full Phase 6 reconciliation remain open.

Latest SOCKS constructor and ClientConfig copy-field oracles: the pinned Java constructor grammar fixture has 492 curated cases, including one input per escape follower in U+0000–U+00FF, 15 Unicode/control-unit probes, five document-whitespace/BOM probes, ten root/route-value shapes, ten grammar edges, four nested bean-property cases, three EOF-comment boundaries, six rejected base-prefixed key forms, eight unquoted scalar-key probes, five numeric padding/suffix probes, four canonical numeric-key collision probes, five cross-type key probes, two rejected unsigned leading-dot keys, thirteen accepted signed/fraction/exponent/suffix key forms, nine rejected special/identifier route-key forms, seven exponent overflow/underflow forms, and four signed-zero edge/collision forms. Separate exhaustive scans cover every BMP code unit and all 1,048,576 supplementary scalar escape followers; Java accepts 253,952 supplementary scalars and native parsing matches all representable outcomes. Isolated Java surrogate units cannot be represented by Rust `String`. Java accepts the additional identity escapes, octal `\1`–`\7`, and `\F` form feed; native parsing matches all constructor outcomes and reflected values. At the root route map, Java accepts unquoted numeric, Boolean, and null keys, canonicalizes numbers to Java text, merges scalar keys with string keys of equal text, retains only the last colliding property, and keeps null distinct from string `"null"`; ordinary identifiers, unsigned leading-dot numbers, special/identifier tokens, and base-prefixed numeric keys reject. Rust preserves these distinctions, including Java's acceptance of `+.5`/`-.5`, rejection of `.5`/`.5F`, and sign retention only for negative nonzero exponent values that underflow to zero. EOF line comments accept a missing root close after a complete route object but reject when the route object or an array remains open. Probes cover Fastjson's seven-bit UTF-16 escape lookup, `\u` nibble positions, and supplementary scalars crossing the four-unit `\u` boundary: Java's resulting high/low pair is preserved, while a lone low surrogate maps to U+FFFD. The corpus also retains separate `\x` bounded-lookup and packed `\u` behavior, plus rejection of truncated escapes and line continuations. Java 5.5.1 `NettyRemotingClient.start/invokeSync` completes both username/password and no-auth local SOCKS5 round trips through scripted proxies; paired Rust transport tests cover both paths. The Java fixtures use explicit 64 KiB socket buffers because this JDK rejects the default zero send-buffer option before connection. The `copyFields` fixture snapshots all 33 non-internal `ClientConfig` fields on Java source, clone, and reset objects; native snapshots match all three. Current `client-config.json` SHA-256: `bf9b63dfb9c38a4f4090a4326a5d14bc9bff62478e4df4330825cb5009bc08f3`; earlier hashes are historical. See [ADR-0036](../architecture/ADR-0036-fastjson-surrogate-boundary.md) and [ADR-0038](../architecture/ADR-0038-fastjson-socks-escape-grammar.md). Broader Fastjson grammar and whole-phase source/API/input reconciliation remain open.

`ClientConfig.toString()` update: Rust `Display` now matches actual Java text for null/uninitialized and populated/initialized namespace states, raw quote handling, signed numeric boundaries, and every one of Java's 14 `LanguageCode` constants. The exact output includes Java's private `namespaceInitialized` flag and unredacted `socksProxyConfig`; Rust's separate `Debug` output remains redacted. Sixteen actual Java cases pass `client_config_to_string_matches_invoked_java`. The fixture SHA-256 at that update was `ff0edae74d25a91bd4a54ca2c01e06c12715d891fe7cbaff956a7a1bb06b2578`; the current client-config fixture hash is recorded in the opening update above. Broader public type/source reconciliation remains open.

`ClientConfig.queuesWithNamespace` and `MessageQueue` setters: six actual Java collection cases establish same-collection return, no iteration for null/empty namespace, null collection/row `NullPointerException`, partial mutation before a null row, and topic-only mutation. Rust now preserves the returned borrowed slice identity and null inputs, and exposes nullable `set_topic`/`set_broker_name` plus `set_queue_id`; both native differential tests pass. The fixture SHA-256 at that update was `4c0b831de2f3139b5be32e59fede60d639296e8dbffa20a9924c08fbabdab4bb`; see the accessor update below for the current fixture. [ADR-0011](../architecture/ADR-0011-message-queue-namespace-mutation.md) records the source contract and adaptation.

`ClientConfig` field accessor update: Rust now provides a getter/setter pair for every one of the 33 non-internal Java settings, retaining `Option` nullability and signed widths. The Java oracle calls every setter and getter on source, clone, and reset objects; `clone_and_reset_copy_every_public_setting_except_java_omissions` matches the direct Java getter snapshots and reflective field snapshots. Client-config fixture SHA-256 at that update: `2824c98496001ab46cc7137400359f51b2afe443d6fee779946606d78f379463`. [ADR-0012](../architecture/ADR-0012-client-config-accessors.md) records the mapping. Constructor inputs and settings activation remain under the broader Phase 6 audit.

`MessageQueue` method update: the pinned Java route fixture now invokes `equals`, `hashCode`, and `toString` on default, populated, and supplementary-Unicode values. Rust `PartialEq` matches same-type field equality, `java_hash_code()` matches Java's signed 32-bit formula, `Display`/`ToString` matches Java's null spelling and field order, and `Default` maps the Java no-argument constructor. The focused `message_queue_hash_equals_and_text_match_invoked_java` test passes. Route fixture SHA-256 `9b3f4be54464f7718632f7bcd7d5bde88559234f2c42141a15f9f48d48bc3a1d`; [ADR-0013](../architecture/ADR-0013-message-queue-java-methods.md) records the method mapping. Rust `Hash` remains its separate collection trait; Java `hashCode` callers use the explicit helper.

Current route DTO extension: `BrokerData.selectBrokerAddr` maps to `BrokerData::java_select_broker_addr`, covered by ten actual Java inputs for master selection, random value selection, null values/maps, and empty maps. Nullable queue/broker entries and nullable filter/mapping values are preserved in the native DTO model, with route refresh rejecting only null queue/broker/static-mapping values before cache publication. The public Java `TopicRouteData.topicRouteDataChanged` method maps to `java_topic_route_data_changed`, now shared by the route cache and covered by 11 invoked Java cases, including null-old and sort exceptions. `java_copy_constructed`, `clone_topic_route_data`, and `deep_clone_topic_route_data` cover the constructor and helper values/errors; shallow clone nested identity remains an owned-value adaptation, while live raw-table identity uses `TopicRouteDataHandle`. Route fixture SHA-256 `7c5f6b1c09937d21a63252e9c1359485ebde19b68284f63de1215781185ad6db`; focused protocol/core regressions pass. Whole-phase source/API/input reconciliation remains open.

Historical TopicRouteData clone-method checkpoint (superseded by `8b69c09e` and `a87e3726`): the original eight Java cases established copied values and null-triggered failures, while Rust then returned owned nested values. The subsequent Java copy and actual refresh oracles (`d437ab34`, `827f6510`) established shallow element identity; Rust now retains those shared nested handles in Java-compatible copies and raw-to-published route copies. Deep clone remains independent. The earlier route fixture SHA-256 was `08038bfb606aaa057869cb3e41256a410ec03d8c4f85003aa32055be69ea21a2`.

Historical SOCKS numeric/scalar extension `457d16b`/`37af420`/`cc6422e`/`09d0878`/`6c4d59b` expanded the pinned Java 5.5.1 constructor/parser differential to 73 `socksConstruction` cases. Twelve additional actual runtime inputs cover exponent thresholds, signed/unsigned leading-dot syntax, decimal scale, overflow, and underflow. Java formats `1e-4` as `1.0E-4`, `1e-7` as `1.0E-7`, `1e7` as `1.0E7`, keeps `1e6` as `1000000.0`, preserves `1.2300`, and rejects unsigned `.5e-3` while accepting signed forms. Native coercion matched all 73 constructor and reflected proxy-map outcomes, including exact decimal text, signed zero and infinity. Its fixture hash `a1054ec9af61312d25037da4470d17629babee026d4e6da6371cb9555b81e175` is superseded by the latest oracle above. Broader grammar and full Phase 6 source/API/input reconciliation remain open.

Route DTO method updates cover BrokerData (`04848b0` through `e6a3389`), QueueData (`329b0f9`/`b04b2d9`/`1df78b5`/`f6add71`), TopicQueueMappingInfo (`18f8565`/`af642aa`/`38ec7a8`), and TopicRouteData (`f9f6450`/`c7d8c01`/`21cc626`). The Java oracles capture these DTOs' equality/hash/comparison/text behavior and six `TopicRouteData.topicRouteDataChanged` sort outcomes. Rust implements the verified methods and uses both sort DTO comparators; static mapping `toString` uses deterministic currIdMap key order while Java's ConcurrentHashMap order is unspecified. Six actual `currIdMap` input cases also cover null/malformed key/value rejection and signed Integer boundaries. Current route fixture SHA-256 `962d63f6f615b552410a5717298cd2f8e10e67be3e26ad897633ce231db6020e`; full workspace/all-target tests, strict all-target/all-feature Clippy, offline Maven verify, comparator/manifest22, fmt and diff pass. Other route DTO methods/input combinations and broader source/API reconciliation remain open.

Route DTO text update `07e62e8`/`449e97e`/`1d6f799` adds actual Java `BrokerData.toString` and `TopicRouteData.toString` cases. Native output matches defaults, explicit null collections and populated nested route values; maps use deterministic Rust key order because Java `HashMap` iteration order is unspecified. Route fixture SHA-256 `d26481b826d4323e5e1f017108de2772051b0caf968257642a101a4f48537fef`; full workspace/all-target tests, strict Clippy, pinned Maven verify, comparator/manifest22, fmt and diff pass. Other DTO/input reconciliation remains open.

SOCKS proxy-selection update `47f3fa0`/`8bf543b` invokes private Java `NettyRemotingClient.getProxy` on eight actual runtime cases: subnet hit/miss, default-route hostname and IPv6, masked `/33`, null route, malformed CIDR and empty map. Native selection matches proxy-address/no-proxy/error outcomes across all eight; no production change was needed. Its then-current client-config fixture SHA-256 `46762834bf876a467f64bbb01e19327f765fcbb21e4c110189c1173a0f14cdf3` is superseded by the later parser fixture above. Focused remoting oracle tests pass. The later authenticated/no-auth Java SOCKS5 round trips close the handshake item; broader parser grammar remains open.

SOCKS CIDR update `1e88146`/`d77c43c`/`c57c8ce` directly invokes pinned Java `RemotingHelper.ipInCIDR` for 12 host/CIDR pairs. Rust now follows Java's `ipToInt` behavior of parsing the first four dot-separated fields and ignoring later fields, while preserving signed octets, Java shift masking, extra slash suffixes and error outcomes. The native regression failed on a five-component address before the fix; all 12 match now. Its then-current client-config fixture SHA-256 `07e8ca582ea56b813a69d3cbedd6031d93f8b74f6dbec245e6d4d7670a3df3c9` is superseded by subsequent parser fixtures above. Full workspace/all-target tests, exact all-feature Clippy, pinned Maven, fmt/diff, comparator and manifest22 pass. The end-to-end Java SOCKS5 path is covered by the current round-trip oracle; other transport/address variants remain open.

SOCKS Fastjson grammar update `bc5a10d`/`21c9fcb`/`3bf035d` invokes additional Java 5.5.1 constructor/parser inputs. Java rejects unquoted identifier values, hexadecimal numbers, `undefined`, `NaN` and `new Date(...)`; accepts duplicate proxy fields with the last value, line comments, and `\x` string escapes. Native parsing matches the observed constructor and parsed-value outcomes across all 33 `socksConstruction` cases. Its then-current client-config fixture SHA-256 `1c3c4a9f023952b5d18b3d503c1441f9d6af70a5ccf397680c758e23b1e8f78c` is superseded by the later parser fixture above. Additional Fastjson grammar remains under audit; later Java SOCKS5 round trips cover the connection handshake.

FactoryTable current follow-up `8153350`/`53a386f`/`3d247a3`/`cdc6dcd` invokes actual Java iterator removal on keySet, values and entrySet, and all null-result branches for `computeIfAbsent`, `computeIfPresent`, absent `compute`, and `merge` removal. Native live-view `remove` methods match all three iterator-removal effects and the null remappings; 48 `managerTable` outcomes pass at client-config fixture SHA-256 `27ef695764fb71aa7bcb707279f4bc4e992f3b8fde61223ff58d3effd977272f`. Full workspace, exact all-feature Clippy, pinned Maven, fmt/diff, comparator and manifest22 pass. Detached snapshots at traversal start provide one permitted `ConcurrentHashMap` weak-iterator outcome: the Java contract requires items present at iterator construction, permits but does not require later changes, and forbids fail-fast exceptions ([Java SE 8 `ConcurrentHashMap`](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentHashMap.html)). Broad source/input audit remains open.

Registry operations `5881829`/`73f1329`/`0dc4928`/`df2230f` invoke Java 5.5.1 `getOrDefault`, `putAll`, `computeIfAbsent`, `computeIfPresent`, `compute`, `merge`, `replaceAll`, `forEach`, `containsValue`, `keySet` and `values` on producer and consumer tables. Rust exposes matching get/put-all and per-key remapping methods plus detached key/value/entry snapshots. Compute-family callbacks run outside the entries mutex under a weakly retained exact-key gate; same-key table writes and producer/consumer registration or unregister wait, so eligible callbacks run once. `replaceAll` callbacks run before taking that gate; under the gate, Rust compares the callback's observed owner with the latest mapping using Java owner equality, then commits by identity against that stable mapping. An unequal value causes a callback retry, while a distinct equal value does not; equality keys mutated during the callback are sampled at replacement time. Java/native races on both tables verify compute serialization and `replaceAll` writer completion, equality-sensitive retry input/count, previous value and final mapping (ADR-0081). The updated Java fixture covers distinct equal owners and mutable-key owners on both tables. Client-config fixture SHA-256 `5f6a28c0199a64a905a5f642501745ce98eae57f55e19b84f38c0c1d0c55e890`. Live view handles expose contains/length/emptiness/remove/clear, predicate `remove_if`/`retain`, and collection-based `remove_all`/`retain_all`, plus detached snapshots. Iterator-removal effects are represented through live-view `remove`; snapshots taken when traversal starts map to an allowed weak-iterator outcome under the Java `ConcurrentHashMap` contract.

Custom registry value equality `4616808`/`b9f5bb6`/`0dc4928`/`0113ced`: actual Java 5.5.1 conditional `ConcurrentMap.replace/remove(key, value)`, `containsValue`, values-view removal and entry-view contains/removal succeed for distinct producer and consumer proxies whose `equals` keys match. Rust owner traits use optional `map_value_equality_key`; `_if_equal` and live views sample that key outside locks, while `_if_same` preserves explicit Arc identity. Client-config fixture SHA-256 `d0c7f7bec5dd54c6570419099cc4d9ba7dd2d94a53baa5a84cdabcc43e8ae01d`; this baseline preceded the mutable-key extension below.

Mutable registry owner equality `c47d07b`/`4659e00`/`d4d8fdc`/`f3f1bc1`: Java 5.5.1 mutates owner equality keys and verifies containsValue changes with the live keys and conditional replace rejects a stale expected key then accepts the current one. Native comparisons sample current keys outside registry locks; replace/remove compare the observed owner and apply an identity-conditional update with retries if the map changed. A corrected regression fails against the prior insertion-time cache and passes with the fix. Client-config fixture SHA-256 `6bfe7bb6ce6bd07bb75fb8a817524603dbfbc5f83e0c9ac7db6808f48cb4bb64`. Full workspace tests, strict all-target/all-feature Clippy, pinned Maven (73 config cases, manifest22), differential comparator, fmt and diff pass. Full Phase 6 source/input reconciliation remains open.

Client check null-topic input update `d591091`: actual Java 5.5.1 `checkClientInBroker()` throws before lookup/RPC for a non-TAG subscription with null topic. Native returns typed `InvalidRoute` without network access; the invoked result is asserted in `tests/client_check.rs`. Fixture SHA-256 `73990a351770ca0d8451590eef16bc3931115f90853193f37f3e4e18d61c841d`; pinned Maven and focused regression pass.

Public heartbeat DTO API update `91702a2`/`3c4f444`: Java `HeartbeatData.computeHeartbeatFingerprint()` maps to `HeartbeatData::compute_heartbeat_fingerprint() -> Result<i32, HeartbeatFingerprintError>`. Seven actual Java runtime fingerprint cases match, and runtime uses this method instead of a private duplicate implementation. Null consumer and nested subscription members map Java `NullPointerException` to explicit error variants. Fixture SHA-256 `118b49612461931280cf65e7912ecceb7deb1d30869a82e7e2d27b71b8afc53c`; workspace, Maven and strict Clippy pass. Other source/API reconciliation remains open.

MessageQueue comparison `c2c34e3`/`b001f65` adds `java_compare_to`, a fallible public method matching eleven invoked Java 5.5.1 cases: null topic/broker failure, UTF-16 string difference, and signed `Integer.compare` queue ID result. Internal Rust `Ord` remains a total order for tree collections. Pinned tag commit `efa1be59` and the `rocketmq-common:5.5.1` bytecode both use `Integer.compare`. A previous claim of source/artifact divergence came from reading the separately changing Java worktree at 5.3.3 HEAD; it is superseded here. Route fixture SHA-256 `a709df838628378dbca6044027cd873fd0b6a80c90591a24bf395207228c3269`; full workspace, pinned Maven and strict Clippy pass. Java equality/hash/text methods are covered by the newer oracle above; broader source/input audit remains open.

SOCKS private route-field coercion `34ce95a`/`b3ada16` matches six more invoked Java constructor cases. Reflected `NettyRemotingClient.proxyMap` turns numeric and Boolean `addr`/credential values into strings, leaves null address null, and converts an empty array address to `"[]"`. The native parser now retains these values with a private JSON-value conversion; the existing public proxy configuration is unchanged. Config fixture SHA-256 `eefbb3c681864effb187cbabcef7e70750b079876d5e45e138d83f6aa6a5264d`; focused red/green, full workspace, strict Clippy and pinned Maven verify pass. This evidence covers these six fields and inputs; remaining Fastjson grammar and connection behavior require audit. Earlier config hashes below are historical.

Public MessageQueue DTO correction `3577198`/`e242764` follows actual Java default, explicit-null and populated decode/JSON cases: topic and brokerName are nullable and omitted when null, while absent queueId defaults to zero. Native public getters and JSON now retain these states; namespace queue and offset transformations carry null topic/broker values. Route fixture SHA-256 `e5520a19c71483fce328312f9d4263c6af7a0bfff4c712e40660ca90626c3453`; focused red/green, full workspace, pinned Maven and strict Clippy pass. The null `compareTo` outcomes are covered by the later 11-case `java_compare_to` oracle above; broader source/input reconciliation remains open.

Nullable command language is now implemented (`40bd7cf`/`63b7feb`): public `ClientConfig.language` and `RemotingCommand.language` retain None; JSON omission decodes as JAVA while explicit null decodes as null, binary null encode returns a typed error, and heartbeat V1/V2 use the configured value. Config copy/reset/runtime and scripted before/after hook/wire checks pass. Config fixture SHA-256 at that update was `9b7a324ef971a1ea4dc3642dd54f1a3bd432684c8ef5dac03de89a827a52d6f4`; full workspace/exact Clippy/Maven/fmt/diff/comparator gates passed. Historical missing/review-required language statements below are superseded; full API/source/input audit remains open.

Nullable Broker values are represented in public `BrokerData.broker_addrs` and `RouteSnapshot.broker_addresses` by `Option<String>` map values (`8fb1ec8`); nullable broker IDs are now `Option<i64>`. The actual Java null-value route golden is enabled and passes, with source-backed publish/admin/subscribe/topic/cleanup/heartbeat behavior. The route DTO differential additionally proves null Long keys are retained separately from numeric IDs, while null Integer keys reject. Older nullable-Broker ignored/missing statements below are historical; nullable command language is also implemented, while other input surfaces remain incomplete.

SOCKS grammar extension `0c61f34`/`7899cea` invokes actual `NettyRemotingClient.loadSocksProxyJson` constructor outcomes. Config fixture SHA-256 at that update was `27caa16b8d8701c5a10153f2592ca3ffffaef72482957b80e433a424e10be7e2`: single quotes, trailing comma, comment, unknown proxy field, missing address and null proxy entry succeed; unquoted CIDR key fails. Native JSON5 parsing now matches these 19 constructor cases. Private route state treats a null value as direct and defers missing-address failure until selection, without a sentinel or public `SocksProxyConfig` change. Full workspace, exact all-feature Clippy, fmt/diff, comparator/manifest and pinned Maven verify exit0. Remaining Fastjson inputs and connection failures still require audit; older config hashes below are historical.

Pinned client source: `rocketmq-all-5.5.1`, commit `efa1be59d6fc747ad220f036c86030f05f0419f9`. Java paths below are relative to `client/src/main/java/org/apache/rocketmq/client/`. This supplements the [runtime evidence inventory](PHASE_6_RUNTIME_INVENTORY.md); it is not complete phase acceptance or a live-server compatibility claim.

### Address conversion helpers and phase ownership

The pinned common `NetworkUtil` also exposes `string2SocketAddress`, `socketAddress2String`, and `convert2IpString` (lines 182–199). Their client call sites belong to later owners: `ClientRemotingProcessor.receiveReplyMessage` converts reply-header born/store host strings at lines 241/245 and is assigned to Phase 15's PUSH_REPLY_MESSAGE completion; `MQAdminImpl.viewMessage` formats the decoded MessageId address at line 275 and belongs to Phase 17's admin/query API. Phase 6 owns only the inbound correlation dispatch boundary for reply pushes, not address materialization or MessageId decoding. Phase 15 now parses reply hosts into resolved or unresolved `MessageHostAddress` values with Java integer-port syntax and range behavior; the invoked Java fixture and Rust regression cover IPv4, unbracketed IPv6, Unicode decimal ports, boundary ports, and malformed inputs. Rust `MessageExt` host getters/setters now use the typed address value, exposes the Java host-string and host-byte methods, and consumer send-back formats resolved addresses through the Java `socketAddress2String` mapping. The message API fixture also covers transient topic/group sysflags, Unicode `Integer.valueOf` inputs, V6 flag setters, topic-filter selection, and diagnostic `toString()` fields. For messages with multiple properties, the Rust string uses stable map order because Java `HashMap` order is unspecified. `convert2IpString` has no call site in the pinned `rocketmq-client` module; its pinned common tests cover numeric loopback and `localhost`, and tools/proxy/store call sites are outside the client-only contract. The endpoint fixture's 49 cases cover `NettyRemotingClient.getHostAndPort`, a separate split-without-resolution method; two native differential tests additionally verify every legal port `0..=65535` and all 370 BMP UTF-16 code units accepted as single decimal digits by the recorded Maven oracle runtime (Java 27, Homebrew).

## ClientConfig: forward and reverse field inventory

Every row maps the Java getter/setter pair to a Rust field. A reverse scan of `ClientConfig` confirms all 33 public Rust fields correspond one-to-one with the 33 public field rows; `namespace_initialized` is the sole private Rust field and maps Java's private `namespaceInitialized` cache. Java plain setters do not validate; Rust retains signed widths and validates at the owning operation. Strings, including SOCKS, are nullable `Option<String>`; AccessChannel and LanguageCode are also nullable. Namespace and NameServer transitions require the named Rust methods, not direct field assignment. `namespaceInitialized` remains private.

Copy column: **C** means both `cloneClientConfig` and `resetClientConfig` copy the field; **O** means Java omits it (clone constructor default, reset receiver retained). Manager column: **K** means the value belongs to the first runtime's retained construction/behavior snapshot for that client ID; later same-ID calls do not compare or replace it. **ID** marks a client-ID component; **owner** means retained for its owning component, not promoted to a runtime construction key. `MQClientManager` is not a global configuration compatibility checker.

| Java field | Rust field | Type / default / input | Copy | Manager | Actual use / remaining activation owner |
| --- | --- | --- | --- | --- | --- |
| namesrvAddr | namesrv_addr | nullable String; system property `rocketmq.namesrv.addr` then Java environment fallback `NAMESRV_ADDR`, including explicit blank | C | K | construction seeds the active remoting NameServer list once from the normalized non-null value; later config mutation leaves that list unchanged. Direct `nameServerAddressSnapshotCases` compares the active list before/after mutation. Forked Java `MQClientInstance.start()` plus Rust differentials cover three post-construction mutations: null at construction→configured before start skips discovery and keeps the active list absent; configured at construction→null before start fetches discovery once while retaining the configured active list; changing a configured address to the empty string also skips discovery and retains the constructor-time active list. Native counting providers match all three observed discovery-call counts; `empty_nameserver_start_schedule_count_matches_java_probe` also matches the four scheduled registrations recorded for the empty-string case |
| clientIP | client_ip | nullable String; automatic local-address selection | C | ID | Java `NetworkUtil`/no-arg `ClientConfig` fixture and `local_address_selection_matches_invoked_java_candidates_and_ipv6_fallback` match both IPv4 and IPv6 candidate order on macOS, the selected address, and the observed IPv6-only subset; the Java `java.net.preferIPv4Stack` startup property maps to the same-named Rust environment key, is snapshotted once, and filters IPv6 candidates and hostname fallback; named IPv6 scope formatting matches the invoked Java interface fixture; pinned Linux Java 5.5.1 captured `lo ::1%lo` with scope ID 1 and its `/proc/net/if_inet6` row, which the Rust Linux row parser and normalizer match (`address-linux.json`, SHA-256 `3f064eed94e36753c4de9f4a9025f5cf1cd4c75041dfa5cd654c0171064a0a4b`); the later Linux ARM64 production run passed all 130 core-library tests, including `linux_production_enumerator_matches_invoked_java_loopback_scope` and the automatic `ClientConfig::new_auto()` resolver test; this closes the recorded Linux host/fixture only. Windows uses source-mapped `GetIfTable2`, unicast/anycast address tables, Preferred/Deprecated DAD filtering, Java list order and separate `GetIfEntry2` state checks; `windows_interface_candidate_pipeline_joins_filters_and_orders_tables` exercises the combined table projection/join/filter/order pipeline with synthetic rows, and an isolated Windows-target module/test type-check passes, but runtime enumeration remains unverified; explicit `new(ip)` represents setter; Windows Rust runtime enumeration and BSD target execution remain under reconciliation; the Linux ARM64 IPv6-only Docker resolver test passes with Docker IPv4 disabled, and same-network-namespace Java/Rust selection now matches `[fd42:6:0:0:0:0:0:2%eth0]` for that container topology, and the complete ordered candidate list also matches in same-namespace pinned Java/Rust probes on a default bridge (3 candidates) and an IPv6-only Docker network (4 candidates); these container results do not prove behavior on a real IPv6-only host; other Linux interface/topology shapes are not covered by the recorded runtime fixture; bytecode and an ordered regression cover multiple external IPv6 selection |
| instanceName | instance_name | nullable String; `rocketmq.client.name` or DEFAULT | C | ID | identity/PID rename; facade startup invocation Phase 7/consumer owners |
| clientCallbackExecutorThreads | client_callback_executor_threads | i32; available processors | C | K | all nonpositive values use effective callback capacity 4; raw signed setting/sharing key retained; six Java executor observations include `Integer.MAX_VALUE` core capacity with zero active threads at construction; a post-construction 1→2 mutation leaves Netty config and executor at 1, and Rust callbacks remain capped at the captured capacity; the Rust dispatcher starts callback jobs on demand, including for the maximum accepted capacity |
| namespace | namespace | nullable String; null | C | owner | lazy endpoint/resource/queue helpers; facade and decoded-message use owning phases/16 |
| namespaceInitialized | private namespace_initialized | bool; false | O | private | setter/getter cache; clone resets, reset preserves receiver |
| namespaceV2 | namespace_v2 | nullable String; null | C | K | NamespaceRpcHook `nsd`/`ns` |
| accessChannel | access_channel | nullable Option; Some(Local), Java LOCAL | O | owner | clone resets toSome(Local), reset retains nullable receiver; trace/hook context Phase16 |
| pollNameServerInterval | poll_name_server_interval | i32 milliseconds; 30000 | C | K | Java scheduler oracle confirms 10ms initial delay and configured fixed-rate period, then proves `startScheduledTask()` reads a post-construction 71ms mutation; positive period required |
| heartbeatBrokerInterval | heartbeat_broker_interval | i32 milliseconds; 30000 | C | K | Java scheduler oracle confirms 1000ms initial delay and configured fixed-rate period, then proves `startScheduledTask()` reads a post-construction 83ms mutation; positive period required (Java comment says microseconds, actual scheduler uses milliseconds) |
| persistConsumerOffsetInterval | persist_consumer_offset_interval | i32 milliseconds; 5000 | C | K | Java scheduler oracle confirms 10000ms initial delay and configured fixed-rate period, then proves `startScheduledTask()` reads a post-construction 97ms mutation; paused-time regression verifies absent capability skipping and successive 10s/15s ticks; actual offset stores Phase 9 |
| pullTimeDelayMillsWhenException | pull_time_delay_millis_when_exception | i64 milliseconds; 1000 | C | owner | push exception retry Phase 10; not a new runtime timer |
| traceMsgBatchNum | trace_msg_batch_num | i32; 10 | O | owner | AsyncTraceDispatcher batching Phase 16 |
| unitMode | unit_mode | bool; false | C | owner | producer/consumer request owners; heartbeat uses each registered consumer's data, not runtime config |
| unitName | unit_name | nullable String; null | C | ID | Java whitespace-sensitive client-ID suffix |
| decodeReadBody | decode_read_body | bool; `com.rocketmq.read.body`, true | C | owner | single message decoder Phase 8 and POP/admin decode owners |
| decodeDecompressBody | decode_decompress_body | bool; `com.rocketmq.decompress.body`, true | C | owner | single message decoder Phase 8 and POP/admin decode owners |
| vipChannelEnabled | vip_channel_enabled | bool; `com.rocketmq.sendMessageWithVIPChannel`, false | C | owner | source-selected Broker RPC VIP conversion; direct Java runtime/API fixtures and native loopback differentials show check-client, consumer-list, and assignment calls re-read the retained setting after construction-time mutation (`10911→10909`); heartbeat sends keep the supplied Broker address unchanged even when this setting is enabled, as shown by the invoked Java API capture and Rust loopback |
| useHeartbeatV2 | use_heartbeat_v2 | bool; `com.rocketmq.heartbeat.v2`, false | C | K | V2 negotiation/full/simple/fingerprint behavior; both actual all-broker wrappers select V2 after mutation even while concurrent V1 remains enabled; ordinary WithLock V1 uses its executor, WithLockV2 V1 stays serial, and both V2 calls run on the caller; native wrapper wire fingerprint differential matches |
| useTLS | use_tls | bool; `tls.enable`, default false | C | K | `ClientConfig.useTLS` snapshots `TlsSystemConfig.tlsEnable` at first class initialization; later property changes do not affect new configs. It is separate from standalone `NettyClientConfig`'s `tls.server.mode` default; `MQClientInstance` copies this value into Netty config; provisioning/live acceptance Phase 19 |
| socksProxyConfig | socks_proxy_config | nullable String; `com.rocketmq.socks.proxy.config`, Some(`{}`) | C | K | null and exact empty retained in core, bridged to lower null/no-proxy parsing; whitespace-only constructor rejection golden; SOCKS transport parsing/connect |
| mqClientApiTimeout | mq_client_api_timeout | i32 milliseconds; 3000 | C | K | fresh per-runtime RPC budget; direct Java runtime fixture records 3000 then 150 after retained-config mutation; Rust delayed loopback confirms the post-construction 150ms timeout |
| detectTimeout | detect_timeout | i32 milliseconds; 200 | C | owner | producer fault detector Phase 7 |
| detectInterval | detect_interval | i32 milliseconds; 2000 | C | owner | producer fault detector Phase 7 |
| language | language | nullable LanguageCode; default JAVA | C | K | heartbeat V1/V2 use config; null JSON omits key and binary encode errors; ADR0007 implemented; CPP admin reset override owning admin phase |
| enableStreamRequestType | enable_stream_request_type | bool; false | C | ID + K | STREAM suffix and pre-user `ReqT` hook from `RequestType::Stream.get_code()` (`0`) |
| sendLatencyEnable | send_latency_enable | bool; `com.rocketmq.sendLatencyEnable`, false | C | owner | producer fault-tolerance selection Phase 7 |
| startDetectorEnable | start_detector_enable | bool; `com.rocketmq.startDetectorEnable`, false | C | owner | producer fault detector Phase 7 |
| enableHeartbeatChannelEventListener | enable_heartbeat_channel_event_listener | bool; true | C | K | sampled at `MQClientInstance` construction to create or omit the weak active-channel listener; later `ClientConfig` mutation does not replace it; successful heartbeat wakes all consumers |
| enableConcurrentHeartbeat | enable_concurrent_heartbeat | bool; false | C | K | wrapper reads the retained setting live, while executor presence/width are captured at construction; `false→true` with no executor returns `false`, and `true→false` uses serial V1 while retaining the executor; both actual Java fixture cases and paired Rust loopback tests are in `heartbeat.rs` |
| concurrentHeartbeatThreadPoolSize | concurrent_heartbeat_thread_pool_size | i32; available processors | C | K | constructor creates a fixed executor; changing retained config 2→1 leaves Java core size 2, and the native parallel-peer regression confirms the original width remains active |
| enableTrace | enable_trace | bool; false | C | owner | trace lifecycle/hooks Phase 16 |
| traceTopic | trace_topic | nullable String; null | C | owner | trace topic Phase 16 |
| maxPageSizeInGetMetadata | max_page_size_in_get_metadata | i32; 2000 | O | owner | metadata pagination in MQClientAPIImpl Phase 17 |

Property booleans follow `Boolean.parseBoolean`: case-insensitive exact `true`, no trimming; malformed values are false. Exact Java property keys are read from the Rust process environment (no JVM System property store). Direct `ClientConfig` properties are constructor inputs, except `tls.enable`, which Rust caches on the first `ClientConfig::new` call to match Java's `TlsSystemConfig` class-initialization snapshot; dynamic zone hooks intentionally reread each request.

Evidence: `config::tests::every_constructor_field_matches_pinned_client_config_defaults` exhaustively specifies every struct field with a private property reader; `java_properties_have_exact_precedence_and_parse_boolean_rules`, `client_tls_enable_uses_global_enable_not_server_mode`, `client_tls_property_snapshot_matches_invoked_java_class_initialization`, `java_property_backed_client_fields_match_pinned_constructor_fixture`, `automatic_client_ip_matches_invoked_java_network_util_and_constructor`, and `local_address_selection_matches_invoked_java_candidates_and_ipv6_fallback` verify property/default and bounded interface-selection inputs. The fresh-JVM `ClientConfigPropertiesProbe` compares defaults and configured values for all eight direct `System.getProperty` fields (`rocketmq.client.name`, decode-read/decompress, VIP, heartbeat V2, SOCKS config, send-latency and start-detector). Its inputs cover case-insensitive true, whitespace-padded false, and explicit empty SOCKS config. TLS and NameServer helper inputs have separate pinned fixtures. `ClientConfigTlsModeProbe` observes `tls.server.mode=enforcing`, then mutates `tls.enable` after the first ClientConfig: Java reports false for both ClientConfig objects, while standalone `NettyClientConfig.useTLS` is true and the runtime copy is false. The Rust child-process regression matches the one-time snapshot. The automatic-IP oracle calls `NetworkUtil.getLocalAddress()` and captures `getLocalInetAddressList()`; Rust matches both macOS IPv4 and IPv6 candidate order, the full selected address, and the IPv6-only subset over the observed host candidates. The one-time `java.net.preferIPv4Stack` process-environment snapshot is checked in a child process, its Boolean parsing follows Java exact-`true` rules, and IPv4-only enumeration excludes IPv6 candidates. `tests/config.rs::clone_and_reset_copy_every_public_setting_except_java_omissions` changes every public field and differentially compares the full source, clone, and reset snapshots against Java's `copyFields` fixture for all 33 non-internal fields, including signed-width extremes. `namespace_cache_setter_clone_reset_and_empty_collection_state_matches_java` covers private cache-state omissions. `centralized_defaults_and_java_clone_copy_exact_fields` and the other invoked `client-config.json` cases supply Java default/identity/resource evidence. Pinned Java bytecode and an ordered native regression cover first-external and first-fallback IPv6 candidate selection. The Unix adapter also mirrors OpenJDK NetworkInterface.isUp by requiring IFF_UP and IFF_RUNNING; `interface_filter_matches_java_up_running_and_exclusions` covers the flag predicate (ADR-0045). The same bytecode confirms that Java returns null when automatic address discovery throws; Rust now retains that null in `client_ip`, covered by `automatic_client_ip_failure_preserves_java_null_default`. Windows target-runtime enumeration, BSD target-runtime verification, real IPv6-only interfaces, and other constructor/activation paths remain under reconciliation. Linux runtime execution passed on Linux ARM64 for the recorded loopback-scope fixture and automatic resolver case; that result does not establish all Linux interface/topology behavior.

Unix interface mapping (`b2589280`, `a24c3bd1`, `b5917fc2`, `96b70d15`): OpenJDK's `addif` moves a logical alias address onto its accessible parent root and also marks the child virtual; RocketMQ skips virtual children, so the address is still present through the root. Rust now maps the alias to that parent or skips it if the parent is unavailable. On Linux, OpenJDK assigns every `/proc/net/if_inet6` row's interface index to `sin6_scope_id`; Rust uses the same proc source for Linux candidates, preserving Java `Inet6Address.getHostAddress()` interface-name formatting even for global candidates. The Linux backend reads IPv4 from `SIOCGIFCONF` and reproduces OpenJDK's interface/address head-insertion order across both families. The BSD/macOS adapter groups `getifaddrs` rows into the same separate family passes and list-head order and uses the JDK interface-index scope; the invoked macOS fixture now matches complete IPv4 and IPv6 candidate order. Synthetic alias, scope, ABI, and ordering cases pass on macOS. Linux production enumeration has run on Linux ARM64 against the recorded loopback-scope fixture; BSD production code has not run on a BSD target because this environment lacks that Rust target. Live Rust behavior on an IPv6-only host remains open.

Manager construction-path evidence: `cbc25b5` invokes real, unstarted Java `MQClientManager.getOrCreateMQClientInstance` and observes its runtime config. `tests/config.rs::manager_configuration_copy_matches_invoked_java_without_mutating_caller` confirms defaulted omitted settings, copied V2/concurrency settings, reset lazy namespace flag, unchanged caller and shared identity. The native entry uses full Rust Clone internally, but the production constructor subsequently applies `clone_client_config`; no production bug was demonstrated. Its historical fixture SHA-256 was `e586dbe60da7abf295fbe43f4c9cf97bfcb3482a62479f0c1401215ad94f7db6`. Config integration eight, workspace, Clippy and Maven verify passed; no transport/server starts in the Java oracle.

The rebalance parser fixture at `8822d4a` had SHA-256 `b6b429a61adfb8ef944ba79495e32c84dc45438c03b2e8f14b740a00a7ad3414`. It adds twenty actual Long.parseLong cases used by the two source RebalanceService interval properties. Arabic/fullwidth/mixed/Devanagari decimal input and ASCII/fullwidth signed i64 bounds are accepted; sign-only/space/superscript/supplementary/overflow inputs are rejected. `rebalance_property_signed_longs_match_invoked_java` checks each case for both properties, including existing nonpositive immediate semantics. Shared private `java_number` also supplies the existing checked-i32 route parser without widening its public boundary. Full workspace, Clippy, Maven drift check, formatter/diff and comparator self-test passed (`/tmp/p6-rebalance-long-*.log`); core library64. This is parser/source-call-site evidence, not twenty isolated executions of Java's static property initializer.

The removal fixture at `cbd643d` had SHA-256 `099ee99aec511e165bf45cc3b1cb512e029223ae02b07abac7e0a09de0a9d299`. Actual `managerRemoval` retains the Java live factory map and verifies first/removal/replacement/stale-removal/absent behavior. The native unconditional-removal test matches these identity outcomes; controlled initializer tests prove per-key waiting, unrelated-key progress and cancellation preserving publication. Both source overloads remove mappings without shutting down the removed runtime.

The live-table fixture at `b31a7d9` had SHA-256 `9e1f92f7b622d7509e93b5c143641a2b0a8b13cd60316bf468ea293872ab273e`. It adds actual `managerTable` map mutation outcomes and the live native `FactoryTable` counterpart. Retained handles observe later changes, primitive writes are atomic per key, conditional comparisons use runtime identity, and snapshot is explicitly a detached iteration copy. [ADR-0006](../architecture/ADR-0006-live-client-factory-table.md) records initialization and cross-manager cleanup ownership. Three integration checks plus private reservation/property tests verify the golden, 64-way put-if-absent, owning-value release, first-owner config/hook retention, logical-ID versus alias cleanup and mutation wait/cancel behavior. Core library68, full workspace, reviewed Clippy, Maven drift check, exact runtime/table gate, comparator/fmt/diff passed (`/tmp/p6-manager-table-*.log`).

The nullable-settings fixture at `90a9de7` had SHA-256 `fecd6c40f585c4076f6bbf5dcae33041e3a46a742b20e527176764b358cd842a`. Actual nullableSettings invokes setters/clone/reset and constructs a real unstarted runtime, plus both command encoding modes. Native `nullable_access_and_socks_settings_match_invoked_java_copy_and_construction` covers nullable access omission/receiver retention, null SOCKS copy and successful no-proxy construction; config integration9, workspace, Clippy, Maven/comparator/fmt/diff pass (`/tmp/p6-null-socks-access-*.log`, `/tmp/p6-null-config-java-verify.log`). The same Java oracle proves null language JSON omits the field and decodes default JAVA, while binary throws NullPointerException. At that stage native language could not represent this; ADR-0007 later implemented the public nullable Option migration and records its passing differential evidence.

The empty-SOCKS fixture at `6f5d663` had SHA-256 `adc1861780ed57c15c625ff9e36286f5644c8264a8365fc2741b5b9c59084a2d`. It adds twelve actual runtime SOCKS constructor cases. Null/exact empty/literal `null`/`{}` succeed and preserve raw Java settings; space, tab/CR/LF, form-feed, backspace, vertical-tab, NBSP, EM SPACE and invalid text fail. `socks_construction_matches_invoked_java_empty_and_whitespace_boundaries` reproduced empty-string rejection before the fix, then matches every case and retained raw native setting. Only exactly empty (not trimmed-empty) and None bridge to the existing lower no-proxy parser; lower public types/parser are unchanged. Config integration10, full workspace, Clippy, Maven drift check, comparator/manifest self-test and fmt/diff passed (`/tmp/p6-socks-blank-*.log`). This closes the demonstrated empty-string mismatch; other permissive Fastjson grammar and Phase6 acceptance remain open. Nullable command language was corrected later in ADR-0007.

The callback-thread fixture at `a2779c3` had SHA-256 `dc377b7e5b289781e2dd0e7d22a6acbcc7b5dde82fbe7e07155484cdb2467e65`. It corrects negative callback-thread constructor rejection. Six actual `MQClientInstance` constructions observe retained Netty config and actual `NettyRemotingClient.publicExecutor` core capacity reflectively: i32MIN/-1/0 all succeed with effective4; 1/4/i32MAX succeed with unchanged positive capacity. The new native constructor golden reproduced i32MIN rejection before implementation; private `callback_thread_effective_capacity_matches_invoked_java_executor` checks the production bridge against actual Java capacities. Raw public config/sharing inputs remain unchanged. P6-V02 retry now fails with enabled concurrent-heartbeat pool0 (source `Executors.newFixedThreadPool` rejects it), not an incorrectly rejected callback value. Core library69, config11, exact runtime7, full workspace, Clippy, Maven drift, comparator/manifest self-test and fmt/diff pass (`/tmp/p6-callback-threads-*.log`). Lower public types/defaults are unchanged; no threads are started for the i32MAX oracle/test. Whole-phase acceptance remains unproven; this historical entry predates ADR-0007's implemented null-language correction.

Initial live-table fixture (`b31a7d9`) SHA-256 `9e1f92f7b622d7509e93b5c143641a2b0a8b13cd60316bf468ea293872ab273e`; it invokes actual `getProducerTable/getConsumerTable` map primitives and runtime selectors. Each retained map has nineteen observed outcomes for put/putIfAbsent/replace/conditional replace/remove/conditional remove/absent/empty-key/size/clear/repopulation. Four further actual consumer method outcomes confirm registration, direct replacement, duplicate rejection and current-mapping unregister. Retained-view and owner-equality additions produce SHA-256 `6ca2b11853282852407ab476bbd2e4b41401aa4cbe816b2d68ee35d0efa801a8`; getOrDefault/putAll update it to `d0c7f7bec5dd54c6570419099cc4d9ba7dd2d94a53baa5a84cdabcc43e8ae01d`; removeIf/retainAll update it to `60212adf7edc40e92763d4df205a151bf6a1787afe4324bf41fd3edae7545c92`; removeAll and owner-equality collection cases produce final SHA-256 `fc7119b073dbd6e6da0d1219352f7cc91606ad3df11ff3fc7a8e6eced759ec4c`. Proxy interface values isolate map mechanics, not producer sending/consumer behavior. The runtime is unstarted, Broker table empty, and always torn down. Source producer registration's concrete DefaultMQProducerImpl input is not claimed exercised by a proxy. Full workspace, strict Clippy, Maven/comparator/manifest22/fmt/diff pass. The live table and tested view methods are implemented; Java weak-iterator visibility maps to the allowed traversal snapshot described above, while exact Java Iterator and generic Collection overload objects are not modeled.

## ClientConfig methods and constants

| Java public API | Rust counterpart / evidence |
| --- | --- |
| `ClientConfig()` no-argument construction | `Default`, `new_auto`; direct Java `NetworkUtil.getLocalAddress`/constructor oracle, macOS IPv4 candidate-order adapter test, IP classification fixture, and Java null-IP fallback on address-discovery failure ([ADR-0045](../architecture/ADR-0045-automatic-client-ip-java-order.md)) |
| buildMQClientId | `build_client_id`; 73 invoked identity cases |
| changeInstanceNameToPID | `change_instance_name_to_pid`; normalized Java PID outcomes and private once/null test |
| getNamesrvAddr/setNamesrvAddr | `get_namesrv_addr/set_namesrv_addr`; endpoint golden and runtime getter wire-boundary check |
| getNamespace/setNamespace (deprecated) | `get_namespace/set_namespace`; lazy-state unit test; raw field is not setter replacement |
| withNamespace(String/Set), withoutNamespace(String/Set) (deprecated) | `with_namespace/with_namespace_set`, `without_namespace/without_namespace_set`; resource golden and private set-dedup test |
| queueWithNamespace/queuesWithNamespace (deprecated) | `queue_with_namespace/queues_with_namespace`; six actual collection cases match returned identity, null/empty short-circuiting, failure points, partial mutation and field preservation; [ADR-0011](../architecture/ADR-0011-message-queue-namespace-mutation.md) |
| cloneClientConfig/resetClientConfig | `clone_client_config/reset_client_config`; `clone_and_reset_copy_every_public_setting_except_java_omissions` differentially checks Java source/clone/reset snapshots for all 33 non-internal fields, plus the private namespace flag test; Rust `Clone` separately copies all fields |
| remaining field getters/setters | exact Java method names and idiomatic Rust accessors are listed in the complete 33-pair table below; `namesrvAddr` and `namespace` use their cache-aware methods; backing fields are crate-private |
| Consumer running-info `MixAll.object2Properties` field projection | Hidden, read-only `ClientConfig::to_java_properties` bridge for the separately packaged Consumer crate; preserves Java field names, raw nullable values, and null omission without exposing mutable storage |
| `toString` | Rust `Display` / `ToString`; 16 actual Java cases match null and populated text, namespace-cache state, unescaped strings, signed values, and all 14 Java `LanguageCode` constants. Java includes raw `socksProxyConfig` and omits `traceMsgBatchNum`/`maxPageSizeInGetMetadata`; Rust `Debug` remains redacted. |
| SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, SOCKS_PROXY_CONFIG, DECODE_READ_BODY, DECODE_DECOMPRESS_BODY, SEND_LATENCY_ENABLE, START_DETECTOR_ENABLE, HEART_BEAT_V2 | same-named `ClientConfig` associated constants; `public_property_names_match_pinned_client_config_constants` checks exact strings |

### Public `ConfigError` result carrier

`ConfigError` is the public Rust result type for ClientConfig helpers that model Java's exception paths. Its variants map to these source-observed outcomes; the type adds typed Rust errors without adding ClientConfig features.

| Rust variant | Java operation / observed outcome | Rust owner and evidence |
| --- | --- | --- |
| `InvalidInstanceEndpoint` | `getNamespace` and callers that first resolve the namespace propagate Java `StringIndexOutOfBoundsException` when an instance endpoint's last slash falls after its first dot | `get_namespace` / namespace helpers; `endpoints` cases in `client-config.json` |
| `NullInstanceName` | `changeInstanceNameToPID` dereferences `instanceName` and throws `NullPointerException` for null | `change_instance_name_to_pid`; `instanceNameNullPidOutcome` in `client-config.json` and `endpoint_namespace_and_resource_helpers_match_invoked_java` |
| `NullQueueCollection` | `queuesWithNamespace` throws `NullPointerException` on a null collection only after a non-empty namespace requires iteration | `queues_with_namespace`; `queuesWithNamespace` fixture cases compare outcome and returned collection identity |
| `NullMessageQueue` | `queuesWithNamespace` throws `NullPointerException` when traversal reaches a null element; earlier queue mutations remain and later elements are not visited | `queues_with_namespace`; `queuesWithNamespace` fixture cases compare failure point and partial mutation |

With no namespace or an empty namespace, `queuesWithNamespace` returns the input as-is—including a null collection or null elements—matching Java's short-circuit before traversal. Automatic client-IP discovery is not an error variant: Java catches discovery failures and leaves `clientIP` null, which `ClientConfig::new_auto` preserves.

### Complete Java setting accessor pairs

The pinned 5.5.1 declaration list contains 33 non-internal getter/setter pairs. This table maps all 66 exact Java `get`/`is`/`set` names to Rust accessors. Each Java getter below is invoked for source, clone, and reset objects by `clientConfigGetters`; native accessors and all three returned snapshots are compared by `clone_and_reset_copy_every_public_setting_except_java_omissions`. `namesrvAddr` and `namespace` retain their cache-aware Rust methods. `namespaceInitialized` is private Java state, not a public accessor pair.

| Java getter / setter | Rust getter / setter | Java setting / Rust field |
| --- | --- | --- |
| `getNamesrvAddr` / `setNamesrvAddr` | `get_namesrv_addr` / `set_namesrv_addr` | `namesrvAddr` / `namesrv_addr` |
| `getClientIP` / `setClientIP` | `client_ip` / `set_client_ip` | `clientIP` / `client_ip` |
| `getInstanceName` / `setInstanceName` | `instance_name` / `set_instance_name` | `instanceName` / `instance_name` |
| `getClientCallbackExecutorThreads` / `setClientCallbackExecutorThreads` | `client_callback_executor_threads` / `set_client_callback_executor_threads` | `clientCallbackExecutorThreads` / `client_callback_executor_threads` |
| `getNamespace` / `setNamespace` | `get_namespace` / `set_namespace` | `namespace` / `namespace` |
| `getNamespaceV2` / `setNamespaceV2` | `namespace_v2` / `set_namespace_v2` | `namespaceV2` / `namespace_v2` |
| `getAccessChannel` / `setAccessChannel` | `access_channel` / `set_access_channel` | `accessChannel` / `access_channel` |
| `getPollNameServerInterval` / `setPollNameServerInterval` | `poll_name_server_interval` / `set_poll_name_server_interval` | `pollNameServerInterval` / `poll_name_server_interval` |
| `getHeartbeatBrokerInterval` / `setHeartbeatBrokerInterval` | `heartbeat_broker_interval` / `set_heartbeat_broker_interval` | `heartbeatBrokerInterval` / `heartbeat_broker_interval` |
| `getPersistConsumerOffsetInterval` / `setPersistConsumerOffsetInterval` | `persist_consumer_offset_interval` / `set_persist_consumer_offset_interval` | `persistConsumerOffsetInterval` / `persist_consumer_offset_interval` |
| `getPullTimeDelayMillsWhenException` / `setPullTimeDelayMillsWhenException` | `pull_time_delay_millis_when_exception` / `set_pull_time_delay_millis_when_exception` | `pullTimeDelayMillsWhenException` / `pull_time_delay_millis_when_exception` |
| `getTraceMsgBatchNum` / `setTraceMsgBatchNum` | `trace_msg_batch_num` / `set_trace_msg_batch_num` | `traceMsgBatchNum` / `trace_msg_batch_num` |
| `isUnitMode` / `setUnitMode` | `unit_mode` / `set_unit_mode` | `unitMode` / `unit_mode` |
| `getUnitName` / `setUnitName` | `unit_name` / `set_unit_name` | `unitName` / `unit_name` |
| `isDecodeReadBody` / `setDecodeReadBody` | `decode_read_body` / `set_decode_read_body` | `decodeReadBody` / `decode_read_body` |
| `isDecodeDecompressBody` / `setDecodeDecompressBody` | `decode_decompress_body` / `set_decode_decompress_body` | `decodeDecompressBody` / `decode_decompress_body` |
| `isVipChannelEnabled` / `setVipChannelEnabled` | `vip_channel_enabled` / `set_vip_channel_enabled` | `vipChannelEnabled` / `vip_channel_enabled` |
| `isUseHeartbeatV2` / `setUseHeartbeatV2` | `use_heartbeat_v2` / `set_use_heartbeat_v2` | `useHeartbeatV2` / `use_heartbeat_v2` |
| `isUseTLS` / `setUseTLS` | `use_tls` / `set_use_tls` | `useTLS` / `use_tls` |
| `getSocksProxyConfig` / `setSocksProxyConfig` | `socks_proxy_config` / `set_socks_proxy_config` | `socksProxyConfig` / `socks_proxy_config` |
| `getMqClientApiTimeout` / `setMqClientApiTimeout` | `mq_client_api_timeout` / `set_mq_client_api_timeout` | `mqClientApiTimeout` / `mq_client_api_timeout` |
| `getDetectTimeout` / `setDetectTimeout` | `detect_timeout` / `set_detect_timeout` | `detectTimeout` / `detect_timeout` |
| `getDetectInterval` / `setDetectInterval` | `detect_interval` / `set_detect_interval` | `detectInterval` / `detect_interval` |
| `getLanguage` / `setLanguage` | `language` / `set_language` | `language` / `language` |
| `isEnableStreamRequestType` / `setEnableStreamRequestType` | `enable_stream_request_type` / `set_enable_stream_request_type` | `enableStreamRequestType` / `enable_stream_request_type` |
| `isSendLatencyEnable` / `setSendLatencyEnable` | `send_latency_enable` / `set_send_latency_enable` | `sendLatencyEnable` / `send_latency_enable` |
| `isStartDetectorEnable` / `setStartDetectorEnable` | `start_detector_enable` / `set_start_detector_enable` | `startDetectorEnable` / `start_detector_enable` |
| `isEnableHeartbeatChannelEventListener` / `setEnableHeartbeatChannelEventListener` | `enable_heartbeat_channel_event_listener` / `set_enable_heartbeat_channel_event_listener` | `enableHeartbeatChannelEventListener` / `enable_heartbeat_channel_event_listener` |
| `isEnableTrace` / `setEnableTrace` | `enable_trace` / `set_enable_trace` | `enableTrace` / `enable_trace` |
| `getTraceTopic` / `setTraceTopic` | `trace_topic` / `set_trace_topic` | `traceTopic` / `trace_topic` |
| `getMaxPageSizeInGetMetadata` / `setMaxPageSizeInGetMetadata` | `max_page_size_in_get_metadata` / `set_max_page_size_in_get_metadata` | `maxPageSizeInGetMetadata` / `max_page_size_in_get_metadata` |
| `isEnableConcurrentHeartbeat` / `setEnableConcurrentHeartbeat` | `enable_concurrent_heartbeat` / `set_enable_concurrent_heartbeat` | `enableConcurrentHeartbeat` / `enable_concurrent_heartbeat` |
| `getConcurrentHeartbeatThreadPoolSize` / `setConcurrentHeartbeatThreadPoolSize` | `concurrent_heartbeat_thread_pool_size` / `set_concurrent_heartbeat_thread_pool_size` | `concurrentHeartbeatThreadPoolSize` / `concurrent_heartbeat_thread_pool_size` |

## MQClientInstance public methods

Methods implemented on `ClientRuntime` unless another owner is specified. A fresh `javap -public` inventory against the pinned 5.5.1 jars records the declaration counts and overload groups: `MQClientInstance` 53 signatures/49 names (constructors, `sendHeartbeatToBroker`, and `updateTopicRouteInfoFromNameServer` overloaded); `MQClientManager` 7/5 (`getOrCreateMQClientInstance`, `removeClientFactory`); `ClientConfig` 78/76 (`withNamespace`, `withoutNamespace`); `ClientRemotingProcessor` 6/6; `TopicPublishInfo` 18/16 (`selectOneMessageQueue`); `RebalanceService` 3/3; `PullMessageService` 11/11; inherited `ServiceThread` 11/10 (`shutdown`). Each declaration name and overload group is represented in the corresponding API/config rows below; the full pinned output is `/private/tmp/p6-javap-public-signatures.txt`. The name check does not prove behavior for every input or source branch; row evidence and unresolved owner boundaries below remain authoritative. Evidence abbreviations refer to current files, not unimplemented test IDs: **R** = `tests/runtime.rs`; **C** = private `runtime/control.rs` tests; **T** = private `runtime/tasks.rs` tests; **H** = `tests/heartbeat.rs`; **Q** = private `route/cache.rs` tests plus route golden; **L** = `tests/runtime_lookup.rs`; **M** = `tests/runtime_metadata.rs`. Future-owner rows remain inactive/PARTIAL, never COMPATIBLE.

| Java public method / overload | Rust counterpart | Evidence / owner |
| --- | --- | --- |
| MQClientInstance(config,index,id[,hook]) | private owned construction through manager `get_or_create[/with_hook]`; native index is not new user tuning | R/C, hook wire tests, boot M |
| topicRouteData2TopicPublishInfo / topicRouteData2TopicSubscribeInfo | `TopicSnapshot::from_route_data(topic, route)` and `from_nullable_route_data(Option<&str>, Option<&TopicRouteData>)` return publish/subscribe queues, order-topic state, static endpoints, and copied route data in one derived snapshot | Q, 35 standard plus ten nullable-argument Java cases compare public conversion and runtime cache publication, including Java `getTopicRouteData()` field values; the empty static-mapping/unmatched-broker case yields no publish queues but keeps readable subscribe queues; null topic survives dynamic, static and ordered queues plus static endpoint keys, null route maps to typed `InvalidRoute`; ordered parsing covers empty separators, repeated broker segments and retained duplicate queues; nullable Broker values decode and publish through the native DTO model (ADR-0008); producer `TopicPublishInfo` selection Phase 7 |
| start / shutdown | `start/shutdown`, serialized owned control task | C: 17 explicit / 18 discovery rollback points, cancellation, running identity/shared-user wire gates; forked direct Java start and Rust differentials verify three live NameServer mutation cases: null→configured skips discovery and retains an absent constructor snapshot; configured→null fetches once and retains the configured constructor snapshot; configured→empty skips discovery and also retains that snapshot; the native scheduler fixture test matches its four registrations. Direct `startScheduledTask()` calls also verify both discovery-registration branches and reread all three interval settings after config mutation |
| getClientId / getBootTimestamp / getClientConfig | `client_id/boot_timestamp/config -> SharedClientConfig` (shared mutable handle) | `RuntimeConfigFixtures.managerSharing` invokes Java getters before/after changing client IP, instance name and unit name on the retained config: `getClientId` stays captured while `buildMQClientId` reflects those mutations; Rust `runtime_client_id_remains_constructor_snapshot_after_config_mutation` compares both IDs. Direct pinned Java `getBootTimestamp` is bounded by constructor wall-clock samples and stays stable after fixture calls; `runtime_metadata` checks shared/start/stop/replacement lifecycle. `getClientConfig` shared-object mutation is covered by config tests; construction-time transport resources remain captured |
| updateTopicRouteInfoFromNameServer() | `update_all_topic_routes`; periodic route tick reuses public entry | R empty-round/no-socket, union/dedup, rejection continuation, null-topic JSON-null no-op and callback registry reentry; T existing schedule checks |
| updateTopicRouteInfoFromNameServer(topic) | `update_topic_route(topic, deadline)` / `update_topic_route_nullable(Option<&str>, deadline)` | R/Q; successful null decode returns false without cache publication (ADR-0039); null topic is sent to NameServer, a missing route returns false, and a returned route maps Java's null-key failure to `InvalidRoute`; actual registered-consumer null-queue cases distinguish ordered-route rejection from static-mapping success and normalized publication; null outer `queueDatas` plus valid static mapping publishes and updates consumer subscription info once; when `isSubscribeTopicNeedUpdate` is true, an equal route causes Java/Rust to return changed=true and republish to producer and consumer; null `BrokerData.brokerAddrs` fails before publication or callbacks, while a three-refresh case retains the preceding route/counts on failure, then republishes a later valid route while preserving old and new broker publish lookups |
| updateTopicRouteInfoFromNameServer(topic,isDefault,producer) | `update_topic_route_from_default(topic, queue_count, deadline)` or ordinary update | R read-based clamp; producer-owned fallback Phase 7 |
| parseOffsetTableFromBroker | `parse_offset_table_from_broker(Option<&BrokerOffsetTable>, Option<&str>) -> Result<HashMap<Option<SharedValue<MessageQueue>>, Option<i64>>, RuntimeError>`; shared ordered input preserves nullable `MessageQueue` identity and caller-visible mutation | M, ten ordinary and seven nullable actual Java cases; explicit namespace conversion, nullable keys/values/queue fields, in-place key mutation, returned-key aliasing, partial mutation before null-key error, and null-table errors across null/empty/nonempty namespaces |
| checkClientInBroker | `check_client_in_broker` | `tests/client_check.rs`, 16 actual Java cases including null non-TAG topic failure before RPC; consumer-start activation Phase 10/13 |
| `sendHeartbeatToAllBrokerWithLock()` / `sendHeartbeatToAllBrokerWithLockV2(isRebalance)` | `send_heartbeat_to_all_brokers` / `send_heartbeat_to_all_brokers_with_rebalance` | H, private heartbeat gate; `heartbeatV2WrapperSettingMutation` invokes both actual wrappers before/after retained-config mutation and records modes `V1,V1,V2,V2` with concurrent-executor flags `true,false,false,false`; native wrapper test observes zero V1 then computed V2 fingerprints through both APIs; WithLockV2 V1 remains serial when V2 is disabled |
| `sendHeartbeatToBroker(id,name,addr[,strictLockMode])` | `send_heartbeat_to_broker` / `send_heartbeat_to_broker_with_lock_mode` | H single-peer/active-listener checks; `heartbeatV2SettingMutation` records actual Java API calls V1→V2 after post-construction config mutation, and `heartbeat_protocol_mode_reads_live_v2_setting_after_construction` consumes the modes and compares V1's zero versus V2's computed wire fingerprint |
| adjustThreadPool | `adjust_thread_pool`; periodic tick reuses public entry | T per-owner construction/poll failure and missing capability; actual push adjustment Phase 10 |
| registerConsumer / unregisterConsumer | `register_consumer/unregister_consumer` identity lease plus source unconditional `unregister_consumer_by_group`; nullable wrappers `register_consumer_nullable(Option<&str>, Option<Arc<dyn ConsumerRuntime>>)` and `unregister_consumer_by_group_nullable(Option<&str>, deadline)` | C/R/registry plus four actual consumer method outcomes and absent/current/cancelled/terminal wire checks; successful registration strongly retains its owner until replacement/removal/clear, and runtime back-references use `Weak<ClientRuntime>`; pinned null group/owner registration returns false, null unregister group throws before RPC; Rust returns `None` for rejected nullable registration and `InvalidRoute` for null removal; concrete consumer Phase 9/10/13; wire `MQClientAPIImpl.unregisterClient` uses `UnregisterClientRequestHeader`, command 35, no body |
| registerProducer / unregisterProducer | `register_producer/unregister_producer` identity lease plus source unconditional `unregister_producer_by_group`; nullable wrappers `register_producer_nullable(Option<&str>, Option<Arc<dyn ProducerRuntime>>)` and `unregister_producer_by_group_nullable(Option<&str>, deadline)` | registry and pinned source/producer header/retention checks; successful registration strongly retains its owner until replacement/removal/clear, and runtime back-references use `Weak<ClientRuntime>`; pinned null group/owner registration returns false, null unregister group throws before RPC; Rust returns `None` for rejected nullable registration and `InvalidRoute` for null removal; `DefaultMQProducerImpl.shutdown(boolean)` unregisters by the facade's current group before changing service state, so a null group throws and leaves it RUNNING, while a changed group removes whichever table entry currently has that key; `ProducerLifecycleFixtures` and the Phase 7 matrix cover this facade behavior. The lower-level Rust `GroupLease` remains identity-bound. Wire `MQClientAPIImpl.unregisterClient` uses `UnregisterClientRequestHeader`, command 35, no body |
| registerAdminExt / unregisterAdminExt | `register_admin_ext(Option<&str>, Option<Arc<dyn Any + Send + Sync>>)` retains the first owner and returns false for null/duplicate inputs; `unregister_admin_ext(Option<&str>)` removes unconditionally by group | 15 fixture-backed Java registration outcomes plus a started-runtime shutdown gate: admin presence retains the runtime mapping; unregister permits shutdown/removal. Strong owner retention/release, empty group, null inputs, duplicates, missing group and re-registration; owner drops occur outside registry locks to permit Rust destructor re-entry; owner back-references use `Weak<ClientRuntime>` to avoid cycles; null key failure maps to `InvalidRoute`; concrete admin operations Phase 17 |
| Rust admin lifecycle lease helpers (no separate Java lease object) | `ClientRuntime::register_admin` returns a `GroupLease` identity token and `unregister_admin` accepts it for a retained admin-group registration; `register_admin_ext` / `unregister_admin_ext` remain the direct Java owner-table adapters | Native lifecycle ownership form for the same registered-admin shutdown gate, not another admin RPC or feature; `admin_leases_are_separate_from_heartbeat_groups_and_runtime_identity`, stale-token and shutdown-gate regressions cover the lease semantics |
| rebalanceLater / rebalanceImmediately / doRebalance | `rebalance_later/rebalance_immediately/do_rebalance` | private rebalance/T; real assignment lifecycle Phase 11 |
| selectProducer / selectConsumer | `select_producer/select_consumer` plus `select_producer_nullable/select_consumer_nullable(Option<&str>)`, cloned strong owners retained by the registry | registry tests and direct pinned null-key cases; Java null group throws `NullPointerException`, Rust nullable wrappers return typed `InvalidRoute`; Java and Rust selectors return owners after caller references are dropped, until removal/clear; owner runtime back-references use `Weak<ClientRuntime>` to avoid `Arc` cycles |
| getBrokerNameFromMessageQueue | `RouteSnapshot::broker_name` / `broker_name_nullable(Option<&MessageQueue>) -> Result<Option<&str>, RuntimeError>` | Q static endpoint hit/miss and empty/absent-map fallback; null queue maps Java `NullPointerException` to `InvalidRoute("null message queue")`, and null queue topic maps to `InvalidRoute("null message queue topic")`; actual runtime cases before and after static-to-ordinary refresh, fixture commits `c7b04506`/`5125b311` |
| `findBrokerAddressInAdmin(brokerName)` / `findBrokerAddressInPublish(brokerName)` / `findBrokerAddressInSubscribe(brokerName, brokerId, onlyThisBroker)` | `RouteSnapshot::admin_address/subscribe_address -> Result<Option<BrokerAddress>, RuntimeError>` and `publish_address -> Option<&str>`; nullable counterparts are `admin_address_nullable/publish_address_nullable/subscribe_address_nullable(Option<&str>, ...)` | `RuntimeRouteFixtures.runtimeBrokerAddressResolution` directly invokes the pinned Java methods in nine cases; `route::cache::tests::broker_address_resolution_matches_invoked_java_runtime_cases` compares exact master/slave, adjacent-slave selection, restricted miss, arbitrary fallback, empty/absent tables, null values/IDs, signed-long overflow, and version lookup. JaCoCo reports all branches covered for these three methods and `findBrokerVersion`; Java HashMap selection among multiple valid entries remains an allowed-set comparison. Null broker name returns Java null (`None`) for all three methods; null Long-key unboxing errors map to `InvalidRoute` |
| findConsumerIdList / queryAssignment | `ClientRuntime::find_consumer_id_list/query_assignment`; exact nullable adapters `find_consumer_id_list_nullable(Option<&str>, Option<&str>)` and `query_assignment_nullable(Option<&str>, ...)` preserve nullable topic/group/strategy/model inputs | Direct invoked Java `MQClientInstance` oracle records routed and empty-Broker branches, refresh count, address, args, signed timeout and error outcomes; null topic throws before the Broker RPC path for both methods and maps to `InvalidRoute`; an empty topic remains a valid route key, is sent to NameServer, and supports consumer-list and assignment Broker lookups; a null consumer group is forwarded and omitted from request ext fields, whereas an empty group is passed and appears on wire as `consumerGroup=""`; response IDs decode normally; `queryAssignment(topic, null, null, null, timeout)` forwards nullable group/strategy/model and sends a body containing only topic/clientId, while empty group/strategy remain encoded as empty strings with a non-null model; both responses decode an empty success body; `runtimeLookupVipMutation` invokes both through the actual `MQClientAPIImpl` remoting path, recording `127.0.0.1:10913` before and `:10911` after mutating the retained VIP setting; Java `consumerCallsAfterTimeoutMutation` records 150ms after changing the retained timeout from its 3000ms default, and the Rust delayed loopback differential verifies timeout behavior. Other Rust loopback cases consume the Java VIP observations, verify both requests reach the transformed port, and compare consumer IDs/assignment args, no-Broker null results and zero/negative timeout silence. Typed response handling now passes the DTO Java Fastjson decoder explicitly for consumer-list, assignment, lock, time-span, group/topic/subscription, and direct-consume responses; controller sync-state, epoch-cache, and election use nullable Java Fastjson decoding. L exact requests, fresh budgets, VIP/error/Set checks; 25 consumer-list DTO inputs, 37 assignment response-body inputs, 20 response assignment-mode inputs, seven attachment decode inputs, one null-key setter/getter API case, and 52 QueryAssignmentRequestBody and 49 SetMessageRequestModeRequestBody input cases across every field token shape and every root shape |
| findBrokerAddrByTopic | `find_broker_address_by_topic` / `find_broker_address_by_topic_nullable(Option<&str>)`, raw topic BrokerData selection | Q/L, 14 invoked candidate/error cases, including null topic (`NullPointerException` → typed `InvalidRoute`), null broker IDs, null broker-list/row failures, and missing-list defaults |
| `RESET_CONSUMER_CLIENT_OFFSET` / `GET_CONSUMER_STATUS_FROM_CLIENT` inbound requests | Push registers reset and status processors; Pull registers status and preserves Java's one-way reset behavior | Phase 17 `ClientRemotingProcessor` coverage: reset fences in-flight pull/callback work before persisting new offsets; status returns the requested namespaced topic snapshot and rejects malformed headers. See [Phase 17 admin matrix](PHASE_17_ADMIN_MATRIX.md) and consumer admin tests. |
| getAnExistTopicRouteData / queryTopicRouteData | `ClientRuntime::get_existing_topic_route/get_existing_topic_route_nullable` and `query_topic_route/query_topic_route_nullable` | L cached-empty/absent refresh; null topic maps Java `NullPointerException` to `InvalidRoute`, and query rejects before refresh |
| MQAdminImpl.fetchPublishMessageQueues(topic) | `PullConsumer::fetch_publish_message_queues` with `ClientRuntime::get_topic_publish_queues` | Phase 9 scripted NameServer cases cover direct route lookup without shared-cache publication, publish queue ordering, namespace removal, repeated lookup, route removal after a successful lookup, missing/empty routes and malformed route data |
| getMQClientAPIImpl | `api`; `remoting` exposes its native lower transport | typed API tests; no parallel transport |
| `getMQAdminImpl` | Producer, PushConsumer, and PullConsumer expose implemented admin operations through their facade owners; Rust does not expose a mutable raw `MQAdminImpl` handle | Phase 17 maps each Java `MQAdmin` operation across all three facades, including Broker errors and malformed responses. The internal Java implementation object has no separate Rust public counterpart; see [Phase 17 admin matrix](PHASE_17_ADMIN_MATRIX.md). |
| `MQClientInstance.getScheduledExecutorService()` | `ClientRuntime::scheduled_executor_service() -> RuntimeScheduler`; supports immediate/delayed async tasks, result-bearing `submit`/`schedule_call_after`, `invoke_all`/`invoke_any` and their `*_with_timeout` variants, fixed-rate/fixed-delay callbacks, per-task cancellation, `shutdown`/`shutdown_now`, shutdown/termination status and async termination wait | Direct pinned source returns the same single-thread `ScheduledExecutorService` used by client periodic registrations. Bulk calls submit in order; `invoke_all` returns per-task values or receive errors in submission order, while `invoke_any` skips panicking/cancelled tasks and stops later queued callbacks after the first normally returned value. Timed `invoke_all_with_timeout` returns completed values, failure states and `CancelledByTimeout` outcomes in submission order; timed `invoke_any_with_timeout` cancels accepted work and returns `RuntimeError::InvocationTimeout` if no value arrives. Rust returns outcome values rather than Java `Future` wrappers. Dropping either bulk-call future cancels accepted work. `RuntimeScheduledCall::join_with_timeout` and `RuntimeScheduledTaskHandle::wait_with_timeout` do not cancel their scheduled callback; task handles expose cancellation, completion, remaining delay and async waiting, while `RuntimeScheduledCall::task_handle` exposes call identity. Graceful shutdown stops periodic work and drains accepted one-shots; immediate shutdown returns handles for queued Rust jobs without cancelling them and signals cancellation to a running Rust future without self-joining. The returned handles preserve identity for submitted calls and remain explicitly cancellable. Paused-time regressions cover pre-start execution, delays, both repeating modes, cancellation, returned values, bulk methods and their failure/empty/timeout/cancel paths, Future status/wait, returned-list membership and pending state, both shutdown modes, runtime shutdown, and shutdown called from the worker. Pinned Java pre-start shutdown returns 32 client tasks (including 30 `ConsumerStatsManager` maintenance jobs) and two pull tasks; Rust seeds the same 30 constructor-time jobs in its client queue, with set-specific sampling and print callbacks, and the shutdown fixture returns the same 32 entries. This scheduler surface is mapped locally; the wider P18-V01 source/API/settings/input/error/control-flow and reverse-export audit remains open. |
| getPullMessageService | pull/pop enqueue/scheduled-task methods on runtime | private pull tests; concrete requests/process queues consumer owners |
| `DefaultMQProducer.getDefaultMQProducerImpl()` / `DefaultMQProducerImpl.getDefaultMQProducer()` | `Producer::{default_mq_producer_impl,default_mq_producer}` borrow the same Rust owner; internal runtime registration and consumer send-back continue to use their existing owner paths | The invoked Java fixture records both object-identity outcomes, and `implementation_state_accessors_match_pinned_java_fixture` checks them plus Rust pointer identity. Phase 6 owns registration/lifecycle; Phase 7 maps the public producer; Phase 9/10 map the Pull/Push retry and producer-fallback flows. The Rust API does not allocate a separate Java-style wrapper; see [the producer matrix](PHASE_7_PRODUCER_MATRIX.md), [Phase 9 matrix](PHASE_9_OFFLINE_METHOD_MATRIX.md), and [Phase 10 matrix](PHASE_10_OFFLINE_METHOD_MATRIX.md). |
| `MQClientInstance.getDefaultMQProducer()` | Internal `CLIENT_INNER_PRODUCER` registration is owned by the Core control actor; retry-topic publication maps to the Consumer owner's send-back path, while user-managed sends map to `Producer` | Reviewed as an accessor on Java's `client.impl.factory` runtime implementation, not as a separate supported Producer/Consumer facade. Pinned in-tree calls are the four consumer retry/send-back flows plus `MQClientInstance` start/shutdown lifecycle; Rust maps those behaviors through its Producer, Consumer, and ClientRuntime owners without exposing a mutable raw inner-producer wrapper. Source and callsite evidence: `MQClientInstance.java` SHA-256 `55619893df475f32332d4a49787ac4a8add0d881de76922d528ba6eb7f3e267d`; `PHASE_10_OFFLINE_METHOD_MATRIX.md` retry-fallback row. |
| getTopicRouteTable | `topic_route_table/TopicRouteTable`, live raw map with nullable `get_nullable`, `get_or_default_nullable`, `contains_key_nullable`, `remove_nullable`, `contains_value_nullable`, `put_nullable`, `put_if_absent_nullable`, `replace_nullable`, `replace_if_equal_nullable`, `remove_if_equal_nullable`, `compute_if_absent_nullable`, `compute_if_present_nullable`, `compute_nullable`, `merge_nullable`, `for_each_nullable`, `replace_all_nullable`, `compute_if_absent_with_nullable_callback`, `compute_if_present_with_nullable_callback`, `compute_with_nullable_callback`, and `merge_with_nullable_callback`; key-set/value/entry-view `contains_nullable`, `remove_nullable`, `remove_all_nullable`, and `retain_all_nullable`; `put_all_nullable_rows` and generic `put_all_nullable_iter`; mutable `TopicRouteDataHandle` aliases and cloneable key/value/entry views; `entry_snapshots()` yields `TopicRouteEntryHandle` values whose `set_value()` / `set_value_nullable()` update the captured entry and backing map; nullable callback results are covered by `replace_all_with_nullable_results`. `RouteSnapshot.raw_routes` is separate from the published topic index, whose route DTO copies retain Java-compatible nested aliases. `TopicRouteDataHandle::downgrade` / `WeakTopicRouteDataHandle::upgrade` expose Rust `Arc`/`Weak` lifetime control for this shared value, used to verify raw-table/snapshot retention; they add no route operation. | `topic_route_table3` now has 149 invoked map/view outcomes, including the six eager null map-callback failures with retained mapping, distinct same-key compute, `replaceAll`, cross-thread `clear`, and reentrant `clear` ordering plus all six `Map.Entry.setValue` behaviors and present/absent/null-key `get`/`getOrDefault`/`containsKey`/`remove` and nullable-value/key `containsValue`/`put`/`putIfAbsent`/`replace`/conditional mutations/`computeIfAbsent`/`computeIfPresent`/`compute`/`merge`, nullable key/value/entry-view arguments, six null collection arguments to view `removeAll`/`retainAll` operations, three null `removeIf` failures with retained mapping, and actual bidirectional DTO alias mutation; 21 direct runtime cleanup shapes cover absent/default fields, null collections/rows/address maps/values, both null-address-map orderings, null broker IDs with live address values, cross-name/ID and sibling-topic address liveness, immediate empty-row removal before a later malformed-row failure, deferred nonempty updates on traversal failure, and retained Broker versions; BrokerData equals/compare/hash/toString, five QueueData method cases, four TopicQueueMappingInfo and four TopicRouteData equals/hash/toString cases, and six `topicRouteDataChanged` sort outcomes; native tests cover defaults, equality, hashing, strings, sorting, bulk mutations, cached/refetched queries and raw/derived/cleanup branches; `TopicRouteDataHandle.snapshot/with_mut` maps Java DTO reads/setters; [ADR-0010](../architecture/ADR-0010-live-raw-topic-route-table.md) and [ADR-0082](../architecture/ADR-0082-raw-route-table-remapping-atomicity.md) cover iterator and same-key mutation semantics; other DTO/input reconciliation remains open |
| `CONSUME_MESSAGE_DIRECTLY` / `GET_CONSUMER_RUNNING_INFO` inbound requests | Push handles direct consume and Running Info; Pull handles Running Info and returns Java's missing-Push-consumer result for direct consume | Phase 17 covers request decoding, malformed or absent fields/body, Java response behavior, direct-consume offset isolation, status responsiveness during a slow callback, Running Info properties/statistics, and executor shutdown joining. Push and Pull owner APIs expose their Running Info snapshots. See [Phase 17 admin matrix](PHASE_17_ADMIN_MATRIX.md). |
| `getConsumerStatsManager` | `ClientRuntime::consumer_stats_manager` and `PushConsumer::get_consumer_stats_manager` expose the runtime-owned `ConsumerStatsManager`; Push/POP paths record metrics and Running Info reads the same group/topic status. `ConsumerStatsManager::with_scheduler(&RuntimeScheduler)` maps Java's scheduler-injected public constructor and registers its 30 maintenance jobs; `ClientRuntime` constructs its shared manager internally with a runtime-module-private allocator and queues the corresponding jobs. Rust's unscheduled public `new`/`Default` constructors were removed because Java has no matching public constructor. Public lifecycle, metric-update, and status methods map to Java methods. | Java creates one manager per `MQClientInstance`; Rust creates one per `ClientRuntime`, shared by Push owners with that runtime and isolated across distinct runtimes even when client IDs match. Rust keeps Java's `topic@group` key and does not automatically expire client stats entries. Five Java `StatsItemSet`s register 10-second, 10-minute, and hourly samplers plus minute/hour/day log callbacks. Rust runtime-owned and scheduler-injected managers register the same 30 set-specific jobs; scheduled callbacks advance only their owning metric series and log only that set. TPS/RT snapshots remain zero until a sample and failed-message totals remain zero until an hourly-window sample; updates and status reads also catch up elapsed sample boundaries. The invoked Java fixture records the before/after sample values; deterministic Core tests and consumer-flow tests cover sampling, callback ownership, identity and isolation. Before runtime binding, the Push getter returns no manager and internal stats updates are skipped; the fallback manager is no longer allocated. See [Phase 10 method matrix](PHASE_10_OFFLINE_METHOD_MATRIX.md) and [Phase 17 admin matrix](PHASE_17_ADMIN_MATRIX.md). |
| getNettyClientConfig | native transport configuration initialized from ClientConfig; no Netty object | All 20 Java `NettyClientConfig` settings/40 accessors, relevant `NettySystemConfig` properties, and construction/start/channel/operation read timing are mapped in [the Phase 3 remoting inventory](PHASE_3_REMOTING_INVENTORY.md); runtime settings and transport behavior have named coverage there. |
| getProducerTable / getConsumerTable | `producer_table/consumer_table`, live `ProducerTable/ConsumerTable`; concrete public wrappers `ProducerKeySetView`, `ProducerValuesView`, `ProducerEntrySetView`, `ConsumerKeySetView`, `ConsumerValuesView`, and `ConsumerEntrySetView` correspond to Java `keySet()`/`values()`/`entrySet()` results (`Set`/`Collection`/`Set`); generic `put_all_nullable_iter` plus slice/map nullable adapters; nullable `get_nullable`, `contains_key_nullable`, `get_or_default_nullable(Option<&str>, Option<Arc<_>>)` and `contains_value_nullable(Option<&Arc<_>>)`; nullable `put_nullable`, `put_if_absent_nullable`, `replace_nullable`, `replace_if_equal_nullable`, `remove_nullable`, and `remove_if_equal_nullable`; nullable live-view membership/removal and predicate/collection inputs; atomic and remapping methods, cloneable `key_set_view/values_view/entry_set_view` handles, `entry_snapshots()` with captured-value `set_value`; detached snapshots; shared callback aliases `TableForEach`, `TableReplaceAll`, and `TableReplaceAllOptionalResult` | registry map/four consumer method outcomes, actual Java null-key read/mutation failures plus null-value `containsValue` and required mutation-value failures mapped to typed `InvalidRoute`; key-view null membership/removal and values `contains(null)` throw NPE, values `remove(null)` and null/invalid entry membership/removal return false; null `removeIf` predicate and null bulk collection throw NPE and map to typed `InvalidRoute`; `getOrDefault(key, null)` returns the mapped owner or null for an absent key; conditional `remove(key, null)` returns false; non-null missing-key read outcomes, Java strong-owner registration retention, retained-view/bulk-operation and six entry replacement outcomes per map, custom-equality proxies, plus a pinned same-key remapping race on each table; [ADR-0009](../architecture/ADR-0009-live-runtime-registry-tables.md) and [ADR-0081](../architecture/ADR-0081-registry-table-remapping-atomicity.md) map weakly consistent traversal, strong owner retention, equality keys, lease/RPC ordering and callback atomicity; [ADR-0079](../architecture/ADR-0079-runtime-map-entry-set-value.md) records entry replacement |

### Nullable registry entry-view adapters

`FactoryEntrySetView`, `ProducerEntrySetView`, and `ConsumerEntrySetView` expose `contains_entry_nullable(key, value)` and `remove_entry_nullable(key, value)` for Java `Map.Entry` membership/removal inputs whose key or value may be null. When either component is absent, both operations return `false`, matching the pinned `ConcurrentHashMap.entrySet()` behavior; the owning tables still use identity comparison for producer/consumer/factory values and the mapped Java equality policy for custom registry values. These split-field adapters complement `contains_nullable(Option<entry>)` and `remove_nullable(Option<entry>)`, which model a null entry object.

### Nullable raw route-view callback adapters

Java `Collection.removeIf(Predicate)` maps to `TopicRouteKeySetView::remove_if_nullable`, `TopicRouteValuesView::remove_if_nullable`, and `TopicRouteEntrySetView::remove_if_nullable`. Each takes the corresponding `runtime::TableKeyViewPredicate`, `TableValueViewPredicate<TopicRouteData>`, or `TableEntryViewPredicate<TopicRouteData>` alias. `None` reproduces Java's eager null-predicate failure as `RuntimeError::InvalidRoute("null topic route predicate")`; the pinned differential confirms no view is traversed and the backing mapping remains. These are nullable argument adapters around the existing non-null `remove_if` methods.

### Nullable raw route-map callback adapters

Java `ConcurrentMap` callback arguments map to `TopicRouteTable::for_each_nullable`, `replace_all_nullable`, `compute_if_absent_with_nullable_callback`, `compute_if_present_with_nullable_callback`, `compute_with_nullable_callback`, and `merge_with_nullable_callback`. They accept the crate's matching `runtime::Table*` callback aliases; `None` returns `RuntimeError::InvalidRoute("null topic route callback")` before visiting, looking up, or mutating the map. The pinned differential compares all six eager NPE outcomes and the retained route mapping. Callback-result `None` remains independently represented by the existing nullable compute/remapping and `replace_all_with_nullable_results` methods.

### Public runtime table callback aliases

The shared public aliases are `runtime::TableForEach<T>` for Java `Map.forEach(BiConsumer)`, `TableReplaceAll<T>` for a non-null-result `Map.replaceAll(BiFunction)`, and `TableReplaceAllOptionalResult<T>` for Rust callers that must represent Java's nullable `replaceAll` result and its per-entry `NullPointerException`/partial-update behavior. The compute/merge aliases `TableComputeIfAbsent<T>`, `TableComputeIfPresent<T>`, `TableCompute<T>`, and `TableMerge<T>` map the corresponding Java remapping functions. `TableKeyViewPredicate`, `TableValueViewPredicate<T>`, and `TableEntryViewPredicate<T>` map Java `Collection.removeIf(Predicate)` callbacks. These are callback parameter types for the exported FactoryTable, producer/consumer tables, and raw route table; they are not additional client settings or callback features.

The public callbacks below are methods of the anonymous `ChannelEventListener` created by the `MQClientInstance` constructor, not declared methods of `MQClientInstance` itself. They are included here because they are observable callback behavior on the runtime's remoting client.

| Pinned Java listener callback | Rust counterpart / evidence |
| --- | --- |
| `onChannelConnect/Close/Exception/Idle(remoteAddr, channel)` | `ChannelEventListener::{on_connect,on_close,on_exception,on_idle}` default no-op methods, matching the Java listener body. |
| `onChannelActive(remoteAddr, channel)` | `HeartbeatChannelListener::on_active_async` snapshots broker addresses, sends non-strict-lock heartbeat to matching broker entries, and wakes rebalance after a successful send; `tests/heartbeat.rs` listener cases and P6-U01 cover the activated path. |

## Phase 6 source-private method reverse inventory

Pinned `javap -p` on `MQClientManager`, `MQClientInstance`, `ClientConfig`, `TopicPublishInfo`, `ClientRemotingProcessor`, `RebalanceService`, `PullMessageService`, and inherited `ServiceThread` finds 26 hand-written private/protected declarations. The scan also finds 11 compiler-generated lambda methods (`MQClientManager.lambda$getOrCreateMQClientInstance$0`, `MQClientInstance.lambda$new$0`, `lambda$startScheduledTask$4..$8`, `lambda$cleanupAfterStartFailure$2/$3`, `lambda$topicRouteData2TopicPublishInfo$1`, and `lambda$sendHeartbeatToAllBrokerConcurrently$9`); those map to their enclosing source methods below and are not independent API or behavior rows. `ClientConfig`, `RebalanceService`, and `PullMessageService` declare no additional private/protected methods beyond their constructors/inherited worker surface.

| Pinned Java source method(s) | Rust flow / disposition |
| --- | --- |
| `MQClientManager()`; `lambda$getOrCreateMQClientInstance$0` | The private singleton constructor maps to the phase-required explicit `ClientRuntimeManager`; the generated initializer maps to per-client-ID single-flight creation and first-runtime reuse in `runtime/manager.rs`. |
| `cleanupAfterConstructionFailure`; `runCleanup`; `cleanupAfterStartFailure` and its two generated cleanup lambdas; `MQClientInstance.lambda$new$0` thread factory | Constructor/start rollback, best-effort cleanup, and owned worker-thread naming map to runtime resource construction and serialized startup rollback; all injected startup failure points are covered by the lifecycle audit and P6-V03. |
| `startScheduledTask`; its five generated scheduled-task lambdas | Java registration sequence, fixed-rate delays, live interval reads, per-task `Throwable` guards, cancellation and join map to the owned task control plane and `runtime/tasks.rs`; direct Java recording-executor and paused-clock cases are listed in the scheduler row above. |
| `cleanOfflineBroker`; `isBrokerAddrExistInTopicRouteTable`; `isBrokerInNameServer` | Route-table cleanup and address-retention checks map to the raw route table / offline-cleanup implementation; pinned null-map, null-list, malformed traversal, historical Broker and retained-address cases are recorded in [the route audit](PHASE_6_ROUTE_AUDIT.md). |
| `isNeedUpdateTopicRouteInfo` | Route refresh decisions map to `update_topic_route` and producer/consumer owner callbacks; equal-route forced publication and owner-change cases are recorded in the route audit. |
| `persistAllConsumerOffset` | The periodic runtime tick invokes the registered optional persistence capability and continues when absent; concrete offset stores remain Phase 9-owned. |
| `sendHeartbeatToBroker` (private `HeartbeatData` overload); `sendHeartbeatToAllBroker`; `sendHeartbeatToBrokerV2`; `sendHeartbeatToAllBrokerV2`; `sendHeartbeatToAllBrokerConcurrently`; `prepareHeartbeatData`; `resetBrokerAddrHeartbeatFingerprintMap`; generated concurrent-heartbeat lambda | Heartbeat snapshot construction, V1/V2 request selection, per-broker send, concurrency/executor snapshot, fingerprints and retry behavior map to `heartbeat.rs` and `tests/heartbeat.rs`; heartbeat source and activation cases are recorded in [the heartbeat audit](PHASE_6_HEARTBEAT_AUDIT.md). |
| `unregisterClient` | Producer/consumer group removal sends the source unregister request before transport teardown while preserving best-effort shutdown; scripted peer and shared-user shutdown evidence is in the lifecycle audit. |
| `findBrokerVersion` | Broker-version lookup maps to route snapshot metadata and is covered with subscribe/admin address selection in the runtime route lookup fixtures. |
| `waitResetOffsetReady` | Concrete Push consumer behavior is implemented in Phase 17: non-orderly reset waits for callback drain; orderly reset waits up to 10 seconds for the consume write lock before updating the offset. Phase 6 owns only the inbound group-routing boundary; final Broker behavior stays in Phase 19. |
| `MQClientInstance.lambda$topicRouteData2TopicPublishInfo$1`; `TopicPublishInfo.selectOneMessageQueue` (private overload) | The generated comparator maps to the Java-compatible route conversion/sort behavior in the route audit. Producer-local queue filter/index/selection remains Phase 7-owned with the rest of `TopicPublishInfo`; Phase 6 only owns route conversion and publication as stated below. |
| `ClientRemotingProcessor.processReplyMessage`; `pullMessage`; `popMessage` | Phase 6 routes reply/pull/pop requests to their registered owner boundary. Reply correlation/materialization is Phase 15-owned; pull/pop execution and message decoding are Phase 9/10-owned. |
| `ServiceThread.waitForRunning`; `onWaitEnd` (protected) | The protected JDK worker wait hooks are not exposed as a Rust public API. Controlled Java worker fixtures cover inherited start/stop mechanics; native runtime tasks use owned Tokio cancellation/notification, with concrete worker loops mapped in the runtime-worker table. |

This is a method-name/source-owner inventory, not a claim that later-phase methods are implemented here or that each Java private branch has full input parity. It closes the bounded non-public declaration scan for these reference classes; behavior evidence and downstream ownership remain as stated per row.

## Rust producer owner contract

The public `ProducerRuntime` trait is `ClientRuntime`'s Rust owner adapter for
Java's internal `MQProducerInner` callbacks. Its methods supply producer route
refresh and transaction-dispatch state.

| Rust adapter method | Java source method / behavior | Evidence / disposition |
| --- | --- | --- |
| `ProducerRuntime::publish_topics` | `MQProducerInner.getPublishTopicList`; Java returns all keys in `DefaultMQProducerImpl.topicPublishInfoTable`, including empty entries retained after route lookup failure. Rust includes namespaced constructor topics, attempted topic lookups and every topic whose route snapshot has been published to this Producer. | `producer::tests::configured_publish_topics_are_namespace_wrapped_once`; `producer::tests::publish_topics_include_cached_dynamic_topics_and_configured_topics`; `scripted_send::topic_without_route_stays_in_producer_refresh_inventory`; Phase 6 all-topic refresh tests. This inventory feeds periodic route refresh. |
| `ProducerRuntime::is_publish_topic_need_update` | `MQProducerInner.isPublishTopicNeedUpdate`; Java requests refresh when the entry is absent or `TopicPublishInfo.ok()` is false. | Producer's cached snapshot predicate uses missing or empty publish queues; Phase 6 route update callbacks and Phase 7 send-route evidence. |
| `ProducerRuntime::update_topic_publish_info` | `MQProducerInner.updateTopicPublishInfo`; Java stores the latest publish-info value by topic. | `ClientRuntime::update_topic_route` publishes accepted route snapshots to registered producer owners; Phase 6 route publication and Phase 7 send-route checks. |
| `ProducerRuntime::transaction_processor` | Java's `MQProducerInner.checkTransactionState` callback is invoked by `ClientRemotingProcessor.checkTransactionState`. Rust supplies the typed request processor when the TransactionProducer is configured. | Request code 39 routing is mapped above; actual transaction callback and state handling belong to Phase 14. |
| `ProducerRuntime::map_value_equality_key` | No Java method with this name. This optional Rust adapter value models Java owner `equals()` for conditional producer/consumer registry operations; lifecycle ownership still uses object identity. | Registry equality and mutable-key differential cases in the Phase 6 requirement audit. |

## Rust consumer owner contract

`ConsumerRuntime` and `MessageRequest` are runtime-owner adapters for Java's
internal `MQConsumerInner`, `MessageRequest`, `PullRequest` / `PopRequest`, and
`PullMessageService` flows. They keep consumer ownership and typed request
dispatch inside the Rust runtime rather than exposing a mutable Java
implementation object.

| Rust adapter method | Java source method / behavior | Evidence / disposition |
| --- | --- | --- |
| `ConsumerRuntime::heartbeat_data` | `MQClientInstance.prepareHeartbeatData` collects the consumer group, model, consume type, start-offset policy, unit mode, and subscriptions represented by the registered `MQConsumerInner`. | [Heartbeat audit](PHASE_6_HEARTBEAT_AUDIT.md) verifies Java `ConsumerData` construction, nullable subscriptions, and full/simple heartbeat branches; Push direct getters are in the Phase 10 matrix. |
| `ConsumerRuntime::is_subscribe_topic_need_update`, `update_topic_subscribe_info`, `persist_consumer_offset`, `try_rebalance` | `MQConsumerInner.isSubscribeTopicNeedUpdate`, `updateTopicSubscribeInfo`, `persistConsumerOffset`, and `tryRebalance` | The Phase 10 Push fixture checks route-update and pause behavior; Phase 11 tests cover active Push/Pull rebalance, assignment updates, and offset persistence. |
| `ConsumerRuntime::adjust_thread_pool` | `DefaultMQPushConsumerImpl.adjustThreadPool`; built-in Java consume services leave `incCorePoolSize` / `decCorePoolSize` as no-ops, and a replacement service receives the hook calls. | Phase 10 invoked fixture verifies the built-in no-op; the replacement-service owner regression verifies a custom decrease hook; explicit `updateCorePoolSize` remains mapped on the Push facade. |
| `PushConsumer::{get_consume_message_service,set_consume_message_service}`; `ConsumeMessageService` | `DefaultMQPushConsumerImpl.get/setConsumeMessageService` and all nine public methods of `impl.consumer.ConsumeMessageService`. Rust exposes a nullable `Arc<dyn ConsumeMessageService>` binding; primary-service replacement receives direct-consume, normal Pull submission, pool adjustment/update and shutdown calls. POP consumption remains on Java's separate private POP service. The concrete `ConsumeMessagePopOrderlyService` constructor and 15 declared public methods are inventoried; its active startup/shutdown/lock-renewal/POP-submission responsibilities map to the Push owner and POP worker, with no standalone Rust implementation type. | Phase 10 Java signature fixture and Rust owner/API regressions cover identity/null accessors, direct and Pull routing, custom pool hooks, shutdown, and POP separation. A pinned implementation-surface test records the POP-orderly constructor and method list; Java's public `submitConsumeRequest` and `processConsumeResult` are stubs, and its `ConsumeRequest.run` ends at the sharded lock lookup. |
| `ConsumerRuntime::pull_message`, `pop_message`; `MessageRequest::{consumer_group,mode}` | `PullMessageService` chooses `PullRequest` or `PopRequest` by `MessageRequest.getMessageRequestMode()` and invokes the registered consumer implementation; group selects the current runtime owner. | `runtime/pull.rs` maps FIFO, delayed scheduling, nullable group lookup and mode dispatch; `tests/pull_dispatch.rs::fifo_mode_dispatch_delay_and_group_selection_use_current_registration` checks the owner boundary. `MessageRequest::as_any` is the Rust downcast adapter for concrete request ownership, with no separate Java method. `PopRequest` now implements this trait and preserves a null group as `None`. |
| `ConsumerRuntime::inbound_processor` and `ConsumerOperation` | Consumer-group inbound operations in `ClientRemotingProcessor`, including Push reset offset, consumer status, running info, and consume-directly handlers | [Inbound matrix](PHASE_6_INBOUND_MATRIX.md) checks request-code routing and absent-handler outcomes; Phase 17 owns the active admin/query handlers. |
| `ConsumerRuntime::map_value_equality_key` | No Java method with this name. Like the producer adapter key, it models Java owner `equals()` only for conditional registry-map operations; lifecycle registration remains identity-based. | Registry equality and mutable-key differential cases in the Phase 6 requirement audit. |

`MessageRequest` carries only group/mode routing data at the runtime boundary;
concrete Push/Pull request state and RPC work remain with their consumer owners.
The trait methods do not add consumer configuration or a new Broker feature.

## RPC API methods consumed by the runtime

| Rust public API | Pinned Java API / behavior | Evidence / boundary |
| --- | --- | --- |
| `MQClientApi::{get_name_server_address_list,with_name_server_address_list,remoting_client}` | `MQClientAPIImpl.getNameServerAddressList()` delegates to the live remoting client's nullable NameServer list; `getRemotingClient()` returns that same remoting client. Rust returns the nullable list snapshot, a lock-scoped mutable alias adapter, and a cloneable handle to the same shared client. | `api_surface_tests::remoting_and_nameserver_getters_preserve_shared_client_state` mutates the NameServer list through the returned handle and clears it through the lock-scoped adapter, observing both changes through the API getter. |
| `MQClientApi::get_default_topic_route(deadline)` | `MQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(timeoutMillis)` queries the fixed auto-create topic `TBW102`; Rust uses the shared route RPC path and an absolute deadline | The invoked Java `RuntimeLookupFixtures.defaultTopicRouteApiCase` records two direct API calls through a proxy `RemotingClient`: success decodes the route, while `TOPIC_NOT_EXIST` returns `MQClientException` code 17; both requests carry code 105, topic `TBW102`, and the supplied timeout. `default_topic_route_api_matches_invoked_java_api_fixture` compares the Rust API request, decoded route fields, response code, and remark against that fixture. `default_route_fetch_uses_auto_create_topic_and_clamps_both_counts_from_read_count` separately checks route publication, rejected, missing-body, and invalid-JSON behavior. The higher-level Java `MQClientInstance.updateTopicRouteInfoFromNameServer(topic, isDefault, producer)` and producer fallback clamp are mapped in the runtime inventory; queue fallback/selection remains Phase 7-owned. |
| `MQClientApi::query_consume_time_span(addr, header, deadline)` | `MQClientAPIImpl.queryConsumeTimeSpan(addr, topic, group, timeoutMillis)` sends request 303, applies `brokerVIPChannel`, decodes `QueryConsumeTimeSpanBody`, and throws `MQBrokerException` for non-SUCCESS replies | `RuntimeLookupFixtures.queryConsumeTimeSpanApiCase` records both requests, decoded timestamp spans and `TOPIC_NOT_EXIST`; `query_consume_time_span_matches_java_invoked_fixture` checks code/header/body/error fields. Push's fresh route lookup, sequential route-order fan-out and 3000 ms per-call deadline are covered by `query_consume_time_span_queries_fresh_route_in_order_and_uses_vip_addresses`. Fixture `lookup.json` SHA-256 `612c0adbd099e5282e027b69a9996575d6efc8293a4b42c70c367da800637e0c`. |
| `MQClientApi::get_broker_config(addr, deadline)` | `MQClientAPIImpl.getBrokerConfig(addr, timeoutMillis)` sends request 26 with no header or body, passes the supplied address unchanged, decodes the successful response body through Java `MixAll.string2Properties`, and preserves Broker errors | `fixtures/producer/core.json:brokerConfigProperties` records empty, escaped, Unicode, malformed-escape, non-ASCII, and trailing-backslash inputs; Core's parser unit test compares Java property results and scripted RPC tests check request fields, missing-body handling, and Broker errors. Empty response bodies fail before parsing in Java; Rust reports a structured malformed-response error. The fixture checkpoint for this read operation is `53f8d3ae387f5df155d0c10484815f3a71d00debafb70c82ed0912f3848bca2f`; the current fixture also includes the adjacent operations below. |
| `MQClientApi::{clean_expired_consume_queue, delete_expired_commit_log, clean_unused_topic_by_addr}` | Pinned `MQClientAPIImpl.cleanExpiredConsumeQueue`, `deleteExpiredCommitLog`, and `cleanUnusedTopicByAddr` send empty requests 306, 329, and 316 through the configured VIP address transform. Each returns `true` only on `SUCCESS`; other response codes become RPC response errors. | `fixtures/producer/core.json:brokerMaintenanceApi` pins all three signatures; `brokerMaintenanceOperations` records request addresses/codes, empty fields/body, and success/error outcomes. `broker_maintenance_operations_match_java_requests_and_outcomes` compares the Java fixture, and `clean_expired_consume_queue_applies_configured_vip_port_offset` verifies configured VIP routing. |
| `MQClientApi::{update_cold_data_flow_ctr_group_config,remove_cold_data_flow_ctr_group_config,get_cold_data_flow_ctr_info,set_commit_log_read_ahead_mode}` | `MQClientAPIImpl.updateColdDataFlowCtrGroupConfig` / `removeColdDataFlowCtrGroupConfig` send request codes 2001/2002 to the configured VIP address; empty Properties and null/empty group inputs are no-ops. `getColdDataFlowCtrInfo` / `setCommitLogReadAheadMode` send request codes 2003/2004 to the supplied address, return nullable body/remark strings, and preserve Broker errors. Null read-ahead mode is omitted by Java's remoting wire encoder. | `fixtures/producer/core.json:coldDataFlowAndReadAheadApi` records all four signatures; `coldDataFlowAndReadAheadOperations` records no-op, request, nullable result, remark, and error branches. Core scripted RPC and VIP-enabled loopback tests compare the request bodies, ext fields, address choice and outcomes. At this slice checkpoint, producer core fixture SHA-256 was `c20f4474d674bd4e0001f19c82fed8c5e2dfbe6f5c911ee6eade0fb1842c7cb0`. |
| `MQClientApi::register_client(addr, heartbeat, deadline)` | `MQClientAPIImpl.registerClient(addr, HeartbeatData, timeoutMillis)` synchronously sends request 34 with an empty `HeartbeatRequestHeader` and the encoded heartbeat body to the supplied address, then returns `true` only for `SUCCESS`; any other Broker response returns `false` without becoming `MQBrokerException`. | `fixtures/producer/core.json:registerClientApi` records the exact public signature; `registerClientOperations` records success and non-success replies and request address/code/timeout/body. `register_client_returns_success_boolean_and_matches_java_request` compares the request body and both boolean results. The existing `send_heartbeat` returns Broker version and raises on non-success, so this is a distinct Java result contract despite sharing the request code. The fixture checkpoint for this slice is `434a3fe8b1cba71732f41ccb710e601e0d8ce4cbcf196265695cef7641b90905`; the current fixture also records the NameServer topic-list RPC below. |
| `MQClientApi::get_topic_list_from_nameserver(deadline)` | `MQClientAPIImpl.getTopicListFromNameServer(timeoutMillis)` sends request 206 without header/body through the configured NameServer selection path and decodes `TopicList`; non-success Broker/NameServer responses are preserved. | `fixtures/producer/core.json:topicListFromNameServerApi` records the exact signature; `topicListFromNameServerOperations` records the null-address Java call, empty request envelope, returned topic set/Broker address and response error. `topic_list_from_nameserver_matches_java_request_and_response` checks request code/language/body/header, decoded values, and Broker errors. A missing success body maps to structured `MalformedResponse`; Java throws `MQClientException` with response code 0. At this slice checkpoint, fixture SHA-256 was `6e0a6f94f934f9e173aa76a5e4e3ab18c70ee1121f41f1dfbb52fbbac303a2ef`. |
| `MQClientApi::{get_topics_by_cluster, get_topics_by_cluster_nullable}` | Pinned `MQClientAPIImpl.getTopicsByCluster(cluster, timeoutMillis)` sends request 224 through the configured NameServer selector with `cluster` in the request header, then decodes `TopicList`; non-success responses are preserved. Java accepts a null cluster because the header's `checkFields()` is a no-op; its nullable value is omitted from the wire fields. | `fixtures/producer/core.json:getTopicsByClusterApi` pins the exact Java signature; `getTopicsByClusterOperations` captures Java custom-header values for nonnull and null clusters and covers success, response error and missing-body outcomes. `get_topics_by_cluster_matches_java_nameserver_request_and_outcomes` compares Java's cluster value with the Rust wire ext field, including null omission, along with request code, language, topic list and error behavior. A missing success body maps to structured `MalformedResponse`; Java throws `MQClientException` with response code 0. Fixture SHA-256 at this slice checkpoint: `ac3462b7351f327839c4a79db5a8a499f6fdbbd56f667f74b2ac5f18fb368fee`. |
| `MQClientApi::get_system_topic_list_from_broker(addr, deadline)` | `MQClientAPIImpl.getSystemTopicListFromBroker(addr, timeoutMillis)` sends request 305 with no header/body, transforms the supplied Broker address through the configured VIP setting, and decodes `TopicList`. | `fixtures/producer/core.json:systemTopicListFromBrokerApi` records the exact signature; `systemTopicListFromBrokerOperations` records request address/code/timeout and success/error/missing-body responses. `system_topic_list_from_broker_matches_java_request_and_response` checks the envelope, decoded set, Broker error and missing-body branch against Java's code-0 `MQClientException`. A missing success body maps to structured `MalformedResponse`. A VIP-enabled Core loopback checks the address transform. Fixture SHA-256 at this slice checkpoint `fbe6f70615e8004c26bdba90fdc824241415d19557b57853a6a563f24becbd32`. |
| `MQClientApi::get_system_topic_list(deadline)` | `MQClientAPIImpl.getSystemTopicList(timeoutMillis)` sends request 304 through NameServer selection, decodes `TopicList`, and when the NameServer set is nonempty and its Broker address is nonblank, requests code 305 and merges the Broker topics into the retained NameServer topic set. | `fixtures/producer/core.json:systemTopicListApi` records the exact signature; `systemTopicListOperations` covers merge, empty/null NameServer sets, blank Broker address, NameServer errors/missing body and Broker errors. `system_topic_list_composes_nameserver_and_broker_responses_like_java` compares the Java call sequence, empty envelopes, timeout, merged topic set and result/error branches. Fixture SHA-256 at this slice checkpoint `38b2eac88f25aa04674f469b4e7a685dc599834969f6a1d24613de8c98efdb23`. |
| `MQClientApi::create_subscription_group_list(addr, configs, deadline)` | `MQClientAPIImpl.createSubscriptionGroupList(addr, configs, timeoutMillis)` sends request 225 with an empty header and Java `SubscriptionGroupList` body to the configured VIP address; `SUCCESS` returns and other response codes become `MQClientException`. | `fixtures/producer/core.json:createSubscriptionGroupListApi` records the exact signature; `createSubscriptionGroupListOperations` records populated, empty and Broker-error calls. `create_subscription_group_list_matches_java_requests_and_outcomes` compares the exact serialized body, request envelope, timeout, success and error; a VIP-enabled loopback verifies the configured address offset. Current fixture SHA-256 `2479e9af49895298bf807b04775db0e3acef6ad2b6a0a6e2cf59a830ca89169e`. |
| `MQClientApi::{get_subscription_group_config,get_subscription_group_config_nullable}` | Pinned `MQClientAPIImpl.getSubscriptionGroupConfig(addr, group, timeoutMillis)` sends request 352 with nullable `group`, preserves the inherited `RpcRequestHeader` fields, applies configured VIP conversion, and decodes `SubscriptionGroupConfig`; an absent success body returns null and non-success replies become response errors. | `fixtures/producer/core.json:subscriptionGroupConfigApi` pins the direct signature and `subscriptionGroupConfigOperations` covers populated/default bodies, null/empty groups, missing body and Broker errors. The `GetSubscriptionGroupConfigRequestHeader` Java→Rust and Rust→Java fixture pair checks all inherited fields and is included in the 73-frame typed header inventory. The Core differential compares request/result behavior; a VIP-enabled loopback verifies address conversion. Producer core fixture SHA-256 is recorded in the release checklist. |
| `MQClientApi::{create_topic_list, create_topic_list_nullable}` | Pinned `MQClientAPIImpl.createTopicList(addr, configs, timeoutMillis)` sends request 18 with a Java `CreateTopicListRequestBody` through the configured VIP address; `SUCCESS` returns and other codes become `MQClientException`. The nullable adapter preserves Java's null list and null list-element behavior. | `fixtures/producer/core.json:createTopicListApi` pins the signature; `createTopicListOperations` records populated/default/custom `TopicConfig` values, empty list, null list, null element and Broker error. The Rust differential compares body bytes, envelope, response and error; a VIP-enabled loopback checks the two-port address offset. Current fixture SHA-256 `fcf27463aa5fde04867de50543847ad53dabc1b5c3b526ae8ac2406481514ec7`. |
| `MQClientApi::{delete_topic_in_broker_list, delete_topic_in_broker_list_nullable}` | Pinned `MQClientAPIImpl.deleteTopicInBrokerList(addr, topics, timeoutMillis)` sends request 5002 with a Java `DeleteTopicListRequestBody` through the configured VIP address. The nullable adapter preserves Java's null list and null elements. | `fixtures/producer/core.json:deleteTopicInBrokerListApi` pins the signature; `deleteTopicInBrokerListOperations` covers populated, empty, null-list, null-element and Broker-error calls. `delete_topic_in_broker_list_matches_java_requests_and_outcomes` compares body bytes, envelope and outcomes; the batch-delete VIP loopback checks the port transform. |
| `MQClientApi::{delete_subscription_group_list, delete_subscription_group_list_nullable}` | Pinned `MQClientAPIImpl.deleteSubscriptionGroupList(addr, groupNames, cleanOffset, timeoutMillis)` sends request 5003 with a Java `DeleteSubscriptionGroupListRequestBody` through the configured VIP address. The body retains `cleanOffset`; its nullable adapter preserves Java's null list and null entries. | `fixtures/producer/core.json:deleteSubscriptionGroupListApi` pins the signature; `deleteSubscriptionGroupListOperations` covers both clean-offset modes, empty, null-list, null-element and Broker-error calls. `delete_subscription_group_list_matches_java_requests_and_outcomes` compares body bytes, envelope and outcomes; the batch-delete VIP loopback checks the port transform. Current fixture SHA-256 `33b7788c15092caaeb2e5cc42ac69eb7bcda5baf5ebc27c2caf01aac9c5e9ff3`. |
| `MQClientApi::{get_unit_topic_list, get_has_unit_sub_topic_list, get_has_unit_sub_ununit_topic_list}` | Pinned `MQClientAPIImpl.getUnitTopicList`, `getHasUnitSubTopicList` and `getHasUnitSubUnUnitTopicList` send requests 311, 312 and 313 through NameServer selection with no header/body and decode `TopicList`. All three keep `%RETRY%` topics when `isUnitMode` is true and remove them when false; response errors are preserved. | `fixtures/producer/core.json:unitTopicListsApi` records the signatures; `unitTopicListsOperations` covers both boolean values for all three calls, response errors and missing bodies. `unit_topic_lists_match_java_nameserver_requests_and_filtering` compares request codes/envelopes, decoded topics and Broker address, filtering, and error branches. Java throws `MQClientException` code 0 for `SUCCESS` without a body; Rust returns structured `MalformedResponse`. Current fixture SHA-256 `bb5a19cb131f922828ff5effb8eb52ac4a0d95e8bae2863775214da4d6664e5b`. |
| `MQClientApi::{update_name_server_config, get_name_server_config}` | Pinned `MQClientAPIImpl.updateNameServerConfig` sends request 318 and UTF-8 `key=value\n` Properties to each explicit or configured NameServer; empty properties are a no-op. It visits all servers and preserves the last non-success response. `getNameServerConfig` sends request 319 with no body to each selected address, parses Java Properties, and stops at the first response error. Missing/empty lists use the configured list; no selected servers returns `None` for reads. | `fixtures/producer/core.json:nameServerConfigApi` records the signatures; `nameServerConfigOperations` covers no-op/fallback/explicit selection, multiple updates with last-error selection, per-server results, empty/malformed properties, first-error stop, missing body and no-server behavior. `update_name_server_config_matches_java_fanout_and_error_selection` compares request codes, body property values, order and errors; `get_name_server_config_matches_java_results_and_short_circuiting` checks per-address maps and errors. Java's missing successful read body throws `NullPointerException`; Rust returns structured `MalformedResponse`. Malformed Properties remain null map values. Current fixture SHA-256 `4276123552ab57632da4a10177b1b529266707234e95785668cfcd12177e783c`. |
| `MQClientApi::{get_kv_config_value, put_kv_config_value, delete_kv_config_value, get_kv_list_by_namespace}` | Pinned `MQClientAPIImpl` NameServer KV methods map requests 101, 100, 102 and 219. Get/list select a configured NameServer; put/delete broadcast across the configured list, visit every server, and preserve the last non-success response. Nullable header fields, response `value`, and absent `KVTable` body match Java. These direct operations are separate from `MqClientAdmin.deleteKvConfig`, which takes one explicit NameServer address. | `fixtures/producer/core.json:kvConfigApi` pins all four method signatures and `kvConfigOperations` records nullable results, response errors, fan-out order, last-error choice, and no-server behavior. `name_server_kv_config_operations_match_java_requests_and_outcomes` compares the Rust request envelopes/fields and outcomes. The four public header models have Java→Rust and Rust→Java typed-frame fixtures; pinned Java verification decodes all 72 header frames. Producer core fixture SHA-256: `7e6aecbca17b9711ed3276de4aae2c500fdd8f90bdeb1bde7a7c59b0d3c402af`. |
| `MQClientApi::{resume_check_half_message, resume_check_half_message_nullable}` | Pinned `MQClientAPIImpl.resumeCheckHalfMessage(addr, topic, msgId, timeoutMillis)` sends request 323 with nullable `topic`/`msgId` fields to the configured VIP Broker address and returns `true` only for `SUCCESS`; every other Broker response returns `false`. | `fixtures/producer/core.json:resumeCheckHalfMessageApi` records the Java signature; `resumeCheckHalfMessageOperations` covers success, non-success, each nullable field and both fields null. The Rust fixture differential compares request 323, header omission, empty body, boolean result and VIP address offset in a loopback. Producer core fixture SHA-256 `bbeea411ebb9a1cbf177a4f0efddbf85023a475288bd123764959acca248efc6`. |
| `MQClientApi::export_pop_record(addr, deadline)` | Pinned `MQClientAPIImpl.exportPopRecord(addr, timeoutMillis)` sends request 200056 (registry name `POP_ROLLBACK`) without a header or body, uses the supplied address unchanged even when VIP is enabled, and raises on non-success responses. | `fixtures/producer/core.json:exportPopRecordApi` pins the signature; `exportPopRecordOperations` covers success/error, address, timeout and empty request envelope. `export_pop_record_matches_java_fixture_and_keeps_supplied_address_with_vip_enabled` compares both outcomes and verifies that the VIP-enabled API connects to the exact address supplied. Producer core fixture SHA-256 `d94fd1f95eba2caf6ce77e312cd53a4109be74048b23db5ad694a2c5b7109242`. |
| `MQClientApi::switch_timer_engine(addr, engine_type, deadline)` | Pinned `MQClientAPIImpl.switchTimerEngine(addr, engineType, timeoutMillis)` sends request 5001 with `E_T` in ext fields, uses the supplied address unchanged even when VIP is enabled, and raises on non-success responses. | `fixtures/producer/core.json:switchTimerEngineApi` records the signature; `switchTimerEngineOperations` covers nonempty and empty `E_T` values plus success/error. `switch_timer_engine_matches_java_fixture_and_keeps_supplied_address_with_vip_enabled` compares request code, field, body, address and outcomes. Producer core fixture SHA-256 `bb28b038fdd66160007d8c757e53ed4cb78ebcad0a91b4ff9d950fa04eccee6c`. |
| `MQClientApi::get_consumer_list_by_group_nullable(addr, Option<&str>, deadline)` | Java `MQClientAPIImpl.getConsumerIdListByGroup(addr, consumerGroup, timeoutMillis)` has one nullable String parameter, not a separate overload. A null group is omitted from ext fields by the Java request-header encoder; empty string remains explicitly encoded. | Direct Java API/runtime cases plus the Rust lookup/wire differential cover null versus empty group, request fields, response DTO decoding, and VIP/timeout behavior. This is a Rust nullable-argument adapter for the Java method, not an additional client capability; non-null callers use `get_consumer_list_by_group`. |

## `ClientRemotingProcessor` public methods

Pinned Java 5.5.1 declares one constructor and five public methods. The constructor is privately owned by `ClientRuntime` and registered with the remoting client; the methods map as follows:

| Java method | Rust counterpart / owner | Evidence and status |
| --- | --- | --- |
| `processRequest(ctx, request)` | `RequestProcessor::process/process_with_context` → `RuntimeProcessor::dispatch` | [Inbound matrix](PHASE_6_INBOUND_MATRIX.md) verifies all eight registered request-code routing slots, channel context, one-way silence and explicit inactive errors; slot routing only, not handler compatibility. |
| `checkTransactionState(ctx, request)` | Producer-group lookup using stored-message `PGROUP` → optional `ProducerRuntime` transaction processor | Matrix code 39; decoder/routing metadata is covered, transaction execution is Phase 14. |
| `notifyConsumerIdsChanged(ctx, request)` | Wake the runtime-wide rebalance worker; `consumerGroup` is optional and used only in Java logging; return no response, including before worker startup | Matrix code 40; direct Java invocation observes the sticky pre-start wakeup permit and null result with/without group; paused-time Rust verifies the permit survives Created state and wakes all consumers after startup; loopback verifies wire silence. |
| `resetOffset(ctx, request)` | Group lookup → `ConsumerOperation::ResetOffset` | Matrix code 220 routes to the active Push reset handler implemented in Phase 17; Pull has no reset operation. Broker-backed verification remains in Phase 19. |
| `getConsumeStatus(ctx, request)` | Group lookup → `ConsumerOperation::ConsumerStatus` | Matrix code 221 routes to the active Push status handler implemented in Phase 17. Broker-backed verification remains in Phase 19. |
| `ClientRemotingProcessor(mqClientFactory)` | Runtime-owned `RuntimeProcessor` registration and weak runtime binding | Lifecycle rollback removes the registered processor before transport close; constructor is not a user-facing Rust API. |

## `TopicPublishInfo` source-to-owner map

Phase 6 owns `MQClientInstance.topicRouteData2TopicPublishInfo` route conversion and publication. Its Java conversion fills `topicRouteData`, `orderTopic`, and `messageQueueList`; route refresh marks `haveTopicRouterInfo=true` when it publishes an accepted route to registered producers. The actual Java `topicPublishInfoPublication` oracle confirms this is separate from `ok()`: an accepted route publishes `haveTopicRouterInfo=true` with a non-null raw route and zero queues, while a missing route yields no callback. Native `TopicSnapshot` carries the route, publish queues, and ordered-route flag; the route snapshot's presence is distinct from an empty queue list and is verified by the loopback regression named above. Phase 7 must preserve that distinction when building producer-local state and deciding whether to use default-topic fallback. Phase 7 owns the remaining producer-local `TopicPublishInfo` surface: the no-argument constructor; `isOrderTopic`/`setOrderTopic`, `getTopicRouteData`/`setTopicRouteData`, `isHaveTopicRouterInfo`/`setHaveTopicRouterInfo`, `getSendWhichQueue`/`setSendWhichQueue`, `ok`, `getMessageQueueList`/`setMessageQueueList`, the `QueueFilter` API, all three `selectOneMessageQueue` overloads, `resetIndex` and its `ThreadLocalIndex` dependency, `getWriteQueueNumsByBroker`, and diagnostic `toString`. No Rust `TopicPublishInfo` type or queue-selection parity is claimed by Phase 6; these source methods are audited with the producer owner in [Phase 7](../../plan/2026/09/08-phase-07-producer-core.md).

## Assignment response DTO input

The runtime's Java `QueryAssignmentResponseBody.decode` returns a set of `MessageQueueAssignment` rows. Rust uses `QueryAssignmentResponseBody.message_queue_assignments: Option<SharedValue<HashSet<Option<SharedQueryAssignment>>>>` to preserve nullable set members, match stable Java same-mode hash-bucket collisions during JSON decode, accept Java's object-to-singleton input, and retain both outer Set and element aliases. Each assignment's nested `message_queue` and `attachments` fields are shared handles as well; caller/getter identity and both mutation directions match Java. Direct API tests verify outer, element, and nested field identity; the core runtime snapshots the Set and elements to preserve its existing owned result type. The current 40-case response corpus (33 Set-field cases plus seven whole-root shapes) covers top-level field token shapes including fraction rejection, array-element token shapes, nested `MessageQueue` field token shapes, and duplicate root, assignment-mode, and nested queue fields; [ADR-0019](../architecture/ADR-0019-assignment-response-set-input.md) records the Set boundary. Java's `MessageQueueAssignment.equals` compares only the queue while `hashCode` also uses mode and attachments; stable same-mode collisions are matched during JSON decode while the DTO retains lawful all-fields `Eq`/`Hash`. Different-mode equal-hash outcomes still depend on process-local enum identity hashes. The response body itself inherits Java Object identity. Each mutable assignment object inside the Set now preserves identity under `d312bb6c` and [ADR-0068](../architecture/ADR-0068-shared-query-assignment-elements.md).

The request DTOs used by runtime assignment APIs are also input-decoded. Ten enum cases plus 35 field-token and seven root-token cases cover `QueryAssignmentRequestBody`; fourteen enum/default cases plus 28 field-token and seven root-token cases cover `SetMessageRequestModeRequestBody`. Java scalar/object/array-to-String conversion, enum rejection/default behavior, primitive integer conversion (including object-to-zero), duplicate last-wins behavior, and accepted/rejected root shapes match the pinned Fastjson oracle. Native tests are `query_assignment_request_enum_inputs_match_invoked_java_fastjson`, `query_assignment_request_field_shapes_match_invoked_java_fastjson`, `query_assignment_request_root_shapes_match_invoked_java_fastjson`, `set_request_mode_field_shapes_match_invoked_java_fastjson`, and `set_request_mode_root_shapes_match_invoked_java_fastjson`; `set_request_mode_inputs_match_invoked_java_fastjson` covers the 14 enum/default cases. Current `lookup.json` SHA-256: `29b0f8811c1024a7423919dba48e9831432fa20618e5a3b389bb1967cef69f02`.

The runtime's Java `GetConsumerListByGroupResponseBody.decode` returns a nullable `List<String>`. The protocol DTO stores `Option<SharedValue<Vec<Option<String>>>>`, preserving the exact mutable List alias supplied to its setter and returned by its getter; the RPC API snapshots to `Option<Vec<Option<String>>>` at the established owned result boundary. Order, duplicates, null members, and recorded Fastjson conversions are preserved. Twenty-three actual Java input/re-encode cases cover field coercions, duplicate-field last-wins behavior, and root shapes [ADR-0018](../architecture/ADR-0018-consumer-list-fastjson-input.md); direct identity and bidirectional mutation cases cover the public alias [ADR-0061](../architecture/ADR-0061-shared-consumer-list-alias.md).

| Java field/API | Rust counterpart | Evidence / disposition |
| --- | --- | --- |
| `MessageRequestMode.getName` | `MessageRequestMode::name` | Two actual Java enum cases verify `PULL` and `POP`; [ADR-0021](../architecture/ADR-0021-request-mode-name-method.md). |
| `MessageQueueAssignment.mode` | `Option<MessageRequestMode>`; `Default` uses `Some(Pull)` | Twenty actual Java cases cover missing/default, explicit null, case-insensitive enum names, numeric ordinals, integer numeric strings, numeric fractions, exact decimal truncation/wrapping, invalid names, and rejection bounds. Native decode and re-encode match; [ADR-0016](../architecture/ADR-0016-assignment-mode-fastjson-input.md) records the tested limits. |
| `MessageQueueAssignment.attachments` | `Option<SharedAssignmentAttachments>` (`SharedValue<BTreeMap<String, Option<String>>>`) | Seven actual Java cases cover scalar-to-string conversion, exact decimal text/exponent formatting, array/object text, null map entries, and invalid top-level shapes. Direct API cases verify setter/getter handle identity and both mutation directions. Null entries survive decode and are omitted during Java-compatible re-encoding; map iteration order is not compared. |
| `MessageQueueAssignment.message_queue` and accessors / `equals` / `toString` | `Option<SharedAssignmentMessageQueue>` plus shared-handle getters/setters, `java_equals`, `Display`/`java_to_string` | Direct Java cases cover queue alias identity and both mutation directions, attachments alias identity and both mutation directions, queue-only equality, null and wrong-type behavior, and exact text; see [ADR-0020](../architecture/ADR-0020-assignment-method-api.md) and [ADR-0066](../architecture/ADR-0066-shared-assignment-nested-aliases.md). Rust's all-fields lawful `Eq`/`Hash` uses value snapshots; mutation after HashSet insertion retains the Java mutable-key hazard. JSON decoding matches stable same-mode attachment-hash collisions; different-mode collisions still depend on process-local enum identity hashes. |

## Check-client request body Fastjson input

| Java DTO | Rust counterpart | Evidence / open surface |
| --- | --- | --- |
| `CheckClientRequestBody` | Nullable fields and `client_id`/`group`/`subscription_data`/`namespace` getters/setters, with shared raw-object last-wins decoding; `subscription_data` uses `SharedValue<SubscriptionData>` so cloned handles retain the same mutable object | Direct Java calls prove setter/getter pointer identity and mutation visibility in both directions; `CheckClientRequestBody.equals` remains identity-based, with no Rust structural equality traits. The 43 actual Java inputs include a seven-token-kind matrix for all four fields, seven root shapes, duplicate-last handling, nested `SubscriptionData` replacement, scalar-to-string conversion, explicit-null omission, unsupported scalar nested data, and ignored unknown fields. The native differential matches acceptance and normalized values; exception classes and other protocol DTO inputs remain outside this evidence. See [ADR-0042](../architecture/ADR-0042-check-client-body-fastjson-input.md) and [ADR-0060](../architecture/ADR-0060-shared-java-dto-reference-aliases.md). |
| `CheckClientRequestBody` inherited `RemotingSerializable` methods | `java_encode`, `java_to_json`, `java_decode*`, `java_decode_list*`, `java_from_json*`, `java_encode_with_features`, `java_encode_optional`, and `java_to_json_optional` | The invoked fixture pins all nine inherited Java signatures and compares default, explicit-null, and populated values for instance/static compact and pretty JSON, bytes, decode, list decode, and `fromJson`. Null bytes/strings, JSON `null`, null list input, and null list members are covered. It inventories all 48 `JSONWriter.Feature` values and checks feature outputs; all 48 are now supported by the mapped DTO adapters. See the consolidated feature behavior and current validation below. Pinned `CheckClientRequestBody.java` SHA-256 `769c533d9d6940c97ffcca009b6b2089a1e2c6eb251b517b9dbb247f5085505a`; current `fixtures/runtime/lookup.json` SHA-256 `73c5d1ea66505a17baa5075a28d2ce9c85b32e5649dc6685e1580b1ef5a43ea7`. |

## BatchAck API and inherited serialization

| Java public surface | Rust counterpart | Evidence / remaining scope |
| --- | --- | --- |
| BatchAck() and its 19 declared public methods | Default, the nine snake_case getter/setter pairs, and Display | The invoked Java fixture pins the constructor and all method signatures. Rust checks the same method-name set and compares defaults, all field values, BitSet aliasing, identity equality, and toString output in batch_ack_public_methods_match_invoked_java; see ADR-0053. |
| BatchAckMessageRequestBody(), four declared methods, and 13 callable methods including inherited RemotingSerializable methods | Default, bean accessors, java_to_json, java_encode, java_encode_with_features, java_decode*, java_decode_list*, java_from_json*, plus java_encode_optional/java_to_json_optional for inherited static null behavior | The Java reflection fixture pins constructors, declared signatures, and the inherited callable method inventory. Three actual Java values compare compact/pretty instance and static JSON, bytes, empty-feature encoding, decode, list decode, and fromJson. Null bytes/strings, JSON null, null list input/members, and static encode/toJson null behavior are checked. |
| BatchAckMessageRequestBody.decode/encode Fastjson behavior | Rust `java_decode_nullable`/`java_encode`/`java_encode_with_features` with the existing typed DTO model | All 111 direct Java input/re-encode rows run through `java_decode_nullable`. Serializer cases cover omitted versus explicit-null defaults and a nullable ACK list row. The fixture records all 48 inherited writer features for default, null, and populated values, and Rust compares them against the mapped adapter behavior; none remain unmapped. `WriteLongAsString` stringifies `BatchAck` long fields; `BrowserCompatible` preserves safe-range integers and stringifies values outside JavaScript's exact integer range; `OptimizedForAscii` preserves fixture bytes; `WriteNameAsSymbol` preserves UTF-8 text output; class-name suppression, field-name handling, and combined boolean precedence match the pinned Java fixtures. Fixture source `BatchAckMessageRequestBody.java` SHA-256: `11b86eb41d3d427e4400aa6741916d57f5f7d035f5b2dec3c86c56b0f2688463`; current aggregate `fixtures/runtime/lookup.json` SHA-256: `73c5d1ea66505a17baa5075a28d2ce9c85b32e5649dc6685e1580b1ef5a43ea7`. |

## Route DTO public methods

The Java no-argument DTO constructors map to Rust `Default`; copy constructors map to owned `Clone` or `TopicRouteData::java_copy_constructed` where Java null-collection defaults differ. Parameterized constructor fields map to Rust struct construction, and Java bean getters/setters map to Rust field access plus the snake_case methods listed in the DTO rows. For `TopicRouteData` and `TopicQueueMappingInfo`, inherited `RemotingSerializable` JSON/byte methods map to the Java-compatible adapters below rather than plain Serde output. The method rows below track behavior requiring more than field access.

Route DTO accessor differential (2026-10-09): an invoked Java 5.5.1 fixture now calls every setter and getter pair on `BrokerData` (5 fields), `QueueData` (5), `TopicQueueMappingInfo` (7), and `TopicRouteData` (5), with populated and null/default snapshots. The Rust integration test reads the Java getter results and compares each corresponding public-field projection, including nullable maps/lists, numeric map keys, and primitive values. Fixture SHA-256 `5b146407b8001de64dd1ad4612d521697e5539a39686e22e43dc4b626de46dcf`; Maven drift verification and `route_dto_public_fields_match_invoked_java_getter_setter_values` pass. This closes those four DTO accessor rows; it does not close other DTO inputs or whole Phase 6 reconciliation.

| Java DTO method | Rust counterpart | Evidence / open surface |
| --- | --- | --- |
| Inherited `RemotingSerializable` methods | `java_to_json`, `java_encode`, `java_decode`, `java_decode_list`, `java_from_json`, plus nullable decode/list/fromJson adapters on `TopicRouteData` and `TopicQueueMappingInfo` | Six direct Java cases cover default, explicit-null, and populated values per DTO. Compact/pretty JSON, encoded bytes, decode/list decode/fromJson are compared; numeric map keys use Fastjson2's unquoted output form. Null utility cases cover null roots and `[null]`; route RPC decoding uses `TopicRouteData::java_decode_nullable`. HashMap/ConcurrentHashMap iteration order remains unspecified by Java. Current route fixture SHA-256 `d53061010582605048578e1d4d7a794764ce5be14a4616cd9c1db72e1a17b725`; see ADR-0039. |
| Route DTO Fastjson input | `rocketmq_classic_common::serde_fastjson` field deserializers plus the private Java-compatible route parser | 527 invoked Java cases cover scalar conversions, integer-string coercion/rejection, seven JSON token kinds for every field in four route DTOs, nested DTO list elements/map values, filter-list/string/object values, quoted and unquoted numeric keys across numeric and string-keyed route maps, syntax/shape rejection, Unicode decimal rejects, numeric tokens, whitespace/BOM behavior, exact-case property matching, duplicate-last fields, singleton List objects/null rows, numeric map-key normalization collisions, and quoted/unquoted null keys, plus 128 raw U+0000–U+001F string probes across route values, field names, filter-map keys, and filter-map values. Java preserves those null Long keys in `BrokerData.brokerAddrs` but rejects null Integer keys in `currIdMap`; native `Option<i64>` keys match. Probes confirm leading zeros, plus signs, fractions, and exponents are accepted in map keys and converted according to the Java map key type; unquoted numeric bean properties reject. The field matrix caught and fixed empty-object acceptance for `TopicQueueMappingInfo.epoch`; `currIdMap` object value `{}` maps to zero while null/array values reject. Filter-server map string/object values are converted to singleton Java strings, and empty string becomes an empty list. Numeric keys in String maps use Java number formatting, including exponent-to-`100.0`; object-to-String coercion emits nested numeric keys as Fastjson does. Colliding textual `Long`/`Integer` keys retain the last property occurrence, including interleaved exact duplicates. Uppercase/title-case/snake-case/case-collision property spellings are ignored. The parser normalizes tested numeric forms, form feed and a leading BOM; rejected inputs retain typed Rust errors rather than Java exception classes. Actual-runtime publication matrices cover the 256 absent/null/empty/populated outer-collection states and the full 625-case five-state product across four route collections. A five-case registered-consumer matrix also compares outcome, cached collection sizes, update checks, and subscription-update callbacks over the route RPC path. Current whole-file `route.json` SHA-256 is `1ac509576845e9f9d78fc4c677702b6c21cf1195d711cb0437dacb48e8e14da1`; the 399-case DTO-input checkpoint was `0472ed8171207d35519adae7521741d359e343a3c378cf75537cb5c29cb41313`. See [ADR-0014](../architecture/ADR-0014-route-dto-fastjson-input.md) and [ADR-0041](../architecture/ADR-0041-route-dto-list-fastjson-input.md); other input grammar remains open. |
| `BrokerData.selectBrokerAddr` | `BrokerData::java_select_broker_addr -> Result<Option<String>, BrokerDataSelectAddrError>` | Ten actual Java cases cover master-first selection, all random fallback values, null master/address values, null address map NPE and empty-map IAE. Rust returns `None` for a selected null address and typed errors for the two exception branches. |
| `MessageQueue()` / copy / `(topic, brokerName, queueId)` constructors; getters/setters | `Default` / `Clone` / `new_nullable` (and `new` for non-null strings); `topic`, `broker_name`, `queue_id` and matching setters | Actual Java method fixtures cover no-argument defaults, copy construction, populated/null bean accessors and constructor-created comparison operands. `message_queue_hash_equals_and_text_match_invoked_java` now constructs both Rust forms from all three Java default/populated/Unicode rows; the route/config differentials cover setters and values. |
| `MessageQueue.equals` / `hashCode` / `toString` | derived value `PartialEq`/`Eq`, `java_hash_code`, `Display` | Three actual Java cases cover default, populated and Unicode/boundary values, self/copy/null/wrong-type equality, and field-by-field inequality; Rust comparison is in `message_queue_hash_equals_and_text_match_invoked_java`. |
| `MessageQueue.compareTo(MessageQueue)` (compiler bridge `compareTo(Object)`) | `java_compare_to -> Result<i32, MessageQueueCompareError>`; `java_compare_to_nullable` maps the null argument | Eleven actual Java pairs invoke the typed method and cover null topic/broker exceptions, UTF-16 order, prefixes, queue-id bounds and equality; an invoked runtime case records `NullPointerException` for a null `other`, matched by `MessageQueueCompareError::NullOther`. `message_queue_compare_to_matches_invoked_java` checks each result or error branch. Bytecode shows the erased bridge first casts `Object` to `MessageQueue` (wrong runtime type throws `ClassCastException`); Rust's typed method cannot receive a wrong-type value. |
| `MessageQueueForC(String, String, int, long)`, bean getters/setters, `equals` / `hashCode` / `toString`, `Comparable<MessageQueueForC>` | `message::MessageQueueForC::new`, read/write accessors, `PartialEq`/`Eq`, `java_hash_code`, `Display`, and `java_compare_to` / `java_compare_to_nullable` | Invoked `RuntimeRouteFixtures` compares populated values, setter outcomes, offset-sensitive equality, the Java hash that omits offset, text output, 12 comparison pairs (UTF-16 strings and wrapping `int`/`long` subtraction), and null failures; `message_queue_for_c_methods_match_invoked_java` checks the fixture. Java also implements `Serializable`; Java object-stream compatibility is not part of this Rust value mapping. Java uses the type in Broker2Client's C-client reset-offset request and `ResetOffsetBodyForC`; this row does not claim that C-specific wire path is implemented in the Rust client. |
| `BrokerData`, `QueueData`, `TopicQueueMappingInfo`, `TopicRouteData` getters/setters | Java bean methods map to the corresponding Rust public fields and named accessors | All 22 field pairs were directly invoked in populated and null/default states; the Rust accessor differential compares every field, including nested route collection shape. Mutable map aliases for `BrokerData.brokerAddrs` and `TopicQueueMappingInfo.currIdMap` are covered by [ADR-0075](../architecture/ADR-0075-shared-route-map-aliases.md); four `TopicRouteData` collection aliases are covered by [ADR-0076](../architecture/ADR-0076-shared-topic-route-data-collections.md). |
| `BrokerData.brokerAddrs` / `TopicQueueMappingInfo.currIdMap` setter and getter aliases | `get_broker_addrs` / `set_broker_addrs` and `get_curr_id_map` / `set_curr_id_map`, backed by `SharedValue`; `_owned` setters retain an owned-map convenience | Pinned Java calls show each setter retains the exact mutable Map and each getter returns that same object. Fixture-backed Rust calls compare `ptr_eq` and mutations through caller/getter handles in both directions. DTO `Clone` still deep-copies the map, `PartialEq` compares map content, and JSON keeps the existing shape. Other route DTO fields and APIs remain under audit; see [ADR-0075](../architecture/ADR-0075-shared-route-map-aliases.md). |
| `BrokerData.equals` | `PartialEq`/`Eq`, comparing broker name and address map | Six actual Java pairs cover metadata-only changes, name/address changes, null versus empty maps, and null address values; `TopicRouteData` equality inherits it. |
| `BrokerData.hashCode` | `BrokerData::java_hash_code` | Three actual Java values verify Java map/Long/String hashing, including nulls, supplementary Unicode, and signed 64-bit keys. |
| `BrokerData.compareTo` | `BrokerData::java_compare_to -> Result<i32, BrokerDataCompareError>`; `java_compare_to_nullable` maps the null argument | Six actual Java pairs cover raw UTF-16 differences, prefixes, equal names and null-name exceptions; an invoked runtime case records `NullPointerException` for a null `other`, matched by `BrokerDataCompareError::NullOther`. `RouteCache` broker sorting calls this comparator and matches three actual `topicRouteDataChanged` cases. Other DTO/input audit rows remain open. |
| `BrokerData.toString` | `BrokerData::java_to_string` | Two actual Java cases cover default null fields and populated name/address/acting-master values, including a null address; map output uses deterministic Rust key order because Java `HashMap` order is unspecified. |
| `QueueData.equals` | Derived `PartialEq`/`Eq`, comparing all five fields | Five actual Java method cases cover equality along with the related hash, string and comparison methods. |
| `QueueData.hashCode` | `QueueData::java_hash_code` | Exact Java prime-31 and UTF-16 name hashing is captured with null/default and Unicode values. |
| `QueueData.compareTo` | `QueueData::java_compare_to -> Result<i32, QueueDataCompareError>`; `java_compare_to_nullable` maps the null argument | Java raw UTF-16 difference and null-name exception are captured; an invoked runtime case records `NullPointerException` for a null `other`, matched by `QueueDataCompareError::NullOther`; route sorting uses this method. |
| `QueueData.toString` | `QueueData::java_to_string` | Exact Java diagnostic text is captured for null and populated fields. |
| `TopicQueueMappingInfo.equals` | Derived `PartialEq`/`Eq`, comparing all fields | Four actual Java pairs cover constructor defaults, null versus empty ID maps, populated maps and dirty-state differences. |
| `TopicQueueMappingInfo.hashCode` | `TopicQueueMappingInfo::java_hash_code` | Exact Java String/primitive/map hash operations match all four invoked pairs. |
| `TopicQueueMappingInfo.toString` | `TopicQueueMappingInfo::java_to_string` | Captured text matches; Rust prints currIdMap by key order, while Java ConcurrentHashMap iteration order is unspecified. |
| `TopicQueueMappingInfo.currIdMap` JSON entries | `Option<SharedTopicQueueMappingIds>` (`SharedValue<BTreeMap<i32, i32>>`) | Six actual Java inputs reject null/malformed keys or values and accept i32 boundary entries; native decode matches acceptance. Rust reports serde errors for rejected inputs instead of Java's `NullPointerException`/Fastjson `JSONException`. Shared wrapping preserves JSON shape; its API alias behavior is covered by [ADR-0075](../architecture/ADR-0075-shared-route-map-aliases.md). |
| `TopicRouteData.equals` | Derived `PartialEq`/`Eq` over nested Java-compatible DTO equality | Four actual Java pairs cover defaults, null/empty collections, ignored BrokerData metadata and queue differences. |
| `TopicRouteData.hashCode` | `TopicRouteData::java_hash_code` | Four actual Java pairs verify nested list/map/string hashes and null collection handling. |
| `TopicRouteData.toString` | `TopicRouteData::java_to_string` | Three actual Java cases cover constructor defaults, explicit null collections and populated queue/broker/filter/mapping DTOs; nested map output uses deterministic Rust key order because Java `HashMap` order is unspecified. |
| `TopicRouteData.topicRouteDataChanged` | `TopicRouteData::java_topic_route_data_changed` | Eleven actual Java cases cover unordered/equal lists, null-old, null list entries and null broker names; Rust returns typed errors for corresponding Java sort exceptions, and `RouteCache` calls the same method. |
| `TopicRouteData` copy constructor / `cloneTopicRouteData` / `deepCloneTopicRouteData` reference behavior | `java_copy_constructed` / `clone_topic_route_data` / `deep_clone_topic_route_data`; Rust `Clone` remains a separate deep-copy operation | Eight direct Java cases cover copied values, null failures, and outer-list/map versus nested row/list identity. The constructor and shallow clone create independent outer collections while retaining mutable QueueData, BrokerData, mapping DTO, and filter-list references; deep clone shares none. The runtime oracle confirms raw-table and published copies share nested objects. Native fixture differentials compare each identity result. See [ADR-0077](../architecture/ADR-0077-route-copy-element-aliases.md). |

## Lite subscription DTO public methods

Pinned 5.5.1 public declarations for the Lite subscription types have direct method oracles. `LiteSubscription.liteTopicSet` preserves stable getter-handle identity; its setter copies and adds members. `LiteSubscriptionDTO.liteTopicSet` and `offsetOption` preserve setter/getter object identity with shared handles, and the control-body Set and its DTO elements preserve outer and element identity.

| Java public API | Rust counterpart | Evidence / disposition |
| --- | --- | --- |
| `LiteSubscription` getters/setters, topic add/remove overloads, constructor, and `toString()` | `LiteSubscription` field accessors with `SharedLiteSubscriptionTopicNames`, `add_lite_topic(s)`, `remove_lite_topic(s)`, `Default`, and `Display` | Direct cases cover same-handle getters, retained-handle mutation across later API calls, copied/additive setter behavior, update time, defaults, and text. Fastjson decode and wire shape are unchanged. See [ADR-0072](../architecture/ADR-0072-shared-lite-subscription-getter-alias.md). |
| `LiteSubscriptionDTO` getters/fluent setters and `toString()` | `LiteSubscriptionDto` accessors/setters with `SharedLiteSubscriptionTopicSet` and `SharedOffsetOption`, plus `Display` | Direct calls cover every field, setter/getter `ptr_eq`, and Set/OffsetOption mutation in both directions; Fastjson decode wraps the nested values and wire shape is unchanged. See [ADR-0070](../architecture/ADR-0070-shared-lite-subscription-dto-aliases.md). |
| `OffsetOption` constructors, constants, getters/setters, `equals(Object)`, `hashCode()`, and `toString()` | `OffsetOption::{new, option_type, set_option_type, value, set_value, java_equals}`; `PartialEq/Eq/Hash` and `Display` | `java_equals` covers null, wrong type, equal and differing fields. Direct Java calls check same-object hash stability, equal-object hash consistency and Set deduplication. The numeric Java hash depends on enum identity and is not portable across processes; [ADR-0059](../architecture/ADR-0059-offset-option-java-equality.md). |
| `LiteSubscriptionCtlRequestBody` subscription Set getter/setter | `LiteSubscriptionCtlRequestBody::{subscription_set, subscription_set_mut, set_subscription_set}` with `SharedLiteSubscriptionSet` and identity-based shared DTO elements | Direct calls cover null/default state, outer `ptr_eq`, supplied DTO element identity, and additions/membership through both aliases. Decoder retains distinct object elements and deduplicates null entries; JSON arrays remain unchanged. See [ADR-0071](../architecture/ADR-0071-shared-lite-subscription-set-aliases.md). |

## Heartbeat DTO public methods

Historical accessor/mutable-borrow checkpoint, superseded for caller/setter/getter Set identity by `7e5b3fa6` / [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md): Heartbeat DTO accessor coverage (2026-10-09): the direct method oracle invokes all Java getters/setters for all four DTOs. Rust exposes `SubscriptionData::{is_class_filter_mode, get_topic, get_sub_string, get_tags_set, get_tags_set_mut, get_sub_version, get_code_set, get_code_set_mut, get_expression_type, get_filter_class_source}` and matching setters; `ProducerData` group accessors; `ConsumerData` group/type/model/from-where/subscription-data-set getters and mutable getter/unit-mode accessors; and `HeartbeatData` client ID, mutable producer/consumer row borrows, fingerprint, and without-sub accessors. Java and Rust cases cover mutation through each assigned/returned collection; only Java preserved caller-owned Set identity at that checkpoint. Original oracle/implementation `21591fc7`; mutable mutation oracle/implementation `33829680` / `5151a308`; checkpoint fixture SHA-256 `a03cd054471d1782251f8635adcc35d6da29f67f9900c215eadf67f76f5d9c30`; [ADR-0058](../architecture/ADR-0058-heartbeat-dto-public-accessors.md).


Heartbeat DTO identity-method correction (2026-10-08): three actual Java 5.5.1 cases reflect `equals` and `hashCode` and invoke them on the same object and two equal-field instances. `ProducerData`, `ConsumerData`, and `HeartbeatData` all inherit both methods from `java.lang.Object`; distinct objects compare unequal and both remain in a Java `HashSet`. Rust now exposes no structural `PartialEq`, `Eq`, or `Hash` for these three DTOs. Its nullable vectors preserve identity-distinct producer/consumer rows; compile-fail doctests guard the trait boundary. `SubscriptionData` remains value-equal and hashable, matching its Java overrides. Fixture SHA-256 at that update: `f32c42d5f1f71f8da573237504e8312cb8495209bf59cb7d54c0960533c0e308`.

Heartbeat inherited serialization update (2026-10-08): pinned `HeartbeatData` extends `RemotingSerializable`. Java's no-argument `encode()` and `toJson()`, `toJson(boolean)`, static `encode(Object)`/`toJson(Object, boolean)`, `decode`, `decodeList`, and `fromJson` map to `HeartbeatData::java_encode`, `java_to_json`, and the new `java_decode*`/`java_from_json*` adapters. The encoder serializes through `serde_json::Value` so object keys match Fastjson2's sorted order; three actual default, explicit-null, and nested populated cases match compact UTF-8 bytes, pretty JSON values, and encode/decode round-trips. Direct Java calls verify null bytes/strings, JSON `null`, null list input, and preservation of a null list member; nullable adapters match these results. `serde_fastjson::deserialize_nullable_string` separately preserves object key order when Fastjson coerces object/array values to String. The empty `JSONWriter.Feature[]` overload matches default encoding. The client's pinned heartbeat send call sites use no-argument `encode()`. Serde errors from heartbeat, assignment, request-mode, Lite subscription, and batch ACK request encoding map to `RpcError::RequestSerialization` with request code and source before sending. Fixture SHA-256 at that update: `385fc38890b6f18fc9ea74a4a4d2a47f52c27bae5ffc7851064c594cef426803`.

All declared Java bean getters/setters on these DTOs now map to Rust snake_case accessors; direct method calls cover defaults, assigned values, null setter outcomes, and Set aliases. Rust shared handles preserve the same mutable Set object supplied to a setter and mutations through caller-held and getter-held handles in both directions. `SubscriptionData.filterClassSource` is accepted from JSON and retained, then omitted during Java serialization and from Java equality/hash/text behavior. Actual fixture cases cover both JSON decode and serialize directions. `tagsSet` and `codeSet` can contain null; Rust uses shared handles around `HashSet<Option<String>>` and `HashSet<Option<i32>>` to preserve those members through JSON and equality. A null member contributes zero to Java set hash. `ConsumerData.subscriptionDataSet` also retains one null member, represented by a shared HashSet of shared SubscriptionData handles with Java value equality. `HeartbeatData` producer and consumer Sets retain one null row while preserving every non-null identity-distinct object; Rust uses shared HashSets of identity-hashed DTO handles and removes repeated nulls on decode. Java `HashSet` iteration order is unspecified; native diagnostic text sorts set elements deterministically, placing null first.

Fastjson input update: the current pinned Java 5.5.1 corpus has 276 cases. It covers scalar coercion, numeric boundaries, parser grammar, collection shapes, seven JSON token kinds on every field, and seven root shapes for each of `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData`. Java accepts null/empty-object roots and rejects array/scalar roots. Native decoders match booleans, scalar-to-string fields, ordered object/array-to-string conversion, nullable set members, plus/zero-prefixed numbers, form feed and leading-BOM acceptance, and vertical-tab/embedded-BOM rejection. `tagsSet` accepts singleton strings/objects but rejects numeric/Boolean scalars; `codeSet` accepts numbers, strings, Booleans, and objects with Java coercion, including exact high-precision decimal narrowing; plain decimal string fields preserve their digits and exponent forms match Java formatting. `subVersion` explicit null maps to zero, omitted fields retain the constructor timestamp, and object/array values for `subVersion` reject. The latest syntax cases additionally cover Fastjson 2.0.63 escape-table and parser-state-specific comment behavior; see the [current heartbeat audit](PHASE_6_HEARTBEAT_AUDIT.md). [ADR-0015](../architecture/ADR-0015-heartbeat-dto-fastjson-input.md) records the field-decoding scope. Current `heartbeat.json` SHA-256: `67b844d3a61ee31ac12c340cf09f30d81083435635d6f17bebc44206b65d37cc`.

| Java public API | Rust counterpart | Evidence / disposition |
| --- | --- | --- |
| `ConsumeType.getTypeCN` | `ConsumeType::type_cn` | Three actual Java constants verify `PULL`, `PUSH`, and `POP`; [ADR-0022](../architecture/ADR-0022-heartbeat-enum-name-methods.md). |
| `MessageModel.getModeCN` | `MessageModel::mode_cn` | Three actual Java constants verify `BROADCASTING`, `CLUSTERING`, and `LITE_SELECTIVE`; [ADR-0022](../architecture/ADR-0022-heartbeat-enum-name-methods.md). |
| `SubscriptionData` field getters/setters | `is_class_filter_mode`, `get_topic`, `get_sub_string`, `get_tags_set`, `get_tags_set_mut`, `get_sub_version`, `get_code_set`, `get_code_set_mut`, `get_expression_type`, `get_filter_class_source`, and matching setters | Direct Java invocation covers constructor defaults, assigned values, null setters, and tag/code Set mutation through getters; caller/setter/getter Set identity is preserved by shared handles. [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md); accessor scope in [ADR-0058](../architecture/ADR-0058-heartbeat-dto-public-accessors.md). |
| `ProducerData.getGroupName/setGroupName` | `get_group_name` / `set_group_name` | Default, assigned group, and null setter outcomes match. |
| `ConsumerData` field getters/setters | `get_group_name`, `get_consume_type`, `get_message_model`, `get_consume_from_where`, `get_subscription_data_set`, `get_subscription_data_set_mut`, `is_unit_mode`, and matching setters | Direct Java invocation covers defaults, assigned values, null setters, and subscription Set mutation through caller-held and getter-held handles; the same Set handle is retained. |
| `HeartbeatData` field getters/setters | `get_client_id`, `get_producer_data_set`, `get_producer_data_set_mut`, `get_consumer_data_set`, `get_consumer_data_set_mut`, `get_heartbeat_fingerprint`, `is_without_sub`, and matching setters | Direct Java invocation covers defaults, assigned values, null setters, and mutation through caller-held and getter-held collection handles; the same Sets and supplied DTO objects are retained. |
| `SubscriptionData()` / `SubscriptionData(String, String)` / `SUB_ALL` | `SubscriptionData::default/new(Option<String>, Option<String>)` / `SubscriptionData::SUB_ALL` | Three actual Java constructor cases include null arguments; `SUB_ALL` is checked as `"*"`. Constructor defaults, empty sets and current subVersion are compared. |
| `SubscriptionData.equals` / `hashCode` | source-compatible `PartialEq/Eq` / `java_hash_code` | Actual Java pairs cover ignored `filterClassSource`, compared-but-unhashed `subVersion`, null versus empty sets, nullable set members and each compared field; exact Java hashes cover Unicode, set sums and null-as-zero element hashes. |
| `SubscriptionData.compareTo` | `java_compare_to` / `java_compare_to_nullable` | Four actual Java pairs cover null-to-`"null"` concatenation, UTF-16 ordering, `@` delimiter collisions, and prefix length results. A direct `compareTo(null)` invocation records `NullPointerException`; Rust maps this nullable argument to `SubscriptionDataCompareError::NullOther`. |
| `SubscriptionData.toString` | `java_to_string` | Actual Java values cover defaults, explicit nulls, Unicode, populated sets and null set members; Rust sorts set text because Java HashSet order is unspecified. |
| `ProducerData.toString` / `ConsumerData.toString` / `HeartbeatData.toString` | each DTO's `java_to_string` | Actual default, null and populated Java DTO cases include nested subscription text and null collection members. Rust uses deterministic null-first set ordering and retains identity-distinct heartbeat rows. |
| `ProducerData.equals/hashCode`, `ConsumerData.equals/hashCode`, `HeartbeatData.equals/hashCode` (inherited) | no structural `PartialEq/Eq/Hash` traits | Reflection shows all six methods resolve to `java.lang.Object`; equal-field distinct DTOs compare false and two instances remain in the Java `HashSet`. Rust compile-fail doctests ensure the corresponding value traits stay absent. |
| `HeartbeatData` inherited `RemotingSerializable` JSON methods | `java_encode`, `java_to_json(pretty)`, `java_decode*`, `java_decode_list*`, and `java_from_json*` | Actual pinned 5.5.1 cases cover default/null/populated encoding, pretty mode, null bytes/strings, JSON `null`, null list input, null list members, decode and list decode. Decoding uses the shared Fastjson input parser; compact output uses sorted JSON object keys to match the pinned artifact. |
| `ConsumerData` enum JSON fields | `ConsumeType`, `MessageModel`, `ConsumeFromWhere` serde mapping | Actual values cover all 3, 3, and 6 Java constants; names are case-insensitive, unknown strings become null, integer ordinals map by Java declaration order, fractional numbers truncate toward zero before ordinal lookup, and out-of-range values are rejected. Native decoding and re-encoding now match. |
| `HeartbeatData` JSON property names | `clientID`, `withoutSub` | Actual Fastjson cases confirm `clientId`, `isWithoutSub`, and mixed/all-uppercase variants are ignored; native serde now ignores these unknown spellings too. |
| `SubscriptionData.tagsSet` / `codeSet` decode/encode and API alias | `Option<SharedSubscriptionTagSet>` / `Option<SharedSubscriptionCodeSet>` | Nullable string/integer members retain value equality and Java null handling. Setters/getters preserve the same outer Set handle; mutations through caller/getter references are bidirectional. |
| `ConsumerData.subscriptionDataSet` decode/encode and API alias | `Option<SharedConsumerSubscriptionDataSet>` (`SharedValue<HashSet<Option<SharedValue<SubscriptionData>>>>`) | Decoded null members deduplicate and serialize as one null; SubscriptionData retains Java value equality. Setter/getter preserve the outer Set and nested DTO identity, with bidirectional mutation visibility. |
| `HeartbeatData.producerDataSet` / `consumerDataSet` decode/encode and API alias | `Option<SharedProducerDataSet>` / `Option<SharedHeartbeatConsumerDataSet>` | Shared HashSets contain identity-equal shared DTO elements, retaining equal-valued distinct ProducerData/ConsumerData instances while deduplicating null once. Setter/getter retain the outer Set and each supplied DTO identity; JSON remains an array. |
| `HeartbeatData.computeHeartbeatFingerprint` | `compute_heartbeat_fingerprint -> Result<i32, HeartbeatFingerprintError>` | Seven actual Java runtime fingerprints plus explicit null collection/primitive inputs. A producer null member has a matching fingerprint; null ConsumerData and SubscriptionData members match Java's NullPointerException through typed errors. The runtime delegates to this public method. |
| Fastjson input for `SubscriptionData`, `ProducerData`, `ConsumerData`, `HeartbeatData` | field-level `serde_fastjson` decoders | 276 actual Java inputs include 140 seven-token-kind probes across all 20 fields, scalar conversion, numeric boundaries, nested DTOs, Java Set shapes, seven root shapes per DTO, and 35 syntax cases. Fastjson accepts its observed `char1`/`\x` string escapes, single-quoted keys/strings, plain object/array trailing commas, and comments only at the recorded parser-state boundaries; unquoted field names, unknown escapes, and comments at the recorded invalid boundaries reject. `tagsSet` accepts singleton string/object but rejects number/Boolean; `codeSet` accepts number/string/Boolean/object, with Boolean→1 and object→0; array conversions deduplicate and preserve null. Exact decimal tokens retain Java low-bit narrowing; numeric-to-string conversions preserve plain decimal digits and match Java exponent formatting. `subVersion` explicit null maps to zero, omission preserves its constructor default, and empty object/array reject. Native acceptance and normalized values match; Serde error classes are not Fastjson exception equivalents. |

The current heartbeat fixture SHA-256 is `67b844d3a61ee31ac12c340cf09f30d81083435635d6f17bebc44206b65d37cc`. The method/constructor and nullable member tests close these heartbeat DTO rows, not remaining source/input or whole-phase acceptance.

Outbound extension request serialization errors map to `RpcError::RequestSerialization { request_code, source }`. The runtime encodes typed JSON bodies before calling remoting, so a serializer error returns before a frame is sent. A failing-serializer unit case verifies both the request-code and Serde-error retention; ordinary heartbeat and extension request tests verify the successful wire path.

## Client enum API: AccessChannel

| Pinned Java API | Rust counterpart / current behavior |
| --- | --- |
| `values()` | `AccessChannel::values() -> [AccessChannel; 2]` returns a fresh value array in declaration order |
| `valueOf(String)` | `from_java_name(Option<&str>) -> Result<AccessChannel, AccessChannelLookupError>` matches exact names and distinguishes Java null/unknown-name failures |
| `name()`, `ordinal()`, `toString()` | `name()`, `ordinal()`, and `Display` match `LOCAL`/`CLOUD`, declaration order, and text |
| `compareTo(AccessChannel)` | `java_compare_to()` returns the Java ordinal difference; Rust `PartialEq` matches enum singleton equality |

Java enum `hashCode()` is identity-based with no stable numeric oracle; Rust does not claim a matching Java hash code. See [ADR-0024](../architecture/ADR-0024-access-channel-enum-api.md).

The exact Rust variants are `AccessChannel::Local` → Java `LOCAL` and `AccessChannel::Cloud` → Java `CLOUD`. The public lookup-result variants are `AccessChannelLookupError::NullName` for Java's null-name `NullPointerException`, and `AccessChannelLookupError::UnknownName(String)` for `IllegalArgumentException` with Java's enum-qualified message. `access_channel_enum_methods_match_invoked_java` compares both constants, order, null/unknown messages, and display behavior.

### Public client RPC enum variants

| Rust public enum | Rust variant → pinned Java value | Java/Rust evidence |
| --- | --- | --- |
| `api::consumer::PullStatus` | `Found` → `FOUND`; `NoNewMessage` → `NO_NEW_MSG`; `NoMatchedMessage` → `NO_MATCHED_MSG`; `OffsetIllegal` → `OFFSET_ILLEGAL` | Java status codes, declaration order, text and lookup outcomes are compared by the RPC fixture tests. |
| `api::consumer::PullStatusLookupError` | `NullName`; `UnknownName(String)` | Null maps to Java `NullPointerException`; unknown names map to the exact `IllegalArgumentException` message. |
| `api::extensions::PopStatus` | `Found` → `FOUND`; `NoNewMsg` → `NO_NEW_MSG`; `PollingFull` → `POLLING_FULL`; `PollingNotFound` → `POLLING_NOT_FOUND` | Java declaration order places `POLLING_FULL` before `POLLING_NOT_FOUND`; lookup and response-code cases are covered. |
| `api::extensions::PopStatusLookupError` | `NullName`; `UnknownName(String)` | Null and unknown Java enum lookups retain their exception category/message in the invoked fixture. |
| `api::extensions::AckStatus` | `Ok` → `OK`; `NoExist` → `NO_EXIST` | Direct Java ACK API fixtures compare returned status values. |
| `api::extensions::AckStatusLookupError` | `NullName`; `UnknownName(String)` | Null and unknown Java enum lookups retain their exception category/message in the method oracle. |
| `api::producer::SendStatus` | `SendOk` → `SEND_OK`; `FlushDiskTimeout` → `FLUSH_DISK_TIMEOUT`; `FlushSlaveTimeout` → `FLUSH_SLAVE_TIMEOUT`; `SlaveNotAvailable` → `SLAVE_NOT_AVAILABLE` | Direct API/status fixtures compare request results, enum declaration order and inherited enum methods. |
| `api::producer::SendStatusLookupError` | `NullName`; `UnknownName(String)` | Null maps to Java `NullPointerException`; unknown names map to the exact `IllegalArgumentException` message. |
| `api::producer::SendKind` | `V1` → `SEND_MESSAGE`; `V2` → `SEND_MESSAGE_V2`; `Batch` → `SEND_BATCH_MESSAGE`; `ReplyV1` → `SEND_REPLY_MESSAGE`; `ReplyV2` → `SEND_REPLY_MESSAGE_V2` | Rust's explicit low-level request selector maps the wire codes. Producer facade selection uses hidden `MQClientApi::send_kind_for_message` to match Java's private `MQClientAPIImpl.sendSmartMsg` switch and reply `MSG_TYPE`; the exact property and first-use semantics are recorded below. |
| `api::producer::SendMode` | `Sync`; `Future`; `Oneway` | Rust invocation mode selects `invokeSync`, async/future and one-way transport behavior; Java exposes these operations as methods rather than this enum. |

### Runtime selectors and public result variants

| Rust public type | Exact Rust variants | Java-facing owner / scope |
| --- | --- | --- |
| `runtime::RuntimeState` | `Created`, `Starting`, `Running`, `ShuttingDown`, `Stopped`, `Failed` | Native observable lifecycle states; transitions and cancellation/rollback are mapped in the [lifecycle audit](PHASE_6_LIFECYCLE_AUDIT.md), not a Java enum API. |
| `runtime::ConsumerOperation` | `ResetOffset` → code 220; `ConsumerStatus` → 221; `RunningInfo` → 307; `ConsumeDirectly` → 309 | Consumer-group-keyed inbound capability selectors. Push implements these Phase 17 operations; Pull status/running-info are implemented, while Pull reset is one-way and Pull direct consume returns Java's missing-Push-consumer error. The [inbound matrix](PHASE_6_INBOUND_MATRIX.md) verifies routing boundaries, not full handler parity. |
| `runtime::RuntimeError` | `ClientCheck`, `AddressingCallbackFailed`, `HeartbeatEncoding`, `InvalidHeartbeatData`, `InvalidSchedule`, `TaskCallbackFailed`, `InvalidState`, `ControlTaskFailed`, `InboundRequestExecutor`, `StartupFailure` (test-only), `DuplicateGroup`, `DuplicateCorrelation`, `RequestReplyAdmissionTimeout`, `InvalidRoute`, `RecursiveRouteTableMutation`, `InvalidOffsetTable`, `InvalidBrokerAddress`, `InvalidBrokerPort`, `Rpc`, `RegistryPoisoned`, `Transport` | Native result carrier, not a Java enum. Call-site mappings cover check-client behavior, invalid heartbeat/offset/route inputs, schedule rejection, duplicate registration/correlation, inbound executor failures, VIP parsing, RPC responses, and transport failures; panic/lock/control-task errors are Rust execution failures. No one-to-one Java error class is claimed. |
| `error::RpcError` | `Transport`, `Header`, `Unsupported`, `RequestSerialization`, `MalformedResponse`, `Response` | Native RPC result carrier. `MalformedResponse` preserves request code, peer and parse/missing-header/body reason; Java counterpart failures are surfaced by each call site's header/body decoder. `Response` retains the Broker code and remark. |

## Protocol enum API: LanguageCode

| Pinned Java API | Rust counterpart / current behavior |
| --- | --- |
| `values()` | `LanguageCode::values() -> [LanguageCode; 14]` returns values in Java declaration order |
| `valueOf(String)` | `value_of_name(Option<&str>) -> Result<LanguageCode, LanguageCodeLookupError>` matches exact names and distinguishes Java null/unknown-name failures |
| `getCode()`; `Enum.name()`; `ordinal()`; `toString()` | `code()`, `name()`, `ordinal()`, and `Display` match all Java values |
| `compareTo(LanguageCode)` | `java_compare_to()` returns Java's ordinal difference; Rust equality matches enum singleton equality |
| `getCode(String)` | `get_code_by_name(Option<&str>)`; exact case-sensitive enum names, null or unknown names return `None` |
| `valueOf(byte)` | `from_java_byte(i8) -> Option<LanguageCode>`; known enum bytes return `Some`, other signed byte values return `None` |
| JSON/private-header decode | Unknown string and byte values decode to `None`, matching Java's nullable result; enum encoding accepts only Java-defined language values |

Java enum `hashCode()` is identity-based with no stable cross-run numeric value; Rust does not claim a matching numeric enum hash. See [ADR-0023](../architecture/ADR-0023-language-code-java-methods.md) for fixture provenance, byte boundaries, and validation.

## Protocol enum API: SerializeType

| Pinned Java API | Rust counterpart / current behavior |
| --- | --- |
| `values()` | `SerializeType::values() -> [SerializeType; 2]` returns Java declaration order |
| `valueOf(String)` | `value_of_name(Option<&str>) -> Result<SerializeType, SerializeTypeLookupError>` matches exact names and reports Java null/unknown-name categories and messages |
| `valueOf(byte)` | `from_java_byte(i8) -> Option<SerializeType>` returns `None` for unknown signed bytes |
| `getCode()`; `Enum.name()`; `ordinal()`; `toString()` | `code()`, `name()`, `ordinal()`, and `Display` match both values |
| `compareTo(SerializeType)` | `java_compare_to()` returns Java's ordinal difference |
| Unknown wire serialization byte | Java lookup yields null and `RemotingCommand.headerDecode` dereferences it; Rust returns `CodecError::UnsupportedSerialization(code)` before constructing a public enum value |

Java enum `hashCode()` is identity-based with no stable cross-run numeric value; Rust does not claim a matching numeric enum hash. See [ADR-0027](../architecture/ADR-0027-serialize-type-java-enum-api.md) for direct oracle cases and the wire boundary.

## Remoting TLS enum API: TlsMode

| Pinned Java API | Rust counterpart / current behavior |
| --- | --- |
| `values()` / `valueOf(String)` | `TlsMode::values()` returns declaration order; `value_of_name(Option<&str>)` matches exact enum constants and typed Java null/unknown lookup failures |
| `name()`, `ordinal()`, `toString()`, `compareTo(TlsMode)` | `name()`, `ordinal()`, `Display`, and `java_compare_to()` match Java enum values and declaration order |
| `getName()` | `get_name()` returns the exact lowercase names `disabled`, `permissive`, and `enforcing` |
| `parse(String)` | `parse(Option<&str>)` recognizes exact lowercase names and defaults to `Permissive`, including null, case mismatches, whitespace, and unknown values |
| Standalone `NettyClientConfig` TLS default | `TransportMode` uses `TlsMode::parse`; absent `tls.enable` enables standalone Netty TLS only for `Enforcing`, while an explicit `tls.enable` boolean takes precedence. Separately, Java `ClientConfig.useTLS` reads `TlsSystemConfig.tlsEnable` (`tls.enable`, false default), and `MQClientInstance` copies that value into its Netty config. |

The pinned Java fixture invokes all enum methods and parse boundary cases; the native differential checks direct outcomes. TLS credential provisioning and end-to-end acceptance remain Phase 19 work. See [ADR-0031](../architecture/ADR-0031-tls-mode-java-enum-api.md).

## Remoting command enum and type API

| Pinned Java API | Rust counterpart / current behavior |
| --- | --- |
| `RemotingCommandType.values()` / `valueOf(String)` / inherited enum methods | `RemotingCommandType::values()`, `value_of_name`, `name`, `ordinal`, `Display`, and `java_compare_to()` match `REQUEST_COMMAND` and `RESPONSE_COMMAND` order and lookup failures |
| `RemotingCommand.getType()` | `RemotingCommand::get_type()` returns the type selected by the response flag and remains consistent with `is_response()` |
| `markResponseType()`, `isResponseType()`, `markOnewayRPC()`, `isOnewayRPC()` | `mark_response()`, `is_response()`, `mark_oneway()`, `is_oneway()` preserve unrelated flag bits and repeated marks are idempotent; direct Java fixtures compare flags, type, and both predicates, including combined and high-bit cases |
| Private command fields with public getters/setters (`code`, `language`, `version`, `opaque`, `flag`, `remark`, `body`, `extFields`, `serializeTypeCurrentRPC`) | Rust `RemotingCommand` exposes corresponding typed fields; these are the idiomatic Rust access paths for Java's getter/setter capabilities. `ext_fields` uses `BTreeMap` rather than Java `HashMap`; ordering is a representation choice, while wire and lookup behavior are covered by codec fixtures. |
| `isSuspended()` / `setSuspended(boolean)` | Rust `suspended` state and `is_suspended()` / `set_suspended(bool)` match the false default and setter/getter behavior; the state is local and is omitted from encoded command headers like Java's non-serialized property. Java's `writeResponse` uses this value in the long-polling RPC metric label. Rust's inbound response path now carries the value into the configured per-client metrics manager; the transport integration test verifies `is_long_polling=true` on the recorded response metric. |
| `RequestType.values()` / `valueOf(String)` / inherited enum methods | `RequestType::values()`, `value_of_name`, `name`, `ordinal`, `Display`, and `java_compare_to()` map the single `STREAM` value |
| `RequestType.valueOf(byte)` / `getCode()` | `RequestType::from_java_byte(i8) -> Option<RequestType>` returns `None` for unknown signed bytes; `get_code() -> i8` returns Java code `0` |

The pinned fixture directly calls both enum APIs, unknown byte lookups, `RemotingCommand.getType()`, response/oneway marking and predicates, including repeated marks and preservation of unrelated flag bits. The Rust stream hook uses the mapped code instead of a duplicated literal. See [ADR-0032](../architecture/ADR-0032-remoting-command-enum-java-api.md).

## Pinned `RemotingCommand` public method inventory

`fixtures/remoting/response-future-api.json` now reflects the complete declared public method surface from the pinned Java 5.5.1 class: no public constructors and 57 public methods. Maven verifies the reflection fixture against that artifact; the Rust test pins the source commit, constructor count, method count, and key state/flag/string methods. This inventory exposes method families that the enum/field crosswalk above did not cover.

| Java public method family | Rust counterpart / evidence | P18 status |
| --- | --- | --- |
| `createRequestCommand`, `createResponseCommand*`, `buildErrorResponse*`, `createNewRequestId` | Rust exposes process-wide request-ID generation and request/response/error factory functions. Exact response/error counterparts include `RemotingCommand::{create_response_command,create_response_command_with_header,create_response_command_with_default_header,create_response_command_with_header_factory,create_response_command_for_header,build_error_response,build_error_response_with_default_header,build_error_response_with_header_factory}`; these adapt Java's `createResponseCommand(int,String)`, `createResponseCommandWithHeader(int,CommandCustomHeader)`, class-header overload, and `buildErrorResponse` overloads. Typed overloads accept `Option<T>` and attach a `CommandHeader`; fields merge when `make_custom_header_to_net` or a codec encoder runs. `SharedCommandHeader<T>` is the cloneable synchronized handle for Java's caller/command shared-header mutation behavior. `create_response_command_with_header_factory` maps Java's reflective no-arg construction failure to a null response after consuming an opaque ID; `build_error_response_with_header_factory` maps the subsequent null dereference to `NullPointerException`. Invoked fixtures cover missing/inaccessible/throwing constructors and the ID delta, along with defaults, flags, fields, version setting, and serialization. `RemotingCommand::default()` matches the pinned constructor's `JAVA` language, zero-valued fields, configured serialization type, and process-wide opaque-ID increment. | UNIT_VERIFIED for the shared-header and fallible-factory behavior; broad P18-V01 remains open |
| `getProtocolType`, `markProtocolType`, `getSerializeTypeConfigInThisServer`, `getHeaderLength` | Rust exposes corresponding helpers and constants. Invoked Java fixtures compare header masking, protocol marking/lookup (including unknown codes), and the serialization setting's property/environment precedence and invalid-value behavior in isolated processes. | UNIT_VERIFIED |
| `encode`, `decode(byte[]/ByteBuffer/ByteBuf)`, `encodeHeader*`, `fastEncodeHeader`, `makeCustomHeaderToNet` | `codec::{frame,json_header,rocketmq_header}` provides byte-vector and incremental stream operations. Both JSON and RocketMQ encoders merge attached `CommandHeader` values into the wire ext-fields; explicit `make_custom_header_to_net` materializes the same merge into the command. `PullMessageRequestHeader` no longer exports the Rust-only `lite_topic`; an incoming `liteTopic` is ignored as an unknown field. `decode_command_custom_header_directly` passes Java's `useFastEncode` mode through `CommandHeader::decode_fields_with_mode`. Invoked Java fixtures and Rust comparisons cover all four Java `FastCodesHeader` implementations (Pull request/response, Send response, and Send V2 request) in both modes. Generic reflection-mode fixtures also cover consumer offset request/response headers and the four Phase 9 offset-boundary response headers, including nullable fields, malformed conversions, and owner unboxing. Rust still requires `CommandHeader + Default`, while Java constructs and inspects headers reflectively; typed headers outside these audited families still use the default mode adapter and remain part of broad P18-V01. | Named fast/reflection and offset-header slices are fixture/unit verified; broad P18-V01 remains open |
| State getters/setters, `getType`, response/oneway methods, suspended state | Typed Rust command fields plus `get_type`, flag helpers, and suspended getter/setter; direct Java fixture cases cover flags and suspended default/set/reset. Inbound response metrics now include the suspended long-poll label through the per-client Remoting metrics manager. `ext_fields` uses `ExtFields`, which retains nullable Java map entries while providing deterministic non-null iteration; invoked Java cases verify the state behavior and null-entry display. | UNIT_VERIFIED |
| `readCustomHeader`, `writeCustomHeader`, `decodeCommandCustomHeader*`, `addExtField*` | `RemotingCommand` keeps an attached typed header separate from ext-fields until materialization/encoding. `read_custom_header` and `read_custom_header_mut` access the cached typed value; `write_custom_header(Option<T>)` attaches or clears it. Callers retaining the header pass a clone of `SharedCommandHeader<T>`, so mutation through either the caller handle or command is visible through the other, including across custom-header materialization and JSON/RocketMQ binary encoding. `decode_command_custom_header` replaces the cache, `decode_command_custom_header_with_cache(true)` reuses it, and `decode_command_custom_header_directly` decodes without caching. `add_ext_field` creates the map; `add_ext_field_if_not_exist` preserves a non-null existing value and returns `ProtocolError` when the map is absent (Java dereferences null and throws). `add_ext_field_nullable` and `add_ext_field_if_not_exist_nullable` preserve Java null keys/values and `HashMap.putIfAbsent` replacement behavior. Invoked fixtures cover shared mutation in both directions, attachment, cached reuse, direct decode, null clearing, ext-field merging, helper outcomes, JSON's literal `"null"` key and omitted null values, and RocketMQ binary-map omission of null entries. The typed header implementations still use Rust's compile-time `CommandHeader` contract; the factory adapter covers Java reflection construction failures. | UNIT_VERIFIED for shared-header and nullable ext-field behavior; broad P18-V01 remains open |
| `getProcessTimer` / `setProcessTimer`, `getCallbackList` / `setCallbackList` | Rust exposes `ProcessTimer` and `CommandCallback` getters/setters; invoked Java cases cover null defaults/clears, same-instance access, timer start/stop/reset state and errors, callback-list mutation and invocation. Guava `Stopwatch` inherits identity-based equality, so distinct stopped timers with equal elapsed state remain unequal; Rust `ProcessTimer` omits structural `PartialEq`/`Eq`, guarded by a compile-fail doctest. Cloning a Rust timer shares its mutable state so cloned command/hook/response-write handles retain the same stopwatch identity as Java's stored object reference; the `cloned_process_timer_shares_java_stopwatch_state` regression covers start/stop/reset visibility across clones. Inbound decoded requests reach processors with a running timer. | UNIT_VERIFIED |
| `toString` | `Display` matches Java's field labels, null text, 32-bit flag formatting, and ext-field contents. Invoked Java fixtures compare null, single-entry, and multi-entry cases; the multi-entry check validates fields as a map and normalizes only their iteration order because Java's `HashMap` does not guarantee it. Rust's sorted `BTreeMap` output is deterministic. | UNIT_VERIFIED with a source-confirmed order disposition |
| Inherited `Object.equals(Object)` / `Object.hashCode()` | Java reflection resolves both methods to `java.lang.Object`; invoked cases show self-equality, unequal distinct commands with the same visible state, and two entries in a `HashSet`. Rust deliberately omits structural `PartialEq`/`Eq`, guarded by a compile-fail doctest. Fixture: `remotingCommandIdentityBehavior`. | UNIT_VERIFIED |

P18-V01 remains open until every method row has a tested Rust counterpart or a source-confirmed disposition that preserves the required public capability and observable flow.

### Consumer offset header reflection decode

`QueryConsumerOffsetRequestHeader`, `UpdateConsumerOffsetRequestHeader`, and `QueryConsumerOffsetResponseHeader` do not implement Java `FastCodesHeader`, so `decodeCommandCustomHeaderDirectly` uses reflection for either `useFastEncode` hint. Java leaves absent boxed/reference fields null, catches a missing or failed conversion per field, and uses `Boolean.parseBoolean` for `setZeroIfNotFound`; the classes' `checkFields()` methods are no-ops. Rust preserves nullable fields and mirrors those outcomes in `decode_fields_with_mode`. The consumer-offset RPC owner checks a nullable query-response `offset` before constructing its result, matching Java's unboxing boundary. The invoked fixture `fixtures/remoting/offset-header-decode-modes.json` covers missing fields, invalid queue/offset conversions, malformed boolean input, and valid values in both hints. This closes only these decoder/API and owner-boundary slices; broad P18-V01 remains open.

The Phase 9 boundary response headers (`SearchOffsetResponseHeader`, `GetMaxOffsetResponseHeader`, `GetMinOffsetResponseHeader`, and `GetEarliestMsgStoretimeResponseHeader`) also take Java's generic reflective path. Their boxed `Long` values remain null when absent or when that field's conversion fails; the four RPC owners then fail when unboxing a required value. Rust now preserves `Option<i64>` during direct mode-aware decoding and performs the owner checks. `fixtures/remoting/offset-boundary-header-decode-modes.json` and `rpc_invoked_offset_boundary_fixtures.rs` cover both mode hints and owner outcomes. See the [Phase 9 method matrix](PHASE_9_OFFLINE_METHOD_MATRIX.md). This closes these four header/owner slices only; broad P18-V01 remains open.

## Runtime worker and inherited ServiceThread methods

| Pinned Java public method | Rust counterpart / disposition |
| --- | --- |
| `RebalanceService.getServiceName()` | `runtime::REBALANCE_SERVICE_NAME`; direct Java call and native comparison pass. |
| `PullMessageService.getServiceName()` | `runtime::PULL_MESSAGE_SERVICE_NAME`; direct Java call and native comparison pass. |
| `RebalanceService.run()` | Owned `runtime/rebalance.rs` worker invokes registered optional rebalance capabilities; Java worker-loop source and property cadence are mapped in the runtime inventory. Real assignment/allocation is Phase 11. |
| `PullMessageService.run()` | Owned `runtime/pull.rs` Phase 6 FIFO worker dispatches pull/pop requests to the current registered consumer capability. Phase 9 owns manual pull RPC/offset stores; Phase 10 owns Push pull request types, ProcessQueue and bounded fetch/consume scheduling; Phase 11 owns assignment/initial-offset activation. |
| `executePullRequestImmediately(pullRequest)` / `executePopPullRequestImmediately(popRequest)` | `execute_pull_request_immediately` / `execute_pop_pull_request_immediately`; FIFO/current-owner dispatch tests cover the runtime boundary. |
| `executePullRequestLater(pullRequest,delay)` / `executePopPullRequestLater(popRequest,delay)` | Public `ClientRuntime::execute_pull_request_later` / `execute_pop_pull_request_later` methods enqueue the owned pull worker's delayed queue; serial deadline and cancellation tests cover scheduling and shutdown. |
| `PullMessageService.executeTask(task)` / `executeTaskLater(task,delay)` | `execute_task` / `execute_task_later` enqueue futures on the PullMessageService delayed worker; one owned worker and Java one-second graceful shutdown window are covered. |
| `PullMessageService.getScheduledExecutorService()` | `ClientRuntime::pull_scheduled_executor_service() -> RuntimeScheduler` uses the separate delayed pull-task queue; supports immediate/delayed async work, result-bearing calls, untimed/timed `invoke_all`/`invoke_any`, fixed-rate/fixed-delay callbacks, per-task cancellation, Future status/wait/delay, `shutdown`/`shutdown_now`, shutdown/termination status and async termination wait. | Direct owner regressions cover pre-start delayed work, cancellation/removal, returned values and bulk calls, both shutdown modes, returned-task identity and pending state, status/wait, runtime shutdown and worker-initiated shutdown. `shutdown_now` returns handles for the queued pull jobs without cancelling them. The getter is separate from the `MQClientInstance` scheduler handle; shutting down this executor leaves the immediate FIFO pull dispatcher available. |
| inherited `start()` / `shutdown()` | `RuntimeServiceThread::{start,shutdown,shutdown_with_interrupt}` expose the start method and both Java shutdown overloads. No-argument `shutdown()` selects `interrupt=false`; PullMessageService also closes its scheduled executor through the same override path. Each strong handle retains the client owner like Java's service reference to `MQClientInstance`; runtime-wide `ClientRuntime::start/shutdown` still owns aggregate lifecycle. |
| inherited `wakeup()` | `RuntimeServiceThread::wakeup()` signals the rebalance worker; PullMessageService's `run()` blocks on its message queue and its inherited wait-point wakeup has no effect on that queue. |
| inherited `isStopped()` | `RuntimeServiceThread::is_stopped()` exposes each worker's stopped flag, including the initial false value, pre-start no-op, and started `make_stop()` transition. |
| inherited `shutdown(boolean)` | `RuntimeServiceThread::shutdown_with_interrupt(interrupt).await` sets the service stopped flag, wakes the worker, and waits up to the pinned 90,000ms join window unless the started thread's daemon snapshot is true. For PullMessageService, `interrupt=true` also releases a worker blocked on the message queue; its scheduled executor closes with the Java one-second grace. |
| inherited `shutdown()` | `RuntimeServiceThread::shutdown().await` maps the no-argument overload to `interrupt=false`, including its pre-start no-op flag behavior. PullMessageService still closes its scheduled executor with the Java one-second grace, even when called before start. |
| inherited `getJoinTime()` | `RuntimeServiceThread::join_time_millis()` and `runtime::SERVICE_THREAD_JOIN_TIME_MILLIS` expose Java's fixed 90,000ms result; direct Java fixture values and Rust handle values are checked. |
| inherited `makeStop()` | `RuntimeServiceThread::make_stop()` is a pre-start no-op and otherwise sets the stop flag, wakes the RebalanceService worker, and does not join it. PullMessageService's inherited `LockSupport.unpark` does not release its blocking message-queue take, so it observes the flag after a current or already-blocked queue item. A direct Java `waitForRunning` probe and Rust worker-liveness regression cover the rebalance wake path. |
| inherited `isDaemon()` / `setDaemon(boolean)` | `RuntimeServiceThread::{is_daemon,set_daemon}` map the default false value and mutable property. The value is captured when a worker starts and controls whether service shutdown performs the Java join wait; Tokio tasks remain owned by the runtime rather than setting an OS thread's process-exit policy. |

The `serviceThreadCases` fixture invokes inherited state/configuration methods on unstarted workers. `serviceLifecycleCases` uses controlled Java `run()` bodies; Rust's runtime regression exercises the public service handles and independent worker lifecycle, including pre-start no-op, repeat start, `make_stop`, interrupting PullMessageService queue wait, daemon setting and restart after full runtime shutdown. Neither fixture contacts a Broker. `ServiceThread.waitForRunning(long)` and `onWaitEnd()` are protected and are not public API rows; concrete worker `run()` behavior is mapped separately. See [ADR-0025](../architecture/ADR-0025-runtime-service-name-methods.md) for names and [ADR-0026](../architecture/ADR-0026-service-thread-api-boundary.md) for the inherited contract. The Tokio worker remains runtime-owned; daemon semantics apply to the per-service join wait.

## Other runtime exports, reverse inventory

Cleanup audit update `75c2729` confirms raw null Broker maps are skipped, matching pinned source guard and two actual runtime cases; no production/API change. Existing raw table reads remain preserved while global addresses are cleaned. Route hash at that stage was `a1e55afbdda8db505efe05f13eb20687a21aa88ecd108bcfc1df6676a20ef558`; reviewed focused core74pass/1ignored/runtime7/topicTable3/workspace/exact Clippy/Maven exit0, fmt/diff/comparator/manifest22 pass. This closes the specific null-map cleanup uncertainty, not other DTO/mutable getter/default-map audits; the nullable lower-model fixes are recorded separately as implemented.

Current raw route counterpart `dae27fb` and collection views `0a03138` provide the tested map surface. `c085d03` adds the actual Java route DTO alias oracle, `82430b5` adds the failing native regression, and `03e3925` implements shared `TopicRouteDataHandle` values. The earlier route fixture at SHA-256 `27d8e568e0e5042aa891cdd08af94b7ffd28667e1dd02d3df5d94d7650646e65` captures three true outcomes for `put` identity and mutations visible in both directions. `snapshot()` returns an owned DTO copy; `with_mut()` updates all aliases. Runtime cached-route queries and the raw table getter return that shared handle. Direct raw-table writes do not republish topics; an actual runtime refresh creates separate outer route collections that retain shared nested DTO/filter-list handles, as recorded in ADR-0077. The earlier `f579c47db7ee87efdd315d71d82e918e3cccf7b782505562b459822a29b4e083` map fixture has 59 outcomes; the current route fixture extends it to 65 with the `Map.Entry.setValue` oracle and native `TopicRouteEntryHandle` adapter (ADR-0078). Java iterator-removal effects use explicit Rust view removals and traversal-start snapshots are allowed by the weak-iterator contract. Full workspace/all-target tests and exact all-target/all-feature Clippy pass. Offline Maven verify passes with route25/config73 and manifest22; the differential comparator self-test, manifest check, fmt and diff checks pass. Other map defaults, malformed-input and broader source reconciliation remain open.

Historical route metadata evidence `3f71c78`/`9241c6e` invokes actual live topic map/getter/query/offline-cleanup behavior and corrects private route equality. Route hash at that stage was `c88c250c9303128b818f2d5f1e7a2a0b6cf3da4a8691bc70564b842566add615`; metadata-only changes retain the old snapshot. At that point the native live raw table remained missing; later `dae27fb` and `0a03138` implement and test its raw/derived behavior and view effects, as recorded above and in the route audit.

Current unregister update `3ccaf54`: `unregister_producer_by_group/unregister_consumer_by_group` expose source unconditional current-mapping cleanup independently of identity-safe lifecycle methods. They always attempt Broker unregister for absent keys while transport is live, preserve best-effort errors, serialize replacement behind cleanup and permit terminal map removal without resource revival. This supersedes the missing group-unregister statement below. Source/native consumer method golden4 and existing map golden38 pass; final workspace core71pass/1lower-review-ignored/registry4/runtime7, exact Clippy and offline Maven exit0 (`/tmp/p6-group-unregister-workspace-final.log`, `-clippy-final.log`, `-java-verify.log`); fmt/diff/comparator/manifest22 pass. No new public setting or worker. Full API/input acceptance remains separate.

`bf4a859` implements returned registry map primitives; `b9f5bb6` adds Java-style conditional equality; `73f1329` adds remapping and snapshot iteration; `51eb87f` adds basic live view handles; `74322a0` adds getOrDefault/putAll; `771e3cc` adds predicate-based view removal/retention; `1a91b2b` adds collection-based removeAll/retainAll; `eca1d066` adds `ProducerEntryHandle` and `ConsumerEntryHandle` snapshots with Java `Map.Entry.setValue` capture and replacement behavior; `948f67e7` adds exact-key remapping and lifecycle exclusion; `201e8075` matches registry `replaceAll` retries using Java owner equality. Both tables export get/contains_key/get_or_default/len/is_empty/snapshot/keys/values/for_each, put/put_all/put_if_absent/replace/remove/clear, conditional `replace_if_same`/`remove_if_same` and Java-equality `replace_if_equal`/`remove_if_equal`, compute variants, merge and replace_all. `key_set_view`, `values_view` and `entry_set_view` return cloneable live handles with contains/len/is_empty/snapshot/remove/clear/remove_if/retain/remove_all/retain_all operations; entry-set views also expose deterministic `entry_snapshots()` whose handles retain their captured value and write replacement mappings. Iterator-removal effects are represented by live-view `remove`; point-in-time view snapshots are one outcome allowed by Java ConcurrentHashMap traversal. Custom Java equality uses an optional key sampled from the live owner outside registry locks; lifecycle leases remain identity-based. No new setting/worker/Broker RPC. Source unconditional group unregister has its own API. Full source/input and view-surface reconciliation remains open.

`ClientRuntimeManager::{new,get_or_create,get_or_create_with_hook,get_or_create_nullable,get_or_create_with_hook_nullable,remove_client_factory,remove_client_factory_by_id,factory_table}` represent MQClientManager creation/both removal overloads/live table with the plan-required explicit resettable owner. `ClientRuntimeManager::shared_default` is a `#[doc(hidden)]` Rust facade bridge used by Producer, PullConsumer, and PushConsumer to share that default manager; it adds no separate Java capability or client setting. `FactoryTable` represents source ConcurrentMap access through native owned reads and per-key mutation primitives, not an additional client setting. The former reverse listing of `ClientRuntimeManager::remove` was incorrect: `remove` belongs to private ManagerLink, not the exported manager. The source's public surface is explicitly reconciled below. The factory keeps its `TopAddressing` injection seam private to unit tests because Java `MQClientAPIFactory` exposes no provider-injection method. Separately, `ClientRuntimeManager::with_top_addressing` is the explicit per-manager form of Java's public `TopAddressing` ServiceLoader extension; it is distinct from the private factory test seam and is mapped in the addressing table below. `DefaultTopAddressing`, `NameServerUpdateCallback`, and their exact public method/constructor mappings to Java's addressing SPI appear in the table below. `state`, `RuntimeState`, `RuntimeError`, and `GroupLease` expose native lifecycle/identity/error results, not additional client settings. `MessageRequest` maps pull/pop dispatch inputs; producer/consumer traits expose only source registration, subscription, route, persistence, adjustment, rebalance and owned inactive inbound capabilities. `ConsumerOperation` names the remaining group-keyed inactive request-code slots in the [inbound matrix](PHASE_6_INBOUND_MATRIX.md); code 40 is handled directly by the runtime-wide rebalance service.

A follow-up exact-name scan across all 16 `runtime/*.rs` and `route/*.rs` source files plus `config.rs` (17 files total) counted 562 public declarations. Nine names absent from this external API map belong only to private or `pub(super)` implementation types: `ControlPlane::{sender,join_terminal}`, `ManagerLink::same_table`, `PullService::stop_accepting`, `RebalanceSignal::{from_properties,next_due,fire_due,close_delayed}`, and `GroupRegistry::has_active_users`. Those helpers are not callable outside the crate; no other public runtime/route/config declaration was missing an exact map name in this bounded scan.

| MQClientManager public API | Native counterpart / current status |
| --- | --- |
| getInstance | explicit shareable `ClientRuntimeManager::new/Clone`, required by phase plan; facade default sharing Phase7 |
| getOrCreateMQClientInstance(config[,hook]) | `get_or_create/get_or_create_with_hook` plus `get_or_create_nullable/get_or_create_with_hook_nullable(Option<&ClientConfig>, ...)`; null config throws Java NPE and maps to typed `InvalidRoute` without publishing a runtime; same-ID calls reuse the first runtime and its cloned config/property/hook snapshot even when later inputs differ, matching Java `get`/`computeIfAbsent`; concurrency/cancellation evidence |
| removeClientFactory(id,instance) | `remove_client_factory` plus `remove_client_factory_nullable(Option<&str>, Option<&Arc<ClientRuntime>>)`; exact identity-safe removal, stale-replacement checks; pinned null ID throws Java NPE and maps to typed `InvalidRoute`, null instance with non-null ID is a no-op |
| removeClientFactory(id) | async `remove_client_factory_by_id` / `remove_client_factory_by_id_nullable(Option<&str>)`; source removal/replacement oracle, per-key initializer wait/cancel tests; null ID throws Java NPE and maps to typed `InvalidRoute`; does not replace identity-safe lifecycle cleanup |
| getFactoryTable | `factory_table` returns live clonable FactoryTable with nullable map reads/mutations and `FactoryKeySetView`/`FactoryValuesView`/`FactoryEntrySetView` null-input adapters; actual null-key map/view failures and required null-value failures map to typed `InvalidRoute`, while values-view `remove(null)` and null/invalid entry membership/removal return false; null `removeIf` predicates and null bulk collection arguments map to typed `InvalidRoute`; null-fallback present/missing outcomes; retained map/view mutations, value-membership, default-helper, remapping, `forEach`, six entry `setValue` outcomes, compute and `replaceAll` same-key writer race oracles; `FactoryEntryHandle` retains its captured value and writes replacement mappings; concurrent/ownership/property tests; published reads exclude private initializer reservations |
| `getOrCreateProduceAccumulator`; `ProduceAccumulator(String)` | `Producer::start` initializes the Rust-owned auto-batch engine. The directly constructible Java helper maps to `rocketmq_classic_producer::ProduceAccumulator::new(Option<String>)`, also re-exported by the facade; the pinned Java helper declares no public batching methods. The implementation-package `MQClientManager` cache is keyed by `buildMQClientId`; Rust shares the manager-scoped engine and live limits for that identity, retains the first Producer as each pending batch's owner, and stops/restarts workers without clearing held entries. The Java identity fixture and `auto_batch_is_shared_by_client_id_and_keeps_first_producer_owner` cover this flow. This closes that bounded cache/ownership slice only; broad P18-V01 remains PARTIAL. |

| Source returned ConcurrentMap capability | Native FactoryTable API |
| --- | --- |
| get/containsKey/containsValue/size/isEmpty/getOrDefault | `get/get_nullable(Option<&str>)/contains_key/contains_key_nullable(Option<&str>)/contains_value/contains_value_nullable(Option<&Arc<ClientRuntime>>)/len/is_empty/get_or_default/get_or_default_nullable(Option<&str>, Option<Arc<ClientRuntime>>)`, owned published values and Java identity equality; null-key `get`, `containsKey`, and `getOrDefault` plus null-value `containsValue` throw Java NPE and their nullable Rust adapters map those cases to `InvalidRoute`; a null default is preserved as `None` on a miss and ignored when a mapping exists |
| put/putIfAbsent/putAll | async `put/put_if_absent/put_all` plus `put_nullable/put_if_absent_nullable(Option<&str>, Option<Arc<ClientRuntime>>)` and `put_all_nullable/put_all_nullable_rows/put_all_nullable_iter`; `put_all` accepts any owned iterator of key/value pairs and follows its traversal order; nullable row iteration accepts any iterator, while the slice adapter forwards to it; null map/key/value failures map to typed `InvalidRoute` and prior per-key writes remain on failure; return preceding identity or None; putAll applies per-key writes, not a multi-key transaction |
| computeIfAbsent/computeIfPresent/compute/merge | async `compute_if_absent`/`compute_if_present`/`compute`/`merge`; `Option::None` represents Java null-result/no mapping or removal; each callback runs once while an exact-key gate serializes its read/callback/write sequence with table mutations and synchronous identity-safe manager removal; callbacks run outside the entries mutex, a Java/native oracle verifies a callback can read the current mapping, and distinct keys keep independent gates (ADR-0080) |
| replaceAll | async `replace_all` and `replace_all_with_nullable_results`; a null callback result maps to typed `InvalidRoute` at that entry and keeps earlier replacements; snapshots keys and runs each callback without the key gate, then compares callback input with the latest mapping using table-specific Java value equality and commits under the gate by identity; unequal concurrent values retry the callback, equal-but-distinct registry owners do not; a missing key ends that visit; this remains a per-key operation, not a whole-map transaction |
| replace(key,value)/replace(key,expected,value) | async `replace/replace_if_same` plus `replace_nullable/replace_if_equal_nullable`; nullable key or required value failures map to typed `InvalidRoute`; conditional comparison uses identity, matching MQClientInstance Object equality |
| remove(key)/remove(key,value) | async `remove/remove_if_same` plus `remove_nullable/remove_if_equal_nullable`; null keys map to typed `InvalidRoute`, while a null conditional value returns false; identity comparison, map changes only, no runtime shutdown |
| clear | async `clear`, not a transaction against concurrent insertion |
| keySet/values/entrySet | cloneable live `FactoryKeySetView`/`FactoryValuesView`/`FactoryEntrySetView` handles with contains/len/is_empty/snapshot/remove/remove_if/remove_all/retain_all/clear and nullable counterparts; exported `TableKeyViewPredicate`, `TableValueViewPredicate<T>`, and `TableEntryViewPredicate<T>` aliases carry nullable predicate arguments; `entry_snapshots()` returns `FactoryEntryHandle`s whose `set_value()` and `set_value_nullable()` return the entry-captured prior runtime and unconditionally replace the current mapping, while null setValue inputs map to typed `InvalidRoute`; conditional entry/value removals use runtime identity; predicates visit detached snapshots. Their `remove` methods represent the observable effects of Java iterator removal; view traversal itself remains detached |
| forEach | `for_each` visits one detached snapshot taken at traversal start and runs callbacks without a manager lock; this is one outcome permitted by ConcurrentHashMap's weak-iterator contract |
| copy/traverse map entries | explicit owned `snapshot`; view snapshots are detached, while retained view handles continue to read and mutate the live table |

### NameServer addressing public API

| Rust public declaration | Pinned Java API / behavior mapping |
| --- | --- |
| `TopAddressing::fetch_ns_addr` / `register_change_callback` | Java `TopAddressing.fetchNSAddr()` / `registerChangeCallBack(NameServerUpdateCallback)`; Rust uses an async nullable result and explicit callback handle in place of the Java blocking SPI. |
| `ClientRuntimeManager::with_top_addressing(provider)` | Java's public `TopAddressing` SPI, loaded by `DefaultTopAddressing` through `ServiceLoader` (the implementation selects the first provider). Rust supplies that capability explicitly and scopes it to a shareable manager; manager clones retain the provider. This is the manager's Rust injection form of the Java extension point, not a new NameServer setting or a factory method. |
| `NameServerUpdateCallback::on_name_server_address_change` | Java `NameServerUpdateCallback.onNameServerAddressChange(String)`; null input/result are represented as `Option`, and callback-driven address changes update the shared remoting NameServer list. |
| `DefaultTopAddressing::new(ws_addr, unit_name)` | Java one- and two-argument constructors; passing `None` represents the no-unit overload. |
| `DefaultTopAddressing::with_parameters(ws_addr, unit_name, parameters)` | Java three-argument constructor `(wsAddr, parameters, unitName)`; Rust argument order follows its builder signature while retaining the same URL composition. |
| `DefaultTopAddressing::with_provider` | Rust explicit `ServiceLoader` provider injection; chained calls retain Java iteration order, fetch returns the first non-null/non-empty address, and callback registration reaches every provider. |
| `DefaultTopAddressing::fetch_ns_addr_with_timeout(verbose, timeout_millis)` | Java `DefaultTopAddressing.fetchNSAddr(boolean, long)`; Rust retains the timeout unit and nullable failure result while performing the HTTP call asynchronously. |
| `DefaultTopAddressing::ns_addr` / `set_ns_addr` | Java `getNsAddr()` / `setNsAddr(String)`; pinned Java fixture and Rust differential cover the null default, set value, and reset to null. |

The pinned Java public signatures come from `rocketmq-common-5.5.1.jar`. The discovery HTTP/URL, callback, provider-fallback, and NameServer mutation cases are covered by `tests/discovery.rs`, the invoked runtime discovery fixture, and the runtime startup mutation cases above.

`fetch_name_server_addr/name_server_addr` map MQClientAPIImpl discovery state, not a new resolver feature. `route_snapshot`, `TopicSnapshot`, `RouteSnapshot`, `BrokerAddress`, and `broker_vip_channel` map Java route/broker/publish/subscribe/static/version tables; raw-table and published route copies have separate outer collections and share nested mutable DTO handles, matching Java copy construction. Raw table DTO aliases use `TopicRouteDataHandle`. `SharedValue<T>` is a small shared mutable DTO handle; the CheckClient body, LiteSubscription and Lite DTOs, and Lite control body use it to preserve their documented aliases. Other migrated handles are listed with their DTOs above; status/reset Map aliases are covered by [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md), heartbeat collection aliases by [ADR-0074](../architecture/ADR-0074-shared-heartbeat-set-aliases.md), and TopicRouteData collection aliases by [ADR-0076](../architecture/ADR-0076-shared-topic-route-data-collections.md). `prepare_heartbeat_data` exposes the Java heartbeat preparation capability for owners/tests; it does not add another periodic task. Failure-injection seams, queue probes, property readers and deterministic selection indices are private/test-only.

This inventory identifies source/public boundaries still requiring work; it does not close complete source-branch, malformed-input, rollback or final whole-plan verification. P6-V04 remains PENDING_PHASE_19 against server 5.3.4.

## Producer `SendResult` public fields

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| `getSendStatus` / `setSendStatus` | `SendResult::status: SendStatus`; enum names and values are mapped in the send-status enum row above |
| `getMsgId` / `setMsgId` | `msg_id`; populated from the unique message ID supplied to the send operation |
| `getOffsetMsgId` / `setOffsetMsgId` | `offset_msg_id: Option<String>`; populated from response-header `msgId` and preserves Java `null` when that field is absent |
| `getMessageQueue` / `setMessageQueue` | Flattened into `topic`, `broker_name`, and `queue_id`; Rust does not return a nested Java `MessageQueue` object here |
| `getQueueOffset` / `setQueueOffset` | `queue_offset` |
| `getTransactionId` / `setTransactionId` | `transaction_id: Option<String>` |
| `getRecallHandle` / `setRecallHandle` | `recall_handle: Option<String>` |
| `getRawRespBody` / `setRawRespBody(byte[])` | `raw_resp_body: Option<Vec<u8>>`; null is preserved and normal `MQClientAPIImpl` send results leave it `None` |
| `getRegionId` / `setRegionId` | `region_id` |
| `isTraceOn` / `setTraceOn` | `trace_on` |
| `SendMessageResponseHeader.getBatchUniqId` | `SendMessageResponseHeader::batch_uniq_id` only; Java `SendResult` has no `batchUniqId` accessor or field |

Pinned `javap -public` on Java `SendResult` and `SendMessageResponseHeader` establishes the accessor boundary. `ProducerMoreInvokedFixtures` invokes the result getter on ordinary and batch sync/async sends and checks that raw response body remains null; the Rust producer tests and invoked-request differential assert the same value. The invoked `SendMessageResponseHeader` decode-mode fixture also confirms that Java preserves a missing `msgId` as null in both fast and reflective decoding, so Rust `SendResult.offset_msg_id` is nullable. `queueId` and `queueOffset` are nullable in the decoded header too, but the send RPC owner requires both before constructing a `SendResult`. Request/response command fixtures continue to cover `batchUniqId` as a response-header input. This table records result-object field/API correspondence and the explicit flattened queue carrier; it does not claim producer-owner or send-retry parity (Phase 7).

## Other core response and route snapshot fields

| Rust public result/field | Pinned Java API / behavior mapping |
| --- | --- |
| `PullResultExt.status` | `PullResult.getPullStatus()`; response-code mapping to the four `PullStatus` values is fixture-backed |
| `PullResultExt.next_begin_offset`, `min_offset`, `max_offset` | `PullResult.getNextBeginOffset()`, `getMinOffset()`, and `getMaxOffset()` |
| `PullResultExt.suggest_which_broker_id` | `PullResultExt.getSuggestWhichBrokerId()` |
| `PullResultExt.offset_delta: Option<i64>` | `PullResultExt.getOffsetDelta(): Long`; preserves nullable extension-header value |
| `PullResultExt.raw_messages: Option<Vec<u8>>` | `PullResultExt.getMessageBinary(): byte[]`; Rust retains the wire body at the RPC boundary. Java `getMsgFoundList()` is populated by a later message-decoding step; Rust's consumer owner remains a later phase. |
| `HeartbeatV2Result.version`, `is_sub_change`, `is_support_heartbeat_v2` | `HeartbeatV2Result.getVersion()`, `isSubChange()`, and `isSupportV2()`; exact Java bean names are retained in this map despite the Rust field spelling |
| `PopReply<H>.status` | Java `PopCallback.onSuccess(PopResult).getPopStatus()`; request-code/status mappings and callback errors are invoked and fixture-backed |
| `PopReply<H>.header` | Parsed POP or POP Lite response header, including `popTime`, `invisibleTime`, `reviveQid`, and ordinary POP `restNum` |
| `PopReply<H>.raw_messages` | Raw wire message body returned before Java's `PopResult.getMsgFoundList()` decode; Rust message decoding belongs to its later consumer layer |
| `ChangeInvisibleTimeResult.status` | `AckResult.getStatus()` (`OK`/`NO_EXIST`) |
| `ChangeInvisibleTimeResult.pop_time`, `invisible_time`, `revive_qid` | `ChangeInvisibleTimeResponseHeader` getters; success exposes parsed values, and missing/invalid primitive values retain zero defaults |
| `ChangeInvisibleTimeResponseHeader` public constructor, `get/setPopTime`, `get/setInvisibleTime`, `get/setReviveQid`, and inherited identity methods | Crate-private Rust backing fields with Java-named getter/setter pairs and `CommandHeader` encode/decode support. It preserves Java's per-field zero defaults for absent/malformed values, signed integer limits, Unicode decimal parsing, and identity-based equality boundary. The pinned fixture checks the exact method inventory, zero public fields, both reflective decode hints, and encoded fields. |
| `ChangeInvisibleTimeResult.extra_info` | `AckResult.getExtraInfo()`; built from request offset, response values, topic retry class, broker name and queue ID; null on non-success |
| `TopicSnapshot.route` | Copied Java `TopicRouteData` as exposed by `TopicPublishInfo.getTopicRouteData()` |
| `TopicSnapshot.publish_queues`, `subscribe_queues` | Results of `MQClientInstance.topicRouteData2TopicPublishInfo/SubscribeInfo`; same route conversion and queue content, with Rust-native collection types |
| `TopicSnapshot.order_topic`, `endpoints` | Java ordered-route flag and static-topic endpoint conversion; queue/endpoint fixture cases are mapped in the route-conversion row above |

The pinned public API scan used `javap -public` on `PullResult`, `PullResultExt`, `HeartbeatV2Result`, `PopResult`, `AckResult`, and `ChangeInvisibleTimeResponseHeader`. Pull/POP Java and Rust invoked RPC fixtures compare response values; extension fixtures compare heartbeat and acknowledgement outcomes; route fixtures cover `TopicSnapshot` publication. `PopReply` and `PullResultExt` deliberately expose pre-decode bytes at the RPC boundary, so the high-level Java message-list carrier is owned by later consumer work.

## Remoting Netty event enum API: NettyEventType

| Pinned Java API | Rust counterpart / current behavior |
| --- | --- |
| `NettyEventType.values()` / `valueOf(String)` / inherited enum methods | `client::NettyEventType::values()`, `value_of_name`, `name`, `ordinal`, `Display`, and `java_compare_to()` match Java names, declaration order, and lookup failures |
| `NettyEvent.getType()` | Native channel event queue stores the public `NettyEventType` directly; listener callbacks remain event-specific and carry the remote address |
| `NettyEvent.getChannel()` | `ChannelEvent` and listener callbacks carry an identity-preserving `ChannelHandle` with address, state, close, and write capabilities. This maps the RocketMQ channel operations without exposing a Netty object or raw Tokio socket; generic Netty methods remain framework-specific. The request-processor context disposition is recorded below. |

The pinned Java fixture invokes all five constants, defensive `values()` copies, and null/unknown/case-mismatched name lookups; the native differential checks 12 outcomes. See [ADR-0033](../architecture/ADR-0033-netty-event-type-java-enum-api.md).

## Remoting public API: request processor context

| Pinned Java 5.5.1 API | Rust counterpart / disposition |
| --- | --- |
| `NettyRequestProcessor.processRequest(ChannelHandlerContext, RemotingCommand)` | `RequestProcessor::process_with_request_context(&RequestContext, RemotingCommand)`; the same channel-identity-preserving context is passed through request-pipeline stages and the processor. Legacy address-only processing adapts through `process_with_context`. |
| `NettyRequestProcessor.rejectRequest()` (default `false`) | `RequestProcessor::reject_request()` (default `false`); `RequestProcessorRegistration` selects an optional per-code Rust executor, with the runtime public executor as its default. |
| `ChannelHandlerContext` operations used by RocketMQ request dispatch | Rust `RequestContext` and `ChannelHandle` expose peer address, channel identity/state, close, and typed `RemotingCommand` writes with or without an operation deadline. The remaining allocator, handler propagation, arbitrary-object writes, promise, and raw Netty channel APIs belong to Netty; Rust uses Tokio and does not claim to recreate that framework surface. |

Pinned source: `remoting/.../NettyRequestProcessor.java` and `NettyRemotingAbstract.buildProcessRequestHandler` at Java commit `efa1be59d6fc747ad220f036c86030f05f0419f9`. Rust coverage: `p18_request_context_exposes_channel_state_and_closes_its_channel`, `p18_request_context_preserves_channel_identity_through_pipeline_and_processor`, `p18_channel_handle_writes_commands_and_closes_its_connection`, `p3_processor_specific_and_default_executors_route_or_reject_tasks`, and `p3_request_pipeline_pipe_runs_new_stage_before_existing_stage`. The separate processor exception-remark formatting difference remains recorded in the Phase 3 inventory; this mapping closes the RocketMQ request-context capability review only.

## Remoting request source enum API: RequestSource

| Pinned Java API | Rust counterpart / current behavior |
| --- | --- |
| `RequestSource.values()` / `valueOf(String)` / inherited enum methods | `remoting::RequestSource::values()`, `value_of_name`, `name`, `ordinal`, `Display`, and `java_compare_to()` match Java declaration order and lookup failures |
| `getValue()` | `get_value()` returns `-1`, `0`, `1`, and `2` in enum order |
| `isValid(Integer)` / `parseInteger(Integer)` | `is_valid(Option<i32>)` accepts `-1..=2`; `parse_integer` maps those values and falls back to `Sdk` for null/out-of-range input |
| `SYSTEM_PROPERTY_KEY` | `RequestSource::SYSTEM_PROPERTY_KEY` exposes `rocketmq.requestSource`; the pinned artifacts contain no call site outside the enum, so no property-loading behavior is claimed |

The pinned fixture invokes 23 enum/integer outcomes plus the property-key constant. See [ADR-0034](../architecture/ADR-0034-request-source-java-enum-api.md).

## Remoting route-state enum API: MessageQueueRouteState

| Pinned Java API | Rust counterpart / current behavior |
| --- | --- |
| `MessageQueueRouteState.values()` / `valueOf(String)` / inherited enum methods | `remoting::MessageQueueRouteState::values()`, `value_of_name`, `name`, `ordinal`, `Display`, and `java_compare_to()` match Java declaration order and lookup behavior |

The direct fixture covers all four route-state constants and null, empty, unknown, and case-mismatched lookups. The pinned artifacts contain no call sites outside the enum; no state transition behavior is claimed. See [ADR-0035](../architecture/ADR-0035-message-queue-route-state-enum-api.md).

## Response body DTO: GetConsumerStatusBody

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| no-arg constructor; `messageQueueTable` and `consumerTable` initialized to empty maps | `GetConsumerStatusBody::default()` initializes both fields to `Some(empty)` |
| `getMessageQueueTable` / `setMessageQueueTable(Map<MessageQueue, Long>)` | `get_message_queue_table` / `get_message_queue_table_mut` / `set_message_queue_table`; `Option<SharedQueueOffsetEntries>` preserves the assigned outer Map handle and nullable queue/offset entries |
| `getConsumerTable` / `setConsumerTable(Map<String, Map<MessageQueue, Long>>)` | `get_consumer_table` / `get_consumer_table_mut` / `set_consumer_table`; `Option<SharedConsumerQueueOffsets>` preserves the outer Map handle and each non-null nested Map handle, including nullable values |
| inherited `decode` / `encode` with Fastjson object map keys | `decode_java_json` parses Java's object-valued MessageQueue keys; `encode_java_json` emits the same body syntax and null omissions |

One hundred forty-three direct Java input cases cover decoded fields, accepted/rejected input shapes and exact body encoding. Additional pinned Java API cases cover constructor defaults, getter/setter values, null setters, and mutation through the assigned and returned outer/nested Maps; Rust preserves outer and non-null nested Map identity through shared handles, with `ptr_eq` and bidirectional mutation checks. `MessageQueue` key identity is not claimed. `BTreeMap` output is deterministic; Java HashMap iteration order is unspecified. See [ADR-0047](../architecture/ADR-0047-consumer-status-fastjson-map-input.md), [ADR-0057](../architecture/ADR-0057-runtime-map-body-public-api.md), and [ADR-0073](../architecture/ADR-0073-shared-runtime-map-body-aliases.md).

## ConsumerRunningInfo DTO identity traits

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| `ConsumerRunningInfo`, `ProcessQueueInfo`, `PopProcessQueueInfo`, and `ConsumeStatus`; each inherits `Object.equals/hashCode` | `ConsumerRunningInfoBody`, `ConsumerProcessQueueInfo`, `ConsumerPopProcessQueueInfo`, and `ConsumerConsumeStatus` intentionally do not implement structural `PartialEq`/`Eq` |

The invoked Java fixture reflects both inherited methods and confirms that separately constructed default instances compare unequal for all four classes. Compile-fail doctests guard the Rust trait boundary. The Rust body round-trip differential compares normalized JSON values, preserving the wire/API check without adding value equality. The fixture now also carries the invoked public-field/API inventories and the nested DTO text-rendering outputs. Fixture SHA-256: `cefcb0d2f68f88acc12996a5068e9f12449676d8313b502fe8d2e9eeccbc69c5`.

### Running Info public property-key constants

| Pinned Java 5.5.1 field | Java string value | Rust counterpart |
| --- | --- | --- |
| `PROP_NAMESERVER_ADDR` | `PROP_NAMESERVER_ADDR` | `ConsumerRunningInfoBody::PROP_NAMESERVER_ADDR` |
| `PROP_THREADPOOL_CORE_SIZE` | `PROP_THREADPOOL_CORE_SIZE` | `ConsumerRunningInfoBody::PROP_THREADPOOL_CORE_SIZE` |
| `PROP_CONSUME_ORDERLY` | `PROP_CONSUMEORDERLY` | `ConsumerRunningInfoBody::PROP_CONSUME_ORDERLY` |
| `PROP_CONSUME_TYPE` | `PROP_CONSUME_TYPE` | `ConsumerRunningInfoBody::PROP_CONSUME_TYPE` |
| `PROP_CLIENT_VERSION` | `PROP_CLIENT_VERSION` | `ConsumerRunningInfoBody::PROP_CLIENT_VERSION` |
| `PROP_CONSUMER_START_TIMESTAMP` | `PROP_CONSUMER_START_TIMESTAMP` | `ConsumerRunningInfoBody::PROP_CONSUMER_START_TIMESTAMP` |

The DTO's Java instance accessors map to public Rust getters/setters while the backing fields remain private, matching Java visibility; the typed body and its nested queue/status structs retain the corresponding wire data. Push/Pull Running Info generation uses these constants. The regression fixture pins the unusual `PROP_CONSUME_ORDERLY` value and the generated Push body rejects the incorrect underscore spelling. Fixture SHA-256 values: `fixtures/consumer/running-info.json` `cefcb0d2f68f88acc12996a5068e9f12449676d8313b502fe8d2e9eeccbc69c5`; dependent `fixtures/admin/query-consume-time-span.json` `a2702341491aea02ece8494c65ac0dc5544e69947d42987414c171f2fb96cc31`.

### Running Info nested DTO text rendering

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| `ProcessQueueInfo.toString()` | `ConsumerProcessQueueInfo` implements `Display`/`ToString` with Java field labels, timestamp formatting, and output text |
| `PopProcessQueueInfo.toString()` | `ConsumerPopProcessQueueInfo` implements `Display`/`ToString` with Java labels, separators, and output text |

The invoked Java fixture records both rendered strings; the Protocol differential compares them exactly. The public `ConsumerRunningInfo` analysis/formatting helpers remain excluded from client flow because pinned-source call sites place them in the separate `rocketmq-tools` artifact.

`analyzeSubscription`, `isPushType`, `analyzeRebalance`, `analyzeProcessQueue`, and `formatString` have call sites in the separate `rocketmq-tools` artifact; pinned-source search found no `rocketmq-client` callers. Phase 17 explicitly excludes the tools suite, so these tools-only helpers are not client-flow gaps.

## Protocol body DTO identity traits

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| `QueryAssignmentRequestBody`, `SetMessageRequestModeRequestBody`, `GetConsumerStatusBody`, `ResetOffsetBody`, and `ConsumeMessageDirectlyResult`; each inherits `Object.equals/hashCode` | Corresponding Rust protocol DTOs intentionally do not implement structural `PartialEq`/`Eq` |

The invoked `ProtocolDtoIdentityFixtures` reflects both inherited methods and confirms self-equality plus unequal distinct default instances for all five classes. Compile-fail doctests guard the Rust trait boundaries. Fixture: `fixtures/protocol/dto-identity.json`, SHA-256 `11f97e14fc3386c4ef61a6d82aa27f8e8b9b723a965fb3ddfb3b30233f0be0d9`.

## Protocol response-header identity traits

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| `AddWritePermOfBrokerResponseHeader`, `ChangeInvisibleTimeResponseHeader`, `GetAllSubscriptionGroupResponseHeader`, `GetAllTopicConfigResponseHeader`, `GetKvConfigResponseHeader` (Java `GetKVConfigResponseHeader`), `WipeWritePermOfBrokerResponseHeader`, `QueryConsumerOffsetResponseHeader`, `SearchOffsetResponseHeader`, `GetMaxOffsetResponseHeader`, `GetMinOffsetResponseHeader`, `GetEarliestMsgStoretimeResponseHeader`, `PullMessageResponseHeader`, `QueryMessageResponseHeader`, `RecallMessageResponseHeader`, `SendMessageResponseHeader`, `UnregisterClientResponseHeader`, `EndTransactionResponseHeader`, `CheckTransactionStateResponseHeader`, `NotificationResponseHeader`, `PopMessageResponseHeader`, `PopLiteMessageResponseHeader`, and `PollingInfoResponseHeader`; each inherits `Object.equals/hashCode` | Corresponding Rust response-header DTOs intentionally do not implement structural `PartialEq`/`Eq` |

`HeaderResponseIdentityFixtures` reflects both inherited methods and confirms self-equality plus unequal distinct default instances for all 22 Java classes. The six newly inventoried headers include three NameServer-package classes (`AddWritePermOfBrokerResponseHeader`, `GetKVConfigResponseHeader`, and `WipeWritePermOfBrokerResponseHeader`); the Rust `GetKvConfigResponseHeader` name maps to Java's capitalization. Compile-fail doctests guard every Rust header boundary. Fixture: `fixtures/protocol/header-response-identity.json`, SHA-256 `4e17f64398e2ae742f1fae55841e365eb7aed89b3f6aaad7eb191e8d282dd8ae`.

## Protocol response-header field visibility and accessors

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| `PullMessageResponseHeader` | Crate-private fields with Java `get*`/`set*` pairs; boxed values remain `Option` |
| `SendMessageResponseHeader` | Crate-private fields with Java accessor pairs for message ID, queue ID/offset, transaction ID, batch unique ID, and recall handle |
| `CheckTransactionStateResponseHeader` | Crate-private nullable fields with Java getter/setter pairs, including `getTranStateTableOffset` |
| `NotificationResponseHeader` | Crate-private booleans with Java `isHasMsg`/`isPollingFull` and setter pairs |
| `PopMessageResponseHeader`, `PopLiteMessageResponseHeader` | Crate-private fields with Java accessor pairs, including `getReviveQid`; primitive defaults and nullable strings are preserved |
| `PollingInfoResponseHeader` | Crate-private `pollingNum` with Java `getPollingNum`/`setPollingNum` |

`HeaderResponseIdentityFixtures` records constructor, public-field, declared-method, and callable-method inventories for all 22 response headers. The seven listed above expose no public fields in Java; Rust compile-fail doctests guard representative field accesses, and the Protocol integration test checks Java accessor inventories and null/default behavior across the complete inventory. `ChangeInvisibleTimeResponseHeader` retains its separate fixture with its exact method list and `publicFieldCount=0`, SHA-256 `2f4a383eec8057a43d97dbd053f2e8c9279d908ec947efcdfb5cfb37393b25bb`. Remoting and Core callers and fixture generation use the accessors. `header-response-identity.json` SHA-256: `4e17f64398e2ae742f1fae55841e365eb7aed89b3f6aaad7eb191e8d282dd8ae`.

## Protocol request-header inherited equality

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| 50 request-header classes in the pinned identity/API fixture that inherit `RpcRequestHeader.equals/hashCode`, including `TopicRequestHeader` and `TopicQueueRequestHeader` descendants | Rust request-header DTOs retain and encode/decode nullable `ns`, `nsd`, `bname`, and `oway`; `PartialEq`, `Eq`, and `Hash` use only those four values, ignoring subclass payload and `TopicRequestHeader.lo`. `UnlockBatchMqRequestHeader` has its own type instead of aliasing the Lock class. `ConsumeMessageDirectlyResultRequestHeader` keeps its subclass `brokerName` separate from inherited `bname`. `NotifyUnsubscribeLiteRequestHeader` now has the same inherited RPC fields, accessors, wire mapping, and base-only equality/hash behavior. |

`RequestHeaderIdentityFixtures` invokes all 50 pinned Java classes in the current identity/API inventory. It confirms that each inherited method is declared by `RpcRequestHeader`, distinct default instances compare equal, any changed base field compares unequal, and payload-only changes leave equality/hash unchanged when the class has a payload field. Rust tests exercise the same four base fields, payload-insensitive equality/hash, and inherited-field codec round-trips for all 50 types. Fixture: `fixtures/protocol/request-header-identity.json`, SHA-256 `38c163fea5ca151b00c450e709cc7006cb442e39799c34b3eecde930d4eeb10d`.

## Protocol request-header field visibility and accessors

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| 50 request-header identity/API inventories, including `LiteSubscriptionCtlRequestHeader` and `NotifyUnsubscribeLiteRequestHeader` | Rust backing fields are private; public snake-case getter/setter pairs map the Java callable accessor inventory. The Lite subscription control and unsubscribe notification headers retain and encode nullable `ns`, `nsd`, `bname`, and `oway`, and their Java inherited equality/hash behavior compares those fields. The `ConsumeMessageDirectlyResultRequestHeader` inherited `lo` field and `PullMessageRequestHeader.liteTopic` are retained by typed encode/decode. |
| `CreateTopicRequestHeader.getTopicFilterTypeEnum()` | `get_topic_filter_type_enum()` maps `SINGLE_TAG`/`MULTI_TAG` to `TopicFilterType` and returns `ProtocolError` for Java `null` or `valueOf` failures; `check_fields()` uses the same conversion. |
| `FastCodesHeader.getAndCheckNotNull` and `writeIfNotNull` on the four implementing headers | Public Rust `FastCodesHeader` default methods provide nullable field lookup and omission of null values through `HeaderMap`; the trait is implemented by Pull request/response and Send V2 request/response headers. |

The Java identity fixture records constructors, public fields, declared methods, and callable methods for all 50 identity/API entries. The Rust inventory test requires Java's public-field list to be empty, verifies each Rust field is private, and cross-checks each Java getter/setter and the two `FastCodesHeader` helpers against the Rust API. Compile-fail doctests guard representative field visibility. Rust identity tests explicitly exercise all 50 fixture entries. Java typed fixtures exercise `liteTopic` and inherited `lo`. `request-header-identity.json` SHA-256: `38c163fea5ca151b00c450e709cc7006cb442e39799c34b3eecde930d4eeb10d`.

`RemainingHeaderApiFixtures` adds Java 5.5.1 constructor, field, declared-method, and callable-method inventories for the 25 request-header classes previously missing from API evidence. The mapping records the NameServer package and `GetKV` spelling, and maps Rust `GetConsumeStatsInBrokerRequestHeader` to Java `GetConsumeStatsInBrokerHeader`. `remaining-header-api.json` SHA-256: `54df2a5effcb30ecdab9303c78d404a05916576bec60d1f33e540ec28ae6b688`. The Protocol tests check no-argument construction against Rust `Default`, private fields, accessor mappings, and a five-fixture union against every one of the 103 public Rust `CommandHeader` implementations. The union normalizes Java's `DeleteKVConfigRequestHeader` spelling and excludes the unrelated `ExportRocksDBConfigType` enum.

## Public consumer notification result DTO

Pinned `org.apache.rocketmq.client.consumer.NotifyResult` has private
`hasMsg`/`pollingFull` fields, one public no-argument constructor, the two
boolean getters, two setters, and a stable `toString()`. Rust exposes
`NotifyResult` from the product facade with private fields, `Default`, matching
snake-case getters/setters, and the Java string form through `Display`; it
does not add value equality. The invoked `MQClientAPIExt` notification fixtures
assert the exact Java constructor/method/public-field inventory, defaults, and
string value.

## Lite topic information RPC and DTOs

`MQClientAPIExt.getLiteTopicInfoAsync` is exposed as `MQClientApi::get_lite_topic_info`.
It sends request 200076 with nullable `parentTopic`/`liteTopic` fields, uses the
future remoting path, maps Broker errors, and returns `None` for a successful
response with no body. A present body maps Java `GetLiteTopicInfoResponseBody`;
its subscriber `Set` and nested `TopicOffset` retain setter/getter identity
through shared handles, and `ClientGroup` equality/hash use its two nullable
values. The invoked Java fixtures cover constructors, field visibility,
accessors, defaults, aliases, successful populated/default bodies, null body,
null request inputs, known/unknown response codes, and invalid JSON. The Core
scripted peer checks the request ext fields, body, result values and error
category. `POLLING_INFO` remains a registry/wire model because pinned 5.5.1 has
no outbound client callsite.

`MQClientAPIImpl.getParentTopicInfo` is exposed as
`MQClientApi::get_parent_topic_info`. It sends request 200075 synchronously
with nullable `topic` in `GetParentTopicInfoRequestHeader`; Java's no-op
`checkFields()` means null is omitted without local rejection. Rust applies
the configured VIP address transform and caller deadline, maps absent success
bodies to `None`, and decodes `GetParentTopicInfoResponseBody` with Java's
primitive defaults, nullable topic/groups, and mutable Set getter alias. The
seven invoked Java cases pin the method/header/DTO inventories and cover
populated/default responses, absent/malformed bodies, null topic, and Broker
errors; the Rust fixture differential checks request code/fields and decoded
outcomes, and a VIP loopback checks the transformed destination.

`MQClientAPIImpl.getLiteClientInfo` is exposed as
`MQClientApi::get_lite_client_info`. It sends request 200077 synchronously with
nullable `parentTopic`, `group`, and `clientId`; the request header sends
`maxCount=1000` by default and rejects nonpositive explicit values. Rust
applies the configured VIP address transform and caller deadline, maps an
absent success body to `None`, and decodes `GetLiteClientInfoResponseBody` with
primitive defaults, nullable strings/Set members, and a mutable Set getter
alias. The seven invoked Java cases pin RPC/header/DTO inventories and cover
populated/default responses, null inputs/body, malformed JSON and Broker
errors; the Rust differential compares request fields and decoded values, and
a VIP loopback verifies the transformed destination.

`MQClientAPIImpl.getLiteGroupInfo` is exposed as
`MQClientApi::get_lite_group_info`. It sends request 200078 synchronously with
nullable `group` and `liteTopic` plus primitive `topK` (default 0); Java's
no-op `checkFields()` accepts negative values. Rust applies the configured VIP
address transform and caller deadline, maps an absent success body to `None`,
and decodes `GetLiteGroupInfoResponseBody` with nullable strings, aliased
`OffsetWrapper`, nullable lag lists and list elements, and Java primitive
defaults. `LiteLagInfo.consumerOffset` defaults to -1 on decode and is omitted
when Fastjson serializes the DTO. Seven invoked Java cases pin the method,
header and DTO inventories and cover populated/default responses, nullable
inputs and list items, absent/malformed bodies, and Broker errors; the Rust
differential checks request fields and decoded values, and a VIP loopback
verifies the transformed destination.

`ClientGroup.clientId` and `ClientGroup.group` are public final Java fields.
Rust keeps both fields private and exposes read-only `client_id()`/`group()`
accessors, so callers retain value access without gaining Java-absent mutation.
Compile-fail doctests guard both field names; the invoked fixture's constructor,
equality, hash, and text behavior remain unchanged.

## NameServer address refresh

Pinned `MQClientAPIExt.updateNameServerAddressList()` rereads its retained
`ClientConfig.namesrvAddr`, returns false without changing the remoting list
when the value is null, and applies even an empty string when present. Rust
provides `ClientRuntime::update_name_server_address_list_from_config()` over
its retained shared config. `nameserver-refresh-{present,null,empty}` fixtures
and the runtime test compare the return value and resulting address membership;
the existing parameter-taking runtime helper remains the direct list-update
entry used by discovery callbacks.

## Common public API: TopicValidator

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| `AUTO_CREATE_TOPIC_KEY_TOPIC`, the `RMQ_SYS_*` constants, `SYSTEM_TOPIC_PREFIX`, `SYNC_BROKER_MEMBER_GROUP_PREFIX` | Matching `TopicValidator` associated constants |
| `VALID_CHAR_BIT_MAP` | `valid_char_bit_map() -> &'static RwLock<[bool; 128]>`; mutation remains synchronized for Rust callers and affects subsequent validation |
| `isTopicOrGroupIllegal(String)` | `is_topic_or_group_illegal(&str)` applies the Java 128-entry UTF-16 character table; `is_topic_or_group_illegal_nullable(Option<&str>)` maps Java's null-input `NullPointerException` to `TopicValidatorError::NullString` |
| `validateTopic(String)` / `validateGroup(String)` | `validate_topic(Option<&str>)` / `validate_group(Option<&str>)` return the Java-compatible validation flag and remark |
| `ValidateResult(boolean, String)` / `isValid()` / `getRemark()`; inherited `Object.equals/hashCode` | `ValidateResult::new(bool, Option<String>)`, `is_valid()`, and `remark() -> Option<&str>` preserve nullable constructor values; no Rust `PartialEq`/`Eq` implementation adds structural equality that Java lacks |
| `isSystemTopic`, `isNotAllowedSendTopic`, `addSystemTopic`, `getSystemTopicSet`, and `getNotAllowedSendTopicSet` | `is_system_topic` / `is_system_topic_nullable`, `is_not_allowed_send_topic` / `is_not_allowed_send_topic_nullable`, `add_system_topic` / `add_system_topic_nullable`, `system_topic_set`, and `not_allowed_send_topic_set`. The getters expose Java's mutable sets; Rust uses synchronized `HashSet<Option<String>>` storage to preserve concurrent access and nullable set members. |

The pinned fixture invokes all constants, validation/classification methods, mutable table/set behavior, and six direct constructor cases covering both valid flags with null, empty, and non-empty remarks. It also checks Java null-input exceptions for `isTopicOrGroupIllegal` and `isSystemTopic`, null membership before and after adding null to the mutable sets, and non-null classification cases. For `ValidateResult`, the fixture reflects that `equals` and `hashCode` are inherited from `java.lang.Object`, verifies self-equality, and verifies that a second instance with identical fields is unequal. A compile-fail rustdoc example guards against adding structural `Eq` to the Rust type. `public_topic_validator_surface_matches_pinned_java_fixture` compares Java outcomes with the nullable Rust adapters and the constructor, including null versus empty/non-null remarks. Updated fixture SHA-256: `5f6b913ce7174c578fe069cd3d1b6badb44c971b8c16c164378ab6129dc23c55`.

## Common public API: PermName permission masks

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| `INDEX_PERM_PRIORITY`, `INDEX_PERM_READ`, `INDEX_PERM_WRITE`, `INDEX_PERM_INHERIT`; `PERM_PRIORITY`, `PERM_READ`, `PERM_WRITE`, `PERM_INHERIT` | Matching public associated constants on `rocketmq_classic_common::PermName`, also re-exported by the top-level facade |
| `perm2String(int)`, `isReadable(int)`, `isWriteable(int)`, `isInherited(int)`, `isValid(int)`, `isValid(String)`, `isPriority(int)`, `isAccessible(int)` | `PermName::{perm_to_string,is_readable,is_writeable,is_inherited,is_valid,is_valid_string,is_priority,is_accessible}`; the string adapter uses Java-compatible `Integer.parseInt` behavior and returns `PermissionParseError` for malformed or overflowing input |

The pinned Producer fixture reflects all eight public fields and methods and
invokes numeric and string inputs, including negative and out-of-range masks,
Arabic-Indic digits, leading plus, whitespace, malformed signs and overflow.
`perm_name_numeric_methods_match_the_pinned_java_fixture`,
`perm_name_string_overload_matches_java_parse_and_validation`, and
`perm_name_constants_match_java_public_fields` compare the Rust API to those
results. The fixture SHA-256 is
`8216bf9c1a66b57a61e88af448f61ab38dc84015ffd872ee340d68896d3ec9b5`. This
closes the mapped permission helper slice only; the Common/P18-V01 source,
input, error, flow and reverse-export audit remains open.

## Common public API: Message diagnostic formatting

| Pinned Java 5.5.1 API | Rust counterpart / current boundary |
| --- | --- |
| `Message.toString()` / `MessageExt.toString()` | Rust `Display` implementations provide the corresponding `.to_string()` behavior; internal `java_to_string` formatting helpers are not exposed as separate public methods |
| `InetSocketAddress.toString()` text used by `MessageExt` host fields | `MessageHostAddress` implements `Display`/`ToString`; its internal formatter is not a separate public API |

Compile-fail rustdoc examples guard the three removed helper exports. `message_public_api_matches_java_551_fixture`, `message_ext_system_flag_helpers_match_java_551`, and `message_ext_socket_address_api_matches_java_551` continue to verify the public formatting and host-address behavior. The `Message`, `MessageExt`, and `MessageClientExt` declared and inherited API crosswalks are recorded below; the workspace-wide reverse-export/source-flow audit remains part of P18-V01.

`messageExtPublicApi` reflects all 44 declared public `MessageExt` methods, and `messageClientExtApi` reflects all four declared public `MessageClientExt` methods. Rust tests pin both sorted Java signature lists alongside constructor, scalar, host, and client-ID behavior. The complete crosswalk is recorded below; the workspace-wide Rust/Common API reconciliation remains open.

Direct-consume subtype follow-up (2026-10-21): `MessageClientExt` converts to
the Rust base input with a retained subtype marker, matching Java's polymorphic
`MessageExt` parameter. `message_id_for_runtime_type` reads the live unique ID
property and physical-ID fallback, and `set_message_id` stays a no-op on the
marked value. The invoked runtime fixture records the callback's actual class.
This closes that direct-consume subtype path only; other subclasses and the
workspace-wide reverse-surface audit remain open.

## Common public API: Message equality semantics

| Pinned Java 5.5.1 API | Rust counterpart / current boundary |
| --- | --- |
| `Message.equals` / `hashCode`, inherited from `Object` | `Message` implements `PartialEq`/`Eq`/`Hash` with a hidden identity token, not field equality; mutation does not change the hash |
| `MessageExt` and `MessageClientExt` inherit `Object.equals/hashCode` through `Message` | All three Rust types compare and hash the same base identity token, including cross-type base/subclass views; constructors, decoding, and deserialization create new identities, while Rust `Clone` preserves its handle token and `Message::clone_message` creates the Java helper's distinct object |

The invoked `messagePublicApi` fixture verifies that `equals` and `hashCode` are inherited from `Object`, each Java object equals itself, distinct instances with identical fields are unequal, hash codes remain stable after field mutation, and base/subclass views compare equal for the same object. Rust's `message_object_identity_traits_match_java_551_fixture` covers `Message`, `MessageExt`, and `MessageClientExt`, cross-type inheritance views, identity-preserving hashing across mutation, Rust `Clone` token preservation, and distinct identities for `clone_message`, decode, and deserialization. `MessageExt` and `MessageClientExt` share the identity of their inherited message view. This closes only this identity-trait reverse-surface slice; the broader Common constructor, property, overload/input, inherited API, and P18-V01 reconciliation remains open. Fixture SHA-256: `2118277b7aad68e1077d92371d5ec14bd8cb77535e272dca406bc3e716d85792`.

## Common public API: Message user-property whitespace validation

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| `Message.putUserProperty(String, String)` rejects keys or values empty after Java `String.trim()` | `put_user_property` and `put_user_property_nullable` trim only code points U+0000 through U+0020 before the empty check |

The invoked Java fixture confirms non-breaking-space keys and em-space values are accepted while ASCII control/space-only keys are rejected. Rust fixture cases compare those outcomes. This closes the `String.trim()` input boundary only; other property inputs and the broader Common/P18-V01 audit remain open.

## Common public API: mutable reserved-property set

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| `MessageConst.STRING_HASH_SET` is a public mutable `HashSet<String>` that may contain null and is consulted before `Message.putUserProperty` validates blank names | `message::constants::STRING_HASH_SET` is a process-wide `LazyLock<RwLock<HashSet<Option<String>>>>`; each insertion checks current membership before Java-compatible blank validation |

The invoked Java fixture adds a custom reserved name, null, and the empty string, then removes `KEYS`; it verifies live membership and that null/empty members reject matching input with the base `RuntimeException` before blank-name validation. Rust represents null membership as `None`, restores each mutation after the differential, and checks membership before validating nullable and non-null keys. Current `fixtures/messages/records.json` SHA-256: `d0cdfbf2599adb0242a1df0dfa8523818a018c35cb34dac2b8418f8ce4bc972d`. This closes the mutable-set null/blank membership and precedence slice only; the remaining Common public API and P18-V01 audit stay open.

## Common public API: Message priority

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| `Message.setPriority(int)` rejects negative values; `Message.getPriority()` parses `PROPERTY_PRIORITY` with `NumberUtils.toInt(..., -1)` | `Message::set_priority` returns `MessagePropertyError::NegativePriority` for negatives; `Message::priority` returns `-1` for absent, malformed, or overflowing values and uses the Java-compatible decimal parser |

The invoked Java fixture and Rust regression cover absent-property map initialization, zero/positive/negative setter values, no mutation after rejected negative input, signed `i32` boundaries, malformed and whitespace inputs, overflow, and Arabic-Indic decimal digits. This closes the priority accessor slice only; broad Common/P18-V01 reconciliation remains open.

P18-V01 subtype follow-up (code revision `da1f4b1e`, 2026-10-21):
`MessageExtSubtype` dispatches virtual `getPriority()` and `setPriority(int)`.
The inherited Rust getter falls back through subtype-aware `getProperty`,
matching Java's delegation; the inherited setter keeps the base negative-value
error and property write. A custom subtype fixture overrides both methods and
accepts `-7`. Fixture SHA-256:
`44eb8359e7df5735cb527a59942190fb74dd4191306a4290ea3bba644a777a5c`.
This closes the custom priority override slice only; broad P18-V01 remains open.

## Common public API: CorrelationIdUtil

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| `CorrelationIdUtil.createCorrelationId()` returns `UUID.randomUUID().toString()` | `rocketmq-classic-common::utils::create_correlation_id()` returns lowercase UUID v4 text; Producer request preparation uses this Common API |

The invoked `request-reply.json` fixture inventories the exact public method signature, and the Common integration test checks UUID v4 version/variant, lowercase hexadecimal digits, and the 8-4-4-4-12 hyphen layout. This closes the surfaced CorrelationIdUtil mapping only; broad P18-V01 reconciliation remains open.

## Common public API: NameServerAddressUtils

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| `INSTANCE_PREFIX`, `INSTANCE_REGEX`, `ENDPOINT_PREFIX`, `NAMESRV_ENDPOINT_PATTERN`, `INST_ENDPOINT_PATTERN`; `getNameServerAddresses`, `validateInstanceEndpoint`, `parseInstanceIdFromEndpoint`, and `getNameSrvAddrFromNamesrvEndpoint` | `utils::name_server_address` exposes the five pattern/value constants and four snake-case methods. Java Pattern fields map to source constants plus Rust matcher functions; null and invalid substring outcomes use typed Rust results. `get_name_server_addresses` reads the shared Java-property overlay first, then `rocketmq.namesrv.addr` from the process environment, then the `NAMESRV_ADDR` environment fallback. |

The invoked `RuntimeConfigFixtures` records every declared public field/method and calls the patterns and helpers over null, empty, malformed, Unicode, path, and line-terminator inputs. The Rust fixture test compares those outcomes. Core's `config/namespace.rs` now delegates address extraction, instance-endpoint validation, and instance-ID parsing to these Common functions, so the direct API and ClientConfig path share the same rules. Current `fixtures/runtime/client-config.json` SHA-256: `39b4944cf67a65dc9c86e203826ceb8e43b3650d72e4ad5cb19af9234f17e19c`. This closes the surfaced NameServerAddressUtils mapping only; broad Common/P18-V01 reconciliation remains open.

## Common public API: Message body array aliases

| Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| `Message.getBody()` returns the live mutable `byte[]`; `Message.setBody(byte[])` and all four body-bearing constructors retain the supplied array reference | `Message::body` and `body_mut` expose borrowed read/write views. `SharedMessageBody` with `set_body_shared` and `shared_body` preserves setter caller/message identity across owned boundaries. `new_shared`, `new_nullable_shared`, `with_tags_shared`, `with_tags_and_keys_shared`, `with_fields_shared`, and `with_nullable_fields_shared` preserve constructor caller/message identity. `with_bytes` and `with_bytes_mut` provide synchronized live access. Owned `set_body` and `set_body_nullable` replace the message's body storage. |

The invoked `messagePublicApi` fixture checks identity for the two-argument, tags, tags-and-keys, and six-argument constructors; caller mutation after construction; setter identity and bidirectional mutation; and detachment after replacement. `message_public_api_matches_java_551_fixture` compares those behaviors with Rust's shared handle and owned replacement setters. Current `records.json` SHA-256: `d0cdfbf2599adb0242a1df0dfa8523818a018c35cb34dac2b8418f8ce4bc972d`. This closes the body-array alias slice; the broader `MessageExt` inherited API and workspace-wide source, input, error, control-flow, and reverse-export audit remain open.

## Common public API: Message constructors and declared methods

The pinned Java 5.5.1 `Message.java` at source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9` declares five public constructors
and 34 public methods. The fixture records the complete sorted declarations;
the following crosswalk covers each constructor and method family.

| Java 5.5.1 declaration | Rust counterpart / behavior |
| --- | --- |
| `Message()`; `(String, byte[])`; `(String, String, byte[])`; `(String, String, String, byte[])`; `(String, String, String, int, byte[], boolean)` | `Default`, `new_nullable`/`new_nullable_shared`, `with_tags`/`with_tags_shared`, `with_tags_and_keys`/`with_tags_and_keys_shared`, and `with_fields`/`with_fields_shared` or `with_nullable_fields`/`with_nullable_fields_shared`. Constructor defaults, nullable topic/body, tags/keys omission, flag, and wait-store property match the invoked Java outcomes. |
| `getTopic` / `setTopic`; `getFlag` / `setFlag`; `getBody` / `setBody` | `topic` / `set_topic` plus nullable forms, `flag` / `set_flag`, and the body views/replacement methods in [Message body array aliases](#common-public-api-message-body-array-aliases). Nullable body/topic values are preserved. |
| `getTags` / `setTags`; `getKeys`; `setKeys(String)` | `tags` / `set_tags` and `set_tags_nullable`; `keys`, `set_keys`, and `set_keys_nullable`. A null property value remains a present map entry with a null value. |
| `setKeys(Collection<String>)` | `set_keys_from` and `set_keys_from_nullable` preserve collection iteration order and Java `String.join` rendering of null elements as `"null"`; a null collection returns `MessageKeyCollectionError::NullCollection` before mutation, corresponding to Java's `NullPointerException` boundary. |
| `getProperty` / `getUserProperty` / `hasProperty` | `get_property` / `get_user_property` and nullable-key adapters, plus `has_property` / `has_nullable_property`. Null-key lookup, missing-key behavior, lazy empty-map creation on lookup, and the non-mutating `hasProperty(null)` path are fixture-covered. |
| `getProperties` | `properties_nullable` / `get_properties` expose null versus present-map state; `shared_properties` provides the mutable live handle needed for Java map identity. `property_entries` exposes nullable Java entries. The invoked `MessageAccessor` fixture covers caller/map aliasing, null keys/values, replacement, and deep-copy behavior. |
| `getDelayTimeLevel` / `setDelayTimeLevel`; `getDelayTimeSec` / `setDelayTimeSec`; `getDelayTimeMs` / `setDelayTimeMs`; `getDeliverTimeMs` / `setDeliverTimeMs` | `delay_time_level`, `delay_time_sec`, `delay_time_ms`, `deliver_time_ms` and matching setters. Missing values default to zero; signed Java decimal parsing, Unicode decimal digits, malformed values, and numeric overflow are compared. Rust returns typed parse errors at Java's exception boundary. |
| `getPriority` / `setPriority` | `priority` / `set_priority` match Java's `NumberUtils.toInt(..., -1)` fallback, nonnegative setter validation, no mutation on rejection, and integer boundary behavior. |
| `isWaitStoreMsgOK` / `setWaitStoreMsgOK`; `setInstanceId`; `getBuyerId` / `setBuyerId`; `getTransactionId` / `setTransactionId` | `is_wait_store_msg_ok` / `set_wait_store_msg_ok`; `set_instance_id` plus nullable form; `buyer_id` / `set_buyer_id` plus nullable form; `transaction_id` / `set_transaction_id`. Null-valued system properties, absent defaults, and transaction field mutation match the fixture. |
| `putUserProperty`; `toString` | `put_user_property` and its nullable adapter preserve reserved-name precedence, Java `String.trim()` blank validation, and typed error categories. Rust `Display` matches Java message formatting for the invoked null, populated, and nullable-property cases. |

`message_public_api_matches_java_551_fixture` pins all five constructor and 34
method signatures and compares defaults, overload inputs, nullable values,
aliasing, property side effects, error branches, numeric boundaries, and text
formatting. Validation on code revision `f5a924fa`: the focused Common test
passed; offline Maven `verify` passed with 37 scenarios, 11 Java message decode
fixtures, fixture drift checks, and Java/Rust cross-decodes. The fixture SHA-256
is `d0cdfbf2599adb0242a1df0dfa8523818a018c35cb34dac2b8418f8ce4bc972d`; the
pinned `Message.java` SHA-256 is
`c58de775771666448168eb959a1d6ebb98959bb42855a4fadaabb5f8f1d158d7`.
This closes the declared `Message` API slice; `MessageExt` inherited method
reconciliation is recorded in the next section. The broad P18-V01 bidirectional
audit remains open.

## Common public API: MessageExt and MessageClientExt declared and inherited methods

The pinned Java 5.5.1 `MessageExt` declares two public constructors and 44
public methods. `MessageClientExt` declares one public no-argument constructor
and four public methods. The Rust wrappers expose the inherited `Message` and
`MessageExt` methods through `Deref`/`DerefMut`; their base-class crosswalk is
above.

| Java 5.5.1 declaration | Rust counterpart / behavior |
| --- | --- |
| `MessageExt()`; `(int, long, SocketAddress, long, SocketAddress, String)` | `MessageExt::new`/`Default` and `MessageExt::with_socket_addresses`. Nullable/resolved/unresolved address and scalar defaults/constructor values are fixture-covered. |
| `get/setBrokerName`, `get/setQueueId`, `get/setBornTimestamp`, `get/setStoreTimestamp`, `get/setMsgId`, `get/setSysFlag`, `get/setBodyCRC`, `get/setQueueOffset`, `get/setCommitLogOffset`, `get/setStoreSize`, `get/setReconsumeTimes`, `get/setPreparedTransactionOffset` | Matching snake-case getters/setters on `MessageExt`; Java null strings map to `Option<String>`, and signed scalar values retain Java's full `int`/`long` range. |
| `get/setBornHost`, `get/setStoreHost`, `getBornHostString`, `getBornHostNameString` | `born_host`/`set_born_host`, `store_host`/`set_store_host`, `born_host_string`, and `born_host_name_string` use `MessageHostAddress` to retain resolved and unresolved Java socket-address states and Java-formatted host text. |
| `getBornHostBytes()` / `(ByteBuffer)`; `getStoreHostBytes()` / `(ByteBuffer)`; static `socketAddress2ByteBuffer(SocketAddress)` / `(SocketAddress, ByteBuffer)` | `MessageHostAddress::network_bytes`, `MessageHostAddress::write_network_bytes`, `MessageExt::born_host_bytes`/`write_born_host_bytes`, and `store_host_bytes`/`write_store_host_bytes` map returned bytes and caller-buffer overloads; `MessageExtSubtype` hooks preserve virtual subtype results and writes. Resolved IPv4/IPv6 output, null/unresolved-host failures, short-buffer prefix writes before port overflow, and custom subtype returns/writes are fixture-tested. |
| `parseTopicFilterType(int)`; `get/setTopicSysFlag`; `get/setGroupSysFlag` | `parse_topic_filter_type`, `topic_sys_flag`/`set_topic_sys_flag`, and `group_sys_flag`/`set_group_sys_flag`; absent/empty/null values, malformed and Unicode decimal inputs, and single/multi-tag flags are compared. |
| `setBornHostV6Flag`; `setStoreHostAddressV6Flag`; `toString` | `set_born_host_v6_flag`, `set_store_host_address_v6_flag`, and `Display` preserve the Java flag bits and observed message text. Identity-based equality/hash behavior is recorded in [Message equality semantics](#common-public-api-message-equality-semantics). |
| `MessageClientExt()`; `getMsgId` / `setMsgId`; `getOffsetMsgId` / `setOffsetMsgId` | `MessageClientExt::new`/`Default`, `message_id`/`set_message_id`, and `offset_message_id`/`set_offset_message_id`. Message ID fallback to offset ID, preference for the unique-message property, ignored setters under those conditions, and inherited Message/MessageExt accessors are fixture-tested. `MessageExtSubtype` dispatches overridden `getMsgId`, `setMsgId`, `getOffsetMsgId`, and `setOffsetMsgId` methods. A custom message-ID getter takes precedence over the base live-unique-key behavior, and its setter does not alter the offset ID. |

`messageExtPublicApi` and `messageClientExtApi` pin all constructor and
declared-method signatures. Rust differentials cover scalar defaults and
boundaries, socket constructor/getter/setter and byte-buffer overloads,
system-flag validation, client message-ID selection, inherited base methods,
and identity behavior. On code revision `f5a924fa`, the four
`message_ext_*` tests and `message_client_ext_api_matches_java_551_fixture`
passed; the current-revision offline Maven `verify` passed with 37 scenarios,
11 Java message decode fixtures, fixture drift checks, and Java/Rust
cross-decodes. `fixtures/messages/records.json` SHA-256 is
`d0cdfbf2599adb0242a1df0dfa8523818a018c35cb34dac2b8418f8ce4bc972d`;
pinned `MessageExt.java` SHA-256 is
`19a256b417a68daac6b1da6fe3b48a9bfa1fef7a9c09a6c8a29fdfb56ae34382`, and
`MessageClientExt.java` SHA-256 is
`fff21b69a3ed518781706cd4e87294c9407cc04f5f33e996d446ba06984a251d`.
This closes the `MessageExt`/`MessageClientExt` declared and inherited API
slice; broad P18-V01 bidirectional source, settings, input/error/flow, and
reverse-export reconciliation remains open.

### Host-byte buffer failure ordering follow-up

Pinned Java `MessageExt.getBornHostBytes()` and `getStoreHostBytes()` throw a
`NullPointerException` when their host is absent. Their `ByteBuffer` overloads
first write the full 4-byte IPv4 or 16-byte IPv6 address, then write the
4-byte port; a buffer with 7 IPv4 bytes or 19 IPv6 bytes therefore overflows
after advancing to position 4 or 16 and retains that address prefix. Rust
returns `MissingAddress` for an absent host and `BufferTooSmall` for these
short slices while preserving the same address prefix. The invoked fixture
also records construction with null born/store hosts. The Rust differential,
Common all-feature suite, strict Common Clippy, formatting, and offline Maven
`verify` pass. Fixture SHA-256:
`6b1a19fef78c1f88f52d7e0851905a6c4a1d6adb98de90fe2028642880a513b3`.
This closes only the host-byte failure-ordering slice; broad P18-V01 remains
open.

Runtime subtype override follow-up (code revision `625e80c6`, 2026-10-21):
`MessageExtSubtype` independently overrides the no-argument born/store host
byte getters and maps each Java `ByteBuffer` overload to a Rust mutable output
slice with a returned byte count. The invoked direct-consume fixture compares
both custom byte arrays and both caller-buffer writes. Java buffer identity
and position metadata are not represented. Fixture SHA-256:
`30af1bcbb2cfbf967f636fbbf7cbf9d54ba886a68c25fbe92df5ddf88000946c`.
This closes the subtype dispatch slice; inherited methods and broad P18-V01
reconciliation remain open.

## Common public API: MessageAccessor

The pinned Java 5.5.1 `MessageAccessor` declares 20 public static methods. Rust keeps these operations on `Message`, `SharedMessageProperties`, `NullableMessageProperties`, and the message property constants rather than introducing a Java-shaped utility type. Java `Message.getProperties()` returns its live mutable map, and `MessageAccessor.setProperties()` stores the supplied map object. Rust's ordinary `set_properties` remains an owned-map operation; `set_properties_shared` plus cloneable `SharedMessageProperties` preserve caller/message aliasing when that observable behavior is required. `shared_properties` is the mutable live-handle counterpart to Java's `getProperties`; `get_properties` remains a read view.

| Java 5.5.1 method | Rust counterpart |
| --- | --- |
| `clearProperty(Message, String)` | `Message::clear_property_nullable` and `Message::clear_system_property` |
| `setProperties(Message, Map)` | `Message::set_properties` for owned values, `Message::set_properties_shared` with `SharedMessageProperties` for retained map identity, and `Message::set_nullable_properties` for null entries |
| `putProperty(Message, String, String)` | `Message::set_property_nullable` |
| Transfer flag, correction flag, origin message ID, MQ2 flag, reconsume time, max reconsume times, consume start timestamp, and Lite topic getters/setters | `Message::{get_property,set_system_property,set_system_property_nullable}` with the matching `message::constants::PROPERTY_*` key |
| `cloneMessage(Message)` | `Message::clone_message`, which creates a distinct message while retaining Java's shared body/property handles until replacement |
| `deepCopyProperties(Map)` | `Clone` on `NullableMessageProperties`, preserving null keys and values in a separately owned map; `Option::clone` preserves a null map |

`message_public_api_matches_java_551_fixture` pins the declared method signatures and compares the property helper values, nullable map behavior, deep-copy independence, `setProperties` caller-map aliasing, and `cloneMessage` aliasing behavior with the invoked Java fixture. The fixture SHA-256 is `02be176d528051f874a159a7fc2867484a8e1c2f8a7cffcf92c588ec8853db91`. This closes the `MessageAccessor` declaration and mapped-behavior slice; the broader Common constructor/property overload, input/error/flow, and reverse-export audit remains open under P18-V01.

## Protocol public API: inherited RemotingSerializable methods

### P18-V01 message magic constant reverse-export correction

The pinned Java 5.5.1 `MessageDecoder` exposes the V1 value as
`MESSAGE_MAGIC_CODE`; it does not declare `MESSAGE_MAGIC_CODE_V1`. Rust keeps
the V1 label private to decoding and exposes the Java-named constant only. The
`MessageDecoder` API fixture continues to compare the public value and V2
value, while a compile-fail rustdoc guard rejects the Rust-only export. This
closes the surfaced constant mismatch only; the aggregate P18-V01 audit
continues across the remaining public items and behavior maps.

| Pinned Java 5.5.1 API | Rust counterpart / current behavior |
| --- | --- |
| Inherited compact/pretty `toJson`, instance `encode`, static `encode`/`toJson`, `decode`, `decodeList`, and `fromJson` | Java-compatible adapters cover `CheckClientRequestBody`, `LiteSubscriptionCtlRequestBody`, `QueryAssignmentRequestBody`, `QueryAssignmentResponseBody`, `QueryConsumeTimeSpanBody`, `SetMessageRequestModeRequestBody`, `MessageRequestModeSerializeWrapper`, `LockBatchRequestBody`, `UnlockBatchRequestBody`, `LockBatchResponseBody`, `GetConsumerListByGroupResponseBody`, `ConsumeMessageDirectlyResult`, `GetConsumerStatusBody`, and `ResetOffsetBody`, including nullable utility results and null list elements. `GetConsumerStatusBody` and `ResetOffsetBody` route through their MessageQueue-key codecs for compact, pretty, encode, decode, and list decode. |
| `QueueTimeSpan.getMinTimeStampStr`, `getMaxTimeStampStr`, and `getConsumeTimeStampStr` | Matching Rust getters and serialized derived fields use the Java date pattern and default local timezone. Signed-millisecond extremes retain Java `Date` year/calendar formatting. |
| `RemotingSerializable.encode(JSONWriter.Feature...)` | `JavaJsonWriterFeature` preserves all 48 pinned enum names and ordinals. `java_encode_with_features(&[...])` is available on the 15 mapped body DTOs, both route DTOs, and `HeartbeatData`; all 48 feature values are mapped for these adapters. Java/Rust fixtures compare 37 representative body cases plus route and heartbeat cases: 26 features change at least one sampled output, while 22 do not change any sampled output. Coverage includes null policies, class metadata and reference detection, number/string formats, escaping and field names, getter handling, bean arrays, and feature combinations. The detailed implementation is in [`writer_feature.rs`](../../crates/rocketmq-classic-protocol/src/writer_feature.rs), with DTO serialization paths in `body.rs`, `route/mod.rs`, and `heartbeat/mod.rs`. Matching sampled baseline output does not prove a feature is inert for every valid value. |

The invoked runtime fixtures pin inherited method signatures, default and populated encodings, Java null utility behavior, and UTC/Asia-Taipei timestamp output including `Long.MIN_VALUE` and `Long.MAX_VALUE`. The writer-feature inventory preserves all 48 names and ordinals and maps all 48 features across the 15 body DTO adapters, both route DTOs, and `HeartbeatData`. Fixtures cover 37 representative body cases plus route and heartbeat cases; 26 features change at least one sampled output and 22 do not change any sampled output. Current local P18-V02 gates passed on code revision `6441ca56`: locked default/all-feature workspace tests, strict all-target/all-feature Clippy, Maven verify (37 scenarios, 68 typed header frames, 15 typed body fixtures, and Java/Rust cross-decodes), formatting, and the recorded offline checks. Broad P18-V01 source/API/settings/input/error/control-flow/reverse-export reconciliation remains open, as do current Ubuntu/native Windows evidence and the remaining Phase 19 Broker/Docker scenarios.

P18-V01 writer-feature follow-up on code commit `b627c15e`: `WriteByteArrayAsBase64`, `WritePairAsJavaBean`, `WriteThrowableClassName`, and `WriterUtilDateAsMillis` preserve outputs for the mapped body, route, and heartbeat DTO graph. The Java oracle probes all 48 features across the fixture cases; these four have no recorded output changes. Pinned Fastjson2 source restricts them to raw byte arrays, Apache Pair values, Throwable values, and `java.util.Date` values. The mapped DTO graph has no such fields: BatchAck's BitSet is written through RocketMQ's custom `BitSetSerializerDeserializer`, and QueueTimeSpan uses long fields plus derived date strings. Rust now accepts the four options and compares each DTO adapter to the fixture baseline. Validation passed: Protocol all-feature tests (125 unit tests, all integration targets, 42 doctests), strict Protocol Clippy, formatting, `git diff --check`, and Maven verify (37 scenarios, fixture checks and cross-decodes). Mapping count is now 27/48; 21 features remain unmapped. Full-workspace P18-V02 still needs a final-revision rerun after the remaining Phase 18 code work.

P18-V01 numeric writer-format follow-up on code commit `b25f6a79`: `WriteBigDecimalAsPlain` and `WriteFloatSpecialAsString` preserve outputs across the mapped body, route, and heartbeat DTO graph. Pinned Fastjson2 uses these features only for BigDecimal, Float, and Double values; the mapped DTOs and map-value types contain none. Java fixtures record no output changes, and Rust compares each mapped adapter against the fixture baseline. Protocol all-feature tests (125 unit tests, all integration targets, 42 doctests), strict Protocol Clippy, formatting, and `git diff --check` pass. Mapping count is now 29/48; 19 features remain unmapped. The final full-workspace P18-V02 rerun remains pending after the remaining Phase 18 code work.

Coverage follow-up on code commit `4ae3735a`: the dedicated `CheckClientRequestBody` and `BatchAckMessageRequestBody` inherited-serialization tests now exercise `WriteNameAsSymbol` and all four `NotWrite*ClassName` flags against the Java fixture baseline for default, null, and populated cases. The Java fixture generator probes every pinned `JSONWriter.Feature`, recording changed output or an exception; absent override entries therefore mean the Java output matches the baseline. The focused Protocol run passed the three matching inherited-serialization tests (including both dedicated DTO tests), and `cargo fmt --all -- --check` plus `git diff --check` passed. This is test-coverage-only; full-workspace P18-V02 remains pending after Phase 18 code work is complete.

Current writer-feature status (2026-10-17, code commit `acd40aff`): `BeanToArray` serializes the mapped DTO schemas in Java property order, including nested beans in collections and typed route/request-mode maps; arbitrary JSON maps remain map-shaped. Null slots are retained, enum fields follow the selected ordinal/name flags, and queue-table map keys keep their Java encoding. The 40 mapped features are covered across the 15 body DTO adapters, both route DTOs, and `HeartbeatData`; eight remain unmapped: `WriteNulls`, `WriteMapNullValue`, `NullAsDefaultValue`, `WriteClassName`, `ReferenceDetection`, `WriteNullListAsEmpty`, `WriteNullStringAsEmpty`, and `WriteNullNumberAsZero`. Protocol all-feature tests (126 unit tests, all integration targets, 42 doctests), strict Protocol Clippy, formatting, diff checks, and Maven verify (37 scenarios, fixture checks, and Java/Rust cross-decodes) pass on this revision. Broad P18-V01 source/API/settings/input/error/flow/reverse-export reconciliation remains open, as do the final full-workspace P18-V02 rerun, current-revision platform CI evidence, and Phase 19 Docker/Broker execution.

P18-V01 Fastjson `WriteNulls` follow-up (2026-10-17, code commit `e7b0acde`): schema-backed serialization restores omitted nullable fields as JSON `null` for mapped DTO beans and nested typed DTOs. The invoked Java feature fixtures and Rust feature loops cover body, route, and heartbeat adapters. Protocol all-feature tests (126 unit tests, all integration targets, and 42 doctests), strict all-target/all-feature Protocol Clippy, formatting, diff checks, and Maven verify (37 scenarios, fixture checks, and Java/Rust cross-decodes) pass. The current writer-feature inventory is 41/48; seven remain unmapped: `WriteMapNullValue`, `NullAsDefaultValue`, `WriteClassName`, `ReferenceDetection`, `WriteNullListAsEmpty`, `WriteNullStringAsEmpty`, and `WriteNullNumberAsZero`. Broad P18-V01 remains open, as do the final full-workspace P18-V02 rerun, current-revision platform CI evidence, and Phase 19 Docker/Broker execution.

P18-V01 Fastjson `WriteMapNullValue` follow-up (2026-10-17, code commit `af0baada`): the invoked Java serializer emits the mapped nullable DTO fields for this flag, with the same sampled output as `WriteNulls`. Rust restores those fields from the DTO schema; map entries already represented as JSON null remain in the value tree. Body, route, and heartbeat feature comparisons pass, as do Protocol all-feature tests (126 unit tests, all integration targets, and 42 doctests), strict Protocol Clippy, formatting, and diff checks. The current writer-feature inventory is 42/48; six remain unmapped: `NullAsDefaultValue`, `WriteClassName`, `ReferenceDetection`, `WriteNullListAsEmpty`, `WriteNullStringAsEmpty`, and `WriteNullNumberAsZero`. Broad P18-V01 remains open, as do the final full-workspace P18-V02 rerun, current-revision platform CI evidence, and Phase 19 Docker/Broker execution.

P18-V01 Fastjson `WriteNullListAsEmpty` follow-up (2026-10-17, code commit `038bf5f8`): DTO-schema traversal converts nullable list/set properties to empty arrays in body, route, and heartbeat outputs, including nested typed beans, while leaving unrelated missing nullable fields omitted. The fixture-backed Protocol all-feature suite (126 unit tests, all integration targets, and 42 doctests), strict Protocol Clippy, formatting, and diff checks pass. The current writer-feature inventory is 43/48; five remain unmapped: `NullAsDefaultValue`, `WriteClassName`, `ReferenceDetection`, `WriteNullStringAsEmpty`, and `WriteNullNumberAsZero`. Broad P18-V01 remains open, as do the final full-workspace P18-V02 rerun, current-revision platform CI evidence, and Phase 19 Docker/Broker execution.

P18-V01 Fastjson `WriteNullStringAsEmpty` follow-up (2026-10-17, code commit `68941489`): schema-backed serialization replaces nullable String fields and nullable String members in typed collections with empty strings across the body, route, and heartbeat DTO adapters. The fixture-backed Protocol all-feature suite (126 unit tests, all integration targets, and 42 doctests), strict Protocol Clippy, formatting, and diff checks pass. The current writer-feature inventory is 44/48; four remain unmapped: `NullAsDefaultValue`, `WriteClassName`, `ReferenceDetection`, and `WriteNullNumberAsZero`. Broad P18-V01 remains open, as do the final full-workspace P18-V02 rerun, current-revision platform CI evidence, and Phase 19 Docker/Broker execution.

P18-V01 Fastjson `WriteNullNumberAsZero` follow-up (2026-10-17, code commit `63a90423`): the flag preserves output for the mapped DTO graph. Schema review found no nullable numeric bean properties; Fastjson2 2.0.63 map and collection writers write null entries directly with `writeNull()` and do not apply this number-null feature. The invoked Java writer fixtures record no overrides, and Rust body, route, and heartbeat feature comparisons match. Protocol all-feature tests (126 unit tests, all integration targets, and 42 doctests), strict Protocol Clippy, formatting, and diff checks pass. The writer-feature inventory is now 45/48; `NullAsDefaultValue`, `WriteClassName`, and `ReferenceDetection` remain unmapped. Broad P18-V01 remains open, as do the final full-workspace P18-V02 rerun, current-revision platform CI evidence, and Phase 19 Docker/Broker execution.

P18-V01 Fastjson `NullAsDefaultValue` follow-up (2026-10-17, code commit `a54512da`): DTO-schema traversal writes null Strings as `""`, collections as `[]`, and maps or nullable nested DTO references as `{}`; nullable enum fields remain `null`, matching Fastjson2 2.0.63. Existing invoked Java writer fixtures cover body, route, and heartbeat defaults, explicit nulls, nested DTOs, and nullable String collection members. Protocol all-feature tests (126 unit tests, all integration targets, and 42 doctests), strict all-target/all-feature Protocol Clippy, formatting, and diff checks pass. Writer-feature mapping is now 46/48; `WriteClassName` and `ReferenceDetection` remain unmapped. Broad P18-V01 remains open, as do the final full-workspace P18-V02 rerun, current-revision platform CI evidence, and Phase 19 Docker/Broker execution.

P18-V01 Fastjson `ReferenceDetection` follow-up (2026-10-17, code commit `eb991847`): serialization preserves `SharedValue` handle identity while constructing the DTO value tree and emits Fastjson `$ref` paths for repeated handles. An invoked Java `TopicRouteData` fixture reuses one `BrokerData` instance twice and records `$.brokerDatas[0]`; Rust emits the same path. Protocol all-feature tests (127 unit tests, all integration targets, and 42 doctests), strict Protocol Clippy, formatting, `git diff --check`, and Maven verify (37 scenarios, fixture checks, and Java/Rust cross-decodes) pass; the final internal-marker refactor also passes the focused shared-route reference test. Writer-feature mapping is now 47/48; `WriteClassName` remains unmapped. Broad P18-V01 remains open, as do the final full-workspace P18-V02 rerun, current-revision platform CI evidence, and Phase 19 Docker/Broker execution.

## Direct runtime consumer-status API — 2026-10-20

Java `MQClientInstance.getConsumerStatus(topic, group)` now maps to
`ClientRuntime::get_consumer_status(Option<&str>, Option<&str>)`. Pull/Push
owner adapters expose current offset-store snapshots; missing/other owners
return empty, null groups fail, and topic/namespace handling remains with
the store. Push raw store replacement and null-store errors are also visible
through inbound status handling. The direct Java 54-case comparison and
current source/test mapping are recorded in
[the Phase 18 runtime status audit](PHASE_18_RUNTIME_CONSUMER_STATUS.md).
This closes this API row only; the broader P18-V01 audit remains open.

## Direct runtime consumer operations — 2026-10-20

The public `MQClientInstance.consumerRunningInfo`, `consumeMessageDirectly`
and `resetOffset` methods now map to `ClientRuntime::consumer_running_info`,
`consume_message_directly` and `reset_offset`. Owner adapters preserve snapshot
aliases, virtual-call ordering, nullable arguments, direct-consume callback
results and offset isolation, shared reset-map reads, runtime monitor ordering,
and per-queue failure behavior. `ConsumerOffsetTable` represents the live Java
Map and `ConsumerOperationFuture` represents an asynchronous Rust owner result;
neither is a new setting or independent feature. The exact Java methods, inputs,
fixtures and regression IDs are recorded in
[the direct runtime consumer-operation audit](PHASE_18_RUNTIME_CONSUMER_OPERATIONS.md).
These scoped rows do not close the complete P18-V01 bidirectional audit.

## Push/POP process-queue idle timeout — 2026-10-21

Java 5.5.1 `ProcessQueue` and `PopProcessQueue` each capture the shared
`rocketmq.client.pull.pullMaxIdleTime` system property (default `120000`) in a
separate static field and expire only when signed
`currentTimeMillis - lastPullTimestamp` is greater than that value. Rust maps
the same dotted name through its process environment, parses Java-compatible
signed decimal longs, and caches the ProcessQueue value on first Push startup or
direct expiry use; the POP owner keeps its separate cache, which Push rebalance
uses for POP assignment expiry. Later environment changes do not replace
either captured value. Both queue types use wrapping `i64` subtraction and the
strict boundary. Invalid Push-start values return the typed
`PushConsumerError::PullIdleTimeSettings`. The
`subscribed-pull-expired` Java/Rust runtime comparison checks Push queue
removal; parser, cache-lifetime, and boundary tests cover the timeout
arithmetic. The `popUnsubscribedTruncation` Java/Rust fixture also verifies
that rebalance marks an unsubscribed POP queue dropped, removes it from the
table, and retires its worker. An already-issued FOUND response still updates
pull statistics, filters messages, ACKs filtered messages, and submits
accepted messages to a consume request that exits on the dropped queue. The
dropped queue also prevents another POP request. The `popToPullTransition`
fixture verifies that a PULL assignment removes the old POP state while
retaining its existing PULL queue. Remaining POP assignment branches, the public
Rebalance/ProcessQueue API audit, and the broader P18-V01 audit remain open.

## ProcessQueue `containsMessage(null)` input — 2026-10-23

Pinned Java `ProcessQueue.containsMessage` returns `false` for a null message
before acquiring the tree lock. Rust adds `contains_message_nullable` while
preserving the direct non-null method. The invoked facade fixture and Rust
comparison cover the null input. Fixture SHA-256:
`edb43a544374bb1c5ae969bcdaa09ea3746a310b3d28dc86b459a18ecfcff551`.
This closes that input branch only; P18-V01 remains open.

## ProcessQueue `fillProcessQueueInfo(null)` input — 2026-10-23

Pinned Java catches and logs the exception caused by a null
`ProcessQueueInfo`, returning normally after releasing the read lock. Rust adds
`fill_process_queue_info_nullable(None)` as an observable no-op. The invoked
facade fixture and Rust comparison cover this input. Fixture SHA-256:
`57e2890ebeace93ecf00e2509e2bf7557c28f681ad19141f81430fecfe57ddf3`.
This closes the null-info input branch only; P18-V01 remains open.

## ProcessQueue `cleanExpiredMsg(null)` input — 2026-10-23

Pinned Java dereferences the consumer with `isConsumeOrderly()` before it
checks queue contents, throwing `NullPointerException` for null. Rust adds
`clean_expired_msg_nullable(None)` and returns a typed null-consumer cleanup
error before scanning. Fixture SHA-256:
`2934eb15b2fb599bd6309fd8d8dc394ad0deef817060bc3883373da0942ce40c`.
This closes the null-consumer input branch only; P18-V01 remains open.

## ProcessQueue `putMessage(null)` input — 2026-10-23

Pinned Java's enhanced-for loop throws `NullPointerException` for a null
message list before inserting or counting messages. Rust adds
`put_message_nullable(None)` and returns the typed Java error without mutating
queue state. Fixture SHA-256:
`be3d3a818e11a39838b83bafed81ec0f67bec266c200a5d13af374a69af91bb4`.
This closes the null-list input branch only; P18-V01 remains open.

## ProcessQueue `putMessage` null-element partial state — 2026-10-23

Pinned Java inserts valid prefix elements and increments bytes before a later
null causes `NullPointerException`; the exception skips the aggregate count,
dispatch flag, and final `MAX_OFFSET` parse. Rust adds
`put_message_items_nullable` to preserve that order and partial state. Fixture
SHA-256:
`01133da592b2f5c73b5fb379e14626651887cf8c0543875b928f266f02221aa1`.
This closes the null-element prefix branch only; P18-V01 remains open.

## ProcessQueue `makeMessageToConsumeAgain` null-element prefix — 2026-10-23

Pinned Java requeues each valid prefix message before a null element throws.
Rust adds `make_message_to_consume_again_items_nullable`, preserving the
requeued prefix, counters, in-flight ownership, and dispatch state before
returning the typed Java null error. Fixture SHA-256:
`2b1daf803430969e4b7bd57f4bc54e9794b3d107b7cc0a1fd5fb1062764efdb7`.
This closes the null-element requeue branch only; P18-V01 remains open.

## ProcessQueue `removeMessage(null)` input — 2026-10-23

Pinned Java catches the null-list exception after updating
`lastConsumeTimestamp`; when the ready map is nonempty it returns
`queueOffsetMax + 1` without changing cached entries or counters. Rust adds
`remove_message_nullable(None)` to preserve that result and timestamp change.
Fixture SHA-256:
`357c266f6bea2c4df5b066d334b75b28314202d1d198a34bd270115345b9096f`.
This closes the null-list removal branch only; P18-V01 remains open.

## ProcessQueue `makeMessageToConsumeAgain(null)` input — 2026-10-23

Pinned Java propagates `NullPointerException` from the enhanced-for loop before
changing queue state. Rust adds `make_message_to_consume_again_nullable(None)`
and returns the typed Java error while leaving entries and counters intact.
Fixture SHA-256:
`a9fb84f276c05f4f55481b03fa2316cf28ec0ca691ecfbe1ae027522abfd6365`.
This closes the null-list requeue branch only; P18-V01 remains open.

## ProcessQueue `removeMessage` null-element prefix — 2026-10-23

Pinned Java catches a null-element exception after removing each valid prefix
message from the ready tree and subtracting its body size. The exception skips
the aggregate count update and final first-key result calculation, so Java
returns the precomputed `queueOffsetMax + 1`. Rust adds
`remove_message_items_nullable` with the same partial tree, byte-size, count,
timestamp, and return-offset effects. The invoked fixture also checks the next
insertion and dispatch result. Fixture SHA-256:
`29cc00f7cf9b8194f08007a12c5e75f0499a5601c140a78855b14cb244bef771`.
This closes the null-element removal branch only; P18-V01 remains open.

## ProcessQueue declared-method crosswalk — 2026-10-23

The pinned Java facade fixture records all 32 declared public `ProcessQueue`
methods. `PHASE_18_REBALANCE_PROCESS_QUEUE_API.md` now maps each Java method to
its Rust owner method and records nullable-input and return-type adapters. The
focused Rust audit regression checks the 32-method denominator, every Java
method name, each documentation mapping, and the corresponding Rust method in
the owner source. No Java fixture refresh was needed; the facade fixture SHA-256
remains `29cc00f7cf9b8194f08007a12c5e75f0499a5601c140a78855b14cb244bef771`.
This closes the `ProcessQueue` declared-method crosswalk slice only; P18-V01
remains open.

## ProcessQueue Rust public support-surface reverse audit — 2026-10-23

The ProcessQueue audit now records the Rust constructor, nullable-input
adapters, AtomicLong-style handles, lock-backed live-tree view and guards, and
typed result/error wrappers against their Java counterparts or lifecycle
dispositions. `every_process_queue_rust_only_surface_has_java_disposition`
checks each listed Rust-only surface against the owner source and audit table.
This closes the ProcessQueue reverse-support-surface slice only; P18-V01
remains open.

## PopProcessQueue null info and method crosswalk — 2026-10-23

Pinned Java `fillPopProcessQueueInfo(null)` propagates
`NullPointerException`. Rust adds `fill_pop_process_queue_info_nullable` and
`PopProcessQueueError::NullInfo`; the invoked facade fixture and Rust comparison
check the Java exception class. The audit also maps all 11 declared public
`PopProcessQueue` methods to Rust methods or `Display::to_string`, and records
the nullable adapter in the reverse Rust API inventory. Updated
`fixtures/consumer/facade-api.json` SHA-256:
`a32d9a980e5079fa5fccaa6f6d46287946883099550e0bc4f04854892b0ca065`.
This closes the PopProcessQueue null-info and direct-method inventory slices
only; P18-V01 remains open.

## `MQClientAPIImpl.getClusterList` stub — 2026-10-21

The pinned `getClusterList(String,long)` implementation ignores both inputs,
performs no RPC, and returns Java's shared immutable `Collections.EMPTY_SET`.
`MQClientApi::get_cluster_list(Option<&str>, Instant)` maps the observable
contract with a shared immutable empty `HashSet` reference. The invoked Java
fixture covers nonempty, null, and empty topics, singleton identity, rejected
mutation, and an empty request list; the Rust regression consumes those cases
and verifies the same result identity and no-request behavior. This closes the
named stub only; the broad P18-V01 inventory remains open.

## Direct `resetMasterFlushOffset` Broker API — 2026-10-21

Pinned `MQClientAPIImpl.resetMasterFlushOffset(String,long)` sends request 908
to the supplied Broker address unchanged, with `masterFlushOffset` encoded as
a signed decimal header field. Java fixes the timeout at 3000 ms, returns on
`SUCCESS`, and throws `MQBrokerException` for other response codes. Core maps
the operation to `MQClientApi::reset_master_flush_offset` and preserves those
request and result semantics. Rust also exposes
`ResetMasterFlushOffsetHeader` with Java's nullable boxed-`Long` getter/setter;
reflection decoding maps missing and malformed values to null. The invoked
Java fixture, typed Java↔Rust frame, and Rust differential cover the signed
boundary, negative input, exact request, and success/error outcomes. This
closes the named API/header rows only; broad P18-V01 remains open.

## Direct `exportRocksDBConfigToJson` Broker API — 2026-10-21

Pinned `MQClientAPIImpl.exportRocksDBConfigToJson(String,List,long)` maps
request 355 with the supplied address and caller timeout. The nullable
`ExportRocksDBConfigToJsonRequestHeader.configType` field serializes
`TOPICS`, `SUBSCRIPTION_GROUPS` and `CONSUMER_OFFSETS` as their Java type names
with a trailing semicolon; an empty list serializes to an empty string. Rust
exposes the header and `ExportRocksDBConfigType` parsing/accessor behavior, and
`MQClientApi::export_rocksdb_config_to_json` returns on `SUCCESS` or preserves
Broker response errors. A nullable-list adapter rejects null before transport.
Java and Rust fixtures cover type parsing, frame fields, success, empty-list,
null-list, and Broker-error cases. This closes the direct API/header slice
only; broad P18-V01 reconciliation remains open.

## Direct NameServer write-permission APIs — 2026-10-21

Pinned `MQClientAPIImpl.wipeWritePermOfBroker(String,String,long)` sends
request 205 and `addWritePermOfBroker(String,String,long)` sends request 327.
Both use the supplied NameServer address and caller timeout, retain nullable or
empty `brokerName`, and return the respective boxed topic-count response field
as an `i32`. A `SUCCESS` reply with a missing or malformed count fails while
Rust unwraps the nullable response field, matching Java's `Integer` unboxing
failure. Other response codes preserve the Broker/NameServer error. Rust maps
the four request/response header models and both Core calls. Invoked Java and
Rust fixtures cover the direct address, request codes, fields, signed result,
null/empty name, malformed/missing count, and Broker rejection. This closes
these direct API/header rows only; broad P18-V01 remains open.

## Direct `checkRocksdbCqWriteProgress` Broker API — 2026-10-21

Pinned `MQClientAPIImpl.checkRocksdbCqWriteProgress(String,String,long,long)`
maps request 354 to `MQClientApi::check_rocksdb_cq_write_progress`. It sends
the supplied address unchanged with the caller deadline, nullable `topic`, and
primitive `checkStoreTime`, without a request body. Core returns an optional
`CheckRocksdbCqWriteResult`; the DTO preserves nullable `checkResult` and
primitive `checkStatus`, and `CheckRocksdbCqWriteStatus` maps the nested Java
enum's names, ordinals, and integer values. Successful null, absent, or empty
bodies return `None`; malformed JSON is a malformed response, and non-success
codes preserve the Broker response. Java invoked fixtures, the typed request
frame, fast/reflection header comparisons, Protocol tests and a Core loopback
cover the request, DTO defaults, enum surface and outcomes. This closes the
named API/header slice only; broad P18-V01, P18-V02 and platform evidence gates
remain open.

## P18-V01 `MqClientAdmin` Rust wrapper inventory — 2026-10-21

`MQClientApi::mq_client_admin()` exposes the complete 19-method Java interface
surface. The Java reflection fixture pins its declared signatures, and the
Core surface test compile-calls all 19 Rust methods. These wrappers reuse the
audited direct RPC implementations; `update_or_create_topic` sends the supplied
header without the local permission check used by the separate
`MQClientAPIImpl.createTopic` entrypoint. Fixture SHA-256:
`16f61c5e859f44062a2ec732e2603ad97b38719b9a0f8fa23fc09ffa5112dfd3`.
This closes the surfaced wrapper omission only; broad P18-V01 remains open.

The same invoked fixture now covers `MqClientAdmin.queryMessage` key filtering
for empty query keys, NBSP, em-space, U+0085 and U+001C. Rust reproduces
Commons `StringUtils.isNotBlank`'s Java `Character.isWhitespace` predicate and
`String.split(" ")`'s retained interior and discarded trailing empty tokens in
the direct Broker wrapper and cross-Broker merger. The direct fixture test and
`query_message_keys_match_java_whitespace_and_split_semantics` cover both
paths; current fixture SHA-256 is
`16f61c5e859f44062a2ec732e2603ad97b38719b9a0f8fa23fc09ffa5112dfd3`.

## Direct Broker registration APIs — 2026-10-21

Pinned `MQClientAPIImpl.addBroker(String,String,long)` sends request 902 with
nullable `configPath`; `removeBroker(String,String,String,long,long)` sends
request 903 with nullable `brokerClusterName` and `brokerName`, plus the
signed 64-bit `brokerId`. Core exposes `MQClientApi::add_broker` and
`remove_broker`, preserves the supplied Broker address without VIP rewriting,
uses each caller-provided timeout, returns on `SUCCESS`, and classifies other
response codes as Broker errors. Public typed
`AddBrokerRequestHeader`/`RemoveBrokerRequestHeader` models preserve Java's
nullable fields and boxed-`Long` header surface. Invoked Java fixtures and Rust
loopback cases cover exact signatures and request fields, null values, both
signed broker-ID boundaries, success/rejection, and address/timeout behavior;
a delayed peer checks that `add_broker` enforces its timeout. Typed Java↔Rust
header fixtures are checked in. Producer core fixture SHA-256:
`9510b46415e7f00c7932c95d9e33dbb02e9b9cc56d1afecec01dfede60ae5e6a`.
This closes these direct API/header rows only; broad P18-V01 remains open.

## Direct `cloneGroupOffset` Broker API — 2026-10-22

Pinned `MQClientAPIImpl.cloneGroupOffset(String,String,String,String,boolean,long)`
sends request 314 with nullable `srcGroup`, `destGroup`, and `topic`, plus the
primitive `offline` flag. Core exposes `MQClientApi::clone_group_offset`, keeps
the caller timeout, applies the configured Broker VIP address transform, and
returns success or a Broker response error. The typed
`CloneGroupOffsetRequestHeader` retains the four inherited `RpcRequestHeader`
fields and Java's no-op `checkFields()` behavior. Invoked Java fixtures and
Rust loopback cases cover ordinary and null string inputs, both `offline`
values, exact wire fields, and an error response; typed Java↔Rust header
fixtures cover all child and inherited fields. Producer core fixture SHA-256:
`c7895cfff96e0150878d761336c34b4ddfec8064249d4af994a3f6796959a8d9`. This
closes this direct RPC/header row only; broad P18-V01 remains open.

## Direct Broker consumer-status API — 2026-10-22

Pinned `MQClientAPIImpl.invokeBrokerToGetConsumerStatus(String,String,String,String,long)`
sends request 223 with nullable `topic`, `group`, and `clientAddr`, applies the
configured VIP-channel transform, and decodes the `consumerTable` from
`GetConsumerStatusBody`. Core exposes
`MQClientApi::invoke_broker_to_get_consumer_status` and returns the nullable
shared consumer-to-queue-offset table. Invoked Java fixtures and Rust loopback
cases cover normal and null inputs, exact request fields, empty and populated
tables, Broker errors, and a `SUCCESS` response without a body. Producer core
fixture SHA-256: `7de333d7c61d4235a6e134e5bdf7dc97cf03c12024dd320b9b96ff3a02beb183`.
This closes this direct RPC row only; broad P18-V01 remains open.

## `MessageClientExt` subtype offset-ID dispatch — 2026-10-21

A caller-defined Java subclass of `MessageClientExt` can override
`getOffsetMsgId()` and `setOffsetMsgId(String)`. The base
`MessageClientExt.getMsgId()` prefers the live `UNIQ_KEY` property, then calls
`getOffsetMsgId()` virtually. `MessageExtSubtype` now exposes owned offset-ID
getter and setter hooks; `MessageClientExt` uses them directly, and its
inherited client-ID path preserves unique-ID precedence and the dynamic
fallback. The invoked Java direct-consume fixture includes offset-only custom
subclasses with and without a unique ID. Rust's wrapper test also checks
setter dispatch, no-op base `setMsgId`, live unique-ID changes, and conversion
to a base-typed `MessageExt`. Fixture SHA-256:
`403b79b69f30957ae2f935be37ec0b8537696909d91066d2126f216eff31996d`.
Code revision `f7d7e8f1` also verifies `setOffsetMsgId(null)` clears the
offset and effective ID and that conversion to a base-typed `MessageExt`
preserves that cleared state. Current `fixtures/messages/records.json`
SHA-256: `d0cdfbf2599adb0242a1df0dfa8523818a018c35cb34dac2b8418f8ce4bc972d`.

P18-V01 custom `MessageClientExt.getMsgId`/`setMsgId` dispatch (test revision
`5e12a174`): an invoked Java subclass confirms its getter bypasses the base
`UNIQ_KEY` lookup and its setter owns custom state, including null, without
changing the physical offset ID. `MessageExtSubtype::message_id` and
`set_message_id` match those virtual calls. The Common message API test file
passes all 26 cases; current `records.json` SHA-256 is
`d0cdfbf2599adb0242a1df0dfa8523818a018c35cb34dac2b8418f8ce4bc972d`.

## P18-V01 direct `queryCorrectionOffset` Broker API — 2026-10-22

Pinned `MQClientAPIImpl.queryCorrectionOffset(String,String,String,Set,long)`
sends request 308 with a `QueryCorrectionOffsetHeader`; its annotated
compare-group/topic fields are still nullable because `checkFields()` is a
no-op. `MQClientApi::query_correction_offset` keeps nullable inputs, joins the
optional filter set as Java does, applies configured VIP conversion, and uses
the caller timeout. `QueryCorrectionOffsetBody` preserves Java's default
empty map, explicit null map and nullable Long values. Invoked Java fixtures
and a Rust loopback test cover six cases, including Broker error and
`SUCCESS` without a body. Header inventory and validation-matrix coverage
include the new public header. Producer core fixture SHA-256 is recorded with
the execution log. This closes this direct API slice only; broad P18-V01
settings/API/flow/input/error/reverse-export reconciliation remains open.

## P18-V01 direct `updateAndGetGroupForbidden` Broker API — 2026-10-22

Pinned `MQClientAPIImpl.updateAndGetGroupForbidden(String,
UpdateGroupForbiddenRequestHeader,long)` sends request 353 with nullable
group/topic/readable and inherited RPC fields. Core exposes
`MQClientApi::update_and_get_group_forbidden`, preserves the caller timeout,
applies configured VIP conversion, and decodes a nullable `GroupForbidden`
response. A missing success body returns `None` as in Java. Four invoked Java
cases and the Rust loopback test cover populated/null headers and results,
success and Broker rejection. The header inventory and field validation
matrix include every request field. This closes this direct API/header/DTO
slice only; broad P18-V01 remains open.

## P18-V01 direct `getBrokerHAStatus` Broker API — 2026-10-22

Pinned `MQClientAPIImpl.getBrokerHAStatus(String,long)` sends request 907 to
the supplied Broker address with no custom header, body, or VIP transformation.
`MQClientApi::get_broker_ha_status` uses the caller timeout and decodes nullable
`HaRuntimeInfo`, including Java's empty connection-list and default client-info
values when fields are absent. The nested connection/client DTOs preserve their
Java getter/setter surface and Fastjson field names. Invoked Java fixtures and
a Rust loopback test cover populated/default DTOs, success without a body,
Broker errors, and the empty request envelope. Producer core fixture SHA-256 is
recorded in the execution log. This closes this direct API slice only; broad
P18-V01 remains open.

## P18-V01 direct `fetchConsumeStatsInBroker` Broker API — 2026-10-22

Pinned `MQClientAPIImpl.fetchConsumeStatsInBroker(String,boolean,long)` sends
request 317 with `GetConsumeStatsInBrokerHeader.isOrder`, applies configured
VIP address conversion, and decodes a `ConsumeStatsList` response. Core exposes
`MQClientApi::fetch_consume_stats_in_broker` with the caller timeout. The DTO
preserves Java's default empty list, explicit null list/address, primitive
totals (including null-to-zero decoding), nested mutable `ConsumeStats` rows,
and `MessageQueue` object keys in each `offsetTable`; its compact Java encoder
retains Fastjson's object-key syntax. Invoked Java fixtures and a Rust loopback
test cover populated/default/null bodies, success without a body, Broker
errors, the exact request header, VIP conversion and timeout. Producer core
fixture SHA-256 is recorded in the execution log. This closes this direct
API/header/DTO slice only; broad P18-V01 remains open.

## P18-V01 direct `viewBrokerStatsData` Broker API — 2026-10-22

Pinned `MQClientAPIImpl.viewBrokerStatsData(String,String,String,long)` sends
request 315 with nullable `statsName` and `statsKey`; their `@CFNotNull`
annotations are not enforced by Java's no-op `checkFields()`. Core exposes
`MQClientApi::view_broker_stats_data`, applies configured VIP conversion, and
uses the caller timeout. The new `BrokerStatsData` and `BrokerStatsItem` DTOs
preserve nullable minute/hour/day periods, shared nested object handles, and
primitive zero defaults, including explicit-null numeric inputs. Invoked Java
fixtures and a Rust loopback test cover populated/default/null results, null
request fields, success without a body, Broker errors, and the request
envelope. The focused Core unit test also checks configured VIP routing.
Producer core fixture SHA-256 is recorded in the execution log. This closes
this direct RPC/header/DTO slice only; broad P18-V01 remains open.

## P18-V01 direct `getTopicConfig` Broker API — 2026-10-22

Pinned `MQClientAPIImpl.getTopicConfig(String,String,long)` sends request 351
with nullable `topic`, inherited `lo=true`, and no-op header validation. Core
exposes `MQClientApi::get_topic_config`, applies configured VIP conversion, and
uses the caller timeout. `TopicConfigAndQueueMapping` preserves `TopicConfig`
defaults and primitive null-to-zero behavior, plus `TopicQueueMappingDetail`,
its inherited mapping fields, integer-keyed `hostedQueues`, and nested
`LogicQueueMappingItem` values. The decoder handles Fastjson's unquoted integer
map keys. Invoked Java fixtures and the Rust loopback test cover populated and
default responses, null primitive fields, missing and JSON-null bodies, null
and empty topics, topic-not-found and generic Broker errors, and malformed
JSON. A focused Core test also checks VIP routing; a delayed-peer test checks
the supplied timeout. Producer core fixture SHA-256 is recorded in the
execution log. This closes this direct RPC/header/DTO slice only; broad
P18-V01 remains open.

## P18-V01 direct `getAllTopicConfig` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.getAllTopicConfig(String,long)` sends
request 21 with `GetAllTopicConfigRequestHeader` fields `topicSeq`,
`dataVersion`, and `maxTopicNum`. Core exposes
`MQClientApi::get_all_topic_config`; it uses the configured metadata page size
(default 2000), configured VIP transformation, and one caller deadline across
all pages. `DataVersion` and `TopicConfigSerializeWrapper` preserve Java JSON
field names, defaults, and null states. Pagination appends topic configs while
the version is stable; a changed version clears the collected table and starts
again at page zero. A missing or reflectively invalid `totalTopicNum` follows
the Java legacy-Broker path and accepts that page as complete. Invoked Java
fixtures and the Rust loopback test cover single/multiple pages, version
restart, legacy response, malformed/missing/null body cases, Broker errors,
and timeout. Bidirectional typed-header and body fixtures cover both headers
and the wrapper. This closes this direct RPC/header/DTO slice only; the broader
P18-V01 API/config/feature/input/error/control-flow/reverse-export audit
remains open.

## P18-V01 direct `getAllSubscriptionGroup` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.getAllSubscriptionGroup(String,long)` sends
request 201 with `GetAllSubscriptionGroupRequestHeader` fields `groupSeq`,
`dataVersion`, and `maxGroupNum`. Core exposes
`MQClientApi::get_all_subscription_group`; it uses the configured metadata
page size (default 2000), configured VIP transformation, and one caller
deadline across all pages. `SubscriptionGroupWrapper` preserves Java defaults,
nullable tables, `DataVersion`, group configurations, and nested forbidden-topic
settings. Pagination merges both tables while the version is stable; a changed
version clears both tables and restarts at page zero. A missing or invalid
`totalGroupNum` follows Java's legacy-Broker path and accepts that page as
complete. Java fixtures and Rust tests cover single/multiple pages, version
restart, legacy response, null/malformed bodies, Broker errors, timeout, and
both wrapper tables. Bidirectional typed-header and body fixtures cover the
new protocol models. This closes only this direct API/header/DTO slice; broad
P18-V01 remains open.

## P18-V01 direct `queryConsumeQueue` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.queryConsumeQueue(String,String,int,long,int,String,long)`
sends request 321. Core exposes `MQClientApi::query_consume_queue`, sends the
nullable topic and consumer group with primitive `queueId`, `index`, and
`count`, applies configured VIP conversion, and uses the supplied timeout.
`QueryConsumeQueueResponseBody` preserves nullable `SubscriptionData`, filter
data, and queue rows plus Java's zero defaults for both queue indexes;
`ConsumeQueueData` preserves all seven Java properties and nullable strings.
Java and Rust offline fixtures cover populated/default/null/missing/malformed
responses, nullable request strings, Broker rejection, timeout, and the
configured VIP route. Header decode-mode checks and bidirectional typed header
and body fixtures pass. This closes only this direct API/header/DTO slice;
the broader P18-V01 settings/API/feature/input/error/control-flow/reverse-export
audit remains open.

## P18-V01 direct `createStaticTopic` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.createStaticTopic` maps `TopicConfig` into
`CreateTopicRequestHeader`, then sends `TopicQueueMappingDetail.encode()` as
the body with request 513. Core exposes `MQClientApi::create_static_topic`,
uses configured VIP conversion and the caller timeout, and preserves Java's
lack of local permission validation. The Fastjson body writer keeps integer
keys in `currIdMap` and `hostedQueues` unquoted. Invoked Java fixtures and Rust
loopback tests compare the header, exact body bytes, success/error outcomes,
timeout, and VIP routing. The reverse audit now has eight open direct APIs;
broader P18-V01 reconciliation remains open.

## P18-V01 direct `createUser` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.createUser` sends request 3001 with a
`CreateUserRequestHeader` and a `UserInfo` body. Core exposes
`MQClientApi::create_user`, preserves nullable user fields and Java's
null-omitting body encoding, uses the supplied timeout, and leaves the Broker
address unchanged when VIP channels are enabled. Invoked Java fixtures and
Rust loopback tests compare the request header/body, success and Broker error,
timeout, and unchanged-address behavior. The direct reverse audit now has
seven open APIs; broader P18-V01 reconciliation remains open.

## P18-V01 direct `updateUser` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.updateUser` sends request 3002 with an
`UpdateUserRequestHeader` and a `UserInfo` body. Core exposes
`MQClientApi::update_user`, reuses the verified nullable user model and
null-omitting body encoding, uses the supplied timeout, and leaves the Broker
address unchanged when VIP channels are enabled. The nullable update header
has Java's no-op validation behavior. Invoked Java fixtures and Rust loopback
tests compare the request header/body, success and Broker error, timeout, typed
header cross-decode, and unchanged-address behavior. The direct reverse audit
now has six open APIs; broader P18-V01 reconciliation remains open.

## P18-V01 direct `deleteUser` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.deleteUser` sends request 3003 with a
`DeleteUserRequestHeader`, no body, and the caller timeout. Core exposes
`MQClientApi::delete_user`, preserves nullable usernames and Java's no-op
header validation, and leaves the Broker address unchanged when VIP channels
are enabled. Invoked Java fixtures and Rust loopback tests compare the header,
absent body, success and Broker error, timeout, typed-header cross-decode, and
unchanged-address behavior. The direct reverse audit now has five open APIs;
broader P18-V01 reconciliation remains open.

## P18-V01 direct `listUser` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.listUser` sends request 3005 with a
`ListUsersRequestHeader`, no body, and the caller timeout. Core exposes
`MQClientApi::list_user`, returns a nullable list of nullable `UserInfo` rows,
and preserves null-body and JSON-null results. The supplied Broker address is
unchanged when VIP channels are enabled. Invoked Java fixtures and Rust
loopback tests compare nullable filters, request framing, populated/empty/null
lists, null members, success and Broker errors, timeout, typed-header
cross-decode, and unchanged-address behavior. Fastjson's object-root input
produces a list containing the original object-root text as a String, despite
the declared `List<UserInfo>` type. Rust now preserves this Java result as an
opaque serialized list value within its `UserInfo` adapter; the loopback
fixture compares the resulting `["{}"]` value while keeping ordinary
`UserInfo` decode and serialization behavior unchanged. The auth-method review
added the previously omitted `getUser` and `getAcl` RPCs, and the subsequent
direct User/ACL rows complete that operation inventory. Broad P18-V01
reconciliation remains open.

## P18-V01 direct `getUser` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.getUser` sends request 3004 with a
`GetUserRequestHeader`, no body, and the caller timeout. Core exposes
`MQClientApi::get_user`, preserves nullable usernames and nullable `UserInfo`
results, and leaves the supplied Broker address unchanged when VIP channels
are enabled. Invoked Java fixtures and Rust loopback tests compare request
headers, populated/default/null responses, success and Broker errors, timeout,
typed-header cross-decode, and unchanged-address behavior. The nullable reply
path now calls `UserInfo::java_decode_nullable`, matching the pinned Fastjson
decoder. Two invoked cases verify trailing text and a second JSON document
after raw U+001A; the existing absent-body and JSON-null cases still return
`None`. Broader P18-V01 reconciliation remains open.

## P18-V01 direct `getAcl` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.getAcl` sends request 3009 with a
`GetAclRequestHeader`, no body, and the caller timeout. Core exposes
`MQClientApi::get_acl` and nullable `AclInfo`, including nested policy and
entry DTOs that retain null collections and null members. The supplied Broker
address is unchanged when VIP channels are enabled. Invoked Java fixtures and
Rust loopback tests compare nullable subjects, default/populated/null/empty
responses, null policy members, success and Broker errors, timeout,
typed-header cross-decode, and unchanged-address behavior. The auth reverse
audit now has four open APIs; broader P18-V01 reconciliation remains open.

## P18-V01 direct `createAcl` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.createAcl` sends request 3006 with a
`CreateAclRequestHeader`, a serialized `AclInfo` body, and the caller timeout.
Core exposes `MQClientApi::create_acl`, derives the nullable header subject
from `AclInfo`, and preserves the supplied Broker address when VIP channels
are enabled. Invoked Java fixtures and Rust loopback tests compare nullable
subjects and policy data, exact request body bytes, success and Broker errors,
timeout, typed-header cross-decode, and unchanged-address behavior. The auth
reverse audit now has three open APIs; broader P18-V01 reconciliation remains
open.

## P18-V01 direct `updateAcl` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.updateAcl` sends request 3007 with an
`UpdateAclRequestHeader`, an `AclInfo` body, and the caller timeout. Core
exposes `MQClientApi::update_acl`, preserves nullable subjects and ACL policy
data, and leaves the supplied Broker address unchanged with VIP channels
enabled. Invoked Java fixtures and Rust loopback tests compare exact body
bytes, nullable fields, success and Broker errors, timeout, typed-header
cross-decode, and unchanged-address behavior. The auth reverse audit now has
two open APIs; broader P18-V01 reconciliation remains open.

## P18-V01 direct `deleteAcl` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.deleteAcl` sends request 3008 with
`DeleteAclRequestHeader.subject` and `.resource`, no body, and the caller
timeout; its nullable `policyType` remains unset by the method. Core exposes
`MQClientApi::delete_acl`, preserves nullable subject/resource values, and
leaves the supplied Broker address unchanged when VIP channels are enabled.
Invoked Java fixtures and Rust loopback tests compare header fields, absent
body, success and Broker errors, timeout, typed-header cross-decode, and
unchanged-address behavior. The auth reverse audit now has one open API;
broader P18-V01 reconciliation remains open.

## P18-V01 direct `listAcl` Broker API — 2026-10-22

Pinned Java 5.5.1 `MQClientAPIImpl.listAcl` sends request 3010 with a
`ListAclsRequestHeader`, no body, and the caller timeout. Core exposes
`MQClientApi::list_acl`, accepts nullable subject/resource filters, and
preserves nullable list roots and nullable `AclInfo` members. The supplied
Broker address is unchanged with VIP channels enabled. Invoked Java fixtures
and Rust loopback tests compare filters, null/default/populated responses,
null list members, Fastjson object-root behavior, success and Broker errors,
timeout, typed-header cross-decode, and unchanged-address behavior. The auth
reverse audit has no remaining direct RPC rows; broader P18-V01 reconciliation
remains open.

## P18-V01 Rustdoc core-export reverse scan — 2026-10-22

An exact-name scan of the `rocketmq-classic-core` Rustdoc JSON export found
several public identifiers whose Java mapping was already recorded under its
Java name or in a different phase matrix. The generated crate-root module
marker is excluded from the symbol count. These entries make the reverse
links explicit; they add no client setting or operation.

| Rust export | Pinned Java counterpart / disposition |
| --- | --- |
| `SessionCredentials::{ACCESS_KEY,SECRET_KEY,SECURITY_TOKEN,SIGNATURE,CHARSET,key_file_path}` | Java `SessionCredentials` public constants and `KEY_FILE` path value. `AclUtils` uses the same signing field names and UTF-8 charset; the Rust-only `KEY_FILE_PROPERTY` environment key is private. |
| `AclClientRpcHook::new`, `RpcHook::before_request_mut_result` | Java `AclClientRPCHook(SessionCredentials)` and `doBeforeRequest`; Rust materializes a pending typed command header before canonicalizing extension fields and body for the signature. The ACL fixture's `customHeaderCase` compares materialized fields, canonical bytes, and signature. |
| `DEFAULT_CHARSET`, `DEFAULT_ALGORITHM` | `AclSigner.DEFAULT_CHARSET` and `AclSigner.DEFAULT_ALGORITHM`. |
| `AclUtilsError`, `is_asterisk`, `is_colon`, `is_comma`, `is_minus`, `with_status_code`, `with_status_code_and_message`, `with_cause`, `with_status_code_and_source`, `set_code` | `AclUtils` scope helpers and `AclException` constructors/accessors. Rust uses typed errors and `Error::source()` for the Java `AclException`/`Throwable` paths; the exact Java helper signature and invoked behavior are in the Phase 16 ACL matrix. |
| `AclUtils::expand_ip`, `AclUtils::v6_ip_process` | Java `AclUtils.expandIP` and `v6ipProcess`. `expand_ip` applies Java default-locale uppercase, including Lithuanian `After_Soft_Dotted` combining-dot context, then pads segments using Java UTF-16 string length. The invoked ACL fixture compares locale and supplementary-character inputs. |
| `AdminQueryError`, `query_message_across_brokers` | Java `MQAdminImpl.queryMessage` aggregation and its route/RPC failures; Phase 17 pins the facade calls, result order and failure handling. |
| `NameServerConfigs` | The Java `MQClientAPIImpl.getNameServerConfig` map from NameServer addresses to nullable property tables. |
| `export_rocksdb_config_to_json_nullable`, `get_producer_connection_list_nullable` | Nullable Rust adapters for Java methods whose list/group inputs may be null; these preserve the Java wire omission and validation branches. |
| `NotifyResult::{is_has_msg,set_has_msg,is_polling_full,set_polling_full}` | Java `MQClientAPIExt.NotifyResult` accessors and stable `toString()`. |
| `LocalTransactionState` | Java `client.producer.LocalTransactionState`, including declaration order and lookup behavior. |
| `TraceBean` fields `topic`, `message_id`, `offset_message_id`, `tags`, `keys`, `store_host`, `client_host`, `store_time_millis`, `body_length`, `retry_times`, `message_type`, `transaction_id`, `transaction_state`, `from_transaction_check` | Java `TraceBean` fields/accessors; `store_time_millis` is retained although the 5.5.1 wire encoder omits it. Java inherits identity `equals`/`hashCode`; Rust omits structural `PartialEq`/`Eq` and compile-fail doctests guard that boundary. The trace fixture records that equal-value beans compare unequal while the same reference compares equal. |
| `TraceContext` fields `trace_type`, `timestamp_millis`, `region_id`, `region_name`, `group_name`, `cost_time_millis`, `success`, `request_id`, `context_code`, `access_channel`, `trace_beans`; `compare_to` / `java_compare_to_nullable` | Java `TraceContext` fields/accessors and `Comparable<TraceContext>`; both comparison methods order timestamps, and the nullable adapter maps Java's `NullPointerException` for null `other` to `TraceContextCompareError::NullOther`. Inherited `equals`/`hashCode` use identity. Rust omits structural `PartialEq`/`Eq`; compile-fail doctests guard that boundary. The trace fixture records equal-state identity, equal-timestamp comparison, and null-argument failure. |
| `TRACE_FIELD_SEPARATOR`, `TRACE_RECORD_SEPARATOR` | Java `TraceConstants.CONTENT_SPLITOR` (`U+0001`) and `FIELD_SPLITOR` (`U+0002`) respectively. Rust names describe their record roles. |
| `RequestResponseCallback`, `RequestTimeoutCause`, `RequestSendFailureCause::{new,message}` | Java `RequestCallback` and the timeout/send-failure `Throwable` values delivered through `RequestResponseFuture`; `RequestSendFailureCause` keeps its message private and exposes read-only access matching `Throwable.getMessage()`. |
| `UniqueIdError` | Checked error carrier for the pinned `MessageClientIDSetter` decoding helpers; valid IDs and accepted layouts are covered by the Phase 7 unique-ID fixture. |
| `expire_if_due`, `fail_send`, `mark_send_succeeded`, `register_reply_request_with_future` | Rust cross-crate request/reply lifecycle adapters for the Java `RequestFutureHolder` correlation table and `RequestResponseFuture` completion paths. Their hidden owner methods do not add a user-facing request/reply mode. |
| `get_or_create_with_hook_and_tls_config` | Rust cross-crate runtime-construction adapter that carries the Java-mapped TLS settings into Producer/Consumer construction. It is hidden from generated public API documentation; Java configuration remains on the mapped ClientConfig/transport settings. |
| `transfer_keys` | Rust trace-encoder helper used by the Producer crate; it is hidden from generated public API documentation. Java exposes the resulting transfer-key data through its trace encoder flow. |
| `RequestFutureTable` | Java `RequestFutureHolder.getRequestFutureTable()` returns the live `ConcurrentHashMap`; Rust wraps the same process-wide correlation table and exposes map operations (`get`/`insert`/`remove`/`clear`/`contains_key`/`len`/`is_empty`/`keys`/`values`). `shared()` and `Default` return the same table view. |
| `RequestFutureError` | Rust typed result for the pinned Java `RequestResponseFuture` null-latch failure: Java stores the response, then throws `NullPointerException` from `putResponseMessage` or wait; Rust stores first and returns `MissingCountDownLatch`. The behavior and error-type adaptation are recorded in the Phase 15 request/reply matrix. |
| `add_write_perm_of_broker`, `wipe_write_perm_of_broker` | Java `MQClientAPIImpl.addWritePermOfBroker` and `wipeWritePermOfBroker`; both send their direct NameServer RPC and return the response topic count. Rust keeps nullable Broker-name inputs and the caller deadline. |
| `delete_subscription_group` | Java `MQClientAPIImpl.deleteSubscriptionGroup` and `MqClientAdminImpl.deleteSubscriptionGroup`; Rust sends the equivalent typed header request and classifies the Broker response. |
| `get_all_producer_info`, `get_broker_runtime_info`, `get_producer_connection_list` | Java `MQClientAPIImpl.getAllProducerInfo`, `getBrokerRuntimeInfo`, and `getProducerConnectionList`; Rust uses the same request codes and nullable response DTOs, and applies the configured VIP address transform to Broker RPCs. |
| `invoke_broker_to_reset_offset` | Java `MqClientAdminImpl.invokeBrokerToResetOffset` typed-header RPC; Rust decodes the Java `ResetOffsetBody` queue-offset response and maps missing/malformed bodies and Broker failures. |
| `get_broker_lite_info`, `trigger_lite_dispatch` | Java `MQClientAPIImpl.getBrokerLiteInfo` and `triggerLiteDispatch` at the pinned 5.5.1 source revision, using request codes 200074 and 200079. Rust preserves Java's VIP transform, nullable response body/header fields, and Broker response classification. |

With the generated crate-root module marker excluded, the Core scan covered
682 named public items and left zero exact-name omissions. It found no
additional Core public setting or client operation beyond
these Java-mapped APIs and the hidden cross-crate adapters. It checks exported
names and their mappings; behavior acceptance remains in the named fixture
and phase-matrix evidence above.

## P18-V01 Rustdoc Producer-export reverse scan — 2026-10-22

The exact-name scan of the `rocketmq-classic-producer` Rustdoc JSON export
covered 357 named public items and found these names without an exact mention
in the compatibility docs.
They are Rust names for pinned Java hooks, contexts, table views, callbacks,
trace dispatch, and request-future lifecycle operations.

| Rust export | Pinned Java counterpart / disposition |
| --- | --- |
| `HookException` | Borrowed Rust `Error` view of Java hook-context `Throwable`; the concrete Rust cause remains downcastable. |
| `broker_addr`, `name_srv_addr`, `communication_mode`, `send_result`, `trace_context` | Fields in Rust `SendMessageContext` / `CheckForbiddenContext`, mapped to Java `SendMessageContext` and `CheckForbiddenContext` fields (`brokerAddr`, `nameSrvAddr`, `communicationMode`, `sendResult`, `traceContext`). `communication_mode` maps Java `CommunicationMode` values and ordinals. |
| `local_transaction_state` | `TransactionSendResult.local_transaction_state` maps Java `TransactionSendResult.getLocalTransactionState()`. |
| `get_instance`, `get_request_future_table`, `start_scheduled_task` | `RequestFutureHolder.getInstance()`, `getRequestFutureTable()`, and `startScheduledTask()`. Rust's async scheduler preserves Java's initial three-second delay and one-second scan period; shutdown ownership is mapped in Phase 15. |
| `TopicPublishInfoEntryHandle` | Write-through Rust entry view for Java `DefaultMQProducerImpl.getTopicPublishInfoTable().entrySet()` entries and `Map.Entry.setValue`; iteration returns detached handles while updates target the live table. |
| `TopicPublishInfoTableError` | Typed Rust errors for Java `ConcurrentHashMap` null key/value/map/callback and null remapping-result failures. Java throws `NullPointerException`; the Phase 7 producer-table matrix records this typed-error adaptation and collection behavior. |
| `TraceProducerKind` | Java `TraceDispatcher.Type` (`PRODUCE`, `CONSUME`), with matching names, order, ordinals, and lookup behavior. |
| `TraceDispatcherError` | Rust `Result` carrier for the Java `AsyncTraceDispatcher` startup, worker, and shutdown lifecycle. It represents Rust worker/lifecycle failures and does not add a trace mode. |
| `TransactionCallbackError::{new,message}` | Typed Rust callback failure for Java `TransactionListener` / `TransactionCheckListener` calls. Its stored message is private and read-only through `message()`, adapting Java throwable construction/message access. It does not implement `Clone`, `PartialEq`, or `Eq`, because Java `Throwable` inherits identity-based equality. Java callback exceptions and null-like results resolve to `UNKNOW`; Rust error, panic, and `None` paths preserve that decision, as recorded in the Phase 14 transaction matrix. |

## P18-V01 Rustdoc Consumer-export reverse scan — 2026-10-22

The exact-name scan of the `rocketmq-classic-consumer` Rustdoc JSON export
covered 445 named public items. Its initial name join found public names for
Java consumer types and methods, plus Rust ownership, async, and typed-error
adapters. The table groups the names by the Java owner whose API they expose.

| Rust export | Pinned Java counterpart / disposition |
| --- | --- |
| `ConsumeConcurrentlyStatus` | `consumer.listener.ConsumeConcurrentlyStatus`, including `CONSUME_SUCCESS` and `RECONSUME_LATER`. |
| `PushMessageListener` | Rust enum selecting Java `MessageListenerConcurrently` or `MessageListenerOrderly`; the selected callback receives the corresponding Java context and status. |
| `TopicMessageQueueChangeListener` | Java `consumer.TopicMessageQueueChangeListener`; Rust stores the listener as a shared callback and preserves queue-change delivery. |
| `SharedFilterMessageHooks` | Shared live Rust hook-list handle for Java `FilterMessageHook` registration. Replacing the handle preserves Java `registerFilterMessageHook(ArrayList)` alias behavior. |
| `ConsumeMessageServiceFuture` | Boxed asynchronous return carrier for the Rust `ConsumeMessageService` extension point; the operation maps to Java `impl.consumer.ConsumeMessageService` lifecycle, direct-consume, and submission methods. |
| `OffsetStoreFuture`, `OffsetStoreError` | Async result and typed failure carrier for Java `consumer.store.OffsetStore` operations; remote RPC and local-file failures map to the existing Java client exception paths. |
| `PullConsumerError`, `LitePullConsumerError` | Rust public error enums for Java Pull/Lite Pull operations. They wrap the Java configuration, client, Broker, offset, and lifecycle failure branches recorded in the Phase 13/14 consumer matrices. |
| `ProcessPullError` | Typed Rust failures for the Java `PullAPIWrapper.processPullResult` FOUND-message decode and inner-batch paths; accepted data and failure order are covered by the Phase 14 pull-result fixtures. |
| `PopProcessError`, `PopReceiptError` | Typed Rust error carriers for Java POP response decoding and `ExtraInfoUtil` receipt parsing. `PopReceiptError` keeps malformed numeric and missing-field outcomes distinct while the original receipt remains available for forward-compatible fields. |
| `PopRequest` | Java `impl.consumer.PopRequest`, including its no-argument constructor, nullable group/topic/queue/process-queue fields, lock-first and init-mode state, `MessageRequestMode.POP`, equality/hash selection, UTF-16 String hashing, and `toString`. `SharedValue<MessageQueue>` and `Arc<PopProcessQueue>` retain the Java setter/getter reference semantics; the nullable `MessageRequest.consumer_group` routes through `select_consumer_nullable`. `fixtures/consumer/facade-api.json` pins all 16 public methods and direct behavior. |
| `PullRequest` | Java `impl.consumer.PullRequest`, including its no-argument constructor, nullable group/queue/process-queue fields, next-offset and previously-locked state, `MessageRequestMode.PULL`, equality/hash selection, UTF-16 String hashing, and `toString`. `SharedValue<MessageQueue>` and `Arc<ProcessQueue>` preserve mutable reference identity. The direct value adapter is separate from the crate-private assignment scheduler request; its 14 public methods and state behavior are pinned in `fixtures/consumer/facade-api.json`. |
| `ProcessQueueAtomicLong` | Rust operation view for Java `ProcessQueue.getMsgCount()` / `getMsgSize()` `AtomicLong` values, including `get_and_add`, `add_and_get`, `get_and_set`, increment/decrement, and `compare_and_set` operations with Java wrapping arithmetic. |
| `ProcessQueueMessageTreeMap`, `ProcessQueueMessageTreeReadGuard`, `ProcessQueueMessageTreeWriteGuard` | Lock-backed Rust views for Java `ProcessQueue.getMsgTreeMap()`'s live `TreeMap`. Read/write guards preserve direct map edits while keeping independent count and byte counters unchanged, matching Java's separate fields. |
| `get_msg_tree_map`, `get_msg_count`, `get_msg_size`, `get_max_span`, `contains_message`, `contains_message_nullable`, `has_temp_message`, `put_message`, `put_message_nullable`, `put_message_items_nullable`, `take_messages`, `remove_message`, `remove_message_nullable`, `make_message_to_consume_again`, `make_message_to_consume_again_nullable`, `make_message_to_consume_again_items_nullable`, `clean_expired_msg`, `clean_expired_msg_nullable`, `fill_process_queue_info`, `fill_process_queue_info_nullable` | Java `ProcessQueue.getMsgTreeMap`, `getMsgCount`, `getMsgSize`, `getMaxSpan`, `containsMessage`, `hasTempMessage`, `putMessage`, `takeMessages`, `removeMessage`, `makeMessageToConsumeAgain`, `cleanExpiredMsg`, and `fillProcessQueueInfo`. The nullable adapters map Java's null-message `false`, null-list put/removal/requeue and null-consumer `NullPointerException` paths, null-element partial put/requeue mutation before `NullPointerException`, and null-info swallowed exception; Rust's queue owner adds generation-safe in-flight bookkeeping behind these same queue operations. |
| `rebalance_lock_max_live_time_millis`, `rebalance_lock_interval_millis`, `is_dropped`, `set_dropped`, `is_locked`, `set_locked`, `is_lock_expired`, `is_pull_expired`, `get_last_lock_timestamp`, `set_last_lock_timestamp`, `get_last_pull_timestamp`, `set_last_pull_timestamp`, `get_last_consume_timestamp`, `set_last_consume_timestamp`, `get_msg_acc_cnt`, `set_msg_acc_cnt`, `get_try_unlock_times`, `inc_try_unlock_times`, `get_consume_lock` | Java `ProcessQueue` public static fields `REBALANCE_LOCK_MAX_LIVE_TIME` and `REBALANCE_LOCK_INTERVAL`, plus its state/accessor methods (`isDropped`, `setDropped`, lock/pull expiration, timestamp, accumulated-count, unlock-attempt, and orderly-consume lock accessors). Rust uses associated accessors to expose the Java field values and reuses their paired first-access snapshot in orderly lock scheduling and expiry checks. `PopProcessQueue.is_pull_expired` maps to the corresponding Java POP queue check. |
| `get_and_add`, `add_and_get`, `get_and_set`, `get_and_increment`, `increment_and_get`, `get_and_decrement`, `decrement_and_get`, `compare_and_set` | Java `AtomicLong` methods returned from `ProcessQueue.getMsgCount()` / `getMsgSize()`; operations mutate the same cached counters. |
| `get_last_pop_timestamp`, `set_last_pop_timestamp`, `get_wai_ack_msg_count`, `inc_found_msg`, `dec_found_msg`, `fill_pop_process_queue_info` | Java `PopProcessQueue` getters, setters, found-message counter methods, and `fillPopProcessQueueInfo`; the Rust spelling `wai` retains Java's declared `getWaiAckMsgCount` typo. |
| `trace_context` | `ConsumeMessageContext.trace_context` maps Java `ConsumeMessageContext.traceContext` and carries hook-owned trace state between before/after callbacks. |
| `LiteTraceKind`, `trace_kind`, `LiteTraceDispatcher`, `LiteTraceError` | Consumer trace kinds map Java `TraceType.SubBefore` / `SubAfter`; `LiteTraceDispatcher` is the Rust async owner for Java `AsyncTraceDispatcher` consume-trace records, and `LiteTraceError` carries Rust producer/worker lifecycle failures. The encoded control-character format is covered by the Phase 16 trace fixtures. |

The root façade source scan covered all 128 direct re-exports with zero
exact-name omissions after these crosswalks. Together, these Rustdoc/source
reverse scans account for the exact-name omissions from the Core, Producer,
Consumer, and façade exports. They do not replace P18-V01's complete
Java source, settings, inherited API, validation, error, and control-flow
reconciliation; that broader audit remains open.

## P18-V01 Rustdoc export refresh after `PullRequest` — 2026-10-24

Regenerated all-feature Rustdoc JSON after adding the public consumer
`PullRequest` value. Excluding each crate root, current product-crate totals
are Common 442, Consumer 479, Core 800, Producer 379, Protocol 1,528, and
Remoting 341 distinct public names. The Consumer crate has 701 public item
records; Remoting has 404. The root facade direct re-export scan now includes
128 names. Rustdoc JSON completed with the workspace's existing `publish` and
Consumer `AtomicU64::fetch_update` warnings. Counts track exported names only;
broad P18-V01 behavior and reverse-export reconciliation remains `PARTIAL`.

## P18-V01 Rustdoc export refresh after `PopRequest` — 2026-10-24

Regenerated all-feature Rustdoc JSON after adding the public consumer
`PopRequest` value and nullable `MessageRequest` group adapter. Excluding each
crate root, current product-crate totals are Common 442, Consumer 472, Core
800, Producer 379, Protocol 1,528, and Remoting 341 distinct public names.
The Consumer crate has 686 public item records; Remoting has 404. The root
facade direct re-export scan now includes 127 names. Rustdoc JSON completed
with the workspace's existing `publish` and Consumer `AtomicU64::fetch_update`
warnings. Counts track exported names only; broad P18-V01 behavior and
reverse-export reconciliation remains `PARTIAL`.

## P18-V01 Rustdoc Common-export reverse scan — 2026-10-22

The `rocketmq-classic-common` reverse scan groups its names by the pinned Java
owner and the existing invoked message-codec or NameServer fixtures.

| Rust export | Pinned Java counterpart / disposition |
| --- | --- |
| `DUP_INFO`, `INDEX_KEY_TYPE`, `INDEX_TAG_TYPE`, `INDEX_UNIQUE_TYPE`, `KEY_SEPARATOR`, `TIMER_ENGINE_FILE_TIME_WHEEL`, `TIMER_ENGINE_ROCKSDB_TIMELINE`, `TIMER_ENGINE_TYPE`, `PROPERTY_BORN_HOST`, `PROPERTY_BORN_TIMESTAMP`, `PROPERTY_BUYER_ID`, `PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS`, `PROPERTY_CLUSTER`, `PROPERTY_CONSUME_START_TIMESTAMP`, `PROPERTY_CORRECTION_FLAG`, `PROPERTY_CORRELATION_ID`, `PROPERTY_CRC32`, `PROPERTY_DELAY_TIME_LEVEL`, `PROPERTY_DLQ_ORIGIN_MESSAGE_ID`, `PROPERTY_DLQ_ORIGIN_TOPIC`, `PROPERTY_EXTEND_UNIQ_INFO`, `PROPERTY_FIRST_POP_TIME`, `PROPERTY_FORWARD_QUEUE_ID`, `PROPERTY_INNER_BASE`, `PROPERTY_INNER_MULTI_DISPATCH`, `PROPERTY_INNER_MULTI_QUEUE_OFFSET`, `PROPERTY_INNER_NUM`, `PROPERTY_INSTANCE_ID`, `PROPERTY_KEYS`, `PROPERTY_MAX_OFFSET`, `PROPERTY_MAX_RECONSUME_TIMES`, `PROPERTY_MESSAGE_REPLY_TO_CLIENT`, `PROPERTY_MESSAGE_TTL`, `PROPERTY_MESSAGE_TYPE`, `PROPERTY_MIN_OFFSET`, `PROPERTY_MQ2_FLAG`, `PROPERTY_MSG_REGION`, `PROPERTY_ORIGIN_GROUP`, `PROPERTY_ORIGIN_MESSAGE_ID`, `PROPERTY_POP_CK_OFFSET`, `PROPERTY_PRODUCER_GROUP`, `PROPERTY_PUSH_REPLY_TIME`, `PROPERTY_REAL_QUEUE_ID`, `PROPERTY_REAL_TOPIC`, `PROPERTY_RECONSUME_TIME`, `PROPERTY_REDIRECT`, `PROPERTY_REPLY_MESSAGE_ARRIVE_TIME`, `PROPERTY_RETRY_TOPIC`, `PROPERTY_SHARDING_KEY`, `PROPERTY_TAGS`, `PROPERTY_TIMER_DELAY_LEVEL`, `PROPERTY_TIMER_DELAY_MS`, `PROPERTY_TIMER_DELAY_SEC`, `PROPERTY_TIMER_DELIVER_MS`, `PROPERTY_TIMER_DEL_UNIQKEY`, `PROPERTY_TIMER_DEQUEUE_MS`, `PROPERTY_TIMER_ENQUEUE_MS`, `PROPERTY_TIMER_OUT_MS`, `PROPERTY_TIMER_ROLL_LABEL`, `PROPERTY_TIMER_ROLL_TIMES`, `PROPERTY_TRACE_CONTEXT`, `PROPERTY_TRACE_SWITCH`, `PROPERTY_TRANSACTION_CHECK_TIMES`, `PROPERTY_TRANSACTION_ID`, `PROPERTY_TRANSACTION_PREPARED`, `PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET`, `PROPERTY_TRANSFER_FLAG`, `PROPERTY_TRANSIENT_GROUP_CONFIG`, `PROPERTY_TRANSIENT_PREFIX`, `PROPERTY_TRANSIENT_TOPIC_CONFIG`, `PROPERTY_TRANS_OFFSET`, `PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX`, `PROPERTY_WAIT_STORE_MSG_OK` | Java `MessageConst` string fields. The Phase 8 message-constants fixture invokes and compares every public field and default reserved-property set. |
| `BORNHOST_V6_FLAG`, `STOREHOSTADDRESS_V6_FLAG`, `INNER_BATCH_FLAG`, `MULTI_TAGS_FLAG`, `NEED_UNWRAP_FLAG`, `TRANSACTION_COMMIT_TYPE`, `TRANSACTION_NOT_TYPE`, `TRANSACTION_PREPARED_TYPE`, `TRANSACTION_ROLLBACK_TYPE`, `COMPRESSION_LZ4_TYPE`, `COMPRESSION_TYPE_COMPARATOR`, `COMPRESSION_ZLIB_TYPE`, `COMPRESSION_ZSTD_TYPE` | Java `MessageSysFlag` and `CompressionType` constants/comparison, with enum values, names, order, lookup, and unsupported-value behavior covered by the Phase 8 invoked fixture. |
| `MESSAGE_FLAG_POSITION`, `MESSAGE_MAGIC_CODE_POSITION`, `MESSAGE_PHYSIC_OFFSET_POSITION`, `MESSAGE_STORE_TIMESTAMP_POSITION`, `PHY_POS_POSITION`, `QUEUE_OFFSET_POSITION`, `SYSFLAG_POSITION` | Java `MessageDecoder` public layout constants; the Phase 8 message-decoder API fixture compares every public constant. `DecodeError` and `MessageVersionError` are typed Rust decode-failure carriers for Java decode rejection paths. |
| `MessagePropertyParseError`, `MessageSysFlagParseError`, `MessageHostAddressError`, `MessageHostAddressBytesError`, `MessageIdError`, `MessageExtEncodeError`, `CompactMessageError`, `CompressionTypeError` | Typed Rust error carriers for Java message property/sysflag parsing, host/ID decoding, `MessageDecoder` compact and wire encoding, and `CompressionType` lookup. Their accepted inputs and observable codec results are covered by the Phase 7/8 message fixtures. |
| `apply_client_decode_ids`, `into_message_ext`, `is_message_client_ext`, `offset_message_id_for_runtime_type`, `runtime_subtype`, `runtime_subtype_name`, `set_runtime_subtype` | Rust adapters for Java `MessageDecoder.clientDecode`, `MessageExt`/`MessageClientExt` subtype dispatch, and Java-virtual message-ID access when an extension object is passed through a base-message API. They preserve the Java subclass capability in Rust's typed message model. |
| `TopicValidator::{AUTO_CREATE_TOPIC_KEY_TOPIC,RMQ_SYS_BENCHMARK_TOPIC,RMQ_SYS_OFFSET_MOVED_EVENT,RMQ_SYS_ROCKSDB_OFFSET_TOPIC,RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,RMQ_SYS_SCHEDULE_TOPIC,RMQ_SYS_SELF_TEST_TOPIC,RMQ_SYS_TRACE_TOPIC,RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,RMQ_SYS_TRANS_HALF_TOPIC,RMQ_SYS_TRANS_OP_HALF_TOPIC,SYSTEM_TOPIC_PREFIX,SYNC_BROKER_MEMBER_GROUP_PREFIX}` | Java `TopicValidator` public constants. The TopicValidator fixture checks these associated values alongside mutable topic-set behavior. |
| `NameServerAddressError`, `get_name_srv_addr_from_namesrv_endpoint`, `matches_namesrv_endpoint`, `parse_instance_id_from_endpoint`, `validate_instance_endpoint` | Java `NameServerAddressUtils` public address-pattern and endpoint helpers. The Phase 6 runtime inventory and invoked client-config fixture cover null, empty, malformed, Unicode, path, and line-terminator inputs. |
| `utils::system_properties` | Hidden cross-crate implementation bridge for the Java factory global-property overlay; it has no standalone Java API counterpart and is intentionally absent from the `rocketmq-classic` facade exports. The facade compile-fail doctest guards this boundary. |

The Common reverse-name set also included `shares_map_with`, a pointer-identity
inspection helper with no Java method counterpart. It was removed from the
public Rust API; its integration assertion now verifies the same two-way
property-map alias behavior through observable mutations.

## P18-V01 Java-shaped constant export correction — 2026-10-22 (`87900fb8`)

The public module-level constants in `rocketmq_classic_common::topic` duplicated
14 constants already exposed on Java-shaped `TopicValidator`; the module
aliases are now private. Likewise, `rocketmq_classic_core::auth` no longer
exports module-level aliases for the five `SessionCredentials` constants or
the Rust-only `KEY_FILE_PROPERTY` string. The Java-mapped constants remain
available on `TopicValidator` and `SessionCredentials`, and
`SessionCredentials::key_file_path` maps Java's `KEY_FILE` value. Compile-fail
rustdoc examples guard both module paths. This closes these reverse-export
aliases only; broad P18-V01 source/settings/input/error/control-flow
reconciliation remains open.

## P18-V01 Rustdoc Remoting-export reverse scan — 2026-10-22

The Remoting Rustdoc scan covers the command, client, metrics, codec, and
error APIs. The name join identifies accessor spelling differences and
Rust-native adapters; the table records the Java owner for the remaining
export names.

| Rust export | Pinned Java counterpart / disposition |
| --- | --- |
| `ProcessTimerError`, `ProcessTimer::{new,started,is_running,elapsed,start,stop,reset}` | Java `RemotingCommand.processTimer` is a Guava `Stopwatch`; Rust supplies the same timer operations with `Result` for Guava's already-running/not-running exception cases. |
| `initialize_client_api_version`, `REMOTING_VERSION_KEY`, `SERIALIZE_TYPE_PROPERTY`, `SERIALIZE_TYPE_ENV`, `get_header_length`, `create_new_request_id`, `create_request_command`, `create_request_command_with_header`, `create_response_command_for_header`, `create_response_command_with_default_header`, `build_error_response_with_default_header`, `get_callback_list`, `get_callback_list_mut`, `set_callback_list`, `get_process_timer`, `get_process_timer_mut`, `set_process_timer`, `get_protocol_type`, `get_serialize_type_config_in_this_server`, `mark_protocol_type` | Java `MQClientAPIImpl`'s static initializer sets `RemotingCommand.REMOTING_VERSION_KEY` to `MQVersion.CURRENT_VERSION`; `initialize_client_api_version` is the hidden Rust cross-crate binding for that initialization. Java `RemotingCommand` request/response factories apply the cached `configVersion`. The remaining names map the Java `RemotingCommand` constants and factory/accessor operations (`getHeaderLength`, `createNewRequestId`, `createRequestCommand`, `createResponseCommand*`, `buildErrorResponse`, `getCallbackList`, `setCallbackList`, `getProcessTimer`, protocol-type, and serialization-type operations). Rust returns an owned typed header view where Java returns its mutable object reference. |
| `event_type`, `get_remoting_metrics_manager`, `set_remoting_metrics_manager`, `register_processor` | `event_type` selects the corresponding Java `ChannelEventListener` callback (`onChannelConnect`, `onChannelClose`, `onChannelException`, `onChannelIdle`, or `onChannelActive`); metrics accessors map `NettyRemotingAbstract`, and `register_processor` maps `RemotingClient.registerProcessor` with the Java processor/executor pair. |
| `is_active`, `is_open`, `is_writable` | Java `ResponseFuture.getChannel()` followed by the corresponding Netty `Channel` state methods; Rust exposes those channel queries through the response future's identity-preserving channel handle. |
| `get_request_code_desc`, `get_response_code_desc`, `remoting_helper` | Java `RemotingHelper.getRequestCodeDesc` and `getResponseCodeDesc`; the module-level Rust function supplies the helper namespace. |
| `contains_null_key`, `get_mut`, `insert_nullable`, `nullable_entries` | Java `RemotingCommand.extFields` is a `HashMap` that permits null keys and values; these map operations preserve that in-memory state while ordinary Rust map access covers non-null entries. |
| `HISTOGRAM_RPC_LATENCY`, `LABEL_IS_LONG_POLLING`, `LABEL_IS_LONG_POLLING_KEY`, `LABEL_PROTOCOL_TYPE`, `LABEL_PROTOCOL_TYPE_KEY`, `LABEL_REQUEST_CODE`, `LABEL_REQUEST_CODE_KEY`, `LABEL_RESPONSE_CODE`, `LABEL_RESPONSE_CODE_KEY`, `LABEL_RESULT`, `LABEL_RESULT_KEY`, `PROTOCOL_TYPE_REMOTING`, `RESULT_CANCELED`, `RESULT_ONEWAY`, `RESULT_PROCESS_REQUEST_FAILED`, `RESULT_SUCCESS`, `RESULT_WRITE_CHANNEL_FAILED` | Java `RemotingMetricsConstant` fields. `AttributeKey`, `AttributeType`, `AttributeTypeOf`, `AttributeValue`, `Attributes`, `AttributesBuilder`, `RemotingMetricsManager`, `RpcLatencyHistogram`, and `WriteAndFlushStatus` adapt the pinned OpenTelemetry API types and `RemotingMetricsManager` behavior. The Rust attribute supplier getter/setter corresponds to the Java public supplier methods, including its testing setter. |
| `AttributesBuilderSupplier`, `as_map`, `attribute_type`, `boundaries_milliseconds`, `description`, `get_attributes_builder_supplier`, `get_metrics_view`, `get_rpc_latency`, `get_write_and_flush_result`, `init_metrics`, `instrument_type`, `new_attributes_builder`, `put_key`, `record_rpc_latency`, `set_attributes_builder_supplier`, `to_builder` | Java OpenTelemetry `Attributes` / `AttributesBuilder`, `AttributeKey` type-specific operations, `RemotingMetricsManager` public methods, and latency histogram view construction. Rust uses typed attribute keys and values in place of generic OpenTelemetry overloads. |
| `ClientConfig` builder/accessor names `async_semaphore_value`, `channel_not_active_interval`, `client_socket_receive_buffer_size`, `client_socket_send_buffer_size`, `close_socket_if_timeout`, `connect_timeout`, `disable_callback_executor`, `disable_netty_worker_group`, `enable_reconnect_for_go_away`, `max_idle_time`, `max_reconnect_interval_time_seconds`, `oneway_semaphore_value`, `pooled_byte_buffer_allocator_enabled`, `scan_available_nameserver`, `worker_threads`, `write_buffer_high_water_mark`, `write_buffer_low_water_mark`, and their `get_*` / `set_*` counterparts | Java `NettyClientConfig` fields and public getters/setters. `get_socks_proxy_config` and `with_credentials` map the Java SOCKS proxy property and username/password configuration. Units, defaults, accepted values, and update timing remain recorded in the Phase 3 setting table. |
| `add_rpc_hook`, `clear_rpc_hooks`, `get_rpc_hooks`, `remove_rpc_hook`, `with_rpc_hooks`, `with_config`, `with_config_and_channel_event_listener`, `with_name_server_address_list`, `with_processor_table`, `get_available_name_srv_list`, `get_name_server_address_list`, `get_channel_event_listener`, `get_callback_executor`, `set_callback_executor`, `get_client_channel_max_idle_time_seconds`, `set_client_channel_max_idle_time_seconds`, `get_client_close_socket_if_timeout`, `set_client_close_socket_if_timeout`, `get_client_socket_receive_buffer_size`, `set_client_socket_receive_buffer_size`, `get_client_socket_send_buffer_size`, `set_client_socket_send_buffer_size`, `get_client_worker_threads`, `set_client_worker_threads`, `get_connect_timeout_millis`, `set_connect_timeout_millis`, `get_disable_callback_executor`, `set_disable_callback_executor`, `get_disable_netty_worker_group`, `set_disable_netty_worker_group`, `get_enable_reconnect_for_go_away`, `set_enable_reconnect_for_go_away`, `get_scan_available_nameserver`, `set_scan_available_nameserver`, `get_use_tls`, `get_write_buffer_high_water_mark`, `set_write_buffer_high_water_mark`, `get_write_buffer_low_water_mark`, `set_write_buffer_low_water_mark`, `is_pooled_byte_buffer_allocator_enabled`, `set_pooled_byte_buffer_allocator_enabled`, `is_address_reachable`, `is_channel_writable`, `close_channels`, `invoke_async`, `invoke_name_server`, `invoke_name_server_sync_with_peer`, `invoke_oneway`, `invoke_sync`, `invoke_sync_with_deadline`, `register_default_processor`, `register_default_processor_with_executor`, `register_owned_processor`, `register_owned_processor_with_executor`, `register_processor_with_executor`, `pipe_request_pipeline`, `set_request_pipeline` | Java `RemotingClient` / `NettyRemotingClient` operations, `NettyClientConfig` accessors, and `NettyRemotingAbstract` live RPC-hook and processor collections. `with_*` functions construct/configure the Rust client; lock-scoped accessors preserve Java live-list/map mutation. `RequestPipeline` and `RequestPipeline::pipe` map Java `remoting.pipeline.RequestPipeline.execute` and default `pipe` ordering. Rust's `ProcessorLease`, `RequestPipelineError`, and `RpcHookError` are ownership/typed-error carriers for those same registrations and callbacks. |
| `RequestPipeline`, `ProcessorLease`, `RequestPipelineError`, `RpcHookError`, `InvokeCallback` | Java `RequestPipeline`, the processor table's processor/executor registrations, RPC hooks, and Java invoke callback completion. Rust leases own registration lifetime; callback errors retain typed Rust outcomes in place of Java exceptions. |
| `decode_next`, `encode_header`, `encode_header_for_body`, `encode_header_into`, `encode_into`, `write_and_flush`, `write_and_flush_without_deadline`, `remote_addr` | Java `RemotingCommand.encode/decode`, `NettyDecoder`, and the `Channel`/`ChannelHandlerContext` command write and peer-address operations. Rust accepts byte buffers and a typed `RemotingCommand` rather than arbitrary Netty objects. |
| `begin_timestamp_millis`, `execute_invoke_callback`, `invoke_callback`, `is_interrupted`, `is_send_request_ok`, `is_timeout`, `new_with_release_once`, `put_response`, `remote_addr`, `request_command`, `semaphore`, `set_cause`, `set_response_command`, `set_send_request_ok`, `wait_response`, `wait_response_interruptible`, `ResponseWaitInterrupt::{new,interrupt}`, `ResponseWaitError::java_exception_name`, `with_request_command_mut`, `with_response_command_mut` | Java `ResponseFuture` state, callback, response, timeout, semaphore, and channel methods. `wait_response_interruptible` plus its per-wait signal maps Java `waitResponse(long)`'s external thread-interruption result into a Rust-native cancellation handle and typed error; the existing `wait_response` remains the simple noninterruptible convenience. Rust `Display` embeds a populated response using the Java-compatible `RemotingCommand` rendering. Callback identity and non-null exception class/message rendering remain runtime-specific. The Java reflection inventory and Rust response-future API tests pin the declared Java methods and observable mutation order. `SemaphoreReleaseOnlyOnce` maps the Java release-once semaphore helper; Rust uses Tokio semaphore permits. |
| `PrivateKeyDecryptionStrategy`, `register_decryption_strategy`, `apply_properties`, `auth_server`, `ca_pem`, `ciphers`, `client_cert_pem`, `client_key_pem`, `from_ca_file`, `from_java_properties`, `key_password`, `load_properties_file`, `protocols`, `test_mode`, `TlsMode`, `TransportMode` | Java `TlsHelper.DecryptionStrategy`, `TlsSystemConfig`, `NettyClientConfig` TLS properties, trust/auth flags, and TLS property-file loading. `from_ca_file` is the Rust constructor for Java's configured CA-file input; it does not add a trust mode. `ErrorSource`, `TimeoutPhase`, and `TransportError` carry the Java transport exception and timeout branches with Rust error typing. |
| `MessageQueueRouteState::{values,value_of_name,name,ordinal,java_compare_to}` | Java `remoting.protocol.route.MessageQueueRouteState` enum operations. `client-config.json` records names, order, ordinals, comparison, lookup errors, and defensive-copy behavior. |
| `downcast_ref`, `retried` | `ErrorSource::downcast_ref` walks Java-mapped cause chains; `GoAwayCause::retried` records whether Java's retry path has already received GO_AWAY. |

The current Remoting scan has 405 Rustdoc public item records and 342 distinct
names, including the synthetic crate-root module marker; after excluding that
marker there are 341 public identifiers. The Protocol `TopicConfigAndQueueMapping`
wrapper belongs to the separate Protocol scan and does not change these
Remoting counts.
`GoAwayCause.connection_generation` was a Rust-only public field with no Java
counterpart and was removed. `GoAwayCause` now retains the peer address and
retry state; its text follows Java's GO_AWAY message shape and uses the peer
address where Netty supplies a numeric channel ID. The crate-root marker is
not a Java API surface. This name crosswalk covers exports; error paths,
settings, and full control-flow remain part of the open P18-V01 audit.

## P18-V01 Remoting command version initialization — 2026-10-23 (`838286ca`)

Pinned Java 5.5.1 `MQClientAPIImpl` initializes
`RemotingCommand.REMOTING_VERSION_KEY` with `MQVersion.CURRENT_VERSION` (515).
Java request and response factories apply the `RemotingCommand.configVersion`
cached at class initialization. Rust initializes the same client API version
from `MQClientApi` construction and routes Core API requests, Core and Consumer
inbound replies, and generic Remoting processor errors through the command
factories. The invoked Java RPC oracle checks every dispatched request against
`MQVersion.CURRENT_VERSION`; the Rust wire regression checks version 515.
Subprocess coverage records command-before-client, client-before-command, and
cached-property ordering.

## P18-V01 Protocol-export semantic crosswalk — 2026-10-22

The Protocol Rustdoc name join is not a Java API matcher: Java bean methods
use camelCase while Rust methods use snake_case, and Java exposes several
collections through declared fields while Rust uses typed aliases and
identity-preserving handles. The refreshed all-feature Rustdoc scan on code
revision `5024360d` records 1,525 distinct names, including the synthetic
crate-root module marker (1,524 excluding it). A raw substring join against
this map leaves 966 names without a literal mention; these are name-join
diagnostics, not missing Java capabilities. Java camelCase, inherited APIs,
collection fields, and Rust ownership adapters require owner-level mapping.
The crosswalk below records the mapped API families, while full
source/default/input/error/control-flow reconciliation remains open.

| Rust export family | Pinned Java counterpart / evidence |
| --- | --- |
| 61 request/response header types in `header/basic.rs` | Java typed header classes and their inherited `RpcRequestHeader` / `TopicRequestHeader` methods. `request-header-identity.json` and `header-response-identity.json` reflect all 61 class APIs. The owner join checked 1,011 Rust public associated functions: 1,004 names map through snake_case-to-camelCase. The seven remaining methods map as follows: `HeartbeatRequestHeader::{java_hash_code,java_to_string}` to inherited `RpcRequestHeader::{hashCode,toString}`; `PopMessageRequestHeader::java_to_string` and `PopLiteMessageRequestHeader::java_to_string` to their Java `toString`; `SendMessageRequestHeaderV2::from_v1` to `createSendMessageRequestHeaderV2`; and `UnregisterClientRequestHeader::{get_client_id,set_client_id}` to Java's capitalization-preserving `getClientID` / `setClientID`. |
| `ExportRocksDBConfigType::{from_string,to_string,get_type_name}`, `ExportRocksDBConfigToJsonRequestHeader::{fetch_config_type,update_config_type}`, `GetConsumeStatsRequestHeader::{fetch_topic_list,update_topic_list}` | Java `ExportRocksDBConfigToJsonRequestHeader.fetchConfigType` and `toString`, plus `GetConsumeStatsRequestHeader.fetchTopicList` and `setTopicList`; Rust names the list update operation explicitly and retains nullable Java list elements. |
| Request and response code constants in `codes/request.rs` and `codes/response.rs`; `JavaEnumLookupError` | Java `RequestCode`, `ResponseCode`, and enum lookup failure paths. `request-response-codes.json` pins the public names and numeric values; `JavaEnumLookupError` is the Rust `Result` carrier for Java lookup failures. |
| Protocol body DTOs and `get_*` / `set_*` accessors | Java body/header bean types, with Rust accessors using snake_case; direct fields are restricted to Java-public fields or use accessors where Java fields are private/final. Existing body/header fixture families cover serialized field names, omission/default behavior, nullable roots and members, and cross-decode. `ConsumerConnection.connectionSet` and `ProducerConnection.connectionSet` map Java `HashSet<Connection>` to `HashSet<Option<SharedValue<Connection>>>`: `Connection` inherits identity-based `Object.equals/hashCode`, so repeated handles and nulls deduplicate while independently decoded same-field objects remain distinct. The identity-set decoder also retains the Java getter/setter shared-container alias. Direct Java and Rust decoder cases are in `fixtures/admin/query-consume-time-span.json` and `fixtures/producer/core.json`. |
| `java_from_json_nullable`, `java_decode_list_nullable`, `ConsumerRunningInfoBody::decode_java_json_body` | Rust typed entry points for the pinned `RemotingSerializable.decode` operation, preserving Java's null-root and list-element behavior for the concrete DTO type. They add no separate wire operation. |
| `AssignmentAttachments`, `BrokerConsumeStatsGroup`, `BrokerConsumeStatsGroups`, `BrokerLiteGroupMeta`, `HostedQueues`, `LiteClientInfoTopicSet`, `MessageQueueSet`, `MessageRequestModeGroup`, `MessageRequestModeMap`, `NullableQueueOffsetEntries`, `QueueOffsetEntries`, `SubscriptionGroupAttributeMap`, `SubscriptionTagSet`, `ProducerDataSet`, `HeartbeatConsumerDataSet`, `SubscriptionCodeSet`, `ConsumerSubscriptionDataSet`, and `Shared*` collection aliases | Java `Map`, `List`, and `Set` fields in the corresponding request, response, route, and heartbeat DTOs. `SharedValue::{new,with,with_mut,snapshot}` carries Java reference construction/access/mutation through synchronized Rust handles; `SharedValue::ptr_eq` maps Java reference identity (`==`). Rust collection choices preserve null keys/values and list/set members where Java's wire model permits them. |
| `set_*_owned`, `*_snapshot`, and `*_mut` DTO adapters | Java collection setter/getter operations expressed through Rust-owned values, identity-preserving handles, lock-scoped mutation, and owned snapshots. They adapt Java object-reference behavior to Rust ownership; they do not add a wire field or RPC operation. |
| `AclPolicyInfo`, `AclPolicyEntryInfo`, `HaClientRuntimeInfo`, `HaConnectionRuntimeInfo`, `TopicConfigFilterType`, `ClusterInfoLookupError`, `ConsumeStatsComputationError`, `ConsumerConnectionComputeMinVersionError`, `MessageQueueAssignmentEqualsError`, `ProducerInfoToStringError`, `TopicRouteDataChangedError`, `TopicRouteDataCloneError`, `SubscriptionGroupAttributeError`, and the other `*Error` values in Protocol | The corresponding Java DTO validation, comparison, computation, parsing, and clone paths. Rust exposes typed failures for Java exception/error outcomes; these types do not add a message, route, or subscription operation. Existing Phase 4/6 and Phase 8–17 fixtures exercise the associated input and result behavior. |
| `BitSet` operations; `DataVersion::{assign_new_one,next_version,next_version_with_state,state_version}`; `ClusterInfo::{retrieve_all_addr_by_cluster,retrieve_all_cluster_names}`; `LogicQueueMappingItem` offset methods; `TopicConfig` / wrapper table accessors; `NamespaceUtil` methods | Java `BitSet`, `DataVersion`, `ClusterInfo`, `LogicQueueMappingItem`, topic/subscription configuration wrapper, and `NamespaceUtil` operations, with Rust snake_case spelling. `DataVersion` mutation and version comparison preserve Java counter/timestamp changes; route lookups and namespaced-topic helpers operate on the same DTO state. These are protocol-model operations, not added transport settings. |
| `TopicRouteData::{publish_queues,subscribe_queues,static_queue_endpoints}` | Java route/queue mapping values exposed by `TopicRouteData.getQueueDatas` and `getTopicQueueMappingByBroker`; Rust computes the same publish/subscribe queue and static endpoint views used by route-cache code. |
| `LiteSubscription::{add_lite_topics,remove_lite_topics}` | Repeated Java `LiteSubscription.addLiteTopic` / `removeLiteTopic` operations expressed as iterator-based Rust convenience methods; each element is processed in input order. |
| `SubscriptionDataSet::{as_slice,contains,insert,remove,iter,len,is_empty,clear}` and `LiteSubscriptionDto::{lite_topic_set,lite_topic_set_mut,offset_option,offset_option_mut}` | Java `Set` / `List` operations and nested DTO references. Rust exposes synchronized collection views and preserves each field's collection kind, nullable members, and aliasing behavior; set iteration order is normalized where Java's `HashSet` order is unspecified. |
| `MessageRequestMode`, `OffsetOptionType`, `LiteSubscriptionAction`, `ConsumeMessageResult`, `CheckRocksdbCqWriteStatus`, `BoundaryType`, `JavaJsonWriterFeature`, `LanguageCode`, `SerializeType` enum methods | Java enum constants and inherited `Enum` operations (`values`, `valueOf`, `name`, `ordinal`, `compareTo`) or the pinned Fastjson `JSONWriter.Feature` API. Java `fromString` / byte parsers map to Rust `value_of_name`, `from_java_byte`, and typed lookup errors. The writer feature surface and protocol enum/code fixtures pin values, order, ordinals, names, and lookup behavior. |
| `new_nullable`, `topic_config_list`, `group_name_list`, `clean_offset`, `decode_java_json`, `encode_java_json`, `of_with_status` | Java nullable construction, body decode/accessor, and `UserInfo` status paths. Rust's nullable constructors retain Java null fields; list accessors decode the corresponding semicolon/string or DTO values without adding a client operation. |
| `TopicConfig` constructors, getters/setters, default queue-count accessors, `copy_from`, equality/hash/string methods, `encode`/`decode`, `TOPIC_MESSAGE_TYPE_ATTRIBUTE`, `LITE_EXPIRATION_ATTRIBUTE`, and `TopicMessageType::{values,name,get_value,value_of_name,topic_message_type_set,parse_from_message_property,metrics_value}`; `TopicConfigFilterType` | Pinned Java `org.apache.rocketmq.common.TopicConfig`, `TopicAttributes`, and `org.apache.rocketmq.common.attribute.TopicMessageType` public APIs. The two Rust attribute-name constants map `TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE` and `LITE_EXPIRATION_ATTRIBUTE`; `name` / `get_value` map inherited `Enum.name` / `TopicMessageType.getValue`, while `value_of_name`, `topic_message_type_set`, `parse_from_message_property`, and `metrics_value` map `valueOf`, `topicMessageTypeSet`, `parseFromMessageProperty`, and `getMetricsValue`. Rust preserves Java constructor defaults, mutable static queue defaults, nullable attribute keys/values and shared-map identity, copy aliasing, Java HashMap bucket ordering including insertion order for colliding keys, decode update and failure order, filter enum values, expiration parsing, and message-type precedence. A Java null key serializes as the property name `"null"`, so it can coexist with the ordinary string key `"null"` as duplicate JSON properties; decoded JSON property `"null"` remains an ordinary string key. Fastjson omits null-valued attributes from JSON while `toString()` retains them. `fixtures/producer/core.json:staticTopicConfigApi`, `topicMessageTypeApi`, and `topicConfigModelOperations` reflect the surface and pin defaults, mutation, nulls, equality/hash, serialization, malformed/partial decoding, Unicode decimal digits, property precedence, and locale-sensitive metrics labels from Java `String.toLowerCase()`; `topic_config_public_api.rs` compares the captured behavior. Rust `SharedValue` and typed `Result` APIs adapt Java reference identity and exceptions to Rust ownership. `TopicConfigAttributeMap` represents nullable keys with `Option<String>` and retains insertion order so entries with equal Java hash buckets serialize in the same order as the pinned Java `HashMap`. |
| `TopicConfig::decode_nullable` | Java `TopicConfig.decode(String)` dereferences its argument before changing fields, so `decode(null)` throws `NullPointerException`. Rust maps the null argument to `TopicConfigError::NullInput`; the invoked Java fixture and `topic_config_public_api.rs` compare both outcomes. |
| `TopicConfigAndQueueMapping::{new,new_nullable,get_mapping_detail,set_mapping_detail,java_to_string}`, `PartialEq`, `Hash`, and `Deref` | Java's no-arg and `(TopicConfig, TopicQueueMappingDetail)` constructors, declared getter/setter/equality/hash/string methods, and inherited `TopicConfig` API. `new` copies scalar configuration while retaining the shared attributes handle and mapping-detail handle; `new_nullable` adapts the constructor's null-config `NullPointerException`; Rust equality and hash preserve Java's value-equality contract, and `java_to_string` includes mapping detail. The wrapper API surface and direct value/alias/null/equality cases are captured under `topicConfigAndQueueMappingApi` in `fixtures/producer/core.json`. |
| `TopicRouteDataChangedError`, `TopicRouteDataCloneError`, `SharedBrokerAddressMap`, `SharedTopicQueueMappingIds`, `SharedQueueDataHandle`, `SharedBrokerDataHandle`, `SharedTopicQueueMappingInfoHandle`, `SharedRouteQueueDataList`, `SharedRouteBrokerDataList`, `SharedFilterServerList`, `SharedFilterServerTable`, `SharedTopicQueueMappingTable` | Java `TopicRouteData`/`BrokerData` collection and copy/equality semantics. The Rust handles retain mutable object identity across shared fields; typed errors report Java comparison/clone failure outcomes. |
| `GroupRetryPolicy`, `GroupRetryPolicyType`, `ExponentialRetryPolicy`, `CustomizedRetryPolicy`, `ResolvedRetryPolicy`, `RetryPolicy`, `SubscriptionGroupConfig` | Java `SubscriptionGroupConfig`, its retry-policy values, enums, and serialized policy fields. Rust's trait selects the Java policy implementation represented by the DTO and does not expose an additional consumer policy mode. |

## P18-V01 Rustdoc workspace reverse-name continuation — 2026-10-23

The workspace Rustdoc JSON reverse scan found 22 exported names that were not
spelled out in the compatibility crosswalk. Source review at pinned Java
commit `efa1be59d6fc747ad220f036c86030f05f0419f9` maps them to existing Java
collection fields, task outcomes, typed exception paths, or a Java spelling
adapter. The collection aliases describe Java `Map`, `List`, and `Set` values
and preserve getter/setter alias behavior already covered by the DTO
fixtures; they add no wire field, operation, or setting.

| Rust export | Pinned Java counterpart / disposition |
| --- | --- |
| `TopicMessagePropertyMap`, `TopicMessageTypeError` | Java `TopicConfig.attributes` and `TopicMessageType.parseFromMessageProperty` / enum lookup. The alias represents the Java `Map<String, String>` input with nullable Rust values; the typed error carries the Java null and unknown-name exception branches. `topic_config_public_api.rs` and the pinned topic-model fixtures cover this surface. |
| `RuntimeScheduledTaskOutcome<T>`, `RuntimeScheduledTaskWaitError` | Java `ScheduledExecutorService` and `Future` results exposed through `MQClientInstance.getScheduledExecutorService()` and `PullMessageService.getScheduledExecutorService()`. These Rust types carry the already-mapped per-task completion, cancellation, timeout, and callback-failure outcomes. The paused-time scheduler regressions cover those branches. |
| `ProducerInfoTable`, `SharedProducerInfoTable` | Java `ProducerTableInfo.data`, including nullable map values and rows. The shared alias preserves the Java mutable map reference returned by its getter. |
| `TopicStatsOffsetTable`, `SharedTopicStatsOffsetTable` | Java `TopicStatsTable.offsetTable`, including `MessageQueue` keys and mutable `TopicOffset` values. |
| `ConsumeStatsOffsetTable`, `SharedConsumeStatsOffsetTable` | Java `ConsumeStats.offsetTable`, including mutable `MessageQueue` keys, nullable entries, and `OffsetWrapper` values. |
| `SharedClusterBrokerAddrTable`, `SharedClusterAddrTable`, `SharedClusterBrokerNames` | Java `ClusterInfo.brokerAddrTable` and `clusterAddrTable`, including each nested broker-name Set. Shared handles preserve the Java collection-reference aliases. |
| `QueryAssignmentSet`, `SharedQueryAssignmentSet` | Java `QueryAssignmentResponseBody.messageQueueAssignments`, a mutable `Set<MessageQueueAssignment>` retained through its getter/setter. |
| `SharedMessageRequestModeGroup` | The nested group map in Java `MessageRequestModeSerializeWrapper.messageRequestModeMap`; the alias represents its mutable inner map. |
| `HostedQueueMappingItems`, `SharedHostedQueues` | Java `TopicQueueMappingDetail.hostedQueues` and each mutable `List<LogicQueueMappingItem>`. |
| `BrokerLiteTopicMeta` | Java `GetBrokerLiteInfoResponseBody.topicMeta`, the nullable topic-to-count map. |
| `ParentTopicInfoGroups` | Java `GetParentTopicInfoResponseBody.groups`, the nullable mutable Set. |
| `LiteLagInfoList` | Java `GetLiteGroupInfoResponseBody.lagCountTopK` and `lagTimestampTopK`, including nullable list members and their shared `LiteLagInfo` values. |
| `GetKvConfigResponseHeader` | Java `GetKVConfigResponseHeader`; the Rust `Kv` capitalization is a spelling adapter for the same NameServer response header. |

This closes the reverse-name disposition for these 22 items only. The full
P18-V01 Java source/API/settings/input/error/control-flow and Rust
reverse-export reconciliation remains open.

## P18-V01 `MQClientAPIImpl` controller/replica API coverage — 2026-10-23

Correction to the earlier same-day tooling-only disposition: the Phase 18 plan
requires every public `MQClientAPIImpl` capability, and source call-site
location in `DefaultMQAdminExt` or `tools` does not remove that client API from
scope. The earlier list also counted `checkRocksdbCqWriteProgress` as missing,
although its Rust counterpart already existed. The pinned 5.5.1 artifact has
143 public method signatures across 132 method names and three constructors;
these eight public methods are now reconciled:

| Java method | Rust Core API / evidence |
| --- | --- |
| `checkRocksdbCqWriteProgress` | `MQClientApi::check_rocksdb_cq_write_progress`; Java response/body and request fixtures in `checkRocksdbCqWriteProgressOperations`, compared in `admin_rpc.rs`. |
| `getBrokerEpochCache` | `MQClientApi::get_broker_epoch_cache`; nullable `EpochEntryCache` body decode and request sequence in `controller_api.rs`. |
| `getInSyncStateData` | `MQClientApi::get_in_sync_state_data`; resolves metadata first, serializes nullable Broker list, then calls the leader; Java fixture and request/body comparison in `controller_api.rs`. |
| `getControllerMetaData` | `MQClientApi::get_controller_meta_data`; nullable custom-header fields, primitive `isLeader` default, fixed 3000 ms timeout, and Java fixture. |
| `getControllerConfig` | `MQClientApi::get_controller_config`; null/empty list falls back to the remoting NameServer list, an absent selection returns `None`, first error stops the call, and malformed Properties remain nullable. |
| `updateControllerConfig` | `MQClientApi::update_controller_config`; empty Properties or absent/empty controllers is a no-op, all selected controllers are called, and the last non-success reply is returned. |
| `electMaster` | `MQClientApi::elect_master`; metadata then leader request, `designateElect=true`, invocation timestamp, custom response header, and nullable `BrokerMemberGroup` body. |
| `cleanControllerBrokerData` | `MQClientApi::clean_controller_broker_data`; metadata then leader request with clean flags and invocation timestamp. |

The public-method join also checked name-level mappings that do not follow a
direct camelCase-to-snake_case conversion. `ackLiteMessageAsync` delegates to
Java `ackMessageAsync` with the same request header; Rust's
`MQClientApi::ack_message` supplies that RPC for POP/Lite owners.
`exportRocksDBConfigToJson` maps to Core
`MQClientApi::export_rocksdb_config_to_json`. `fetchNameServerAddr`,
`onNameServerAddressChange`, and `updateNameServerAddressList` map to
`ClientRuntime::fetch_name_server_addr`,
`MQClientApiFactory::on_name_server_address_change`, and the runtime/Remoting
address-list updates. `resetMasterFlushOffset` maps to Core
`MQClientApi::reset_master_flush_offset`. Java's `getEarliestMsgStoretime`
maps to `get_earliest_msg_store_time` in the Phase 9 offset RPC crosswalk.
`recallMessageAsync` maps to the Rust
Producer's future-returning recall operation and its Core RPC path.

The invoked fixture `controllerAdminApi` records the seven method signatures
that were missing from Core. `controllerAdminOperations` records their ten
Java requests, including fixed 3000 ms controller calls and caller-selected
2345 ms config calls. Rust integration checks compare request codes, optional
headers and bodies against that fixture; the dynamic invocation timestamp and
Java `Properties` iteration order are normalized. The request-code constants,
including `CLEAN_BROKER_DATA=1011`, were already present in the protocol crate.

This closes the controller/replica API slice only. Broad P18-V01 Java
source/API/settings/input/error/control-flow and reverse-export reconciliation
remains open.

The current Rustdoc JSON scan finds 52 distinct public names in
`api/controller.rs`, including the module, seven RPC methods, DTO types and
fields, DTO operation adapters, constructors, and typed null-dereference
error. The RPC methods are listed above; the other names map to pinned Java
5.5.1 DTOs as follows:

| Rust public names | Java DTO and disposition |
| --- | --- |
| `controller` | Rust module grouping the controller client and DTO adapters; it adds no separate Java capability. |
| `ControllerDtoError`; `new`; `with_alive`; `with_end_offset`; `EpochEntry::from_entry` | Rust typed error for Java null dereferences, no-argument/full-argument DTO constructors, both `ReplicaIdentity` constructors, both value constructors for `EpochEntry`, and Java's `EpochEntry(EpochEntry)` copy constructor. |
| `GetMetaDataResponseHeader::{group,controller_leader_id,controller_leader_address,is_leader,peers}` | Java `GetMetaDataResponseHeader` custom header; direct Rust fields map the Java getter/setter values, and `CommandHeader` maps ext-field encoding/decoding. |
| `ElectMasterResponseHeader::{master_broker_id,master_address,master_epoch,sync_state_set_epoch}` | Java `ElectMasterResponseHeader` custom header; direct Rust fields map its nullable values and `CommandHeader` maps ext-field encoding/decoding. |
| `BrokerReplicasInfo::{replicas_info_table}`; `ReplicasInfo::{master_broker_id,master_address,master_epoch,sync_state_set_epoch,in_sync_replicas,not_in_sync_replicas}`; `ReplicaIdentity::{broker_name,broker_id,broker_address,alive}` | Java `BrokerReplicasInfo`, nested `ReplicasInfo`, and nested `ReplicaIdentity` JSON body fields. Rust serde uses Java camel-case property names while public Rust fields use snake case. Nested map keys/values and list entries retain Java nullable states. |
| `EpochEntryCache::{cluster_name,broker_name,broker_id,epoch_list,max_offset}`; `EpochEntry::{epoch,start_offset,end_offset}` | Java `EpochEntryCache` and `EpochEntry` JSON body fields. Rust serde uses Java camel-case property names; the default end offset is `i64::MAX`, matching Java `Long.MAX_VALUE`. |
| `BrokerMemberGroup::{cluster,broker_name,broker_addrs,minimum_broker_id}` | Java `BrokerMemberGroup` fields and `minimumBrokerId()`. The Rust method returns `Result<i64, ControllerDtoError>`: empty maps return zero, while null maps/keys preserve Java's null-dereference path as a typed error. |
| `BrokerReplicasInfo::add_replica_info`; `ReplicasInfo::{is_exist_in_sync,is_exist_in_not_sync,is_exist_in_all_replicas}`; `ReplicaIdentity::{java_equals,java_hash_code}` | Java `addReplicaInfo`, three `isExistIn*` methods, `equals`, and `hashCode`, with Rust snake-case names except explicit `java_*` semantic methods. `Display` maps Java `toString` for the DTOs that override it. |

Controller DTO operation parity was added in `fix(compatibility): align
controller replica DTO semantics`. `BrokerReplicasInfo::add_replica_info`
maps `addReplicaInfo`, including initialized-map null keys/values and the
null-table failure. `ReplicasInfo::is_exist_in_sync`,
`is_exist_in_not_sync`, and `is_exist_in_all_replicas` map the three Java
membership methods, including list-null errors, nullable query short-circuit
order, `ArrayList.contains` equality behavior, and `||` short-circuiting.
`ReplicaIdentity::java_equals` and `java_hash_code` compare/hash only broker
name, ID, and address; no structural Rust equality/hash traits are added to
this Java identity-equality DTO. Java `toString`/`hashCode` overrides are
mapped by `Display` or explicit `java_hash_code` methods where present in
Java; DTOs inheriting `Object.equals`/`hashCode` do not receive derived Rust
value equality. `EpochEntry::from_entry` maps Java's copy constructor.
Fixture `controllerDtoApi` inventories the Java constructors and methods;
`controllerDtoOperations` records the observed membership, equality, hashes,
display, and null-failure outcomes, and the Core fixture-backed test compares
those cases. This closes the identified controller DTO behavior gaps only;
the broad P18-V01 reconciliation remains open.
The pinned `rocketmq-remoting-5.5.1-sources.jar` SHA-256 is
`ac2d2ad39cbe35e2125ce8cfdd2dd1c20ff7007f9960c1c4b597e292123a6bf6`.

## P18-V01 Rustdoc reverse-export delta — 2026-10-23

Regenerated all-feature Rustdoc JSON at workspace revision `e0937f80` and
compared distinct public names with the pinned inventory at `1dc3a7e2`.
Current product-crate counts are Common 439, Consumer 460, Core 740,
Producer 362, Protocol 1,524, and Remoting 337. The Consumer name set adds 15
names and Core adds 58; neither set removes a previously exported name. The
remaining four crate counts match their recorded inventory. This is a
name-set comparison: a name absent from the delta can still be an additional
method on an existing public type when another item already used that name.

| Newly observed public names | Pinned Java counterpart and disposition |
| --- | --- |
| Core: `factory`, `MQClientApiFactory`, `MQClientApiFactoryError`, `NameserverAccessConfig`, `RemotingClientCreator`, `ClientApiArrayError`, `SharedClientApiArray`, `index`, `set`, `get_client`, `get_clients`, `on_name_server_address_change`, `pre_shutdown`, `namesrv_addr`, `namesrv_domain`, `namesrv_domain_subgroup`, `with_remoting_client_creator`, `with_refresh_scheduler`, `with_optional_refresh_scheduler`, `new_nullable`, `new_nullable_with_remoting_client_creator`, `NullNameServerAccessConfig`, `NullRefreshScheduler` | The `factory` module and pool/access types map Java `MQClientAPIFactory`, `NameserverAccessConfig`, and `ObjectCreator<RemotingClient>`. Java's six-argument and seven-argument constructors map to `MQClientApiFactory::new` and `with_remoting_client_creator`; its seven-argument overload is the public remoting-client injection path. `new_nullable` adapts nullable Java `NameserverAccessConfig`, `namePrefix`, and scheduler references: null access config writes VIP=false before `NullNameServerAccessConfig`, null prefix concatenates as `nullN_0`, and null scheduler follows the static/domain behavior below. `new_nullable_with_remoting_client_creator` adds the nullable seven-argument overload, forwarding an injected creator or using the default path when Java's `ObjectCreator` is null. The invoked factory fixture and Core regression cover both behaviors. `get_client`, `get_clients`, `on_name_server_address_change`, and lifecycle methods map the same Java factory operations. `SharedClientApiArray` and its `get`/`set`/snapshot operations retain Java's live mutable `MQClientAPIExt[]` behavior; `index` identifies its bounds-error slot. Rust typed errors adapt Java exception outcomes. Nameserver fields/getters map all three Java `NameserverAccessConfig` constructor values and getter methods. The injected factory fixture and direct factory tests cover both constructors, nullable inputs, selection, array aliasing, address changes, initialization, and lifecycle. The `TopAddressing` test seam is not exported. |
| Core: `MQClientApi` names `get_name_server_address_list`, `with_name_server_address_list`, `remoting_client` | Java `MQClientAPIImpl.getNameServerAddressList` and `getRemotingClient`. The lock-scoped callback is the Rust safe-access form for the live mutable Java list; the owned getter returns a snapshot. The API factory fixture and remoting list tests cover the returned values and alias updates. |
| Core: `RuntimeScheduledCall`, `RuntimeScheduledTaskHandle`, `RuntimeScheduledTaskOutcome`, `RuntimeScheduledTaskWaitError`, `RuntimeScheduler`, `await_termination`, `cancel`, `execute`, `invoke_all`, `invoke_all_with_timeout`, `invoke_any`, `invoke_any_with_timeout`, `is_cancelled`, `is_finished`, `is_shutdown`, `is_terminated`, `join`, `join_with_timeout`, `remaining_delay`, `schedule_after`, `schedule_at_fixed_rate`, `schedule_call_after`, `schedule_with_fixed_delay`, `shutdown_now`, `submit`, `task_handle`, `wait_with_timeout`, `scheduled_executor_service`, `pull_scheduled_executor_service` | Java `MQClientInstance.getScheduledExecutorService` and `PullMessageService.getScheduledExecutorService`, plus the returned `ScheduledExecutorService` and `Future` behaviors. The Rust methods express immediate/delayed scheduling, periodic modes, returned-value calls, bulk calls, cancellation/status/wait, remaining delay, and graceful/immediate shutdown. Result and outcome types adapt Java `Future` values, cancellation, timeout, and failure states to Rust ownership and `Result`. The paused-time runtime tests and the scheduler API rows above cover these operations and lifecycle branches. |
| Core: `RuntimeServiceThread`, `is_daemon`, `is_stopped`, `join_time_millis`, `make_stop`, `pull_message_service`, `rebalance_service`, `service_name`, `set_daemon`, `shutdown_with_interrupt`, `wakeup` | Java `ServiceThread` inherited lifecycle/state methods and the `PullMessageService` / `RebalanceService` owners. Rust exposes each worker through `ClientRuntime`; `service_name` maps the owner-specific Java service name. `shutdown()` maps the no-argument overload, and `shutdown_with_interrupt` maps `shutdown(boolean)`. The runtime service tests and API rows above cover start, both shutdown forms, stop, wakeup, daemon state, bounded shutdown, and owner retention. |
| Consumer: `ProcessQueueError`, `PopProcessQueueError`, `clean_expired_msg_nullable`, `contains_message_nullable`, `fill_pop_process_queue_info_nullable`, `fill_process_queue_info_nullable`, `make_message_to_consume_again_items_nullable`, `make_message_to_consume_again_nullable`, `put_message_items_nullable`, `put_message_nullable`, `remove_message_items_nullable`, `remove_message_nullable` | Java `ProcessQueue` and `PopProcessQueue` methods with nullable lists, messages, consumers, or info DTOs. The explicit nullable adapters preserve null guards, exception classes, caught errors, and any Java prefix mutation before a null-element failure; typed errors carry the corresponding Java exception branch. The direct invoked Java fixtures and `PHASE_18_REBALANCE_PROCESS_QUEUE_API.md` comparison tests cover the method and mutation paths. |
| Consumer: `local_offset_store_dir` | Java public static `LocalFileOffsetStore.LOCAL_OFFSET_STORE_DIR`, exposed by the Rust accessor with the same default and property-backed value; its Java fixture and offset-store tests cover the constant. |
| Consumer: `rebalance_lock_interval_millis`, `rebalance_lock_max_live_time_millis` | Java public static `ProcessQueue.REBALANCE_LOCK_INTERVAL` and `REBALANCE_LOCK_MAX_LIVE_TIME`, exposed by Rust accessors that preserve the class-initialization snapshot and parse errors. `PHASE_18_REBALANCE_PROCESS_QUEUE_API.md` and the orderly-lock fixture/tests cover defaults, properties, and first-access caching. |

The direct Java source comparison confirms the factory's optional
`ObjectCreator<RemotingClient>` constructor, correcting the earlier statement
that conflated remoting-client creator injection with `TopAddressing`
injection. No unmatched Rust export was found in this 73-name delta. This
closes the current reverse-name delta only; the broad P18-V01 input/error/
control-flow audit remains open.

### P18-V01 factory scheduler injection API gap — 2026-10-23

The pinned Java reflection fixture records `ScheduledExecutorService` in both
public `MQClientAPIFactory` constructors. Rust's earlier `new` and
`with_remoting_client_creator` paths always used Tokio, so callers had no
equivalent scheduler injection point. Core now exports `FactoryRefreshFuture`,
`FactoryRefreshTask`, and `FactoryRefreshScheduler`; the
`with_refresh_scheduler` builder accepts a caller-owned scheduler. When
NameServer discovery is configured, `start` registers one callback per client
with Java's 10-second initial delay and 120-second fixed rate. A rejected
registration becomes `MQClientApiFactoryError::RefreshScheduleFailed`; clients
from earlier successful slots remain visible; the rejected slot remains null
and its remoting client is not started, matching Java's creation and
registration order. The default paths still use factory-owned Tokio refresh
workers.

The 2026-10-23 11:40 all-feature Rustdoc scan counted Common 439, Consumer 460, Core 744,
Producer 362, Protocol 1,524, and Remoting 337 public names. Against the
`1dc3a7e2` inventory, Core now adds 62 names and Consumer adds 15, a total of
77 additions with no removed names. The four new Core names relative to the
previous scan are the scheduler trait, its future/task aliases, and the
builder; all map to the Java constructor's scheduler parameter. The two
factory regressions verify per-client registrations, Java timing, slot-local
updates, and rejected-registration failure handling. This closes the public
scheduler-injection gap only; broad P18-V01 remains open.

The subsequent nullable factory constructor adapters added three public Core
names: `new_nullable`, `new_nullable_with_remoting_client_creator`, and
`NullNameServerAccessConfig`. The all-feature Rustdoc JSON scan at that
revision counted
Common 439, Consumer 460, Core 747, Producer 362, Protocol 1,524, and Remoting
337 public names after excluding crate-root items. The injected Java fixture
and Rust regression cover the seven-argument creator path with a null prefix,
as well as the null creator default path. This closes the named nullable
factory constructor inputs only; broad P18-V01 remains open.

The factory fixture also covers a negative `clientNum`: Java accepts it in
the constructor, then `start()` throws `NegativeArraySizeException` before
assigning the client array. Rust returns `NegativeClientCount` from the same
pre-publication point, so `get_clients()` remains null. The invoked fixture
and Core regression compare the error class and retained state; this closes
only that negative-count start branch.

The header crosswalk is a reflection-backed method-name join, not proof of all
input/error branches. The DTO/body, route, heartbeat, enum, configuration,
and subscription entries still require the full source/default/validation/
error/control-flow audit called for by P18-V01; that audit remains open.

## P18-V01 direct derived-getter override follow-up — 2026-10-22

The pinned Java `Message` getters `getTags`, `getKeys`, `isWaitStoreMsgOK`,
`getBuyerId`, `getDelayTimeLevel`, `getDelayTimeSec`, `getDelayTimeMs`,
`getDeliverTimeMs`, `getTopicSysFlag`, and `getGroupSysFlag` are individually
virtual. Property hooks preserve the base implementation's delegation through
`getProperty`, but do not represent a subtype that overrides one of these
getters directly. `MessageExtSubtype` now has explicit hooks for all ten, and
the corresponding Rust getters prefer a direct subtype override before using
the base property path.

The exact new public Rust hook names are
`MessageExtSubtype::{tags,keys,wait_store_msg_ok,buyer_id,delay_time_level,
delay_time_sec,delay_time_ms,deliver_time_ms,topic_sys_flag,group_sys_flag}`;
each maps to the corresponding Java virtual getter in this list.

The invoked `DerivedGetterMessageExt` fixture returns values that differ from
its overridden `getProperty` values and counts all ten virtual getter calls;
the Rust direct-consume test matches each result, property fallback, and call
count. `fixtures/runtime/consumer-operations.json` SHA-256:
`0d818f3b8a711661a382de19a07db7292c5976f20ddee9a6106b78b9e8860f82`.
This closes the direct-override slice only; broad P18-V01 settings, other
Java classes, input/error/control-flow, and reverse-export reconciliation
remain open.

### Nullable direct-getter return continuation — 2026-10-22

An invoked Java subtype returns explicit null from `getTags`, `getKeys`,
`getBuyerId`, `getTopicSysFlag`, and `getGroupSysFlag` while its
`getProperty` override supplies non-null strings; the sysflag property strings
are malformed. Rust's nested-Option getter hooks preserve Java null and skip
property parsing when the direct override is present. The fixture compares
all five null results, retained property results, and virtual-call count.
`fixtures/runtime/consumer-operations.json` SHA-256:
`8640635c5eb044d1485c182be377a308f909885477d01916ba234fb34ef70b7b`.
Focused Common/Consumer Clippy, formatting, the six-test direct-consume suite,
and Java fixture verification pass. This closes nullable direct results for
these getters only; broader P18-V01 remains open.

## P18-V01 writer-feature inventory and `UserInfo` surface follow-up — 2026-10-22 (`c5892d2c`)

The older writer-feature continuation in this map reports `47/48` and says
`WriteClassName` is unmapped. That status is superseded by the current Rust
adapter implementation: all 48 pinned `JSONWriter.Feature` values are marked
implemented and compared against Java fixture output across 41 body cases, 13
route cases, and 3 heartbeat cases. `WriteClassName` covers root and typed
nested DTO names, map and Set markers, and Java `long` suffixes. The Rust enum
inventory test now fails if any pinned value is not marked implemented.

The pinned Java `UserInfo` class is a plain DTO, not a
`RemotingSerializable` subclass. It has generic static serialization utility
methods but no instance `encode(JSONWriter.Feature...)` method. Rust removed
the unmatched public `UserInfo::java_encode_with_features` method and added a
compile-fail guard. Protocol all-feature tests (156 unit tests, all integration
targets, 158 doctests), strict Protocol Clippy, formatting, and pinned Maven
verification (37 scenarios, invoked fixtures, and Java/Rust cross-decodes)
pass. This closes the `WriteClassName` inventory discrepancy and this
`UserInfo` over-export only; broader P18-V01 settings/API/input/error/control-
flow/reverse-export reconciliation remains open.

## P18-V01 inherited writer API and static-topic mapping — 2026-10-22 (`4a0074f6`)

The pinned Java inheritance audit removes the instance writer-feature API
from five plain classes: `CheckRocksdbCqWriteResult`, `AclInfo`, nested
`AclPolicyInfo`, nested `AclPolicyEntryInfo`, and `TopicConfigAndQueueMapping`.
Their type-specific `java_encode` and `java_to_json` helpers remain as Rust
adapters to Java's generic static utilities. `UserInfo` has the same
plain-class disposition in the preceding section. Compile-fail Rustdoc guards
cover these unmatched instance methods.

`TopicQueueMappingDetail` inherits from `TopicQueueMappingInfo`, which
inherits from `RemotingSerializable`; `LogicQueueMappingItem` also inherits
the writer-feature encoder. The route schema maps `hostedQueues` as a map of
nullable lists containing nullable `LogicQueueMappingItem` values, including
nested `BeanToArray`, `WriteClassName`, null, long and numeric-key behavior.
The Java fixture compares direct-root defaults and populated cases for
`LogicQueueMappingItem`, plus default, null and populated
`TopicQueueMappingDetail` cases, against all 48 writer features.

The static-topic method inventory now includes `LogicQueueMappingItem`'s
constructor, Java property accessors, offset conversion helpers, equality,
hash and string behavior. `TopicQueueMappingDetail` exposes its inherited
mapping accessors plus Java constructor defaults and `putMappingInfo`,
`getMappingInfo`, `buildIdMap`, `computeMaxOffsetFromMapping`,
`cloneAsMappingInfo`, and `checkIfAsPhysical`. Its `equals`/`hashCode` use
`hostedQueues` alone; metadata remains visible to `toString` but does not
affect equality. Java-oracle cases cover collection aliasing, metadata
exclusion, null/empty distinctions, defaults, and the helper methods'
NullPointerException boundaries. Rust maps those null failures through
`TopicQueueMappingDetailError`. `fixtures/runtime/route.json` SHA-256:
`d1c152fc1596f657018e9ed0679355fc43db1da1f607a73ed028f9834c651e8d`.

Protocol all-feature tests, strict Protocol Clippy, workspace default and
all-feature tests, strict workspace Clippy, pinned Maven verification, and
the current P18-V02 server-free checks pass. This closes the named
writer-feature inheritance and static-topic method slices; broad P18-V01
source/API/settings/input/error/control-flow and reverse-export
reconciliation remains open.

## P18-V01 static-topic API surface follow-up — 2026-10-22 (`e415be18`)

The pinned public API comparison adds `TopicQueueMappingInfo::LEVEL_0` and
the inherited `TopicQueueMappingDetail::LEVEL_0` constants. Direct-root
`LogicQueueMappingItem` fixtures now compare its getters and setters, strict
and loose physical/static offset conversions, maximum static offset,
end-offset and logical-offset decisions, offset delta, hash code, and text
against Java for default, populated, and undecided values. Java spells the
logical-offset predicate `checkIfLogicoffsetDecided`; Rust exposes the same
operation as `check_if_logic_offset_decided`.

The refreshed route fixture has SHA-256:
`e1b5ca4167b237c2011e8ecd62600a54ac0b43e522e4ae164157db2c7ddb5716`.
Protocol all-feature tests (159 unit tests and 163 doctests), strict Protocol
Clippy, format/diff checks, direct Java runtime route generation (36 invoked
cases), and pinned Maven `verify` pass on this revision. The full workspace
gate documented for the preceding revision has not yet been refreshed.
Broader P18-V01 reconciliation and current-revision remote Ubuntu and native
Windows execution remain open.

## P18-V01 `LogicQueueMappingItem.equals` direct probe — 2026-10-22 (`70671a33`)

The direct Java fixture now invokes `LogicQueueMappingItem.equals` for equal
default and populated values, each of the eight field differences, and a null
versus empty `bname`. Rust's `PartialEq` result matches all 11 Java outcomes.
The refreshed `fixtures/runtime/route.json` SHA-256 is
`0fcad65ecc00a9c55657113ed494d0d818621f5dcd4fa11e5da87e49c61d9470`.
The focused Protocol differential, strict Protocol Clippy, format/diff checks,
and pinned Maven `verify` pass. This closes the direct equality probe only;
broad P18-V01 reconciliation remains open.

## P18-V01 `TopicConfigAndQueueMapping` direct API probe — 2026-10-22 (`7397bb16`)

The pinned Java surface has two public constructors, five declared methods, and
27 public methods including inherited `TopicConfig` behavior. The direct
fixture compares wrapper defaults and `toString`, the copy constructor,
configuration field copies, shared attribute aliasing, mapping-detail handle
retention and mutation, nullable mapping setters, null-config failure, and
equality/hash outcomes. The Java oracle confirms mapping metadata outside
`hostedQueues` does not affect mapping-detail equality. Rust adds the matching
constructor, `java_to_string`, and `Hash` implementations; the nullable
constructor adapter maps Java's null dereference to `TopicConfigError`.

Protocol all-feature tests pass (159 unit tests, all integration targets, and
163 doctests), as do strict Protocol Clippy, formatting/diff checks, the
focused wrapper test, and pinned Maven `verify` (37 scenarios, 36 runtime
route/VIP cases, 106 typed headers and 19 typed bodies decoded by Java).
`fixtures/producer/core.json` SHA-256:
`39cf5836801a5d5664a61199200dd96b77084cdec557bcc6581eee353967bb39`.
The full workspace gate must be refreshed after this source change. Broad
P18-V01 reconciliation and current-revision remote Ubuntu and native Windows
execution remain open.

## P18-V02 full workspace refresh after static-topic wrapper — 2026-10-22 (`7397bb16`)

The locked default and all-feature workspace tests, strict workspace Clippy,
format/diff checks, differential comparator self-test and 37-scenario
manifest, bounded fuzz smoke, and race/leak smoke pass on the wrapper source
revision. The release facade examples build; workspace benches compile;
workspace metadata and all seven product package listings include `LICENSE`
and `NOTICE`. Four Core Unique ID tests pass under both UTC and Asia/Taipei.
Windows GNU Core all-target checking and Consumer stack-capture `--no-run`
cross-compilation pass; native Windows execution and remote Ubuntu CI remain
unverified. The single-run P18-05 benchmark is unchanged by this DTO-only
source slice. Broad P18-V01 reconciliation remains open.

## P18-V01 NamespaceUtil reverse-export correction — 2026-10-22 (`5024360d`)

The pinned Java `NamespaceUtil` declares four public constants on the class
and nine public method signatures. Rust retains the four Java-shaped
`NamespaceUtil` associated constants and methods. Four duplicate
module-level constants in `rocketmq_classic_protocol::namespace` had been
public aliases without workspace callers; they are now private, with
compile-fail rustdoc coverage preventing imports through that path. This
removes four Rust exports that had no Java counterpart and does not alter the
Java-shaped facade. Broad P18-V01 source/API/settings/input/error/control-flow
and reverse-export reconciliation remains open.

## P18-V01 metadata wrapper identity equality — 2026-10-22

Java `TopicConfigSerializeWrapper` and `SubscriptionGroupWrapper` inherit
`equals` and `hashCode` from `Object`; equal serialized state therefore does
not make distinct wrapper instances equal. Rust removes derived structural
`PartialEq`/`Eq` from both response-body types, and compile-fail doctests guard
against restoring value equality. The direct Java fixture checks equal-state
encoded bytes, distinct-object and same-reference equality, and the declaring
owners of both methods. `fixtures/producer/core.json` SHA-256:
`f0b625d90d319a81070a3cc342f1e655c67d74aa747e0934f12c4b5ed0911095`.
Code commit: `91f842f4`.

## P18-V01 current Rustdoc export inventory — 2026-10-22 (`1dc3a7e2`)

Regenerated all-feature Rustdoc JSON from the current source tree and counted
distinct public item names in each product library, excluding the generated
crate-root marker: Common 439, Consumer 445, Core 682, Producer 362, Protocol
1,524, and Remoting 337. This confirms the previously recorded counts for
Consumer, Core, Protocol, and Remoting, and supersedes the Producer scan's
earlier count of 357. The current name inventory also provides the baseline for
the Common owner crosswalk above. These counts establish exported-name scope;
they do not establish behavior coverage or close broad P18-V01 reconciliation.

## P18-V01 Rustdoc export count refresh — 2026-10-22 (`4248ce38`)

Rebuilt all-feature Rustdoc JSON after the Remoting shutdown and decoder API
corrections. Distinct exported names excluding the generated crate-root marker
remain Common 439, Consumer 445, Core 682, Producer 362, Protocol 1,524, and
Remoting 337. The generated Remoting export has no public `decode_many` or
`decode_many_as_client`; the Java-mapped `decode_many_compatible` and
`decode_many_as_client_compatible` remain. This verifies the current scope count
and the decoder export correction; it does not establish behavior coverage or
close broad P18-V01 reconciliation. The doc build passes with the existing
workspace `publish` and Consumer `AtomicU64::fetch_update` warnings.

## P18-V01 `MessageDecoder.decodeProperties(ByteBuffer)` record-input parity — 2026-10-22 (`67d0d049`)

Pinned Java 5.5.1 reads the system flag, magic code, host-width flags, body
length, topic length, and property length directly. This method does not check
the record's declared total store size. Rust previously delegated to full
record decoding and rejected a zero store size before reading properties; it
now follows the direct field offsets. A positive property length whose text
parses no entries returns an empty map, while a nonpositive length returns
null. Invoked Java fixtures cover both boundaries. The refreshed
`fixtures/messages/records.json` SHA-256 is
`e2fab00b22e5954f54095fe7215848e4e317d6f62461cea166b5cd8be715b317`.

The focused Common differential, Common suite, locked default and all-feature
workspace tests, strict all-target/all-feature Clippy, formatting/diff checks,
and pinned Maven `verify` pass. Maven verifies 37 scenarios, 36 route/VIP
cases, 106 typed headers, and 19 typed bodies. This closes the
`decodeProperties` record-position and positive-length empty-map slice only;
broad P18-V01 API/settings/source/input/error/control-flow and reverse-export
reconciliation remains open.

## P18-V01 `MessageDecoder.decodeProperties(ByteBuffer)` cursor and truncation parity — 2026-10-22 (`2e1122cf`)

Pinned Java 5.5.1 advances the supplied buffer through the property bytes on a
positive length. Zero or negative property lengths return null with the
position just after the length field. A truncated positive-length value throws
`BufferUnderflowException` and leaves the position at the property start.
Rust's mapped decoder now returns both the decoded properties and consumed
byte count, preserving these observable cursor and truncation outcomes. The
invoked Java fixture covers ordinary records, zero declared store size,
positive malformed text, nonpositive lengths, and truncated property bytes.
The refreshed `fixtures/messages/records.json` SHA-256 is
`699dfaae816b753105119193e3b268fe16fb2b83ad0199473a5665a7d6b76436`.

This closes the tested `decodeProperties` cursor and truncation slice only;
broad P18-V01 API/settings/source/input/error/control-flow and reverse-export
reconciliation remains open.

## P18-V01 `MessageDecoder.decode(ByteBuffer)` declared-size handling — 2026-10-22 (`8acbf137`)

Pinned Java reads and stores the declared total-size integer but does not use it
to bound or validate the following fields. Rust now retains that integer on
`MessageExt`, parses through the body/topic/property lengths, returns the
actual bytes consumed, and applies its configured record-size limit to those
parsed bytes. Invoked Java cases with declared sizes `0`, `-1`, and
`Integer.MAX_VALUE` all decode the same message and leave the buffer after its
properties. Rust also decodes concatenated records with malformed declared
sizes using the parsed-field boundaries. Fixture SHA-256:
`41ed0208cdf79da2491554ed08683e4f18b40d6bce1d21d246771dd58f0e15ae`.

This closes the tested declared-size and concatenated-boundary behavior only;
broad P18-V01 API/settings/source/input/error/control-flow and reverse-export
reconciliation remains open.

## P18-V01 full `MessageDecoder.decode(ByteBuffer)` nonpositive field lengths — 2026-10-22 (`8ca83fc8`)

Pinned Java consumes body bytes only when the declared body length is positive,
and consumes property bytes only when their signed-short length is positive.
Zero and negative values therefore produce an absent body or property map and
leave the cursor immediately after the length field. Rust now follows those
branches; positive lengths retain the existing bounded field parsing. Invoked
Java fixtures cover a negative body length with the body omitted and a negative
property length with property bytes still following the length field. Their
decoded values and final positions match Rust. Fixture SHA-256:
`9cd0b64fe95643177c533d6cc2fd1f33c957e54cd3bd9559ecb07f6083939938`.

This closes the tested nonpositive body/property length behavior only; broad
P18-V01 API/settings/source/input/error/control-flow and reverse-export
reconciliation remains open.

## P18-V01 `MessageDecoder` method-surface crosswalk — 2026-10-22

The pinned Java 5.5.1 `MessageDecoder` declares 26 public method signatures and
one public no-argument constructor. Rust represents this stateless Java helper
as Common module functions and types. The method groups map as follows:

| Pinned Java methods | Rust Common mapping | Direct evidence |
| --- | --- | --- |
| `encode(MessageExt,boolean)`, `encodeUniquely(MessageExt,boolean)` | `encode_message_ext`, `encode_message_ext_uniquely` | MessageExt encoder fixture covers V1/V2 hosts, standard/unique output, compression, padding and short store size. |
| `decode(ByteBuffer,...)`, `clientDecode(ByteBuffer,boolean)` | `decode_one`, `decode_one_as_client`, `client_decode`, `DecodeOptions` | Invoked overload snapshots cover body, decompression, properties-string, CRC and client-ID choices; malformed store-size and nonpositive-length branches are recorded above. |
| `decodes`, `decodesBatch` | `decode_many_compatible`, `decode_many_as_client_compatible`, `client_decodes`, `client_decodes_with_body` | Concatenated fixtures compare per-record data and consumed boundaries. Invoked malformed-tail fixtures cover the no-argument and `readBody` overloads of `decodes`, plus `decodesBatch` with both client modes and both `readBody` settings; public Rust batch paths return the valid prefix like Java. |
| `encodeMessage`, `encodeMessages`, `decodeMessage(ByteBuffer)`, `decodeMessages(ByteBuffer)`, `decodeMessage(MessageExt,List)` | `encode_compact_message(s)`, `decode_compact_message(s)`, `decode_inner_batch` | Compact API fixture covers single/batch bytes, decode results, batch expansion and malformed inputs. `decode_inner_batch` marks each expanded `MessageExt` as Java's runtime `MessageClientExt`, preserving virtual message-ID lookup, no-op `setMsgId`, offset-ID fallback and `UNIQ_KEY` precedence. The invoked fixture compares these inherited/subtype outcomes. |
| `messageProperties2String`, `string2messageProperties`, `decodeProperties(ByteBuffer)` | `encode_properties_nullable`, `encode_nullable_property_entries`, `decode_properties_nullable`, `decode_record_properties` | Direct fixture covers null/empty/malformed property text, record offsets, cursor positions and truncation. |
| `createMessageId(SocketAddress,...)`, `decodeMessageId(String)` | `create_message_id`, `decode_message_id` | Message ID API fixture compares IPv4/IPv6 output, setters, malformed hex, short values and ports. |
| `createMessageId(ByteBuffer,ByteBuffer,...)` | `create_message_id_from_buffers(&mut Cursor<&mut [u8]>, &mut Cursor<&[u8]>, ...)` | Rust cursors preserve the caller-visible buffer positions. The output cursor resets to zero (Java `flip`), the address cursor copies from its current position through its limit, and the output cursor advances through the address and eight-byte offset. The pinned fixture compares IPv4/IPv6 IDs, positioned buffers, short addresses, output-capacity failure, address-put overflow, offset-put overflow, and both cursor positions on success/failure. The immutable host-byte helper has been removed from the current public mapping. |
| `createCrc32(ByteBuffer,int)`, `createCrc32(ByteBuf,int)` | `append_crc32_property` | Fixture compares both Java buffer overloads' emitted bytes. |
| `countInnerMsgNum(ByteBuffer)` | `count_inner_messages(&mut Cursor<&[u8]>)` | Rust advances the caller cursor through valid records to the limit. The invoked fixture covers a nonzero starting position, an incomplete declared record (cursor remains after `getInt`), and a truncated size prefix (cursor is unchanged). Positive sizes below four follow Java's cursor movement and subsequent parsing. Finite negative sizes follow Java's in-range rewind; the fixture compares an offset-8 to offset-5 rewind before failure. Zero-size and in-range negative-cycle inputs keep both Java and Rust calls running; bounded child-process probes confirm neither call returns. |

The no-argument Java constructor has no state or instance methods; Rust exposes
these operations directly as module functions. The fixture records the complete
26-signature Java surface in `fixtures/messages/records.json`. This crosswalk
accounts for the callable surface but does not close P18-V01 behavior, error,
control-flow, settings, or reverse-export reconciliation.

## P18-V01 Java `decodesBatch` malformed-tail behavior — 2026-10-22 (`4e718e35`)

Pinned Java `MessageDecoder.decode(ByteBuffer,...)` catches record decode
exceptions, advances its input to the limit, and returns null. Its
`decodesBatch` loop then stops and returns the messages decoded before that
record. The new invoked fixture encodes a valid `MessageExt`, appends one
malformed byte, and records the one-message prefix, runtime class and IDs, and
the final Java buffer position/limit. The refreshed
`fixtures/messages/records.json` SHA-256 is
`d96af9e17c6a06adb1519eefcb66b546cea5c0e6bda52b3233a6b2fe5440ee6c`.

Rust keeps strict `decode_many` error reporting and adds
`decode_many_as_client_compatible` for Java batch flows. Pull result, POP reply,
and both admin query paths now retain the valid prefix on a malformed tail.
Pull processing reapplies IDs after compact-batch expansion so expanded
messages preserve their `UNIQ_KEY` behavior and physical offset IDs. `view_message`
continues using the one-record decoder. Common, pull, POP, and admin regressions
cover these outcomes.

Default and all-feature workspace tests, strict all-target/all-feature Clippy,
format and diff checks, and pinned Maven verify pass. Maven verifies 37
scenarios, 36 route/VIP cases, 106 typed headers, 19 typed bodies, and all 11
message decode fixtures. This closes the batch malformed-tail control-flow
slice only; broad P18-V01 reconciliation, current-revision remote Ubuntu CI,
native Windows runtime, and Phase 19 Broker/Docker evidence remain open.

## P18-V01 `MessageDecoder.decodes(ByteBuffer)` default overload — 2026-10-22 (`2c1cd60a`)

The malformed-tail oracle now directly invokes Java's no-argument
`decodes(ByteBuffer)` overload. It records the one-message client prefix,
`MessageClientExt` runtime class, and final buffer position/limit; Common tests
compare that result with Rust `client_decodes`, the corresponding default
body-reading helper. Fixture SHA-256:
`46a18677fc673222639e53ee2871f4836fa427f97d284f918c1bdeb57129beb0`.

The Common crate test suite, Common all-target/all-feature strict Clippy,
format/diff checks, and pinned Maven verify pass. This closes the no-argument
`decodes` malformed-tail overload slice only; broad P18-V01 API/settings/source/
input/error/control-flow/reverse-export reconciliation, current-revision
remote Ubuntu CI, native Windows runtime, and Phase 19 Broker/Docker evidence
remain open.

## P18-V01 Java `decodes` and both `decodesBatch` modes — 2026-10-22 (`2c1f0a73`)

The direct Java `MessageDecoder.decodes(ByteBuffer, readBody)` overload uses
the same null-stop loop as `decodesBatch`. The invoked malformed-tail fixture
now calls `decodes` and `decodesBatch` with `isClient=true` and `isClient=false`,
compares each one-message prefix, records `MessageClientExt` versus
`MessageExt`, and confirms Java advances every input buffer to its limit. The
refreshed `fixtures/messages/records.json` SHA-256 is
`2be37d4f72d8e088a7bc52eb77de5d36192d48ba17de849619c02da31d6c2b97`.

Rust adds `decode_many_compatible` for Java's non-client batch mode and routes
`client_decodes` and `client_decodes_with_body` through the client-compatible
prefix path. The strict `decode_many` and `decode_many_as_client` APIs still
report malformed records. Common regressions compare the direct client and
generic paths, subtype, body option, and strict errors. This closes the direct
`decodes` and generic `decodesBatch` malformed-tail slice only; broad P18-V01
API/settings/source/input/error/control-flow/reverse-export reconciliation,
current-revision remote Ubuntu CI, native Windows runtime, and Phase 19
Broker/Docker evidence remain open.

## P18-V01 `MQClientAPIImpl.sendSmartMsg` setting and reply request codes — 2026-10-23

Pinned Java 5.5.1 initializes private static `sendSmartMsg` from
`org.apache.rocketmq.client.sendSmartMsg` when the API implementation class is
initialized. The default is `true`; Java `Boolean.parseBoolean` treats only
case-insensitive `true` as true. Ordinary messages use V2 when enabled and V1
when disabled. A message whose `MSG_TYPE` is exactly `reply` uses the matching
reply request code; batch messages use `SEND_BATCH_MESSAGE`, unless the reply
marker selects the reply branch first.

Rust reads the same property name from the process environment and captures it
when `MQClientApi` is constructed. The hidden cross-crate selector
`MQClientApi::send_kind_for_message` applies it in the Producer owner.
`Producer::prepare` records the exact reply marker; the send path selects
V1/V2, ReplyV1/ReplyV2, or Batch with Java's branch order. The selector adds no
user-facing Rust setting.

Pinned Java `compat.InvokedRpcOracle` invokes normal, reply, and batch sends
with the private switch toggled both ways. Rust's isolated Core property test
covers unset, false, uppercase true, whitespace, and malformed property
values; the Producer isolated-process regression checks normal, reply, and
batch selection with the property both false and true. This closes the named
system-setting and Producer request-code selection slice only; broad P18-V01
source/API/settings/input/error/control-flow and reverse-export reconciliation
remains PARTIAL.

The same pinned-source property scan found `java.io.tmpdir` only in
`remoting/.../TopicQueueMappingUtils.writeToTemp`. The pinned client has no
call site for that remapping snapshot helper; it is not a client configuration
or a client operation.

## P18-V01 `LocalFileOffsetStore.LOCAL_OFFSET_STORE_DIR` public field — 2026-10-23 (`6d0f4523`)

Pinned Java 5.5.1 declares `LOCAL_OFFSET_STORE_DIR` as a public static final
field and snapshots `rocketmq.client.localOffsetStoreDir` or the
`user.home/.rocketmq_offsets` fallback when the class initializes. Rust now
exposes `LocalFileOffsetStore::local_offset_store_dir()` and uses that same
first-access snapshot when constructing the store. This closes the missing
public-field mapping and the prior per-construction environment reread. The
invoked Java fixture verifies the value before and after changing `user.home`;
the isolated Rust subprocess regression checks that value and the actual path
for explicit, empty, home-fallback, empty-home, and missing-home cases.

Validation passed: the focused Rust subprocess test, Consumer all-features
package tests, strict Consumer Clippy, focused Java runtime-offset fixture
verify, and full offline Maven `verify` (exit 0). This closes only this
field/property slice; broad P18-V01 remains partial.

## P18-V01 Java client process-property census — 2026-10-23 (`ed909c11`)

The pinned Java source scan used `git grep` for `System.getProperty`,
`System.getenv`, `Boolean.getBoolean`, `Integer.getInteger`, `Long.getLong`,
and `System.setProperty` at client commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`, across `client`, `common`, and
`remoting`. Rust production environment reads were reverse-scanned in the
Common, Protocol, Remoting, Core, Producer, and Consumer crates.

| Java setting owner | Rust owner / existing source-backed evidence |
| --- | --- |
| `ClientConfig` properties and NameServer address/domain inputs | Core `ClientConfig`, `NameServerAddressUtils`, and `NameServerAddressProvider`; the complete 33-field/property and initialization-timing inventory is above. |
| `SessionCredentials.KEY_FILE` | Core `SessionCredentials::key_file_path` and ACL credential/file fixtures; [Phase 16 security matrix](PHASE_16_NAMESPACE_SECURITY_OPERATIONS_MATRIX.md). |
| `LocalFileOffsetStore.LOCAL_OFFSET_STORE_DIR` | Consumer `LocalFileOffsetStore::local_offset_store_dir()` exposes the Java public static field value and shares its one-time snapshot with store construction; the default home-directory fallback, property override, path layout, and restart persistence are recorded in the [Phase 9 matrix](PHASE_9_OFFLINE_METHOD_MATRIX.md). |
| `ProcessQueue`, `PopProcessQueue`, `ConsumeMessageOrderlyService`, and `RebalancePushImpl` timing properties | Consumer queue/orderly owners; separate class-initialization caches and exact defaults are recorded in the [Phase 12 matrix](PHASE_12_ORDERLY_MATRIX.md) and [runtime consumer operations](PHASE_18_RUNTIME_CONSUMER_OPERATIONS.md). |
| `MQClientAPIImpl.sendSmartMsg` and `RemotingCommand.REMOTING_VERSION_KEY` | Core API initialization and Producer send-kind selection; the invoked oracle and subprocess tests are recorded in the current P18-V01 audit entries. |
| `DefaultMQProducer` compression type/level and recall-trace enable | Producer config and trace hook; constructor property parsing and source selection are recorded in the [Phase 7 producer matrix](PHASE_7_PRODUCER_MATRIX.md) and [Phase 16 operations matrix](PHASE_16_NAMESPACE_SECURITY_OPERATIONS_MATRIX.md). |
| `NettyClientConfig`, `NettySystemConfig`, `TlsSystemConfig`, and `RemotingCommand` serialization type | Remoting config, TLS property state, and serialization-type initialization; see the [Phase 3 Remoting inventory](PHASE_3_REMOTING_INVENTORY.md). |
| `DynamicalExtFieldRPCHook` zone properties/environment fallbacks | Core dynamic RPC hook; values are read at each hook invocation, as recorded in the Phase 3 inventory. |
| `MQClientAPIFactory.init` NameServer/VIP properties | The direct Java-public pool API is mapped to Core `MQClientApiFactory`; its owned `ClientConfig` disables VIP and carries the configured NameServer address, while domain settings feed the `TopAddressing` provider. Domain lookup and refresh results update only the corresponding client slot, matching each Java API instance's own current address; Rust creates one cancellable refresh task per slot, matching Java's per-client 10-second initial delay and 120-second fixed rate. Provider change callbacks register once per active slot and update only that slot. The factory's explicit address-change API visits the public mutable client array in index order, updates earlier slots, then fails at the first null slot; a null address follows the same iteration and failure order. Shutdown follows pinned `AsyncShutdownHelper`: it attempts every array entry concurrently, a null slot's task NPE is swallowed while non-null clients still shut down, and a started zero-length array fails in `await` because the helper never creates its latch; Rust skips null slots and maps the empty-array failure to `EmptyClientArray`. Java `getClient()` on a started zero-length pool fails at `ThreadLocalRandom.nextInt(0)` with `IllegalArgumentException`; Rust returns the same typed `EmptyClientArray` failure for that condition. The Java fixture explicitly closes the removed client after recording that factory shutdown cannot reach it. The Java class also writes these values to JVM-wide properties. Rust applies those writes to a shared hidden in-process overlay in Java source order; later `ClientConfig` instances and Common NameServer lookup observe the values and failure side effects, while the operating-system environment remains unchanged. The Common `utils::system_properties` module is a public hidden cross-crate implementation bridge and is not re-exported by the `rocketmq-classic` facade. Java exposes remoting-client injection through its seven-argument constructor's `ObjectCreator<RemotingClient>` parameter; Rust maps that overload to `MQClientApiFactory::with_remoting_client_creator`. Both Java constructors accept a `ScheduledExecutorService`; Rust exposes that option through `FactoryRefreshScheduler` and `with_refresh_scheduler`, which registers one callback per client at the same 10-second initial delay and 120-second fixed rate. The scheduler remains caller-owned; the default constructor uses Tokio workers. `with_optional_refresh_scheduler(None)` represents Java null: static-address startup ignores it, while domain startup raises `NullRefreshScheduler` after callback registration and initial discovery, before client start and slot assignment. Rejected schedule registration is returned as `RefreshScheduleFailed` at that same pre-start boundary, preserving registration order. The separate `TopAddressing` provider seam is Rust-only, private to unit tests, and guarded by a compile-fail Rustdoc example. The pinned standard manager still constructs one `MQClientAPIImpl` per `MQClientInstance`; the separate factory surface is available for direct callers. |

The factory's Rust-only nullable adapters also record Java's null input
ordering: a null `NameserverAccessConfig` first writes VIP=false and then maps
the thrown `NullPointerException` to `NullNameServerAccessConfig`; a null
`namePrefix` forms the instance name `nullN_0`. The nullable seven-argument
adapter forwards a supplied remoting creator and uses the standard transport
when its creator is null. The invoked factory fixture and Rust regressions
cover these observations. The remaining common-module reads
are server/configuration utilities, JVM or
OS implementation selection, or the unused `TopicQueueMappingUtils` temp-file
helper; they do not add a client setting or an active client operation. The
reverse Rust scan found no unexplained production environment-backed public
setting. The earlier P18 scope review excluded `MQClientAPIFactory` based on
its lack of a standard client callsite; the [Phase 6 runtime inventory](PHASE_6_RUNTIME_INVENTORY.md)
records the correction and current Rust mapping. This census closes the
process-property slice only; broad P18-V01 remains PARTIAL.

## P18-V01 `MessageDecoder` body-skip malformed-tail coverage — 2026-10-22 (`608fe801`)

The invoked malformed-tail fixture now also calls Java `decodes(..., false)`
and `decodesBatch(..., readBody=false, isClient=true/false)`. It compares their
prefix counts with Rust `client_decodes_with_body` and
`decode_many_compatible` using `read_body=false`, and asserts Java leaves each
buffer at its limit. Rust regressions confirm the client subtype is retained
and the body is absent from the decoded value; Rust's byte-slice APIs expose no
mutable cursor.
The refreshed `fixtures/messages/records.json` SHA-256 is
`039d8dba7a268b43bc7e171f4f84bd739a0a3695c357e4ef7be35ded29c71a53`.

The focused Common regression, default and all-feature workspace tests,
strict all-target/all-feature Clippy, formatting/diff checks, and pinned Maven
verify pass. This closes the body-skip dimension of the malformed-tail
decoder slice only; broad P18-V01 API/settings/source/input/error/control-flow/
reverse-export reconciliation, current-revision remote Ubuntu CI, native
Windows runtime, and Phase 19 Broker/Docker evidence remain open.

## P18-V01 strict batch helper reverse-export correction — 2026-10-22 (`95525c06`)

Reverse API review found public Rust `decode_many` and
`decode_many_as_client` functions with strict malformed-record errors, while
pinned Java exposes only prefix-preserving `decodes` and `decodesBatch` batch
methods. The strict helpers were not used by production owners. They have been
removed from Common's public exports; Core, Remoting fuzz, and Common tests
now use the Java-compatible batch APIs. Strict failure details remain private
inside the Common decoder for safety-limit accounting. Generated Common
Rustdoc confirms the strict names are absent from the public API.

Default and all-feature workspace tests, strict workspace Clippy,
format/diff checks, Common Rustdoc, differential self-test and 37-scenario
manifest validation, fuzz smoke, and pinned Maven verify pass. This closes
this Rust-only public decoder surface mismatch only; broad P18-V01
API/settings/source/input/error/control-flow/reverse-export reconciliation,
current-revision remote Ubuntu CI, native Windows runtime, and Phase 19
Broker/Docker evidence remain open.

## P18-V01 `MQClientAPIFactory` null-slot failure ordering — 2026-10-23 (`1b1d4e2a`)

Pinned Java's explicit NameServer update traverses the mutable client array
sequentially. A null entry throws after preserving updates to lower indexes;
passing a null address still invokes each client and reaches the same null
entry. Rust mirrors this order and returns `NullClient(index)`. The invoked
Java fixture records both partial states and error names, and the Core
factory regression compares those values before restoring the client slot.
The full local server-free gate passed on this change. This closes this
failure-order mapping only; broad P18-V01 reconciliation remains partial.

## P18-V01 Common topic and Producer topic-list Rust names — 2026-10-23

| Rust public names | Pinned Java counterpart and adaptation |
| --- | --- |
| Common: `TOPIC_MESSAGE_TYPE_ATTRIBUTE`, `LITE_EXPIRATION_ATTRIBUTE`, `TopicMessageType::topic_message_type_set`, `parse_from_message_property`, and `metrics_value` | `TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE` / `LITE_EXPIRATION_ATTRIBUTE` and `TopicMessageType.topicMessageTypeSet`, `parseFromMessageProperty`, and `getMetricsValue`. The `TopicConfig`/`TopicMessageType` row above records the reflected fixtures and behavior coverage. |
| Producer: `ProducerTopicList::{new,snapshot,len,is_empty,contains,push,insert,set,remove,clear,with_read,with_mut}` | `DefaultMQProducer.getTopics()` and `setTopics(List<String>)` retain and return the same live list. `ProducerTopicList` is the shared Rust handle; its list operations map ordinary `List` reads and mutations, `with_read` / `with_mut` provide lock-scoped access to the aliased collection, and `snapshot` returns an owned point-in-time copy. They do not add a Producer operation or setting. |

## P18-V01 `MQClientAPIFactory` protected-hook scope — 2026-10-23

Pinned Java also declares protected `init()` and `createAndStart(String)`
subclass hooks. The base `init` constructor call maps to Rust factory `build`;
per-slot base creation maps to `start`. The pinned client source/tests contain
no subclass of `MQClientAPIFactory`, and `createAndStart` is called only by
the base `start` loop. These protected overrides are not Java-public client
methods or settings, and Rust has no Java-style subclass dispatch. Public
construction and lifecycle behavior plus its remoting-creator and scheduler
dependencies remain mapped. Third-party Java subclass source compatibility
is outside this public API parity scope; see the runtime inventory for the
source scan and full disposition.

## P18-V01 `MQClientAPIFactory` refresh-registration failure ordering — 2026-10-23

Pinned `createAndStart` constructs the API client, performs initial discovery,
registers that client's fixed-rate refresh, then starts it; the factory stores
the returned client in its array only after the method succeeds. Rust now uses
the same per-slot sequence. If registration fails, earlier completed slots
remain started and the failing and later slots remain null. The invoked Java
fixture records a `RejectedExecutionException`, one registration attempt, and
the null first slot; the Core regression compares the same slot state and
checks the Rust creator ran before registration. Refresh callbacks retain the
corresponding remoting client, matching Java's per-instance callback target.


## P18-V01 `MQClientAPIFactory` null scheduler behavior — 2026-10-23

Java's constructors permit a null `ScheduledExecutorService`. Static-address
startup never uses it and succeeds; domain startup constructs the API client,
registers its provider callback, performs the initial lookup, then throws
`NullPointerException` when it schedules refresh, leaving that client-array
slot null. Rust exposes `with_optional_refresh_scheduler(None)` to represent
the input; its domain branch returns `NullRefreshScheduler` at the same point,
while static mode proceeds normally. The invoked factory fixture covers both
modes and the Core test compares the exception and null slot.

## P18-V01 inherited `ServiceThread.makeStop()` wake behavior — 2026-10-23 (`4c74d38e`)

Pinned Java `ServiceThread.makeStop()` returns before mutation when the service
has not started; otherwise it sets `stopped` and calls `wakeup()` without
joining. Rust's `RuntimeServiceThread::make_stop()` now wakes the Rebalance
worker after setting the flag. PullMessageService retains the queue wait
behavior: its inherited `LockSupport.unpark` does not release a blocking queue
take. A direct Java probe blocks in protected `waitForRunning(60_000)` and
confirms `makeStop()` releases it promptly; the Rust regression waits for the
owned Rebalance worker to exit after the public handle call. The refreshed
`fixtures/runtime/client-config.json` SHA-256 is
`aff5aa095941819f7cb3a1f419a66000e5b83177f7badd0d4625b85552e53387`.
Core all-feature tests (247 unit tests, all integration targets, 21 doctests),
strict Core Clippy, formatting/diff checks, and offline Maven verification
pass. This closes the `makeStop()` wake branch only; broad P18-V01
reconciliation remains open.

## P18-V01 `ServiceThread.shutdown()` overload mapping — 2026-10-23 (`6e994989`)

Pinned Java declares both `shutdown()` and `shutdown(boolean)`; the former
delegates to the latter with `false`. Rust now exposes `RuntimeServiceThread::shutdown()`
for the no-argument form and `shutdown_with_interrupt(bool)` for the explicit
overload. This preserves PullMessageService's dynamic shutdown override, which
also closes its scheduled executor when invoked before start. The existing
Java fixture invokes the no-argument method on unstarted workers, and Rust
regressions compare pre-start state and Pull scheduler closure. Core
all-feature tests (247 unit tests, all integration targets, 21 doctests),
strict Core Clippy, all-feature workspace Rustdoc JSON, formatting/diff
checks, and offline Maven verification pass. This closes the overload mapping
only; broad P18-V01 reconciliation remains open.

## P18-V01 `TopicConfig.decode(null)` parity — 2026-10-23

The invoked Java fixture now calls `TopicConfig.decode(null)` directly and
records `NullPointerException`, confirming the argument dereference occurs
before any decode mutation. Rust's `TopicConfig::decode_nullable(None)` returns
`TopicConfigError::NullInput`; the Protocol regression compares these explicit
Java and Rust outcomes. `fixtures/producer/core.json` SHA-256:
`f1a221f9c6c572de934a7c84e7e31ed4015f326ac3e78bdeb7f79cf27cffa10a`.
The focused Protocol test, all-feature Protocol suite (including 166
doctests), strict Protocol Clippy, formatting/diff checks, and offline Maven
verification pass. This closes this null-input branch only; broad P18-V01
reconciliation remains open.

## P18-V01 `TopicConfig.attributes` nullable map keys — 2026-10-23

The pinned Java setter retains a `Map` with a null key, and its getter returns
the same object. The direct fixture distinguishes a null key from the ordinary
string key `"null"`, records mutations through both references, and captures
`encode()`, `toString()`, and decode behavior. Fastjson emits both keys as
duplicate `"null"` JSON properties, omits null-valued map entries from JSON,
and prints those entries in `toString()`; decoding a JSON property named
`"null"` creates an ordinary string key. Rust's `TopicConfigAttributeMap` now
uses `Option<String>` keys, preserves Java HashMap bucket order, and keeps the
decoded string-key behavior. The focused `topic_config_public_api.rs` regression
compares the Java oracle and raw serialized properties. Fixture SHA-256:
`d166aa9c873ed7f2ccf26a25dc5d43850e10b7eabcc15a62535c9d265fd9b237`.

## P18-V01 current-tree Rustdoc export counts — 2026-10-23 (`98457158`)

Regenerated all-feature Rustdoc JSON after the response-header fixture
expansion. Distinct public item names, excluding each crate-root marker, are
Common 439, Consumer 460, Core 747, Producer 361, Protocol 1,524, and Remoting
337. The Protocol change was test/fixture-only and did not add public names.
This confirms the current export counts; counts do not establish behavior
coverage. Broad P18-V01 source/API/settings/input/error/control-flow and
reverse-export reconciliation remains open.

## P18-V01 `MessageQueueAssignment` nullable attachment keys — 2026-10-23 (`5dffd30b`)

The pinned Java `setAttachments(Map<String, String>)` retains a map containing
a null key, and `getAttachments()` returns that same object. The invoked
`assignmentNullAttachmentApiCase` records the null key separately from the
literal string key `"null"`, setter/getter mutation visibility, and Fastjson
output. Fastjson writes a null key as the JSON property name `"null"`; when the
literal key is also present, both entries are emitted with that property name.
Rust `AssignmentAttachments` now represents keys as `Option<String>` inside the
shared handle, preserving caller/getter aliasing. Its DTO encoder writes
`None` as `"null"`, keeps decoded JSON property `"null"` as a string key, and
omits null-valued entries while snapshots retain them. Fixture SHA-256
`42af689aed0d150ee19fef5d8138627bb09db9085139b5221d579e57a0053283`.
The Protocol regression parses the encoded value and also checks raw JSON text
contains both duplicate `"null"` properties (test follow-up `1b8c0d7d`).

Full serial offline workspace tests, strict all-target/all-feature Clippy,
Protocol/Core tests, pinned offline Maven `verify`, formatting, and diff checks
pass. This closes the nullable attachment-key API/input/encoding gap only;
broad P18-V01 reconciliation remains open, Phase 6 remains PARTIAL, and
P6-V04 remains `PENDING_PHASE_19`.

## P18-V01 cross-crate internal-producer helper exports — 2026-10-23 (`325c77fe`)

| Rust public helper | Pinned Java owner and disposition |
| --- | --- |
| `ClientRuntime::get_or_insert_extension<T>` (`#[doc(hidden)]`) | Cross-crate implementation storage for one strongly retained typed extension per `ClientRuntime`. The only in-tree extension is the runtime's `CLIENT_INNER_PRODUCER`, matching the per-`MQClientInstance` owner identity returned by `MQClientInstance.getDefaultMQProducer()`. This is an ownership adapter, not another client setting or producer operation. |
| `Producer::send_internal_with_runtime` (`#[doc(hidden)]`) | Cross-crate entry to the Producer send loop using an already-owned `ClientRuntime`, without registering a separate user producer. It maps the internal retry-topic send through Java's `CLIENT_INNER_PRODUCER` and `DefaultMQProducerImpl.sendDefaultImpl` path. |

Both helpers must be Rust `pub` for the separate Core, Producer, and Consumer
crates to call each other. They are hidden from generated public Rustdoc.
`ClientRuntime` is not re-exported by the high-level facade; the Producer helper
is callable on the facade's re-exported `Producer` type, but its Rustdoc-hidden
name marks it as an internal cross-crate adapter. The Core runtime-extension
identity test, Consumer shared-owner/runtime-change test, and Producer
fault-selector test cover their only in-tree use. Broad P18-V01 public
API/settings/input/error/control-flow and reverse-export reconciliation
remains open.

## P18-V01 Cargo feature-flag reverse scan — 2026-10-23 (`ea17b8fc`)

Offline Cargo metadata reports an empty `features` map for all eight workspace
packages: Common, Consumer, Core, Protocol, Remoting, Producer, the facade, and
the integration runner. No workspace-defined Cargo feature or default/optional
feature branch adds a Rust-only client capability; `--all-features` does not
select an additional workspace configuration. This closes the Cargo
feature-flag inventory only. Broad P18-V01 settings, public APIs, inputs,
errors, control flow, and reverse-export reconciliation remain open.

## P18-V01 `doc(hidden)` Rust API reverse scan — 2026-10-23 (`df5c3c59`)

A source scan of library crates found 29 `#[doc(hidden)]` annotations across
15 Rust source files. Hidden documentation does not make a Rust `pub` item
inaccessible to callers. The following crosswalk records the Java behavior
behind these implementation-facing public items; this is a narrow surface
inventory and does not close the broader source/input/error/control-flow
audit.

| Hidden Rust item family | Pinned Java counterpart and disposition |
| --- | --- |
| Common `utils::system_properties::{get_property,apply_mq_client_api_factory_properties}` | Java `System.getProperty`/`System.setProperty` effects from `MQClientAPIFactory.init` and later Common/ClientConfig reads. Rust's retained in-process overlay carries those source-ordered side effects; it is not an additional user configuration store. |
| `ClientConfig::to_java_properties`; `ClientRuntimeManager::{shared_default,get_or_create_with_hook_and_tls_config}` | `MixAll.object2Properties`, Java's shared `MQClientManager` owner, and the mapped TLS/config values passed when creating that owner. These are cross-crate/reflection adapters, not separate Java settings or owner controls. |
| `RequestFutureTable::{expire_due,expire_due_for_current_runtime}`; `PendingReplyControl::{mark_send_succeeded,fail_send,cancel,expire_if_due}`; `ClientRuntime::register_reply_request_with_future` | Java `RequestFutureHolder`'s request-future scan and `RequestResponseFuture` reply, timeout, send-failure, cancellation, and owner-registration paths. They carry existing request/reply completion behavior; `cancel` is internal cleanup, not another wire mode. The Phase 15 matrix records the lifecycle disposition. |
| `ClientRuntime::get_or_insert_extension<T>`; `Producer::send_internal_with_runtime` | The runtime-scoped `CLIENT_INNER_PRODUCER` from `MQClientInstance.getDefaultMQProducer()` and its `DefaultMQProducerImpl.sendDefaultImpl` path. These cross-crate owner adapters are mapped separately above; they add no user producer setting or send mode. |
| `MQClientApi::{send_kind_for_message,probe_broker}` | Java `MQClientAPIImpl.sendSmartMsg` request-code selection and `DefaultMQProducerImpl`'s `MQFaultStrategy` detector probe through `getMaxOffset`. These hidden Core entry points preserve Producer behavior rather than add RPCs. |
| `CommandHeader::decode_fields_with_mode`; `RemotingCommand::{initialize_client_api_version,custom_header,cached_header,process_timer,callback_list}`; `CustomHeaderValue` | Java `decodeCommandCustomHeaderDirectly(useFastEncode)`, `MQClientAPIImpl`'s cached remoting-version initialization, and `RemotingCommand` typed-header/timer/callback state exposed through its mapped methods. `CustomHeaderValue` is a type-erased storage carrier, not a separate Java bean. The fast/reflection and command-state rows above record the behavior and adapter limits. |
| `RemotingClient::invoke_sync_with_timeout_millis` | Java `NettyRemotingClient.invokeSync` with its signed millisecond timeout and channel-acquisition-before-expiry ordering. This is the low-level adapter used by client API calls; it adds no transport operation beyond the Java sync call. |
| Hidden `unique_id` module: `UniqueIdError`, `UniqueIdGenerator::{new,set_if_missing,create}`, `{get_ip_from_id,get_ip_str_from_id,get_pid_from_id,get_nearly_time_from_id,create_fake_ip}`; `TraceDispatcher::new`; `TraceContext::transfer_keys` | Java `MessageClientIDSetter` constructor, unique-ID creation/set/decode helpers, and fake-IP behavior; Java `AsyncTraceDispatcher` owner construction; and trace encoder transfer-key selection. Their public Java declarations or owner-flow dispositions are recorded in the Phase 7 and Phase 16 matrices. |

## P18-V01 inbound remoting failure-flow comparison — 2026-10-23

The pinned 5.5.1 `NettyRemotingAbstract.processRequestCommand` path was
invoked with a local `EmbeddedChannel`, direct executor, and real Java
`RPCHook`, `RequestPipeline`, and `NettyRequestProcessor` implementations.
`fixtures/runtime/hooks.json` now records normal failures from each stage,
`AbortProcessException` from before/pipeline/after, and a one-way processor
failure. The fixture pins response presence/code/opaque and the observed
before, pipeline, processor, and after callback sequence. The Rust
`p18_inbound_hook_pipeline_and_processor_failures_match_java_flow_fixture`
loopback test compares each of those outcomes. It confirms SYSTEM_ERROR for
ordinary stage failures, the specified abort response codes, Java's skip and
after-hook ordering, and no response for one-way failures. Exception remarks
are normalized to their Java exception class in this fixture; Rust's
error-message text and Java's exception-plus-stack-frame text remain a
diagnostic-only difference. This closes the previously vague processor,
pipeline, and hook response-flow concern, not the broader P18-V01 audit.

This pass reviewed each of the 29 hidden-documentation annotations, including
the public hidden fields and bridge methods. It found no additional client
setting or wire operation in this set. Rust `pub` accessibility remains part
of the API surface even where Rustdoc hides the name; the table records its
adapter role rather than waiving the broader P18-V01 reverse-export review.

Phase 18 JSON remoting header input follow-up (2026-10-24, `d897a9e2`): the
5.5.1 `RemotingCommand.headerDecode` path calls `RemotingSerializable.decode`
for JSON headers. Rust `json_header::decode` now uses the shared Fastjson
input layer. Invoked Java and Rust cases cover both raw U+001A EOF suffix forms
and single-quoted fields with a trailing comma. Fixture SHA-256:
`f0b5433e0c7c9f4566dee85048196860c47b948e4204cb08aa09b062bb628c0c`. This
closes these JSON-header syntax inputs only; broad P18-V01 remains open.

## P18 public nested `LatencyFaultToleranceImpl.FaultItem` API

Pinned Java 5.5.1 exposes `LatencyFaultToleranceImpl.FaultItem` with an inner
constructor and 15 public methods, including both the typed `compareTo(FaultItem)`
method and compiler-generated `compareTo(Object)` bridge. Producer exports the
Rust-native `FaultItem` and `FaultItemCompareError` from its crate and facade.
The constructor accepts a nullable name; methods map `updateNotAvailableDuration`,
`setReachable`, `setCheckStamp`, availability/reachability accessors, name and
latency/timestamp accessors, `equals(Object)`, `hashCode`, `toString`, and both
comparison paths. Atomic fields preserve Java's volatile visibility. Java's
implicit outer-strategy reference is omitted because none of the helper's
methods read it.

The invoked Java fixture and `latency_fault_item_public_surface_and_behavior_match_invoked_java`
cover constructor/method signatures, initial state, positive/zero/negative and
overflow duration updates, availability precedence, signed latency/timestamp
comparison, equality/hash field selection, `Comparable` null/cast errors,
nullable and supplementary-Unicode names, and text output. The facade test
checks the type re-export. Current `fixtures/producer/core.json` SHA-256:
`d6c23e4c1d213f02fd257abd0526b525d18f33167f1f26bda8510b75d189f398`. This
closes this nested value API row only; broad P18-V01 source/API/settings/input/
error/control-flow and reverse-export reconciliation remains open.
Code commit: `62f67cbb`. Execution record:
`../../rlogs/2026/10/24-phase-18-fault-item-public-api__20261024T153809Z.rlog`.

## P18-V01 pinned `MQClientAPIImpl` overload crosswalk — 2026-10-24 (`091eba55`)

The Java reference is source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9` (RocketMQ 5.5.1 release), not the
current worktree revision of the separate Java checkout. The pinned
`MQClientAPIImpl.java` SHA-256 is
`bfb0ebf8e4b4c0fd4ad5a4db46d64a7c8897250fb411487793a23f29aa094737`.
The exact source scan contains 132 public method declarations, 121 distinct
method names, and seven overloaded groups. This table accounts for every
member of those seven groups and links each member to a Rust adapter and
invocation evidence.

| Java 5.5.1 overload group and member | Rust mapping | Invoked evidence |
| --- | --- | --- |
| `sendMessage` short overload with context/producer; full overload with callback, publish info, client instance and retry count | `MQClientApi::send_message` expresses request kind/mode and one Broker attempt; Producer owns routing, retries and async callback completion. | `send-success.json` invokes the short overload; `send-full-overload.json` invokes the full synchronous overload. Existing invoked Producer/RPC callback fixtures cover async success/error callbacks and retry ownership. `java_invoked_cases_match_rust_requests_and_outcomes` compares request and outcome. |
| `batchAckMessageAsync` with `BatchAckMessageRequestBody`; overload with topic, group and `List<String>` receipt metadata | Core `MQClientApi::batch_ack_message` sends the body overload. Consumer `batch_ack_message_from_extra_info` groups the list overload into the Java body, including Java `HashMap` iteration, `BitSet` indexing and `BatchAckMessageRequestBody.encode()`. | `fixtures/rpc-invoked-pop-ack/batch-ack-0-normal.json` covers the body overload; `batch-ack-extra-info.json` covers receipt parsing and list grouping. `rpc_invoked_pop_ack_fixtures.rs` and `pop_batch_ack_invoked.rs` compare both paths. |
| `searchOffset(addr, topic, queueId, timestamp, timeout)`; `searchOffset(addr, MessageQueue, timestamp, timeout)`; `searchOffset(addr, MessageQueue, timestamp, boundaryType, timeout)` | `MQClientApi::search_offset` takes `SearchOffsetRequestHeader`; the caller supplies exactly the fields each overload populates. The no-boundary overload sets `LOWER`; the deprecated overload omits broker/boundary fields. | `search-offset-deprecated.json` and `search-offset-default-boundary.json` are direct invocations. The lower/upper explicit-boundary cases are in `fixtures/rpc-invoked-offset-boundaries/`; Core compares requests and signed results. |
| `getConsumeStats` with no topic filters; `List<String>`; topic; topic plus list | `MQClientApi::get_consume_stats_sync` takes `GetConsumeStatsRequestHeader`; absent, nullable and populated topic fields/list values stay distinct. The separate `MqClientAdmin` path remains future-based. | `consume-stats-default.json`, `consume-stats-list.json`, `consume-stats-topic.json`, and `consume-stats-topic-list.json` invoke all four Java methods. The direct RPC fixture test compares fields, sync mode and decoded result; `admin_rpc.rs` covers the separate admin future. |
| `getTopicRouteInfoFromNameServer(topic, timeout)` and overload with `allowTopicNotExist` | `MQClientApi::get_topic_route` maps the same request and response. The boolean affects only the Java warning emitted for `TOPIC_NOT_EXIST`; both overloads return the same client error. | `route-not-found.json` invokes the default overload and `route-not-found-explicit-false.json` invokes the explicit overload; route success/error cases also compare request and outcome. |
| `deleteTopicInNameServer(addr, topic, timeout)` and overload with `clusterName` | `MQClientApi::delete_topic_in_nameserver` takes `DeleteTopicFromNamesrvRequestHeader`, carrying the optional cluster field explicitly. | `delete-topic-nameserver.json` and `delete-topic-nameserver-cluster.json` invoke both overloads and compare request fields and success. |
| `invokeBrokerToResetOffset` default overload; overload with explicit `queueId`/`offset`; overload with `isC` | `invoke_broker_to_reset_offset_for_client` maps the default `queueId = -1`, `offset = -1` and optional CPP language; `invoke_broker_to_reset_offset_at_offset` maps the explicit fields. The lower-level header API remains available for `MqClientAdmin`. | `reset-offset-default.json`, `reset-offset-explicit.json`, and `reset-offset-cpp.json` invoke all three members and compare language, header, response body and outcome. |

Code commit: `091eba55`. The direct Rust invocation test passes all 29
`fixtures/rpc-invoked` cases. Pinned Maven `verify` passes fixture-drift
validation, all 37 manifest scenarios and Java/Rust cross-decodes. This closes
the `MQClientAPIImpl` overload inventory/evidence slice; it does not by itself
close the broader P18-V01 source, settings, input, error, control-flow and
reverse-export reconciliation.
