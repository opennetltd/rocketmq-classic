# ADR-0040: Match Java Lite subscription collection inputs

Status: Accepted

## Context

Pinned Java 5.5.1 declarations show `LiteSubscriptionCtlRequestBody.subscriptionSet` is a `Set<LiteSubscriptionDTO>`, and both `LiteSubscriptionDTO.liteTopicSet` and `LiteSubscription.liteTopicSet` are `Set<String>`. `LiteSubscriptionDTO` has no `equals`/`hashCode` override, so the outer Java set retains separate DTO objects with equal field values but deduplicates repeated null members. Hash-set iteration order is unspecified. `LiteSubscription.liteTopicSet` is initialized with `ConcurrentHashMap.newKeySet()`, which rejects null topics; its `setLiteTopicSet` method adds values to the existing set.

Fastjson accepts an object for either set field as a singleton set. A string becomes a singleton for `liteTopicSet`; the empty string becomes an empty set. Array members use Java String coercion, and each `Set<String>` removes duplicate coerced values. The DTO's ordinary Set accepts a null member; `LiteSubscription` rejects a null set and null members when its setter adds them to the concurrent key set. Numeric and Boolean scalars are rejected as whole `liteTopicSet` values. `LiteSubscriptionDTO.version` and `OffsetOption.value` are primitive longs: missing/null values become zero; numeric strings, fractions and Booleans follow Java long conversion; object and array values reject.

## Decision

Represent `LiteSubscriptionDTO.liteTopicSet` as `Option<BTreeSet<Option<String>>>` so Rust callers see the DTO set behavior, deduplication and nullable members directly. Decode string and object values using the shared Fastjson String coercion, arrays member by member, an empty string as an empty set, and null as no set.

Keep `subscription_set` as `Option<Vec<Option<LiteSubscriptionDto>>>` as the Rust wire representation. Fastjson's object-to-singleton conversion is retained, duplicate null rows collapse to one, and separately decoded DTO objects remain separate even when their field values match. The vector preserves a deterministic input order; the Java HashSet's iteration order has no stable cross-process promise.

Decode primitive long fields through the Java-compatible integer adapters. Null values default to zero; `OffsetOption.value` and `LiteSubscriptionDTO.version` reject object and array input.

Map `LiteSubscription` separately with `group`, `topic`, a non-null `BTreeSet<String>`, and `update_time`. Its default constructor initializes the set and current epoch milliseconds. String/object/array Set inputs use Java coercion; null sets and null members reject because the Java concurrent set cannot store null. Repeated `liteTopicSet` properties accumulate through the Java setter, while repeated scalar properties use the last value. `addLiteTopic` and `removeLiteTopic` update the timestamp before their operation; collection overloads do the same. `setLiteTopicSet` adds without clearing or changing the timestamp. The Rust API exposes fluent setters, set accessors, and Java-shaped `Display` text.

## Evidence

The direct pinned Java DTO input corpus has 114 cases: the original 33 enum/default/duplicate/collection inputs, 49 per-field token shapes, seven DTO wrapper roots, seven `subscriptionSet` field shapes, seven `liteTopicSet` member shapes, seven `OffsetOption.value` shapes, and four set/empty-string cases. Native differentials match acceptance and encoded values. Test/fix commits: `bdbe660`/`8f1ddd0` and `d3a298e`/`ab3d0e2`; empty-string oracle: `df69ec0`. The shared `lookup.json` fixture adds a direct `LiteSubscription` method case and 16 Fastjson inputs for its set, scalar fields, duplicate properties, and primitive timestamp. Current fixture SHA-256: `d7865ce0f360704c6ada2c059c28893b30e3f77acde0ad8d9dd19bde291068a5`.

## Java identity and public type mapping

Pinned 5.5.1 `LiteSubscriptionDTO`, `LiteSubscription`, and `LiteSubscriptionCtlRequestBody` inherit identity-based `Object.equals`/`hashCode`; Rust no longer gives these models structural `PartialEq`/`Eq`. Their three Java identity cases confirm distinct equal-field objects compare false and remain separate HashSet members. The Rust API also exports `body::LiteSubscriptionDTO` and `header::GetConsumerListByGroupResponseBody` aliases for the pinned Java simple class names.

The Rust Lite types now map `LiteSubscription` fields, constructor defaults, topic mutation and timestamp methods; `LiteSubscriptionDTO` fluent setters and value getters; `OffsetOption` constructors, accessors, constants, equality and collection hashing; and the control body's Set getter/setter. Direct pinned Java cases verify the public method results and `toString` forms. Java's `OffsetOption.hashCode()` includes enum identity hashing, so Rust provides value-consistent `Hash` for collections without claiming the JVM-specific numeric hash. Broader Phase 6 source/API/input reconciliation remains open, and P6-V04 stays `PENDING_PHASE_19`.
