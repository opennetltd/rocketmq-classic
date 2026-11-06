# ADR-0072: Preserve the LiteSubscription topic Set getter alias

- Status: Accepted
- Date: 2026-10-10

## Context

Pinned RocketMQ 5.5.1 calls show that `LiteSubscription.getLiteTopicSet()` returns the same mutable Set object on every call. Mutations made through that Set are visible to the subscription, and changes made through `addLiteTopic`, `addLiteTopics`, and the remove methods are visible through an earlier getter result. `setLiteTopicSet` adds supplied members without clearing existing topics, but it copies members rather than retaining the supplied Set object. The setter also leaves `updateTime` unchanged.

Rust previously returned a borrow of an owned `BTreeSet<String>`. That exposed member mutation only while the subscription borrow remained available and did not preserve a stable getter handle across later API calls.

## Decision

Store `LiteSubscription.lite_topic_set` as `SharedLiteSubscriptionTopicNames`, an alias for `SharedValue<BTreeSet<String>>`. Each getter clones the shared handle, and all topic add/remove methods mutate the contained set. `set_lite_topic_set` continues to copy and add members without clearing or changing `update_time`. Fastjson decoding wraps its result in the default shared set; JSON serialization and `Display` keep their existing output.

## Evidence

`RuntimeLookupFixtures.liteSubscriptionApiCases` now records whether the setter retains its input, whether later input mutation is visible, whether two getter calls return the same object, and whether getter-side mutation reaches the subscription. The pinned Java result is false for setter Set retention and later input mutation, and true for stable getter identity and getter mutation visibility. The Rust differential asserts those outcomes and checks mutations through an earlier cloned handle after later DTO operations.

`fixtures/runtime/lookup.json` SHA-256: `7d56171dba60258e9ec45ad91c483f51c0a049a65adaf2f62212c416e780094a`.

## Consequences and remaining boundary

Rust callers retain the getter result as a `SharedValue` and access its contents through `with`/`with_mut`. Setter input ownership remains independent, and the setter preserves Java's additive behavior. This closes the `LiteSubscription.liteTopicSet` getter alias. Heartbeat collection aliases and runtime status/reset Map aliases were open at this ADR checkpoint and were later closed by ADR-0074 and ADR-0073. Whole Phase 6 source/API/input reconciliation remains open. P6-V04 stays `PENDING_PHASE_19`.

## Validation

Code commit `e33c43b2` passes the protocol crate suite (108 unit tests, integration tests, and 15 doctests), full workspace/all-target tests with local socket access, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test, 22-scenario manifest validation, formatting, and diff checks.
