# ADR-0076: Preserve TopicRouteData collection aliases

- Status: Accepted
- Date: 2026-10-10

## Context

Direct pinned RocketMQ 5.5.1 calls show that `TopicRouteData` retains the mutable objects passed to `setQueueDatas`, `setBrokerDatas`, `setFilterServerTable`, and `setTopicQueueMappingByBroker`. Each getter returns the same object. The queue and broker lists retain their DTO elements, the filter map retains each nested list, and the mapping map retains each `TopicQueueMappingInfo`. Mutations through caller and getter references are visible in both directions.

Rust previously stored these collections by value, so moving a collection into the DTO lost caller-side identity and getters returned owned values or snapshots.

## Decision

Represent the four fields with `SharedValue` aliases. Queue/Broker collection handles contain shared mutable DTO element handles; filter tables contain shared nested filter-list handles; the mapping table contains shared mapping DTO handles. Named getter/setter methods retain and return these handles. `_owned` setters wrap plain Rust collection values for callers that do not need external aliases. Fastjson decode wraps each mutable collection level and element. Serialization and content equality operate on snapshots.

`TopicRouteData::Clone` continues to deep-copy its prior value semantics rather than aliasing collection handles. Java-compatible copy-constructor/clone methods separately preserve Java's shallow nested-reference behavior; see [ADR-0077](ADR-0077-route-copy-element-aliases.md). Runtime conversion and cache readers use explicit owned snapshots where they require Rust-owned values.

## Consequences

Rust callers can keep a collection or nested DTO handle across a setter call and observe mutations through the DTO getter, matching Java's reference behavior. Callers needing independent data can use the `_owned` setter or `*_snapshot` readers. Public field types change from plain collections to shared aliases; callers that previously mutated fields directly must use `with_mut`, a named setter, or a snapshot.

This closes only the four `TopicRouteData` collection alias rows. Other route API/input reconciliation and the full Phase 6 matrix remain open; P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Oracle: `RuntimeRouteFixtures.routeDtoReferenceAliasCases`.
- Fixture: `fixtures/runtime/route.json`, SHA-256 `d01c51ec40fdcf96931dd0191271a2b23fc396ec00f18f949aa629e678172e2b` after the copy/publication alias continuations.
- Native regression: `topic_route_data_collection_accessors_preserve_java_aliases`.
- Implementation commit: `45b8a5fc`.
- Validation: full workspace/all-target tests; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios and Java cross-decodes); comparator self-test; manifest validation (22 scenarios); formatting and diff checks.
