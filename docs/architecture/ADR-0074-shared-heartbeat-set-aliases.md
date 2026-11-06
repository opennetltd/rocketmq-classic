# ADR-0074: Preserve heartbeat DTO Set aliases

- Status: Accepted
- Date: 2026-10-10

## Context

Pinned RocketMQ 5.5.1 getter/setter calls show that `SubscriptionData.tagsSet` and `codeSet`, `ConsumerData.subscriptionDataSet`, and `HeartbeatData.producerDataSet` and `consumerDataSet` retain the exact mutable Sets passed to their setters. Each getter returns the same Set. The Java oracle also mutates the contained DTOs through caller-held and getter-held references in both directions.

## Decision

Represent each nullable Set field with an `Option<SharedValue<...>>` handle and make getter methods return cloned handles to the same Set. Decoders wrap parsed Sets and elements; setters that accept shared handles retain those handles. `_owned` setter helpers accept the prior owned collection shapes for Rust callers that do not need caller-side aliasing.

The nested equality rules follow the Java DTOs:

- `tagsSet` and `codeSet` contain nullable strings and integers with value equality.
- `ConsumerData.subscriptionDataSet` contains shared `SubscriptionData` values whose `Eq`/`Hash` use Java's structural DTO equality. Mutating a member after insertion retains Java's mutable-key HashSet behavior.
- Heartbeat producer and consumer Sets contain shared DTO handles with pointer-based equality and hashing. Equal-field distinct `ProducerData` or `ConsumerData` instances therefore remain distinct Set members, as with Java `Object.equals/hashCode`.

`SharedValue::clone` aliases a handle. The DTO types keep deep `Clone` implementations so cloning the Rust DTO preserves the previous owned-copy behavior rather than creating a hidden collection alias. `with` and `with_mut` provide synchronized access; serialization reads the handles transparently and preserves the existing JSON arrays, null/default rules, and member deduplication.

Runtime heartbeat preparation stores shared DTO elements in shared Sets. The simple-heartbeat path deep-clones its DTO and clears only the clone's nested subscription Sets.

## Consequences

Rust now models the Java caller/setter/getter identity and mutation behavior for the five heartbeat Set fields. The API migration changes their public Rust field/getter types to shared handles; the `_owned` setter helpers provide a direct path from existing owned HashSets and vectors.

The Java API fixture checks outer Set identity, nested element identity where Java DTOs are mutable objects, caller-to-getter mutation, getter-to-caller mutation, and final collection sizes. Separate wire tests retain null element deduplication, duplicate identity-based heartbeat DTO rows, and unchanged JSON output.

This closes the audited heartbeat collection alias gap only. Broader Phase 6 source/API/input and reverse-export reconciliation remains PARTIAL, and P6-V04 stays `PENDING_PHASE_19`.

## Evidence

- Implementation: `7e5b3fa6` — Preserve heartbeat DTO set aliases.
- Direct Java cases: `subscriptionDataApiCase`, `consumerDataApiCase`, and `heartbeatDataApiCase` in `fixtures/runtime/heartbeat.json`.
- Rust differential: `heartbeat_dto_public_accessors_match_invoked_java`, plus runtime registration, preparation, and wire tests.
- Current `fixtures/runtime/heartbeat.json` SHA-256: `7c50b6ff17d46cc6659ab69e75a69c56d28a7a187ea198440c093acefa1f39b3`.
- Validation: protocol crate tests (108 unit tests, integrations, 15 doctests); full workspace all-target tests with local loopback access; strict all-target/all-feature Clippy; pinned offline Maven `verify` (22 scenarios, Java cross-decodes, and runtime fixtures); comparator self-test; manifest validation (22 scenarios); formatting and diff checks.
- Oracle generated with pinned RocketMQ 5.5.1 artifacts; the requested alternate Java source checkout was unavailable.
