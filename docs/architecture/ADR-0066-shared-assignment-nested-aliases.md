# ADR-0066: Preserve MessageQueueAssignment nested aliases

- Status: Accepted
- Date: 2026-10-10

## Context

Pinned RocketMQ 5.5.1 Java retains the exact `MessageQueue` and attachments `Map` passed to `MessageQueueAssignment` setters. Each getter returns that same object. Mutations through either the setter-held reference or the getter are visible through the other reference. Rust previously stored owned nested values, so its getters could reproduce value mutation but not this object identity.

## Decision

Represent `message_queue` as `Option<SharedAssignmentMessageQueue>` and `attachments` as `Option<SharedAssignmentAttachments>`, where both aliases wrap their values in `SharedValue`. Fastjson decoding wraps each non-null nested value, getters clone the shared handle, and setters accept the handle. The existing wire JSON shape and null-entry omission for attachments remain unchanged.

`MessageQueueAssignment` keeps a lawful Rust all-fields `PartialEq`/`Eq`/`Hash` implementation by comparing and hashing snapshots of the queue and attachments handles plus the mode. Its explicit `java_equals` method continues to model Java's queue-only comparison and null-queue failure. This Rust collection contract is intentionally distinct from Java's method-level `equals`/identity-derived `hashCode` behavior.

These nested values participate in a `HashSet` when contained by `QueryAssignmentResponseBody`. Mutating a queue or attachment after inserting its assignment can change the assignment's hash and leave the entry in its original bucket; Java has the same mutable-key hazard. Local Clippy allowances identify only the Java-compatible insertion/test sites that intentionally exercise this behavior.

## Evidence

The direct `assignmentMethodCases` oracle checks setter/getter identity and mutation visibility in both directions for a populated queue and attachments map. Rust verifies cloned-handle `ptr_eq` and the same two mutation paths. Fixture SHA-256 at this nested-field checkpoint: `cf03a6b58e3f312d8f1a4bc95a0384bc004338bc2d529141694bbf1810b63a71`. The assignment element alias is subsequently closed by `d312bb6c` and [ADR-0068](ADR-0068-shared-query-assignment-elements.md).

## Consequences and remaining boundary

Nested queue and map aliases are preserved for each `MessageQueueAssignment`. The `MessageQueueAssignment` element identity was a separate layer from the outer Set and nested queue/map aliases; it is now preserved by `d312bb6c` and [ADR-0068](ADR-0068-shared-query-assignment-elements.md). The core runtime continues returning its existing owned Set result by snapshotting the shared response Set. Phase 6 remains partial and P6-V04 remains `PENDING_PHASE_19`.

## Validation

`cargo test --workspace --all-targets --locked`, `cargo fmt --all -- --check`, strict all-target/all-feature Clippy, pinned offline Maven `verify` (22 scenarios and Java cross-decodes), the differential comparator self-test, manifest validation (22 scenarios), and `git diff --check` pass for code commit `aa4803df`. Maven used the pinned RocketMQ 5.5.1 artifacts and the temporary repository documented in the Phase 6 audit.
