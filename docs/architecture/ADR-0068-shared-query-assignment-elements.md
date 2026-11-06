# ADR-0068: Preserve QueryAssignment response element aliases

- Status: Accepted
- Date: 2026-10-10

## Context

Pinned RocketMQ 5.5.1 Java retains the same mutable `MessageQueueAssignment` object supplied as a member of `QueryAssignmentResponseBody.messageQueueAssignments`. Iterating the getter Set returns that same element. Mutating its mode through the caller-held object is visible through the getter element, and a getter-held element mutation is visible through the caller-held object. ADR-0065 already preserved the outer Set identity; ADR-0066 preserved each assignment's nested queue and attachments aliases.

## Decision

Represent `QueryAssignmentSet` as `HashSet<Option<SharedQueryAssignment>>`, where `SharedQueryAssignment` is `SharedValue<MessageQueueAssignment>`. Fastjson decoding wraps each non-null element; null Set members and the existing JSON array shape remain unchanged. The shared element handle implements value-based `Eq` and `Hash` by snapshotting the wrapped `MessageQueueAssignment`, preserving the Rust Set membership contract that existed before this correction.

The core runtime continues returning its established owned `HashSet<Option<MessageQueueAssignment>>` by snapshotting both the Set and its element handles at the RPC boundary.

`MessageQueueAssignment` is mutable and its fields participate in the Rust value hash. Mutating a set element after insertion may leave it in its original HashSet bucket, matching Java's mutable-key hazard. Tests restore the original mode before any later Set operations.

This decision preserves object identity and current Rust equality/hash semantics. It does not claim equivalence for Java's process-local `MessageQueueAssignment.equals`/`hashCode` collisions, which remain documented separately.

## Evidence

The direct `queryAssignmentResponseApiCase` oracle records Java `==` identity for the setter-supplied and getter-returned element, and verifies mode mutation in both directions. The native differential asserts `ptr_eq` and the same observations. Fixture SHA-256 at this correction: `a4d4c85dc1ab17a240906f0e48348ebf2644c31bfcdd987d958fc1368b0e20de`.

## Consequences and remaining boundary

The outer response Set, each `MessageQueueAssignment` element, and each assignment's nested queue/map now retain shared identity in protocol DTO use. `ClientRuntime::query_assignment` still returns owned assignment values. Other Phase 6 alias, source, and input reconciliation remains open; P6-V04 remains `PENDING_PHASE_19`.

## Validation

Code commit `d312bb6c` passes `cargo test --workspace --all-targets --locked` (with local socket access), strict all-target/all-feature Clippy, pinned offline Maven `verify` (22 scenarios and Java cross-decodes), comparator self-test, 22-scenario manifest validation, formatting, and diff checks.
