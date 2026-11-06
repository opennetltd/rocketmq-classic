# ADR-0062: Preserve Java lock-batch Set aliases

- Status: Accepted
- Date: 2026-10-10

## Context

Pinned RocketMQ 5.5.1 `LockBatchRequestBody`, `UnlockBatchRequestBody`, and `LockBatchResponseBody` retain the exact `Set<MessageQueue>` passed to their setters and return that same Set instance from their getters. Mutations through the caller and getter references are mutually visible. These DTO classes inherit identity-based `Object.equals` and `Object.hashCode`.

## Decision

The outer nullable Set fields use a shared handle, now named `Option<SharedMessageQueueSet>`. Its Set is `MessageQueueSet = HashSet<Option<SharedAssignmentMessageQueue>>`; each queue handle has Java `MessageQueue` value-based equality/hash while retaining the mutable element object. [ADR-0067](ADR-0067-shared-lock-set-element-aliases.md) records the nested element correction and its direct Java evidence. JSON serialization remains transparent to the wrapped Set, preserving array form, null members, empty defaults, and nullable Set behavior.

Remove derived structural equality and hashing from the three DTO body types. Keep `MQClientApi::lock_batch_mq`'s existing owned `Option<HashSet<Option<MessageQueue>>>` result by snapshotting the response field at that API boundary.

At the original outer-container checkpoint, nested `MessageQueue` instances were still owned values. That gap is closed by ADR-0067. Mutating a queue after insertion can still change its Set hash in both Java and Rust, retaining Java's mutable-key hazard.

## Consequences

Protocol DTO callers retain or receive a cloned handle for the same Set and the same queue elements. Mutations through either outer or nested handles are visible to all aliases. Runtime API callers continue to receive an owned snapshot.

## Evidence

- Direct pinned Java 5.5.1 API fixtures assert `getterReturnsAssignedSet`, caller/getter mutation visibility, and inherited `Object` declaring classes for equality/hash on all three DTOs.
- Native differential: `remaining_runtime_body_public_methods_match_invoked_java`.
- Implementation/oracle commit: `045b46e7`.
- Fixture SHA-256 at the outer-Set checkpoint: `07fb5f45492598ce7ca844903bd0440bbbaabd7baddb9bdfbbb5c96a5b39a7ad`.
- Nested element alias implementation: `625122ee`; `lookup.json` SHA-256 at the nested lock-element correction: `4b4d410671a673533e1238a8bcf8e939134884296610ec6d1045c1022c6818c6`; [ADR-0067](ADR-0067-shared-lock-set-element-aliases.md).
- Validation for the nested alias correction: protocol tests/doc tests, full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify, comparator self-test, 22-scenario manifest validation, format, and diff checks.
