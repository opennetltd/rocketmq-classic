# ADR-0067: Preserve lock-batch MessageQueue element aliases

- Status: Accepted
- Date: 2026-10-10

## Context

Pinned RocketMQ 5.5.1 Java retains the mutable `MessageQueue` objects inside the Sets assigned to `LockBatchRequestBody.mqSet`, `UnlockBatchRequestBody.mqSet`, and `LockBatchResponseBody.lockOKMQSet`. Iterating any matching getter returns the same element object. Mutating the setter-held or getter-returned queue changes what the other reference observes. ADR-0062 already preserved the outer Set object but stored its queue elements as owned values.

## Decision

Represent these fields as `Option<SharedMessageQueueSet>`, where `SharedMessageQueueSet` wraps `MessageQueueSet = HashSet<Option<SharedAssignmentMessageQueue>>`. Decode each non-null queue into a shared handle. `SharedValue<MessageQueue>` implements value-based `PartialEq`, `Eq`, and `Hash`, matching Java `MessageQueue` Set membership and duplicate suppression while preserving element identity. The shared Set wrapper continues to preserve outer-container identity.

The core `MQClientApi::lock_batch_mq` keeps its established owned result type by snapshotting each queue handle at the RPC boundary. JSON arrays, null members, defaults, and text output are unchanged.

Because `MessageQueue` fields affect its value hash, changing a queue after inserting it into a HashSet can leave it in its old bucket. Java has the same mutable-key hazard. The fixture-backed test restores the original key value before performing Set lookup/removal, and narrowly scoped Clippy allowances document the Java-compatible mutable key sites.

## Evidence

The direct `lockBatchRequestApiCase`, `unlockBatchRequestApiCase`, and `lockBatchResponseApiCase` Java cases now record element `==` identity plus caller-to-getter and getter-to-caller mutation visibility. Rust verifies those observations with handle `ptr_eq` and both mutation directions for all three DTOs. `fixtures/runtime/lookup.json` SHA-256 at this correction: `4b4d410671a673533e1238a8bcf8e939134884296610ec6d1045c1022c6818c6`.

## Consequences and remaining boundary

Rust callers pass a shared queue handle when they need to retain the identity of a queue placed in one of these Sets. DTO Set and element aliases now match Java, while the core RPC result remains an owned snapshot. Other Phase 6 alias/input/source reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

## Validation

Code commit `625122ee` passes `cargo test --workspace --all-targets --locked`, `cargo check --workspace --all-targets --locked`, strict all-target/all-feature Clippy, pinned offline Maven `verify` (22 scenarios and Java cross-decodes), comparator self-test, manifest validation (22 scenarios), formatting, and diff checks.
