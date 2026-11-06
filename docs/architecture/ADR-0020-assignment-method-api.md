# ADR-0020: Map MessageQueueAssignment public methods

Status: Accepted

## Context

The pinned Java `MessageQueueAssignment` exposes bean getters/setters, `equals`, and `toString`. The initial Rust value model had public fields and derived equality but no accessor pair, no Java-shaped text, and no method that represented Java's queue-only equality and its null-queue failure. The later shared-handle correction is recorded in [ADR-0066](ADR-0066-shared-assignment-nested-aliases.md).

## Decision

Add idiomatic getters/setters, `Display` and `java_to_string`, and a typed `java_equals` method. The explicit equality method compares only `message_queue`, returns false for a null other value, returns true for the same object reference, and reports Java's null `messageQueue` dereference for distinct objects. Rust's all-fields `PartialEq`/`Eq`/`Hash` keeps a lawful hash-collection contract. Since ADR-0066, the nested queue and attachments use `SharedValue` handles; Rust equality and hashing compare their value snapshots.

## Evidence

Two actual Java 5.5.1 method cases cover default and populated accessors/text plus queue-only equality when mode and attachments differ, different queues, self/null/wrong-type inputs, and the null-queue exception. `assignment_methods_match_invoked_java_public_api` compares the Rust accessor values, `Display`, and explicit equality outcomes.

Combined lookup fixture SHA-256 at the original method-API checkpoint: `0c9ccc65b6c95d28d90d8de1bbac000436fe4468f1e7ebace6449ca1cf25c8e2`.

Java `MessageRequestMode` inherits identity-based `Object.hashCode`, so its numeric hash changes across JVM runs and has no stable enum-value equivalent. Rust keeps standard `Hash`; reproducing Java's hash-bucket collision effects is not claimed. This closes the recorded accessor/equality/text cases only; the wider Phase 6 API/input audit remains open.

## Historical checkpoint: mutable attachments getter

The pinned Java getter returns the same Map passed to `setAttachments()`. At this checkpoint Rust's `attachments_mut()` reproduced getter-side value mutation through an owned `BTreeMap`, but did not preserve caller identity. ADR-0066 supersedes that ownership boundary: setter and getter now share an `AssignmentAttachments` object.

- Direct Java evidence and Rust differential: `assignmentMethodCases` in `fixtures/runtime/lookup.json`; `assignment_methods_match_invoked_java_public_api`.
- Oracle/test commit: `359ed253`; no implementation change was needed.
- Checkpoint aggregate `fixtures/runtime/lookup.json` SHA-256: `8b6dab97da3ebca80f494b07ebcc7b12da396d700587137e136ea8620ed8d972`.
- Full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Phase 6 remains PARTIAL and P6-V04 stays PENDING_PHASE_19.

## Historical checkpoint: mutable nested queue getter

The pinned Java getter returns the same `MessageQueue` assigned with `setMessageQueue()`. At this checkpoint Rust's `message_queue_mut()` reproduced getter-side value mutation, but its move setter did not preserve caller identity. ADR-0066 supersedes that ownership boundary: setter and getter now share an `AssignmentMessageQueue` object.

- Direct Java evidence and Rust differential: populated `assignmentMethodCases` entry in `fixtures/runtime/lookup.json`; `assignment_methods_match_invoked_java_public_api`.
- Oracle/test commit: `caff0e22`; implementation commit: `8fff4781`.
- Checkpoint aggregate `fixtures/runtime/lookup.json` SHA-256: `9cdbcae68d20569b3d0cfdbf774aaf4183a63430438d9e3d74e8ad2e64b863d1`.
- Full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Phase 6 remains PARTIAL and P6-V04 stays PENDING_PHASE_19.

## Nested caller/getter alias correction

ADR-0066 closes the nested queue and attachments identity gap. Getter calls return cloned shared handles; setter calls retain the same handles; `with`/`with_mut` mutate the referenced Java-compatible value in either direction. The response Set's assignment element object identity is now preserved by [ADR-0068](ADR-0068-shared-query-assignment-elements.md).
