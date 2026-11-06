# ADR-0019: Match assignment response Set inputs

Status: Accepted

## Context

Phase 6 `QueryAssignmentResponseBody.messageQueueAssignments` is a Java `Set<MessageQueueAssignment>`. Rust previously represented it as `Option<Vec<MessageQueueAssignment>>`, which rejected a Fastjson object-to-singleton conversion, rejected null members, and retained duplicate rows that Java's set removed.

## Decision

Represent the response field and runtime result as `Option<HashSet<Option<MessageQueueAssignment>>>`. Decode JSON arrays as sets, a JSON object as a singleton set, and null/missing fields as no set. Retain a null member, deduplicate repeated nulls and identical rows, and reject string, number, and Boolean top-level values. Default encoding omits a null set and preserves a null member as JSON null. The Rust set uses the normal derived `Eq`/`Hash` contract across all assignment fields.

## Evidence

`RuntimeLookupFixtures.assignmentBodyCases` invokes the pinned Java 5.5.1 `QueryAssignmentResponseBody.decode` for 12 missing/null/empty, singleton-object, null-member, duplicate, same-queue/different-mode, and rejected scalar cases. `assignment_body_inputs_match_invoked_java_fastjson` compares acceptance, decoded values, and re-encoded response shape. A runtime socket case verifies null-member retention and duplicate-row collapse.

Combined lookup fixture SHA-256 at this update: `0c5d9337f8d8adb3961ecbbcf63970aca2f8074048bc25b15d75de025fd2f809`. The current fixture after the additional set inputs is recorded in the Phase 6 compatibility inventory.

`MessageQueueAssignment.equals` in the pinned Java class compares only `messageQueue`, while its `hashCode` includes `messageQueue`, `mode`, and `attachments`. That violates Java's normal equals/hash contract and gives hash-collision-dependent set behavior for rows with the same queue but different metadata. [ADR-0020](ADR-0020-assignment-method-api.md) adds an explicit Rust `java_equals` helper for the public equality method while keeping lawful all-fields Rust `Eq`/`Hash`; a Java fixture now constructs an equal-hash pair at runtime by selecting an attachment hash that cancels the current JVM's enum identity-hash difference. Java confirms equality, equal hash, and one retained set member. This result is deliberately not reproduced by the native all-fields Eq/Hash set because the identity hashes are process-local and cannot be derived from the wire DTO; exact collision-dependent Set outcomes remain a documented boundary. This decision closes the recorded response-field inputs and standard set cases only; broader assignment API reconciliation, whole-Phase 6 source/API/input reconciliation, and P6-V04 Phase 19 evidence remain open.

## Array-element input continuation (2026-10-09)

At this update the pinned `QueryAssignmentResponseBody.decode` fixture contained 22 cases. Seven cases extended the set member shapes: string, number, Boolean, nested array, nested-array object, mixed null/object, and mixed scalar/object. Java rejects every non-null member that is not an object and accepts `[null, {}]`; the native fixture differential matched the decoded values and encoded response shape. The lookup fixture SHA-256 at that update was `778b74e60be12bd4a29efd484e467ae055ed525a1a8f8207c04182b580627bde`.

## Nested MessageQueue input continuation (2026-10-09)

The current pinned decoder corpus contains 29 cases, including seven `MessageQueueAssignment.messageQueue` token shapes. Java accepts null, empty-object, and populated-object values, and rejects string, number, Boolean, and array values. The native fixture differential compares nullable queue accessors (so absent and explicit-null string fields remain observable) plus the encoded response shape. Current combined lookup fixture SHA-256: `a9d27f0941069a6424a74dcfe6e70bc7f9cff21acdbe74d85da342423ef6b5d0`. This closes the probed nested queue token shapes; other assignment DTO inputs and Phase 6 source/branch reconciliation remain open.
