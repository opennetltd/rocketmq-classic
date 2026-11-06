# ADR-0065: Preserve QueryAssignment response Set identity

- Status: Accepted
- Date: 2026-10-10

## Context

RocketMQ 5.5.1 `QueryAssignmentResponseBody.setMessageQueueAssignments` retains the supplied mutable Set and `getMessageQueueAssignments` returns that same object. The pinned Java API oracle already exercised mutation through both references. The Rust DTO previously moved the `HashSet` into the body and exposed a mutable borrow, so changes through the caller's retained collection could not reach the DTO.

The same oracle now records that this response DTO inherits `equals` and `hashCode` from `java.lang.Object`: one instance equals itself, while an equal-content distinct instance does not.

## Decision

Represent `message_queue_assignments` as `Option<SharedQueryAssignmentSet>`, where `QueryAssignmentSet` is `HashSet<Option<SharedQueryAssignment>>`. ADR-0068 records the shared element handle. The Fastjson decoder wraps both the outer Set and each non-null assignment; the setter accepts the outer handle, and the getter returns a cloned handle. `ptr_eq`, `with`, `with_mut`, and `snapshot` make identity, mutation, and detached reads explicit. Serde continues to emit the same JSON Set/array wire shape.

Remove structural `PartialEq` and `Eq` from `QueryAssignmentResponseBody` to match Java Object identity. The core runtime API snapshots the Set so its existing owned return type does not change.

## Consequences

The direct differential verifies handle identity, caller-to-getter and getter-to-caller mutation, null setter behavior, and Java equality/hash declaration. At this outer-Set checkpoint, `MessageQueueAssignment` element identity remained open. It is closed by `d312bb6c` and [ADR-0068](ADR-0068-shared-query-assignment-elements.md).

This closes the response DTO outer Set alias; ADR-0068 separately closes its assignment element alias. Full Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

## Evidence

- Pinned Java invocation: `RuntimeLookupFixtures.queryAssignmentResponseApiCase` in `fixtures/runtime/lookup.json`.
- Fixture SHA-256 at the outer-Set checkpoint: `fb436699a2c7d128fd96c73e3f106add77904444fa581485a5ddc1d929bc251e`.
- Native differential: `assignment_body_public_methods_match_invoked_java` (within `assignment_body_public_methods_match_invoked_java`'s API fixture assertions).
- Outer-Set implementation commit: `dbaed386`; element alias correction: `d312bb6c` / [ADR-0068](ADR-0068-shared-query-assignment-elements.md).
- Validation: protocol tests; workspace/all-target checks and tests; strict all-target/all-feature Clippy; pinned offline Maven fixture verification and cross-decodes; comparator self-test and manifest validation; formatting and diff checks.
- Oracle uses the pinned RocketMQ 5.5.1 artifacts; the separate source checkout was unavailable.
