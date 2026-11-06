# ADR-0071: Preserve Lite control-body Set and element aliases

- Status: Accepted
- Date: 2026-10-10

## Context

Pinned RocketMQ 5.5.1 confirms `LiteSubscriptionCtlRequestBody.setSubscriptionSet` retains the supplied mutable Set and `getSubscriptionSet` returns that same object. The Set contains the same mutable `LiteSubscriptionDTO` objects supplied by its caller; getter iteration returns the identical instances. Additions through caller-held and getter-held Set references are visible in both directions. The previous Rust body owned a `Vec<Option<LiteSubscriptionDto>>`, which matched decoded values and mutable getter operations but did not share either the outer Set or element objects with the setter caller.

Both `LiteSubscriptionCtlRequestBody` and `LiteSubscriptionDTO` inherit identity-based `Object.equals`/`hashCode`. Distinct DTO instances with equal fields remain separate Set elements, while inserting the same DTO instance twice deduplicates it.

## Decision

Represent nullable DTO elements with `LiteSubscriptionDtoSet = HashSet<Option<SharedValue<LiteSubscriptionDto>>>`, where DTO handles compare and hash by stable allocation identity. Store the Set in `SharedLiteSubscriptionSet = SharedValue<LiteSubscriptionDtoSet>`. Setters retain the outer handle, getters clone it, and decoded JSON rows are individually wrapped before insertion. Null elements collapse to one Set member; separately decoded DTO objects remain identity-distinct. JSON serialization continues to produce the Java field and array shape.

## Evidence

`liteSubscriptionControlApiCase` records outer Set identity, setter-element identity, caller-side and getter-side membership, both mutation directions, and final size. The Rust differential asserts `ptr_eq` for the outer Set and supplied DTO handle, then verifies identity-based duplicate behavior and mutations through both handles. Existing Fastjson cases cover null/duplicate behavior and wire serialization. `fixtures/runtime/lookup.json` SHA-256: `4a74a1a23d7a6eb861f792e02bd9167e978b70d43ed42dba3ee7241815a359f3`.

## Consequences and remaining boundary

Rust callers supply `SharedLiteSubscriptionSet` handles containing shared DTO handles; HashSet membership uses object identity and is stable when DTO fields change. The collection alias is closed for the Lite control body. At this ADR checkpoint, `LiteSubscription.liteTopicSet`, heartbeat collection aliases, and runtime status/reset Map aliases remained open; they were later closed by ADR-0072, ADR-0074, and ADR-0073 respectively. Whole Phase 6 source/API/input reconciliation remains open. P6-V04 stays `PENDING_PHASE_19`.

## Validation

Code commit `66adb0c4` passes the focused Lite differential and protocol suite (108 unit tests, integration tests, and 15 protocol doctests), full workspace/all-target tests with local socket access, strict all-target/all-feature Clippy, pinned offline Maven `verify` (22 scenarios and Java cross-decodes), comparator self-test, 22-scenario manifest validation, formatting, and diff checks.
