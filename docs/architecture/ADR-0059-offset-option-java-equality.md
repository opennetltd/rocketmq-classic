# ADR-0059: Map OffsetOption Java equality

- Status: Accepted
- Date: 2026-10-09

## Context

Pinned RocketMQ 5.5.1 exposes `OffsetOption.equals(Object)` and `hashCode()`. `equals` returns false for null and unrelated classes, then compares the primitive value and enum constant identity. The overridden hash calculation includes `OffsetOption.Type.hashCode()`, which is a JVM-local identity hash and has no portable numeric value across processes.

Rust already derived value equality and `Hash`, so equal options had the same `HashSet` membership behavior, but it lacked a Java-shaped comparison for null and unrelated values and had no direct method oracle for the hash contract.

## Decision

Add `OffsetOption::java_equals(Option<&dyn Any>)` to map Java's `equals(Object)` boundary: null and wrong-type inputs return false; matching `OffsetOption` values use the existing value equality. Keep derived `Eq` and Rust `Hash`, which preserve equal-value collection behavior. Do not assert or invent a cross-process Java hash integer; invoke Java `hashCode()` only to verify same-object stability and equal-object hash consistency within that Java process.

## Consequences

Rust now exposes a named comparison that covers Java's broader Object argument, while its normal `PartialEq` remains convenient for same-type Rust use. `HashSet` membership matches Java for equal options. The numeric Java hash is intentionally not part of cross-runtime fixture equality because the enum component depends on JVM object identity.

## Evidence

- Pinned `javap -public` and bytecode for `OffsetOption.equals(Object)`/`hashCode()` and `OffsetOption.Type` in RocketMQ 5.5.1.
- Direct Java cases cover equal/different values and types, null/wrong-type arguments, same-object hash stability, equal-object hash consistency, and Set deduplication.
- Rust differential: `lite_subscription_related_dto_apis_match_invoked_java`.
- Implementation/oracle commit: `e7fb09f6`.
- Lookup fixture SHA-256: `72e92cb461d8b4842622850296c01e0fd604b4d49250990bc3beea2cfcefc544`.
- Validation: full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes), differential comparator self-test and 22-scenario manifest validation, formatting, and diff checks.
- Oracle uses pinned RocketMQ 5.5.1 artifacts; the requested alternate Java source checkout was unavailable.
