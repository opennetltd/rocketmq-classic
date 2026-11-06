# ADR-0029: Map client status enum APIs to Java

Status: implemented; full Phase 6 reconciliation remains open.

## Source contract

RocketMQ 5.5.1 exposes `PullStatus`, `PopStatus`, `AckStatus`, and `SendStatus`
as public Java enums. Each has generated `values()` and `valueOf(String)` and
inherited `name()`, `ordinal()`, `toString()`, and `compareTo()` behavior.
`PopStatus` declaration order is `FOUND`, `NO_NEW_MSG`, `POLLING_FULL`,
`POLLING_NOT_FOUND`.

## Rust mapping

The Rust status types expose declaration-order `values()`, exact Java names,
ordinals, display text, ordinal comparison, and `value_of_name`. Typed lookup
errors preserve the Java null/unknown categories and messages. `PopStatus::values`
and `ordinal` explicitly follow Java order even though the existing Rust variant
declaration order differs.

## Evidence and limits

The pinned Java fixture directly calls each enum's `values()` and `valueOf`,
captures declaration order, defensive array copies, all inherited enum
methods, and null/empty/case-mismatched/unknown lookup failures. The core
differential checks all 40 cases. Full workspace tests, strict Clippy, pinned
offline Maven verification, formatter, and diff checks pass. This maps the
four status enum surfaces, not all Phase 6 API or runtime behavior.
