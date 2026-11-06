# ADR-0049: Match ConsumeMessageDirectlyResult Fastjson field input

- Status: Accepted
- Date: 2026-10-09

## Context

The existing Java differential exercised `ConsumeMessageDirectlyResult.consumeResult` enum values, but did not cover the primitive and string fields or repeated properties. The pinned RocketMQ 5.5.1 Java oracle now invokes Fastjson 2.0.63 for all five fields, seven JSON token kinds per field, seven root shapes, and repeated fields.

## Decision

Keep the public Rust field types and use a custom `Deserialize` implementation that first applies the shared Fastjson last-property-wins object decoder, then applies the shared Java-compatible field coercers. Implement `Default` explicitly so its values match the Java no-argument constructor.

The 66-case corpus confirms that missing `autoCommit` defaults to `true`; explicit null or numeric/fractional values for primitive booleans become `false`, while Boolean values are retained. `remark` uses Fastjson String coercion, including compact JSON text for arrays and objects. `spentTimeMills` maps null to zero, truncates numeric fractions, and converts Booleans to 1/0; strings, arrays and objects reject. `consumeResult` retains its existing enum name/ordinal behavior. A null root decodes as a nullable DTO, an empty object uses constructor defaults, and non-object non-null roots reject. Every repeated known field keeps its final value.

## Consequences

`ConsumeMessageDirectlyResult` now matches the tested Java input, constructor defaults, and normalized encoding while keeping idiomatic Rust field types. Java exception classes and untested Fastjson inputs remain outside this adapter's contract. This DTO closure does not change the Phase 6 PARTIAL status or P6-V04 ownership.

## Evidence

- Direct Java fixture: `consumeDirectResultCases` in `fixtures/runtime/lookup.json` (66 cases; SHA-256 `c7662fe1bbbe62726eef0eda14774a3bc08b3910581d771153c3b7b04eccaf40`).
- Oracle/test commit: `910cbd2`; Rust decoder commit: `00d2ea8`; constructor-default regression/fix: `6d53470`/`b4ec6f4`.
- Validation: full workspace/all-target tests with local loopback access, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java/Rust fixture checks), formatting, and diff checks.
