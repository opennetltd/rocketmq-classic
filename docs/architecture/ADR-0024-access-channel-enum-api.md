# ADR-0024: Map the Java AccessChannel enum API

Status: implemented in `2eeca29`; whole-phase source and API reconciliation
remains open.

## Source contract

Pinned Java 5.5.1 `org.apache.rocketmq.client.AccessChannel` declares `LOCAL` and
`CLOUD`, generated `values()` and `valueOf(String)`, and inherits enum
`name()`, `ordinal()`, `toString()`, and `compareTo()`. `values()` returns a
defensive array copy. `valueOf(null)` throws `NullPointerException` with
`Name is null`; unknown or differently cased names throw
`IllegalArgumentException` with `No enum constant
org.apache.rocketmq.client.AccessChannel.<name>`.

## Rust mapping

`AccessChannel::values()` returns `[Local, Cloud]` by value; `name()`,
`ordinal()`, `Display`, and `java_compare_to()` match Java's names, declaration
order, text, and ordinal difference. `from_java_name(Option<&str>)` returns the
known variant or a typed `AccessChannelLookupError` for the Java null and
unknown-name categories. Rust variant equality matches enum singleton
identity. Java enum `hashCode()` is identity-based and has no stable cross-run
numeric value, so no Java numeric hash helper is claimed.

## Evidence and limits

The generated `accessChannelMethodCases` fixture invokes both constants and
name lookups for null, empty, exact, lowercase, and unknown input. It also
checks ordering, `compareTo`, `toString`, round-trip lookup, and defensive
`values()` copying. The `client-config.json` SHA-256 at this update was
`aa8e0e0e60096bdc7ffa9f7312331ef226e8254f9bd7aeccf9f9a85be1bd3d2f`.

Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned
offline Maven verification, the differential comparator/manifest self-test,
formatter, and diff checks pass. The typed Rust errors reproduce the observed
lookup categories and messages; Java exception objects and stack traces are not
represented. This closes the recorded AccessChannel enum cases, not the wider
Phase 6 audit or P6-V04 Phase 19 evidence.
