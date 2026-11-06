# ADR-0016: Match Java assignment mode Fastjson inputs

Status: Accepted

## Context

Phase 6 `queryAssignment` decodes `QueryAssignmentResponseBody`, whose nested `MessageQueueAssignment.mode` is a Java `MessageRequestMode`. Native Serde accepted only the canonical uppercase enum names and represented the field as non-nullable, while the pinned Fastjson decoder has additional name, ordinal, null, and default behavior.

## Decision

Represent `MessageQueueAssignment.mode` as `Option<MessageRequestMode>`. Its Rust `Default` and an absent JSON field use `Some(Pull)`, matching the Java no-argument field default. Explicit null and unknown enum names decode to `None`; re-encoding omits that field, matching Java's default JSON output.

Known enum names are case-insensitive. Integer ordinals and integer-valued strings map by declaration order (`0 = PULL`, `1 = POP`); JSON numeric fractions truncate toward zero before ordinal lookup. Decimal number inputs use Java's exact BigDecimal-to-int narrowing: after truncation, the low 32 bits are used before checking the enum ordinal. Thus `0.9999999999999999999` becomes `PULL` and `4294967296.0` wraps to `PULL`, while the plain integer `4294967296` rejects. A fractional string such as `"1.9"` remains an unknown enum name and decodes to null. Other out-of-range ordinals and Boolean input are rejected.

## Evidence

`RuntimeLookupFixtures.assignmentModeCases` invokes 20 actual Java 5.5.1 `QueryAssignmentResponseBody.decode` inputs. The cases compare acceptance, decoded mode, and encoded response shape. The new decimal precision probes reproduce the f64 mismatch before the fix, confirm decimal wrapping versus plain-integer rejection, and pass with raw-token decoding. `assignment_mode_inputs_match_invoked_java_fastjson` checks the same fields with the native DTO.

Lookup fixture SHA-256 at the original 17-case update: `ef5ca72024a2556f5027998d3e18f1d82dfaa36d8961c2628389e9b2b69711ea`. Current combined lookup fixture SHA-256: `42a05d175175b94dc7e1bfc051940f70b16a01cbba0345ff2a35a5a0fa2d57e1`.

Focused and full workspace tests, strict all-target/all-feature Clippy, pinned offline Maven verification, comparator/manifest, formatting, and diff checks pass. Rust parse errors do not reproduce Fastjson exception classes; this closes the recorded inputs only, not all assignment DTO combinations or Phase 6 reconciliation.
