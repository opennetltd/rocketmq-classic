# ADR-0021: Map MessageRequestMode.getName

Status: Accepted

## Context

The pinned Java `MessageRequestMode` enum exposes `getName()` in addition to its `PULL` and `POP` constants. The Rust enum represented those values for serialization and assignment DTOs but had no named accessor matching this public method.

## Decision

Add `MessageRequestMode::name() -> &'static str`, returning `"PULL"` for `Pull` and `"POP"` for `Pop`.

## Evidence

`RuntimeLookupFixtures.messageRequestModeMethodCases` invokes Java `getName()` on every enum constant. `message_request_mode_name_matches_invoked_java_public_api` compares the Rust method with both recorded values.

Current lookup fixture SHA-256: `0c9ccc65b6c95d28d90d8de1bbac000436fe4468f1e7ebace6449ca1cf25c8e2`.

This maps the enum's `getName()` method only; broader Phase 6 API/input reconciliation remains open.
