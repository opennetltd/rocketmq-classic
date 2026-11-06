# ADR-0022: Map heartbeat enum display-name methods

Status: Accepted

## Context

The pinned Java heartbeat enums expose `ConsumeType.getTypeCN()` and `MessageModel.getModeCN()`. The Rust enums already represented the values used in heartbeat DTO fields but had no matching public methods.

## Decision

Add `ConsumeType::type_cn() -> &'static str` and `MessageModel::mode_cn() -> &'static str`. Consume types return `PULL`, `PUSH`, or `POP`; message models return their enum names `BROADCASTING`, `CLUSTERING`, or `LITE_SELECTIVE`.

## Evidence

`RuntimeHeartbeatFixtures.consumerEnumMethodCases` invokes both Java methods on every actual enum constant. `consumer_enum_public_methods_match_invoked_java_values` compares all six returned strings.

Heartbeat fixture SHA-256 at the enum-method update: `7e9521037d5c7b9904ea25ae18ec9923bf6c42d3f9aa65ecf7abb80dd4388589`.

This maps the two enum display-name methods only; the broader Phase 6 DTO/API audit remains open.
