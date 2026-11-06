# ADR-0058: Heartbeat DTO public collection accessors

- Status: Accepted
- Date: 2026-10-09

## Context

Pinned RocketMQ 5.5.1 declares Java bean getters and setters for all fields on `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData`. Rust represents scalar DTO values as public fields and now exposes Rust-idiomatic methods for each declared Java accessor.

## Decision

Keep nullable scalar fields explicit with `Option`; preserve Java enum names and existing JSON field shapes. The collection alias behavior is now recorded separately in [ADR-0074](ADR-0074-shared-heartbeat-set-aliases.md): getters return shared handles for all five Set fields, and setters retain the supplied handle. Rust's `_owned` setter helpers remain available for callers that want to transfer an owned collection without constructing a handle.

Direct Java cases and Rust comparisons cover constructor defaults, assigned property values, null setter results, and mutations through caller-held and getter-held collection references. `SubscriptionData.subVersion` uses a time-window assertion for its constructor-time value.

## Consequences

The four DTO bean method surfaces map to Rust accessors without changing serialized JSON. Java caller/getter Set identity and mutation are preserved for `SubscriptionData.tagsSet/codeSet`, `ConsumerData.subscriptionDataSet`, and `HeartbeatData.producerDataSet/consumerDataSet`. ADR-0074 defines the collection value-versus-identity equality boundaries and the deep-clone behavior.

These bounded DTO method and collection alias rows are closed. Broader Phase 6 source/API/input and reverse-export reconciliation remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.

## Evidence

- Pinned `javap -public` signatures for `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData` in RocketMQ 5.5.1.
- Java direct cases: `subscriptionDataApiCase`, `producerDataApiCase`, `consumerDataApiCase`, and `heartbeatDataApiCase` in `fixtures/runtime/heartbeat.json`.
- Rust differential: `heartbeat_dto_public_accessors_match_invoked_java`.
- Original API oracle/implementation: `21591fc7`; mutable Set mutation oracle/implementation: `33829680` / `5151a308`; alias continuation: `7e5b3fa6` / ADR-0074.
- Current heartbeat fixture SHA-256: `7c50b6ff17d46cc6659ab69e75a69c56d28a7a187ea198440c093acefa1f39b3`.
- Current full validation is recorded in [ADR-0074](ADR-0074-shared-heartbeat-set-aliases.md) and the Phase 6 execution log.
- Oracle uses pinned RocketMQ 5.5.1 artifacts; the requested alternate Java source checkout was unavailable.
