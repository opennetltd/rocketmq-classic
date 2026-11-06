# ADR-0075: Preserve route DTO mutable Map aliases

- Status: Accepted
- Date: 2026-10-10

## Context

Pinned RocketMQ 5.5.1 method calls show that `BrokerData.setBrokerAddrs` and `TopicQueueMappingInfo.setCurrIdMap` retain the exact mutable Map passed by the caller. Each corresponding getter returns that same Map. Java mutations through either reference are visible through the other.

The Rust route DTOs stored these maps by value. A setter/getter-shaped API could preserve mutation by borrowing the field, but could not preserve the caller's Map identity after ownership moved into the DTO.

## Decision

Represent `BrokerData.broker_addrs` as `Option<SharedBrokerAddressMap>` and `TopicQueueMappingInfo.curr_id_map` as `Option<SharedTopicQueueMappingIds>`. Both aliases wrap the existing map value in `SharedValue`, whose cloned handles refer to the same synchronized mutable value.

`get_broker_addrs` / `set_broker_addrs` and `get_curr_id_map` / `set_curr_id_map` retain and return the same handle. The `_owned` setters wrap owned maps for callers that do not need an external alias. Decoders wrap their owned parsed maps, and serializers read a snapshot while preserving the existing Fastjson-compatible JSON map keys and values.

The DTO `Clone` implementations continue to deep-copy the maps, matching the prior Rust copy behavior. `PartialEq` compares map contents to preserve Java DTO value equality. Handle identity is exposed by the accessors without changing equality or the wire representation.

## Consequences

Rust callers can now retain a map handle and observe mutations made through the DTO getter, and callers can mutate the handle after passing it to the setter. Access to a shared map uses `with` and `with_mut`; callers who need an independent owned copy can use `snapshot`. The public map field types change to the shared aliases, so code that previously mutated a field as a plain `BTreeMap` must use these handle methods or an owned setter.

This closes the audited alias gap for these two route DTO maps only. Other route DTO fields and APIs, full malformed-input coverage, and Phase 6 source/API/settings/reverse-export reconciliation remain open; the full audit stays PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Java oracle and fixture: `RuntimeRouteFixtures` / `routeDtoReferenceAliases` in `fixtures/runtime/route.json`.
- Route fixture SHA-256: `bf3ce0cb617989cf7e331dabfd738b8fc5a650cdc4b36037eae30f8add8960d3`.
- Implementation: `65cdabce` — Preserve route DTO map aliases.
- Differential regression: `route_dto_mutable_map_accessors_preserve_java_aliases`.
- Validation: full workspace/all-target tests with loopback access; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios and Java cross-decodes); comparator self-test; manifest validation (22 scenarios); formatting and diff checks.
