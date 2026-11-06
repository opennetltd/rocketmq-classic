# ADR-0057: Map consumer status and reset offset body methods

- Status: Accepted
- Date: 2026-10-09

Historical implementation status: the caller-owned outer and nested Map alias gap described below was closed by `2f07fe94`; see [ADR-0073](ADR-0073-shared-runtime-map-body-aliases.md). The observations and Java API contract recorded here remain valid.

## Context

Pinned RocketMQ 5.5.1 `GetConsumerStatusBody` exposes getters and setters for `messageQueueTable` and `consumerTable`. `ResetOffsetBody` exposes getters and setters for `offsetTable`. Existing direct inputs exercised their Fastjson behavior but did not invoke these public methods on constructed objects.

## Decision

Keep the existing Rust snake_case accessors and add direct Java invocation oracles plus Rust differentials. Both Java constructors initialize their maps empty. The cases cover getter values after assignment, explicit-null setter results, nullable Long entries, empty nested maps, null nested maps, and Java object identity for the assigned outer and nested maps. `GetConsumerStatusBody` has mutable Rust borrows for both maps, including nested consumer rows, and `ResetOffsetBody` has a mutable borrow for its offset rows so callers can reproduce mutation through each body getter.

Rust stores the data as owned entry vectors and a `BTreeMap`; shared getters lend immutable views and the runtime body mutable getters lend mutable borrows. The Rust differential matches mutation through those borrows, values, ordering where defined by the native entry representation, null states, and defaults. Rust still does not preserve Java's shared identity between the setter's caller-owned Maps and the body, or between an externally held nested Map and the Rust-owned rows.

## Consequences

The public methods, constructor defaults, and getter-side mutation now have direct coverage in addition to the DTO input matrix. The caller-owned mutable Map alias boundary remains explicit; overall Phase 6 source/API/input reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

## Evidence

- Pinned `javap -public` confirms the four `GetConsumerStatusBody` methods and two `ResetOffsetBody` methods.
- Direct Java cases: `consumerStatusBodyApiCase` and `resetOffsetBodyApiCase` in `fixtures/runtime/lookup.json`.
- Rust differential: `consumer_status_and_reset_offset_public_methods_match_invoked_java`.
- Original oracle/test commit: `471b7160`; GetConsumerStatusBody mutation oracle/implementation: `89aef2e2` / `3d5cfd44`; ResetOffsetBody mutation oracle/implementation: `a6e6d723` / `ff81313a`.
- Current aggregate lookup fixture SHA-256: `b77cd6ce3487c18e1c2d9b34c06ff2354f9c025ab65ac15a1255a3ed9fb31bc3`.
- Validation: focused GetConsumerStatusBody/ResetOffsetBody differential, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks.
- Oracle uses pinned RocketMQ 5.5.1 artifacts; the requested alternate source checkout was unavailable.
