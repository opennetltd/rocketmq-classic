# ADR-0073: Preserve status and reset body Map aliases

- Status: Accepted
- Date: 2026-10-10

## Context

Pinned RocketMQ 5.5.1 API cases confirm that `GetConsumerStatusBody` retains the exact mutable `messageQueueTable` and `consumerTable` Maps passed to its setters. Its nested consumer Maps are retained as well. `ResetOffsetBody` likewise retains the Map passed to `setOffsetTable`. Mutations through caller-held references and getters are mutually visible.

## Decision

Represent the assigned outer tables with `SharedValue` handles. `GetConsumerStatusBody.message_queue_table` and `ResetOffsetBody.offset_table` use `Option<SharedQueueOffsetEntries>`, where `SharedQueueOffsetEntries` is a shared handle to the queue/offset entry vector. `GetConsumerStatusBody.consumer_table` uses `Option<SharedConsumerQueueOffsets>`; each non-null consumer entry contains its own `SharedQueueOffsetEntries` handle. Setters retain a supplied handle and getters clone the same handle, so `ptr_eq` reports identity and mutations flow in both directions.

Fastjson decoding wraps each decoded outer table and each non-null nested consumer table in a new handle. Serialization and `encode_java_json` read snapshots through the handles, preserving existing Java JSON shape, null/default behavior, and entry encoding. `with` and `with_mut` provide access; `snapshot` returns an owned value. This preserves Map-container aliases only. It does not claim identity for individual `MessageQueue` keys.

## Consequences

These public DTO fields now have explicit shared-handle types. Rust callers can retain handles across setter calls or getter calls and observe mutations through all aliases. The pinned direct API cases assert outer and nested table identity and both mutation directions. Existing value comparisons are preserved through structural equality on snapshots.

## Evidence

- Implementation commit: `2f07fe94` — Preserve status and reset map aliases.
- Direct pinned Java API oracle: `consumerStatusBodyApiCase` and `resetOffsetBodyApiCase` in `fixtures/runtime/lookup.json`.
- `lookup.json` SHA-256 remains `7d56171dba60258e9ec45ad91c483f51c0a049a65adaf2f62212c416e780094a`.
- Passed protocol tests (108 unit tests, integrations, 15 doctests), full workspace/all-target tests with local socket access, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and Java cross-decodes), comparator self-test, 22-scenario manifest validation, formatting, and diff checks.
- The separate Phase 6 source/API/input reconciliation remains PARTIAL; P6-V04 stays `PENDING_PHASE_19`.
