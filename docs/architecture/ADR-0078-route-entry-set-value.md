# ADR-0078: Preserve raw route entry replacement semantics

- Status: Accepted
- Date: 2026-10-10

## Context

`MQClientInstance.getTopicRouteTable()` returns a `ConcurrentMap`. A pinned Java runtime call obtains an entry from `entrySet().iterator()` and invokes `Map.Entry.setValue`. RocketMQ's Java 5.5.1 `ConcurrentHashMap.MapEntry` keeps the value captured when that entry was created. `setValue` updates the entry's local value, writes the new value to the backing map by key, and returns the entry's captured previous value. A later direct table replacement does not change an already returned entry's local value; calling `setValue` on it still writes unconditionally and returns that local captured value.

## Decision

Expose `TopicRouteEntrySetView::entry_snapshots()`, which returns detached entries in deterministic key order. Each `TopicRouteEntryHandle` stores its own captured route handle. `value()` reads that capture; `set_value()` replaces the capture and writes the same handle through `TopicRouteTable::put`, returning the capture that preceded the call. Other entry snapshots stay unchanged.

The route table's entry traversal remains a detached snapshot, one result permitted by Java's weakly consistent `ConcurrentHashMap` iterator contract. This adapter matches entry replacement and captured-value behavior without claiming Java iterator ordering or mutation visibility.

## Consequences

Rust callers can replace a raw route mapping through a retained entry handle while preserving Java's mutable route DTO identity. `TopicRouteDataHandle` remains the shared mutable value object; replacing a mapping does not mutate other aliases to the displaced route.

This closes the invoked `Map.Entry.setValue` behavior for the raw topic route table only. Other source/API and route-input reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Java oracle: `RuntimeRouteFixtures.runtimeTopicRouteTable` entry-set cases.
- Oracle and implementation commits: `306950d2` / `7832c140`.
- Native regression: `retained_raw_topic_table_matches_invoked_java_primitives_and_cached_queries`.
- `fixtures/runtime/route.json` SHA-256 when this route-only checkpoint was recorded: `35027b576c05eb456edd937f8475c121923c012c8297777a27e9657fbed3b116`; whole-file SHA-256 at the later route-matrix checkpoint was `d342d5b80077220113b89415d722a55350e7d6f5d316c5e8cbac3f7e8ddf265e`, before the subsequent raw-table concurrency cases.
- Validation: pinned Java fixture generation/verification; focused Rust differential; full workspace tests; strict Clippy; Maven verify; comparator/manifest checks; formatting and diff checks.
