# ADR-0079: Preserve runtime owner-table entry replacement semantics

- Status: Accepted
- Date: 2026-10-10

## Context

`MQClientInstance.getProducerTable()`, `getConsumerTable()`, and `MQClientManager.getFactoryTable()` expose Java `ConcurrentMap` instances. Direct pinned RocketMQ 5.5.1 calls obtained entries from each map's `entrySet()` and invoked `Map.Entry.setValue`. The entry returns its captured old value, changes its own captured value, and unconditionally writes the new value to the map. A separate map replacement does not refresh an existing entry; a later `setValue` still returns its entry-local capture and replaces the current mapping. The raw route table behavior is recorded separately in [ADR-0078](ADR-0078-route-entry-set-value.md).

## Decision

Expose `entry_snapshots()` on producer, consumer, and factory entry-set views. Each `ProducerEntryHandle`, `ConsumerEntryHandle`, and `FactoryEntryHandle` carries its key and a private captured value. Calls through clones of the same handle are serialized. `set_value()` writes the mapping through the backing live table, then updates that capture and returns the value captured immediately before the call. Existing entry handles do not track later writes made directly through the table.

Registry and factory writes are asynchronous in Rust. Do not update an entry's cached value before awaiting its table mutation: cancellation while a registry write waits behind unregister must leave the entry snapshot unchanged when no mapping write occurs. A successful `set_value()` preserves the Java-observed entry and backing-map results.

Entry traversal remains a deterministic detached snapshot, one outcome permitted by Java's weakly consistent `ConcurrentHashMap` iterator contract. This API preserves the observed entry mutation while leaving Rust's owned runtime values and lifecycle handling intact.

## Consequences

Callers can use retained registry and manager entry handles to replace a mapping with the same captured-value behavior observed from Java. The entry set itself remains live for later snapshots and direct mutations. This closes the invoked `Map.Entry.setValue` cases for the producer, consumer, and factory maps; full Phase 6 source/API/input reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Java oracle: `RuntimeConfigFixtures` producer-table, consumer-table, and manager-table cases.
- Oracle, base implementation, and async cancellation fix commits: `a2dcae02` / `eca1d066` / `cb42bee3`.
- Native regressions: `retained_registry_collection_views_match_invoked_java`, `retained_factory_table_and_atomic_mutations_match_invoked_java`, and `running_table_writes_wait_for_unregister_and_cancelled_queue_writes_do_not_commit` (including canceled and subsequent successful entry replacement).
- Current `fixtures/runtime/client-config.json` SHA-256: `172bf979f0f0aa5ee715c52064ebb6c90740d04fb4aa2b5d43eaab3c5d0437f6`.
- Validation: full workspace tests; strict all-target/all-feature Clippy; pinned offline Maven verification (22 scenarios); comparator self-test and manifest validation (22 scenarios); formatting and diff checks.
