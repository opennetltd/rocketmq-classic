# ADR-0081: Serialize registry compute remapping per key

- Status: Accepted
- Date: 2026-10-10

## Context

`MQClientInstance.getProducerTable()` and `getConsumerTable()` expose Java `ConcurrentMap` instances backed by `ConcurrentHashMap`. For an eligible `computeIfAbsent`, `computeIfPresent`, `compute`, or `merge`, a conflicting same-key writer waits for the callback and that writer race does not rerun it. `replaceAll` differs: its callback runs outside the bin lock, a same-key writer can finish during it, and a failed conditional replacement retries the callback with the writer's current value.

Rust registry compute-family remapping originally read a value, ran the callback, then sent a conditional mutation. A same-key write could land between those steps and make a compute callback run again, unlike Java. The native public table also shares its map with registration and unregister lifecycle operations, which are ordered by the runtime control actor. Applying the same gate across `replaceAll` callbacks would be incorrect because Java permits a writer to run during them.

## Decision

Use weakly retained async gates indexed by `(table kind, exact key)`. Producer and consumer maps have separate key spaces. Ordinary public writes, compute-family callbacks, producer/consumer registration and unregister operations, and pending registration cleanup use the same key gate. `replaceAll` invokes its callback before acquiring the gate. Under the gate it rereads the mapping and compares that value with the callback's captured value using Java owner equality; an equal current value is then replaced by identity while the gate keeps the mapping stable. Owner equality is sampled after the callback, so a mutable owner key changed during the callback participates in the same comparison Java performs at conditional replacement.

Compute-family remapping keeps the gate while it reads the mapping and runs the callback. `replaceAll` reads and calls its function without a gate, then acquires the gate and rereads the current value. If Java owner equality says the current mapping differs from the callback input, it retries with that mapping; if they compare equal, it sends an identity-conditional write against the reread value. It stops if the mapping disappeared. Table mutations keep the gate until the control actor confirms the backing-map update. Registration and unregister commands carry their owned gate into the actor; the actor releases it immediately after changing the registry map, before any Broker unregister RPC. If an undelivered registration must be rolled back while its key is busy, cleanup waits outside the actor before it enqueues the unregister command. This keeps the actor free to apply the mutation and avoids a same-key wait cycle.

Reads do not acquire the mutation gate or the registry entries mutex for the duration of a callback, so the callback can read the current mapping. Different table keys can proceed independently. Weak gate entries are pruned as the registry grows. `replaceAll` snapshots keys and conditionally updates each still-present key; it is not an all-map transaction. `clear` remains a direct map operation rather than an all-key transaction.

## Consequences

Compute-family remapping callbacks run once for their eligible mapping. Same-key table writes and lifecycle changes wait until those callbacks' map updates are applied. `replaceAll` callbacks can retry after a concurrent write whose value is unequal under Java owner equality; an equal-but-distinct owner value does not trigger a retry, and the callback's computed value replaces the current equal mapping. The writer can complete while the callback is running. The actor continues to define mutation order against unregister work, and Broker network waits do not hold the table key gate.

The producer and consumer Java oracle runs both a blocked `compute` callback and a blocked `replaceAll` callback with a concurrent same-key writer. It writes a distinct owner that compares equal to the callback input without causing a retry, and separately mutates the captured owner's equality key during the callback before the writer publishes the now-equal value. Native tests compare the compute-family serialization outcomes and, separately, `replaceAll` writer completion, live Java-equality comparison, callback input/count, writer previous value and final mapping for both table types. Raw route map ordering is covered by [ADR-0082](ADR-0082-raw-route-table-remapping-atomicity.md).

## Evidence

- Pinned Java 5.5.1 oracle: `RuntimeConfigFixtures.tableDefaultOperations` and its equal-value `replaceAll` race helper, invoked on both producer and consumer tables. One case mutates the callback's captured owner's equality key before the conditional replacement.
- Fixture: `fixtures/runtime/client-config.json`, SHA-256 `5f6a28c0199a64a905a5f642501745ce98eae57f55e19b84f38c0c1d0c55e890`.
- Native regressions: `registry_default_map_operations_match_invoked_java` covers compute-family contention; `registry_replace_all_uses_java_value_equality_for_concurrent_update` covers distinct-but-equal and mutable-key owner replacement for both maps.
- Validation: all eight focused registry-table tests, the full workspace, strict Clippy, pinned Maven, comparator and manifest checks pass; details are recorded in the Phase 6 execution log. Whole Phase 6 API/source/input/platform reconciliation remains open; P6-V04 remains `PENDING_PHASE_19`.
