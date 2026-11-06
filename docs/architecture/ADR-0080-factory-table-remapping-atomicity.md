# ADR-0080: Serialize FactoryTable compute remapping per key

- Status: Accepted
- Date: 2026-10-10

## Context

`MQClientManager.getFactoryTable()` exposes a Java `ConcurrentHashMap`. Its `compute`, `computeIfPresent`, `computeIfAbsent`, and `merge` callbacks participate in the per-key mapping operation. A same-key writer waits for those callbacks, which therefore run once for that writer race. `replaceAll` has different behavior: its callback runs outside the bin lock, a same-key writer can finish during it, and a failed conditional replacement retries the callback with the current value.

Rust `FactoryTable` originally read a value, called the closure without a per-key guard, and then conditionally wrote it. That did not match Java's compute-family serialization. The first correction added exact-key gates for those compute-family operations. A later `replaceAll` race showed that holding the same gate through its callback was also incorrect: Java lets a same-key writer complete and may call the replace function again after the conditional write misses.

## Decision

Use weakly retained mutation gates keyed by the exact Java-compatible table key. Each gate has async and synchronous, reentrant exclusion. Ordinary `FactoryTable` mutations and compute-family remapping hold the gates for their read/callback/write operation. `replaceAll` reads the current mapping and runs the callback without a key gate, then acquires both gates for an identity-conditional write. A missed conditional write releases the gate and retries with the latest mapping; a missing key ends that entry's visit. Same-ID runtime creation and asynchronous ID removal use the async gate. Synchronous identity-safe manager removal looks up the same key gate and uses its synchronous half. Different keys do not share a gate; expired weak entries are pruned when the registry grows, so arbitrary aliases are not retained indefinitely.

Callbacks run without holding the manager's entries mutex, so ordinary reads can inspect the current table while a remapping callback runs. Compute-family callbacks hold their key gates; `replaceAll` callbacks do not. The synchronous state lock is held only while updating gate state, not while user code runs, so callback panic does not poison future operations. `replaceAll` visits a traversal-start key snapshot and conditionally updates each key separately; it is not an all-map transaction.

## Consequences

Compute-family callbacks are invoked once for an eligible mapping, and same-key writers plus synchronous identity-safe manager removal wait until the computed result is published. `replaceAll` callbacks can be retried when a same-key writer wins; its writer can complete during the callback. Ordinary reads continue during both callback forms. The callback tests use the actual manager factory table and pinned Java fixture, including identity-safe removal contention, `replaceAll` retries, read visibility and panic recovery. The manager rendezvous test also proves two distinct keys that collided under the prior shard mask initialize concurrently. Producer/consumer registry-table callback concurrency is covered separately by [ADR-0081](ADR-0081-registry-table-remapping-atomicity.md), and raw route map ordering by [ADR-0082](ADR-0082-raw-route-table-remapping-atomicity.md). Phase 6 source/API/input reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Pinned Java 5.5.1 oracle: `RuntimeConfigFixtures.managerTable`; `compute` holds its same-key writer through the callback, while the `replaceAll` race lets the writer finish, retries with the writer's value, and publishes the computed value.
- Fixture: `fixtures/runtime/client-config.json`, SHA-256 `2cf1a273a3276010ec117e7a8e68b6216ac5c7403999b02da05c5a8d2f9510b2`.
- Native regressions: `concurrent_factory_table_remapping_serializes_same_key_callback_like_java`, `factory_table_replace_all_retries_after_concurrent_same_key_write`, `identity_safe_manager_removal_waits_for_factory_table_remapping`, callback read coverage, `remapping_callback_panic_does_not_poison_factory_table_mutations`, and the unrelated-key construction rendezvous.
- The new Java/native `replaceAll` race compares writer completion, callback retry input/count, the writer's previous value, and the final mapping. Full workspace, strict Clippy, Maven, comparator and manifest checks for this correction are recorded in the Phase 6 execution log.
