# ADR-0082: Serialize raw route-table remapping per topic

Status: implemented for the invoked raw-table race; broader Phase 6 acceptance remains open.

## Source contract

`MQClientInstance.getTopicRouteTable()` exposes a Java `ConcurrentMap`. The pinned Java 5.5.1 oracle distinguishes compute-style operations from `replaceAll`:

- A blocked `compute` callback reads the current mapping and runs once. A same-key writer waits until compute publishes its result, then receives that result as its previous value and leaves the writer value current.
- `replaceAll` invokes its callback without excluding a same-key writer. If the writer changes the mapping before the conditional replacement, Java compares the observed values and calls the callback again with the new value.
- `clear` waits for a blocked same-key `compute` callback, then removes that computed mapping. The callback still runs once and returns its computed value.
- A `clear()` invoked reentrantly inside a same-key `compute` callback completes and removes that mapping before the callback returns. The callback's computed value is still returned by `compute`, but it is not reinserted after the callback because `clear()` already removed the observed mapping.

Different keys do not share this per-key ordering requirement.

## Native implementation

`RouteCache` owns a weak registry of mutation gates keyed by exact topic. Compute-style `TopicRouteTable` remapping callbacks (`compute_if_absent`, `compute_if_present`, `compute`, and `merge`), direct and conditional writes, collection-view conditional removals, and normal route-refresh publication use the same gate. These callbacks run without holding the route snapshot lock, so reads can inspect the current mapping while they run. `replace_all` takes a key snapshot but does not hold the key gate during its callback; it gates only its compare-and-replace, uses Java route-value equality, and retries the callback after a conflicting write. It is not an all-map transaction. `put_all` applies independent per-key writes and never holds gates for multiple keys at once.

The route refresh path performs route comparison and conversion before taking a topic gate, then holds the gate across the raw-map observation and coherent snapshot publication. This avoids making an unrelated conversion hold up a direct table mutation while ensuring the publication and a same-topic remap have a single order. `clear` snapshots the current keys and removes them one at a time under their topic gates, so it waits for a same-topic compute without becoming a whole-table transaction. If the current thread already owns a topic gate through a compute callback, `clear` removes that key directly; another thread still waits on the gate. Concurrent insertions after the key snapshot are not covered by that clear pass. Gates are weakly retained and expired entries are pruned when the registry grows.

Reentrant same-topic writes from a remapping callback return `RuntimeError::RecursiveRouteTableMutation` instead of waiting on their own gate, except `clear`, whose Java behavior removes the current mapping synchronously.

## Evidence

The `runtimeTopicRouteTable` Java fixture records four compute-style race outcomes, four `replaceAll` race outcomes, four clear/compute race outcomes, and four reentrant-clear outcomes. Its complete `runtimeTopicRouteTable.primitives` map has 81 outcomes; whole-file `fixtures/runtime/route.json` SHA-256 is `1eb5f057f1a1855674fddda7313ec63874bbf3462228de03d8d03da50189803e`. The native `retained_raw_topic_table_matches_invoked_java_primitives_and_cached_queries` test compares all primitive/view outcomes, confirms `replaceAll` retries after a writer runs during its callback, checks clear waiting/removal after compute, checks that reentrant clear observes and leaves the mapping absent while returning the computed value, and directly checks that a recursive compute-style same-topic write is rejected without deadlock.

Focused route-table coverage, `cargo test --workspace --all-targets --locked`, strict workspace/all-target/all-feature Clippy, pinned offline Maven verify, formatting, and diff checks pass. The broader Phase 6 source/API/input/platform reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.
