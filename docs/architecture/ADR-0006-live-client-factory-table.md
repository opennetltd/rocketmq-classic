# ADR-0006: Owned live access to the client factory table

Status: implemented for Phase 6; final phase acceptance remains separate.

## Context

Pinned Java 5.5.1 MQClientManager.getFactoryTable returns its actual ConcurrentMap.
Retaining that value observes later creations/removals, and map mutations affect
subsequent manager lookups. Returning a detached native HashMap would lose this
public behavior. Native manager entries additionally own per-key initialization
and immutable sharing inputs; exposing those private entries would leak ownership
internals rather than the source runtime map.

## Decision

ClientRuntimeManager.factory_table returns a clonable FactoryTable handle owning
the same table. get/contains_key/len/is_empty read published runtimes, excluding
private unpublished initializer reservations. snapshot explicitly returns a
detached owned iteration copy; retaining FactoryTable itself remains live.

Provide put, put_if_absent, replace, replace_if_same, remove, remove_if_same and
clear. Conditional values compare Arc identity, as MQClientInstance inherits
Object equality. Mutations await only the affected key's initializer, then verify
that its entry is still current before applying the operation under the table
lock. No synchronous guard crosses await; no owner callback runs under these
locks. clear is not a transaction against concurrent insertion, like the source
concurrent map. These operations supply source map mutation primitives with Rust
ownership, not Java collection/thread implementations.

Replacement entries retain the inserted runtime's original property snapshot
and user-hook identity. The receiving manager's compatibility checks cannot be
bypassed by rereading current process properties or inventing a hook. This
metadata is private and adds no public setting.

Explicit native managers permit a runtime to be present in more than one table.
Runtime cleanup therefore keeps deduplicated weak table links and removes only
its own identity under its logical client ID from each linked table. It must not
remove a replacement. Arbitrary alias keys, including an empty key, are allowed;
cleanup does not erase aliases, matching source cleanup by logical ID. Removing,
replacing or clearing mappings does not shut down the removed runtime.

## Evidence

Actual unstarted Java managerTable oracle records retained-view freshness and
put/putIfAbsent/conditional replace/remove/absent/alias/clear outcomes. Native
integration checks match each outcome, race 64 put_if_absent calls and prove
owned-value release. Other checks verify original hook/config/property sharing,
cross-manager stale-safe shutdown, unpublished reservation visibility, per-key
mutation waits, unrelated-key concurrency and cancelled mutation preserving the
eventual mapping. Existing identity-safe lifecycle and startup rollback suites
remain required gates. Java runtimes/transports never start in the oracle and
always receive construction teardown.

Rust collection traversal and detached iteration copies are not claimed to have
JVM iterator identity/order. Java null keys/values are not representable in the
typed native signatures; the source ConcurrentMap rejects them. Phase 7 producer
accumulation and Phase 19 server validation remain their named owners.
