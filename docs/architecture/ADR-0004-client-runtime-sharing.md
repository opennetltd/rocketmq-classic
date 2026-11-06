# ADR-0004: Explicit per-client runtime ownership

Status: accepted for construction/sharing. Lifecycle, group leases and task acceptance are still in progress in Phase 6.

The Phase 6 plan proposed ADR-0002, but that number already belongs to the accepted NameServer response-peer decision. This decision uses the next free number; it does not change the planned ownership contract.

Pinned Java 5.5.1 `MQClientManager.getOrCreateMQClientInstance` owns a `computeIfAbsent` key before constructing resources. `removeClientFactory(id, instance)` is identity-aware. `MQClientInstance` construction does not start its remoting transport; start failure later removes only that instance. Java uses a singleton; the Rust plan explicitly requires an injectable/shareable manager instead.

`ClientRuntimeManager` clones share a table. The table contains per-ID entries; each entry owns an async initialization mutex and a write-once runtime cell. The short table lock never spans construction or await. Only the winning key owner constructs a transport; no losing construction is created and discarded. Different IDs initialize concurrently. A failed constructor conditionally removes its entry. Waiters on that old entry must recheck the authoritative mapping before construction. Cancelled initialization releases its per-key guard and drops unpublished resources, allowing a later retry.

Runtime removal reads the immutable identity cell, not a best-effort `try_lock` on initialization: concurrent lookup cannot spuriously suppress required cleanup. It compares the mapped runtime pointer before removing the entry. An old runtime/entry cannot remove a replacement.

The manager uses the first entry for a client ID without comparing later configuration, environment properties, or RPC hook identity. The first initializer's cloned config, construction-time property snapshot, and hook remain attached to the runtime; subsequent same-ID callers receive that runtime and their input config remains unchanged. This matches Java's `get` followed by `computeIfAbsent` flow. `ClientConfig::clone_client_config` follows Java's copy omissions; Rust `Clone` remains an exact value copy.

Executable evidence: `tests/runtime.rs` concurrent identity/configuration/retry/removal cases and manager unit tests for independent-key rendezvous, stale entry removal, cleanup during a lookup lock and cancelled unpublished initialization. These prove construction ownership, not yet the P6-V01 running task/socket gate. Resource start/shutdown, registration identity leases, joined periodic work and startup-failure rollback must complete before Phase 6 acceptance.

## Owned lifecycle/control extension

The runtime now owns one command worker for registration/start/unregister/shutdown. A weak reference is upgraded only while processing a command; the idle worker does not keep a runtime/manager cycle alive. Registry producer/consumer handles are weak, while callers own actual implementations. Explicit group identities, not runtime Arc counts, guard normal stop. Admin groups participate in the stop guard but not heartbeat preparation.

Java's synchronized unregister includes Broker RPCs; a local pointer check alone cannot prevent a late unregister from invalidating replacement Broker state. Rust registration APIs therefore become async, queued behind the same owned control worker. No synchronous guard crosses await. Caller cancellation drops its response receiver, not the in-flight command. A private pending-delivery guard undoes registration when a caller never accepts the returned token. Accepted lease cloning/dropping does not implicitly change registration; owning facades must unregister explicitly.

The control JoinHandle stays stored during polling. An async join gate serializes wakers; a cancelled joiner releases the gate without detaching the handle, and another caller can complete the same join. Startup failure closes current resources, clears unusable group registrations, retains Failed on old handles and removes only the manager's current identity. Normal stop unregisters the actual Java internal producer group before closing transport. Registration/transport fault checkpoints, parallel start/stop, pending-delivery cancellation, cancelled terminal join and scripted external/internal unregister ordering are executable checks. Discovery/inbound resources and their checkpoints remain pending; this extension does not close the phase gate.

## Owned periodic extension

The control worker owns one periodic worker, not one independently overlapping task per timer. This preserves Java's single scheduled executor and fixed-rate catch-up. Initial deadlines are recorded at registration; the next deadline advances from the previous deadline rather than completion. A slow callback delays later due jobs. Route, heartbeat, persistence and adjustment callbacks operate on upgraded registry snapshots outside locks, and a callback panic does not kill subsequent timer jobs.

Activation occurs only after successful registration of all current schedules. Startup failure and last-registration stop cancel and join periodic work before transport cleanup. Tests use private per-runtime liveness counters and paused time to prove single-worker ownership, all seven current rollback boundaries with discovery (six with explicit addresses), dropped pending callbacks and no ticks after stop.

Concurrent V1 heartbeat uses a bounded set of borrowed futures owned by the same round. Cancelling the round drops all pending peer futures; no detached task pool remains. V2 negotiation caches peer support/fingerprints under short locks, with actual RPC awaits outside those locks. Rust canonical set order is a private fingerprint implementation choice; singleton fingerprints are fixture-tested against Java, while Java HashSet iteration order is not imposed on multi-element Rust sets. Channel-active heartbeat and full inbound/wakeup ownership remain unaccepted.

## NameServer discovery extension

Absent explicit NameServer configuration triggers a best-effort fetch before transport start, then a 10s/120s fixed-rate job owned by the same periodic worker. Discovery failures do not invalidate a previous address list or stop the runtime. Source-compatible callback updates reject only null, whereas fetch also rejects Java blank values.

Java's first TopAddressing ServiceLoader provider becomes an explicitly supplied Rust capability on the manager. Default HTTP fallback is preserved for absent/empty provider results; callback registration occurs per runtime. The provider receives a callback holding only Weak discovery state, so retaining callbacks does not retain stopped/dropped runtimes. Manager clones retain the same provider and table; domain/subgroup properties are part of the runtime-sharing snapshot. No new public configuration key or unresettable plugin registry is added.

The HTTP implementation uses the [pinned reqwest ClientBuilder API](https://docs.rs/reqwest/0.12.28/reqwest/struct.ClientBuilder.html), native CA roots and the existing remoting AWS-LC backend. Existing remoting/TLS regressions verify that the dependency does not introduce conflicting rustls providers. HTTP/URL/callback tests and 19 invoked Java offline fixtures cover current discovery paths; live endpoint/TLS release verification remains separate from Phase 6 unit acceptance.

## Owned inbound extension

Start installs eight registrations using identity leases owned by the control worker. Failure and normal stop remove only those exact registrations; an old lease cannot remove a replacement even when both use the same processor handle. The processor holds a weak runtime reference and selects upgraded producer/consumer capabilities outside registry locks, preventing transport/runtime cycles and allowing callback reentry.

Transaction routing reads actual stored-message PGROUP, reply routing uses CORRELATION_ID rather than responder producerGroup, and the group-keyed consumer operations use their exact group header keys. Code 40 is different: Java decodes the optional consumerGroup for logging, wakes runtime-wide rebalance, catches handler exceptions and returns null; Rust signals the owned rebalance service and emits no response. Lower remoting forwards channel address through a default-compatible processor method. Remaining feature owners receive original commands; absence returns explicit unsupported with an activation phase, not mock success. Full transaction/reply/admin semantics remain unaccepted until their owning phases; reply activation must preserve successful acknowledgement of unknown/late correlations.

All eight current slot boundaries participate in rollback tests (15 current checkpoints with discovery, 14 with explicit addresses). Scripted loopback checks prove inactive errors, code 40 and registered no-dispatch silence, one-way silence, and active context/response routing. The [inbound matrix](../compatibility/PHASE_6_INBOUND_MATRIX.md) records source provenance and deferred behavior. Consumer-change wakeup uses the existing rebalance worker; pull feature behavior and cumulative Phase 6 gates remain open.

## Rebalance wakeup extension

One additional owned worker represents Java RebalanceService, not a task per consumer/wakeup. Notify's sticky single permit coalesces repeated signals, including those sent before worker activation. The last-round timestamp starts at runtime construction. Minimum/default/custom interval tests exercise throttling, unbalanced retries and balanced waits. All optional tryRebalance owners are visited outside locks, and each owner's panic is isolated without changing Java's aggregate result semantics.

Delayed wakeups are deadlines in the existing periodic worker, not detached timer tasks. They serialize behind scheduled callbacks as Java's shared single scheduled executor does. The control worker closes/clears their queue and cancels/joins rebalance before shutting periodic work and transport. Private liveness probes and startup failure checkpoints now include this worker (16 with discovery, 15 with explicit addresses). A pending callback is dropped on cancellation and no post-stop round runs.

Property keys are the exact Java rebalance wait/minimum keys and participate in shared-runtime compatibility checks; no new ClientConfig fields or public tuning settings are introduced. Rust-native optional capabilities expose the wakeup/round boundary while actual consumer queue allocation remains Phase 11. Pull request dispatch ownership remains open.

## Pull request/task ownership extension

The runtime owns independent FIFO dispatch and scheduled-task workers, corresponding to PullMessageService's request queue and single scheduled executor. Requests carry only group/mode/opaque owner state at this phase. Current consumer lookup happens at dispatch, so a scheduled request sees a replacement consumer rather than a captured stale owner. Optional pull/pop futures are invoked outside locks; actual consumer fetch/request/process-queue implementation remains in consumer phases. Missing capability is not claimed as feature success.

Scheduled request jobs enqueue into FIFO, while generic future jobs represent Java Runnable capabilities. Deadline plus insertion sequence determines serial scheduler order. A blocked generic job delays later scheduled requests/tasks without blocking FIFO dispatch. Panic containment permits later work, and cleanup drops user futures/requests outside the queue mutex.

Construction starts no workers. Explicit scheduling in Created state lazily starts only the scheduled worker through the owned control plane, matching Java executor availability before transport start; normal start reuses it. Startup rollback records actual worker creation order and joins in reverse. Normal stop closes new submissions, cancels/joins FIFO, allows the scheduled worker 1000ms graceful completion, then cancels/joins and clears remaining work. Private probes now cover 18 discovery/17 explicit-address failure checkpoints and exactly two pull workers for concurrent starts. Unit/paused-clock checks prove Created scheduling, graceful completion, cancellation and no post-stop execution. No per-request task or consumer allocation scheduler is introduced.

## Channel-active heartbeat extension

The existing remoting event worker now polls a borrowed asynchronous active callback, instead of the runtime spawning event tasks. Default sync listeners adapt through the same method. Callback construction/poll panics are contained, and transport shutdown cancels pending callback work before joining the worker. This adds no startup worker or failure checkpoint.

The optional core listener is bound to a Weak runtime after manager publication. It uses a coherent Broker snapshot, matches the active address, and sends single-peer heartbeat with the shared nonblocking heartbeat gate. RPC success records the Broker version and wakes the existing runtime-wide rebalance worker. Busy initial connection callbacks, unknown addresses, disabled listeners and terminal runtimes do not start new work. Explicit single-peer sends retain Java's absence of the all-round producer-only slave filter, and V2 state is shared across both entry points.

Wire tests prove active heartbeat/wakeup and unregister coexist. A retained listener cannot retain/revive a stopped runtime, and a pending async callback is dropped on shutdown without detaching work. Complete source/config/route/hook and cumulative phase gates remain open.

## Effective route expansion extension

The coherent cache reuses corrected Phase 1 converters rather than adding a second runtime converter. Ordinary publication preserves stable broker-name-only QueueData ordering and uses the first matching BrokerData's master availability; subscription deduplicates queues. Static conversion shares effective logical queue space with endpoint construction. The pinned Java total-count threshold remains -1, distinct from highest-epoch per-ID endpoint selection. Only exact global scope strips its prefix; null scope contributes no queues. Equal-ID/equal-epoch HashMap tie ordering is not a portable Java guarantee.

The private memory budget follows raw branch selection, permissions/master availability, deduplicated subscription counts and static scope maxima. Suppressed ordinary counts and repeated mapping totals do not consume expansion budget. Separator-only ordered configuration remains ordered-empty instead of falling through to ordinary publication. Genuinely excessive effective collections are rejected before conversion, preserving the prior snapshot. The frame-size-derived limit is an internal safety boundary, not a new public setting. Fourteen actual Java route/endpoint cases and hostile/redundant metadata tests verify these boundaries; admin/default-producer fallback and refresh serialization still require source audit before phase acceptance.

## NameServer refresh serialization extension

One runtime-local async gate represents Java's lockNamesrv. Both route refresh and offline Broker cleanup wait at most 3000ms. Refresh holds the gate through RPC, conversion, publication and owner callbacks, serializing different topics without blocking snapshot readers. Cleanup timeout skips cleanup rather than blocking heartbeat indefinitely. The gate does not extend the original absolute RPC deadline. Guard ownership makes cancellation release the gate; cancelled waiting futures leave no task/resource to join. Different runtimes remain independent, and no worker or public tuning setting is added.

The snapshot now exposes Admin address resolution with selected peer slave/version metadata. Typed String addresses cannot contain Java null entries; available-entry selection is Rust-native rather than emulating JVM HashMap order. Subscribe adjacent lookup preserves Java's original requested nonzero-ID slave flag, even if the adjacent signed ID is zero. Source/branch tests and scripted serialized RPCs verify these boundaries; default-producer fallback and full phase source/acceptance audits remain open.

## Default-route and owner-refresh extension

Default-producer queue count stays producer-owned and is passed explicitly into the runtime's default-route branch. That branch queries Java's fixed auto-create topic TBW102 through the existing typed RPC and NameServer gate, then assigns both read/write counts from min(producer limit, fetched read count). Conversion/publication uses the requested topic. No producer setting is promoted into a shared runtime key; producer send/fallback selection remains Phase 7-owned.

Rejected replies and SUCCESS without body retain Java's runtime false/unchanged outcome, while lower typed API error classifications remain intact. Transport and malformed JSON errors propagate without replacing snapshots. Owner need-update callbacks run only when the route is unchanged, and owner demand forces coherent Broker/topic publication even if the raw route compares equal. This restores that topic's Broker table after another topic overwrote the same Broker name. The existing shared NameServer gate protects compare/check/publish ordering, without awaiting under registry/cache locks or adding tasks.

## Mutable RPC interception support

Java runtime hooks mutate command metadata before user signing. Lower remoting therefore exposes a default-compatible mutable-before hook capability instead of pre-applying fields outside the actual hook chain. Old read-only/result hooks adapt automatically. Owned command mutation is visible to subsequent hooks and encoding, with modified async request snapshots and after/retry copies. Inbound pipeline/processor/after hooks and response identity share the modified command. Existing hook error/panic containment and task ownership are retained; no new worker or public configuration is introduced. Runtime namespace/stream/dynamic activation and source-specific heartbeat language still require their own implementation/verification.

## Runtime RPC hook activation

Runtime construction now registers namespace, optional stream, user, then dynamic zone hooks. Namespace/stream precede user signing; dynamic zone fields follow it. NamespaceV2 and stream enablement affect generated request fields; if two configs produce the same client ID, the first runtime's retained hook chain remains active. Dynamic zone property/environment inputs are reevaluated per invocation and therefore are not frozen as per-runtime compatibility snapshots. Exact Java precedence/blank semantics are retained through a private property-reader seam, not a new public setting.

The typed API explicitly creates Java-default commands while configured language applies only to heartbeat V1/V2. Lower remoting retains its independent Rust command default. Thirty-six actual Java hook cases, live-read/same-ID first-owner checks and actual NameServer/heartbeat/oneway wire checks validate these boundaries. Activation adds no tasks, startup checkpoints or strong runtime links. Automatic IP and final source/cumulative phase acceptance audits remain open.
