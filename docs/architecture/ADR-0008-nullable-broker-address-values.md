# ADR-0008: Preserve nullable Broker address values

Status: implemented in `8fb1ec8` under the user's explicit public API parity instruction. The independent ADR-0007 language migration is also implemented. Phase 6 as a whole remains incomplete.

## Confirmed source and invoked behavior

Pinned client 5.5.1 commit `efa1be59d6fc747ad220f036c86030f05f0419f9` has `BrokerData.brokerAddrs: HashMap<Long,String>`. Its JSON decoder accepts a null map value. This is distinct from a null map, whose runtime publication fails as already covered by the null-map oracle.

`RuntimeRouteFixtures.runtimeNullBrokerValues` invokes actual JSON-to-TopicRouteData decode, real unstarted MQClientInstance refresh/cache getters and actual publish/admin/subscribe/topic lookups through the existing route-return-only API adapter. Two inputs use master0=null, with and without slave1="slave:10911":

- Both decode and refresh succeed, preserving master key0 and its null value in the cached raw route.
- Both have two ordinary publish and two subscribe queues. `topicRouteData2TopicPublishInfo` checks **containsKey(MASTER_ID)**, not nonnull master value.
- Publish and exact-master subscribe address lookups return null for both inputs. Admin returns null for the null-only map and the valid slave for the mixed map: source Admin iterates until it finds a nonnull value.
- Repeated actual topic lookup only returns null or the present slave, without imposing a JVM random sequence or asserting that both choices must appear in a finite sample.

Both original and adapter APIs are torn down; no transport or server starts. The historical ignored native regression failed with `invalid type: null, expected a string`; `f0cb682` enabled it and independently reproduced the red result before implementation. The test now passes without ignore in `8fb1ec8`.

## Proposed model and migration

Change public `BrokerData.broker_addrs` from `Option<BTreeMap<i64,String>>` to `Option<BTreeMap<i64,Option<String>>>`. Preserve absent/null map versus a present map with absent key versus present-null address. Retain exact wire property names and raw key/null values. Current Phase 6 `RouteSnapshot.broker_addresses` must also preserve nullable values coherently; adapt consumers rather than invent another parallel route DTO/cache.

Audit every lower/core/fixture/test call site: ordinary/static converters and effective expansion budget, cached equality/refresh, topic random selection, publish/admin/subscribe lookups, offline cleanup, heartbeat/version/active-channel matching and typed lookup RPC inputs. In particular:

- Do not suppress publish queues because the master **value** is null when its key exists.
- Publish lookup exposes no address for a null master. Admin skips null entries; subscribe exact/adjacent checks require nonnull values, while arbitrary fallback follows the source's first-entry behavior rather than borrowing Admin's skip-null scan.
- Topic selection retains null-valued candidates when the nonnull master shortcut is unavailable. A selected null is no address, not a fabricated connection target.
- Preserve raw null entries for refresh/cache comparisons and serialization. Address-bearing BrokerAddress results only exist when the selected address is nonnull; no nullable BrokerAddress field is inferred without source evidence.
- Remove the regression's ignore only when the entire invoked decode/refresh/conversion/lookup check passes, and rerun full exact gates after migration.

No public tuning setting, worker, retry, server capability or new dependency is proposed. Native collection order/RNG ownership remains Rust-native; this ADR does not demand a duplicate Java HashMap implementation.

## Rejected substitutes and review boundary

Dropping null entries removes the master key, incorrectly eliminating the two publish queues and changing cached raw route state. Empty-string or literal-null sentinels fabricate nonnull addresses and alter lookup/transport behavior. Rejecting the whole route is the confirmed current mismatch, not Java's behavior. A permissive core-only JSON shim cannot preserve the completed public DTO contract for downstream callers.

The public parity requirement in the active user objective authorized this source-backed correction; no approval of the separate nullable command-language migration was inferred. That migration is now independently implemented under ADR-0007. The general matrix and Phase 6 audit remain PARTIAL because the broader source/input reconciliation is unfinished.

## Implementation evidence

Public `BrokerData.broker_addrs` and `RouteSnapshot.broker_addresses` now retain present-null entries. Decode/encode, raw/derived snapshot equality, ordinary/static conversion bounds, topic random selection, publish/admin/subscribe lookups, offline cleanup, heartbeat/active-channel matching and unregister use that model; address-bearing `BrokerAddress` is produced only for nonnull values. Source method inspection confirms nullable heartbeat and unregister peers are skipped, Admin scans for a nonnull value, Subscribe retries a nonnull adjacent slave before its first-entry fallback, and `BrokerData.selectBrokerAddr` samples all values when master is null. Tests include the invoked Java route golden, protocol null/absent round-trip, deterministic null/slave selection, null exact/adjacent/fallback, cleanup and a scripted null-master/valid-slave heartbeat peer. Full Rust workspace, exact all-target/all-feature Clippy, fmt/diff, comparator/manifest and pinned offline Maven verify exited 0 after the migration.
