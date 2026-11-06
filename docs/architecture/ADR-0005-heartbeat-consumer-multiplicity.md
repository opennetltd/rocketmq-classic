# ADR-0005: Preserve heartbeat consumer metadata multiplicity

Status: Accepted by owner, 2026-10-06.

## Context

Pinned Java client 5.5.1 ConsumerData inherits Object equality. Runtime
preparation creates one new object per consumer registration. Thus its Set
serializes equal metadata more than once. The earlier Rust protocol model used
HashSet with structural equality and silently discarded those entries, both
when preparing a full heartbeat and when clearing subscriptions for V2 simple
heartbeats. Actual Java preparation, native decoding and scripted wire checks
demonstrated two entries versus one.

This corrects an earlier completed public protocol contract. The owner explicitly
approved Vec on 2026-10-06; it is not a compatibility waiver.

## Decision

Change HeartbeatData.consumer_data_set to Vec<ConsumerData>. Push each registered
consumer's owned metadata once; derive the V2 simple body without collecting into
a structural set. Preserve duplicate array entries on decoding and encoding.
Keep the Java wire name consumerDataSet and all request codes unchanged.

ProducerData and SubscriptionData retain their value-based sets, matching their
Java equality behavior. ConsumerData's existing structural equality/hash helpers
remain available for value comparison, but no longer decide heartbeat membership.
Vec traversal reflects native registration snapshots, not guaranteed Java object
set iteration. Fingerprint canonicalization sorts without deduplication.

## Consequences and evidence

Rust callers constructing this public field must use Vec/push instead of
HashSet/insert. Wire shape remains an array. Vec equality is order-sensitive;
cross-runtime comparisons of multiple consumers must compare a duplicate-preserving
multiset, not a deduplicated set or arbitrary JVM ordering.

RuntimeHeartbeatFixtures adds five actual Java multiplicity cases, canonically
sorting consumer rows while retaining duplicates. Protocol round-trip and runtime
registration tests cover equal/full/simple/different/unregister paths. The V2 wire
test covers equal full/simple, changed full, then equal simple after subscription
removal. All three new checks reproduced the count mismatch before implementation.
No JVM identity emulation, public setting, worker or dependency is added.
