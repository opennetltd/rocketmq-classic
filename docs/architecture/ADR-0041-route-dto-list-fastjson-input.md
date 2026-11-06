# ADR-0041: Match Java route DTO list inputs

Status: Accepted

## Context

`TopicRouteData.queueDatas` and `brokerDatas` are Java `List<QueueData>` and `List<BrokerData>` fields decoded with Fastjson 2.0.63. Java accepts an object in either field and wraps it in a singleton list; array values may also contain null rows. Rust modeled nullable rows and lists, but the derived `Vec` decoder accepted only arrays.

## Decision

Keep the public Rust shapes `Option<Vec<Option<QueueData>>>` and `Option<Vec<Option<BrokerData>>>`. Decode each field from its raw token: preserve null as no list, retain null array members, parse array members independently, and wrap an object as one non-null member. Reject scalar field values and non-object, non-null list rows. Parsing each raw nested DTO token retains that DTO's Fastjson field coercions and duplicate-property handling.

The shared `serde_fastjson::deserialize_optional_nullable_vec_or_single` helper also serves the Lite subscription control body, which has the same Java List/object/null boundary. This does not claim parity for uninvoked list element shapes or Java exception classes.

## Evidence

Four direct pinned Java 5.5.1 cases cover singleton objects for both route lists and null rows for both arrays. The native 121-case route DTO differential failed on `queueDatas` receiving an object; after the fix it matches all 125 direct cases. Oracle/test commit `05fa719`; shared implementation commit `399dbc1`; route fixture SHA-256 `a733fa2d1a13e8aed4237bd2e848b1908b262dbc9f724c0e52b6b60d56495b77`.

Focused route and Lite differentials, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify with deterministic fixture checks (22 scenarios), differential comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. Whole Phase 6 source/API/input reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.
