# ADR-0018: Match Java consumer-list response inputs

Status: Accepted

## Context

Phase 6 `getConsumerIdListByGroup` decodes `GetConsumerListByGroupResponseBody.consumerIdList` as a Java `List<String>`. The Rust response DTO and RPC method previously required non-null strings and did not model Fastjson's conversions for top-level scalar values, nested array/object elements, or null list members.

## Decision

Represent the decoded list as `Option<Vec<Option<String>>>` in the protocol DTO, typed API, and runtime lookup. Decode a JSON string as a singleton list (or an empty list for an empty string), array items with Java Fastjson string conversion, and null array items as `None`. Missing, explicit-null, object, number, and Boolean values for the field decode to `None`; other accepted values retain their Java-observed conversions. Encode null list elements as JSON null and omit a null list field.

## Evidence

`RuntimeLookupFixtures.consumerListCases` invokes the pinned Java 5.5.1 `GetConsumerListByGroupResponseBody.decode` for 13 cases covering missing/null/empty, order and duplicates, scalar and nested items, and top-level object/string/number/fraction/Boolean values. The added high-precision case confirms the list preserves the exact plain decimal `9223372036854775807.123456789012345678901` and formats exponent `1e2` as `100.0`. `consumer_list_inputs_match_invoked_java_fastjson` checks acceptance, decoded values, and re-encoded DTO shape from raw JSON input. A scripted `MQClientApi` test verifies null members survive the RPC layer.

Combined lookup fixture SHA-256 at the original 12-case update: `0c9ccc65b6c95d28d90d8de1bbac000436fe4468f1e7ebace6449ca1cf25c8e2`. Current combined lookup fixture SHA-256: `ed97d76aac400d5aabc76c05f5d309f6ba8f73a56b614320781cbcdf93f1c04f`.

Full workspace tests, strict all-target Clippy, pinned offline Maven verification, comparator/manifest checks, formatting, and diff checks pass. Rust parse errors are not asserted to reproduce Fastjson exception classes. This closes the recorded consumer-list inputs only; whole-Phase 6 source/API/input reconciliation and P6-V04 Phase 19 evidence remain open.
