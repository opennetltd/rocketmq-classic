# ADR-0039: Preserve nullable Java route decode results

Status: Accepted

## Context

Pinned Java 5.5.1 `MQClientAPIImpl.getTopicRouteInfoFromNameServer` decodes a successful route response through `TopicRouteData.decode(body, TopicRouteData.class)`. The inherited `RemotingSerializable` helpers return null for null input and JSON `null`; `decodeList` also returns null for null input and preserves null list members. Rust's original route decoder returned a concrete `TopicRouteData`, so it could not represent these successful null results.

The route API must distinguish a JSON body containing `null` from a missing response body. Java utility null semantics apply to the former; the native RPC adapter continues to report a missing body as `MalformedResponse`.

## Decision

Both route DTOs expose nullable inherited-utility adapters: `java_decode_nullable`, `java_decode_list_nullable`, and `java_from_json_nullable`. They represent a null root with `Option`, and list members with `Option` as well. The route RPC methods return `Result<Option<TopicRouteData>, RpcError>` and decode through `java_decode_nullable`.

`ClientRuntime::update_route` returns `false` immediately when the successful API response has no route DTO. It does not publish or replace a cached route. The null-topic member of `update_all_topic_routes` treats a null route as a no-op; a nonnull route for a null topic remains an invalid-route error.

## Consequences

Callers of the route RPC API handle the successful nullable result explicitly. Non-success response codes, missing response bodies, malformed JSON, and transport failures retain their existing error categories. A route refresh that receives JSON `null` preserves its last coherent cached snapshot.

## Evidence

The pinned Java fixture directly invokes decode/fromJson with null input and JSON `null`, decodeList with null input and `[null]`, on both `TopicRouteData` and `TopicQueueMappingInfo`, and records the corresponding null roots/list members. Route fixture SHA-256: `55ff6cf477d53e78f5715294613e3a98d21796b729118a04feb999b213c17245`.

Oracle commits: `5eaccba`, `f007a4a`, `c00e1a8`. Regression commits: `ffc5c8c`, `722b5bd`, `8420373`. Implementation commit: `bdfa5cb`. Test coverage follow-up: `95cda6c` makes the null-topic request in `all_topic_refresh_unions_registered_topics_and_continues_after_rejection` receive JSON `null`; it verifies the union continues and the nullable result is a no-op. Full workspace/all-target tests, strict Clippy, pinned offline Maven verify (manifest22), comparator self-test, formatting, and diff checks pass. The per-topic loopback runtime test verifies that a JSON-null refresh returns false and retains the same cached snapshot allocation.

This closes the invoked nullable route decode and refresh cases only. Other route DTO/input combinations and the full Phase 6 source/API/field/branch reconciliation remain open; P6-V04 remains PENDING_PHASE_19.
