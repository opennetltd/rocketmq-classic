# ADR-0077: Match TopicRouteData shallow-copy aliases

- Status: Accepted
- Date: 2026-10-10

## Context

The pinned RocketMQ 5.5.1 `TopicRouteData` copy constructor and `cloneTopicRouteData()` create new outer queue/broker lists and maps while retaining their mutable nested values. Direct Java cases show that QueueData rows, BrokerData rows, TopicQueueMappingInfo values, and nested filter-server lists have the same object identity in the source and shallow copy. `deepCloneTopicRouteData()` copies the nested values instead. An actual route refresh on an unstarted runtime confirms that the raw-table route and published route have separate outer containers but share the same nested queue/broker DTOs and filter-server lists.

The native compatibility fixture had already recorded these outcomes, but the Rust differential compared only copied DTO values. `java_copy_constructed()` also used the Rust `Clone` implementation, which deep-copied nested values and did not preserve Java's alias behavior.

## Decision

`TopicRouteData::java_copy_constructed()` creates new outer `SharedValue` collections by cloning their contents; the collection contents contain shared handles, so nested mutable references remain identical. `clone_topic_route_data()` uses the same shallow outer-container behavior after applying Java's null-container checks. `deep_clone_topic_route_data()` retains its independent nested copies. Route-cache refresh uses `java_copy_constructed()` for the published route, matching the actual Java runtime boundary. The standard Rust `Clone` trait remains a deep value copy, so Rust callers still have an explicit non-aliasing clone operation.

The Java fixture records identity for each outer collection and representative nested object/list. Rust checks `ptr_eq` for the corresponding handles across all eight input cases.

## Consequences

Mutating a nested DTO or filter list through the Java-compatible copy reaches the source handle, matching Java. Mutating the copied outer list/map does not alter the source container. Callers that need fully independent data use `deep_clone_topic_route_data()` or Rust `Clone`.

This closes the tested `TopicRouteData` copy/clone identity boundaries only. Other route source/API/input questions and Phase 6 reconciliation remain open; P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Java oracle: `RuntimeRouteFixtures.topicRouteDataCloneCases`.
- Copy/clone oracle and implementation commits: `d437ab34` / `8b69c09e`.
- Runtime publication oracle and implementation commits: `827f6510` / `a87e3726`.
- Current `fixtures/runtime/route.json` SHA-256: `d01c51ec40fdcf96931dd0191271a2b23fc396ec00f18f949aa629e678172e2b`.
- Native differentials: `topic_route_data_copy_and_clone_methods_match_invoked_java_values_and_failures` and `accepted_empty_route_keeps_route_presence_separate_from_publish_queue_ok`.
- Validation: full workspace/all-target tests with loopback access; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios and Java cross-decodes); comparator self-test; manifest validation (22 scenarios); formatting and diff checks.
