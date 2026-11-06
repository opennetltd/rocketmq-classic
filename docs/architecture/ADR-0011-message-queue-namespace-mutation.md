# ADR-0011: Preserve Java MessageQueue namespace mutation semantics

Status: implemented for the invoked Java 5.5.1 branches; broader Phase 6 API reconciliation remains open.

## Source contract

Pinned `ClientConfig.queuesWithNamespace(Collection<MessageQueue>)` returns the same collection. It resolves the namespace first, then returns without iterating when the namespace is null or empty. With a non-empty namespace it iterates and mutates each existing `MessageQueue` through `setTopic`; a null collection throws `NullPointerException`, and a null element throws after any earlier queues have already been changed. `MessageQueue` exposes setters for topic, broker name, and queue ID, including nullable strings.

## Rust representation

`ClientConfig::queues_with_namespace` accepts and returns `Option<&mut [Option<MessageQueue>]>`. The optional slice and optional rows represent Java null collection/element inputs; the returned borrow preserves collection identity. The method skips iteration for null/empty namespaces, changes only each queue's topic, and returns typed `NullQueueCollection` or `NullMessageQueue` errors at the matching failure point. Earlier row mutations remain visible after a null-row error. `MessageQueue` exposes `set_topic`, `set_broker_name`, and `set_queue_id` with nullable strings.

## Evidence

The pinned runtime fixture captures six `queuesWithNamespace` cases, including null collection, empty namespace with a null row, partial mutation before failure, already-prefixed topics, and nullable topic. `message_queue_setters_match_invoked_java` compares setter state against the runtime; `queues_with_namespace_matches_invoked_java_collection_and_null_behavior` compares outcomes, collection identity, and all rows. Client-config fixture SHA-256: `4c0b831de2f3139b5be32e59fede60d639296e8dbffa20a9924c08fbabdab4bb`.
