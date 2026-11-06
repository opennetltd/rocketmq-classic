# ADR-0083: Preserve ProcessQueue ownership through concurrent completion

Status: implemented for local queue state; scheduler and listener integration remain open.

## Source contract

Java 5.5.1 `ProcessQueue` stores messages by queue offset, tracks cached message count and body bytes, moves messages into consume work, and reports the next offset after removal. Its queue snapshot and cached values are used by push-consumer flow control. Phase 10 strengthens the same ownership boundary so concurrent listener completion, retries, and reassignment cannot skip unresolved messages.

The Java source and test oracle are pinned to commit `efa1be59d6fc747ad220f036c86030f05f0419f9`:

- `client/src/main/java/org/apache/rocketmq/client/impl/consumer/ProcessQueue.java`, SHA-256 `41add02d3770ef5c44229c4abc279504e15a81470eb4bf2d95cd3e5027580219`.
- `client/src/test/java/org/apache/rocketmq/client/impl/consumer/ProcessQueueTest.java`, SHA-256 `67ff9bd9913b76944c3e7a75d6f219525799b135049f7c86a1b6cace9111c762`.

## Decision

- Keep each queue's messages in a `BTreeMap` keyed by queue offset. Each entry stays in that map while queued or in flight, so cached count and body bytes include listener work until its terminal result.
- Give each `ProcessQueue` an immutable assignment generation. Mutating operations require that generation and reject mismatches or a dropped assignment. The assignment owner must drop the old queue before removing or replacing it.
- Identify listener batches and mark their entries in flight under a short mutex-protected transition. Return cloned `Arc<MessageExt>` handles to the caller; no queue lock survives into callback execution.
- A duplicate for a ready offset replaces its stored message and adjusts bytes by the body-size difference. A duplicate for an in-flight offset leaves the listener-owned copy unchanged. Neither path increments cached count.
- Completion removes only terminal offsets. All other entries return to ready state for retry. The candidate commit offset is the lowest unresolved offset, or the highest observed offset plus one after the queue is drained.
- Dropping an assignment preserves its unresolved snapshot for diagnosis/redelivery and rejects late insertions, completions, and offset candidates. Lock state and timestamps are retained for Phase 12 without enabling orderly processing here.

## Evidence

`process_queue::tests` covers out-of-order insertion and completion, duplicate count/byte accounting, retained in-flight accounting, gaps, partial requeue, drained next offsets, and stale-generation rejection against a replacement queue. These are local unit tests and do not claim Broker behavior.

The proposed Phase 10 filename `ADR-0003-consumer-process-queue.md` conflicts with the existing `ADR-0003-typed-rpc-invocation-modes.md`; this decision uses the next available repository ADR number instead of replacing that record.
