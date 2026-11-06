# Pull consumer offset recovery

`PullConsumer::pull` returns decoded messages and the Broker's `next_begin_offset`; it does not advance or persist the consumer offset. The application should finish processing the whole returned batch before it records that next offset:

1. Pull at the current offset.
2. Apply the application's side effect to each message successfully.
3. Call `update_consume_offset` with `next_begin_offset`.
4. Call `persist_offset` (or `persist_consumer_offset`) to save the progress.

If the process crashes after a side effect succeeds but before the offset is persisted, it can pull that batch again after restart. This is at-least-once recovery: processing may be repeated, so application side effects should be idempotent or deduplicated. Persisting the offset before processing completes can skip messages after a crash.

The [`pull_consumer` example](../../examples/pull_consumer.rs) shows the order. It reads the stored offset, falls back to the queue's minimum offset when no offset exists (or optionally searches from `ROCKETMQ_EXAMPLE_START_TIMESTAMP_MS`), processes a `FOUND` batch, then records and persists the returned next offset. Set `ROCKETMQ_EXAMPLE_BOUNDARY=upper` to select the UPPER search boundary.

For timestamp-based starts, `search_offset` uses Java's LOWER boundary by default. Use `search_offset_with_boundary(&queue, timestamp_millis, BoundaryType::Upper)` when the upper boundary is required.
