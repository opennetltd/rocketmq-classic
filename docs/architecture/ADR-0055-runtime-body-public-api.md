# ADR-0055: Map remaining runtime body public methods

- Status: Accepted
- Date: 2026-10-09

## Context

The pinned RocketMQ 5.5.1 body DTOs expose bean methods and diagnostic text that were not covered by the existing input and runtime-flow checks. The Rust DTOs had the corresponding data but some had no named getter/setter methods or Java-shaped `Display` implementation.

## Decision

Add Rust snake_case accessors for `CheckClientRequestBody`, `GetConsumerListByGroupResponseBody`, `LockBatchRequestBody`, `UnlockBatchRequestBody`, `LockBatchResponseBody`, and `ConsumeMessageDirectlyResult`. Add `Display` implementations for lock/unlock request bodies and direct-consume results.

Keep Java constructor defaults: nullable check-client fields; nullable consumer IDs; empty lock/unlock queue Sets with false `onlyThisBroker`; an empty lock response Set; and direct-consume defaults of `order=false`, `autoCommit=true`, null result/remark, and zero elapsed milliseconds. Preserve the invoked Java strings for the three `Display` implementations, including nested `MessageQueue` text.

This ADR's initial accessors used owned values plus mutable borrows. The alias status has since been superseded for `CheckClientRequestBody.subscriptionData` by [ADR-0060](ADR-0060-shared-java-dto-reference-aliases.md) and for `GetConsumerListByGroupResponseBody.consumerIdList` by [ADR-0061](ADR-0061-shared-consumer-list-alias.md). The three lock Set containers and their nested `MessageQueue` elements use shared handles under [ADR-0062](ADR-0062-shared-lock-set-aliases.md) and [ADR-0067](ADR-0067-shared-lock-set-element-aliases.md). `BatchAckMessageRequestBody.acks` and its nested objects use shared handles under [ADR-0063](ADR-0063-shared-batch-ack-list-alias.md) and [ADR-0064](ADR-0064-shared-batch-ack-nested-aliases.md), and `QueryAssignmentResponseBody.messageQueueAssignments` and its elements retain shared aliases under [ADR-0065](ADR-0065-shared-query-assignment-set-alias.md) and [ADR-0068](ADR-0068-shared-query-assignment-elements.md). Each `MessageQueueAssignment` also retains its nested queue and attachments aliases under [ADR-0066](ADR-0066-shared-assignment-nested-aliases.md). The response Set and each assignment element now retain shared aliases; other DTO fields remain under audit.

## Consequences

The public fields, defaults, nullable setters, and tested diagnostic strings are mapped for these six DTOs. The wider Phase 6 public API and source/input audit remains incomplete; this ADR does not claim phase acceptance. P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Direct pinned Java calls: `checkClientBodyApiCase`, `consumerListApiCase`, `lockBatchRequestApiCase`, `unlockBatchRequestApiCase`, `lockBatchResponseApiCase`, and `consumeDirectResultApiCase` in `fixtures/runtime/lookup.json`.
- Rust differential: `remaining_runtime_body_public_methods_match_invoked_java`.
- Original method mapping: Java oracle `7e1155f8`, Rust implementation `bca4329c`.
- Later shared-alias implementations: CheckClient `a732974a`; consumer list `456849fb`; lock Set containers `045b46e7`; lock Set elements `625122ee`; BatchAck List `a32d00f7`; BatchAck element/BitSet `7ba69080`; assignment Set `dbaed386`; assignment elements `d312bb6c`.
- Current aggregate `fixtures/runtime/lookup.json` SHA-256: `a4d4c85dc1ab17a240906f0e48348ebf2644c31bfcdd987d958fc1368b0e20de`.
- Validation: workspace/all-target tests; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes); comparator self-test and manifest22; format and diff checks.
- Oracle uses pinned RocketMQ 5.5.1 artifacts. The requested source checkout at `/Users/nanashi07/Projects/3rd/rocketmq` was unavailable.
