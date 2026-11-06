# ADR-0056: Remove the non-Java unlock response body

- Status: Accepted
- Date: 2026-10-09

## Context

The Rust protocol crate exported `UnlockBatchResponseBody`, but the pinned RocketMQ 5.5.1 client, common, and remoting artifacts contain no class with that name. The Rust type had no internal call sites. Java's `LockBatchResponseBody` is the shared response DTO: both lock and unlock response fixtures encode an instance of it.

## Decision

Remove the unused Rust-only `UnlockBatchResponseBody` public DTO. Keep `LockBatchResponseBody` as the typed representation for both response paths. Preserve the existing Java `body-unlock-response` fixture, which verifies the shared DTO with unlock request code 42.

## Consequences

This closes a reverse-export mismatch. Existing lock and unlock wire fixture checks remain valid, and the protocol crate no longer suggests a Java response class that does not exist. Phase 6 remains PARTIAL because other source/API/input and final plan reconciliation is open; P6-V04 stays `PENDING_PHASE_19`.

## Evidence

- Pinned artifact inventory: `jar tf` over the RocketMQ 5.5.1 client/common/remoting jars contains no `UnlockBatchResponseBody.class`.
- `javap -public` shows `LockBatchResponseBody` as the shared DTO with a no-argument constructor and `getLockOKMQSet` / `setLockOKMQSet` methods.
- `compat/java-reference/src/main/java/compat/Main.java` constructs one `LockBatchResponseBody` and writes it for both lock response code 41 and unlock response code 42.
- No Rust internal references remained; `cargo test -p rocketmq-classic-protocol --all-targets --locked` passes (106 unit tests and all protocol integration tests).
- Implementation commit: `a5fb87ce`.
- Pinned Java artifact version: RocketMQ 5.5.1.
