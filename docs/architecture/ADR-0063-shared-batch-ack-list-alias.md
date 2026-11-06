# ADR-0063: Preserve BatchAck request List alias identity

- Status: Accepted
- Date: 2026-10-10

## Context

Pinned RocketMQ 5.5.1 `BatchAckMessageRequestBody.setAcks` stores the supplied mutable List, and `getAcks` returns that same object. Mutations through either reference are visible through the other. The class inherits identity-based `equals` and `hashCode` from `java.lang.Object`; equal field contents do not make two distinct body instances equal.

The Rust body previously owned `Option<Vec<Option<BatchAck>>>`. Its getter could lend that vector for mutation, but moving a caller's vector into the setter did not retain a caller-visible alias. Deriving `PartialEq` and `Eq` also gave the Rust body structural value equality unlike Java.

## Decision

Represent `acks` as `Option<SharedValue<Vec<Option<BatchAck>>>>`. The setter stores the shared handle and the getter clones it, so `ptr_eq` identifies the same outer List and mutation through either handle updates the same contents. The custom Fastjson-compatible deserializer wraps decoded Lists in `SharedValue`; serde serialization preserves the existing nullable List and JSON array wire representation.

Remove `PartialEq` and `Eq` from `BatchAckMessageRequestBody` and keep the type without `Hash`, matching Java's identity-based body semantics at the Rust trait boundary. `BatchAck` element and BitSet object aliases were still open at this List-only checkpoint; they are now preserved under [ADR-0064](ADR-0064-shared-batch-ack-nested-aliases.md).

## Consequences

The outer `acks` List identity and both mutation directions now match the pinned Java API. Core serialization and RPC behavior retain the same body wire shape. Callers use `SharedValue::new` to create a List handle and `with`/`with_mut`/`snapshot` for safe access. This correction closed the outer List alias. The later [ADR-0064](ADR-0064-shared-batch-ack-nested-aliases.md) closes the contained element and BitSet aliases; broader Phase 6 API and source reconciliation remains incomplete, and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Direct pinned Java invocation: `batchAckApiCase` in `fixtures/runtime/lookup.json` records assigned-list identity, caller/getter mutation visibility, Object equality/hash declaring classes, and same-instance versus equal-content distinct-instance outcomes.
- Rust differential: `batch_ack_public_methods_match_invoked_java` checks pointer identity, both mutation directions, list contents, and identity-method observations.
- Implementation and fixture commit: `a32d00f7`.
- Checkpoint `fixtures/runtime/lookup.json` SHA-256: `0f0c32b433f81c717aab9fe6bcb4767466eacb4e08343d8c76b3c965dc9e1654`.
- Validation: `cargo test -p rocketmq-classic-protocol --locked`; full `cargo test --workspace --all-targets --locked` with local socket access; `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; pinned offline Maven verify (22 scenarios and Java cross-decodes); comparator self-test; manifest validation (22 scenarios); formatting and diff checks.
- Oracle uses pinned RocketMQ 5.5.1 Maven artifacts; the requested Java source checkout was unavailable.
