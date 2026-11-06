# ADR-0064: Preserve BatchAck element and BitSet aliases

- Status: Accepted
- Date: 2026-10-10

## Context

Pinned RocketMQ 5.5.1 stores the exact `BatchAck` instance assigned as a List member. A caller-held BatchAck and the object returned through the body List observe each other's mutations. `BatchAck.getBitSet()` likewise returns the exact mutable `BitSet` supplied to `setBitSet`. Reflection confirms `BatchAck` inherits identity-based `equals` and `hashCode` from `Object`; equal property values do not make distinct BatchAck instances equal.

The Rust DTO previously stored List members as `BatchAck` values and its `bit_set` field as a value-owned BitSet. This matched values and wire encoding but lost caller-held object identity. Deriving structural `PartialEq` and `Eq` for BatchAck also differed from Java.

## Decision

Represent the body field as `Option<SharedValue<Vec<Option<SharedValue<BatchAck>>>>>` and BatchAck's BitSet property as `Option<SharedValue<BitSet>>`. The existing body List handle, cloned per-element handles, and cloned BitSet handles preserve the corresponding assigned-object identities; caller/getter mutation uses `with_mut`. Java-compatible decoding wraps each decoded BatchAck and BitSet. Serde serialization keeps the existing compact BatchAck fields, nullable rows, and Base64 BitSet representation.

Remove structural `PartialEq` and `Eq` from BatchAck and keep it without `Hash`. Keep the value-oriented `BitSet` operations and independent snapshots; `SharedValue::clone` clones object identity, while `snapshot` creates a value copy. The previous borrowing `get_bit_set_mut` accessor is replaced by the shared handle returned from `get_bit_set`.

## Consequences

The BatchAck body now matches Java alias behavior for the outer List, each BatchAck element, and each assigned BitSet. Java identity-method observations are retained in the fixture; Rust makes no structural DTO equality claim. Compact JSON and Base64 wire representations remain unchanged. Other Phase 6 DTO aliases, source/API/input reconciliation, and P6-V04 remain open.

## Evidence

- Direct pinned Java invocation: `batchAckApiCase` in `fixtures/runtime/lookup.json` records BatchAck Object method declarations/outcomes, assigned BatchAck element identity, mutation through both references, and BitSet assigned-object identity/mutation.
- Rust differential: `batch_ack_public_methods_match_invoked_java` verifies list, element, and BitSet pointer identity and both mutation directions.
- Implementation and fixture commit: `7ba69080`.
- Current `fixtures/runtime/lookup.json` SHA-256: `e2275802581a7a8c9d5271349377bf5b347d59d45af269e8e6569f1f682674b8`.
- Validation: `cargo test -p rocketmq-classic-protocol --locked`; full `cargo test --workspace --all-targets --locked` with local socket access; `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; `cargo check --workspace --all-targets --locked`; pinned offline Maven verify (22 scenarios and Java cross-decodes); comparator self-test; manifest validation (22 scenarios); formatting and diff checks.
- Oracle uses pinned RocketMQ 5.5.1 Maven artifacts; the requested Java source checkout was unavailable.
