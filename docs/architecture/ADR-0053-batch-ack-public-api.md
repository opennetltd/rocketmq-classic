# ADR-0053: Map BatchAck public methods

- Status: Accepted
- Date: 2026-10-09

## Context

The pinned RocketMQ 5.5.1 `BatchAck` API exposes a no-argument constructor, getters and setters for nine properties, and `toString()`. `BatchAckMessageRequestBody` exposes getters and setters for `brokerName` and `acks`. The Rust DTOs previously matched wire input but did not expose the corresponding method surface or Java diagnostic text.

## Decision

Expose Rust snake_case getters and setters for all nine `BatchAck` properties and both body properties. Use `Default` for Java's no-argument constructor values, and implement `Display` so `BatchAck::to_string()` matches the invoked Java output, including BitSet index formatting.

Rust stores the Java `List<BatchAck>` as `Option<SharedValue<Vec<Option<SharedValue<BatchAck>>>>>` and `BatchAck.bit_set` as `Option<SharedValue<BitSet>>`. Cloned getter handles preserve the outer List, individual BatchAck elements, and assigned BitSet object identities; `with`/`with_mut` access their shared values. Serialization retains the existing compact JSON and Base64 wire shapes. Both `BatchAckMessageRequestBody` and `BatchAck` lack structural `PartialEq`, `Eq`, and `Hash` implementations, matching Java's inherited identity-based `Object.equals` and `Object.hashCode`.

## Consequences

The invoked constructor, getter, setter, and `toString()` values match the pinned Java oracle. The setter/getter preserve the outer List, contained BatchAck instances, and assigned BitSet objects; mutation through caller and getter handles is visible through the other reference. Both DTO types use Java identity semantics rather than structural Rust equality. Other DTO aliases remain under Phase 6 reconciliation; the overall phase remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Direct pinned Java invocation: `batchAckApiCase` in `fixtures/runtime/lookup.json`, covering defaults, all property readbacks, `toString()`, body defaults and values, BatchAck and BitSet assigned-object identity, List identity and mutations in both directions, Object equality/hash declaring classes and outcomes, and null setter behavior.
- Original API oracle/implementation: `7f3a4f0e` / `c709e743`; mutable-list oracle/implementation: `51b92e77` / `f4d85af0`.
- Current aggregate `fixtures/runtime/lookup.json` SHA-256: `e2275802581a7a8c9d5271349377bf5b347d59d45af269e8e6569f1f682674b8`.

- Aggregate fixture SHA-256 at this ADR's BatchAck API checkpoint: `d16675236d7b63ddc1ffd69c80df896370479f952f0af9f961214ed7d0237632`. Later DTO method oracles update the aggregate; see [ADR-0055](ADR-0055-runtime-body-public-api.md) for the current digest.
- Validation: `cargo test --workspace --all-targets --locked`; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios, fixture drift, Java cross-decodes); `cargo fmt --all --check`; `git diff --check`.
- Oracle uses pinned RocketMQ 5.5.1 artifacts; the requested alternate Java source checkout was unavailable.

## Mutable BitSet getter continuation

Historical getter checkpoint, superseded by [ADR-0064](ADR-0064-shared-batch-ack-nested-aliases.md): the pinned Java `BatchAck.getBitSet()` returns the same mutable `BitSet` passed to `setBitSet`. At that checkpoint Rust used `get_bit_set_mut()` over its owned value and did not retain Java object identity.

- Direct Java evidence: `batchAckApiCase` in `fixtures/runtime/lookup.json`; Rust differential: `batch_ack_public_methods_match_invoked_java`.
- Oracle/test commit: `bd3d2e71`; implementation commit: `d1dc7dfd`.
- Checkpoint aggregate `fixtures/runtime/lookup.json` SHA-256: `abfe31546b68799aced43ff4f631f5dea37a4c14efac1dfc60a1ed637a22b970`.
- Full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Rust does not preserve identity with the caller-owned Java `BitSet`; Phase 6 remains PARTIAL and P6-V04 stays PENDING_PHASE_19.

## Inherited RemotingSerializable API continuation

The pinned BatchAckMessageRequestBody extends RemotingSerializable. The Rust DTO now exposes Fastjson-compatible instance adapters for compact/pretty JSON, byte encoding, single-value decode, list decode, and fromJson, with nullable forms where Java returns null. Optional-value adapters map the inherited static encode(Object) and toJson(Object, boolean) null results. The shared crate-private Fastjson writer is used for key ordering and Java tab indentation.

The invoked Java fixture records both DTO constructors, all 19 declared BatchAck methods, the four declared body methods, and the body's 13 callable methods including inherited overloads. Three Java cases cover defaults, explicit-null fields, and a populated body with a null list member. The existing 111 input cases now exercise the Rust nullable Java decoder. The feature-varargs overload is verified with an empty feature list; non-empty Fastjson writer features remain outside this adapter and the broader inherited-surface audit stays open.

- Pinned source: BatchAckMessageRequestBody.java SHA-256 11b86eb41d3d427e4400aa6741916d57f5f7d035f5b2dec3c86c56b0f2688463.
- Invoked fixture: fixtures/runtime/lookup.json SHA-256 581a2a5dad120533595717673bd395b6078d978f937ce0c888ca994ba7f81fd7.
- Focused and all-feature Protocol tests, strict Protocol Clippy, Java fixture drift verification, and full Maven verify pass for this change.

## Java-compatible BitSet value continuation

`BatchAck.bit_set` uses a shared handle around the Java-compatible Rust `BitSet` value rather than exposing raw Base64 bytes as its mutation API. It maps the functional Java surface: index and range set/clear/flip/get, boolean setter overloads, next/previous set/clear searches, `intersects`, `and/or/xor/andNot`, cardinality, length, emptiness, size/capacity, byte and long-array values, set-bit iteration, equality, Java hash/text, and canonical `toByteArray()` encoding. BatchAck JSON keeps the existing Base64 field and omits an empty bit set as before. Negative indexes and invalid ranges return typed `BitSetError` values; the direct Java oracle confirms those operations throw `IndexOutOfBoundsException`.

- Direct Java evidence covers bits 0, 2, 5, 7, 8, 63, 64, and 127; clear/flip, cardinality, length, canonical bytes, and negative-index failures.
- Oracle/test commits: `ce97d07c` and `98d26175`; implementation/test commit: `7d2f2b98`.
- `fixtures/runtime/lookup.json` SHA-256: `4ebfec17860c569431bfb2b7b952f59c83b3eb08d683b6198e4f24d6143cb400`.
- Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass.

The direct oracle covers word-boundary crossing at bits 63–65, capacity growth and clear behavior, slices, signed long-word output, Java hash/text/stream results, bulk operations, and equality independent of capacity. Rust maps byte/long array values with slices and does not preserve a Java NIO buffer cursor or identity. The assigned Java `BitSet` object identity is preserved by the current `SharedValue` handle; value snapshots remain independent. Oracle commits `4196f933`, `2953f259`, `9dd083ed`, `79328f27`, and `a5385223`; implementation/test commit `0f600297`; checkpoint lookup fixture SHA-256 `4a8f9a0b49c6e5acdcef48ee9da7bc9d81c5d28361d79f8a8400dd6636ae0719`. Whole Phase 6 API/source reconciliation remains open and P6-V04 stays PENDING_PHASE_19.
