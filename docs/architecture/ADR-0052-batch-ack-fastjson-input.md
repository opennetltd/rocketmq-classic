# ADR-0052: Match BatchAck Fastjson input and defaults

- Status: Accepted
- Date: 2026-10-09

## Context

BatchAckMessageRequestBody and BatchAck used derived Serde decoding, which rejects inputs accepted by pinned RocketMQ 5.5.1 Fastjson and does not preserve Java constructor defaults. The remoting artifact defines acks as a Java List, primitive numeric fields default to zero, string fields use Fastjson coercion, and the BitSet codec uses Base64 with empty values omitted on encoding.

## Decision

Use Fastjson-compatible last-property-wins decoders for both DTO levels. Preserve nullable rows in acks, accept the observed singleton-object and Boolean-to-null-list conversions, apply Java string and primitive numeric coercions/defaults, and decode the BitSet through its Base64 adapter. Omit null and empty BitSets when encoding.

The 111-case Java corpus invokes BatchAckMessageRequestBody.decode and encode. It covers seven root shapes, each JSON token kind for brokerName, acks, and every compact BatchAck field, list null/singleton/duplicate states, duplicate properties, unknown fields, and Base64 inputs. Java accepts unpadded standard Base64 (`"BQ"` and `"BQE"`) and re-encodes it canonically; it also ignores unused low pad bits (`"BR=="`, `"BFB="`) and normalizes their output. It rejects partial/excess padding, a one-character input, whitespace, URL-safe alphabet characters, and invalid symbols. Rust uses Java-compatible trailing-bit tolerance while retaining strict padding and alphabet checks; rejected forms remain errors.

## Consequences

The tested Java 5.5.1 input and encode behavior now matches the native body DTO. `BatchAckMessageRequestBody.acks` uses `Option<SharedValue<Vec<Option<SharedValue<BatchAck>>>>>` to preserve Java null List members, the assigned List alias, and each contained BatchAck object alias. `BatchAck.bit_set` is also a shared handle. SharedValue serialization preserves the existing JSON array/null and Base64 wire shapes. This DTO boundary does not change the Phase 6 PARTIAL status or P6-V04 ownership.

## Evidence

- Direct Java input corpus: `batchAckBodyInputCases` in `fixtures/runtime/lookup.json` (111 cases); SHA-256 at this continuation `98bc999e3880f25eefc1980ff55eeec7dac9264ddbec98c7b0db669fd6cdce82`. The later `batchAckApiCase` extends the aggregate lookup fixture; its current public-method evidence is recorded in [ADR-0053](ADR-0053-batch-ack-public-api.md).
- Oracle and fixture commit: 58ad8480; implementation and Rust differential: 381b6edf.
- Base64 continuation: Java oracle/fixture `c34f9dba`; red native regression `0350546f`; decoder fix `cc3ebc70`. Whitespace boundary extension: Java oracle/fixture `32ce689a`; native corpus assertion `238923da`. Pad-bit/padding extension: Java oracle/fixture `63dfa47f`; red native regression `ff888cff`; decoder fix `2677a08b`.
- Continuation validation: focused Java fixture verification via pinned offline Maven; `batch_ack_body_inputs_match_invoked_java_fastjson` and all three focused BatchAck tests; strict protocol-crate Clippy. The complete workspace and Phase 6 remain under the broader audit.
- Validation: focused BatchAck differential; cargo test --workspace --all-targets --locked; strict all-target/all-feature Clippy; pinned offline Maven verify (fixture drift and Java cross-decodes, 22 RPC scenarios); cargo fmt --all --check; git diff --check.
- Java behavior was invoked from pinned RocketMQ 5.5.1 artifacts; no alternate Java source checkout was used.
