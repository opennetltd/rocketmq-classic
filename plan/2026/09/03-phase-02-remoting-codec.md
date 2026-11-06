# Plan: Phase 2 — Remoting frame codec

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 2; execution index: [`README.md`](README.md).
> Translation: [`03-phase-02-remoting-codec.zh-tw.md`](03-phase-02-remoting-codec.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## Execution contract

- Port **all Java client 5.5.1 settings, features and control flows**, including defaults, validation, callbacks, errors, retries and lifecycle. Map Java source/method/configuration to Rust and unit-test IDs in both directions; no extra public Rust setting/feature and no missing Java client capability. Rust naming/ownership may differ, not observable semantics. Internal test seams stay private.
- Implement the **client only**. Java client 5.5.1 remains the oracle; **RocketMQ server V5_3_4 (5.3.4)** is the production/local Docker baseline, not the client source baseline. A server limitation does not authorize deleting a client feature; compare both clients against the same server and retain source/fixture coverage for unavailable success paths.
- **Phase 0–18 primarily use unit tests**, supplemented by Java golden/offline differential tests and small scripted loopback tests where transport requires them. No live Broker or Docker is a phase prerequisite. Broker, TLS/auth cluster, cross-language, end-to-end and server stress execution belongs to [Phase 19](20-phase-19-docker-local-validation.md); non-local checks become owner-executed manual items there. Phase acceptance is not final compatibility/release acceptance.

## 1. Goal and scope

Cross-decode full Remoting frames with the Java oracle, including fragmented and malicious input. Keep the codec independent of live sockets.

## 2. Prerequisites and current-state analysis

Phases 0–1 are accepted. Extend the oracle fixtures with transaction and request/reply commands now even though their public APIs arrive later.

- Read/complete first: [Phase 1](02-phase-01-common-protocol-models.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `RemotingCommand`
- `RocketMQSerializable`
- `RemotingSerializable`
- `NettyEncoder`
- `NettyDecoder`
- `FastCodesHeader`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-remoting/Cargo.toml
crates/rocketmq-classic-remoting/src/command.rs
crates/rocketmq-classic-remoting/src/error.rs
crates/rocketmq-classic-remoting/src/codec/frame.rs
crates/rocketmq-classic-remoting/src/codec/json_header.rs
crates/rocketmq-classic-remoting/src/codec/rocketmq_header.rs
crates/rocketmq-classic-protocol/src/header/mod.rs
crates/rocketmq-classic-remoting/tests/codec.rs
```

For a complete frame, total_length = 4 + header_length + body_length; the first length word is excluded. All framing integers are big-endian. Configure one maximum complete-frame size and document whether it includes the prefix (use includes-prefix internally); validate before reserve/split. Put HeaderMap, ProtocolError and header conversion traits in protocol, not remoting: Phase 4 protocol headers must never depend upward on remoting. RemotingCommand keeps the raw ext_fields map even after typed decoding.

## 4. Ordered implementation tasks

### 4.1 P2-01 — Add the command and codec errors

Create the remoting crate depending only on protocol/common and used codec dependencies. RemotingCommand owns code/language/version/opaque/flag/remark/ext_fields/body/serialization; response and oneway helpers manipulate only their own flag bits. Default language is RUST. Preserve absent versus empty remark/body where Java distinguishes them; record any wire-equivalent collapse.

### 4.2 P2-02 — Validate lengths before reading or allocating

Use checked arithmetic for total length and 24-bit header length. Reject total_length below 4, signed-negative/impossible lengths, header longer than payload, unsupported serialization and configured-limit overflow. Incomplete input is NeedMore, not a malformed frame. The encoder must also reject fields that cannot fit their Java wire widths.

### 4.3 P2-03 — Implement JSON headers

Generate exact Java field names and value types, including LanguageCode JSON representation rather than assuming the binary byte is serialized as a number. Keep unknown extFields strings. Cross-decode Java→Rust and Rust→Java; compare semantic JSON without requiring object-key order.

### 4.4 P2-04 — Implement the private header layout

Transcribe and document each field width/order from RocketMQSerializable before coding, including code/version narrowing, remark length and ext-field key/value length widths. Read inside a bounded header slice; reject truncation, invalid string encoding and inconsistent nested lengths without panic. Test large request codes against Java private-codec behavior rather than silently narrowing them; operations that cannot use this codec must use a Java-supported serialization path.

### 4.5 P2-05 — Define the shared header boundary

Define encode_fields(&mut HeaderMap) and decode_fields(&HeaderMap) in protocol with typed validation errors. Implement one small fixture-backed header as proof, leaving the complete header set to Phase 4. Typed decode borrows the raw map; it never consumes or replaces the command map. No reflection or duplicate unknown-field bag is needed.

### 4.6 P2-06 / P2-07 — Finish incremental decoding before optimization

Implement tokio-util Decoder/Encoder only if useful for the upcoming transport; BytesMut parsing itself stays testable. Exercise every byte split, multiple frames and an incomplete final frame; leave incomplete input intact. Use bounded direct buffer writes where simple. Generate equivalent Java fixtures through both the optimized FastCodesHeader path and the ordinary encoder; both must cross-decode to the same semantics, even if Rust has no separate fast path. Add a `ponytail:` comment if deferring an alternate fast-header path: golden-correct codec first, add specialization only when Phase 18 measurements justify it.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Run these checks without Docker or a live Broker; use Java fixtures, controlled clocks and scripted peers. Shared commands and evidence rules are in the execution index.

- [x] **P2-V01** — Cross-decode route/heartbeat/send/pull/offset/lock/transaction/reply fixtures under both supported serializations where the Java codec permits them.
- [x] **P2-V02** — Test length 0/1/0xFFFFFF/overflow, configured frame limit ±1, high-bit total lengths, nested truncation and unknown fields/codes.
- [x] **P2-V03** — Every fragmentation point and concatenated-frame case emits exactly the expected commands; malformed bytes never panic or cause unbounded reserve.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-remoting --test codec --locked
cargo test -p rocketmq-classic-protocol --locked
mvn -f compat/java-reference/pom.xml verify
```

## 6. Acceptance and handoff

Phase 3 receives a bounded codec and raw command type. Register Rust-generated fixture files for Java decoding in Maven verify; a Rust-only round trip is insufficient.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 3](04-phase-03-remoting-transport.md).
