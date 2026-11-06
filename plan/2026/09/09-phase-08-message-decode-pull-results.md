# Plan: Phase 8 — Message binary decode and pull results

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 8; execution index: [`README.md`](README.md).
> Translation: [`09-phase-08-message-decode-pull-results.zh-tw.md`](09-phase-08-message-decode-pull-results.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## Execution contract

- Port **all Java client 5.5.1 settings, features and control flows**, including defaults, validation, callbacks, errors, retries and lifecycle. Map Java source/method/configuration to Rust and unit-test IDs in both directions; no extra public Rust setting/feature and no missing Java client capability. Rust naming/ownership may differ, not observable semantics. Internal test seams stay private.
- Implement the **client only**. Java client 5.5.1 remains the oracle; **RocketMQ server V5_3_4 (5.3.4)** is the production/local Docker baseline, not the client source baseline. A server limitation does not authorize deleting a client feature; compare both clients against the same server and retain source/fixture coverage for unavailable success paths.
- **Phase 0–18 primarily use unit tests**, supplemented by Java golden/offline differential tests and small scripted loopback tests where transport requires them. No live Broker or Docker is a phase prerequisite. Broker, TLS/auth cluster, cross-language, end-to-end and server stress execution belongs to [Phase 19](20-phase-19-docker-local-validation.md); non-local checks become owner-executed manual items there. Phase acceptance is not final compatibility/release acceptance.

## 1. Goal and scope

Decode Broker message records and process pull results without scheduling consumption or committing offsets.

## 2. Prerequisites and current-state analysis

Phases 0–7 are accepted. Reuse MessageExt, property/flag constants, compression backends and raw PullResultExt; create the consumer crate only for result processing now.

- Read/complete first: [Phase 7](08-phase-07-producer-core.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `MessageDecoder`
- `MessageExt`
- `MessageSysFlag`
- `PullResultExt`
- `PullAPIWrapper.processPullResult`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-common/src/message/decoder.rs
crates/rocketmq-classic-common/src/compression/
crates/rocketmq-classic-consumer/Cargo.toml
crates/rocketmq-classic-consumer/src/pull/result.rs
crates/rocketmq-classic-common/tests/message_decode.rs
fixtures/messages/
```

Separate decode_one(record, options, limits) from decode_many(body, options, limits). Bound each record by its declared store size and require forward progress. Options preserve Java decodeReadBody/decodeDecompressBody behavior. Limits cover input record, decompressed bytes and aggregate result size; invalid/oversized data returns a structured error before unbounded allocation. Do not confuse store message format, batch-producer format and Remoting framing.

## 4. Ordered implementation tasks

### 4.1 P8-01 — Freeze the record layout before parsing

Generate Broker/Java binary fixtures and a source-derived field-width/order table, including magic/version variants, CRC, offsets, timestamps, host flags, reconsume count, prepared offset, body, topic and properties. Parse with a bounded cursor and checked arithmetic. Handle IPv4/IPv6 born/store hosts independently, topic-length variants and concatenated records; reject unknown magic, truncated fields and impossible lengths. Preserve CRC representation; verify CRC only according to the Java path/configuration, not an invented default.

### 4.2 P8-02 — Honor body and compression options

Decode all compression types used by the pinned client and supported flags; unknown compression is an explicit error when decompression is requested. Stream decompression through a byte cap to prevent decompression bombs. If body reading is disabled, advance over body without allocating it; if decompression is disabled, preserve compressed bytes and flags. Fixture-test Java message-ID construction from hosts/offsets separately from producer unique IDs.

### 4.3 P8-03 — Attach decoding to pull outcomes

Consume Phase 5 PullResultExt only after its status is mapped. FOUND decodes messages; NO_NEW_MSG/NO_MATCHED_MSG/OFFSET_ILLEGAL preserve next/min/max offsets and suggested broker even with no body. Apply Java wrapper enrichment such as broker/queue metadata and min/max properties where source requires it. Retain raw bytes only if needed by the public result; otherwise release them after processing to avoid duplicate buffers.

### 4.4 P8-04 — Implement only the correct post-pull filtering

Port tag filtering and class-filter-related behavior that actually occurs in PullAPIWrapper. SQL92 remains Broker-side. Do not attempt to execute arbitrary Java class-filter bytecode in Rust; port every client-side public class-filter API/configuration and its source-defined registration/upload/filter-server flow; server-side Java execution remains outside the client implementation. A locally filtered batch may be empty while nextBeginOffset still advances to the Broker-provided value.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Run these checks without Docker or a live Broker; use Java fixtures, controlled clocks and scripted peers. Shared commands and evidence rules are in the execution index.

- [x] **P8-V01** — Java-decoded JSON equals Rust for single/multiple records, all supported magic/host/compression combinations and body-option combinations.
- [x] **P8-V02** — Truncate valid records at every byte; mutate sizes/magic/properties and use oversized decompression output: no panic, overread or unbounded allocation.
- [x] **P8-V03** — FOUND/empty-filter/no-new/no-match/illegal-offset preserve Java next-offset and suggested-broker semantics.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-common --test message_decode --locked
cargo test -p rocketmq-classic-consumer --locked
```

## 6. Acceptance and handoff

Phase 9 receives decoded pull results with explicit limits and no hidden offset mutation. Add all new binary fixtures and normalization rules to the evidence manifest.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 9](10-phase-09-pull-offset-store.md).
