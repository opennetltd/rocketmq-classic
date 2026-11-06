# Plan: Phase 4 — Typed request headers and protocol bodies

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 4; execution index: [`README.md`](README.md).
> Translation: [`05-phase-04-typed-headers-bodies.zh-tw.md`](05-phase-04-typed-headers-bodies.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## Execution contract

- Port **all Java client 5.5.1 settings, features and control flows**, including defaults, validation, callbacks, errors, retries and lifecycle. Map Java source/method/configuration to Rust and unit-test IDs in both directions; no extra public Rust setting/feature and no missing Java client capability. Rust naming/ownership may differ, not observable semantics. Internal test seams stay private.
- Implement the **client only**. Java client 5.5.1 remains the oracle; **RocketMQ server V5_3_4 (5.3.4)** is the production/local Docker baseline, not the client source baseline. A server limitation does not authorize deleting a client feature; compare both clients against the same server and retain source/fixture coverage for unavailable success paths.
- **Phase 0–18 primarily use unit tests**, supplemented by Java golden/offline differential tests and small scripted loopback tests where transport requires them. No live Broker or Docker is a phase prerequisite. Broker, TLS/auth cluster, cross-language, end-to-end and server stress execution belongs to [Phase 19](20-phase-19-docker-local-validation.md); non-local checks become owner-executed manual items there. Phase acceptance is not final compatibility/release acceptance.

## 1. Goal and scope

Replace known header string-map construction with typed codecs, preserving raw command fields and exact Java validation behavior. This phase performs no network calls.

## 2. Prerequisites and current-state analysis

Phases 0–3 are accepted; Phase 2 already placed header traits and ProtocolError in protocol. Extend those traits rather than creating a protocol→remoting dependency.

- Read/complete first: [Phase 3](04-phase-03-remoting-transport.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `remoting protocol/header and protocol/body packages`
- `SendMessageRequestHeaderV2 conversion helpers`
- `CommandCustomHeader validation`
- `MQClientAPIImpl request construction and response decoding`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-protocol/src/header/
crates/rocketmq-classic-protocol/src/body/
crates/rocketmq-classic-protocol/src/rpc.rs
crates/rocketmq-classic-protocol/tests/headers.rs
docs/compatibility/REQUEST_CODES.md
fixtures/remoting/
```

Create a per-operation table: code, request header, response header, body codec, required/optional fields, oneway support and fixture IDs. Derive requiredness from both Java header validation and actual call sites, not annotations alone. Missing, empty, zero and false are different cases. All Java names, including V2 short keys, must be fixture-backed. Each conversion returns an error naming the field and expected type without exposing secrets.

## 4. Ordered implementation tasks

### 4.1 P4-01 — Complete the shared conversion contract

Use the Phase 2 encode/decode traits. Add only repeated field-parsing helpers, with Java-compatible boolean/numeric string handling and overflow checks. Decode from a borrowed HeaderMap; keep unknown fields in the original RemotingCommand. Test header re-encoding into a copy of the raw map so known changes do not delete unknown fields.

### 4.2 P4-02 — Implement the first RPC wave end-to-end

In order: route, send V1/V2, batch send, pull, query/update offset, heartbeat, unregister, consumer list and lock/unlock. Add response headers as well as request headers: send IDs/queue offset, pull next/min/max/suggested broker and offset-query result are required by Phase 5. Reuse Phase 1 route/heartbeat bodies. Capture V1↔V2 field mapping and optional send/batch fields from Java conversions, not guessed letter assignments.

### 4.3 P4-03 — Add consumer extension wire models

Implement send-back, assignment/request-mode, Lite/POP/ACK/change-invisibility and notification/polling structures actually used by the pinned client. Follow the source call graph rather than assuming LITE_PULL_MESSAGE is the normal DefaultLitePullConsumer path. Include batch ACK only when a real client call site exists. Record unsupported public exposure separately from wire-model completeness.

### 4.4 P4-04 — Add transaction, reply and runtime inbound models

Implement end/check-transaction, reply-send/push headers and body shapes. Add notification, reset-offset, consumer-status and direct-consume models needed for later processor activation; no handler behavior yet. Keep correlation/reply property names in the common constant module. Distinguish ext-field serialization from message binary bodies and JSON bodies.

### 4.5 P4-05 — Audit every known header against the oracle

Generate positive and negative Java fixtures for every implemented header. Use table-driven Rust tests for canonical maps, absent optionals, missing requireds, bad numbers, boolean edge cases and signed boundaries. Update REQUEST_CODES.md to distinguish models ready from RPC implemented. Do not introduce code generation unless a small audited table clearly eliminates duplicated definitions.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Run these checks without Docker or a live Broker; use Java fixtures, controlled clocks and scripted peers. Shared commands and evidence rules are in the execution index.

- [x] **P4-V01** — Each header has a Java→Rust fixture and a Rust→Java decoding assertion, including response headers.
- [x] **P4-V02** — Unknown ext fields survive typed modification; all invalid/missing required fields fail at the protocol boundary.
- [x] **P4-V03** — V1/V2 equivalent send headers normalize identically; JSON bodies preserve semantic collection and omission rules.

Acceptance evidence: the [Phase 4 compatibility matrix](../../../docs/compatibility/REQUEST_CODES.md) records the exhaustive header inventory, 38 Java-decoded Rust header frames, 14 semantic body frames, and the ACK Lite source disposition. The [Phase 4 closure record](../10/01-phase-04-compatibility-closure.md#7-completion-record) and append-only [execution log](../../../rlogs/2026/09/05-phase-04-typed-headers-bodies__20261004T021500Z.rlog) contain the pinned fixture provenance and passing command results. This accepts the Phase 4 typed wire-model gate; it does not claim later RPC or live Broker coverage.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-protocol --test headers --locked
cargo test -p rocketmq-classic-remoting --locked
mvn -f compat/java-reference/pom.xml verify
```

## 6. Acceptance and handoff

Phase 5 can build all primary requests without ad-hoc string maps. Missing optional-family evidence stays TODO/PARTIAL and blocks that family, not an unrelated basic RPC.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 5](06-phase-05-typed-rpc-api.md).
