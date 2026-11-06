# Plan: Phase 3 — TCP/TLS remoting transport

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 3; execution index: [`README.md`](README.md).
> Translation: [`04-phase-03-remoting-transport.zh-tw.md`](04-phase-03-remoting-transport.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## Execution contract

- Port **all Java client 5.5.1 settings, features and control flows**, including defaults, validation, callbacks, errors, retries and lifecycle. Map Java source/method/configuration to Rust and unit-test IDs in both directions; no extra public Rust setting/feature and no missing Java client capability. Rust naming/ownership may differ, not observable semantics. Internal test seams stay private.
- Implement the **client only**. Java client 5.5.1 remains the oracle; **RocketMQ server V5_3_4 (5.3.4)** is the production/local Docker baseline, not the client source baseline. A server limitation does not authorize deleting a client feature; compare both clients against the same server and retain source/fixture coverage for unavailable success paths.
- **Phase 0–18 primarily use unit tests**, supplemented by Java golden/offline differential tests and small scripted loopback tests where transport requires them. No live Broker or Docker is a phase prerequisite. Broker, TLS/auth cluster, cross-language, end-to-end and server stress execution belongs to [Phase 19](20-phase-19-docker-local-validation.md); non-local checks become owner-executed manual items there. Phase acceptance is not final compatibility/release acceptance.

## 1. Goal and scope

Provide deadline-bound invoke/oneway and inbound dispatch with exactly one terminal owner per request and explicit task shutdown.

## 2. Prerequisites and current-state analysis

Phase 2 cross-codec checks pass. Use scripted loopback peers and ephemeral test certificates for transport/TLS tests. Docker Broker/TLS profiles are created in Phase 19.

- Read/complete first: [Phase 2](03-phase-02-remoting-codec.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `NettyRemotingClient`
- `NettyRemotingAbstract`
- `ResponseFuture`
- `InvokeCallback`
- `TLS configuration and channel events`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-remoting/src/client/mod.rs
crates/rocketmq-classic-remoting/src/client/connection.rs
crates/rocketmq-classic-remoting/src/client/connection_manager.rs
crates/rocketmq-classic-remoting/src/client/pending.rs
crates/rocketmq-classic-remoting/src/client/processor.rs
crates/rocketmq-classic-remoting/src/client/tls.rs
crates/rocketmq-classic-remoting/tests/transport.rs
docs/architecture/ADR-0001-remoting-transport.md
```

invoke(addr, command, deadline) returns one future; invoke_oneway never inserts a pending response. A pending entry includes connection generation, deadline and oneshot sender. A short synchronous lock may protect a map only if no guard crosses await; remove is the terminal ownership operation. One reader and one unbounded outbound enqueue/writer path per connection plus an owned timeout scheduler are sufficient; do not spawn a timeout task per request. Use Tokio time so tests can advance deadlines deterministically.

## 4. Ordered implementation tasks

### 4.1 P3-01 / P3-02 — Own and reuse connection generations

Serialize writes, enqueue outbound frames without a hard queue-capacity limit, and retain join handles; match Java Netty's writeAndFlush behavior rather than imposing a Rust-only queue-full rejection. Normalize address keys without breaking IPv6, DNS/SNI or TLS identity. Single-flight creation per key shares healthy connections; a stale close removes only its own generation. Cancelled creation must close unpublished sockets, and different addresses may connect concurrently.

### 4.2 P3-03 / P3-04 — Make pending ownership atomic

Use wrapping atomic opaque generation; on wrap do not replace a still-pending entry with the same ID. Insert before writing so an immediate response cannot be missed. Response, deadline scan, caller cancellation, write failure, close and shutdown all remove-and-complete through one function. On connection failure remove only that generation. Late/duplicate responses are ignored with bounded diagnostics; a cancelled invoke promptly removes its pending entry.

### 4.3 P3-05 / P3-06 — Apply one deadline to all stages

Admission, connection acquisition, TCP/TLS handshake, socket write and response wait consume the same deadline. Outbound enqueue itself is unbounded and does not wait for queue capacity. An already-expired invoke must not send. Distinguish connect, timeout, closed and malformed-frame errors. Oneway sets its bit, enqueues without a hard queue-capacity limit, and follows Java's asynchronous write-failure handling; it never claims Broker storage success.

### 4.4 P3-07 — Dispatch inbound requests off the reader

Classify response versus request before pending lookup. Dispatch requests by raw code through owned processor workers with queue admission matching Java's executor backlog (unbounded where the Java executor uses an unbounded queue); send replies with the original opaque and response bit. Unsupported requests get the Java-compatible unsupported code, except inbound oneway commands must not receive replies. Define rejection behavior from the oracle; never silently drop accepted transaction/reply work.

### 4.5 P3-08 — Implement verified TLS

Use tokio-rustls/rustls with normal trust-chain and hostname verification. Plain TCP and TLS are explicit configuration choices, not automatic insecure fallback. Add local CA trust input and client certificate support when required by the chosen profile. Preserve DNS names for SNI; connect/handshake still use the original deadline. Do not embed test private keys in production defaults.

### 4.6 P3-09 — Close and join all owned work

Reject new work, cancel creation, close connections, drain pending requests, stop processors/timers and join tasks. Define repeated shutdown as safe and ensure a task never awaits its own join. Record Java flow mappings and ownership in ADR-0001. Any observable discrepancy blocks parity and requires review, not an intentional-difference waiver.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Run these checks without Docker or a live Broker; use Java fixtures, controlled clocks and scripted peers. Shared commands and evidence rules are in the execution index.

- [x] **P3-V01** — Barrier-controlled response/timeout/cancellation/shutdown races produce at most one completion and zero pending entries; repeat through opaque wrap.
- [x] **P3-V02** — Old-generation close cannot evict a replacement; concurrent creation shares one connection; unbounded outbound enqueue accepts beyond former queue sizes while a blocked socket write respects the operation deadline.
- [x] **P3-V03** — Loopback inbound requests, unsupported/oneway behavior and concurrent responses work without blocking the reader.

- [x] **P3-U01** — Scripted TCP/TLS peers verify raw route framing, trusted/untrusted CA and hostname failures, deadlines and zero owned tasks after shutdown.

These four checked items record local automated-test evidence only; they do not constitute Phase 3 acceptance. The Java/Rust executor and callback lifecycle review remains documented as partial where Tokio scheduling/ownership differs, and P3-V04 remains deferred to Phase 19.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-remoting --test transport --locked
cargo test -p rocketmq-classic-remoting --locked
```

### 5.2 Phase 19 live-test handoff

Retain these case IDs and behavior requirements for Phase 19 execution against Docker server 5.3.4; they are not this phase's pass conditions and must not be marked PASS early.

- [ ] **P3-V04** — Local Broker raw route invoke works; local TLS trusted CA succeeds and wrong hostname/untrusted CA fails; shutdown leaves zero tasks/sockets.

## 6. Acceptance and handoff

Transport contains no Producer retry or RocketMQ correlation table. Phase 4 uses the protocol-owned traits; Phase 5 receives deadline-based single-attempt transport.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 4](05-phase-04-typed-headers-bodies.md).
