# Plan: Phase 1 — Common types, constants and protocol models

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 1; execution index: [`README.md`](README.md).
> Translation: [`02-phase-01-common-protocol-models.zh-tw.md`](02-phase-01-common-protocol-models.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## Execution contract

- Port **all Java client 5.5.1 settings, features and control flows**, including defaults, validation, callbacks, errors, retries and lifecycle. Map Java source/method/configuration to Rust and unit-test IDs in both directions; no extra public Rust setting/feature and no missing Java client capability. Rust naming/ownership may differ, not observable semantics. Internal test seams stay private.
- Implement the **client only**. Java client 5.5.1 remains the oracle; **RocketMQ server V5_3_4 (5.3.4)** is the production/local Docker baseline, not the client source baseline. A server limitation does not authorize deleting a client feature; compare both clients against the same server and retain source/fixture coverage for unavailable success paths.
- **Phase 0–18 primarily use unit tests**, supplemented by Java golden/offline differential tests and small scripted loopback tests where transport requires them. No live Broker or Docker is a phase prerequisite. Broker, TLS/auth cluster, cross-language, end-to-end and server stress execution belongs to [Phase 19](20-phase-19-docker-local-validation.md); non-local checks become owner-executed manual items there. Phase acceptance is not final compatibility/release acceptance.

## 1. Goal and scope

Create the minimum buildable Rust workspace and lossless message/protocol models. No network I/O, Producer retries or Consumer scheduling belongs here.

## 2. Prerequisites and current-state analysis

Phase 0 must have verified the baseline and generated model fixtures. The workspace does not exist yet; only common and protocol crates are created now. Add the remaining crates in their owning phases rather than creating empty scaffolding.

- Read/complete first: [Phase 0](01-phase-00-reference-oracle.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `Message`
- `MessageExt`
- `MessageQueue`
- `MessageConst`
- `MessageDecoder (properties only)`
- `MessageSysFlag`
- `PullSysFlag`
- `LanguageCode`
- `SerializeType`
- `RequestCode`
- `ResponseCode`
- `TopicRouteData`
- `HeartbeatData`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
Cargo.toml
Cargo.lock
rust-toolchain.toml
crates/rocketmq-classic-common/src/lib.rs
crates/rocketmq-classic-common/src/message/
crates/rocketmq-classic-common/src/sysflag/
crates/rocketmq-classic-protocol/src/lib.rs
crates/rocketmq-classic-protocol/src/codes/
crates/rocketmq-classic-protocol/src/route/
crates/rocketmq-classic-protocol/src/heartbeat/
```

Use edition 2024 with a verified supporting toolchain pinned in rust-toolchain.toml; record the chosen MSRV, rather than guessing one from the edition. Set publish=false initially. Protocol depends on common, never the reverse. Introduce bytes, serde/serde_json and thiserror only where used, with versions resolved and locked during implementation. Request/response codes and opaque are i32; offsets/timestamps and broker IDs use Java-compatible signed widths. Keep wire models separate from validated user constructors so sentinel values remain representable.

## 4. Ordered implementation tasks

### 4.1 P1-00 — Bootstrap and verify the workspace

Create the root manifest and two real library crates with the master-plan package names. Use workspace dependency versions and commit Cargo.lock. Add built-in Rust tests, fmt/clippy/test CI and a package metadata check for the dependency direction. Do not add dashmap, parking_lot, compression backends or async frameworks before a concrete use exists.

### 4.2 P1-01 — Fix primitive and unknown-value policies

Document widths beside their wire models. Use raw integer codes or newtypes with known constants, not closed enums that reject unknown requests. For tagged value types expose Unknown(raw) where it can be preserved. Test negative sentinels and min/max values before conversion to usize; validation must precede length allocation.

### 4.3 P1-02 / P1-03 — Port language and serialization mappings

Audit every LanguageCode against the pinned source, including JAVA=0 and RUST=12. Preserve unknown language bytes. SerializeType has JSON=0 and ROCKETMQ=1; represent unknown tags for diagnostics but do not claim to decode them. The marked-length arithmetic itself is owned by Phase 2.

### 4.4 P1-04 — Separate user messages from decoded messages

Implement Message, MessageExt and MessageQueue with private invariants and accessors. MessageExt stores broker offsets, hosts, timestamps, sysFlag, reconsume count and IDs without narrowing. Implement MessageQueue equality/hash/order from Java field semantics; Java string ordering is UTF-16 code-unit ordering, so test supplementary Unicode rather than assuming Rust String::cmp is identical. Do not put transport state into these values.

### 4.5 P1-05 — Encode and parse message properties

Capture Java separator constants and handling of empty/malformed/duplicate entries in fixtures before implementation. Reuse one property encoder/parser. Known system property names are constants; retain unknown properties. User validation must not silently truncate separators or overwrite reserved fields. Compare deterministic bytes only for controlled property order; otherwise compare decoded maps.

### 4.6 P1-06 — Centralize flags

Port client-relevant MessageSysFlag/PullSysFlag masks and helper functions. CompressionType describes flag values here; actual compression/decompression is added in Phases 7/8. Test combined flags, clearing bits and unknown reserved bits without changing unrelated bits.

### 4.7 P1-07 / P1-08 — Implement route and heartbeat models

Use explicit serde field names and omission rules from Java fixtures. Preserve broker-ID map keys, ordered/static-topic metadata, permissions and subscription expression/version fields. Write pure publish/subscribe conversion helpers, including absent master, unreadable/unwritable queues and static-route cases. Sort only at the consumer of a model when Java requires it; do not impose fake JSON set order.

### 4.8 P1-09 — Complete the numeric registry

Port the full pinned RequestCode/ResponseCode inventory, not just the master-plan sample table. Compare names/values to the Phase 0 inventory and annotate intentional aliases rather than rejecting all duplicate numbers. Update REQUEST_CODES.md with unimplemented operation status; no RPC becomes supported merely because a constant exists.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Run these checks without Docker or a live Broker; use Java fixtures, controlled clocks and scripted peers. Shared commands and evidence rules are in the execution index.

- [x] **P1-V01** — Raw codes, signed boundaries, LanguageCode and SerializeType mappings round-trip without field loss.
- [x] **P1-V02** — Property golden cases cover separators, Unicode, empty input, duplicates and unknown keys.
- [x] **P1-V03** — Java queue ordering/allocation input fixtures and route/heartbeat JSON semantics match, including static metadata.
- [x] **P1-V04** — Every Phase 0 model field is representable and package dependency inspection shows no cycle.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo metadata --format-version 1 --no-deps
```

## 6. Acceptance and handoff

Phase 2 receives validated value types and constants, not network logic. Record exact source provenance and any unknown JSON-field preservation policy in the matrix.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 2](03-phase-02-remoting-codec.md).
