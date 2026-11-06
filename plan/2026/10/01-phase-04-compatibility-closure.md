# Plan: Phase 4 Compatibility Coverage Closure

> Parent: [`../09/00-rocketmq-classic-plan.md`](../09/00-rocketmq-classic-plan.md), Phase 4 implementation plan: [`../09/05-phase-04-typed-headers-bodies.md`](../09/05-phase-04-typed-headers-bodies.md).
> This scoped follow-up was opened to close Phase 4 typed-header/body evidence gaps. Its completed outcome is recorded in section 7 and in the Phase 4 matrix; it accepts the typed wire-model gate only, not later RPC or live Broker coverage.

## 1. Goal and scope

Complete executable Java 5.5.1 compatibility coverage for implemented Phase 4 headers and bodies, close identified model/oracle gaps, and update the Phase 4 matrix and append-only session log with observed results. Keep the phase incomplete until every acceptance criterion below is met. Do not expand into RPC execution or live broker validation.

## 2. State when this closure audit started (2026-10-01)

- `crates/rocketmq-classic-protocol/tests/headers.rs` covers Java-to-Rust fixtures and a subset of required/invalid header cases. Rust-to-Java Maven verification compares all ext fields for 19 Rust-generated header frames; this does not yet prove complete bidirectional coverage for every implemented header.
- Seven Rust-generated body fixtures are compared semantically by the Java 5.5.1 oracle through `mvn verify`.
- `docs/compatibility/REQUEST_CODES.md` records the remaining partials: Reply V2/push, POP Lite response, consumer-list response fixture, lock/unlock body oracle, and ACK Lite body/source audit.
- `rlogs/2026/09/05-phase-04-typed-headers-bodies__20261004T021500Z.rlog` records previous commands and results. Preserve its append-only history.
- Existing worktree changes are user/session work and must be preserved. Check `git status --short` before every editing batch and do not revert unrelated changes.

## 3. Implementation steps

### 3.1 Close header fixture coverage (P4-V01)

1. Inventory every implemented `CommandHeader` in `crates/rocketmq-classic-protocol/src/header/basic.rs` and its public export in `header/mod.rs`. Cross-reference Java fixture stems and assertions in `compat/java-reference/src/main/java/compat/Main.java`, `crates/rocketmq-classic-protocol/tests/headers.rs`, and the Maven Rust-frame verifier.
2. For each implemented request and response header, ensure there is a pinned Java 5.5.1 fixture, a Rust decode assertion checking every modeled field, and a Rust-generated frame decoded by Java with exact expected ext-field comparison. Add fixtures/assertions for omissions, not only positive populated values, where Java encoding omits null fields or uses primitive defaults.
3. Make coverage inventory-driven (a checked-in table or equivalent exhaustive case list), so adding a public implemented header without both directions fails a test. Do not assert a header model for ACK_LITE unless a pinned Java custom-header call site is found.

### 3.2 Complete table-driven validation coverage (P4-V02)

1. In `crates/rocketmq-classic-protocol/tests/headers.rs`, define per-header cases with valid baseline fields, required-field set, numeric fields, booleans, and Java fixture association. Exercise all headers with required fields, including response headers.
2. For each required field, remove it independently and assert the exact `ProtocolError::MissingField` field name. For each numeric field, test malformed input and out-of-range values for its Rust width; test signed boundaries where applicable. For optional fields, distinguish missing from empty, zero, and false. Verify boolean parsing against Java `Boolean.parseBoolean` behavior.
3. Keep the raw ext-field preservation test and run typed encode into a cloned map; assert unknown entries remain unchanged. Avoid duplicating header-specific ad hoc tests when one table row can express the case.

### 3.3 Reconcile remaining model/oracle gaps

1. **Reply V2 and push:** inspect pinned `rocketmq-all-5.5.1` header classes, conversion helpers, and `MQClientAPIImpl` call sites. Add only source-backed typed models and Java fixtures for actual request/response shapes; compare full fields and applicable message-body semantics. If a shape is not a custom header, document that fact and test the body separately.
2. **POP Lite response:** inspect the exact Java response header/body classes and call site. Implement missing typed response model(s), fixture generation, decode assertions, and Rust-to-Java assertions; preserve binary message body boundaries.
3. **Consumer-list response:** use the actual Java `ConsumerListByGroupResponseBody` encoding to generate fixtures for null/absent, empty, and populated `consumerIdList`; assert Rust semantic decoding/encoding without collapsing absent and empty.
4. **Lock/unlock body oracle:** generate Java 5.5.1 request and response JSON fixtures for `LockBatchRequestBody`, `UnlockBatchRequestBody`, `LockBatchResponseBody`, and `UnlockBatchResponseBody`. Compare Rust output semantically and test collection/omission behavior; inherited header fields remain covered by header fixtures.
5. **ACK Lite audit:** inspect source and call graph for request body construction and ACK Lite response handling. Add source-backed body model/tests only where an actual Java body exists. Retain no custom-header model if the client passes a null custom header. Record source files, method/call-site evidence, and exact disposition in the matrix.

### 3.4 Synchronize compatibility evidence

Update `docs/compatibility/REQUEST_CODES.md` per completed case with fixture IDs, direction, model status, and remaining gaps. Keep model readiness distinct from RPC implementation. Keep Phase 4 verification checkboxes unchecked until corresponding automated gates run. Append a dated entry to the existing rlog (never rewrite prior lines) with changed paths, fixture provenance, case IDs, exact commands/results, and any residual TODO/PARTIAL items.

## 4. Verification

Run focused checks while developing, then all repository gates:

```bash
cargo test -p rocketmq-classic-protocol --test headers --locked
cargo test --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
mvn -q -f compat/java-reference/pom.xml verify
python3 tests/differential/compare.py --self-test
git diff --check
```

Maven `verify` must work from a clean generated-fixture directory and regenerate Rust fixture inputs as configured. Report actual command outcomes; do not infer passes from existing fixtures or prior session logs.

## 5. Acceptance criteria

- P4-V01 passes through an executable exhaustive inventory: every implemented request/response header has Java-to-Rust all-field assertions and Rust-to-Java exact-field assertions.
- P4-V02 passes table-driven required, malformed numeric, overflow, boolean, optional-value, and unknown-field preservation cases for the applicable headers.
- P4-V03 continues to pass for normalized Send V1/V2 headers and semantic JSON body fixtures, including omission and collection distinctions.
- Each identified model/oracle gap is either closed with pinned-source fixtures and tests or explicitly unresolved with source evidence and a blocking status; no unsupported ACK Lite header is invented.
- The compatibility matrix and append-only rlog match the executed evidence. Phase 4 remains marked incomplete if any required case is pending, partial, or lacks an executable oracle.

## 6. Constraints and open evidence

Use the pinned Java client 5.5.1 as the wire-format oracle. Do not change protocol behavior based on inference or server limitations. If source annotations and call-site behavior disagree, preserve the distinction in tests/matrix and resolve from the actual Java construction/decoding path. No Docker, live broker, new test framework, or unrelated Phase 5 RPC work is required.

## 7. Completion record (2026-10-04)

The closure work is complete for P4-V01/V02/V03. The final inventory covers every public `basic.rs` `CommandHeader` plus the unlock alias in both directions, and the Maven verifier checks 38 Rust-generated header frames. Table-driven Rust tests cover every modeled header field, required/optional cases, malformed and boundary numbers, Java boolean parsing, and unknown-field preservation. The body gate checks 14 Java-decoded semantic frames, including absent/empty/populated consumer lists, lock/unlock bodies, and reply V2/push binary body bytes. The pinned ACK Lite audit found no distinct Lite body/header call path to model; the shared method uses ACK_MESSAGE for a non-null request header.

Final evidence: `cargo test -p rocketmq-classic-protocol --test headers --locked`, `cargo test --workspace --locked`, `cargo fmt --all -- --check`, strict workspace/all-target/all-feature Clippy, `mvn -q -f compat/java-reference/pom.xml clean verify`, `python3 tests/differential/compare.py --self-test`, and `git diff --check` all passed. The append-only execution log records fixture provenance and results. The remaining RPC wiring, public client surface, and Broker integration are later-phase scope, not open Phase 4 typed-model acceptance items.
