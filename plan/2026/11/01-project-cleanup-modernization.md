# Plan: Project cleanup and maintainability review

## 1. Goal, baseline, and limits

Review date: 2026-11-08. Source baseline: `16386296`. English is authoritative; the companion `.zh-tw.md` has equivalent scope. This change adds plans only.

Reviewed the workspace manifests and module inventory across all seven library crates and `integration-tests`, with targeted source/caller inspection of Consumer, Core, Producer, Protocol, Common and Remoting; also inspected examples, Java oracle configuration, fixtures layout, scripts, differential runner, compatibility status, release checklist and previous plans. This is a repository-wide structural review with focused findings, not a claim of line-by-line semantic verification of every file or proof that every public item is used externally.

Keep Rust 1.85.0, edition 2024, Java client 5.5.1 compatibility and Broker 5.3.4 validation baseline. Existing syntax is already modern in many places (`OnceLock`, `let else`, `is_some_and`, Tokio); age alone is not a reason to rewrite. Dependency currency/security was not audited against online registries; no version upgrade is proposed.

## 2. Findings and priority

Paths below are repository-relative. Line numbers describe this baseline and will move during implementation.

| ID / priority | Evidence | Assessment and result |
| --- | --- | --- |
| F01 / P1 | `crates/rocketmq-classic-consumer/src/process_queue.rs:1`, `pull/result.rs:223,233,256`, `push/consumer.rs:2863`, `push/pull_service.rs:446,464` | Early-phase comments and blanket/item `allow(dead_code)` remain after integration. Filter-server routing is called by both Pull and Push; scheduler `pause` has a production caller. Remove obsolete suppressions and describe current ownership. |
| F02 / P1 | Consumer `process_queue.rs:1803` (`set_lock_state`), `push/consumer.rs:2864` (`activate_process_queue`), `push/pull_service.rs:465` (`resume`) | Compiler reports all three unused in the production library; inspected callers are unit tests. Compile these helpers under `cfg(test)` rather than ship them or delete useful tests. Production uses `set_lock_state_at`, `activate_process_queue_with_lock`, and the other scheduler controls. |
| F03 / P2 | `crates/rocketmq-classic-core/src/runtime/registry.rs:207,773` | `GroupRegistry::clear` is only called by unit tests. `PendingReplyEntry.cancelled` is read only there, while other cancellation fields remain active. Narrow the helper and that entry field to test builds, retaining cancellation tests and all production cancellation owners. |
| F04 / P2 | Core `src/java_number.rs:3,30`; Consumer `src/process_queue.rs:95,126`; Protocol `src/header/mod.rs`; Common `src/message/host_address.rs` and `src/permission.rs` | Java UTF-16 decimal digit tables and signed-number parsing are duplicated. Consolidate equivalent primitives in Common after behavior comparison. Preserve signs, Unicode digits, bounds and caller-specific error conversion. Rust `str::parse` is not an equivalent replacement. |
| F05 / P2 | `crates/rocketmq-classic-common/src/serde_fastjson.rs` and `crates/rocketmq-classic-protocol/src/serde_fastjson.rs` | Duplicated recursive JSON string/coercion machinery can drift. Protocol additionally has `NumberToken` and leading-dot handling; implementations are not interchangeable. Extract only proven common primitives; retain specialized parsing. |
| F06 / P2 | Consumer `src/push/consumer.rs` = 18,073 lines, tests start at 7,527; Remoting `src/client/mod.rs` = 5,856 lines, tests start at 3,036; Protocol `src/header/basic.rs` = 9,053 lines, tests start at 8,250 | Production behavior and large inline test modules are difficult to review together. First move tests into child modules; then split production code by responsibility without broadening visibility or public APIs. |
| F07 / P3 | Producer `src/admin.rs:355`; Consumer `src/admin.rs:35` | Repeated saturating epoch-millisecond conversion, plus identical `first_query_message` implementations in these admin modules. Small duplication; consolidate only if a narrow Core/Common helper reduces overall complexity. Do not force all clock functions into one policy. |
| F08 / P2 | `README.md` repeats historical/current gate narratives; `docs/release-checklist.md` has 11,345 lines including dated follow-ups; `docs/compatibility/CURRENT_STATUS.md` already exists | Current guidance is buried in historical evidence. Make current status authoritative and link from a short checklist/README. Preserve historical audit records and revision-qualified limits. |

No fully unused production subsystem or removable public API was established. Test-only code is unnecessary in production builds but remains necessary for verification. F04–F07 are maintenance opportunities, not demonstrated behavior defects or measured performance improvements.

## 3. Compatibility boundaries

- Retain deprecated transaction listeners/settings, consumer implementation accessors and header aliases: they are intentional Java compatibility surfaces with fixtures/tests. Keep `allow(deprecated)` where compatibility tests require it.
- Do not merge Producer and Consumer service-state enums merely because variant names overlap: order, derives and exposed methods differ.
- Retain boxed futures used for trait objects, heterogeneous futures or recursive async construction; do not mechanically replace them with async trait methods.
- Keep MD5/SHA1/DES and TLS compatibility behavior until a separate protocol/security decision changes the contract. This plan does not alter them.
- Preserve OS-specific branches and their justified lint exceptions, Java oracle fixtures, license/NOTICE copies and historical `rlogs`. The metadata checker requires per-crate license copies.
- Do not delete phase-named wrappers such as `scripts/phase7-producer-scripted-tests.sh`: the Phase 7 matrix still references it. No GitHub Actions workflow is required by this plan.

## 4. Implementation phases

### 4.1 Phase A — lint scope and test-only code (F01–F03)

1. Record forced dead-code diagnostics before edits. Remove Consumer's file-wide suppression and the obsolete routing/scheduler item suppressions.
2. Add `#[cfg(test)]` to the three F02 helpers. Retain bodies and callers. Refresh comments to explain actual ownership rather than future phases.
3. Add `#[cfg(test)]` to private `GroupRegistry::clear`, `PendingReplyEntry.cancelled`, and its initializer entries; retain the `PendingReply` and guard cancellation flags used in production. Check for conditional imports after compiling.
4. Inspect remaining diagnostics individually. Never hide unrelated warnings with another module-wide suppression. Retain documented platform/compatibility exceptions where justified.
5. Validate Consumer/Core unit and integration tests and strict Clippy. Commit this phase independently; do not mix file moves with it.

### 4.2 Phase B — shared numeric semantics (F04)

1. Compare callers and bounds in all five listed implementations. Preserve the existing Java behavior tests.
2. Put UTF-16 digit lookup and signed decimal parsing into `common/src/utils/java_number.rs`; expose only the minimum cross-crate helpers through a `#[doc(hidden)]` module. Move the existing Core implementation rather than add a dependency from Common to Core/Protocol.
3. Before switching callers, compare old/new parsing for empty/sign-only input, whitespace, ASCII and fullwidth/Arabic digits, mixed digits, surrogate units, signed minima/maxima and overflow. Exhaustively compare digit lookup for all 65,536 `u16` values.
4. Migrate Core, Consumer and Protocol plus equivalent Common callers. Keep wrappers where return width or error semantics differ. Remove duplicated tables only after equivalence passes. Update compatibility inventories if helper exports are tracked there.

### 4.3 Phase C — module organization (F06)

1. Move inline test modules to `push/consumer/tests.rs`, `client/tests.rs` and `header/basic/tests.rs` using `#[cfg(test)] mod tests;`. Preserve test names, private access and assertions; compare discovered test lists before/after.
2. In separate commits, move Push admin/runtime adapters and assignment/consume logic into child modules; move Remoting invocation/dispatch implementation blocks into child modules; split basic headers into request/response families and re-export original names from `basic`.
3. Keep public import paths and macro expansion behavior. Use `pub(super)` only where child modules require it; do not make helpers public to satisfy tests. Review lock lifetimes and awaits for accidental changes during moves.
4. Run affected crate tests after each split, then full workspace checks. Do not add replacement abstractions or rewrite algorithms during these moves.

### 4.4 Phase D — bounded deduplication (F05, F07)

1. For Fastjson, compare raw values, duplicate keys, nulls, nested arrays/objects, numeric keys, exponent/suffix forms and leading-dot tokens against existing fixtures. Extract only identical JSON writing/scalar helpers to Common; Protocol may depend on Common, never the reverse. If abstraction requires mode flags throughout the parser or changes coercion results, retain the duplication and document why.
2. For admin code, consider a hidden Core helper for first-result extraction and a Common helper with an explicit saturating epoch-millis contract. Preserve null-first-element behavior and Consumer's three-day lookback. Leave Instant-based deadlines and differing time/error policies alone. Skip extraction if it merely adds public surface for trivial code.
3. Run Common/Protocol fixture tests, all three admin test targets and Maven oracle verification. No fixture regeneration to make behavior changes pass.

### 4.5 Phase E — documentation and final validation (F08)

1. Keep current revision, dated verification limits and MAN-01–MAN-04 in `CURRENT_STATUS.md`; replace repeated README narratives with concise links and retain usage guidance.
2. Separate the current release checklist from historical sections. Move historical content verbatim into dated archive documents; maintain anchors/links or provide an explicit link map for moved references. Do not silently rewrite past PASS claims as current results.
3. Run the complete local validation below. For runtime refactors, owner executes affected Broker 5.3.4 scenarios using the documented Docker/differential workflow; record source revision and output location. Do not claim release approval or full live revalidation until actually executed.

## 5. Verification and observed checks

Executed during review (all succeeded; warnings are expected diagnostic findings):

```bash
cargo rustc -p rocketmq-classic-consumer --lib --locked --offline -- --force-warn dead_code
cargo rustc -p rocketmq-classic-core --lib --locked --offline -- --force-warn dead_code
cargo rustc -p rocketmq-classic-remoting --lib --locked --offline -- --force-warn dead_code
```

Consumer reported exactly F02's three methods; Core reported F03's field and method. Remoting reported only `ParsedProxyRoute::MissingAddress` credentials (`username`/`password`); `client/config.rs:155–165` documents intentional retention of malformed Java proxy configuration, so retain them. Production-library diagnostics alone do not prove public APIs unused. Review did not run the full tests, Clippy, Maven or live scenarios.

Implementation gates:

```bash
cargo fmt --all --check
cargo test -p rocketmq-classic-consumer -p rocketmq-classic-core --locked --offline
cargo test --workspace --all-features --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
python3 scripts/check-workspace-metadata.py
mvn -o -f compat/java-reference/pom.xml verify
git diff --check
```

Use cached dependencies for offline gates; if unavailable, record the blocked gate rather than claim PASS. For F06 compare `cargo test -p <affected-package> --locked -- --list` before/after. After F04–F06, run the existing documented Linux/Windows cross-target checks on configured hosts; macOS-only compilation cannot validate those branches. Owner-run live checks are the final implementation gate, covering Pull/Push rebalance and orderly handoff, request/reply cancellation, class-filter routing where supported, plus remoting deadlines/shutdown for touched paths. Use existing scenario definitions rather than inventing IDs.

## 6. Acceptance criteria

- Only identified test helpers are removed from production compilation; all test coverage remains.
- Consumer broad dead-code suppression and obsolete future-phase comments are removed; remaining exceptions have a current reason.
- Shared primitives preserve Java Unicode/overflow/null/coercion behavior; specialized implementations remain when equivalence is unproven.
- Public API paths, wire output, lifecycle, cancellation and offset behavior remain compatible. Rust 1.85.0 and crate dependency direction remain valid.
- Test moves preserve discovered tests. No dependencies or fixtures are removed based solely on text-search absence.
- Both languages remain synchronized; historical evidence stays accessible; final records distinguish executed gates from owner-pending checks.
