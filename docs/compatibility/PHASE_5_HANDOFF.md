# Phase 5 implementation handoff

Local phase acceptance: P5-V01, P5-V03 and P5-U01 verified. P5-V02 has partial Phase 19 evidence: route, producer, and consumer subcases pass; the extension subcase remains pending. Branch: `06-phase-05-typed-rpc-api`. No live Broker, NameServer or Docker was required or contacted for the Phase 5 acceptance; no final Java compatibility/release claim is made.

## Requirement audit

| Requirement | Authoritative implementation/evidence |
| --- | --- |
| P5-01 downward core crate and typed errors | Core Cargo.toml depends on common/protocol/remoting; RpcError preserves typed sources, response code/remark/request/peer. `rpc_errors.rs` exercises actual connect/channel/malformed-frame/config/lifecycle outcomes; malformed domain headers/bodies and unknown response codes are checked in invoked suites |
| P5-02 route/failover/deadline | `api/route.rs`, `rpc.rs` route/failover/no-server/stalled/exhausted-deadline cases, invoked `route-*` fixtures; no writable route is invented |
| P5-03 one producer attempt and transaction | `api/producer.rs`; five send variants, four domain statuses, sync/future/oneway, identifiers/region/trace and end-transaction remark; Java dispatch fixtures and `every_send_variant_classifies_reply_before_decoding` |
| P5-04/P5-07 consumer and method-specific status mapping | `api/consumer.rs`, `consumer.rs`, invoked consumer families; ordinary/Lite pull code selection, four pull outcomes/raw bytes/offsets, offset-not-found, list/send-back, lock/unlock and oneway modes |
| P5-05/P5-06 lifecycle/extensions | `api/extensions.rs` and invoked extension/POP/ACK families; heartbeat V2 negotiation, unregister, assignment/request mode, Lite subscription, POP/POP Lite, ACK/ACK Lite, batch ACK/change-invisibility. Primitive-default reflective response behavior is compared against actual Java invocation |
| P5-08 executable evidence/live registration | 148 persisted invoked cases, Maven drift checks, eight corresponding Rust suites, complete SHA-256 manifest; original P5-V02 IDs/profile/comparison rules retained in scenarios.yaml with execution_phase=19 |
| P5-V01 per-method domain/unknown/malformed branches | Complete bidirectional method/branch inventory in PHASE_5_OFFLINE_METHOD_MATRIX.md; no-payload and oneway response corruption are explicitly N/A because Java decodes nothing |
| P5-V03 attempt/deadline/pending | Broker single-attempt table covers all methods/modes including Lite pull; NameServer selection/failover tests, mode late-response tests, oneway pending/correlation tests and atomic terminal/opaque/cancellation/shutdown tests. Sync timeout transfers permit to pending until terminal cleanup |
| P5-U01 actual Java modes and results | Pinned MQClientAPIImpl/MQClientAPIExt calls with only Java transport mocked; scripted Rust checks compare code/ext/body/outcome. Callback objects are represented by Rust futures, with separately verified wait/cleanup semantics |
| Public API/configuration consistency | 22 typed domain methods plus constructor; private helper seams; no core settings added. Every method/type maps to a Java capability in the reverse inventory. Models reused from prior phases; no parallel transport or singleton |
| Branch/commits and phase contract | All Phase 5 work remains on its dedicated branch and reviewable changes are separate commits; plans were not edited. ADR-0003 records additive lower-layer sync adapters and pending ownership correction |

## Final verification

Commands executed successfully against the completed implementation:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo test -p rocketmq-classic-core --test rpc --locked
mvn -f compat/java-reference/pom.xml verify
python3 tests/differential/compare.py --self-test
shasum -a 256 -c docs/compatibility/P5_INVOKED_SHA256SUMS.txt
```

Socket tests require loopback permission in the restricted execution environment; permission-denied runs were rerun with that permission. Final workspace test output is `/tmp/p5-final-workspace.log`; Maven invoked drift verification passed after the final Lite pull fixtures. JDK 21.0.2 / Maven 3.9.16 and locked Rust 1.85.0 were used. The source/artifact SHA-256 values and exact reference commit are in compat/java-reference/README.md. `P5_INVOKED_SHA256SUMS.txt` covers all 148 invocation fixtures, not only the new subset.

Recent incremental commits: `0d3d708` malformed pull/lock/assignment; `2c25a09` sync/future wait selection; `478b91d` async producer/pull fixtures; `04adc53` retained sync pending admission; `6f00cf2` private helpers and error/source tests; `410985d` Lite pull flag/code selection. Earlier Phase 5 commits remain in branch history.

## Downstream ownership

Phase 6 receives route, heartbeat V1/V2 and unregister primitives over a caller-owned/shared RemotingClient. Phase 7 supplies producer queue selection/retry/unique-ID/batch preparation. Phase 8 attaches a single binary message decoder to existing raw pull/POP bytes. Phase 13 supplies public Lite/POP consumer state and extra-info attachment using these lower-level calls. Phase 19 has passing server 5.3.4 differential evidence for the P5-V02 route, producer, and consumer subcases; the extension subcase remains pending. No final Java compatibility or release claim is made.
