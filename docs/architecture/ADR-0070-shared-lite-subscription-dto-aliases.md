# ADR-0070: Preserve LiteSubscriptionDTO nested aliases

- Status: Accepted
- Date: 2026-10-10

## Context

The pinned RocketMQ 5.5.1 API oracle confirms that `LiteSubscriptionDTO.setLiteTopicSet` retains the supplied mutable Set and `getLiteTopicSet` returns that exact object. The same is true for the mutable `OffsetOption` supplied through `setOffsetOption`. Mutations through either the setter-held reference or the getter are visible through the other reference. The Rust DTO previously owned copies of both values and exposed mutable borrows, so its getters matched final values but lost Java object identity across the setter boundary.

## Decision

Represent `LiteSubscriptionDTO.lite_topic_set` as `Option<SharedLiteSubscriptionTopicSet>` and `offset_option` as `Option<SharedOffsetOption>`. Setters accept the same `SharedValue` handles retained by callers, and getters clone those handles. Fastjson decoding wraps each decoded value once; serialization and `Display` read the current shared value without changing the Java field names or wire shape. The topic Set remains a `BTreeSet` to match Java `TreeSet` ordering.

`LiteSubscriptionDTO` continues to use Java identity semantics and has no structural Rust `PartialEq`/`Eq` implementation. `OffsetOption` itself retains value equality, matching its Java overrides; the shared holder does not change that DTO's contract.

## Evidence

The direct Java `liteSubscriptionDtoApiCase` records Set/object `==` identity, setter/getter mutation in both directions, field values, and `toString()`. The Rust differential asserts shared-handle pointer identity, applies mutations from each handle, and compares the final Set, nested offset, and diagnostic text. `fixtures/runtime/lookup.json` SHA-256: `1a0df76edff7063ea8790fc0cfbf0321eca6469c766a9be191e6596f7403d799`.

## Consequences and remaining boundary

Callers use `SharedValue::new` when supplying either mutable nested object and can update it with `with_mut`; DTO decode/encode remains transparent. This closes only `LiteSubscriptionDTO.liteTopicSet` and `offsetOption`. At this ADR checkpoint, `LiteSubscription.liteTopicSet`, `LiteSubscriptionCtlRequestBody.subscriptionSet`, heartbeat Sets, and runtime status/reset Maps still required separate review; later checkpoints closed them in ADR-0072, ADR-0071, ADR-0074, and ADR-0073 respectively. Whole Phase 6 reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

## Validation

Code commit `60251cee` passes the focused Java differential and public API/wire tests; `cargo test --workspace --all-targets --locked` with local socket access; `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; workspace doctests (15 protocol doctests); pinned offline Maven `verify` (22 scenarios and Java cross-decodes); comparator self-test; 22-scenario manifest validation; formatting; and diff checks.
