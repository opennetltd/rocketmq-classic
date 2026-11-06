# ADR-0060: Preserve Java DTO reference aliases with shared Rust handles

- Status: Accepted
- Date: 2026-10-10

## Context

Some pinned RocketMQ DTO setters retain the supplied mutable object, and their getters return that same object. A Rust setter that consumes a value and exposes only a borrow matches DTO-owned mutation but cannot preserve a caller-held alias. `CheckClientRequestBody.setSubscriptionData` is an invoked example: Java mutations through either the original `SubscriptionData` reference or the body getter are visible through both references.

`CheckClientRequestBody` also inherits identity-based `Object.equals`/`hashCode`; it does not compare its fields structurally.

## Decision

Add protocol-level `SharedValue<T>`, a cloneable `Arc<RwLock<T>>` handle. Cloning the handle retains identity; `snapshot` returns an owned copy; `with` and `with_mut` provide synchronized access; serde serializes the contained DTO value transparently. Poisoned locks recover their value so an interrupted mutation does not permanently disable later access. A closure must not re-enter the same handle while its lock is held.

Use `Option<SharedValue<SubscriptionData>>` for `CheckClientRequestBody.subscription_data`. The setter stores a handle, and its getter returns a cloned handle to the same object. Ordinary construction can convert an owned `SubscriptionData` with `SharedValue::from`. Remove derived structural equality and hashing from `CheckClientRequestBody` because those traits do not match its Java declaration.

Apply shared handles to other DTO fields only after their setter/getter alias behavior has direct Java evidence. Later evidence extends this decision to the specific nested fields recorded in [ADR-0064](ADR-0064-shared-batch-ack-nested-aliases.md), [ADR-0066](ADR-0066-shared-assignment-nested-aliases.md), [ADR-0067](ADR-0067-shared-lock-set-element-aliases.md), [ADR-0068](ADR-0068-shared-query-assignment-elements.md), [ADR-0069](ADR-0069-shared-request-mode-wrapper-aliases.md), [ADR-0070](ADR-0070-shared-lite-subscription-dto-aliases.md), [ADR-0071](ADR-0071-shared-lite-subscription-set-aliases.md), [ADR-0072](ADR-0072-shared-lite-subscription-getter-alias.md), and [ADR-0073](ADR-0073-shared-runtime-map-body-aliases.md). This decision does not claim that other collection or nested-object aliases are already compatible.

## Consequences

Rust callers can retain a cloned `SharedValue` before assigning it and observe mutations in both directions. The protocol DTO field is now an explicit shared handle, so callers read or mutate it through the handle methods. JSON shape remains the wrapped Java DTO shape. `SharedValue` does not add an equality/hash policy; DTOs retain their separately verified Java identity or value semantics.

## Evidence

- Pinned Java 5.5.1 `RuntimeLookupFixtures.checkClientBodyApiCase` records same-reference getter behavior, external mutation visibility, getter mutation visibility, and the declaring classes for `equals` and `hashCode`.
- Native differential: `remaining_runtime_body_public_methods_match_invoked_java`.
- Implementation/oracle commit: `a732974a`.
- Fixture SHA-256: `dcb46e55ac972d2d621585f61fb28cc67fd398d8d5ada31bd1586a5abe07a8b3`.
- Validation: protocol crate tests and doc tests, full workspace/all-target tests, strict workspace Clippy, pinned offline Maven verify, comparator self-test, 22-scenario manifest validation, format, and diff checks.
