# ADR-0061: Preserve the consumer-list DTO's Java List alias

- Status: Accepted
- Date: 2026-10-10

## Context

Pinned RocketMQ 5.5.1 `GetConsumerListByGroupResponseBody.setConsumerIdList` retains its argument. `getConsumerIdList` returns the same List instance, and mutation through the setter reference or getter changes the same contents. The body inherits identity-based `Object.equals` and `Object.hashCode`.

## Decision

Store the DTO field as `Option<SharedValue<Vec<Option<String>>>>`. Cloning a `SharedValue` preserves its `Arc` identity; `with` and `with_mut` access the same List contents. The shared wrapper serializes transparently, preserving the Java JSON field and nullable outer/inner values. Remove Rust structural `PartialEq`/`Eq` from the body because the pinned Java class does not override object identity methods.

Keep `MQClientApi::get_consumer_list_by_group`'s owned `Option<Vec<Option<String>>>` return shape. Snapshot the shared DTO field at that boundary so this compatibility correction does not change the client API result type.

## Consequences

Callers of the protocol DTO can retain a cloned handle before calling the setter and observe mutations from either the caller handle or DTO getter. Access is synchronized by the shared handle. Decoded DTO values receive a fresh handle around their parsed List. Rust callers now use `SharedValue::snapshot`, `with`, and `with_mut` to inspect or mutate the List.

## Evidence

- Pinned Java 5.5.1 `RuntimeLookupFixtures.consumerListApiCase` asserts getter pointer identity, visibility of caller/getter mutations, `Object` declaring classes for `equals` and `hashCode`, same-instance equality, and unequal distinct instances with equal fields.
- Native differential: `remaining_runtime_body_public_methods_match_invoked_java`.
- Implementation/oracle commit: `456849fb`.
- Fixture SHA-256: `372a5250f6af3ced4c17ef23eb9e05dbe78523ceb6b75a4cd03fe1589fabf80e`.
- Validation: protocol tests/doc tests, full workspace/all-target tests, strict workspace Clippy, pinned offline Maven verify, comparator self-test, 22-scenario manifest validation, format, and diff checks.
