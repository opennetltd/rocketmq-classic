# ADR-0069: Preserve request-mode wrapper Map aliases

- Status: Accepted
- Date: 2026-10-10

## Context

Pinned RocketMQ 5.5.1 `MessageRequestModeSerializeWrapper` retains the mutable map supplied to `setMessageRequestModeMap`. The outer topic Map retains each nested consumer-group Map, and each group Map retains its mutable `SetMessageRequestModeRequestBody` values. Getter references at all three levels return the same objects. Mutations through caller-held and getter-held Map/DTO references are visible in both directions. The earlier Rust implementation matched mutations through its getters but moved owned nested `BTreeMap`s from the setter.

The direct Java oracle also confirms the wrapper inherits `equals` and `hashCode` from `java.lang.Object`.

## Decision

Use `SharedMessageRequestModeMap = SharedValue<MessageRequestModeMap>`, where the outer topic map holds `SharedMessageRequestModeGroup` values and each group map holds `SharedValue<SetMessageRequestModeRequestBody>` values. The public setter and getter use the shared outer handle; Fastjson decoding wraps each non-null Map and DTO level. JSON names, null behavior, defaults, and serialized Map shape remain unchanged.

Remove structural `PartialEq` and `Eq` from `MessageRequestModeSerializeWrapper` to match Java identity. Keep the constructor's non-null empty Map default and the existing owned value representation for independently decoded `SetMessageRequestModeRequestBody` instances outside this wrapper.

## Evidence

`messageRequestModeWrapperApiCase` records outer Map, group Map, and request-body `==` identity; caller-side and getter-side mutation visibility at all levels; and inherited equality/hash declaration. The Rust differential checks `ptr_eq` for each layer and compares the resulting Java JSON. The 33-case Fastjson matrix still checks absent/default/null/root/nested token and duplicate-property behavior. `fixtures/runtime/lookup.json` SHA-256: `1a0df76edff7063ea8790fc0cfbf0321eca6469c766a9be191e6596f7403d799`.

## Consequences and remaining boundary

Wrapper callers can retain shared handles to its outer Map, each topic's group Map, and each row DTO. JSON serialization is unchanged. Other Phase 6 aliases, source/input reconciliation, and P6-V04 remain open.

## Validation

Code commit `853b3fdd` passes workspace/all-target tests with local socket access, strict all-target/all-feature Clippy, workspace doctests (15 protocol doctests), pinned offline Maven `verify` (22 scenarios and Java cross-decodes), comparator self-test, 22-scenario manifest validation, formatting, and diff checks.
