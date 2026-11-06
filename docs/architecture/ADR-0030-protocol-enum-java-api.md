# ADR-0030: Map protocol enum APIs to Java

Status: implemented; full Phase 6 reconciliation remains open.

## Source contract

The pinned RocketMQ 5.5.1 public protocol enums include `MessageRequestMode`,
`LiteSubscriptionAction`, `OffsetOption.Type`, `CMResult`, `ConsumeType`,
`MessageModel`, and `ConsumeFromWhere`. Each provides generated `values()` and
`valueOf(String)` and inherited name/order/text/comparison methods. The first
`MessageRequestMode.getName`, `ConsumeType.getTypeCN`, and
`MessageModel.getModeCN` are custom methods already mapped in Rust. Java
declaration order for `MessageModel` is
`BROADCASTING`, `CLUSTERING`, `LITE_SELECTIVE`.

## Rust mapping

Each corresponding Rust enum now exposes declaration-order `values()`,
`value_of_name`, `name`, `ordinal`, `Display`, and `java_compare_to`. The
shared `JavaEnumLookupError` preserves Java's null/unknown categories and
messages. `MessageModel`'s Rust `Ord` now compares Java ordinals, correcting
the previous derived order of its first two variants.

## Evidence and limits

`protocolEnumMethodCases` in `fixtures/runtime/client-config.json` invokes the
pinned Java methods for all seven enums, including null, empty, case-mismatched,
and unknown names. The native differential checks 65 cases, including values
array copies and `MessageModel` ordering. Existing request-mode and heartbeat
tests continue to cover the custom name methods. Full workspace tests, strict
Clippy, pinned offline Maven verification, formatter, and diff checks pass.
This maps these enum APIs, not the remaining Phase 6 source/API/input audit.
