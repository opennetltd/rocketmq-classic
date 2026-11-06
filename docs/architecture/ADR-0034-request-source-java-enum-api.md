# ADR-0034: Map Java RequestSource enum API

Status: implemented; full Phase 6 reconciliation remains open.

## Source contract

RocketMQ 5.5.1 exposes `org.apache.rocketmq.remoting.protocol.RequestSource` with
`SDK`, `PROXY_FOR_ORDER`, `PROXY_FOR_BROADCAST`, and `PROXY_FOR_STREAM`. The
public enum includes `getValue()`, `isValid(Integer)`, `parseInteger(Integer)`,
and `SYSTEM_PROPERTY_KEY` in addition to generated enum methods. The values are
`-1`, `0`, `1`, and `2`; integer parsing accepts only `-1..=2` and falls back to
`SDK` for null or out-of-range values.

## Rust mapping

The public `remoting::RequestSource` maps enum order/names/text/lookups,
`get_value`, `is_valid`, `parse_integer`, and the exact property-key constant.
The pinned client, common, and remoting artifact scan found no class references
to `RequestSource` outside the enum itself. The enum and key are therefore
mapped as public API; no property-loading behavior is inferred from the
constant.

## Evidence and limits

`remotingEnumMethodCases.RequestSource` in `fixtures/runtime/client-config.json`
invokes the enum methods and records 23 enum/integer input outcomes plus the
constant value. The native differential checks the names, ordinals, comparisons,
lookup errors, getter values, validity boundaries, parser fallbacks, and key.
Whole Phase 6 source/API/input reconciliation remains open.
