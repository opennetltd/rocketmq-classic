# ADR-0035: Map Java MessageQueueRouteState enum API

Status: implemented; full Phase 6 reconciliation remains open.

## Source contract

RocketMQ 5.5.1 exposes the public
`org.apache.rocketmq.remoting.protocol.route.MessageQueueRouteState` enum with
`Expired`, `ReadOnly`, `Normal`, and `WriteOnly` in that declaration order. Its
public surface is the generated enum API: `values`, `valueOf`, name, ordinal,
text, and comparison behavior. The pinned artifact scan found no references to
the enum from other classes, so no route-state transition behavior is inferred.

## Rust mapping

The public `remoting::MessageQueueRouteState` enum maps the four values, order,
exact names and text, defensive `values()` behavior, comparison, and typed Java
name-lookup errors.

## Evidence and limits

`remotingEnumMethodCases.MessageQueueRouteState` in
`fixtures/runtime/client-config.json` invokes each enum method and null,
unknown, empty, and case-mismatched lookups. The native differential checks all
cases. This maps the public enum API only; it does not claim state-machine use
in the pinned client runtime. Whole Phase 6 source/API/input reconciliation
remains open.
