# ADR-0033: Map Java NettyEventType enum API

Status: implemented; full Phase 6 reconciliation remains open.

## Source contract

RocketMQ 5.5.1 exposes `org.apache.rocketmq.remoting.netty.NettyEventType`
with `CONNECT`, `CLOSE`, `IDLE`, `EXCEPTION`, and `ACTIVE` in that order. The
enum has generated `values()` and `valueOf(String)` plus inherited name,
ordinal, text, and comparison behavior. `NettyEvent.getType()` returns one of
these values.

## Rust mapping

The public `client::NettyEventType` enum matches the Java constants, ordinals,
lookup errors, display names, and comparisons. The connection event queue now
uses this public type directly, so its declaration order and event identity
share one definition. Listener methods continue to receive the remote address
through their event-specific callbacks.

## Evidence and limits

`nettyEventTypeMethodCases` in `fixtures/runtime/client-config.json` invokes all
five constants, defensive `values()` copies, and null/unknown/case-mismatched
name lookups. The native differential checks 12 Java cases.
Java `NettyEvent.getChannel()` and channel-event callbacks expose Java `Channel`
objects. Rust callbacks and `ChannelEvent` carry an identity-preserving
`ChannelHandle` with address, state, close, and write capabilities. Connection
generation remains private transport bookkeeping.
This provides the mapped Rust channel operations without fabricating a Netty
object or exposing a raw Tokio socket; it does not reproduce the full Netty
Channel method surface. The enum mapping is separate from that carrier
mapping. Full Phase 6 source/API reconciliation remains open.
