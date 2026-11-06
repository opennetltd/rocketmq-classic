# ADR-0084: Map the Java heartbeat request header API

Status: implemented for `HeartbeatRequestHeader`; the broad Phase 18 API and
reverse-export audit remains open.

## Source contract

The pinned RocketMQ Java 5.5.1 source at
`efa1be59d6fc747ad220f036c86030f05f0419f9` declares
`HeartbeatRequestHeader` as an `RpcRequestHeader` subtype. Client
`MQClientAPIImpl.sendHeartbeat` and `sendHeartbeatV2` construct it for request
code `HEART_BEAT`. The subtype declares no payload fields; its inherited
nullable fields are `ns`, `nsd`, `bname`, and `oway`. `RpcRequestHeader`
provides structural equality/hash and a field-ordered `toString`; the deprecated
`getBname`/`setBname` aliases coexist with `getBrokerName`/`setBrokerName`.

Before this change, Rust heartbeat RPCs used the empty
`LiteSubscriptionCtlRequestHeader`. Default requests had the same empty ext
field map, but the public header type and its inherited API were missing.

## Decision

- Export a Rust `HeartbeatRequestHeader` with nullable namespace, namespaced,
  broker-name, and oneway fields and the matching Java RPC-header equality and
  `Hash` behavior.
- Encode and decode those fields using the Java ext-field keys. Default values
  omit all four fields, preserving the existing heartbeat request frame.
- Provide Java-compatible getter/setter aliases, deprecated `bname` accessors,
  integer `hashCode` calculation, and `toString` rendering.
- Use the new header in both Rust heartbeat RPC methods. Keep the empty Lite
  subscription control header for its separate request code.

## Evidence

- `fixtures/remoting/header-heartbeat-request.json` records the pinned Java
  typed frame, with SHA-256
  `d52b9cfbb0e640702e0eb6cea45430a237ac7120513854b36f677447fc3cfe46`.
- `fixtures/protocol/request-header-identity.json` includes Java equality,
  hash, inherited method, and formatting evidence for this header.
- `cargo test --locked -p rocketmq-classic-protocol --test header_inventory`
  passes all three tests; the focused request-header identity test and all 14
  Core extension tests pass.
- `TZ=UTC mvn -o -q -f compat/java-reference/pom.xml verify` passes all 37
  scenarios, fixture drift checks, and Java/Rust cross-decodes, including 56
  Rust typed-header frames.
- Strict all-target/all-feature Clippy passes for Protocol and Core.

This decision closes only the heartbeat request-header slice. It does not
close P18-V01's full Java API, overload, input/error/flow, inherited-surface,
or Rust reverse-export audit.
