# Security, namespace and tracing

This page describes the client-side APIs and settings implemented against the
Apache RocketMQ Java client 5.5.1. Local TLS and ACL tests use synthetic
credentials and certificates. Broker interoperability is tracked separately
for Phase 19.

## Namespace

Set the namespace on each client configuration before starting the client. The
client wraps logical topic and group names at the wire boundary and restores
logical topic names on returned messages and queues.

```rust
use rocketmq_classic::{Producer, ProducerConfig};

let mut config = ProducerConfig::new("orders-producer")?;
config
    .client_config_mut()
    .set_namespace(Some("tenant-a".to_owned()));
let producer = Producer::new(config);
producer.start().await?;
```

The same `ClientConfig::set_namespace` setting is available through
`PushConsumerConfig`, `PullConsumerConfig`, and `LitePullConsumerConfig`.
Pass logical queue and topic names to the public APIs; do not add the namespace
prefix yourself.

`ClientConfig::set_namespace_v2` selects the Java 5.5.1 RPC namespace metadata
path. It adds `ns` and `nsd` to remoting requests while keeping the configured
resource names unchanged. The internal trace producer carries this metadata and
uses the already namespaced host group once.

## ACL request signing

The RPC hook signs each outbound request with Java-compatible HMAC-SHA1 and
adds the access key, optional security token, and signature to request fields.
The hook runs for each request attempt, so a retry is signed again after its
route and queue fields have been set.

```rust
use std::sync::Arc;

use rocketmq_classic::{
    AclClientRpcHook, AclUtils, Producer, ProducerConfig, RpcHook, SessionCredentials,
};

let credentials = SessionCredentials::new(access_key, secret_key);
let hook: Arc<dyn RpcHook> = Arc::new(AclClientRpcHook::new(credentials));
let producer = Producer::new_with_rpc_hook(
    ProducerConfig::new("orders-producer")?,
    Some(hook),
);
producer.start().await?;
```

For a Java-compatible ACL YAML file, use
`AclUtils::get_acl_rpc_hook_from_file(path)`. Treat a `None` result
as a configuration error and stop startup; do not silently create an unsigned
client. `SessionCredentials::Debug` redacts credential values. Keep secret
files outside source control and restrict their filesystem permissions.

The facade also exposes the Java helper family as `AclSigner`, `AclUtils`, and
`AclException`. `AclUtils` accepts a reader or file path for typed YAML loading
and for ACL hook creation. For a non-UTF-8 signer charset, encode the payload
and key first, then use `AclSigner::cal_signature_with_material`; the built-in
client hook uses Java's default UTF-8/HmacSHA1 path.

The same RPC hook can be passed to `PushConsumer::new_with_rpc_hook`.

## TLS

Set `ClientConfig::set_use_tls(true)` to select TLS for a RocketMQ client.
Java-compatible TLS properties are read when the shared client runtime is
created. The properties include `tls.test.mode.enable`,
`tls.client.authServer`, `tls.client.trustCertPath`, `tls.client.certPath`,
`tls.client.keyPath`, `tls.client.keyPassword`, `tls.ciphers`, and
`tls.protocols`; `tls.config.file` selects an optional properties file whose
values override process properties.

The client applies `tls.ciphers` and `tls.protocols` to its TLS context, with
unsupported names causing TLS context creation to fail. Their ClientHello
advertisements depend on the configured cipher and protocol intersection. For
example, a single TLS 1.2 cipher with both TLS 1.2 and TLS 1.3 enabled produces
a TLS 1.2 ClientHello, matching the pinned Java 5.5.1 client. A TLS 1.3 cipher
with TLS 1.3 enabled produces a TLS 1.3 ClientHello. Rust applies the same
property and file-precedence rules.

For verified server identity, set both `tls.test.mode.enable=false` and
`tls.client.authServer=true`. Provide a trusted CA with
`tls.client.trustCertPath`, or use the system trust store. A client certificate
and key must be provided together when the Broker requires mutual TLS. Bad
certificate or key material fails client construction before worker tasks
start. The client does not fall back to plaintext after a TLS failure.

Producer and consumer configs also accept an explicit `TlsConfig`. Setting it
enables TLS for the shared runtime and lets the caller supply CA bytes, server
verification behavior, or client identity directly:

```rust
use std::path::Path;

use rocketmq_classic::{Producer, ProducerConfig, TlsConfig};

let mut config = ProducerConfig::new("orders-producer")?;
config.set_tls_config(Some(TlsConfig::from_ca_file(Path::new("/etc/rocketmq/ca.pem"))?));
let producer = Producer::new(config);
producer.start().await?;
```

The first config used for a shared client identity owns that runtime's TLS
settings, matching the Java client manager's first-runtime reuse behavior.

The lower-level remoting API also accepts an explicit TLS configuration:

```rust
use std::path::Path;

use rocketmq_classic_remoting::client::{RemotingClient, TlsConfig, TransportMode};

let tls = TlsConfig::from_ca_file(Path::new("/etc/rocketmq/ca.pem"))?;
let remoting = RemotingClient::new(TransportMode::Tls(tls));
remoting.start().await?;
```

`TlsConfig::from_ca_file` enables CA-chain and server-name validation. Avoid
Java's trust-all test mode outside isolated compatibility tests.

## Hooks and tracing

Producer and consumer hooks receive the logical operation context with the
namespace and access channel. RPC hooks operate on remoting commands. Hook
methods are infallible callbacks: a panic is caught and remaining hooks
continue. RPC hooks can return an error, which aborts that remoting request.
Hook implementations run synchronously on the calling operation path, so avoid
blocking work. Producer sync sends run after-hooks once per Broker attempt.
Future-mode sends run before once, after once when the async request is
submitted, and after again when retries finish with a result or error, matching
the Java client's callback sites. A Future send to an explicit queue retries
that same Broker; a routed Future send may select another Broker.

Enable Java-compatible client message tracing with `ClientConfig::set_enable_trace(true)`.
Use `ClientConfig::set_trace_topic(Some(topic))` to select a trace topic and
`set_trace_msg_batch_num(value)` to set the batch size. The client uses an
isolated internal producer, so trace sends do not recursively produce trace
records. Trace queue overflow drops the new trace context without blocking the
business send or consume path. Client shutdown flushes queued trace contexts
before stopping the trace producer.

## Connection events

The lower-level remoting client can receive connect, close, exception, idle,
and active events through `ChannelEventListener`. Existing address callbacks
remain available; `on_channel_event` groups the event type, peer address, and an
identity-preserving `ChannelHandle` for the affected connection. Connection
generation remains private transport bookkeeping. URI userinfo is redacted from
the peer address before delivery. Events are
delivered by an owned worker outside the transport reader. The event queue is
bounded; events may be dropped when it is full, so use it for telemetry and
connection observation, not correctness-critical work. Keep callbacks short
and move blocking work to an application-owned executor. Shutdown cancels
pending asynchronous active-event callbacks.
