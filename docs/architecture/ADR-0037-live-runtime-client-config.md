# ADR-0037: Expose the live runtime ClientConfig

- Status: accepted
- Date: 2026-10-08

## Context

Pinned RocketMQ 5.5.1 `MQClientManager` clones the caller's `ClientConfig` before constructing an `MQClientInstance`. The instance retains that mutable object, and `getClientConfig()` returns the same object. Runtime methods read selected settings later: `NamespaceRpcHook` reads `namespaceV2` for each request; `MQClientAPIImpl` reads language, VIP enablement, and timeout while issuing calls; and `MQClientInstance` reads heartbeat V2 and concurrent-heartbeat enablement during each heartbeat. Other settings build the remoting client, listener, or concurrent heartbeat executor during construction, and scheduled intervals are consumed when the instance starts.

Rust previously returned `&ClientConfig` from `ClientRuntime::config()`, preventing callers from mutating the runtime's retained settings and leaving request hooks/API calls on construction-time values. The current compatibility goal requires the public runtime config and supported live read sites to follow the pinned behavior.

## Decision

- Store the runtime config in a shared `Arc<RwLock<ClientConfig>>` and return that handle from `ClientRuntime::config()`. `ClientRuntimeManager` continues to clone caller input before runtime construction, so mutations through the runtime handle do not mutate the caller's original value.
- Share the same handle with the runtime-created `MQClientApi` and namespace RPC hook. Read language and namespace at request time, and read timeout, VIP, heartbeat V2, and concurrent-heartbeat enablement at their runtime call sites.
- Preserve construction and start boundaries. TLS/SOCKS transport, callback worker count, channel listener, initial NameServer list, stream hook registration, and concurrent heartbeat executor presence/width use their source-time values. The concurrent-heartbeat enable flag remains live; enabling it later when construction did not create the executor returns the Java wrapper's false outcome. Scheduled intervals are captured when `start()` registers the tasks.
- Use short synchronous lock reads and do not hold a config guard across an `await`. Callers must drop their read/write guard before invoking runtime methods that read the config.
- The immutable lower-level `RemotingClient` transport configuration remains a separate Phase 3 boundary documented by ADR-0001; this decision concerns the core `ClientRuntime` `ClientConfig` returned by `MQClientInstance.getClientConfig()`.

## Evidence

The pinned Java fixture confirms `getClientConfig()` returns the same object and that mutation through it is visible through a later getter call (`managed.configGetterSameObject` and `managed.configMutationVisible`). Native coverage includes `manager_configuration_copy_matches_invoked_java_without_mutating_caller`, `runtime_config_mutations_reach_heartbeat_language_and_namespace_hook`, `rpc_timeout_reads_the_live_runtime_configuration`, the post-construction V2 heartbeat test, the post-construction VIP lookup test, `concurrent_v1_uses_parallel_peers_and_busy_heartbeat_gate_skips_a_round`, and `enabling_concurrent_heartbeat_after_construction_has_no_executor`.

The updated `client-config.json` SHA-256 is `ad49c9b917811d1b542bf7f8b2af9ffa07ba3cde22105a00a69bd02f2ade093a`. Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verification, comparator/manifest checks, formatting, and diff checks pass.

This closes the runtime config alias/mutability gap for the audited fields. It does not complete Phase 6 bidirectional source/API/field/branch reconciliation or claim live Broker acceptance; P6-V04 remains PENDING_PHASE_19.
