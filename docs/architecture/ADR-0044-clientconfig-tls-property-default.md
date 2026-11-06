# ADR-0044: Match ClientConfig TLS property default

Status: Accepted

## Context

RocketMQ 5.5.1 keeps two TLS inputs. `TlsSystemConfig.tlsEnable` reads `tls.enable` with a false default, while `tlsMode` reads `tls.server.mode`. `ClientConfig.useTLS` initializes from `tlsEnable`. A standalone `NettyClientConfig` derives its own default from whether `tlsMode` is `ENFORCING`, but `MQClientInstance` explicitly copies `ClientConfig.useTLS` into its Netty config.

Rust `ClientConfig::with_properties` previously used the enforcing mode as the fallback for `use_tls`, so `tls.server.mode=enforcing` incorrectly enabled ClientConfig TLS when `tls.enable` was absent.

Java's `TlsSystemConfig.tlsEnable` is initialized once by the JVM. Changing `tls.enable` after that class has initialized does not change later `ClientConfig` instances, because each reads the retained static field.

## Decision

Derive `ClientConfig.use_tls` only from the `tls.enable` property, using false when absent or malformed. Keep `tls.server.mode` handling for the separate transport-mode/standalone Netty default. Runtime construction continues to use the ClientConfig value copied into the lower configuration.

Cache the process `tls.enable` value on the first Rust `ClientConfig::new` call. This mirrors Java's class-initialized value for later ClientConfig construction; the other direct property-backed fields remain per-construction inputs.

## Evidence

The isolated `ClientConfigTlsModeProbe` sets `tls.server.mode=enforcing`, clears `tls.enable`, and constructs both Java configuration classes in a fresh JVM. It then changes `tls.enable` to true and constructs a second ClientConfig. The pinned 5.5.1 fixture records false for both ClientConfig instances, `TlsSystemConfig.tlsEnable=false`, standalone `NettyClientConfig.useTLS=true`, and the runtime-copied value false. The native `client_tls_enable_uses_global_enable_not_server_mode` regression covers mode/default separation; `client_tls_property_snapshot_matches_invoked_java_class_initialization` launches a fresh Rust test process and covers the mutation lifecycle. Its test commit is `ddb56eb`; fixture `client-config-tls.json` SHA-256 `018510bf6f67c1e6a556fb0dffc1ed50ba64cab2930f4785dfa421cd466b0c68`.

Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and deterministic fixture checks), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes the TLS constructor default and class-initialization snapshot boundary. TLS credential provisioning and live acceptance remain Phase 19 work; other Phase 6 source/API/input reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.
