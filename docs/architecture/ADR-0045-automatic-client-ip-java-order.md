# ADR-0045: Match Java automatic client-IP ordering on macOS

Status: Accepted

## Context

RocketMQ 5.5.1 initializes `ClientConfig.clientIP` with `NetworkUtil.getLocalAddress()`. The helper filters loopback addresses, prefers IPv4, selects the first address classified as external, and otherwise selects the final IPv4 candidate. The native automatic constructor uses the same selection policy over OS interface addresses.

On the pinned macOS/JDK host, Java and Rust received the same private IPv4 candidates in opposite order. Java selected `10.10.10.111`; Rust `ClientConfig::new_auto()` selected `192.168.139.3`.

## Decision

On macOS, reverse only the IPv4 candidate sequence returned by `getifaddrs` before applying the existing selection policy. Keep IPv6 order and the generic selection function unchanged. Other operating systems retain their current interface adapters.

## Evidence

The Java fixture directly records `NetworkUtil.getLocalAddress()`, ordered eligible candidates, and the no-argument `ClientConfig.getClientIP()`. The native `automatic_client_ip_matches_invoked_java_network_util_and_constructor` regression failed with `192.168.139.3` before implementation `5791e28`; oracle/test commit `482e02d`. A macOS-only unit test verifies that the adapter reverses IPv4 candidates while leaving IPv6 entries in place. Fixture `client-config.json` SHA-256: `138164047171a6e1bb649d3b70239a023245341852cd4036148d80d3c722b5ea`.

The pinned `NetworkUtil.getLocalInetAddress()` bytecode catches exceptions from interface enumeration, selection, and `InetAddress.getLocalHost()`, logs the failure, and returns `null`; the no-argument `ClientConfig` constructor stores that return value directly in `clientIP`. `ClientConfig::new_auto` now preserves its Rust `Result` signature but builds a configuration with `client_ip: None` when native enumeration or hostname resolution fails. `automatic_client_ip_failure_preserves_java_null_default` injects that failure and checks the resulting null-valued setting and `null@DEFAULT` client ID.

## Unix interface running-state filter (2026-10-10)

RocketMQ's pinned `NetworkUtil.getLocalInetAddressList()` bytecode accepts an interface only when `NetworkInterface.isUp()` is true. OpenJDK's Unix native implementation defines that result as both `IFF_UP` and `IFF_RUNNING` ([OpenJDK `NetworkInterface.c`](https://github.com/openjdk/jdk/blob/master/src/java.base/unix/native/libnet/NetworkInterface.c#L3056-L3065)); the Java 27 API describes it as “up and running” ([Java 27 API](https://download.java.net/java/early_access/jdk27/docs/api/java.base/java/net/NetworkInterface.html)). The Unix adapter previously tested only `IFF_UP`; it now requires both flags. `interface_filter_matches_java_up_running_and_exclusions` covers UP-only, RUNNING-only, both flags, and the existing point-to-point, alias, and bridge exclusions. All eight `local_address` tests pass on the current macOS host. This closes the source-level flag predicate; non-macOS enumeration order and an actual IPv6-only host remain unverified.

Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and deterministic fixture checks), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes the observed pinned macOS IPv4 case. Non-macOS enumeration and IPv6-only fallback remain under reconciliation; this is not Phase 6 acceptance, and P6-V04 remains `PENDING_PHASE_19`.

## Candidate-list selection audit (2026-10-09)

The existing direct Java fixture also records the ordered result of `NetworkUtil.getLocalInetAddressList()`. A macOS-only differential now compares the IPv4 candidate sequence after the native OS adapter, then compares Java and Rust selected addresses over the full current candidate lists and over their IPv6-only subsets. Java and native full interface enumeration order differs, but the pinned host's IPv4 sequence and selected result match; the observed IPv6-only fallback also selects the same address. This host comparison alone does not prove non-macOS adapter behavior or an actual IPv6-only host; the pinned bytecode selection branch is audited below.


## IPv6 ordered-candidate branch from pinned bytecode (2026-10-09)

The pinned RocketMQ 5.5.1 NetworkUtil.getLocalInetAddress bytecode partitions candidates into IPv4 and IPv6 lists while preserving each family's order. With no IPv4 candidates, it returns the first IPv6 candidate for which UtilAll.isInternalV6IP is false; if every IPv6 candidate is internal, it returns index zero. Rust select follows these branches. Regression 8632ec69 adds two external IPv6 candidates in a known order and asserts the first is selected, alongside the existing all-internal fallback case. The source-backed selector branch is now covered; OS interface enumeration outside macOS and an actual IPv6-only host remain open.
