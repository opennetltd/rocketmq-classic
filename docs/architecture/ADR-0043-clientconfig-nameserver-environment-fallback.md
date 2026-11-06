# ADR-0043: Match ClientConfig NameServer environment fallback

Status: Accepted

## Context

RocketMQ 5.5.1 initializes `ClientConfig.namesrvAddr` through `NameServerAddressUtils.getNameServerAddresses()`, which reads the `rocketmq.namesrv.addr` system property first and falls back to the `NAMESRV_ADDR` environment variable. Rust's constructor instead used `ROCKETMQ_NAMESRV_ADDR`, a Rust-only alias not present in the pinned source contract.

## Decision

Retain the existing Rust process-environment adapter for Java system-property keys. Resolve NameServer configuration from `rocketmq.namesrv.addr` first and `NAMESRV_ADDR` second. An explicitly present empty system-property value still wins. Do not recognize `ROCKETMQ_NAMESRV_ADDR` as an alternate key.

## Evidence

The pinned Java fixture invokes `ClientConfig` construction with `rocketmq.namesrv.addr` set to `system:9876` and to an explicit empty string, and records the source constants `rocketmq.namesrv.addr` and `NAMESRV_ADDR`. The native property-input regression failed when supplied only `NAMESRV_ADDR`; after the correction it verifies fallback, precedence, explicit blank, and rejection of the Rust-only alias. Oracle/test commit `e630688`; implementation commit `07f8dee`; `client-config.json` SHA-256 `8db2067f605af9abdabfd212cf02da379f986f6021fb8a456bc50c5da8b7ecdf`.

Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify and fixture drift checks (22 scenarios), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. Other ClientConfig constructor/property and activation inputs remain under reconciliation; P6-V04 stays `PENDING_PHASE_19`.
