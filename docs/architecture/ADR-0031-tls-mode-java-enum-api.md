# ADR-0031: Map Java TlsMode enum APIs

Status: implemented; full Phase 6 reconciliation remains open.

## Source contract

RocketMQ 5.5.1 exposes `org.apache.rocketmq.remoting.common.TlsMode` with
`DISABLED`, `PERMISSIVE`, and `ENFORCING` in that declaration order. The enum
has `getName()` values `disabled`, `permissive`, and `enforcing`. Its static
`parse(String)` performs exact, case-sensitive matching and returns
`PERMISSIVE` for null or any unrecognized input. Generated `values()` and
`valueOf(String)`, plus inherited `name()`, `ordinal()`, `toString()`, and
`compareTo()`, keep normal Java enum behavior.

## Rust mapping

The public remoting API exports `TlsMode` with matching variants and order,
`values`, `value_of_name`, `name`, `ordinal`, `Display`, `java_compare_to`,
`get_name`, and `parse(Option<&str>)`. The Java null and unknown lookup errors
use the shared typed enum lookup error. `TransportMode` now uses `TlsMode::parse`
to decide whether an absent `tls.enable` setting inherits enforcing mode;
explicit `tls.enable` values continue to override that mode.

## Evidence and limits

`tlsModeMethodCases` in `fixtures/runtime/client-config.json` directly invokes
all three constants, defensive `values()` copies, string lookups, `getName()`,
and `parse()` for null, exact names, case mismatches, whitespace, empty, and
unknown values. The remoting unit differential checks the captured results,
including exact Java lookup error messages. Full workspace tests, strict
Clippy, pinned offline Maven verification, formatter, and diff checks pass.
This maps the `TlsMode` API and its use in transport selection; Phase 19 still
owns live TLS provisioning and end-to-end acceptance. Whole Phase 6
reconciliation remains open.
