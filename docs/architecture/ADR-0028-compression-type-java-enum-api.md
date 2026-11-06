# ADR-0028: Match the Java CompressionType enum API

Status: implemented; full Phase 6 reconciliation remains open.

## Source contract

Pinned RocketMQ 5.5.1 `CompressionType` has exactly `LZ4`, `ZSTD`, and
`ZLIB`. The enum exposes its generated and inherited value/name/order/text/
comparison methods, plus `getValue`, `getCompressionFlag`, `of(String)`, and
`findByValue(int)`. `of` removes Java `String.trim()` characters (UTF-16 units
at or below U+0020), uppercases the name, and throws for unsupported names.
`findByValue` maps 0 and 3 to `ZLIB`, 1 to `LZ4`, and 2 to `ZSTD`; other values
throw `RuntimeException` with the value in the message.

`MessageSysFlag.getCompressionType(int)` masks bits `0x700`, shifts by eight,
and delegates to `findByValue`. Thus an unset compression tag (0) resolves to
`ZLIB`, and masked values 4–7 fail.

## Rust mapping

`CompressionType` has only the three Java variants and provides Rust-shaped
counterparts for the enum methods. `compression_type(flag)` returns
`Result<CompressionType, CompressionTypeError>` so unknown wire flag values
stay out of the public enum. The error type retains lookup categories and
stable Java lookup messages; `of(null)` is a typed null-name error because the
JDK-generated message is not stable across runtime versions.

## Evidence and limits

`compressionTypeMethodCases` in `fixtures/runtime/client-config.json` directly
invokes Java enum methods, both name lookup methods, integer lookup, and
`MessageSysFlag.getCompressionType`. It covers each constant, defensive
`values()` copies, unknown values and a non-ASCII space that Java `trim()` does
not remove. The native common-crate differential checks every case. Focused
common tests, full workspace tests, strict Clippy, pinned offline Maven
verification, formatter, and diff checks pass. This closes this enum mapping,
not the remaining Phase 6 audit or P6-V04 Phase 19 handoff.
