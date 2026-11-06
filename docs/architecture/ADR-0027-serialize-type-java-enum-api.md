# ADR-0027: Map the Java SerializeType enum API

Status: implemented; full Phase 6 reconciliation remains open.

## Source contract

The pinned RocketMQ 5.5.1 `SerializeType` enum exposes `JSON` and `ROCKETMQ`,
generated `values()`/`valueOf(String)`, inherited `name()`, `ordinal()`,
`toString()`, and `compareTo()`, plus custom `valueOf(byte)` and `getCode()`.
The custom byte lookup returns null for unknown signed bytes. Generated string
lookup throws for null and unknown names, with the Java enum constant message.

`RemotingCommand.getProtocolType(int)` returns null for an unknown marker byte.
Its private `headerDecode` then dereferences the null enum during `ordinal()`.

## Rust mapping

`SerializeType` has exactly two variants. `values()`, `value_of_name()`,
`name()`, `ordinal()`, `Display`, and `java_compare_to()` map the generated and
inherited enum methods. `code()` maps `getCode()`, and
`from_java_byte(i8) -> Option<SerializeType>` maps the custom nullable lookup.
There is no public unknown-code variant or raw-code constructor.

For an unknown frame marker, Rust returns the typed
`CodecError::UnsupportedSerialization(code)` before creating a public enum
value. This keeps malformed input out of the API while following the same
unsupported-marker path in the decoder.

## Evidence and limits

`serializeTypeMethodCases` in `fixtures/runtime/client-config.json` invokes
Java methods for both constants, declaration order, defensive values-array
copies, string/byte lookups, text, ordinal comparison, and exact lookup
exceptions. The full frame decoder's Java null dereference is confirmed in the
pinned 5.5.1 bytecode; Rust's typed error is the native error representation.

Protocol enum tests compare the direct fixture cases. Remoting codec tests
verify that an unknown wire marker returns `UnsupportedSerialization` and that
known JSON and RocketMQ frames continue to decode. Full workspace, strict
Clippy, pinned offline Maven verification, formatter, and diff checks pass.
This maps the enum surface and recorded marker behavior, not the remaining
Phase 6 API/input audit or P6-V04 Phase 19 evidence.
