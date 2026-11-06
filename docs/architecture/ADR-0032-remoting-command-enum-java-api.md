# ADR-0032: Map remoting command enum APIs

Status: implemented; full Phase 6 reconciliation remains open.

## Source contract

RocketMQ 5.5.1 exposes `RemotingCommandType` with `REQUEST_COMMAND` and
`RESPONSE_COMMAND`, and `RequestType` with `STREAM` and byte code `0`. Both
provide generated `values()`/`valueOf(String)` and inherited enum name, ordinal,
text, and comparison behavior. `RequestType.valueOf(byte)` returns null when
the byte is unknown, and `getCode()` returns the stored byte. Public
`RemotingCommand.getType()` selects request or response type from its response
flag.

## Rust mapping

The remoting crate now exports `RemotingCommandType` and `RequestType` with the
matching Java enum methods. `RequestType::from_java_byte(i8)` preserves the
nullable byte lookup as `Option<RequestType>`, and `get_code()` returns the
signed Java byte. `RemotingCommand::get_type()` maps the current response flag.
The stream request hook obtains `ReqT` from `RequestType::Stream.get_code()`
instead of a duplicated string literal.

## Evidence and limits

`remotingEnumMethodCases` in `fixtures/runtime/client-config.json` directly
invokes both enums, byte values at signed boundaries, request/response command
type selection, and explicit response marking. The native command unit
differential checks 22 Java cases, including exact lookup errors, ordinal
comparisons, defensive `values()` copies, and unknown-byte null results. Full
workspace tests, strict Clippy, pinned offline Maven verification, formatter,
and diff checks pass. This maps the listed remoting APIs and stream hook input;
full Phase 6 source/API reconciliation remains open.
