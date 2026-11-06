# ADR-0023: Map Java LanguageCode methods and nullable wire decoding

Status: implemented; full Phase 6 reconciliation remains open.

## Source contract

The pinned RocketMQ 5.5.1 `LanguageCode` enum exposes its 14 constants,
generated `values()` and `valueOf(String)`, inherited `name()`, `ordinal()`,
`toString()`, and `compareTo()`, plus custom `getCode()`, `getCode(String)`, and
`valueOf(byte)` methods. Java returns null from its custom name/byte lookups for
unknown inputs; generated `valueOf(String)` throws for null or unknown names.
Actual Java cases cover all constants, declaration order, defensive values-array
copies, comparison, text, boundary and negative bytes, null and case-mismatched
names, and unknown names. `RemotingSerializable.decode` also returns a command
with null language for an unknown JSON language string.

## Rust mapping

`LanguageCode` contains exactly the 14 Java constants. `values()`,
`value_of_name()`, `name()`, `ordinal()`, `Display`, and `java_compare_to()` map
the Java enum methods. `code()` maps `getCode()`, `get_code_by_name()` maps
`getCode(String)`, and `from_java_byte(i8) -> Option<LanguageCode>` maps the
custom `valueOf(byte)` lookup. There is no public raw-code variant or separate
byte conversion API.

JSON and private-header decoding now use the nullable Java lookup behavior, so
unknown wire names and byte codes decode to `None`. Since every representable
`LanguageCode` has a Java enum name and code, JSON and private-header encoders
only receive Java-defined values.

## Evidence and limits

`languageCodeMethodCases` in `fixtures/runtime/client-config.json` is generated
by invoking the pinned Java methods directly, including generated enum methods
and custom lookups. The fixture also records the actual Java unknown-name
decode result; its SHA-256 is recorded in the compatibility map and run log.

Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned
offline Maven verification, formatter, and diff checks pass. This closes
the recorded enum lookup and unknown-wire decode cases, not the remaining Phase
6 source/API/input audit or P6-V04 Phase 19 evidence.
