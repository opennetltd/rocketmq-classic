# ADR-0048: Match Java ResetOffsetBody Fastjson Map decoding

Status: Accepted

## Context

Pinned RocketMQ Java 5.5.1 declares `ResetOffsetBody.offsetTable` as `Map<MessageQueue, Long>`. The constructor initializes it to an empty map, so an omitted field decodes as empty and encodes as `{"offsetTable":{}}`; explicit null survives decode and is omitted by Fastjson encoding (`{}`). Fastjson writes `MessageQueue` instances directly as object map keys, producing the non-standard form `{"offsetTable":{{"brokerName":"b","queueId":1,"topic":"t"}:9}}`. Decode accepts this form, rejects a JSON string containing serialized queue JSON as the key, and applies last-value wins to duplicate queue keys and duplicate body fields. A null `Long` map value is accepted and retained after decode, then omitted during encoding. Null `topic` and `brokerName` fields in a `MessageQueue` map key are omitted by Fastjson; its primitive `queueId` is always emitted, using zero when absent or null.

## Decision

Represent the nullable table as `Option<Vec<(MessageQueue, Option<i64>)>>`. `Default` uses `Some(empty)` to match Java constructor state; the outer `Option` preserves explicit null, and the inner `Option` preserves nullable Java `Long` values. Add Java-shaped table accessors, `decode_java_json` for Fastjson object-valued keys and raw last-wins fields, and `encode_java_json` that omits null table entries and null string fields inside queue keys while retaining primitive `queueId`. Queue rows are normalized through a `BTreeMap`, so Rust ordering is deterministic and duplicate queue keys collapse last-wins; Java `HashMap` iteration order is unspecified.

Standard Serde serialization remains the JSON representation of Rust-owned fields. `encode_java_json` is the Java body-wire encoder.

## Evidence

Seventy-seven direct pinned Java fixture cases invoke `ResetOffsetBody.decode` and `encode`, covering every JSON token type for `offsetTable`, non-object roots, omitted/null/empty tables, Fastjson and quoted queue keys, duplicate keys and fields, unknown fields, invalid table shapes and keys, nullable offsets, nullable/missing `MessageQueue` key fields, all seven queue-key field token shapes and Java `Long` coercions. Long values accept booleans as 1/0, numeric strings including a leading plus, and decimal strings with truncation; whitespace-surrounded numeric strings reject and an empty string becomes null. Fractions truncate toward zero, plain decimal overflow wraps to the low 64 bits, extreme exponent values and numeric strings follow Java double-to-long conversion, including saturation and underflow to zero, and integer overflow rejects. Null entries decode and are omitted by Java encoding. Null queue names are omitted from key objects; queue-key string fields use Java String coercion, while primitive queueId accepts numeric strings, truncates fractions, maps Boolean values, defaults null/objects to zero and rejects arrays. Native differential checks acceptance, normalized rows, and exact encoded body strings. Fixture `fixtures/runtime/lookup.json` SHA-256: `202dc2d502473e2e2a73861bfcc82e359668c8f5222744b619052be04c9c014f`. Earlier shape-test commits: `3d517eb`, `d8a027b`; coercion/key test commits: `76cc4eb`, `c7e1e8c`, `9cfd106`, `2260129`, `85cd833`; implementations: `f6a75eb`, `eadcb33`.

Phase 6's broader source/API/settings/input reconciliation remains open; the matrix remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.
