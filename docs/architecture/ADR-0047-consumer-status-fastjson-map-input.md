# ADR-0047: Match Java consumer status Map decoding

Status: Accepted

## Context

Pinned RocketMQ Java 5.5.1 declares `GetConsumerStatusBody.messageQueueTable` as `Map<MessageQueue, Long>` and `consumerTable` as `Map<String, Map<MessageQueue, Long>>`. The constructor initializes both outer maps as empty. Explicit null outer maps, null nested consumer maps, and null `Long` values survive decode; omitted fields retain their initialized empty maps. Fastjson writes `MessageQueue` instances directly as object map keys, producing the non-standard body form `{{"brokerName":"b","queueId":1,"topic":"t"}:9}`. Decode accepts that form, rejects a JSON string containing serialized queue JSON as the key, and applies last-value wins to duplicate queue keys and duplicate body fields. Encoding omits null map fields, null nested values, and null `Long` entries. When a `MessageQueue` is a map key, null `topic` and `brokerName` fields are omitted while primitive `queueId` is emitted.

## Decision

Represent nullable queue tables as `Option<Vec<(MessageQueue, Option<i64>)>>` and the consumer table as `Option<BTreeMap<String, Option<Vec<(MessageQueue, Option<i64>)>>>>`. The vectors retain queue/value rows while allowing Java MessageQueue equality to collapse duplicates during decode; the inner `Option<i64>` represents nullable Java `Long`. `Default` uses `Some(empty)` for both outer maps, matching the Java constructor. Accessors and setters preserve the optional states.

Add `GetConsumerStatusBody::decode_java_json` for the Fastjson body syntax. Its narrow parser rewrites object-valued map keys into a JSON-compatible intermediate form, rejects quoted queue-object keys, and then uses the raw-object last-wins decoder. `encode_java_json` emits the object-key wire syntax and omits explicit-null maps, null nested maps, and null `Long` entries. Standard Serde serialization remains a JSON representation of the owned Rust fields; it is not the Java wire encoder.

Rust's BTreeMap ordering makes consumer rows deterministic; Java HashMap iteration order is unspecified. This mapping does not claim Java object identity or a portable Java HashMap ordering contract.

## Evidence

The direct pinned Java fixture invokes `GetConsumerStatusBody.decode` and `encode` for 143 inputs, covering every JSON token type for both tables, non-object roots, omitted/null/empty fields, object-valued and quoted queue keys, nested tables/nulls, duplicate keys/fields, malformed types, nullable/omitted `MessageQueue` key fields, all seven token shapes for each queue-key field, and Java `Long` coercions. Long values accept booleans as 1/0, numeric strings including a leading plus, and decimal strings with truncation; whitespace-surrounded numeric strings reject and an empty string becomes null. Fractions truncate toward zero, plain decimal overflow wraps to the low 64 bits, extreme exponent values and numeric strings follow Java double-to-long conversion, including saturation and underflow to zero, and integer overflow rejects. Null entries decode and are omitted by Java encoding. Null queue strings are omitted from encoded keys; missing/null queue IDs decode as zero, and queue ID arrays reject. Key `topic`/`brokerName` scalar, array and object values follow Java String coercion. Native differential checks acceptance, normalized maps and exact encoded body strings. Fixture `fixtures/runtime/lookup.json` SHA-256: `202dc2d502473e2e2a73861bfcc82e359668c8f5222744b619052be04c9c014f`. Earlier shape-test checkpoints: `a13cf18`, `4ac436e`, `030f7a5`; coercion test commits: `76cc4eb`, `c7e1e8c`, `9cfd106`, `2260129`, `85cd833`; implementations: `f6a75eb`, `eadcb33`.

Phase 6's broader source/API/settings/input reconciliation remains open; the matrix remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.
