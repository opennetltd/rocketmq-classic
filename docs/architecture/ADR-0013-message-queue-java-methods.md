# ADR-0013: Map MessageQueue Java value methods

Status: implemented for the invoked Java 5.5.1 method cases; other DTO and Phase 6 source/input reconciliation remains open.

## Source contract

Pinned Java `MessageQueue` exposes no-argument and copy constructors, value-based `equals`, a concrete `hashCode`, and `toString`. Its hash uses the Java sequence `31 * (31 * (31 + brokerNameHash) + queueId) + topicHash`, with null strings contributing zero and signed 32-bit overflow. Its text prints null strings as `null` in `MessageQueue [topic=..., brokerName=..., queueId=...]` form.

## Rust representation

`Default` maps the no-argument constructor and `Clone` maps copying. Derived `PartialEq`/`Eq` matches same-type field equality. `java_hash_code()` returns Java's signed hash value; `Display` and Rust `ToString` provide Java's diagnostic text. Derived Rust `Hash` remains available for Rust hash collections and is distinct from the Java `hashCode` value.

## Evidence

The actual Java route fixture captures defaults, populated fields, supplementary Unicode, signed ID boundaries, equality against self/copy/null/wrong type/each changed field, hashCode, and toString. `message_queue_hash_equals_and_text_match_invoked_java` matches the supported typed comparisons, Java hash, and display text; it also verifies `MessageQueue::default()` against Java's no-argument constructor. Route fixture SHA-256: `9b3f4be54464f7718632f7bcd7d5bde88559234f2c42141a15f9f48d48bc3a1d`.
