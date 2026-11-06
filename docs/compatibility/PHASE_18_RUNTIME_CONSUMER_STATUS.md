# Phase 18 runtime consumer-status API reconciliation

Reference: Java client 5.5.1, commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`,
`client/src/main/java/org/apache/rocketmq/client/impl/factory/MQClientInstance.java`,
`getConsumerStatus(String topic, String group)`.

| Java branch | Rust implementation | Offline evidence |
| --- | --- | --- |
| Null group fails `ConcurrentHashMap.get` | `ClientRuntime::get_consumer_status(topic, group)` uses `select_consumer_nullable`; typed `InvalidRoute` | Nine null-group cases in the direct Java fixture and Consumer integration test |
| Absent group or non-Pull/Push consumer returns an empty map | Absent lookup returns empty; other `ConsumerRuntime` owners use the empty default `consumer_status` adapter | Missing, empty and custom-consumer group cases |
| Pull/Push forwards topic to the selected offset store | Pull/Push registration adapters call `OffsetStore::clone_offset_table` | Both actual Rust facades are started and compared to Java for null, empty, exact, absent, untrimmed, control, Unicode and namespace-prefixed topic inputs |
| No implicit namespace conversion | Topic passes through unchanged | Namespace-prefixed and untrimmed topic cases |
| Push raw `setOffsetStore` changes subsequent status queries | Push registration and inbound status processor read the current owner store; null maps to an error | Java replacement/null transitions, public integration test and `push_status_processor_observes_replaced_and_null_offset_stores` |
| Snapshot map is separate from the store | Owned Rust result map | Clearing the result leaves the store unchanged; later store updates are visible in the next query |
| Group lookup reflects registration/removal | Live runtime registry | Removing Pull leaves Push queryable with its independent offsets |

`RuntimeOffsetFixtures` invokes all 54 group/topic combinations on the actual
Java runtime before it starts. It also records live Push offset-store replacement
and null-store failure. This adds the missing direct public runtime capability;
the existing Broker inbound handler remains covered separately. No Broker or
Docker process is used. The fixture is `fixtures/runtime/offsets.json`, SHA-256
`0ac2cfdb7f5194c206f75aff4156643f196bd3bdb4585dda8040662efcf95a2d`.
The prior blank-filter-only fixture hash in the Phase 9 matrix is a checkpoint.

Rust maps nullable Java arguments to `Option<&str>` and Java exceptions to typed
errors. Its `ConsumerRuntime::consumer_status` method is the owner adapter for
the runtime method; it adds no client setting or independent operation.

This closes the direct consumer-status row. The broad P18-V01 review remains
open. The direct `resetOffset`, `consumerRunningInfo` and
`consumeMessageDirectly` entrypoints are now mapped in
[the runtime consumer-operation audit](PHASE_18_RUNTIME_CONSUMER_OPERATIONS.md).
Existing inbound/owner paths do not alone establish the full public runtime
API. Phase 19 live evidence remains pending.
