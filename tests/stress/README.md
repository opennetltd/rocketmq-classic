# Offline deterministic race and leak smoke

Run the bounded repeat suite with:

```bash
bash tests/stress/run-offline-smoke.sh
```

The script runs 20 barrier-, paused-time-, and scripted-peer cases three
times. It fails if a named test filter matches zero tests or any test fails.
It contacts no Broker and requires no Docker daemon.

The selected assertions cover these resource and ownership boundaries:

| Area | Test evidence |
| --- | --- |
| Remoting pending entries and opaque generations | Pending/timeout/cancel/shutdown terminal races retain one owner and leave no pending entries. |
| Remoting tasks and sockets | Start/shutdown and connection-publication races close published or unpublished sockets and join owned tasks. |
| NameServer availability scan | Saturated scan admission follows Java's configured-order prefix and scan-loop abort behavior. |
| Remoting shutdown admission | Public-executor submissions remain accepted through the channel-event worker join wait and are rejected after the wait boundary. |
| Reconnect and callbacks | GO_AWAY retry obtains a fresh connection; pending callbacks drain on shutdown. |
| Shared runtime construction | Barrier-synchronized first lookups share one runtime and transport socket; worker counters return to zero after shutdown. |
| Request/reply registry | Reply/timeout/send-failure/cancellation races claim one terminal path; admission slots become available after cleanup. |
| Concurrent producer sends | 2,000 async sends run concurrently against a scripted loopback Broker; every send completes, each body arrives once, and all message IDs are unique. |
| Consumer scheduling and offsets | Membership join/leave converges; reset fences slow callbacks and late pulls; stale lock generations cannot restore ownership. |
| Local offset persistence | 128 atomic snapshots of eight queues remain readable during the run, and all final offsets reload after restart. |
| Orderly shutdown | Slow listener/rebalance work is bounded and dropped work cannot advance committed offsets. |

The test assertions, not process memory, are the evidence for pending entries,
connections, owned tasks, admission permits, queue generations and offsets.
Broker-backed sustained producer load is covered by `P7-V04`: Java 5.5.1 and
Rust each send 8,192 fixed-seed variable-size messages with at most 32
concurrent sends, matching byte totals and checksums, contiguous offsets, and a
120-second deadline. Each client then completes three 20-send start/shutdown
cycles, with pending requests, connections, and owned task groups returning to
zero each time. The checked-in scenario manifest records the historical pass;
raw outputs are written to the local test-artifact directory and are not
committed.

`P9-V03` checks that a consumer restart resumes from the Broker-persisted
offset. `P7-V03` checks client retry across two Brokers after injected
retryable responses. Replicated Broker HA failover remains an owner-specific
Phase 19 obligation because these local Brokers do not reproduce the approved
production replica topology.

Build and test outputs are not committed. See
[the test artifact policy](../../docs/compatibility/TEST_ARTIFACTS.md).
