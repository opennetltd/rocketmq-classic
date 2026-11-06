# Apache RocketMQ Server 5.3.4 results

This report tracks Broker interoperability separately from the Java client
5.5.1 API and offline fixture audit. A passing row proves only its listed
scenario and profile.

## Pinned basic profile

- Official ASF image: [`apache/rocketmq:5.3.4` Docker Hub tag](https://hub.docker.com/layers/apache/rocketmq/5.3.4/images/sha256-a6976cd5841facfda294616a0b8797e7f41fa0292d51ad4e46b2ff224ce64995), tied to the Apache [5.3.4 release](https://rocketmq.apache.org/release-notes/2025/11/26/5.3.4/).
  Multi-platform index digest
  `sha256:d57ee4ebbd9d7bb6e185775bde41c128e98f185b25bf0b05c77d4e6c010572b8`.
- Image inspection and runtime: Ubuntu 24.04, Eclipse Temurin OpenJDK 8u472,
  `linux/arm64`; Docker Engine 29.8.2 and Compose 5.5.0 on the host. The pinned
  image ID matches the configured digest, and `mqadmin clusterList` reported
  server version `V5_3_4`.
- Observed host runtime: Docker Engine 29.8.2, Compose 5.5.0, `linux/aarch64`;
  `mqadmin clusterList` reported server version `V5_3_4`.
- NameServer and Broker health checks passed. Broker `broker-a` advertised
  `broker:10911` on the isolated Compose network.
- The scenario topic is created explicitly with four read/write queues;
  Broker auto-creation remains disabled.

## Executed scenarios

| Scenario | Historical result | Raw output |
| --- | --- | --- |
| `ROUTE-001-get-existing-topic` | PASS — Java 5.5.1 and Rust returned the same four route rows for `compat_phase0_route`, broker `broker-a`, queue IDs 0–3. The owned topic was deleted and the Compose project, network, and volume were removed. | `20261017T052003Z` (raw output not retained) |
| `ROUTE-002-topic-not-exist` | PASS — Java 5.5.1 and Rust both returned `NO_ROUTE` for `compat_phase0_missing`. The isolated Compose project was removed after the run. | `20261017T051903Z` (raw output not retained) |
| `HEARTBEAT-001` | PASS — Java 5.5.1 and Rust sent the same encoded producer heartbeat and both received Broker response version `481`. The isolated Compose project was removed after the run. | `20261017T053144Z` (raw output not retained) |
| `PROD-001-sync-send` | PASS — Java and Rust both received `SEND_OK`; Rust read the Java-produced message and Java read the Rust-produced message. Both readbacks matched topic `compat_phase0_send`, queue 0, returned queue offsets, and body `fixed`. The owned topic and isolated Compose project were removed after the run. | `20261017T054628Z` (raw output not retained) |
| `PROD-002-async-send` | PASS — Java's async callback fired once and Rust's Future resolved once; both returned `SEND_OK`. Each client read the other client's message and matched topic `compat_phase0_send`, queue 0, the returned queue offset, and body `fixed`. The owned topic and isolated Compose project were removed after the run. | `20261017T055737Z` (raw output not retained) |
| `PROD-003-oneway-send` | PASS — Java and Rust oneway calls returned without a Broker response. Broker queue max offsets advanced from 0 to 1 and then 2; Rust read the Java message at queue offset 0, and Java read the Rust message at offset 1. The owned topic and isolated Compose project, network, and volume were removed after the run. | `20261017T063258Z` (raw output not retained) |
| `PROD-004-batch-send` | PASS — Java and Rust both returned `SEND_OK` for two messages. Rust read both Java messages by their physical offset IDs at queue offsets 0 and 1; Java read both Rust messages at offsets 2 and 3. Bodies matched `batch-0` and `batch-1`. The owned topic and isolated Compose project, network, and volume were removed after the run. | `20261017T065458Z` (raw output not retained) |
| `PROD-005-send-timeout` | PASS — a local TCP proxy delayed each direct Broker response by 600 ms beyond the 100 ms client deadline. Java and Rust both reported timeout; Broker max offset reached 2, and both clients pulled the same two distinct messages. The topic and isolated two-Broker Compose project, network, and volumes were removed. | `20261025T104539Z` (raw output not retained) |
| `P7-V03` | PASS — Java and Rust each received an injected Remoting `NO_PERMISSION` response from both Brokers, then succeeded on attempt three with the same body and unique ID. Each client also exhausted a 900 ms send deadline while a proxy delayed the real Broker response by 1,800 ms. Both scenario topics were deleted before Compose teardown. | `20261025T150708Z` (raw output not retained) |
| `P3-V04` | PASS — Java/Rust raw NameServer routes and trusted/untrusted CA outcomes matched; Rust rejected a trusted wrong-host certificate, while Java 5.5.1 accepted it without endpoint identification. TLS requests used a local terminating proxy forwarding to the isolated plain Broker; all clients shut down cleanly and the project was removed. | `20261025T113910Z` (raw output not retained) |
| `P15-V03` | PASS — Java/Rust request and reply delivery matched in both directions on two Brokers. Both clients timed out with no responder; delayed opposite-language replies arrived after timeout without a second callback or retained correlation. All six scenario topics were deleted from both Brokers before Compose teardown. | `20261025T143716Z` (raw output not retained) |
| `P10-V03` | PASS — Java-seeded messages exercised Rust Push success, partial acknowledgement, retry, and failure. The failed message was delivered twice at listener counts 0/1, then reached the configured DLQ with its body/key intact and Broker `reconsumeTimes=2`. Both Brokers' scenario topic and the isolated Compose project were removed. | `20261025T153016Z` (raw output not retained) |
| `P11-V03` | PASS — A Java Push consumer first acquired all eight scenario queues. With Rust joined, the clients converged to four disjoint queues each; one transient overlapping snapshot occurred during convergence and was allowed. After Java left, Rust converged to all eight queues. The topic and isolated Compose project were removed. | `20261025T154631Z` (raw output not retained) |
| `P11-V04` | PASS — Java producer to Rust Push and Rust producer to Java Push each delivered through one retry on the other Broker. Java and Rust offset queries matched at 1 on both Brokers. Each Push client completed three start/shutdown cycles with schedulers and runtime resources quiesced; scenario topics and the isolated Compose project were removed. | `20261025T155944Z` (raw output not retained) |
| `P12-V03` | PASS — Java held orderly callbacks on both Brokers while Rust joined, acquired the transferred locks, processed redelivered messages and successors, and advanced the offsets to 2 before Java returned. No stale callback regressed either offset; both clients shut down and both queues were lockable and releasable afterward. | `20261025T173114Z` (raw output not retained) |
| `P14-V03` | PASS — Java/Rust commit messages were visible to the opposite-language pull client; both rollback bodies stayed absent and normal queue max offsets were unchanged; Unknown transactions were committed by bounded real Broker checks and read back by the opposite-language client in both directions. | `20261025T164955Z` (raw output not retained) |
| `P16-V02` | PASS — Java and Rust accepted valid ACL signatures and rejected wrong secrets and tampered heartbeat bodies. Mutual TLS accepted trusted clients and rejected untrusted CAs and missing client certificates; Rust rejected a trusted wrong-host certificate while Java 5.5.1 accepted its trusted chain. The ACL Broker ran from the checksum-verified official 5.3.4 binary distribution in a pinned amd64 JDK container; the NameServer used the pinned 5.3.4 image. | `20261025T174649Z` (raw output not retained) |
| `P17-V02` | PASS — Java/Rust duplicate-key and bounded-time results matched. Proxy-injected Broker B failure preserved the Broker A seed; terminal failures from both Brokers mapped to `NO_MESSAGE` (208). The topic and isolated Compose resources were removed. | `20261025T172205Z` (raw output not retained) |

The first batch attempt at source revision `2f9396e3` sent successfully from
Java but failed in the Rust probe because it read only the first returned
physical offset ID and treated the stored message body as a compact batch. The
diagnostic run is retained at
`20261017T064439Z` (raw output not retained);
the runner was corrected to read every returned ID before the passing run above.

The two route runs used source commit `91ecd510`; the heartbeat run used
`1a77ac68`; the sync-send run used `95e38101`; the async-send run used
`6f2a0c12`; the oneway-send run used `48ed3f58`; the batch-send run used
`8262d722`. Evidence metadata records the
full revision, toolchains, Docker platform, Compose/configuration hashes,
commands, duration, client outputs, and cleanup results. Local home/repository
paths and the host name were redacted before the raw tool output was removed.
Current
results cover route lookup, heartbeat, synchronous/asynchronous/oneway/batch
sends with cross-language readback, consumer offsets and pulls, queue lock
contention, average assignment over two live Brokers, transaction
commit/rollback visibility, bidirectional request/reply including no-responder
timeouts and late replies, producer timeout, client TLS through a local
terminating proxy, and P17-V02 multi-Broker query parity with bounded-time,
partial-failure, and terminal-error checks, plus P7-V04 bounded server-backed
producer stress. The local profiles do not reproduce owner-specific
Broker-native TLS/auth policy, replicated HA failover, or production capacity
and SLO requirements.

## Local server coverage and remaining acceptance

The scenario manifest contains 37 live cases, all with passing evidence. The
eight basic route/heartbeat/send scenarios above, `P3-V04`, the four consumer
cases, the P5 route row, `P6-V04`, `ORDER-001-lock-unlock`,
`REBAL-001-average-10q-3c`, and the two transaction rows passed. `P0-V03` also
passes as an aggregate over
eleven clean constituent runs, `P7-V02` covers high-level producer send modes,
and `P9-V03` covers manual pull and offset resume. `P7-V04` sent 8,192
fixed-seed variable-size messages from each client with a maximum of 32
concurrent sends. Both clients matched the 6,292,961-byte payload total and
checksum; Java received contiguous offsets 0–8,191 and Rust received
8,192–16,383. Each then completed three 20-send producer start/shutdown cycles
with zero pending requests, connections, or owned task groups after every
shutdown. See P7-V04 evidence (raw output not retained).
`P7-V03` verifies two-Broker retry avoidance and stable body/unique-ID
preservation after injected retryable responses, then checks each client's
bounded send deadline against a delayed real Broker response. `P10-V03`
checks Java-produced messages through Rust Push success, partial acknowledgement,
retry, and a configured Broker DLQ transition with matching body/key.
`P11-V03` verifies Java/Rust Push group join and leave on the two-Broker
profile, with stable disjoint ownership while both clients are active and full
Rust ownership after Java shuts down. One transient overlap during convergence
was tolerated.
`REQ-001-request-reply-success` checks both cross-language request/reply
directions; `P15-V03` adds no-responder and late-reply checks for both clients;
`PROD-005-send-timeout` checks commit visibility after a timed-out send.
`P5-V02-extensions` compares matching Java/Rust heartbeat, heartbeat V2,
assignment, request-mode, standard POP, ACK, batch ACK, and visibility-change
results; both
clients receive response code 3 for unsupported Lite subscription request 200071
and Lite POP request 200070. Apache 5.3.4 maps unregistered request codes to
this response in its [remoting dispatcher](https://github.com/apache/rocketmq/blob/rocketmq-all-5.3.4/remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java)
and defines code 3 as `REQUEST_CODE_NOT_SUPPORTED` in
[RemotingSysResponseCode](https://github.com/apache/rocketmq/blob/rocketmq-all-5.3.4/remoting/src/main/java/org/apache/rocketmq/remoting/protocol/RemotingSysResponseCode.java).
This matched rejection does not verify Lite success. A separate historical
supplementary run against the unmodified official 5.5.0 image passed Lite
subscription and Lite POP with matching Java/Rust results and message body;
its summary is `supplementary-lite-5.5.0/20261025T135033Z` (raw output not
retained).
The profile pins image digest
`sha256:7e8f6c9dbd9df742ed26ba69c00d4ad69e2f86a56f3ca7782ff8144dd0798132`
and enables LMQ plus multi-dispatch only in its isolated Broker config. The
[Apache RocketMQ 5.5.0 release](https://github.com/apache/rocketmq/releases/tag/rocketmq-all-5.5.0)
introduced Lite Mode. This supplementary success does not change the 5.3.4
baseline or by itself establish production support.
`P13-V02` compares the Java/Rust POP receipt lifecycle on two Brokers,
including delivery, expiry redelivery, ACK, invisibility changes, mixed queue
mapping, and malformed receipt rejection. `P14-V03` additionally passes
Java/Rust commit and rollback visibility plus Unknown resolution through actual
Broker checks in both directions. `P17-V02` compares duplicate-key and bounded
timestamp queries, then verifies partial and terminal Broker failure behavior
with injected `SYSTEM_ERROR` query replies. The query records omit Broker names,
so the check matches the unique bodies of messages explicitly seeded to each
Broker; both clients return terminal `NO_MESSAGE` code 208 after both Brokers
fail. `P12-V03` passes mixed-client orderly callback handoff, final offset, and
post-shutdown lock checks. `P16-V02` also passes ACL signature and mutual-TLS
outcomes using the checksum-verified official 5.3.4 binary distribution in an
amd64 JDK container, which includes the Linux x86_64 RocksDB JNI library that
the pinned ARM64 image lacks. All 37 registered local scenarios have passing
evidence. The two-Broker profiles cover producer stress,
retry/deadline/lifecycle, lock/unlock, average queue assignment, the P10 Push
retry/DLQ path, and the listed P17 query path. Broader query conditions and
feature-specific profiles remain incomplete. No server limitation is inferred
from an unrun case. Owner-specific network, security, HA, and production
workload acceptance remains tracked separately in MAN-01 through MAN-04.

## Current-candidate rerun — 2026-10-25

The seven implemented cases were rerun against the pinned image on candidate
revision `553564a21e4d2a40148c60637f4817c7d6f26b74`, using fresh isolated
Compose projects, recreated topics, Java 5.5.1 Maven probes, and Rust 1.85.0
probes. All seven passed and their new evidence replaced the manifest pointers:

| Scenario | Result | Evidence |
| --- | --- | --- |
| `ROUTE-001-get-existing-topic` | PASS — four matching route rows | `20261025T021228Z` (raw output not retained) |
| `ROUTE-002-topic-not-exist` | PASS — both clients returned no route | `20261025T021336Z` (raw output not retained) |
| `HEARTBEAT-001` | PASS — encoded heartbeat and response version match | `20261025T021430Z` (raw output not retained) |
| `PROD-001-sync-send` | PASS — send results and bidirectional readback match | `20261025T021526Z` (raw output not retained) |
| `PROD-002-async-send` | PASS — single completion and bidirectional readback match | `20261025T021643Z` (raw output not retained) |
| `PROD-003-oneway-send` | PASS — no-response sends reached queues and cross-readback matched | `20261025T021802Z` (raw output not retained) |
| `PROD-004-batch-send` | PASS — both messages from each client were readable by the other | `20261025T021932Z` (raw output not retained) |

The evidence records the image digest, runtime platform, server startup version,
toolchains, commands, and project cleanup. The remaining 30 scenario IDs have
no live runner, and no two-Broker, TLS, ACL, or feature-specific profile was
executed. These seven results do not establish complete client parity or final
release acceptance.

## Current consumer probes — 2026-10-25

Four basic-profile consumer cases ran on source revision
`cb351d8ae6a67b9bdb4cbf9fdacd497602e0b9a4`. Each run used a fresh Compose
project and reports `source_code_dirty=false`; topic and profile cleanup
completed successfully.

| Scenario | Result | Evidence |
| --- | --- | --- |
| `CONS-001-query-offset` | PASS — Rust read Java offset 17; Java read Rust offset 23 | `20261025T025233Z` (raw output not retained) |
| `CONS-002-pull-found` | PASS — both clients returned `FOUND`, offsets 0–1, and the same `pull-seed` response body | `20261025T031409Z` (raw output not retained) |
| `CONS-003-pull-no-new-msg` | PASS — both clients returned `NO_NEW_MSG` with empty response bodies | `20261025T031529Z` (raw output not retained) |
| `CONS-004-update-offset` | PASS — Rust read Java offset 31; Java read Rust offset 47 | `20261025T031646Z` (raw output not retained) |

One clean `CONS-004` attempt timed out on the Java update RPC and was cleaned
up without a pass; a later fresh project passed. An early `CONS-002` attempt
also exposed that direct pulls need the Java `PullSysFlag` subscription bit;
the probe now uses the same flag in Java and Rust. Neither diagnostic run is
used as a passing manifest result. The 26 remaining live cases still prevent
full Phase 19 acceptance.

## P5 existing and missing topic routes — 2026-10-25

`P5-V02-route` passed on committed revision
`6300029907b78a03cb1276a434393972330bdf0d` with
`source_code_dirty=false`. Java and Rust matched four route rows for
`compat_p5_route` and both returned `NO_ROUTE` for `compat_p5_missing`. The
scenario-owned topic and Compose project were cleaned after evidence capture:
`20261025T081906Z` (raw output not retained).
At that checkpoint, 25 cases remained and prevented full Phase 19 acceptance.

## Two-Broker lock and unlock — 2026-10-25

The `two-broker` profile starts separate `broker-a` and `broker-b` instances
from the pinned 5.3.4 image and waits for both to register with the NameServer.
`ORDER-001-lock-unlock` passed on committed revision
`6684ff8c22c92fdb0fc3af8f97574fd3e34c9a82` with
`source_code_dirty=false`. Java and Rust both acquired the same unlocked
`broker-a` queue; Rust was denied while Java held it, then acquired it after
Java's synchronous unlock. Rust's unlock allowed Java to acquire the queue
again. The scenario topic, group, project, network, and volumes were cleaned:
`20261025T083111Z` (raw output not retained).

This validates two-Broker profile startup and the listed lock behavior; it is
not replicated HA or failover evidence. Live coverage at that checkpoint was
13/37, with 24 cases pending.

## Two-Broker average queue assignment — 2026-10-25

`REBAL-001-average-10q-3c` passed on committed revision
`b347d11c5e33c18d3e443c852bc8680e83892c2f` with
`source_code_dirty=false`. Java 5.5.1 and Rust fetched the same live route of
five queues from each Broker, then produced identical assignments for three
sorted consumer IDs: 4, 3, and 3 queues. The assertion checked that all ten
unique queue IDs across `broker-a` and `broker-b` were covered. The topic,
Compose project, containers, network, and volumes were removed after capture:
`20261025T090131Z` (raw output not retained).

An earlier rehearsal at `20261025T085632Z` was a false positive: the runner
provisioned and cleaned up but did not execute either probe. Its manifest PASS
pointer was removed and the directory is retained with a diagnostic note. A
corrected dirty-source replay at `20261025T085935Z` passed the assertions but
is diagnostic-only; the clean run above is authoritative. Live coverage is
At that checkpoint, live coverage was 14/37, with 23 cases pending.

## Transaction commit and rollback visibility — 2026-10-25

`TX-001-commit` and `TX-002-rollback` passed on committed revision
`1cc67ce358d4f191b20dc33e71b59a175a57eb59`; both runs record
`source_code_dirty=false`. A dedicated transaction profile advertises its
Broker at `127.0.0.1:30911` so Java 5.5.1 and Rust can reach the same isolated
Broker from the host. Each run creates an owned topic and consumer group.

For TX-001, both transactional producers returned `COMMIT_MESSAGE`; Java and
Rust pulls matched byte-for-byte, returned `FOUND` with offsets 0–2, and
contained both `transaction-body` messages. The Broker queue max offset was
2. For TX-002, both producers returned `ROLLBACK_MESSAGE`; matching pulls
returned `NO_NEW_MSG` with empty bodies and all queue offsets at 0. Both topics
and Compose projects, networks, and volumes were removed after evidence capture:
`TX-001` (raw output not retained) and
`TX-002` (raw output not retained).

An earlier transaction replay at `20261025T091320Z` and `20261025T091433Z`
checked callbacks and queue offsets but not consumer visibility; the later
clean-source runs above include the stronger pull assertions and are the
manifest evidence. Live coverage is now 16/37, with 21 cases pending. The last
full local gate predates the transaction probes.

## Phase 0 aggregate verification — 2026-10-25

`P0-V03` passed on committed revision
`34f14b727e200016961d5189f160dc96ceb3a06e` with
`source_code_dirty=false`. It replayed 11 constituent scenarios in isolated
projects: route, heartbeat, four producer modes, four consumer/offset cases,
and two-Broker lock/unlock. Each child returned PASS, recorded the same clean
source revision, and completed its topic and Compose cleanup. The parent run
summary is `20261025T092635Z`; raw status tables and child command logs are not
retained.
No Phase 19 Docker containers, volumes, or networks remained afterward. The
individual manifest pointers were refreshed to the child runs referenced by
this aggregate. Live coverage is now 17/37, with 20 cases pending.

## High-level producer send modes — 2026-10-25

`P7-V02` passed on committed revision
`f9bfeb2aa120638a12ca0eba87c8fb2f7a4ddcec` with
`source_code_dirty=false`. The `host-routed` profile advertises the pinned
5.3.4 Broker at `127.0.0.1:40911`, allowing both high-level clients to discover
the same Broker through the NameServer. Java and Rust matched the outcome for
sync send, one async completion, oneway without a Broker response, explicit
queue 1, selector queue 2, a valid two-message batch, and local rejection of a
mixed-topic batch. The topic and Compose resources were removed after capture:
`20261025T094705Z` (raw output not retained).
Live coverage is now 18/37, with 19 cases pending.

## Shared-runtime heartbeat and unregister — 2026-10-25

`P6-V04` passed on committed revision
`1261cedd9245fe9d38b32d3ff34333bad8179284` with
`source_code_dirty=false`. Both Java and Rust started a consumer and two
producer groups on one client runtime. The Broker delivered a `DROP` message
before the subscription changed from `*` to `KEEP`, then suppressed a later
`DROP` while delivering `KEEP`. Producer A unregistered; producer B still
sent successfully and its message reached the consumer. The run exited 0 and
removed its topic and host-routed Compose resources:
`20261025T095527Z` (raw output not retained).
Live coverage is now 19/37, with 18 cases pending.

## Explicit pull statuses and offset resume — 2026-10-25

`P9-V03` passed on committed revision
`3343347dab7c640cd9fdbf24467c08dac4175580` with
`source_code_dirty=false`. Java 5.5.1 and Rust matched FOUND,
NO_MATCHED_MSG, NO_NEW_MSG, and OFFSET_ILLEGAL results, all suggesting Broker
ID 0. Both read the seeded body, returned min/max/search offsets `0/2/0`, and
resumed at offset 1 after a new consumer instance started. The run exited 0
and removed its topic and host-routed Compose resources:
`20261025T100359Z` (raw output not retained).
Live coverage is now 20/37, with 17 cases pending.

## Cross-language request/reply — 2026-10-25

`REQ-001-request-reply-success` passed on committed revision
`bb77548634ac623bd621a1afbb87a4558163c6c5` with `source_code_dirty=false`.
Java requested on `compat_phase0_request_java` and received the Rust Broker
reply; Rust requested on `compat_phase0_request_rust` and received the Java
Broker reply. Both returned exactly one reply with the expected body, matching
correlation ID, reply-to, 15-second TTL, and `reply` message type. Each
responder log records the request it consumed and the reply send result.
Both topics and the isolated host-routed Compose project were removed after
capture. Evidence is under
`20261025T103216Z` (raw output not retained).
Live coverage is now 21/37, with 16 cases pending.

## Producer timeout — 2026-10-25

`PROD-005-send-timeout` passed on clean revision
`77afd66edd59a2257d1c5772d68e86262bbd148e` with
`source_code_dirty=false`. A local TCP proxy delayed each direct Broker
response by 600 ms while both probes used the manifest's 100 ms send timeout.
Java reported `RemotingTimeoutException` at 104 ms and Rust reported its RPC
timeout at 102 ms. The sends began at queue offsets 0 and 1; Broker max offset
and both cross-language pull results confirmed exactly two committed messages,
with each expected body present once. Topic deletion and two-Broker Compose
teardown passed. No matching containers remained after cleanup. Evidence is
under `20261025T104539Z` (raw output not retained).
Live coverage is now 22/37, with 15 cases pending.

## P5 producer RPC variants — 2026-10-25

`P5-V02-producer` passed on clean revision
`e00e8c4f161509c392b6116a17a5968dd39a95e2` with
`source_code_dirty=false`. Java 5.5.1 and Rust matched `SEND_OK`, topic,
queue, and sequential offsets for raw send V1, send V2, and batch calls.
Reply V1/V2 and `endTransactionOneway` both returned the expected one-way
outcome; the reply calls used a registered scenario-owned client identity.
Java and Rust each advanced the queue by three stored messages. The topic,
reply consumer group, and basic-profile Compose project were removed. Evidence
is under `20261025T110552Z` (raw output not retained).
Live coverage is now 23/37, with 14 cases pending.

## P5 consumer RPC variants — 2026-10-25

`P5-V02-consumer` passed on clean revision
`df61d2f784176a0841839be47c8c9f02183e5948` with `source_code_dirty=false`.
Java 5.5.1 and Rust matched ordinary pull and Lite pull (`FOUND`, two
messages, min/max offsets 0/2), offset update/query at 1, consumer-list
availability, send-back success, queue lock, and unlock. The topic and
scenario-owned group were removed with the isolated two-Broker project.
Evidence is under
`20261025T111709Z` (raw output not retained).
Live coverage is now 24/37, with 13 cases pending.

## P5 extension RPCs — 2026-10-25

The clean-source attempt `20261025T115801Z` used revision
`91b0bc564029145a3a1b2193919bc551be67290e` (`source_code_dirty=false`). Java
5.5.1 successfully completed heartbeat, heartbeat V2, assignment, and
request-mode setup. Its `syncLiteSubscriptionAsync` call then received Broker
response code 3 with remark `request type 200071 not supported`. The fail-fast
runner did not execute the Rust probe, seed POP messages, or assert the other
extension outcomes, so this run makes no parity or PASS claim. The manifest
keeps `P5-V02-extensions` pending. The scenario topic and derived Lite topic
were deleted, and the isolated Compose project, network, and volume were
removed. Diagnostic evidence is under
`20261025T115801Z` (raw output not retained).
Live coverage remains 25/37, with 12 scenarios pending.

## P5 extension RPCs — 2026-10-25 clean-source pass

Run `20261025T132049Z` on clean revision
`d93c978e04c3619d3c8fe6e1c0d82c4de05f050b` passed the Java/Rust comparison.
Both clients matched heartbeat, heartbeat V2, assignment, request-mode,
standard POP, ACK, batch ACK, and visibility-change outcomes, and both
received response code 3 for unsupported Lite subscription (request 200071)
and Lite POP (request 200070). The pinned 5.3.4 dispatcher uses code 3 for
unregistered request processors ([source](https://github.com/apache/rocketmq/blob/rocketmq-all-5.3.4/remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java));
the response constant is
[`REQUEST_CODE_NOT_SUPPORTED`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.3.4/remoting/src/main/java/org/apache/rocketmq/remoting/protocol/RemotingSysResponseCode.java).
The runner deleted the parent and derived Lite topics and removed the
isolated Compose resources. Live coverage is now 27/37 with 10 scenarios
pending. Evidence: `20261025T132049Z` (raw output not retained).

## P13 POP lifecycle first attempt — 2026-10-25

The clean-source run `20261025T121016Z` used revision
`61d4686732423ded5393a54b542a6ab4d3d1d04f` (`source_code_dirty=false`). It
created the isolated two-Broker project and the scenario-owned topic, groups,
and Java queue 0/1 messages. Java's first immediate POP returned
`POLLING_NOT_FOUND`; no lifecycle assertion passed and Rust consumption did
not run. That probe omitted per-group POP request-mode setup. The retry
`20261025T121249Z` used `23a3131e` (`source_code_dirty=false`) and set POP mode
immediately before each POP, but also returned `POLLING_NOT_FOUND`. A third
run `20261025T121555Z` on `dcee59c3` prepared both clients' POP modes before
seeding and returned the same response. No attempt passed a lifecycle
assertion or started Rust consumption. All runs removed the topic, containers,
network, and volumes. Evidence is under
`20261025T121016Z` (raw output not retained),
`20261025T121249Z` (raw output not retained), and
`20261025T121555Z` (raw output not retained). A fourth
clean-source run `20261025T122129Z` on `9d196db1` added a seed-queue guard and
stopped before POP: Java requested queue 0, but the Broker returned `SEND_OK`
for `broker-a:1`, while queue 0's max offset stayed 0. No lifecycle assertion
passed and Rust consumption did not run. This identifies a Java seed-routing
mismatch; the next probe will use the route-aware queue selector. Evidence is
under `20261025T122129Z` (raw output not retained).
All runs removed the topic and isolated Compose resources. The manifest remains
pending and coverage remains 25/37, with 12 cases pending.

The clean-source retry `20261025T122748Z` on `e2c71402` used a Java
route-aware producer selector and the host-mapped Broker address. Queue 0
passed its send and max-offset checks. Queue 1 returned `SEND_OK` with queue ID
1 and offset 0, but the immediate max-offset query was still 0, so the seed
guard stopped before POP. No lifecycle assertion passed and Rust consumption
did not run. The runner removed the topic and Compose resources. Evidence is
under `20261025T122748Z` (raw output not retained). The
next attempt will allow the Broker's max-offset view to catch up after send.

The next clean-source run `20261025T123103Z` on `f81b9c79` confirmed both
queue seeds with `SEND_OK` and max offset 1. Java's first POP still returned
`POLLING_NOT_FOUND`; no lifecycle assertion passed and Rust consumption did
not run. The runner removed the topic and Compose resources. Evidence is under
`20261025T123103Z` (raw output not retained). The next
probe will avoid repeating the request-mode update after seeding and will set
the POP init mode explicitly to the queue minimum.

Run `20261025T123522Z` on `6a240da4` applied that change and again passed both
seed checks, but Java's first queue 0 POP returned `POLLING_NOT_FOUND`. No
lifecycle assertion passed and Rust consumption did not run. The runner removed
the topic and Compose resources; evidence is under
`20261025T123522Z` (raw output not retained). The next
diagnostic will record min/max queue offsets and the POP group's committed
offset around the request.

Run `20261025T123824Z` on `6837d847` added that capture: queue 0 had min/max
offsets `0/1`, the request used init mode 0, and the POP group's committed
offset was already 1 before the first POP and remained 1 afterward. Java
returned `POLLING_NOT_FOUND`, so no lifecycle assertion passed and Rust
consumption did not run. This shows the group begins past the only seeded
message. The runner removed the topic and Compose resources; evidence is under
`20261025T123824Z` (raw output not retained). The next
probe will locate when that offset is advanced by recording it after mode
preparation and after each seed.

Run `20261025T124251Z` on `1333586f` showed all six groups' queue 0 offsets
were `NOT_FOUND` after mode preparation and after each seed. Before the first
Java POP, the committed group offset was 1 while the queue was at offsets
`0/1`; POP again returned `POLLING_NOT_FOUND`. No lifecycle assertion passed
and Rust consumption did not run. This places the offset transition between
the seed checks and POP request. The runner cleaned its topic and Compose
resources; evidence is under
`20261025T124251Z` (raw output not retained). Next, the
runner will initialize the scenario groups at offset 0 after seeding and verify
that starting point before POP.

Run `20261025T124657Z` on `d68d8f12` initialized and verified offset 0 on both
queues for all six groups after seeding. The separate Java consume probe still
observed offset 1 before its first POP, with queue min/max `0/1`, and received
`POLLING_NOT_FOUND`. No lifecycle assertion passed and Rust consumption did
not run. The exact point after reset where the offset changes remains
unlocalized. The runner removed the topic and Compose resources; evidence is
under `20261025T124657Z` (raw output not retained).
Next, trace the group offset immediately after API startup and around the
min/max queries preceding POP.

Clean-source run `20261025T130156Z` on
`99d2dc68484ab8de43da86b997dc20dc49267c3c` passed `P13-V02`: Java and
Rust matched initial delivery, expiry redelivery, ACK, change-invisibility,
replacement receipt, mixed queue mapping, and malformed receipt rejection.
The runner removed the scenario topic, containers, network, and volumes. The
manifest now records PASS; current coverage is 26/37, with 11 scenarios
pending. Evidence is under
`20261025T130156Z` (raw output not retained).

P13 interpretation update — run `20261025T125153Z` on `9568a2bb` traced the
offset at each lifecycle request. The first Java delivery started with group
offset 0 and succeeded; offset 1 appeared after that delivery. The second POP,
which requests expiry redelivery, started at offset 1 and returned
`POLLING_NOT_FOUND`. Thus the earlier `pop-diagnostic` rows recorded the first
*failing* request, not necessarily the first POP in the lifecycle. Runs without
per-request stage rows do not distinguish those cases. The post-seed offset
reset works for initial delivery; expiry redelivery remains unresolved. The
runner removed the topic and Compose resources; evidence is under
`20261025T125153Z` (raw output not retained).

Run `20261025T125713Z` on `89f481e7` passed every Java P13 lifecycle step:
initial delivery, expiry redelivery after the 6-second wait, ACK, visibility
change, replacement receipt, mixed queue mapping, and malformed receipt
rejection. Rust then received the message but its direct low-level POP reply did
not contain a `POP_CK` message property; the runner stopped before any Rust
lifecycle assertion. Java's API decorates that receipt from response-header
offsets, so the Rust live probe will apply the same mapping. The isolated topic
and Compose resources were removed; evidence is under
`20261025T125713Z` (raw output not retained).


## Final Phase 19 local aggregate — 2026-10-25

At source revision 1e909440b39720e814a1db9d79d92bbc6d733713,
<code>ROCKETMQ_CLASSIC_TEST_ENV=local-disposable bash
integration-tests/run-local.sh --all</code> passed all 23 recorded gates. The
run replayed and reconciled all 37 manifest scenarios with PASS evidence; raw
aggregate output is not retained. At the time, the manifest pointed to the
latest successful per-scenario runs. It now retains only `PASS_RECORDED`
summaries without artifact paths.

Docker queries after completion found no Phase 19 project containers, volumes,
or networks. Synthetic private-key copies were removed from the local TLS run
outputs before those raw outputs were deleted. This local aggregate does not
provide the owner-specific evidence listed in MAN-01 through MAN-04.


## Final Phase 19 local aggregate — 2026-10-26

At source revision `413f735ad0ab41717acb9d76e2cb664b42076f4d`,
`ROCKETMQ_CLASSIC_TEST_ENV=local-disposable bash integration-tests/run-local.sh --all`
passed all 23 aggregate gates. The run executed every Phase 19 live scenario;
reconciliation reported 37/37 manifest rows with current clean-source PASS
evidence and the aggregate exited 0; raw output is not retained. At the time,
the manifest pointed to the latest successful per-scenario runs. It now keeps
only `PASS_RECORDED` summaries without artifact paths; raw failed and
diagnostic outputs are not retained.

After completion, Docker queries found no Phase 19 project containers, volumes,
or networks. Temporary synthetic private keys were removed from the TLS
scenario outputs before the raw outputs were deleted. The local run confirms the available macOS/Docker environment; it
does not supply MAN-01 through MAN-04 owner-network, organization-security,
production HA, or representative workload/SLO evidence.

The push-triggered GitHub Actions follow-up `40383732405` checked the same code
revision but all three jobs failed before starting, with zero runner steps.
GitHub's annotation says recent account payments failed or the Actions
spending limit must be increased. This is retained as historical run evidence:
CI run evidence (raw output not retained).
Per the user's 2026-10-27 direction, the repository's GitHub Actions CI
workflow has since been removed; remote CI is no longer part of the validation
gate. The accepted Phase 19 validation is the completed local aggregate above.
