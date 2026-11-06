# Local RocketMQ integration environment

The `basic` profile provisions the pinned Apache RocketMQ 5.3.4 multi-platform
image. The image index digest is
`sha256:d57ee4ebbd9d7bb6e185775bde41c128e98f185b25bf0b05c77d4e6c010572b8`.
The Broker advertises the Compose service name `broker`, so clients must run on
the same Compose network. Host port mappings bind only to `127.0.0.1` and are
for diagnostics; they do not make the advertised Broker address host-routable.

The `two-broker-host` profile is for host-run Java and Rust clients that need
NameServer routes to both Brokers. It advertises `127.0.0.1:50911` and
`127.0.0.1:50921`, with the NameServer mapped to host port `19876` by default.
Set `ROCKETMQ_NAMESRV_HOST_PORT` before setup to select another unused loopback
port. It uses separate volumes from the Compose-network `two-broker` profile.
Since each Broker advertises a host loopback address, run topic and group admin
commands against each Broker's own loopback address from its matching service
container (`broker-a-host` or `broker-b-host`). The profile is still an
unmodified RocketMQ 5.3.4 topology; the host route mapping is test-only.

Provisioning requires an explicit disposable-test marker and profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
bash integration-tests/setup.sh
```

The setup waits for NameServer and Broker health checks and then polls
`mqadmin clusterList` until `broker-a` is registered. Broker topic and
subscription-group auto-creation are disabled so scenarios must create their
own resources. Data is stored in a Compose-project-scoped volume.

Tear down only the matching local test project and its volume:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
bash integration-tests/cleanup.sh
```

`COMPOSE_PROJECT_NAME` may be set to isolate concurrent runs; it must start
with `rocketmq-classic-p19-`. Run one of the implemented live cases:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario ROUTE-001-get-existing-topic
```

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario ROUTE-002-topic-not-exist
```

The TLS lifecycle handoff uses the isolated TLS profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=tls \
tests/differential/run.sh --scenario P3-V04
```

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario HEARTBEAT-001
```

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario PROD-001-sync-send
```

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario PROD-002-async-send
```

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario PROD-003-oneway-send
```

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario PROD-004-batch-send
```

The runner creates a unique Compose project, provisions a manifest-owned
topic when required, then compares Java 5.5.1 and Rust route outcomes,
heartbeat bodies and Broker response versions, or synchronous/asynchronous/
oneway/batch send results, async completion counts, Broker queue offsets,
consumer-offset writes and cross-client queries, pull status/offsets/message
bodies, batch message bodies read by every returned physical offset ID,
bidirectional cross-language Broker readback, queue-lock acquisition,
contention and unlock release, live average queue assignment, and transaction
commit/rollback visibility through Java and Rust consumer pulls. `P6-V04`
checks live subscription changes, Broker-side tag filtering, producer
unregister, and continued use by the remaining producer. `P9-V03` covers
explicit pull statuses, suggested-broker selection, offset-boundary RPCs, and
resuming from a broker-persisted offset after a consumer restart.
`REQ-001-request-reply-success` checks Java request/Rust reply and Rust
request/Java reply on separate scenario-owned topics, including the correlated
reply fields and each responder's observed request.
`P15-V03` repeats both successful directions on the host-routed two-Broker
profile, then checks no-responder timeouts in both clients and late replies
after timeout without a second callback or retained correlation.
`P3-V04` invokes a raw route on the local NameServer, then sends Java and Rust
RocketMQ requests through a locally generated TLS terminator that forwards to
the disposable plain Broker. Both clients verify the configured CA chain,
accept the trusted certificate with a wrong hostname as Java 5.5.1 does, reject
an untrusted CA, and report zero retained remoting resources at shutdown.
`P7-V04` uses two host-routed Brokers for a fixed-seed workload of 8,192
variable-size messages per client, with at most 32 concurrent sends. It checks
matching payload bytes and checksum, contiguous Broker queue offsets, and a
120-second deadline. Each client then completes three additional 20-send
producer start/shutdown cycles with pending requests, connections, and task
groups returning to zero after each shutdown.
`P11-V04` sends Java messages through Rust Push on Broker A and Rust messages
through Java Push on Broker B; both listeners retry once before succeeding.
Java and Rust offset queries match at 1 on each Broker, and three Push
start/shutdown cycles per client finish with no scheduler/runtime work retained.
`P7-V03` uses two host-routed Brokers and transparent response proxies. Each
proxy injects one Remoting `NO_PERMISSION` response without forwarding that
send, then forwards the client's retry to the other Broker; Java and Rust
must recover on attempt three while preserving the original body and unique
ID. Each client then hits a 900 ms send deadline against a delayed real Broker
response. The proxies remain active through topic cleanup and disposable
project teardown.
`PROD-005-send-timeout` delays each direct Broker response beyond the reviewed
100 ms deadline and verifies the timed-out sends were each committed once by
cross-language pull readback.
`P5-V02-producer` compares raw producer V1, V2, batch, reply V1/V2, and
end-transaction calls on the pinned Broker.
`P5-V02-consumer` compares raw pull and Lite pull, offset query/update,
consumer-list, send-back, and queue lock/unlock calls on the two-Broker profile.
The 5.3.4 extension case records matching unsupported Lite subscription and
POP responses. To exercise the successful Lite subscription and POP paths, run
the separate unmodified official RocketMQ 5.5.0 profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
bash tests/differential/run-p5-lite-supplementary.sh
```

That runner pins the multi-platform 5.5.0 image digest and uses an isolated
Broker config with LMQ and multi-dispatch enabled. Clean run
`20261025T135033Z` passed matching Java/Rust Lite subscription and Lite POP
success. It archives its own results and does not change the mandatory 5.3.4
matrix or by itself establish production support. It removes its isolated
topics, containers, network, and volume after the run. Toolchain,
configuration, output, and cleanup records are written outside the checkout to
`ROCKETMQ_CLASSIC_EVIDENCE_DIR` or a system temporary directory. A manifest row
becomes `PASS` in the local run copy only after comparison and cleanup succeed.

The runner now implements all 37 manifest scenarios. The full local aggregate
run reconciled every row after comparison and cleanup; the checked-in manifest
records `PASS_RECORDED` summaries, while raw test artifacts remain local and
are not committed. Set `ROCKETMQ_CLASSIC_EVIDENCE_DIR` to choose an output
directory outside the checkout. This establishes the registered local
compatibility matrix only; owner-specific deployment validation remains in
[docs/owner-validation.md](../docs/owner-validation.md). See
[test artifact handling](../docs/compatibility/TEST_ARTIFACTS.md).
`--phase N` and `--all` fail closed when any selected case has no runner.

The two-Broker lock case can be replayed with:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=two-broker \
tests/differential/run.sh --scenario ORDER-001-lock-unlock
```

The two-Broker average rebalance scenario can be replayed with:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=two-broker \
tests/differential/run.sh --scenario REBAL-001-average-10q-3c
```

The local transaction visibility cases use the host-routed `transaction`
profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=transaction \
tests/differential/run.sh --scenario TX-001-commit
```

Run the rollback case by replacing the scenario ID with `TX-002-rollback`.
The checks compare Java/Rust transaction results and pulls; commit requires two
visible messages and rollback requires an empty queue.

The high-level producer overload matrix uses the host-routed profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=host-routed \
tests/differential/run.sh --scenario P7-V02
```

It checks sync, async, oneway, explicit-queue, selector, valid-batch, and
mixed-topic batch rejection behavior against the same isolated Broker.

The high-level producer retry and deadline check uses the host-routed
two-Broker profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=two-broker-host \
tests/differential/run.sh --scenario P7-V03
```

The shared-runtime lifecycle check also uses the host-routed profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=host-routed \
tests/differential/run.sh --scenario P6-V04
```

It verifies that changing the consumer filter takes effect at the Broker and
that producer B remains functional after producer A unregisters.

The explicit pull and offset-resume checks use the same host-routed profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=host-routed \
tests/differential/run.sh --scenario P9-V03
```

They compare all four pull statuses, suggested broker ID, min/max/search
offsets, and the next offset after a new consumer instance starts.

The Phase 15 request/reply case uses the host-routed two-Broker profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=two-broker-host \
tests/differential/run.sh --scenario P15-V03
```

It checks both successful directions, no-responder timeouts, and delayed
opposite-language replies after timeout, then deletes its six scenario topics
from both Brokers.

P16-V02 uses the pinned 5.3.4 NameServer and a local container built from the
official Apache 5.3.4 binary distribution. The builder checks the published
SHA-512 before extracting the archive and pins its amd64 Temurin 8 JDK base image.
The official 5.3.4 distribution contains the Linux x86_64 RocksDB JNI library
used by the ACL metadata provider; the pinned linux/arm64 Docker image does not
contain a Linux ARM64 native library. Docker amd64 emulation is required on an
ARM64 host. The scenario runner builds this test-only container automatically
and still runs Java and Rust against RocketMQ 5.3.4; no Broker code or
distribution files are patched.

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=tls-auth \
bash tests/differential/run.sh --scenario P16-V02
```

Live runs require the pinned runner toolchains: Maven on JDK 27 and Rust 1.85.0,
matching the repository CI configuration.

The full local entry point is:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
bash integration-tests/run-local.sh --all
```

It records and runs all server-free gates first. It then checks every Phase 19
manifest row before starting any Docker resource. Missing scenario runners
produce a `BLOCKED` result with the missing IDs in the local log; the full
matrix cannot report success until all IDs pass. The recorded 2026-10-26 local
aggregate passed 23 gates and all 37 live cases, including `P16-V02` through
the verified Apache 5.3.4 binary-distribution profile described above.
`P5-V02-extensions` passed the 5.3.4 parity case with matching unsupported
responses for Lite subscription and Lite POP; positive Lite success-path
coverage passed separately on the official 5.5.0 image and is not part of the
5.3.4 manifest count. The checked-in manifest retains these historical results
as `PASS_RECORDED`; raw outputs are not committed. A fresh aggregate run against
the current revision remains pending. Starting a profile by itself does not
mark a scenario passed.

All run outputs stay outside the checkout. See
[the test artifact policy](../docs/compatibility/TEST_ARTIFACTS.md) for the
default location and `ROCKETMQ_CLASSIC_EVIDENCE_DIR` override.
