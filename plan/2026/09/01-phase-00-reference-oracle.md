# Plan: Phase 0 — Freeze the reference and test oracle

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 0; execution index: [`README.md`](README.md).
> Translation: [`01-phase-00-reference-oracle.zh-tw.md`](01-phase-00-reference-oracle.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## Execution contract

- Port **all Java client 5.5.1 settings, features and control flows**, including defaults, validation, callbacks, errors, retries and lifecycle. Map Java source/method/configuration to Rust and unit-test IDs in both directions; no extra public Rust setting/feature and no missing Java client capability. Rust naming/ownership may differ, not observable semantics. Internal test seams stay private.
- Implement the **client only**. Java client 5.5.1 remains the oracle; **RocketMQ server V5_3_4 (5.3.4)** is the production/local Docker baseline, not the client source baseline. A server limitation does not authorize deleting a client feature; compare both clients against the same server and retain source/fixture coverage for unavailable success paths.
- **Phase 0–18 primarily use unit tests**, supplemented by Java golden/offline differential tests and small scripted loopback tests where transport requires them. No live Broker or Docker is a phase prerequisite. Broker, TLS/auth cluster, cross-language, end-to-end and server stress execution belongs to [Phase 19](20-phase-19-docker-local-validation.md); non-local checks become owner-executed manual items there. Phase acceptance is not final compatibility/release acceptance.

## 1. Goal and scope

Build the executable offline Java oracle and complete source/settings/feature inventory before Rust protocol implementation. Docker cluster creation and live scenario execution are Phase 19 work.

## 2. Prerequisites and current-state analysis

The repository currently contains only the master plan and SVG diagrams: no Cargo workspace, Java harness, fixtures or tests exist. Treat the release/tag/commit in the master plan as the requested baseline, not as independently verified evidence.

- Read/complete first: [Execution index](README.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `RemotingCommand`
- `RocketMQSerializable`
- `MQClientAPIImpl`
- `MQClientInstance`
- `DefaultMQProducerImpl`
- `PullAPIWrapper`
- `ProcessQueue`
- `RebalanceImpl`
- `RemoteBrokerOffsetStore`
- `LocalFileOffsetStore`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
compat/java-reference/pom.xml
compat/java-reference/README.md
compat/java-reference/src/main/java/compat/Main.java
fixtures/README.md
fixtures/remoting/
fixtures/messages/
tests/differential/scenarios.yaml
tests/differential/compare.py
docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md
docs/compatibility/REQUEST_CODES.md
docs/compatibility/KNOWN_DIFFERENCES.md
```

The oracle CLI has `fixtures --output <dir>`, `decode --input <file>`, `scenario --id <id> --output <file>` and `self-test` commands. Invoke it with Maven exec and `compat.Main`; one result JSON per scenario. Store `scenarios.yaml` as a JSON-compatible YAML document so the Python standard library can read it without a YAML dependency. Each entry records ID, required profile, owning phase, controlled inputs and the exact normalization rules. The Python comparator compares objects without key-order sensitivity and preserves array order unless that specific field is explicitly a set. Never normalize away error codes, queue assignment, retry counts, ordering or offset relationships.

## 4. Ordered implementation tasks

### 4.1 P0-00 — Verify the baseline before generating evidence

Resolve `rocketmq-all-5.5.1` to its peeled commit and compare with `efa1be59d6fc747ad220f036c86030f05f0419f9`. Resolve Maven `org.apache.rocketmq:rocketmq-client:5.5.1` and matching source artifacts. Record artifact SHA-256, tag/commit, JDK/Maven versions and source paths in the harness README. Do not substitute `develop`, another artifact version or an unverified Docker image if retrieval fails; block protocol work for owner review.

### 4.2 P0-01 — Create evidence and scope inventories

Create the master-plan matrix columns, adding Java setting name/type/default/unit/range/validation, public method/overload, source control-flow branches, Rust mappings and unit-test IDs. Audit Java-to-Rust completeness and Rust-to-Java absence of extra public features/settings. Include inherited, deprecated and environment/system-property configuration paths. All client capabilities are mandatory, including Lite/POP, allocation strategies, admin, namespace, hooks, security and trace; no optional scope exclusions.

### 4.3 P0-02 — Implement protocol fixture I/O

Use Java public APIs or a narrowly documented same-package adapter where the release requires it. Generate JSON and ROCKETMQ commands with explicit opaque, timestamps and properties where possible. Write raw full frames plus decoded semantic JSON, body length and body SHA-256. The decoder accepts both serializations and fails on malformed input. Add self-tests to Maven verify; do not rely on disabled Java assertions.

### 4.4 P0-03 — Create deterministic scenarios and comparator

Register all Phase 0 scenario IDs plus heartbeat, offset update and negative headers. Implement offline fixture/allocation/header scenarios and comparator self-tests now. Register live route/send/pull/offset/lock scenarios with execution_phase=19, required profile and isolated topic/group requirements; pending live cases never report PASS.

### 4.5 P0-04 — Specify Phase 19 environment requirements

Record server_version=5.3.4 separately from java_client_version=5.5.1 in scenario metadata. Inventory basic, two-Broker, TLS/auth and feature-specific environment needs for P19-01/02. Do not create/start Docker or require live server evidence here. Unit-test manifest validation and missing-prerequisite reporting.

### 4.6 P0-05 — Check in reproducible evidence

Use Appendix B fixture names. Every .bin has a .json sibling with source version, scenario, serialization, request code, normalized fields, payload SHA-256 and creation command; preserve raw captures separately from normalized results. Require route, heartbeat, send, pull, offset query/update and lock/unlock fixture families now. Later phases extend this inventory before coding their feature. Wire a dependency/version check into Maven verify and document fixture regeneration and Java licensing/NOTICE provenance.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Run these checks without Docker or a live Broker; use Java fixtures, controlled clocks and scripted peers. Shared commands and evidence rules are in the execution index.

- [x] **P0-V01** — Run fixture generation twice; metadata hashes match raw files and normalized semantic results match.
- [x] **P0-V02** — Java decodes both codec families it produced; malformed fixtures fail; comparator self-test detects meaningful differences.
- [x] **P0-V04** — Changing the Maven client version causes verification to fail; no scenario with missing prerequisites reports PASS.

- [x] **P0-U01** — Fixture construction covers route, heartbeat, send, pull, offsets and lock/unlock without a Broker; scenario metadata separates client/server versions and pending live evidence.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
mvn -f compat/java-reference/pom.xml verify
mvn -f compat/java-reference/pom.xml exec:java -Dexec.mainClass=compat.Main -Dexec.args="fixtures --output target/java-fixtures"
python3 tests/differential/compare.py --self-test
```

### 5.2 Phase 19 live-test handoff

Retain these case IDs and behavior requirements for Phase 19 execution against Docker server 5.3.4; they are not this phase's pass conditions and must not be marked PASS early.

- [ ] **P0-V03** — Basic local cluster: Java producer/consumer, route, heartbeat, pull, offset and lock operations produce nonempty machine-readable evidence.

## 6. Acceptance and handoff

Hand off the verified Java baseline, reproducible offline fixtures, complete source/configuration inventory and Phase 19 live-test manifest. Phase 1 starts after offline oracle checks pass; no working cluster is required.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 1](02-phase-01-common-protocol-models.md).
