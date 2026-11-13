# Plan 02a — Close Push Pull Fan-Out Validation Gaps

## 1. Goal and Scope

Follow up on Plan 02 without changing its dispatch architecture or rewriting its historical evidence. Review baseline: `889901e9` (2026-11-13 review). Fix the permanent `P10-V05` probes/checker and test-resource ownership so implementation meets Plan 02 §§4.5–4.7 and acceptance criterion 6.

The owned `JoinSet`, pre-spawn guard, async Pull selection, and separate Pull/POP suspend constants are present. The scripted-peer fan-out test passes locally. Live execution remains owner-executed and `PENDING_PHASE_19`; its absence alone is not a deviation. No production dispatch change is currently required by these findings.

## 2. Confirmed Findings

| Priority | Evidence | Deviation / effect |
| --- | --- | --- |
| P1 | `compat/java-reference/src/main/java/compat/LivePullFanoutProbe.java:83,110` | `routeQueues` calls `producer.fetchPublishMessageQueues` before `producer.start`. The locally cached pinned 5.5.1 sources show `DefaultMQProducerImpl.fetchPublishMessageQueues` calls `makeSureStateOK`, which rejects non-RUNNING state. The live Java probe cannot reach its gate; compilation and argument smoke tests miss this failure. |
| P2 | `tests/differential/check_p10_fanout.py::check_language` | The GATE timestamp is not validated against sends; SEND_END records are collected but not required or checked for uniqueness and matching queue/time. Review-generated fixtures with all SEND_END rows removed, and fixtures publishing before GATE, both incorrectly pass. This cannot enforce the planned empty-topic-before-publish contract or complete successful-send evidence. |
| P2 | Both probe recorders (`FanoutProbeState`, Java `Recorder`) | Gate state counts historical starts, not active queue requests; it does not reject duplicate active starts. Response violations are inferred only on a later start, rather than at the response, and gate reads occur separately. Both listeners suppress repeat message-key deliveries before emitting evidence, hiding duplicates from a checker that claims to detect them. Startup timeout is measured after route setup rather than separating startup from the first-Pull admission window. |
| P2 | `crates/rocketmq-classic-consumer/tests/push_pull_fanout.rs::broker_peer` and final teardown; `tests/differential/run.sh` subgroup creation | Broker reader/writer/handler tasks are detached; teardown aborts only accept tasks and silently tolerates a join timeout. It does not prove all peer tasks finish as Plan 02 requires. The runner marks both groups created only after the whole creation loop succeeds, so failure creating the second group omits explicit cleanup of the first. |

Review checks: the current checker self-test passes 11 synthetic cases; `run.sh --check-all` passes with 38 rows; `cargo test -p rocketmq-classic-consumer --test push_pull_fanout --locked` passes (1 test). These successes do not close the findings above. The checker counterexamples were created in a temporary directory; no production code or existing fixtures were modified during review.

## 3. Implementation Plan

### 3.1 Repair Java probe acquisition and cleanup

- Start the consumer first as intended, then start the producer before route lookup. Place both acquisitions under explicit cleanup ownership so producer-start, route, gate, send, and delivery failures still close every successfully started client. Remove the later duplicate producer start.
- Apply equivalent cleanup ownership in Rust: `producer.start().await?` currently occurs after consumer startup but before the operation/cleanup block; an error must still shut down the consumer. Do not change public APIs or transport policy.
- Prove the Java acquisition order with a focused regression using the pinned API or a small internal lifecycle seam, plus the eventual live smoke run. Argument validation and compilation alone are insufficient.

### 3.2 Make probe state and evidence faithful

- In both recorders maintain an active map by target queue and request identity. Reject a start while that queue is active; match each response to its active request before removing it. Filter exact topic/group as today.
- Evaluate and latch the gate under the same state lock: exactly 16 expected queues active, no target response yet, and elapsed time from the first target start at most 5 s. A response before gate latching records a violation immediately. Once latched, normal responses and follow-ups are allowed; do not flag every later start merely because a response has occurred.
- Give startup a separate bounded wait and enforce the admission window from the first target start. Use a consistent monotonic timestamp origin per process and make gate snapshots atomic; never compare process clocks.
- Emit every target delivery before deduplicating keys for progress tracking. Retain deduplication only for deciding when all distinct messages arrived, so duplicate evidence remains visible to the checker. Record the first callback timestamp at callback entry as today.
- Add focused recorder tests for response-before-gate without a subsequent start, duplicate active start, valid follow-up after a latched gate, and repeated delivery logging. Keep the ready file dependent on a valid latched gate.

### 3.3 Enforce complete evidence in the checker

- Parse and validate GATE timestamp, count, and span against recorded Pull starts. Require the gate at/after the final initial start and before any target response; require every SEND_START at/after that gate. Validate ordering by timestamps, not file-print interleaving.
- Require exactly one SEND_START and successful SEND_END for each seeded key/queue, matching queue/key, nonnegative queue offset, and send-end time at/after send-start. Reject missing, duplicate, unknown, or mismatched SEND_END records. Delivery may precede SEND_END, as Plan 02 allows, but cannot precede SEND_START or occur after shutdown.
- Require complete matching assignment/send/delivery sets and preserve one legitimate final outstanding long-poll per queue at shutdown. Reject duplicate delivery rows now that probes expose them. Keep per-process timing and existing 5 s bounds.
- Extend `--self-test` with both reproduced counterexamples, gate before the final initial start, response between the final initial start and gate, duplicate/mismatched SEND_END, duplicate delivery, and valid callback-before-send-return. Each negative fixture must fail for the intended reason, and the valid fixture must pass.

### 3.4 Own peer resources and partial setup

- In the scripted peer, retain all accepted connection reader/writer/handler tasks in an owned collection. On teardown close acceptance, signal connection cancellation, and join every child; abort/drain stragglers within a bounded cleanup strategy. Never silently accept a timeout. Make the test retain a cleanup witness demonstrating no connection task survives; preserve both held-response rounds.
- In `tests/differential/run.sh`, record each successfully created fan-out group immediately in an array and clean only that recorded set on success/failure. Add each group before attempting the next. Retain existing disposable-environment guards and topic cleanup.
- Verify subgroup partial failure using a stubbed command runner: first creation succeeds, second fails, and the first is deleted. Verify peer cleanup during normal shutdown and an injected test failure. Do not require Docker for these focused checks.

### 3.5 Evidence and final owner execution

Append a new `rlogs/2026/11/02a-push-pull-fanout-validation-fixes__<UTC>.rlog`; update ADR-0085 / compatibility evidence only where the corrected validation warrants it. Preserve Plan 02 and earlier logs. Do not relabel the previously recorded 37-case live aggregate as covering P10-V05.

After local checks, the owner executes P10-V05 against the disposable host-routed Broker and archives both probe logs and checker results. Keep live status pending until a real run records its result. The full aggregate remains the final Phase 19 obligation, not a prerequisite for authoring this correction plan.

## 4. Verification

```sh
python3 tests/differential/check_p10_fanout.py --self-test
bash -n tests/differential/run.sh
bash tests/differential/run.sh --check-all
cargo test -p rocketmq-classic-consumer --test push_pull_fanout --locked
cargo test -p rocketmq-classic-consumer push::pull_service::tests --locked
cargo test -p rocketmq-classic-integration --bin live_client --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
mvn -f compat/java-reference/pom.xml verify
git diff --check
```

Run the new lifecycle, recorder, and stubbed-cleanup regressions as part of their owning test targets. Record exact Maven environment limitations separately; the previous execution log reports an exact-JDK fixture mismatch and a passing supplemental portable gate, not a passing standard verify.

Final owner-executed commands:

```sh
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=host-routed bash tests/differential/run.sh --scenario P10-V05
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable bash integration-tests/run-local.sh --all
```

## 5. Acceptance Criteria

1. The Java probe reaches route lookup with a RUNNING producer; both probes clean up clients after every partial acquisition failure.
2. The gate proves simultaneous empty-queue Pulls before publishing, with atomic state and a first-Pull-based admission window. Duplicate active requests and duplicate deliveries remain observable failures.
3. The checker rejects pre-gate publishing and missing/invalid send completions; valid callback-before-send-return and final outstanding long-polls still pass.
4. Every peer task is joined or aborted/drained with an observable completion witness; partial subgroup creation cleans up the groups already created.
5. Local checks and bilingual documents agree; live results remain pending until owner execution supplies evidence. Production dispatch behavior and Plan 02 ownership decisions remain unchanged.
