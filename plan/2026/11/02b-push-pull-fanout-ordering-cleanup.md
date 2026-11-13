# Plan 02b — Align Fan-Out Event Ordering and Failure Cleanup

## 1. Goal and Scope

Review baseline: `142881db`. Complete the remaining Plan 02a §§3.2–3.4 requirements in the P10-V05 recorders, checker, and scripted-peer test. Preserve the production Pull dispatcher, pinned versions, public settings, Plan 02/02a, and historical logs. No new diagram is needed for these local corrections.

The Java producer now starts before route lookup, successful subgroup creations are recorded immediately, both listeners emit duplicates, and the earlier pre-gate-send/missing-SEND_END checker defects are fixed. Live P10-V05 remains `PENDING_PHASE_19`, as permitted by the plans; no live execution was performed in this review.

## 2. Remaining Findings

### 2.1 P2 — Recorder timestamps can contradict the atomic gate

`integration-tests/src/bin/live_client.rs:839–861` samples `now` before taking the recorder lock, then uses that sample both to update the start inventory and to latch GATE. Concurrent Rust Pull tasks can acquire the lock in a different order from their timestamp samples. For example, one task samples 100 and waits; another samples 200 and records its start first; the delayed task then completes the active set and latches GATE at 100. The checker correctly rejects that gate because a covered start is at 200. This is a source-confirmed possible interleaving, not a claim of a reproduced live failure.

`earliest_start` also records the first lock winner rather than necessarily the earliest sampled start. Response timestamps and `try_latch_gate(now)` use the same outside-lock timestamp pattern. Java `LivePullFanoutProbe.Recorder` similarly samples outside its synchronized section. Plan 02a requires atomic gate state and consistent monotonic evidence; locking the map alone does not establish timestamp order.

### 2.2 P2 — The checker accepts a gate outside the admission window

`tests/differential/check_p10_fanout.py:179–193` validates the first-start span and the lower bound on GATE, but not elapsed time from the first start to GATE. A review-generated fixture with all starts near 1 s, GATE at 11 s, responses shifted later, and valid sends/deliveries after GATE is incorrectly accepted under the 5 s admission window. The probes attempt to enforce this limit themselves; the checker does not independently enforce the evidence contract promised by Plan 02a.

### 2.3 P2 — Peer failure cleanup still drops unfinished handles

`crates/rocketmq-classic-consumer/tests/push_pull_fanout.rs:93–98` moves a `JoinHandle` into `timeout`. On expiry it drops the unfinished handle and panics, detaching that task. `join_recorded_tasks` then abandons the remaining vector on its first panic. The main test only reaches explicit cleanup after all setup and fan-out assertions succeed; the abandoned-connection test requests normal cooperative cleanup and does not exercise an assertion failure before teardown. Thus the Plan 02a requirement to clean every child after an injected test failure is not yet demonstrated. The task witness is also constructed inside the spawned body, leaving its increment unreleased if the task is aborted before its first poll.

## 3. Implementation Plan

### 3.1 Serialize recorder transitions and their timestamps

- Define Pull event times as recorder state-transition times. Acquire the state lock, sample the monotonic clock, then update active/start/response state and latch any gate in that same critical section. Return the timestamp and immutable rows/snapshot for emission after unlocking. Do not call the output sink while holding the state lock.
- Apply the policy to Rust `pull_start`, `pull_end`, and the production gate-check path, and Java `Recorder.pullStart`, `pullEnd`, and `tryLatchGate`. Ensure a gate timestamp is at/after every start it covers and that the recorded first start is the admission-window origin. Preserve callback-entry delivery timestamps and per-process clocks.
- Keep deterministic clock injection private to tests. Add a coordinated concurrency test in which one invocation is delayed before acquiring the lock while another records first; verify the resulting evidence passes the checker and cannot contain an earlier GATE than a covered start. Cover a response racing the last initial start: it either invalidates the pre-gate barrier or follows a valid latch, consistently in state and evidence.
- Preserve duplicate active-start detection, legal follow-ups after a gate, complete delivery logging, and the separate startup bound. Do not relax the checker to accept contradictory event times.

### 3.2 Check the gate's elapsed time directly

- In `check_language`, require `gate_at - min(first_starts.values())` to be nonnegative and at most `admission_window_ms * 1_000_000`, in addition to existing start-span, response-order, and send-order checks. Compare nanoseconds before rounding values for reports.
- Extend `--self-test` with the reproduced delayed-gate fixture, an exactly-at-limit valid gate, and a gate just beyond the limit. Keep responses and sends after the gate in these cases so the assertion tests the elapsed gate limit rather than a different ordering failure.
- Preserve callback-before-send-return and legitimate final outstanding Pulls. Continue rejecting the Plan 02a counterexamples; do not change the 5 s contract or cross-process clock policy.

### 3.3 Drain peer ownership before reporting failure

- Keep each handle owned while waiting, using a mutable handle reference for timeouts. On timeout retain the failure reason, abort that handle, and await its completion. Drain every other recorded task even if one times out or panics; report accumulated errors only after all handles have been observed and the live witness has reached zero.
- Construct `LiveWitness` before spawn and move it into the future, so abort-before-first-poll releases the already-incremented counter. Add a direct early-abort regression.
- Restructure the fan-out test so setup/scenario failure cannot bypass cleanup. Keep peer ownership outside the fallible scenario body, capture its result/panic, signal cancellation, join accept tasks, drain all child tasks, and only then propagate the scenario failure. Retain a Drop-based abort fallback for unexpected early unwinding, with async draining on the normal failure path.
- Add an injected assertion-failure test while a connection is held open, and a forced cleanup-timeout test with more than one child. Both must prove every handle is drained and the witness is zero before the original failure is reported. Preserve the existing two held-response rounds and abandoned-connection regression.

### 3.4 Evidence and final owner validation

Append `rlogs/2026/11/02b-push-pull-fanout-ordering-cleanup__<UTC>.rlog` with the checker counterexample, concurrency tests, failure-cleanup tests, and actual command results. Update ADR-0085 / compatibility evidence only for the corrected guarantees. Leave earlier plans and rlogs intact.

The final owner-executed P10-V05 run must archive both probe logs and checker verdict using the existing evidence mechanism. Keep `PENDING_PHASE_19` until real Broker execution records a result; earlier 37-case aggregate results do not cover this case.

## 4. Verification

Review actually ran: checker self-test (20 cases), runner self-tests, runner `--check-all` (38 rows), scripted-peer integration tests (2 passed), and Rust live-client tests (7 passed). All passed; the new delayed-gate counterexample still passed incorrectly. No full workspace or Maven gate was rerun during review.

Implementation requirements:

```sh
python3 tests/differential/check_p10_fanout.py --self-test
bash tests/differential/run.sh --self-test
bash tests/differential/run.sh --check-all
cargo test -p rocketmq-classic-consumer --test push_pull_fanout --locked
cargo test -p rocketmq-classic-integration --bin live_client --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
mvn -f compat/java-reference/pom.xml verify
git diff --check
```

Include the new Java recorder regression in `LivePullFanoutProbeFixtures` and its existing Maven execution. Record exact-JDK fixture limitations separately from supplemental portable verification, as in Plan 02a.

Final owner-executed commands:

```sh
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=host-routed bash tests/differential/run.sh --scenario P10-V05
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable bash integration-tests/run-local.sh --all
```

## 5. Acceptance Criteria

1. Concurrent recorder invocations cannot produce a GATE earlier than a covered start; response/gate ordering agrees between state and emitted evidence.
2. The checker rejects a gate reached after the admission window, including the reproduced 10 s case, without weakening prior checks.
3. Assertion failure, child panic, cleanup timeout, and abort-before-first-poll leave no unobserved peer handle or live witness; failures are reported after cleanup.
4. Existing local regressions still pass, English/Traditional Chinese plans agree, and live status remains pending until owner execution supplies evidence. Production dispatch and public behavior are unchanged.
