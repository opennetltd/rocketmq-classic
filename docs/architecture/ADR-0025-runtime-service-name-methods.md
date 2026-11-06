# ADR-0025: Map Java runtime service-name methods

Status: implemented in `4ebd683`; whole-phase source and API reconciliation
remains open.

## Source contract

Pinned Java 5.5.1 `RebalanceService` and `PullMessageService` both publicly
declare `getServiceName()`. Their bytecode returns
`getClass().getSimpleName()`, producing `RebalanceService` and
`PullMessageService`, respectively.

## Rust mapping

`runtime::REBALANCE_SERVICE_NAME` and `runtime::PULL_MESSAGE_SERVICE_NAME`
expose the two stable results. This is a narrow value mapping; it does not
claim that the Rust runtime exposes Java worker objects or Java thread APIs.

## Evidence and limits

The Java `serviceNameCases` fixture constructs both services with null
collaborators and calls each public method without starting a worker. The
native integration test compares both values. The `PullMessageService`
constructor creates a scheduled executor, but the fixture submits no task, so
its thread factory is not invoked.

The `fixtures/runtime/client-config.json` SHA-256 at this update was
`57114f6198944de08fa6801207525abbbfd9e1532efe93fa02d2c8c64ab98fa4`.
Workspace tests, strict Clippy, pinned offline Maven verification, comparator
self-test, 22-scenario manifest validation, formatter, and diff checks pass.
Worker dispatch, scheduling, and shutdown behavior are covered by separate
runtime mappings; remaining source/API/input reconciliation and P6-V04 Phase
19 evidence are still open.
