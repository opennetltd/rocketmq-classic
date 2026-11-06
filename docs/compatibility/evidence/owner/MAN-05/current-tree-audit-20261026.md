# MAN-05 current-tree source/API delta audit

Date: 2026-10-26

Disposition: N/A for additional feature-specific owner validation. This does
not approve production use or close MAN-01 through MAN-04.

## Scope

- Java oracle: Apache RocketMQ client 5.5.1, source commit
  `efa1be59d6fc747ad220f036c86030f05f0419f9`.
- Broad API/settings/input/error/flow/reverse-export audit:
  [PHASE_6_REQUIREMENT_AUDIT.md](../../../PHASE_6_REQUIREMENT_AUDIT.md), which
  inventories 132 `MQClientAPIImpl` declarations, seven overload groups, 29
  direct RPC invocation cases, facade exports, configuration, and behavior.
- Prior focused delta audit:
  [2026-10-25 MAN-05 audit](current-tree-audit-20261025.md).
- Code revision reviewed here:
  `413f735ad0ab41717acb9d76e2cb664b42076f4d`.

## Delta since the previous focused audit

The intervening Rust source change is confined to
`crates/rocketmq-classic-consumer/src/stack.rs`, the implementation behind the
existing consumer running-info stack capture behavior. Linux now chooses an
unblocked default-ignore signal for each target thread, falls back through
`SIGWINCH`, `SIGURG`, `SIGCHLD`, and `SIGIO`, and restores the prior handler
around each capture. A signal mask on an unrelated thread no longer prevents a
target thread's capture. The Windows path retains thread names during
enumeration and bounds each thread capture within the overall snapshot
interval. These changes improve reliability and bound the existing stack
report; they add no public method, configuration setting, or client feature.

The other Java changes in this range are compatibility-harness code: a bounded
producer stress probe and normalization of wall-clock-derived sampled TPS in a
fixture. They do not change Rust client behavior or expand the public surface.

The historical current-revision local run passed both workspace test suites,
strict Clippy, Java/Maven verification, race/leak and fuzz smoke, release
examples, benchmark build/run, metadata/package checks, timezone-specific
coverage, Docker toolchain validation, and all live scenarios. Its summary
records 23/23 aggregate gates and all 37 clean-source scenarios passing. The
manifest retains `PASS_RECORDED` outcomes; raw run outputs are not retained.
Targeted stack tests are included in the workspace suites. The existing
Windows stack-runtime CI result (raw output not retained)
passed on the preceding workflow revision, which contains the same stack
implementation.

A current-revision remote workflow was requested as run `40383732405`, but
GitHub started none of its three jobs and ran zero steps because account
payments failed or the Actions spending limit must be increased. This is a
historical run result, not a source/API result; its raw details are not
retained.

## Validation disposition updated — 2026-10-27

Per the user's direction, `.github/workflows/ci.yml` has been removed and
GitHub Actions CI is no longer a validation requirement. The completed local
Phase 19 aggregate above is the accepted validation record for this tree.
This workflow change does not alter the source/API audit or resolve the
separate MAN-01 through MAN-04 owner inputs.

No additional source-backed feature-specific case needs an owner-only service.
MAN-01 through MAN-04 remain blocked on production network, organization
security, HA topology, and workload/SLO inputs. Their absence still blocks
final compatibility/release approval.
