# Current compatibility status

## ASCII numeric scope follow-up — 2026-11-08

The user's revised scope intentionally replaces Java Unicode integer parsing with
Rust standard ASCII integer parsing. It covers numeric properties, permissions,
headers, route counts/ports, remoting settings and consume timestamps; Unicode
message/topic text remains supported. See the [numeric policy](KNOWN_DIFFERENCES.md#ascii-numeric-text-policy--2026-11-08)
and [follow-up execution log](../../rlogs/2026/11/01-project-cleanup-modernization__20261108TASCII.rlog)
for regression results and the distinction from historical Java Unicode parity claims.
The final follow-up tree passed 2,074 tests across 94 workspace test/doc suites,
strict all-target/all-feature Clippy, formatting and metadata checks. Windows GNU
and Linux ARM64 offline Docker compilation checks passed. Supplemental portable
Maven verification passed; the standard exact-JDK environment limitation described
below remains unchanged. Broker live/owner validation was not repeated.
The original cleanup plan remains unchanged; this later scope supersedes its
Unicode numeric compatibility constraint. Release/owner approval remains open.

## Cleanup execution — 2026-11-08

The November cleanup preserves the pinned Java client, Rust MSRV and Broker baseline.
It scopes test helpers, consolidates numeric primitives and reorganizes modules.
The final cleanup source commit is `785a0294` on `feat/remove_legacy_code`.
On `781ec756`, locked offline workspace all-feature tests and doctests passed
(2,075 tests); strict all-target/all-feature Clippy passed again after the final
visibility-only narrowing. Windows GNU cross-checks and Linux ARM64 offline Docker
workspace checks passed before that narrowing. Formatting and metadata checks passed.
The standard offline Maven gate encountered exact address-fixture drift with
Homebrew JDK `27.0.1.0.3` versus the fixture runtime `27`; fixtures remain unchanged.
Supplemental offline Maven verification with the existing
`-Druntime.address.fixture.verify.mode=--verify-portable` mode passed, including
106 Rust-generated header fixtures and 19 typed body fixtures. This does not
turn the standard exact-environment Maven gate into a pass.
The [execution log](../../rlogs/2026/11/01-project-cleanup-modernization__20261108.rlog)
records phase commits and final local validation. The
[regression handoff](../testing/cleanup-validation.md) records owner-run and platform
verification separately. These changes do not extend the dated live evidence below;
MAN-01–MAN-04 and final compatibility/release approval remain open.

## Parity-remediation evidence — 2026-10-27

The Java oracle is Apache RocketMQ client 5.5.1, source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`. The latest parity-remediation
source revision is `7e0e9955` on branch `fix/parity-review-2026-10-27`.

Review findings R01–R09 are addressed and locally verified on the remediation
branch: complete message-batch decoding and delivery; POP orderly control flow;
listener-task cancellation ownership; shared POP shutdown wait budgets; Lite
initial-offset selection; live Pull/Lite settings; Lite retry-delay and
FLOW_CONTROL handling; nullable Pull/Lite consumer groups; and accurate TLS
hostname guidance. The locked workspace test suite passed, including 287
Consumer unit tests, every Consumer integration suite, and all workspace
doctests. Strict all-target/all-feature Clippy, offline Maven `verify` (37
scenario manifest plus the pinned Java fixture suite), workspace metadata,
formatting, and diff checks also passed. Paused-time tests cover Lite exception
delays of 4,500 ms and 1,000 ms, FLOW_CONTROL at 20 ms, cancellation, and
immediate acceptance of a successful batch. The external facade test exercises
the new live and nullable APIs.

The latest full local aggregate is historical, not a result for this source
revision: on `413f735ad0ab41717acb9d76e2cb664b42076f4d` (2026-10-26), the
recorded local aggregate passed 23/23 gates and the Phase 19 scenario manifest
recorded 37/37 live scenarios as passing. The full aggregate was not rerun
after the current code changes. Three affected live scenarios were rerun
against the pinned 5.3.4 image on current revision `7e0e9955`, from clean source:
`P9-V03` passed all four Pull statuses, suggested Broker, boundary RPCs, and
resume offset (115 seconds); `P5-V02-consumer` passed Pull/Lite Pull, offsets,
consumer list, send-back, and lock RPCs (76 seconds); `P13-V02` passed POP
delivery, expiry redelivery, ACK, visibility update, replacement receipt,
mixed queue mapping, and malformed receipt checks (118 seconds). Their complete
raw output is retained locally at
`plan/2026/10/02-project-parity-review-evidence/live/` and is excluded from
Git. Historical aggregate output was removed under the repository artifact
policy. GitHub Actions was removed as a validation requirement on 2026-10-27;
no current-revision remote CI result is claimed.

The dated [MAN-05 current-tree audit](evidence/owner/MAN-05/current-tree-audit-20261026.md)
describes its reviewed revision, `413f735a`, and remains historical. This
status records the later consumer-source delta at `7e0e9955`; the current live
replays are limited to the three scenarios listed above, not the entire
workspace live Broker matrix.
MAN-01 through MAN-04 still require owner network, security, HA, and workload
inputs, so final compatibility and release approval remain open.

The detailed remediation run notes are in
[`rlogs/2026/10/02-project-parity-review__20261027T062249Z.rlog`](../../rlogs/2026/10/02-project-parity-review__20261027T062249Z.rlog).
Raw review probes and the three current live-scenario result sets remain local
under `plan/2026/10/02-project-parity-review-evidence/` and are excluded from
Git as requested. The [release checklist](../release-checklist.md) and dated
Phase 9 and Phase 13 matrices preserve the earlier scenario history.
