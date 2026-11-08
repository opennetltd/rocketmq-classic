# Cleanup regression validation

Plan: [November cleanup](../../plan/2026/11/01-project-cleanup-modernization.md).
Local results and candidate revisions: [execution log](../../rlogs/2026/11/01-project-cleanup-modernization__20261108.rlog).

## Local automated scope

Run locked workspace all-feature tests, strict all-target/all-feature Clippy, formatting,
metadata checks and offline Maven verify as listed in the plan. Rust tests use local
unit/scripted loopback peers and fixtures; Maven uses the pinned local Java oracle.
Test moves preserve names and assertions; source-reading inventories now include the
request/response and Push child modules. The later ASCII-only scope removes the `java_number` helpers entirely; numeric text
now uses standard Rust parsing. Hidden Common `utils::fastjson_number` exports remain
cross-crate implementation helpers, not Java API additions. See the
[ASCII numeric policy](../compatibility/KNOWN_DIFFERENCES.md#ascii-numeric-text-policy--2026-11-08).
Raw-token Fastjson handling remains specialized; its shared numeric primitives were
byte-identical before extraction. Admin first-result/time helpers remain local because
extracting these small functions would add cross-crate API surface for trivial code.

The standard offline Maven gate is blocked by exact address-fixture drift on the
installed Homebrew JDK `27.0.1.0.3` (fixture runtime `27`). Supplemental command
`mvn -o -f compat/java-reference/pom.xml -Druntime.address.fixture.verify.mode=--verify-portable verify`
passed without fixture changes. To close the standard gate, owner must use the
recorded exact JDK/interface environment from the Java reference README and rerun
`mvn -o -f compat/java-reference/pom.xml verify`; expected result is no fixture drift.
The portable mode checks the same Java major and portable behavior while excluding
runtime/vendor/interface-specific scope metadata, so it is recorded separately.

## Owner-executed local Broker checks — not executed in this cleanup session

Prerequisites: disposable Docker Compose resources from
[integration-tests](../../integration-tests/README.md), cached pinned Broker 5.3.4 image,
Java reference toolchain, isolated topics/groups and the existing differential manifest.
Never use production/shared clusters for these checks.

1. Select existing Pull/Push, rebalance/orderly, request/reply and remoting scenarios
   from `tests/differential/scenarios.yaml` using the runner's `--list` output.
2. Follow the integration README to start the required local profile; run selected
   cases with `tests/differential/run.sh --scenario ID` (replace ID with the manifest entry).
3. Verify Pull/Push offsets and generation ownership, orderly handoff, request/reply
   cancellation, class-filter routing where supported, deadline handling and shutdown.
   Expected result: unchanged Java/Rust comparison results, no offset loss or orphan work.
4. Record candidate revision, selected IDs, outcomes and local raw artifact locations;
   clean up only the disposable profile using the documented workflow.

## Platform verification

On a configured Windows cross-build host, run
`cargo check --workspace --all-targets --all-features --target x86_64-pc-windows-gnu --locked --offline`.
Linux ARM64 was checked offline in the existing `rust:1.85.0-bookworm` image,
using `RUSTUP_TOOLCHAIN=1.85.0`, a read-only workspace/registry cache and a separate
target directory with networking disabled. For Linux x86_64 use the existing local
Linux/Docker toolchain and the equivalent workspace
check for `x86_64-unknown-linux-gnu`. Expected result: no compilation/lint regressions
in OS-gated branches; cross-checking does not establish runtime behavior.
See the execution log for which checks actually ran and any toolchain blockers.
Native Windows/Linux runtime validation requiring other hosts is documented only:
prerequisites are the pinned toolchain and local fixture peers, steps are the same
affected crate unit suites on that OS, and expected results are no platform-specific
stack/address/property regressions. No external hosts were accessed.

MAN-01–MAN-04 remain separate owner-specific network/security/HA/workload validation
under [owner validation](../owner-validation.md); this cleanup does not record approval.
