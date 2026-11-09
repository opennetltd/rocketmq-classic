# Release preparation checklist

The Java client baseline is RocketMQ 5.5.1; the local Broker baseline is 5.3.4.
This checklist does not authorize publication, a release tag, deployment or a compatibility claim.

[Current compatibility status](compatibility/CURRENT_STATUS.md) is the source for dated validation results and their revision limits. Historical PASS records do not certify later source changes.

- [ ] Record the candidate source revision and local formatting, tests, strict Clippy, metadata and Maven oracle results.
- [ ] Complete affected local Broker scenarios and record the exact source revision and artifact locations under the [artifact policy](compatibility/TEST_ARTIFACTS.md).
- [ ] Complete the [cleanup regression handoff](testing/cleanup-validation.md), including platform checks and owner-run cases.
- [ ] Resolve MAN-01–MAN-04 with the inputs and acceptance criteria in [owner validation](owner-validation.md).
- [ ] Obtain final compatibility/release approval after the above evidence is reviewed.

The complete earlier checklist is retained verbatim in [the dated archive](release-checklist-history-20261027.md). Use the [historical heading link map](release-checklist-history-links.md) for former fragment references. No earlier result has been promoted to a current result.
