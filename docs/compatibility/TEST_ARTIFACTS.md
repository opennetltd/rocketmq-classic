# Test artifact policy

Build and test outputs are local files and must not be committed. The repository
tracks source, test code, and intentional test inputs or golden fixtures; it does
not track generated build directories, raw test logs, reports, or per-run
evidence.

Differential and Docker integration runners write each run under a unique
directory in the operating system's temporary directory by default. Set
`ROCKETMQ_CLASSIC_EVIDENCE_DIR` to choose another location; it must resolve
outside the checkout. The runners reject paths inside the repository. The full
`integration-tests/run-local.sh --all` run also keeps its copied scenario
manifest and aggregate logs in this external directory.

The checked-in `tests/differential/scenarios.yaml` keeps historical results as
`PASS_RECORDED` entries without raw evidence paths. A live run updates only its
external manifest copy to `PASS` and points that copy at the local output.

Cargo and Maven build outputs use their normal `target/` directories, which are
ignored by Git. `fixtures/` and test TLS files under source-controlled fixture
directories are intentional test inputs and remain tracked. Offline stress
tests may print results to the terminal; build products remain under `target/`.

If a runner creates a new output under the checkout, move that output to the
external artifact directory and add an appropriate ignore rule before merging
the runner change.
