#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_manifest="$repo_root/tests/differential/scenarios.yaml"
manifest=""

usage() {
    cat <<'EOF'
Usage:
  ROCKETMQ_CLASSIC_TEST_ENV=local-disposable bash integration-tests/run-local.sh --all

Runs the server-free workspace gates, every registered Phase 19 live scenario,
and the final local-output reconciliation. Test artifacts are written outside
the repository under ROCKETMQ_CLASSIC_EVIDENCE_DIR or the system temp directory.
The live runner refuses to start Compose if any selected manifest row has no implementation.
EOF
}

if [[ $# -ne 1 || "$1" != "--all" ]]; then
    usage >&2
    exit 2
fi

if [[ "${ROCKETMQ_CLASSIC_TEST_ENV:-}" != "local-disposable" ]]; then
    echo "Refusing local validation: set ROCKETMQ_CLASSIC_TEST_ENV=local-disposable" >&2
    exit 2
fi

if ! command -v cargo >/dev/null || ! command -v mvn >/dev/null || ! command -v python3 >/dev/null; then
    echo "Cargo, Maven, and Python 3 are required for local validation" >&2
    exit 2
fi

cd "$repo_root"
if [[ -z "${ROCKETMQ_CLASSIC_EVIDENCE_DIR:-}" ]]; then
    evidence_root="$(mktemp -d "${TMPDIR:-/tmp}/rocketmq-classic-test-artifacts.XXXXXX")"
    evidence_root="$(cd "$evidence_root" && pwd -P)"
else
    mkdir -p "$ROCKETMQ_CLASSIC_EVIDENCE_DIR"
    evidence_root="$(cd "$ROCKETMQ_CLASSIC_EVIDENCE_DIR" && pwd -P)"
fi
case "$evidence_root/" in
    "$repo_root/"*)
        echo "Test artifacts must be stored outside the repository: $evidence_root" >&2
        exit 2
        ;;
esac
export ROCKETMQ_CLASSIC_EVIDENCE_DIR="$evidence_root"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
started_epoch="$(date +%s)"
manifest="$evidence_root/scenarios-$run_id.json"
cp "$source_manifest" "$manifest"
export ROCKETMQ_CLASSIC_SCENARIOS_MANIFEST="$manifest"
evidence_dir="$evidence_root/run-local/$run_id"
mkdir -p "$evidence_dir"
result="FAIL"
steps_file="$evidence_dir/steps.tsv"
: >"$steps_file"

redact_paths() {
    python3 - "$evidence_dir" "$repo_root" "${HOME:-}" <<'PY'
import sys
from pathlib import Path

directory = Path(sys.argv[1])
replacements = [(sys.argv[2].encode(), b"<repo>"), (sys.argv[3].encode(), b"<home>")]
for path in directory.iterdir():
    if not path.is_file():
        continue
    contents = path.read_bytes()
    for source, replacement in replacements:
        if source:
            contents = contents.replace(source, replacement)
    path.write_bytes(contents)
PY
}

finish() {
    exit_code=$?
    trap - EXIT
    ended_epoch="$(date +%s)"
    if [[ "$result" == "PASS" && $exit_code -eq 0 ]]; then
        printf 'PASS all local gates and all Phase 19 scenario IDs\n' >"$evidence_dir/result.txt"
    elif [[ "$result" == "BLOCKED" ]]; then
        if [[ $exit_code -eq 0 ]]; then
            exit_code=3
        fi
        printf 'BLOCKED full local validation: one or more Phase 19 scenario implementations are missing\n' >"$evidence_dir/result.txt"
    else
        result="FAIL"
        if [[ $exit_code -eq 0 ]]; then
            exit_code=1
        fi
        printf 'FAIL local validation (exit=%s)\n' "$exit_code" >"$evidence_dir/result.txt"
    fi
    printf 'result=%s\nstarted_epoch=%s\nended_epoch=%s\nrevision=%s\n' \
        "$result" "$started_epoch" "$ended_epoch" "$(git rev-parse HEAD)" \
        >"$evidence_dir/metadata.txt"
    redact_paths || exit_code=1
    if [[ "$result" == "PASS" && $exit_code -eq 0 ]]; then
        echo "Local validation passed. Local artifacts: $evidence_dir"
    elif [[ "$result" == "BLOCKED" ]]; then
        echo "Local validation blocked by missing live implementations. Local artifacts: $evidence_dir" >&2
    else
        echo "Local validation failed. Local artifacts: $evidence_dir" >&2
    fi
    exit "$exit_code"
}
trap finish EXIT

run_step() {
    step_name="$1"
    shift
    log_file="$evidence_dir/$step_name.log"
    printf 'Running %s\n' "$step_name"
    if "$@" >"$log_file" 2>&1; then
        printf 'PASS\t%s\n' "$step_name" >>"$steps_file"
        printf 'PASS %s\n' "$step_name"
    else
        step_status=$?
        printf 'FAIL\t%s\t%s\n' "$step_name" "$step_status" >>"$steps_file"
        printf 'FAIL %s (exit=%s)\n' "$step_name" "$step_status" >&2
        tail -n 100 "$log_file" >&2 || true
        return "$step_status"
    fi
}

{
    printf 'ROCKETMQ_CLASSIC_TEST_ENV=local-disposable bash integration-tests/run-local.sh --all\n'
    printf 'ROCKETMQ_CLASSIC_TEST_ENV=local-disposable\n'
    printf 'java_client=5.5.1\nserver=5.3.4\n'
    printf 'cargo fmt --all -- --check\n'
    printf 'cargo test --workspace --locked --offline -- --test-threads=1\n'
    printf 'cargo test --workspace --all-features --locked --offline -- --test-threads=1\n'
    printf 'cargo check --workspace --all-targets --all-features --locked --offline\n'
    printf 'cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings\n'
    printf 'TZ=UTC mvn -o -q -f compat/java-reference/pom.xml verify\n'
    printf 'python3 tests/differential/compare.py --self-test\n'
    printf 'python3 tests/differential/compare.py --validate-manifest\n'
    printf 'bash tests/stress/run-offline-smoke.sh\n'
    printf 'cargo run --locked --offline -p rocketmq-classic-remoting --example fuzz_smoke\n'
    printf 'cargo build -p rocketmq-classic --examples --release --locked --offline\n'
    printf 'cargo bench --workspace --no-run --locked --offline\n'
    printf 'cargo bench -p rocketmq-classic --bench offline --locked --offline\n'
    printf 'cargo metadata --locked --format-version 1 --no-deps\n'
    printf 'python3 scripts/check-workspace-metadata.py\n'
    printf 'cargo package --workspace --list --locked --allow-dirty\n'
    printf 'TZ=UTC cargo test --locked --offline -p rocketmq-classic-core unique_id\n'
    printf 'TZ=Asia/Taipei cargo test --locked --offline -p rocketmq-classic-core unique_id\n'
    printf 'tests/differential/run.sh --check-all\n'
    printf 'Conditional after successful --check-all: tests/differential/run.sh --all\n'
} >"$evidence_dir/commands.txt"
{
    uname -srm
    cargo --version
    rustc --version
    mvn -version
    java -version
    docker version --format 'Docker client {{.Client.Version}} / server {{.Server.Version}}'
    docker compose version
    docker info --format 'Docker platform {{.OSType}}/{{.Architecture}}'
} >"$evidence_dir/toolchain.txt" 2>&1

run_step cargo-format cargo fmt --all -- --check
run_step diff-check git diff --check
run_step workspace-tests-default cargo test --workspace --locked --offline -- --test-threads=1
run_step workspace-tests-all-features cargo test --workspace --all-features --locked --offline -- --test-threads=1
run_step workspace-check cargo check --workspace --all-targets --all-features --locked --offline
run_step workspace-clippy cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
run_step java-verify env TZ=UTC mvn -o -q -f compat/java-reference/pom.xml verify
run_step differential-self-test python3 tests/differential/compare.py --self-test
run_step differential-manifest python3 tests/differential/compare.py --validate-manifest
run_step race-leak-smoke bash tests/stress/run-offline-smoke.sh
run_step fuzz-smoke cargo run --locked --offline -p rocketmq-classic-remoting --example fuzz_smoke
run_step release-examples cargo build -p rocketmq-classic --examples --release --locked --offline
run_step benchmark-build cargo bench --workspace --no-run --locked --offline
run_step benchmark-run cargo bench -p rocketmq-classic --bench offline --locked --offline
run_step cargo-metadata cargo metadata --locked --format-version 1 --no-deps
run_step workspace-metadata python3 scripts/check-workspace-metadata.py
run_step package-content cargo package --workspace --list --locked --allow-dirty
run_step unique-id-utc env TZ=UTC cargo test --locked --offline -p rocketmq-classic-core unique_id
run_step unique-id-taipei env TZ=Asia/Taipei cargo test --locked --offline -p rocketmq-classic-core unique_id

printf 'Running differential-live-coverage\n'
coverage_log="$evidence_dir/differential-live-coverage.log"
if "$repo_root/tests/differential/run.sh" --check-all >"$coverage_log" 2>&1; then
    printf 'PASS\tdifferential-live-coverage\n' >>"$steps_file"
    cat "$coverage_log"
else
    coverage_status=$?
    cat "$coverage_log" >&2
    if [[ $coverage_status -eq 3 ]]; then
        result="BLOCKED"
        printf 'BLOCKED\tdifferential-live-coverage\t%s\n' "$coverage_status" >>"$steps_file"
        exit "$coverage_status"
    fi
    printf 'FAIL\tdifferential-live-coverage\t%s\n' "$coverage_status" >>"$steps_file"
    exit "$coverage_status"
fi

run_step docker-toolchain bash -c 'command -v docker >/dev/null && docker version --format "Docker client {{.Client.Version}} / server {{.Server.Version}}" && docker compose version'
run_step live-scenarios env ROCKETMQ_CLASSIC_TEST_ENV=local-disposable "$repo_root/tests/differential/run.sh" --all
run_step scenario-evidence-reconciliation python3 - "$manifest" "$evidence_root" <<'PY'
import json
import sys
from pathlib import Path

manifest = json.loads(Path(sys.argv[1]).read_text())
root = Path(sys.argv[2])
required = [row for row in manifest["scenarios"] if row.get("execution_phase") == 19]
if len(required) != 37:
    raise SystemExit(f"expected 37 Phase 19 rows, found {len(required)}")
failures = []
for row in required:
    scenario_id = row["id"]
    evidence = row.get("evidence")
    if row.get("live_status") != "PASS" or not evidence:
        failures.append(f"{scenario_id}: live_status={row.get('live_status')} evidence={evidence}")
        continue
    evidence_path = Path(evidence)
    if not evidence_path.is_absolute():
        evidence_path = root / evidence_path
    results = []
    if evidence_path.is_dir():
        direct_result = evidence_path / "result.txt"
        if direct_result.is_file():
            results.append(direct_result)
        results.extend(evidence_path.glob("*/result.txt"))
    if not any(path.read_text(errors="replace").startswith(f"PASS {scenario_id}:") for path in results):
        failures.append(f"{scenario_id}: PASS evidence result is absent from {evidence}")
if failures:
    raise SystemExit("Phase 19 evidence reconciliation failed:\n  " + "\n  ".join(failures))
print(f"Reconciled all {len(required)} Phase 19 PASS rows and evidence results")
PY

result="PASS"
