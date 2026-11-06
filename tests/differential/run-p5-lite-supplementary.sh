#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="$repo_root/integration-tests/compose-lite-5.5.0.yaml"
image_ref="apache/rocketmq:5.5.0@sha256:7e8f6c9dbd9df742ed26ba69c00d4ad69e2f86a56f3ca7782ff8144dd0798132"
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
run_slug="$(printf '%s' "$run_id" | tr '[:upper:]' '[:lower:]')"
project_name="rocketmq-classic-p19-lite-supplementary-$run_slug"
topic="compat_p5_lite_supplementary"
group="compat_p5_lite_supplementary"
lite_topic='%LMQ%$'"$topic"'$entries'
repo_commit="$(git -C "$repo_root" rev-parse HEAD)"
evidence_dir="$evidence_root/supplementary-lite-5.5.0/$run_id"
namesrv_port="${ROCKETMQ_SUPPLEMENTARY_NAMESRV_PORT:-29876}"
broker_port="${ROCKETMQ_SUPPLEMENTARY_BROKER_PORT:-29911}"

if [[ "${ROCKETMQ_CLASSIC_TEST_ENV:-}" != "local-disposable" ]]; then
    echo "Refusing to provision: set ROCKETMQ_CLASSIC_TEST_ENV=local-disposable" >&2
    exit 2
fi
if ! [[ "$project_name" =~ ^rocketmq-classic-p19-[a-z0-9-]+$ ]]; then
    echo "Invalid isolated Compose project name" >&2
    exit 2
fi
for command_name in docker mvn cargo python3; do
    command -v "$command_name" >/dev/null || { echo "Required command unavailable: $command_name" >&2; exit 2; }
done
for port in "$namesrv_port" "$broker_port"; do
    if ! [[ "$port" =~ ^[0-9]+$ ]] || ((port < 1024 || port > 65535)); then
        echo "Supplementary host ports must be between 1024 and 65535" >&2
        exit 2
    fi
    if nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then
        echo "Supplementary host port $port is already occupied" >&2
        exit 2
    fi
done

export COMPOSE_PROJECT_NAME="$project_name"
export ROCKETMQ_SUPPLEMENTARY_NAMESRV_PORT="$namesrv_port"
export ROCKETMQ_SUPPLEMENTARY_BROKER_PORT="$broker_port"
compose=(docker compose --project-name "$project_name" --file "$compose_file")
if [[ -n "$("${compose[@]}" ps --all --quiet 2>/dev/null)" ]]; then
    echo "Refusing to reuse nonempty Compose project $project_name" >&2
    exit 2
fi
mkdir -p "$evidence_dir"
status=0
started=false
topic_created=false
broker_addr="127.0.0.1:$broker_port"

cat >"$evidence_dir/commands.txt" <<COMMANDS
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_SUPPLEMENTARY_NAMESRV_PORT=$namesrv_port ROCKETMQ_SUPPLEMENTARY_BROKER_PORT=$broker_port COMPOSE_PROJECT_NAME=$project_name docker compose --project-name $project_name --file integration-tests/compose-lite-5.5.0.yaml up --detach --wait --wait-timeout 180
mqadmin clusterList -n namesrv:9876
mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 1 -w 1 -a +message.type=LITE -n namesrv:9876
mqadmin updateSubGroup --attributes +lite.bind.topic=$topic for Java/Rust Lite groups
Java 5.5.1 live-p5-extensions-probe prepare; Rust live_client p5-extension-prepare
Java send-lite parent-topic entries p5-extension-lite after subscriptions register
Java live-p5-extensions-probe lite-pop; Rust live_client p5-extension-lite-pop
mqadmin deleteTopic -c RocketMQLocalTest -t $lite_topic -n namesrv:9876
mqadmin deleteTopic -c RocketMQLocalTest -t $topic -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose-lite-5.5.0.yaml down --volumes --remove-orphans
COMMANDS

source_code_dirty=true
if [[ -z "$(git -C "$repo_root" status --porcelain -- \
    compat/java-reference/pom.xml \
    compat/java-reference/src/main/java/compat/LiveP5ExtensionsProbe.java \
    compat/java-reference/src/main/java/compat/LiveProducerProbe.java \
    integration-tests/src/bin/live_client.rs \
    integration-tests/compose-lite-5.5.0.yaml \
    integration-tests/config/broker-lite-5.5.0.conf \
    tests/differential/run-p5-lite-supplementary.sh)" ]]; then
    source_code_dirty=false
fi
{
    printf 'scenario=P5-V02-extensions-lite-success\nprofile=supplementary-lite-5.5.0\nproject=%s\nrun_id=%s\n' "$project_name" "$run_id"
    printf 'java_client=5.5.1\nserver=5.5.0\nserver_image=%s\nsource_revision=%s\nsource_code_dirty=%s\n' \
        "$image_ref" "$repo_commit" "$source_code_dirty"
    printf 'broker_config=integration-tests/config/broker-lite-5.5.0.conf\nbroker_config_enableLmq=true\nbroker_config_enableMultiDispatch=true\n'
    printf 'topic=%s\nlite_topic=%s\nconsumer_group_prefix=%s\nmethods=heartbeat,heartbeat-v2,assignment,request-mode,lite-subscription,pop-lite,unregister\n' \
        "$topic" "$lite_topic" "$group"
} >"$evidence_dir/metadata.txt"
if ! cargo build --locked --offline -p rocketmq-classic-integration --bin live_client \
    >"$evidence_dir/rust-build.log" 2>&1; then
    cat "$evidence_dir/rust-build.log" >&2
    exit 1
fi
if ! mvn -q -o -f "$repo_root/compat/java-reference/pom.xml" -DskipTests compile \
    >"$evidence_dir/java-compile.log" 2>&1; then
    cat "$evidence_dir/java-compile.log" >&2
    exit 1
fi
{
    uname -srm
    cargo --version
    rustc --version
    mvn -version
    java -version
    docker version --format 'Docker client {{.Client.Version}} / server {{.Server.Version}}'
    docker compose version
    docker info --format 'Docker platform {{.OSType}}/{{.Architecture}}'
    docker image inspect "$image_ref" --format '{{.Id}} {{json .RepoDigests}} {{.Os}}/{{.Architecture}}'
    python3 - "$compose_file" "$repo_root/integration-tests/config/broker-lite-5.5.0.conf" <<'PYHASH'
import hashlib, sys
from pathlib import Path
for name in sys.argv[1:]:
    path = Path(name)
    print(f"sha256 {hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}")
PYHASH
} >"$evidence_dir/toolchain.txt" 2>&1

cleanup() {
    local exit_status=$?
    local cleanup_status=0
    if ((exit_status != 0)); then
        status=1
    fi
    set +e
    if [[ "$started" == true ]]; then
        "${compose[@]}" logs --no-color --timestamps >"$evidence_dir/broker-logs.log" 2>&1
        if [[ "$topic_created" == true ]]; then
            : >"$evidence_dir/topic-cleanup.log"
            for topic_to_delete in "$lite_topic" "$topic"; do
                "${compose[@]}" exec --no-TTY broker ./mqadmin deleteTopic -c RocketMQLocalTest \
                    -t "$topic_to_delete" -n namesrv:9876 >>"$evidence_dir/topic-cleanup.log" 2>&1
                (( $? == 0 )) || cleanup_status=1
            done
        fi
        "${compose[@]}" down --volumes --remove-orphans >"$evidence_dir/compose-cleanup.log" 2>&1
        (( $? == 0 )) || cleanup_status=1
        if [[ -n "$("${compose[@]}" ps --all --quiet 2>/dev/null)" ]]; then
            echo "Compose containers remain after teardown" >>"$evidence_dir/compose-cleanup.log"
            cleanup_status=1
        fi
    fi
    if ((cleanup_status)); then
        status=1
    fi
    local ended_epoch
    ended_epoch="$(date +%s)"
    printf 'exit_status=%s\nduration_seconds=%s\n' "$status" "$((ended_epoch - started_epoch))" \
        >>"$evidence_dir/metadata.txt"
    if ((status == 0)); then
        printf 'PASS P5-V02 supplementary Lite success: Java and Rust registered and consumed Lite messages on unmodified RocketMQ 5.5.0\n' \
            >"$evidence_dir/result.txt"
    else
        printf 'FAIL P5-V02 supplementary Lite success: inspect archived evidence\n' >"$evidence_dir/result.txt"
    fi
    python3 - "$evidence_dir" "$repo_root" "${HOME:-}" <<'PY'
import sys
from pathlib import Path
for path in Path(sys.argv[1]).iterdir():
    if not path.is_file():
        continue
    content = path.read_bytes()
    for source, replacement in ((sys.argv[2].encode(), b"<repo>"), (sys.argv[3].encode(), b"<home>")):
        if source:
            content = content.replace(source, replacement)
    path.write_bytes(content)
PY
    trap - EXIT
    exit "$status"
}
started_epoch="$(date +%s)"
trap cleanup EXIT

if ! "${compose[@]}" config --quiet >"$evidence_dir/compose-config.log" 2>&1; then
    cat "$evidence_dir/compose-config.log" >&2
    status=1
    exit 1
fi
if ! "${compose[@]}" up --detach --wait --wait-timeout 180 >"$evidence_dir/setup.log" 2>&1; then
    cat "$evidence_dir/setup.log" >&2
    started=true
    status=1
    exit 1
fi
started=true
registered=false
: >"$evidence_dir/server-version.log"
for attempt in {1..90}; do
    if output="$("${compose[@]}" exec --no-TTY broker ./mqadmin clusterList -n namesrv:9876 2>&1)" \
        && [[ "$output" == *"broker-a"* ]]; then
        printf '%s\n' "$output" >"$evidence_dir/server-version.log"
        registered=true
        break
    fi
    printf '%s\n' "$output" >"$evidence_dir/server-version.log"
    sleep 1
done
cat "$evidence_dir/server-version.log"
if [[ "$registered" != true ]] || ! rg -q 'V5_5_0' "$evidence_dir/server-version.log"; then
    echo "Broker 5.5.0 did not register before the bounded readiness deadline" >&2
    status=1
    exit 1
fi

echo "Creating a LITE parent topic and isolated client groups"
topic_created=true
if ! "${compose[@]}" exec --no-TTY broker ./mqadmin updateTopic -c RocketMQLocalTest \
    -t "$topic" -r 1 -w 1 -a '+message.type=LITE' -n namesrv:9876 >"$evidence_dir/topic-create.log" 2>&1; then
    cat "$evidence_dir/topic-create.log" >&2
    status=1
    exit 1
fi
: >"$evidence_dir/group-create.log"
for language in java rust; do
    for suffix in base lite; do
        subgroup_args=(-c RocketMQLocalTest -g "${group}_${language}_${suffix}" -n namesrv:9876)
        if [[ "$suffix" == "lite" ]]; then
            subgroup_args+=(--attributes "+lite.bind.topic=$topic")
        fi
        "${compose[@]}" exec --no-TTY broker ./mqadmin updateSubGroup "${subgroup_args[@]}" \
            >>"$evidence_dir/group-create.log" 2>&1 || {
            cat "$evidence_dir/group-create.log" >&2
            status=1
            exit 1
        }
    done
done

if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
    -Dexec.args="prepare $broker_addr $topic ${group}_java" \
    exec:java@live-p5-extensions-probe >"$evidence_dir/java-prepare.tsv" 2>"$evidence_dir/java-prepare.stderr"; then
    cat "$evidence_dir/java-prepare.stderr" >&2
    status=1
    exit 1
fi
"$repo_root/target/debug/live_client" p5-extension-prepare "$broker_addr" "$topic" "${group}_rust" \
    >"$evidence_dir/rust-prepare.tsv" 2>"$evidence_dir/rust-prepare.stderr" || {
        cat "$evidence_dir/rust-prepare.stderr" >&2
        status=1
        exit 1
    }
if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
    -Dexec.args="send-lite $broker_addr $topic entries p5-extension-lite" \
    exec:java@live-producer-probe >"$evidence_dir/java-lite-seed.tsv" 2>"$evidence_dir/java-lite-seed.stderr"; then
    cat "$evidence_dir/java-lite-seed.stderr" >&2
    status=1
    exit 1
fi
mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
    -Dexec.args="lite-pop $broker_addr $topic ${group}_java" \
    exec:java@live-p5-extensions-probe >"$evidence_dir/java-lite-pop.tsv" 2>"$evidence_dir/java-lite-pop.stderr" || {
        cat "$evidence_dir/java-lite-pop.stderr" >&2
        status=1
        exit 1
    }
"$repo_root/target/debug/live_client" p5-extension-lite-pop "$broker_addr" "$topic" "${group}_rust" \
    >"$evidence_dir/rust-lite-pop.tsv" 2>"$evidence_dir/rust-lite-pop.stderr" || {
        cat "$evidence_dir/rust-lite-pop.stderr" >&2
        status=1
        exit 1
    }

python3 - "$evidence_dir" <<'PY'
import sys
from pathlib import Path

root = Path(sys.argv[1])
methods = {
    "heartbeat", "heartbeat-v2", "unregister", "assignment", "request-mode",
    "lite-subscription", "pop-lite",
}
expected_lite_body = b"p5-extension-lite".hex()

def read(prefix):
    rows = {}
    for path in (root / f"{prefix}-prepare.tsv", root / f"{prefix}-lite-pop.tsv"):
        for line in path.read_text().splitlines():
            fields = line.split("\t")
            if len(fields) < 3 or fields[0] != "P5E" or fields[1] not in methods:
                raise SystemExit(f"{path}: malformed P5 result {fields!r}")
            if fields[1] in rows:
                raise SystemExit(f"{path}: duplicate {fields[1]} result")
            rows[fields[1]] = fields[2:]
    if set(rows) != methods:
        raise SystemExit(f"{prefix}: missing methods {sorted(methods - set(rows))}")
    if rows["lite-subscription"] != ["OK"] or rows["pop-lite"] != ["FOUND", expected_lite_body]:
        raise SystemExit(f"{prefix}: Lite success path failed: {rows!r}")
    if rows["request-mode"] != ["OK"] or rows["unregister"] != ["OK"]:
        raise SystemExit(f"{prefix}: synchronous operation failed: {rows!r}")
    return rows

java = read("java")
rust = read("rust")
for method in methods:
    if java[method] != rust[method]:
        raise SystemExit(f"Java/Rust {method} outcomes differ: {java[method]!r} vs {rust[method]!r}")
PY
printf 'Java and Rust Lite subscription and POP success paths matched on 5.5.0.\n' >"$evidence_dir/comparison.txt"
echo "Supplementary Lite success-path checks passed; local outputs are at $evidence_dir and teardown will verify cleanup."
