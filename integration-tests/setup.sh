#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_root/integration-tests/compose.yaml"

if [[ "${ROCKETMQ_CLASSIC_TEST_ENV:-}" != "local-disposable" ]]; then
    echo "Refusing to provision: set ROCKETMQ_CLASSIC_TEST_ENV=local-disposable" >&2
    exit 2
fi

if [[ "${ROCKETMQ_PROFILE:-}" != "basic" && "${ROCKETMQ_PROFILE:-}" != "two-broker" && "${ROCKETMQ_PROFILE:-}" != "two-broker-host" && "${ROCKETMQ_PROFILE:-}" != "transaction" && "${ROCKETMQ_PROFILE:-}" != "host-routed" && "${ROCKETMQ_PROFILE:-}" != "tls" && "${ROCKETMQ_PROFILE:-}" != "tls-auth" ]]; then
    echo "ROCKETMQ_PROFILE must name an implemented profile (currently: basic, two-broker, two-broker-host, transaction, host-routed, tls, or tls-auth)" >&2
    exit 2
fi
if [[ "$ROCKETMQ_PROFILE" == "tls-auth" && ( -z "${ROCKETMQ_BROKER_AUTH_CONFIG:-}" || ! -r "${ROCKETMQ_BROKER_AUTH_CONFIG:-}" ) ]]; then
    echo "tls-auth requires ROCKETMQ_BROKER_AUTH_CONFIG to name the per-run Broker config with synthetic credentials" >&2
    exit 2
fi
if [[ "$ROCKETMQ_PROFILE" == "tls-auth" && "${ROCKETMQ_AUTH_BROKER_IMAGE:-}" != "rocketmq-classic-p19-official-5.3.4-amd64:88508d548042" ]]; then
    echo "tls-auth requires the locally built, SHA-512-verified Apache 5.3.4 binary-distribution image from build-official-rocketmq-5.3.4-amd64.sh" >&2
    exit 2
fi
if [[ "$ROCKETMQ_PROFILE" == "tls-auth" && "${ROCKETMQ_AUTH_BROKER_PLATFORM:-}" != "linux/amd64" ]]; then
    echo "tls-auth requires ROCKETMQ_AUTH_BROKER_PLATFORM=linux/amd64 for the verified RocketMQ 5.3.4 Linux RocksDB JNI library" >&2
    exit 2
fi

project_name="${COMPOSE_PROJECT_NAME:-rocketmq-classic-p19-local}"
if [[ ! "$project_name" =~ ^rocketmq-classic-p19-[a-z0-9-]+$ ]]; then
    echo "COMPOSE_PROJECT_NAME must start with rocketmq-classic-p19-" >&2
    exit 2
fi

export COMPOSE_PROJECT_NAME="$project_name"
compose=(docker compose --project-name "$project_name" --file "$compose_file" --profile "$ROCKETMQ_PROFILE")
broker_service=broker
if [[ "$ROCKETMQ_PROFILE" == "transaction" ]]; then
    broker_service=broker-tx
elif [[ "$ROCKETMQ_PROFILE" == "host-routed" ]]; then
    broker_service=broker-host
elif [[ "$ROCKETMQ_PROFILE" == "two-broker-host" ]]; then
    broker_service=broker-a-host
elif [[ "$ROCKETMQ_PROFILE" == "tls-auth" ]]; then
    broker_service=broker-auth
fi

"${compose[@]}" config --quiet
"${compose[@]}" up --detach --wait --wait-timeout 180

for attempt in {1..90}; do
    if [[ "$ROCKETMQ_PROFILE" == "tls-auth" ]]; then
        printf 'RocketMQ 5.3.4 tls-auth profile passed NameServer and ACL-enabled Broker health checks in Compose project %s\n' "$project_name"
        exit 0
    fi
    if output="$("${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin clusterList -n namesrv:9876 2>&1)" \
        && [[ "$output" == *"broker-a"* ]]; then
        if [[ "$ROCKETMQ_PROFILE" == "basic" || "$ROCKETMQ_PROFILE" == "transaction" || "$ROCKETMQ_PROFILE" == "host-routed" || "$ROCKETMQ_PROFILE" == "tls" || ( "$ROCKETMQ_PROFILE" == "two-broker-host" && "$output" == *"broker-b"* ) || ( "$ROCKETMQ_PROFILE" == "two-broker" && "$output" == *"broker-b"* ) ]]; then
            printf '%s\n' "$output"
            printf 'RocketMQ 5.3.4 %s profile is ready in Compose project %s\n' "$ROCKETMQ_PROFILE" "$project_name"
            exit 0
        fi
    fi
    sleep 1
done

printf '%s\n' "${output:-Broker did not register with the NameServer}" >&2
"${compose[@]}" logs --no-color >&2
exit 1
