#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_root/integration-tests/compose.yaml"

if [[ "${ROCKETMQ_CLASSIC_TEST_ENV:-}" != "local-disposable" ]]; then
    echo "Refusing cleanup: set ROCKETMQ_CLASSIC_TEST_ENV=local-disposable" >&2
    exit 2
fi

if [[ "${ROCKETMQ_PROFILE:-basic}" != "basic" && "${ROCKETMQ_PROFILE:-basic}" != "two-broker" && "${ROCKETMQ_PROFILE:-basic}" != "two-broker-host" && "${ROCKETMQ_PROFILE:-basic}" != "transaction" && "${ROCKETMQ_PROFILE:-basic}" != "host-routed" && "${ROCKETMQ_PROFILE:-basic}" != "tls" && "${ROCKETMQ_PROFILE:-basic}" != "tls-auth" ]]; then
    echo "ROCKETMQ_PROFILE must name an implemented profile (currently: basic, two-broker, two-broker-host, transaction, host-routed, tls, or tls-auth)" >&2
    exit 2
fi

project_name="${COMPOSE_PROJECT_NAME:-rocketmq-classic-p19-local}"
if [[ ! "$project_name" =~ ^rocketmq-classic-p19-[a-z0-9-]+$ ]]; then
    echo "COMPOSE_PROJECT_NAME must start with rocketmq-classic-p19-" >&2
    exit 2
fi

docker compose \
    --project-name "$project_name" \
    --file "$compose_file" \
    --profile "${ROCKETMQ_PROFILE:-basic}" \
    down --volumes --remove-orphans
