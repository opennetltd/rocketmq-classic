#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# The live probe uses lifecycle snapshots that are compiled only for this
# differential harness, keeping those diagnostics out of normal crate builds.
export RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }--cfg=rocketmq_classic_test_support"
source_manifest="$repo_root/tests/differential/scenarios.yaml"
manifest="${ROCKETMQ_CLASSIC_SCENARIOS_MANIFEST:-$source_manifest}"
scenario_ids=()
scenario_id_count=0
selection=""

usage() {
    cat <<'EOF'
Usage:
  tests/differential/run.sh --scenario ID
  tests/differential/run.sh --phase N
  tests/differential/run.sh --all
  tests/differential/run.sh --check-all
  tests/differential/run.sh --list

Only scenarios with a complete local runner are executable. --phase and --all
fail closed if the selected inventory contains any unimplemented scenario.
EOF
}

while (($#)); do
    case "$1" in
        --scenario)
            [[ $# -ge 2 ]] || { usage >&2; exit 2; }
            selection="scenario"
            scenario_ids+=("$2")
            scenario_id_count=$((scenario_id_count + 1))
            shift 2
            ;;
        --phase)
            [[ $# -ge 2 && -z "$selection" ]] || { usage >&2; exit 2; }
            selection="phase"
            phase_number="$2"
            shift 2
            ;;
        --all)
            [[ -z "$selection" ]] || { usage >&2; exit 2; }
            selection="all"
            shift
            ;;
        --check-all)
            [[ -z "$selection" ]] || { usage >&2; exit 2; }
            selection="check-all"
            shift
            ;;
        --list)
            [[ -z "$selection" ]] || { usage >&2; exit 2; }
            selection="list"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

[[ -n "$selection" ]] || { usage >&2; exit 2; }
if [[ "$selection" == "scenario" && $scenario_id_count -eq 0 ]]; then
    echo "At least one scenario ID is required" >&2
    exit 2
fi
if [[ "$selection" != "scenario" && $scenario_id_count -ne 0 ]]; then
    echo "--scenario cannot be combined with another selector" >&2
    exit 2
fi

if [[ "$selection" == "scenario" || "$selection" == "phase" || "$selection" == "all" ]]; then
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
    if [[ -n "${ROCKETMQ_CLASSIC_SCENARIOS_MANIFEST:-}" ]]; then
        manifest="$(python3 - "$ROCKETMQ_CLASSIC_SCENARIOS_MANIFEST" <<'PY'
import sys
from pathlib import Path
print(Path(sys.argv[1]).resolve())
PY
)"
        case "$manifest" in
            "$repo_root/"*)
                echo "A live run cannot update a scenario manifest inside the repository: $manifest" >&2
                exit 2
                ;;
        esac
    else
        manifest="$evidence_root/scenarios.json"
        cp "$source_manifest" "$manifest"
    fi
    export ROCKETMQ_CLASSIC_SCENARIOS_MANIFEST="$manifest"
fi

selected_file="$(mktemp)"
trap 'rm -f "$selected_file"' EXIT
python_args=("$manifest" "$selection" "${phase_number:-}")
if ((scenario_id_count)); then
    python_args+=("${scenario_ids[@]}")
fi
if ! python3 - "${python_args[@]}" >"$selected_file" <<'PY'
import json
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
selection = sys.argv[2]
phase = sys.argv[3]
scenario_ids = sys.argv[4:]
data = json.loads(manifest_path.read_text())
scenarios = data.get("scenarios")
if not isinstance(scenarios, list) or not scenarios:
    raise SystemExit("scenario manifest is empty or malformed")
if data.get("java_client_version") != "5.5.1" or data.get("server_version") != "5.3.4":
    raise SystemExit("scenario manifest version pins must remain Java 5.5.1 / server 5.3.4")

ids = [scenario.get("id") for scenario in scenarios]
if any(not isinstance(item, str) or not item for item in ids) or len(ids) != len(set(ids)):
    raise SystemExit("scenario manifest IDs must be nonempty and unique")

if selection in ("all", "check-all"):
    selected = scenarios
elif selection == "phase":
    if not phase.isdecimal():
        raise SystemExit("--phase requires a nonnegative integer")
    selected = [scenario for scenario in scenarios if scenario.get("phase") == int(phase)]
elif selection == "scenario":
    by_id = {scenario["id"]: scenario for scenario in scenarios}
    missing = [scenario_id for scenario_id in scenario_ids if scenario_id not in by_id]
    if missing:
        raise SystemExit("scenario ID not present in manifest: " + ", ".join(missing))
    selected = [by_id[scenario_id] for scenario_id in scenario_ids]
else:
    selected = scenarios

if not selected:
    raise SystemExit("selector matched no scenarios")
for scenario in selected:
    scenario_id = scenario.get("id")
    profile = scenario.get("profile")
    inputs = scenario.get("inputs", {})
    topic = inputs.get("topic", "-")
    if scenario.get("id") == "REQ-001-request-reply-success":
        if not isinstance(topic, str) or not topic.endswith("_java"):
            raise SystemExit("REQ-001 Java request topic must end with _java")
        expected_reverse_topic = topic[:-5] + "_rust"
        if inputs.get("reverse_topic") != expected_reverse_topic:
            raise SystemExit("REQ-001 reverse request topic must be the matching _rust topic")
    if scenario.get("id") == "PROD-005-send-timeout" and inputs.get("timeout_ms") != 100:
        raise SystemExit("PROD-005 timeout must remain the reviewed 100 ms deadline")
    if scenario_id == "P15-V03" and (
        inputs.get("request_timeout_ms") != 4500
        or inputs.get("no_reply_hold_ms") != 1500
        or inputs.get("late_reply_delay_ms") != 6500
        or inputs.get("late_reply_hold_ms") != 4000
    ):
        raise SystemExit("P15-V03 timeout and late-reply bounds differ from the reviewed live case")
    if scenario_id == "P7-V03" and (
        scenario.get("profile") != "two-broker-host"
        or inputs.get("retry_count") != 2
        or inputs.get("retry_timeout_ms") != 10000
        or inputs.get("deadline_timeout_ms") != 900
        or inputs.get("response_delay_ms") != 1800
        or inputs.get("retry_response_code") != 16
        or inputs.get("retry_body") != "p7-retry-original-body"
        or inputs.get("deadline_body") != "p7-deadline-original-body"
    ):
        raise SystemExit("P7-V03 retry and deadline bounds differ from the reviewed live case")
    if scenario_id == "P10-V03" and (
        scenario.get("profile") != "two-broker"
        or inputs.get("topic") != "compat_p10_retry"
        or inputs.get("group") != "compat_p10_push"
        or inputs.get("assignment") != "test-only-fixed"
        or inputs.get("checks") != [
            "java-produced-message", "listener-success", "listener-partial",
            "listener-failure", "retry-count", "configured-dlq-transition"
        ]
    ):
        raise SystemExit("P10-V03 retry and DLQ inputs differ from the reviewed live case")
    if scenario_id == "P11-V03" and (
        scenario.get("profile") != "two-broker"
        or inputs.get("topic") != "compat_p11_mixed"
        or inputs.get("group") != "compat_p11_mixed"
        or inputs.get("clients") != ["java-5.5.1", "rust"]
        or inputs.get("checks") != [
            "join", "leave", "convergence-timeout",
            "no-persistent-duplicate-ownership", "transient-redelivery-allowed"
        ]
    ):
        raise SystemExit("P11-V03 mixed-group inputs differ from the reviewed live case")
    if scenario_id == "P11-V04" and (
        profile != "two-broker"
        or inputs.get("topic") != "compat_p11_push"
        or inputs.get("group") != "compat_p11_push"
        or inputs.get("checks") != [
            "java-producer-to-rust-push", "rust-producer-to-java-push", "retry",
            "offsets", "repeated-shutdown-no-scheduler-leak"
        ]
    ):
        raise SystemExit("P11-V04 Push interoperability inputs differ from the reviewed live case")
    if scenario_id == "P12-V03" and (
        profile != "two-broker"
        or inputs.get("topic") != "compat_p12_orderly"
        or inputs.get("group") != "compat_p12_orderly"
        or inputs.get("clients") != ["java-5.5.1", "rust"]
        or inputs.get("checks") != [
            "orderly-lock-contention", "slow-callback-rebalance",
            "no-stale-offset-commit", "shutdown-cleanup",
            "broker-lock-expiry-convergence"
        ]
        or scenario.get("assertions") != inputs.get("checks")
    ):
        raise SystemExit("P12-V03 orderly-consumer inputs differ from the reviewed live case")
    if scenario_id == "P14-V03" and (
        profile != "two-broker"
        or inputs.get("topic") != "compat_p14_transaction"
        or inputs.get("group") != "compat_p14_transaction"
        or inputs.get("clients") != ["java-5.5.1", "rust"]
        or inputs.get("checks") != [
            "commit-visible", "rollback-invisible", "unknown-check-resolution",
            "bounded-check-interval"
        ]
        or scenario.get("assertions") != inputs.get("checks")
        or scenario.get("client_directions") != [
            "java-5.5.1-transactional-producer->rust-consumer",
            "rust-transactional-producer->java-5.5.1-consumer"
        ]
    ):
        raise SystemExit("P14-V03 transaction inputs differ from the reviewed live case")
    if scenario_id == "P16-V02" and (
        profile != "tls-auth"
        or inputs.get("checks") != [
            "signed-request-accepted", "wrong-secret-rejected", "tampered-body-rejected",
            "trusted-ca-accepted", "wrong-hostname-accepted-with-trusted-ca",
            "untrusted-ca-rejected",
            "missing-client-certificate-rejected"
        ]
        or scenario.get("assertions") != inputs.get("checks")
        or scenario.get("client_directions") != [
            "java-5.5.1-client->server-5.3.4", "rust-client->server-5.3.4"
        ]
    ):
        raise SystemExit("P16-V02 security inputs differ from the reviewed live case")
    if scenario_id == "P17-V02" and (
        profile != "two-broker"
        or inputs.get("topic") != "compat_p17_admin_query"
        or inputs.get("keys") != ["duplicate-key", "bounded-time-range"]
        or inputs.get("checks") != [
            "java-rust-results-match", "partial-broker-failure", "terminal-error-mapping"
        ]
        or scenario.get("assertions") != inputs.get("checks")
        or scenario.get("client_directions") != [
            "java-5.5.1-client->server-5.3.4", "rust-client->server-5.3.4"
        ]
    ):
        raise SystemExit("P17-V02 query inputs differ from the reviewed live case")
    if scenario_id == "P7-V04" and (
        profile != "two-broker-host"
        or inputs.get("topic") != "compat_p7_lifecycle"
        or inputs.get("messages") != 8192
        or inputs.get("max_inflight") != 32
        or inputs.get("seed") != 1592594996
        or inputs.get("checks") != [
            "sustained-sends", "fixed-seed-variable-payloads", "bounded-concurrency",
            "contiguous-broker-offsets", "matching-workload-checksum",
            "repeated-start-shutdown", "zero-pending-requests", "zero-connections",
            "zero-owned-tasks"
        ]
        or scenario.get("assertions") != inputs.get("checks")
    ):
        raise SystemExit("P7-V04 bounded stress inputs differ from the reviewed live case")
    if scenario_id == "P5-V02-producer" and inputs.get("methods") != [
        "send-v1", "send-v2", "batch", "reply-v1", "reply-v2", "end-transaction"
    ]:
        raise SystemExit("P5 producer method inventory differs from the reviewed live case")
    if scenario_id == "P5-V02-consumer" and inputs.get("methods") != [
        "pull", "lite-pull", "query-offset", "update-offset", "consumer-list",
        "send-back", "lock", "unlock"
    ]:
        raise SystemExit("P5 consumer method inventory differs from the reviewed live case")
    if scenario_id == "P5-V02-extensions" and inputs.get("methods") != [
        "heartbeat", "heartbeat-v2", "unregister", "assignment", "request-mode",
        "lite-subscription", "pop", "pop-lite", "ack", "batch-ack", "change-invisible-time"
    ]:
        raise SystemExit("P5 extension method inventory differs from the reviewed live case")
    producer_group = inputs.get("producer_group", inputs.get("transaction_group", inputs.get("group", "-")))
    consumer_group = inputs.get("consumer_group", inputs.get("group", "-"))
    body = inputs.get("body", "-")
    key_prefix = inputs.get("key_prefix", "-")
    java_expected_max_offset = inputs.get("java_expected_max_offset", "-")
    rust_expected_max_offset = inputs.get("rust_expected_max_offset", "-")
    java_expected_queue_offset = inputs.get("java_expected_queue_offset", "-")
    rust_expected_queue_offset = inputs.get("rust_expected_queue_offset", "-")
    message_count = inputs.get("messages", "-")
    queue_id = inputs.get("queue_id", "-")
    java_offset = inputs.get("java_offset", "-")
    rust_offset = inputs.get("rust_offset", "-")
    pull_offset = inputs.get("pull_offset", "-")
    missing_topic = inputs.get("missing_topic", "-")
    queue_count = inputs.get("queues", "-")
    consumer_ids = inputs.get("consumers", "-")
    if not isinstance(profile, str) or not profile:
        raise SystemExit(f"{scenario_id}: missing profile")
    if not isinstance(key_prefix, str) or not key_prefix:
        raise SystemExit(f"{scenario_id}: invalid key prefix")
    print(
        f"{scenario_id}\t{profile}\t{topic}\t{producer_group}\t{body}\t{key_prefix}\t"
        f"{java_expected_max_offset}\t{rust_expected_max_offset}\t"
        f"{java_expected_queue_offset}\t{rust_expected_queue_offset}\t{message_count}\t"
        f"{java_offset}\t{rust_offset}\t{queue_id}\t{pull_offset}\t{missing_topic}\t"
        f"{queue_count}\t{consumer_ids}\t{consumer_group}"
    )
PY
then
    exit 2
fi

if [[ "$selection" == "list" ]]; then
    cat "$selected_file"
    exit 0
fi

implemented=("ROUTE-001-get-existing-topic" "ROUTE-002-topic-not-exist" "HEARTBEAT-001" "PROD-001-sync-send" "PROD-002-async-send" "PROD-003-oneway-send" "PROD-004-batch-send" "PROD-005-send-timeout" "CONS-001-query-offset" "CONS-002-pull-found" "CONS-003-pull-no-new-msg" "CONS-004-update-offset" "ORDER-001-lock-unlock" "REBAL-001-average-10q-3c" "TX-001-commit" "TX-002-rollback" "P3-V04" "P5-V02-route" "P5-V02-producer" "P5-V02-consumer" "P5-V02-extensions" "P0-V03" "P6-V04" "P7-V02" "P7-V03" "P7-V04" "P9-V03" "P10-V03" "P11-V03" "P11-V04" "P12-V03" "P13-V02" "P14-V03" "P16-V02" "P17-V02" "REQ-001-request-reply-success" "P15-V03")
unsupported=()
while IFS=$'\t' read -r scenario_id profile topic producer_group body key_prefix java_expected_max_offset rust_expected_max_offset java_expected_queue_offset rust_expected_queue_offset message_count java_offset rust_offset queue_id pull_offset missing_topic queue_count consumer_ids consumer_group; do
    is_implemented=false
    for implemented_id in "${implemented[@]}"; do
        if [[ "$scenario_id" == "$implemented_id" ]]; then
            is_implemented=true
            break
        fi
    done
    if [[ "$is_implemented" != true ]]; then
        unsupported+=("$scenario_id")
    fi
done <"$selected_file"

if ((${#unsupported[@]})); then
    printf 'Refusing to skip unimplemented scenarios (%d):\n' "${#unsupported[@]}" >&2
    printf '  %s\n' "${unsupported[@]}" >&2
    echo "No Docker resources were started and no selected scenario counts as passed." >&2
    exit 3
fi

if [[ "$selection" == "check-all" ]]; then
    row_count="$(wc -l <"$selected_file" | tr -d ' ')"
    printf 'Every selected live scenario has a local runner (%s rows); no Docker resources were started.\n' \
        "$row_count"
    exit 0
fi

if [[ "$selection" != "scenario" ]]; then
    while IFS=$'\t' read -r scenario_id profile _; do
        printf 'Running manifest scenario %s with profile %s\n' "$scenario_id" "$profile"
        # Child probes must not consume the parent loop's manifest input.
        ROCKETMQ_PROFILE="$profile" "$repo_root/tests/differential/run.sh" --scenario "$scenario_id" </dev/null
    done <"$selected_file"
    exit 0
fi

IFS=$'\t' read -r scenario_id profile topic producer_group body key_prefix java_expected_max_offset rust_expected_max_offset java_expected_queue_offset rust_expected_queue_offset message_count java_offset rust_offset queue_id pull_offset missing_topic queue_count consumer_ids consumer_group <"$selected_file"
rust_request_topic=""
timeout_ms=""
p15_topics=()
p15_timeout_ms=4500
p15_no_reply_hold_ms=1500
p15_late_reply_delay_ms=6500
p15_late_reply_hold_ms=4000
p7_retry_timeout_ms=10000
p7_deadline_timeout_ms=900
p7_response_delay_ms=1800
p7_retry_body="p7-retry-original-body"
p7_deadline_body="p7-deadline-original-body"
p10_retry_max_times=1
if [[ "$scenario_id" == "REQ-001-request-reply-success" ]]; then
    if [[ "$topic" != *_java ]]; then
        echo "The Java request topic must end with _java so the reverse-direction topic can be derived safely" >&2
        exit 3
    fi
    rust_request_topic="${topic%_java}_rust"
fi
if [[ "$scenario_id" == "P15-V03" ]]; then
    p15_topics=("${topic}_reverse" "${topic}_no_reply_java" "${topic}_no_reply_rust" "${topic}_late_java" "${topic}_late_rust")
fi
if [[ "$scenario_id" == "P10-V03" ]]; then
    p10_retry_topic="%RETRY%${consumer_group}"
    p10_dlq_topic="%DLQ%${consumer_group}"
fi
if [[ "$scenario_id" == "PROD-005-send-timeout" ]]; then
    timeout_ms=100
fi
case "$profile:$scenario_id" in
    basic:*) ;;
    two-broker:ORDER-001-lock-unlock|two-broker:REBAL-001-average-10q-3c|two-broker:PROD-005-send-timeout|two-broker:P5-V02-consumer|two-broker:P10-V03|two-broker:P11-V03|two-broker:P11-V04|two-broker:P12-V03|two-broker:P13-V02|two-broker:P14-V03|two-broker:P17-V02) ;;
    two-broker-host:P7-V03|two-broker-host:P7-V04|two-broker-host:P15-V03) ;;
    tls-auth:P16-V02) ;;
    transaction:TX-001-commit|transaction:TX-002-rollback) ;;
    tls:P3-V04) ;;
    host-routed:P6-V04|host-routed:P7-V02|host-routed:P9-V03) ;;
    host-routed:REQ-001-request-reply-success) ;;
    *)
        echo "Scenario metadata differs from the implemented profile" >&2
        exit 3
        ;;
esac
case "$scenario_id:$topic" in
    ROUTE-001-get-existing-topic:compat_phase0_route|ROUTE-002-topic-not-exist:compat_phase0_missing) ;;
    P3-V04:compat_p3_tls) ;;
    HEARTBEAT-001:-)
        if [[ "$producer_group" != "compat_phase0" ]]; then
            echo "Scenario metadata differs from the reviewed heartbeat implementation" >&2
            exit 3
        fi
        ;;
    PROD-001-sync-send:compat_phase0_send|PROD-002-async-send:compat_phase0_send|PROD-003-oneway-send:compat_phase0_send)
        if [[ "$body" != "fixed" ]]; then
            echo "Scenario metadata differs from the reviewed producer-send implementation" >&2
            exit 3
        fi
        if [[ "$scenario_id" == "PROD-003-oneway-send" && "$key_prefix" != "compat_phase0_oneway" ]]; then
            echo "One-way send key prefix differs from the reviewed scenario input" >&2
            exit 3
        fi
        if [[ "$scenario_id" == "PROD-003-oneway-send" && \
            ("$java_expected_max_offset" != "1" || "$rust_expected_max_offset" != "2" || \
                "$java_expected_queue_offset" != "0" || "$rust_expected_queue_offset" != "1") ]]; then
            echo "One-way expected queue offsets differ from the isolated topic's empty start" >&2
            exit 3
        fi
        ;;
    PROD-004-batch-send:compat_phase0_batch)
        if [[ "$body" != "batch" || "$message_count" != "2" ]]; then
            echo "Batch send metadata differs from the reviewed two-message input" >&2
            exit 3
        fi
        ;;
    P5-V02-producer:compat_p5_send)
        if [[ "$profile" != "basic" ]]; then
            echo "P5 producer RPC coverage requires the basic profile" >&2
            exit 3
        fi
        ;;
    P5-V02-consumer:compat_p5_consumer)
        if [[ "$profile" != "two-broker" || "$consumer_group" != "compat_p5_consumer" ]]; then
            echo "P5 consumer RPC coverage requires the two-broker profile and scenario group" >&2
            exit 3
        fi
        ;;
    P5-V02-extensions:compat_p5_extension)
        if [[ "$profile" != "basic" || "$consumer_group" != "compat_p5_extension" ]]; then
            echo "P5 extension RPC coverage requires the basic profile and scenario group" >&2
            exit 3
        fi
        ;;
    P13-V02:compat_p13_pop)
        if [[ "$profile" != "two-broker" || "$consumer_group" != "compat_p13_pop" ]]; then
            echo "P13 POP lifecycle coverage requires the two-broker profile and scenario group" >&2
            exit 3
        fi
        ;;
    CONS-001-query-offset:compat_phase0_query_offset)
        if [[ "$producer_group" != "compat_phase0_query_group" || "$java_offset" != "17" || "$rust_offset" != "23" || "$queue_id" != "0" ]]; then
            echo "Consumer-offset metadata differs from the reviewed reciprocal update/query case" >&2
            exit 3
        fi
        ;;
    CONS-004-update-offset:compat_phase0_update_offset)
        if [[ "$producer_group" != "compat_phase0_update_group" || "$java_offset" != "31" || "$rust_offset" != "47" || "$queue_id" != "0" ]]; then
            echo "Offset-update metadata differs from the reviewed reciprocal update/query case" >&2
            exit 3
        fi
        ;;
    CONS-002-pull-found:compat_phase0_pull_found)
        if [[ "$producer_group" != "compat_phase0_pull_found_group" || "$body" != "pull-seed" || "$queue_id" != "0" || "$pull_offset" != "0" ]]; then
            echo "Found-pull metadata differs from the reviewed seeded queue case" >&2
            exit 3
        fi
        ;;
    CONS-003-pull-no-new-msg:compat_phase0_pull_empty)
        if [[ "$producer_group" != "compat_phase0_pull_empty_group" || "$queue_id" != "0" || "$pull_offset" != "0" ]]; then
            echo "Empty-pull metadata differs from the reviewed empty queue case" >&2
            exit 3
        fi
        ;;
    P5-V02-route:compat_p5_route)
        if [[ "$missing_topic" != "compat_p5_missing" ]]; then
            echo "P5 route metadata differs from the reviewed existing/missing topic pair" >&2
            exit 3
        fi
        ;;
    ORDER-001-lock-unlock:compat_phase0_order_topic)
        if [[ "$profile" != "two-broker" || "$producer_group" != "compat_phase0_order" ]]; then
            echo "Lock/unlock metadata differs from the reviewed two-broker queue-lock case" >&2
            exit 3
        fi
        ;;
    REBAL-001-average-10q-3c:compat_phase0_rebalance)
        if [[ "$profile" != "two-broker" || "$queue_count" != "10" || "$consumer_ids" != "consumer-a,consumer-b,consumer-c" ]]; then
            echo "Rebalance metadata differs from the reviewed 10-queue / 3-consumer case" >&2
            exit 3
        fi
        ;;
    TX-001-commit:compat_phase0_tx|TX-002-rollback:compat_phase0_tx)
        expected_transaction_key="compat_tx_commit"
        expected_consumer_group="compat_phase0_tx_consumer_group"
        if [[ "$scenario_id" == "TX-002-rollback" ]]; then
            expected_transaction_key="compat_tx_rollback"
        fi
        if [[ "$profile" != "transaction" || "$producer_group" != "compat_phase0_tx_group" || "$consumer_group" != "$expected_consumer_group" || "$key_prefix" != "$expected_transaction_key" || "$body" != "transaction-body" ]]; then
            echo "Transaction metadata differs from the reviewed local commit/rollback cases" >&2
            exit 3
        fi
        ;;
    P7-V02:compat_p7_send)
        if [[ "$profile" != "host-routed" ]]; then
            echo "P7-V02 requires the host-routed producer profile" >&2
            exit 3
        fi
        ;;
    P7-V03:compat_p7_retry)
        if [[ "$profile" != "two-broker-host" ]]; then
            echo "P7-V03 requires the host-routed two-Broker profile" >&2
            exit 3
        fi
        ;;
    P10-V03:compat_p10_retry)
        if [[ "$profile" != "two-broker" || "$consumer_group" != "compat_p10_push" ]]; then
            echo "P10-V03 requires the two-Broker profile and the reviewed Push consumer group" >&2
            exit 3
        fi
        ;;
    P11-V03:compat_p11_mixed)
        if [[ "$profile" != "two-broker" || "$consumer_group" != "compat_p11_mixed" ]]; then
            echo "P11-V03 requires the two-Broker profile and the reviewed mixed-client group" >&2
            exit 3
        fi
        ;;
    P11-V04:compat_p11_push)
        if [[ "$profile" != "two-broker" || "$consumer_group" != "compat_p11_push" ]]; then
            echo "P11-V04 requires the two-Broker profile and the reviewed Push consumer group" >&2
            exit 3
        fi
        ;;
    P12-V03:compat_p12_orderly)
        if [[ "$profile" != "two-broker" || "$consumer_group" != "compat_p12_orderly" ]]; then
            echo "P12-V03 requires the two-Broker profile and the reviewed orderly consumer group" >&2
            exit 3
        fi
        ;;
    P14-V03:compat_p14_transaction)
        if [[ "$profile" != "two-broker" || "$consumer_group" != "compat_p14_transaction" ]]; then
            echo "P14-V03 requires the two-Broker profile and the reviewed transaction group" >&2
            exit 3
        fi
        ;;
    P16-V02:-) ;;
    P17-V02:compat_p17_admin_query) ;;
    P7-V04:compat_p7_lifecycle)
        if [[ "$profile" != "two-broker-host" ]]; then
            echo "P7-V04 requires the host-routed two-Broker profile" >&2
            exit 3
        fi
        ;;
    P15-V03:compat_p15_request_reply)
        if [[ "$profile" != "two-broker-host" || "$consumer_group" != "compat_p15_request_reply" ]]; then
            echo "P15-V03 metadata differs from the reviewed two-Broker request/reply case" >&2
            exit 3
        fi
        ;;
    P9-V03:compat_p9_pull)
        if [[ "$profile" != "host-routed" || "$consumer_group" != "compat_p9_pull" ]]; then
            echo "P9-V03 metadata differs from the reviewed explicit-pull case" >&2
            exit 3
        fi
        ;;
    REQ-001-request-reply-success:compat_phase0_request_java)
        if [[ "$profile" != "host-routed" || "$consumer_group" != "compat_req_responder" ]]; then
            echo "Request/reply metadata differs from the reviewed host-routed case" >&2
            exit 3
        fi
        ;;
    PROD-005-send-timeout:compat_phase0_timeout)
        if [[ "$profile" != "two-broker" || "$timeout_ms" != "100" ]]; then
            echo "Producer timeout metadata differs from the reviewed two-Broker 100 ms case" >&2
            exit 3
        fi
        ;;
    P6-V04:compat_p6_shared)
        if [[ "$profile" != "host-routed" || "$consumer_group" != "compat_p6_consumer" ]]; then
            echo "P6-V04 metadata differs from the reviewed shared-runtime case" >&2
            exit 3
        fi
        ;;
    P0-V03:-)
        if [[ "$profile" != "basic" ]]; then
            echo "P0-V03 must use the reviewed basic-profile aggregate" >&2
            exit 3
        fi
        ;;
    *)
        echo "Scenario metadata differs from the reviewed case implementations" >&2
        exit 3
        ;;
esac
if [[ "$scenario_id" != "ROUTE-001-get-existing-topic" && "$scenario_id" != "ROUTE-002-topic-not-exist" && "$scenario_id" != "HEARTBEAT-001" && "$scenario_id" != "PROD-001-sync-send" && "$scenario_id" != "PROD-002-async-send" && "$scenario_id" != "PROD-003-oneway-send" && "$scenario_id" != "PROD-004-batch-send" && "$scenario_id" != "PROD-005-send-timeout" && "$scenario_id" != "CONS-001-query-offset" && "$scenario_id" != "CONS-004-update-offset" && "$scenario_id" != "CONS-002-pull-found" && "$scenario_id" != "CONS-003-pull-no-new-msg" && "$scenario_id" != "ORDER-001-lock-unlock" && "$scenario_id" != "REBAL-001-average-10q-3c" && "$scenario_id" != "TX-001-commit" && "$scenario_id" != "TX-002-rollback" && "$scenario_id" != "P3-V04" && "$scenario_id" != "P5-V02-route" && "$scenario_id" != "P5-V02-producer" && "$scenario_id" != "P5-V02-consumer" && "$scenario_id" != "P5-V02-extensions" && "$scenario_id" != "P0-V03" && "$scenario_id" != "P6-V04" && "$scenario_id" != "P7-V02" && "$scenario_id" != "P7-V03" && "$scenario_id" != "P7-V04" && "$scenario_id" != "P9-V03" && "$scenario_id" != "P10-V03" && "$scenario_id" != "P11-V03" && "$scenario_id" != "P11-V04" && "$scenario_id" != "P12-V03" && "$scenario_id" != "P13-V02" && "$scenario_id" != "P14-V03" && "$scenario_id" != "P16-V02" && "$scenario_id" != "P17-V02" && "$scenario_id" != "REQ-001-request-reply-success" && "$scenario_id" != "P15-V03" ]]; then
    echo "No implementation is registered for scenario '$scenario_id'" >&2
    exit 3
fi
if [[ "${ROCKETMQ_CLASSIC_TEST_ENV:-}" != "local-disposable" ]]; then
    echo "Refusing to run live scenario: set ROCKETMQ_CLASSIC_TEST_ENV=local-disposable" >&2
    exit 2
fi
if [[ "${ROCKETMQ_PROFILE:-}" != "$profile" ]]; then
    echo "ROCKETMQ_PROFILE must explicitly equal the manifest profile '$profile'" >&2
    exit 2
fi
manifest_profile="$profile"
if [[ "$scenario_id" == "P10-V03" || "$scenario_id" == "P11-V03" || "$scenario_id" == "P11-V04" || "$scenario_id" == "P12-V03" || "$scenario_id" == "P14-V03" || "$scenario_id" == "P17-V02" ]]; then
    # The manifest topology is two Brokers. Host-run probes need reachable
    # addresses from the two-Broker Compose variant.
    profile="two-broker-host"
fi
if [[ "$scenario_id" == "P14-V03" ]]; then
    export ROCKETMQ_BROKER_A_HOST_CONFIG=./config/broker-a-transaction-host.conf
    export ROCKETMQ_BROKER_B_HOST_CONFIG=./config/broker-b-transaction-host.conf
fi
maven_java_version="$(mvn -version 2>&1 | sed -n 's/^Java version: \([^,]*\),.*/\1/p')"
maven_java_major="${maven_java_version%%.*}"
if [[ "$maven_java_major" != "27" ]]; then
    echo "The live Java 5.5.1 runner requires the CI-pinned Maven JDK 27; found '${maven_java_version:-unknown}'" >&2
    exit 2
fi
rustc_version="$(rustc --version | awk '{print $2}')"
if [[ "$rustc_version" != "1.85.0" ]]; then
    echo "The live Rust runner requires the workspace-pinned Rust 1.85.0; found '${rustc_version:-unknown}'" >&2
    exit 2
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
started_epoch="$(date +%s)"
run_slug="$(printf '%s' "$run_id" | tr '[:upper:]' '[:lower:]')"
project_slug="$(printf '%s' "$scenario_id" | tr '[:upper:]_' '[:lower:]-')"
java_oneway_key=""
rust_oneway_key=""
if [[ "$scenario_id" == "PROD-003-oneway-send" ]]; then
    java_oneway_key="${key_prefix}_java_${run_slug}"
    rust_oneway_key="${key_prefix}_rust_${run_slug}"
fi
project_name="${COMPOSE_PROJECT_NAME:-rocketmq-classic-p19-${project_slug}-${run_slug}}"
if [[ ! "$project_name" =~ ^rocketmq-classic-p19-[a-z0-9-]+$ ]]; then
    echo "COMPOSE_PROJECT_NAME must start with rocketmq-classic-p19- and use lowercase letters, digits, and hyphens" >&2
    exit 2
fi
export COMPOSE_PROJECT_NAME="$project_name"
export ROCKETMQ_PROFILE="$profile"
default_namesrv_host_port=19876
if [[ "$profile" == "tls-auth" ]]; then
    default_namesrv_host_port=61876
fi
export ROCKETMQ_NAMESRV_HOST_PORT="${ROCKETMQ_NAMESRV_HOST_PORT:-$default_namesrv_host_port}"
default_broker_host_port=20911
if [[ "$profile" == "transaction" ]]; then
    default_broker_host_port=30911
elif [[ "$profile" == "host-routed" ]]; then
    default_broker_host_port=40911
elif [[ "$profile" == "two-broker-host" ]]; then
    default_broker_host_port=50911
elif [[ "$profile" == "tls-auth" ]]; then
    default_broker_host_port=61911
fi
export ROCKETMQ_BROKER_HOST_PORT="${ROCKETMQ_BROKER_HOST_PORT:-$default_broker_host_port}"
if [[ "$profile" == "tls-auth" && ( "$ROCKETMQ_NAMESRV_HOST_PORT" != "61876" || "$ROCKETMQ_BROKER_HOST_PORT" != "61911" ) ]]; then
    echo "The tls-auth profile reserves NameServer/Broker ports 61876/61911 for its local probes" >&2
    exit 2
fi
if [[ "$profile" == "transaction" && "$ROCKETMQ_BROKER_HOST_PORT" != "30911" ]]; then
    echo "The transaction profile advertises broker-a on 127.0.0.1:30911; set ROCKETMQ_BROKER_HOST_PORT=30911" >&2
    exit 2
fi
if [[ "$profile" == "host-routed" && "$ROCKETMQ_BROKER_HOST_PORT" != "40911" ]]; then
    echo "The host-routed profile advertises broker-a on 127.0.0.1:40911; set ROCKETMQ_BROKER_HOST_PORT=40911" >&2
    exit 2
fi
if [[ "$profile" == "two-broker-host" ]]; then
    export ROCKETMQ_BROKER_B_HOST_PORT="${ROCKETMQ_BROKER_B_HOST_PORT:-50921}"
    if [[ "$ROCKETMQ_BROKER_HOST_PORT" != "50911" || "$ROCKETMQ_BROKER_B_HOST_PORT" != "50921" ]]; then
        echo "The two-Broker host profile advertises 127.0.0.1:50911 and 127.0.0.1:50921" >&2
        exit 2
    fi
    if [[ "$scenario_id" == "P7-V03" || "$scenario_id" == "P17-V02" ]]; then
        if [[ "${ROCKETMQ_BROKER_A_PUBLISHED_HOST_PORT:-51911}" != "51911" || \
            "${ROCKETMQ_BROKER_B_PUBLISHED_HOST_PORT:-51921}" != "51921" ]]; then
            echo "$scenario_id requires direct Broker ports 51911/51921 behind proxies at advertised ports 50911/50921" >&2
            exit 2
        fi
        export ROCKETMQ_BROKER_A_PUBLISHED_HOST_PORT=51911
        export ROCKETMQ_BROKER_B_PUBLISHED_HOST_PORT=51921
    else
        export ROCKETMQ_BROKER_A_PUBLISHED_HOST_PORT="${ROCKETMQ_BROKER_A_PUBLISHED_HOST_PORT:-50911}"
        export ROCKETMQ_BROKER_B_PUBLISHED_HOST_PORT="${ROCKETMQ_BROKER_B_PUBLISHED_HOST_PORT:-50921}"
    fi
fi
compose=(docker compose --project-name "$project_name" --file "$repo_root/integration-tests/compose.yaml" --profile "$profile")
broker_service=broker
if [[ "$profile" == "transaction" ]]; then
    broker_service=broker-tx
elif [[ "$profile" == "host-routed" ]]; then
    broker_service=broker-host
elif [[ "$profile" == "two-broker-host" ]]; then
    broker_service=broker-a-host
elif [[ "$profile" == "tls-auth" ]]; then
    broker_service=broker-auth
fi
if ! command -v docker >/dev/null || ! docker compose version >/dev/null 2>&1; then
    echo "Docker Engine and Docker Compose are required for live differential scenarios" >&2
    exit 2
fi
if [[ -n "$("${compose[@]}" ps --all --quiet 2>/dev/null)" ]]; then
    echo "Refusing to reuse nonempty Compose project '$project_name'" >&2
    exit 2
fi

evidence_dir="$evidence_root/${scenario_id}/${run_id}"
namesrv_addr="127.0.0.1:${ROCKETMQ_NAMESRV_HOST_PORT}"
broker_addr="127.0.0.1:${ROCKETMQ_BROKER_HOST_PORT}"
code_dirty_paths=(Cargo.toml Cargo.lock crates/ compat/java-reference/ integration-tests/
    scripts/check-workspace-metadata.py
    compat/java-reference/pom.xml
    compat/java-reference/src/main/java/compat/LiveMixedGroupProbe.java
    compat/java-reference/src/main/java/compat/LiveOrderlyInteropProbe.java
    compat/java-reference/src/main/java/compat/LivePushInteropProbe.java
    compat/java-reference/src/main/java/compat/LiveRouteProbe.java
    compat/java-reference/src/main/java/compat/LiveP3RemotingProbe.java
    compat/java-reference/src/main/java/compat/LiveP16SecurityProbe.java
    compat/java-reference/src/main/java/compat/LiveP17AdminQueryProbe.java
    compat/java-reference/src/main/java/compat/LiveHeartbeatProbe.java
    compat/java-reference/src/main/java/compat/LiveProducerProbe.java
    compat/java-reference/src/main/java/compat/LiveP5ProducerProbe.java
    compat/java-reference/src/main/java/compat/LiveP5ExtensionsProbe.java
    compat/java-reference/src/main/java/compat/LiveP13PopProbe.java
    compat/java-reference/src/main/java/compat/LiveConsumerProbe.java
    compat/java-reference/src/main/java/compat/LiveRebalanceProbe.java
    compat/java-reference/src/main/java/compat/LiveTransactionProbe.java
    compat/java-reference/src/main/java/compat/LiveHighLevelProducerProbe.java
    compat/java-reference/src/main/java/compat/LiveProducerLifecycleProbe.java
    compat/java-reference/src/main/java/compat/LiveSharedRuntimeProbe.java
    compat/java-reference/src/main/java/compat/LivePullConsumerProbe.java
    compat/java-reference/src/main/java/compat/LiveRequestReplyProbe.java
    tests/differential/delayed_response_proxy.py tests/differential/tls_broker_proxy.py tests/differential/acl_tampering_proxy.py tests/differential/query_failure_proxy.py tests/differential/check_p17_admin_query.py
    crates/rocketmq-classic-remoting/src/client/mod.rs
    integration-tests/Cargo.toml integration-tests/src/bin/live_client.rs
    integration-tests/build-official-rocketmq-5.3.4-amd64.sh integration-tests/docker
    integration-tests/compose.yaml integration-tests/config/broker.conf integration-tests/config/broker-b.conf integration-tests/config/broker-tx.conf integration-tests/config/broker-host.conf integration-tests/config/broker-a-host.conf integration-tests/config/broker-b-host.conf integration-tests/config/broker-a-transaction-host.conf integration-tests/config/broker-b-transaction-host.conf integration-tests/config/broker-auth.conf
    integration-tests/setup.sh integration-tests/cleanup.sh
    tests/differential/compare.py tests/differential/run.sh tests/differential/check_mixed_ownership.py tests/differential/check_p11_push.py tests/differential/check_p12_orderly.py)
if [[ -n "$(git -C "$repo_root" status --porcelain -- "${code_dirty_paths[@]}")" ]]; then
    source_code_dirty=true
else
    source_code_dirty=false
fi
if ! python3 - "$repo_root" <<'PY'
import json
import subprocess
import sys
from pathlib import Path

root = Path(sys.argv[1])
try:
    committed = subprocess.check_output(
        ["git", "show", "HEAD:tests/differential/scenarios.yaml"], cwd=root, text=True
    )
    current = (root / "tests/differential/scenarios.yaml").read_text()
    before = json.loads(committed)
    after = json.loads(current)
    for manifest in (before, after):
        for scenario in manifest.get("scenarios", []):
            scenario.pop("live_status", None)
            scenario.pop("evidence", None)
    if before != after:
        raise SystemExit(1)
except (OSError, subprocess.CalledProcessError, json.JSONDecodeError):
    raise SystemExit(1)
PY
then
    source_code_dirty=true
fi
mkdir -p "$evidence_dir"
cp "$selected_file" "$evidence_dir/selected.tsv"
auth_broker_image=""
if [[ "$scenario_id" == "P16-V02" ]]; then
    if ! auth_broker_image="$(bash "$repo_root/integration-tests/build-official-rocketmq-5.3.4-amd64.sh" \
        2>"$evidence_dir/official-distribution-build.log")"; then
        cat "$evidence_dir/official-distribution-build.log" >&2
        exit 1
    fi
    export ROCKETMQ_AUTH_BROKER_IMAGE="$auth_broker_image"
    export ROCKETMQ_AUTH_BROKER_PLATFORM=linux/amd64
fi
server_image=apache/rocketmq:5.3.4@sha256:d57ee4ebbd9d7bb6e185775bde41c128e98f185b25bf0b05c77d4e6c010572b8
if [[ -n "$auth_broker_image" ]]; then
    server_image="$auth_broker_image"
fi
printf 'scenario=%s\nprofile=%s\nproject=%s\nrun_id=%s\njava_client=5.5.1\nserver=5.3.4\nserver_image=%s\nsource_revision=%s\nsource_code_dirty=%s\nproducer_group=%s\nbody=%s\nseed=not-applicable\n' \
    "$scenario_id" "$manifest_profile" "$project_name" "$run_id" \
    "$server_image" \
    "$(git -C "$repo_root" rev-parse HEAD)" "$source_code_dirty" "$producer_group" "$body" \
    >"$evidence_dir/metadata.txt"
if [[ "$profile" != "$manifest_profile" ]]; then
    printf 'runtime_compose_profile=%s\nruntime_profile_reason=host-routed-two-broker-profile-for-host-client-reachability\n' "$profile" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "PROD-003-oneway-send" ]]; then
    printf 'java_message_key=%s\nrust_message_key=%s\n' "$java_oneway_key" "$rust_oneway_key" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "PROD-004-batch-send" ]]; then
    printf 'message_count=%s\nbody_prefix=%s\n' "$message_count" "$body" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "CONS-001-query-offset" || "$scenario_id" == "CONS-004-update-offset" ]]; then
    printf 'java_offset=%s\nrust_offset=%s\nqueue_id=%s\n' "$java_offset" "$rust_offset" "$queue_id" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "CONS-002-pull-found" || "$scenario_id" == "CONS-003-pull-no-new-msg" ]]; then
    printf 'queue_id=%s\npull_offset=%s\nbody=%s\n' "$queue_id" "$pull_offset" "$body" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P5-V02-route" ]]; then
    printf 'existing_topic=%s\nmissing_topic=%s\n' "$topic" "$missing_topic" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "ORDER-001-lock-unlock" ]]; then
    printf 'topic=%s\nqueue=broker-a:0\njava_client_id=java-order-owner\nrust_client_id=rust-order-contender\n' "$topic" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "REBAL-001-average-10q-3c" ]]; then
    printf 'topic=%s\nqueue_count=%s\nconsumer_ids=%s\n' "$topic" "$queue_count" "$consumer_ids" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "TX-001-commit" || "$scenario_id" == "TX-002-rollback" ]]; then
    printf 'consumer_group=%s\n' "$consumer_group" >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "REQ-001-request-reply-success" ]]; then
    printf 'consumer_group=%s\nrequest_topics=%s,%s\nrequest_timeout_ms=15000\n' "$consumer_group" "$topic" "$rust_request_topic" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P15-V03" ]]; then
    printf 'consumer_group=%s\nrequest_topics=%s,%s\nno_responder_topics=%s,%s\nlate_reply_topics=%s,%s\nrequest_timeout_ms=%s\nno_reply_hold_ms=%s\nlate_reply_delay_ms=%s\nlate_reply_hold_ms=%s\n' \
        "$consumer_group" "$topic" "${p15_topics[0]}" "${p15_topics[1]}" "${p15_topics[2]}" \
        "${p15_topics[3]}" "${p15_topics[4]}" "$p15_timeout_ms" "$p15_no_reply_hold_ms" \
        "$p15_late_reply_delay_ms" "$p15_late_reply_hold_ms" >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P10-V03" ]]; then
    printf 'consumer_group=%s\nassignment=%s\nretry_topic=%s\ndlq_topic=%s\nmax_reconsume_times=%s\nexpected_listener_reconsume_counts=0,1\nexpected_dlq_reconsume_times=%s\ninitial_messages=p10-success,p10-partial-ack,p10-partial-retry\nfailure_message=p10-failure-dlq\nfixed_queue=broker-a:0\n' \
        "$consumer_group" "test-only-fixed" "$p10_retry_topic" "$p10_dlq_topic" "$p10_retry_max_times" "$((p10_retry_max_times + 1))" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P11-V03" ]]; then
    printf 'consumer_group=%s\nqueues_per_broker=4\nexpected_queue_count=8\nconvergence_timeout_seconds=90\ninternal_retry_topics=excluded-from-scenario-topic-ownership\ntransient_redelivery=allowed\n' \
        "$consumer_group" >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P11-V04" ]]; then
    printf 'consumer_group=%s\ntopic_queues=broker-a:0,broker-b:0\njava_producer_body=p11-java-to-rust-retry\nrust_producer_body=p11-rust-to-java-retry\nlistener_reconsume_counts=0,1\nmax_reconsume_times=1\nshutdown_cycles_per_client=3\noffset_queries=Java-and-Rust-per-broker\nretry_topic=%%RETRY%%%s\n' \
        "$consumer_group" "$consumer_group" >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P12-V03" ]]; then
    printf 'consumer_group=%s\ntopic_queues=broker-a:0,broker-b:0\njava_client_id_suffix=z-p12-java\nrust_client_id_suffix=a-p12-rust\nmessages_per_queue=p12-slow,p12-next\nunlock_delay_ms=1000\nslow_callback=held-until-rust-commits\nshutdown_lock_probe=broker-a:0,broker-b:0\n' \
        "$consumer_group" >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P14-V03" ]]; then
    printf 'consumer_group=%s\ntopic_queues_per_broker=1\nbroker_transaction_check_interval_ms=2000\ncheck_callback_timeout_seconds=25\n' \
        "$consumer_group" >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P16-V02" ]]; then
    auth_broker_base_image=eclipse-temurin:8-jdk@sha256:a0c061a98b00f4ec24a33296dd491efabf5ad948607f60ef382a295024279068
    auth_broker_image_id="$(docker image inspect "$auth_broker_image" --format '{{.Id}}')"
    printf 'name_server_image=apache/rocketmq:5.3.4@sha256:d57ee4ebbd9d7bb6e185775bde41c128e98f185b25bf0b05c77d4e6c010572b8\nauth_broker_image=%s\nauth_broker_image_id=%s\nauth_broker_platform=linux/amd64\nauth_broker_base_image=%s\nserver_distribution=Apache RocketMQ 5.3.4 binary release\nserver_distribution_sha512=88508d5480421ce1582e56aa2e3b6e28ea6b88418cf618fd79a78f996a7ae853ee1ea98780377235ed396ce2bc82b4f3d88a838c72076760466702d881bfc6ae\n' \
        "$auth_broker_image" "$auth_broker_image_id" "$auth_broker_base_image" >>"$evidence_dir/metadata.txt"
    printf 'checks=signed-request-accepted,wrong-secret-rejected,tampered-body-rejected,trusted-ca-accepted,wrong-hostname-accepted-with-trusted-ca,untrusted-ca-rejected,missing-client-certificate-rejected\nmutual_tls=true\ninsecure_tls_bypass=false\ncredentials=per-run-random-synthetic-and-redacted\n' \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P17-V02" ]]; then
    printf 'topic=%s\nquery_keys=duplicate-key,bounded-time-range\nbrokers=broker-a,broker-b\nproxy_listeners=127.0.0.1:50911,127.0.0.1:50921\ndirect_broker_ports=51911,51921\npartial_failure=broker-b\nterminal_failure=both-brokers\n' \
        "$topic" >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "PROD-005-send-timeout" ]]; then
    printf 'topic=%s\nsend_timeout_ms=%s\nproxy_response_delay_ms=600\nexpected_committed_messages=2\n' "$topic" "$timeout_ms" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P3-V04" ]]; then
    printf 'topic=%s\nraw_route=NameServer\nTLS_endpoint=local-disposable Broker proxy\n' "$topic" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P7-V04" ]]; then
    printf 'topic=%s\nbrokers=127.0.0.1:50911,127.0.0.1:50921\nstress_messages=%s\nstress_max_inflight=32\nstress_seed=1592594996\nstress_deadline_ms=120000\ncycles=3\nsends_per_cycle=20\n' \
        "$topic" "$message_count" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P5-V02-producer" ]]; then
    printf 'topic=%s\nreply_consumer_group=compat_p5_reply_group\nreply_client_id=compat-p5-client\nmethods=send-v1,send-v2,batch,reply-v1,reply-v2,end-transaction\n' "$topic" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P5-V02-consumer" ]]; then
    printf 'topic=%s\nconsumer_group=%s\njava_client_id=compat-p5-java-consumer\nrust_client_id=compat-p5-rust-consumer\nmethods=pull,lite-pull,query-offset,update-offset,consumer-list,send-back,lock,unlock\n' "$topic" "$consumer_group" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P5-V02-extensions" ]]; then
    printf 'topic=%s\nlite_topic=%%LMQ%%%s%%entries\nconsumer_group_prefix=%s\njava_group_prefix=%s_java\nrust_group_prefix=%s_rust\nmethods=heartbeat,heartbeat-v2,unregister,assignment,request-mode,lite-subscription,pop,pop-lite,ack,batch-ack,change-invisible-time\n' \
        "$topic" "$topic" "$consumer_group" "$consumer_group" "$consumer_group" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P13-V02" ]]; then
    printf 'topic=%s\nqueues=broker-a:0,broker-a:1\njava_group_prefix=%s_java\nrust_group_prefix=%s_rust\nchecks=pop-delivery,expiry-redelivery,ack,change-invisibility,replacement-receipt,mixed-queue-mapping,malformed-extra-info-rejected\n' \
        "$topic" "$consumer_group" "$consumer_group" >>"$evidence_dir/metadata.txt"
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
    python3 - "$repo_root/integration-tests/compose.yaml" "$repo_root/integration-tests/config/broker.conf" "$repo_root/integration-tests/config/broker-b.conf" "$repo_root/integration-tests/config/broker-tx.conf" "$repo_root/integration-tests/config/broker-host.conf" "$repo_root/integration-tests/config/broker-a-host.conf" "$repo_root/integration-tests/config/broker-b-host.conf" "$repo_root/integration-tests/config/broker-auth.conf" "$repo_root/integration-tests/build-official-rocketmq-5.3.4-amd64.sh" "$repo_root/integration-tests/docker/rocketmq-5.3.4-amd64.Dockerfile" "$repo_root/integration-tests/docker/rocketmq-5.3.4-entrypoint.sh" <<'PY'
import hashlib
import sys
from pathlib import Path

for filename in sys.argv[1:]:
    path = Path(filename)
    print(f"sha256 {hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}")
PY
} >"$evidence_dir/toolchain.txt" 2>&1
if [[ "$scenario_id" == "P16-V02" ]]; then
    docker image inspect "$auth_broker_image" \
        --format 'P16 auth Broker image {{.Id}} / {{.Os}}/{{.Architecture}}' \
        >>"$evidence_dir/toolchain.txt" 2>&1
fi
if [[ "$scenario_id" == "P17-V02" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=two-broker COMPOSE_PROJECT_NAME=$project_name bash tests/differential/run.sh --scenario P17-V02
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-a-host ./mqadmin updateTopic -b 127.0.0.1:50911 -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-b-host ./mqadmin updateTopic -b 127.0.0.1:50921 -t $topic -r 1 -w 1 -n namesrv:9876
python3 tests/differential/query_failure_proxy.py --label broker-a --listen-port 50911 --upstream 127.0.0.1:51911 --mode-file <evidence>/proxy-a-mode.txt --ready-file <evidence>/proxy-a-ready.txt --log-file <evidence>/proxy-a-events.tsv
python3 tests/differential/query_failure_proxy.py --label broker-b --listen-port 50921 --upstream 127.0.0.1:51921 --mode-file <evidence>/proxy-b-mode.txt --ready-file <evidence>/proxy-b-ready.txt --log-file <evidence>/proxy-b-events.tsv
mvn -q -f compat/java-reference/pom.xml -Dexec.args="seed $namesrv_addr $topic" exec:java@live-p17-admin-query-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="query $namesrv_addr $topic p17-duplicate-key 0 9223372036854775807" exec:java@live-p17-admin-query-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- p17-query $namesrv_addr $topic p17-duplicate-key 0 9223372036854775807
python3 tests/differential/check_p17_admin_query.py prepare <evidence>
# Repeat the Java/Rust query commands with the begin/end values from bounded-range.txt.
# Set proxy-b-mode.txt to error for the partial-failure probes, then proxy-a-mode.txt to error for terminal mapping.
python3 tests/differential/check_p17_admin_query.py verify <evidence>
EOF
elif [[ "$scenario_id" == "P16-V02" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
bash integration-tests/build-official-rocketmq-5.3.4-amd64.sh
ROCKETMQ_AUTH_BROKER_IMAGE=$auth_broker_image ROCKETMQ_AUTH_BROKER_PLATFORM=linux/amd64 ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=tls-auth COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
python3 tests/differential/acl_tampering_proxy.py --upstream $broker_addr --ready-file <evidence>/acl-proxy-ready.txt --log-file <evidence>/acl-proxy-events.tsv
python3 tests/differential/tls_broker_proxy.py --upstream $broker_addr --cert <evidence>/tls/server.pem --key <evidence>/tls/server.key --client-ca <evidence>/tls/ca.pem --ready-file <evidence>/tls-proxy-ready.txt --log-file <evidence>/tls-proxy-events.tsv
mvn -q -f compat/java-reference/pom.xml -Dexec.args="auth-{valid,wrong-secret,tampered} <broker-or-acl-proxy>" exec:java@live-p16-security-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- p16-auth <broker-or-acl-proxy> {valid,wrong-secret,tampered}
mvn -q -f compat/java-reference/pom.xml -Dexec.args="tls-{trusted,wrong-host,untrusted,missing-client} <tls-proxy> <ca> <client-cert|-> <client-key|->" exec:java@live-p16-security-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- p16-tls <tls-proxy> {trusted,wrong-host,untrusted,missing-client} <ca> <client-cert|-> <client-key|->
EOF
elif [[ "$scenario_id" == "ORDER-001-lock-unlock" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateSubGroup -c RocketMQLocalTest -g $producer_group -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="lock $broker_addr $producer_group java-initial $topic broker-a 0" exec:java@live-consumer-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="unlock $broker_addr $producer_group java-initial $topic broker-a 0" exec:java@live-consumer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- lock $broker_addr $producer_group rust-initial $topic broker-a 0
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- unlock $broker_addr $producer_group rust-initial $topic broker-a 0
mvn -q -f compat/java-reference/pom.xml -Dexec.args="lock $broker_addr $producer_group java-order-owner $topic broker-a 0" exec:java@live-consumer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- lock $broker_addr $producer_group rust-order-contender $topic broker-a 0
mvn -q -f compat/java-reference/pom.xml -Dexec.args="unlock $broker_addr $producer_group java-order-owner $topic broker-a 0" exec:java@live-consumer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- lock $broker_addr $producer_group rust-order-contender $topic broker-a 0
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- unlock $broker_addr $producer_group rust-order-contender $topic broker-a 0
EOF
elif [[ "$scenario_id" == "REBAL-001-average-10q-3c" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 5 -w 5 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic $consumer_ids" exec:java@live-rebalance-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- rebalance $namesrv_addr $topic $consumer_ids
EOF
elif [[ "$scenario_id" == "TX-001-commit" || "$scenario_id" == "TX-002-rollback" ]]; then
    transaction_decision="commit"
    if [[ "$scenario_id" == "TX-002-rollback" ]]; then
        transaction_decision="rollback"
    fi
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec $broker_service ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec $broker_service ./mqadmin updateSubGroup -c RocketMQLocalTest -g $consumer_group -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic $producer_group $transaction_decision ${key_prefix}_java $body" exec:java@live-transaction-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- transaction $namesrv_addr $topic $producer_group $transaction_decision ${key_prefix}_rust $body
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- max-offset $broker_addr $topic 0
mvn -q -f compat/java-reference/pom.xml -Dexec.args="pull $broker_addr $consumer_group $topic 0 0" exec:java@live-consumer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- pull $broker_addr $consumer_group $topic 0 0
EOF
elif [[ "$scenario_id" == "P7-V04" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=two-broker-host COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-a-host ./mqadmin updateTopic -b 127.0.0.1:50911 -t $topic -r 4 -w 4 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-b-host ./mqadmin updateTopic -b 127.0.0.1:50921 -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic $message_count 32 1592594996 0" exec:java@live-producer-stress-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- p7-stress $namesrv_addr $topic $message_count 32 1592594996 $message_count
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic" exec:java@live-producer-lifecycle-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- p7-lifecycle $namesrv_addr $topic
mvn -q -f compat/java-reference/pom.xml -Dexec.args="delete-topic 127.0.0.1:50911 $topic" exec:java@live-producer-lifecycle-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="delete-topic 127.0.0.1:50921 $topic" exec:java@live-producer-lifecycle-probe
EOF
elif [[ "$scenario_id" == "P7-V02" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=host-routed COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic" exec:java@live-high-level-producer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- p7-producer $namesrv_addr $topic
EOF
elif [[ "$scenario_id" == "P6-V04" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=host-routed COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateSubGroup -c RocketMQLocalTest -g ${consumer_group}_java -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateSubGroup -c RocketMQLocalTest -g ${consumer_group}_rust -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic compat_p6_producer_a compat_p6_producer_b $consumer_group" exec:java@live-shared-runtime-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- p6-shared-runtime $namesrv_addr $topic compat_p6_producer_a compat_p6_producer_b $consumer_group
EOF
elif [[ "$scenario_id" == "P5-V02-extensions" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile basic exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile basic exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t %LMQ%${topic}%entries -r 1 -w 1 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="prepare $broker_addr $topic ${consumer_group}_java" exec:java@live-p5-extensions-probe
target/debug/live_client p5-extension-prepare $broker_addr $topic ${consumer_group}_rust
mvn -q -f compat/java-reference/pom.xml -Dexec.args="send $broker_addr $topic p5-extension-seed" exec:java@live-producer-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="send $broker_addr %LMQ%${topic}%entries p5-extension-lite" exec:java@live-producer-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="consume $broker_addr $topic ${consumer_group}_java" exec:java@live-p5-extensions-probe
target/debug/live_client p5-extension-consume $broker_addr $topic ${consumer_group}_rust
EOF
elif [[ "$scenario_id" == "P13-V02" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=two-broker COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="prepare $broker_addr $topic java" exec:java@live-p13-pop-probe
target/debug/live_client p13-pop-prepare $broker_addr $topic rust
mvn -q -f compat/java-reference/pom.xml -Dexec.args="seed $broker_addr $namesrv_addr $topic java" exec:java@live-p13-pop-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="consume $broker_addr $topic java" exec:java@live-p13-pop-probe
target/debug/live_client p13-pop-consume $broker_addr $topic rust
EOF
elif [[ "$scenario_id" == "P11-V04" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=two-broker-host COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-a-host ./mqadmin updateTopic -b 127.0.0.1:50911 -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-b-host ./mqadmin updateTopic -b 127.0.0.1:50921 -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-a-host ./mqadmin updateSubGroup -b 127.0.0.1:50911 -g $consumer_group -r 1 -q 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-b-host ./mqadmin updateSubGroup -b 127.0.0.1:50921 -g $consumer_group -r 1 -q 1 -n namesrv:9876
target/debug/live_client p11-push $namesrv_addr $topic $consumer_group p11-java-to-rust-retry <rust-push-ready>
mvn -q -f compat/java-reference/pom.xml -Dexec.args="send $namesrv_addr $topic broker-a 0 p11-java-to-rust-retry" exec:java@live-push-interop-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="query 127.0.0.1:50911 $consumer_group $topic 0" exec:java@live-consumer-probe
target/debug/live_client offset-query-named 127.0.0.1:50911 broker-a $consumer_group $topic 0
mvn -q -f compat/java-reference/pom.xml -Dexec.args="consume $namesrv_addr $topic $consumer_group p11-rust-to-java-retry <java-push-ready>" exec:java@live-push-interop-probe
target/debug/live_client p11-producer $namesrv_addr $topic compat_p11_rust_producer p11-rust-to-java-retry
mvn -q -f compat/java-reference/pom.xml -Dexec.args="query 127.0.0.1:50921 $consumer_group $topic 0" exec:java@live-consumer-probe
target/debug/live_client offset-query-named 127.0.0.1:50921 broker-b $consumer_group $topic 0
python3 tests/differential/check_p11_push.py <evidence-dir> $topic p11-java-to-rust-retry p11-rust-to-java-retry
Three public Push consumer start/shutdown cycles per language must quiesce before scenario cleanup.
EOF
elif [[ "$scenario_id" == "P11-V03" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=two-broker-host COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-a-host ./mqadmin updateTopic -b 127.0.0.1:50911 -t $topic -r 4 -w 4 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-b-host ./mqadmin updateTopic -b 127.0.0.1:50921 -t $topic -r 4 -w 4 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-a-host ./mqadmin updateSubGroup -b 127.0.0.1:50911 -g $consumer_group -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-b-host ./mqadmin updateSubGroup -b 127.0.0.1:50921 -g $consumer_group -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic $consumer_group <java-ownership-file> <java-stop-file>" exec:java@live-mixed-group-probe
target/debug/live_client p11-mixed-group $namesrv_addr $topic $consumer_group <rust-ownership-file> <rust-stop-file> <rust-exited-file>
python3 tests/differential/check_mixed_ownership.py java-only $topic <java-ownership-file> <rust-ownership-file> <java-exited-file> 90
python3 tests/differential/check_mixed_ownership.py joined $topic <java-ownership-file> <rust-ownership-file> <java-exited-file> 90
python3 tests/differential/check_mixed_ownership.py rust-only $topic <java-ownership-file> <rust-ownership-file> <java-exited-file> 90
The host-routed two-Broker Compose profile is used because the clients run on the host and need reachable advertised Broker addresses. The ownership checker requires four stable samples within 90 seconds at each membership transition; transient redelivery is allowed while assignments converge.
EOF
elif [[ "$scenario_id" == "P12-V03" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=two-broker-host COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-a-host ./mqadmin updateTopic -b 127.0.0.1:50911 -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-b-host ./mqadmin updateTopic -b 127.0.0.1:50921 -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-a-host ./mqadmin updateSubGroup -b 127.0.0.1:50911 -g $consumer_group -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-b-host ./mqadmin updateSubGroup -b 127.0.0.1:50921 -g $consumer_group -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="update 127.0.0.1:50911 $consumer_group $topic 0 0" exec:java@live-consumer-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="update 127.0.0.1:50921 $consumer_group $topic 0 0" exec:java@live-consumer-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="seed $namesrv_addr $topic broker-a 0 p12-slow-a" exec:java@live-orderly-interop-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="seed $namesrv_addr $topic broker-a 0 p12-next-a" exec:java@live-orderly-interop-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="seed $namesrv_addr $topic broker-b 0 p12-slow-b" exec:java@live-orderly-interop-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="seed $namesrv_addr $topic broker-b 0 p12-next-b" exec:java@live-orderly-interop-probe
JAVA_TOOL_OPTIONS=-Drocketmq.client.unlockDelayTimeMills=1000 mvn -q -f compat/java-reference/pom.xml -Dexec.args="consumer $namesrv_addr $topic $consumer_group <java-ownership> <java-ready> <java-stop> <marker-dir> <release-file>" exec:java@live-orderly-interop-probe
env rocketmq.client.unlockDelayTimeMills=1000 target/debug/live_client p12-orderly $namesrv_addr $topic $consumer_group <rust-ownership> <rust-ready> <rust-stop> <rust-exited> <marker-dir>
target/debug/live_client offset-query-named 127.0.0.1:50911 broker-a $consumer_group $topic 0
target/debug/live_client offset-query-named 127.0.0.1:50921 broker-b $consumer_group $topic 0
python3 tests/differential/check_p12_orderly.py held <marker-dir> <java-ownership> $topic 30
target/debug/live_client offset-query-named 127.0.0.1:50911 broker-a $consumer_group $topic 0
target/debug/live_client offset-query-named 127.0.0.1:50921 broker-b $consumer_group $topic 0
python3 tests/differential/check_p12_orderly.py handoff <marker-dir> <rust-ownership> $topic 90
After Rust advances its queue to offset 2, stop Rust while Java's older callbacks remain gated, release those callbacks, stop Java, then query both offsets and probe/release both locks with a fresh client ID. This leaves no live offset writer to mask a stale Java completion. Java and Rust use an explicit 1-second orderly unlock-delay property; the Broker remains the unmodified 5.3.4 image.
EOF
elif [[ "$scenario_id" == "P14-V03" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=two-broker-host ROCKETMQ_BROKER_A_HOST_CONFIG=./config/broker-a-transaction-host.conf ROCKETMQ_BROKER_B_HOST_CONFIG=./config/broker-b-transaction-host.conf COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-a-host ./mqadmin updateTopic -b 127.0.0.1:50911 -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-b-host ./mqadmin updateTopic -b 127.0.0.1:50921 -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-a-host ./mqadmin updateSubGroup -b 127.0.0.1:50911 -g $consumer_group -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-b-host ./mqadmin updateSubGroup -b 127.0.0.1:50921 -g $consumer_group -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="p14 $namesrv_addr $topic $producer_group commit p14-java-commit-key p14-java-commit" exec:java@live-transaction-probe
target/debug/live_client p14-pull <java-result-broker-address> <java-result-broker-name> $consumer_group $topic 0 0
target/debug/live_client p14-transaction $namesrv_addr $topic $producer_group commit p14-rust-commit-key p14-rust-commit
mvn -q -f compat/java-reference/pom.xml -Dexec.args="pull <rust-result-broker-address> $consumer_group $topic 0 0" exec:java@live-consumer-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="p14 $namesrv_addr $topic $producer_group rollback p14-java-rollback-key p14-java-rollback" exec:java@live-transaction-probe
target/debug/live_client p14-pull <java-rollback-broker-address> <java-rollback-broker-name> $consumer_group $topic 0 0
target/debug/live_client p14-transaction $namesrv_addr $topic $producer_group rollback p14-rust-rollback-key p14-rust-rollback
mvn -q -f compat/java-reference/pom.xml -Dexec.args="pull <rust-rollback-broker-address> $consumer_group $topic 0 0" exec:java@live-consumer-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="p14 $namesrv_addr $topic $producer_group unknown p14-java-unknown-key p14-java-unknown" exec:java@live-transaction-probe
target/debug/live_client p14-pull <java-unknown-broker-address> <java-unknown-broker-name> $consumer_group $topic 0 0
target/debug/live_client p14-transaction $namesrv_addr $topic $producer_group unknown p14-rust-unknown-key p14-rust-unknown
mvn -q -f compat/java-reference/pom.xml -Dexec.args="pull <rust-unknown-broker-address> $consumer_group $topic 0 0" exec:java@live-consumer-probe
The isolated Brokers use transactionCheckInterval=2000 ms. Unknown callbacks must receive an actual Broker check and commit within the 25-second probe timeout; each message is read directly from the Broker and queue named in its transaction send result.
EOF
elif [[ "$scenario_id" == "P10-V03" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=two-broker-host COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-a-host ./mqadmin updateTopic -b 127.0.0.1:50911 -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-b-host ./mqadmin updateTopic -b 127.0.0.1:50921 -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-a-host ./mqadmin updateSubGroup -b 127.0.0.1:50911 -g $consumer_group -r $p10_retry_max_times -q 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-b-host ./mqadmin updateSubGroup -b 127.0.0.1:50921 -g $consumer_group -r $p10_retry_max_times -q 1 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="seed-p10 $namesrv_addr $topic initial" exec:java@live-high-level-producer-probe
target/debug/live_client p10-push $namesrv_addr $topic $consumer_group <evidence>/rust-ready <evidence>/seed-failure
mvn -q -f compat/java-reference/pom.xml -Dexec.args="seed-p10 $namesrv_addr $topic failure" exec:java@live-high-level-producer-probe
The manifest profile is two-broker; the host-routed two-Broker Compose profile publishes NameServer-advertised Broker endpoints to the host Java/Rust clients. The runner releases the failure seed gate after Java sends the failure message; Rust records callbacks and verifies the retry-topic/DLQ transition before shutdown. Broker 5.3.4 stores the terminal DLQ copy with reconsumeTimes=retryMaxTimes+1 after the final send-back.
EOF
elif [[ "$scenario_id" == "P9-V03" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=host-routed COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateSubGroup -c RocketMQLocalTest -g ${consumer_group}_java -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateSubGroup -c RocketMQLocalTest -g ${consumer_group}_rust -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="seed $namesrv_addr $topic" exec:java@live-pull-consumer-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="probe $namesrv_addr $topic ${consumer_group}_java" exec:java@live-pull-consumer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- p9-pull $namesrv_addr $topic ${consumer_group}_rust
EOF
elif [[ "$scenario_id" == "REQ-001-request-reply-success" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=host-routed COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateTopic -c RocketMQLocalTest -t $rust_request_topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateSubGroup -c RocketMQLocalTest -g $consumer_group -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateSubGroup -c RocketMQLocalTest -g ${consumer_group}_java -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateSubGroup -c RocketMQLocalTest -g ${consumer_group}_rust -n namesrv:9876
Rust responder / Java requester, then Java responder / Rust requester; each responder emits READY before its paired request.
EOF
elif [[ "$scenario_id" == "P7-V03" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=two-broker-host ROCKETMQ_BROKER_A_PUBLISHED_HOST_PORT=51911 ROCKETMQ_BROKER_B_PUBLISHED_HOST_PORT=51921 COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-a-host ./mqadmin updateTopic -b 127.0.0.1:50911 -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker-host exec broker-b-host ./mqadmin updateTopic -b 127.0.0.1:50921 -t $topic -r 1 -w 1 -n namesrv:9876
python3 tests/differential/delayed_response_proxy.py --upstream 127.0.0.1:51911 --listen-port 50911 --delay-ms $p7_response_delay_ms --delay-flag <evidence>/broker-a-proxy.delay-flag --delay-send-flag <evidence>/broker-a-proxy.delay-send-flag --reject-send-flag <evidence>/broker-a-proxy.reject-flag --reject-response-code 16 --ready-file <evidence>/broker-a-proxy.ready --log-file <evidence>/broker-a-proxy.tsv
python3 tests/differential/delayed_response_proxy.py --upstream 127.0.0.1:51921 --listen-port 50921 --delay-ms $p7_response_delay_ms --delay-flag <evidence>/broker-b-proxy.delay-flag --delay-send-flag <evidence>/broker-b-proxy.delay-send-flag --reject-send-flag <evidence>/broker-b-proxy.reject-flag --reject-response-code 16 --ready-file <evidence>/broker-b-proxy.ready --log-file <evidence>/broker-b-proxy.tsv
The runner gates each client after route lookup, injects one NO_PERMISSION response per Broker without forwarding those two send requests, then forwards the third attempt; it records all attempts and keeps the proxies alive through topic deletion.
retry_timeout_ms=$p7_retry_timeout_ms
deadline_timeout_ms=$p7_deadline_timeout_ms
retry_response_code=16
EOF
elif [[ "$scenario_id" == "P15-V03" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=two-broker-host COMPOSE_PROJECT_NAME=$project_name bash tests/differential/run.sh --scenario P15-V03
The runner provisions the isolated two-Broker host-routed profile, creates six one-queue topics on both Brokers, creates the two responder groups, runs bidirectional success plus both no-responder and late-reply language directions, checks timeout/callback/correlation state, and tears down only this Compose project.
request_timeout_ms=$p15_timeout_ms
no_reply_hold_ms=$p15_no_reply_hold_ms
late_reply_delay_ms=$p15_late_reply_delay_ms
late_reply_hold_ms=$p15_late_reply_hold_ms
EOF
elif [[ "$scenario_id" == "PROD-005-send-timeout" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=two-broker COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 1 -w 1 -n namesrv:9876
python3 tests/differential/delayed_response_proxy.py --upstream $broker_addr --delay-ms 600 --delay-flag <evidence>/delay-next-response --ready-file <evidence>/proxy-ready.txt --log-file <evidence>/proxy.tsv
mvn -q -f compat/java-reference/pom.xml -Dexec.args="timeout-send <proxy-address> $topic compat_timeout_java timeout-java-body $timeout_ms <java-go-file>" exec:java@live-producer-probe
target/debug/live_client timeout-send <proxy-address> $topic compat_timeout_rust timeout-rust-body $timeout_ms <rust-go-file>
mvn -q -f compat/java-reference/pom.xml -Dexec.args="pull $broker_addr compat_timeout_readback $topic 0 0" exec:java@live-consumer-probe
target/debug/live_client pull $broker_addr compat_timeout_readback $topic 0 0
target/debug/live_client max-offset $broker_addr $topic 0
EOF
elif [[ "$scenario_id" == "P5-V02-producer" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateSubGroup -c RocketMQLocalTest -g compat_p5_reply_group -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$broker_addr $topic" exec:java@live-p5-producer-probe
target/debug/live_client p5-producer $broker_addr $topic compat_p5_producer <java-max-offset-from-java-p5-producer.tsv>
EOF
elif [[ "$scenario_id" == "P5-V02-consumer" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=two-broker COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile two-broker exec broker ./mqadmin updateSubGroup -c RocketMQLocalTest -g $consumer_group -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="send $broker_addr $topic p5-java-seed" exec:java@live-producer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- send $broker_addr $topic p5-rust-seed
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$broker_addr $consumer_group $topic p5-java-seed" exec:java@live-p5-consumer-probe
target/debug/live_client p5-consumer $broker_addr $consumer_group $topic p5-rust-seed
EOF
elif [[ "$scenario_id" == "P3-V04" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=tls COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
mvn -q -f compat/java-reference/pom.xml -Dexec.args="route $namesrv_addr $topic" exec:java@live-p3-remoting-probe
target/debug/live_client p3-raw-route $namesrv_addr $topic
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -config <evidence>/ca.cnf -keyout <evidence>/ca.key -out <evidence>/ca.pem
openssl req -new -newkey rsa:2048 -nodes -config <evidence>/server-req.cnf -keyout <evidence>/server.key -out <evidence>/server.csr
openssl x509 -req -in <evidence>/server.csr -CA <evidence>/ca.pem -CAkey <evidence>/ca.key -set_serial 1 -days 2 -sha256 -extfile <evidence>/server-ext.cnf -extensions server_cert -out <evidence>/server.pem
python3 tests/differential/tls_broker_proxy.py --upstream $broker_addr --cert <evidence>/server.pem --key <evidence>/server.key --ready-file <evidence>/tls-proxy-ready.txt --log-file <evidence>/tls-proxy.tsv
Java and Rust TLS calls use the trusted CA, then independently test a trusted wrong-host certificate and an untrusted CA; both accept the trusted wrong-host certificate without endpoint identification and reject the untrusted CA; all clients shut down before the next probe.
EOF
elif [[ "$scenario_id" == "P0-V03" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario ROUTE-001-get-existing-topic
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario HEARTBEAT-001
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario PROD-001-sync-send
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario PROD-002-async-send
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario PROD-003-oneway-send
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario PROD-004-batch-send
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario CONS-001-query-offset
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario CONS-002-pull-found
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario CONS-003-pull-no-new-msg
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario CONS-004-update-offset
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=two-broker bash tests/differential/run.sh --scenario ORDER-001-lock-unlock
EOF
elif [[ "$scenario_id" == "HEARTBEAT-001" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$broker_addr $producer_group" exec:java@live-heartbeat-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- heartbeat $broker_addr $producer_group
EOF
elif [[ "$scenario_id" == "CONS-001-query-offset" || "$scenario_id" == "CONS-004-update-offset" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateSubGroup -c RocketMQLocalTest -g $producer_group -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="update $broker_addr $producer_group $topic $queue_id $java_offset" exec:java@live-consumer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- offset-query $broker_addr $producer_group $topic $queue_id
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- offset-update $broker_addr $producer_group $topic $queue_id $rust_offset
mvn -q -f compat/java-reference/pom.xml -Dexec.args="query $broker_addr $producer_group $topic $queue_id" exec:java@live-consumer-probe
EOF
elif [[ "$scenario_id" == "CONS-002-pull-found" || "$scenario_id" == "CONS-003-pull-no-new-msg" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateSubGroup -c RocketMQLocalTest -g $producer_group -n namesrv:9876
EOF
    if [[ "$scenario_id" == "CONS-002-pull-found" ]]; then
        cat >>"$evidence_dir/commands.txt" <<EOF
mvn -q -f compat/java-reference/pom.xml -Dexec.args="send $broker_addr $topic $body" exec:java@live-producer-probe
EOF
    fi
    cat >>"$evidence_dir/commands.txt" <<EOF
mvn -q -f compat/java-reference/pom.xml -Dexec.args="pull $broker_addr $producer_group $topic $queue_id $pull_offset" exec:java@live-consumer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- pull $broker_addr $producer_group $topic $queue_id $pull_offset
EOF
elif [[ "$scenario_id" == "P5-V02-route" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic" exec:java@live-route-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- route $namesrv_addr $topic
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $missing_topic" exec:java@live-route-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- route $namesrv_addr $missing_topic
EOF
elif [[ "$scenario_id" == "PROD-001-sync-send" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="send $broker_addr $topic $body" exec:java@live-producer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- view $broker_addr $topic '<java-offset-message-id>'
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- send $broker_addr $topic $body
mvn -q -f compat/java-reference/pom.xml -Dexec.args="view $broker_addr $topic <rust-offset-message-id>" exec:java@live-producer-probe
EOF
elif [[ "$scenario_id" == "PROD-002-async-send" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="async-send $broker_addr $topic $body" exec:java@live-producer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- view $broker_addr $topic '<java-offset-message-id>'
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- async-send $broker_addr $topic $body
mvn -q -f compat/java-reference/pom.xml -Dexec.args="view $broker_addr $topic <rust-offset-message-id>" exec:java@live-producer-probe
EOF
elif [[ "$scenario_id" == "PROD-003-oneway-send" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="oneway-send $broker_addr $topic $body $java_oneway_key $java_expected_max_offset" exec:java@live-producer-probe
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin queryMsgByKey -t $topic -k $java_oneway_key -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin queryMsgById -t $topic -i <java-unique-key> -n namesrv:9876
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- view $broker_addr $topic <java-offset-id>
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- oneway-send $broker_addr $topic $body $rust_oneway_key $rust_expected_max_offset
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin queryMsgByKey -t $topic -k $rust_oneway_key -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin queryMsgById -t $topic -i <rust-unique-key> -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="view $broker_addr $topic <rust-offset-id>" exec:java@live-producer-probe
EOF
elif [[ "$scenario_id" == "PROD-004-batch-send" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="batch-send $broker_addr $topic $body $message_count" exec:java@live-producer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- view-batch $broker_addr $topic '<java-offset-message-ids>'
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- batch-send $broker_addr $topic $body $message_count
mvn -q -f compat/java-reference/pom.xml -Dexec.args="view-batch $broker_addr $topic <rust-offset-message-ids>" exec:java@live-producer-probe
EOF
else
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic" exec:java@live-route-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- route $namesrv_addr $topic
EOF
fi
topic_created=false
topic_cleanup_list=("$topic")
if [[ "$scenario_id" == "P11-V04" ]]; then
    topic_cleanup_list+=("%RETRY%${consumer_group}")
fi
if [[ "$scenario_id" == "P5-V02-extensions" ]]; then
    topic_cleanup_list+=("%LMQ%${topic}%entries")
fi
if [[ "$scenario_id" == "REQ-001-request-reply-success" ]]; then
    topic_cleanup_list+=("$rust_request_topic")
fi
if [[ "$scenario_id" == "P15-V03" ]]; then
    topic_cleanup_list+=("${p15_topics[@]}")
fi
responder_pid=""
timeout_proxy_pid=""
tls_proxy_pid=""
timeout_probe_pid=""
p7_proxy_pids=()
p7_probe_pid=""
p10_probe_pid=""
p11_java_pid=""
p11_rust_pid=""
p12_java_pid=""
p12_rust_pid=""
group_created=false
p16_proxy_pids=()
p17_proxy_pids=()
p16_auth_config_path=""
cleanup() {
    status=$?
    trap - EXIT
    set +e
    if [[ -n "$responder_pid" ]] && kill -0 "$responder_pid" 2>/dev/null; then
        kill "$responder_pid" 2>/dev/null
        wait "$responder_pid" 2>/dev/null
    fi
    if [[ -n "$timeout_probe_pid" ]] && kill -0 "$timeout_probe_pid" 2>/dev/null; then
        kill "$timeout_probe_pid" 2>/dev/null
        wait "$timeout_probe_pid" 2>/dev/null
    fi
    if [[ -n "$timeout_proxy_pid" ]] && kill -0 "$timeout_proxy_pid" 2>/dev/null; then
        kill "$timeout_proxy_pid" 2>/dev/null
        wait "$timeout_proxy_pid" 2>/dev/null
    fi
    if [[ -n "$tls_proxy_pid" ]] && kill -0 "$tls_proxy_pid" 2>/dev/null; then
        kill "$tls_proxy_pid" 2>/dev/null
        wait "$tls_proxy_pid" 2>/dev/null
    fi
    if [[ -n "$p7_probe_pid" ]] && kill -0 "$p7_probe_pid" 2>/dev/null; then
        kill "$p7_probe_pid" 2>/dev/null
        wait "$p7_probe_pid" 2>/dev/null
    fi
    if [[ -n "$p10_probe_pid" ]] && kill -0 "$p10_probe_pid" 2>/dev/null; then
        kill "$p10_probe_pid" 2>/dev/null
        wait "$p10_probe_pid" 2>/dev/null
    fi
    for p11_probe_pid in "$p11_java_pid" "$p11_rust_pid"; do
        if [[ -n "$p11_probe_pid" ]] && kill -0 "$p11_probe_pid" 2>/dev/null; then
            kill "$p11_probe_pid" 2>/dev/null
            wait "$p11_probe_pid" 2>/dev/null
        fi
    done
    for p12_probe_pid in "$p12_java_pid" "$p12_rust_pid"; do
        if [[ -n "$p12_probe_pid" ]] && kill -0 "$p12_probe_pid" 2>/dev/null; then
            kill "$p12_probe_pid" 2>/dev/null
            wait "$p12_probe_pid" 2>/dev/null
        fi
    done
    if [[ "$scenario_id" == "P16-V02" ]]; then
        "${compose[@]}" logs --no-color --timestamps --since 10m --tail 5000 namesrv broker-auth 2>&1 \
            | python3 -c 'import sys; text=sys.stdin.read(); text=text.replace(sys.argv[1], "[REDACTED]").replace(sys.argv[2], "[REDACTED]"); sys.stdout.write(text)' \
                "$P16_ACCESS_KEY" "$P16_SECRET_KEY" >"$evidence_dir/broker-logs.log"
    fi
    if [[ "$scenario_id" == "P13-V02" ]]; then
        "${compose[@]}" logs --no-color --timestamps --since 10m --tail 5000 broker broker-b \
            >"$evidence_dir/broker-logs.log" 2>&1
        for log_service in broker broker-b; do
            {
                printf '%s internal POP and Broker logs:\n' "$log_service"
                "${compose[@]}" exec --no-TTY "$log_service" sh -c '
                    for log in /home/rocketmq/logs/rocketmqlogs/pop.log /home/rocketmq/logs/rocketmqlogs/broker.log; do
                        if [ -f "$log" ]; then
                            echo "--- $log"
                            tail -n 2000 "$log"
                        fi
                    done
                '
            } >>"$evidence_dir/broker-internal-logs.log" 2>&1
        done
    fi
    if [[ "$scenario_id" == "P7-V03" || "$scenario_id" == "P7-V04" || "$scenario_id" == "P10-V03" || "$scenario_id" == "P11-V03" || "$scenario_id" == "P11-V04" || "$scenario_id" == "P12-V03" || "$scenario_id" == "P14-V03" || "$scenario_id" == "P15-V03" || "$scenario_id" == "P17-V02" ]]; then
        "${compose[@]}" logs --no-color --timestamps --since 10m --tail 5000 broker-a-host broker-b-host \
            >"$evidence_dir/broker-logs.log" 2>&1
    fi
    if [[ "$scenario_id" == "P14-V03" ]]; then
        for log_service in broker-a-host broker-b-host; do
            log_name="${log_service%-host}"
            "${compose[@]}" exec --no-TTY "$log_service" sh -c '
                log=/home/rocketmq/logs/rocketmqlogs/transaction.log
                if [ -f "$log" ]; then
                    tail -n 4000 "$log"
                else
                    echo "transaction log not found: $log" >&2
                    exit 1
                fi
            ' >"$evidence_dir/${log_name}-transaction.log" 2>&1
        done
    fi
    if [[ ("$scenario_id" == "P12-V03" || "$scenario_id" == "P14-V03") && "$group_created" == true ]]; then
        : >"$evidence_dir/group-cleanup.log"
        for broker_admin_target in "broker-a-host:127.0.0.1:50911" "broker-b-host:127.0.0.1:50921"; do
            topic_service="${broker_admin_target%%:*}"
            topic_broker="${broker_admin_target#*:}"
            "${compose[@]}" exec --no-TTY "$topic_service" ./mqadmin deleteSubGroup -b "$topic_broker" -g "$consumer_group" -n namesrv:9876 \
                >>"$evidence_dir/group-cleanup.log" 2>&1
            group_delete_status=$?
            if ((group_delete_status)); then
                status=1
                echo "Scenario consumer-group cleanup failed for $consumer_group on $topic_broker; see $evidence_dir/group-cleanup.log" >&2
            fi
        done
    fi
    if [[ "$topic_created" == true ]]; then
        : >"$evidence_dir/topic-cleanup.log"
        for topic_to_delete in "${topic_cleanup_list[@]}"; do
            if [[ "$profile" == "two-broker-host" ]]; then
                for broker_admin_target in "broker-a-host:127.0.0.1:50911" "broker-b-host:127.0.0.1:50921"; do
                    topic_service="${broker_admin_target%%:*}"
                    topic_broker="${broker_admin_target#*:}"
                    mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
                        -Dexec.args="delete-topic $topic_broker $topic_to_delete" \
                        exec:java@live-producer-lifecycle-probe \
                        >>"$evidence_dir/topic-cleanup.log" 2>&1
                    delete_status=$?
                    if ((delete_status)); then
                        status=1
                        echo "Scenario topic cleanup failed for $topic_to_delete on $topic_broker; see $evidence_dir/topic-cleanup.log" >&2
                    fi
                done
            else
                "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin deleteTopic -c RocketMQLocalTest -t "$topic_to_delete" -n namesrv:9876 \
                    >>"$evidence_dir/topic-cleanup.log" 2>&1
                delete_status=$?
                if ((delete_status)); then
                    status=1
                    echo "Scenario topic cleanup failed for $topic_to_delete; see $evidence_dir/topic-cleanup.log" >&2
                fi
            fi
        done
    fi
    for p7_proxy_pid in "${p7_proxy_pids[@]:-}"; do
        if [[ -n "$p7_proxy_pid" ]] && kill -0 "$p7_proxy_pid" 2>/dev/null; then
            kill "$p7_proxy_pid" 2>/dev/null
            wait "$p7_proxy_pid" 2>/dev/null
        fi
    done
    for p16_proxy_pid in "${p16_proxy_pids[@]:-}"; do
        if [[ -n "$p16_proxy_pid" ]] && kill -0 "$p16_proxy_pid" 2>/dev/null; then
            kill "$p16_proxy_pid" 2>/dev/null
            wait "$p16_proxy_pid" 2>/dev/null
        fi
    done
    for p17_proxy_pid in "${p17_proxy_pids[@]:-}"; do
        if [[ -n "$p17_proxy_pid" ]] && kill -0 "$p17_proxy_pid" 2>/dev/null; then
            kill "$p17_proxy_pid" 2>/dev/null
            wait "$p17_proxy_pid" 2>/dev/null
        fi
    done
    ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE="$profile" \
        COMPOSE_PROJECT_NAME="$project_name" bash "$repo_root/integration-tests/cleanup.sh" \
        >"$evidence_dir/compose-cleanup.log" 2>&1
    compose_cleanup_status=$?
    if ((compose_cleanup_status)); then
        status=1
        echo "Compose cleanup failed; see $evidence_dir/compose-cleanup.log" >&2
    fi
    if [[ -n "$p16_auth_config_path" ]]; then
        rm -f "$p16_auth_config_path"
    fi
    if [[ "$scenario_id" == "P16-V02" && -d "$evidence_dir/tls" ]]; then
        rm -f "$evidence_dir/tls"/*.key "$evidence_dir/tls"/*.csr "$evidence_dir/tls"/*.srl
        printf 'private_keys_removed=true\n' >>"$evidence_dir/metadata.txt"
    fi
    if ! python3 - "$evidence_dir" "$repo_root" "${HOME:-}" <<'PY'
import sys
from pathlib import Path

evidence_dir = Path(sys.argv[1])
replacements = [
    (sys.argv[2].encode(), b"<repo>"),
    (sys.argv[3].encode(), b"<home>"),
]
for path in evidence_dir.iterdir():
    if not path.is_file():
        continue
    contents = path.read_bytes()
    for source, replacement in replacements:
        if source:
            contents = contents.replace(source, replacement)
    path.write_bytes(contents)
PY
    then
        status=1
        echo "Failed to redact local paths from scenario evidence" >&2
    fi
    ended_epoch="$(date +%s)"
    if ((status == 0)); then
        if ! python3 - "$manifest" "$scenario_id" "$evidence_dir" <<'PY'
import json
import os
import re
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
scenario_id = sys.argv[2]
evidence = sys.argv[3]
lines = manifest_path.read_text().splitlines(keepends=True)
matches = 0
updated = []
for line in lines:
    body = line[:-1] if line.endswith("\n") else line
    match = re.fullmatch(r"(\s*)(\{.*\})(,?)(\s*)", body)
    if match is None:
        updated.append(line)
        continue
    scenario = json.loads(match.group(2))
    if scenario.get("id") != scenario_id:
        updated.append(line)
        continue
    matches += 1
    scenario["live_status"] = "PASS"
    scenario["evidence"] = evidence
    replacement = match.group(1) + json.dumps(scenario, separators=(",", ":")) + match.group(3) + match.group(4)
    updated.append(replacement + ("\n" if line.endswith("\n") else ""))

if matches != 1:
    raise SystemExit(f"expected one manifest row for {scenario_id}, found {matches}")
temporary = manifest_path.with_name(f".{manifest_path.name}.{os.getpid()}.tmp")
temporary.write_text("".join(updated))
temporary.replace(manifest_path)
PY
        then
            status=1
            echo "Failed to reconcile the successful scenario into the manifest" >&2
        fi
    fi
    printf 'exit_status=%s\n' "$status" >>"$evidence_dir/metadata.txt"
    printf 'duration_seconds=%s\n' "$((ended_epoch - started_epoch))" >>"$evidence_dir/metadata.txt"
    rm -f "$selected_file"
    exit "$status"
}
trap cleanup EXIT

if [[ "$scenario_id" == "P16-V02" ]]; then
    p16_access_key="$(openssl rand -hex 16)"
    p16_secret_key="$(openssl rand -hex 32)"
    export P16_ACCESS_KEY="$p16_access_key"
    export P16_SECRET_KEY="$p16_secret_key"
    p16_auth_config_path="$(mktemp "${TMPDIR:-/tmp}/rocketmq-p16-broker.XXXXXX")"
    chmod 600 "$p16_auth_config_path"
    sed -e "s/P16_RUNTIME_USER/$p16_access_key/g" \
        -e "s/P16_RUNTIME_SECRET/$p16_secret_key/g" \
        "$repo_root/integration-tests/config/broker-auth.conf" >"$p16_auth_config_path"
    export ROCKETMQ_BROKER_AUTH_CONFIG="$p16_auth_config_path"
    p16_tls_dir="$evidence_dir/tls"
    mkdir -p "$p16_tls_dir"
    openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
        -keyout "$p16_tls_dir/ca.key" -out "$p16_tls_dir/ca.pem" \
        -subj "/CN=rocketmq-classic-p16-test-ca" >"$p16_tls_dir/ca-generation.log" 2>&1
    openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
        -keyout "$p16_tls_dir/untrusted-ca.key" -out "$p16_tls_dir/untrusted-ca.pem" \
        -subj "/CN=rocketmq-classic-p16-untrusted-test-ca" >"$p16_tls_dir/untrusted-ca-generation.log" 2>&1
    openssl req -new -newkey rsa:2048 -nodes \
        -keyout "$p16_tls_dir/server.key" -out "$p16_tls_dir/server.csr" \
        -subj "/CN=localhost" >"$p16_tls_dir/server-key-generation.log" 2>&1
    cat >"$p16_tls_dir/server.ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
EOF
    openssl x509 -req -days 2 -in "$p16_tls_dir/server.csr" \
        -CA "$p16_tls_dir/ca.pem" -CAkey "$p16_tls_dir/ca.key" \
        -CAcreateserial -CAserial "$p16_tls_dir/ca.srl" \
        -extfile "$p16_tls_dir/server.ext" -out "$p16_tls_dir/server.pem" \
        >"$p16_tls_dir/server-signing.log" 2>&1
    openssl req -new -newkey rsa:2048 -nodes \
        -keyout "$p16_tls_dir/client.key" -out "$p16_tls_dir/client.csr" \
        -subj "/CN=rocketmq-classic-p16-test-client" >"$p16_tls_dir/client-key-generation.log" 2>&1
    cat >"$p16_tls_dir/client.ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
EOF
    openssl x509 -req -days 2 -in "$p16_tls_dir/client.csr" \
        -CA "$p16_tls_dir/ca.pem" -CAkey "$p16_tls_dir/ca.key" \
        -CAcreateserial -CAserial "$p16_tls_dir/ca.srl" \
        -extfile "$p16_tls_dir/client.ext" -out "$p16_tls_dir/client.pem" \
        >"$p16_tls_dir/client-signing.log" 2>&1
    chmod 600 "$p16_tls_dir"/*.key
    {
        openssl dgst -sha256 "$p16_tls_dir/server.pem"
        openssl dgst -sha256 "$p16_tls_dir/client.pem"
        openssl dgst -sha256 "$p16_tls_dir/ca.pem"
        openssl dgst -sha256 "$p16_tls_dir/untrusted-ca.pem"
        openssl x509 -in "$p16_tls_dir/server.pem" -noout -subject -issuer
        openssl x509 -in "$p16_tls_dir/client.pem" -noout -subject -issuer
    } >"$p16_tls_dir/certificate-fingerprints.txt" 2>&1
fi

echo "Provisioning isolated $profile profile in $project_name"
if [[ "$scenario_id" == "P0-V03" ]]; then
    printf 'Aggregate parent starts no Broker; each constituent run owns and removes its Compose project.\n' \
        >"$evidence_dir/setup.log"
else
    if ! ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE="$profile" \
        COMPOSE_PROJECT_NAME="$project_name" bash "$repo_root/integration-tests/setup.sh" \
        >"$evidence_dir/setup.log" 2>&1; then
        cat "$evidence_dir/setup.log" >&2
        exit 1
    fi
fi
cat "$evidence_dir/setup.log"

if [[ "$scenario_id" == "P16-V02" ]]; then
    start_p16_proxy() {
        local script_name="$1"
        local label="$2"
        shift 2
        python3 -u "$repo_root/tests/differential/$script_name" "$@" \
            >"$evidence_dir/${label}-proxy.stdout.log" \
            2>"$evidence_dir/${label}-proxy.stderr.log" &
        local proxy_pid=$!
        p16_proxy_pids+=("$proxy_pid")
        local ready_file="$evidence_dir/${label}-proxy-ready.txt"
        for attempt in {1..100}; do
            if [[ -s "$ready_file" ]]; then
                return 0
            fi
            if ! kill -0 "$proxy_pid" 2>/dev/null; then
                echo "$label proxy exited before becoming ready; inspect evidence logs" >&2
                exit 1
            fi
            sleep 0.1
        done
        echo "$label proxy did not become ready; inspect evidence logs" >&2
        exit 1
    }
    run_p16_java() {
        local label="$1"
        local expected="$2"
        local args="$3"
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            "-Dexec.args=$args" exec:java@live-p16-security-probe \
            >"$evidence_dir/java-${label}.tsv" 2>"$evidence_dir/java-${label}.stderr.log"; then
            echo "Java P16 probe $label failed; inspect its evidence output" >&2
            exit 1
        fi
        if ! grep -Fxq "$expected" "$evidence_dir/java-${label}.tsv"; then
            echo "Java P16 probe $label returned an unexpected evidence row" >&2
            exit 1
        fi
    }
    run_p16_rust() {
        local label="$1"
        local expected="$2"
        shift 2
        if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- "$@" \
            >"$evidence_dir/rust-${label}.tsv" 2>"$evidence_dir/rust-${label}.stderr.log"; then
            echo "Rust P16 probe $label failed; inspect its evidence output" >&2
            exit 1
        fi
        if ! grep -Fxq "$expected" "$evidence_dir/rust-${label}.tsv"; then
            echo "Rust P16 probe $label returned an unexpected evidence row" >&2
            exit 1
        fi
    }
    start_p16_proxy acl_tampering_proxy.py acl \
        --upstream "$broker_addr" \
        --ready-file "$evidence_dir/acl-proxy-ready.txt" \
        --log-file "$evidence_dir/acl-proxy-events.tsv"
    p16_acl_proxy_port="$(cat "$evidence_dir/acl-proxy-ready.txt")"
    p16_acl_proxy_addr="127.0.0.1:$p16_acl_proxy_port"
    run_p16_java auth-valid $'P16\tACL\tvalid\tACCEPTED\t0\t0\t0' "auth-valid $broker_addr"
    run_p16_rust auth-valid $'P16\tACL\tvalid\tACCEPTED\t0\t0\t0' p16-auth "$broker_addr" valid
    run_p16_java auth-wrong-secret $'P16\tACL\twrong-secret\tREJECTED\t0\t0\t0' "auth-wrong-secret $broker_addr"
    run_p16_rust auth-wrong-secret $'P16\tACL\twrong-secret\tREJECTED\t0\t0\t0' p16-auth "$broker_addr" wrong-secret
    run_p16_java auth-tampered $'P16\tACL\ttampered\tREJECTED\t0\t0\t0' "auth-tampered $p16_acl_proxy_addr"
    run_p16_rust auth-tampered $'P16\tACL\ttampered\tREJECTED\t0\t0\t0' p16-auth "$p16_acl_proxy_addr" tampered

    start_p16_proxy tls_broker_proxy.py tls \
        --upstream "$broker_addr" \
        --cert "$p16_tls_dir/server.pem" \
        --key "$p16_tls_dir/server.key" \
        --client-ca "$p16_tls_dir/ca.pem" \
        --ready-file "$evidence_dir/tls-proxy-ready.txt" \
        --log-file "$evidence_dir/tls-proxy-events.tsv"
    p16_tls_proxy_port="$(cat "$evidence_dir/tls-proxy-ready.txt")"
    for language in java rust; do
        for mode in trusted wrong-host untrusted missing-client; do
            p16_tls_addr="localhost:$p16_tls_proxy_port"
            p16_tls_ca="$p16_tls_dir/ca.pem"
            p16_tls_cert="$p16_tls_dir/client.pem"
            p16_tls_key="$p16_tls_dir/client.key"
            if [[ "$mode" == "wrong-host" ]]; then
                p16_tls_addr="127.0.0.1:$p16_tls_proxy_port"
            elif [[ "$mode" == "untrusted" ]]; then
                p16_tls_ca="$p16_tls_dir/untrusted-ca.pem"
            elif [[ "$mode" == "missing-client" ]]; then
                p16_tls_cert="-"
                p16_tls_key="-"
            fi
            p16_tls_outcome=REJECTED
            if [[ "$mode" == "trusted" || "$mode" == "wrong-host" ]]; then
                p16_tls_outcome=ACCEPTED
            fi
            printf -v expected 'P16\tTLS\t%s\t%s\t0\t0\t0' "$mode" "$p16_tls_outcome"
            if [[ "$language" == "java" ]]; then
                run_p16_java "tls-$mode" "$expected" \
                    "tls-$mode $p16_tls_addr $p16_tls_ca $p16_tls_cert $p16_tls_key"
            else
                run_p16_rust "tls-$mode" "$expected" p16-tls \
                    "$p16_tls_addr" "$mode" "$p16_tls_ca" "$p16_tls_cert" "$p16_tls_key"
            fi
        done
    done

    tampered_count="$(awk -F '\t' '$2 == "TAMPERED_BODY" { count++ } END { print count + 0 }' \
        "$evidence_dir/acl-proxy-events.tsv")"
    tls_accepted_count="$(awk -F '\t' '$2 == "TLS_ACCEPTED" { count++ } END { print count + 0 }' \
        "$evidence_dir/tls-proxy-events.tsv")"
    tls_rejected_count="$(awk -F '\t' '$2 ~ /^HANDSHAKE_REJECTED:/ { count++ } END { print count + 0 }' \
        "$evidence_dir/tls-proxy-events.tsv")"
    if [[ "$tampered_count" != "2" || "$tls_accepted_count" -lt 3 || "$tls_rejected_count" -lt 4 ]]; then
        echo "P16 proxy evidence did not show both body mutations and expected mTLS handshakes" >&2
        exit 1
    fi
    for p16_proxy_pid in "${p16_proxy_pids[@]}"; do
        kill "$p16_proxy_pid" 2>/dev/null || true
        wait "$p16_proxy_pid" 2>/dev/null || true
    done
    p16_proxy_pids=()
    printf 'PASS P16-V02: Java 5.5.1 and Rust accepted valid ACL signatures and rejected wrong secrets and tampered heartbeat bodies; mutual TLS accepted trusted clients and trusted wrong-host certificates without endpoint identification, and rejected untrusted CAs and missing client certificates\n' \
        | tee "$evidence_dir/result.txt"
    exit 0
fi

if [[ "$scenario_id" == "P17-V02" ]]; then
    echo "Starting per-Broker forwarding proxies for the two-Broker admin-query case"
    printf 'forward\n' >"$evidence_dir/proxy-a-mode.txt"
    printf 'forward\n' >"$evidence_dir/proxy-b-mode.txt"
    start_p17_proxy() {
        local label="$1"
        local listen_port="$2"
        local upstream_port="$3"
        local mode_file="$evidence_dir/proxy-${label#broker-}-mode.txt"
        local ready_file="$evidence_dir/proxy-${label#broker-}-ready.txt"
        local log_file="$evidence_dir/proxy-${label#broker-}-events.tsv"
        local prefix="$evidence_dir/proxy-${label#broker-}"
        python3 -u "$repo_root/tests/differential/query_failure_proxy.py" \
            --label "$label" --listen-port "$listen_port" \
            --upstream "127.0.0.1:$upstream_port" \
            --mode-file "$mode_file" --ready-file "$ready_file" --log-file "$log_file" \
            >"$prefix.stdout.log" 2>"$prefix.stderr.log" &
        local proxy_pid=$!
        p17_proxy_pids+=("$proxy_pid")
        for _ in {1..100}; do
            if [[ -s "$ready_file" ]]; then
                return 0
            fi
            if ! kill -0 "$proxy_pid" 2>/dev/null; then
                cat "$prefix.stdout.log" "$prefix.stderr.log" >&2 2>/dev/null || true
                return 1
            fi
            sleep 0.1
        done
        echo "P17 proxy $label did not become ready on 127.0.0.1:$listen_port" >&2
        return 1
    }
    start_p17_proxy broker-a 50911 51911
    start_p17_proxy broker-b 50921 51921

    echo "Creating the scenario-owned topic on each Broker"
    topic_created=true
    : >"$evidence_dir/topic-create.log"
    for broker_admin_target in "broker-a-host:127.0.0.1:50911" "broker-b-host:127.0.0.1:50921"; do
        topic_service="${broker_admin_target%%:*}"
        topic_broker="${broker_admin_target#*:}"
        if ! "${compose[@]}" exec --no-TTY "$topic_service" ./mqadmin updateTopic \
            -b "$topic_broker" -t "$topic" -r 1 -w 1 -n namesrv:9876 \
            >>"$evidence_dir/topic-create.log" 2>&1; then
            cat "$evidence_dir/topic-create.log" >&2
            exit 1
        fi
    done

    echo "Seeding duplicate keys and the bounded timestamp fixtures"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        "-Dexec.args=seed $namesrv_addr $topic" exec:java@live-p17-admin-query-probe \
        >"$evidence_dir/java-seed.tsv" 2>"$evidence_dir/java-seed.stderr.log"; then
        cat "$evidence_dir/java-seed.tsv" "$evidence_dir/java-seed.stderr.log" >&2 2>/dev/null || true
        exit 1
    fi
    if [[ "$(awk -F '\t' '$1 == "P17" && $2 == "SEND" { count++ } END { print count + 0 }' "$evidence_dir/java-seed.tsv")" != 4 ]] \
        || ! grep -Fxq $'P17\tSEED\t4\t2\t2' "$evidence_dir/java-seed.tsv" \
        || ! grep -Fq $'P17\tSEND\tp17-duplicate-key\tbroker-a\t' "$evidence_dir/java-seed.tsv" \
        || ! grep -Fq $'P17\tSEND\tp17-duplicate-key\tbroker-b\t' "$evidence_dir/java-seed.tsv" \
        || ! grep -Fq $'P17\tSEND\tp17-bounded-time-range\tbroker-a\t' "$evidence_dir/java-seed.tsv" \
        || ! grep -Fq $'P17\tSEND\tp17-bounded-time-range\tbroker-b\t' "$evidence_dir/java-seed.tsv"; then
        echo "Java seed did not report all four Broker-targeted messages" >&2
        exit 1
    fi
    sleep 1

    run_p17_java_query() {
        local label="$1"
        local key="$2"
        local begin_timestamp="$3"
        local end_timestamp="$4"
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            "-Dexec.args=query $namesrv_addr $topic $key $begin_timestamp $end_timestamp" \
            exec:java@live-p17-admin-query-probe \
            >"$evidence_dir/java-${label}.tsv" 2>"$evidence_dir/java-${label}.stderr.log"; then
            cat "$evidence_dir/java-${label}.tsv" "$evidence_dir/java-${label}.stderr.log" >&2 2>/dev/null || true
            echo "Java P17 query $label failed" >&2
            exit 1
        fi
    }
    run_p17_rust_query() {
        local label="$1"
        local key="$2"
        local begin_timestamp="$3"
        local end_timestamp="$4"
        if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
            p17-query "$namesrv_addr" "$topic" "$key" "$begin_timestamp" "$end_timestamp" \
            >"$evidence_dir/rust-${label}.tsv" 2>"$evidence_dir/rust-${label}.stderr.log"; then
            cat "$evidence_dir/rust-${label}.tsv" "$evidence_dir/rust-${label}.stderr.log" >&2 2>/dev/null || true
            echo "Rust P17 query $label failed" >&2
            exit 1
        fi
    }
    run_p17_java_query duplicate p17-duplicate-key 0 9223372036854775807
    run_p17_rust_query duplicate p17-duplicate-key 0 9223372036854775807
    run_p17_java_query bounded-all p17-bounded-time-range 0 9223372036854775807
    run_p17_rust_query bounded-all p17-bounded-time-range 0 9223372036854775807
    if ! python3 "$repo_root/tests/differential/check_p17_admin_query.py" prepare "$evidence_dir" \
        >"$evidence_dir/prepare.tsv" 2>"$evidence_dir/prepare.stderr.log"; then
        cat "$evidence_dir/prepare.tsv" "$evidence_dir/prepare.stderr.log" >&2 2>/dev/null || true
        exit 1
    fi
    IFS=$'\t' read -r p17_marker p17_begin_timestamp p17_end_timestamp \
        < <(grep -F $'P17_PREPARED\t' "$evidence_dir/prepare.tsv")
    if [[ "$p17_marker" != "P17_PREPARED" || ! "$p17_begin_timestamp" =~ ^[0-9]+$ || ! "$p17_end_timestamp" =~ ^[0-9]+$ ]]; then
        echo "P17 checker did not provide a valid bounded query interval" >&2
        exit 1
    fi
    run_p17_java_query bounded-range p17-bounded-time-range "$p17_begin_timestamp" "$p17_end_timestamp"
    run_p17_rust_query bounded-range p17-bounded-time-range "$p17_begin_timestamp" "$p17_end_timestamp"

    set_p17_proxy_mode() {
        local label="$1"
        local mode="$2"
        local target="$evidence_dir/proxy-${label#broker-}-mode.txt"
        local temporary="$target.tmp.$$"
        printf '%s\n' "$mode" >"$temporary"
        mv "$temporary" "$target"
    }
    echo "Injecting a Broker B query failure while Broker A remains available"
    set_p17_proxy_mode broker-b error
    run_p17_java_query partial p17-duplicate-key 0 9223372036854775807
    run_p17_rust_query partial p17-duplicate-key 0 9223372036854775807
    echo "Injecting query failures at both Brokers to verify terminal mapping"
    set_p17_proxy_mode broker-a error
    run_p17_java_query terminal p17-duplicate-key 0 9223372036854775807
    run_p17_rust_query terminal p17-duplicate-key 0 9223372036854775807

    if ! python3 "$repo_root/tests/differential/check_p17_admin_query.py" verify "$evidence_dir" \
        >"$evidence_dir/verify.tsv" 2>"$evidence_dir/verify.stderr.log"; then
        cat "$evidence_dir/verify.tsv" "$evidence_dir/verify.stderr.log" >&2 2>/dev/null || true
        exit 1
    fi
    cat "$evidence_dir/result.txt"
    exit 0
fi

if [[ "$scenario_id" == "P5-V02-extensions" ]]; then
    echo "Creating the one-queue extension topic and isolated Java/Rust POP groups"
    topic_created=true
    if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateTopic -c RocketMQLocalTest -t "$topic" -r 1 -w 1 -n namesrv:9876 \
        >"$evidence_dir/topic-create.log" 2>&1; then
        cat "$evidence_dir/topic-create.log" >&2
        exit 1
    fi
    if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateTopic -c RocketMQLocalTest -t "%LMQ%${topic}%entries" -r 1 -w 1 -n namesrv:9876 \
        >>"$evidence_dir/topic-create.log" 2>&1; then
        cat "$evidence_dir/topic-create.log" >&2
        exit 1
    fi
    : >"$evidence_dir/group-create.log"
    for language in java rust; do
        for suffix in base lite pop ack batch_ack change; do
            extension_group="${consumer_group}_${language}_${suffix}"
            if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateSubGroup -c RocketMQLocalTest -g "$extension_group" -n namesrv:9876 \
                >>"$evidence_dir/group-create.log" 2>&1; then
                cat "$evidence_dir/group-create.log" >&2
                exit 1
            fi
        done
    done
fi

if [[ "$scenario_id" == "ROUTE-001-get-existing-topic" || "$scenario_id" == "PROD-001-sync-send" || "$scenario_id" == "PROD-002-async-send" || "$scenario_id" == "PROD-003-oneway-send" || "$scenario_id" == "PROD-004-batch-send" || "$scenario_id" == "PROD-005-send-timeout" || "$scenario_id" == "CONS-001-query-offset" || "$scenario_id" == "CONS-002-pull-found" || "$scenario_id" == "CONS-003-pull-no-new-msg" || "$scenario_id" == "CONS-004-update-offset" || "$scenario_id" == "ORDER-001-lock-unlock" || "$scenario_id" == "REBAL-001-average-10q-3c" || "$scenario_id" == "TX-001-commit" || "$scenario_id" == "TX-002-rollback" || "$scenario_id" == "P3-V04" || "$scenario_id" == "P5-V02-route" || "$scenario_id" == "P5-V02-producer" || "$scenario_id" == "P5-V02-consumer" || "$scenario_id" == "P6-V04" || "$scenario_id" == "P7-V02" || "$scenario_id" == "P7-V03" || "$scenario_id" == "P7-V04" || "$scenario_id" == "P9-V03" || "$scenario_id" == "P10-V03" || "$scenario_id" == "P11-V03" || "$scenario_id" == "P11-V04" || "$scenario_id" == "P12-V03" || "$scenario_id" == "P13-V02" || "$scenario_id" == "P14-V03" || "$scenario_id" == "REQ-001-request-reply-success" || "$scenario_id" == "P15-V03" ]]; then
    echo "Creating scenario-owned topic $topic"
    topic_created=true
    topic_queue_count=4
    if [[ "$scenario_id" == "P3-V04" || "$scenario_id" == "P6-V04" || "$scenario_id" == "P7-V03" || "$scenario_id" == "P9-V03" || "$scenario_id" == "P10-V03" || "$scenario_id" == "P11-V04" || "$scenario_id" == "P12-V03" || "$scenario_id" == "P14-V03" || "$scenario_id" == "REQ-001-request-reply-success" || "$scenario_id" == "P15-V03" || "$scenario_id" == "PROD-005-send-timeout" || "$scenario_id" == "P5-V02-consumer" ]]; then
        topic_queue_count=1
    elif [[ "$scenario_id" == "REBAL-001-average-10q-3c" ]]; then
        topic_queue_count=5
    elif [[ "$scenario_id" == "TX-001-commit" || "$scenario_id" == "TX-002-rollback" ]]; then
        topic_queue_count=1
    fi
    if [[ "$profile" == "two-broker-host" ]]; then
        for broker_admin_target in "broker-a-host:127.0.0.1:50911" "broker-b-host:127.0.0.1:50921"; do
            topic_service="${broker_admin_target%%:*}"
            topic_broker="${broker_admin_target#*:}"
            if ! "${compose[@]}" exec --no-TTY "$topic_service" ./mqadmin updateTopic -b "$topic_broker" -t "$topic" -r "$topic_queue_count" -w "$topic_queue_count" -n namesrv:9876 \
                >>"$evidence_dir/topic-create.log" 2>&1; then
                cat "$evidence_dir/topic-create.log" >&2
                exit 1
            fi
        done
    elif ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateTopic -c RocketMQLocalTest -t "$topic" -r "$topic_queue_count" -w "$topic_queue_count" -n namesrv:9876 \
        >"$evidence_dir/topic-create.log" 2>&1; then
        cat "$evidence_dir/topic-create.log" >&2
        exit 1
    fi
    if [[ "$scenario_id" == "REQ-001-request-reply-success" ]]; then
        if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateTopic -c RocketMQLocalTest -t "$rust_request_topic" -r 1 -w 1 -n namesrv:9876 \
            >>"$evidence_dir/topic-create.log" 2>&1; then
            cat "$evidence_dir/topic-create.log" >&2
            exit 1
        fi
    fi
    if [[ "$scenario_id" == "P15-V03" ]]; then
        for extra_topic in "${p15_topics[@]}"; do
            for broker_admin_target in "broker-a-host:127.0.0.1:50911" "broker-b-host:127.0.0.1:50921"; do
                topic_service="${broker_admin_target%%:*}"
                topic_broker="${broker_admin_target#*:}"
                if ! "${compose[@]}" exec --no-TTY "$topic_service" ./mqadmin updateTopic -b "$topic_broker" -t "$extra_topic" -r 1 -w 1 -n namesrv:9876 \
                    >>"$evidence_dir/topic-create.log" 2>&1; then
                    cat "$evidence_dir/topic-create.log" >&2
                    exit 1
                fi
            done
        done
    fi
    if [[ "$scenario_id" == "CONS-001-query-offset" || "$scenario_id" == "CONS-002-pull-found" || "$scenario_id" == "CONS-003-pull-no-new-msg" || "$scenario_id" == "CONS-004-update-offset" || "$scenario_id" == "ORDER-001-lock-unlock" || "$scenario_id" == "P5-V02-consumer" ]]; then
        echo "Creating scenario-owned subscription group $producer_group"
        if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateSubGroup -c RocketMQLocalTest -g "$producer_group" -n namesrv:9876 \
            >"$evidence_dir/group-create.log" 2>&1; then
            cat "$evidence_dir/group-create.log" >&2
            exit 1
        fi
    elif [[ "$scenario_id" == "TX-001-commit" || "$scenario_id" == "TX-002-rollback" || "$scenario_id" == "P6-V04" ]]; then
        echo "Creating scenario-owned consumer group $consumer_group"
        if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateSubGroup -c RocketMQLocalTest -g "$consumer_group" -n namesrv:9876 \
            >"$evidence_dir/group-create.log" 2>&1; then
            cat "$evidence_dir/group-create.log" >&2
            exit 1
        fi
    elif [[ "$scenario_id" == "PROD-005-send-timeout" ]]; then
        echo "Creating scenario-owned readback consumer group compat_timeout_readback"
        if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateSubGroup -c RocketMQLocalTest -g compat_timeout_readback -n namesrv:9876 \
            >"$evidence_dir/group-create.log" 2>&1; then
            cat "$evidence_dir/group-create.log" >&2
            exit 1
        fi
    elif [[ "$scenario_id" == "P5-V02-producer" ]]; then
        echo "Creating scenario-owned reply consumer group compat_p5_reply_group"
        if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateSubGroup -c RocketMQLocalTest -g compat_p5_reply_group -n namesrv:9876 \
            >"$evidence_dir/group-create.log" 2>&1; then
            cat "$evidence_dir/group-create.log" >&2
            exit 1
        fi
    elif [[ "$scenario_id" == "P13-V02" ]]; then
        echo "Creating scenario-owned Java/Rust POP lifecycle groups"
        : >"$evidence_dir/group-create.log"
        for language in java rust; do
            for suffix in expiry change mixed; do
                pop_group="${consumer_group}_${language}_${suffix}"
                if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateSubGroup -c RocketMQLocalTest -g "$pop_group" -n namesrv:9876 \
                    >>"$evidence_dir/group-create.log" 2>&1; then
                    cat "$evidence_dir/group-create.log" >&2
                    exit 1
                fi
            done
        done
    elif [[ "$scenario_id" == "P9-V03" ]]; then
        for group_suffix in java rust; do
            pull_group="${consumer_group}_${group_suffix}"
            if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateSubGroup -c RocketMQLocalTest -g "$pull_group" -n namesrv:9876 \
                >>"$evidence_dir/group-create.log" 2>&1; then
                cat "$evidence_dir/group-create.log" >&2
                exit 1
            fi
        done
    elif [[ "$scenario_id" == "P15-V03" ]]; then
        echo "Creating scenario-owned Java and Rust reply responder groups on both Brokers"
        : >"$evidence_dir/group-create.log"
        for responder_group in "${consumer_group}_java" "${consumer_group}_rust"; do
            for broker_admin_target in "broker-a-host:127.0.0.1:50911" "broker-b-host:127.0.0.1:50921"; do
                topic_service="${broker_admin_target%%:*}"
                topic_broker="${broker_admin_target#*:}"
                if ! "${compose[@]}" exec --no-TTY "$topic_service" ./mqadmin updateSubGroup -b "$topic_broker" -g "$responder_group" -n namesrv:9876 \
                    >>"$evidence_dir/group-create.log" 2>&1; then
                    cat "$evidence_dir/group-create.log" >&2
                    exit 1
                fi
            done
        done
    elif [[ "$scenario_id" == "P10-V03" ]]; then
        echo "Creating scenario-owned Push consumer group with retry limit $p10_retry_max_times"
        : >"$evidence_dir/group-create.log"
        for broker_admin_target in "broker-a-host:127.0.0.1:50911" "broker-b-host:127.0.0.1:50921"; do
            topic_service="${broker_admin_target%%:*}"
            topic_broker="${broker_admin_target#*:}"
            if ! "${compose[@]}" exec --no-TTY "$topic_service" ./mqadmin updateSubGroup -b "$topic_broker" -g "$consumer_group" -r "$p10_retry_max_times" -q 1 -n namesrv:9876 \
                >>"$evidence_dir/group-create.log" 2>&1; then
                cat "$evidence_dir/group-create.log" >&2
                exit 1
            fi
        done
    elif [[ "$scenario_id" == "P11-V03" ]]; then
        echo "Creating scenario-owned mixed Java/Rust Push group on both Brokers"
        : >"$evidence_dir/group-create.log"
        for broker_admin_target in "broker-a-host:127.0.0.1:50911" "broker-b-host:127.0.0.1:50921"; do
            topic_service="${broker_admin_target%%:*}"
            topic_broker="${broker_admin_target#*:}"
            if ! "${compose[@]}" exec --no-TTY "$topic_service" ./mqadmin updateSubGroup -b "$topic_broker" -g "$consumer_group" -n namesrv:9876 \
                >>"$evidence_dir/group-create.log" 2>&1; then
                cat "$evidence_dir/group-create.log" >&2
                exit 1
            fi
        done
    elif [[ "$scenario_id" == "P11-V04" ]]; then
        echo "Creating scenario-owned bidirectional Push retry group with one retry on both Brokers"
        : >"$evidence_dir/group-create.log"
        for broker_admin_target in "broker-a-host:127.0.0.1:50911" "broker-b-host:127.0.0.1:50921"; do
            topic_service="${broker_admin_target%%:*}"
            topic_broker="${broker_admin_target#*:}"
            if ! "${compose[@]}" exec --no-TTY "$topic_service" ./mqadmin updateSubGroup -b "$topic_broker" -g "$consumer_group" -r 1 -q 1 -n namesrv:9876 \
                >>"$evidence_dir/group-create.log" 2>&1; then
                cat "$evidence_dir/group-create.log" >&2
                exit 1
            fi
        done
    elif [[ "$scenario_id" == "P12-V03" ]]; then
        echo "Creating scenario-owned orderly consumer group on both Brokers"
        group_created=true
        : >"$evidence_dir/group-create.log"
        for broker_admin_target in "broker-a-host:127.0.0.1:50911" "broker-b-host:127.0.0.1:50921"; do
            topic_service="${broker_admin_target%%:*}"
            topic_broker="${broker_admin_target#*:}"
            if ! "${compose[@]}" exec --no-TTY "$topic_service" ./mqadmin updateSubGroup -b "$topic_broker" -g "$consumer_group" -n namesrv:9876 \
                >>"$evidence_dir/group-create.log" 2>&1; then
                cat "$evidence_dir/group-create.log" >&2
                exit 1
            fi
        done
    elif [[ "$scenario_id" == "P14-V03" ]]; then
        echo "Creating scenario-owned transaction consumer group on both Brokers"
        group_created=true
        : >"$evidence_dir/group-create.log"
        for broker_admin_target in "broker-a-host:127.0.0.1:50911" "broker-b-host:127.0.0.1:50921"; do
            topic_service="${broker_admin_target%%:*}"
            topic_broker="${broker_admin_target#*:}"
            if ! "${compose[@]}" exec --no-TTY "$topic_service" ./mqadmin updateSubGroup -b "$topic_broker" -g "$consumer_group" -n namesrv:9876 \
                >>"$evidence_dir/group-create.log" 2>&1; then
                cat "$evidence_dir/group-create.log" >&2
                exit 1
            fi
        done
    elif [[ "$scenario_id" == "REQ-001-request-reply-success" ]]; then
        echo "Creating scenario-owned responder group $consumer_group"
        for responder_group in "$consumer_group" "${consumer_group}_java" "${consumer_group}_rust"; do
            if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateSubGroup -c RocketMQLocalTest -g "$responder_group" -n namesrv:9876 \
                >>"$evidence_dir/group-create.log" 2>&1; then
                cat "$evidence_dir/group-create.log" >&2
                exit 1
            fi
        done
    fi
elif [[ "$scenario_id" == "ROUTE-002-topic-not-exist" ]]; then
    echo "Confirming that scenario topic $topic has no route"
elif [[ "$scenario_id" == "HEARTBEAT-001" ]]; then
    echo "Sending a Java/Rust heartbeat for producer group $producer_group"
else
    echo "Querying the same route with both clients"
fi

echo "Compiling Java 5.5.1 and Rust live probes"
if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" -DskipTests compile \
    >"$evidence_dir/java-compile.log" 2>&1; then
    cat "$evidence_dir/java-compile.log" >&2
    exit 1
fi
echo "Java live probe sources compiled"
if ! cargo build --locked -p rocketmq-classic-integration --bin live_client \
    >"$evidence_dir/rust-build.log" 2>&1; then
    cat "$evidence_dir/rust-build.log" >&2
    exit 1
fi
echo "Rust live probe build completed"

if [[ "$scenario_id" == "P7-V03" ]]; then
    echo "Starting transparent response proxies for the bounded deadline phase"
    start_p7_proxy() {
        local broker_name="$1"
        local listen_port="$2"
        local upstream_port="$3"
        local proxy_prefix="$evidence_dir/$broker_name-proxy"
        python3 "$repo_root/tests/differential/delayed_response_proxy.py" \
            --upstream "127.0.0.1:$upstream_port" \
            --delay-ms "$p7_response_delay_ms" \
            --listen-port "$listen_port" \
            --delay-flag "$proxy_prefix.delay-flag" \
            --delay-send-flag "$proxy_prefix.delay-send-flag" \
            --reject-send-flag "$proxy_prefix.reject-flag" \
            --reject-response-code 16 \
            --ready-file "$proxy_prefix.ready" \
            --log-file "$proxy_prefix.tsv" \
            >"$proxy_prefix.stdout" 2>"$proxy_prefix.stderr" &
        p7_proxy_pid="$!"
        p7_proxy_pids+=("$p7_proxy_pid")
        for _ in {1..50}; do
            if [[ -s "$proxy_prefix.ready" ]]; then
                return 0
            fi
            if ! kill -0 "$p7_proxy_pid" 2>/dev/null; then
                cat "$proxy_prefix.stdout" "$proxy_prefix.stderr" >&2 2>/dev/null || true
                return 1
            fi
            sleep 0.1
        done
        echo "P7-V03 proxy $broker_name did not become ready on 127.0.0.1:$listen_port" >&2
        return 1
    }
    start_p7_proxy broker-a 50911 51911
    start_p7_proxy broker-b 50921 51921
fi

start_reply_responder() {
    local language="$1"
    local reply_body="$2"
    local request_topic="$3"
    local output_file="$4"
    local delay_ms="${5:-0}"
    if [[ "$language" == "java" ]]; then
        mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="respond $namesrv_addr $request_topic ${consumer_group}_java compat_req_java_responder $reply_body $delay_ms" \
            exec:java@live-request-reply-probe >"$output_file" 2>"${output_file%.tsv}.stderr" &
    else
        "$repo_root/target/debug/live_client" p19-respond "$namesrv_addr" "$request_topic" \
            "${consumer_group}_rust" compat_req_rust_responder "$reply_body" "$delay_ms" \
            >"$output_file" 2>"${output_file%.tsv}.stderr" &
    fi
    responder_pid=$!
    for _ in {1..30}; do
        if grep -Fqx $'REQ\tREADY' "$output_file" 2>/dev/null; then
            return 0
        fi
        if ! kill -0 "$responder_pid" 2>/dev/null; then
            wait "$responder_pid" 2>/dev/null || true
            responder_pid=""
            cat "$output_file" "${output_file%.tsv}.stderr" 2>/dev/null >&2 || true
            return 1
        fi
        sleep 1
    done
    echo "$language responder did not become ready" >&2
    return 1
}

wait_reply_responder() {
    local output_file="$1"
    if ! wait "$responder_pid"; then
        cat "$output_file" "${output_file%.tsv}.stderr" 2>/dev/null >&2 || true
        responder_pid=""
        return 1
    fi
    responder_pid=""
    if ! awk -F '\t' 'NF == 7 && $1 == "REQ" && $2 == "RESPONDER" && $3 == "SEND_OK" { count++ }
        END { if (count != 1) exit 1 }' "$output_file"; then
        echo "responder did not send exactly one successful reply" >&2
        cat "$output_file" "${output_file%.tsv}.stderr" 2>/dev/null >&2 || true
        return 1
    fi
}

run_reply_requester() {
    local language="$1"
    local request_topic="$2"
    local body="$3"
    local output_file="$4"
    if [[ "$language" == "java" ]]; then
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="request $namesrv_addr $request_topic compat_req_java_requester $body" \
            exec:java@live-request-reply-probe >"$output_file" \
            2>"${output_file%.tsv}.stderr"; then
            cat "$output_file" "${output_file%.tsv}.stderr" 2>/dev/null >&2 || true
            return 1
        fi
    else
        if ! "$repo_root/target/debug/live_client" p19-request "$namesrv_addr" "$request_topic" \
            compat_req_rust_requester "$body" >"$output_file" \
            2>"${output_file%.tsv}.stderr"; then
            cat "$output_file" "${output_file%.tsv}.stderr" 2>/dev/null >&2 || true
            return 1
        fi
    fi
}

run_request_timeout() {
    local language="$1"
    local mode="$2"
    local request_topic="$3"
    local group="$4"
    local body="$5"
    local output_file="$6"
    local command="p19-timeout"
    local java_mode="timeout"
    local hold_ms="$p15_no_reply_hold_ms"
    if [[ "$mode" == "late" ]]; then
        command="p19-late-timeout"
        java_mode="late-timeout"
        hold_ms="$p15_late_reply_hold_ms"
    fi
    if [[ "$language" == "java" ]]; then
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="$java_mode $namesrv_addr $request_topic $group $body $p15_timeout_ms $hold_ms" \
            exec:java@live-request-reply-probe >"$output_file" \
            2>"${output_file%.tsv}.stderr"; then
            cat "$output_file" "${output_file%.tsv}.stderr" 2>/dev/null >&2 || true
            return 1
        fi
    else
        if ! "$repo_root/target/debug/live_client" "$command" "$namesrv_addr" "$request_topic" \
            "$group" "$body" "$p15_timeout_ms" "$hold_ms" >"$output_file" \
            2>"${output_file%.tsv}.stderr"; then
            cat "$output_file" "${output_file%.tsv}.stderr" 2>/dev/null >&2 || true
            return 1
        fi
    fi
}

start_timeout_proxy() {
    python3 "$repo_root/tests/differential/delayed_response_proxy.py" \
        --upstream "$broker_addr" --delay-ms 600 \
        --delay-flag "$evidence_dir/delay-next-response" \
        --ready-file "$evidence_dir/proxy-ready.txt" \
        --log-file "$evidence_dir/proxy.tsv" \
        >"$evidence_dir/proxy.stdout" 2>"$evidence_dir/proxy.stderr" &
    timeout_proxy_pid=$!
    for _ in {1..100}; do
        if [[ -s "$evidence_dir/proxy-ready.txt" ]]; then
            timeout_proxy_addr="127.0.0.1:$(cat "$evidence_dir/proxy-ready.txt")"
            return 0
        fi
        if ! kill -0 "$timeout_proxy_pid" 2>/dev/null; then
            wait "$timeout_proxy_pid" 2>/dev/null || true
            timeout_proxy_pid=""
            cat "$evidence_dir/proxy.stdout" "$evidence_dir/proxy.stderr" 2>/dev/null >&2 || true
            return 1
        fi
        sleep 0.1
    done
    echo "delayed-response proxy did not become ready" >&2
    return 1
}

start_timeout_client() {
    local language="$1"
    local producer_group="$2"
    local message_body="$3"
    local output_file="$4"
    local go_file="$5"
    if [[ "$language" == "java" ]]; then
        mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="timeout-send $timeout_proxy_addr $topic $producer_group $message_body $timeout_ms $go_file" \
            exec:java@live-producer-probe >"$output_file" 2>"${output_file%.tsv}.stderr" &
    else
        "$repo_root/target/debug/live_client" timeout-send "$timeout_proxy_addr" "$topic" \
            "$producer_group" "$message_body" "$timeout_ms" "$go_file" \
            >"$output_file" 2>"${output_file%.tsv}.stderr" &
    fi
    timeout_probe_pid=$!
    for _ in {1..300}; do
        if awk -F '\t' -v language="$language" \
            '$1 == "TIMEOUT" && $2 == "READY" && $3 == language { found = 1 }
            END { if (!found) exit 1 }' "$output_file" 2>/dev/null; then
            return 0
        fi
        if ! kill -0 "$timeout_probe_pid" 2>/dev/null; then
            wait "$timeout_probe_pid" 2>/dev/null || true
            timeout_probe_pid=""
            cat "$output_file" "${output_file%.tsv}.stderr" 2>/dev/null >&2 || true
            return 1
        fi
        sleep 0.1
    done
    echo "$language timeout probe did not become ready" >&2
    return 1
}

release_timeout_client() {
    local go_file="$1"
    touch "$evidence_dir/delay-next-response"
    touch "$go_file"
    if ! wait "$timeout_probe_pid"; then
        cat "$evidence_dir/java-timeout.tsv" "$evidence_dir/java-timeout.stderr" \
            "$evidence_dir/rust-timeout.tsv" "$evidence_dir/rust-timeout.stderr" 2>/dev/null >&2 || true
        timeout_probe_pid=""
        return 1
    fi
    timeout_probe_pid=""
}

wait_for_timeout_proxy_delays() {
    local expected="$1"
    for _ in {1..80}; do
        delayed_count="$(awk -F '\t' '$1 == "PROXY" && $2 == "DELAY_DONE" { count++ }
            END { print count + 0 }' "$evidence_dir/proxy.tsv" 2>/dev/null || true)"
        if ((delayed_count >= expected)); then
            return 0
        fi
        sleep 0.1
    done
    echo "proxy observed $delayed_count delayed Broker responses; expected at least $expected" >&2
    cat "$evidence_dir/proxy.tsv" >&2
    return 1
}

if [[ "$scenario_id" == "PROD-001-sync-send" ]]; then
    echo "Sending one message from each language and reading each with the other client"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="send $broker_addr $topic $body" exec:java@live-producer-probe \
        >"$evidence_dir/java-send.tsv" 2>"$evidence_dir/java-send.stderr"; then
        cat "$evidence_dir/java-send.tsv" "$evidence_dir/java-send.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! awk -F '\t' -v topic="$topic" \
        'NF != 7 || $1 != "SEND" || $2 != "SEND_OK" || $3 != topic || $4 != 0 || $5 !~ /^[0-9]+$/ || $6 !~ /^[[:xdigit:]]+$/ || $7 !~ /^[0-9]+$/ { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/java-send.tsv"; then
        echo "Java synchronous send did not return a valid SEND_OK result" >&2
        exit 1
    fi
    IFS=$'\t' read -r _ java_send_status java_send_topic java_queue_id java_queue_offset java_offset_message_id java_physical_offset \
        <"$evidence_dir/java-send.tsv"
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        view "$broker_addr" "$topic" "$java_offset_message_id" \
        >"$evidence_dir/rust-read-java.tsv" 2>"$evidence_dir/rust-read-java.stderr"; then
        cat "$evidence_dir/rust-read-java.stderr" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        send "$broker_addr" "$topic" "$body" \
        >"$evidence_dir/rust-send.tsv" 2>"$evidence_dir/rust-send.stderr"; then
        cat "$evidence_dir/rust-send.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v topic="$topic" \
        'NF != 7 || $1 != "SEND" || $2 != "SEND_OK" || $3 != topic || $4 != 0 || $5 !~ /^[0-9]+$/ || $6 !~ /^[[:xdigit:]]+$/ || $7 !~ /^[0-9]+$/ { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/rust-send.tsv"; then
        echo "Rust synchronous send did not return a valid SEND_OK result" >&2
        exit 1
    fi
    IFS=$'\t' read -r _ rust_send_status rust_send_topic rust_queue_id rust_queue_offset rust_offset_message_id rust_physical_offset \
        <"$evidence_dir/rust-send.tsv"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="view $broker_addr $topic $rust_offset_message_id" exec:java@live-producer-probe \
        >"$evidence_dir/java-read-rust.tsv" 2>"$evidence_dir/java-read-rust.stderr"; then
        cat "$evidence_dir/java-read-rust.tsv" "$evidence_dir/java-read-rust.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if [[ "$java_send_status" != "$rust_send_status" || "$java_send_topic" != "$rust_send_topic" || "$java_queue_id" != "$rust_queue_id" ]]; then
        echo "Java and Rust synchronous send results differ in status, topic, or queue" >&2
        exit 1
    fi
    expected_body_hex="$(python3 -c 'import sys; print(sys.argv[1].encode().hex())' "$body")"
    for readback in "$evidence_dir/rust-read-java.tsv" "$evidence_dir/java-read-rust.tsv"; do
        if ! awk -F '\t' -v topic="$topic" -v body="$expected_body_hex" \
            'NF != 5 || $1 != "MESSAGE" || $2 != topic || $3 != 0 || $4 !~ /^[0-9]+$/ || $5 != body { exit 1 }
             END { if (NR != 1) exit 1 }' "$readback"; then
            echo "Cross-language readback did not match the scenario topic and body: $readback" >&2
            exit 1
        fi
    done
    printf 'PASS %s: matching SEND_OK results and bidirectional cross-language readback\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "PROD-002-async-send" ]]; then
    echo "Sending one asynchronous message from each language and reading each with the other client"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="async-send $broker_addr $topic $body" exec:java@live-producer-probe \
        >"$evidence_dir/java-async-send.tsv" 2>"$evidence_dir/java-async-send.stderr"; then
        cat "$evidence_dir/java-async-send.tsv" "$evidence_dir/java-async-send.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! awk -F '\t' -v topic="$topic" \
        'NF != 8 || $1 != "ASYNC_CALLBACK" || $2 != 1 || $3 != "SEND_OK" || $4 != topic || $5 != 0 || $6 !~ /^[0-9]+$/ || $7 !~ /^[[:xdigit:]]+$/ || $8 !~ /^[0-9]+$/ { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/java-async-send.tsv"; then
        echo "Java asynchronous send did not produce exactly one successful callback result" >&2
        exit 1
    fi
    IFS=$'\t' read -r _ _ java_send_status java_send_topic java_queue_id java_queue_offset java_offset_message_id java_physical_offset \
        <"$evidence_dir/java-async-send.tsv"
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        view "$broker_addr" "$topic" "$java_offset_message_id" \
        >"$evidence_dir/rust-read-java.tsv" 2>"$evidence_dir/rust-read-java.stderr"; then
        cat "$evidence_dir/rust-read-java.stderr" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        async-send "$broker_addr" "$topic" "$body" \
        >"$evidence_dir/rust-async-send.tsv" 2>"$evidence_dir/rust-async-send.stderr"; then
        cat "$evidence_dir/rust-async-send.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v topic="$topic" \
        'NF != 8 || $1 != "ASYNC_FUTURE" || $2 != 1 || $3 != "SEND_OK" || $4 != topic || $5 != 0 || $6 !~ /^[0-9]+$/ || $7 !~ /^[[:xdigit:]]+$/ || $8 !~ /^[0-9]+$/ { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/rust-async-send.tsv"; then
        echo "Rust asynchronous send did not resolve to exactly one successful future result" >&2
        exit 1
    fi
    IFS=$'\t' read -r _ _ rust_send_status rust_send_topic rust_queue_id rust_queue_offset rust_offset_message_id rust_physical_offset \
        <"$evidence_dir/rust-async-send.tsv"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="view $broker_addr $topic $rust_offset_message_id" exec:java@live-producer-probe \
        >"$evidence_dir/java-read-rust.tsv" 2>"$evidence_dir/java-read-rust.stderr"; then
        cat "$evidence_dir/java-read-rust.tsv" "$evidence_dir/java-read-rust.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if [[ "$java_send_status" != "$rust_send_status" || "$java_send_topic" != "$rust_send_topic" || "$java_queue_id" != "$rust_queue_id" ]]; then
        echo "Java and Rust asynchronous send results differ in status, topic, or queue" >&2
        exit 1
    fi
    expected_body_hex="$(python3 -c 'import sys; print(sys.argv[1].encode().hex())' "$body")"
    for readback in "$evidence_dir/rust-read-java.tsv" "$evidence_dir/java-read-rust.tsv"; do
        if ! awk -F '\t' -v topic="$topic" -v body="$expected_body_hex" \
            'NF != 5 || $1 != "MESSAGE" || $2 != topic || $3 != 0 || $4 !~ /^[0-9]+$/ || $5 != body { exit 1 }
             END { if (NR != 1) exit 1 }' "$readback"; then
            echo "Cross-language readback did not match the scenario topic and body: $readback" >&2
            exit 1
        fi
    done
    printf 'PASS %s: matching async results, one Java callback / Rust future completion, and bidirectional cross-language readback\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "PROD-003-oneway-send" ]]; then
    echo "Sending one oneway message from each language, then reading each with the other client"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="oneway-send $broker_addr $topic $body $java_oneway_key $java_expected_max_offset" exec:java@live-producer-probe \
        >"$evidence_dir/java-oneway.tsv" 2>"$evidence_dir/java-oneway.stderr"; then
        cat "$evidence_dir/java-oneway.tsv" "$evidence_dir/java-oneway.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    expected_body_hex="$(python3 -c 'import sys; print(sys.argv[1].encode().hex())' "$body")"
    if ! awk -F '\t' -v topic="$topic" -v key="$java_oneway_key" -v max_offset="$java_expected_max_offset" \
        'NR == 1 && (NF != 4 || $1 != "ONEWAY_RETURN" || $2 != "NO_RESPONSE" || $3 != topic || $4 != key) { exit 1 }
         NR == 2 && (NF != 2 || $1 != "QUEUE_MAX_OFFSET" || $2 != max_offset) { exit 1 }
         END { if (NR != 2) exit 1 }' "$evidence_dir/java-oneway.tsv"; then
        echo "Java oneway send must return without a response and advance the Broker queue" >&2
        exit 1
    fi

    query_message_id() {
        local message_key="$1"
        local language="$2"
        local raw_output="$evidence_dir/broker-query-$language.txt"
        local offset_output="$evidence_dir/broker-offset-query-$language.txt"
        local key_query="$evidence_dir/$language-key-query.tsv"
        local normalized_output="$evidence_dir/$language-message-id.tsv"
        if ! "${compose[@]}" exec --no-TTY broker ./mqadmin queryMsgByKey \
            -t "$topic" -k "$message_key" -n namesrv:9876 \
            >"$raw_output" 2>"$evidence_dir/broker-query-$language.stderr"; then
            cat "$raw_output" "$evidence_dir/broker-query-$language.stderr" 2>/dev/null >&2 || true
            return 1
        fi
        if ! awk -v key="$message_key" '
            NF == 3 && $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/ {
                printf "KEY_QUERY\t%s\t%s\t%s\t%s\n", key, $1, $2, $3
                found++
            }
            END { if (found != 1) exit 1 }
        ' "$raw_output" >"$key_query"; then
            echo "Broker key query did not return exactly one indexed message for $language" >&2
            cat "$raw_output" >&2
            return 1
        fi
        IFS=$'\t' read -r _ _ unique_key _ _ <"$key_query"
        if ! "${compose[@]}" exec --no-TTY broker ./mqadmin queryMsgById \
            -t "$topic" -i "$unique_key" -n namesrv:9876 \
            >"$offset_output" 2>"$evidence_dir/broker-offset-query-$language.stderr"; then
            cat "$offset_output" "$evidence_dir/broker-offset-query-$language.stderr" 2>/dev/null >&2 || true
            return 1
        fi
        if ! awk -F ':' -v key="$message_key" '
            {
                value = $2
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
                if ($1 == "OffsetID") offset_id = value
                if ($1 == "Queue ID") queue_id = value
                if ($1 == "Queue Offset") queue_offset = value
            }
            END {
                if ((length(offset_id) != 32 && length(offset_id) != 56) ||
                    offset_id !~ /^[0-9A-Fa-f]+$/ || queue_id !~ /^[0-9]+$/ ||
                    queue_offset !~ /^[0-9]+$/) exit 1
                printf "MESSAGE_ID\t%s\t%s\t%s\t%s\n", key, offset_id, queue_id, queue_offset
            }
        ' "$offset_output" >"$normalized_output"; then
            echo "Broker message-ID query did not return a physical offset ID for $language" >&2
            cat "$offset_output" >&2
            return 1
        fi
    }

    query_message_id "$java_oneway_key" java
    IFS=$'\t' read -r _ _ java_message_id _ java_queue_offset <"$evidence_dir/java-message-id.tsv"
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        view "$broker_addr" "$topic" "$java_message_id" \
        >"$evidence_dir/rust-read-java.tsv" 2>"$evidence_dir/rust-read-java.stderr"; then
        cat "$evidence_dir/rust-read-java.stderr" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        oneway-send "$broker_addr" "$topic" "$body" "$rust_oneway_key" "$rust_expected_max_offset" \
        >"$evidence_dir/rust-oneway.tsv" 2>"$evidence_dir/rust-oneway.stderr"; then
        cat "$evidence_dir/rust-oneway.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v topic="$topic" -v key="$rust_oneway_key" -v max_offset="$rust_expected_max_offset" \
        'NR == 1 && (NF != 4 || $1 != "ONEWAY_RETURN" || $2 != "NO_RESPONSE" || $3 != topic || $4 != key) { exit 1 }
         NR == 2 && (NF != 2 || $1 != "QUEUE_MAX_OFFSET" || $2 != max_offset) { exit 1 }
         END { if (NR != 2) exit 1 }' "$evidence_dir/rust-oneway.tsv"; then
        echo "Rust oneway send must return without a response and advance the Broker queue" >&2
        exit 1
    fi

    query_message_id "$rust_oneway_key" rust
    IFS=$'\t' read -r _ _ rust_message_id _ rust_queue_offset <"$evidence_dir/rust-message-id.tsv"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="view $broker_addr $topic $rust_message_id" exec:java@live-producer-probe \
        >"$evidence_dir/java-read-rust.tsv" 2>"$evidence_dir/java-read-rust.stderr"; then
        cat "$evidence_dir/java-read-rust.tsv" "$evidence_dir/java-read-rust.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    for language_readback in "rust-read-java.tsv:$java_queue_offset" "java-read-rust.tsv:$rust_queue_offset"; do
        readback="${language_readback%%:*}"
        expected_queue_offset="${language_readback##*:}"
        if ! awk -F '\t' -v topic="$topic" -v queue_offset="$expected_queue_offset" -v body="$expected_body_hex" \
            'NF != 5 || $1 != "MESSAGE" || $2 != topic || $3 != 0 || $4 != queue_offset || $5 != body { exit 1 }
             END { if (NR != 1) exit 1 }' "$evidence_dir/$readback"; then
            echo "Cross-language oneway readback did not match the stored message: $readback" >&2
            exit 1
        fi
    done
    printf 'PASS %s: both oneway calls returned without a Broker result; each keyed message reached the queue and was read by the other client\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "PROD-004-batch-send" ]]; then
    echo "Sending one two-message batch from each language and decoding the other client's batch"
    validate_batch_send() {
        python3 - "$1" "$topic" "$message_count" <<'PY'
import re
import sys
from pathlib import Path

rows = [line.split("\t") for line in Path(sys.argv[1]).read_text().splitlines()]
topic = sys.argv[2]
count = int(sys.argv[3])
if len(rows) != 1 or len(rows[0]) != 9:
    raise SystemExit("batch send evidence must contain one nine-column result")
row = rows[0]
ids = row[5].split(",")
first_id = row[6]
if (
    row[0] != "BATCH_SEND"
    or row[1] != "SEND_OK"
    or row[2] != topic
    or row[3] != "0"
    or not row[4].isdecimal()
    or len(ids) != count
    or ids[0] != first_id
    or any(not re.fullmatch(r"[0-9a-fA-F]+", value) or len(value) not in {32, 56} for value in ids)
    or not re.fullmatch(r"[0-9a-fA-F]+", first_id)
    or len(first_id) not in {32, 56}
    or not row[7].isdecimal()
    or int(first_id[-16:], 16) != int(row[7])
    or row[8] != str(count)
):
    raise SystemExit("batch send result or physical-offset ID is invalid")
PY
    }
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="batch-send $broker_addr $topic $body $message_count" exec:java@live-producer-probe \
        >"$evidence_dir/java-batch-send.tsv" 2>"$evidence_dir/java-batch-send.stderr"; then
        cat "$evidence_dir/java-batch-send.tsv" "$evidence_dir/java-batch-send.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! validate_batch_send "$evidence_dir/java-batch-send.tsv"; then
        echo "Java batch send did not return one successful result for all messages" >&2
        exit 1
    fi
    IFS=$'\t' read -r _ java_send_status java_send_topic java_queue_id java_queue_offset java_offset_message_ids java_offset_message_id java_physical_offset java_message_count \
        <"$evidence_dir/java-batch-send.tsv"
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        view-batch "$broker_addr" "$topic" "$java_offset_message_ids" \
        >"$evidence_dir/rust-read-java.tsv" 2>"$evidence_dir/rust-read-java.stderr"; then
        cat "$evidence_dir/rust-read-java.stderr" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        batch-send "$broker_addr" "$topic" "$body" "$message_count" \
        >"$evidence_dir/rust-batch-send.tsv" 2>"$evidence_dir/rust-batch-send.stderr"; then
        cat "$evidence_dir/rust-batch-send.stderr" >&2
        exit 1
    fi
    if ! validate_batch_send "$evidence_dir/rust-batch-send.tsv"; then
        echo "Rust batch send did not return one successful result for all messages" >&2
        exit 1
    fi
    IFS=$'\t' read -r _ rust_send_status rust_send_topic rust_queue_id rust_queue_offset rust_offset_message_ids rust_offset_message_id rust_physical_offset rust_message_count \
        <"$evidence_dir/rust-batch-send.tsv"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="view-batch $broker_addr $topic $rust_offset_message_ids" exec:java@live-producer-probe \
        >"$evidence_dir/java-read-rust.tsv" 2>"$evidence_dir/java-read-rust.stderr"; then
        cat "$evidence_dir/java-read-rust.tsv" "$evidence_dir/java-read-rust.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if [[ "$java_send_status" != "$rust_send_status" || "$java_send_topic" != "$rust_send_topic" || \
        "$java_queue_id" != "$rust_queue_id" || "$java_message_count" != "$rust_message_count" || \
        "$rust_queue_offset" -ne $((java_queue_offset + message_count)) ]]; then
        echo "Java and Rust batch send result metadata or queue progression differs" >&2
        exit 1
    fi
    first_body_hex="$(python3 -c 'import sys; print((sys.argv[1] + "-0").encode().hex())' "$body")"
    second_body_hex="$(python3 -c 'import sys; print((sys.argv[1] + "-1").encode().hex())' "$body")"
    for language_readback in "rust-read-java.tsv:$java_queue_offset" "java-read-rust.tsv:$rust_queue_offset"; do
        readback="${language_readback%%:*}"
        expected_queue_offset="${language_readback##*:}"
        if ! awk -F '\t' -v topic="$topic" -v first_offset="$expected_queue_offset" \
            -v first_body="$first_body_hex" -v second_body="$second_body_hex" \
            'NR == 1 && (NF != 5 || $1 != "MESSAGE" || $2 != topic || $3 != 0 || $4 != first_offset || $5 != first_body) { exit 1 }
             NR == 2 && (NF != 5 || $1 != "MESSAGE" || $2 != topic || $3 != 0 || $4 != first_offset + 1 || $5 != second_body) { exit 1 }
             END { if (NR != 2) exit 1 }' "$evidence_dir/$readback"; then
            echo "Cross-language batch readback did not decode both expected messages: $readback" >&2
            exit 1
        fi
    done
    printf 'PASS %s: Java and Rust batch sends succeeded and each decoded both messages from the other client\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P5-V02-producer" ]]; then
    for client in java rust; do
        if [[ "$client" == "java" ]]; then
            if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
                -Dexec.args="$broker_addr $topic" exec:java@live-p5-producer-probe \
                >"$evidence_dir/java-p5-producer.tsv" 2>"$evidence_dir/java-p5-producer.stderr"; then
                cat "$evidence_dir/java-p5-producer.tsv" "$evidence_dir/java-p5-producer.stderr" 2>/dev/null >&2 || true
                exit 1
            fi
        else
            java_max_offset="$(awk -F '\t' '$1 == "P5" && $2 == "MAX" { value = $3 } END { if (value == "") exit 1; print value }' \
                "$evidence_dir/java-p5-producer.tsv")"
            if ! "$repo_root/target/debug/live_client" p5-producer "$broker_addr" "$topic" compat_p5_producer "$java_max_offset" \
                >"$evidence_dir/rust-p5-producer.tsv" 2>"$evidence_dir/rust-p5-producer.stderr"; then
                cat "$evidence_dir/rust-p5-producer.tsv" "$evidence_dir/rust-p5-producer.stderr" 2>/dev/null >&2 || true
                exit 1
            fi
        fi
    done
    if ! python3 - "$topic" "$evidence_dir/java-p5-producer.tsv" \
        "$evidence_dir/rust-p5-producer.tsv" <<'PY'
import sys
from pathlib import Path

topic, java_path, rust_path = sys.argv[1:]
methods = ["send-v1", "send-v2", "batch", "reply-v1", "reply-v2"]

def parse(path):
    rows = [line.split("\t") for line in Path(path).read_text().splitlines()]
    if len(rows) != 8:
        raise SystemExit(f"{path}: expected base, three send, two one-way reply, end-transaction, and max-offset rows")
    if rows[0][:2] != ["P5", "BASE"] or len(rows[0]) != 3:
        raise SystemExit(f"{path}: missing initial queue offset")
    base = int(rows[0][2])
    sends = rows[1:4]
    for index, (row, method) in enumerate(zip(sends, methods[:3])):
        if len(row) != 7 or row[:3] != ["P5", "RESULT", method]:
            raise SystemExit(f"{path}: malformed result for {method}: {row!r}")
        if row[3:] != ["SEND_OK", topic, "0", str(base + index)]:
            raise SystemExit(f"{path}: unexpected send result for {method}: {row!r}")
    for row, method in zip(rows[4:6], methods[3:]):
        if row != ["P5", "RESULT", method, "ONEWAY"]:
            raise SystemExit(f"{path}: one-way reply result differs for {method}: {row!r}")
    if rows[6] != ["P5", "RESULT", "end-transaction", "ONEWAY"]:
        raise SystemExit(f"{path}: end-transaction one-way result differs: {rows[6]!r}")
    final_offset = base + 3
    if rows[7] != ["P5", "MAX", str(final_offset)]:
        raise SystemExit(f"{path}: unexpected final queue max offset: {rows[7]!r}")
    return base, sends, final_offset

java_base, java_sends, java_max = parse(java_path)
rust_base, rust_sends, rust_max = parse(rust_path)
if java_base != 0 or rust_base != java_max or rust_max != java_max + 3:
    raise SystemExit(
        "Java and Rust producer passes did not execute consecutively from the expected queue offsets "
        f"(java_base={java_base}, java_max={java_max}, rust_base={rust_base}, rust_max={rust_max})"
    )
for java_row, rust_row in zip(java_sends, rust_sends):
    if java_row[2:6] != rust_row[2:6]:
        raise SystemExit(f"Java/Rust {java_row[2]} producer results differ: {java_row!r} vs {rust_row!r}")
PY
    then
        echo "Java/Rust P5 producer RPC results or committed queue offsets differ" >&2
        cat "$evidence_dir/java-p5-producer.tsv" "$evidence_dir/rust-p5-producer.tsv" >&2 || true
        exit 1
    fi
    printf 'PASS P5-V02-producer: Java and Rust matched V1, V2, and batch send results, plus one-way reply V1/V2 and endTransactionOneway outcomes\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P5-V02-consumer" ]]; then
    for client in java rust; do
        if [[ "$client" == "java" ]]; then
            if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
                -Dexec.args="send $broker_addr $topic p5-java-seed" \
                exec:java@live-producer-probe >"$evidence_dir/java-seed.tsv" \
                2>"$evidence_dir/java-seed.stderr"; then
                cat "$evidence_dir/java-seed.tsv" "$evidence_dir/java-seed.stderr" 2>/dev/null >&2 || true
                exit 1
            fi
        else
            if ! "$repo_root/target/debug/live_client" send "$broker_addr" "$topic" p5-rust-seed \
                >"$evidence_dir/rust-seed.tsv" 2>"$evidence_dir/rust-seed.stderr"; then
                cat "$evidence_dir/rust-seed.tsv" "$evidence_dir/rust-seed.stderr" 2>/dev/null >&2 || true
                exit 1
            fi
        fi
    done
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="$broker_addr $consumer_group $topic p5-java-seed" \
        exec:java@live-p5-consumer-probe >"$evidence_dir/java-p5-consumer.tsv" \
        2>"$evidence_dir/java-p5-consumer.stderr"; then
        cat "$evidence_dir/java-p5-consumer.tsv" "$evidence_dir/java-p5-consumer.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! "$repo_root/target/debug/live_client" p5-consumer "$broker_addr" "$consumer_group" "$topic" p5-rust-seed \
        >"$evidence_dir/rust-p5-consumer.tsv" 2>"$evidence_dir/rust-p5-consumer.stderr"; then
        cat "$evidence_dir/rust-p5-consumer.tsv" "$evidence_dir/rust-p5-consumer.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-p5-consumer.tsv" "$evidence_dir/rust-p5-consumer.tsv" <<'PY'
import sys
from pathlib import Path

expected = [
    ["P5C", "PULL", "FOUND", "2", "0", "2"],
    ["P5C", "LITE_PULL", "FOUND", "2", "0", "2"],
    ["P5C", "UPDATE_OFFSET", "1"],
    ["P5C", "QUERY_OFFSET", "1"],
    ["P5C", "CONSUMER_LIST", "PRESENT"],
    ["P5C", "SEND_BACK", "OK"],
    ["P5C", "LOCK", "true"],
    ["P5C", "UNLOCK", "OK"],
]
for path in sys.argv[1:]:
    rows = [line.split("\t") for line in Path(path).read_text().splitlines()]
    if rows != expected:
        raise SystemExit(f"{path}: consumer RPC results differ from the expected Java/Rust oracle: {rows!r}")
java = Path(sys.argv[1]).read_bytes()
rust = Path(sys.argv[2]).read_bytes()
if java != rust:
    raise SystemExit("Java and Rust P5 consumer RPC result rows differ")
PY
    then
        echo "Java/Rust P5 consumer RPC outcomes or queue results differ" >&2
        exit 1
    fi
    printf 'PASS P5-V02-consumer: Java and Rust matched pull, lite-pull, offset, consumer-list, send-back, lock and unlock RPC outcomes\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P5-V02-extensions" ]]; then
    for client in java rust; do
        client_group="${consumer_group}_${client}"
        if [[ "$client" == "java" ]]; then
            echo "P5-V02-extensions: preparing Java group $client_group"
            if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
                -Dexec.args="prepare $broker_addr $topic $client_group" \
                exec:java@live-p5-extensions-probe >"$evidence_dir/java-p5-extension-prepare.tsv" \
                2>"$evidence_dir/java-p5-extension-prepare.stderr"; then
                cat "$evidence_dir/java-p5-extension-prepare.tsv" "$evidence_dir/java-p5-extension-prepare.stderr" 2>/dev/null >&2 || true
                exit 1
            fi
        else
            echo "P5-V02-extensions: preparing Rust group $client_group"
            if ! "$repo_root/target/debug/live_client" p5-extension-prepare "$broker_addr" "$topic" "$client_group" \
                >"$evidence_dir/rust-p5-extension-prepare.tsv" 2>"$evidence_dir/rust-p5-extension-prepare.stderr"; then
                cat "$evidence_dir/rust-p5-extension-prepare.tsv" "$evidence_dir/rust-p5-extension-prepare.stderr" 2>/dev/null >&2 || true
                exit 1
            fi
        fi
    done
    lite_topic="%LMQ%${topic}%entries"
    echo "P5-V02-extensions: seeding the parent topic"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="send $broker_addr $topic p5-extension-seed" \
        exec:java@live-producer-probe >"$evidence_dir/java-extension-seed.tsv" \
        2>"$evidence_dir/java-extension-seed.stderr"; then
        cat "$evidence_dir/java-extension-seed.tsv" "$evidence_dir/java-extension-seed.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    echo "P5-V02-extensions: seeding the Lite topic"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="send $broker_addr $lite_topic p5-extension-lite" \
        exec:java@live-producer-probe >"$evidence_dir/java-extension-lite-seed.tsv" \
        2>"$evidence_dir/java-extension-lite-seed.stderr"; then
        cat "$evidence_dir/java-extension-lite-seed.tsv" "$evidence_dir/java-extension-lite-seed.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    for client in java rust; do
        client_group="${consumer_group}_${client}"
        if [[ "$client" == "java" ]]; then
            echo "P5-V02-extensions: consuming with Java"
            if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
                -Dexec.args="consume $broker_addr $topic $client_group" \
                exec:java@live-p5-extensions-probe >"$evidence_dir/java-p5-extension-consume.tsv" \
                2>"$evidence_dir/java-p5-extension-consume.stderr"; then
                cat "$evidence_dir/java-p5-extension-consume.tsv" "$evidence_dir/java-p5-extension-consume.stderr" 2>/dev/null >&2 || true
                exit 1
            fi
        else
            echo "P5-V02-extensions: consuming with Rust"
            if ! "$repo_root/target/debug/live_client" p5-extension-consume "$broker_addr" "$topic" "$client_group" \
                >"$evidence_dir/rust-p5-extension-consume.tsv" 2>"$evidence_dir/rust-p5-extension-consume.stderr"; then
                cat "$evidence_dir/rust-p5-extension-consume.tsv" "$evidence_dir/rust-p5-extension-consume.stderr" 2>/dev/null >&2 || true
                exit 1
            fi
        fi
    done
    echo "P5-V02-extensions: comparing Java/Rust outcomes"
    if ! python3 - "$evidence_dir" <<'PY'
import sys
from pathlib import Path

root = Path(sys.argv[1])
methods = {
    "heartbeat", "heartbeat-v2", "unregister", "assignment", "request-mode",
    "lite-subscription", "pop", "pop-lite", "ack", "batch-ack", "change-invisible-time",
}
expected_bodies = {
    "pop": b"p5-extension-seed".hex(),
}

def read(prefix):
    paths = [root / f"{prefix}-p5-extension-prepare.tsv", root / f"{prefix}-p5-extension-consume.tsv"]
    rows = {}
    for path in paths:
        for line in path.read_text().splitlines():
            fields = line.split("\t")
            if len(fields) < 3 or fields[0] != "P5E" or fields[1] not in methods:
                raise SystemExit(f"{path}: malformed or unregistered extension result {fields!r}")
            if fields[1] in rows:
                raise SystemExit(f"{path}: duplicate {fields[1]} result")
            rows[fields[1]] = fields[2:]
    if set(rows) != methods:
        raise SystemExit(f"{prefix}: expected {sorted(methods)}, got {sorted(rows)}")
    if rows["heartbeat"][0] != "OK" or rows["heartbeat-v2"][0] != "OK":
        raise SystemExit(f"{prefix}: heartbeat result was not successful")
    if rows["unregister"] != ["OK"] or rows["request-mode"] != ["OK"]:
        raise SystemExit(f"{prefix}: synchronous extension operation failed: {rows!r}")
    if rows["lite-subscription"] != ["UNSUPPORTED", "200071", "3"]:
        raise SystemExit(f"{prefix}: Lite subscription did not return the pinned 5.3.4 unsupported response: {rows['lite-subscription']!r}")
    if rows["pop-lite"] != ["UNSUPPORTED", "200070", "3"]:
        raise SystemExit(f"{prefix}: Lite POP did not return the pinned 5.3.4 unsupported response: {rows['pop-lite']!r}")
    for name, body in expected_bodies.items():
        if rows[name] != ["FOUND", body]:
            raise SystemExit(f"{prefix}: {name} did not return the seeded message: {rows[name]!r}")
    if rows["ack"] != ["OK"] or rows["batch-ack"] != ["OK"]:
        raise SystemExit(f"{prefix}: ACK result was not successful: {rows!r}")
    if rows["change-invisible-time"] != ["OK", "METADATA"]:
        raise SystemExit(f"{prefix}: invisibility change omitted successful metadata: {rows!r}")
    return rows

java = read("java")
rust = read("rust")
for method in methods:
    if java[method] != rust[method]:
        raise SystemExit(f"Java/Rust {method} outcomes differ: {java[method]!r} vs {rust[method]!r}")
PY
    then
        echo "Java/Rust P5 extension outcomes or seeded POP results differ" >&2
        exit 1
    fi
    printf 'PASS P5-V02-extensions: Java and Rust matched heartbeat, assignment, request-mode, Lite subscription, POP, ACK, and invisibility-change results\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P11-V03" ]]; then
    java_ownership_file="$evidence_dir/java-ownership.txt"
    rust_ownership_file="$evidence_dir/rust-ownership.txt"
    java_stop_file="$evidence_dir/java-stop"
    java_exited_file="$evidence_dir/java-exited"
    rust_stop_file="$evidence_dir/rust-stop"
    rust_exited_file="$evidence_dir/rust-exited"
    rm -f "$java_ownership_file" "$rust_ownership_file" "$java_stop_file" \
        "$java_exited_file" "$rust_stop_file" "$rust_exited_file"
    echo "Starting Java Push consumer and waiting for all eight queues"
    mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="$namesrv_addr $topic $consumer_group $java_ownership_file $java_stop_file" \
        exec:java@live-mixed-group-probe \
        >"$evidence_dir/java-p11-mixed.tsv" 2>"$evidence_dir/java-p11-mixed.stderr" &
    p11_java_pid="$!"
    if ! python3 "$repo_root/tests/differential/check_mixed_ownership.py" \
        java-only "$topic" "$java_ownership_file" "$rust_ownership_file" \
        "$java_exited_file" 90 2>&1 | tee "$evidence_dir/java-only-convergence.tsv"; then
        cat "$evidence_dir/java-p11-mixed.tsv" "$evidence_dir/java-p11-mixed.stderr" 2>/dev/null >&2 || true
        exit 1
    fi

    echo "Joining Rust Push consumer to the Java-owned shared group"
    "$repo_root/target/debug/live_client" p11-mixed-group "$namesrv_addr" "$topic" \
        "$consumer_group" "$rust_ownership_file" "$rust_stop_file" "$rust_exited_file" \
        >"$evidence_dir/rust-p11-mixed.tsv" 2>"$evidence_dir/rust-p11-mixed.stderr" &
    p11_rust_pid="$!"
    if ! python3 "$repo_root/tests/differential/check_mixed_ownership.py" \
        joined "$topic" "$java_ownership_file" "$rust_ownership_file" \
        "$java_exited_file" 90 2>&1 | tee "$evidence_dir/join-convergence.tsv"; then
        cat "$evidence_dir/java-p11-mixed.tsv" "$evidence_dir/rust-p11-mixed.tsv" \
            "$evidence_dir/java-p11-mixed.stderr" "$evidence_dir/rust-p11-mixed.stderr" 2>/dev/null >&2 || true
        exit 1
    fi

    : >"$java_stop_file"
    if ! wait "$p11_java_pid"; then
        p11_java_pid=""
        cat "$evidence_dir/java-p11-mixed.tsv" "$evidence_dir/java-p11-mixed.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    p11_java_pid=""
    : >"$java_exited_file"
    if ! python3 "$repo_root/tests/differential/check_mixed_ownership.py" \
        rust-only "$topic" "$java_ownership_file" "$rust_ownership_file" \
        "$java_exited_file" 90 2>&1 | tee "$evidence_dir/leave-convergence.tsv"; then
        cat "$evidence_dir/rust-p11-mixed.tsv" "$evidence_dir/rust-p11-mixed.stderr" 2>/dev/null >&2 || true
        exit 1
    fi

    : >"$rust_stop_file"
    if ! wait "$p11_rust_pid"; then
        p11_rust_pid=""
        cat "$evidence_dir/rust-p11-mixed.tsv" "$evidence_dir/rust-p11-mixed.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    p11_rust_pid=""
    printf 'PASS P11-V03: Java and Rust Push consumers converged from single-member to disjoint shared ownership and back to Rust ownership of all eight queues within 90 seconds; transient redelivery was allowed\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P11-V04" ]]; then
    p11_rust_ready="$evidence_dir/rust-push-ready"
    p11_java_ready="$evidence_dir/java-push-ready"
    p11_java_to_rust_body="p11-java-to-rust-retry"
    p11_rust_to_java_body="p11-rust-to-java-retry"
    echo "Starting Rust Push on Broker A's queue, then sending a Java message that retries once"
    rm -f "$p11_rust_ready" "$p11_java_ready"
    "$repo_root/target/debug/live_client" p11-push "$namesrv_addr" "$topic" \
        "$consumer_group" "$p11_java_to_rust_body" "$p11_rust_ready" \
        >"$evidence_dir/rust-push.tsv" 2>"$evidence_dir/rust-push.stderr" &
    p11_rust_pid="$!"
    for _ in {1..1000}; do
        if [[ -s "$p11_rust_ready" ]]; then
            break
        fi
        if ! kill -0 "$p11_rust_pid" 2>/dev/null; then
            wait "$p11_rust_pid" 2>/dev/null || true
            p11_rust_pid=""
            cat "$evidence_dir/rust-push.tsv" "$evidence_dir/rust-push.stderr" 2>/dev/null >&2 || true
            exit 1
        fi
        sleep 0.1
    done
    if [[ ! -s "$p11_rust_ready" ]]; then
        echo "Rust Push consumer did not become ready for the Java-produced message" >&2
        exit 1
    fi
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="send $namesrv_addr $topic broker-a 0 $p11_java_to_rust_body" \
        exec:java@live-push-interop-probe \
        >"$evidence_dir/java-producer.tsv" 2>"$evidence_dir/java-producer.stderr"; then
        cat "$evidence_dir/java-producer.tsv" "$evidence_dir/java-producer.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! wait "$p11_rust_pid"; then
        p11_rust_pid=""
        cat "$evidence_dir/rust-push.tsv" "$evidence_dir/rust-push.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    p11_rust_pid=""

    query_p11_offset() {
        local broker_name="$1"
        local broker_address="$2"
        local suffix="$3"
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="query $broker_address $consumer_group $topic 0" \
            exec:java@live-consumer-probe \
            >"$evidence_dir/java-offset-$suffix.tsv" 2>"$evidence_dir/java-offset-$suffix.stderr"; then
            cat "$evidence_dir/java-offset-$suffix.tsv" "$evidence_dir/java-offset-$suffix.stderr" 2>/dev/null >&2 || true
            return 1
        fi
        if ! "$repo_root/target/debug/live_client" offset-query-named "$broker_address" \
            "$broker_name" "$consumer_group" "$topic" 0 \
            >"$evidence_dir/rust-offset-$suffix.tsv" 2>"$evidence_dir/rust-offset-$suffix.stderr"; then
            cat "$evidence_dir/rust-offset-$suffix.tsv" "$evidence_dir/rust-offset-$suffix.stderr" 2>/dev/null >&2 || true
            return 1
        fi
    }
    if ! query_p11_offset broker-a 127.0.0.1:50911 broker-a; then
        exit 1
    fi

    echo "Starting Java Push, then sending a Rust message to Broker B's queue"
    mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="consume $namesrv_addr $topic $consumer_group $p11_rust_to_java_body $p11_java_ready" \
        exec:java@live-push-interop-probe \
        >"$evidence_dir/java-push.tsv" 2>"$evidence_dir/java-push.stderr" &
    p11_java_pid="$!"
    for _ in {1..1000}; do
        if [[ -s "$p11_java_ready" ]]; then
            break
        fi
        if ! kill -0 "$p11_java_pid" 2>/dev/null; then
            wait "$p11_java_pid" 2>/dev/null || true
            p11_java_pid=""
            cat "$evidence_dir/java-push.tsv" "$evidence_dir/java-push.stderr" 2>/dev/null >&2 || true
            exit 1
        fi
        sleep 0.1
    done
    if [[ ! -s "$p11_java_ready" ]]; then
        echo "Java Push consumer did not become ready for the Rust-produced message" >&2
        exit 1
    fi
    if ! "$repo_root/target/debug/live_client" p11-producer "$namesrv_addr" "$topic" \
        compat_p11_rust_producer "$p11_rust_to_java_body" \
        >"$evidence_dir/rust-producer.tsv" 2>"$evidence_dir/rust-producer.stderr"; then
        cat "$evidence_dir/rust-producer.tsv" "$evidence_dir/rust-producer.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! wait "$p11_java_pid"; then
        p11_java_pid=""
        cat "$evidence_dir/java-push.tsv" "$evidence_dir/java-push.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    p11_java_pid=""
    if ! query_p11_offset broker-b 127.0.0.1:50921 broker-b; then
        exit 1
    fi
    if ! python3 "$repo_root/tests/differential/check_p11_push.py" "$evidence_dir" \
        "$topic" "$p11_java_to_rust_body" "$p11_rust_to_java_body"; then
        exit 1
    fi
    printf 'PASS P11-V04: Java producer to Rust Push and Rust producer to Java Push each retried once, committed matching Broker offsets, and completed three shutdown cycles without retained scheduler tasks\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P12-V03" ]]; then
    marker_dir="$evidence_dir/markers"
    java_ownership="$evidence_dir/java-orderly-ownership.txt"
    rust_ownership="$evidence_dir/rust-orderly-ownership.txt"
    java_ready="$evidence_dir/java-orderly-ready"
    rust_ready="$evidence_dir/rust-orderly-ready"
    java_stop="$evidence_dir/java-orderly-stop"
    rust_stop="$evidence_dir/rust-orderly-stop"
    rust_exited="$evidence_dir/rust-orderly-exited"
    release_gate="$evidence_dir/java-slow-callback-release"
    mkdir -p "$marker_dir"
    rm -f "$java_ownership" "$rust_ownership" "$java_ready" "$rust_ready" \
        "$java_stop" "$rust_stop" "$rust_exited" "$release_gate"

    echo "Initializing the two Broker offsets and seeding one slow plus one following message per queue"
    : >"$evidence_dir/seed.log"
    for broker_target in "broker-a:127.0.0.1:50911:a" "broker-b:127.0.0.1:50921:b"; do
        broker_name="${broker_target%%:*}"
        broker_tail="${broker_target#*:}"
        broker_address="${broker_tail%:*}"
        suffix="${broker_tail##*:}"
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="update $broker_address $consumer_group $topic 0 0" \
            exec:java@live-consumer-probe >>"$evidence_dir/seed.log" 2>&1; then
            cat "$evidence_dir/seed.log" >&2
            exit 1
        fi
        for message_body in "p12-slow-$suffix" "p12-next-$suffix"; do
            if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
                -Dexec.args="seed $namesrv_addr $topic $broker_name 0 $message_body" \
                exec:java@live-orderly-interop-probe >>"$evidence_dir/seed.log" 2>&1; then
                cat "$evidence_dir/seed.log" >&2
                exit 1
            fi
        done
    done

    echo "Starting Java orderly ownership and holding its callbacks before Rust joins"
    JAVA_TOOL_OPTIONS="-Drocketmq.client.unlockDelayTimeMills=1000" \
        mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="consumer $namesrv_addr $topic $consumer_group $java_ownership $java_ready $java_stop $marker_dir $release_gate" \
        exec:java@live-orderly-interop-probe \
        >"$evidence_dir/java-p12-orderly.tsv" 2>"$evidence_dir/java-p12-orderly.stderr" &
    p12_java_pid="$!"
    for _ in {1..900}; do
        if [[ -s "$java_ready" ]]; then
            break
        fi
        if ! kill -0 "$p12_java_pid" 2>/dev/null; then
            wait "$p12_java_pid" 2>/dev/null || true
            p12_java_pid=""
            cat "$evidence_dir/java-p12-orderly.tsv" "$evidence_dir/java-p12-orderly.stderr" 2>/dev/null >&2 || true
            exit 1
        fi
        sleep 0.1
    done
    if [[ ! -s "$java_ready" ]]; then
        echo "Java orderly consumer did not become ready" >&2
        exit 1
    fi
    if ! python3 "$repo_root/tests/differential/check_p12_orderly.py" held \
        "$marker_dir" "$java_ownership" "$topic" 45 \
        >"$evidence_dir/java-lock-contention.tsv" 2>&1; then
        cat "$evidence_dir/java-lock-contention.tsv" "$evidence_dir/java-p12-orderly.tsv" \
            "$evidence_dir/java-p12-orderly.stderr" >&2
        exit 1
    fi
    for broker_target in "broker-a:127.0.0.1:50911" "broker-b:127.0.0.1:50921"; do
        broker_name="${broker_target%%:*}"
        broker_address="${broker_target#*:}"
        if ! "$repo_root/target/debug/live_client" offset-query-named "$broker_address" \
            "$broker_name" "$consumer_group" "$topic" 0 \
            >"$evidence_dir/offset-before-rust-$broker_name.tsv" \
            2>"$evidence_dir/offset-before-rust-$broker_name.stderr" \
            || ! python3 "$repo_root/tests/differential/check_p12_orderly.py" offset \
                "$evidence_dir/offset-before-rust-$broker_name.tsv" 0 \
                >"$evidence_dir/offset-before-rust-$broker_name-check.tsv" 2>&1; then
            cat "$evidence_dir/offset-before-rust-$broker_name.tsv" \
                "$evidence_dir/offset-before-rust-$broker_name.stderr" \
                "$evidence_dir/offset-before-rust-$broker_name-check.tsv" >&2 2>/dev/null || true
            exit 1
        fi
    done

    echo "Starting Rust orderly consumer; the older Java callbacks remain gated"
    env rocketmq.client.unlockDelayTimeMills=1000 \
        "$repo_root/target/debug/live_client" p12-orderly "$namesrv_addr" "$topic" \
        "$consumer_group" "$rust_ownership" "$rust_ready" "$rust_stop" "$rust_exited" "$marker_dir" \
        >"$evidence_dir/rust-p12-orderly.tsv" 2>"$evidence_dir/rust-p12-orderly.stderr" &
    p12_rust_pid="$!"
    for _ in {1..900}; do
        if [[ -s "$rust_ready" ]]; then
            break
        fi
        if ! kill -0 "$p12_rust_pid" 2>/dev/null; then
            wait "$p12_rust_pid" 2>/dev/null || true
            p12_rust_pid=""
            cat "$evidence_dir/rust-p12-orderly.tsv" "$evidence_dir/rust-p12-orderly.stderr" 2>/dev/null >&2 || true
            exit 1
        fi
        sleep 0.1
    done
    if [[ ! -s "$rust_ready" ]]; then
        echo "Rust orderly consumer did not become ready" >&2
        exit 1
    fi
    if ! python3 "$repo_root/tests/differential/check_p12_orderly.py" handoff \
        "$marker_dir" "$rust_ownership" "$topic" 90 \
        >"$evidence_dir/slow-callback-handoff.tsv" 2>&1; then
        cat "$evidence_dir/slow-callback-handoff.tsv" "$evidence_dir/java-p12-orderly.tsv" \
            "$evidence_dir/rust-p12-orderly.tsv" "$evidence_dir/java-p12-orderly.stderr" \
            "$evidence_dir/rust-p12-orderly.stderr" >&2
        exit 1
    fi
    handoff_broker="$(awk -F '\t' '/^P12\tHANDOFF/ { split($3, queue, ":"); print queue[1] }' "$evidence_dir/slow-callback-handoff.tsv")"
    case "$handoff_broker" in
        broker-a) handoff_address=127.0.0.1:50911 ;;
        broker-b) handoff_address=127.0.0.1:50921 ;;
        *) echo "P12 handoff checker did not report a Broker queue" >&2; exit 1 ;;
    esac

    query_p12_offset() {
        local broker_name="$1"
        local address="$2"
        local output="$3"
        "$repo_root/target/debug/live_client" offset-query-named "$address" "$broker_name" \
            "$consumer_group" "$topic" 0 >"$output" 2>"${output%.tsv}.stderr"
    }
    wait_p12_offset() {
        local broker_name="$1"
        local address="$2"
        local expected="$3"
        local suffix="$4"
        local output="$evidence_dir/offset-$suffix.tsv"
        for _ in {1..80}; do
            if query_p12_offset "$broker_name" "$address" "$output" \
                && python3 "$repo_root/tests/differential/check_p12_orderly.py" offset "$output" "$expected" \
                    >"$evidence_dir/offset-$suffix-check.tsv" 2>&1; then
                return 0
            fi
            sleep 0.5
        done
        cat "$output" "${output%.tsv}.stderr" "$evidence_dir/offset-$suffix-check.tsv" >&2 2>/dev/null || true
        echo "P12 Broker offset did not converge to $expected for $broker_name:0" >&2
        return 1
    }
    if ! wait_p12_offset "$handoff_broker" "$handoff_address" 2 rust-owner-committed; then
        exit 1
    fi

    echo "Stopping Rust while Java's old callbacks remain gated, so a later stale commit cannot be masked by Rust"
    : >"$rust_stop"
    if ! wait "$p12_rust_pid"; then
        p12_rust_pid=""
        cat "$evidence_dir/rust-p12-orderly.tsv" "$evidence_dir/rust-p12-orderly.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    p12_rust_pid=""
    if [[ ! -s "$rust_exited" ]]; then
        echo "Rust orderly consumer did not complete shutdown" >&2
        exit 1
    fi
    if ! python3 "$repo_root/tests/differential/check_p12_orderly.py" java-still-held "$marker_dir" \
        >"$evidence_dir/java-still-held.tsv" 2>&1; then
        cat "$evidence_dir/java-still-held.tsv" "$evidence_dir/java-p12-orderly.tsv" \
            "$evidence_dir/java-p12-orderly.stderr" >&2
        exit 1
    fi

    echo "Releasing Java's obsolete callbacks and checking the Broker offsets after both consumers stop"
    : >"$release_gate"
    if ! python3 "$repo_root/tests/differential/check_p12_orderly.py" java-ended "$marker_dir" 30 \
        >"$evidence_dir/java-callback-release.tsv" 2>&1; then
        cat "$evidence_dir/java-callback-release.tsv" "$evidence_dir/java-p12-orderly.tsv" \
            "$evidence_dir/rust-p12-orderly.tsv" >&2
        exit 1
    fi
    : >"$java_stop"
    if ! wait "$p12_java_pid"; then
        p12_java_pid=""
        cat "$evidence_dir/java-p12-orderly.tsv" "$evidence_dir/java-p12-orderly.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    p12_java_pid=""
    if [[ ! -s "$evidence_dir/java-exited" ]]; then
        echo "Java orderly consumer did not complete shutdown" >&2
        exit 1
    fi
    if ! wait_p12_offset broker-a 127.0.0.1:50911 2 broker-a-final; then
        exit 1
    fi
    if ! wait_p12_offset broker-b 127.0.0.1:50921 2 broker-b-final; then
        exit 1
    fi

    wait_for_p12_lock() {
        local broker_name="$1"
        local address="$2"
        local lock_output="$evidence_dir/lock-$broker_name.tsv"
        local unlock_output="$evidence_dir/unlock-$broker_name.tsv"
        for _ in {1..40}; do
            if "$repo_root/target/debug/live_client" lock "$address" "$consumer_group" \
                p12-cleanup-probe "$topic" "$broker_name" 0 >"$lock_output" 2>"${lock_output%.tsv}.stderr" \
                && rg -q '^LOCK\ttrue\t' "$lock_output"; then
                "$repo_root/target/debug/live_client" unlock "$address" "$consumer_group" \
                    p12-cleanup-probe "$topic" "$broker_name" 0 >"$unlock_output" 2>"${unlock_output%.tsv}.stderr" \
                    || return 1
                rg -q '^UNLOCK\tOK$' "$unlock_output" && return 0
            fi
            sleep 1
        done
        cat "$lock_output" "${lock_output%.tsv}.stderr" "$unlock_output" "${unlock_output%.tsv}.stderr" >&2 2>/dev/null || true
        echo "P12 Broker lock did not converge after consumer shutdown for $broker_name:0" >&2
        return 1
    }
    wait_for_p12_lock broker-a 127.0.0.1:50911 || exit 1
    wait_for_p12_lock broker-b 127.0.0.1:50921 || exit 1
    printf 'PASS P12-V03: Java held orderly callbacks on both Brokers while Rust joined; Rust acquired the transferred Broker lock, processed the redelivered message and its successor, and advanced the offset to 2 before Java returned; stale completion did not regress either offset, both consumers shut down, and both Broker queues became lockable and releasable\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P14-V03" ]]; then
    p14_pull_output="$evidence_dir/p14-pull.tsv"
    p14_expected_state() {
        case "$1" in
            commit) printf 'COMMIT_MESSAGE' ;;
            rollback) printf 'ROLLBACK_MESSAGE' ;;
            unknown) printf 'UNKNOW' ;;
        esac
    }
    p14_broker_address() {
        case "$1" in
            broker-a) printf '127.0.0.1:50911' ;;
            broker-b) printf '127.0.0.1:50921' ;;
            *) return 1 ;;
        esac
    }
    p14_validate_transaction() {
        local output="$1" decision="$2" key="$3" language="$4" expected_state
        expected_state="$(p14_expected_state "$decision")"
        python3 - "$output" "$expected_state" "$key" "$decision" "$language" <<'PY'
import sys
from pathlib import Path

path, expected_state, expected_key, decision, language = sys.argv[1:]
rows = [line.split("\t") for line in Path(path).read_text().splitlines()]
transactions = [row for row in rows if row[:2] == ["P14", "TX"]]
if len(transactions) != 1:
    raise SystemExit(f"expected one P14 transaction result, got {transactions!r}")
row = transactions[0]
if len(row) != 8 or row[2] != "SEND_OK" or row[3] != expected_state or row[4] != expected_key:
    raise SystemExit(f"transaction result differs from {decision}: {row!r}")
if row[5] not in ("broker-a", "broker-b") or row[6] != "0":
    raise SystemExit(f"transaction result does not identify a configured one-queue Broker: {row!r}")
if decision == "unknown":
    checks = [row for row in rows if row[:2] == ["P14", "CHECK"]]
    if len(checks) != 1 or len(checks[0]) != 5 or checks[0][2] != "COMMIT":
        raise SystemExit(f"Unknown did not resolve through a Broker check: {checks!r}")
    if not checks[0][3].isdecimal() or int(checks[0][3]) < 1:
        raise SystemExit(f"Unknown check callback was not recorded: {checks[0]!r}")
    if not checks[0][4].isdecimal() or int(checks[0][4]) > 20_000:
        raise SystemExit(f"Unknown check exceeded the configured bound: {checks[0]!r}")
if language == "rust":
    hooks = [row for row in rows if row[:2] == ["P14", "END_HOOK"]]
    expected_hook_address = {
        "broker-a": "127.0.0.1:50911",
        "broker-b": "127.0.0.1:50921",
    }[row[5]]
    if decision in ("commit", "rollback"):
        if (
            len(hooks) != 1
            or len(hooks[0]) != 7
            or hooks[0][2] != expected_hook_address
            or hooks[0][5] != expected_state
            or hooks[0][6] != "false"
        ):
            raise SystemExit(f"Rust local end-transaction hook differs from {decision}: {hooks!r}")
    else:
        local = [
            hook
            for hook in hooks
            if len(hook) == 7
            and hook[2] == expected_hook_address
            and hook[5] == "UNKNOW"
            and hook[6] == "false"
        ]
        checked = [
            hook
            for hook in hooks
            if len(hook) == 7
            and hook[2] == expected_hook_address
            and hook[5] == "COMMIT_MESSAGE"
            and hook[6] == "true"
        ]
        if len(hooks) != 2 or len(local) != 1 or len(checked) != 1:
            raise SystemExit(f"Rust Unknown did not reach both local and Broker-check end hooks: {hooks!r}")
PY
    }
    p14_run_producer() {
        local language="$1" decision="$2" key="$3" body="$4" output="$5"
        if [[ "$language" == "java" ]]; then
            mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
                -Dexec.args="p14 $namesrv_addr $topic $producer_group $decision $key $body" \
                exec:java@live-transaction-probe >"$output" 2>"${output%.tsv}.stderr"
        else
            "$repo_root/target/debug/live_client" p14-transaction "$namesrv_addr" "$topic" \
                "$producer_group" "$decision" "$key" "$body" \
                >"$output" 2>"${output%.tsv}.stderr"
        fi
    }
    p14_run_pull() {
        local language="$1" broker="$2" output="$3"
        local address
        address="$(p14_broker_address "$broker")" || return 1
        if [[ "$language" == "java" ]]; then
            mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
                -Dexec.args="pull $address $consumer_group $topic 0 0" \
                exec:java@live-consumer-probe >"$output" 2>"${output%.tsv}.stderr"
        else
            "$repo_root/target/debug/live_client" p14-pull "$address" "$broker" \
                "$consumer_group" "$topic" 0 0 >"$output" 2>"${output%.tsv}.stderr"
        fi
    }
    p14_pull_has_body() {
        python3 - "$1" "$2" <<'PY'
import sys
from pathlib import Path

row = Path(sys.argv[1]).read_text().rstrip("\n").split("\t")
body = sys.argv[2].encode().hex()
if len(row) != 7 or row[0] != "PULL":
    raise SystemExit(f"pull output has invalid shape: {row!r}")
if row[6] and (len(row[6]) % 2 or any(ch not in "0123456789abcdef" for ch in row[6])):
    raise SystemExit("pull output body is not lowercase hexadecimal")
raise SystemExit(0 if body in row[6] else 1)
PY
    }
    p14_wait_body() {
        local language="$1" broker="$2" body="$3" output="$4"
        for _ in {1..20}; do
            if p14_run_pull "$language" "$broker" "$output" && p14_pull_has_body "$output" "$body"; then
                return 0
            fi
            sleep 0.5
        done
        cat "$output" "${output%.tsv}.stderr" >&2 2>/dev/null || true
        echo "P14 committed message '$body' was not visible to the $language consumer on $broker:0" >&2
        return 1
    }
    p14_assert_no_body() {
        local language="$1" broker="$2" body="$3" output="$4"
        p14_run_pull "$language" "$broker" "$output" || return 1
        if p14_pull_has_body "$output" "$body"; then
            echo "P14 rollback message '$body' became visible to the $language consumer" >&2
            return 1
        fi
    }
    p14_current_max() {
        local broker_address="$1" label="$2" output="$evidence_dir/p14-max-$2.tsv" stderr="$evidence_dir/p14-max-$2.stderr"
        local offset
        "$repo_root/target/debug/live_client" max-offset "$broker_address" "$topic" 0 \
            >"$output" 2>"$stderr" || return 1
        offset="$(awk -F '\t' 'NF == 2 && $1 == "MAX_OFFSET" { print $2 }' "$output")"
        if [[ ! "$offset" =~ ^[0-9]+$ ]]; then
            cat "$output" "$stderr" >&2 2>/dev/null || true
            echo "P14 could not read a Broker queue max offset at $broker_address" >&2
            return 1
        fi
        printf '%s' "$offset"
    }
    p14_run_case() {
        local language="$1" decision="$2" key="$3" body="$4"
        local output="$evidence_dir/${language}-p14-${decision}.tsv"
        if ! p14_run_producer "$language" "$decision" "$key" "$body" "$output"; then
            cat "$output" "${output%.tsv}.stderr" 2>/dev/null >&2 || true
            return 1
        fi
        p14_validate_transaction "$output" "$decision" "$key" "$language" || return 1
        awk -F '\t' '$1 == "P14" && $2 == "TX" { print $6 }' "$output"
    }

    echo "Checking Java-to-Rust commit visibility, Rust-to-Java commit visibility, both rollbacks, and actual Broker-check resolution in both producer directions"
    java_commit_broker="$(p14_run_case java commit p14-java-commit-key p14-java-commit)" || exit 1
    p14_wait_body rust "$java_commit_broker" p14-java-commit "$evidence_dir/rust-p14-java-commit-pull.tsv" || exit 1
    rust_commit_broker="$(p14_run_case rust commit p14-rust-commit-key p14-rust-commit)" || exit 1
    p14_wait_body java "$rust_commit_broker" p14-rust-commit "$evidence_dir/java-p14-rust-commit-pull.tsv" || exit 1

    broker_a_max_before_rollback="$(p14_current_max 127.0.0.1:50911 before-broker-a)" || exit 1
    broker_b_max_before_rollback="$(p14_current_max 127.0.0.1:50921 before-broker-b)" || exit 1
    java_rollback_broker="$(p14_run_case java rollback p14-java-rollback-key p14-java-rollback)" || exit 1
    rust_rollback_broker="$(p14_run_case rust rollback p14-rust-rollback-key p14-rust-rollback)" || exit 1
    for rollback in "java:$java_rollback_broker:p14-java-rollback" "rust:$rust_rollback_broker:p14-rust-rollback"; do
        IFS=: read -r rollback_language rollback_broker rollback_body <<<"$rollback"
        rollback_address="$(p14_broker_address "$rollback_broker")" || exit 1
        p14_assert_no_body "$([[ "$rollback_language" == "java" ]] && printf rust || printf java)" \
            "$rollback_broker" "$rollback_body" "$evidence_dir/${rollback_language}-p14-rollback-pull.tsv" || exit 1
        max_after="$(p14_current_max "$rollback_address" "after-rollback-$rollback_broker")" || exit 1
        if [[ "$rollback_broker" == "broker-a" ]]; then
            max_before="$broker_a_max_before_rollback"
        else
            max_before="$broker_b_max_before_rollback"
        fi
        if [[ "$max_before" != "$max_after" ]]; then
            echo "P14 rollback advanced normal queue max offset on $rollback_broker from $max_before to $max_after" >&2
            exit 1
        fi
        printf 'P14\tROLLBACK_INVISIBLE\t%s\t%s\t%s\n' "$rollback_language" "$rollback_broker" "$max_after" \
            >>"$evidence_dir/rollback-visibility.tsv"
    done

    java_unknown_broker="$(p14_run_case java unknown p14-java-unknown-key p14-java-unknown)" || exit 1
    p14_wait_body rust "$java_unknown_broker" p14-java-unknown "$evidence_dir/rust-p14-java-unknown-pull.tsv" || exit 1
    rust_unknown_broker="$(p14_run_case rust unknown p14-rust-unknown-key p14-rust-unknown)" || exit 1
    p14_wait_body java "$rust_unknown_broker" p14-rust-unknown "$evidence_dir/java-p14-rust-unknown-pull.tsv" || exit 1
    printf 'PASS P14-V03: Java and Rust commits were visible to the opposite-language pull client; both rollback messages remained absent without advancing normal queue offsets; Unknown transactions were resolved by actual bounded Broker checks in both directions\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P10-V03" ]]; then
    p10_ready_file="$evidence_dir/rust-ready"
    p10_failure_go_file="$evidence_dir/seed-failure"
    echo "Seeding Java success/partial messages, then exercising the bounded Push retry and DLQ path"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="seed-p10 $namesrv_addr $topic initial" exec:java@live-high-level-producer-probe \
        >"$evidence_dir/java-p10-initial-seed.tsv" 2>"$evidence_dir/java-p10-initial-seed.stderr"; then
        cat "$evidence_dir/java-p10-initial-seed.tsv" "$evidence_dir/java-p10-initial-seed.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    rm -f "$p10_ready_file" "$p10_failure_go_file"
    "$repo_root/target/debug/live_client" p10-push "$namesrv_addr" "$topic" "$consumer_group" \
        "$p10_ready_file" "$p10_failure_go_file" \
        >"$evidence_dir/rust-p10-push.tsv" 2>"$evidence_dir/rust-p10-push.stderr" &
    p10_probe_pid="$!"
    for _ in {1..1000}; do
        if [[ -s "$p10_ready_file" ]]; then
            break
        fi
        if ! kill -0 "$p10_probe_pid" 2>/dev/null; then
            wait "$p10_probe_pid" 2>/dev/null || true
            p10_probe_pid=""
            cat "$evidence_dir/rust-p10-push.tsv" "$evidence_dir/rust-p10-push.stderr" 2>/dev/null >&2 || true
            exit 1
        fi
        sleep 0.1
    done
    if [[ ! -s "$p10_ready_file" ]]; then
        echo "Rust Push listener did not acknowledge the recovered partial batch" >&2
        kill "$p10_probe_pid" 2>/dev/null || true
        wait "$p10_probe_pid" 2>/dev/null || true
        p10_probe_pid=""
        cat "$evidence_dir/rust-p10-push.tsv" "$evidence_dir/rust-p10-push.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="seed-p10 $namesrv_addr $topic failure" exec:java@live-high-level-producer-probe \
        >"$evidence_dir/java-p10-failure-seed.tsv" 2>"$evidence_dir/java-p10-failure-seed.stderr"; then
        cat "$evidence_dir/java-p10-failure-seed.tsv" "$evidence_dir/java-p10-failure-seed.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    : >"$p10_failure_go_file"
    if ! wait "$p10_probe_pid"; then
        p10_probe_pid=""
        cat "$evidence_dir/rust-p10-push.tsv" "$evidence_dir/rust-p10-push.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    p10_probe_pid=""
    if ! python3 - "$evidence_dir" "$topic" "$consumer_group" "$p10_retry_max_times" <<'PY'
import sys
from pathlib import Path

root = Path(sys.argv[1])
topic = sys.argv[2]
group = sys.argv[3]
retry_limit = int(sys.argv[4])

def rows(name, prefix):
    result = []
    for line in (root / name).read_text().splitlines():
        fields = line.split("\t")
        if fields[:len(prefix)] == prefix:
            result.append(fields)
    return result

initial = rows("java-p10-initial-seed.tsv", ["P10", "PRODUCED"])
expected_initial = [
    ["P10", "PRODUCED", "initial", "p10-success", "p10-success-key", "broker-a", "0"],
    ["P10", "PRODUCED", "initial", "p10-partial-ack", "p10-partial-ack-key", "broker-a", "0"],
    ["P10", "PRODUCED", "initial", "p10-partial-retry", "p10-partial-retry-key", "broker-a", "0"],
]
if initial != expected_initial:
    raise SystemExit(f"Java initial P10 seed rows differ: {initial!r}")
failure_seed = rows("java-p10-failure-seed.tsv", ["P10", "PRODUCED"])
expected_failure = [["P10", "PRODUCED", "failure", "p10-failure-dlq", "p10-failure-dlq-key", "broker-a", "0"]]
if failure_seed != expected_failure:
    raise SystemExit(f"Java P10 failure seed row differs: {failure_seed!r}")

observed = rows("rust-p10-push.tsv", ["P10", "CALLBACK"])
counts = {}
for row in observed:
    if len(row) != 5:
        raise SystemExit(f"malformed Rust Push callback row: {row!r}")
    counts.setdefault(row[2], []).append(int(row[3]))
expected_counts = {
    "p10-success": [0],
    "p10-partial-ack": [0],
    "p10-partial-retry": [0, 1],
    "p10-failure-dlq": [0, 1],
}
if counts != expected_counts:
    raise SystemExit(f"Push callback/retry counts differ: {counts!r}")
if rows("rust-p10-push.tsv", ["P10", "READY"]) != [["P10", "READY", "FIXED", "broker-a", "0"]]:
    raise SystemExit("test-only fixed assignment was not pinned to broker-a queue 0")
partial = rows("rust-p10-push.tsv", ["P10", "BATCH", "PARTIAL"])
if partial != [["P10", "BATCH", "PARTIAL", "p10-success,p10-partial-ack,p10-partial-retry", "0,0,0", "1"]]:
    raise SystemExit(f"partial acknowledgement boundary differs: {partial!r}")
recovered = rows("rust-p10-push.tsv", ["P10", "BATCH", "SUCCESS"])
if [row[3:5] for row in recovered] != [["p10-partial-retry", "1"]]:
    raise SystemExit(f"recovered partial message was not acknowledged: {recovered!r}")
failures = rows("rust-p10-push.tsv", ["P10", "BATCH", "RECONSUME_LATER"])
if [row[3:5] for row in failures] != [["p10-failure-dlq", "0"], ["p10-failure-dlq", "1"]]:
    raise SystemExit(f"failure callbacks did not exhaust exactly one retry: {failures!r}")
dlq = rows("rust-p10-push.tsv", ["P10", "DLQ"])
expected_dlq_topic = f"%DLQ%{group}"
if dlq != [["P10", "DLQ", expected_dlq_topic, "p10-failure-dlq", str(retry_limit + 1), "p10-failure-dlq-key"]]:
    raise SystemExit(f"configured DLQ result differs: {dlq!r}")
if rows("rust-p10-push.tsv", ["P10", "PASS"]) != [["P10", "PASS", "LISTENER_SUCCESS_PARTIAL_FAILURE", "RETRIES=1", "DLQ=FOUND"]]:
    raise SystemExit("Rust Push consumer did not report the complete P10 result")
PY
    then
        echo "P10-V03 listener, retry count, or DLQ assertions failed" >&2
        exit 1
    fi
    printf 'PASS P10-V03: Java messages exercised Rust Push success, partial acknowledgement and failure; one retry reached the configured DLQ with matching body and key\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P13-V02" ]]; then
    echo "Seeding queue 0/1 and exercising both Java and Rust POP receipt lifecycles"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="prepare $broker_addr $topic java" exec:java@live-p13-pop-probe \
        >"$evidence_dir/java-p13-pop-prepare.tsv" 2>"$evidence_dir/java-p13-pop-prepare.stderr"; then
        cat "$evidence_dir/java-p13-pop-prepare.tsv" "$evidence_dir/java-p13-pop-prepare.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! "$repo_root/target/debug/live_client" p13-pop-prepare "$broker_addr" "$topic" rust \
        >"$evidence_dir/rust-p13-pop-prepare.tsv" 2>"$evidence_dir/rust-p13-pop-prepare.stderr"; then
        cat "$evidence_dir/rust-p13-pop-prepare.tsv" "$evidence_dir/rust-p13-pop-prepare.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="seed $broker_addr $namesrv_addr $topic java" exec:java@live-p13-pop-probe \
        >"$evidence_dir/java-p13-pop-seed.tsv" 2>"$evidence_dir/java-p13-pop-seed.stderr"; then
        cat "$evidence_dir/java-p13-pop-seed.tsv" "$evidence_dir/java-p13-pop-seed.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="consume $broker_addr $topic java" exec:java@live-p13-pop-probe \
        >"$evidence_dir/java-p13-pop-consume.tsv" 2>"$evidence_dir/java-p13-pop-consume.stderr"; then
        cat "$evidence_dir/java-p13-pop-consume.tsv" "$evidence_dir/java-p13-pop-consume.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! "$repo_root/target/debug/live_client" p13-pop-consume "$broker_addr" "$topic" rust \
        >"$evidence_dir/rust-p13-pop-consume.tsv" 2>"$evidence_dir/rust-p13-pop-consume.stderr"; then
        cat "$evidence_dir/rust-p13-pop-consume.tsv" "$evidence_dir/rust-p13-pop-consume.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! python3 - "$evidence_dir" <<'PY'
import sys
from pathlib import Path

root = Path(sys.argv[1])
expected_seed = [
    ["P13", "seed", "p13-java-q0", "0", "0", "1"],
    ["P13", "seed", "p13-java-q1", "1", "0", "1"],
]
seed = [line.split("\t") for line in (root / "java-p13-pop-seed.tsv").read_text().splitlines()]
seed = [row for row in seed if len(row) >= 2 and row[:2] == ["P13", "seed"]]
if seed != expected_seed:
    raise SystemExit(f"Java seed rows differ: {seed!r}")
for language in ("java", "rust"):
    prepare = [line.split("\t") for line in (root / f"{language}-p13-pop-prepare.tsv").read_text().splitlines()]
    prepare = [row for row in prepare if len(row) >= 2 and row[:2] == ["P13", "prepare"]]
    expected_prepare = [["P13", "prepare", f"compat_p13_pop_{language}_{suffix}", "POP"] for suffix in ("expiry", "change", "mixed")]
    if prepare != expected_prepare:
        raise SystemExit(f"{language} POP mode setup rows differ: {prepare!r}")
expected = {
    "expiry-redelivery": ["P13", "expiry-redelivery", "OK", "0"],
    "change-invisibility": ["P13", "change-invisibility", "OK"],
    "replacement-receipt": ["P13", "replacement-receipt", "OK", "ACKED"],
    "mixed-queue-mapping": ["P13", "mixed-queue-mapping", "OK", "0,1"],
    "malformed-extra-info-rejected": ["P13", "malformed-extra-info-rejected", "OK"],
    "pop-delivery": ["P13", "pop-delivery", "OK", "5"],
    "ack": ["P13", "ack", "OK", "4"],
}
normalized = []
for language in ("java", "rust"):
    rows = [line.split("\t") for line in (root / f"{language}-p13-pop-consume.tsv").read_text().splitlines()]
    by_name = {row[1]: row for row in rows if len(row) >= 2 and row[0] == "P13" and row[1] in expected}
    if len(by_name) != len(expected) or by_name != expected:
        raise SystemExit(f"{language} lifecycle rows differ: {rows!r}")
    normalized.append(by_name)
if normalized[0] != normalized[1]:
    raise SystemExit("Java and Rust lifecycle rows differ")
PY
    then
        echo "Java/Rust POP receipt lifecycle outcomes differ" >&2
        exit 1
    fi
    printf 'PASS P13-V02: Java and Rust matched POP delivery, expiry redelivery, ACK, visibility change, replacement receipt, mixed queue mapping, and malformed receipt rejection\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "PROD-005-send-timeout" ]]; then
    echo "Delaying each direct Broker response beyond the Java/Rust 100 ms send deadline"
    start_timeout_proxy

    start_timeout_client java compat_timeout_java timeout-java-body \
        "$evidence_dir/java-timeout.tsv" "$evidence_dir/java-go"
    release_timeout_client "$evidence_dir/java-go"
    wait_for_timeout_proxy_delays 1

    start_timeout_client rust compat_timeout_rust timeout-rust-body \
        "$evidence_dir/rust-timeout.tsv" "$evidence_dir/rust-go"
    release_timeout_client "$evidence_dir/rust-go"
    wait_for_timeout_proxy_delays 2

    if ! "$repo_root/target/debug/live_client" max-offset "$broker_addr" "$topic" 0 \
        >"$evidence_dir/max-offset.tsv" 2>"$evidence_dir/max-offset.stderr"; then
        cat "$evidence_dir/max-offset.tsv" "$evidence_dir/max-offset.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="pull $broker_addr compat_timeout_readback $topic 0 0" \
        exec:java@live-consumer-probe >"$evidence_dir/java-pull.tsv" \
        2>"$evidence_dir/java-pull.stderr"; then
        cat "$evidence_dir/java-pull.tsv" "$evidence_dir/java-pull.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! "$repo_root/target/debug/live_client" pull "$broker_addr" \
        compat_timeout_readback "$topic" 0 0 >"$evidence_dir/rust-pull.tsv" \
        2>"$evidence_dir/rust-pull.stderr"; then
        cat "$evidence_dir/rust-pull.tsv" "$evidence_dir/rust-pull.stderr" 2>/dev/null >&2 || true
        exit 1
    fi

    if ! python3 - "$timeout_ms" "$evidence_dir/java-timeout.tsv" \
        "$evidence_dir/rust-timeout.tsv" "$evidence_dir/proxy.tsv" \
        "$evidence_dir/max-offset.tsv" "$evidence_dir/java-pull.tsv" \
        "$evidence_dir/rust-pull.tsv" <<'PY'
import sys
from pathlib import Path

timeout_ms = int(sys.argv[1])
java_path, rust_path, proxy_path, max_offset_path, java_pull_path, rust_pull_path = sys.argv[2:]

def lines(path):
    return [line.split("\t") for line in Path(path).read_text().splitlines()]

def check_timeout(path, language, body, expected_offset):
    rows = lines(path)
    ready = [row for row in rows if row[:3] == ["TIMEOUT", "READY", language]]
    result = [row for row in rows if row[:4] == ["TIMEOUT", "RESULT", language, "TIMEOUT"]]
    if len(ready) != 1 or len(ready[0]) != 4 or ready[0][3] != str(expected_offset):
        raise SystemExit(f"{path}: client did not prime the expected starting offset {expected_offset}")
    if len(result) != 1 or len(result[0]) != 7 or not result[0][4]:
        raise SystemExit(f"{path}: expected one typed timeout outcome")
    elapsed = int(result[0][5])
    if elapsed < timeout_ms or elapsed >= 2_000 or result[0][6] != str(expected_offset):
        raise SystemExit(f"{path}: timeout duration or starting queue offset differs: {result[0]!r}")
    return body.encode().hex()

java_body = check_timeout(java_path, "java", "timeout-java-body", 0)
rust_body = check_timeout(rust_path, "rust", "timeout-rust-body", 1)
proxy_rows = lines(proxy_path)
delayed = [row for row in proxy_rows if row[:2] == ["PROXY", "DELAY_DONE"]]
if len(delayed) != 2:
    raise SystemExit(f"proxy did not delay exactly two Broker responses: {delayed!r}")
if lines(max_offset_path) != [["MAX_OFFSET", "2"]]:
    raise SystemExit("expected exactly two queue entries after the two timed-out sends")

java_pull, rust_pull = lines(java_pull_path), lines(rust_pull_path)
if len(java_pull) != 1 or len(rust_pull) != 1 or len(java_pull[0]) != 7 or len(rust_pull[0]) != 7:
    raise SystemExit("Java and Rust pull probes must each return one complete result")
for path, row in ((java_pull_path, java_pull[0]), (rust_pull_path, rust_pull[0])):
    if row[0] != "PULL" or row[1] != "FOUND" or row[2] != "2" or row[4] != "2":
        raise SystemExit(f"{path}: pull did not observe exactly two committed messages: {row!r}")
    raw = row[6].lower()
    if raw.count(java_body) != 1 or raw.count(rust_body) != 1:
        raise SystemExit(f"{path}: timed-out send bodies were missing or duplicated")
if java_pull[0] != rust_pull[0]:
    raise SystemExit("Java and Rust pull results differ for the messages committed after timeout")
PY
    then
        echo "Java/Rust producer timeout or duplicate-delivery relationship differs" >&2
        exit 1
    fi
    printf 'PASS PROD-005-send-timeout: Java and Rust each timed out at 100 ms; both sends were committed exactly once and matched on pull readback\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "HEARTBEAT-001" ]]; then
    echo "Sending the same heartbeat through both client APIs"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="$broker_addr $producer_group" exec:java@live-heartbeat-probe \
        >"$evidence_dir/java-heartbeat.tsv" 2>"$evidence_dir/java-heartbeat.stderr"; then
        cat "$evidence_dir/java-heartbeat.tsv" "$evidence_dir/java-heartbeat.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- heartbeat "$broker_addr" "$producer_group" \
        >"$evidence_dir/rust-heartbeat.tsv" 2>"$evidence_dir/rust-heartbeat.stderr"; then
        cat "$evidence_dir/rust-heartbeat.stderr" >&2
        exit 1
    fi
    if [[ ! -s "$evidence_dir/java-heartbeat.tsv" || ! -s "$evidence_dir/rust-heartbeat.tsv" ]]; then
        echo "A client produced empty heartbeat evidence" >&2
        exit 1
    fi
    if ! cmp -s "$evidence_dir/java-heartbeat.tsv" "$evidence_dir/rust-heartbeat.tsv"; then
        diff -u "$evidence_dir/java-heartbeat.tsv" "$evidence_dir/rust-heartbeat.tsv" \
            >"$evidence_dir/heartbeat-diff.txt" || true
        cat "$evidence_dir/heartbeat-diff.txt" >&2
        exit 1
    fi
    if ! awk -F '\t' \
        'NF != 3 || $1 != "HEARTBEAT" || $2 !~ /^[0-9]+$/ || $3 !~ /^[[:xdigit:]]+$/ || length($3) % 2 { exit 1 }
         END { if (NR != 1) exit 1 }' \
        "$evidence_dir/java-heartbeat.tsv"; then
        echo "Heartbeat evidence must contain one response version and encoded request body" >&2
        exit 1
    fi
    printf 'PASS %s: matching response version and encoded request body\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "CONS-001-query-offset" || "$scenario_id" == "CONS-004-update-offset" ]]; then
    echo "Writing the Java offset, querying with Rust, then reversing the client direction"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="update $broker_addr $producer_group $topic $queue_id $java_offset" \
        exec:java@live-consumer-probe \
        >"$evidence_dir/java-offset-update.tsv" 2>"$evidence_dir/java-offset-update.stderr"; then
        cat "$evidence_dir/java-offset-update.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v offset="$java_offset" \
        'NF != 2 || $1 != "OFFSET_UPDATED" || $2 != offset { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/java-offset-update.tsv"; then
        echo "Java consumer-offset update returned an unexpected result" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        offset-query "$broker_addr" "$producer_group" "$topic" "$queue_id" \
        >"$evidence_dir/rust-offset-query.tsv" 2>"$evidence_dir/rust-offset-query.stderr"; then
        cat "$evidence_dir/rust-offset-query.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v offset="$java_offset" \
        'NF != 2 || $1 != "OFFSET" || $2 != offset { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/rust-offset-query.tsv"; then
        echo "Rust did not read the offset written by Java" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        offset-update "$broker_addr" "$producer_group" "$topic" "$queue_id" "$rust_offset" \
        >"$evidence_dir/rust-offset-update.tsv" 2>"$evidence_dir/rust-offset-update.stderr"; then
        cat "$evidence_dir/rust-offset-update.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v offset="$rust_offset" \
        'NF != 2 || $1 != "OFFSET_UPDATED" || $2 != offset { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/rust-offset-update.tsv"; then
        echo "Rust consumer-offset update returned an unexpected result" >&2
        exit 1
    fi
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="query $broker_addr $producer_group $topic $queue_id" \
        exec:java@live-consumer-probe \
        >"$evidence_dir/java-offset-query.tsv" 2>"$evidence_dir/java-offset-query.stderr"; then
        cat "$evidence_dir/java-offset-query.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v offset="$rust_offset" \
        'NF != 2 || $1 != "OFFSET" || $2 != offset { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/java-offset-query.tsv"; then
        echo "Java did not read the offset written by Rust" >&2
        exit 1
    fi
    printf 'PASS %s: Java and Rust read back each other’s consumer-offset updates\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "CONS-002-pull-found" || "$scenario_id" == "CONS-003-pull-no-new-msg" ]]; then
    expected_status="NO_NEW_MSG"
    if [[ "$scenario_id" == "CONS-002-pull-found" ]]; then
        expected_status="FOUND"
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="send $broker_addr $topic $body" exec:java@live-producer-probe \
            >"$evidence_dir/java-seed-send.tsv" 2>"$evidence_dir/java-seed-send.stderr"; then
            cat "$evidence_dir/java-seed-send.stderr" >&2
            exit 1
        fi
        if ! awk -F '\t' -v topic="$topic" \
            'NF != 7 || $1 != "SEND" || $2 != "SEND_OK" || $3 != topic || $4 != "0" || $5 != "0" || $6 !~ /^[[:xdigit:]]+$/ || $7 !~ /^[0-9]+$/ { exit 1 }
             END { if (NR != 1) exit 1 }' "$evidence_dir/java-seed-send.tsv"; then
            echo "Java pull seed did not land at queue 0 offset 0" >&2
            exit 1
        fi
    fi
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="pull $broker_addr $producer_group $topic $queue_id $pull_offset" \
        exec:java@live-consumer-probe \
        >"$evidence_dir/java-pull.tsv" 2>"$evidence_dir/java-pull.stderr"; then
        cat "$evidence_dir/java-pull.stderr" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        pull "$broker_addr" "$producer_group" "$topic" "$queue_id" "$pull_offset" \
        >"$evidence_dir/rust-pull.tsv" 2>"$evidence_dir/rust-pull.stderr"; then
        cat "$evidence_dir/rust-pull.stderr" >&2
        exit 1
    fi
    if ! cmp -s "$evidence_dir/java-pull.tsv" "$evidence_dir/rust-pull.tsv"; then
        diff -u "$evidence_dir/java-pull.tsv" "$evidence_dir/rust-pull.tsv" \
            >"$evidence_dir/pull-diff.txt" || true
        cat "$evidence_dir/pull-diff.txt" >&2
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-pull.tsv" "$expected_status" "$body" <<'PY'
import sys
from pathlib import Path

row = Path(sys.argv[1]).read_text().rstrip("\n").split("\t")
expected_status = sys.argv[2]
expected_body = sys.argv[3]
if len(row) != 7 or row[0] != "PULL" or row[1] != expected_status:
    raise SystemExit("pull result status/shape differs from the expected case")
if any(not value.isdecimal() for value in row[2:6]):
    raise SystemExit("pull result offsets/broker ID must be decimal values")
if row[6] and (len(row[6]) % 2 or any(ch not in "0123456789abcdef" for ch in row[6])):
    raise SystemExit("pull response body must be lowercase hexadecimal")
if expected_status == "FOUND":
    if row[2:6] != ["1", "0", "1", "0"]:
        raise SystemExit("found pull must report offsets 0..1 and broker ID 0")
    if not expected_body or expected_body.encode() not in bytes.fromhex(row[6]):
        raise SystemExit("found pull response does not contain the seeded message body")
elif row[6]:
    raise SystemExit("empty pull must not return a message body")
PY
    then
        echo "Java/Rust pull output does not satisfy the scenario result" >&2
        exit 1
    fi
    if [[ "$expected_status" == "FOUND" ]]; then
        printf 'PASS %s: matching FOUND status, offsets, and seeded message body\n' "$scenario_id" \
            | tee "$evidence_dir/result.txt"
    else
        printf 'PASS %s: matching NO_NEW_MSG status and empty response body\n' "$scenario_id" \
            | tee "$evidence_dir/result.txt"
    fi
elif [[ "$scenario_id" == "ORDER-001-lock-unlock" ]]; then
    run_java_lock() {
        local output_file="$1"
        local client_id="$2"
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="lock $broker_addr $producer_group $client_id $topic broker-a 0" exec:java@live-consumer-probe \
            >"$output_file" 2>"${output_file%.tsv}.stderr"; then
            cat "$output_file" "${output_file%.tsv}.stderr" 2>/dev/null >&2 || true
            return 1
        fi
    }
    run_java_unlock() {
        local output_file="$1"
        local client_id="$2"
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="unlock $broker_addr $producer_group $client_id $topic broker-a 0" exec:java@live-consumer-probe \
            >"$output_file" 2>"${output_file%.tsv}.stderr"; then
            cat "$output_file" "${output_file%.tsv}.stderr" 2>/dev/null >&2 || true
            return 1
        fi
    }
    run_rust_lock() {
        local output_file="$1"
        local client_id="$2"
        if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
            lock "$broker_addr" "$producer_group" "$client_id" "$topic" broker-a 0 \
            >"$output_file" 2>"${output_file%.tsv}.stderr"; then
            cat "${output_file%.tsv}.stderr" >&2
            return 1
        fi
    }
    run_rust_unlock() {
        local output_file="$1"
        local client_id="$2"
        if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
            unlock "$broker_addr" "$producer_group" "$client_id" "$topic" broker-a 0 \
            >"$output_file" 2>"${output_file%.tsv}.stderr"; then
            cat "${output_file%.tsv}.stderr" >&2
            return 1
        fi
    }
    validate_lock() {
        local output_file="$1"
        local expected="$2"
        awk -F '\t' -v acquired="$expected" -v topic="$topic" \
            'NF != 5 || $1 != "LOCK" || $2 != acquired || $3 != topic || $4 != "broker-a" || $5 != 0 { exit 1 }
             END { if (NR != 1) exit 1 }' "$output_file"
    }
    validate_unlock() {
        local output_file="$1"
        awk -F '\t' 'NF != 2 || $1 != "UNLOCK" || $2 != "OK" { exit 1 }
            END { if (NR != 1) exit 1 }' "$output_file"
    }

    echo "Comparing Java and Rust lock acquisition and cross-client contention"
    run_java_lock "$evidence_dir/java-initial-lock.tsv" java-initial
    run_java_unlock "$evidence_dir/java-initial-unlock.tsv" java-initial
    run_rust_lock "$evidence_dir/rust-initial-lock.tsv" rust-initial
    run_rust_unlock "$evidence_dir/rust-initial-unlock.tsv" rust-initial
    if ! cmp -s "$evidence_dir/java-initial-lock.tsv" "$evidence_dir/rust-initial-lock.tsv" \
        || ! validate_lock "$evidence_dir/java-initial-lock.tsv" true \
        || ! validate_unlock "$evidence_dir/java-initial-unlock.tsv" \
        || ! validate_unlock "$evidence_dir/rust-initial-unlock.tsv"; then
        echo "Java and Rust must both acquire and release the same unlocked queue" >&2
        exit 1
    fi
    run_java_lock "$evidence_dir/java-owner-lock.tsv" java-order-owner
    run_rust_lock "$evidence_dir/rust-contended-lock.tsv" rust-order-contender
    if ! validate_lock "$evidence_dir/java-owner-lock.tsv" true \
        || ! validate_lock "$evidence_dir/rust-contended-lock.tsv" false; then
        echo "A second client must not acquire a queue while Java owns its lock" >&2
        exit 1
    fi
    run_java_unlock "$evidence_dir/java-owner-unlock.tsv" java-order-owner
    if ! validate_unlock "$evidence_dir/java-owner-unlock.tsv"; then
        echo "Java queue unlock did not complete synchronously" >&2
        exit 1
    fi
    run_rust_lock "$evidence_dir/rust-after-unlock-lock.tsv" rust-order-contender
    if ! validate_lock "$evidence_dir/rust-after-unlock-lock.tsv" true; then
        echo "Rust client could not acquire the queue after Java released its lock" >&2
        exit 1
    fi
    run_rust_unlock "$evidence_dir/rust-owner-unlock.tsv" rust-order-contender
    if ! validate_unlock "$evidence_dir/rust-owner-unlock.tsv"; then
        echo "Rust queue unlock did not complete synchronously" >&2
        exit 1
    fi
    run_java_lock "$evidence_dir/java-after-unlock-lock.tsv" java-final
    if ! validate_lock "$evidence_dir/java-after-unlock-lock.tsv" true; then
        echo "Java client could not acquire the queue after Rust released its lock" >&2
        exit 1
    fi
    run_java_unlock "$evidence_dir/java-final-unlock.tsv" java-final
    if ! validate_unlock "$evidence_dir/java-final-unlock.tsv"; then
        echo "Final Java queue unlock did not complete synchronously" >&2
        exit 1
    fi
    printf 'PASS %s: Java/Rust both acquired the unlocked queue; Rust was denied while Java held it and acquired it after Java unlock; reverse release also passed\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "REBAL-001-average-10q-3c" ]]; then
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="$namesrv_addr $topic $consumer_ids" exec:java@live-rebalance-probe \
        >"$evidence_dir/java-rebalance.tsv" 2>"$evidence_dir/java-rebalance.stderr"; then
        cat "$evidence_dir/java-rebalance.tsv" "$evidence_dir/java-rebalance.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        rebalance "$namesrv_addr" "$topic" "$consumer_ids" \
        >"$evidence_dir/rust-rebalance.tsv" 2>"$evidence_dir/rust-rebalance.stderr"; then
        cat "$evidence_dir/rust-rebalance.stderr" >&2
        exit 1
    fi
    if ! cmp -s "$evidence_dir/java-rebalance.tsv" "$evidence_dir/rust-rebalance.tsv"; then
        diff -u "$evidence_dir/java-rebalance.tsv" "$evidence_dir/rust-rebalance.tsv" \
            >"$evidence_dir/rebalance-diff.txt" || true
        cat "$evidence_dir/rebalance-diff.txt" >&2
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-rebalance.tsv" "$topic" "$consumer_ids" <<'PY'
import sys
from pathlib import Path

rows = [line.split("\t") for line in Path(sys.argv[1]).read_text().splitlines()]
topic = sys.argv[2]
consumers = sorted(sys.argv[3].split(","))
if len(rows) != len(consumers):
    raise SystemExit("assignment output must contain one row per consumer")
assignments = {}
for row in rows:
    if len(row) != 3 or row[0] != "ASSIGN" or row[1] in assignments:
        raise SystemExit("assignment output has an invalid or duplicate consumer row")
    assignments[row[1]] = row[2].split(",") if row[2] else []
if list(assignments) != consumers:
    raise SystemExit("consumer assignment rows must use Java's sorted consumer order")
if [len(assignments[consumer]) for consumer in consumers] != [4, 3, 3]:
    raise SystemExit("10 queues over three clients must assign 4, 3, and 3 queues")
all_queues = [queue for consumer in consumers for queue in assignments[consumer]]
if len(all_queues) != 10 or len(set(all_queues)) != 10:
    raise SystemExit("all 10 queue assignments must be unique and complete")
expected = {
    f"{topic}|{broker}|{queue_id}"
    for broker in ("broker-a", "broker-b")
    for queue_id in range(5)
}
if set(all_queues) != expected:
    raise SystemExit("assignments must cover queues 0-4 on both Brokers")
PY
    then
        echo "Live 10-queue route assignment does not match the expected three-consumer split" >&2
        exit 1
    fi
    printf 'PASS %s: Java and Rust matched all 10 live Broker queues with a 4/3/3 assignment split\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "TX-001-commit" || "$scenario_id" == "TX-002-rollback" ]]; then
    transaction_decision="commit"
    expected_state="COMMIT_MESSAGE"
    expected_max_offset=2
    if [[ "$scenario_id" == "TX-002-rollback" ]]; then
        transaction_decision="rollback"
        expected_state="ROLLBACK_MESSAGE"
        expected_max_offset=0
    fi
    java_key="${key_prefix}_java"
    rust_key="${key_prefix}_rust"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="$namesrv_addr $topic $producer_group $transaction_decision $java_key $body" \
        exec:java@live-transaction-probe >"$evidence_dir/java-transaction.tsv" \
        2>"$evidence_dir/java-transaction.stderr"; then
        cat "$evidence_dir/java-transaction.tsv" "$evidence_dir/java-transaction.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        transaction "$namesrv_addr" "$topic" "$producer_group" "$transaction_decision" "$rust_key" "$body" \
        >"$evidence_dir/rust-transaction.tsv" 2>"$evidence_dir/rust-transaction.stderr"; then
        cat "$evidence_dir/rust-transaction.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v state="$expected_state" -v key="$java_key" \
        'NF != 5 || $1 != "TX" || $2 != "SEND_OK" || $3 != state || $4 != key || $5 == "" { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/java-transaction.tsv"; then
        echo "Java transaction send result does not match expected state $expected_state" >&2
        exit 1
    fi
    if ! awk -F '\t' -v state="$expected_state" -v key="$rust_key" \
        'NF != 5 || $1 != "TX" || $2 != "SEND_OK" || $3 != state || $4 != key || $5 == "" { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/rust-transaction.tsv"; then
        echo "Rust transaction send result does not match expected state $expected_state" >&2
        exit 1
    fi
    sleep 2
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        max-offset "$broker_addr" "$topic" 0 >"$evidence_dir/queue-max-offset.tsv" \
        2>"$evidence_dir/queue-max-offset.stderr"; then
        cat "$evidence_dir/queue-max-offset.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v expected="$expected_max_offset" \
        'NF != 2 || $1 != "MAX_OFFSET" || $2 != expected { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/queue-max-offset.tsv"; then
        echo "Broker queue max offset differs from expected $expected_max_offset" >&2
        exit 1
    fi
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="pull $broker_addr $consumer_group $topic 0 0" exec:java@live-consumer-probe \
        >"$evidence_dir/java-pull.tsv" 2>"$evidence_dir/java-pull.stderr"; then
        cat "$evidence_dir/java-pull.stderr" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        pull "$broker_addr" "$consumer_group" "$topic" 0 0 \
        >"$evidence_dir/rust-pull.tsv" 2>"$evidence_dir/rust-pull.stderr"; then
        cat "$evidence_dir/rust-pull.stderr" >&2
        exit 1
    fi
    if ! cmp -s "$evidence_dir/java-pull.tsv" "$evidence_dir/rust-pull.tsv"; then
        diff -u "$evidence_dir/java-pull.tsv" "$evidence_dir/rust-pull.tsv" \
            >"$evidence_dir/pull-diff.txt" || true
        cat "$evidence_dir/pull-diff.txt" >&2
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-pull.tsv" "$scenario_id" <<'PY'
import sys
from pathlib import Path

row = Path(sys.argv[1]).read_text().rstrip("\n").split("\t")
scenario_id = sys.argv[2]
if len(row) != 7 or row[0] != "PULL" or any(not value.isdecimal() for value in row[2:6]):
    raise SystemExit("transaction consumer pull has an invalid shape")
if row[6] and (len(row[6]) % 2 or any(ch not in "0123456789abcdef" for ch in row[6])):
    raise SystemExit("transaction pull body must be lowercase hexadecimal")
if scenario_id == "TX-001-commit":
    if row[1] != "FOUND" or row[2:6] != ["2", "0", "2", "0"]:
        raise SystemExit("commit pull must return both committed messages at offsets 0..2")
    if bytes.fromhex(row[6]).count(b"transaction-body") < 2:
        raise SystemExit("commit pull does not contain both transaction message bodies")
else:
    if row[1] != "NO_NEW_MSG" or row[2:6] != ["0", "0", "0", "0"] or row[6]:
        raise SystemExit("rollback pull must return no message and retain empty queue offsets")
PY
    then
        echo "Java/Rust transaction pull output does not satisfy the visibility assertion" >&2
        exit 1
    fi
    if [[ "$scenario_id" == "TX-001-commit" ]]; then
        printf 'PASS %s: Java/Rust commits matched; both committed messages were visible and max offset was %s\n' \
            "$scenario_id" "$expected_max_offset" | tee "$evidence_dir/result.txt"
    else
        printf 'PASS %s: Java/Rust rollbacks matched; no message was visible and max offset was %s\n' \
            "$scenario_id" "$expected_max_offset" | tee "$evidence_dir/result.txt"
    fi
elif [[ "$scenario_id" == "P9-V03" ]]; then
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="seed $namesrv_addr $topic" exec:java@live-pull-consumer-probe \
        >"$evidence_dir/seed.tsv" 2>"$evidence_dir/seed.stderr"; then
        cat "$evidence_dir/seed.tsv" "$evidence_dir/seed.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="probe $namesrv_addr $topic ${consumer_group}_java" \
        exec:java@live-pull-consumer-probe \
        >"$evidence_dir/java-pull-consumer.tsv" \
        2>"$evidence_dir/java-pull-consumer.stderr"; then
        cat "$evidence_dir/java-pull-consumer.tsv" "$evidence_dir/java-pull-consumer.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        p9-pull "$namesrv_addr" "$topic" "${consumer_group}_rust" \
        >"$evidence_dir/rust-pull-consumer.tsv" \
        2>"$evidence_dir/rust-pull-consumer.stderr"; then
        cat "$evidence_dir/rust-pull-consumer.tsv" "$evidence_dir/rust-pull-consumer.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-pull-consumer.tsv" \
        "$evidence_dir/rust-pull-consumer.tsv" <<'PY'
import sys
from pathlib import Path

expected_statuses = ["FOUND", "NO_MATCHED_MSG", "NO_NEW_MSG", "OFFSET_ILLEGAL"]

def parse(path):
    pulls = []
    bodies = []
    boundary = None
    resume = None
    for line in Path(path).read_text().splitlines():
        if not line.startswith("P9\t"):
            continue
        row = line.split("\t")
        if row[1] == "PULL":
            if len(row) != 7:
                raise SystemExit(f"invalid pull result row: {line}")
            pull = (row[2], *map(int, row[3:]))
            if pull[0] not in expected_statuses:
                raise SystemExit(f"unexpected pull status: {pull[0]}")
            if pull[2:4] != (0, 2) or pull[4] != 0:
                raise SystemExit(f"pull boundaries or suggested broker differ: {pull!r}")
            pulls.append(pull)
        elif row[1] == "BODY":
            bodies.append(row[2])
        elif row[1] == "BOUNDARY":
            boundary = tuple(map(int, row[2:]))
        elif row[1] == "RESUME":
            resume = int(row[2])
        else:
            raise SystemExit(f"unexpected P9 result row: {line}")
    if [pull[0] for pull in pulls] != expected_statuses:
        raise SystemExit(f"pull statuses differ from expected sequence: {pulls!r}")
    if bodies != ["p9-seed-match"]:
        raise SystemExit(f"manual pull returned unexpected body list: {bodies!r}")
    if boundary != (0, 2, 0):
        raise SystemExit(f"offset boundary RPCs returned {boundary!r}")
    if resume != 1:
        raise SystemExit(f"restarted pull consumer resumed at {resume!r}")
    return pulls, bodies, boundary, resume

java = parse(sys.argv[1])
rust = parse(sys.argv[2])
if java != rust:
    raise SystemExit(f"Java/Rust pull outcomes differ: {java!r} != {rust!r}")
PY
    then
        echo "Java/Rust pull-consumer behavior does not satisfy P9-V03" >&2
        exit 1
    fi
    printf 'PASS P9-V03: Java and Rust matched all four pull statuses, suggested broker, boundary RPCs, and persisted resume offset\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "REQ-001-request-reply-success" ]]; then
    start_reply_responder rust reply-java-to-rust "$topic" "$evidence_dir/rust-responder.tsv"
    run_reply_requester java "$topic" request-java-to-rust "$evidence_dir/java-request-rust-reply.tsv"
    wait_reply_responder "$evidence_dir/rust-responder.tsv"

    start_reply_responder java reply-rust-to-java "$rust_request_topic" "$evidence_dir/java-responder.tsv"
    run_reply_requester rust "$rust_request_topic" request-rust-to-java "$evidence_dir/rust-request-java-reply.tsv"
    wait_reply_responder "$evidence_dir/java-responder.tsv"

    if ! python3 - "$topic" "$rust_request_topic" \
        "$evidence_dir/java-request-rust-reply.tsv" \
        "$evidence_dir/rust-responder.tsv" \
        "$evidence_dir/rust-request-java-reply.tsv" \
        "$evidence_dir/java-responder.tsv" <<'PY'
import sys
from pathlib import Path

java_topic, rust_topic, java_request_path, rust_responder_path, rust_request_path, java_responder_path = sys.argv[1:]

def parse_request(path, expected_topic, request_body, reply_body):
    lines = Path(path).read_text().splitlines()
    requests = [line.split("\t") for line in lines if line.startswith("REQ\tREQUEST\t")]
    replies = [line.split("\t") for line in lines if line.startswith("REQ\tREPLY\t")]
    if len(requests) != 1 or len(replies) != 1:
        raise SystemExit(f"{path}: expected exactly one request and one returned reply")
    request, reply = requests[0], replies[0]
    if len(request) != 9 or request[2] != expected_topic or bytes.fromhex(request[3]) != request_body.encode():
        raise SystemExit(f"{path}: request topic/body or result shape differs: {request!r}")
    if not request[4] or request[4] == "-" or request[5] == "-" or request[6] != "15000":
        raise SystemExit(f"{path}: request omitted correlation/reply-to/TTL metadata: {request!r}")
    if len(reply) != 9 or not reply[2].endswith("_REPLY_TOPIC"):
        raise SystemExit(f"{path}: reply topic or result shape differs: {reply!r}")
    if bytes.fromhex(reply[3]) != reply_body.encode() or reply[4] != request[4]:
        raise SystemExit(f"{path}: reply body or correlation does not match its request: {reply!r}")
    if reply[5] != request[5] or reply[6] != request[6] or reply[8] != "reply":
        raise SystemExit(f"{path}: reply-to, TTL, or type metadata differs: {reply!r}")
    return (
        request[2], request[3], request[4], request[5], request[6], request[7],
        reply[2], reply[3], reply[8]
    )

def parse_responder(path, expected_topic, expected_body):
    lines = Path(path).read_text().splitlines()
    received = [line.split("\t") for line in lines if line.startswith("REQ\tRECEIVED\t")]
    sent = [line.split("\t") for line in lines if line.startswith("REQ\tRESPONDER\t")]
    if len(received) != 1 or len(received[0]) != 6:
        raise SystemExit(f"{path}: expected exactly one request observation")
    if received[0][2] != expected_topic or bytes.fromhex(received[0][3]) != expected_body.encode():
        raise SystemExit(f"{path}: responder consumed the wrong request: {received[0]!r}")
    if len(sent) != 1 or len(sent[0]) != 7 or sent[0][2] != "SEND_OK":
        raise SystemExit(f"{path}: expected exactly one successful reply send")
    if not sent[0][3].endswith("_REPLY_TOPIC"):
        raise SystemExit(f"{path}: responder sent to an unexpected topic: {sent[0]!r}")
    if sent[0][4:] != received[0][3:]:
        raise SystemExit(f"{path}: reply send did not use the received request metadata")
    return sent[0][3:]

java_rust = parse_request(java_request_path, java_topic, "request-java-to-rust", "reply-java-to-rust")
rust_java = parse_request(rust_request_path, rust_topic, "request-rust-to-java", "reply-rust-to-java")
java_rust_sent = parse_responder(rust_responder_path, java_topic, "request-java-to-rust")
rust_java_sent = parse_responder(java_responder_path, rust_topic, "request-rust-to-java")
if java_rust[0] != java_topic or java_rust[1] != java_rust_sent[1] or java_rust[2] != java_rust_sent[2] or java_rust[3] != java_rust_sent[3]:
    raise SystemExit("Rust responder did not receive the expected Java request")
if rust_java[0] != rust_topic or rust_java[1] != rust_java_sent[1] or rust_java[2] != rust_java_sent[2] or rust_java[3] != rust_java_sent[3]:
    raise SystemExit("Java responder did not receive the expected Rust request")
if java_rust[6] != java_rust_sent[0] or rust_java[6] != rust_java_sent[0]:
    raise SystemExit("responder send topic differs from the topic returned to the requester")
if java_rust[6] != rust_java[6] or java_rust[8] != rust_java[8]:
    raise SystemExit("Java/Rust request-reply topic or message type semantics differ")
PY
    then
        echo "Cross-client request/reply behavior does not satisfy REQ-001" >&2
        exit 1
    fi
    printf 'PASS REQ-001-request-reply-success: Java and Rust each received exactly one correlated Broker reply from the other client; reply body and metadata matched\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P15-V03" ]]; then
    echo "Checking bidirectional replies, no-responder timeouts, and late-reply terminal state"
    start_reply_responder rust p15-java-to-rust-reply "$topic" "$evidence_dir/rust-responder.tsv"
    run_reply_requester java "$topic" p15-java-to-rust-request "$evidence_dir/java-request-rust-reply.tsv"
    wait_reply_responder "$evidence_dir/rust-responder.tsv"

    start_reply_responder java p15-rust-to-java-reply "${p15_topics[0]}" "$evidence_dir/java-responder.tsv"
    run_reply_requester rust "${p15_topics[0]}" p15-rust-to-java-request "$evidence_dir/rust-request-java-reply.tsv"
    wait_reply_responder "$evidence_dir/java-responder.tsv"

    run_request_timeout java none "${p15_topics[1]}" compat_p15_no_reply_java p15-no-reply-java "$evidence_dir/java-no-reply.tsv"
    run_request_timeout rust none "${p15_topics[2]}" compat_p15_no_reply_rust p15-no-reply-rust "$evidence_dir/rust-no-reply.tsv"

    start_reply_responder rust p15-late-java-reply "${p15_topics[3]}" "$evidence_dir/rust-late-responder.tsv" "$p15_late_reply_delay_ms"
    run_request_timeout java late "${p15_topics[3]}" compat_p15_late_java p15-late-java "$evidence_dir/java-late-timeout.tsv"
    wait_reply_responder "$evidence_dir/rust-late-responder.tsv"

    start_reply_responder java p15-late-rust-reply "${p15_topics[4]}" "$evidence_dir/java-late-responder.tsv" "$p15_late_reply_delay_ms"
    run_request_timeout rust late "${p15_topics[4]}" compat_p15_late_rust p15-late-rust "$evidence_dir/rust-late-timeout.tsv"
    wait_reply_responder "$evidence_dir/java-late-responder.tsv"

    if ! python3 - "$topic" "${p15_topics[0]}" "${p15_topics[1]}" "${p15_topics[2]}" \
        "${p15_topics[3]}" "${p15_topics[4]}" "$p15_timeout_ms" \
        "$p15_no_reply_hold_ms" "$p15_late_reply_hold_ms" \
        "$evidence_dir/java-request-rust-reply.tsv" "$evidence_dir/rust-responder.tsv" \
        "$evidence_dir/rust-request-java-reply.tsv" "$evidence_dir/java-responder.tsv" \
        "$evidence_dir/java-no-reply.tsv" "$evidence_dir/rust-no-reply.tsv" \
        "$evidence_dir/java-late-timeout.tsv" "$evidence_dir/rust-late-timeout.tsv" \
        "$evidence_dir/rust-late-responder.tsv" "$evidence_dir/java-late-responder.tsv" <<'PY'
import sys
from pathlib import Path

(java_topic, rust_topic, java_no_reply_topic, rust_no_reply_topic,
 java_late_topic, rust_late_topic, timeout_ms, no_reply_hold_ms,
 late_hold_ms, java_request_path, rust_responder_path, rust_request_path,
 java_responder_path, java_timeout_path, rust_timeout_path, java_late_path,
 rust_late_path, rust_late_responder_path, java_late_responder_path) = sys.argv[1:]
timeout_ms = int(timeout_ms)
no_reply_hold_ms = int(no_reply_hold_ms)
late_hold_ms = int(late_hold_ms)

def lines(path):
    return Path(path).read_text().splitlines()

def parse_request(path, expected_topic, request_body, reply_body):
    rows = [line.split("\t") for line in lines(path) if line.startswith("REQ\t")]
    requests = [row for row in rows if len(row) > 1 and row[1] == "REQUEST"]
    replies = [row for row in rows if len(row) > 1 and row[1] == "REPLY"]
    if len(requests) != 1 or len(replies) != 1:
        raise SystemExit(f"{path}: expected one requester observation and one returned reply")
    request, reply = requests[0], replies[0]
    if len(request) != 9 or request[2] != expected_topic or bytes.fromhex(request[3]) != request_body.encode():
        raise SystemExit(f"{path}: request topic/body or result shape differs: {request!r}")
    if not request[4] or request[4] == "-" or request[5] == "-" or request[6] != "15000":
        raise SystemExit(f"{path}: request correlation/reply-to/TTL fields are missing: {request!r}")
    if len(reply) != 9 or not reply[2].endswith("_REPLY_TOPIC"):
        raise SystemExit(f"{path}: reply topic or result shape differs: {reply!r}")
    if bytes.fromhex(reply[3]) != reply_body.encode() or reply[4] != request[4]:
        raise SystemExit(f"{path}: reply body or correlation differs: {reply!r}")
    if reply[5] != request[5] or reply[6] != request[6] or reply[8] != "reply":
        raise SystemExit(f"{path}: reply metadata differs from request: {reply!r}")
    return request[4], request[5], reply[2]

def parse_responder(path, expected_topic, expected_body):
    rows = [line.split("\t") for line in lines(path) if line.startswith("REQ\t")]
    received = [row for row in rows if len(row) > 1 and row[1] == "RECEIVED"]
    sent = [row for row in rows if len(row) > 1 and row[1] == "RESPONDER"]
    if len(received) != 1 or len(received[0]) != 6:
        raise SystemExit(f"{path}: expected one received request: {received!r}")
    if received[0][2] != expected_topic or bytes.fromhex(received[0][3]) != expected_body.encode():
        raise SystemExit(f"{path}: responder consumed an unexpected request: {received[0]!r}")
    if len(sent) != 1 or len(sent[0]) != 7 or sent[0][2] != "SEND_OK":
        raise SystemExit(f"{path}: expected one successful reply send: {sent!r}")
    if not sent[0][3].endswith("_REPLY_TOPIC") or sent[0][4:] != received[0][3:]:
        raise SystemExit(f"{path}: reply send metadata differs from received request: {sent[0]!r}")
    return received[0], sent[0]

def parse_timeout(path, expected_topic, expected_hold):
    rows = [line.split("\t") for line in lines(path) if line.startswith("REQ\tTIMEOUT\t")]
    if len(rows) != 1 or len(rows[0]) != 7:
        raise SystemExit(f"{path}: expected one timeout row: {rows!r}")
    row = rows[0]
    elapsed = int(row[4])
    if row[2] != expected_topic or not row[3] or elapsed < timeout_ms - 500 or elapsed > timeout_ms + 6000:
        raise SystemExit(f"{path}: timeout topic/correlation/duration differs: {row!r}")
    if row[5] != "false" or int(row[6]) != expected_hold:
        raise SystemExit(f"{path}: timeout left a pending correlation or used the wrong hold: {row!r}")
    return row[3]

def parse_late(path, expected_topic, expected_hold, java):
    rows = [line.split("\t") for line in lines(path) if line.startswith("REQ\tLATE_TIMEOUT\t")]
    if len(rows) != 1 or len(rows[0]) != 8:
        raise SystemExit(f"{path}: expected one late-timeout row: {rows!r}")
    row = rows[0]
    valid_outcome = row[7] == "org.apache.rocketmq.client.exception.RequestTimeoutException" if java else row[7] == "TIMEOUT"
    if row[2] != expected_topic or not row[3] or row[4] != "1" or row[5] != "false":
        raise SystemExit(f"{path}: late reply changed callback or correlation state: {row!r}")
    if int(row[6]) != expected_hold or not valid_outcome:
        raise SystemExit(f"{path}: timeout callback outcome differs: {row!r}")
    return row[3]

java_success_corr, _, java_reply_topic = parse_request(
    java_request_path, java_topic, "p15-java-to-rust-request", "p15-java-to-rust-reply")
rust_success_received, _ = parse_responder(
    rust_responder_path, java_topic, "p15-java-to-rust-request")
rust_success_corr, _, rust_reply_topic = parse_request(
    rust_request_path, rust_topic, "p15-rust-to-java-request", "p15-rust-to-java-reply")
java_success_received, _ = parse_responder(
    java_responder_path, rust_topic, "p15-rust-to-java-request")
if java_success_corr != rust_success_received[4] or rust_success_corr != java_success_received[4]:
    raise SystemExit("cross-language responder correlation differs from requester")
if java_reply_topic != rust_reply_topic:
    raise SystemExit("Java and Rust successful request replies use different reply topics")

parse_timeout(java_timeout_path, java_no_reply_topic, no_reply_hold_ms)
parse_timeout(rust_timeout_path, rust_no_reply_topic, no_reply_hold_ms)
java_late_corr = parse_late(java_late_path, java_late_topic, late_hold_ms, True)
rust_late_corr = parse_late(rust_late_path, rust_late_topic, late_hold_ms, False)
rust_late_received, _ = parse_responder(
    rust_late_responder_path, java_late_topic, "p15-late-java")
java_late_received, _ = parse_responder(
    java_late_responder_path, rust_late_topic, "p15-late-rust")
if java_late_corr != rust_late_received[4] or rust_late_corr != java_late_received[4]:
    raise SystemExit("late reply responder did not receive the matching timed-out request")
PY
    then
        echo "Java/Rust request/reply timeout or late-reply behavior differs from P15-V03" >&2
        exit 1
    fi
    printf 'PASS P15-V03: bidirectional replies matched; Java and Rust no-responder timeouts removed correlations; opposite-language late replies arrived after timeout without changing the single terminal callback\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P3-V04" ]]; then
    cat >"$evidence_dir/ca.cnf" <<'EOF'
[req]
prompt = no
distinguished_name = distinguished_name
x509_extensions = ca_extensions
[distinguished_name]
CN = rocketmq-classic-phase19-test-ca
[ca_extensions]
basicConstraints = critical,CA:true
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
EOF
    cat >"$evidence_dir/untrusted-ca.cnf" <<'EOF'
[req]
prompt = no
distinguished_name = distinguished_name
x509_extensions = ca_extensions
[distinguished_name]
CN = rocketmq-classic-phase19-untrusted-ca
[ca_extensions]
basicConstraints = critical,CA:true
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
EOF
    cat >"$evidence_dir/server-req.cnf" <<'EOF'
[req]
prompt = no
distinguished_name = distinguished_name
[distinguished_name]
CN = localhost
EOF
    cat >"$evidence_dir/server-ext.cnf" <<'EOF'
[server_cert]
basicConstraints = critical,CA:false
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost
EOF
    openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 2 \
        -config "$evidence_dir/ca.cnf" -keyout "$evidence_dir/ca.key" \
        -out "$evidence_dir/ca.pem" >"$evidence_dir/ca-generate.log" 2>&1
    openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 2 \
        -config "$evidence_dir/untrusted-ca.cnf" -keyout "$evidence_dir/untrusted-ca.key" \
        -out "$evidence_dir/untrusted-ca.pem" >>"$evidence_dir/ca-generate.log" 2>&1
    openssl req -new -newkey rsa:2048 -nodes -sha256 \
        -config "$evidence_dir/server-req.cnf" -keyout "$evidence_dir/server.key" \
        -out "$evidence_dir/server.csr" >"$evidence_dir/server-generate.log" 2>&1
    openssl x509 -req -in "$evidence_dir/server.csr" \
        -CA "$evidence_dir/ca.pem" -CAkey "$evidence_dir/ca.key" -set_serial 1 \
        -days 2 -sha256 -extfile "$evidence_dir/server-ext.cnf" \
        -extensions server_cert -out "$evidence_dir/server.pem" \
        >"$evidence_dir/server-sign.log" 2>&1
    openssl verify -CAfile "$evidence_dir/ca.pem" "$evidence_dir/server.pem" \
        >"$evidence_dir/server-verify.txt" 2>&1
    python3 "$repo_root/tests/differential/tls_broker_proxy.py" \
        --upstream "$broker_addr" --cert "$evidence_dir/server.pem" \
        --key "$evidence_dir/server.key" --ready-file "$evidence_dir/tls-proxy-ready.txt" \
        --log-file "$evidence_dir/tls-proxy.tsv" \
        >"$evidence_dir/tls-proxy.stdout" 2>"$evidence_dir/tls-proxy.stderr" &
    tls_proxy_pid=$!
    for _ in {1..100}; do
        if [[ -s "$evidence_dir/tls-proxy-ready.txt" ]]; then
            break
        fi
        if ! kill -0 "$tls_proxy_pid" 2>/dev/null; then
            wait "$tls_proxy_pid" 2>/dev/null || true
            tls_proxy_pid=""
            cat "$evidence_dir/tls-proxy.stdout" "$evidence_dir/tls-proxy.stderr" >&2 || true
            exit 1
        fi
        sleep 0.1
    done
    if [[ ! -s "$evidence_dir/tls-proxy-ready.txt" ]]; then
        echo "TLS Broker proxy did not become ready" >&2
        exit 1
    fi
    tls_proxy_port="$(cat "$evidence_dir/tls-proxy-ready.txt")"
    tls_localhost_addr="localhost:$tls_proxy_port"
    tls_ip_addr="127.0.0.1:$tls_proxy_port"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="route $namesrv_addr $topic" exec:java@live-p3-remoting-probe \
        >"$evidence_dir/java-p3-route.tsv" 2>"$evidence_dir/java-p3-route.stderr"; then
        cat "$evidence_dir/java-p3-route.tsv" "$evidence_dir/java-p3-route.stderr" >&2 || true
        exit 1
    fi
    if ! "$repo_root/target/debug/live_client" p3-raw-route "$namesrv_addr" "$topic" \
        >"$evidence_dir/rust-p3-route.tsv" 2>"$evidence_dir/rust-p3-route.stderr"; then
        cat "$evidence_dir/rust-p3-route.tsv" "$evidence_dir/rust-p3-route.stderr" >&2 || true
        exit 1
    fi
    for mode in trusted wrong-host untrusted; do
        tls_addr="$tls_localhost_addr"
        ca_path="$evidence_dir/ca.pem"
        rust_expected_outcome=accepted
        if [[ "$mode" == "wrong-host" ]]; then
            tls_addr="$tls_ip_addr"
            rust_expected_outcome=accepted
        elif [[ "$mode" == "untrusted" ]]; then
            ca_path="$evidence_dir/untrusted-ca.pem"
            rust_expected_outcome=rejected
        fi
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="tls $tls_addr $ca_path $mode" exec:java@live-p3-remoting-probe \
            >"$evidence_dir/java-p3-$mode.tsv" 2>"$evidence_dir/java-p3-$mode.stderr"; then
            cat "$evidence_dir/java-p3-$mode.tsv" "$evidence_dir/java-p3-$mode.stderr" >&2 || true
            exit 1
        fi
        if ! "$repo_root/target/debug/live_client" p3-tls "$tls_addr" "$ca_path" \
            "$mode" "$rust_expected_outcome" >"$evidence_dir/rust-p3-$mode.tsv" \
            2>"$evidence_dir/rust-p3-$mode.stderr"; then
            cat "$evidence_dir/rust-p3-$mode.tsv" "$evidence_dir/rust-p3-$mode.stderr" >&2 || true
            exit 1
        fi
    done
    for _ in {1..50}; do
        accepted_count="$(awk -F '\t' '$1 == "TLS_PROXY" && $2 == "TLS_ACCEPTED" { count++ } END { print count + 0 }' "$evidence_dir/tls-proxy.tsv")"
        closed_count="$(awk -F '\t' '$1 == "TLS_PROXY" && $2 == "CONNECTION_CLOSED" { count++ } END { print count + 0 }' "$evidence_dir/tls-proxy.tsv")"
        rejected_count="$(awk -F '\t' '$1 == "TLS_PROXY" && $2 ~ /^HANDSHAKE_REJECTED:/ { count++ } END { print count + 0 }' "$evidence_dir/tls-proxy.tsv")"
        if ((accepted_count >= 3 && closed_count >= 3 && rejected_count >= 3)); then
            break
        fi
        sleep 0.1
    done
    if [[ -n "$tls_proxy_pid" ]] && kill -0 "$tls_proxy_pid" 2>/dev/null; then
        kill "$tls_proxy_pid" 2>/dev/null
        wait "$tls_proxy_pid" 2>/dev/null || true
        tls_proxy_pid=""
    fi
    if ! python3 - "$evidence_dir" <<'PY'
import sys
from pathlib import Path

root = Path(sys.argv[1])
expected = {
    "java": {"trusted": "ACCEPTED", "wrong-host": "ACCEPTED", "untrusted": "REJECTED"},
    "rust": {"trusted": "ACCEPTED", "wrong-host": "ACCEPTED", "untrusted": "REJECTED"},
}

def parse(path, kind, client=None):
    rows = [line.split("\t") for line in path.read_text().splitlines() if line.startswith("P3\t")]
    if kind == "route":
        if len(rows) != 1 or len(rows[0]) != 7 or rows[0][:3] != ["P3", "ROUTE", "FOUND"]:
            raise SystemExit(f"{path}: malformed raw route result {rows!r}")
        if not rows[0][3].isdecimal() or int(rows[0][3]) < 1 or rows[0][4:] != ["0", "0", "0"]:
            raise SystemExit(f"{path}: route shutdown leaked resources {rows[0]!r}")
        return rows[0][2:]
    if len(rows) != 1 or len(rows[0]) != 7 or rows[0][:2] != ["P3", "TLS"]:
        raise SystemExit(f"{path}: malformed TLS result {rows!r}")
    row = rows[0]
    if row[2] not in expected[client] or row[3] != expected[client][row[2]] or row[4:] != ["0", "0", "0"]:
        raise SystemExit(f"{path}: TLS outcome or shutdown state differs {row!r}")
    return tuple(row[2:])

if parse(root / "java-p3-route.tsv", "route") != parse(root / "rust-p3-route.tsv", "route"):
    raise SystemExit("Java/Rust raw route results differ")
for mode in expected["java"]:
    java = parse(root / f"java-p3-{mode}.tsv", "tls", "java")
    rust = parse(root / f"rust-p3-{mode}.tsv", "tls", "rust")
    if java[0] != mode or rust[0] != mode:
        raise SystemExit(f"Java/Rust TLS mode differs for {mode}: {java!r} != {rust!r}")
    if java != rust:
        raise SystemExit(f"Java/Rust TLS trust outcomes differ for {mode}: {java!r} != {rust!r}")

proxy_rows = [line.split("\t") for line in (root / "tls-proxy.tsv").read_text().splitlines()]
accepted = sum(len(row) >= 2 and row[1] == "TLS_ACCEPTED" for row in proxy_rows)
closed = sum(len(row) >= 2 and row[1] == "CONNECTION_CLOSED" for row in proxy_rows)
rejected = sum(len(row) >= 2 and row[1].startswith("HANDSHAKE_REJECTED:") for row in proxy_rows)
if (accepted, closed, rejected) != (4, 4, 2):
    raise SystemExit(f"TLS proxy observed accepted/closed/rejected {(accepted, closed, rejected)!r}, expected (4, 4, 2)")
PY
    then
        echo "P3-V04 raw route, TLS trust checks, or shutdown lifecycle did not match" >&2
        exit 1
    fi
    printf 'PASS P3-V04: Java and Rust invoked a live NameServer route, accepted trusted CA chains with matching and wrong hostnames, rejected the untrusted CA, and shut down with no pending requests, channels, or worker tasks\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P6-V04" ]]; then
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="$namesrv_addr $topic compat_p6_producer_a compat_p6_producer_b $consumer_group" \
        exec:java@live-shared-runtime-probe \
        >"$evidence_dir/java-shared-runtime.tsv" \
        2>"$evidence_dir/java-shared-runtime.stderr"; then
        cat "$evidence_dir/java-shared-runtime.tsv" "$evidence_dir/java-shared-runtime.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        p6-shared-runtime "$namesrv_addr" "$topic" \
        compat_p6_producer_a compat_p6_producer_b "$consumer_group" \
        >"$evidence_dir/rust-shared-runtime.tsv" \
        2>"$evidence_dir/rust-shared-runtime.stderr"; then
        cat "$evidence_dir/rust-shared-runtime.tsv" "$evidence_dir/rust-shared-runtime.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-shared-runtime.tsv" \
        "$evidence_dir/rust-shared-runtime.tsv" <<'PY'
import sys
from pathlib import Path

expected_received = [
    "p6-before-change",
    "p6-keep-after-change",
    "p6-survivor-after-unregister",
]

def parse(path):
    received = []
    markers = {}
    for line in Path(path).read_text().splitlines():
        if not line.startswith("P6\t"):
            continue
        row = line.split("\t")
        if len(row) != 3:
            raise SystemExit(f"invalid shared-runtime result row: {line}")
        if row[1] == "RECEIVED":
            received.append(row[2])
        elif row[1] in markers:
            raise SystemExit(f"duplicate shared-runtime marker: {line}")
        else:
            markers[row[1]] = row[2]
    if received != expected_received:
        raise SystemExit(f"unexpected consumed messages: {received!r}")
    expected_markers = {
        "SUBSCRIPTION": "*->KEEP",
        "UNREGISTER": "compat_p6_producer_a",
        "SURVIVOR": "SEND_OK",
    }
    if markers != expected_markers:
        raise SystemExit(f"shared-runtime lifecycle markers differ: {markers!r}")
    return received, markers

java = parse(sys.argv[1])
rust = parse(sys.argv[2])
if java != rust:
    raise SystemExit(f"Java/Rust shared-runtime results differ: {java!r} != {rust!r}")
PY
    then
        echo "Java/Rust shared-runtime behavior does not satisfy P6-V04" >&2
        exit 1
    fi
    printf 'PASS P6-V04: Java and Rust applied the new subscription, filtered the old tag, and retained the surviving producer\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P7-V04" ]]; then
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="$namesrv_addr $topic $message_count 32 1592594996 0" \
        exec:java@live-producer-stress-probe \
        >"$evidence_dir/java-producer-stress.tsv" \
        2>"$evidence_dir/java-producer-stress.stderr"; then
        cat "$evidence_dir/java-producer-stress.tsv" "$evidence_dir/java-producer-stress.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        p7-stress "$namesrv_addr" "$topic" "$message_count" 32 1592594996 "$message_count" \
        >"$evidence_dir/rust-producer-stress.tsv" \
        2>"$evidence_dir/rust-producer-stress.stderr"; then
        cat "$evidence_dir/rust-producer-stress.tsv" "$evidence_dir/rust-producer-stress.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-producer-stress.tsv" \
        "$evidence_dir/rust-producer-stress.tsv" "$message_count" <<'PY'
import sys
from pathlib import Path

count = int(sys.argv[3])
expected = {
    "java": (0, count - 1),
    "rust": (count, 2 * count - 1),
}

def parse(path, language):
    rows = [line.split("\t") for line in Path(path).read_text().splitlines()
            if line.startswith("P7S\tLOAD\t")]
    if len(rows) != 1 or len(rows[0]) != 13:
        raise SystemExit(f"{path}: expected exactly one 13-column P7S load row")
    row = rows[0]
    if row[:3] != ["P7S", "LOAD", language]:
        raise SystemExit(f"{path}: unexpected P7S language row: {row!r}")
    numeric = [int(value) for value in row[3:6] + row[7:13]]
    messages, max_inflight, payload_bytes = numeric[:3]
    elapsed_ms, first_offset, last_offset, pending, connections, tasks = numeric[3:]
    if messages != count or max_inflight != 32 or payload_bytes <= count * 256:
        raise SystemExit(f"{path}: stress workload limits or payload size differ: {row!r}")
    if len(row[6]) != 16 or any(character not in "0123456789abcdef" for character in row[6]):
        raise SystemExit(f"{path}: invalid fixed-seed workload checksum: {row[6]!r}")
    if elapsed_ms > 120_000 or (first_offset, last_offset) != expected[language]:
        raise SystemExit(f"{path}: stress deadline or queue offsets differ: {row!r}")
    if (pending, connections, tasks) != (0, 0, 0):
        raise SystemExit(f"{path}: producer shutdown retained resources: {row!r}")
    return messages, max_inflight, payload_bytes, row[6]

java = parse(sys.argv[1], "java")
rust = parse(sys.argv[2], "rust")
if java != rust:
    raise SystemExit(f"Java/Rust fixed-seed workload differs: {java!r} != {rust!r}")
PY
    then
        echo "Java/Rust bounded producer stress behavior does not satisfy P7-V04" >&2
        exit 1
    fi
    if ! mvn -q -e -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="$namesrv_addr $topic" exec:java@live-producer-lifecycle-probe \
        >"$evidence_dir/java-producer-lifecycle.tsv" \
        2>"$evidence_dir/java-producer-lifecycle.stderr"; then
        cat "$evidence_dir/java-producer-lifecycle.tsv" "$evidence_dir/java-producer-lifecycle.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        p7-lifecycle "$namesrv_addr" "$topic" \
        >"$evidence_dir/rust-producer-lifecycle.tsv" \
        2>"$evidence_dir/rust-producer-lifecycle.stderr"; then
        cat "$evidence_dir/rust-producer-lifecycle.tsv" "$evidence_dir/rust-producer-lifecycle.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-producer-lifecycle.tsv" \
        "$evidence_dir/rust-producer-lifecycle.tsv" <<'PY'
import sys
from pathlib import Path

def parse(path, language):
    rows = []
    for line in Path(path).read_text().splitlines():
        if not line.startswith("P7L\t"):
            continue
        row = line.split("\t")
        if len(row) != 8 or row[:3] != ["P7L", "CYCLE", language]:
            raise SystemExit(f"{path}: malformed lifecycle result {line!r}")
        if not all(value.isdecimal() for value in row[3:]):
            raise SystemExit(f"{path}: nonnumeric lifecycle result {line!r}")
        cycle, sends, pending, connections, tasks = map(int, row[3:])
        if (cycle, sends, pending, connections, tasks) != (len(rows), 20, 0, 0, 0):
            raise SystemExit(f"{path}: unexpected lifecycle state {line!r}")
        rows.append(tuple(row[3:]))
    if len(rows) != 3:
        raise SystemExit(f"{path}: expected three start/shutdown cycles, found {len(rows)}")
    return rows

java = parse(sys.argv[1], "java")
rust = parse(sys.argv[2], "rust")
if java != rust:
    raise SystemExit(f"Java/Rust lifecycle results differ: {java!r} != {rust!r}")
PY
    then
        echo "Producer lifecycle behavior does not satisfy P7-V04" >&2
        exit 1
    fi
    printf 'PASS P7-V04: Java and Rust each sent %s fixed-seed variable-size messages with at most 32 concurrent sends, matching payload bytes/checksum and contiguous Broker offsets; three additional 20-send lifecycle cycles returned to zero pending requests, connections, and owned task groups after every shutdown\n' "$message_count" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P7-V03" ]]; then
    run_p7_retry_probe() {
        local language="$1"
        local mode="$2"
        local body="$3"
        local timeout="$4"
        local ready_file="$evidence_dir/${language}-${mode}.ready"
        local go_file="$evidence_dir/${language}-${mode}.go"
        local output_file="$evidence_dir/${language}-${mode}.tsv"
        local stderr_file="$evidence_dir/${language}-${mode}.stderr"
        rm -f "$ready_file" "$go_file"
        if [[ "$language" == "java" ]]; then
            mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
                -Dexec.args="retry-probe $namesrv_addr $topic $mode $body $timeout $ready_file $go_file" \
                exec:java@live-high-level-producer-probe \
                >"$output_file" 2>"$stderr_file" &
        else
            "$repo_root/target/debug/live_client" p7-retry \
                "$namesrv_addr" "$topic" "$mode" "$body" "$timeout" "$ready_file" "$go_file" \
                >"$output_file" 2>"$stderr_file" &
        fi
        p7_probe_pid="$!"
        for _ in {1..120}; do
            if [[ -s "$ready_file" ]]; then
                break
            fi
            if ! kill -0 "$p7_probe_pid" 2>/dev/null; then
                wait "$p7_probe_pid" 2>/dev/null || true
                p7_probe_pid=""
                cat "$output_file" "$stderr_file" >&2 2>/dev/null || true
                return 1
            fi
            sleep 0.1
        done
        if [[ ! -s "$ready_file" ]]; then
            echo "$language $mode producer did not fetch both Broker routes" >&2
            kill "$p7_probe_pid" 2>/dev/null || true
            wait "$p7_probe_pid" 2>/dev/null || true
            p7_probe_pid=""
            cat "$output_file" "$stderr_file" >&2 2>/dev/null || true
            return 1
        fi
        if [[ "$mode" == "retry" ]]; then
            : >"$evidence_dir/broker-a-proxy.reject-flag"
            : >"$evidence_dir/broker-b-proxy.reject-flag"
        elif [[ "$mode" == "deadline" ]]; then
            : >"$evidence_dir/broker-a-proxy.delay-send-flag"
            : >"$evidence_dir/broker-b-proxy.delay-send-flag"
        fi
        : >"$go_file"
        if ! wait "$p7_probe_pid"; then
            p7_probe_pid=""
            rm -f "$evidence_dir/broker-a-proxy.reject-flag" "$evidence_dir/broker-b-proxy.reject-flag"
            cat "$output_file" "$stderr_file" >&2 2>/dev/null || true
            return 1
        fi
        p7_probe_pid=""
        if [[ "$mode" == "retry" ]]; then
            rm -f "$evidence_dir/broker-a-proxy.reject-flag" "$evidence_dir/broker-b-proxy.reject-flag"
        elif [[ "$mode" == "deadline" ]]; then
            rm -f "$evidence_dir/broker-a-proxy.delay-send-flag" "$evidence_dir/broker-b-proxy.delay-send-flag"
            sleep $(((p7_response_delay_ms + 999) / 1000))
        fi
    }

    echo "Retrying proxy-injected NO_PERMISSION Broker responses across both routes and exhausting a bounded send deadline"
    for language in java rust; do
        if ! run_p7_retry_probe "$language" retry "$p7_retry_body" "$p7_retry_timeout_ms"; then
            exit 1
        fi
    done

    for language in java rust; do
        if ! run_p7_retry_probe "$language" deadline "$p7_deadline_body" "$p7_deadline_timeout_ms"; then
            exit 1
        fi
    done

    if ! python3 - "$evidence_dir" "$p7_retry_body" "$p7_deadline_body" \
        "$p7_deadline_timeout_ms" "$p7_response_delay_ms" <<'PY'
import sys
from pathlib import Path

root = Path(sys.argv[1])
retry_body = sys.argv[2]
deadline_body = sys.argv[3]
deadline_timeout = int(sys.argv[4])
response_delay = int(sys.argv[5])

def records(language, mode, expected_body):
    path = root / f"{language}-{mode}.tsv"
    attempts = []
    errors = {}
    results = []
    routes = []
    for line in path.read_text().splitlines():
        row = line.split("\t")
        if row[0] == "ROUTE":
            routes.append(row)
        elif row[0] == "ATTEMPT":
            if len(row) != 7 or row[1] != language:
                raise SystemExit(f"{path}: malformed attempt row {line!r}")
            attempt = int(row[2])
            body = bytes.fromhex(row[5]).decode() if language == "java" else row[5]
            attempts.append((attempt, row[3], row[4], body, row[6]))
        elif row[0] == "ATTEMPT_ERROR":
            if len(row) != 4 or row[1] != language:
                raise SystemExit(f"{path}: malformed attempt error row {line!r}")
            errors[int(row[2])] = row[3]
        elif row[0] == "RESULT" and len(row) >= 5:
            results.append(row)
    if len(routes) != 1 or not routes[0][-1].split(",") == ["broker-a", "broker-b"]:
        raise SystemExit(f"{path}: producer did not resolve both Brokers: {routes!r}")
    if not attempts or [row[0] for row in attempts] != list(range(1, len(attempts) + 1)):
        raise SystemExit(f"{path}: attempt sequence is missing or unordered: {attempts!r}")
    if any(row[3] != expected_body or not row[4] for row in attempts):
        raise SystemExit(f"{path}: body or unique ID changed/was missing: {attempts!r}")
    if len({row[4] for row in attempts}) != 1:
        raise SystemExit(f"{path}: unique ID changed across retries: {attempts!r}")
    if len(results) != 1 or results[0][1:3] != [language, mode]:
        raise SystemExit(f"{path}: expected one {language}/{mode} result: {results!r}")
    return attempts, errors, results[0]

for language in ("java", "rust"):
    attempts, errors, result = records(language, "retry", retry_body)
    if len(attempts) != 3 or [row[2] for row in attempts] != ["ERROR", "ERROR", "SEND_OK"]:
        raise SystemExit(f"{language}: two Broker errors should recover on attempt three: {attempts!r}")
    if any("16" not in errors.get(index, "") for index in (1, 2)) or errors.get(3):
        raise SystemExit(f"{language}: injected NO_PERMISSION classification differs: {errors!r}")
    brokers = [row[1] for row in attempts]
    if set(brokers) != {"broker-a", "broker-b"} or any(a == b for a, b in zip(brokers, brokers[1:])):
        raise SystemExit(f"{language}: retries did not avoid the previous failed Broker: {brokers!r}")
    if brokers[0] != brokers[2] or result[3:5] != ["SUCCESS", "SEND_OK"]:
        raise SystemExit(f"{language}: third attempt did not return SEND_OK after retry: {result!r}")

for language in ("java", "rust"):
    attempts, errors, result = records(language, "deadline", deadline_body)
    if len(attempts) != 1 or attempts[0][2] != "ERROR":
        raise SystemExit(f"{language}: deadline should terminate after one in-flight attempt: {attempts!r}")
    if result[3] != "TIMEOUT":
        raise SystemExit(f"{language}: delayed Broker response was not classified as timeout: {result!r}")
    elapsed = int(result[-1])
    if elapsed < deadline_timeout - 200 or elapsed > response_delay + 5000:
        raise SystemExit(f"{language}: deadline elapsed outside the reviewed bound: {elapsed} ms")

delays = 0
injected = 0
for proxy in ("broker-a-proxy.tsv", "broker-b-proxy.tsv"):
    path = root / proxy
    rows = path.read_text().splitlines()
    delays += sum(line.startswith("PROXY\tDELAY_START\t") for line in rows)
    injected += sum(line.startswith("PROXY\tINJECTED_RESPONSE:16:") for line in rows)
if delays != 2:
    raise SystemExit(f"expected one delayed response for each language's deadline, observed {delays}")
if injected != 4:
    raise SystemExit(f"expected two injected NO_PERMISSION responses for each language, observed {injected}")
PY
    then
        echo "P7-V03 retry, status, deadline, body, or unique-ID assertions failed" >&2
        exit 1
    fi
    printf 'PASS P7-V03: Java and Rust each recovered on their third attempt after one injected NO_PERMISSION from both Brokers with stable body/unique ID, then exhausted the 900 ms send deadline after one delayed-response attempt\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P7-V02" ]]; then
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="$namesrv_addr $topic" exec:java@live-high-level-producer-probe \
        >"$evidence_dir/java-high-level-producer.tsv" \
        2>"$evidence_dir/java-high-level-producer.stderr"; then
        cat "$evidence_dir/java-high-level-producer.tsv" "$evidence_dir/java-high-level-producer.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        p7-producer "$namesrv_addr" "$topic" \
        >"$evidence_dir/rust-high-level-producer.tsv" \
        2>"$evidence_dir/rust-high-level-producer.stderr"; then
        cat "$evidence_dir/rust-high-level-producer.tsv" "$evidence_dir/rust-high-level-producer.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-high-level-producer.tsv" \
        "$evidence_dir/rust-high-level-producer.tsv" <<'PY'
import sys
from pathlib import Path

expected_methods = {"SYNC", "ASYNC", "ONEWAY", "EXPLICIT_QUEUE", "SELECTOR", "VALID_BATCH", "INVALID_BATCH"}

def parse(path):
    results = {}
    for line in Path(path).read_text().splitlines():
        if not line.startswith("P7\t"):
            continue
        row = line.split("\t")
        if len(row) < 3 or row[1] in results:
            raise SystemExit(f"invalid or duplicate producer result row: {line}")
        results[row[1]] = row[2:]
    if set(results) != expected_methods:
        raise SystemExit(f"producer methods differ from expected set: {sorted(results)}")
    return results

def normalize(results):
    sync = results["SYNC"]
    if len(sync) != 2 or sync[0] != "SEND_OK" or not sync[1].isdecimal():
        raise SystemExit("sync send must return SEND_OK and a queue ID")
    async_result = results["ASYNC"]
    if len(async_result) != 3 or async_result[0] != "1" or async_result[1] != "SEND_OK" or not async_result[2].isdecimal():
        raise SystemExit("async send must complete once with SEND_OK")
    if results["ONEWAY"] != ["NO_RESPONSE"]:
        raise SystemExit("oneway send must return without a Broker response")
    explicit = results["EXPLICIT_QUEUE"]
    if explicit != ["SEND_OK", "1"]:
        raise SystemExit("explicit-queue send must succeed on queue 1")
    selector = results["SELECTOR"]
    if selector != ["SEND_OK", "2"]:
        raise SystemExit("selector send must succeed on selected queue 2")
    batch = results["VALID_BATCH"]
    if len(batch) != 3 or batch[0] != "SEND_OK" or not batch[1].isdecimal() or batch[2] != "2":
        raise SystemExit("valid batch must send two messages successfully")
    if results["INVALID_BATCH"] != ["REJECTED"]:
        raise SystemExit("mixed-topic batch must be rejected")
    return {
        "SYNC": (sync[0],),
        "ASYNC": tuple(async_result[:2]),
        "ONEWAY": tuple(results["ONEWAY"]),
        "EXPLICIT_QUEUE": tuple(explicit),
        "SELECTOR": tuple(selector),
        "VALID_BATCH": (batch[0], batch[2]),
        "INVALID_BATCH": tuple(results["INVALID_BATCH"]),
    }

java = normalize(parse(sys.argv[1]))
rust = normalize(parse(sys.argv[2]))
if java != rust:
    raise SystemExit(f"Java/Rust high-level producer outcomes differ: {java!r} != {rust!r}")
PY
    then
        echo "Java/Rust high-level producer outcomes do not satisfy P7-V02" >&2
        exit 1
    fi
    printf 'PASS P7-V02: Java and Rust matched all seven high-level producer send modes\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P0-V03" ]]; then
    constituents=(
        "ROUTE-001-get-existing-topic:basic"
        "HEARTBEAT-001:basic"
        "PROD-001-sync-send:basic"
        "PROD-002-async-send:basic"
        "PROD-003-oneway-send:basic"
        "PROD-004-batch-send:basic"
        "CONS-001-query-offset:basic"
        "CONS-002-pull-found:basic"
        "CONS-003-pull-no-new-msg:basic"
        "CONS-004-update-offset:basic"
        "ORDER-001-lock-unlock:two-broker"
    )
    printf 'scenario\tstatus\tevidence\n' >"$evidence_dir/constituent-results.tsv"
    expected_revision="$(git -C "$repo_root" rev-parse HEAD)"
    for item in "${constituents[@]}"; do
        child_id="${item%%:*}"
        child_profile="${item#*:}"
        child_log="$evidence_dir/child-${child_id}.log"
        echo "Replaying $child_id in a fresh $child_profile project"
        if ! env -u COMPOSE_PROJECT_NAME \
            ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE="$child_profile" \
            bash "$repo_root/tests/differential/run.sh" --scenario "$child_id" \
            >"$child_log" 2>&1; then
            cat "$child_log" >&2
            exit 1
        fi
        if ! python3 - "$manifest" "$repo_root" "$child_id" "$expected_revision" \
            "$evidence_dir/constituent-results.tsv" <<'PY'
import json
import sys
from pathlib import Path

manifest_path, root, scenario_id, expected_revision, output = sys.argv[1:]
rows = json.loads(Path(manifest_path).read_text()).get("scenarios", [])
matches = [row for row in rows if row.get("id") == scenario_id]
if len(matches) != 1 or matches[0].get("live_status") != "PASS":
    raise SystemExit(f"constituent {scenario_id} has no passing manifest result")
evidence = matches[0].get("evidence")
if not isinstance(evidence, str) or not evidence:
    raise SystemExit(f"constituent {scenario_id} has no evidence pointer")
run_dir = Path(root) / evidence
metadata = dict(
    line.split("=", 1)
    for line in (run_dir / "metadata.txt").read_text().splitlines()
    if "=" in line
)
if metadata.get("source_revision") != expected_revision:
    raise SystemExit(f"constituent {scenario_id} ran a different source revision")
if metadata.get("source_code_dirty") != "false" or metadata.get("exit_status") != "0":
    raise SystemExit(f"constituent {scenario_id} did not pass from clean source and cleanup")
if not (run_dir / "result.txt").read_text().startswith("PASS "):
    raise SystemExit(f"constituent {scenario_id} result file is not PASS")
with Path(output).open("a") as stream:
    stream.write(f"{scenario_id}\tPASS\t{evidence}\n")
PY
        then
            echo "Constituent $child_id does not have clean passing evidence" >&2
            exit 1
        fi
    done
    printf 'PASS P0-V03: all %s Phase 0 route, producer, consumer, heartbeat, offset, and lock checks passed on %s\n' \
        "${#constituents[@]}" "$expected_revision" | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "ROUTE-001-get-existing-topic" || "$scenario_id" == "ROUTE-002-topic-not-exist" || "$scenario_id" == "P5-V02-route" ]]; then
    route_topics=("$topic")
    if [[ "$scenario_id" == "P5-V02-route" ]]; then
        route_topics+=("$missing_topic")
    fi
    route_rows=0
    for route_index in "${!route_topics[@]}"; do
        route_topic="${route_topics[$route_index]}"
        suffix=""
        if ((route_index)); then
            suffix="-missing"
        fi
        java_route_file="$evidence_dir/java-route${suffix}.tsv"
        rust_route_file="$evidence_dir/rust-route${suffix}.tsv"
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="$namesrv_addr $route_topic" exec:java@live-route-probe \
            >"$java_route_file" 2>"$evidence_dir/java-route${suffix}.stderr"; then
            cat "$java_route_file" "$evidence_dir/java-route${suffix}.stderr" 2>/dev/null >&2 || true
            exit 1
        fi
        if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- route "$namesrv_addr" "$route_topic" \
            >"$rust_route_file" 2>"$evidence_dir/rust-route${suffix}.stderr"; then
            cat "$evidence_dir/rust-route${suffix}.stderr" >&2
            exit 1
        fi
        if [[ ! -s "$java_route_file" || ! -s "$rust_route_file" ]]; then
            echo "A client produced empty route evidence for $route_topic" >&2
            exit 1
        fi
        if ! cmp -s "$java_route_file" "$rust_route_file"; then
            diff -u "$java_route_file" "$rust_route_file" \
                >"$evidence_dir/route${suffix}-diff.txt" || true
            cat "$evidence_dir/route${suffix}-diff.txt" >&2
            exit 1
        fi
        route_is_existing=true
        if [[ "$scenario_id" == "ROUTE-002-topic-not-exist" || "$suffix" == "-missing" ]]; then
            route_is_existing=false
        fi
        if [[ "$route_is_existing" == true ]]; then
            if ! awk -F '\t' -v topic="$route_topic" \
                'NF != 4 || $1 != "QUEUE" || $2 != topic || $3 != "broker-a" || $4 !~ /^[0-3]$/ || seen[$4]++ { exit 1 }
                 END { if (NR != 4) exit 1 }' "$java_route_file"; then
                echo "Route evidence must contain exactly queue IDs 0-3 for $route_topic on broker-a" >&2
                exit 1
            fi
            route_rows=$((route_rows + $(wc -l <"$java_route_file" | tr -d ' ')))
        else
            expected_route="$evidence_dir/expected-route${suffix}.tsv"
            printf 'NO_ROUTE\t%s\n' "$route_topic" >"$expected_route"
            if ! cmp -s "$expected_route" "$java_route_file"; then
                echo "Missing-topic scenario must produce exactly NO_ROUTE for $route_topic" >&2
                exit 1
            fi
            rm -f "$expected_route"
        fi
    done
    if [[ "$scenario_id" == "P5-V02-route" ]]; then
        printf 'PASS %s: both clients matched existing and missing topic routes (%s existing queue rows)\n' "$scenario_id" "$route_rows" \
            | tee "$evidence_dir/result.txt"
    else
        printf 'PASS %s: %s matching route rows\n' "$scenario_id" "$(wc -l <"$evidence_dir/java-route.tsv" | tr -d ' ')" \
            | tee "$evidence_dir/result.txt"
    fi
else
    echo "No executable assertion handler exists for $scenario_id" >&2
    exit 3
fi
echo "Evidence: $evidence_dir"
