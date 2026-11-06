#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

run_repeated() {
    local label="$1"
    shift
    for pass in 1 2 3; do
        printf 'stress smoke %s pass %s/3\n' "$label" "$pass"
        local output
        if ! output="$("$@" 2>&1)"; then
            printf '%s\n' "$output"
            return 1
        fi
        printf '%s\n' "$output"
        if [[ "$output" == *"running 0 tests"* || "$output" != *"test result: ok"* ]]; then
            printf 'stress smoke did not execute a passing test: %s\n' "$label" >&2
            return 1
        fi
    done
}

run_repeated remoting-pending-and-opaque \
    cargo test --locked -p rocketmq-classic-remoting --lib pending_terminal_race_and_opaque_wrap_never_replace_live_entries
run_repeated remoting-deadline-cancel-shutdown \
    cargo test --locked -p rocketmq-classic-remoting --lib wire_response_timeout_cancellation_and_shutdown_race_leaves_no_pending
run_repeated remoting-connection-publication \
    cargo test --locked -p rocketmq-classic-remoting --lib connection_creation_racing_shutdown_closes_unpublished_or_published_socket
run_repeated remoting-start-stop-ownership \
    cargo test --locked -p rocketmq-classic-remoting --lib start_and_shutdown_race_leaves_no_owned_tasks_or_connections
run_repeated remoting-nameserver-scan-abort-order \
    cargo test --locked -p rocketmq-classic-remoting --lib client::tests::name_server_scan_batch_matches_java_abort_policy_submission_order
run_repeated remoting-public-executor-shutdown-boundary \
    cargo test --offline --locked -p rocketmq-classic-remoting --lib shutdown_waits_for_channel_event_worker_only_until_java_join_limit
run_repeated remoting-reconnect \
    cargo test --locked -p rocketmq-classic-remoting --test transport p3_go_away_retries_once_on_a_new_connection
run_repeated remoting-pending-callback-shutdown \
    cargo test --locked -p rocketmq-classic-remoting --test transport p3_shutdown_drains_callback_from_pending_invoke
run_repeated runtime-concurrent-construction \
    cargo test --locked -p rocketmq-classic-core --test runtime p6_v01_many_first_lookups_share_one_unstarted_runtime_and_transport_owner
run_repeated request-reply-terminal-owner \
    cargo test --locked -p rocketmq-classic-core --lib concurrent_reply_and_timeout_claim_exactly_one_terminal_owner
run_repeated request-reply-send-cancel-race \
    cargo test --locked -p rocketmq-classic-core --lib reply_races_with_send_failure_or_cancellation_without_double_claim
run_repeated request-reply-admission-cleanup \
    cargo test --locked -p rocketmq-classic-core --lib pending_reply_slots_are_bounded_until_terminal_removal
run_repeated producer-callback-timeout-shutdown \
    cargo test --locked -p rocketmq-classic-producer --lib callback_timeout_uses_java_housekeeping_initial_delay_and_period
run_repeated producer-two-thousand-concurrent-sends \
    cargo test --locked -p rocketmq-classic-producer --test scripted_send producer_handles_two_thousand_concurrent_async_sends_without_losing_requests
run_repeated consumer-assignment-churn \
    cargo test --locked -p rocketmq-classic-consumer --lib clustering_membership_join_leave_and_unavailable_cycles_converge
run_repeated consumer-offset-callback-fence \
    cargo test --locked -p rocketmq-classic-consumer --lib reset_offset_fences_a_slow_callback_and_persists_only_the_new_value
run_repeated consumer-offset-late-pull-fence \
    cargo test --locked -p rocketmq-classic-consumer --lib reset_offset_fences_a_late_pull_response_before_persisting_the_new_value
run_repeated consumer-long-offset-persistence \
    cargo test --locked -p rocketmq-classic-consumer --lib offset::local::tests::repeated_offset_persistence_keeps_long_running_snapshot_restorable
run_repeated consumer-stale-generation-fence \
    cargo test --locked -p rocketmq-classic-consumer --lib stale_and_dropped_lock_responses_cannot_restore_queue_ownership
run_repeated orderly-slow-callback-shutdown \
    cargo test --locked -p rocketmq-classic-consumer --lib orderly_rebalance_bounds_slow_callback_wait_and_never_commits_dropped_work

printf 'offline deterministic race/leak smoke passed: 20 cases x 3 repetitions\n'
