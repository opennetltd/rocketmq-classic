use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_consumer::{
    ConsumeOrderlyContext, ConsumeOrderlyStatus, ProcessQueue, PushConsumerConfig,
};

#[test]
fn public_orderly_settings_context_and_statuses_match_java_551_fixture() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/consumer/orderly-settings.json"
    ))
    .expect("Java orderly fixture is valid JSON");

    let lock_constants = &fixture["lockConstants"];
    assert_eq!(
        ProcessQueue::rebalance_lock_max_live_time_millis().unwrap(),
        lock_constants["maxLiveTimeMillis"]
    );
    assert_eq!(
        ProcessQueue::rebalance_lock_interval_millis().unwrap(),
        lock_constants["intervalMillis"]
    );

    let config = PushConsumerConfig::new("orderly-fixture").unwrap();
    let defaults = &fixture["pushConsumerDefaults"];
    assert_eq!(config.max_reconsume_times(), defaults["maxReconsumeTimes"]);
    assert_eq!(
        config.suspend_current_queue_time_millis(),
        defaults["suspendCurrentQueueTimeMillis"]
    );
    assert_eq!(
        config.consume_timeout_minutes(),
        defaults["consumeTimeoutMinutes"]
    );
    assert_eq!(
        config.consume_message_batch_max_size(),
        defaults["consumeMessageBatchMaxSize"]
    );
    assert_eq!(config.pull_batch_size(), defaults["pullBatchSize"]);
    assert_eq!(
        config.pull_batch_size_in_bytes(),
        defaults["pullBatchSizeInBytes"]
    );

    config.set_max_reconsume_times(
        fixture["configuredPublicSettings"]["maxReconsumeTimes"]
            .as_i64()
            .unwrap() as i32,
    );
    config.set_suspend_current_queue_time_millis(
        fixture["configuredPublicSettings"]["suspendCurrentQueueTimeMillis"]
            .as_i64()
            .unwrap(),
    );
    config.set_pull_batch_size(
        fixture["configuredPublicSettings"]["pullBatchSize"]
            .as_i64()
            .unwrap() as i32,
    );
    config.set_pull_batch_size_in_bytes(
        fixture["configuredPublicSettings"]["pullBatchSizeInBytes"]
            .as_i64()
            .unwrap() as i32,
    );
    assert_eq!(config.max_reconsume_times(), 7);
    assert_eq!(config.pull_batch_size(), 7);
    assert_eq!(config.pull_batch_size_in_bytes(), 12_345);

    let queue_fixture = &fixture["orderlyContextDefaults"]["messageQueue"];
    let queue = MessageQueue::new(
        queue_fixture["topic"].as_str().unwrap(),
        queue_fixture["brokerName"].as_str().unwrap(),
        queue_fixture["queueId"].as_i64().unwrap() as i32,
    );
    let mut context = ConsumeOrderlyContext::new(queue.clone());
    assert_eq!(context.message_queue(), &queue);
    assert_eq!(
        context.auto_commit(),
        fixture["orderlyContextDefaults"]["autoCommit"]
    );
    assert_eq!(
        context.suspend_current_queue_time_millis(),
        fixture["orderlyContextDefaults"]["suspendCurrentQueueTimeMillis"]
    );
    context.set_auto_commit(false);
    context.set_suspend_current_queue_time_millis(7_654);
    assert_eq!(
        context.auto_commit(),
        fixture["mutatedContext"]["autoCommit"]
    );
    assert_eq!(
        context.suspend_current_queue_time_millis(),
        fixture["mutatedContext"]["suspendCurrentQueueTimeMillis"]
    );

    let statuses = [
        ("SUCCESS", ConsumeOrderlyStatus::Success),
        ("ROLLBACK", ConsumeOrderlyStatus::Rollback),
        ("COMMIT", ConsumeOrderlyStatus::Commit),
        (
            "SUSPEND_CURRENT_QUEUE_A_MOMENT",
            ConsumeOrderlyStatus::SuspendCurrentQueueAMoment,
        ),
    ];
    let expected_statuses = fixture["orderlyStatuses"].as_array().unwrap();
    assert_eq!(statuses.len(), expected_statuses.len());
    for ((name, _), expected) in statuses.iter().zip(expected_statuses) {
        assert_eq!(*name, expected.as_str().unwrap());
    }

    assert_eq!(fixture["lockConstants"]["maxLiveTimeMillis"], 30_000);
    assert_eq!(fixture["lockConstants"]["intervalMillis"], 20_000);
    assert_eq!(
        fixture["orderlyRuntimeProperties"]["unlockDelayTimeMillis"],
        20_000
    );
    assert_eq!(
        fixture["orderlyRuntimeProperties"]["maxContinuousConsumeMillis"],
        60_000
    );
}

#[test]
fn process_queue_rebalance_lock_properties_follow_java_static_initialization() {
    const CHILD_MARKER: &str = "ROCKETMQ_TEST_REBALANCE_LOCK_SETTINGS_CHILD";
    const MAX_LIVE_TIME_PROPERTY: &str = "rocketmq.client.rebalance.lockMaxLiveTime";
    const INTERVAL_PROPERTY: &str = "rocketmq.client.rebalance.lockInterval";

    if std::env::var_os(CHILD_MARKER).is_some() {
        assert_eq!(
            ProcessQueue::rebalance_lock_max_live_time_millis().unwrap(),
            45_123
        );
        assert_eq!(
            ProcessQueue::rebalance_lock_interval_millis().unwrap(),
            23_456
        );

        // Java reads both System properties into static fields once, when the
        // class initializes. Changing the Rust process environment after the
        // first accessor call must leave both captured values unchanged.
        unsafe {
            std::env::set_var(MAX_LIVE_TIME_PROPERTY, "10");
            std::env::set_var(INTERVAL_PROPERTY, "20");
        }
        assert_eq!(
            ProcessQueue::rebalance_lock_max_live_time_millis().unwrap(),
            45_123
        );
        assert_eq!(
            ProcessQueue::rebalance_lock_interval_millis().unwrap(),
            23_456
        );
        return;
    }

    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "process_queue_rebalance_lock_properties_follow_java_static_initialization",
            "--nocapture",
        ])
        .env(CHILD_MARKER, "1")
        .env(MAX_LIVE_TIME_PROPERTY, "45123")
        .env(INTERVAL_PROPERTY, "23456")
        .output()
        .expect("spawn isolated lock-property test process");
    assert!(
        output.status.success(),
        "child test failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
