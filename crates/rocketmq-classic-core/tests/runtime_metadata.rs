use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, RuntimeState},
};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

fn millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[tokio::test(start_paused = true)]
async fn boot_timestamp_does_not_follow_simulated_monotonic_clock() {
    let mut config = ClientConfig::new("metadata-clock");
    config.set_namesrv_addr(Some(String::new()));
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let boot = runtime.boot_timestamp();
    tokio::time::advance(std::time::Duration::from_secs(86_400)).await;
    assert_eq!(runtime.boot_timestamp(), boot);
}

#[tokio::test]
async fn boot_timestamp_is_constructor_wall_clock_and_stable_across_shared_lifecycle() {
    let java_fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
    assert!(
        java_fixture["bootTimestampInsideConstructorWindow"]
            .as_bool()
            .unwrap()
    );
    assert!(
        java_fixture["bootTimestampStableAfterFixtureCalls"]
            .as_bool()
            .unwrap()
    );

    let manager = ClientRuntimeManager::new();
    let mut config = ClientConfig::new("metadata");
    config.set_namesrv_addr(Some(String::new()));
    let before = millis();
    let runtime = manager.get_or_create(&config).await.unwrap();
    let after = millis();
    let boot = runtime.boot_timestamp();
    assert!((before..=after).contains(&boot));
    assert_eq!(runtime.state(), RuntimeState::Created);
    let shared = manager.get_or_create(&config).await.unwrap();
    assert!(Arc::ptr_eq(&runtime, &shared));
    assert_eq!(shared.boot_timestamp(), boot);
    runtime.start().await.unwrap();
    assert_eq!(runtime.boot_timestamp(), boot);
    runtime.shutdown().await.unwrap();
    assert_eq!(runtime.boot_timestamp(), boot);
    let before = millis();
    let replacement = manager.get_or_create(&config).await.unwrap();
    let after = millis();
    assert!(!Arc::ptr_eq(&runtime, &replacement));
    assert!((before..=after).contains(&replacement.boot_timestamp()));
}

#[tokio::test]
#[allow(clippy::mutable_key_type)] // The Java-compatible result has mutable MessageQueue keys.
async fn explicit_offset_namespace_conversion_matches_invoked_java() {
    use indexmap::IndexMap;
    use rocketmq_classic_common::message::MessageQueue;
    use rocketmq_classic_core::runtime::{BrokerOffsetTable, RuntimeError};
    use rocketmq_classic_protocol::SharedValue;
    use serde_json::{Value, json};

    fn sorted_rows(mut rows: Vec<Value>, nullable: bool) -> Vec<Value> {
        rows.sort_by(|left, right| {
            let left_queue = if nullable { &left["queue"] } else { left };
            let right_queue = if nullable { &right["queue"] } else { right };
            let left_is_null = nullable && left_queue.is_null();
            let right_is_null = nullable && right_queue.is_null();
            left_is_null
                .cmp(&right_is_null)
                .then_with(|| {
                    left_queue["topic"]
                        .as_str()
                        .cmp(&right_queue["topic"].as_str())
                })
                .then_with(|| {
                    left_queue["brokerName"]
                        .as_str()
                        .cmp(&right_queue["brokerName"].as_str())
                })
                .then_with(|| {
                    left_queue["queueId"]
                        .as_i64()
                        .cmp(&right_queue["queueId"].as_i64())
                })
        });
        rows
    }

    fn rows_from_entries(
        entries: impl IntoIterator<Item = (Option<SharedValue<MessageQueue>>, Option<i64>)>,
        nullable: bool,
    ) -> Vec<Value> {
        let rows = entries
            .into_iter()
            .map(|(queue, offset)| {
                let queue = queue.map(|queue| queue.snapshot());
                if nullable {
                    let queue = queue.map_or(Value::Null, |queue| {
                        json!({
                            "topic": queue.topic(),
                            "brokerName": queue.broker_name(),
                            "queueId": queue.queue_id(),
                        })
                    });
                    json!({ "queue": queue, "offset": offset })
                } else {
                    let queue = queue.expect("non-nullable fixture has a queue key");
                    json!({
                        "topic": queue.topic(),
                        "brokerName": queue.broker_name(),
                        "queueId": queue.queue_id(),
                        "offset": offset,
                    })
                }
            })
            .collect();
        sorted_rows(rows, nullable)
    }

    fn table_rows(table: &BrokerOffsetTable, nullable: bool) -> Vec<Value> {
        let entries = table.with(|table| {
            table
                .iter()
                .map(|(queue, offset)| (queue.clone(), *offset))
                .collect::<Vec<_>>()
        });
        rows_from_entries(entries, nullable)
    }

    fn map_rows(
        table: &std::collections::HashMap<Option<SharedValue<MessageQueue>>, Option<i64>>,
        nullable: bool,
    ) -> Vec<Value> {
        rows_from_entries(
            table.iter().map(|(queue, offset)| (queue.clone(), *offset)),
            nullable,
        )
    }

    fn fixture_rows(value: &Value, nullable: bool) -> Vec<Value> {
        sorted_rows(value.as_array().unwrap().clone(), nullable)
    }

    fn table(value: &Value, case_name: Option<&str>) -> Option<BrokerOffsetTable> {
        if value.is_null() {
            return None;
        }
        let mut entries = value
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                let nested_queue = row.get("queue");
                let queue = nested_queue.unwrap_or(row);
                let queue = if nested_queue.is_some() && queue.is_null() {
                    None
                } else {
                    Some(SharedValue::new(MessageQueue::new_nullable(
                        queue["topic"].as_str().map(str::to_owned),
                        queue["brokerName"].as_str().map(str::to_owned),
                        queue["queueId"].as_i64().unwrap() as i32,
                    )))
                };
                (queue, row["offset"].as_i64())
            })
            .collect::<Vec<_>>();

        // The Java oracle constructs this nullable input as a LinkedHashMap,
        // inserting the valid key before the null key. Its exported rows are
        // sorted for fixture stability, so restore that source iteration order.
        if case_name == Some("namespace-null-key-after-valid-queue") {
            entries.sort_by_key(|(queue, _)| queue.is_none());
        }
        Some(SharedValue::new(
            entries.into_iter().collect::<IndexMap<_, _>>(),
        ))
    }

    fn assert_returned_keys_alias_input(
        input: &BrokerOffsetTable,
        output: &std::collections::HashMap<Option<SharedValue<MessageQueue>>, Option<i64>>,
    ) {
        let input_keys =
            input.with(|table| table.keys().filter_map(Clone::clone).collect::<Vec<_>>());
        for key in output.keys().flatten() {
            assert!(input_keys.iter().any(|input_key| input_key.ptr_eq(key)));
        }
    }

    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/offsets.json")).unwrap();
    let mut config = ClientConfig::new("offset-client");
    config.set_namespace(Some("ignored-config-namespace".into()));
    config.set_namesrv_addr(Some(String::new()));
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let input = table(&case["input"], None).unwrap();
        let output = runtime
            .parse_offset_table_from_broker(Some(&input), case["namespace"].as_str())
            .unwrap();
        assert_eq!(
            map_rows(&output, false),
            fixture_rows(&case["output"], false)
        );
        assert_eq!(
            table_rows(&input, false),
            fixture_rows(&case["mutatedInput"], false)
        );
        assert_returned_keys_alias_input(&input, &output);
        assert_eq!(
            runtime.config().write().unwrap().get_namespace().unwrap(),
            Some("ignored-config-namespace")
        );
    }
    for case in fixture["nullableCases"].as_array().unwrap() {
        let case_name = case["name"].as_str();
        let input = table(&case["input"], case_name);
        let result =
            runtime.parse_offset_table_from_broker(input.as_ref(), case["namespace"].as_str());
        if let Some(input) = input.as_ref() {
            assert_eq!(
                table_rows(input, true),
                fixture_rows(&case["mutatedInput"], true),
                "{}",
                case_name.unwrap()
            );
        }
        if case["error"].is_null() {
            let output = result.unwrap();
            assert_eq!(
                map_rows(&output, true),
                fixture_rows(&case["output"], true),
                "{}",
                case_name.unwrap()
            );
            if let Some(input) = input.as_ref() {
                assert_returned_keys_alias_input(input, &output);
            }
        } else {
            let expected_reason = match case_name.unwrap() {
                "null-input-map"
                | "null-input-map-empty-namespace"
                | "null-input-map-nonempty-namespace" => "null offset table",
                "namespace-null-key-after-valid-queue" => "null queue key with namespace",
                name => panic!("unexpected Java offset-table error case {name}"),
            };
            match result {
                Err(RuntimeError::InvalidOffsetTable(reason)) => {
                    assert_eq!(reason, expected_reason, "{}", case_name.unwrap())
                }
                other => panic!("{}: {other:?}", case_name.unwrap()),
            }
        }
    }
}
