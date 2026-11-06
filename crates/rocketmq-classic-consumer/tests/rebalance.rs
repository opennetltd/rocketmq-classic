use std::{collections::HashSet, sync::Arc};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_consumer::rebalance::{
    AllocateMessageQueueAveragely, AllocateMessageQueueAveragelyByCircle,
    AllocateMessageQueueByConfig, AllocateMessageQueueByMachineRoom,
    AllocateMessageQueueConsistentHash, AllocateMessageQueueStrategy, HashFunction, RebalanceError,
};

const JAVA_ALLOCATION_FIXTURE: &str = include_str!("../../../fixtures/rebalance/allocation.json");

fn fixture_queue(value: &serde_json::Value) -> MessageQueue {
    MessageQueue::new(
        value["topic"].as_str().expect("fixture topic"),
        value["brokerName"].as_str().expect("fixture broker name"),
        value["queueId"].as_i64().expect("fixture queue id") as i32,
    )
}

fn queue_key(queue: &MessageQueue) -> String {
    format!(
        "{}|{}|{}",
        queue.topic().expect("fixture topic"),
        queue.broker_name().expect("fixture broker name"),
        queue.queue_id()
    )
}

fn java_string_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

fn java_string_hash_code(value: &str) -> i32 {
    value.encode_utf16().fold(0i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn fixture_strategy(
    name: &str,
    config: &serde_json::Value,
) -> Box<dyn AllocateMessageQueueStrategy> {
    match name {
        "AVG" => Box::new(AllocateMessageQueueAveragely),
        "AVG_BY_CIRCLE" => Box::new(AllocateMessageQueueAveragelyByCircle),
        "CONFIG" => {
            let strategy = AllocateMessageQueueByConfig::default();
            strategy.set_message_queue_list(Some(
                config["messageQueueList"]
                    .as_array()
                    .expect("configured queue list")
                    .iter()
                    .map(fixture_queue)
                    .collect(),
            ));
            Box::new(strategy)
        }
        "MACHINE_ROOM" => {
            let strategy = AllocateMessageQueueByMachineRoom::default();
            strategy.set_consumeridcs(Some(
                config["consumeridcs"]
                    .as_array()
                    .expect("machine room IDs")
                    .iter()
                    .map(|value| value.as_str().expect("machine room ID").to_owned())
                    .collect(),
            ));
            Box::new(strategy)
        }
        "CONSISTENT_HASH" => Box::new(
            AllocateMessageQueueConsistentHash::with_virtual_node_count(
                config["virtualNodeCnt"]
                    .as_i64()
                    .expect("virtual node count") as i32,
            )
            .expect("fixture virtual node count"),
        ),
        "CONSISTENT_HASH_CUSTOM" => {
            let hash_function: Arc<dyn HashFunction> =
                Arc::new(|key: &str| i64::from(java_string_hash_code(key) % 7 - 3));
            Box::new(
                AllocateMessageQueueConsistentHash::with_hash_function(
                    config["virtualNodeCnt"]
                        .as_i64()
                        .expect("virtual node count") as i32,
                    hash_function,
                )
                .expect("fixture virtual node count"),
            )
        }
        _ => panic!("unknown Java allocation strategy {name}"),
    }
}

fn expected_keys(value: &serde_json::Value) -> Vec<String> {
    value
        .as_array()
        .expect("Java assignment list")
        .iter()
        .map(|value| value.as_str().expect("Java queue key").to_owned())
        .collect()
}

fn strategy_configured_keys(config: &serde_json::Value) -> Vec<String> {
    config["messageQueueList"]
        .as_array()
        .expect("configured queue list")
        .iter()
        .map(fixture_queue)
        .map(|queue| queue_key(&queue))
        .collect()
}

fn assert_config_result(
    result: Result<Vec<MessageQueue>, RebalanceError>,
    config: &serde_json::Value,
) {
    assert_eq!(
        result
            .expect("config strategy does not validate allocation inputs")
            .iter()
            .map(queue_key)
            .collect::<Vec<_>>(),
        strategy_configured_keys(config)
    );
}

#[test]
fn all_java_allocation_strategies_match_551_fixtures() {
    let fixture: serde_json::Value =
        serde_json::from_str(JAVA_ALLOCATION_FIXTURE).expect("Java allocation fixture JSON");
    let mut checked_cases = 0;

    for case in fixture["cases"].as_array().expect("fixture cases") {
        checked_cases += 1;
        let strategy_name = case["strategy"].as_str().expect("strategy name");
        let strategy = fixture_strategy(strategy_name, &case["strategyConfig"]);
        assert_eq!(
            strategy.name(),
            case["strategyName"].as_str().expect("Java strategy name")
        );
        let mut queues = case["queues"]
            .as_array()
            .expect("fixture queue list")
            .iter()
            .map(fixture_queue)
            .collect::<Vec<_>>();
        let mut consumer_ids = case["consumerIds"]
            .as_array()
            .expect("fixture client IDs")
            .iter()
            .map(|value| value.as_str().expect("fixture client ID").to_owned())
            .collect::<Vec<_>>();
        queues.sort();
        consumer_ids.sort_by(|left, right| java_string_cmp(left, right));

        let validation = &case["validation"];
        let expected_empty_current = validation["emptyCurrentClient"]
            .as_str()
            .expect("empty current validation result");
        let empty_current = strategy.allocate("group", "", &queues, &consumer_ids);
        if expected_empty_current == "NONE" {
            assert_config_result(empty_current, &case["strategyConfig"]);
        } else {
            assert_eq!(
                expected_empty_current, "currentCID is empty",
                "{}",
                case["id"]
            );
            assert_eq!(
                empty_current,
                Err(RebalanceError::EmptyCurrentClient),
                "{}",
                case["id"]
            );
        }

        let expected_empty_queues = validation["emptyQueueList"]
            .as_str()
            .expect("empty queue validation result");
        let current_client = consumer_ids.first().map_or("consumer-a", String::as_str);
        let empty_queues = strategy.allocate("group", current_client, &[], &consumer_ids);
        if expected_empty_queues == "NONE" {
            assert_config_result(empty_queues, &case["strategyConfig"]);
        } else {
            assert_eq!(
                expected_empty_queues, "mqAll is null or mqAll empty",
                "{}",
                case["id"]
            );
            assert_eq!(
                empty_queues,
                Err(RebalanceError::EmptyMessageQueueList),
                "{}",
                case["id"]
            );
        }

        let expected_empty_consumers = validation["emptyConsumerIdList"]
            .as_str()
            .expect("empty consumer validation result");
        let empty_consumers = strategy.allocate("group", "consumer-a", &queues, &[]);
        if expected_empty_consumers == "NONE" {
            assert_config_result(empty_consumers, &case["strategyConfig"]);
        } else if queues.is_empty() {
            assert_eq!(
                expected_empty_consumers, "mqAll is null or mqAll empty",
                "{}",
                case["id"]
            );
            assert_eq!(
                empty_consumers,
                Err(RebalanceError::EmptyMessageQueueList),
                "{}",
                case["id"]
            );
        } else {
            assert_eq!(
                expected_empty_consumers, "cidAll is null or cidAll empty",
                "{}",
                case["id"]
            );
            assert_eq!(
                empty_consumers,
                Err(RebalanceError::EmptyConsumerIdList),
                "{}",
                case["id"]
            );
        }

        if queues.is_empty() && strategy_name != "CONFIG" {
            for client_id in &consumer_ids {
                assert!(
                    strategy
                        .allocate("group", client_id, &queues, &consumer_ids)
                        .is_err(),
                    "{} empty queue list",
                    case["id"]
                );
            }
            continue;
        }

        let mut all_assignments = Vec::new();
        for client_id in &consumer_ids {
            let expected = expected_keys(&case["assignments"][client_id.as_str()]);
            let actual = strategy
                .allocate("group", client_id, &queues, &consumer_ids)
                .expect("valid Java strategy allocation")
                .iter()
                .map(queue_key)
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "{} client {client_id}", case["id"]);
            all_assignments.extend(actual);
        }

        let expected_missing = expected_keys(&case["assignments"]["missing-client"]);
        let missing = strategy
            .allocate("group", "missing-client", &queues, &consumer_ids)
            .expect("Java missing-client result is not an error");
        assert_eq!(
            missing.iter().map(queue_key).collect::<Vec<_>>(),
            expected_missing
        );

        if strategy_name != "CONFIG" {
            let unique = all_assignments.iter().cloned().collect::<HashSet<_>>();
            assert_eq!(unique.len(), all_assignments.len(), "disjoint assignments");
            let expected_queues = queues.iter().map(queue_key).collect::<HashSet<_>>();
            assert_eq!(unique, expected_queues, "all eligible queues assigned once");
        }
    }

    assert_eq!(
        checked_cases, 30,
        "five Java strategies and custom hash across five scenarios"
    );
}

#[test]
fn allocation_strategy_options_match_java_defaults_and_validation() {
    let by_config = AllocateMessageQueueByConfig::default();
    assert_eq!(
        by_config.allocate("", "", &[], &[]).unwrap(),
        Vec::<MessageQueue>::new()
    );

    let by_machine_room = AllocateMessageQueueByMachineRoom::default();
    let queue = MessageQueue::new("orders", "roomA@broker@extra", 0);
    let client_ids = vec!["client".to_owned()];
    assert_eq!(
        by_machine_room.allocate("g", "client", std::slice::from_ref(&queue), &client_ids),
        Err(RebalanceError::MissingMachineRoomConfiguration)
    );
    by_machine_room.set_consumeridcs(Some(HashSet::from(["roomA".to_owned()])));
    assert!(
        by_machine_room
            .allocate("g", "client", std::slice::from_ref(&queue), &client_ids)
            .unwrap()
            .is_empty()
    );

    let mut split_queues = vec![
        MessageQueue::new("orders", "roomA@broker@", 0),
        MessageQueue::new("orders", "roomA@", 1),
        MessageQueue::new("orders", "@broker", 2),
        MessageQueue::new("orders", "roomA@broker@extra", 3),
    ];
    split_queues.sort();
    by_machine_room.set_consumeridcs(Some(HashSet::from(["".to_owned(), "roomA".to_owned()])));
    let split_allocated = by_machine_room
        .allocate("g", "client", &split_queues, &client_ids)
        .unwrap()
        .iter()
        .map(queue_key)
        .collect::<Vec<_>>();
    assert_eq!(
        split_allocated,
        vec!["orders|@broker|2", "orders|roomA@broker@|0"]
    );

    assert_eq!(
        AllocateMessageQueueConsistentHash::new().virtual_node_count(),
        10
    );
    assert!(matches!(
        AllocateMessageQueueConsistentHash::with_virtual_node_count(-1),
        Err(RebalanceError::InvalidVirtualNodeCount(-1))
    ));
    assert_eq!(
        AllocateMessageQueueConsistentHash::with_virtual_node_count(0)
            .unwrap()
            .virtual_node_count(),
        0
    );
    let zero_nodes = AllocateMessageQueueConsistentHash::with_virtual_node_count(0).unwrap();
    assert!(
        zero_nodes
            .allocate("g", "client", std::slice::from_ref(&queue), &client_ids)
            .unwrap()
            .is_empty()
    );
}
