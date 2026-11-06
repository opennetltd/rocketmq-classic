use std::{collections::BTreeMap, sync::Arc};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_consumer::{
    ConsumeConcurrentlyStatus, PullConsumerConfig, PushConsumer, PushConsumerConfig,
    PushMessageListener,
    offset::{OffsetStore, RemoteBrokerOffsetStore},
    pull::PullConsumer,
};
use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, ConsumerRuntime, RuntimeError},
};
use rocketmq_classic_protocol::heartbeat::ConsumerData;
use serde_json::{Value, json};

struct OtherConsumer;
impl ConsumerRuntime for OtherConsumer {
    fn heartbeat_data(&self) -> ConsumerData {
        ConsumerData::default()
    }
}

#[tokio::test]
async fn runtime_consumer_status_matches_direct_java_calls_and_live_store_changes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/offsets.json")).unwrap();
    let manager = ClientRuntimeManager::shared_default();
    let mut client_config = ClientConfig::new("runtime-consumer-status");
    client_config.set_instance_name(Some("status-fixture".into()));
    client_config.set_namesrv_addr(Some(String::new()));
    let runtime = manager.get_or_create(&client_config).await.unwrap();
    let pull_store = Arc::new(RemoteBrokerOffsetStore::new(runtime.clone(), "pull"));
    let push_store = Arc::new(RemoteBrokerOffsetStore::new(runtime.clone(), "push"));
    let queue = MessageQueue::new("orders", "broker", 0);
    pull_store.update_offset(&queue, 42, false);
    push_store.update_offset(&queue, 42, false);

    let mut pull_config = PullConsumerConfig::new("pull").unwrap();
    *pull_config.client_config_mut() = client_config.clone();
    let pull = Arc::new(PullConsumer::new(pull_config));
    pull.set_offset_store(Some(pull_store.clone()))
        .await
        .unwrap();
    pull.start().await.unwrap();
    let mut push_config = PushConsumerConfig::new("push").unwrap();
    *push_config.client_config_mut() = client_config;
    let push = Arc::new(PushConsumer::new(push_config));
    push.set_offset_store(Some(push_store.clone()))
        .await
        .unwrap();
    push.register_message_listener(PushMessageListener::concurrently(|_, context| async move {
        (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
    }))
    .await
    .unwrap();
    push.start().await.unwrap();
    runtime
        .consumer_table()
        .put("other", Arc::new(OtherConsumer))
        .await
        .unwrap();

    runtime
        .remoting()
        .with_name_server_address_list(|addresses| addresses.unwrap().clear());
    for (group, consume_type) in [("pull", "CONSUME_ACTIVELY"), ("push", "CONSUME_PASSIVELY")] {
        let info = runtime.consumer_running_info(Some(group)).unwrap().unwrap();
        let properties = info.get_properties().unwrap().snapshot();
        assert_eq!(properties["PROP_CONSUME_TYPE"], consume_type);
        assert_eq!(properties["PROP_CLIENT_VERSION"], "V5_5_1");
        assert_eq!(properties["PROP_NAMESERVER_ADDR"], "");
        assert!(info.get_jstack().is_none());
    }
    assert!(
        runtime
            .consumer_running_info(Some("missing"))
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        runtime.consumer_running_info(None),
        Err(RuntimeError::InvalidRoute(_))
    ));

    let cases = fixture["consumerStatusCases"].as_array().unwrap();
    assert_eq!(cases.len(), 54);
    for case in cases {
        let result = runtime.get_consumer_status(case["topic"].as_str(), case["group"].as_str());
        if case["error"].is_null() {
            let table = result.unwrap().into_iter().collect::<BTreeMap<_, _>>();
            let rows = table
                .into_iter()
                .map(|(queue, offset)| {
                    json!({"topic": queue.topic(), "brokerName": queue.broker_name(),
                        "queueId": queue.queue_id(), "offset": offset})
                })
                .collect::<Vec<_>>();
            assert_eq!(json!(rows), case["output"], "{case}");
        } else {
            assert_eq!(case["error"], "NullPointerException");
            assert!(matches!(result, Err(RuntimeError::InvalidRoute(_))));
        }
    }

    let mut detached = runtime.get_consumer_status(None, Some("pull")).unwrap();
    detached.clear();
    pull_store.update_offset(&queue, 73, false);
    assert_eq!(
        runtime.get_consumer_status(None, Some("pull")).unwrap()[&queue],
        73
    );
    assert_eq!(
        runtime.get_consumer_status(None, Some("push")).unwrap()[&queue],
        42
    );
    push.set_offset_store(Some(pull_store.clone()))
        .await
        .unwrap();
    assert_eq!(
        runtime.get_consumer_status(None, Some("push")).unwrap()[&queue],
        fixture["consumerStatusStoreTransitions"]["pushReplacedStore"][0]["offset"]
            .as_i64()
            .unwrap()
    );
    push.set_offset_store(None).await.unwrap();
    assert_eq!(
        fixture["consumerStatusStoreTransitions"]["pushNullStoreError"],
        "NullPointerException"
    );
    assert!(matches!(
        runtime.get_consumer_status(None, Some("push")),
        Err(RuntimeError::InvalidOffsetTable(_))
    ));
    push.set_offset_store(Some(push_store.clone()))
        .await
        .unwrap();
    runtime.consumer_table().remove("other").await.unwrap();
    pull.shutdown().await.unwrap();
    assert!(
        runtime
            .get_consumer_status(None, Some("pull"))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        runtime.get_consumer_status(None, Some("push")).unwrap()[&queue],
        42
    );
    push.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}
