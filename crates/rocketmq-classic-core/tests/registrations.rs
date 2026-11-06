use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, ConsumerRuntime, ProducerRuntime, RuntimeError, RuntimeState},
};
use rocketmq_classic_protocol::{
    SharedValue,
    heartbeat::{
        ConsumeFromWhere, ConsumeType, ConsumerData, ConsumerSubscriptionDataSet, HeartbeatData,
        MessageModel, SubscriptionData,
    },
};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};
use tokio::time::{Duration, Instant};

struct Producer;
impl ProducerRuntime for Producer {}

struct Consumer(Mutex<ConsumerData>);
impl ConsumerRuntime for Consumer {
    fn heartbeat_data(&self) -> ConsumerData {
        self.0.lock().unwrap().clone()
    }
}

fn java_registered_consumer_fields() -> ConsumerData {
    ConsumerData {
        consume_type: Some(ConsumeType::ConsumeActively),
        message_model: Some(MessageModel::Clustering),
        consume_from_where: Some(ConsumeFromWhere::ConsumeFromLastOffset),
        ..Default::default()
    }
}

fn shared_subscription_set(
    values: impl IntoIterator<Item = SubscriptionData>,
) -> SharedValue<ConsumerSubscriptionDataSet> {
    SharedValue::from(
        values
            .into_iter()
            .map(|value| Some(SharedValue::from(value)))
            .collect::<ConsumerSubscriptionDataSet>(),
    )
}

fn first_shared<T: Clone>(set: &SharedValue<HashSet<Option<SharedValue<T>>>>) -> T {
    set.with(|values| {
        values
            .iter()
            .flatten()
            .next()
            .expect("one set element")
            .snapshot()
    })
}

fn compare_java(name: &str, actual: &HeartbeatData) {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/heartbeat.json")).unwrap();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["case"] == name)
        .unwrap();
    let expected: HeartbeatData = serde_json::from_value(case["body"].clone()).unwrap();
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap(),
        "{name}"
    );
}

#[tokio::test]
async fn nullable_registration_and_group_removal_match_invoked_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
    let java = &fixture["runtimeLookupInvocation"];
    for field in [
        "nullGroupConsumerRegistrationAccepted",
        "nullOwnerConsumerRegistrationAccepted",
        "nullGroupProducerRegistrationAccepted",
        "nullOwnerProducerRegistrationAccepted",
    ] {
        assert_eq!(java[field], false, "{field}");
    }
    assert_eq!(java["registrationTablesRemainEmpty"], true);
    assert_eq!(
        java["nullConsumerUnregisterOutcome"],
        "NullPointerException"
    );
    assert_eq!(
        java["nullProducerUnregisterOutcome"],
        "NullPointerException"
    );

    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("null-registration"))
        .await
        .unwrap();
    let consumer: Arc<dyn ConsumerRuntime> =
        Arc::new(Consumer(Mutex::new(ConsumerData::default())));
    let producer: Arc<dyn ProducerRuntime> = Arc::new(Producer);

    assert!(
        runtime
            .register_consumer_nullable(None, Some(consumer.clone()))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        runtime
            .register_consumer_nullable(Some("null-owner"), None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        runtime
            .register_consumer_nullable(None, None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        runtime
            .register_producer_nullable(None, Some(producer.clone()))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        runtime
            .register_producer_nullable(Some("null-owner"), None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        runtime
            .register_producer_nullable(None, None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        runtime
            .unregister_consumer_by_group_nullable(None, Instant::now())
            .await,
        Err(RuntimeError::InvalidRoute("null consumer group"))
    ));
    assert!(matches!(
        runtime
            .unregister_producer_by_group_nullable(None, Instant::now())
            .await,
        Err(RuntimeError::InvalidRoute("null producer group"))
    ));
    assert_eq!(runtime.state(), RuntimeState::Created);
}

#[tokio::test]
async fn distinct_registered_keys_preserve_equal_and_simple_consumer_metadata() {
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::new("192.0.2.6"))
        .await
        .unwrap();
    let first = Arc::new(Consumer(Mutex::new(ConsumerData {
        group_name: Some("consumer".into()),
        subscription_data_set: Some(shared_subscription_set([SubscriptionData {
            topic: Some("other".into()),
            sub_string: Some("tag".into()),
            sub_version: 13,
            ..Default::default()
        }])),
        ..java_registered_consumer_fields()
    })));
    let a = runtime
        .register_consumer("registered-a", first.clone())
        .await
        .unwrap();
    let mut b = runtime
        .register_consumer("registered-b", first.clone())
        .await
        .unwrap();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/heartbeat.json")).unwrap();
    let check = |name: &str, without_sub| {
        let case = fixture["multiplicityCases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["case"] == name)
            .unwrap();
        let actual = runtime.prepare_heartbeat_data(without_sub).unwrap();
        let expected: HeartbeatData = serde_json::from_value(case["body"].clone()).unwrap();
        assert_eq!(
            actual
                .consumer_data_set
                .as_ref()
                .unwrap()
                .with(|rows| rows.len()),
            case["body"]["consumerDataSet"].as_array().unwrap().len(),
            "{name}"
        );
        let mut actual_rows: Vec<_> = actual.consumer_data_set.as_ref().unwrap().with(|rows| {
            rows.iter()
                .map(|row| serde_json::to_string(row).unwrap())
                .collect()
        });
        let mut expected_rows: Vec<_> = expected.consumer_data_set.as_ref().unwrap().with(|rows| {
            rows.iter()
                .map(|row| serde_json::to_string(row).unwrap())
                .collect()
        });
        actual_rows.sort();
        expected_rows.sort();
        assert_eq!(actual_rows, expected_rows, "{name}");
    };
    check("equal-full", false);
    check("equal-simple", true);
    runtime
        .unregister_consumer(&b, Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    let second = Arc::new(Consumer(Mutex::new(ConsumerData {
        subscription_data_set: Some(shared_subscription_set([SubscriptionData {
            topic: Some("second".into()),
            sub_string: Some("*".into()),
            sub_version: 14,
            ..Default::default()
        }])),
        ..first.0.lock().unwrap().clone()
    })));
    b = runtime
        .register_consumer("registered-b", second.clone())
        .await
        .unwrap();
    check("different-full", false);
    check("different-simple", true);
    runtime
        .unregister_consumer(&a, Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    check("one-after-unregister", false);
    runtime
        .unregister_consumer(&b, Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(
        runtime
            .prepare_heartbeat_data(false)
            .unwrap()
            .consumer_data_set
            .as_ref()
            .unwrap()
            .with(|rows| rows.is_empty())
    );
}

#[tokio::test]
async fn duplicate_groups_and_stale_identity_tokens_cannot_remove_replacements() {
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::new("192.0.2.6"))
        .await
        .unwrap();
    let producer = Arc::new(Producer);
    let old = runtime
        .register_producer("group", producer.clone())
        .await
        .unwrap();
    assert!(matches!(
        runtime.register_producer("group", producer.clone()).await,
        Err(RuntimeError::DuplicateGroup { .. })
    ));
    let deadline = Instant::now() + Duration::from_secs(1);
    assert!(runtime.unregister_producer(&old, deadline).await.unwrap());
    let replacement = runtime
        .register_producer("group", producer.clone())
        .await
        .unwrap();
    assert!(!runtime.unregister_producer(&old, deadline).await.unwrap());
    assert_eq!(
        runtime
            .prepare_heartbeat_data(false)
            .unwrap()
            .producer_data_set
            .as_ref()
            .unwrap()
            .with(|rows| rows.len()),
        1
    );
    assert!(
        runtime
            .unregister_producer(&replacement, deadline)
            .await
            .unwrap()
    );
    assert!(
        runtime
            .prepare_heartbeat_data(false)
            .unwrap()
            .producer_data_set
            .as_ref()
            .unwrap()
            .with(|rows| rows.is_empty())
    );
}

#[tokio::test]
async fn heartbeat_snapshots_follow_consumer_versions_and_without_subscription_flag() {
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::new("192.0.2.6"))
        .await
        .unwrap();
    let producer = Arc::new(Producer);
    compare_java("empty", &runtime.prepare_heartbeat_data(false).unwrap());
    let consumer = Arc::new(Consumer(Mutex::new(ConsumerData {
        group_name: Some("consumer".into()),
        subscription_data_set: Some(shared_subscription_set([SubscriptionData {
            topic: Some("topic".into()),
            sub_string: Some("*".into()),
            sub_version: 12,
            ..Default::default()
        }])),
        ..java_registered_consumer_fields()
    })));
    let producer_lease = runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    compare_java(
        "producer-only",
        &runtime.prepare_heartbeat_data(false).unwrap(),
    );
    let consumer_lease = runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    let first = runtime.prepare_heartbeat_data(false).unwrap();
    compare_java("both", &first);
    assert_eq!(first.client_id.as_deref(), Some("192.0.2.6@DEFAULT"));
    assert_eq!(
        first
            .producer_data_set
            .as_ref()
            .unwrap()
            .with(|rows| rows.len()),
        1
    );
    let first_consumer = first_shared(first.consumer_data_set.as_ref().unwrap());
    assert_eq!(
        first_consumer
            .subscription_data_set
            .as_ref()
            .unwrap()
            .with(|rows| rows
                .iter()
                .flatten()
                .next()
                .unwrap()
                .with(|row| row.sub_version)),
        12
    );
    consumer.0.lock().unwrap().subscription_data_set =
        Some(shared_subscription_set([SubscriptionData {
            topic: Some("other".into()),
            sub_string: Some("tag".into()),
            sub_version: 13,
            ..Default::default()
        }]));
    let second = runtime.prepare_heartbeat_data(false).unwrap();
    compare_java("subscription-change", &second);
    assert_ne!(
        serde_json::to_value(&first).unwrap(),
        serde_json::to_value(&second).unwrap()
    );
    let without = runtime.prepare_heartbeat_data(true).unwrap();
    compare_java("without-sub", &without);
    assert!(without.is_without_sub);
    assert!(
        first_shared(without.consumer_data_set.as_ref().unwrap())
            .subscription_data_set
            .unwrap()
            .with(|rows| rows.is_empty())
    );
    let deadline = Instant::now() + Duration::from_secs(1);
    assert!(
        runtime
            .unregister_producer(&producer_lease, deadline)
            .await
            .unwrap()
    );
    compare_java(
        "consumer-only",
        &runtime.prepare_heartbeat_data(false).unwrap(),
    );
    assert_eq!(
        runtime
            .prepare_heartbeat_data(false)
            .unwrap()
            .consumer_data_set
            .as_ref()
            .unwrap()
            .with(|rows| rows.len()),
        1
    );
    assert!(
        runtime
            .unregister_consumer(&consumer_lease, deadline)
            .await
            .unwrap()
    );
    compare_java(
        "after-unregister",
        &runtime.prepare_heartbeat_data(false).unwrap(),
    );
    assert!(
        runtime
            .prepare_heartbeat_data(false)
            .unwrap()
            .consumer_data_set
            .as_ref()
            .unwrap()
            .with(|rows| rows.is_empty())
    );
}

#[tokio::test]
async fn admin_leases_are_separate_from_heartbeat_groups_and_runtime_identity() {
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::new("first"))
        .await
        .unwrap();
    let other = manager
        .get_or_create(&ClientConfig::new("other"))
        .await
        .unwrap();
    let lease = runtime.register_admin("admin").await.unwrap();
    assert!(matches!(
        runtime.register_admin("admin").await,
        Err(RuntimeError::DuplicateGroup { .. })
    ));
    assert!(!other.unregister_admin(&lease).await.unwrap());
    assert!(
        runtime
            .prepare_heartbeat_data(false)
            .unwrap()
            .producer_data_set
            .as_ref()
            .unwrap()
            .with(|rows| rows.is_empty())
    );
    assert!(runtime.unregister_admin(&lease).await.unwrap());
    assert!(!runtime.unregister_admin(&lease).await.unwrap());
}

#[tokio::test]
async fn heartbeat_callbacks_can_reenter_registry_without_runtime_owner_cycles() {
    use rocketmq_classic_core::runtime::ClientRuntime;
    struct ReentrantConsumer(std::sync::Weak<ClientRuntime>);
    impl ConsumerRuntime for ReentrantConsumer {
        fn heartbeat_data(&self) -> ConsumerData {
            let runtime = self.0.upgrade().unwrap();
            assert!(runtime.select_producer("producer").unwrap().is_some());
            assert!(runtime.select_consumer("consumer").unwrap().is_some());
            ConsumerData {
                group_name: Some("consumer".into()),
                ..Default::default()
            }
        }
    }
    struct WeakRuntimeProducer(std::sync::Weak<ClientRuntime>);
    impl ProducerRuntime for WeakRuntimeProducer {
        fn publish_topics(&self) -> std::collections::BTreeSet<String> {
            assert!(!self.0.upgrade().unwrap().client_id().is_empty());
            Default::default()
        }
    }
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::new("first"))
        .await
        .unwrap();
    let weak = Arc::downgrade(&runtime);
    let producer = Arc::new(WeakRuntimeProducer(weak.clone()));
    let consumer = Arc::new(ReentrantConsumer(weak.clone()));
    let _producer_lease = runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    let _consumer_lease = runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    let (done, result) = std::sync::mpsc::channel();
    let runner = runtime.clone();
    let thread = std::thread::spawn(move || {
        done.send(runner.prepare_heartbeat_data(false).unwrap())
            .unwrap()
    });
    assert_eq!(
        result
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap()
            .consumer_data_set
            .as_ref()
            .unwrap()
            .with(|rows| rows.len()),
        1
    );
    thread.join().unwrap();
    drop(manager);
    drop(runtime);
    assert!(weak.upgrade().is_none()); // Registry owner back-references must be weak.
    drop(producer);
    drop(consumer);
}
