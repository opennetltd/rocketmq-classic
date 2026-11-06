use rocketmq_classic::{
    ClientConfig, ConsumerServiceState, ConsumerStatsManager, DEFAULT_CONSUMER_GROUP,
    DEFAULT_PRODUCER_GROUP, EndTransactionContext, EndTransactionHook, FaultItem,
    FaultItemCompareError, Message, MessageBatch, MessageExtSubtype, MqHelper, NotifyResult,
    NullablePublishInfoQueueFilters, OrderlyListenerFuture, PermName, PermissionParseError,
    PopProcessQueue, PopRequest, PopResult, PopStatus, ProduceAccumulator, Producer,
    ProducerConfig, ProducerPublishQueueList, ProducerQueueIndex, ProducerServiceState,
    ProducerTopicList, PublishInfoQueueFilter, PullRequest, QueryResult, SharedMessageBody,
    SharedMessageProperties, TopicPublishInfo, TopicPublishInfoEntryHandle,
    TopicPublishInfoEntrySetView, TopicPublishInfoKeySetView, TopicPublishInfoTable,
    TopicPublishInfoTableError, TopicPublishInfoValuesView, TopicRouteData, TopicRouteDataHandle,
};
use rocketmq_classic_core::runtime::ClientRuntimeManager;

fn assert_public_type<T: ?Sized>() {}

#[tokio::test]
async fn phase_owned_public_types_are_available_from_the_facade_crate() {
    assert_eq!(DEFAULT_CONSUMER_GROUP, "DEFAULT_CONSUMER");
    assert_eq!(DEFAULT_PRODUCER_GROUP, "DEFAULT_PRODUCER");
    assert!(PermName::is_valid_string("٧").unwrap());
    assert_public_type::<PermName>();
    assert_public_type::<PermissionParseError>();
    assert_public_type::<PopProcessQueue>();
    assert_public_type::<PopRequest>();
    assert_public_type::<PullRequest>();
    assert_eq!(
        PopRequest::default().message_request_mode(),
        rocketmq_classic_protocol::body::MessageRequestMode::Pop
    );
    assert_eq!(
        PullRequest::default().message_request_mode(),
        rocketmq_classic_protocol::body::MessageRequestMode::Pull
    );

    let producer = Producer::new(ProducerConfig::new("facade-api-probe").unwrap());
    let _: Option<ProducerServiceState> = producer.service_state();
    let _: TopicPublishInfoTable = producer.get_topic_publish_info_table();
    let batch =
        MessageBatch::generate_from_list(vec![Message::new("orders", b"body".to_vec())]).unwrap();
    assert_eq!(batch.topic(), "orders");
    assert!(!batch.encode().unwrap().is_empty());
    assert_public_type::<MessageBatch>();

    let topic_list = ProducerTopicList::from(vec!["startup-topic".to_owned()]);
    let topic_list_alias = topic_list.clone();
    producer.set_topics(Some(topic_list));
    topic_list_alias.push("later-topic");
    assert_eq!(
        producer.topics().unwrap().snapshot(),
        vec!["startup-topic".to_owned(), "later-topic".to_owned()]
    );
    assert_public_type::<ProducerTopicList>();

    let shared_properties =
        SharedMessageProperties::from_properties(std::collections::BTreeMap::new());
    let mut message = Message::new("shared-properties", b"body".to_vec());
    message.set_properties_shared(Some(shared_properties.clone()));
    shared_properties.insert(Some("key".to_owned()), Some("value".to_owned()));
    assert_eq!(message.get_property("key").as_deref(), Some("value"));
    assert_public_type::<SharedMessageProperties>();
    assert_public_type::<dyn MessageExtSubtype>();

    let shared_body = SharedMessageBody::new(b"body".to_vec());
    let shared_body_message = Message::new_shared("shared-body-constructor", shared_body.clone());
    assert_eq!(shared_body_message.body().as_ref(), b"body");
    message.set_body_shared(Some(shared_body.clone()));
    shared_body.with_bytes_mut(|bytes| bytes[0] = b'B');
    assert_eq!(message.body().as_ref(), b"Body");
    assert_public_type::<SharedMessageBody>();

    assert_public_type::<ConsumerServiceState>();
    assert_public_type::<ConsumerStatsManager>();
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::default())
        .await
        .unwrap();
    let scheduler = runtime.scheduled_executor_service();
    let stats = ConsumerStatsManager::with_scheduler(&scheduler).unwrap();
    stats.inc_pull_rt("workers", "orders", 5);
    let _status = stats.consume_status("workers", "orders");
    runtime.shutdown().await.unwrap();
    assert_public_type::<OrderlyListenerFuture>();
    assert_public_type::<PopResult>();
    assert_public_type::<PopStatus>();
    assert_public_type::<MqHelper>();
    let mut notification = NotifyResult::default();
    assert!(!notification.is_has_msg());
    assert!(!notification.is_polling_full());
    notification.set_has_msg(true);
    notification.set_polling_full(true);
    assert_eq!(
        notification.to_string(),
        "NotifyResult{hasMsg=true, pollingFull=true}"
    );
    assert_public_type::<NotifyResult>();
    assert_public_type::<EndTransactionContext<'static>>();
    assert_public_type::<dyn EndTransactionHook>();
    let fault_item = FaultItem::new(Some("fault-item".to_owned()));
    fault_item.set_current_latency(12);
    assert_eq!(fault_item.get_current_latency(), 12);
    assert_public_type::<FaultItem>();
    assert_public_type::<FaultItemCompareError>();
    drop(ProduceAccumulator::new(Some("facade-producer".to_owned())));
    drop(ProduceAccumulator::new(None));
    assert_public_type::<ProduceAccumulator>();
    assert_public_type::<NullablePublishInfoQueueFilters<'static>>();
    assert_public_type::<ProducerPublishQueueList>();
    assert_public_type::<ProducerQueueIndex>();
    assert_public_type::<PublishInfoQueueFilter<'static>>();
    assert_public_type::<QueryResult>();
    assert_public_type::<TopicPublishInfo>();
    assert_public_type::<TopicPublishInfoEntryHandle>();
    assert_public_type::<TopicPublishInfoEntrySetView>();
    assert_public_type::<TopicPublishInfoKeySetView>();
    assert_public_type::<TopicPublishInfoTableError>();
    assert_public_type::<TopicPublishInfoValuesView>();
    assert_public_type::<TopicRouteData>();
    assert_public_type::<TopicRouteDataHandle>();
}
