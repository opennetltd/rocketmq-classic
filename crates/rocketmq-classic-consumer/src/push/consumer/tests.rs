use super::*;
use crate::offset::OffsetStoreFuture;
use crate::push::listener::MessageQueueListener;
use crate::push::pull_service::PullFlowControlReason;
use bytes::BytesMut;
use rocketmq_classic_common::message::{
    Message, MessageExt, MessageExtSubtype, MessageHostAddress,
};
use rocketmq_classic_core::api::consumer::PullResultExt;
use rocketmq_classic_protocol::body::GetConsumerStatusBody;
use rocketmq_classic_protocol::codes::{request, response};
use rocketmq_classic_protocol::header::HeaderMap;
use rocketmq_classic_remoting::client::RequestProcessor;
use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

#[test]
fn orderly_timeout_preserves_java_null_and_exception_priority() {
    assert_eq!(
        orderly_consume_return_type("RETURNNULL", None, true),
        "RETURNNULL"
    );
    assert_eq!(
        orderly_consume_return_type("EXCEPTION", None, true),
        "EXCEPTION"
    );
    assert_eq!(
        orderly_consume_return_type("SUCCESS", Some(ConsumeOrderlyStatus::Success), true),
        "TIME_OUT"
    );
}

#[test]
fn inner_producer_retry_response_codes_match_java_551_defaults() {
    for code in [
        response::TOPIC_NOT_EXIST,
        response::SERVICE_NOT_AVAILABLE,
        response::SYSTEM_ERROR,
        response::SYSTEM_BUSY,
        response::NO_PERMISSION,
        response::NO_BUYER_ID,
        response::NOT_IN_CURRENT_UNIT,
        response::GO_AWAY,
    ] {
        let error = rocketmq_classic_core::error::RpcError::Response {
            request_code: request::SEND_MESSAGE,
            peer: "broker-a".to_owned(),
            code,
            remark: None,
        };
        assert!(retryable_inner_producer_send_error(&error), "code {code}");
    }

    let error = rocketmq_classic_core::error::RpcError::Response {
        request_code: request::SEND_MESSAGE,
        peer: "broker-a".to_owned(),
        code: response::MESSAGE_ILLEGAL,
        remark: None,
    };
    assert!(!retryable_inner_producer_send_error(&error));
}

#[test]
fn retry_publish_queue_avoids_last_broker_until_it_must_fallback() {
    let queues = [
        MessageQueue::new("orders", "broker-a", 0),
        MessageQueue::new("orders", "broker-b", 0),
        MessageQueue::new("orders", "broker-c", 0),
    ];

    assert_eq!(
        select_retry_publish_queue(&queues, 0, None).and_then(MessageQueue::broker_name),
        Some("broker-a")
    );
    assert_eq!(
        select_retry_publish_queue(&queues, 0, Some("broker-a"))
            .and_then(MessageQueue::broker_name),
        Some("broker-b")
    );
    assert_eq!(
        select_retry_publish_queue(&queues, 2, Some("broker-a"))
            .and_then(MessageQueue::broker_name),
        Some("broker-c")
    );
    assert_eq!(
        select_retry_publish_queue(&queues[..1], 0, Some("broker-a"))
            .and_then(MessageQueue::broker_name),
        Some("broker-a")
    );
}

#[tokio::test]
async fn inner_producer_is_shared_by_runtime_and_replaced_on_runtime_change() {
    let config = PushConsumerConfig::new("inner-producer-owner-one").unwrap();
    let runtime_config = config.client_config().clone();
    let manager = ClientRuntimeManager::new();
    let runtime = manager.get_or_create(&runtime_config).await.unwrap();
    let first = PushConsumer::with_manager(config, manager.clone());
    let second = PushConsumer::with_manager(
        PushConsumerConfig::new("inner-producer-owner-two").unwrap(),
        manager,
    );

    let first_producer = first.inner_producer(&runtime).unwrap();
    let second_producer = second.inner_producer(&runtime).unwrap();
    assert!(Arc::ptr_eq(&first_producer, &second_producer));

    let other_manager = ClientRuntimeManager::new();
    let other_runtime = other_manager.get_or_create(&runtime_config).await.unwrap();
    let replacement = first.inner_producer(&other_runtime).unwrap();
    let shared_replacement = second.inner_producer(&other_runtime).unwrap();
    assert!(!Arc::ptr_eq(&first_producer, &replacement));
    assert!(Arc::ptr_eq(&replacement, &shared_replacement));
}

#[test]
fn trace_dispatcher_accessor_is_empty_before_start() {
    let config = PushConsumerConfig::new("trace-dispatcher-accessor").unwrap();
    let consumer = PushConsumer::new(config);
    assert!(consumer.trace_dispatcher().is_none());
}

#[tokio::test]
async fn push_owner_shutdown_stops_its_trace_dispatcher() {
    let config = PushConsumerConfig::new("push-trace-owner-shutdown").unwrap();
    let manager = ClientRuntimeManager::new();
    let dispatcher = Arc::new(
        rocketmq_classic_producer::TraceDispatcher::new(
            config.client_config(),
            "push-trace-owner-shutdown",
            rocketmq_classic_producer::TraceProducerKind::Consume,
            None,
            manager.clone(),
            None,
        )
        .unwrap(),
    );
    let consumer = PushConsumer::with_manager(config, manager);
    assert!(consumer.trace_dispatcher.set(dispatcher.clone()).is_ok());

    consumer.shutdown_trace_dispatcher().await;

    assert!(
        !dispatcher.append(rocketmq_classic_core::trace::TraceContext::new(
            rocketmq_classic_core::trace::TraceType::Recall,
        ))
    );
    dispatcher.shutdown().await.unwrap();
}

#[test]
fn raw_push_implementation_accessor_retains_identity() {
    let group = "phase18-push-implementation-accessor";
    let config = PushConsumerConfig::new(group).unwrap();
    let consumer = Arc::new(PushConsumer::new(config));

    let first = consumer.get_default_mq_push_consumer_impl();
    let second = consumer.get_default_mq_push_consumer_impl();
    let first_rebalance = consumer.get_rebalance_impl();
    let second_rebalance = consumer.get_rebalance_impl();

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    assert_eq!(fixture["implementationNonNull"], true);
    assert_eq!(fixture["implementationIdentityStable"], true);
    assert_eq!(fixture["rebalanceImplementationNonNull"], true);
    assert_eq!(fixture["rebalanceImplementationIdentityStable"], true);
    assert_eq!(fixture["configuredConsumerGroup"], group);
    assert_eq!(fixture["rawFacadeConsumerIdentityStable"], true);
    assert_eq!(fixture["rawFacadeConsumerGroup"], group);
    assert_eq!(fixture["clientFactoryInitiallyNull"], true);
    assert_eq!(fixture["clientFactorySetterPreservesIdentity"], true);
    assert_eq!(fixture["consumeMessageServiceInitiallyNull"], true);
    assert_eq!(
        fixture["consumeMessageServiceSetterPreservesIdentity"],
        true
    );
    assert_eq!(
        fixture["consumerStatsManagerGetterPreservesFactoryIdentity"],
        true
    );
    assert_eq!(
        fixture["rawImplementationPublicMethodSignatures"]
            .as_array()
            .unwrap()
            .len(),
        71,
        "pinned Java 5.5.1 raw implementation method inventory changed"
    );
    assert_eq!(fixture["effectiveMaxReconsumeTimesDefault"], 16);
    assert!(Arc::ptr_eq(&consumer, &first));
    assert!(Arc::ptr_eq(&first, &second));
    assert!(std::ptr::eq(first.config(), second.config()));
    assert!(std::ptr::eq(first_rebalance, consumer.as_ref()));
    assert!(std::ptr::eq(second_rebalance, first_rebalance));
    assert_eq!(first.config().consumer_group(), Some(group.to_owned()));
    assert_eq!(first.get_max_reconsume_times(), 16);
    assert!(first.get_consumer_stats_manager().is_none());

    let custom_config = PushConsumerConfig::new("phase18-push-max-reconsume-custom").unwrap();
    custom_config.set_max_reconsume_times(7);
    let custom_consumer = PushConsumer::new(custom_config);
    assert_eq!(fixture["effectiveMaxReconsumeTimesCustom"], 7);
    assert_eq!(custom_consumer.get_max_reconsume_times(), 7);
}

#[tokio::test]
async fn rebalance_client_factory_setter_replaces_nullable_runtime_reference() {
    let mut config = PushConsumerConfig::new("rebalance-factory-setter").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = PushConsumer::with_manager(config, manager.clone());
    assert!(consumer.get_client_runtime().await.is_none());

    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    consumer.set_client_runtime(Some(runtime.clone()));
    assert!(Arc::ptr_eq(
        &consumer.get_client_runtime().await.unwrap(),
        &runtime
    ));

    consumer.set_client_runtime(None);
    assert!(consumer.get_client_runtime().await.is_none());
    assert!(!runtime.client_id().is_empty());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn implementation_factory_setter_is_independent_of_rebalance_factory() {
    let mut config = PushConsumerConfig::new("implementation-factory-setter").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let consumer = Arc::new(PushConsumer::with_manager(
        config,
        ClientRuntimeManager::new(),
    ));
    assert!(consumer.get_implementation_client_runtime().await.is_none());

    let implementation_manager = ClientRuntimeManager::new();
    let rebalance_manager = ClientRuntimeManager::new();
    let implementation_runtime = implementation_manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    let rebalance_runtime = rebalance_manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();

    consumer.set_implementation_client_runtime(Some(implementation_runtime.clone()));
    consumer.set_client_runtime(Some(rebalance_runtime.clone()));
    let stats = consumer.get_consumer_stats_manager().unwrap();
    assert!(Arc::ptr_eq(
        &stats,
        &implementation_runtime.consumer_stats_manager()
    ));
    assert!(Arc::ptr_eq(
        &consumer.get_implementation_client_runtime().await.unwrap(),
        &implementation_runtime
    ));
    assert!(Arc::ptr_eq(
        &consumer.get_client_runtime().await.unwrap(),
        &rebalance_runtime
    ));

    consumer.set_implementation_client_runtime(None);
    assert!(consumer.get_implementation_client_runtime().await.is_none());
    assert!(consumer.get_consumer_stats_manager().is_none());
    assert!(Arc::ptr_eq(
        &consumer.get_client_runtime().await.unwrap(),
        &rebalance_runtime
    ));
    implementation_runtime.shutdown().await.unwrap();
    rebalance_runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn raw_push_listener_accessors_match_java_registration_flow() {
    let config = PushConsumerConfig::new("phase18-push-implementation-accessor").unwrap();
    let consumer = Arc::new(PushConsumer::new(config));
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();

    assert!(consumer.get_message_listener().is_none());
    assert_eq!(fixture["rawMessageListenerInitiallyNull"], true);

    let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
        |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    );
    consumer
        .register_message_listener(PushMessageListener::Concurrently(listener.clone()))
        .await
        .unwrap();
    let listener_getter_preserves_identity = match consumer.get_message_listener() {
        Some(PushMessageListener::Concurrently(current)) => Arc::ptr_eq(&listener, &current),
        _ => false,
    };
    assert_eq!(fixture["rawMessageListenerGetterPreservesIdentity"], true);
    assert!(listener_getter_preserves_identity);
}

#[test]
fn push_live_runtime_settings_drive_owner_flow_helpers() {
    let consumer = PushConsumer::new(PushConsumerConfig::new("push-live-runtime").unwrap());
    consumer
        .config()
        .set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    consumer
        .config()
        .set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
    consumer.config().set_unit_mode(true);
    consumer.config().set_max_reconsume_times(9);
    consumer.config().set_pull_threshold_for_queue(10);
    consumer.config().set_pull_threshold_size_for_queue_mib(100);
    consumer.config().set_consume_concurrently_max_span(1);

    assert_eq!(
        consumer.message_model(),
        rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting
    );
    assert_eq!(consumer.consume_init_mode(), 0);
    assert!(consumer.is_unit_mode());
    assert_eq!(consumer.get_max_reconsume_times(), 9);

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = ProcessQueue::new_for_assignment(queue, 1);
    process_queue
        .insert_messages(1, (0..5).map(message).collect())
        .unwrap();
    let thresholds = consumer.active_pull_queue_thresholds();
    assert_eq!(thresholds.message_count, 10);
    assert_eq!(thresholds.size_mib, 100);
    assert_eq!(thresholds.max_span, 1);
    assert_eq!(
        flow_control_reason(&process_queue.snapshot(1).unwrap(), thresholds),
        Some(PullFlowControlReason::MaxSpan)
    );
}

#[test]
fn push_topic_thresholds_match_java_global_process_queue_count() {
    let config = PushConsumerConfig::new("phase18-push-queue-thresholds").unwrap();
    config.set_pull_threshold_for_queue(777);
    config.set_pull_threshold_size_for_queue_mib(77);
    config.set_pull_threshold_for_topic(20);
    config.set_pull_threshold_size_for_topic_mib(12);
    let consumer = PushConsumer::new(config);
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    let expected = |name: &str| {
        let values = fixture[name].as_array().unwrap();
        (
            values[0].as_i64().unwrap() as i32,
            values[1].as_i64().unwrap() as i32,
        )
    };
    let active = || {
        (
            consumer.config.pull_threshold_for_queue(),
            consumer.config.pull_threshold_size_for_queue_mib(),
        )
    };

    consumer.update_pull_thresholds_after_rebalance();
    assert_eq!(active(), expected("pushQueueThresholdZeroAssignment"));

    let queues = [
        MessageQueue::new("orders", "broker-a", 0),
        MessageQueue::new("orders", "broker-a", 1),
        MessageQueue::new("events", "broker-b", 0),
        MessageQueue::new("events", "broker-b", 1),
    ];
    write_lock(&consumer.process_queues).insert(
        queues[0].clone(),
        Arc::new(ProcessQueue::new_for_assignment(queues[0].clone(), 1)),
    );
    consumer.update_pull_thresholds_after_rebalance();
    assert_eq!(active(), expected("pushQueueThresholdSingleAssignment"));

    for (generation, queue) in queues.iter().enumerate().skip(1) {
        write_lock(&consumer.process_queues).insert(
            queue.clone(),
            Arc::new(ProcessQueue::new_for_assignment(
                queue.clone(),
                generation as u64 + 1,
            )),
        );
    }
    consumer.update_pull_thresholds_after_rebalance();
    assert_eq!(
        active(),
        expected("pushQueueThresholdFourAssignmentsAcrossTopics")
    );
    let backlog = ProcessQueue::new_for_assignment(queues[0].clone(), 100);
    backlog
        .insert_messages(100, (0..6).map(message).collect())
        .unwrap();
    assert_eq!(
        flow_control_reason(
            &backlog.snapshot(100).unwrap(),
            consumer.active_pull_queue_thresholds(),
        ),
        Some(PullFlowControlReason::MessageCount),
        "the scaled global threshold must drive the actual queue flow-control decision"
    );

    write_lock(&consumer.process_queues).remove(&queues[3]);
    consumer.update_pull_thresholds_after_rebalance();
    assert_eq!(
        active(),
        expected("pushQueueThresholdThreeAssignmentsAcrossTopics")
    );

    write_lock(&consumer.process_queues).clear();
    consumer.update_pull_thresholds_after_rebalance();
    assert_eq!(active(), expected("pushQueueThresholdZeroAfterAssignment"));
    assert_eq!(fixture["pushQueueThresholdHeartbeatCalls"], 5);
}

#[tokio::test]
async fn push_adjust_thread_pool_matches_java_default_service_noop() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    assert_eq!(fixture["adjustThreadPoolDecreaseCalls"], 1);
    assert_eq!(fixture["adjustThreadPoolCorePoolSizeBefore"], 7);
    assert_eq!(fixture["adjustThreadPoolCorePoolSizeAfter"], 7);

    let mut config = PushConsumerConfig::new("phase18-adjust-pool").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_adjust_thread_pool_nums_threshold(10);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    install_started_consumer(
        &consumer,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    consumer.consume_core_pool_size.store(7, Ordering::Release);
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 1));
    process_queue.set_msg_acc_cnt(10);
    write_lock(&consumer.process_queues).insert(queue, process_queue.clone());
    consumer.adjust_thread_pool();
    assert_eq!(consumer.consume_core_pool_size.load(Ordering::Acquire), 7);
    process_queue.set_msg_acc_cnt(0);
    consumer.adjust_thread_pool();
    assert_eq!(consumer.consume_core_pool_size.load(Ordering::Acquire), 7);
    consumer.shutdown().await.unwrap();
}

#[test]
fn push_rebalance_consume_init_mode_matches_java_for_every_policy() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    let cases = [
        (
            ConsumeFromWhere::ConsumeFromLastOffset,
            "CONSUME_FROM_LAST_OFFSET",
        ),
        (
            ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst,
            "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST",
        ),
        (
            ConsumeFromWhere::ConsumeFromMinOffset,
            "CONSUME_FROM_MIN_OFFSET",
        ),
        (
            ConsumeFromWhere::ConsumeFromMaxOffset,
            "CONSUME_FROM_MAX_OFFSET",
        ),
        (
            ConsumeFromWhere::ConsumeFromFirstOffset,
            "CONSUME_FROM_FIRST_OFFSET",
        ),
        (
            ConsumeFromWhere::ConsumeFromTimestamp,
            "CONSUME_FROM_TIMESTAMP",
        ),
    ];

    for (policy, java_name) in cases {
        let config = PushConsumerConfig::new("phase18-pop-init-mode").unwrap();
        config.set_consume_from_where(policy);
        let consumer = PushConsumer::new(config);
        assert_eq!(
            i64::from(consumer.consume_init_mode()),
            fixture["pushConsumeInitModes"][java_name].as_i64().unwrap(),
            "consume init mode for {java_name}"
        );
    }
}

#[tokio::test]
async fn query_consume_time_span_queries_fresh_route_in_order_and_uses_vip_addresses() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    assert_eq!(fixture["queryConsumeTimeSpanNullGroupOmitted"], true);
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nameserver_address = nameserver.local_addr().unwrap().to_string();
    let first_broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_address = first_broker.local_addr().unwrap();
    let first_vip_address = format!(
        "{}:{}",
        first_address.ip(),
        first_address.port().checked_add(2).unwrap()
    );
    let second_broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_address = second_broker.local_addr().unwrap();
    let second_vip_address = format!(
        "{}:{}",
        second_address.ip(),
        second_address.port().checked_add(2).unwrap()
    );
    let route_body = serde_json::to_vec(&serde_json::json!({
        "queueDatas": [],
        "brokerDatas": [
            {"cluster": "cluster-a", "brokerName": "broker-z", "brokerAddrs": {"0": first_vip_address}},
            {"cluster": "cluster-a", "brokerName": "broker-a", "brokerAddrs": {"0": second_vip_address}}
        ],
        "filterServerTable": {}
    }))
    .unwrap();
    let nameserver_peer = tokio::spawn(async move {
        let (mut stream, _) = nameserver.accept().await.unwrap();
        let incoming = read_remoting_command(&mut stream).await;
        assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
        assert_eq!(
            incoming
                .ext_fields
                .as_ref()
                .and_then(|fields| fields.get("topic"))
                .map(String::as_str),
            Some("orders")
        );
        write_remoting_reply_with_body(
            &mut stream,
            &incoming,
            response::SUCCESS,
            None,
            Some(route_body),
        )
        .await;
    });

    let first_peer = tokio::spawn(async move {
        let (mut stream, _) = first_broker.accept().await.unwrap();
        let incoming = read_remoting_command(&mut stream).await;
        assert_eq!(incoming.code, request::QUERY_CONSUME_TIME_SPAN);
        let fields = incoming.ext_fields.as_ref().unwrap();
        assert_eq!(fields.get("topic").map(String::as_str), Some("orders"));
        assert!(!fields.contains_key("group"));
        write_remoting_reply_with_body(
            &mut stream,
            &incoming,
            response::SUCCESS,
            None,
            Some(
                br#"{"consumeTimeSpanSet":[{"messageQueue":{"topic":"orders","brokerName":"broker-z","queueId":3},"minTimeStamp":1,"maxTimeStamp":9,"consumeTimeStamp":5,"delayTime":4}]}"#.to_vec(),
            ),
        )
        .await;
    });
    let second_peer = tokio::spawn(async move {
        let (mut stream, _) = second_broker.accept().await.unwrap();
        let incoming = read_remoting_command(&mut stream).await;
        assert_eq!(incoming.code, request::QUERY_CONSUME_TIME_SPAN);
        let fields = incoming.ext_fields.as_ref().unwrap();
        assert_eq!(fields.get("topic").map(String::as_str), Some("orders"));
        assert!(!fields.contains_key("group"));
        write_remoting_reply_with_body(
            &mut stream,
            &incoming,
            response::SUCCESS,
            None,
            Some(
                br#"{"consumeTimeSpanSet":[{"messageQueue":{"topic":"orders","brokerName":"broker-a","queueId":1},"minTimeStamp":2,"maxTimeStamp":10,"consumeTimeStamp":7,"delayTime":3}]}"#.to_vec(),
            ),
        )
        .await;
    });

    let mut config = PushConsumerConfig::new("consume-time-span-group").unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(nameserver_address));
    config.client_config_mut().set_vip_channel_enabled(true);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        offset_store,
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    runtime.remoting().start().unwrap();
    consumer.set_consumer_group(None);

    let spans = consumer.query_consume_time_span("orders").await.unwrap();
    nameserver_peer.await.unwrap();
    first_peer.await.unwrap();
    second_peer.await.unwrap();
    assert_eq!(spans.len(), 2);
    assert_eq!(
        spans
            .iter()
            .map(|span| {
                span.as_ref()
                    .unwrap()
                    .get_message_queue()
                    .unwrap()
                    .with(|queue| queue.broker_name().unwrap().to_owned())
            })
            .collect::<Vec<_>>(),
        ["broker-z".to_owned(), "broker-a".to_owned()]
    );
    assert_eq!(spans[0].as_ref().unwrap().get_delay_time(), 4);
    assert_eq!(spans[1].as_ref().unwrap().get_delay_time(), 3);

    consumer.set_consumer_group(Some("consume-time-span-group".into()));
    consumer.shutdown().await.unwrap();
    let _ = runtime.shutdown().await;
}

#[test]
fn push_raw_consume_hook_entrypoints_match_java() {
    struct Hook(Arc<std::sync::Mutex<Vec<String>>>);

    impl ConsumeMessageHook for Hook {
        fn hook_name(&self) -> &str {
            "push-raw-hook-fixture"
        }

        fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
            self.0.lock().unwrap().push(format!(
                "before:{}",
                context.status.as_deref().unwrap_or("null")
            ));
            context.status = Some("hook-before".to_owned());
        }

        fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
            self.0.lock().unwrap().push(format!(
                "after:{}:{}",
                context.status.as_deref().unwrap_or("null"),
                context.success
            ));
        }
    }

    let config = PushConsumerConfig::new("phase18-push-implementation-accessor").unwrap();
    let consumer = PushConsumer::new(config);
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    assert!(!consumer.has_hook());
    assert_eq!(fixture["hasConsumeHookBeforeRegistration"], false);

    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    consumer.register_consume_message_hook(Arc::new(Hook(events.clone())));
    assert!(consumer.has_hook());
    let mut context =
        ConsumeMessageContext::new(None, None, MessageQueue::new("orders", "broker-a", 0), None);
    context.status = Some("seed".to_owned());
    consumer.execute_hook_before(&mut context);
    context.success = true;
    consumer.execute_hook_after(&mut context);

    let events = events.lock().unwrap().clone();
    assert_eq!(fixture["hasConsumeHookAfterRegistration"], true);
    assert_eq!(fixture["directHookConsumerGroupNull"], true);
    assert_eq!(
        fixture["directHookEvents"],
        serde_json::json!(["before:seed", "after:hook-before:true"])
    );
    assert_eq!(events, ["before:seed", "after:hook-before:true"]);
    assert_eq!(context.status.as_deref(), Some("hook-before"));
    assert_eq!(context.consumer_group, None);
}

#[test]
fn push_consume_message_context_preserves_a_live_null_group() {
    let mut config = PushConsumerConfig::new("push-null-consume-context").unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    let consumer = PushConsumer::new(config);
    consumer.set_consumer_group(None);

    let context = consumer.new_consume_message_context(
        Vec::new(),
        MessageQueue::new("tenant%orders", "broker-a", 0),
    );
    assert_eq!(context.consumer_group, None);
    assert_eq!(context.namespace.as_deref(), Some("tenant"));
}

#[test]
fn push_raw_filter_hook_and_topic_helpers_match_java() {
    struct FilterHook(Arc<std::sync::Mutex<Vec<String>>>);

    impl FilterMessageHook for FilterHook {
        fn hook_name(&self) -> &str {
            "push-raw-filter-hook-fixture"
        }

        fn filter_message(&self, context: &mut FilterMessageContext) {
            self.0.lock().unwrap().push(format!(
                "{}:{}",
                context.unit_mode,
                context.messages.len()
            ));
            context.messages.truncate(1);
        }
    }

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    let group = "phase18-push-topic-helpers";
    let mut config = PushConsumerConfig::new(group).unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    let consumer = PushConsumer::new(config);

    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    consumer.register_filter_message_hook(Arc::new(FilterHook(events.clone())));
    assert!(consumer.pull_api.has_hook());
    assert_eq!(
        fixture["pushPopFilterHookEvents"],
        serde_json::json!(["false:2"])
    );
    let first = crate::test_support::message_ext_from(Message::new("orders", b"one".to_vec()));
    let second = crate::test_support::message_ext_from(Message::new("orders", b"two".to_vec()));
    let (accepted, discarded) =
        consumer.filter_pop_messages(&SubscriptionData::default(), vec![first, second]);
    assert_eq!(*events.lock().unwrap(), ["false:2"]);
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].body().as_ref(), b"one");
    assert_eq!(discarded.len(), 1);
    assert_eq!(discarded[0].body().as_ref(), b"two");

    let queues = BTreeSet::from([
        MessageQueue::new("tenant%orders", "broker-a", 1),
        MessageQueue::new("orders", "broker-b", 2),
        MessageQueue::new("other%events", "broker-c", 3),
    ]);
    let parsed = consumer
        .parse_subscribe_message_queues(&queues)
        .unwrap()
        .iter()
        .map(|queue| {
            format!(
                "{}|{}|{}",
                queue.topic().unwrap_or_default(),
                queue.broker_name().unwrap_or_default(),
                queue.queue_id()
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(fixture["parsedSubscribeQueues"], serde_json::json!(parsed));

    let mut pop_retry_messages = [
        crate::test_support::message_ext_from(Message::new(
            "%RETRY%phase18-push-topic-helpers_orders",
            b"".to_vec(),
        )),
        crate::test_support::message_ext_from(Message::new("%RETRY%other_orders", b"".to_vec())),
        crate::test_support::message_ext_from(Message::new("orders", b"".to_vec())),
    ];
    consumer.try_reset_pop_retry_topic(&mut pop_retry_messages, group);
    let pop_retry_topics = pop_retry_messages
        .iter()
        .map(|message| message.topic())
        .collect::<Vec<_>>();
    assert_eq!(
        fixture["resetPopRetryTopics"],
        serde_json::json!(pop_retry_topics)
    );

    let mut reset_messages = [
        {
            let mut message = crate::test_support::message_ext_from(Message::new(
                "%RETRY%phase18-push-topic-helpers",
                b"".to_vec(),
            ));
            message
                .set_system_property(PROPERTY_RETRY_TOPIC, "tenant%orders")
                .unwrap();
            message
        },
        crate::test_support::message_ext_from(Message::new("tenant%events", b"".to_vec())),
        crate::test_support::message_ext_from(Message::new("other%events", b"".to_vec())),
    ];
    consumer.reset_retry_and_namespace(&mut reset_messages, group);
    let reset_topics = reset_messages
        .iter()
        .map(|message| message.topic())
        .collect::<Vec<_>>();
    assert_eq!(
        fixture["resetRetryAndNamespaceTopics"],
        serde_json::json!(reset_topics)
    );
}

#[tokio::test]
async fn push_raw_rebalance_route_entrypoints_match_java() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    let consumer = Arc::new(PushConsumer::new(
        PushConsumerConfig::new("phase18-push-implementation-accessor").unwrap(),
    ));
    consumer.subscribe("orders", "*").await.unwrap();

    assert_eq!(
        consumer.is_subscribe_topic_need_update("orders"),
        fixture["subscribeInfoNeededBeforeRoute"]
    );
    assert!(!consumer.is_subscribe_topic_need_update("unregistered"));
    assert_eq!(fixture["unregisteredTopicNeedsRoute"], false);
    consumer
        .update_topic_subscribe_info(
            "orders",
            BTreeSet::from([MessageQueue::new("orders", "broker-a", 1)]),
        )
        .await;
    assert_eq!(
        consumer.is_subscribe_topic_need_update("orders"),
        fixture["subscribeInfoNeededAfterRoute"]
    );
    consumer.suspend().await;
    assert_eq!(
        consumer.try_rebalance().await,
        fixture["tryRebalanceWhilePaused"]
    );
    consumer.do_rebalance().await;
    consumer.resume().await;
}

#[tokio::test]
async fn push_raw_offset_store_accessors_match_java_null_and_identity() {
    let config = PushConsumerConfig::new("phase18-push-implementation-accessor").unwrap();
    let consumer = PushConsumer::new(config);
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    assert_eq!(
        consumer.get_offset_store().is_none(),
        fixture["offsetStoreInitiallyNull"]
    );

    let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    consumer
        .set_offset_store(Some(store.clone()))
        .await
        .unwrap();
    let retained = consumer.get_offset_store().unwrap();
    assert!(Arc::ptr_eq(&store, &retained));
    assert_eq!(fixture["offsetStoreSetterPreservesIdentity"], true);

    consumer.set_offset_store(None).await.unwrap();
    assert!(consumer.get_offset_store().is_none());
    assert_eq!(fixture["offsetStoreSetterAcceptsNull"], true);
}

#[tokio::test]
async fn push_raw_offset_entrypoints_match_java_and_persist_only_active_pull_queue() {
    let config = PushConsumerConfig::new("phase18-push-implementation-accessor").unwrap();
    let consumer = Arc::new(PushConsumer::new(config));
    let store = Arc::new(RecordingOffsetStore::default());
    let store_dyn: Arc<dyn OffsetStore> = store.clone();
    consumer
        .set_offset_store(Some(store_dyn.clone()))
        .await
        .unwrap();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    let assigned = MessageQueue::new("orders", "broker-a", 2);
    let unassigned = MessageQueue::new("orders", "broker-b", 3);

    consumer.update_consume_offset(&assigned, 41).unwrap();
    consumer.update_consume_offset(&unassigned, 99).unwrap();
    assert_eq!(
        store.updates.lock().unwrap().as_slice(),
        &[(assigned.clone(), 41, false), (unassigned, 99, false)]
    );
    assert_eq!(
        fixture["offsetUpdates"],
        serde_json::json!(["orders|broker-a|2|41|false", "orders|broker-b|3|99|false"])
    );

    assert!(matches!(
        consumer.persist_consumer_offset().await,
        Err(PushConsumerError::ServiceStateNotReady(Some(
            ConsumerServiceState::CreateJust
        )))
    ));
    assert!(store.persist_calls.lock().unwrap().is_empty());

    write_lock(&consumer.process_queues).insert(
        assigned.clone(),
        Arc::new(ProcessQueue::new_for_assignment(assigned.clone(), 1)),
    );
    consumer.set_service_state(Some(ConsumerServiceState::Running));
    consumer.persist_consumer_offset().await.unwrap();
    assert_eq!(
        store.persist_calls.lock().unwrap().as_slice(),
        &[vec![assigned.clone()]]
    );
    assert_eq!(
        fixture["persistedQueues"],
        serde_json::json!(["orders|broker-a|2"])
    );

    let registration = PushConsumerRegistration {
        group: consumer.group_name().unwrap_or_default(),
        message_model: consumer.message_model(),
        consume_from_where: consumer.consume_from_where(),
        unit_mode: consumer.is_unit_mode(),
        subscriptions: consumer.subscriptions.clone(),
        topic_queues: consumer.topic_queues.clone(),
        pull_scheduler: Arc::new(PullScheduler::new()),
        pull_rpc_workers: Arc::new(PullRpcWorkers::new()),
        offset_store: Arc::downgrade(&store_dyn),
        consumer: Arc::downgrade(&consumer),
        runtime: std::sync::Weak::new(),
    };
    consumer
        .config()
        .set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    consumer
        .config()
        .set_consume_from_where(ConsumeFromWhere::ConsumeFromTimestamp);
    consumer.config().set_unit_mode(true);
    let heartbeat = ConsumerRuntime::heartbeat_data(&registration);
    assert_eq!(
        heartbeat.message_model,
        Some(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting)
    );
    assert_eq!(
        heartbeat.consume_from_where,
        Some(ConsumeFromWhere::ConsumeFromTimestamp)
    );
    assert!(heartbeat.unit_mode);
    ConsumerRuntime::persist_consumer_offset(&registration)
        .expect("running consumer registers periodic offset persistence")
        .await;
    assert_eq!(
        store.persist_calls.lock().unwrap().as_slice(),
        &[vec![assigned.clone()], vec![assigned]]
    );
}

#[test]
fn push_facade_group_getter_and_heartbeat_follow_live_java_group() {
    let oracle: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/runtime/heartbeat.json"
    ))
    .unwrap();
    let oracle = &oracle["pushGroupNameMutation"];
    assert_eq!(oracle["defaultConstructorGroup"], "DEFAULT_CONSUMER");

    let config = PushConsumerConfig::new("push-group-before").unwrap();
    let consumer = Arc::new(PushConsumer::new(config));
    let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let registration = PushConsumerRegistration {
        group: "push-group-before".to_owned(),
        message_model: consumer.message_model(),
        consume_from_where: consumer.consume_from_where(),
        unit_mode: consumer.is_unit_mode(),
        subscriptions: consumer.subscriptions.clone(),
        topic_queues: consumer.topic_queues.clone(),
        pull_scheduler: Arc::new(PullScheduler::new()),
        pull_rpc_workers: Arc::new(PullRpcWorkers::new()),
        offset_store: Arc::downgrade(&store),
        consumer: Arc::downgrade(&consumer),
        runtime: std::sync::Weak::new(),
    };

    assert_eq!(
        consumer.get_consumer_group().as_deref(),
        Some("push-group-before")
    );
    assert_eq!(consumer.group_name().as_deref(), Some("push-group-before"));
    assert_eq!(oracle["facadeGroupBefore"], "push-group-before");
    assert_eq!(oracle["implementationGroupBefore"], "push-group-before");
    assert_eq!(
        ConsumerRuntime::heartbeat_data(&registration)
            .group_name
            .as_deref(),
        Some(oracle["heartbeatGroupBefore"].as_str().unwrap())
    );

    consumer.set_consumer_group(Some("push-group-after".into()));
    assert_eq!(
        consumer.get_consumer_group().as_deref(),
        Some("push-group-after")
    );
    assert_eq!(consumer.group_name().as_deref(), Some("push-group-after"));
    assert_eq!(oracle["facadeGroupAfter"], "push-group-after");
    assert_eq!(oracle["implementationGroupAfter"], "push-group-after");
    assert_eq!(
        ConsumerRuntime::heartbeat_data(&registration)
            .group_name
            .as_deref(),
        Some(oracle["heartbeatGroupAfter"].as_str().unwrap())
    );

    consumer.set_consumer_group(None);
    assert_eq!(consumer.get_consumer_group(), None);
    assert_eq!(consumer.group_name(), None);
    assert_eq!(oracle["facadeGroupNull"], true);
    assert_eq!(oracle["implementationGroupNull"], true);
    assert_eq!(
        ConsumerRuntime::heartbeat_data(&registration).group_name,
        None
    );
    assert_eq!(oracle["heartbeatGroupNull"], true);
    assert_eq!(
        oracle["originalRegistrationRemainsAfterCurrentGroupUnregister"],
        true
    );
}

#[tokio::test]
async fn push_start_namespaces_and_retains_group_before_consumer_validation() {
    let mut config = PushConsumerConfig::new("push-start-group").unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    let consumer = Arc::new(PushConsumer::new(config));
    assert_eq!(consumer.group_name().as_deref(), Some("push-start-group"));

    assert!(matches!(
        consumer.start().await,
        Err(PushConsumerError::MissingMessageListener)
    ));
    assert_eq!(
        consumer.group_name().as_deref(),
        Some("tenant%push-start-group")
    );
    assert_eq!(
        consumer.config().consumer_group().as_deref(),
        Some("tenant%push-start-group")
    );

    let default = PushConsumerConfig::new_default().unwrap();
    assert_eq!(
        default.consumer_group().as_deref(),
        Some("DEFAULT_CONSUMER")
    );
    assert!(matches!(
        default.validate("DEFAULT_CONSUMER"),
        Err(PushConsumerError::DefaultConsumerGroup)
    ));
}

#[tokio::test]
async fn push_raw_consumer_inner_accessors_match_java() {
    let config = PushConsumerConfig::new("phase18-push-implementation-accessor").unwrap();
    let consumer = PushConsumer::new(config);
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();

    assert_eq!(
        consumer.group_name(),
        Some(fixture["groupName"].as_str().unwrap().to_owned())
    );
    assert_eq!(fixture["messageModel"], "CLUSTERING");
    assert_eq!(
        consumer.message_model(),
        rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
    );
    assert_eq!(fixture["consumeType"], "CONSUME_PASSIVELY");
    assert_eq!(consumer.consume_type(), ConsumeType::ConsumePassively);
    assert_eq!(fixture["consumeFromWhere"], "CONSUME_FROM_LAST_OFFSET");
    assert_eq!(
        consumer.consume_from_where(),
        ConsumeFromWhere::ConsumeFromLastOffset
    );
    assert_eq!(
        consumer.is_unit_mode(),
        fixture["unitMode"].as_bool().unwrap()
    );
    assert_eq!(
        consumer.get_message_queue_listener().is_none(),
        fixture["messageQueueListenerInitiallyNull"]
            .as_bool()
            .unwrap()
    );
    assert_eq!(
        consumer.is_pause(),
        fixture["pauseBeforeSet"].as_bool().unwrap()
    );
    consumer.set_pause(true);
    assert!(consumer.is_pause());
    assert_eq!(fixture["pauseAfterSetTrue"], true);
    consumer.set_pause(false);
    assert!(!consumer.is_pause());
    assert_eq!(fixture["pauseAfterSetFalse"], false);
    assert_eq!(
        consumer.is_consume_orderly(),
        fixture["consumeOrderlyBeforeSet"].as_bool().unwrap()
    );
    consumer.set_consume_orderly(true);
    assert!(consumer.is_consume_orderly());
    assert_eq!(fixture["consumeOrderlyAfterSetTrue"], true);
    consumer.set_consume_orderly(false);
    assert!(!consumer.is_consume_orderly());
    assert_eq!(fixture["consumeOrderlyAfterSetFalse"], false);
    assert_eq!(
        consumer.pull_time_delay_millis_when_exception(),
        fixture["pullDelayBeforeSet"].as_i64().unwrap()
    );
    consumer.set_pull_time_delay_millis_when_exception(73);
    assert_eq!(
        consumer.pull_time_delay_millis_when_exception(),
        fixture["pullDelayAfterSet"].as_i64().unwrap()
    );
    assert_eq!(
        consumer.pull_retry_error("custom delay").delay_millis,
        fixture["pullDelayAfterSet"].as_i64().unwrap()
    );
    assert_eq!(
        consumer
            .pull_retry_error_from_rpc(rocketmq_classic_core::error::RpcError::Unsupported(
                "custom delay",
            ))
            .delay_millis,
        fixture["pullDelayAfterSet"].as_i64().unwrap()
    );
    assert_eq!(
        consumer
            .pull_retry_error_from_rpc(rocketmq_classic_core::error::RpcError::Response {
                request_code: 11,
                peer: "broker-a".to_owned(),
                code: response::FLOW_CONTROL,
                remark: None,
            })
            .delay_millis,
        PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL
    );
    assert_eq!(
        consumer.subscriptions().len(),
        fixture["subscriptionCountBefore"].as_u64().unwrap() as usize
    );

    let table = consumer.get_subscription_inner();
    assert!(Arc::ptr_eq(&table, &consumer.get_subscription_inner()));
    consumer.subscribe("orders", "*").await.unwrap();
    assert_eq!(fixture["subscriptionInnerIdentityStable"], true);
    let mut topics = consumer
        .subscriptions()
        .into_iter()
        .filter_map(|subscription| subscription.topic)
        .collect::<Vec<_>>();
    topics.sort();
    assert_eq!(
        serde_json::json!(topics),
        fixture["subscriptionTopicsAfterSubscribe"]
    );
    assert_eq!(table.read().unwrap().len(), topics.len());
}

#[tokio::test]
async fn push_service_state_accessors_match_java_null_and_start_guard() {
    let group = "phase18-push-implementation-accessor";
    let config = PushConsumerConfig::new(group).unwrap();
    let consumer = Arc::new(PushConsumer::new(config));

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    assert_eq!(
        consumer.service_state(),
        Some(ConsumerServiceState::CreateJust)
    );
    assert_eq!(fixture["initialServiceState"], "CREATE_JUST");

    consumer.set_service_state(None);
    assert_eq!(consumer.service_state(), None);
    assert_eq!(fixture["serviceStateSetterAcceptsNull"], true);
    assert!(matches!(
        consumer.start().await,
        Err(PushConsumerError::ServiceStateNotReady(None))
    ));

    consumer.set_service_state(Some(ConsumerServiceState::StartFailed));
    assert_eq!(fixture["startFailedStateBlocksStart"], true);
    assert!(matches!(
        consumer.start().await,
        Err(PushConsumerError::ServiceStateNotReady(Some(
            ConsumerServiceState::StartFailed
        )))
    ));
    assert_eq!(
        consumer.service_state(),
        Some(ConsumerServiceState::StartFailed)
    );

    consumer.set_service_state(Some(ConsumerServiceState::Running));
    assert!(consumer.is_running().await);
    consumer.set_service_state(Some(ConsumerServiceState::CreateJust));
    assert!(!consumer.is_running().await);
    assert!(matches!(
        consumer.start().await,
        Err(PushConsumerError::MissingMessageListener)
    ));
    assert_eq!(
        consumer.service_state(),
        Some(ConsumerServiceState::StartFailed)
    );
}

#[derive(Default)]
struct RecordingOffsetStore {
    update_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    persist_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    remove_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    reset_events: Mutex<BTreeMap<i32, Vec<String>>>,
    fail_persist_queues: Mutex<HashSet<MessageQueue>>,
    fail_read_queues: Mutex<HashSet<MessageQueue>>,
    values: Mutex<HashMap<MessageQueue, i64>>,
    persisted: Mutex<HashMap<MessageQueue, i64>>,
    frozen: Mutex<HashSet<MessageQueue>>,
    updates: Mutex<Vec<(MessageQueue, i64, bool)>>,
    reads: Mutex<Vec<(MessageQueue, ReadOffsetType)>>,
    persist_calls: Mutex<Vec<Vec<MessageQueue>>>,
    fail_persist: Mutex<bool>,
    persist_started: Mutex<Option<Arc<tokio::sync::Barrier>>>,
    persist_release: Mutex<Option<Arc<tokio::sync::Barrier>>>,
}

impl RecordingOffsetStore {
    fn seed_persisted(&self, queue: MessageQueue, offset: i64) {
        self.persisted.lock().unwrap().insert(queue, offset);
    }

    fn set_persist_failure(&self, fail: bool) {
        *self.fail_persist.lock().unwrap() = fail;
    }

    fn set_persist_barriers(
        &self,
        started: Arc<tokio::sync::Barrier>,
        release: Arc<tokio::sync::Barrier>,
    ) {
        *self.persist_started.lock().unwrap() = Some(started);
        *self.persist_release.lock().unwrap() = Some(release);
    }
}

impl OffsetStore for RecordingOffsetStore {
    fn load(&self) -> OffsetStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn update_offset(&self, queue: &MessageQueue, offset: i64, increase_only: bool) {
        self.reset_events
            .lock()
            .unwrap()
            .entry(queue.queue_id())
            .or_default()
            .push(format!("update:{offset}"));
        self.updates
            .lock()
            .unwrap()
            .push((queue.clone(), offset, increase_only));
        if self.frozen.lock().unwrap().contains(queue) {
            return;
        }
        let mut values = self.values.lock().unwrap();
        if !increase_only || values.get(queue).is_none_or(|current| offset > *current) {
            values.insert(queue.clone(), offset);
        }
        drop(values);
        let hook = self.update_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    fn update_and_freeze_offset(&self, queue: &MessageQueue, offset: i64) {
        self.updates
            .lock()
            .unwrap()
            .push((queue.clone(), offset, false));
        self.frozen.lock().unwrap().insert(queue.clone());
        self.values.lock().unwrap().insert(queue.clone(), offset);
    }

    fn read_offset<'a>(
        &'a self,
        queue: &'a MessageQueue,
        mode: ReadOffsetType,
    ) -> OffsetStoreFuture<'a, OffsetRead> {
        Box::pin(async move {
            self.reads.lock().unwrap().push((queue.clone(), mode));
            if self.fail_read_queues.lock().unwrap().contains(queue) {
                return Err(OffsetStoreError::Runtime(
                    "injected offset read failure".into(),
                ));
            }
            let memory = self.values.lock().unwrap().get(queue).copied();
            let stored = self.persisted.lock().unwrap().get(queue).copied();
            Ok(match mode {
                ReadOffsetType::ReadFromMemory => memory,
                ReadOffsetType::ReadFromStore => stored,
                ReadOffsetType::MemoryFirstThenStore => memory.or(stored),
            }
            .map_or(OffsetRead::NotFound, OffsetRead::Found))
        })
    }

    fn persist<'a>(&'a self, queue: &'a MessageQueue) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async move {
            self.reset_events
                .lock()
                .unwrap()
                .entry(queue.queue_id())
                .or_default()
                .push("persist".into());
            let started = self.persist_started.lock().unwrap().clone();
            if let Some(started) = started {
                started.wait().await;
            }
            let release = self.persist_release.lock().unwrap().clone();
            if let Some(release) = release {
                release.wait().await;
            }
            if *self.fail_persist.lock().unwrap()
                || self.fail_persist_queues.lock().unwrap().contains(queue)
            {
                return Err(OffsetStoreError::Runtime("injected persist failure".into()));
            }
            if let Some(offset) = self.values.lock().unwrap().get(queue).copied() {
                self.persisted.lock().unwrap().insert(queue.clone(), offset);
            }
            let hook = self.persist_hook.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook();
            }
            Ok(())
        })
    }

    fn persist_all<'a>(&'a self, queues: &'a [MessageQueue]) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async move {
            self.persist_calls.lock().unwrap().push(queues.to_vec());
            if *self.fail_persist.lock().unwrap() {
                return Err(OffsetStoreError::Runtime("injected persist failure".into()));
            }
            let values = self.values.lock().unwrap();
            let mut persisted = self.persisted.lock().unwrap();
            for queue in queues {
                if let Some(offset) = values.get(queue) {
                    persisted.insert(queue.clone(), *offset);
                }
            }
            Ok(())
        })
    }

    fn remove_offset(&self, queue: &MessageQueue) {
        self.reset_events
            .lock()
            .unwrap()
            .entry(queue.queue_id())
            .or_default()
            .push("remove".into());
        self.values.lock().unwrap().remove(queue);
        self.frozen.lock().unwrap().remove(queue);
        let hook = self.remove_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    fn clone_offset_table(&self, topic: Option<&str>) -> HashMap<MessageQueue, i64> {
        self.values
            .lock()
            .unwrap()
            .iter()
            .filter(|(queue, _)| topic.is_none_or(|topic| queue.topic() == Some(topic)))
            .map(|(queue, offset)| (queue.clone(), *offset))
            .collect()
    }

    fn update_consume_offset_to_broker<'a>(
        &'a self,
        _queue: &'a MessageQueue,
        _offset: i64,
        _oneway: bool,
    ) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn pull_result(status: PullStatus, next_begin_offset: i64) -> crate::pull::ProcessedPullResult {
    crate::pull::ProcessedPullResult {
        result: PullResultExt {
            status,
            next_begin_offset,
            min_offset: 0,
            max_offset: next_begin_offset,
            suggest_which_broker_id: 0,
            offset_delta: None,
            raw_messages: None,
        },
        messages: (status == PullStatus::Found).then(Vec::new),
    }
}

fn message(offset: i64) -> MessageExt {
    let mut message =
        crate::test_support::message_ext_from(Message::new("orders", b"body".to_vec()));
    message.set_queue_offset(offset);
    message
}

struct FixedStoreHostSubtype {
    store_host: MessageHostAddress,
    setter_calls: AtomicUsize,
}

impl MessageExtSubtype for FixedStoreHostSubtype {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn store_host(&self) -> Option<Option<&MessageHostAddress>> {
        Some(Some(&self.store_host))
    }

    fn set_store_host(&self, _store_host: Option<MessageHostAddress>) -> bool {
        self.setter_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        true
    }
}

fn pop_receipt_message(receipt: Option<&str>) -> MessageExt {
    let mut message =
        crate::test_support::message_ext_from(Message::new("orders", b"body".to_vec()));
    if let Some(receipt) = receipt {
        message
            .set_system_property(PROPERTY_POP_CK, receipt)
            .unwrap();
    }
    message
}

async fn read_remoting_command<R>(stream: &mut R) -> RemotingCommand
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes).unwrap().unwrap()
}

async fn write_remoting_reply<W>(
    stream: &mut W,
    request: &RemotingCommand,
    code: i32,
    fields: Option<HeaderMap>,
) where
    W: tokio::io::AsyncWrite + Unpin,
{
    write_remoting_reply_with_body(stream, request, code, fields, None).await;
}

async fn write_remoting_reply_with_body<W>(
    stream: &mut W,
    request: &RemotingCommand,
    code: i32,
    fields: Option<HeaderMap>,
    body: Option<Vec<u8>>,
) where
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut reply = RemotingCommand {
        code,
        opaque: request.opaque,
        ext_fields: fields.map(Into::into),
        body,
        ..Default::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply).unwrap())
        .await
        .unwrap();
}

fn pop_test_route(address: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "queueDatas": [{
            "brokerName": "broker-a",
            "readQueueNums": 1,
            "writeQueueNums": 1,
            "perm": 6,
            "topicSysFlag": 0
        }],
        "brokerDatas": [{
            "cluster": "cluster",
            "brokerName": "broker-a",
            "brokerAddrs": {"0": address}
        }],
        "filterServerTable": {}
    }))
    .unwrap()
}

fn pop_test_message_record() -> Vec<u8> {
    let now = epoch_millis();
    let mut record = Vec::new();
    record.extend_from_slice(&0i32.to_be_bytes());
    record.extend_from_slice(&(-626_843_481i32).to_be_bytes());
    record.extend_from_slice(&0i32.to_be_bytes());
    record.extend_from_slice(&0i32.to_be_bytes());
    record.extend_from_slice(&0i32.to_be_bytes());
    record.extend_from_slice(&10i64.to_be_bytes());
    record.extend_from_slice(&10i64.to_be_bytes());
    record.extend_from_slice(&0i32.to_be_bytes());
    record.extend_from_slice(&now.to_be_bytes());
    record.extend_from_slice(&[127, 0, 0, 1, 0, 0, 0, 0]);
    record.extend_from_slice(&now.to_be_bytes());
    record.extend_from_slice(&[127, 0, 0, 1, 0, 0, 0, 0]);
    record.extend_from_slice(&0i32.to_be_bytes());
    record.extend_from_slice(&0i64.to_be_bytes());
    record.extend_from_slice(&1i32.to_be_bytes());
    record.push(b'x');
    record.push(6);
    record.extend_from_slice(b"orders");
    record.extend_from_slice(&0i16.to_be_bytes());
    let size = i32::try_from(record.len()).unwrap();
    record[..4].copy_from_slice(&size.to_be_bytes());
    record
}

fn pop_test_message_record_with_receipt(receipt: &str) -> Vec<u8> {
    let properties =
        encode_nullable_property_entries(Some(&[(Some(PROPERTY_POP_CK), Some(receipt))]))
            .into_bytes();
    let mut record = pop_test_message_record();
    record.truncate(record.len() - 2);
    record.extend_from_slice(&i16::try_from(properties.len()).unwrap().to_be_bytes());
    record.extend_from_slice(&properties);
    let size = i32::try_from(record.len()).unwrap();
    record[..4].copy_from_slice(&size.to_be_bytes());
    record
}

fn pop_test_message_record_with_tag_and_receipt(tag: &str, receipt: &str) -> Vec<u8> {
    let properties = encode_nullable_property_entries(Some(&[
        (Some(PROPERTY_POP_CK), Some(receipt)),
        (Some(PROPERTY_TAGS), Some(tag)),
    ]))
    .into_bytes();
    let mut record = pop_test_message_record();
    record.truncate(record.len() - 2);
    record.extend_from_slice(&i16::try_from(properties.len()).unwrap().to_be_bytes());
    record.extend_from_slice(&properties);
    let size = i32::try_from(record.len()).unwrap();
    record[..4].copy_from_slice(&size.to_be_bytes());
    record
}

async fn accept_remoting_request(
    listener: &TcpListener,
    current: &mut Option<TcpStream>,
) -> RemotingCommand {
    loop {
        if current.is_none() {
            *current = Some(listener.accept().await.unwrap().0);
        }
        let stream = current.as_mut().unwrap();
        match timeout(Duration::from_millis(250), read_remoting_command(stream)).await {
            Ok(command) => return command,
            Err(_) => *current = None,
        }
    }
}

#[test]
fn pop_batch_expiry_uses_receipt_invisibility_boundary_and_rejects_invalid_receipts() {
    let message = pop_receipt_message(Some("10 1000 50 2 0 broker-a 0 10"));
    assert!(!pop_batch_is_expired(std::slice::from_ref(&message), 1_049));
    assert!(pop_batch_is_expired(std::slice::from_ref(&message), 1_050));

    assert!(pop_batch_is_expired(&[], 1_049));
    assert!(pop_batch_is_expired(&[pop_receipt_message(None)], 1_049));
    for receipt in [
        "invalid receipt",
        "10 invalid 50 2 0 broker-a 0 10",
        "10 1000 50",
        "10 0 50 2 0 broker-a 0 10",
        "10 1000 0 2 0 broker-a 0 10",
    ] {
        assert!(
            pop_batch_is_expired(&[pop_receipt_message(Some(receipt))], 1_049),
            "receipt should be treated as expired: {receipt}"
        );
    }
}

#[tokio::test]
async fn expired_pop_batch_skips_listener_and_releases_found_message_count() {
    let mut config = PushConsumerConfig::new("expired-pop-batch").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config.client_config())
        .await
        .unwrap();
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 1));
    process_queue.inc_found_messages(1);
    let listener_called = Arc::new(AtomicBool::new(false));
    let listener = PushMessageListener::concurrently({
        let listener_called = listener_called.clone();
        move |_messages, context| {
            listener_called.store(true, Ordering::Release);
            std::future::ready((Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context))
        }
    });

    consumer
        .clone()
        .consume_pop_batch(
            PopConsumeBatchContext {
                process_queue: process_queue.clone(),
                runtime: runtime.clone(),
                queue,
                route_topic: "orders".to_owned(),
                consumer_group: "expired-pop-batch".to_owned(),
                stats_consumer_group: "expired-pop-batch".to_owned(),
            },
            listener,
            vec![pop_receipt_message(Some("10 1000 50 2 0 broker-a 0 10"))],
        )
        .await;

    assert!(!listener_called.load(Ordering::Acquire));
    assert_eq!(process_queue.wait_ack_count(), 0);
    let _ = runtime.shutdown().await;
}

#[tokio::test]
async fn pop_orderly_path_skips_listener_hooks_and_listener_result_side_effects() {
    let mut config = PushConsumerConfig::new("pop-orderly-stats-group").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_pop_threshold_for_queue(96);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config.client_config())
        .await
        .unwrap();
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 2));
    process_queue.inc_found_messages(96);
    // pop_loop accounts the newly returned message before dispatch.
    process_queue.inc_found_messages(1);
    let receipt = format!("10 {} 60000 2 0 broker-a 0 10", epoch_millis());
    let listener_called = Arc::new(AtomicBool::new(false));
    let listener = PushMessageListener::orderly({
        let listener_called = listener_called.clone();
        move |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
            listener_called.store(true, Ordering::Release);
            async move { (Some(ConsumeOrderlyStatus::Success), context) }
        }
    });
    let hook_calls = Arc::new(Mutex::new(Vec::new()));
    struct Hook(Arc<Mutex<Vec<&'static str>>>);
    impl ConsumeMessageHook for Hook {
        fn hook_name(&self) -> &str {
            "pop-orderly-parity"
        }

        fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {
            self.0.lock().unwrap().push("before");
        }

        fn consume_message_after(&self, _context: &mut ConsumeMessageContext) {
            self.0.lock().unwrap().push("after");
        }
    }
    consumer.register_consume_message_hook(Arc::new(Hook(hook_calls.clone())));

    consumer
        .clone()
        .consume_pop_batch(
            PopConsumeBatchContext {
                process_queue: process_queue.clone(),
                runtime: runtime.clone(),
                queue,
                route_topic: "orders".to_owned(),
                consumer_group: "pop-orderly-stats-group".to_owned(),
                stats_consumer_group: "pop-orderly-stats-group".to_owned(),
            },
            listener,
            vec![pop_receipt_message(Some(&receipt))],
        )
        .await;

    assert!(!listener_called.load(Ordering::Acquire));
    assert!(hook_calls.lock().unwrap().is_empty());
    assert_eq!(process_queue.wait_ack_count(), 97);
    assert!(pop_queue_wait_ack_flow_controlled(
        process_queue.wait_ack_count(),
        consumer.config.pop_threshold_for_queue()
    ));
    assert!(!pop_queue_wait_ack_flow_controlled(96, 96));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn pop_worker_shutdown_cancels_its_listener_child_and_prevents_late_ack() {
    struct NotifyOnDrop(Arc<tokio::sync::Notify>);
    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let cancelled = Arc::new(tokio::sync::Notify::new());
    let late_side_effect = Arc::new(AtomicBool::new(false));
    let listener = PushMessageListener::concurrently({
        let started = started.clone();
        let release = release.clone();
        let cancelled = cancelled.clone();
        let late_side_effect = late_side_effect.clone();
        move |_messages, context| {
            let started = started.clone();
            let release = release.clone();
            let cancelled = cancelled.clone();
            let late_side_effect = late_side_effect.clone();
            async move {
                let _drop_signal = NotifyOnDrop(cancelled);
                started.notify_one();
                release.notified().await;
                late_side_effect.store(true, Ordering::Release);
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            }
        }
    });
    let mut config = PushConsumerConfig::new("pop-child-cancellation").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    *write_lock(&consumer.pop_listener) = Some(listener);

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 1));
    process_queue.inc_found_messages(1);
    let receipt = format!("10 {} 60000 2 0 broker-a 0 10", epoch_millis());
    consumer
        .dispatch_pop_messages(
            process_queue.clone(),
            runtime.clone(),
            queue,
            "orders".to_owned(),
            "pop-child-cancellation".to_owned(),
            vec![pop_receipt_message(Some(&receipt))],
        )
        .await;
    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("POP listener did not start");

    consumer
        .shutdown_with_await_termination_millis(100)
        .await
        .unwrap();
    timeout(Duration::from_secs(1), cancelled.notified())
        .await
        .expect("aborting the POP worker must drop its listener child");
    release.notify_waiters();
    tokio::task::yield_now().await;

    assert!(!late_side_effect.load(Ordering::Acquire));
    assert_eq!(process_queue.wait_ack_count(), 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelling_direct_consume_drops_its_listener_child() {
    struct NotifyOnDrop(Arc<tokio::sync::Notify>);
    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let cancelled = Arc::new(tokio::sync::Notify::new());
    let late_side_effect = Arc::new(AtomicBool::new(false));
    let listener: Arc<dyn MessageListenerConcurrently> = {
        let started = started.clone();
        let release = release.clone();
        let cancelled = cancelled.clone();
        let late_side_effect = late_side_effect.clone();
        Arc::new(
            move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let started = started.clone();
                let release = release.clone();
                let cancelled = cancelled.clone();
                let late_side_effect = late_side_effect.clone();
                async move {
                    let _drop_signal = NotifyOnDrop(cancelled);
                    started.notify_one();
                    release.notified().await;
                    late_side_effect.store(true, Ordering::Release);
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        )
    };
    let service =
        ConsumeService::Concurrent(Arc::new(ConcurrentConsumeService::new(1, 1, listener)));
    let config = PushConsumerConfig::new("direct-consume-cancellation").unwrap();
    let consumer = Arc::new(PushConsumer::with_manager(
        config,
        ClientRuntimeManager::new(),
    ));
    let task_consumer = consumer.clone();
    let task = tokio::spawn(async move {
        task_consumer
            .consume_message_directly_using_service(
                retry_test_message(10),
                Some("broker-a".to_owned()),
                service,
            )
            .await
    });
    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("direct listener did not start");

    task.abort();
    let _ = task.await;
    timeout(Duration::from_secs(1), cancelled.notified())
        .await
        .expect("cancelling direct consume must drop its listener child");
    release.notify_waiters();
    tokio::task::yield_now().await;

    assert!(!late_side_effect.load(Ordering::Acquire));
}

#[tokio::test]
async fn pop_consume_dispatches_ack_redelivery_and_replacement_receipt() {
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nameserver_address = nameserver.local_addr().unwrap().to_string();
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let route = pop_test_route(&broker_address);

    let nameserver_peer = tokio::spawn(async move {
        let (mut stream, _) = nameserver.accept().await.unwrap();
        let incoming = read_remoting_command(&mut stream).await;
        assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
        let mut reply = RemotingCommand {
            code: response::SUCCESS,
            opaque: incoming.opaque,
            body: Some(route),
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
    });

    let now = epoch_millis();
    let initial_receipt = format!("10 {now} 60000 2 0 broker-a 0 10");
    let redelivery_receipt = format!("10 {now} 30000 2 0 broker-a 0 10");
    let replacement_receipt = format!("10 {} 45000 3 0 broker-a 0 10", now + 1);
    let expected_receipts = [
        initial_receipt.clone(),
        initial_receipt.clone(),
        redelivery_receipt.clone(),
        replacement_receipt.clone(),
    ];
    let broker_peer = tokio::spawn(async move {
        let mut stream = None;
        let expected_codes = [
            request::ACK_MESSAGE,
            request::CHANGE_MESSAGE_INVISIBLETIME,
            request::CHANGE_MESSAGE_INVISIBLETIME,
            request::ACK_MESSAGE,
        ];
        for (index, expected_code) in expected_codes.into_iter().enumerate() {
            let incoming = accept_remoting_request(&broker_listener, &mut stream).await;
            assert_eq!(incoming.code, expected_code);
            let fields = incoming.ext_fields.as_ref().unwrap();
            assert_eq!(
                fields.get("consumerGroup"),
                Some(&"pop-lifecycle-group".into())
            );
            assert_eq!(fields.get("topic"), Some(&"orders".into()));
            assert_eq!(fields.get("queueId"), Some(&"0".into()));
            assert_eq!(fields.get("offset"), Some(&"10".into()));
            assert_eq!(fields.get("extraInfo"), Some(&expected_receipts[index]));
            if expected_code == request::CHANGE_MESSAGE_INVISIBLETIME {
                let (pop_time, invisible_time, revive_qid) = if index == 1 {
                    (now, 30_000, 2)
                } else {
                    (now + 1, 45_000, 3)
                };
                assert_eq!(
                    fields.get("invisibleTime"),
                    Some(&(if index == 1 { "10000" } else { "30000" }).to_owned())
                );
                write_remoting_reply(
                    stream.as_mut().unwrap(),
                    &incoming,
                    response::SUCCESS,
                    Some(HeaderMap::from([
                        ("popTime".into(), pop_time.to_string()),
                        ("invisibleTime".into(), invisible_time.to_string()),
                        ("reviveQid".into(), revive_qid.to_string()),
                    ])),
                )
                .await;
            } else {
                write_remoting_reply(stream.as_mut().unwrap(), &incoming, response::SUCCESS, None)
                    .await;
            }
        }
    });

    let mut config = PushConsumerConfig::new("pop-lifecycle-group").unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(nameserver_address));
    config
        .client_config_mut()
        .set_instance_name(Some("pop-lifecycle-client".into()));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let pop_hook_events = Arc::new(Mutex::new(Vec::new()));
    struct PopHook(Arc<Mutex<Vec<String>>>);
    impl ConsumeMessageHook for PopHook {
        fn hook_name(&self) -> &str {
            "pop-consume-hook"
        }

        fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {
            self.0.lock().unwrap().push("before".into());
        }

        fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
            self.0.lock().unwrap().push(format!(
                "{}:{}",
                context.status.as_deref().unwrap_or("missing"),
                context.success
            ));
        }
    }
    consumer.register_consume_message_hook(Arc::new(PopHook(pop_hook_events.clone())));
    let runtime = manager
        .get_or_create(consumer.config.client_config())
        .await
        .unwrap();
    consumer.bind_stats_manager(runtime.consumer_stats_manager());
    runtime.remoting().start().unwrap();
    runtime
        .update_topic_route("orders", Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    nameserver_peer.await.unwrap();
    *write_lock(&consumer.consume_stats_group) = Some("pop-lifecycle-group".to_owned());
    consumer.set_consumer_group(Some("pop-lifecycle-live-group".into()));

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let successful_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 1));
    successful_queue.inc_found_messages(1);
    let listener_calls = Arc::new(AtomicUsize::new(0));
    let successful_listener = PushMessageListener::concurrently({
        let listener_calls = listener_calls.clone();
        move |_messages, context| {
            listener_calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready((Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context))
        }
    });
    consumer
        .clone()
        .consume_pop_batch(
            PopConsumeBatchContext {
                process_queue: successful_queue.clone(),
                runtime: runtime.clone(),
                queue: queue.clone(),
                route_topic: "orders".into(),
                consumer_group: "pop-lifecycle-group".into(),
                stats_consumer_group: consumer.consume_stats_group(),
            },
            successful_listener,
            vec![pop_receipt_message(Some(&initial_receipt))],
        )
        .await;
    assert_eq!(listener_calls.load(Ordering::SeqCst), 1);
    assert_eq!(successful_queue.wait_ack_count(), 0);
    tokio::time::sleep(Duration::from_secs(11)).await;
    assert!(
        consumer
            .stats_manager()
            .unwrap()
            .consume_status("pop-lifecycle-group", "orders")
            .get_consume_ok_tps()
            > 0.0
    );
    assert_eq!(
        consumer
            .stats_manager()
            .unwrap()
            .consume_status("pop-lifecycle-live-group", "orders")
            .get_consume_ok_tps(),
        0.0
    );

    let retry_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 2));
    retry_queue.inc_found_messages(1);
    let retry_listener = PushMessageListener::concurrently(|_messages, context| {
        std::future::ready((Some(ConsumeConcurrentlyStatus::ReconsumeLater), context))
    });
    consumer
        .clone()
        .consume_pop_batch(
            PopConsumeBatchContext {
                process_queue: retry_queue.clone(),
                runtime: runtime.clone(),
                queue: queue.clone(),
                route_topic: "orders".into(),
                consumer_group: "pop-lifecycle-group".into(),
                stats_consumer_group: consumer.consume_stats_group(),
            },
            retry_listener,
            vec![pop_receipt_message(Some(&initial_receipt))],
        )
        .await;
    assert_eq!(retry_queue.wait_ack_count(), 0);
    assert_eq!(
        pop_hook_events.lock().unwrap().as_slice(),
        [
            "before",
            "CONSUME_SUCCESS:true",
            "before",
            "RECONSUME_LATER:false"
        ]
    );

    let mut changed_message = pop_receipt_message(Some(&redelivery_receipt));
    consumer
        .change_pop_invisible_time(
            &runtime,
            &mut changed_message,
            "orders",
            "pop-lifecycle-group",
            30_000,
        )
        .await;
    assert_eq!(
        changed_message.properties().get(PROPERTY_POP_CK),
        Some(&replacement_receipt)
    );
    assert_eq!(
        consumer
            .ack_pop_message(&runtime, &changed_message, "orders", "pop-lifecycle-group")
            .await
            .unwrap(),
        AckStatus::Ok
    );

    broker_peer.await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn broker_assignment_error_preserves_worker_until_empty_assignment() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let pop_rebalance = fixture["popRebalance"].as_array().unwrap();
    assert_eq!(pop_rebalance.len(), 4);
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nameserver_address = nameserver.local_addr().unwrap().to_string();
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let route = pop_test_route(&broker_address);
    let nameserver_peer = tokio::spawn(async move {
        let (mut stream, _) = nameserver.accept().await.unwrap();
        let incoming = read_remoting_command(&mut stream).await;
        assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
        let mut reply = RemotingCommand {
            code: response::SUCCESS,
            opaque: incoming.opaque,
            body: Some(route),
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
    });

    let pop_request_count = Arc::new(AtomicUsize::new(0));
    let peer_pop_request_count = pop_request_count.clone();
    let broker_peer = tokio::spawn(async move {
        let mut stream = None;
        let mut assignment_calls = 0;
        loop {
            let incoming = accept_remoting_request(&broker_listener, &mut stream).await;
            match incoming.code {
                request::QUERY_ASSIGNMENT => {
                    let body: serde_json::Value = serde_json::from_slice(
                        incoming.body.as_deref().expect("assignment request body"),
                    )
                    .unwrap();
                    assert_eq!(body["topic"], "orders");
                    assert_eq!(body["consumerGroup"], "pop-assignment-flow");
                    assignment_calls += 1;
                    if assignment_calls == 3 {
                        write_remoting_reply(
                            stream.as_mut().unwrap(),
                            &incoming,
                            response::SYSTEM_ERROR,
                            None,
                        )
                        .await;
                        continue;
                    }
                    let assignments = if assignment_calls <= 4 {
                        serde_json::json!([{
                            "messageQueue": {
                                "topic": "orders",
                                "brokerName": "broker-a",
                                "queueId": 0
                            },
                            "mode": "POP"
                        }])
                    } else {
                        serde_json::json!([])
                    };
                    write_remoting_reply_with_body(
                        stream.as_mut().unwrap(),
                        &incoming,
                        response::SUCCESS,
                        None,
                        Some(
                            serde_json::to_vec(&serde_json::json!({
                                "messageQueueAssignments": assignments
                            }))
                            .unwrap(),
                        ),
                    )
                    .await;
                }
                request::POP_MESSAGE => {
                    let fields = incoming.ext_fields.as_ref().unwrap();
                    assert_eq!(fields.get("maxMsgNums"), Some(&"7".to_owned()));
                    assert_eq!(fields.get("invisibleTime"), Some(&"12345".to_owned()));
                    if peer_pop_request_count.fetch_add(1, Ordering::SeqCst) == 0 {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        write_remoting_reply_with_body(
                            stream.as_mut().unwrap(),
                            &incoming,
                            response::SUCCESS,
                            Some(HeaderMap::from([
                                ("popTime".into(), epoch_millis().to_string()),
                                ("invisibleTime".into(), "60000".into()),
                                ("reviveQid".into(), "2".into()),
                                ("restNum".into(), "0".into()),
                            ])),
                            Some(pop_test_message_record()),
                        )
                        .await;
                    } else {
                        write_remoting_reply(
                            stream.as_mut().unwrap(),
                            &incoming,
                            response::PULL_NOT_FOUND,
                            None,
                        )
                        .await;
                    }
                }
                request::HEART_BEAT | request::UNREGISTER_CLIENT | request::CHECK_CLIENT_CONFIG => {
                    write_remoting_reply(
                        stream.as_mut().unwrap(),
                        &incoming,
                        response::SUCCESS,
                        None,
                    )
                    .await;
                }
                code => panic!("unexpected scripted Broker request code {code}"),
            }
        }
    });

    let mut config = PushConsumerConfig::new("pop-assignment-flow").unwrap();
    config.set_client_rebalance(false);
    config
        .client_config_mut()
        .set_namesrv_addr(Some(nameserver_address));
    config
        .client_config_mut()
        .set_instance_name(Some("pop-assignment-client".into()));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    write_lock(&consumer.subscriptions).insert(
        "orders".to_owned(),
        SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
    );
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    consumer.config().set_pop_batch_nums(7);
    consumer.config().set_pop_invisible_time_millis(12_345);
    runtime.remoting().start().unwrap();
    runtime
        .update_topic_route("orders", Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    nameserver_peer.await.unwrap();

    let queue = MessageQueue::new("orders", "broker-a", 0);
    assert!(consumer.rebalance_service.rebalance(&consumer).await);
    timeout(Duration::from_secs(3), async {
        while pop_request_count.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("assigned POP worker did not issue POP_MESSAGE");
    timeout(Duration::from_secs(12), async {
        while consumer
            .stats_manager()
            .unwrap()
            .consume_status("pop-assignment-flow", "orders")
            .get_pull_rt()
            <= 0.0
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("POP_FOUND must record pull latency in consumer statistics");
    let status = consumer
        .stats_manager()
        .unwrap()
        .consume_status("pop-assignment-flow", "orders");
    assert!(
        status.get_pull_rt() > 0.0,
        "unexpected POP stats: {status:?}"
    );
    assert!(
        status.get_pull_tps() > 0.0,
        "unexpected POP stats: {status:?}"
    );
    let process_queue = read_lock(&consumer.pop_process_queues)
        .get(&queue)
        .cloned()
        .expect("Broker POP assignment must activate its queue");
    assert!(!process_queue.is_dropped());
    assert!(
        consumer
            .pop_workers
            .lock()
            .unwrap()
            .get(&queue)
            .is_some_and(|worker| !worker.is_finished())
    );

    process_queue.set_last_pop_timestamp(epoch_millis() - 120_001);
    consumer
        .pull_max_idle_time_millis
        .store(i64::MAX, Ordering::Release);
    consumer
        .pop_pull_max_idle_time_millis
        .store(-1, Ordering::Release);
    let requests_before_replacement = pop_request_count.load(Ordering::SeqCst);
    assert!(consumer.rebalance_service.rebalance(&consumer).await);
    assert_eq!(
        process_queue.is_dropped(),
        pop_rebalance[0]["oldDropped"].as_bool().unwrap()
    );
    let replacement = read_lock(&consumer.pop_process_queues)
        .get(&queue)
        .cloned()
        .expect("authoritative POP assignment must replace an expired queue");
    assert_eq!(
        !Arc::ptr_eq(&process_queue, &replacement),
        pop_rebalance[0]["replaced"].as_bool().unwrap()
    );
    assert_eq!(
        read_lock(&consumer.pop_process_queues).contains_key(&queue),
        pop_rebalance[0]["present"].as_bool().unwrap()
    );
    timeout(Duration::from_secs(3), async {
        while pop_request_count.load(Ordering::SeqCst) <= requests_before_replacement {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replacement POP worker did not start");

    assert!(
        consumer
            .pop_workers
            .lock()
            .unwrap()
            .get(&queue)
            .is_some_and(|worker| !worker.is_finished())
    );

    consumer
        .pop_pull_max_idle_time_millis
        .store(-1, Ordering::Release);
    assert!(!consumer.rebalance_service.rebalance(&consumer).await);
    assert!(!replacement.is_dropped());
    assert!(Arc::ptr_eq(
        &replacement,
        &read_lock(&consumer.pop_process_queues)[&queue]
    ));
    assert_eq!(
        replacement.is_dropped(),
        pop_rebalance[1]["dropped"].as_bool().unwrap()
    );
    assert_eq!(
        read_lock(&consumer.pop_process_queues).contains_key(&queue),
        pop_rebalance[1]["present"].as_bool().unwrap()
    );
    assert_eq!(
        Arc::ptr_eq(
            &replacement,
            &read_lock(&consumer.pop_process_queues)[&queue]
        ),
        pop_rebalance[1]["same"].as_bool().unwrap()
    );
    assert!(
        consumer
            .pop_workers
            .lock()
            .unwrap()
            .get(&queue)
            .is_some_and(|worker| !worker.is_finished())
    );

    consumer
        .pop_pull_max_idle_time_millis
        .store(120_000, Ordering::Release);
    replacement.set_last_pop_timestamp(epoch_millis());
    replacement.drop_assignment();
    consumer.rebalance_service.rebalance(&consumer).await;
    assert_eq!(
        replacement.is_dropped(),
        pop_rebalance[2]["dropped"].as_bool().unwrap()
    );
    assert_eq!(
        read_lock(&consumer.pop_process_queues).contains_key(&queue),
        pop_rebalance[2]["present"].as_bool().unwrap()
    );
    assert_eq!(
        Arc::ptr_eq(
            &replacement,
            &read_lock(&consumer.pop_process_queues)[&queue]
        ),
        pop_rebalance[2]["same"].as_bool().unwrap()
    );

    assert!(consumer.rebalance_service.rebalance(&consumer).await);
    assert_eq!(
        replacement.is_dropped(),
        pop_rebalance[3]["dropped"].as_bool().unwrap()
    );
    assert_eq!(
        read_lock(&consumer.pop_process_queues).contains_key(&queue),
        pop_rebalance[3]["present"].as_bool().unwrap()
    );
    assert!(!consumer.pop_workers.lock().unwrap().contains_key(&queue));
    let pop_count_after_removal = pop_request_count.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        pop_request_count.load(Ordering::SeqCst),
        pop_count_after_removal,
        "removed POP workers must not issue more Broker requests"
    );

    consumer.shutdown().await.unwrap();
    broker_peer.abort();
}

#[tokio::test]
async fn assigned_pull_queue_expiry_matches_java_replacement_and_retention() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let pull_rebalance = fixture["pullRebalance"].as_array().unwrap();
    assert_eq!(pull_rebalance.len(), 4);

    let mut config = PushConsumerConfig::new("pull-assignment-expiry").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    config.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let queue = MessageQueue::new("orders", "broker-a", 0);
    write_lock(&consumer.subscriptions).insert(
        "orders".to_owned(),
        SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
    );
    write_lock(&consumer.topic_queues).insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
    let _runtime = install_started_consumer(
        &consumer,
        &manager,
        store,
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;

    let expired = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 91));
    expired.set_last_pull_timestamp(epoch_millis() - 120_001);
    write_lock(&consumer.process_queues).insert(queue.clone(), expired.clone());
    consumer
        .pull_max_idle_time_millis
        .store(-1, Ordering::Release);
    consumer.rebalance_service.rebalance(&consumer).await;
    let replacement = read_lock(&consumer.process_queues)
        .get(&queue)
        .cloned()
        .expect("assigned PULL queue must replace an expired process queue");
    assert_eq!(
        expired.is_dropped(),
        pull_rebalance[0]["oldDropped"].as_bool().unwrap()
    );
    assert_eq!(
        read_lock(&consumer.process_queues).contains_key(&queue),
        pull_rebalance[0]["present"].as_bool().unwrap()
    );
    assert_eq!(
        !Arc::ptr_eq(&expired, &replacement),
        pull_rebalance[0]["replaced"].as_bool().unwrap()
    );

    consumer
        .pull_max_idle_time_millis
        .store(i64::MAX, Ordering::Release);
    replacement.set_last_pull_timestamp(epoch_millis());
    consumer.rebalance_service.rebalance(&consumer).await;
    assert_eq!(
        replacement.is_dropped(),
        pull_rebalance[1]["dropped"].as_bool().unwrap()
    );
    assert_eq!(
        read_lock(&consumer.process_queues).contains_key(&queue),
        pull_rebalance[1]["present"].as_bool().unwrap()
    );
    assert_eq!(
        Arc::ptr_eq(&replacement, &read_lock(&consumer.process_queues)[&queue]),
        pull_rebalance[1]["same"].as_bool().unwrap()
    );

    replacement
        .drop_assignment(replacement.generation())
        .unwrap();
    consumer.rebalance_service.rebalance(&consumer).await;
    assert_eq!(
        replacement.is_dropped(),
        pull_rebalance[2]["dropped"].as_bool().unwrap()
    );
    assert_eq!(
        read_lock(&consumer.process_queues).contains_key(&queue),
        pull_rebalance[2]["present"].as_bool().unwrap()
    );
    assert_eq!(
        Arc::ptr_eq(&replacement, &read_lock(&consumer.process_queues)[&queue]),
        pull_rebalance[2]["same"].as_bool().unwrap()
    );

    write_lock(&consumer.topic_queues).insert("orders".to_owned(), BTreeSet::new());
    consumer.rebalance_service.rebalance(&consumer).await;
    assert_eq!(
        replacement.is_dropped(),
        pull_rebalance[3]["dropped"].as_bool().unwrap()
    );
    assert_eq!(
        read_lock(&consumer.process_queues).contains_key(&queue),
        pull_rebalance[3]["present"].as_bool().unwrap()
    );

    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn removed_pop_queue_finishes_inflight_found_callback_before_retiring() {
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nameserver_address = nameserver.local_addr().unwrap().to_string();
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let route = pop_test_route(&broker_address);
    let nameserver_peer = tokio::spawn(async move {
        let (mut stream, _) = nameserver.accept().await.unwrap();
        let incoming = read_remoting_command(&mut stream).await;
        assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
        let mut reply = RemotingCommand {
            code: response::SUCCESS,
            opaque: incoming.opaque,
            body: Some(route),
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
    });

    let (pop_received, pop_received_signal) = tokio::sync::oneshot::channel();
    let (release_pop, mut release_pop_signal) = tokio::sync::oneshot::channel();
    let (ack_received, ack_received_signal) = tokio::sync::oneshot::channel();
    let filtered_receipt = format!("17 {} 60000 2 0 broker-a 0 10", epoch_millis());
    let accepted_receipt = format!("18 {} 60000 2 0 broker-a 0 11", epoch_millis());
    let mut found_record = pop_test_message_record_with_receipt(&filtered_receipt);
    found_record.extend_from_slice(&pop_test_message_record_with_tag_and_receipt(
        "blue",
        &accepted_receipt,
    ));
    let ack_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let peer_ack_count = ack_count.clone();
    let broker_peer = tokio::spawn(async move {
        let (request_sender, mut request_receiver) = tokio::sync::mpsc::unbounded_channel::<(
            RemotingCommand,
            Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
        )>();
        let mut readers = tokio::task::JoinSet::new();
        let mut assignment_calls = 0;
        let mut held_pop = None;
        let mut pop_received = Some(pop_received);
        let mut ack_received = Some(ack_received);
        loop {
            tokio::select! {
                accepted = broker_listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    let (mut reader, writer) = stream.into_split();
                    let writer = Arc::new(tokio::sync::Mutex::new(writer));
                    let sender = request_sender.clone();
                    let task_writer = writer.clone();
                    readers.spawn(async move {
                        loop {
                            let incoming = read_remoting_command(&mut reader).await;
                            if sender.send((incoming, task_writer.clone())).is_err() {
                                break;
                            }
                        }
                    });
                }
                Some((incoming, writer)) = request_receiver.recv() => {
                    match incoming.code {
                        request::QUERY_ASSIGNMENT => {
                            let body: serde_json::Value = serde_json::from_slice(
                                incoming.body.as_deref().expect("assignment request body"),
                            )
                            .unwrap();
                            assert_eq!(body["topic"], "orders");
                            assert_eq!(body["consumerGroup"], "pop-inflight-drop");
                            assignment_calls += 1;
                            let assignments = if assignment_calls == 1 {
                                serde_json::json!([{
                                    "messageQueue": {
                                        "topic": "orders",
                                        "brokerName": "broker-a",
                                        "queueId": 0
                                    },
                                    "mode": "POP"
                                }])
                            } else {
                                serde_json::json!([])
                            };
                            let mut writer = writer.lock().await;
                            write_remoting_reply_with_body(
                                &mut *writer,
                                &incoming,
                                response::SUCCESS,
                                None,
                                Some(
                                    serde_json::to_vec(&serde_json::json!({
                                        "messageQueueAssignments": assignments
                                    }))
                                    .unwrap(),
                                ),
                            )
                            .await;
                        }
                        request::POP_MESSAGE => {
                            let fields = incoming.ext_fields.as_ref().unwrap();
                            assert_eq!(fields.get("maxMsgNums"), Some(&"7".to_owned()));
                            held_pop = Some((incoming, writer));
                            pop_received.take().unwrap().send(()).unwrap();
                        }
                        request::ACK_MESSAGE => {
                            peer_ack_count.fetch_add(1, Ordering::SeqCst);
                            let fields = incoming.ext_fields.as_ref().unwrap();
                            assert_eq!(
                                fields.get("consumerGroup"),
                                Some(&"pop-inflight-drop".to_owned())
                            );
                            assert_eq!(fields.get("topic"), Some(&"orders".to_owned()));
                            assert_eq!(fields.get("extraInfo"), Some(&filtered_receipt));
                            let mut writer = writer.lock().await;
                            write_remoting_reply(
                                &mut *writer,
                                &incoming,
                                response::SUCCESS,
                                None,
                            )
                            .await;
                            if let Some(ack_received) = ack_received.take() {
                                ack_received.send(()).unwrap();
                            }
                        }
                        request::HEART_BEAT
                        | request::UNREGISTER_CLIENT
                        | request::CHECK_CLIENT_CONFIG => {
                            let mut writer = writer.lock().await;
                            write_remoting_reply(
                                &mut *writer,
                                &incoming,
                                response::SUCCESS,
                                None,
                            )
                            .await;
                        }
                        code => panic!("unexpected scripted Broker request code {code}"),
                    }
                }
                release = &mut release_pop_signal, if held_pop.is_some() => {
                    release.unwrap();
                    let (request, writer) = held_pop.take().unwrap();
                    let mut writer = writer.lock().await;
                    write_remoting_reply_with_body(
                        &mut *writer,
                        &request,
                        response::SUCCESS,
                        Some(HeaderMap::from([
                            ("popTime".into(), epoch_millis().to_string()),
                            ("invisibleTime".into(), "60000".into()),
                            ("reviveQid".into(), "2".into()),
                            ("restNum".into(), "0".into()),
                        ])),
                        Some(found_record.clone()),
                    )
                    .await;
                }
                Some(_) = readers.join_next(), if !readers.is_empty() => {}
            }
        }
    });

    let mut config = PushConsumerConfig::new("pop-inflight-drop").unwrap();
    config.set_client_rebalance(false);
    config
        .client_config_mut()
        .set_namesrv_addr(Some(nameserver_address));
    config
        .client_config_mut()
        .set_instance_name(Some("pop-inflight-drop-client".into()));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let mut subscription =
        SubscriptionData::new(Some("orders".to_owned()), Some("blue".to_owned()));
    subscription.set_tags_set_owned(Some(HashSet::from([Some("blue".to_owned())])));
    write_lock(&consumer.subscriptions).insert("orders".to_owned(), subscription);
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    let listener_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    *write_lock(&consumer.pop_listener) = Some(PushMessageListener::concurrently({
        let listener_calls = listener_calls.clone();
        move |_messages, context| {
            listener_calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready((Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context))
        }
    }));
    consumer.config().set_pop_batch_nums(7);
    consumer.config().set_pop_invisible_time_millis(60_000);
    runtime.remoting().start().unwrap();
    runtime
        .update_topic_route("orders", Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    nameserver_peer.await.unwrap();

    assert!(consumer.rebalance_service.rebalance(&consumer).await);
    timeout(Duration::from_secs(3), pop_received_signal)
        .await
        .expect("assigned POP worker did not issue POP_MESSAGE")
        .unwrap();
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = read_lock(&consumer.pop_process_queues)
        .get(&queue)
        .cloned()
        .expect("the initial POP assignment must exist");

    assert!(consumer.rebalance_service.rebalance(&consumer).await);
    assert!(process_queue.is_dropped());
    assert!(!read_lock(&consumer.pop_process_queues).contains_key(&queue));
    assert!(!consumer.pop_workers.lock().unwrap().contains_key(&queue));
    assert_eq!(consumer.pop_retired_workers.lock().unwrap().len(), 1);
    release_pop.send(()).unwrap();
    timeout(Duration::from_secs(5), ack_received_signal)
        .await
        .expect("FOUND callback must ACK filtered messages after assignment removal")
        .unwrap();
    timeout(Duration::from_secs(3), async {
        while consumer
            .pop_retired_workers
            .lock()
            .unwrap()
            .iter()
            .any(|worker| !worker.is_finished())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("retired POP worker must exit after processing its in-flight response");
    timeout(Duration::from_secs(3), async {
        while consumer
            .pop_consume_workers
            .lock()
            .unwrap()
            .iter()
            .any(|worker| !worker.is_finished())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("accepted message consume request must finish on the dropped queue");
    assert_eq!(listener_calls.load(Ordering::SeqCst), 0);
    assert_eq!(ack_count.load(Ordering::SeqCst), 1);

    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
    broker_peer.abort();
}

#[tokio::test]
async fn rebalance_truncates_unsubscribed_pop_queue_and_retires_inflight_worker() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let expected = &fixture["popUnsubscribedTruncation"];

    let mut config = PushConsumerConfig::new("pop-unsubscribed-truncation").unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(String::new()));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 1));
    let worker_stopped = Arc::new(AtomicBool::new(false));
    struct WorkerDropSignal(Arc<AtomicBool>);
    impl Drop for WorkerDropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let (finish_worker, worker_finished) = tokio::sync::oneshot::channel();
    let drop_signal = WorkerDropSignal(worker_stopped.clone());
    let worker = tokio::spawn(async move {
        let _drop_signal = drop_signal;
        let _ = worker_finished.await;
    });
    write_lock(&consumer.pop_process_queues).insert(queue.clone(), process_queue.clone());
    consumer
        .pop_workers
        .lock()
        .unwrap()
        .insert(queue.clone(), worker);

    assert!(consumer.rebalance_service.rebalance(&consumer).await);
    assert_eq!(
        process_queue.is_dropped(),
        expected["dropped"].as_bool().unwrap()
    );
    assert_eq!(
        read_lock(&consumer.pop_process_queues).contains_key(&queue),
        expected["present"].as_bool().unwrap()
    );
    assert!(!consumer.pop_workers.lock().unwrap().contains_key(&queue));
    assert!(!worker_stopped.load(Ordering::Acquire));
    assert_eq!(consumer.pop_retired_workers.lock().unwrap().len(), 1);
    finish_worker.send(()).unwrap();
    timeout(Duration::from_secs(1), async {
        while !worker_stopped.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("retired POP worker must finish after its in-flight operation");

    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn rebalance_switch_to_pull_removes_pop_queue_and_retires_inflight_worker() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let expected = &fixture["popToPullTransition"];

    let mut config = PushConsumerConfig::new("pop-to-pull").unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(String::new()));
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let queue = MessageQueue::new("orders", "broker-a", 0);
    write_lock(&consumer.subscriptions).insert(
        "orders".to_owned(),
        SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
    );
    write_lock(&consumer.topic_queues).insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;

    let pull_process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 2));
    let pop_process_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 3));
    write_lock(&consumer.process_queues).insert(queue.clone(), pull_process_queue.clone());
    write_lock(&consumer.pop_process_queues).insert(queue.clone(), pop_process_queue.clone());
    let worker_stopped = Arc::new(AtomicBool::new(false));
    struct WorkerDropSignal(Arc<AtomicBool>);
    impl Drop for WorkerDropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let (finish_worker, worker_finished) = tokio::sync::oneshot::channel();
    let drop_signal = WorkerDropSignal(worker_stopped.clone());
    let worker = tokio::spawn(async move {
        let _drop_signal = drop_signal;
        let _ = worker_finished.await;
    });
    consumer
        .pop_workers
        .lock()
        .unwrap()
        .insert(queue.clone(), worker);

    assert!(consumer.rebalance_service.rebalance(&consumer).await);
    assert_eq!(
        pop_process_queue.is_dropped(),
        expected["popDropped"].as_bool().unwrap()
    );
    assert_eq!(
        read_lock(&consumer.pop_process_queues).contains_key(&queue),
        expected["popPresent"].as_bool().unwrap()
    );
    let retained_pull_queue = read_lock(&consumer.process_queues)
        .get(&queue)
        .cloned()
        .expect("PULL assignment retains its active process queue");
    assert_eq!(
        read_lock(&consumer.process_queues).contains_key(&queue),
        expected["pullPresent"].as_bool().unwrap()
    );
    assert_eq!(
        Arc::ptr_eq(&pull_process_queue, &retained_pull_queue),
        expected["pullSame"].as_bool().unwrap()
    );
    assert!(!retained_pull_queue.is_dropped());
    assert!(!consumer.pop_workers.lock().unwrap().contains_key(&queue));
    assert!(!worker_stopped.load(Ordering::Acquire));
    assert_eq!(consumer.pop_retired_workers.lock().unwrap().len(), 1);
    finish_worker.send(()).unwrap();
    timeout(Duration::from_secs(1), async {
        while !worker_stopped.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("retired POP worker must finish after its in-flight operation");

    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

fn retry_test_message(offset: i64) -> MessageExt {
    // Java 5.5.1 producer-produced `ipv4-v1` record from
    // fixtures/messages/records.json (SHA-256 2F93B0A748E645D8AD2CEA027ABDF2C356ED16B74CEAF70A63D97B0A04861BD0CF).
    use base64::Engine;
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/messages/records.json"
    ))
    .unwrap();
    let decoded = &fixture["cases"][0]["decoded"];
    let properties: HashMap<String, String> =
        serde_json::from_value(decoded["properties"].clone()).unwrap();
    let mut body = Message::new(
        decoded["topic"].as_str().unwrap(),
        base64::engine::general_purpose::STANDARD
            .decode(decoded["bodyBase64"].as_str().unwrap())
            .unwrap(),
    );
    body.set_flag(decoded["flag"].as_i64().unwrap() as i32);
    for (key, value) in properties {
        body.set_system_property(key, value).unwrap();
    }
    let mut message = crate::test_support::message_ext_from(body);
    message.set_queue_offset(offset);
    message.set_broker_name(Some("broker-a".into()));
    message.set_commit_log_offset(offset + 100);
    message.set_message_id(Some(format!("broker-msg-{offset}")));
    message.set_reconsume_times(2);
    message
        .set_system_property(PROPERTY_ORIGIN_MESSAGE_ID, "origin-msg")
        .unwrap();
    message
        .set_system_property(PROPERTY_RECONSUME_TIME, "2")
        .unwrap();
    message
        .set_system_property(PROPERTY_MAX_RECONSUME_TIMES, "7")
        .unwrap();
    message
        .set_system_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "source-uniq")
        .unwrap();
    message
        .set_system_property(PROPERTY_TRANSACTION_PREPARED, "true")
        .unwrap();
    message
        .set_system_property(PROPERTY_KEYS, "order-1")
        .unwrap();
    message
}

fn retry_topic_message(topic: &str, origin_topic: Option<&str>, offset: i64) -> MessageExt {
    let mut message = crate::test_support::message_ext_from(Message::new(topic, Vec::new()));
    message.set_queue_offset(offset);
    if let Some(origin_topic) = origin_topic {
        message
            .set_system_property(PROPERTY_RETRY_TOPIC, origin_topic)
            .unwrap();
    }
    message
}

fn consume_request(
    queue: MessageQueue,
    generation: u64,
    messages: Vec<MessageExt>,
) -> ConsumeRequest {
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue, generation));
    process_queue.insert_messages(generation, messages).unwrap();
    let batch = process_queue
        .take_batch(generation, usize::MAX)
        .unwrap()
        .unwrap();
    ConsumeRequest {
        process_queue,
        batch,
    }
}

struct ConsumerGroupHookRecorder {
    before: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    after: Arc<std::sync::Mutex<Vec<Option<String>>>>,
}

impl ConsumeMessageHook for ConsumerGroupHookRecorder {
    fn hook_name(&self) -> &str {
        "consumer-group-hook-recorder"
    }

    fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
        self.before
            .lock()
            .unwrap()
            .push(context.consumer_group.clone());
    }

    fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
        self.after
            .lock()
            .unwrap()
            .push(context.consumer_group.clone());
    }
}

fn idle_consume_service() -> Arc<ConcurrentConsumeService> {
    Arc::new(ConcurrentConsumeService::new(
        1,
        1,
        Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        ),
    ))
}

#[derive(Default)]
struct RecordingConsumeMessageService {
    direct_calls: AtomicUsize,
    pull_submissions: AtomicUsize,
    pop_submissions: AtomicUsize,
    shutdown_calls: AtomicUsize,
    submitted_offsets: Mutex<Vec<i64>>,
}

impl ConsumeMessageService for RecordingConsumeMessageService {
    fn start(&self) -> ConsumeMessageServiceFuture<'_, ()> {
        Box::pin(async {})
    }

    fn shutdown(&self, _await_termination_millis: i64) -> ConsumeMessageServiceFuture<'_, ()> {
        self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {})
    }

    fn update_core_pool_size(&self, _core_pool_size: i32) {}

    fn inc_core_pool_size(&self) {}

    fn dec_core_pool_size(&self) {}

    fn core_pool_size(&self) -> i32 {
        1
    }

    fn consume_message_directly(
        &self,
        _message: MessageExt,
        _broker_name: Option<String>,
    ) -> ConsumeMessageServiceFuture<'_, ConsumeMessageDirectlyResult> {
        self.direct_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { ConsumeMessageDirectlyResult::default() })
    }

    fn submit_consume_request(
        &self,
        messages: Vec<Arc<MessageExt>>,
        _process_queue: Arc<ProcessQueue>,
        _message_queue: MessageQueue,
        _dispatch_to_consume: bool,
    ) {
        self.pull_submissions.fetch_add(1, Ordering::SeqCst);
        self.submitted_offsets
            .lock()
            .unwrap()
            .extend(messages.iter().map(|message| message.queue_offset()));
    }

    fn submit_pop_consume_request(
        &self,
        _messages: Vec<MessageExt>,
        _process_queue: Arc<PopProcessQueue>,
        _message_queue: MessageQueue,
    ) {
        self.pop_submissions.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn replacement_consume_service_receives_push_work_but_not_pop_work() {
    let mut config = PushConsumerConfig::new("replacement-consume-message-service").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let _runtime = install_started_consumer(
        &consumer,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;

    let replacement = Arc::new(RecordingConsumeMessageService::default());
    let replacement_api: Arc<dyn ConsumeMessageService> = replacement.clone();
    consumer.set_consume_message_service(Some(replacement_api));

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 40));
    process_queue
        .insert_messages(40, vec![retry_test_message(50)])
        .unwrap();
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    let context = consumer.pull_context().await.unwrap();
    consumer.submit_consume_batch(&context, process_queue.clone(), 40);

    assert_eq!(replacement.pull_submissions.load(Ordering::SeqCst), 1);
    assert_eq!(*replacement.submitted_offsets.lock().unwrap(), [50]);
    assert_eq!(process_queue.snapshot(40).unwrap().in_flight_count, 1);

    consumer
        .consume_message_directly(retry_test_message(51), Some("broker-a".into()))
        .await
        .unwrap();
    assert_eq!(replacement.direct_calls.load(Ordering::SeqCst), 1);

    let pop_queue = Arc::new(PopProcessQueue::new_for_assignment(queue.clone(), 1));
    pop_queue.inc_found_messages(1);
    consumer
        .dispatch_pop_messages(
            pop_queue.clone(),
            context.runtime,
            queue,
            "orders".into(),
            "replacement-consume-message-service".into(),
            vec![retry_test_message(52)],
        )
        .await;
    assert_eq!(replacement.pop_submissions.load(Ordering::SeqCst), 0);
    assert_eq!(pop_queue.wait_ack_count(), 0);

    consumer.shutdown().await.unwrap();
    assert_eq!(replacement.shutdown_calls.load(Ordering::SeqCst), 1);
    consumer.set_consume_message_service(None);
    assert!(matches!(
        consumer.update_core_pool_size(2).await,
        Err(PushConsumerError::ConsumeMessageServiceUnavailable)
    ));
}

async fn install_started_consumer(
    consumer: &Arc<PushConsumer>,
    manager: &ClientRuntimeManager,
    offset_store: Arc<dyn OffsetStore>,
    consume_service: Arc<ConcurrentConsumeService>,
    pull_scheduler: Arc<PullScheduler>,
    pull_rpc_workers: Arc<PullRpcWorkers>,
) -> Arc<ClientRuntime> {
    install_started_consumer_with_service(
        consumer,
        manager,
        offset_store,
        ConsumeService::Concurrent(consume_service),
        None,
        pull_scheduler,
        pull_rpc_workers,
    )
    .await
}

async fn install_started_consumer_with_service(
    consumer: &Arc<PushConsumer>,
    manager: &ClientRuntimeManager,
    offset_store: Arc<dyn OffsetStore>,
    consume_service: ConsumeService,
    orderly_lock_settings: Option<OrderlyLockSettings>,
    pull_scheduler: Arc<PullScheduler>,
    pull_rpc_workers: Arc<PullRpcWorkers>,
) -> Arc<ClientRuntime> {
    *consumer.owner.lock().unwrap() = Arc::downgrade(consumer);
    consumer.bind_builtin_consume_message_service(Arc::new(BuiltinConsumeMessageService {
        owner: Arc::downgrade(consumer),
        service: consume_service.clone(),
        orderly_lock_settings,
    }));
    consumer.consume_orderly.store(
        matches!(&consume_service, ConsumeService::Orderly(_)),
        Ordering::Release,
    );
    consumer
        .consume_service_max_threads
        .store(consume_service.maximum_threads(), Ordering::Release);
    consumer
        .consume_service_initialized
        .store(true, Ordering::Release);
    let mut client_config = consumer.config.client_config().clone();
    let consumer_group = client_config
        .with_namespace(consumer.config.consumer_group().as_deref())
        .unwrap()
        .unwrap();
    consumer
        .config
        .set_consumer_group(Some(consumer_group.clone()));
    *write_lock(&consumer.consume_stats_group) = Some(consumer_group.clone());
    consumer
        .rebalance_service
        .set_strategy(consumer.config.allocate_message_queue_strategy());
    consumer
        .rebalance_service
        .set_message_model(consumer.config.message_model());
    let runtime = manager
        .get_or_create(consumer.config.client_config())
        .await
        .unwrap();
    consumer.bind_stats_manager(runtime.consumer_stats_manager());
    let owner: Arc<dyn ConsumerRuntime> = Arc::new(PushConsumerRegistration {
        group: consumer_group.clone(),
        message_model: consumer.config.message_model(),
        consume_from_where: consumer.config.consume_from_where(),
        unit_mode: consumer.config.unit_mode(),
        subscriptions: consumer.subscriptions.clone(),
        topic_queues: consumer.topic_queues.clone(),
        pull_scheduler: pull_scheduler.clone(),
        pull_rpc_workers: pull_rpc_workers.clone(),
        offset_store: Arc::downgrade(&offset_store),
        consumer: Arc::downgrade(consumer),
        runtime: Arc::downgrade(&runtime),
    });
    let lease = runtime
        .register_consumer(&consumer_group, owner)
        .await
        .unwrap();
    *consumer.offset_store.lock().unwrap() = Some(offset_store.clone());
    *consumer.state.lock().await = State::Started {
        runtime: Arc::downgrade(&runtime),
        _lease: lease,
        offset_store,
        consume_service: consume_service.clone(),
        orderly_lock_settings,
        pull_scheduler: pull_scheduler.clone(),
        pull_rpc_workers: pull_rpc_workers.clone(),
    };
    consumer.set_service_state(Some(ConsumerServiceState::Running));
    consume_service
        .start(
            Arc::downgrade(consumer),
            orderly_lock_settings,
            orderly_lock_settings.is_some()
                && consumer.config.message_model()
                    == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering,
        )
        .await;
    pull_scheduler.start(Arc::downgrade(&runtime)).unwrap();
    pull_rpc_workers
        .start(Arc::downgrade(consumer), pull_scheduler)
        .unwrap();
    runtime
}

#[tokio::test(start_paused = true)]
async fn raw_task_entrypoints_use_the_owner_runtime_and_lifecycle() {
    let mut config = PushConsumerConfig::new("push-task-entrypoints").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();

    assert_eq!(fixture["executeTaskRuns"], 1);
    assert_eq!(fixture["executeTaskServiceCalls"], 1);
    assert_eq!(fixture["executeTaskLaterRuns"], 1);
    assert_eq!(fixture["executeTaskLaterDelayMillis"][0], 17);

    assert!(matches!(
        consumer.execute_task(async {}).await,
        Err(PushConsumerError::NotStarted)
    ));
    assert!(matches!(
        consumer.execute_task_later(async {}, 1).await,
        Err(PushConsumerError::NotStarted)
    ));

    install_started_consumer(
        &consumer,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;

    let immediate_ran = Arc::new(AtomicBool::new(false));
    let task_flag = immediate_ran.clone();
    consumer
        .execute_task(async move {
            task_flag.store(true, Ordering::Release);
        })
        .await
        .unwrap();
    for _ in 0..64 {
        if immediate_ran.load(Ordering::Acquire) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(immediate_ran.load(Ordering::Acquire));

    let delayed_ran = Arc::new(AtomicBool::new(false));
    let delayed_flag = delayed_ran.clone();
    consumer
        .execute_task_later(
            async move {
                delayed_flag.store(true, Ordering::Release);
            },
            25,
        )
        .await
        .unwrap();
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(!delayed_ran.load(Ordering::Acquire));
    tokio::time::advance(Duration::from_millis(24)).await;
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(!delayed_ran.load(Ordering::Acquire));
    tokio::time::advance(Duration::from_millis(1)).await;
    for _ in 0..64 {
        if delayed_ran.load(Ordering::Acquire) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(delayed_ran.load(Ordering::Acquire));

    let negative_delay_ran = Arc::new(AtomicBool::new(false));
    let negative_delay_flag = negative_delay_ran.clone();
    consumer
        .execute_task_later(
            async move {
                negative_delay_flag.store(true, Ordering::Release);
            },
            -1,
        )
        .await
        .unwrap();
    for _ in 0..64 {
        if negative_delay_ran.load(Ordering::Acquire) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(negative_delay_ran.load(Ordering::Acquire));

    consumer.shutdown().await.unwrap();
    assert!(matches!(
        consumer.execute_task(async {}).await,
        Err(PushConsumerError::Shutdown)
    ));
}

async fn install_started_orderly_consumer(
    consumer: &Arc<PushConsumer>,
    manager: &ClientRuntimeManager,
    offset_store: Arc<dyn OffsetStore>,
    listener: Arc<dyn MessageListenerOrderly>,
    orderly_lock_settings: Option<OrderlyLockSettings>,
) -> Arc<ClientRuntime> {
    let orderly_lock_settings = Some(orderly_lock_settings.unwrap_or_else(|| {
        OrderlyLockSettings::from_values(30_000, 20_000)
            .expect("Java default orderly lock settings are valid")
    }));
    let service = Arc::new(OrderlyConsumeService::new(
        consumer.config.consume_thread_min(),
        consumer.config.consume_thread_max(),
        listener,
    ));
    install_started_consumer_with_service(
        consumer,
        manager,
        offset_store,
        ConsumeService::Orderly(service),
        orderly_lock_settings,
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await
}

fn script_rebalance_membership(
    consumer: &PushConsumer,
    topic: &str,
    results: impl IntoIterator<Item = Option<Vec<String>>>,
) {
    consumer
        .rebalance_membership
        .lock()
        .unwrap()
        .entry(topic.to_owned())
        .or_default()
        .extend(results);
}

#[test]
fn java_string_order_and_consume_timestamp_parsing_match_utility_semantics() {
    assert_eq!(
        java_string_cmp("\u{10000}", "\u{e000}"),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        java_string_cmp("client-a", "client-b"),
        std::cmp::Ordering::Less
    );
    assert!(parse_java_timestamp_millis("20240229010203").is_some());
    assert!(parse_java_timestamp_millis("20230229010203").is_some());
    assert!(parse_java_timestamp_millis("2024022901020x").is_some());
    assert!(parse_java_timestamp_millis("+20240229010203").is_none());
}

#[test]
fn java_initial_offset_fixtures_match_policy_and_retry_topic_selection() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/rebalance/initial-offsets.json"
    ))
    .unwrap();
    let max_offset = fixture["mockMaxOffset"].as_i64().unwrap();
    let search_offset = fixture["mockSearchOffset"].as_i64().unwrap();
    let mut checked = 0;
    for case in fixture["cases"].as_array().unwrap() {
        checked += 1;
        let policy = match case["consumeFromWhere"].as_str().unwrap() {
            "CONSUME_FROM_LAST_OFFSET" => ConsumeFromWhere::ConsumeFromLastOffset,
            "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST" => {
                ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
            }
            "CONSUME_FROM_MIN_OFFSET" => ConsumeFromWhere::ConsumeFromMinOffset,
            "CONSUME_FROM_MAX_OFFSET" => ConsumeFromWhere::ConsumeFromMaxOffset,
            "CONSUME_FROM_FIRST_OFFSET" => ConsumeFromWhere::ConsumeFromFirstOffset,
            "CONSUME_FROM_TIMESTAMP" => ConsumeFromWhere::ConsumeFromTimestamp,
            other => panic!("unknown fixture consume policy {other}"),
        };
        let topic = case["topic"].as_str().unwrap();
        let stored_offset = case["storedOffset"].as_i64().unwrap();
        let stored = if stored_offset == -1 {
            OffsetRead::NotFound
        } else {
            OffsetRead::Found(stored_offset)
        };
        let plan = initial_offset_plan(
            policy,
            topic.starts_with("%RETRY%"),
            stored,
            case["consumeTimestamp"].as_str().unwrap(),
        );
        if case.get("errorCode").is_some() {
            assert!(matches!(plan, Err(PushConsumerError::InvalidOffset)));
            continue;
        }
        let plan = plan.unwrap();
        let (query, actual_offset) = match plan {
            InitialOffsetPlan::Stored(offset) => ("NONE", offset),
            InitialOffsetPlan::Fixed(offset) => ("NONE", offset),
            InitialOffsetPlan::MaxOffset => ("MAX_OFFSET", max_offset),
            InitialOffsetPlan::SearchTimestamp(_) => ("SEARCH_OFFSET", search_offset),
        };
        assert_eq!(query, case["query"].as_str().unwrap(), "{}", case["id"]);
        assert_eq!(
            actual_offset,
            case["result"].as_i64().unwrap(),
            "{}",
            case["id"]
        );
    }
    assert_eq!(checked, 24);
}

#[tokio::test]
async fn initial_offsets_prefer_store_and_never_turn_query_failures_into_zero() {
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let policies = [
        ConsumeFromWhere::ConsumeFromLastOffset,
        ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst,
        ConsumeFromWhere::ConsumeFromMinOffset,
        ConsumeFromWhere::ConsumeFromMaxOffset,
        ConsumeFromWhere::ConsumeFromFirstOffset,
        ConsumeFromWhere::ConsumeFromTimestamp,
    ];
    for policy in policies {
        let mut config = PushConsumerConfig::new("initial-offset-stored").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = PushConsumer::with_manager(config, manager.clone());
        consumer.config().set_consume_from_where(policy);
        let runtime = manager
            .get_or_create(consumer.config.client_config())
            .await
            .unwrap();
        let store = Arc::new(RecordingOffsetStore::default());
        store.seed_persisted(queue.clone(), 73);
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        assert_eq!(
            consumer
                .compute_initial_offset(&runtime, &store_dyn, &queue)
                .await
                .unwrap(),
            73,
            "stored offset must take precedence for {policy:?}"
        );
        let _ = runtime.shutdown().await;
    }

    for (policy, topic, expected) in [
        (ConsumeFromWhere::ConsumeFromFirstOffset, "orders", Some(0)),
        (
            ConsumeFromWhere::ConsumeFromLastOffset,
            "%RETRY%group",
            Some(0),
        ),
        (
            ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst,
            "%RETRY%group",
            Some(0),
        ),
        (
            ConsumeFromWhere::ConsumeFromMinOffset,
            "%RETRY%group",
            Some(0),
        ),
        (
            ConsumeFromWhere::ConsumeFromMaxOffset,
            "%RETRY%group",
            Some(0),
        ),
        (ConsumeFromWhere::ConsumeFromTimestamp, "%RETRY%group", None),
        (ConsumeFromWhere::ConsumeFromLastOffset, "orders", None),
        (ConsumeFromWhere::ConsumeFromTimestamp, "orders", None),
    ] {
        let mut config = PushConsumerConfig::new("initial-offset-missing").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = PushConsumer::with_manager(config, manager.clone());
        consumer.config().set_consume_from_where(policy);
        consumer.config().set_consume_timestamp("20240229010203");
        let runtime = manager
            .get_or_create(consumer.config.client_config())
            .await
            .unwrap();
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let queue = MessageQueue::new(topic, "broker-a", 0);
        let result = consumer
            .compute_initial_offset(&runtime, &store_dyn, &queue)
            .await;
        match expected {
            Some(expected) => assert_eq!(result.unwrap(), expected, "{policy:?} {topic}"),
            None => assert!(
                result.is_err(),
                "{policy:?} {topic} must retain the query error"
            ),
        }
        assert!(store.values.lock().unwrap().is_empty());
        let _ = runtime.shutdown().await;
    }
}

#[tokio::test]
async fn broadcasting_rebalance_activates_every_queue_with_local_offsets_and_notifies() {
    let changes = Arc::new(Mutex::new(Vec::new()));
    let recorded_changes = changes.clone();
    let queue_listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
        recorded_changes
            .lock()
            .unwrap()
            .push((topic.to_owned(), all.cloned(), assigned.clone()));
    });
    let mut config = PushConsumerConfig::new("rebalance-broadcast").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    config.set_client_rebalance(false);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    consumer
        .config()
        .set_message_queue_listener(Some(queue_listener));
    let store = Arc::new(RecordingOffsetStore::default());
    let store_dyn: Arc<dyn OffsetStore> = store.clone();
    let scheduler = Arc::new(PullScheduler::new());
    let _runtime = install_started_consumer(
        &consumer,
        &manager,
        store_dyn,
        idle_consume_service(),
        scheduler,
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    let first = MessageQueue::new("orders", "broker-a", 0);
    let second = MessageQueue::new("orders", "broker-a", 1);
    write_lock(&consumer.subscriptions).insert(
        "orders".to_owned(),
        SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
    );
    write_lock(&consumer.topic_queues).insert(
        "orders".to_owned(),
        BTreeSet::from([first.clone(), second.clone()]),
    );
    store.seed_persisted(first.clone(), 17);
    store.seed_persisted(second.clone(), 29);
    consumer.paused.store(true, Ordering::Release);

    assert!(consumer.rebalance_service.rebalance(&consumer).await);
    {
        let queues = read_lock(&consumer.process_queues);
        assert_eq!(queues.len(), 2);
        assert!(queues.contains_key(&first) && queues.contains_key(&second));
    }
    assert_eq!(store.values.lock().unwrap().get(&first), Some(&17));
    assert_eq!(store.values.lock().unwrap().get(&second), Some(&29));
    {
        let changes = changes.lock().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].0, "orders");
        assert_eq!(
            changes[0].1,
            Some(HashSet::from([first.clone(), second.clone()]))
        );
        assert_eq!(changes[0].2, HashSet::from([first, second]));
    }
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_clustering_route_notifies_empty_and_keeps_assignment_without_membership_lookup() {
    let changes = Arc::new(Mutex::new(Vec::new()));
    let recorded_changes = changes.clone();
    let queue_listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
        recorded_changes
            .lock()
            .unwrap()
            .push((topic.to_owned(), all.cloned(), assigned.clone()));
    });
    let mut config = PushConsumerConfig::new("rebalance-route-missing").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_queue_listener(Some(queue_listener));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 37));
    store.update_offset(&queue, 45, false);
    write_lock(&consumer.subscriptions).insert(
        "orders".to_owned(),
        SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
    );
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    let _runtime = install_started_consumer(
        &consumer,
        &manager,
        store.clone(),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    script_rebalance_membership(&consumer, "orders", [Some(vec!["peer-a".to_owned()])]);
    consumer.paused.store(true, Ordering::Release);

    assert!(consumer.rebalance_service.rebalance(&consumer).await);
    assert!(
        read_lock(&consumer.process_queues)
            .get(&queue)
            .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
    );
    assert!(!process_queue.snapshot(37).unwrap().dropped);
    assert_eq!(store.values.lock().unwrap().get(&queue), Some(&45));
    assert_eq!(
        consumer
            .rebalance_membership
            .lock()
            .unwrap()
            .get("orders")
            .unwrap()
            .len(),
        1,
        "the pinned Java client handles a missing route before membership lookup"
    );
    {
        let changes = changes.lock().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].0, "orders");
        assert_eq!(changes[0].1, Some(HashSet::new()));
        assert!(changes[0].2.is_empty());
    }
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_broadcasting_route_notifies_empty_and_returns_balanced() {
    let changes = Arc::new(Mutex::new(Vec::new()));
    let recorded_changes = changes.clone();
    let queue_listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
        recorded_changes
            .lock()
            .unwrap()
            .push((topic.to_owned(), all.cloned(), assigned.clone()));
    });
    let mut config = PushConsumerConfig::new("rebalance-broadcast-route-missing").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    config.set_message_queue_listener(Some(queue_listener));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    write_lock(&consumer.subscriptions).insert(
        "orders".to_owned(),
        SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
    );
    let _runtime = install_started_consumer(
        &consumer,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    consumer.paused.store(true, Ordering::Release);

    assert!(consumer.rebalance_service.rebalance(&consumer).await);
    {
        let changes = changes.lock().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].0, "orders");
        assert_eq!(changes[0].1, Some(HashSet::new()));
        assert!(changes[0].2.is_empty());
    }
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_clustering_route_notifies_empty_and_keeps_assignment_without_membership_lookup() {
    let changes = Arc::new(Mutex::new(Vec::new()));
    let recorded_changes = changes.clone();
    let queue_listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
        recorded_changes
            .lock()
            .unwrap()
            .push((topic.to_owned(), all.cloned(), assigned.clone()));
    });
    let mut config = PushConsumerConfig::new("rebalance-route-empty").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_queue_listener(Some(queue_listener));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 37));
    store.update_offset(&queue, 45, false);
    write_lock(&consumer.subscriptions).insert(
        "orders".to_owned(),
        SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
    );
    write_lock(&consumer.topic_queues).insert("orders".to_owned(), BTreeSet::new());
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    let _runtime = install_started_consumer(
        &consumer,
        &manager,
        store.clone(),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    script_rebalance_membership(&consumer, "orders", [Some(vec!["peer-a".to_owned()])]);
    consumer.paused.store(true, Ordering::Release);

    assert!(consumer.rebalance_service.rebalance(&consumer).await);
    assert!(
        read_lock(&consumer.process_queues)
            .get(&queue)
            .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
    );
    assert!(!process_queue.snapshot(37).unwrap().dropped);
    assert_eq!(store.values.lock().unwrap().get(&queue), Some(&45));
    assert_eq!(
        consumer
            .rebalance_membership
            .lock()
            .unwrap()
            .get("orders")
            .unwrap()
            .len(),
        1,
        "the pinned Java client handles an empty route before membership lookup"
    );
    {
        let changes = changes.lock().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].0, "orders");
        assert_eq!(changes[0].1, Some(HashSet::new()));
        assert!(changes[0].2.is_empty());
    }
    consumer.shutdown().await.unwrap();
}

#[test]
fn broker_assignment_partition_preserves_mixed_pull_pop_modes() {
    let pull_queue = MessageQueue::new("orders", "broker-a", 0);
    let pop_queue = MessageQueue::new("orders", "broker-a", 1);
    let default_pull_queue = MessageQueue::new("orders", "broker-a", 2);
    let make_assignment = |queue: Option<MessageQueue>, mode: Option<MessageRequestMode>| {
        let mut assignment = MessageQueueAssignment::default();
        assignment.set_message_queue(queue.map(SharedValue::new));
        assignment.set_mode(mode);
        Some(assignment)
    };
    let assignments = [
        make_assignment(Some(pull_queue.clone()), Some(MessageRequestMode::Pull)),
        make_assignment(Some(pop_queue.clone()), Some(MessageRequestMode::Pop)),
        make_assignment(Some(default_pull_queue.clone()), None),
        make_assignment(None, Some(MessageRequestMode::Pop)),
    ];

    let partition = partition_broker_assignments(assignments).unwrap();
    assert_eq!(
        partition.assigned,
        HashSet::from([
            pull_queue.clone(),
            pop_queue.clone(),
            default_pull_queue.clone()
        ])
    );
    assert_eq!(
        partition.pull,
        HashSet::from([pull_queue, default_pull_queue])
    );
    assert_eq!(partition.pop, HashSet::from([pop_queue]));
}

#[test]
fn broker_assignment_with_a_null_entry_is_not_authoritative() {
    assert!(partition_broker_assignments([None]).is_none());
}

#[test]
fn pop_to_pull_transition_uses_the_java_retry_topic_and_retains_its_route() {
    let mut config = PushConsumerConfig::new("retry-transition").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let consumer = PushConsumer::new(config);
    let retry_topic = "%RETRY%retry-transition_orders";

    consumer.subscribe_pop_retry_topic("orders", "retry-transition");
    assert!(read_lock(&consumer.subscriptions).contains_key(retry_topic));
    write_lock(&consumer.topic_queues).insert(
        retry_topic.to_owned(),
        BTreeSet::from([MessageQueue::new(retry_topic, "broker-a", 0)]),
    );

    consumer.unsubscribe_pop_retry_topic("orders", "retry-transition");
    assert!(!read_lock(&consumer.subscriptions).contains_key(retry_topic));
    assert!(read_lock(&consumer.topic_queues).contains_key(retry_topic));
}

#[test]
fn broker_assignment_modes_match_java_pop_retry_subscription_transitions() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let rows = fixture["popRetryTransitions"].as_array().unwrap();
    assert_eq!(rows.len(), 5);

    for row in rows {
        let group = row["group"].as_str().unwrap();
        let topic = row["topic"].as_str().unwrap();
        let retry_topic = row["retryTopic"].as_str().unwrap();
        let mut config = PushConsumerConfig::new(group).unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let consumer = PushConsumer::new(config);
        if row["initialSubscription"].as_bool().unwrap() {
            consumer.subscribe_pop_retry_topic(topic, group);
        }
        write_lock(&consumer.topic_queues).insert(
            retry_topic.to_owned(),
            BTreeSet::from([MessageQueue::new(retry_topic, "broker-a", 7)]),
        );

        let pull_queues = row["pullQueueIds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|queue_id| MessageQueue::new(topic, "broker-a", queue_id.as_i64().unwrap() as i32))
            .collect::<HashSet<_>>();
        let pop_queues = row["popQueueIds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|queue_id| MessageQueue::new(topic, "broker-a", queue_id.as_i64().unwrap() as i32))
            .collect::<HashSet<_>>();
        consumer.reconcile_pop_retry_topic_subscription(topic, group, &pull_queues, &pop_queues);

        assert_eq!(
            read_lock(&consumer.subscriptions).contains_key(retry_topic),
            row["subscriptionPresent"].as_bool().unwrap(),
            "{} subscription",
            row["case"].as_str().unwrap()
        );
        assert_eq!(
            read_lock(&consumer.topic_queues).contains_key(retry_topic),
            row["routePresent"].as_bool().unwrap(),
            "{} route",
            row["case"].as_str().unwrap()
        );
    }
}

#[tokio::test]
async fn lite_selective_rebalance_leaves_assignments_for_pop_handling() {
    let changes = Arc::new(Mutex::new(Vec::new()));
    let recorded_changes = changes.clone();
    let queue_listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
        recorded_changes
            .lock()
            .unwrap()
            .push((topic.to_owned(), all.cloned(), assigned.clone()));
    });
    let mut config = PushConsumerConfig::new("rebalance-lite-selective").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::LiteSelective);
    config.set_message_queue_listener(Some(queue_listener));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 41));
    store.update_offset(&queue, 52, false);
    write_lock(&consumer.subscriptions).insert(
        "orders".to_owned(),
        SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
    );
    write_lock(&consumer.topic_queues).insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    let _runtime = install_started_consumer(
        &consumer,
        &manager,
        store,
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    script_rebalance_membership(&consumer, "orders", [Some(vec!["peer-a".to_owned()])]);
    consumer.paused.store(true, Ordering::Release);

    assert!(consumer.rebalance_service.rebalance(&consumer).await);
    assert!(
        read_lock(&consumer.process_queues)
            .get(&queue)
            .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
    );
    assert_eq!(
        consumer
            .rebalance_membership
            .lock()
            .unwrap()
            .get("orders")
            .unwrap()
            .len(),
        1,
        "LiteSelective must not enter the clustering allocator"
    );
    assert!(changes.lock().unwrap().is_empty());

    consumer.config().set_client_rebalance(false);
    assert!(
        !consumer
            .rebalance_service
            .rebalance_with_order(&consumer, Some(true))
            .await
    );
    assert_eq!(
        consumer
            .rebalance_membership
            .lock()
            .unwrap()
            .get("orders")
            .unwrap()
            .len(),
        1,
        "isOrder does not turn off broker assignment for LiteSelective"
    );
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn unavailable_membership_keeps_the_previous_queue_assignment() {
    let mut config = PushConsumerConfig::new("rebalance-membership-unavailable").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 31));
    store.update_offset(&queue, 45, false);
    write_lock(&consumer.subscriptions).insert(
        "orders".to_owned(),
        SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
    );
    write_lock(&consumer.topic_queues).insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    let _runtime = install_started_consumer(
        &consumer,
        &manager,
        store.clone(),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;

    assert!(!consumer.rebalance_service.rebalance(&consumer).await);
    assert!(
        read_lock(&consumer.process_queues)
            .get(&queue)
            .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
    );
    assert!(!process_queue.snapshot(31).unwrap().dropped);
    assert_eq!(store.values.lock().unwrap().get(&queue), Some(&45));
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn client_rebalance_false_uses_broker_assignment_unless_orderly_forces_local() {
    let mut config = PushConsumerConfig::new("rebalance-client-disabled").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    consumer.config().set_client_rebalance(false);
    let store = Arc::new(RecordingOffsetStore::default());
    let queue = MessageQueue::new("orders", "broker-a", 0);
    store.seed_persisted(queue.clone(), 19);
    write_lock(&consumer.subscriptions).insert(
        "orders".to_owned(),
        SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
    );
    write_lock(&consumer.topic_queues).insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store,
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    let client_id = runtime.client_id().to_owned();
    script_rebalance_membership(
        &consumer,
        "orders",
        [Some(vec![client_id.clone()]), Some(vec![client_id])],
    );
    consumer.paused.store(true, Ordering::Release);

    assert!(!consumer.rebalance_service.rebalance(&consumer).await);
    assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
    assert_eq!(
        consumer
            .rebalance_membership
            .lock()
            .unwrap()
            .get("orders")
            .unwrap()
            .len(),
        2,
        "broker assignment mode must not request the local membership list"
    );

    assert!(
        !consumer
            .rebalance_service
            .rebalance_with_order(&consumer, Some(true))
            .await
    );
    assert_eq!(
        consumer
            .rebalance_membership
            .lock()
            .unwrap()
            .get("orders")
            .unwrap()
            .len(),
        2,
        "the doRebalance isOrder argument does not select local assignment"
    );

    consumer.config().set_client_rebalance(true);
    assert!(consumer.rebalance_service.rebalance(&consumer).await);
    let process_queue = read_lock(&consumer.process_queues)
        .get(&queue)
        .cloned()
        .expect("local assignment creates the process queue");
    assert!(process_queue.is_locked());

    consumer.config().set_client_rebalance(false);
    consumer.consume_orderly.store(true, Ordering::Release);
    assert!(consumer.rebalance_service.rebalance(&consumer).await);
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn clustering_membership_join_leave_and_unavailable_cycles_converge() {
    let mut config = PushConsumerConfig::new("rebalance-membership-churn").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let queues = (0..4)
        .map(|queue_id| MessageQueue::new("orders", "broker-a", queue_id))
        .collect::<Vec<_>>();
    write_lock(&consumer.subscriptions).insert(
        "orders".to_owned(),
        SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
    );
    write_lock(&consumer.topic_queues)
        .insert("orders".to_owned(), queues.iter().cloned().collect());
    for (index, queue) in queues.iter().enumerate() {
        store.seed_persisted(queue.clone(), 100 + index as i64);
    }
    let _runtime = install_started_consumer(
        &consumer,
        &manager,
        store.clone(),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    let runtime = match &*consumer.state.lock().await {
        State::Started { runtime, .. } => runtime.upgrade().unwrap(),
        State::Created | State::Shutdown => unreachable!(),
    };
    consumer.paused.store(true, Ordering::Release);
    let current_id = runtime.client_id().to_owned();
    let all = queues.clone();
    let expected = |ids: &[&str]| {
        let mut ids = ids.iter().map(|id| (*id).to_owned()).collect::<Vec<_>>();
        ids.sort_by(|left, right| java_string_cmp(left, right));
        consumer
            .config
            .allocate_message_queue_strategy()
            .allocate(
                consumer.config.consumer_group().as_deref().unwrap(),
                &current_id,
                &all,
                &ids,
            )
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>()
    };

    let peer = "peer-z";
    script_rebalance_membership(
        &consumer,
        "orders",
        [
            Some(vec![current_id.clone(), peer.to_owned()]),
            Some(vec![current_id.clone()]),
            Some(vec![
                "client-a".to_owned(),
                "client-b".to_owned(),
                current_id.clone(),
            ]),
            None,
            Some(Vec::new()),
            Some(vec![peer.to_owned()]),
            Some(vec![current_id.clone()]),
        ],
    );
    let expected_sets = [
        expected(&[&current_id, peer]),
        expected(&[&current_id]),
        expected(&["client-a", "client-b", &current_id]),
        expected(&["client-a", "client-b", &current_id]),
        expected(&["client-a", "client-b", &current_id]),
        HashSet::new(),
        expected(&[&current_id]),
    ];
    for (round, expected) in expected_sets.iter().enumerate() {
        let balanced = consumer.rebalance_service.rebalance(&consumer).await;
        assert!(
            balanced,
            "membership round {round} should match Java result"
        );
        let working = read_lock(&consumer.process_queues)
            .iter()
            .filter(|(queue, process_queue)| {
                queue.topic() == Some("orders")
                    && process_queue
                        .snapshot(process_queue.generation())
                        .is_ok_and(|snapshot| !snapshot.dropped)
            })
            .map(|(queue, _)| queue.clone())
            .collect::<HashSet<_>>();
        assert_eq!(&working, expected, "membership round {round}");
    }
    assert_eq!(store.persisted.lock().unwrap().len(), queues.len());
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn queue_removal_fences_a_listener_already_in_flight() {
    let mut config = PushConsumerConfig::new("rebalance-listener-race").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let store_dyn: Arc<dyn OffsetStore> = store.clone();
    let scheduler = Arc::new(PullScheduler::new());
    let consume_service = idle_consume_service();
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store_dyn.clone(),
        consume_service.clone(),
        scheduler.clone(),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    let queue = MessageQueue::new("orders", "broker-a", 0);
    store.update_offset(&queue, 50, false);
    let request = consume_request(queue.clone(), 72, vec![message(50)]);
    let process_queue = request.process_queue.clone();
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    let listener_started = Arc::new(tokio::sync::Barrier::new(2));
    let listener_release = Arc::new(tokio::sync::Barrier::new(2));
    let started = listener_started.clone();
    let release = listener_release.clone();
    let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
        move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
            let started = started.clone();
            let release = release.clone();
            async move {
                started.wait().await;
                release.wait().await;
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            }
        },
    );
    let running_consumer = consumer.clone();
    let listener_task = tokio::spawn(async move {
        running_consumer
            .run_consume_request(request, listener)
            .await;
    });
    listener_started.wait().await;
    process_queue.drop_assignment(72).unwrap();
    scheduler.cancel(&queue, 72);
    let context = PullContext {
        runtime: runtime.clone(),
        offset_store: store_dyn,
        scheduler,
        consume_service: ConsumeService::Concurrent(consume_service),
    };
    assert!(
        consumer
            .remove_process_queue_safely(&context, &queue, &process_queue)
            .await
    );
    assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
    listener_release.wait().await;
    listener_task.await.unwrap();
    assert_eq!(store.values.lock().unwrap().get(&queue), None);
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));
    consumer.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn runtime_reset_matches_java_live_maps_nulls_and_per_queue_failures() {
    struct NonPush;
    impl ConsumerRuntime for NonPush {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData::default()
        }
    }
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let cases = fixture["resetOffset"].as_array().unwrap();
    assert_eq!(cases.len(), 15);
    let retention_cases = fixture["rebalanceRetention"].as_array().unwrap();
    assert_eq!(retention_cases.len(), 3);
    for case in cases.iter().chain(retention_cases) {
        let name = case["name"].as_str().unwrap();
        let mut config = PushConsumerConfig::new(format!("reset-{name}")).unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let runtime = if case["orderly"].as_bool().unwrap() {
            install_started_orderly_consumer(
                &consumer,
                &manager,
                store_dyn,
                Arc::new(EmptyOrderlyListener),
                None,
            )
            .await
        } else {
            install_started_consumer(
                &consumer,
                &manager,
                store_dyn,
                idle_consume_service(),
                Arc::new(PullScheduler::new()),
                Arc::new(PullRpcWorkers::new()),
            )
            .await
        };
        let queue0 = MessageQueue::new("orders", "broker-a", 0);
        let queue1 = MessageQueue::new("orders", "broker-a", 1);
        if name.starts_with("subscribed-null-value-") || name == "subscribed-pull-expired" {
            write_lock(&consumer.subscriptions).insert(
                "orders".into(),
                SubscriptionData::new(Some("orders".into()), Some("*".into())),
            );
            if name.ends_with("-assigned") || name == "subscribed-pull-expired" {
                write_lock(&consumer.topic_queues)
                    .insert("orders".into(), BTreeSet::from([queue0.clone()]));
            }
        }
        let mut queues = BTreeMap::new();
        if name != "empty-push-null-inputs" {
            let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue0.clone(), 100));
            if name == "subscribed-pull-expired" {
                process_queue
                    .set_last_pull_timestamp_for_assignment(100, epoch_millis() - 120_001)
                    .unwrap();
                store
                    .fail_read_queues
                    .lock()
                    .unwrap()
                    .insert(queue0.clone());
                assert!(process_queue.is_pull_expired_with_timeout(120_000));
                assert!(read_lock(&consumer.subscriptions).contains_key("orders"));
                assert!(
                    read_lock(&consumer.topic_queues)
                        .get("orders")
                        .is_some_and(|queues| queues.contains(&queue0))
                );
                assert_eq!(
                    consumer.rebalance_service.message_model_for_test(),
                    rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting
                );
            }
            write_lock(&consumer.process_queues).insert(queue0.clone(), process_queue.clone());
            queues.insert(0, process_queue);
            store.values.lock().unwrap().insert(queue0.clone(), 50);
        }
        if name == "per-queue-failure" {
            let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue1.clone(), 101));
            write_lock(&consumer.process_queues).insert(queue1.clone(), process_queue.clone());
            queues.insert(1, process_queue);
            store.values.lock().unwrap().insert(queue1.clone(), 60);
            store
                .fail_persist_queues
                .lock()
                .unwrap()
                .insert(queue0.clone());
        }
        let owner = if name == "pull-group" {
            Arc::new(NonPush) as Arc<dyn ConsumerRuntime>
        } else {
            runtime
                .select_consumer(&format!("reset-{name}"))
                .unwrap()
                .unwrap()
        };
        runtime
            .consumer_table()
            .put("selected", owner)
            .await
            .unwrap();
        let offsets = case["offsets"].as_array().map(|rows| {
            SharedValue::new(
                rows.iter()
                    .map(|row| {
                        let queue = row["queueId"]
                            .as_i64()
                            .map(|id| MessageQueue::new("orders", "broker-a", id as i32));
                        (queue, row["offset"].as_i64())
                    })
                    .collect::<BTreeMap<_, _>>(),
            )
        });
        let reset_runtime = runtime.clone();
        let reset_offsets = offsets.clone();
        let topic = case["topic"].as_str().map(str::to_owned);
        let group = case["group"].as_str().map(str::to_owned);
        let reset = tokio::spawn(async move {
            reset_runtime
                .reset_offset(topic.as_deref(), group.as_deref(), reset_offsets)
                .await
        });
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        if name.starts_with("live-") {
            assert!(consumer.is_pause());
            assert!(queues[&0].snapshot(100).unwrap().dropped);
            if name == "live-map-update" {
                offsets.as_ref().unwrap().with_mut(|offsets| {
                    offsets.insert(Some(queue0.clone()), Some(77));
                });
            } else {
                let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue1.clone(), 101));
                write_lock(&consumer.process_queues).insert(queue1.clone(), process_queue.clone());
                queues.insert(1, process_queue);
                store.values.lock().unwrap().insert(queue1.clone(), 60);
            }
        }
        tokio::time::advance(RESET_OFFSET_MAX_WAIT).await;
        let result = reset.await.unwrap();
        if name == "subscribed-pull-expired" {
            assert!(!consumer.is_pause());
        }
        assert_eq!(
            result.is_err(),
            !case["error"].is_null(),
            "{name}: {result:?}"
        );
        let rows = queues.iter().map(|(id, process_queue)| {
            let queue = if *id == 0 { &queue0 } else { &queue1 };
            serde_json::json!({"id": id, "present": read_lock(&consumer.process_queues).contains_key(queue),
                "dropped": process_queue.snapshot(process_queue.generation()).unwrap().dropped,
                "value": store.values.lock().unwrap().get(queue).copied(),
                "persisted": store.persisted.lock().unwrap().get(queue).copied()})
        }).collect::<Vec<_>>();
        assert_eq!(serde_json::json!(rows), case["queues"], "{name}");
        assert_eq!(
            serde_json::to_value(&*store.reset_events.lock().unwrap()).unwrap(),
            case["events"],
            "{name}"
        );
        assert_eq!(
            consumer.is_pause(),
            case["pausedAfter"].as_bool().unwrap(),
            "{name}"
        );
        runtime.consumer_table().remove("selected").await.unwrap();
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn reset_store_handoff_uses_live_fields_and_calls_remove_outside_queue_lock() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let expected = &fixture["resetStoreHandoff"];
    let mut config = PushConsumerConfig::new("reset-store-handoff").unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(String::new()));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let first = Arc::new(RecordingOffsetStore::default());
    let second = Arc::new(RecordingOffsetStore::default());
    let third = Arc::new(RecordingOffsetStore::default());
    let queue = MessageQueue::new("orders", "broker-a", 0);
    for (store, offset) in [(&first, 50), (&second, 60), (&third, 70)] {
        store.values.lock().unwrap().insert(queue.clone(), offset);
    }
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        first.clone(),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 100));
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    let weak = Arc::downgrade(&consumer);
    let store = second.clone();
    *first.update_hook.lock().unwrap() = Some(Arc::new(move || {
        *weak.upgrade().unwrap().offset_store.lock().unwrap() = Some(store.clone());
    }));
    let weak = Arc::downgrade(&consumer);
    let store = third.clone();
    *second.persist_hook.lock().unwrap() = Some(Arc::new(move || {
        *weak.upgrade().unwrap().offset_store.lock().unwrap() = Some(store.clone());
    }));
    let weak = Arc::downgrade(&consumer);
    let expected_queue = queue.clone();
    *third.remove_hook.lock().unwrap() = Some(Arc::new(move || {
        let consumer = weak.upgrade().unwrap();
        let queues = consumer
            .process_queues
            .try_read()
            .expect("custom store callback must run outside queue map locks");
        assert!(queues.contains_key(&expected_queue));
    }));
    runtime
        .reset_offset(
            Some("orders"),
            Some("reset-store-handoff"),
            Some(SharedValue::new(BTreeMap::from([(
                Some(queue.clone()),
                Some(7),
            )]))),
        )
        .await
        .unwrap();
    assert!(expected["error"].is_null());
    assert_eq!(
        serde_json::json!([
            first.values.lock().unwrap().get(&queue).copied(),
            second.values.lock().unwrap().get(&queue).copied(),
            third.values.lock().unwrap().get(&queue).copied(),
        ]),
        expected["storeValues"]
    );
    assert_eq!(
        second.persisted.lock().unwrap().get(&queue).copied(),
        expected["queues"][0]["persisted"].as_i64()
    );
    assert_eq!(
        read_lock(&consumer.process_queues).contains_key(&queue),
        expected["queues"][0]["present"].as_bool().unwrap()
    );
    assert_eq!(
        process_queue.snapshot(100).unwrap().dropped,
        expected["queues"][0]["dropped"].as_bool().unwrap()
    );
    assert_eq!(first.reset_events.lock().unwrap()[&0], vec!["update:7"]);
    assert_eq!(second.reset_events.lock().unwrap()[&0], vec!["persist"]);
    assert_eq!(third.reset_events.lock().unwrap()[&0], vec!["remove"]);
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn resume_completes_each_target_round_without_rebalancing_siblings() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let manager = ClientRuntimeManager::new();
    let mut owners = Vec::new();
    let mut runtimes = Vec::new();
    let mut stores = Vec::new();
    for group in ["resume-selected", "resume-sibling"] {
        let mut config = PushConsumerConfig::new(group).unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        let owner = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        runtimes.push(
            install_started_consumer(
                &owner,
                &manager,
                store.clone(),
                idle_consume_service(),
                Arc::new(PullScheduler::new()),
                Arc::new(PullRpcWorkers::new()),
            )
            .await,
        );
        owners.push(owner);
        stores.push(store);
    }
    assert!(Arc::ptr_eq(&runtimes[0], &runtimes[1]));
    let queue = MessageQueue::new("orders", "broker-a", 0);
    write_lock(&owners[1].process_queues).insert(
        queue.clone(),
        Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 200)),
    );
    let cases = fixture["resume"].as_array().unwrap();
    assert_eq!(cases.len(), 2);
    for (round, expected) in cases.iter().enumerate() {
        let process_queue = Arc::new(ProcessQueue::new_for_assignment(
            queue.clone(),
            round as u64 + 100,
        ));
        write_lock(&owners[0].process_queues).insert(queue.clone(), process_queue.clone());
        if round == 0 {
            owners[0].suspend().await;
        }
        owners[0].resume().await;
        assert_eq!(
            serde_json::json!({
                "present": read_lock(&owners[0].process_queues).contains_key(&queue),
                "dropped": process_queue.snapshot(process_queue.generation()).unwrap().dropped,
                "paused": owners[0].is_pause(),
                "siblingPresent": read_lock(&owners[1].process_queues).contains_key(&queue),
            }),
            *expected,
            "resume round {round}"
        );
        assert!(
            stores
                .iter()
                .all(|store| store.reset_events.lock().unwrap().is_empty())
        );
    }
    for owner in owners {
        owner.shutdown().await.unwrap();
    }
    runtimes[0].shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn reset_finally_preserves_a_rebalance_callback_suspend_without_holding_state() {
    let mut config = PushConsumerConfig::new("reset-resume-callback").unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(String::new()));
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    write_lock(&consumer.subscriptions).insert(
        "orders".into(),
        SubscriptionData::new(Some("orders".into()), Some("*".into())),
    );
    let owner = Arc::downgrade(&consumer);
    consumer
        .config()
        .set_message_queue_listener(Some(Arc::new(move |_, _, _| {
            let owner = owner.upgrade().unwrap();
            assert!(
                owner.state.try_lock().is_ok(),
                "state must be available to callbacks"
            );
            owner.paused.store(true, Ordering::Release);
        })));
    consumer
        .reset_consumer_offsets_shared(None, None)
        .await
        .unwrap();
    assert!(
        consumer.is_pause(),
        "finally must retain the callback's new suspend"
    );
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn rebalance_truncates_unsubscribed_queues_without_store_side_effects() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let expected = &fixture["unsubscribedTruncation"];
    let mut config = PushConsumerConfig::new("unsubscribed-truncation").unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(String::new()));
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store.clone(),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 100));
    // Match the Java probe's nullable-reset precondition at entry to its
    // real doRebalance: dropped queue, retained offset and no subscription.
    process_queue.drop_assignment(100).unwrap();
    process_queue.clear();
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    store.values.lock().unwrap().insert(queue.clone(), 50);
    consumer.do_rebalance().await;
    assert_eq!(
        read_lock(&consumer.process_queues).contains_key(&queue),
        expected["queues"][0]["present"].as_bool().unwrap()
    );
    assert_eq!(
        process_queue.snapshot(100).unwrap().dropped,
        expected["queues"][0]["dropped"].as_bool().unwrap()
    );
    assert_eq!(
        store.values.lock().unwrap().get(&queue).copied(),
        expected["queues"][0]["value"].as_i64()
    );
    assert_eq!(
        store.persisted.lock().unwrap().get(&queue).copied(),
        expected["queues"][0]["persisted"].as_i64()
    );
    assert_eq!(
        serde_json::to_value(&*store.reset_events.lock().unwrap()).unwrap(),
        expected["events"]
    );
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn cancelled_runtime_reset_resumes_consumer_and_releases_the_monitor() {
    let mut config = PushConsumerConfig::new("cancelled-runtime-reset").unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(String::new()));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store,
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    let reset_runtime = runtime.clone();
    let reset = tokio::spawn(async move {
        reset_runtime
            .reset_offset(None, Some("cancelled-runtime-reset"), None)
            .await
    });
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(consumer.is_pause());
    reset.abort();
    assert!(reset.await.unwrap_err().is_cancelled());
    assert!(!consumer.is_pause());
    // A cancelled caller must not leave the monitor or scheduler paused.
    assert!(
        runtime
            .register_consumer_nullable(None, None)
            .await
            .unwrap()
            .is_none()
    );
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn reset_offset_fences_a_slow_callback_and_persists_only_the_new_value() {
    let mut config = PushConsumerConfig::new("reset-offset-listener-race").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let store_dyn: Arc<dyn OffsetStore> = store.clone();
    let scheduler = Arc::new(PullScheduler::new());
    let consume_service = idle_consume_service();
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store_dyn.clone(),
        consume_service.clone(),
        scheduler,
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    let queue = MessageQueue::new("orders", "broker-a", 0);
    store.update_offset(&queue, 50, false);
    let request = consume_request(queue.clone(), 73, vec![message(50)]);
    let process_queue = request.process_queue.clone();
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    let listener_started = Arc::new(tokio::sync::Barrier::new(2));
    let listener_release = Arc::new(tokio::sync::Barrier::new(2));
    let started = listener_started.clone();
    let release = listener_release.clone();
    let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
        move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
            let started = started.clone();
            let release = release.clone();
            async move {
                started.wait().await;
                release.wait().await;
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            }
        },
    );
    let running_consumer = consumer.clone();
    let listener_task = tokio::spawn(async move {
        running_consumer
            .run_consume_request(request, listener)
            .await;
    });
    listener_started.wait().await;

    let reset_consumer = consumer.clone();
    let reset_queue = queue.clone();
    let reset = tokio::spawn(async move {
        reset_consumer
            .reset_consumer_offsets("orders", vec![(reset_queue, Some(7))])
            .await
    });
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(process_queue.snapshot(73).unwrap().dropped);
    assert_eq!(store.values.lock().unwrap().get(&queue), Some(&50));
    assert_eq!(store.persisted.lock().unwrap().get(&queue), None);
    tokio::time::advance(RESET_OFFSET_MAX_WAIT).await;
    tokio::task::yield_now().await;
    reset.await.unwrap().unwrap();
    assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
    assert_eq!(store.values.lock().unwrap().get(&queue), None);
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&7));

    listener_release.wait().await;
    listener_task.await.unwrap();
    assert_eq!(store.values.lock().unwrap().get(&queue), None);
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn orderly_reset_waits_java_lock_window_then_removes_the_dropped_queue() {
    let mut config = PushConsumerConfig::new("reset-offset-orderly-lock-window").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let store_dyn: Arc<dyn OffsetStore> = store.clone();
    let runtime = install_started_orderly_consumer(
        &consumer,
        &manager,
        store_dyn,
        Arc::new(EmptyOrderlyListener),
        None,
    )
    .await;
    let queue = MessageQueue::new("orders", "broker-a", 0);
    store.update_offset(&queue, 50, false);
    store.persist(&queue).await.unwrap();
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 73));
    process_queue
        .set_lock_state(73, true, epoch_millis())
        .unwrap();
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());

    let consume_gate = process_queue.orderly_consume_gate();
    let _consume_guard = consume_gate.write_owned().await;
    let reset_consumer = consumer.clone();
    let reset_queue = queue.clone();
    let reset = tokio::spawn(async move {
        reset_consumer
            .reset_consumer_offsets("orders", vec![(reset_queue, Some(7))])
            .await
    });
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(process_queue.snapshot(73).unwrap().dropped);

    tokio::time::advance(Duration::from_millis(9_999)).await;
    tokio::task::yield_now().await;
    assert!(
        !reset.is_finished(),
        "Java waits up to 10 seconds for orderly consumption"
    );
    assert!(read_lock(&consumer.process_queues).contains_key(&queue));

    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert!(
        !reset.is_finished(),
        "Java's queue-removal path gets a further 500ms lock attempt"
    );
    tokio::time::advance(Duration::from_millis(500)).await;
    tokio::task::yield_now().await;

    assert!(reset.await.unwrap().is_ok());
    assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
    assert_eq!(store.values.lock().unwrap().get(&queue), Some(&7));
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&7));
    store.update_offset(&queue, 8, true);
    assert_eq!(store.values.lock().unwrap().get(&queue), Some(&8));
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn orderly_reset_commits_then_unlocks_and_removes_when_the_gate_is_available() {
    let mut config = PushConsumerConfig::new("reset-offset-orderly-removal").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let store_dyn: Arc<dyn OffsetStore> = store.clone();
    let runtime = install_started_orderly_consumer(
        &consumer,
        &manager,
        store_dyn,
        Arc::new(EmptyOrderlyListener),
        None,
    )
    .await;
    let queue = MessageQueue::new("orders", "broker-a", 0);
    store.update_offset(&queue, 50, false);
    store.persist(&queue).await.unwrap();
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 74));
    process_queue
        .set_lock_state(74, true, epoch_millis())
        .unwrap();
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    consumer
        .scripted_lock_transport
        .lock()
        .unwrap()
        .unlock_results
        .push_back(Ok(()));

    consumer
        .reset_consumer_offsets("orders", vec![(queue.clone(), Some(7))])
        .await
        .unwrap();

    assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
    assert_eq!(store.values.lock().unwrap().get(&queue), None);
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&7));
    assert_eq!(
        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .unlock_bodies
            .len(),
        1
    );
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn reset_offset_ignores_queues_without_an_active_process_queue() {
    let mut config = PushConsumerConfig::new("reset-offset-unassigned-queue").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let store_dyn: Arc<dyn OffsetStore> = store.clone();
    let runtime = install_started_orderly_consumer(
        &consumer,
        &manager,
        store_dyn,
        Arc::new(EmptyOrderlyListener),
        None,
    )
    .await;
    let queue = MessageQueue::new("orders", "broker-a", 1);
    store.update_offset(&queue, 50, false);
    store.persist(&queue).await.unwrap();

    consumer
        .reset_consumer_offsets("orders", vec![(queue.clone(), Some(7))])
        .await
        .unwrap();

    assert_eq!(store.values.lock().unwrap().get(&queue), Some(&50));
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn reset_offset_with_null_value_retains_offset_through_unsubscribed_truncation() {
    let mut config = PushConsumerConfig::new("reset-offset-null-value").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let store_dyn: Arc<dyn OffsetStore> = store.clone();
    let runtime = install_started_orderly_consumer(
        &consumer,
        &manager,
        store_dyn,
        Arc::new(EmptyOrderlyListener),
        None,
    )
    .await;
    let queue = MessageQueue::new("orders", "broker-a", 0);
    store.update_offset(&queue, 50, false);
    store.persist(&queue).await.unwrap();
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 73));
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());

    consumer
        .reset_consumer_offsets("orders", vec![(queue.clone(), None)])
        .await
        .unwrap();

    assert!(process_queue.snapshot(73).unwrap().dropped);
    assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
    assert_eq!(store.values.lock().unwrap().get(&queue), Some(&50));
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn reset_offset_fences_a_late_pull_response_before_persisting_the_new_value() {
    let mut config = PushConsumerConfig::new("reset-offset-pull-race").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let store_dyn: Arc<dyn OffsetStore> = store.clone();
    let scheduler = Arc::new(PullScheduler::new());
    let consume_service = idle_consume_service();
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store_dyn.clone(),
        consume_service.clone(),
        scheduler.clone(),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    let queue = MessageQueue::new("orders", "broker-a", 0);
    store.update_offset(&queue, 50, false);
    store.persist(&queue).await.unwrap();
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 73));
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    let stale_request = Arc::new(PullRequest::new(
        "reset-offset-pull-race",
        process_queue.clone(),
        50,
    ));
    let persist_started = Arc::new(tokio::sync::Barrier::new(2));
    let persist_release = Arc::new(tokio::sync::Barrier::new(2));
    store.set_persist_barriers(persist_started.clone(), persist_release.clone());

    let reset_consumer = consumer.clone();
    let reset_queue = queue.clone();
    let reset_task = tokio::spawn(async move {
        reset_consumer
            .reset_consumer_offsets("orders", vec![(reset_queue, Some(7))])
            .await
    });
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(process_queue.snapshot(73).unwrap().dropped);
    tokio::time::advance(RESET_OFFSET_MAX_WAIT).await;
    tokio::task::yield_now().await;
    persist_started.wait().await;
    assert!(process_queue.snapshot(73).unwrap().dropped);
    assert_eq!(store.values.lock().unwrap().get(&queue), Some(&7));
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));

    let mut late_result = pull_result(PullStatus::Found, 51);
    late_result.messages = Some(vec![message(50)]);
    consumer
        .apply_pull_result(
            stale_request,
            PullContext {
                runtime: runtime.clone(),
                offset_store: store_dyn,
                scheduler,
                consume_service: ConsumeService::Concurrent(consume_service),
            },
            late_result,
        )
        .await;
    assert_eq!(store.values.lock().unwrap().get(&queue), Some(&7));
    assert_eq!(
        store.updates.lock().unwrap().as_slice(),
        &[(queue.clone(), 50, false), (queue.clone(), 7, false)]
    );

    persist_release.wait().await;
    reset_task.await.unwrap().unwrap();
    assert_eq!(store.values.lock().unwrap().get(&queue), None);
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&7));
    assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn direct_consume_invokes_listener_without_advancing_regular_offsets() {
    let mut config = PushConsumerConfig::new("direct-consume-offset-isolation").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let store_dyn: Arc<dyn OffsetStore> = store.clone();
    let queue = MessageQueue::new("orders", "broker-a", 0);
    store.update_offset(&queue, 50, false);
    store.persist(&queue).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let listener: Arc<dyn MessageListenerConcurrently> = {
        let calls = calls.clone();
        Arc::new(
            move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let calls = calls.clone();
                async move {
                    assert_eq!(messages.len(), 1);
                    assert_eq!(messages[0].topic(), "orders");
                    assert_eq!(context.message_queue().topic(), Some("orders"));
                    assert_eq!(context.message_queue().broker_name(), Some("broker-a"));
                    calls.fetch_add(1, Ordering::AcqRel);
                    (Some(ConsumeConcurrentlyStatus::ReconsumeLater), context)
                }
            },
        )
    };
    let consume_service = Arc::new(ConcurrentConsumeService::new(1, 1, listener));
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store_dyn,
        consume_service.clone(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;

    let result = consumer
        .consume_message_directly(message(812), Some("broker-a".into()))
        .await
        .unwrap();

    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(
        result.get_consume_result(),
        Some(ConsumeMessageResult::CrLater)
    );
    assert_eq!(store.values.lock().unwrap().get(&queue), Some(&50));
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));
    assert!(!read_lock(&consumer.process_queues).contains_key(&queue));

    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn independent_runtime_owners_with_same_client_id_isolate_consumer_stats() {
    let mut first_config = PushConsumerConfig::new("stats-isolation-group").unwrap();
    first_config.client_config_mut().set_namesrv_addr(None);
    first_config
        .client_config_mut()
        .set_client_ip(Some("198.51.100.41".into()));
    first_config
        .client_config_mut()
        .set_instance_name(Some("stats-isolation-instance".into()));
    let mut second_config = PushConsumerConfig::new("stats-isolation-group").unwrap();
    second_config.client_config_mut().set_namesrv_addr(None);
    second_config
        .client_config_mut()
        .set_client_ip(Some("198.51.100.41".into()));
    second_config
        .client_config_mut()
        .set_instance_name(Some("stats-isolation-instance".into()));

    let first = PushConsumer::with_manager(first_config, ClientRuntimeManager::new());
    let second = PushConsumer::with_manager(second_config, ClientRuntimeManager::new());
    let first_runtime = first
        .manager
        .get_or_create(first.config.client_config())
        .await
        .unwrap();
    let second_runtime = second
        .manager
        .get_or_create(second.config.client_config())
        .await
        .unwrap();
    assert_eq!(first_runtime.client_id(), second_runtime.client_id());
    first.bind_stats_manager(first_runtime.consumer_stats_manager());
    second.bind_stats_manager(second_runtime.consumer_stats_manager());
    let first_stats = first.get_consumer_stats_manager().unwrap();
    let second_stats = second.get_consumer_stats_manager().unwrap();
    assert!(!Arc::ptr_eq(&first_stats, &second_stats));
    let java_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    assert_eq!(
        java_fixture["consumerStatsManagersAreInstanceScoped"].as_bool(),
        Some(true)
    );

    first_stats.inc_pull_rt("stats-isolation-group", "stats-isolation-topic", 3);
    first_stats.inc_pull_tps("stats-isolation-group", "stats-isolation-topic", 4);

    let second_status =
        second_stats.consume_status("stats-isolation-group", "stats-isolation-topic");
    assert_eq!(second_status.get_pull_tps(), 0.0);

    first_runtime.shutdown().await.unwrap();
    second_runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn push_consumers_sharing_one_runtime_share_consumer_stats_manager_identity() {
    let client_ip = Some("198.51.100.42".to_owned());
    let instance_name = Some("shared-stats-runtime".to_owned());
    let mut first_config = PushConsumerConfig::new("shared-stats-group-a").unwrap();
    first_config.client_config_mut().set_namesrv_addr(None);
    first_config
        .client_config_mut()
        .set_client_ip(client_ip.clone());
    first_config
        .client_config_mut()
        .set_instance_name(instance_name.clone());
    let mut second_config = PushConsumerConfig::new("shared-stats-group-b").unwrap();
    second_config.client_config_mut().set_namesrv_addr(None);
    second_config.client_config_mut().set_client_ip(client_ip);
    second_config
        .client_config_mut()
        .set_instance_name(instance_name);

    let manager = ClientRuntimeManager::new();
    let first = Arc::new(PushConsumer::with_manager(first_config, manager.clone()));
    let second = Arc::new(PushConsumer::with_manager(second_config, manager.clone()));
    assert!(first.get_consumer_stats_manager().is_none());
    assert!(second.get_consumer_stats_manager().is_none());
    let first_runtime = install_started_consumer(
        &first,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    let second_runtime = install_started_consumer(
        &second,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    assert!(Arc::ptr_eq(&first_runtime, &second_runtime));
    let first_stats = first.get_consumer_stats_manager().unwrap();
    let second_stats = second.get_consumer_stats_manager().unwrap();
    assert!(Arc::ptr_eq(&first_stats, &second_stats));

    first_stats.inc_pull_rt("shared-stats-group-a", "orders", 7);
    first_stats.inc_pull_tps("shared-stats-group-a", "orders", 3);
    assert_eq!(
        second_stats
            .consume_status("shared-stats-group-b", "orders")
            .get_pull_tps(),
        0.0
    );

    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
    first_runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_consume_stats_keep_the_start_group_after_facade_mutation() {
    let mut config = PushConsumerConfig::new("consume-stats-snapshot-group").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store,
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["consumeServiceGroupBeforeMutation"],
        "phase18-consume-service-group"
    );
    assert_eq!(
        fixture["consumeServiceFacadeGroupAfterMutation"],
        "phase18-consume-service-group-mutated"
    );
    for key in [
        "concurrentConsumeServiceGroup",
        "orderlyConsumeServiceGroup",
        "popConcurrentConsumeServiceGroup",
        "popOrderlyConsumeServiceGroup",
    ] {
        assert_eq!(fixture[key], fixture["consumeServiceGroupBeforeMutation"]);
    }

    consumer.set_consumer_group(Some("consume-stats-live-group".into()));
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let request = consume_request(queue, 191, vec![message(1)]);
    let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
        |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    );
    consumer
        .clone()
        .run_consume_request(request, listener)
        .await;
    tokio::time::sleep(Duration::from_secs(11)).await;

    assert!(
        consumer
            .stats_manager()
            .unwrap()
            .consume_status("consume-stats-snapshot-group", "orders")
            .get_consume_ok_tps()
            > 0.0
    );
    assert_eq!(
        consumer
            .stats_manager()
            .unwrap()
            .consume_status("consume-stats-live-group", "orders")
            .get_consume_ok_tps(),
        0.0
    );
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_consume_retry_topics_match_java_after_facade_group_mutation() {
    let group = "phase18-consume-retry-snapshot";
    let live_group = "phase18-consume-retry-live";
    let mut config = PushConsumerConfig::new(group).unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store,
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    consumer.set_consumer_group(Some(live_group.to_owned()));
    let hook_before_groups = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hook_after_groups = Arc::new(std::sync::Mutex::new(Vec::new()));
    consumer.register_consume_message_hook(Arc::new(ConsumerGroupHookRecorder {
        before: hook_before_groups.clone(),
        after: hook_after_groups.clone(),
    }));

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    let received_topics = Arc::new(std::sync::Mutex::new(Vec::new()));
    let listener: Arc<dyn MessageListenerConcurrently> = {
        let received_topics = received_topics.clone();
        Arc::new(
            move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let received_topics = received_topics.clone();
                async move {
                    *received_topics.lock().unwrap() = messages
                        .iter()
                        .map(|message| message.topic().to_owned())
                        .collect();
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        )
    };
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let request = consume_request(
        queue,
        193,
        vec![
            retry_topic_message(
                "%RETRY%tenant%phase18-consume-retry-snapshot_orders",
                None,
                1,
            ),
            retry_topic_message("%RETRY%phase18-consume-retry-live", Some("tenant%retry"), 2),
            retry_topic_message("tenant%events", None, 3),
        ],
    );
    consumer
        .clone()
        .run_consume_request(request, listener)
        .await;

    assert_eq!(
        fixture["concurrentConsumeCallbackTopics"],
        serde_json::json!(*received_topics.lock().unwrap())
    );
    assert_eq!(
        fixture["consumeHookBeforeGroups"][0],
        serde_json::json!(hook_before_groups.lock().unwrap()[0])
    );
    assert_eq!(
        fixture["consumeHookAfterGroups"][0],
        serde_json::json!(hook_after_groups.lock().unwrap()[0])
    );
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn direct_consume_retry_topics_use_the_service_group_snapshot() {
    let group = "phase18-consume-retry-snapshot";
    let mut config = PushConsumerConfig::new(group).unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let received_topics = Arc::new(std::sync::Mutex::new(Vec::new()));
    let listener: Arc<dyn MessageListenerConcurrently> = {
        let received_topics = received_topics.clone();
        Arc::new(
            move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let received_topics = received_topics.clone();
                async move {
                    *received_topics.lock().unwrap() = messages
                        .iter()
                        .map(|message| message.topic().to_owned())
                        .collect();
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        )
    };
    let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store,
        Arc::new(ConcurrentConsumeService::new(1, 1, listener)),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    consumer.set_consumer_group(Some("phase18-consume-retry-live".into()));

    let result = consumer
        .consume_message_directly(
            retry_topic_message(
                "%RETRY%tenant%phase18-consume-retry-snapshot",
                Some("tenant%direct-concurrent"),
                4,
            ),
            Some("broker-a".to_owned()),
        )
        .await
        .unwrap();
    assert_eq!(
        result.get_consume_result(),
        Some(ConsumeMessageResult::CrSuccess)
    );

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["concurrentConsumeDirectTopics"],
        serde_json::json!(*received_topics.lock().unwrap())
    );
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn orderly_consume_retry_topics_match_java_snapshot_and_live_group_paths() {
    let group = "phase18-consume-retry-snapshot";
    let live_group = "phase18-consume-retry-live";
    let mut config = PushConsumerConfig::new(group).unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let received_topics = Arc::new(std::sync::Mutex::new(Vec::new()));
    let listener: Arc<dyn MessageListenerOrderly> = {
        let received_topics = received_topics.clone();
        Arc::new(
            move |messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                let received_topics = received_topics.clone();
                async move {
                    received_topics.lock().unwrap().push(
                        messages
                            .iter()
                            .map(|message| message.topic().to_owned())
                            .collect::<Vec<_>>(),
                    );
                    (Some(ConsumeOrderlyStatus::Success), context)
                }
            },
        )
    };
    let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let runtime =
        install_started_orderly_consumer(&consumer, &manager, store, listener.clone(), None).await;
    consumer.set_consumer_group(Some(live_group.to_owned()));
    let hook_before_groups = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hook_after_groups = Arc::new(std::sync::Mutex::new(Vec::new()));
    consumer.register_consume_message_hook(Arc::new(ConsumerGroupHookRecorder {
        before: hook_before_groups.clone(),
        after: hook_after_groups.clone(),
    }));

    let direct_result = consumer
        .consume_message_directly(
            retry_topic_message(
                "%RETRY%tenant%phase18-consume-retry-snapshot",
                Some("tenant%direct-orderly"),
                5,
            ),
            Some("broker-a".to_owned()),
        )
        .await
        .unwrap();
    assert_eq!(
        direct_result.get_consume_result(),
        Some(ConsumeMessageResult::CrSuccess)
    );

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue, 194));
    process_queue
        .insert_messages(
            194,
            vec![retry_topic_message(
                "%RETRY%phase18-consume-retry-live",
                Some("tenant%orderly-live"),
                6,
            )],
        )
        .unwrap();
    consumer
        .clone()
        .run_orderly_consume_request(
            OrderlyConsumeRequest {
                process_queue,
                generation: 194,
            },
            listener,
        )
        .await;

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    {
        let received_topics = received_topics.lock().unwrap();
        assert_eq!(
            fixture["orderlyConsumeDirectTopics"],
            serde_json::json!(received_topics[0])
        );
        assert_eq!(
            fixture["orderlyConsumeCallbackTopics"],
            serde_json::json!(received_topics[1])
        );
    }
    assert_eq!(
        fixture["consumeHookBeforeGroups"][1],
        serde_json::json!(hook_before_groups.lock().unwrap()[0])
    );
    assert_eq!(
        fixture["consumeHookAfterGroups"][1],
        serde_json::json!(hook_after_groups.lock().unwrap()[0])
    );
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn orderly_consume_stats_keep_the_start_group_after_facade_mutation() {
    let mut config = PushConsumerConfig::new("orderly-stats-snapshot-group").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
        |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
            (Some(ConsumeOrderlyStatus::Success), context)
        },
    );
    let runtime =
        install_started_orderly_consumer(&consumer, &manager, store, listener.clone(), None).await;

    consumer.set_consumer_group(Some("orderly-stats-live-group".into()));
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue, 192));
    process_queue
        .insert_messages(192, vec![message(1)])
        .unwrap();
    consumer
        .clone()
        .run_orderly_consume_request(
            OrderlyConsumeRequest {
                process_queue,
                generation: 192,
            },
            listener,
        )
        .await;
    tokio::time::sleep(Duration::from_secs(11)).await;

    assert!(
        consumer
            .stats_manager()
            .unwrap()
            .consume_status("orderly-stats-snapshot-group", "orders")
            .get_consume_ok_tps()
            > 0.0
    );
    assert_eq!(
        consumer
            .stats_manager()
            .unwrap()
            .consume_status("orderly-stats-live-group", "orders")
            .get_consume_ok_tps(),
        0.0
    );
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn running_info_returns_queue_snapshot_and_java_config_properties() {
    let mut config = PushConsumerConfig::new("running-info-snapshot").unwrap();
    config.set_pull_threshold_for_queue(777);
    config.set_pull_threshold_size_for_queue_mib(77);
    config.set_pull_threshold_for_topic(20);
    config.set_pull_threshold_size_for_topic_mib(12);
    config.client_config_mut().set_namesrv_addr(None);
    config.client_config_mut().set_socks_proxy_config(Some(
        r#"{"10.1.0.0/16":{"addr":"proxy.internal:1080","username":"reader","password":"secret"}}"#
            .into(),
    ));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let raw_running_info_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    consumer.subscribe("orders", "blue").await.unwrap();
    consumer.subscribe("empty", "*").await.unwrap();
    let before_start_running_info = consumer.consumer_running_info().await;
    assert!(
        raw_running_info_fixture["runningInfoBeforeStartThrowsNullPointerException"]
            .as_bool()
            .unwrap()
    );
    assert!(matches!(
        before_start_running_info,
        Err(PushConsumerError::NotStarted)
    ));
    let store = Arc::new(RecordingOffsetStore::default());
    let store_dyn: Arc<dyn OffsetStore> = store.clone();
    let queue = MessageQueue::new("orders", "broker-a", 0);
    store.update_offset(&queue, 17, false);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 19));
    process_queue
        .insert_messages(19, vec![message(11)])
        .unwrap();
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue);
    consumer.update_pull_thresholds_after_rebalance();

    let pop_queue = MessageQueue::new("orders", "broker-a", 2);
    let pop_process_queue = Arc::new(PopProcessQueue::new_for_assignment(pop_queue.clone(), 23));
    pop_process_queue.inc_found_messages(3);
    pop_process_queue.set_last_pop_timestamp(1_700_000_000_005);
    write_lock(&consumer.pop_process_queues).insert(pop_queue.clone(), pop_process_queue);

    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store_dyn.clone(),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    consumer
        .consumer_start_timestamp
        .store(1_700_000_000_000, Ordering::Release);
    let stats = consumer.stats_manager().unwrap();
    stats.inc_pull_rt("running-info-snapshot", "orders", 500);
    stats.inc_pull_tps("running-info-snapshot", "orders", 2);
    stats.inc_consume_rt("running-info-snapshot", "orders", 8);
    stats.inc_consume_ok_tps("running-info-snapshot", "orders", 3);
    stats.inc_consume_failed_tps("running-info-snapshot", "orders", 1);
    tokio::time::sleep(Duration::from_secs(11)).await;

    consumer.consume_core_pool_size.store(7, Ordering::Release);
    let raw_running_info = consumer.consumer_running_info().await.unwrap();
    assert_eq!(
        raw_running_info
            .get_subscription_set()
            .unwrap()
            .with(Vec::len),
        raw_running_info_fixture["rawRunningInfoSubscriptionCount"]
            .as_u64()
            .unwrap() as usize
    );
    assert_eq!(
        raw_running_info.get_mq_table().unwrap().with(Vec::len),
        raw_running_info_fixture["rawRunningInfoQueueCount"]
            .as_u64()
            .unwrap() as usize
    );
    assert_eq!(
        raw_running_info.get_mq_pop_table().unwrap().with(Vec::len),
        raw_running_info_fixture["rawRunningInfoPopQueueCount"]
            .as_u64()
            .unwrap() as usize
    );
    assert_eq!(
        raw_running_info
            .get_status_table()
            .unwrap()
            .with(BTreeMap::len),
        raw_running_info_fixture["rawRunningInfoStatusCount"]
            .as_u64()
            .unwrap() as usize
    );
    let properties = raw_running_info.get_properties().unwrap().snapshot();
    assert_eq!(
        properties["PROP_THREADPOOL_CORE_SIZE"],
        raw_running_info_fixture["rawRunningInfoCorePoolSize"]
            .as_str()
            .unwrap()
    );
    assert_eq!(
        properties[ConsumerRunningInfoBody::PROP_CONSUME_ORDERLY],
        "false"
    );
    assert!(!properties.contains_key("PROP_CONSUME_ORDERLY"));

    let processor = crate::admin::ConsumerRunningInfoProcessor::for_push(
        Arc::downgrade(&consumer),
        Arc::downgrade(&store_dyn),
        Arc::downgrade(&runtime),
    );
    let response = processor
        .process(RemotingCommand {
            ext_fields: Some(
                BTreeMap::from([
                    ("consumerGroup".into(), "running-info-snapshot".into()),
                    (
                        "clientId".into(),
                        "client-id-is-not-used-for-filtering".into(),
                    ),
                    ("jstackEnable".into(), "false".into()),
                ])
                .into(),
            ),
            ..Default::default()
        })
        .unwrap()
        .unwrap();
    assert_eq!(response.code, response::SUCCESS);
    let bytes = response.body.unwrap();
    let json =
        ConsumerRunningInfoBody::decode_java_json(std::str::from_utf8(&bytes).unwrap()).unwrap();
    assert!(json.get("jstack").is_none());
    let properties = json["properties"].as_object().unwrap();
    assert_eq!(properties["PROP_CONSUME_TYPE"], "CONSUME_PASSIVELY");
    assert_eq!(properties["PROP_CONSUMER_START_TIMESTAMP"], "1700000000000");
    assert_eq!(
        properties["pullThresholdForQueue"],
        raw_running_info_fixture["pushQueueThresholdSingleAssignment"][0]
            .as_i64()
            .unwrap()
            .to_string()
    );
    assert_eq!(
        properties["pullThresholdSizeForQueue"],
        raw_running_info_fixture["pushQueueThresholdSingleAssignment"][1]
            .as_i64()
            .unwrap()
            .to_string()
    );
    assert_eq!(
        properties["socksProxyConfig"],
        r#"{"10.1.0.0/16":{"addr":"proxy.internal:1080","username":"reader","password":"secret"}}"#
    );
    assert!(properties.get("tlsConfig").is_none());
    assert_eq!(json["subscriptionSet"].as_array().unwrap().len(), 2);
    assert_eq!(json["statusTable"]["orders"]["pullRT"], 500.0);
    assert!(json["statusTable"]["orders"]["pullTPS"].as_f64().unwrap() > 0.0);
    assert_eq!(json["statusTable"]["orders"]["consumeRT"], 8.0);
    assert!(
        json["statusTable"]["orders"]["consumeOKTPS"]
            .as_f64()
            .unwrap()
            > 0.0
    );
    assert!(
        json["statusTable"]["orders"]["consumeFailedTPS"]
            .as_f64()
            .unwrap()
            > 0.0
    );
    assert_eq!(json["statusTable"]["orders"]["consumeFailedMsgs"], 0);
    let encoded_queue = r#"{"brokerName":"broker-a","queueId":0,"topic":"orders"}"#;
    let encoded_pop_queue = r#"{"brokerName":"broker-a","queueId":2,"topic":"orders"}"#;
    assert_eq!(
        json["mqTable"][encoded_queue]["commitOffset"],
        serde_json::Value::from(17)
    );
    assert_eq!(json["mqTable"][encoded_queue]["cachedMsgMinOffset"], 11);
    assert_eq!(json["mqTable"][encoded_queue]["cachedMsgMaxOffset"], 11);
    assert_eq!(json["mqTable"][encoded_queue]["cachedMsgCount"], 1);
    assert_eq!(json["mqPopTable"][encoded_pop_queue]["waitAckCount"], 3);
    assert_eq!(
        json["mqPopTable"][encoded_pop_queue]["lastPopTimestamp"],
        1_700_000_000_005i64
    );
    assert!(
        std::str::from_utf8(&bytes)
            .unwrap()
            .contains("socksProxyConfig")
    );

    let response = processor
        .process(RemotingCommand {
            ext_fields: Some(
                BTreeMap::from([
                    ("consumerGroup".into(), "running-info-snapshot".into()),
                    ("clientId".into(), "ignored-client-id".into()),
                    ("jstackEnable".into(), "true".into()),
                ])
                .into(),
            ),
            ..Default::default()
        })
        .unwrap()
        .unwrap();
    let body = ConsumerRunningInfoBody::decode_java_json(
        std::str::from_utf8(response.body.as_deref().unwrap()).unwrap(),
    )
    .unwrap();
    assert!(
        body["jstack"]
            .as_str()
            .is_some_and(|stack| !stack.is_empty())
    );
    assert!(
        body["jstack"]
            .as_str()
            .is_some_and(|stack| stack.contains("running_info_returns_queue_snapshot"))
    );

    consumer.set_consumer_group(None);
    let null_group_running_info =
        consumer.running_info_body(&runtime, None, Some(store_dyn.clone()));
    assert!(
        !null_group_running_info
            .get_properties()
            .unwrap()
            .snapshot()
            .contains_key("consumerGroup")
    );
    consumer.set_consumer_group(Some("running-info-snapshot".into()));

    consumer.shutdown().await.unwrap();
    let shutdown_running_info = consumer.consumer_running_info().await.unwrap();
    assert_eq!(
        shutdown_running_info.get_mq_table().unwrap().with(Vec::len),
        raw_running_info_fixture["rawRunningInfoAfterShutdownQueueCount"]
            .as_u64()
            .unwrap() as usize
    );
    assert_eq!(
        shutdown_running_info
            .get_mq_pop_table()
            .unwrap()
            .with(Vec::len),
        raw_running_info_fixture["rawRunningInfoAfterShutdownPopQueueCount"]
            .as_u64()
            .unwrap() as usize
    );
    assert_eq!(
        shutdown_running_info
            .get_status_table()
            .unwrap()
            .with(BTreeMap::len),
        raw_running_info_fixture["rawRunningInfoAfterShutdownStatusCount"]
            .as_u64()
            .unwrap() as usize
    );
    assert_eq!(
        shutdown_running_info.get_properties().unwrap().snapshot()["PROP_THREADPOOL_CORE_SIZE"],
        raw_running_info_fixture["rawRunningInfoAfterShutdownCorePoolSize"]
            .as_str()
            .unwrap()
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn consumer_status_stays_responsive_during_a_slow_direct_consume_callback() {
    let mut config = PushConsumerConfig::new("status-during-direct-consume").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let store_dyn: Arc<dyn OffsetStore> = store.clone();
    let queue = MessageQueue::new("orders", "broker-a", 0);
    store.update_offset(&queue, 42, false);
    let listener_started = Arc::new(tokio::sync::Barrier::new(2));
    let listener_release = Arc::new(tokio::sync::Barrier::new(2));
    let started = listener_started.clone();
    let release = listener_release.clone();
    let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
        move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
            let started = started.clone();
            let release = release.clone();
            async move {
                started.wait().await;
                release.wait().await;
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            }
        },
    );
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store_dyn.clone(),
        Arc::new(ConcurrentConsumeService::new(1, 1, listener)),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;

    let direct_consume = {
        let consumer = consumer.clone();
        tokio::spawn(async move {
            consumer
                .consume_message_directly(message(99), Some("broker-a".into()))
                .await
                .unwrap()
        })
    };
    listener_started.wait().await;
    let status_processor = crate::admin::ConsumerStatusProcessor::new(Arc::downgrade(&store_dyn));
    let response = tokio::time::timeout(Duration::from_secs(1), async {
        status_processor.process(RemotingCommand {
            ext_fields: Some(
                BTreeMap::from([
                    ("topic".into(), "orders".into()),
                    ("group".into(), "status-during-direct-consume".into()),
                ])
                .into(),
            ),
            ..Default::default()
        })
    })
    .await
    .expect("status snapshot must not wait for a consumer callback")
    .unwrap()
    .unwrap();
    assert_eq!(response.code, response::SUCCESS);
    let body = GetConsumerStatusBody::decode_java_json(
        std::str::from_utf8(response.body.as_deref().unwrap()).unwrap(),
    )
    .unwrap();
    body.get_message_queue_table()
        .unwrap()
        .with(|entries| assert_eq!(entries, &vec![(queue.clone(), Some(42))]));

    listener_release.wait().await;
    assert_eq!(
        direct_consume.await.unwrap().get_consume_result(),
        Some(ConsumeMessageResult::CrSuccess)
    );
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn queue_removal_fences_a_pull_response_already_in_flight() {
    let mut config = PushConsumerConfig::new("rebalance-pull-race").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let store_dyn: Arc<dyn OffsetStore> = store.clone();
    let scheduler = Arc::new(PullScheduler::new());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store_dyn.clone(),
        idle_consume_service(),
        scheduler.clone(),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 73));
    store.update_offset(&queue, 50, false);
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    let request = Arc::new(PullRequest::new(
        "rebalance-pull-race",
        process_queue.clone(),
        50,
    ));
    scheduler.schedule(request.clone(), 0).unwrap();
    let active_request = scheduler.take_due().unwrap();
    let response_ready = Arc::new(tokio::sync::Barrier::new(2));
    let response_release = Arc::new(tokio::sync::Barrier::new(2));
    let task_ready = response_ready.clone();
    let task_release = response_release.clone();
    let pull_consumer = consumer.clone();
    let pull_store = store_dyn.clone();
    let pull_scheduler = scheduler.clone();
    let pull_runtime = runtime.clone();
    let updates_before = store.updates.lock().unwrap().len();
    let pull_task = tokio::spawn(async move {
        task_ready.wait().await;
        task_release.wait().await;
        let context = PullContext {
            runtime: pull_runtime,
            offset_store: pull_store,
            scheduler: pull_scheduler,
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
        };
        let mut result = pull_result(PullStatus::Found, 51);
        result.messages = Some(vec![message(50)]);
        pull_consumer
            .apply_pull_result(request, context, result)
            .await;
    });
    response_ready.wait().await;

    process_queue.drop_assignment(73).unwrap();
    scheduler.cancel(&queue, 73);
    let removal_context = PullContext {
        runtime,
        offset_store: store_dyn,
        scheduler: scheduler.clone(),
        consume_service: ConsumeService::Concurrent(idle_consume_service()),
    };
    assert!(
        consumer
            .remove_process_queue_safely(&removal_context, &queue, &process_queue)
            .await
    );
    response_release.wait().await;
    pull_task.await.unwrap();
    scheduler.finish(&active_request);

    assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
    assert_eq!(process_queue.snapshot(73).unwrap().cached_count, 0);
    assert_eq!(store.updates.lock().unwrap().len(), updates_before);
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_offset_persist_retains_the_dropped_generation_for_retry() {
    let mut config = PushConsumerConfig::new("rebalance-remove-retry").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let scheduler = Arc::new(PullScheduler::new());
    let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store.clone(),
        idle_consume_service(),
        scheduler.clone(),
        pull_rpc_workers,
    )
    .await;
    let queue = MessageQueue::new("orders", "broker-a", 2);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 44));
    process_queue.drop_assignment(44).unwrap();
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    store.update_offset(&queue, 81, false);
    let context = PullContext {
        runtime: runtime.clone(),
        offset_store: store.clone(),
        scheduler,
        consume_service: ConsumeService::Concurrent(idle_consume_service()),
    };

    store.set_persist_failure(true);
    assert!(
        !consumer
            .remove_process_queue_safely(&context, &queue, &process_queue)
            .await
    );
    assert!(
        read_lock(&consumer.process_queues)
            .get(&queue)
            .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
    );
    assert!(process_queue.snapshot(44).unwrap().dropped);
    assert_eq!(store.values.lock().unwrap().get(&queue), Some(&81));

    store.set_persist_failure(false);
    assert!(
        consumer
            .remove_process_queue_safely(&context, &queue, &process_queue)
            .await
    );
    assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&81));
    assert!(!store.values.lock().unwrap().contains_key(&queue));
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn activation_fences_old_generations_and_rolls_back_scheduler_failure() {
    let mut config = PushConsumerConfig::new("rebalance-activation").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let scheduler = Arc::new(PullScheduler::new());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store.clone(),
        idle_consume_service(),
        scheduler.clone(),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    write_lock(&consumer.subscriptions).insert(
        "orders".to_owned(),
        SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
    );
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let original = consumer.activate_process_queue(&queue, 10).await.unwrap();
    assert!(Arc::ptr_eq(
        &original,
        &consumer.activate_process_queue(&queue, 999).await.unwrap()
    ));
    original.drop_assignment(original.generation()).unwrap();
    assert!(matches!(
        consumer.activate_process_queue(&queue, 20).await,
        Err(PushConsumerError::ProcessQueueRemovalPending(_))
    ));
    assert!(
        read_lock(&consumer.process_queues)
            .get(&queue)
            .is_some_and(|current| Arc::ptr_eq(current, &original))
    );

    let context = PullContext {
        runtime: runtime.clone(),
        offset_store: store.clone(),
        scheduler: scheduler.clone(),
        consume_service: ConsumeService::Concurrent(idle_consume_service()),
    };
    assert!(
        consumer
            .remove_process_queue_safely(&context, &queue, &original)
            .await
    );
    let replacement = consumer.activate_process_queue(&queue, 20).await.unwrap();
    assert!(replacement.generation() > original.generation());

    let failed_queue = MessageQueue::new("orders", "broker-a", 1);
    scheduler.stop().await;
    assert!(matches!(
        consumer.activate_process_queue(&failed_queue, 30).await,
        Err(PushConsumerError::PullScheduler(_))
    ));
    assert!(!read_lock(&consumer.process_queues).contains_key(&failed_queue));
    assert!(!store.values.lock().unwrap().contains_key(&failed_queue));
    consumer.shutdown().await.unwrap();
}

#[test]
fn broker_flow_control_uses_the_java_retry_delay() {
    let consumer = PushConsumer::new(PushConsumerConfig::new("pull-delay-config").unwrap());
    let flow_control =
        consumer.pull_retry_error_from_rpc(rocketmq_classic_core::error::RpcError::Response {
            request_code: 11,
            peer: "broker-a".to_owned(),
            code: response::FLOW_CONTROL,
            remark: None,
        });
    assert_eq!(
        flow_control.delay_millis,
        PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL
    );

    let failure = consumer.pull_retry_error_from_rpc(
        rocketmq_classic_core::error::RpcError::Unsupported("test failure"),
    );
    assert_eq!(failure.delay_millis, 1_000);
}

#[tokio::test(start_paused = true)]
async fn push_pull_outcomes_insert_found_messages_and_correct_only_drained_offsets() {
    let mut config = PushConsumerConfig::new("pull-outcomes").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let scheduler = Arc::new(PullScheduler::new());
    let consume_service = idle_consume_service();
    let context = || PullContext {
        runtime: runtime.clone(),
        offset_store: offset_store.clone(),
        scheduler: scheduler.clone(),
        consume_service: ConsumeService::Concurrent(consume_service.clone()),
    };

    let empty_queue = MessageQueue::new("orders", "broker-a", 0);
    let empty_process_queue = Arc::new(ProcessQueue::new_for_assignment(empty_queue.clone(), 1));
    let empty_request = Arc::new(PullRequest::new("pull-outcomes", empty_process_queue, 10));
    scheduler.schedule(empty_request.clone(), 0).unwrap();
    let active_empty_request = scheduler.take_due().unwrap();
    consumer
        .apply_pull_result(
            empty_request.clone(),
            context(),
            pull_result(PullStatus::NoNewMessage, 20),
        )
        .await;
    scheduler.finish(&active_empty_request);
    assert_eq!(
        offset_store.updates.lock().unwrap().as_slice(),
        &[(empty_queue.clone(), 20, true)]
    );
    let follow_up = scheduler.take_due().unwrap();
    scheduler.finish(&follow_up);

    let unresolved_queue = MessageQueue::new("orders", "broker-a", 1);
    let unresolved_process_queue = Arc::new(ProcessQueue::new_for_assignment(
        unresolved_queue.clone(),
        2,
    ));
    unresolved_process_queue
        .insert_messages(2, vec![message(30)])
        .unwrap();
    let unresolved_request = Arc::new(PullRequest::new(
        "pull-outcomes",
        unresolved_process_queue.clone(),
        30,
    ));
    scheduler.schedule(unresolved_request.clone(), 0).unwrap();
    let active_unresolved_request = scheduler.take_due().unwrap();
    consumer
        .apply_pull_result(
            unresolved_request.clone(),
            context(),
            pull_result(PullStatus::NoMatchedMessage, 35),
        )
        .await;
    scheduler.finish(&active_unresolved_request);
    assert_eq!(offset_store.updates.lock().unwrap().len(), 1);
    assert_eq!(
        unresolved_process_queue.snapshot(2).unwrap().cached_count,
        1
    );
    let follow_up = scheduler.take_due().unwrap();
    scheduler.finish(&follow_up);

    let found_queue = MessageQueue::new("orders", "broker-a", 2);
    let found_process_queue = Arc::new(ProcessQueue::new_for_assignment(found_queue, 3));
    let found_request = Arc::new(PullRequest::new(
        "pull-outcomes",
        found_process_queue.clone(),
        40,
    ));
    scheduler.schedule(found_request.clone(), 0).unwrap();
    let active_found_request = scheduler.take_due().unwrap();
    let mut found = pull_result(PullStatus::Found, 42);
    found.messages = Some(vec![message(40)]);
    consumer.config().set_pull_interval_millis(25);
    consumer
        .apply_pull_result(found_request.clone(), context(), found)
        .await;
    scheduler.finish(&active_found_request);
    let snapshot = found_process_queue.snapshot(3).unwrap();
    assert_eq!(snapshot.cached_count, 1);
    assert_eq!(found_request.next_offset(), 42);
    assert!(scheduler.take_due().is_none());
    tokio::time::advance(Duration::from_millis(25)).await;
    assert_eq!(scheduler.take_due().unwrap().next_offset(), 42);
}

#[tokio::test]
async fn push_pull_request_uses_live_batch_settings_from_consumer_config() {
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nameserver_address = nameserver.local_addr().unwrap().to_string();
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let broker_route = pop_test_route(&broker_address);

    let nameserver_peer = tokio::spawn(async move {
        let (mut stream, _) = nameserver.accept().await.unwrap();
        let incoming = read_remoting_command(&mut stream).await;
        assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
        write_remoting_reply_with_body(
            &mut stream,
            &incoming,
            response::SUCCESS,
            None,
            Some(broker_route),
        )
        .await;
    });

    let broker_peer = tokio::spawn(async move {
        let mut stream = None;
        let incoming = accept_remoting_request(&broker_listener, &mut stream).await;
        assert_eq!(incoming.code, request::PULL_MESSAGE);
        let fields = incoming.ext_fields.as_ref().unwrap();
        assert_eq!(
            fields.get("consumerGroup"),
            Some(&"push-live-pull-batch".to_owned())
        );
        assert_eq!(fields.get("maxMsgNums"), Some(&"7".to_owned()));
        assert_eq!(fields.get("maxMsgBytes"), Some(&"12345".to_owned()));
        assert_eq!(fields.get("subscription"), Some(&"tagA".to_owned()));
        assert_eq!(fields.get("commitOffset"), Some(&"42".to_owned()));
        // Java simple-consumer suspend value; the POP path keeps 20000.
        assert_eq!(
            fields.get("suspendTimeoutMillis"),
            Some(&"15000".to_owned())
        );
        assert!(
            rocketmq_classic_common::sysflag::pull::has_commit_offset_flag(
                fields.get("sysFlag").unwrap().parse().unwrap()
            )
        );
        write_remoting_reply(
            stream.as_mut().unwrap(),
            &incoming,
            response::PULL_NOT_FOUND,
            Some(HeaderMap::from([
                ("nextBeginOffset".into(), "0".into()),
                ("minOffset".into(), "0".into()),
                ("maxOffset".into(), "0".into()),
                ("suggestWhichBrokerId".into(), "0".into()),
            ])),
        )
        .await;
    });

    let mut config = PushConsumerConfig::new("push-live-pull-batch").unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(nameserver_address));
    config
        .client_config_mut()
        .set_instance_name(Some("push-live-pull-batch".into()));
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    config.set_pull_batch_size(3);
    config.set_pull_batch_size_in_bytes(4_096);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    runtime
        .update_topic_route("orders", Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    nameserver_peer.await.unwrap();

    consumer.config().set_pull_batch_size(7);
    consumer.config().set_pull_batch_size_in_bytes(12_345);
    consumer
        .config()
        .set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
    consumer.config().set_post_subscription_when_pull(true);
    assert_eq!(consumer.config().pull_batch_size(), 7);
    assert_eq!(consumer.config().pull_batch_size_in_bytes(), 12_345);
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let offset_store = Arc::new(RecordingOffsetStore::default());
    offset_store.update_offset(&queue, 42, false);

    let request = PullRequest::new(
        "push-live-pull-batch",
        Arc::new(ProcessQueue::new_for_assignment(queue, 1)),
        0,
    );
    consumer.set_consumer_group(Some("push-live-pull-mutated-group".into()));
    let context = PullContext {
        runtime: runtime.clone(),
        offset_store,
        scheduler: Arc::new(PullScheduler::new()),
        consume_service: ConsumeService::Concurrent(idle_consume_service()),
    };
    let subscription = SubscriptionData::new(Some("orders".into()), Some("tagA".into()));

    let pull_result = consumer
        .fetch_push_result(&context, &request, &subscription)
        .await;
    assert!(pull_result.is_ok());
    broker_peer.await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[test]
fn pull_and_pop_keep_separate_broker_suspend_values() {
    // The Pull header carries the private simple-consumer suspend value; the
    // POP worker keeps its own long-poll wait and `pollTime + 10s` deadline.
    assert_eq!(PULL_BROKER_SUSPEND_MAX_TIME_MILLIS, 15_000);
    assert_eq!(BROKER_SUSPEND_MAX_TIME_MILLIS, 20_000);
    assert_eq!(CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND, 30_000);
}

#[tokio::test]
async fn closed_pull_admission_releases_the_slot_without_an_rpc_attempt() {
    let config = PushConsumerConfig::new("closed-pull-admission").unwrap();
    let consumer = Arc::new(PushConsumer::new(config));
    let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let scheduler = Arc::new(PullScheduler::new());
    // Never started, so admission is closed and no dispatcher task exists.
    let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
    let queue = MessageQueue::new("orders", "broker-a", 0);
    scheduler
        .schedule(
            Arc::new(PullRequest::new(
                "closed-pull-admission",
                Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 1)),
                7,
            )),
            0,
        )
        .unwrap();
    let admitted = scheduler.take_due().expect("one admitted request");
    assert_eq!(scheduler.in_flight_count(), 1);

    let registration = PushConsumerRegistration {
        group: "closed-pull-admission".to_owned(),
        message_model: consumer.message_model(),
        consume_from_where: consumer.consume_from_where(),
        unit_mode: consumer.is_unit_mode(),
        subscriptions: consumer.subscriptions.clone(),
        topic_queues: consumer.topic_queues.clone(),
        pull_scheduler: scheduler.clone(),
        pull_rpc_workers: pull_rpc_workers.clone(),
        offset_store: Arc::downgrade(&store),
        consumer: Arc::downgrade(&consumer),
        runtime: std::sync::Weak::new(),
    };

    let request: Arc<dyn rocketmq_classic_core::runtime::MessageRequest> = admitted;
    ConsumerRuntime::pull_message(&registration, request)
        .expect("closed admission still consumes the pull submission")
        .await;

    // The queue-generation slot is released and no retry is re-queued.
    assert_eq!(scheduler.in_flight_count(), 0);
    assert_eq!(scheduler.scheduled_pull_count(), 0);
    assert!(scheduler.take_due().is_none());
    assert_eq!(pull_rpc_workers.active_admissions(), 0);
}

#[tokio::test]
async fn one_pull_attempt_is_made_per_admission_and_a_follow_up_waits_for_release() {
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nameserver_address = nameserver.local_addr().unwrap().to_string();
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let broker_route = pop_test_route(&broker_address);

    let nameserver_peer = tokio::spawn(async move {
        let mut stream = None;
        for _ in 0..4 {
            let incoming = accept_remoting_request(&nameserver, &mut stream).await;
            if incoming.code != request::GET_ROUTEINFO_BY_TOPIC {
                continue;
            }
            write_remoting_reply_with_body(
                stream.as_mut().unwrap(),
                &incoming,
                response::SUCCESS,
                None,
                Some(broker_route.clone()),
            )
            .await;
        }
    });

    let pull_attempts = Arc::new(AtomicUsize::new(0));
    let attempts = pull_attempts.clone();
    let broker_peer = tokio::spawn(async move {
        let mut stream = None;
        while attempts.load(Ordering::Acquire) < 3 {
            let incoming = accept_remoting_request(&broker_listener, &mut stream).await;
            assert_eq!(incoming.code, request::PULL_MESSAGE);
            let fields = incoming.ext_fields.as_ref().unwrap();
            // Java simple-consumer suspend value, not the POP long-poll value.
            assert_eq!(
                fields.get("suspendTimeoutMillis"),
                Some(&"15000".to_owned())
            );
            attempts.fetch_add(1, Ordering::AcqRel);
            write_remoting_reply(
                stream.as_mut().unwrap(),
                &incoming,
                response::SYSTEM_ERROR,
                None,
            )
            .await;
        }
    });

    let mut config = PushConsumerConfig::new("pull-single-attempt").unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(nameserver_address));
    config
        .client_config_mut()
        .set_instance_name(Some("pull-single-attempt".into()));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    consumer.subscribe("orders", "tagA").await.unwrap();
    let scheduler = Arc::new(PullScheduler::new());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        scheduler.clone(),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    runtime.remoting().start().unwrap();
    runtime
        .update_topic_route("orders", Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 1));
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    scheduler
        .schedule(
            Arc::new(PullRequest::new("pull-single-attempt", process_queue, 5)),
            0,
        )
        .unwrap();
    let admitted = scheduler.take_due().expect("one admitted request");

    consumer
        .clone()
        .run_pull_worker_request(admitted.clone())
        .await;

    // One admission issues exactly one Pull RPC even though the broker failed.
    assert_eq!(pull_attempts.load(Ordering::Acquire), 1);
    // The follow-up waits while the slot is held, then becomes due on release.
    assert_eq!(scheduler.scheduled_pull_count(), 0);
    scheduler.finish(&admitted);
    assert_eq!(scheduler.in_flight_count(), 0);
    assert_eq!(scheduler.scheduled_pull_count(), 1);

    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
    broker_peer.abort();
    nameserver_peer.abort();
}

#[tokio::test(start_paused = true)]
async fn push_pull_uses_the_future_rpc_mode_within_the_thirty_second_deadline() {
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nameserver_address = nameserver.local_addr().unwrap().to_string();
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let broker_route = pop_test_route(&broker_address);

    let nameserver_peer = tokio::spawn(async move {
        let (mut stream, _) = nameserver.accept().await.unwrap();
        let incoming = read_remoting_command(&mut stream).await;
        assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
        write_remoting_reply_with_body(
            &mut stream,
            &incoming,
            response::SUCCESS,
            None,
            Some(broker_route),
        )
        .await;
    });

    // The scripted peer answers after the 30 s client deadline but inside the
    // future-mode transport grace window. A synchronous pull attempt would have
    // been cut off at the deadline itself.
    let broker_peer = tokio::spawn(async move {
        let (mut stream, _) = broker_listener.accept().await.unwrap();
        let incoming = read_remoting_command(&mut stream).await;
        assert_eq!(incoming.code, request::PULL_MESSAGE);
        assert_eq!(
            incoming
                .ext_fields
                .as_ref()
                .unwrap()
                .get("suspendTimeoutMillis"),
            Some(&"15000".to_owned())
        );
        tokio::time::sleep(Duration::from_millis(
            CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND as u64 + 500,
        ))
        .await;
        write_remoting_reply(
            &mut stream,
            &incoming,
            response::PULL_NOT_FOUND,
            Some(HeaderMap::from([
                ("nextBeginOffset".into(), "9".into()),
                ("minOffset".into(), "0".into()),
                ("maxOffset".into(), "9".into()),
                ("suggestWhichBrokerId".into(), "0".into()),
            ])),
        )
        .await;
    });

    let mut config = PushConsumerConfig::new("pull-future-mode").unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(nameserver_address));
    config
        .client_config_mut()
        .set_instance_name(Some("pull-future-mode".into()));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    runtime
        .update_topic_route("orders", Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    nameserver_peer.await.unwrap();

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let request = PullRequest::new(
        "pull-future-mode",
        Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 1)),
        4,
    );
    let context = PullContext {
        runtime: runtime.clone(),
        offset_store: Arc::new(RecordingOffsetStore::default()),
        scheduler: Arc::new(PullScheduler::new()),
        consume_service: ConsumeService::Concurrent(idle_consume_service()),
    };
    consumer.subscribe("orders", "tagA").await.unwrap();
    let subscription = read_lock(&consumer.subscriptions)
        .get("orders")
        .cloned()
        .expect("subscribed topic");

    let pull_result = match consumer
        .fetch_push_result(&context, &request, &subscription)
        .await
    {
        Ok(result) => result,
        Err(error) => panic!(
            "future-mode pull should complete inside the deadline grace: {}",
            error.reason
        ),
    };
    assert_eq!(pull_result.result.next_begin_offset, 9);
    assert_eq!(pull_result.result.status, PullStatus::NoNewMessage);

    broker_peer.await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn activated_pull_request_keeps_the_start_group_after_facade_mutation() {
    let mut config = PushConsumerConfig::new("pull-request-group-snapshot").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    consumer.subscribe("orders", "*").await.unwrap();
    let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        store,
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    consumer.set_consumer_group(Some("pull-request-live-group".into()));

    let queue = MessageQueue::new("orders", "broker-a", 0);
    consumer.activate_process_queue(&queue, 0).await.unwrap();
    let scheduler = match &*consumer.state.lock().await {
        State::Started { pull_scheduler, .. } => pull_scheduler.clone(),
        State::Created | State::Shutdown => unreachable!(),
    };
    let request = scheduler.take_due().unwrap();
    assert_eq!(request.consumer_group(), "pull-request-group-snapshot");

    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn first_orderly_locked_pull_refreshes_offset_once_like_java() {
    let mut config = PushConsumerConfig::new("orderly-first-lock-offset").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let store = Arc::new(RecordingOffsetStore::default());
    let queue = MessageQueue::new("orders", "broker-a", 0);
    store.seed_persisted(queue.clone(), 15);
    store.update_offset(&queue, 8, false);
    let store_dyn: Arc<dyn OffsetStore> = store.clone();
    let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
        |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
            (Some(ConsumeOrderlyStatus::Success), context)
        },
    );
    let runtime =
        install_started_orderly_consumer(&consumer, &manager, store_dyn, listener, None).await;
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 101));
    process_queue
        .set_lock_state(101, true, epoch_millis())
        .unwrap();
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    consumer
        .scripted_lock_transport
        .lock()
        .unwrap()
        .unlock_results
        .push_back(Ok(()));
    let request = Arc::new(PullRequest::new(
        "orderly-first-lock-offset",
        process_queue,
        8,
    ));
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["orderlyPullStatsGroup"],
        "phase18-orderly-pull-offset"
    );
    assert_eq!(
        fixture["orderlyPullFacadeGroupAfterMutation"],
        "phase18-orderly-pull-offset-mutated"
    );

    consumer
        .clone()
        .run_pull_worker_request(request.clone())
        .await;
    assert_eq!(
        request.next_offset(),
        fixture["orderlyFirstLockedPullOffset"].as_i64().unwrap()
    );
    assert!(request.previously_locked());
    assert_eq!(fixture["orderlyFirstLockedPullPreviouslyLocked"], true);
    assert_eq!(
        store.reads.lock().unwrap().as_slice(),
        &[(queue.clone(), ReadOffsetType::ReadFromStore)]
    );

    store.seed_persisted(queue.clone(), 21);
    consumer
        .clone()
        .run_pull_worker_request(request.clone())
        .await;
    assert_eq!(request.next_offset(), 15);
    assert_eq!(
        fixture["orderlySecondLockedPullOffset"].as_i64().unwrap(),
        request.next_offset()
    );
    assert_eq!(
        store.reads.lock().unwrap().as_slice(),
        &[(queue, ReadOffsetType::ReadFromStore)]
    );
    assert_eq!(fixture["orderlySecondLockedPullStoreReads"], 1);

    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn pull_result_stats_use_the_request_group_snapshot() {
    let config = PushConsumerConfig::new("pull-stats-live-group").unwrap();
    let manager = ClientRuntimeManager::new();
    let consumer = PushConsumer::with_manager(config, manager.clone());
    let runtime = manager
        .get_or_create(consumer.config.client_config())
        .await
        .unwrap();
    consumer.bind_stats_manager(runtime.consumer_stats_manager());
    let request_group = "pull-stats-request-group";
    let request = PullRequest::new(
        request_group,
        Arc::new(ProcessQueue::new_for_assignment(
            MessageQueue::new("orders", "broker-a", 0),
            1,
        )),
        0,
    );
    consumer.set_consumer_group(Some("pull-stats-mutated-group".into()));
    consumer.record_pull_result_stats(&request, "orders", Duration::from_millis(2), 1);
    std::thread::sleep(Duration::from_secs(11));

    assert!(
        consumer
            .stats_manager()
            .unwrap()
            .consume_status(request_group, "orders")
            .get_pull_tps()
            > 0.0
    );
    assert_eq!(
        consumer
            .stats_manager()
            .unwrap()
            .consume_status("pull-stats-mutated-group", "orders")
            .get_pull_tps(),
        0.0
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn push_pull_rejects_stale_results_and_fences_offset_illegal_recovery() {
    let config = PushConsumerConfig::new("pull-generation-fence").unwrap();
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let scheduler = Arc::new(PullScheduler::new());
    let consume_service = idle_consume_service();
    let context = || PullContext {
        runtime: runtime.clone(),
        offset_store: offset_store.clone(),
        scheduler: scheduler.clone(),
        consume_service: ConsumeService::Concurrent(consume_service.clone()),
    };

    let stale_queue = MessageQueue::new("orders", "broker-a", 3);
    let stale_process_queue = Arc::new(ProcessQueue::new_for_assignment(stale_queue, 4));
    let stale_request = Arc::new(PullRequest::new(
        "pull-generation-fence",
        stale_process_queue.clone(),
        50,
    ));
    scheduler.schedule(stale_request.clone(), 0).unwrap();
    let active_stale_request = scheduler.take_due().unwrap();
    stale_process_queue.drop_assignment(4).unwrap();
    let mut stale_result = pull_result(PullStatus::Found, 51);
    stale_result.messages = Some(vec![message(50)]);
    consumer
        .apply_pull_result(stale_request, context(), stale_result)
        .await;
    scheduler.finish(&active_stale_request);
    assert_eq!(stale_process_queue.snapshot(4).unwrap().cached_count, 0);
    assert!(offset_store.updates.lock().unwrap().is_empty());

    let illegal_queue = MessageQueue::new("orders", "broker-a", 4);
    let illegal_process_queue =
        Arc::new(ProcessQueue::new_for_assignment(illegal_queue.clone(), 5));
    let illegal_request = Arc::new(PullRequest::new(
        "pull-generation-fence",
        illegal_process_queue.clone(),
        60,
    ));
    consumer
        .apply_pull_result(
            illegal_request,
            context(),
            pull_result(PullStatus::OffsetIllegal, 55),
        )
        .await;
    assert!(illegal_process_queue.snapshot(5).unwrap().dropped);
    assert!(read_lock(&consumer.offset_corrections_pending).contains(&illegal_queue));
    assert_eq!(
        offset_store.updates.lock().unwrap().as_slice(),
        &[(illegal_queue, 55, false)]
    );

    let _ = runtime.shutdown().await;
}

#[tokio::test]
async fn concurrent_ack_index_sends_only_the_unacknowledged_tail_back() {
    let config = PushConsumerConfig::new("consume-ack-index").unwrap();
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let scheduler = Arc::new(PullScheduler::new());
    let context = PullContext {
        runtime: runtime.clone(),
        offset_store: offset_store.clone(),
        scheduler,
        consume_service: ConsumeService::Concurrent(idle_consume_service()),
    };
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let request = consume_request(
        queue.clone(),
        31,
        vec![retry_test_message(10), retry_test_message(11)],
    );
    consumer
        .scripted_retry_transport
        .lock()
        .unwrap()
        .broker_results
        .push_back(Ok(()));
    let mut callback_context = ConsumeConcurrentlyContext::new(queue.clone());
    callback_context.set_ack_index(0);
    consumer
        .clone()
        .complete_consume_request(
            request,
            context,
            Some(ConsumeConcurrentlyStatus::ConsumeSuccess),
            callback_context,
        )
        .await;

    {
        let scripted = consumer.scripted_retry_transport.lock().unwrap();
        assert_eq!(scripted.broker_headers.len(), 1);
        assert_eq!(scripted.broker_headers[0].get_origin_topic(), "orders");
        assert_eq!(scripted.broker_headers[0].get_offset(), 111);
        assert_eq!(
            scripted.broker_headers[0].get_origin_msg_id(),
            Some("broker-msg-11")
        );
        assert_eq!(
            scripted.broker_headers[0].get_max_reconsume_times(),
            Some(16)
        );
        assert!(scripted.retry_messages.is_empty());
    }
    assert_eq!(
        offset_store.updates.lock().unwrap().as_slice(),
        &[(queue, 12, true)]
    );
    let _ = runtime.shutdown().await;
}

#[tokio::test]
async fn expired_concurrent_messages_match_java_process_queue_cleanup() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/expired-messages.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["provenance"]["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );
    let null_consumer_error = ProcessQueue::new()
        .clean_expired_msg_nullable(None)
        .await
        .expect_err("Java dereferences the consumer before scanning messages");
    assert!(matches!(
        &null_consumer_error,
        ExpiredMessageCleanupError::NullPushConsumer
    ));
    assert_eq!(
        null_consumer_error.to_string(),
        fixture["nullConsumerErrorClass"].as_str().unwrap()
    );

    let config = PushConsumerConfig::new("expired-message-cleanup").unwrap();
    config.set_consume_timeout_minutes(1);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;

    let make_message = |offset: i64, body_size: usize, started_at: i64| {
        let mut message = Message::new("orders", vec![b'x'; body_size]);
        message
            .set_system_property(PROPERTY_CONSUME_START_TIMESTAMP, started_at.to_string())
            .unwrap();
        let mut message = crate::test_support::message_ext_from(message);
        message.set_queue_offset(offset);
        message.set_broker_name(Some("broker-a".to_owned()));
        message.set_commit_log_offset(offset);
        message
    };
    let now_millis = 106_001;
    let direct_clean_now = epoch_millis();

    let expired_queue = MessageQueue::new("orders", "broker-a", 0);
    let expired_process_queue =
        Arc::new(ProcessQueue::new_for_assignment(expired_queue.clone(), 101));
    expired_process_queue
        .insert_messages(
            101,
            vec![
                make_message(10, 2, direct_clean_now - 60_001),
                // Keep this entry within the timeout window with margin;
                // cleanup reads the wall clock again after its async send.
                make_message(11, 3, direct_clean_now - 59_000),
            ],
        )
        .unwrap();
    write_lock(&consumer.process_queues).insert(expired_queue, expired_process_queue.clone());
    consumer
        .scripted_retry_transport
        .lock()
        .unwrap()
        .broker_results
        .push_back(Ok(()));
    expired_process_queue
        .clean_expired_msg(&consumer)
        .await
        .unwrap();
    let expired_snapshot = expired_process_queue.snapshot(101).unwrap();
    assert_eq!(
        expired_snapshot.cached_count,
        fixture["expiredHead"]["cachedCount"]
    );
    assert_eq!(
        expired_snapshot.cached_bytes,
        fixture["expiredHead"]["cachedBytes"]
    );
    assert_eq!(expired_snapshot.minimum_offset, Some(11));
    assert_eq!(expired_snapshot.maximum_offset, Some(11));

    let failed_queue = MessageQueue::new("orders", "broker-a", 1);
    let failed_process_queue =
        Arc::new(ProcessQueue::new_for_assignment(failed_queue.clone(), 102));
    failed_process_queue
        .insert_messages(
            102,
            vec![make_message(20, 2, 46_000), make_message(21, 3, 46_001)],
        )
        .unwrap();
    write_lock(&consumer.process_queues).insert(failed_queue.clone(), failed_process_queue.clone());
    {
        let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
        scripted
            .broker_results
            .extend(std::iter::repeat_with(|| Err("scripted send-back failure".into())).take(2));
        scripted
            .retry_results
            .extend(std::iter::repeat_with(|| Err("scripted retry failure".into())).take(2));
    }
    failed_process_queue
        .clean_expired_msg(&consumer)
        .await
        .unwrap();
    let failed_snapshot = failed_process_queue.snapshot(102).unwrap();
    assert_eq!(
        failed_snapshot.cached_count,
        fixture["sendBackFailure"]["cachedCount"]
    );
    assert_eq!(
        failed_snapshot.cached_bytes,
        fixture["sendBackFailure"]["cachedBytes"]
    );
    assert_eq!(failed_snapshot.minimum_offset, Some(20));
    assert_eq!(failed_snapshot.maximum_offset, Some(21));
    write_lock(&consumer.process_queues).remove(&failed_queue);

    let limit_queue = MessageQueue::new("orders", "broker-a", 2);
    let limit_process_queue = Arc::new(ProcessQueue::new_for_assignment(limit_queue.clone(), 103));
    let limit_messages = (0..18)
        .map(|index| make_message(60 + index, 2 + index as usize, 46_000))
        .collect();
    limit_process_queue
        .insert_messages(103, limit_messages)
        .unwrap();
    write_lock(&consumer.process_queues).insert(limit_queue.clone(), limit_process_queue.clone());
    {
        let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
        scripted
            .broker_results
            .extend(std::iter::repeat_with(|| Err("scripted send-back failure".into())).take(16));
        scripted
            .retry_results
            .extend(std::iter::repeat_with(|| Err("scripted retry failure".into())).take(16));
    }
    consumer
        .clean_expired_process_queue_messages_at(now_millis)
        .await
        .unwrap();
    let limit_snapshot = limit_process_queue.snapshot(103).unwrap();
    assert_eq!(
        limit_snapshot.cached_count,
        fixture["attemptLimit"]["cachedCount"]
    );
    assert_eq!(
        limit_snapshot.cached_bytes,
        fixture["attemptLimit"]["cachedBytes"]
    );
    assert_eq!(limit_snapshot.minimum_offset, Some(60));
    assert_eq!(limit_snapshot.maximum_offset, Some(77));
    write_lock(&consumer.process_queues).remove(&limit_queue);

    let orderly_queue = MessageQueue::new("orders", "broker-a", 3);
    let orderly_process_queue =
        Arc::new(ProcessQueue::new_for_assignment(orderly_queue.clone(), 104));
    orderly_process_queue
        .insert_messages(104, vec![make_message(30, 2, 46_000)])
        .unwrap();
    write_lock(&consumer.process_queues).insert(orderly_queue, orderly_process_queue.clone());
    consumer.set_consume_orderly(true);
    consumer
        .clean_expired_process_queue_messages_at(now_millis)
        .await
        .unwrap();
    consumer.set_consume_orderly(false);
    let orderly_snapshot = orderly_process_queue.snapshot(104).unwrap();
    assert_eq!(
        orderly_snapshot.cached_count,
        fixture["orderlySkipped"]["cachedCount"]
    );
    assert_eq!(
        orderly_snapshot.cached_bytes,
        fixture["orderlySkipped"]["cachedBytes"]
    );
    assert_eq!(orderly_snapshot.minimum_offset, Some(30));

    {
        let scripted = consumer.scripted_retry_transport.lock().unwrap();
        let attempts = scripted
            .broker_headers
            .iter()
            .map(|header| format!("{}:{}", header.get_offset(), header.get_delay_level()))
            .collect::<Vec<_>>();
        assert_eq!(attempts.len(), 19);
        assert_eq!(attempts[0], "10:3");
        assert_eq!(attempts[1..3], ["20:3", "20:3"]);
        assert!(attempts[3..19].iter().all(|attempt| attempt == "60:3"));
        for (name, range) in [
            ("expiredHead", 0..1),
            ("sendBackFailure", 1..3),
            ("attemptLimit", 3..19),
            ("orderlySkipped", 19..19),
        ] {
            let fixture_attempts = fixture[name]["attempts"]
                .as_array()
                .unwrap()
                .iter()
                .map(|attempt| attempt.as_str().unwrap().to_owned())
                .collect::<Vec<_>>();
            let rust_attempts = attempts[range].to_vec();
            assert_eq!(rust_attempts, fixture_attempts, "{name}");
        }
    }

    let malformed_queue = MessageQueue::new("orders", "broker-a", 4);
    let malformed_process_queue = Arc::new(ProcessQueue::new_for_assignment(
        malformed_queue.clone(),
        105,
    ));
    let mut malformed_message = make_message(40, 4, 0);
    malformed_message
        .set_system_property(PROPERTY_CONSUME_START_TIMESTAMP, "not-a-long".to_owned())
        .unwrap();
    malformed_process_queue
        .insert_messages(105, vec![malformed_message])
        .unwrap();
    let direct_error = malformed_process_queue
        .clean_expired_msg(&consumer)
        .await
        .unwrap_err();
    assert!(matches!(
        &direct_error,
        ExpiredMessageCleanupError::InvalidConsumeStartTimestamp { offset: 40, value }
            if value == "not-a-long"
    ));
    assert_eq!(
        fixture["malformedTimestamp"]["errorClass"],
        "java.lang.NumberFormatException"
    );
    let malformed_snapshot = malformed_process_queue.snapshot(105).unwrap();
    assert_eq!(
        malformed_snapshot.cached_count,
        fixture["malformedTimestamp"]["cachedCount"]
    );
    assert_eq!(
        malformed_snapshot.cached_bytes,
        fixture["malformedTimestamp"]["cachedBytes"]
    );
    assert_eq!(malformed_snapshot.minimum_offset, Some(40));
    assert_eq!(malformed_snapshot.maximum_offset, Some(40));

    write_lock(&consumer.process_queues).clear();
    write_lock(&consumer.process_queues).insert(malformed_queue, malformed_process_queue.clone());
    let scheduled_error = consumer
        .clean_expired_process_queue_messages_at(now_millis)
        .await
        .unwrap_err();
    assert_eq!(scheduled_error, direct_error);
    assert!(
        fixture["malformedTimestamp"]["attempts"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        malformed_process_queue.snapshot(105).unwrap().cached_count,
        1
    );

    let empty_queue = MessageQueue::new("orders", "broker-a", 5);
    let empty_process_queue = Arc::new(ProcessQueue::new_for_assignment(empty_queue, 106));
    let mut empty_message = make_message(50, 4, 0);
    empty_message
        .set_system_property(PROPERTY_CONSUME_START_TIMESTAMP, String::new())
        .unwrap();
    empty_process_queue
        .insert_messages(106, vec![empty_message])
        .unwrap();
    empty_process_queue
        .clean_expired_msg(&consumer)
        .await
        .unwrap();
    let empty_snapshot = empty_process_queue.snapshot(106).unwrap();
    assert_eq!(
        empty_snapshot.cached_count,
        fixture["emptyTimestamp"]["cachedCount"]
    );
    assert_eq!(
        empty_snapshot.cached_bytes,
        fixture["emptyTimestamp"]["cachedBytes"]
    );
    assert_eq!(empty_snapshot.minimum_offset, Some(50));
    assert!(fixture["emptyTimestamp"]["errorClass"].is_null());
    assert!(
        fixture["emptyTimestamp"]["attempts"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let missing_queue = MessageQueue::new("orders", "broker-a", 6);
    let missing_process_queue = Arc::new(ProcessQueue::new_for_assignment(missing_queue, 107));
    let missing_message = Message::new("orders", vec![b'x'; 4]);
    let mut missing_message = crate::test_support::message_ext_from(missing_message);
    missing_message.set_queue_offset(51);
    missing_message.set_broker_name(Some("broker-a".to_owned()));
    missing_process_queue
        .insert_messages(107, vec![missing_message])
        .unwrap();
    missing_process_queue
        .clean_expired_msg(&consumer)
        .await
        .unwrap();
    let missing_snapshot = missing_process_queue.snapshot(107).unwrap();
    assert_eq!(
        missing_snapshot.cached_count,
        fixture["missingTimestamp"]["cachedCount"]
    );
    assert_eq!(
        missing_snapshot.cached_bytes,
        fixture["missingTimestamp"]["cachedBytes"]
    );
    assert_eq!(missing_snapshot.minimum_offset, Some(51));
    assert!(fixture["missingTimestamp"]["errorClass"].is_null());
    assert!(
        fixture["missingTimestamp"]["attempts"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let orderly_malformed_queue = MessageQueue::new("orders", "broker-a", 7);
    let orderly_malformed_process_queue = Arc::new(ProcessQueue::new_for_assignment(
        orderly_malformed_queue,
        108,
    ));
    let mut orderly_malformed_message = make_message(52, 4, 0);
    orderly_malformed_message
        .set_system_property(PROPERTY_CONSUME_START_TIMESTAMP, "not-a-long".to_owned())
        .unwrap();
    orderly_malformed_process_queue
        .insert_messages(108, vec![orderly_malformed_message])
        .unwrap();
    consumer.set_consume_orderly(true);
    orderly_malformed_process_queue
        .clean_expired_msg(&consumer)
        .await
        .unwrap();
    consumer.set_consume_orderly(false);
    let orderly_malformed_snapshot = orderly_malformed_process_queue.snapshot(108).unwrap();
    assert_eq!(
        orderly_malformed_snapshot.cached_count,
        fixture["orderlyMalformedTimestamp"]["cachedCount"]
    );
    assert_eq!(orderly_malformed_snapshot.minimum_offset, Some(52));
    assert!(fixture["orderlyMalformedTimestamp"]["errorClass"].is_null());
    assert!(
        fixture["orderlyMalformedTimestamp"]["attempts"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let unicode_queue = MessageQueue::new("orders", "broker-a", 8);
    let unicode_process_queue = Arc::new(ProcessQueue::new_for_assignment(unicode_queue, 109));
    let mut unicode_message = make_message(40, 4, 0);
    unicode_message
        .set_system_property(PROPERTY_CONSUME_START_TIMESTAMP, "٤٦٠٠٠".to_owned())
        .unwrap();
    unicode_process_queue
        .insert_messages(109, vec![unicode_message])
        .unwrap();
    consumer
        .scripted_retry_transport
        .lock()
        .unwrap()
        .broker_results
        .push_back(Ok(()));
    let attempts_before = consumer
        .scripted_retry_transport
        .lock()
        .unwrap()
        .broker_headers
        .len();
    assert!(matches!(
        unicode_process_queue.clean_expired_msg(&consumer).await,
        Err(ExpiredMessageCleanupError::InvalidConsumeStartTimestamp { offset: 40, .. })
    ));
    let unicode_snapshot = unicode_process_queue.snapshot(109).unwrap();
    assert_eq!(unicode_snapshot.cached_count, 1);
    assert_eq!(unicode_snapshot.cached_bytes, 4);
    assert_eq!(unicode_snapshot.minimum_offset, Some(40));
    assert_eq!(
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .broker_headers
            .len(),
        attempts_before
    );

    consumer.shutdown().await.unwrap();
    let _ = runtime.shutdown().await;
}

#[tokio::test]
async fn concurrent_consume_start_timestamp_allows_expiry_while_listener_is_running() {
    let config = PushConsumerConfig::new("expired-message-in-flight").unwrap();
    config.set_consume_timeout_minutes(1);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        offset_store.clone(),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 104));
    process_queue
        .insert_messages(104, vec![retry_test_message(40)])
        .unwrap();
    let batch = process_queue.take_batch(104, 1).unwrap().unwrap();
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    let request = ConsumeRequest {
        process_queue: process_queue.clone(),
        batch,
    };

    let listener_started = Arc::new(tokio::sync::Notify::new());
    let continue_listener = Arc::new(tokio::sync::Notify::new());
    let observed_start = Arc::new(Mutex::new(None::<String>));
    let listener: Arc<dyn MessageListenerConcurrently> = {
        let listener_started = listener_started.clone();
        let continue_listener = continue_listener.clone();
        let observed_start = observed_start.clone();
        Arc::new(
            move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let listener_started = listener_started.clone();
                let continue_listener = continue_listener.clone();
                let observed_start = observed_start.clone();
                async move {
                    *observed_start.lock().unwrap() = messages
                        .first()
                        .and_then(|message| message.get_property(PROPERTY_CONSUME_START_TIMESTAMP));
                    listener_started.notify_one();
                    continue_listener.notified().await;
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        )
    };

    consumer
        .scripted_retry_transport
        .lock()
        .unwrap()
        .broker_results
        .push_back(Ok(()));
    let consume = {
        let consumer = consumer.clone();
        tokio::spawn(async move { consumer.run_consume_request(request, listener).await })
    };
    listener_started.notified().await;
    let timestamp = observed_start
        .lock()
        .unwrap()
        .as_deref()
        .unwrap()
        .parse::<i64>()
        .unwrap();
    assert_eq!(
        process_queue
            .expired_head_message(104, timestamp + 60_001, 60_000)
            .unwrap()
            .map(|(offset, _)| offset),
        Some(40)
    );
    consumer
        .clean_expired_process_queue_messages_at(timestamp + 60_001)
        .await
        .unwrap();
    assert_eq!(process_queue.snapshot(104).unwrap().cached_count, 0);

    continue_listener.notify_one();
    consume.await.unwrap();
    assert!(offset_store.updates.lock().unwrap().is_empty());
    assert_eq!(process_queue.snapshot(104).unwrap().cached_count, 0);

    consumer.shutdown().await.unwrap();
    let _ = runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn concurrent_consume_service_runs_expiry_sweep_after_java_initial_delay() {
    let config = PushConsumerConfig::new("expired-message-schedule").unwrap();
    config.set_consume_timeout_minutes(1);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 105));
    let mut expired = retry_test_message(50);
    expired
        .set_system_property(
            PROPERTY_CONSUME_START_TIMESTAMP,
            (epoch_millis() - 61_000).to_string(),
        )
        .unwrap();
    process_queue.insert_messages(105, vec![expired]).unwrap();
    write_lock(&consumer.process_queues).insert(queue, process_queue.clone());
    consumer
        .scripted_retry_transport
        .lock()
        .unwrap()
        .broker_results
        .push_back(Ok(()));

    tokio::time::advance(Duration::from_secs(60)).await;
    for _ in 0..32 {
        if process_queue.snapshot(105).unwrap().cached_count == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(process_queue.snapshot(105).unwrap().cached_count, 0);
    assert_eq!(
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .broker_headers
            .len(),
        1
    );

    consumer.shutdown().await.unwrap();
    let _ = runtime.shutdown().await;
}

#[tokio::test]
async fn broker_send_back_failure_publishes_java_compatible_retry_metadata() {
    let mut config = PushConsumerConfig::new("consume-retry-fallback").unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let context = PullContext {
        runtime: runtime.clone(),
        offset_store: offset_store.clone(),
        scheduler: Arc::new(PullScheduler::new()),
        consume_service: ConsumeService::Concurrent(idle_consume_service()),
    };
    let queue = MessageQueue::new("orders", "broker-a", 1);
    let request = consume_request(queue.clone(), 32, vec![retry_test_message(20)]);
    {
        let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
        scripted
            .broker_results
            .push_back(Err("broker refused send-back".into()));
        scripted.retry_attempt_scripts.push_back(vec![
            Err("inner producer first Broker failed".into()),
            Err("inner producer second Broker failed".into()),
            Ok(()),
        ]);
    }
    let callback_context = ConsumeConcurrentlyContext::new(queue.clone());
    consumer
        .clone()
        .complete_consume_request(
            request,
            context,
            Some(ConsumeConcurrentlyStatus::ReconsumeLater),
            callback_context,
        )
        .await;

    {
        let scripted = consumer.scripted_retry_transport.lock().unwrap();
        assert_eq!(scripted.retry_attempt_counts, [3]);
        let retry = &scripted.retry_messages[0];
        assert_eq!(retry.topic(), "%RETRY%tenant%consume-retry-fallback");
        assert_eq!(
            retry
                .properties()
                .get(PROPERTY_RETRY_TOPIC)
                .map(String::as_str),
            Some("tenant%orders")
        );
        assert_eq!(
            retry
                .properties()
                .get(PROPERTY_ORIGIN_MESSAGE_ID)
                .map(String::as_str),
            Some("origin-msg")
        );
        assert_eq!(
            retry
                .properties()
                .get(PROPERTY_RECONSUME_TIME)
                .map(String::as_str),
            Some("3")
        );
        assert_eq!(
            retry
                .properties()
                .get(PROPERTY_MAX_RECONSUME_TIMES)
                .map(String::as_str),
            Some("16")
        );
        assert_eq!(
            retry
                .properties()
                .get(PROPERTY_DELAY_TIME_LEVEL)
                .map(String::as_str),
            Some("5")
        );
        assert_eq!(
            retry
                .properties()
                .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                .map(String::as_str),
            Some("source-uniq")
        );
        assert_eq!(
            retry.properties().get(PROPERTY_KEYS).map(String::as_str),
            Some("order-1")
        );
        assert!(
            !retry
                .properties()
                .contains_key(PROPERTY_TRANSACTION_PREPARED)
        );
    }
    assert_eq!(
        offset_store.updates.lock().unwrap().as_slice(),
        &[(queue, 21, true)]
    );
    let _ = runtime.shutdown().await;
}

#[tokio::test]
async fn send_back_uses_live_facade_group_and_namespaced_retry_topic() {
    let mut config = PushConsumerConfig::new("phase18-send-back-group-snapshot").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        Arc::new(RecordingOffsetStore::default()),
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    consumer.set_consumer_group(Some("phase18-consume-retry-live".into()));

    let offset_store = Arc::new(RecordingOffsetStore::default());
    let context = PullContext {
        runtime: runtime.clone(),
        offset_store,
        scheduler: Arc::new(PullScheduler::new()),
        consume_service: ConsumeService::Concurrent(idle_consume_service()),
    };
    let queue = MessageQueue::new("tenant%orders", "broker-a", 1);
    let mut retry_input = retry_test_message(90);
    retry_input.set_property_nullable(None, Some("value".to_owned()));
    let request = consume_request(queue.clone(), 195, vec![retry_input]);
    {
        let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
        scripted
            .broker_results
            .push_back(Err("broker refused send-back".into()));
        scripted.retry_results.push_back(Ok(()));
    }
    consumer
        .clone()
        .complete_consume_request(
            request,
            context,
            Some(ConsumeConcurrentlyStatus::ReconsumeLater),
            ConsumeConcurrentlyContext::new(queue),
        )
        .await;

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    let message_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/messages/records.json"
    ))
    .unwrap();
    {
        let scripted = consumer.scripted_retry_transport.lock().unwrap();
        assert_eq!(scripted.broker_headers.len(), 1);
        assert_eq!(
            Some(scripted.broker_headers[0].get_group()),
            fixture["brokerSendBackConsumerGroups"][0].as_str()
        );
        assert_eq!(scripted.retry_messages.len(), 1);
        assert_eq!(
            scripted.retry_messages[0].topic(),
            fixture["sendBackFallbackRetryTopics"][0].as_str().unwrap()
        );
        assert_eq!(
            scripted.retry_messages[0]
                .get_nullable_property(None)
                .as_deref(),
            Some("value")
        );
        assert_eq!(scripted.retry_wire_properties.len(), 1);
        assert!(
            scripted.retry_wire_properties[0].contains(
                message_fixture["messagePublicApi"]["nullKeyPropertyEncoded"]
                    .as_str()
                    .unwrap()
            )
        );
    }

    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_send_back_overloads_use_message_and_explicit_broker_names() {
    let config = PushConsumerConfig::new("public-send-back").unwrap();
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        offset_store,
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    {
        let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
        scripted
            .broker_results
            .extend([Ok(()), Ok(()), Ok(()), Ok(())]);
    }

    let message = retry_test_message(7);
    consumer.send_message_back(&message, 2).await.unwrap();
    consumer
        .send_message_back_to_broker(&message, 3, Some("broker-override"))
        .await
        .unwrap();
    let mut store_host_message = retry_test_message(8);
    store_host_message.set_broker_name(None);
    store_host_message.set_store_host(Some(MessageHostAddress::new("127.0.0.1", 10911)));
    consumer
        .send_message_back_to_broker(&store_host_message, 4, None)
        .await
        .unwrap();
    let mut custom_store_host_message = retry_test_message(9);
    let custom_store_host_subtype = Arc::new(FixedStoreHostSubtype {
        store_host: MessageHostAddress::new("127.0.0.1", 10912),
        setter_calls: AtomicUsize::new(0),
    });
    custom_store_host_message.set_runtime_subtype(custom_store_host_subtype.clone());
    custom_store_host_message.set_broker_name(None);
    custom_store_host_message.set_store_host(Some(MessageHostAddress::new("127.0.0.1", 10911)));
    consumer
        .send_message_back_to_broker(&custom_store_host_message, 5, None)
        .await
        .unwrap();

    {
        let scripted = consumer.scripted_retry_transport.lock().unwrap();
        assert_eq!(scripted.broker_headers.len(), 4);
        assert_eq!(
            scripted.broker_headers[0].get_broker_name(),
            Some("broker-a")
        );
        assert_eq!(scripted.broker_headers[0].get_delay_level(), 2);
        assert_eq!(
            scripted.broker_headers[1].get_broker_name(),
            Some("broker-override")
        );
        assert_eq!(scripted.broker_headers[1].get_delay_level(), 3);
        assert_eq!(scripted.broker_headers[2].get_broker_name(), None);
        assert_eq!(scripted.broker_headers[2].get_delay_level(), 4);
        assert_eq!(
            scripted.broker_targets[2],
            (None, Some("127.0.0.1:10911".to_owned()))
        );
        assert_eq!(scripted.broker_headers[3].get_broker_name(), None);
        assert_eq!(scripted.broker_headers[3].get_delay_level(), 5);
        assert_eq!(
            scripted.broker_targets[3],
            (None, Some("127.0.0.1:10912".to_owned()))
        );
        assert!(scripted.retry_messages.is_empty());
    }
    assert_eq!(
        custom_store_host_subtype
            .setter_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    consumer.shutdown().await.unwrap();
    let _ = runtime.shutdown().await;
}

#[tokio::test]
async fn failed_send_back_retains_and_increments_messages_for_redelivery() {
    let config = PushConsumerConfig::new("consume-retry-retention").unwrap();
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let context = PullContext {
        runtime: runtime.clone(),
        offset_store: offset_store.clone(),
        scheduler: Arc::new(PullScheduler::new()),
        consume_service: ConsumeService::Concurrent(idle_consume_service()),
    };
    let queue = MessageQueue::new("orders", "broker-a", 2);
    let request = consume_request(queue.clone(), 33, vec![retry_test_message(30)]);
    let process_queue = request.process_queue.clone();
    {
        let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
        scripted
            .broker_results
            .push_back(Err("broker refused send-back".into()));
        scripted
            .retry_results
            .push_back(Err("retry-topic publish failed".into()));
    }
    consumer
        .clone()
        .complete_consume_request(
            request,
            context,
            Some(ConsumeConcurrentlyStatus::ReconsumeLater),
            ConsumeConcurrentlyContext::new(queue.clone()),
        )
        .await;

    let snapshot = process_queue.snapshot(33).unwrap();
    assert_eq!(snapshot.cached_count, 1);
    assert_eq!(snapshot.in_flight_count, 0);
    let redelivery = process_queue.take_batch(33, 1).unwrap().unwrap();
    assert_eq!(redelivery.messages()[0].reconsume_times(), 3);
    assert_eq!(
        offset_store.updates.lock().unwrap().as_slice(),
        &[(queue, 30, true)]
    );
    let _ = runtime.shutdown().await;
}

#[tokio::test]
async fn concurrent_listener_success_commits_the_complete_fixture_batch() {
    let mut config = PushConsumerConfig::new("consume-success").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let consume_service = idle_consume_service();
    let scheduler = Arc::new(PullScheduler::new());
    let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        consume_service,
        scheduler,
        pull_rpc_workers,
    )
    .await;

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let request = consume_request(
        queue.clone(),
        50,
        vec![retry_test_message(60), retry_test_message(61)],
    );
    let process_queue = request.process_queue.clone();
    let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
        |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
            assert_eq!(messages.len(), 2);
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    );
    consumer
        .clone()
        .run_consume_request(request, listener)
        .await;

    let snapshot = process_queue.snapshot(50).unwrap();
    assert_eq!(snapshot.cached_count, 0);
    assert_eq!(snapshot.in_flight_count, 0);
    assert_eq!(
        offset_store.updates.lock().unwrap().as_slice(),
        &[(queue, 62, true)]
    );
    let _ = runtime.shutdown().await;
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn consume_message_hooks_wrap_the_listener_and_share_context_state() {
    struct Hook(Arc<std::sync::Mutex<Vec<String>>>);

    impl ConsumeMessageHook for Hook {
        fn hook_name(&self) -> &str {
            "consume-hook-test"
        }

        fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
            assert!(!context.success);
            assert!(context.status.is_none());
            assert!(context.access_channel.is_none());
            assert_eq!(
                context.consumer_group.as_deref(),
                Some("tenant%consume-hook-lifecycle")
            );
            assert_eq!(context.namespace.as_deref(), Some("tenant"));
            assert_eq!(context.properties, Some(HashMap::new()));
            context
                .properties
                .as_mut()
                .unwrap()
                .insert("hook-state".into(), "retained".into());
            self.0.lock().unwrap().push("before".into());
        }

        fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
            assert!(context.success);
            assert_eq!(context.status.as_deref(), Some("CONSUME_SUCCESS"));
            assert_eq!(
                context.access_channel,
                Some(rocketmq_classic_core::config::AccessChannel::Local)
            );
            assert_eq!(
                context
                    .properties
                    .as_ref()
                    .and_then(|properties| properties.get("hook-state"))
                    .map(String::as_str),
                Some("retained")
            );
            self.0.lock().unwrap().push("after".into());
        }
    }

    let mut config = PushConsumerConfig::new("consume-hook-lifecycle").unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    consumer.register_consume_message_hook(Arc::new(Hook(events.clone())));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let request = consume_request(queue, 60, vec![retry_test_message(70)]);
    let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
        |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
            assert_eq!(messages.len(), 1);
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    );
    consumer
        .clone()
        .run_consume_request(request, listener)
        .await;

    assert_eq!(events.lock().unwrap().as_slice(), ["before", "after"]);
    let _ = runtime.shutdown().await;
    consumer.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn concurrent_consume_timeout_classifies_after_listener_returns_without_cancelling_it() {
    struct Hook(Arc<std::sync::Mutex<Vec<String>>>);

    impl ConsumeMessageHook for Hook {
        fn hook_name(&self) -> &str {
            "consume-timeout-hook"
        }

        fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {
            self.0.lock().unwrap().push("before".into());
        }

        fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
            let return_type = context
                .properties
                .as_ref()
                .and_then(|properties| properties.get("CONSUME_CONTEXT_TYPE"))
                .map(String::as_str)
                .unwrap_or("missing");
            self.0.lock().unwrap().push(format!(
                "after:{}:{}:{return_type}",
                context.status.as_deref().unwrap_or("missing"),
                context.success
            ));
        }
    }

    struct CallbackDrop(Arc<AtomicBool>);

    impl Drop for CallbackDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let listener: Arc<dyn MessageListenerConcurrently> = {
        let started = started.clone();
        let release = release.clone();
        let dropped = dropped.clone();
        Arc::new(
            move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let started = started.clone();
                let release = release.clone();
                let dropped = dropped.clone();
                async move {
                    let _drop_signal = CallbackDrop(dropped);
                    started.notify_one();
                    release.notified().await;
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        )
    };

    let mut config = PushConsumerConfig::new("consume-timeout-hook").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_consume_timeout_minutes(2);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    consumer.register_consume_message_hook(Arc::new(Hook(events.clone())));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        idle_consume_service(),
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;

    let request = consume_request(
        MessageQueue::new("orders", "broker-a", 0),
        61,
        vec![retry_test_message(71)],
    );
    let task = tokio::spawn({
        let consumer = consumer.clone();
        async move { consumer.run_consume_request(request, listener).await }
    });
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .unwrap();

    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::task::yield_now().await;
    consumer.config().set_consume_timeout_minutes(1);
    assert!(
        !dropped.load(Ordering::Acquire),
        "consume timeout must classify a completed callback, not cancel it"
    );
    assert_eq!(events.lock().unwrap().as_slice(), ["before"]);

    release.notify_one();
    task.await.unwrap();
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(
        events.lock().unwrap().as_slice(),
        ["before", "after:CONSUME_SUCCESS:true:TIME_OUT"]
    );
    let _ = runtime.shutdown().await;
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_listener_without_status_sends_fixture_back_for_retry() {
    let mut config = PushConsumerConfig::new("consume-null-status").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    consumer.config().set_max_reconsume_times(2);
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let consume_service = idle_consume_service();
    let scheduler = Arc::new(PullScheduler::new());
    let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        consume_service,
        scheduler,
        pull_rpc_workers,
    )
    .await;
    consumer
        .scripted_retry_transport
        .lock()
        .unwrap()
        .broker_results
        .push_back(Ok(()));

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let request = consume_request(queue.clone(), 51, vec![retry_test_message(70)]);
    let process_queue = request.process_queue.clone();
    let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
        |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move { (None, context) },
    );
    consumer
        .clone()
        .run_consume_request(request, listener)
        .await;

    assert_eq!(process_queue.snapshot(51).unwrap().cached_count, 0);
    {
        let scripted = consumer.scripted_retry_transport.lock().unwrap();
        assert_eq!(scripted.broker_headers.len(), 1);
        assert_eq!(scripted.broker_headers[0].get_offset(), 170);
        assert_eq!(
            scripted.broker_headers[0].get_max_reconsume_times(),
            Some(2)
        );
    }
    assert_eq!(
        offset_store.updates.lock().unwrap().as_slice(),
        &[(queue, 71, true)]
    );
    let _ = runtime.shutdown().await;
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_listener_panic_is_isolated_and_retried() {
    let mut config = PushConsumerConfig::new("consume-listener-panic").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let consume_service = idle_consume_service();
    let scheduler = Arc::new(PullScheduler::new());
    let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        consume_service,
        scheduler,
        pull_rpc_workers,
    )
    .await;
    consumer
        .scripted_retry_transport
        .lock()
        .unwrap()
        .broker_results
        .push_back(Ok(()));

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let request = consume_request(queue.clone(), 52, vec![retry_test_message(80)]);
    let process_queue = request.process_queue.clone();
    let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
        |_messages: Vec<MessageExt>, _context: ConsumeConcurrentlyContext| async move {
            panic!("scripted concurrent-listener panic")
        },
    );
    consumer
        .clone()
        .run_consume_request(request, listener)
        .await;

    assert_eq!(process_queue.snapshot(52).unwrap().cached_count, 0);
    assert_eq!(
        offset_store.updates.lock().unwrap().as_slice(),
        &[(queue, 81, true)]
    );
    let _ = runtime.shutdown().await;
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn private_fixed_assignment_retries_java_fixture_with_broker_limit() {
    let mut config = PushConsumerConfig::new("consume-fixed-assignment").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    consumer.subscribe("orders", "*").await.unwrap();
    consumer.suspend().await;
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let consume_service = idle_consume_service();
    let scheduler = Arc::new(PullScheduler::new());
    let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        consume_service,
        scheduler,
        pull_rpc_workers,
    )
    .await;
    consumer
        .scripted_retry_transport
        .lock()
        .unwrap()
        .broker_results
        .push_back(Ok(()));

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = consumer.activate_process_queue(&queue, 90).await.unwrap();
    let generation = process_queue.generation();
    process_queue
        .insert_messages(generation, vec![retry_test_message(90)])
        .unwrap();
    let batch = process_queue
        .take_batch(generation, usize::MAX)
        .unwrap()
        .unwrap();
    consumer
        .clone()
        .complete_consume_request(
            ConsumeRequest {
                process_queue: process_queue.clone(),
                batch,
            },
            consumer.pull_context().await.unwrap(),
            Some(ConsumeConcurrentlyStatus::ReconsumeLater),
            ConsumeConcurrentlyContext::new(queue.clone()),
        )
        .await;

    assert_eq!(process_queue.snapshot(generation).unwrap().cached_count, 0);
    {
        let scripted = consumer.scripted_retry_transport.lock().unwrap();
        assert_eq!(scripted.broker_headers.len(), 1);
        assert_eq!(scripted.broker_headers[0].get_offset(), 190);
        assert_eq!(
            scripted.broker_headers[0].get_origin_msg_id(),
            Some("broker-msg-90")
        );
        assert_eq!(
            scripted.broker_headers[0].get_max_reconsume_times(),
            Some(16)
        );
    }
    assert_eq!(
        offset_store.updates.lock().unwrap().as_slice(),
        &[(queue.clone(), 90, false), (queue, 91, true)]
    );
    let _ = runtime.shutdown().await;
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn broadcasting_drops_failed_messages_without_clustering_retry() {
    let config = PushConsumerConfig::new("consume-broadcast-failure").unwrap();
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let context = PullContext {
        runtime: runtime.clone(),
        offset_store: offset_store.clone(),
        scheduler: Arc::new(PullScheduler::new()),
        consume_service: ConsumeService::Concurrent(idle_consume_service()),
    };
    let queue = MessageQueue::new("orders", "broker-a", 3);
    let request = consume_request(queue.clone(), 34, vec![retry_test_message(40)]);
    let process_queue = request.process_queue.clone();
    consumer
        .clone()
        .complete_consume_request(
            request,
            context,
            Some(ConsumeConcurrentlyStatus::ReconsumeLater),
            ConsumeConcurrentlyContext::new(queue.clone()),
        )
        .await;

    assert_eq!(process_queue.snapshot(34).unwrap().cached_count, 0);
    {
        let scripted = consumer.scripted_retry_transport.lock().unwrap();
        assert!(scripted.broker_headers.is_empty());
        assert!(scripted.retry_messages.is_empty());
    }
    assert_eq!(
        offset_store.updates.lock().unwrap().as_slice(),
        &[(queue, 41, true)]
    );
    let _ = runtime.shutdown().await;
}

#[tokio::test]
async fn startup_rejects_missing_listener_and_starts_orderly_listener() {
    let consumer = Arc::new(PushConsumer::with_manager(
        PushConsumerConfig::new("missing-listener").unwrap(),
        ClientRuntimeManager::new(),
    ));
    assert!(matches!(
        consumer.start().await,
        Err(PushConsumerError::MissingMessageListener)
    ));
    assert!(!consumer.is_running().await);

    let mut config = PushConsumerConfig::new("orderly-listener").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let consumer = Arc::new(PushConsumer::with_manager(
        config,
        ClientRuntimeManager::new(),
    ));
    consumer
        .register_message_listener(PushMessageListener::Orderly(Arc::new(EmptyOrderlyListener)))
        .await
        .unwrap();
    consumer.start().await.unwrap();
    assert!(consumer.is_running().await);
    assert!(consumer.is_consume_orderly());
    consumer.shutdown().await.unwrap();
}

struct NamedAllocationStrategy(&'static str);

impl crate::rebalance::AllocateMessageQueueStrategy for NamedAllocationStrategy {
    fn allocate(
        &self,
        _consumer_group: &str,
        _current_client_id: &str,
        queues: &[MessageQueue],
        _consumer_ids: &[String],
    ) -> Result<Vec<MessageQueue>, crate::rebalance::RebalanceError> {
        Ok(queues.to_vec())
    }

    fn name(&self) -> &str {
        self.0
    }
}

#[tokio::test]
async fn startup_snapshots_the_current_rebalance_settings() {
    let mut config = PushConsumerConfig::new("rebalance-strategy-snapshot").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let consumer = Arc::new(PushConsumer::with_manager(
        config,
        ClientRuntimeManager::new(),
    ));
    consumer
        .config()
        .set_allocate_message_queue_strategy(Arc::new(NamedAllocationStrategy(
            "configured-before-start",
        )));
    consumer
        .config()
        .set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    consumer
        .register_message_listener(PushMessageListener::concurrently(
            |_messages: Vec<MessageExt>, context| async move {
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        ))
        .await
        .unwrap();

    consumer.start().await.unwrap();
    assert_eq!(
        consumer.rebalance_service.strategy_name(),
        "configured-before-start"
    );
    assert_eq!(
        consumer.rebalance_service.message_model_for_test(),
        rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting
    );

    consumer
        .config()
        .set_allocate_message_queue_strategy(Arc::new(NamedAllocationStrategy(
            "configured-after-start",
        )));
    consumer
        .config()
        .set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
    assert_eq!(
        consumer.rebalance_service.strategy_name(),
        "configured-before-start",
        "Java installs the configured strategy into RebalanceImpl during start"
    );
    assert_eq!(
        consumer.rebalance_service.message_model_for_test(),
        rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting,
        "Java keeps RebalanceImpl's message-model snapshot after start"
    );
    assert_eq!(
        consumer.message_model(),
        rocketmq_classic_protocol::heartbeat::MessageModel::Clustering,
        "the facade getter still reflects the live consumer setting"
    );
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_registers_push_consumer_and_shutdown_releases_it() {
    let mut config = PushConsumerConfig::new("push-lifecycle").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    consumer
        .register_message_listener(PushMessageListener::concurrently(
            |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
                (
                    Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                    context,
                )
            },
        ))
        .await
        .unwrap();
    consumer.subscribe("orders", "*").await.unwrap();
    consumer.subscribe("temporary", "*").await.unwrap();
    assert!(!consumer.unsubscribe("temporary").await.unwrap());
    assert!(consumer.unsubscribe("tenant%temporary").await.unwrap());
    assert!(!consumer.unsubscribe("tenant%temporary").await.unwrap());
    let subscription_table = consumer.get_subscription_inner();
    {
        let subscriptions = read_lock(&subscription_table);
        assert_eq!(
            subscriptions
                .get("tenant%orders")
                .and_then(|subscription| subscription.sub_string.as_deref()),
            Some("*")
        );
    }

    consumer.start().await.unwrap();
    assert!(consumer.is_running().await);
    let (consume_service, pull_scheduler, pull_rpc_workers) = match &*consumer.state.lock().await {
        State::Started {
            consume_service,
            pull_scheduler,
            pull_rpc_workers,
            ..
        } => (
            consume_service.clone(),
            pull_scheduler.clone(),
            pull_rpc_workers.clone(),
        ),
        _ => panic!("consumer should own a started consume service"),
    };
    assert_eq!(consume_service.worker_count(), 20);
    assert_eq!(pull_scheduler.worker_count(), 1);
    // Owned dispatch admits one task per in-flight pull; with no queue
    // assignment yet there are no admitted requests, not four idle workers.
    assert_eq!(pull_rpc_workers.active_admissions(), 0);
    assert!(consumer.update_core_pool_size(4).await.is_ok());
    assert_eq!(consume_service.worker_count(), 4);
    assert!(consumer.update_core_pool_size(20).await.is_ok());
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    let registration = runtime
        .select_consumer("tenant%push-lifecycle")
        .unwrap()
        .expect("started consumer should be registered");
    let heartbeat_topics = registration
        .heartbeat_data()
        .subscription_data_set
        .expect("push consumers publish subscriptions")
        .snapshot()
        .into_iter()
        .flatten()
        .filter_map(|subscription| subscription.snapshot().topic)
        .collect::<std::collections::BTreeSet<_>>();
    assert!(heartbeat_topics.contains("tenant%orders"));
    assert!(heartbeat_topics.contains("%RETRY%tenant%push-lifecycle"));
    consumer.subscribe("late", "*").await.unwrap();
    assert!(
        runtime
            .select_consumer("tenant%push-lifecycle")
            .unwrap()
            .unwrap()
            .heartbeat_data()
            .subscription_data_set
            .unwrap()
            .snapshot()
            .iter()
            .any(|subscription| {
                subscription.as_ref().is_some_and(|subscription| {
                    subscription.snapshot().topic.as_deref() == Some("tenant%late")
                })
            })
    );
    let running_offset_store = consumer.get_offset_store();
    assert!(running_offset_store.is_some());
    consumer.set_offset_store(None).await.unwrap();
    assert!(consumer.get_offset_store().is_none());
    consumer
        .set_offset_store(running_offset_store)
        .await
        .unwrap();
    assert!(consumer.get_offset_store().is_some());
    assert!(matches!(
        consumer.start().await,
        Err(PushConsumerError::AlreadyStarted)
    ));
    assert!(consumer.unsubscribe("tenant%orders").await.unwrap());

    consumer.shutdown().await.unwrap();
    assert!(!consumer.is_running().await);
    assert_eq!(consume_service.worker_count(), 0);
    assert_eq!(pull_scheduler.worker_count(), 0);
    assert_eq!(pull_rpc_workers.active_admissions(), 0);
    assert!(matches!(
        consumer.start().await,
        Err(PushConsumerError::Shutdown)
    ));
}

#[tokio::test]
async fn update_core_pool_size_matches_java_void_and_lifecycle_semantics() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    assert_eq!(fixture["updateCorePoolSizeReturnType"], "void");
    assert_eq!(
        fixture["updateCorePoolSizeNullServiceThrowsNullPointerException"],
        true
    );
    assert_eq!(
        fixture["concurrentRaisedMaxUpdateThrowsIllegalArgumentException"],
        true
    );
    assert_eq!(
        fixture["orderlyRaisedMaxUpdateThrowsIllegalArgumentException"],
        true
    );
    assert_eq!(
        fixture["concurrentShutdownRaisedMaxUpdateThrowsIllegalArgumentException"],
        true
    );
    assert_eq!(
        fixture["orderlyShutdownRaisedMaxUpdateThrowsIllegalArgumentException"],
        true
    );

    fn config(
        group: &str,
        model: rocketmq_classic_protocol::heartbeat::MessageModel,
    ) -> PushConsumerConfig {
        let mut config = PushConsumerConfig::new(group).unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_consume_thread_min(2);
        config.set_consume_thread_max(8);
        config.set_message_model(model);
        config
    }

    let created = PushConsumer::new(config(
        "push-core-pool-created",
        rocketmq_classic_protocol::heartbeat::MessageModel::Clustering,
    ));
    assert!(matches!(
        created.update_core_pool_size(4).await,
        Err(PushConsumerError::NotStarted)
    ));
    assert!(matches!(
        created.update_core_pool_size(0).await,
        Err(PushConsumerError::NotStarted)
    ));

    let concurrent_manager = ClientRuntimeManager::new();
    let concurrent = Arc::new(PushConsumer::with_manager(
        config(
            "push-core-pool-concurrent",
            rocketmq_classic_protocol::heartbeat::MessageModel::Clustering,
        ),
        concurrent_manager.clone(),
    ));
    let concurrent_listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
        |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    );
    let concurrent_service = Arc::new(ConcurrentConsumeService::new(2, 8, concurrent_listener));
    let concurrent_offset_store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let concurrent_runtime = install_started_consumer_with_service(
        &concurrent,
        &concurrent_manager,
        concurrent_offset_store,
        ConsumeService::Concurrent(concurrent_service.clone()),
        None,
        Arc::new(PullScheduler::new()),
        Arc::new(PullRpcWorkers::new()),
    )
    .await;
    assert_eq!(
        concurrent_service.worker_count(),
        fixture["concurrentCorePoolSizeBefore"]
    );
    assert!(concurrent.update_core_pool_size(6).await.is_ok());
    assert_eq!(
        concurrent_service.worker_count(),
        fixture["concurrentCorePoolSizeAfterValid"]
    );
    assert!(concurrent.update_core_pool_size(8).await.is_ok());
    assert!(concurrent.update_core_pool_size(0).await.is_ok());
    assert_eq!(
        concurrent_service.worker_count(),
        fixture["concurrentCorePoolSizeAfterInvalid"]
    );
    concurrent.config().set_consume_thread_max(10);
    assert!(matches!(
        concurrent.update_core_pool_size(9).await,
        Err(PushConsumerError::InvalidSetting {
            name: "corePoolSize",
            value: 9,
            minimum: 1,
            maximum: 8,
        })
    ));
    assert_eq!(
        concurrent_service.worker_count(),
        fixture["concurrentCorePoolSizeAfterRaisedMaxFailure"]
    );
    assert!(concurrent.update_core_pool_size(8).await.is_ok());
    assert_eq!(
        concurrent_service.worker_count(),
        fixture["concurrentCorePoolSizeAtExecutorMaximum"]
    );
    concurrent.shutdown().await.unwrap();
    assert_eq!(
        concurrent_service.worker_count(),
        0,
        "shutdown retires all Rust workers"
    );
    assert!(matches!(
        concurrent.update_core_pool_size(9).await,
        Err(PushConsumerError::InvalidSetting {
            name: "corePoolSize",
            value: 9,
            minimum: 1,
            maximum: 8,
        })
    ));
    assert!(concurrent.update_core_pool_size(7).await.is_ok());
    assert_eq!(
        concurrent.consume_core_pool_size.load(Ordering::Acquire),
        fixture["concurrentCorePoolSizeAfterShutdownUpdate"]
    );
    drop(concurrent_runtime);

    let orderly_manager = ClientRuntimeManager::new();
    let orderly = Arc::new(PushConsumer::with_manager(
        config(
            "push-core-pool-orderly",
            rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting,
        ),
        orderly_manager.clone(),
    ));
    let orderly_listener: Arc<dyn MessageListenerOrderly> = Arc::new(
        |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
            (Some(ConsumeOrderlyStatus::Success), context)
        },
    );
    let orderly_offset_store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let orderly_runtime = install_started_orderly_consumer(
        &orderly,
        &orderly_manager,
        orderly_offset_store,
        orderly_listener,
        None,
    )
    .await;
    let orderly_service = match &*orderly.state.lock().await {
        State::Started {
            consume_service, ..
        } => consume_service.clone(),
        _ => panic!("orderly consumer should own a started consume service"),
    };
    assert_eq!(
        orderly_service.worker_count(),
        fixture["orderlyCorePoolSizeBefore"]
    );
    assert!(orderly.update_core_pool_size(6).await.is_ok());
    assert_eq!(
        orderly_service.worker_count(),
        fixture["orderlyCorePoolSizeAfterValid"]
    );
    assert!(orderly.update_core_pool_size(8).await.is_ok());
    assert!(orderly.update_core_pool_size(-1).await.is_ok());
    assert_eq!(
        orderly_service.worker_count(),
        fixture["orderlyCorePoolSizeAfterInvalid"]
    );
    orderly.config().set_consume_thread_max(10);
    assert!(matches!(
        orderly.update_core_pool_size(9).await,
        Err(PushConsumerError::InvalidSetting {
            name: "corePoolSize",
            value: 9,
            minimum: 1,
            maximum: 8,
        })
    ));
    assert_eq!(
        orderly_service.worker_count(),
        fixture["orderlyCorePoolSizeAfterRaisedMaxFailure"]
    );
    assert!(orderly.update_core_pool_size(8).await.is_ok());
    assert_eq!(
        orderly_service.worker_count(),
        fixture["orderlyCorePoolSizeAtExecutorMaximum"]
    );
    orderly.shutdown().await.unwrap();
    assert_eq!(
        orderly_service.worker_count(),
        0,
        "shutdown retires all Rust workers"
    );
    assert!(matches!(
        orderly.update_core_pool_size(9).await,
        Err(PushConsumerError::InvalidSetting {
            name: "corePoolSize",
            value: 9,
            minimum: 1,
            maximum: 8,
        })
    ));
    assert!(orderly.update_core_pool_size(7).await.is_ok());
    assert_eq!(
        orderly.consume_core_pool_size.load(Ordering::Acquire),
        fixture["orderlyCorePoolSizeAfterShutdownUpdate"]
    );
    drop(orderly_runtime);
}

#[tokio::test]
async fn push_shutdown_unregisters_the_current_java_group() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/runtime/heartbeat.json"
    ))
    .unwrap();
    let expected_retained =
        fixture["pushGroupNameMutation"]["originalRegistrationRemainsAfterCurrentGroupUnregister"]
            .as_bool()
            .unwrap();

    let mut config = PushConsumerConfig::new("push-shutdown-group-before").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let listener: Arc<dyn super::super::listener::MessageListenerConcurrently> = Arc::new(
        |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
            (
                Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                context,
            )
        },
    );
    let consume_service = Arc::new(ConcurrentConsumeService::new(1, 1, listener));
    let pull_scheduler = Arc::new(PullScheduler::new());
    let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
    let runtime = install_started_consumer(
        &consumer,
        &manager,
        offset_store,
        consume_service,
        pull_scheduler,
        pull_rpc_workers,
    )
    .await;

    assert!(
        runtime
            .select_consumer("push-shutdown-group-before")
            .unwrap()
            .is_some()
    );
    consumer.set_consumer_group(Some("push-shutdown-group-after".into()));
    consumer.shutdown().await.unwrap();

    assert!(
        runtime
            .select_consumer("push-shutdown-group-after")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        runtime
            .select_consumer("push-shutdown-group-before")
            .unwrap()
            .is_some(),
        expected_retained
    );
}

#[tokio::test]
async fn duplicate_group_registration_rolls_back_without_running_state() {
    let manager = ClientRuntimeManager::new();
    let make_consumer = || {
        let mut config = PushConsumerConfig::new("same-group").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        Arc::new(PushConsumer::with_manager(config, manager.clone()))
    };
    let first = make_consumer();
    let second = make_consumer();
    let listener = || {
        PushMessageListener::concurrently(
            |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
                (
                    Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                    context,
                )
            },
        )
    };
    first.register_message_listener(listener()).await.unwrap();
    second.register_message_listener(listener()).await.unwrap();

    first.start().await.unwrap();
    assert_eq!(first.service_state(), Some(ConsumerServiceState::Running));
    assert!(matches!(
        second.start().await,
        Err(PushConsumerError::Runtime(_))
    ));
    assert_eq!(
        second.service_state(),
        Some(ConsumerServiceState::CreateJust)
    );
    assert!(!second.is_running().await);
    assert!(first.is_running().await);
    first.shutdown().await.unwrap();
    assert_eq!(
        first.service_state(),
        Some(ConsumerServiceState::ShutdownAlready)
    );
    assert!(!first.is_running().await);
}

#[tokio::test]
async fn startup_rolls_back_each_runtime_registration_and_route_stage() {
    for stage in StartupStage::all() {
        let mut config = PushConsumerConfig::new(format!("rollback-{}", stage.name())).unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        consumer
            .register_message_listener(PushMessageListener::concurrently(
                |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
                    (
                        Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                        context,
                    )
                },
            ))
            .await
            .unwrap();
        *consumer
            .startup_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(stage);

        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::TestStartupFault(_))
        ));
        assert!(!consumer.is_running().await);
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        assert!(
            runtime
                .select_consumer(consumer.config().consumer_group().as_deref().unwrap())
                .unwrap()
                .is_none()
        );
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn factory_start_failure_preserves_java_push_consumer_registration() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/lifecycle.json"
    ))
    .unwrap();
    let java = &fixture["pushFactoryStartFailure"];
    assert_eq!(java["exception"], "IllegalArgumentException");
    assert_eq!(java["consumerRegistered"], true);
    assert_eq!(java["consumerServiceState"], "START_FAILED");
    assert_eq!(java["factoryServiceState"], "START_FAILED");

    let group = "push-factory-start-failure";
    let mut config = PushConsumerConfig::new(group).unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(String::new()));
    config.client_config_mut().set_poll_name_server_interval(0);
    let manager = ClientRuntimeManager::new();
    let client_config = config.client_config().clone();
    let runtime = manager.get_or_create(&client_config).await.unwrap();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    consumer
        .register_message_listener(PushMessageListener::concurrently(
            |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
                (
                    Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                    context,
                )
            },
        ))
        .await
        .unwrap();

    assert!(matches!(
        consumer.start().await,
        Err(PushConsumerError::Runtime(
            rocketmq_classic_core::runtime::RuntimeError::InvalidSchedule
        ))
    ));
    assert_eq!(
        consumer.service_state(),
        Some(ConsumerServiceState::StartFailed)
    );
    assert!(!consumer.is_running().await);
    assert_eq!(
        runtime.state(),
        rocketmq_classic_core::runtime::RuntimeState::Failed
    );
    assert!(runtime.select_consumer(group).unwrap().is_some());
    assert!(Arc::ptr_eq(
        &runtime,
        &manager.get_or_create(&client_config).await.unwrap()
    ));
    assert!(
        runtime
            .unregister_consumer_by_group(
                group,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap()
    );
}

struct EmptyOrderlyListener;
impl super::super::listener::MessageListenerOrderly for EmptyOrderlyListener {
    fn consume_message(
        &self,
        _messages: Vec<rocketmq_classic_common::message::MessageExt>,
        context: super::super::listener::ConsumeOrderlyContext,
    ) -> super::super::listener::OrderlyListenerFuture {
        Box::pin(async move {
            (
                Some(super::super::listener::ConsumeOrderlyStatus::Success),
                context,
            )
        })
    }
}

#[test]
fn clustering_start_injects_retry_subscription_and_broadcast_does_not() {
    let clustering = PushConsumer::with_manager(
        PushConsumerConfig::new("orders").unwrap(),
        ClientRuntimeManager::new(),
    );
    clustering.copy_subscriptions("orders").unwrap();
    assert!(read_lock(&clustering.get_subscription_inner()).contains_key("%RETRY%orders"));

    let config = PushConsumerConfig::new("broadcast-orders").unwrap();
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    let broadcasting = PushConsumer::with_manager(config, ClientRuntimeManager::new());
    broadcasting.copy_subscriptions("broadcast-orders").unwrap();
    assert!(
        !read_lock(&broadcasting.get_subscription_inner())
            .keys()
            .any(|topic| topic.starts_with("%RETRY%"))
    );
}

#[tokio::test]
async fn class_filter_subscription_matches_java_facade_and_heartbeat_flow() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    let mut config = PushConsumerConfig::new("phase18-class-filter-subscribe").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));

    let before_class = fixture["classFilterBeforeStartClassName"].as_str().unwrap();
    let before_source = fixture["classFilterBeforeStartSource"].as_str().unwrap();
    consumer
        .subscribe_with_class_filter("class-filter-orders", before_class, before_source)
        .await
        .unwrap();

    let subscription_table = consumer.get_subscription_inner();
    let before_start = read_lock(&subscription_table)
        .get("tenant%class-filter-orders")
        .cloned()
        .expect("class-filter subscription is stored before start");
    assert_eq!(
        before_start.get_topic(),
        fixture["classFilterBeforeStartTopic"].as_str()
    );
    assert_eq!(
        before_start.get_sub_string(),
        fixture["classFilterBeforeStartClassName"].as_str()
    );
    assert_eq!(
        before_start.is_class_filter_mode(),
        fixture["classFilterBeforeStartMode"].as_bool().unwrap()
    );
    assert_eq!(
        before_start.get_filter_class_source(),
        fixture["classFilterBeforeStartSource"].as_str()
    );
    let before_wire = serde_json::to_value(&before_start).unwrap();
    assert_eq!(before_wire["topic"], fixture["classFilterBeforeStartTopic"]);
    assert_eq!(
        before_wire["subString"],
        fixture["classFilterBeforeStartClassName"]
    );
    assert_eq!(
        before_wire["classFilterMode"],
        fixture["classFilterBeforeStartMode"]
    );
    assert!(before_wire.get("filterClassSource").is_none());
    assert_eq!(
        fixture["classFilterSourceSerialized"],
        serde_json::json!(false)
    );

    consumer
        .register_message_listener(PushMessageListener::concurrently(
            |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
                (
                    Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                    context,
                )
            },
        ))
        .await
        .unwrap();
    consumer.start().await.unwrap();
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    let class_topic = fixture["classFilterBeforeStartTopic"].as_str().unwrap();
    let registration = runtime
        .select_consumer("tenant%phase18-class-filter-subscribe")
        .unwrap()
        .expect("started consumer is registered");
    let heartbeat_subscription = registration
        .heartbeat_data()
        .subscription_data_set
        .expect("Push heartbeat carries subscriptions")
        .snapshot()
        .into_iter()
        .flatten()
        .find_map(|subscription| {
            let subscription = subscription.snapshot();
            (subscription.topic.as_deref() == Some(class_topic)).then_some(subscription)
        })
        .expect("class-filter subscription is present in heartbeat state");
    assert_eq!(
        heartbeat_subscription.sub_string.as_deref(),
        fixture["classFilterBeforeStartClassName"].as_str()
    );
    assert_eq!(
        heartbeat_subscription.filter_class_source.as_deref(),
        fixture["classFilterBeforeStartSource"].as_str()
    );

    let after_class = fixture["classFilterAfterFactoryClassName"]
        .as_str()
        .unwrap();
    let after_source = fixture["classFilterAfterFactorySource"].as_str().unwrap();
    consumer
        .subscribe_with_class_filter("class-filter-orders", after_class, after_source)
        .await
        .unwrap();
    let updated_registration = runtime
        .select_consumer("tenant%phase18-class-filter-subscribe")
        .unwrap()
        .expect("started consumer remains registered");
    let updated_heartbeat_subscription = updated_registration
        .heartbeat_data()
        .subscription_data_set
        .expect("Push heartbeat carries subscriptions")
        .snapshot()
        .into_iter()
        .flatten()
        .find_map(|subscription| {
            let subscription = subscription.snapshot();
            (subscription.topic.as_deref() == Some(class_topic)).then_some(subscription)
        })
        .expect("class-filter update reaches heartbeat state");
    assert_eq!(
        updated_heartbeat_subscription.topic.as_deref(),
        fixture["classFilterAfterFactoryTopic"].as_str()
    );
    assert_eq!(
        updated_heartbeat_subscription.sub_string.as_deref(),
        fixture["classFilterAfterFactoryClassName"].as_str()
    );
    assert_eq!(
        updated_heartbeat_subscription.class_filter_mode,
        fixture["classFilterAfterFactoryMode"].as_bool().unwrap()
    );
    assert_eq!(
        updated_heartbeat_subscription
            .filter_class_source
            .as_deref(),
        fixture["classFilterAfterFactorySource"].as_str()
    );
    assert_eq!(
        fixture["classFilterAfterFactoryHeartbeatCalls"],
        serde_json::json!(1)
    );
    let mut rust_wire = serde_json::to_value(&updated_heartbeat_subscription).unwrap();
    let mut java_wire = fixture["classFilterAfterFactoryWireData"].clone();
    assert!(rust_wire.get("filterClassSource").is_none());
    rust_wire.as_object_mut().unwrap().remove("subVersion");
    java_wire.as_object_mut().unwrap().remove("subVersion");
    assert_eq!(rust_wire, java_wire);

    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn legacy_subscription_map_is_namespaced_and_copied_into_heartbeat_state() {
    let mut config = PushConsumerConfig::new("orders").unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    let consumer = PushConsumer::with_manager(config, ClientRuntimeManager::new());
    consumer
        .set_subscription(HashMap::from([(
            "orders".to_owned(),
            "gold || silver".to_owned(),
        )]))
        .await
        .unwrap();

    assert_eq!(
        consumer.get_subscription().get("tenant%orders"),
        Some(&"gold || silver".to_owned())
    );
    consumer.copy_subscriptions("tenant%orders").unwrap();
    assert_eq!(
        read_lock(&consumer.get_subscription_inner())
            .get("tenant%orders")
            .and_then(SubscriptionData::get_sub_string),
        Some("gold || silver")
    );
}

#[test]
fn retry_and_dlq_messages_restore_logical_topics_for_orderly_and_pop() {
    let mut config = PushConsumerConfig::new("namespace-consumer").unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    let consumer = PushConsumer::with_manager(config, ClientRuntimeManager::new());
    let mut client_config = consumer.config.client_config().clone();
    let namespaced_group = client_config
        .with_namespace(consumer.config.consumer_group().as_deref())
        .unwrap()
        .unwrap();

    let make_messages = || {
        let mut retry = retry_test_message(1);
        retry.set_topic(format!("%RETRY%{namespaced_group}"));
        retry
            .set_system_property(PROPERTY_RETRY_TOPIC, "tenant%orders")
            .unwrap();

        let mut dlq = retry_test_message(2);
        dlq.set_topic(format!(
            "%DLQ%tenant%{}",
            consumer.config().consumer_group().unwrap()
        ));

        let mut normal = retry_test_message(3);
        normal.set_topic("tenant%orders");
        vec![retry, dlq, normal]
    };

    let mut orderly_messages = make_messages();
    consumer.reset_retry_and_namespace_for_group(&mut orderly_messages, &namespaced_group);
    assert_eq!(
        orderly_messages
            .iter()
            .map(|message| message.topic())
            .collect::<Vec<_>>(),
        ["orders", "%DLQ%namespace-consumer", "orders"]
    );

    let mut pop_messages = make_messages();
    consumer.reset_retry_and_namespace_for_pop(&mut pop_messages, &namespaced_group);
    assert_eq!(
        pop_messages
            .iter()
            .map(|message| message.topic())
            .collect::<Vec<_>>(),
        ["orders", "%DLQ%namespace-consumer", "orders"]
    );
}

#[tokio::test]
async fn shutdown_aborts_a_slow_listener_without_committing_its_batch() {
    struct NotifyOnDrop(Arc<tokio::sync::Notify>);
    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let cancelled = Arc::new(tokio::sync::Notify::new());
    let listener: Arc<dyn MessageListenerConcurrently> = {
        let started = started.clone();
        let release = release.clone();
        let cancelled = cancelled.clone();
        Arc::new(
            move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let started = started.clone();
                let release = release.clone();
                let cancelled = cancelled.clone();
                async move {
                    let _drop_signal = NotifyOnDrop(cancelled);
                    started.notify_one();
                    release.notified().await;
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        )
    };
    let mut config = PushConsumerConfig::new("slow-consumer-shutdown").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_consume_thread_min(1);
    config.set_consume_thread_max(1);
    config.set_await_termination_millis_when_shutdown(10);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let consume_service = Arc::new(ConcurrentConsumeService::new(1, 1, listener));
    let scheduler = Arc::new(PullScheduler::new());
    let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
    let _runtime = install_started_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        consume_service.clone(),
        scheduler.clone(),
        pull_rpc_workers.clone(),
    )
    .await;

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 40));
    process_queue
        .insert_messages(40, vec![retry_test_message(50)])
        .unwrap();
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    offset_store.update_offset(&queue, 50, false);
    let context = consumer.pull_context().await.unwrap();
    consumer.submit_consume_batch(&context, process_queue.clone(), 40);
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .unwrap();

    consumer.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), cancelled.notified())
        .await
        .unwrap();
    let snapshot = process_queue.snapshot(40).unwrap();
    assert!(snapshot.dropped);
    assert_eq!(snapshot.cached_count, 1);
    assert_eq!(snapshot.in_flight_count, 1);
    assert_eq!(offset_store.values.lock().unwrap().get(&queue), Some(&50));
    assert_eq!(consume_service.worker_count(), 0);
    assert_eq!(scheduler.worker_count(), 0);
    assert_eq!(pull_rpc_workers.active_admissions(), 0);
}

#[tokio::test(start_paused = true)]
async fn pop_shutdown_wait_budget_is_independent_of_worker_count() {
    async fn elapsed_for(worker_count: usize) -> Duration {
        let handles = (0..worker_count)
            .map(|_| tokio::spawn(std::future::pending::<()>()))
            .collect();
        tokio::task::yield_now().await;
        let started = Instant::now();
        shutdown_pop_worker_handles(handles, 100).await;
        started.elapsed()
    }

    let one_worker = elapsed_for(1).await;
    let twenty_workers = elapsed_for(20).await;
    let service_budget = Duration::from_millis(200);
    assert!(
        one_worker <= service_budget,
        "one worker waited {one_worker:?}"
    );
    assert!(
        twenty_workers <= service_budget,
        "twenty workers waited {twenty_workers:?}"
    );
    assert!(
        twenty_workers <= one_worker + Duration::from_millis(10),
        "wait grew with worker count: one={one_worker:?}, twenty={twenty_workers:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_with_explicit_wait_overrides_configured_wait() {
    struct NotifyOnDrop(Arc<tokio::sync::Notify>);
    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    let started = Arc::new(tokio::sync::Notify::new());
    let completed = Arc::new(AtomicBool::new(false));
    let cancelled = Arc::new(tokio::sync::Notify::new());
    let listener: Arc<dyn MessageListenerConcurrently> = {
        let started = started.clone();
        let completed = completed.clone();
        let cancelled = cancelled.clone();
        Arc::new(
            move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let started = started.clone();
                let completed = completed.clone();
                let cancelled = cancelled.clone();
                async move {
                    let _drop_signal = NotifyOnDrop(cancelled);
                    started.notify_one();
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    completed.store(true, Ordering::Release);
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        )
    };
    let mut config = PushConsumerConfig::new("shutdown-explicit-wait-override").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_consume_thread_min(1);
    config.set_consume_thread_max(1);
    config.set_await_termination_millis_when_shutdown(0);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let consume_service = Arc::new(ConcurrentConsumeService::new(1, 1, listener));
    let scheduler = Arc::new(PullScheduler::new());
    let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
    let _runtime = install_started_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        consume_service.clone(),
        scheduler.clone(),
        pull_rpc_workers.clone(),
    )
    .await;

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 40));
    process_queue
        .insert_messages(40, vec![retry_test_message(50)])
        .unwrap();
    write_lock(&consumer.process_queues).insert(queue, process_queue.clone());
    consumer.submit_consume_batch(&consumer.pull_context().await.unwrap(), process_queue, 40);
    started.notified().await;

    let shutdown_consumer = consumer.clone();
    let shutdown = tokio::spawn(async move {
        shutdown_consumer
            .shutdown_with_await_termination_millis(100)
            .await
            .unwrap();
    });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    tokio::time::advance(Duration::from_millis(50)).await;
    shutdown.await.unwrap();

    assert!(completed.load(Ordering::Acquire));
    assert_eq!(consume_service.worker_count(), 0);
    assert_eq!(scheduler.worker_count(), 0);
    assert_eq!(pull_rpc_workers.active_admissions(), 0);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), cancelled.notified())
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn orderly_batch_lock_applies_partial_results_grouped_by_broker() {
    let mut config = PushConsumerConfig::new("orderly-partial-lock").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    let queues = [
        MessageQueue::new("orders", "broker-a", 0),
        MessageQueue::new("orders", "broker-a", 1),
        MessageQueue::new("orders", "broker-b", 0),
    ];
    let process_queues = queues
        .iter()
        .enumerate()
        .map(|(index, queue)| {
            let process_queue = Arc::new(ProcessQueue::new_for_assignment(
                queue.clone(),
                80 + index as u64,
            ));
            if index == 1 {
                process_queue
                    .set_lock_state(81, true, epoch_millis())
                    .unwrap();
            }
            process_queue
        })
        .collect::<Vec<_>>();
    {
        let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
        scripted.lock_results.extend([
            Ok(Some(HashSet::from([Some(queues[0].clone())]))),
            Ok(Some(HashSet::new())),
        ]);
    }

    let locked = consumer
        .lock_orderly_process_queues(&runtime, &process_queues)
        .await;
    assert_eq!(locked, HashSet::from([queues[0].clone()]));
    assert!(process_queues[0].is_lock_fresh(80, 30_000).unwrap());
    assert!(!process_queues[1].snapshot(81).unwrap().locked);
    assert!(!process_queues[2].snapshot(82).unwrap().locked);

    {
        let scripted = consumer.scripted_lock_transport.lock().unwrap();
        assert_eq!(scripted.lock_bodies.len(), 2);
        let mut grouped_queue_counts = scripted
            .lock_bodies
            .iter()
            .map(|body| body.get_mq_set().unwrap().with(|set| set.len()))
            .collect::<Vec<_>>();
        grouped_queue_counts.sort_unstable();
        assert_eq!(grouped_queue_counts, vec![1, 2]);
    }
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn orderly_lock_requests_keep_the_rebalance_group_snapshot() {
    let group = "phase18-consume-service-group";
    let mut config = PushConsumerConfig::new(group).unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
        |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
            (Some(ConsumeOrderlyStatus::Success), context)
        },
    );
    let store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let runtime =
        install_started_orderly_consumer(&consumer, &manager, store, listener, None).await;
    consumer.set_consumer_group(Some("phase18-consume-service-group-mutated".into()));

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    assert_eq!(fixture["rebalanceConsumerGroupAfterFacadeMutation"], group);
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 195));
    consumer
        .scripted_lock_transport
        .lock()
        .unwrap()
        .lock_results
        .push_back(Ok(Some(HashSet::from([Some(queue.clone())]))));

    assert_eq!(
        consumer
            .lock_orderly_process_queues(&runtime, &[process_queue])
            .await,
        HashSet::from([queue])
    );
    assert_eq!(
        consumer.scripted_lock_transport.lock().unwrap().lock_bodies[0].get_consumer_group(),
        fixture["rebalanceConsumerGroupAfterFacadeMutation"].as_str()
    );

    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn orderly_lock_errors_and_nullable_sets_preserve_previous_state() {
    let mut config = PushConsumerConfig::new("orderly-lock-uncertain").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue, 83));
    process_queue
        .set_lock_state(83, true, epoch_millis())
        .unwrap();
    let queues = std::slice::from_ref(&process_queue);

    consumer
        .scripted_lock_transport
        .lock()
        .unwrap()
        .lock_results
        .push_back(Err("scripted timeout".into()));
    consumer.lock_orderly_process_queues(&runtime, queues).await;
    assert!(process_queue.snapshot(83).unwrap().locked);
    assert!(process_queue.is_lock_fresh(83, 30_000).unwrap());

    consumer
        .scripted_lock_transport
        .lock()
        .unwrap()
        .lock_results
        .push_back(Ok(None));
    consumer.lock_orderly_process_queues(&runtime, queues).await;
    assert!(process_queue.snapshot(83).unwrap().locked);
    assert!(process_queue.is_lock_fresh(83, 30_000).unwrap());

    consumer
        .scripted_lock_transport
        .lock()
        .unwrap()
        .lock_results
        .push_back(Ok(Some(HashSet::new())));
    consumer.lock_orderly_process_queues(&runtime, queues).await;
    assert!(!process_queue.snapshot(83).unwrap().locked);
    assert!(!process_queue.is_lock_fresh(83, 30_000).unwrap());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn older_orderly_renewal_response_cannot_overwrite_the_newer_attempt() {
    let mut config = PushConsumerConfig::new("orderly-lock-race").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 84));
    let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
    let (second_sender, second_receiver) = tokio::sync::oneshot::channel();
    let requested = {
        let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
        scripted
            .lock_waiters
            .extend([first_receiver, second_receiver]);
        scripted.lock_requested.clone()
    };

    let first_notice = requested.notified();
    let first_consumer = consumer.clone();
    let first_runtime = runtime.clone();
    let first_queue = process_queue.clone();
    let first = tokio::spawn(async move {
        first_consumer
            .lock_orderly_process_queues(&first_runtime, &[first_queue])
            .await
    });
    first_notice.await;

    let second_notice = requested.notified();
    let second_consumer = consumer.clone();
    let second_runtime = runtime.clone();
    let second_queue = process_queue.clone();
    let second = tokio::spawn(async move {
        second_consumer
            .lock_orderly_process_queues(&second_runtime, &[second_queue])
            .await
    });
    second_notice.await;

    first_sender
        .send(Ok(Some(HashSet::from([Some(queue.clone())]))))
        .unwrap();
    assert!(first.await.unwrap().is_empty());
    assert!(!process_queue.snapshot(84).unwrap().locked);

    second_sender
        .send(Ok(Some(HashSet::from([Some(queue.clone())]))))
        .unwrap();
    assert_eq!(second.await.unwrap(), HashSet::from([queue]));
    assert!(process_queue.snapshot(84).unwrap().locked);
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn orderly_batch_lock_age_starts_at_each_broker_response() {
    let mut config = PushConsumerConfig::new("orderly-lock-response-time").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    let queue_a = MessageQueue::new("orders", "broker-a", 0);
    let queue_b = MessageQueue::new("orders", "broker-b", 0);
    let process_queue_a = Arc::new(ProcessQueue::new_for_assignment(queue_a.clone(), 94));
    let process_queue_b = Arc::new(ProcessQueue::new_for_assignment(queue_b.clone(), 95));
    let (response_a, waiter_a) = tokio::sync::oneshot::channel();
    let (response_b, waiter_b) = tokio::sync::oneshot::channel();
    let requested = {
        let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
        scripted.lock_waiters.extend([waiter_a, waiter_b]);
        scripted.lock_requested.clone()
    };

    let first_request = requested.notified();
    let task_consumer = consumer.clone();
    let task_runtime = runtime.clone();
    let task_queues = [process_queue_a.clone(), process_queue_b.clone()];
    let lock_task = tokio::spawn(async move {
        task_consumer
            .lock_orderly_process_queues(&task_runtime, &task_queues)
            .await
    });
    first_request.await;
    response_a
        .send(Ok(Some(HashSet::from([Some(queue_a.clone())]))))
        .unwrap();

    let second_request = requested.notified();
    second_request.await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    response_b
        .send(Ok(Some(HashSet::from([Some(queue_b.clone())]))))
        .unwrap();

    assert_eq!(
        lock_task.await.unwrap(),
        HashSet::from([queue_a.clone(), queue_b.clone()])
    );
    assert!(!process_queue_a.is_lock_fresh(94, 30_000).unwrap());
    assert!(process_queue_b.is_lock_fresh(95, 30_000).unwrap());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn late_orderly_lock_for_dropped_generation_is_unlocked() {
    let mut config = PushConsumerConfig::new("orderly-dropped-lock-race").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let runtime = manager
        .get_or_create(consumer.config().client_config())
        .await
        .unwrap();
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 85));
    let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
    let requested = {
        let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
        scripted.lock_waiters.push_back(response_receiver);
        scripted.unlock_results.push_back(Ok(()));
        scripted.lock_requested.clone()
    };

    let notice = requested.notified();
    let task_consumer = consumer.clone();
    let task_runtime = runtime.clone();
    let task_queue = process_queue.clone();
    let task = tokio::spawn(async move {
        task_consumer
            .lock_orderly_process_queues(&task_runtime, &[task_queue])
            .await
    });
    notice.await;
    process_queue.drop_assignment(85).unwrap();
    response_sender
        .send(Ok(Some(HashSet::from([Some(queue.clone())]))))
        .unwrap();

    assert!(task.await.unwrap().is_empty());
    assert!(process_queue.snapshot(85).unwrap().dropped);
    {
        let scripted = consumer.scripted_lock_transport.lock().unwrap();
        assert_eq!(scripted.unlock_bodies.len(), 1);
    }
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn orderly_success_holds_until_commit_and_rollback_replays_in_order() {
    let mut config = PushConsumerConfig::new("orderly-status-flow").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    config.set_consume_message_batch_max_size(1);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let hook_events = Arc::new(Mutex::new(Vec::new()));
    struct Hook(Arc<Mutex<Vec<String>>>);
    impl ConsumeMessageHook for Hook {
        fn hook_name(&self) -> &str {
            "orderly-consume-hook"
        }

        fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {
            self.0.lock().unwrap().push("before".into());
        }

        fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
            self.0.lock().unwrap().push(format!(
                "{}:{}",
                context.status.as_deref().unwrap_or("missing"),
                context.success
            ));
        }
    }
    consumer.register_consume_message_hook(Arc::new(Hook(hook_events.clone())));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let outcomes = Arc::new(Mutex::new(VecDeque::from([
        (Some(ConsumeOrderlyStatus::Success), false, -1),
        (Some(ConsumeOrderlyStatus::Rollback), false, 30_000),
        (Some(ConsumeOrderlyStatus::Success), true, -1),
        (Some(ConsumeOrderlyStatus::Success), true, -1),
    ])));
    let seen = Arc::new(Mutex::new(Vec::<Vec<i64>>::new()));
    let listener: Arc<dyn MessageListenerOrderly> = {
        let outcomes = outcomes.clone();
        let seen = seen.clone();
        Arc::new(
            move |messages: Vec<MessageExt>, mut context: ConsumeOrderlyContext| {
                let (status, auto_commit, suspend_millis) =
                    outcomes.lock().unwrap().pop_front().unwrap();
                let offsets = messages
                    .iter()
                    .map(MessageExt::queue_offset)
                    .collect::<Vec<_>>();
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push(offsets);
                    context.set_auto_commit(auto_commit);
                    context.set_suspend_current_queue_time_millis(suspend_millis);
                    (status, context)
                }
            },
        )
    };
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let runtime = install_started_orderly_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        listener.clone(),
        None,
    )
    .await;

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 86));
    process_queue
        .insert_messages(86, vec![retry_test_message(10), retry_test_message(11)])
        .unwrap();
    let request = OrderlyConsumeRequest {
        process_queue: process_queue.clone(),
        generation: 86,
    };
    consumer
        .clone()
        .run_orderly_consume_request(request, listener.clone())
        .await;
    assert_eq!(process_queue.snapshot(86).unwrap().cached_count, 2);
    assert!(offset_store.updates.lock().unwrap().is_empty());

    let request = OrderlyConsumeRequest {
        process_queue: process_queue.clone(),
        generation: 86,
    };
    consumer
        .clone()
        .run_orderly_consume_request(request, listener)
        .await;
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        &[vec![10], vec![11], vec![10], vec![11]]
    );
    assert_eq!(process_queue.snapshot(86).unwrap().cached_count, 0);
    assert_eq!(
        offset_store.updates.lock().unwrap().as_slice(),
        &[(queue.clone(), 11, false), (queue, 12, false)]
    );
    assert_eq!(
        hook_events.lock().unwrap().as_slice(),
        [
            "before",
            "SUCCESS:true",
            "before",
            "ROLLBACK:false",
            "before",
            "SUCCESS:true",
            "before",
            "SUCCESS:true"
        ]
    );
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn orderly_clustering_rebalance_locks_before_activating_pull_queue() {
    let mut config = PushConsumerConfig::new("orderly-lock-before-pull").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
    config.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let queue = MessageQueue::new("orders", "broker-a", 0);
    write_lock(&consumer.subscriptions).insert(
        "orders".to_owned(),
        SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
    );
    write_lock(&consumer.topic_queues).insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
    let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
        |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
            (Some(ConsumeOrderlyStatus::Success), context)
        },
    );
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let runtime =
        install_started_orderly_consumer(&consumer, &manager, offset_store_dyn, listener, None)
            .await;
    let client_id = runtime.client_id().to_owned();
    script_rebalance_membership(
        &consumer,
        "orders",
        [Some(vec![client_id.clone()]), Some(vec![client_id])],
    );
    {
        let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
        scripted.lock_results.extend([
            Ok(Some(HashSet::new())),
            Ok(Some(HashSet::from([Some(queue.clone())]))),
        ]);
        scripted.unlock_results.push_back(Ok(()));
    }
    consumer.paused.store(true, Ordering::Release);

    assert!(
        !consumer
            .rebalance_once(&crate::rebalance::AllocateMessageQueueAveragely, None)
            .await
    );
    assert!(!read_lock(&consumer.process_queues).contains_key(&queue));

    consumer.paused.store(false, Ordering::Release);
    assert!(consumer.do_rebalance_with_order(true).await);
    let process_queue = read_lock(&consumer.process_queues)
        .get(&queue)
        .cloned()
        .expect("successful Broker lock permits assignment activation");
    assert!(
        process_queue
            .snapshot(process_queue.generation())
            .unwrap()
            .locked
    );
    assert_eq!(offset_store.values.lock().unwrap().get(&queue), Some(&0));
    {
        let scripted = consumer.scripted_lock_transport.lock().unwrap();
        assert_eq!(scripted.lock_bodies.len(), 2);
        assert!(scripted.lock_bodies.iter().all(|body| {
            body.get_mq_set().is_some_and(|set| {
                set.with(|queues| queues.contains(&Some(SharedValue::new(queue.clone()))))
            })
        }));
    }

    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn orderly_max_continuous_consume_property_yields_between_batches() {
    let mut config = PushConsumerConfig::new("orderly-continuous-limit").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    config.set_consume_message_batch_max_size(1);
    config.set_consume_thread_min(1);
    config.set_consume_thread_max(1);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let callback_count = Arc::new(AtomicUsize::new(0));
    let listener: Arc<dyn MessageListenerOrderly> = {
        let callback_count = callback_count.clone();
        Arc::new(
            move |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                let callback_count = callback_count.clone();
                async move {
                    callback_count.fetch_add(1, Ordering::SeqCst);
                    tokio::time::advance(Duration::from_millis(1)).await;
                    (Some(ConsumeOrderlyStatus::Success), context)
                }
            },
        )
    };
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let settings = OrderlyLockSettings::from_all_values(30_000, 20_000, 20_000, 0).unwrap();
    let runtime = install_started_orderly_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        listener.clone(),
        Some(settings),
    )
    .await;
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 95));
    process_queue
        .insert_messages(95, vec![retry_test_message(400), retry_test_message(401)])
        .unwrap();
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    offset_store.update_offset(&queue, 400, false);

    consumer
        .clone()
        .run_orderly_consume_request(
            OrderlyConsumeRequest {
                process_queue: process_queue.clone(),
                generation: 95,
            },
            listener,
        )
        .await;

    assert_eq!(callback_count.load(Ordering::SeqCst), 1);
    assert_eq!(process_queue.snapshot(95).unwrap().cached_count, 1);
    assert_eq!(
        process_queue.snapshot(95).unwrap().next_commit_offset,
        Some(401)
    );
    assert_eq!(
        offset_store.updates.lock().unwrap().last(),
        Some(&(queue, 401, false))
    );
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn orderly_retry_limit_publishes_retry_topic_and_failed_publish_requeues() {
    let mut config = PushConsumerConfig::new("orderly-retry-limit").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    config.set_max_reconsume_times(2);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
        |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
            (
                Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment),
                context,
            )
        },
    );
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let runtime = install_started_orderly_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        listener.clone(),
        None,
    )
    .await;
    consumer
        .scripted_retry_transport
        .lock()
        .unwrap()
        .retry_results
        .push_back(Ok(()));

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 87));
    process_queue
        .insert_messages(87, vec![retry_test_message(20)])
        .unwrap();
    consumer
        .clone()
        .run_orderly_consume_request(
            OrderlyConsumeRequest {
                process_queue: process_queue.clone(),
                generation: 87,
            },
            listener.clone(),
        )
        .await;
    assert_eq!(process_queue.snapshot(87).unwrap().cached_count, 0);
    assert_eq!(
        offset_store.updates.lock().unwrap().as_slice(),
        &[(queue, 21, false)]
    );
    {
        let scripted = consumer.scripted_retry_transport.lock().unwrap();
        assert!(scripted.broker_headers.is_empty());
        assert_eq!(scripted.retry_messages.len(), 1);
        let retry = &scripted.retry_messages[0];
        assert_eq!(retry.topic(), "%RETRY%orderly-retry-limit");
        assert_eq!(
            retry
                .properties()
                .get(PROPERTY_RETRY_TOPIC)
                .map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            retry
                .properties()
                .get(PROPERTY_RECONSUME_TIME)
                .map(String::as_str),
            Some("3")
        );
        assert_eq!(
            retry
                .properties()
                .get(PROPERTY_MAX_RECONSUME_TIMES)
                .map(String::as_str),
            Some("2")
        );
        assert_eq!(
            retry
                .properties()
                .get(PROPERTY_DELAY_TIME_LEVEL)
                .map(String::as_str),
            Some("5")
        );
        assert!(
            !retry
                .properties()
                .contains_key(PROPERTY_TRANSACTION_PREPARED)
        );
    }
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();

    let mut config = PushConsumerConfig::new("orderly-retry-fallback-failure").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    config.set_max_reconsume_times(2);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let runtime = install_started_orderly_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        listener.clone(),
        None,
    )
    .await;
    consumer
        .scripted_retry_transport
        .lock()
        .unwrap()
        .retry_results
        .push_back(Err("retry publish rejected".into()));
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 88));
    process_queue
        .insert_messages(88, vec![retry_test_message(30)])
        .unwrap();
    consumer
        .clone()
        .run_orderly_consume_request(
            OrderlyConsumeRequest {
                process_queue: process_queue.clone(),
                generation: 88,
            },
            listener,
        )
        .await;
    assert_eq!(process_queue.snapshot(88).unwrap().cached_count, 1);
    assert!(offset_store.updates.lock().unwrap().is_empty());
    let redelivery = process_queue.take_batch(88, 1).unwrap().unwrap();
    assert_eq!(redelivery.messages()[0].reconsume_times(), 3);
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn orderly_callbacks_on_same_queue_are_serialized_in_offset_order() {
    let mut config = PushConsumerConfig::new("orderly-serialized-queue").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    config.set_consume_thread_min(2);
    config.set_consume_thread_max(2);
    config.set_consume_message_batch_max_size(1);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let first_started = Arc::new(tokio::sync::Notify::new());
    let second_started = Arc::new(tokio::sync::Notify::new());
    let release_first = Arc::new(tokio::sync::Notify::new());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let listener: Arc<dyn MessageListenerOrderly> = {
        let first_started = first_started.clone();
        let second_started = second_started.clone();
        let release_first = release_first.clone();
        let seen = seen.clone();
        Arc::new(
            move |messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                let offset = messages[0].queue_offset();
                let first_started = first_started.clone();
                let second_started = second_started.clone();
                let release_first = release_first.clone();
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push(offset);
                    if offset == 100 {
                        first_started.notify_one();
                        release_first.notified().await;
                    } else {
                        second_started.notify_one();
                    }
                    (Some(ConsumeOrderlyStatus::Success), context)
                }
            },
        )
    };
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store;
    let runtime = install_started_orderly_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        listener.clone(),
        None,
    )
    .await;
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue, 96));
    process_queue
        .insert_messages(96, vec![retry_test_message(100), retry_test_message(101)])
        .unwrap();

    let first_notice = first_started.notified();
    let first_consumer = consumer.clone();
    let first_queue = process_queue.clone();
    let first_listener = listener.clone();
    let first = tokio::spawn(async move {
        first_consumer
            .run_orderly_consume_request(
                OrderlyConsumeRequest {
                    process_queue: first_queue,
                    generation: 96,
                },
                first_listener,
            )
            .await
    });
    first_notice.await;

    let second_notice = second_started.notified();
    let second_consumer = consumer.clone();
    let second_queue = process_queue.clone();
    let second_listener = listener.clone();
    let second = tokio::spawn(async move {
        second_consumer
            .run_orderly_consume_request(
                OrderlyConsumeRequest {
                    process_queue: second_queue,
                    generation: 96,
                },
                second_listener,
            )
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), second_notice)
            .await
            .is_err()
    );

    release_first.notify_one();
    tokio::time::timeout(Duration::from_secs(1), second_started.notified())
        .await
        .expect("the next callback should run after the first releases the queue gate");
    first.await.unwrap();
    second.await.unwrap();
    assert_eq!(seen.lock().unwrap().as_slice(), &[100, 101]);
    assert_eq!(process_queue.snapshot(96).unwrap().cached_count, 0);
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn orderly_callbacks_on_different_queues_make_progress_independently() {
    let mut config = PushConsumerConfig::new("orderly-independent-queues").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    config.set_consume_thread_min(2);
    config.set_consume_thread_max(2);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let slow_started = Arc::new(tokio::sync::Notify::new());
    let release_slow = Arc::new(tokio::sync::Notify::new());
    let fast_finished = Arc::new(tokio::sync::Notify::new());
    let listener: Arc<dyn MessageListenerOrderly> = {
        let slow_started = slow_started.clone();
        let release_slow = release_slow.clone();
        let fast_finished = fast_finished.clone();
        Arc::new(
            move |messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                let offset = messages[0].queue_offset();
                let slow_started = slow_started.clone();
                let release_slow = release_slow.clone();
                let fast_finished = fast_finished.clone();
                async move {
                    if offset == 100 {
                        slow_started.notify_one();
                        release_slow.notified().await;
                    } else {
                        fast_finished.notify_one();
                    }
                    (Some(ConsumeOrderlyStatus::Success), context)
                }
            },
        )
    };
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let runtime = install_started_orderly_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        listener.clone(),
        None,
    )
    .await;

    let queue_a = MessageQueue::new("orders", "broker-a", 0);
    let queue_b = MessageQueue::new("orders", "broker-a", 1);
    let process_queue_a = Arc::new(ProcessQueue::new_for_assignment(queue_a, 89));
    let process_queue_b = Arc::new(ProcessQueue::new_for_assignment(queue_b, 90));
    process_queue_a
        .insert_messages(89, vec![retry_test_message(100)])
        .unwrap();
    process_queue_b
        .insert_messages(90, vec![retry_test_message(200)])
        .unwrap();

    let slow_notice = slow_started.notified();
    let task_consumer = consumer.clone();
    let task_listener = listener.clone();
    let task_a = process_queue_a.clone();
    let slow = tokio::spawn(async move {
        task_consumer
            .run_orderly_consume_request(
                OrderlyConsumeRequest {
                    process_queue: task_a,
                    generation: 89,
                },
                task_listener,
            )
            .await
    });
    slow_notice.await;

    let fast_notice = fast_finished.notified();
    let task_consumer = consumer.clone();
    let task_listener = listener.clone();
    let task_b = process_queue_b.clone();
    let fast = tokio::spawn(async move {
        task_consumer
            .run_orderly_consume_request(
                OrderlyConsumeRequest {
                    process_queue: task_b,
                    generation: 90,
                },
                task_listener,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), fast_notice)
        .await
        .expect("the second queue must not wait on the first queue's callback");
    fast.await.unwrap();
    release_slow.notify_one();
    slow.await.unwrap();

    assert_eq!(process_queue_a.snapshot(89).unwrap().cached_count, 0);
    assert_eq!(process_queue_b.snapshot(90).unwrap().cached_count, 0);
    assert_eq!(offset_store.updates.lock().unwrap().len(), 2);
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn orderly_lock_loss_during_callback_rolls_back_without_committing() {
    let mut config = PushConsumerConfig::new("orderly-lock-loss").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_consume_thread_min(1);
    config.set_consume_thread_max(1);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let callback_started = Arc::new(tokio::sync::Notify::new());
    let release_callback = Arc::new(tokio::sync::Notify::new());
    let listener: Arc<dyn MessageListenerOrderly> = {
        let callback_started = callback_started.clone();
        let release_callback = release_callback.clone();
        Arc::new(
            move |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                let callback_started = callback_started.clone();
                let release_callback = release_callback.clone();
                async move {
                    callback_started.notify_one();
                    release_callback.notified().await;
                    (Some(ConsumeOrderlyStatus::Success), context)
                }
            },
        )
    };
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let settings = OrderlyLockSettings::from_values(30_000, 20_000).unwrap();
    let runtime = install_started_orderly_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        listener.clone(),
        Some(settings),
    )
    .await;

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 91));
    process_queue
        .insert_messages(91, vec![retry_test_message(300)])
        .unwrap();
    process_queue
        .set_lock_state(91, true, epoch_millis())
        .unwrap();
    let callback_notice = callback_started.notified();
    let task_consumer = consumer.clone();
    let task_listener = listener.clone();
    let task_queue = process_queue.clone();
    let task = tokio::spawn(async move {
        task_consumer
            .run_orderly_consume_request(
                OrderlyConsumeRequest {
                    process_queue: task_queue,
                    generation: 91,
                },
                task_listener,
            )
            .await
    });
    callback_notice.await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    release_callback.notify_one();
    task.await.unwrap();

    let snapshot = process_queue.snapshot(91).unwrap();
    assert_eq!(snapshot.cached_count, 1);
    assert_eq!(snapshot.in_flight_count, 0);
    assert!(snapshot.next_commit_offset == Some(300));
    assert!(offset_store.updates.lock().unwrap().is_empty());
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn orderly_lock_renewal_uses_java_initial_delay_and_interval() {
    let mut config = PushConsumerConfig::new("orderly-lock-renewal").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_consume_thread_min(1);
    config.set_consume_thread_max(1);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
    let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
        |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
            (Some(ConsumeOrderlyStatus::Success), context)
        },
    );
    let settings = OrderlyLockSettings::from_values(30_000, 20_000).unwrap();
    let runtime = install_started_orderly_consumer(
        &consumer,
        &manager,
        offset_store,
        listener,
        Some(settings),
    )
    .await;
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 92));
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    let requested = {
        let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
        scripted.lock_results.extend([
            Ok(Some(HashSet::from([Some(queue.clone())]))),
            Ok(Some(HashSet::new())),
        ]);
        scripted.unlock_results.push_back(Ok(()));
        scripted.lock_requested.clone()
    };

    let first_notice = requested.notified();
    tokio::time::advance(Duration::from_millis(999)).await;
    tokio::task::yield_now().await;
    assert!(
        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .lock_bodies
            .is_empty()
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::time::timeout(Duration::from_secs(1), first_notice)
        .await
        .expect("first Java renewal starts after 1000ms");
    assert!(process_queue.snapshot(92).unwrap().locked);

    let second_notice = requested.notified();
    tokio::time::advance(Duration::from_millis(20_000)).await;
    tokio::time::timeout(Duration::from_secs(1), second_notice)
        .await
        .expect("subsequent Java renewals use the configured interval");
    assert!(!process_queue.snapshot(92).unwrap().locked);
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn orderly_rebalance_bounds_slow_callback_wait_and_never_commits_dropped_work() {
    let mut config = PushConsumerConfig::new("orderly-slow-rebalance").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
    config.set_consume_thread_min(1);
    config.set_consume_thread_max(1);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    consumer
        .scripted_lock_transport
        .lock()
        .unwrap()
        .unlock_results
        .push_back(Ok(()));
    let callback_started = Arc::new(tokio::sync::Notify::new());
    let release_callback = Arc::new(tokio::sync::Notify::new());
    let listener: Arc<dyn MessageListenerOrderly> = {
        let callback_started = callback_started.clone();
        let release_callback = release_callback.clone();
        Arc::new(
            move |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                let callback_started = callback_started.clone();
                let release_callback = release_callback.clone();
                async move {
                    callback_started.notify_one();
                    release_callback.notified().await;
                    (Some(ConsumeOrderlyStatus::Success), context)
                }
            },
        )
    };
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let runtime = install_started_orderly_consumer(
        &consumer,
        &manager,
        offset_store_dyn,
        listener.clone(),
        None,
    )
    .await;
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 93));
    process_queue
        .set_lock_state(93, true, epoch_millis())
        .unwrap();
    process_queue
        .insert_messages(93, vec![retry_test_message(400)])
        .unwrap();
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    offset_store.update_offset(&queue, 400, false);
    let context = consumer.pull_context().await.unwrap();

    let callback_notice = callback_started.notified();
    let task_consumer = consumer.clone();
    let task_listener = listener.clone();
    let task_queue = process_queue.clone();
    let task = tokio::spawn(async move {
        task_consumer
            .run_orderly_consume_request(
                OrderlyConsumeRequest {
                    process_queue: task_queue,
                    generation: 93,
                },
                task_listener,
            )
            .await
    });
    callback_notice.await;
    process_queue.drop_assignment(93).unwrap();
    let removal_started = Instant::now();
    assert!(
        !consumer
            .remove_process_queue_safely(&context, &queue, &process_queue)
            .await
    );
    assert!(Instant::now().duration_since(removal_started) <= Duration::from_millis(500));
    assert!(read_lock(&consumer.process_queues).contains_key(&queue));
    tokio::time::advance(Duration::from_millis(20_001)).await;
    assert!(
        consumer
            .remove_process_queue_safely(&context, &queue, &process_queue)
            .await
    );
    assert_eq!(
        offset_store.persisted.lock().unwrap().get(&queue),
        Some(&400)
    );
    assert!(!read_lock(&consumer.process_queues).contains_key(&queue));

    release_callback.notify_one();
    task.await.unwrap();
    assert_eq!(process_queue.snapshot(93).unwrap().in_flight_count, 1);
    assert_eq!(offset_store.updates.lock().unwrap().len(), 1);
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn orderly_shutdown_persists_offsets_and_reports_unconfirmed_unlock() {
    let mut config = PushConsumerConfig::new("orderly-unlock-failure").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
    let manager = ClientRuntimeManager::new();
    let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
    let offset_store = Arc::new(RecordingOffsetStore::default());
    let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
        |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
            (Some(ConsumeOrderlyStatus::Success), context)
        },
    );
    let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
    let runtime =
        install_started_orderly_consumer(&consumer, &manager, offset_store_dyn, listener, None)
            .await;
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new_for_assignment(queue.clone(), 94));
    process_queue
        .set_lock_state(94, true, epoch_millis())
        .unwrap();
    write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
    offset_store.update_offset(&queue, 500, false);
    let unassigned_queue = MessageQueue::new("stale-orders", "broker-b", 1);
    offset_store.update_offset(&unassigned_queue, 900, false);
    consumer
        .scripted_lock_transport
        .lock()
        .unwrap()
        .unlock_results
        .push_back(Err("scripted unlock timeout".to_owned()));

    assert!(matches!(
        consumer.shutdown().await,
        Err(PushConsumerError::OrderlyUnlockFailed)
    ));
    assert_eq!(
        offset_store.persisted.lock().unwrap().get(&queue),
        Some(&500)
    );
    assert!(
        !offset_store
            .persisted
            .lock()
            .unwrap()
            .contains_key(&unassigned_queue)
    );
    assert_eq!(
        offset_store.persist_calls.lock().unwrap().as_slice(),
        &[vec![queue.clone()]]
    );
    assert!(process_queue.snapshot(94).unwrap().locked);
    assert!(read_lock(&consumer.pending_orderly_unlocks).contains(&queue));
    assert_eq!(
        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .unlock_bodies
            .len(),
        1
    );
    runtime.shutdown().await.unwrap();
}
