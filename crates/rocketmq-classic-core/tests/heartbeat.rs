use bytes::BytesMut;
use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntime, ClientRuntimeManager, ConsumerRuntime, ProducerRuntime},
};
use rocketmq_classic_protocol::{
    SharedValue,
    codes::request,
    heartbeat::{ConsumerData, HeartbeatData, SubscriptionData},
};
use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};

struct Producer;

#[tokio::test]
async fn v2_full_and_simple_wire_preserve_duplicate_consumer_metadata() {
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = broker.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        for round in 0..4 {
            let command = read(&mut stream).await;
            assert_eq!(command.code, request::HEART_BEAT);
            let json: serde_json::Value =
                serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
            let rows = json["consumerDataSet"].as_array().unwrap();
            assert_eq!(rows.len(), 2, "round {round}: actual wire multiplicity");
            let simple = round == 1 || round == 3;
            assert_eq!(json["withoutSub"], simple);
            if round == 2 {
                assert_ne!(rows[0], rows[1]);
            } else {
                assert_eq!(rows[0], rows[1]);
            }
            if simple {
                assert!(
                    rows.iter()
                        .all(|row| row["subscriptionDataSet"].as_array().unwrap().is_empty())
                );
            }
            respond(&mut stream, &command, None, true, false).await;
        }
    });
    let mut config = ClientConfig::new("duplicate-wire");
    config.set_use_heartbeat_v2(true);
    config.set_vip_channel_enabled(false);
    config.set_enable_heartbeat_channel_event_listener(false);
    let runtime = runtime(config, &[address]).await;
    let first = Arc::new(Consumer(Mutex::new(SubscriptionData {
        topic: Some("topic".into()),
        sub_string: Some("*".into()),
        sub_version: 12,
        ..Default::default()
    })));
    let second = Arc::new(Consumer(Mutex::new(first.0.lock().unwrap().clone())));
    let _a = runtime
        .register_consumer("registered-a", first.clone())
        .await
        .unwrap();
    let _b = runtime
        .register_consumer("registered-b", second.clone())
        .await
        .unwrap();
    for round in 0..4 {
        if round == 2 {
            second.0.lock().unwrap().topic = Some("second".into());
        }
        assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
    }
    tokio::time::timeout(Duration::from_secs(2), peer)
        .await
        .unwrap()
        .unwrap();
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn heartbeat_protocol_mode_reads_live_v2_setting_after_construction() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/heartbeat.json")).unwrap();
    let java_mode = &fixture["heartbeatV2SettingMutation"];
    assert!(!java_mode["configuredBefore"].as_bool().unwrap());
    assert!(java_mode["configuredAfterMutation"].as_bool().unwrap());
    assert_eq!(java_mode["modeCalls"], serde_json::json!(["V1", "V2"]));
    assert!(java_mode["v1Outcome"].as_bool().unwrap());
    assert!(java_mode["v2Outcome"].as_bool().unwrap());

    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = broker.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        let mut observed_modes = Vec::new();
        for _ in 0..2 {
            let command = read(&mut stream).await;
            assert_eq!(command.code, request::HEART_BEAT);
            let body: HeartbeatData =
                serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
            observed_modes.push(if body.heartbeat_fingerprint == 0 {
                "V1"
            } else {
                "V2"
            });
            respond(&mut stream, &command, None, true, false).await;
        }
        assert_eq!(observed_modes, ["V1", "V2"]);
    });
    let mut config = ClientConfig::new("heartbeat-mode");
    config.set_use_heartbeat_v2(false);
    config.set_enable_heartbeat_channel_event_listener(false);
    let runtime = runtime(config, &[address.clone()]).await;
    assert!(!runtime.config().read().unwrap().use_heartbeat_v2());
    let producer = Arc::new(Producer);
    let _lease = runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    assert_eq!(
        runtime
            .prepare_heartbeat_data(false)
            .unwrap()
            .producer_data_set
            .unwrap()
            .with(|rows| rows.len()),
        1
    );
    assert_eq!(runtime.route_snapshot().unwrap().broker_addresses.len(), 1);
    assert!(
        runtime
            .send_heartbeat_to_broker(0, "broker", &address)
            .await
            .unwrap()
    );
    runtime.config().write().unwrap().set_use_heartbeat_v2(true);
    assert!(
        runtime
            .send_heartbeat_to_broker(0, "broker", &address)
            .await
            .unwrap()
    );
    peer.await.unwrap();
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn all_broker_wrapper_reads_live_v2_before_concurrent_v1_setting() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/heartbeat.json")).unwrap();
    let java_case = &fixture["heartbeatV2WrapperSettingMutation"];
    assert!(!java_case["configuredBeforeMutation"].as_bool().unwrap());
    assert!(java_case["concurrentConfigured"].as_bool().unwrap());
    assert!(java_case["executorPresent"].as_bool().unwrap());
    assert!(java_case["configuredAfterMutation"].as_bool().unwrap());
    assert!(java_case["v1WrapperOutcome"].as_bool().unwrap());
    assert!(java_case["v1WithLockV2Outcome"].as_bool().unwrap());
    assert!(java_case["v2WrapperOutcome"].as_bool().unwrap());
    assert!(java_case["v2WithLockV2Outcome"].as_bool().unwrap());
    assert_eq!(
        java_case["sendModes"],
        serde_json::json!(["V1", "V1", "V2", "V2"])
    );
    assert_eq!(
        java_case["sendOnConcurrentExecutor"],
        serde_json::json!([true, false, false, false])
    );

    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = broker.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        for expected_v2 in [false, false, true, true] {
            let command = read(&mut stream).await;
            assert_eq!(command.code, request::HEART_BEAT);
            let body: HeartbeatData =
                serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
            assert_eq!(body.heartbeat_fingerprint != 0, expected_v2);
            respond(&mut stream, &command, None, true, false).await;
        }
    });

    let mut config = ClientConfig::new("all-broker-v2-mode");
    config.set_use_heartbeat_v2(false);
    config.set_enable_concurrent_heartbeat(true);
    config.set_concurrent_heartbeat_thread_pool_size(2);
    let runtime = runtime(config, &[address]).await;
    let producer = Arc::new(Producer);
    let _lease = runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
    assert!(
        runtime
            .send_heartbeat_to_all_brokers_with_rebalance(false)
            .await
            .unwrap()
    );
    runtime.config().write().unwrap().set_use_heartbeat_v2(true);
    assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
    assert!(
        runtime
            .send_heartbeat_to_all_brokers_with_rebalance(false)
            .await
            .unwrap()
    );

    tokio::time::timeout(Duration::from_secs(2), peer)
        .await
        .unwrap()
        .unwrap();
    runtime.remoting().shutdown().await;
}

impl ProducerRuntime for Producer {}
struct Consumer(Mutex<SubscriptionData>);
impl ConsumerRuntime for Consumer {
    fn heartbeat_data(&self) -> ConsumerData {
        ConsumerData {
            group_name: Some("consumer".into()),
            subscription_data_set: Some(SharedValue::from(HashSet::from([Some(
                SharedValue::from(self.0.lock().unwrap().clone()),
            )]))),
            ..Default::default()
        }
    }
}

#[tokio::test]
async fn enabled_vip_does_not_rewrite_an_explicit_heartbeat_address() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/heartbeat.json")).unwrap();
    let java = &fixture["heartbeatVipAddress"];
    assert_eq!(java["vipEnabled"], true);
    assert_eq!(java["inputAddress"], java["apiAddress"]);
    assert_eq!(java["mode"], "V1");
    assert_eq!(java["outcome"], true);

    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = broker.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        let command = read(&mut stream).await;
        assert_eq!(command.code, request::HEART_BEAT);
        respond(&mut stream, &command, None, false, false).await;
    });
    let mut config = ClientConfig::new("heartbeat-vip-address");
    config.set_namesrv_addr(Some(String::new()));
    config.set_vip_channel_enabled(true);
    config.set_enable_heartbeat_channel_event_listener(false);
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    let producer = Arc::new(Producer);
    let _lease = runtime
        .register_producer("heartbeat-vip-address", producer.clone())
        .await
        .unwrap();
    assert!(
        runtime
            .send_heartbeat_to_broker(0, "broker", &address)
            .await
            .unwrap()
    );
    tokio::time::timeout(Duration::from_secs(2), peer)
        .await
        .unwrap()
        .unwrap();
    runtime.remoting().shutdown().await;
}

async fn read(stream: &mut TcpStream) -> RemotingCommand {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes).unwrap().unwrap()
}

async fn respond(
    stream: &mut TcpStream,
    command: &RemotingCommand,
    body: Option<Vec<u8>>,
    support: bool,
    sub_change: bool,
) {
    let mut reply = RemotingCommand {
        opaque: command.opaque,
        version: 551,
        body,
        ext_fields: Some(
            std::collections::BTreeMap::from([
                ("IS_SUPPORT_HEART_BEAT_V2".into(), support.to_string()),
                ("IS_SUB_CHANGE".into(), sub_change.to_string()),
            ])
            .into(),
        ),
        ..Default::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply).unwrap())
        .await
        .unwrap();
}

async fn runtime(config: ClientConfig, addresses: &[String]) -> Arc<ClientRuntime> {
    let broker_addrs = addresses
        .iter()
        .enumerate()
        .map(|(id, address)| (id.to_string(), address))
        .collect::<HashMap<_, _>>();
    runtime_with_broker_addrs(config, serde_json::json!(broker_addrs)).await
}

async fn runtime_with_broker_addrs(
    mut config: ClientConfig,
    broker_addrs: serde_json::Value,
) -> Arc<ClientRuntime> {
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.set_namesrv_addr(Some(nameserver.local_addr().unwrap().to_string()));
    let route = serde_json::to_vec(&serde_json::json!({
        "queueDatas": [{"brokerName":"broker", "readQueueNums":1,"writeQueueNums":1,"perm":6,"topicSysFlag":0}],
        "brokerDatas": [{"cluster":"cluster", "brokerName":"broker", "brokerAddrs":broker_addrs}], "filterServerTable":{}
    })).unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = nameserver.accept().await.unwrap();
        let command = read(&mut stream).await;
        assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
        respond(&mut stream, &command, Some(route), false, false).await;
    });
    let manager = ClientRuntimeManager::new();
    let runtime = manager.get_or_create(&config).await.unwrap();
    runtime.remoting().start().unwrap();
    runtime
        .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    server.await.unwrap();
    runtime
}

#[tokio::test]
async fn null_broker_value_is_cached_but_never_dialed_for_heartbeat() {
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = broker.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        let command = read(&mut stream).await;
        assert_eq!(command.code, request::HEART_BEAT);
        respond(&mut stream, &command, None, false, false).await;
    });
    let mut config = ClientConfig::new("null-heartbeat-peer");
    config.set_enable_heartbeat_channel_event_listener(false);
    let runtime =
        runtime_with_broker_addrs(config, serde_json::json!({"0":null,"1":address})).await;
    let consumer = Arc::new(Consumer(Mutex::new(SubscriptionData::default())));
    let _lease = runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    assert_eq!(
        runtime.route_snapshot().unwrap().broker_addresses["broker"][&Some(0)],
        None
    );
    assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
    tokio::time::timeout(Duration::from_secs(2), peer)
        .await
        .unwrap()
        .unwrap();
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn heartbeat_unboxes_nonnull_null_ids_and_skips_null_values() {
    let mut config = ClientConfig::new("null-heartbeat-broker-id");
    config.set_enable_heartbeat_channel_event_listener(false);
    let runtime =
        runtime_with_broker_addrs(config, serde_json::json!({"null":"127.0.0.1:1"})).await;
    let producer = Arc::new(Producer);
    let _lease = runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
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
    assert!(runtime.route_snapshot().unwrap().broker_addresses["broker"].contains_key(&None));
    let result = runtime.send_heartbeat_to_all_brokers().await;
    assert!(
        matches!(
            &result,
            Err(rocketmq_classic_core::runtime::RuntimeError::InvalidRoute(
                "null broker address id"
            ))
        ),
        "unexpected heartbeat result: {result:?}"
    );
    let consumer = Arc::new(Consumer(Mutex::new(SubscriptionData::default())));
    let _consumer_lease = runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    assert!(matches!(
        runtime.send_heartbeat_to_all_brokers().await,
        Err(rocketmq_classic_core::runtime::RuntimeError::InvalidRoute(
            "null broker address id"
        ))
    ));
    runtime.remoting().shutdown().await;

    let mut config = ClientConfig::new("null-heartbeat-broker-id-null-value");
    config.set_enable_heartbeat_channel_event_listener(false);
    let runtime = runtime_with_broker_addrs(config, serde_json::json!({"null":null})).await;
    let producer = Arc::new(Producer);
    let _lease = runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    assert!(runtime.route_snapshot().unwrap().broker_addresses["broker"].contains_key(&None));
    assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn single_peer_sends_to_slave_for_producer_only_and_reports_wire_failure() {
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = broker.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        let command = read(&mut stream).await;
        assert_eq!(command.code, request::HEART_BEAT);
        let body: HeartbeatData = serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
        assert!(
            body.consumer_data_set
                .as_ref()
                .unwrap()
                .with(|rows| rows.is_empty())
        );
        respond(&mut stream, &command, None, false, false).await;
        let command = read(&mut stream).await;
        let mut response = RemotingCommand {
            code: 1,
            opaque: command.opaque,
            ..Default::default()
        };
        response.mark_response();
        stream
            .write_all(&frame::encode(&response).unwrap())
            .await
            .unwrap();
    });
    let mut config = ClientConfig::new("single-peer");
    config.set_enable_heartbeat_channel_event_listener(false);
    let runtime = runtime(config, std::slice::from_ref(&address)).await;
    assert!(
        !runtime
            .send_heartbeat_to_broker(1, "broker", &address)
            .await
            .unwrap()
    );
    let producer = Arc::new(Producer);
    let lease = runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    assert!(
        runtime
            .send_heartbeat_to_broker(1, "broker", &address)
            .await
            .unwrap()
    );
    assert!(
        !runtime
            .send_heartbeat_to_broker(1, "broker", &address)
            .await
            .unwrap()
    );
    assert_eq!(
        runtime
            .route_snapshot()
            .unwrap()
            .subscribe_address("broker", 0, true)
            .unwrap()
            .unwrap()
            .version,
        551
    );
    peer.await.unwrap();
    runtime
        .unregister_producer(&lease, Instant::now())
        .await
        .unwrap();
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn active_broker_connection_sends_heartbeat_and_wakes_runtime_wide_rebalance() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Rebalancer(AtomicUsize);
    impl ConsumerRuntime for Rebalancer {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData {
                group_name: Some("rebalance-owner".into()),
                ..Default::default()
            }
        }
        fn try_rebalance(
            &self,
        ) -> Option<std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>>>
        {
            Some(Box::pin(async {
                self.0.fetch_add(1, Ordering::SeqCst);
                true
            }))
        }
    }
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = broker.local_addr().unwrap().to_string();
    let heartbeats = Arc::new(AtomicUsize::new(0));
    let observed = heartbeats.clone();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        loop {
            let mut prefix = [0; 4];
            if stream.read_exact(&mut prefix).await.is_err() {
                break;
            }
            let mut bytes = BytesMut::from(&prefix[..]);
            bytes.resize(u32::from_be_bytes(prefix) as usize + 4, 0);
            stream.read_exact(&mut bytes[4..]).await.unwrap();
            let command = frame::decode_next(&mut bytes).unwrap().unwrap();
            if command.code == request::HEART_BEAT {
                observed.fetch_add(1, Ordering::SeqCst);
            }
            respond(&mut stream, &command, None, false, false).await;
        }
    });
    let runtime = runtime(
        ClientConfig::new("active-heartbeat"),
        std::slice::from_ref(&address),
    )
    .await;
    assert!(runtime.remoting().get_channel_event_listener().is_some());
    let first = Arc::new(Rebalancer(AtomicUsize::new(0)));
    let second = Arc::new(Rebalancer(AtomicUsize::new(0)));
    let a = runtime
        .register_consumer("first", first.clone())
        .await
        .unwrap();
    let b = runtime
        .register_consumer("second", second.clone())
        .await
        .unwrap();
    runtime.start().await.unwrap();
    runtime
        .remoting()
        .invoke_sync(
            &address,
            RemotingCommand {
                code: 9999,
                ..Default::default()
            },
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while first.0.load(Ordering::SeqCst) == 0 || second.0.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(heartbeats.load(Ordering::SeqCst) > 0);
    assert_eq!(
        runtime
            .route_snapshot()
            .unwrap()
            .subscribe_address("broker", 0, true)
            .unwrap()
            .unwrap()
            .version,
        551
    );
    runtime
        .unregister_consumer(&a, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    runtime
        .unregister_consumer(&b, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    runtime.shutdown().await.unwrap();
    peer.await.unwrap();
    let weak = Arc::downgrade(&runtime);
    drop(runtime);
    assert!(weak.upgrade().is_none(), "listener retained its runtime");
}

#[tokio::test]
async fn disabled_or_unknown_channel_does_not_send_unsolicited_heartbeat() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/heartbeat.json")).unwrap();
    let java_listener_cases = fixture["channelEventListenerCases"].as_array().unwrap();
    for enabled in [false, true] {
        let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = broker.local_addr().unwrap().to_string();
        let mut config = ClientConfig::new(format!("no-active-heartbeat-{enabled}"));
        config.set_enable_heartbeat_channel_event_listener(enabled);
        let runtime = runtime(
            config,
            if enabled {
                &[]
            } else {
                std::slice::from_ref(&address)
            },
        )
        .await;
        let java_case = java_listener_cases
            .iter()
            .find(|case| case["configured"].as_bool() == Some(enabled))
            .unwrap();
        let listener_before_mutation = runtime.remoting().get_channel_event_listener();
        assert_eq!(
            listener_before_mutation.is_some(),
            java_case["listenerInitiallyPresent"].as_bool().unwrap()
        );
        runtime
            .config()
            .write()
            .unwrap()
            .set_enable_heartbeat_channel_event_listener(!enabled);
        let listener_after_mutation = runtime.remoting().get_channel_event_listener();
        assert_eq!(
            listener_after_mutation.is_some(),
            java_case["listenerAfterMutationPresent"].as_bool().unwrap()
        );
        let same_listener = match (&listener_before_mutation, &listener_after_mutation) {
            (Some(before), Some(after)) => Arc::ptr_eq(before, after),
            (None, None) => true,
            _ => false,
        };
        assert_eq!(
            same_listener,
            java_case["sameListenerAfterMutation"].as_bool().unwrap()
        );
        let producer = Arc::new(Producer);
        let lease = runtime
            .register_producer("producer", producer.clone())
            .await
            .unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = broker.accept().await.unwrap();
            let command = read(&mut stream).await;
            assert_eq!(command.code, 9999);
            respond(&mut stream, &command, None, false, false).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(50), read(&mut stream))
                    .await
                    .is_err()
            );
        });
        runtime
            .remoting()
            .invoke_sync(
                &address,
                RemotingCommand {
                    code: 9999,
                    ..Default::default()
                },
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        peer.await.unwrap();
        assert_eq!(
            runtime.remoting().get_channel_event_listener().is_some(),
            enabled
        );
        runtime
            .unregister_producer(&lease, Instant::now())
            .await
            .unwrap();
        runtime.remoting().shutdown().await;
    }
}

#[tokio::test]
async fn v2_negotiates_full_simple_subscription_change_version_and_rebalance() {
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = broker.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        let mut fingerprints = Vec::new();
        // No V2 support -> full retry; support -> reduced; sub change -> full.
        for (index, (simple, support, changed)) in [
            (false, false, false),
            (false, true, false),
            (true, true, true),
            (false, true, false),
            (true, true, false),
            (false, true, false),
            (false, true, false),
        ]
        .into_iter()
        .enumerate()
        {
            let command = read(&mut stream).await;
            assert_eq!(command.code, request::HEART_BEAT);
            let body: HeartbeatData =
                serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
            assert_eq!(body.client_id.as_deref(), Some("heartbeat@DEFAULT"));
            assert_eq!(body.is_without_sub, simple, "round {index}");
            assert_eq!(
                body.producer_data_set
                    .as_ref()
                    .unwrap()
                    .with(|rows| rows.len()),
                1
            );
            let consumer = body
                .consumer_data_set
                .as_ref()
                .unwrap()
                .with(|rows| rows.iter().flatten().next().unwrap().snapshot());
            assert_eq!(
                consumer
                    .subscription_data_set
                    .as_ref()
                    .unwrap()
                    .with(|rows| rows.is_empty()),
                simple
            );
            if !simple {
                assert_eq!(
                    consumer
                        .subscription_data_set
                        .as_ref()
                        .unwrap()
                        .with(|rows| rows
                            .iter()
                            .flatten()
                            .next()
                            .unwrap()
                            .with(|row| row.sub_version)),
                    if index < 4 { 12 } else { 99 }
                );
            }
            fingerprints.push(body.heartbeat_fingerprint);
            respond(&mut stream, &command, None, support, changed).await;
        }
        assert!(
            fingerprints[..6]
                .iter()
                .all(|fingerprint| *fingerprint == fingerprints[0])
        );
        assert_ne!(fingerprints[6], fingerprints[0]);
    });
    let mut config = ClientConfig::new("heartbeat");
    config.set_use_heartbeat_v2(false);
    let runtime = runtime(config, std::slice::from_ref(&address)).await;
    runtime.config().write().unwrap().set_use_heartbeat_v2(true);
    let producer = Arc::new(Producer);
    let consumer = Arc::new(Consumer(Mutex::new(SubscriptionData {
        topic: Some("topic".into()),
        sub_string: Some("*".into()),
        sub_version: 12,
        ..Default::default()
    })));
    runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    for index in 0..4 {
        if index % 2 == 0 {
            assert!(
                runtime
                    .send_heartbeat_to_broker(0, "broker", &address)
                    .await
                    .unwrap()
            );
        } else {
            assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
        }
    }
    consumer.0.lock().unwrap().sub_version = 99;
    assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
    assert!(
        runtime
            .send_heartbeat_to_all_brokers_with_rebalance(true)
            .await
            .unwrap()
    );
    consumer.0.lock().unwrap().sub_string = Some("changed".into());
    assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
    assert_eq!(
        runtime
            .route_snapshot()
            .unwrap()
            .subscribe_address("broker", 0, true)
            .unwrap()
            .unwrap()
            .version,
        551
    );
    peer.await.unwrap();
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn v1_producer_only_skips_slave_and_empty_registry_skips_all() {
    let master = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slave = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addresses = [
        master.local_addr().unwrap().to_string(),
        slave.local_addr().unwrap().to_string(),
    ];
    let peer = tokio::spawn(async move {
        let (mut stream, _) = master.accept().await.unwrap();
        let command = read(&mut stream).await;
        let body: HeartbeatData = serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
        assert_eq!(command.code, request::HEART_BEAT);
        assert!(
            body.consumer_data_set
                .as_ref()
                .unwrap()
                .with(|rows| rows.is_empty())
        );
        assert_eq!(
            body.producer_data_set
                .as_ref()
                .unwrap()
                .with(|rows| rows.len()),
            1
        );
        assert_eq!(body.heartbeat_fingerprint, 0);
        assert!(!body.is_without_sub);
        respond(&mut stream, &command, None, false, false).await;
    });
    let runtime = runtime(ClientConfig::new("heartbeat"), &addresses).await;
    assert!(!runtime.send_heartbeat_to_all_brokers().await.unwrap());
    assert!(
        !runtime
            .send_heartbeat_to_broker_with_lock_mode(0, "broker", &addresses[0], false)
            .await
            .unwrap()
    );
    let producer = Arc::new(Producer);
    runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
    peer.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), slave.accept())
            .await
            .is_err()
    );
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn failed_peer_does_not_prevent_heartbeat_to_other_brokers() {
    let master = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slave = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addresses = [
        master.local_addr().unwrap().to_string(),
        slave.local_addr().unwrap().to_string(),
    ];
    let failed = tokio::spawn(async move {
        let (mut stream, _) = master.accept().await.unwrap();
        let command = read(&mut stream).await;
        let mut reply = RemotingCommand {
            code: 1,
            opaque: command.opaque,
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
    });
    let successful = tokio::spawn(async move {
        let (mut stream, _) = slave.accept().await.unwrap();
        let command = read(&mut stream).await;
        respond(&mut stream, &command, None, false, false).await;
    });
    let runtime = runtime(ClientConfig::new("heartbeat"), &addresses).await;
    let consumer = Arc::new(Consumer(Mutex::new(SubscriptionData::default())));
    runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap()); // Java outer return is not an all-peers-success flag.
    failed.await.unwrap();
    successful.await.unwrap();
    let snapshot = runtime.route_snapshot().unwrap();
    assert_eq!(
        snapshot
            .subscribe_address("broker", 0, true)
            .unwrap()
            .unwrap()
            .version,
        0
    );
    assert_eq!(
        snapshot
            .subscribe_address("broker", 1, true)
            .unwrap()
            .unwrap()
            .version,
        551
    );
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn concurrent_v1_uses_parallel_peers_and_busy_heartbeat_gate_skips_a_round() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/heartbeat.json")).unwrap();
    let java_case = &fixture["concurrentHeartbeatPoolSizeMutation"];
    assert_eq!(java_case["configuredBeforeMutation"], 2);
    assert_eq!(java_case["executorCoreSizeBeforeMutation"], 2);
    assert_eq!(java_case["configuredAfterMutation"], 1);
    assert_eq!(java_case["executorCoreSizeAfterMutation"], 2);

    let master = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slave = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addresses = [
        master.local_addr().unwrap().to_string(),
        slave.local_addr().unwrap().to_string(),
    ];
    let (ready, received) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let (second, second_received) = tokio::sync::oneshot::channel();
    let first = tokio::spawn(async move {
        let (mut stream, _) = master.accept().await.unwrap();
        let command = read(&mut stream).await;
        ready.send(()).unwrap();
        second_received.await.unwrap(); // Sequential execution would deadlock here.
        released.await.unwrap();
        respond(&mut stream, &command, None, false, false).await;
    });
    let second_peer = tokio::spawn(async move {
        let (mut stream, _) = slave.accept().await.unwrap();
        let command = read(&mut stream).await;
        let body: HeartbeatData = serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
        assert_eq!(
            body.consumer_data_set
                .as_ref()
                .unwrap()
                .with(|rows| rows.len()),
            1
        );
        second.send(()).unwrap();
        respond(&mut stream, &command, None, false, false).await;
    });
    let mut config = ClientConfig::new("heartbeat");
    config.set_enable_concurrent_heartbeat(true);
    config.set_concurrent_heartbeat_thread_pool_size(2);
    let runtime = runtime(config, &addresses).await;
    {
        let config = runtime.config();
        let mut config = config.write().unwrap();
        config.set_concurrent_heartbeat_thread_pool_size(1);
    }
    let consumer = Arc::new(Consumer(Mutex::new(SubscriptionData::default())));
    runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    let sending = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.send_heartbeat_to_all_brokers().await })
    };
    tokio::time::timeout(Duration::from_secs(2), received)
        .await
        .unwrap()
        .unwrap();
    assert!(!runtime.send_heartbeat_to_all_brokers().await.unwrap());
    assert!(
        !runtime
            .send_heartbeat_to_broker_with_lock_mode(0, "broker", &addresses[0], false)
            .await
            .unwrap()
    );
    release.send(()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), sending)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    );
    first.await.unwrap();
    second_peer.await.unwrap();
    let snapshot = runtime.route_snapshot().unwrap();
    assert_eq!(
        snapshot
            .subscribe_address("broker", 0, true)
            .unwrap()
            .unwrap()
            .version,
        551
    );
    assert_eq!(
        snapshot
            .subscribe_address("broker", 1, true)
            .unwrap()
            .unwrap()
            .version,
        551
    );
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn enabling_concurrent_heartbeat_after_construction_has_no_executor() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/heartbeat.json")).unwrap();
    let java_case = &fixture["concurrentHeartbeatExecutorMutation"];
    assert_eq!(java_case["preparedProducerCount"], 1);
    assert!(
        !java_case["executorPresentBeforeMutation"]
            .as_bool()
            .unwrap()
    );
    assert!(java_case["configuredAfterMutation"].as_bool().unwrap());
    assert!(!java_case["executorPresentAfterMutation"].as_bool().unwrap());
    assert!(!java_case["sendOutcome"].as_bool().unwrap());

    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = broker.local_addr().unwrap().to_string();
    let mut config = ClientConfig::new("late-concurrent-heartbeat");
    config.set_enable_concurrent_heartbeat(false);
    let runtime = runtime(config, std::slice::from_ref(&address)).await;
    runtime
        .config()
        .write()
        .unwrap()
        .set_enable_concurrent_heartbeat(true);
    let consumer = Arc::new(Consumer(Mutex::new(SubscriptionData::default())));
    let _lease = runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    let prepared = runtime.prepare_heartbeat_data(false).unwrap();
    assert_eq!(
        prepared
            .consumer_data_set
            .as_ref()
            .unwrap()
            .with(|rows| rows.len()),
        1
    );
    assert!(!runtime.send_heartbeat_to_all_brokers().await.unwrap());
    assert!(
        tokio::time::timeout(Duration::from_millis(25), broker.accept())
            .await
            .is_err()
    );
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn disabling_concurrent_heartbeat_after_construction_uses_serial_path() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/heartbeat.json")).unwrap();
    let java_case = &fixture["concurrentHeartbeatFlagMutationToSerial"];
    assert_eq!(java_case["preparedProducerCount"], 1);
    assert!(java_case["configuredBeforeMutation"].as_bool().unwrap());
    assert!(
        java_case["executorPresentBeforeMutation"]
            .as_bool()
            .unwrap()
    );
    assert!(!java_case["configuredAfterMutation"].as_bool().unwrap());
    assert!(java_case["executorPresentAfterMutation"].as_bool().unwrap());
    assert!(java_case["sendOutcome"].as_bool().unwrap());
    assert_eq!(java_case["sendModes"], serde_json::json!(["V1"]));

    let master = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slave = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addresses = [
        master.local_addr().unwrap().to_string(),
        slave.local_addr().unwrap().to_string(),
    ];
    let first_delivery = Arc::new(AtomicBool::new(false));
    let (first_seen, mut first_received) = tokio::sync::mpsc::unbounded_channel();
    let (second_seen, mut second_received) = tokio::sync::mpsc::unbounded_channel();
    let (release, release_rx) = tokio::sync::watch::channel(false);
    let mut peers = Vec::new();
    for (name, listener) in [("master", master), ("slave", slave)] {
        let first_delivery = first_delivery.clone();
        let first_seen = first_seen.clone();
        let second_seen = second_seen.clone();
        let mut release_rx = release_rx.clone();
        peers.push(tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let command = read(&mut stream).await;
            if !first_delivery.swap(true, Ordering::SeqCst) {
                first_seen.send(name).unwrap();
                while !*release_rx.borrow() {
                    release_rx.changed().await.unwrap();
                }
            } else {
                second_seen.send(name).unwrap();
            }
            respond(&mut stream, &command, None, false, false).await;
        }));
    }

    let mut config = ClientConfig::new("serial-after-concurrent-heartbeat");
    config.set_enable_concurrent_heartbeat(true);
    config.set_concurrent_heartbeat_thread_pool_size(2);
    let runtime = runtime(config, &addresses).await;
    runtime
        .config()
        .write()
        .unwrap()
        .set_enable_concurrent_heartbeat(false);
    let consumer = Arc::new(Consumer(Mutex::new(SubscriptionData::default())));
    let _lease = runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    let prepared = runtime.prepare_heartbeat_data(false).unwrap();
    assert_eq!(
        prepared
            .consumer_data_set
            .as_ref()
            .unwrap()
            .with(|rows| rows.len()),
        1
    );
    assert_eq!(
        runtime.route_snapshot().unwrap().broker_addresses["broker"].len(),
        2
    );
    let mut sending = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.send_heartbeat_to_all_brokers().await })
    };
    tokio::select! {
        result = &mut sending => panic!("heartbeat send completed before peer accepted: {result:?}"),
        first = tokio::time::timeout(Duration::from_secs(2), first_received.recv()) => {
            first.unwrap().unwrap();
        }
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), second_received.recv())
            .await
            .is_err(),
        "the second broker must not receive a heartbeat before the first reply"
    );

    release.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), second_received.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), sending)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    );
    for peer in peers {
        peer.await.unwrap();
    }
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn rebalance_wrapper_uses_serial_v1_even_when_concurrent_heartbeat_is_enabled() {
    for is_rebalance in [false, true] {
        let master = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let slave = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addresses = [
            master.local_addr().unwrap().to_string(),
            slave.local_addr().unwrap().to_string(),
        ];
        let (ready, received) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let first = tokio::spawn(async move {
            let (mut stream, _) = master.accept().await.unwrap();
            let command = read(&mut stream).await;
            ready.send(()).unwrap();
            released.await.unwrap();
            respond(&mut stream, &command, None, false, false).await;
        });
        let mut config = ClientConfig::new(format!("serial-v1-wrapper-{is_rebalance}"));
        config.set_enable_concurrent_heartbeat(true);
        config.set_concurrent_heartbeat_thread_pool_size(2);
        let runtime = runtime(config, &addresses).await;
        let consumer = Arc::new(Consumer(Mutex::new(SubscriptionData::default())));
        runtime
            .register_consumer("consumer", consumer.clone())
            .await
            .unwrap();
        let sending = {
            let runtime = runtime.clone();
            tokio::spawn(async move {
                runtime
                    .send_heartbeat_to_all_brokers_with_rebalance(is_rebalance)
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(2), received)
            .await
            .unwrap()
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), slave.accept())
                .await
                .is_err(),
            "V2 wrapper's V1 fallback must not execute concurrently"
        );
        release.send(()).unwrap();
        let (mut stream, _) = tokio::time::timeout(Duration::from_secs(2), slave.accept())
            .await
            .unwrap()
            .unwrap();
        let command = read(&mut stream).await;
        let body: HeartbeatData = serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
        assert_eq!(body.heartbeat_fingerprint, 0);
        assert!(!body.is_without_sub);
        respond(&mut stream, &command, None, false, false).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(2), sending)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        );
        first.await.unwrap();
        runtime.remoting().shutdown().await;
    }
}

#[tokio::test]
async fn preparation_failure_returns_false_without_socket_and_releases_heartbeat_gate() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Failing(AtomicBool);
    impl ConsumerRuntime for Failing {
        fn heartbeat_data(&self) -> ConsumerData {
            assert!(
                !self.0.load(Ordering::SeqCst),
                "consumer preparation failure"
            );
            ConsumerData {
                group_name: Some("consumer".into()),
                ..Default::default()
            }
        }
    }
    for v2 in [false, true] {
        let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = broker.local_addr().unwrap().to_string();
        let mut config = ClientConfig::new(format!("failed-preparation-{v2}"));
        config.set_use_heartbeat_v2(v2);
        config.set_enable_heartbeat_channel_event_listener(false);
        let runtime = runtime(config, std::slice::from_ref(&address)).await;
        let consumer = Arc::new(Failing(AtomicBool::new(true)));
        runtime
            .register_consumer("consumer", consumer.clone())
            .await
            .unwrap();
        assert!(!runtime.send_heartbeat_to_all_brokers().await.unwrap());
        for rebalance in [false, true] {
            assert!(
                !runtime
                    .send_heartbeat_to_all_brokers_with_rebalance(rebalance)
                    .await
                    .unwrap()
            );
        }
        assert!(
            !runtime
                .send_heartbeat_to_broker(0, "broker", &address)
                .await
                .unwrap()
        );
        assert!(
            !runtime
                .send_heartbeat_to_broker_with_lock_mode(0, "broker", &address, false)
                .await
                .unwrap()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), broker.accept())
                .await
                .is_err()
        );
        consumer.0.store(false, Ordering::SeqCst);
        let peer = tokio::spawn(async move {
            let (mut stream, _) = broker.accept().await.unwrap();
            let command = read(&mut stream).await;
            let body: HeartbeatData =
                serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
            assert_eq!(
                body.consumer_data_set
                    .as_ref()
                    .unwrap()
                    .with(|rows| rows.len()),
                1
            );
            assert!(!body.is_without_sub);
            respond(&mut stream, &command, None, v2, false).await;
        });
        assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
        peer.await.unwrap();
        runtime.remoting().shutdown().await;
    }
}
