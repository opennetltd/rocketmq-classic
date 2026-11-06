use bytes::BytesMut;
use rocketmq_classic_core::{
    config::ClientConfig,
    error::RpcError,
    runtime::{ClientRuntimeManager, ConsumerRuntime, RuntimeError},
};
use rocketmq_classic_protocol::{
    SharedValue,
    body::CheckClientRequestBody,
    codes::request,
    heartbeat::{ConsumerData, SubscriptionData},
};
use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
use std::{collections::HashSet, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};

struct Consumer(Option<Vec<SubscriptionData>>);
impl ConsumerRuntime for Consumer {
    fn heartbeat_data(&self) -> ConsumerData {
        ConsumerData {
            group_name: Some("different-callback-group".into()),
            subscription_data_set: self.0.as_ref().map(|subscriptions| {
                SharedValue::from(
                    subscriptions
                        .iter()
                        .cloned()
                        .map(|row| Some(SharedValue::from(row)))
                        .collect::<HashSet<_>>(),
                )
            }),
            ..Default::default()
        }
    }
}
fn subscription(expression: &str) -> SubscriptionData {
    SubscriptionData {
        topic: Some("topic".into()),
        sub_string: Some("a > 1".into()),
        sub_version: 42,
        expression_type: Some(expression.into()),
        ..Default::default()
    }
}
async fn read(socket: &mut TcpStream) -> RemotingCommand {
    let mut length = [0; 4];
    socket.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    socket.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes).unwrap().unwrap()
}
async fn reply(
    socket: &mut TcpStream,
    command: &RemotingCommand,
    code: i32,
    body: Option<Vec<u8>>,
) {
    let mut response = RemotingCommand {
        opaque: command.opaque,
        code,
        body,
        remark: Some("rejected".into()),
        ..Default::default()
    };
    response.mark_response();
    socket
        .write_all(&frame::encode(&response).unwrap())
        .await
        .unwrap();
}

fn check_peer(
    listener: TcpListener,
    expected_port: u16,
    expected_body: serde_json::Value,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let command = read(&mut socket).await;
        assert_eq!(command.code, request::CHECK_CLIENT_CONFIG);
        assert_eq!(socket.local_addr().unwrap().port(), expected_port);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(command.body.as_ref().unwrap()).unwrap(),
            expected_body
        );
        reply(&mut socket, &command, 0, None).await;
    })
}

#[test]
fn check_client_body_matches_actual_java_api_payload() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-check.json")).unwrap();
    for case in fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["invocations"] == 1)
    {
        let mut body = CheckClientRequestBody::default();
        body.set_client_id(Some("check-client@DEFAULT".into()));
        body.set_group(Some("registered-group".into()));
        body.set_subscription_data(Some(
            subscription(case["expression"].as_str().unwrap()).into(),
        ));
        body.set_namespace(None);
        assert_eq!(serde_json::to_value(body).unwrap(), case["body"]);
        assert_eq!(case["requestCode"], request::CHECK_CLIENT_CONFIG);
        assert_eq!(case["language"], "JAVA");
        assert_eq!(case["address"], "192.0.2.6:10909");
        assert_eq!(case["timeout"], 1234);
    }
}

#[tokio::test]
async fn tags_empty_subscriptions_and_missing_route_do_not_fetch_or_send() {
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("check-client");
    config.set_namesrv_addr(Some(nameserver.local_addr().unwrap().to_string()));
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    let consumer = Arc::new(Consumer(Some(vec![
        subscription("TAG"),
        subscription(""),
        subscription("SQL92"),
    ])));
    let lease = runtime
        .register_consumer("registered-group", consumer.clone())
        .await
        .unwrap();
    runtime.check_client_in_broker().await.unwrap();
    runtime
        .unregister_consumer(&lease, Instant::now())
        .await
        .unwrap();
    let empty = Arc::new(Consumer(Some(vec![])));
    runtime
        .register_consumer("empty", empty.clone())
        .await
        .unwrap();
    runtime.check_client_in_broker().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), nameserver.accept())
            .await
            .is_err()
    );
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn non_tag_check_uses_registered_key_vip_and_source_error_boundaries() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-check.json")).unwrap();
    for case in fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["invocations"] == 1 && case["expression"] == "SQL92")
    {
        let broker = loop {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            if listener.local_addr().unwrap().port() <= u16::MAX - 2 {
                break listener;
            }
        };
        let broker_address = broker.local_addr().unwrap();
        let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = ClientConfig::new("check-client");
        config.set_namesrv_addr(Some(nameserver.local_addr().unwrap().to_string()));
        config.set_vip_channel_enabled(true);
        config.set_enable_heartbeat_channel_event_listener(false);
        config.set_mq_client_api_timeout(1234);
        let route = serde_json::to_vec(&serde_json::json!({"queueDatas":[],"brokerDatas":[{"cluster":"cluster","brokerName":"broker","brokerAddrs":{"0":format!("127.0.0.1:{}", u32::from(broker_address.port()) + 2)}}],"filterServerTable":{}})).unwrap();
        let name_peer = tokio::spawn(async move {
            let (mut socket, _) = nameserver.accept().await.unwrap();
            let command = read(&mut socket).await;
            assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
            reply(&mut socket, &command, 0, Some(route)).await;
            let mut byte = [0];
            assert_eq!(
                socket.read(&mut byte).await.unwrap(),
                0,
                "check must not refresh the route"
            );
        });
        let expected_body = case["body"].clone();
        let transport_failure = case["transportFailure"].as_bool().unwrap();
        let code = case["responseCode"].as_i64().unwrap() as i32;
        let peer = tokio::spawn(async move {
            let (mut socket, _) = broker.accept().await.unwrap();
            let command = read(&mut socket).await;
            assert_eq!(command.code, request::CHECK_CLIENT_CONFIG);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(command.body.as_ref().unwrap())
                    .unwrap(),
                expected_body
            );
            if !transport_failure {
                // Java ignores the response body entirely, even invalid JSON.
                reply(&mut socket, &command, code, Some(b"invalid".to_vec())).await;
                let mut byte = [0];
                assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
            }
        });
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        runtime.remoting().start().unwrap();
        let consumer = Arc::new(Consumer(Some(vec![
            subscription("TAG"),
            subscription("SQL92"),
        ])));
        runtime
            .register_consumer("registered-group", consumer.clone())
            .await
            .unwrap();
        runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        let result = runtime.check_client_in_broker().await;
        if transport_failure {
            assert!(
                matches!(result, Err(RuntimeError::ClientCheck { ref expression_type, .. }) if expression_type == "SQL92")
            );
        } else if code != 0 {
            assert!(
                matches!(result, Err(RuntimeError::Rpc(RpcError::Response { code: 44, remark: Some(ref remark), .. })) if remark == "rejected")
            );
        } else {
            result.unwrap();
            let empty = Arc::new(Consumer(Some(vec![])));
            runtime
                .register_consumer("a-empty", empty.clone())
                .await
                .unwrap();
            // The native registry traverses a-empty first. An empty consumer
            // ends the whole round, even though the SQL consumer remains.
            runtime.check_client_in_broker().await.unwrap();
        }
        runtime.remoting().shutdown().await;
        peer.await.unwrap();
        name_peer.await.unwrap();
    }
    // Ensure the test's source oracle actually includes all three outcomes.
    let outcomes: HashSet<_> = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["invocations"] == 1 && case["expression"] == "SQL92")
        .map(|case| {
            (
                case["responseCode"].as_i64().unwrap(),
                case["transportFailure"].as_bool().unwrap(),
            )
        })
        .collect();
    assert_eq!(outcomes.len(), 3);
}

#[tokio::test]
async fn vip_mutation_after_runtime_construction_matches_invoked_java_check_api() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-check.json")).unwrap();
    let mutation = &fixture["vipMutation"];
    assert_eq!(mutation["invocations"], 2);
    assert_eq!(mutation["vipDisabled"][0], "127.0.0.1:10911");
    assert_eq!(mutation["vipEnabled"][0], "127.0.0.1:10909");
    let expected_body = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| {
            case["expression"] == "SQL92"
                && case["invocations"] == 1
                && case["responseCode"] == 0
                && case["transportFailure"] == false
        })
        .unwrap()["body"]
        .clone();

    let (vip_broker, ordinary_broker) = loop {
        let vip_broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let vip_port = vip_broker.local_addr().unwrap().port();
        if vip_port > u16::MAX - 2 {
            continue;
        }
        match TcpListener::bind(format!("127.0.0.1:{}", vip_port + 2)).await {
            Ok(ordinary_broker) => break (vip_broker, ordinary_broker),
            Err(_) => continue,
        }
    };
    let vip_port = vip_broker.local_addr().unwrap().port();
    let ordinary_port = ordinary_broker.local_addr().unwrap().port();
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nameserver_addr = nameserver.local_addr().unwrap().to_string();
    let route = serde_json::to_vec(&serde_json::json!({
        "queueDatas": [],
        "brokerDatas": [{
            "cluster": "cluster",
            "brokerName": "broker",
            "brokerAddrs": {"0": format!("127.0.0.1:{ordinary_port}")}
        }],
        "filterServerTable": {}
    }))
    .unwrap();
    let name_peer = tokio::spawn(async move {
        let (mut socket, _) = nameserver.accept().await.unwrap();
        let command = read(&mut socket).await;
        assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
        reply(&mut socket, &command, 0, Some(route)).await;
    });
    let mut config = ClientConfig::new("check-client");
    config.set_namesrv_addr(Some(nameserver_addr));
    config.set_vip_channel_enabled(false);
    config.set_enable_heartbeat_channel_event_listener(false);
    config.set_mq_client_api_timeout(1234);
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    let consumer = Arc::new(Consumer(Some(vec![subscription("SQL92")])));
    assert!(consumer.heartbeat_data().subscription_data_set.is_some());
    let _lease = runtime
        .register_consumer("registered-group", consumer.clone())
        .await
        .unwrap();
    assert!(
        runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap()
    );
    assert_eq!(
        runtime.find_broker_address_by_topic("topic").unwrap(),
        Some(format!("127.0.0.1:{ordinary_port}"))
    );

    let ordinary_peer = check_peer(ordinary_broker, ordinary_port, expected_body.clone());
    runtime.check_client_in_broker().await.unwrap();
    runtime
        .config()
        .write()
        .unwrap()
        .set_vip_channel_enabled(true);
    let vip_peer = check_peer(vip_broker, vip_port, expected_body);
    runtime.check_client_in_broker().await.unwrap();

    runtime.remoting().shutdown().await;
    ordinary_peer.await.unwrap();
    vip_peer.await.unwrap();
    name_peer.await.unwrap();
}

#[tokio::test]
async fn null_subscription_set_matches_invoked_java_whole_round_return() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-check.json")).unwrap();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["subscriptions"].is_null() && case["expression"] == "SQL92")
        .unwrap();
    assert_eq!(case["outcome"], "OK");
    assert_eq!(case["invocations"], 0);
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("null-subscriptions-check"))
        .await
        .unwrap();
    let consumer = Arc::new(Consumer(None));
    let _lease = runtime
        .register_consumer("registered-group", consumer)
        .await
        .unwrap();
    runtime.check_client_in_broker().await.unwrap();
}

#[tokio::test]
async fn non_tag_null_topic_matches_invoked_java_failure_without_rpc() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-check.json")).unwrap();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["topicNull"] == true)
        .unwrap();
    assert_eq!(case["outcome"], "NullPointerException");
    assert_eq!(case["invocations"], 0);
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("null-check-topic"))
        .await
        .unwrap();
    let owner = Arc::new(Consumer(Some(vec![SubscriptionData {
        topic: None,
        expression_type: Some("SQL92".into()),
        ..Default::default()
    }])));
    let _lease = runtime
        .register_consumer("registered-group", owner.clone())
        .await
        .unwrap();
    assert!(matches!(
        runtime.check_client_in_broker().await,
        Err(RuntimeError::InvalidRoute("null subscription topic"))
    ));
}
