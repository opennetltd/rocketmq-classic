use std::{
    collections::BTreeMap,
    fmt::Write as _,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::Engine;
use bytes::BytesMut;
use rocketmq_classic_common::topic::TopicValidator;
use rocketmq_classic_core::error::RpcError;
use rocketmq_classic_core::route::broker_vip_channel;
use rocketmq_classic_producer::{Producer, ProducerConfig, ProducerError};
use rocketmq_classic_protocol::codes::{request, response};
use rocketmq_classic_protocol::header::HeaderMap;
use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
    time::timeout,
};

async fn read_command(stream: &mut TcpStream) -> Option<RemotingCommand> {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.ok()?;
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.ok()?;
    frame::decode_next(&mut bytes).ok().flatten()
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

async fn write_response(
    stream: &mut TcpStream,
    request: &RemotingCommand,
    code: i32,
    body: Option<Vec<u8>>,
) {
    let mut reply = RemotingCommand {
        code,
        opaque: request.opaque,
        body,
        ..Default::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply).unwrap())
        .await
        .unwrap();
}

async fn write_response_fields(
    stream: &mut TcpStream,
    request: &RemotingCommand,
    code: i32,
    ext_fields: HeaderMap,
) {
    let mut reply = RemotingCommand {
        code,
        opaque: request.opaque,
        ext_fields: Some(ext_fields.into()),
        ..Default::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply).unwrap())
        .await
        .unwrap();
}

async fn write_response_body_fields(
    stream: &mut TcpStream,
    request: &RemotingCommand,
    code: i32,
    body: Vec<u8>,
    ext_fields: HeaderMap,
) {
    let mut reply = RemotingCommand {
        code,
        opaque: request.opaque,
        body: Some(body),
        ext_fields: Some(ext_fields.into()),
        ..Default::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply).unwrap())
        .await
        .unwrap();
}

fn java_message_record(name: &str) -> Vec<u8> {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
    let record = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == name)
        .unwrap()["recordBase64"]
        .as_str()
        .unwrap();
    base64::engine::general_purpose::STANDARD
        .decode(record)
        .unwrap()
}

fn physical_message_id(address: &str, offset: i64) -> String {
    let address: SocketAddr = address.parse().unwrap();
    let mut bytes = match address.ip() {
        std::net::IpAddr::V4(address) => address.octets().to_vec(),
        std::net::IpAddr::V6(address) => address.octets().to_vec(),
    };
    bytes.extend_from_slice(&i32::from(address.port()).to_be_bytes());
    bytes.extend_from_slice(&offset.to_be_bytes());
    let mut message_id = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut message_id, "{byte:02X}").unwrap();
    }
    message_id
}

async fn query_broker_peer() -> (
    String,
    JoinHandle<()>,
    mpsc::UnboundedReceiver<RemotingCommand>,
) {
    query_broker_peer_with_response(response::SUCCESS).await
}

async fn query_broker_peer_with_response(
    query_response_code: i32,
) -> (
    String,
    JoinHandle<()>,
    mpsc::UnboundedReceiver<RemotingCommand>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (sender, requests) = mpsc::unbounded_channel();
    let record = java_message_record("ipv4-v1");
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        while let Some(command) = read_command(&mut stream).await {
            let _ = sender.send(command.clone());
            if command.code == request::QUERY_MESSAGE {
                if query_response_code == response::SUCCESS {
                    write_response_body_fields(
                        &mut stream,
                        &command,
                        response::SUCCESS,
                        record.clone(),
                        HeaderMap::from([
                            ("indexLastUpdateTimestamp".into(), "200".into()),
                            ("indexLastUpdatePhyoffset".into(), "100".into()),
                        ]),
                    )
                    .await;
                } else {
                    write_response(&mut stream, &command, query_response_code, None).await;
                }
            } else if !command.is_oneway() {
                write_response(&mut stream, &command, response::SUCCESS, None).await;
            }
        }
    });
    (address, task, requests)
}

async fn name_server_peer(
    route: Vec<u8>,
) -> (
    String,
    JoinHandle<()>,
    mpsc::UnboundedReceiver<RemotingCommand>,
) {
    name_server_peer_with_response(route, response::SUCCESS).await
}

async fn name_server_peer_with_response(
    route: Vec<u8>,
    response_code: i32,
) -> (
    String,
    JoinHandle<()>,
    mpsc::UnboundedReceiver<RemotingCommand>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (sender, requests) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        while let Some(command) = read_command(&mut stream).await {
            let _ = sender.send(command.clone());
            if command.code == request::GET_ROUTEINFO_BY_TOPIC {
                write_response(&mut stream, &command, response_code, Some(route.clone())).await;
            } else if !command.is_oneway() {
                write_response(&mut stream, &command, response::SUCCESS, None).await;
            }
        }
    });
    (address, task, requests)
}

async fn broker_peer(
    response_codes: Vec<i32>,
) -> (
    String,
    JoinHandle<()>,
    mpsc::UnboundedReceiver<RemotingCommand>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (sender, requests) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut codes = response_codes.into_iter();
        while let Some(command) = read_command(&mut stream).await {
            let _ = sender.send(command.clone());
            write_response(
                &mut stream,
                &command,
                codes.next().unwrap_or(response::SUCCESS),
                None,
            )
            .await;
        }
    });
    (address, task, requests)
}

async fn offset_broker_peer() -> (
    String,
    JoinHandle<()>,
    mpsc::UnboundedReceiver<RemotingCommand>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (sender, requests) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        while let Some(command) = read_command(&mut stream).await {
            let _ = sender.send(command.clone());
            if command.code == request::HEART_BEAT {
                write_response(&mut stream, &command, response::SUCCESS, None).await;
                continue;
            }
            let (code, value) = match command.code {
                request::SEARCH_OFFSET_BY_TIMESTAMP => ("offset", "11"),
                request::GET_MAX_OFFSET => ("offset", "22"),
                request::GET_MIN_OFFSET => ("offset", "3"),
                request::GET_EARLIEST_MSG_STORETIME => ("timestamp", "44"),
                other => panic!("unexpected Broker request {other}"),
            };
            write_response_fields(
                &mut stream,
                &command,
                response::SUCCESS,
                HeaderMap::from([(code.to_owned(), value.to_owned())]),
            )
            .await;
        }
    });
    (address, task, requests)
}

fn route_body(brokers: &[(&str, &str)]) -> Vec<u8> {
    let queue_data = brokers
        .iter()
        .map(|(name, _)| {
            json!({
                "brokerName": name,
                "readQueueNums": 4,
                "writeQueueNums": 4,
                "perm": 6,
                "topicSysFlag": 0
            })
        })
        .collect::<Vec<_>>();
    let broker_data = brokers
        .iter()
        .map(|(name, address)| {
            json!({
                "cluster": "DefaultCluster",
                "brokerName": name,
                "brokerAddrs": {"0": address}
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&json!({
        "queueDatas": queue_data,
        "brokerDatas": broker_data,
        "filterServerTable": {}
    }))
    .unwrap()
}

fn producer_config(namesrv: String) -> ProducerConfig {
    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let mut config = ProducerConfig::new(format!("admin-producer-{id}")).unwrap();
    config.client_config_mut().set_namesrv_addr(Some(namesrv));
    config
        .client_config_mut()
        .set_instance_name(Some(format!("admin-{}-{id}", std::process::id())));
    config
}

#[tokio::test]
async fn fetch_publish_queues_reports_missing_empty_and_malformed_routes() {
    let not_found = route_body(&[]);
    let (namesrv, namesrv_task, _) =
        name_server_peer_with_response(not_found, response::TOPIC_NOT_EXIST).await;
    let producer = Arc::new(Producer::new(producer_config(namesrv)));
    producer.start().await.unwrap();
    assert!(matches!(
        producer.fetch_publish_message_queues("orders").await,
        Err(ProducerError::NoRoute(topic)) if topic == "orders"
    ));
    producer.shutdown().await.unwrap();
    namesrv_task.abort();

    let (namesrv, namesrv_task, _) = name_server_peer(route_body(&[])).await;
    let producer = Arc::new(Producer::new(producer_config(namesrv)));
    producer.start().await.unwrap();
    assert!(matches!(
        producer.fetch_publish_message_queues("orders").await,
        Err(ProducerError::NoRoute(topic)) if topic == "orders"
    ));
    producer.shutdown().await.unwrap();
    namesrv_task.abort();

    let (namesrv, namesrv_task, _) = name_server_peer(b"{".to_vec()).await;
    let producer = Arc::new(Producer::new(producer_config(namesrv)));
    producer.start().await.unwrap();
    let malformed_result = producer.fetch_publish_message_queues("orders").await;
    assert!(
        matches!(
            &malformed_result,
            Err(ProducerError::Runtime(
                rocketmq_classic_core::runtime::RuntimeError::Rpc(RpcError::MalformedResponse {
                    request_code: request::GET_ROUTEINFO_BY_TOPIC,
                    ..
                })
            ))
        ),
        "unexpected malformed-route result: {malformed_result:?}"
    );
    producer.shutdown().await.unwrap();
    namesrv_task.abort();
}

#[tokio::test]
async fn create_topic_sorts_brokers_and_accepts_partial_success() {
    let (broker_a, broker_a_task, mut broker_a_requests) =
        broker_peer(vec![response::SERVICE_NOT_AVAILABLE; 5]).await;
    let (broker_b, broker_b_task, mut broker_b_requests) =
        broker_peer(vec![response::SUCCESS]).await;
    let route = route_body(&[("broker-b", &broker_b), ("broker-a", &broker_a)]);
    let (namesrv, namesrv_task, mut namesrv_requests) = name_server_peer(route).await;
    let mut config = producer_config(namesrv);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    let producer = Arc::new(Producer::new(config));
    producer.start().await.unwrap();

    let attributes = BTreeMap::from([("+unit".to_owned(), "true".to_owned())]);
    producer
        .create_topic("TBW102", "orders", 4, Some(&attributes))
        .await
        .unwrap();

    let route_request = timeout(Duration::from_secs(1), namesrv_requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(route_request.code, request::GET_ROUTEINFO_BY_TOPIC);
    assert_eq!(
        route_request
            .ext_fields
            .as_ref()
            .and_then(|fields| fields.get("topic"))
            .map(String::as_str),
        Some(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC)
    );

    let mut broker_a_count = 0;
    for _ in 0..5 {
        let command = timeout(Duration::from_secs(1), broker_a_requests.recv())
            .await
            .unwrap()
            .unwrap();
        broker_a_count += 1;
        assert_eq!(command.code, request::UPDATE_AND_CREATE_TOPIC);
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("topic"),
            Some(&"tenant%orders".to_owned())
        );
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("defaultTopic"),
            Some(&"TBW102".to_owned())
        );
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("attributes"),
            Some(&String::new())
        );
    }
    let success = timeout(Duration::from_secs(1), broker_b_requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(success.code, request::UPDATE_AND_CREATE_TOPIC);
    assert_eq!(broker_a_count, 5);

    producer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_a_task.abort();
    broker_b_task.abort();
}

#[tokio::test]
async fn view_message_unique_key_fallback_uses_java_query_defaults_and_namespaced_topic() {
    let (broker, broker_task, mut broker_requests) = query_broker_peer().await;
    let (namesrv, namesrv_task, mut namesrv_requests) =
        name_server_peer(route_body(&[("broker-a", &broker)])).await;
    let mut config = producer_config(namesrv);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    let producer = Arc::new(Producer::new(config));
    producer.start().await.unwrap();

    let before_millis = current_time_millis();
    let message = producer
        .view_message("orders", "producer-unique-1")
        .await
        .unwrap()
        .unwrap();
    let after_millis = current_time_millis();
    assert_eq!(message.topic(), "orders");
    assert_eq!(message.message_id(), Some("producer-unique-1"));
    assert_eq!(
        message.offset_message_id(),
        Some("C633641400002A9F0000018BCFE56829")
    );

    let route_request = timeout(Duration::from_secs(1), async {
        loop {
            let command = namesrv_requests.recv().await.unwrap();
            if command.code == request::GET_ROUTEINFO_BY_TOPIC
                && command
                    .ext_fields
                    .as_ref()
                    .and_then(|fields| fields.get("topic"))
                    .is_some_and(|topic| topic == "tenant%orders")
            {
                break command;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        route_request.ext_fields.unwrap().get("topic"),
        Some(&"tenant%orders".to_owned())
    );
    let query_request = timeout(Duration::from_secs(1), async {
        loop {
            let command = broker_requests.recv().await.unwrap();
            if command.code == request::QUERY_MESSAGE {
                break command;
            }
        }
    })
    .await
    .unwrap();
    let fields = query_request.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"tenant%orders".to_owned()));
    assert_eq!(fields.get("key"), Some(&"producer-unique-1".to_owned()));
    assert_eq!(fields.get("maxNum"), Some(&"32".to_owned()));
    let begin_timestamp: i64 = fields.get("beginTimestamp").unwrap().parse().unwrap();
    let expected_minimum = before_millis - 3 * 24 * 60 * 60 * 1_000;
    let expected_maximum = after_millis - 3 * 24 * 60 * 60 * 1_000;
    assert!((expected_minimum..=expected_maximum).contains(&begin_timestamp));
    assert_eq!(fields.get("endTimestamp"), Some(&i64::MAX.to_string()));
    assert_eq!(fields.get("indexType"), Some(&"U".to_owned()));
    assert_eq!(fields.get("_UNIQUE_KEY_QUERY"), Some(&"true".to_owned()));

    producer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn view_message_unique_key_fallback_preserves_java_terminal_no_message_error() {
    let (broker, broker_task, _) =
        query_broker_peer_with_response(response::SERVICE_NOT_AVAILABLE).await;
    let (namesrv, namesrv_task, _) = name_server_peer(route_body(&[("broker-a", &broker)])).await;
    let producer = Arc::new(Producer::new(producer_config(namesrv)));
    producer.start().await.unwrap();

    let query_error = producer.view_message("orders", "missing-unique-key").await;
    assert!(
        matches!(
            &query_error,
            Err(ProducerError::AdminQuery(
                rocketmq_classic_core::api::admin::AdminQueryError::Rpc(RpcError::Response {
                    request_code: request::QUERY_MESSAGE,
                    code: response::NO_MESSAGE,
                    ..
                })
            ))
        ),
        "unexpected terminal query result: {query_error:?}"
    );

    producer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn view_message_falls_back_to_unique_key_query_after_physical_lookup_failure() {
    let (broker, broker_task, mut broker_requests) = query_broker_peer().await;
    let (namesrv, namesrv_task, mut namesrv_requests) =
        name_server_peer(route_body(&[("broker-a", &broker)])).await;
    let mut config = producer_config(namesrv);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    let producer = Arc::new(Producer::new(config));
    producer.start().await.unwrap();

    let invalid = producer
        .view_message("orders", "producer-unique-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(invalid.message_id(), Some("producer-unique-1"));
    let invalid_query = timeout(Duration::from_secs(1), async {
        loop {
            let command = broker_requests.recv().await.unwrap();
            if command.code == request::QUERY_MESSAGE {
                break command;
            }
            assert_eq!(command.code, request::HEART_BEAT);
        }
    })
    .await
    .unwrap();
    assert_eq!(invalid_query.code, request::QUERY_MESSAGE);
    assert_eq!(
        invalid_query.ext_fields.unwrap().get("_UNIQUE_KEY_QUERY"),
        Some(&"true".to_owned())
    );

    let message_id = physical_message_id(&broker, 77);
    let fallback_error = producer
        .view_message("orders", &message_id)
        .await
        .unwrap_err();
    assert!(matches!(
        fallback_error,
        ProducerError::AdminQuery(rocketmq_classic_core::api::admin::AdminQueryError::Rpc(
            RpcError::Response {
                request_code: request::QUERY_MESSAGE,
                code: response::NO_MESSAGE,
                ..
            }
        ))
    ));
    let physical_request = timeout(Duration::from_secs(1), async {
        loop {
            let command = broker_requests.recv().await.unwrap();
            if command.code == request::VIEW_MESSAGE_BY_ID {
                break command;
            }
            assert_ne!(command.code, request::QUERY_MESSAGE);
        }
    })
    .await
    .unwrap();
    let fields = physical_request.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"orders".to_owned()));
    assert_eq!(fields.get("offset"), Some(&"77".to_owned()));
    let unique_query = timeout(Duration::from_secs(1), async {
        loop {
            let command = broker_requests.recv().await.unwrap();
            if command.code == request::QUERY_MESSAGE {
                break command;
            }
            assert_eq!(command.code, request::HEART_BEAT);
        }
    })
    .await
    .unwrap();
    assert_eq!(unique_query.code, request::QUERY_MESSAGE);
    let route = timeout(Duration::from_secs(1), namesrv_requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(route.code, request::GET_ROUTEINFO_BY_TOPIC);
    assert_eq!(
        route.ext_fields.unwrap().get("topic"),
        Some(&"tenant%orders".to_owned())
    );

    producer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn view_message_uses_java_permissive_decoder_for_non_hex_physical_ids() {
    let (broker, broker_task, mut broker_requests) = query_broker_peer().await;
    let (namesrv, namesrv_task, _) = name_server_peer(route_body(&[("broker-a", &broker)])).await;
    let producer = Arc::new(Producer::new(producer_config(namesrv)));
    producer.start().await.unwrap();

    let mut message_id = physical_message_id(&broker, 77);
    message_id.pop();
    message_id.push('G');
    assert!(matches!(
        producer.view_message("orders", &message_id).await,
        Err(ProducerError::AdminQuery(
            rocketmq_classic_core::api::admin::AdminQueryError::Rpc(RpcError::Response {
                request_code: request::QUERY_MESSAGE,
                code: response::NO_MESSAGE,
                ..
            })
        ))
    ));

    let physical_request = timeout(Duration::from_secs(1), async {
        loop {
            let command = broker_requests.recv().await.unwrap();
            if command.code == request::VIEW_MESSAGE_BY_ID {
                break command;
            }
            assert_ne!(command.code, request::QUERY_MESSAGE);
        }
    })
    .await
    .unwrap();
    let fields = physical_request.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"orders".to_owned()));
    assert_eq!(fields.get("offset"), Some(&"255".to_owned()));

    let fallback_query = timeout(Duration::from_secs(1), async {
        loop {
            let command = broker_requests.recv().await.unwrap();
            if command.code == request::QUERY_MESSAGE {
                break command;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        fallback_query.ext_fields.unwrap().get("key"),
        Some(&message_id)
    );

    producer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn create_topic_validates_topic_before_contacting_nameserver() {
    let (broker, broker_task, _) = broker_peer(vec![]).await;
    let (namesrv, namesrv_task, mut namesrv_requests) =
        name_server_peer(route_body(&[("broker", &broker)])).await;
    let producer = Arc::new(Producer::new(producer_config(namesrv)));
    producer.start().await.unwrap();

    assert!(
        producer
            .create_topic("TBW102", "bad.topic", 4, None)
            .await
            .is_err()
    );
    assert!(
        timeout(Duration::from_millis(50), namesrv_requests.recv())
            .await
            .is_err()
    );

    producer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn create_topic_returns_the_last_broker_error_after_five_attempts() {
    let (broker, broker_task, mut broker_requests) =
        broker_peer(vec![response::NO_PERMISSION; 5]).await;
    let (namesrv, namesrv_task, _) = name_server_peer(route_body(&[("broker", &broker)])).await;
    let producer = Arc::new(Producer::new(producer_config(namesrv)));
    producer.start().await.unwrap();

    let error = producer
        .create_topic("TBW102", "orders", 4, None)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ProducerError::Rpc(RpcError::Response {
            code: response::NO_PERMISSION,
            ..
        })
    ));
    for _ in 0..5 {
        let command = timeout(Duration::from_secs(1), broker_requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(command.code, request::UPDATE_AND_CREATE_TOPIC);
    }

    producer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn producer_admin_offset_facade_routes_namespaced_queues_to_the_named_broker() {
    let (broker, broker_task, mut broker_requests) = offset_broker_peer().await;
    let (namesrv, namesrv_task, mut namesrv_requests) =
        name_server_peer(route_body(&[("broker-a", &broker)])).await;
    let mut config = producer_config(namesrv);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    let producer = Arc::new(Producer::new(config));
    producer.start().await.unwrap();
    let queue = rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 3);

    assert_eq!(producer.search_offset(&queue, 9).await.unwrap(), 11);
    assert_eq!(producer.get_max_offset(&queue).await.unwrap(), 22);
    assert_eq!(producer.get_min_offset(&queue).await.unwrap(), 3);
    assert_eq!(
        producer.get_earliest_msg_store_time(&queue).await.unwrap(),
        44
    );

    let route_request = timeout(Duration::from_secs(1), namesrv_requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(route_request.code, request::GET_ROUTEINFO_BY_TOPIC);
    assert_eq!(
        route_request.ext_fields.as_ref().unwrap().get("topic"),
        Some(&"tenant%orders".to_owned())
    );

    for (expected_code, expected_timestamp) in [
        (request::SEARCH_OFFSET_BY_TIMESTAMP, Some("9")),
        (request::GET_MAX_OFFSET, None),
        (request::GET_MIN_OFFSET, None),
        (request::GET_EARLIEST_MSG_STORETIME, None),
    ] {
        let command = loop {
            let command = timeout(Duration::from_secs(1), broker_requests.recv())
                .await
                .unwrap()
                .unwrap();
            if command.code != request::HEART_BEAT {
                break command;
            }
        };
        assert_eq!(command.code, expected_code);
        let fields = command.ext_fields.as_ref().unwrap();
        assert_eq!(fields.get("topic"), Some(&"tenant%orders".to_owned()));
        assert_eq!(fields.get("queueId"), Some(&"3".to_owned()));
        assert_eq!(fields.get("bname"), Some(&"broker-a".to_owned()));
        if let Some(timestamp) = expected_timestamp {
            assert_eq!(fields.get("timestamp"), Some(&timestamp.to_owned()));
            assert_eq!(fields.get("boundaryType"), Some(&"LOWER".to_owned()));
        }
        if expected_code == request::GET_MAX_OFFSET {
            assert_eq!(fields.get("committed"), Some(&"true".to_owned()));
        }
    }

    producer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn producer_admin_facade_forwards_broker_permission_errors() {
    let (broker, broker_task, _) = broker_peer(vec![response::NO_PERMISSION; 7]).await;
    let (namesrv, namesrv_task, _) = name_server_peer(route_body(&[("broker-a", &broker)])).await;
    let producer = Arc::new(Producer::new(producer_config(namesrv)));
    producer.start().await.unwrap();

    let queue = rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 0);
    assert!(matches!(
        producer.get_max_offset(&queue).await,
        Err(ProducerError::Rpc(RpcError::Response {
            request_code: request::GET_MAX_OFFSET,
            code: response::NO_PERMISSION,
            ..
        }))
    ));
    assert!(matches!(
        producer
            .create_topic("TBW102", "admin-permission-topic", 1, None)
            .await,
        Err(ProducerError::Rpc(RpcError::Response {
            request_code: request::UPDATE_AND_CREATE_TOPIC,
            code: response::NO_PERMISSION,
            ..
        }))
    ));

    producer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();
}

#[test]
fn vip_topic_creation_uses_the_shared_java_address_transform() {
    assert_eq!(
        broker_vip_channel(true, "127.0.0.1:10911").unwrap(),
        "127.0.0.1:10909"
    );
}
