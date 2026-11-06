use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Write as _,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::Engine;
use bytes::BytesMut;
use rocketmq_classic_common::message::{MessageExt, MessageQueue};
use rocketmq_classic_consumer::{
    ConsumeConcurrentlyStatus, PullConsumerConfig, PushConsumer, PushConsumerConfig,
    PushMessageListener, pull::PullConsumer,
};
use rocketmq_classic_protocol::{
    codes::{request, response},
    header::{CommandHeader, HeaderMap, QueryMessageResponseHeader},
};
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
    ext_fields: Option<HeaderMap>,
) {
    let mut reply = RemotingCommand {
        code,
        opaque: request.opaque,
        body,
        ext_fields: ext_fields.map(Into::into),
        ..Default::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply).unwrap())
        .await
        .unwrap();
}

fn route_body(address: &str) -> Vec<u8> {
    route_body_with_queue_count(address, 4)
}

fn route_body_with_queue_count(address: &str, queue_count: i32) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "queueDatas": [{
            "brokerName": "broker-a",
            "readQueueNums": queue_count,
            "writeQueueNums": queue_count,
            "perm": 6,
            "topicSysFlag": 0
        }],
        "brokerDatas": [{
            "cluster": "DefaultCluster",
            "brokerName": "broker-a",
            "brokerAddrs": {"0": address}
        }],
        "filterServerTable": {}
    }))
    .unwrap()
}

fn empty_route_body() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "queueDatas": [],
        "brokerDatas": [],
        "filterServerTable": {}
    }))
    .unwrap()
}

fn java_message_record() -> Vec<u8> {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
    let record = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "ipv4-v1")
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

async fn nameserver_peer(route: Vec<u8>) -> (String, JoinHandle<()>) {
    nameserver_peer_with_response(route, response::SUCCESS).await
}

async fn nameserver_peer_with_response(
    route: Vec<u8>,
    response_code: i32,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            while let Some(command) = read_command(&mut stream).await {
                if command.code == request::GET_ROUTEINFO_BY_TOPIC {
                    write_response(
                        &mut stream,
                        &command,
                        response_code,
                        Some(route.clone()),
                        None,
                    )
                    .await;
                } else if !command.is_oneway() {
                    write_response(&mut stream, &command, response::SUCCESS, None, None).await;
                }
            }
        }
    });
    (address, task)
}

async fn nameserver_peer_with_route_responses(
    route_responses: Vec<(i32, Vec<u8>)>,
) -> (
    String,
    JoinHandle<()>,
    mpsc::UnboundedReceiver<Option<String>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (topic_sender, topic_receiver) = mpsc::unbounded_channel();
    let fallback = route_responses
        .last()
        .cloned()
        .unwrap_or((response::TOPIC_NOT_EXIST, Vec::new()));
    let task = tokio::spawn(async move {
        let mut route_responses = VecDeque::from(route_responses);
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            while let Some(command) = read_command(&mut stream).await {
                if command.code == request::GET_ROUTEINFO_BY_TOPIC {
                    let topic = command
                        .ext_fields
                        .as_ref()
                        .and_then(|fields| fields.get("topic"))
                        .cloned();
                    topic_sender.send(topic).unwrap();
                    let (response_code, route) = route_responses
                        .pop_front()
                        .unwrap_or_else(|| fallback.clone());
                    write_response(&mut stream, &command, response_code, Some(route), None).await;
                } else if !command.is_oneway() {
                    write_response(&mut stream, &command, response::SUCCESS, None, None).await;
                }
            }
        }
    });
    (address, task, topic_receiver)
}

async fn nameserver_peer_with_topic_routes(
    topic_routes: Vec<(String, Vec<u8>)>,
    default_route: Vec<u8>,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            while let Some(command) = read_command(&mut stream).await {
                if command.code == request::GET_ROUTEINFO_BY_TOPIC {
                    let topic = command
                        .ext_fields
                        .as_ref()
                        .and_then(|fields| fields.get("topic"));
                    let route = topic_routes
                        .iter()
                        .find(|(route_topic, _)| Some(route_topic) == topic)
                        .map(|(_, route)| route)
                        .unwrap_or(&default_route);
                    write_response(
                        &mut stream,
                        &command,
                        response::SUCCESS,
                        Some(route.clone()),
                        None,
                    )
                    .await;
                } else if !command.is_oneway() {
                    write_response(&mut stream, &command, response::SUCCESS, None, None).await;
                }
            }
        }
    });
    (address, task)
}

async fn broker_peer() -> (
    String,
    JoinHandle<()>,
    tokio::sync::mpsc::UnboundedReceiver<RemotingCommand>,
) {
    broker_peer_with_query_response(response::SUCCESS).await
}

async fn broker_peer_with_query_response(
    query_response_code: i32,
) -> (
    String,
    JoinHandle<()>,
    tokio::sync::mpsc::UnboundedReceiver<RemotingCommand>,
) {
    broker_peer_with_responses(query_response_code, response::SUCCESS, response::SUCCESS).await
}

async fn broker_peer_with_responses(
    query_response_code: i32,
    offset_response_code: i32,
    create_topic_response_code: i32,
) -> (
    String,
    JoinHandle<()>,
    tokio::sync::mpsc::UnboundedReceiver<RemotingCommand>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (sender, requests) = tokio::sync::mpsc::unbounded_channel();
    let record = java_message_record();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            while let Some(command) = read_command(&mut stream).await {
                let _ = sender.send(command.clone());
                let (code, body, ext_fields) = match command.code {
                    request::SEARCH_OFFSET_BY_TIMESTAMP
                    | request::GET_MAX_OFFSET
                    | request::GET_MIN_OFFSET
                    | request::GET_EARLIEST_MSG_STORETIME
                        if offset_response_code != response::SUCCESS =>
                    {
                        (offset_response_code, None, None)
                    }
                    request::SEARCH_OFFSET_BY_TIMESTAMP => (
                        response::SUCCESS,
                        None,
                        Some(HeaderMap::from([("offset".into(), "11".into())])),
                    ),
                    request::GET_MAX_OFFSET => (
                        response::SUCCESS,
                        None,
                        Some(HeaderMap::from([("offset".into(), "22".into())])),
                    ),
                    request::GET_MIN_OFFSET => (
                        response::SUCCESS,
                        None,
                        Some(HeaderMap::from([("offset".into(), "3".into())])),
                    ),
                    request::GET_EARLIEST_MSG_STORETIME => (
                        response::SUCCESS,
                        None,
                        Some(HeaderMap::from([("timestamp".into(), "44".into())])),
                    ),
                    request::PULL_MESSAGE => (
                        response::PULL_NOT_FOUND,
                        None,
                        Some(HeaderMap::from([
                            ("suggestWhichBrokerId".into(), "0".into()),
                            ("nextBeginOffset".into(), "0".into()),
                            ("minOffset".into(), "0".into()),
                            ("maxOffset".into(), "0".into()),
                        ])),
                    ),
                    request::GET_CONSUMER_LIST_BY_GROUP => (
                        response::SUCCESS,
                        Some(br#"{"consumerIdList":[]}"#.to_vec()),
                        None,
                    ),
                    request::UPDATE_AND_CREATE_TOPIC => (create_topic_response_code, None, None),
                    request::QUERY_MESSAGE => {
                        if query_response_code != response::SUCCESS {
                            (query_response_code, None, None)
                        } else {
                            let mut fields = HeaderMap::new();
                            let mut header = QueryMessageResponseHeader::default();
                            header.set_index_last_update_timestamp(Some(200));
                            header.set_index_last_update_phyoffset(Some(100));
                            header.encode_fields(&mut fields);
                            write_response(
                                &mut stream,
                                &command,
                                response::SUCCESS,
                                Some(record.clone()),
                                Some(fields),
                            )
                            .await;
                            continue;
                        }
                    }
                    request::VIEW_MESSAGE_BY_ID => (response::SUCCESS, Some(record.clone()), None),
                    _ => (response::SUCCESS, None, None),
                };
                write_response(&mut stream, &command, code, body, ext_fields).await;
            }
        }
    });
    (address, task, requests)
}

#[tokio::test]
async fn push_admin_offset_facade_routes_namespaced_queues_to_the_named_broker() {
    let (broker, broker_task, mut broker_requests) = broker_peer().await;
    let (namesrv, namesrv_task) = nameserver_peer(route_body(&broker)).await;
    let mut config = PushConsumerConfig::new("push-admin-offsets").unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    config
        .client_config_mut()
        .set_instance_name(Some("push-admin-offsets".into()));
    config.client_config_mut().set_namesrv_addr(Some(namesrv));
    let consumer = Arc::new(PushConsumer::new(config));
    consumer
        .register_message_listener(PushMessageListener::concurrently(
            |_messages: Vec<MessageExt>, context| async move {
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        ))
        .await
        .unwrap();
    consumer.start().await.unwrap();

    let queue = MessageQueue::new("orders", "broker-a", 3);
    assert_eq!(consumer.search_offset(&queue, 9).await.unwrap(), 11);
    assert_eq!(consumer.get_max_offset(&queue).await.unwrap(), 22);
    assert_eq!(consumer.get_min_offset(&queue).await.unwrap(), 3);
    assert_eq!(
        consumer.get_earliest_msg_store_time(&queue).await.unwrap(),
        44
    );

    for (expected_code, expected_timestamp) in [
        (request::SEARCH_OFFSET_BY_TIMESTAMP, Some("9")),
        (request::GET_MAX_OFFSET, None),
        (request::GET_MIN_OFFSET, None),
        (request::GET_EARLIEST_MSG_STORETIME, None),
    ] {
        let command = timeout(Duration::from_secs(1), async {
            loop {
                let command = broker_requests.recv().await.unwrap();
                if command.code == expected_code {
                    break command;
                }
            }
        })
        .await
        .unwrap();
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

    consumer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn push_reset_offset_by_timestamp_matches_java_subscribed_topic_flow() {
    let group = "phase18-push-implementation-accessor";
    let (broker, broker_task, mut broker_requests) = broker_peer().await;
    let (namesrv, namesrv_task) = nameserver_peer_with_topic_routes(
        vec![
            ("orders".to_owned(), route_body_with_queue_count(&broker, 2)),
            (format!("%RETRY%{group}"), empty_route_body()),
        ],
        empty_route_body(),
    )
    .await;
    let mut config = PushConsumerConfig::new(group).unwrap();
    config
        .client_config_mut()
        .set_instance_name(Some("push-reset-offset-by-timestamp".into()));
    config.client_config_mut().set_namesrv_addr(Some(namesrv));
    let consumer = Arc::new(PushConsumer::new(config));
    consumer.subscribe("orders", "*").await.unwrap();
    consumer
        .register_message_listener(PushMessageListener::concurrently(
            |_messages: Vec<MessageExt>, context| async move {
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        ))
        .await
        .unwrap();
    consumer.start().await.unwrap();

    consumer.reset_offset_by_timestamp(12345).await.unwrap();
    let mut searches = Vec::new();
    timeout(Duration::from_secs(2), async {
        while searches.len() < 2 {
            let command = broker_requests.recv().await.unwrap();
            if command.code == request::SEARCH_OFFSET_BY_TIMESTAMP {
                searches.push(command);
            }
        }
    })
    .await
    .unwrap();
    searches.sort_by_key(|command| {
        command
            .ext_fields
            .as_ref()
            .and_then(|fields| fields.get("queueId"))
            .and_then(|value| value.parse::<i32>().ok())
    });
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    for (queue_id, command) in searches.iter().enumerate() {
        let fields = command.ext_fields.as_ref().unwrap();
        assert_eq!(fields.get("topic"), Some(&"orders".to_owned()));
        assert_eq!(fields.get("queueId"), Some(&queue_id.to_string()));
        assert_eq!(fields.get("bname"), Some(&"broker-a".to_owned()));
        assert_eq!(fields.get("timestamp"), Some(&"12345".to_owned()));
        assert_eq!(fields.get("boundaryType"), Some(&"LOWER".to_owned()));
        assert_eq!(
            fixture["timestampResetSearches"][queue_id],
            format!("orders|broker-a|{queue_id}|12345")
        );
    }
    let actual_offsets = consumer
        .get_offset_store()
        .unwrap()
        .clone_offset_table(Some("orders"))
        .into_iter()
        .map(|(queue, offset)| {
            format!(
                "{}|{}|{}|{}",
                queue.topic().unwrap(),
                queue.broker_name().unwrap(),
                queue.queue_id(),
                offset
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    // Java applies resetOffset only to queues currently present in the
    // consumer's ProcessQueue table. This scripted startup has no assignment,
    // so the search results are collected but no offset-table entry remains.
    assert!(fixture["timestampResetCalls"][0]["offsets"].is_array());
    assert!(actual_offsets.is_empty());

    consumer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn push_and_pull_admin_facades_forward_broker_permission_errors() {
    let (push_broker, push_broker_task, _) = broker_peer_with_responses(
        response::SUCCESS,
        response::NO_PERMISSION,
        response::NO_PERMISSION,
    )
    .await;
    let (push_namesrv, push_namesrv_task) = nameserver_peer(route_body(&push_broker)).await;
    let mut push_config = PushConsumerConfig::new("push-admin-permission-error").unwrap();
    push_config
        .client_config_mut()
        .set_namesrv_addr(Some(push_namesrv));
    push_config
        .client_config_mut()
        .set_instance_name(Some("push-admin-permission-error".into()));
    let push = Arc::new(PushConsumer::new(push_config));
    push.register_message_listener(PushMessageListener::concurrently(
        |_messages: Vec<MessageExt>, context| async move {
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    ))
    .await
    .unwrap();
    push.start().await.unwrap();
    let queue = MessageQueue::new("orders", "broker-a", 0);
    assert!(matches!(
        push.get_max_offset(&queue).await,
        Err(rocketmq_classic_consumer::PushConsumerError::Rpc(
            rocketmq_classic_core::error::RpcError::Response {
                request_code: request::GET_MAX_OFFSET,
                code: response::NO_PERMISSION,
                ..
            }
        ))
    ));
    assert!(matches!(
        push.create_topic("TBW102", "admin-permission-topic", 1, None)
            .await,
        Err(rocketmq_classic_consumer::PushConsumerError::AdminQuery(
            rocketmq_classic_core::api::admin::AdminQueryError::Rpc(
                rocketmq_classic_core::error::RpcError::Response {
                    request_code: request::UPDATE_AND_CREATE_TOPIC,
                    code: response::NO_PERMISSION,
                    ..
                }
            )
        ))
    ));
    push.shutdown().await.unwrap();
    push_namesrv_task.abort();
    push_broker_task.abort();

    let (pull_broker, pull_broker_task, _) = broker_peer_with_responses(
        response::SUCCESS,
        response::NO_PERMISSION,
        response::NO_PERMISSION,
    )
    .await;
    let (pull_namesrv, pull_namesrv_task) = nameserver_peer(route_body(&pull_broker)).await;
    let mut pull_config = PullConsumerConfig::new("pull-admin-permission-error").unwrap();
    pull_config
        .client_config_mut()
        .set_namesrv_addr(Some(pull_namesrv));
    pull_config
        .client_config_mut()
        .set_instance_name(Some("pull-admin-permission-error".into()));
    let pull = Arc::new(PullConsumer::new(pull_config));
    pull.start().await.unwrap();
    assert!(matches!(
        pull.get_max_offset(&queue).await,
        Err(rocketmq_classic_consumer::PullConsumerError::Rpc(
            rocketmq_classic_core::error::RpcError::Response {
                request_code: request::GET_MAX_OFFSET,
                code: response::NO_PERMISSION,
                ..
            }
        ))
    ));
    assert!(matches!(
        pull.create_topic("TBW102", "admin-permission-topic", 1, None)
            .await,
        Err(rocketmq_classic_consumer::PullConsumerError::AdminQuery(
            rocketmq_classic_core::api::admin::AdminQueryError::Rpc(
                rocketmq_classic_core::error::RpcError::Response {
                    request_code: request::UPDATE_AND_CREATE_TOPIC,
                    code: response::NO_PERMISSION,
                    ..
                }
            )
        ))
    ));
    pull.shutdown().await.unwrap();
    pull_namesrv_task.abort();
    pull_broker_task.abort();
}

#[tokio::test]
async fn push_and_pull_query_facades_preserve_java_client_message_ids() {
    let (push_broker, push_broker_task, mut push_requests) = broker_peer().await;
    let (push_namesrv, push_namesrv_task) = nameserver_peer(route_body(&push_broker)).await;
    let mut push_config = PushConsumerConfig::new("push-admin-query").unwrap();
    push_config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    push_config
        .client_config_mut()
        .set_instance_name(Some("push-admin-query".into()));
    push_config
        .client_config_mut()
        .set_namesrv_addr(Some(push_namesrv));
    let push = Arc::new(PushConsumer::new(push_config));
    push.register_message_listener(PushMessageListener::concurrently(
        |_messages: Vec<MessageExt>, context| async move {
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    ))
    .await
    .unwrap();
    push.start().await.unwrap();

    let push_before = current_time_millis();
    let push_message = push
        .view_message("orders", "producer-unique-1")
        .await
        .unwrap()
        .unwrap();
    let push_after = current_time_millis();
    assert_eq!(push_message.topic(), "orders");
    assert_eq!(push_message.message_id(), Some("producer-unique-1"));
    assert_eq!(
        push_message.offset_message_id(),
        Some("C633641400002A9F0000018BCFE56829")
    );
    let push_query = timeout(Duration::from_secs(1), async {
        loop {
            let command = push_requests.recv().await.unwrap();
            if command.code == request::QUERY_MESSAGE {
                break command;
            }
        }
    })
    .await
    .unwrap();
    let fields = push_query.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"tenant%orders".to_owned()));
    assert_eq!(fields.get("maxNum"), Some(&"32".to_owned()));
    let begin_timestamp: i64 = fields.get("beginTimestamp").unwrap().parse().unwrap();
    assert!(
        (push_before - 3 * 24 * 60 * 60 * 1_000..=push_after - 3 * 24 * 60 * 60 * 1_000)
            .contains(&begin_timestamp)
    );
    assert_eq!(fields.get("endTimestamp"), Some(&i64::MAX.to_string()));
    assert_eq!(fields.get("indexType"), Some(&"U".to_owned()));
    assert_eq!(fields.get("_UNIQUE_KEY_QUERY"), Some(&"true".to_owned()));
    push.shutdown().await.unwrap();
    push_namesrv_task.abort();
    push_broker_task.abort();

    let (pull_broker, pull_broker_task, mut pull_requests) = broker_peer().await;
    let (pull_namesrv, pull_namesrv_task) = nameserver_peer(route_body(&pull_broker)).await;
    let mut pull_config = PullConsumerConfig::new("pull-admin-query").unwrap();
    pull_config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    pull_config
        .client_config_mut()
        .set_instance_name(Some("pull-admin-query".into()));
    pull_config
        .client_config_mut()
        .set_namesrv_addr(Some(pull_namesrv));
    let pull = Arc::new(PullConsumer::new(pull_config));
    pull.start().await.unwrap();

    let pull_before = current_time_millis();
    let pull_message = pull
        .view_message("orders", "producer-unique-1")
        .await
        .unwrap()
        .unwrap();
    let pull_after = current_time_millis();
    assert_eq!(pull_message.topic(), "orders");
    assert_eq!(pull_message.message_id(), Some("producer-unique-1"));
    assert_eq!(
        pull_message.offset_message_id(),
        Some("C633641400002A9F0000018BCFE56829")
    );
    let pull_query = timeout(Duration::from_secs(1), async {
        loop {
            let command = pull_requests.recv().await.unwrap();
            if command.code == request::QUERY_MESSAGE {
                break command;
            }
        }
    })
    .await
    .unwrap();
    let fields = pull_query.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"tenant%orders".to_owned()));
    assert_eq!(fields.get("maxNum"), Some(&"32".to_owned()));
    let begin_timestamp: i64 = fields.get("beginTimestamp").unwrap().parse().unwrap();
    assert!(
        (pull_before - 3 * 24 * 60 * 60 * 1_000..=pull_after - 3 * 24 * 60 * 60 * 1_000)
            .contains(&begin_timestamp)
    );
    assert_eq!(fields.get("endTimestamp"), Some(&i64::MAX.to_string()));
    assert_eq!(fields.get("indexType"), Some(&"U".to_owned()));
    assert_eq!(fields.get("_UNIQUE_KEY_QUERY"), Some(&"true".to_owned()));
    pull.shutdown().await.unwrap();
    pull_namesrv_task.abort();
    pull_broker_task.abort();
}

#[tokio::test]
async fn push_and_pull_view_message_unique_key_fallbacks_preserve_terminal_no_message_errors() {
    let (push_broker, push_broker_task, _) =
        broker_peer_with_query_response(response::SERVICE_NOT_AVAILABLE).await;
    let (push_namesrv, push_namesrv_task) = nameserver_peer(route_body(&push_broker)).await;
    let mut push_config = PushConsumerConfig::new("push-admin-query-no-message").unwrap();
    push_config
        .client_config_mut()
        .set_namesrv_addr(Some(push_namesrv));
    push_config
        .client_config_mut()
        .set_instance_name(Some("push-admin-query-no-message".into()));
    let push = Arc::new(PushConsumer::new(push_config));
    push.register_message_listener(PushMessageListener::concurrently(
        |_messages: Vec<MessageExt>, context| async move {
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    ))
    .await
    .unwrap();
    push.start().await.unwrap();
    assert!(matches!(
        push.view_message("orders", "missing-key").await,
        Err(rocketmq_classic_consumer::PushConsumerError::AdminQuery(
            rocketmq_classic_core::api::admin::AdminQueryError::Rpc(
                rocketmq_classic_core::error::RpcError::Response {
                    request_code: request::QUERY_MESSAGE,
                    code: response::NO_MESSAGE,
                    ..
                }
            )
        ))
    ));
    push.shutdown().await.unwrap();
    push_namesrv_task.abort();
    push_broker_task.abort();

    let (pull_broker, pull_broker_task, _) =
        broker_peer_with_query_response(response::SERVICE_NOT_AVAILABLE).await;
    let (pull_namesrv, pull_namesrv_task) = nameserver_peer(route_body(&pull_broker)).await;
    let mut pull_config = PullConsumerConfig::new("pull-admin-query-no-message").unwrap();
    pull_config
        .client_config_mut()
        .set_namesrv_addr(Some(pull_namesrv));
    pull_config
        .client_config_mut()
        .set_instance_name(Some("pull-admin-query-no-message".into()));
    let pull = Arc::new(PullConsumer::new(pull_config));
    pull.start().await.unwrap();
    let pull_query_error = pull.view_message("orders", "missing-key").await;
    assert!(
        matches!(
            &pull_query_error,
            Err(rocketmq_classic_consumer::PullConsumerError::AdminQuery(
                rocketmq_classic_core::api::admin::AdminQueryError::Rpc(
                    rocketmq_classic_core::error::RpcError::Response {
                        request_code: request::QUERY_MESSAGE,
                        code: response::NO_MESSAGE,
                        ..
                    }
                )
            ))
        ),
        "unexpected pull query result: {pull_query_error:?}"
    );
    pull.shutdown().await.unwrap();
    pull_namesrv_task.abort();
    pull_broker_task.abort();
}

#[tokio::test]
async fn push_and_pull_view_message_match_java_unique_key_fallback_and_physical_lookup() {
    let (push_broker, push_broker_task, mut push_requests) = broker_peer().await;
    let (push_namesrv, push_namesrv_task) = nameserver_peer(route_body(&push_broker)).await;
    let mut push_config = PushConsumerConfig::new("push-admin-view").unwrap();
    push_config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    push_config
        .client_config_mut()
        .set_instance_name(Some("push-admin-view".into()));
    push_config
        .client_config_mut()
        .set_namesrv_addr(Some(push_namesrv));
    let push = Arc::new(PushConsumer::new(push_config));
    push.register_message_listener(PushMessageListener::concurrently(
        |_messages: Vec<MessageExt>, context| async move {
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    ))
    .await
    .unwrap();
    push.start().await.unwrap();

    let invalid = push
        .view_message("orders", "producer-unique-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(invalid.message_id(), Some("producer-unique-1"));
    let push_query = timeout(Duration::from_secs(1), async {
        loop {
            let command = push_requests.recv().await.unwrap();
            if command.code == request::QUERY_MESSAGE {
                break command;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(push_query.code, request::QUERY_MESSAGE);
    assert_eq!(
        push_query.ext_fields.unwrap().get("topic"),
        Some(&"tenant%orders".to_owned())
    );
    let push_id = physical_message_id(&push_broker, 71);
    let push_message = push
        .view_message("orders", &push_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(push_message.topic(), "orders");
    assert_eq!(push_message.message_id(), Some("producer-unique-1"));
    let push_view = timeout(Duration::from_secs(1), async {
        loop {
            let command = push_requests.recv().await.unwrap();
            if command.code == request::VIEW_MESSAGE_BY_ID {
                break command;
            }
            assert_ne!(command.code, request::QUERY_MESSAGE);
        }
    })
    .await
    .unwrap();
    let fields = push_view.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"tenant%orders".to_owned()));
    assert_eq!(fields.get("offset"), Some(&"71".to_owned()));
    while let Ok(command) = push_requests.try_recv() {
        assert_ne!(command.code, request::QUERY_MESSAGE);
    }

    let mut push_non_hex_id = physical_message_id(&push_broker, 71);
    push_non_hex_id.pop();
    push_non_hex_id.push('G');
    let push_message = push
        .view_message("orders", &push_non_hex_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(push_message.message_id(), Some("producer-unique-1"));
    let push_non_hex_view = timeout(Duration::from_secs(1), async {
        loop {
            let command = push_requests.recv().await.unwrap();
            if command.code == request::VIEW_MESSAGE_BY_ID {
                break command;
            }
            assert_ne!(command.code, request::QUERY_MESSAGE);
        }
    })
    .await
    .unwrap();
    assert_eq!(
        push_non_hex_view.ext_fields.unwrap().get("offset"),
        Some(&"255".to_owned())
    );
    while let Ok(command) = push_requests.try_recv() {
        assert_ne!(command.code, request::QUERY_MESSAGE);
    }
    push.shutdown().await.unwrap();
    push_namesrv_task.abort();
    push_broker_task.abort();

    let (pull_broker, pull_broker_task, mut pull_requests) = broker_peer().await;
    let (pull_namesrv, pull_namesrv_task) = nameserver_peer(route_body(&pull_broker)).await;
    let mut pull_config = PullConsumerConfig::new("pull-admin-view").unwrap();
    pull_config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    pull_config
        .client_config_mut()
        .set_instance_name(Some("pull-admin-view".into()));
    pull_config
        .client_config_mut()
        .set_namesrv_addr(Some(pull_namesrv));
    let pull = Arc::new(PullConsumer::new(pull_config));
    pull.start().await.unwrap();

    let invalid = pull
        .view_message("orders", "producer-unique-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(invalid.message_id(), Some("producer-unique-1"));
    let pull_query = timeout(Duration::from_secs(1), async {
        loop {
            let command = pull_requests.recv().await.unwrap();
            if command.code == request::QUERY_MESSAGE {
                break command;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(pull_query.code, request::QUERY_MESSAGE);
    assert_eq!(
        pull_query.ext_fields.unwrap().get("topic"),
        Some(&"tenant%orders".to_owned())
    );
    let pull_id = physical_message_id(&pull_broker, 72);
    let pull_message = pull
        .view_message("orders", &pull_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pull_message.topic(), "orders");
    assert_eq!(pull_message.message_id(), Some("producer-unique-1"));
    let pull_view = timeout(Duration::from_secs(1), async {
        loop {
            let command = pull_requests.recv().await.unwrap();
            if command.code == request::VIEW_MESSAGE_BY_ID {
                break command;
            }
            assert_ne!(command.code, request::QUERY_MESSAGE);
        }
    })
    .await
    .unwrap();
    let fields = pull_view.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"orders".to_owned()));
    assert_eq!(fields.get("offset"), Some(&"72".to_owned()));
    while let Ok(command) = pull_requests.try_recv() {
        assert_ne!(command.code, request::QUERY_MESSAGE);
    }

    let mut pull_non_hex_id = physical_message_id(&pull_broker, 72);
    pull_non_hex_id.pop();
    pull_non_hex_id.push('G');
    let pull_message = pull
        .view_message("orders", &pull_non_hex_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pull_message.message_id(), Some("producer-unique-1"));
    let pull_non_hex_view = timeout(Duration::from_secs(1), async {
        loop {
            let command = pull_requests.recv().await.unwrap();
            if command.code == request::VIEW_MESSAGE_BY_ID {
                break command;
            }
            assert_ne!(command.code, request::QUERY_MESSAGE);
        }
    })
    .await
    .unwrap();
    assert_eq!(
        pull_non_hex_view.ext_fields.unwrap().get("offset"),
        Some(&"255".to_owned())
    );
    while let Ok(command) = pull_requests.try_recv() {
        assert_ne!(command.code, request::QUERY_MESSAGE);
    }
    pull.shutdown().await.unwrap();
    pull_namesrv_task.abort();
    pull_broker_task.abort();
}

#[tokio::test]
async fn push_and_pull_create_topic_and_fetch_subscribe_queues_match_java_facades() {
    let (push_broker, push_broker_task, mut push_requests) = broker_peer().await;
    let (push_namesrv, push_namesrv_task) = nameserver_peer(route_body(&push_broker)).await;
    let mut push_config = PushConsumerConfig::new("push-admin-topic").unwrap();
    push_config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    push_config
        .client_config_mut()
        .set_instance_name(Some("push-admin-topic".into()));
    push_config
        .client_config_mut()
        .set_namesrv_addr(Some(push_namesrv));
    let push = Arc::new(PushConsumer::new(push_config));
    push.register_message_listener(PushMessageListener::concurrently(
        |_messages: Vec<MessageExt>, context| async move {
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    ))
    .await
    .unwrap();
    push.start().await.unwrap();

    let attributes = BTreeMap::from([("+ignored".to_owned(), "true".to_owned())]);
    push.create_topic("TBW102", "created", 3, Some(&attributes))
        .await
        .unwrap();
    let push_create = timeout(Duration::from_secs(1), async {
        loop {
            let command = push_requests.recv().await.unwrap();
            if command.code == request::UPDATE_AND_CREATE_TOPIC {
                break command;
            }
        }
    })
    .await
    .unwrap();
    let fields = push_create.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"tenant%created".to_owned()));
    assert_eq!(fields.get("defaultTopic"), Some(&"TBW102".to_owned()));
    assert_eq!(fields.get("readQueueNums"), Some(&"3".to_owned()));
    assert_eq!(fields.get("writeQueueNums"), Some(&"3".to_owned()));
    assert_eq!(fields.get("topicSysFlag"), Some(&"0".to_owned()));
    assert_eq!(fields.get("attributes"), Some(&String::new()));

    let push_queues = push.fetch_subscribe_message_queues("orders").await.unwrap();
    assert_eq!(push_queues.len(), 4);
    assert!(push_queues.contains(&MessageQueue::new("orders", "broker-a", 0)));
    push.shutdown().await.unwrap();
    push_namesrv_task.abort();
    push_broker_task.abort();

    let (pull_broker, pull_broker_task, mut pull_requests) = broker_peer().await;
    let (pull_namesrv, pull_namesrv_task) = nameserver_peer(route_body(&pull_broker)).await;
    let mut pull_config = PullConsumerConfig::new("pull-admin-topic").unwrap();
    pull_config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    pull_config
        .client_config_mut()
        .set_instance_name(Some("pull-admin-topic".into()));
    pull_config
        .client_config_mut()
        .set_namesrv_addr(Some(pull_namesrv));
    let pull = Arc::new(PullConsumer::new(pull_config));
    pull.start().await.unwrap();

    pull.create_topic_with_sys_flag("TBW102", "created", 2, 7, Some(&attributes))
        .await
        .unwrap();
    let pull_create = timeout(Duration::from_secs(1), async {
        loop {
            let command = pull_requests.recv().await.unwrap();
            if command.code == request::UPDATE_AND_CREATE_TOPIC {
                break command;
            }
        }
    })
    .await
    .unwrap();
    let fields = pull_create.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"tenant%created".to_owned()));
    assert_eq!(fields.get("defaultTopic"), Some(&"TBW102".to_owned()));
    assert_eq!(fields.get("readQueueNums"), Some(&"2".to_owned()));
    assert_eq!(fields.get("topicSysFlag"), Some(&"7".to_owned()));
    assert_eq!(fields.get("attributes"), Some(&String::new()));

    let pull_queues = pull.fetch_subscribe_message_queues("orders").await.unwrap();
    assert_eq!(pull_queues.len(), 4);
    assert!(pull_queues.contains(&MessageQueue::new("orders", "broker-a", 0)));
    let publish_queues = pull
        .fetch_publish_message_queues("tenant%orders")
        .await
        .unwrap();
    assert_eq!(
        publish_queues,
        (0..4)
            .map(|queue_id| MessageQueue::new("orders", "broker-a", queue_id))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        pull.fetch_publish_message_queues("tenant%orders")
            .await
            .unwrap(),
        publish_queues,
        "an unchanged NameServer route still returns the cached publish queue list"
    );
    pull.shutdown().await.unwrap();
    pull_namesrv_task.abort();
    pull_broker_task.abort();
}

#[tokio::test]
async fn pull_publish_queue_refresh_rejects_a_route_removed_from_name_server() {
    let route = route_body("unused");
    let (namesrv, namesrv_task, mut requested_topics) = nameserver_peer_with_route_responses(vec![
        (response::SUCCESS, route.clone()),
        (response::TOPIC_NOT_EXIST, route),
    ])
    .await;
    let mut config = PullConsumerConfig::new("pull-admin-publish-route-refresh").unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    config.client_config_mut().set_namesrv_addr(Some(namesrv));
    config
        .client_config_mut()
        .set_instance_name(Some("pull-admin-publish-route-refresh".into()));
    let pull = Arc::new(PullConsumer::new(config));
    pull.start().await.unwrap();

    let queues = pull
        .fetch_publish_message_queues("tenant%orders")
        .await
        .unwrap();
    assert_eq!(
        queues,
        (0..4)
            .map(|queue_id| MessageQueue::new("orders", "broker-a", queue_id))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        requested_topics.recv().await.unwrap().as_deref(),
        Some("tenant%orders")
    );
    let error = pull
        .fetch_publish_message_queues("tenant%orders")
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Can not find Message Queue for this topic, tenant%orders"
    );
    assert!(matches!(
        error,
        rocketmq_classic_consumer::PullConsumerError::PublishMessageQueuesFailed { topic, .. }
            if topic == "tenant%orders"
    ));
    assert_eq!(
        requested_topics.recv().await.unwrap().as_deref(),
        Some("tenant%orders")
    );

    pull.shutdown().await.unwrap();
    namesrv_task.abort();
}

#[tokio::test]
async fn push_and_pull_subscribe_queue_queries_handle_missing_empty_and_malformed_routes() {
    let (namesrv, namesrv_task) =
        nameserver_peer_with_response(route_body("unused"), response::TOPIC_NOT_EXIST).await;
    let mut config = PushConsumerConfig::new("push-admin-route-not-found").unwrap();
    config.client_config_mut().set_namesrv_addr(Some(namesrv));
    config
        .client_config_mut()
        .set_instance_name(Some("push-admin-route-not-found".into()));
    let push = Arc::new(PushConsumer::new(config));
    push.register_message_listener(PushMessageListener::concurrently(
        |_messages: Vec<MessageExt>, context| async move {
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    ))
    .await
    .unwrap();
    push.start().await.unwrap();
    assert!(push.fetch_subscribe_message_queues("orders").await.is_err());
    push.shutdown().await.unwrap();
    namesrv_task.abort();

    let empty_route = serde_json::to_vec(&json!({
        "queueDatas": [],
        "brokerDatas": [],
        "filterServerTable": {}
    }))
    .unwrap();
    let (broker, broker_task, _) = broker_peer().await;
    let (namesrv, namesrv_task) = nameserver_peer_with_topic_routes(
        vec![("orders".into(), empty_route.clone())],
        route_body(&broker),
    )
    .await;
    let mut config = PushConsumerConfig::new("push-admin-empty-route").unwrap();
    config.client_config_mut().set_namesrv_addr(Some(namesrv));
    config
        .client_config_mut()
        .set_instance_name(Some("push-admin-empty-route".into()));
    let push = Arc::new(PushConsumer::new(config));
    push.register_message_listener(PushMessageListener::concurrently(
        |_messages: Vec<MessageExt>, context| async move {
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    ))
    .await
    .unwrap();
    push.start().await.unwrap();
    assert!(
        push.fetch_subscribe_message_queues("orders")
            .await
            .unwrap()
            .is_empty()
    );
    push.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();

    let (broker, broker_task, _) = broker_peer().await;
    let (namesrv, namesrv_task) = nameserver_peer_with_topic_routes(
        vec![("orders".into(), b"{".to_vec())],
        route_body(&broker),
    )
    .await;
    let mut config = PushConsumerConfig::new("push-admin-malformed-route").unwrap();
    config.client_config_mut().set_namesrv_addr(Some(namesrv));
    config
        .client_config_mut()
        .set_instance_name(Some("push-admin-malformed-route".into()));
    let push = Arc::new(PushConsumer::new(config));
    push.register_message_listener(PushMessageListener::concurrently(
        |_messages: Vec<MessageExt>, context| async move {
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    ))
    .await
    .unwrap();
    push.start().await.unwrap();
    assert!(push.fetch_subscribe_message_queues("orders").await.is_err());
    push.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();

    let (namesrv, namesrv_task) = nameserver_peer(b"{".to_vec()).await;
    let mut config = PullConsumerConfig::new("pull-admin-malformed-route").unwrap();
    config.client_config_mut().set_namesrv_addr(Some(namesrv));
    config
        .client_config_mut()
        .set_instance_name(Some("pull-admin-malformed-route".into()));
    let pull = Arc::new(PullConsumer::new(config));
    pull.start().await.unwrap();
    assert!(pull.fetch_subscribe_message_queues("orders").await.is_err());
    let error = pull
        .fetch_publish_message_queues("orders")
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Can not find Message Queue for this topic, orders"
    );
    assert!(matches!(
        error,
        rocketmq_classic_consumer::PullConsumerError::PublishMessageQueuesFailed { topic, .. }
            if topic == "orders"
    ));
    pull.shutdown().await.unwrap();
    namesrv_task.abort();

    let (namesrv, namesrv_task) =
        nameserver_peer_with_response(route_body("unused"), response::TOPIC_NOT_EXIST).await;
    let mut config = PullConsumerConfig::new("pull-admin-route-not-found").unwrap();
    config.client_config_mut().set_namesrv_addr(Some(namesrv));
    config
        .client_config_mut()
        .set_instance_name(Some("pull-admin-route-not-found".into()));
    let pull = Arc::new(PullConsumer::new(config));
    pull.start().await.unwrap();
    assert!(pull.fetch_subscribe_message_queues("orders").await.is_err());
    let error = pull
        .fetch_publish_message_queues("orders")
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Can not find Message Queue for this topic, orders"
    );
    assert!(matches!(
        error,
        rocketmq_classic_consumer::PullConsumerError::PublishMessageQueuesFailed { topic, .. }
            if topic == "orders"
    ));
    pull.shutdown().await.unwrap();
    namesrv_task.abort();

    let (namesrv, namesrv_task) = nameserver_peer(empty_route).await;
    let mut config = PullConsumerConfig::new("pull-admin-empty-route").unwrap();
    config.client_config_mut().set_namesrv_addr(Some(namesrv));
    config
        .client_config_mut()
        .set_instance_name(Some("pull-admin-empty-route".into()));
    let pull = Arc::new(PullConsumer::new(config));
    pull.start().await.unwrap();
    assert!(matches!(
        pull.fetch_subscribe_message_queues("orders").await,
        Err(rocketmq_classic_consumer::PullConsumerError::EmptyMessageQueues(topic))
            if topic == "orders"
    ));
    let error = pull
        .fetch_publish_message_queues("orders")
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Unknow why, Can not find Message Queue for this topic, orders"
    );
    assert!(matches!(
        error,
        rocketmq_classic_consumer::PullConsumerError::NoPublishMessageQueues(topic)
            if topic == "orders"
    ));
    pull.shutdown().await.unwrap();
    namesrv_task.abort();
}
