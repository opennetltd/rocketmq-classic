use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::BytesMut;
use rocketmq_classic_common::message::{
    Message, MessageQueue,
    constants::{KEY_SEPARATOR, PROPERTY_KEYS, PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX},
    decode_properties,
};
use rocketmq_classic_core::{
    runtime::{ClientRuntimeManager, ProducerRuntime},
    trace::{LocalTransactionState, MessageType, TraceBean, TraceContext, TraceType},
};
use rocketmq_classic_producer::{
    Producer, ProducerConfig, ProducerError, RecallMessageHandle, SendStatus, TraceDispatcher,
    TraceProducerKind,
};
use rocketmq_classic_protocol::{
    codes::{request, response},
    header::HeaderMap,
};
use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

#[derive(Clone, Debug)]
struct SentRequest {
    broker: String,
    request_code: i32,
    queue_id: Option<i32>,
    producer_group: Option<String>,
    topic: Option<String>,
    body: Option<Vec<u8>>,
    properties: Option<String>,
    unique_id: Option<String>,
    oneway: bool,
}

async fn read_command(stream: &mut TcpStream) -> Option<RemotingCommand> {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.ok()?;
    let frame_length = u32::from_be_bytes(length) as usize;
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(frame_length + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.ok()?;
    frame::decode_next(&mut bytes).ok().flatten()
}

async fn wait_for_attempts(attempts: &AtomicUsize, expected: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while attempts.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("broker peer did not receive the expected request");
}

fn trace_rows(requests: &[SentRequest]) -> Vec<String> {
    requests
        .iter()
        .filter(|request| {
            matches!(
                request.request_code,
                request::SEND_MESSAGE_V2 | request::SEND_MESSAGE
            )
        })
        .filter_map(|request| request.body.as_deref())
        .flat_map(|body| {
            String::from_utf8_lossy(body)
                .split('\u{2}')
                .filter(|row| !row.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn trace_message_keys(request: &SentRequest) -> BTreeSet<String> {
    request
        .properties
        .as_deref()
        .map(decode_properties)
        .unwrap_or_default()
        .get(PROPERTY_KEYS)
        .map(|keys| {
            keys.split(KEY_SEPARATOR)
                .filter(|key| !key.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
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
        ..RemotingCommand::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply).unwrap())
        .await
        .unwrap();
}

fn route_body(brokers: &[(&str, &str)]) -> Vec<u8> {
    let queue_datas = brokers
        .iter()
        .map(|(name, _)| {
            json!({
                "brokerName": name,
                "readQueueNums": 1,
                "writeQueueNums": 1,
                "perm": 6,
                "topicSysFlag": 0
            })
        })
        .collect::<Vec<_>>();
    let broker_datas = brokers
        .iter()
        .map(|(name, address)| {
            json!({
                "cluster": "DefaultCluster",
                "brokerName": name,
                "brokerAddrs": {"0": address},
                "enableActingMaster": false
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&json!({
        "orderTopicConf": null,
        "queueDatas": queue_datas,
        "brokerDatas": broker_datas,
        "filterServerTable": {},
        "topicQueueMappingByBroker": {}
    }))
    .unwrap()
}

fn route_body_without_publish_addresses() -> Vec<u8> {
    let brokers = ["broker-a", "broker-b"];
    let queue_datas = brokers
        .iter()
        .map(|name| {
            json!({
                "brokerName": name,
                "readQueueNums": 1,
                "writeQueueNums": 1,
                "perm": 6,
                "topicSysFlag": 0
            })
        })
        .collect::<Vec<_>>();
    let broker_datas = brokers
        .iter()
        .map(|name| {
            json!({
                "cluster": "DefaultCluster",
                "brokerName": name,
                // Preserve the Java publish queue while modeling the missing
                // master address-table entry after that route was installed.
                "brokerAddrs": {"0": null},
                "enableActingMaster": false
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&json!({
        "orderTopicConf": null,
        "queueDatas": queue_datas,
        "brokerDatas": broker_datas,
        "filterServerTable": {},
        "topicQueueMappingByBroker": {}
    }))
    .unwrap()
}

async fn name_server_peer(
    route: Vec<u8>,
    missing_topics: BTreeSet<String>,
) -> (String, JoinHandle<()>, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_from_task = requests.clone();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        while let Some(command) = read_command(&mut stream).await {
            if command.code == request::GET_ROUTEINFO_BY_TOPIC {
                let topic = command
                    .ext_fields
                    .as_ref()
                    .and_then(|fields| fields.get("topic"))
                    .cloned()
                    .unwrap_or_default();
                requests_from_task.lock().unwrap().push(topic.clone());
                if missing_topics.contains(&topic) {
                    write_response(&mut stream, &command, response::TOPIC_NOT_EXIST, None, None)
                        .await;
                } else {
                    write_response(
                        &mut stream,
                        &command,
                        response::SUCCESS,
                        Some(route.clone()),
                        None,
                    )
                    .await;
                }
            } else if !command.is_oneway() {
                write_response(&mut stream, &command, response::SUCCESS, None, None).await;
            }
        }
    });
    (address, task, requests)
}

async fn broker_peer(
    broker: &'static str,
    global_attempts: Arc<AtomicUsize>,
    fail_first: usize,
    store_then_timeout_first: bool,
    requests: Arc<Mutex<Vec<SentRequest>>>,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        while let Some(command) = read_command(&mut stream).await {
            if !matches!(
                command.code,
                request::SEND_MESSAGE_V2 | request::SEND_BATCH_MESSAGE
            ) {
                if !command.is_oneway() {
                    write_response(&mut stream, &command, response::SUCCESS, None, None).await;
                }
                continue;
            }
            let properties = command
                .ext_fields
                .as_ref()
                .and_then(|fields| fields.get("i"))
                .cloned();
            let decoded_properties = properties
                .as_deref()
                .map(decode_properties)
                .unwrap_or_default();
            let unique_id = decoded_properties.get("UNIQ_KEY").cloned();
            let producer_group = command
                .ext_fields
                .as_ref()
                .and_then(|fields| fields.get("a"))
                .cloned();
            let topic = command
                .ext_fields
                .as_ref()
                .and_then(|fields| fields.get("b"))
                .cloned();
            requests.lock().unwrap().push(SentRequest {
                broker: broker.to_owned(),
                request_code: command.code,
                queue_id: command
                    .ext_fields
                    .as_ref()
                    .and_then(|fields| fields.get("e"))
                    .and_then(|value| value.parse().ok()),
                producer_group,
                topic,
                body: command.body.clone(),
                properties,
                unique_id,
                oneway: command.is_oneway(),
            });
            let attempt = global_attempts.fetch_add(1, Ordering::SeqCst);
            if command.is_oneway() {
                continue;
            }
            if store_then_timeout_first && attempt == 0 {
                // Treat receipt of the request as a Broker store, then lose
                // the acknowledgement to exercise the duplicate-delivery case.
                continue;
            }
            if attempt < fail_first {
                write_response(
                    &mut stream,
                    &command,
                    response::SERVICE_NOT_AVAILABLE,
                    None,
                    None,
                )
                .await;
            } else {
                write_response(
                    &mut stream,
                    &command,
                    response::SUCCESS,
                    None,
                    Some(
                        [
                            ("msgId".into(), "offset-id".into()),
                            ("queueId".into(), "0".into()),
                            ("queueOffset".into(), "7".into()),
                            ("MSG_REGION".into(), "DefaultRegion".into()),
                        ]
                        .into(),
                    ),
                )
                .await;
            }
        }
    });
    (address, task)
}

async fn recall_broker_peer() -> (String, JoinHandle<()>, Arc<Mutex<Vec<RemotingCommand>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_from_task = requests.clone();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        while let Some(command) = read_command(&mut stream).await {
            if command.code == request::RECALL_MESSAGE {
                requests_from_task.lock().unwrap().push(command.clone());
                write_response(
                    &mut stream,
                    &command,
                    response::SUCCESS,
                    None,
                    Some([("msgId".into(), "recalled-id".into())].into()),
                )
                .await;
            } else if !command.is_oneway() {
                write_response(&mut stream, &command, response::SUCCESS, None, None).await;
            }
        }
    });
    (address, task, requests)
}

fn producer_config(group: &str, nameserver: &str) -> ProducerConfig {
    let mut config = ProducerConfig::new(group).unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(nameserver.to_owned()));
    config
        .client_config_mut()
        .set_instance_name(Some(format!("phase7-{group}")));
    config
}

struct UnknownTransaction;

impl rocketmq_classic_producer::TransactionListener for UnknownTransaction {
    fn execute_local_transaction(
        &self,
        _message: &mut Message,
        _argument: Option<&(dyn std::any::Any + Send + Sync)>,
    ) -> Result<
        Option<rocketmq_classic_producer::LocalTransactionState>,
        rocketmq_classic_producer::TransactionCallbackError,
    > {
        Ok(Some(
            rocketmq_classic_producer::LocalTransactionState::Unknown,
        ))
    }

    fn check_local_transaction(
        &self,
        _message: &mut rocketmq_classic_common::message::MessageExt,
    ) -> Result<
        Option<rocketmq_classic_producer::LocalTransactionState>,
        rocketmq_classic_producer::TransactionCallbackError,
    > {
        Ok(Some(
            rocketmq_classic_producer::LocalTransactionState::Unknown,
        ))
    }
}

#[tokio::test]
async fn transaction_half_send_wraps_topic_and_group_once() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, route_requests) =
        name_server_peer(route, BTreeSet::new()).await;
    let mut config = producer_config("transaction-namespace", &nameserver);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    let producer = rocketmq_classic_producer::TransactionProducer::new(config);
    producer.set_transaction_listener(Some(Arc::new(UnknownTransaction)));
    producer.start().await.unwrap();

    let mut message = Message::new("orders", b"half-message".to_vec());
    producer
        .send_message_in_transaction(&mut message, None)
        .await
        .unwrap();

    assert_eq!(message.topic(), "tenant%orders");
    assert!(
        route_requests
            .lock()
            .unwrap()
            .iter()
            .any(|topic| topic == "tenant%orders")
    );
    let half_send = requests
        .lock()
        .unwrap()
        .iter()
        .find(|request| request.request_code == request::SEND_MESSAGE_V2)
        .cloned()
        .expect("transaction half message reached the Broker");
    assert_eq!(half_send.topic.as_deref(), Some("tenant%orders"));
    assert_eq!(
        half_send.producer_group.as_deref(),
        Some("tenant%transaction-namespace")
    );
    let properties = decode_properties(half_send.properties.as_deref().unwrap_or_default());
    assert_eq!(properties.get("TRAN_MSG").map(String::as_str), Some("true"));
    assert_eq!(
        properties.get("PGROUP").map(String::as_str),
        Some("tenant%transaction-namespace")
    );

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn recall_wraps_topic_and_group_once_at_the_namespace_wire_boundary() {
    let (broker, broker_task, broker_requests) = recall_broker_peer().await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, route_requests) =
        name_server_peer(route, BTreeSet::new()).await;
    let mut config = producer_config("recall-namespace", &nameserver);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    let producer = Arc::new(Producer::new(config));
    producer.start().await.unwrap();

    let handle = RecallMessageHandle::build_handle("orders", "broker-a", "123", "message-id");
    assert_eq!(
        producer.recall_message("orders", &handle).await.unwrap(),
        Some("recalled-id".to_owned())
    );

    assert!(
        route_requests
            .lock()
            .unwrap()
            .iter()
            .any(|topic| topic == "tenant%orders")
    );
    {
        let requests = broker_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let fields = requests[0].ext_fields.as_ref().unwrap();
        assert_eq!(
            fields.get("topic").map(String::as_str),
            Some("tenant%orders")
        );
        assert_eq!(
            fields.get("producerGroup").map(String::as_str),
            Some("tenant%recall-namespace")
        );
        assert_eq!(fields.get("recallHandle"), Some(&handle));
    }

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn trace_flush_waits_for_scripted_broker_delivery() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let config = producer_config("trace-flush-parent", &nameserver);
    let dispatcher = TraceDispatcher::new(
        config.client_config(),
        "trace-flush-parent",
        TraceProducerKind::Produce,
        None,
        ClientRuntimeManager::new(),
        None,
    )
    .unwrap();
    dispatcher.start().await.unwrap();
    assert!(dispatcher.append(TraceContext {
        trace_type: Some(TraceType::Pub),
        timestamp_millis: 1,
        region_id: Some("region-a".into()),
        region_name: Some(String::new()),
        group_name: Some("trace-flush-parent".into()),
        cost_time_millis: 2,
        success: true,
        request_id: None,
        context_code: 0,
        access_channel: None,
        trace_beans: Some(vec![TraceBean {
            topic: Some("orders".into()),
            message_id: Some("message-id".into()),
            body_length: 7,
            message_type: Some(MessageType::Normal),
            ..TraceBean::default()
        }]),
    }));

    dispatcher.flush().await.unwrap();

    let delivered = requests.lock().unwrap().clone();
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].broker, "broker-a");
    let body = String::from_utf8(delivered[0].body.clone().unwrap()).unwrap();
    assert!(body.contains("Pub\u{1}"));
    assert!(body.contains("message-id"));
    dispatcher.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn trace_dispatcher_uses_java_bean_selection_and_preserves_all_keys() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) = broker_peer("broker-a", attempts, 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let config = producer_config("trace-bean-selection", &nameserver);
    let dispatcher = TraceDispatcher::new(
        config.client_config(),
        "trace-bean-selection",
        TraceProducerKind::Produce,
        None,
        ClientRuntimeManager::new(),
        None,
    )
    .unwrap();
    dispatcher.start().await.unwrap();

    let make_bean = |id: &str| TraceBean {
        topic: Some("orders".into()),
        message_id: Some(id.into()),
        keys: Some(format!("{id}-key")),
        store_host: Some("127.0.0.1:10911".into()),
        message_type: Some(MessageType::Normal),
        transaction_id: Some(format!("transaction-{id}")),
        transaction_state: Some(LocalTransactionState::Commit),
        ..TraceBean::default()
    };
    let make_context = |trace_type, beans| {
        let mut context = TraceContext::new(trace_type);
        context.timestamp_millis = 1000;
        context.region_id = Some("region-a".into());
        context.group_name = Some("trace-bean-selection".into());
        context.trace_beans = Some(beans);
        context
    };

    let mut invalid_pub_tail = make_bean("pub-second");
    invalid_pub_tail.message_type = None;
    let mut invalid_transaction_tail = make_bean("transaction-second");
    invalid_transaction_tail.message_type = None;
    invalid_transaction_tail.transaction_state = None;

    let cases = [
        (
            "Pub",
            make_context(
                TraceType::Pub,
                vec![make_bean("pub-first"), make_bean("pub-second")],
            ),
            1,
            ["pub-first", "pub-second"],
        ),
        (
            "EndTransaction",
            make_context(
                TraceType::EndTransaction,
                vec![
                    make_bean("transaction-first"),
                    make_bean("transaction-second"),
                ],
            ),
            1,
            ["transaction-first", "transaction-second"],
        ),
        (
            "Recall",
            make_context(
                TraceType::Recall,
                vec![make_bean("recall-first"), make_bean("recall-second")],
            ),
            1,
            ["recall-first", "recall-second"],
        ),
        (
            "SubBefore",
            make_context(
                TraceType::SubBefore,
                vec![make_bean("sub-first"), make_bean("sub-second")],
            ),
            2,
            ["sub-first", "sub-second"],
        ),
        (
            "Pub",
            make_context(
                TraceType::Pub,
                vec![make_bean("pub-tail-first"), invalid_pub_tail],
            ),
            1,
            ["pub-tail-first", "pub-second"],
        ),
        (
            "EndTransaction",
            make_context(
                TraceType::EndTransaction,
                vec![
                    make_bean("transaction-tail-first"),
                    invalid_transaction_tail,
                ],
            ),
            1,
            ["transaction-tail-first", "transaction-second"],
        ),
    ];

    for (trace_type, context, expected_row_count, message_ids) in cases {
        requests.lock().unwrap().clear();
        assert!(dispatcher.append(context));
        dispatcher.flush().await.unwrap();

        let delivered = requests.lock().unwrap().clone();
        assert_eq!(delivered.len(), 1, "{trace_type}");
        let rows = trace_rows(&delivered);
        assert_eq!(rows.len(), expected_row_count, "{trace_type}");
        assert!(rows[0].starts_with(&format!("{trace_type}\u{1}")));
        assert!(rows[0].contains(message_ids[0]));
        if expected_row_count == 2 {
            assert!(rows[1].contains(message_ids[1]));
        } else {
            assert!(!rows[0].contains(message_ids[1]));
        }

        let expected_keys = message_ids
            .into_iter()
            .flat_map(|id| [id.to_owned(), format!("{id}-key")])
            .collect::<BTreeSet<_>>();
        assert_eq!(
            trace_message_keys(&delivered[0]),
            expected_keys,
            "{trace_type}"
        );
    }

    let mut invalid_selected_pub = make_bean("invalid-selected");
    invalid_selected_pub.message_type = None;
    requests.lock().unwrap().clear();
    assert!(dispatcher.append(make_context(TraceType::Pub, vec![invalid_selected_pub],)));
    assert!(dispatcher.append(make_context(
        TraceType::Pub,
        vec![make_bean("valid-after-invalid")],
    )));
    dispatcher.flush().await.unwrap();
    let delivered = requests.lock().unwrap().clone();
    assert_eq!(delivered.len(), 1);
    let rows = trace_rows(&delivered);
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("valid-after-invalid"));
    assert_eq!(
        trace_message_keys(&delivered[0]),
        BTreeSet::from([
            "valid-after-invalid".to_owned(),
            "valid-after-invalid-key".to_owned(),
        ])
    );

    dispatcher.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn trace_shutdown_waits_for_scripted_broker_delivery() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let mut config = producer_config("trace-shutdown-parent", &nameserver);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    config
        .client_config_mut()
        .set_namespace_v2(Some("tenant-v2".into()));
    let dispatcher = TraceDispatcher::new(
        config.client_config(),
        "tenant%trace-shutdown-parent",
        TraceProducerKind::Produce,
        Some("custom-trace"),
        ClientRuntimeManager::new(),
        None,
    )
    .unwrap();
    dispatcher.start().await.unwrap();
    assert!(dispatcher.append(TraceContext {
        trace_type: Some(TraceType::Pub),
        timestamp_millis: 1,
        region_id: Some("region-a".into()),
        region_name: Some(String::new()),
        group_name: Some("trace-shutdown-parent".into()),
        cost_time_millis: 2,
        success: true,
        request_id: None,
        context_code: 0,
        access_channel: None,
        trace_beans: Some(vec![TraceBean {
            topic: Some("orders".into()),
            message_id: Some("message-id".into()),
            body_length: 7,
            message_type: Some(MessageType::Normal),
            ..TraceBean::default()
        }]),
    }));

    dispatcher.shutdown().await.unwrap();

    let delivered = requests.lock().unwrap().clone();
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].broker, "broker-a");
    assert_eq!(delivered[0].topic.as_deref(), Some("custom-trace"));
    assert!(delivered[0].producer_group.as_deref().is_some_and(|group| {
        group.starts_with("_INNER_TRACE_PRODUCER-tenant%trace-shutdown-parent-PRODUCE-")
    }));
    let body = String::from_utf8(delivered[0].body.clone().unwrap()).unwrap();
    assert!(body.contains("Pub\u{1}"));
    assert!(body.contains("message-id"));
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn dropping_trace_dispatcher_flushes_queued_data() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let config = producer_config("trace-drop-parent", &nameserver);
    let dispatcher = TraceDispatcher::new(
        config.client_config(),
        "trace-drop-parent",
        TraceProducerKind::Produce,
        Some("custom-trace"),
        ClientRuntimeManager::new(),
        None,
    )
    .unwrap();
    dispatcher.start().await.unwrap();
    assert!(dispatcher.append(TraceContext {
        trace_type: Some(TraceType::Pub),
        timestamp_millis: 1,
        region_id: Some("region-a".into()),
        region_name: Some(String::new()),
        group_name: Some("trace-drop-parent".into()),
        cost_time_millis: 2,
        success: true,
        request_id: None,
        context_code: 0,
        access_channel: None,
        trace_beans: Some(vec![TraceBean {
            topic: Some("orders".into()),
            message_id: Some("message-id".into()),
            body_length: 7,
            message_type: Some(MessageType::Normal),
            ..TraceBean::default()
        }]),
    }));

    drop(dispatcher);
    wait_for_attempts(&attempts, 1).await;

    let delivered = requests.lock().unwrap().clone();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].broker, "broker-a");
    assert_eq!(delivered[0].topic.as_deref(), Some("custom-trace"));
    let body = String::from_utf8(delivered[0].body.clone().unwrap()).unwrap();
    assert!(body.contains("Pub\u{1}"));
    assert!(body.contains("message-id"));
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn sync_send_retries_across_two_brokers_and_preserves_one_prepared_message() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker_a, broker_a_task) =
        broker_peer("broker-a", attempts.clone(), 2, false, requests.clone()).await;
    let (broker_b, broker_b_task) =
        broker_peer("broker-b", attempts.clone(), 2, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker_a), ("broker-b", &broker_b)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let producer = Arc::new(Producer::new(producer_config("sync-retry", &nameserver)));
    producer.start().await.unwrap();

    let mut message = Message::new("orders", b"immutable-body".to_vec());
    let result = producer.send(&mut message).await.unwrap();
    assert_eq!(result.status, SendStatus::SendOk);
    assert_eq!(result.offset_msg_id.as_deref(), Some("offset-id"));
    assert_eq!(result.queue_offset, 7);
    assert!(matches!(result.msg_id.len(), 32 | 56));
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(message.body().as_ref(), b"immutable-body");
    let unique_id = message
        .get_property("UNIQ_KEY")
        .expect("Java send should put the generated ID on the caller message");

    let sent = requests.lock().unwrap().clone();
    assert_eq!(sent.len(), 3);
    assert_eq!(
        sent.iter()
            .map(|request| request.unique_id.as_deref())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([Some(result.msg_id.as_str())])
    );
    assert!(
        sent.iter()
            .all(|request| { request.unique_id.as_deref() == Some(unique_id.as_str()) })
    );
    assert!(
        sent.iter()
            .all(|request| request.body.as_deref() == Some(&b"immutable-body"[..]))
    );
    assert!(
        sent.iter()
            .all(|request| request.request_code == request::SEND_MESSAGE_V2)
    );
    assert!(sent.iter().any(|request| request.broker == "broker-a"));
    assert!(sent.iter().any(|request| request.broker == "broker-b"));

    assert!(matches!(
        producer.start().await,
        Err(ProducerError::AlreadyStarted)
    ));
    producer.shutdown().await.unwrap();
    producer.shutdown().await.unwrap();
    assert!(matches!(
        producer.send(&mut message).await,
        Err(ProducerError::Shutdown)
    ));
    nameserver_task.abort();
    broker_a_task.abort();
    broker_b_task.abort();
}

#[tokio::test]
async fn producer_handles_two_thousand_concurrent_async_sends_without_losing_requests() {
    const CONCURRENT_SENDS: usize = 2_000;

    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::with_capacity(CONCURRENT_SENDS)));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let producer = Arc::new(Producer::new(producer_config(
        "concurrent-send-stress",
        &nameserver,
    )));
    producer.start().await.unwrap();

    let mut sends = tokio::task::JoinSet::new();
    for index in 0..CONCURRENT_SENDS {
        let producer = producer.clone();
        sends.spawn(async move {
            let body = format!("concurrent-send-{index}").into_bytes();
            let mut message = Message::new("concurrent-send", body);
            producer
                .send_future_with_timeout(&mut message, std::time::Duration::from_secs(30))
                .await
                .expect("concurrent send returns a successful Java-style result");
        });
    }

    let completed = tokio::time::timeout(std::time::Duration::from_secs(45), async {
        let mut completed = 0;
        while let Some(result) = sends.join_next().await {
            result.expect("concurrent send task does not panic");
            completed += 1;
        }
        completed
    })
    .await
    .expect("all concurrent sends finish before the stress deadline");
    assert_eq!(completed, CONCURRENT_SENDS);
    assert_eq!(attempts.load(Ordering::SeqCst), CONCURRENT_SENDS);

    let sent = requests.lock().unwrap().clone();
    assert_eq!(sent.len(), CONCURRENT_SENDS);
    assert!(sent.iter().all(|request| {
        request.request_code == request::SEND_MESSAGE_V2
            && request.topic.as_deref() == Some("concurrent-send")
    }));
    let unique_ids = sent
        .iter()
        .filter_map(|request| request.unique_id.as_deref())
        .collect::<BTreeSet<_>>();
    assert_eq!(unique_ids.len(), CONCURRENT_SENDS);
    let bodies = sent
        .iter()
        .filter_map(|request| request.body.clone())
        .collect::<BTreeSet<_>>();
    let expected_bodies = (0..CONCURRENT_SENDS)
        .map(|index| format!("concurrent-send-{index}").into_bytes())
        .collect::<BTreeSet<_>>();
    assert_eq!(bodies, expected_bodies);

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn direct_send_preserves_existing_unique_id_on_caller_and_wire() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let producer = Arc::new(Producer::new(producer_config(
        "preserve-existing-unique-id",
        &nameserver,
    )));
    producer.start().await.unwrap();

    let mut message = Message::new("orders", b"caller-id-body".to_vec());
    message
        .set_system_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "caller-provided-id")
        .unwrap();
    producer.send_direct(&mut message).await.unwrap();

    assert_eq!(
        message
            .properties()
            .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .map(String::as_str),
        Some("caller-provided-id")
    );
    let sent = requests.lock().unwrap().clone();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].unique_id.as_deref(), Some("caller-provided-id"));
    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn default_topic_route_fallback_and_async_mode_response_policy_match_java() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 1, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, route_requests) =
        name_server_peer(route, BTreeSet::from(["orders".to_owned()])).await;
    let mut config = producer_config("future-policy", &nameserver);
    config.set_retry_times_when_send_async_failed(0);
    let producer = Arc::new(Producer::new(config));
    producer.start().await.unwrap();

    let mut message = Message::new("orders", b"future-body".to_vec());
    let result = producer.send_future(&mut message).await;
    assert!(matches!(result, Err(ProducerError::Rpc(_))));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(
        route_requests
            .lock()
            .unwrap()
            .contains(&"TBW102".to_owned())
    );
    assert!(requests.lock().unwrap().iter().all(|request| {
        request
            .unique_id
            .as_deref()
            .is_some_and(|id| matches!(id.len(), 32 | 56))
    }));

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn producer_start_initializes_configured_topic_routes_and_default_fallback() {
    let route = serde_json::to_vec(&json!({
        "orderTopicConf": null,
        "queueDatas": [{
            "brokerName": "startup-broker",
            "readQueueNums": 8,
            "writeQueueNums": 8,
            "perm": 6,
            "topicSysFlag": 0
        }],
        "brokerDatas": [{
            "cluster": "DefaultCluster",
            "brokerName": "startup-broker",
            "brokerAddrs": {"0": null},
            "enableActingMaster": false
        }],
        "filterServerTable": {},
        "topicQueueMappingByBroker": {}
    }))
    .unwrap();
    let missing_topics = BTreeSet::from(["startup-topic".to_owned()]);
    let (nameserver, nameserver_task, route_requests) =
        name_server_peer(route, missing_topics).await;
    let mut config = producer_config("startup-route-init", &nameserver);
    config.set_default_topic_queue_nums(2);
    config.set_topics(Some(vec!["startup-topic".into()].into()));
    let producer = Arc::new(Producer::new(config));

    producer.start().await.unwrap();

    let requests = route_requests.lock().unwrap().clone();
    assert_eq!(requests.first().map(String::as_str), Some("startup-topic"));
    assert!(requests.contains(&"TBW102".to_owned()));
    assert!(producer.publish_topics().contains("startup-topic"));
    assert_eq!(
        producer
            .get_topic_publish_info_table()
            .get("startup-topic")
            .unwrap()
            .message_queue_list()
            .unwrap()
            .len(),
        2
    );

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
}

#[tokio::test]
async fn topic_without_route_stays_in_producer_refresh_inventory() {
    let route = route_body(&[]);
    let missing_topics = BTreeSet::from(["orders".to_owned(), "TBW102".to_owned()]);
    let (nameserver, nameserver_task, route_requests) =
        name_server_peer(route, missing_topics).await;
    let producer = Arc::new(Producer::new(producer_config("missing-route", &nameserver)));
    producer.start().await.unwrap();

    assert!(matches!(
        producer
            .send(&mut Message::new("orders", b"no-route".to_vec()))
            .await,
        Err(ProducerError::NoRoute(_))
    ));
    assert!(
        route_requests
            .lock()
            .unwrap()
            .contains(&"orders".to_owned())
    );
    assert!(
        route_requests
            .lock()
            .unwrap()
            .contains(&"TBW102".to_owned())
    );
    assert!(producer.publish_topics().contains("orders"));

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
}

#[tokio::test]
async fn default_send_missing_broker_addresses_matches_java_retry_error() {
    let route = route_body_without_publish_addresses();
    let (nameserver, nameserver_task, route_requests) =
        name_server_peer(route, BTreeSet::new()).await;
    let mut config = producer_config("missing-broker-address-retry", &nameserver);
    config.set_retry_times_when_send_failed(1);
    let producer = Arc::new(Producer::new(config));
    producer.start().await.unwrap();

    let error = producer
        .send(&mut Message::new(
            "orders",
            b"missing broker address".to_vec(),
        ))
        .await
        .unwrap_err();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["missingBrokerAddressRetry"];
    assert_eq!(expected["outcome"], "MQClientException");
    assert_eq!(expected["responseCode"], 10003);
    assert_eq!(expected["messageContainsAttemptCount"], true);
    assert_eq!(expected["messageContainsBothBrokers"], true);
    assert_eq!(expected["causeClass"], "MQClientException");
    assert_eq!(expected["causeMessageContainsBrokerFailure"], true);
    match &error {
        ProducerError::SendRetriesExhausted {
            message,
            response_code,
            source,
        } => {
            assert_eq!(
                *response_code,
                expected["responseCode"].as_i64().unwrap() as i32
            );
            assert!(message.contains("Send [2] times"));
            assert!(message.contains("broker-a") && message.contains("broker-b"));
            assert!(source.to_string().contains("The broker["));
            assert!(source.to_string().contains(" not exist"));
        }
        other => panic!("expected Java retry-exhausted error, got {other:?}"),
    }
    assert_eq!(route_requests.lock().unwrap().as_slice(), ["orders"]);

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
}

#[tokio::test]
async fn explicit_queue_missing_broker_address_matches_java_error() {
    let route = route_body_without_publish_addresses();
    let (nameserver, nameserver_task, route_requests) =
        name_server_peer(route, BTreeSet::new()).await;
    let producer = Arc::new(Producer::new(producer_config(
        "missing-broker-address-explicit",
        &nameserver,
    )));
    producer.start().await.unwrap();

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let error = producer
        .send_to_queue(
            &mut Message::new("orders", b"missing broker address".to_vec()),
            &queue,
        )
        .await
        .unwrap_err();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["missingBrokerAddressExplicit"];
    assert_eq!(expected["outcome"], "MQClientException");
    assert_eq!(expected["responseCode"], -1);
    assert_eq!(expected["messageContainsMissingBroker"], true);
    assert_eq!(expected["causeClass"], "none");
    assert!(error.to_string().contains("The broker[broker-a] not exist"));
    assert_eq!(route_requests.lock().unwrap().as_slice(), ["orders"]);

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
}

#[tokio::test]
async fn selector_send_missing_broker_address_matches_java_error() {
    let route = route_body_without_publish_addresses();
    let (nameserver, nameserver_task, route_requests) =
        name_server_peer(route, BTreeSet::new()).await;
    let producer = Arc::new(Producer::new(producer_config(
        "missing-broker-address-selector",
        &nameserver,
    )));
    producer.start().await.unwrap();

    let error = producer
        .send_with_selector(
            &mut Message::new("orders", b"missing broker address".to_vec()),
            |queues, _| queues.first().cloned(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ProducerError::BrokerAddressNotFound { broker } if broker == "broker-a"
    ));
    assert_eq!(route_requests.lock().unwrap().as_slice(), ["orders"]);

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
}

#[tokio::test]
async fn oneway_sends_once_and_unknown_explicit_broker_fails_before_io() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let producer = Arc::new(Producer::new(producer_config("oneway", &nameserver)));
    producer.start().await.unwrap();

    let mut message = Message::new("orders", b"oneway-body".to_vec());
    let invalid_queue = MessageQueue::new("orders", "not-in-route", 0);
    assert!(matches!(
        producer.send_to_queue(&mut message, &invalid_queue).await,
        Err(ProducerError::BrokerAddressNotFound { broker }) if broker == "not-in-route"
    ));
    assert!(requests.lock().unwrap().is_empty());

    producer.send_oneway(&mut message).await.unwrap();
    wait_for_attempts(&attempts, 1).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(requests.lock().unwrap().len(), 1);
    assert!(requests.lock().unwrap()[0].oneway);

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn public_queue_selector_and_batch_modes_use_the_scripted_route() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let producer = Arc::new(Producer::new(producer_config(
        "public-send-modes",
        &nameserver,
    )));
    producer.start().await.unwrap();

    let queues = producer
        .fetch_publish_message_queues("orders")
        .await
        .unwrap();
    assert_eq!(queues.len(), 1);
    let queue = queues[0].clone();
    let mut message = Message::new("orders", b"single-mode".to_vec());

    producer.send_to_queue(&mut message, &queue).await.unwrap();
    producer
        .send_to_queue_future(&mut message, &queue)
        .await
        .unwrap();
    producer
        .send_to_queue_future_with_timeout(&mut message, &queue, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    producer
        .send_to_queue_oneway(&mut message, &queue)
        .await
        .unwrap();
    producer
        .send_oneway_with_timeout(&mut message, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    producer
        .send_with_selector(&mut message, |queues, _| queues.first().cloned())
        .await
        .unwrap();
    producer
        .send_future_with_selector(&mut message, |queues, _| queues.first().cloned())
        .await
        .unwrap();
    producer
        .send_future_with_selector_with_timeout(
            &mut message,
            |queues, _| queues.first().cloned(),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
    producer
        .send_oneway_with_selector(&mut message, |queues, _| queues.first().cloned())
        .await
        .unwrap();

    let mut batch = [
        Message::new("orders", b"batch-one".to_vec()),
        Message::new("orders", b"batch-two".to_vec()),
    ];
    producer
        .send_batch_with_timeout(&mut batch, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    producer.send_batch_future(&mut batch).await.unwrap();
    producer
        .send_batch_future_with_timeout(&mut batch, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    producer
        .send_batch_to_queue(&mut batch, &queue)
        .await
        .unwrap();
    producer
        .send_batch_to_queue_with_timeout(&mut batch, &queue, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    producer
        .send_batch_to_queue_future(&mut batch, &queue)
        .await
        .unwrap();
    producer
        .send_batch_to_queue_future_with_timeout(
            &mut batch,
            &queue,
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();

    let sent = requests.lock().unwrap().clone();
    assert_eq!(sent.len(), 16);
    assert_eq!(
        sent.iter()
            .filter(|request| request.request_code == request::SEND_BATCH_MESSAGE)
            .count(),
        7
    );
    assert_eq!(sent.iter().filter(|request| request.oneway).count(), 3);
    assert!(sent.iter().all(|request| request.body.is_some()));

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn explicit_and_selected_queue_ids_outside_the_route_are_sent_to_the_routed_broker() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let producer = Arc::new(Producer::new(producer_config(
        "caller-selected-queue",
        &nameserver,
    )));
    producer.start().await.unwrap();

    let mut message = Message::new("orders", b"selected-outside-route".to_vec());
    let explicit = MessageQueue::new("orders", "broker-a", 7);
    producer
        .send_to_queue(&mut message, &explicit)
        .await
        .unwrap();
    producer
        .send_to_queue_future_with_timeout(
            &mut message,
            &MessageQueue::new("orders", "broker-a", 8),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
    producer
        .send_to_queue_oneway(&mut message, &MessageQueue::new("orders", "broker-a", 9))
        .await
        .unwrap();
    producer
        .send_with_selector(&mut message, |_, _| {
            Some(MessageQueue::new("orders", "broker-a", 10))
        })
        .await
        .unwrap();
    producer
        .send_future_with_selector_with_timeout(
            &mut message,
            |_, _| Some(MessageQueue::new("orders", "broker-a", 11)),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
    producer
        .send_oneway_with_selector(&mut message, |_, _| {
            Some(MessageQueue::new("orders", "broker-a", 12))
        })
        .await
        .unwrap();
    wait_for_attempts(&attempts, 6).await;

    let sent = requests.lock().unwrap().clone();
    assert_eq!(sent.len(), 6);
    assert!(sent.iter().all(|request| request.broker == "broker-a"));
    assert_eq!(
        sent.iter()
            .map(|request| request.queue_id.unwrap())
            .collect::<BTreeSet<_>>(),
        (7..=12).collect()
    );

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn invoke_message_queue_selector_returns_the_namespaced_queue_without_sending() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let mut config = producer_config("selector-api", &nameserver);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    let producer = Arc::new(Producer::new(config));
    producer.start().await.unwrap();

    let java_fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let java_snapshot = &java_fixture["implementationSelectorMessageSnapshot"];
    let mut message = Message::new("tenant%orders", b"select-only".to_vec());
    message.set_flag(java_snapshot["selectorFlag"].as_i64().unwrap() as i32);
    message.set_transaction_id(Some(
        java_snapshot["sourceTransactionId"]
            .as_str()
            .unwrap()
            .to_owned(),
    ));
    message
        .put_user_property(
            "fixture-key",
            java_snapshot["selectorProperty"].as_str().unwrap(),
        )
        .unwrap();
    let selected = producer
        .invoke_message_queue_selector(
            &message,
            |queues, selected_message| {
                assert_eq!(
                    selected_message.topic(),
                    java_snapshot["selectorTopic"].as_str().unwrap()
                );
                assert_eq!(selected_message.body(), message.body());
                assert_eq!(selected_message.flag(), java_snapshot["selectorFlag"]);
                assert_eq!(
                    selected_message.get_property("fixture-key").as_deref(),
                    java_snapshot["selectorProperty"].as_str()
                );
                assert_eq!(selected_message.transaction_id(), None);
                assert_eq!(
                    message.topic(),
                    java_snapshot["sourceTopic"].as_str().unwrap()
                );
                assert_eq!(
                    message.transaction_id(),
                    java_snapshot["sourceTransactionId"].as_str()
                );
                assert_eq!(queues.len(), 1);
                assert_eq!(
                    queues[0].topic(),
                    Some(java_snapshot["selectorQueueTopic"].as_str().unwrap())
                );
                queues.first().cloned()
            },
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();

    assert_eq!(message.topic(), "tenant%orders");
    assert_eq!(selected.topic(), Some("tenant%orders"));
    assert_eq!(selected.broker_name(), Some("broker-a"));
    assert_eq!(selected.queue_id(), 0);

    assert_eq!(java_snapshot["selectorMessageIsDistinct"], true);
    assert_eq!(java_snapshot["selectorBodyShared"], true);
    assert_eq!(java_snapshot["selectorPropertiesShared"], true);
    assert_eq!(java_snapshot["selectorTransactionIdIsNull"], true);

    let no_queue = producer
        .invoke_message_queue_selector(&message, |_, _| None, std::time::Duration::from_secs(1))
        .await;
    assert!(matches!(no_queue, Err(ProducerError::NoQueueSelected)));

    let java_invoke = &java_fixture["implementationSelectorFailure"]["invoke"];
    assert_eq!(java_invoke["errorClass"], "MQClientException");
    assert_eq!(java_invoke["causeClass"], "IllegalStateException");
    let selector_failure = producer
        .invoke_message_queue_selector(
            &message,
            |_, _| panic!("fixture-selector-failure"),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert_eq!(
        selector_failure.to_string(),
        java_invoke["errorMessage"].as_str().unwrap()
    );
    assert!(matches!(
        selector_failure,
        ProducerError::SelectorPanicked(ref message)
            if message == java_invoke["causeMessage"].as_str().unwrap()
    ));

    let java_send = &java_fixture["implementationSelectorFailure"]["send"];
    assert_eq!(java_send["errorMessage"], java_invoke["errorMessage"]);
    let selector_send_failure = producer
        .send_with_selector(&mut message, |_, selected_message| {
            assert_eq!(
                selected_message.topic(),
                java_snapshot["selectorTopic"].as_str().unwrap()
            );
            assert_eq!(selected_message.transaction_id(), None);
            panic!("fixture-selector-failure")
        })
        .await
        .unwrap_err();
    assert_eq!(
        selector_send_failure.to_string(),
        java_send["errorMessage"].as_str().unwrap()
    );
    assert!(matches!(
        selector_send_failure,
        ProducerError::SelectorPanicked(ref message)
            if message == java_send["causeMessage"].as_str().unwrap()
    ));

    let timed_out = producer
        .invoke_message_queue_selector(
            &message,
            |queues, _| {
                std::thread::sleep(std::time::Duration::from_millis(5));
                queues.first().cloned()
            },
            std::time::Duration::from_millis(1),
        )
        .await;
    assert!(matches!(timed_out, Err(ProducerError::Timeout)));

    assert!(requests.lock().unwrap().is_empty());
    assert_eq!(attempts.load(Ordering::SeqCst), 0);

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn stored_message_with_lost_ack_can_be_retried_and_duplicated() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker_a, broker_a_task) =
        broker_peer("broker-a", attempts.clone(), 0, true, requests.clone()).await;
    let (broker_b, broker_b_task) =
        broker_peer("broker-b", attempts.clone(), 0, true, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker_a), ("broker-b", &broker_b)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let mut config = producer_config("stored-timeout", &nameserver);
    config.set_send_timeout_millis(500);
    config.set_send_msg_max_timeout_per_request(20);
    let producer = Arc::new(Producer::new(config));
    producer.start().await.unwrap();

    let mut message = Message::new("orders", b"store-before-timeout".to_vec());
    let result = producer.send(&mut message).await.unwrap();
    assert_eq!(result.status, SendStatus::SendOk);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let sent = requests.lock().unwrap().clone();
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().all(|request| {
        request.body.as_deref() == Some(&b"store-before-timeout"[..])
            && request.unique_id == sent[0].unique_id
    }));

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_a_task.abort();
    broker_b_task.abort();
}
