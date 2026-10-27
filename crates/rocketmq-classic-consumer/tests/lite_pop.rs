use std::{
    collections::{BTreeSet, HashMap},
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use base64::Engine;
use bytes::BytesMut;
use rocketmq_classic_common::message::{
    MessageExt, MessageHostAddress, MessageQueue, encode_message_ext,
};
use rocketmq_classic_consumer::push::PushConsumerConfig;
use rocketmq_classic_consumer::{
    LitePullConsumer, LitePullConsumerConfig, LitePullConsumerError,
    offset::{OffsetRead, OffsetStore, OffsetStoreFuture, ReadOffsetType},
};
use rocketmq_classic_core::runtime::{ClientRuntimeManager, RuntimeState};
use rocketmq_classic_protocol::codes::{request, response};
use rocketmq_classic_protocol::header::HeaderMap;
use rocketmq_classic_protocol::heartbeat::{ConsumeFromWhere, MessageModel};
use rocketmq_classic_remoting::{RemotingCommand, client::RpcHook, codec::frame};
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, oneshot},
    time::timeout,
};

#[derive(Default)]
struct LiteScriptedOffsetStore {
    memory: Mutex<HashMap<MessageQueue, i64>>,
    persisted: Mutex<HashMap<MessageQueue, i64>>,
}

impl OffsetStore for LiteScriptedOffsetStore {
    fn load(&self) -> OffsetStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn update_offset(&self, queue: &MessageQueue, offset: i64, _increase_only: bool) {
        self.memory.lock().unwrap().insert(queue.clone(), offset);
    }

    fn update_and_freeze_offset(&self, queue: &MessageQueue, offset: i64) {
        self.update_offset(queue, offset, false);
    }

    fn read_offset<'a>(
        &'a self,
        queue: &'a MessageQueue,
        mode: ReadOffsetType,
    ) -> OffsetStoreFuture<'a, OffsetRead> {
        Box::pin(async move {
            let memory = self.memory.lock().unwrap().get(queue).copied();
            let persisted = self.persisted.lock().unwrap().get(queue).copied();
            Ok(match mode {
                ReadOffsetType::ReadFromMemory => memory,
                ReadOffsetType::ReadFromStore => persisted,
                ReadOffsetType::MemoryFirstThenStore => memory.or(persisted),
            }
            .map_or(OffsetRead::NotFound, OffsetRead::Found))
        })
    }

    fn persist<'a>(&'a self, queue: &'a MessageQueue) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async move {
            if let Some(offset) = self.memory.lock().unwrap().get(queue).copied() {
                self.persisted.lock().unwrap().insert(queue.clone(), offset);
            }
            Ok(())
        })
    }

    fn persist_all<'a>(&'a self, queues: &'a [MessageQueue]) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async move {
            let memory = self.memory.lock().unwrap();
            let mut persisted = self.persisted.lock().unwrap();
            for queue in queues {
                if let Some(offset) = memory.get(queue) {
                    persisted.insert(queue.clone(), *offset);
                }
            }
            Ok(())
        })
    }

    fn remove_offset(&self, queue: &MessageQueue) {
        self.memory.lock().unwrap().remove(queue);
    }

    fn clone_offset_table(&self, topic: Option<&str>) -> HashMap<MessageQueue, i64> {
        self.memory
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

struct ScriptedBrokerRequest {
    command: RemotingCommand,
    reply: oneshot::Sender<ScriptedReply>,
}

#[derive(Debug)]
struct ScriptedReply {
    code: i32,
    fields: Option<HeaderMap>,
    body: Option<Vec<u8>>,
}

async fn read_command<R: AsyncRead + Unpin>(stream: &mut R) -> Option<RemotingCommand> {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.ok()?;
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.ok()?;
    frame::decode_next(&mut bytes).ok()?
}

async fn write_reply<W: AsyncWrite + Unpin>(stream: &mut W, opaque: i32, reply: ScriptedReply) {
    let mut response_command = RemotingCommand {
        code: reply.code,
        opaque,
        ext_fields: reply.fields.map(Into::into),
        body: reply.body,
        ..Default::default()
    };
    response_command.mark_response();
    stream
        .write_all(&frame::encode(&response_command).unwrap())
        .await
        .unwrap();
}

fn broker_route(address: &str) -> Vec<u8> {
    broker_route_with(address, "broker-a", 1)
}

fn broker_route_with(address: &str, broker_name: &str, read_queue_nums: i32) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "queueDatas": [{
            "brokerName": broker_name,
            "readQueueNums": read_queue_nums,
            "writeQueueNums": read_queue_nums,
            "perm": 6,
            "topicSysFlag": 0
        }],
        "brokerDatas": [{
            "cluster": "cluster",
            "brokerName": broker_name,
            "brokerAddrs": {"0": address}
        }],
        "filterServerTable": {}
    }))
    .unwrap()
}

async fn start_scripted_nameserver(route: Vec<u8>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let route = route.clone();
            tokio::spawn(async move {
                while let Some(command) = read_command(&mut stream).await {
                    let body =
                        (command.code == request::GET_ROUTEINFO_BY_TOPIC).then(|| route.clone());
                    write_reply(
                        &mut stream,
                        command.opaque,
                        ScriptedReply {
                            code: response::SUCCESS,
                            fields: None,
                            body,
                        },
                    )
                    .await;
                }
            });
        }
    });
    (address, task)
}

async fn start_observed_scripted_nameserver(
    route: Vec<u8>,
) -> (
    String,
    mpsc::UnboundedReceiver<RemotingCommand>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (route_tx, route_rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let route = route.clone();
            let route_tx = route_tx.clone();
            tokio::spawn(async move {
                while let Some(command) = read_command(&mut stream).await {
                    let body = if command.code == request::GET_ROUTEINFO_BY_TOPIC {
                        let _ = route_tx.send(command.clone());
                        Some(route.clone())
                    } else {
                        None
                    };
                    write_reply(
                        &mut stream,
                        command.opaque,
                        ScriptedReply {
                            code: response::SUCCESS,
                            fields: None,
                            body,
                        },
                    )
                    .await;
                }
            });
        }
    });
    (address, route_rx, task)
}

fn start_scripted_broker(
    listener: TcpListener,
) -> (
    mpsc::UnboundedReceiver<ScriptedBrokerRequest>,
    tokio::task::JoinHandle<()>,
) {
    let (requests, task, _, _) =
        start_scripted_broker_with_check_response(listener, response::SUCCESS);
    (requests, task)
}

fn start_scripted_broker_with_check_response(
    listener: TcpListener,
    check_client_response: i32,
) -> (
    mpsc::UnboundedReceiver<ScriptedBrokerRequest>,
    tokio::task::JoinHandle<()>,
    mpsc::UnboundedReceiver<RemotingCommand>,
    mpsc::UnboundedReceiver<RemotingCommand>,
) {
    let (request_tx, request_rx) = mpsc::unbounded_channel::<ScriptedBrokerRequest>();
    let (check_tx, check_rx) = mpsc::unbounded_channel::<RemotingCommand>();
    let (heartbeat_tx, heartbeat_rx) = mpsc::unbounded_channel::<RemotingCommand>();
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let request_tx = request_tx.clone();
            let check_tx = check_tx.clone();
            let heartbeat_tx = heartbeat_tx.clone();
            tokio::spawn(async move {
                let (mut reader, writer) = stream.into_split();
                let writer = Arc::new(tokio::sync::Mutex::new(writer));
                while let Some(command) = read_command(&mut reader).await {
                    if matches!(
                        command.code,
                        request::HEART_BEAT | request::UNREGISTER_CLIENT
                    ) || command.code == request::CHECK_CLIENT_CONFIG
                    {
                        if command.code == request::CHECK_CLIENT_CONFIG {
                            let _ = check_tx.send(command.clone());
                        } else if command.code == request::HEART_BEAT {
                            let _ = heartbeat_tx.send(command.clone());
                        }
                        let writer = writer.clone();
                        let opaque = command.opaque;
                        let code = if command.code == request::CHECK_CLIENT_CONFIG {
                            check_client_response
                        } else {
                            response::SUCCESS
                        };
                        tokio::spawn(async move {
                            write_reply(
                                &mut *writer.lock().await,
                                opaque,
                                ScriptedReply {
                                    code,
                                    fields: None,
                                    body: None,
                                },
                            )
                            .await;
                        });
                        continue;
                    }
                    let (reply_tx, reply_rx) = oneshot::channel();
                    if request_tx
                        .send(ScriptedBrokerRequest {
                            command: command.clone(),
                            reply: reply_tx,
                        })
                        .is_err()
                    {
                        return;
                    }
                    let writer = writer.clone();
                    let opaque = command.opaque;
                    tokio::spawn(async move {
                        if let Ok(reply) = reply_rx.await {
                            write_reply(&mut *writer.lock().await, opaque, reply).await;
                        }
                    });
                }
            });
        }
    });
    (request_rx, task, check_rx, heartbeat_rx)
}

fn start_paused_check_broker(
    listener: TcpListener,
) -> (
    mpsc::UnboundedReceiver<RemotingCommand>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let (check_tx, check_rx) = mpsc::unbounded_channel();
    let (release_tx, release_rx) = oneshot::channel();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let check_tx = check_tx.clone();
            let release_rx = release_rx.clone();
            tokio::spawn(async move {
                while let Some(command) = read_command(&mut stream).await {
                    if command.code == request::CHECK_CLIENT_CONFIG {
                        let _ = check_tx.send(command.clone());
                        let wait = release_rx
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take();
                        if let Some(wait) = wait {
                            let _ = wait.await;
                        }
                    }
                    write_reply(
                        &mut stream,
                        command.opaque,
                        ScriptedReply {
                            code: response::SUCCESS,
                            fields: None,
                            body: None,
                        },
                    )
                    .await;
                }
            });
        }
    });
    (check_rx, release_tx, task)
}

async fn next_request(
    receiver: &mut mpsc::UnboundedReceiver<ScriptedBrokerRequest>,
    code: i32,
) -> ScriptedBrokerRequest {
    let request = timeout(Duration::from_secs(4), receiver.recv())
        .await
        .expect("scripted broker timed out waiting for request")
        .expect("scripted broker request stream closed");
    assert_eq!(request.command.code, code);
    request
}

async fn next_request_servicing_group(
    receiver: &mut mpsc::UnboundedReceiver<ScriptedBrokerRequest>,
    consumer_id: &str,
    expected_code: i32,
) -> ScriptedBrokerRequest {
    loop {
        let request = timeout(Duration::from_secs(4), receiver.recv())
            .await
            .expect("scripted broker timed out waiting for Lite pull request")
            .expect("scripted broker request stream closed");
        if request.command.code == request::GET_CONSUMER_LIST_BY_GROUP {
            request
                .reply
                .send(ScriptedReply {
                    code: response::SUCCESS,
                    fields: None,
                    body: Some(
                        serde_json::json!({"consumerIdList": [consumer_id]})
                            .to_string()
                            .into_bytes(),
                    ),
                })
                .unwrap();
            continue;
        }
        assert_eq!(request.command.code, expected_code);
        return request;
    }
}

async fn next_lite_pull_request(
    receiver: &mut mpsc::UnboundedReceiver<ScriptedBrokerRequest>,
    consumer_id: &str,
) -> ScriptedBrokerRequest {
    next_request_servicing_group(receiver, consumer_id, request::LITE_PULL_MESSAGE).await
}

fn reply_offset(request: ScriptedBrokerRequest, code: i32, name: &str, offset: i64) {
    request
        .reply
        .send(ScriptedReply {
            code,
            fields: Some(HeaderMap::from([(name.into(), offset.to_string())])),
            body: None,
        })
        .unwrap();
}

fn pull_reply(request: ScriptedBrokerRequest, code: i32, offset: i64, message: bool) {
    let body = message.then(|| {
        let fixture: Value =
            serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
        let mut bytes = base64::engine::general_purpose::STANDARD
            .decode(
                fixture["cases"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|case| case["name"] == "ipv4-v1")
                    .unwrap()["recordBase64"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap();
        bytes[20..28].copy_from_slice(&(offset - 1).to_be_bytes());
        bytes
    });
    pull_reply_with_body(request, code, offset, body);
}

fn pull_reply_with_body(
    request: ScriptedBrokerRequest,
    code: i32,
    offset: i64,
    body: Option<Vec<u8>>,
) {
    let fields = HeaderMap::from([
        ("suggestWhichBrokerId".into(), "0".into()),
        ("nextBeginOffset".into(), offset.to_string()),
        ("minOffset".into(), "0".into()),
        ("maxOffset".into(), "30".into()),
    ]);
    request
        .reply
        .send(ScriptedReply {
            code,
            fields: Some(fields),
            body,
        })
        .unwrap();
}

fn concatenated_lz4_record() -> Vec<u8> {
    let mut stream = rocketmq_classic_common::compression::compress(
        rocketmq_classic_common::sysflag::message::CompressionType::Lz4,
        b"first-",
        5,
    )
    .unwrap();
    stream.extend_from_slice(
        &rocketmq_classic_common::compression::compress(
            rocketmq_classic_common::sysflag::message::CompressionType::Lz4,
            b"second",
            5,
        )
        .unwrap(),
    );
    let mut message = MessageExt::new();
    message.set_topic("orders");
    message.set_body(stream);
    message.set_sys_flag(
        rocketmq_classic_common::sysflag::message::COMPRESSED_FLAG
            | rocketmq_classic_common::sysflag::message::COMPRESSION_LZ4_TYPE,
    );
    message.set_born_host(Some(MessageHostAddress::from_ip_address(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        10_911,
    )));
    message.set_store_host(Some(MessageHostAddress::from_ip_address(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        10_912,
    )));
    encode_message_ext(&message, false).expect("precompressed LZ4 record encodes")
}

fn lite_config_with_store(
    group: &str,
    instance: &str,
    nameserver: &str,
    store: Arc<LiteScriptedOffsetStore>,
) -> LitePullConsumerConfig {
    let mut config = LitePullConsumerConfig::new(group).unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(nameserver.to_owned()));
    config
        .client_config_mut()
        .set_instance_name(Some(instance.to_owned()));
    config
        .client_config_mut()
        .set_poll_name_server_interval(60_000);
    config
        .client_config_mut()
        .set_heartbeat_broker_interval(60_000);
    config
        .client_config_mut()
        .set_persist_consumer_offset_interval(60_000);
    config.set_auto_commit(false);
    config
        .set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .unwrap();
    config.set_offset_store(Some(store));
    config
}

fn java_fixture() -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/consumer/lite-pop.json");
    serde_json::from_slice(&std::fs::read(path).expect("pinned Java Lite/POP fixture"))
        .expect("valid pinned Java Lite/POP fixture")
}

#[tokio::test]
async fn scripted_lite_seek_pause_resume_commit_and_restart_preserve_delivery_offsets() {
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let route = broker_route(&broker_address);
    let (nameserver, nameserver_task) = start_scripted_nameserver(route).await;
    let (mut requests, broker_task) = start_scripted_broker(broker_listener);
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let store = Arc::new(LiteScriptedOffsetStore::default());

    let first = Arc::new(
        LitePullConsumer::new(lite_config_with_store(
            "lite-scripted-flow",
            "lite-scripted-client-1",
            &nameserver,
            store.clone(),
        ))
        .unwrap(),
    );
    first.assign([queue.clone()]).await.unwrap();
    first.start().await.unwrap();

    let original_pull = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    assert_eq!(
        original_pull.command.ext_fields.as_ref().unwrap()["queueOffset"],
        "0"
    );

    let seek_consumer = first.clone();
    let seek_queue = queue.clone();
    let invalid_seek = tokio::spawn(async move { seek_consumer.seek(&seek_queue, 31).await });
    let minimum = next_request(&mut requests, request::GET_MIN_OFFSET).await;
    reply_offset(minimum, response::SUCCESS, "offset", 0);
    let maximum = next_request(&mut requests, request::GET_MAX_OFFSET).await;
    reply_offset(maximum, response::SUCCESS, "offset", 30);
    assert!(matches!(
        invalid_seek.await.unwrap(),
        Err(LitePullConsumerError::InvalidSeekOffset {
            offset: 31,
            minimum: 0,
            maximum: 30
        })
    ));

    let seek_consumer = first.clone();
    let seek_queue = queue.clone();
    let valid_seek = tokio::spawn(async move { seek_consumer.seek(&seek_queue, 10).await });
    let minimum = next_request(&mut requests, request::GET_MIN_OFFSET).await;
    reply_offset(minimum, response::SUCCESS, "offset", 0);
    let maximum = next_request(&mut requests, request::GET_MAX_OFFSET).await;
    reply_offset(maximum, response::SUCCESS, "offset", 30);
    valid_seek.await.unwrap().unwrap();

    // The original pull reply arrives after seek invalidated its generation.
    pull_reply(original_pull, response::SUCCESS, 2, true);
    let pulled_after_seek = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    assert_eq!(
        pulled_after_seek.command.ext_fields.as_ref().unwrap()["queueOffset"],
        "10"
    );
    pull_reply(pulled_after_seek, response::SUCCESS, 11, true);

    let delivered = first.poll(Some(3_000)).await.unwrap();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].queue_offset(), 10);

    let pending_pull = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    assert_eq!(
        pending_pull.command.ext_fields.as_ref().unwrap()["queueOffset"],
        "11"
    );
    first.pause([queue.clone()]);
    pull_reply(pending_pull, response::PULL_NOT_FOUND, 11, false);
    assert!(
        timeout(Duration::from_millis(150), requests.recv())
            .await
            .is_err()
    );

    first
        .commit_offsets([(queue.clone(), 11)], true)
        .await
        .unwrap();
    first.commit_queues([queue.clone()], true).await.unwrap();
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&11));

    first.resume([queue.clone()]);
    let resumed_pull = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    assert_eq!(
        resumed_pull.command.ext_fields.as_ref().unwrap()["queueOffset"],
        "11"
    );
    first.pause([queue.clone()]);
    pull_reply(resumed_pull, response::PULL_NOT_FOUND, 11, false);
    first.shutdown().await.unwrap();

    let restarted = Arc::new(
        LitePullConsumer::new(lite_config_with_store(
            "lite-scripted-flow",
            "lite-scripted-client-2",
            &nameserver,
            store.clone(),
        ))
        .unwrap(),
    );
    restarted.assign([queue.clone()]).await.unwrap();
    restarted.start().await.unwrap();
    let restarted_pull = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    assert_eq!(
        restarted_pull.command.ext_fields.as_ref().unwrap()["queueOffset"],
        "11"
    );
    restarted.pause([queue]);
    pull_reply(restarted_pull, response::PULL_NOT_FOUND, 11, false);
    restarted.shutdown().await.unwrap();

    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn scripted_lite_delivers_concatenated_lz4_record_before_committing_its_offset() {
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let (nameserver, nameserver_task) =
        start_scripted_nameserver(broker_route(&broker_address)).await;
    let (mut requests, broker_task) = start_scripted_broker(broker_listener);
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let store = Arc::new(LiteScriptedOffsetStore::default());
    let mut config = lite_config_with_store(
        "lite-concat-lz4",
        "lite-concat-lz4-client",
        &nameserver,
        store.clone(),
    );
    config.set_pull_thread_nums(1);
    let consumer = Arc::new(LitePullConsumer::new(config).unwrap());
    consumer.assign([queue.clone()]).await.unwrap();
    consumer.start().await.unwrap();

    let pull = next_lite_pull_request(&mut requests, "lite-concat-lz4-client").await;
    pull_reply_with_body(pull, response::SUCCESS, 1, Some(concatenated_lz4_record()));
    let delivered = timeout(Duration::from_secs(3), consumer.poll(Some(3_000)))
        .await
        .expect("the concatenated LZ4 message should reach Lite poll")
        .unwrap();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].body().as_ref(), b"first-second");
    assert_eq!(delivered[0].queue_offset(), 0);

    consumer.pause([queue.clone()]);
    consumer.commit_queues([queue.clone()], true).await.unwrap();
    assert_eq!(store.memory.lock().unwrap().get(&queue), Some(&1));
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&1));

    consumer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn lite_rpc_hook_stream_identity_and_namespace_config_reach_runtime() {
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let route = broker_route(&broker_address);
    let (nameserver, nameserver_task) = start_scripted_nameserver(route).await;
    let (mut requests, broker_task) = start_scripted_broker(broker_listener);
    let store = Arc::new(LiteScriptedOffsetStore::default());
    let observed_codes = Arc::new(Mutex::new(Vec::new()));
    let hook_codes = observed_codes.clone();
    let hook: Arc<dyn RpcHook> = Arc::new(move |_address: &str, command: &RemotingCommand| {
        hook_codes.lock().unwrap().push(command.code);
    });
    let mut config = lite_config_with_store(
        "lite-rpc-hook-flow",
        "lite-rpc-hook-client",
        &nameserver,
        store.clone(),
    );
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    assert!(config.client_config().enable_stream_request_type());

    let consumer = Arc::new(
        LitePullConsumer::new_with_rpc_hook(config, Some(hook)).expect("Lite RPC-hook constructor"),
    );
    let queue = MessageQueue::new("orders", "broker-a", 0);
    consumer.assign([queue.clone()]).await.unwrap();
    consumer.start().await.unwrap();
    assert_eq!(
        consumer.get_consumer_group().as_deref(),
        Some("tenant%lite-rpc-hook-flow")
    );

    let pull = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    let fields = pull.command.ext_fields.as_ref().unwrap();
    assert_eq!(fields.get("topic"), Some(&"tenant%orders".to_owned()));
    assert_eq!(
        fields.get("consumerGroup"),
        Some(&"tenant%lite-rpc-hook-flow".to_owned())
    );
    assert!(
        observed_codes
            .lock()
            .unwrap()
            .contains(&request::LITE_PULL_MESSAGE)
    );

    consumer.pause([queue.clone()]);
    pull_reply(pull, response::PULL_NOT_FOUND, 0, false);
    consumer
        .commit_offsets([(queue.clone(), 5)], true)
        .await
        .unwrap();
    let namespaced_queue = MessageQueue::new("tenant%orders", "broker-a", 0);
    assert_eq!(
        store.memory.lock().unwrap().get(&namespaced_queue),
        Some(&5)
    );
    assert_eq!(
        store.persisted.lock().unwrap().get(&namespaced_queue),
        Some(&5)
    );
    assert_eq!(consumer.committed(&queue).await.unwrap(), 5);
    assert_eq!(
        consumer.assignment().await.unwrap(),
        [queue].into_iter().collect()
    );

    consumer.set_consumer_group("later-lite-rpc-hook-flow");
    assert!(matches!(
        consumer.start().await,
        Err(LitePullConsumerError::AlreadyStarted)
    ));
    assert_eq!(
        consumer.get_consumer_group().as_deref(),
        Some("tenant%later-lite-rpc-hook-flow")
    );

    consumer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn lite_runtime_settings_can_be_changed_after_consumer_construction() {
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let route = broker_route(&broker_address);
    let (nameserver, nameserver_task) = start_scripted_nameserver(route).await;
    let (mut requests, broker_task) = start_scripted_broker(broker_listener);
    let store = Arc::new(LiteScriptedOffsetStore::default());
    let config = lite_config_with_store(
        "lite-post-construction-settings",
        "lite-post-construction-client",
        &nameserver,
        store,
    );
    let consumer = Arc::new(LitePullConsumer::new(config).unwrap());
    let queue = MessageQueue::new("orders", "broker-a", 0);

    consumer.set_pull_thread_nums(2);
    consumer.set_pull_batch_size(11);
    consumer.set_auto_commit_interval_millis(2_000);
    consumer.set_auto_commit_interval_millis(999);
    consumer.set_pull_threshold_for_all(23);
    consumer.set_consume_max_span(29);
    consumer.set_pull_threshold_for_queue(31);
    consumer.set_pull_threshold_size_for_queue(37);
    consumer.set_poll_timeout_millis(20);
    consumer.set_consumer_pull_timeout_millis(47);
    consumer.set_consumer_timeout_millis_when_suspend(25_000);
    consumer.set_topic_metadata_check_interval_millis(43);
    assert_eq!(consumer.pull_thread_nums(), 2);
    assert_eq!(consumer.auto_commit_interval_millis(), 2_000);
    assert_eq!(consumer.pull_batch_size(), 11);
    assert_eq!(consumer.pull_threshold_for_all(), 23);
    assert_eq!(consumer.consume_max_span(), 29);
    assert_eq!(consumer.pull_threshold_for_queue(), 31);
    assert_eq!(consumer.pull_threshold_size_for_queue(), 37);
    assert_eq!(consumer.poll_timeout_millis(), 20);
    assert_eq!(consumer.consumer_pull_timeout_millis(), 47);
    assert_eq!(consumer.consumer_timeout_millis_when_suspend(), 25_000);
    assert_eq!(consumer.topic_metadata_check_interval_millis(), 43);

    consumer.assign([queue.clone()]).await.unwrap();
    consumer.start().await.unwrap();
    let first_pull = next_lite_pull_request(&mut requests, "lite-post-construction-client").await;
    assert_eq!(
        first_pull.command.ext_fields.as_ref().unwrap()["maxMsgNums"],
        "11"
    );

    consumer.set_pull_batch_size(13);
    pull_reply(first_pull, response::PULL_NOT_FOUND, 0, false);
    let next_pull = next_lite_pull_request(&mut requests, "lite-post-construction-client").await;
    assert_eq!(
        next_pull.command.ext_fields.as_ref().unwrap()["maxMsgNums"],
        "13"
    );

    consumer.set_consumer_timeout_millis_when_suspend(35);
    pull_reply(next_pull, response::PULL_NOT_FOUND, 0, false);
    let _after_timeout =
        next_lite_pull_request(&mut requests, "lite-post-construction-client").await;

    let poll = timeout(Duration::from_secs(1), consumer.poll(None))
        .await
        .expect("the changed no-argument poll timeout must be used");
    assert!(poll.unwrap().is_empty());

    consumer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn scripted_lite_helpers_cover_assignment_offsets_commits_and_name_server_updates() {
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let (nameserver, nameserver_task) =
        start_scripted_nameserver(broker_route(&broker_address)).await;
    let (updated_nameserver, updated_nameserver_task) =
        start_scripted_nameserver(broker_route_with(&broker_address, "broker-b", 2)).await;
    let (mut requests, broker_task) = start_scripted_broker(broker_listener);
    let store = Arc::new(LiteScriptedOffsetStore::default());
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let mut config = lite_config_with_store(
        "lite-helper-flow",
        "lite-helper-client",
        &nameserver,
        store.clone(),
    );
    config.set_pull_thread_nums(1);
    let consumer = Arc::new(LitePullConsumer::new(config).unwrap());
    consumer
        .set_sub_expression_for_assign("orders", "green")
        .await
        .unwrap();
    consumer.assign([queue.clone()]).await.unwrap();
    consumer.start().await.unwrap();
    assert!(matches!(
        consumer
            .set_sub_expression_for_assign("orders", "new-tag")
            .await,
        Err(LitePullConsumerError::AssignmentExpressionAfterStartup)
    ));

    let first_pull = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    let pull_fields = first_pull.command.ext_fields.as_ref().unwrap();
    assert_eq!(pull_fields.get("subscription"), Some(&"green".to_owned()));
    assert_eq!(pull_fields.get("expressionType"), Some(&"TAG".to_owned()));
    pull_reply(first_pull, response::SUCCESS, 1, true);
    consumer.set_consumer_group_nullable(None);
    let messages = consumer.poll(None).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].queue_offset(), 0);
    consumer.set_consumer_group("lite-helper-flow");

    let next_pull = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    assert_eq!(
        next_pull.command.ext_fields.as_ref().unwrap()["queueOffset"],
        "1"
    );
    consumer.pause([queue.clone()]);
    pull_reply(next_pull, response::PULL_NOT_FOUND, 1, false);

    consumer.commit_queues([queue.clone()], true).await.unwrap();
    assert_eq!(store.memory.lock().unwrap().get(&queue), Some(&1));
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&1));
    consumer
        .commit_sync_offsets([(queue.clone(), 7)], true)
        .await
        .unwrap();
    assert_eq!(store.memory.lock().unwrap().get(&queue), Some(&7));
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&7));

    let timestamp_consumer = consumer.clone();
    let timestamp_queue = queue.clone();
    let timestamp_lookup = tokio::spawn(async move {
        timestamp_consumer
            .offset_for_timestamp(&timestamp_queue, 1_700_000_000_000)
            .await
    });
    let timestamp_request = next_request(&mut requests, request::SEARCH_OFFSET_BY_TIMESTAMP).await;
    assert_eq!(
        timestamp_request.command.ext_fields.as_ref().unwrap()["timestamp"],
        "1700000000000"
    );
    reply_offset(timestamp_request, response::SUCCESS, "offset", 8);
    assert_eq!(timestamp_lookup.await.unwrap().unwrap(), 8);

    let timestamp_consumer = consumer.clone();
    let timestamp_queue = queue.clone();
    let failed_lookup = tokio::spawn(async move {
        timestamp_consumer
            .offset_for_timestamp(&timestamp_queue, 1_700_000_000_001)
            .await
    });
    let failed_request = next_request(&mut requests, request::SEARCH_OFFSET_BY_TIMESTAMP).await;
    reply_offset(failed_request, response::SYSTEM_ERROR, "offset", 0);
    assert!(failed_lookup.await.unwrap().is_err());

    let seek_consumer = consumer.clone();
    let seek_queue = queue.clone();
    let seek_begin = tokio::spawn(async move { seek_consumer.seek_to_begin(&seek_queue).await });
    let minimum = next_request(&mut requests, request::GET_MIN_OFFSET).await;
    reply_offset(minimum, response::SUCCESS, "offset", 0);
    let minimum = next_request(&mut requests, request::GET_MIN_OFFSET).await;
    reply_offset(minimum, response::SUCCESS, "offset", 0);
    let maximum = next_request(&mut requests, request::GET_MAX_OFFSET).await;
    reply_offset(maximum, response::SUCCESS, "offset", 30);
    seek_begin.await.unwrap().unwrap();
    consumer.resume([queue.clone()]);
    let seek_begin_pull = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    assert_eq!(
        seek_begin_pull.command.ext_fields.as_ref().unwrap()["queueOffset"],
        "0"
    );
    consumer.pause([queue.clone()]);
    pull_reply(seek_begin_pull, response::PULL_NOT_FOUND, 0, false);

    let seek_consumer = consumer.clone();
    let seek_queue = queue.clone();
    let seek_end = tokio::spawn(async move { seek_consumer.seek_to_end(&seek_queue).await });
    let maximum = next_request(&mut requests, request::GET_MAX_OFFSET).await;
    reply_offset(maximum, response::SUCCESS, "offset", 30);
    let minimum = next_request(&mut requests, request::GET_MIN_OFFSET).await;
    reply_offset(minimum, response::SUCCESS, "offset", 0);
    let maximum = next_request(&mut requests, request::GET_MAX_OFFSET).await;
    reply_offset(maximum, response::SUCCESS, "offset", 30);
    seek_end.await.unwrap().unwrap();
    consumer.resume([queue.clone()]);
    let seek_end_pull = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    assert_eq!(
        seek_end_pull.command.ext_fields.as_ref().unwrap()["queueOffset"],
        "30"
    );
    consumer.pause([queue.clone()]);
    pull_reply(seek_end_pull, response::PULL_NOT_FOUND, 30, false);

    assert_eq!(
        consumer.fetch_message_queues("orders").await.unwrap(),
        [queue.clone()].into_iter().collect()
    );
    consumer
        .update_name_server_address(updated_nameserver.clone())
        .await
        .unwrap();
    assert_eq!(
        consumer.fetch_message_queues("catalog").await.unwrap(),
        [
            MessageQueue::new("catalog", "broker-b", 0),
            MessageQueue::new("catalog", "broker-b", 1),
        ]
        .into_iter()
        .collect()
    );

    consumer.shutdown().await.unwrap();
    nameserver_task.abort();
    updated_nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn scripted_lite_subscription_unsubscribe_and_metadata_callbacks_match_java() {
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let (nameserver, nameserver_task) =
        start_scripted_nameserver(broker_route(&broker_address)).await;
    let (updated_nameserver, updated_nameserver_task) =
        start_scripted_nameserver(broker_route_with(&broker_address, "broker-a", 2)).await;
    let (mut requests, broker_task) = start_scripted_broker(broker_listener);
    let store = Arc::new(LiteScriptedOffsetStore::default());
    let mut config = lite_config_with_store(
        "lite-subscription-flow",
        "lite-subscription-client",
        &nameserver,
        store,
    );
    config.set_topic_metadata_check_interval_millis(10);
    config.client_config_mut().set_poll_name_server_interval(10);
    config.set_auto_commit(false);
    let consumer_id = config.client_config().build_client_id();
    let consumer = Arc::new(LitePullConsumer::new(config).unwrap());
    let selector = rocketmq_classic_consumer::pull::MessageSelector::by_tag("green");
    let (assignment_tx, mut assignment_rx) = mpsc::unbounded_channel();
    consumer
        .subscribe_with_listener(
            "orders",
            &selector,
            Arc::new(move |topic, all, assigned| {
                let _ = assignment_tx.send((topic.to_owned(), all.cloned(), assigned.clone()));
            }),
        )
        .await
        .unwrap();
    let (metadata_tx, mut metadata_rx) = mpsc::unbounded_channel();
    consumer
        .register_topic_message_queue_change_listener(
            "orders",
            Arc::new(move |topic, queues| {
                let _ = metadata_tx.send((topic.to_owned(), queues.clone()));
            }),
        )
        .await
        .unwrap();

    assert!(consumer.subscribe_with_expression("", "*").await.is_err());
    consumer.start().await.unwrap();
    let initial_pull = next_lite_pull_request(&mut requests, &consumer_id).await;
    assert_eq!(
        initial_pull.command.ext_fields.as_ref().unwrap()["subscription"],
        "green"
    );
    assert_eq!(
        initial_pull.command.ext_fields.as_ref().unwrap()["expressionType"],
        "TAG"
    );
    let (listener_topic, all_queues, assigned_queues) =
        timeout(Duration::from_secs(4), assignment_rx.recv())
            .await
            .expect("message-queue listener was not called")
            .expect("message-queue listener channel closed");
    let first_queue = MessageQueue::new("orders", "broker-a", 0);
    assert_eq!(listener_topic, "orders");
    assert_eq!(
        all_queues,
        Some([first_queue.clone()].into_iter().collect())
    );
    assert_eq!(assigned_queues, [first_queue.clone()].into_iter().collect());
    consumer.pause([first_queue.clone()]);
    pull_reply(initial_pull, response::PULL_NOT_FOUND, 0, false);
    assert!(metadata_rx.try_recv().is_err());

    consumer
        .update_name_server_address(updated_nameserver.clone())
        .await
        .unwrap();
    assert!(metadata_rx.try_recv().is_err());
    let (changed_topic, changed_queues) = timeout(Duration::from_secs(15), metadata_rx.recv())
        .await
        .expect("scheduled topic metadata listener was not called after route change")
        .expect("topic metadata listener channel closed");
    let second_queue = MessageQueue::new("orders", "broker-a", 1);
    assert_eq!(changed_topic, "orders");
    assert_eq!(
        changed_queues,
        [first_queue.clone(), second_queue.clone()]
            .into_iter()
            .collect()
    );
    let added_pull = next_lite_pull_request(&mut requests, &consumer_id).await;
    assert_eq!(
        added_pull.command.ext_fields.as_ref().unwrap()["queueId"],
        "1"
    );
    consumer.pause([second_queue.clone()]);
    pull_reply(added_pull, response::PULL_NOT_FOUND, 0, false);
    assert_eq!(
        consumer.assignment().await.unwrap(),
        [first_queue, second_queue].into_iter().collect()
    );

    let poll_consumer = consumer.clone();
    let mut poll = tokio::spawn(async move { poll_consumer.poll(Some(350)).await });
    tokio::time::sleep(Duration::from_millis(25)).await;
    let address_update_consumer = consumer.clone();
    let address_update = tokio::spawn(async move {
        address_update_consumer
            .update_name_server_address(updated_nameserver)
            .await
    });
    timeout(Duration::from_millis(100), address_update)
        .await
        .expect("NameServer address update waited for synchronized poll")
        .expect("NameServer address update task failed")
        .unwrap();
    let unsubscribe_consumer = consumer.clone();
    let mut unsubscription =
        tokio::spawn(async move { unsubscribe_consumer.unsubscribe("orders").await });
    assert!(
        timeout(Duration::from_millis(100), &mut unsubscription)
            .await
            .is_err(),
        "unsubscribe must wait while Java-equivalent synchronized poll is waiting"
    );
    assert!(
        timeout(Duration::from_secs(1), &mut poll)
            .await
            .expect("poll did not finish at its timeout")
            .expect("poll task failed")
            .unwrap()
            .is_empty()
    );
    assert!(unsubscription.await.unwrap().unwrap());
    assert!(consumer.assignment().await.unwrap().is_empty());
    assert!(!consumer.unsubscribe("orders").await.unwrap());
    consumer.shutdown().await.unwrap();
    nameserver_task.abort();
    updated_nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn running_lite_subscribe_sends_updated_heartbeat_and_refreshes_all_routes() {
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let (nameserver, mut route_requests, nameserver_task) =
        start_observed_scripted_nameserver(broker_route(&broker_address)).await;
    let (mut requests, broker_task, _, mut heartbeats) =
        start_scripted_broker_with_check_response(broker_listener, response::SUCCESS);
    let mut config = lite_config_with_store(
        "lite-running-subscribe-refresh",
        "lite-running-subscribe-refresh-client",
        &nameserver,
        Arc::new(LiteScriptedOffsetStore::default()),
    );
    config.set_message_model(MessageModel::Broadcasting);
    let consumer = Arc::new(LitePullConsumer::new(config).unwrap());
    consumer
        .subscribe_with_expression("orders", "green")
        .await
        .unwrap();

    let broker_service = tokio::spawn(async move {
        while let Some(broker_request) = requests.recv().await {
            let reply = match broker_request.command.code {
                request::LITE_PULL_MESSAGE => ScriptedReply {
                    code: response::PULL_NOT_FOUND,
                    fields: Some(HeaderMap::from([
                        ("suggestWhichBrokerId".into(), "0".into()),
                        ("nextBeginOffset".into(), "0".into()),
                        ("minOffset".into(), "0".into()),
                        ("maxOffset".into(), "1".into()),
                    ])),
                    body: None,
                },
                request::QUERY_CONSUMER_OFFSET
                | request::GET_MIN_OFFSET
                | request::GET_MAX_OFFSET => ScriptedReply {
                    code: response::SUCCESS,
                    fields: Some(HeaderMap::from([("offset".into(), "0".into())])),
                    body: None,
                },
                _ => ScriptedReply {
                    code: response::SUCCESS,
                    fields: None,
                    body: None,
                },
            };
            let _ = broker_request.reply.send(reply);
        }
    });

    consumer.start().await.unwrap();
    while route_requests.try_recv().is_ok() {}
    while heartbeats.try_recv().is_ok() {}

    consumer
        .subscribe_with_expression("invoices", "paid")
        .await
        .unwrap();

    let heartbeat = timeout(Duration::from_secs(2), async {
        loop {
            let command = heartbeats
                .recv()
                .await
                .expect("broker heartbeat observation channel closed");
            let body = command.body.as_deref().unwrap_or_default();
            if String::from_utf8_lossy(body).contains("\"topic\":\"invoices\"") {
                break command;
            }
        }
    })
    .await
    .expect("running subscribe did not send a heartbeat with the new subscription");
    assert_eq!(heartbeat.code, request::HEART_BEAT);

    let expected_topics = BTreeSet::from(["invoices".to_owned(), "orders".to_owned()]);
    let refreshed_topics = timeout(Duration::from_secs(2), async {
        let mut refreshed_topics = BTreeSet::new();
        while refreshed_topics != expected_topics {
            let command = route_requests
                .recv()
                .await
                .expect("NameServer route observation channel closed");
            assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
            refreshed_topics.insert(
                command
                    .ext_fields
                    .as_ref()
                    .expect("route query omitted its header")
                    .get("topic")
                    .expect("route query omitted its topic")
                    .to_owned(),
            );
        }
        refreshed_topics
    })
    .await
    .expect("running subscribe did not immediately refresh every subscribed route");
    assert_eq!(refreshed_topics, expected_topics);

    consumer.shutdown().await.unwrap();
    broker_service.abort();
    broker_task.abort();
    nameserver_task.abort();
}

#[tokio::test]
async fn lite_queue_listener_registration_preserves_java_route_failure_semantics() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/consumer/lifecycle.json")).unwrap();
    let java = &fixture["litePullListenerRegistrationRouteFailure"];
    assert_eq!(java["exception"], "MQClientException");
    assert_eq!(java["retryException"], "MQClientException");
    assert_eq!(java["listenerRegistered"], true);
    assert_eq!(java["consumerServiceState"], "RUNNING");
    assert_eq!(java["factoryServiceState"], "RUNNING");

    let (nameserver, nameserver_task) =
        start_scripted_nameserver(broker_route("127.0.0.1:1")).await;
    let mut config = lite_config_with_store(
        "lite-listener-registration-route-failure",
        "lite-listener-registration-route-failure-client",
        "127.0.0.1:1",
        Arc::new(LiteScriptedOffsetStore::default()),
    );
    config.client_config_mut().set_mq_client_api_timeout(100);
    let consumer = Arc::new(LitePullConsumer::new(config).unwrap());
    consumer.start().await.unwrap();

    let (callback_tx, mut callback_rx) = mpsc::unbounded_channel();
    let listener = Arc::new(
        move |topic: &str, queues: &std::collections::BTreeSet<MessageQueue>| {
            let _ = callback_tx.send((topic.to_owned(), queues.clone()));
        },
    );
    let registration_error = consumer
        .register_topic_message_queue_change_listener(
            "listener-registration-route-failure",
            listener.clone(),
        )
        .await
        .unwrap_err();
    assert!(matches!(registration_error, LitePullConsumerError::Pull(_)));
    let retry_error = consumer
        .register_topic_message_queue_change_listener(
            "listener-registration-route-failure",
            listener,
        )
        .await
        .unwrap_err();
    assert!(matches!(retry_error, LitePullConsumerError::Pull(_)));
    assert!(consumer.is_running().await);

    consumer
        .update_name_server_address(nameserver.clone())
        .await
        .unwrap();
    assert!(callback_rx.try_recv().is_err());
    let (topic, queues) = timeout(Duration::from_secs(15), callback_rx.recv())
        .await
        .expect("scheduled metadata check did not recover the retained listener")
        .expect("queue listener channel closed");
    assert_eq!(topic, "listener-registration-route-failure");
    assert_eq!(
        queues,
        [MessageQueue::new(topic, "broker-a", 0)]
            .into_iter()
            .collect()
    );

    consumer.shutdown().await.unwrap();
    nameserver_task.abort();
}

#[tokio::test]
async fn lite_sql_subscription_broker_check_failure_matches_java_startup() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/consumer/lifecycle.json")).unwrap();
    let java = &fixture["litePullBrokerSubscriptionCheckFailure"];
    assert_eq!(java["exception"], "MQClientException");
    assert_eq!(java["consumerServiceState"], "SHUTDOWN_ALREADY");
    assert_eq!(java["factoryServiceState"], "SHUTDOWN_ALREADY");
    assert_eq!(java["consumerRegistered"], false);
    assert_eq!(java["checkRequest"]["code"], request::CHECK_CLIENT_CONFIG);

    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let (nameserver, nameserver_task) =
        start_scripted_nameserver(broker_route(&broker_address)).await;
    let (mut requests, broker_task, mut check_requests, _) =
        start_scripted_broker_with_check_response(broker_listener, 44);
    let mut config = lite_config_with_store(
        "consumer-broker-check-lite",
        "consumer-broker-check-lite-client",
        &nameserver,
        Arc::new(LiteScriptedOffsetStore::default()),
    );
    config.set_message_model(MessageModel::Broadcasting);
    config.client_config_mut().set_mq_client_api_timeout(100);
    let client_id = config.client_config().build_client_id();
    let group = config.consumer_group().to_owned();
    let manager = ClientRuntimeManager::shared_default();
    let runtime = manager.get_or_create(config.client_config()).await.unwrap();
    let consumer = Arc::new(LitePullConsumer::new(config).unwrap());
    let selector = rocketmq_classic_consumer::pull::MessageSelector::by_sql("a > 1");
    consumer
        .subscribe_with_selector("lite-broker-check", &selector)
        .await
        .unwrap();

    let request_service = tokio::spawn(async move {
        while let Some(request) = requests.recv().await {
            let reply = match request.command.code {
                request::GET_CONSUMER_LIST_BY_GROUP => ScriptedReply {
                    code: response::SUCCESS,
                    fields: None,
                    body: Some(
                        serde_json::json!({"consumerIdList": [client_id]})
                            .to_string()
                            .into_bytes(),
                    ),
                },
                request::LITE_PULL_MESSAGE => ScriptedReply {
                    code: response::PULL_NOT_FOUND,
                    fields: Some(HeaderMap::from([
                        ("suggestWhichBrokerId".into(), "0".into()),
                        ("nextBeginOffset".into(), "0".into()),
                        ("minOffset".into(), "0".into()),
                        ("maxOffset".into(), "1".into()),
                    ])),
                    body: None,
                },
                request::QUERY_CONSUMER_OFFSET
                | request::GET_MIN_OFFSET
                | request::GET_MAX_OFFSET => ScriptedReply {
                    code: response::SUCCESS,
                    fields: Some(HeaderMap::from([("offset".into(), "0".into())])),
                    body: None,
                },
                _ => ScriptedReply {
                    code: response::SUCCESS,
                    fields: None,
                    body: None,
                },
            };
            let _ = request.reply.send(reply);
        }
    });

    let start_error = consumer.start().await.unwrap_err();
    assert!(matches!(start_error, LitePullConsumerError::Pull(_)));
    assert!(!consumer.is_running().await);
    assert_eq!(runtime.state(), RuntimeState::Stopped);
    assert!(runtime.select_consumer(&group).unwrap().is_none());

    let check = timeout(Duration::from_secs(2), check_requests.recv())
        .await
        .expect("Java startup check was not sent to the Broker")
        .expect("check request stream closed");
    assert_eq!(
        check.code,
        java["checkRequest"]["code"].as_i64().unwrap() as i32
    );
    let body: Value = serde_json::from_slice(check.body.as_deref().unwrap()).unwrap();
    assert_eq!(body["group"], java["checkRequest"]["body"]["group"]);
    let mut subscription = body["subscriptionData"].clone();
    subscription.as_object_mut().unwrap().remove("subVersion");
    assert_eq!(
        subscription,
        java["checkRequest"]["body"]["subscriptionData"]
    );

    manager
        .remove_client_factory(runtime.client_id(), &runtime)
        .unwrap();
    request_service.abort();
    broker_task.abort();
    nameserver_task.abort();
}

#[tokio::test]
async fn lite_listener_registration_waits_for_start_like_java_monitor() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/consumer/lifecycle.json")).unwrap();
    let java = &fixture["litePullMethodSynchronization"];
    assert_eq!(java["assign"], true);
    assert_eq!(java["isRunning"], true);
    assert_eq!(java["assignment"], true);
    assert_eq!(java["seek"], true);
    assert_eq!(java["commitAll"], true);
    assert_eq!(java["commitMap"], true);
    assert_eq!(java["commitSet"], true);
    assert_eq!(java["start"], true);
    assert_eq!(java["setSubExpressionForAssign"], true);
    assert_eq!(java["subscribeExpression"], true);
    assert_eq!(java["subscribeSelector"], true);
    assert_eq!(java["subscribeWithListener"], true);
    assert_eq!(java["unsubscribe"], true);
    assert_eq!(java["poll"], true);
    assert_eq!(java["shutdown"], true);
    assert_eq!(java["registerTopicMessageQueueChangeListener"], true);
    assert_eq!(java["updateNameServerAddr"], false);

    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let (nameserver, nameserver_task) =
        start_scripted_nameserver(broker_route(&broker_address)).await;
    let (mut check_requests, release_check, broker_task) =
        start_paused_check_broker(broker_listener);
    let mut config = lite_config_with_store(
        "lite-listener-registration-start-lock",
        "lite-listener-registration-start-lock-client",
        &nameserver,
        Arc::new(LiteScriptedOffsetStore::default()),
    );
    config.set_message_model(MessageModel::Broadcasting);
    config.client_config_mut().set_mq_client_api_timeout(5_000);
    let consumer = Arc::new(LitePullConsumer::new(config).unwrap());
    let selector = rocketmq_classic_consumer::pull::MessageSelector::by_sql("a > 1");
    consumer
        .subscribe_with_selector("lite-listener-registration-start-lock", &selector)
        .await
        .unwrap();

    let start_consumer = consumer.clone();
    let mut start = tokio::spawn(async move { start_consumer.start().await });
    let check = timeout(Duration::from_secs(4), check_requests.recv())
        .await
        .expect("LitePull start did not reach the Broker subscription check")
        .expect("Broker check request stream closed");
    assert_eq!(check.code, request::CHECK_CLIENT_CONFIG);

    let register_consumer = consumer.clone();
    let mut registration = tokio::spawn(async move {
        register_consumer
            .register_topic_message_queue_change_listener(
                "listener-registration-during-start",
                Arc::new(|_, _| {}),
            )
            .await
    });
    assert!(
        timeout(Duration::from_millis(100), &mut registration)
            .await
            .is_err(),
        "listener registration must wait for the Java-equivalent synchronized start"
    );
    let subscribe_consumer = consumer.clone();
    let mut subscription = tokio::spawn(async move {
        subscribe_consumer
            .subscribe_with_expression("subscription-during-start", "*")
            .await
    });
    assert!(
        timeout(Duration::from_millis(100), &mut subscription)
            .await
            .is_err(),
        "subscription must wait for the Java-equivalent synchronized start"
    );
    let unsubscribe_consumer = consumer.clone();
    let mut unsubscription = tokio::spawn(async move {
        unsubscribe_consumer
            .unsubscribe("lite-listener-registration-start-lock")
            .await
    });
    assert!(
        timeout(Duration::from_millis(100), &mut unsubscription)
            .await
            .is_err(),
        "unsubscription must wait for the Java-equivalent synchronized start"
    );
    let assign_consumer = consumer.clone();
    let mut assignment = tokio::spawn(async move {
        assign_consumer
            .assign([MessageQueue::new("other", "broker-a", 0)])
            .await
    });
    assert!(
        timeout(Duration::from_millis(100), &mut assignment)
            .await
            .is_err(),
        "assignment must wait for the Java-equivalent synchronized start"
    );
    let expression_consumer = consumer.clone();
    let mut assignment_expression = tokio::spawn(async move {
        expression_consumer
            .set_sub_expression_for_assign("other", "*")
            .await
    });
    assert!(
        timeout(Duration::from_millis(100), &mut assignment_expression)
            .await
            .is_err(),
        "assignment expression changes must wait for the Java-equivalent synchronized start"
    );
    let running_consumer = consumer.clone();
    let mut running_state = tokio::spawn(async move { running_consumer.is_running().await });
    assert!(
        timeout(Duration::from_millis(100), &mut running_state)
            .await
            .is_err(),
        "running-state inspection must wait for the Java-equivalent synchronized start"
    );
    let assigned_consumer = consumer.clone();
    let mut assigned_state = tokio::spawn(async move { assigned_consumer.assignment().await });
    assert!(
        timeout(Duration::from_millis(100), &mut assigned_state)
            .await
            .is_err(),
        "assignment inspection must wait for the Java-equivalent synchronized start"
    );
    let commit_consumer = consumer.clone();
    let mut commit = tokio::spawn(async move { commit_consumer.commit().await });
    assert!(
        timeout(Duration::from_millis(100), &mut commit)
            .await
            .is_err(),
        "commitAll must wait for the Java-equivalent synchronized start"
    );

    release_check
        .send(())
        .expect("paused Broker check was already released");
    timeout(Duration::from_secs(4), &mut start)
        .await
        .expect("LitePull start did not finish after the Broker check")
        .expect("LitePull start task failed")
        .unwrap();
    timeout(Duration::from_secs(4), &mut registration)
        .await
        .expect("listener registration did not resume after start")
        .expect("listener registration task failed")
        .unwrap();
    timeout(Duration::from_secs(4), &mut subscription)
        .await
        .expect("subscription did not resume after start")
        .expect("subscription task failed")
        .unwrap();
    assert!(
        timeout(Duration::from_secs(4), &mut unsubscription)
            .await
            .expect("unsubscription did not resume after start")
            .expect("unsubscription task failed")
            .unwrap()
    );
    assert!(matches!(
        timeout(Duration::from_secs(4), &mut assignment)
            .await
            .expect("assignment did not resume after start")
            .expect("assignment task failed"),
        Err(LitePullConsumerError::SubscriptionModeConflict)
    ));
    assert!(matches!(
        timeout(Duration::from_secs(4), &mut assignment_expression)
            .await
            .expect("assignment expression did not resume after start")
            .expect("assignment expression task failed"),
        Err(LitePullConsumerError::AssignmentExpressionAfterStartup)
    ));
    assert!(
        timeout(Duration::from_secs(4), &mut running_state)
            .await
            .expect("running-state inspection did not resume after start")
            .expect("running-state task failed")
    );
    timeout(Duration::from_secs(4), &mut assigned_state)
        .await
        .expect("assignment inspection did not resume after start")
        .expect("assignment inspection task failed")
        .unwrap();
    timeout(Duration::from_secs(4), &mut commit)
        .await
        .expect("commitAll did not resume after start")
        .expect("commitAll task failed")
        .unwrap();
    assert!(consumer.is_running().await);

    consumer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn lite_metadata_schedule_deadline_anchors_before_post_start_checks() {
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let (nameserver, nameserver_task) =
        start_scripted_nameserver(broker_route(&broker_address)).await;
    let (updated_nameserver, updated_nameserver_task) =
        start_scripted_nameserver(broker_route_with(&broker_address, "broker-a", 2)).await;
    let (mut check_requests, release_check, broker_task) =
        start_paused_check_broker(broker_listener);
    let mut config = lite_config_with_store(
        "lite-metadata-start-deadline",
        "lite-metadata-start-deadline-client",
        &nameserver,
        Arc::new(LiteScriptedOffsetStore::default()),
    );
    config.client_config_mut().set_mq_client_api_timeout(30_000);
    let consumer = Arc::new(LitePullConsumer::new(config).unwrap());
    let selector = rocketmq_classic_consumer::pull::MessageSelector::by_sql("a > 1");
    consumer
        .subscribe_with_selector("orders", &selector)
        .await
        .unwrap();
    let (metadata_tx, mut metadata_rx) = mpsc::unbounded_channel();
    consumer
        .register_topic_message_queue_change_listener(
            "orders",
            Arc::new(move |topic, queues| {
                let _ = metadata_tx.send((topic.to_owned(), queues.clone()));
            }),
        )
        .await
        .unwrap();

    let start_consumer = consumer.clone();
    let mut start = tokio::spawn(async move { start_consumer.start().await });
    let check = timeout(Duration::from_secs(4), check_requests.recv())
        .await
        .expect("LitePull start did not reach the Broker subscription check")
        .expect("Broker check request stream closed");
    assert_eq!(check.code, request::CHECK_CLIENT_CONFIG);

    consumer
        .update_name_server_address(updated_nameserver)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10_100)).await;
    release_check
        .send(())
        .expect("paused Broker check was already released");
    timeout(Duration::from_secs(4), &mut start)
        .await
        .expect("LitePull start did not finish after the Broker check")
        .expect("LitePull start task failed")
        .unwrap();

    let (topic, queues) = timeout(Duration::from_secs(2), metadata_rx.recv())
        .await
        .expect("metadata timer restarted its initial delay after startup")
        .expect("metadata listener channel closed");
    assert_eq!(topic, "orders");
    assert_eq!(
        queues,
        [
            MessageQueue::new("orders", "broker-a", 0),
            MessageQueue::new("orders", "broker-a", 1),
        ]
        .into_iter()
        .collect()
    );

    consumer.shutdown().await.unwrap();
    nameserver_task.abort();
    updated_nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn committed_reads_use_broker_offsets_and_preserve_rpc_errors() {
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let (nameserver, nameserver_task) =
        start_scripted_nameserver(broker_route_with(&broker_address, "broker-a", 3)).await;
    let (mut requests, broker_task) = start_scripted_broker(broker_listener);
    let mut config = LitePullConsumerConfig::new("lite-remote-offsets").unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(nameserver));
    config
        .client_config_mut()
        .set_instance_name(Some("lite-remote-offsets-client".into()));
    config
        .client_config_mut()
        .set_poll_name_server_interval(60_000);
    config
        .client_config_mut()
        .set_heartbeat_broker_interval(60_000);
    config
        .client_config_mut()
        .set_persist_consumer_offset_interval(60_000);
    config.set_auto_commit(false);
    config.set_pull_thread_nums(1);
    config
        .set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .unwrap();
    let consumer_id = config.client_config().build_client_id();
    let consumer = Arc::new(LitePullConsumer::new(config).unwrap());
    let assigned_queue = MessageQueue::new("orders", "broker-a", 0);
    consumer.assign([assigned_queue.clone()]).await.unwrap();
    consumer.start().await.unwrap();

    let initial_offset_request =
        next_request_servicing_group(&mut requests, &consumer_id, request::QUERY_CONSUMER_OFFSET)
            .await;
    assert_eq!(
        initial_offset_request.command.ext_fields.as_ref().unwrap()["queueId"],
        "0"
    );
    reply_offset(initial_offset_request, response::SUCCESS, "offset", 5);
    let pull = next_lite_pull_request(&mut requests, &consumer_id).await;
    assert_eq!(
        pull.command.ext_fields.as_ref().unwrap()["queueOffset"],
        "5"
    );
    consumer.pause([assigned_queue.clone()]);
    pull_reply(pull, response::PULL_NOT_FOUND, 5, false);
    assert_eq!(consumer.committed(&assigned_queue).await.unwrap(), 5);

    let committed_queue = MessageQueue::new("orders", "broker-a", 1);
    let read_consumer = consumer.clone();
    let read_queue = committed_queue.clone();
    let read = tokio::spawn(async move { read_consumer.committed(&read_queue).await });
    let read_request = next_request(&mut requests, request::QUERY_CONSUMER_OFFSET).await;
    assert_eq!(
        read_request.command.ext_fields.as_ref().unwrap()["queueId"],
        "1"
    );
    reply_offset(read_request, response::SUCCESS, "offset", 17);
    assert_eq!(read.await.unwrap().unwrap(), 17);

    let missing_queue = MessageQueue::new("orders", "broker-a", 2);
    let read_consumer = consumer.clone();
    let read_queue = missing_queue.clone();
    let failed_read = tokio::spawn(async move { read_consumer.committed(&read_queue).await });
    let failed_request = next_request(&mut requests, request::QUERY_CONSUMER_OFFSET).await;
    assert_eq!(
        failed_request.command.ext_fields.as_ref().unwrap()["queueId"],
        "2"
    );
    reply_offset(failed_request, response::SYSTEM_ERROR, "offset", 0);
    assert!(failed_read.await.unwrap().is_err());

    consumer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[test]
fn pinned_java_lite_pull_surface_defaults_and_setter_rules_are_recorded() {
    let fixture = java_fixture();
    assert_eq!(
        fixture["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );
    let defaults = &fixture["litePullDefaults"];
    assert_eq!(defaults["noArgumentConsumerGroup"], "DEFAULT_CONSUMER");
    assert_eq!(defaults["enableStreamRequestType"], true);
    assert_eq!(defaults["messageModel"], "CLUSTERING");
    assert_eq!(defaults["autoCommit"], true);
    assert_eq!(defaults["pullThreadNums"], 20);
    assert_eq!(defaults["autoCommitIntervalMillis"], 5_000);
    assert_eq!(defaults["pullBatchSize"], 10);
    assert_eq!(defaults["pullThresholdForAll"], 10_000);
    assert_eq!(defaults["pullThresholdForQueue"], 1_000);
    assert_eq!(defaults["pullThresholdSizeForQueue"], 100);
    assert_eq!(defaults["consumeMaxSpan"], 2_000);
    assert_eq!(defaults["pollTimeoutMillis"], 5_000);
    assert_eq!(defaults["topicMetadataCheckIntervalMillis"], 30_000);
    assert_eq!(defaults["consumeFromWhere"], "CONSUME_FROM_LAST_OFFSET");
    assert_eq!(defaults["consumeTimestampThirtyMinutesAgo"], true);
    assert_eq!(
        fixture["litePullConfigured"]["belowMinimumAutoCommitIntervalIgnored"],
        true
    );
    assert_eq!(
        fixture["litePullConfigured"]["autoCommitIntervalMillis"],
        2_000
    );

    let api = fixture["litePullApi"]
        .as_array()
        .expect("LitePullConsumer methods");
    for method in [
        "buildSubscriptionsForHeartbeat",
        "subscribe(",
        "unsubscribe(",
        "assign(",
        "poll(",
        "seek(",
        "pause(",
        "resume(",
        "commit(",
        "commitSync(",
        "assignment(",
        "committed(",
        "offsetForTimestamp(",
        "fetchMessageQueues(",
        "registerTopicMessageQueueChangeListener(",
        "updateNameServerAddress(",
    ] {
        assert!(
            api.iter()
                .any(|signature| signature.as_str().unwrap().contains(method)),
            "missing Java LitePullConsumer API {method}"
        );
    }
    let config_api = fixture["liteConfigApi"]
        .as_array()
        .expect("Lite config methods");
    for method in [
        "getSubscriptionsForHeartbeat",
        "setConnectBrokerByUser",
        "setDefaultBrokerId",
        "setPullBatchSize",
        "setAutoCommitIntervalMillis",
        "setConsumeFromWhere",
        "setOffsetStore",
        "setEnableMsgTrace",
        "setCustomizedTraceTopic",
        "setMessageQueueListener",
    ] {
        assert!(
            config_api
                .iter()
                .any(|signature| signature.as_str().unwrap().contains(method)),
            "missing Java DefaultLitePullConsumer setting API {method}"
        );
    }
    assert!(config_api.iter().any(|signature| {
        signature
            .as_str()
            .unwrap()
            .contains("getBrokerSuspendMaxTimeMillis")
    }));
    assert!(!config_api.iter().any(|signature| {
        signature
            .as_str()
            .unwrap()
            .contains("setBrokerSuspendMaxTimeMillis")
    }));
    let constructors = fixture["liteConstructors"]
        .as_array()
        .expect("Lite constructors");
    for constructor in [
        "DefaultLitePullConsumer()",
        "DefaultLitePullConsumer(java.lang.String)",
        "DefaultLitePullConsumer(org.apache.rocketmq.remoting.RPCHook)",
        "DefaultLitePullConsumer(java.lang.String,org.apache.rocketmq.remoting.RPCHook)",
        "DefaultLitePullConsumer(java.lang.String,java.lang.String,org.apache.rocketmq.remoting.RPCHook)",
    ] {
        assert!(
            constructors
                .iter()
                .any(|signature| signature.as_str().unwrap().contains(constructor)),
            "missing Java DefaultLitePullConsumer constructor {constructor}"
        );
    }
    assert!(
        LitePullConsumerConfig::new("stream-default")
            .unwrap()
            .client_config()
            .enable_stream_request_type()
    );
    assert_eq!(
        fixture["heartbeatSubscriptions"]["heartbeat-tag"]["expressionType"],
        "TAG"
    );
    assert_eq!(
        fixture["heartbeatSubscriptions"]["heartbeat-tag"]["tagCount"],
        2
    );
    assert_eq!(
        fixture["heartbeatSubscriptions"]["heartbeat-sql"]["expressionType"],
        "SQL92"
    );
    assert_eq!(
        fixture["invalidInputs"]["emptyAssignment"],
        "IllegalArgumentException"
    );
    assert_eq!(
        fixture["invalidInputs"]["subscribeThenAssign"],
        "IllegalStateException"
    );
    assert_eq!(
        fixture["invalidInputs"]["assignThenSubscribe"],
        "MQClientException/IllegalStateException"
    );
    assert_eq!(
        fixture["invalidInputs"]["suspendTimeout"],
        "MQClientException"
    );
    assert_eq!(
        fixture["invalidInputs"]["consumeFromWhere"],
        "RuntimeException"
    );
}

#[test]
fn pinned_java_pop_settings_result_status_and_receipt_fields_are_recorded() {
    let fixture = java_fixture();
    assert_eq!(fixture["pushPopDefaults"]["popThresholdForQueue"], 96);
    assert_eq!(fixture["pushPopDefaults"]["popInvisibleTime"], 60_000);
    assert_eq!(fixture["pushPopDefaults"]["popBatchNums"], 32);
    assert_eq!(fixture["popResultDefaults"]["status"], "FOUND");
    assert_eq!(fixture["popResultDefaults"]["popTime"], 0);
    assert_eq!(
        fixture["popResult"]["statusNames"],
        serde_json::json!(["FOUND", "NO_NEW_MSG", "POLLING_FULL", "POLLING_NOT_FOUND"])
    );
    assert_eq!(fixture["receipt"]["raw"], "17 123 456 2 1 broker-a 4 19");
    assert_eq!(fixture["receipt"]["checkpointQueueOffset"], 17);
    assert_eq!(fixture["receipt"]["popTime"], 123);
    assert_eq!(fixture["receipt"]["invisibleTime"], 456);
    assert_eq!(fixture["receipt"]["reviveQueueId"], 2);
    assert_eq!(fixture["receipt"]["brokerName"], "broker-a");
    assert_eq!(fixture["receipt"]["queueId"], 4);
    assert_eq!(fixture["receipt"]["queueOffset"], 19);
    assert_eq!(
        fixture["invalidReceipts"]["missingFields"],
        "IllegalArgumentException"
    );
    assert_eq!(
        fixture["invalidReceipts"]["invalidQueueOffset"],
        "NumberFormatException"
    );
}

#[tokio::test]
async fn lite_config_defaults_and_mode_exclusivity_match_java() {
    let config = LitePullConsumerConfig::new("lite-workers").unwrap();
    assert!(config.auto_commit());
    assert_eq!(config.pull_thread_nums(), 20);
    assert_eq!(config.auto_commit_interval_millis(), 5_000);
    assert_eq!(config.pull_batch_size(), 10);
    assert_eq!(config.pull_threshold_for_all(), 10_000);
    assert_eq!(config.pull_threshold_for_queue(), 1_000);
    assert_eq!(config.pull_threshold_size_for_queue(), 100);
    assert_eq!(config.consume_max_span(), 2_000);
    assert_eq!(config.poll_timeout_millis(), 5_000);
    assert_eq!(config.topic_metadata_check_interval_millis(), 30_000);
    assert_eq!(config.allocate_message_queue_strategy().name(), "AVG");
    assert_eq!(config.broker_suspend_max_time_millis(), 20_000);
    assert_eq!(config.consumer_timeout_millis_when_suspend(), 30_000);
    assert_eq!(config.consumer_pull_timeout_millis(), 10_000);
    assert_eq!(config.message_model(), MessageModel::Clustering);
    assert_eq!(
        config.consume_from_where(),
        ConsumeFromWhere::ConsumeFromLastOffset
    );
    assert!(!config.enable_msg_trace());
    assert!(config.consume_timestamp().len() >= 14);

    let consumer = LitePullConsumer::new(config).unwrap();
    consumer
        .subscribe_with_expression("orders", "*")
        .await
        .unwrap();
    assert!(
        consumer
            .assign(vec![MessageQueue::new("orders", "broker-a", 0)])
            .await
            .is_err()
    );

    let config = LitePullConsumerConfig::new("lite-assigned").unwrap();
    let consumer = LitePullConsumer::new(config).unwrap();
    consumer
        .assign(vec![MessageQueue::new("orders", "broker-a", 0)])
        .await
        .unwrap();
    assert!(
        consumer
            .subscribe_with_expression("orders", "*")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn lite_default_constructor_and_consume_origin_validation_match_java() {
    let config = LitePullConsumerConfig::new_default().unwrap();
    assert_eq!(config.consumer_group(), "DEFAULT_CONSUMER");
    let consumer = std::sync::Arc::new(LitePullConsumer::new(config).unwrap());
    assert!(matches!(
        consumer.start().await,
        Err(LitePullConsumerError::Pull(
            rocketmq_classic_consumer::PullConsumerError::DefaultConsumerGroup
        ))
    ));

    let mut config = LitePullConsumerConfig::new("lite-invalid-consume-origin").unwrap();
    assert!(matches!(
        config.set_consume_from_where(ConsumeFromWhere::ConsumeFromMinOffset),
        Err(LitePullConsumerError::InvalidConsumeFromWhere(
            ConsumeFromWhere::ConsumeFromMinOffset
        ))
    ));
    assert!(matches!(
        config.set_consume_from_where(ConsumeFromWhere::ConsumeFromMaxOffset),
        Err(LitePullConsumerError::InvalidConsumeFromWhere(
            ConsumeFromWhere::ConsumeFromMaxOffset
        ))
    ));
    assert!(matches!(
        config
            .set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst),
        Err(LitePullConsumerError::InvalidConsumeFromWhere(
            ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
        ))
    ));
    config
        .set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .unwrap();
    assert_eq!(
        config.consume_from_where(),
        ConsumeFromWhere::ConsumeFromFirstOffset
    );
}

#[tokio::test]
async fn explicit_lite_assignment_returns_the_callers_queue_topic_with_namespace() {
    let mut config = LitePullConsumerConfig::new("lite-namespaced-assignment").unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    let consumer = LitePullConsumer::new(config).unwrap();
    let queue = MessageQueue::new("orders", "broker-a", 0);
    consumer.assign([queue.clone()]).await.unwrap();

    assert_eq!(
        consumer.assignment().await.unwrap(),
        [queue].into_iter().collect()
    );
}

#[test]
fn push_pop_settings_match_pinned_java_defaults_and_setters() {
    let fixture = java_fixture();
    let defaults = &fixture["pushPopDefaults"];
    let config = PushConsumerConfig::new("pop-workers").unwrap();
    assert_eq!(
        config.pop_threshold_for_queue(),
        defaults["popThresholdForQueue"]
    );
    assert_eq!(
        config.pop_invisible_time_millis(),
        defaults["popInvisibleTime"]
    );
    assert_eq!(config.pop_batch_nums(), defaults["popBatchNums"]);

    let configured = &fixture["pushPopConfigured"];
    config.set_pop_threshold_for_queue(configured["popThresholdForQueue"].as_i64().unwrap() as i32);
    config.set_pop_invisible_time_millis(configured["popInvisibleTime"].as_i64().unwrap());
    config.set_pop_batch_nums(configured["popBatchNums"].as_i64().unwrap() as i32);
    assert_eq!(
        config.pop_threshold_for_queue(),
        configured["popThresholdForQueue"]
    );
    assert_eq!(
        config.pop_invisible_time_millis(),
        configured["popInvisibleTime"]
    );
    assert_eq!(config.pop_batch_nums(), configured["popBatchNums"]);
}

#[test]
fn lite_auto_commit_interval_below_java_minimum_is_ignored() {
    let mut config = LitePullConsumerConfig::new("lite-commit").unwrap();
    config.set_auto_commit_interval_millis(999);
    assert_eq!(config.auto_commit_interval_millis(), 5_000);
    config.set_auto_commit_interval_millis(2_000);
    assert_eq!(config.auto_commit_interval_millis(), 2_000);
}

#[test]
fn lite_configured_values_match_the_pinned_java_consumer_settings() {
    let fixture = java_fixture();
    let configured = &fixture["litePullConfigured"];
    let mut config = LitePullConsumerConfig::new("lite-configured-group").unwrap();
    config.set_auto_commit(configured["autoCommit"].as_bool().unwrap());
    config
        .set_auto_commit_interval_millis(configured["autoCommitIntervalMillis"].as_i64().unwrap());
    config
        .set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .unwrap();
    config.set_consume_max_span(configured["consumeMaxSpan"].as_i64().unwrap() as i32);
    config.set_consume_timestamp(configured["consumeTimestamp"].as_str().unwrap());
    config.set_consumer_pull_timeout_millis(
        configured["consumerPullTimeoutMillis"].as_i64().unwrap(),
    );
    config.set_consumer_timeout_millis_when_suspend(
        configured["consumerTimeoutMillisWhenSuspend"]
            .as_i64()
            .unwrap(),
    );
    config.set_customized_trace_topic(Some(
        configured["customizedTraceTopic"]
            .as_str()
            .unwrap()
            .to_owned(),
    ));
    config.set_default_broker_id(17);
    config.set_enable_msg_trace(configured["enableMessageTrace"].as_bool().unwrap());
    config.set_message_model(MessageModel::Broadcasting);
    config.set_poll_timeout_millis(configured["pollTimeoutMillis"].as_i64().unwrap());
    config.set_pull_batch_size(configured["pullBatchSize"].as_i64().unwrap() as i32);
    config.set_pull_thread_nums(configured["pullThreadNums"].as_i64().unwrap() as i32);
    config.set_pull_threshold_for_all(configured["pullThresholdForAll"].as_i64().unwrap());
    config
        .set_pull_threshold_for_queue(configured["pullThresholdForQueue"].as_i64().unwrap() as i32);
    config.set_pull_threshold_size_for_queue(
        configured["pullThresholdSizeForQueue"].as_i64().unwrap() as i32,
    );
    config.set_topic_metadata_check_interval_millis(
        configured["topicMetadataCheckIntervalMillis"]
            .as_i64()
            .unwrap(),
    );
    config.set_unit_mode(configured["unitMode"].as_bool().unwrap());
    config.set_connect_broker_by_user(true);
    config.set_message_queue_listener(Some(std::sync::Arc::new(|_, _, _| {})));

    assert_eq!(config.consumer_group(), "lite-configured-group");
    assert!(!config.auto_commit());
    assert_eq!(config.auto_commit_interval_millis(), 2_000);
    assert_eq!(
        config.consume_from_where(),
        ConsumeFromWhere::ConsumeFromFirstOffset
    );
    assert_eq!(config.consume_max_span(), 29);
    assert_eq!(config.consume_timestamp(), "20000101000000");
    assert_eq!(config.consumer_pull_timeout_millis(), 47);
    assert_eq!(config.consumer_timeout_millis_when_suspend(), 53);
    assert_eq!(
        config.customized_trace_topic(),
        Some("trace-configured-topic")
    );
    assert!(config.client_config().enable_trace());
    assert_eq!(
        config.client_config().trace_topic(),
        Some("trace-configured-topic")
    );
    assert_eq!(config.default_broker_id(), 17);
    assert!(config.enable_msg_trace());
    assert_eq!(config.message_model(), MessageModel::Broadcasting);
    assert_eq!(config.poll_timeout_millis(), 41);
    assert_eq!(config.pull_batch_size(), 13);
    assert_eq!(config.pull_thread_nums(), 7);
    assert_eq!(config.pull_threshold_for_all(), 23);
    assert_eq!(config.pull_threshold_for_queue(), 31);
    assert_eq!(config.pull_threshold_size_for_queue(), 37);
    assert_eq!(config.topic_metadata_check_interval_millis(), 43);
    assert!(config.unit_mode());
    assert!(config.client_config().unit_mode());
    assert!(config.connect_broker_by_user());
    assert!(config.message_queue_listener().is_some());
}

#[tokio::test]
async fn lite_trace_dispatcher_uses_java_topic_defaults_and_lifecycle() {
    let mut config = LitePullConsumerConfig::new("lite-trace-lifecycle").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    // This integration-style lifecycle case should not share the process-wide
    // client runtime with the concurrently running Lite consumer tests.
    config
        .client_config_mut()
        .set_instance_name(Some("lite-trace-lifecycle-test".into()));
    config.set_enable_msg_trace(true);
    let consumer = std::sync::Arc::new(LitePullConsumer::new(config).unwrap());
    assert!(consumer.trace_dispatcher().is_none());

    consumer.start().await.unwrap();
    let dispatcher = consumer.trace_dispatcher().unwrap();
    assert_eq!(dispatcher.trace_topic_name(), "RMQ_SYS_TRACE_TOPIC");
    consumer.shutdown().await.unwrap();
    assert!(!consumer.is_running().await);
}

#[tokio::test]
async fn lite_start_validates_consumer_timeouts_and_thread_settings_before_runtime_start() {
    let mut config = LitePullConsumerConfig::new("lite-invalid-timeout").unwrap();
    config.set_consumer_timeout_millis_when_suspend(19_999);
    let consumer = std::sync::Arc::new(LitePullConsumer::new(config).unwrap());
    assert!(matches!(
        consumer.start().await,
        Err(LitePullConsumerError::Pull(_))
    ));
    assert!(!consumer.is_running().await);

    let mut config = LitePullConsumerConfig::new("lite-invalid-threads").unwrap();
    config.set_pull_thread_nums(-1);
    let consumer = std::sync::Arc::new(LitePullConsumer::new(config).unwrap());
    assert!(matches!(
        consumer.start().await,
        Err(LitePullConsumerError::InvalidPullThreadCount)
    ));

    let mut config = LitePullConsumerConfig::new("lite-zero-threads").unwrap();
    config.set_pull_thread_nums(0);
    config.client_config_mut().set_namesrv_addr(None);
    let consumer = std::sync::Arc::new(LitePullConsumer::new(config).unwrap());
    consumer.start().await.unwrap();
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn lite_poll_requires_start_and_auto_commit_is_live_configurable() {
    let config = LitePullConsumerConfig::new("lite-poll-state").unwrap();
    let consumer = LitePullConsumer::new(config).unwrap();
    assert!(matches!(
        consumer.poll(Some(0)).await,
        Err(LitePullConsumerError::NotStarted)
    ));
    assert!(consumer.is_auto_commit());
    consumer.set_auto_commit(false);
    assert!(!consumer.is_auto_commit());
    assert!(!consumer.is_connect_broker_by_user());
    consumer.set_connect_broker_by_user(true);
    assert!(consumer.is_connect_broker_by_user());
    assert_eq!(consumer.default_broker_id(), 0);
    consumer.set_default_broker_id(17);
    assert_eq!(consumer.default_broker_id(), 17);
}

#[tokio::test]
async fn lite_assignment_is_available_before_start_and_created_shutdown_is_a_noop() {
    let config = LitePullConsumerConfig::new("lite-assignment-before-start").unwrap();
    let consumer = LitePullConsumer::new(config).unwrap();
    assert!(matches!(
        consumer.assign(Vec::<MessageQueue>::new()).await,
        Err(LitePullConsumerError::EmptyAssignment)
    ));
    let queues = vec![MessageQueue::new("orders", "broker-a", 0)];
    consumer.assign(queues.clone()).await.unwrap();
    assert!(matches!(
        consumer.set_sub_expression_for_assign("orders", "  ").await,
        Err(LitePullConsumerError::EmptyAssignmentExpression)
    ));
    assert_eq!(
        consumer.assignment().await.unwrap(),
        queues.into_iter().collect()
    );

    consumer.shutdown().await.unwrap();
    assert!(!consumer.is_running().await);
    consumer
        .set_sub_expression_for_assign("orders", "paid")
        .await
        .unwrap();
}

#[tokio::test]
async fn lite_metadata_offset_and_seek_helpers_report_not_started_errors() {
    let consumer = Arc::new(
        LitePullConsumer::new(LitePullConsumerConfig::new("lite-helper-before-start").unwrap())
            .unwrap(),
    );
    let queue = MessageQueue::new("orders", "broker-a", 0);
    assert!(matches!(
        consumer.fetch_message_queues("orders").await,
        Err(LitePullConsumerError::NotStarted)
    ));
    assert!(matches!(
        consumer.update_name_server_address("127.0.0.1:9876").await,
        Err(LitePullConsumerError::NotStarted)
    ));
    assert!(matches!(
        consumer.offset_for_timestamp(&queue, 0).await,
        Err(LitePullConsumerError::NotStarted)
    ));
    assert!(matches!(
        consumer.committed(&queue).await,
        Err(LitePullConsumerError::NotStarted)
    ));
    assert!(matches!(
        consumer.seek_to_begin(&queue).await,
        Err(LitePullConsumerError::NotStarted)
    ));
    assert!(matches!(
        consumer.seek_to_end(&queue).await,
        Err(LitePullConsumerError::NotStarted)
    ));
    assert!(
        consumer
            .register_topic_message_queue_change_listener("", Arc::new(|_, _| {}))
            .await
            .is_ok()
    );
}
