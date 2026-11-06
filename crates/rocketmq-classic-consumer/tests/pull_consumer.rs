use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use base64::Engine;
use bytes::BytesMut;
use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_consumer::{
    PullConsumerConfig, PullConsumerError,
    offset::{OffsetRead, OffsetStore, OffsetStoreFuture, ReadOffsetType},
    pull::{FilterMessageContext, FilterMessageHook, PullAPIWrapper, PullConsumer},
};
use rocketmq_classic_protocol::{
    codes::{request, response},
    header::{BoundaryType, HeaderMap},
};
use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

struct UnitModeHook(Arc<std::sync::atomic::AtomicBool>);

impl FilterMessageHook for UnitModeHook {
    fn hook_name(&self) -> &str {
        "unit-mode-test"
    }

    fn filter_message(&self, context: &mut FilterMessageContext) {
        self.0
            .store(context.unit_mode, std::sync::atomic::Ordering::SeqCst);
    }
}

#[derive(Default)]
struct RecordingOffsetStore {
    values: Mutex<HashMap<MessageQueue, i64>>,
    updates: Mutex<Vec<(MessageQueue, i64, bool)>>,
    single_persists: Mutex<Vec<MessageQueue>>,
    all_persists: Mutex<Vec<Vec<MessageQueue>>>,
}

impl OffsetStore for RecordingOffsetStore {
    fn load(&self) -> OffsetStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn update_offset(&self, queue: &MessageQueue, offset: i64, increase_only: bool) {
        self.updates
            .lock()
            .unwrap()
            .push((queue.clone(), offset, increase_only));
        self.values.lock().unwrap().insert(queue.clone(), offset);
    }

    fn update_and_freeze_offset(&self, queue: &MessageQueue, offset: i64) {
        self.update_offset(queue, offset, false);
    }

    fn read_offset<'a>(
        &'a self,
        queue: &'a MessageQueue,
        _mode: ReadOffsetType,
    ) -> OffsetStoreFuture<'a, OffsetRead> {
        Box::pin(async move {
            Ok(self
                .values
                .lock()
                .unwrap()
                .get(queue)
                .copied()
                .map_or(OffsetRead::NotFound, OffsetRead::Found))
        })
    }

    fn persist<'a>(&'a self, queue: &'a MessageQueue) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async move {
            self.single_persists.lock().unwrap().push(queue.clone());
            Ok(())
        })
    }

    fn persist_all<'a>(&'a self, queues: &'a [MessageQueue]) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async move {
            self.all_persists.lock().unwrap().push(queues.to_vec());
            Ok(())
        })
    }

    fn remove_offset(&self, queue: &MessageQueue) {
        self.values.lock().unwrap().remove(queue);
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

async fn read_command(stream: &mut TcpStream) -> RemotingCommand {
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
    code: i32,
    suggest_broker_id: i64,
    body: Option<Vec<u8>>,
) {
    let fields = HeaderMap::from([
        ("suggestWhichBrokerId".into(), suggest_broker_id.to_string()),
        ("nextBeginOffset".into(), "101".into()),
        ("minOffset".into(), "0".into()),
        ("maxOffset".into(), "200".into()),
    ]);
    let mut reply = RemotingCommand {
        code,
        opaque: command.opaque,
        ext_fields: Some(fields.into()),
        body,
        ..Default::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply).unwrap())
        .await
        .unwrap();
}

async fn respond_empty(stream: &mut TcpStream, command: &RemotingCommand, code: i32) {
    let mut reply = RemotingCommand {
        code,
        opaque: command.opaque,
        ..Default::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply).unwrap())
        .await
        .unwrap();
}

fn route(master_address: &str, slave_address: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "queueDatas": [{
            "brokerName": "broker",
            "readQueueNums": 3,
            "writeQueueNums": 3,
            "perm": 6,
            "topicSysFlag": 0
        }],
        "brokerDatas": [{
            "cluster": "cluster",
            "brokerName": "broker",
            "brokerAddrs": {"0": master_address, "1": slave_address}
        }],
        "filterServerTable": {}
    }))
    .unwrap()
}

fn message_record() -> Vec<u8> {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "ipv4-v1")
        .unwrap();
    base64::engine::general_purpose::STANDARD
        .decode(case["recordBase64"].as_str().unwrap())
        .unwrap()
}

fn message_record_with_topic(topic: &str) -> Vec<u8> {
    let mut record = message_record();
    let original = b"orders";
    let topic_position = (1..record.len() - original.len())
        .find(|position| {
            record[*position..].starts_with(original)
                && record[*position - 1] == original.len() as u8
        })
        .expect("fixture contains its one-byte length-prefixed topic");
    assert!(topic.len() < i8::MAX as usize);
    record[topic_position - 1] = topic.len() as u8;
    record.splice(
        topic_position..topic_position + original.len(),
        topic.bytes(),
    );
    let store_size = i32::from_be_bytes(record[..4].try_into().unwrap());
    let updated_store_size = store_size + topic.len() as i32 - original.len() as i32;
    record[..4].copy_from_slice(&updated_store_size.to_be_bytes());
    record
}

async fn read_or_accept(
    listener: &TcpListener,
    current: &mut Option<TcpStream>,
) -> RemotingCommand {
    loop {
        if current.is_none() {
            *current = Some(listener.accept().await.unwrap().0);
        }
        let stream = current.as_mut().unwrap();
        match timeout(Duration::from_millis(500), read_command(stream)).await {
            Ok(command) => return command,
            Err(_) => *current = None,
        }
    }
}

async fn read_pull_command(
    listener: &TcpListener,
    current: &mut Option<TcpStream>,
) -> RemotingCommand {
    loop {
        let command = read_or_accept(listener, current).await;
        if command.code == request::HEART_BEAT {
            respond_empty(current.as_mut().unwrap(), &command, response::SUCCESS).await;
        } else {
            return command;
        }
    }
}

#[tokio::test]
async fn pull_routes_per_queue_returns_decoded_messages_and_only_explicitly_persists_offsets() {
    let master_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let master_address = master_listener.local_addr().unwrap().to_string();
    let slave_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slave_address = slave_listener.local_addr().unwrap().to_string();
    let nameserver_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nameserver_address = nameserver_listener.local_addr().unwrap().to_string();
    let route = route(&master_address, &slave_address);

    let nameserver_peer = tokio::spawn(async move {
        let (mut stream, _) = nameserver_listener.accept().await.unwrap();
        let command = read_command(&mut stream).await;
        assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("topic"),
            Some(&"tenant%orders".to_owned())
        );
        let mut reply = RemotingCommand {
            code: response::SUCCESS,
            opaque: command.opaque,
            body: Some(route),
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
    });

    let master_peer = tokio::spawn(async move {
        let mut stream = None;
        for (request_index, expected_queue_id) in [0, 1, 2].into_iter().enumerate() {
            let command = read_pull_command(&master_listener, &mut stream).await;
            assert_eq!(command.code, request::PULL_MESSAGE);
            let fields = command.ext_fields.as_ref().unwrap();
            assert_eq!(
                fields.get("consumerGroup"),
                Some(&"tenant%pull-group".to_owned())
            );
            assert_eq!(fields.get("topic"), Some(&"tenant%orders".to_owned()));
            assert_eq!(fields.get("queueId"), Some(&expected_queue_id.to_string()));
            assert_eq!(fields.get("sysFlag"), Some(&"4".to_owned()));
            assert_eq!(fields.get("commitOffset"), Some(&"0".to_owned()));
            assert_eq!(fields.get("expressionType"), Some(&"TAG".to_owned()));
            assert_eq!(fields.get("subVersion"), Some(&"0".to_owned()));
            assert_eq!(fields.get("bname"), Some(&"broker".to_owned()));
            assert!(!fields.contains_key("brokerName"));
            assert_eq!(
                fields.get("maxMsgBytes"),
                Some(
                    &(if request_index == 0 {
                        "4096".to_owned()
                    } else {
                        i32::MAX.to_string()
                    })
                )
            );
            if expected_queue_id == 0 {
                respond(
                    stream.as_mut().unwrap(),
                    &command,
                    response::SUCCESS,
                    1,
                    Some(message_record_with_topic("tenant%orders")),
                )
                .await;
            } else if expected_queue_id == 1 {
                respond(
                    stream.as_mut().unwrap(),
                    &command,
                    response::PULL_NOT_FOUND,
                    0,
                    None,
                )
                .await;
            } else {
                respond(
                    stream.as_mut().unwrap(),
                    &command,
                    response::PULL_OFFSET_MOVED,
                    0,
                    None,
                )
                .await;
            }
        }
    });

    let slave_peer = tokio::spawn(async move {
        let (stream, _) = slave_listener.accept().await.unwrap();
        let mut current = Some(stream);
        let command = read_pull_command(&slave_listener, &mut current).await;
        assert_eq!(command.code, request::PULL_MESSAGE);
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("sysFlag"),
            Some(&"4".to_owned())
        );
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("commitOffset"),
            Some(&"0".to_owned())
        );
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("queueId"),
            Some(&"0".to_owned())
        );
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("maxMsgBytes"),
            Some(&i32::MAX.to_string())
        );
        respond(
            current.as_mut().unwrap(),
            &command,
            response::PULL_RETRY_IMMEDIATELY,
            1,
            None,
        )
        .await;
    });

    let unique_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string();
    let mut config = PullConsumerConfig::new("pull-group").unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    config
        .client_config_mut()
        .set_namesrv_addr(Some(nameserver_address));
    config
        .client_config_mut()
        .set_instance_name(Some(format!("pull-consumer-test-{unique_id}")));
    config
        .client_config_mut()
        .set_poll_name_server_interval(60_000);
    config
        .client_config_mut()
        .set_heartbeat_broker_interval(60_000);
    config
        .client_config_mut()
        .set_persist_consumer_offset_interval(60_000);
    config.set_unit_mode(true);

    let store = Arc::new(RecordingOffsetStore::default());
    let consumer = Arc::new(PullConsumer::new(config));
    let pre_start_wrapper = Arc::new(PullAPIWrapper::new(false));
    consumer.set_pull_api_wrapper(Some(pre_start_wrapper.clone()));
    let hook_ran_in_unit_mode = Arc::new(std::sync::atomic::AtomicBool::new(false));
    consumer.register_filter_message_hook(Arc::new(UnitModeHook(hook_ran_in_unit_mode.clone())));
    consumer
        .set_offset_store(Some(store.clone()))
        .await
        .unwrap();
    consumer.start().await.unwrap();
    let started_wrapper = consumer.pull_api_wrapper().unwrap();
    assert!(!Arc::ptr_eq(&started_wrapper, &pre_start_wrapper));

    let queue0 = MessageQueue::new("orders", "broker", 0);
    let queue1 = MessageQueue::new("orders", "broker", 1);
    assert!(matches!(
        consumer.pull_with_max_size(&queue0, None, 100, 16, 0).await,
        Err(PullConsumerError::InvalidMaxMessageBytes)
    ));
    let found = consumer
        .pull_with_max_size(&queue0, None, 100, 16, 4096)
        .await
        .unwrap();
    assert_eq!(
        found.result.status,
        rocketmq_classic_core::api::consumer::PullStatus::Found
    );
    assert_eq!(found.result.next_begin_offset, 101);
    let messages = found.messages.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].topic(), "orders");
    assert!(hook_ran_in_unit_mode.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        consumer
            .pull(&queue0, None, 101, 16)
            .await
            .unwrap()
            .result
            .status,
        rocketmq_classic_core::api::consumer::PullStatus::NoMatchedMessage
    );
    assert_eq!(
        consumer
            .pull(&queue1, None, 100, 16)
            .await
            .unwrap()
            .result
            .status,
        rocketmq_classic_core::api::consumer::PullStatus::NoNewMessage
    );
    let queue2 = MessageQueue::new("orders", "broker", 2);
    assert_eq!(
        consumer
            .pull(&queue2, None, 100, 16)
            .await
            .unwrap()
            .result
            .status,
        rocketmq_classic_core::api::consumer::PullStatus::OffsetIllegal
    );
    assert!(store.updates.lock().unwrap().is_empty());
    assert!(store.single_persists.lock().unwrap().is_empty());
    assert!(store.all_persists.lock().unwrap().is_empty());

    consumer
        .update_consume_offset(&queue0, found.result.next_begin_offset)
        .await
        .unwrap();
    assert_eq!(
        consumer.fetch_consume_offset(&queue0, false).await.unwrap(),
        101
    );
    consumer.persist_offset(&queue0).await.unwrap();
    consumer.persist_consumer_offset().await.unwrap();
    assert_eq!(store.updates.lock().unwrap().len(), 1);
    assert_eq!(store.single_persists.lock().unwrap().len(), 1);
    assert_eq!(store.all_persists.lock().unwrap().len(), 1);
    let namespaced_queue0 = MessageQueue::new("tenant%orders", "broker", 0);
    assert_eq!(
        store.values.lock().unwrap().get(&namespaced_queue0),
        Some(&101)
    );
    assert_eq!(
        store.updates.lock().unwrap().as_slice(),
        &[(namespaced_queue0.clone(), 101, false)]
    );
    assert_eq!(
        store.single_persists.lock().unwrap().as_slice(),
        &[namespaced_queue0.clone()]
    );
    assert_eq!(
        store.all_persists.lock().unwrap().as_slice(),
        &[Vec::<MessageQueue>::new()]
    );

    timeout(Duration::from_secs(3), nameserver_peer)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(3), master_peer)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(3), slave_peer)
        .await
        .unwrap()
        .unwrap();
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn boundary_lookups_route_to_master_and_preserve_signed_success_values() {
    let master_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let master_address = master_listener.local_addr().unwrap().to_string();
    let slave_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slave_address = slave_listener.local_addr().unwrap().to_string();
    let nameserver_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nameserver_address = nameserver_listener.local_addr().unwrap().to_string();
    let route = route(&master_address, &slave_address);

    let nameserver_peer = tokio::spawn(async move {
        let (mut stream, _) = nameserver_listener.accept().await.unwrap();
        let command = read_command(&mut stream).await;
        assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("topic"),
            Some(&"orders".to_owned())
        );
        let mut reply = RemotingCommand {
            code: response::SUCCESS,
            opaque: command.opaque,
            body: Some(route),
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
    });

    let master_peer = tokio::spawn(async move {
        let mut stream = None;
        let expected_codes = [
            request::SEARCH_OFFSET_BY_TIMESTAMP,
            request::GET_MAX_OFFSET,
            request::GET_MIN_OFFSET,
            request::GET_EARLIEST_MSG_STORETIME,
        ];
        for (index, expected_code) in expected_codes.into_iter().enumerate() {
            let command = loop {
                let command = read_or_accept(&master_listener, &mut stream).await;
                if command.code == request::HEART_BEAT {
                    respond_empty(stream.as_mut().unwrap(), &command, response::SUCCESS).await;
                    continue;
                }
                break command;
            };
            assert_eq!(command.code, expected_code);
            let fields = command.ext_fields.as_ref().unwrap();
            assert_eq!(fields.get("topic"), Some(&"orders".to_owned()));
            assert_eq!(fields.get("queueId"), Some(&"2".to_owned()));
            assert_eq!(fields.get("bname"), Some(&"broker".to_owned()));
            match expected_code {
                request::SEARCH_OFFSET_BY_TIMESTAMP => {
                    assert_eq!(fields.get("timestamp"), Some(&"-7".to_owned()));
                    assert_eq!(fields.get("boundaryType"), Some(&"UPPER".to_owned()));
                }
                request::GET_MAX_OFFSET => {
                    assert_eq!(fields.get("committed"), Some(&"true".to_owned()));
                }
                request::GET_MIN_OFFSET | request::GET_EARLIEST_MSG_STORETIME => {}
                _ => unreachable!(),
            }
            let response_fields = match index {
                0 | 1 => HeaderMap::from([("offset".into(), "-1".into())]),
                2 => HeaderMap::from([("offset".into(), "3".into())]),
                3 => HeaderMap::from([("timestamp".into(), "-1".into())]),
                _ => unreachable!(),
            };
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: command.opaque,
                ext_fields: Some(response_fields.into()),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .as_mut()
                .unwrap()
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        }
    });

    let unique_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string();
    let mut config = PullConsumerConfig::new("boundary-group").unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(nameserver_address));
    config
        .client_config_mut()
        .set_instance_name(Some(format!("pull-boundary-test-{unique_id}")));
    config
        .client_config_mut()
        .set_poll_name_server_interval(60_000);
    config
        .client_config_mut()
        .set_heartbeat_broker_interval(60_000);
    config
        .client_config_mut()
        .set_persist_consumer_offset_interval(60_000);
    let consumer = Arc::new(PullConsumer::new(config));
    consumer
        .set_offset_store(Some(Arc::new(RecordingOffsetStore::default())))
        .await
        .unwrap();
    consumer.start().await.unwrap();

    let queue = MessageQueue::new("orders", "broker", 2);
    assert_eq!(
        consumer
            .search_offset_with_boundary(&queue, -7, BoundaryType::Upper)
            .await
            .unwrap(),
        -1
    );
    assert_eq!(consumer.get_max_offset(&queue).await.unwrap(), -1);
    assert_eq!(consumer.get_min_offset(&queue).await.unwrap(), 3);
    assert_eq!(
        consumer.get_earliest_msg_store_time(&queue).await.unwrap(),
        -1
    );

    timeout(Duration::from_secs(3), nameserver_peer)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(3), master_peer)
        .await
        .unwrap()
        .unwrap();
    consumer.shutdown().await.unwrap();
}
