use std::time::Duration;

use bytes::BytesMut;
use rocketmq_classic_core::{
    api::{
        MQClientApi,
        producer::{SendKind, SendMode},
    },
    error::RpcError,
};
use rocketmq_classic_protocol::{
    body::{
        BatchAckMessageRequestBody, LiteSubscriptionCtlRequestBody, LockBatchRequestBody,
        QueryAssignmentRequestBody, SetMessageRequestModeRequestBody, UnlockBatchRequestBody,
    },
    codes::{request, response},
    header::{
        AckMessageRequestHeader, ChangeInvisibleTimeRequestHeader,
        ConsumerSendMsgBackRequestHeader, EndTransactionRequestHeader, HeaderMap,
        PopLiteMessageRequestHeader, PopMessageRequestHeader, PullMessageRequestHeader,
        QueryConsumerOffsetRequestHeader, RecallMessageRequestHeader,
        UnregisterClientRequestHeader, UpdateConsumerOffsetRequestHeader,
    },
    heartbeat::HeartbeatData,
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RemotingClient, TransportMode},
    codec::frame,
    error::TransportError,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Instant, timeout},
};

async fn read_command(stream: &mut tokio::net::TcpStream) -> RemotingCommand {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes).unwrap().unwrap()
}

async fn scripted_probe_peer(
    replies: Vec<(i32, Option<String>, Option<HeaderMap>)>,
) -> (String, tokio::task::JoinHandle<Vec<RemotingCommand>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut requests = Vec::with_capacity(replies.len());
        for (code, remark, ext_fields) in replies {
            let request = read_command(&mut stream).await;
            let mut reply = RemotingCommand {
                code,
                opaque: request.opaque,
                remark,
                ext_fields: ext_fields.map(Into::into),
                ..RemotingCommand::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
            requests.push(request);
        }
        requests
    });
    (addr, task)
}

// A peer that consumes one request without responding, then watches for a retry.
async fn silent_peer() -> (String, tokio::task::JoinHandle<RemotingCommand>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let command = read_command(&mut stream).await;
        let mut extra = [0; 1];
        match timeout(Duration::from_millis(250), stream.read(&mut extra)).await {
            Ok(Ok(0)) | Err(_) => {}
            other => panic!("unexpected second attempt: {other:?}"),
        }
        assert!(
            timeout(Duration::from_millis(25), listener.accept())
                .await
                .is_err(),
            "unexpected retry on a new connection"
        );
        command
    });
    (addr, task)
}

// Each case sends to a silent Broker: a timed-out request must not retry, while
// a oneway call must complete without waiting for the Broker's response.
macro_rules! broker_attempt {
    ($name:expr, $code:expr, $oneway:expr, |$api:ident, $addr:ident, $deadline:ident| $call:expr) => {{
        let (address, peer) = silent_peer().await;
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let $api = MQClientApi::new(client.clone());
        let $addr = address.as_str();
        let $deadline = Instant::now() + Duration::from_millis(100);
        let result = $call.await;
        if $oneway {
            assert!(result.is_ok(), "{}: {result:?}", $name);
        } else {
            assert!(
                matches!(result, Err(RpcError::Transport(ref error)) if error.is_timeout() || matches!(error, TransportError::SendRequest { .. })),
                "{}: {result:?}",
                $name
            );
        }
        let command = peer.await.unwrap();
        assert_eq!(command.code, $code, "{}", $name);
        assert_eq!(command.is_oneway(), $oneway, "{}", $name);
        client.shutdown().await;
    }};
}

#[tokio::test]
async fn broker_methods_use_one_attempt_and_honor_deadline() {
    broker_attempt!(
        "pull_message",
        request::PULL_MESSAGE,
        false,
        |api, addr, deadline| api.pull_message(
            addr,
            &{
                let mut __phase18_header_0 = PullMessageRequestHeader::default();
                __phase18_header_0.set_sys_flag(Some(0));
                __phase18_header_0
            },
            deadline
        )
    );
    broker_attempt!(
        "pull_message_future",
        request::PULL_MESSAGE,
        false,
        |api, addr, deadline| api.pull_message_future(
            addr,
            &{
                let mut __phase18_header_1 = PullMessageRequestHeader::default();
                __phase18_header_1.set_sys_flag(Some(0));
                __phase18_header_1
            },
            deadline
        )
    );
    broker_attempt!(
        "lite_pull_message",
        request::LITE_PULL_MESSAGE,
        false,
        |api, addr, deadline| api.pull_message(
            addr,
            &{
                let mut __phase18_header_2 = PullMessageRequestHeader::default();
                __phase18_header_2.set_sys_flag(Some(
                    rocketmq_classic_common::sysflag::pull::build_pull_sys_flag_with_lite(
                        false, false, false, false, true,
                    ),
                ));
                __phase18_header_2
            },
            deadline
        )
    );
    broker_attempt!(
        "lite_pull_message_future",
        request::LITE_PULL_MESSAGE,
        false,
        |api, addr, deadline| api.pull_message_future(
            addr,
            &{
                let mut __phase18_header_3 = PullMessageRequestHeader::default();
                __phase18_header_3.set_sys_flag(Some(
                    rocketmq_classic_common::sysflag::pull::build_pull_sys_flag_with_lite(
                        false, false, false, false, true,
                    ),
                ));
                __phase18_header_3
            },
            deadline
        )
    );
    broker_attempt!(
        "query_consumer_offset",
        request::QUERY_CONSUMER_OFFSET,
        false,
        |api, addr, deadline| api.query_consumer_offset(
            addr,
            &QueryConsumerOffsetRequestHeader::default(),
            deadline
        )
    );
    broker_attempt!(
        "update_consumer_offset",
        request::UPDATE_CONSUMER_OFFSET,
        false,
        |api, addr, deadline| api.update_consumer_offset(
            addr,
            &UpdateConsumerOffsetRequestHeader::default(),
            false,
            deadline
        )
    );
    broker_attempt!(
        "update_consumer_offset oneway",
        request::UPDATE_CONSUMER_OFFSET,
        true,
        |api, addr, deadline| api.update_consumer_offset(
            addr,
            &UpdateConsumerOffsetRequestHeader::default(),
            true,
            deadline
        )
    );
    broker_attempt!(
        "get_consumer_list_by_group",
        request::GET_CONSUMER_LIST_BY_GROUP,
        false,
        |api, addr, deadline| api.get_consumer_list_by_group(addr, "group", deadline)
    );
    broker_attempt!(
        "consumer_send_message_back",
        request::CONSUMER_SEND_MSG_BACK,
        false,
        |api, addr, deadline| api.consumer_send_message_back(
            addr,
            &ConsumerSendMsgBackRequestHeader::default(),
            deadline
        )
    );
    broker_attempt!(
        "lock_batch_mq",
        request::LOCK_BATCH_MQ,
        false,
        |api, addr, deadline| api.lock_batch_mq(addr, &LockBatchRequestBody::default(), deadline)
    );
    broker_attempt!(
        "unlock_batch_mq",
        request::UNLOCK_BATCH_MQ,
        false,
        |api, addr, deadline| api.unlock_batch_mq(
            addr,
            &UnlockBatchRequestBody::default(),
            false,
            deadline
        )
    );
    broker_attempt!(
        "unlock_batch_mq oneway",
        request::UNLOCK_BATCH_MQ,
        true,
        |api, addr, deadline| api.unlock_batch_mq(
            addr,
            &UnlockBatchRequestBody::default(),
            true,
            deadline
        )
    );

    broker_attempt!(
        "send_heartbeat",
        request::HEART_BEAT,
        false,
        |api, addr, deadline| api.send_heartbeat(addr, &HeartbeatData::default(), deadline)
    );
    broker_attempt!(
        "send_heartbeat_v2",
        request::HEART_BEAT,
        false,
        |api, addr, deadline| api.send_heartbeat_v2(addr, &HeartbeatData::default(), deadline)
    );
    broker_attempt!(
        "unregister_client",
        request::UNREGISTER_CLIENT,
        false,
        |api, addr, deadline| api.unregister_client(
            addr,
            &UnregisterClientRequestHeader::default(),
            deadline
        )
    );
    broker_attempt!(
        "query_assignment",
        request::QUERY_ASSIGNMENT,
        false,
        |api, addr, deadline| api.query_assignment(
            addr,
            &QueryAssignmentRequestBody::default(),
            deadline
        )
    );
    broker_attempt!(
        "set_message_request_mode",
        request::SET_MESSAGE_REQUEST_MODE,
        false,
        |api, addr, deadline| api.set_message_request_mode(
            addr,
            &SetMessageRequestModeRequestBody::default(),
            deadline
        )
    );
    broker_attempt!(
        "lite_subscription_ctl",
        request::LITE_SUBSCRIPTION_CTL,
        false,
        |api, addr, deadline| api.lite_subscription_ctl(
            addr,
            &LiteSubscriptionCtlRequestBody::default(),
            deadline
        )
    );
    broker_attempt!(
        "pop_message",
        request::POP_MESSAGE,
        false,
        |api, addr, deadline| api.pop_message(addr, &PopMessageRequestHeader::default(), deadline)
    );
    broker_attempt!(
        "pop_lite_message",
        request::POP_LITE_MESSAGE,
        false,
        |api, addr, deadline| api.pop_lite_message(
            addr,
            &PopLiteMessageRequestHeader::default(),
            deadline
        )
    );
    broker_attempt!(
        "ack_message",
        request::ACK_MESSAGE,
        false,
        |api, addr, deadline| api.ack_message(addr, &AckMessageRequestHeader::default(), deadline)
    );
    broker_attempt!(
        "batch_ack_message",
        request::BATCH_ACK_MESSAGE,
        false,
        |api, addr, deadline| api.batch_ack_message(
            addr,
            &BatchAckMessageRequestBody::default(),
            deadline
        )
    );
    broker_attempt!(
        "change_invisible_time",
        request::CHANGE_MESSAGE_INVISIBLETIME,
        false,
        |api, addr, deadline| api.change_invisible_time(
            addr,
            "broker",
            &ChangeInvisibleTimeRequestHeader::default(),
            deadline
        )
    );

    broker_attempt!(
        "end_transaction_oneway",
        request::END_TRANSACTION,
        true,
        |api, addr, deadline| api.end_transaction_oneway(
            addr,
            &EndTransactionRequestHeader::default(),
            Some("done".into()),
            deadline
        )
    );
    broker_attempt!(
        "recall_message",
        request::RECALL_MESSAGE,
        false,
        |api, addr, deadline| api.recall_message(
            addr,
            &{
                let mut __phase18_header_4 = RecallMessageRequestHeader::default();
                __phase18_header_4.set_producer_group(Some("recall-group".into()));
                __phase18_header_4.set_topic("recall-topic".into());
                __phase18_header_4.set_recall_handle("v1-handle".into());
                __phase18_header_4.set_broker_name(Some("broker-a".into()));
                __phase18_header_4
            },
            deadline
        )
    );
    for (kind, code) in [
        (SendKind::V1, request::SEND_MESSAGE),
        (SendKind::V2, request::SEND_MESSAGE_V2),
        (SendKind::Batch, request::SEND_BATCH_MESSAGE),
        (SendKind::ReplyV1, request::SEND_REPLY_MESSAGE),
        (SendKind::ReplyV2, request::SEND_REPLY_MESSAGE_V2),
    ] {
        for mode in [SendMode::Sync, SendMode::Future, SendMode::Oneway] {
            broker_attempt!(
                format!("send_message {kind:?} {mode:?}"),
                code,
                mode == SendMode::Oneway,
                |api, addr, deadline| api.send_message(
                    addr,
                    "broker",
                    &Default::default(),
                    b"payload".to_vec(),
                    "unique",
                    kind,
                    mode,
                    deadline
                )
            );
        }
    }
}

#[tokio::test]
async fn recall_message_matches_java_request_header_and_decodes_broker_msg_id() {
    let (addr, peer) = scripted_probe_peer(vec![(
        response::SUCCESS,
        None,
        Some(HeaderMap::from([("msgId".into(), "recalled-id".into())])),
    )])
    .await;
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let api = MQClientApi::new(client.clone());
    let header = {
        let mut __phase18_header_5 = RecallMessageRequestHeader::default();
        __phase18_header_5.set_producer_group(Some("recall-group".into()));
        __phase18_header_5.set_topic("recall-topic".into());
        __phase18_header_5.set_recall_handle("v1-handle".into());
        __phase18_header_5.set_broker_name(Some("broker-a".into()));
        __phase18_header_5
    };

    assert_eq!(
        api.recall_message(&addr, &header, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap(),
        Some("recalled-id".to_owned())
    );
    let requests = peer.await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].code, request::RECALL_MESSAGE);
    assert_eq!(
        requests[0].ext_fields.as_ref().unwrap(),
        &HeaderMap::from([
            ("producerGroup".into(), "recall-group".into()),
            ("topic".into(), "recall-topic".into()),
            ("recallHandle".into(), "v1-handle".into()),
            ("bname".into(), "broker-a".into()),
        ])
    );
    client.shutdown().await;
}

#[tokio::test]
async fn recall_message_preserves_broker_errors_and_nullable_success_header() {
    let (addr, peer) = scripted_probe_peer(vec![(
        response::SYSTEM_ERROR,
        Some("broker failed".into()),
        None,
    )])
    .await;
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let api = MQClientApi::new(client.clone());
    let result = api
        .recall_message(
            &addr,
            &{
                let mut __phase18_header_6 = RecallMessageRequestHeader::default();
                __phase18_header_6.set_topic("recall-topic".into());
                __phase18_header_6.set_recall_handle("v1-handle".into());
                __phase18_header_6
            },
            Instant::now() + Duration::from_secs(1),
        )
        .await;
    assert!(
        matches!(result, Err(RpcError::Response { code, .. }) if code == response::SYSTEM_ERROR)
    );
    assert_eq!(peer.await.unwrap()[0].code, request::RECALL_MESSAGE);
    client.shutdown().await;

    let (addr, peer) =
        scripted_probe_peer(vec![(response::SUCCESS, None, Some(HeaderMap::new()))]).await;
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let api = MQClientApi::new(client.clone());
    let result = api
        .recall_message(
            &addr,
            &{
                let mut __phase18_header_7 = RecallMessageRequestHeader::default();
                __phase18_header_7.set_topic("recall-topic".into());
                __phase18_header_7.set_recall_handle("v1-handle".into());
                __phase18_header_7
            },
            Instant::now() + Duration::from_secs(1),
        )
        .await;
    assert_eq!(result.unwrap(), None);
    assert_eq!(peer.await.unwrap()[0].code, request::RECALL_MESSAGE);
    client.shutdown().await;

    let (addr, peer) = scripted_probe_peer(vec![(response::SUCCESS, None, None)]).await;
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let api = MQClientApi::new(client.clone());
    let result = api
        .recall_message(
            &addr,
            &{
                let mut __phase18_header_8 = RecallMessageRequestHeader::default();
                __phase18_header_8.set_topic("recall-topic".into());
                __phase18_header_8.set_recall_handle("v1-handle".into());
                __phase18_header_8
            },
            Instant::now() + Duration::from_secs(1),
        )
        .await;
    assert_eq!(result.unwrap(), None);
    assert_eq!(peer.await.unwrap()[0].code, request::RECALL_MESSAGE);
    client.shutdown().await;
}

async fn route_reply(code: i32, body: Option<Vec<u8>>) -> (MQClientApi, RemotingClient) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut length = [0; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut bytes = BytesMut::from(&length[..]);
        let size = u32::from_be_bytes(length) as usize;
        bytes.resize(size + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        let incoming = frame::decode_next(&mut bytes).unwrap().unwrap();
        assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
        assert_eq!(incoming.ext_fields.unwrap()["topic"], "test-topic");
        let mut reply = RemotingCommand {
            code,
            opaque: incoming.opaque,
            remark: Some("not found".into()),
            body,
            ..RemotingCommand::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.update_name_server_address_list(vec![address.to_string()]);
    client.start().unwrap();
    (MQClientApi::new(client.clone()), client)
}

#[tokio::test]
async fn route_success_unknown_status_and_malformed_success() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let java_route = fixture["inheritedSerializationCases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["id"] == "route-populated")
        .unwrap();
    let nonempty = java_route["instanceEncodedJson"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (api, client) = route_reply(response::SUCCESS, Some(nonempty)).await;
    let route = api
        .get_topic_route("test-topic", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    let queues = route.publish_queues("test-topic");
    assert_eq!(queues.len(), 2);
    assert_eq!(queues[0].broker_name(), Some("broker"));
    assert_eq!(queues[0].queue_id(), 0);
    assert_eq!(queues[1].queue_id(), 1);
    let brokers = route.broker_datas_snapshot().unwrap();
    let broker = brokers[0].as_ref().unwrap();
    assert_eq!(
        broker
            .broker_addrs
            .as_ref()
            .unwrap()
            .with(|addresses| addresses.get(&Some(0)).cloned().flatten()),
        Some("master:10911".to_owned())
    );
    let mappings = route.topic_queue_mapping_by_broker_snapshot().unwrap();
    let mapping = mappings["broker"].as_ref().unwrap();
    assert_eq!(
        mapping
            .curr_id_map
            .as_ref()
            .unwrap()
            .with(|values| values.len()),
        2
    );
    client.shutdown().await;
    for (code, body) in [
        (response::SUCCESS, Some(br#"{"queueDatas":[],"brokerDatas":[],"filterServerTable":{},"topicQueueMappingByBroker":null}"#.to_vec())),
        (response::TOPIC_NOT_EXIST, None),
        (987654, None),
        (response::SUCCESS, None),
        (response::SUCCESS, Some(b"{".to_vec())),
    ] {
        let (api, client) = route_reply(code, body.clone()).await;
        let result = api
            .get_topic_route("test-topic", Instant::now() + Duration::from_secs(2))
            .await;
        match (code, body) {
            (response::SUCCESS, Some(body)) if body.starts_with(b"{\"queueDatas\"") => {
                assert!(result
                    .unwrap()
                    .unwrap()
                    .publish_queues("test-topic")
                    .is_empty());
            }
            (response::SUCCESS, _) => assert!(matches!(result, Err(RpcError::MalformedResponse { .. }))),
            (expected, _) => assert!(matches!(result, Err(RpcError::Response { code, peer, request_code: request::GET_ROUTEINFO_BY_TOPIC, .. }) if code == expected && peer == client.get_name_server_address_list().unwrap()[0])),
        }
        client.shutdown().await;
    }
}

#[tokio::test]
async fn route_json_null_success_body_returns_none() {
    let (api, client) = route_reply(response::SUCCESS, Some(b"null".to_vec())).await;
    let result = api
        .get_topic_route("test-topic", Instant::now() + Duration::from_secs(2))
        .await;
    assert!(matches!(result, Ok(None)), "{result:?}");
    client.shutdown().await;
}

#[tokio::test]
async fn route_fails_over_unavailable_nameserver_before_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let good = listener.local_addr().unwrap();
    let unavailable = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bad = unavailable.local_addr().unwrap();
    drop(unavailable);
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut len = [0; 4];
        stream.read_exact(&mut len).await.unwrap();
        let mut bytes = BytesMut::from(&len[..]);
        bytes.resize(u32::from_be_bytes(len) as usize + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        let incoming = frame::decode_next(&mut bytes).unwrap().unwrap();
        let mut reply = RemotingCommand {
            code: response::TOPIC_NOT_EXIST,
            opaque: incoming.opaque,
            ..RemotingCommand::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
    });
    let client = RemotingClient::new(TransportMode::Plain);
    // The selector starts at index 1; this forces the first connection to fail.
    client.update_name_server_address_list(vec![good.to_string(), bad.to_string()]);
    client.start().unwrap();
    let result = MQClientApi::new(client.clone())
        .get_topic_route("test-topic", Instant::now() + Duration::from_secs(2))
        .await;
    assert!(
        matches!(result, Err(RpcError::Response { peer, code: response::TOPIC_NOT_EXIST, .. }) if peer == good.to_string())
    );
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn route_failover_returns_success_from_second_nameserver_within_original_deadline() {
    let good_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let good = good_listener.local_addr().unwrap().to_string();
    let unavailable = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bad = unavailable.local_addr().unwrap().to_string();
    drop(unavailable);
    let peer = tokio::spawn(async move {
        let (mut stream, _) = good_listener.accept().await.unwrap();
        let incoming = read_command(&mut stream).await;
        assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
        assert_eq!(incoming.ext_fields.as_ref().unwrap()["topic"], "test-topic");
        let mut reply = RemotingCommand {
            code: response::SUCCESS,
            opaque: incoming.opaque,
            body: Some(br#"{"queueDatas":[],"brokerDatas":[],"filterServerTable":{},"topicQueueMappingByBroker":null}"#.to_vec()),
            ..RemotingCommand::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
        assert!(
            timeout(Duration::from_millis(100), read_command(&mut stream))
                .await
                .is_err(),
            "route lookup retried after a successful response"
        );
    });
    let client = RemotingClient::new(TransportMode::Plain);
    // The initial selector chooses index 1, the refused endpoint.
    client.update_name_server_address_list(vec![good.clone(), bad]);
    client.start().unwrap();
    let route = timeout(
        Duration::from_secs(2),
        MQClientApi::new(client.clone())
            .get_topic_route("test-topic", Instant::now() + Duration::from_secs(1)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert!(route.publish_queues("test-topic").is_empty());
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn route_without_nameserver_keeps_transport_error() {
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let result = MQClientApi::new(client.clone())
        .get_topic_route("test-topic", Instant::now() + Duration::from_secs(1))
        .await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(TransportError::InvalidConfiguration(_)))
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn broker_send_modes_make_one_attempt_without_retry_on_timeout() {
    for mode in [SendMode::Sync, SendMode::Future] {
        let (addr, peer) = silent_peer().await;
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let result = MQClientApi::new(client.clone())
            .send_message(
                &addr,
                "broker",
                &Default::default(),
                b"payload".to_vec(),
                "unique",
                SendKind::V1,
                mode,
                Instant::now() + Duration::from_millis(100),
            )
            .await;
        assert!(matches!(result, Err(RpcError::Transport(_))), "{result:?}");
        let command = peer.await.unwrap();
        assert_eq!(command.code, request::SEND_MESSAGE);
        assert!(!command.is_oneway());
        client.shutdown().await;
    }
}

#[tokio::test]
async fn oneway_offset_returns_without_reply_and_expired_deadline_emits_nothing() {
    let (addr, peer) = silent_peer().await;
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let api = MQClientApi::new(client.clone());
    let header = UpdateConsumerOffsetRequestHeader::default();
    api.update_consumer_offset(
        &addr,
        &header,
        true,
        Instant::now() + Duration::from_secs(2),
    )
    .await
    .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::UPDATE_CONSUMER_OFFSET);
    assert!(command.is_oneway());
    let result = api
        .update_consumer_offset(
            &addr,
            &header,
            true,
            Instant::now() - Duration::from_millis(1),
        )
        .await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(TransportError::Timeout))
    ));
    client.shutdown().await;
}

// An expired oneway deadline must not put a frame on the wire, regardless of
// which public facade entry point selects the oneway transport.
#[tokio::test]
async fn expired_oneway_variants_emit_no_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let api = MQClientApi::new(client.clone());
    let expired = Instant::now() - Duration::from_millis(1);
    let results = [
        api.update_consumer_offset(
            &addr,
            &UpdateConsumerOffsetRequestHeader::default(),
            true,
            expired,
        )
        .await,
        api.unlock_batch_mq(&addr, &UnlockBatchRequestBody::default(), true, expired)
            .await,
        api.end_transaction_oneway(
            &addr,
            &EndTransactionRequestHeader::default(),
            None,
            expired,
        )
        .await,
    ];
    for result in results {
        assert!(
            matches!(result, Err(RpcError::Transport(TransportError::Timeout))),
            "{result:?}"
        );
    }
    for kind in [
        SendKind::V1,
        SendKind::V2,
        SendKind::Batch,
        SendKind::ReplyV1,
        SendKind::ReplyV2,
    ] {
        let result = api
            .send_message(
                &addr,
                "broker",
                &Default::default(),
                vec![1],
                "unique",
                kind,
                SendMode::Oneway,
                expired,
            )
            .await;
        assert!(
            matches!(result, Err(RpcError::Transport(TransportError::Timeout))),
            "{kind:?}: {result:?}"
        );
    }
    assert!(
        timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
    client.shutdown().await;
}

#[tokio::test]
async fn unsolicited_oneway_reply_without_pending_does_not_poison_next_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let oneway = read_command(&mut stream).await;
        assert_eq!(oneway.code, request::UPDATE_CONSUMER_OFFSET);
        assert!(oneway.is_oneway());
        let mut unsolicited = RemotingCommand {
            code: response::SYSTEM_ERROR,
            opaque: oneway.opaque,
            ..RemotingCommand::default()
        };
        unsolicited.mark_response();
        stream
            .write_all(&frame::encode(&unsolicited).unwrap())
            .await
            .unwrap();
        // Give the reader time to discard the unsolicited reply before a new
        // request is registered; concurrent late replies need separate coverage.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let next = timeout(Duration::from_secs(1), read_command(&mut stream))
            .await
            .unwrap();
        assert_eq!(next.code, request::QUERY_CONSUMER_OFFSET);
        assert!(!next.is_oneway());
        let mut reply = RemotingCommand {
            code: response::SUCCESS,
            opaque: next.opaque,
            ext_fields: Some([("offset".into(), "42".into())].into()),
            ..RemotingCommand::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let api = MQClientApi::new(client.clone());
    api.update_consumer_offset(
        &addr,
        &Default::default(),
        true,
        Instant::now() + Duration::from_secs(2),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        api.query_consumer_offset(
            &addr,
            &Default::default(),
            Instant::now() + Duration::from_secs(2)
        )
        .await
        .unwrap(),
        Some(42)
    );
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn nameserver_deadline_does_not_fail_over_after_first_peer_stalls() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first = listener.local_addr().unwrap().to_string();
    let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_addr = second.local_addr().unwrap().to_string();
    let stalled = [listener, second].map(|listener| {
        tokio::spawn(async move {
            let Ok(Ok((mut stream, _))) =
                timeout(Duration::from_millis(300), listener.accept()).await
            else {
                return 0;
            };
            let Ok(command) = timeout(Duration::from_millis(300), read_command(&mut stream)).await
            else {
                return 0;
            };
            assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
            tokio::time::sleep(Duration::from_millis(250)).await;
            1
        })
    });
    let client = RemotingClient::new(TransportMode::Plain);
    // Address order is shuffled, so both peers stall; only the selected one may see a request.
    client.update_name_server_address_list(vec![second_addr, first]);
    client.start().unwrap();
    let result = MQClientApi::new(client.clone())
        .get_topic_route("test-topic", Instant::now() + Duration::from_millis(100))
        .await;
    assert!(matches!(result, Err(RpcError::Transport(_))), "{result:?}");
    let mut attempts = 0;
    for peer in stalled {
        attempts += peer.await.unwrap();
    }
    assert_eq!(attempts, 1);
    client.shutdown().await;
}

#[tokio::test]
async fn producer_broker_probe_matches_get_max_offset_request_and_response_validation() {
    let replies = vec![
        (
            response::SUCCESS,
            None,
            Some([("offset".into(), "17".into())].into()),
        ),
        (response::SYSTEM_ERROR, Some("probe rejected".into()), None),
        (response::SUCCESS, None, None),
        (
            response::SUCCESS,
            None,
            Some([("offset".into(), "not-an-offset".into())].into()),
        ),
    ];
    let (addr, peer) = scripted_probe_peer(replies).await;
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let api = MQClientApi::new(client.clone());

    api.probe_broker(
        &addr,
        "probe-topic",
        Instant::now() + Duration::from_secs(2),
    )
    .await
    .unwrap();

    let broker_error = api
        .probe_broker(
            &addr,
            "probe-topic",
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        broker_error,
        RpcError::Response {
            request_code: request::GET_MAX_OFFSET,
            code: response::SYSTEM_ERROR,
            remark: Some(ref remark),
            ..
        } if remark == "probe rejected"
    ));

    let missing_offset = api
        .probe_broker(
            &addr,
            "probe-topic",
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        missing_offset,
        RpcError::MalformedResponse {
            request_code: request::GET_MAX_OFFSET,
            reason,
            ..
        } if reason == "missing max offset"
    ));

    let invalid_offset = api
        .probe_broker(
            &addr,
            "probe-topic",
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        invalid_offset,
        RpcError::MalformedResponse {
            request_code: request::GET_MAX_OFFSET,
            reason,
            ..
        } if reason.starts_with("invalid max offset:")
    ));

    let requests = peer.await.unwrap();
    assert_eq!(requests.len(), 4);
    for command in requests {
        assert_eq!(command.code, request::GET_MAX_OFFSET);
        assert!(!command.is_oneway());
        let fields = command.ext_fields.expect("probe request fields");
        assert_eq!(fields.get("topic").map(String::as_str), Some("probe-topic"));
        assert_eq!(fields.get("queueId").map(String::as_str), Some("0"));
        assert_eq!(fields.get("committed").map(String::as_str), Some("true"));
    }
    client.shutdown().await;
}
