use std::time::Duration;

use bytes::BytesMut;
use rocketmq_classic_core::{
    api::{MQClientApi, consumer::PullStatus},
    error::RpcError,
};
use rocketmq_classic_protocol::{
    body::{LockBatchRequestBody, UnlockBatchRequestBody},
    codes::{request, response},
    header::{
        CommandHeader, ConsumerSendMsgBackRequestHeader, HeaderMap, PullMessageRequestHeader,
        PullMessageResponseHeader, QueryConsumerOffsetRequestHeader,
        QueryConsumerOffsetResponseHeader, UpdateConsumerOffsetRequestHeader,
    },
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RemotingClient, TransportMode},
    codec::frame,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::Instant,
};

async fn scripted(
    code: i32,
    fields: Option<HeaderMap>,
    body: Option<Vec<u8>>,
    expected: i32,
    oneway: bool,
) -> (MQClientApi, RemotingClient, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut len = [0; 4];
        stream.read_exact(&mut len).await.unwrap();
        let mut bytes = BytesMut::from(&len[..]);
        bytes.resize(u32::from_be_bytes(len) as usize + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        let incoming = frame::decode_next(&mut bytes).unwrap().unwrap();
        assert_eq!(incoming.code, expected);
        if !oneway {
            let mut reply = RemotingCommand {
                code,
                ext_fields: fields.map(Into::into),
                body,
                opaque: incoming.opaque,
                remark: Some("broker error".into()),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        }
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    (MQClientApi::new(client.clone()), client, addr)
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

fn fields<H: CommandHeader>(header: &H) -> HeaderMap {
    let mut fields = HeaderMap::new();
    header.encode_fields(&mut fields);
    fields
}

#[tokio::test]
async fn pull_statuses_and_malformed_success() {
    let header = {
        let mut __phase18_header_0 = PullMessageRequestHeader::default();
        __phase18_header_0.set_sys_flag(Some(0));
        __phase18_header_0
    };
    let mut response = PullMessageResponseHeader::default();
    response.set_next_begin_offset(Some(12));
    response.set_min_offset(Some(1));
    response.set_max_offset(Some(30));
    response.set_suggest_which_broker_id(Some(2));
    let response_header = fields(&response);
    for (code, status) in [
        (response::SUCCESS, PullStatus::Found),
        (response::PULL_NOT_FOUND, PullStatus::NoNewMessage),
        (
            response::PULL_RETRY_IMMEDIATELY,
            PullStatus::NoMatchedMessage,
        ),
        (response::PULL_OFFSET_MOVED, PullStatus::OffsetIllegal),
    ] {
        let (api, client, addr) = scripted(
            code,
            Some(response_header.clone()),
            Some(vec![0, 255, 42]),
            request::PULL_MESSAGE,
            false,
        )
        .await;
        let result = api.pull_message(&addr, &header, deadline()).await.unwrap();
        assert_eq!(result.status, status);
        assert_eq!(result.next_begin_offset, 12);
        assert_eq!(result.min_offset, 1);
        assert_eq!(result.max_offset, 30);
        assert_eq!(result.suggest_which_broker_id, 2);
        assert_eq!(result.offset_delta, None);
        assert_eq!(result.raw_messages, Some(vec![0, 255, 42]));
        client.shutdown().await;
    }
    for key in [
        "nextBeginOffset",
        "minOffset",
        "maxOffset",
        "suggestWhichBrokerId",
    ] {
        let mut invalid = response_header.clone();
        invalid.insert(key.into(), "invalid".into());
        let (api, client, addr) = scripted(
            response::SUCCESS,
            Some(invalid),
            None,
            request::PULL_MESSAGE,
            false,
        )
        .await;
        malformed(
            api.pull_message(&addr, &header, deadline()).await,
            request::PULL_MESSAGE,
            &addr,
        );
        client.shutdown().await;
    }
    for key in [
        "nextBeginOffset",
        "minOffset",
        "maxOffset",
        "suggestWhichBrokerId",
    ] {
        let mut missing = response_header.clone();
        missing.remove(key);
        let (api, client, addr) = scripted(
            response::SUCCESS,
            Some(missing),
            None,
            request::PULL_MESSAGE,
            false,
        )
        .await;
        malformed(
            api.pull_message(&addr, &header, deadline()).await,
            request::PULL_MESSAGE,
            &addr,
        );
        client.shutdown().await;
    }
    let (api, client, addr) = scripted(
        response::SUCCESS,
        Some(response_header.clone()),
        None,
        request::PULL_MESSAGE,
        false,
    )
    .await;
    assert_eq!(
        api.pull_message(&addr, &header, deadline())
            .await
            .unwrap()
            .raw_messages,
        None
    );
    client.shutdown().await;
    for (code, fields) in [(response::SUCCESS, None), (9123, None)] {
        let (api, client, addr) = scripted(code, fields, None, request::PULL_MESSAGE, false).await;
        let result = api.pull_message(&addr, &header, deadline()).await;
        if code == response::SUCCESS {
            assert!(matches!(result, Err(RpcError::MalformedResponse { .. })));
        } else {
            assert!(matches!(result, Err(RpcError::Response { code: 9123, .. })));
        }
        client.shutdown().await;
    }
}

#[tokio::test]
async fn offset_lookup_and_update_modes() {
    for code in [response::SUCCESS, response::QUERY_NOT_FOUND, 9123] {
        let (api, client, addr) = scripted(
            code,
            Some(fields(&{
                let mut header = QueryConsumerOffsetResponseHeader::default();
                header.set_offset(Some(7));
                header
            })),
            None,
            request::QUERY_CONSUMER_OFFSET,
            false,
        )
        .await;
        let result = api
            .query_consumer_offset(
                &addr,
                &QueryConsumerOffsetRequestHeader::default(),
                deadline(),
            )
            .await;
        match code {
            response::SUCCESS => assert_eq!(result.unwrap(), Some(7)),
            response::QUERY_NOT_FOUND => assert_eq!(result.unwrap(), None),
            _ => assert!(matches!(result, Err(RpcError::Response { .. }))),
        }
        client.shutdown().await;
    }
    let (api, client, addr) = scripted(
        response::SUCCESS,
        None,
        None,
        request::QUERY_CONSUMER_OFFSET,
        false,
    )
    .await;
    assert!(matches!(
        api.query_consumer_offset(
            &addr,
            &QueryConsumerOffsetRequestHeader::default(),
            deadline()
        )
        .await,
        Err(RpcError::MalformedResponse { .. })
    ));
    client.shutdown().await;
    for oneway in [false, true] {
        let (api, client, addr) = scripted(
            response::SUCCESS,
            None,
            None,
            request::UPDATE_CONSUMER_OFFSET,
            oneway,
        )
        .await;
        api.update_consumer_offset(
            &addr,
            &UpdateConsumerOffsetRequestHeader::default(),
            oneway,
            deadline(),
        )
        .await
        .unwrap();
        client.shutdown().await;
    }
}

#[tokio::test]
async fn list_send_back_lock_and_unlock() {
    let (api, client, addr) = scripted(
        response::SUCCESS,
        None,
        Some(br#"{"lockOKMQSet":[{"topic":"orders","brokerName":"broker","queueId":2}]}"#.to_vec()),
        request::LOCK_BATCH_MQ,
        false,
    )
    .await;
    let locked = api
        .lock_batch_mq(&addr, &LockBatchRequestBody::default(), deadline())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(locked.len(), 1);
    let locked_queue = locked.iter().next().unwrap().as_ref().unwrap();
    assert_eq!(locked_queue.topic(), Some("orders"));
    assert_eq!(locked_queue.broker_name(), Some("broker"));
    assert_eq!(locked_queue.queue_id(), 2);
    client.shutdown().await;
    let (api, client, addr) = scripted(
        response::SUCCESS,
        None,
        Some(br#"{"consumerIdList":["a","b"]}"#.to_vec()),
        request::GET_CONSUMER_LIST_BY_GROUP,
        false,
    )
    .await;
    assert_eq!(
        api.get_consumer_list_by_group(&addr, "workers", deadline())
            .await
            .unwrap(),
        Some(vec![Some("a".into()), Some("b".into())])
    );
    client.shutdown().await;
    let (api, client, addr) = scripted(
        response::SUCCESS,
        None,
        Some(br#"{"consumerIdList":[null,1]}"#.to_vec()),
        request::GET_CONSUMER_LIST_BY_GROUP,
        false,
    )
    .await;
    assert_eq!(
        api.get_consumer_list_by_group(&addr, "workers", deadline())
            .await
            .unwrap(),
        Some(vec![None, Some("1".into())])
    );
    client.shutdown().await;
    let (api, client, addr) = scripted(
        response::SUCCESS,
        None,
        None,
        request::GET_CONSUMER_LIST_BY_GROUP,
        false,
    )
    .await;
    assert!(matches!(
        api.get_consumer_list_by_group(&addr, "workers", deadline())
            .await,
        Err(RpcError::Response {
            code: response::SUCCESS,
            ..
        })
    ));
    client.shutdown().await;
    let (api, client, addr) =
        scripted(9123, None, None, request::CONSUMER_SEND_MSG_BACK, false).await;
    assert!(matches!(
        api.consumer_send_message_back(
            &addr,
            &ConsumerSendMsgBackRequestHeader::default(),
            deadline()
        )
        .await,
        Err(RpcError::Response {
            request_code: request::CONSUMER_SEND_MSG_BACK,
            ..
        })
    ));
    client.shutdown().await;
    let (api, client, addr) = scripted(
        response::SUCCESS,
        None,
        Some(br#"{"lockOKMQSet":[]}"#.to_vec()),
        request::LOCK_BATCH_MQ,
        false,
    )
    .await;
    assert!(
        api.lock_batch_mq(&addr, &LockBatchRequestBody::default(), deadline())
            .await
            .unwrap()
            .unwrap()
            .is_empty()
    );
    client.shutdown().await;
    let (api, client, addr) =
        scripted(response::SUCCESS, None, None, request::LOCK_BATCH_MQ, false).await;
    assert!(matches!(
        api.lock_batch_mq(&addr, &LockBatchRequestBody::default(), deadline())
            .await,
        Err(RpcError::MalformedResponse { .. })
    ));
    client.shutdown().await;
    let (api, client, addr) = scripted(9123, None, None, request::LOCK_BATCH_MQ, false).await;
    assert!(matches!(
        api.lock_batch_mq(&addr, &LockBatchRequestBody::default(), deadline())
            .await,
        Err(RpcError::Response { code: 9123, .. })
    ));
    client.shutdown().await;
    for oneway in [true, false] {
        let (api, client, addr) = scripted(
            response::SUCCESS,
            None,
            None,
            request::UNLOCK_BATCH_MQ,
            oneway,
        )
        .await;
        api.unlock_batch_mq(
            &addr,
            &UnlockBatchRequestBody::default(),
            oneway,
            deadline(),
        )
        .await
        .unwrap();
        client.shutdown().await;
    }
}

#[tokio::test]
async fn consumer_list_reply_uses_pinned_java_fastjson_eof_rules() {
    for (body, use_future) in [
        ("{\"consumerIdList\":[\"before\"]}\u{001a}trailing", false),
        (
            "{\"consumerIdList\":[\"before\"]}\u{001a}{\"consumerIdList\":[\"after\"]}",
            true,
        ),
    ] {
        let (api, client, addr) = scripted(
            response::SUCCESS,
            None,
            Some(body.as_bytes().to_vec()),
            request::GET_CONSUMER_LIST_BY_GROUP,
            false,
        )
        .await;
        let result = if use_future {
            api.get_consumer_list_by_group_future(&addr, Some("workers"), deadline())
                .await
        } else {
            api.get_consumer_list_by_group(&addr, "workers", deadline())
                .await
        };
        assert_eq!(result.unwrap(), Some(vec![Some("before".into())]));
        client.shutdown().await;
    }
}

fn rejected(
    result: Result<impl std::fmt::Debug, RpcError>,
    request_code: i32,
    addr: &str,
    code: i32,
) {
    match result {
        Err(RpcError::Response {
            request_code: actual,
            peer,
            code: actual_code,
            remark,
        }) => {
            assert_eq!(actual, request_code);
            assert_eq!(peer, addr);
            assert_eq!(actual_code, code);
            assert_eq!(remark.as_deref(), Some("broker error"));
        }
        other => panic!("expected broker rejection: {other:?}"),
    }
}

fn malformed(result: Result<impl std::fmt::Debug, RpcError>, request_code: i32, addr: &str) {
    match result {
        Err(RpcError::MalformedResponse {
            request_code: actual,
            peer,
            reason,
        }) => {
            assert_eq!(actual, request_code);
            assert_eq!(peer, addr);
            assert!(!reason.is_empty());
        }
        other => panic!("expected malformed response: {other:?}"),
    }
}

#[tokio::test]
async fn every_consumer_rpc_rejects_unexpected_status_before_decoding() {
    for code in [response::QUERY_NOT_FOUND, 9123] {
        let (api, client, addr) = scripted(
            code,
            None,
            Some(b"invalid json".to_vec()),
            request::PULL_MESSAGE,
            false,
        )
        .await;
        rejected(
            api.pull_message(
                &addr,
                &{
                    let mut __phase18_header_1 = PullMessageRequestHeader::default();
                    __phase18_header_1.set_sys_flag(Some(0));
                    __phase18_header_1
                },
                deadline(),
            )
            .await,
            request::PULL_MESSAGE,
            &addr,
            code,
        );
        client.shutdown().await;

        let (api, client, addr) =
            scripted(code, None, None, request::QUERY_CONSUMER_OFFSET, false).await;
        let query_result = api
            .query_consumer_offset(
                &addr,
                &QueryConsumerOffsetRequestHeader::default(),
                deadline(),
            )
            .await;
        if code == response::QUERY_NOT_FOUND {
            assert_eq!(query_result.unwrap(), None);
        } else {
            rejected(query_result, request::QUERY_CONSUMER_OFFSET, &addr, code);
        }
        client.shutdown().await;

        let (api, client, addr) =
            scripted(code, None, None, request::UPDATE_CONSUMER_OFFSET, false).await;
        rejected(
            api.update_consumer_offset(
                &addr,
                &UpdateConsumerOffsetRequestHeader::default(),
                false,
                deadline(),
            )
            .await,
            request::UPDATE_CONSUMER_OFFSET,
            &addr,
            code,
        );
        client.shutdown().await;

        let (api, client, addr) = scripted(
            code,
            None,
            Some(b"invalid json".to_vec()),
            request::GET_CONSUMER_LIST_BY_GROUP,
            false,
        )
        .await;
        rejected(
            api.get_consumer_list_by_group(&addr, "workers", deadline())
                .await,
            request::GET_CONSUMER_LIST_BY_GROUP,
            &addr,
            code,
        );
        client.shutdown().await;

        let (api, client, addr) =
            scripted(code, None, None, request::CONSUMER_SEND_MSG_BACK, false).await;
        rejected(
            api.consumer_send_message_back(
                &addr,
                &ConsumerSendMsgBackRequestHeader::default(),
                deadline(),
            )
            .await,
            request::CONSUMER_SEND_MSG_BACK,
            &addr,
            code,
        );
        client.shutdown().await;

        let (api, client, addr) = scripted(
            code,
            None,
            Some(b"invalid json".to_vec()),
            request::LOCK_BATCH_MQ,
            false,
        )
        .await;
        rejected(
            api.lock_batch_mq(&addr, &LockBatchRequestBody::default(), deadline())
                .await,
            request::LOCK_BATCH_MQ,
            &addr,
            code,
        );
        client.shutdown().await;

        let (api, client, addr) = scripted(code, None, None, request::UNLOCK_BATCH_MQ, false).await;
        rejected(
            api.unlock_batch_mq(&addr, &UnlockBatchRequestBody::default(), false, deadline())
                .await,
            request::UNLOCK_BATCH_MQ,
            &addr,
            code,
        );
        client.shutdown().await;
    }
}

#[tokio::test]
async fn successful_consumer_responses_decode_only_required_payloads() {
    for fields in [
        None,
        Some(HeaderMap::from([("offset".into(), "bad".into())])),
    ] {
        let (api, client, addr) = scripted(
            response::SUCCESS,
            fields,
            None,
            request::QUERY_CONSUMER_OFFSET,
            false,
        )
        .await;
        malformed(
            api.query_consumer_offset(
                &addr,
                &QueryConsumerOffsetRequestHeader::default(),
                deadline(),
            )
            .await,
            request::QUERY_CONSUMER_OFFSET,
            &addr,
        );
        client.shutdown().await;
    }
    for body in [b"invalid json".to_vec(), b"null".to_vec()] {
        let (api, client, addr) = scripted(
            response::SUCCESS,
            None,
            Some(body.clone()),
            request::GET_CONSUMER_LIST_BY_GROUP,
            false,
        )
        .await;
        malformed(
            api.get_consumer_list_by_group(&addr, "workers", deadline())
                .await,
            request::GET_CONSUMER_LIST_BY_GROUP,
            &addr,
        );
        client.shutdown().await;

        let (api, client, addr) = scripted(
            response::SUCCESS,
            None,
            Some(body),
            request::LOCK_BATCH_MQ,
            false,
        )
        .await;
        malformed(
            api.lock_batch_mq(&addr, &LockBatchRequestBody::default(), deadline())
                .await,
            request::LOCK_BATCH_MQ,
            &addr,
        );
        client.shutdown().await;
    }
    for body in [br#"{}"#.to_vec(), br#"{"consumerIdList":[]}"#.to_vec()] {
        let (api, client, addr) = scripted(
            response::SUCCESS,
            None,
            Some(body.clone()),
            request::GET_CONSUMER_LIST_BY_GROUP,
            false,
        )
        .await;
        let expected = if body == b"{}" { None } else { Some(vec![]) };
        assert_eq!(
            api.get_consumer_list_by_group(&addr, "workers", deadline())
                .await
                .unwrap(),
            expected
        );
        client.shutdown().await;
    }
    let (api, client, addr) = scripted(
        response::SUCCESS,
        None,
        None,
        request::CONSUMER_SEND_MSG_BACK,
        false,
    )
    .await;
    api.consumer_send_message_back(
        &addr,
        &ConsumerSendMsgBackRequestHeader::default(),
        deadline(),
    )
    .await
    .unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn consumer_void_success_has_no_malformed_response_payload() {
    // Update, send-back and unlock decode no success body or headers; malformed
    // success is meaningless for these methods, even when the peer sends junk.
    let junk = Some(b"invalid json".to_vec());
    let fields = Some(HeaderMap::from([("offset".into(), "invalid".into())]));
    let (api, client, addr) = scripted(
        response::SUCCESS,
        fields.clone(),
        junk.clone(),
        request::UPDATE_CONSUMER_OFFSET,
        false,
    )
    .await;
    api.update_consumer_offset(&addr, &Default::default(), false, deadline())
        .await
        .unwrap();
    client.shutdown().await;

    let (api, client, addr) = scripted(
        response::SUCCESS,
        fields.clone(),
        junk.clone(),
        request::CONSUMER_SEND_MSG_BACK,
        false,
    )
    .await;
    api.consumer_send_message_back(&addr, &Default::default(), deadline())
        .await
        .unwrap();
    client.shutdown().await;

    let (api, client, addr) = scripted(
        response::SUCCESS,
        fields,
        junk,
        request::UNLOCK_BATCH_MQ,
        false,
    )
    .await;
    api.unlock_batch_mq(&addr, &Default::default(), false, deadline())
        .await
        .unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn consumer_list_missing_success_body_is_domain_error() {
    let (api, client, addr) = scripted(
        response::SUCCESS,
        None,
        None,
        request::GET_CONSUMER_LIST_BY_GROUP,
        false,
    )
    .await;
    rejected(
        api.get_consumer_list_by_group(&addr, "workers", deadline())
            .await,
        request::GET_CONSUMER_LIST_BY_GROUP,
        &addr,
        response::SUCCESS,
    );
    client.shutdown().await;
}
