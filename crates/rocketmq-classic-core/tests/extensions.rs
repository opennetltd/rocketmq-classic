use std::time::Duration;

use bytes::BytesMut;
use rocketmq_classic_core::{
    api::{
        MQClientApi,
        extensions::{AckStatus, PopStatus},
    },
    error::RpcError,
};
use rocketmq_classic_protocol::{
    body::{
        BatchAckMessageRequestBody, LiteSubscriptionCtlRequestBody, QueryAssignmentRequestBody,
        SetMessageRequestModeRequestBody,
    },
    codes::{request, response},
    header::{
        ChangeInvisibleTimeRequestHeader, PopLiteMessageRequestHeader, PopMessageRequestHeader,
        ProtocolError, UnregisterClientRequestHeader,
    },
    heartbeat::HeartbeatData,
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

async fn peer(
    code: i32,
    status: i32,
    body: Option<Vec<u8>>,
    fields: Option<std::collections::BTreeMap<String, String>>,
) -> (
    MQClientApi,
    RemotingClient,
    String,
    tokio::task::JoinHandle<RemotingCommand>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut len = [0; 4];
        stream.read_exact(&mut len).await.unwrap();
        let mut bytes = BytesMut::from(&len[..]);
        bytes.resize(u32::from_be_bytes(len) as usize + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        let incoming = frame::decode_next(&mut bytes).unwrap().unwrap();
        assert_eq!(incoming.code, code);
        let mut reply = RemotingCommand {
            code: status,
            opaque: incoming.opaque,
            version: 515,
            body,
            ext_fields: fields.map(Into::into),
            remark: Some("rejected".into()),
            ..RemotingCommand::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
        incoming
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    (MQClientApi::new(client.clone()), client, addr, task)
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

fn valid_change_invisible_header() -> ChangeInvisibleTimeRequestHeader {
    {
        let mut __phase18_header_0 = ChangeInvisibleTimeRequestHeader::default();
        __phase18_header_0.set_consumer_group(Some("consumer".into()));
        __phase18_header_0.set_topic(Some("oracle-topic".into()));
        __phase18_header_0.set_queue_id(Some(2));
        __phase18_header_0.set_extra_info(Some("checkpoint".into()));
        __phase18_header_0.set_offset(Some(17));
        __phase18_header_0.set_invisible_time(Some(456));
        __phase18_header_0
    }
}

#[tokio::test]
async fn heartbeat_versions_and_unregister_header() {
    let (api, client, addr, task) = peer(
        request::HEART_BEAT,
        response::SUCCESS,
        None,
        Some(
            [
                ("IS_SUB_CHANGE".into(), "true".into()),
                ("IS_SUPPORT_HEART_BEAT_V2".into(), "true".into()),
            ]
            .into(),
        ),
    )
    .await;
    let result = api
        .send_heartbeat_v2(
            &addr,
            &HeartbeatData {
                client_id: Some("client".into()),
                ..Default::default()
            },
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(result.version, 515);
    assert!(result.is_sub_change && result.is_support_heartbeat_v2);
    let incoming = task.await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&incoming.body.unwrap()).unwrap()["clientID"],
        "client"
    );
    client.shutdown().await;

    let (api, client, addr, task) = peer(
        request::UNREGISTER_CLIENT,
        response::SYSTEM_ERROR,
        None,
        None,
    )
    .await;
    let err = api
        .unregister_client(
            &addr,
            &{
                let mut __phase18_header_1 = UnregisterClientRequestHeader::default();
                __phase18_header_1.set_client_id("c".into());
                __phase18_header_1.set_producer_group(None);
                __phase18_header_1.set_consumer_group(Some("g".into()));
                __phase18_header_1
            },
            deadline(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        RpcError::Response {
            code: response::SYSTEM_ERROR,
            request_code: request::UNREGISTER_CLIENT,
            ..
        }
    ));
    assert_eq!(
        task.await.unwrap().ext_fields.unwrap()["consumerGroup"],
        "g"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn assignment_and_request_mode_shapes() {
    for (status, body) in [
        (
            response::SUCCESS,
            Some(br#"{"messageQueueAssignments":[]}"#.to_vec()),
        ),
        (response::SUCCESS, None),
        (999, None),
    ] {
        let (api, client, addr, task) =
            peer(request::QUERY_ASSIGNMENT, status, body.clone(), None).await;
        let mut request = QueryAssignmentRequestBody::default();
        request.set_topic(Some("orders".into()));
        let result = api.query_assignment(&addr, &request, deadline()).await;
        match (status, body) {
            (response::SUCCESS, Some(_)) => {
                assert_eq!(
                    result
                        .unwrap()
                        .get_message_queue_assignments()
                        .unwrap()
                        .snapshot()
                        .len(),
                    0
                )
            }
            (response::SUCCESS, None) => {
                assert!(matches!(result, Err(RpcError::MalformedResponse { .. })))
            }
            _ => assert!(matches!(result, Err(RpcError::Response { code: 999, .. }))),
        }
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&task.await.unwrap().body.unwrap())
                .unwrap()["topic"],
            "orders"
        );
        client.shutdown().await;
    }
    let (api, client, addr, task) = peer(
        request::SET_MESSAGE_REQUEST_MODE,
        response::SUCCESS,
        None,
        None,
    )
    .await;
    api.set_message_request_mode(
        &addr,
        &SetMessageRequestModeRequestBody::default(),
        deadline(),
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&task.await.unwrap().body.unwrap()).unwrap()["mode"],
        "PULL"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn pop_status_and_ack_semantics() {
    for (status, expected) in [
        (response::PULL_NOT_FOUND, Some(PopStatus::PollingNotFound)),
        (response::POLLING_TIMEOUT, Some(PopStatus::PollingNotFound)),
        (response::POLLING_FULL, Some(PopStatus::PollingFull)),
        (999, None),
    ] {
        let fields = (status != 999).then(|| {
            [
                ("popTime".into(), "1".into()),
                ("invisibleTime".into(), "2".into()),
                ("reviveQid".into(), "0".into()),
                ("restNum".into(), "3".into()),
            ]
            .into()
        });
        let (api, client, addr, task) = peer(request::POP_MESSAGE, status, None, fields).await;
        let result = api
            .pop_message(&addr, &PopMessageRequestHeader::default(), deadline())
            .await;
        match expected {
            Some(expected) => assert_eq!(result.unwrap().status, expected),
            None => assert!(matches!(result, Err(RpcError::Response { code: 999, .. }))),
        }
        task.await.unwrap();
        client.shutdown().await;
    }
    for status in [response::SUCCESS, 999] {
        let (api, client, addr, task) = peer(request::ACK_MESSAGE, status, None, None).await;
        let result = api
            .ack_message(&addr, &Default::default(), deadline())
            .await
            .unwrap();
        assert_eq!(
            result,
            if status == response::SUCCESS {
                AckStatus::Ok
            } else {
                AckStatus::NoExist
            }
        );
        task.await.unwrap();
        client.shutdown().await;
    }
}

#[tokio::test]
async fn change_invisible_time_returns_java_ack_metadata() {
    for (status, fields, expected) in [
        (
            response::SUCCESS,
            Some(
                [
                    ("popTime".into(), "12".into()),
                    ("invisibleTime".into(), "30".into()),
                    ("reviveQid".into(), "2".into()),
                ]
                .into(),
            ),
            (Some(12), Some(30), Some(2)),
        ),
        (response::SUCCESS, None, (Some(0), Some(0), Some(0))),
        (
            999,
            Some(
                [
                    ("popTime".into(), "12".into()),
                    ("invisibleTime".into(), "30".into()),
                    ("reviveQid".into(), "2".into()),
                ]
                .into(),
            ),
            (None, None, None),
        ),
    ] {
        let (api, client, addr, task) =
            peer(request::CHANGE_MESSAGE_INVISIBLETIME, status, None, fields).await;
        let result = api
            .change_invisible_time(
                &addr,
                "broker",
                &valid_change_invisible_header(),
                deadline(),
            )
            .await;
        match status {
            response::SUCCESS => {
                let result = result.unwrap();
                assert_eq!(result.status, AckStatus::Ok);
                assert_eq!(
                    (result.pop_time, result.invisible_time, result.revive_qid),
                    expected
                );
                let (pop_time, invisible_time, revive_qid) = expected;
                assert_eq!(
                    result.extra_info.as_deref(),
                    Some(
                        format!(
                            "17 {} {} {} 0 broker 2 17",
                            pop_time.unwrap(),
                            invisible_time.unwrap(),
                            revive_qid.unwrap()
                        )
                        .as_str()
                    )
                );
            }
            _ => {
                let result = result.unwrap();
                assert_eq!(result.status, AckStatus::NoExist);
                assert_eq!(
                    (result.pop_time, result.invisible_time, result.revive_qid),
                    expected
                );
            }
        }
        task.await.unwrap();
        client.shutdown().await;
    }
}

#[tokio::test]
async fn change_invisible_time_success_requires_request_fields_used_by_java_callback() {
    for missing_field in ["offset", "topic", "queueId"] {
        let (api, client, addr, task) = peer(
            request::CHANGE_MESSAGE_INVISIBLETIME,
            response::SUCCESS,
            None,
            Some(
                [
                    ("popTime".into(), "12".into()),
                    ("invisibleTime".into(), "30".into()),
                    ("reviveQid".into(), "2".into()),
                ]
                .into(),
            ),
        )
        .await;
        let mut header = valid_change_invisible_header();
        match missing_field {
            "offset" => header.set_offset(None),
            "topic" => header.set_topic(None),
            "queueId" => header.set_queue_id(None),
            _ => unreachable!(),
        }

        let result = api
            .change_invisible_time(&addr, "broker", &header, deadline())
            .await;
        assert!(matches!(
            result,
            Err(RpcError::Header(ProtocolError::MissingField(field))) if field == missing_field
        ));
        task.await.unwrap();
        client.shutdown().await;
    }
}

#[tokio::test]
async fn synchronous_extension_status_matrix() {
    for status in [response::SUCCESS, response::SYSTEM_ERROR, 999] {
        macro_rules! check {
            ($code:expr, $method:ident, $data:expr) => {{
                let (api, client, addr, task) = peer($code, status, None, None).await;
                let result = api.$method(&addr, &$data, deadline()).await;
                if status == response::SUCCESS {
                    if $code == request::QUERY_ASSIGNMENT {
                        assert!(matches!(result, Err(RpcError::MalformedResponse { .. })));
                    } else {
                        assert!(result.is_ok(), "code {}: {result:?}", $code);
                    }
                } else {
                    assert!(matches!(result, Err(RpcError::Response { request_code, code, peer, remark })
                        if request_code == $code && code == status && peer == addr && remark.as_deref() == Some("rejected")));
                }
                task.await.unwrap();
                client.shutdown().await;
            }};
        }
        check!(
            request::HEART_BEAT,
            send_heartbeat,
            HeartbeatData::default()
        );
        check!(
            request::HEART_BEAT,
            send_heartbeat_v2,
            HeartbeatData::default()
        );
        check!(
            request::UNREGISTER_CLIENT,
            unregister_client,
            UnregisterClientRequestHeader::default()
        );
        check!(
            request::QUERY_ASSIGNMENT,
            query_assignment,
            QueryAssignmentRequestBody::default()
        );
        check!(
            request::SET_MESSAGE_REQUEST_MODE,
            set_message_request_mode,
            SetMessageRequestModeRequestBody::default()
        );
        check!(
            request::LITE_SUBSCRIPTION_CTL,
            lite_subscription_ctl,
            LiteSubscriptionCtlRequestBody::default()
        );
    }
}

#[tokio::test]
async fn async_extension_status_and_malformed_matrix() {
    for status in [
        response::SUCCESS,
        response::PULL_NOT_FOUND,
        response::POLLING_TIMEOUT,
        response::POLLING_FULL,
        999,
    ] {
        for lite in [false, true] {
            let code = if lite {
                request::POP_LITE_MESSAGE
            } else {
                request::POP_MESSAGE
            };
            let fields = Some(
                [
                    ("popTime".into(), "1".into()),
                    ("invisibleTime".into(), "2".into()),
                    ("reviveQid".into(), "0".into()),
                    ("restNum".into(), "3".into()),
                ]
                .into(),
            );
            let (api, client, addr, task) = peer(code, status, Some(vec![1]), fields).await;
            let result = if lite {
                api.pop_lite_message(&addr, &PopLiteMessageRequestHeader::default(), deadline())
                    .await
                    .map(|r| r.status)
            } else {
                api.pop_message(&addr, &PopMessageRequestHeader::default(), deadline())
                    .await
                    .map(|r| r.status)
            };
            let expected = match status {
                response::SUCCESS => PopStatus::Found,
                response::POLLING_FULL => PopStatus::PollingFull,
                _ => PopStatus::PollingNotFound,
            };
            if status == 999 {
                assert!(matches!(result, Err(RpcError::Response { code: 999, .. })));
            } else {
                assert_eq!(result.unwrap(), expected);
            }
            task.await.unwrap();
            client.shutdown().await;
        }
    }
    for code in [request::POP_MESSAGE, request::POP_LITE_MESSAGE] {
        for (status, body, fields) in [
            (response::SUCCESS, None, None),
            (response::SUCCESS, Some(vec![1]), None),
            (
                response::POLLING_TIMEOUT,
                None,
                Some([("popTime".into(), "invalid".into())].into()),
            ),
        ] {
            let missing_body = body.is_none();
            let (api, client, addr, task) = peer(code, status, body, fields).await;
            let result = if code == request::POP_MESSAGE {
                api.pop_message(&addr, &Default::default(), deadline())
                    .await
                    .map(|_| ())
            } else {
                api.pop_lite_message(&addr, &Default::default(), deadline())
                    .await
                    .map(|_| ())
            };
            if status == response::SUCCESS && missing_body {
                assert!(
                    matches!(result, Err(RpcError::MalformedResponse { request_code, .. }) if request_code == code)
                );
            } else {
                result.unwrap();
            }
            task.await.unwrap();
            client.shutdown().await;
        }
    }
    for status in [response::SUCCESS, response::SYSTEM_ERROR, 999] {
        for batch in [false, true] {
            let code = if batch {
                request::BATCH_ACK_MESSAGE
            } else {
                request::ACK_MESSAGE
            };
            let (api, client, addr, task) = peer(code, status, None, None).await;
            let result = if batch {
                api.batch_ack_message(&addr, &BatchAckMessageRequestBody::default(), deadline())
                    .await
            } else {
                api.ack_message(&addr, &Default::default(), deadline())
                    .await
            };
            assert_eq!(
                result.unwrap(),
                if status == response::SUCCESS {
                    AckStatus::Ok
                } else {
                    AckStatus::NoExist
                }
            );
            assert_eq!(task.await.unwrap().body.is_some(), batch);
            client.shutdown().await;
        }
    }
    for key in ["popTime", "invisibleTime", "reviveQid"] {
        for value in [
            None,
            Some("invalid"),
            Some("99999999999999999999999999999999999"),
        ] {
            let mut fields: std::collections::BTreeMap<String, String> = [
                ("popTime".into(), "12".into()),
                ("invisibleTime".into(), "30".into()),
                ("reviveQid".into(), "2".into()),
            ]
            .into();
            if let Some(value) = value {
                fields.insert(key.into(), value.into());
            } else {
                fields.remove(key);
            }
            let (api, client, addr, task) = peer(
                request::CHANGE_MESSAGE_INVISIBLETIME,
                response::SUCCESS,
                None,
                Some(fields),
            )
            .await;
            let result = api
                .change_invisible_time(
                    &addr,
                    "broker",
                    &valid_change_invisible_header(),
                    deadline(),
                )
                .await;
            let result = result.unwrap();
            assert_eq!(result.status, AckStatus::Ok);
            assert_eq!(result.pop_time, Some(if key == "popTime" { 0 } else { 12 }));
            assert_eq!(
                result.invisible_time,
                Some(if key == "invisibleTime" { 0 } else { 30 })
            );
            assert_eq!(
                result.revive_qid,
                Some(if key == "reviveQid" { 0 } else { 2 })
            );
            task.await.unwrap();
            client.shutdown().await;
        }
    }
}

#[tokio::test]
async fn assignment_invalid_json_and_heartbeat_v2_default_flags() {
    for body in [Some(b"not json".to_vec()), Some(b"{}".to_vec())] {
        let (api, client, addr, task) =
            peer(request::QUERY_ASSIGNMENT, response::SUCCESS, body, None).await;
        let result = api
            .query_assignment(&addr, &Default::default(), deadline())
            .await;
        match result {
            Err(RpcError::MalformedResponse { request_code, .. }) => {
                assert_eq!(request_code, request::QUERY_ASSIGNMENT)
            }
            Ok(body) => assert!(body.get_message_queue_assignments().is_none()),
            other => panic!("unexpected assignment reply: {other:?}"),
        }
        task.await.unwrap();
        client.shutdown().await;
    }
    for fields in [
        None,
        Some(
            [
                ("IS_SUB_CHANGE".into(), "not true".into()),
                ("IS_SUPPORT_HEART_BEAT_V2".into(), "FALSE".into()),
            ]
            .into(),
        ),
    ] {
        let (api, client, addr, task) =
            peer(request::HEART_BEAT, response::SUCCESS, None, fields).await;
        let result = api
            .send_heartbeat_v2(&addr, &Default::default(), deadline())
            .await
            .unwrap();
        assert_eq!(result.version, 515);
        assert!(!result.is_sub_change && !result.is_support_heartbeat_v2);
        task.await.unwrap();
        client.shutdown().await;
    }
}

#[tokio::test]
async fn populated_assignment_and_pop_replies_preserve_domain_data() {
    let (api, client, addr, task) = peer(
        request::QUERY_ASSIGNMENT,
        response::SUCCESS,
        Some(br#"{"messageQueueAssignments":[{"messageQueue":{"topic":"orders","brokerName":"broker","queueId":2},"mode":"PULL","attachments":{"key":"value"}}]}"#.to_vec()),
        None,
    ).await;
    let result = api
        .query_assignment(&addr, &Default::default(), deadline())
        .await
        .unwrap();
    let assignments = result
        .get_message_queue_assignments()
        .unwrap()
        .snapshot()
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(assignments.len(), 1);
    let assignment = assignments.first().unwrap().as_ref().unwrap().snapshot();
    let queue = assignment.message_queue().unwrap().snapshot();
    assert_eq!(
        (queue.topic(), queue.broker_name(), queue.queue_id()),
        (Some("orders"), Some("broker"), 2)
    );
    assert_eq!(
        assignment
            .attachments()
            .unwrap()
            .snapshot()
            .get(&Some("key".into()))
            .cloned(),
        Some(Some("value".into()))
    );
    task.await.unwrap();
    client.shutdown().await;

    let fields = Some(
        [
            ("popTime".into(), "1".into()),
            ("invisibleTime".into(), "2".into()),
            ("reviveQid".into(), "0".into()),
            ("restNum".into(), "3".into()),
        ]
        .into(),
    );
    for code in [request::POP_MESSAGE, request::POP_LITE_MESSAGE] {
        for (status, expected) in [
            (response::SUCCESS, PopStatus::Found),
            (response::PULL_NOT_FOUND, PopStatus::PollingNotFound),
            (response::POLLING_TIMEOUT, PopStatus::PollingNotFound),
            (response::POLLING_FULL, PopStatus::PollingFull),
        ] {
            let (api, client, addr, task) =
                peer(code, status, Some(vec![0, 255, 42]), fields.clone()).await;
            if code == request::POP_MESSAGE {
                let result = api
                    .pop_message(&addr, &Default::default(), deadline())
                    .await
                    .unwrap();
                assert_eq!(result.status, expected);
                assert_eq!(result.raw_messages, Some(vec![0, 255, 42]));
                let header = result.header.unwrap();
                assert_eq!(
                    (
                        header.get_pop_time(),
                        header.get_invisible_time(),
                        header.get_revive_qid(),
                        header.get_rest_num()
                    ),
                    (1, 2, 0, 3)
                );
            } else {
                let result = api
                    .pop_lite_message(&addr, &Default::default(), deadline())
                    .await
                    .unwrap();
                assert_eq!(result.status, expected);
                assert_eq!(result.raw_messages, Some(vec![0, 255, 42]));
                let header = result.header.unwrap();
                assert_eq!(
                    (
                        header.get_pop_time(),
                        header.get_invisible_time(),
                        header.get_revive_qid()
                    ),
                    (1, 2, 0)
                );
            }
            task.await.unwrap();
            client.shutdown().await;
        }
    }
}

#[tokio::test]
async fn void_extension_success_ignores_unread_payload() {
    // These methods consume only the response status. There is no decoded success
    // payload to make malformed; junk body/fields must not change a success.
    let junk = Some(b"not json".to_vec());
    let fields = Some([("unexpected".into(), "not a number".into())].into());
    let (api, client, addr, task) = peer(
        request::UNREGISTER_CLIENT,
        response::SUCCESS,
        junk.clone(),
        fields.clone(),
    )
    .await;
    api.unregister_client(&addr, &Default::default(), deadline())
        .await
        .unwrap();
    task.await.unwrap();
    client.shutdown().await;

    let (api, client, addr, task) = peer(
        request::SET_MESSAGE_REQUEST_MODE,
        response::SUCCESS,
        junk.clone(),
        fields.clone(),
    )
    .await;
    api.set_message_request_mode(&addr, &Default::default(), deadline())
        .await
        .unwrap();
    task.await.unwrap();
    client.shutdown().await;

    let (api, client, addr, task) = peer(
        request::LITE_SUBSCRIPTION_CTL,
        response::SUCCESS,
        junk,
        fields,
    )
    .await;
    api.lite_subscription_ctl(&addr, &Default::default(), deadline())
        .await
        .unwrap();
    task.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn heartbeat_and_ack_success_have_no_malformed_payload() {
    // Heartbeat reads only the wire version; V2 extension fields are optional and invalid
    // flags default false. Ack and batch ack only inspect the status code.
    for v2 in [false, true] {
        let (api, client, addr, task) = peer(
            request::HEART_BEAT,
            response::SUCCESS,
            Some(b"invalid json".to_vec()),
            Some([("IS_SUB_CHANGE".into(), "invalid".into())].into()),
        )
        .await;
        if v2 {
            let result = api
                .send_heartbeat_v2(&addr, &Default::default(), deadline())
                .await
                .unwrap();
            assert_eq!(result.version, 515);
            assert!(!result.is_sub_change && !result.is_support_heartbeat_v2);
        } else {
            assert_eq!(
                api.send_heartbeat(&addr, &Default::default(), deadline())
                    .await
                    .unwrap(),
                515
            );
        }
        task.await.unwrap();
        client.shutdown().await;
    }
    for batch in [false, true] {
        let code = if batch {
            request::BATCH_ACK_MESSAGE
        } else {
            request::ACK_MESSAGE
        };
        for status in [response::SUCCESS, response::SYSTEM_ERROR, 999] {
            let (api, client, addr, task) =
                peer(code, status, Some(b"invalid json".to_vec()), None).await;
            let result = if batch {
                api.batch_ack_message(&addr, &Default::default(), deadline())
                    .await
            } else {
                api.ack_message(&addr, &Default::default(), deadline())
                    .await
            };
            assert_eq!(
                result.unwrap(),
                if status == response::SUCCESS {
                    AckStatus::Ok
                } else {
                    AckStatus::NoExist
                }
            );
            task.await.unwrap();
            client.shutdown().await;
        }
    }
}

#[tokio::test]
async fn pop_header_fields_default_missing_invalid_and_out_of_range_values() {
    for code in [request::POP_MESSAGE, request::POP_LITE_MESSAGE] {
        let keys: &[&str] = if code == request::POP_MESSAGE {
            &["popTime", "invisibleTime", "reviveQid", "restNum"]
        } else {
            &["popTime", "invisibleTime", "reviveQid"]
        };
        for &key in keys {
            for value in [None, Some("NaN"), Some("999999999999999999999999999999")] {
                let mut fields: std::collections::BTreeMap<String, String> = [
                    ("popTime".into(), "1".into()),
                    ("invisibleTime".into(), "2".into()),
                    ("reviveQid".into(), "3".into()),
                    ("restNum".into(), "4".into()),
                ]
                .into();
                if let Some(value) = value {
                    fields.insert(key.into(), value.into());
                } else {
                    fields.remove(key);
                }
                let (api, client, addr, task) =
                    peer(code, response::SUCCESS, Some(vec![0xff]), Some(fields)).await;
                let result = if code == request::POP_MESSAGE {
                    api.pop_message(&addr, &Default::default(), deadline())
                        .await
                        .map(|_| ())
                } else {
                    api.pop_lite_message(&addr, &Default::default(), deadline())
                        .await
                        .map(|_| ())
                };
                result.unwrap();
                task.await.unwrap();
                client.shutdown().await;
            }
        }
    }
}

#[tokio::test]
async fn change_invisibility_defaults_revive_queue_id_outside_i32() {
    for revive_qid in ["2147483648", "-2147483649"] {
        let fields = Some(
            [
                ("popTime".into(), "12".into()),
                ("invisibleTime".into(), "30".into()),
                ("reviveQid".into(), revive_qid.into()),
            ]
            .into(),
        );
        let (api, client, addr, task) = peer(
            request::CHANGE_MESSAGE_INVISIBLETIME,
            response::SUCCESS,
            None,
            fields,
        )
        .await;
        let result = api
            .change_invisible_time(
                &addr,
                "broker",
                &valid_change_invisible_header(),
                deadline(),
            )
            .await;
        let result = result.unwrap();
        assert_eq!(result.status, AckStatus::Ok);
        assert_eq!(result.revive_qid, Some(0));
        assert_eq!(result.pop_time, Some(12));
        task.await.unwrap();
        client.shutdown().await;
    }
}

#[tokio::test]
async fn change_invisibility_non_success_ignores_invalid_primitive_header() {
    let fields = Some([("popTime".into(), "bad".into())].into());
    let (api, client, addr, task) =
        peer(request::CHANGE_MESSAGE_INVISIBLETIME, 999, None, fields).await;
    let result = api
        .change_invisible_time(&addr, "broker", &Default::default(), deadline())
        .await;
    assert_eq!(result.unwrap().status, AckStatus::NoExist);
    task.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn pop_unknown_response_precedes_malformed_success_fields() {
    for code in [request::POP_MESSAGE, request::POP_LITE_MESSAGE] {
        let (api, client, addr, task) = peer(code, 999, None, None).await;
        let result = if code == request::POP_MESSAGE {
            api.pop_message(&addr, &Default::default(), deadline())
                .await
                .map(|_| ())
        } else {
            api.pop_lite_message(&addr, &Default::default(), deadline())
                .await
                .map(|_| ())
        };
        assert!(
            matches!(result, Err(RpcError::Response { request_code, code: 999, peer, remark })
            if request_code == code && peer == addr && remark.as_deref() == Some("rejected"))
        );
        task.await.unwrap();
        client.shutdown().await;
    }
}
