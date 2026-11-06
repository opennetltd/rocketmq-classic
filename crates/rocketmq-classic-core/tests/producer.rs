use std::time::Duration;

use bytes::BytesMut;
use rocketmq_classic_core::{
    api::{
        MQClientApi,
        producer::{SendKind, SendMode, SendStatus},
    },
    error::RpcError,
};
use rocketmq_classic_protocol::{
    codes::{request, response},
    header::{EndTransactionRequestHeader, SendMessageRequestHeaderV1},
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

#[test]
fn send_smart_switch_reads_java_boolean_property_and_selects_request_codes() {
    const CHILD_PROPERTY: &str = "ROCKETMQ_TEST_SEND_SMART_BOOLEAN_CHILD";
    const SEND_SMART_PROPERTY: &str = "org.apache.rocketmq.client.sendSmartMsg";

    if let Ok(value) = std::env::var(CHILD_PROPERTY) {
        let smart = value == "missing" || value.eq_ignore_ascii_case("true");
        let _api = MQClientApi::new(RemotingClient::new(TransportMode::Plain));
        assert_eq!(
            MQClientApi::send_kind_for_message(false, false),
            if smart { SendKind::V2 } else { SendKind::V1 }
        );
        assert_eq!(
            MQClientApi::send_kind_for_message(false, true),
            if smart {
                SendKind::ReplyV2
            } else {
                SendKind::ReplyV1
            }
        );
        assert_eq!(
            MQClientApi::send_kind_for_message(true, false),
            SendKind::Batch
        );
        assert_eq!(
            MQClientApi::send_kind_for_message(true, true),
            if smart {
                SendKind::ReplyV2
            } else {
                SendKind::ReplyV1
            }
        );
        return;
    }

    for value in ["missing", "false", "TRUE", " true ", "not-a-boolean"] {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "send_smart_switch_reads_java_boolean_property_and_selects_request_codes",
            "--nocapture",
        ]);
        command.env(CHILD_PROPERTY, value);
        if value == "missing" {
            command.env_remove(SEND_SMART_PROPERTY);
        } else {
            command.env(SEND_SMART_PROPERTY, value);
        }
        let output = command
            .output()
            .expect("spawn isolated sendSmart property test process");
        assert!(
            output.status.success(),
            "{value} child test failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

async fn peer(
    code: i32,
    reply_fields: bool,
    expected_request: i32,
    oneway: bool,
) -> (String, tokio::task::JoinHandle<()>) {
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
        assert_eq!(incoming.code, expected_request);
        if expected_request == request::END_TRANSACTION {
            assert_eq!(incoming.remark.as_deref(), Some("commit"));
        } else {
            assert_eq!(incoming.body.as_deref(), Some(&b"payload"[..]));
            let fields = incoming.ext_fields.as_ref().unwrap();
            assert_eq!(
                fields
                    .get(
                        if expected_request == request::SEND_MESSAGE
                            || expected_request == request::SEND_REPLY_MESSAGE
                        {
                            "topic"
                        } else {
                            "b"
                        }
                    )
                    .map(String::as_str),
                Some("orders")
            );
        }
        if oneway {
            assert!(incoming.is_oneway());
            return;
        }
        let mut reply = RemotingCommand {
            code,
            opaque: incoming.opaque,
            remark: Some("rejected".into()),
            ext_fields: reply_fields.then(|| {
                [
                    ("msgId".into(), "offset-id".into()),
                    ("queueId".into(), "2".into()),
                    ("queueOffset".into(), "17".into()),
                    ("transactionId".into(), "tx".into()),
                    ("batchUniqId".into(), "batch-id".into()),
                    ("recallHandle".into(), "recall".into()),
                    ("MSG_REGION".into(), "europe".into()),
                    ("TRACE_ON".into(), "false".into()),
                ]
                .into()
            }),
            ..RemotingCommand::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
    });
    (addr, task)
}

fn header() -> SendMessageRequestHeaderV1 {
    {
        let mut __phase18_header_0 = SendMessageRequestHeaderV1::default();
        __phase18_header_0.set_topic("orders".into());
        __phase18_header_0
    }
}

fn assert_send_result(
    result: rocketmq_classic_core::api::producer::SendResult,
    status: SendStatus,
) {
    assert_eq!(result.status, status);
    assert_eq!(result.msg_id, "uniq");
    assert_eq!(result.offset_msg_id.as_deref(), Some("offset-id"));
    assert_eq!(result.topic, "orders");
    assert_eq!(result.broker_name, "broker");
    assert_eq!(result.queue_id, 2);
    assert_eq!(result.queue_offset, 17);
    assert_eq!(result.transaction_id.as_deref(), Some("tx"));
    assert_eq!(result.recall_handle.as_deref(), Some("recall"));
    assert_eq!(result.raw_resp_body, None);
    assert_eq!(result.region_id, "europe");
    assert!(!result.trace_on);
}

#[tokio::test]
async fn send_statuses_errors_and_malformed_headers() {
    for (code, expected) in [
        (response::SUCCESS, Some(SendStatus::SendOk)),
        (
            response::FLUSH_DISK_TIMEOUT,
            Some(SendStatus::FlushDiskTimeout),
        ),
        (
            response::FLUSH_SLAVE_TIMEOUT,
            Some(SendStatus::FlushSlaveTimeout),
        ),
        (
            response::SLAVE_NOT_AVAILABLE,
            Some(SendStatus::SlaveNotAvailable),
        ),
        (98765, None),
    ] {
        let (addr, task) = peer(code, true, request::SEND_BATCH_MESSAGE, false).await;
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let result = MQClientApi::new(client.clone())
            .send_message(
                &addr,
                "broker",
                &header(),
                b"payload".to_vec(),
                "uniq",
                SendKind::Batch,
                SendMode::Sync,
                Instant::now() + Duration::from_secs(2),
            )
            .await;
        if let Some(expected) = expected {
            let result = result.unwrap().unwrap();
            assert_eq!(result.status, expected);
            assert_eq!(result.msg_id, "uniq");
            assert_eq!(result.offset_msg_id.as_deref(), Some("offset-id"));
            assert_eq!(result.transaction_id.as_deref(), Some("tx"));
            assert_eq!(result.recall_handle.as_deref(), Some("recall"));
            assert_eq!(result.raw_resp_body, None);
            assert_eq!(result.region_id, "europe");
            assert!(!result.trace_on);
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    code: 98765,
                    request_code: request::SEND_BATCH_MESSAGE,
                    ..
                })
            ));
        }
        task.await.unwrap();
        client.shutdown().await;
    }
    let (addr, task) = peer(response::SUCCESS, false, request::SEND_MESSAGE, false).await;
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    assert!(matches!(
        MQClientApi::new(client.clone())
            .send_message(
                &addr,
                "broker",
                &header(),
                b"payload".to_vec(),
                "uniq",
                SendKind::V1,
                SendMode::Future,
                Instant::now() + Duration::from_secs(2)
            )
            .await,
        Err(RpcError::MalformedResponse { .. })
    ));
    task.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn send_result_preserves_null_offset_msg_id_from_java_response_header() {
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
        assert_eq!(incoming.code, request::SEND_MESSAGE);

        let mut reply = RemotingCommand {
            code: response::SUCCESS,
            opaque: incoming.opaque,
            ext_fields: Some(
                [
                    ("queueId".into(), "2".into()),
                    ("queueOffset".into(), "17".into()),
                ]
                .into(),
            ),
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
    let result = MQClientApi::new(client.clone())
        .send_message(
            &addr,
            "broker",
            &header(),
            b"payload".to_vec(),
            "uniq",
            SendKind::V1,
            SendMode::Sync,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.msg_id, "uniq");
    assert_eq!(result.offset_msg_id, None);
    assert_eq!(result.queue_id, 2);
    assert_eq!(result.queue_offset, 17);
    task.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn oneway_send_and_end_transaction_do_not_wait_for_reply() {
    for (code, kind) in [
        (request::SEND_MESSAGE, SendKind::V1),
        (request::SEND_MESSAGE_V2, SendKind::V2),
        (request::SEND_REPLY_MESSAGE, SendKind::ReplyV1),
        (request::SEND_REPLY_MESSAGE_V2, SendKind::ReplyV2),
    ] {
        let (addr, task) = peer(0, false, code, true).await;
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        assert!(
            MQClientApi::new(client.clone())
                .send_message(
                    &addr,
                    "broker",
                    &header(),
                    b"payload".to_vec(),
                    "uniq",
                    kind,
                    SendMode::Oneway,
                    Instant::now() + Duration::from_secs(2)
                )
                .await
                .unwrap()
                .is_none()
        );
        task.await.unwrap();
        client.shutdown().await;
    }
    let (addr, task) = peer(0, false, request::END_TRANSACTION, true).await;
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    MQClientApi::new(client.clone())
        .end_transaction_oneway(
            &addr,
            &EndTransactionRequestHeader::default(),
            Some("commit".into()),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    task.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn every_send_variant_classifies_reply_before_decoding() {
    for (kind, code) in [
        (SendKind::V1, request::SEND_MESSAGE),
        (SendKind::V2, request::SEND_MESSAGE_V2),
        (SendKind::Batch, request::SEND_BATCH_MESSAGE),
        (SendKind::ReplyV1, request::SEND_REPLY_MESSAGE),
        (SendKind::ReplyV2, request::SEND_REPLY_MESSAGE_V2),
    ] {
        for mode in [SendMode::Sync, SendMode::Future] {
            for (response_code, reply_fields) in [
                (response::SUCCESS, true),
                (response::FLUSH_DISK_TIMEOUT, true),
                (response::FLUSH_SLAVE_TIMEOUT, true),
                (response::SLAVE_NOT_AVAILABLE, true),
                (99123, false),
                (response::SUCCESS, false),
                (response::FLUSH_DISK_TIMEOUT, false),
                (response::FLUSH_SLAVE_TIMEOUT, false),
                (response::SLAVE_NOT_AVAILABLE, false),
            ] {
                let (addr, task) = peer(response_code, reply_fields, code, false).await;
                let client = RemotingClient::new(TransportMode::Plain);
                client.start().unwrap();
                let result = MQClientApi::new(client.clone())
                    .send_message(
                        &addr,
                        "broker",
                        &header(),
                        b"payload".to_vec(),
                        "uniq",
                        kind,
                        mode,
                        Instant::now() + Duration::from_secs(2),
                    )
                    .await;
                match (response_code, reply_fields) {
                    (99123, _) => assert!(matches!(
                        result,
                        Err(RpcError::Response { code: 99123, .. })
                    )),
                    (_, false) => {
                        assert!(matches!(result, Err(RpcError::MalformedResponse { .. })))
                    }
                    (response::SUCCESS, _) => {
                        assert_send_result(result.unwrap().unwrap(), SendStatus::SendOk)
                    }
                    (response::FLUSH_DISK_TIMEOUT, _) => {
                        assert_send_result(result.unwrap().unwrap(), SendStatus::FlushDiskTimeout)
                    }
                    (response::FLUSH_SLAVE_TIMEOUT, _) => {
                        assert_send_result(result.unwrap().unwrap(), SendStatus::FlushSlaveTimeout)
                    }
                    _ => {
                        assert_send_result(result.unwrap().unwrap(), SendStatus::SlaveNotAvailable)
                    }
                }
                task.await.unwrap();
                client.shutdown().await;
            }
        }
    }
}
