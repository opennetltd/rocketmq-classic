use std::{fs, path::PathBuf, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_core::{
    api::{
        MQClientApi,
        producer::{SendKind, SendMode, SendStatus},
    },
    error::RpcError,
};
use rocketmq_classic_protocol::header::SendMessageRequestHeaderV1;
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RemotingClient, TransportMode},
    codec::frame,
};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::Instant,
};

fn decode_base64(value: &Value) -> Option<Vec<u8>> {
    value.as_str().map(|encoded| {
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let (mut bytes, mut bits, mut count) = (Vec::new(), 0u32, 0);
        for ch in encoded.bytes().take_while(|ch| *ch != b'=') {
            bits = (bits << 6) | alphabet.iter().position(|digit| *digit == ch).unwrap() as u32;
            count += 6;
            if count >= 8 {
                count -= 8;
                bytes.push((bits >> count) as u8);
                bits &= (1 << count) - 1;
            }
        }
        bytes
    })
}

#[tokio::test]
async fn java_invoked_producer_more_requests_and_outcomes_match() {
    let dir =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rpc-invoked-producer-more");
    let names = [
        "send-v1-sync",
        "send-v1-async",
        "send-v1-oneway",
        "send-reply-v1-sync",
        "send-reply-v1-async",
        "send-reply-v1-oneway",
        "send-v2-sync",
        "send-v2-async",
        "send-v2-oneway",
        "send-reply-v2-sync",
        "send-reply-v2-async",
        "send-reply-v2-oneway",
        "send-batch-v1-sync",
        "send-batch-v1-async",
        "send-batch-v1-oneway",
        "send-batch-v2-sync",
        "send-batch-v2-async",
        "send-batch-v2-oneway",
        "send-v2-malformed",
        "send-v2-unknown",
        "send-v2-malformed-async",
        "send-v2-unknown-async",
    ];
    assert_eq!(fs::read_dir(&dir).unwrap().count(), names.len());
    for name in names {
        let fixture: Value =
            serde_json::from_slice(&fs::read(dir.join(format!("{name}.json"))).unwrap()).unwrap();
        assert_eq!(fixture["case"], name);
        assert_eq!(
            fixture["provenance"]["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(fixture["provenance"]["serverBaselineContacted"], false);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let expected = fixture.clone();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut len = [0; 4];
            stream.read_exact(&mut len).await.unwrap();
            let mut frame_bytes = BytesMut::from(&len[..]);
            frame_bytes.resize(u32::from_be_bytes(len) as usize + 4, 0);
            stream.read_exact(&mut frame_bytes[4..]).await.unwrap();
            let incoming = frame::decode_next(&mut frame_bytes).unwrap().unwrap();
            assert_eq!(
                incoming.code,
                expected["request"]["code"].as_i64().unwrap() as i32
            );
            assert_eq!(
                incoming.ext_fields.as_ref().unwrap(),
                &serde_json::from_value::<rocketmq_classic_protocol::header::HeaderMap>(
                    expected["request"]["ext"].clone(),
                )
                .unwrap()
            );
            assert_eq!(
                incoming.body,
                decode_base64(&expected["request"]["bodyBase64"])
            );
            let oneway = expected["mode"] == "ONEWAY";
            assert_eq!(incoming.is_oneway(), oneway);
            if oneway {
                assert!(expected["response"].is_null());
                return;
            }
            assert!(expected["mode"] == "SYNC" || expected["mode"] == "ASYNC");
            let response = &expected["response"];
            let mut reply = RemotingCommand {
                code: response["code"].as_i64().unwrap() as i32,
                opaque: incoming.opaque,
                ext_fields: Some(serde_json::from_value(response["ext"].clone()).unwrap()),
                body: decode_base64(&response["bodyBase64"]),
                ..Default::default()
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
        let deadline = Instant::now() + Duration::from_secs(3);
        let outcome = {
            let header = {
                let mut __phase18_header_0 = SendMessageRequestHeaderV1::default();
                __phase18_header_0.set_producer_group(Some("producer".into()));
                __phase18_header_0.set_topic("oracle-topic".into());
                __phase18_header_0.set_default_topic("TBW102".into());
                __phase18_header_0.set_default_topic_queue_nums(4);
                __phase18_header_0.set_queue_id(2);
                __phase18_header_0.set_born_timestamp(1);
                __phase18_header_0.set_properties(Some(String::new()));
                __phase18_header_0.set_reconsume_times(Some(0));
                __phase18_header_0.set_unit_mode(
                    (!name.contains("v1") || name.starts_with("send-batch")).then_some(false),
                );
                __phase18_header_0.set_batch(
                    (!name.contains("v1") || name.starts_with("send-batch"))
                        .then_some(name.starts_with("send-batch")),
                );
                __phase18_header_0
            };
            let kind = if name.starts_with("send-batch") {
                SendKind::Batch
            } else if name.starts_with("send-reply-v1") {
                SendKind::ReplyV1
            } else if name.starts_with("send-reply") {
                SendKind::ReplyV2
            } else if name.starts_with("send-v1") {
                SendKind::V1
            } else {
                SendKind::V2
            };
            let body = if name.starts_with("send-batch") {
                decode_base64(&fixture["request"]["bodyBase64"]).unwrap()
            } else if name.starts_with("send-reply") {
                b"reply".to_vec()
            } else {
                b"payload".to_vec()
            };
            let mode = if fixture["mode"] == "ONEWAY" {
                SendMode::Oneway
            } else if fixture["mode"] == "ASYNC" {
                SendMode::Future
            } else {
                SendMode::Sync
            };
            match api
                .send_message(
                    &addr, "broker", &header, body, "unused", kind, mode, deadline,
                )
                .await
            {
                Ok(None) => "ok".into(),
                Ok(Some(result)) => {
                    assert_eq!(result.raw_resp_body, None, "{name}");
                    let status = match result.status {
                        SendStatus::SendOk => "SEND_OK",
                        SendStatus::FlushDiskTimeout => "FLUSH_DISK_TIMEOUT",
                        SendStatus::FlushSlaveTimeout => "FLUSH_SLAVE_TIMEOUT",
                        SendStatus::SlaveNotAvailable => "SLAVE_NOT_AVAILABLE",
                    };
                    format!(
                        "{status};queueOffset={};offsetMsgId={}",
                        result.queue_offset,
                        result.offset_msg_id.as_deref().unwrap_or("null")
                    )
                }
                Err(RpcError::Response { code, .. }) => format!("MQBrokerException;code={code}"),
                Err(RpcError::MalformedResponse { request_code, .. })
                    if name.starts_with("send-v2-malformed") =>
                {
                    // Java dereferences the absent queueId; Rust rejects the malformed header.
                    assert_eq!(request_code, 310);
                    "NullPointerException".into()
                }
                other => panic!("unexpected producer result: {other:?}"),
            }
        };
        assert_eq!(outcome, fixture["outcome"], "{name}");
        tokio::time::timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
        client.shutdown().await;
    }
}
