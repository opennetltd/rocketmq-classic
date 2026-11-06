use std::{fs, path::PathBuf, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_core::{
    api::{
        MQClientApi,
        producer::{SendKind, SendMode, SendStatus},
    },
    error::RpcError,
};
use rocketmq_classic_protocol::header::{EndTransactionRequestHeader, SendMessageRequestHeaderV1};
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
async fn java_invoked_producer_requests_and_outcomes_match() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rpc-invoked-producer");
    let names = [
        "send-v2-send-ok",
        "send-v2-flush-disk-timeout",
        "send-v2-flush-slave-timeout",
        "send-v2-slave-not-available",
        "send-v2-error",
        "send-v2-oneway",
        "send-batch-oneway",
        "send-reply-v2-oneway",
        "end-transaction-oneway",
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
            assert_eq!(expected["mode"], "SYNC");
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
        let outcome = if name == "end-transaction-oneway" {
            let header = {
                let mut __phase18_header_0 = EndTransactionRequestHeader::default();
                __phase18_header_0.set_producer_group(Some("producer".into()));
                __phase18_header_0.set_tran_state_table_offset(Some(3));
                __phase18_header_0.set_commit_log_offset(Some(9));
                __phase18_header_0.set_commit_or_rollback(Some(8));
                __phase18_header_0.set_from_transaction_check(Some(false));
                __phase18_header_0.set_msg_id(Some("transaction-id".into()));
                __phase18_header_0
            };
            api.end_transaction_oneway(&addr, &header, Some("offline end".into()), deadline)
                .await
                .unwrap();
            "ok".to_owned()
        } else {
            let header = {
                let mut __phase18_header_1 = SendMessageRequestHeaderV1::default();
                __phase18_header_1.set_producer_group(Some("producer".into()));
                __phase18_header_1.set_topic("oracle-topic".into());
                __phase18_header_1.set_default_topic("TBW102".into());
                __phase18_header_1.set_default_topic_queue_nums(4);
                __phase18_header_1.set_queue_id(2);
                __phase18_header_1.set_born_timestamp(1);
                __phase18_header_1.set_properties(Some(String::new()));
                __phase18_header_1.set_reconsume_times(Some(0));
                __phase18_header_1.set_unit_mode(Some(false));
                __phase18_header_1.set_batch(Some(name == "send-batch-oneway"));
                __phase18_header_1
            };
            let (kind, body) = match name {
                "send-batch-oneway" => (
                    SendKind::Batch,
                    decode_base64(&fixture["request"]["bodyBase64"]).unwrap(),
                ),
                "send-reply-v2-oneway" => (SendKind::ReplyV2, b"reply".to_vec()),
                _ => (SendKind::V2, b"payload".to_vec()),
            };
            let mode = if fixture["mode"] == "ONEWAY" {
                SendMode::Oneway
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
