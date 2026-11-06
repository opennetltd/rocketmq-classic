use std::{fs, path::PathBuf, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_core::{
    api::{MQClientApi, extensions::AckStatus},
    error::RpcError,
};
use rocketmq_classic_protocol::{
    SharedValue,
    body::BatchAckMessageRequestBody,
    header::{
        AckMessageRequestHeader, ChangeInvisibleTimeRequestHeader, PopLiteMessageRequestHeader,
        PopMessageRequestHeader,
    },
};
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

fn bytes(value: &Value) -> Option<Vec<u8>> {
    value.as_str().map(|encoded| {
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let (mut result, mut bits, mut count) = (Vec::new(), 0u32, 0);
        for byte in encoded.bytes().take_while(|byte| *byte != b'=') {
            bits = (bits << 6) | alphabet.iter().position(|item| *item == byte).unwrap() as u32;
            count += 6;
            if count >= 8 {
                count -= 8;
                result.push((bits >> count) as u8);
                bits &= (1 << count) - 1;
            }
        }
        result
    })
}

fn ack_outcome(result: Result<AckStatus, RpcError>) -> String {
    match result.unwrap() {
        AckStatus::Ok => "status=OK",
        AckStatus::NoExist => "status=NO_EXIST",
    }
    .into()
}

#[tokio::test]
async fn invoked_pop_ack_callbacks_match_rust_domain_outcomes() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rpc-invoked-pop-ack");
    let mut files: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .is_some_and(|name| name != "batch-ack-extra-info.json")
        })
        .collect();
    files.sort();
    assert_eq!(files.len(), 29);
    for file in files {
        let fixture: Value = serde_json::from_slice(&fs::read(file).unwrap()).unwrap();
        assert_eq!(fixture["mode"], "ASYNC");
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
            let mut buffer = BytesMut::from(&len[..]);
            buffer.resize(u32::from_be_bytes(len) as usize + 4, 0);
            stream.read_exact(&mut buffer[4..]).await.unwrap();
            let incoming = frame::decode_next(&mut buffer).unwrap().unwrap();
            assert!(!incoming.is_oneway());
            assert_eq!(
                incoming.code,
                expected["request"]["code"].as_i64().unwrap() as i32
            );
            assert_eq!(
                incoming.ext_fields,
                Some(serde_json::from_value(expected["request"]["ext"].clone()).unwrap())
            );
            match (incoming.body, bytes(&expected["request"]["bodyBase64"])) {
                (Some(actual), Some(reference)) => assert_eq!(
                    serde_json::from_slice::<Value>(&actual).unwrap(),
                    serde_json::from_slice::<Value>(&reference).unwrap()
                ),
                (None, None) => (),
                other => panic!("body presence {other:?}"),
            }
            let mut reply = RemotingCommand {
                code: expected["response"]["code"].as_i64().unwrap() as i32,
                opaque: incoming.opaque,
                ext_fields: Some(
                    serde_json::from_value(expected["response"]["ext"].clone()).unwrap(),
                ),
                body: bytes(&expected["response"]["bodyBase64"]),
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
        let result = match fixture["family"].as_str().unwrap() {
            "pop" | "pop-lite" => {
                let status = if fixture["family"] == "pop" {
                    api.pop_message(
                        &addr,
                        &{
                            let mut __phase18_header_0 = PopMessageRequestHeader::default();
                            __phase18_header_0.set_consumer_group(Some("consumer".into()));
                            __phase18_header_0.set_topic(Some("oracle-topic".into()));
                            __phase18_header_0.set_queue_id(2);
                            __phase18_header_0.set_max_msg_nums(1);
                            __phase18_header_0.set_invisible_time(456);
                            __phase18_header_0.set_born_time(123);
                            __phase18_header_0.set_order(Some(false));
                            __phase18_header_0
                        },
                        deadline,
                    )
                    .await
                    .map(|reply| {
                        assert_eq!(
                            reply.raw_messages,
                            bytes(&fixture["response"]["bodyBase64"])
                        );
                        let header = reply.header.unwrap();
                        let defaults = fixture["case"]
                            .as_str()
                            .unwrap()
                            .ends_with("header-default");
                        assert_eq!(header.get_pop_time(), if defaults { 0 } else { 123 });
                        assert_eq!(header.get_invisible_time(), if defaults { 0 } else { 456 });
                        assert_eq!(header.get_revive_qid(), if defaults { 0 } else { 2 });
                        assert_eq!(header.get_rest_num(), if defaults { 0 } else { 7 });
                        reply.status
                    })
                } else {
                    api.pop_lite_message(
                        &addr,
                        &{
                            let mut __phase18_header_1 = PopLiteMessageRequestHeader::default();
                            __phase18_header_1.set_client_id(Some("client".into()));
                            __phase18_header_1.set_consumer_group(Some("consumer".into()));
                            __phase18_header_1.set_topic(Some("oracle-topic".into()));
                            __phase18_header_1.set_max_msg_num(1);
                            __phase18_header_1.set_invisible_time(456);
                            __phase18_header_1.set_born_time(123);
                            __phase18_header_1
                        },
                        deadline,
                    )
                    .await
                    .map(|reply| {
                        assert_eq!(
                            reply.raw_messages,
                            bytes(&fixture["response"]["bodyBase64"])
                        );
                        let header = reply.header.unwrap();
                        let defaults = fixture["case"]
                            .as_str()
                            .unwrap()
                            .ends_with("header-default");
                        assert_eq!(header.get_pop_time(), if defaults { 0 } else { 123 });
                        assert_eq!(header.get_invisible_time(), if defaults { 0 } else { 456 });
                        assert_eq!(header.get_revive_qid(), if defaults { 0 } else { 2 });
                        reply.status
                    })
                };
                match status {
                    Ok(rocketmq_classic_core::api::extensions::PopStatus::Found) => {
                        "status=FOUND".into()
                    }
                    Ok(rocketmq_classic_core::api::extensions::PopStatus::PollingFull) => {
                        "status=POLLING_FULL".into()
                    }
                    Ok(rocketmq_classic_core::api::extensions::PopStatus::PollingNotFound) => {
                        "status=POLLING_NOT_FOUND".into()
                    }
                    Err(RpcError::Response { code, .. }) => format!("error={code}"),
                    Err(RpcError::MalformedResponse { .. }) => "malformed".into(),
                    other => panic!("pop {other:?}"),
                }
            }
            "ack" | "ack-lite" => ack_outcome(
                api.ack_message(
                    &addr,
                    &{
                        let mut __phase18_header_2 = AckMessageRequestHeader::default();
                        __phase18_header_2.set_consumer_group("consumer".into());
                        __phase18_header_2.set_topic("oracle-topic".into());
                        __phase18_header_2.set_queue_id(2);
                        __phase18_header_2.set_offset(17);
                        __phase18_header_2.set_extra_info("checkpoint".into());
                        __phase18_header_2
                    },
                    deadline,
                )
                .await,
            ),
            "batch-ack" => {
                let mut body = BatchAckMessageRequestBody::default();
                body.set_broker_name(Some("broker".into()));
                body.set_acks(Some(SharedValue::new(vec![])));
                ack_outcome(api.batch_ack_message(&addr, &body, deadline).await)
            }
            "change" => {
                match api
                    .change_invisible_time(
                        &addr,
                        "broker",
                        &{
                            let mut __phase18_header_3 =
                                ChangeInvisibleTimeRequestHeader::default();
                            __phase18_header_3.set_consumer_group(Some("consumer".into()));
                            __phase18_header_3.set_topic(Some("oracle-topic".into()));
                            __phase18_header_3.set_queue_id(Some(2));
                            __phase18_header_3.set_offset(Some(17));
                            __phase18_header_3.set_extra_info(Some("checkpoint".into()));
                            __phase18_header_3.set_invisible_time(Some(456));
                            __phase18_header_3
                        },
                        deadline,
                    )
                    .await
                {
                    Ok(result) if result.status == AckStatus::Ok => format!(
                        "status=OK;extra={}",
                        result.extra_info.as_deref().expect("success extra info")
                    ),
                    Ok(_) => "status=NO_EXIST".into(),
                    Err(RpcError::MalformedResponse { .. }) => "malformed".into(),
                    other => panic!("change {other:?}"),
                }
            }
            other => panic!("family {other}"),
        };
        assert_eq!(result, fixture["outcome"], "{}", fixture["case"]);
        peer.await.unwrap();
        client.shutdown().await;
    }
}
