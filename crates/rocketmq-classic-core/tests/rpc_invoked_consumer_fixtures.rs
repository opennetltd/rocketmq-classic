use std::{fs, path::PathBuf, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_core::{api::MQClientApi, error::RpcError};
use rocketmq_classic_protocol::header::ConsumerSendMsgBackRequestHeader;
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

fn decode_base64(encoded: &str) -> Vec<u8> {
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let (mut result, mut bits, mut count) = (Vec::new(), 0u32, 0);
    for char in encoded.bytes().take_while(|char| *char != b'=') {
        let digit = alphabet.iter().position(|item| *item == char).unwrap();
        bits = (bits << 6) | digit as u32;
        count += 6;
        if count >= 8 {
            count -= 8;
            result.push((bits >> count) as u8);
            bits &= (1 << count) - 1;
        }
    }
    result
}

fn body(value: &Value) -> Option<Vec<u8>> {
    value.as_str().map(decode_base64)
}

#[tokio::test]
async fn java_invoked_consumer_requests_and_outcomes_match() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rpc-invoked-consumer");
    let names = [
        "consumer-list-populated",
        "consumer-list-empty",
        "consumer-list-no-body",
        "consumer-list-error",
        "consumer-list-unknown",
        "consumer-list-invalid-json",
        "send-back-success",
        "send-back-error",
        "send-back-unknown",
    ];
    assert_eq!(fs::read_dir(&dir).unwrap().count(), names.len());
    for name in names {
        let fixture: Value =
            serde_json::from_slice(&fs::read(dir.join(format!("{name}.json"))).unwrap()).unwrap();
        assert_eq!(fixture["case"], name);
        assert_eq!(fixture["mode"], "SYNC");
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
            assert_eq!(incoming.body, body(&expected["request"]["bodyBase64"]));
            assert!(!incoming.is_oneway());
            let response = &expected["response"];
            let mut reply = RemotingCommand {
                code: response["code"].as_i64().unwrap() as i32,
                opaque: incoming.opaque,
                remark: response["remark"].as_str().map(str::to_owned),
                ext_fields: Some(serde_json::from_value(response["ext"].clone()).unwrap()),
                body: body(&response["bodyBase64"]),
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
        let outcome = if name.starts_with("consumer-list-") {
            match api
                .get_consumer_list_by_group(&addr, "consumer", deadline)
                .await
            {
                Ok(Some(clients)) => format!(
                    "clients={}",
                    clients
                        .into_iter()
                        .map(|client| client.unwrap_or_else(|| "null".into()))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                Err(RpcError::Response { code: 0, .. }) => {
                    "MQBrokerException;code=0;malformed".into()
                }
                Err(RpcError::Response { code, .. }) => format!("MQBrokerException;code={code}"),
                Err(RpcError::MalformedResponse { .. }) => "JSONException;malformed".into(),
                other => panic!("unexpected consumer list result: {other:?}"),
            }
        } else {
            let header = {
                let mut __phase18_header_0 = ConsumerSendMsgBackRequestHeader::default();
                __phase18_header_0.set_group("consumer".into());
                __phase18_header_0.set_broker_name(Some("broker".into()));
                __phase18_header_0.set_origin_topic("oracle-topic".into());
                __phase18_header_0.set_offset(19);
                __phase18_header_0.set_delay_level(3);
                __phase18_header_0.set_origin_msg_id(Some("retry-id".into()));
                __phase18_header_0.set_max_reconsume_times(Some(5));
                __phase18_header_0
            };
            match api
                .consumer_send_message_back(&addr, &header, deadline)
                .await
            {
                Ok(()) => "ok".into(),
                Err(RpcError::Response { code, .. }) => format!("MQBrokerException;code={code}"),
                other => panic!("unexpected send back result: {other:?}"),
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
