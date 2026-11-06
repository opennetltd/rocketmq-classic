use std::{fs, path::PathBuf, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_core::{api::MQClientApi, error::RpcError};
use rocketmq_classic_protocol::SharedValue;
use rocketmq_classic_protocol::body::{
    LiteSubscriptionAction, LiteSubscriptionCtlRequestBody, LiteSubscriptionDto,
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

fn decode_base64(value: &Value) -> Option<Vec<u8>> {
    value.as_str().map(|encoded| {
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let (mut result, mut bits, mut count) = (Vec::new(), 0u32, 0);
        for byte in encoded.bytes().take_while(|byte| *byte != b'=') {
            bits = (bits << 6)
                | alphabet
                    .iter()
                    .position(|item| *item == byte)
                    .expect("base64 digit") as u32;
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

#[tokio::test]
#[allow(clippy::mutable_key_type)] // This differential exercises Java identity-hashed Set entries.
async fn invoked_lite_subscription_extension_matches_rust() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rpc-invoked-ext");
    assert_eq!(fs::read_dir(&dir).unwrap().count(), 3);
    for name in [
        "lite-subscription-success",
        "lite-subscription-error",
        "lite-subscription-unknown",
    ] {
        let fixture: Value =
            serde_json::from_slice(&fs::read(dir.join(format!("{name}.json"))).unwrap()).unwrap();
        assert_eq!(fixture["case"], name);
        assert_eq!(fixture["mode"], "ASYNC");
        assert_eq!(
            fixture["provenance"]["entrypoint"],
            "MQClientAPIExt.syncLiteSubscriptionAsync"
        );
        assert_eq!(fixture["provenance"]["serverBaselineContacted"], false);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let expected_fixture = fixture.clone();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut len = [0; 4];
            stream.read_exact(&mut len).await.unwrap();
            let mut frame_bytes = BytesMut::from(&len[..]);
            frame_bytes.resize(u32::from_be_bytes(len) as usize + 4, 0);
            stream.read_exact(&mut frame_bytes[4..]).await.unwrap();
            let incoming = frame::decode_next(&mut frame_bytes).unwrap().unwrap();
            let expected = &expected_fixture["request"];
            assert_eq!(incoming.code, expected["code"].as_i64().unwrap() as i32);
            assert_eq!(
                incoming.ext_fields,
                Some(serde_json::from_value(expected["ext"].clone()).unwrap())
            );
            assert!(!incoming.is_oneway());
            // Java and Rust JSON serializers may order keys differently; compare decoded wire values.
            assert_eq!(
                serde_json::from_slice::<Value>(incoming.body.as_ref().unwrap()).unwrap(),
                serde_json::from_slice::<Value>(&decode_base64(&expected["bodyBase64"]).unwrap())
                    .unwrap()
            );
            let response = &expected_fixture["response"];
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
        let body = LiteSubscriptionCtlRequestBody {
            subscription_set: Some(SharedValue::new(std::collections::HashSet::from([Some(
                SharedValue::new(LiteSubscriptionDto {
                    action: Some(LiteSubscriptionAction::CompleteAdd),
                    client_id: Some("client".into()),
                    group: Some("workers".into()),
                    topic: Some("orders".into()),
                    lite_topic_set: Some(SharedValue::new(std::collections::BTreeSet::from([
                        Some("lite-orders".into()),
                    ]))),
                    version: 0,
                    ..Default::default()
                }),
            )]))),
        };
        let outcome = match api
            .lite_subscription_ctl(&addr, &body, Instant::now() + Duration::from_secs(3))
            .await
        {
            Ok(()) => "ok".to_owned(),
            Err(RpcError::Response { code, .. }) => format!("MQBrokerException;code={code}"),
            other => panic!("unexpected extension result: {other:?}"),
        };
        assert_eq!(outcome, fixture["outcome"], "{name}");
        tokio::time::timeout(Duration::from_secs(3), peer)
            .await
            .expect("scripted peer timed out")
            .expect("scripted peer request comparison failed");
        client.shutdown().await;
    }
}
