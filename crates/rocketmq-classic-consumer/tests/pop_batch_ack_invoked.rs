use std::{fs, path::PathBuf, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_consumer::pop::batch_ack_message_from_extra_info;
use rocketmq_classic_core::api::{MQClientApi, extensions::AckStatus};
use rocketmq_classic_protocol::codes::request;
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
        let (mut output, mut bits, mut count) = (Vec::new(), 0u32, 0);
        for byte in encoded.bytes().take_while(|byte| *byte != b'=') {
            bits = (bits << 6) | alphabet.iter().position(|item| *item == byte).unwrap() as u32;
            count += 6;
            if count >= 8 {
                count -= 8;
                output.push((bits >> count) as u8);
                bits &= (1 << count) - 1;
            }
        }
        output
    })
}

#[tokio::test]
async fn list_extra_info_batch_ack_matches_the_pinned_java_overload() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/rpc-invoked-pop-ack/batch-ack-extra-info.json");
    let fixture: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(fixture["family"], "batch-ack-extra-info");
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
        assert_eq!(incoming.code, request::BATCH_ACK_MESSAGE);
        assert_eq!(incoming.version, 515);
        assert_eq!(incoming.ext_fields.as_ref().unwrap().len(), 0);
        assert_eq!(
            incoming.body.as_ref(),
            bytes(&expected["request"]["bodyBase64"]).as_ref()
        );
        let response = &expected["response"];
        let mut reply = RemotingCommand {
            code: response["code"].as_i64().unwrap() as i32,
            opaque: incoming.opaque,
            ext_fields: Some(serde_json::from_value(response["ext"].clone()).unwrap()),
            body: bytes(&response["bodyBase64"]),
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
    let input = &fixture["input"];
    let receipts = input["extraInfoList"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().map(str::to_owned))
        .collect::<Vec<_>>();
    let outcome = batch_ack_message_from_extra_info(
        &api,
        &addr,
        input["topic"].as_str(),
        input["consumerGroup"].as_str(),
        Some(&receipts),
        Instant::now() + Duration::from_secs(3),
    )
    .await;
    assert!(matches!(outcome, Ok(AckStatus::Ok)), "{outcome:?}");
    assert_eq!(fixture["outcome"], "status=OK");
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn batch_ack_extra_info_reports_null_and_malformed_inputs_before_network_io() {
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let api = MQClientApi::new(client.clone());
    let deadline = Instant::now() + Duration::from_secs(1);
    assert!(matches!(
        batch_ack_message_from_extra_info(
            &api,
            "unused",
            Some("orders"),
            Some("workers"),
            None,
            deadline
        )
        .await,
        Err(rocketmq_classic_consumer::pop::PopBatchAckError::NullExtraInfoList)
    ));
    let null_receipt = [None];
    assert!(matches!(
        batch_ack_message_from_extra_info(
            &api,
            "unused",
            Some("orders"),
            Some("workers"),
            Some(&null_receipt),
            deadline
        )
        .await,
        Err(rocketmq_classic_consumer::pop::PopBatchAckError::NullExtraInfo { index: 0 })
    ));
    let malformed = [Some("not-a-receipt".to_owned())];
    assert!(matches!(
        batch_ack_message_from_extra_info(
            &api,
            "unused",
            Some("orders"),
            Some("workers"),
            Some(&malformed),
            deadline
        )
        .await,
        Err(rocketmq_classic_consumer::pop::PopBatchAckError::Receipt(_))
    ));
    client.shutdown().await;
}
