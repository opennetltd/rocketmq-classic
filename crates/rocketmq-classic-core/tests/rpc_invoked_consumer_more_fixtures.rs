use std::{fs, path::PathBuf, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_core::{
    api::{MQClientApi, consumer::PullStatus},
    error::RpcError,
};
use rocketmq_classic_protocol::{
    SharedValue,
    body::{LockBatchRequestBody, UnlockBatchRequestBody},
    header::PullMessageRequestHeader,
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
async fn java_invoked_consumer_more_requests_and_outcomes_match() {
    let dir =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rpc-invoked-consumer-more");
    let names = [
        "pull-found",
        "pull-no-new",
        "pull-no-matched",
        "pull-offset-illegal",
        "pull-error",
        "pull-unknown",
        "pull-no-header",
        "pull-invalid-header",
        "pull-found-async",
        "pull-no-new-async",
        "pull-no-matched-async",
        "pull-offset-illegal-async",
        "pull-error-async",
        "pull-unknown-async",
        "pull-no-header-async",
        "pull-invalid-header-async",
        "lite-pull-found",
        "lite-pull-no-new",
        "lite-pull-no-matched",
        "lite-pull-offset-illegal",
        "lite-pull-error",
        "lite-pull-unknown",
        "lite-pull-no-header",
        "lite-pull-invalid-header",
        "lite-pull-found-async",
        "lite-pull-no-new-async",
        "lite-pull-no-matched-async",
        "lite-pull-offset-illegal-async",
        "lite-pull-error-async",
        "lite-pull-unknown-async",
        "lite-pull-no-header-async",
        "lite-pull-invalid-header-async",
        "lock-success",
        "lock-error",
        "lock-unknown",
        "lock-no-body",
        "lock-invalid-json",
        "unlock-sync-success",
        "unlock-sync-error",
        "unlock-sync-unknown",
        "unlock-oneway",
    ];
    assert_eq!(fs::read_dir(&dir).unwrap().count(), names.len());
    for name in names {
        let fixture: Value =
            serde_json::from_slice(&fs::read(dir.join(format!("{name}.json"))).unwrap()).unwrap();
        assert_eq!(fixture["case"], name);
        assert_eq!(
            fixture["mode"],
            if name == "unlock-oneway" {
                "ONEWAY"
            } else if name.ends_with("-async") {
                "ASYNC"
            } else {
                "SYNC"
            }
        );
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
            if expected["request"]["bodyBase64"].is_null() {
                assert_eq!(incoming.body, None);
            } else {
                assert_eq!(
                    serde_json::from_slice::<Value>(incoming.body.as_ref().unwrap()).unwrap(),
                    serde_json::from_slice::<Value>(
                        &body(&expected["request"]["bodyBase64"]).unwrap()
                    )
                    .unwrap()
                );
            }
            assert_eq!(incoming.is_oneway(), expected["mode"] == "ONEWAY");
            if expected["mode"] == "ONEWAY" {
                return;
            }
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
        let queue = MessageQueue::new("oracle-topic", "broker", 2);
        let outcome = if name.starts_with("pull-") || name.starts_with("lite-pull-") {
            let header = {
                let mut __phase18_header_0 = PullMessageRequestHeader::default();
                __phase18_header_0.set_consumer_group(Some("consumer".into()));
                __phase18_header_0.set_topic(Some("oracle-topic".into()));
                __phase18_header_0.set_queue_id(Some(2));
                __phase18_header_0.set_queue_offset(Some(10));
                __phase18_header_0.set_max_msg_nums(Some(1));
                __phase18_header_0.set_sys_flag(Some(if name.starts_with("lite-pull-") {
                    16
                } else {
                    0
                }));
                __phase18_header_0.set_commit_offset(Some(0));
                __phase18_header_0.set_suspend_timeout_millis(Some(0));
                __phase18_header_0.set_sub_version(Some(1));
                __phase18_header_0
            };
            let result = if fixture["mode"] == "ASYNC" {
                api.pull_message_future(&addr, &header, deadline).await
            } else {
                api.pull_message(&addr, &header, deadline).await
            };
            match result {
                Ok(result) => {
                    let status = match result.status {
                        PullStatus::Found => "Found",
                        PullStatus::NoNewMessage => "NoNewMessage",
                        PullStatus::NoMatchedMessage => "NoMatchedMessage",
                        PullStatus::OffsetIllegal => "OffsetIllegal",
                    };
                    let raw = result
                        .raw_messages
                        .as_ref()
                        .map(|bytes| {
                            assert_eq!(bytes, &[0, 255, 42]);
                            "AP8q"
                        })
                        .unwrap_or("null");
                    format!(
                        "status={status};next={};min={};max={};broker={};raw={raw}",
                        result.next_begin_offset,
                        result.min_offset,
                        result.max_offset,
                        result.suggest_which_broker_id
                    )
                }
                Err(RpcError::Response {
                    code, request_code, ..
                }) => {
                    assert_eq!(
                        request_code,
                        fixture["request"]["code"].as_i64().unwrap() as i32
                    );
                    format!("MQBrokerException;code={code}")
                }
                Err(RpcError::MalformedResponse { request_code, .. }) => {
                    assert_eq!(
                        request_code,
                        fixture["request"]["code"].as_i64().unwrap() as i32
                    );
                    "malformed".into()
                }
                other => panic!("unexpected pull result: {other:?}"),
            }
        } else if name.starts_with("lock-") {
            let mut body = LockBatchRequestBody::default();
            body.set_consumer_group(Some("consumer".into()));
            body.set_client_id(Some("client".into()));
            body.set_mq_set(Some(SharedValue::new(std::collections::HashSet::from([
                Some(SharedValue::new(queue)),
            ]))));
            match api.lock_batch_mq(&addr, &body, deadline).await {
                Ok(Some(queues)) => {
                    assert_eq!(queues.len(), 1);
                    let queue = queues.iter().find_map(Option::as_ref).unwrap();
                    format!(
                        "queues={}:{}:{}",
                        queue.topic().unwrap_or_default(),
                        queue.broker_name().unwrap_or_default(),
                        queue.queue_id()
                    )
                }
                Ok(None) => "queues=null".into(),
                Err(RpcError::Response { code, .. }) => format!("MQBrokerException;code={code}"),
                Err(RpcError::MalformedResponse { .. }) => "malformed".into(),
                other => panic!("unexpected lock result: {other:?}"),
            }
        } else {
            let mut body = UnlockBatchRequestBody::default();
            body.set_consumer_group(Some("consumer".into()));
            body.set_client_id(Some("client".into()));
            body.set_mq_set(Some(SharedValue::new(std::collections::HashSet::from([
                Some(SharedValue::new(queue)),
            ]))));
            match api
                .unlock_batch_mq(&addr, &body, name == "unlock-oneway", deadline)
                .await
            {
                Ok(()) => "ok".into(),
                Err(RpcError::Response { code, .. }) => format!("MQBrokerException;code={code}"),
                other => panic!("unexpected unlock result: {other:?}"),
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
