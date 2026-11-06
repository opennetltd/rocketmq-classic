use std::{fs, path::PathBuf, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_core::{api::MQClientApi, error::RpcError};
use rocketmq_classic_protocol::{
    body::{LockBatchRequestBody, UnlockBatchRequestBody},
    header::{
        ConsumerSendMsgBackRequestHeader, GetMaxOffsetRequestHeader, GetMinOffsetRequestHeader,
        HeaderMap, QueryConsumerOffsetRequestHeader, SearchOffsetRequestHeader,
        UpdateConsumerOffsetRequestHeader,
    },
    heartbeat::HeartbeatData,
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

fn ext(value: &Value) -> HeaderMap {
    serde_json::from_value(value.clone()).unwrap()
}

fn request_command(request: &Value) -> RemotingCommand {
    RemotingCommand {
        code: request["code"].as_i64().unwrap() as i32,
        version: request["version"].as_i64().unwrap() as i32,
        ext_fields: Some(ext(&request["ext"]).into()),
        body: body(&request["bodyBase64"]),
        ..Default::default()
    }
}

fn outcome_error(error: RpcError) -> String {
    match error {
        RpcError::OffsetNotFound { code, .. } => format!("OffsetNotFoundException;code={code}"),
        RpcError::Response { code, .. } => format!("MQBrokerException;code={code}"),
        RpcError::MalformedResponse { .. } => "JSONException;malformed".to_owned(),
        other => panic!("unexpected extension future error: {other:?}"),
    }
}

#[tokio::test]
async fn invoked_extension_future_and_oneway_methods_match_java_551() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rpc-invoked-ext-more");
    let names = [
        "heartbeat-future-success",
        "heartbeat-future-error",
        "heartbeat-future-unknown",
        "heartbeat-oneway-success",
        "send-back-future-success",
        "send-back-future-error",
        "query-offset-future-success",
        "query-offset-future-null-offset",
        "query-offset-future-not-found",
        "update-offset-future-success",
        "update-offset-future-error",
        "update-offset-future-unknown",
        "update-offset-oneway-success",
        "consumer-list-future-populated",
        "consumer-list-future-empty",
        "consumer-list-future-success-empty-body",
        "consumer-list-future-null-list",
        "consumer-list-future-error",
        "max-offset-future-success",
        "max-offset-future-null-offset",
        "max-offset-future-error",
        "min-offset-future-success",
        "min-offset-future-null-offset",
        "min-offset-future-error",
        "search-offset-future-success",
        "search-offset-future-null-offset",
        "search-offset-future-error",
        "lock-batch-future-success",
        "lock-batch-future-error",
        "unlock-batch-oneway-success",
        "invoke-raw-error-response",
        "invoke-oneway-success",
    ];

    for name in names {
        let fixture: Value =
            serde_json::from_slice(&fs::read(dir.join(format!("{name}.json"))).unwrap()).unwrap();
        assert_eq!(fixture["case"], name);
        assert_eq!(
            fixture["provenance"]["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(
            fixture["mode"],
            if name.ends_with("oneway-success") {
                "ONEWAY"
            } else if name.starts_with("consumer-list-")
                || name.starts_with("max-offset-")
                || name.starts_with("min-offset-")
            {
                "ASYNC_CALLBACK"
            } else {
                "ASYNC"
            }
        );
        assert_eq!(
            fixture["provenance"]["entrypoint"],
            if name.starts_with("heartbeat-") {
                if name.starts_with("heartbeat-oneway-") {
                    "MQClientAPIExt.sendHeartbeatOneway"
                } else {
                    "MQClientAPIExt.sendHeartbeatAsync"
                }
            } else if name.starts_with("send-back-") {
                "MQClientAPIExt.sendMessageBackAsync"
            } else if name.starts_with("query-offset-") {
                "MQClientAPIExt.queryConsumerOffsetWithFuture"
            } else if name.starts_with("update-offset-oneway-") {
                "MQClientAPIExt.updateConsumerOffsetOneWay"
            } else if name.starts_with("update-offset-") {
                "MQClientAPIExt.updateConsumerOffsetAsync"
            } else if name.starts_with("consumer-list-") {
                "MQClientAPIExt.getConsumerListByGroupAsync"
            } else if name.starts_with("max-offset-") {
                "MQClientAPIExt.getMaxOffset"
            } else if name.starts_with("min-offset-") {
                "MQClientAPIExt.getMinOffset"
            } else if name.starts_with("search-offset-") {
                "MQClientAPIExt.searchOffset"
            } else if name.starts_with("lock-batch-") {
                "MQClientAPIExt.lockBatchMQWithFuture"
            } else if name.starts_with("unlock-batch-") {
                "MQClientAPIExt.unlockBatchMQOneway"
            } else if name.starts_with("invoke-oneway-") {
                "MQClientAPIExt.invokeOneway"
            } else {
                "MQClientAPIExt.invoke"
            }
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
            let expected_request = &expected["request"];
            assert_eq!(
                incoming.code,
                expected_request["code"].as_i64().unwrap() as i32,
                "{name}"
            );
            assert_eq!(
                incoming.ext_fields.as_ref().unwrap(),
                &ext(&expected_request["ext"]),
                "{name}"
            );
            match (&incoming.body, body(&expected_request["bodyBase64"])) {
                (None, None) => {}
                (Some(actual), Some(expected_body)) => {
                    match (
                        serde_json::from_slice::<Value>(actual),
                        serde_json::from_slice::<Value>(&expected_body),
                    ) {
                        (Ok(actual), Ok(expected)) => assert_eq!(actual, expected, "{name}"),
                        _ => assert_eq!(actual, &expected_body, "{name}"),
                    }
                }
                (actual, expected) => assert_eq!(actual, &expected, "{name}"),
            }
            let oneway = expected["mode"] == "ONEWAY";
            assert_eq!(incoming.is_oneway(), oneway, "{name}");
            if !oneway {
                let response = &expected["response"];
                let mut reply = RemotingCommand {
                    code: response["code"].as_i64().unwrap() as i32,
                    version: response["version"].as_i64().unwrap() as i32,
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
            }
        });

        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let api = MQClientApi::new(client.clone());
        let deadline = Instant::now() + Duration::from_secs(3);
        let outcome = if name.starts_with("heartbeat-") {
            let heartbeat = HeartbeatData {
                client_id: Some("client".into()),
                ..HeartbeatData::default()
            };
            if name == "heartbeat-oneway-success" {
                api.send_heartbeat_oneway(&addr, &heartbeat, deadline)
                    .await
                    .map_or_else(outcome_error, |_| "ok".to_owned())
            } else {
                match api.send_heartbeat_async(&addr, &heartbeat, deadline).await {
                    Ok(version) => format!("version={version}"),
                    Err(error) => outcome_error(error),
                }
            }
        } else if name.starts_with("send-back-") {
            let mut header = ConsumerSendMsgBackRequestHeader::default();
            header.set_group("workers".into());
            header.set_broker_name(Some("broker".into()));
            header.set_origin_topic("orders".into());
            header.set_offset(19);
            header.set_delay_level(3);
            header.set_origin_msg_id(Some("retry-id".into()));
            header.set_max_reconsume_times(Some(5));
            match api
                .consumer_send_message_back_async(&addr, &header, deadline)
                .await
            {
                Ok(reply) => format!(
                    "rawCode={};remark={}",
                    reply.code,
                    reply.remark.unwrap_or_else(|| "null".to_owned())
                ),
                Err(error) => outcome_error(error),
            }
        } else if name.starts_with("query-offset-") {
            let mut header = QueryConsumerOffsetRequestHeader::default();
            header.set_consumer_group(Some("workers".into()));
            header.set_topic(Some("orders".into()));
            header.set_queue_id(Some(2));
            match api
                .query_consumer_offset_with_future(&addr, &header, deadline)
                .await
            {
                Ok(Some(offset)) => format!("offset={offset}"),
                Ok(None) => "offset=null".to_owned(),
                Err(error) => outcome_error(error),
            }
        } else if name.starts_with("update-offset-") {
            let mut header = UpdateConsumerOffsetRequestHeader::default();
            header.set_consumer_group(Some("workers".into()));
            header.set_topic(Some("orders".into()));
            header.set_queue_id(Some(2));
            header.set_commit_offset(Some(17));
            if name == "update-offset-oneway-success" {
                api.update_consumer_offset_oneway(&addr, &header, deadline)
                    .await
                    .map_or_else(outcome_error, |_| "ok".to_owned())
            } else {
                api.update_consumer_offset_async(&addr, &header, deadline)
                    .await
                    .map_or_else(outcome_error, |_| "ok".to_owned())
            }
        } else if name.starts_with("consumer-list-") {
            match api
                .get_consumer_list_by_group_future(&addr, Some("workers"), deadline)
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
                Ok(None) => "clients=null".to_owned(),
                Err(error) => outcome_error(error),
            }
        } else if name.starts_with("max-offset-") {
            let mut header = GetMaxOffsetRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header.set_queue_id(Some(2));
            header.set_committed(true);
            match api.get_max_offset_future(&addr, &header, deadline).await {
                Ok(Some(offset)) => format!("offset={offset}"),
                Ok(None) => "offset=null".to_owned(),
                Err(error) => outcome_error(error),
            }
        } else if name.starts_with("min-offset-") {
            let mut header = GetMinOffsetRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header.set_queue_id(Some(2));
            match api.get_min_offset_future(&addr, &header, deadline).await {
                Ok(Some(offset)) => format!("offset={offset}"),
                Ok(None) => "offset=null".to_owned(),
                Err(error) => outcome_error(error),
            }
        } else if name.starts_with("search-offset-") {
            let mut header = SearchOffsetRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header.set_queue_id(Some(2));
            header.set_timestamp(Some(1_700_000_000_000));
            match api.search_offset_future(&addr, &header, deadline).await {
                Ok(Some(offset)) => format!("offset={offset}"),
                Ok(None) => "offset=null".to_owned(),
                Err(error) => outcome_error(error),
            }
        } else if name.starts_with("lock-batch-") {
            let request: LockBatchRequestBody =
                serde_json::from_slice(&body(&fixture["request"]["bodyBase64"]).unwrap()).unwrap();
            match api
                .lock_batch_mq_with_future(&addr, &request, deadline)
                .await
            {
                Ok(Some(queues)) => format!("queues={}", queues.len()),
                Ok(None) => "queues=null".to_owned(),
                Err(error) => outcome_error(error),
            }
        } else if name == "unlock-batch-oneway-success" {
            let request: UnlockBatchRequestBody =
                serde_json::from_slice(&body(&fixture["request"]["bodyBase64"]).unwrap()).unwrap();
            api.unlock_batch_mq_oneway(&addr, &request, deadline)
                .await
                .map_or_else(outcome_error, |_| "ok".to_owned())
        } else if name.starts_with("invoke-raw-") {
            let request = request_command(&fixture["request"]);
            match api.invoke(&addr, request, deadline).await {
                Ok(reply) => format!(
                    "rawCode={};remark={}",
                    reply.code,
                    reply.remark.unwrap_or_else(|| "null".to_owned())
                ),
                Err(error) => outcome_error(error),
            }
        } else {
            let request = request_command(&fixture["request"]);
            api.invoke_oneway(&addr, request, deadline)
                .await
                .map_or_else(outcome_error, |_| "ok".to_owned())
        };

        assert_eq!(outcome, fixture["outcome"].as_str().unwrap(), "{name}");
        tokio::time::timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
        client.shutdown().await;
    }
}
