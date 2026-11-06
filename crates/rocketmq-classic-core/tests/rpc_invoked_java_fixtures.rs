use std::{fs, path::PathBuf, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_core::{
    api::{
        MQClientApi,
        producer::{SendKind, SendMode, SendStatus},
    },
    error::RpcError,
};
use rocketmq_classic_protocol::{
    codes::LanguageCode,
    header::BoundaryType,
    header::{
        DeleteTopicFromNamesrvRequestHeader, GetConsumeStatsRequestHeader,
        QueryConsumerOffsetRequestHeader, SearchOffsetRequestHeader, SendMessageRequestHeaderV1,
        UpdateConsumerOffsetRequestHeader,
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
        // Fixture bodies are base64; avoid adding a production dependency for test-only decoding.
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut result = Vec::new();
        let mut bits = 0u32;
        let mut count = 0;
        for char in encoded.bytes().take_while(|char| *char != b'=') {
            let digit = alphabet
                .iter()
                .position(|item| *item == char)
                .expect("base64 digit");
            bits = (bits << 6) | digit as u32;
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

async fn scripted(
    fixture: Value,
    route: bool,
) -> (
    MQClientApi,
    RemotingClient,
    String,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut len = [0; 4];
        stream.read_exact(&mut len).await.unwrap();
        let mut frame_bytes = BytesMut::from(&len[..]);
        frame_bytes.resize(u32::from_be_bytes(len) as usize + 4, 0);
        stream.read_exact(&mut frame_bytes[4..]).await.unwrap();
        let incoming = frame::decode_next(&mut frame_bytes).unwrap().unwrap();
        let expected = &fixture["request"];
        assert_eq!(incoming.code, expected["code"].as_i64().unwrap() as i32);
        assert_eq!(incoming.version, 515, "pinned Java MQVersion.V5_5_1");
        assert_eq!(
            incoming.ext_fields.as_ref().unwrap(),
            &serde_json::from_value::<rocketmq_classic_protocol::header::HeaderMap>(
                expected["ext"].clone(),
            )
            .unwrap()
        );
        assert_eq!(
            incoming.body.as_ref(),
            bytes(&expected["bodyBase64"]).as_ref()
        );
        if fixture.get("requestLanguage").is_some() {
            assert_eq!(incoming.language, Some(LanguageCode::Cpp));
        }
        let oneway = fixture["mode"] == "ONEWAY";
        assert_eq!(incoming.is_oneway(), oneway);
        if oneway {
            assert!(fixture["response"].is_null());
            return;
        }
        assert_eq!(fixture["mode"], "SYNC");
        let expected = &fixture["response"];
        let mut reply = RemotingCommand {
            code: expected["code"].as_i64().unwrap() as i32,
            opaque: incoming.opaque,
            ext_fields: Some(serde_json::from_value(expected["ext"].clone()).unwrap()),
            body: bytes(&expected["bodyBase64"]),
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
    });
    let client = RemotingClient::new(TransportMode::Plain);
    if route {
        client.update_name_server_address_list(vec![addr.clone()]);
    }
    client.start().unwrap();
    (MQClientApi::new(client.clone()), client, addr, peer)
}

#[tokio::test]
async fn java_invoked_cases_match_rust_requests_and_outcomes() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rpc-invoked");
    let entries: Vec<_> = fs::read_dir(&dir).unwrap().collect();
    assert_eq!(
        entries.len(),
        29,
        "all pinned Java invoked cases must be compared"
    );
    for name in [
        "route-success",
        "route-not-found",
        "route-not-found-explicit-false",
        "route-unknown",
        "route-no-body",
        "route-invalid-json",
        "send-success",
        "send-full-overload",
        "send-unknown",
        "send-no-header",
        "offset-success",
        "offset-not-found",
        "offset-unknown",
        "offset-no-header",
        "search-offset-deprecated",
        "search-offset-default-boundary",
        "reset-offset-cpp",
        "reset-offset-explicit",
        "reset-offset-default",
        "consume-stats-topic-list",
        "consume-stats-default",
        "consume-stats-list",
        "consume-stats-topic",
        "delete-topic-nameserver",
        "delete-topic-nameserver-cluster",
        "update-success",
        "update-error",
        "update-unknown",
        "update-oneway",
    ] {
        let fixture: Value =
            serde_json::from_slice(&fs::read(dir.join(format!("{name}.json"))).unwrap()).unwrap();
        assert_eq!(fixture["case"], name);
        assert_eq!(
            fixture["provenance"]["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(fixture["provenance"]["serverBaselineContacted"], false);
        let (api, client, addr, peer) = scripted(fixture.clone(), name.starts_with("route-")).await;
        let deadline = Instant::now() + Duration::from_secs(3);
        let outcome = match name {
            name if name.starts_with("route-") => {
                match api.get_topic_route("oracle-topic", deadline).await {
                    Ok(Some(route)) => {
                        format!("orderTopicConf={}", route.order_topic_conf.unwrap())
                    }
                    Ok(None) => "null".into(),
                    Err(RpcError::Response { code, .. }) => {
                        format!("MQClientException;code={code}")
                    }
                    Err(RpcError::MalformedResponse { .. }) => {
                        if name == "route-invalid-json" {
                            "JsonParseException;malformed".into()
                        } else {
                            "MQClientException;code=0;malformed".into()
                        }
                    }
                    other => panic!("unexpected route result: {other:?}"),
                }
            }
            name if name.starts_with("send-") => {
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
                    __phase18_header_0.set_unit_mode(Some(false));
                    __phase18_header_0.set_batch(Some(false));
                    __phase18_header_0
                };
                match api
                    .send_message(
                        &addr,
                        "broker",
                        &header,
                        b"payload".to_vec(),
                        "unused",
                        SendKind::V2,
                        SendMode::Sync,
                        deadline,
                    )
                    .await
                {
                    Ok(Some(result)) => {
                        assert_eq!(result.status, SendStatus::SendOk);
                        format!(
                            "SEND_OK;queueOffset={};offsetMsgId={}",
                            result.queue_offset,
                            result.offset_msg_id.as_deref().unwrap_or("null")
                        )
                    }
                    Err(RpcError::Response { code, .. }) => {
                        format!("MQBrokerException;code={code}")
                    }
                    Err(RpcError::MalformedResponse { .. }) => {
                        "NullPointerException;malformed".into()
                    }
                    other => panic!("unexpected send result: {other:?}"),
                }
            }
            name if name.starts_with("update-") => {
                let mut header = UpdateConsumerOffsetRequestHeader::default();
                header.set_consumer_group(Some("consumer".into()));
                header.set_topic(Some("oracle-topic".into()));
                header.set_queue_id(Some(2));
                header.set_commit_offset(Some(42));
                match api
                    .update_consumer_offset(&addr, &header, name == "update-oneway", deadline)
                    .await
                {
                    Ok(()) => "ok".into(),
                    Err(RpcError::Response { code, .. }) => {
                        format!("MQBrokerException;code={code}")
                    }
                    other => panic!("unexpected update result: {other:?}"),
                }
            }
            name if name.starts_with("offset-") => {
                let mut header = QueryConsumerOffsetRequestHeader::default();
                header.set_consumer_group(Some("consumer".into()));
                header.set_topic(Some("oracle-topic".into()));
                header.set_queue_id(Some(2));
                match api.query_consumer_offset(&addr, &header, deadline).await {
                    Ok(Some(offset)) => format!("offset={offset}"),
                    Ok(None) => "OffsetNotFoundException;code=22".into(),
                    Err(RpcError::Response { code, .. }) => {
                        if code == 22 {
                            format!("OffsetNotFoundException;code={code}")
                        } else {
                            format!("MQBrokerException;code={code}")
                        }
                    }
                    Err(RpcError::MalformedResponse { .. }) => {
                        "NullPointerException;malformed".into()
                    }
                    other => panic!("unexpected offset result: {other:?}"),
                }
            }
            "search-offset-deprecated" | "search-offset-default-boundary" => {
                let mut header = SearchOffsetRequestHeader::default();
                header.set_topic(Some("oracle-topic".into()));
                header.set_queue_id(Some(2));
                header.set_timestamp(Some(-7));
                if name == "search-offset-default-boundary" {
                    header.set_boundary_type(Some(BoundaryType::Lower));
                    header.set_broker_name(Some("broker".into()));
                }
                format!(
                    "offset={}",
                    api.search_offset(&addr, &header, deadline).await.unwrap()
                )
            }
            "reset-offset-cpp" => {
                match api
                    .invoke_broker_to_reset_offset_for_client(
                        &addr,
                        "oracle-topic",
                        "workers",
                        1_712_345_678_901,
                        true,
                        true,
                        deadline,
                    )
                    .await
                {
                    Ok(Some(offsets)) => {
                        assert!(offsets.snapshot().is_empty());
                        "ok".to_owned()
                    }
                    other => panic!("unexpected reset offset result: {other:?}"),
                }
            }
            "reset-offset-explicit" => {
                match api
                    .invoke_broker_to_reset_offset_at_offset(
                        &addr,
                        "oracle-topic",
                        "workers",
                        1_712_345_678_901,
                        3,
                        Some(27),
                        deadline,
                    )
                    .await
                {
                    Ok(Some(offsets)) => {
                        let offsets = offsets.snapshot();
                        assert_eq!(offsets.len(), 1);
                        assert_eq!(offsets[0].1, Some(27));
                        "offset=27".to_owned()
                    }
                    other => panic!("unexpected explicit reset offset result: {other:?}"),
                }
            }
            "reset-offset-default" => {
                match api
                    .invoke_broker_to_reset_offset_for_client(
                        &addr,
                        "oracle-topic",
                        "workers",
                        1_712_345_678_901,
                        true,
                        false,
                        deadline,
                    )
                    .await
                {
                    Ok(Some(offsets)) => {
                        assert!(offsets.snapshot().is_empty());
                        "ok".to_owned()
                    }
                    other => panic!("unexpected default reset offset result: {other:?}"),
                }
            }
            name if name.starts_with("consume-stats-") => {
                let mut header = GetConsumeStatsRequestHeader::default();
                header.set_consumer_group(Some("workers".into()));
                match name {
                    "consume-stats-topic-list" => {
                        header.set_topic(Some("orders".into()));
                        header.update_topic_list(Some(&[Some("orders".into()), None]));
                    }
                    "consume-stats-list" => {
                        header.update_topic_list(Some(&[
                            Some("orders".into()),
                            Some("payments".into()),
                        ]));
                    }
                    "consume-stats-topic" => header.set_topic(Some("orders".into())),
                    "consume-stats-default" => {}
                    _ => panic!("unexpected consume stats overload: {name}"),
                }
                match api.get_consume_stats_sync(&addr, &header, deadline).await {
                    Ok(Some(stats)) => {
                        assert!(
                            stats
                                .get_offset_table()
                                .unwrap()
                                .with(|offsets| offsets.is_empty())
                        );
                        "ok".to_owned()
                    }
                    other => panic!("unexpected consume stats result: {other:?}"),
                }
            }
            name if name.starts_with("delete-topic-nameserver") => {
                let mut header = DeleteTopicFromNamesrvRequestHeader::default();
                header.set_topic(Some("oracle-topic".into()));
                if name == "delete-topic-nameserver-cluster" {
                    header.set_cluster_name(Some("cluster-a".into()));
                }
                api.delete_topic_in_nameserver(&addr, &header, deadline)
                    .await
                    .unwrap();
                "ok".to_owned()
            }
            _ => panic!("unexpected case {name}"),
        };
        assert_eq!(outcome, fixture["outcome"], "{name}");
        tokio::time::timeout(Duration::from_secs(3), peer)
            .await
            .expect("scripted peer timed out")
            .expect("scripted peer request comparison failed");
        client.shutdown().await;
    }
}
