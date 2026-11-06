use std::{fs, path::PathBuf, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_core::{api::MQClientApi, error::RpcError};
use rocketmq_classic_protocol::{
    codes::{request, response},
    header::{
        BoundaryType, CommandHeader, GetEarliestMsgStoretimeRequestHeader,
        GetMaxOffsetRequestHeader, GetMinOffsetRequestHeader, HeaderMap, SearchOffsetRequestHeader,
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

const CASES: [&str; 6] = [
    "search-offset-lower-empty",
    "search-offset-upper",
    "max-offset-committed-empty",
    "min-offset",
    "earliest-storetime-signed",
    "search-offset-error",
];

fn fixture(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/rpc-invoked-offset-boundaries")
        .join(format!("{name}.json"));
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

fn search_header(queue: &MessageQueue, request: &Value) -> SearchOffsetRequestHeader {
    let mut header = SearchOffsetRequestHeader::default();
    header.set_topic(Some(queue.topic().unwrap().to_owned()));
    header.set_queue_id(Some(queue.queue_id()));
    header.set_timestamp(Some(
        request["timestamp"].as_str().unwrap().parse().unwrap(),
    ));
    header.set_boundary_type(Some(
        BoundaryType::value_of_name(request["boundaryType"].as_str()).unwrap(),
    ));
    header.set_broker_name(Some("broker".into()));
    header
}

fn max_offset_header(queue: &MessageQueue, request: &Value) -> GetMaxOffsetRequestHeader {
    let mut header = GetMaxOffsetRequestHeader::default();
    header.set_topic(Some(queue.topic().unwrap().to_owned()));
    header.set_queue_id(Some(queue.queue_id()));
    header.set_committed(request["committed"] == "true");
    header.set_broker_name(Some("broker".into()));
    header
}

fn min_offset_header(queue: &MessageQueue) -> GetMinOffsetRequestHeader {
    let mut header = GetMinOffsetRequestHeader::default();
    header.set_topic(Some(queue.topic().unwrap().to_owned()));
    header.set_queue_id(Some(queue.queue_id()));
    header.set_broker_name(Some("broker".into()));
    header
}

fn earliest_storetime_header(queue: &MessageQueue) -> GetEarliestMsgStoretimeRequestHeader {
    let mut header = GetEarliestMsgStoretimeRequestHeader::default();
    header.set_topic(Some(queue.topic().unwrap().to_owned()));
    header.set_queue_id(Some(queue.queue_id()));
    header.set_broker_name(Some("broker".into()));
    header
}

async fn read_command(stream: &mut tokio::net::TcpStream) -> RemotingCommand {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes).unwrap().unwrap()
}

fn rpc_header(fixture: &Value) -> (i32, HeaderMap) {
    let request_fixture = &fixture["request"];
    let queue = MessageQueue::new("oracle-topic", "broker", 2);
    let mut fields = HeaderMap::new();
    match request_fixture["code"].as_i64().unwrap() as i32 {
        request::SEARCH_OFFSET_BY_TIMESTAMP => {
            search_header(&queue, &request_fixture["ext"]).encode_fields(&mut fields);
        }
        request::GET_MAX_OFFSET => {
            max_offset_header(&queue, &request_fixture["ext"]).encode_fields(&mut fields);
        }
        request::GET_MIN_OFFSET => min_offset_header(&queue).encode_fields(&mut fields),
        request::GET_EARLIEST_MSG_STORETIME => {
            earliest_storetime_header(&queue).encode_fields(&mut fields);
        }
        code => panic!("unexpected offset boundary request code {code}"),
    }
    (request_fixture["code"].as_i64().unwrap() as i32, fields)
}

async fn invoke(api: &MQClientApi, addr: &str, fixture: &Value) -> Result<i64, RpcError> {
    let queue = MessageQueue::new("oracle-topic", "broker", 2);
    let request_fixture = &fixture["request"];
    let deadline = Instant::now() + Duration::from_secs(3);
    match request_fixture["code"].as_i64().unwrap() as i32 {
        request::SEARCH_OFFSET_BY_TIMESTAMP => {
            let header = search_header(&queue, &request_fixture["ext"]);
            api.search_offset(addr, &header, deadline).await
        }
        request::GET_MAX_OFFSET => {
            let header = max_offset_header(&queue, &request_fixture["ext"]);
            api.get_max_offset(addr, &header, deadline).await
        }
        request::GET_MIN_OFFSET => {
            let header = min_offset_header(&queue);
            api.get_min_offset(addr, &header, deadline).await
        }
        request::GET_EARLIEST_MSG_STORETIME => {
            let header = earliest_storetime_header(&queue);
            api.get_earliest_msg_storetime(addr, &header, deadline)
                .await
        }
        code => panic!("unexpected offset boundary request code {code}"),
    }
}

async fn invoke_with_response_fields(
    fixture: &Value,
    response_fields: Option<HeaderMap>,
) -> Result<i64, RpcError> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let expected = fixture.clone();
    let response_fields = response_fields.clone();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let command = read_command(&mut stream).await;
        let (request_code, request_fields) = rpc_header(&expected);
        assert_eq!(command.code, request_code);
        assert_eq!(command.ext_fields.as_ref().unwrap(), &request_fields);
        assert!(command.body.is_none());
        assert!(!command.is_oneway());
        let mut reply = RemotingCommand {
            code: response::SUCCESS,
            opaque: command.opaque,
            ext_fields: response_fields.map(Into::into),
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
    let result = invoke(&api, &addr, fixture).await;
    tokio::time::timeout(Duration::from_secs(3), peer)
        .await
        .unwrap()
        .unwrap();
    client.shutdown().await;
    result
}

#[tokio::test]
async fn java_invoked_offset_boundary_requests_and_outcomes_match() {
    for name in CASES {
        let fixture = fixture(name);
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
            let command = read_command(&mut stream).await;
            assert_eq!(
                command.code,
                expected["request"]["code"].as_i64().unwrap() as i32
            );
            assert_eq!(
                command.ext_fields.as_ref().unwrap(),
                &serde_json::from_value::<rocketmq_classic_protocol::header::HeaderMap>(
                    expected["request"]["ext"].clone(),
                )
                .unwrap()
            );
            assert!(command.body.is_none());
            assert!(!command.is_oneway());
            let response_fixture = &expected["response"];
            let mut reply = RemotingCommand {
                code: response_fixture["code"].as_i64().unwrap() as i32,
                opaque: command.opaque,
                remark: response_fixture["remark"].as_str().map(str::to_owned),
                ext_fields: Some(serde_json::from_value(response_fixture["ext"].clone()).unwrap()),
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
        let (code, fields) = rpc_header(&fixture);
        assert_eq!(code, fixture["request"]["code"].as_i64().unwrap() as i32);
        assert_eq!(
            fields,
            serde_json::from_value(fixture["request"]["ext"].clone()).unwrap(),
            "{name}"
        );
        let result = invoke(&api, &addr, &fixture).await;
        match fixture["outcome"].as_str().unwrap() {
            outcome if outcome.starts_with("offset=") => {
                assert_eq!(
                    result.unwrap().to_string(),
                    outcome.strip_prefix("offset=").unwrap()
                );
            }
            "timestamp=-1" => assert_eq!(result.unwrap(), -1),
            "MQBrokerException;code=1" => match result {
                Err(RpcError::Response {
                    request_code: request::SEARCH_OFFSET_BY_TIMESTAMP,
                    code: response::SYSTEM_ERROR,
                    remark: Some(remark),
                    ..
                }) => assert_eq!(remark, "offline"),
                other => panic!("unexpected Broker error result: {other:?}"),
            },
            outcome => panic!("unexpected fixture outcome {outcome}"),
        }
        tokio::time::timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
        client.shutdown().await;
    }
}

#[tokio::test]
async fn offset_boundary_rpc_unboxes_required_java_response_values() {
    for (case_name, field_name, request_code) in [
        (
            "search-offset-lower-empty",
            "offset",
            request::SEARCH_OFFSET_BY_TIMESTAMP,
        ),
        (
            "max-offset-committed-empty",
            "offset",
            request::GET_MAX_OFFSET,
        ),
        ("min-offset", "offset", request::GET_MIN_OFFSET),
        (
            "earliest-storetime-signed",
            "timestamp",
            request::GET_EARLIEST_MSG_STORETIME,
        ),
    ] {
        let fixture = fixture(case_name);
        assert_eq!(
            fixture["request"]["code"].as_i64(),
            Some(i64::from(request_code))
        );
        for response_fields in [
            None,
            Some(HeaderMap::new()),
            Some(HeaderMap::from([(
                field_name.to_owned(),
                "not-a-long".to_owned(),
            )])),
        ] {
            match invoke_with_response_fields(&fixture, response_fields).await {
                Err(RpcError::MalformedResponse {
                    request_code: actual_code,
                    reason,
                    ..
                }) => {
                    assert_eq!(actual_code, request_code, "{case_name}");
                    assert!(reason.contains(field_name), "{case_name}: {reason}");
                }
                other => panic!("{case_name}: expected malformed response, got {other:?}"),
            }
        }
    }
}
