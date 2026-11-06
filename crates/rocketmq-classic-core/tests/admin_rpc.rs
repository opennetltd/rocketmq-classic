use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use base64::Engine;
use bytes::BytesMut;
use rocketmq_classic_common::message::{
    MessageExt, MessageHostAddress, decode_many_compatible, encode_message_ext,
};
use rocketmq_classic_core::{
    api::{MQClientApi, admin::AdminQueryRequest},
    error::RpcError,
};
use rocketmq_classic_protocol::{
    SharedValue,
    body::{
        AclInfo, CheckRocksdbCqWriteResult, CheckRocksdbCqWriteStatus, ConsumeQueueData,
        QueryConsumeQueueResponseBody, TopicConfig, TopicConfigAndQueueMapping,
        TopicQueueMappingDetail, UserInfo,
    },
    codes::{request, response},
    header::{
        BoundaryType, CommandHeader, ConsumeMessageDirectlyResultRequestHeader,
        CreateTopicRequestHeader, DeleteKvConfigRequestHeader,
        DeleteSubscriptionGroupRequestHeader, DeleteTopicFromNamesrvRequestHeader,
        DeleteTopicRequestHeader, ExportRocksDBConfigType, GetConsumeStatsRequestHeader,
        GetConsumerConnectionListRequestHeader, GetConsumerRunningInfoRequestHeader,
        GetEarliestMsgStoretimeRequestHeader, GetMaxOffsetRequestHeader, GetMinOffsetRequestHeader,
        GetTopicStatsInfoRequestHeader, HeaderMap, QueryConsumeTimeSpanRequestHeader,
        QueryMessageRequestHeader, QueryMessageResponseHeader,
        QuerySubscriptionByConsumerRequestHeader, QueryTopicConsumeByWhoRequestHeader,
        QueryTopicsByConsumerRequestHeader, ResetOffsetRequestHeader, SearchOffsetRequestHeader,
        UpdateGroupForbiddenRequestHeader, ViewMessageRequestHeader,
    },
    heartbeat::HeartbeatData,
    heartbeat::SubscriptionData,
    route::TopicRouteData,
    subscription::{
        ExponentialRetryPolicy, GroupRetryPolicy, GroupRetryPolicyType, SimpleSubscriptionData,
        SubscriptionDataSet, SubscriptionGroupConfig,
    },
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RemotingClient, TransportMode},
    codec::frame,
};
use serde_json::{Value, json};
use std::net::{IpAddr, Ipv4Addr};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::Instant,
};

async fn read_command(stream: &mut tokio::net::TcpStream) -> RemotingCommand {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes).unwrap().unwrap()
}

async fn scripted_peer(
    code: i32,
    header: Option<HeaderMap>,
    body: Option<Vec<u8>>,
) -> (String, tokio::task::JoinHandle<RemotingCommand>) {
    scripted_peer_after(code, header, body, Duration::ZERO).await
}

async fn scripted_peer_after(
    code: i32,
    header: Option<HeaderMap>,
    body: Option<Vec<u8>>,
    response_delay: Duration,
) -> (String, tokio::task::JoinHandle<RemotingCommand>) {
    scripted_peer_after_with_remark(code, None, header, body, response_delay).await
}

async fn scripted_peer_with_remark(
    code: i32,
    remark: Option<String>,
    header: Option<HeaderMap>,
    body: Option<Vec<u8>>,
) -> (String, tokio::task::JoinHandle<RemotingCommand>) {
    scripted_peer_after_with_remark(code, remark, header, body, Duration::ZERO).await
}

async fn scripted_peer_after_with_remark(
    code: i32,
    remark: Option<String>,
    header: Option<HeaderMap>,
    body: Option<Vec<u8>>,
    response_delay: Duration,
) -> (String, tokio::task::JoinHandle<RemotingCommand>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        tokio::time::sleep(response_delay).await;
        let mut reply = RemotingCommand {
            code,
            opaque: request.opaque,
            remark,
            ext_fields: header.map(Into::into),
            body,
            ..RemotingCommand::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
        request
    });
    (addr, task)
}

async fn scripted_peer_sequence(
    responses: Vec<Value>,
) -> (String, tokio::task::JoinHandle<Vec<RemotingCommand>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut requests = Vec::with_capacity(responses.len());
        for response in responses {
            let request = read_command(&mut stream).await;
            let response_delay = response["delayMillis"].as_u64().unwrap_or_default();
            if response_delay > 0 {
                tokio::time::sleep(Duration::from_millis(response_delay)).await;
            }
            let ext_fields = response["extFields"].as_object().map(|_| {
                serde_json::from_value::<rocketmq_classic_remoting::ExtFields>(
                    response["extFields"].clone(),
                )
                .unwrap()
            });
            let body = response["body"]
                .as_str()
                .map(|body| body.as_bytes().to_vec());
            let mut reply = RemotingCommand {
                code: response["code"].as_i64().unwrap() as i32,
                opaque: request.opaque,
                remark: response["remark"].as_str().map(str::to_owned),
                ext_fields,
                body,
                ..RemotingCommand::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
            requests.push(request);
        }
        requests
    });
    (addr, task)
}

fn java_message_record(name: &str) -> Vec<u8> {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == name)
        .unwrap();
    base64::engine::general_purpose::STANDARD
        .decode(case["recordBase64"].as_str().unwrap())
        .unwrap()
}

async fn api() -> (MQClientApi, RemotingClient) {
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    (MQClientApi::new(client.clone()), client)
}

#[tokio::test]
async fn producer_diagnostic_rpcs_match_invoked_java_fixture() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    for (operation, request_code) in [
        (
            "getProducerConnectionList",
            request::GET_PRODUCER_CONNECTION_LIST,
        ),
        ("getAllProducerInfo", request::GET_ALL_PRODUCER_INFO),
        ("getBrokerRuntimeInfo", request::GET_BROKER_RUNTIME_INFO),
    ] {
        for case in fixture["producerDiagnosticsOperations"][operation]["cases"]
            .as_array()
            .unwrap()
        {
            let response_code = case["responseCode"].as_i64().unwrap() as i32;
            let response_body = case["responseBody"]
                .as_str()
                .map(|body| body.as_bytes().to_vec());
            let (addr, peer) = scripted_peer_with_remark(
                response_code,
                case["requests"][0]["responseRemark"]
                    .as_str()
                    .map(str::to_owned)
                    .or_else(|| case["errorRemark"].as_str().map(str::to_owned)),
                None,
                response_body,
            )
            .await;
            let (client_api, client) = api().await;
            let deadline = Instant::now() + Duration::from_secs(2);
            let actual = match operation {
                "getProducerConnectionList" => {
                    let group = case["producerGroup"].as_str();
                    client_api
                        .get_producer_connection_list_nullable(&addr, group, deadline)
                        .await
                        .map(|value| {
                            value.map(|value| {
                                serde_json::from_str(&value.java_to_json(false).unwrap()).unwrap()
                            })
                        })
                }
                "getAllProducerInfo" => client_api
                    .get_all_producer_info(&addr, deadline)
                    .await
                    .map(|value| {
                        value.map(|value| {
                            serde_json::from_str(&value.java_to_json(false).unwrap()).unwrap()
                        })
                    }),
                "getBrokerRuntimeInfo" => client_api
                    .get_broker_runtime_info(&addr, deadline)
                    .await
                    .map(|value| {
                        value.map(|value| {
                            serde_json::from_str(&value.java_to_json(false).unwrap()).unwrap()
                        })
                    }),
                _ => unreachable!(),
            };

            if response_code == response::SUCCESS {
                let actual = actual.unwrap().unwrap_or(Value::Null);
                let expected = case["responseBody"]
                    .as_str()
                    .map(|body| serde_json::from_str::<Value>(body).unwrap())
                    .unwrap_or(Value::Null);
                assert_eq!(actual, expected, "{operation}: {case}");
            } else {
                assert!(
                    matches!(
                        actual,
                        Err(RpcError::Response {
                            request_code: actual_code,
                        peer: error_peer,
                            code,
                            remark: Some(remark),
                            ..
                        }) if actual_code == request_code
                        && error_peer == addr
                            && code == response_code
                            && Some(remark.as_str()) == case["errorRemark"].as_str()
                    ),
                    "{operation}: {case}"
                );
            }

            let request = peer.await.unwrap();
            assert_eq!(request.code, request_code);
            assert_eq!(
                request.language,
                Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
            );
            assert!(request.body.is_none());
            let expected_fields = case["requests"][0]["extFields"]
                .as_object()
                .map(|_| case["requests"][0]["extFields"].clone())
                .unwrap_or_else(|| json!({}));
            let actual_fields = request
                .ext_fields
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({}));
            assert_eq!(actual_fields, expected_fields);
            client.shutdown().await;
        }
    }
}

async fn compare_batch_delete_fixture(
    api: &MQClientApi,
    fixture: &Value,
    operations_key: &str,
    request_code: i32,
    group_list: bool,
) {
    let cases = fixture[operations_key]["cases"].as_array().unwrap();
    for case in cases {
        let values = if case["values"].is_null() {
            None
        } else {
            Some(serde_json::from_value::<Vec<Option<String>>>(case["values"].clone()).unwrap())
        };
        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], "broker-a:10911");
        assert_eq!(expected_request["requestCode"], request_code);
        assert_eq!(expected_request["timeoutMillis"], 2345);
        assert_eq!(expected_request["extFieldsPresent"], false);
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(expected_request["requestBodyBase64"].as_str().unwrap())
            .unwrap();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let (addr, peer) = scripted_peer_with_remark(
            response_code,
            case["responseRemark"].as_str().map(str::to_owned),
            None,
            None,
        )
        .await;
        let deadline = Instant::now() + Duration::from_secs(2);
        let result = if group_list {
            api.delete_subscription_group_list_nullable(
                &addr,
                values.as_deref(),
                case["cleanOffset"].as_bool().unwrap(),
                deadline,
            )
            .await
        } else {
            api.delete_topic_in_broker_list_nullable(&addr, values.as_deref(), deadline)
                .await
        };
        if case["outcome"] == "returned" {
            result.unwrap();
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: actual_code,
                    peer: error_peer,
                    code,
                    remark: Some(remark),
                }) if actual_code == request_code
                    && error_peer == addr
                    && code == response_code
                    && case["responseRemark"].as_str() == Some(remark.as_str())
            ));
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request_code);
        assert_eq!(request.body.as_deref(), Some(expected_body.as_slice()));
        assert!(
            request
                .ext_fields
                .as_ref()
                .is_none_or(|fields| fields.is_empty())
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
}

fn java_query_message_response_header(
    timestamp: i64,
    phyoffset: i64,
) -> QueryMessageResponseHeader {
    let mut header = QueryMessageResponseHeader::default();
    header.set_index_last_update_timestamp(Some(timestamp));
    header.set_index_last_update_phyoffset(Some(phyoffset));
    header
}

fn java_reset_offset_header() -> ResetOffsetRequestHeader {
    {
        let mut __phase18_header_0 = ResetOffsetRequestHeader::default();
        __phase18_header_0.set_group("workers".into());
        __phase18_header_0.set_topic("orders".into());
        __phase18_header_0.set_queue_id(1);
        __phase18_header_0.set_offset(Some(9));
        __phase18_header_0.set_timestamp(1_712_345_678_901);
        __phase18_header_0.set_force(true);
        __phase18_header_0.set_namespace(Some("tenant".into()));
        __phase18_header_0.set_namespaced(Some(true));
        __phase18_header_0.set_broker_name(Some("broker-a".into()));
        __phase18_header_0.set_oneway(Some(false));
        __phase18_header_0
    }
}

async fn invoke_offset_operation(
    api: &MQClientApi,
    addr: &str,
    code: i32,
) -> Result<i64, RpcError> {
    let deadline = Instant::now() + Duration::from_secs(2);
    match code {
        request::SEARCH_OFFSET_BY_TIMESTAMP => {
            let mut header = SearchOffsetRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header.set_queue_id(Some(0));
            header.set_timestamp(Some(1_700_000_000_000));
            header.set_boundary_type(Some(BoundaryType::value_of_name(Some("LOWER")).unwrap()));
            api.search_offset(addr, &header, deadline).await
        }
        request::GET_MAX_OFFSET => {
            let mut header = GetMaxOffsetRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header.set_queue_id(Some(0));
            api.get_max_offset(addr, &header, deadline).await
        }
        request::GET_MIN_OFFSET => {
            let mut header = GetMinOffsetRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header.set_queue_id(Some(0));
            api.get_min_offset(addr, &header, deadline).await
        }
        request::GET_EARLIEST_MSG_STORETIME => {
            let mut header = GetEarliestMsgStoretimeRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header.set_queue_id(Some(0));
            api.get_earliest_msg_storetime(addr, &header, deadline)
                .await
        }
        _ => panic!("unexpected admin offset request code {code}"),
    }
}

#[tokio::test]
async fn create_topic_sends_java_request_code_and_header_fields() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["updateOrCreateTopic"]["success"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let header = {
        let mut __phase18_header_1 = CreateTopicRequestHeader::default();
        __phase18_header_1.set_topic(Some("orders".into()));
        __phase18_header_1.set_default_topic(Some("TBW102".into()));
        __phase18_header_1.set_read_queue_nums(Some(4));
        __phase18_header_1.set_write_queue_nums(Some(4));
        __phase18_header_1.set_perm(Some(6));
        __phase18_header_1.set_topic_filter_type(Some("SINGLE_TAG".into()));
        __phase18_header_1.set_topic_sys_flag(Some(3));
        __phase18_header_1.set_order(Some(false));
        __phase18_header_1.set_attributes(Some("+unit=true".into()));
        __phase18_header_1.set_force(Some(false));
        __phase18_header_1.set_namespace(Some("tenant".into()));
        __phase18_header_1.set_namespaced(Some(true));
        __phase18_header_1.set_broker_name(Some("broker-a".into()));
        __phase18_header_1.set_oneway(Some(false));
        __phase18_header_1
    };

    api.create_topic(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(
        command.code,
        expected["requestCode"].as_i64().unwrap() as i32
    );
    assert!(command.body.is_none());
    let actual_fields = command.ext_fields.as_ref().unwrap();
    let expected_fields = expected["requestExtFields"].as_object().unwrap();
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (key, value) in expected_fields {
        assert_eq!(
            actual_fields.get(key).map(String::as_str),
            value.as_str(),
            "field {key}"
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_update_topic_does_not_add_local_permission_validation() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["updateOrCreateTopic"]["invalidPermission"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let mut header = CreateTopicRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_perm(Some(8));
    api.mq_client_admin()
        .update_or_create_topic(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::UPDATE_AND_CREATE_TOPIC);
    assert_eq!(
        command.body,
        expected["requestBody"]
            .as_str()
            .map(|s| s.as_bytes().to_vec())
    );
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        expected["requestExtFields"]
    );
    client.shutdown().await;
}

#[tokio::test]
async fn create_topic_rejects_invalid_permission_before_network_io() {
    let (api, client) = api().await;
    let mut header = CreateTopicRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_perm(Some(8));
    let error = api
        .create_topic(
            "not-an-address",
            &header,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "CODE: 16  DESC: topicPermission value: 8 is invalid.\nFor more information, please visit the url, https://rocketmq.apache.org/docs/faq/"
    );
    assert!(matches!(
        &error,
        RpcError::ClientValidation {
            request_code: request::UPDATE_AND_CREATE_TOPIC,
            response_code: response::NO_PERMISSION,
            message,
        } if message == "topicPermission value: 8 is invalid."
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn update_broker_config_sends_java_properties_body() {
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let properties = HashMap::from([
        ("brokerPermission".to_owned(), "7".to_owned()),
        ("brokerRole".to_owned(), "ASYNC_MASTER".to_owned()),
    ]);
    api.update_broker_config(&addr, &properties, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::UPDATE_BROKER_CONFIG);
    assert_eq!(
        command.language,
        Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
    );
    assert!(command.ext_fields.is_none());
    let lines = String::from_utf8(command.body.unwrap())
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        lines,
        std::collections::HashSet::from([
            "brokerPermission=7".to_owned(),
            "brokerRole=ASYNC_MASTER".to_owned(),
        ])
    );
    client.shutdown().await;
}

#[tokio::test]
async fn get_broker_config_matches_java_properties_decoding_cases() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["getBrokerConfigApi"][0],
        "java.util.Properties org.apache.rocketmq.client.impl.MQClientAPIImpl.getBrokerConfig(java.lang.String,long)"
    );
    for case in fixture["brokerConfigProperties"].as_array().unwrap() {
        let input = case["input"].as_str().unwrap();
        if input.is_empty() {
            // An empty body is omitted from the RocketMQ frame and Java's
            // getBrokerConfig dereferences null before calling string2Properties.
            continue;
        }
        let (addr, peer) =
            scripted_peer(response::SUCCESS, None, Some(input.as_bytes().to_vec())).await;
        let (client_api, client) = api().await;
        let properties = client_api
            .get_broker_config(&addr, Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(properties.is_none(), case["isNull"], "input {input:?}");
        let expected = case["entries"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
            .collect::<HashMap<_, _>>();
        if let Some(properties) = properties {
            assert_eq!(properties, expected, "input {input:?}");
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::GET_BROKER_CONFIG);
        assert!(request.ext_fields.is_none());
        assert!(request.body.is_none());
        client.shutdown().await;
    }
}

#[tokio::test]
async fn get_broker_config_rejects_missing_body_and_preserves_broker_error() {
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (client_api, client) = api().await;
    assert!(matches!(
        client_api
            .get_broker_config(&addr, Instant::now() + Duration::from_secs(2))
            .await,
        Err(RpcError::MalformedResponse {
            request_code: request::GET_BROKER_CONFIG,
            peer,
            reason,
        }) if peer == addr && reason == "successful broker-config reply has no body"
    ));
    assert_eq!(peer.await.unwrap().code, request::GET_BROKER_CONFIG);
    client.shutdown().await;

    let (addr, peer) = scripted_peer_with_remark(
        response::SYSTEM_ERROR,
        Some("broker config unavailable".to_owned()),
        None,
        None,
    )
    .await;
    let (client_api, client) = api().await;
    assert!(matches!(
        client_api
            .get_broker_config(&addr, Instant::now() + Duration::from_secs(2))
            .await,
        Err(RpcError::Response {
            request_code: request::GET_BROKER_CONFIG,
            peer,
            code: response::SYSTEM_ERROR,
            remark: Some(remark),
        }) if peer == addr && remark == "broker config unavailable"
    ));
    assert_eq!(peer.await.unwrap().code, request::GET_BROKER_CONFIG);
    client.shutdown().await;
}

#[tokio::test]
async fn cold_data_flow_and_read_ahead_operations_match_java_fixture() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures = fixture["coldDataFlowAndReadAheadApi"].as_array().unwrap();
    for signature in [
        "void org.apache.rocketmq.client.impl.MQClientAPIImpl.updateColdDataFlowCtrGroupConfig(java.lang.String,java.util.Properties,long)",
        "void org.apache.rocketmq.client.impl.MQClientAPIImpl.removeColdDataFlowCtrGroupConfig(java.lang.String,java.lang.String,long)",
        "java.lang.String org.apache.rocketmq.client.impl.MQClientAPIImpl.getColdDataFlowCtrInfo(java.lang.String,long)",
        "java.lang.String org.apache.rocketmq.client.impl.MQClientAPIImpl.setCommitLogReadAheadMode(java.lang.String,java.lang.String,long)",
    ] {
        assert!(signatures.iter().any(|value| value == signature));
    }

    let cases = fixture["coldDataFlowAndReadAheadOperations"]
        .as_object()
        .unwrap();
    assert_eq!(cases["callsAfterEmptyUpdate"], 0);
    assert_eq!(cases["callsAfterNullAndEmptyRemove"], 2);
    assert_eq!(cases["updateOutcome"], "returned");
    assert_eq!(cases["updateErrorOutcome"], "MQBrokerException");
    assert_eq!(cases["updateErrorCode"], response::SYSTEM_ERROR);
    assert_eq!(cases["removeOutcome"], "returned");
    assert_eq!(cases["removeErrorOutcome"], "MQBrokerException");
    assert_eq!(cases["removeErrorCode"], response::SYSTEM_ERROR);
    assert_eq!(cases["infoValue"], "coldData=enabled\n");
    assert_eq!(
        cases.get("emptyInfoValue").cloned().unwrap_or(Value::Null),
        Value::Null
    );
    assert_eq!(
        cases
            .get("missingInfoValue")
            .cloned()
            .unwrap_or(Value::Null),
        Value::Null
    );
    assert_eq!(cases["infoErrorOutcome"], "MQBrokerException");
    assert_eq!(cases["infoErrorCode"], response::SYSTEM_ERROR);
    assert_eq!(cases["setModeResult"], "updated");
    assert_eq!(
        cases.get("emptyModeResult").cloned().unwrap_or(Value::Null),
        Value::Null
    );
    assert_eq!(
        cases.get("nullModeResult").cloned().unwrap_or(Value::Null),
        Value::Null
    );
    assert_eq!(cases["modeErrorOutcome"], "MQBrokerException");
    assert_eq!(cases["modeErrorCode"], response::SYSTEM_ERROR);

    let requests = cases["requests"].as_array().unwrap();
    assert_eq!(requests.len(), 12);
    let (client_api, client) = api().await;
    client_api
        .update_cold_data_flow_ctr_group_config(
            "unused",
            &HashMap::new(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    client_api
        .remove_cold_data_flow_ctr_group_config(
            "unused",
            None,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    client_api
        .remove_cold_data_flow_ctr_group_config(
            "unused",
            Some(""),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();

    for case in requests {
        let code = case["requestCode"].as_i64().unwrap() as i32;
        let body = case["body"].as_str().map(|value| value.as_bytes().to_vec());
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let response_body = case["responseBody"]
            .as_str()
            .map(|value| value.as_bytes().to_vec());
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, response_body)
                .await;
        assert!(case["address"].as_str().unwrap().ends_with(":10913"));
        let result = match code {
            request::UPDATE_COLD_DATA_FLOW_CTR_CONFIG => {
                let (key, value) = case["body"]
                    .as_str()
                    .unwrap()
                    .trim_end()
                    .split_once('=')
                    .unwrap();
                client_api
                    .update_cold_data_flow_ctr_group_config(
                        &addr,
                        &HashMap::from([(key.to_owned(), value.to_owned())]),
                        Instant::now() + Duration::from_secs(2),
                    )
                    .await
                    .map(|()| Value::String("returned".to_owned()))
            }
            request::REMOVE_COLD_DATA_FLOW_CTR_CONFIG => client_api
                .remove_cold_data_flow_ctr_group_config(
                    &addr,
                    case["body"].as_str(),
                    Instant::now() + Duration::from_secs(2),
                )
                .await
                .map(|()| Value::String("returned".to_owned())),
            request::GET_COLD_DATA_FLOW_CTR_INFO => client_api
                .get_cold_data_flow_ctr_info(&addr, Instant::now() + Duration::from_secs(2))
                .await
                .map(|value| value.map(Value::String).unwrap_or(Value::Null)),
            request::SET_COMMITLOG_READ_MODE => {
                let mode = case["extFields"]["READ_AHEAD_MODE"].as_str();
                client_api
                    .set_commit_log_read_ahead_mode(
                        &addr,
                        mode,
                        Instant::now() + Duration::from_secs(2),
                    )
                    .await
                    .map(|value| value.map(Value::String).unwrap_or(Value::Null))
            }
            _ => panic!("unexpected Broker config operation code {code}"),
        };
        let request = peer.await.unwrap();
        assert_eq!(request.code, code);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert_eq!(request.body.as_deref(), body.as_deref());
        let expected_ext = case["extFields"]
            .as_object()
            .filter(|fields| !fields.is_empty())
            .map(|fields| {
                fields
                    .iter()
                    .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
                    .collect::<HashMap<_, _>>()
            });
        let actual_ext = request.ext_fields.map(|fields| {
            fields
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<HashMap<_, _>>()
        });
        assert_eq!(actual_ext, expected_ext, "request code {code}");
        match (result, response_code) {
            (Ok(actual), response::SUCCESS) => {
                let expected = match code {
                    request::UPDATE_COLD_DATA_FLOW_CTR_CONFIG
                    | request::REMOVE_COLD_DATA_FLOW_CTR_CONFIG => Value::String("returned".into()),
                    request::GET_COLD_DATA_FLOW_CTR_INFO
                        if case["responseBody"]
                            .as_str()
                            .is_some_and(|body| !body.is_empty()) =>
                    {
                        case["responseBody"].clone()
                    }
                    request::GET_COLD_DATA_FLOW_CTR_INFO => Value::Null,
                    request::SET_COMMITLOG_READ_MODE
                        if case["responseRemark"]
                            .as_str()
                            .is_some_and(|remark| !remark.is_empty()) =>
                    {
                        case["responseRemark"].clone()
                    }
                    request::SET_COMMITLOG_READ_MODE => Value::Null,
                    _ => unreachable!(),
                };
                assert_eq!(actual, expected, "request code {code}");
            }
            (
                Err(RpcError::Response {
                    code: actual_code,
                    remark,
                    ..
                }),
                expected_code,
            ) => {
                assert_eq!(actual_code, expected_code);
                assert_eq!(remark, response_remark);
            }
            (result, expected_code) => panic!(
                "request code {code} result {result:?} disagrees with response {expected_code}"
            ),
        }
    }
    client.shutdown().await;
}

#[tokio::test]
async fn register_client_returns_success_boolean_and_matches_java_request() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures = fixture["registerClientApi"].as_array().unwrap();
    assert!(signatures.iter().any(|value| value
        == "boolean org.apache.rocketmq.client.impl.MQClientAPIImpl.registerClient(java.lang.String,org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData,long)"));
    let cases = fixture["registerClientOperations"].as_object().unwrap();
    assert_eq!(cases["success"], true);
    assert_eq!(cases["nonSuccess"], false);
    let requests = cases["requests"].as_array().unwrap();
    assert_eq!(requests.len(), 2);

    let mut heartbeat = HeartbeatData::default();
    heartbeat.set_client_id(Some("registration-client".to_owned()));
    let (client_api, client) = api().await;
    for case in requests {
        assert_eq!(case["address"], "127.0.0.1:10913");
        assert_eq!(case["requestCode"], request::HEART_BEAT);
        assert_eq!(case["timeoutMillis"], 4321);
        let body = case["body"].as_str().unwrap().as_bytes().to_vec();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let (addr, peer) = scripted_peer_with_remark(
            response_code,
            case["responseRemark"].as_str().map(str::to_owned),
            None,
            None,
        )
        .await;
        let result = client_api
            .register_client(&addr, &heartbeat, Instant::now() + Duration::from_secs(2))
            .await;
        assert_eq!(result.unwrap(), response_code == response::SUCCESS);

        let request = peer.await.unwrap();
        assert_eq!(request.code, request::HEART_BEAT);
        assert_eq!(request.body.as_deref(), Some(body.as_slice()));
        assert!(
            request
                .ext_fields
                .as_ref()
                .is_none_or(|fields| fields.is_empty())
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn topic_list_from_nameserver_matches_java_request_and_response() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures = fixture["topicListFromNameServerApi"].as_array().unwrap();
    assert!(signatures.iter().any(|value| value
        == "org.apache.rocketmq.remoting.protocol.body.TopicList org.apache.rocketmq.client.impl.MQClientAPIImpl.getTopicListFromNameServer(long)"));
    let cases = fixture["topicListFromNameServerOperations"]
        .as_object()
        .unwrap();
    assert_eq!(cases["topics"], json!(["orders", "payments"]));
    assert_eq!(cases["brokerAddr"], "broker-a:10911");
    assert_eq!(cases["errorOutcome"], "MQClientException");
    assert_eq!(cases["errorCode"], response::SYSTEM_ERROR);
    let requests = cases["requests"].as_array().unwrap();
    assert_eq!(requests.len(), 2);

    let (client_api, client) = api().await;
    for case in requests {
        assert_eq!(case["addressWasNull"], true);
        assert_eq!(
            case["requestCode"],
            request::GET_ALL_TOPIC_LIST_FROM_NAMESERVER
        );
        assert_eq!(case["extFieldsPresent"], false);
        assert_eq!(case["requestBodyPresent"], false);
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_body = case["responseBodyBase64"].as_str().map(|body| {
            base64::engine::general_purpose::STANDARD
                .decode(body)
                .unwrap()
        });
        let (addr, peer) = scripted_peer_with_remark(
            response_code,
            case["responseRemark"].as_str().map(str::to_owned),
            None,
            response_body,
        )
        .await;
        client.update_name_server_address_list(vec![addr.clone()]);

        let result = client_api
            .get_topic_list_from_nameserver(Instant::now() + Duration::from_secs(2))
            .await;
        if response_code == response::SUCCESS {
            let topic_list = result.unwrap();
            assert_eq!(
                topic_list.get_topic_list().unwrap().snapshot(),
                HashSet::from(["orders".to_owned(), "payments".to_owned()])
            );
            assert_eq!(topic_list.get_broker_addr(), Some("broker-a:10911"));
        } else {
            match result {
                Err(RpcError::Response {
                    request_code,
                    peer,
                    code,
                    remark: Some(remark),
                }) => {
                    assert_eq!(request_code, request::GET_ALL_TOPIC_LIST_FROM_NAMESERVER);
                    assert_eq!(peer, addr);
                    assert_eq!(code, response_code);
                    assert_eq!(remark, "nameserver unavailable");
                }
                other => panic!("unexpected NameServer result: {other:?}"),
            }
        }

        let request = peer.await.unwrap();
        assert_eq!(request.code, request::GET_ALL_TOPIC_LIST_FROM_NAMESERVER);
        assert!(request.body.is_none());
        assert!(
            request
                .ext_fields
                .as_ref()
                .is_none_or(|fields| fields.is_empty())
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn system_topic_list_composes_nameserver_and_broker_responses_like_java() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures = fixture["systemTopicListApi"].as_array().unwrap();
    assert!(signatures.iter().any(|value| value
        == "org.apache.rocketmq.remoting.protocol.body.TopicList org.apache.rocketmq.client.impl.MQClientAPIImpl.getSystemTopicList(long)"));
    let cases = fixture["systemTopicListOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 7);

    for case in cases {
        let calls = case["calls"].as_array().unwrap();
        assert!(matches!(calls.len(), 1 | 2));
        let name_server_body = case["nameServerResponseBodyBase64"].as_str().map(|body| {
            base64::engine::general_purpose::STANDARD
                .decode(body)
                .unwrap()
        });
        let (name_server_body, broker_peer) = if calls.len() == 2 {
            let (broker_addr, broker_peer) = scripted_peer_with_remark(
                case["brokerResponseCode"].as_i64().unwrap() as i32,
                case["brokerResponseRemark"].as_str().map(str::to_owned),
                None,
                case["brokerResponseBodyBase64"].as_str().map(|body| {
                    base64::engine::general_purpose::STANDARD
                        .decode(body)
                        .unwrap()
                }),
            )
            .await;
            let mut topic_list = rocketmq_classic_protocol::body::TopicList::java_decode(
                name_server_body.as_deref().unwrap(),
            )
            .unwrap();
            topic_list.set_broker_addr(Some(broker_addr));
            (Some(topic_list.java_encode().unwrap()), Some(broker_peer))
        } else {
            (name_server_body, None)
        };
        let (name_server_addr, name_server_peer) = scripted_peer_with_remark(
            case["nameServerResponseCode"].as_i64().unwrap() as i32,
            case["nameServerResponseRemark"].as_str().map(str::to_owned),
            None,
            name_server_body,
        )
        .await;
        let (client_api, client) = api().await;
        client.update_name_server_address_list(vec![name_server_addr]);
        let result = client_api
            .get_system_topic_list(Instant::now() + Duration::from_secs(2))
            .await;
        if case["outcome"] == "returned" {
            let actual_topics = result
                .unwrap()
                .get_topic_list()
                .map(|topics| topics.snapshot());
            let expected_topics = case["topics"].as_array().map(|topics| {
                topics
                    .iter()
                    .map(|topic| topic.as_str().unwrap().to_owned())
                    .collect::<HashSet<_>>()
            });
            assert_eq!(actual_topics, expected_topics, "case {}", case["name"]);
        } else if case["name"] == "name-server-missing-body" {
            assert!(matches!(
                result,
                Err(RpcError::MalformedResponse {
                    request_code: request::GET_SYSTEM_TOPIC_LIST_FROM_NS,
                    ..
                })
            ));
        } else {
            let expected_code = case["errorCode"].as_i64().unwrap() as i32;
            let expected_request_code = if calls.len() == 2 {
                request::GET_SYSTEM_TOPIC_LIST_FROM_BROKER
            } else {
                request::GET_SYSTEM_TOPIC_LIST_FROM_NS
            };
            assert!(
                matches!(
                    result,
                    Err(RpcError::Response {
                        request_code,
                        code,
                        ..
                    }) if request_code == expected_request_code && code == expected_code
                ),
                "case {}",
                case["name"]
            );
        }

        let name_server_call = &calls[0];
        assert_eq!(name_server_call["addressWasNull"], true);
        assert_eq!(
            name_server_call["requestCode"],
            request::GET_SYSTEM_TOPIC_LIST_FROM_NS
        );
        assert_eq!(name_server_call["timeoutMillis"], 2345);
        assert_eq!(name_server_call["extFieldsPresent"], false);
        assert_eq!(name_server_call["requestBodyPresent"], false);
        let request = name_server_peer.await.unwrap();
        assert_eq!(request.code, request::GET_SYSTEM_TOPIC_LIST_FROM_NS);
        assert!(request.body.is_none());
        assert!(
            request
                .ext_fields
                .as_ref()
                .is_none_or(|fields| fields.is_empty())
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );

        if let Some(broker_peer) = broker_peer {
            let broker_call = &calls[1];
            assert_eq!(broker_call["address"], "broker-a:10911");
            assert_eq!(
                broker_call["requestCode"],
                request::GET_SYSTEM_TOPIC_LIST_FROM_BROKER
            );
            assert_eq!(broker_call["timeoutMillis"], 2345);
            assert_eq!(broker_call["extFieldsPresent"], false);
            assert_eq!(broker_call["requestBodyPresent"], false);
            let request = broker_peer.await.unwrap();
            assert_eq!(request.code, request::GET_SYSTEM_TOPIC_LIST_FROM_BROKER);
            assert!(request.body.is_none());
            assert!(
                request
                    .ext_fields
                    .as_ref()
                    .is_none_or(|fields| fields.is_empty())
            );
            assert_eq!(
                request.language,
                Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
            );
        }
        client.shutdown().await;
    }
}

#[tokio::test]
async fn system_topic_list_from_broker_matches_java_request_and_response() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures = fixture["systemTopicListFromBrokerApi"].as_array().unwrap();
    assert!(signatures.iter().any(|value| value
        == "org.apache.rocketmq.remoting.protocol.body.TopicList org.apache.rocketmq.client.impl.MQClientAPIImpl.getSystemTopicListFromBroker(java.lang.String,long)"));
    let cases = fixture["systemTopicListFromBrokerOperations"]
        .as_object()
        .unwrap();
    assert_eq!(
        cases["topics"],
        json!(["rmq_sys_TRACE_DATA", "rmq_sys_TRANS_HALF_TOPIC"])
    );
    assert_eq!(cases["errorOutcome"], "MQClientException");
    assert_eq!(cases["errorCode"], response::SYSTEM_ERROR);
    assert_eq!(cases["missingBodyOutcome"], "MQClientException");
    assert_eq!(cases["missingBodyCode"], response::SUCCESS);
    let requests = cases["requests"].as_array().unwrap();
    assert_eq!(requests.len(), 3);

    let (client_api, client) = api().await;
    for case in requests {
        assert_eq!(case["address"], "broker-a:10911");
        assert_eq!(
            case["requestCode"],
            request::GET_SYSTEM_TOPIC_LIST_FROM_BROKER
        );
        assert_eq!(case["extFieldsPresent"], false);
        assert_eq!(case["requestBodyPresent"], false);
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_body = case["responseBodyBase64"].as_str().map(|body| {
            base64::engine::general_purpose::STANDARD
                .decode(body)
                .unwrap()
        });
        let body_present = response_body.is_some();
        let (addr, peer) = scripted_peer_with_remark(
            response_code,
            case["responseRemark"].as_str().map(str::to_owned),
            None,
            response_body,
        )
        .await;
        let result = client_api
            .get_system_topic_list_from_broker(&addr, Instant::now() + Duration::from_secs(2))
            .await;
        if response_code == response::SUCCESS && body_present {
            assert_eq!(
                result.unwrap().get_topic_list().unwrap().snapshot(),
                HashSet::from([
                    "rmq_sys_TRACE_DATA".to_owned(),
                    "rmq_sys_TRANS_HALF_TOPIC".to_owned(),
                ])
            );
        } else if response_code == response::SUCCESS {
            assert!(matches!(
                result,
                Err(RpcError::MalformedResponse {
                    request_code: request::GET_SYSTEM_TOPIC_LIST_FROM_BROKER,
                    peer: error_peer,
                    ..
                }) if error_peer == addr
            ));
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request::GET_SYSTEM_TOPIC_LIST_FROM_BROKER,
                    peer: error_peer,
                    code,
                    remark: Some(remark),
                }) if error_peer == addr
                    && code == response_code
                    && remark == "system topics unavailable"
            ));
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::GET_SYSTEM_TOPIC_LIST_FROM_BROKER);
        assert!(request.body.is_none());
        assert!(
            request
                .ext_fields
                .as_ref()
                .is_none_or(|fields| fields.is_empty())
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn create_subscription_group_matches_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures = fixture["createSubscriptionGroupApi"].as_array().unwrap();
    assert!(signatures.iter().any(|value| value
        == "void org.apache.rocketmq.client.impl.MQClientAPIImpl.createSubscriptionGroup(java.lang.String,org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig,long)"));
    let cases = fixture["createSubscriptionGroupOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 4);

    let (client_api, client) = api().await;
    for case in cases {
        let config = if case["config"].is_null() {
            None
        } else {
            Some(serde_json::from_value::<SubscriptionGroupConfig>(case["config"].clone()).unwrap())
        };
        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], "broker-a:10911");
        assert_eq!(
            expected_request["requestCode"],
            request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP
        );
        assert_eq!(expected_request["timeoutMillis"], 2345);
        assert_eq!(expected_request["extFieldsPresent"], false);
        let expected_body = expected_request["requestBodyBase64"]
            .as_str()
            .map(|encoded| {
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .unwrap()
            });
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let (addr, peer) = scripted_peer_with_remark(
            response_code,
            case["responseRemark"].as_str().map(str::to_owned),
            None,
            None,
        )
        .await;
        let result = client_api
            .create_subscription_group(
                &addr,
                config.as_ref(),
                Instant::now() + Duration::from_secs(2),
            )
            .await;
        if case["outcome"] == "returned" {
            result.unwrap();
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP,
                    peer: error_peer,
                    code,
                    remark: Some(remark),
                }) if error_peer == addr
                    && code == response_code
                    && remark == "subscription group rejected"
            ));
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP);
        assert_eq!(request.body, expected_body);
        assert!(
            request
                .ext_fields
                .as_ref()
                .is_none_or(|fields| fields.is_empty())
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn name_server_kv_config_operations_match_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures = fixture["kvConfigApi"].as_array().unwrap();
    assert_eq!(signatures.len(), 4);
    for signature in signatures {
        let signature = signature.as_str().unwrap();
        assert!(signature.contains("MQClientAPIImpl."));
        assert!(signature.contains("KV"));
    }

    let cases = fixture["kvConfigOperations"]["cases"].as_array().unwrap();
    for case in cases {
        let operation = case["operation"].as_str().unwrap();
        if !matches!(operation, "get" | "list") {
            continue;
        }
        let expected = &case["requests"][0];
        let response_code = expected["responseCode"].as_i64().unwrap() as i32;
        let response_remark = expected["responseRemark"].as_str().map(str::to_owned);
        let response_header = if operation == "get" && case["result"].is_string() {
            Some(HeaderMap::from([(
                "value".to_owned(),
                case["result"].as_str().unwrap().to_owned(),
            )]))
        } else {
            None
        };
        let response_body = if operation == "list" && case["result"].is_object() {
            Some(serde_json::to_vec(&case["result"]).unwrap())
        } else {
            None
        };
        let (addr, peer) = scripted_peer_with_remark(
            response_code,
            response_remark.clone(),
            response_header,
            response_body,
        )
        .await;
        let (client_api, client) = api().await;
        client.update_name_server_address_list(vec![addr.clone()]);
        let deadline = Instant::now() + Duration::from_secs(2);
        let result = if operation == "get" {
            client_api
                .get_kv_config_value(case["namespace"].as_str(), case["key"].as_str(), deadline)
                .await
                .map(|value| json!(value))
        } else {
            client_api
                .get_kv_list_by_namespace(case["namespace"].as_str(), deadline)
                .await
                .map(|table| serde_json::to_value(table).unwrap())
        };
        if response_code == response::SUCCESS {
            assert_eq!(result.unwrap(), case["result"], "{case}");
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    code,
                    remark,
                    ..
                }) if code == response_code && remark == response_remark
            ));
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, expected["requestCode"]);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert!(request.body.is_none());
        let actual_fields = request
            .ext_fields
            .map(serde_json::to_value)
            .transpose()
            .unwrap()
            .unwrap_or_else(|| json!({}));
        assert_eq!(actual_fields, expected["extFields"]);
        client.shutdown().await;
    }

    for case in cases {
        let operation = case["operation"].as_str().unwrap();
        if !matches!(operation, "put" | "delete") {
            continue;
        }
        let mut name_server_addrs = Vec::new();
        let mut peers = Vec::new();
        for expected in case["requests"].as_array().unwrap() {
            let response_code = expected["responseCode"].as_i64().unwrap() as i32;
            let (addr, peer) = scripted_peer_with_remark(
                response_code,
                expected["responseRemark"].as_str().map(str::to_owned),
                None,
                None,
            )
            .await;
            name_server_addrs.push(addr);
            peers.push((expected.clone(), peer));
        }
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        if !name_server_addrs.is_empty() {
            client.update_name_server_address_list(name_server_addrs.clone());
            client.with_name_server_address_list(|current| {
                *current.unwrap() = name_server_addrs.clone();
            });
        }
        let client_api = MQClientApi::new(client.clone());
        let deadline = Instant::now() + Duration::from_secs(3);
        let result = if operation == "put" {
            client_api
                .put_kv_config_value(
                    case["namespace"].as_str(),
                    case["key"].as_str(),
                    case["value"].as_str(),
                    deadline,
                )
                .await
        } else {
            client_api
                .delete_kv_config_value(case["namespace"].as_str(), case["key"].as_str(), deadline)
                .await
        };
        if case["outcome"] == "returned" {
            result.unwrap();
        } else {
            let last = case["requests"].as_array().unwrap().last().unwrap();
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    code,
                    remark: Some(remark),
                    ..
                }) if code == last["responseCode"].as_i64().unwrap() as i32
                    && remark == last["responseRemark"].as_str().unwrap()
            ));
        }
        for (expected, peer) in peers {
            let request = peer.await.unwrap();
            assert_eq!(request.code, expected["requestCode"]);
            assert_eq!(
                request.language,
                Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
            );
            assert!(request.body.is_none());
            let actual_fields = request
                .ext_fields
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({}));
            assert_eq!(actual_fields, expected["extFields"]);
        }
        client.shutdown().await;
    }
}

#[tokio::test]
async fn broker_maintenance_operations_match_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures = fixture["brokerMaintenanceApi"].as_array().unwrap();
    assert_eq!(signatures.len(), 3);
    let cases = fixture["brokerMaintenanceOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 6);
    for case in cases {
        let operation = case["operation"].as_str().unwrap();
        let expected = &case["requests"][0];
        let response_code = expected["responseCode"].as_i64().unwrap() as i32;
        let response_remark = expected["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, None).await;
        let (client_api, client) = api().await;
        let result = match operation {
            "cleanExpiredConsumeQueue" => {
                client_api
                    .clean_expired_consume_queue(&addr, Instant::now() + Duration::from_secs(2))
                    .await
            }
            "deleteExpiredCommitLog" => {
                client_api
                    .delete_expired_commit_log(&addr, Instant::now() + Duration::from_secs(2))
                    .await
            }
            "cleanUnusedTopicByAddr" => {
                client_api
                    .clean_unused_topic_by_addr(&addr, Instant::now() + Duration::from_secs(2))
                    .await
            }
            _ => panic!("unknown Java Broker maintenance operation: {operation}"),
        };
        if case["outcome"] == "returned" {
            assert_eq!(result.unwrap(), case["result"]);
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    code,
                    remark,
                    ..
                }) if code == response_code && remark == response_remark
            ));
        }
        let request = peer.await.unwrap();
        assert_eq!(
            request.code,
            expected["requestCode"].as_i64().unwrap() as i32
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert!(
            request
                .ext_fields
                .as_ref()
                .is_none_or(|fields| fields.is_empty())
        );
        assert!(request.body.is_none());
        client.shutdown().await;
    }
}

#[tokio::test]
async fn create_subscription_group_list_matches_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures = fixture["createSubscriptionGroupListApi"]
        .as_array()
        .unwrap();
    assert!(signatures.iter().any(|value| value
        == "void org.apache.rocketmq.client.impl.MQClientAPIImpl.createSubscriptionGroupList(java.lang.String,java.util.List,long)"));
    let cases = fixture["createSubscriptionGroupListOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 3);

    let (client_api, client) = api().await;
    for case in cases {
        let configs: Vec<SubscriptionGroupConfig> =
            serde_json::from_value(case["configs"].clone()).unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], "broker-a:10911");
        assert_eq!(
            expected_request["requestCode"],
            request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST
        );
        assert_eq!(expected_request["timeoutMillis"], 2345);
        assert_eq!(expected_request["extFieldsPresent"], false);
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(expected_request["requestBodyBase64"].as_str().unwrap())
            .unwrap();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let (addr, peer) = scripted_peer_with_remark(
            response_code,
            case["responseRemark"].as_str().map(str::to_owned),
            None,
            None,
        )
        .await;
        let result = client_api
            .create_subscription_group_list(
                &addr,
                &configs,
                Instant::now() + Duration::from_secs(2),
            )
            .await;
        if case["outcome"] == "returned" {
            result.unwrap();
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST,
                    peer: error_peer,
                    code,
                    remark: Some(remark),
                }) if error_peer == addr
                    && code == response_code
                    && remark == "subscription groups rejected"
            ));
        }
        let request = peer.await.unwrap();
        assert_eq!(
            request.code,
            request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST
        );
        assert_eq!(request.body.as_deref(), Some(expected_body.as_slice()));
        assert!(
            request
                .ext_fields
                .as_ref()
                .is_none_or(|fields| fields.is_empty())
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn subscription_group_config_lookup_matches_java_requests_and_results() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["subscriptionGroupConfigApi"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let cases = fixture["subscriptionGroupConfigOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 4);
    for case in cases {
        let expected = &case["requests"][0];
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let body = case["responseBodyBase64"].as_str().map(|encoded| {
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap()
        });
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, body.clone())
                .await;
        let (client_api, client) = api().await;
        let group = case["group"].as_str();
        let result = client_api
            .get_subscription_group_config_nullable(
                &addr,
                group,
                Instant::now() + Duration::from_secs(2),
            )
            .await;
        if case["outcome"] == "returned" {
            let actual = result.unwrap();
            assert_eq!(actual.is_none(), case["resultIsNull"]);
            if let Some(actual) = actual {
                let expected: Value = serde_json::from_slice(body.as_deref().unwrap()).unwrap();
                let actual: Value =
                    serde_json::from_str(&actual.java_to_json(false).unwrap()).unwrap();
                assert_eq!(actual, expected, "{}", case["name"]);
            }
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response { code, remark, .. })
                    if code == response_code && remark == response_remark
            ));
        }
        let request = peer.await.unwrap();
        assert_eq!(
            request.code,
            expected["requestCode"].as_i64().unwrap() as i32
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert_eq!(
            request
                .ext_fields
                .as_ref()
                .and_then(|fields| fields.get("group"))
                .map(String::as_str),
            group
        );
        assert!(request.body.is_none());
        client.shutdown().await;
    }
}

#[tokio::test]
async fn create_topic_list_matches_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures = fixture["createTopicListApi"].as_array().unwrap();
    assert!(signatures.iter().any(|value| value
        == "void org.apache.rocketmq.client.impl.MQClientAPIImpl.createTopicList(java.lang.String,java.util.List,long)"));
    let cases = fixture["createTopicListOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 5);

    let (client_api, client) = api().await;
    for case in cases {
        let configs = if case["configs"].is_null() {
            None
        } else {
            Some(
                serde_json::from_value::<Vec<Option<TopicConfig>>>(case["configs"].clone())
                    .unwrap(),
            )
        };
        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], "broker-a:10911");
        assert_eq!(
            expected_request["requestCode"],
            request::UPDATE_AND_CREATE_TOPIC_LIST
        );
        assert_eq!(expected_request["timeoutMillis"], 2345);
        assert_eq!(expected_request["extFieldsPresent"], false);
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(expected_request["requestBodyBase64"].as_str().unwrap())
            .unwrap();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let (addr, peer) = scripted_peer_with_remark(
            response_code,
            case["responseRemark"].as_str().map(str::to_owned),
            None,
            None,
        )
        .await;
        let result = client_api
            .create_topic_list_nullable(
                &addr,
                configs.as_deref(),
                Instant::now() + Duration::from_secs(2),
            )
            .await;
        if case["outcome"] == "returned" {
            result.unwrap();
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request::UPDATE_AND_CREATE_TOPIC_LIST,
                    peer: error_peer,
                    code,
                    remark: Some(remark),
                }) if error_peer == addr
                    && code == response_code
                    && remark == "topics rejected"
            ));
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::UPDATE_AND_CREATE_TOPIC_LIST);
        assert_eq!(request.body.as_deref(), Some(expected_body.as_slice()));
        assert!(
            request
                .ext_fields
                .as_ref()
                .is_none_or(|fields| fields.is_empty())
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn delete_topic_in_broker_list_matches_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures = fixture["deleteTopicInBrokerListApi"].as_array().unwrap();
    assert!(signatures.iter().any(|value| value
        == "void org.apache.rocketmq.client.impl.MQClientAPIImpl.deleteTopicInBrokerList(java.lang.String,java.util.List,long)"));
    let (client_api, client) = api().await;
    compare_batch_delete_fixture(
        &client_api,
        &fixture,
        "deleteTopicInBrokerListOperations",
        request::DELETE_TOPIC_IN_BROKER_LIST,
        false,
    )
    .await;
    client.shutdown().await;
}

#[tokio::test]
async fn delete_subscription_group_list_matches_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures = fixture["deleteSubscriptionGroupListApi"]
        .as_array()
        .unwrap();
    assert!(signatures.iter().any(|value| value
        == "void org.apache.rocketmq.client.impl.MQClientAPIImpl.deleteSubscriptionGroupList(java.lang.String,java.util.List,boolean,long)"));
    let (client_api, client) = api().await;
    compare_batch_delete_fixture(
        &client_api,
        &fixture,
        "deleteSubscriptionGroupListOperations",
        request::DELETE_SUBSCRIPTION_GROUP_LIST,
        true,
    )
    .await;
    client.shutdown().await;
}

#[test]
fn get_cluster_list_matches_java_immutable_empty_singleton_without_rpc() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["getClusterListApi"],
        json!([
            "java.util.Set org.apache.rocketmq.client.impl.MQClientAPIImpl.getClusterList(java.lang.String,long)"
        ])
    );
    let cases = fixture["getClusterListOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 3);
    let client = RemotingClient::new(TransportMode::Plain);
    let client_api = MQClientApi::new(client.clone());
    for case in cases {
        assert_eq!(case["outcome"], "returned");
        assert_eq!(case["resultClass"], "java.util.Collections$EmptySet");
        assert_eq!(case["size"], 0);
        assert_eq!(case["sameSingleton"], true);
        assert_eq!(case["mutationThrows"], true);
        assert!(case["requests"].as_array().unwrap().is_empty());
        let result = client_api.get_cluster_list(
            case["topic"].as_str(),
            Instant::now() + Duration::from_secs(1),
        );
        assert!(result.is_empty());
        assert!(std::ptr::eq(
            result,
            client_api.get_cluster_list(None, Instant::now() + Duration::from_secs(1))
        ));
    }
}

#[tokio::test]
async fn reset_master_flush_offset_matches_java_header_timeout_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["resetMasterFlushOffsetApi"],
        json!([
            "void org.apache.rocketmq.client.impl.MQClientAPIImpl.resetMasterFlushOffset(java.lang.String,long)"
        ])
    );
    let cases = fixture["resetMasterFlushOffsetOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 2);
    let (client_api, client) = api().await;
    for case in cases {
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) = scripted_peer_with_remark(response_code, remark, None, None).await;
        let offset = case["masterFlushOffset"].as_i64().unwrap();
        let result = client_api.reset_master_flush_offset(&addr, offset).await;
        if case["outcome"] == "returned" {
            assert!(result.is_ok());
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request::RESET_MASTER_FLUSH_OFFSET,
                    peer: error_peer,
                    code,
                    remark: Some(error_remark),
                }) if error_peer == addr
                    && code == response_code
                    && Some(error_remark.as_str()) == case["responseRemark"].as_str()
            ));
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::RESET_MASTER_FLUSH_OFFSET);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert_eq!(request.body, None);
        assert_eq!(request.ext_fields.as_ref().unwrap().len(), 1);
        assert_eq!(
            request.ext_fields.as_ref().unwrap()["masterFlushOffset"],
            offset.to_string()
        );
        let java_request = &case["requests"][0];
        assert_eq!(java_request["address"], "broker-a:10911");
        assert_eq!(
            java_request["requestCode"],
            request::RESET_MASTER_FLUSH_OFFSET
        );
        assert_eq!(java_request["timeoutMillis"], 3000);
        assert_eq!(
            java_request["extFields"]["masterFlushOffset"],
            offset.to_string()
        );
        assert!(java_request["requestBodyBase64"].is_null());
    }
    client.shutdown().await;
}

#[tokio::test]
async fn add_and_remove_broker_match_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["addBrokerApi"],
        json!([
            "void org.apache.rocketmq.client.impl.MQClientAPIImpl.addBroker(java.lang.String,java.lang.String,long)"
        ])
    );
    assert_eq!(
        fixture["removeBrokerApi"],
        json!([
            "void org.apache.rocketmq.client.impl.MQClientAPIImpl.removeBroker(java.lang.String,java.lang.String,java.lang.String,long,long)"
        ])
    );
    let cases = fixture["addRemoveBrokerOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 4);

    for case in cases {
        let expected_request = &case["requests"][0];
        let timeout_millis = case["timeoutMillis"].as_i64().unwrap();
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(expected_request["timeoutMillis"], timeout_millis);
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark, None, None).await;
        let (client_api, client) = api().await;
        let result = match case["operation"].as_str().unwrap() {
            "add" => {
                client_api
                    .add_broker(&addr, case["configPath"].as_str(), timeout_millis)
                    .await
            }
            "remove" => {
                client_api
                    .remove_broker(
                        &addr,
                        case["clusterName"].as_str(),
                        case["brokerName"].as_str(),
                        case["brokerId"].as_i64().unwrap(),
                        timeout_millis,
                    )
                    .await
            }
            operation => panic!("unexpected Broker management operation {operation}"),
        };
        if case["outcome"] == "returned" {
            result.unwrap();
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code,
                    peer: error_peer,
                    code,
                    remark: Some(error_remark),
                }) if request_code == expected_request["requestCode"].as_i64().unwrap() as i32
                    && error_peer == addr
                    && code == response_code
                    && Some(error_remark.as_str()) == case["responseRemark"].as_str()
            ));
        }

        let request = peer.await.unwrap();
        assert_eq!(request.code, expected_request["requestCode"]);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert!(request.body.is_none());
        let actual_fields = request
            .ext_fields
            .map(serde_json::to_value)
            .transpose()
            .unwrap()
            .unwrap_or_else(|| json!({}));
        assert_eq!(actual_fields, expected_request["extFields"]);
        client.shutdown().await;
    }
}

#[tokio::test]
async fn clone_group_offset_matches_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["cloneGroupOffsetApi"],
        json!([
            "void org.apache.rocketmq.client.impl.MQClientAPIImpl.cloneGroupOffset(java.lang.String,java.lang.String,java.lang.String,java.lang.String,boolean,long)"
        ])
    );
    let cases = fixture["cloneGroupOffsetOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 3);

    for case in cases {
        let expected_request = &case["requests"][0];
        let timeout_millis = case["timeoutMillis"].as_i64().unwrap();
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(expected_request["timeoutMillis"], timeout_millis);
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark, None, None).await;
        let (client_api, client) = api().await;
        let result = client_api
            .clone_group_offset(
                &addr,
                case["srcGroup"].as_str(),
                case["destGroup"].as_str(),
                case["topic"].as_str(),
                case["offline"].as_bool().unwrap(),
                timeout_millis,
            )
            .await;
        if case["outcome"] == "returned" {
            result.unwrap();
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code,
                    peer: error_peer,
                    code,
                    remark: Some(error_remark),
                }) if request_code == expected_request["requestCode"].as_i64().unwrap() as i32
                    && error_peer == addr
                    && code == response_code
                    && Some(error_remark.as_str()) == case["responseRemark"].as_str()
            ));
        }

        let request = peer.await.unwrap();
        assert_eq!(request.code, expected_request["requestCode"]);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert!(request.body.is_none());
        let actual_fields = request
            .ext_fields
            .map(serde_json::to_value)
            .transpose()
            .unwrap()
            .unwrap_or_else(|| json!({}));
        assert_eq!(actual_fields, expected_request["extFields"]);
        client.shutdown().await;
    }
}

#[tokio::test]
async fn invoke_broker_to_get_consumer_status_matches_java_fixture() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["invokeBrokerToGetConsumerStatusApi"],
        json!([
            "java.util.Map org.apache.rocketmq.client.impl.MQClientAPIImpl.invokeBrokerToGetConsumerStatus(java.lang.String,java.lang.String,java.lang.String,java.lang.String,long)"
        ])
    );
    let cases = fixture["invokeBrokerToGetConsumerStatusOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 4);

    for case in cases {
        let expected_request = &case["requests"][0];
        let timeout_millis = case["timeoutMillis"].as_i64().unwrap();
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(expected_request["timeoutMillis"], timeout_millis);
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let response_body = case["responseBody"]
            .as_str()
            .map(|body| body.as_bytes().to_vec());
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark, None, response_body).await;
        let (client_api, client) = api().await;
        let result = client_api
            .invoke_broker_to_get_consumer_status(
                &addr,
                case["topic"].as_str(),
                case["group"].as_str(),
                case["clientAddr"].as_str(),
                timeout_millis,
            )
            .await;
        if case["outcome"] == "returned" {
            let table = result.unwrap();
            let consumer_ids =
                table.map(|table| table.snapshot().keys().cloned().collect::<Vec<_>>());
            assert_eq!(
                serde_json::to_value(consumer_ids).unwrap(),
                case["consumerIds"]
            );
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code,
                    peer: error_peer,
                    code,
                    remark,
                }) if request_code == expected_request["requestCode"].as_i64().unwrap() as i32
                    && error_peer == addr
                    && code == response_code
                    && remark.as_deref() == case["responseRemark"].as_str()
            ));
        }

        let request = peer.await.unwrap();
        assert_eq!(request.code, expected_request["requestCode"]);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert!(request.body.is_none());
        let actual_fields = request
            .ext_fields
            .map(serde_json::to_value)
            .transpose()
            .unwrap()
            .unwrap_or_else(|| json!({}));
        assert_eq!(actual_fields, expected_request["extFields"]);
        client.shutdown().await;
    }
}

#[tokio::test]
async fn query_correction_offset_matches_java_fixture() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["queryCorrectionOffsetApi"],
        json!([
            "java.util.Map org.apache.rocketmq.client.impl.MQClientAPIImpl.queryCorrectionOffset(java.lang.String,java.lang.String,java.lang.String,java.util.Set,long)"
        ])
    );
    let cases = fixture["queryCorrectionOffsetOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 6);

    for case in cases {
        let expected_request = &case["requests"][0];
        let timeout_millis = case["timeoutMillis"].as_i64().unwrap();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let response_body = case["responseBody"]
            .as_str()
            .map(|body| body.as_bytes().to_vec());
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark, None, response_body).await;
        let (client_api, client) = api().await;
        let filter_groups = case["filterGroup"]
            .as_str()
            .map(|group| HashSet::from([group.to_owned()]));
        let result = client_api
            .query_correction_offset(
                &addr,
                case["topic"].as_str(),
                case["group"].as_str(),
                filter_groups.as_ref(),
                timeout_millis,
            )
            .await;
        if case["outcome"] == "returned" {
            let offsets = result.unwrap();
            let null_offset_present = offsets
                .as_ref()
                .is_some_and(|offsets| offsets.get(&5) == Some(&None));
            assert_eq!(null_offset_present, case["nullOffsetPresent"]);
            let normalized_offsets = offsets.map(|offsets| {
                offsets
                    .into_iter()
                    .filter_map(|(queue_id, offset)| offset.map(|offset| (queue_id, offset)))
                    .collect::<HashMap<_, _>>()
            });
            assert_eq!(
                serde_json::to_value(normalized_offsets).unwrap(),
                case["correctionOffsets"]
            );
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code,
                    peer: error_peer,
                    code,
                    remark,
                }) if request_code == expected_request["requestCode"].as_i64().unwrap() as i32
                    && error_peer == addr
                    && code == response_code
                    && remark.as_deref() == case["responseRemark"].as_str()
            ));
        }

        let request = peer.await.unwrap();
        assert_eq!(request.code, expected_request["requestCode"]);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert!(request.body.is_none());
        assert_eq!(
            request
                .ext_fields
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
        );
        client.shutdown().await;
    }
}

#[tokio::test]
async fn update_and_get_group_forbidden_matches_java_fixture() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["updateAndGetGroupForbiddenApi"],
        json!([
            "org.apache.rocketmq.remoting.protocol.subscription.GroupForbidden org.apache.rocketmq.client.impl.MQClientAPIImpl.updateAndGetGroupForbidden(java.lang.String,org.apache.rocketmq.remoting.protocol.header.UpdateGroupForbiddenRequestHeader,long)"
        ])
    );
    let cases = fixture["updateAndGetGroupForbiddenOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 4);

    for case in cases {
        let expected_request = &case["requests"][0];
        let timeout_millis = case["timeoutMillis"].as_i64().unwrap();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let response_body = case["responseBody"]
            .as_str()
            .map(|body| body.as_bytes().to_vec());
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark, None, response_body).await;
        let (client_api, client) = api().await;
        let mut header = UpdateGroupForbiddenRequestHeader::default();
        header.set_topic(case["topic"].as_str().map(str::to_owned));
        header.set_group(case["group"].as_str().map(str::to_owned));
        header.set_readable(case["readable"].as_bool());
        if case["withInheritedFields"].as_bool().unwrap() {
            header.set_namespace(Some("tenant".to_owned()));
            header.set_namespaced(Some(true));
            header.set_broker_name(Some("broker-a".to_owned()));
            header.set_oneway(Some(false));
            header.set_lo(Some(true));
        }
        let result = client_api
            .update_and_get_group_forbidden(&addr, &header, timeout_millis)
            .await;
        if case["outcome"] == "returned" {
            let forbidden = result.unwrap();
            if case["resultIsNull"].as_bool().unwrap() {
                assert!(forbidden.is_none());
            } else {
                let forbidden = forbidden.unwrap();
                assert_eq!(forbidden.get_topic(), case["resultTopic"].as_str());
                assert_eq!(forbidden.get_group(), case["resultGroup"].as_str());
                assert_eq!(forbidden.get_readable(), case["resultReadable"].as_bool());
            }
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code,
                    peer: error_peer,
                    code,
                    remark,
                }) if request_code == expected_request["requestCode"].as_i64().unwrap() as i32
                    && error_peer == addr
                    && code == response_code
                    && remark.as_deref() == case["responseRemark"].as_str()
            ));
        }

        let request = peer.await.unwrap();
        assert_eq!(request.code, expected_request["requestCode"]);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert!(request.body.is_none());
        assert_eq!(
            request
                .ext_fields
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
        );
        client.shutdown().await;
    }
}

#[tokio::test]
async fn get_broker_ha_status_matches_java_fixture() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["getBrokerHAStatusApi"],
        json!([
            "org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo org.apache.rocketmq.client.impl.MQClientAPIImpl.getBrokerHAStatus(java.lang.String,long)"
        ])
    );
    let cases = fixture["getBrokerHAStatusOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 4);

    for case in cases {
        let expected_request = &case["requests"][0];
        let timeout_millis = case["timeoutMillis"].as_i64().unwrap();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let response_body = case["responseBody"]
            .as_str()
            .map(|body| body.as_bytes().to_vec());
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark, None, response_body).await;
        let (client_api, client) = api().await;
        let result = client_api.get_broker_ha_status(&addr, timeout_millis).await;
        if case["outcome"] == "returned" {
            let status = result.unwrap();
            if case["resultIsNull"].as_bool().unwrap() {
                assert!(status.is_none());
            } else {
                let status = status.unwrap();
                assert_eq!(
                    status.java_to_json(false).unwrap(),
                    case["resultJson"].as_str().unwrap()
                );
                if case["name"] == "ha-populated" {
                    assert!(status.is_master());
                    assert_eq!(status.get_master_commit_log_max_offset(), 987_654_321);
                    assert_eq!(status.get_in_sync_slave_nums(), 2);
                    status
                        .get_ha_connection_info()
                        .unwrap()
                        .with(|connections| {
                            assert_eq!(connections.len(), 1);
                            let connection = connections[0].as_ref().unwrap();
                            connection.with(|connection| {
                                assert_eq!(connection.get_addr(), Some("10.0.0.2:10912"));
                                assert_eq!(connection.get_slave_ack_offset(), 987_654_300);
                                assert_eq!(connection.get_diff(), 21);
                                assert!(connection.is_in_sync());
                                assert_eq!(connection.get_transferred_byte_in_second(), 4096);
                                assert_eq!(connection.get_transfer_from_where(), 987_650_000);
                            });
                        });
                    status.get_ha_client_runtime_info().unwrap().with(|client| {
                        assert_eq!(client.get_master_addr(), Some("10.0.0.1:10912"));
                        assert_eq!(client.get_transferred_byte_in_second(), 2048);
                        assert_eq!(client.get_max_offset(), 987_654_321);
                        assert_eq!(client.get_last_read_timestamp(), 123_456_789);
                        assert_eq!(client.get_last_write_timestamp(), 123_456_790);
                        assert_eq!(client.get_master_flush_offset(), 987_654_320);
                    });
                }
            }
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code,
                    peer: error_peer,
                    code,
                    remark,
                }) if request_code == expected_request["requestCode"].as_i64().unwrap() as i32
                    && error_peer == addr
                    && code == response_code
                    && remark.as_deref() == case["responseRemark"].as_str()
            ));
        }

        let request = peer.await.unwrap();
        assert_eq!(request.code, expected_request["requestCode"]);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert!(request.ext_fields.is_none());
        assert!(request.body.is_none());
        client.shutdown().await;
    }
}

#[tokio::test]
async fn fetch_consume_stats_in_broker_matches_java_fixture() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["fetchConsumeStatsInBrokerApi"],
        json!([
            "org.apache.rocketmq.remoting.protocol.body.ConsumeStatsList org.apache.rocketmq.client.impl.MQClientAPIImpl.fetchConsumeStatsInBroker(java.lang.String,boolean,long)"
        ])
    );
    let cases = fixture["fetchConsumeStatsInBrokerOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 6);

    for case in cases {
        let expected_request = &case["requests"][0];
        let timeout_millis = case["timeoutMillis"].as_i64().unwrap();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let response_body = case["responseBody"]
            .as_str()
            .map(|body| body.as_bytes().to_vec());
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark, None, response_body).await;
        let (client_api, client) = api().await;
        let result = client_api
            .fetch_consume_stats_in_broker(
                &addr,
                case["isOrder"].as_bool().unwrap(),
                timeout_millis,
            )
            .await;
        if case["outcome"] == "returned" {
            let stats = result.unwrap();
            assert_eq!(
                stats.java_to_json(false).unwrap(),
                case["resultJson"].as_str().unwrap()
            );
            if case["name"] == "stats-populated" {
                assert_eq!(stats.get_broker_addr(), Some("broker-a:10911"));
                assert_eq!(stats.get_total_diff(), 15);
                assert_eq!(stats.get_total_inflight_diff(), 10);
                stats.get_consume_stats_list().unwrap().with(|groups| {
                    assert_eq!(groups.len(), 1);
                    let group = groups[0].as_ref().unwrap();
                    group.with(|group| {
                        let stats_list = group.get("workers").unwrap().as_ref().unwrap();
                        stats_list.with(|stats_list| {
                            assert_eq!(stats_list.len(), 1);
                            let stats = stats_list[0].as_ref().unwrap();
                            stats.with(|stats| {
                                assert_eq!(stats.get_consume_tps(), 3.5);
                                assert_eq!(stats.compute_total_diff().unwrap(), 15);
                                assert_eq!(stats.compute_inflight_total_diff().unwrap(), 10);
                            });
                        });
                    });
                });
            }
            if case["name"] == "stats-null-primitives" {
                assert_eq!(stats.get_total_diff(), 0);
                assert_eq!(stats.get_total_inflight_diff(), 0);
            }
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code,
                    peer: error_peer,
                    code,
                    remark,
                }) if request_code == expected_request["requestCode"].as_i64().unwrap() as i32
                    && error_peer == addr
                    && code == response_code
                    && remark.as_deref() == case["responseRemark"].as_str()
            ));
        }

        let request = peer.await.unwrap();
        assert_eq!(request.code, expected_request["requestCode"]);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert!(request.body.is_none());
        assert_eq!(
            request
                .ext_fields
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
        );
        client.shutdown().await;
    }
}

#[tokio::test]
async fn view_broker_stats_data_matches_java_fixture() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["viewBrokerStatsDataApi"],
        json!([
            "org.apache.rocketmq.remoting.protocol.body.BrokerStatsData org.apache.rocketmq.client.impl.MQClientAPIImpl.viewBrokerStatsData(java.lang.String,java.lang.String,java.lang.String,long)"
        ])
    );
    for (surface, methods) in [
        (
            "brokerStatsDataApi",
            vec![
                "getStatsMinute",
                "setStatsMinute",
                "getStatsHour",
                "setStatsHour",
                "getStatsDay",
                "setStatsDay",
            ],
        ),
        (
            "brokerStatsItemApi",
            vec![
                "getSum", "setSum", "getTps", "setTps", "getAvgpt", "setAvgpt",
            ],
        ),
    ] {
        let declared = fixture[surface]["declaredPublicMethods"]
            .as_array()
            .unwrap();
        for method in methods {
            assert!(declared.iter().any(|signature| {
                signature
                    .as_str()
                    .is_some_and(|signature| signature.contains(&format!(".{method}(")))
            }));
        }
    }
    let alias = SharedValue::new(rocketmq_classic_protocol::body::BrokerStatsItem::default());
    let mut stats_dto = rocketmq_classic_protocol::body::BrokerStatsData::default();
    stats_dto.set_stats_minute(Some(alias.clone()));
    assert!(stats_dto.get_stats_minute().unwrap().ptr_eq(&alias));
    let cases = fixture["viewBrokerStatsDataOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 6);

    for case in cases {
        let expected_request = &case["requests"][0];
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let response_body = case["responseBody"]
            .as_str()
            .map(|body| body.as_bytes().to_vec());
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark, None, response_body).await;
        let (client_api, client) = api().await;
        let result = client_api
            .view_broker_stats_data(
                &addr,
                case["statsName"].as_str(),
                case["statsKey"].as_str(),
                case["timeoutMillis"].as_i64().unwrap(),
            )
            .await;
        if case["outcome"] == "returned" {
            let stats = result.unwrap();
            if case["resultIsNull"] == true {
                assert!(stats.is_none());
            } else {
                let stats = stats.unwrap();
                assert_eq!(
                    serde_json::from_str::<Value>(&stats.java_to_json(false).unwrap()).unwrap(),
                    serde_json::from_str::<Value>(case["resultJson"].as_str().unwrap()).unwrap()
                );
                if case["name"] == "stats-populated" {
                    stats.get_stats_minute().unwrap().with(|minute| {
                        assert_eq!(minute.get_sum(), 1200);
                        assert_eq!(minute.get_tps(), 20.0);
                        assert_eq!(minute.get_avgpt(), 1.25);
                    });
                    stats.get_stats_hour().unwrap().with(|hour| {
                        assert_eq!(hour.get_sum(), 43200);
                        assert_eq!(hour.get_tps(), 12.0);
                        assert_eq!(hour.get_avgpt(), 1.5);
                    });
                    stats.get_stats_day().unwrap().with(|day| {
                        assert_eq!(day.get_sum(), 1_000_000);
                        assert_eq!(day.get_tps(), 11.574);
                        assert_eq!(day.get_avgpt(), 1.75);
                    });
                }
                if case["name"] == "stats-null-primitives" {
                    stats.get_stats_minute().unwrap().with(|minute| {
                        assert_eq!(minute.get_sum(), 0);
                        assert_eq!(minute.get_tps(), 0.0);
                        assert_eq!(minute.get_avgpt(), 0.0);
                    });
                }
            }
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code,
                    peer: error_peer,
                    code,
                    remark,
                }) if request_code == expected_request["requestCode"].as_i64().unwrap() as i32
                    && error_peer == addr
                    && code == response_code
                    && remark.as_deref() == case["responseRemark"].as_str()
            ));
        }

        let request = peer.await.unwrap();
        assert_eq!(request.code, expected_request["requestCode"]);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert!(request.body.is_none());
        assert_eq!(
            request
                .ext_fields
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
        );
        client.shutdown().await;
    }
}

#[tokio::test]
async fn view_broker_stats_data_honors_the_java_timeout_argument() {
    let (addr, peer) = scripted_peer_after(
        response::SUCCESS,
        None,
        Some(b"{}".to_vec()),
        Duration::from_millis(150),
    )
    .await;
    let (client_api, client) = api().await;
    let result = client_api
        .view_broker_stats_data(&addr, Some("SNDBYTES"), Some("orders@0"), 25)
        .await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn get_topic_config_matches_java_request_and_static_mapping_fixtures() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["getTopicConfigApi"],
        json!([
            "org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping org.apache.rocketmq.client.impl.MQClientAPIImpl.getTopicConfig(java.lang.String,java.lang.String,long)"
        ])
    );
    for (surface, expected_methods) in [
        (
            "getTopicConfigRequestHeaderApi",
            vec!["getTopic", "setTopic", "checkFields"],
        ),
        (
            "topicConfigAndQueueMappingApi",
            vec!["getMappingDetail", "setMappingDetail"],
        ),
        (
            "topicQueueMappingDetailApi",
            vec!["getHostedQueues", "setHostedQueues"],
        ),
        (
            "logicQueueMappingItemApi",
            vec![
                "getGen",
                "getQueueId",
                "getBname",
                "getLogicOffset",
                "getStartOffset",
                "getEndOffset",
                "getTimeOfStart",
                "getTimeOfEnd",
                "setGen",
                "setQueueId",
                "setBname",
                "setLogicOffset",
                "setStartOffset",
                "setEndOffset",
                "setTimeOfStart",
                "setTimeOfEnd",
            ],
        ),
    ] {
        let declared = fixture[surface]["declaredPublicMethods"]
            .as_array()
            .unwrap();
        for method in expected_methods {
            assert!(declared.iter().any(|signature| {
                signature
                    .as_str()
                    .is_some_and(|signature| signature.contains(&format!(".{method}(")))
            }));
        }
        assert_eq!(
            fixture[surface]["declaredPublicMethodCount"]
                .as_u64()
                .unwrap(),
            declared.len() as u64
        );
    }

    let mut response = TopicConfigAndQueueMapping::default();
    let detail =
        SharedValue::new(rocketmq_classic_protocol::body::TopicQueueMappingDetail::default());
    response.set_mapping_detail(Some(detail.clone()));
    assert!(response.get_mapping_detail().unwrap().ptr_eq(&detail));

    let cases = fixture["getTopicConfigOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 11);
    for case in cases {
        let expected_request = &case["requests"][0];
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let response_body = case["responseBody"]
            .as_str()
            .map(|body| body.as_bytes().to_vec());
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark, None, response_body).await;
        let (client_api, client) = api().await;
        let result = client_api
            .get_topic_config(
                &addr,
                case["topic"].as_str(),
                case["timeoutMillis"].as_i64().unwrap(),
            )
            .await;
        if case["outcome"] == "returned" {
            let config = result.unwrap();
            assert_eq!(config.is_none(), case["resultIsNull"]);
            if let Some(config) = config {
                if case["name"] != "config-null-primitives" {
                    assert_eq!(
                        config,
                        TopicConfigAndQueueMapping::java_decode(
                            case["resultJson"].as_str().unwrap().as_bytes()
                        )
                        .unwrap(),
                        "{}",
                        case["name"]
                    );
                }
                if case["name"] == "config-populated" {
                    assert_eq!(config.topic_name(), Some("orders"));
                    assert_eq!(config.read_queue_nums(), 8);
                    assert_eq!(config.write_queue_nums(), 6);
                    let detail = config.get_mapping_detail().unwrap();
                    detail.with(|detail| {
                        assert_eq!(
                            detail.topic_queue_mapping_info().topic.as_deref(),
                            Some("orders")
                        );
                        let hosted = detail.get_hosted_queues().unwrap();
                        hosted.with(|queues| {
                            let items = queues.get(&2).unwrap().as_ref().unwrap();
                            items.with(|items| {
                                let item = items[0].as_ref().unwrap();
                                assert_eq!(item.generation(), 1);
                                assert_eq!(item.queue_id(), 1);
                                assert_eq!(item.compute_max_static_queue_offset(), 160);
                            });
                        });
                    });
                }
                if case["name"] == "config-null-primitives" {
                    assert_eq!(config.read_queue_nums(), 0);
                    assert_eq!(config.write_queue_nums(), 0);
                    assert_eq!(config.perm(), 0);
                    assert!(config.attributes().is_none());
                }
                if case["name"] == "config-mapping-defaults" {
                    let detail = config.get_mapping_detail().unwrap();
                    detail.with(|detail| {
                        assert_eq!(
                            detail.topic_queue_mapping_info().scope.as_deref(),
                            Some("__global__")
                        );
                        assert!(
                            detail
                                .get_hosted_queues()
                                .unwrap()
                                .with(|queues| queues.is_empty())
                        );
                    });
                }
            }
        } else if case["outcome"] == "MQBrokerException" {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code,
                    peer: error_peer,
                    code,
                    remark,
                }) if request_code == expected_request["requestCode"].as_i64().unwrap() as i32
                    && error_peer == addr
                    && code == response_code
                    && remark.as_deref() == case["responseRemark"].as_str()
            ));
        } else {
            assert!(matches!(
                result,
                Err(RpcError::MalformedResponse { request_code, peer: error_peer, .. })
                    if request_code == request::GET_TOPIC_CONFIG && error_peer == addr
            ));
        }

        let request = peer.await.unwrap();
        assert_eq!(request.code, expected_request["requestCode"]);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert!(request.body.is_none());
        assert_eq!(
            request
                .ext_fields
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
        );
        client.shutdown().await;
    }
}

#[tokio::test]
async fn query_consume_queue_matches_java_request_and_response_fixtures() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["queryConsumeQueueApi"],
        json!([
            "org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody org.apache.rocketmq.client.impl.MQClientAPIImpl.queryConsumeQueue(java.lang.String,java.lang.String,int,long,int,java.lang.String,long)"
        ])
    );
    for (surface, expected_methods) in [
        (
            "queryConsumeQueueRequestHeaderApi",
            vec![
                "getTopic",
                "setTopic",
                "getQueueId",
                "setQueueId",
                "getIndex",
                "setIndex",
                "getCount",
                "setCount",
                "getConsumerGroup",
                "setConsumerGroup",
                "checkFields",
            ],
        ),
        (
            "queryConsumeQueueResponseBodyApi",
            vec![
                "getSubscriptionData",
                "setSubscriptionData",
                "getFilterData",
                "setFilterData",
                "getQueueData",
                "setQueueData",
                "getMaxQueueIndex",
                "setMaxQueueIndex",
                "getMinQueueIndex",
                "setMinQueueIndex",
            ],
        ),
        (
            "consumeQueueDataApi",
            vec![
                "getPhysicOffset",
                "setPhysicOffset",
                "getPhysicSize",
                "setPhysicSize",
                "getTagsCode",
                "setTagsCode",
                "getExtendDataJson",
                "setExtendDataJson",
                "getBitMap",
                "setBitMap",
                "isEval",
                "setEval",
                "getMsg",
                "setMsg",
                "toString",
            ],
        ),
    ] {
        let declared = fixture[surface]["declaredPublicMethods"]
            .as_array()
            .unwrap();
        for method in expected_methods {
            assert!(
                declared.iter().any(|signature| {
                    signature
                        .as_str()
                        .is_some_and(|signature| signature.contains(&format!(".{method}(")))
                }),
                "{surface} lacks {method}"
            );
        }
        assert_eq!(
            fixture[surface]["declaredPublicMethodCount"]
                .as_u64()
                .unwrap(),
            declared.len() as u64
        );
    }

    let cases = fixture["queryConsumeQueueOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 9);
    for case in cases
        .iter()
        .filter(|case| !case["vipEnabled"].as_bool().unwrap())
    {
        let name = case["name"].as_str().unwrap();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let response_body = case["responseBody"]
            .as_str()
            .map(|body| body.as_bytes().to_vec());
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark, None, response_body).await;
        let (client_api, client) = api().await;
        let result = client_api
            .query_consume_queue(
                &addr,
                case["topic"].as_str(),
                case["queueId"].as_i64().unwrap() as i32,
                case["index"].as_i64().unwrap(),
                case["count"].as_i64().unwrap() as i32,
                case["consumerGroup"].as_str(),
                case["timeoutMillis"].as_i64().unwrap(),
            )
            .await;
        if case["outcome"] == "returned" {
            let value = result.unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(value.is_none(), case["resultIsNull"]);
            if let Some(value) = value {
                let actual: Value =
                    serde_json::from_str(&value.java_to_json(false).unwrap()).unwrap();
                let expected: Value =
                    serde_json::from_str(case["resultJson"].as_str().unwrap()).unwrap();
                assert_eq!(actual, expected, "{name}");
                if name == "queue-populated" {
                    assert_eq!(value.get_filter_data(), Some("filter-data"));
                    assert_eq!(value.get_max_queue_index(), 20);
                    assert_eq!(value.get_min_queue_index(), 4);
                    let subscription = value.get_subscription_data().unwrap();
                    assert_eq!(
                        subscription
                            .with(|subscription| subscription.get_sub_string().map(str::to_owned)),
                        Some("*".to_owned())
                    );
                    let rows = value.get_queue_data().unwrap();
                    assert!(rows.with(|rows| rows.len() == 1));
                    let row = rows.with(|rows| rows[0].as_ref().unwrap().clone());
                    row.with(|row| {
                        assert_eq!(row.get_physic_offset(), 987_654_321);
                        assert_eq!(row.get_physic_size(), 128);
                        assert_eq!(row.get_tags_code(), 171_234);
                        assert_eq!(row.get_bit_map(), Some("1010"));
                        assert!(row.is_eval());
                        assert_eq!(row.get_msg(), Some("message-body"));
                    });
                }
                if name == "queue-defaults" {
                    assert!(value.get_subscription_data().is_none());
                    assert!(value.get_filter_data().is_none());
                    assert!(value.get_queue_data().is_none());
                    assert_eq!(value.get_max_queue_index(), 0);
                    assert_eq!(value.get_min_queue_index(), 0);
                }
                if name == "queue-null-fields-and-row" {
                    assert!(value.get_subscription_data().is_none());
                    assert!(value.get_filter_data().is_none());
                    assert_eq!(value.get_max_queue_index(), 0);
                    assert_eq!(value.get_min_queue_index(), 0);
                    let rows = value.get_queue_data().unwrap();
                    rows.with(|rows| {
                        assert_eq!(rows.len(), 2);
                        assert!(rows[0].is_none());
                        rows[1].as_ref().unwrap().with(|row| {
                            assert_eq!(row.get_physic_offset(), 0);
                            assert_eq!(row.get_physic_size(), 0);
                            assert_eq!(row.get_tags_code(), 0);
                            assert_eq!(row.get_extend_data_json(), None);
                            assert_eq!(row.get_bit_map(), None);
                            assert!(!row.is_eval());
                            assert_eq!(row.get_msg(), None);
                        });
                    });
                }
            }
        } else if case["outcome"] == "MQClientException" {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code,
                    peer: error_peer,
                    code,
                    remark,
                }) if request_code == request::QUERY_CONSUME_QUEUE
                    && error_peer == addr
                    && code == response_code
                    && remark.as_deref() == case["responseRemark"].as_str()
            ));
        } else {
            assert!(matches!(
                result,
                Err(RpcError::MalformedResponse { request_code, peer: error_peer, .. })
                    if request_code == request::QUERY_CONSUME_QUEUE && error_peer == addr
            ));
        }

        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], case["address"], "{name}");
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::QUERY_CONSUME_QUEUE, "{name}");
        assert_eq!(request.code, expected_request["requestCode"], "{name}");
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
            "{name}"
        );
        assert!(request.body.is_none(), "{name}");
        assert_eq!(
            request
                .ext_fields
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"],
            "{name}"
        );
        client.shutdown().await;
    }

    let mut response_body = QueryConsumeQueueResponseBody::default();
    let mut java_frame = BytesMut::from(
        &include_bytes!("../../../fixtures/remoting/body-query-consume-queue.bin")[..],
    );
    let java_command = frame::decode_next(&mut java_frame).unwrap().unwrap();
    assert_eq!(java_command.code, request::QUERY_CONSUME_QUEUE);
    let java_body =
        QueryConsumeQueueResponseBody::java_decode(java_command.body.as_deref().unwrap()).unwrap();
    assert_eq!(java_body.get_max_queue_index(), 20);
    assert_eq!(java_body.get_min_queue_index(), 4);
    assert_eq!(java_body.get_filter_data(), Some("filter-data"));
    assert_eq!(
        java_body
            .get_subscription_data()
            .unwrap()
            .with(|subscription| subscription.get_topic().map(str::to_owned)),
        Some("orders".into())
    );
    assert!(
        java_body
            .get_queue_data()
            .unwrap()
            .with(|rows| rows.len() == 1)
    );

    let subscription = SharedValue::new(SubscriptionData::default());
    response_body.set_subscription_data(Some(subscription.clone()));
    assert!(
        response_body
            .get_subscription_data()
            .unwrap()
            .ptr_eq(&subscription)
    );
    let mut row = ConsumeQueueData::default();
    row.set_physic_offset(987_654_321);
    row.set_physic_size(128);
    row.set_tags_code(171_234);
    row.set_extend_data_json(Some("{\"filterBitMap\":\"AQID\"}".into()));
    row.set_bit_map(Some("1010".into()));
    row.set_eval(true);
    row.set_msg(Some("message-body".into()));
    let row = SharedValue::new(row);
    let rows = SharedValue::new(vec![Some(row.clone())]);
    response_body.set_queue_data(Some(rows.clone()));
    assert!(response_body.get_queue_data().unwrap().ptr_eq(&rows));
    assert!(
        response_body
            .get_queue_data()
            .unwrap()
            .with(|items| items[0].as_ref().unwrap().ptr_eq(&row))
    );
    assert_eq!(
        row.with(|row| row.java_to_string()),
        fixture["queryConsumeQueueOperations"]["consumeQueueDataToString"]
    );
    row.with_mut(|row| row.set_msg(Some("changed".into())));
    assert_eq!(
        row.with(|row| row.get_msg().map(str::to_owned)),
        Some("changed".into())
    );
}

#[tokio::test]
async fn query_consume_queue_honors_the_java_timeout_argument() {
    let (addr, peer) = scripted_peer_after(
        response::SUCCESS,
        None,
        Some(b"{}".to_vec()),
        Duration::from_millis(150),
    )
    .await;
    let (client_api, client) = api().await;
    let result = client_api
        .query_consume_queue(&addr, Some("orders"), 2, 0, 16, Some("workers"), 25)
        .await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn get_topic_config_honors_the_java_timeout_argument() {
    let (addr, peer) = scripted_peer_after(
        response::SUCCESS,
        None,
        Some(b"{}".to_vec()),
        Duration::from_millis(150),
    )
    .await;
    let (client_api, client) = api().await;
    let result = client_api.get_topic_config(&addr, Some("orders"), 25).await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn create_static_topic_matches_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["createStaticTopicApi"],
        json!([
            "void org.apache.rocketmq.client.impl.MQClientAPIImpl.createStaticTopic(java.lang.String,java.lang.String,org.apache.rocketmq.common.TopicConfig,org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail,boolean,long)"
        ])
    );
    for (surface, required_methods) in [
        (
            "createStaticTopicRequestHeaderApi",
            vec![
                "getTopic",
                "setTopic",
                "getDefaultTopic",
                "setDefaultTopic",
                "getReadQueueNums",
                "setReadQueueNums",
                "getWriteQueueNums",
                "setWriteQueueNums",
                "getPerm",
                "setPerm",
                "getTopicFilterType",
                "setTopicFilterType",
                "getTopicSysFlag",
                "setTopicSysFlag",
                "getOrder",
                "setOrder",
                "getForce",
                "setForce",
                "checkFields",
            ],
        ),
        (
            "staticTopicConfigApi",
            vec![
                "getTopicName",
                "getReadQueueNums",
                "getWriteQueueNums",
                "getPerm",
                "getTopicFilterType",
                "getTopicSysFlag",
                "isOrder",
            ],
        ),
    ] {
        let methods = fixture[surface]["declaredPublicMethods"]
            .as_array()
            .unwrap();
        for method in required_methods {
            assert!(
                methods.iter().any(|signature| {
                    signature
                        .as_str()
                        .is_some_and(|signature| signature.contains(&format!(".{method}(")))
                }),
                "{surface} lacks {method}"
            );
        }
    }

    let cases = fixture["createStaticTopicOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 7);
    let (client_api, client) = api().await;
    for case in cases
        .iter()
        .filter(|case| !case["vipEnabled"].as_bool().unwrap())
    {
        let name = case["name"].as_str().unwrap();
        let topic_config =
            serde_json::from_str::<TopicConfig>(case["topicConfigJson"].as_str().unwrap()).unwrap();
        let mapping_detail = TopicQueueMappingDetail::java_decode(
            case["mappingDetailJson"].as_str().unwrap().as_bytes(),
        )
        .unwrap();
        if name == "populated" {
            assert_eq!(topic_config.topic_name(), Some("orders"));
            assert_eq!(topic_config.read_queue_nums(), 8);
            assert_eq!(topic_config.write_queue_nums(), 6);
            assert_eq!(topic_config.perm(), 7);
            assert_eq!(topic_config.topic_sys_flag(), 3);
            assert!(topic_config.is_order());
            assert_eq!(mapping_detail.get_topic(), Some("orders"));
            assert_eq!(mapping_detail.get_total_queues(), 4);
        }

        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(
            expected_request["requestCode"],
            request::UPDATE_AND_CREATE_STATIC_TOPIC
        );
        assert_eq!(expected_request["timeoutMillis"], case["timeoutMillis"]);
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(expected_request["requestBodyBase64"].as_str().unwrap())
            .unwrap();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, None).await;
        let result = client_api
            .create_static_topic(
                &addr,
                case["defaultTopic"].as_str(),
                &topic_config,
                &mapping_detail,
                case["force"].as_bool().unwrap(),
                case["timeoutMillis"].as_i64().unwrap(),
            )
            .await;
        if case["outcome"] == "returned" {
            result.unwrap_or_else(|error| panic!("{name}: {error}"));
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request::UPDATE_AND_CREATE_STATIC_TOPIC,
                    peer: error_peer,
                    code,
                    remark: Some(remark),
                }) if error_peer == addr
                    && code == response_code
                    && response_remark.as_deref() == Some(remark.as_str())
            ));
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::UPDATE_AND_CREATE_STATIC_TOPIC);
        assert_eq!(
            request.body.as_deref(),
            Some(expected_body.as_slice()),
            "{name}"
        );
        assert_eq!(
            request
                .ext_fields
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"],
            "{name}"
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn create_static_topic_honors_the_java_timeout_argument() {
    let (addr, peer) =
        scripted_peer_after(response::SUCCESS, None, None, Duration::from_millis(150)).await;
    let (client_api, client) = api().await;
    let config = TopicConfig::new("orders");
    let mapping = TopicQueueMappingDetail::default();
    let result = client_api
        .create_static_topic(&addr, Some("TBW102"), &config, &mapping, false, 25)
        .await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn create_user_matches_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["createUserApi"],
        json!([
            "void org.apache.rocketmq.client.impl.MQClientAPIImpl.createUser(java.lang.String,org.apache.rocketmq.remoting.protocol.body.UserInfo,long)"
        ])
    );
    for (surface, required_methods) in [
        (
            "createUserRequestHeaderApi",
            vec!["getUsername", "setUsername", "checkFields"],
        ),
        (
            "userInfoApi",
            vec![
                "of",
                "getUsername",
                "setUsername",
                "getPassword",
                "setPassword",
                "getUserType",
                "setUserType",
                "getUserStatus",
                "setUserStatus",
            ],
        ),
    ] {
        let methods = fixture[surface]["declaredPublicMethods"]
            .as_array()
            .unwrap();
        for method in required_methods {
            assert!(
                methods.iter().any(|signature| {
                    signature
                        .as_str()
                        .is_some_and(|signature| signature.contains(&format!(".{method}(")))
                }),
                "{surface} lacks {method}"
            );
        }
    }
    assert_eq!(
        fixture["createUserRequestHeaderApi"]["publicConstructors"],
        json!([
            "org.apache.rocketmq.remoting.protocol.header.CreateUserRequestHeader()",
            "org.apache.rocketmq.remoting.protocol.header.CreateUserRequestHeader(java.lang.String)"
        ])
    );
    assert_eq!(
        fixture["userInfoApi"]["publicConstructors"],
        json!(["org.apache.rocketmq.remoting.protocol.body.UserInfo()"])
    );
    let user_info_methods = fixture["userInfoApi"]["declaredPublicMethods"]
        .as_array()
        .unwrap();
    for signature in [
        "org.apache.rocketmq.remoting.protocol.body.UserInfo org.apache.rocketmq.remoting.protocol.body.UserInfo.of(java.lang.String,java.lang.String,java.lang.String)",
        "org.apache.rocketmq.remoting.protocol.body.UserInfo org.apache.rocketmq.remoting.protocol.body.UserInfo.of(java.lang.String,java.lang.String,java.lang.String,java.lang.String)",
    ] {
        assert!(
            user_info_methods
                .iter()
                .any(|method| method.as_str() == Some(signature)),
            "missing UserInfo factory: {signature}"
        );
    }

    let cases = fixture["createUserOperations"]["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 6);
    let (client_api, client) = api().await;
    for case in cases
        .iter()
        .filter(|case| !case["vipEnabled"].as_bool().unwrap())
    {
        let name = case["name"].as_str().unwrap();
        let user_info =
            serde_json::from_str::<UserInfo>(case["userInfoJson"].as_str().unwrap()).unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(expected_request["requestCode"], request::AUTH_CREATE_USER);
        assert_eq!(expected_request["timeoutMillis"], case["timeoutMillis"]);
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(expected_request["requestBodyBase64"].as_str().unwrap())
            .unwrap();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, None).await;
        let result = client_api
            .create_user(&addr, &user_info, case["timeoutMillis"].as_i64().unwrap())
            .await;
        if case["outcome"] == "returned" {
            result.unwrap_or_else(|error| panic!("{name}: {error}"));
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request::AUTH_CREATE_USER,
                    peer: error_peer,
                    code,
                    remark: Some(remark),
                }) if error_peer == addr
                    && code == response_code
                    && response_remark.as_deref() == Some(remark.as_str())
            ));
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::AUTH_CREATE_USER);
        assert_eq!(
            request.body.as_deref(),
            Some(expected_body.as_slice()),
            "{name}"
        );
        assert_eq!(
            request
                .ext_fields
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
                .as_object()
                .map_or_else(|| json!({}), |fields| json!(fields)),
            "{name}"
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn create_user_honors_the_java_timeout_argument() {
    let (addr, peer) =
        scripted_peer_after(response::SUCCESS, None, None, Duration::from_millis(150)).await;
    let (client_api, client) = api().await;
    let user_info = UserInfo::of(
        Some("alice".into()),
        Some("fixture-password".into()),
        Some("ADMIN".into()),
    );
    let result = client_api.create_user(&addr, &user_info, 25).await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn update_user_matches_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["updateUserApi"],
        json!([
            "void org.apache.rocketmq.client.impl.MQClientAPIImpl.updateUser(java.lang.String,org.apache.rocketmq.remoting.protocol.body.UserInfo,long)"
        ])
    );
    let methods = fixture["updateUserRequestHeaderApi"]["declaredPublicMethods"]
        .as_array()
        .unwrap();
    for method in ["getUsername", "setUsername", "checkFields"] {
        assert!(
            methods.iter().any(|signature| {
                signature
                    .as_str()
                    .is_some_and(|signature| signature.contains(&format!(".{method}(")))
            }),
            "updateUserRequestHeaderApi lacks {method}"
        );
    }
    assert_eq!(
        fixture["updateUserRequestHeaderApi"]["publicConstructors"],
        json!([
            "org.apache.rocketmq.remoting.protocol.header.UpdateUserRequestHeader()",
            "org.apache.rocketmq.remoting.protocol.header.UpdateUserRequestHeader(java.lang.String)"
        ])
    );

    let cases = fixture["updateUserOperations"]["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 5);
    let (client_api, client) = api().await;
    for case in cases
        .iter()
        .filter(|case| !case["vipEnabled"].as_bool().unwrap())
    {
        let name = case["name"].as_str().unwrap();
        let user_info =
            serde_json::from_str::<UserInfo>(case["userInfoJson"].as_str().unwrap()).unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(expected_request["requestCode"], request::AUTH_UPDATE_USER);
        assert_eq!(expected_request["timeoutMillis"], case["timeoutMillis"]);
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(expected_request["requestBodyBase64"].as_str().unwrap())
            .unwrap();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, None).await;
        let result = client_api
            .update_user(&addr, &user_info, case["timeoutMillis"].as_i64().unwrap())
            .await;
        if case["outcome"] == "returned" {
            result.unwrap_or_else(|error| panic!("{name}: {error}"));
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request::AUTH_UPDATE_USER,
                    peer: error_peer,
                    code,
                    remark: Some(remark),
                }) if error_peer == addr
                    && code == response_code
                    && response_remark.as_deref() == Some(remark.as_str())
            ));
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::AUTH_UPDATE_USER, "{name}");
        assert_eq!(
            request.body.as_deref(),
            Some(expected_body.as_slice()),
            "{name}"
        );
        assert_eq!(
            request
                .ext_fields
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
                .as_object()
                .map_or_else(|| json!({}), |fields| json!(fields)),
            "{name}"
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn update_user_honors_the_java_timeout_argument() {
    let (addr, peer) =
        scripted_peer_after(response::SUCCESS, None, None, Duration::from_millis(150)).await;
    let (client_api, client) = api().await;
    let user_info = UserInfo::of(
        Some("alice".into()),
        Some("fixture-password".into()),
        Some("ADMIN".into()),
    );
    let result = client_api.update_user(&addr, &user_info, 25).await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn delete_user_matches_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["deleteUserApi"],
        json!([
            "void org.apache.rocketmq.client.impl.MQClientAPIImpl.deleteUser(java.lang.String,java.lang.String,long)"
        ])
    );
    let methods = fixture["deleteUserRequestHeaderApi"]["declaredPublicMethods"]
        .as_array()
        .unwrap();
    for method in ["getUsername", "setUsername", "checkFields"] {
        assert!(
            methods.iter().any(|signature| {
                signature
                    .as_str()
                    .is_some_and(|signature| signature.contains(&format!(".{method}(")))
            }),
            "deleteUserRequestHeaderApi lacks {method}"
        );
    }
    assert_eq!(
        fixture["deleteUserRequestHeaderApi"]["publicConstructors"],
        json!([
            "org.apache.rocketmq.remoting.protocol.header.DeleteUserRequestHeader()",
            "org.apache.rocketmq.remoting.protocol.header.DeleteUserRequestHeader(java.lang.String)"
        ])
    );

    let cases = fixture["deleteUserOperations"]["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 5);
    let (client_api, client) = api().await;
    for case in cases
        .iter()
        .filter(|case| !case["vipEnabled"].as_bool().unwrap())
    {
        let name = case["name"].as_str().unwrap();
        let username = case["username"].as_str();
        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(expected_request["requestCode"], request::AUTH_DELETE_USER);
        assert_eq!(expected_request["timeoutMillis"], case["timeoutMillis"]);
        assert_eq!(expected_request["requestBodyPresent"], false);
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, None).await;
        let result = client_api
            .delete_user(&addr, username, case["timeoutMillis"].as_i64().unwrap())
            .await;
        if case["outcome"] == "returned" {
            result.unwrap_or_else(|error| panic!("{name}: {error}"));
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request::AUTH_DELETE_USER,
                    peer: error_peer,
                    code,
                    remark: Some(remark),
                }) if error_peer == addr
                    && code == response_code
                    && response_remark.as_deref() == Some(remark.as_str())
            ));
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::AUTH_DELETE_USER, "{name}");
        assert!(request.body.is_none(), "{name}");
        assert_eq!(
            request
                .ext_fields
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
                .as_object()
                .map_or_else(|| json!({}), |fields| json!(fields)),
            "{name}"
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn delete_user_honors_the_java_timeout_argument() {
    let (addr, peer) =
        scripted_peer_after(response::SUCCESS, None, None, Duration::from_millis(150)).await;
    let (client_api, client) = api().await;
    let result = client_api.delete_user(&addr, Some("alice"), 25).await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn list_user_matches_java_requests_responses_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["listUserApi"],
        json!([
            "java.util.List org.apache.rocketmq.client.impl.MQClientAPIImpl.listUser(java.lang.String,java.lang.String,long)"
        ])
    );
    let methods = fixture["listUsersRequestHeaderApi"]["declaredPublicMethods"]
        .as_array()
        .unwrap();
    for method in ["getFilter", "setFilter", "checkFields"] {
        assert!(
            methods.iter().any(|signature| {
                signature
                    .as_str()
                    .is_some_and(|signature| signature.contains(&format!(".{method}(")))
            }),
            "listUsersRequestHeaderApi lacks {method}"
        );
    }
    assert_eq!(
        fixture["listUsersRequestHeaderApi"]["publicConstructors"],
        json!([
            "org.apache.rocketmq.remoting.protocol.header.ListUsersRequestHeader()",
            "org.apache.rocketmq.remoting.protocol.header.ListUsersRequestHeader(java.lang.String)"
        ])
    );

    let cases = fixture["listUserOperations"]["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 8);
    let (client_api, client) = api().await;
    for case in cases
        .iter()
        .filter(|case| !case["vipEnabled"].as_bool().unwrap())
    {
        let name = case["name"].as_str().unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(expected_request["requestCode"], request::AUTH_LIST_USER);
        assert_eq!(expected_request["timeoutMillis"], case["timeoutMillis"]);
        assert_eq!(expected_request["requestBodyPresent"], false);
        let expected_body = case["responseBodyBase64"].as_str().map(|body| {
            base64::engine::general_purpose::STANDARD
                .decode(body)
                .unwrap()
        });
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, expected_body)
                .await;
        let result = client_api
            .list_user(
                &addr,
                case["filter"].as_str(),
                case["timeoutMillis"].as_i64().unwrap(),
            )
            .await;
        match case["outcome"].as_str().unwrap() {
            "returned" => {
                let received = result.unwrap_or_else(|error| panic!("{name}: {error}"));
                let result_is_null = received.is_none();
                assert_eq!(
                    serde_json::to_value(received).unwrap(),
                    serde_json::from_str::<Value>(case["resultJson"].as_str().unwrap()).unwrap(),
                    "{name}"
                );
                assert_eq!(case["resultIsNull"], result_is_null, "{name}");
            }
            "MQBrokerException" => assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request::AUTH_LIST_USER,
                    peer: error_peer,
                    code,
                    remark: Some(remark),
                }) if error_peer == addr
                    && code == response_code
                    && response_remark.as_deref() == Some(remark.as_str())
            )),
            _ => assert!(
                matches!(
                    result,
                    Err(RpcError::MalformedResponse {
                        request_code: request::AUTH_LIST_USER,
                        peer: error_peer,
                        ..
                    }) if error_peer == addr
                ),
                "{name}"
            ),
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::AUTH_LIST_USER, "{name}");
        assert!(request.body.is_none(), "{name}");
        assert_eq!(
            request
                .ext_fields
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
                .as_object()
                .map_or_else(|| json!({}), |fields| json!(fields)),
            "{name}"
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn list_user_honors_the_java_timeout_argument() {
    let (addr, peer) =
        scripted_peer_after(response::SUCCESS, None, None, Duration::from_millis(150)).await;
    let (client_api, client) = api().await;
    let result = client_api.list_user(&addr, Some("ali"), 25).await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn list_acl_matches_java_requests_responses_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["listAclApi"],
        json!([
            "java.util.List org.apache.rocketmq.client.impl.MQClientAPIImpl.listAcl(java.lang.String,java.lang.String,java.lang.String,long)"
        ])
    );
    let methods = fixture["listAclsRequestHeaderApi"]["declaredPublicMethods"]
        .as_array()
        .unwrap();
    for method in [
        "getSubjectFilter",
        "setSubjectFilter",
        "getResourceFilter",
        "setResourceFilter",
        "checkFields",
    ] {
        assert!(methods.iter().any(|signature| {
            signature
                .as_str()
                .is_some_and(|signature| signature.contains(&format!(".{method}(")))
        }));
    }
    assert_eq!(
        fixture["listAclsRequestHeaderApi"]["publicConstructors"],
        json!([
            "org.apache.rocketmq.remoting.protocol.header.ListAclsRequestHeader()",
            "org.apache.rocketmq.remoting.protocol.header.ListAclsRequestHeader(java.lang.String,java.lang.String)"
        ])
    );

    let cases = fixture["listAclOperations"]["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 8);
    let (client_api, client) = api().await;
    for case in cases
        .iter()
        .filter(|case| !case["vipEnabled"].as_bool().unwrap())
    {
        let name = case["name"].as_str().unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(expected_request["requestCode"], request::AUTH_LIST_ACL);
        assert_eq!(expected_request["timeoutMillis"], case["timeoutMillis"]);
        assert_eq!(expected_request["requestBodyPresent"], false);
        let expected_body = case["responseBodyBase64"].as_str().map(|body| {
            base64::engine::general_purpose::STANDARD
                .decode(body)
                .unwrap()
        });
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, expected_body)
                .await;
        let result = client_api
            .list_acl(
                &addr,
                case["subjectFilter"].as_str(),
                case["resourceFilter"].as_str(),
                case["timeoutMillis"].as_i64().unwrap(),
            )
            .await;
        if name == "object-root" {
            assert_eq!(case["resultJson"], "[\"{}\"]");
            assert!(matches!(
                result,
                Err(RpcError::MalformedResponse {
                    request_code: request::AUTH_LIST_ACL,
                    peer: error_peer,
                    ..
                }) if error_peer == addr
            ));
        } else {
            match case["outcome"].as_str().unwrap() {
                "returned" => {
                    let received = result.unwrap_or_else(|error| panic!("{name}: {error}"));
                    let result_is_null = received.is_none();
                    assert_eq!(
                        serde_json::to_value(received).unwrap(),
                        serde_json::from_str::<Value>(case["resultJson"].as_str().unwrap())
                            .unwrap(),
                        "{name}"
                    );
                    assert_eq!(case["resultIsNull"], result_is_null, "{name}");
                }
                "MQBrokerException" => assert!(matches!(
                    result,
                    Err(RpcError::Response {
                        request_code: request::AUTH_LIST_ACL,
                        peer: error_peer,
                        code,
                        remark: Some(remark),
                    }) if error_peer == addr
                        && code == response_code
                        && response_remark.as_deref() == Some(remark.as_str())
                )),
                _ => panic!("unexpected Java outcome for {name}"),
            }
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::AUTH_LIST_ACL, "{name}");
        assert!(request.body.is_none(), "{name}");
        assert_eq!(
            request
                .ext_fields
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
                .as_object()
                .map_or_else(|| json!({}), |fields| json!(fields)),
            "{name}"
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn list_acl_honors_the_java_timeout_argument() {
    let (addr, peer) =
        scripted_peer_after(response::SUCCESS, None, None, Duration::from_millis(150)).await;
    let (client_api, client) = api().await;
    let result = client_api
        .list_acl(&addr, Some("ali"), Some("topic"), 25)
        .await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn get_user_matches_java_requests_responses_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["getUserApi"],
        json!([
            "org.apache.rocketmq.remoting.protocol.body.UserInfo org.apache.rocketmq.client.impl.MQClientAPIImpl.getUser(java.lang.String,java.lang.String,long)"
        ])
    );
    let methods = fixture["getUserRequestHeaderApi"]["declaredPublicMethods"]
        .as_array()
        .unwrap();
    for method in ["getUsername", "setUsername", "checkFields"] {
        assert!(
            methods.iter().any(|signature| {
                signature
                    .as_str()
                    .is_some_and(|signature| signature.contains(&format!(".{method}(")))
            }),
            "getUserRequestHeaderApi lacks {method}"
        );
    }
    assert_eq!(
        fixture["getUserRequestHeaderApi"]["publicConstructors"],
        json!([
            "org.apache.rocketmq.remoting.protocol.header.GetUserRequestHeader()",
            "org.apache.rocketmq.remoting.protocol.header.GetUserRequestHeader(java.lang.String)"
        ])
    );

    let cases = fixture["getUserOperations"]["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 10);
    let (client_api, client) = api().await;
    for case in cases
        .iter()
        .filter(|case| !case["vipEnabled"].as_bool().unwrap())
    {
        let name = case["name"].as_str().unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(expected_request["requestCode"], request::AUTH_GET_USER);
        assert_eq!(expected_request["timeoutMillis"], case["timeoutMillis"]);
        assert_eq!(expected_request["requestBodyPresent"], false);
        let expected_body = case["responseBodyBase64"].as_str().map(|body| {
            base64::engine::general_purpose::STANDARD
                .decode(body)
                .unwrap()
        });
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, expected_body)
                .await;
        let result = client_api
            .get_user(
                &addr,
                case["username"].as_str(),
                case["timeoutMillis"].as_i64().unwrap(),
            )
            .await;
        if case["outcome"] == "returned" {
            let received = result.unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(case["resultIsNull"], received.is_none(), "{name}");
            assert_eq!(
                serde_json::to_value(received).unwrap(),
                serde_json::from_str::<Value>(case["resultJson"].as_str().unwrap()).unwrap(),
                "{name}"
            );
        } else {
            assert!(
                matches!(
                    result,
                    Err(RpcError::Response {
                        request_code: request::AUTH_GET_USER,
                        peer: error_peer,
                        code,
                        remark: Some(remark),
                    }) if error_peer == addr
                        && code == response_code
                        && response_remark.as_deref() == Some(remark.as_str())
                ),
                "{name}"
            );
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::AUTH_GET_USER, "{name}");
        assert!(request.body.is_none(), "{name}");
        assert_eq!(
            request
                .ext_fields
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
                .as_object()
                .map_or_else(|| json!({}), |fields| json!(fields)),
            "{name}"
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn get_user_honors_the_java_timeout_argument() {
    let (addr, peer) =
        scripted_peer_after(response::SUCCESS, None, None, Duration::from_millis(150)).await;
    let (client_api, client) = api().await;
    let result = client_api.get_user(&addr, Some("alice"), 25).await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn get_acl_matches_java_requests_responses_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["getAclApi"],
        json!([
            "org.apache.rocketmq.remoting.protocol.body.AclInfo org.apache.rocketmq.client.impl.MQClientAPIImpl.getAcl(java.lang.String,java.lang.String,long)"
        ])
    );
    let header_methods = fixture["getAclRequestHeaderApi"]["declaredPublicMethods"]
        .as_array()
        .unwrap();
    for method in ["getSubject", "setSubject", "checkFields"] {
        assert!(header_methods.iter().any(|signature| {
            signature
                .as_str()
                .is_some_and(|signature| signature.contains(&format!(".{method}(")))
        }));
    }
    assert_eq!(
        fixture["getAclRequestHeaderApi"]["publicConstructors"],
        json!([
            "org.apache.rocketmq.remoting.protocol.header.GetAclRequestHeader()",
            "org.apache.rocketmq.remoting.protocol.header.GetAclRequestHeader(java.lang.String)"
        ])
    );
    for (surface, methods) in [
        (
            "aclInfoApi",
            [
                "getSubject",
                "setSubject",
                "getPolicies",
                "setPolicies",
                "of",
            ]
            .as_slice(),
        ),
        (
            "aclPolicyInfoApi",
            [
                "getPolicyType",
                "setPolicyType",
                "getEntries",
                "setEntries",
                "of",
            ]
            .as_slice(),
        ),
        (
            "aclPolicyEntryInfoApi",
            [
                "getResource",
                "setResource",
                "getActions",
                "setActions",
                "getSourceIps",
                "setSourceIps",
                "getDecision",
                "setDecision",
                "of",
            ]
            .as_slice(),
        ),
    ] {
        let actual = fixture[surface]["declaredPublicMethods"]
            .as_array()
            .unwrap();
        for method in methods {
            assert!(
                actual.iter().any(|signature| {
                    signature
                        .as_str()
                        .is_some_and(|signature| signature.contains(&format!(".{method}(")))
                }),
                "{surface} lacks {method}"
            );
        }
        assert_eq!(
            fixture[surface]["publicConstructors"],
            json!([format!(
                "{}()",
                fixture[surface]["className"].as_str().unwrap()
            )])
        );
    }

    let cases = fixture["getAclOperations"]["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 11);
    let (client_api, client) = api().await;
    for case in cases
        .iter()
        .filter(|case| !case["vipEnabled"].as_bool().unwrap())
    {
        let name = case["name"].as_str().unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(expected_request["requestCode"], request::AUTH_GET_ACL);
        assert_eq!(expected_request["timeoutMillis"], case["timeoutMillis"]);
        assert_eq!(expected_request["requestBodyPresent"], false);
        let expected_body = case["responseBodyBase64"].as_str().map(|body| {
            base64::engine::general_purpose::STANDARD
                .decode(body)
                .unwrap()
        });
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, expected_body)
                .await;
        let result = client_api
            .get_acl(
                &addr,
                case["subject"].as_str(),
                case["timeoutMillis"].as_i64().unwrap(),
            )
            .await;
        if case["outcome"] == "returned" {
            let received = result.unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(case["resultIsNull"], received.is_none(), "{name}");
            assert_eq!(
                serde_json::to_value(received).unwrap(),
                serde_json::from_str::<Value>(case["resultJson"].as_str().unwrap()).unwrap(),
                "{name}"
            );
        } else {
            assert!(
                matches!(
                    result,
                    Err(RpcError::Response {
                        request_code: request::AUTH_GET_ACL,
                        peer: error_peer,
                        code,
                        remark: Some(remark),
                    }) if error_peer == addr
                        && code == response_code
                        && response_remark.as_deref() == Some(remark.as_str())
                ),
                "{name}"
            );
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::AUTH_GET_ACL, "{name}");
        assert!(request.body.is_none(), "{name}");
        assert_eq!(
            request
                .ext_fields
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
                .as_object()
                .map_or_else(|| json!({}), |fields| json!(fields)),
            "{name}"
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn get_acl_honors_the_java_timeout_argument() {
    let (addr, peer) =
        scripted_peer_after(response::SUCCESS, None, None, Duration::from_millis(150)).await;
    let (client_api, client) = api().await;
    let result = client_api.get_acl(&addr, Some("subject"), 25).await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn create_acl_matches_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["createAclApi"],
        json!([
            "void org.apache.rocketmq.client.impl.MQClientAPIImpl.createAcl(java.lang.String,org.apache.rocketmq.remoting.protocol.body.AclInfo,long)"
        ])
    );
    let methods = fixture["createAclRequestHeaderApi"]["declaredPublicMethods"]
        .as_array()
        .unwrap();
    for method in ["getSubject", "setSubject", "checkFields"] {
        assert!(methods.iter().any(|signature| {
            signature
                .as_str()
                .is_some_and(|signature| signature.contains(&format!(".{method}(")))
        }));
    }
    assert_eq!(
        fixture["createAclRequestHeaderApi"]["publicConstructors"],
        json!([
            "org.apache.rocketmq.remoting.protocol.header.CreateAclRequestHeader()",
            "org.apache.rocketmq.remoting.protocol.header.CreateAclRequestHeader(java.lang.String)"
        ])
    );

    let cases = fixture["createAclOperations"]["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 7);
    let (client_api, client) = api().await;
    for case in cases
        .iter()
        .filter(|case| !case["vipEnabled"].as_bool().unwrap())
    {
        let name = case["name"].as_str().unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(expected_request["requestCode"], request::AUTH_CREATE_ACL);
        assert_eq!(expected_request["timeoutMillis"], case["timeoutMillis"]);
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(expected_request["requestBodyBase64"].as_str().unwrap())
            .unwrap();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, None).await;
        let acl_info: AclInfo =
            serde_json::from_str(case["aclInfoJson"].as_str().unwrap()).unwrap();
        let result = client_api
            .create_acl(&addr, &acl_info, case["timeoutMillis"].as_i64().unwrap())
            .await;
        if case["outcome"] == "returned" {
            result.unwrap_or_else(|error| panic!("{name}: {error}"));
        } else {
            assert!(
                matches!(
                    result,
                    Err(RpcError::Response {
                        request_code: request::AUTH_CREATE_ACL,
                        peer: error_peer,
                        code,
                        remark: Some(remark),
                    }) if error_peer == addr
                        && code == response_code
                        && response_remark.as_deref() == Some(remark.as_str())
                ),
                "{name}"
            );
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::AUTH_CREATE_ACL, "{name}");
        assert_eq!(
            request.body.as_deref(),
            Some(expected_body.as_slice()),
            "{name}"
        );
        assert_eq!(
            request
                .ext_fields
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
                .as_object()
                .map_or_else(|| json!({}), |fields| json!(fields)),
            "{name}"
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn create_acl_honors_the_java_timeout_argument() {
    let (addr, peer) =
        scripted_peer_after(response::SUCCESS, None, None, Duration::from_millis(150)).await;
    let (client_api, client) = api().await;
    let acl_info = AclInfo::default();
    let result = client_api.create_acl(&addr, &acl_info, 25).await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    // A short timeout can expire before the local peer reads a complete frame;
    // this test asserts the deadline result, so stop the peer before shutdown.
    peer.abort();
    let _ = peer.await;
    client.shutdown().await;
}

#[tokio::test]
async fn update_acl_matches_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["updateAclApi"],
        json!([
            "void org.apache.rocketmq.client.impl.MQClientAPIImpl.updateAcl(java.lang.String,org.apache.rocketmq.remoting.protocol.body.AclInfo,long)"
        ])
    );
    let methods = fixture["updateAclRequestHeaderApi"]["declaredPublicMethods"]
        .as_array()
        .unwrap();
    for method in ["getSubject", "setSubject", "checkFields"] {
        assert!(methods.iter().any(|signature| {
            signature
                .as_str()
                .is_some_and(|signature| signature.contains(&format!(".{method}(")))
        }));
    }
    assert_eq!(
        fixture["updateAclRequestHeaderApi"]["publicConstructors"],
        json!([
            "org.apache.rocketmq.remoting.protocol.header.UpdateAclRequestHeader()",
            "org.apache.rocketmq.remoting.protocol.header.UpdateAclRequestHeader(java.lang.String)"
        ])
    );

    let cases = fixture["updateAclOperations"]["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 7);
    let (client_api, client) = api().await;
    for case in cases
        .iter()
        .filter(|case| !case["vipEnabled"].as_bool().unwrap())
    {
        let name = case["name"].as_str().unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(expected_request["requestCode"], request::AUTH_UPDATE_ACL);
        assert_eq!(expected_request["timeoutMillis"], case["timeoutMillis"]);
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(expected_request["requestBodyBase64"].as_str().unwrap())
            .unwrap();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, None).await;
        let acl_info: AclInfo =
            serde_json::from_str(case["aclInfoJson"].as_str().unwrap()).unwrap();
        let result = client_api
            .update_acl(&addr, &acl_info, case["timeoutMillis"].as_i64().unwrap())
            .await;
        if case["outcome"] == "returned" {
            result.unwrap_or_else(|error| panic!("{name}: {error}"));
        } else {
            assert!(
                matches!(
                    result,
                    Err(RpcError::Response {
                        request_code: request::AUTH_UPDATE_ACL,
                        peer: error_peer,
                        code,
                        remark: Some(remark),
                    }) if error_peer == addr
                        && code == response_code
                        && response_remark.as_deref() == Some(remark.as_str())
                ),
                "{name}"
            );
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::AUTH_UPDATE_ACL, "{name}");
        assert_eq!(
            request.body.as_deref(),
            Some(expected_body.as_slice()),
            "{name}"
        );
        assert_eq!(
            request
                .ext_fields
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
                .as_object()
                .map_or_else(|| json!({}), |fields| json!(fields)),
            "{name}"
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn update_acl_honors_the_java_timeout_argument() {
    let (addr, peer) =
        scripted_peer_after(response::SUCCESS, None, None, Duration::from_millis(150)).await;
    let (client_api, client) = api().await;
    let acl_info = AclInfo::default();
    let result = client_api.update_acl(&addr, &acl_info, 25).await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn delete_acl_matches_java_requests_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["deleteAclApi"],
        json!([
            "void org.apache.rocketmq.client.impl.MQClientAPIImpl.deleteAcl(java.lang.String,java.lang.String,java.lang.String,long)"
        ])
    );
    let methods = fixture["deleteAclRequestHeaderApi"]["declaredPublicMethods"]
        .as_array()
        .unwrap();
    for method in [
        "getSubject",
        "setSubject",
        "getPolicyType",
        "setPolicyType",
        "getResource",
        "setResource",
        "checkFields",
    ] {
        assert!(methods.iter().any(|signature| {
            signature
                .as_str()
                .is_some_and(|signature| signature.contains(&format!(".{method}(")))
        }));
    }
    assert_eq!(
        fixture["deleteAclRequestHeaderApi"]["publicConstructors"],
        json!([
            "org.apache.rocketmq.remoting.protocol.header.DeleteAclRequestHeader()",
            "org.apache.rocketmq.remoting.protocol.header.DeleteAclRequestHeader(java.lang.String,java.lang.String)"
        ])
    );

    let cases = fixture["deleteAclOperations"]["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 7);
    let (client_api, client) = api().await;
    for case in cases
        .iter()
        .filter(|case| !case["vipEnabled"].as_bool().unwrap())
    {
        let name = case["name"].as_str().unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(expected_request["requestCode"], request::AUTH_DELETE_ACL);
        assert_eq!(expected_request["timeoutMillis"], case["timeoutMillis"]);
        assert_eq!(expected_request["requestBodyPresent"], false);
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, None).await;
        let subject = case["subject"].as_str();
        let resource = case["resource"].as_str();
        let result = client_api
            .delete_acl(
                &addr,
                subject,
                resource,
                case["timeoutMillis"].as_i64().unwrap(),
            )
            .await;
        if case["outcome"] == "returned" {
            result.unwrap_or_else(|error| panic!("{name}: {error}"));
        } else {
            assert!(
                matches!(
                    result,
                    Err(RpcError::Response {
                        request_code: request::AUTH_DELETE_ACL,
                        peer: error_peer,
                        code,
                        remark: Some(remark),
                    }) if error_peer == addr
                        && code == response_code
                        && response_remark.as_deref() == Some(remark.as_str())
                ),
                "{name}"
            );
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::AUTH_DELETE_ACL, "{name}");
        assert!(request.body.is_none(), "{name}");
        assert_eq!(
            request
                .ext_fields
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .unwrap()
                .unwrap_or_else(|| json!({})),
            expected_request["extFields"]
                .as_object()
                .map_or_else(|| json!({}), |fields| json!(fields)),
            "{name}"
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn delete_acl_honors_the_java_timeout_argument() {
    let (addr, peer) =
        scripted_peer_after(response::SUCCESS, None, None, Duration::from_millis(150)).await;
    let (client_api, client) = api().await;
    let result = client_api
        .delete_acl(&addr, Some("subject"), Some("topic-a"), 25)
        .await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn get_all_topic_config_matches_java_pagination_and_version_fixtures() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["getAllTopicConfigApi"],
        json!([
            "org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper org.apache.rocketmq.client.impl.MQClientAPIImpl.getAllTopicConfig(java.lang.String,long)"
        ])
    );
    for (surface, expected_methods) in [
        (
            "getAllTopicConfigRequestHeaderApi",
            vec![
                "getTopicSeq",
                "setTopicSeq",
                "getDataVersion",
                "setDataVersion",
                "getMaxTopicNum",
                "setMaxTopicNum",
            ],
        ),
        (
            "getAllTopicConfigResponseHeaderApi",
            vec!["getTotalTopicNum", "setTotalTopicNum"],
        ),
        (
            "topicConfigSerializeWrapperApi",
            vec![
                "getTopicConfigTable",
                "setTopicConfigTable",
                "getDataVersion",
                "setDataVersion",
            ],
        ),
        (
            "dataVersionApi",
            vec![
                "getStateVersion",
                "setStateVersion",
                "getTimestamp",
                "setTimestamp",
                "getCounter",
                "setCounter",
                "assignNewOne",
                "nextVersion",
                "compare",
            ],
        ),
    ] {
        let declared = fixture[surface]["declaredPublicMethods"]
            .as_array()
            .unwrap();
        for method in expected_methods {
            assert!(
                declared.iter().any(|signature| {
                    signature
                        .as_str()
                        .is_some_and(|signature| signature.contains(&format!(".{method}(")))
                }),
                "{surface} lacks {method}"
            );
        }
        assert_eq!(
            fixture[surface]["declaredPublicMethodCount"]
                .as_u64()
                .unwrap(),
            declared.len() as u64
        );
    }

    let cases = fixture["getAllTopicConfigOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 12);
    for case in cases {
        let expected_requests = case["requests"].as_array().unwrap();
        let timeout_millis = case["timeoutMillis"].as_i64().unwrap();
        let name = case["name"].as_str().unwrap();
        let (addr, peer) = if expected_requests.is_empty() {
            ("127.0.0.1:1".to_owned(), None)
        } else {
            let responses = case["responses"]
                .as_array()
                .unwrap()
                .iter()
                .take(expected_requests.len())
                .cloned()
                .collect();
            let (addr, peer) = scripted_peer_sequence(responses).await;
            (addr, Some(peer))
        };
        let (client_api, client) = api().await;
        let result = client_api.get_all_topic_config(&addr, timeout_millis).await;

        if case["outcome"] == "returned" {
            let value = result.unwrap_or_else(|error| panic!("{name}: {error}"));
            let actual_json: Value =
                serde_json::from_str(&value.java_to_json(false).unwrap()).unwrap();
            let expected_json: Value =
                serde_json::from_str(case["resultJson"].as_str().unwrap()).unwrap();
            assert_eq!(actual_json, expected_json, "{name}");
            let actual_topics: Vec<_> = value
                .topic_config_table()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            assert_eq!(
                actual_topics,
                case["resultTopics"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(Value::as_str)
                    .map(Option::unwrap)
                    .collect::<Vec<_>>(),
                "{name}"
            );
            let expected_version = &case["resultDataVersion"];
            let actual_version = value.data_version().unwrap();
            assert_eq!(
                actual_version.state_version(),
                expected_version["stateVersion"].as_i64().unwrap(),
                "{name}"
            );
            assert_eq!(
                actual_version.timestamp(),
                expected_version["timestamp"].as_i64().unwrap(),
                "{name}"
            );
            assert_eq!(
                actual_version.counter(),
                expected_version["counter"].as_i64(),
                "{name}"
            );
        } else if name.starts_with("snapshot-timeout") || expected_requests.is_empty() {
            assert!(
                matches!(
                    result,
                    Err(RpcError::Transport(ref error)) if error.is_timeout()
                ),
                "{name}: {result:?}"
            );
        } else if case["outcome"] == "MQBrokerException" {
            assert!(
                matches!(
                    result,
                    Err(RpcError::Response {
                        request_code,
                        code,
                        ref remark,
                        ..
                    }) if request_code == request::GET_ALL_TOPIC_CONFIG
                        && code == case["errorCode"].as_i64().unwrap() as i32
                        && remark.as_deref() == case["responses"][0]["remark"].as_str()
                ),
                "{name}: {result:?}"
            );
        } else {
            assert!(
                matches!(
                    result,
                    Err(RpcError::MalformedResponse { request_code, .. })
                        if request_code == request::GET_ALL_TOPIC_CONFIG
                ),
                "{name}: {result:?}"
            );
        }

        if let Some(peer) = peer {
            let requests = peer.await.unwrap();
            assert_eq!(requests.len(), expected_requests.len(), "{name}");
            for (request, expected) in requests.iter().zip(expected_requests) {
                assert_eq!(request.code, request::GET_ALL_TOPIC_CONFIG, "{name}");
                assert_eq!(
                    request.language,
                    Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    "{name}"
                );
                assert!(request.body.is_none(), "{name}");
                assert_eq!(
                    request
                        .ext_fields
                        .as_ref()
                        .map(serde_json::to_value)
                        .transpose()
                        .unwrap()
                        .unwrap_or_else(|| json!({})),
                    expected["extFields"],
                    "{name}"
                );
            }
        }
        client.shutdown().await;
    }
}

#[tokio::test]
async fn get_all_subscription_group_matches_java_pagination_and_version_fixtures() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["getAllSubscriptionGroupApi"],
        json!([
            "org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper org.apache.rocketmq.client.impl.MQClientAPIImpl.getAllSubscriptionGroup(java.lang.String,long)"
        ])
    );
    for (surface, expected_methods) in [
        (
            "getAllSubscriptionGroupRequestHeaderApi",
            vec![
                "getGroupSeq",
                "setGroupSeq",
                "getDataVersion",
                "setDataVersion",
                "getMaxGroupNum",
                "setMaxGroupNum",
            ],
        ),
        (
            "getAllSubscriptionGroupResponseHeaderApi",
            vec!["getTotalGroupNum", "setTotalGroupNum"],
        ),
        (
            "subscriptionGroupWrapperApi",
            vec![
                "getSubscriptionGroupTable",
                "setSubscriptionGroupTable",
                "getForbiddenTable",
                "setForbiddenTable",
                "getDataVersion",
                "setDataVersion",
            ],
        ),
    ] {
        let declared = fixture[surface]["declaredPublicMethods"]
            .as_array()
            .unwrap();
        for method in expected_methods {
            assert!(
                declared.iter().any(|signature| {
                    signature
                        .as_str()
                        .is_some_and(|signature| signature.contains(&format!(".{method}(")))
                }),
                "{surface} lacks {method}"
            );
        }
        assert_eq!(
            fixture[surface]["declaredPublicMethodCount"]
                .as_u64()
                .unwrap(),
            declared.len() as u64
        );
    }

    let cases = fixture["getAllSubscriptionGroupOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 11);
    for case in cases {
        let expected_requests = case["requests"].as_array().unwrap();
        let timeout_millis = case["timeoutMillis"].as_i64().unwrap();
        let name = case["name"].as_str().unwrap();
        let (addr, peer) = if expected_requests.is_empty() {
            ("127.0.0.1:1".to_owned(), None)
        } else {
            let responses = case["responses"]
                .as_array()
                .unwrap()
                .iter()
                .take(expected_requests.len())
                .cloned()
                .collect();
            let (addr, peer) = scripted_peer_sequence(responses).await;
            (addr, Some(peer))
        };
        let (client_api, client) = api().await;
        let result = client_api
            .get_all_subscription_group(&addr, timeout_millis)
            .await;

        if case["outcome"] == "returned" {
            let value = result.unwrap_or_else(|error| panic!("{name}: {error}"));
            let actual_json: Value =
                serde_json::from_str(&value.java_to_json(false).unwrap()).unwrap();
            let expected_json: Value =
                serde_json::from_str(case["resultJson"].as_str().unwrap()).unwrap();
            assert_eq!(actual_json, expected_json, "{name}");
            let actual_groups: Vec<_> = value
                .subscription_group_table()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            assert_eq!(
                actual_groups,
                case["resultGroups"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(Value::as_str)
                    .map(Option::unwrap)
                    .collect::<Vec<_>>(),
                "{name}"
            );
            let actual_forbidden = value.forbidden_table().unwrap();
            for (group, topics) in case["resultForbiddenTable"].as_object().unwrap() {
                let expected_topics = topics.as_object().unwrap();
                assert_eq!(
                    actual_forbidden[group].len(),
                    expected_topics.len(),
                    "{name}"
                );
                for (topic, forbidden) in expected_topics {
                    assert_eq!(
                        actual_forbidden[group][topic],
                        forbidden.as_i64().unwrap() as i32,
                        "{name}"
                    );
                }
            }
            let expected_version = &case["resultDataVersion"];
            let actual_version = value.data_version().unwrap();
            assert_eq!(
                actual_version.state_version(),
                expected_version["stateVersion"].as_i64().unwrap(),
                "{name}"
            );
            assert_eq!(
                actual_version.timestamp(),
                expected_version["timestamp"].as_i64().unwrap(),
                "{name}"
            );
            assert_eq!(
                actual_version.counter(),
                expected_version["counter"].as_i64(),
                "{name}"
            );
        } else if name.starts_with("snapshot-timeout") || expected_requests.is_empty() {
            assert!(
                matches!(
                    result,
                    Err(RpcError::Transport(ref error)) if error.is_timeout()
                ),
                "{name}: {result:?}"
            );
        } else if case["outcome"] == "MQBrokerException" {
            assert!(
                matches!(
                    result,
                    Err(RpcError::Response {
                        request_code,
                        code,
                        ref remark,
                        ..
                    }) if request_code == request::GET_ALL_SUBSCRIPTIONGROUP_CONFIG
                        && code == case["errorCode"].as_i64().unwrap() as i32
                        && remark.as_deref() == case["responses"][0]["remark"].as_str()
                ),
                "{name}: {result:?}"
            );
        } else {
            assert!(
                matches!(
                    result,
                    Err(RpcError::MalformedResponse { request_code, .. })
                        if request_code == request::GET_ALL_SUBSCRIPTIONGROUP_CONFIG
                ),
                "{name}: {result:?}"
            );
        }

        if let Some(peer) = peer {
            let requests = peer.await.unwrap();
            assert_eq!(requests.len(), expected_requests.len(), "{name}");
            for (request, expected) in requests.iter().zip(expected_requests) {
                assert_eq!(
                    request.code,
                    request::GET_ALL_SUBSCRIPTIONGROUP_CONFIG,
                    "{name}"
                );
                assert_eq!(
                    request.language,
                    Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    "{name}"
                );
                assert!(request.body.is_none(), "{name}");
                assert_eq!(
                    request
                        .ext_fields
                        .as_ref()
                        .map(serde_json::to_value)
                        .transpose()
                        .unwrap()
                        .unwrap_or_else(|| json!({})),
                    expected["extFields"],
                    "{name}"
                );
            }
        }
        client.shutdown().await;
    }
}

#[tokio::test]
async fn add_broker_honors_the_java_timeout_argument() {
    let (addr, peer) =
        scripted_peer_after(response::SUCCESS, None, None, Duration::from_millis(150)).await;
    let (client_api, client) = api().await;
    let result = client_api.add_broker(&addr, Some("broker.conf"), 25).await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(ref error)) if error.is_timeout()
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn check_rocksdb_cq_write_progress_matches_java_fixture() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["checkRocksdbCqWriteProgressApi"],
        json!([
            "org.apache.rocketmq.common.CheckRocksdbCqWriteResult org.apache.rocketmq.client.impl.MQClientAPIImpl.checkRocksdbCqWriteProgress(java.lang.String,java.lang.String,long,long)"
        ])
    );
    let result_api = &fixture["checkRocksdbCqWriteResultApi"];
    assert_eq!(fixture["checkRocksdbCqWriteResultPublicFields"], json!([]));
    for method in [
        "getCheckResult",
        "setCheckResult",
        "getCheckStatus",
        "setCheckStatus",
    ] {
        assert!(
            result_api["declaredPublicMethods"]
                .as_array()
                .unwrap()
                .iter()
                .any(|signature| signature.as_str().unwrap().contains(&format!(".{method}(")))
        );
    }
    let status_api = &fixture["checkRocksdbCqWriteStatusApi"];
    let status_methods = status_api["declaredPublicMethods"].as_array().unwrap();
    assert!(
        status_methods
            .iter()
            .any(|signature| signature.as_str().unwrap().contains(".getValue("))
    );
    let java_statuses = fixture["checkRocksdbCqWriteStatusValues"]
        .as_array()
        .unwrap();
    assert_eq!(java_statuses.len(), 4);
    for (index, (status, name, ordinal, value)) in [
        (CheckRocksdbCqWriteStatus::CheckOk, "CHECK_OK", 0, 0),
        (CheckRocksdbCqWriteStatus::CheckNotOk, "CHECK_NOT_OK", 1, 1),
        (
            CheckRocksdbCqWriteStatus::CheckInProgress,
            "CHECK_IN_PROGRESS",
            2,
            2,
        ),
        (CheckRocksdbCqWriteStatus::CheckError, "CHECK_ERROR", 3, 3),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(java_statuses[index]["name"], name);
        assert_eq!(java_statuses[index]["ordinal"], ordinal);
        assert_eq!(java_statuses[index]["value"], value);
        assert_eq!(status.name(), name);
        assert_eq!(status.ordinal(), ordinal);
        assert_eq!(status.get_value(), value);
        assert_eq!(
            CheckRocksdbCqWriteStatus::value_of_name(Some(name)),
            Ok(status)
        );
    }
    assert_eq!(CheckRocksdbCqWriteStatus::values().len(), 4);

    let cases = fixture["checkRocksdbCqWriteProgressOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 8);
    let (client_api, client) = api().await;
    for case in cases {
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let response_body = case["responseBody"]
            .as_str()
            .map(|body| body.as_bytes().to_vec());
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark, None, response_body).await;
        let topic = case["topic"].as_str();
        let check_store_time = case["checkStoreTime"].as_i64().unwrap();
        let result = client_api
            .check_rocksdb_cq_write_progress(
                &addr,
                topic,
                check_store_time,
                Instant::now() + Duration::from_millis(2345),
            )
            .await;
        match case["outcome"].as_str().unwrap() {
            "returned" => {
                if case["resultIsNull"] == true {
                    assert!(result.unwrap().is_none(), "{}", case["name"]);
                } else {
                    let result: CheckRocksdbCqWriteResult = result.unwrap().unwrap();
                    assert_eq!(result.get_check_result(), case["checkResult"].as_str());
                    assert_eq!(
                        result.get_check_status(),
                        case["checkStatus"].as_i64().unwrap_or_default() as i32
                    );
                }
            }
            "MQClientException" => assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request::CHECK_ROCKSDB_CQ_WRITE_PROGRESS,
                    peer: error_peer,
                    code,
                    remark: Some(error_remark),
                }) if error_peer == addr
                    && code == response_code
                    && Some(error_remark.as_str()) == case["responseRemark"].as_str()
            )),
            "JSONException" => assert!(matches!(
                result,
                Err(RpcError::MalformedResponse {
                    request_code: request::CHECK_ROCKSDB_CQ_WRITE_PROGRESS,
                    peer: error_peer,
                    ..
                }) if error_peer == addr
            )),
            outcome => panic!("unexpected Java result {outcome}"),
        }

        let request = peer.await.unwrap();
        assert_eq!(request.code, request::CHECK_ROCKSDB_CQ_WRITE_PROGRESS);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert!(request.body.is_none());
        let fields = request.ext_fields.as_ref().unwrap();
        assert_eq!(
            fields.get("topic").map(String::as_str),
            topic,
            "{}",
            case["name"]
        );
        assert_eq!(fields["checkStoreTime"], check_store_time.to_string());

        let java_request = &case["requests"][0];
        assert_eq!(java_request["address"], "broker-a:10911");
        assert_eq!(
            java_request["requestCode"],
            request::CHECK_ROCKSDB_CQ_WRITE_PROGRESS
        );
        assert_eq!(java_request["timeoutMillis"], 2345);
        assert_eq!(
            java_request["extFields"]["topic"].as_str(),
            topic,
            "{}",
            case["name"]
        );
        assert_eq!(
            java_request["extFields"]["checkStoreTime"],
            check_store_time.to_string()
        );
        assert!(java_request["requestBodyBase64"].is_null());
    }
    client.shutdown().await;
}

#[tokio::test]
async fn export_rocksdb_config_to_json_matches_java_request_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["exportRocksDBConfigToJsonApi"],
        json!([
            "void org.apache.rocketmq.client.impl.MQClientAPIImpl.exportRocksDBConfigToJson(java.lang.String,java.util.List,long)"
        ])
    );
    let cases = fixture["exportRocksDBConfigToJsonOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 4);
    let (client_api, client) = api().await;
    for case in cases {
        if case["name"] == "null-list" {
            assert_eq!(case["outcome"], "NullPointerException");
            assert!(case["requests"].as_array().unwrap().is_empty());

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            assert!(matches!(
                client_api
                    .export_rocksdb_config_to_json_nullable(
                        &addr,
                        None,
                        Instant::now() + Duration::from_millis(2345),
                    )
                    .await,
                Err(RpcError::Header(
                    rocketmq_classic_protocol::header::ProtocolError::MissingField("configType")
                ))
            ));
            assert!(
                tokio::time::timeout(Duration::from_millis(50), listener.accept())
                    .await
                    .is_err()
            );
            continue;
        }

        let config_types = case["configTypeNames"]
            .as_array()
            .unwrap()
            .iter()
            .map(|name| ExportRocksDBConfigType::value_of(name.as_str().unwrap()).unwrap())
            .collect::<Vec<_>>();
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let (addr, peer) =
            scripted_peer_with_remark(response_code, response_remark.clone(), None, None).await;
        let result = client_api
            .export_rocksdb_config_to_json(
                &addr,
                &config_types,
                Instant::now() + Duration::from_millis(2345),
            )
            .await;
        if response_code == response::SUCCESS {
            assert!(result.is_ok());
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request::EXPORT_ROCKSDB_CONFIG_TO_JSON,
                    peer: error_peer,
                    code,
                    remark: Some(error_remark),
                }) if error_peer == addr
                    && code == response_code
                    && Some(error_remark.as_str()) == case["responseRemark"].as_str()
            ));
        }

        let request = peer.await.unwrap();
        assert_eq!(request.code, request::EXPORT_ROCKSDB_CONFIG_TO_JSON);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert_eq!(request.body, None);
        assert_eq!(request.ext_fields.as_ref().unwrap().len(), 1);
        assert_eq!(
            request.ext_fields.as_ref().unwrap()["configType"],
            case["configType"]
        );
        assert_eq!(
            ExportRocksDBConfigType::to_string(&config_types),
            case["configType"]
        );

        let java_request = &case["requests"][0];
        assert_eq!(java_request["address"], "broker-a:10911");
        assert_eq!(
            java_request["requestCode"],
            request::EXPORT_ROCKSDB_CONFIG_TO_JSON
        );
        assert_eq!(java_request["timeoutMillis"], 2345);
        assert_eq!(java_request["extFields"]["configType"], case["configType"]);
        assert!(java_request["requestBodyBase64"].is_null());
    }
    client.shutdown().await;
}

#[tokio::test]
async fn write_permission_name_server_rpcs_match_java_fixture() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["wipeWritePermOfBrokerApi"],
        json!([
            "int org.apache.rocketmq.client.impl.MQClientAPIImpl.wipeWritePermOfBroker(java.lang.String,java.lang.String,long)"
        ])
    );
    assert_eq!(
        fixture["addWritePermOfBrokerApi"],
        json!([
            "int org.apache.rocketmq.client.impl.MQClientAPIImpl.addWritePermOfBroker(java.lang.String,java.lang.String,long)"
        ])
    );
    let cases = fixture["writePermissionOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 12);
    let (client_api, client) = api().await;
    for case in cases {
        let operation = case["operation"].as_str().unwrap();
        let request_code = if operation == "wipe" {
            request::WIPE_WRITE_PERM_OF_BROKER
        } else {
            request::ADD_WRITE_PERM_OF_BROKER
        };
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let response_remark = case["responseRemark"].as_str().map(str::to_owned);
        let mut response_fields = HeaderMap::new();
        if let Some(response_count) = case["responseCount"].as_str() {
            response_fields.insert(
                case["responseField"].as_str().unwrap().to_owned(),
                response_count.to_owned(),
            );
        }
        let response_header = (!response_fields.is_empty()).then_some(response_fields);
        let (addr, peer) = scripted_peer_with_remark(
            response_code,
            response_remark.clone(),
            response_header,
            None,
        )
        .await;
        let broker_name = case["brokerName"].as_str();
        let deadline = Instant::now() + Duration::from_millis(2345);
        let result = if operation == "wipe" {
            client_api
                .wipe_write_perm_of_broker(&addr, broker_name, deadline)
                .await
        } else {
            client_api
                .add_write_perm_of_broker(&addr, broker_name, deadline)
                .await
        };

        match case["outcome"].as_str().unwrap() {
            "returned" => assert_eq!(result.unwrap(), case["result"].as_i64().unwrap() as i32),
            "MQClientException" => assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: actual_code,
                    peer: error_peer,
                    code,
                    remark: Some(error_remark),
                }) if actual_code == request_code
                    && error_peer == addr
                    && code == response_code
                    && Some(error_remark.as_str()) == case["responseRemark"].as_str()
            )),
            "NullPointerException" => assert!(matches!(
                result,
                Err(RpcError::MalformedResponse {
                    request_code: actual_code,
                    peer: error_peer,
                    ..
                }) if actual_code == request_code && error_peer == addr
            )),
            outcome => panic!("unexpected Java result {outcome}"),
        }

        let request = peer.await.unwrap();
        assert_eq!(request.code, request_code);
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
        assert_eq!(request.body, None);
        let fields = request.ext_fields.as_ref().unwrap();
        if let Some(broker_name) = broker_name {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields["brokerName"], broker_name);
        } else {
            assert!(fields.is_empty());
        }

        let java_request = &case["requests"][0];
        assert_eq!(java_request["address"], "namesrv-a:9876");
        assert_eq!(java_request["requestCode"], request_code);
        assert_eq!(java_request["timeoutMillis"], 2345);
        assert!(java_request["requestBodyBase64"].is_null());
        if let Some(broker_name) = broker_name {
            assert_eq!(java_request["extFields"]["brokerName"], broker_name);
        } else {
            assert!(java_request["extFields"].as_object().unwrap().is_empty());
        }
    }
    client.shutdown().await;
}

#[tokio::test]
async fn get_topics_by_cluster_matches_java_nameserver_request_and_outcomes() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures = fixture["getTopicsByClusterApi"].as_array().unwrap();
    assert!(signatures.iter().any(|value| value
        == "org.apache.rocketmq.remoting.protocol.body.TopicList org.apache.rocketmq.client.impl.MQClientAPIImpl.getTopicsByCluster(java.lang.String,long)"));
    let cases = fixture["getTopicsByClusterOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 4);

    let (client_api, client) = api().await;
    for case in cases {
        let java_request = &case["requests"][0];
        assert_eq!(java_request["addressWasNull"], true);
        assert_eq!(java_request["requestCode"], request::GET_TOPICS_BY_CLUSTER);
        assert_eq!(java_request["requestBodyPresent"], false);
        assert_eq!(java_request["headerCluster"], case["cluster"]);
        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let body = case["responseBodyBase64"].as_str().map(|body| {
            base64::engine::general_purpose::STANDARD
                .decode(body)
                .unwrap()
        });
        let (addr, peer) = scripted_peer_with_remark(
            response_code,
            case["responseRemark"].as_str().map(str::to_owned),
            None,
            body,
        )
        .await;
        client.update_name_server_address_list(vec![addr.clone()]);
        let result = client_api
            .get_topics_by_cluster_nullable(
                case["cluster"].as_str(),
                Instant::now() + Duration::from_secs(2),
            )
            .await;
        if case["outcome"] == "returned" {
            let topic_list = result.unwrap();
            assert_eq!(
                topic_list.get_topic_list().unwrap().snapshot(),
                HashSet::from(["orders".to_owned(), "payments".to_owned()])
            );
            assert_eq!(topic_list.get_broker_addr(), Some("broker-a:10911"));
        } else if case["name"] == "missing-body" {
            assert!(matches!(
                result,
                Err(RpcError::MalformedResponse {
                    request_code: request::GET_TOPICS_BY_CLUSTER,
                    peer: error_peer,
                    ..
                }) if error_peer == addr
            ));
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request::GET_TOPICS_BY_CLUSTER,
                    peer: error_peer,
                    code,
                    remark: Some(remark),
                }) if error_peer == addr
                    && code == response_code
                    && remark == "cluster lookup rejected"
            ));
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::GET_TOPICS_BY_CLUSTER);
        assert!(request.body.is_none());
        if let Some(cluster) = java_request["headerCluster"].as_str() {
            assert_eq!(request.ext_fields.as_ref().unwrap()["cluster"], cluster);
        } else {
            assert!(
                request
                    .ext_fields
                    .as_ref()
                    .is_none_or(|fields| !fields.contains_key("cluster"))
            );
        }
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn unit_topic_lists_match_java_nameserver_requests_and_filtering() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures = fixture["unitTopicListsApi"].as_array().unwrap();
    assert_eq!(signatures.len(), 3);
    let signatures: HashSet<&str> = signatures
        .iter()
        .map(|signature| signature.as_str().unwrap())
        .collect();
    assert_eq!(
        signatures,
        HashSet::from([
            "org.apache.rocketmq.remoting.protocol.body.TopicList org.apache.rocketmq.client.impl.MQClientAPIImpl.getUnitTopicList(boolean,long)",
            "org.apache.rocketmq.remoting.protocol.body.TopicList org.apache.rocketmq.client.impl.MQClientAPIImpl.getHasUnitSubTopicList(boolean,long)",
            "org.apache.rocketmq.remoting.protocol.body.TopicList org.apache.rocketmq.client.impl.MQClientAPIImpl.getHasUnitSubUnUnitTopicList(boolean,long)",
        ])
    );
    let cases = fixture["unitTopicListsOperations"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 12);

    let (client_api, client) = api().await;
    for case in cases {
        let method = case["method"].as_str().unwrap();
        let request_code = match method {
            "getUnitTopicList" => request::GET_UNIT_TOPIC_LIST,
            "getHasUnitSubTopicList" => request::GET_HAS_UNIT_SUB_TOPIC_LIST,
            "getHasUnitSubUnUnitTopicList" => request::GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST,
            other => panic!("unexpected Java unit-topic-list method: {other}"),
        };
        let java_request = &case["requests"][0];
        assert_eq!(java_request["addressWasNull"], true);
        assert_eq!(java_request["requestCode"], request_code);
        assert_eq!(java_request["timeoutMillis"], 2345);
        assert_eq!(java_request["extFieldsPresent"], false);
        assert_eq!(java_request["requestBodyPresent"], false);

        let response_code = case["responseCode"].as_i64().unwrap() as i32;
        let body = case["responseBodyBase64"].as_str().map(|body| {
            base64::engine::general_purpose::STANDARD
                .decode(body)
                .unwrap()
        });
        let (addr, peer) = scripted_peer_with_remark(
            response_code,
            case["responseRemark"].as_str().map(str::to_owned),
            None,
            body,
        )
        .await;
        client.update_name_server_address_list(vec![addr.clone()]);
        let deadline = Instant::now() + Duration::from_secs(2);
        let unit_mode = case["unitMode"].as_bool().unwrap();
        let result = match method {
            "getUnitTopicList" => client_api.get_unit_topic_list(unit_mode, deadline).await,
            "getHasUnitSubTopicList" => {
                client_api
                    .get_has_unit_sub_topic_list(unit_mode, deadline)
                    .await
            }
            "getHasUnitSubUnUnitTopicList" => {
                client_api
                    .get_has_unit_sub_ununit_topic_list(unit_mode, deadline)
                    .await
            }
            _ => unreachable!(),
        };

        if case["outcome"] == "returned" {
            let topic_list = result.unwrap();
            let expected_topics: HashSet<String> = case["topics"]
                .as_array()
                .unwrap()
                .iter()
                .map(|topic| topic.as_str().unwrap().to_owned())
                .collect();
            assert_eq!(
                topic_list.get_topic_list().unwrap().snapshot(),
                expected_topics
            );
            assert_eq!(topic_list.get_broker_addr(), Some("broker-a:10911"));
        } else if response_code == response::SUCCESS {
            assert!(matches!(
                result,
                Err(RpcError::MalformedResponse {
                    request_code: code,
                    peer: error_peer,
                    ..
                }) if code == request_code && error_peer == addr
            ));
        } else {
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: code,
                    peer: error_peer,
                    code: response_code,
                    remark: Some(remark),
                }) if code == request_code
                    && error_peer == addr
                    && response_code == 1
                    && remark == "unit topic lookup rejected"
            ));
        }

        let request = peer.await.unwrap();
        assert_eq!(request.code, request_code);
        assert!(request.body.is_none());
        assert!(
            request
                .ext_fields
                .as_ref()
                .is_none_or(|fields| fields.is_empty())
        );
        assert_eq!(
            request.language,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn update_name_server_config_matches_java_fanout_and_error_selection() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let signatures: HashSet<&str> = fixture["nameServerConfigApi"]
        .as_array()
        .unwrap()
        .iter()
        .map(|signature| signature.as_str().unwrap())
        .collect();
    assert_eq!(
        signatures,
        HashSet::from([
            "java.util.Map org.apache.rocketmq.client.impl.MQClientAPIImpl.getNameServerConfig(java.util.List,long)",
            "void org.apache.rocketmq.client.impl.MQClientAPIImpl.updateNameServerConfig(java.util.Properties,java.util.List,long)",
        ])
    );
    let cases = fixture["nameServerConfigOperations"]["update"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 6);

    for case in cases {
        let java_calls = case["requests"].as_array().unwrap();
        let mut actual_addresses = Vec::new();
        let mut peers = Vec::new();
        for java_call in java_calls {
            let response_code = java_call["responseCode"].as_i64().unwrap() as i32;
            let (addr, peer) = scripted_peer_with_remark(
                response_code,
                java_call["responseRemark"].as_str().map(str::to_owned),
                None,
                None,
            )
            .await;
            actual_addresses.push(addr);
            peers.push(peer);
        }

        let configured_count = case["configuredAddresses"].as_array().unwrap().len();
        let mut configured_addresses = actual_addresses.clone();
        while configured_addresses.len() < configured_count {
            configured_addresses.push(format!("unused-configured-{configured_count}"));
        }
        let input_addresses = case["addressInput"].as_array();
        let requested_count = input_addresses.map_or(configured_count, |addresses| addresses.len());
        let mut address_argument = actual_addresses.clone();
        while address_argument.len() < requested_count {
            address_argument.push(format!("unused-explicit-{}", address_argument.len()));
        }

        let (client_api, client) = api().await;
        client.update_name_server_address_list(configured_addresses);
        let address_argument = if case["addressInput"].is_null() {
            None
        } else if input_addresses.is_some_and(|addresses| addresses.is_empty()) {
            Some(&[][..])
        } else {
            Some(address_argument.as_slice())
        };
        let properties: HashMap<String, String> = case["properties"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
            .collect();
        let result = client_api
            .update_name_server_config(
                &properties,
                address_argument,
                Instant::now() + Duration::from_secs(2),
            )
            .await;

        if case["outcome"] == "returned" {
            assert!(result.is_ok(), "{}: {result:?}", case["name"]);
        } else {
            let expected_error_code = case["errorCode"].as_i64().unwrap() as i32;
            let expected_error_remark =
                case["requests"].as_array().unwrap().last().unwrap()["responseRemark"]
                    .as_str()
                    .unwrap();
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request_code_value,
                    peer,
                    code,
                    remark: Some(remark),
                }) if request_code_value == request::UPDATE_NAMESRV_CONFIG
                    && peer == *actual_addresses.last().unwrap()
                    && code == expected_error_code
                    && remark == expected_error_remark
            ));
        }

        for (index, peer) in peers.into_iter().enumerate() {
            let request = peer.await.unwrap();
            let java_call = &java_calls[index];
            assert_eq!(java_call["address"], case["requests"][index]["address"]);
            assert_eq!(java_call["requestCode"], request::UPDATE_NAMESRV_CONFIG);
            assert_eq!(java_call["timeoutMillis"], 2345);
            assert_eq!(java_call["extFieldsPresent"], false);
            assert_eq!(request.code, request::UPDATE_NAMESRV_CONFIG);
            assert!(
                request
                    .ext_fields
                    .as_ref()
                    .is_none_or(|fields| fields.is_empty())
            );
            assert_eq!(
                request.language,
                Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
            );
            let body_bytes = request.body.as_deref().unwrap();
            let body = std::str::from_utf8(body_bytes).unwrap();
            assert!(body.ends_with('\n'));
            let sent_properties: HashMap<String, String> = body
                .lines()
                .map(|line| {
                    let (key, value) = line.split_once('=').unwrap();
                    (key.to_owned(), value.to_owned())
                })
                .collect();
            assert_eq!(sent_properties, properties);
            if properties.len() == 1 {
                let java_body = base64::engine::general_purpose::STANDARD
                    .decode(java_call["requestBodyBase64"].as_str().unwrap())
                    .unwrap();
                assert_eq!(request.body.as_deref(), Some(java_body.as_slice()));
            }
        }
        client.shutdown().await;
    }
}

#[tokio::test]
async fn get_name_server_config_matches_java_results_and_short_circuiting() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let cases = fixture["nameServerConfigOperations"]["get"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 6);

    for case in cases {
        let java_calls = case["requests"].as_array().unwrap();
        let mut actual_addresses = Vec::new();
        let mut java_to_actual_address = HashMap::new();
        let mut peers = Vec::new();
        for java_call in java_calls {
            let response_code = java_call["responseCode"].as_i64().unwrap() as i32;
            let response_body = java_call["responseBodyBase64"].as_str().map(|body| {
                base64::engine::general_purpose::STANDARD
                    .decode(body)
                    .unwrap()
            });
            let (addr, peer) = scripted_peer_with_remark(
                response_code,
                java_call["responseRemark"].as_str().map(str::to_owned),
                None,
                response_body,
            )
            .await;
            java_to_actual_address.insert(
                java_call["address"].as_str().unwrap().to_owned(),
                addr.clone(),
            );
            actual_addresses.push(addr);
            peers.push(peer);
        }

        let configured_count = case["configuredAddresses"].as_array().unwrap().len();
        let mut configured_addresses = actual_addresses.clone();
        while configured_addresses.len() < configured_count {
            configured_addresses.push(format!("unused-configured-{configured_count}"));
        }
        let input_addresses = case["addressInput"].as_array();
        let requested_count = input_addresses.map_or(configured_count, |addresses| addresses.len());
        let mut address_argument = actual_addresses.clone();
        while address_argument.len() < requested_count {
            address_argument.push(format!("unused-explicit-{}", address_argument.len()));
        }

        let (client_api, client) = api().await;
        client.update_name_server_address_list(configured_addresses);
        let address_argument = if case["addressInput"].is_null() {
            None
        } else if input_addresses.is_some_and(|addresses| addresses.is_empty()) {
            Some(&[][..])
        } else {
            Some(address_argument.as_slice())
        };
        let result = client_api
            .get_name_server_config(address_argument, Instant::now() + Duration::from_secs(2))
            .await;

        if case["outcome"] == "returned" {
            let configurations = result.unwrap();
            if case["configurations"].is_null() {
                assert!(configurations.is_none());
            } else {
                let configurations = configurations.unwrap();
                let java_configurations = case["configurations"].as_object().unwrap();
                let null_properties: HashSet<&str> = case["nullProperties"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|address| address.as_str().unwrap())
                    .collect();
                assert_eq!(
                    configurations.len(),
                    java_configurations.len().max(null_properties.len())
                );
                for (java_addr, expected_properties) in java_configurations {
                    let actual_addr = &java_to_actual_address[java_addr];
                    let actual_properties = configurations.get(actual_addr).unwrap();
                    if null_properties.contains(java_addr.as_str()) {
                        assert!(actual_properties.is_none());
                    } else {
                        let expected_properties: HashMap<String, String> = expected_properties
                            .as_object()
                            .unwrap()
                            .iter()
                            .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
                            .collect();
                        assert_eq!(actual_properties.as_ref().unwrap(), &expected_properties);
                    }
                }
                for java_addr in null_properties {
                    let actual_addr = &java_to_actual_address[java_addr];
                    assert!(configurations.get(actual_addr).unwrap().is_none());
                }
            }
        } else if case["outcome"] == "NullPointerException" {
            assert!(matches!(
                result,
                Err(RpcError::MalformedResponse {
                    request_code: request::GET_NAMESRV_CONFIG,
                    peer,
                    ..
                }) if peer == actual_addresses[0]
            ));
        } else {
            let expected_error_code = case["errorCode"].as_i64().unwrap() as i32;
            assert!(matches!(
                result,
                Err(RpcError::Response {
                    request_code: request_code_value,
                    peer,
                    code,
                    remark: Some(remark),
                }) if request_code_value == request::GET_NAMESRV_CONFIG
                    && peer == actual_addresses[0]
                    && code == expected_error_code
                    && remark == "config read rejected"
            ));
        }

        for (index, peer) in peers.into_iter().enumerate() {
            let request = peer.await.unwrap();
            let java_call = &java_calls[index];
            assert_eq!(java_call["requestCode"], request::GET_NAMESRV_CONFIG);
            assert_eq!(java_call["timeoutMillis"], 2345);
            assert_eq!(java_call["extFieldsPresent"], false);
            assert_eq!(request.code, request::GET_NAMESRV_CONFIG);
            assert!(request.body.is_none());
            assert!(
                request
                    .ext_fields
                    .as_ref()
                    .is_none_or(|fields| fields.is_empty())
            );
            assert_eq!(
                request.language,
                Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
            );
        }
        client.shutdown().await;
    }
}

#[tokio::test]
async fn update_broker_config_empty_properties_is_a_noop() {
    let (api, client) = api().await;
    assert!(
        api.update_broker_config(
            "not-an-address",
            &HashMap::new(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .is_ok()
    );
    client.shutdown().await;
}

#[tokio::test]
async fn update_broker_config_rejects_invalid_and_malformed_permissions_locally() {
    let (api, client) = api().await;
    let invalid = HashMap::from([("brokerPermission".to_owned(), "8".to_owned())]);
    let error = api
        .update_broker_config(
            "not-an-address",
            &invalid,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "CODE: 16  DESC: brokerPermission value: 8 is invalid.\nFor more information, please visit the url, https://rocketmq.apache.org/docs/faq/"
    );
    assert!(matches!(
        &error,
        RpcError::ClientValidation {
            request_code: request::UPDATE_BROKER_CONFIG,
            response_code: response::NO_PERMISSION,
            message,
        } if message == "brokerPermission value: 8 is invalid."
    ));

    let malformed = HashMap::from([("brokerPermission".to_owned(), " 7".to_owned())]);
    assert!(matches!(
        api.update_broker_config(
            "not-an-address",
            &malformed,
            Instant::now() + Duration::from_secs(2),
        )
        .await,
        Err(RpcError::PermissionParse(source)) if source.to_string() == "For input string: \" 7\""
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn update_broker_config_preserves_broker_error() {
    let (addr, peer) = scripted_peer_with_remark(
        response::NO_PERMISSION,
        Some("broker denied update".to_owned()),
        None,
        None,
    )
    .await;
    let (api, client) = api().await;
    let properties = HashMap::from([("brokerRole".to_owned(), "ASYNC_MASTER".to_owned())]);
    assert!(matches!(
        api.update_broker_config(&addr, &properties, Instant::now() + Duration::from_secs(2))
            .await,
        Err(RpcError::Response {
            request_code: request::UPDATE_BROKER_CONFIG,
            peer,
            code: response::NO_PERMISSION,
            remark: Some(remark),
        }) if peer == addr && remark == "broker denied update"
    ));
    assert_eq!(peer.await.unwrap().code, request::UPDATE_BROKER_CONFIG);
    client.shutdown().await;
}

#[tokio::test]
async fn create_topic_preserves_broker_permission_error() {
    let (addr, peer) = scripted_peer(response::NO_PERMISSION, None, None).await;
    let (api, client) = api().await;
    let error = api
        .create_topic(
            &addr,
            &{
                let mut __phase18_header_2 = CreateTopicRequestHeader::default();
                __phase18_header_2.set_topic(Some("orders".into()));
                __phase18_header_2.set_default_topic(Some("TBW102".into()));
                __phase18_header_2
            },
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::UPDATE_AND_CREATE_TOPIC,
            code: response::NO_PERMISSION,
            ..
        }
    ));
    assert_eq!(peer.await.unwrap().code, request::UPDATE_AND_CREATE_TOPIC);
    client.shutdown().await;
}

#[tokio::test]
async fn create_topic_matches_mq_client_admin_error_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["updateOrCreateTopic"]["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        expected["responseCode"].as_i64().unwrap() as i32,
        Some(expected["responseRemark"].as_str().unwrap().to_owned()),
        None,
        None,
    )
    .await;
    let (api, client) = api().await;
    let error = api
        .create_topic(
            &addr,
            &{
                let mut __phase18_header_3 = CreateTopicRequestHeader::default();
                __phase18_header_3.set_topic(Some("orders".into()));
                __phase18_header_3.set_default_topic(Some("TBW102".into()));
                __phase18_header_3.set_read_queue_nums(Some(4));
                __phase18_header_3.set_write_queue_nums(Some(4));
                __phase18_header_3.set_perm(Some(6));
                __phase18_header_3.set_topic_filter_type(Some("SINGLE_TAG".into()));
                __phase18_header_3.set_topic_sys_flag(Some(3));
                __phase18_header_3.set_order(Some(false));
                __phase18_header_3.set_attributes(Some("+unit=true".into()));
                __phase18_header_3.set_force(Some(false));
                __phase18_header_3.set_namespace(Some("tenant".into()));
                __phase18_header_3.set_namespaced(Some(true));
                __phase18_header_3.set_broker_name(Some("broker-a".into()));
                __phase18_header_3.set_oneway(Some(false));
                __phase18_header_3
            },
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::UPDATE_AND_CREATE_TOPIC,
            peer,
            code,
            remark: Some(remark),
        } if peer == addr
            && code == expected["responseCode"].as_i64().unwrap() as i32
            && remark == expected["responseRemark"].as_str().unwrap()
    ));
    assert_eq!(peer.await.unwrap().code, request::UPDATE_AND_CREATE_TOPIC);
    client.shutdown().await;
}

#[tokio::test]
async fn delete_topic_in_broker_matches_mq_client_admin_success_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteTopicInBroker"]["success"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let mut header = DeleteTopicRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    header.set_lo(Some(true));

    api.delete_topic_in_broker(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(
        command.code,
        expected["requestCode"].as_i64().unwrap() as i32
    );
    assert!(command.body.is_none());
    let actual_fields = command.ext_fields.as_ref().unwrap();
    let expected_fields = expected["requestExtFields"].as_object().unwrap();
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (key, value) in expected_fields {
        assert_eq!(
            actual_fields.get(key).map(String::as_str),
            value.as_str(),
            "field {key}"
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn delete_topic_in_broker_preserves_java_broker_error() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteTopicInBroker"]["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        expected["responseCode"].as_i64().unwrap() as i32,
        Some(expected["responseRemark"].as_str().unwrap().to_owned()),
        None,
        None,
    )
    .await;
    let (api, client) = api().await;
    let mut header = DeleteTopicRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    header.set_lo(Some(true));
    let error = api
        .delete_topic_in_broker(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::DELETE_TOPIC_IN_BROKER,
            peer,
            code,
            remark: Some(remark),
        } if peer == addr
            && code == expected["responseCode"].as_i64().unwrap() as i32
            && remark == expected["responseRemark"].as_str().unwrap()
    ));
    assert_eq!(peer.await.unwrap().code, request::DELETE_TOPIC_IN_BROKER);
    client.shutdown().await;
}

#[tokio::test]
async fn delete_topic_in_nameserver_matches_mq_client_admin_success_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteTopicInNameserver"]["success"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let mut header = DeleteTopicFromNamesrvRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_cluster_name(Some("cluster-a".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    header.set_lo(Some(true));

    api.delete_topic_in_nameserver(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(
        command.code,
        expected["requestCode"].as_i64().unwrap() as i32
    );
    assert!(command.body.is_none());
    let actual_fields = command.ext_fields.as_ref().unwrap();
    let expected_fields = expected["requestExtFields"].as_object().unwrap();
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (key, value) in expected_fields {
        assert_eq!(
            actual_fields.get(key).map(String::as_str),
            value.as_str(),
            "field {key}"
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn delete_topic_in_nameserver_preserves_java_broker_error() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteTopicInNameserver"]["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        expected["responseCode"].as_i64().unwrap() as i32,
        Some(expected["responseRemark"].as_str().unwrap().to_owned()),
        None,
        None,
    )
    .await;
    let (api, client) = api().await;
    let mut header = DeleteTopicFromNamesrvRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_cluster_name(Some("cluster-a".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    header.set_lo(Some(true));
    let error = api
        .delete_topic_in_nameserver(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::DELETE_TOPIC_IN_NAMESRV,
            peer,
            code,
            remark: Some(remark),
        } if peer == addr
            && code == expected["responseCode"].as_i64().unwrap() as i32
            && remark == expected["responseRemark"].as_str().unwrap()
    ));
    assert_eq!(peer.await.unwrap().code, request::DELETE_TOPIC_IN_NAMESRV);
    client.shutdown().await;
}

#[tokio::test]
async fn delete_kv_config_matches_mq_client_admin_success_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteKvConfig"]["success"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let mut header = DeleteKvConfigRequestHeader::default();
    header.set_namespace(Some("config-namespace".into()));
    header.set_key(Some("config-key".into()));

    api.delete_kv_config(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(
        command.code,
        expected["requestCode"].as_i64().unwrap() as i32
    );
    assert!(command.body.is_none());
    let actual_fields = command.ext_fields.as_ref().unwrap();
    let expected_fields = expected["requestExtFields"].as_object().unwrap();
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (key, value) in expected_fields {
        assert_eq!(
            actual_fields.get(key).map(String::as_str),
            value.as_str(),
            "field {key}"
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn delete_kv_config_preserves_java_nameserver_error() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteKvConfig"]["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        expected["responseCode"].as_i64().unwrap() as i32,
        Some(expected["responseRemark"].as_str().unwrap().to_owned()),
        None,
        None,
    )
    .await;
    let (api, client) = api().await;
    let mut header = DeleteKvConfigRequestHeader::default();
    header.set_namespace(Some("config-namespace".into()));
    header.set_key(Some("config-key".into()));
    let error = api
        .delete_kv_config(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::DELETE_KV_CONFIG,
            peer,
            code,
            remark: Some(remark),
        } if peer == addr
            && code == expected["responseCode"].as_i64().unwrap() as i32
            && remark == expected["responseRemark"].as_str().unwrap()
    ));
    assert_eq!(peer.await.unwrap().code, request::DELETE_KV_CONFIG);
    client.shutdown().await;
}

#[tokio::test]
async fn delete_subscription_group_matches_mq_client_admin_success_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteSubscriptionGroup"]["success"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let mut header = DeleteSubscriptionGroupRequestHeader::default();
    header.set_group_name(Some("workers".into()));
    header.set_clean_offset(true);
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));

    api.delete_subscription_group(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(
        command.code,
        expected["requestCode"].as_i64().unwrap() as i32
    );
    assert!(command.body.is_none());
    let actual_fields = command.ext_fields.as_ref().unwrap();
    let expected_fields = expected["requestExtFields"].as_object().unwrap();
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (key, value) in expected_fields {
        assert_eq!(
            actual_fields.get(key).map(String::as_str),
            value.as_str(),
            "field {key}"
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn delete_subscription_group_preserves_java_broker_error() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteSubscriptionGroup"]["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        expected["responseCode"].as_i64().unwrap() as i32,
        Some(expected["responseRemark"].as_str().unwrap().to_owned()),
        None,
        None,
    )
    .await;
    let (api, client) = api().await;
    let mut header = DeleteSubscriptionGroupRequestHeader::default();
    header.set_group_name(Some("workers".into()));
    header.set_clean_offset(true);
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    let error = api
        .delete_subscription_group(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::DELETE_SUBSCRIPTIONGROUP,
            peer,
            code,
            remark: Some(remark),
        } if peer == addr
            && code == expected["responseCode"].as_i64().unwrap() as i32
            && remark == expected["responseRemark"].as_str().unwrap()
    ));
    assert_eq!(peer.await.unwrap().code, request::DELETE_SUBSCRIPTIONGROUP);
    client.shutdown().await;
}

#[tokio::test]
async fn invoke_broker_to_reset_offset_matches_mq_client_admin_success_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["invokeBrokerToResetOffset"]["success"];
    let body = expected["responseBody"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (api, client) = api().await;

    let result = api
        .invoke_broker_to_reset_offset(
            &addr,
            &java_reset_offset_header(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap()
        .unwrap()
        .snapshot();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::INVOKE_BROKER_TO_RESET_OFFSET);
    assert!(command.body.is_none());
    let actual_fields = command.ext_fields.as_ref().unwrap();
    let expected_fields = expected["requestExtFields"].as_object().unwrap();
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (key, value) in expected_fields {
        assert_eq!(
            actual_fields.get(key).map(String::as_str),
            value.as_str(),
            "field {key}"
        );
    }
    assert_eq!(result.len(), expected["result"].as_array().unwrap().len());
    for (queue, offset) in result {
        let expected_entry = expected["result"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| {
                entry["topic"] == queue.topic().unwrap()
                    && entry["brokerName"] == queue.broker_name().unwrap()
                    && entry["queueId"] == queue.queue_id()
            })
            .unwrap();
        assert_eq!(offset, Some(expected_entry["offset"].as_i64().unwrap()));
    }
    client.shutdown().await;
}

#[tokio::test]
async fn invoke_broker_to_reset_offset_preserves_java_broker_error() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["invokeBrokerToResetOffset"]["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        expected["responseCode"].as_i64().unwrap() as i32,
        Some(expected["responseRemark"].as_str().unwrap().to_owned()),
        None,
        None,
    )
    .await;
    let (api, client) = api().await;
    let error = api
        .invoke_broker_to_reset_offset(
            &addr,
            &java_reset_offset_header(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::INVOKE_BROKER_TO_RESET_OFFSET,
            peer,
            code,
            remark: Some(remark),
        } if peer == addr
            && code == expected["responseCode"].as_i64().unwrap() as i32
            && remark == expected["responseRemark"].as_str().unwrap()
    ));
    assert_eq!(
        peer.await.unwrap().code,
        request::INVOKE_BROKER_TO_RESET_OFFSET
    );
    client.shutdown().await;
}

#[tokio::test]
async fn invoke_broker_to_reset_offset_maps_success_without_body_like_java() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["invokeBrokerToResetOffset"]["successWithoutBody"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let error = api
        .invoke_broker_to_reset_offset(
            &addr,
            &java_reset_offset_header(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::INVOKE_BROKER_TO_RESET_OFFSET,
            peer,
            code,
            remark: None,
        } if peer == addr && code == expected["responseCode"].as_i64().unwrap() as i32
    ));
    assert_eq!(
        peer.await.unwrap().code,
        request::INVOKE_BROKER_TO_RESET_OFFSET
    );
    client.shutdown().await;
}

#[tokio::test]
async fn query_subscription_by_consumer_sends_java_header_and_decodes_subscription() {
    let body = serde_json::json!({
        "subscriptionData": {
            "classFilterMode": false,
            "topic": "orders",
            "subString": "tag-a",
            "subVersion": 123,
            "expressionType": "TAG",
            "tagsSet": [],
            "codeSet": []
        },
        "group": "workers",
        "topic": "orders"
    });
    let (addr, peer) =
        scripted_peer(response::SUCCESS, None, Some(body.to_string().into_bytes())).await;
    let (api, client) = api().await;
    let mut header = QuerySubscriptionByConsumerRequestHeader::default();
    header.set_group(Some("workers".into()));
    header.set_topic(Some("orders".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    header.set_lo(Some(true));

    let subscription = api
        .query_subscription_by_consumer(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .expect("the Java response contains SubscriptionData");

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::QUERY_SUBSCRIPTION_BY_CONSUMER);
    let mut expected_fields = HeaderMap::new();
    header.encode_fields(&mut expected_fields);
    assert_eq!(command.ext_fields, Some(expected_fields.into()));
    assert_eq!(
        subscription.with(|data: &SubscriptionData| {
            (
                data.get_topic().map(str::to_owned),
                data.get_sub_string().map(str::to_owned),
                data.get_sub_version(),
            )
        }),
        (Some("orders".into()), Some("tag-a".into()), 123)
    );
    client.shutdown().await;
}

#[tokio::test]
async fn query_subscription_by_consumer_preserves_nullable_subscription_data() {
    let (addr, peer) = scripted_peer(
        response::SUCCESS,
        None,
        Some(br#"{"subscriptionData":null,"group":"workers","topic":"orders"}"#.to_vec()),
    )
    .await;
    let (api, client) = api().await;
    let mut header = QuerySubscriptionByConsumerRequestHeader::default();
    header.set_group(Some("workers".into()));
    header.set_topic(Some("orders".into()));

    let result = api
        .query_subscription_by_consumer(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    assert!(result.is_none());
    assert_eq!(
        peer.await.unwrap().code,
        request::QUERY_SUBSCRIPTION_BY_CONSUMER
    );
    client.shutdown().await;
}

#[tokio::test]
async fn query_subscription_by_consumer_preserves_broker_error() {
    let (addr, peer) = scripted_peer(
        response::SUBSCRIPTION_NOT_EXIST,
        None,
        Some(b"ignored on error".to_vec()),
    )
    .await;
    let (api, client) = api().await;
    let mut header = QuerySubscriptionByConsumerRequestHeader::default();
    header.set_group(Some("workers".into()));
    header.set_topic(Some("orders".into()));

    let error = api
        .query_subscription_by_consumer(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::QUERY_SUBSCRIPTION_BY_CONSUMER,
            peer,
            code: response::SUBSCRIPTION_NOT_EXIST,
            remark: None,
        } if peer == addr
    ));
    assert_eq!(
        peer.await.unwrap().code,
        request::QUERY_SUBSCRIPTION_BY_CONSUMER
    );
    client.shutdown().await;
}

#[tokio::test]
async fn get_topic_stats_info_sends_java_header_and_decodes_topic_stats_table() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/topic-stats-table.json"
    ))
    .unwrap();
    let body = fixture["populatedJson"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (api, client) = api().await;
    let mut header = GetTopicStatsInfoRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    header.set_lo(Some(true));

    let stats = api
        .get_topic_stats_info(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_TOPIC_STATS_INFO);
    let mut expected_fields = HeaderMap::new();
    header.encode_fields(&mut expected_fields);
    assert_eq!(command.ext_fields, Some(expected_fields.into()));
    assert_eq!(stats.get_topic_put_tps(), 1.25);
    let table = stats.get_offset_table().unwrap();
    assert_eq!(table.with(|table| table.len()), 1);
    assert_eq!(
        table.with(|table| {
            table[&rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 1)]
                .as_ref()
                .unwrap()
                .with(|offset| offset.get_max_offset())
        }),
        16
    );
    client.shutdown().await;
}

#[tokio::test]
async fn get_topic_stats_info_preserves_java_null_body_results() {
    for body in [None, Some(b"null".to_vec())] {
        let (addr, peer) = scripted_peer(response::SUCCESS, None, body).await;
        let (api, client) = api().await;
        let mut header = GetTopicStatsInfoRequestHeader::default();
        header.set_topic(Some("orders".into()));
        let result = api
            .get_topic_stats_info(&addr, &header, Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert!(result.is_none());
        assert_eq!(peer.await.unwrap().code, request::GET_TOPIC_STATS_INFO);
        client.shutdown().await;
    }
}

#[tokio::test]
async fn get_topic_stats_info_preserves_broker_errors_and_rejects_malformed_body() {
    let (addr, peer) = scripted_peer(
        response::TOPIC_NOT_EXIST,
        None,
        Some(b"ignored on error".to_vec()),
    )
    .await;
    let (client_api, client) = api().await;
    let mut header = GetTopicStatsInfoRequestHeader::default();
    header.set_topic(Some("orders".into()));
    let error = client_api
        .get_topic_stats_info(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::GET_TOPIC_STATS_INFO,
            peer,
            code: response::TOPIC_NOT_EXIST,
            remark: None,
        } if peer == addr
    ));
    assert_eq!(peer.await.unwrap().code, request::GET_TOPIC_STATS_INFO);
    client.shutdown().await;

    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(b"not-json".to_vec())).await;
    let (api, client) = api().await;
    let mut header = GetTopicStatsInfoRequestHeader::default();
    header.set_topic(Some("orders".into()));
    let error = api
        .get_topic_stats_info(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::MalformedResponse {
            request_code: request::GET_TOPIC_STATS_INFO,
            peer,
            ..
        } if peer == addr
    ));
    assert_eq!(peer.await.unwrap().code, request::GET_TOPIC_STATS_INFO);
    client.shutdown().await;
}

#[tokio::test]
async fn query_consume_time_span_matches_mq_client_admin_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let success_case = &fixture["success"];
    let body = success_case["responseBody"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (client_api, client) = api().await;
    let mut header = QueryConsumeTimeSpanRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_group(Some("workers".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    let spans = client_api
        .query_consume_time_span(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(
        command.code,
        success_case["requestCode"].as_i64().unwrap() as i32
    );
    let actual_fields = command.ext_fields.as_ref().unwrap();
    let expected_fields = success_case["requestExtFields"].as_object().unwrap();
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (key, value) in expected_fields {
        assert_eq!(
            actual_fields.get(key).map(String::as_str),
            value.as_str(),
            "field {key}"
        );
    }
    assert_eq!(
        spans.len(),
        success_case["result"].as_array().unwrap().len()
    );
    let span = spans[0].as_ref().unwrap();
    let expected = &success_case["result"][0];
    let queue = span.get_message_queue().unwrap().snapshot();
    assert_eq!(queue.topic(), expected["topic"].as_str());
    assert_eq!(queue.broker_name(), expected["brokerName"].as_str());
    assert_eq!(
        queue.queue_id(),
        expected["queueId"].as_i64().unwrap() as i32
    );
    assert_eq!(
        span.get_min_time_stamp(),
        expected["minTimeStamp"].as_i64().unwrap()
    );
    assert_eq!(
        span.get_max_time_stamp(),
        expected["maxTimeStamp"].as_i64().unwrap()
    );
    assert_eq!(
        span.get_consume_time_stamp(),
        expected["consumeTimeStamp"].as_i64().unwrap()
    );
    assert_eq!(
        span.get_delay_time(),
        expected["delayTime"].as_i64().unwrap()
    );
    client.shutdown().await;

    let error_case = &fixture["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().to_owned()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .query_consume_time_span(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::QUERY_CONSUME_TIME_SPAN,
            peer,
            code,
            remark: Some(remark),
        } if peer == addr
            && code == error_case["responseCode"].as_i64().unwrap() as i32
            && remark == error_case["responseRemark"].as_str().unwrap()
    ));
    assert_eq!(peer.await.unwrap().code, request::QUERY_CONSUME_TIME_SPAN);
    client.shutdown().await;
}

#[tokio::test]
async fn query_topic_consume_by_who_sends_java_header_and_decodes_group_list() {
    let (addr, peer) = scripted_peer(
        response::SUCCESS,
        None,
        Some(br#"{"groupList":["workers",null]}"#.to_vec()),
    )
    .await;
    let (api, client) = api().await;
    let mut header = QueryTopicConsumeByWhoRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    header.set_lo(Some(true));

    let groups = api
        .query_topic_consume_by_who(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::QUERY_TOPIC_CONSUME_BY_WHO);
    let mut expected_fields = HeaderMap::new();
    header.encode_fields(&mut expected_fields);
    assert_eq!(command.ext_fields, Some(expected_fields.into()));
    let group_list = groups.get_group_list().unwrap();
    assert!(group_list.with(|values| values.contains(&Some("workers".into()))));
    assert!(group_list.with(|values| values.contains(&None)));
    client.shutdown().await;
}

#[tokio::test]
async fn query_topic_consume_by_who_preserves_nullable_group_list() {
    let (addr, peer) = scripted_peer(
        response::SUCCESS,
        None,
        Some(br#"{"groupList":null}"#.to_vec()),
    )
    .await;
    let (api, client) = api().await;
    let mut header = QueryTopicConsumeByWhoRequestHeader::default();
    header.set_topic(Some("orders".into()));

    let result = api
        .query_topic_consume_by_who(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    assert!(result.get_group_list().is_none());
    assert_eq!(
        peer.await.unwrap().code,
        request::QUERY_TOPIC_CONSUME_BY_WHO
    );
    client.shutdown().await;
}

#[tokio::test]
async fn query_topic_consume_by_who_preserves_broker_error() {
    let (addr, peer) = scripted_peer(
        response::TOPIC_NOT_EXIST,
        None,
        Some(b"ignored on error".to_vec()),
    )
    .await;
    let (api, client) = api().await;
    let mut header = QueryTopicConsumeByWhoRequestHeader::default();
    header.set_topic(Some("orders".into()));

    let error = api
        .query_topic_consume_by_who(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::QUERY_TOPIC_CONSUME_BY_WHO,
            peer,
            code: response::TOPIC_NOT_EXIST,
            remark: None,
        } if peer == addr
    ));
    assert_eq!(
        peer.await.unwrap().code,
        request::QUERY_TOPIC_CONSUME_BY_WHO
    );
    client.shutdown().await;
}

#[tokio::test]
async fn offset_admin_rpcs_preserve_broker_errors_and_reject_missing_response_fields() {
    for code in [
        request::SEARCH_OFFSET_BY_TIMESTAMP,
        request::GET_MAX_OFFSET,
        request::GET_MIN_OFFSET,
        request::GET_EARLIEST_MSG_STORETIME,
    ] {
        let (error_address, error_peer) = scripted_peer(response::NO_PERMISSION, None, None).await;
        let (api, client) = api().await;
        let error = invoke_offset_operation(&api, &error_address, code)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RpcError::Response {
                request_code,
                code: response::NO_PERMISSION,
                ..
            } if request_code == code
        ));
        assert_eq!(error_peer.await.unwrap().code, code);

        let (malformed_address, malformed_peer) =
            scripted_peer(response::SUCCESS, Some(HeaderMap::new()), None).await;
        let malformed = invoke_offset_operation(&api, &malformed_address, code)
            .await
            .unwrap_err();
        assert!(matches!(
            malformed,
            RpcError::MalformedResponse { request_code, .. } if request_code == code
        ));
        assert_eq!(malformed_peer.await.unwrap().code, code);
        client.shutdown().await;
    }
}

#[tokio::test]
async fn query_message_sets_unique_flag_and_decodes_java_message_records() {
    let mut fields = HeaderMap::new();
    java_query_message_response_header(1_700_000_200_041, 1_700_000_000_041)
        .encode_fields(&mut fields);
    let message_record = java_message_record("ipv4-v1");
    let (addr, peer) = scripted_peer(
        response::SUCCESS,
        Some(fields),
        Some(message_record.clone()),
    )
    .await;
    let (api, client) = api().await;
    let header = {
        let mut __phase18_header_4 = QueryMessageRequestHeader::default();
        __phase18_header_4.set_topic(Some("orders".into()));
        __phase18_header_4.set_key(Some("producer-unique-1".into()));
        __phase18_header_4.set_max_num(Some(32));
        __phase18_header_4.set_begin_timestamp(Some(1_700_000_000_000));
        __phase18_header_4.set_end_timestamp(Some(1_700_000_300_000));
        __phase18_header_4.set_index_type(Some("UNIQUE_KEY".into()));
        __phase18_header_4
    };

    let result = api
        .query_message(
            &addr,
            &header,
            true,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::QUERY_MESSAGE);
    let mut expected_fields = HeaderMap::new();
    header.encode_fields(&mut expected_fields);
    expected_fields.insert("_UNIQUE_KEY_QUERY".into(), "true".into());
    assert_eq!(command.ext_fields, Some(expected_fields.into()));
    assert_eq!(result.get_index_last_update_timestamp(), 1_700_000_200_041);
    let mut messages: Vec<MessageExt> = decode_many_compatible(&message_record, Default::default());
    for message in &mut messages {
        let offset_id = message.message_id().map(str::to_owned);
        let client_id = message
            .properties()
            .get("UNIQ_KEY")
            .cloned()
            .or_else(|| offset_id.clone());
        message.set_offset_message_id(offset_id);
        message.set_message_id(client_id);
    }
    let returned_messages = result
        .get_message_list()
        .unwrap()
        .snapshot()
        .into_iter()
        .flatten()
        .map(|message| message.snapshot())
        .collect::<Vec<_>>();
    let returned_values = returned_messages
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected_values = messages
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(returned_values, expected_values);
    client.shutdown().await;
}

#[tokio::test]
async fn query_message_keeps_all_24_valid_compressed_records() {
    const BODY_SIZE: usize = 3 * 1024 * 1024;

    let mut message = MessageExt::new();
    message.set_topic("orders");
    message.set_body(vec![0x5a; BODY_SIZE]);
    message.set_sys_flag(rocketmq_classic_common::sysflag::message::COMPRESSED_FLAG);
    message
        .set_system_property("UNIQ_KEY", "producer-unique-1")
        .unwrap();
    message.set_born_host(Some(MessageHostAddress::from_ip_address(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        10_911,
    )));
    message.set_store_host(Some(MessageHostAddress::from_ip_address(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        10_912,
    )));
    let record = encode_message_ext(&message, true).expect("compressed message encodes");
    let mut batch = Vec::new();
    for _ in 0..24 {
        batch.extend_from_slice(&record);
    }
    let mut fields = HeaderMap::new();
    java_query_message_response_header(1_700_000_200_041, 1_700_000_000_041)
        .encode_fields(&mut fields);
    let (addr, peer) = scripted_peer(response::SUCCESS, Some(fields), Some(batch)).await;
    let (api, client) = api().await;
    let mut header = QueryMessageRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_key(Some("producer-unique-1".into()));
    header.set_max_num(Some(32));

    let result = api
        .query_message(
            &addr,
            &header,
            true,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();

    peer.await.unwrap();
    let messages = result.get_message_list().unwrap().snapshot();
    assert_eq!(messages.len(), 24);
    assert!(
        messages
            .iter()
            .all(|message| message.as_ref().unwrap().with(|message| {
                message.body().len() == BODY_SIZE
                    && message.message_id() == Some("producer-unique-1")
            }))
    );
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_query_message_matches_invoked_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let query = &fixture["queryMessage"];
    let response_body = base64::engine::general_purpose::STANDARD
        .decode(query["responseBodyBase64"].as_str().unwrap())
        .unwrap();

    for case_name in [
        "keyQueryDecompressed",
        "keyQueryCompressed",
        "uniqueQuery",
        "emptyKeyQuery",
        "nonBreakingSpaceQuery",
        "javaWhitespaceQuery",
        "nextLineQuery",
        "fileSeparatorQuery",
    ] {
        let case = &query[case_name];
        let unique_key = case["uniqueKeyFlag"].as_bool().unwrap();
        let decompress_body = case["decompressBody"].as_bool().unwrap();
        let query_key = case["requestExtFields"]["key"].as_str().unwrap();
        let header = {
            let mut __phase18_header_5 = QueryMessageRequestHeader::default();
            __phase18_header_5.set_topic(Some("orders".into()));
            __phase18_header_5.set_key(Some(query_key.to_owned()));
            __phase18_header_5.set_max_num(Some(3));
            __phase18_header_5.set_begin_timestamp(Some(1_700_000_000_000));
            __phase18_header_5.set_end_timestamp(Some(1_700_000_300_000));
            __phase18_header_5.set_index_type(Some("KEY".into()));
            __phase18_header_5
        };
        let (addr, peer) =
            scripted_peer(response::SUCCESS, None, Some(response_body.clone())).await;
        let (api, client) = api().await;
        let messages = api
            .mq_client_admin()
            .query_message(
                &addr,
                unique_key,
                decompress_body,
                &header,
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();

        let command = peer.await.unwrap();
        assert_eq!(command.code, request::QUERY_MESSAGE);
        assert_eq!(
            serde_json::to_value(command.ext_fields).unwrap(),
            case["requestExtFields"]
        );
        let actual = messages
            .iter()
            .map(|message| {
                serde_json::json!({
                    "topic": message.topic(),
                    "messageId": message.message_id(),
                    "offsetMessageId": message.offset_message_id(),
                    "bodyBase64": base64::engine::general_purpose::STANDARD.encode(message.body().as_ref()),
                    "sysFlag": message.sys_flag(),
                    "keys": message.keys(),
                    "properties": serde_json::to_value(message.properties()).unwrap(),
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(Value::Array(actual), case["result"]);
        client.shutdown().await;
    }

    let header = {
        let mut __phase18_header_6 = QueryMessageRequestHeader::default();
        __phase18_header_6.set_topic(Some("orders".into()));
        __phase18_header_6.set_key(Some("green".into()));
        __phase18_header_6.set_max_num(Some(3));
        __phase18_header_6.set_begin_timestamp(Some(1_700_000_000_000));
        __phase18_header_6.set_end_timestamp(Some(1_700_000_300_000));
        __phase18_header_6.set_index_type(Some("KEY".into()));
        __phase18_header_6
    };
    let (not_found_address, not_found_peer) = scripted_peer_with_remark(
        response::QUERY_NOT_FOUND,
        Some("query not found".into()),
        None,
        None,
    )
    .await;
    let (not_found_api, client) = api().await;
    let messages = not_found_api
        .mq_client_admin()
        .query_message(
            &not_found_address,
            false,
            true,
            &header,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert!(messages.is_empty());
    let command = not_found_peer.await.unwrap();
    assert_eq!(command.code, request::QUERY_MESSAGE);
    client.shutdown().await;

    let (error_address, error_peer) = scripted_peer_with_remark(
        response::SYSTEM_ERROR,
        Some("query failed".into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .query_message(
            &error_address,
            false,
            true,
            &header,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    let command = error_peer.await.unwrap();
    assert_eq!(command.code, request::QUERY_MESSAGE);
    assert!(
        matches!(error, RpcError::Response { code: 1, remark: Some(remark), .. } if remark == "query failed")
    );
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_view_message_matches_invoked_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let view = &fixture["viewMessage"];
    let success = &view["success"];
    let response_body = base64::engine::general_purpose::STANDARD
        .decode(success["responseBodyBase64"].as_str().unwrap())
        .unwrap();
    let header = {
        let mut __phase18_header_7 = ViewMessageRequestHeader::default();
        __phase18_header_7.set_topic(Some("orders".into()));
        __phase18_header_7.set_offset(Some(4));
        __phase18_header_7
    };
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(response_body)).await;
    let (view_api, client) = api().await;
    let message = view_api
        .mq_client_admin()
        .view_message(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::VIEW_MESSAGE_BY_ID);
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        success["requestExtFields"]
    );
    assert_eq!(
        serde_json::json!({
            "topic": message.topic(),
            "messageId": message.message_id(),
            "offsetMessageId": message.offset_message_id(),
            "bodyBase64": base64::engine::general_purpose::STANDARD.encode(message.body().as_ref()),
            "sysFlag": message.sys_flag(),
            "keys": message.keys(),
            "properties": serde_json::to_value(message.properties()).unwrap(),
        }),
        success["result"]
    );
    client.shutdown().await;

    let error_case = &view["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .view_message(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::VIEW_MESSAGE_BY_ID);
    assert!(matches!(
        error,
        RpcError::Response { code: 17, remark: Some(remark), .. }
            if remark == "view topic not found"
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_consume_message_directly_matches_invoked_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let consume = &fixture["consumeMessageDirectly"];
    let success = &consume["success"];
    let header = {
        let mut __phase18_header_8 = ConsumeMessageDirectlyResultRequestHeader::default();
        __phase18_header_8.set_consumer_group("workers".into());
        __phase18_header_8.set_client_id(Some("client-a".into()));
        __phase18_header_8.set_msg_id(Some("message-1".into()));
        __phase18_header_8.set_broker_name(Some("broker-a".into()));
        __phase18_header_8.set_topic(Some("orders".into()));
        __phase18_header_8.set_topic_sys_flag(Some(3));
        __phase18_header_8.set_group_sys_flag(Some(5));
        __phase18_header_8.set_namespace(Some("tenant".into()));
        __phase18_header_8.set_namespaced(Some(true));
        __phase18_header_8.set_bname(Some("rpc-broker-a".into()));
        __phase18_header_8.set_oneway(Some(false));
        __phase18_header_8
    };
    let body = success["responseBody"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (admin_api, client) = api().await;
    let result = admin_api
        .mq_client_admin()
        .consume_message_directly(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::CONSUME_MESSAGE_DIRECTLY);
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        success["requestExtFields"]
    );
    assert_eq!(serde_json::to_value(result).unwrap(), success["result"]);
    client.shutdown().await;

    let error_case = &consume["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .consume_message_directly(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::CONSUME_MESSAGE_DIRECTLY);
    assert!(matches!(
        error,
        RpcError::Response { code: 1, remark: Some(remark), .. }
            if remark == "direct consume failed"
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn consume_message_directly_rejects_malformed_success_body() {
    let header = {
        let mut __phase18_header_9 = ConsumeMessageDirectlyResultRequestHeader::default();
        __phase18_header_9.set_consumer_group("workers".into());
        __phase18_header_9
    };
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(b"{".to_vec())).await;
    let (api, client) = api().await;
    let error = api
        .mq_client_admin()
        .consume_message_directly(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::CONSUME_MESSAGE_DIRECTLY);
    assert!(matches!(
        error,
        RpcError::MalformedResponse {
            request_code: request::CONSUME_MESSAGE_DIRECTLY,
            ..
        }
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_get_broker_cluster_info_matches_invoked_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let cluster = &fixture["getBrokerClusterInfo"];
    let success = &cluster["success"];
    let body = success["responseBody"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (admin_api, client) = api().await;
    let result = admin_api
        .mq_client_admin()
        .get_broker_cluster_info(&addr, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_BROKER_CLUSTER_INFO);
    assert!(command.ext_fields.is_none());
    assert_eq!(serde_json::to_value(result).unwrap(), success["result"]);
    client.shutdown().await;

    let no_body_case = &cluster["successWithoutBody"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (no_body_api, client) = api().await;
    let result = no_body_api
        .mq_client_admin()
        .get_broker_cluster_info(&addr, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_BROKER_CLUSTER_INFO);
    assert!(command.ext_fields.is_none());
    assert_eq!(
        serde_json::to_value(result).unwrap(),
        no_body_case["result"]
    );
    client.shutdown().await;

    let error_case = &cluster["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .get_broker_cluster_info(&addr, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_BROKER_CLUSTER_INFO);
    assert!(matches!(
        error,
        RpcError::Response { code: 1, remark: Some(remark), .. }
            if remark == "cluster info unavailable"
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_get_consumer_connection_list_matches_invoked_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let connection_list = &fixture["getConsumerConnectionList"];
    let success = &connection_list["success"];
    let header = {
        let mut __phase18_header_10 = GetConsumerConnectionListRequestHeader::default();
        __phase18_header_10.set_consumer_group(Some("workers".into()));
        __phase18_header_10.set_namespace(Some("tenant".into()));
        __phase18_header_10.set_namespaced(Some(true));
        __phase18_header_10.set_broker_name(Some("broker-a".into()));
        __phase18_header_10.set_oneway(Some(false));
        __phase18_header_10
    };
    let body = success["responseBody"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (admin_api, client) = api().await;
    let result = admin_api
        .mq_client_admin()
        .get_consumer_connection_list(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_CONNECTION_LIST);
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        success["requestExtFields"]
    );

    let connections = result
        .get_connection_set()
        .unwrap()
        .snapshot()
        .into_iter()
        .map(|connection| {
            connection.unwrap().with(|connection| {
                json!({
                    "clientId": connection.get_client_id(),
                    "clientAddr": connection.get_client_addr(),
                    "language": connection.get_language().map(|language| language.name()),
                    "version": connection.get_version(),
                })
            })
        })
        .collect::<Vec<_>>();

    let subscription_table = result.get_subscription_table().unwrap().snapshot();
    let subscription = subscription_table["orders"]
        .as_ref()
        .unwrap()
        .with(|subscription| {
            let mut tags = subscription
                .get_tags_set()
                .unwrap()
                .snapshot()
                .into_iter()
                .collect::<Vec<_>>();
            tags.sort();
            let mut codes = subscription
                .get_code_set()
                .unwrap()
                .snapshot()
                .into_iter()
                .collect::<Vec<_>>();
            codes.sort();
            json!({
                "classFilterMode": subscription.is_class_filter_mode(),
                "topic": subscription.get_topic(),
                "subString": subscription.get_sub_string(),
                "tagsSet": tags,
                "codeSet": codes,
                "subVersion": subscription.get_sub_version(),
                "expressionType": subscription.get_expression_type(),
            })
        });
    let actual = json!({
        "connectionSet": connections,
        "subscriptionTable": {"orders": subscription},
        "consumeType": result.get_consume_type().map(|value| value.name()),
        "messageModel": result.get_message_model().map(|value| value.name()),
        "consumeFromWhere": result.get_consume_from_where().map(|value| value.name()),
    });
    assert_eq!(actual, success["result"]);
    client.shutdown().await;

    let no_body_case = &connection_list["successWithoutBody"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (no_body_api, client) = api().await;
    let result = no_body_api
        .mq_client_admin()
        .get_consumer_connection_list(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_CONNECTION_LIST);
    assert_eq!(result.is_none(), no_body_case["result"].is_null());
    client.shutdown().await;

    let error_case = &connection_list["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .get_consumer_connection_list(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_CONNECTION_LIST);
    assert!(matches!(
        error,
        RpcError::Response { code: 1, remark: Some(remark), .. }
            if remark == "consumer connections unavailable"
    ));
    client.shutdown().await;

    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(b"{".to_vec())).await;
    let (malformed_api, client) = api().await;
    let error = malformed_api
        .mq_client_admin()
        .get_consumer_connection_list(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_CONNECTION_LIST);
    assert!(matches!(
        error,
        RpcError::MalformedResponse {
            request_code: request::GET_CONSUMER_CONNECTION_LIST,
            ..
        }
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_get_consume_stats_matches_invoked_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let consume_stats = &fixture["getConsumeStats"];
    assert_eq!(
        consume_stats["method"],
        "public abstract java.util.concurrent.CompletableFuture<org.apache.rocketmq.remoting.protocol.admin.ConsumeStats> org.apache.rocketmq.client.MqClientAdmin.getConsumeStats(java.lang.String,org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader,long)"
    );
    let success = &consume_stats["success"];
    let header = {
        let mut __phase18_header_11 = GetConsumeStatsRequestHeader::default();
        __phase18_header_11.set_consumer_group(Some("workers".into()));
        __phase18_header_11.set_topic(Some("ignored-topic".into()));
        __phase18_header_11.set_topic_list(Some("orders;payments;".into()));
        __phase18_header_11.set_namespace(Some("tenant".into()));
        __phase18_header_11.set_namespaced(Some(true));
        __phase18_header_11.set_broker_name(Some("broker-a".into()));
        __phase18_header_11.set_oneway(Some(false));
        __phase18_header_11
    };
    let body = success["responseBody"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (admin_api, client) = api().await;
    let result = admin_api
        .mq_client_admin()
        .get_consume_stats(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUME_STATS);
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        success["requestExtFields"]
    );
    assert_eq!(result.get_consume_tps(), 1.25);
    assert_eq!(result.compute_total_diff().unwrap(), 13);
    assert_eq!(result.compute_inflight_total_diff().unwrap(), 8);
    let offsets = result.get_offset_table().unwrap().snapshot();
    assert_eq!(offsets.len(), 2);
    let orders = offsets
        [&rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 1)]
        .as_ref()
        .unwrap()
        .clone();
    assert_eq!(orders.with(|offset| offset.get_last_timestamp()), 1000);
    client.shutdown().await;

    let no_body_case = &consume_stats["successWithoutBody"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (no_body_api, client) = api().await;
    let result = no_body_api
        .mq_client_admin()
        .get_consume_stats(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUME_STATS);
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        no_body_case["requestExtFields"]
    );
    assert!(result.is_none());
    client.shutdown().await;

    let error_case = &consume_stats["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .get_consume_stats(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUME_STATS);
    assert!(matches!(
        error,
        RpcError::Response { code: 1, remark: Some(remark), .. }
            if remark == "consume stats unavailable"
    ));
    client.shutdown().await;

    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(b"{".to_vec())).await;
    let (malformed_api, client) = api().await;
    let error = malformed_api
        .mq_client_admin()
        .get_consume_stats(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUME_STATS);
    assert!(matches!(
        error,
        RpcError::MalformedResponse {
            request_code: request::GET_CONSUME_STATS,
            ..
        }
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_get_consumer_running_info_matches_invoked_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let operation = &fixture["getConsumerRunningInfo"];
    assert_eq!(
        operation["method"],
        "public abstract java.util.concurrent.CompletableFuture<org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo> org.apache.rocketmq.client.MqClientAdmin.getConsumerRunningInfo(java.lang.String,org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader,long)"
    );
    let header = {
        let mut __phase18_header_12 = GetConsumerRunningInfoRequestHeader::default();
        __phase18_header_12.set_consumer_group("workers".into());
        __phase18_header_12.set_client_id("client-a".into());
        __phase18_header_12.set_jstack_enable(true);
        __phase18_header_12.set_namespace(Some("tenant".into()));
        __phase18_header_12.set_namespaced(Some(true));
        __phase18_header_12.set_broker_name(Some("broker-a".into()));
        __phase18_header_12.set_oneway(Some(false));
        __phase18_header_12
    };
    let success = &operation["success"];
    let body = success["responseBody"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (admin_api, client) = api().await;
    let result = admin_api
        .mq_client_admin()
        .get_consumer_running_info(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_RUNNING_INFO);
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        success["requestExtFields"]
    );
    let properties = result.get_properties().unwrap().snapshot();
    let subscription_set = result.get_subscription_set().unwrap().snapshot();
    let mq_table = result.get_mq_table().unwrap().snapshot();
    let mq_pop_table = result.get_mq_pop_table().unwrap().snapshot();
    let status_table = result.get_status_table().unwrap().snapshot();
    let user_consumer_info = result.get_user_consumer_info().unwrap().snapshot();
    assert_eq!(
        properties.get("PROP_CONSUME_TYPE").map(String::as_str),
        Some("CONSUME_PASSIVELY")
    );
    assert_eq!(subscription_set.len(), 1);
    assert_eq!(subscription_set[0].topic.as_deref(), Some("orders"));
    assert_eq!(mq_table.len(), 1);
    assert_eq!(mq_table[0].0.queue_id(), 1);
    assert_eq!(mq_table[0].1.get_commit_offset(), 17);
    assert_eq!(mq_pop_table[0].1.get_wait_ack_count(), 2);
    assert_eq!(status_table["orders"].get_consume_failed_msgs(), 6);
    assert_eq!(
        user_consumer_info.get("fixture").map(String::as_str),
        Some("value")
    );
    assert_eq!(result.get_jstack(), Some("jstack-fixture"));
    assert_eq!(
        success["result"]["subscriptionSetSize"].as_u64(),
        Some(subscription_set.len() as u64)
    );
    assert_eq!(success["result"]["mqTableSize"], mq_table.len());
    assert_eq!(success["result"]["mqPopTableSize"], mq_pop_table.len());
    client.shutdown().await;

    let no_body_case = &operation["successWithoutBody"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (no_body_api, client) = api().await;
    let result = no_body_api
        .mq_client_admin()
        .get_consumer_running_info(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_RUNNING_INFO);
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        no_body_case["requestExtFields"]
    );
    assert!(result.is_none());
    assert_eq!(no_body_case["resultIsNull"], result.is_none());
    client.shutdown().await;

    let error_case = &operation["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .get_consumer_running_info(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_RUNNING_INFO);
    assert!(matches!(
        error,
        RpcError::Response { code: 1, remark: Some(remark), .. }
            if remark == "consumer running info unavailable"
    ));
    client.shutdown().await;

    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(b"{".to_vec())).await;
    let (malformed_api, client) = api().await;
    let error = malformed_api
        .mq_client_admin()
        .get_consumer_running_info(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_RUNNING_INFO);
    assert!(matches!(
        error,
        RpcError::MalformedResponse {
            request_code: request::GET_CONSUMER_RUNNING_INFO,
            ..
        }
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_update_or_create_subscription_group_matches_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let operation = &fixture["updateOrCreateSubscriptionGroup"];
    assert_eq!(
        operation["method"],
        "public abstract java.util.concurrent.CompletableFuture<java.lang.Void> org.apache.rocketmq.client.MqClientAdmin.updateOrCreateSubscriptionGroup(java.lang.String,org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig,long)"
    );

    let mut config = SubscriptionGroupConfig::default();
    config.set_group_name(Some("workers".into()));
    config.set_consume_enable(false);
    config.set_consume_from_min_enable(false);
    config.set_consume_broadcast_enable(false);
    config.set_consume_message_orderly(true);
    config.set_retry_queue_nums(4);
    config.set_retry_max_times(8);
    let mut retry = GroupRetryPolicy::default();
    retry.set_type(Some(GroupRetryPolicyType::Exponential));
    retry.set_exponential_retry_policy(Some(SharedValue::new(ExponentialRetryPolicy::new(
        1_000, 60_000, 3,
    ))));
    config.set_group_retry_policy(Some(SharedValue::new(retry)));
    config.set_broker_id(7);
    config.set_which_broker_when_consume_slowly(9);
    config.set_notify_consumer_ids_changed_enable(false);
    config.set_group_sys_flag(3);
    config.set_consume_timeout_minute(12);
    let subscriptions: SubscriptionDataSet = [
        Some(SharedValue::new(SimpleSubscriptionData::new(
            Some("orders".into()),
            Some("TAG".into()),
            Some("tag-a || tag-b".into()),
            10,
        ))),
        Some(SharedValue::new(SimpleSubscriptionData::new(
            Some("payments".into()),
            Some("SQL92".into()),
            Some("amount > 0".into()),
            20,
        ))),
    ]
    .into_iter()
    .collect();
    config.set_subscription_data_set(Some(SharedValue::new(subscriptions)));
    config.get_attributes().unwrap().with_mut(|attributes| {
        attributes.insert("tenant".into(), Some("north".into()));
        attributes.insert("stage".into(), Some("blue".into()));
    });

    let success = &operation["success"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (admin_api, client) = api().await;
    admin_api
        .mq_client_admin()
        .update_or_create_subscription_group(
            &addr,
            &config,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP);
    assert_eq!(command.ext_fields, None);
    assert_eq!(
        command.body.as_deref().unwrap(),
        success["requestBody"].as_str().unwrap().as_bytes()
    );
    client.shutdown().await;

    let error_case = &operation["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .update_or_create_subscription_group(
            &addr,
            &config,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP);
    assert_eq!(command.ext_fields, None);
    assert_eq!(
        command.body.as_deref().unwrap(),
        error_case["requestBody"].as_str().unwrap().as_bytes()
    );
    assert!(matches!(
        error,
        RpcError::Response { code: 1, remark: Some(remark), .. }
            if remark == "subscription group update failed"
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn query_message_preserves_broker_response_code_and_remark() {
    let (addr, peer) = scripted_peer(17, None, None).await;
    let (api, client) = api().await;
    let error = api
        .query_message(
            &addr,
            &QueryMessageRequestHeader::default(),
            false,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::QUERY_MESSAGE);
    assert!(matches!(error, RpcError::Response { code: 17, .. }));
    client.shutdown().await;
}

#[tokio::test]
async fn query_message_rejects_success_without_response_header() {
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(vec![])).await;
    let (api, client) = api().await;
    let error = api
        .query_message(
            &addr,
            &QueryMessageRequestHeader::default(),
            false,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    peer.await.unwrap();
    assert!(matches!(
        error,
        RpcError::MalformedResponse {
            request_code: request::QUERY_MESSAGE,
            ..
        }
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn query_message_unboxes_nullable_java_timestamp_at_the_result_boundary() {
    let (api, client) = api().await;
    for timestamp in [None, Some("not-a-long")] {
        let mut fields = HeaderMap::from([("indexLastUpdatePhyoffset".into(), "7".into())]);
        if let Some(timestamp) = timestamp {
            fields.insert("indexLastUpdateTimestamp".into(), timestamp.into());
        }
        let (addr, peer) = scripted_peer(
            response::SUCCESS,
            Some(fields),
            Some(java_message_record("ipv4-v1")),
        )
        .await;
        let error = api
            .query_message(
                &addr,
                &QueryMessageRequestHeader::default(),
                false,
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap_err();
        peer.await.unwrap();
        assert!(matches!(
            error,
            RpcError::MalformedResponse {
                request_code: request::QUERY_MESSAGE,
                reason,
                ..
            } if reason.contains("indexLastUpdateTimestamp")
        ));
    }
    client.shutdown().await;
}

#[tokio::test]
async fn query_message_keeps_decoded_prefix_with_malformed_success_body_tail() {
    let mut fields = HeaderMap::new();
    java_query_message_response_header(200, 1).encode_fields(&mut fields);
    let mut body = java_message_record("ipv4-v1");
    body.push(0);
    let (addr, peer) = scripted_peer(response::SUCCESS, Some(fields), Some(body)).await;
    let (api, client) = api().await;
    let result = api
        .query_message(
            &addr,
            &QueryMessageRequestHeader::default(),
            false,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    peer.await.unwrap();
    let messages = result.get_message_list().unwrap().snapshot();
    assert_eq!(messages.len(), 1);
    assert!(messages[0].as_ref().unwrap().with(|message| {
        message.is_message_client_ext()
            && message.topic() == "orders"
            && message.message_id() == Some("producer-unique-1")
    }));
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_filters_unique_key_merges_watermarks_and_keeps_partial_successes() {
    let message_record = java_message_record("ipv4-v1");
    let decoded = decode_many_compatible(&message_record, Default::default());
    let offset_message_id = decoded[0].message_id().unwrap().to_owned();
    let unique_key = decoded[0].properties().get("UNIQ_KEY").unwrap().to_owned();
    assert_ne!(offset_message_id, unique_key);

    let mut first_header = HeaderMap::new();
    java_query_message_response_header(100, 1).encode_fields(&mut first_header);
    let (first_address, first_peer) = scripted_peer(
        response::SUCCESS,
        Some(first_header),
        Some(message_record.clone()),
    )
    .await;

    let mut second_header = HeaderMap::new();
    java_query_message_response_header(200, 2).encode_fields(&mut second_header);
    let (second_address, second_peer) = scripted_peer(
        response::SUCCESS,
        Some(second_header),
        Some(message_record.clone()),
    )
    .await;
    let (failed_address, failed_peer) = scripted_peer(response::NO_PERMISSION, None, None).await;

    let route = TopicRouteData::java_decode(
        serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [
                {"cluster": "cluster-a", "brokerName": "broker-a", "brokerAddrs": {"0": first_address}},
                {"cluster": "cluster-a", "brokerName": "broker-b", "brokerAddrs": {"0": second_address}},
                {"cluster": "cluster-a", "brokerName": "broker-c", "brokerAddrs": {"0": failed_address}}
            ],
            "filterServerTable": {}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let query = AdminQueryRequest::by_unique_key("orders", unique_key.clone(), 32, 10, 99);
    let (api, client) = api().await;

    let result = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(result.get_index_last_update_timestamp(), 200);
    let messages = result.get_message_list().unwrap().snapshot();
    assert_eq!(messages.len(), 2);
    assert!(messages.iter().flatten().all(|message| {
        message.with(|message| {
            message.message_id() == Some(unique_key.as_str())
                && message.offset_message_id() == Some(offset_message_id.as_str())
        })
    }));
    for peer in [first_peer, second_peer] {
        let command = peer.await.unwrap();
        assert_eq!(command.code, request::QUERY_MESSAGE);
        let fields = command.ext_fields.unwrap();
        assert_eq!(fields.get("key"), Some(&unique_key));
        assert_eq!(fields.get("maxNum"), Some(&"32".to_owned()));
        assert_eq!(fields.get("beginTimestamp"), Some(&"10".to_owned()));
        assert_eq!(fields.get("endTimestamp"), Some(&"99".to_owned()));
        assert_eq!(fields.get("indexType"), Some(&"U".to_owned()));
        assert_eq!(fields.get("_UNIQUE_KEY_QUERY"), Some(&"true".to_owned()));
    }
    assert_eq!(failed_peer.await.unwrap().code, request::QUERY_MESSAGE);
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_preserves_java_response_completion_order_and_time_bounds() {
    let first_record = java_message_record("ipv4-v1");
    let mut second_record = first_record.clone();
    let body_marker = b"hello-v1";
    let body_offset = second_record
        .windows(body_marker.len())
        .position(|window| window == body_marker)
        .expect("the Java fixture contains a fixed-length message body");
    second_record[body_offset..body_offset + body_marker.len()].copy_from_slice(b"hello-v2");

    let mut first_header = HeaderMap::new();
    java_query_message_response_header(200, 20).encode_fields(&mut first_header);
    let (first_address, first_peer) = scripted_peer_after(
        response::SUCCESS,
        Some(first_header),
        Some(first_record),
        Duration::from_millis(75),
    )
    .await;

    let mut second_header = HeaderMap::new();
    java_query_message_response_header(100, 10).encode_fields(&mut second_header);
    let (second_address, second_peer) =
        scripted_peer(response::SUCCESS, Some(second_header), Some(second_record)).await;

    let route = TopicRouteData::java_decode(
        serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [
                {"cluster": "cluster-a", "brokerName": "broker-a", "brokerAddrs": {"0": first_address}},
                {"cluster": "cluster-a", "brokerName": "broker-b", "brokerAddrs": {"0": second_address}}
            ],
            "filterServerTable": {}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let begin = 1_700_000_200_000;
    let end = 1_700_000_200_100;
    let query = AdminQueryRequest::by_unique_key("orders", "producer-unique-1", 32, begin, end);
    let (api, client) = api().await;

    let result = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(result.get_index_last_update_timestamp(), 200);
    let messages = result.get_message_list().unwrap().snapshot();
    assert_eq!(messages.len(), 2);
    assert!(
        messages[0]
            .as_ref()
            .unwrap()
            .with(|message| message.body().as_ref() == b"hello-v2")
    );
    assert!(
        messages[1]
            .as_ref()
            .unwrap()
            .with(|message| message.body().as_ref() == b"hello-v1")
    );
    for peer in [first_peer, second_peer] {
        let command = peer.await.unwrap();
        let fields = command.ext_fields.unwrap();
        assert_eq!(fields.get("beginTimestamp"), Some(&begin.to_string()));
        assert_eq!(fields.get("endTimestamp"), Some(&end.to_string()));
        assert_eq!(fields.get("maxNum"), Some(&"32".to_owned()));
    }
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_maps_only_terminal_broker_failures_to_java_no_message() {
    let (first_address, first_peer) = scripted_peer(response::NO_PERMISSION, None, None).await;
    let (second_address, second_peer) = scripted_peer(response::SYSTEM_ERROR, None, None).await;
    let route = TopicRouteData::java_decode(
        serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [
                {"cluster": "cluster-a", "brokerName": "broker-a", "brokerAddrs": {"0": first_address}},
                {"cluster": "cluster-a", "brokerName": "broker-b", "brokerAddrs": {"0": second_address}}
            ],
            "filterServerTable": {}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let query = AdminQueryRequest::by_key("orders", "order-key", 32, 10, 99);
    let (api, client) = api().await;

    let error = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::QUERY_MESSAGE,
            code: response::NO_MESSAGE,
            remark: Some(remark),
            ..
        } if remark == "query message by key finished, but no message."
    ));
    assert_eq!(first_peer.await.unwrap().code, request::QUERY_MESSAGE);
    assert_eq!(second_peer.await.unwrap().code, request::QUERY_MESSAGE);
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_returns_java_no_message_error_after_filtering() {
    let message_record = java_message_record("ipv4-v1");
    let mut header = HeaderMap::new();
    java_query_message_response_header(100, 1).encode_fields(&mut header);
    let (address, peer) =
        scripted_peer(response::SUCCESS, Some(header), Some(message_record)).await;
    let route = TopicRouteData::java_decode(
        serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [{"cluster": "cluster-a", "brokerName": "broker-a", "brokerAddrs": {"0": address}}],
            "filterServerTable": {}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let query = AdminQueryRequest::by_unique_key("orders", "not-the-message-id", 32, 10, 99);
    let (api, client) = api().await;

    let error = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::QUERY_MESSAGE,
            code: response::NO_MESSAGE,
            remark: Some(remark),
            ..
        } if remark == "query message by key finished, but no message."
    ));
    assert_eq!(peer.await.unwrap().code, request::QUERY_MESSAGE);
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_matches_java_null_index_type_filter() {
    let message_record = java_message_record("ipv4-v1");
    let mut header = HeaderMap::new();
    java_query_message_response_header(100, 1).encode_fields(&mut header);
    let (address, peer) =
        scripted_peer(response::SUCCESS, Some(header), Some(message_record)).await;
    let route = TopicRouteData::java_decode(
        serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [{"cluster": "cluster-a", "brokerName": "broker-a", "brokerAddrs": {"0": address}}],
            "filterServerTable": {}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let query = AdminQueryRequest::by_key("orders", "order-key", 32, 10, 99)
        .with_index_type(None)
        .with_last_key(None);
    let (api, client) = api().await;

    let error = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::QUERY_MESSAGE,
            code: response::NO_MESSAGE,
            remark: Some(remark),
            ..
        } if remark == "query message by key finished, but no message."
    ));
    let command = peer.await.unwrap();
    let fields = command.ext_fields.unwrap();
    assert!(!fields.contains_key("indexType"));
    assert!(!fields.contains_key("lastKey"));
    assert_eq!(fields.get("_UNIQUE_KEY_QUERY"), Some(&"false".to_owned()));
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_returns_java_no_message_error_for_empty_routes() {
    let route = TopicRouteData::java_decode(
        br#"{"queueDatas":[],"brokerDatas":[],"filterServerTable":{}}"#,
    )
    .unwrap();
    let query = AdminQueryRequest::by_key("orders", "order-key", 32, 10, 99);
    let (api, client) = api().await;

    let error = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::QUERY_MESSAGE,
            code: response::NO_MESSAGE,
            remark: Some(remark),
            ..
        } if remark == "query message by key finished, but no message."
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_applies_java_cluster_filter_and_last_key() {
    let message_record = java_message_record("ipv4-v1");
    let mut header = HeaderMap::new();
    java_query_message_response_header(100, 1).encode_fields(&mut header);
    let (selected_address, selected_peer) =
        scripted_peer(response::SUCCESS, Some(header), Some(message_record)).await;
    let (filtered_address, mut filtered_peer) = scripted_peer(response::SUCCESS, None, None).await;
    let route = TopicRouteData::java_decode(
        serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [
                {"cluster": "cluster-a", "brokerName": "broker-a", "brokerAddrs": {"0": selected_address}},
                {"cluster": "cluster-b", "brokerName": "broker-b", "brokerAddrs": {"0": filtered_address}}
            ],
            "filterServerTable": {}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let query = AdminQueryRequest::by_unique_key("orders", "producer-unique-1", 32, 10, 99)
        .in_cluster("cluster-a")
        .with_last_key(Some("page-token".into()));
    let (api, client) = api().await;

    let result = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(result.get_message_list().unwrap().snapshot().len(), 1);
    let command = selected_peer.await.unwrap();
    assert_eq!(command.code, request::QUERY_MESSAGE);
    let fields = command.ext_fields.unwrap();
    assert_eq!(fields.get("lastKey"), Some(&"page-token".to_owned()));
    assert_eq!(fields.get("_UNIQUE_KEY_QUERY"), Some(&"true".to_owned()));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut filtered_peer)
            .await
            .is_err()
    );
    filtered_peer.abort();
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_uses_cluster_for_logical_topic_route_and_request() {
    let message_record = java_message_record("ipv4-v1");
    let mut header = HeaderMap::new();
    java_query_message_response_header(100, 1).encode_fields(&mut header);
    let (address, peer) =
        scripted_peer(response::SUCCESS, Some(header), Some(message_record)).await;
    let route = TopicRouteData::java_decode(
        serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [{"cluster": "other-cluster", "brokerName": "broker-a", "brokerAddrs": {"0": address}}],
            "filterServerTable": {}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let query = AdminQueryRequest::by_unique_key("%LMQ%child", "producer-unique-1", 32, 10, 99)
        .with_cluster_name(Some("parent".into()));
    let (api, client) = api().await;

    let result = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(result.get_message_list().unwrap().snapshot().len(), 1);
    let command = peer.await.unwrap();
    let fields = command.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"parent".to_owned()));
    assert_eq!(fields.get("_UNIQUE_KEY_QUERY"), Some(&"true".to_owned()));
    client.shutdown().await;
}

#[tokio::test]
async fn view_message_sends_physical_offset_and_decodes_one_java_message() {
    let message_record = java_message_record("ipv4-v1");
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(message_record)).await;
    let (api, client) = api().await;
    let header = {
        let mut __phase18_header_13 = ViewMessageRequestHeader::default();
        __phase18_header_13.set_topic(Some("orders".into()));
        __phase18_header_13.set_offset(Some(1_700_000_000_041));
        __phase18_header_13
    };

    let message = api
        .view_message(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::VIEW_MESSAGE_BY_ID);
    let mut expected_fields = HeaderMap::new();
    header.encode_fields(&mut expected_fields);
    assert_eq!(command.ext_fields, Some(expected_fields.into()));
    assert_eq!(message.topic(), "orders");
    assert_eq!(message.body().as_ref(), b"hello-v1");
    assert_eq!(
        message.message_id(),
        message.properties().get("UNIQ_KEY").map(String::as_str)
    );
    assert!(message.offset_message_id().is_some());
    client.shutdown().await;
}

#[tokio::test]
async fn view_message_rejects_malformed_success_body() {
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(vec![0, 0, 0, 1])).await;
    let (api, client) = api().await;
    let error = api
        .view_message(
            &addr,
            &ViewMessageRequestHeader::default(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    peer.await.unwrap();
    assert!(matches!(
        error,
        RpcError::MalformedResponse {
            request_code: request::VIEW_MESSAGE_BY_ID,
            ..
        }
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn view_message_preserves_not_found_response() {
    let (addr, peer) = scripted_peer(response::NO_MESSAGE, None, None).await;
    let (api, client) = api().await;
    let error = api
        .view_message(
            &addr,
            &ViewMessageRequestHeader::default(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::VIEW_MESSAGE_BY_ID,
            code: response::NO_MESSAGE,
            ..
        }
    ));
    assert_eq!(peer.await.unwrap().code, request::VIEW_MESSAGE_BY_ID);
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_surface_matches_pinned_java_method_inventory() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let methods = fixture["interfacePublicMethods"].as_array().unwrap();
    assert_eq!(methods.len(), 19);
    let java_names = methods
        .iter()
        .map(|method| {
            method
                .as_str()
                .unwrap()
                .split('(')
                .next()
                .unwrap()
                .rsplit('.')
                .next()
                .unwrap()
        })
        .collect::<HashSet<_>>();
    let expected = HashSet::from([
        "queryMessage",
        "getTopicStatsInfo",
        "queryConsumeTimeSpan",
        "updateOrCreateTopic",
        "updateOrCreateSubscriptionGroup",
        "deleteTopicInBroker",
        "deleteTopicInNameserver",
        "deleteKvConfig",
        "deleteSubscriptionGroup",
        "invokeBrokerToResetOffset",
        "viewMessage",
        "getBrokerClusterInfo",
        "getConsumerConnectionList",
        "queryTopicsByConsumer",
        "querySubscriptionByConsumer",
        "getConsumeStats",
        "queryTopicConsumeByWho",
        "getConsumerRunningInfo",
        "consumeMessageDirectly",
    ]);
    assert_eq!(java_names, expected);

    // Compile-check every Rust owner method. The futures are dropped without
    // polling, so this surface inventory test does not issue Broker requests.
    let (api, client) = api().await;
    let admin = api.mq_client_admin();
    let address = "127.0.0.1:10911";
    let deadline = Instant::now() + Duration::from_secs(2);
    let query_message = QueryMessageRequestHeader::default();
    let topic_stats = GetTopicStatsInfoRequestHeader::default();
    let consume_time_span = QueryConsumeTimeSpanRequestHeader::default();
    let create_topic = CreateTopicRequestHeader::default();
    let create_subscription_group = SubscriptionGroupConfig::default();
    let delete_topic = DeleteTopicRequestHeader::default();
    let delete_topic_nameserver = DeleteTopicFromNamesrvRequestHeader::default();
    let delete_kv = DeleteKvConfigRequestHeader::default();
    let delete_subscription_group = DeleteSubscriptionGroupRequestHeader::default();
    let reset_offset = ResetOffsetRequestHeader::default();
    let view_message = ViewMessageRequestHeader::default();
    let consumer_connections = GetConsumerConnectionListRequestHeader::default();
    let topics_by_consumer = QueryTopicsByConsumerRequestHeader::default();
    let subscription_by_consumer = QuerySubscriptionByConsumerRequestHeader::default();
    let consume_stats = GetConsumeStatsRequestHeader::default();
    let topic_consume_by_who = QueryTopicConsumeByWhoRequestHeader::default();
    let consumer_running_info = GetConsumerRunningInfoRequestHeader::default();
    let consume_directly = ConsumeMessageDirectlyResultRequestHeader::default();

    drop(admin.query_message(address, false, false, &query_message, deadline));
    drop(admin.get_topic_stats_info(address, &topic_stats, deadline));
    drop(admin.query_consume_time_span(address, &consume_time_span, deadline));
    drop(admin.update_or_create_topic(address, &create_topic, deadline));
    drop(admin.update_or_create_subscription_group(address, &create_subscription_group, deadline));
    drop(admin.delete_topic_in_broker(address, &delete_topic, deadline));
    drop(admin.delete_topic_in_nameserver(address, &delete_topic_nameserver, deadline));
    drop(admin.delete_kv_config(address, &delete_kv, deadline));
    drop(admin.delete_subscription_group(address, &delete_subscription_group, deadline));
    drop(admin.invoke_broker_to_reset_offset(address, &reset_offset, deadline));
    drop(admin.view_message(address, &view_message, deadline));
    drop(admin.get_broker_cluster_info(address, deadline));
    drop(admin.get_consumer_connection_list(address, &consumer_connections, deadline));
    drop(admin.query_topics_by_consumer(address, &topics_by_consumer, deadline));
    drop(admin.query_subscription_by_consumer(address, &subscription_by_consumer, deadline));
    drop(admin.get_consume_stats(address, &consume_stats, deadline));
    drop(admin.query_topic_consume_by_who(address, &topic_consume_by_who, deadline));
    drop(admin.get_consumer_running_info(address, &consumer_running_info, deadline));
    drop(admin.consume_message_directly(address, &consume_directly, deadline));
    client.shutdown().await;
}
