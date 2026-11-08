use super::*;

#[test]
fn response_header_identity_matches_invoked_java() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/protocol/header-response-identity.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["clientArtifact"],
        "org.apache.rocketmq:rocketmq-client:5.5.1"
    );
    assert_eq!(
        fixture["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );
    assert_eq!(fixture["serverContacted"], false);
    assert_eq!(
        fixture["recallMessageIdNullability"]["defaultMsgIdIsNull"],
        true
    );
    assert_eq!(
        fixture["recallMessageIdNullability"]["msgIdAfterNullSetterIsNull"],
        true
    );
    assert_eq!(
        fixture["recallMessageIdNullability"]["nullExtFieldsDecodeReturnsNullMsgId"],
        true
    );
    assert_eq!(
        fixture["recallMessageIdNullability"]["emptyExtFieldsMissingMsgIdRejected"],
        false
    );
    let cases = fixture["identityCases"].as_object().unwrap();
    assert_eq!(cases.len(), 22);
    let api_inventories = fixture["apiInventories"].as_object().unwrap();
    assert_eq!(api_inventories.len(), 22);
    for (rust_name, inventory) in api_inventories {
        assert_eq!(
            inventory["publicFields"],
            serde_json::json!([]),
            "{rust_name}"
        );
    }
    let recall_methods = api_inventories["RecallMessageResponseHeader"]["callablePublicMethods"]
        .as_array()
        .unwrap();
    assert!(recall_methods.iter().any(|method| {
        method
            .as_str()
            .unwrap()
            .ends_with("RecallMessageResponseHeader.getMsgId()")
    }));
    assert!(recall_methods.iter().any(|method| {
        method
            .as_str()
            .unwrap()
            .ends_with("RecallMessageResponseHeader.setMsgId(java.lang.String)")
    }));
    for (rust_name, identity) in cases {
        let expected_java_class = match rust_name.as_str() {
            "AddWritePermOfBrokerResponseHeader" => {
                format!("org.apache.rocketmq.remoting.protocol.header.namesrv.{rust_name}")
            }
            "GetKvConfigResponseHeader" => {
                "org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigResponseHeader"
                    .to_owned()
            }
            "WipeWritePermOfBrokerResponseHeader" => {
                format!("org.apache.rocketmq.remoting.protocol.header.namesrv.{rust_name}")
            }
            _ => format!("org.apache.rocketmq.remoting.protocol.header.{rust_name}"),
        };
        assert_eq!(identity["javaClass"], expected_java_class);
        assert_eq!(
            identity["equalsDeclaredBy"], "java.lang.Object",
            "{rust_name}"
        );
        assert_eq!(
            identity["hashCodeDeclaredBy"], "java.lang.Object",
            "{rust_name}"
        );
        assert_eq!(identity["sameObjectEqualsItself"], true, "{rust_name}");
        assert_eq!(
            identity["distinctDefaultObjectsEquals"], false,
            "{rust_name}"
        );
    }
}

fn request_header_hash<T: std::hash::Hash>(value: &T) -> u64 {
    use std::hash::Hasher;

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

macro_rules! check_rpc_request_header {
    ($header:ty, $broker_field:ident, $prepare:expr, $payload_change:expr, $decode_fast:expr) => {{
        let mut header = <$header>::default();
        header.namespace = Some("namespace".into());
        header.namespaced = Some(true);
        header.$broker_field = Some("broker".into());
        header.oneway = Some(false);
        ($prepare)(&mut header);

        let mut payload_change = header.clone();
        ($payload_change)(&mut payload_change);
        assert_eq!(
            header,
            payload_change,
            "{} payload equality",
            stringify!($header)
        );
        assert_eq!(
            request_header_hash(&header),
            request_header_hash(&payload_change),
            "{} payload hash",
            stringify!($header)
        );

        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        assert_eq!(fields.get("ns").map(String::as_str), Some("namespace"));
        assert_eq!(fields.get("nsd").map(String::as_str), Some("true"));
        assert_eq!(fields.get("bname").map(String::as_str), Some("broker"));
        assert_eq!(fields.get("oway").map(String::as_str), Some("false"));
        let decoded = <$header>::decode_fields_with_mode(&fields, $decode_fast).unwrap();
        assert_eq!(
            header,
            decoded,
            "{} base-field round trip",
            stringify!($header)
        );

        let mut changed_namespace = header.clone();
        changed_namespace.namespace = Some("other-namespace".into());
        assert_ne!(header, changed_namespace, "{} ns", stringify!($header));
        let mut changed_namespaced = header.clone();
        changed_namespaced.namespaced = Some(false);
        assert_ne!(header, changed_namespaced, "{} nsd", stringify!($header));
        let mut changed_broker = header.clone();
        changed_broker.$broker_field = Some("other-broker".into());
        assert_ne!(header, changed_broker, "{} bname", stringify!($header));
        let mut changed_oneway = header.clone();
        changed_oneway.oneway = Some(true);
        assert_ne!(header, changed_oneway, "{} oway", stringify!($header));
    }};
    ($header:ty, $broker_field:ident, $prepare:expr, $payload_change:expr) => {
        check_rpc_request_header!($header, $broker_field, $prepare, $payload_change, true);
    };
}

#[test]
fn request_header_equality_and_hash_match_rpc_base_fields() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/protocol/request-header-identity.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["clientArtifact"],
        "org.apache.rocketmq:rocketmq-client:5.5.1"
    );
    assert_eq!(
        fixture["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );
    assert_eq!(fixture["serverContacted"], false);
    let cases = fixture["identityCases"].as_object().unwrap();
    assert_eq!(cases.len(), 50);
    let rpc_semantics_types: std::collections::BTreeSet<_> = include_str!("requests.rs")
        .split_once("impl_rpc_request_header_semantics!(")
        .unwrap()
        .1
        .split_once(");")
        .unwrap()
        .0
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect();
    let fixture_rpc_types: std::collections::BTreeSet<_> = cases
        .keys()
        .filter(|name| name.as_str() != "ConsumeMessageDirectlyResultRequestHeader")
        .cloned()
        .collect();
    assert_eq!(
        fixture_rpc_types, rpc_semantics_types,
        "Java identity/API inventory must cover every Rust RpcRequestHeader equality mapping"
    );
    for (rust_name, identity) in cases {
        let java_class = if rust_name == "GetRouteInfoRequestHeader" {
            "org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader"
                .to_owned()
        } else if rust_name == "DeleteTopicFromNamesrvRequestHeader" {
            "org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader"
                .to_owned()
        } else if rust_name == "SendMessageRequestHeaderV1" {
            "org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader".to_owned()
        } else if rust_name == "QueryCorrectionOffsetRequestHeader" {
            "org.apache.rocketmq.remoting.protocol.header.QueryCorrectionOffsetHeader".to_owned()
        } else {
            format!("org.apache.rocketmq.remoting.protocol.header.{rust_name}")
        };
        assert_eq!(identity["javaClass"], java_class, "{rust_name}");
        assert_eq!(
            identity["equalsDeclaredBy"], "org.apache.rocketmq.remoting.rpc.RpcRequestHeader",
            "{rust_name}"
        );
        assert_eq!(
            identity["hashCodeDeclaredBy"], "org.apache.rocketmq.remoting.rpc.RpcRequestHeader",
            "{rust_name}"
        );
        assert_eq!(identity["sameObjectEqualsItself"], true, "{rust_name}");
        assert_eq!(
            identity["distinctDefaultObjectsEquals"], true,
            "{rust_name}"
        );
        assert_eq!(identity["equalBaseFieldsEquals"], true, "{rust_name}");
        assert_eq!(identity["equalBaseFieldsHashEqual"], true, "{rust_name}");
        assert_eq!(identity["payloadDifferenceIgnored"], true, "{rust_name}");
        assert_eq!(
            identity["payloadDifferenceTested"],
            !matches!(
                rust_name.as_str(),
                "LockBatchMqRequestHeader"
                    | "LiteSubscriptionCtlRequestHeader"
                    | "UnlockBatchMqRequestHeader"
                    | "HeartbeatRequestHeader"
            ),
            "{rust_name}"
        );
        for field in ["ns", "nsd", "bname", "oway"] {
            assert_eq!(
                identity["baseFieldDifferenceEquals"][field], false,
                "{rust_name}.{field}"
            );
        }
    }

    check_rpc_request_header!(
        QueryConsumerOffsetRequestHeader,
        broker_name,
        |_| {},
        |h: &mut QueryConsumerOffsetRequestHeader| { h.consumer_group = Some("payload".into()) }
    );
    check_rpc_request_header!(
        HeartbeatRequestHeader,
        broker_name,
        |_| {},
        |_: &mut HeartbeatRequestHeader| {}
    );
    check_rpc_request_header!(
        LiteSubscriptionCtlRequestHeader,
        broker_name,
        |_| {},
        |_: &mut LiteSubscriptionCtlRequestHeader| {}
    );
    check_rpc_request_header!(
        NotifyUnsubscribeLiteRequestHeader,
        broker_name,
        |_| {},
        |header: &mut NotifyUnsubscribeLiteRequestHeader| { header.lite_topic = "payload".into() }
    );
    check_rpc_request_header!(
        CloneGroupOffsetRequestHeader,
        broker_name,
        |_| {},
        |header: &mut CloneGroupOffsetRequestHeader| { header.src_group = Some("payload".into()) }
    );
    check_rpc_request_header!(
        GetSubscriptionGroupConfigRequestHeader,
        broker_name,
        |_| {},
        |header: &mut GetSubscriptionGroupConfigRequestHeader| {
            header.group = Some("payload".into())
        }
    );
    check_rpc_request_header!(
        GetTopicConfigRequestHeader,
        broker_name,
        |_| {},
        |header: &mut GetTopicConfigRequestHeader| header.topic = Some("payload".into())
    );
    check_rpc_request_header!(
        QueryConsumeQueueRequestHeader,
        broker_name,
        |_| {},
        |header: &mut QueryConsumeQueueRequestHeader| { header.topic = Some("payload".into()) }
    );
    check_rpc_request_header!(
        QueryCorrectionOffsetRequestHeader,
        broker_name,
        |_| {},
        |header: &mut QueryCorrectionOffsetRequestHeader| { header.topic = Some("payload".into()) }
    );
    check_rpc_request_header!(
        UpdateGroupForbiddenRequestHeader,
        broker_name,
        |_| {},
        |header: &mut UpdateGroupForbiddenRequestHeader| { header.group = Some("payload".into()) }
    );
    check_rpc_request_header!(
        UpdateConsumerOffsetRequestHeader,
        broker_name,
        |_| {},
        |h: &mut UpdateConsumerOffsetRequestHeader| h.commit_offset = Some(17)
    );
    check_rpc_request_header!(
        QueryConsumeTimeSpanRequestHeader,
        broker_name,
        |_| {},
        |h: &mut QueryConsumeTimeSpanRequestHeader| h.topic = Some("payload".into())
    );
    check_rpc_request_header!(
        SearchOffsetRequestHeader,
        broker_name,
        |_| {},
        |h: &mut SearchOffsetRequestHeader| h.timestamp = Some(17)
    );
    check_rpc_request_header!(
        GetMaxOffsetRequestHeader,
        broker_name,
        |_| {},
        |h: &mut GetMaxOffsetRequestHeader| h.committed = false
    );
    check_rpc_request_header!(
        GetMinOffsetRequestHeader,
        broker_name,
        |_| {},
        |h: &mut GetMinOffsetRequestHeader| h.queue_id = Some(17)
    );
    check_rpc_request_header!(
        GetEarliestMsgStoretimeRequestHeader,
        broker_name,
        |_| {},
        |h: &mut GetEarliestMsgStoretimeRequestHeader| h.queue_id = Some(17)
    );
    check_rpc_request_header!(
        PullMessageRequestHeader,
        broker_name,
        |_| {},
        |h: &mut PullMessageRequestHeader| h.consumer_group = Some("payload".into())
    );
    check_rpc_request_header!(
        UnregisterClientRequestHeader,
        broker_name,
        |_| {},
        |h: &mut UnregisterClientRequestHeader| h.client_id = "payload".into()
    );
    check_rpc_request_header!(
        GetRouteInfoRequestHeader,
        broker_name,
        |_| {},
        |h: &mut GetRouteInfoRequestHeader| h.topic = "payload".into()
    );
    check_rpc_request_header!(
        CreateTopicRequestHeader,
        broker_name,
        |h: &mut CreateTopicRequestHeader| h.topic_filter_type = Some("SINGLE_TAG".into()),
        |h: &mut CreateTopicRequestHeader| h.topic = Some("payload".into())
    );
    check_rpc_request_header!(
        DeleteTopicRequestHeader,
        broker_name,
        |_| {},
        |h: &mut DeleteTopicRequestHeader| h.topic = Some("payload".into())
    );
    check_rpc_request_header!(
        DeleteTopicFromNamesrvRequestHeader,
        broker_name,
        |_| {},
        |h: &mut DeleteTopicFromNamesrvRequestHeader| h.topic = Some("payload".into())
    );
    check_rpc_request_header!(
        DeleteSubscriptionGroupRequestHeader,
        broker_name,
        |_| {},
        |h: &mut DeleteSubscriptionGroupRequestHeader| h.group_name = Some("payload".into())
    );
    check_rpc_request_header!(
        QueryMessageRequestHeader,
        broker_name,
        |_| {},
        |h: &mut QueryMessageRequestHeader| h.key = Some("payload".into())
    );
    check_rpc_request_header!(
        QueryTopicsByConsumerRequestHeader,
        broker_name,
        |_| {},
        |h: &mut QueryTopicsByConsumerRequestHeader| h.group = Some("payload".into())
    );
    check_rpc_request_header!(
        QuerySubscriptionByConsumerRequestHeader,
        broker_name,
        |_| {},
        |h: &mut QuerySubscriptionByConsumerRequestHeader| h.topic = Some("payload".into())
    );
    check_rpc_request_header!(
        QueryTopicConsumeByWhoRequestHeader,
        broker_name,
        |_| {},
        |h: &mut QueryTopicConsumeByWhoRequestHeader| h.topic = Some("payload".into())
    );
    check_rpc_request_header!(
        GetTopicStatsInfoRequestHeader,
        broker_name,
        |_| {},
        |h: &mut GetTopicStatsInfoRequestHeader| h.topic = Some("payload".into())
    );
    check_rpc_request_header!(
        GetConsumerListByGroupRequestHeader,
        broker_name,
        |_| {},
        |h: &mut GetConsumerListByGroupRequestHeader| h.consumer_group = "payload".into()
    );
    check_rpc_request_header!(
        LockBatchMqRequestHeader,
        broker_name,
        |_| {},
        |_: &mut LockBatchMqRequestHeader| {}
    );
    check_rpc_request_header!(
        UnlockBatchMqRequestHeader,
        broker_name,
        |_| {},
        |_: &mut UnlockBatchMqRequestHeader| {}
    );
    check_rpc_request_header!(
        ConsumerSendMsgBackRequestHeader,
        broker_name,
        |_| {},
        |h: &mut ConsumerSendMsgBackRequestHeader| h.group = "payload".into()
    );
    check_rpc_request_header!(
        RecallMessageRequestHeader,
        broker_name,
        |_| {},
        |h: &mut RecallMessageRequestHeader| h.recall_handle = "payload".into()
    );
    check_rpc_request_header!(
        EndTransactionRequestHeader,
        broker_name,
        |h: &mut EndTransactionRequestHeader| h.commit_or_rollback = Some(8),
        |h: &mut EndTransactionRequestHeader| h.transaction_id = Some("payload".into())
    );
    check_rpc_request_header!(
        CheckTransactionStateRequestHeader,
        broker_name,
        |_| {},
        |h: &mut CheckTransactionStateRequestHeader| h.transaction_id = Some("payload".into())
    );
    check_rpc_request_header!(
        ReplyMessageRequestHeader,
        broker_name,
        |_| {},
        |h: &mut ReplyMessageRequestHeader| h.producer_group = "payload".into()
    );
    check_rpc_request_header!(
        NotificationRequestHeader,
        broker_name,
        |_| {},
        |h: &mut NotificationRequestHeader| h.attempt_id = Some("payload".into())
    );
    check_rpc_request_header!(
        ResetOffsetRequestHeader,
        broker_name,
        |_| {},
        |h: &mut ResetOffsetRequestHeader| h.group = "payload".into()
    );
    check_rpc_request_header!(
        GetConsumerStatusRequestHeader,
        broker_name,
        |_| {},
        |h: &mut GetConsumerStatusRequestHeader| h.topic = "payload".into()
    );
    check_rpc_request_header!(
        GetConsumerConnectionListRequestHeader,
        broker_name,
        |_| {},
        |h: &mut GetConsumerConnectionListRequestHeader| {
            h.consumer_group = Some("payload".into())
        }
    );
    check_rpc_request_header!(
        GetConsumeStatsRequestHeader,
        broker_name,
        |_| {},
        |h: &mut GetConsumeStatsRequestHeader| { h.consumer_group = Some("payload".into()) }
    );
    check_rpc_request_header!(
        GetConsumerRunningInfoRequestHeader,
        broker_name,
        |_| {},
        |h: &mut GetConsumerRunningInfoRequestHeader| h.consumer_group = "payload".into()
    );
    check_rpc_request_header!(
        ConsumeMessageDirectlyResultRequestHeader,
        rpc_broker_name,
        |_| {},
        |h: &mut ConsumeMessageDirectlyResultRequestHeader| h.broker_name = Some("payload".into())
    );
    check_rpc_request_header!(
        PopMessageRequestHeader,
        broker_name,
        |_| {},
        |h: &mut PopMessageRequestHeader| h.consumer_group = Some("payload".into())
    );
    check_rpc_request_header!(
        PopLiteMessageRequestHeader,
        broker_name,
        |_| {},
        |h: &mut PopLiteMessageRequestHeader| h.client_id = Some("payload".into())
    );
    check_rpc_request_header!(
        PopLiteMessageRequestHeader,
        broker_name,
        |_| {},
        |h: &mut PopLiteMessageRequestHeader| h.client_id = Some("payload".into())
    );
    check_rpc_request_header!(
        AckMessageRequestHeader,
        broker_name,
        |_| {},
        |h: &mut AckMessageRequestHeader| h.consumer_group = "payload".into()
    );
    check_rpc_request_header!(
        ChangeInvisibleTimeRequestHeader,
        broker_name,
        |_| {},
        |h: &mut ChangeInvisibleTimeRequestHeader| h.consumer_group = Some("payload".into())
    );
    check_rpc_request_header!(
        PollingInfoRequestHeader,
        broker_name,
        |_| {},
        |h: &mut PollingInfoRequestHeader| h.consumer_group = "payload".into()
    );
    check_rpc_request_header!(
        SendMessageRequestHeaderV1,
        broker_name,
        |_| {},
        |h: &mut SendMessageRequestHeaderV1| h.topic = "payload".into()
    );
    check_rpc_request_header!(
        SendMessageRequestHeaderV2,
        broker_name,
        |_| {},
        |h: &mut SendMessageRequestHeaderV2| h.b = Some("payload".into()),
        false
    );
}

#[test]
fn pop_lite_timeout_method_matches_java_arithmetic_and_threshold() {
    let header = PopLiteMessageRequestHeader {
        born_time: 100,
        poll_time: 200,
        ..PopLiteMessageRequestHeader::default()
    };
    assert!(!header.is_timeout_too_much_at(800));
    assert!(header.is_timeout_too_much_at(801));

    let wrapped = PopLiteMessageRequestHeader {
        born_time: 1,
        ..PopLiteMessageRequestHeader::default()
    };
    assert!(wrapped.is_timeout_too_much_at(i64::MIN));

    let regular = PopMessageRequestHeader {
        born_time: 100,
        poll_time: 200,
        ..PopMessageRequestHeader::default()
    };
    assert!(!regular.is_timeout_too_much_at(800));
    assert!(regular.is_timeout_too_much_at(801));
}

#[test]
fn query_offset_preserves_unknown_fields_and_optional_false() {
    let raw = HeaderMap::from([
        ("consumerGroup".into(), "workers".into()),
        ("topic".into(), "orders".into()),
        ("queueId".into(), "0".into()),
        ("setZeroIfNotFound".into(), "false".into()),
        ("futureField".into(), "keep".into()),
    ]);
    let mut updated = raw.clone();
    let mut typed = QueryConsumerOffsetRequestHeader::decode_fields(&raw).unwrap();
    typed.set_zero_if_not_found = Some(true);
    typed.encode_fields(&mut updated);

    assert_eq!(updated["setZeroIfNotFound"], "true");
    assert_eq!(updated["futureField"], "keep");
    assert_eq!(raw["setZeroIfNotFound"], "false");
}

#[test]
fn send_v1_and_v2_round_trip_compact_fields() {
    let v1 = SendMessageRequestHeaderV1 {
        producer_group: Some("producer".into()),
        topic: "orders".into(),
        default_topic: "TBW102".into(),
        default_topic_queue_nums: 4,
        queue_id: 2,
        sys_flag: 1,
        born_timestamp: i64::MAX,
        flag: 7,
        properties: Some("TAGS\u{1}blue".into()),
        reconsume_times: Some(0),
        unit_mode: Some(true),
        max_reconsume_times: Some(16),
        batch: Some(false),
        ..SendMessageRequestHeaderV1::default()
    };
    let v2 = SendMessageRequestHeaderV2::from_v1(&v1);
    let mut v2_fields = HeaderMap::new();
    v2.encode_fields(&mut v2_fields);
    let decoded = SendMessageRequestHeaderV2::decode_fields(&v2_fields).unwrap();
    assert_eq!(decoded, v2);
    assert_eq!(v2_fields["a"], "producer");
    assert!(!v2_fields.contains_key("n"));
}

#[test]
fn nullable_producer_group_is_omitted_from_send_headers() {
    let header = SendMessageRequestHeaderV1 {
        producer_group: None,
        broker_name: Some("short-broker".into()),
        namespace: Some("tenant".into()),
        namespaced: Some(true),
        logical: Some(true),
        oneway: Some(false),
        ..SendMessageRequestHeaderV1::default()
    };
    let mut fields = HeaderMap::new();
    header.encode_fields(&mut fields);
    assert!(!fields.contains_key("producerGroup"));
    assert_eq!(
        SendMessageRequestHeaderV1::decode_fields(&fields),
        Ok(header.clone())
    );

    let compact = SendMessageRequestHeaderV2::from_v1(&header);
    assert_eq!(compact.n.as_deref(), Some("short-broker"));
    assert_eq!(compact.namespace, None);
    assert_eq!(compact.namespaced, None);
    assert_eq!(compact.logical, None);
    assert_eq!(compact.broker_name, None);
    assert_eq!(compact.oneway, None);
    let mut compact_fields = HeaderMap::new();
    compact.encode_fields(&mut compact_fields);
    assert!(!compact_fields.contains_key("a"));
    assert_eq!(
        compact_fields.get("n").map(String::as_str),
        Some("short-broker")
    );
    assert!(!compact_fields.contains_key("bname"));
    assert!(!compact_fields.contains_key("ns"));
    assert!(!compact_fields.contains_key("nsd"));
    assert!(!compact_fields.contains_key("lo"));
    assert!(!compact_fields.contains_key("oway"));
    assert_eq!(
        SendMessageRequestHeaderV2::decode_fields(&compact_fields).unwrap(),
        compact
    );
}

#[test]
fn pull_headers_round_trip_required_and_optional_fields() {
    let header = PullMessageRequestHeader {
        consumer_group: Some("workers".into()),
        topic: Some("orders".into()),
        queue_id: Some(-1),
        queue_offset: Some(i64::MIN),
        max_msg_nums: Some(32),
        sys_flag: Some(0),
        commit_offset: Some(0),
        suspend_timeout_millis: Some(15_000),
        sub_version: Some(i64::MAX),
        proxy_forward_client_id: Some("proxy".into()),
        broker_name: Some("broker-a".into()),
        ..PullMessageRequestHeader::default()
    };
    let mut fields = HeaderMap::new();
    header.encode_fields(&mut fields);
    assert_eq!(PullMessageRequestHeader::decode_fields(&fields), Ok(header));
    assert!(!fields.contains_key("subscription"));
    assert_eq!(fields["proxyFrowardClientId"], "proxy");
    assert_eq!(fields["bname"], "broker-a");
}

#[test]
fn pull_request_header_round_trips_java_lite_topic_field() {
    let mut fields = HeaderMap::new();
    fields.insert("consumerGroup".into(), "workers".into());
    fields.insert("topic".into(), "orders".into());
    fields.insert("queueId".into(), "1".into());
    fields.insert("queueOffset".into(), "9".into());
    fields.insert("maxMsgNums".into(), "32".into());
    fields.insert("sysFlag".into(), "0".into());
    fields.insert("commitOffset".into(), "8".into());
    fields.insert("suspendTimeoutMillis".into(), "15000".into());
    fields.insert("subVersion".into(), "1".into());
    fields.insert("liteTopic".into(), "lite-orders".into());

    let decoded = PullMessageRequestHeader::decode_fields(&fields).unwrap();
    assert_eq!(decoded.get_lite_topic(), Some("lite-orders"));
    let mut encoded = HeaderMap::new();
    decoded.encode_fields(&mut encoded);
    assert_eq!(encoded.get("liteTopic"), Some(&"lite-orders".to_owned()));
}

#[test]
fn missing_and_invalid_response_header_values_name_the_field() {
    assert!(matches!(
        QueryConsumerOffsetResponseHeader::decode_fields(&HeaderMap::new()),
        Err(ProtocolError::MissingField("offset"))
    ));
    let decoded = QueryConsumerOffsetResponseHeader::decode_fields(&HeaderMap::from([(
        "offset".into(),
        "12".into(),
    )]))
    .unwrap();
    assert_eq!(decoded.offset, Some(12));
    assert!(matches!(
        QueryConsumerOffsetResponseHeader::decode_fields(&HeaderMap::from([(
            "offset".into(),
            "9223372036854775808".into()
        )])),
        Err(ProtocolError::InvalidField {
            field: "offset",
            ..
        })
    ));
}

#[test]
fn recall_headers_match_java_field_names_and_nullable_response_msg_id() {
    let request = RecallMessageRequestHeader {
        producer_group: Some("recall-group".into()),
        topic: "orders".into(),
        recall_handle: "handle".into(),
        broker_name: Some("broker-a".into()),
        ..RecallMessageRequestHeader::default()
    };
    let mut fields = HeaderMap::new();
    request.encode_fields(&mut fields);
    assert_eq!(
        fields,
        HeaderMap::from([
            ("producerGroup".into(), "recall-group".into()),
            ("topic".into(), "orders".into()),
            ("recallHandle".into(), "handle".into()),
            ("bname".into(), "broker-a".into()),
        ])
    );
    assert_eq!(
        RecallMessageRequestHeader::decode_fields(&fields),
        Ok(request)
    );
    assert_eq!(
        RecallMessageResponseHeader::decode_fields(&HeaderMap::new())
            .unwrap()
            .get_msg_id(),
        None
    );
    let decoded = RecallMessageResponseHeader::decode_fields(&HeaderMap::from([(
        "msgId".into(),
        "recalled-id".into(),
    )]))
    .unwrap();
    assert_eq!(decoded.get_msg_id(), Some("recalled-id"));

    let mut response = RecallMessageResponseHeader::default();
    response.set_msg_id(Some("recalled-id".into()));
    assert_eq!(response.get_msg_id(), Some("recalled-id"));
    let mut encoded = HeaderMap::new();
    response.encode_fields(&mut encoded);
    assert_eq!(
        encoded.get("msgId").map(String::as_str),
        Some("recalled-id")
    );
    response.set_msg_id(None);
    assert_eq!(response.get_msg_id(), None);
}
