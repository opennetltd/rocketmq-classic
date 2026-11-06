use rocketmq_classic_common::message::{MessageQueue, TopicFilterType};
use rocketmq_classic_protocol::{SharedValue, heartbeat::SubscriptionData};
use rocketmq_classic_protocol::{
    body::{
        BatchAckMessageRequestBody, BitSet, ConsumeMessageDirectlyResult, ConsumeMessageResult,
        ConsumerConsumeStatus, ConsumerListByGroupResponseBody, ConsumerPopProcessQueueInfo,
        ConsumerProcessQueueInfo, ConsumerRunningInfoBody, GetConsumerStatusBody,
        LiteSubscriptionAction, LiteSubscriptionCtlRequestBody, LockBatchRequestBody,
        LockBatchResponseBody, MessageRequestMode, QueryAssignmentRequestBody, ResetOffsetBody,
        SetMessageRequestModeRequestBody, UnlockBatchRequestBody,
    },
    header::{
        AckMessageRequestHeader, AddBrokerRequestHeader, AddWritePermOfBrokerRequestHeader,
        AddWritePermOfBrokerResponseHeader, ChangeInvisibleTimeRequestHeader,
        ChangeInvisibleTimeResponseHeader, CheckRocksdbCqWriteProgressRequestHeader,
        CheckTransactionStateRequestHeader, CheckTransactionStateResponseHeader,
        CloneGroupOffsetRequestHeader, CommandHeader, ConsumeMessageDirectlyResultRequestHeader,
        ConsumerSendMsgBackRequestHeader, CreateAclRequestHeader, CreateTopicRequestHeader,
        CreateUserRequestHeader, DeleteAclRequestHeader, DeleteKvConfigRequestHeader,
        DeleteSubscriptionGroupRequestHeader, DeleteTopicFromNamesrvRequestHeader,
        DeleteTopicRequestHeader, DeleteUserRequestHeader, EndTransactionRequestHeader,
        EndTransactionResponseHeader, ExportRocksDBConfigToJsonRequestHeader,
        ExportRocksDBConfigType, FastCodesHeader, GetAclRequestHeader,
        GetAllSubscriptionGroupRequestHeader, GetAllSubscriptionGroupResponseHeader,
        GetAllTopicConfigRequestHeader, GetAllTopicConfigResponseHeader,
        GetConsumeStatsInBrokerRequestHeader, GetConsumeStatsRequestHeader,
        GetConsumerConnectionListRequestHeader, GetConsumerListByGroupRequestHeader,
        GetConsumerRunningInfoRequestHeader, GetConsumerStatusRequestHeader,
        GetEarliestMsgStoretimeRequestHeader, GetEarliestMsgStoretimeResponseHeader,
        GetKvConfigRequestHeader, GetKvConfigResponseHeader, GetKvListByNamespaceRequestHeader,
        GetLiteClientInfoRequestHeader, GetLiteGroupInfoRequestHeader,
        GetLiteTopicInfoRequestHeader, GetMaxOffsetRequestHeader, GetMaxOffsetResponseHeader,
        GetMinOffsetRequestHeader, GetMinOffsetResponseHeader, GetParentTopicInfoRequestHeader,
        GetRouteInfoRequestHeader, GetSubscriptionGroupConfigRequestHeader,
        GetTopicConfigRequestHeader, GetTopicStatsInfoRequestHeader,
        GetTopicsByClusterRequestHeader, GetUserRequestHeader, HeaderMap, HeartbeatRequestHeader,
        ListAclsRequestHeader, ListUsersRequestHeader, LiteSubscriptionCtlRequestHeader,
        LockBatchMqRequestHeader, NotificationRequestHeader, NotificationResponseHeader,
        NotifyUnsubscribeLiteRequestHeader, PollingInfoRequestHeader, PollingInfoResponseHeader,
        PopLiteMessageRequestHeader, PopLiteMessageResponseHeader, PopMessageRequestHeader,
        PopMessageResponseHeader, PullMessageRequestHeader, PullMessageResponseHeader,
        PutKvConfigRequestHeader, QueryConsumeQueueRequestHeader,
        QueryConsumeTimeSpanRequestHeader, QueryConsumerOffsetRequestHeader,
        QueryConsumerOffsetResponseHeader, QueryCorrectionOffsetRequestHeader,
        QueryMessageRequestHeader, QueryMessageResponseHeader,
        QuerySubscriptionByConsumerRequestHeader, QueryTopicConsumeByWhoRequestHeader,
        QueryTopicsByConsumerRequestHeader, RecallMessageRequestHeader,
        RecallMessageResponseHeader, RemoveBrokerRequestHeader, ReplyMessageRequestHeader,
        ResetMasterFlushOffsetHeader, ResetOffsetRequestHeader, SearchOffsetRequestHeader,
        SearchOffsetResponseHeader, SendMessageRequestHeaderV1, SendMessageRequestHeaderV2,
        SendMessageResponseHeader, UnlockBatchMqRequestHeader, UnregisterClientRequestHeader,
        UnregisterClientResponseHeader, UpdateAclRequestHeader, UpdateConsumerOffsetRequestHeader,
        UpdateGroupForbiddenRequestHeader, UpdateUserRequestHeader,
        ViewBrokerStatsDataRequestHeader, ViewMessageRequestHeader,
        WipeWritePermOfBrokerRequestHeader, WipeWritePermOfBrokerResponseHeader,
    },
};

mod support;
use support::{java_camel_to_snake, rust_header_method_exists};

fn json_header_map(name: &str) -> HeaderMap {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let frame = std::fs::read(format!("{root}/fixtures/remoting/{name}.bin")).unwrap();
    let marked = u32::from_be_bytes(frame[4..8].try_into().unwrap());
    let header_len = (marked & 0x00ff_ffff) as usize;
    let header: serde_json::Value = serde_json::from_slice(&frame[8..8 + header_len]).unwrap();
    serde_json::from_value(header["extFields"].clone()).unwrap()
}

fn java_ext_fields(stem: &str) -> HeaderMap {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let metadata: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(format!("{root}/fixtures/remoting/{stem}.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(metadata["java_client_version"], "5.5.1");
    serde_json::from_value(metadata["normalized_fields"]["ext_fields"].clone()).unwrap()
}

fn java_body(stem: &str) -> Vec<u8> {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let path = format!("{root}/fixtures/remoting/{stem}.bin");
    let frame = std::fs::read(path).unwrap();
    let marked = u32::from_be_bytes(frame[4..8].try_into().unwrap());
    let header_len = (marked & 0x00ff_ffff) as usize;
    let header_end = 8 + header_len;
    frame[header_end..].to_vec()
}

#[test]
fn java_request_and_response_header_fixtures_decode() {
    let route =
        GetRouteInfoRequestHeader::decode_fields(&java_ext_fields("header-route-request")).unwrap();
    assert_eq!(route.get_topic(), "orders");

    let create_topic =
        CreateTopicRequestHeader::decode_fields(&java_ext_fields("header-create-topic-request"))
            .unwrap();
    assert_eq!(create_topic.get_topic(), Some("orders"));
    assert_eq!(create_topic.get_default_topic(), Some("TBW102"));
    assert_eq!(create_topic.get_read_queue_nums(), Some(4));
    assert_eq!(create_topic.get_write_queue_nums(), Some(4));
    assert_eq!(create_topic.get_perm(), Some(6));
    assert_eq!(create_topic.get_topic_filter_type(), Some("SINGLE_TAG"));
    assert!(matches!(
        create_topic.get_topic_filter_type_enum(),
        Ok(TopicFilterType::SingleTag)
    ));
    assert_eq!(create_topic.get_topic_sys_flag(), Some(3));
    assert_eq!(create_topic.get_attributes(), Some("message.type=normal"));
    assert_eq!(create_topic.get_order(), Some(false));
    assert_eq!(create_topic.get_force(), Some(false));

    let query_message =
        QueryMessageRequestHeader::decode_fields(&java_ext_fields("header-query-message-request"))
            .unwrap();
    assert_eq!(query_message.get_topic(), Some("orders"));
    assert_eq!(query_message.get_key(), Some("order-42"));
    assert_eq!(query_message.get_max_num(), Some(32));
    assert_eq!(query_message.get_begin_timestamp(), Some(1_712_345_678_000));
    assert_eq!(query_message.get_end_timestamp(), Some(1_712_345_679_000));
    assert_eq!(query_message.get_index_type(), Some("UNIQUE_KEY"));
    assert_eq!(query_message.get_last_key(), Some("last-index-key"));

    let query_message_response = QueryMessageResponseHeader::decode_fields(&java_ext_fields(
        "header-query-message-response",
    ))
    .unwrap();
    assert_eq!(
        query_message_response.get_index_last_update_timestamp(),
        Some(1_712_345_679_000)
    );
    assert_eq!(
        query_message_response.get_index_last_update_phyoffset(),
        Some(9876)
    );

    let view_message =
        ViewMessageRequestHeader::decode_fields(&java_ext_fields("header-view-message-request"))
            .unwrap();
    assert_eq!(view_message.get_topic(), Some("orders"));
    assert_eq!(view_message.get_offset(), Some(9876));

    let lite_pop =
        PopLiteMessageResponseHeader::decode_fields(&java_ext_fields("header-pop-lite-response"))
            .unwrap();
    assert_eq!(lite_pop.get_pop_time(), 10);
    assert_eq!(lite_pop.get_invisible_time(), 20);
    assert_eq!(lite_pop.get_revive_qid(), 1);
    assert_eq!(lite_pop.get_start_offset_info(), Some("orders@0:100"));
    assert_eq!(lite_pop.get_msg_offset_info(), Some("orders@0:101"));
    assert_eq!(lite_pop.get_order_count_info(), Some("0"));

    let query = QueryConsumerOffsetRequestHeader::decode_fields(&java_ext_fields(
        "header-query-offset-request",
    ))
    .unwrap();
    assert_eq!(query.get_consumer_group(), Some("workers"));
    assert_eq!(query.get_queue_id(), Some(1));

    let query_time_span = QueryConsumeTimeSpanRequestHeader::decode_fields(&java_ext_fields(
        "header-query-consume-time-span-request",
    ))
    .unwrap();
    assert_eq!(query_time_span.get_topic(), Some("orders"));
    assert_eq!(query_time_span.get_group(), Some("workers"));

    let correction = QueryCorrectionOffsetRequestHeader::decode_fields(&java_ext_fields(
        "header-query-correction-offset-request",
    ))
    .unwrap();
    assert_eq!(correction.get_topic(), Some("orders"));
    assert_eq!(correction.get_compare_group(), Some("workers"));
    assert_eq!(correction.get_filter_groups(), Some("ignored-group"));
    assert_eq!(correction.get_namespace(), Some("tenant"));
    assert_eq!(correction.get_namespaced(), Some(true));
    assert_eq!(correction.get_broker_name(), Some("broker-a"));
    assert_eq!(correction.get_oneway(), Some(false));
    assert_eq!(correction.get_lo(), Some(true));

    let group_forbidden = UpdateGroupForbiddenRequestHeader::decode_fields(&java_ext_fields(
        "header-update-group-forbidden-request",
    ))
    .unwrap();
    assert_eq!(group_forbidden.get_topic(), Some("orders"));
    assert_eq!(group_forbidden.get_group(), Some("workers"));
    assert_eq!(group_forbidden.get_readable(), Some(true));
    assert_eq!(group_forbidden.get_namespace(), Some("tenant"));
    assert_eq!(group_forbidden.get_namespaced(), Some(true));
    assert_eq!(group_forbidden.get_broker_name(), Some("broker-a"));
    assert_eq!(group_forbidden.get_oneway(), Some(false));
    assert_eq!(group_forbidden.get_lo(), Some(true));

    let update = UpdateConsumerOffsetRequestHeader::decode_fields(&java_ext_fields(
        "header-update-offset-request",
    ))
    .unwrap();
    assert_eq!(update.get_commit_offset(), Some(9));

    let pull =
        PullMessageRequestHeader::decode_fields(&java_ext_fields("header-pull-request")).unwrap();
    assert_eq!(pull.get_queue_offset(), Some(9));
    assert_eq!(pull.get_sub_version(), Some(1_712_345_678_901));

    let pull_response =
        PullMessageResponseHeader::decode_fields(&java_ext_fields("header-pull-response")).unwrap();
    assert_eq!(pull_response.get_next_begin_offset(), Some(10));
    let offset_response = QueryConsumerOffsetResponseHeader::decode_fields(&java_ext_fields(
        "header-query-offset-response",
    ))
    .unwrap();
    assert_eq!(offset_response.get_offset(), Some(9));
    let send_response =
        SendMessageResponseHeader::decode_fields(&java_ext_fields("header-send-response")).unwrap();
    assert_eq!(send_response.get_msg_id(), Some("msg-1"));
    let recall = RecallMessageRequestHeader::decode_fields(&java_ext_fields(
        "header-recall-message-request",
    ))
    .unwrap();
    assert_eq!(recall.get_producer_group(), Some("producers"));
    assert_eq!(recall.get_topic(), "orders");
    assert_eq!(
        recall.get_recall_handle(),
        "message-id%orders%1712345678901"
    );
    assert_eq!(recall.get_namespace(), Some("tenant"));
    assert_eq!(recall.get_namespaced(), Some(true));
    assert_eq!(recall.get_broker_name(), Some("broker-a"));
    assert_eq!(recall.get_oneway(), Some(false));
    assert_eq!(recall.get_lo(), Some(true));
    let recall_response = RecallMessageResponseHeader::decode_fields(&java_ext_fields(
        "header-recall-message-response",
    ))
    .unwrap();
    assert_eq!(recall_response.get_msg_id(), Some("broker-message-id"));
    let unregister =
        UnregisterClientRequestHeader::decode_fields(&java_ext_fields("header-unregister-request"))
            .unwrap();
    assert_eq!(unregister.get_client_id(), "client-1");
    assert_eq!(unregister.get_producer_group(), Some("producers"));
    let send_back = ConsumerSendMsgBackRequestHeader::decode_fields(&java_ext_fields(
        "header-send-back-request",
    ))
    .unwrap();
    assert_eq!(send_back.get_group(), "workers");
    assert_eq!(send_back.get_offset(), 9);

    let pop =
        PopMessageRequestHeader::decode_fields(&java_ext_fields("header-pop-request")).unwrap();
    assert_eq!(pop.get_max_msg_nums(), 16);
    assert_eq!(pop.get_invisible_time(), 30_000);
    let ack =
        AckMessageRequestHeader::decode_fields(&java_ext_fields("header-ack-request")).unwrap();
    assert_eq!(ack.get_offset(), 9);
    let change = ChangeInvisibleTimeRequestHeader::decode_fields(&java_ext_fields(
        "header-change-invisible-request",
    ))
    .unwrap();
    assert_eq!(change.get_invisible_time(), Some(60_000));
    let notification =
        NotificationRequestHeader::decode_fields(&java_ext_fields("header-notification-request"))
            .unwrap();
    assert_eq!(notification.get_born_time(), 1_712_345_678_901);
    let polling =
        PollingInfoRequestHeader::decode_fields(&java_ext_fields("header-polling-request"))
            .unwrap();
    assert_eq!(polling.get_queue_id(), 1);
    let polling_response =
        PollingInfoResponseHeader::decode_fields(&java_ext_fields("header-polling-response"))
            .unwrap();
    assert_eq!(polling_response.get_polling_num(), 2);
    let end = EndTransactionRequestHeader::decode_fields(&java_ext_fields(
        "header-end-transaction-request",
    ))
    .unwrap();
    assert_eq!(end.get_transaction_id(), Some("tx-1"));
    let check = CheckTransactionStateRequestHeader::decode_fields(&java_ext_fields(
        "header-check-transaction-request",
    ))
    .unwrap();
    assert_eq!(check.get_offset_msg_id(), Some("offset-1"));
    let check_response = CheckTransactionStateResponseHeader::decode_fields(&java_ext_fields(
        "header-check-transaction-response",
    ))
    .unwrap();
    assert_eq!(check_response.get_commit_or_rollback(), Some(8));
    let reply =
        ReplyMessageRequestHeader::decode_fields(&java_ext_fields("header-reply-request")).unwrap();
    assert_eq!(reply.get_born_host(), "127.0.0.1:1001");
    let notification_response =
        NotificationResponseHeader::decode_fields(&java_ext_fields("header-notification-response"))
            .unwrap();
    assert!(notification_response.is_has_msg());
    let reset =
        ResetOffsetRequestHeader::decode_fields(&java_ext_fields("header-reset-offset-request"))
            .unwrap();
    assert_eq!(reset.get_offset(), Some(9));
    let status = GetConsumerStatusRequestHeader::decode_fields(&java_ext_fields(
        "header-consumer-status-request",
    ))
    .unwrap();
    assert_eq!(status.get_client_addr(), None);
    let direct = ConsumeMessageDirectlyResultRequestHeader::decode_fields(&java_ext_fields(
        "header-consume-directly-request",
    ))
    .unwrap();
    assert_eq!(direct.get_msg_id(), None);
    assert_eq!(direct.get_lo(), Some(true));
    let lite =
        PopLiteMessageRequestHeader::decode_fields(&java_ext_fields("header-pop-lite-request"))
            .unwrap();
    assert_eq!(lite.get_client_id(), Some("client"));
    assert!(UnregisterClientResponseHeader::decode_fields(&HeaderMap::new()).is_ok());
    assert!(
        UnregisterClientResponseHeader::decode_fields(&java_ext_fields(
            "header-unregister-response"
        ))
        .is_ok()
    );
    let lock =
        LockBatchMqRequestHeader::decode_fields(&java_ext_fields("header-lock-batch-request"))
            .unwrap();
    assert_eq!(lock.get_namespace(), Some("tenant"));
    assert_eq!(lock.get_broker_name(), Some("broker-a"));
    let unlock =
        UnlockBatchMqRequestHeader::decode_fields(&java_ext_fields("header-unlock-batch-request"))
            .unwrap();
    assert_eq!(unlock.get_namespace(), Some("tenant"));
    assert!(
        EndTransactionResponseHeader::decode_fields(&java_ext_fields(
            "header-end-transaction-response"
        ))
        .is_ok()
    );
    let lite_ctl = LiteSubscriptionCtlRequestHeader::decode_fields(&java_ext_fields(
        "header-lite-subscription-request",
    ))
    .unwrap();
    assert_eq!(lite_ctl, LiteSubscriptionCtlRequestHeader::default());
    let unsubscribe = NotifyUnsubscribeLiteRequestHeader::decode_fields(&java_ext_fields(
        "header-notify-unsubscribe-lite-request",
    ))
    .unwrap();
    assert_eq!(unsubscribe.get_lite_topic(), "lite-orders");

    let send_batch =
        SendMessageRequestHeaderV2::decode_fields(&java_ext_fields("header-send-batch-v2"))
            .unwrap();
    assert_eq!(send_batch.is_m(), Some(false));
    let assignment: QueryAssignmentRequestBody =
        serde_json::from_slice(&java_body("body-query-assignment")).unwrap();
    assert_eq!(
        assignment.get_message_model(),
        Some(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering)
    );

    let mut pop_response = PopMessageResponseHeader::default();
    pop_response.set_pop_time(1);
    pop_response.set_invisible_time(2);
    pop_response.set_revive_qid(3);
    pop_response.set_rest_num(4);
    let mut fields = HeaderMap::new();
    pop_response.encode_fields(&mut fields);
    let decoded_pop_response = PopMessageResponseHeader::decode_fields(&fields).unwrap();
    let mut decoded_pop_fields = HeaderMap::new();
    decoded_pop_response.encode_fields(&mut decoded_pop_fields);
    assert_eq!(decoded_pop_fields, fields);

    let consumers = GetConsumerListByGroupRequestHeader::decode_fields(&java_ext_fields(
        "header-consumer-list-request",
    ))
    .unwrap();
    assert_eq!(consumers.get_consumer_group(), "workers");

    let mut pop_fields = java_ext_fields("header-pop-lite-response");
    assert_eq!(
        PopLiteMessageResponseHeader::decode_fields(&pop_fields)
            .unwrap()
            .get_pop_time(),
        10
    );
    for missing in ["popTime", "invisibleTime", "reviveQid"] {
        pop_fields = java_ext_fields("header-pop-lite-response");
        pop_fields.remove(missing);
        let decoded = PopLiteMessageResponseHeader::decode_fields(&pop_fields).unwrap();
        match missing {
            "popTime" => assert_eq!(decoded.get_pop_time(), 0),
            "invisibleTime" => assert_eq!(decoded.get_invisible_time(), 0),
            "reviveQid" => assert_eq!(decoded.get_revive_qid(), 0),
            _ => unreachable!("test field list is exhaustive"),
        }
    }
}

#[test]
fn reset_master_flush_offset_header_matches_java_nullable_long_and_reflection_decode() {
    let raw = java_ext_fields("header-reset-master-flush-offset");
    let decoded = ResetMasterFlushOffsetHeader::decode_fields(&raw).unwrap();
    assert_eq!(decoded.get_master_flush_offset(), Some(9_876_543_210));
    let mut encoded = HeaderMap::new();
    decoded.encode_fields(&mut encoded);
    assert_eq!(encoded, raw);

    let malformed = HeaderMap::from([("masterFlushOffset".into(), "not-a-long".into())]);
    assert_eq!(
        ResetMasterFlushOffsetHeader::decode_fields(&malformed)
            .unwrap()
            .get_master_flush_offset(),
        None
    );
    assert_eq!(
        ResetMasterFlushOffsetHeader::decode_fields(&HeaderMap::new())
            .unwrap()
            .get_master_flush_offset(),
        None
    );

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    for case_name in [
        "reset-master-flush-offset-valid",
        "reset-master-flush-offset-missing",
        "reset-master-flush-offset-invalid",
    ] {
        let case = &fixture["cases"][case_name];
        let fields: HeaderMap = serde_json::from_value(case["fields"].clone()).unwrap();
        for (mode_name, use_fast_encode) in [("fast", true), ("reflection", false)] {
            let expected = &case[mode_name];
            let decoded =
                ResetMasterFlushOffsetHeader::decode_fields_with_mode(&fields, use_fast_encode)
                    .unwrap();
            assert_eq!(
                decoded.get_master_flush_offset(),
                expected["decodedFields"]["masterFlushOffset"].as_i64(),
                "{case_name} {mode_name}"
            );
            let mut encoded = HeaderMap::new();
            decoded.encode_fields(&mut encoded);
            let expected_fields: HeaderMap =
                serde_json::from_value(expected["encodedFields"].clone()).unwrap();
            assert_eq!(encoded, expected_fields, "{case_name} {mode_name}");
        }
    }
}

#[test]
fn query_consume_queue_header_matches_java_decode_modes() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    for case_name in [
        "query-consume-queue-valid",
        "query-consume-queue-missing",
        "query-consume-queue-invalid-numbers",
    ] {
        let case = &fixture["cases"][case_name];
        let fields: HeaderMap = serde_json::from_value(case["fields"].clone()).unwrap();
        for (mode_name, use_fast_encode) in [("fast", true), ("reflection", false)] {
            let expected = &case[mode_name];
            let header =
                QueryConsumeQueueRequestHeader::decode_fields_with_mode(&fields, use_fast_encode)
                    .unwrap();
            let decoded = &expected["decodedFields"];
            assert_eq!(
                header.get_topic(),
                decoded["topic"].as_str(),
                "{case_name} {mode_name}"
            );
            assert_eq!(
                header.get_consumer_group(),
                decoded["consumerGroup"].as_str(),
                "{case_name} {mode_name}"
            );
            assert_eq!(
                header.get_queue_id(),
                decoded["queueId"].as_i64().unwrap() as i32
            );
            assert_eq!(header.get_index(), decoded["index"].as_i64().unwrap());
            assert_eq!(
                header.get_count(),
                decoded["count"].as_i64().unwrap() as i32
            );
            assert_eq!(header.get_broker_name(), decoded["bname"].as_str());
            assert_eq!(header.get_namespace(), decoded["ns"].as_str());
            assert_eq!(header.get_namespaced(), decoded["nsd"].as_bool());
            assert_eq!(header.get_lo(), decoded["lo"].as_bool());
            assert_eq!(header.get_oneway(), decoded["oway"].as_bool());
            let mut encoded = HeaderMap::new();
            header.encode_fields(&mut encoded);
            let expected_fields: HeaderMap =
                serde_json::from_value(expected["encodedFields"].clone()).unwrap();
            assert_eq!(encoded, expected_fields, "{case_name} {mode_name}");
        }
    }

    let mut left = QueryConsumeQueueRequestHeader::default();
    let mut right = QueryConsumeQueueRequestHeader::default();
    right.set_topic(Some("orders".into()));
    right.set_queue_id(2);
    right.set_index(13);
    right.set_count(4);
    right.set_consumer_group(Some("workers".into()));
    assert_eq!(
        left, right,
        "Java inherited equality ignores subclass fields"
    );
    right.set_namespace(Some("tenant".into()));
    assert_ne!(left, right, "Java inherited equality includes RPC fields");
    left.set_namespace(Some("tenant".into()));
    assert_eq!(left, right);
}

#[test]
fn export_rocksdb_config_header_and_enum_match_java_contract() {
    let raw = java_ext_fields("header-export-rocksdb-config-request");
    let header = ExportRocksDBConfigToJsonRequestHeader::decode_fields(&raw).unwrap();
    let expected = [
        ExportRocksDBConfigType::Topics,
        ExportRocksDBConfigType::SubscriptionGroups,
        ExportRocksDBConfigType::ConsumerOffsets,
    ];
    assert_eq!(
        header.get_config_type(),
        Some("topics;subscriptionGroups;consumerOffsets;")
    );
    assert_eq!(header.fetch_config_type().unwrap(), expected);

    let mut encoded = HeaderMap::new();
    header.encode_fields(&mut encoded);
    assert_eq!(encoded, raw);
    let mut updated = ExportRocksDBConfigToJsonRequestHeader::default();
    updated.update_config_type(&expected);
    assert_eq!(updated.get_config_type(), header.get_config_type());
    updated.set_config_type(None);
    assert_eq!(updated.get_config_type(), None);
    assert!(matches!(
        updated.fetch_config_type(),
        Err(rocketmq_classic_protocol::header::ProtocolError::MissingField("configType"))
    ));

    assert_eq!(
        ExportRocksDBConfigType::from_string(" topics ; SUBSCRIPTIONGROUPS;consumerOffsets;")
            .unwrap(),
        expected
    );
    assert!(ExportRocksDBConfigType::from_string("topics;unknown").is_err());
    assert_eq!(
        ExportRocksDBConfigType::to_string(&expected),
        "topics;subscriptionGroups;consumerOffsets;"
    );
    assert_eq!(ExportRocksDBConfigType::values(), &expected);
    assert_eq!(
        ExportRocksDBConfigType::SubscriptionGroups.name(),
        "SUBSCRIPTION_GROUPS"
    );
    assert_eq!(ExportRocksDBConfigType::ConsumerOffsets.ordinal(), 2);
    assert_eq!(ExportRocksDBConfigType::value_of("TOPICS"), Ok(expected[0]));

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    for case_name in [
        "export-rocksdb-config-valid",
        "export-rocksdb-config-empty",
        "export-rocksdb-config-missing",
    ] {
        let case = &fixture["cases"][case_name];
        let fields: HeaderMap = serde_json::from_value(case["fields"].clone()).unwrap();
        for mode in ["fast", "reflection"] {
            let expected_fields: HeaderMap =
                serde_json::from_value(case[mode]["encodedFields"].clone()).unwrap();
            let decoded = ExportRocksDBConfigToJsonRequestHeader::decode_fields_with_mode(
                &fields,
                mode == "fast",
            )
            .unwrap();
            let mut actual_fields = HeaderMap::new();
            decoded.encode_fields(&mut actual_fields);
            assert_eq!(actual_fields, expected_fields, "{case_name} {mode}");
        }
    }

    let producer_fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    for case in producer_fixture["exportRocksDBConfigTypeParsing"]["cases"]
        .as_array()
        .unwrap()
    {
        let input = case["input"].as_str().unwrap();
        if case["outcome"] == "returned" {
            let parsed = ExportRocksDBConfigType::from_string(input).unwrap();
            assert_eq!(
                parsed.iter().map(|value| value.name()).collect::<Vec<_>>(),
                case["enumNames"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_str().unwrap())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                parsed
                    .iter()
                    .map(|value| value.get_type_name())
                    .collect::<Vec<_>>(),
                case["typeNames"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_str().unwrap())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                ExportRocksDBConfigType::to_string(&parsed),
                case["serialized"]
            );
        } else {
            match ExportRocksDBConfigType::from_string(input).unwrap_err() {
                rocketmq_classic_protocol::header::ProtocolError::InvalidField {
                    reason, ..
                } => {
                    assert_eq!(reason, case["errorMessage"]);
                }
                error => panic!("unexpected error: {error}"),
            }
        }
    }
}

#[test]
fn check_rocksdb_cq_write_progress_header_matches_java_decode_modes() {
    let raw = java_ext_fields("header-check-rocksdb-cq-write-progress-request");
    let header = CheckRocksdbCqWriteProgressRequestHeader::decode_fields(&raw).unwrap();
    assert_eq!(header.get_topic(), Some("orders"));
    assert_eq!(header.get_check_store_time(), 1_712_345_678_901);
    let mut encoded = HeaderMap::new();
    header.encode_fields(&mut encoded);
    assert_eq!(encoded, raw);

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    for case_name in [
        "check-rocksdb-cq-write-progress-valid",
        "check-rocksdb-cq-write-progress-missing",
        "check-rocksdb-cq-write-progress-invalid-time",
    ] {
        let case = &fixture["cases"][case_name];
        let fields: HeaderMap = serde_json::from_value(case["fields"].clone()).unwrap();
        for (mode_name, use_fast_encode) in [("fast", true), ("reflection", false)] {
            let expected = &case[mode_name];
            let decoded = CheckRocksdbCqWriteProgressRequestHeader::decode_fields_with_mode(
                &fields,
                use_fast_encode,
            )
            .unwrap();
            assert_eq!(
                decoded.get_topic(),
                expected["decodedFields"]["topic"].as_str(),
                "{case_name} {mode_name}"
            );
            assert_eq!(
                decoded.get_check_store_time(),
                expected["decodedFields"]["checkStoreTime"]
                    .as_i64()
                    .unwrap(),
                "{case_name} {mode_name}"
            );
            let mut encoded = HeaderMap::new();
            decoded.encode_fields(&mut encoded);
            let expected_fields: HeaderMap =
                serde_json::from_value(expected["encodedFields"].clone()).unwrap();
            assert_eq!(encoded, expected_fields, "{case_name} {mode_name}");
        }
    }
}

#[test]
fn write_permission_headers_match_java_nullable_and_boxed_fields() {
    let wipe_request_fields = java_ext_fields("header-wipe-write-perm-request");
    let wipe_request =
        WipeWritePermOfBrokerRequestHeader::decode_fields(&wipe_request_fields).unwrap();
    assert_eq!(wipe_request.get_broker_name(), Some("broker-a"));
    let mut encoded = HeaderMap::new();
    wipe_request.encode_fields(&mut encoded);
    assert_eq!(encoded, wipe_request_fields);

    let add_request_fields = java_ext_fields("header-add-write-perm-request");
    let add_request =
        AddWritePermOfBrokerRequestHeader::decode_fields(&add_request_fields).unwrap();
    assert_eq!(add_request.get_broker_name(), Some("broker-a"));
    let mut encoded = HeaderMap::new();
    add_request.encode_fields(&mut encoded);
    assert_eq!(encoded, add_request_fields);

    let mut missing_request = WipeWritePermOfBrokerRequestHeader::default();
    assert!(missing_request.get_broker_name().is_none());
    assert!(missing_request.check_fields().is_ok());
    missing_request.set_broker_name(None);
    let mut encoded = HeaderMap::new();
    missing_request.encode_fields(&mut encoded);
    assert!(encoded.is_empty());

    let wipe_response_fields = java_ext_fields("header-wipe-write-perm-response");
    let wipe_response =
        WipeWritePermOfBrokerResponseHeader::decode_fields(&wipe_response_fields).unwrap();
    assert_eq!(wipe_response.get_wipe_topic_count(), Some(17));
    let mut encoded = HeaderMap::new();
    wipe_response.encode_fields(&mut encoded);
    assert_eq!(encoded, wipe_response_fields);

    let add_response_fields = java_ext_fields("header-add-write-perm-response");
    let add_response =
        AddWritePermOfBrokerResponseHeader::decode_fields(&add_response_fields).unwrap();
    assert_eq!(add_response.get_add_topic_count(), Some(-3));
    let mut encoded = HeaderMap::new();
    add_response.encode_fields(&mut encoded);
    assert_eq!(encoded, add_response_fields);

    for (header_name, field_name) in [
        ("wipe-write-perm-response", "wipeTopicCount"),
        ("add-write-perm-response", "addTopicCount"),
    ] {
        let mut malformed = HeaderMap::new();
        malformed.insert(field_name.into(), "not-an-int".into());
        let mut decoded = HeaderMap::new();
        if header_name.starts_with("wipe") {
            let header = WipeWritePermOfBrokerResponseHeader::decode_fields(&malformed).unwrap();
            assert!(header.get_wipe_topic_count().is_none());
            header.encode_fields(&mut decoded);
        } else {
            let header = AddWritePermOfBrokerResponseHeader::decode_fields(&malformed).unwrap();
            assert!(header.get_add_topic_count().is_none());
            header.encode_fields(&mut decoded);
        }
        assert!(decoded.is_empty());
    }

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    for case_name in [
        "wipe-write-perm-request-valid",
        "wipe-write-perm-request-missing",
        "add-write-perm-request-valid",
        "add-write-perm-request-missing",
        "wipe-write-perm-response-valid",
        "wipe-write-perm-response-missing",
        "wipe-write-perm-response-invalid",
        "add-write-perm-response-valid",
        "add-write-perm-response-missing",
        "add-write-perm-response-invalid",
    ] {
        let case = &fixture["cases"][case_name];
        let fields: HeaderMap = serde_json::from_value(case["fields"].clone()).unwrap();
        for mode in ["fast", "reflection"] {
            assert_eq!(case[mode]["outcome"], "OK", "{case_name} {mode}");
            let mut actual = HeaderMap::new();
            match case["headerType"].as_str().unwrap() {
                "wipe-write-perm-request" => {
                    WipeWritePermOfBrokerRequestHeader::decode_fields_with_mode(
                        &fields,
                        mode == "fast",
                    )
                    .unwrap()
                    .encode_fields(&mut actual)
                }
                "add-write-perm-request" => {
                    AddWritePermOfBrokerRequestHeader::decode_fields_with_mode(
                        &fields,
                        mode == "fast",
                    )
                    .unwrap()
                    .encode_fields(&mut actual)
                }
                "wipe-write-perm-response" => {
                    WipeWritePermOfBrokerResponseHeader::decode_fields_with_mode(
                        &fields,
                        mode == "fast",
                    )
                    .unwrap()
                    .encode_fields(&mut actual)
                }
                "add-write-perm-response" => {
                    AddWritePermOfBrokerResponseHeader::decode_fields_with_mode(
                        &fields,
                        mode == "fast",
                    )
                    .unwrap()
                    .encode_fields(&mut actual)
                }
                other => panic!("unexpected write-permission header type {other}"),
            }
            let expected: HeaderMap =
                serde_json::from_value(case[mode]["encodedFields"].clone()).unwrap();
            assert_eq!(actual, expected, "{case_name} {mode}");
        }
    }
}

#[test]
fn lite_topic_info_request_header_matches_java_public_api_and_nullable_fields() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/rpc-invoked-ext-more/lite-topic-info-success.json"
    ))
    .unwrap();
    let api = &fixture["liteTopicInfoApi"];
    assert_eq!(api["requestHeaderPublicFields"], serde_json::json!([]));
    assert_eq!(
        api["requestHeaderConstructors"],
        serde_json::json!([
            "public org.apache.rocketmq.remoting.protocol.header.GetLiteTopicInfoRequestHeader()"
        ])
    );
    assert_eq!(
        api["requestHeaderPublicMethods"],
        serde_json::json!([
            "public java.lang.String org.apache.rocketmq.remoting.protocol.header.GetLiteTopicInfoRequestHeader.getLiteTopic()",
            "public java.lang.String org.apache.rocketmq.remoting.protocol.header.GetLiteTopicInfoRequestHeader.getParentTopic()",
            "public void org.apache.rocketmq.remoting.protocol.header.GetLiteTopicInfoRequestHeader.checkFields() throws org.apache.rocketmq.remoting.exception.RemotingCommandException",
            "public void org.apache.rocketmq.remoting.protocol.header.GetLiteTopicInfoRequestHeader.setLiteTopic(java.lang.String)",
            "public void org.apache.rocketmq.remoting.protocol.header.GetLiteTopicInfoRequestHeader.setParentTopic(java.lang.String)"
        ])
    );

    let mut header = GetLiteTopicInfoRequestHeader::default();
    assert_eq!(header.get_parent_topic(), None);
    assert_eq!(header.get_lite_topic(), None);
    header.check_fields().unwrap();
    let mut fields = HeaderMap::new();
    header.encode_fields(&mut fields);
    assert!(fields.is_empty());

    header.set_parent_topic(Some("lite-parent".into()));
    header.set_lite_topic(Some("%LMQ%lite-parent%orders".into()));
    header.check_fields().unwrap();
    let mut encoded = HeaderMap::new();
    header.encode_fields(&mut encoded);
    assert_eq!(encoded, java_ext_fields("header-lite-topic-info-request"));
    let decoded = GetLiteTopicInfoRequestHeader::decode_fields(&encoded).unwrap();
    assert_eq!(decoded.get_parent_topic(), Some("lite-parent"));
    assert_eq!(decoded.get_lite_topic(), Some("%LMQ%lite-parent%orders"));
}

#[test]
fn lite_client_info_request_header_matches_java_defaults_and_validation() {
    let mut header = GetLiteClientInfoRequestHeader::default();
    assert_eq!(header.get_parent_topic(), None);
    assert_eq!(header.get_group(), None);
    assert_eq!(header.get_client_id(), None);
    assert_eq!(header.get_max_count(), 1000);
    header.check_fields().unwrap();

    header.set_parent_topic(Some("lite-parent".into()));
    header.set_group(Some("workers".into()));
    header.set_client_id(Some("client-a".into()));
    let mut encoded = HeaderMap::new();
    header.encode_fields(&mut encoded);
    assert_eq!(encoded, java_ext_fields("header-lite-client-info-request"));
    assert_eq!(encoded.get("maxCount").map(String::as_str), Some("1000"));
    let decoded = GetLiteClientInfoRequestHeader::decode_fields_checked(&encoded).unwrap();
    assert_eq!(decoded.get_parent_topic(), Some("lite-parent"));
    assert_eq!(decoded.get_group(), Some("workers"));
    assert_eq!(decoded.get_client_id(), Some("client-a"));
    assert_eq!(decoded.get_max_count(), 1000);

    header.set_max_count(0);
    assert!(header.check_fields().is_err());
}

#[test]
fn lite_group_info_request_header_matches_java_defaults_and_nullable_inputs() {
    let mut header = GetLiteGroupInfoRequestHeader::default();
    assert_eq!(header.get_group(), None);
    assert_eq!(header.get_lite_topic(), None);
    assert_eq!(header.get_top_k(), 0);
    header.check_fields().unwrap();

    let mut default_fields = HeaderMap::new();
    header.encode_fields(&mut default_fields);
    assert_eq!(
        default_fields,
        HeaderMap::from([("topK".into(), "0".into())])
    );

    header.set_group(Some("workers".into()));
    header.set_lite_topic(Some("%LMQ%lite-parent%orders".into()));
    header.set_top_k(5);
    let mut encoded = HeaderMap::new();
    header.encode_fields(&mut encoded);
    assert_eq!(encoded, java_ext_fields("header-lite-group-info-request"));
    let decoded = GetLiteGroupInfoRequestHeader::decode_fields_checked(&encoded).unwrap();
    assert_eq!(decoded.get_group(), Some("workers"));
    assert_eq!(decoded.get_lite_topic(), Some("%LMQ%lite-parent%orders"));
    assert_eq!(decoded.get_top_k(), 5);

    header.set_group(None);
    header.set_lite_topic(None);
    header.set_top_k(-3);
    header.check_fields().unwrap();
}

#[test]
fn parent_topic_info_request_header_matches_java_fixture_and_nullable_topic() {
    let mut header = GetParentTopicInfoRequestHeader::default();
    assert_eq!(header.get_topic(), None);
    let mut fields = HeaderMap::new();
    header.encode_fields(&mut fields);
    assert!(fields.is_empty());

    header.set_topic(Some("lite-parent".into()));
    let mut encoded = HeaderMap::new();
    header.encode_fields(&mut encoded);
    assert_eq!(encoded, java_ext_fields("header-parent-topic-info-request"));
    assert_eq!(
        encoded.get("topic").map(String::as_str),
        Some("lite-parent")
    );
    let decoded = GetParentTopicInfoRequestHeader::decode_fields(&encoded).unwrap();
    assert_eq!(decoded.get_topic(), Some("lite-parent"));
}

#[test]
fn create_topic_filter_type_enum_getter_preserves_java_value_of_failures() {
    let mut header = CreateTopicRequestHeader::default();
    assert!(header.get_topic_filter_type_enum().is_err());
    header.set_topic_filter_type(Some("MULTI_TAG".into()));
    assert!(matches!(
        header.get_topic_filter_type_enum(),
        Ok(TopicFilterType::MultiTag)
    ));
    header.set_topic_filter_type(Some("NOT_A_FILTER".into()));
    assert!(header.get_topic_filter_type_enum().is_err());
}

#[test]
fn response_header_accessors_match_java_inventory_and_null_defaults() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/header-response-identity.json"
    ))
    .unwrap();
    let inventories = &fixture["apiInventories"];
    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");

    for (class, inventory) in inventories.as_object().unwrap() {
        assert_eq!(
            inventory["publicFields"],
            serde_json::json!([]),
            "Java {class} must not expose public fields"
        );
        let declaration = format!("pub struct {class}");
        let struct_tail = source
            .split_once(&declaration)
            .unwrap_or_else(|| panic!("missing Rust response header {class}"))
            .1;
        let struct_body = if struct_tail.starts_with(" {") {
            struct_tail.split_once("\n}\n").unwrap().0
        } else {
            ""
        };
        assert!(
            struct_body
                .lines()
                .all(|line| !line.trim_start().starts_with("pub ")),
            "Rust {class} must not expose public fields"
        );

        for signature in inventory["callablePublicMethods"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(serde_json::Value::as_str)
        {
            let (prefix, parameters) = signature.split_once('(').unwrap();
            let name = prefix
                .split_whitespace()
                .last()
                .unwrap()
                .rsplit('.')
                .next()
                .unwrap();
            let rust_method: Option<String> = match name {
                "encode" => Some("encode_fields".into()),
                "decode" => Some("decode_fields".into()),
                "checkFields" => Some("check_fields".into()),
                _ if matches!(
                    name,
                    "getAndCheckNotNull" | "writeIfNotNull" | "writeInt" | "writeLong"
                ) =>
                {
                    Some(java_camel_to_snake(name))
                }
                _ if name.starts_with("get") || name.starts_with("is") => {
                    let parameter_count = if parameters.starts_with(')') {
                        0
                    } else {
                        parameters.matches(',').count() + 1
                    };
                    (parameter_count == 0).then(|| java_camel_to_snake(name))
                }
                _ if name.starts_with("set") => {
                    let parameter_count = if parameters.starts_with(')') {
                        0
                    } else {
                        parameters.matches(',').count() + 1
                    };
                    (parameter_count == 1).then(|| java_camel_to_snake(name))
                }
                _ => None,
            };
            if let Some(rust_method) = rust_method {
                assert!(
                    rust_header_method_exists(source, trait_source, class, &rust_method),
                    "Java {class}::{name} has no Rust {class}::{rust_method}"
                );
            }
        }
    }

    for (class, methods) in [
        (
            "PullMessageResponseHeader",
            ["getNextBeginOffset()", "setNextBeginOffset(java.lang.Long)"].as_slice(),
        ),
        (
            "SendMessageResponseHeader",
            ["getMsgId()", "setMsgId(java.lang.String)"].as_slice(),
        ),
        (
            "CheckTransactionStateResponseHeader",
            [
                "getProducerGroup()",
                "setProducerGroup(java.lang.String)",
                "getTranStateTableOffset()",
            ]
            .as_slice(),
        ),
        (
            "NotificationResponseHeader",
            ["isHasMsg()", "setHasMsg(boolean)"].as_slice(),
        ),
        (
            "PopMessageResponseHeader",
            ["getPopTime()", "setPopTime(long)", "getReviveQid()"].as_slice(),
        ),
        (
            "PopLiteMessageResponseHeader",
            ["getPopTime()", "setPopTime(long)", "getReviveQid()"].as_slice(),
        ),
        (
            "PollingInfoResponseHeader",
            ["getPollingNum()", "setPollingNum(int)"].as_slice(),
        ),
    ] {
        let java_methods = inventories[class]["declaredPublicMethods"]
            .as_array()
            .unwrap();
        for method in methods {
            assert!(
                java_methods.iter().any(|signature| {
                    signature.as_str().unwrap().ends_with(&format!(".{method}"))
                }),
                "missing Java method {class}.{method}"
            );
        }
        assert_eq!(inventories[class]["publicFields"], serde_json::json!([]));
    }

    let mut pull = PullMessageResponseHeader::default();
    assert_eq!(pull.get_next_begin_offset(), None);
    pull.set_next_begin_offset(Some(10));
    assert_eq!(pull.get_next_begin_offset(), Some(10));

    let mut send = SendMessageResponseHeader::default();
    assert_eq!(send.get_msg_id(), None);
    send.set_msg_id(Some("message-id".into()));
    assert_eq!(send.get_msg_id(), Some("message-id"));
    send.set_msg_id(None);
    assert_eq!(send.get_msg_id(), None);

    let mut transaction = CheckTransactionStateResponseHeader::default();
    assert_eq!(transaction.get_producer_group(), None);
    assert_eq!(transaction.get_tran_state_table_offset(), None);
    assert_eq!(transaction.get_commit_log_offset(), None);
    assert_eq!(transaction.get_commit_or_rollback(), None);
    transaction.set_producer_group(Some("producers".into()));
    transaction.set_tran_state_table_offset(Some(3));
    transaction.set_commit_log_offset(Some(9));
    transaction.set_commit_or_rollback(Some(8));
    assert_eq!(transaction.get_producer_group(), Some("producers"));
    assert_eq!(transaction.get_tran_state_table_offset(), Some(3));
    assert_eq!(transaction.get_commit_log_offset(), Some(9));
    assert_eq!(transaction.get_commit_or_rollback(), Some(8));

    let mut notification = NotificationResponseHeader::default();
    assert!(!notification.is_has_msg());
    assert!(!notification.is_polling_full());
    notification.set_has_msg(true);
    notification.set_polling_full(true);
    assert!(notification.is_has_msg());
    assert!(notification.is_polling_full());

    let mut pop = PopMessageResponseHeader::default();
    assert_eq!(pop.get_pop_time(), 0);
    assert_eq!(pop.get_start_offset_info(), None);
    pop.set_pop_time(12);
    pop.set_start_offset_info(Some("queue-offsets".into()));
    assert_eq!(pop.get_pop_time(), 12);
    assert_eq!(pop.get_start_offset_info(), Some("queue-offsets"));

    let mut pop_lite = PopLiteMessageResponseHeader::default();
    assert_eq!(pop_lite.get_order_count_info(), None);
    pop_lite.set_order_count_info(Some("0".into()));
    assert_eq!(pop_lite.get_order_count_info(), Some("0"));

    let mut polling = PollingInfoResponseHeader::default();
    assert_eq!(polling.get_polling_num(), 0);
    polling.set_polling_num(2);
    assert_eq!(polling.get_polling_num(), 2);
}

#[test]
fn fast_codes_header_numeric_helpers_write_java_decimal_fields() {
    let header = PullMessageResponseHeader::default();
    let mut fields = HeaderMap::new();
    header.write_long(&mut fields, "minimum", i64::MIN);
    header.write_int(&mut fields, "maximum", i32::MAX);
    header.write_if_not_null(&mut fields, "nullable", Some(i64::MAX));
    header.write_if_not_null::<i32>(&mut fields, "omitted", None);

    assert_eq!(
        fields,
        HeaderMap::from([
            ("minimum".to_owned(), i64::MIN.to_string()),
            ("maximum".to_owned(), i32::MAX.to_string()),
            ("nullable".to_owned(), i64::MAX.to_string()),
        ])
    );
    assert_eq!(
        header.get_and_check_not_null(&fields, "minimum"),
        Some(i64::MIN.to_string().as_str())
    );
    assert_eq!(header.get_and_check_not_null(&fields, "missing"), None);
}

#[test]
fn optional_numeric_header_reflection_failures_match_java_551() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let fixture: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(format!(
            "{root}/fixtures/remoting/optional-numeric-header-decode-modes.json"
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        fixture["client"],
        "org.apache.rocketmq:rocketmq-client:5.5.1"
    );
    assert_eq!(
        fixture["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );
    assert_eq!(fixture["serverContacted"], false);
    let cases = fixture["cases"].as_object().unwrap();
    assert_eq!(cases.len(), 18);

    for (case_name, case) in cases {
        let header_type = case["headerType"].as_str().unwrap();
        let fields: HeaderMap = serde_json::from_value(case["fields"].clone()).unwrap();
        for (mode_name, use_fast_encode) in [("fast", true), ("reflection", false)] {
            let expected_mode = &case[mode_name];
            assert_eq!(expected_mode["outcome"], "OK", "{case_name} {mode_name}");
            let expected: HeaderMap =
                serde_json::from_value(expected_mode["encodedFields"].clone()).unwrap();
            let actual = match header_type {
                "send-back" => encode_decoded_header::<ConsumerSendMsgBackRequestHeader>(
                    &fields,
                    use_fast_encode,
                ),
                "reset-offset" => {
                    encode_decoded_header::<ResetOffsetRequestHeader>(&fields, use_fast_encode)
                }
                "consume-directly" => encode_decoded_header::<
                    ConsumeMessageDirectlyResultRequestHeader,
                >(&fields, use_fast_encode),
                "reply-message" => {
                    encode_decoded_header::<ReplyMessageRequestHeader>(&fields, use_fast_encode)
                }
                "send-v1" => {
                    encode_decoded_header::<SendMessageRequestHeaderV1>(&fields, use_fast_encode)
                }
                "pop" => encode_decoded_header::<PopMessageRequestHeader>(&fields, use_fast_encode),
                "pop-lite" => {
                    encode_decoded_header::<PopLiteMessageRequestHeader>(&fields, use_fast_encode)
                }
                "pop-response" => {
                    encode_decoded_header::<PopMessageResponseHeader>(&fields, use_fast_encode)
                }
                "pop-lite-response" => {
                    encode_decoded_header::<PopLiteMessageResponseHeader>(&fields, use_fast_encode)
                }
                other => panic!("unknown Java header type {other}"),
            }
            .unwrap_or_else(|error| panic!("{case_name} {mode_name}: {error}"));
            assert_eq!(actual, expected, "{case_name} {mode_name}");
        }
    }

    let default_header = PopMessageRequestHeader::default();
    let mut default_fields = HeaderMap::new();
    default_header.encode_fields(&mut default_fields);
    let expected_default: HeaderMap =
        serde_json::from_value(cases["pop-defaults"]["reflection"]["encodedFields"].clone())
            .unwrap();
    assert_eq!(default_fields, expected_default);
    assert_eq!(default_header.get_consumer_group(), None);
    assert_eq!(default_header.get_topic(), None);
    assert_eq!(default_header.get_order(), Some(false));

    let default_lite_header = PopLiteMessageRequestHeader::default();
    let mut default_lite_fields = HeaderMap::new();
    default_lite_header.encode_fields(&mut default_lite_fields);
    let expected_lite_default: HeaderMap =
        serde_json::from_value(cases["pop-lite-defaults"]["reflection"]["encodedFields"].clone())
            .unwrap();
    assert_eq!(default_lite_fields, expected_lite_default);
    assert_eq!(default_lite_header.get_client_id(), None);
    assert_eq!(default_lite_header.get_consumer_group(), None);
    assert_eq!(default_lite_header.get_topic(), None);

    let default_pop_response = PopMessageResponseHeader::default();
    let mut default_pop_response_fields = HeaderMap::new();
    default_pop_response.encode_fields(&mut default_pop_response_fields);
    let expected_pop_response_fields: HeaderMap = serde_json::from_value(
        cases["pop-response-defaults"]["reflection"]["encodedFields"].clone(),
    )
    .unwrap();
    assert_eq!(default_pop_response_fields, expected_pop_response_fields);

    let default_lite_response = PopLiteMessageResponseHeader::default();
    let mut default_lite_response_fields = HeaderMap::new();
    default_lite_response.encode_fields(&mut default_lite_response_fields);
    let expected_lite_response_fields: HeaderMap = serde_json::from_value(
        cases["pop-lite-response-defaults"]["reflection"]["encodedFields"].clone(),
    )
    .unwrap();
    assert_eq!(default_lite_response_fields, expected_lite_response_fields);

    let timeout_cases = fixture["popLiteTimeoutCases"].as_object().unwrap();
    assert_eq!(timeout_cases.len(), 2);
    let mut elapsed = PopLiteMessageRequestHeader::default();
    elapsed.set_born_time(1);
    elapsed.set_poll_time(1);
    assert_eq!(
        elapsed.is_timeout_too_much(),
        timeout_cases["elapsed"].as_bool().unwrap()
    );
    elapsed.set_born_time(i64::MAX);
    elapsed.set_poll_time(0);
    assert_eq!(
        elapsed.is_timeout_too_much(),
        timeout_cases["far-future"].as_bool().unwrap()
    );

    let pop_timeout_cases = fixture["popTimeoutCases"].as_object().unwrap();
    assert_eq!(pop_timeout_cases.len(), 2);
    let mut elapsed_pop = PopMessageRequestHeader::default();
    elapsed_pop.set_born_time(1);
    elapsed_pop.set_poll_time(1);
    assert_eq!(
        elapsed_pop.is_timeout_too_much(),
        pop_timeout_cases["elapsed"].as_bool().unwrap()
    );
    elapsed_pop.set_born_time(i64::MAX);
    elapsed_pop.set_poll_time(0);
    assert_eq!(
        elapsed_pop.is_timeout_too_much(),
        pop_timeout_cases["far-future"].as_bool().unwrap()
    );

    let pop_api = &fixture["popHeaderApi"];
    assert_eq!(pop_api.as_object().unwrap().len(), 8);
    assert_eq!(
        PopMessageRequestHeader::default().java_to_string(),
        pop_api["popMessageToStringDefault"]
    );
    assert_eq!(
        PopLiteMessageRequestHeader::default().java_to_string(),
        pop_api["popLiteToStringDefault"]
    );
    let mut pop_api_header = PopMessageRequestHeader::default();
    pop_api_header.set_consumer_group(Some("workers".into()));
    pop_api_header.set_topic(Some("订单".into()));
    pop_api_header.set_queue_id(-9);
    pop_api_header.set_max_msg_nums(3);
    pop_api_header.set_invisible_time(30_000);
    pop_api_header.set_poll_time(1_000);
    pop_api_header.set_born_time(1_712_345_678_901);
    pop_api_header.set_init_mode(1);
    pop_api_header.set_exp_type(Some("TAG".into()));
    pop_api_header.set_exp(Some("blue".into()));
    pop_api_header.set_order(None);
    assert_eq!(pop_api_header.get_queue_id(), -1);
    assert_eq!(pop_api_header.get_order(), None);
    assert!(!pop_api_header.is_order());
    assert_eq!(
        pop_api_header.java_to_string(),
        pop_api["popMessageToStringCustom"]
    );
    pop_api_header.set_order(Some(true));
    assert_eq!(
        pop_api_header.is_order(),
        pop_api["popMessageTrueOrderPredicate"]
    );
    assert_eq!(pop_api["popMessageNegativeQueueIdGetter"], -1);
    assert!(pop_api["popMessageNullableOrderGetter"].is_null());
    assert_eq!(pop_api["popMessageNullableOrderPredicate"], false);
    let mut lite_api_header = PopLiteMessageRequestHeader::default();
    lite_api_header.set_consumer_group(Some("workers".into()));
    lite_api_header.set_topic(Some("订单".into()));
    lite_api_header.set_max_msg_num(4);
    lite_api_header.set_invisible_time(30_000);
    lite_api_header.set_poll_time(1_000);
    lite_api_header.set_born_time(1_712_345_678_901);
    lite_api_header.set_attempt_id(Some("attempt".into()));
    lite_api_header.set_client_id(Some("client".into()));
    assert_eq!(
        lite_api_header.java_to_string(),
        pop_api["popLiteToStringCustom"]
    );
}

#[test]
fn change_invisible_time_header_decode_modes_match_java_551() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let fixture: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(format!(
            "{root}/fixtures/remoting/change-invisible-time-header-decode-modes.json"
        ))
        .unwrap(),
    )
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
    let cases = fixture["cases"].as_object().unwrap();
    assert_eq!(cases.len(), 2);

    for (case_name, case) in cases {
        let fields: HeaderMap = serde_json::from_value(case["fields"].clone()).unwrap();
        for (mode_name, use_fast_encode) in [("fast", true), ("reflection", false)] {
            let expected_mode = &case[mode_name];
            assert_eq!(expected_mode["outcome"], "OK", "{case_name} {mode_name}");
            let header =
                ChangeInvisibleTimeRequestHeader::decode_fields_with_mode(&fields, use_fast_encode)
                    .unwrap_or_else(|error| panic!("{case_name} {mode_name}: {error}"));
            let mut encoded = HeaderMap::new();
            header.encode_fields(&mut encoded);
            let expected: HeaderMap =
                serde_json::from_value(expected_mode["encodedFields"].clone()).unwrap();
            assert_eq!(encoded, expected, "{case_name} {mode_name}");
            assert_eq!(
                serde_json::json!({
                    "consumerGroup": header.get_consumer_group(),
                    "topic": header.get_topic(),
                    "queueId": header.get_queue_id(),
                    "extraInfo": header.get_extra_info(),
                    "offset": header.get_offset(),
                    "invisibleTime": header.get_invisible_time(),
                    "liteTopic": header.get_lite_topic(),
                    "suspend": header.is_suspend(),
                }),
                expected_mode["values"],
                "{case_name} {mode_name} values"
            );
        }
    }
}

fn encode_decoded_header<T: CommandHeader>(
    fields: &HeaderMap,
    use_fast_encode: bool,
) -> Result<HeaderMap, rocketmq_classic_protocol::header::ProtocolError> {
    let decoded = T::decode_fields_with_mode(fields, use_fast_encode)?;
    let mut encoded = HeaderMap::new();
    decoded.encode_fields(&mut encoded);
    Ok(encoded)
}

#[test]
fn transaction_and_reply_headers_round_trip_typed_fields() {
    let mut end = EndTransactionRequestHeader::default();
    end.set_topic(Some("orders".into()));
    end.set_producer_group(Some("producers".into()));
    end.set_tran_state_table_offset(Some(3));
    end.set_commit_log_offset(Some(9));
    end.set_commit_or_rollback(Some(8));
    end.set_from_transaction_check(Some(false));
    end.set_msg_id(Some("msg-1".into()));
    end.set_transaction_id(Some("tx-1".into()));
    let mut fields = HeaderMap::new();
    end.encode_fields(&mut fields);
    assert_eq!(EndTransactionRequestHeader::decode_fields(&fields), Ok(end));

    let mut null_group_end = EndTransactionRequestHeader::default();
    null_group_end.set_commit_or_rollback(Some(8));
    fields.clear();
    null_group_end.encode_fields(&mut fields);
    assert!(!fields.contains_key("producerGroup"));
    assert_eq!(
        EndTransactionRequestHeader::decode_fields(&fields),
        Ok(null_group_end)
    );

    let mut check = CheckTransactionStateRequestHeader::default();
    check.set_topic(Some("orders".into()));
    check.set_tran_state_table_offset(3);
    check.set_commit_log_offset(9);
    check.set_msg_id(Some("msg-1".into()));
    check.set_transaction_id(Some("tx-1".into()));
    check.set_offset_msg_id(Some("offset-id".into()));
    fields.clear();
    check.encode_fields(&mut fields);
    assert_eq!(
        CheckTransactionStateRequestHeader::decode_fields(&fields),
        Ok(check)
    );

    let mut response = CheckTransactionStateResponseHeader::default();
    response.set_producer_group(Some("producers".into()));
    response.set_tran_state_table_offset(Some(3));
    response.set_commit_log_offset(Some(9));
    response.set_commit_or_rollback(Some(8));
    fields.clear();
    response.encode_fields(&mut fields);
    let decoded_response = CheckTransactionStateResponseHeader::decode_fields(&fields).unwrap();
    let mut decoded_response_fields = HeaderMap::new();
    decoded_response.encode_fields(&mut decoded_response_fields);
    assert_eq!(decoded_response_fields, fields);

    let mut reply = ReplyMessageRequestHeader::default();
    reply.set_producer_group("producers".into());
    reply.set_topic("orders".into());
    reply.set_default_topic("TBW102".into());
    reply.set_default_topic_queue_nums(4);
    reply.set_queue_id(2);
    reply.set_sys_flag(1);
    reply.set_born_timestamp(5);
    reply.set_flag(7);
    reply.set_properties(None);
    reply.set_reconsume_times(None);
    reply.set_unit_mode(false);
    reply.set_born_host("127.0.0.1:1".into());
    reply.set_store_host("127.0.0.1:2".into());
    reply.set_store_timestamp(6);
    fields.clear();
    reply.encode_fields(&mut fields);
    assert_eq!(ReplyMessageRequestHeader::decode_fields(&fields), Ok(reply));
}

#[test]
fn transaction_headers_match_pinned_java_fields_including_rpc_parent() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/transaction.json")).unwrap();
    let transaction_flow = &fixture["transactionFlow"];
    let fields = |value: &serde_json::Value| -> HeaderMap {
        value
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
            .collect()
    };

    for value in transaction_flow["initialEndTransaction"]
        .as_object()
        .unwrap()
        .values()
    {
        let expected = fields(value);
        let header = EndTransactionRequestHeader::decode_fields(&expected).unwrap();
        let mut actual = HeaderMap::new();
        header.encode_fields(&mut actual);
        assert_eq!(actual, expected);
        assert_eq!(header.get_broker_name(), Some("broker-a"));
    }

    let expected_check = fields(&transaction_flow["brokerCheckRequest"]);
    let check = CheckTransactionStateRequestHeader::decode_fields(&expected_check).unwrap();
    let mut actual_check = HeaderMap::new();
    check.encode_fields(&mut actual_check);
    assert_eq!(actual_check, expected_check);
    assert_eq!(check.get_namespace(), Some("fixture-namespace"));
    assert_eq!(check.get_namespaced(), Some(true));
    assert_eq!(check.get_broker_name(), Some("broker-a"));
    assert_eq!(check.get_oneway(), Some(false));

    let mut default_end = HeaderMap::new();
    EndTransactionRequestHeader::default().encode_fields(&mut default_end);
    assert_eq!(
        default_end.get("fromTransactionCheck").map(String::as_str),
        Some("false")
    );
}

#[test]
fn transaction_end_response_and_reply_property_names_are_java_constants() {
    use rocketmq_classic_common::message::constants::*;
    let mut fields = HeaderMap::from([("unexpected".into(), "value".into())]);
    rocketmq_classic_protocol::header::EndTransactionResponseHeader.encode_fields(&mut fields);
    assert_eq!(fields.get("unexpected").map(String::as_str), Some("value"));
    assert_eq!(PROPERTY_CORRELATION_ID, "CORRELATION_ID");
    assert_eq!(PROPERTY_MESSAGE_REPLY_TO_CLIENT, "REPLY_TO_CLIENT");
    assert_eq!(PROPERTY_MESSAGE_TTL, "TTL");
    assert_eq!(PROPERTY_REPLY_MESSAGE_ARRIVE_TIME, "ARRIVE_TIME");
    assert_eq!(PROPERTY_PUSH_REPLY_TIME, "PUSH_REPLY_TIME");
}

#[test]
fn java_json_body_fixtures_decode_to_typed_assignment_lite_and_batch_models() {
    let assignment: QueryAssignmentRequestBody =
        serde_json::from_slice(&java_body("body-query-assignment")).unwrap();
    assert_eq!(assignment.get_topic(), Some("orders"));
    assert_eq!(
        assignment.get_strategy_name(),
        Some("AllocateMessageQueueAveragely")
    );

    let mode: SetMessageRequestModeRequestBody =
        serde_json::from_slice(&java_body("body-set-request-mode")).unwrap();
    assert_eq!(mode.get_mode(), Some(MessageRequestMode::Pop));
    assert_eq!(mode.get_pop_share_queue_num(), 2);

    let lite: LiteSubscriptionCtlRequestBody =
        serde_json::from_slice(&java_body("body-lite-subscription")).unwrap();
    let dto = lite.subscription_set.as_ref().unwrap().with(|items| {
        items
            .iter()
            .next()
            .and_then(Option::as_ref)
            .expect("Java fixture subscription entries are non-null")
            .clone()
    });
    assert_eq!(
        dto.with(|dto| dto.action),
        Some(LiteSubscriptionAction::CompleteAdd)
    );
    assert_eq!(
        dto.with(|dto| dto
            .offset_option
            .as_ref()
            .unwrap()
            .with(|option| option.value)),
        1
    );

    let batch: BatchAckMessageRequestBody =
        serde_json::from_slice(&java_body("body-batch-ack")).unwrap();
    assert_eq!(batch.get_broker_name(), Some("broker-a"));
    assert_eq!(
        batch.get_acks().unwrap().snapshot()[0]
            .as_ref()
            .unwrap()
            .with(|ack| ack.get_bit_set().unwrap().snapshot()),
        BitSet::from_byte_array(&[0b0000_0101])
    );

    for name in ["body-reset-offset", "body-consumer-status"] {
        let raw = java_body(name);
        if name == "body-reset-offset" {
            let typed =
                ResetOffsetBody::decode_java_json(std::str::from_utf8(&raw).unwrap()).unwrap();
            let value = serde_json::to_value(&typed).unwrap();
            assert_eq!(
                value["offsetTable"]
                    .as_object()
                    .unwrap()
                    .values()
                    .next()
                    .unwrap(),
                9
            );
            let offset_table = typed.get_offset_table().unwrap().snapshot();
            let (queue, offset) = &offset_table[0];
            assert_eq!(queue.topic(), Some("orders"));
            assert_eq!(queue.broker_name(), Some("broker-a"));
            assert_eq!(*offset, Some(9));
            assert_eq!(typed.encode_java_json().unwrap(), raw);
        } else {
            let typed = GetConsumerStatusBody::decode_java_json(std::str::from_utf8(&raw).unwrap())
                .unwrap();
            let value = serde_json::to_value(&typed).unwrap();
            assert_eq!(
                value["messageQueueTable"]
                    .as_object()
                    .unwrap()
                    .values()
                    .next()
                    .unwrap(),
                9
            );
            let message_queue_table = typed.get_message_queue_table().unwrap().snapshot();
            let (queue, offset) = &message_queue_table[0];
            assert_eq!(queue.topic(), Some("orders"));
            assert_eq!(queue.broker_name(), Some("broker-a"));
            assert_eq!(*offset, Some(9));
            assert_eq!(typed.encode_java_json().unwrap(), raw);
        }
    }

    let direct: ConsumeMessageDirectlyResult =
        serde_json::from_slice(&java_body("body-consume-directly")).unwrap();
    assert_eq!(
        direct.get_consume_result(),
        Some(ConsumeMessageResult::CrSuccess)
    );
    assert_eq!(
        serde_json::to_value(&direct).unwrap(),
        serde_json::from_slice::<serde_json::Value>(&java_body("body-consume-directly")).unwrap()
    );
}

#[test]
fn consumer_running_info_body_matches_pinned_java_5_5_1_json() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let fixture: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(format!("{root}/fixtures/consumer/running-info.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        fixture["client"],
        "org.apache.rocketmq:rocketmq-client:5.5.1"
    );
    assert_eq!(
        fixture["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );
    assert_eq!(fixture["serverContacted"], false);
    for (name, value) in [
        (
            "PROP_NAMESERVER_ADDR",
            ConsumerRunningInfoBody::PROP_NAMESERVER_ADDR,
        ),
        (
            "PROP_THREADPOOL_CORE_SIZE",
            ConsumerRunningInfoBody::PROP_THREADPOOL_CORE_SIZE,
        ),
        (
            "PROP_CONSUME_ORDERLY",
            ConsumerRunningInfoBody::PROP_CONSUME_ORDERLY,
        ),
        (
            "PROP_CONSUME_TYPE",
            ConsumerRunningInfoBody::PROP_CONSUME_TYPE,
        ),
        (
            "PROP_CLIENT_VERSION",
            ConsumerRunningInfoBody::PROP_CLIENT_VERSION,
        ),
        (
            "PROP_CONSUMER_START_TIMESTAMP",
            ConsumerRunningInfoBody::PROP_CONSUMER_START_TIMESTAMP,
        ),
    ] {
        assert_eq!(
            fixture["propertyKeys"][name].as_str(),
            Some(value),
            "{name}"
        );
    }
    let api_inventories = &fixture["apiInventories"];
    assert_eq!(
        api_inventories["ConsumerRunningInfo"]["publicFields"],
        serde_json::json!([
            "PROP_CLIENT_VERSION",
            "PROP_CONSUMER_START_TIMESTAMP",
            "PROP_CONSUME_ORDERLY",
            "PROP_CONSUME_TYPE",
            "PROP_NAMESERVER_ADDR",
            "PROP_THREADPOOL_CORE_SIZE"
        ])
    );
    for class in ["ProcessQueueInfo", "PopProcessQueueInfo", "ConsumeStatus"] {
        assert_eq!(
            api_inventories[class]["publicFields"],
            serde_json::json!([]),
            "{class} must expose no public instance fields"
        );
    }
    let identity = &fixture["identitySemantics"];
    for class in [
        "ConsumerRunningInfo",
        "ProcessQueueInfo",
        "PopProcessQueueInfo",
        "ConsumeStatus",
    ] {
        assert_eq!(
            identity[class]["equalsDeclaredBy"], "java.lang.Object",
            "{class}"
        );
        assert_eq!(
            identity[class]["hashCodeDeclaredBy"], "java.lang.Object",
            "{class}"
        );
        assert_eq!(identity[class]["sameObjectEqualsItself"], true, "{class}");
        assert_eq!(
            identity[class]["distinctDefaultObjectsEquals"], false,
            "{class}"
        );
    }

    let queue = MessageQueue::new("orders", "broker-a", 1);
    let pop_queue = MessageQueue::new("orders", "broker-a", 2);
    let mut subscription = SubscriptionData::new(Some("orders".into()), Some("*".into()));
    subscription.sub_version = 1_700_000_000_001;
    subscription.tags_set = Some(SharedValue::new(std::collections::HashSet::from([
        Some("blue".to_owned()),
        Some("green".to_owned()),
    ])));
    let mut properties = std::collections::BTreeMap::new();
    properties.insert(
        ConsumerRunningInfoBody::PROP_NAMESERVER_ADDR.into(),
        "127.0.0.1:9876;".into(),
    );
    properties.insert(
        ConsumerRunningInfoBody::PROP_CONSUME_ORDERLY.into(),
        "true".into(),
    );
    properties.insert(
        ConsumerRunningInfoBody::PROP_THREADPOOL_CORE_SIZE.into(),
        "7".into(),
    );
    properties.insert(
        ConsumerRunningInfoBody::PROP_CONSUME_TYPE.into(),
        "CONSUME_PASSIVELY".into(),
    );
    properties.insert(
        ConsumerRunningInfoBody::PROP_CLIENT_VERSION.into(),
        "V5_5_1".into(),
    );
    properties.insert(
        ConsumerRunningInfoBody::PROP_CONSUMER_START_TIMESTAMP.into(),
        "1700000000000".into(),
    );
    let mut process_queue = ConsumerProcessQueueInfo::default();
    process_queue.set_commit_offset(17);
    process_queue.set_cached_msg_min_offset(11);
    process_queue.set_cached_msg_max_offset(20);
    process_queue.set_cached_msg_count(3);
    process_queue.set_cached_msg_size_in_mib(2);
    process_queue.set_transaction_msg_min_offset(2);
    process_queue.set_transaction_msg_max_offset(3);
    process_queue.set_transaction_msg_count(1);
    process_queue.set_locked(true);
    process_queue.set_try_unlock_times(4);
    process_queue.set_last_lock_timestamp(1_700_000_000_002);
    process_queue.set_droped(false);
    process_queue.set_last_pull_timestamp(1_700_000_000_003);
    process_queue.set_last_consume_timestamp(1_700_000_000_004);
    assert_eq!(
        process_queue.to_string(),
        fixture["toStrings"]["ProcessQueueInfo"]
    );
    let mut pop_process_queue = ConsumerPopProcessQueueInfo::default();
    pop_process_queue.set_wait_ack_count(2);
    pop_process_queue.set_droped(false);
    pop_process_queue.set_last_pop_timestamp(1_700_000_000_005);
    assert_eq!(
        pop_process_queue.to_string(),
        fixture["toStrings"]["PopProcessQueueInfo"]
    );
    let mut status = ConsumerConsumeStatus::default();
    status.set_pull_rt(0.5);
    status.set_pull_tps(2.5);
    status.set_consume_rt(3.5);
    status.set_consume_ok_tps(4.5);
    status.set_consume_failed_tps(0.25);
    status.set_consume_failed_msgs(6);
    let mut body = ConsumerRunningInfoBody::default();
    body.set_properties(Some(SharedValue::new(properties)));
    body.set_subscription_set(Some(SharedValue::new(vec![subscription])));
    body.set_mq_table(Some(SharedValue::new(vec![(queue, process_queue)])));
    body.set_mq_pop_table(Some(SharedValue::new(vec![(pop_queue, pop_process_queue)])));
    body.set_status_table(Some(SharedValue::new(std::collections::BTreeMap::from([
        ("orders".into(), status),
    ]))));
    body.get_user_consumer_info()
        .unwrap()
        .with_mut(|values| values.insert("fixture".into(), "value".into()));
    body.set_jstack(Some("jstack-fixture".into()));
    let encoded = body.encode_java_json().unwrap();
    let mut expected =
        ConsumerRunningInfoBody::decode_java_json(fixture["body"].as_str().unwrap()).unwrap();
    let mut actual =
        ConsumerRunningInfoBody::decode_java_json(std::str::from_utf8(&encoded).unwrap()).unwrap();
    sort_java_hash_set_fields(&mut expected);
    sort_java_hash_set_fields(&mut actual);
    assert_eq!(
        actual, expected,
        "only Java HashSet iteration order is normalized"
    );

    let running_info =
        ConsumerRunningInfoBody::decode_java_json_body(fixture["body"].as_str().unwrap()).unwrap();
    let properties = running_info.get_properties().unwrap().snapshot();
    assert_eq!(
        properties[ConsumerRunningInfoBody::PROP_CONSUME_ORDERLY],
        "true"
    );
    assert_eq!(
        properties[ConsumerRunningInfoBody::PROP_THREADPOOL_CORE_SIZE],
        "7"
    );
    assert_eq!(
        properties.get("PROP_CONSUME_TYPE").map(String::as_str),
        Some("CONSUME_PASSIVELY")
    );
    let subscriptions = running_info.get_subscription_set().unwrap().snapshot();
    let mq_table = running_info.get_mq_table().unwrap().snapshot();
    let mq_pop_table = running_info.get_mq_pop_table().unwrap().snapshot();
    let status_table = running_info.get_status_table().unwrap().snapshot();
    let user_consumer_info = running_info.get_user_consumer_info().unwrap().snapshot();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(mq_table.len(), 1);
    let (queue, process_queue) = &mq_table[0];
    assert_eq!(queue.topic(), Some("orders"));
    assert_eq!(queue.broker_name(), Some("broker-a"));
    assert_eq!(queue.queue_id(), 1);
    assert_eq!(process_queue.get_commit_offset(), 17);
    assert!(process_queue.is_locked());
    assert_eq!(mq_pop_table.len(), 1);
    assert_eq!(mq_pop_table[0].1.get_wait_ack_count(), 2);
    assert_eq!(status_table["orders"].get_consume_failed_msgs(), 6);
    assert_eq!(
        user_consumer_info.get("fixture").map(String::as_str),
        Some("value")
    );
    assert_eq!(running_info.get_jstack(), Some("jstack-fixture"));
}

fn sort_java_hash_set_fields(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            for (name, value) in fields {
                if matches!(name.as_str(), "tagsSet" | "codeSet") {
                    if let serde_json::Value::Array(values) = value {
                        values.sort_by_key(serde_json::Value::to_string);
                    }
                } else {
                    sort_java_hash_set_fields(value);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                sort_java_hash_set_fields(value);
            }
        }
        _ => {}
    }
}

#[test]
fn java_consumer_list_and_lock_bodies_preserve_optional_and_collection_semantics() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let read_body = |stem: &str| {
        let frame = std::fs::read(format!("{root}/fixtures/remoting/{stem}.bin")).unwrap();
        let marked = u32::from_be_bytes(frame[4..8].try_into().unwrap());
        let end = 8 + (marked & 0x00ff_ffff) as usize;
        frame[end..].to_vec()
    };
    let absent: ConsumerListByGroupResponseBody =
        serde_json::from_slice(&read_body("body-consumer-list-absent")).unwrap();
    let empty: ConsumerListByGroupResponseBody =
        serde_json::from_slice(&read_body("body-consumer-list-empty")).unwrap();
    let populated: ConsumerListByGroupResponseBody =
        serde_json::from_slice(&read_body("body-consumer-list-populated")).unwrap();
    assert!(absent.get_consumer_id_list().is_none());
    assert_eq!(
        empty.get_consumer_id_list().unwrap().snapshot(),
        Vec::<Option<String>>::new()
    );
    assert_eq!(
        populated.get_consumer_id_list().unwrap().snapshot(),
        vec![Some("client-a".into()), Some("client-b".into())]
    );

    let lock: LockBatchRequestBody =
        serde_json::from_slice(&read_body("body-lock-request")).unwrap();
    let unlock: UnlockBatchRequestBody =
        serde_json::from_slice(&read_body("body-unlock-request")).unwrap();
    let lock_mq_set = lock.get_mq_set().unwrap().with(|queues| {
        queues
            .iter()
            .map(|queue| queue.as_ref().map(|queue| queue.snapshot()))
            .collect::<std::collections::HashSet<_>>()
    });
    assert_eq!(
        lock_mq_set
            .iter()
            .next()
            .unwrap()
            .as_ref()
            .unwrap()
            .broker_name(),
        Some("broker-a")
    );
    assert!(!lock.is_only_this_broker());
    assert!(unlock.is_only_this_broker());
    let response: LockBatchResponseBody =
        serde_json::from_slice(&read_body("body-lock-response")).unwrap();
    let response_set = response.get_lock_okmq_set().unwrap().with(|queues| {
        queues
            .iter()
            .map(|queue| queue.as_ref().map(|queue| queue.snapshot()))
            .collect::<std::collections::HashSet<_>>()
    });
    assert_eq!(
        response_set
            .iter()
            .next()
            .unwrap()
            .as_ref()
            .unwrap()
            .queue_id(),
        1
    );
    let unlock_response: LockBatchResponseBody =
        serde_json::from_slice(&read_body("body-unlock-response")).unwrap();
    let unlock_response_set = unlock_response.get_lock_okmq_set().unwrap().with(|queues| {
        queues
            .iter()
            .map(|queue| queue.as_ref().map(|queue| queue.snapshot()))
            .collect::<std::collections::HashSet<_>>()
    });
    assert_eq!(
        unlock_response_set
            .iter()
            .next()
            .unwrap()
            .as_ref()
            .unwrap()
            .topic(),
        Some("orders")
    );
}

#[test]
fn java_v1_send_conversion_matches_v2_fixture() {
    let v1 = SendMessageRequestHeaderV1::decode_fields(&java_ext_fields("header-send-v1")).unwrap();
    let v2 = SendMessageRequestHeaderV2::decode_fields(&java_ext_fields("header-send-v2")).unwrap();
    assert_eq!(v2, SendMessageRequestHeaderV2::from_v1(&v1));
    assert_eq!(v2.is_k(), Some(false));
    assert_eq!(v2.is_m(), Some(false));
}

#[test]
fn java_frames_match_typed_header_semantics() {
    let route =
        GetRouteInfoRequestHeader::decode_fields(&json_header_map("header-route-request")).unwrap();
    assert_eq!(route.get_topic(), "orders");
    let v1 = SendMessageRequestHeaderV1::decode_fields(&json_header_map("header-send-v1")).unwrap();
    let v2 = SendMessageRequestHeaderV2::decode_fields(&json_header_map("header-send-v2")).unwrap();
    assert_eq!(v2, SendMessageRequestHeaderV2::from_v1(&v1));
    let response =
        SendMessageResponseHeader::decode_fields(&json_header_map("header-send-response")).unwrap();
    assert_eq!(response.get_queue_offset(), Some(99));
}

#[test]
fn typed_header_update_preserves_unknown_fields_and_lite_requireds_fail() {
    let mut raw = java_ext_fields("header-notify-unsubscribe-lite-request");
    raw.insert("futureField".into(), "opaque-value".into());
    let mut typed = NotifyUnsubscribeLiteRequestHeader::decode_fields(&raw).unwrap();
    typed.set_lite_topic("lite-orders-v2".into());
    typed.encode_fields(&mut raw);
    assert_eq!(
        raw.get("futureField").map(String::as_str),
        Some("opaque-value")
    );
    assert_eq!(
        raw.get("liteTopic").map(String::as_str),
        Some("lite-orders-v2")
    );

    for missing in ["liteTopic", "consumerGroup", "clientId"] {
        let mut fields = java_ext_fields("header-notify-unsubscribe-lite-request");
        fields.remove(missing);
        let error = NotifyUnsubscribeLiteRequestHeader::decode_fields(&fields)
            .unwrap_err()
            .to_string();
        assert!(error.contains(missing), "{error}");
    }
}

#[test]
#[allow(deprecated)]
fn query_consumer_offset_header_public_api_matches_java_fixture() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let fixture: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(format!(
            "{root}/fixtures/remoting/offset-header-decode-modes.json"
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        fixture["client"],
        "org.apache.rocketmq:rocketmq-client:5.5.1"
    );
    let query_api = &fixture["queryRequestApi"];
    let update_api = &fixture["updateRequestApi"];
    let response_api = &fixture["queryResponseApi"];
    for api in [query_api, update_api, response_api] {
        assert_eq!(api["publicFields"], serde_json::json!([]));
        assert_eq!(api["publicConstructors"].as_array().unwrap().len(), 1);
    }
    let query_methods = query_api["callablePublicMethods"].as_array().unwrap();
    for method in [
        "getConsumerGroup",
        "setConsumerGroup",
        "getTopic",
        "setTopic",
        "getQueueId",
        "setQueueId",
        "getSetZeroIfNotFound",
        "setSetZeroIfNotFound",
        "getBname",
        "setBname",
        "getBrokerName",
        "setBrokerName",
        "getNamespace",
        "setNamespace",
        "getNamespaced",
        "setNamespaced",
        "getLo",
        "setLo",
        "getOneway",
        "setOneway",
        "checkFields",
        "toString",
    ] {
        assert!(
            query_methods
                .iter()
                .any(|signature| signature.as_str().unwrap().contains(&format!(".{method}("))),
            "Java fixture does not declare {method}"
        );
    }
    let update_methods = update_api["callablePublicMethods"].as_array().unwrap();
    for method in ["getCommitOffset", "setCommitOffset", "getLo", "setLo"] {
        assert!(
            update_methods
                .iter()
                .any(|signature| signature.as_str().unwrap().contains(&format!(".{method}("))),
            "Java fixture does not declare {method}"
        );
    }
    let response_methods = response_api["callablePublicMethods"].as_array().unwrap();
    for method in ["getOffset", "setOffset", "checkFields"] {
        assert!(
            response_methods
                .iter()
                .any(|signature| signature.as_str().unwrap().contains(&format!(".{method}("))),
            "Java fixture does not declare {method}"
        );
    }

    let mut header = QueryConsumerOffsetRequestHeader::default();
    header.set_consumer_group(Some("workers".into()));
    header.set_topic(Some("orders".into()));
    header.set_queue_id(Some(2));
    header.set_set_zero_if_not_found(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_lo(Some(false));
    header.set_oneway(Some(false));
    assert_eq!(header.get_consumer_group(), Some("workers"));
    assert_eq!(header.get_topic(), Some("orders"));
    assert_eq!(header.get_queue_id(), Some(2));
    assert_eq!(header.get_set_zero_if_not_found(), Some(true));
    assert_eq!(header.get_broker_name(), Some("broker-a"));
    assert_eq!(header.get_bname(), Some("broker-a"));
    assert_eq!(header.get_namespace(), Some("tenant"));
    assert_eq!(header.get_namespaced(), Some(true));
    assert_eq!(header.get_lo(), Some(false));
    assert_eq!(header.get_oneway(), Some(false));
    assert_eq!(header.to_string(), fixture["queryRequestToString"]);

    let mut update = UpdateConsumerOffsetRequestHeader::default();
    update.set_consumer_group(Some("workers".into()));
    update.set_topic(Some("orders".into()));
    update.set_queue_id(Some(2));
    update.set_commit_offset(Some(9));
    update.set_broker_name(Some("broker-a".into()));
    update.set_namespace(Some("tenant".into()));
    update.set_namespaced(Some(true));
    update.set_lo(Some(false));
    update.set_oneway(Some(false));
    assert_eq!(update.get_consumer_group(), Some("workers"));
    assert_eq!(update.get_topic(), Some("orders"));
    assert_eq!(update.get_queue_id(), Some(2));
    assert_eq!(update.get_commit_offset(), Some(9));
    assert_eq!(update.get_broker_name(), Some("broker-a"));
    assert_eq!(update.get_bname(), Some("broker-a"));
    assert_eq!(update.get_namespace(), Some("tenant"));
    assert_eq!(update.get_namespaced(), Some(true));
    assert_eq!(update.get_lo(), Some(false));
    assert_eq!(update.get_oneway(), Some(false));
    assert_eq!(update.to_string(), fixture["updateRequestToString"]);

    let update_input = &fixture["cases"]["update-valid"]["fields"];
    let update_input: HeaderMap = serde_json::from_value(update_input.clone()).unwrap();
    let decoded_update =
        UpdateConsumerOffsetRequestHeader::decode_fields_with_mode(&update_input, false).unwrap();
    assert_eq!(decoded_update.get_lo(), Some(true));
    let mut encoded_update = HeaderMap::new();
    decoded_update.encode_fields(&mut encoded_update);
    assert_eq!(encoded_update.get("lo").map(String::as_str), Some("true"));

    let mut response = QueryConsumerOffsetResponseHeader::default();
    response.set_offset(Some(9));
    assert_eq!(response.get_offset(), Some(9));
}

#[test]
fn header_required_and_nullable_fields_follow_java_contracts() {
    let empty = HeaderMap::new();
    let err = GetRouteInfoRequestHeader::decode_fields(&empty)
        .unwrap_err()
        .to_string();
    assert!(err.contains("topic"));
    let err = QueryConsumerOffsetRequestHeader::decode_fields(&empty)
        .unwrap_err()
        .to_string();
    assert!(err.contains("consumerGroup"));
    let valid_prefix = HeaderMap::from([
        ("consumerGroup".into(), "g".into()),
        ("topic".into(), "t".into()),
        ("queueId".into(), "0".into()),
    ]);
    let err = UpdateConsumerOffsetRequestHeader::decode_fields(&valid_prefix)
        .unwrap_err()
        .to_string();
    assert!(err.contains("commitOffset"));
    let empty_send_v2 = SendMessageRequestHeaderV2::decode_fields(&empty).unwrap();
    assert_eq!(empty_send_v2.get_a(), None);
    assert_eq!(empty_send_v2.get_b(), None);
    assert_eq!(empty_send_v2.get_d(), None);
    let empty_send_response = SendMessageResponseHeader::decode_fields(&empty).unwrap();
    assert_eq!(empty_send_response.get_msg_id(), None);
    assert_eq!(empty_send_response.get_queue_id(), None);
    assert_eq!(empty_send_response.get_queue_offset(), None);
    let empty_pull_response = PullMessageResponseHeader::decode_fields(&empty).unwrap();
    assert_eq!(empty_pull_response.get_suggest_which_broker_id(), None);
    assert_eq!(empty_pull_response.get_next_begin_offset(), None);
    assert_eq!(empty_pull_response.get_min_offset(), None);
    assert_eq!(empty_pull_response.get_max_offset(), None);
    let err = QueryConsumerOffsetResponseHeader::decode_fields(&empty)
        .unwrap_err()
        .to_string();
    assert!(err.contains("offset"));
    let query_message = QueryMessageRequestHeader::decode_fields(&empty).unwrap();
    assert_eq!(query_message.get_topic(), None);
    assert_eq!(query_message.get_key(), None);
    assert_eq!(query_message.get_max_num(), None);
    assert_eq!(query_message.get_begin_timestamp(), None);
    assert_eq!(query_message.get_end_timestamp(), None);
    let query_message_response = QueryMessageResponseHeader::decode_fields(&empty).unwrap();
    assert_eq!(
        query_message_response.get_index_last_update_timestamp(),
        None
    );
    assert_eq!(
        query_message_response.get_index_last_update_phyoffset(),
        None
    );
    let view_message = ViewMessageRequestHeader::decode_fields(&empty).unwrap();
    assert_eq!(view_message.get_topic(), None);
    assert_eq!(view_message.get_offset(), None);
    let err = CreateTopicRequestHeader::decode_fields(&empty)
        .unwrap_err()
        .to_string();
    assert!(err.contains("topicFilterType"));

    let mut invalid_topic_filter = java_ext_fields("header-create-topic-request");
    invalid_topic_filter.insert("topicFilterType".into(), "NOT_A_FILTER".into());
    let err = CreateTopicRequestHeader::decode_fields(&invalid_topic_filter)
        .unwrap_err()
        .to_string();
    assert!(err.contains("topicFilterType"));

    let mut bad = HeaderMap::from([
        ("consumerGroup".into(), "g".into()),
        ("topic".into(), "t".into()),
        ("queueId".into(), "2147483648".into()),
    ]);
    let err = QueryConsumerOffsetRequestHeader::decode_fields(&bad)
        .unwrap_err()
        .to_string();
    assert!(err.contains("queueId"));
    bad.insert("queueId".into(), "0".into());
    bad.insert("setZeroIfNotFound".into(), "yes".into());
    let decoded = QueryConsumerOffsetRequestHeader::decode_fields(&bad).unwrap();
    assert_eq!(decoded.get_set_zero_if_not_found(), Some(false));

    let mut transaction = HeaderMap::from([
        ("producerGroup".into(), "g".into()),
        ("tranStateTableOffset".into(), "1".into()),
        ("commitLogOffset".into(), "2".into()),
        ("commitOrRollback".into(), "overflow".into()),
        ("msgId".into(), "m".into()),
    ]);
    let err = EndTransactionRequestHeader::decode_fields(&transaction)
        .unwrap_err()
        .to_string();
    assert!(err.contains("commitOrRollback"));
    transaction.insert("commitOrRollback".into(), "8".into());
    assert_eq!(
        EndTransactionRequestHeader::decode_fields(&transaction)
            .unwrap()
            .get_from_transaction_check(),
        Some(false)
    );

    let mut v1_fields = HeaderMap::from([
        ("producerGroup".into(), "p".into()),
        ("topic".into(), "t".into()),
        ("defaultTopic".into(), "d".into()),
        ("defaultTopicQueueNums".into(), "1".into()),
        ("queueId".into(), "0".into()),
        ("sysFlag".into(), "0".into()),
        ("bornTimestamp".into(), "0".into()),
        ("flag".into(), "0".into()),
    ]);
    assert_eq!(
        SendMessageRequestHeaderV1::decode_fields(&v1_fields)
            .unwrap()
            .is_unit_mode(),
        None
    );
    v1_fields.insert("unitMode".into(), "false".into());
    assert_eq!(
        SendMessageRequestHeaderV1::decode_fields(&v1_fields)
            .unwrap()
            .is_unit_mode(),
        Some(false)
    );
}

#[test]
fn header_validation_matrix() {
    use rocketmq_classic_protocol::header::ProtocolError;
    use std::collections::BTreeSet;

    struct Case {
        model: &'static str,
        fixture: &'static str,
        decode: fn(&HeaderMap) -> Result<HeaderMap, ProtocolError>,
        update: fn(&mut HeaderMap) -> Result<(), ProtocolError>,
        required: &'static str,
        i32_fields: &'static str,
        i64_fields: &'static str,
        optional: &'static str,
        booleans: &'static str,
    }
    macro_rules! row {
        ($model:ty, $fixture:literal, $required:literal, $i32:literal, $i64:literal, $optional:literal, $bools:literal) => {
            Case {
                model: stringify!($model),
                fixture: $fixture,
                decode: |map| {
                    let decoded = <$model>::decode_fields(map)?;
                    let mut encoded = HeaderMap::new();
                    decoded.encode_fields(&mut encoded);
                    Ok(encoded)
                },
                update: |map| {
                    let decoded = <$model>::decode_fields(map)?;
                    decoded.encode_fields(map);
                    Ok(())
                },
                required: $required,
                i32_fields: $i32,
                i64_fields: $i64,
                optional: $optional,
                booleans: $bools,
            }
        };
    }
    let mut cases = [
        row!(
            GetRouteInfoRequestHeader,
            "route-request",
            "topic",
            "",
            "",
            "acceptStandardJsonOnly ns nsd bname oway",
            "acceptStandardJsonOnly nsd oway"
        ),
        row!(
            CreateTopicRequestHeader,
            "create-topic-request",
            "topic defaultTopic readQueueNums writeQueueNums perm topicFilterType",
            "readQueueNums writeQueueNums perm topicSysFlag",
            "",
            "topicSysFlag attributes ns nsd bname oway lo",
            "order force nsd oway lo"
        ),
        row!(
            CreateUserRequestHeader,
            "create-user-request",
            "",
            "",
            "",
            "username",
            ""
        ),
        row!(
            CreateAclRequestHeader,
            "create-acl-request",
            "",
            "",
            "",
            "subject",
            ""
        ),
        row!(
            DeleteAclRequestHeader,
            "delete-acl-request",
            "",
            "",
            "",
            "subject policyType resource",
            ""
        ),
        row!(
            UpdateAclRequestHeader,
            "update-acl-request",
            "",
            "",
            "",
            "subject",
            ""
        ),
        row!(
            UpdateUserRequestHeader,
            "update-user-request",
            "",
            "",
            "",
            "username",
            ""
        ),
        row!(
            DeleteUserRequestHeader,
            "delete-user-request",
            "",
            "",
            "",
            "username",
            ""
        ),
        row!(
            GetUserRequestHeader,
            "get-user-request",
            "",
            "",
            "",
            "username",
            ""
        ),
        row!(
            GetAclRequestHeader,
            "get-acl-request",
            "",
            "",
            "",
            "subject",
            ""
        ),
        row!(
            ListAclsRequestHeader,
            "list-acls-request",
            "",
            "",
            "",
            "subjectFilter resourceFilter",
            ""
        ),
        row!(
            ListUsersRequestHeader,
            "list-users-request",
            "",
            "",
            "",
            "filter",
            ""
        ),
        row!(
            QueryMessageRequestHeader,
            "query-message-request",
            "",
            "maxNum",
            "beginTimestamp endTimestamp",
            "topic key indexType lastKey ns nsd bname oway lo",
            "nsd oway lo"
        ),
        row!(
            QueryMessageResponseHeader,
            "query-message-response",
            "",
            "",
            "indexLastUpdateTimestamp indexLastUpdatePhyoffset",
            "",
            ""
        ),
        row!(
            QueryTopicsByConsumerRequestHeader,
            "query-topics-by-consumer-request",
            "",
            "",
            "",
            "group ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            QuerySubscriptionByConsumerRequestHeader,
            "query-subscription-by-consumer-request",
            "",
            "",
            "",
            "group topic ns nsd bname oway lo",
            "nsd oway lo"
        ),
        row!(
            QueryTopicConsumeByWhoRequestHeader,
            "query-topic-consume-by-who-request",
            "",
            "",
            "",
            "topic ns nsd bname oway lo",
            "nsd oway lo"
        ),
        row!(
            QueryCorrectionOffsetRequestHeader,
            "query-correction-offset-request",
            "",
            "",
            "",
            "filterGroups compareGroup topic ns nsd bname oway lo",
            "nsd oway lo"
        ),
        row!(
            UpdateGroupForbiddenRequestHeader,
            "update-group-forbidden-request",
            "",
            "",
            "",
            "group topic readable ns nsd bname oway lo",
            "readable nsd oway lo"
        ),
        row!(
            GetTopicStatsInfoRequestHeader,
            "topic-stats-info-request",
            "",
            "",
            "",
            "topic ns nsd bname oway lo",
            "nsd oway lo"
        ),
        row!(
            GetTopicConfigRequestHeader,
            "topic-config-request",
            "",
            "",
            "",
            "topic ns nsd bname oway lo",
            "nsd oway lo"
        ),
        row!(
            GetAllTopicConfigRequestHeader,
            "get-all-topic-config-request",
            "",
            "topicSeq maxTopicNum",
            "",
            "dataVersion",
            ""
        ),
        row!(
            GetAllSubscriptionGroupRequestHeader,
            "get-all-subscription-group-request",
            "",
            "groupSeq maxGroupNum",
            "",
            "dataVersion",
            ""
        ),
        row!(
            GetAllSubscriptionGroupResponseHeader,
            "get-all-subscription-group-response",
            "",
            "totalGroupNum",
            "",
            "",
            ""
        ),
        row!(
            QueryConsumeQueueRequestHeader,
            "query-consume-queue-request",
            "",
            "queueId count",
            "index",
            "topic consumerGroup bname ns nsd lo oway",
            "nsd lo oway"
        ),
        row!(
            GetAllTopicConfigResponseHeader,
            "get-all-topic-config-response",
            "",
            "totalTopicNum",
            "",
            "",
            ""
        ),
        row!(
            GetTopicsByClusterRequestHeader,
            "topics-by-cluster-request",
            "",
            "",
            "",
            "cluster",
            ""
        ),
        row!(
            GetSubscriptionGroupConfigRequestHeader,
            "get-subscription-group-config-request",
            "",
            "",
            "",
            "group ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            DeleteTopicRequestHeader,
            "delete-topic-request",
            "",
            "",
            "",
            "topic ns nsd bname oway lo",
            "nsd oway lo"
        ),
        row!(
            DeleteTopicFromNamesrvRequestHeader,
            "delete-topic-from-namesrv-request",
            "",
            "",
            "",
            "topic clusterName ns nsd bname oway lo",
            "nsd oway lo"
        ),
        row!(
            DeleteKvConfigRequestHeader,
            "delete-kv-config-request",
            "",
            "",
            "",
            "namespace key",
            ""
        ),
        row!(
            GetKvConfigRequestHeader,
            "get-kv-config-request",
            "",
            "",
            "",
            "namespace key",
            ""
        ),
        row!(
            PutKvConfigRequestHeader,
            "put-kv-config-request",
            "",
            "",
            "",
            "namespace key value",
            ""
        ),
        row!(
            GetKvListByNamespaceRequestHeader,
            "get-kv-list-by-namespace-request",
            "",
            "",
            "",
            "namespace",
            ""
        ),
        row!(
            GetKvConfigResponseHeader,
            "get-kv-config-response",
            "",
            "",
            "",
            "value",
            ""
        ),
        row!(
            WipeWritePermOfBrokerRequestHeader,
            "wipe-write-perm-request",
            "",
            "",
            "",
            "brokerName",
            ""
        ),
        row!(
            AddWritePermOfBrokerRequestHeader,
            "add-write-perm-request",
            "",
            "",
            "",
            "brokerName",
            ""
        ),
        row!(
            AddBrokerRequestHeader,
            "add-broker-request",
            "",
            "",
            "",
            "configPath",
            ""
        ),
        row!(
            RemoveBrokerRequestHeader,
            "remove-broker-request",
            "",
            "",
            "brokerId",
            "brokerClusterName brokerName",
            ""
        ),
        row!(
            CloneGroupOffsetRequestHeader,
            "clone-group-offset-request",
            "",
            "",
            "",
            "srcGroup destGroup topic ns nsd bname oway",
            "offline nsd oway"
        ),
        row!(
            WipeWritePermOfBrokerResponseHeader,
            "wipe-write-perm-response",
            "",
            "wipeTopicCount",
            "",
            "",
            ""
        ),
        row!(
            AddWritePermOfBrokerResponseHeader,
            "add-write-perm-response",
            "",
            "addTopicCount",
            "",
            "",
            ""
        ),
        row!(
            DeleteSubscriptionGroupRequestHeader,
            "delete-subscription-group-request",
            "",
            "",
            "",
            "groupName ns nsd bname oway",
            "cleanOffset nsd oway"
        ),
        row!(
            ViewMessageRequestHeader,
            "view-message-request",
            "",
            "",
            "offset",
            "topic",
            ""
        ),
        row!(
            SendMessageRequestHeaderV1,
            "send-v1",
            "topic defaultTopic defaultTopicQueueNums queueId sysFlag bornTimestamp flag",
            "defaultTopicQueueNums queueId sysFlag flag reconsumeTimes maxReconsumeTimes",
            "bornTimestamp",
            "producerGroup properties reconsumeTimes unitMode maxReconsumeTimes batch ns nsd bname lo oway",
            "unitMode batch nsd lo oway"
        ),
        row!(
            SendMessageRequestHeaderV2,
            "send-v2",
            "",
            "d e f h j l",
            "g",
            "a b c i j k l m n ns nsd bname lo oway",
            "k m nsd lo oway"
        ),
        row!(
            PullMessageRequestHeader,
            "pull-request",
            "",
            "queueId maxMsgNums sysFlag maxMsgBytes requestSource",
            "queueOffset commitOffset suspendTimeoutMillis subVersion",
            "consumerGroup topic liteTopic subscription expressionType maxMsgBytes requestSource proxyFrowardClientId queueId queueOffset maxMsgNums sysFlag commitOffset suspendTimeoutMillis subVersion bname ns nsd lo oway",
            "nsd lo oway"
        ),
        row!(
            PullMessageResponseHeader,
            "pull-response",
            "",
            "topicSysFlag groupSysFlag forbiddenType",
            "suggestWhichBrokerId nextBeginOffset minOffset maxOffset offsetDelta",
            "suggestWhichBrokerId nextBeginOffset minOffset maxOffset offsetDelta topicSysFlag groupSysFlag forbiddenType",
            ""
        ),
        row!(
            QueryConsumerOffsetRequestHeader,
            "query-offset-request",
            "consumerGroup topic queueId",
            "queueId",
            "",
            "setZeroIfNotFound bname ns nsd lo oway",
            "setZeroIfNotFound nsd lo oway"
        ),
        row!(
            QueryConsumeTimeSpanRequestHeader,
            "query-consume-time-span-request",
            "",
            "",
            "",
            "topic group ns nsd bname oway lo",
            "nsd oway lo"
        ),
        row!(
            QueryConsumerOffsetResponseHeader,
            "query-offset-response",
            "offset",
            "",
            "offset",
            "",
            ""
        ),
        row!(
            SearchOffsetRequestHeader,
            "search-offset-request",
            "topic queueId timestamp",
            "queueId",
            "timestamp",
            "liteTopic boundaryType bname ns nsd lo oway",
            "nsd lo oway"
        ),
        row!(
            GetMaxOffsetRequestHeader,
            "max-offset-request",
            "topic queueId",
            "queueId",
            "",
            "bname committed ns nsd lo oway",
            "committed nsd lo oway"
        ),
        row!(
            GetMinOffsetRequestHeader,
            "min-offset-request",
            "topic queueId",
            "queueId",
            "",
            "bname ns nsd lo oway",
            "nsd lo oway"
        ),
        row!(
            GetEarliestMsgStoretimeRequestHeader,
            "earliest-msg-storetime-request",
            "topic queueId",
            "queueId",
            "",
            "bname ns nsd lo oway",
            "nsd lo oway"
        ),
        row!(
            SearchOffsetResponseHeader,
            "search-offset-response",
            "offset",
            "",
            "offset",
            "",
            ""
        ),
        row!(
            GetMaxOffsetResponseHeader,
            "max-offset-response",
            "offset",
            "",
            "offset",
            "",
            ""
        ),
        row!(
            GetMinOffsetResponseHeader,
            "min-offset-response",
            "offset",
            "",
            "offset",
            "",
            ""
        ),
        row!(
            GetEarliestMsgStoretimeResponseHeader,
            "earliest-msg-storetime-response",
            "timestamp",
            "",
            "timestamp",
            "",
            ""
        ),
        row!(
            UpdateConsumerOffsetRequestHeader,
            "update-offset-request",
            "consumerGroup topic queueId commitOffset",
            "queueId",
            "commitOffset",
            "bname ns nsd lo oway",
            "nsd lo oway"
        ),
        row!(
            SendMessageResponseHeader,
            "send-response",
            "msgId queueId queueOffset",
            "queueId",
            "queueOffset",
            "transactionId batchUniqId recallHandle",
            ""
        ),
        row!(
            UnregisterClientRequestHeader,
            "unregister-request",
            "clientID",
            "",
            "",
            "producerGroup consumerGroup ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            UnregisterClientResponseHeader,
            "unregister-response",
            "",
            "",
            "",
            "",
            ""
        ),
        row!(
            ConsumerSendMsgBackRequestHeader,
            "send-back-request",
            "group originTopic offset delayLevel",
            "delayLevel maxReconsumeTimes",
            "offset",
            "bname originMsgId maxReconsumeTimes ns nsd oway",
            "unitMode nsd oway"
        ),
        row!(
            EndTransactionRequestHeader,
            "end-transaction-request",
            "tranStateTableOffset commitLogOffset commitOrRollback msgId",
            "commitOrRollback",
            "tranStateTableOffset commitLogOffset",
            "producerGroup topic transactionId ns nsd bname oway",
            "fromTransactionCheck nsd oway"
        ),
        row!(
            EndTransactionResponseHeader,
            "end-transaction-response",
            "",
            "",
            "",
            "",
            ""
        ),
        row!(
            CheckTransactionStateRequestHeader,
            "check-transaction-request",
            "tranStateTableOffset commitLogOffset",
            "",
            "tranStateTableOffset commitLogOffset",
            "topic msgId transactionId offsetMsgId ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            CheckTransactionStateResponseHeader,
            "check-transaction-response",
            "producerGroup tranStateTableOffset commitLogOffset commitOrRollback",
            "commitOrRollback",
            "tranStateTableOffset commitLogOffset",
            "",
            ""
        ),
        row!(
            ReplyMessageRequestHeader,
            "reply-request",
            "producerGroup topic defaultTopic defaultTopicQueueNums queueId sysFlag bornTimestamp flag bornHost storeHost storeTimestamp",
            "defaultTopicQueueNums queueId sysFlag flag reconsumeTimes",
            "bornTimestamp storeTimestamp",
            "properties reconsumeTimes ns nsd bname oway",
            "unitMode nsd oway"
        ),
        row!(
            RecallMessageRequestHeader,
            "recall-message-request",
            "topic recallHandle",
            "",
            "",
            "producerGroup ns nsd bname oway lo",
            "nsd oway lo"
        ),
        row!(
            RecallMessageResponseHeader,
            "recall-message-response",
            "",
            "",
            "",
            "msgId",
            ""
        ),
        row!(
            NotificationRequestHeader,
            "notification-request",
            "consumerGroup topic queueId pollTime bornTime",
            "queueId",
            "pollTime bornTime",
            "order attemptId clientId expType exp ns nsd bname oway",
            "order isLiteConsumer nsd oway"
        ),
        row!(
            NotificationResponseHeader,
            "notification-response",
            "",
            "",
            "",
            "",
            "hasMsg pollingFull"
        ),
        row!(
            ResetOffsetRequestHeader,
            "reset-offset-request",
            "group topic timestamp",
            "queueId",
            "offset timestamp",
            "queueId offset ns nsd bname oway",
            "isForce nsd oway"
        ),
        row!(
            ResetMasterFlushOffsetHeader,
            "reset-master-flush-offset",
            "",
            "",
            "masterFlushOffset",
            "",
            ""
        ),
        row!(
            ExportRocksDBConfigToJsonRequestHeader,
            "export-rocksdb-config-request",
            "",
            "",
            "",
            "configType",
            ""
        ),
        row!(
            CheckRocksdbCqWriteProgressRequestHeader,
            "check-rocksdb-cq-write-progress-request",
            "",
            "",
            "checkStoreTime",
            "topic",
            ""
        ),
        row!(
            GetConsumerStatusRequestHeader,
            "consumer-status-request",
            "topic group",
            "",
            "",
            "clientAddr ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            GetConsumerConnectionListRequestHeader,
            "consumer-connection-list-request",
            "",
            "",
            "",
            "consumerGroup ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            GetConsumeStatsRequestHeader,
            "consume-stats-request",
            "",
            "",
            "",
            "consumerGroup topic topicList ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            GetConsumeStatsInBrokerRequestHeader,
            "consume-stats-in-broker-request",
            "",
            "",
            "",
            "",
            "isOrder"
        ),
        row!(
            ViewBrokerStatsDataRequestHeader,
            "view-broker-stats-data-request",
            "",
            "",
            "",
            "statsName statsKey",
            ""
        ),
        row!(
            GetConsumerRunningInfoRequestHeader,
            "consumer-running-info-request",
            "consumerGroup clientId",
            "",
            "",
            "ns nsd bname oway",
            "jstackEnable nsd oway"
        ),
        row!(
            GetConsumerListByGroupRequestHeader,
            "consumer-list-request",
            "consumerGroup",
            "",
            "",
            "ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            LockBatchMqRequestHeader,
            "lock-batch-request",
            "",
            "",
            "",
            "ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            UnlockBatchMqRequestHeader,
            "unlock-batch-request",
            "",
            "",
            "",
            "ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            PopMessageRequestHeader,
            "pop-request",
            "",
            "queueId maxMsgNums initMode",
            "invisibleTime pollTime bornTime",
            "consumerGroup topic expType exp order attemptId ns nsd bname oway",
            "order nsd oway"
        ),
        row!(
            PopMessageResponseHeader,
            "pop-response",
            "",
            "reviveQid",
            "popTime invisibleTime restNum",
            "startOffsetInfo msgOffsetInfo orderCountInfo",
            ""
        ),
        row!(
            PopLiteMessageRequestHeader,
            "pop-lite-request",
            "",
            "maxMsgNum",
            "invisibleTime pollTime bornTime",
            "clientId consumerGroup topic attemptId ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            PopLiteMessageResponseHeader,
            "pop-lite-response",
            "",
            "reviveQid",
            "popTime invisibleTime",
            "startOffsetInfo msgOffsetInfo orderCountInfo",
            ""
        ),
        row!(
            AckMessageRequestHeader,
            "ack-request",
            "consumerGroup topic queueId extraInfo offset",
            "queueId",
            "offset",
            "liteTopic ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            ChangeInvisibleTimeRequestHeader,
            "change-invisible-request",
            "",
            "queueId",
            "offset invisibleTime",
            "consumerGroup topic queueId extraInfo offset invisibleTime liteTopic ns nsd bname oway",
            "suspend nsd oway"
        ),
        row!(
            ChangeInvisibleTimeResponseHeader,
            "change-invisible-response",
            "",
            "reviveQid",
            "popTime invisibleTime",
            "",
            ""
        ),
        row!(
            PollingInfoRequestHeader,
            "polling-request",
            "consumerGroup topic queueId",
            "queueId",
            "",
            "ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            PollingInfoResponseHeader,
            "polling-response",
            "pollingNum",
            "pollingNum",
            "",
            "",
            ""
        ),
        row!(
            HeartbeatRequestHeader,
            "heartbeat-request",
            "",
            "",
            "",
            "ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            LiteSubscriptionCtlRequestHeader,
            "lite-subscription-request",
            "",
            "",
            "",
            "ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            GetLiteClientInfoRequestHeader,
            "lite-client-info-request",
            "",
            "maxCount",
            "",
            "parentTopic group clientId",
            ""
        ),
        row!(
            GetLiteGroupInfoRequestHeader,
            "lite-group-info-request",
            "",
            "topK",
            "",
            "group liteTopic",
            ""
        ),
        row!(
            GetLiteTopicInfoRequestHeader,
            "lite-topic-info-request",
            "",
            "",
            "",
            "parentTopic liteTopic",
            ""
        ),
        row!(
            GetParentTopicInfoRequestHeader,
            "parent-topic-info-request",
            "",
            "",
            "",
            "topic",
            ""
        ),
        row!(
            NotifyUnsubscribeLiteRequestHeader,
            "notify-unsubscribe-lite-request",
            "liteTopic consumerGroup clientId",
            "",
            "",
            "ns nsd bname oway",
            "nsd oway"
        ),
        row!(
            ConsumeMessageDirectlyResultRequestHeader,
            "consume-directly-request",
            "consumerGroup",
            "topicSysFlag groupSysFlag",
            "",
            "clientId msgId brokerName topic topicSysFlag groupSysFlag ns nsd bname oway lo",
            "nsd oway lo"
        ),
    ];
    let send_v2 = cases
        .iter_mut()
        .find(|case| case.model == "SendMessageRequestHeaderV2")
        .unwrap();
    send_v2.decode = |map| {
        let decoded = SendMessageRequestHeaderV2::decode_fields_with_mode(map, false)?;
        let mut encoded = HeaderMap::new();
        decoded.encode_fields(&mut encoded);
        Ok(encoded)
    };
    send_v2.update = |map| {
        let decoded = SendMessageRequestHeaderV2::decode_fields_with_mode(map, false)?;
        decoded.encode_fields(map);
        Ok(())
    };
    // Keep the matrix tied to the public implementations, not just to fixture names.
    let source = include_str!("../src/header/basic.rs");
    let implementations: BTreeSet<_> = source
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("impl CommandHeader for ")
                .and_then(|name| name.strip_suffix(" {"))
        })
        .collect();
    let matrix: BTreeSet<_> = cases.iter().map(|case| case.model).collect();
    assert_eq!(matrix.len(), cases.len(), "duplicate validation row");
    let expected = implementations;
    assert!(source.contains("pub struct UnlockBatchMqRequestHeader"));
    assert_eq!(
        matrix, expected,
        "update the matrix for every public header"
    );

    for case in cases {
        let baseline = java_ext_fields(&format!("header-{}", case.fixture));
        (case.decode)(&baseline).unwrap_or_else(|e| panic!("{}: {e}", case.fixture));
        let required: BTreeSet<_> = case.required.split_whitespace().collect();
        let i32_fields: BTreeSet<_> = case.i32_fields.split_whitespace().collect();
        let i64_fields: BTreeSet<_> = case.i64_fields.split_whitespace().collect();
        let optional: BTreeSet<_> = case.optional.split_whitespace().collect();
        let booleans: BTreeSet<_> = case.booleans.split_whitespace().collect();
        assert!(
            i32_fields.is_disjoint(&i64_fields),
            "{} numeric width overlap",
            case.model
        );
        assert!(
            required.is_disjoint(&optional),
            "{} required/optional overlap",
            case.model
        );
        let mut modeled = required.clone();
        modeled.extend(optional.iter().copied());
        modeled.extend(booleans.iter().copied());
        modeled.extend(i32_fields.iter().copied());
        modeled.extend(i64_fields.iter().copied());
        let implementation = case.model;
        let encode_source = source
            .split_once(&format!("impl CommandHeader for {implementation} {{"))
            .unwrap()
            .1
            .split_once("fn decode_fields")
            .unwrap()
            .0;
        let encoded_keys: BTreeSet<_> = ["fields.insert(", "put_optional("]
            .into_iter()
            .flat_map(|call| encode_source.match_indices(call))
            .map(|(index, call)| {
                let rest = &encode_source[index + call.len()..];
                rest.split_once('"').unwrap().1.split_once('"').unwrap().0
            })
            .collect();
        assert_eq!(
            modeled, encoded_keys,
            "{} matrix must classify every modeled wire field",
            case.model
        );
        let mut raw = baseline.clone();
        for &name in &optional {
            let value = if name == "boundaryType" {
                "LOWER"
            } else if i32_fields.contains(name) || i64_fields.contains(name) {
                "0"
            } else if booleans.contains(name) {
                "false"
            } else {
                ""
            };
            raw.insert(name.into(), value.into());
        }
        for &name in &booleans {
            raw.insert(name.into(), "false".into());
        }
        raw.insert("futureField".into(), "opaque-value".into());
        let encoded = (case.decode)(&raw).unwrap();
        assert_eq!(
            encoded.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            modeled,
            "{} incomplete modeled field coverage",
            case.model
        );
        (case.update)(&mut raw).unwrap();
        assert_eq!(
            raw.get("futureField").map(String::as_str),
            Some("opaque-value")
        );
        for name in &modeled {
            assert_eq!(
                raw.get(*name),
                encoded.get(*name),
                "{} updated {name}",
                case.model
            );
        }
        for name in case.required.split_whitespace() {
            let mut fields = baseline.clone();
            fields.remove(name);
            let decoded = (case.decode)(&fields);
            if (case.model == "CreateTopicRequestHeader" && name == "topicFilterType")
                || (case.model == "EndTransactionRequestHeader" && name == "commitOrRollback")
            {
                assert!(
                    matches!(decoded, Err(ProtocolError::InvalidField { field, .. }) if field == name),
                    "{} {name}",
                    case.fixture
                );
            } else if case.model == "EndTransactionRequestHeader" {
                assert!(decoded.is_ok(), "{} {name}", case.fixture);
                assert!(
                    !decoded.unwrap().contains_key(name),
                    "{} encoded missing Java reflection field {name}",
                    case.fixture
                );
            } else if case.model == "SendMessageResponseHeader"
                || (case.model == "CreateTopicRequestHeader" && name != "topicFilterType")
            {
                assert!(decoded.is_ok(), "{} {name}", case.fixture);
                assert!(
                    !decoded.unwrap().contains_key(name),
                    "{} encoded missing Java nullable field {name}",
                    case.fixture
                );
            } else {
                assert_eq!(
                    decoded,
                    Err(ProtocolError::MissingField(name)),
                    "{} {name}",
                    case.fixture
                );
            }
            fields.insert(name.into(), "".into());
            let empty_value_is_accepted = if case.model == "EndTransactionRequestHeader" {
                name != "commitOrRollback"
            } else if case.model == "CreateTopicRequestHeader" {
                name != "topicFilterType"
            } else if matches!(
                case.model,
                "PopMessageResponseHeader"
                    | "PopLiteMessageResponseHeader"
                    | "GetAllSubscriptionGroupRequestHeader"
                    | "GetAllSubscriptionGroupResponseHeader"
            ) {
                true
            } else {
                !i32_fields.contains(name) && !i64_fields.contains(name)
            };
            assert_eq!(
                (case.decode)(&fields).is_ok(),
                empty_value_is_accepted,
                "{} empty required {name}",
                case.fixture
            );
        }
        for (width, names) in [(32, case.i32_fields), (64, case.i64_fields)] {
            for name in names.split_whitespace() {
                for value in [
                    "",
                    "not-a-number",
                    if width == 32 {
                        "2147483648"
                    } else {
                        "9223372036854775808"
                    },
                    if width == 32 {
                        "-2147483649"
                    } else {
                        "-9223372036854775809"
                    },
                ] {
                    let mut fields = baseline.clone();
                    fields.insert(name.into(), value.into());
                    if matches!(
                        case.model,
                        "ChangeInvisibleTimeRequestHeader"
                            | "SendMessageRequestHeaderV2"
                            | "QueryMessageRequestHeader"
                            | "QueryMessageResponseHeader"
                            | "ViewMessageRequestHeader"
                            | "ResetMasterFlushOffsetHeader"
                            | "WipeWritePermOfBrokerResponseHeader"
                            | "AddWritePermOfBrokerResponseHeader"
                            | "GetAllSubscriptionGroupRequestHeader"
                            | "GetAllSubscriptionGroupResponseHeader"
                    ) {
                        let decoded = (case.decode)(&fields)
                            .unwrap_or_else(|e| panic!("{} {name}={value}: {e}", case.fixture));
                        assert!(
                            !decoded.contains_key(name),
                            "{} reflection decoding should ignore invalid {name}={value}",
                            case.fixture
                        );
                    } else if matches!(
                        case.model,
                        "ChangeInvisibleTimeResponseHeader"
                            | "PopMessageRequestHeader"
                            | "PopLiteMessageRequestHeader"
                            | "PopMessageResponseHeader"
                            | "PopLiteMessageResponseHeader"
                            | "CheckRocksdbCqWriteProgressRequestHeader"
                            | "QueryConsumeQueueRequestHeader"
                    ) {
                        let decoded = (case.decode)(&fields)
                            .unwrap_or_else(|e| panic!("{} {name}={value}: {e}", case.fixture));
                        assert_eq!(
                            decoded.get(name).map(String::as_str),
                            Some("0"),
                            "{} primitive reflection decoding should retain zero for invalid {name}={value}",
                            case.fixture
                        );
                    } else if case.model == "CreateTopicRequestHeader" {
                        let decoded = (case.decode)(&fields)
                            .unwrap_or_else(|e| panic!("{} {name}={value}: {e}", case.fixture));
                        assert!(
                            !decoded.contains_key(name),
                            "{} reflective decoding should ignore invalid {name}={value}",
                            case.fixture
                        );
                    } else if case.model == "EndTransactionRequestHeader"
                        && name != "commitOrRollback"
                    {
                        let decoded = (case.decode)(&fields)
                            .unwrap_or_else(|e| panic!("{} {name}={value}: {e}", case.fixture));
                        assert!(
                            !decoded.contains_key(name),
                            "{} reflective decoding should ignore invalid {name}={value}",
                            case.fixture
                        );
                    } else {
                        assert!(
                            matches!((case.decode)(&fields), Err(ProtocolError::InvalidField { field, .. }) if field == name),
                            "{} {name}={value}",
                            case.fixture
                        );
                    }
                }
                for value in if width == 32 {
                    ["2147483647", "-2147483648"]
                } else {
                    ["9223372036854775807", "-9223372036854775808"]
                } {
                    let mut fields = baseline.clone();
                    fields.insert(name.into(), value.into());
                    if case.model == "EndTransactionRequestHeader"
                        && name == "commitOrRollback"
                        && !["0", "8", "12"].contains(&value)
                    {
                        assert!(
                            matches!((case.decode)(&fields), Err(ProtocolError::InvalidField { field, .. }) if field == name),
                            "{} {name}={value} is outside the Java commit-state values",
                            case.fixture
                        );
                        continue;
                    }
                    let decoded = (case.decode)(&fields)
                        .unwrap_or_else(|e| panic!("{} {name}={value}: {e}", case.fixture));
                    assert_eq!(
                        decoded.get(name).map(String::as_str),
                        Some(value),
                        "{} {name}={value}",
                        case.fixture
                    );
                }
                let mut fields = baseline.clone();
                fields.insert(name.into(), "0".into());
                assert_eq!(
                    (case.decode)(&fields)
                        .unwrap()
                        .get(name)
                        .map(String::as_str),
                    Some("0"),
                    "{} {name}=0",
                    case.fixture
                );
            }
        }
        for name in case.optional.split_whitespace() {
            let mut fields = baseline.clone();
            fields.remove(name);
            assert!(
                (case.decode)(&fields).is_ok(),
                "{} absent {name}",
                case.fixture
            );
            let absent = (case.decode)(&fields).unwrap();
            let values: &[&str] = if name == "boundaryType" {
                &["LOWER", "UPPER"]
            } else if name == "committed" {
                &["false"]
            } else if i32_fields.contains(name) || i64_fields.contains(name) {
                &["0"]
            } else if booleans.contains(name) {
                &["false", "true", ""]
            } else {
                &[""]
            };
            for &value in values {
                let mut present = fields.clone();
                present.insert(name.into(), value.into());
                let decoded = (case.decode)(&present)
                    .unwrap_or_else(|e| panic!("{} {name}={value}: {e}", case.fixture));
                if case.model == "PopMessageRequestHeader"
                    && name == "order"
                    && matches!(value, "false" | "")
                {
                    assert_eq!(decoded, absent, "{} absent vs {name}={value}", case.fixture);
                } else {
                    assert_ne!(decoded, absent, "{} absent vs {name}={value}", case.fixture);
                }
                assert_eq!(
                    decoded.get(name).map(String::as_str),
                    Some(if booleans.contains(name) && value.is_empty() {
                        "false"
                    } else {
                        value
                    }),
                    "{} {name}={value}",
                    case.fixture
                );
            }
        }
        for name in case.booleans.split_whitespace() {
            let mut absent = baseline.clone();
            absent.remove(name);
            let mut explicit_false = absent.clone();
            explicit_false.insert(name.into(), "false".into());
            let absent_decoded = (case.decode)(&absent).unwrap();
            let false_decoded = (case.decode)(&explicit_false).unwrap();
            if optional.contains(name)
                && !(case.model == "PopMessageRequestHeader" && name == "order")
            {
                assert_ne!(
                    absent_decoded, false_decoded,
                    "{} {name}: None vs Some(false)",
                    case.fixture
                );
            } else {
                assert_eq!(
                    absent_decoded, false_decoded,
                    "{} {name}: absent defaults to false",
                    case.fixture
                );
            }
            for (value, expected) in [
                ("TRUE", "true"),
                ("TrUe", "true"),
                ("false", "false"),
                ("yes", "false"),
                ("1", "false"),
                (" true ", "false"),
                ("", "false"),
            ] {
                let mut fields = baseline.clone();
                fields.insert(name.into(), value.into());
                let mut canonical = baseline.clone();
                canonical.insert(name.into(), expected.into());
                assert_eq!(
                    (case.decode)(&fields),
                    (case.decode)(&canonical),
                    "{} {name}={value}",
                    case.fixture
                );
            }
        }
    }
}
